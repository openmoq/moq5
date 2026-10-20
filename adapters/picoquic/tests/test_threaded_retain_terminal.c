/*
 * Terminal retention on the threaded SERVER (cfg.retain_terminal_until_ack)
 * and the per-connection user slot.
 *
 * A relay binding may keep using a connection's session after the pump that
 * saw it close: its detach can be held for later pumps. These rows hold the
 * session pointer the way such an application does -- read it on every later
 * pump, not only through iteration -- and acknowledge only when they choose.
 * The session must stay valid until then, whatever ended the connection, and
 * leave iteration only after the acknowledgment. Real server and client over
 * loopback; bounded waits only.
 *
 * Rows (argv[1]):
 *   peer_close     the client facade is stopped (a peer terminal)
 *   app_close      the server app closes the connection itself
 *   service_fatal  a service failure in the post-pump pass (test seam)
 *   cap            retained connections count toward max_connections
 *   cap_legacy     the same with retention off (control)
 *   legacy         retention off: the connection leaves without an ack
 *   prefix         retention set past an old struct_size is not read
 *   client         retention on a client facade is refused
 *   stop           stop/destroy reclaim an unacknowledged record
 */

#include <moq/picoquic_threaded.h>
#include <moq/session.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* The appended field starts at or past the sizeof of the layout that ended
 * at `target`, on every ABI: an old caller's tail padding is never read. */
#define PRE_RETAIN_SIZEOF                                                   \
    ((offsetof(moq_pq_threaded_cfg_t, target) + sizeof(void *) +           \
      alignof(moq_pq_threaded_cfg_t) - 1) /                                 \
     alignof(moq_pq_threaded_cfg_t) * alignof(moq_pq_threaded_cfg_t))
_Static_assert(offsetof(moq_pq_threaded_cfg_t, retain_terminal_until_ack) >=
                   PRE_RETAIN_SIZEOF,
               "retain_terminal_until_ack must start past the old sizeof");

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                        \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

/* Test seam of the test-internals build: a service failure in the next
 * service pass, queuing the real SESSION_CLOSED. */
void moq_pq_threaded_test_arm_service_fatal(moq_pq_threaded_conn_t *c);

#ifndef MOQ_TEST_CERT_PATH
int main(void)
{
    printf("SKIP: threaded_retain_terminal (no test certificates)\n");
    return 0;
}
#else

#define SERVER_CLOSE_CODE 0x5254u   /* the server app's close code */
#define HOLD_PUMPS        6         /* pumps the session is kept after close */

/* -- Counting allocator --------------------------------------------- */

typedef struct { int64_t balance; } count_alloc_t;

static void *ca_alloc(size_t size, void *ctx)
{
    void *p = malloc(size);
    if (p) ((count_alloc_t *)ctx)->balance++;
    return p;
}

static void *ca_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx)
{
    (void)old_sz;
    void *p = realloc(ptr, new_sz);
    if (p && !ptr) ((count_alloc_t *)ctx)->balance++;
    return p;
}

static void ca_free(void *ptr, size_t size, void *ctx)
{
    (void)size;
    if (ptr) ((count_alloc_t *)ctx)->balance--;
    free(ptr);
}

/* -- Ports and certificates ----------------------------------------- */

static int pick_free_udp_port(void)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    int port = 0;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0) {
        socklen_t len = sizeof(a);
        if (getsockname(fd, (struct sockaddr *)&a, &len) == 0)
            port = (int)ntohs(a.sin_port);
    }
    close(fd);
    return port;
}

static moq_result_t create_server(moq_pq_threaded_cfg_t *cfg,
                                  moq_pq_threaded_t **out, int *out_port)
{
    moq_result_t rc = MOQ_ERR_INTERNAL;
    for (int attempt = 0; attempt < 16; attempt++) {
        int port = pick_free_udp_port();
        if (port == 0) continue;
        cfg->port = port;
        *out = NULL;
        rc = moq_pq_threaded_create(cfg, out);
        if (rc == MOQ_OK) { *out_port = port; return rc; }
    }
    return rc;
}

static int certs_readable(void)
{
    const char *paths[2] = { MOQ_TEST_CERT_PATH, MOQ_TEST_KEY_PATH };
    for (int i = 0; i < 2; i++) {
        FILE *f = fopen(paths[i], "rb");
        if (!f) {
            fprintf(stderr, "FAIL: test certificate not readable: %s\n",
                    paths[i]);
            return 0;
        }
        fclose(f);
    }
    return 1;
}

static int idle_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                     uint64_t now, void *ctx)
{
    (void)t; (void)lane; (void)now; (void)ctx;
    return 0;
}

/* -- The server application ----------------------------------------- *
 * Follows connections through iteration, keeps the first connection's
 * session pointer, and after that session's SESSION_CLOSED keeps reading it
 * for HOLD_PUMPS pumps before acknowledging (when allowed to). */

enum { ACT_NONE, ACT_APP_CLOSE, ACT_SERVICE_FATAL };

static int user_tags[4];   /* distinct addresses for the user slot */

typedef struct {
    _Atomic int action;                 /* what the server does once established */
    _Atomic int allow_ack;              /* app thread: acknowledging is permitted */
    _Atomic int ack_enabled;            /* row expects the ack contract (retention) */

    moq_pq_threaded_conn_t *conn;   /* the first connection; network thread */
    moq_session_t *held;            /* its session, held past its close */
    _Atomic int accepted;               /* distinct connections seen */
    _Atomic int established;
    _Atomic int acted;
    _Atomic int closed_seen;
    _Atomic int held_pumps;             /* pumps since SESSION_CLOSED */
    _Atomic int acked;
    _Atomic int ack_dup_ok;
    _Atomic int ack_live_wrong_state;   /* ack on a live connection refused */
    _Atomic int ack_early_wrong_state;  /* ack before SESSION_CLOSED was polled */
    _Atomic int ack_legacy_wrong_state; /* retention off: ack refused */
    _Atomic int user_ok;                /* the slot returned its value every pump */
    _Atomic int user_bad;
    _Atomic int vanished_before_ack;    /* left iteration with no ack */
    _Atomic int vanished_after_ack;
    _Atomic int seen_after_ack;         /* still iterated on a later pump */
    _Atomic int pumps_after_close_without_conn;
} srv_ctx_t;

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *vctx)
{
    (void)t; (void)now;
    srv_ctx_t *s = (srv_ctx_t *)vctx;
    int present = 0;
    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_lane_next_conn(lane, c)) != NULL) {
        if (moq_picoquic_threaded_conn_user(c) == NULL) {
            moq_picoquic_threaded_conn_set_user(
                c, &user_tags[s->accepted < 4 ? s->accepted : 3]);
            (s->accepted += 1);
            if (!s->conn) {
                s->conn = c;
                s->held = moq_pq_threaded_conn_session(c);
            }
        }
        if (c != s->conn) continue;
        present = 1;
        if (s->acked) { s->seen_after_ack = 1; continue; }
        if (moq_picoquic_threaded_conn_user(c) == &user_tags[0]) s->user_ok++;
        else s->user_bad++;

        moq_session_t *sess = moq_pq_threaded_conn_session(c);
        if (!sess || sess != s->held) { s->user_bad++; continue; }
        if (!s->established && moq_session_state(sess) == MOQ_SESS_ESTABLISHED) {
            (s->established = 1);
            if (moq_picoquic_threaded_conn_ack_terminal(c) ==
                MOQ_ERR_WRONG_STATE)
                s->ack_live_wrong_state = 1;
        }
        if (s->established && !s->acted && s->action != ACT_NONE) {
            s->acted = 1;
            if (s->action == ACT_APP_CLOSE) {
                if (moq_pq_threaded_conn_close(c, SERVER_CLOSE_CODE) != MOQ_OK)
                    s->user_bad++;
            } else {
                moq_pq_threaded_test_arm_service_fatal(c);
            }
        }
        if (s->ack_enabled && !s->closed_seen && s->acted == 1 &&
            s->action == ACT_APP_CLOSE &&
            moq_picoquic_threaded_conn_ack_terminal(c) == MOQ_ERR_WRONG_STATE)
            s->ack_early_wrong_state = 1;

        moq_event_t ev[8];
        size_t ne = 0;
        moq_session_poll_events_ex(sess, ev, 8, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            if (ev[i].kind == MOQ_EVENT_SESSION_CLOSED)
                (s->closed_seen = 1);
            moq_event_cleanup(&ev[i]);
        }
        if (!s->closed_seen) continue;
        if (!s->ack_enabled) {
            if (moq_picoquic_threaded_conn_ack_terminal(c) ==
                MOQ_ERR_WRONG_STATE)
                s->ack_legacy_wrong_state = 1;
            continue;
        }
        if (s->held_pumps < HOLD_PUMPS ||
            !(s->allow_ack))
            continue;
        if (moq_picoquic_threaded_conn_ack_terminal(c) == MOQ_OK) {
            s->ack_dup_ok =
                moq_picoquic_threaded_conn_ack_terminal(c) == MOQ_OK;
            (s->acked = 1);
        }
    }
    /* The held session is read on every pump after its close, including
     * pumps that no longer iterate the connection: what a binding with a
     * held detach does. A reclaimed session is a use-after-free here. */
    if (s->held && s->closed_seen && !s->acked &&
        (present || s->ack_enabled)) {
        if (moq_session_state(s->held) != MOQ_SESS_CLOSED) s->user_bad++;
        moq_event_t ev[4];
        size_t ne = 0;
        moq_session_poll_events_ex(s->held, ev, 4, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&ev[i]);
        s->held_pumps++;
        if (!present) s->pumps_after_close_without_conn++;
    }
    if (s->conn && !present) {
        if ((s->acked))
            (s->vanished_after_ack = 1);
        else
            (s->vanished_before_ack = 1);
        s->conn = NULL;
        if (!s->acked) s->held = NULL;   /* legacy: nothing left to hold */
    }
    return 0;
}

/* The server stops pumping once no connection is left, so departure is
 * judged here, on the app thread, from the connection count. */
static void note_departure(moq_pq_threaded_t *srv, srv_ctx_t *s)
{
    if (moq_pq_threaded_conn_count(srv) != 0 || !s->accepted) return;
    if ((s->acked))
        (s->vanished_after_ack = 1);
    else
        (s->vanished_before_ack = 1);
}

/* A wall-clock bound. A wake with no connection completes at once, so a
 * count of waits is no bound on time. */
static struct timespec deadline_in(int seconds)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    t.tv_sec += seconds;
    return t;
}

static int time_before(const struct timespec *dl)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec < dl->tv_sec ||
           (now.tv_sec == dl->tv_sec && now.tv_nsec < dl->tv_nsec);
}

/* Drive the server's pumps (wake + wait) until cond holds or 20 s pass. */
#define DRIVE_UNTIL(srv, cond)                                             \
    for (struct timespec dl_ = deadline_in(20); time_before(&dl_) && !(cond);) { \
        moq_pq_threaded_wake(srv);                                         \
        moq_pq_threaded_wait(srv, 10000);                                  \
    }

static void server_cfg(moq_pq_threaded_cfg_t *cfg, srv_ctx_t *s,
                       const moq_alloc_t *alloc, bool retain)
{
    moq_pq_threaded_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = alloc;
    cfg->perspective = MOQ_PERSPECTIVE_SERVER;
    cfg->cert_path = MOQ_TEST_CERT_PATH;
    cfg->key_path = MOQ_TEST_KEY_PATH;
    cfg->insecure_skip_verify = true;
    cfg->on_lane_pump = server_pump;
    cfg->on_lane_pump_ctx = s;
    cfg->idle_timeout_ms = 1000;
    cfg->retain_terminal_until_ack = retain;
}

static moq_pq_threaded_t *client_connect(int port)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "localhost";
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = idle_pump;
    moq_pq_threaded_t *cli = NULL;
    return moq_pq_threaded_create(&cfg, &cli) == MOQ_OK ? cli : NULL;
}

static void client_close(moq_pq_threaded_t **cli)
{
    if (!*cli) return;
    moq_pq_threaded_stop(*cli);
    moq_pq_threaded_destroy(*cli);
    *cli = NULL;
}

/* One held-terminal row: connect, end the connection by `how`, hold the
 * session HOLD_PUMPS pumps past its close, then acknowledge. */
static void row_held(const char *how)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.ack_enabled = 1;
    s.allow_ack = 1;
    s.action = strcmp(how, "app_close") == 0       ? ACT_APP_CLOSE
               : strcmp(how, "service_fatal") == 0 ? ACT_SERVICE_FATAL
                                                   : ACT_NONE;
    count_alloc_t ca = { 0 };
    moq_alloc_t alloc = { .alloc = ca_alloc, .realloc = ca_realloc,
                          .free = ca_free, .ctx = &ca };
    moq_pq_threaded_cfg_t cfg;
    server_cfg(&cfg, &s, &alloc, true);
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    CHECK(create_server(&cfg, &srv, &port) == MOQ_OK);
    if (!srv) return;
    moq_pq_threaded_t *cli = client_connect(port);
    CHECK(cli != NULL);
    DRIVE_UNTIL(srv, (s.established));
    CHECK(s.established);
    if (s.action == ACT_NONE) client_close(&cli);
    DRIVE_UNTIL(srv, (note_departure(srv, &s),
                      (s.vanished_after_ack) ||
                      (s.vanished_before_ack)));

    CHECK(s.closed_seen);
    CHECK(s.ack_live_wrong_state);
    CHECK(s.held_pumps >= HOLD_PUMPS);
    CHECK(s.pumps_after_close_without_conn == 0);
    CHECK(s.acked);
    CHECK(s.ack_dup_ok);
    CHECK(s.vanished_after_ack);
    CHECK(!s.vanished_before_ack);
    CHECK(!s.seen_after_ack);
    CHECK(s.user_ok > 0 && s.user_bad == 0);
    if (s.action == ACT_APP_CLOSE) CHECK(s.ack_early_wrong_state);
    CHECK(moq_pq_threaded_conn_count(srv) == 0);
    /* Off the network thread and on NULL. */
    CHECK(moq_picoquic_threaded_conn_ack_terminal(NULL) == MOQ_ERR_INVAL);
    CHECK(moq_picoquic_threaded_conn_user(NULL) == NULL);

    client_close(&cli);
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    CHECK(ca.balance == 0);
}

/* Connect a client and drive the server until it accepts one more
 * connection. picoquic itself counts a closing connection toward the same
 * cap until it deletes it -- the previous client's, and a refused attempt's
 * own -- so a refused client is replaced and retried, a bounded number of
 * times, each retry after the server has run long enough for the refused
 * attempt to drain. Returns the accepted client, or NULL. */
static moq_pq_threaded_t *connect_until_accepted(moq_pq_threaded_t *srv,
                                                 srv_ctx_t *s, int port,
                                                 int attempts)
{
    int before = (s->accepted);
    for (int a = 0; a < attempts; a++) {
        moq_pq_threaded_t *cli = client_connect(port);
        for (struct timespec dl = deadline_in(5); time_before(&dl);) {
            moq_pq_threaded_wake(srv);
            moq_pq_threaded_wait(srv, 10000);
            if ((s->accepted) > before)
                return cli;
            if (!cli || moq_pq_threaded_is_fatal(cli)) break;
        }
        client_close(&cli);
        /* Let the refused attempt drain: wait without a wake, which returns
         * early only on real activity. */
        for (int i = 0; i < 50; i++) moq_pq_threaded_wait(srv, 10000);
    }
    return NULL;
}

/* max_connections = 1. With retention, an unacknowledged terminal keeps its
 * place: a second client is refused, however often it retries, until the
 * first is acknowledged. Without it (the control), the same retries are
 * accepted once picoquic has deleted its own closing connection, so the
 * refusal above is the retained record, not picoquic's. */
static void row_cap(bool retain)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.ack_enabled = retain;
    moq_pq_threaded_cfg_t cfg;
    server_cfg(&cfg, &s, moq_alloc_default(), retain);
    cfg.max_connections = 1;
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    CHECK(create_server(&cfg, &srv, &port) == MOQ_OK);
    if (!srv) return;
    moq_pq_threaded_t *a = client_connect(port);
    DRIVE_UNTIL(srv, (s.established));
    CHECK(s.established);
    client_close(&a);
    if (retain) {
        DRIVE_UNTIL(srv, s.held_pumps >= HOLD_PUMPS);
        CHECK(s.closed_seen && !s.acked);
        CHECK(moq_pq_threaded_conn_count(srv) == 1);
        /* A retained terminal is not the legacy accessors' live connection. */
        CHECK(moq_pq_threaded_session(srv) == NULL);
    } else {
        DRIVE_UNTIL(srv, (note_departure(srv, &s), s.vanished_before_ack));
        CHECK(s.vanished_before_ack);
    }

    moq_pq_threaded_t *b = connect_until_accepted(srv, &s, port, 12);
    if (retain) {
        CHECK(b == NULL);
        CHECK((s.accepted) == 1);
        CHECK(moq_pq_threaded_conn_count(srv) == 1);
        (s.allow_ack = 1);
        DRIVE_UNTIL(srv, (note_departure(srv, &s),
                          (s.vanished_after_ack)));
        CHECK(s.vanished_after_ack);
        CHECK(moq_pq_threaded_conn_count(srv) == 0);
        b = connect_until_accepted(srv, &s, port, 12);
    }
    CHECK(b != NULL);
    CHECK((s.accepted) == 2);
    client_close(&b);
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
}

/* Retention off, or set past the caller's struct_size: the terminal leaves
 * iteration after its observation pump, with no acknowledgment, and an ack
 * is refused. */
static void row_legacy(bool truncated)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    moq_pq_threaded_cfg_t cfg;
    server_cfg(&cfg, &s, moq_alloc_default(), truncated);
    if (truncated)
        cfg.struct_size = (uint32_t)offsetof(moq_pq_threaded_cfg_t,
                                             _reserved_retain);
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    CHECK(create_server(&cfg, &srv, &port) == MOQ_OK);
    if (!srv) return;
    moq_pq_threaded_t *cli = client_connect(port);
    DRIVE_UNTIL(srv, (s.established));
    CHECK(s.established);
    client_close(&cli);
    DRIVE_UNTIL(srv, (note_departure(srv, &s),
                      (s.vanished_before_ack)));
    CHECK(s.closed_seen);
    CHECK(s.vanished_before_ack);
    CHECK(s.ack_legacy_wrong_state);
    CHECK(s.ack_live_wrong_state);
    CHECK(s.user_ok > 0 && s.user_bad == 0);
    CHECK(moq_pq_threaded_conn_count(srv) == 0);
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
}

static void row_client(void)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "localhost";
    cfg.port = 4433;
    cfg.on_lane_pump = idle_pump;
    cfg.retain_terminal_until_ack = true;
    moq_pq_threaded_t *cli = (moq_pq_threaded_t *)&cfg;
    CHECK(moq_pq_threaded_create(&cfg, &cli) == MOQ_ERR_INVAL);
    CHECK(cli == NULL);
}

/* An unacknowledged record is reclaimed by stop/destroy. */
static void row_stop(void)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    s.ack_enabled = 1;
    count_alloc_t ca = { 0 };
    moq_alloc_t alloc = { .alloc = ca_alloc, .realloc = ca_realloc,
                          .free = ca_free, .ctx = &ca };
    moq_pq_threaded_cfg_t cfg;
    server_cfg(&cfg, &s, &alloc, true);
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    CHECK(create_server(&cfg, &srv, &port) == MOQ_OK);
    if (!srv) return;
    moq_pq_threaded_t *cli = client_connect(port);
    DRIVE_UNTIL(srv, (s.established));
    client_close(&cli);
    DRIVE_UNTIL(srv, s.held_pumps >= HOLD_PUMPS);
    CHECK(s.closed_seen && !s.acked);
    CHECK(moq_pq_threaded_conn_count(srv) == 1);
    CHECK(moq_pq_threaded_session(srv) == NULL);
    moq_pq_threaded_stop(srv);
    moq_pq_threaded_destroy(srv);
    CHECK(ca.balance == 0);
}

int main(int argc, char **argv)
{
    const char *row = argc > 1 ? argv[1] : "peer_close";
    if (!certs_readable()) return 1;
    if (strcmp(row, "peer_close") == 0 || strcmp(row, "app_close") == 0 ||
        strcmp(row, "service_fatal") == 0)
        row_held(row);
    else if (strcmp(row, "cap") == 0) row_cap(true);
    else if (strcmp(row, "cap_legacy") == 0) row_cap(false);
    else if (strcmp(row, "legacy") == 0) row_legacy(false);
    else if (strcmp(row, "prefix") == 0) row_legacy(true);
    else if (strcmp(row, "client") == 0) row_client();
    else if (strcmp(row, "stop") == 0) row_stop();
    else {
        fprintf(stderr, "unknown row %s\n", row);
        return 2;
    }
    if (failures) {
        printf("FAILED: %d check(s) [threaded_retain_terminal %s]\n",
               failures, row);
        return 1;
    }
    printf("PASS: threaded_retain_terminal %s\n", row);
    return 0;
}
#endif /* MOQ_TEST_CERT_PATH */
