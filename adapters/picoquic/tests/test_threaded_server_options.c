/*
 * Threaded facade options: streaming_objects, socket_buffer_bytes,
 * bind_address, and the server's pump and wake behaviour.
 * Real servers and clients over loopback; bounded waits only.
 *
 * Rows (argv[1]):
 *   streaming_on    a client with streaming_objects receives OBJECT_CHUNK
 *   streaming_off   the default delivers whole OBJECT_RECEIVED (control)
 *   socket_buffer   a server with a set socket buffer size serves a client
 *   wake_pressure   a server woken without pause by another thread still
 *                   reads its sockets: the client's request is answered
 *   bind_v4         bound to 0.0.0.0: an IPv4 client is served, IPv6 is not
 *   bind_v6         bound to ::: an IPv6 client is served, IPv4 is not
 *   socket_buffer_limit  a buffer size past INT32_MAX is refused at create
 *   idle_wake       a server with no connection pumps when woken
 *   idle_deadline   a server with no connection pumps at its app deadline,
 *                   unwoken
 */

#include <moq/picoquic_threaded.h>
#include <moq/session.h>
#include <moq/rcbuf.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <stdatomic.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                        \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

#ifndef MOQ_TEST_CERT_PATH
int main(void)
{
    printf("SKIP: threaded_server_options (no test certificates)\n");
    return 0;
}
#else

#define OBJ_LEN 6000u   /* larger than one packet: several chunks */

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

/* -- Server: accept SUBSCRIBE, write one OBJ_LEN object ------------- */

typedef struct {
    _Atomic int accepted;     /* connections seen */
    _Atomic int served;       /* objects written */
    _Atomic int requests;     /* SUBSCRIBE requests seen */
    _Atomic int pumps;
    _Atomic int events;       /* events polled, all connections */
    _Atomic int kinds[64];    /* by kind */
    _Atomic int accept_rc;    /* first refused accept */
    _Atomic int write_rc;     /* first refused open/write */
    struct idle_dl *dl;       /* set before create: the app deadline */
} srv_ctx_t;

/* The app deadline of the idle_deadline row, owned by the network thread:
 * the driver asks for it through arm_in_us; the server's pump arms it in its
 * own clock (the pump's now, picoquic's clock, which the time check also
 * uses) and reports, through the atomics, the pump whose now reached it. */
typedef struct idle_dl {
    _Atomic uint64_t arm_in_us;  /* driver: arm this far after the next pump */
    uint64_t at;                 /* network thread only; UINT64_MAX: none */
    _Atomic uint64_t armed_now;  /* the arming pump's now; 0 until armed */
    _Atomic uint64_t due_now;    /* the first pump's now at or past `at` */
    _Atomic int early_pumps;     /* pumps after arming, before `at` */
} idle_dl_t;

static void idle_dl_on_pump(idle_dl_t *d, uint64_t now)
{
    uint64_t arm = atomic_exchange(&d->arm_in_us, 0);
    if (arm) {
        d->at = now + arm;
        d->armed_now = now;
        return;
    }
    if (d->at == UINT64_MAX)
        return;
    if (now >= d->at) {
        d->due_now = now;
        d->at = UINT64_MAX;
    } else {
        d->early_pumps++;
    }
}

static int user_tag;

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *vctx)
{
    (void)t;
    srv_ctx_t *s = (srv_ctx_t *)vctx;
    (s->pumps += 1);
    if (s->dl) idle_dl_on_pump(s->dl, now);
    moq_pq_threaded_conn_t *c = NULL;
    while ((c = moq_pq_threaded_lane_next_conn(lane, c)) != NULL) {
        if (moq_picoquic_threaded_conn_user(c) == NULL) {
            moq_picoquic_threaded_conn_set_user(c, &user_tag);
            (s->accepted += 1);
        }
        moq_session_t *sess = moq_pq_threaded_conn_session(c);
        if (!sess) continue;
        moq_event_t ev[8];
        size_t ne = 0;
        moq_session_poll_events_ex(sess, ev, 8, sizeof(moq_event_t), &ne);
        for (size_t i = 0; i < ne; i++) {
            s->events++;
            if (ev[i].kind < 64) s->kinds[ev[i].kind]++;
            if (ev[i].kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                moq_subscription_t sub = ev[i].u.subscribe_request.sub;
                moq_accept_subscribe_cfg_t acfg;
                moq_accept_subscribe_cfg_init(&acfg);
                s->requests++;
                moq_result_t arc = moq_session_accept_subscribe(sess, sub, &acfg, now);
                if (arc < 0 && !s->accept_rc) s->accept_rc = (int)arc;
                if (arc >= 0) {
                    static uint8_t body[OBJ_LEN];
                    memset(body, 0x5a, sizeof(body));
                    moq_subgroup_cfg_t sg;
                    moq_subgroup_cfg_init(&sg);
                    sg.group_id = 0;
                    moq_subgroup_handle_t h;
                    moq_rcbuf_t *b = NULL;
                    if (moq_session_open_subgroup(sess, sub, &sg, now, &h) >= 0 &&
                        moq_rcbuf_create(moq_alloc_default(), body, sizeof(body),
                                         &b) == MOQ_OK) {
                        if (moq_session_write_object(sess, h, 0, b, now) >= 0)
                            (s->served += 1);
                        moq_rcbuf_decref(b);
                        moq_session_close_subgroup(sess, h, now);
                    }
                }
            }
            moq_event_cleanup(&ev[i]);
        }
    }
    return 0;
}

static const struct sockaddr *g_bind;   /* the next server's bind address */

/* Called by the facade on the network thread only, like the pump. */
static uint64_t idle_deadline_fn(void *ctx)
{
    return ((idle_dl_t *)ctx)->at;
}

static moq_result_t server_create(int port, uint32_t sockbuf,
                                  srv_ctx_t *s, moq_pq_threaded_t **out)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.cert_path = MOQ_TEST_CERT_PATH;
    cfg.key_path = MOQ_TEST_KEY_PATH;
    cfg.port = port;
    cfg.on_lane_pump = server_pump;
    cfg.on_lane_pump_ctx = s;
    cfg.socket_buffer_bytes = sockbuf;
    cfg.bind_address = g_bind;
    if (s->dl) {
        cfg.app_deadline_us = idle_deadline_fn;
        cfg.app_deadline_ctx = s->dl;
    }
    /* draft-16: the client's SUBSCRIBE needs request credit; the capacity is
     * left at its documented default (64). */
    cfg.send_request_capacity = true;
    *out = NULL;
    return moq_pq_threaded_create(&cfg, out);
}

/* -- Client: subscribe, count object events ------------------------- */

typedef struct {
    _Atomic int subscribed;
    _Atomic int sub_rc;          /* first refused subscribe */
    _Atomic int pumps;           /* client pumps run */
    _Atomic int pending;         /* the session had output pending at its last pump */
    _Atomic int state;           /* session state at its last pump */
    _Atomic int established;
    _Atomic int chunks;          /* OBJECT_CHUNK events */
    _Atomic int chunk_bytes;
    _Atomic int chunk_end;       /* a chunk with end = true */
    _Atomic int whole;           /* OBJECT_RECEIVED events */
    _Atomic int whole_bytes;
} cli_ctx_t;

static int client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *vctx)
{
    (void)lane;
    cli_ctx_t *c = (cli_ctx_t *)vctx;
    moq_session_t *sess = moq_pq_threaded_session(t);
    if (!sess) return 0;
    c->pumps++;
    c->pending = moq_session_has_pending_output(sess);
    c->state = (int)moq_session_state(sess);
    if (moq_session_state(sess) == MOQ_SESS_ESTABLISHED)
        (c->established = 1);
    if (!c->subscribed && moq_session_state(sess) == MOQ_SESS_ESTABLISHED) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("opt") };
        sc.track_namespace.parts = ns;
        sc.track_namespace.count = 1;
        sc.track_name = MOQ_BYTES_LITERAL("t");
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        moq_result_t src = moq_session_subscribe(sess, &sc, now, &sub);
        if (src >= 0)
            c->subscribed = 1;
        else if (!c->sub_rc)
            c->sub_rc = (int)src;
    }
    moq_event_t ev[16];
    size_t ne = 0;
    moq_session_poll_events_ex(sess, ev, 16, sizeof(moq_event_t), &ne);
    for (size_t i = 0; i < ne; i++) {
        if (ev[i].kind == MOQ_EVENT_OBJECT_CHUNK) {
            const moq_object_chunk_event_t *oc = &ev[i].u.object_chunk;
            c->chunk_bytes += oc->chunk ? (int)moq_rcbuf_len(oc->chunk) : 0;
            if (oc->end) c->chunk_end = 1;
            (c->chunks += 1);
        } else if (ev[i].kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev[i].u.object_received;
            c->whole_bytes += o->payload ? (int)moq_rcbuf_len(o->payload) : 0;
            (c->whole += 1);
        }
        moq_event_cleanup(&ev[i]);
    }
    return 0;
}

static const char *g_client_host = "localhost";

static moq_pq_threaded_t *client_create(int port, bool streaming, cli_ctx_t *c)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = g_client_host;
    cfg.port = port;
    cfg.insecure_skip_verify = true;
    cfg.on_lane_pump = client_pump;
    cfg.on_lane_pump_ctx = c;
    cfg.streaming_objects = streaming;
    moq_pq_threaded_t *cli = NULL;
    return moq_pq_threaded_create(&cfg, &cli) == MOQ_OK ? cli : NULL;
}

static void stop(moq_pq_threaded_t **f)
{
    if (!*f) return;
    moq_pq_threaded_stop(*f);
    moq_pq_threaded_destroy(*f);
    *f = NULL;
}

/* Drive a facade until cond holds or 20 s pass. */
#define DRIVE_UNTIL(f, cond)                                               \
    for (struct timespec dl_ = deadline_in(20); time_before(&dl_) && !(cond);) { \
        moq_pq_threaded_wake(f);                                           \
        moq_pq_threaded_wait(f, 10000);                                    \
    }

static void row_streaming(bool on)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        port = pick_free_udp_port();
        if (port && server_create(port, 0, &s, &srv) != MOQ_OK) srv = NULL;
    }
    CHECK(srv != NULL);
    if (!srv) return;
    cli_ctx_t c;
    memset(&c, 0, sizeof(c));
    moq_pq_threaded_t *cli = client_create(port, on, &c);
    CHECK(cli != NULL);
    if (cli) {
        DRIVE_UNTIL(cli, (on ? (c.chunk_end)
                             : (c.whole)));
    }
    printf("streaming: established=%d subscribed=%d served=%d accepted=%d "
           "chunks=%d whole=%d sub_rc=%d\n", c.established, c.subscribed, s.served,
           s.accepted, c.chunks, c.whole, c.sub_rc);
    if (on) {
        CHECK(c.chunks >= 1);
        CHECK(c.chunk_end);
        CHECK(c.chunk_bytes == (int)OBJ_LEN);
        CHECK(c.whole == 0);
    } else {
        CHECK(c.whole == 1);
        CHECK(c.whole_bytes == (int)OBJ_LEN);
        CHECK(c.chunks == 0);
    }
    stop(&cli);
    stop(&srv);
}

static void row_socket_buffer(void)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        port = pick_free_udp_port();
        if (port && server_create(port, 1u << 20, &s, &srv) != MOQ_OK)
            srv = NULL;
    }
    CHECK(srv != NULL);
    if (!srv) return;
    cli_ctx_t c;
    memset(&c, 0, sizeof(c));
    moq_pq_threaded_t *cli = client_create(port, false, &c);
    if (cli) DRIVE_UNTIL(cli, (c.whole));
    CHECK(c.whole == 1);
    stop(&cli);
    stop(&srv);
}

typedef struct { moq_pq_threaded_t *t; _Atomic int stop; } hammer_t;

static void *hammer_main(void *arg)
{
    hammer_t *h = (hammer_t *)arg;
    while (!(h->stop))
        (void)moq_pq_threaded_wake(h->t);
    return NULL;
}

/* picoquic's select reads only its wake pipe while the pipe is readable, so
 * a wake written on every pass would keep the server's sockets unread. Under
 * a thread that wakes the server without pause, a client's subscription must
 * still be answered. */
static void row_wake_pressure(void)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        port = pick_free_udp_port();
        if (port && server_create(port, 0, &s, &srv) != MOQ_OK) srv = NULL;
    }
    CHECK(srv != NULL);
    if (!srv) return;
    /* Several wakers, as several lanes and the coordinator would be. */
    hammer_t h = { .t = srv, .stop = 0 };
    pthread_t th[4];
    for (int i = 0; i < 4; i++)
        CHECK(pthread_create(&th[i], NULL, hammer_main, &h) == 0);
    cli_ctx_t c;
    memset(&c, 0, sizeof(c));
    moq_pq_threaded_t *cli = client_create(port, false, &c);
    CHECK(cli != NULL);
    for (struct timespec dl = deadline_in(10);
         time_before(&dl) && !(c.whole);) {
        if (cli && !c.subscribed) moq_pq_threaded_wake(cli);
        if (cli) moq_pq_threaded_wait(cli, 10000);
    }
    (h.stop = 1);
    for (int i = 0; i < 4; i++) pthread_join(th[i], NULL);
    printf("wake_pressure: established %d subscribed %d objects %d, server "
           "requests %d\n", c.established, c.subscribed, c.whole, s.requests);
    CHECK(c.whole == 1);
    stop(&cli);
    stop(&srv);
}

/* One client of the given address family against the server: served (it
 * receives the object) or not within a bounded wait. */
static bool served_from(int port, const char *host, int seconds)
{
    g_client_host = host;
    cli_ctx_t c;
    memset(&c, 0, sizeof(c));
    moq_pq_threaded_t *cli = client_create(port, false, &c);
    g_client_host = "localhost";
    if (!cli) return false;
    for (struct timespec dl = deadline_in(seconds);
         time_before(&dl) && !(c.whole) &&
         !moq_pq_threaded_is_fatal(cli);) {
        if (!(c.subscribed))
            moq_pq_threaded_wake(cli);
        moq_pq_threaded_wait(cli, 10000);
    }
    bool ok = (c.whole) == 1;
    stop(&cli);
    return ok;
}

/* A wildcard bind address keeps its family: the server listens on that
 * family alone. */
static void row_bind(int af)
{
    struct sockaddr_storage ss;
    memset(&ss, 0, sizeof(ss));
    if (af == AF_INET) {
        struct sockaddr_in *v4 = (struct sockaddr_in *)&ss;
        v4->sin_family = AF_INET;
        v4->sin_addr.s_addr = htonl(INADDR_ANY);
    } else {
        struct sockaddr_in6 *v6 = (struct sockaddr_in6 *)&ss;
        v6->sin6_family = AF_INET6;
        v6->sin6_addr = in6addr_any;
    }
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    moq_pq_threaded_t *srv = NULL;
    int port = 0;
    g_bind = (const struct sockaddr *)&ss;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        port = pick_free_udp_port();
        if (port && server_create(port, 0, &s, &srv) != MOQ_OK) srv = NULL;
    }
    g_bind = NULL;
    CHECK(srv != NULL);
    if (!srv) return;
    bool v4 = served_from(port, "127.0.0.1", af == AF_INET ? 20 : 3);
    bool v6 = served_from(port, "::1", af == AF_INET6 ? 20 : 3);
    printf("bind %s: IPv4 client served %d, IPv6 client served %d\n",
           af == AF_INET ? "0.0.0.0" : "::", (int)v4, (int)v6);
    CHECK(v4 == (af == AF_INET));
    CHECK(v6 == (af == AF_INET6));
    stop(&srv);
}

static void row_socket_buffer_limit(void)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    moq_pq_threaded_t *srv = (moq_pq_threaded_t *)&s;
    CHECK(server_create(pick_free_udp_port(), (uint32_t)INT32_MAX + 1u,
                        &s, &srv) == MOQ_ERR_INVAL);
    CHECK(srv == NULL);
}

static moq_pq_threaded_t *idle_server(srv_ctx_t *s)
{
    moq_pq_threaded_t *srv = NULL;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        int port = pick_free_udp_port();
        if (port && server_create(port, 0, s, &srv) != MOQ_OK) srv = NULL;
    }
    return srv;
}

/* An application's work does not wait for a client: a wake pumps a server
 * that has none. */
static void row_idle_wake(void)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    moq_pq_threaded_t *srv = idle_server(&s);
    CHECK(srv != NULL);
    if (!srv) return;
    int before = (s.pumps);
    for (struct timespec dl = deadline_in(5);
         time_before(&dl) && (s.pumps) == before;) {
        CHECK(moq_pq_threaded_wake(srv) == MOQ_OK);
        moq_pq_threaded_wait(srv, 50000);
    }
    CHECK((s.pumps) > before);
    CHECK((s.accepted) == 0);
    stop(&srv);
}

/* A server with no connection, never woken after the deadline is armed,
 * still pumps when its application deadline comes due: a pump whose now
 * reaches the deadline is observed, well inside 3 s (where the loop's own
 * longest wait would otherwise end). Pumps before the deadline (the arming
 * wake's after_send, say) are counted, not taken for it. */
static void row_idle_deadline(void)
{
    srv_ctx_t s;
    memset(&s, 0, sizeof(s));
    static idle_dl_t dl;
    memset(&dl, 0, sizeof(dl));
    dl.at = UINT64_MAX;
    s.dl = &dl;
    moq_pq_threaded_t *srv = idle_server(&s);
    CHECK(srv != NULL);
    if (!srv) return;
    /* Per the app_deadline_us contract, a changed deadline is announced with
     * one wake; that wake's pass pumps, and the pump arms it. */
    dl.arm_in_us = 300000u;
    CHECK(moq_pq_threaded_wake(srv) == MOQ_OK);
    for (struct timespec end = deadline_in(2);
         time_before(&end) && dl.armed_now == 0;)
        moq_pq_threaded_wait(srv, 10000);
    CHECK(dl.armed_now != 0);
    for (struct timespec end = deadline_in(3);
         time_before(&end) && dl.due_now == 0;)
        moq_pq_threaded_wait(srv, 50000);
    uint64_t armed = dl.armed_now, due = dl.due_now;
    printf("idle_deadline: armed at %llu, due pump %llu us later, "
           "%d earlier pumps\n", (unsigned long long)armed,
           due ? (unsigned long long)(due - armed) : 0ull, dl.early_pumps);
    CHECK(due != 0);
    CHECK(due >= armed + 300000u);
    stop(&srv);
}

int main(int argc, char **argv)
{
    const char *row = argc > 1 ? argv[1] : "streaming_on";
    if (strcmp(row, "streaming_on") == 0) row_streaming(true);
    else if (strcmp(row, "streaming_off") == 0) row_streaming(false);
    else if (strcmp(row, "socket_buffer") == 0) row_socket_buffer();
    else if (strcmp(row, "wake_pressure") == 0) row_wake_pressure();
    else if (strcmp(row, "bind_v4") == 0) row_bind(AF_INET);
    else if (strcmp(row, "bind_v6") == 0) row_bind(AF_INET6);
    else if (strcmp(row, "socket_buffer_limit") == 0) row_socket_buffer_limit();
    else if (strcmp(row, "idle_wake") == 0) row_idle_wake();
    else if (strcmp(row, "idle_deadline") == 0) row_idle_deadline();
    else {
        fprintf(stderr, "unknown row %s\n", row);
        return 2;
    }
    if (failures) {
        printf("FAILED: %d check(s) [threaded_server_options %s]\n",
               failures, row);
        return 1;
    }
    printf("PASS: threaded_server_options %s\n", row);
    return 0;
}
#endif /* MOQ_TEST_CERT_PATH */
