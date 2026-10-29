/*
 * The managed stream-resource block over real MsQuic loopback, through the
 * managed facade on both ends. Public managed API only.
 *
 * A managed server publishes groups to a managed client subscriber, one
 * object per group, and keeps every subgroup open (no FIN) until the main
 * thread releases them. Each open subgroup is one server-initiated
 * WebTransport uni stream: a local reservation on the server and a native
 * peer uni grant on the client, held by both ends together with the
 * session's persistent control and request streams.
 *
 * Row "default": every field zero. The provider backs the client's whole
 * default uni grant (8): three critical streams and the server's control
 * stream leave DEFAULT_OPEN subgroups open at once. The next one waits for
 * native credit, not adapter or engine storage, and is delivered once the
 * open subgroups close and their records retire. Before release the run
 * observes, in each session's own serialized domain through the public
 * budget query, the client's whole uni grant resident and the server's
 * extra subgroup charged as a pending local start; nothing retires before
 * release, so exactly DEFAULT_OPEN groups can have arrived.
 *
 * Row "sized": the client grants OPEN_SUBGROUPS + 3 + spare uni credit and
 * the server reserves SIZED_LOCAL local streams; every group must arrive
 * while all subgroups are still open. That needs the credit to reach the
 * wire, max_local_streams to reach the listener, and the attach adapter's
 * table to follow the queried api_capacity on both ends.
 *
 * Row "local-bound": the same credit with the server's max_local_streams at
 * BOUND_LOCAL. The server's control stream and BOUND_LOCAL - 1 subgroups are
 * open at once; the next subgroup's local open waits, and is delivered
 * after the open subgroups close. Before release the run observes, in the
 * server's serialized domain, all BOUND_LOCAL reservations resident and at
 * least one refused local open, so exactly BOUND_OPEN groups can have
 * arrived.
 *
 * Row "invalid": create() refuses a block outside wtquic's plan bounds with
 * MOQ_ERR_INVAL.
 *
 * The waits are hang guards, never timing assertions.
 *
 * Usage: test_wtquic_msquic_managed_capacity <cert.pem> <key.pem>
 */
#include <moq/wtquic.h>
#include <moq/wtquic_msquic_managed.h>
#include <moq/session.h>
#include <moq/rcbuf.h>

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static int g_fail;
#define CHECK(c)                                                            \
    do {                                                                    \
        if (!(c)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);    \
            g_fail++;                                                       \
        }                                                                   \
    } while (0)

#define WAIT_SECS 20
#define OPEN_SUBGROUPS 40u   /* past the default grant and the default L */
/* Client default uni grant 8, less three critical streams and the server's
 * draft-18 control stream. */
#define DEFAULT_OPEN 4u
#define DEFAULT_SUBGROUPS (DEFAULT_OPEN + 1u)
#define PEER_UNI (OPEN_SUBGROUPS + 3u + 8u)
#define SIZED_LOCAL 64u
/* The server's control stream holds one local reservation. */
#define BOUND_LOCAL 8u
#define BOUND_OPEN (BOUND_LOCAL - 1u)
#define BOUND_SUBGROUPS (BOUND_OPEN + 2u)

static const char *cert_path;
static const char *key_path;

/* Shared run state, guarded by mu. The pumps record and act; the main thread
 * waits and asserts. */
static struct run {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    unsigned groups;          /* groups the publisher opens */
    /* server */
    bool have_sub;
    moq_subscription_t sub;
    bool published;
    int publish_errors;
    unsigned opened;          /* subgroups opened and still open */
    moq_subgroup_handle_t sg[OPEN_SUBGROUPS];
    bool release;             /* main thread: close the subgroups now */
    bool closed_all;
    /* client */
    bool subscribed;
    int subscribe_errors;
    bool seen[OPEN_SUBGROUPS];
    unsigned distinct;        /* distinct groups received */
    unsigned distinct_while_open; /* ... received before release */
    unsigned objects;
    int object_errors;
    bool client_closed;
    /* occupancy each pump last read from its own session's budget */
    uint32_t srv_local_resident;
    uint64_t srv_local_blocked;
    uint32_t cli_peer_uni_resident;
    int budget_errors;
} g;

/* Caller holds g.mu, inside the lane pump: the session's serialized domain. */
static bool read_budget(moq_wtquic_msquic_managed_conn_t *c,
                        wtq_stream_budget_t *out)
{
    moq_wtquic_conn_t *a = moq_wtquic_msquic_managed_conn_adapter(c);
    wtq_session_t *ws = a ? moq_wtquic_conn_wtq_session(a) : NULL;
    *out = (wtq_stream_budget_t)WTQ_STREAM_BUDGET_INIT;
    if (ws == NULL)
        return false;
    if (wtq_session_stream_budget(ws, out) != WTQ_OK) {
        g.budget_errors++;
        return false;
    }
    return true;
}

static void run_reset(unsigned groups)
{
    pthread_mutex_lock(&g.mu);
    g.groups = groups;
    g.have_sub = g.published = g.release = g.closed_all = false;
    g.publish_errors = 0;
    g.opened = 0;
    g.subscribed = false;
    g.subscribe_errors = 0;
    memset(g.seen, 0, sizeof(g.seen));
    g.distinct = g.distinct_while_open = g.objects = 0;
    g.object_errors = 0;
    g.client_closed = false;
    g.srv_local_resident = 0;
    g.srv_local_blocked = 0;
    g.cli_peer_uni_resident = 0;
    g.budget_errors = 0;
    pthread_mutex_unlock(&g.mu);
}

static uint8_t payload_byte(uint64_t group) { return (uint8_t)(0xA0u + group); }

static void server_publish(moq_session_t *s)
{
    for (unsigned i = 0; i < g.groups; i++) {
        moq_subgroup_cfg_t sgc;
        moq_subgroup_cfg_init(&sgc);
        sgc.group_id = i;
        sgc.publisher_priority = 128;
        if (moq_session_open_subgroup(s, g.sub, &sgc, 0, &g.sg[i]) < 0) {
            g.publish_errors++;
            break;
        }
        g.opened++;
        uint8_t bytes[32];
        memset(bytes, payload_byte(i), sizeof(bytes));
        moq_rcbuf_t *buf = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &buf) < 0) {
            g.publish_errors++;
            break;
        }
        if (moq_session_write_object(s, g.sg[i], 0, buf, 0) < 0)
            g.publish_errors++;
        moq_rcbuf_decref(buf);
    }
}

static int server_pump(moq_wtquic_msquic_managed_t *m,
                       moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                       void *user)
{
    (void)m; (void)now_us; (void)user;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_wtquic_msquic_managed_conn_session(c);
        if (s == NULL)
            continue;
        pthread_mutex_lock(&g.mu);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST && !g.have_sub) {
                g.sub = ev.u.subscribe_request.sub;
                g.have_sub = true;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                if (moq_session_accept_subscribe(s, g.sub, &ac, 0) < 0)
                    g.publish_errors++;
            }
            moq_event_cleanup(&ev);
        }
        if (g.have_sub && !g.published) {
            g.published = true;
            server_publish(s);
        }
        if (g.release && !g.closed_all) {
            g.closed_all = true;
            for (unsigned i = 0; i < g.opened; i++)
                if (moq_session_close_subgroup(s, g.sg[i], 0) < 0)
                    g.publish_errors++;
        }
        wtq_stream_budget_t b;
        if (read_budget(c, &b)) {
            g.srv_local_resident = b.local_resident;
            g.srv_local_blocked = b.local_open_blocked;
        }
        pthread_cond_broadcast(&g.cv);
        pthread_mutex_unlock(&g.mu);
    }
    return 0;
}

static int client_pump(moq_wtquic_msquic_managed_t *m,
                       moq_wtquic_msquic_managed_lane_t *lane, uint64_t now_us,
                       void *user)
{
    (void)m; (void)now_us; (void)user;
    for (moq_wtquic_msquic_managed_conn_t *c =
             moq_wtquic_msquic_lane_next_conn(lane, NULL);
         c != NULL; c = moq_wtquic_msquic_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_wtquic_msquic_managed_conn_session(c);
        if (s == NULL)
            continue;
        pthread_mutex_lock(&g.mu);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE && !g.subscribed) {
                static const moq_bytes_t ns[] = {
                    { (const uint8_t *)"cap", 3 },
                };
                moq_subscribe_cfg_t sc;
                moq_subscription_t sub;
                moq_subscribe_cfg_init(&sc);
                sc.track_namespace.parts = (moq_bytes_t *)ns;
                sc.track_namespace.count = 1;
                sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
                sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
                sc.start_group = 0;
                sc.start_object = 0;
                g.subscribed = true;
                if (moq_session_subscribe(s, &sc, 0, &sub) < 0)
                    g.subscribe_errors++;
            } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                uint64_t grp = ev.u.object_received.group_id;
                const moq_rcbuf_t *pl = ev.u.object_received.payload;
                const uint8_t *p = pl ? moq_rcbuf_data(pl) : NULL;
                size_t n = pl ? moq_rcbuf_len(pl) : 0;
                bool ok = grp < g.groups && n == 32;
                for (size_t i = 0; ok && i < n; i++)
                    ok = p[i] == payload_byte(grp);
                if (!ok) {
                    g.object_errors++;
                } else if (!g.seen[grp]) {
                    g.seen[grp] = true;
                    g.distinct++;
                    if (!g.release)
                        g.distinct_while_open++;
                }
                g.objects++;
            } else if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                g.client_closed = true;
            }
            moq_event_cleanup(&ev);
        }
        wtq_stream_budget_t b;
        if (read_budget(c, &b))
            g.cli_peer_uni_resident = b.peer_uni_resident;
        pthread_cond_broadcast(&g.cv);
        pthread_mutex_unlock(&g.mu);
    }
    return 0;
}

/* Wait until *field reaches `least` (under g.mu). */
static bool wait_count(const unsigned *field, unsigned least)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&g.mu);
    int rc = 0;
    while (*field < least && rc == 0)
        rc = pthread_cond_timedwait(&g.cv, &g.mu, &deadline);
    bool hit = *field >= least;
    pthread_mutex_unlock(&g.mu);
    return hit;
}

static bool wait_flag(const bool *field)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&g.mu);
    int rc = 0;
    while (!*field && rc == 0)
        rc = pthread_cond_timedwait(&g.cv, &g.mu, &deadline);
    bool hit = *field;
    pthread_mutex_unlock(&g.mu);
    return hit;
}

static void set_resources(moq_wtquic_msquic_managed_cfg_t *cfg,
                          uint32_t local, uint32_t uni)
{
    /* session queues deep enough for every subgroup's actions and objects */
    cfg->max_actions = 1024;
    cfg->max_events = 256;
    cfg->max_local_streams = local;
    cfg->peer_uni_streams = uni;
    cfg->peer_bidi_streams = 0;
}

typedef enum {
    BOUND_NONE,         /* every group fits: all arrive while open */
    BOUND_CLIENT_CREDIT, /* the client's native uni grant is the limit */
    BOUND_SERVER_LOCAL, /* the server's max_local_streams is the limit */
} bound_t;

/* Caller holds g.mu. The bound is in force: its owning session reports the
 * limiting resource fully held, with the next stream refused or pending. */
static bool bound_observed(bound_t bound, uint32_t limit, unsigned open_expect)
{
    if (g.distinct < open_expect)
        return false;
    switch (bound) {
    case BOUND_CLIENT_CREDIT:
        /* client grant resident; server control + every subgroup charged,
         * the last one as a pending native start */
        return g.cli_peer_uni_resident == limit &&
               g.srv_local_resident == 1u + g.groups;
    case BOUND_SERVER_LOCAL:
        return g.srv_local_resident == limit && g.srv_local_blocked >= 1;
    default:
        return true;
    }
}

/* Wake both facades so each pump re-reads its own session; the deadline is a
 * hang guard, never the proof. */
static bool wait_bound(moq_wtquic_msquic_managed_t *srv,
                       moq_wtquic_msquic_managed_t *cli, bound_t bound,
                       uint32_t limit, unsigned open_expect)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += WAIT_SECS;
    pthread_mutex_lock(&g.mu);
    bool hit = bound_observed(bound, limit, open_expect);
    while (!hit) {
        pthread_mutex_unlock(&g.mu);
        (void)moq_wtquic_msquic_managed_wake(srv);
        (void)moq_wtquic_msquic_managed_wake(cli);
        pthread_mutex_lock(&g.mu);
        struct timespec slice;
        clock_gettime(CLOCK_REALTIME, &slice);
        slice.tv_nsec += 50 * 1000 * 1000;
        if (slice.tv_nsec >= 1000000000L) {
            slice.tv_sec++;
            slice.tv_nsec -= 1000000000L;
        }
        if (slice.tv_sec > deadline.tv_sec ||
            (slice.tv_sec == deadline.tv_sec && slice.tv_nsec > deadline.tv_nsec))
            slice = deadline;
        int rc = pthread_cond_timedwait(&g.cv, &g.mu, &slice);
        hit = bound_observed(bound, limit, open_expect);
        if (rc != 0 && !hit && (slice.tv_sec == deadline.tv_sec &&
                                slice.tv_nsec == deadline.tv_nsec))
            break;
    }
    pthread_mutex_unlock(&g.mu);
    return hit;
}

/* `open_expect` groups must arrive while every subgroup is still open; the
 * rest (if any) after the subgroups are closed. A bounded row releases only
 * after its bound is observed, so no group past `open_expect` can have
 * arrived before release unless the bound was not enforced. */
static void run_row(const char *name, uint32_t server_local, uint32_t client_uni,
                    unsigned groups, unsigned open_expect, bound_t bound,
                    uint32_t limit)
{
    run_reset(groups);

    moq_wtquic_msquic_managed_cfg_t scfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = "127.0.0.1";
    scfg.port = 0;
    scfg.cert_path = cert_path;
    scfg.key_path = key_path;
    scfg.on_lane_pump = server_pump;
    scfg.max_connections = 1;
    set_resources(&scfg, server_local, 0);
    moq_wtquic_msquic_managed_t *srv = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&scfg, &srv) == MOQ_OK);
    if (srv == NULL)
        return;
    uint16_t port = moq_wtquic_msquic_managed_port(srv);
    CHECK(port != 0);

    moq_wtquic_msquic_managed_cfg_t ccfg;
    moq_wtquic_msquic_managed_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = moq_alloc_default();
    ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    ccfg.host = "127.0.0.1";
    ccfg.port = port;
    ccfg.insecure_skip_verify = true;
    ccfg.on_lane_pump = client_pump;
    set_resources(&ccfg, 0, client_uni);
    moq_wtquic_msquic_managed_t *cli = NULL;
    CHECK(moq_wtquic_msquic_managed_create(&ccfg, &cli) == MOQ_OK);

    if (cli != NULL) {
        CHECK(wait_count(&g.distinct, open_expect));
        CHECK(wait_bound(srv, cli, bound, limit, open_expect));
        pthread_mutex_lock(&g.mu);
        g.release = true;
        unsigned while_open = g.distinct_while_open;
        unsigned opened = g.opened;
        uint32_t srv_local = g.srv_local_resident;
        uint64_t srv_blocked = g.srv_local_blocked;
        uint32_t cli_uni = g.cli_peer_uni_resident;
        pthread_mutex_unlock(&g.mu);
        printf("%s: opened=%u received_while_open=%u/%u server_local=%u "
               "server_blocked=%llu client_peer_uni=%u\n", name, opened,
               while_open, groups, srv_local, (unsigned long long)srv_blocked,
               cli_uni);
        if (while_open != open_expect) {
            pthread_mutex_lock(&g.mu);
            for (unsigned i = 0; i < groups; i++)
                if (!g.seen[i])
                    printf("%s: group %u not received\n", name, i);
            pthread_mutex_unlock(&g.mu);
        }
        CHECK(moq_wtquic_msquic_managed_negotiated_version(cli) ==
              MOQ_VERSION_DRAFT_18);
        CHECK(opened == groups);
        CHECK(while_open == open_expect);
        CHECK(moq_wtquic_msquic_managed_wake(srv) == MOQ_OK);
        CHECK(wait_flag(&g.closed_all));
        /* a stream that waited for credit or a local reservation is
         * delivered once the open subgroups retire */
        CHECK(wait_count(&g.distinct, groups));
    }

    if (cli != NULL) {
        CHECK(moq_wtquic_msquic_managed_stop(cli) == MOQ_OK);
        CHECK(!moq_wtquic_msquic_managed_is_fatal(cli));
        moq_wtquic_msquic_managed_destroy(cli);
    }
    CHECK(moq_wtquic_msquic_managed_stop(srv) == MOQ_OK);
    moq_wtquic_msquic_managed_destroy(srv);

    pthread_mutex_lock(&g.mu);
    CHECK(g.publish_errors == 0);
    CHECK(g.subscribe_errors == 0);
    CHECK(g.object_errors == 0);
    CHECK(g.objects == groups);
    CHECK(g.budget_errors == 0);
    pthread_mutex_unlock(&g.mu);
}

static void invalid_row(void)
{
    struct { uint32_t local, uni, bidi; } bad[] = {
        { 1025, 0, 0 },       /* past the local bound */
        { 0, 2, 0 },          /* no room for the critical streams */
        { 0, 1025, 1 },
        { 0, 0, 1025 },
        { 0, 1000, 25 },      /* 1025 peer records */
        { 0, 1020, 0 },       /* with the default 7 bidi: 1027 */
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        moq_wtquic_msquic_managed_cfg_t cfg;
        moq_wtquic_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        cfg.host = "127.0.0.1";
        cfg.port = 9; /* never dialled: create() refuses first */
        cfg.on_lane_pump = client_pump;
        cfg.max_local_streams = bad[i].local;
        cfg.peer_uni_streams = bad[i].uni;
        cfg.peer_bidi_streams = bad[i].bidi;
        moq_wtquic_msquic_managed_t *m = (moq_wtquic_msquic_managed_t *)0x1;
        moq_result_t rc = moq_wtquic_msquic_managed_create(&cfg, &m);
        if (rc != MOQ_ERR_INVAL)
            fprintf(stderr, "invalid row %zu: rc=%d\n", i, (int)rc);
        CHECK(rc == MOQ_ERR_INVAL);
        CHECK(m == NULL);
        if (rc == MOQ_OK && m != NULL) {
            (void)moq_wtquic_msquic_managed_stop(m);
            moq_wtquic_msquic_managed_destroy(m);
        }
    }
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    cert_path = argv[1];
    key_path = argv[2];
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);

    invalid_row();
    run_row("default", 0, 0, DEFAULT_SUBGROUPS, DEFAULT_OPEN,
            BOUND_CLIENT_CREDIT, 8u);
    run_row("sized", SIZED_LOCAL, PEER_UNI, OPEN_SUBGROUPS, OPEN_SUBGROUPS,
            BOUND_NONE, 0u);
    run_row("local-bound", BOUND_LOCAL, PEER_UNI, BOUND_SUBGROUPS, BOUND_OPEN,
            BOUND_SERVER_LOCAL, BOUND_LOCAL);

    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
    if (g_fail) {
        fprintf(stderr, "%d check(s) failed\n", g_fail);
        return 1;
    }
    printf("wtquic_msquic_managed_capacity: OK\n");
    return 0;
}
