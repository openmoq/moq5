/*
 * A PicoQUIC connection's close through the relay binding and its reap pass:
 * the facade retains the terminal connection and its session while the
 * binding tears its state down, and the reap pass acknowledges it only once
 * the binding has let the session go.
 *
 * The closing connection owns a namespace that 33 core-level watchers
 * observe, so its close fans NS_GONE out to every one of them through the
 * binding's teardown. The pump that drives the facade is the production
 * shape: attach on first sight, moqr_bind_pump, then moqr_relay_reap_pass_pq.
 * Real PicoQUIC server and client over loopback. The core and binding are the
 * test-internals builds; the core's close hold is the only seam used.
 *
 * Rows (argv[3]):
 *   plain  the teardown completes within its pump: every watcher receives
 *          NS_GONE, the binding releases the session, and the connection is
 *          reclaimed, with no reap refusal.
 *   held   the core refuses the binding's close (WOULD_BLOCK, before any
 *          effect) across HELD_PUMPS pumps. Through every one of them the
 *          session stays owned by the binding and retained by the facade --
 *          on the lane, unacknowledged, its state readable -- and no watcher
 *          has NS_GONE yet. Then the hold is released: the close completes,
 *          the watchers get NS_GONE exactly once each, the terminal is
 *          acknowledged exactly once, the connection is reclaimed, and every
 *          byte the core and binding allocated is returned.
 */

#include "../cli/conn_reap.h"

#include <moq/picoquic_threaded.h>
#include <moq/relay/moqr_bind.h>
#include <moq/relay/relay.h>
#include <moq/session.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* The core test-internals close hold (relay/src/core/relay.c). */
extern void moqr_core_debug_hold_binding_close(moqr_core_t *c,
                                               uint32_t attempts);
extern uint32_t moqr_core_debug_binding_close_held(const moqr_core_t *c,
                                                   uint64_t *out_cookie);

static int failures = 0;

#define CHECK(expr)                                                     \
    do {                                                                \
        if (!(expr)) {                                                  \
            fprintf(stderr, "FAIL: %s:%d: %s\n",                        \
                    __FILE__, __LINE__, #expr);                         \
            failures++;                                                 \
        }                                                               \
    } while (0)

#define WATCHERS   33u
#define INTENTS    4u
#define HELD_PUMPS 4
#define WATCHER_COOKIE(i) (MOQR_SHARD_COOKIE_BASE + 0x100u + (i))

/* -- a counting allocator for the core and binding ------------------------ */

static _Atomic long long g_live_bytes;

static void *
count_alloc(size_t n, void *ctx)
{
    (void)ctx;
    void *p = malloc(n);
    if (p != NULL) g_live_bytes += (long long)n;
    return p;
}

static void *
count_realloc(void *p, size_t old_n, size_t new_n, void *ctx)
{
    (void)ctx;
    void *q = realloc(p, new_n);
    if (q != NULL) g_live_bytes += (long long)new_n - (long long)old_n;
    return q;
}

static void
count_free(void *p, size_t n, void *ctx)
{
    (void)ctx;
    if (p != NULL) g_live_bytes -= (long long)n;
    free(p);
}

static const moq_alloc_t k_count_alloc = {
    .ctx = NULL, .alloc = count_alloc, .realloc = count_realloc,
    .free = count_free,
};

/* -- the relay side, on the server facade's network thread ---------------- */

typedef struct {
    moqr_core_t *core;
    moqr_bind_t *bind;
    bool         hold;             /* the held row */
    /* network thread only */
    moq_session_t *bound;          /* the session the binding attached */
    bool           released_hold;
    /* published to the driver */
    _Atomic int  attached;
    _Atomic int  ns_found;         /* NS_FOUND intents routed to watchers */
    _Atomic int  ns_gone;          /* NS_GONE intents routed to watchers */
    _Atomic int  ns_gone_while_held;
    _Atomic int  other;            /* any other routed intent */
    _Atomic int  held_pumps;       /* pumps: session closed, still bound */
    _Atomic int  lost_while_bound; /* bound, but no longer on the lane */
    _Atomic int  acked_while_bound;
    _Atomic int  released;         /* the binding let the session go */
    _Atomic int  acked;            /* terminals acknowledged by the reap */
    _Atomic int  reap_failed;
    _Atomic unsigned held_closes;  /* closes the hold refused */
    _Atomic unsigned long long held_cookie;
} relay_t;

static bool
router(void *ctx, const moqr_intent_t *it, uint64_t now_us)
{
    (void)now_us;
    relay_t *r = ctx;
    if (it->kind == MOQR_INTENT_NS_GONE) {
        r->ns_gone++;
        if (r->hold && !r->released_hold) r->ns_gone_while_held++;
    } else if (it->kind == MOQR_INTENT_NS_FOUND) {
        r->ns_found++;
    } else {
        r->other++;
    }
    return true;
}

static bool
on_lane(moq_pq_threaded_lane_t *lane, const moq_session_t *s)
{
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, NULL);
         c != NULL; c = moq_pq_threaded_lane_next_conn(lane, c)) {
        if (moq_pq_threaded_conn_session(c) == s) return true;
    }
    return false;
}

static int
server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
            uint64_t now_us, void *vctx)
{
    (void)t;
    relay_t *r = vctx;
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, NULL);
         c != NULL; c = moq_pq_threaded_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_pq_threaded_conn_session(c);
        if (s == NULL) {
            continue;
        }
        if (moq_picoquic_threaded_conn_user(c) == NULL) {
            if (moqr_bind_conn_open(r->bind, s, moq_session_version(s)) ==
                MOQR_OK) {
                moq_picoquic_threaded_conn_set_user(c, MOQR_CONN_OPENED);
                r->bound = s;
                r->attached++;
                if (r->hold) {
                    moqr_core_debug_hold_binding_close(r->core, UINT32_MAX);
                }
            } else {
                moq_picoquic_threaded_conn_set_user(c, MOQR_CONN_DEAD);
                (void)moq_pq_threaded_conn_close(c, 0);
            }
        }
    }
    (void)moqr_bind_pump(r->bind, now_us);
    if (r->bound != NULL) {
        if (moqr_bind_conn_is_open(r->bind, r->bound)) {
            /* Still the binding's: the facade must still hold it. */
            if (!on_lane(lane, r->bound)) {
                r->lost_while_bound++;
            } else if (moq_session_state(r->bound) == MOQ_SESS_CLOSED) {
                r->held_pumps++;
            }
        } else {
            r->released = 1;
        }
    }
    uint64_t cookie = 0;
    r->held_closes = moqr_core_debug_binding_close_held(r->core, &cookie);
    r->held_cookie = cookie;
    moqr_reap_stats_t st = { 0 };
    bool bound_before = r->bound != NULL &&
                        moqr_bind_conn_is_open(r->bind, r->bound);
    if (!moqr_relay_reap_pass_pq(r->bind, lane, &st)) {
        r->reap_failed = 1;
    }
    r->acked += (int)st.acked;
    if (bound_before && st.acked > 0) {
        r->acked_while_bound++;
    }
    /* After HELD_PUMPS held pumps, release the hold: the next pump's retry
     * completes the close. */
    if (r->hold && !r->released_hold && r->held_pumps >= HELD_PUMPS) {
        r->released_hold = true;
        moqr_core_debug_hold_binding_close(r->core, 0);
    }
    if (r->released && !(r->bound != NULL && on_lane(lane, r->bound))) {
        r->bound = NULL;
    }
    return 0;
}

/* -- the publisher, on the client facade's network thread ----------------- */

typedef struct {
    _Atomic int announced;
    _Atomic int accepted;
} client_t;

static int
client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
            uint64_t now_us, void *vctx)
{
    (void)lane;
    client_t *c = vctx;
    moq_session_t *s = moq_pq_threaded_session(t);
    if (s == NULL || moq_session_state(s) != MOQ_SESS_ESTABLISHED) {
        return 0;
    }
    if (!c->announced) {
        moq_bytes_t parts[2] = { MOQ_BYTES_LITERAL("dead"),
                                 MOQ_BYTES_LITERAL("owned") };
        moq_publish_namespace_cfg_t pc;
        moq_publish_namespace_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ .parts = parts, .count = 2 };
        moq_announcement_t ann;
        if (moq_session_publish_namespace(s, &pc, now_us, &ann) == MOQ_OK) {
            c->announced = 1;
        }
    }
    moq_event_t ev[1];
    while (moq_session_poll_events(s, ev, 1) > 0) {
        if (ev[0].kind == MOQ_EVENT_NAMESPACE_ACCEPTED) {
            c->accepted = 1;
        }
        moq_event_cleanup(&ev[0]);
    }
    return 0;
}

/* -- the run -------------------------------------------------------------- */

static int
free_udp_port(void)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in a;
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t len = sizeof(a);
    int port = 0;
    if (bind(fd, (struct sockaddr *)&a, sizeof(a)) == 0 &&
        getsockname(fd, (struct sockaddr *)&a, &len) == 0)
        port = ntohs(a.sin_port);
    close(fd);
    return port;
}

static struct timespec
deadline_in(int seconds)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    t.tv_sec += seconds;
    return t;
}

static int
time_before(const struct timespec *dl)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec < dl->tv_sec ||
           (now.tv_sec == dl->tv_sec && now.tv_nsec < dl->tv_nsec);
}

int
main(int argc, char **argv)
{
    if (argc != 4 || (strcmp(argv[3], "plain") != 0 &&
                      strcmp(argv[3], "held") != 0)) {
        fprintf(stderr, "usage: %s cert key plain|held\n", argv[0]);
        return 2;
    }
    const char *row = argv[3];
    static relay_t r;
    memset(&r, 0, sizeof(r));
    r.hold = strcmp(row, "held") == 0;
    moqr_core_relay_cfg_t cc;
    moqr_core_relay_cfg_init_sized(&cc, sizeof(cc), &k_count_alloc);
    cc.max_bindings = 80;
    cc.max_ns_nodes = 8;
    cc.max_ns_subs = WATCHERS;
    cc.max_intents = INTENTS;
    CHECK(moqr_core_create(&cc, &r.core) == MOQR_OK);
    moqr_bind_cfg_t bc;
    moqr_bind_cfg_init_sized(&bc, sizeof(bc), &k_count_alloc);
    bc.core = r.core;
    bc.max_conns = 4;
    bc.router = router;
    bc.router_ctx = &r;
    CHECK(moqr_bind_create(&bc, &r.bind) == MOQR_OK);
    if (failures) return 1;
    /* The watchers: core bindings of their own, each observing the prefix. */
    moq_bytes_t pfx[1] = { MOQ_BYTES_LITERAL("dead") };
    moqr_ns_t prefix = { .parts = pfx, .count = 1 };
    for (uint32_t i = 0; i < WATCHERS; i++) {
        moqr_binding_t w;
        CHECK(moqr_core_binding_open(r.core, WATCHER_COOKIE(i), &w) == MOQR_OK);
        CHECK(moqr_core_ns_subscribe(r.core, w, prefix, 0xD000u + i) == MOQR_OK);
    }

    int port = 0;
    moq_pq_threaded_t *srv = NULL;
    for (int attempt = 0; attempt < 16 && !srv; attempt++) {
        port = free_udp_port();
        moq_pq_threaded_cfg_t sc;
        moq_pq_threaded_cfg_init_sized(&sc, sizeof(sc));
        sc.alloc = moq_alloc_default();
        sc.perspective = MOQ_PERSPECTIVE_SERVER;
        sc.cert_path = argv[1];
        sc.key_path = argv[2];
        sc.port = port;
        sc.send_request_capacity = true;
        sc.on_lane_pump = server_pump;
        sc.on_lane_pump_ctx = &r;
        sc.retain_terminal_until_ack = true;
        if (moq_pq_threaded_create(&sc, &srv) != MOQ_OK) srv = NULL;
    }
    CHECK(srv != NULL);
    static client_t cl;
    memset(&cl, 0, sizeof(cl));
    moq_pq_threaded_t *cli = NULL;
    if (srv) {
        moq_pq_threaded_cfg_t cc2;
        moq_pq_threaded_cfg_init_sized(&cc2, sizeof(cc2));
        cc2.alloc = moq_alloc_default();
        cc2.perspective = MOQ_PERSPECTIVE_CLIENT;
        cc2.host = "127.0.0.1";
        cc2.port = port;
        cc2.insecure_skip_verify = true;
        cc2.send_request_capacity = true;
        cc2.on_lane_pump = client_pump;
        cc2.on_lane_pump_ctx = &cl;
        CHECK(moq_pq_threaded_create(&cc2, &cli) == MOQ_OK);
    }
    /* The namespace is announced and the watchers have it. */
    for (struct timespec dl = deadline_in(20); cli && time_before(&dl) &&
         !cl.accepted;) {
        moq_pq_threaded_wake(cli);
        moq_pq_threaded_wake(srv);
        moq_pq_threaded_wait(srv, 10000);
    }
    CHECK(cl.accepted);
    CHECK(r.attached == 1);
    CHECK(r.ns_found == (int)WATCHERS);
    /* The publisher goes away: a peer terminal. The server is woken so it
     * keeps pumping while the close is held, and until the binding has
     * released the session and the connection is gone -- not merely until
     * the connection is gone, which a facade that dropped a bound session
     * would reach first. */
    if (cli) {
        moq_pq_threaded_stop(cli);
        moq_pq_threaded_destroy(cli);
    }
    for (struct timespec dl = deadline_in(10); srv && time_before(&dl) &&
         !(r.released && moq_pq_threaded_conn_count(srv) == 0);) {
        moq_pq_threaded_wake(srv);
        moq_pq_threaded_wait(srv, 10000);
    }
    size_t left = srv ? moq_pq_threaded_conn_count(srv) : 0;
    printf("held_detach %s: held pumps %d, closes held %u (cookie %#llx), "
           "NS_GONE %d of %u (%d while held), acked %d, connections left %zu\n",
           row, r.held_pumps, r.held_closes, r.held_cookie, r.ns_gone,
           WATCHERS, r.ns_gone_while_held, r.acked, left);
    CHECK(r.released);
    CHECK(r.ns_gone == (int)WATCHERS);
    CHECK(r.other == 0);
    CHECK(r.acked == 1);
    CHECK(r.acked_while_bound == 0);
    CHECK(r.lost_while_bound == 0);
    CHECK(!r.reap_failed);
    CHECK(left == 0);
    if (r.hold) {
        CHECK(r.held_pumps >= HELD_PUMPS);
        CHECK(r.held_closes >= (unsigned)HELD_PUMPS);
        CHECK(r.ns_gone_while_held == 0);
        /* The close it held was the connection's, never a watcher's. */
        CHECK(r.held_cookie != 0 &&
              (r.held_cookie < WATCHER_COOKIE(0) ||
               r.held_cookie > WATCHER_COOKIE(WATCHERS - 1u)));
    } else {
        CHECK(r.held_closes == 0);
    }
    if (srv) {
        moq_pq_threaded_stop(srv);
        moq_pq_threaded_destroy(srv);
    }
    moqr_bind_destroy(r.bind);
    moqr_core_destroy(r.core);
    printf("held_detach %s: core and binding bytes outstanding %lld\n", row,
           (long long)g_live_bytes);
    CHECK(g_live_bytes == 0);
    if (failures) {
        printf("FAILED: %d check(s) [relay_pq_held_detach %s]\n", failures, row);
        return 1;
    }
    printf("PASS: relay_pq_held_detach %s\n", row);
    return 0;
}
