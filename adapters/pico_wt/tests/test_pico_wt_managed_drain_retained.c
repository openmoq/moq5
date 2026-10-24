/*
 * The managed WebTransport facade's drain holds while the session retains
 * reliable lifecycle output, over real draft-18 loopback at the facade's
 * ordinary capacities.
 *
 * The client issues SUBSCRIBE_COUNT subscriptions at once, after draining its
 * setup events, and then stops polling events. The server accepts all but the
 * last; once the client has acknowledged every SUBSCRIBE_OK, so its event
 * queue holds them, the server rejects the last. The REQUEST_ERROR and FIN
 * reach the client while its event queue is full: the session records the
 * reciprocal FIN as retained output and refuses the input (the error event has
 * no slot), leaving its action ring empty. Only the application's event
 * polling releases it.
 *
 * While the client's application withholds polling, a pass parked after
 * publishing its drain state must read: session output pending, bridge and
 * provider quiet, acknowledged, no wake since the pass began -- and the public
 * probe PENDING. Released, the client polls the SUBSCRIBE_OKs and then the
 * exact SUBSCRIBE_ERROR, its drain reaches DONE, and the server's bridge
 * retires exactly the rejected stream: the client's FIN reached the peer.
 *
 * White-box: the stage hook and drain facts are MOQ_PICO_WT_TESTING seams of
 * pico_wt_managed.c, compiled into this test; the bridge's stream count is
 * read on the owner network thread. Preparation failures fail the row and
 * unwind; every wait is bounded by a monotonic deadline.
 *
 * Each row's server starts on a port the IPv4 allocator found free when it
 * handed it out (tests/support/loopback_port.h). Nothing reserves it once the
 * allocator's socket closes, so a start may still find it taken. The facade
 * reports every start failure alike, so a failed start probes its port:
 * observed held (EADDRINUSE), the start moves to a fresh port, a bounded
 * number of times; free, or not probeable, the failure is the setup's own
 * and fatal, its cause printed. Every start prints its port and result. A
 * failed row is never retried. --port-control runs the classification
 * alone; --port-guards, its handling of starts that unexpectedly succeed;
 * --port-guards-setup-failure, that those guards refuse a setup failure in
 * place of such a start.
 */
#include <moq/pico_wt_managed.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>

#include "pico_wt_adapter.h"
#include "pico_wt_test_seam.h"
#include "../../../tests/support/loopback_port.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int failures = 0;
static bool expecting_failures;   /* a control that provokes its own CHECKs */
#define CHECK(expr)                                                          \
    do {                                                                     \
        if (!(expr)) {                                                       \
            fprintf(stderr, "%s: %s:%d: %s\n",                               \
                    expecting_failures ? "expected failure" : "FAIL",       \
                    __FILE__, __LINE__, #expr);                              \
            failures++;                                                      \
        }                                                                    \
    } while (0)

enum { BOUND_MS = 5000, SUBSCRIBE_COUNT = 17, REJECTED = SUBSCRIBE_COUNT - 1 };
#define REJECT_CODE MOQ_REQUEST_ERROR_UNAUTHORIZED

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    moq_pico_wt_managed_t *srv, *cli;

    /* server pump */
    int                accepted, accept_failed, requests;
    bool               held;            /* the request it will reject */
    moq_subscription_t held_sub;
    bool               allow_reject, rejected;
    moq_result_t       reject_rc;
    unsigned           srv_sample_req, srv_sample_done;
    size_t             srv_streams;

    /* client pump */
    bool               cli_hook_installed;
    bool               subscribed, subscribe_failed;
    moq_subscription_t subs[SUBSCRIBE_COUNT];
    size_t             idle_capacity;   /* action slots free when idle */
    int                pre_state;       /* the last setup observation */
    uint64_t           pre_credit;
    size_t             pre_capacity;
    bool               pre_pending;
    bool               withhold;        /* the client app does not poll */
    unsigned           cli_sample_req, cli_sample_done;
    bool               cli_pending;
    size_t             cli_capacity;
    int                oks_first_batch; /* OKs in the first released pump */
    bool               first_batch_done;
    int                oks_before_error, other_events;
    int                errors;          /* every SUBSCRIBE_ERROR, all pumps */
    unsigned           released_pumps;
    bool               error_seen;
    moq_subscription_t error_sub;
    moq_request_error_t error_code;

    /* client stage hook */
    bool               arm, observed, parked, release, park_timed_out;
    int                overtaken;       /* armed passes a wake overtook */
    /* The arming-order control: hold one pass at stage-2 entry, before it
     * reads the arm, so the arm and its wake land while it is in flight. */
    bool               hold_next;       /* the next pass is to be held */
    bool               pass_held;       /* a pass is held NOW (cleared when
                                         * the hold ends, under g.mu) */
    bool               hold_release, hold_ended, hold_timed_out;
    int                hold_bound_ms;   /* 0: BOUND_MS */
    moq_pwt_test_drain_facts_t facts;
} ctx_t;

static ctx_t g;
static void lock(void) { pthread_mutex_lock(&g.mu); }
static void unlock(void) { pthread_mutex_unlock(&g.mu); }

static uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* Under g.mu: wait for *flag within bound_ms. */
static bool wait_flag_ms(bool *flag, int bound_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    long long ns = (long long)ts.tv_nsec + (long long)(bound_ms % 1000) * 1000000LL;
    ts.tv_sec += bound_ms / 1000 + (time_t)(ns / 1000000000LL);
    ts.tv_nsec = (long)(ns % 1000000000LL);
    int rc = 0;
    while (!*flag && rc == 0) rc = pthread_cond_timedwait(&g.cv, &g.mu, &ts);
    return *flag;
}

/* Under g.mu: wait for *flag within BOUND_MS. */
static bool wait_flag(bool *flag) { return wait_flag_ms(flag, BOUND_MS); }

/* The forced-order row's verdict on its hold: it must have been released
 * while active, never timed out. Under g.mu. */
static bool forced_hold_ok(void) { return g.hold_ended && !g.hold_timed_out; }

/* Under g.mu: wait until *done == want within BOUND_MS. */
static bool wait_count(unsigned *done, unsigned want)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += BOUND_MS / 1000;
    int rc = 0;
    while (*done != want && rc == 0) rc = pthread_cond_timedwait(&g.cv, &g.mu, &ts);
    return *done == want;
}

/* The drain probe reaches `want` (1 DONE, 0 PENDING) within a BOUND_MS
 * monotonic deadline. A wake invalidates the published state until a later
 * pass republishes it: wake once, read as passes complete, and wake again
 * only when a monotonic second has passed since the last wake. */
static bool probe_reaches(moq_pico_wt_managed_t *m, int want)
{
    uint64_t start = mono_us(), deadline = start + (uint64_t)BOUND_MS * 1000u;
    uint64_t next_wake = start + 1000000u;
    moq_pico_wt_managed_wake(m);
    for (;;) {
        if (moq_pico_wt_managed_drain_state(m) == want) return true;
        uint64_t now = mono_us();
        if (now >= deadline) return false;
        if (now >= next_wake) {
            moq_pico_wt_managed_wake(m);
            next_wake = now + 1000000u;
        }
        uint64_t slice = deadline - now < 10000u ? deadline - now : 10000u;
        if (moq_pico_wt_managed_wait(m, slice) == MOQ_ERR_CLOSED) return false;
    }
}

/* A sample taken by the owner's next pump: request one, wake, wait for it. */
static bool sample(moq_pico_wt_managed_t *m, unsigned *req, unsigned *done)
{
    lock();
    unsigned want = ++*req;
    unlock();
    moq_pico_wt_managed_wake(m);
    lock();
    bool ok = wait_count(done, want);
    unlock();
    return ok;
}

/* -- server ---------------------------------------------------------- */

static int track_index(moq_bytes_t name)
{
    if (name.len < 2 || name.data[0] != 't') return -1;
    int v = 0;
    for (size_t i = 1; i < name.len; i++) {
        if (name.data[i] < '0' || name.data[i] > '9') return -1;
        v = v * 10 + (name.data[i] - '0');
    }
    return v;
}

static int server_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (!s) return 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            int idx = track_index(ev.u.subscribe_request.track_name);
            moq_subscription_t sub = ev.u.subscribe_request.sub;
            lock();
            g.requests++;
            unlock();
            if (idx == REJECTED) {
                lock(); g.held = true; g.held_sub = sub; pthread_cond_broadcast(&g.cv); unlock();
            } else {
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                bool ok = idx >= 0 && moq_session_accept_subscribe(s, sub, &ac, now) == MOQ_OK;
                lock();
                if (ok) g.accepted++; else g.accept_failed++;
                pthread_cond_broadcast(&g.cv);
                unlock();
            }
        }
        moq_event_cleanup(&ev);
    }
    lock();
    bool reject = g.allow_reject && g.held && !g.rejected;
    moq_subscription_t held = g.held_sub;
    unsigned req = g.srv_sample_req;
    unlock();
    if (reject) {
        moq_reject_subscribe_cfg_t rc;
        moq_reject_subscribe_cfg_init(&rc);
        rc.error_code = REJECT_CODE;
        moq_result_t r = moq_session_reject_subscribe(s, held, &rc, now);
        lock(); g.reject_rc = r; g.rejected = true; pthread_cond_broadcast(&g.cv); unlock();
    }
    if (req != g.srv_sample_done) {
        moq_pico_wt_conn_t *conn = moq_pico_wt_managed_test_conn(m);
        size_t n = conn ? moq_transport_bridge_stream_count(conn->bridge) : 0;
        lock(); g.srv_streams = n; g.srv_sample_done = req; pthread_cond_broadcast(&g.cv); unlock();
    }
    return 0;
}

/* -- client ---------------------------------------------------------- */

static void client_stage(moq_pico_wt_managed_t *m, int stage, void *ctx)
{
    (void)ctx;
    if (stage != 2) return;
    lock();
    if (g.hold_next) {
        g.hold_next = false;
        g.pass_held = true;
        pthread_cond_broadcast(&g.cv);
        if (!wait_flag_ms(&g.hold_release, g.hold_bound_ms ? g.hold_bound_ms : BOUND_MS))
            g.hold_timed_out = true;            /* a fixture failure, reported */
        g.pass_held = false;
        g.hold_ended = true;
        pthread_cond_broadcast(&g.cv);
    }
    bool mine = g.arm && !g.observed;
    unlock();
    if (!mine) return;
    moq_pwt_test_drain_facts_t f;
    moq_pico_wt_managed_test_drain_facts(m, &f);
    /* Observe only a pass no wake overtook. A pass already running when the
     * test armed is overtaken by the arming wake, and the wake still pending
     * guarantees another pass, which began after it. */
    if (!f.gen_unchanged) {
        lock(); g.overtaken++; unlock();
        return;
    }
    lock();
    g.facts = f;
    g.observed = g.parked = true;
    pthread_cond_broadcast(&g.cv);
    if (!wait_flag(&g.release)) g.park_timed_out = true;
    g.parked = false;
    pthread_cond_broadcast(&g.cv);
    unlock();
}

/* Issues every request; false if the session refused one. */
static bool subscribe_all(moq_session_t *s, uint64_t now)
{
    for (int i = 0; i < SUBSCRIBE_COUNT; i++) {
        char name[8];
        snprintf(name, sizeof(name), "t%d", i);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("retained") };
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ parts, 1 };
        sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        if (moq_session_subscribe(s, &sc, now, &g.subs[i]) != MOQ_OK) return false;
    }
    return true;
}

static int client_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)ctx;
    lock();
    bool install = !g.cli_hook_installed;
    unlock();
    if (install) {
        moq_pico_wt_managed_test_set_stage_hook(m, client_stage, NULL);
        lock(); g.cli_hook_installed = true; pthread_cond_broadcast(&g.cv); unlock();
    }
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (!s) return 0;

    lock();
    bool subscribed = g.subscribed || g.subscribe_failed, withhold = g.withhold;
    unsigned req = g.cli_sample_req;
    unlock();

    if (!subscribed) {
        /* Setup: drain every event, then issue all requests at once with
         * the idle ring as the baseline, before any response can arrive. */
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) == 1) moq_event_cleanup(&ev);
        /* Wait (a later pump) for room for every request and an idle
         * baseline: nothing owed. Draft-18 carries no request-ID credit
         * (moq_session_peer_request_capacity reads 0); its requests are
         * bounded by the transport's bidirectional stream credit, which the
         * server's count of received requests checks. */
        lock();
        g.pre_state = (int)moq_session_state(s);
        g.pre_credit = moq_session_peer_request_capacity(s);
        g.pre_capacity = moq_session_action_capacity(s);
        g.pre_pending = moq_session_has_pending_output(s);
        unlock();
        if (moq_session_state(s) != MOQ_SESS_ESTABLISHED ||
            moq_session_action_capacity(s) < SUBSCRIBE_COUNT ||
            moq_session_has_pending_output(s))
            return 0;
        size_t idle = moq_session_action_capacity(s);
        bool ok = subscribe_all(s, now);
        lock();
        g.idle_capacity = idle;
        if (ok) { g.subscribed = true; g.withhold = true; }
        else g.subscribe_failed = true;
        pthread_cond_broadcast(&g.cv);
        unlock();
        return 0;
    }

    if (withhold) {
        if (req != g.cli_sample_done) {
            bool pending = moq_session_has_pending_output(s);
            size_t cap = moq_session_action_capacity(s);
            lock();
            g.cli_pending = pending; g.cli_capacity = cap; g.cli_sample_done = req;
            pthread_cond_broadcast(&g.cv);
            unlock();
        }
        return 0;
    }

    /* Released: the application polls again. Every event is accounted for
     * across all released pumps: SUBSCRIBE_OKs before the first error, every
     * SUBSCRIBE_ERROR, and anything else (an OK after the error included). */
    lock();
    bool seen = g.error_seen;
    unlock();
    int oks = 0, other = 0, errors = 0;
    moq_subscription_t esub;
    memset(&esub, 0, sizeof(esub));
    moq_request_error_t ecode = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK && !seen && errors == 0) oks++;
        else if (ev.kind == MOQ_EVENT_SUBSCRIBE_ERROR) {
            if (!seen && errors == 0) {
                esub = ev.u.subscribe_error.sub;
                ecode = ev.u.subscribe_error.error_code;
            }
            errors++;
        } else other++;
        moq_event_cleanup(&ev);
    }
    lock();
    if (!g.first_batch_done) { g.oks_first_batch = oks; g.first_batch_done = true; }
    g.oks_before_error += oks;
    g.other_events += other;
    if (errors > 0 && !g.error_seen) {
        g.error_seen = true; g.error_sub = esub; g.error_code = ecode;
    }
    g.errors += errors;
    g.released_pumps++;
    pthread_cond_broadcast(&g.cv);
    unlock();
    return 0;
}

/* -- row ------------------------------------------------------------- */

static moq_pico_wt_managed_t *make(bool server, int port, const char *cert,
                                    const char *key, moq_result_t *rc)
{
    moq_pico_wt_managed_cfg_t cfg;
    moq_pico_wt_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.port = port;
    cfg.path = "/moq";
    cfg.wt_protocols = "moqt-18";
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 64;
    if (server) {
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.on_pump = server_pump;
    } else {
        cfg.host = "127.0.0.1";
        cfg.insecure_skip_verify = true;
        cfg.on_pump = client_pump;
    }
    moq_pico_wt_managed_t *m = NULL;
    *rc = moq_pico_wt_managed_create(&cfg, &m);
    return *rc == MOQ_OK ? m : NULL;
}

/* -- the server's port ----------------------------------------------- */

typedef struct {
    const char *row;
    const char *cert, *key;
    unsigned starts;
    unsigned listening;  /* starts that left a live server */
    bool setup_failed;   /* a start failed for a reason other than a held
                          * port: no more starts */
} server_start_t;

typedef enum { PORT_FREE, PORT_HELD, PORT_UNKNOWN } port_state_t;

/* Whether another socket holds `port`. HELD only when a bind of it (IPv4, or
 * IPv6-only) fails with EADDRINUSE; FREE when both binds succeed; UNKNOWN
 * when a probe itself fails, its cause in `why`. */
static port_state_t port_probe(uint16_t port, char *why, size_t why_len)
{
    port_state_t st = PORT_FREE;
    for (int v6 = 0; v6 < 2; v6++) {
        const char *fam = v6 ? "IPv6" : "IPv4";
        const int fd = socket(v6 ? AF_INET6 : AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) {
            snprintf(why, why_len, "%s socket: %s", fam, strerror(errno));
            if (st != PORT_HELD) st = PORT_UNKNOWN;
            continue;
        }
        int rc;
        if (v6) {
            int one = 1;
            struct sockaddr_in6 a6;
            (void)setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof(one));
            memset(&a6, 0, sizeof(a6));
            a6.sin6_family = AF_INET6;
            a6.sin6_addr = in6addr_any;
            a6.sin6_port = htons(port);
            rc = bind(fd, (const struct sockaddr *)&a6, sizeof(a6));
        } else {
            struct sockaddr_in a;
            memset(&a, 0, sizeof(a));
            a.sin_family = AF_INET;
            a.sin_addr.s_addr = htonl(INADDR_ANY);
            a.sin_port = htons(port);
            rc = bind(fd, (const struct sockaddr *)&a, sizeof(a));
        }
        const int err = errno;
        close(fd);
        if (rc == 0) continue;
        if (err == EADDRINUSE) {
            st = PORT_HELD;
            continue;
        }
        snprintf(why, why_len, "%s bind: %s", fam, strerror(err));
        if (st != PORT_HELD) st = PORT_UNKNOWN;
    }
    return st;
}

/* moq_test_listen_ipv4_port()'s start: the row's server on `port`. A live
 * server is never overwritten. A start that fails on a port observed held
 * lets the helper try a fresh one; one that fails with the port free, or not
 * probeable, ends the row's starts. */
static bool start_server(void *ctx, uint16_t port)
{
    server_start_t *s = (server_start_t *)ctx;
    if (s->setup_failed) return false;
    if (g.srv) {
        printf("%s: no start on port %u: a server is already live\n", s->row,
               (unsigned)port);
        s->setup_failed = true;
        return false;
    }
    moq_result_t rc;
    s->starts++;
    g.srv = make(true, port, s->cert, s->key, &rc);
    if (g.srv) {
        s->listening++;
        printf("%s: server listening on port %u (start %u)\n", s->row,
               (unsigned)port, s->starts);
        return true;
    }
    char why[128] = "";
    switch (port_probe(port, why, sizeof(why))) {
    case PORT_HELD:
        printf("%s: server start %u on port %u failed (%d): the port is "
               "held (EADDRINUSE), trying another\n", s->row, s->starts,
               (unsigned)port, rc);
        return false;
    case PORT_FREE:
        printf("%s: server start %u on port %u failed (%d) with the port "
               "free: a setup failure\n", s->row, s->starts, (unsigned)port, rc);
        break;
    case PORT_UNKNOWN:
        printf("%s: server start %u on port %u failed (%d) and the port "
               "could not be probed (%s): a setup failure\n", s->row,
               s->starts, (unsigned)port, rc, why);
        break;
    }
    s->setup_failed = true;
    return false;
}

static void stop_server(void)
{
    if (!g.srv) return;
    moq_pico_wt_managed_stop(g.srv);
    moq_pico_wt_managed_destroy(g.srv);
    g.srv = NULL;
}

/* The row's server and its client, on a port from the IPv4 allocator. */
static bool make_pair(const char *row, const char *cert, const char *key)
{
    server_start_t s = { .row = row, .cert = cert, .key = key };
    const uint16_t port = moq_test_listen_ipv4_port(start_server, &s);
    if (!port) {
        printf("%s: no server after %u start(s)\n", row, s.starts);
        return false;
    }
    moq_result_t rc;
    g.cli = make(false, port, cert, key, &rc);
    if (!g.cli)
        printf("%s: client for port %u failed (%d)\n", row, (unsigned)port, rc);
    return g.cli != NULL;
}

static void run_row(const char *cert, const char *key, bool force_overtake)
{
    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);
    bool ok;

    const bool made = make_pair(force_overtake ? "retained, overtaken" : "retained",
                                cert, key);
    CHECK(made && g.srv && g.cli);
    if (!made || !g.srv || !g.cli) goto out;

    /* Every request issued and answered or held. */
    lock();
    ok = wait_flag(&g.subscribed) && wait_flag(&g.held);
    for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u;
         ok && g.accepted + g.accept_failed < REJECTED && mono_us() < end;) {
        unlock(); moq_pico_wt_managed_wait(g.srv, 10000); lock();
    }
    int accepted = g.accepted, accept_failed = g.accept_failed, requests = g.requests;
    if (!ok || accepted != REJECTED)
        fprintf(stderr, "setup: subscribed=%d subscribe_failed=%d held=%d "
                "requests=%d accepted=%d accept_failed=%d; last setup pump: "
                "state=%d peer_credit=%llu action_capacity=%zu pending=%d\n",
                g.subscribed, g.subscribe_failed, g.held, g.requests, accepted,
                accept_failed, g.pre_state, (unsigned long long)g.pre_credit,
                g.pre_capacity, g.pre_pending);
    unlock();
    CHECK(ok);
    CHECK(requests == SUBSCRIBE_COUNT);       /* every request had credit */
    CHECK(accepted == REJECTED && accept_failed == 0);
    if (!ok || accepted != REJECTED || requests != SUBSCRIBE_COUNT) goto out;

    /* The client acknowledged every SUBSCRIBE_OK: its queue holds them. */
    ok = probe_reaches(g.srv, 1);
    CHECK(ok);
    if (!ok) goto out;
    ok = sample(g.cli, &g.cli_sample_req, &g.cli_sample_done);
    lock();
    bool pending_before = g.cli_pending;
    size_t idle = g.idle_capacity, cap_before = g.cli_capacity;
    unlock();
    CHECK(ok);
    CHECK(!pending_before);                   /* baseline: nothing retained */
    CHECK(cap_before == idle);

    /* The rejection, then its acknowledgement by the client. */
    lock(); g.allow_reject = true; unlock();
    moq_pico_wt_managed_wake(g.srv);
    lock();
    ok = wait_flag(&g.rejected);
    moq_result_t reject_rc = g.reject_rc;
    unlock();
    CHECK(ok && reject_rc == MOQ_OK);
    if (!ok || reject_rc != MOQ_OK) goto out;
    ok = probe_reaches(g.srv, 1);
    CHECK(ok);
    if (!ok) goto out;

    /* Retained: the client session owes output with an empty action ring. */
    ok = sample(g.cli, &g.cli_sample_req, &g.cli_sample_done);
    lock();
    bool pending_retained = g.cli_pending;
    size_t cap_retained = g.cli_capacity;
    unlock();
    CHECK(ok);
    CHECK(pending_retained);
    CHECK(cap_retained == idle);
    ok = sample(g.srv, &g.srv_sample_req, &g.srv_sample_done);
    lock(); size_t streams_held = g.srv_streams; unlock();
    CHECK(ok);

    /* Park the client's next pass after it publishes its drain state. */
    /* The arm and its wake are one step under g.mu: no pass can consume the
     * arm before the wake advances the generation, so a pass that began
     * earlier fails gen_unchanged and is not observed. Lock order: g.mu, then
     * the adapter's mutex inside wake; the network thread never holds the
     * adapter's mutex while it takes g.mu (pumps and stage 2 run without it). */
    if (force_overtake) {
        /* A pass that began before the arm: held at stage-2 entry (a wake
         * starts it); the arm and its wake land while it is in flight. */
        lock(); g.hold_next = true; unlock();
        moq_result_t hrc = moq_pico_wt_managed_wake(g.cli);
        if (hrc != MOQ_OK) { CHECK(hrc == MOQ_OK); goto out; }
    }
    lock();
    /* Seen under g.mu, the hold is active until this thread releases g.mu
     * or the hold: the hook clears pass_held only under g.mu. */
    bool active = !force_overtake || wait_flag(&g.pass_held);
    moq_result_t wrc = MOQ_ERR_INTERNAL;
    if (active) {
        g.arm = true;
        wrc = moq_pico_wt_managed_wake(g.cli);
        if (force_overtake) {
            g.hold_release = true;
            pthread_cond_broadcast(&g.cv);
        }
    }
    bool parked = active && wrc == MOQ_OK && wait_flag(&g.parked);
    moq_pwt_test_drain_facts_t f = g.facts;
    int probe = parked ? moq_pico_wt_managed_drain_state(g.cli) : -99;
    unlock();
    CHECK(active);
    CHECK(parked);
    if (parked) {
        CHECK(f.ready);
        CHECK(f.gen_unchanged);
        CHECK(f.session_pending);             /* the retained FIN */
        CHECK(!f.bridge_pending);
        CHECK(f.backlog_empty);
        CHECK(f.acked);
        CHECK(probe == 0);                    /* PENDING */
    }
    /* Still held across passes before release. */
    ok = sample(g.srv, &g.srv_sample_req, &g.srv_sample_done);
    lock(); size_t streams_still = g.srv_streams; unlock();
    CHECK(ok && streams_still == streams_held);

    /* Release: the application polls its events again. */
    lock(); g.withhold = false; g.release = true; pthread_cond_broadcast(&g.cv); unlock();
    moq_pico_wt_managed_wake(g.cli);
    lock();
    ok = wait_flag(&g.error_seen);
    moq_subscription_t esub = g.error_sub;
    moq_request_error_t ecode = g.error_code;
    int first = g.oks_first_batch, before = g.oks_before_error, other = g.other_events;
    unlock();
    CHECK(ok);
    CHECK(moq_subscription_eq(esub, g.subs[REJECTED]));
    CHECK(ecode == REJECT_CODE);
    CHECK(first == REJECTED);                 /* the OKs that held the queue */
    CHECK(before == REJECTED);
    CHECK(other == 0);
    printf("retained: %d SUBSCRIBE_OK in the first released poll, %d before the "
           "error, %d other events\n", first, before, other);
    CHECK(probe_reaches(g.cli, 1));           /* DONE */
    /* One more released pump after DONE: still exactly one error and
     * nothing else. */
    lock();
    unsigned pumps = g.released_pumps;
    unlock();
    moq_result_t wrc2 = moq_pico_wt_managed_wake(g.cli);
    lock();
    for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u;
         wrc2 == MOQ_OK && g.released_pumps == pumps && mono_us() < end;) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_nsec += 10000000;
        if (ts.tv_nsec >= 1000000000) { ts.tv_sec++; ts.tv_nsec -= 1000000000; }
        (void)pthread_cond_timedwait(&g.cv, &g.mu, &ts);
    }
    bool pumped = g.released_pumps != pumps;
    int errors = g.errors;
    other = g.other_events;
    unlock();
    CHECK(pumped);
    CHECK(errors == 1);                       /* exactly once */
    CHECK(other == 0);

    /* The client's FIN reached the server: its bridge retires the stream. */
    bool retired = false;
    size_t streams_after = streams_held;
    for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u; !retired && mono_us() < end;) {
        if (!sample(g.srv, &g.srv_sample_req, &g.srv_sample_done)) break;
        lock(); streams_after = g.srv_streams; unlock();
        retired = streams_after + 1 == streams_held;
        if (!retired) moq_pico_wt_managed_wait(g.srv, 10000);
    }
    printf("retained: server bridge streams %zu held, %zu after release\n",
           streams_held, streams_after);
    lock(); int overtaken = g.overtaken; unlock();
    if (force_overtake) {
        CHECK(overtaken >= 1);                /* the held pass was not observed */
        lock(); bool hold_ok = forced_hold_ok(); unlock();
        CHECK(hold_ok);                       /* released while active */
    }
    printf("retained: %d armed pass(es) overtaken by a wake, not observed\n", overtaken);
    CHECK(retired);
    lock(); bool timed_out = g.park_timed_out; unlock();
    CHECK(!timed_out);

out:
    lock();
    g.release = g.hold_release = true;
    pthread_cond_broadcast(&g.cv);
    unlock();
    if (g.cli) { moq_pico_wt_managed_stop(g.cli); moq_pico_wt_managed_destroy(g.cli); }
    if (g.srv) { moq_pico_wt_managed_stop(g.srv); moq_pico_wt_managed_destroy(g.srv); }
    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
}

/* -- the forced hold's timeout negative ------------------------------ */

/* A hold the test never releases ends by timeout: the hook records it, the
 * hold is no longer active, and the forced row's verdict rejects it. The
 * hold bound is the fixture's own (short) one; nothing sleeps. */
static void run_hold_timeout_negative(const char *cert, const char *key)
{
    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);
    const bool made = make_pair("forced hold timeout", cert, key);
    CHECK(made && g.srv && g.cli);
    if (!made || !g.srv || !g.cli) goto out;
    lock();
    bool hooked = wait_flag(&g.cli_hook_installed);
    g.hold_bound_ms = 50;
    g.hold_next = true;
    unlock();
    CHECK(hooked);
    if (!hooked) goto out;
    moq_result_t hrc = moq_pico_wt_managed_wake(g.cli);
    CHECK(hrc == MOQ_OK);
    lock();
    bool ended = hrc == MOQ_OK && wait_flag(&g.hold_ended);
    bool timed_out = g.hold_timed_out, still_active = g.pass_held;
    bool verdict = forced_hold_ok();
    unlock();
    CHECK(ended);
    CHECK(timed_out);                         /* recorded */
    CHECK(!still_active);                     /* not mistaken for an active hold */
    CHECK(!verdict);                          /* the forced row would reject it */

out:
    lock();
    g.release = g.hold_release = true;
    pthread_cond_broadcast(&g.cv);
    unlock();
    if (g.cli) { moq_pico_wt_managed_stop(g.cli); moq_pico_wt_managed_destroy(g.cli); }
    if (g.srv) { moq_pico_wt_managed_stop(g.srv); moq_pico_wt_managed_destroy(g.srv); }
    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
}

/* -- the port classification's controls ---------------------------- */

/* What a branch did: its starts, the starts that left a live server, whether
 * one failed for a reason other than a held port, the expectations it
 * recorded as failed, and whether a server was still live when it returned. */
typedef struct {
    unsigned starts, listening;
    bool setup_failed;
    int failed;
    bool live_after;
} branch_outcome_t;

static branch_outcome_t branch_outcome(const server_start_t *s, int failures_before)
{
    return (branch_outcome_t){ .starts = s->starts, .listening = s->listening,
                               .setup_failed = s->setup_failed,
                               .failed = failures - failures_before,
                               .live_after = g.srv != NULL };
}

/* The held branch, on `port`: a start there must fail and be classed as
 * held, and the helper's next start must listen on another port. Nothing
 * dependent runs once an expectation fails, and every server it creates is
 * stopped before it returns. */
static branch_outcome_t held_branch(const char *cert, const char *key, uint16_t port)
{
    const int before = failures;
    server_start_t s = { .row = "port control, held", .cert = cert, .key = key };
    const bool started = start_server(&s, port);
    CHECK(!started && g.srv == NULL && !s.setup_failed && s.starts == 1);
    if (started || g.srv || s.setup_failed) {
        stop_server();
        return branch_outcome(&s, before);
    }
    const uint16_t next = moq_test_listen_ipv4_port(start_server, &s);
    CHECK(next != 0 && next != port && g.srv != NULL && !s.setup_failed &&
          s.listening == 1);
    stop_server();
    return branch_outcome(&s, before);
}

/* The setup-failure branch, with `cert`: the start must fail with its port
 * free, be classed as a setup failure and end the starts after one. Any
 * server it creates is stopped before it returns. */
static branch_outcome_t setup_failure_branch(const char *cert, const char *key)
{
    const int before = failures;
    server_start_t s = { .row = "port control, missing certificate",
                         .cert = cert, .key = key };
    const uint16_t port = moq_test_listen_ipv4_port(start_server, &s);
    CHECK(port == 0 && s.setup_failed && s.starts == 1 && g.srv == NULL);
    stop_server();
    return branch_outcome(&s, before);
}

static void port_control_begin(void)
{
    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);
}

/* Every server is gone before the synchronization it uses. */
static void port_control_end(void)
{
    stop_server();
    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
}

/* With a socket holding a port on IPv4, as another application's might, a
 * server start on that port fails and is classed as held, and the helper's
 * next start lands on another port and listens. A start that fails with its
 * port free -- here a certificate that does not exist -- is classed as a
 * setup failure and ends the starts after one. */
static void run_port_controls(const char *cert, const char *key)
{
    port_control_begin();
    struct sockaddr_in a;
    socklen_t len = sizeof(a);
    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    const int fd = socket(AF_INET, SOCK_DGRAM, 0);
    const bool holding = fd >= 0 &&
        bind(fd, (const struct sockaddr *)&a, sizeof(a)) == 0 &&
        getsockname(fd, (struct sockaddr *)&a, &len) == 0;
    const uint16_t held = holding ? ntohs(a.sin_port) : 0;
    CHECK(holding && held != 0);
    if (holding) held_branch(cert, key, held);
    if (fd >= 0) close(fd);
    setup_failure_branch("/nonexistent/moq-test-cert.pem", key);
    port_control_end();
}

/* The guards' verdict on a branch given a start that unexpectedly succeeds:
 * exactly one start, and it listened; the branch recorded its one failed
 * expectation and stopped that server. A start that failed for a reason of
 * its own, a second start or a server left live is another outcome, never
 * accepted in its place. */
static bool unexpected_start_handled(const branch_outcome_t *o)
{
    return o->starts == 1 && o->listening == 1 && !o->setup_failed &&
           o->failed == 1 && !o->live_after;
}

static void print_outcome(const char *branch, const branch_outcome_t *o)
{
    printf("port guards, %s: %u start(s), %u listening, setup failure %s, "
           "%d failed expectation(s), server live after %s\n", branch,
           o->starts, o->listening, o->setup_failed ? "yes" : "no", o->failed,
           o->live_after ? "yes" : "no");
}

/* Both branches as the guards run them, the held branch on `port`, both with
 * `cert`: their outcomes, the failures they record set aside. */
static void guarded_branches(const char *cert, const char *key, uint16_t port,
                             branch_outcome_t *held, branch_outcome_t *setup)
{
    const int before = failures;
    expecting_failures = true;
    *held = held_branch(cert, key, port);
    failures = before;
    *setup = setup_failure_branch(cert, key);
    failures = before;
    expecting_failures = false;
    print_outcome("held branch", held);
    print_outcome("setup branch", setup);
}

/* The branches' own guards, against starts that unexpectedly succeed: the
 * held branch given a port nobody holds, and the setup-failure branch given
 * the real certificate. Each branch's one start must listen, and the branch
 * must record its one failed expectation, stop that server and start no
 * second one. The failures they record are this control's expected outcome,
 * so they are counted and set aside. */
static void run_port_guards(const char *cert, const char *key)
{
    port_control_begin();
    const uint16_t free_port = moq_test_ipv4_free_port();
    CHECK(free_port != 0);
    if (free_port) {
        branch_outcome_t held, setup;
        guarded_branches(cert, key, free_port, &held, &setup);
        CHECK(unexpected_start_handled(&held));
        CHECK(unexpected_start_handled(&setup));
    }
    port_control_end();
}

/* The guards' negative: both branches given a certificate that does not
 * exist, so each start fails with its port free -- a setup failure in place
 * of the unexpected start. The held branch then records one failed
 * expectation and leaves no server live, the outcome a count of failures and
 * live servers alone would accept; but nothing listened, so the guards must
 * refuse both branches. */
static void run_port_guards_setup_failure(const char *key)
{
    port_control_begin();
    const uint16_t free_port = moq_test_ipv4_free_port();
    CHECK(free_port != 0);
    if (free_port) {
        branch_outcome_t held, setup;
        guarded_branches("/nonexistent/moq-test-cert.pem", key, free_port,
                         &held, &setup);
        CHECK(held.starts == 1 && held.listening == 0 && held.setup_failed &&
              held.failed == 1 && !held.live_after);
        CHECK(setup.starts == 1 && setup.listening == 0 && setup.setup_failed &&
              setup.failed == 0 && !setup.live_after);
        CHECK(!unexpected_start_handled(&held));
        CHECK(!unexpected_start_handled(&setup));
    }
    port_control_end();
}

int main(int argc, char **argv)
{
    if (argc == 4 && !strcmp(argv[1], "--port-guards")) {
        run_port_guards(argv[2], argv[3]);
        printf("%s: pico_wt managed drain retained: an unexpected start is "
               "recorded and stopped, never overwritten\n",
               failures ? "FAIL" : "PASS");
        return failures ? 1 : 0;
    }
    if (argc == 3 && !strcmp(argv[1], "--port-guards-setup-failure")) {
        run_port_guards_setup_failure(argv[2]);
        printf("%s: pico_wt managed drain retained: the guards refuse a setup "
               "failure in place of an unexpected start\n",
               failures ? "FAIL" : "PASS");
        return failures ? 1 : 0;
    }
    if (argc == 4 && !strcmp(argv[1], "--port-control")) {
        run_port_controls(argv[2], argv[3]);
        printf("%s: pico_wt managed drain retained: a held port is left for "
               "a fresh one, a setup failure ends the starts\n",
               failures ? "FAIL" : "PASS");
        return failures ? 1 : 0;
    }
    if (argc < 3) {
        printf("SKIP: pico_wt managed drain retained (no cert/key)\n");
        return 0;
    }
    int before = failures;
    run_row(argv[1], argv[2], false);
    printf("%s: pico_wt managed drain holds for retained output\n",
           failures == before ? "PASS" : "FAIL");
    before = failures;
    run_row(argv[1], argv[2], true);
    printf("%s: pico_wt managed drain holds for retained output, a pass in flight at arming is not observed\n",
           failures == before ? "PASS" : "FAIL");
    before = failures;
    run_hold_timeout_negative(argv[1], argv[2]);
    printf("%s: pico_wt managed forced hold: a timed-out hold is recorded and rejected\n",
           failures == before ? "PASS" : "FAIL");
    return failures ? 1 : 0;
}
