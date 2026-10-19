/*
 * The threaded adapter's drain holds while the session retains reliable
 * lifecycle output, over real draft-18 loopback at the adapter's ordinary
 * capacities -- the same row as the managed WebTransport facade's.
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
 * exact SUBSCRIBE_ERROR, its drain reaches DONE, and the server's QUIC
 * connection receives the client's FIN on exactly one more request stream.
 *
 * White-box: the stage hook, drain facts and the server's unfinished-stream
 * count are MOQ_PQ_THREADED_TESTING seams (test-internals build), read on the
 * owner network thread. Preparation failures fail the row and unwind; every
 * wait is bounded by a monotonic deadline.
 */
#include <moq/picoquic_threaded.h>
#include <moq/session.h>

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    size_t connections;
    bool   ready, session_pending, bridge_pending, backlog_empty, acked, gen_unchanged;
} moq_pq_threaded_test_drain_facts_t;
extern void moq_pq_threaded_test_set_stage_hook(
    moq_pq_threaded_t *t, void (*fn)(moq_pq_threaded_t *, int, void *), void *ctx);
extern void moq_pq_threaded_test_drain_facts(moq_pq_threaded_t *t,
                                             moq_pq_threaded_test_drain_facts_t *out);
typedef struct {
    size_t open, fin, reset;
} moq_pq_threaded_test_bidi_halves_t;
extern void moq_pq_threaded_test_bidi_halves(moq_pq_threaded_t *t, bool peer_initiated,
                                             moq_pq_threaded_test_bidi_halves_t *out);

static int failures = 0;
#define CHECK(expr)                                                          \
    do {                                                                     \
        if (!(expr)) {                                                       \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);  \
            failures++;                                                      \
        }                                                                    \
    } while (0)

enum { BOUND_MS = 5000, SUBSCRIBE_COUNT = 17, REJECTED = SUBSCRIBE_COUNT - 1 };
#define REJECT_CODE MOQ_REQUEST_ERROR_UNAUTHORIZED

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    moq_pq_threaded_t *srv, *cli;

    /* server pump */
    int                accepted, accept_failed, requests;
    bool               held;            /* the request it will reject */
    moq_subscription_t held_sub;
    bool               allow_reject, rejected;
    moq_result_t       reject_rc;
    unsigned           srv_sample_req, srv_sample_done;
    moq_pq_threaded_test_bidi_halves_t srv_halves; /* client request halves */

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
    moq_pq_threaded_test_drain_facts_t facts;
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
static bool probe_reaches(moq_pq_threaded_t *m, int want)
{
    uint64_t start = mono_us(), deadline = start + (uint64_t)BOUND_MS * 1000u;
    uint64_t next_wake = start + 1000000u;
    moq_pq_threaded_wake(m);
    for (;;) {
        if (moq_pq_threaded_drain_state(m) == want) return true;
        uint64_t now = mono_us();
        if (now >= deadline) return false;
        if (now >= next_wake) {
            moq_pq_threaded_wake(m);
            next_wake = now + 1000000u;
        }
        uint64_t slice = deadline - now < 10000u ? deadline - now : 10000u;
        moq_result_t rc = moq_pq_threaded_wait(m, slice);
        if (rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_INTERNAL) return false;
    }
}

/* A sample taken by the owner's next pump: request one, wake, wait for it. */
static bool sample(moq_pq_threaded_t *m, unsigned *req, unsigned *done)
{
    lock();
    unsigned want = ++*req;
    unlock();
    moq_pq_threaded_wake(m);
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

static void server_session(moq_session_t *s, uint64_t now)
{
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
    unlock();
    if (reject) {
        moq_reject_subscribe_cfg_t rc;
        moq_reject_subscribe_cfg_init(&rc);
        rc.error_code = REJECT_CODE;
        moq_result_t r = moq_session_reject_subscribe(s, held, &rc, now);
        lock(); g.reject_rc = r; g.rejected = true; pthread_cond_broadcast(&g.cv); unlock();
    }
}

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *ctx)
{
    (void)ctx;
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, NULL); c;
         c = moq_pq_threaded_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_pq_threaded_conn_session(c);
        if (s) server_session(s, now);
    }
    lock();
    unsigned req = g.srv_sample_req;
    unlock();
    if (req != g.srv_sample_done) {
        moq_pq_threaded_test_bidi_halves_t h;
        moq_pq_threaded_test_bidi_halves(t, true, &h);
        lock(); g.srv_halves = h; g.srv_sample_done = req; pthread_cond_broadcast(&g.cv); unlock();
    }
    return 0;
}

/* -- client ---------------------------------------------------------- */

static void client_stage(moq_pq_threaded_t *m, int stage, void *ctx)
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
    moq_pq_threaded_test_drain_facts_t f;
    moq_pq_threaded_test_drain_facts(m, &f);
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

static int client_pump(moq_pq_threaded_t *m, moq_pq_threaded_lane_t *lane,
                       uint64_t now, void *ctx)
{
    (void)ctx; (void)lane;
    lock();
    bool install = !g.cli_hook_installed;
    unlock();
    if (install) {
        moq_pq_threaded_test_set_stage_hook(m, client_stage, NULL);
        lock(); g.cli_hook_installed = true; pthread_cond_broadcast(&g.cv); unlock();
    }
    moq_session_t *s = moq_pq_threaded_session(m);
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

static const char *const k_d18[] = { "moqt-18" };

static moq_pq_threaded_t *make(bool server, int port, const char *cert, const char *key)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.port = port;
    cfg.alpn_list = k_d18;
    cfg.alpn_count = 1;
    if (server) {
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.on_lane_pump = server_pump;
    } else {
        cfg.host = "localhost";
        cfg.insecure_skip_verify = true;
        cfg.on_lane_pump = client_pump;
    }
    moq_pq_threaded_t *t = NULL;
    return moq_pq_threaded_create(&cfg, &t) == MOQ_OK ? t : NULL;
}

static void run_row(int port, const char *cert, const char *key, bool force_overtake)
{
    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);
    bool ok;

    g.srv = make(true, port, cert, key);
    g.cli = g.srv ? make(false, port, cert, key) : NULL;
    CHECK(g.srv && g.cli);
    if (!g.srv || !g.cli) goto out;

    /* Every request issued and answered or held. */
    lock();
    ok = wait_flag(&g.subscribed) && wait_flag(&g.held);
    for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u;
         ok && g.accepted + g.accept_failed < REJECTED && mono_us() < end;) {
        unlock(); moq_pq_threaded_wait(g.srv, 10000); lock();
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
    moq_pq_threaded_wake(g.srv);
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
    lock(); moq_pq_threaded_test_bidi_halves_t held_h = g.srv_halves; unlock();
    /* Every client request half is open while the FIN is retained. */
    CHECK(held_h.open == SUBSCRIBE_COUNT && held_h.fin == 0 && held_h.reset == 0);
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
        moq_result_t hrc = moq_pq_threaded_wake(g.cli);
        if (hrc != MOQ_OK) { CHECK(hrc == MOQ_OK); goto out; }
    }
    lock();
    /* Seen under g.mu, the hold is active until this thread releases g.mu
     * or the hold: the hook clears pass_held only under g.mu. */
    bool active = !force_overtake || wait_flag(&g.pass_held);
    moq_result_t wrc = MOQ_ERR_INTERNAL;
    if (active) {
        g.arm = true;
        wrc = moq_pq_threaded_wake(g.cli);
        if (force_overtake) {
            g.hold_release = true;
            pthread_cond_broadcast(&g.cv);
        }
    }
    bool parked = active && wrc == MOQ_OK && wait_flag(&g.parked);
    moq_pq_threaded_test_drain_facts_t f = g.facts;
    int probe = parked ? moq_pq_threaded_drain_state(g.cli) : -99;
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
    lock(); moq_pq_threaded_test_bidi_halves_t still_h = g.srv_halves; unlock();
    CHECK(ok && still_h.open == held_h.open && still_h.fin == 0 && still_h.reset == 0);

    /* Release: the application polls its events again. */
    lock(); g.withhold = false; g.release = true; pthread_cond_broadcast(&g.cv); unlock();
    moq_pq_threaded_wake(g.cli);
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
    moq_result_t wrc2 = moq_pq_threaded_wake(g.cli);
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

    /* Exactly one formerly open client request half is retired at the
     * server, and the halves still sampled show no RESET. If picoquic has
     * already deleted that stream, its FIN/RESET flags are gone from this
     * snapshot, so the snapshot cannot tell how that half terminated: the
     * row asserts retirement, not a graceful-FIN trace. */
    bool retired = false;
    moq_pq_threaded_test_bidi_halves_t after_h = held_h;
    for (uint64_t end = mono_us() + (uint64_t)BOUND_MS * 1000u; !retired && mono_us() < end;) {
        if (!sample(g.srv, &g.srv_sample_req, &g.srv_sample_done)) break;
        lock(); after_h = g.srv_halves; unlock();
        retired = after_h.open + 1 == held_h.open;
        if (!retired) moq_pq_threaded_wait(g.srv, 10000);
    }
    size_t total = after_h.open + after_h.fin + after_h.reset;
    CHECK(total <= held_h.open);              /* no new client request stream */
    size_t gone = total <= held_h.open ? held_h.open - total : 0;
    printf("retained: server view of client request halves: open %zu held; after "
           "release open=%zu fin=%zu reset=%zu deleted=%zu\n", held_h.open,
           after_h.open, after_h.fin, after_h.reset, gone);
    CHECK(after_h.reset == 0);
    CHECK(after_h.fin + gone == 1);
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
    if (g.cli) { moq_pq_threaded_stop(g.cli); moq_pq_threaded_destroy(g.cli); }
    if (g.srv) { moq_pq_threaded_stop(g.srv); moq_pq_threaded_destroy(g.srv); }
    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
}

/* -- the forced hold's timeout negative ------------------------------ */

/* A hold the test never releases ends by timeout: the hook records it, the
 * hold is no longer active, and the forced row's verdict rejects it. The
 * hold bound is the fixture's own (short) one; nothing sleeps. */
static void run_hold_timeout_negative(int port, const char *cert, const char *key)
{
    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);
    g.srv = make(true, port, cert, key);
    g.cli = g.srv ? make(false, port, cert, key) : NULL;
    CHECK(g.srv && g.cli);
    if (!g.srv || !g.cli) goto out;
    lock();
    bool hooked = wait_flag(&g.cli_hook_installed);
    g.hold_bound_ms = 50;
    g.hold_next = true;
    unlock();
    CHECK(hooked);
    if (!hooked) goto out;
    moq_result_t hrc = moq_pq_threaded_wake(g.cli);
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
    if (g.cli) { moq_pq_threaded_stop(g.cli); moq_pq_threaded_destroy(g.cli); }
    if (g.srv) { moq_pq_threaded_stop(g.srv); moq_pq_threaded_destroy(g.srv); }
    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        printf("SKIP: threaded drain retained (no cert/key)\n");
        return 0;
    }
    srand((unsigned)getpid());
    int port = 17700 + (rand() % 400);
    int before = failures;
    run_row(port, argv[1], argv[2], false);
    printf("%s: threaded drain holds for retained output\n",
           failures == before ? "PASS" : "FAIL");
    before = failures;
    run_row(port + 5, argv[1], argv[2], true);
    printf("%s: threaded drain holds for retained output, a pass in flight at arming is not observed\n",
           failures == before ? "PASS" : "FAIL");
    before = failures;
    run_hold_timeout_negative(port + 10, argv[1], argv[2]);
    printf("%s: threaded forced hold: a timed-out hold is recorded and rejected\n",
           failures == before ? "PASS" : "FAIL");
    return failures ? 1 : 0;
}
