/*
 * The threaded adapter's drain gate on session pending output, observed at a
 * parked pass boundary (MOQ_PQ_THREADED_TESTING stage hooks; never shipped).
 *
 * One row per changed publication path: the client's single-connection pass
 * (publisher = client) and the server's per-connection aggregate (publisher
 * = server). In each row:
 *   1. the publishing side reaches drain DONE (the baseline: control traffic
 *      flushed and acknowledged, so nothing but the observer can hold it);
 *   2. its pump admits, on the network thread, more datagram objects than one
 *      pass drains (the bridge services at most 256 actions per call, and the
 *      pass is service -> pump -> service, so only the post-pump service
 *      meets this batch);
 *   3. at stage 2 -- the drain snapshot just published, no facade lock held --
 *      the network thread records the drain inputs and parks; the row asserts
 *      the causal isolation: session output pending, bridge outbound FIFO
 *      empty, no stream backlog, every send half acknowledged, ready, one
 *      connection, no wake since the pass began; the public drain probe, read
 *      on the test thread while parked, must be PENDING;
 *   4. released, later passes drain the rest and the probe reaches DONE.
 * The old-pass rows (one per site) pin the publication's generation check:
 * a pass is held at stage 1 -- its pump over -- while the test queues the
 * batch and wakes; released, that SAME pass is parked at stage 2 after it
 * published. Its post-service drain inputs are all quiet (the batch never
 * reached its pump), yet a wake overtook it: the public probe must read
 * PENDING. Released, a later pass admits and drains the batch.
 * The hook is installed from the pump (the owner executor), never from the
 * test thread. Parking is bounded; every failure path releases the network
 * thread before stop/destroy.
 *
 * Datagrams are an observer-integration workload on the adapter's existing
 * public datagram path (the threaded helper negotiates QUIC DATAGRAM by
 * default). They prove the gate, not any datagram delivery or acknowledgement.
 */
#include <moq/picoquic_threaded.h>
#include <moq/rcbuf.h>

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* MOQ_PQ_THREADED_TESTING seams (deliberately not in the public header). */
typedef struct {
    size_t connections;
    bool   ready, session_pending, bridge_pending, backlog_empty, acked, gen_unchanged;
} moq_pq_threaded_test_drain_facts_t;
extern void moq_pq_threaded_test_set_stage_hook(
    moq_pq_threaded_t *t, void (*fn)(moq_pq_threaded_t *, int, void *), void *ctx);
extern void moq_pq_threaded_test_drain_facts(moq_pq_threaded_t *t,
                                             moq_pq_threaded_test_drain_facts_t *out);

static int failures = 0;
#define CHECK(expr)                                                          \
    do {                                                                     \
        if (!(expr)) {                                                       \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);  \
            failures++;                                                      \
        }                                                                    \
    } while (0)

#ifdef MOQ_TEST_CERT_PATH

enum { BATCH = 600, BOUND_MS = 5000, MAX_ACTIONS = 2048 };

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    bool               publisher_is_server;
    moq_pq_threaded_t *publisher;          /* whose stage hook observes */
    bool               hook_installed;     /* by the publisher's pump */
    /* subscription plumbing (network threads) */
    bool               subscribed;
    bool               established;
    moq_subscription_t pub_sub;
    /* the batch */
    bool               arm;                /* the test asks for the batch */
    bool               admitted_this_pass; /* pump -> stage 2 of that pass */
    int                admitted;
    int                batches;         /* batches admitted, for group ids */
    int                overtaken;       /* admitting passes a wake overtook */
    int                short_batches;   /* refused for room: not a whole batch */
    /* the parked observation */
    bool               parked, release, observed;
    bool               park_timed_out;     /* the hook gave up waiting */
    moq_pq_threaded_test_drain_facts_t facts;
    /* the old pass: held at stage 1, then parked at stage 2 of that pass */
    bool               hold1_next, held1, hold1_release, hold1_timed_out;
    bool               old_at_stage2, parked2, park2_release, park2_timed_out;
    moq_pq_threaded_test_drain_facts_t facts2;
} row_t;

static row_t g;

static void lock(void) { pthread_mutex_lock(&g.mu); }
static void unlock(void) { pthread_mutex_unlock(&g.mu); }

/* Wait (bounded) for *flag to become true; g.mu held. */
static bool wait_flag(bool *flag)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += BOUND_MS / 1000;
    int rc = 0;
    while (!*flag && rc == 0) rc = pthread_cond_timedwait(&g.cv, &g.mu, &ts);
    return *flag;
}

static void stage_hook(moq_pq_threaded_t *t, int stage, void *ctx)
{
    (void)ctx;
    if (stage == 1) {
        lock();
        if (g.hold1_next) {
            g.hold1_next = false;
            g.held1 = true;
            pthread_cond_broadcast(&g.cv);
            if (!wait_flag(&g.hold1_release)) g.hold1_timed_out = true;
            g.held1 = false;
            g.old_at_stage2 = !g.hold1_timed_out;
            pthread_cond_broadcast(&g.cv);
        }
        unlock();
        return;
    }
    if (stage != 2) return;
    lock();
    bool old = g.old_at_stage2;
    g.old_at_stage2 = false;
    unlock();
    if (old) {
        /* Exactly this pass, whatever it published: no skip. */
        moq_pq_threaded_test_drain_facts_t of;
        moq_pq_threaded_test_drain_facts(t, &of);
        lock();
        g.facts2 = of;
        g.parked2 = true;
        pthread_cond_broadcast(&g.cv);
        if (!wait_flag(&g.park2_release)) g.park2_timed_out = true;
        g.parked2 = false;
        pthread_cond_broadcast(&g.cv);
        unlock();
        return;
    }
    lock();
    bool mine = g.admitted_this_pass && !g.observed;
    unlock();
    if (!mine) return;
    moq_pq_threaded_test_drain_facts_t f;
    moq_pq_threaded_test_drain_facts(t, &f);   /* network thread, in the window */
    /* Observe only a pass no wake overtook. A pass already running when the
     * test armed is overtaken by the arming wake; the wake still pending
     * guarantees another pass, which admits a fresh batch. */
    if (!f.gen_unchanged) {
        lock();
        g.overtaken++;
        g.admitted = 0;
        g.admitted_this_pass = false;
        unlock();
        return;
    }
    lock();
    g.facts = f;
    g.observed = true;
    g.parked = true;
    pthread_cond_broadcast(&g.cv);
    if (!wait_flag(&g.release))                 /* bounded: never hangs the pass */
        g.park_timed_out = true;                /* a fixture failure, reported */
    g.parked = false;
    pthread_cond_broadcast(&g.cv);
    unlock();
}

static void handle_events(moq_session_t *s, bool publisher)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) == 1) {
        if (publisher && ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            moq_subscription_t sub = ev.u.subscribe_request.sub;
            moq_accept_subscribe_cfg_t ac;
            moq_accept_subscribe_cfg_init(&ac);
            if (moq_session_accept_subscribe(s, sub, &ac, 0) == MOQ_OK) {
                lock();
                g.pub_sub = sub;
                g.established = true;
                pthread_cond_broadcast(&g.cv);
                unlock();
            }
        }
        moq_event_cleanup(&ev);
    }
}

static void subscribe_once(moq_session_t *s, uint64_t now)
{
    if (g.subscribed || moq_session_state(s) != MOQ_SESS_ESTABLISHED) return;
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("gate") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ parts, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sub;
    if (moq_session_subscribe(s, &sc, now, &sub) == MOQ_OK) g.subscribed = true;
}

/* On the publisher's pump: the whole batch, in this pass. A batch the
 * session has no room for in full -- an overtaken pass's batch still being
 * drained ahead of it -- is not one: it is left to drain, uncounted, and a
 * later pass admits a whole batch. */
static void admit_batch(moq_session_t *s, uint64_t now)
{
    lock();
    bool go = g.arm && g.established && g.admitted == 0;
    moq_subscription_t sub = g.pub_sub;
    uint64_t base = 1 + (uint64_t)g.batches * BATCH;
    unlock();
    if (!go) return;
    static const uint8_t byte = 7;
    int n = 0;
    moq_result_t rc = MOQ_OK;
    for (int i = 0; i < BATCH; i++) {
        moq_rcbuf_t *b = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), &byte, 1, &b) < 0) break;
        rc = moq_session_send_object_datagram(
            s, sub, base + (uint64_t)i, 0, 128, true, b, NULL, 0, now);
        moq_rcbuf_decref(b);
        if (rc != MOQ_OK) break;
        n++;
    }
    lock();
    g.batches++;
    if (n < BATCH && rc == MOQ_ERR_BUFFER) {
        g.short_batches++;
    } else {
        g.admitted = n;
        g.admitted_this_pass = true;
    }
    unlock();
}

/* On a pump (the owner executor): install the hook on the publisher once.
 * The publisher is published by the test thread after both facades exist,
 * so it is read under the fixture mutex, as is the installed flag. */
static void install_hook_once(moq_pq_threaded_t *t)
{
    lock();
    bool mine = t == g.publisher && !g.hook_installed;
    unlock();
    if (!mine) return;
    moq_pq_threaded_test_set_stage_hook(t, stage_hook, NULL);
    lock();
    g.hook_installed = true;
    pthread_cond_broadcast(&g.cv);
    unlock();
}

static int server_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane, uint64_t now, void *ctx)
{
    (void)ctx;
    install_hook_once(t);
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, NULL); c;
         c = moq_pq_threaded_lane_next_conn(lane, c)) {
        moq_session_t *s = moq_pq_threaded_conn_session(c);
        if (!s) continue;
        if (g.publisher_is_server) {
            handle_events(s, true);
            admit_batch(s, now);
        } else {
            subscribe_once(s, now);
            handle_events(s, false);
        }
    }
    return 0;
}

static int client_pump(moq_pq_threaded_t *t, moq_pq_threaded_lane_t *lane, uint64_t now, void *ctx)
{
    (void)lane; (void)ctx;
    install_hook_once(t);
    moq_session_t *s = moq_pq_threaded_session(t);
    if (!s) return 0;
    if (g.publisher_is_server) {
        subscribe_once(s, now);
        handle_events(s, false);
    } else {
        handle_events(s, true);
        admit_batch(s, now);
    }
    return 0;
}

static moq_pq_threaded_t *make(bool server, int port)
{
    moq_pq_threaded_cfg_t cfg;
    moq_pq_threaded_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg.port = port;
    cfg.max_actions = MAX_ACTIONS;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    if (server) {
        cfg.cert_path = MOQ_TEST_CERT_PATH;
        cfg.key_path = MOQ_TEST_KEY_PATH;
        cfg.on_lane_pump = server_pump;
    } else {
        cfg.host = "localhost";
        cfg.insecure_skip_verify = true;
        cfg.on_lane_pump = client_pump;
    }
    moq_pq_threaded_t *t = NULL;
    return moq_pq_threaded_create(&cfg, &t) == MOQ_OK ? t : NULL;
}

static uint64_t mono_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

/* The publisher's drain probe reaches `want` (1 DONE, 0 PENDING) within a
 * BOUND_MS monotonic deadline. A wake invalidates the published snapshot
 * until a later pass republishes it, so waking before every read can keep
 * the probe at PENDING indefinitely: wake once, read as passes complete,
 * and wake again only when a monotonic second has passed since the last
 * wake (an idle loop). wait() can return at once on activity, so time is
 * measured, not counted. A closed or failed facade ends the wait. */
static bool probe_reaches(moq_pq_threaded_t *t, int want)
{
    uint64_t start = mono_us(), deadline = start + (uint64_t)BOUND_MS * 1000u;
    uint64_t next_wake = start + 1000000u;
    moq_pq_threaded_wake(t);
    for (;;) {
        if (moq_pq_threaded_drain_state(t) == want) return true;
        uint64_t now = mono_us();
        if (now >= deadline) return false;
        if (now >= next_wake) {
            moq_pq_threaded_wake(t);
            next_wake = now + 1000000u;
        }
        uint64_t slice = deadline - now < 10000u ? deadline - now : 10000u;
        moq_result_t rc = moq_pq_threaded_wait(t, slice);
        if (rc == MOQ_ERR_CLOSED || rc == MOQ_ERR_INTERNAL) return false;
    }
}

static void run_row(bool publisher_is_server, int port, bool old_pass)
{
    int before = failures;
    memset(&g, 0, sizeof(g));
    pthread_mutex_init(&g.mu, NULL);
    pthread_cond_init(&g.cv, NULL);
    g.publisher_is_server = publisher_is_server;
    moq_pq_threaded_t *srv = make(true, port);
    moq_pq_threaded_t *cli = srv ? make(false, port) : NULL;
    CHECK(srv && cli);
    if (!srv || !cli) goto out;
    lock();
    g.publisher = publisher_is_server ? srv : cli;
    unlock();

    /* The subscription, then the baseline: drain DONE before the batch. */
    lock();
    bool est = wait_flag(&g.established);
    unlock();
    CHECK(est);
    if (!est) goto out;
    lock();
    bool hooked = wait_flag(&g.hook_installed);
    unlock();
    CHECK(hooked);
    /* The baseline is a prerequisite: without it the row proves nothing. */
    bool baseline = hooked && probe_reaches(g.publisher, 1);
    CHECK(baseline);
    if (!baseline) goto out;

    if (old_pass) {
        /* Hold the next pass after its pump (request and wake under g.mu),
         * queue the batch and wake while it is held, then park that same pass
         * after it publishes. The ordinary batch observation stays off. */
        lock();
        g.observed = true;
        g.hold1_next = true;
        moq_result_t hrc = moq_pq_threaded_wake(g.publisher);
        bool held1 = hrc == MOQ_OK && wait_flag(&g.held1);
        moq_result_t arc = MOQ_ERR_INTERNAL;
        if (held1) {
            g.arm = true;                          /* the pump is over */
            arc = moq_pq_threaded_wake(g.publisher);
            g.hold1_release = true;
            pthread_cond_broadcast(&g.cv);
        }
        bool parked2 = held1 && arc == MOQ_OK && wait_flag(&g.parked2);
        moq_pq_threaded_test_drain_facts_t of = g.facts2;
        int admitted_old = g.admitted;
        int probe2 = parked2 ? moq_pq_threaded_drain_state(g.publisher) : -99;
        unlock();
        CHECK(held1);
        CHECK(parked2);
        if (parked2) {
            CHECK(admitted_old == 0);             /* the batch missed this pump */
            CHECK(of.connections == 1);
            CHECK(of.ready);
            CHECK(!of.session_pending);
            CHECK(!of.bridge_pending);
            CHECK(of.backlog_empty);
            CHECK(of.acked);
            CHECK(!of.gen_unchanged);             /* overtaken by the wake */
            CHECK(probe2 == 0);                   /* PENDING, not a stale DONE */
        }
        lock();
        g.park2_release = true;
        pthread_cond_broadcast(&g.cv);
        unlock();
        CHECK(probe_reaches(g.publisher, 1));    /* a later pass drains it */
        lock();
        int admitted_later = g.admitted;
        bool to = g.hold1_timed_out || g.park2_timed_out;
        unlock();
        CHECK(admitted_later == BATCH);
        CHECK(!to);
        goto out;
    }

    /* The batch, admitted by the publisher's pump: one wake asks for a pass;
     * nothing else wakes the publisher until the observation is read. The
     * arm and its wake are one step under g.mu: no pass can consume the arm
     * before the wake advances the generation, so a pass that began earlier
     * fails gen_unchanged and is not observed. Lock order: g.mu, then the
     * adapter's mutex inside wake; the network thread never holds the
     * adapter's mutex while it takes g.mu (pumps and stage 2 run without it). */
    lock();
    g.arm = true;
    moq_result_t wrc = moq_pq_threaded_wake(g.publisher);
    /* Held from the parked check through the probe read: the hook waits on
     * g.cv, so it cannot resume (and its pass cannot republish) until this
     * thread releases g.mu. The hook holds no adapter mutex, so the probe's
     * own locking does not deadlock. */
    bool parked = wrc == MOQ_OK && wait_flag(&g.parked);
    moq_pq_threaded_test_drain_facts_t f = g.facts;
    int admitted = g.admitted;
    int probe = parked ? moq_pq_threaded_drain_state(g.publisher) : -99;
    unlock();
    CHECK(parked);
    if (parked) {
        CHECK(admitted == BATCH);
        CHECK(f.connections == 1);
        CHECK(f.ready);
        CHECK(f.gen_unchanged);       /* no wake overtook this pass */
        CHECK(!f.bridge_pending);     /* the bridge holds nothing outbound */
        CHECK(f.backlog_empty);       /* no stream backlog */
        CHECK(f.acked);               /* every send half acknowledged */
        CHECK(f.session_pending);     /* only the observer is left */
        CHECK(probe == 0);            /* ... and it holds the drain */
    }
    lock();
    g.release = true;
    pthread_cond_broadcast(&g.cv);
    unlock();
    /* Released: later passes drain the rest of the batch. */
    CHECK(probe_reaches(g.publisher, 1));
    lock();
    bool timed_out = g.park_timed_out;
    unlock();
    CHECK(!timed_out);                 /* the park ended by release, not timeout */

out:
    lock();
    g.release = g.hold1_release = g.park2_release = true;   /* never leave a pass held */
    pthread_cond_broadcast(&g.cv);
    unlock();
    if (cli) { moq_pq_threaded_stop(cli); moq_pq_threaded_destroy(cli); }
    if (srv) { moq_pq_threaded_stop(srv); moq_pq_threaded_destroy(srv); }
    pthread_cond_destroy(&g.cv);
    pthread_mutex_destroy(&g.mu);
    printf("%s: threaded drain %s, %s site\n", failures == before ? "PASS" : "FAIL",
           old_pass ? "old-pass publication" : "gate",
           publisher_is_server ? "server aggregate" : "client single-connection");
}

int main(void)
{
    srand((unsigned)getpid());
    int port = 16200 + (rand() % 400);
    run_row(false, port, false);
    run_row(true, port + 7, false);
    run_row(false, port + 14, true);
    run_row(true, port + 21, true);
    return failures ? 1 : 0;
}

#else

int main(void)
{
    printf("SKIP: threaded drain gates (no MOQ_TEST_CERT_PATH)\n");
    return 0;
}

#endif
