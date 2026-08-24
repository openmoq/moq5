/*
 * Doorbell scheduling against a REAL session event backlog.
 *
 * The managed doorbell re-drives a lane on its own -- with no further
 * external wake -- exactly while the application is making progress
 * through its queued events, and it stops the moment progress stops. Both
 * halves are asserted here, over a real child session with real queued
 * events, because either one alone is satisfiable by a wrong scheduler:
 *
 *   bounded drain -- more events than one callback will take, ONE external
 *     wake, a callback that polls a declared few per generation. The event
 *     progress token advances, events remain, and the doorbell must invoke
 *     further generations by itself until the backlog is gone;
 *
 *   non-drain -- the same backlog, a callback that polls NOTHING. The token
 *     cannot advance, so one external wake owes exactly one pump generation
 *     and the lane must then park with the backlog still present.
 *
 * A spinning build is stopped INSIDE the callback: past its allowance the
 * pump returns nonzero, the documented pump-exit path, which latches
 * lane->pump_exit and m->pump_exit and suppresses the re-arm. So a runaway
 * is observed rather than hung, and when the guard fires the terminal
 * consequences are asserted too. stop() is never called from a callback.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <stdatomic.h>

#include "msq_child_pair.h"

static int failures = 0;

/* Lock-free mirror of the spin probe, so a waiter can be released the
 * instant the probe fires instead of sitting out the barrier's hang guard.
 * The probe sets it and then broadcasts the barrier condvar; nothing here
 * takes both mutexes, so there is no ordering hazard. */
static atomic_bool g_excess_flag;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

extern void moq_msq_test_bell_ring_raw(moq_msquic_managed_lane_t *lane);

enum {
    OBJ_LEN = 64,
    /* enough that a poll-few callback cannot finish in one generation */
    N_OBJECTS = 8,
    /* how many events one generation may take in the bounded-drain row */
    POLL_PER_PUMP = 2,
    /* generations a correct build may take before the probe calls it a
     * spin and terminates the lane */
    PUMP_ALLOWANCE = 64,

    /*
     * The bounded row's MEASURED drain phase carries a finite, DECLARED
     * event inventory, which is what makes its pump-generation count
     * exact rather than a calibration.
     *
     * The peer opens ONE subgroup, writes N_OBJECTS objects on it, and
     * closes it. So the child's queue at the single external wake holds:
     *
     *   - N_OBJECTS x MOQ_EVENT_OBJECT_RECEIVED. The pair is built with
     *     streaming_objects = false, so whole objects are delivered and
     *     no MOQ_EVENT_OBJECT_CHUNK can appear;
     *   - exactly one MOQ_EVENT_SUBGROUP_FINISHED. rx_finish_stream()
     *     (core/src/session/session_receive.c:272) emits it once per
     *     subgroup stream that FINs with a resolved subgroup ID on a
     *     bound subscription; the objects resolve the ID and
     *     moq_session_close_subgroup() supplies the FIN, so one opened
     *     and closed subgroup owes exactly one.
     *
     * SETUP_COMPLETE and SUBSCRIBE_OK are consumed during fill_backlog(),
     * before the measured interval, and the row asserts that.
     */
    SUBGROUPS_CLOSED = 1,
    SUBGROUP_FINISHED_OWED = SUBGROUPS_CLOSED,
    MEASURED_EVENTS = N_OBJECTS + SUBGROUP_FINISHED_OWED,
    /* ceil(MEASURED_EVENTS / POLL_PER_PUMP), integer-only and with no
     * overflow risk at these magnitudes: each generation takes at most
     * POLL_PER_PUMP events, and a correct build re-arms only while the
     * token moved AND events remain -- so the final partial generation
     * empties the queue and nothing re-arms after it. */
    EXPECTED_PUMP_DELTA =
        (MEASURED_EVENTS + POLL_PER_PUMP - 1) / POLL_PER_PUMP,
};

/* --- what the child's pump does, and what it records ------------------- */

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;

    unsigned poll_limit;   /* events this callback may take per generation */
    bool subscribe;        /* drive the subscription from the pump */
    bool subscribed;

    uint64_t pumps;        /* on_lane_pump entries */
    uint64_t limit;        /* entries permitted before the probe fires */
    bool excess;           /* the probe fired: this build re-armed forever */
    unsigned nonzero_returns; /* callbacks that returned pump-exit */

    int setup;
    int sub_ok;
    int objects;           /* OBJECT_RECEIVED drained so far */
    int sub_finished;      /* SUBGROUP_FINISHED drained so far */
    int unexpected_events; /* any kind this fixture does not declare */
    unsigned first_unexpected_kind;
    int object_errors;
    uint64_t seen_mask;    /* object ids drained, for exactly-once */
    uint64_t next_expected;/* arrival order */
    int order_errors;
} g_pump = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
    .limit = UINT64_MAX,
};

static void pump_reset(unsigned poll_limit, bool subscribe)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.poll_limit = poll_limit;
    g_pump.subscribe = subscribe;
    g_pump.subscribed = false;
    g_pump.pumps = 0;
    g_pump.limit = UINT64_MAX;
    g_pump.excess = false;
    atomic_store(&g_excess_flag, false);
    g_pump.nonzero_returns = 0;
    g_pump.setup = 0;
    g_pump.sub_ok = 0;
    g_pump.objects = 0;
    g_pump.sub_finished = 0;
    g_pump.unexpected_events = 0;
    g_pump.first_unexpected_kind = 0;
    g_pump.object_errors = 0;
    g_pump.seen_mask = 0;
    g_pump.next_expected = 0;
    g_pump.order_errors = 0;
    pthread_mutex_unlock(&g_pump.mu);
}

static int counting_pump(moq_msquic_managed_t *m,
                         moq_msquic_managed_lane_t *lane, uint64_t now_us,
                         void *user)
{
    moq_session_t *s = moq_msquic_managed_session(m);
    moq_event_t ev;
    bool stop_now = false;
    unsigned taken = 0;

    (void)lane;
    (void)user;
    pthread_mutex_lock(&g_pump.mu);
    g_pump.pumps++;
    if (g_pump.pumps > g_pump.limit && !g_pump.excess) {
        g_pump.excess = true;
        g_pump.nonzero_returns++;
        stop_now = true;
    }
    if (s != NULL && !stop_now) {
        while (taken < g_pump.poll_limit &&
               moq_session_poll_events(s, &ev, 1) > 0) {
            taken++;
            switch (ev.kind) {
            case MOQ_EVENT_SETUP_COMPLETE:
                g_pump.setup++;
                break;
            case MOQ_EVENT_SUBSCRIBE_OK:
                g_pump.sub_ok++;
                break;
            case MOQ_EVENT_OBJECT_RECEIVED: {
                const moq_rcbuf_t *pl = ev.u.object_received.payload;
                uint64_t oid = ev.u.object_received.object_id;

                if (oid != g_pump.next_expected)
                    g_pump.order_errors++;
                g_pump.next_expected = oid + 1;
                if (oid < 64) {
                    if (g_pump.seen_mask & (1ull << oid))
                        g_pump.object_errors++; /* duplicate */
                    g_pump.seen_mask |= 1ull << oid;
                }
                if (pl == NULL || moq_rcbuf_len(pl) != OBJ_LEN) {
                    g_pump.object_errors++;
                } else {
                    uint8_t want[OBJ_LEN];

                    payload_fill(want, OBJ_LEN);
                    if (memcmp(moq_rcbuf_data(pl), want, OBJ_LEN) != 0)
                        g_pump.object_errors++;
                }
                g_pump.objects++;
                break;
            }
            case MOQ_EVENT_SUBGROUP_FINISHED:
                g_pump.sub_finished++;
                break;
            default:
                /* every kind this fixture can legitimately see is named
                 * above; anything else means the declared inventory the
                 * exact pump-generation count is derived from is wrong */
                if (g_pump.unexpected_events == 0)
                    g_pump.first_unexpected_kind = ev.kind;
                g_pump.unexpected_events++;
                break;
            }
            moq_event_cleanup(&ev);
        }
        if (g_pump.subscribe && g_pump.setup > 0 && !g_pump.subscribed) {
            static const moq_bytes_t ns[] = {
                { (const uint8_t *)"live", 4 }
            };
            moq_subscribe_cfg_t sc;
            moq_subscription_t sub;

            moq_subscribe_cfg_init(&sc);
            sc.track_namespace.parts = (moq_bytes_t *)ns;
            sc.track_namespace.count = 1;
            sc.track_name = (moq_bytes_t){ (const uint8_t *)"video", 5 };
            sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
            g_pump.subscribed =
                moq_session_subscribe(s, &sc, now_us, &sub) == MOQ_OK;
        }
    }
    pthread_cond_broadcast(&g_pump.cv);
    pthread_mutex_unlock(&g_pump.mu);
    if (stop_now) {
        /* release any waiter immediately: a re-arming build never reaches
         * the barrier, so the barrier alone would only free them when its
         * hang guard expired */
        atomic_store(&g_excess_flag, true);
        pthread_mutex_lock(&g_gate.mu);
        pthread_cond_broadcast(&g_gate.cv);
        pthread_mutex_unlock(&g_gate.mu);
    }
    return stop_now ? 1 : 0;
}

/* Wait for the lane to park at the barrier, OR for the spin probe to fire.
 * Returns true only for the parked case; the caller's own !excess_now()
 * assertion is what reports a spin. */
static bool await_barrier_or_excess(uint64_t want)
{
    struct timespec abs;
    bool parked;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_gate.mu);
    while (g_gate.entered < want && !g_gate.expired &&
           !atomic_load(&g_excess_flag))
        if (pthread_cond_timedwait(&g_gate.cv, &g_gate.mu, &abs) != 0)
            break;
    parked = g_gate.entered >= want && !g_gate.expired;
    pthread_mutex_unlock(&g_gate.mu);
    return parked;
}

static uint64_t pumps_now(void)
{
    uint64_t v;

    pthread_mutex_lock(&g_pump.mu);
    v = g_pump.pumps;
    pthread_mutex_unlock(&g_pump.mu);
    return v;
}

static bool excess_now(void)
{
    bool v;

    pthread_mutex_lock(&g_pump.mu);
    v = g_pump.excess;
    pthread_mutex_unlock(&g_pump.mu);
    return v;
}

static void set_limit(uint64_t limit)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.limit = limit;
    pthread_mutex_unlock(&g_pump.mu);
}

static int objects_now(void)
{
    int v;

    pthread_mutex_lock(&g_pump.mu);
    v = g_pump.objects;
    pthread_mutex_unlock(&g_pump.mu);
    return v;
}

/* When the spin probe fires, the frozen terminal consequences must hold.
 * Conditional so a healthy row stays green, but it executes under a
 * re-arm-forever mutant, which is where it matters. */
static void check_excess_terminal(moq_msquic_managed_t *m)
{
    if (!excess_now())
        return;
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.nonzero_returns == 1);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(moq_msquic_managed_wake(m) == MOQ_ERR_CLOSED);
}

static uint64_t lane_sweeps(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_lane_stats_t st;

    if (moq_msquic_lane_get_stats(lane, &st, sizeof(st)) != MOQ_OK)
        return UINT64_MAX;
    return st.pump_sweeps;
}

/* --- driving the peer -------------------------------------------------- */

/* Bring the flow up and leave N_OBJECTS queued on the child, WITHOUT the
 * child having drained them. Returns false if the flow did not establish.
 */
static bool fill_backlog(struct pair *p)
{
    enum { MAX_STEPS = 40 };
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    bool accepted = false, published = false;
    int steps = 0;

    while (steps++ < MAX_STEPS && !published) {
        moq_event_t ev;

        if (!step(p))
            return false;
        while (moq_session_poll_events(p->peer, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                server_sub = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        if (!accepted && moq_subscription_is_valid(server_sub)) {
            moq_accept_subscribe_cfg_t acc;

            moq_accept_subscribe_cfg_init(&acc);
            accepted = moq_session_accept_subscribe(p->peer, server_sub,
                                                    &acc, 0) == MOQ_OK;
            continue;
        }
        if (accepted) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;

            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.subgroup_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(p->peer, server_sub, &sgc, 0,
                                          &sg) != MOQ_OK)
                return false;
            for (int i = 0; i < N_OBJECTS; i++) {
                uint8_t bytes[OBJ_LEN];
                moq_rcbuf_t *buf = NULL;

                payload_fill(bytes, OBJ_LEN);
                if (moq_rcbuf_create(moq_alloc_default(), bytes, OBJ_LEN,
                                     &buf) < 0)
                    return false;
                (void)moq_session_write_object(p->peer, sg, (uint64_t)i,
                                               buf, 0);
                moq_rcbuf_decref(buf);
            }
            (void)moq_session_close_subgroup(p->peer, sg, 0);
            published = true;
        }
    }
    return published;
}

/* --- 1. bounded drain: the doorbell re-drives itself ------------------- */

static void t_bounded_drain_redrives_autonomously(void)
{
    int before = failures;
    struct pair p;

    pump_reset(POLL_PER_PUMP, true);
    if (!pair_up(&p, false, sizeof(moq_msquic_managed_cfg_t),
                 counting_pump)) {
        CHECK(0 && "pair up");
        pair_down(&p);
        return;
    }
    CHECK(fill_backlog(&p));

    /* deliver the whole backlog to the child's transport, then hand the
     * lane ONE external wake and take our hands off */
    relay(&p);
    /* the fixture is non-vacuous by construction: there really is a
     * backlog for the doorbell to re-drive */
    CHECK(moq_msq_conn_has_events(p.adapter));

    /* the setup/control phase is already OUTSIDE the measured interval,
     * and none of the measured backlog has been consumed yet -- so the
     * pump-generation count below measures the declared inventory and
     * nothing else */
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.setup == 1);
    CHECK(g_pump.sub_ok == 1);
    CHECK(g_pump.objects == 0);
    CHECK(g_pump.sub_finished == 0);
    CHECK(g_pump.unexpected_events == 0);
    pthread_mutex_unlock(&g_pump.mu);

    set_limit(pumps_now() + PUMP_ALLOWANCE);
    uint64_t pumps_before = pumps_now();

    CHECK(moq_msquic_lane_wake(p.lane) == MOQ_OK);

    /* ONE release of the currently parked barrier is enough: a correct
     * build performs every token-driven re-pump before it reaches the
     * next pre-wait barrier, so the whole backlog is gone by the time we
     * observe it there. A build that does not re-arm parks after its
     * first partial drain and fails immediately -- no idle-cap loop, and
     * therefore no way for a missing re-arm to become a timeout. */
    gate_release(++p.gen);
    CHECK(await_barrier_or_excess(p.gen + 1));

    CHECK(!excess_now());
    check_excess_terminal(p.m);

    pthread_mutex_lock(&g_pump.mu);
    /* the COMPLETE declared inventory drained, and nothing else ... */
    CHECK(g_pump.objects == N_OBJECTS);
    CHECK(g_pump.seen_mask == ((1ull << N_OBJECTS) - 1)); /* exactly once */
    CHECK(g_pump.order_errors == 0);                      /* in order */
    CHECK(g_pump.object_errors == 0);                     /* byte-exact */
    CHECK(g_pump.sub_finished == SUBGROUP_FINISHED_OWED);
    CHECK(g_pump.unexpected_events == 0);
    CHECK(g_pump.first_unexpected_kind == 0);
    /* ... in EXACTLY the generations the inventory owes. One external
     * wake buys one; every further generation is the doorbell re-driving
     * itself, and a build that re-arms without remaining events takes a
     * further empty one -- which this equality rejects. */
    CHECK(g_pump.pumps == pumps_before + EXPECTED_PUMP_DELTA);
    pthread_mutex_unlock(&g_pump.mu);
    /* and the production event queue really is empty */
    CHECK(!moq_msq_conn_has_events(p.adapter));

    pair_down(&p);
    if (failures == before)
        printf("PASS: bounded_drain_redrives_autonomously\n");
}

/* --- 2. non-drain: one wake, one pump, then park ----------------------- */

static void t_non_drain_parks_with_backlog(void)
{
    int before = failures;
    struct pair p;

    /* establish and fill with a polling pump, then stop polling: the
     * backlog has to be real for "does not drain" to mean anything */
    pump_reset(POLL_PER_PUMP, true);
    if (!pair_up(&p, false, sizeof(moq_msquic_managed_cfg_t),
                 counting_pump)) {
        CHECK(0 && "pair up");
        pair_down(&p);
        return;
    }
    CHECK(fill_backlog(&p));
    relay(&p);

    pthread_mutex_lock(&g_pump.mu);
    g_pump.poll_limit = 0; /* from now on the application takes nothing */
    pthread_mutex_unlock(&g_pump.mu);

    /* Arm the spin probe BEFORE the first release: a build that re-arms
     * without token movement never reaches the barrier again, so the probe
     * -- not the barrier's hang guard -- is what has to free this wait. */
    set_limit(pumps_now() + PUMP_ALLOWANCE);

    /* let the lane settle at the barrier with the backlog undrained */
    gate_release(++p.gen);
    CHECK(await_barrier_or_excess(p.gen + 1));
    CHECK(!excess_now());
    int drained_before = objects_now();

    /* the backlog is real, which is what makes "does not drain" mean
     * something */
    CHECK(moq_msq_conn_has_events(p.adapter));

    uint64_t pumps_before = pumps_now();
    uint64_t sweeps_before = lane_sweeps(p.lane);

    CHECK(sweeps_before != UINT64_MAX);
    CHECK(moq_msquic_lane_wake(p.lane) == MOQ_OK);
    gate_release(++p.gen);
    CHECK(await_barrier_or_excess(p.gen + 1));

    /* exactly one generation for the one wake: with no event consumed the
     * token cannot advance, so nothing may re-arm */
    CHECK(!excess_now());
    check_excess_terminal(p.m);
    CHECK(pumps_now() == pumps_before + 1);
    CHECK(lane_sweeps(p.lane) == sweeps_before + 1);
    CHECK(objects_now() == drained_before); /* the backlog is still there */
    CHECK(objects_now() < N_OBJECTS);
    CHECK(moq_msq_conn_has_events(p.adapter));

    /* and a raw bell generation, which arms no work at all, buys nothing */
    uint64_t pumps_at_bell = pumps_now();

    moq_msq_test_bell_ring_raw(p.lane);
    gate_release(++p.gen);
    CHECK(await_barrier_or_excess(p.gen + 1));
    CHECK(!excess_now());
    CHECK(pumps_now() == pumps_at_bell);
    CHECK(moq_msq_conn_has_events(p.adapter)); /* still undrained */

    pair_down(&p);
    if (failures == before)
        printf("PASS: non_drain_parks_with_backlog\n");
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);
    moq_msq_test_prewait = prewait_hook;

    t_bounded_drain_redrives_autonomously();
    t_non_drain_parks_with_backlog();

    if (failures == 0)
        printf("PASS: msquic_no_spin\n");
    else
        fprintf(stderr, "FAIL: msquic_no_spin (%d)\n", failures);
    return failures;
}
