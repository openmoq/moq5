/*
 * Managed MsQuic reap FAIRNESS: an acknowledged, transport-reapable server
 * child must be reclaimed while its lane still has work continuously owed.
 *
 * doorbell_main() handles an armed lane by running doorbell_pump_sweep() and
 * immediately `continue`ing; doorbell_reap() is reached only on an iteration
 * whose loop-top predicate found NO work. A callback that arms same-lane work
 * on every pass — moq_msquic_lane_wake(), or event progress rearming the
 * coalesced pump — therefore keeps an eligible child linked, holding its
 * cfg.max_connections reserve, for as long as the lane stays busy. The
 * existing tests cover different rules: msquic_reap pins the ORDER of work
 * inside a reap pass, and msquic_terminal_ack pins eventual release once its
 * pump work has settled. Neither keeps work owed while an eligible child waits.
 *
 * This is fairness AFTER acknowledgment. It changes no terminal-acknowledgment
 * semantics, and the acknowledged/reapable preconditions are asserted here
 * rather than assumed.
 *
 * NETWORK-FREE. The facade comes from a MOQ_MSQUIC_TESTING constructor that
 * builds only lane and doorbell state: no registration, no configuration, no
 * credential, no listener, no socket, no certificate file. Nothing here can
 * accept, connect, or carry a byte, so the test runs where no UDP bind is
 * possible. Acceptance is an exact operation count — the child must be
 * reclaimed while the pump counter is still BELOW the churn gate, i.e. while
 * the lane is still being rearmed on every pass. The per-call waits are
 * fail-closed hang guards and are never an acceptance oracle.
 *
 * The child is SYNTHESIZED by a second seam. It is not a live accept: there is
 * no transport handle and no real SHUTDOWN_COMPLETE. What is production about
 * it is the reserve accounting, the session, the lane linkage, the bridge's
 * own close entry (so SESSION_CLOSED is genuinely queued), and mgd_hook's
 * reapability rule — which is why the transport half is read back through an
 * independent seam rather than assumed.
 *
 * Alongside the fairness case, stop and pump-exit controls pin its boundaries.
 * A lane whose callback returns nonzero proves the callback suppressor is
 * load-bearing and that an unacknowledged child survives to forced teardown.
 * A stopped lane is
 * ordered, not timed: the callback is PARKED inside its pump still owning
 * lane->mu, stop runs on a helper thread, and a seam publishes the instant
 * stop's facade flag is latched — before stop reaches for any lane lock — so
 * the callback is released only once that fact holds. The doorbell then leaves
 * that pump with stop already visible, which is exactly what the new reap site
 * must decline: the child is reclaimed by stop's own forced cleanup, once.
 * A separate continuously-rearmed stop control pins the older loop-top
 * contract: stop publishes the facade-wide atomic before taking lane->mu, so
 * the doorbell must leave immediately after the callback returns even though
 * that callback armed another pass. This protects stop without requiring a
 * scheduling-dependent mutex yield between ordinary pump passes.
 *
 * Compiled with MOQ_MSQUIC_TESTING (seams never ship).
 *
 * Usage: test_msquic_reap_fairness
 */
#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <moq/msquic_managed.h>

#include <msquic.h>

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

static int failures;
#define CHECK(c)                                                            \
    do {                                                                    \
        if (!(c)) {                                                         \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c);    \
            failures++;                                                     \
        }                                                                   \
    } while (0)

/* The churn gate is an OPERATION count, not a duration: the lane is rearmed on
 * every one of these pumps, so a scheduler that only reaps when the lane falls
 * idle cannot reclaim within it. Sized far beyond the single pump a fair
 * scheduler needs, and cheap — each pass sweeps one connection. */
enum { GATE_PUMPS = 20000, POST_REAP_MIN = 64, POST_EXIT_SWEEPS = 8,
       STOP_REARM_PUMPS = 32, SPIN_MAX = 40000, WAIT_US = 2 * 1000 };

/* Fail-closed bound on the ordered stop control's two rendezvous. Expiry is a
 * failure, never a result. */
#define HOLD_GUARD_US (10u * 1000u * 1000u)

#define CLOSE_CODE 0x21u

/* --- white-box seams (MOQ_MSQUIC_TESTING build only) ---------------------- */
extern void (*moq_msq_test_reap_gap)(moq_msquic_managed_lane_t *lane);
extern size_t moq_msq_test_lane_reapable(moq_msquic_managed_lane_t *lane);
extern bool moq_msq_test_lane_inject_terminal_child(
    moq_msquic_managed_lane_t *lane, uint64_t close_code);
extern moq_result_t moq_msq_test_managed_create_lanes_only(
    const moq_msquic_managed_cfg_t *cfg, moq_msquic_managed_t **out);
extern void (*moq_msq_test_stop_latched)(moq_msquic_managed_t *m);


enum { PH_HOLD = 0, PH_RUN = 1, PH_DONE = 2 };

struct fx {
    atomic_int phase;
    atomic_int pumps;          /* on_lane_pump sweeps entered              */
    atomic_int pumps_with_conn;/* ... that were presented the child        */
    atomic_int closed_polled;
    atomic_int ack_rc;
    atomic_int acked_at_pump;
    atomic_int reaps;          /* physical reclamations on this lane       */
    atomic_int reaps_at_ack;
    atomic_int pumps_at_reap;
    atomic_int with_conn_at_reap; /* pumps_with_conn AT the reclamation    */
    atomic_int churn_wakes;
    atomic_int pumps_after_reap;  /* callbacks that ran after it           */
    atomic_int post_reap_rearms;  /* ... and rearmed the lane themselves   */
    atomic_int nonzero_returns;
    /* controls */
    atomic_int exit_at_pump;   /* the callback that returned nonzero       */
    atomic_int ack_before_exit;
    atomic_int held_at_pump;   /* the callback parked inside its pump      */
    atomic_int reaps_at_hold;
    atomic_int hold_expired;   /* the hang guard fired instead of release  */
    atomic_int wake_in_hold;   /* facade wake() seen by the held callback  */
};

#define RC_UNSET 0x7fffffff

/* The doorbell-facing rendezvous. Both sides of every handshake are under this
 * mutex, so each ordering below is established by the program rather than by
 * the scheduler. It is NOT lane->mu — a parked callback already holds that one
 * — and nothing else is taken under it, so it stays a leaf. */
static struct bar {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool held;         /* the lane callback is parked inside its pump   */
    bool release;      /* the test permits it to return                 */
    bool stop_latched; /* stop() published its facade flag              */
    bool reaped;       /* a reclamation completed on the watched lane   */
} g_bar = { .mu = PTHREAD_MUTEX_INITIALIZER, .cv = PTHREAD_COND_INITIALIZER };

static void bar_reset(void)
{
    pthread_mutex_lock(&g_bar.mu);
    g_bar.held = false;
    g_bar.release = false;
    g_bar.stop_latched = false;
    g_bar.reaped = false;
    pthread_mutex_unlock(&g_bar.mu);
}

static void bar_set(bool *flag)
{
    pthread_mutex_lock(&g_bar.mu);
    *flag = true;
    pthread_cond_broadcast(&g_bar.cv);
    pthread_mutex_unlock(&g_bar.mu);
}

/* Returns the flag's value at exit: false means the guard expired. */
static bool bar_wait(bool *flag, uint64_t timeout_us)
{
    struct timespec abs;
    bool got;

    clock_gettime(CLOCK_REALTIME, &abs);
    abs.tv_sec += (time_t)(timeout_us / 1000000u);
    abs.tv_nsec += (long)(timeout_us % 1000000u) * 1000;
    if (abs.tv_nsec >= 1000000000L) {
        abs.tv_sec += 1;
        abs.tv_nsec -= 1000000000L;
    }
    pthread_mutex_lock(&g_bar.mu);
    while (!*flag)
        if (pthread_cond_timedwait(&g_bar.cv, &g_bar.mu, &abs) != 0)
            break;
    got = *flag;
    pthread_mutex_unlock(&g_bar.mu);
    return got;
}

/* The doorbell thread reads these; the main thread publishes them after the
 * facade constructor has already spawned it, so neither has a happens-before
 * edge and both are atomic. */
static struct fx *_Atomic g_fx;
static moq_msquic_managed_lane_t *_Atomic g_lane;
/* Installed only around the ordered stop control's single stop() call, and
 * scoped to that facade. */
static moq_msquic_managed_t *_Atomic g_stop_m;

/* Fires inside doorbell_reap, once the child is freed and its reserve released
 * — so publishing here is the one point at which BOTH the counter and the
 * facade's connection count are settled for that reclamation. */
static void reap_gap_hook(moq_msquic_managed_lane_t *lane)
{
    struct fx *f = atomic_load(&g_fx);

    if (f == NULL || lane != atomic_load(&g_lane))
        return;
    if (atomic_fetch_add(&f->reaps, 1) == 0) {
        atomic_store(&f->pumps_at_reap, atomic_load(&f->pumps));
        atomic_store(&f->with_conn_at_reap, atomic_load(&f->pumps_with_conn));
    }
    bar_set(&g_bar.reaped);
}

/* Fires inside moq_msquic_managed_stop(), after its facade flag is published
 * and before it takes any lane lock. */
static void stop_latched_hook(moq_msquic_managed_t *m)
{
    if (m != atomic_load(&g_stop_m))
        return;
    bar_set(&g_bar.stop_latched);
}

/* --- the lane callback ---------------------------------------------------- */

static int fair_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                     uint64_t now, void *user)
{
    struct fx *f = user;
    int phase = atomic_load(&f->phase);
    int idx = atomic_fetch_add(&f->pumps, 1) + 1;
    moq_msquic_managed_conn_t *c = NULL;
    bool reaped = atomic_load(&f->reaps) >= 1;

    (void)m;
    (void)now;
    if (phase == PH_HOLD)
        return 0; /* the child is placed but untouched: nothing consumed */
    if (reaped)
        atomic_fetch_add(&f->pumps_after_reap, 1);

    while ((c = moq_msquic_lane_next_conn(lane, c)) != NULL) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        moq_event_t ev;

        atomic_fetch_add(&f->pumps_with_conn, 1);
        if (s == NULL)
            continue;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED)
                atomic_fetch_add(&f->closed_polled, 1);
            moq_event_cleanup(&ev);
        }
        if (atomic_load(&f->closed_polled) > 0 &&
            atomic_load(&f->ack_rc) == RC_UNSET &&
            (atomic_load(&f->exit_at_pump) == 0 ||
             atomic_load(&f->ack_before_exit) != 0)) {
            atomic_store(&f->reaps_at_ack, atomic_load(&f->reaps));
            atomic_store(&f->ack_rc, moq_msquic_managed_conn_ack_terminal(c));
            atomic_store(&f->acked_at_pump, idx);
        }
    }

    /* The pump-exit control: leave through a nonzero result. */
    if (atomic_load(&f->exit_at_pump) < 0) {
        atomic_store(&f->exit_at_pump, idx);
        atomic_fetch_add(&f->nonzero_returns, 1);
        return 1;
    }

    /* The churn. Before the reclamation it runs to the gate; after it, the
     * lane keeps being rearmed for a fixed number of further callbacks, so
     * "the callback still runs once its child is gone" is an operation count
     * and not something the scheduler happened to do. */
    bool rearm = reaped ? atomic_load(&f->pumps_after_reap) <= POST_REAP_MIN
                        : idx < GATE_PUMPS;
    if (phase == PH_RUN && rearm) {
        atomic_fetch_add(&f->churn_wakes, 1);
        if (reaped)
            atomic_fetch_add(&f->post_reap_rearms, 1);
        (void)moq_msquic_lane_wake(lane);
    }
    return 0;
}

/* The ordered stop control's callback: transfer the terminal, acknowledge it,
 * then PARK inside the pump — lane->mu still held by this doorbell — until the
 * test has seen stop latch its facade flag. No churn: the point is the single
 * pump that returns with stop already visible. */
static int stop_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
                     uint64_t now, void *user)
{
    struct fx *f = user;
    int idx = atomic_fetch_add(&f->pumps, 1) + 1;
    moq_msquic_managed_conn_t *c = NULL;

    (void)now;
    while ((c = moq_msquic_lane_next_conn(lane, c)) != NULL) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        moq_event_t ev;

        atomic_fetch_add(&f->pumps_with_conn, 1);
        if (s == NULL)
            continue;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED)
                atomic_fetch_add(&f->closed_polled, 1);
            moq_event_cleanup(&ev);
        }
        if (atomic_load(&f->closed_polled) > 0 &&
            atomic_load(&f->ack_rc) == RC_UNSET) {
            atomic_store(&f->reaps_at_ack, atomic_load(&f->reaps));
            atomic_store(&f->ack_rc, moq_msquic_managed_conn_ack_terminal(c));
            atomic_store(&f->acked_at_pump, idx);
        }
    }
    /* Park only once, and only on a child this callback really acknowledged:
     * a hold without the acknowledgment would prove nothing about the reap
     * site, which declines an unacknowledged child on eligibility alone. */
    if (atomic_load(&f->ack_rc) == MOQ_OK &&
        atomic_load(&f->held_at_pump) == 0) {
        atomic_store(&f->held_at_pump, idx);
        atomic_store(&f->reaps_at_hold, atomic_load(&f->reaps));
        bar_set(&g_bar.held);
        if (!bar_wait(&g_bar.release, HOLD_GUARD_US))
            atomic_store(&f->hold_expired, 1);
        /* The flag the new reap site reads, observed from inside this
         * still-running callback through the public surface: on a SERVER
         * facade that has not pump-exited, CLOSED can only be the facade stop.
         * Its early return makes this a pure read — it arms nothing. */
        atomic_store(&f->wake_in_hold, moq_msquic_managed_wake(m));
    }
    return 0;
}

/* Rearm on every delivered pass through a declared boundary, then park while
 * still owning lane->mu. The stop hook publishes the facade atomic before its
 * caller attempts that mutex. Once the callback observes the publication and
 * returns, the loop-top predicate must suppress the already-armed next pass.
 * A callback beyond the boundary returns nonzero so a broken predicate fails
 * deterministically without leaving the test runner hung. */
static int stop_rearm_pump(moq_msquic_managed_t *m,
                           moq_msquic_managed_lane_t *lane, uint64_t now,
                           void *user)
{
    struct fx *f = user;
    int idx = atomic_fetch_add(&f->pumps, 1) + 1;

    (void)now;
    if (idx > STOP_REARM_PUMPS) {
        atomic_fetch_add(&f->nonzero_returns, 1);
        return 1;
    }

    if (moq_msquic_lane_wake(lane) == MOQ_OK)
        atomic_fetch_add(&f->churn_wakes, 1);

    if (idx == STOP_REARM_PUMPS) {
        atomic_store(&f->held_at_pump, idx);
        bar_set(&g_bar.held);
        if (!bar_wait(&g_bar.stop_latched, HOLD_GUARD_US))
            atomic_store(&f->hold_expired, 1);
        atomic_store(&f->wake_in_hold, moq_msquic_managed_wake(m));
    }
    return 0;
}

/* --- the network-free rig -------------------------------------------------- */

struct rig {
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane;
};

static bool rig_up(struct rig *r, struct fx *f, uint32_t max_conns,
                   moq_msquic_lane_pump_fn pump)
{
    moq_msquic_managed_cfg_t cfg;

    memset(r, 0, sizeof(*r));
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.max_events = 64;
    cfg.max_connections = max_conns;
    cfg.lane_count = 1;
    cfg.on_lane_pump = pump;
    cfg.on_lane_pump_user = f;
    /* no host, no port, no cert, no key: the constructor opens no listener */
    if (moq_msq_test_managed_create_lanes_only(&cfg, &r->m) != MOQ_OK ||
        r->m == NULL)
        return false;
    r->lane = moq_msquic_managed_lane(r->m, 0);
    if (r->lane == NULL)
        return false;
    atomic_store(&g_lane, r->lane);
    moq_msq_test_reap_gap = reap_gap_hook;
    return true;
}

static void rig_down(struct rig *r)
{
    moq_msq_test_reap_gap = NULL;
    atomic_store(&g_lane, NULL);
    if (r->m != NULL) {
        (void)moq_msquic_managed_stop(r->m);
        moq_msquic_managed_destroy(r->m);
        r->m = NULL;
    }
}

static void fx_init(struct fx *f)
{
    memset(f, 0, sizeof(*f));
    atomic_store(&f->ack_rc, RC_UNSET);
    atomic_store(&f->phase, PH_HOLD);
    atomic_store(&g_fx, f);
    bar_reset();
}

/* --- 1. fairness ----------------------------------------------------------- */

static void t_reap_is_fair_under_continuous_work(void)
{
    int before = failures;
    struct fx f;
    struct rig r;

    fx_init(&f);
    if (!rig_up(&r, &f, 1, fair_pump)) {
        CHECK(0 && "lanes-only facade");
        rig_down(&r);
        atomic_store(&g_fx, NULL);
        return;
    }

    bool placed = moq_msq_test_lane_inject_terminal_child(r.lane, CLOSE_CODE);

    /* The transport half, read back independently while the callback still
     * holds: nothing the application did can account for it. */
    size_t reapable_before = moq_msq_test_lane_reapable(r.lane);
    size_t conns_before = moq_msquic_managed_conn_count(r.m);
    int reaps_before_run = atomic_load(&f.reaps);

    atomic_store(&f.phase, PH_RUN);
    (void)moq_msquic_managed_wake(r.m);

    /* The child must go while the lane is still armed on every pass. */
    bool reclaimed = false, gate_spent = false;
    int spins = 0;

    for (; spins < SPIN_MAX && !reclaimed && !gate_spent; spins++) {
        (void)moq_msquic_managed_wait(r.m, WAIT_US);
        reclaimed = atomic_load(&f.reaps) >= 1 &&
                    moq_msquic_managed_conn_count(r.m) == 0;
        gate_spent = atomic_load(&f.pumps) >= GATE_PUMPS;
    }

    /* The post-reap barrier: the churn keeps running until a fixed number of
     * further callbacks have been delivered with the child gone. */
    bool barrier = false;
    int spins2 = 0;

    for (; spins2 < SPIN_MAX && !barrier; spins2++) {
        (void)moq_msquic_managed_wait(r.m, WAIT_US);
        barrier = atomic_load(&f.pumps_after_reap) >= POST_REAP_MIN;
        if (atomic_load(&f.pumps) >= GATE_PUMPS)
            break;
    }
    int with_conn_end = atomic_load(&f.pumps_with_conn);
    /* the facade is still normally running: not closed, not pump-exited */
    moq_result_t wake_after = moq_msquic_managed_wake(r.m);

    atomic_store(&f.phase, PH_DONE);

    bool settled = false;

    for (int i = 0; i < SPIN_MAX && !settled; i++) {
        (void)moq_msquic_managed_wait(r.m, WAIT_US);
        settled = atomic_load(&f.reaps) >= 1 &&
                  moq_msquic_managed_conn_count(r.m) == 0;
    }
    size_t conns_settled = moq_msquic_managed_conn_count(r.m);
    bool fatal = moq_msquic_managed_is_fatal(r.m);

    rig_down(&r);
    atomic_store(&g_fx, NULL);

    printf("FAIRNESS: placed=%d reapable_before=%zu conns_before=%zu "
           "pumps=%d with_conn=%d closed_polled=%d ack_rc=%d acked_at=%d "
           "reaps_at_ack=%d reaps=%d pumps_at_reap=%d with_conn_at_reap=%d "
           "after_reap=%d post_rearms=%d churn=%d gate=%d gate_spent=%d "
           "nonzero=%d spins=%d/%d conns_settled=%zu\n",
           (int)placed, reapable_before, conns_before, atomic_load(&f.pumps),
           with_conn_end, atomic_load(&f.closed_polled),
           atomic_load(&f.ack_rc), atomic_load(&f.acked_at_pump),
           atomic_load(&f.reaps_at_ack), atomic_load(&f.reaps),
           atomic_load(&f.pumps_at_reap), atomic_load(&f.with_conn_at_reap),
           atomic_load(&f.pumps_after_reap),
           atomic_load(&f.post_reap_rearms), atomic_load(&f.churn_wakes),
           GATE_PUMPS, (int)gate_spent, atomic_load(&f.nonzero_returns),
           spins, spins2, conns_settled);

    /* setup and controls: green in both worlds */
    CHECK(placed);
    CHECK(reapable_before == 1);
    CHECK(conns_before == 1);
    CHECK(reaps_before_run == 0);
    CHECK(atomic_load(&f.reaps_at_ack) == 0);
    CHECK(atomic_load(&f.closed_polled) == 1);
    CHECK(atomic_load(&f.ack_rc) == MOQ_OK);
    CHECK(atomic_load(&f.acked_at_pump) > 0);
    CHECK(atomic_load(&f.churn_wakes) >= 1);
    CHECK(spins < SPIN_MAX);
    CHECK(!fatal);
    CHECK(settled);
    CHECK(atomic_load(&f.reaps) == 1);
    CHECK(conns_settled == 0);

    /* fairness, all measured inside the churn window */
    CHECK(!gate_spent);
    CHECK(atomic_load(&f.pumps_at_reap) < GATE_PUMPS);
    CHECK(with_conn_end <= 2);

    /* the callback keeps being delivered afterwards, for a declared count,
     * and every one of those callbacks rearmed the lane and returned zero */
    CHECK(barrier);
    CHECK(atomic_load(&f.pumps_after_reap) >= POST_REAP_MIN);
    CHECK(atomic_load(&f.post_reap_rearms) >= POST_REAP_MIN);
    CHECK(with_conn_end == atomic_load(&f.with_conn_at_reap));
    CHECK(atomic_load(&f.nonzero_returns) == 0);
    CHECK(wake_after == MOQ_OK);

    if (failures == before)
        printf("PASS: reap_is_fair_under_continuous_work\n");
}

/* --- 2. control: pump exit ------------------------------------------------- */

static void t_pump_exit_retains_child(bool ack_first)
{
    int before = failures;
    struct fx f;
    struct rig r;

    fx_init(&f);
    atomic_store(&f.exit_at_pump, -1); /* arm: the next RUN callback exits */
    atomic_store(&f.ack_before_exit, ack_first ? 1 : 0);
    if (!rig_up(&r, &f, 1, fair_pump)) {
        CHECK(0 && "lanes-only facade");
        rig_down(&r);
        atomic_store(&g_fx, NULL);
        return;
    }

    bool placed = moq_msq_test_lane_inject_terminal_child(r.lane, CLOSE_CODE);

    atomic_store(&f.phase, PH_RUN);
    (void)moq_msquic_managed_wake(r.m);

    bool exited = false;
    int spins = 0;

    for (; spins < SPIN_MAX && !exited; spins++) {
        (void)moq_msquic_managed_wait(r.m, WAIT_US);
        exited = atomic_load(&f.exit_at_pump) > 0;
    }
    int pumps_at_exit = atomic_load(&f.pumps);

    /* An acknowledged child is eligible, so it WILL be reclaimed. Rendezvous
     * with that reclamation instead of sampling for it: the reap hook publishes
     * only once the child is freed and its reserve released, whereas the two
     * counters below are written at different points inside doorbell_reap and
     * can be read mid-reclamation. Nothing here is timed. */
    bool reaped_ok = !ack_first || bar_wait(&g_bar.reaped, HOLD_GUARD_US);

    /* Arm the lane from OUTSIDE until the doorbell has MEASURABLY run that many
     * more work-branch iterations — sweeps advance even while the callback is
     * suppressed, so this is the operation count that gives the assertion below
     * something to be true about. A blind arming loop would let a doorbell that
     * never got the lane satisfy "no later callback ran" vacuously; here that
     * outcome exhausts the spin bound and fails instead. */
    uint64_t sweeps_at_exit = 0;
    moq_msquic_lane_stats_t st;

    if (moq_msquic_lane_get_stats(r.lane, &st, sizeof(st)) == MOQ_OK)
        sweeps_at_exit = st.pump_sweeps;

    bool swept = false;
    int spins2 = 0;

    for (; spins2 < SPIN_MAX && !swept; spins2++) {
        (void)moq_msquic_lane_wake(r.lane);
        (void)moq_msquic_managed_wait(r.m, WAIT_US);
        if (moq_msquic_lane_get_stats(r.lane, &st, sizeof(st)) == MOQ_OK)
            swept = st.pump_sweeps >= sweeps_at_exit + POST_EXIT_SWEEPS;
    }

    int pumps_after_exit = atomic_load(&f.pumps);
    int reaps_after_exit = atomic_load(&f.reaps);
    size_t conns_held = moq_msquic_managed_conn_count(r.m);
    moq_result_t wake_rc = moq_msquic_managed_wake(r.m);
    /* wait() reports pending ACTIVITY ahead of the terminal and consumes one
     * latch per call, so a sweep that landed after the last consume answers
     * MOQ_OK once more. Drain to the terminal instead of sampling: what this
     * pins is the facade state, not how many sweeps the doorbell managed. The
     * bound is a hang guard and is asserted, never an oracle. */
    moq_result_t wait_rc = MOQ_OK;
    int wait_spins = 0;

    for (; wait_spins < SPIN_MAX && wait_rc == MOQ_OK; wait_spins++)
        wait_rc = moq_msquic_managed_wait(r.m, 0);
    bool fatal = moq_msquic_managed_is_fatal(r.m);

    rig_down(&r); /* forced teardown reclaims it, exactly once */
    atomic_store(&g_fx, NULL);

    printf("PUMP-EXIT(ack_first=%d): placed=%d exit_at=%d pumps_at_exit=%d "
           "pumps_after=%d ack_rc=%d reaped_ok=%d swept=%d/%d reaps=%d "
           "conns_held=%zu wake=%d wait=%d wait_spins=%d fatal=%d\n",
           (int)ack_first, (int)placed, atomic_load(&f.exit_at_pump),
           pumps_at_exit, pumps_after_exit, atomic_load(&f.ack_rc),
           (int)reaped_ok, (int)swept, spins2, reaps_after_exit, conns_held,
           wake_rc, wait_rc, wait_spins, (int)fatal);

    CHECK(placed);
    CHECK(exited);
    CHECK(spins < SPIN_MAX);
    CHECK(reaped_ok);
    /* the doorbell really did run its work branch after the exit */
    CHECK(swept);
    CHECK(spins2 < SPIN_MAX);
    CHECK(atomic_load(&f.nonzero_returns) == 1);
    if (ack_first)
        CHECK(atomic_load(&f.ack_rc) == MOQ_OK);
    else
        CHECK(atomic_load(&f.ack_rc) == RC_UNSET);
    /* no later application callback runs, however hard the lane is rearmed */
    CHECK(pumps_after_exit == pumps_at_exit);
    /* What happens to the child depends only on ELIGIBILITY. Unacknowledged, it
     * is ineligible and is retained until forced teardown. Acknowledged, it is
     * eligible and is reclaimed on the next pass — pump exit is not a reap
     * condition anywhere, and it cannot be one here: a suppressed callback can
     * no longer rearm the lane, so the doorbell falls idle immediately and the
     * no-work site is reached regardless of which pass takes it. */
    if (ack_first) {
        CHECK(reaps_after_exit == 1);
        CHECK(conns_held == 0);
    } else {
        CHECK(reaps_after_exit == 0);
        CHECK(conns_held == 1);
    }
    /* clean closed, not fatal */
    CHECK(wake_rc == MOQ_ERR_CLOSED);
    CHECK(wait_spins < SPIN_MAX);
    CHECK(wait_rc == MOQ_ERR_CLOSED);
    CHECK(!fatal);

    if (failures == before)
        printf("PASS: pump_exit_retains_child(ack_first=%d)\n", (int)ack_first);
}

/* --- 3. control: stop, ordered against a held callback --------------------- */

struct stop_arg {
    moq_msquic_managed_t *m;
    moq_result_t rc;
};

static void *stop_thread(void *arg)
{
    struct stop_arg *a = arg;

    a->rc = moq_msquic_managed_stop(a->m);
    return NULL;
}

/*
 * The order is built, not waited for. Every step below is a fact one side
 * publishes and the other side blocks on:
 *
 *   1. the callback transfers and acknowledges the terminal, then PARKS inside
 *      its pump — the doorbell still owns lane->mu, so nothing else can take it;
 *   2. stop runs on a helper thread: it publishes its facade flag and then
 *      blocks on that same lane mutex;
 *   3. the seam fires between those two acts, so the test learns the flag is
 *      latched without inspecting private state and without a timeout;
 *   4. only then is the callback released, so the pump necessarily returns into
 *      a doorbell for which stop is already visible.
 *
 * That is the state the new post-pump reap site must decline. The child is
 * eligible — acknowledged and transport-quiesced — so an unguarded site would
 * take it there; the guarded one leaves it to stop's forced cleanup, which runs
 * after the doorbell has been joined and never fires the reap hook.
 */
static void t_stop_skips_the_fairness_reap(void)
{
    int before = failures;
    struct fx f;
    struct rig r;
    struct stop_arg sa;
    pthread_t th;

    fx_init(&f);
    atomic_store(&f.wake_in_hold, RC_UNSET);
    /* running before the child exists: the first pump acknowledges it */
    atomic_store(&f.phase, PH_RUN);
    if (!rig_up(&r, &f, 1, stop_pump)) {
        CHECK(0 && "lanes-only facade");
        rig_down(&r);
        atomic_store(&g_fx, NULL);
        return;
    }

    bool placed = moq_msq_test_lane_inject_terminal_child(r.lane, CLOSE_CODE);

    /* 1. parked, with lane->mu held. Nothing below may touch that mutex until
     * the release, so no lane_wake() and no lane-scoped seam here. */
    bool held_ok = bar_wait(&g_bar.held, HOLD_GUARD_US);
    int held_at = atomic_load(&f.held_at_pump);
    int closed_at_hold = atomic_load(&f.closed_polled);
    int ack_at_hold = atomic_load(&f.ack_rc);
    int reaps_at_hold = atomic_load(&f.reaps_at_hold);

    /* 2. stop from outside every callback; 3. wait for its own latch fact */
    sa.m = r.m;
    sa.rc = RC_UNSET;
    atomic_store(&g_stop_m, r.m);
    moq_msq_test_stop_latched = stop_latched_hook;
    bool spawned = pthread_create(&th, NULL, stop_thread, &sa) == 0;
    bool latched = spawned && bar_wait(&g_bar.stop_latched, HOLD_GUARD_US);
    int pumps_at_latch = atomic_load(&f.pumps);
    int reaps_at_latch = atomic_load(&f.reaps);

    /* 4. release: the pump returns into a stop-latched doorbell */
    bar_set(&g_bar.release);
    if (spawned)
        pthread_join(th, NULL);
    moq_msq_test_stop_latched = NULL;
    atomic_store(&g_stop_m, NULL);

    int pumps_after_stop = atomic_load(&f.pumps);
    int reaps_after_stop = atomic_load(&f.reaps);
    size_t conns_after_stop = moq_msquic_managed_conn_count(r.m);

    /* every doorbell is joined: hammer the lane and prove the callback count
     * never moves again */
    for (int i = 0; i < 64; i++) {
        (void)moq_msquic_lane_wake(r.lane);
        (void)moq_msquic_managed_wait(r.m, WAIT_US);
    }
    int pumps_settled = atomic_load(&f.pumps);
    bool fatal = moq_msquic_managed_is_fatal(r.m);

    rig_down(&r);
    atomic_store(&g_fx, NULL);

    printf("STOP-ORDERED: placed=%d held=%d held_at=%d closed=%d ack_rc=%d "
           "reaps_at_hold=%d spawned=%d latched=%d pumps_at_latch=%d "
           "reaps_at_latch=%d wake_in_hold=%d hold_expired=%d stop_rc=%d "
           "pumps_after=%d reaps_after=%d pumps_settled=%d conns_after=%zu "
           "fatal=%d\n",
           (int)placed, (int)held_ok, held_at, closed_at_hold, ack_at_hold,
           reaps_at_hold, (int)spawned, (int)latched, pumps_at_latch,
           reaps_at_latch, atomic_load(&f.wake_in_hold),
           atomic_load(&f.hold_expired), sa.rc, pumps_after_stop,
           reaps_after_stop, pumps_settled, conns_after_stop, (int)fatal);

    CHECK(placed);
    /* the ordering itself, with no guard expiry anywhere */
    CHECK(held_ok);
    CHECK(spawned);
    CHECK(latched);
    CHECK(atomic_load(&f.hold_expired) == 0);
    /* the terminal was transferred and acknowledged BEFORE the release */
    CHECK(held_at == 1);
    CHECK(closed_at_hold == 1);
    CHECK(ack_at_hold == MOQ_OK);
    CHECK(reaps_at_hold == 0);
    /* stop was latched while that callback was still held: no further callback
     * had run, and the held callback itself read the facade flag back */
    CHECK(pumps_at_latch == held_at);
    CHECK(reaps_at_latch == 0);
    CHECK(atomic_load(&f.wake_in_hold) == MOQ_ERR_CLOSED);
    /* THE stop-path assertion: the post-pump fairness site declined the
     * eligible child, so no ordinary reap ever ran on this lane */
    CHECK(reaps_after_stop == 0);
    /* no later application callback, across the release or after stop returned */
    CHECK(pumps_after_stop == held_at);
    CHECK(pumps_settled == held_at);
    /* forced cleanup removed the child and released the reserve, exactly once */
    CHECK(sa.rc == MOQ_OK);
    CHECK(conns_after_stop == 0);
    CHECK(!fatal);

    if (failures == before)
        printf("PASS: stop_skips_the_fairness_reap\n");
}

/* --- 4. controls: facade stop versus continuously armed work -------------- */

static void t_stop_before_pump_entry(void)
{
    int before = failures;
    struct fx f;
    struct rig r;

    fx_init(&f);
    atomic_store(&f.phase, PH_RUN);
    if (!rig_up(&r, &f, 1, stop_rearm_pump)) {
        CHECK(0 && "lanes-only facade");
        rig_down(&r);
        atomic_store(&g_fx, NULL);
        return;
    }

    moq_result_t stop_rc = moq_msquic_managed_stop(r.m);
    moq_result_t wake_rc = moq_msquic_managed_wake(r.m);
    moq_result_t wait_rc = moq_msquic_managed_wait(r.m, 0);
    moq_msquic_lane_stats_t st;
    bool stats_ok =
        moq_msquic_lane_get_stats(r.lane, &st, sizeof(st)) == MOQ_OK;
    bool fatal = moq_msquic_managed_is_fatal(r.m);

    rig_down(&r);
    atomic_store(&g_fx, NULL);

    printf("STOP-BEFORE-PUMP: stop=%d wake=%d wait=%d pumps=%d "
           "stats=%d sweeps=%llu fatal=%d\n",
           stop_rc, wake_rc, wait_rc, atomic_load(&f.pumps), (int)stats_ok,
           stats_ok ? (unsigned long long)st.pump_sweeps : 0, (int)fatal);

    CHECK(stop_rc == MOQ_OK);
    CHECK(wake_rc == MOQ_ERR_CLOSED);
    CHECK(wait_rc == MOQ_ERR_CLOSED);
    CHECK(atomic_load(&f.pumps) == 0);
    CHECK(stats_ok);
    if (stats_ok)
        CHECK(st.pump_sweeps == 0);
    CHECK(!fatal);

    if (failures == before)
        printf("PASS: stop_before_pump_entry\n");
}

static void t_stop_during_continuous_rearm(void)
{
    int before = failures;
    struct fx f;
    struct rig r;
    struct stop_arg sa;
    pthread_t th;

    fx_init(&f);
    atomic_store(&f.phase, PH_RUN);
    atomic_store(&f.wake_in_hold, RC_UNSET);
    if (!rig_up(&r, &f, 1, stop_rearm_pump)) {
        CHECK(0 && "lanes-only facade");
        rig_down(&r);
        atomic_store(&g_fx, NULL);
        return;
    }

    (void)moq_msquic_managed_wake(r.m);
    bool held = bar_wait(&g_bar.held, HOLD_GUARD_US);
    int pumps_at_hold = atomic_load(&f.pumps);
    int rearms_at_hold = atomic_load(&f.churn_wakes);

    sa.m = r.m;
    sa.rc = RC_UNSET;
    atomic_store(&g_stop_m, r.m);
    moq_msq_test_stop_latched = stop_latched_hook;
    bool spawned = held && pthread_create(&th, NULL, stop_thread, &sa) == 0;
    bool latched = spawned && bar_wait(&g_bar.stop_latched, HOLD_GUARD_US);
    if (spawned)
        pthread_join(th, NULL);
    moq_msq_test_stop_latched = NULL;
    atomic_store(&g_stop_m, NULL);

    int pumps_after = atomic_load(&f.pumps);
    int rearms_after = atomic_load(&f.churn_wakes);
    moq_msquic_lane_stats_t st;
    bool stats_ok =
        moq_msquic_lane_get_stats(r.lane, &st, sizeof(st)) == MOQ_OK;
    moq_result_t wake_rc = moq_msquic_managed_wake(r.m);
    moq_result_t wait_rc = MOQ_OK;
    int wait_spins = 0;

    /* The completed pump may leave a coalesced activity latch. Drain that
     * public notification before asking for the facade's terminal result. */
    for (; wait_spins < SPIN_MAX && wait_rc == MOQ_OK; wait_spins++)
        wait_rc = moq_msquic_managed_wait(r.m, 0);
    bool fatal = moq_msquic_managed_is_fatal(r.m);

    rig_down(&r);
    atomic_store(&g_fx, NULL);

    printf("STOP-REARM: held=%d pumps_at=%d rearmed_at=%d spawned=%d "
           "latched=%d hold_expired=%d wake_in_hold=%d stop=%d pumps=%d "
           "rearmed=%d nonzero=%d stats=%d sweeps=%llu wake=%d wait=%d/%d "
           "fatal=%d\n",
           (int)held, pumps_at_hold, rearms_at_hold, (int)spawned,
           (int)latched, atomic_load(&f.hold_expired),
           atomic_load(&f.wake_in_hold), sa.rc, pumps_after, rearms_after,
           atomic_load(&f.nonzero_returns), (int)stats_ok,
           stats_ok ? (unsigned long long)st.pump_sweeps : 0, wake_rc,
           wait_rc, wait_spins, (int)fatal);

    CHECK(held);
    CHECK(pumps_at_hold == STOP_REARM_PUMPS);
    CHECK(rearms_at_hold == STOP_REARM_PUMPS);
    CHECK(spawned);
    CHECK(latched);
    CHECK(atomic_load(&f.hold_expired) == 0);
    CHECK(atomic_load(&f.wake_in_hold) == MOQ_ERR_CLOSED);
    CHECK(sa.rc == MOQ_OK);
    /* The already-armed pass is suppressed by the facade stop observed at the
     * loop top. No scheduler timing or mutex-acquisition race is the oracle. */
    CHECK(pumps_after == STOP_REARM_PUMPS);
    CHECK(rearms_after == STOP_REARM_PUMPS);
    CHECK(atomic_load(&f.nonzero_returns) == 0);
    CHECK(stats_ok);
    if (stats_ok)
        CHECK(st.pump_sweeps == STOP_REARM_PUMPS);
    CHECK(wake_rc == MOQ_ERR_CLOSED);
    CHECK(wait_spins < SPIN_MAX);
    CHECK(wait_rc == MOQ_ERR_CLOSED);
    CHECK(!fatal);

    if (failures == before)
        printf("PASS: stop_during_continuous_rearm\n");
}

int main(void)
{
    static const QUIC_API_TABLE *lib_pin;

    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }

    t_reap_is_fair_under_continuous_work();
    t_pump_exit_retains_child(false);
    t_pump_exit_retains_child(true);
    t_stop_skips_the_fairness_reap();
    t_stop_before_pump_entry();
    t_stop_during_continuous_rearm();

    if (failures == 0)
        printf("PASS: msquic_reap_fairness\n");
    else
        fprintf(stderr, "FAIL: msquic_reap_fairness (%d)\n", failures);
    return failures;
}
