/*
 * Cross-lane doorbell wake delivery (the bell): a newly armed cross-lane
 * wake cycle must promptly wake the destination lane's doorbell instead of
 * being consumed at the 200 ms idle cap. Correctness is asserted on a
 * STRUCTURAL fact, not a wall-clock threshold: the pre-wait hook pins the
 * doorbell in the exact check->wait window, and the delivering idle wait
 * must return because the bell generation advanced (a MOQ_MSQUIC_TESTING
 * counter), not because the cap expired — the silent-flag path returns via
 * the cap and fails that assertion deterministically. Also covers exact
 * cross-lane consumption deltas, cycle coalescing, same-lane / external /
 * cross-lane cause classification, bidirectional wakes without lock-order
 * inversion, wake activity racing stop(), and the callback-context stats
 * rule. Wall-clock deltas are printed as diagnostics only. Compiles the
 * adapter + managed sources directly (test seams on); links no production
 * adapter library.
 */

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
#include <unistd.h>

/* the doorbell pre-wait hook (msquic_managed.c, MOQ_MSQUIC_TESTING only) */
extern void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);
/* idle waits this lane returned from on a bell generation advance (a ring
 * delivered the wake) rather than the deadline expiring — the structural
 * "delivered promptly by the bell, not the idle cap" discriminator
 * (msquic_managed.c, MOQ_MSQUIC_TESTING only). */
extern uint64_t moq_msq_test_bell_wakes(moq_msquic_managed_lane_t *lane);

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static uint64_t now_us(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000u + (uint64_t)ts.tv_nsec / 1000u;
}

static moq_result_t stats_of(moq_msquic_managed_lane_t *lane,
                             moq_msquic_lane_stats_t *st)
{
    memset(st, 0, sizeof(*st));
    return moq_msquic_lane_get_stats(lane, st, sizeof(*st));
}

/* --- pump dispatcher ------------------------------------------------------ */

enum {
    MODE_NONE = 0,
    MODE_CROSS_ONE,     /* lane 0's pump wakes lane 1 once (budget-gated)  */
    MODE_CROSS_TRIPLE,  /* lane 0's pump wakes lane 1 three times in one
                         * pump window (budget-gated)                      */
    MODE_PINGPONG,      /* each lane's pump wakes the other while its own
                         * budget lasts                                    */
    MODE_CTX,           /* lane 0's pump probes the stats context rule     */
    MODE_SELF,          /* lane 0's pump wakes its OWN lane (same-lane arm) */
};

static _Atomic int g_mode;
static _Atomic int g_budget_a;  /* lane 0's action budget */
static _Atomic int g_budget_b;  /* lane 1's action budget */
/* written on the doorbell thread, polled by main: atomics */
static _Atomic int g_rc_own;
static _Atomic int g_rc_other;
#define RC_UNSET INT32_MIN

/* consume one unit of a budget; false when exhausted */
static bool budget_take(_Atomic int *b)
{
    int v = atomic_load(b);

    while (v > 0)
        if (atomic_compare_exchange_weak(b, &v, v - 1))
            return true;
    return false;
}

static int pump_dispatch(moq_msquic_managed_t *m,
                         moq_msquic_managed_lane_t *lane, uint64_t t,
                         void *user)
{
    (void)t; (void)user;
    uint32_t idx = moq_msquic_lane_index(lane);
    int mode = atomic_load(&g_mode);

    if (mode == MODE_CROSS_ONE && idx == 0 && budget_take(&g_budget_a)) {
        CHECK(moq_msquic_lane_wake(moq_msquic_managed_lane(m, 1)) ==
              MOQ_OK);
    } else if (mode == MODE_CROSS_TRIPLE && idx == 0 &&
               budget_take(&g_budget_a)) {
        moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);

        CHECK(moq_msquic_lane_wake(l1) == MOQ_OK);
        CHECK(moq_msquic_lane_wake(l1) == MOQ_OK);
        CHECK(moq_msquic_lane_wake(l1) == MOQ_OK);
    } else if (mode == MODE_PINGPONG) {
        if (idx == 0 && budget_take(&g_budget_a))
            CHECK(moq_msquic_lane_wake(moq_msquic_managed_lane(m, 1)) ==
                  MOQ_OK);
        if (idx == 1 && budget_take(&g_budget_b))
            CHECK(moq_msquic_lane_wake(moq_msquic_managed_lane(m, 0)) ==
                  MOQ_OK);
    } else if (mode == MODE_CTX && idx == 0 && budget_take(&g_budget_a)) {
        moq_msquic_lane_stats_t st;

        atomic_store(&g_rc_own,
                     (int)stats_of(moq_msquic_managed_lane(m, 0), &st));
        atomic_store(&g_rc_other,
                     (int)stats_of(moq_msquic_managed_lane(m, 1), &st));
    } else if (mode == MODE_SELF && idx == 0 && budget_take(&g_budget_a)) {
        /* wake our OWN lane from inside its pump: the same-lane arm */
        CHECK(moq_msquic_lane_wake(lane) == MOQ_OK);
    }
    return 0;
}

/* --- pre-wait hook plumbing ----------------------------------------------- */

/* One-shot park: the first time the TARGET lane's doorbell reaches the
 * check->wait window, the hook reports "in window" and holds the doorbell
 * there (bounded) until the test releases it, then disarms itself. */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    moq_msquic_managed_lane_t *_Atomic target;
    bool in_window;
    bool release;
} g_hook = {
    PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, NULL, false, false
};

static void hook_prewait(moq_msquic_managed_lane_t *lane)
{
    if (atomic_load(&g_hook.target) != lane)
        return;
    pthread_mutex_lock(&g_hook.mu);
    g_hook.in_window = true;
    pthread_cond_broadcast(&g_hook.cv);
    struct timespec abs;

    clock_gettime(CLOCK_REALTIME, &abs);
    abs.tv_sec += 3;    /* bounded: a lost release cannot hang the suite */
    while (!g_hook.release)
        if (pthread_cond_timedwait(&g_hook.cv, &g_hook.mu, &abs) != 0)
            break;
    g_hook.in_window = false;
    atomic_store(&g_hook.target, NULL);   /* one-shot */
    pthread_mutex_unlock(&g_hook.mu);
}

static void hook_arm(moq_msquic_managed_lane_t *lane)
{
    pthread_mutex_lock(&g_hook.mu);
    g_hook.in_window = false;
    g_hook.release = false;
    pthread_mutex_unlock(&g_hook.mu);
    atomic_store(&g_hook.target, lane);
}

static bool hook_await_window(void)
{
    struct timespec abs;
    bool in;

    clock_gettime(CLOCK_REALTIME, &abs);
    abs.tv_sec += 3;
    pthread_mutex_lock(&g_hook.mu);
    while (!g_hook.in_window)
        if (pthread_cond_timedwait(&g_hook.cv, &g_hook.mu, &abs) != 0)
            break;
    in = g_hook.in_window;
    pthread_mutex_unlock(&g_hook.mu);
    return in;
}

static void hook_release(void)
{
    pthread_mutex_lock(&g_hook.mu);
    g_hook.release = true;
    pthread_cond_broadcast(&g_hook.cv);
    pthread_mutex_unlock(&g_hook.mu);
}

/* --- rig ------------------------------------------------------------------ */

static moq_msquic_managed_t *mk_server(const char *cert, const char *key)
{
    moq_msquic_managed_cfg_t cfg;

    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.host = "127.0.0.1";
    cfg.port = 0;
    cfg.cert_path = cert;
    cfg.key_path = key;
    cfg.max_connections = 4;
    cfg.lane_count = 2;
    cfg.on_lane_pump = pump_dispatch;
    cfg.on_lane_pump_user = NULL;
    moq_msquic_managed_t *m = NULL;

    if (moq_msquic_managed_create(&cfg, &m) != MOQ_OK)
        return NULL;
    atomic_store(&g_mode, MODE_NONE);
    atomic_store(&g_budget_a, 0);
    atomic_store(&g_budget_b, 0);
    return m;
}

/* poll an l-stats predicate with a bounded window; true when it held */
#define POLL_US 1000
#define POLL_TRIES 2500
static bool poll_until(moq_msquic_managed_lane_t *lane,
                       bool (*pred)(const moq_msquic_lane_stats_t *,
                                    const moq_msquic_lane_stats_t *),
                       const moq_msquic_lane_stats_t *pre,
                       moq_msquic_lane_stats_t *out)
{
    for (int i = 0; i < POLL_TRIES; i++) {
        if (stats_of(lane, out) != MOQ_OK)
            return false;
        if (pred(out, pre))
            return true;
        usleep(POLL_US);
    }
    return false;
}

static bool pred_one_more_sample(const moq_msquic_lane_stats_t *st,
                                 const moq_msquic_lane_stats_t *pre)
{
    return st->wake_to_pump_samples == pre->wake_to_pump_samples + 1;
}

static bool pred_one_more_cross(const moq_msquic_lane_stats_t *st,
                                const moq_msquic_lane_stats_t *pre)
{
    return st->wakes_cross_lane == pre->wakes_cross_lane + 1;
}

static bool pred_one_more_same(const moq_msquic_lane_stats_t *st,
                               const moq_msquic_lane_stats_t *pre)
{
    return st->wakes_same_lane == pre->wakes_same_lane + 1;
}

/* --- (1) the check->wait race window -------------------------------------- */

/* Park the destination doorbell in the exact window between its no-work
 * predicate and its sleep; arm a cross-lane wake there; prove the pump is
 * driven by a bell generation advance, not the idle cap. The silent-flag
 * behavior leaves the generation unchanged, so its delivering wait returns
 * on the cap timeout and the bell-wake counter never moves — a structural
 * failure, independent of host load. */
static void t_race_window(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key);

    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);
    moq_msquic_lane_stats_t pre, mid, post;

    CHECK(stats_of(l1, &pre) == MOQ_OK);
    atomic_store(&g_mode, MODE_CROSS_ONE);
    hook_arm(l1);
    CHECK(hook_await_window());   /* l1's doorbell is parked in the race
                                   * window: predicate seen empty, not yet
                                   * asleep */
    /* fire the cross-lane wake from lane 0's callback while l1 is parked */
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);
    CHECK(poll_until(l1, pred_one_more_cross, &pre, &mid));
    /* still parked: the cycle is armed but nothing consumed it */
    CHECK(mid.pump_sweeps == pre.pump_sweeps);
    CHECK(mid.wake_to_pump_samples == pre.wake_to_pump_samples);

    uint64_t bell_pre = moq_msq_test_bell_wakes(l1);
    uint64_t t0 = now_us();

    hook_release();
    CHECK(poll_until(l1, pred_one_more_sample, &pre, &post));

    uint64_t wall = now_us() - t0;
    uint64_t sample = post.wake_to_pump_total_us - pre.wake_to_pump_total_us;
    uint64_t bell_post = moq_msq_test_bell_wakes(l1);

    /* The structural discriminator: the delivering idle wait returned
     * because the bell generation advanced — exactly one such wake for this
     * cross-lane delivery. The silent-flag path returns via the cap timeout
     * and never moves this counter, so it fails here regardless of host
     * load. (wall/sample are diagnostics only.) */
    CHECK(bell_post == bell_pre + 1);
    CHECK(post.pump_sweeps >= pre.pump_sweeps + 1);
    CHECK(post.wakes_cross_lane == pre.wakes_cross_lane + 1);
    printf("RACE_WINDOW,release_to_pump_us=%llu,sample_us=%llu,bell_wakes=%llu\n",
           (unsigned long long)wall, (unsigned long long)sample,
           (unsigned long long)(bell_post - bell_pre));

    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: wake armed in the check->wait window pumps promptly\n");
}

/* --- (2) cross-lane consumption: exact per-cycle deltas ------------------- */

static void t_cross_consumption(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key);

    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);
    uint64_t delays[10];

    atomic_store(&g_mode, MODE_CROSS_ONE);
    for (int it = 0; it < 10; it++) {
        moq_msquic_lane_stats_t pre, post;

        CHECK(stats_of(l1, &pre) == MOQ_OK);
        atomic_store(&g_budget_a, 1);
        CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);
        CHECK(poll_until(l1, pred_one_more_sample, &pre, &post));
        /* exact deltas: one armed cycle, one consuming pump, one sample */
        CHECK(post.wakes_cross_lane == pre.wakes_cross_lane + 1);
        CHECK(post.pump_sweeps == pre.pump_sweeps + 1);
        CHECK(post.wake_to_pump_samples == pre.wake_to_pump_samples + 1);
        delays[it] = post.wake_to_pump_total_us -
                     pre.wake_to_pump_total_us;
        /* delay is a printed diagnostic only; correctness here is the exact
         * per-cycle accounting above, and the prompt-delivery mechanism is
         * proven structurally in t_race_window. */
        printf("CROSS_WAKE,iter=%d,delay_us=%llu\n", it,
               (unsigned long long)delays[it]);
    }

    uint64_t mn = UINT64_MAX, mx = 0, sum = 0;

    for (int it = 0; it < 10; it++) {
        if (delays[it] < mn)
            mn = delays[it];
        if (delays[it] > mx)
            mx = delays[it];
        sum += delays[it];
    }
    printf("CROSS_WAKE_DIST,n=10,min_us=%llu,avg_us=%llu,max_us=%llu\n",
           (unsigned long long)mn, (unsigned long long)(sum / 10),
           (unsigned long long)mx);

    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: cross-lane wake consumption (exact per-cycle "
               "deltas x10)\n");
}

/* --- (3) bidirectional wakes: no deadlock, both sides pump ---------------- */

static void *seed_wake(void *arg)
{
    moq_msquic_lane_wake(arg);
    return NULL;
}

static void t_bidirectional(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key);

    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);
    moq_msquic_lane_stats_t pre0, pre1, post0, post1;

    CHECK(stats_of(l0, &pre0) == MOQ_OK);
    CHECK(stats_of(l1, &pre1) == MOQ_OK);
    atomic_store(&g_budget_a, 3);
    atomic_store(&g_budget_b, 3);
    atomic_store(&g_mode, MODE_PINGPONG);

    /* seed both lanes near-simultaneously from two app threads; each
     * pump then cross-wakes the other from inside its own locked lane
     * context until its budget drains */
    pthread_t ta, tb;

    CHECK(pthread_create(&ta, NULL, seed_wake, l0) == 0);
    CHECK(pthread_create(&tb, NULL, seed_wake, l1) == 0);
    pthread_join(ta, NULL);
    pthread_join(tb, NULL);

    /* both budgets must drain: 3 cross-wakes landed on each side (the
     * bounded poll doubles as the no-deadlock proof) */
    bool done = false;

    for (int i = 0; i < POLL_TRIES && !done; i++) {
        CHECK(stats_of(l0, &post0) == MOQ_OK);
        CHECK(stats_of(l1, &post1) == MOQ_OK);
        done = post0.wakes_cross_lane == pre0.wakes_cross_lane + 3 &&
               post1.wakes_cross_lane == pre1.wakes_cross_lane + 3;
        if (!done)
            usleep(POLL_US);
    }
    CHECK(done);
    /* consumed: at least one sample each; cycles may coalesce but can
     * never exceed the explicit requests (1 external seed + 3 cross) */
    CHECK(post0.wake_to_pump_samples > pre0.wake_to_pump_samples);
    CHECK(post1.wake_to_pump_samples > pre1.wake_to_pump_samples);
    CHECK(post0.wake_to_pump_samples <= pre0.wake_to_pump_samples + 4);
    CHECK(post1.wake_to_pump_samples <= pre1.wake_to_pump_samples + 4);
    CHECK(post0.pump_sweeps > pre0.pump_sweeps);
    CHECK(post1.pump_sweeps > pre1.pump_sweeps);

    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: bidirectional cross-wakes drain without deadlock\n");
}

/* --- (4) coalescing: one open cycle, exact counters ------------------------ */

static void t_coalescing(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key);

    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);
    moq_msquic_lane_stats_t pre, mid, post, fin;

    CHECK(stats_of(l1, &pre) == MOQ_OK);
    atomic_store(&g_mode, MODE_CROSS_TRIPLE);
    hook_arm(l1);
    CHECK(hook_await_window());   /* park l1 so all three requests land on
                                   * ONE open cycle deterministically */
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);

    bool armed = false;

    for (int i = 0; i < POLL_TRIES && !armed; i++) {
        CHECK(stats_of(l1, &mid) == MOQ_OK);
        armed = mid.wakes_cross_lane == pre.wakes_cross_lane + 3;
        if (!armed)
            usleep(POLL_US);
    }
    CHECK(armed);
    /* three requests, one open cycle: exactly two found it open */
    CHECK(mid.wakes_coalesced == pre.wakes_coalesced + 2);
    CHECK(mid.wake_to_pump_samples == pre.wake_to_pump_samples);

    hook_release();
    CHECK(poll_until(l1, pred_one_more_sample, &pre, &post));
    /* exactly ONE consuming pump, ONE sample for the whole cycle */
    CHECK(post.wake_to_pump_samples == pre.wake_to_pump_samples + 1);
    CHECK(post.wakes_coalesced == pre.wakes_coalesced + 2);

    /* a later cycle arms and wakes again */
    atomic_store(&g_mode, MODE_CROSS_ONE);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);
    CHECK(poll_until(l1, pred_one_more_sample, &post, &fin));
    CHECK(fin.wake_to_pump_samples == pre.wake_to_pump_samples + 2);
    CHECK(fin.wakes_cross_lane == pre.wakes_cross_lane + 4);
    CHECK(fin.wakes_coalesced == pre.wakes_coalesced + 2);

    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: coalesced requests = one cycle, one sample; "
               "later cycle re-arms\n");
}

/* --- (5) mixed wake causes stay distinguishable ---------------------------- */

static void t_mixed_causes(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key);

    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);
    moq_msquic_lane_stats_t parked, post, ext;

    /* a hook-held cross wake: snapshot while parked so the consume window
     * is tiny — its prompt service must NOT read as a deadline or idle-cap
     * event, or as any other arm */
    atomic_store(&g_mode, MODE_CROSS_ONE);
    hook_arm(l1);
    CHECK(hook_await_window());

    moq_msquic_lane_stats_t pre;

    CHECK(stats_of(l1, &pre) == MOQ_OK);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);
    CHECK(poll_until(l1, pred_one_more_cross, &pre, &parked));
    hook_release();
    CHECK(poll_until(l1, pred_one_more_sample, &pre, &post));
    CHECK(post.wakes_cross_lane == pre.wakes_cross_lane + 1);
    CHECK(post.wakes_same_lane == pre.wakes_same_lane);
    CHECK(post.wakes_external == pre.wakes_external);
    CHECK(post.deadline_sweeps == pre.deadline_sweeps);
    CHECK(post.idle_cap_wakes == pre.idle_cap_wakes);

    /* an external wake stays an external wake */
    CHECK(moq_msquic_lane_wake(l1) == MOQ_OK);
    CHECK(poll_until(l1, pred_one_more_sample, &post, &ext));
    CHECK(ext.wakes_external == post.wakes_external + 1);
    CHECK(ext.wakes_cross_lane == post.wakes_cross_lane);

    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: wake causes stay distinguishable (no idle/deadline "
               "misclassification)\n");
}

/* --- (5b) same-lane arm counts as same-lane, not cross/external ------------ */

static void t_same_lane_cause(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key);

    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_lane_stats_t pre, post;

    CHECK(stats_of(l0, &pre) == MOQ_OK);
    atomic_store(&g_mode, MODE_SELF);
    atomic_store(&g_budget_a, 1);
    /* one external wake triggers lane 0's pump; the pump then wakes its OWN
     * lane once — the same-lane arm, on the doorbell thread with lane->mu
     * already held. */
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);
    CHECK(poll_until(l0, pred_one_more_same, &pre, &post));
    /* the self-wake is counted as same-lane; the trigger as external;
     * nothing lands on cross-lane. */
    CHECK(post.wakes_same_lane == pre.wakes_same_lane + 1);
    CHECK(post.wakes_external == pre.wakes_external + 1);
    CHECK(post.wakes_cross_lane == pre.wakes_cross_lane);

    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: same-lane arm counts as same-lane, not cross/external\n");
}

/* --- (6) wake activity racing stop() --------------------------------------- */

static _Atomic int g_hammer_stop;

static void *hammer_main(void *arg)
{
    moq_msquic_managed_lane_t *lane = arg;

    while (!atomic_load(&g_hammer_stop)) {
        moq_msquic_lane_wake(lane);
        usleep(100);
    }
    return NULL;
}

static void t_wake_vs_stop(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key);

    CHECK(m != NULL);
    if (m == NULL)
        return;
    pthread_t ha, hb;

    atomic_store(&g_hammer_stop, 0);
    CHECK(pthread_create(&ha, NULL, hammer_main,
                         moq_msquic_managed_lane(m, 0)) == 0);
    CHECK(pthread_create(&hb, NULL, hammer_main,
                         moq_msquic_managed_lane(m, 1)) == 0);
    usleep(20000);
    /* stop() runs while both lanes are being hammered with wakes: the bell
     * must ring/quiesce cleanly and never strand a doorbell (stop joins the
     * doorbells; a bounded stop return is the no-strand proof). The wakers
     * are stopped and JOINED before destroy(), so this exercises
     * wake-vs-stop only — the public API does not permit lane_wake() to race
     * destroy(), and this test does not attempt it. */
    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    atomic_store(&g_hammer_stop, 1);
    pthread_join(ha, NULL);
    pthread_join(hb, NULL);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: wake activity racing stop() quiesces cleanly\n");
}

/* --- (7) callback-context stats rule unchanged ------------------------------ */

static void t_context_rule(const char *cert, const char *key)
{
    int before = failures;
    moq_msquic_managed_t *m = mk_server(cert, key);

    CHECK(m != NULL);
    if (m == NULL)
        return;
    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_lane_stats_t pre, post;

    CHECK(stats_of(l0, &pre) == MOQ_OK);   /* external read: valid */
    atomic_store(&g_rc_own, RC_UNSET);
    atomic_store(&g_rc_other, RC_UNSET);
    atomic_store(&g_mode, MODE_CTX);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(l0) == MOQ_OK);

    bool probed = false;

    for (int i = 0; i < POLL_TRIES && !probed; i++) {
        probed = atomic_load(&g_rc_own) != RC_UNSET &&
                 atomic_load(&g_rc_other) != RC_UNSET;
        if (!probed)
            usleep(POLL_US);
    }
    CHECK(probed);
    CHECK(atomic_load(&g_rc_own) == (int)MOQ_OK);
    CHECK(atomic_load(&g_rc_other) == (int)MOQ_ERR_WRONG_STATE);
    CHECK(stats_of(l0, &post) == MOQ_OK);       /* external still valid   */

    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: callback-context stats rule unchanged\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: %s <cert.pem> <key.pem>\n", argv[0]);
        return 2;
    }
    const char *cert = argv[1], *key = argv[2];

    /* one library reference for the whole process (matches the other
     * managed suites). Never released; reachable until exit. */
    static const QUIC_API_TABLE *lib_pin;

    if (QUIC_FAILED(MsQuicOpen2(&lib_pin))) {
        fprintf(stderr, "MsQuicOpen2 failed\n");
        return 2;
    }
    moq_msq_test_prewait = hook_prewait;

    t_race_window(cert, key);
    t_cross_consumption(cert, key);
    t_bidirectional(cert, key);
    t_coalescing(cert, key);
    t_mixed_causes(cert, key);
    t_same_lane_cause(cert, key);
    t_wake_vs_stop(cert, key);
    t_context_rule(cert, key);

    if (failures == 0)
        printf("PASS: msquic_wake\n");
    else
        fprintf(stderr, "FAIL: msquic_wake (%d)\n", failures);
    return failures;
}
