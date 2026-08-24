/*
 * Deterministic managed-MsQuic doorbell wake proofs.
 *
 * NETWORK-FREE. The MOQ_MSQUIC_TESTING constructor builds only the facade's
 * lanes and doorbell threads: no registration, configuration, credential,
 * listener, socket, or certificate. Every ordering assertion uses an explicit
 * callback, pre-wait, or stop-publication rendezvous. Timed waits are only
 * fail-closed hang guards around named predicates; elapsed time is never an
 * acceptance condition.
 *
 * Coverage retained from the former listener-backed test: bell generation and
 * the check->wait race, exact same/cross/external wake accounting, coalescing,
 * bidirectional cross-wakes without lock inversion, callback confinement,
 * wake-after-stop ordering, and invalid argument/result classification.
 * Compiles the adapter and managed sources directly with test seams enabled;
 * no test seam ships in the production adapter library.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <moq/msquic_managed.h>

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

/* MOQ_MSQUIC_TESTING-only seams. */
extern void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);
extern uint64_t moq_msq_test_bell_generation(
    moq_msquic_managed_lane_t *lane);
extern uint64_t moq_msq_test_bell_wakes(moq_msquic_managed_lane_t *lane);
extern void (*moq_msq_test_stop_latched)(moq_msquic_managed_t *m);
extern moq_result_t moq_msq_test_managed_create_lanes_only(
    const moq_msquic_managed_cfg_t *cfg, moq_msquic_managed_t **out);

static int failures;

#define CHECK(expr)                                                         \
    do {                                                                    \
        if (!(expr)) {                                                      \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__,      \
                    #expr);                                                 \
            failures++;                                                     \
        }                                                                   \
    } while (0)

enum { LANE_COUNT = 2 };
#define GUARD_US (3u * 1000u * 1000u)

static void guard_deadline(struct timespec *abs)
{
    clock_gettime(CLOCK_REALTIME, abs);
    abs->tv_sec += (time_t)(GUARD_US / 1000000u);
    abs->tv_nsec += (long)(GUARD_US % 1000000u) * 1000;
    if (abs->tv_nsec >= 1000000000L) {
        abs->tv_sec += 1;
        abs->tv_nsec -= 1000000000L;
    }
}

static moq_result_t stats_of(moq_msquic_managed_lane_t *lane,
                             moq_msquic_lane_stats_t *st)
{
    memset(st, 0, sizeof(*st));
    return moq_msquic_lane_get_stats(lane, st, sizeof(*st));
}

/* Assert the exact generation delta while the source callback is known to
 * have completed. On a missing-ring mutant, issue one EXTERNAL wake as a
 * deterministic cleanup ring. The caller declares that extra wake in its
 * cause/coalescing oracle; elapsed time never advances the test. */
static bool cross_ring_or_cleanup(moq_msquic_managed_lane_t *lane,
                                  uint64_t generation_pre,
                                  uint64_t expected_rings)
{
    uint64_t generation_post = moq_msq_test_bell_generation(lane);
    bool cleanup = generation_post != generation_pre + expected_rings;

    CHECK(generation_post == generation_pre + expected_rings);
    if (cleanup) {
        CHECK(moq_msquic_lane_wake(lane) == MOQ_OK);
        CHECK(moq_msq_test_bell_generation(lane) == generation_post + 1);
    }
    return cleanup;
}

/* Callback-completion rendezvous. The callback publishes only after all work
 * for that pass has been issued. Main never calls an adapter function while
 * holding this mutex, so it cannot invert against a doorbell's lane lock. */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    uint64_t pumps[LANE_COUNT];
} g_progress = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void progress_reset(void)
{
    pthread_mutex_lock(&g_progress.mu);
    memset(g_progress.pumps, 0, sizeof(g_progress.pumps));
    pthread_mutex_unlock(&g_progress.mu);
}

static void progress_note(uint32_t lane)
{
    pthread_mutex_lock(&g_progress.mu);
    if (lane < LANE_COUNT)
        g_progress.pumps[lane]++;
    pthread_cond_broadcast(&g_progress.cv);
    pthread_mutex_unlock(&g_progress.mu);
}

static uint64_t progress_pumps(uint32_t lane)
{
    uint64_t n = 0;

    pthread_mutex_lock(&g_progress.mu);
    if (lane < LANE_COUNT)
        n = g_progress.pumps[lane];
    pthread_mutex_unlock(&g_progress.mu);
    return n;
}

static bool progress_await_pumps(uint32_t lane, uint64_t want)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_progress.mu);
    while (lane < LANE_COUNT && g_progress.pumps[lane] < want)
        if (pthread_cond_timedwait(&g_progress.cv, &g_progress.mu, &abs) != 0)
            break;
    reached = lane < LANE_COUNT && g_progress.pumps[lane] >= want;
    pthread_mutex_unlock(&g_progress.mu);
    return reached;
}

/* Hold both doorbell callbacks while each owns its own lane mutex, release
 * them to cross-wake together, then keep each callback in the rendezvous
 * until BOTH lock-free cross flags are installed. Thus a missing bell ring
 * cannot strand either side at the idle cap: both doorbells are already
 * running and observe ext_wake when their callbacks return. */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    unsigned arrived;
    unsigned crossed;
    bool release;
    bool expired;
} g_bidir = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void bidir_arm(void)
{
    pthread_mutex_lock(&g_bidir.mu);
    g_bidir.arrived = 0;
    g_bidir.crossed = 0;
    g_bidir.release = false;
    g_bidir.expired = false;
    pthread_mutex_unlock(&g_bidir.mu);
}

static void bidir_before_cross(uint32_t lane)
{
    struct timespec abs;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_bidir.mu);
    g_bidir.arrived |= 1u << lane;
    pthread_cond_broadcast(&g_bidir.cv);
    while (!g_bidir.release)
        if (pthread_cond_timedwait(&g_bidir.cv, &g_bidir.mu, &abs) != 0) {
            g_bidir.expired = true;
            break;
        }
    pthread_mutex_unlock(&g_bidir.mu);
}

static void bidir_after_cross(uint32_t lane)
{
    struct timespec abs;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_bidir.mu);
    g_bidir.crossed |= 1u << lane;
    pthread_cond_broadcast(&g_bidir.cv);
    while (g_bidir.crossed != 3u)
        if (pthread_cond_timedwait(&g_bidir.cv, &g_bidir.mu, &abs) != 0) {
            g_bidir.expired = true;
            break;
        }
    pthread_mutex_unlock(&g_bidir.mu);
}

static bool bidir_await_arrived(void)
{
    struct timespec abs;
    bool arrived;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_bidir.mu);
    while (g_bidir.arrived != 3u)
        if (pthread_cond_timedwait(&g_bidir.cv, &g_bidir.mu, &abs) != 0)
            break;
    arrived = g_bidir.arrived == 3u;
    pthread_mutex_unlock(&g_bidir.mu);
    return arrived;
}

static void bidir_release(void)
{
    pthread_mutex_lock(&g_bidir.mu);
    g_bidir.release = true;
    pthread_cond_broadcast(&g_bidir.cv);
    pthread_mutex_unlock(&g_bidir.mu);
}

static bool bidir_complete(void)
{
    bool complete;

    pthread_mutex_lock(&g_bidir.mu);
    complete = !g_bidir.expired && g_bidir.arrived == 3u &&
               g_bidir.crossed == 3u;
    pthread_mutex_unlock(&g_bidir.mu);
    return complete;
}

/* --- pump dispatcher ----------------------------------------------------- */

enum {
    MODE_NONE = 0,
    MODE_CROSS_ONE,
    MODE_CROSS_TRIPLE,
    MODE_PINGPONG,
    MODE_CTX,
    MODE_SELF,
};

static _Atomic int g_mode;
static _Atomic int g_budget_a;
static _Atomic int g_budget_b;
static _Atomic int g_rc_own;
static _Atomic int g_rc_other;
#define RC_UNSET INT32_MIN

static bool budget_take(_Atomic int *budget)
{
    int v = atomic_load(budget);

    while (v > 0)
        if (atomic_compare_exchange_weak(budget, &v, v - 1))
            return true;
    return false;
}

static int pump_dispatch(moq_msquic_managed_t *m,
                         moq_msquic_managed_lane_t *lane, uint64_t now,
                         void *user)
{
    uint32_t idx = moq_msquic_lane_index(lane);
    int mode = atomic_load(&g_mode);

    (void)now;
    (void)user;
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
        if (idx == 0 && budget_take(&g_budget_a)) {
            bidir_before_cross(idx);
            CHECK(moq_msquic_lane_wake(moq_msquic_managed_lane(m, 1)) ==
                  MOQ_OK);
            bidir_after_cross(idx);
        }
        if (idx == 1 && budget_take(&g_budget_b)) {
            bidir_before_cross(idx);
            CHECK(moq_msquic_lane_wake(moq_msquic_managed_lane(m, 0)) ==
                  MOQ_OK);
            bidir_after_cross(idx);
        }
    } else if (mode == MODE_CTX && idx == 0 && budget_take(&g_budget_a)) {
        moq_msquic_lane_stats_t st;

        atomic_store(&g_rc_own,
                     (int)stats_of(moq_msquic_managed_lane(m, 0), &st));
        atomic_store(&g_rc_other,
                     (int)stats_of(moq_msquic_managed_lane(m, 1), &st));
    } else if (mode == MODE_SELF && idx == 0 && budget_take(&g_budget_a)) {
        CHECK(moq_msquic_lane_wake(lane) == MOQ_OK);
    }
    progress_note(idx);
    return 0;
}

/* --- deterministic pre-wait window -------------------------------------- */

/* Each test using the hook arms lane 1 BEFORE its lanes-only facade is
 * created. Its first doorbell pass is therefore caught before the first idle
 * wait; no idle-cap expiry is needed to reach the race window. */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool armed;
    uint32_t target_index;
    bool in_window;
    bool release;
    bool expired;
} g_hook = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void hook_prewait(moq_msquic_managed_lane_t *lane)
{
    struct timespec abs;

    pthread_mutex_lock(&g_hook.mu);
    if (!g_hook.armed || moq_msquic_lane_index(lane) != g_hook.target_index) {
        pthread_mutex_unlock(&g_hook.mu);
        return;
    }
    g_hook.in_window = true;
    pthread_cond_broadcast(&g_hook.cv);
    guard_deadline(&abs);
    while (!g_hook.release)
        if (pthread_cond_timedwait(&g_hook.cv, &g_hook.mu, &abs) != 0) {
            g_hook.expired = true;
            break;
        }
    g_hook.in_window = false;
    g_hook.armed = false;
    pthread_cond_broadcast(&g_hook.cv);
    pthread_mutex_unlock(&g_hook.mu);
}

static void hook_arm(uint32_t target_index)
{
    pthread_mutex_lock(&g_hook.mu);
    g_hook.armed = true;
    g_hook.target_index = target_index;
    g_hook.in_window = false;
    g_hook.release = false;
    g_hook.expired = false;
    pthread_mutex_unlock(&g_hook.mu);
}

static bool hook_await_window(void)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_hook.mu);
    while (!g_hook.in_window && !g_hook.expired)
        if (pthread_cond_timedwait(&g_hook.cv, &g_hook.mu, &abs) != 0)
            break;
    reached = g_hook.in_window && !g_hook.expired;
    pthread_mutex_unlock(&g_hook.mu);
    return reached;
}

static void hook_release(void)
{
    pthread_mutex_lock(&g_hook.mu);
    g_hook.release = true;
    if (!g_hook.in_window)
        g_hook.armed = false;
    pthread_cond_broadcast(&g_hook.cv);
    pthread_mutex_unlock(&g_hook.mu);
}

/* --- ordered stop-publication window ------------------------------------ */

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    moq_msquic_managed_t *target;
    bool latched;
    bool release;
    bool expired;
} g_stop = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void stop_latched_hook(moq_msquic_managed_t *m)
{
    struct timespec abs;

    pthread_mutex_lock(&g_stop.mu);
    if (m != g_stop.target) {
        pthread_mutex_unlock(&g_stop.mu);
        return;
    }
    g_stop.latched = true;
    pthread_cond_broadcast(&g_stop.cv);
    guard_deadline(&abs);
    while (!g_stop.release)
        if (pthread_cond_timedwait(&g_stop.cv, &g_stop.mu, &abs) != 0) {
            g_stop.expired = true;
            break;
        }
    pthread_mutex_unlock(&g_stop.mu);
}

static void stop_arm(moq_msquic_managed_t *m)
{
    pthread_mutex_lock(&g_stop.mu);
    g_stop.target = m;
    g_stop.latched = false;
    g_stop.release = false;
    g_stop.expired = false;
    pthread_mutex_unlock(&g_stop.mu);
}

static bool stop_await_latched(void)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_stop.mu);
    while (!g_stop.latched && !g_stop.expired)
        if (pthread_cond_timedwait(&g_stop.cv, &g_stop.mu, &abs) != 0)
            break;
    reached = g_stop.latched && !g_stop.expired;
    pthread_mutex_unlock(&g_stop.mu);
    return reached;
}

static void stop_release(void)
{
    pthread_mutex_lock(&g_stop.mu);
    g_stop.release = true;
    pthread_cond_broadcast(&g_stop.cv);
    pthread_mutex_unlock(&g_stop.mu);
}

/* --- lanes-only rig ------------------------------------------------------ */

struct rig {
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane[LANE_COUNT];
};

static bool rig_up(struct rig *r)
{
    moq_msquic_managed_cfg_t cfg;
    bool valid = true;

    memset(r, 0, sizeof(*r));
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.max_connections = 4;
    cfg.lane_count = LANE_COUNT;
    cfg.on_lane_pump = pump_dispatch;
    if (moq_msq_test_managed_create_lanes_only(&cfg, &r->m) != MOQ_OK)
        return false;
    CHECK(moq_msquic_managed_lane_count(r->m) == LANE_COUNT);
    for (uint32_t i = 0; i < LANE_COUNT; i++) {
        r->lane[i] = moq_msquic_managed_lane(r->m, i);
        CHECK(r->lane[i] != NULL);
        if (r->lane[i] == NULL) {
            valid = false;
        } else {
            CHECK(moq_msquic_lane_index(r->lane[i]) == i);
            if (moq_msquic_lane_index(r->lane[i]) != i)
                valid = false;
        }
    }
    if (!valid) {
        moq_msquic_managed_destroy(r->m);
        memset(r, 0, sizeof(*r));
        return false;
    }
    atomic_store(&g_mode, MODE_NONE);
    atomic_store(&g_budget_a, 0);
    atomic_store(&g_budget_b, 0);
    progress_reset();
    return r->lane[0] != NULL && r->lane[1] != NULL;
}

static void rig_down(struct rig *r)
{
    hook_release();
    if (r->m != NULL) {
        CHECK(moq_msquic_managed_stop(r->m) == MOQ_OK);
        moq_msquic_managed_destroy(r->m);
        r->m = NULL;
    }
}

/* --- (0) invalid argument/result classification ------------------------- */

static void t_invalid_results(void)
{
    int before = failures;
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_t *m = (moq_msquic_managed_t *)(uintptr_t)1;
    moq_msquic_lane_stats_t st;
    struct rig r;

    CHECK(moq_msq_test_managed_create_lanes_only(NULL, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.lane_count = LANE_COUNT;
    cfg.on_lane_pump = pump_dispatch;
    CHECK(moq_msq_test_managed_create_lanes_only(&cfg, NULL) == MOQ_ERR_INVAL);
    cfg.on_lane_pump = NULL;
    CHECK(moq_msq_test_managed_create_lanes_only(&cfg, &m) == MOQ_ERR_INVAL);
    CHECK(m == NULL);

    CHECK(rig_up(&r));
    if (r.m == NULL)
        return;
    CHECK(moq_msquic_managed_lane(NULL, 0) == NULL);
    CHECK(moq_msquic_managed_lane(r.m, LANE_COUNT) == NULL);
    CHECK(moq_msquic_lane_wake(NULL) == MOQ_ERR_INVAL);
    CHECK(moq_msquic_lane_get_stats(NULL, &st, sizeof(st)) == MOQ_ERR_INVAL);
    CHECK(moq_msquic_lane_get_stats(r.lane[0], NULL, sizeof(st)) ==
          MOQ_ERR_INVAL);
    CHECK(moq_msquic_lane_get_stats(r.lane[0], &st,
                                    MOQ_MSQUIC_LANE_STATS_V0_SIZE - 1) ==
          MOQ_ERR_INVAL);
    rig_down(&r);
    if (failures == before)
        printf("PASS: wake lanes-only invalid results\n");
}

/* --- (1) check->wait race window ---------------------------------------- */

static void t_race_window(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t pre, mid, post;

    hook_arm(1);
    CHECK(rig_up(&r));
    if (r.m == NULL) {
        hook_release();
        return;
    }
    CHECK(hook_await_window());
    CHECK(stats_of(r.lane[1], &pre) == MOQ_OK);
    uint64_t generation_pre = moq_msq_test_bell_generation(r.lane[1]);
    uint64_t source_pumps = progress_pumps(0);
    uint64_t pumps = progress_pumps(1);

    atomic_store(&g_mode, MODE_CROSS_ONE);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(progress_await_pumps(0, source_pumps + 1));
    CHECK(atomic_load(&g_budget_a) == 0);
    bool cleanup_ring = cross_ring_or_cleanup(r.lane[1], generation_pre, 1);
    CHECK(stats_of(r.lane[1], &mid) == MOQ_OK);
    CHECK(mid.wakes_cross_lane == pre.wakes_cross_lane + 1);
    CHECK(mid.wakes_external == pre.wakes_external + (cleanup_ring ? 1u : 0u));
    CHECK(mid.wakes_coalesced ==
          pre.wakes_coalesced + (cleanup_ring ? 1u : 0u));
    CHECK(mid.pump_sweeps == pre.pump_sweeps);
    CHECK(mid.wake_to_pump_samples == pre.wake_to_pump_samples);
    CHECK(mid.idle_cap_wakes == pre.idle_cap_wakes);

    uint64_t bell_pre = moq_msq_test_bell_wakes(r.lane[1]);

    hook_release();
    CHECK(progress_await_pumps(1, pumps + 1));
    CHECK(stats_of(r.lane[1], &post) == MOQ_OK);
    CHECK(moq_msq_test_bell_wakes(r.lane[1]) == bell_pre + 1);
    CHECK(post.pump_sweeps == pre.pump_sweeps + 1);
    CHECK(post.wake_to_pump_samples == pre.wake_to_pump_samples + 1);
    CHECK(post.wakes_cross_lane == pre.wakes_cross_lane + 1);
    CHECK(post.wakes_external ==
          pre.wakes_external + (cleanup_ring ? 1u : 0u));
    CHECK(post.wakes_coalesced ==
          pre.wakes_coalesced + (cleanup_ring ? 1u : 0u));
    CHECK(post.idle_cap_wakes == pre.idle_cap_wakes);

    rig_down(&r);
    if (failures == before)
        printf("PASS: wake survives the check->wait window\n");
}

/* --- (2) cross-lane consumption ---------------------------------------- */

static void t_cross_consumption(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r));
    if (r.m == NULL)
        return;
    atomic_store(&g_mode, MODE_CROSS_ONE);
    for (int i = 0; i < 10; i++) {
        moq_msquic_lane_stats_t pre, post;
        uint64_t source_pumps = progress_pumps(0);
        uint64_t pumps = progress_pumps(1);

        CHECK(stats_of(r.lane[1], &pre) == MOQ_OK);
        uint64_t generation_pre = moq_msq_test_bell_generation(r.lane[1]);

        atomic_store(&g_budget_a, 1);
        CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
        CHECK(progress_await_pumps(0, source_pumps + 1));
        CHECK(atomic_load(&g_budget_a) == 0);
        bool cleanup_ring =
            cross_ring_or_cleanup(r.lane[1], generation_pre, 1);

        CHECK(progress_await_pumps(1, pumps + 1));
        CHECK(stats_of(r.lane[1], &post) == MOQ_OK);
        CHECK(post.wakes_cross_lane == pre.wakes_cross_lane + 1);
        CHECK(post.wakes_external ==
              pre.wakes_external + (cleanup_ring ? 1u : 0u));
        CHECK(post.wakes_coalesced ==
              pre.wakes_coalesced + (cleanup_ring ? 1u : 0u));
        CHECK(post.pump_sweeps == pre.pump_sweeps + 1);
        CHECK(post.wake_to_pump_samples == pre.wake_to_pump_samples + 1);
        CHECK(post.idle_cap_wakes == pre.idle_cap_wakes);
    }
    rig_down(&r);
    if (failures == before)
        printf("PASS: ten exact cross-lane wake cycles\n");
}

/* --- (3) bidirectional cross-wake lock ordering ------------------------- */

static void t_bidirectional(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r));
    if (r.m == NULL)
        return;
    atomic_store(&g_mode, MODE_PINGPONG);

    for (int i = 0; i < 3; i++) {
        moq_msquic_lane_stats_t pre0, pre1, post0, post1;
        uint64_t pumps0 = progress_pumps(0);
        uint64_t pumps1 = progress_pumps(1);
        uint64_t gen0 = moq_msq_test_bell_generation(r.lane[0]);
        uint64_t gen1 = moq_msq_test_bell_generation(r.lane[1]);

        CHECK(stats_of(r.lane[0], &pre0) == MOQ_OK);
        CHECK(stats_of(r.lane[1], &pre1) == MOQ_OK);
        bidir_arm();
        atomic_store(&g_budget_a, 1);
        atomic_store(&g_budget_b, 1);
        CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
        CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
        CHECK(bidir_await_arrived());
        bidir_release();
        CHECK(progress_await_pumps(0, pumps0 + 1));
        CHECK(progress_await_pumps(1, pumps1 + 1));
        CHECK(atomic_load(&g_budget_a) == 0);
        CHECK(atomic_load(&g_budget_b) == 0);
        CHECK(bidir_complete());
        CHECK(moq_msq_test_bell_generation(r.lane[0]) == gen0 + 2);
        CHECK(moq_msq_test_bell_generation(r.lane[1]) == gen1 + 2);
        CHECK(progress_await_pumps(0, pumps0 + 2));
        CHECK(progress_await_pumps(1, pumps1 + 2));
        CHECK(stats_of(r.lane[0], &post0) == MOQ_OK);
        CHECK(stats_of(r.lane[1], &post1) == MOQ_OK);
        CHECK(post0.wakes_cross_lane == pre0.wakes_cross_lane + 1);
        CHECK(post1.wakes_cross_lane == pre1.wakes_cross_lane + 1);
        CHECK(post0.wakes_external == pre0.wakes_external + 1);
        CHECK(post1.wakes_external == pre1.wakes_external + 1);
        CHECK(post0.wakes_coalesced == pre0.wakes_coalesced);
        CHECK(post1.wakes_coalesced == pre1.wakes_coalesced);
        CHECK(post0.wake_to_pump_samples ==
              pre0.wake_to_pump_samples + 2);
        CHECK(post1.wake_to_pump_samples ==
              pre1.wake_to_pump_samples + 2);
        CHECK(post0.pump_sweeps == pre0.pump_sweeps + 2);
        CHECK(post1.pump_sweeps == pre1.pump_sweeps + 2);
        CHECK(post0.idle_cap_wakes == pre0.idle_cap_wakes);
        CHECK(post1.idle_cap_wakes == pre1.idle_cap_wakes);
    }
    rig_down(&r);
    if (failures == before)
        printf("PASS: bidirectional cross-wakes drain without inversion\n");
}

/* --- (4) coalescing ------------------------------------------------------ */

static void t_coalescing(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t pre, mid, post, fin;

    hook_arm(1);
    CHECK(rig_up(&r));
    if (r.m == NULL) {
        hook_release();
        return;
    }
    CHECK(hook_await_window());
    CHECK(stats_of(r.lane[1], &pre) == MOQ_OK);
    uint64_t generation_pre = moq_msq_test_bell_generation(r.lane[1]);
    uint64_t source_pumps = progress_pumps(0);
    uint64_t pumps = progress_pumps(1);

    atomic_store(&g_mode, MODE_CROSS_TRIPLE);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(progress_await_pumps(0, source_pumps + 1));
    CHECK(atomic_load(&g_budget_a) == 0);
    bool cleanup_first =
        cross_ring_or_cleanup(r.lane[1], generation_pre, 3);

    CHECK(stats_of(r.lane[1], &mid) == MOQ_OK);
    CHECK(mid.wakes_cross_lane == pre.wakes_cross_lane + 3);
    CHECK(mid.wakes_external ==
          pre.wakes_external + (cleanup_first ? 1u : 0u));
    CHECK(mid.wakes_coalesced ==
          pre.wakes_coalesced + 2 + (cleanup_first ? 1u : 0u));
    CHECK(mid.wake_to_pump_samples == pre.wake_to_pump_samples);
    CHECK(mid.idle_cap_wakes == pre.idle_cap_wakes);

    hook_release();
    CHECK(progress_await_pumps(1, pumps + 1));
    CHECK(stats_of(r.lane[1], &post) == MOQ_OK);
    CHECK(post.pump_sweeps == pre.pump_sweeps + 1);
    CHECK(post.wake_to_pump_samples == pre.wake_to_pump_samples + 1);
    CHECK(post.wakes_external ==
          pre.wakes_external + (cleanup_first ? 1u : 0u));
    CHECK(post.wakes_coalesced ==
          pre.wakes_coalesced + 2 + (cleanup_first ? 1u : 0u));
    CHECK(post.idle_cap_wakes == pre.idle_cap_wakes);

    pumps = progress_pumps(1);
    source_pumps = progress_pumps(0);
    generation_pre = moq_msq_test_bell_generation(r.lane[1]);
    atomic_store(&g_mode, MODE_CROSS_ONE);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(progress_await_pumps(0, source_pumps + 1));
    CHECK(atomic_load(&g_budget_a) == 0);
    bool cleanup_second =
        cross_ring_or_cleanup(r.lane[1], generation_pre, 1);

    CHECK(progress_await_pumps(1, pumps + 1));
    CHECK(stats_of(r.lane[1], &fin) == MOQ_OK);
    CHECK(fin.pump_sweeps == post.pump_sweeps + 1);
    CHECK(fin.wake_to_pump_samples == pre.wake_to_pump_samples + 2);
    CHECK(fin.wakes_cross_lane == pre.wakes_cross_lane + 4);
    CHECK(fin.wakes_external == pre.wakes_external +
                                      (cleanup_first ? 1u : 0u) +
                                      (cleanup_second ? 1u : 0u));
    CHECK(fin.wakes_coalesced == pre.wakes_coalesced + 2 +
                                       (cleanup_first ? 1u : 0u) +
                                       (cleanup_second ? 1u : 0u));
    CHECK(fin.idle_cap_wakes == pre.idle_cap_wakes);
    rig_down(&r);
    if (failures == before)
        printf("PASS: one open cycle coalesces and later re-arms\n");
}

/* --- (5) cause classification ------------------------------------------- */

static void t_mixed_causes(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t pre, parked, post, ext;

    hook_arm(1);
    CHECK(rig_up(&r));
    if (r.m == NULL) {
        hook_release();
        return;
    }
    CHECK(hook_await_window());
    CHECK(stats_of(r.lane[1], &pre) == MOQ_OK);
    uint64_t generation_pre = moq_msq_test_bell_generation(r.lane[1]);
    uint64_t source_pumps = progress_pumps(0);
    uint64_t pumps = progress_pumps(1);

    atomic_store(&g_mode, MODE_CROSS_ONE);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(progress_await_pumps(0, source_pumps + 1));
    CHECK(atomic_load(&g_budget_a) == 0);
    bool cleanup_cross =
        cross_ring_or_cleanup(r.lane[1], generation_pre, 1);

    CHECK(stats_of(r.lane[1], &parked) == MOQ_OK);
    CHECK(parked.wakes_cross_lane == pre.wakes_cross_lane + 1);
    CHECK(parked.wakes_external ==
          pre.wakes_external + (cleanup_cross ? 1u : 0u));
    CHECK(parked.wakes_coalesced ==
          pre.wakes_coalesced + (cleanup_cross ? 1u : 0u));
    hook_release();
    CHECK(progress_await_pumps(1, pumps + 1));
    CHECK(stats_of(r.lane[1], &post) == MOQ_OK);
    CHECK(post.wakes_cross_lane == pre.wakes_cross_lane + 1);
    CHECK(post.wakes_same_lane == pre.wakes_same_lane);
    CHECK(post.wakes_external ==
          pre.wakes_external + (cleanup_cross ? 1u : 0u));
    CHECK(post.wakes_coalesced ==
          pre.wakes_coalesced + (cleanup_cross ? 1u : 0u));
    CHECK(post.deadline_sweeps == pre.deadline_sweeps);
    CHECK(post.idle_cap_wakes == pre.idle_cap_wakes);

    pumps = progress_pumps(1);
    CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
    CHECK(progress_await_pumps(1, pumps + 1));
    CHECK(stats_of(r.lane[1], &ext) == MOQ_OK);
    CHECK(ext.wakes_external == post.wakes_external + 1);
    CHECK(ext.wakes_cross_lane == post.wakes_cross_lane);
    CHECK(ext.wakes_same_lane == post.wakes_same_lane);
    CHECK(ext.wakes_coalesced == post.wakes_coalesced);
    CHECK(ext.idle_cap_wakes == post.idle_cap_wakes);
    rig_down(&r);
    if (failures == before)
        printf("PASS: wake causes remain distinct\n");
}

static void t_same_lane_cause(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t pre, post;
    uint64_t pumps;

    CHECK(rig_up(&r));
    if (r.m == NULL)
        return;
    CHECK(stats_of(r.lane[0], &pre) == MOQ_OK);
    pumps = progress_pumps(0);
    atomic_store(&g_mode, MODE_SELF);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(progress_await_pumps(0, pumps + 1));
    CHECK(atomic_load(&g_budget_a) == 0);
    CHECK(stats_of(r.lane[0], &post) == MOQ_OK);
    CHECK(post.wakes_same_lane == pre.wakes_same_lane + 1);
    CHECK(post.wakes_external == pre.wakes_external + 1);
    CHECK(post.wakes_cross_lane == pre.wakes_cross_lane);
    rig_down(&r);
    if (failures == before)
        printf("PASS: same-lane wake is not cross/external\n");
}

/* --- (6) wakes ordered after facade stop publication -------------------- */

struct stop_call {
    moq_msquic_managed_t *m;
    moq_result_t rc;
};

static void *stop_main(void *arg)
{
    struct stop_call *call = arg;

    call->rc = moq_msquic_managed_stop(call->m);
    return NULL;
}

static void t_wake_vs_stop(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t pre0, pre1, post0, post1;
    struct stop_call call;
    pthread_t thread;
    bool started;

    CHECK(rig_up(&r));
    if (r.m == NULL)
        return;
    atomic_store(&g_mode, MODE_NONE);
    CHECK(stats_of(r.lane[0], &pre0) == MOQ_OK);
    CHECK(stats_of(r.lane[1], &pre1) == MOQ_OK);
    uint64_t pumps0 = progress_pumps(0);
    uint64_t pumps1 = progress_pumps(1);

    stop_arm(r.m);
    call.m = r.m;
    call.rc = MOQ_ERR_INTERNAL;
    started = pthread_create(&thread, NULL, stop_main, &call) == 0;
    CHECK(started);
    if (!started) {
        stop_release();
        pthread_mutex_lock(&g_stop.mu);
        g_stop.target = NULL;
        pthread_mutex_unlock(&g_stop.mu);
        rig_down(&r);
        return;
    }
    CHECK(stop_await_latched());
    /* The facade flag is now published, while stop is held before taking any
     * lane lock. Both accepted wake calls therefore race the exact post-stop
     * window without relying on a scheduler or duration. */
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
    stop_release();
    pthread_join(thread, NULL);
    CHECK(call.rc == MOQ_OK);
    CHECK(stats_of(r.lane[0], &post0) == MOQ_OK);
    CHECK(stats_of(r.lane[1], &post1) == MOQ_OK);
    CHECK(post0.wakes_external == pre0.wakes_external + 1);
    CHECK(post1.wakes_external == pre1.wakes_external + 1);
    CHECK(progress_pumps(0) == pumps0);
    CHECK(progress_pumps(1) == pumps1);
    CHECK(moq_msquic_managed_stop(r.m) == MOQ_OK); /* idempotent result */
    pthread_mutex_lock(&g_stop.mu);
    CHECK(!g_stop.expired);
    g_stop.target = NULL;
    pthread_mutex_unlock(&g_stop.mu);
    moq_msquic_managed_destroy(r.m);
    r.m = NULL;
    if (failures == before)
        printf("PASS: post-publication wakes cannot strand stop\n");
}

/* --- (7) callback confinement ------------------------------------------- */

static void t_context_rule(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t pre, post;
    uint64_t pumps;

    CHECK(rig_up(&r));
    if (r.m == NULL)
        return;
    CHECK(stats_of(r.lane[0], &pre) == MOQ_OK);
    pumps = progress_pumps(0);
    atomic_store(&g_rc_own, RC_UNSET);
    atomic_store(&g_rc_other, RC_UNSET);
    atomic_store(&g_mode, MODE_CTX);
    atomic_store(&g_budget_a, 1);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(progress_await_pumps(0, pumps + 1));
    CHECK(atomic_load(&g_budget_a) == 0);
    CHECK(atomic_load(&g_rc_own) == (int)MOQ_OK);
    CHECK(atomic_load(&g_rc_other) == (int)MOQ_ERR_WRONG_STATE);
    CHECK(stats_of(r.lane[0], &post) == MOQ_OK);
    rig_down(&r);
    if (failures == before)
        printf("PASS: callback stats confinement\n");
}

int main(void)
{
    moq_msq_test_prewait = hook_prewait;
    moq_msq_test_stop_latched = stop_latched_hook;

    t_invalid_results();
    t_race_window();
    t_cross_consumption();
    t_bidirectional();
    t_coalescing();
    t_mixed_causes();
    t_same_lane_cause();
    t_wake_vs_stop();
    t_context_rule();

    moq_msq_test_prewait = NULL;
    moq_msq_test_stop_latched = NULL;
    if (failures == 0)
        printf("PASS: msquic_wake\n");
    else
        fprintf(stderr, "FAIL: msquic_wake (%d)\n", failures);
    return failures;
}
