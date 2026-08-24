/*
 * Deterministic managed-lane statistics proofs.
 *
 * NETWORK-FREE. The facade borrows a fake QUIC_API_TABLE and creates only
 * lanes, doorbells, and synthetic managed children through the production
 * child/adapter path. Injected monotonic time drives deadline and idle-cause
 * classification. Timed condition waits are fail-closed guards around named
 * predicates; elapsed time is never an acceptance condition.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "msquic_internal.h"
#include "support/fake_msq_table.h"
#include "support/msq_test_seams.h"

#include <stddef.h>

/* --- the frozen v0 prefix, pinned to a constant ---------------------------- *
 *
 * The floor is defined from offsetof(flush_bytes), so a field inserted ANYWHERE
 * before it silently moves the floor -- the definition cannot catch that on its
 * own. Pinning the byte count to a literal makes the drift a build failure, and
 * requiring every diagnostic field to begin at or after the floor proves the
 * additions are appended rather than interleaved. */
_Static_assert(MOQ_MSQUIC_LANE_STATS_V0_SIZE == 112u,
               "the frozen v0 lane-stats floor must never move");
_Static_assert(offsetof(moq_msquic_lane_stats_t, idle_cap_wakes) == 56u,
               "v0 field offsets are frozen");
_Static_assert(offsetof(moq_msquic_lane_stats_t, service_passes) == 88u,
               "v0 field offsets are frozen");
_Static_assert(offsetof(moq_msquic_lane_stats_t, flush_bytes) == 104u,
               "v0 field offsets are frozen");
#ifdef MOQ_MSQUIC_TESTING
_Static_assert(offsetof(moq_msquic_lane_stats_t, deadline_queries) >=
                   MOQ_MSQUIC_LANE_STATS_V0_SIZE,
               "diagnostic fields are appended past the frozen prefix");
_Static_assert(offsetof(moq_msquic_lane_stats_t, deadline_class_none) >=
                   MOQ_MSQUIC_LANE_STATS_V0_SIZE,
               "diagnostic fields are appended past the frozen prefix");
_Static_assert(offsetof(moq_msquic_lane_stats_t, deadline_class_future) >=
                   MOQ_MSQUIC_LANE_STATS_V0_SIZE,
               "diagnostic fields are appended past the frozen prefix");
_Static_assert(offsetof(moq_msquic_lane_stats_t, deadline_class_due) >=
                   MOQ_MSQUIC_LANE_STATS_V0_SIZE,
               "diagnostic fields are appended past the frozen prefix");
#endif

#include <moq/msquic_managed.h>

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

/* MOQ_MSQUIC_TESTING-only seams. */
extern void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);
extern void moq_msq_test_bell_ring_raw(moq_msquic_managed_lane_t *lane);
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out);
extern bool moq_msq_test_lane_inject_idle_child(
    moq_msquic_managed_lane_t *lane);
extern moq_result_t moq_msq_test_lane_inject_live_child(
    moq_msquic_managed_lane_t *lane, HQUIC connection,
    moq_msquic_managed_conn_t **out);
extern moq_msquic_conn_t *moq_msq_test_managed_conn_adapter(
    moq_msquic_managed_conn_t *conn);

static int failures;

#define CHECK(expr)                                                         \
    do {                                                                    \
        if (!(expr)) {                                                      \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__,    \
                    #expr);                                                 \
            failures++;                                                     \
        }                                                                   \
    } while (0)

enum { MAX_TEST_LANES = 2 };
#define GUARD_US (3u * 1000u * 1000u)

static void guard_deadline(struct timespec *abs)
{
    clock_gettime(CLOCK_REALTIME, abs);
    abs->tv_sec += (time_t)(GUARD_US / 1000000u);
    abs->tv_nsec += (long)(GUARD_US % 1000000u) * 1000;
    if (abs->tv_nsec >= 1000000000L) {
        abs->tv_sec++;
        abs->tv_nsec -= 1000000000L;
    }
}

static _Atomic uint64_t g_now_us;

static _Atomic uint64_t g_now_calls;

static uint64_t test_now_us(void)
{
    atomic_fetch_add(&g_now_calls, 1);
    return atomic_load(&g_now_us);
}

static void set_now(uint64_t now_us)
{
    atomic_store(&g_now_us, now_us);
}

static moq_result_t stats_of(moq_msquic_managed_lane_t *lane,
                             moq_msquic_lane_stats_t *st)
{
    memset(st, 0, sizeof(*st));
    return moq_msquic_lane_get_stats(lane, st, sizeof(*st));
}

/* Doorbell pre-wait rendezvous. Each enabled lane stops after computing its
 * exact wait cause but before sleeping. Main releases numbered entries one at
 * a time; the timeout only prevents a broken mutant from hanging the suite. */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool enabled[MAX_TEST_LANES];
    uint64_t entered[MAX_TEST_LANES];
    uint64_t released[MAX_TEST_LANES];
    bool expired;
} g_gate = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void gate_prepare(unsigned mask)
{
    pthread_mutex_lock(&g_gate.mu);
    memset(g_gate.enabled, 0, sizeof(g_gate.enabled));
    memset(g_gate.entered, 0, sizeof(g_gate.entered));
    memset(g_gate.released, 0, sizeof(g_gate.released));
    for (unsigned i = 0; i < MAX_TEST_LANES; i++)
        g_gate.enabled[i] = (mask & (1u << i)) != 0;
    g_gate.expired = false;
    pthread_mutex_unlock(&g_gate.mu);
}

static void prewait_hook(moq_msquic_managed_lane_t *lane)
{
    uint32_t idx = moq_msquic_lane_index(lane);
    struct timespec abs;

    if (idx >= MAX_TEST_LANES)
        return;
    guard_deadline(&abs);
    pthread_mutex_lock(&g_gate.mu);
    if (!g_gate.enabled[idx]) {
        pthread_mutex_unlock(&g_gate.mu);
        return;
    }
    uint64_t mine = ++g_gate.entered[idx];
    pthread_cond_broadcast(&g_gate.cv);
    while (g_gate.enabled[idx] && g_gate.released[idx] < mine)
        if (pthread_cond_timedwait(&g_gate.cv, &g_gate.mu, &abs) != 0) {
            g_gate.expired = true;
            g_gate.enabled[idx] = false;
            pthread_cond_broadcast(&g_gate.cv);
            break;
        }
    pthread_mutex_unlock(&g_gate.mu);
}

static bool gate_await(uint32_t idx, uint64_t want)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_gate.mu);
    while (idx < MAX_TEST_LANES && g_gate.entered[idx] < want &&
           !g_gate.expired)
        if (pthread_cond_timedwait(&g_gate.cv, &g_gate.mu, &abs) != 0)
            break;
    reached = idx < MAX_TEST_LANES && g_gate.entered[idx] >= want &&
              !g_gate.expired;
    pthread_mutex_unlock(&g_gate.mu);
    return reached;
}

static void gate_release(uint32_t idx, uint64_t entry)
{
    pthread_mutex_lock(&g_gate.mu);
    if (idx < MAX_TEST_LANES && g_gate.released[idx] < entry)
        g_gate.released[idx] = entry;
    pthread_cond_broadcast(&g_gate.cv);
    pthread_mutex_unlock(&g_gate.mu);
}

static void gate_disable(void)
{
    pthread_mutex_lock(&g_gate.mu);
    memset(g_gate.enabled, 0, sizeof(g_gate.enabled));
    pthread_cond_broadcast(&g_gate.cv);
    pthread_mutex_unlock(&g_gate.mu);
}

enum pump_mode {
    PUMP_NONE,
    PUMP_CONTEXT,
    PUMP_FLUSH,
    PUMP_DRAIN,
    PUMP_APP,
};

static _Atomic uint64_t g_app_target;
static _Atomic uint64_t g_app_calls;
static _Atomic uint64_t g_app_bad_ctx;

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    enum pump_mode mode;
    uint64_t calls[MAX_TEST_LANES];
    unsigned actions;
    unsigned flush_count;
    size_t flush_len;
    moq_msquic_managed_conn_t *managed_conn;
    moq_msquic_conn_t *adapter;
    uint64_t stream_id;
    moq_transport_result_t open_rc;
    moq_transport_result_t write_rc;
    bool action_done;
    bool context_done;
    bool context_release;
    bool context_expired;
    moq_result_t rc_own;
    moq_result_t rc_other;
    moq_result_t rc_wake_own_1;
    moq_result_t rc_wake_own_2;
    moq_result_t rc_wake_other;
    uint64_t session_closed;
    uint64_t other_events;
    uint64_t terminal_acks;
    moq_result_t last_ack_rc;
} g_pump = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void pump_reset(void)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.mode = PUMP_NONE;
    memset(g_pump.calls, 0, sizeof(g_pump.calls));
    g_pump.actions = 0;
    g_pump.flush_count = 0;
    g_pump.flush_len = 0;
    g_pump.managed_conn = NULL;
    g_pump.adapter = NULL;
    g_pump.stream_id = UINT64_MAX;
    g_pump.open_rc = MOQ_TRANSPORT_ERROR;
    g_pump.write_rc = MOQ_TRANSPORT_ERROR;
    g_pump.action_done = false;
    g_pump.context_done = false;
    g_pump.context_release = false;
    g_pump.context_expired = false;
    g_pump.rc_own = MOQ_ERR_INTERNAL;
    g_pump.rc_other = MOQ_ERR_INTERNAL;
    g_pump.rc_wake_own_1 = MOQ_ERR_INTERNAL;
    g_pump.rc_wake_own_2 = MOQ_ERR_INTERNAL;
    g_pump.rc_wake_other = MOQ_ERR_INTERNAL;
    g_pump.session_closed = 0;
    g_pump.other_events = 0;
    g_pump.terminal_acks = 0;
    g_pump.last_ack_rc = MOQ_ERR_INTERNAL;
    pthread_mutex_unlock(&g_pump.mu);
}

static void pump_arm(enum pump_mode mode, unsigned actions,
                     unsigned flush_count, size_t flush_len)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.mode = mode;
    g_pump.actions = actions;
    g_pump.flush_count = flush_count;
    g_pump.flush_len = flush_len;
    g_pump.action_done = false;
    pthread_mutex_unlock(&g_pump.mu);
}

static bool pump_await_calls(uint32_t lane, uint64_t want)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_pump.mu);
    while (lane < MAX_TEST_LANES && g_pump.calls[lane] < want)
        if (pthread_cond_timedwait(&g_pump.cv, &g_pump.mu, &abs) != 0)
            break;
    reached = lane < MAX_TEST_LANES && g_pump.calls[lane] >= want;
    pthread_mutex_unlock(&g_pump.mu);
    return reached;
}

static bool pump_await_action(void)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_pump.mu);
    while (!g_pump.action_done)
        if (pthread_cond_timedwait(&g_pump.cv, &g_pump.mu, &abs) != 0)
            break;
    reached = g_pump.action_done;
    pthread_mutex_unlock(&g_pump.mu);
    return reached;
}

static bool pump_await_context(void)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_pump.mu);
    while (!g_pump.context_done && !g_pump.context_expired)
        if (pthread_cond_timedwait(&g_pump.cv, &g_pump.mu, &abs) != 0)
            break;
    reached = g_pump.context_done && !g_pump.context_expired;
    pthread_mutex_unlock(&g_pump.mu);
    return reached;
}

static void pump_release_context(void)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.context_release = true;
    pthread_cond_broadcast(&g_pump.cv);
    pthread_mutex_unlock(&g_pump.mu);
}

static void pump_drain_terminals(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_managed_conn_t *c = NULL;

    while ((c = moq_msquic_lane_next_conn(lane, c)) != NULL) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        moq_event_t ev;
        bool closed = false;

        if (s == NULL)
            continue;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
                closed = true;
                g_pump.session_closed++;
            } else {
                g_pump.other_events++;
            }
            moq_event_cleanup(&ev);
        }
        if (closed) {
            g_pump.last_ack_rc =
                moq_msquic_managed_conn_ack_terminal(c);
            if (g_pump.last_ack_rc == MOQ_OK)
                g_pump.terminal_acks++;
        }
    }
}

static void pump_flush_actions(moq_msquic_managed_lane_t *lane)
{
    moq_msquic_managed_conn_t *mc = moq_msquic_lane_next_conn(lane, NULL);
    uint8_t bytes[512];

    if (mc == NULL || g_pump.flush_len > sizeof(bytes)) {
        g_pump.write_rc = MOQ_TRANSPORT_ERROR;
        return;
    }
    memset(bytes, 0x6b, sizeof(bytes));
    g_pump.managed_conn = mc;
    g_pump.adapter = moq_msq_test_managed_conn_adapter(mc);
    const moq_transport_endpoint_ops_t *ops =
        moq_msquic_test_ops(g_pump.adapter);
    g_pump.open_rc = ops->open_uni(g_pump.adapter, &g_pump.stream_id);
    if (g_pump.open_rc != MOQ_TRANSPORT_OK)
        return;
    g_pump.write_rc = MOQ_TRANSPORT_OK;
    for (unsigned i = 0; i < g_pump.flush_count; i++) {
        moq_transport_result_t rc = ops->write(
            g_pump.adapter, g_pump.stream_id, bytes, g_pump.flush_len, false);

        if (rc != MOQ_TRANSPORT_OK) {
            g_pump.write_rc = rc;
            break;
        }
        moq_msquic_test_flush(g_pump.adapter);
    }
}

static int test_pump(moq_msquic_managed_t *m,
                     moq_msquic_managed_lane_t *lane, uint64_t now_us,
                     void *user)
{
    uint32_t idx = moq_msquic_lane_index(lane);
    struct timespec abs;

    (void)now_us;
    (void)user;
    pthread_mutex_lock(&g_pump.mu);
    if (g_pump.mode == PUMP_CONTEXT && idx == 0 && g_pump.actions > 0) {
        g_pump.actions--;
        moq_msquic_lane_stats_t st;
        moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
        moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);

        g_pump.rc_own = stats_of(l0, &st);
        g_pump.rc_other = stats_of(l1, &st);
        g_pump.rc_wake_own_1 = moq_msquic_lane_wake(l0);
        g_pump.rc_wake_own_2 = moq_msquic_lane_wake(l0);
        g_pump.rc_wake_other = moq_msquic_lane_wake(l1);
        g_pump.context_done = true;
        pthread_cond_broadcast(&g_pump.cv);
        guard_deadline(&abs);
        while (!g_pump.context_release)
            if (pthread_cond_timedwait(&g_pump.cv, &g_pump.mu, &abs) != 0) {
                g_pump.context_expired = true;
                break;
            }
    } else if (g_pump.mode == PUMP_FLUSH && g_pump.actions > 0) {
        g_pump.actions--;
        pump_flush_actions(lane);
        g_pump.action_done = true;
        pthread_cond_broadcast(&g_pump.cv);
    } else if (g_pump.mode == PUMP_DRAIN) {
        pump_drain_terminals(lane);
    } else if (g_pump.mode == PUMP_APP && g_pump.actions > 0) {
        g_pump.actions--;
        atomic_store(&g_app_target, UINT64_MAX);
        g_pump.action_done = true;
        pthread_cond_broadcast(&g_pump.cv);
    }
    if (idx < MAX_TEST_LANES)
        g_pump.calls[idx]++;
    pthread_cond_broadcast(&g_pump.cv);
    pthread_mutex_unlock(&g_pump.mu);
    return 0;
}

static uint64_t app_deadline_cb(void *ctx)
{
    atomic_fetch_add(&g_app_calls, 1);
    if (ctx != &g_app_target) {
        atomic_fetch_add(&g_app_bad_ctx, 1);
        atomic_store(&g_app_target, UINT64_MAX);
        return UINT64_MAX;
    }
    return atomic_load(&g_app_target);
}

struct rig {
    fake_msq_t fake;
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane[MAX_TEST_LANES];
    uint32_t lane_count;
};

static bool rig_up(struct rig *r, uint32_t lane_count,
                   uint64_t session_idle_timeout_us,
                   uint64_t (*app_deadline)(void *ctx), void *app_ctx,
                   size_t cfg_size)
{
    moq_msquic_managed_cfg_t cfg;

    memset(r, 0, sizeof(*r));
    fake_msq_init(&r->fake, false);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.max_connections = 4;
    cfg.lane_count = lane_count;
    cfg.on_lane_pump = test_pump;
    cfg.session_idle_timeout_us = session_idle_timeout_us;
    cfg.app_deadline_us = app_deadline;
    cfg.app_deadline_ctx = app_ctx;
    cfg.struct_size = (uint32_t)cfg_size;
    if (moq_msq_test_managed_create_lanes_only_api(
            &cfg, fake_msq_table(&r->fake), &r->m) != MOQ_OK)
        return false;
    r->lane_count = lane_count;
    CHECK(moq_msquic_managed_lane_count(r->m) == lane_count);
    for (uint32_t i = 0; i < lane_count; i++) {
        r->lane[i] = moq_msquic_managed_lane(r->m, i);
        CHECK(r->lane[i] != NULL);
        CHECK(moq_msquic_lane_index(r->lane[i]) == i);
    }
    return true;
}

static void rig_down(struct rig *r)
{
    gate_disable();
    pump_release_context();
    if (r->m != NULL) {
        CHECK(moq_msquic_managed_stop(r->m) == MOQ_OK);
        moq_msquic_managed_destroy(r->m);
        r->m = NULL;
    }
}

static bool inject_and_settle(struct rig *r,
                              moq_msquic_managed_conn_t **managed,
                              moq_msquic_conn_t **adapter)
{
    if (!gate_await(0, 1))
        return false;
    if (moq_msq_test_lane_inject_live_child(
            r->lane[0], fake_msq_conn_handle(&r->fake), managed) != MOQ_OK)
        return false;
    *adapter = moq_msq_test_managed_conn_adapter(*managed);
    gate_release(0, 1);
    return pump_await_calls(0, 1) && gate_await(0, 2);
}

static QUIC_STATUS deliver_conn_event(fake_msq_t *fake,
                                      moq_msquic_conn_t *adapter,
                                      QUIC_CONNECTION_EVENT_TYPE type,
                                      uint64_t code)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = type;
    if (type == QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER)
        ev.SHUTDOWN_INITIATED_BY_PEER.ErrorCode = code;
    else if (type == QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT)
        ev.SHUTDOWN_INITIATED_BY_TRANSPORT.ErrorCode = code;
    return moq_msquic_conn_callback()(fake_msq_conn_handle(fake), adapter,
                                      &ev);
}

/* --- argument and ABI-prefix discipline --------------------------------- */

static void t_invalid_and_prefix(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t st;

    gate_prepare(1u << 0);
    pump_reset();
    CHECK(rig_up(&r, 1, 0, NULL, NULL, sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL)
        return;
    CHECK(gate_await(0, 1));

    moq_msquic_managed_t *bad = (moq_msquic_managed_t *)(uintptr_t)1;
    CHECK(moq_msq_test_managed_create_lanes_only_api(
              NULL, NULL, &bad) == MOQ_ERR_INVAL);
    CHECK(bad == NULL);
    CHECK(moq_msq_test_managed_create_lanes_only_api(
              NULL, fake_msq_table(&r.fake), NULL) == MOQ_ERR_INVAL);

    memset(&st, 0x5a, sizeof(st));
    CHECK(moq_msquic_lane_get_stats(NULL, &st, sizeof(st)) == MOQ_ERR_INVAL);
    CHECK(moq_msquic_lane_get_stats(r.lane[0], NULL, sizeof(st)) ==
          MOQ_ERR_INVAL);
    uint8_t refused[sizeof(st)];
    memset(refused, 0xa5, sizeof(refused));
    CHECK(moq_msquic_lane_get_stats(
              r.lane[0], (moq_msquic_lane_stats_t *)refused,
              MOQ_MSQUIC_LANE_STATS_V0_SIZE - 1) == MOQ_ERR_INVAL);
    for (size_t i = 0; i < sizeof(refused); i++)
        CHECK(refused[i] == 0xa5);

    union {
        max_align_t align;
        uint8_t bytes[sizeof(moq_msquic_lane_stats_t) + 32];
    } out;
    memset(out.bytes, 0xa5, sizeof(out.bytes));
    CHECK(moq_msquic_lane_get_stats(
              r.lane[0], (moq_msquic_lane_stats_t *)out.bytes,
              MOQ_MSQUIC_LANE_STATS_V0_SIZE) == MOQ_OK);
    moq_msquic_lane_stats_t want;
    memset(&want, 0, sizeof(want));
    want.struct_size = (uint32_t)MOQ_MSQUIC_LANE_STATS_V0_SIZE;
    CHECK(memcmp(out.bytes, &want, MOQ_MSQUIC_LANE_STATS_V0_SIZE) == 0);
    for (size_t i = MOQ_MSQUIC_LANE_STATS_V0_SIZE; i < sizeof(out.bytes); i++)
        CHECK(out.bytes[i] == 0xa5);

    memset(out.bytes, 0xa5, sizeof(out.bytes));
    CHECK(moq_msquic_lane_get_stats(
              r.lane[0], (moq_msquic_lane_stats_t *)out.bytes,
              sizeof(out.bytes)) == MOQ_OK);
    memset(&want, 0, sizeof(want));
    want.struct_size = (uint32_t)sizeof(want);
    CHECK(memcmp(out.bytes, &want, sizeof(want)) == 0);
    for (size_t i = sizeof(want); i < sizeof(out.bytes); i++)
        CHECK(out.bytes[i] == 0);

    rig_down(&r);
    if (failures == before)
        printf("PASS: invalid arguments and sized-output discipline\n");
}

/* --- deterministic idle and explicit-wake attribution ------------------- */

static void t_idle_classification(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a, b;

    set_now(1000000);
    gate_prepare(1u << 0);
    pump_reset();
    CHECK(rig_up(&r, 1, 0, NULL, NULL, sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL)
        return;
    CHECK(gate_await(0, 1));
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);
    set_now(1200000); /* exactly the production idle cap */
    moq_msq_test_bell_ring_raw(r.lane[0]);
    gate_release(0, 1);
    CHECK(gate_await(0, 2));
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);
    CHECK(b.idle_cap_wakes == a.idle_cap_wakes + 1);
    CHECK(b.deadline_sweeps == a.deadline_sweeps);
    CHECK(b.wake_to_pump_samples == a.wake_to_pump_samples);
    CHECK(b.pump_sweeps == a.pump_sweeps);
    rig_down(&r);
    if (failures == before)
        printf("PASS: idle-cap cause classified without elapsed-time wait\n");
}

static void t_external_wake(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a, b;

    set_now(2000000);
    gate_prepare(1u << 0);
    pump_reset();
    CHECK(rig_up(&r, 1, 0, NULL, NULL, sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL)
        return;
    CHECK(gate_await(0, 1));
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    set_now(2000037);
    gate_release(0, 1);
    CHECK(pump_await_calls(0, 1));
    CHECK(gate_await(0, 2));
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);
    CHECK(b.wakes_external == a.wakes_external + 1);
    CHECK(b.wakes_same_lane == a.wakes_same_lane);
    CHECK(b.wakes_cross_lane == a.wakes_cross_lane);
    CHECK(b.wakes_coalesced == a.wakes_coalesced);
    CHECK(b.pump_sweeps == a.pump_sweeps + 1);
    CHECK(b.wake_to_pump_samples == a.wake_to_pump_samples + 1);
    CHECK(b.wake_to_pump_total_us == a.wake_to_pump_total_us + 37);
    CHECK(b.wake_to_pump_max_us == 37);
    CHECK(b.deadline_sweeps == a.deadline_sweeps);
    CHECK(b.idle_cap_wakes == a.idle_cap_wakes);
    rig_down(&r);
    if (failures == before)
        printf("PASS: external wake has one exact latency sample\n");
}

static void t_callback_contexts(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a0, a1, b0, b1;

    set_now(3000000);
    gate_prepare((1u << 0) | (1u << 1));
    pump_reset();
    CHECK(rig_up(&r, 2, 0, NULL, NULL, sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL)
        return;
    CHECK(gate_await(0, 1));
    CHECK(gate_await(1, 1));
    CHECK(stats_of(r.lane[0], &a0) == MOQ_OK);
    CHECK(stats_of(r.lane[1], &a1) == MOQ_OK);
    pump_arm(PUMP_CONTEXT, 1, 0, 0);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    set_now(3000010);
    gate_release(0, 1);
    CHECK(pump_await_context());

    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.rc_own == MOQ_OK);
    CHECK(g_pump.rc_other == MOQ_ERR_WRONG_STATE);
    CHECK(g_pump.rc_wake_own_1 == MOQ_OK);
    CHECK(g_pump.rc_wake_own_2 == MOQ_OK);
    CHECK(g_pump.rc_wake_other == MOQ_OK);
    pthread_mutex_unlock(&g_pump.mu);

    set_now(3000020);
    pump_release_context();
    gate_release(1, 1);
    CHECK(pump_await_calls(0, 2));
    CHECK(pump_await_calls(1, 1));
    CHECK(gate_await(0, 2));
    CHECK(gate_await(1, 2));
    CHECK(stats_of(r.lane[0], &b0) == MOQ_OK);
    CHECK(stats_of(r.lane[1], &b1) == MOQ_OK);

    CHECK(b0.wakes_external == a0.wakes_external + 1);
    CHECK(b0.wakes_same_lane == a0.wakes_same_lane + 2);
    CHECK(b0.wakes_cross_lane == a0.wakes_cross_lane);
    CHECK(b0.wakes_coalesced == a0.wakes_coalesced + 1);
    CHECK(b0.pump_sweeps == a0.pump_sweeps + 2);
    CHECK(b0.wake_to_pump_samples == a0.wake_to_pump_samples + 2);
    CHECK(b0.wake_to_pump_total_us == a0.wake_to_pump_total_us + 20);
    CHECK(b0.wake_to_pump_max_us == 10);

    CHECK(b1.wakes_cross_lane == a1.wakes_cross_lane + 1);
    CHECK(b1.wakes_external == a1.wakes_external);
    CHECK(b1.wakes_same_lane == a1.wakes_same_lane);
    CHECK(b1.wakes_coalesced == a1.wakes_coalesced);
    CHECK(b1.pump_sweeps == a1.pump_sweeps + 1);
    CHECK(b1.wake_to_pump_samples == a1.wake_to_pump_samples + 1);
    CHECK(b1.wake_to_pump_total_us == a1.wake_to_pump_total_us + 10);
    CHECK(b1.wake_to_pump_max_us == 10);
    rig_down(&r);
    if (failures == before)
        printf("PASS: callback contexts and wake causes are exact\n");
}

/* --- deterministic app/session deadlines -------------------------------- */

static void t_app_deadline_full(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a, b;

    set_now(4000000);
    atomic_store(&g_app_target, UINT64_MAX);
    atomic_store(&g_app_calls, 0);
    atomic_store(&g_app_bad_ctx, 0);
    gate_prepare(1u << 0);
    pump_reset();
    CHECK(rig_up(&r, 1, 0, app_deadline_cb, &g_app_target,
                 sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL)
        return;
    CHECK(gate_await(0, 1));
    CHECK(atomic_load(&g_app_calls) == 1);
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);

    pump_arm(PUMP_APP, 2, 0, 0);
    set_now(4000100);
    atomic_store(&g_app_target, 4000100);
    moq_msq_test_bell_ring_raw(r.lane[0]);
    gate_release(0, 1);
    CHECK(pump_await_calls(0, 1));
    CHECK(gate_await(0, 2));

    set_now(4000200);
    atomic_store(&g_app_target, 4000200);
    moq_msq_test_bell_ring_raw(r.lane[0]);
    gate_release(0, 2);
    CHECK(pump_await_calls(0, 2));
    CHECK(gate_await(0, 3));
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);
    CHECK(atomic_load(&g_app_calls) == 5);
    CHECK(atomic_load(&g_app_bad_ctx) == 0);
    CHECK(b.deadline_sweeps == a.deadline_sweeps + 2);
    CHECK(b.pump_sweeps == a.pump_sweeps + 2);
    CHECK(b.service_passes == a.service_passes);
    CHECK(b.wake_to_pump_samples == a.wake_to_pump_samples);
    CHECK(b.idle_cap_wakes == a.idle_cap_wakes);
    rig_down(&r);
    if (failures == before)
        printf("PASS: app deadline drives two exact due transitions\n");
}

static void t_app_deadline_partial_gate(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a, b;

    set_now(5000000);
    atomic_store(&g_app_target, 5000000);
    atomic_store(&g_app_calls, 0);
    atomic_store(&g_app_bad_ctx, 0);
    gate_prepare(1u << 0);
    pump_reset();
    CHECK(rig_up(&r, 1, 0, app_deadline_cb, &g_app_target,
                 offsetof(moq_msquic_managed_cfg_t, app_deadline_ctx)));
    if (r.m == NULL)
        return;
    CHECK(gate_await(0, 1));
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);
    moq_msq_test_bell_ring_raw(r.lane[0]);
    gate_release(0, 1);
    CHECK(gate_await(0, 2));
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);
    CHECK(atomic_load(&g_app_calls) == 0);
    CHECK(atomic_load(&g_app_bad_ctx) == 0);
    CHECK(b.deadline_sweeps == a.deadline_sweeps);
    CHECK(b.pump_sweeps == a.pump_sweeps);
    rig_down(&r);
    if (failures == before)
        printf("PASS: partial app-deadline ABI block is ignored\n");
}

static void t_session_deadline(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_managed_conn_t *managed = NULL;
    moq_msquic_conn_t *adapter = NULL;
    moq_msquic_lane_stats_t a, b;

    set_now(6000000);
    gate_prepare(1u << 0);
    pump_reset();
    CHECK(rig_up(&r, 1, 500, NULL, NULL,
                 sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL)
        return;
    bool injected = inject_and_settle(&r, &managed, &adapter);
    CHECK(injected);
    CHECK(managed != NULL && adapter != NULL);
    if (!injected || managed == NULL || adapter == NULL) {
        rig_down(&r);
        return;
    }
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);
    pump_arm(PUMP_DRAIN, 0, 0, 0);
    set_now(6000500);
    moq_msq_test_bell_ring_raw(r.lane[0]);
    gate_release(0, 2);
    CHECK(pump_await_calls(0, 2));
    CHECK(gate_await(0, 3));
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);
    CHECK(b.deadline_sweeps == a.deadline_sweeps + 1);
    CHECK(b.pump_sweeps == a.pump_sweeps + 1);
    CHECK(b.service_passes == a.service_passes + 2);
    CHECK(b.wake_to_pump_samples == a.wake_to_pump_samples);

    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.session_closed == 1);
    CHECK(g_pump.other_events == 0);
    CHECK(g_pump.terminal_acks == 1);
    CHECK(g_pump.last_ack_rc == MOQ_OK);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(deliver_conn_event(&r.fake, adapter,
                             QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE,
                             0) == QUIC_STATUS_SUCCESS);
    gate_release(0, 3);
    CHECK(pump_await_calls(0, 3));
    CHECK(gate_await(0, 4));
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);
    CHECK(atomic_load(&r.fake.conn_closes) == 1);
    rig_down(&r);
    if (failures == before)
        printf("PASS: session deadline service and pump are exact\n");
}

/* --- fake accepted sends, concurrent reads, reap, and stop folding ------- */

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    moq_msquic_managed_lane_t *lane;
    _Atomic int stop;
    _Atomic int phase;
    uint64_t snapshots;
    bool started;
    bool saw_live;
    bool saw_reaped;
} g_poller = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void *poller_main(void *arg)
{
    (void)arg;
    pthread_mutex_lock(&g_poller.mu);
    g_poller.started = true;
    pthread_cond_broadcast(&g_poller.cv);
    pthread_mutex_unlock(&g_poller.mu);
    while (!atomic_load(&g_poller.stop)) {
        moq_msquic_lane_stats_t st;
        if (stats_of(g_poller.lane, &st) == MOQ_OK) {
            pthread_mutex_lock(&g_poller.mu);
            g_poller.snapshots++;
            if (st.flush_sends == 32 && st.flush_bytes == 256)
                g_poller.saw_live = true;
            if (atomic_load(&g_poller.phase) == 2 &&
                st.flush_sends == 32 && st.flush_bytes == 256)
                g_poller.saw_reaped = true;
            pthread_cond_broadcast(&g_poller.cv);
            pthread_mutex_unlock(&g_poller.mu);
        }
        sched_yield();
    }
    return NULL;
}

static bool poller_await(bool reaped)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_poller.mu);
    while (!(reaped ? g_poller.saw_reaped : g_poller.saw_live))
        if (pthread_cond_timedwait(&g_poller.cv, &g_poller.mu, &abs) != 0)
            break;
    reached = reaped ? g_poller.saw_reaped : g_poller.saw_live;
    pthread_mutex_unlock(&g_poller.mu);
    return reached;
}

static bool poller_start(pthread_t *thread,
                         moq_msquic_managed_lane_t *lane)
{
    struct timespec abs;
    bool started;

    pthread_mutex_lock(&g_poller.mu);
    g_poller.lane = lane;
    g_poller.snapshots = 0;
    g_poller.started = false;
    g_poller.saw_live = false;
    g_poller.saw_reaped = false;
    atomic_store(&g_poller.stop, 0);
    atomic_store(&g_poller.phase, 1);
    pthread_mutex_unlock(&g_poller.mu);
    if (pthread_create(thread, NULL, poller_main, NULL) != 0)
        return false;
    guard_deadline(&abs);
    pthread_mutex_lock(&g_poller.mu);
    while (!g_poller.started)
        if (pthread_cond_timedwait(&g_poller.cv, &g_poller.mu, &abs) != 0)
            break;
    started = g_poller.started;
    pthread_mutex_unlock(&g_poller.mu);
    if (!started) {
        atomic_store(&g_poller.stop, 1);
        pthread_join(*thread, NULL);
    }
    return started;
}

static void poller_stop(pthread_t thread)
{
    atomic_store(&g_poller.stop, 1);
    pthread_join(thread, NULL);
}

static void t_flush_poll_and_reap(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_managed_conn_t *managed = NULL;
    moq_msquic_conn_t *adapter = NULL;
    moq_msquic_lane_stats_t base, live, reaped;
    pthread_t poller;
    bool poller_running = false;

    set_now(7000000);
    gate_prepare(1u << 0);
    pump_reset();
    CHECK(rig_up(&r, 1, 0, NULL, NULL, sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL)
        return;
    bool injected = inject_and_settle(&r, &managed, &adapter);
    CHECK(injected);
    CHECK(managed != NULL && adapter != NULL);
    if (!injected || managed == NULL || adapter == NULL) {
        rig_down(&r);
        return;
    }
    CHECK(stats_of(r.lane[0], &base) == MOQ_OK);
    poller_running = poller_start(&poller, r.lane[0]);
    CHECK(poller_running);
    if (!poller_running) {
        /* Fail here, not two guards later: with no poller the awaits below
         * would each burn their full GUARD_US bound before reporting. Nothing
         * is owed — poller_start joins the thread itself when the start times
         * out, and a refused pthread_create leaves none to join. */
        rig_down(&r);
        return;
    }

    pump_arm(PUMP_FLUSH, 1, 32, 8);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    set_now(7000011);
    gate_release(0, 2);
    CHECK(pump_await_action());
    CHECK(pump_await_calls(0, 2));
    CHECK(gate_await(0, 3));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.open_rc == MOQ_TRANSPORT_OK);
    CHECK(g_pump.write_rc == MOQ_TRANSPORT_OK);
    CHECK(g_pump.managed_conn == managed);
    CHECK(g_pump.adapter == adapter);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(r.fake.send_count == 32);
    CHECK(fake_msq_pending_sends(&r.fake) == 32);
    CHECK(moq_msquic_conn_pending_sends(adapter) == 32);
    for (int i = 0; i < r.fake.send_count; i++)
        CHECK(r.fake.sends[i].total == 8);
    CHECK(stats_of(r.lane[0], &live) == MOQ_OK);
    bool live_exact = live.flush_sends == 32 && live.flush_bytes == 256;
    CHECK(live_exact);
    CHECK(live.service_passes == base.service_passes + 1);
    if (live_exact)
        CHECK(poller_await(false));

    CHECK(fake_msq_deliver_send_complete(&r.fake, false));
    CHECK(fake_msq_pending_sends(&r.fake) == 31);
    CHECK(moq_msquic_conn_pending_sends(adapter) == 31);
    while (fake_msq_deliver_send_complete(&r.fake, false))
        ;
    CHECK(fake_msq_pending_sends(&r.fake) == 0);
    CHECK(moq_msquic_conn_pending_sends(adapter) == 0);
    gate_release(0, 3);
    CHECK(pump_await_calls(0, 3));
    CHECK(gate_await(0, 4));
    CHECK(stats_of(r.lane[0], &live) == MOQ_OK);
    CHECK(live.flush_sends == 32 && live.flush_bytes == 256);

    pump_arm(PUMP_DRAIN, 0, 0, 0);
    CHECK(deliver_conn_event(
              &r.fake, adapter,
              QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER,
              0x47) == QUIC_STATUS_SUCCESS);
    gate_release(0, 4);
    CHECK(pump_await_calls(0, 4));
    CHECK(gate_await(0, 5));
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.session_closed == 1);
    CHECK(g_pump.other_events == 0);
    CHECK(g_pump.terminal_acks == 1);
    CHECK(g_pump.last_ack_rc == MOQ_OK);
    pthread_mutex_unlock(&g_pump.mu);

    CHECK(deliver_conn_event(&r.fake, adapter,
                             QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE,
                             0) == QUIC_STATUS_SUCCESS);
    gate_release(0, 5);
    CHECK(pump_await_calls(0, 5));
    CHECK(gate_await(0, 6));
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);
    CHECK(atomic_load(&r.fake.conn_closes) == 1);
    CHECK(stats_of(r.lane[0], &reaped) == MOQ_OK);
    bool reaped_exact =
        reaped.flush_sends == 32 && reaped.flush_bytes == 256;
    CHECK(reaped_exact);
    atomic_store(&g_poller.phase, 2);
    if (reaped_exact)
        CHECK(poller_await(true));
    if (poller_running) {
        poller_stop(poller);
        poller_running = false;
    }
    pthread_mutex_lock(&g_poller.mu);
    CHECK(g_poller.snapshots > 0);
    pthread_mutex_unlock(&g_poller.mu);
    rig_down(&r);
    if (failures == before)
        printf("PASS: accepted sends survive ordinary reap under polling\n");
}

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool called;
    bool bad_ctx;
} g_shutdown = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void shutdown_hook(fake_msq_t *fake, void *ctx)
{
    (void)fake;
    pthread_mutex_lock(&g_shutdown.mu);
    if (ctx != &g_shutdown)
        g_shutdown.bad_ctx = true;
    g_shutdown.called = true;
    pthread_cond_broadcast(&g_shutdown.cv);
    pthread_mutex_unlock(&g_shutdown.mu);
}

static bool shutdown_await(void)
{
    struct timespec abs;
    bool called;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_shutdown.mu);
    while (!g_shutdown.called)
        if (pthread_cond_timedwait(&g_shutdown.cv, &g_shutdown.mu, &abs) != 0)
            break;
    called = g_shutdown.called;
    pthread_mutex_unlock(&g_shutdown.mu);
    return called;
}

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

static void t_flush_survives_stop(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_managed_conn_t *managed = NULL;
    moq_msquic_conn_t *adapter = NULL;
    moq_msquic_lane_stats_t pre, post;
    struct stop_call call;
    pthread_t thread;

    set_now(8000000);
    gate_prepare(1u << 0);
    pump_reset();
    CHECK(rig_up(&r, 1, 0, NULL, NULL, sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL)
        return;
    bool injected = inject_and_settle(&r, &managed, &adapter);
    CHECK(injected);
    CHECK(managed != NULL && adapter != NULL);
    if (!injected || managed == NULL || adapter == NULL) {
        rig_down(&r);
        return;
    }
    pump_arm(PUMP_FLUSH, 1, 3, 73);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    set_now(8000013);
    gate_release(0, 2);
    CHECK(pump_await_action());
    CHECK(pump_await_calls(0, 2));
    CHECK(gate_await(0, 3));
    CHECK(r.fake.send_count == 3);
    CHECK(fake_msq_pending_sends(&r.fake) == 3);
    CHECK(moq_msquic_conn_pending_sends(adapter) == 3);
    for (int i = 0; i < r.fake.send_count; i++)
        CHECK(r.fake.sends[i].total == 73);
    while (fake_msq_deliver_send_complete(&r.fake, false))
        ;
    CHECK(fake_msq_pending_sends(&r.fake) == 0);
    CHECK(moq_msquic_conn_pending_sends(adapter) == 0);
    gate_release(0, 3);
    CHECK(pump_await_calls(0, 3));
    CHECK(gate_await(0, 4));
    CHECK(stats_of(r.lane[0], &pre) == MOQ_OK);
    CHECK(pre.flush_sends == 3 && pre.flush_bytes == 219);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    gate_disable();
    pthread_mutex_lock(&g_shutdown.mu);
    g_shutdown.called = false;
    g_shutdown.bad_ctx = false;
    pthread_mutex_unlock(&g_shutdown.mu);
    r.fake.on_conn_shutdown = shutdown_hook;
    r.fake.on_conn_shutdown_ctx = &g_shutdown;
    call.m = r.m;
    call.rc = MOQ_ERR_INTERNAL;
    int thread_rc = pthread_create(&thread, NULL, stop_main, &call);
    CHECK(thread_rc == 0);
    if (thread_rc != 0) {
        r.fake.on_conn_shutdown = NULL;
        r.fake.on_conn_shutdown_ctx = NULL;
        CHECK(deliver_conn_event(&r.fake, adapter,
                                 QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE,
                                 0) == QUIC_STATUS_SUCCESS);
        rig_down(&r);
        return;
    }
    CHECK(shutdown_await());
    pthread_mutex_lock(&g_shutdown.mu);
    CHECK(!g_shutdown.bad_ctx);
    pthread_mutex_unlock(&g_shutdown.mu);
    CHECK(atomic_load(&r.fake.conn_shutdowns) == 1);
    CHECK(r.fake.last_conn_shutdown_code == 0);
    CHECK(deliver_conn_event(&r.fake, adapter,
                             QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE,
                             0) == QUIC_STATUS_SUCCESS);
    CHECK(pthread_join(thread, NULL) == 0);
    CHECK(call.rc == MOQ_OK);
    CHECK(atomic_load(&r.fake.conn_closes) == 1);
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);
    CHECK(stats_of(r.lane[0], &post) == MOQ_OK);
    CHECK(post.flush_sends == 3);
    CHECK(post.flush_bytes == 219);
    r.fake.on_conn_shutdown = NULL;
    r.fake.on_conn_shutdown_ctx = NULL;
    moq_msquic_managed_destroy(r.m);
    r.m = NULL;
    if (failures == before)
        printf("PASS: accepted sends survive stop cleanup exactly\n");
}


/* --- the no-work deadline scan ------------------------------------------- *
 *
 * The doorbell's no-work path queries EVERY present connection for its next
 * deadline, and each query walks that session's capacity tables. Nothing
 * counted that: `deadline_sweeps` counts only a deadline that actually FIRED,
 * so an idle lane reports zero sweeps while paying for one full scan per pass.
 * These cases pin the scan itself — how many queries a step makes, how they
 * classify, and that the population is stable across steps — with no clock
 * oracle: virtual time is set explicitly and every step is a single production
 * `mgd_doorbell_step` on this thread.
 */

/* One no-work step with N active-idle children. */
static void scan_case(unsigned n)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a, b;

    set_now(9000000);
    moq_msq_test_no_doorbell = true;
    CHECK(rig_up(&r, 1, 0, NULL, NULL, sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL) {
        moq_msq_test_no_doorbell = false;
        return;
    }
    for (unsigned i = 0; i < n; i++)
        CHECK(moq_msq_test_lane_inject_idle_child(r.lane[0]));

    /* Every injected child must be LIVE and IDLE, not merely present: a
     * terminal or reapable child is a different population and would make the
     * scan counts mean something else. */
    moq_msq_test_lane_row_t row;
    moq_msq_test_child_row_t kids[4];
    CHECK(n <= 4u);
    CHECK(moq_msq_test_lane_snapshot(r.lane[0], &row, kids, 4) == n);
    CHECK(row.conn_count == n);
    CHECK(!row.pump_pending);
    CHECK(!row.wake_pending);
    for (unsigned i = 0; i < n; i++) {
        CHECK(!kids[i].shutdown_complete);
        CHECK(!kids[i].reapable);
        CHECK(!kids[i].app_terminal_acked);
        CHECK(!kids[i].terminal_enqueued);
        CHECK(!kids[i].terminal_observed);
        CHECK(!kids[i].bridge_fatal);
        CHECK(kids[i].bridge_fatal_code == 0);
        CHECK(kids[i].close_feed_commits == 0);
    }

    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);
    CHECK(moq_msq_test_lane_step(r.lane[0]) == MOQ_MSQ_TEST_STEP_IDLE);
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);

    /* exactly one query per present connection */
    CHECK(b.deadline_queries == a.deadline_queries + n);
    /* the partition is complete AND exact: an idle session with no timeout
     * has no deadline at all, so every query must land in `none`. A sum-only
     * check would let a query move between classes unnoticed. */
    CHECK((b.deadline_class_none - a.deadline_class_none) +
              (b.deadline_class_future - a.deadline_class_future) +
              (b.deadline_class_due - a.deadline_class_due) ==
          b.deadline_queries - a.deadline_queries);
    CHECK(b.deadline_class_none == a.deadline_class_none + n);
    CHECK(b.deadline_class_future == a.deadline_class_future);
    CHECK(b.deadline_class_due == a.deadline_class_due);
    /* no deadline fired, so the existing counter stays flat while the scan
     * scales -- the exact gap this slice exists to make visible */
    CHECK(b.deadline_sweeps == a.deadline_sweeps);
    CHECK(b.service_passes == a.service_passes);

    /* the population survives repeated steps until the test tears it down */
    CHECK(moq_msq_test_lane_step(r.lane[0]) == MOQ_MSQ_TEST_STEP_IDLE);
    moq_msquic_lane_stats_t c;
    CHECK(stats_of(r.lane[0], &c) == MOQ_OK);
    CHECK(c.deadline_queries == b.deadline_queries + n);
    CHECK(moq_msq_test_lane_snapshot(r.lane[0], &row, kids, 4) == n);
    CHECK(row.conn_count == n);
    CHECK(c.deadline_sweeps == a.deadline_sweeps);
    /* still live and idle after repeated steps */
    for (unsigned i = 0; i < n; i++) {
        CHECK(!kids[i].shutdown_complete);
        CHECK(!kids[i].reapable);
        CHECK(!kids[i].terminal_enqueued);
        CHECK(!kids[i].bridge_fatal);
        CHECK(kids[i].close_feed_commits == 0);
    }
    CHECK(!row.pump_pending);

    rig_down(&r);
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: no-work scan queries exactly %u of %u children\n", n, n);
}

static void t_deadline_scan_counts(void)
{
    scan_case(0);
    scan_case(1);
    scan_case(3);
}

/* Sessions that DO have a deadline, still in the future: the queries must all
 * classify as `future`, which is what distinguishes the class rule from a
 * bare count. */
static void t_deadline_scan_future_class(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a, b;
    const unsigned n = 3;

    set_now(9700000);
    moq_msq_test_no_doorbell = true;
    /* a session idle timeout gives every child a real, later deadline */
    CHECK(rig_up(&r, 1, 1000000, NULL, NULL,
                 sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL) {
        moq_msq_test_no_doorbell = false;
        return;
    }
    for (unsigned i = 0; i < n; i++)
        CHECK(moq_msq_test_lane_inject_idle_child(r.lane[0]));
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);
    CHECK(moq_msq_test_lane_step(r.lane[0]) == MOQ_MSQ_TEST_STEP_IDLE);
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);

    CHECK(b.deadline_queries == a.deadline_queries + n);
    CHECK(b.deadline_class_future == a.deadline_class_future + n);
    CHECK(b.deadline_class_none == a.deadline_class_none);
    CHECK(b.deadline_class_due == a.deadline_class_due);
    CHECK(b.deadline_sweeps == a.deadline_sweeps);

    rig_down(&r);
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: future deadlines classify as future, not none\n");
}

/* A due deadline visits the complete set: every child is serviced once. */
static void t_deadline_scan_due(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a, b;
    const unsigned n = 3;

    set_now(9500000);
    moq_msq_test_no_doorbell = true;
    atomic_store(&g_app_target, UINT64_MAX);
    CHECK(rig_up(&r, 1, 0, app_deadline_cb, &g_app_target,
                 sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL) {
        moq_msq_test_no_doorbell = false;
        return;
    }
    for (unsigned i = 0; i < n; i++)
        CHECK(moq_msq_test_lane_inject_idle_child(r.lane[0]));
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);

    /* an application deadline that is already due at the injected clock */
    atomic_store(&g_app_target, 9500000);
    CHECK(moq_msq_test_lane_step(r.lane[0]) == MOQ_MSQ_TEST_STEP_TICKED);
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);

    /* the scan still queried every child once ... */
    CHECK(b.deadline_queries == a.deadline_queries + n);
    /* ... one sweep fired, and every child was serviced exactly once */
    CHECK(b.deadline_sweeps == a.deadline_sweeps + 1);
    /* Two passes per child, and that is the production identity, not a
     * tolerance: the due branch services every child once, then the pump
     * sweep drains every child once so what the pump queued reaches the
     * wire. Neither visit is allowed to skip a child. */
    CHECK(b.service_passes == a.service_passes + 2u * n);
    CHECK(b.pump_sweeps == a.pump_sweeps + 1);

    atomic_store(&g_app_target, UINT64_MAX);
    rig_down(&r);
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: a due deadline services the complete set\n");
}


/* A real per-connection DUE class: sessions with an idle timeout, and virtual
 * time advanced past it. The application-deadline case above proves a service
 * identity, not this: there every per-connection query still returns
 * UINT64_MAX and classifies `none`. */
static void t_deadline_scan_due_class(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_lane_stats_t a, b;
    const unsigned n = 3;

    set_now(9800000);
    moq_msq_test_no_doorbell = true;
    CHECK(rig_up(&r, 1, 1000000, NULL, NULL,
                 sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL) {
        moq_msq_test_no_doorbell = false;
        return;
    }
    for (unsigned i = 0; i < n; i++)
        CHECK(moq_msq_test_lane_inject_idle_child(r.lane[0]));
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);

    /* past every child's session deadline */
    set_now(9800000 + 2000000);
    moq_msq_test_step_t st = moq_msq_test_lane_step(r.lane[0]);
    CHECK(st == MOQ_MSQ_TEST_STEP_TICKED);
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);

    CHECK(b.deadline_queries == a.deadline_queries + n);
    CHECK(b.deadline_class_due == a.deadline_class_due + n);
    CHECK(b.deadline_class_none == a.deadline_class_none);
    CHECK(b.deadline_class_future == a.deadline_class_future);
    /* the partition still adds up */
    CHECK((b.deadline_class_none - a.deadline_class_none) +
              (b.deadline_class_future - a.deadline_class_future) +
              (b.deadline_class_due - a.deadline_class_due) ==
          b.deadline_queries - a.deadline_queries);
    /* and a due minimum fires exactly one sweep */
    CHECK(b.deadline_sweeps == a.deadline_sweeps + 1);

    rig_down(&r);
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: session deadlines past now classify as due\n");
}

/* The diagnostic must not add per-connection clock reads to the O(N) path:
 * an idle step's clock-call count is constant as N grows. */
static void t_scan_clock_calls_constant(void)
{
    int before = failures;
    uint64_t per_n[3];
    const unsigned ns[3] = { 0u, 1u, 3u };

    for (unsigned k = 0; k < 3u; k++) {
        struct rig r;

        set_now(9900000);
        moq_msq_test_no_doorbell = true;
        CHECK(rig_up(&r, 1, 0, NULL, NULL,
                     sizeof(moq_msquic_managed_cfg_t)));
        if (r.m == NULL) {
            moq_msq_test_no_doorbell = false;
            return;
        }
        for (unsigned i = 0; i < ns[k]; i++)
            CHECK(moq_msq_test_lane_inject_idle_child(r.lane[0]));
        atomic_store(&g_now_calls, 0);
        CHECK(moq_msq_test_lane_step(r.lane[0]) == MOQ_MSQ_TEST_STEP_IDLE);
        per_n[k] = atomic_load(&g_now_calls);
        rig_down(&r);
        moq_msq_test_no_doorbell = false;
    }
    /* Constant, not merely sublinear: one classification instant per scan
     * plus the scheduler's own reads, none of them per connection. */
    CHECK(per_n[0] == per_n[1]);
    CHECK(per_n[1] == per_n[2]);
    if (failures != before)
        printf("  [clock] calls per idle step: N=0 %llu  N=1 %llu  N=3 %llu\n",
               (unsigned long long)per_n[0], (unsigned long long)per_n[1],
               (unsigned long long)per_n[2]);
    if (failures == before)
        printf("PASS: idle-step clock reads are constant in N\n");
}

/* A v0-sized caller gets the frozen prefix and nothing else: the diagnostic
 * fields must never be written into a buffer that does not declare room for
 * them. The frozen boundary here is the literal measured v0 size, not
 * MOQ_MSQUIC_LANE_STATS_V0_SIZE, so a layout shift cannot move the oracle
 * along with the bug. */
static void t_v0_sized_caller_untouched(void)
{
    int before = failures;
    struct rig r;
    enum { V0_BYTES = 112u, PAD = 128u };
    unsigned char buf[V0_BYTES + PAD];

    set_now(9700000);
    moq_msq_test_no_doorbell = true;
    CHECK(rig_up(&r, 1, 1000000, NULL, NULL,
                 sizeof(moq_msquic_managed_cfg_t)));
    if (r.m == NULL) {
        moq_msq_test_no_doorbell = false;
        return;
    }
    for (unsigned i = 0; i < 3u; i++)
        CHECK(moq_msq_test_lane_inject_idle_child(r.lane[0]));
    /* run the instrumented scan so the diagnostic fields are non-zero and a
     * stray write would be visible rather than coincidentally zero */
    set_now(9700000 + 2000000);
    CHECK(moq_msq_test_lane_step(r.lane[0]) == MOQ_MSQ_TEST_STEP_TICKED);

    memset(buf, 0xA5, sizeof(buf));
    CHECK(moq_msquic_lane_get_stats(
              r.lane[0], (moq_msquic_lane_stats_t *)(void *)buf, V0_BYTES) ==
          MOQ_OK);

    /* exactly V0_BYTES written, self-declared */
    uint32_t stamped;
    memcpy(&stamped, buf, sizeof(stamped));
    CHECK(stamped == V0_BYTES);
    /* every byte past the frozen prefix is still the poison */
    bool clean = true;
    for (unsigned i = V0_BYTES; i < sizeof(buf); i++)
        if (buf[i] != 0xA5u)
            clean = false;
    CHECK(clean);
    /* and the prefix really did carry the v0 payload */
    uint64_t fb;
    memcpy(&fb, buf + (V0_BYTES - sizeof(uint64_t)), sizeof(fb));
    CHECK(fb != 0xA5A5A5A5A5A5A5A5ull);

    rig_down(&r);
    moq_msq_test_no_doorbell = false;
    if (failures == before)
        printf("PASS: a v0-sized caller receives only the frozen prefix\n");
}

int main(void)
{
    moq_msq_test_now_us = test_now_us;
    moq_msq_test_prewait = prewait_hook;

    t_invalid_and_prefix();
    t_idle_classification();
    t_external_wake();
    t_callback_contexts();
    t_app_deadline_full();
    t_app_deadline_partial_gate();
    t_session_deadline();
    t_flush_poll_and_reap();
    t_flush_survives_stop();
    t_deadline_scan_counts();
    t_deadline_scan_future_class();
    t_deadline_scan_due_class();
    t_scan_clock_calls_constant();
    t_v0_sized_caller_untouched();
    t_deadline_scan_due();

    gate_disable();
    moq_msq_test_prewait = NULL;
    moq_msq_test_now_us = NULL;
    if (failures == 0)
        printf("PASS: msquic_lane_stats\n");
    else
        fprintf(stderr, "FAIL: msquic_lane_stats (%d)\n", failures);
    return failures;
}
