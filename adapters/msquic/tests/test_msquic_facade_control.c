/*
 * Facade-level control surface, deterministically: drain refuses new
 * admissions while releasing the reserve, stop is idempotent, the facade's
 * pending-send/datagram aggregates really are the sum across lanes, and a
 * SERVER facade with a live child reports a bounded wait as MOQ_DONE rather
 * than claiming it is terminal.
 *
 * The facade is the lanes-only testing constructor: no listener, socket,
 * credential or certificate exists here, and `mgd_terminal()` branches on
 * stop/pump-exit (and, for a client only, the connection terminal latches)
 * rather than on a listener, so a listener-free server is the right shape
 * for the wait row. Real listener creation stays owned by the conformance
 * rail.
 *
 * Admissions run through the exact production listener callback; children
 * that must emit traffic are injected and driven through the production
 * endpoint ops over per-connection fake tables, so a lane's contribution is
 * attributable to that lane.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "msquic_internal.h"

#include "support/fake_msq_table.h"

#include <moq/msquic_managed.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* MOQ_MSQUIC_TESTING-only seams. */
extern void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out);
extern QUIC_STATUS moq_msq_test_listener_accept(moq_msquic_managed_t *m,
                                                HQUIC connection);
extern moq_result_t moq_msq_test_lane_inject_live_child(
    moq_msquic_managed_lane_t *lane, HQUIC connection,
    moq_msquic_managed_conn_t **out);
extern moq_msquic_conn_t *moq_msq_test_managed_conn_adapter(
    moq_msquic_managed_conn_t *conn);

enum { MAX_LANES = 2, MAX_CONNS = 6, GUARD_US = 5 * 1000 * 1000 };

/* Every bounded wait here is a fail-closed hang guard, never a verdict:
 * expiry fails the case rather than deciding it. */
static void guard_deadline(struct timespec *abs)
{
    clock_gettime(CLOCK_REALTIME, abs);
    abs->tv_sec += GUARD_US / 1000000;
}

/* --- pre-wait gate: one released entry is one parked generation -------- */

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool enabled[MAX_LANES];
    uint64_t entered[MAX_LANES];
    uint64_t released[MAX_LANES];
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
    for (unsigned i = 0; i < MAX_LANES; i++)
        g_gate.enabled[i] = (mask & (1u << i)) != 0;
    g_gate.expired = false;
    pthread_mutex_unlock(&g_gate.mu);
}

static void prewait_hook(moq_msquic_managed_lane_t *lane)
{
    uint32_t idx = moq_msquic_lane_index(lane);
    struct timespec abs;

    if (idx >= MAX_LANES)
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
    while (idx < MAX_LANES && g_gate.entered[idx] < want && !g_gate.expired)
        if (pthread_cond_timedwait(&g_gate.cv, &g_gate.mu, &abs) != 0)
            break;
    reached = idx < MAX_LANES && g_gate.entered[idx] >= want &&
              !g_gate.expired;
    pthread_mutex_unlock(&g_gate.mu);
    return reached;
}

static void gate_release(uint32_t idx, uint64_t entry)
{
    pthread_mutex_lock(&g_gate.mu);
    if (idx < MAX_LANES && g_gate.released[idx] < entry)
        g_gate.released[idx] = entry;
    pthread_cond_broadcast(&g_gate.cv);
    pthread_mutex_unlock(&g_gate.mu);
}

static void gate_disable(void)
{
    pthread_mutex_lock(&g_gate.mu);
    for (unsigned i = 0; i < MAX_LANES; i++) {
        g_gate.enabled[i] = false;
        g_gate.released[i] = UINT64_MAX;
    }
    pthread_cond_broadcast(&g_gate.cv);
    pthread_mutex_unlock(&g_gate.mu);
}

/* --- pump: quiet by default, or emits a declared number of sends ------- */

static struct {
    pthread_mutex_t mu;
    unsigned flush_left[MAX_LANES]; /* generations still owing sends */
    unsigned flush_count[MAX_LANES];
    unsigned dgram_count[MAX_LANES];
    unsigned flush_len;
    int open_rc[MAX_LANES];
    int write_rc[MAX_LANES];
} g_pump = { .mu = PTHREAD_MUTEX_INITIALIZER };

static void pump_reset(void)
{
    pthread_mutex_lock(&g_pump.mu);
    memset(g_pump.flush_left, 0, sizeof(g_pump.flush_left));
    memset(g_pump.flush_count, 0, sizeof(g_pump.flush_count));
    memset(g_pump.dgram_count, 0, sizeof(g_pump.dgram_count));
    g_pump.flush_len = 8;
    for (unsigned i = 0; i < MAX_LANES; i++) {
        g_pump.open_rc[i] = MOQ_TRANSPORT_OK;
        g_pump.write_rc[i] = MOQ_TRANSPORT_OK;
    }
    pthread_mutex_unlock(&g_pump.mu);
}

static void pump_arm_flush(uint32_t idx, unsigned sends, unsigned dgrams)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.flush_left[idx] = 1;
    g_pump.flush_count[idx] = sends;
    g_pump.dgram_count[idx] = dgrams;
    pthread_mutex_unlock(&g_pump.mu);
}

static int test_pump(moq_msquic_managed_t *m,
                     moq_msquic_managed_lane_t *lane, uint64_t now_us,
                     void *user)
{
    uint32_t idx = moq_msquic_lane_index(lane);
    uint8_t bytes[64];

    (void)m;
    (void)now_us;
    (void)user;
    if (idx >= MAX_LANES)
        return 0;
    pthread_mutex_lock(&g_pump.mu);
    if (g_pump.flush_left[idx] == 0) {
        pthread_mutex_unlock(&g_pump.mu);
        return 0;
    }
    g_pump.flush_left[idx]--;
    unsigned count = g_pump.flush_count[idx];
    unsigned dgrams = g_pump.dgram_count[idx];
    unsigned len = g_pump.flush_len;
    pthread_mutex_unlock(&g_pump.mu);

    moq_msquic_managed_conn_t *mc = moq_msquic_lane_next_conn(lane, NULL);

    if (mc == NULL || len > sizeof(bytes))
        return 0;
    moq_msquic_conn_t *adapter = moq_msq_test_managed_conn_adapter(mc);
    const moq_transport_endpoint_ops_t *ops = moq_msquic_test_ops(adapter);
    uint64_t sid = 0;
    int open_rc = ops->open_uni(adapter, &sid);
    int write_rc = MOQ_TRANSPORT_OK;

    memset(bytes, 0x6b, sizeof(bytes));
    if (open_rc == MOQ_TRANSPORT_OK) {
        for (unsigned i = 0; i < count; i++) {
            write_rc = ops->write(adapter, sid, bytes, len, false);
            if (write_rc != MOQ_TRANSPORT_OK)
                break;
            moq_msquic_test_flush(adapter);
        }
    }
    for (unsigned i = 0; i < dgrams; i++)
        if (ops->send_datagram(adapter, bytes, len) != MOQ_TRANSPORT_OK) {
            write_rc = MOQ_TRANSPORT_ERROR;
            break;
        }
    pthread_mutex_lock(&g_pump.mu);
    g_pump.open_rc[idx] = open_rc;
    g_pump.write_rc[idx] = write_rc;
    pthread_mutex_unlock(&g_pump.mu);
    return 0;
}

/* --- chooser: counts calls, and can drain from inside itself ----------- */

static struct {
    pthread_mutex_t mu;
    unsigned calls;
    bool drain_from_chooser; /* exercise a drain that lands mid-accept */
    uint32_t lane;
} g_chooser = { .mu = PTHREAD_MUTEX_INITIALIZER };

static void chooser_reset(bool drain_from_chooser, uint32_t lane)
{
    pthread_mutex_lock(&g_chooser.mu);
    g_chooser.calls = 0;
    g_chooser.drain_from_chooser = drain_from_chooser;
    g_chooser.lane = lane;
    pthread_mutex_unlock(&g_chooser.mu);
}

static unsigned chooser_calls(void)
{
    unsigned v;

    pthread_mutex_lock(&g_chooser.mu);
    v = g_chooser.calls;
    pthread_mutex_unlock(&g_chooser.mu);
    return v;
}

/* Runs with NO adapter lock held, which is what makes the drain below a
 * legal public call from here. */
static uint32_t counting_chooser(moq_msquic_managed_t *m,
                                 const moq_msquic_accept_info_t *info,
                                 void *user)
{
    bool drain;
    uint32_t lane;

    (void)info;
    (void)user;
    pthread_mutex_lock(&g_chooser.mu);
    g_chooser.calls++;
    drain = g_chooser.drain_from_chooser;
    lane = g_chooser.lane;
    pthread_mutex_unlock(&g_chooser.mu);
    if (drain)
        moq_msquic_managed_drain(m);
    return lane;
}

/* --- rig --------------------------------------------------------------- */

static fake_msq_t g_table_fake;
static fake_msq_t g_conn_fake[MAX_CONNS];

struct rig {
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane[MAX_LANES];
    uint32_t lane_count;
    bool live[MAX_CONNS];
    /* An ADMITTED child carries the handler the listener registered; an
     * INJECTED one does not, so its terminal must be delivered through the
     * production connection callback with the adapter as context. */
    moq_msquic_conn_t *adapter[MAX_CONNS];
};

static bool rig_up_chooser(struct rig *r, uint32_t lane_count,
                           uint32_t max_conns,
                           moq_msquic_choose_lane_fn chooser)
{
    moq_msquic_managed_cfg_t cfg;

    memset(r, 0, sizeof(*r));
    fake_msq_init(&g_table_fake, false);
    for (int i = 0; i < MAX_CONNS; i++)
        fake_msq_init(&g_conn_fake[i], false);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.max_connections = max_conns;
    cfg.lane_count = lane_count;
    cfg.on_lane_pump = test_pump;
    cfg.choose_lane = chooser;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    if (moq_msq_test_managed_create_lanes_only_api(
            &cfg, fake_msq_table(&g_table_fake), &r->m) != MOQ_OK)
        return false;
    r->lane_count = lane_count;
    if (moq_msquic_managed_lane_count(r->m) != lane_count)
        return false;
    for (uint32_t i = 0; i < lane_count; i++) {
        r->lane[i] = moq_msquic_managed_lane(r->m, i);
        if (r->lane[i] == NULL)
            return false;
    }
    return true;
}

static bool rig_up(struct rig *r, uint32_t lane_count, uint32_t max_conns)
{
    return rig_up_chooser(r, lane_count, max_conns, NULL);
}

static QUIC_STATUS rig_accept(struct rig *r, int slot)
{
    QUIC_STATUS st = moq_msq_test_listener_accept(
        r->m, fake_msq_conn_handle(&g_conn_fake[slot]));

    if (QUIC_SUCCEEDED(st))
        r->live[slot] = true;
    return st;
}

/* Datagrams are refused until the peer advertises a size, so each child
 * that must send them is handed the state change a real peer would. */
static void enable_datagrams(struct rig *r, int slot, uint16_t max)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED;
    ev.DATAGRAM_STATE_CHANGED.SendEnabled = TRUE;
    ev.DATAGRAM_STATE_CHANGED.MaxSendLength = max;
    (void)moq_msquic_conn_callback()(
        fake_msq_conn_handle(&g_conn_fake[slot]), r->adapter[slot], &ev);
}

/* Retire one in-flight datagram the way MsQuic finalizes it. */
static bool complete_one_datagram(struct rig *r, int slot)
{
    void *ctx = fake_msq_next_dgram_ctx(&g_conn_fake[slot]);
    QUIC_CONNECTION_EVENT ev;

    if (ctx == NULL)
        return false;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_DATAGRAM_SEND_STATE_CHANGED;
    ev.DATAGRAM_SEND_STATE_CHANGED.ClientContext = ctx;
    /* only a FINAL state retires the send: SENT is not one */
    ev.DATAGRAM_SEND_STATE_CHANGED.State = QUIC_DATAGRAM_SEND_ACKNOWLEDGED;
    (void)moq_msquic_conn_callback()(
        fake_msq_conn_handle(&g_conn_fake[slot]), r->adapter[slot], &ev);
    return true;
}

/* The transport terminal a real shutdown delivers. stop() waits for it, so
 * every live child owes one before teardown. */
static void deliver_shutdown_complete(struct rig *r, int slot)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE;
    if (r->adapter[slot] != NULL)
        (void)moq_msquic_conn_callback()(
            fake_msq_conn_handle(&g_conn_fake[slot]), r->adapter[slot], &ev);
    else if (fake_msq_conn_cb_installed(&g_conn_fake[slot]))
        (void)fake_msq_deliver_conn_event(&g_conn_fake[slot], &ev);
    r->live[slot] = false;
}

static void rig_down(struct rig *r)
{
    if (r->m == NULL)
        return;
    gate_disable();
    for (int i = 0; i < MAX_CONNS; i++) {
        if (!r->live[i])
            continue;
        deliver_shutdown_complete(r, i);
    }
    (void)moq_msquic_managed_stop(r->m);
    moq_msquic_managed_destroy(r->m);
    r->m = NULL;
}

/* Every operation the shared fake records for a connection identity that
 * was refused: nothing may have touched it. */
static void check_untouched(const fake_msq_t *f)
{
    CHECK(!fake_msq_conn_cb_installed(f));
    CHECK(atomic_load(&f->conn_set_configs) == 0);
    CHECK(atomic_load(&f->conn_shutdowns) == 0);
    CHECK(atomic_load(&f->conn_closes) == 0);
    CHECK(f->stream_count == 0);
    CHECK(f->send_count == 0);
    CHECK(f->dgram_count == 0);
}

/* --- 1. drain refuses new admissions and releases the reserve ---------- */

static void t_drain_refuses_and_releases_reserve(void)
{
    int before = failures;
    struct rig r;

    gate_prepare(0);
    pump_reset();
    if (!rig_up(&r, 1, 4)) {
        CHECK(0 && "rig up");
        return;
    }

    /* room for four, one admitted */
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_SUCCESS);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    moq_msquic_managed_drain(r.m);

    /* the cap is nowhere near reached: only draining refuses this */
    CHECK(rig_accept(&r, 1) == QUIC_STATUS_CONNECTION_REFUSED);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);
    check_untouched(&g_conn_fake[1]);
    /* the refusal released its reserve rather than consuming a slot: a
     * second refusal reports the same count, not a decreasing budget */
    CHECK(rig_accept(&r, 2) == QUIC_STATUS_CONNECTION_REFUSED);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);
    check_untouched(&g_conn_fake[2]);

    /* draining is not terminal for the facade itself */
    CHECK(!moq_msquic_managed_is_closed(r.m));
    CHECK(!moq_msquic_managed_is_fatal(r.m));

    rig_down(&r);
    if (failures == before)
        printf("PASS: drain_refuses_and_releases_reserve\n");
}

/* --- 1b. the two drain checks are independently load-bearing ----------- */

/*
 * The listener consults `draining` TWICE, and the chooser runs between
 * them with no adapter lock held. That gap is what makes each check
 * separately observable:
 *
 *   - already draining when the accept arrives: the FIRST check must
 *     refuse before the chooser is ever consulted, so a chooser call
 *     count of zero is the oracle;
 *   - drain landing DURING the accept (issued from inside the chooser,
 *     which is a legal public call from there): the first check saw a
 *     live facade, so only the SECOND check can refuse.
 *
 * Neither row can be satisfied by the other check doing the work.
 */
static void t_drain_refused_before_the_chooser(void)
{
    int before = failures;
    struct rig r;

    gate_prepare(0);
    pump_reset();
    chooser_reset(false, 0);
    if (!rig_up_chooser(&r, 1, 4, counting_chooser)) {
        CHECK(0 && "rig up");
        return;
    }

    /* a live facade consults the chooser ... */
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_SUCCESS);
    CHECK(chooser_calls() == 1);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* ... and a drained one must refuse before reaching it */
    moq_msquic_managed_drain(r.m);
    CHECK(rig_accept(&r, 1) == QUIC_STATUS_CONNECTION_REFUSED);
    CHECK(chooser_calls() == 1); /* NOT consulted: the first check refused */
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);
    check_untouched(&g_conn_fake[1]);

    rig_down(&r);
    if (failures == before)
        printf("PASS: drain_refused_before_the_chooser\n");
}

static void t_drain_during_the_accept_is_refused(void)
{
    int before = failures;
    struct rig r;

    gate_prepare(0);
    pump_reset();
    /* the chooser drains mid-accept and then names a perfectly valid lane */
    chooser_reset(true, 0);
    if (!rig_up_chooser(&r, 1, 4, counting_chooser)) {
        CHECK(0 && "rig up");
        return;
    }

    /* the facade is live on entry, so the first check passes and the
     * chooser really does run -- only the post-chooser check can refuse */
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_CONNECTION_REFUSED);
    CHECK(chooser_calls() == 1);
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);
    check_untouched(&g_conn_fake[0]);

    /* the refusal released its reserve rather than consuming a slot: with
     * a cap of 4 and nothing admitted, a further accept still refuses only
     * because of the drain, and the count stays put */
    CHECK(rig_accept(&r, 1) == QUIC_STATUS_CONNECTION_REFUSED);
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);
    check_untouched(&g_conn_fake[1]);

    rig_down(&r);
    if (failures == before)
        printf("PASS: drain_during_the_accept_is_refused\n");
}

/* --- 2. stop is idempotent --------------------------------------------- */

static void t_stop_is_idempotent(void)
{
    int before = failures;
    struct rig r;

    gate_prepare(0);
    pump_reset();
    if (!rig_up(&r, 1, 2)) {
        CHECK(0 && "rig up");
        return;
    }
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_SUCCESS);

    /* hand the child the transport terminal a real shutdown delivers, so
     * stop has something to complete rather than something to wait on */
    CHECK(fake_msq_conn_cb_installed(&g_conn_fake[0]));
    deliver_shutdown_complete(&r, 0);

    CHECK(moq_msquic_managed_stop(r.m) == MOQ_OK);
    /* the contract: a second stop is accepted, not refused */
    CHECK(moq_msquic_managed_stop(r.m) == MOQ_OK);
    CHECK(moq_msquic_managed_stop(r.m) == MOQ_OK);
    /* and a stopped facade refuses a wake, which is how a caller learns it */
    CHECK(moq_msquic_managed_wake(r.m) == MOQ_ERR_CLOSED);

    moq_msquic_managed_destroy(r.m);
    r.m = NULL;
    if (failures == before)
        printf("PASS: stop_is_idempotent\n");
}

/* --- 3. the facade aggregates really sum across lanes ------------------ */

/* The facade counters are per-lane mirrors refreshed at each sweep's end,
 * summed under m->mu. A child on lane 1 alone would already fail a
 * lane-0-only reader; two children with DIFFERENT counts additionally fail
 * a reader that returns one lane's value, or the largest. BOTH counters are
 * exercised, with different per-lane numbers, so a correct send aggregate
 * cannot cover for a broken datagram one -- and both must be exactly zero
 * once the work retires and the facade stops. */
static void t_pending_counters_sum_across_lanes(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_managed_conn_t *c0 = NULL, *c1 = NULL;
    enum { SENDS_L0 = 3, SENDS_L1 = 5, DGRAMS_L0 = 2, DGRAMS_L1 = 4 };

    gate_prepare((1u << 0) | (1u << 1));
    pump_reset();
    if (!rig_up(&r, 2, 4)) {
        CHECK(0 && "rig up");
        return;
    }
    CHECK(gate_await(0, 1));
    CHECK(gate_await(1, 1));

    /* one child per lane, each on its OWN fake connection so the sends it
     * makes are attributable to that lane */
    CHECK(moq_msq_test_lane_inject_live_child(
              r.lane[0], fake_msq_conn_handle(&g_conn_fake[0]), &c0) ==
          MOQ_OK);
    CHECK(moq_msq_test_lane_inject_live_child(
              r.lane[1], fake_msq_conn_handle(&g_conn_fake[1]), &c1) ==
          MOQ_OK);
    r.live[0] = r.live[1] = true;
    r.adapter[0] = moq_msq_test_managed_conn_adapter(c0);
    r.adapter[1] = moq_msq_test_managed_conn_adapter(c1);
    CHECK(c0 != NULL && c1 != NULL);
    if (c0 == NULL || c1 == NULL) {
        rig_down(&r);
        return;
    }

    /* datagrams are only accepted once the peer has advertised a size */
    enable_datagrams(&r, 0, 1200);
    enable_datagrams(&r, 1, 1200);

    /* let the injection generation settle */
    gate_release(0, 1);
    gate_release(1, 1);
    CHECK(gate_await(0, 2));
    CHECK(gate_await(1, 2));
    CHECK(moq_msquic_managed_pending_sends(r.m) == 0);
    CHECK(moq_msquic_managed_pending_datagrams(r.m) == 0);

    /* one generation per lane, each emitting its OWN declared send and
     * datagram counts; the mirrors are refreshed at that same sweep's end */
    pump_arm_flush(0, SENDS_L0, DGRAMS_L0);
    pump_arm_flush(1, SENDS_L1, DGRAMS_L1);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
    gate_release(0, 2);
    gate_release(1, 2);
    CHECK(gate_await(0, 3));
    CHECK(gate_await(1, 3));

    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.open_rc[0] == MOQ_TRANSPORT_OK);
    CHECK(g_pump.open_rc[1] == MOQ_TRANSPORT_OK);
    CHECK(g_pump.write_rc[0] == MOQ_TRANSPORT_OK);
    CHECK(g_pump.write_rc[1] == MOQ_TRANSPORT_OK);
    pthread_mutex_unlock(&g_pump.mu);

    /* each lane's own fake saw exactly its own work ... */
    CHECK(fake_msq_pending_sends(&g_conn_fake[0]) == SENDS_L0);
    CHECK(fake_msq_pending_sends(&g_conn_fake[1]) == SENDS_L1);
    CHECK(fake_msq_pending_dgrams(&g_conn_fake[0]) == DGRAMS_L0);
    CHECK(fake_msq_pending_dgrams(&g_conn_fake[1]) == DGRAMS_L1);
    /* ... and the facade reports the SUM of each, not one lane's share */
    CHECK(moq_msquic_managed_pending_sends(r.m) == SENDS_L0 + SENDS_L1);
    CHECK(moq_msquic_managed_pending_datagrams(r.m) ==
          DGRAMS_L0 + DGRAMS_L1);

    /* retiring the work drains both aggregates back to zero */
    while (fake_msq_deliver_send_complete(&g_conn_fake[0], false))
        ;
    while (fake_msq_deliver_send_complete(&g_conn_fake[1], false))
        ;
    for (int i = 0; i < DGRAMS_L0; i++)
        CHECK(complete_one_datagram(&r, 0));
    for (int i = 0; i < DGRAMS_L1; i++)
        CHECK(complete_one_datagram(&r, 1));
    CHECK(fake_msq_pending_dgrams(&g_conn_fake[0]) == 0);
    CHECK(fake_msq_pending_dgrams(&g_conn_fake[1]) == 0);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
    gate_release(0, 3);
    gate_release(1, 3);
    CHECK(gate_await(0, 4));
    CHECK(gate_await(1, 4));
    CHECK(moq_msquic_managed_pending_sends(r.m) == 0);
    CHECK(moq_msquic_managed_pending_datagrams(r.m) == 0);

    /*
     * Ordinary completion returning the aggregates to zero says nothing
     * about stop(): stop starts from whatever the last sweep left behind,
     * so proving its clearing needs the mirrors NONZERO on entry. Arm a
     * fresh batch on both lanes and leave it outstanding.
     */
    pump_arm_flush(0, SENDS_L0, DGRAMS_L0);
    pump_arm_flush(1, SENDS_L1, DGRAMS_L1);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
    gate_release(0, 4);
    gate_release(1, 4);
    CHECK(gate_await(0, 5));
    CHECK(gate_await(1, 5));
    CHECK(moq_msquic_managed_pending_sends(r.m) == SENDS_L0 + SENDS_L1);
    CHECK(moq_msquic_managed_pending_datagrams(r.m) ==
          DGRAMS_L0 + DGRAMS_L1);

    /* the transport terminal the injected children need, delivered while
     * both lanes are still parked so no pump can observe it early */
    deliver_shutdown_complete(&r, 0);
    deliver_shutdown_complete(&r, 1);
    r.live[0] = r.live[1] = false;

    gate_disable();
    CHECK(moq_msquic_managed_stop(r.m) == MOQ_OK);
    CHECK(moq_msquic_managed_pending_sends(r.m) == 0);
    CHECK(moq_msquic_managed_pending_datagrams(r.m) == 0);

    moq_msquic_managed_destroy(r.m);
    r.m = NULL;
    rig_down(&r);
    CHECK(r.m == NULL);
    if (failures == before)
        printf("PASS: pending_counters_sum_across_lanes\n");
}

/* --- 4. a live server facade reports MOQ_DONE, not MOQ_ERR_CLOSED ------ */

/* wait() must distinguish "nothing happened within the bound" from "this
 * facade is finished". A SERVER outlives any one connection, so with a live
 * child and no activity the answer is MOQ_DONE. The lanes are held at the
 * pre-wait barrier throughout, so no pump can run and nothing can publish
 * activity between draining it and polling; a zero timeout then makes the
 * poll itself instantaneous, leaving no window to race in. */
static void t_server_wait_reports_done_not_closed(void)
{
    int before = failures;
    struct rig r;
    moq_msquic_managed_conn_t *c0 = NULL;

    gate_prepare(1u << 0);
    pump_reset();
    if (!rig_up(&r, 1, 4)) {
        CHECK(0 && "rig up");
        return;
    }
    CHECK(gate_await(0, 1));
    CHECK(moq_msq_test_lane_inject_live_child(
              r.lane[0], fake_msq_conn_handle(&g_conn_fake[0]), &c0) ==
          MOQ_OK);
    r.live[0] = true;
    r.adapter[0] = moq_msq_test_managed_conn_adapter(c0);
    CHECK(c0 != NULL);
    gate_release(0, 1);
    CHECK(gate_await(0, 2));

    /* consume the activity the injection published; the lane is parked at
     * the barrier, so this is the last activity that can exist */
    moq_result_t rc;
    int drained = 0;
    while ((rc = moq_msquic_managed_wait(r.m, 0)) == MOQ_OK && drained < 64)
        drained++;

    /* the declared answer: bounded expiry, not a terminal claim */
    CHECK(rc == MOQ_DONE);
    CHECK(moq_msquic_managed_wait(r.m, 0) == MOQ_DONE);
    /* the facade is genuinely live: not terminal by either latch, still
     * holding its child, and wake() -- the public oracle for "neither stop
     * nor pump-exit is latched" -- still succeeds */
    CHECK(!moq_msquic_managed_is_closed(r.m));
    CHECK(!moq_msquic_managed_is_fatal(r.m));
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);
    CHECK(moq_msquic_managed_wake(r.m) == MOQ_OK);

    rig_down(&r);
    if (failures == before)
        printf("PASS: server_wait_reports_done_not_closed\n");
}

int main(void)
{
    /* line-buffered: a case that stalls still shows how far it got */
    setvbuf(stdout, NULL, _IOLBF, 0);
    moq_msq_test_prewait = prewait_hook;

    t_drain_refuses_and_releases_reserve();
    t_drain_refused_before_the_chooser();
    t_drain_during_the_accept_is_refused();
    t_stop_is_idempotent();
    t_pending_counters_sum_across_lanes();
    t_server_wait_reports_done_not_closed();

    if (failures == 0)
        printf("PASS: msquic_facade_control\n");
    else
        fprintf(stderr, "FAIL: msquic_facade_control (%d)\n", failures);
    return failures;
}
