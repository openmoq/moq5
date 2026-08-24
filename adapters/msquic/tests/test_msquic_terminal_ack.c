/*
 * Managed MsQuic terminal-child LIFETIME and acknowledgment classification.
 *
 * A server observes terminal by polling MOQ_EVENT_SESSION_CLOSED inside the
 * lane pump. A BOUNDED consumer — one that returns from a pump before it has
 * drained that event — needs the child, its session and its own per-connection
 * state to survive until it has actually consumed the terminal. It cannot
 * detach late either: session and connection POINTER VALUES are not stable
 * identities, so a child that stops appearing from lane_next_conn simply
 * vanishes from under whatever state the application keyed on it.
 * moq_msquic_managed_conn_ack_terminal is what closes that: the child is
 * reclaimed when the APPLICATION says so, not when a pump opportunity passed.
 *
 * NETWORK-FREE. The facade borrows a fake QUIC_API_TABLE and opens no
 * listener, socket, credential or certificate. Terminal children are built by
 * the production child path with a real closed session, a real SESSION_CLOSED
 * and the production reapability latch; admission is driven through the exact
 * production listener callback. Injected monotonic time, a doorbell pre-wait
 * gate and explicit pump generations determine every ordering. Timed condition
 * waits are fail-closed deadlock guards around named predicates; elapsed time
 * is never an acceptance condition.
 *
 * Real transport terminal ARRIVAL and a real peer's refusal remain owned by
 * the physical boundary (test_msquic_loopback's t_cap_refusal and the adapter
 * callback rail); this file owns the lifetime and classification contracts.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "msquic_internal.h"
#include "support/fake_msq_table.h"

#include <moq/msquic_managed.h>
#include <moq/session.h>

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

/* MOQ_MSQUIC_TESTING-only seams. */
extern uint64_t (*moq_msq_test_now_us)(void);
extern void (*moq_msq_test_prewait)(moq_msquic_managed_lane_t *lane);
extern void (*moq_msq_test_reap_gap)(moq_msquic_managed_lane_t *lane);
extern size_t moq_msq_test_lane_reapable(moq_msquic_managed_lane_t *lane);
extern bool moq_msq_test_lane_inject_terminal_child(
    moq_msquic_managed_lane_t *lane, uint64_t close_code);
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out);
extern QUIC_STATUS moq_msq_test_listener_accept(moq_msquic_managed_t *m,
                                                HQUIC connection);

static int failures;

#define CHECK(expr)                                                         \
    do {                                                                    \
        if (!(expr)) {                                                      \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__,    \
                    #expr);                                                 \
            failures++;                                                     \
        }                                                                   \
    } while (0)

#define GUARD_US (3u * 1000u * 1000u)
#define CLOSE_CODE 0x11u
#define BIND_MAGIC 0x5ea15eeduL
/* No moq_result_t is this, so an untouched slot is distinguishable from every
 * real answer. */
#define RC_UNSET 0x7fffffff

enum { MAX_TEST_CONNS = 2 };

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

/* --- injected clock ------------------------------------------------------ */

static _Atomic uint64_t g_now_us;

static uint64_t test_now_us(void)
{
    return atomic_load(&g_now_us);
}

static void set_now(uint64_t now_us)
{
    atomic_store(&g_now_us, now_us);
}

/* --- the application's own per-connection state --------------------------- */

/* Published through the supported `user` slot. It is the identity bridge
 * across pumps — deliberately not a retained session pointer. */
struct app_bind {
    unsigned long magic;
    int attached_on_pump;
};

/* --- doorbell pre-wait gate ---------------------------------------------- */

/* The single lane stops after computing its exact wait cause but before
 * sleeping, so every pump generation and every reap decision is released one
 * at a time. The timeout only prevents a broken mutant from hanging. */
static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool enabled;
    uint64_t entered;
    uint64_t released;
    bool expired;
} g_gate = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void gate_prepare(bool enabled)
{
    pthread_mutex_lock(&g_gate.mu);
    g_gate.enabled = enabled;
    g_gate.entered = 0;
    g_gate.released = 0;
    g_gate.expired = false;
    pthread_mutex_unlock(&g_gate.mu);
}

static bool gate_await(uint64_t want)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_gate.mu);
    while (g_gate.entered < want && !g_gate.expired)
        if (pthread_cond_timedwait(&g_gate.cv, &g_gate.mu, &abs) != 0)
            break;
    reached = g_gate.entered >= want && !g_gate.expired;
    pthread_mutex_unlock(&g_gate.mu);
    return reached;
}

static void gate_release(uint64_t entry)
{
    pthread_mutex_lock(&g_gate.mu);
    if (g_gate.released < entry)
        g_gate.released = entry;
    pthread_cond_broadcast(&g_gate.cv);
    pthread_mutex_unlock(&g_gate.mu);
}

static void gate_disable(void)
{
    pthread_mutex_lock(&g_gate.mu);
    g_gate.enabled = false;
    pthread_cond_broadcast(&g_gate.cv);
    pthread_mutex_unlock(&g_gate.mu);
}

/* --- observed doorbell facts --------------------------------------------- */

static struct {
    moq_msquic_managed_lane_t *_Atomic watch;
    atomic_int reaps;              /* reclamations at the reap seam        */
    atomic_int violation;          /* reclaimed with the terminal unconsumed */
    atomic_int idle_after_terminal;/* pre-waits reached after the terminal */
    atomic_int quiesced_retained;  /* a pre-wait saw a reapable child kept */
    atomic_int idle1_pumps;        /* pump count at the FIRST such pre-wait */
} g_dor;

static void dor_reset(moq_msquic_managed_lane_t *lane)
{
    atomic_store(&g_dor.watch, lane);
    atomic_store(&g_dor.reaps, 0);
    atomic_store(&g_dor.violation, 0);
    atomic_store(&g_dor.idle_after_terminal, 0);
    atomic_store(&g_dor.quiesced_retained, 0);
    atomic_store(&g_dor.idle1_pumps, -1);
}

/* --- lane pump ----------------------------------------------------------- */

enum pump_mode {
    PUMP_IDLE,
    PUMP_DECLINE,       /* see the terminal child, consume nothing, return  */
    PUMP_HANDBACK,      /* require the same identity back, poll, do not ack */
    PUMP_RELEASE,       /* acknowledge, then acknowledge again              */
    PUMP_ACK_UNOBSERVED,/* acknowledge without ever polling the terminal    */
    PUMP_GREEDY,        /* attach, poll and acknowledge in ONE pump         */
    PUMP_CLIENT,        /* client side: poll its own terminal, then ack     */
    PUMP_SURVEY,        /* count the children this lane can iterate         */
};

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    enum pump_mode mode;
    uint64_t calls;

    struct app_bind bind;

    /* identity, captured on first sight and required back afterwards */
    moq_msquic_managed_conn_t *child;
    moq_session_t *session;
    void *user_ptr;
    bool identity_mismatch;
    bool session_mismatch;
    bool user_mismatch;
    bool user_value_bad;
    bool bind_attached;

    /* generations */
    uint64_t decline_pump;
    uint64_t handback_pump;
    uint64_t declines;

    /* terminal observation */
    bool state_closed_seen;
    uint64_t closed_polled;
    uint64_t other_events;
    uint32_t other_kind;

    /* survey */
    size_t seen;

    /* acknowledgment results, as they occur */
    moq_result_t ack_unobserved;
    moq_result_t ack_ok;
    moq_result_t ack_dup;
    moq_result_t ack_client;
} g_pump = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void pump_reset(void)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.mode = PUMP_IDLE;
    g_pump.calls = 0;
    memset(&g_pump.bind, 0, sizeof(g_pump.bind));
    g_pump.child = NULL;
    g_pump.session = NULL;
    g_pump.user_ptr = NULL;
    g_pump.identity_mismatch = false;
    g_pump.session_mismatch = false;
    g_pump.user_mismatch = false;
    g_pump.user_value_bad = false;
    g_pump.bind_attached = false;
    g_pump.decline_pump = 0;
    g_pump.handback_pump = 0;
    g_pump.declines = 0;
    g_pump.state_closed_seen = false;
    g_pump.closed_polled = 0;
    g_pump.other_events = 0;
    g_pump.other_kind = 0;
    g_pump.seen = 0;
    g_pump.ack_unobserved = RC_UNSET;
    g_pump.ack_ok = RC_UNSET;
    g_pump.ack_dup = RC_UNSET;
    g_pump.ack_client = RC_UNSET;
    pthread_mutex_unlock(&g_pump.mu);
}

static void pump_arm(enum pump_mode mode)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.mode = mode;
    pthread_mutex_unlock(&g_pump.mu);
}

/* Capture the child on first sight; on every later sight require the SAME
 * child, the SAME session and the SAME application state back. */
static void pump_bind_identity(moq_msquic_managed_conn_t *c, moq_session_t *s,
                               uint64_t idx)
{
    if (g_pump.child == NULL) {
        g_pump.child = c;
        g_pump.session = s;
        g_pump.bind.magic = BIND_MAGIC;
        g_pump.bind.attached_on_pump = (int)idx;
        moq_msquic_managed_conn_set_user(c, &g_pump.bind);
        g_pump.user_ptr = &g_pump.bind;
        g_pump.bind_attached = true;
        return;
    }
    if (c != g_pump.child)
        g_pump.identity_mismatch = true;
    if (s != g_pump.session)
        g_pump.session_mismatch = true;

    const struct app_bind *b = moq_msquic_managed_conn_user(c);

    if ((const void *)b != g_pump.user_ptr)
        g_pump.user_mismatch = true;
    else if (b == NULL || b->magic != BIND_MAGIC ||
             b->attached_on_pump != (int)g_pump.decline_pump)
        g_pump.user_value_bad = true;
}

/* Every event is classified: this fixture owes exactly one SESSION_CLOSED and
 * nothing else, and an unexpected kind is recorded rather than dropped. */
static void pump_drain(moq_session_t *s)
{
    moq_event_t ev;

    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            g_pump.closed_polled++;
        } else {
            g_pump.other_events++;
            if (g_pump.other_kind == 0)
                g_pump.other_kind = ev.kind;
        }
        moq_event_cleanup(&ev);
    }
}

/* Every draining row owes exactly one SESSION_CLOSED and nothing else. One
 * checker so a future row cannot forget half the event oracle. Call with
 * g_pump.mu held. */
static void check_drained_exactly_one(void)
{
    CHECK(g_pump.closed_polled == 1);
    CHECK(g_pump.other_events == 0);
    if (g_pump.other_events != 0)
        fprintf(stderr, "  unexpected event kind=%u\n", g_pump.other_kind);
}

static int test_pump(moq_msquic_managed_t *m,
                     moq_msquic_managed_lane_t *lane, uint64_t now_us,
                     void *user)
{
    moq_msquic_managed_conn_t *c = NULL;
    enum pump_mode mode;
    uint64_t idx;

    (void)m;
    (void)now_us;
    (void)user;
    pthread_mutex_lock(&g_pump.mu);
    mode = g_pump.mode;
    idx = ++g_pump.calls;
    g_pump.seen = 0;

    while ((c = moq_msquic_lane_next_conn(lane, c)) != NULL) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);

        g_pump.seen++;
        if (s == NULL)
            continue;
        if (moq_session_state(s) == MOQ_SESS_CLOSED)
            g_pump.state_closed_seen = true;

        switch (mode) {
        case PUMP_DECLINE:
            /* Attach the application's state, see the closed session, and
             * return having consumed NOTHING and acknowledged nothing. */
            pump_bind_identity(c, s, idx);
            if (g_pump.decline_pump == 0)
                g_pump.decline_pump = idx;
            g_pump.declines++;
            break;
        case PUMP_HANDBACK:
            pump_bind_identity(c, s, idx);
            if (g_pump.handback_pump == 0)
                g_pump.handback_pump = idx;
            pump_drain(s);
            break;
        case PUMP_RELEASE:
            pump_bind_identity(c, s, idx);
            if (g_pump.ack_ok == RC_UNSET) {
                g_pump.ack_ok = moq_msquic_managed_conn_ack_terminal(c);
                g_pump.ack_dup = moq_msquic_managed_conn_ack_terminal(c);
            }
            break;
        case PUMP_ACK_UNOBSERVED:
            /* deliberately no poll: the terminal has not been observed */
            if (g_pump.ack_unobserved == RC_UNSET)
                g_pump.ack_unobserved =
                    moq_msquic_managed_conn_ack_terminal(c);
            break;
        case PUMP_GREEDY:
            pump_bind_identity(c, s, idx);
            if (g_pump.decline_pump == 0)
                g_pump.decline_pump = idx;
            pump_drain(s);
            if (g_pump.closed_polled > 0 && g_pump.ack_ok == RC_UNSET) {
                g_pump.ack_ok = moq_msquic_managed_conn_ack_terminal(c);
                g_pump.ack_dup = moq_msquic_managed_conn_ack_terminal(c);
            }
            break;
        case PUMP_CLIENT:
            /* observe this side's own terminal first, so a refusal is
             * attributable to the client rule and not to the unobserved one */
            pump_drain(s);
            if (g_pump.closed_polled > 0 && g_pump.ack_client == RC_UNSET)
                g_pump.ack_client = moq_msquic_managed_conn_ack_terminal(c);
            break;
        case PUMP_SURVEY:
        case PUMP_IDLE:
            break;
        }
    }
    pthread_cond_broadcast(&g_pump.cv);
    pthread_mutex_unlock(&g_pump.mu);
    return 0;
}

/* --- doorbell hooks ------------------------------------------------------- */

/* Reclamation instant: doorbell_reap has just freed a victim. If the terminal
 * was seen in a pump and SESSION_CLOSED has not been consumed, the session was
 * destroyed out from under the application. */
static void reap_gap_hook(moq_msquic_managed_lane_t *lane)
{
    bool unconsumed;

    if (lane != atomic_load(&g_dor.watch))
        return;
    atomic_fetch_add(&g_dor.reaps, 1);
    pthread_mutex_lock(&g_pump.mu);
    unconsumed = g_pump.state_closed_seen && g_pump.closed_polled == 0;
    pthread_mutex_unlock(&g_pump.mu);
    if (unconsumed)
        atomic_store(&g_dor.violation, 1);
}

/* The doorbell is about to sleep: it found no lane work and has already run a
 * reap pass. A fully quiesced child still present here means that pass
 * DECLINED to reclaim it. Runs with lane->mu released, so the reapable probe
 * (which takes it) is safe, and it is taken BEFORE the gate blocks. */
static void prewait_hook(moq_msquic_managed_lane_t *lane)
{
    struct timespec abs;
    bool seen;

    if (lane == atomic_load(&g_dor.watch)) {
        pthread_mutex_lock(&g_pump.mu);
        seen = g_pump.state_closed_seen;
        uint64_t pumps = g_pump.calls;

        pthread_mutex_unlock(&g_pump.mu);
        if (seen) {
            atomic_fetch_add(&g_dor.idle_after_terminal, 1);
            if (moq_msq_test_lane_reapable(lane) >= 1) {
                atomic_store(&g_dor.quiesced_retained, 1);
                int unset = -1;

                (void)atomic_compare_exchange_strong(&g_dor.idle1_pumps,
                                                     &unset, (int)pumps);
            }
        }
    }

    guard_deadline(&abs);
    pthread_mutex_lock(&g_gate.mu);
    if (!g_gate.enabled) {
        pthread_mutex_unlock(&g_gate.mu);
        return;
    }
    uint64_t mine = ++g_gate.entered;

    pthread_cond_broadcast(&g_gate.cv);
    while (g_gate.enabled && g_gate.released < mine)
        if (pthread_cond_timedwait(&g_gate.cv, &g_gate.mu, &abs) != 0) {
            g_gate.expired = true;
            g_gate.enabled = false;
            pthread_cond_broadcast(&g_gate.cv);
            break;
        }
    pthread_mutex_unlock(&g_gate.mu);
}

/* --- rig ------------------------------------------------------------------ */

/* One fake owns the borrowed API table; one fake per synthesized accept
 * supplies a distinct connection handle. File-scope because a fake_msq_t
 * carries a whole per-endpoint stream/send model. */
static fake_msq_t g_table_fake;
static fake_msq_t g_conn_fake[MAX_TEST_CONNS];

struct rig {
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane;
    bool live[MAX_TEST_CONNS];
};

static bool rig_up(struct rig *r, moq_perspective_t perspective,
                   uint32_t max_conns)
{
    moq_msquic_managed_cfg_t cfg;

    memset(r, 0, sizeof(*r));
    fake_msq_init(&g_table_fake, false);
    for (int i = 0; i < MAX_TEST_CONNS; i++)
        fake_msq_init(&g_conn_fake[i], false);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = perspective;
    cfg.max_connections = max_conns;
    cfg.lane_count = 1;
    cfg.max_events = 64;
    cfg.on_lane_pump = test_pump;
    if (moq_msq_test_managed_create_lanes_only_api(
            &cfg, fake_msq_table(&g_table_fake), &r->m) != MOQ_OK)
        return false;
    if (moq_msquic_managed_lane_count(r->m) != 1) {
        CHECK(false);
        return false;
    }
    r->lane = moq_msquic_managed_lane(r->m, 0);
    if (r->lane == NULL || moq_msquic_lane_index(r->lane) != 0) {
        CHECK(false);
        return false;
    }
    dor_reset(r->lane);
    return true;
}

static QUIC_STATUS rig_accept(struct rig *r, int slot)
{
    QUIC_STATUS st = moq_msq_test_listener_accept(
        r->m, fake_msq_conn_handle(&g_conn_fake[slot]));

    if (QUIC_SUCCEEDED(st))
        r->live[slot] = true;
    return st;
}

/* The complete per-identity operation inventory the shared fake records for a
 * connection handle. Anything the listener path did NOT do must be zero, so a
 * refusal cannot leave an implicit allowance behind. */
static void check_conn_ops(fake_msq_t *f, bool cb_installed, int configs)
{
    CHECK(fake_msq_conn_cb_installed(f) == cb_installed);
    CHECK(atomic_load(&f->conn_set_configs) == configs);
    CHECK(atomic_load(&f->conn_shutdowns) == 0);
    CHECK(atomic_load(&f->conn_closes) == 0);
    CHECK(f->last_conn_shutdown_code == 0);
    CHECK(f->stream_count == 0);
    CHECK(f->send_count == 0);
    CHECK(f->dgram_count == 0);
    CHECK(f->bidi_count == 0);
    CHECK(f->uni_count == 0);
}

static QUIC_STATUS deliver_registered(fake_msq_t *f,
                                      QUIC_CONNECTION_EVENT_TYPE type)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = type;
    return fake_msq_deliver_conn_event(f, &ev);
}

static void rig_down(struct rig *r)
{
    gate_disable();
    /* an accepted child holds a live transport handle: stop() waits for its
     * SHUTDOWN_COMPLETE, so deliver it first. Injected terminal children bind
     * no handle and are already quiesced. */
    for (int i = 0; i < MAX_TEST_CONNS; i++) {
        if (!r->live[i] || !fake_msq_conn_cb_installed(&g_conn_fake[i]))
            continue;
        (void)deliver_registered(&g_conn_fake[i],
                                 QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
        r->live[i] = false;
    }
    atomic_store(&g_dor.watch, NULL);
    if (r->m != NULL) {
        CHECK(moq_msquic_managed_stop(r->m) == MOQ_OK);
        moq_msquic_managed_destroy(r->m);
        r->m = NULL;
    }
}

/* Release one doorbell generation: exactly one pump runs, then every reap
 * opportunity the doorbell reaches on that pass completes -- the post-pump site
 * and the no-work site both run before it parks at its next pre-wait. What a
 * released generation therefore fixes is that one pump ran and the whole
 * production reap path finished before the next observation, not that a single
 * reap call occurred. */
static bool rig_step(uint64_t *entry)
{
    gate_release(*entry);
    (*entry)++;
    return gate_await(*entry);
}

/* --- 1. delayed application handback -------------------------------------- */

static void t_delayed_handback(void)
{
    int before = failures;
    struct rig r;
    uint64_t entry = 1;

    set_now(1000000);
    gate_prepare(true);
    pump_reset();
    if (!rig_up(&r, MOQ_PERSPECTIVE_SERVER, 1)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(entry));

    CHECK(moq_msq_test_lane_inject_terminal_child(r.lane, CLOSE_CODE));
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* pump 1: the bounded consumer declines the terminal */
    pump_arm(PUMP_DECLINE);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.calls == 1);
    CHECK(g_pump.declines == 1);
    CHECK(g_pump.decline_pump == 1);
    CHECK(g_pump.bind_attached);
    CHECK(g_pump.state_closed_seen);
    CHECK(g_pump.closed_polled == 0);
    CHECK(g_pump.child != NULL);
    moq_msquic_managed_conn_t *child = g_pump.child;

    pthread_mutex_unlock(&g_pump.mu);

    /* the decline provably precedes the reap decision AND the later pump:
     * the doorbell's first idle observation after the terminal happened with
     * exactly one pump run, and it found the child reapable yet retained */
    CHECK(atomic_load(&g_dor.quiesced_retained) == 1);
    CHECK(atomic_load(&g_dor.idle1_pumps) == 1);
    CHECK(atomic_load(&g_dor.reaps) == 0);
    CHECK(atomic_load(&g_dor.violation) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* pump 2: the contract owes the same child, session and user state back */
    pump_arm(PUMP_HANDBACK);
    CHECK(moq_msquic_lane_wake(r.lane) == MOQ_OK);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.calls == 2);
    CHECK(g_pump.handback_pump == 2);
    CHECK(g_pump.decline_pump < g_pump.handback_pump);
    CHECK(g_pump.child == child);
    CHECK(!g_pump.identity_mismatch);
    CHECK(!g_pump.session_mismatch);
    CHECK(!g_pump.user_mismatch);
    CHECK(!g_pump.user_value_bad);
    check_drained_exactly_one();
    pthread_mutex_unlock(&g_pump.mu);
    /* consumed but not acknowledged: still retained */
    CHECK(atomic_load(&g_dor.reaps) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* pump 3: acknowledge, and only now may it be reclaimed */
    pump_arm(PUMP_RELEASE);
    CHECK(moq_msquic_lane_wake(r.lane) == MOQ_OK);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.ack_ok == MOQ_OK);
    CHECK(g_pump.ack_dup == MOQ_OK);
    CHECK(g_pump.closed_polled == 1);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(atomic_load(&g_dor.reaps) == 1);
    CHECK(atomic_load(&g_dor.violation) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);

    rig_down(&r);
    if (failures == before)
        printf("PASS: declined terminal is handed back with its own state\n");
}

/* --- 2. admission reserve while unacknowledged ---------------------------- */

static void t_admission_reserve_while_unacked(void)
{
    int before = failures;
    struct rig r;
    uint64_t entry = 1;

    set_now(1100000);
    gate_prepare(true);
    pump_reset();
    if (!rig_up(&r, MOQ_PERSPECTIVE_SERVER, 1)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(entry));

    CHECK(moq_msq_test_lane_inject_terminal_child(r.lane, CLOSE_CODE));
    pump_arm(PUMP_DECLINE);
    CHECK(rig_step(&entry));

    /* transport-quiesced, a reap decision completed, and still present */
    CHECK(atomic_load(&g_dor.quiesced_retained) == 1);
    CHECK(atomic_load(&g_dor.reaps) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* the exact production admission path, while still unacknowledged */
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_CONNECTION_REFUSED);
    /* the refused identity saw NO operation of any kind */
    check_conn_ops(&g_conn_fake[0], false, 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* and no second child was published to the lane */
    pump_arm(PUMP_SURVEY);
    CHECK(moq_msquic_lane_wake(r.lane) == MOQ_OK);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.seen == 1);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(atomic_load(&g_dor.reaps) == 0);

    /* poll and acknowledge inside the owning pump */
    pump_arm(PUMP_HANDBACK);
    CHECK(moq_msquic_lane_wake(r.lane) == MOQ_OK);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    check_drained_exactly_one();
    pthread_mutex_unlock(&g_pump.mu);
    pump_arm(PUMP_RELEASE);
    CHECK(moq_msquic_lane_wake(r.lane) == MOQ_OK);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.ack_ok == MOQ_OK);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(atomic_load(&g_dor.reaps) == 1);
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);

    /* the reserve really came back: the same admission path now succeeds */
    CHECK(rig_accept(&r, 1) == QUIC_STATUS_SUCCESS);
    /* the admitted identity saw EXACTLY the admission operations, before any
     * cleanup: a handler and one configuration, and nothing else */
    check_conn_ops(&g_conn_fake[1], true, 1);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    rig_down(&r);
    if (failures == before)
        printf("PASS: unacknowledged child holds the admission reserve\n");
}

/* --- 3. exact acknowledgment classification ------------------------------- */

static void t_ack_classification(void)
{
    int before = failures;
    struct rig r;
    uint64_t entry = 1;

    /* NULL, needing no facade at all */
    CHECK(moq_msquic_managed_conn_ack_terminal(NULL) == MOQ_ERR_INVAL);

    set_now(1200000);
    gate_prepare(true);
    pump_reset();
    if (!rig_up(&r, MOQ_PERSPECTIVE_SERVER, 1)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(entry));
    CHECK(moq_msq_test_lane_inject_terminal_child(r.lane, CLOSE_CODE));

    /* row: server child, inside its owning pump, terminal NOT yet polled */
    pump_arm(PUMP_ACK_UNOBSERVED);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.seen == 1);              /* it was inside the pump window */
    CHECK(g_pump.state_closed_seen);      /* a terminal really exists      */
    CHECK(g_pump.closed_polled == 0);     /* and was not observed          */
    CHECK(g_pump.ack_unobserved == MOQ_ERR_WRONG_STATE);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(atomic_load(&g_dor.reaps) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* observe the terminal, and capture the live handle for the row below */
    pump_arm(PUMP_HANDBACK);
    CHECK(moq_msquic_lane_wake(r.lane) == MOQ_OK);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    check_drained_exactly_one();
    moq_msquic_managed_conn_t *held = g_pump.child;

    pthread_mutex_unlock(&g_pump.mu);
    CHECK(held != NULL);

    /* row: OUTSIDE any lane pump, after observation, on a live retained
     * handle. It is unreclaimable (unacknowledged, facade running) so the only
     * rule left to refuse it is the owning-pump window; this thread is outside
     * every pump by construction. The handle is never dereferenced here. */
    int reaps_before = atomic_load(&g_dor.reaps);

    CHECK(reaps_before == 0);
    moq_result_t ack_outside = held != NULL
        ? moq_msquic_managed_conn_ack_terminal(held) : RC_UNSET;

    CHECK(ack_outside == MOQ_ERR_WRONG_STATE);
    /* refused INERTLY: a further doorbell generation releases nothing */
    pump_arm(PUMP_IDLE);
    CHECK(moq_msquic_lane_wake(r.lane) == MOQ_OK);
    CHECK(rig_step(&entry));
    CHECK(atomic_load(&g_dor.reaps) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);
    CHECK(atomic_load(&g_dor.quiesced_retained) == 1);

    /* rows: accepted inside the owning pump, and the harmless duplicate */
    pump_arm(PUMP_RELEASE);
    CHECK(moq_msquic_lane_wake(r.lane) == MOQ_OK);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.ack_ok == MOQ_OK);
    CHECK(g_pump.ack_dup == MOQ_OK);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(atomic_load(&g_dor.reaps) == 1);
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);
    rig_down(&r);

    /* row: a CLIENT child, after this side observed its own terminal */
    struct rig rc;
    uint64_t centry = 1;

    set_now(1300000);
    gate_prepare(true);
    pump_reset();
    if (!rig_up(&rc, MOQ_PERSPECTIVE_CLIENT, 1)) {
        rig_down(&rc);
        return;
    }
    CHECK(gate_await(centry));
    CHECK(moq_msq_test_lane_inject_terminal_child(rc.lane, CLOSE_CODE));
    pump_arm(PUMP_CLIENT);
    CHECK(rig_step(&centry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.seen == 1);
    CHECK(g_pump.state_closed_seen);
    /* observed, so the refusal is the client rule, not the unobserved one */
    check_drained_exactly_one();
    CHECK(g_pump.ack_client == MOQ_ERR_WRONG_STATE);
    pthread_mutex_unlock(&g_pump.mu);
    /* the client is exempt from acknowledgment and is reclaimed anyway */
    CHECK(atomic_load(&g_dor.reaps) == 1);
    CHECK(moq_msquic_managed_conn_count(rc.m) == 0);
    rig_down(&rc);

    if (failures == before)
        printf("PASS: ack_terminal classifies every guard exactly\n");
}

/* --- control: the same-pump consume and release --------------------------- */

/* The independent discriminator this control adds: the final event really is
 * delivered on a pump where the child is visible, so the bounded case's
 * "unconsumed" is the application's deliberate choice, not a missing event. */
static void t_greedy_same_pump(void)
{
    int before = failures;
    struct rig r;
    uint64_t entry = 1;

    set_now(1400000);
    gate_prepare(true);
    pump_reset();
    if (!rig_up(&r, MOQ_PERSPECTIVE_SERVER, 1)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(entry));
    CHECK(moq_msq_test_lane_inject_terminal_child(r.lane, CLOSE_CODE));

    pump_arm(PUMP_GREEDY);
    CHECK(rig_step(&entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.calls == 1);
    CHECK(g_pump.seen == 1);
    CHECK(g_pump.bind_attached);
    CHECK(g_pump.closed_polled == 1);
    CHECK(g_pump.other_events == 0);
    CHECK(g_pump.ack_ok == MOQ_OK);
    CHECK(g_pump.ack_dup == MOQ_OK);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(atomic_load(&g_dor.reaps) == 1);
    CHECK(atomic_load(&g_dor.violation) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);

    rig_down(&r);
    if (failures == before)
        printf("PASS: same-pump consume and release reclaims exactly once\n");
}

int main(void)
{
    moq_msq_test_now_us = test_now_us;
    moq_msq_test_prewait = prewait_hook;
    moq_msq_test_reap_gap = reap_gap_hook;

    t_greedy_same_pump();
    t_delayed_handback();
    t_admission_reserve_while_unacked();
    t_ack_classification();

    moq_msq_test_reap_gap = NULL;
    moq_msq_test_prewait = NULL;
    moq_msq_test_now_us = NULL;
    if (failures != 0) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("all msquic terminal-ack tests passed\n");
    return 0;
}
