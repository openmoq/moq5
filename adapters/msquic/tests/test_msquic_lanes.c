/*
 * Deterministic managed-lane placement, partition, wake, deadline and
 * callback-confinement proofs.
 *
 * NETWORK-FREE. The facade borrows a fake QUIC_API_TABLE and opens no
 * listener, socket, credential or certificate. Accepted connections are
 * synthesized through the EXACT production listener callback, so the
 * reserve, chooser, range check, stop re-check, child construction, lane
 * append, handler registration and bind all run unchanged. Connection
 * events arrive through the production connection callback that listener
 * registered. Injected monotonic time drives deadline and idle-cause
 * classification. Timed condition waits are fail-closed guards around named
 * predicates; elapsed time is never an acceptance condition.
 */

#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "msquic_internal.h"
#include "support/fake_msq_table.h"
#include "support/fake_endpoint.h" /* raw peer for the deadline pair */

#include <moq/msquic_managed.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>

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
extern void moq_msq_test_bell_ring_raw(moq_msquic_managed_lane_t *lane);
extern moq_result_t moq_msq_test_managed_create_lanes_only_api(
    const moq_msquic_managed_cfg_t *cfg, const QUIC_API_TABLE *api,
    moq_msquic_managed_t **out);
extern QUIC_STATUS moq_msq_test_listener_accept(moq_msquic_managed_t *m,
                                                HQUIC connection);
extern void (*moq_msq_test_guard_pre_lock)(moq_msquic_managed_lane_t *lane);
extern void (*moq_msq_test_stats_pre_lock)(moq_msquic_managed_lane_t *lane);

static int failures;

#define CHECK(expr)                                                         \
    do {                                                                    \
        if (!(expr)) {                                                      \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__,    \
                    #expr);                                                 \
            failures++;                                                     \
        }                                                                   \
    } while (0)

enum { MAX_TEST_LANES = 2, MAX_TEST_CONNS = 4 };
#define GUARD_US (3u * 1000u * 1000u)

/* The user slot's round-trip witness: a value derived from the connection
 * itself, so a pass can verify per-connection state without a side map. */
#define USER_TAG(c) ((void *)((uintptr_t)(c) ^ (uintptr_t)0x5eed5eedu))

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

static moq_result_t stats_of(moq_msquic_managed_lane_t *lane,
                             moq_msquic_lane_stats_t *st)
{
    memset(st, 0, sizeof(*st));
    return moq_msquic_lane_get_stats(lane, st, sizeof(*st));
}

/* --- doorbell pre-wait rendezvous ---------------------------------------- */

/* Each enabled lane stops after computing its exact wait cause but before
 * sleeping. Main releases numbered entries one at a time; the timeout only
 * prevents a broken mutant from hanging the suite. */
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

/* --- lane pump ----------------------------------------------------------- */

enum pump_mode {
    PUMP_IDLE,
    PUMP_SURVEY,    /* one-pass iteration, lane scoping, user slot        */
    PUMP_TERMINAL,  /* drain events, optionally acknowledge the terminal  */
    PUMP_DEADLINE,  /* read the child's next session deadline in-pump     */
    PUMP_CLOSE,     /* the legitimate in-pump connection close            */
    PUMP_HOLD_A,    /* park lane 0 for the guard-serialization schedule   */
    PUMP_XWAKE,     /* lane 1 parks; lane 0 issues the cross-lane wake    */
    PUMP_XISSUE,    /* lane 1 issues one cross-lane wake toward lane 0    */
    PUMP_SESSION,   /* accept the peer's subscribe, then open a subgroup  */
};

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    enum pump_mode mode;
    uint64_t calls[MAX_TEST_LANES];

    /* concurrent-entry detector, per lane */
    int depth[MAX_TEST_LANES];
    int max_depth[MAX_TEST_LANES];

    /* survey */
    size_t seen[MAX_TEST_LANES];   /* connections iterated in the last pass */
    bool wrong_lane;
    bool bad_index;
    bool user_mismatch;
    size_t user_checked;
    bool tag_users;                /* pass 1 tags, pass 2 verifies          */
    moq_msquic_managed_conn_t *captured; /* first conn a survey iterated    */

    /* terminal */
    bool ack;
    uint64_t closed_seen;
    uint64_t other_events;
    uint64_t acks;
    moq_result_t last_ack_rc;

    /* deadline */
    uint64_t dl;
    uint64_t dl_now;
    bool dl_read;
    uint64_t sg_opened_at;

    /* subscription script driving a real subgroup delivery deadline */
    moq_subscription_t sub;
    bool have_sub;
    bool updated;
    bool sg_open;
    uint64_t ev_setup;
    uint64_t ev_sub_request;
    uint64_t ev_sub_updated;
    uint64_t ev_unexpected;
    uint32_t ev_unexpected_kind;
    bool sub_handle_mismatch;
    bool accept_failed;
    moq_session_state_t child_state;

    /* close */
    moq_msquic_managed_conn_t *close_target;
    uint64_t closes_issued;
} g_pump = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void pump_reset(void)
{
    pthread_mutex_lock(&g_pump.mu);
    memset(&g_pump.calls, 0, sizeof(g_pump.calls));
    memset(&g_pump.depth, 0, sizeof(g_pump.depth));
    memset(&g_pump.max_depth, 0, sizeof(g_pump.max_depth));
    memset(&g_pump.seen, 0, sizeof(g_pump.seen));
    g_pump.mode = PUMP_IDLE;
    g_pump.wrong_lane = false;
    g_pump.bad_index = false;
    g_pump.user_mismatch = false;
    g_pump.user_checked = 0;
    g_pump.tag_users = true;
    g_pump.captured = NULL;
    g_pump.ack = false;
    g_pump.closed_seen = 0;
    g_pump.other_events = 0;
    g_pump.acks = 0;
    g_pump.last_ack_rc = MOQ_ERR_INTERNAL;
    g_pump.dl = 0;
    g_pump.dl_now = 0;
    g_pump.dl_read = false;
    g_pump.sg_opened_at = 0;
    g_pump.sub = MOQ_SUBSCRIPTION_INVALID;
    g_pump.have_sub = false;
    g_pump.updated = false;
    g_pump.sg_open = false;
    g_pump.ev_setup = 0;
    g_pump.ev_sub_request = 0;
    g_pump.ev_sub_updated = 0;
    g_pump.ev_unexpected = 0;
    g_pump.ev_unexpected_kind = 0;
    g_pump.sub_handle_mismatch = false;
    g_pump.accept_failed = false;
    g_pump.child_state = MOQ_SESS_IDLE;
    g_pump.close_target = NULL;
    g_pump.closes_issued = 0;
    pthread_mutex_unlock(&g_pump.mu);
}

static void pump_arm(enum pump_mode mode)
{
    pthread_mutex_lock(&g_pump.mu);
    g_pump.mode = mode;
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

static uint64_t pump_calls(uint32_t lane)
{
    uint64_t v;

    pthread_mutex_lock(&g_pump.mu);
    v = lane < MAX_TEST_LANES ? g_pump.calls[lane] : 0;
    pthread_mutex_unlock(&g_pump.mu);
    return v;
}

/* --- the guard-serialization schedule ------------------------------------ */

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool a_parked;
    bool release_a;
    bool a_expired;
    bool depth2_while_held;
    bool t2_at_boundary;   /* the guard is about to take lane A's mutex   */
    bool t2_done;
    QUIC_STATUS t2_status;
    bool t3_at_boundary;   /* the stats reader is about to take it too    */
    bool t3_done;
    moq_result_t t3_rc;
    moq_msquic_managed_lane_t *watch;
    bool b_waked;
    moq_result_t b_rc;
} g_sched = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void sched_reset(void)
{
    pthread_mutex_lock(&g_sched.mu);
    g_sched.a_parked = false;
    g_sched.release_a = false;
    g_sched.a_expired = false;
    g_sched.depth2_while_held = false;
    g_sched.t2_at_boundary = false;
    g_sched.t2_done = false;
    g_sched.t2_status = QUIC_STATUS_INTERNAL_ERROR;
    g_sched.t3_at_boundary = false;
    g_sched.t3_done = false;
    g_sched.t3_rc = MOQ_ERR_INTERNAL;
    g_sched.watch = NULL;
    g_sched.b_waked = false;
    g_sched.b_rc = MOQ_ERR_INTERNAL;
    pthread_mutex_unlock(&g_sched.mu);
}

static void sched_release_a(void)
{
    pthread_mutex_lock(&g_sched.mu);
    g_sched.release_a = true;
    pthread_cond_broadcast(&g_sched.cv);
    pthread_mutex_unlock(&g_sched.mu);
}

/* --- the cross-lane wake schedule ---------------------------------------- */

static struct {
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool l1_held;             /* lane 1's callback is parked, mu held      */
    bool l0_wake_returned;    /* lane 0's lane_wake call RETURNED          */
    bool l1_observed_return;  /* ... and lane 1 saw it while still held    */
    bool l1_expired;          /* the guard fired instead of the predicate  */
    bool release_l1;
    bool l0_issued;
    moq_result_t l0_rc;
} g_x = {
    .mu = PTHREAD_MUTEX_INITIALIZER,
    .cv = PTHREAD_COND_INITIALIZER,
};

static void xwake_reset(void)
{
    pthread_mutex_lock(&g_x.mu);
    g_x.l1_held = false;
    g_x.l0_wake_returned = false;
    g_x.l1_observed_return = false;
    g_x.l1_expired = false;
    g_x.release_l1 = false;
    g_x.l0_issued = false;
    g_x.l0_rc = MOQ_ERR_INTERNAL;
    pthread_mutex_unlock(&g_x.mu);
}

static void xwake_release(void)
{
    pthread_mutex_lock(&g_x.mu);
    g_x.release_l1 = true;
    pthread_cond_broadcast(&g_x.cv);
    pthread_mutex_unlock(&g_x.mu);
}

/* --- on_activity observer ------------------------------------------------ */

static struct {
    pthread_mutex_t mu;
    bool armed;
    uint64_t runs;
    moq_msquic_managed_lane_t *lane;
    moq_msquic_managed_conn_t *conn;
    bool next_conn_null;
    bool session_null;
    moq_result_t wake_rc;
    int shutdowns_after_close;
} g_obs = { .mu = PTHREAD_MUTEX_INITIALIZER };

static fake_msq_t *g_obs_fake;

static void on_activity_probe(moq_msquic_managed_t *m, void *ctx)
{
    (void)m;
    (void)ctx;
    pthread_mutex_lock(&g_obs.mu);
    if (!g_obs.armed) {
        pthread_mutex_unlock(&g_obs.mu);
        return;
    }
    g_obs.armed = false; /* one-shot: the observed bump is the named one */
    g_obs.runs++;
    g_obs.next_conn_null =
        moq_msquic_lane_next_conn(g_obs.lane, NULL) == NULL;
    g_obs.session_null =
        moq_msquic_managed_conn_session(g_obs.conn) == NULL;
    /* a close from here must be refused, so the transport must see no
     * shutdown at all before the legitimate in-pump close */
    moq_msquic_managed_conn_close(g_obs.conn, 0x11);
    g_obs.shutdowns_after_close = atomic_load(&g_obs_fake->conn_shutdowns);
    g_obs.wake_rc = moq_msquic_lane_wake(g_obs.lane);
    pthread_mutex_unlock(&g_obs.mu);
}

/* --- the pump ------------------------------------------------------------ */

static void pump_survey(moq_msquic_managed_lane_t *lane, uint32_t idx)
{
    moq_msquic_managed_conn_t *c = NULL;
    size_t seen = 0;

    while ((c = moq_msquic_lane_next_conn(lane, c)) != NULL) {
        seen++;
        if (g_pump.captured == NULL)
            g_pump.captured = c;
        if (moq_msquic_managed_conn_lane(c) != lane)
            g_pump.wrong_lane = true;
        if (g_pump.tag_users) {
            moq_msquic_managed_conn_set_user(c, USER_TAG(c));
        } else {
            if (moq_msquic_managed_conn_user(c) != USER_TAG(c))
                g_pump.user_mismatch = true;
            g_pump.user_checked++;
        }
    }
    if (idx < MAX_TEST_LANES)
        g_pump.seen[idx] = seen;
}

static void pump_terminal(moq_msquic_managed_lane_t *lane)
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
                g_pump.closed_seen++;
            } else {
                g_pump.other_events++;
            }
            moq_event_cleanup(&ev);
        }
        (void)closed;
        /* Acknowledge unconditionally once armed: observation is a durable
         * session fact, so the acknowledging pump need not be the one that
         * polled the terminal, and the product's own gate is the oracle. */
        if (g_pump.ack) {
            g_pump.last_ack_rc = moq_msquic_managed_conn_ack_terminal(c);
            if (g_pump.last_ack_rc == MOQ_OK)
                g_pump.acks++;
        }
    }
}

/* The server half of the deadline fixture: accept the peer's subscription,
 * adopt the delivery timeout it asks for, then open a subgroup and leave it
 * open -- that is what gives the session a lasting FAR deadline. */
static void pump_session(moq_msquic_managed_lane_t *lane, uint64_t now_us)
{
    moq_msquic_managed_conn_t *c = NULL;

    while ((c = moq_msquic_lane_next_conn(lane, c)) != NULL) {
        moq_session_t *s = moq_msquic_managed_conn_session(c);
        moq_event_t ev;

        if (s == NULL)
            continue;
        g_pump.child_state = moq_session_state(s);
        /* Every event is classified: the declared sequence owes exactly one
         * SUBSCRIBE_REQUEST and one SUBSCRIBE_UPDATED, and anything else is
         * recorded by kind rather than dropped. */
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) {
                g_pump.ev_setup++;
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                moq_accept_subscribe_cfg_t ac;

                g_pump.ev_sub_request++;
                /* count every request, accept only the declared first one --
                 * so a stray extra subscription is NAMED by the counter
                 * instead of silently replacing the arm's subscription */
                if (!g_pump.have_sub) {
                    g_pump.sub = ev.u.subscribe_request.sub;
                    moq_accept_subscribe_cfg_init(&ac);
                    if (moq_session_accept_subscribe(s, g_pump.sub, &ac,
                                                     now_us) == MOQ_OK)
                        g_pump.have_sub = true;
                    else
                        g_pump.accept_failed = true;
                }
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                g_pump.ev_sub_updated++;
                if (ev.u.subscribe_updated.sub._opaque !=
                    g_pump.sub._opaque)
                    g_pump.sub_handle_mismatch = true;
                g_pump.updated = true;
            } else {
                g_pump.ev_unexpected++;
                if (g_pump.ev_unexpected_kind == 0)
                    g_pump.ev_unexpected_kind = ev.kind;
            }
            moq_event_cleanup(&ev);
        }
        if (g_pump.have_sub && g_pump.updated && !g_pump.sg_open) {
            moq_subgroup_cfg_t sgc;
            moq_subgroup_handle_t sg;

            moq_subgroup_cfg_init(&sgc);
            sgc.group_id = 0;
            sgc.publisher_priority = 200;
            if (moq_session_open_subgroup(s, g_pump.sub, &sgc, now_us,
                                          &sg) == MOQ_OK) {
                g_pump.sg_open = true;
                g_pump.sg_opened_at = now_us;
                g_pump.dl = moq_session_next_deadline_us(s);
                g_pump.dl_now = now_us;
                g_pump.dl_read = true;
            }
        }
    }
}

static void pump_deadline(moq_msquic_managed_lane_t *lane, uint64_t now_us)
{
    moq_msquic_managed_conn_t *c = moq_msquic_lane_next_conn(lane, NULL);
    moq_session_t *s = c != NULL ? moq_msquic_managed_conn_session(c) : NULL;

    if (s == NULL)
        return;
    g_pump.dl = moq_session_next_deadline_us(s);
    g_pump.dl_now = now_us;
    g_pump.dl_read = true;
}

static void pump_hold_a(moq_msquic_managed_t *m,
                        moq_msquic_managed_lane_t *lane, uint32_t idx)
{
    struct timespec abs;

    if (idx == 0) {
        pthread_mutex_lock(&g_sched.mu);
        if (!g_sched.a_parked) {
            g_sched.a_parked = true;
            pthread_cond_broadcast(&g_sched.cv);
            guard_deadline(&abs);
            while (!g_sched.release_a)
                if (pthread_cond_timedwait(&g_sched.cv, &g_sched.mu,
                                           &abs) != 0) {
                    g_sched.a_expired = true;
                    break;
                }
        }
        pthread_mutex_unlock(&g_sched.mu);
        return;
    }
    (void)lane;
    pthread_mutex_lock(&g_sched.mu);
    if (!g_sched.b_waked) {
        moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);

        g_sched.b_rc = moq_msquic_lane_wake(l0);
        g_sched.b_waked = true;
        pthread_cond_broadcast(&g_sched.cv);
    }
    pthread_mutex_unlock(&g_sched.mu);
}

static void pump_xwake(moq_msquic_managed_t *m,
                       moq_msquic_managed_lane_t *lane, uint32_t idx)
{
    struct timespec abs;

    if (idx == 1) {
        pthread_mutex_lock(&g_x.mu);
        if (!g_x.l1_held) {
            g_x.l1_held = true;
            pthread_cond_broadcast(&g_x.cv);
            guard_deadline(&abs);
            /* the load-bearing predicate: lane 0's call must RETURN while
             * this callback still holds lane 1 */
            while (!g_x.l0_wake_returned && !g_x.release_l1)
                if (pthread_cond_timedwait(&g_x.cv, &g_x.mu, &abs) != 0) {
                    g_x.l1_expired = true;
                    break;
                }
            g_x.l1_observed_return = g_x.l0_wake_returned;
        }
        pthread_mutex_unlock(&g_x.mu);
        return;
    }
    (void)lane;
    bool issue = false;

    pthread_mutex_lock(&g_x.mu);
    if (!g_x.l0_issued) {
        g_x.l0_issued = true;
        issue = true;
    }
    pthread_mutex_unlock(&g_x.mu);
    if (!issue)
        return;

    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);
    moq_result_t rc = moq_msquic_lane_wake(l1);

    pthread_mutex_lock(&g_x.mu);
    g_x.l0_rc = rc;
    g_x.l0_wake_returned = true;
    pthread_cond_broadcast(&g_x.cv);
    pthread_mutex_unlock(&g_x.mu);
}

/* The same cross-lane arm, issued from lane 1's callback with no parking:
 * the deadline row needs the wake's CAUSE recorded on lane 0, not a hold. */
static void pump_xissue(moq_msquic_managed_t *m)
{
    bool issue = false;

    pthread_mutex_lock(&g_x.mu);
    if (!g_x.l0_issued) {
        g_x.l0_issued = true;
        issue = true;
    }
    pthread_mutex_unlock(&g_x.mu);
    if (!issue)
        return;

    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_result_t rc = moq_msquic_lane_wake(l0);

    pthread_mutex_lock(&g_x.mu);
    g_x.l0_rc = rc;
    g_x.l0_wake_returned = true;
    pthread_cond_broadcast(&g_x.cv);
    pthread_mutex_unlock(&g_x.mu);
}

static int test_pump(moq_msquic_managed_t *m,
                     moq_msquic_managed_lane_t *lane, uint64_t now_us,
                     void *user)
{
    uint32_t idx = moq_msquic_lane_index(lane);
    enum pump_mode mode;

    (void)user;
    if (idx >= MAX_TEST_LANES) {
        pthread_mutex_lock(&g_pump.mu);
        g_pump.bad_index = true;
        pthread_mutex_unlock(&g_pump.mu);
        return 0;
    }

    pthread_mutex_lock(&g_pump.mu);
    mode = g_pump.mode;
    g_pump.depth[idx]++;
    if (g_pump.depth[idx] > g_pump.max_depth[idx])
        g_pump.max_depth[idx] = g_pump.depth[idx];
    bool nested = g_pump.depth[idx] > 1;
    pthread_mutex_unlock(&g_pump.mu);

    if (nested) {
        pthread_mutex_lock(&g_sched.mu);
        if (g_sched.a_parked && !g_sched.release_a)
            g_sched.depth2_while_held = true;
        pthread_mutex_unlock(&g_sched.mu);
    }

    switch (mode) {
    case PUMP_SURVEY:
        pthread_mutex_lock(&g_pump.mu);
        pump_survey(lane, idx);
        pthread_mutex_unlock(&g_pump.mu);
        break;
    case PUMP_TERMINAL:
        pthread_mutex_lock(&g_pump.mu);
        pump_terminal(lane);
        pthread_mutex_unlock(&g_pump.mu);
        break;
    case PUMP_DEADLINE:
        pthread_mutex_lock(&g_pump.mu);
        pump_deadline(lane, now_us);
        pthread_mutex_unlock(&g_pump.mu);
        break;
    case PUMP_SESSION:
        pthread_mutex_lock(&g_pump.mu);
        pump_session(lane, now_us);
        pthread_mutex_unlock(&g_pump.mu);
        break;
    case PUMP_CLOSE:
        pthread_mutex_lock(&g_pump.mu);
        if (g_pump.close_target != NULL) {
            moq_msquic_managed_conn_close(g_pump.close_target, 0x22);
            g_pump.close_target = NULL;
            g_pump.closes_issued++;
        }
        pthread_mutex_unlock(&g_pump.mu);
        break;
    case PUMP_HOLD_A:
        pump_hold_a(m, lane, idx);
        break;
    case PUMP_XWAKE:
        pump_xwake(m, lane, idx);
        break;
    case PUMP_XISSUE:
        if (idx == 1)
            pump_xissue(m);
        break;
    case PUMP_IDLE:
        break;
    }

    pthread_mutex_lock(&g_pump.mu);
    g_pump.depth[idx]--;
    g_pump.calls[idx]++;
    pthread_cond_broadcast(&g_pump.cv);
    pthread_mutex_unlock(&g_pump.mu);
    return 0;
}

/* --- rig ----------------------------------------------------------------- */

/* One fake owns the borrowed API table; one fake per synthesized accept
 * supplies a distinct connection handle. They are file-scope because a
 * fake_msq_t carries the whole per-endpoint stream/send model. */
static fake_msq_t g_table_fake;
static fake_msq_t g_conn_fake[MAX_TEST_CONNS];

struct rig {
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane[MAX_TEST_LANES];
    uint32_t lane_count;
    bool live[MAX_TEST_CONNS];   /* accepted and not yet quiesced */
};

static bool rig_up(struct rig *r, uint32_t lane_count, uint32_t max_conns,
                   uint64_t session_idle_timeout_us,
                   moq_msquic_choose_lane_fn choose_lane, void *choose_user,
                   moq_msquic_activity_fn on_activity)
{
    moq_msquic_managed_cfg_t cfg;

    memset(r, 0, sizeof(*r));
    fake_msq_init(&g_table_fake, false);
    for (int i = 0; i < MAX_TEST_CONNS; i++)
        fake_msq_init(&g_conn_fake[i], false);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.max_connections = max_conns;
    cfg.lane_count = lane_count;
    cfg.on_lane_pump = test_pump;
    cfg.choose_lane = choose_lane;
    cfg.choose_lane_user = choose_user;
    cfg.on_activity = on_activity;
    cfg.session_idle_timeout_us = session_idle_timeout_us;
    /* a peer that subscribes needs request capacity from this side, and the
     * facade only grants it when asked */
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 16;
    if (moq_msq_test_managed_create_lanes_only_api(
            &cfg, fake_msq_table(&g_table_fake), &r->m) != MOQ_OK)
        return false;
    r->lane_count = lane_count;
    /* Fail-CLOSED on the lane accessors: every later step indexes r->lane[]
     * and the pre-wait gate keys on the reported index, so a broken accessor
     * must stop the rig here rather than be dereferenced downstream. The
     * accessor CONTRACTS themselves are proven by t_lane_accessors. */
    if (moq_msquic_managed_lane_count(r->m) != lane_count) {
        CHECK(false);
        return false;
    }
    for (uint32_t i = 0; i < lane_count; i++) {
        r->lane[i] = moq_msquic_managed_lane(r->m, i);
        if (r->lane[i] == NULL || moq_msquic_lane_index(r->lane[i]) != i) {
            CHECK(false);
            return false;
        }
        for (uint32_t j = 0; j < i; j++)
            if (r->lane[j] == r->lane[i]) {
                CHECK(false);
                return false;
            }
    }
    return true;
}

static QUIC_STATUS deliver_registered(fake_msq_t *f,
                                      QUIC_CONNECTION_EVENT_TYPE type,
                                      uint64_t code)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = type;
    if (type == QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER)
        ev.SHUTDOWN_INITIATED_BY_PEER.ErrorCode = code;
    else if (type == QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE) {
        ev.STREAMS_AVAILABLE.BidirectionalCount = (uint16_t)code;
        ev.STREAMS_AVAILABLE.UnidirectionalCount = (uint16_t)code;
    }
    return fake_msq_deliver_conn_event(f, &ev);
}

/* Accept through the exact production listener callback. */
static QUIC_STATUS rig_accept(struct rig *r, int slot)
{
    QUIC_STATUS st = moq_msq_test_listener_accept(
        r->m, fake_msq_conn_handle(&g_conn_fake[slot]));

    if (QUIC_SUCCEEDED(st))
        r->live[slot] = true;
    return st;
}

/* A locally shut connection delivers SHUTDOWN_COMPLETE; without it stop()
 * would legitimately wait for a transport terminal that never arrives. */
static void rig_quiesce(struct rig *r)
{
    gate_disable();
    sched_release_a();
    xwake_release();
    for (int i = 0; i < MAX_TEST_CONNS; i++) {
        if (!r->live[i] || !fake_msq_conn_cb_installed(&g_conn_fake[i]))
            continue;
        (void)deliver_registered(&g_conn_fake[i],
                                 QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE, 0);
        r->live[i] = false;
    }
}

static void rig_down(struct rig *r)
{
    rig_quiesce(r);
    if (r->m != NULL) {
        CHECK(moq_msquic_managed_stop(r->m) == MOQ_OK);
        moq_msquic_managed_destroy(r->m);
        r->m = NULL;
    }
}

/* --- choosers ------------------------------------------------------------ */

static struct {
    uint32_t calls;
    bool bad_info;
    uint32_t returns[8];
    uint32_t next;
} g_chooser;

static void chooser_reset(const uint32_t *plan, uint32_t n)
{
    memset(&g_chooser, 0, sizeof(g_chooser));
    for (uint32_t i = 0; i < n && i < 8; i++)
        g_chooser.returns[i] = plan[i];
    g_chooser.next = n;
}

static uint32_t choose_planned(moq_msquic_managed_t *m,
                               const moq_msquic_accept_info_t *info,
                               void *user)
{
    (void)m;
    (void)user;
    if (info == NULL || info->struct_size != (uint32_t)sizeof(*info))
        g_chooser.bad_info = true;
    uint32_t i = g_chooser.calls++;

    return i < g_chooser.next ? g_chooser.returns[i] : 0;
}

/* --- 0. lane accessor contracts ------------------------------------------ */

/* The three public lane accessors every later row indexes through. Proven on
 * their own facade, before any rig, so a broken accessor is named here rather
 * than surfacing as a downstream dereference. */
static void t_lane_accessors(void)
{
    int before = failures;
    moq_msquic_managed_cfg_t cfg;
    moq_msquic_managed_t *m = NULL;

    set_now(900000);
    gate_prepare(0);
    pump_reset();
    fake_msq_init(&g_table_fake, false);
    moq_msquic_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    cfg.max_connections = 2;
    cfg.lane_count = 2;
    cfg.on_lane_pump = test_pump;
    CHECK(moq_msq_test_managed_create_lanes_only_api(
              &cfg, fake_msq_table(&g_table_fake), &m) == MOQ_OK);
    if (m == NULL)
        return;

    CHECK(moq_msquic_managed_lane_count(m) == 2);

    moq_msquic_managed_lane_t *l0 = moq_msquic_managed_lane(m, 0);
    moq_msquic_managed_lane_t *l1 = moq_msquic_managed_lane(m, 1);

    CHECK(l0 != NULL);
    CHECK(l1 != NULL);
    /* identity, not just non-NULL: a lookup that returns the same lane for
     * every index satisfies non-NULL and must still be rejected */
    CHECK(l0 != l1);
    if (l0 != NULL)
        CHECK(moq_msquic_lane_index(l0) == 0);
    if (l1 != NULL)
        CHECK(moq_msquic_lane_index(l1) == 1);

    /* out of range and NULL inputs */
    CHECK(moq_msquic_managed_lane(m, 2) == NULL);
    CHECK(moq_msquic_managed_lane(m, UINT32_MAX) == NULL);
    CHECK(moq_msquic_managed_lane(NULL, 0) == NULL);
    CHECK(moq_msquic_managed_lane_count(NULL) == 0);
    CHECK(moq_msquic_lane_index(NULL) == 0);

    CHECK(moq_msquic_managed_stop(m) == MOQ_OK);
    moq_msquic_managed_destroy(m);
    if (failures == before)
        printf("PASS: lane count, in-range lookup identity and lane index\n");
}

/* --- 1. lane partition over synthesized accepts -------------------------- */

/* Placement is deliberately NOT the default round-robin order, so a mutant
 * that ignores the chooser's return is observable in the per-lane counts. */
static void t_partition(void)
{
    int before = failures;
    struct rig r;
    const uint32_t plan[3] = { 1, 1, 0 };

    set_now(1000000);
    gate_prepare((1u << 0) | (1u << 1));
    pump_reset();
    chooser_reset(plan, 3);
    if (!rig_up(&r, 2, 4, 0, choose_planned, NULL, NULL)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(0, 1));
    CHECK(gate_await(1, 1));

    /* Placement is proven PER ACCEPTED IDENTITY, not by final totals: each
     * accept is followed by a survey, so the cumulative pair after accept k
     * is unique to the chooser's answer for that identity. */
    static const size_t want0[3] = { 0, 0, 1 };
    static const size_t want1[3] = { 1, 2, 2 };
    uint64_t entry = 1;

    pump_arm(PUMP_SURVEY);
    for (int i = 0; i < 3; i++) {
        CHECK(rig_accept(&r, i) == QUIC_STATUS_SUCCESS);
        CHECK(moq_msquic_managed_wake(r.m) == MOQ_OK);
        gate_release(0, entry);
        gate_release(1, entry);
        entry++;
        CHECK(pump_await_calls(0, (uint64_t)i + 1));
        CHECK(pump_await_calls(1, (uint64_t)i + 1));
        CHECK(gate_await(0, entry));
        CHECK(gate_await(1, entry));
        pthread_mutex_lock(&g_pump.mu);
        CHECK(g_pump.seen[0] == want0[i]);
        CHECK(g_pump.seen[1] == want1[i]);
        pthread_mutex_unlock(&g_pump.mu);
    }
    CHECK(g_chooser.calls == 3);
    CHECK(!g_chooser.bad_info);
    CHECK(moq_msquic_managed_conn_count(r.m) == 3);
    for (int i = 0; i < 3; i++) {
        CHECK(fake_msq_conn_cb_installed(&g_conn_fake[i]));
        CHECK(atomic_load(&g_conn_fake[i].conn_set_configs) == 1);
    }
    CHECK(atomic_load(&g_conn_fake[3].conn_set_configs) == 0);
    pthread_mutex_lock(&g_pump.mu);
    CHECK(!g_pump.wrong_lane);
    CHECK(!g_pump.bad_index);
    g_pump.tag_users = false;
    pthread_mutex_unlock(&g_pump.mu);

    /* final one-pass survey: the lane owning two children sees BOTH in a
     * single iteration, and every user slot survived across pumps */
    CHECK(moq_msquic_managed_wake(r.m) == MOQ_OK);
    gate_release(0, entry);
    gate_release(1, entry);
    entry++;
    CHECK(pump_await_calls(0, 4));
    CHECK(pump_await_calls(1, 4));
    CHECK(gate_await(0, entry));
    CHECK(gate_await(1, entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.seen[0] == 1);
    CHECK(g_pump.seen[1] == 2);
    CHECK(g_pump.user_checked == 3);
    CHECK(!g_pump.user_mismatch);
    CHECK(!g_pump.wrong_lane);
    CHECK(g_pump.max_depth[0] == 1);
    CHECK(g_pump.max_depth[1] == 1);
    pthread_mutex_unlock(&g_pump.mu);

    rig_down(&r);
    if (failures == before)
        printf("PASS: chooser placement, lane-scoped iteration, user slots\n");
}

/* --- 2. out-of-range chooser refusal ------------------------------------- */

static void t_chooser_refusal(void)
{
    int before = failures;
    struct rig r;
    const uint32_t plan[2] = { 0, 99 };

    set_now(1100000);
    gate_prepare((1u << 0) | (1u << 1));
    pump_reset();
    chooser_reset(plan, 2);
    if (!rig_up(&r, 2, 4, 0, choose_planned, NULL, NULL)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(0, 1));
    CHECK(gate_await(1, 1));

    CHECK(rig_accept(&r, 0) == QUIC_STATUS_SUCCESS);
    size_t admitted = moq_msquic_managed_conn_count(r.m);

    CHECK(admitted == 1);
    /* out of range refuses rather than clamping, and releases the reserve */
    CHECK(rig_accept(&r, 1) == QUIC_STATUS_CONNECTION_REFUSED);
    CHECK(g_chooser.calls == 2);
    CHECK(moq_msquic_managed_conn_count(r.m) == admitted);
    CHECK(!fake_msq_conn_cb_installed(&g_conn_fake[1]));
    CHECK(atomic_load(&g_conn_fake[1].conn_set_configs) == 0);
    CHECK(atomic_load(&g_conn_fake[1].conn_closes) == 0);

    pump_arm(PUMP_SURVEY);
    CHECK(moq_msquic_managed_wake(r.m) == MOQ_OK);
    gate_release(0, 1);
    gate_release(1, 1);
    CHECK(pump_await_calls(0, 1));
    CHECK(pump_await_calls(1, 1));
    CHECK(gate_await(0, 2));
    CHECK(gate_await(1, 2));
    pthread_mutex_lock(&g_pump.mu);
    /* the refused identity is published to no lane */
    CHECK(g_pump.seen[0] == 1);
    CHECK(g_pump.seen[1] == 0);
    CHECK(!g_pump.wrong_lane);
    pthread_mutex_unlock(&g_pump.mu);

    rig_down(&r);
    if (failures == before)
        printf("PASS: out-of-range chooser refuses, releases, publishes nothing\n");
}

/* --- 3. default round-robin placement (NEW COVERAGE) --------------------- */

/* Not inherited from the physical fixture: nothing exercised the NULL-chooser
 * pick before. Placement is asserted per accept, so the exact order 0,1,0,1 is
 * what passes -- cumulative counts alone would not distinguish 1,0,1,0. */
static void t_default_round_robin(void)
{
    int before = failures;
    struct rig r;
    static const size_t want0[MAX_TEST_CONNS] = { 1, 1, 2, 2 };
    static const size_t want1[MAX_TEST_CONNS] = { 0, 1, 1, 2 };

    set_now(1200000);
    gate_prepare((1u << 0) | (1u << 1));
    pump_reset();
    if (!rig_up(&r, 2, 4, 0, NULL, NULL, NULL)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(0, 1));
    CHECK(gate_await(1, 1));
    pump_arm(PUMP_SURVEY);

    for (int i = 0; i < MAX_TEST_CONNS; i++) {
        CHECK(rig_accept(&r, i) == QUIC_STATUS_SUCCESS);
        CHECK(moq_msquic_managed_wake(r.m) == MOQ_OK);
        gate_release(0, (uint64_t)i + 1);
        gate_release(1, (uint64_t)i + 1);
        CHECK(pump_await_calls(0, (uint64_t)i + 1));
        CHECK(pump_await_calls(1, (uint64_t)i + 1));
        CHECK(gate_await(0, (uint64_t)i + 2));
        CHECK(gate_await(1, (uint64_t)i + 2));
        pthread_mutex_lock(&g_pump.mu);
        CHECK(g_pump.seen[0] == want0[i]);
        CHECK(g_pump.seen[1] == want1[i]);
        pthread_mutex_unlock(&g_pump.mu);
    }
    pthread_mutex_lock(&g_pump.mu);
    CHECK(!g_pump.wrong_lane);
    CHECK(!g_pump.bad_index);
    pthread_mutex_unlock(&g_pump.mu);

    rig_down(&r);
    if (failures == before)
        printf("PASS: default round-robin places 0,1,0,1 exactly\n");
}

/* --- 4. terminal visible, acknowledged, then reclaimed ------------------- */

static void t_terminal_before_reclamation(void)
{
    int before = failures;
    struct rig r;
    const uint32_t plan[1] = { 0 };

    set_now(1300000);
    gate_prepare(1u << 0);
    pump_reset();
    chooser_reset(plan, 1);
    if (!rig_up(&r, 1, 2, 0, choose_planned, NULL, NULL)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(0, 1));
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_SUCCESS);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    pump_arm(PUMP_TERMINAL);
    CHECK(deliver_registered(&g_conn_fake[0],
                             QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER,
                             0x0) == QUIC_STATUS_SUCCESS);
    CHECK(deliver_registered(&g_conn_fake[0],
                             QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE,
                             0) == QUIC_STATUS_SUCCESS);
    r.live[0] = false; /* the transport terminal is already delivered */

    /* the terminal is visible in a lane pump ... */
    gate_release(0, 1);
    CHECK(pump_await_calls(0, 1));
    CHECK(gate_await(0, 2));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.closed_seen == 1);
    CHECK(g_pump.other_events == 0);
    pthread_mutex_unlock(&g_pump.mu);
    /* ... and reaching the next wait means the reap window already ran */
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);
    CHECK(atomic_load(&g_conn_fake[0].conn_closes) == 0);

    /* an unacknowledged terminal child survives a further pump */
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    gate_release(0, 2);
    CHECK(pump_await_calls(0, 2));
    CHECK(gate_await(0, 3));
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);
    CHECK(atomic_load(&g_conn_fake[0].conn_closes) == 0);

    /* acknowledging it permits ordinary reclamation, before any stop */
    pthread_mutex_lock(&g_pump.mu);
    g_pump.ack = true;
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    gate_release(0, 3);
    CHECK(pump_await_calls(0, 3));
    CHECK(gate_await(0, 4));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.acks == 1);
    CHECK(g_pump.last_ack_rc == MOQ_OK);
    CHECK(g_pump.closed_seen == 1);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(moq_msquic_managed_conn_count(r.m) == 0);
    CHECK(atomic_load(&g_conn_fake[0].conn_closes) == 1);

    rig_down(&r);
    if (failures == before)
        printf("PASS: terminal observed, acknowledged, then reclaimed\n");
}

/* --- 5. lane-selective wake and managed wake-all ------------------------- */

static void t_wake_selectivity(void)
{
    int before = failures;
    struct rig r;

    set_now(1400000);
    gate_prepare((1u << 0) | (1u << 1));
    pump_reset();
    /* lane_count is clamped to max_connections, so the cap must admit both
     * lanes even though this row accepts nothing */
    if (!rig_up(&r, 2, 2, 0, NULL, NULL, NULL)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(0, 1));
    CHECK(gate_await(1, 1));

    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    gate_release(0, 1);
    gate_release(1, 1);
    CHECK(pump_await_calls(0, 1));
    /* lane 1 completed a whole loop iteration and reached its wait again;
     * that it pumped nothing is therefore a decided fact, not a timeout */
    CHECK(gate_await(1, 2));
    CHECK(gate_await(0, 2));
    CHECK(pump_calls(1) == 0);
    CHECK(pump_calls(0) == 1);

    CHECK(moq_msquic_managed_wake(r.m) == MOQ_OK);
    gate_release(0, 2);
    gate_release(1, 2);
    CHECK(pump_await_calls(0, 2));
    CHECK(pump_await_calls(1, 1));
    CHECK(gate_await(0, 3));
    CHECK(gate_await(1, 3));
    CHECK(pump_calls(0) == 2);
    CHECK(pump_calls(1) == 1);

    rig_down(&r);
    if (failures == before)
        printf("PASS: lane wake is selective, managed wake reaches every lane\n");
}

/* --- 6. cross-lane wake returns while the target is held ----------------- */

static void t_cross_lane_wake_returns_while_held(void)
{
    int before = failures;
    struct rig r;

    set_now(1500000);
    gate_prepare(0);
    pump_reset();
    xwake_reset();
    if (!rig_up(&r, 2, 2, 0, NULL, NULL, NULL)) {
        rig_down(&r);
        return;
    }
    pump_arm(PUMP_XWAKE);

    /* lane 1 parks inside its own pump, holding its lane mutex */
    CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
    struct timespec abs;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_x.mu);
    while (!g_x.l1_held)
        if (pthread_cond_timedwait(&g_x.cv, &g_x.mu, &abs) != 0)
            break;
    bool held = g_x.l1_held;

    pthread_mutex_unlock(&g_x.mu);
    CHECK(held);

    /* lane 0's callback issues the cross-lane wake to the held lane */
    uint64_t l1_before = pump_calls(1);

    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    guard_deadline(&abs);
    pthread_mutex_lock(&g_x.mu);
    while (!g_x.l0_wake_returned)
        if (pthread_cond_timedwait(&g_x.cv, &g_x.mu, &abs) != 0)
            break;
    pthread_mutex_unlock(&g_x.mu);

    xwake_release();
    CHECK(pump_await_calls(1, l1_before + 2));

    pthread_mutex_lock(&g_x.mu);
    CHECK(g_x.l0_rc == MOQ_OK);
    CHECK(g_x.l1_observed_return); /* the return was seen WHILE held */
    CHECK(!g_x.l1_expired);
    pthread_mutex_unlock(&g_x.mu);
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.max_depth[0] == 1);
    CHECK(g_pump.max_depth[1] == 1);
    pthread_mutex_unlock(&g_pump.mu);

    rig_down(&r);
    if (failures == before)
        printf("PASS: cross-lane wake returns while held, then repumps\n");
}

/* --- raw peer for the deadline pair --------------------------------------

 * The managed child is the SERVER. A raw CLIENT session drives a real
 * SUBSCRIBE and a real delivery-timeout update over its own bridge and fake
 * endpoint; its wire bytes are relayed into the child through the production
 * connection/stream callbacks, and the child's control writes are relayed
 * back. No socket and no second adapter: this is the same shape the attach
 * units use for their receive pair. */

struct peer {
    moq_session_t *s;
    moq_transport_bridge_t *bridge;
    fake_endpoint_t ep;
    fake_msq_t *child;          /* the accepted child's fake transport */
    fake_msq_stream_t *ctrl;    /* the child's view of the control bidi */
    uint64_t ctrl_id;
    bool ctrl_open;
    size_t child_send_cur;

    /* Fail-closed classification. Every relayed result is checked and every
     * endpoint operation and child send is recognised as either the control
     * traffic or the subgroup traffic this fixture declares; anything else is
     * counted and named rather than dropped. */
    uint64_t svc_bad;          /* peer bridge service != MOQ_OK          */
    uint64_t ing_bad;          /* peer control ingress != MOQ_OK         */
    uint64_t deliver_bad;      /* PEER_STREAM_STARTED delivery failed    */
    uint64_t op_open_bidi;     /* the one control bidi                   */
    uint64_t op_ctrl_write;    /* writes on it                           */
    uint64_t op_unexpected;
    int op_unexpected_kind;
    uint64_t send_ctrl;        /* child writes on the control bidi       */
    uint64_t send_subgroup;    /* child writes on its own uni subgroup   */
    uint64_t send_unexpected;
};

static bool peer_up(struct peer *p, fake_msq_t *child)
{
    moq_session_cfg_t scfg;

    memset(p, 0, sizeof(*p));
    p->child = child;
    p->op_unexpected_kind = -1;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 16;
    if (moq_session_create(&scfg, 0, &p->s) < 0)
        return false;
    fake_endpoint_init(&p->ep, 2, 0); /* client-initiated id bases */
    moq_transport_bridge_cfg_t bcfg;

    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    return moq_transport_bridge_create(&bcfg, p->s, &p->ep.vtable, &p->ep,
                                       &p->bridge) == MOQ_OK;
}

static void peer_down(struct peer *p)
{
    if (p->bridge != NULL)
        moq_transport_bridge_destroy(p->bridge);
    if (p->s != NULL)
        moq_session_destroy(p->s);
    p->bridge = NULL;
    p->s = NULL;
}

static QUIC_STATUS deliver_peer_bidi(fake_msq_t *f, fake_msq_stream_t *st)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED;
    ev.PEER_STREAM_STARTED.Stream = (HQUIC)st;
    ev.PEER_STREAM_STARTED.Flags = 0; /* bidirectional */
    return fake_msq_deliver_conn_event(f, &ev);
}

/* One relay step, run from the main thread while the child's doorbell is
 * parked at its pre-wait: service the peer, hand its wire bytes to the child
 * through the production callbacks, then hand the child's control writes
 * back. */
static void peer_relay(struct peer *p, uint64_t now_us)
{
    if (moq_transport_bridge_service(p->bridge, now_us) != MOQ_OK)
        p->svc_bad++;
    for (size_t i = 0; i < p->ep.count; i++) {
        fake_op_t *o = &p->ep.ops[i];

        if (o->kind == FAKE_OP_OPEN_BIDI && !p->ctrl_open) {
            p->op_open_bidi++;
            p->ctrl_id = o->stream_id;
            p->ctrl = fake_msq_peer_stream(p->child, o->stream_id, false);
            p->ctrl_open = p->ctrl != NULL;
            if (!p->ctrl_open ||
                deliver_peer_bidi(p->child, p->ctrl) != QUIC_STATUS_SUCCESS)
                p->deliver_bad++;
        } else if (o->kind == FAKE_OP_WRITE && p->ctrl_open &&
                   o->stream_id == p->ctrl_id) {
            p->op_ctrl_write++;
            fake_msq_deliver_receive(p->ctrl, o->data, o->data_len, o->fin);
        } else {
            p->op_unexpected++;
            if (p->op_unexpected_kind < 0)
                p->op_unexpected_kind = (int)o->kind;
        }
    }
    fake_endpoint_clear_ops(&p->ep);

    for (; p->child_send_cur < (size_t)p->child->send_count;
         p->child_send_cur++) {
        fake_msq_send_t *snd = &p->child->sends[p->child_send_cur];

        if (p->ctrl_open && snd->stream == p->ctrl) {
            p->send_ctrl++;
            if (moq_transport_bridge_on_peer_control_bytes(
                    p->bridge, p->ctrl_id, snd->bytes, snd->bytes_len,
                    (snd->flags & QUIC_SEND_FLAG_FIN) != 0, now_us) != MOQ_OK)
                p->ing_bad++;
        } else if (snd->stream != NULL && snd->stream->uni) {
            /* the child's own subgroup stream: expected, and deliberately not
             * relayed -- this fixture arms a deadline, it does not deliver
             * objects */
            p->send_subgroup++;
        } else {
            p->send_unexpected++;
        }
    }
    while (fake_msq_deliver_send_complete(p->child, false))
        ;
}

/* Relay, then let the child's lane run exactly one doorbell cycle. */
static bool peer_step(struct peer *p, struct rig *r, uint64_t *entry,
                      uint64_t now_us)
{
    peer_relay(p, now_us);
    CHECK(moq_msquic_lane_wake(r->lane[0]) == MOQ_OK);
    gate_release(0, *entry);
    (*entry)++;
    return gate_await(0, *entry);
}

/* --- 7. far deadline, idle cap, and wake consumption --------------------- */

#define FAR_DELIVERY_US 30000000u /* the subgroup's delivery timeout */
#define CAP_STEP_US 200000u       /* the production doorbell idle cap */

static void t_far_deadline_causes(void)
{
    int before = failures;
    struct rig r;
    struct peer p;
    moq_msquic_lane_stats_t a, b, c, d;
    const uint32_t plan[1] = { 0 };
    const uint64_t t0 = 2000000;
    uint64_t entry = 1;

    set_now(t0);
    gate_prepare(1u << 0);
    pump_reset();
    chooser_reset(plan, 1);
    /* no session idle timeout: the ONLY finite deadline this row can read is
     * the subgroup delivery deadline it arms below */
    if (!rig_up(&r, 2, 2, 0, choose_planned, NULL, NULL)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(0, entry));
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_SUCCESS);
    CHECK(deliver_registered(&g_conn_fake[0],
                             QUIC_CONNECTION_EVENT_CONNECTED,
                             0) == QUIC_STATUS_SUCCESS);
    CHECK(deliver_registered(&g_conn_fake[0],
                             QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE,
                             16) == QUIC_STATUS_SUCCESS);

    if (!peer_up(&p, &g_conn_fake[0])) {
        peer_down(&p);
        rig_down(&r);
        CHECK(false);
        return;
    }
    pump_arm(PUMP_SESSION);
    CHECK(moq_session_start(p.s, t0) == MOQ_OK);

    /* establish, subscribe, ask for the far delivery timeout, open the
     * subgroup -- every step driven by real wire bytes on both sides */
    bool subscribed = false;
    bool asked = false;
    uint64_t cev_setup = 0, cev_sub_ok = 0, cev_unexpected = 0;
    uint32_t cev_unexpected_kind = 0;
    bool sub_ok_mismatch = false;
    moq_subscription_t csub = MOQ_SUBSCRIPTION_INVALID;
    static const moq_bytes_t ns[] = { { (const uint8_t *)"lane", 4 } };

    for (int i = 0; i < 24 && !g_pump.sg_open; i++) {
        moq_event_t ev;

        while (moq_session_poll_events(p.s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE)
                cev_setup++;
            else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
                cev_sub_ok++;
                if (ev.u.subscribe_ok.sub._opaque != csub._opaque)
                    sub_ok_mismatch = true;
            } else {
                cev_unexpected++;
                if (cev_unexpected_kind == 0)
                    cev_unexpected_kind = ev.kind;
            }
            if (ev.kind == MOQ_EVENT_SETUP_COMPLETE && !subscribed) {
                moq_subscribe_cfg_t sc;

                moq_subscribe_cfg_init(&sc);
                sc.track_namespace.parts = (moq_bytes_t *)ns;
                sc.track_namespace.count = 1;
                sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
                sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
                if (moq_session_subscribe(p.s, &sc, t0, &csub) == MOQ_OK)
                    subscribed = true;
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK && !asked) {
                moq_subscription_update_cfg_t uc;

                moq_subscription_update_cfg_init(&uc);
                uc.has_delivery_timeout = true;
                uc.delivery_timeout_us = FAR_DELIVERY_US;
                if (moq_session_update_subscription(p.s, csub, &uc, t0) ==
                    MOQ_OK)
                    asked = true;
            }
            moq_event_cleanup(&ev);
        }
        if (!peer_step(&p, &r, &entry, t0))
            break;
    }

    CHECK(subscribed);
    CHECK(asked);

    /* One more relay so the subgroup's own stream traffic is classified too,
     * then the whole arm is checked fail-closed. */
    peer_relay(&p, t0);
    /* both peers reached exactly the declared sequence, and nothing else */
    CHECK(cev_setup == 1);
    CHECK(cev_sub_ok == 1);
    CHECK(cev_unexpected == 0);
    if (cev_unexpected != 0)
        fprintf(stderr, "  unexpected client event kind=%u\n",
                cev_unexpected_kind);
    CHECK(!sub_ok_mismatch);
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.ev_setup == 1);
    CHECK(g_pump.ev_sub_request == 1);
    CHECK(g_pump.ev_sub_updated == 1);
    CHECK(g_pump.ev_unexpected == 0);
    if (g_pump.ev_unexpected != 0)
        fprintf(stderr, "  unexpected server event kind=%u\n",
                g_pump.ev_unexpected_kind);
    CHECK(!g_pump.sub_handle_mismatch);
    CHECK(!g_pump.accept_failed);
    CHECK(g_pump.child_state == MOQ_SESS_ESTABLISHED);
    pthread_mutex_unlock(&g_pump.mu);

    /* every relayed result succeeded, and every endpoint operation and child
     * send was one of the two traffic classes this fixture declares */
    CHECK(p.svc_bad == 0);
    CHECK(p.ing_bad == 0);
    CHECK(p.deliver_bad == 0);
    CHECK(p.op_open_bidi == 1);
    CHECK(p.op_ctrl_write == 3);
    CHECK(p.op_unexpected == 0);
    if (p.op_unexpected != 0)
        fprintf(stderr, "  unexpected endpoint op kind=%d\n",
                p.op_unexpected_kind);
    CHECK(p.send_ctrl == 3);
    CHECK(p.send_subgroup == 1);
    CHECK(p.send_unexpected == 0);
    CHECK(moq_session_state(p.s) == MOQ_SESS_ESTABLISHED);
    CHECK(!moq_transport_bridge_is_fatal(p.bridge));
    CHECK(!moq_transport_bridge_is_closed(p.bridge));

    pthread_mutex_lock(&g_pump.mu);
    bool armed = g_pump.sg_open && g_pump.dl_read;

    CHECK(g_pump.have_sub);
    CHECK(g_pump.updated);
    CHECK(armed);
    if (armed) {
        /* the armed deadline is an EXACT injected-clock relation against the
         * delivery timeout the peer asked for -- not an inequality */
        CHECK(g_pump.dl_now == t0);
        CHECK(g_pump.sg_opened_at == t0);
        CHECK(g_pump.dl != UINT64_MAX);
        CHECK(g_pump.dl - g_pump.dl_now == FAR_DELIVERY_US);
        CHECK(g_pump.dl - g_pump.dl_now > CAP_STEP_US);
    }
    pthread_mutex_unlock(&g_pump.mu);
    pump_arm(PUMP_IDLE);
    if (!armed) {
        peer_down(&p);
        rig_down(&r);
        return;
    }

    /* let the lane settle so the next release is a bare wait cycle */
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    gate_release(0, entry);
    entry++;
    CHECK(gate_await(0, entry));

    /* the wait is bounded by the idle cap, not by that far deadline: at
     * exactly the cap the wake classifies as an idle-cap expiry, nothing else */
    CHECK(stats_of(r.lane[0], &a) == MOQ_OK);
    set_now(t0 + CAP_STEP_US);
    moq_msq_test_bell_ring_raw(r.lane[0]);
    gate_release(0, entry);
    entry++;
    CHECK(gate_await(0, entry));
    CHECK(stats_of(r.lane[0], &b) == MOQ_OK);
    CHECK(b.idle_cap_wakes == a.idle_cap_wakes + 1);
    CHECK(b.deadline_sweeps == a.deadline_sweeps);
    CHECK(b.wake_to_pump_samples == a.wake_to_pump_samples);
    CHECK(b.pump_sweeps == a.pump_sweeps);

    /* a callback-origin cross-lane wake to the SAME armed lane carries the
     * opposite signature: consumed as a wake, with no deadline sweep */
    CHECK(stats_of(r.lane[0], &c) == MOQ_OK);
    xwake_reset();
    pump_arm(PUMP_XISSUE);
    CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
    struct timespec abs;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_x.mu);
    while (!g_x.l0_wake_returned)
        if (pthread_cond_timedwait(&g_x.cv, &g_x.mu, &abs) != 0)
            break;
    bool issued = g_x.l0_wake_returned;
    moq_result_t issue_rc = g_x.l0_rc;

    pthread_mutex_unlock(&g_x.mu);
    CHECK(issued);
    CHECK(issue_rc == MOQ_OK);

    gate_release(0, entry);
    entry++;
    CHECK(gate_await(0, entry));
    CHECK(stats_of(r.lane[0], &d) == MOQ_OK);
    CHECK(d.wake_to_pump_samples == c.wake_to_pump_samples + 1);
    CHECK(d.wakes_cross_lane == c.wakes_cross_lane + 1);
    CHECK(d.pump_sweeps == c.pump_sweeps + 1);
    CHECK(d.deadline_sweeps == c.deadline_sweeps);
    CHECK(d.idle_cap_wakes == c.idle_cap_wakes);

    peer_down(&p);
    rig_down(&r);
    if (failures == before)
        printf("PASS: far subgroup deadline armed, idle cap and wake causes separated\n");
}

/* --- 8. on_activity confinement ------------------------------------------ */

static void t_on_activity_confined(void)
{
    int before = failures;
    struct rig r;
    const uint32_t plan[1] = { 0 };

    set_now(1600000);
    gate_prepare(1u << 0);
    pump_reset();
    chooser_reset(plan, 1);
    pthread_mutex_lock(&g_obs.mu);
    g_obs.armed = false;
    g_obs.runs = 0;
    g_obs.lane = NULL;
    g_obs.conn = NULL;
    g_obs.next_conn_null = false;
    g_obs.session_null = false;
    g_obs.wake_rc = MOQ_ERR_INTERNAL;
    g_obs.shutdowns_after_close = -1;
    pthread_mutex_unlock(&g_obs.mu);
    g_obs_fake = &g_conn_fake[0];

    if (!rig_up(&r, 1, 2, 0, choose_planned, NULL, on_activity_probe)) {
        rig_down(&r);
        return;
    }
    CHECK(gate_await(0, 1));

    /* the accept bumps activity itself; the observer stays disarmed across
     * it, so no listener/doorbell ordering can satisfy this row */
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_SUCCESS);
    pthread_mutex_lock(&g_obs.mu);
    CHECK(g_obs.runs == 0);
    pthread_mutex_unlock(&g_obs.mu);

    /* learn the exact child identity from its own lane pump */
    uint64_t entry = 1;

    pump_arm(PUMP_SURVEY);
    gate_release(0, entry);
    entry++;
    CHECK(pump_await_calls(0, 1));
    CHECK(gate_await(0, entry));
    moq_msquic_managed_conn_t *child;

    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.seen[0] == 1);
    child = g_pump.captured;
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(child != NULL);
    pump_arm(PUMP_IDLE);
    if (child == NULL) {
        rig_down(&r);
        return;
    }

    /* Arm against the known child. The doorbell is parked at its pre-wait, so
     * the only activity bump that can run now is the transport callback's own
     * guard leave -- mgd_tls_lane set, the pump window already closed. */
    pthread_mutex_lock(&g_obs.mu);
    g_obs.lane = r.lane[0];
    g_obs.conn = child;
    g_obs.armed = true;
    pthread_mutex_unlock(&g_obs.mu);

    moq_msquic_lane_stats_t s0, s1, s2, s3;

    CHECK(stats_of(r.lane[0], &s0) == MOQ_OK);
    CHECK(deliver_registered(&g_conn_fake[0],
                             QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE,
                             8) == QUIC_STATUS_SUCCESS);
    CHECK(stats_of(r.lane[0], &s1) == MOQ_OK);
    /* the observer's wake took the SAME-LANE arm, which is the only arm the
     * qualifying context can take: a bump moved past the lane context would
     * still return MOQ_OK, but as an external wake */
    CHECK(s1.wakes_same_lane == s0.wakes_same_lane + 1);
    CHECK(s1.wakes_external == s0.wakes_external);
    CHECK(s1.wakes_cross_lane == s0.wakes_cross_lane);

    pthread_mutex_lock(&g_obs.mu);
    CHECK(g_obs.runs == 1);
    CHECK(g_obs.next_conn_null);
    CHECK(g_obs.session_null);
    CHECK(g_obs.wake_rc == MOQ_OK);
    /* the close was refused: the transport saw no shutdown at all */
    CHECK(g_obs.shutdowns_after_close == 0);
    pthread_mutex_unlock(&g_obs.mu);
    CHECK(atomic_load(&g_conn_fake[0].conn_shutdowns) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* The same refusals from the doorbell's own post-pump bump, where a
     * sweep IS live -- there the lane really does hold iterable connections,
     * so the refusal can only come from the closed pump window. */
    pthread_mutex_lock(&g_obs.mu);
    g_obs.runs = 0;
    g_obs.next_conn_null = false;
    g_obs.session_null = false;
    g_obs.wake_rc = MOQ_ERR_INTERNAL;
    g_obs.shutdowns_after_close = -1;
    g_obs.armed = true;
    pthread_mutex_unlock(&g_obs.mu);
    /* the guard already armed this lane's pump, so releasing the gate is
     * enough -- no external wake is added that would pollute the cause deltas */
    CHECK(stats_of(r.lane[0], &s2) == MOQ_OK);
    gate_release(0, entry);
    entry++;
    CHECK(gate_await(0, entry));
    CHECK(stats_of(r.lane[0], &s3) == MOQ_OK);
    CHECK(s3.wakes_same_lane == s2.wakes_same_lane + 1);
    CHECK(s3.wakes_external == s2.wakes_external);
    CHECK(s3.wakes_cross_lane == s2.wakes_cross_lane);
    pthread_mutex_lock(&g_obs.mu);
    CHECK(g_obs.runs == 1);
    CHECK(g_obs.next_conn_null);
    CHECK(g_obs.session_null);
    CHECK(g_obs.wake_rc == MOQ_OK);
    CHECK(g_obs.shutdowns_after_close == 0);
    pthread_mutex_unlock(&g_obs.mu);
    CHECK(atomic_load(&g_conn_fake[0].conn_shutdowns) == 0);
    CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    /* the same call from inside the pump window is honoured exactly once */
    pthread_mutex_lock(&g_pump.mu);
    g_pump.close_target = child;
    pthread_mutex_unlock(&g_pump.mu);
    pump_arm(PUMP_CLOSE);
    /* the observer is disarmed now, so an ordinary external wake is the
     * simplest trigger and adds no cause the row above measures */
    CHECK(moq_msquic_lane_wake(r.lane[0]) == MOQ_OK);
    gate_release(0, entry);
    entry++;
    CHECK(gate_await(0, entry));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.closes_issued == 1);
    pthread_mutex_unlock(&g_pump.mu);
    CHECK(atomic_load(&g_conn_fake[0].conn_shutdowns) == 1);

    rig_down(&r);
    if (failures == before)
        printf("PASS: on_activity refuses lane, session and connection close\n");
}

/* --- 9. guard serialization: four contenders ----------------------------- */

struct t2_arg { fake_msq_t *fake; };
struct t3_arg { moq_msquic_managed_lane_t *lane; };

/* Published from inside the production guard, immediately before it takes the
 * lane mutex -- so "the contender reached the lock boundary" is an observed
 * production fact, not the worker's intent to get there. */
static void guard_pre_lock_hook(moq_msquic_managed_lane_t *lane)
{
    pthread_mutex_lock(&g_sched.mu);
    if (lane == g_sched.watch) {
        g_sched.t2_at_boundary = true;
        pthread_cond_broadcast(&g_sched.cv);
    }
    pthread_mutex_unlock(&g_sched.mu);
}

static void stats_pre_lock_hook(moq_msquic_managed_lane_t *lane)
{
    pthread_mutex_lock(&g_sched.mu);
    if (lane == g_sched.watch) {
        g_sched.t3_at_boundary = true;
        pthread_cond_broadcast(&g_sched.cv);
    }
    pthread_mutex_unlock(&g_sched.mu);
}

static void *transport_worker(void *p)
{
    struct t2_arg *a = p;

    QUIC_STATUS st = deliver_registered(
        a->fake, QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE, 8);

    pthread_mutex_lock(&g_sched.mu);
    g_sched.t2_status = st;
    g_sched.t2_done = true;
    pthread_cond_broadcast(&g_sched.cv);
    pthread_mutex_unlock(&g_sched.mu);
    return NULL;
}

static void *stats_worker(void *p)
{
    struct t3_arg *a = p;
    moq_msquic_lane_stats_t st;
    moq_result_t rc = stats_of(a->lane, &st);

    pthread_mutex_lock(&g_sched.mu);
    g_sched.t3_rc = rc;
    g_sched.t3_done = true;
    pthread_cond_broadcast(&g_sched.cv);
    pthread_mutex_unlock(&g_sched.mu);
    return NULL;
}

static bool sched_await(bool *flag)
{
    struct timespec abs;
    bool reached;

    guard_deadline(&abs);
    pthread_mutex_lock(&g_sched.mu);
    while (!*flag)
        if (pthread_cond_timedwait(&g_sched.cv, &g_sched.mu, &abs) != 0)
            break;
    reached = *flag;
    pthread_mutex_unlock(&g_sched.mu);
    return reached;
}

static void t_guard_serialization(void)
{
    int before = failures;
    struct rig r;
    const uint32_t plan[1] = { 0 };
    pthread_t t2, t3;
    struct t2_arg a2;
    struct t3_arg a3;

    set_now(1700000);
    gate_prepare(0);
    pump_reset();
    sched_reset();
    chooser_reset(plan, 1);
    if (!rig_up(&r, 2, 2, 0, choose_planned, NULL, NULL)) {
        rig_down(&r);
        return;
    }
    pump_arm(PUMP_HOLD_A);
    pthread_mutex_lock(&g_sched.mu);
    g_sched.watch = r.lane[0];
    pthread_mutex_unlock(&g_sched.mu);
    moq_msq_test_guard_pre_lock = guard_pre_lock_hook;
    moq_msq_test_stats_pre_lock = stats_pre_lock_hook;
    CHECK(rig_accept(&r, 0) == QUIC_STATUS_SUCCESS);

    /* contender 1: lane 0's doorbell parked inside its pump, lane 0 held */
    CHECK(sched_await(&g_sched.a_parked));

    /* contender 2: a real transport callback on the same lane, which must
     * block in the guard rather than produce a second concurrent pump */
    a2.fake = &g_conn_fake[0];
    int rc2 = pthread_create(&t2, NULL, transport_worker, &a2);

    CHECK(rc2 == 0);
    if (rc2 == 0)
        CHECK(sched_await(&g_sched.t2_at_boundary));

    /* contender 4: an application thread snapshotting the held lane */
    a3.lane = r.lane[0];
    int rc3 = pthread_create(&t3, NULL, stats_worker, &a3);

    CHECK(rc3 == 0);
    if (rc3 == 0)
        CHECK(sched_await(&g_sched.t3_at_boundary));

    /* contender 3: lane 1's own callback taking the cross-lane wake arm */
    CHECK(moq_msquic_lane_wake(r.lane[1]) == MOQ_OK);
    CHECK(sched_await(&g_sched.b_waked));

    sched_release_a();
    if (rc2 == 0)
        CHECK(pthread_join(t2, NULL) == 0);
    if (rc3 == 0)
        CHECK(pthread_join(t3, NULL) == 0);

    moq_msq_test_guard_pre_lock = NULL;
    moq_msq_test_stats_pre_lock = NULL;
    pthread_mutex_lock(&g_sched.mu);
    CHECK(!g_sched.a_expired);
    CHECK(!g_sched.depth2_while_held);
    CHECK(g_sched.t2_at_boundary);
    CHECK(g_sched.t3_at_boundary);
    CHECK(g_sched.t2_done);
    CHECK(g_sched.t2_status == QUIC_STATUS_SUCCESS);
    CHECK(g_sched.t3_done);
    CHECK(g_sched.t3_rc == MOQ_OK);
    CHECK(g_sched.b_rc == MOQ_OK);
    pthread_mutex_unlock(&g_sched.mu);

    /* the cross-lane wake was recorded, so lane 0 pumps again */
    CHECK(pump_await_calls(0, 2));
    pthread_mutex_lock(&g_pump.mu);
    CHECK(g_pump.max_depth[0] == 1);
    CHECK(g_pump.max_depth[1] == 1);
    pthread_mutex_unlock(&g_pump.mu);

    rig_down(&r);
    if (failures == before)
        printf("PASS: lane guard serializes pump, transport, wake and stats\n");
}

int main(void)
{
    moq_msq_test_now_us = test_now_us;
    moq_msq_test_prewait = prewait_hook;

    t_lane_accessors();
    t_partition();
    t_chooser_refusal();
    t_default_round_robin();
    t_terminal_before_reclamation();
    t_wake_selectivity();
    t_cross_lane_wake_returns_while_held();
    t_far_deadline_causes();
    t_on_activity_confined();
    t_guard_serialization();

    moq_msq_test_prewait = NULL;
    moq_msq_test_now_us = NULL;
    if (failures != 0) {
        fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    printf("all msquic lane tests passed\n");
    return 0;
}
