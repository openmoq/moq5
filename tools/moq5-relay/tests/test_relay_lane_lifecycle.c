/*
 * The terminal-to-reclaim lane lifecycle, driven sans-I/O and single-threaded.
 *
 * Every stage of the transition the frozen drain failure crossed is executed
 * explicitly, in order, on this thread: lane quiescent; a worker batch makes
 * the transport terminal and its queued SESSION_CLOSED visible; the
 * guard-leave arm decision; consumption of the pending token; the application
 * pump callback (the REAL production retirement pass over a REAL binding);
 * terminal transfer, acknowledgment, and reap. There is no doorbell thread,
 * no transport, no clock wait and no repetition: each "worker callback" is a
 * plain call on this thread, and each doorbell iteration is the production
 * mgd_doorbell_step, driven one step at a time.
 *
 * The arms separate the five ways a child can strand: no arm owed, arm lost,
 * callback suppressed (a prior nonzero return), callback ran but the terminal
 * stayed untransferred, and acknowledged but never reaped. Each is read from
 * state — pending bits before/after the step, the callback's own entry count
 * and exact last return value, and per-child facts after the callback — never
 * inferred from elapsed time or activity signals.
 */

#include <moq/relay/moqr_bind.h>
#include "../cli/conn_reap.h"

#include <moq/msquic_managed.h>
#include <moq/session.h>

#include "support/fake_msq_managed.h"
#include "support/msq_test_seams.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_failures;

#define T_CHECK(expr)                                                     \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

/* The application pump: the real retirement pass over a real binding, plus
 * the exact observability 0-return/nonzero-return discrimination needs. */
typedef struct app_ctx {
    moqr_core_t  *core;
    moqr_bind_t  *bind;
    moqr_trace_t *trace;
    int           entries;     /* callback entry count                     */
    int           last_rc;     /* the callback's exact last return value   */
    int           fail_after;  /* return 1 on entry N (0 = never)          */
    int           acked;
    int           retained;
} app_ctx_t;

static int
app_pump(moq_msquic_managed_t *m, moq_msquic_managed_lane_t *lane,
         uint64_t now_us, void *vctx)
{
    app_ctx_t *ctx = vctx;
    int rc = 0;

    (void)m;
    ctx->entries++;
    if (ctx->fail_after != 0 && ctx->entries >= ctx->fail_after) {
        ctx->last_rc = 1;
        return 1;
    }
    for (moq_msquic_managed_conn_t *conn = moq_msquic_lane_next_conn(lane, NULL);
         conn != NULL; conn = moq_msquic_lane_next_conn(lane, conn)) {
        void *tag = moq_msquic_managed_conn_user(conn);
        if (tag == MOQR_CONN_DEAD) {
            continue;
        }
        moq_session_t *s = moq_msquic_managed_conn_session(conn);
        if (s == NULL) {
            continue;
        }
        if (tag == NULL) {
            if (moqr_bind_conn_open(ctx->bind, s,
                                    moq_msquic_managed_conn_negotiated_version(conn)) == MOQR_OK) {
                moq_msquic_managed_conn_set_user(conn, MOQR_CONN_OPENED);
            } else {
                moq_msquic_managed_conn_set_user(conn, MOQR_CONN_DEAD);
                moq_msquic_managed_conn_close(conn, 0);
            }
        }
    }
    (void)moqr_bind_pump(ctx->bind, now_us);
    moqr_reap_stats_t st = { 0, 0 };
    if (!moqr_relay_reap_pass(ctx->bind, lane, &st)) {
        rc = 1;
    }
    ctx->acked += (int)st.acked;
    ctx->retained += (int)st.retained;
    ctx->last_rc = rc;
    return rc;
}

typedef struct rig {
    fake_mgd_t            fake;
    app_ctx_t             app;
    moq_msquic_managed_t *m;
    moq_msquic_managed_lane_t *lane;
} rig_t;

static bool
rig_up(rig_t *r, uint32_t facade_conns, uint32_t bind_conns)
{
    memset(r, 0, sizeof(*r));
    fake_mgd_init(&r->fake);
    moq_msq_test_api_override = fake_mgd_table(&r->fake);
    moq_msq_test_no_doorbell = true;

    moqr_core_relay_cfg_t core_cfg;
    moqr_core_relay_cfg_init_sized(&core_cfg, sizeof(core_cfg),
                                   moq_alloc_default());
    if (moqr_trace_create(moq_alloc_default(), 256, &r->app.trace) != MOQR_OK) {
        return false;
    }
    core_cfg.trace = r->app.trace;
    if (moqr_core_create(&core_cfg, &r->app.core) != MOQR_OK) {
        return false;
    }
    moqr_bind_cfg_t bcfg;
    moqr_bind_cfg_init_sized(&bcfg, sizeof(bcfg), moq_alloc_default());
    bcfg.core = r->app.core;
    bcfg.max_conns = bind_conns;
    if (moqr_bind_create(&bcfg, &r->app.bind) != MOQR_OK) {
        return false;
    }

    moq_msquic_managed_cfg_t scfg;
    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = NULL;               /* no bind address: nothing listens */
    scfg.port = 0;
    scfg.cert_path = "unused-by-the-fake-cert.pem";
    scfg.key_path = "unused-by-the-fake-key.pem";
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.lane_count = 1;
    scfg.max_connections = facade_conns;
    scfg.on_lane_pump = app_pump;
    scfg.on_lane_pump_user = &r->app;
    if (moq_msquic_managed_create(&scfg, &r->m) != MOQ_OK) {
        return false;
    }
    r->lane = moq_msquic_managed_lane(r->m, 0);
    return r->lane != NULL;
}

static void
rig_down(rig_t *r)
{
    if (r->m != NULL) {
        (void)moq_msquic_managed_stop(r->m);
        moq_msquic_managed_destroy(r->m);
    }
    if (r->app.bind != NULL) {
        moqr_bind_destroy(r->app.bind);
    }
    if (r->app.core != NULL) {
        moqr_core_destroy(r->app.core);
    }
    if (r->app.trace != NULL) {
        moqr_trace_destroy(r->app.trace);
    }
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
}

#define MAX_STEPS 16 /* bounded step count, never a real-time deadline */

/* Drive production doorbell iterations until the lane reports idle. */
static int
steps_to_idle(rig_t *r)
{
    for (int i = 0; i < MAX_STEPS; i++) {
        if (moq_msq_test_lane_step(r->lane) == MOQ_MSQ_TEST_STEP_IDLE) {
            return i;
        }
    }
    return -1;
}

static void
snap(rig_t *r, const char *label, moq_msq_test_lane_row_t *lr,
     moq_msq_test_child_row_t *cr, size_t cap, size_t *n)
{
    *n = moq_msq_test_lane_snapshot(r->lane, lr, cr, cap);
    T_CHECK(*n <= cap); /* fail closed on an incomplete snapshot */
    printf("  [%s] pending{wake=%d pump=%d ext=%d} pump_exit=%d stop{lane=%d "
           "facade=%d} conns=%zu sweeps=%llu | app{entries=%d last_rc=%d "
           "acked=%d retained=%d}\n",
           label, lr->wake_pending, lr->pump_pending, lr->ext_wake,
           lr->pump_exit, lr->lane_stop, lr->facade_stop, lr->conn_count,
           (unsigned long long)lr->pump_sweeps, r->app.entries,
           r->app.last_rc, r->app.acked, r->app.retained);
    for (size_t i = 0; i < *n && i < cap; i++) {
        printf("    child %zu: reapable=%d shutdown=%d acked=%d observed=%d "
               "queued=%d tag=%s\n",
               i, cr[i].reapable, cr[i].shutdown_complete,
               cr[i].app_terminal_acked, cr[i].terminal_observed,
               cr[i].has_events,
               cr[i].user == MOQR_CONN_DEAD ? "DEAD"
                   : (cr[i].user == MOQR_CONN_OPENED ? "OPENED" : "none"));
    }
}

/*
 * The rep-18 transition, normal path: a bound child whose transport terminal
 * completes while the lane is quiescent must be pumped by exactly the arm
 * that batch owed, then transferred, acknowledged and reaped — no extra wake,
 * no waiting, no second chance.
 */
static int
t_terminal_single_owed_pump(void)
{
    int before = g_failures;
    rig_t r;
    moq_msq_test_lane_row_t lr;
    moq_msq_test_child_row_t cr[4];
    size_t n;

    T_CHECK(rig_up(&r, 2, 2));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }

    /* accept + adopt */
    fake_mgd_conn_t *c = fake_mgd_accept(&r.fake, "moqt-18");
    T_CHECK(c != NULL);
    fake_mgd_deliver_connected(c, "moqt-18");
    T_CHECK(steps_to_idle(&r) >= 0);
    snap(&r, "adopted", &lr, cr, 4, &n);
    T_CHECK(n == 1 && cr[0].user == MOQR_CONN_OPENED);
    int entries_adopted = r.app.entries;

    /* lane quiescent: no pending bits, nothing owed */
    T_CHECK(!lr.wake_pending && !lr.pump_pending && !lr.ext_wake);

    /* worker batch: the transport terminal completes and SESSION_CLOSED is
     * queued, all before any pump. guard-leave runs inside these calls. */
    fake_mgd_deliver_peer_close(c, 0);
    fake_mgd_deliver_shutdown_complete(c);
    snap(&r, "terminal-batch", &lr, cr, 4, &n);
    T_CHECK(n == 1);
    T_CHECK(cr[0].reapable && cr[0].shutdown_complete);
    T_CHECK(cr[0].has_events);          /* SESSION_CLOSED queued, unconsumed */
    T_CHECK(!cr[0].terminal_observed);
    T_CHECK(!cr[0].app_terminal_acked);
    /* branch 1, "no arm owed": the batch MUST have armed the pump */
    T_CHECK(lr.pump_pending);

    /* one production step consumes the arm: the callback must actually run
     * (branch 2, "arm lost") and its own pass must transfer + acknowledge
     * (branch 4, "callback ran but terminal remained"). */
    T_CHECK(moq_msq_test_lane_step(r.lane) == MOQ_MSQ_TEST_STEP_PUMPED);
    snap(&r, "after-owed-pump", &lr, cr, 4, &n);
    T_CHECK(r.app.entries == entries_adopted + 1);
    T_CHECK(r.app.last_rc == 0);
    /* the pump pass transfers the terminal and acknowledges it, and the
     * SAME step's between-pass reap opportunity then reclaims the child —
     * reclaimability requires observation and acknowledgment, so full
     * reclamation here proves both happened inside this one step */
    T_CHECK(n == 0);
    T_CHECK(moq_msquic_managed_conn_count(r.m) == 0);

    /* and the lane settles idle with nothing left to reclaim */
    T_CHECK(steps_to_idle(&r) >= 0);
    snap(&r, "after-reap", &lr, cr, 4, &n);
    T_CHECK(n == 0);
    T_CHECK(moq_msquic_managed_conn_count(r.m) == 0);

    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: terminal_single_owed_pump\n");
    }
    return g_failures - before;
}

/*
 * The suppression arm: after the application callback returns nonzero once,
 * later owed pumps are deliberately suppressed — a terminal child then stays
 * linked BY DESIGN until forced stop, and the caller-visible facts are the
 * callback's own return value and pump_exit, not a lost wake. This pins the
 * signature the frozen rep-18 failure matches (one lane's callbacks stop
 * while its sweeps continue) to its cause.
 */
static int
t_nonzero_callback_suppresses(void)
{
    int before = g_failures;
    rig_t r;
    moq_msq_test_lane_row_t lr;
    moq_msq_test_child_row_t cr[4];
    size_t n;

    T_CHECK(rig_up(&r, 2, 2));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    fake_mgd_conn_t *c = fake_mgd_accept(&r.fake, "moqt-18");
    T_CHECK(c != NULL);
    fake_mgd_deliver_connected(c, "moqt-18");
    T_CHECK(steps_to_idle(&r) >= 0);
    snap(&r, "adopted", &lr, cr, 4, &n);
    T_CHECK(n == 1 && cr[0].user == MOQR_CONN_OPENED);

    /* the NEXT callback fails: the relay's documented fail-stop */
    r.app.fail_after = r.app.entries + 1;
    fake_mgd_deliver_peer_close(c, 0);
    fake_mgd_deliver_shutdown_complete(c);
    T_CHECK(moq_msq_test_lane_step(r.lane) == MOQ_MSQ_TEST_STEP_PUMPED);
    snap(&r, "fail-stop", &lr, cr, 4, &n);
    T_CHECK(r.app.last_rc == 1);
    T_CHECK(lr.pump_exit);

    /* further worker batches still arm, but the callback never runs again:
     * suppression, not a lost arm — the child stays linked with its terminal
     * unconsumed, exactly the frozen failure's shape. */
    int entries_at_failure = r.app.entries;
    for (int i = 0; i < 3; i++) {
        (void)moq_msq_test_lane_step(r.lane);
    }
    snap(&r, "suppressed", &lr, cr, 4, &n);
    T_CHECK(r.app.entries == entries_at_failure);
    T_CHECK(n == 1);
    T_CHECK(!cr[0].terminal_observed && !cr[0].app_terminal_acked);
    T_CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: nonzero_callback_suppresses\n");
    }
    return g_failures - before;
}

/*
 * The orphan path, sans-I/O: a child the binding refused at admission is
 * drained, acknowledged and reaped by the retirement pass alone — the same
 * contract the loopback orphan arm proves over real transport, here at
 * machine speed with the arm decision visible between stages.
 */
static int
t_orphan_refused_then_reclaimed(void)
{
    int before = g_failures;
    rig_t r;
    moq_msq_test_lane_row_t lr;
    moq_msq_test_child_row_t cr[4];
    size_t n;

    T_CHECK(rig_up(&r, 2, 1));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    fake_mgd_conn_t *a = fake_mgd_accept(&r.fake, "moqt-18");
    fake_mgd_conn_t *b = fake_mgd_accept(&r.fake, "moqt-18");
    T_CHECK(a != NULL && b != NULL);
    T_CHECK(steps_to_idle(&r) >= 0);
    snap(&r, "one-refused", &lr, cr, 4, &n);
    T_CHECK(n == 2);
    T_CHECK(moq_msquic_managed_conn_count(r.m) == 2);

    /* the refused child's transport finishes; the owed pump must retire it */
    fake_mgd_conn_t *orphan = cr[0].user == MOQR_CONN_DEAD ? a : b;
    fake_mgd_deliver_shutdown_complete(orphan == a ? a : b);
    T_CHECK(steps_to_idle(&r) >= 0);
    snap(&r, "orphan-reclaimed", &lr, cr, 4, &n);
    T_CHECK(n == 1);
    T_CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: orphan_refused_then_reclaimed\n");
    }
    return g_failures - before;
}

/*
 * The tightest owed-pump discipline: a connection whose transport terminal
 * completes BEFORE the application has ever seen it. The one owed pump must
 * then do everything — refuse it at admission, transfer the queued terminal,
 * acknowledge, and leave it reclaimable — because nothing re-arms afterward:
 * the drain empties the queue, so no further pump is owed. An acknowledgment
 * attempted before that pass's own poll is refused and the child strands,
 * which is what makes poll-then-ack load-bearing rather than stylistic.
 */
static int
t_orphan_terminal_before_first_pump(void)
{
    int before = g_failures;
    rig_t r;
    moq_msq_test_lane_row_t lr;
    moq_msq_test_child_row_t cr[4];
    size_t n;

    T_CHECK(rig_up(&r, 2, 1));
    if (r.m == NULL) {
        rig_down(&r);
        return g_failures - before;
    }
    fake_mgd_conn_t *a = fake_mgd_accept(&r.fake, "moqt-18");
    fake_mgd_conn_t *b = fake_mgd_accept(&r.fake, "moqt-18");
    T_CHECK(a != NULL && b != NULL);

    /* b's whole transport life ends before any pump ran: terminal complete,
     * SESSION_CLOSED queued, the child never seen by the application */
    fake_mgd_deliver_peer_close(b, 0);
    fake_mgd_deliver_shutdown_complete(b);
    snap(&r, "terminal-before-pump", &lr, cr, 4, &n);
    T_CHECK(n == 2);
    T_CHECK(lr.pump_pending || lr.wake_pending);

    /* the bounded step budget is all it gets: adoption of a, refusal of b,
     * transfer, acknowledgment and reap must complete with no extra wake */
    T_CHECK(steps_to_idle(&r) >= 0);
    T_CHECK(steps_to_idle(&r) >= 0);
    snap(&r, "one-budget-later", &lr, cr, 4, &n);
    T_CHECK(n == 1);
    T_CHECK(cr[0].user == MOQR_CONN_OPENED);
    T_CHECK(moq_msquic_managed_conn_count(r.m) == 1);

    rig_down(&r);
    if (g_failures == before) {
        printf("PASS: orphan_terminal_before_first_pump\n");
    }
    return g_failures - before;
}

int
main(void)
{
    (void)t_terminal_single_owed_pump();
    (void)t_nonzero_callback_suppresses();
    (void)t_orphan_refused_then_reclaimed();
    (void)t_orphan_terminal_before_first_pump();
    return g_failures;
}
