/*
 * The transport terminal must always reach the SESSION, exactly once.
 *
 * The adapter records a transport close and feeds it to the bridge, which
 * turns it into the session's one MOQ_EVENT_SESSION_CLOSED. A managed child is
 * only reclaimable once the application has OBSERVED and acknowledged that
 * event, so a terminal that never reaches the session strands the child and
 * its admission reserve for the life of the facade.
 *
 * The case that matters is a bridge that already latched a terminal OF ITS OWN
 * -- an endpoint op that failed hard while the bridge was dispatching. That
 * latch tells the session nothing, so the real transport terminal which
 * follows is the only terminal the session will ever be offered. Both bridge
 * terminal flavors are covered: an unclean transport completion and an
 * orderly peer application close. The bridge's first fatal cause must win in
 * both cases.
 *
 * Deterministic and socket-free: one thread, the production connection
 * callback and service entries over the fake QUIC_API_TABLE. No sleeps, no
 * elapsed time, no repetition; every ordering is forced by the call sequence.
 *
 * Usage: test_msquic_close_feed
 */

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "msquic_internal.h"

#include "support/fake_msq_table.h"

#include <moq/session.h>
#include <moq/transport_bridge.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* The code the app close carries in the stop control. */
#define LOCAL_CLOSE_CODE 0x7u
#define PEER_CLOSE_CODE 0x11u

/* --- rig ---------------------------------------------------------------- */

struct rig {
    fake_msq_t fake;
    moq_session_t *session;
    moq_msquic_conn_t *conn;
};

static int rig_up(struct rig *r, moq_msquic_hook_fn hook, void *hook_user)
{
    moq_session_cfg_t scfg;
    moq_msquic_conn_cfg_t cfg;

    memset(r, 0, sizeof(*r));
    fake_msq_init(&r->fake, true);
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    if (moq_session_create(&scfg, 0, &r->session) < 0)
        return -1;
    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.session = r->session;
    cfg.api = fake_msq_table(&r->fake);
    cfg.hook = hook;
    cfg.hook_user = hook_user;
    if (moq_msquic_conn_create(&cfg, &r->conn) != MOQ_OK)
        return -1;
    if (moq_msquic_conn_bind(r->conn, fake_msq_conn_handle(&r->fake)) !=
        MOQ_OK)
        return -1;
    return 0;
}

static void rig_down(struct rig *r)
{
    if (r->conn != NULL)
        moq_msquic_conn_destroy(r->conn);
    if (r->session != NULL)
        moq_session_destroy(r->session);
}

static void deliver_conn_event(struct rig *r, QUIC_CONNECTION_EVENT_TYPE t)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = t;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn, &ev);
}

/* Drain the session's events; report how many terminals came out and the code
 * the last one carried. */
struct drained {
    int total;
    int closed;
    uint64_t close_code;
};

static struct drained drain_events(struct rig *r)
{
    struct drained d;
    moq_event_t ev;

    memset(&d, 0, sizeof(d));
    while (moq_session_poll_events(r->session, &ev, 1) > 0) {
        d.total++;
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            d.closed++;
            d.close_code = ev.u.closed.code;
        }
        moq_event_cleanup(&ev);
    }
    return d;
}

static void print_state(const char *tag, struct rig *r)
{
    bool obs = false;
    bool enq = moq_transport_bridge_terminal_facts(r->conn->bridge, &obs);

    printf("  %-22s fatal=%d closed=%d close_pending=%d close_fed=%d "
           "shutdown_complete=%d sess=%d enqueued=%d observed=%d "
           "shutdowns=%d feeds=%u\n",
           tag, (int)moq_msquic_conn_is_fatal(r->conn),
           (int)moq_transport_bridge_is_closed(r->conn->bridge),
           (int)r->conn->close_pending, (int)r->conn->close_fed,
           (int)r->conn->shutdown_complete,
           (int)moq_session_state(r->session), (int)enq, (int)obs,
           r->fake.conn_shutdowns, r->conn->test_close_feed_commits);
}

/* The whole point, asserted the same way everywhere: after the transport
 * terminal the session has been told exactly once, and the adapter fed it
 * exactly once. */
static void check_told_once(struct rig *r, struct drained d,
                            uint64_t expect_code)
{
    bool obs = false;
    bool enq = moq_transport_bridge_terminal_facts(r->conn->bridge, &obs);

    CHECK(r->conn->close_fed);
    CHECK(r->conn->test_close_feed_commits == 1);
    CHECK(enq);                          /* the session really queued it */
    CHECK(obs);                          /* ... and the drain transferred it */
    CHECK(d.total == 1);
    CHECK(d.closed == 1);
    CHECK(d.close_code == expect_code);
    CHECK(moq_session_state(r->session) == MOQ_SESS_CLOSED);
}

/* No further terminal can appear, however hard the connection is serviced. */
static void check_no_second_terminal(struct rig *r)
{
    struct drained again;

    moq_msquic_conn_service(r->conn);
    deliver_conn_event(r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    moq_msquic_conn_service(r->conn);
    again = drain_events(r);
    CHECK(again.total == 0);
    CHECK(again.closed == 0);
    CHECK(r->conn->test_close_feed_commits == 1);
}

/* --- 1. the defect: an internal fatal swallows the transport terminal ----- */

/*
 * The bridge latches its own fatal when an endpoint op fails hard mid-dispatch
 * -- here a StreamStart the transport refuses, exactly as one racing a closing
 * connection does. That latch is not a session terminal: the session is still
 * open with nothing queued. The transport terminal that follows is its last
 * notice and must be delivered.
 */
static void arm_internal_fatal(struct rig *r)
{
    /* the client's SETUP wants its control bidi; the transport refuses it */
    r->fake.stream_start_fails = 1;
    deliver_conn_event(r, QUIC_CONNECTION_EVENT_CONNECTED);
    print_state("after-connected", r);

    /* The bridge owns a first fatal cause, but the session still has no
     * terminal. This is the exact precondition the later transport close must
     * repair rather than treating bridge-fatal as session-closed. */
    bool obs0 = false;
    bool enq0 = moq_transport_bridge_terminal_facts(r->conn->bridge, &obs0);
    struct drained d0 = drain_events(r);

    CHECK(moq_msquic_conn_is_fatal(r->conn));
    CHECK(moq_transport_bridge_fatal_code(r->conn->bridge) == 0x1);
    CHECK(!enq0);
    CHECK(!obs0);
    CHECK(d0.total == 0);
    CHECK(moq_session_state(r->session) != MOQ_SESS_CLOSED);
    CHECK(!r->conn->close_pending);
    CHECK(!r->conn->close_fed);
    CHECK(r->conn->test_close_feed_commits == 0);
    CHECK(r->fake.conn_shutdowns == 1);
}

static void t_internal_fatal_then_transport_error(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("INTERNAL-FATAL-THEN-ERROR:\n");
    arm_internal_fatal(&r);

    /* A bare SHUTDOWN_COMPLETE is unclean and arrives after the callback that
     * caused ConnectionShutdown. MsQuic callbacks are serialized, never
     * recursive. */
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    print_state("after-terminal", &r);

    struct drained d = drain_events(&r);

    printf("  events: closed=%d total=%d code=%llu\n", d.closed, d.total,
           (unsigned long long)d.close_code);
    check_told_once(&r, d, 0x1);
    CHECK(moq_msquic_conn_is_fatal(r.conn));
    CHECK(!moq_transport_bridge_is_closed(r.conn->bridge));
    CHECK(moq_transport_bridge_fatal_code(r.conn->bridge) == 0x1);
    check_no_second_terminal(&r);

    rig_down(&r);
    if (failures == before)
        printf("PASS: internal_fatal_then_transport_error\n");
}

static void t_internal_fatal_then_clean_close(void)
{
    int before = failures;
    struct rig r;
    QUIC_CONNECTION_EVENT ev;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("INTERNAL-FATAL-THEN-CLEAN-CLOSE:\n");
    arm_internal_fatal(&r);

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER;
    ev.SHUTDOWN_INITIATED_BY_PEER.ErrorCode = PEER_CLOSE_CODE;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r.fake), r.conn, &ev);
    print_state("after-peer-close", &r);

    struct drained d = drain_events(&r);

    /* The bridge was already fatal. The clean transport indication supplies
     * the missing session terminal but cannot replace the first cause or flip
     * the bridge to clean-closed. */
    check_told_once(&r, d, 0x1);
    CHECK(moq_msquic_conn_is_fatal(r.conn));
    CHECK(!moq_transport_bridge_is_closed(r.conn->bridge));
    CHECK(moq_transport_bridge_fatal_code(r.conn->bridge) == 0x1);

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    check_no_second_terminal(&r);

    rig_down(&r);
    if (failures == before)
        printf("PASS: internal_fatal_then_clean_close\n");
}

/* --- 2. control: terminal BEFORE any service pass ------------------------ */

static void t_terminal_before_service(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("BEFORE-SERVICE:\n");

    /* nothing has serviced this connection yet */
    CHECK(!r.conn->close_pending);
    CHECK(!moq_msquic_conn_is_fatal(r.conn));

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    print_state("after-terminal", &r);

    struct drained d = drain_events(&r);

    check_told_once(&r, d, 0);
    /* flavor: an unsolicited completion is an unclean death, so the adapter
     * feeds it as a transport ERROR and the bridge latches fatal, not closed */
    CHECK(moq_msquic_conn_is_fatal(r.conn));
    CHECK(!moq_transport_bridge_is_closed(r.conn->bridge));
    check_no_second_terminal(&r);

    rig_down(&r);
    if (failures == before)
        printf("PASS: terminal_before_service\n");
}

/* --- 2b. control: a CLEAN peer close, the other flavor ------------------- */

static void t_clean_peer_close(void)
{
    int before = failures;
    struct rig r;
    QUIC_CONNECTION_EVENT ev;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("CLEAN-PEER-CLOSE:\n");

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(&r);

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER;
    ev.SHUTDOWN_INITIATED_BY_PEER.ErrorCode = PEER_CLOSE_CODE;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r.fake), r.conn, &ev);
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    print_state("after-peer-close", &r);

    struct drained d = drain_events(&r);

    /* flavor and code: the peer's orderly application close, carried through */
    check_told_once(&r, d, PEER_CLOSE_CODE);
    CHECK(moq_transport_bridge_is_closed(r.conn->bridge));
    CHECK(!moq_msquic_conn_is_fatal(r.conn));
    check_no_second_terminal(&r);

    rig_down(&r);
    if (failures == before)
        printf("PASS: clean_peer_close\n");
}

/* --- 3. control: terminal DURING an active service pass ------------------ */

/* Synthetic guard control: inject a callback while the application hook has a
 * service pass on the stack. Production MsQuic connection callbacks are
 * serialized and non-recursive; this pins the generic re-entrancy defense and
 * is not evidence for the already-fatal defect above. */
static int g_reentrant_hook_calls;
static bool g_reentrant_saw_in_service;
static bool g_reentrant_saw_unfed;

static void reentrant_hook(moq_msquic_conn_t *conn, void *user)
{
    struct rig *r = user;

    (void)conn;
    if (g_reentrant_hook_calls++ != 0)
        return;                  /* exactly one terminal, from the first pass */
    g_reentrant_saw_in_service = r->conn->in_service;
    deliver_conn_event(r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    /* the re-entrant service could not feed it: that is the active pass's job */
    g_reentrant_saw_unfed = r->conn->close_pending && !r->conn->close_fed;
}

static void t_terminal_during_service(void)
{
    int before = failures;
    struct rig r;

    g_reentrant_hook_calls = 0;
    g_reentrant_saw_in_service = false;
    g_reentrant_saw_unfed = false;
    CHECK(rig_up(&r, reentrant_hook, &r) == 0);
    printf("DURING-SERVICE:\n");

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_CONNECTED);
    print_state("after-connected", &r);

    struct drained d = drain_events(&r);

    CHECK(g_reentrant_hook_calls >= 1);
    CHECK(g_reentrant_saw_in_service);   /* the terminal really was in-pass */
    CHECK(g_reentrant_saw_unfed);        /* and the re-entrant call deferred */
    CHECK(!r.conn->in_service);          /* the pass has exited */
    check_told_once(&r, d, 0);
    check_no_second_terminal(&r);

    rig_down(&r);
    if (failures == before)
        printf("PASS: terminal_during_service\n");
}

/* --- 4. control: terminal AFTER a completed service pass ----------------- */

static void t_terminal_after_service(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("AFTER-SERVICE:\n");

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_CONNECTED);
    moq_msquic_conn_service(r.conn);
    /* a healthy connection: the control bidi opened and nothing is terminal */
    CHECK(!moq_msquic_conn_is_fatal(r.conn));
    CHECK(!r.conn->in_service);
    CHECK(fake_msq_stream_at(&r.fake, 0) != NULL);
    struct drained d0 = drain_events(&r);

    CHECK(d0.closed == 0);

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    print_state("after-terminal", &r);

    struct drained d = drain_events(&r);

    check_told_once(&r, d, 0);
    check_no_second_terminal(&r);

    rig_down(&r);
    if (failures == before)
        printf("PASS: terminal_after_service\n");
}

/* --- 5. control: duplicate completion ------------------------------------ */

static void t_duplicate_completion(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("DUPLICATE:\n");

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(&r);
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    print_state("after-3-terminals", &r);

    struct drained d = drain_events(&r);

    check_told_once(&r, d, 0);
    check_no_second_terminal(&r);

    rig_down(&r);
    if (failures == before)
        printf("PASS: duplicate_completion\n");
}

/* --- 6. control: the application stops the session ----------------------- */

/*
 * A local close is the session's own terminal: it queues SESSION_CLOSED and a
 * CLOSE_SESSION action the bridge turns into a transport shutdown. The
 * transport terminal that follows must not produce a second one.
 */
static void t_local_stop_then_terminal(void)
{
    int before = failures;
    struct rig r;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("LOCAL-STOP:\n");

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(&r);

    CHECK(moq_session_close(r.session, LOCAL_CLOSE_CODE, NULL, 0) == MOQ_OK);
    moq_msquic_conn_service(r.conn);
    print_state("after-local-close", &r);

    struct drained d = drain_events(&r);

    /* the session's own terminal, carrying the application's code */
    CHECK(d.total == 1);
    CHECK(d.closed == 1);
    CHECK(d.close_code == LOCAL_CLOSE_CODE);
    CHECK(moq_session_state(r.session) == MOQ_SESS_CLOSED);
    CHECK(r.fake.conn_shutdowns >= 1);

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    print_state("after-terminal", &r);

    struct drained d2 = drain_events(&r);

    CHECK(d2.total == 0);               /* exactly one terminal, total */
    CHECK(d2.closed == 0);
    CHECK(r.conn->close_fed);
    check_no_second_terminal(&r);

    rig_down(&r);
    if (failures == before)
        printf("PASS: local_stop_then_terminal\n");
}

/* ======================================================================== *
 * Local-close terminal classification
 *
 * A locally initiated close is a distinct transport terminal cause, and the
 * adapter must record it as such. The four causes and what each must retain:
 *
 *   local close        clean,   the exact code the application closed with
 *   peer app close     clean,   the peer's code
 *   transport failure  unclean, the transport's code
 *   bare completion    unclean, code 0 -- the fallback, used ONLY when no
 *                      initiating fact was recorded
 *
 * Code alone does not separate them: a local close, a transport failure and
 * the fallback can all carry code 0, so the cleanliness classification is the
 * only discriminator between the local cause and the two unclean ones. All
 * three code-0 shapes are exercised independently below.
 *
 * The contract these cases pin, derived from the connection's existing
 * serialization (one lane, callbacks serialized, conn_service re-entrancy
 * guarded):
 *
 *   1. the first non-fallback initiating fact recorded under the connection's
 *      serialization wins;
 *   2. a local close publishes pending + clean + its exact code BEFORE calling
 *      ConnectionShutdown, so nothing observed after that call -- not even a
 *      callback delivered synchronously from inside it -- can find the state
 *      unclassified or displace it;
 *   3. peer application close is clean and retains the peer code;
 *   4. transport failure is unclean and retains the transport code;
 *   5. SHUTDOWN_COMPLETE synthesizes the unclean/code-0 fallback only when no
 *      prior initiating fact exists;
 *   6. the winning fact is fed to the bridge once; later callbacks can neither
 *      feed again nor replace it.
 *
 * Rule 2 is what makes the classification observable at all: ep_close_transport
 * returns after an asynchronous ConnectionShutdown, so the only moment at which
 * "the local cause was recorded before the transport was told" can be checked
 * is from inside that call. The synchronous injections below are that
 * discriminator -- a test instrument, not a claim that production MsQuic
 * delivers connection callbacks re-entrantly.
 *
 * What a local close does NOT change, and these cases assert it so the claim
 * stays honest: dispatching the session's CLOSE_SESSION action already marks
 * the BRIDGE closed with the local code, so the adapter's later terminal feed
 * -- right or wrong -- is idempotently ignored and the session terminal the
 * application sees is unaffected either way. The defect under test is the
 * adapter retaining and attempting the WRONG terminal fact, which is what a
 * managed facade, a diagnostic, or any second consumer of the adapter's
 * classification would read.
 * ======================================================================== */

#define TRANSPORT_CLOSE_CODE 0x2bu
#define LOCAL_CLOSE_CODE_ZERO 0x0u

/* A declared test time, independent of any payload length. moq_session_close()
 * takes (s, code, reason, now_us) and derives the reason length itself; the
 * fourth argument is NOT a length. */
#define TEST_NOW_US 4242u

/* The reason phrase, declared once so its expected length is derived from the
 * string rather than written twice. */
static const char k_local_reason[] = "malformed local reason";
#define LOCAL_REASON_LEN (sizeof(k_local_reason) - 1)

/* Named so a failure inventory is stable across runs and attributable to one
 * property rather than to a line number. */
#define CHECKN(name, expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL[%s]: %s:%d: %s\n", (name), __FILE__, __LINE__, \
                #expr); \
        failures++; \
    } \
} while (0)

static void deliver_peer_close(struct rig *r, uint64_t code)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER;
    ev.SHUTDOWN_INITIATED_BY_PEER.ErrorCode = code;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn, &ev);
}

static void deliver_transport_error(struct rig *r, uint64_t code)
{
    QUIC_CONNECTION_EVENT ev;

    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT;
    ev.SHUTDOWN_INITIATED_BY_TRANSPORT.ErrorCode = code;
    moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn, &ev);
}

/* The classification the adapter retains, as one comparable value. */
struct cause {
    bool pending;
    bool clean;
    uint64_t code;
};

static struct cause cause_of(struct rig *r)
{
    struct cause c;

    c.pending = r->conn->close_pending;
    c.clean = r->conn->close_clean;
    c.code = r->conn->close_code;
    return c;
}

static void print_cause(const char *tag, struct cause c)
{
    printf("  %-22s pending=%d clean=%d code=%llu\n", tag, (int)c.pending,
           (int)c.clean, (unsigned long long)c.code);
}

/* Field-specific diagnostic naming: "<tag>.<field>", so the sorted inventory
 * identifies exactly which property failed. */
static void checkn_field(const char *tag, const char *field, bool ok)
{
    char name[96];

    if (ok)
        return;
    snprintf(name, sizeof(name), "%s.%s", tag, field);
    fprintf(stderr, "FAIL[%s]\n", name);
    failures++;
}

/*
 * Observes the adapter's own state at the instant the transport is told to
 * shut down, and optionally delivers connection callbacks synchronously from
 * inside that call.
 */
struct shutdown_probe {
    struct rig *rig;
    int calls;
    struct cause at_call;       /* state on the FIRST ConnectionShutdown */
    uint64_t code_at_call;      /* the code that call carried */
    bool inject_peer;
    bool inject_transport;
    bool inject_complete;
};

static void shutdown_probe_cb(fake_msq_t *f, void *ctx)
{
    struct shutdown_probe *p = ctx;

    if (p->calls++ == 0) {
        p->at_call = cause_of(p->rig);
        p->code_at_call = f->last_conn_shutdown_code;
    }
    if (p->inject_peer)
        deliver_peer_close(p->rig, PEER_CLOSE_CODE);
    if (p->inject_transport)
        deliver_transport_error(p->rig, TRANSPORT_CLOSE_CODE);
    if (p->inject_complete)
        deliver_conn_event(p->rig, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
}

/* Establish, then close locally with `code` and no reason, servicing the
 * connection so the bridge really dispatches the session's CLOSE_SESSION
 * action through ep_close_transport. Returns the session's own terminal
 * drain. */
static struct drained local_close(struct rig *r, uint64_t code)
{
    deliver_conn_event(r, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(r);
    CHECK(moq_session_close(r->session, code, NULL, TEST_NOW_US) == MOQ_OK);
    moq_msquic_conn_service(r->conn);
    return drain_events(r);
}

/*
 * Every local-origin case ends the same way: the completion arrives, the
 * connection is serviced again, and neither the classification nor the feed
 * count moves. The bridge checks are the honest composition statement -- the
 * session terminal already committed with the local code when the
 * CLOSE_SESSION action was dispatched, and no later adapter feed may disturb
 * it. They pass today and must keep passing after the fix.
 */
static void check_local_settles(struct rig *r, const char *tag,
                                uint64_t expect_code)
{
    struct drained after;
    struct cause c;

    deliver_conn_event(r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    moq_msquic_conn_service(r->conn);
    deliver_conn_event(r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    moq_msquic_conn_service(r->conn);
    after = drain_events(r);
    c = cause_of(r);
    print_cause("settled", c);

    checkn_field(tag, "pending", c.pending);
    checkn_field(tag, "clean", c.clean);
    checkn_field(tag, "code", c.code == expect_code);
    checkn_field(tag, "fed", r->conn->close_fed);
    checkn_field(tag, "one_feed", r->conn->test_close_feed_commits == 1);
    checkn_field(tag, "no_events", after.total == 0);
    checkn_field(tag, "no_closed", after.closed == 0);
    checkn_field(tag, "bridge_closed",
                 moq_transport_bridge_is_closed(r->conn->bridge));
    checkn_field(tag, "bridge_code",
                 moq_transport_bridge_close_code(r->conn->bridge) ==
                     expect_code);
    checkn_field(tag, "bridge_not_fatal", !moq_msquic_conn_is_fatal(r->conn));
}

/* --- L1: the local cause is published before the transport is told ------- */

static void t_local_close_publishes_before_shutdown(void)
{
    int before = failures;
    struct rig r;
    struct shutdown_probe probe;
    struct cause after_service;
    struct drained d;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    memset(&probe, 0, sizeof(probe));
    probe.rig = &r;
    r.fake.on_conn_shutdown = shutdown_probe_cb;
    r.fake.on_conn_shutdown_ctx = &probe;
    printf("LOCAL-CLOSE-PUBLISH-ORDER:\n");

    d = local_close(&r, LOCAL_CLOSE_CODE);
    after_service = cause_of(&r);
    print_cause("at-shutdown-call", probe.at_call);
    print_cause("after-service", after_service);

    /* the session's own terminal, which is not in question here */
    CHECK(d.closed == 1);
    CHECK(d.close_code == LOCAL_CLOSE_CODE);
    CHECK(probe.calls == 1);

    /* the transport really was told, with the application's code */
    CHECKN("local.shutdown_code", probe.code_at_call == LOCAL_CLOSE_CODE);

    /* rule 2: recorded BEFORE the call, so nothing after it can find the
     * state unclassified */
    CHECKN("local.published_before_shutdown", probe.at_call.pending);
    CHECKN("local.clean_before_shutdown", probe.at_call.clean);
    CHECKN("local.code_before_shutdown",
           probe.at_call.code == LOCAL_CLOSE_CODE);

    /* and still retained once the service pass that issued it returns */
    CHECKN("local.pending_after_service", after_service.pending);
    CHECKN("local.clean_after_service", after_service.clean);
    CHECKN("local.code_after_service",
           after_service.code == LOCAL_CLOSE_CODE);

    check_local_settles(&r, "local.settled", LOCAL_CLOSE_CODE);

    rig_down(&r);
    if (failures == before)
        printf("PASS: local_close_publishes_before_shutdown\n");
}

/* --- L2: three independent code-0 causes -------------------------------- */

/*
 * All three carry code 0, so only the cleanliness classification separates the
 * local cause from the two unclean ones. Each is produced by its own
 * production path on its own rig -- a local close, a real
 * SHUTDOWN_INITIATED_BY_TRANSPORT carrying 0, and a bare SHUTDOWN_COMPLETE --
 * and the two distinctions are asserted directly rather than inferred.
 */
static void t_local_close_code_zero_vs_unclean_zero(void)
{
    int before = failures;
    struct rig local;
    struct rig transport;
    struct rig fallback;
    struct cause lc;
    struct cause tc;
    struct cause fc;
    struct drained ld;
    struct drained td;
    struct drained fd;

    printf("CODE-ZERO-THREE-WAY:\n");

    /* control: a real transport failure whose code happens to be 0 */
    CHECK(rig_up(&transport, NULL, NULL) == 0);
    deliver_conn_event(&transport, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(&transport);
    deliver_transport_error(&transport, 0);
    deliver_conn_event(&transport, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    td = drain_events(&transport);
    tc = cause_of(&transport);
    print_cause("transport-code-0", tc);
    CHECKN("transport0.pending", tc.pending);
    CHECKN("transport0.unclean", !tc.clean);
    CHECKN("transport0.code_zero", tc.code == 0);
    CHECKN("transport0.one_event", td.total == 1 && td.closed == 1);
    CHECKN("transport0.event_code", td.close_code == 0);
    CHECKN("transport0.bridge_fatal",
           moq_msquic_conn_is_fatal(transport.conn));
    CHECKN("transport0.bridge_fatal_code",
           moq_transport_bridge_fatal_code(transport.conn->bridge) == 0);
    CHECKN("transport0.bridge_not_closed",
           !moq_transport_bridge_is_closed(transport.conn->bridge));
    CHECKN("transport0.one_feed",
           transport.conn->test_close_feed_commits == 1);

    /* control: the fallback, produced by a bare completion */
    CHECK(rig_up(&fallback, NULL, NULL) == 0);
    deliver_conn_event(&fallback, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(&fallback);
    deliver_conn_event(&fallback, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    fd = drain_events(&fallback);
    fc = cause_of(&fallback);
    print_cause("bare-completion", fc);
    CHECKN("fallback.pending", fc.pending);
    CHECKN("fallback.unclean", !fc.clean);
    CHECKN("fallback.code_zero", fc.code == 0);
    CHECKN("fallback.one_event", fd.total == 1 && fd.closed == 1);
    CHECKN("fallback.event_code", fd.close_code == 0);
    CHECKN("fallback.bridge_fatal", moq_msquic_conn_is_fatal(fallback.conn));
    CHECKN("fallback.bridge_fatal_code",
           moq_transport_bridge_fatal_code(fallback.conn->bridge) == 0);
    CHECKN("fallback.bridge_not_closed",
           !moq_transport_bridge_is_closed(fallback.conn->bridge));
    CHECKN("fallback.one_feed", fallback.conn->test_close_feed_commits == 1);

    /* the subject: a local close carrying code 0 */
    CHECK(rig_up(&local, NULL, NULL) == 0);
    ld = local_close(&local, LOCAL_CLOSE_CODE_ZERO);
    lc = cause_of(&local);
    print_cause("local-code-0", lc);

    CHECK(ld.closed == 1);
    CHECK(ld.close_code == LOCAL_CLOSE_CODE_ZERO);

    CHECKN("local0.pending", lc.pending);
    CHECKN("local0.clean", lc.clean);
    CHECKN("local0.code_zero", lc.code == LOCAL_CLOSE_CODE_ZERO);
    /* the two discriminations themselves */
    CHECKN("local0.distinct_from_transport", lc.clean != tc.clean);
    CHECKN("local0.distinct_from_fallback", lc.clean != fc.clean);

    check_local_settles(&local, "local0.settled", LOCAL_CLOSE_CODE_ZERO);

    rig_down(&local);
    rig_down(&fallback);
    rig_down(&transport);
    if (failures == before)
        printf("PASS: local_close_code_zero_vs_unclean_zero\n");
}

/* --- L3/L4/L5/L6: the recorded local fact wins over injected callbacks --- */

static void local_close_wins_injection(const char *label, const char *tag,
                                       bool peer, bool transport,
                                       bool complete)
{
    int before = failures;
    struct rig r;
    struct shutdown_probe probe;
    struct cause after_service;
    struct drained d;
    char what[96];

    CHECK(rig_up(&r, NULL, NULL) == 0);
    memset(&probe, 0, sizeof(probe));
    probe.rig = &r;
    probe.inject_peer = peer;
    probe.inject_transport = transport;
    probe.inject_complete = complete;
    r.fake.on_conn_shutdown = shutdown_probe_cb;
    r.fake.on_conn_shutdown_ctx = &probe;
    printf("%s:\n", label);

    d = local_close(&r, LOCAL_CLOSE_CODE);
    after_service = cause_of(&r);
    print_cause("at-shutdown-call", probe.at_call);
    print_cause("after-service", after_service);

    CHECK(d.closed == 1);
    CHECK(d.close_code == LOCAL_CLOSE_CODE);
    CHECK(probe.calls == 1);

    checkn_field(tag, "clean", after_service.clean);
    checkn_field(tag, "code", after_service.code == LOCAL_CLOSE_CODE);
    checkn_field(tag, "pending", after_service.pending);

    snprintf(what, sizeof(what), "%s.settled", tag);
    check_local_settles(&r, what, LOCAL_CLOSE_CODE);

    rig_down(&r);
    if (failures == before)
        printf("PASS: %s\n", label);
}

static void t_local_close_wins_injected_peer(void)
{
    local_close_wins_injection("LOCAL-VS-INJECTED-PEER", "local_vs_peer",
                               true, false, false);
}

static void t_local_close_wins_injected_transport(void)
{
    local_close_wins_injection("LOCAL-VS-INJECTED-TRANSPORT",
                               "local_vs_transport", false, true, false);
}

static void t_local_close_wins_injected_completion(void)
{
    local_close_wins_injection("LOCAL-VS-INJECTED-COMPLETION",
                               "local_vs_complete", false, false, true);
}

static void t_local_close_wins_injected_all(void)
{
    local_close_wins_injection("LOCAL-VS-INJECTED-ALL", "local_vs_all",
                               true, true, true);
}

/* --- L7: the borrowed reason is not retained ---------------------------- */

struct reason_probe {
    struct shutdown_probe shutdown;
    int closed_events;
    int matched_events;
};

/*
 * moq_session_close() BORROWS the reason (session.h:479-482): the caller must
 * keep it valid until the CLOSE_SESSION action and the SESSION_CLOSED event
 * have both been polled, and no longer.
 *
 * The buffer lives in THIS frame. Both documented consumers run here -- the
 * service pass that dispatches the action, then the event drain whose reason
 * span is compared against the still-valid bytes -- and the function returns
 * only once both are done. The borrow's lifetime therefore genuinely ends when
 * this frame is torn down, so everything the caller does afterwards happens
 * after the storage is gone. Under a sanitizer build with stack-use-after-
 * return detection this makes a later read through a retained pointer a
 * reported error rather than a silent one.
 */
static void issue_local_close_with_reason(struct rig *r,
                                          struct reason_probe *out)
{
    char reason[sizeof(k_local_reason)];
    moq_event_t ev;

    memcpy(reason, k_local_reason, sizeof(k_local_reason));

    deliver_conn_event(r, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(r);

    CHECK(moq_session_close(r->session, LOCAL_CLOSE_CODE, reason,
                            TEST_NOW_US) == MOQ_OK);

    /* consumer 1: the CLOSE_SESSION action, dispatched through the bridge */
    moq_msquic_conn_service(r->conn);

    /* consumer 2: the SESSION_CLOSED event, compared while still borrowed */
    while (moq_session_poll_events(r->session, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            out->closed_events++;
            if (ev.u.closed.code == LOCAL_CLOSE_CODE &&
                ev.u.closed.reason.len == LOCAL_REASON_LEN &&
                ev.u.closed.reason.data != NULL &&
                memcmp(ev.u.closed.reason.data, reason,
                       LOCAL_REASON_LEN) == 0)
                out->matched_events++;
        }
        moq_event_cleanup(&ev);
    }
    /* both consumers are done: the borrow ends with this frame */
}

static void t_local_close_reason_not_retained(void)
{
    int before = failures;
    struct rig r;
    struct reason_probe probe;
    struct cause after_service;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    memset(&probe, 0, sizeof(probe));
    probe.shutdown.rig = &r;
    r.fake.on_conn_shutdown = shutdown_probe_cb;
    r.fake.on_conn_shutdown_ctx = &probe.shutdown;
    printf("LOCAL-CLOSE-REASON-BORROW:\n");

    issue_local_close_with_reason(&r, &probe);

    after_service = cause_of(&r);
    print_cause("after-service", after_service);

    CHECKN("reason.session_terminal", probe.closed_events == 1);
    CHECKN("reason.session_bytes", probe.matched_events == 1);

    CHECKN("reason.published_before_shutdown", probe.shutdown.at_call.pending);
    CHECKN("reason.clean_before_shutdown", probe.shutdown.at_call.clean);
    CHECKN("reason.code_before_shutdown",
           probe.shutdown.at_call.code == LOCAL_CLOSE_CODE);
    CHECKN("reason.pending_after_service", after_service.pending);
    CHECKN("reason.clean_after_service", after_service.clean);
    CHECKN("reason.code_after_service",
           after_service.code == LOCAL_CLOSE_CODE);

    /* every transport activity below happens after the borrow's lifetime */
    check_local_settles(&r, "reason.settled", LOCAL_CLOSE_CODE);

    rig_down(&r);
    if (failures == before)
        printf("PASS: local_close_reason_not_retained\n");
}

/* --- controls: the non-local causes, on the pre-fix product -------------- */

static void t_ctl_peer_close_cause(void)
{
    int before = failures;
    struct rig r;
    struct cause c;
    struct drained d;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("CTL-PEER-CAUSE:\n");
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(&r);

    deliver_peer_close(&r, PEER_CLOSE_CODE);
    c = cause_of(&r);
    print_cause("peer", c);

    CHECKN("ctl_peer.pending", c.pending);
    CHECKN("ctl_peer.clean", c.clean);
    CHECKN("ctl_peer.code", c.code == PEER_CLOSE_CODE);

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    d = drain_events(&r);

    CHECKN("ctl_peer.one_event", d.total == 1 && d.closed == 1);
    CHECKN("ctl_peer.event_code", d.close_code == PEER_CLOSE_CODE);
    CHECKN("ctl_peer.bridge_closed",
           moq_transport_bridge_is_closed(r.conn->bridge));
    CHECKN("ctl_peer.bridge_close_code",
           moq_transport_bridge_close_code(r.conn->bridge) ==
               PEER_CLOSE_CODE);
    CHECKN("ctl_peer.bridge_not_fatal", !moq_msquic_conn_is_fatal(r.conn));
    CHECKN("ctl_peer.one_feed", r.conn->test_close_feed_commits == 1);
    CHECKN("ctl_peer.code_kept", r.conn->close_code == PEER_CLOSE_CODE);
    CHECKN("ctl_peer.clean_kept", r.conn->close_clean);

    rig_down(&r);
    if (failures == before)
        printf("PASS: ctl_peer_close_cause\n");
}

static void t_ctl_transport_error_cause(void)
{
    int before = failures;
    struct rig r;
    struct cause c;
    struct drained d;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("CTL-TRANSPORT-CAUSE:\n");
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(&r);

    deliver_transport_error(&r, TRANSPORT_CLOSE_CODE);
    c = cause_of(&r);
    print_cause("transport", c);

    CHECKN("ctl_transport.pending", c.pending);
    CHECKN("ctl_transport.unclean", !c.clean);
    CHECKN("ctl_transport.code", c.code == TRANSPORT_CLOSE_CODE);

    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    d = drain_events(&r);

    CHECKN("ctl_transport.one_event", d.total == 1 && d.closed == 1);
    CHECKN("ctl_transport.event_code", d.close_code == TRANSPORT_CLOSE_CODE);
    CHECKN("ctl_transport.bridge_fatal", moq_msquic_conn_is_fatal(r.conn));
    CHECKN("ctl_transport.bridge_fatal_code",
           moq_transport_bridge_fatal_code(r.conn->bridge) ==
               TRANSPORT_CLOSE_CODE);
    CHECKN("ctl_transport.bridge_not_closed",
           !moq_transport_bridge_is_closed(r.conn->bridge));
    CHECKN("ctl_transport.one_feed", r.conn->test_close_feed_commits == 1);
    CHECKN("ctl_transport.code_kept",
           r.conn->close_code == TRANSPORT_CLOSE_CODE);
    CHECKN("ctl_transport.unclean_kept", !r.conn->close_clean);

    rig_down(&r);
    if (failures == before)
        printf("PASS: ctl_transport_error_cause\n");
}

/* First recorded cause wins, in both orders, and repeats change nothing. */
static void ctl_order(const char *label, const char *tag, bool peer_first)
{
    int before = failures;
    struct rig r;
    struct cause c;
    struct drained d;
    uint64_t want = peer_first ? (uint64_t)PEER_CLOSE_CODE
                               : (uint64_t)TRANSPORT_CLOSE_CODE;

    CHECK(rig_up(&r, NULL, NULL) == 0);
    printf("%s:\n", label);
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_CONNECTED);
    (void)drain_events(&r);

    if (peer_first) {
        deliver_peer_close(&r, PEER_CLOSE_CODE);
        deliver_transport_error(&r, TRANSPORT_CLOSE_CODE);
    } else {
        deliver_transport_error(&r, TRANSPORT_CLOSE_CODE);
        deliver_peer_close(&r, PEER_CLOSE_CODE);
    }
    /* repeats of both, plus repeated completions */
    deliver_peer_close(&r, PEER_CLOSE_CODE + 1);
    deliver_transport_error(&r, TRANSPORT_CLOSE_CODE + 1);
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    deliver_conn_event(&r, QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE);
    moq_msquic_conn_service(r.conn);
    d = drain_events(&r);

    c = cause_of(&r);
    print_cause("winner", c);

    checkn_field(tag, "clean", c.clean == peer_first);
    checkn_field(tag, "code", c.code == want);
    checkn_field(tag, "one_feed", r.conn->test_close_feed_commits == 1);
    checkn_field(tag, "one_event", d.total == 1 && d.closed == 1);
    checkn_field(tag, "event_code", d.close_code == want);
    if (peer_first) {
        checkn_field(tag, "bridge_closed",
                     moq_transport_bridge_is_closed(r.conn->bridge));
        checkn_field(tag, "bridge_close_code",
                     moq_transport_bridge_close_code(r.conn->bridge) == want);
        checkn_field(tag, "bridge_not_fatal",
                     !moq_msquic_conn_is_fatal(r.conn));
    } else {
        checkn_field(tag, "bridge_fatal", moq_msquic_conn_is_fatal(r.conn));
        checkn_field(tag, "bridge_fatal_code",
                     moq_transport_bridge_fatal_code(r.conn->bridge) == want);
        checkn_field(tag, "bridge_not_closed",
                     !moq_transport_bridge_is_closed(r.conn->bridge));
    }

    rig_down(&r);
    if (failures == before)
        printf("PASS: %s\n", label);
}

static void t_ctl_peer_then_transport(void)
{
    ctl_order("CTL-PEER-THEN-TRANSPORT", "ctl_peer_first", true);
}

static void t_ctl_transport_then_peer(void)
{
    ctl_order("CTL-TRANSPORT-THEN-PEER", "ctl_transport_first", false);
}

int main(void)
{
    t_internal_fatal_then_transport_error();
    t_internal_fatal_then_clean_close();
    t_terminal_before_service();
    t_clean_peer_close();
    t_terminal_during_service();
    t_terminal_after_service();
    t_duplicate_completion();
    t_local_stop_then_terminal();

    t_ctl_peer_close_cause();
    t_ctl_transport_error_cause();
    t_ctl_peer_then_transport();
    t_ctl_transport_then_peer();

    t_local_close_publishes_before_shutdown();
    t_local_close_code_zero_vs_unclean_zero();
    t_local_close_wins_injected_peer();
    t_local_close_wins_injected_transport();
    t_local_close_wins_injected_completion();
    t_local_close_wins_injected_all();
    t_local_close_reason_not_retained();

    if (failures == 0)
        printf("PASS: msquic_close_feed\n");
    else
        fprintf(stderr, "FAIL: msquic_close_feed (%d)\n", failures);
    return failures;
}
