#include "test_cases.h"

int test_create_destroy(void)
{
    int failures = 0;
    fake_endpoint_t ep;
    held_endpoint_init(&ep, 100, 200);

    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) == MOQ_OK);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;
    HELD_REQUIRE(held_bridge_create(&bcfg, s, &ep.vtable, &ep, &b) == MOQ_OK);
    MOQ_TEST_CHECK(b != NULL);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(b) == 0);

    held_bridge_destroy(b);
    moq_session_destroy(s);
    return failures;
}


int test_create_rejects_bad_ops(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    moq_session_create(&cfg, 0, &s);

    moq_transport_endpoint_ops_t bad = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;

    MOQ_TEST_CHECK(held_bridge_create(&bcfg, s, &bad, &bad, &b) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(b == NULL);

    moq_session_destroy(s);
    return failures;
}


int test_setup_handshake(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    MOQ_TEST_CHECK(setup_handshake(&tp));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}


int test_control_write_backpressure(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    held_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(tp.client_ep.block_count > 0);

    tp.client_ep.block_write = false;
    tp.client_ep.block_count = 0;

    held_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}


int test_transport_close(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    held_bridge_close(tp.client_bridge, 0x42, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.client_bridge) == 0);

    test_pair_destroy(&tp);
    return failures;
}


int test_datagram_inbound_not_fatal(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    moq_result_t rc = moq_transport_bridge_on_peer_datagram(
        tp.server_bridge, (const uint8_t *)"test", 4, 0);
    MOQ_TEST_CHECK(rc >= 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}


int test_inbound_uni_after_setup(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* After handshake, deliver some uni bytes to the server.
     * The session should accept them (it's established). */
    uint8_t dummy[4] = {0x01, 0x02, 0x03, 0x04};
    moq_result_t rc = held_bridge_uni_bytes(
        tp.server_bridge, 5000, dummy, 4, false, 0);
    MOQ_TEST_CHECK(rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}


static moq_transport_result_t error_close_transport(void *ctx, uint64_t code,
                                                     const uint8_t *r, size_t l)
{
    (void)ctx; (void)code; (void)r; (void)l;
    return MOQ_TRANSPORT_ERROR;
}


int test_close_error_is_fatal_not_closed(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Replace close_transport with one that always returns ERROR */
    tp.client_ep.vtable.close_transport = error_close_transport;

    /* Deliver control FIN to client using the control stream ID
     * that was established during handshake (client opened bidi 2000) */
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000,
        NULL, 0, true, 0);

    /* service() tries close_transport → ERROR → fatal, not closed */
    held_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}


/* -- Regression: empty uni on unknown stream ----------------------- */

int test_empty_uni_no_ghost_stream(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    size_t before = moq_transport_bridge_stream_count(tp.server_bridge);

    moq_result_t rc = held_bridge_uni_bytes(
        tp.server_bridge, 9999, NULL, 0, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.server_bridge) == before);

    test_pair_destroy(&tp);
    return failures;
}


/* -- Regression: truncated vtable rejected -------------------------- */

int test_truncated_vtable_rejected(void)
{
    int failures = 0;
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *s = NULL;
    moq_session_create(&cfg, 0, &s);

    moq_transport_endpoint_ops_t trunc = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    trunc.open_uni = fake_open_uni;
    trunc.open_bidi = fake_open_bidi;
    trunc.write = fake_write;
    trunc.reset_stream = fake_reset;
    trunc.stop_sending = fake_stop;
    trunc.close_transport = fake_close;
    /* Shrink struct_size so close_transport is outside declared bounds.
     * The old bug would still read close_transport and accept it. The
     * fix uses HAS_FIELD() and rejects because struct_size is too small. */
    trunc.struct_size = (uint32_t)offsetof(moq_transport_endpoint_ops_t,
                                            close_transport);

    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    moq_transport_bridge_t *b = NULL;

    moq_result_t rc = held_bridge_create(
        &bcfg, s, &trunc, &trunc, &b);
    MOQ_TEST_CHECK(rc == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(b == NULL);

    moq_session_destroy(s);
    return failures;
}


/* Bogus outbound datagram result test deferred to conformance suite —
 * triggering SEND_DATAGRAM requires a full subscribe+datagram flow.
 * The sanitizer switch in transport_bridge.c is verified by inspection
 * and will be covered by conformance scenarios. */

/* -- Hard retry: control write blocked then close ------------------- */

int test_close_retry_after_blocked_control(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    /* Block writes so control setup goes pending */
    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    held_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));

    /* Deliver control FIN while write is still blocked.
     * The bridge should notify the session and schedule deferred close. */
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);

    /* Unblock and service — close must happen */
    tp.client_ep.block_write = false;
    held_bridge_service(tp.client_bridge, 0);

    /* Bridge must be closed (not fatal), with endpoint close observed */
    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(fake_endpoint_find(&tp.client_ep, FAKE_OP_CLOSE) != NULL);

    test_pair_destroy(&tp);
    return failures;
}


/*
 * The deferred close clears ordinary outbound pending work before retaining
 * its own record. That cleanup is load-bearing: without it the close would
 * queue behind writes that the peer is never going to accept.
 *
 * Reached the same way as the blocked-control case above: real control-write
 * backpressure gives genuine ordinary pending depth, then a peer control FIN
 * arms the close while the transport still refuses it.
 */
int test_deferred_close_clears_ordinary_pending(void)
{
    int failures = 0;
    test_pair_t tp;
    uint32_t before;
    uint64_t expected_code;

    if (test_pair_init(&tp) < 0) return 1;

    /* real control-write backpressure, so ordinary pending is not synthetic */
    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    (void)held_bridge_service(tp.client_bridge, 0);
    before = tp.client_bridge->pending_count;
    if (before == 0) {
        fprintf(stderr, "CLEANUP: no ordinary pending work to clear; the "
                "assertion below would be vacuous\n");
        failures++;
    }

    /* the transport refuses the close too, so the close must be retained */
    tp.client_ep.block_close = true;
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);
    MOQ_TEST_CHECK(tp.client_bridge->needs_close);
    expected_code = tp.client_bridge->needs_close_code;

    (void)held_bridge_service(tp.client_bridge, 0);

    /* the ordinary work is gone, replaced by exactly the close record */
    if (tp.client_bridge->pending_count != 1) {
        fprintf(stderr, "CLEANUP: pending went %u -> %u; the deferred close "
                "did not replace ordinary work with exactly its own record\n",
                before, tp.client_bridge->pending_count);
        failures++;
    } else if (tp.client_bridge->pending[0].kind != PENDING_CLOSE_TRANSPORT) {
        fprintf(stderr, "CLEANUP: the single retained item is kind %d, not the "
                "deferred close\n", (int)tp.client_bridge->pending[0].kind);
        failures++;
    } else if (tp.client_bridge->pending[0].error_code != expected_code) {
        fprintf(stderr, "CLEANUP: the retained close carries code %llu, not "
                "the %llu the peer's FIN earned\n",
                (unsigned long long)tp.client_bridge->pending[0].error_code,
                (unsigned long long)expected_code);
        failures++;
    }
    /* and this is ordinary backpressure, not an error */
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    /* letting the close through completes cleanly, with no residue */
    tp.client_ep.block_close = false;
    tp.client_ep.block_write = false;
    (void)held_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(fake_endpoint_find(&tp.client_ep, FAKE_OP_CLOSE) != NULL);

    test_pair_destroy(&tp);
    return failures;
}


/* -- Hard retry: close_transport WOULD_BLOCK then succeeds ---------- */

int test_close_retry_would_block(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Block close, then trigger control FIN */
    tp.client_ep.block_close = true;
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);

    held_bridge_service(tp.client_bridge, 0);
    /* Close should be pending (WOULD_BLOCK) */
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.client_bridge));

    /* Unblock and retry */
    tp.client_ep.block_close = false;
    fake_endpoint_clear_ops(&tp.client_ep);
    held_bridge_service(tp.client_bridge, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(fake_endpoint_find(&tp.client_ep, FAKE_OP_CLOSE) != NULL);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}


/* -- A control stream may not end ----------------------------------- *
 *
 * Draft-16 section 3.3: the control stream must not be closed at the
 * transport layer during the session's lifetime; doing so closes the session
 * as a PROTOCOL_VIOLATION (0x3, section 3.4). The peer can end a half of it
 * three ways: a FIN or a RESET_STREAM of its own half, or a STOP_SENDING of
 * ours. Each closes a session that is still open with that code, on the side
 * that opened the stream and on the side that accepted it, whether or not
 * control input was still retained when it happened.
 *
 * A session that has already closed keeps the close it decided first. Its
 * code and reason reach the transport, and its one terminal event carries the
 * same, however the control stream ends afterwards.
 */

typedef enum {
    CTRL_END_FIN = 0,
    CTRL_END_RESET,
    CTRL_END_STOP
} ctrl_end_t;

static const char *const ctrl_end_names[] = { "FIN", "RESET", "STOP_SENDING" };

/* The peer's own stream code, chosen so a close that merely echoed it could
 * not be mistaken for the protocol violation. */
#define CTRL_END_PEER_CODE 0x55u

/* The pair's one control stream is the client's first bidirectional stream,
 * which both endpoints key as 2000. */
#define CTRL_END_STREAM 2000u

/* The close an application decides first in these rows. */
#define CTRL_END_FIRST_CODE 0x2u
static const char ctrl_end_first_reason[] = "first";

static moq_result_t ctrl_end_deliver(moq_transport_bridge_t *b, uint64_t sid,
                                     ctrl_end_t how)
{
    switch (how) {
    case CTRL_END_FIN:
        return moq_transport_bridge_on_peer_control_bytes(
            b, sid, NULL, 0, true, 0);
    case CTRL_END_RESET:
        return held_bridge_reset(b, sid, CTRL_END_PEER_CODE, 0);
    case CTRL_END_STOP:
    default:
        return moq_transport_bridge_on_peer_stop_sending(
            b, sid, CTRL_END_PEER_CODE, 0);
    }
}

typedef struct {
    size_t   closed_events;
    uint64_t code;
    size_t   reason_len;
    uint8_t  reason[64];
} ctrl_end_terminal_t;

/* Takes every event the session still holds and keeps its terminal: how many
 * SESSION_CLOSED events there were, and the code and reason of the last. */
static void ctrl_end_drain_events(moq_session_t *s, ctrl_end_terminal_t *t)
{
    moq_event_t ev;

    memset(t, 0, sizeof(*t));
    t->code = UINT64_MAX;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            t->closed_events++;
            t->code = ev.u.closed.code;
            t->reason_len = ev.u.closed.reason.len;
            if (t->reason_len > sizeof(t->reason))
                t->reason_len = sizeof(t->reason);
            if (t->reason_len > 0)
                memcpy(t->reason, ev.u.closed.reason.data, t->reason_len);
        }
        moq_event_cleanup(&ev);
    }
}

typedef enum {
    CTRL_REASON_NONE = 0,   /* no reason phrase: the bridge's own close */
    CTRL_REASON_GIVEN,      /* exactly the given phrase */
    CTRL_REASON_SESSION     /* the session's own phrase, which is not empty */
} ctrl_reason_t;

/* One decision, told the same way twice: the session's single terminal event
 * and the transport's single close carry this code and this reason, the
 * bridge ended closed rather than fatal with nothing left pending, and a
 * later service pass adds neither a close nor an event. */
static int ctrl_end_expect_close(moq_session_t *s, moq_transport_bridge_t *b,
                                 fake_endpoint_t *ep, uint64_t code,
                                 ctrl_reason_t kind, const char *reason,
                                 const char *what)
{
    int failures = 0;
    ctrl_end_terminal_t t;
    const fake_op_t *op;
    size_t want_len = kind == CTRL_REASON_GIVEN ? strlen(reason) : 0;

    MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
    ctrl_end_drain_events(s, &t);
    MOQ_TEST_CHECK_EQ_SIZE(t.closed_events, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(t.code, code);
    if (kind == CTRL_REASON_SESSION) {
        MOQ_TEST_CHECK(t.reason_len > 0);
        want_len = t.reason_len;
        reason = (const char *)t.reason;
    }
    MOQ_TEST_CHECK_EQ_SIZE(t.reason_len, want_len);
    if (t.reason_len == want_len && want_len > 0)
        MOQ_TEST_CHECK(memcmp(t.reason, reason, want_len) == 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(b));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(b));
    MOQ_TEST_CHECK_EQ_U64(moq_transport_bridge_close_code(b), code);
    MOQ_TEST_CHECK_EQ_SIZE(fake_endpoint_count_kind(ep, FAKE_OP_CLOSE),
                           (size_t)1);
    op = fake_endpoint_find(ep, FAKE_OP_CLOSE);
    MOQ_TEST_CHECK(op != NULL);
    if (op) {
        MOQ_TEST_CHECK_EQ_U64(op->error_code, code);
        MOQ_TEST_CHECK_EQ_SIZE(op->data_len, want_len);
        if (op->data_len == want_len && want_len > 0)
            MOQ_TEST_CHECK(memcmp(op->data, reason, want_len) == 0);
    }
    MOQ_TEST_CHECK(!ep->overflowed);
    MOQ_TEST_CHECK(!ep->truncated);

    MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(fake_endpoint_count_kind(ep, FAKE_OP_CLOSE),
                           (size_t)1);
    ctrl_end_drain_events(s, &t);
    MOQ_TEST_CHECK_EQ_SIZE(t.closed_events, (size_t)0);

    if (failures) fprintf(stderr, "CTRLEND: %s\n", what);
    return failures;
}

/* The control stream's end has just been delivered to a session that was
 * still open: the session is closed, the bridge owes the transport the
 * protocol-violation close, and one service pass performs it. */
static int ctrl_end_expect_owed_violation(moq_session_t *s,
                                          moq_transport_bridge_t *b,
                                          fake_endpoint_t *ep,
                                          const char *what)
{
    int failures = 0;

    MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
    MOQ_TEST_CHECK(b->needs_close);
    MOQ_TEST_CHECK_EQ_U64(b->needs_close_code, 0x3);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
    fake_endpoint_clear_ops(ep);
    MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
    if (failures) fprintf(stderr, "CTRLEND: %s\n", what);
    return failures +
           ctrl_end_expect_close(s, b, ep, 0x3, CTRL_REASON_NONE, NULL, what);
}

/* A pair in which one side has a single event slot, still holding its own
 * SETUP_COMPLETE: the next control message that needs an event blocks in
 * that side's session, and its bridge retains the control retry. */
static int ctrl_end_retaining_pair(test_pair_t *tp, bool server)
{
    moq_event_t ev;
    moq_session_t *other;

    if ((server ? test_pair_init_server_events(tp, 1)
                : test_pair_init_ex(tp, 1)) < 0)
        return -1;
    moq_session_start(tp->client, 0);
    pump_until_quiescent(tp, 20, 0);
    other = server ? tp->client : tp->server;
    while (moq_session_poll_events(other, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    return 0;
}

/* The peer's GOAWAY as its bridge writes it: the bytes and the stream they
 * travel on. */
static bool ctrl_end_peer_goaway(test_pair_t *tp, bool server, uint8_t *out,
                                 size_t cap, size_t *len, uint64_t *sid)
{
    moq_session_t *peer = server ? tp->client : tp->server;
    moq_transport_bridge_t *pb = server ? tp->client_bridge : tp->server_bridge;
    fake_endpoint_t *pep = server ? &tp->client_ep : &tp->server_ep;
    bool found = false;

    if (moq_session_goaway(peer, NULL, 0, 0) != MOQ_OK) return false;
    if (held_bridge_service(pb, 0) != MOQ_OK) return false;
    for (size_t i = 0; i < pep->count; i++) {
        const fake_op_t *o = &pep->ops[i];
        if (o->kind != FAKE_OP_WRITE || o->data_len == 0 || o->data_len > cap)
            continue;
        memcpy(out, o->data, o->data_len);
        *len = o->data_len;
        *sid = o->stream_id;
        found = true;
    }
    fake_endpoint_clear_ops(pep);
    return found;
}

/* The drafts whose control channel is a pair of unidirectional streams. */
static const int ctrl_end_uni_drafts[] = { 18, 22 };
#define CTRL_END_UNI_DRAFTS \
    (sizeof(ctrl_end_uni_drafts) / sizeof(ctrl_end_uni_drafts[0]))
/* The ways a control stream can end: FIN, RESET, STOP_SENDING. */
#define CTRL_END_WAYS ((size_t)(CTRL_END_STOP - CTRL_END_FIN + 1))

/* An established pair of one of those drafts, the client's events drained. */
static int ctrl_end_uni_pair(test_pair_t *tp, int draft)
{
    moq_event_t ev;

    if ((draft == 22 ? d22_pair_init(tp) : d18_pair_init(tp, 0)) < 0)
        return -1;
    moq_session_start(tp->client, 0);
    moq_session_start(tp->server, 0);
    if (draft == 22)
        priority_pump(tp, true, 0);
    else
        d18_shuttle_until_quiescent(tp, 30, 0);
    while (moq_session_poll_events(tp->client, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    return 0;
}

/* An established pair of one of those drafts whose client has a single event
 * slot, still holding its own SETUP_COMPLETE: the next control message that
 * needs an event blocks in the client's session, and its bridge retains the
 * control retry. */
static int ctrl_end_uni_retaining_pair(test_pair_t *tp, int draft)
{
    moq_event_t ev;

    if ((draft == 22 ? d22_pair_init_events(tp, 1) : d18_pair_init(tp, 1)) < 0)
        return -1;
    moq_session_start(tp->client, 0);
    moq_session_start(tp->server, 0);
    if (draft == 22)
        priority_pump(tp, true, 0);
    else
        d18_shuttle_until_quiescent(tp, 30, 0);
    while (moq_session_poll_events(tp->server, &ev, 1) > 0)
        moq_event_cleanup(&ev);
    return 0;
}

/* Control input as the transport delivers it: on the bidirectional control
 * stream of draft-16, or on the peer's unidirectional control stream. */
static moq_result_t ctrl_end_feed(moq_transport_bridge_t *b, bool uni,
                                  uint64_t sid, const uint8_t *data,
                                  size_t len, bool fin)
{
    return uni ? held_bridge_uni_bytes(b, sid, data, len, fin, 0)
               : moq_transport_bridge_on_peer_control_bytes(b, sid, data, len,
                                                            fin, 0);
}

int test_bidi_control_stream_end_is_violation(void)
{
    int failures = 0;
    char what[96];

    /* Each way of ending the stream, on each side, with nothing retained. */
    for (int server = 0; server < 2; server++) {
        for (int how = CTRL_END_FIN; how <= CTRL_END_STOP; how++) {
            test_pair_t tp;
            if (test_pair_init(&tp) < 0) return failures + 1;
            if (!setup_handshake(&tp)) {
                test_pair_destroy(&tp);
                return failures + 1;
            }
            moq_session_t *s = server ? tp.server : tp.client;
            moq_transport_bridge_t *b =
                server ? tp.server_bridge : tp.client_bridge;
            fake_endpoint_t *ep = server ? &tp.server_ep : &tp.client_ep;
            snprintf(what, sizeof(what), "%s side, peer %s",
                     server ? "accepting" : "opening", ctrl_end_names[how]);

            MOQ_TEST_CHECK(!moq_transport_bridge_uses_uni_control(b));
            MOQ_TEST_CHECK(moq_transport_bridge_stream_is_control(
                b, CTRL_END_STREAM));
            MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
            MOQ_TEST_CHECK(!b->needs_close);

            MOQ_TEST_CHECK_EQ_INT(
                (int)ctrl_end_deliver(b, CTRL_END_STREAM, (ctrl_end_t)how),
                (int)MOQ_OK);
            failures += ctrl_end_expect_owed_violation(s, b, ep, what);
            test_pair_destroy(&tp);
        }
    }

    /* The stream is recognised by either identity alone. The opening side
     * knows it from its own output before any control input arrives; the
     * accepting side knows it from the peer's first control bytes before it
     * has written anything. A RESET or a STOP then closes the session. */
    for (int server = 0; server < 2; server++) {
        for (int how = CTRL_END_RESET; how <= CTRL_END_STOP; how++) {
            test_pair_t tp;
            const fake_op_t *setup;

            if (test_pair_init(&tp) < 0) return failures + 1;
            moq_session_t *s = server ? tp.server : tp.client;
            moq_transport_bridge_t *b =
                server ? tp.server_bridge : tp.client_bridge;
            fake_endpoint_t *ep = server ? &tp.server_ep : &tp.client_ep;
            snprintf(what, sizeof(what), "%s side, %s identity only, peer %s",
                     server ? "accepting" : "opening",
                     server ? "peer-input" : "local-output",
                     ctrl_end_names[how]);

            moq_session_start(tp.client, 0);
            MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(tp.client_bridge, 0),
                                  (int)MOQ_OK);
            setup = fake_endpoint_find(&tp.client_ep, FAKE_OP_WRITE);
            MOQ_TEST_CHECK(setup != NULL && setup->data_len > 1);
            if (server && setup) {
                /* One byte of the client's setup: an incomplete message,
                 * which the accepting side buffers without answering. */
                MOQ_TEST_CHECK_EQ_INT(
                    (int)moq_transport_bridge_on_peer_control_bytes(
                        b, setup->stream_id, setup->data, 1, false, 0),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK(b->peer_control_open);
                MOQ_TEST_CHECK(!b->control_open);
            } else {
                MOQ_TEST_CHECK(b->control_open);
                MOQ_TEST_CHECK(!b->peer_control_open);
            }
            MOQ_TEST_CHECK(moq_session_state(s) != MOQ_SESS_CLOSED);
            MOQ_TEST_CHECK(!b->needs_close);

            MOQ_TEST_CHECK_EQ_INT(
                (int)ctrl_end_deliver(b, CTRL_END_STREAM, (ctrl_end_t)how),
                (int)MOQ_OK);
            failures += ctrl_end_expect_owed_violation(s, b, ep, what);
            test_pair_destroy(&tp);
        }
    }

    /* The same three while control input is retained, on each side. That
     * side's single event slot stays occupied by its own SETUP_COMPLETE, so
     * the peer's GOAWAY blocks in the session and the bridge holds a control
     * retry. */
    for (int server = 0; server < 2; server++) {
        for (int how = CTRL_END_FIN; how <= CTRL_END_STOP; how++) {
            test_pair_t tp;
            moq_event_t ev;
            uint8_t goaway[128];
            size_t goaway_len = 0;
            uint64_t sid = 0;

            if (ctrl_end_retaining_pair(&tp, server != 0) < 0)
                return failures + 1;
            moq_session_t *s = server ? tp.server : tp.client;
            moq_transport_bridge_t *b =
                server ? tp.server_bridge : tp.client_bridge;
            fake_endpoint_t *ep = server ? &tp.server_ep : &tp.client_ep;
            snprintf(what, sizeof(what),
                     "%s side, control input retained, peer %s",
                     server ? "accepting" : "opening", ctrl_end_names[how]);

            MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
            MOQ_TEST_CHECK(s->event_head != s->event_tail);
            MOQ_TEST_CHECK(ctrl_end_peer_goaway(&tp, server != 0, goaway,
                                                sizeof(goaway), &goaway_len,
                                                &sid));
            /* The FIN row ends the stream with the bytes that block. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_transport_bridge_on_peer_control_bytes(
                    b, sid, goaway, goaway_len, how == CTRL_END_FIN, 0),
                (int)MOQ_ERR_WOULD_BLOCK);
            MOQ_TEST_CHECK(b->pending_control);
            MOQ_TEST_CHECK(!b->needs_close);
            MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
            MOQ_TEST_CHECK(!s->goaway_received);

            if (how == CTRL_END_FIN) {
                /* A retained FIN is delivered after the input it followed:
                 * free the slot, and one service pass handles the GOAWAY,
                 * then the FIN, and closes. */
                MOQ_TEST_CHECK(b->pending_control_fin);
                MOQ_TEST_CHECK(moq_session_poll_events(s, &ev, 1) == 1);
                moq_event_cleanup(&ev);
                fake_endpoint_clear_ops(ep);
                MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0),
                                      (int)MOQ_OK);
                MOQ_TEST_CHECK(!b->pending_control);
                MOQ_TEST_CHECK(s->goaway_received);
                failures += ctrl_end_expect_close(s, b, ep, 0x3,
                                                  CTRL_REASON_NONE, NULL, what);
            } else {
                /* A RESET or a STOP does not wait for the retained input. */
                MOQ_TEST_CHECK(!b->pending_control_fin);
                MOQ_TEST_CHECK_EQ_INT(
                    (int)ctrl_end_deliver(b, sid, (ctrl_end_t)how),
                    (int)MOQ_OK);
                MOQ_TEST_CHECK(!s->goaway_received);
                failures += ctrl_end_expect_owed_violation(s, b, ep, what);
            }
            test_pair_destroy(&tp);
        }
    }

    /* The transport's own report that it is done with the stream is local
     * bookkeeping, not a peer signal: it closes nothing. */
    for (int server = 0; server < 2; server++) {
        test_pair_t tp;
        if (test_pair_init(&tp) < 0) return failures + 1;
        if (!setup_handshake(&tp)) {
            test_pair_destroy(&tp);
            return failures + 1;
        }
        moq_session_t *s = server ? tp.server : tp.client;
        moq_transport_bridge_t *b =
            server ? tp.server_bridge : tp.client_bridge;

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_stream_terminal(
                b, CTRL_END_STREAM, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!b->needs_close);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
        test_pair_destroy(&tp);
    }

    /* A unidirectional control stream of a later draft is unaffected: its FIN
     * was, and remains, the same protocol violation. */
    for (size_t d = 0; d < CTRL_END_UNI_DRAFTS; d++) {
        test_pair_t tp;

        if (ctrl_end_uni_pair(&tp, ctrl_end_uni_drafts[d]) < 0)
            return failures + 1;
        snprintf(what, sizeof(what),
                 "draft-%d unidirectional control stream, peer FIN",
                 ctrl_end_uni_drafts[d]);
        MOQ_TEST_CHECK(moq_transport_bridge_uses_uni_control(tp.client_bridge));
        MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(tp.client_bridge->peer_ctrl_uni_open);

        MOQ_TEST_CHECK_EQ_INT(
            (int)held_bridge_uni_bytes(
                tp.client_bridge, tp.client_bridge->peer_ctrl_uni_stream_id,
                NULL, 0, true, 0),
            (int)MOQ_OK);
        failures += ctrl_end_expect_owed_violation(
            tp.client, tp.client_bridge, &tp.client_ep, what);
        test_pair_destroy(&tp);
    }

    return failures;
}


/* Delivers the control stream's end twice to a session that has already
 * closed. Neither delivery may arm a close of its own or turn the bridge
 * fatal. */
static int ctrl_end_deliver_late(moq_transport_bridge_t *b, uint64_t sid,
                                 ctrl_end_t how, const char *what)
{
    int failures = 0;

    for (int again = 0; again < 2; again++) {
        MOQ_TEST_CHECK_EQ_INT((int)ctrl_end_deliver(b, sid, how), (int)MOQ_OK);
        MOQ_TEST_CHECK(!b->needs_close);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
    }
    if (failures) fprintf(stderr, "CTRLEND: %s (late delivery)\n", what);
    return failures;
}

int test_control_stream_end_keeps_first_close(void)
{
    int failures = 0;
    char what[112];

    /* The application closed the session, and the control stream ends before
     * the bridge has given that close to the transport. */
    for (int server = 0; server < 2; server++) {
        for (int how = CTRL_END_FIN; how <= CTRL_END_STOP; how++) {
            test_pair_t tp;
            if (test_pair_init(&tp) < 0) return failures + 1;
            if (!setup_handshake(&tp)) {
                test_pair_destroy(&tp);
                return failures + 1;
            }
            moq_session_t *s = server ? tp.server : tp.client;
            moq_transport_bridge_t *b =
                server ? tp.server_bridge : tp.client_bridge;
            fake_endpoint_t *ep = server ? &tp.server_ep : &tp.client_ep;
            snprintf(what, sizeof(what),
                     "%s side, local close queued, then peer %s",
                     server ? "accepting" : "opening", ctrl_end_names[how]);

            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_close(s, CTRL_END_FIRST_CODE,
                                       ctrl_end_first_reason, 0),
                (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
            MOQ_TEST_CHECK(!b->needs_close);
            MOQ_TEST_CHECK_EQ_SIZE((size_t)b->pending_count, (size_t)0);

            failures += ctrl_end_deliver_late(b, CTRL_END_STREAM,
                                              (ctrl_end_t)how, what);
            fake_endpoint_clear_ops(ep);
            MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
            failures += ctrl_end_expect_close(
                s, b, ep, CTRL_END_FIRST_CODE, CTRL_REASON_GIVEN,
                ctrl_end_first_reason, what);
            test_pair_destroy(&tp);
        }
    }

    /* The same, after the transport refused that close: the bridge retains
     * it with its code and reason, and the control stream ends while it is
     * retained. The refusal is backpressure, so the pass that met it still
     * reports success. */
    for (int server = 0; server < 2; server++) {
        for (int how = CTRL_END_FIN; how <= CTRL_END_STOP; how++) {
            test_pair_t tp;
            if (test_pair_init(&tp) < 0) return failures + 1;
            if (!setup_handshake(&tp)) {
                test_pair_destroy(&tp);
                return failures + 1;
            }
            moq_session_t *s = server ? tp.server : tp.client;
            moq_transport_bridge_t *b =
                server ? tp.server_bridge : tp.client_bridge;
            fake_endpoint_t *ep = server ? &tp.server_ep : &tp.client_ep;
            snprintf(what, sizeof(what),
                     "%s side, local close refused, then peer %s",
                     server ? "accepting" : "opening", ctrl_end_names[how]);

            ep->block_close = true;
            ep->block_count = 0;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_close(s, CTRL_END_FIRST_CODE,
                                       ctrl_end_first_reason, 0),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
            MOQ_TEST_CHECK(ep->block_count > 0);
            MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
            MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
            MOQ_TEST_CHECK(!b->needs_close);
            MOQ_TEST_CHECK_EQ_SIZE((size_t)b->pending_count, (size_t)1);
            if (b->pending_count == 1) {
                MOQ_TEST_CHECK(b->pending[0].kind == PENDING_CLOSE_TRANSPORT);
                MOQ_TEST_CHECK_EQ_U64(b->pending[0].error_code,
                                      CTRL_END_FIRST_CODE);
            }

            failures += ctrl_end_deliver_late(b, CTRL_END_STREAM,
                                              (ctrl_end_t)how, what);
            MOQ_TEST_CHECK_EQ_SIZE((size_t)b->pending_count, (size_t)1);
            if (b->pending_count == 1) {
                MOQ_TEST_CHECK(b->pending[0].kind == PENDING_CLOSE_TRANSPORT);
                MOQ_TEST_CHECK_EQ_U64(b->pending[0].error_code,
                                      CTRL_END_FIRST_CODE);
            }

            ep->block_close = false;
            fake_endpoint_clear_ops(ep);
            MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
            failures += ctrl_end_expect_close(
                s, b, ep, CTRL_END_FIRST_CODE, CTRL_REASON_GIVEN,
                ctrl_end_first_reason, what);
            test_pair_destroy(&tp);
        }
    }

    /* The session closed itself over a peer protocol error (a second GOAWAY),
     * with its own code and reason, and the control stream ends before that
     * close has been given to the transport. */
    for (int server = 0; server < 2; server++) {
        for (int how = CTRL_END_FIN; how <= CTRL_END_STOP; how++) {
            test_pair_t tp;
            uint8_t goaway[128];
            size_t goaway_len = 0;
            uint64_t sid = 0;

            if (test_pair_init(&tp) < 0) return failures + 1;
            if (!setup_handshake(&tp)) {
                test_pair_destroy(&tp);
                return failures + 1;
            }
            moq_session_t *s = server ? tp.server : tp.client;
            moq_transport_bridge_t *b =
                server ? tp.server_bridge : tp.client_bridge;
            fake_endpoint_t *ep = server ? &tp.server_ep : &tp.client_ep;
            snprintf(what, sizeof(what),
                     "%s side, protocol-error close queued, then peer %s",
                     server ? "accepting" : "opening", ctrl_end_names[how]);

            MOQ_TEST_CHECK(ctrl_end_peer_goaway(&tp, server != 0, goaway,
                                                sizeof(goaway), &goaway_len,
                                                &sid));
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_transport_bridge_on_peer_control_bytes(
                    b, sid, goaway, goaway_len, false, 0),
                (int)MOQ_OK);
            MOQ_TEST_CHECK(s->goaway_received);
            MOQ_TEST_CHECK(moq_session_state(s) != MOQ_SESS_CLOSED);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_transport_bridge_on_peer_control_bytes(
                    b, sid, goaway, goaway_len, false, 0),
                (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);
            MOQ_TEST_CHECK(!b->needs_close);

            failures += ctrl_end_deliver_late(b, sid, (ctrl_end_t)how, what);
            fake_endpoint_clear_ops(ep);
            MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
            failures += ctrl_end_expect_close(s, b, ep, 0x3,
                                              CTRL_REASON_SESSION, NULL, what);
            test_pair_destroy(&tp);
        }
    }

    /* A retained FIN whose retained input closes the session by itself. Two
     * GOAWAYs and the FIN arrive together while no event can be queued; once
     * one can, the retry handles the first GOAWAY and meets the protocol
     * error in the second. The session's own close stands, with its reason;
     * the FIN that follows adds nothing. */
    for (int server = 0; server < 2; server++) {
        test_pair_t tp;
        moq_event_t ev;
        uint8_t goaway[128];
        uint8_t twice[256];
        size_t goaway_len = 0;
        uint64_t sid = 0;

        if (ctrl_end_retaining_pair(&tp, server != 0) < 0)
            return failures + 1;
        moq_session_t *s = server ? tp.server : tp.client;
        moq_transport_bridge_t *b =
            server ? tp.server_bridge : tp.client_bridge;
        fake_endpoint_t *ep = server ? &tp.server_ep : &tp.client_ep;
        snprintf(what, sizeof(what),
                 "%s side, retained input ends in a protocol error, then its "
                 "FIN", server ? "accepting" : "opening");

        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(s->event_head != s->event_tail);
        MOQ_TEST_CHECK(ctrl_end_peer_goaway(&tp, server != 0, goaway,
                                            sizeof(goaway), &goaway_len, &sid));
        memcpy(twice, goaway, goaway_len);
        memcpy(twice + goaway_len, goaway, goaway_len);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_control_bytes(
                b, sid, twice, 2 * goaway_len, true, 0),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(b->pending_control);
        MOQ_TEST_CHECK(b->pending_control_fin);
        MOQ_TEST_CHECK(!b->needs_close);
        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);

        MOQ_TEST_CHECK(moq_session_poll_events(s, &ev, 1) == 1);
        moq_event_cleanup(&ev);
        fake_endpoint_clear_ops(ep);
        MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(!b->pending_control);
        MOQ_TEST_CHECK(!b->pending_control_fin);
        MOQ_TEST_CHECK(s->goaway_received);
        failures += ctrl_end_expect_close(s, b, ep, 0x3, CTRL_REASON_SESSION,
                                          NULL, what);
        test_pair_destroy(&tp);
    }

    /* Control input is already retained when the application closes the
     * session. Later input on that control stream, bytes with or without a
     * FIN, is accepted and not delivered, and its FIN adds no debt; the
     * retained input and the close decided first stay as they are until the
     * transport takes that close. Each control topology, with the close still
     * queued and with the close refused by the transport. */
    for (size_t n = 0; n < 4 * 4; n++) {
        static const struct {
            int draft;
            bool server;
            const char *name;
        } where[4] = {
            { 16, false, "draft-16 opening side" },
            { 16, true,  "draft-16 accepting side" },
            { 18, false, "draft-18 client" },
            { 22, false, "draft-22 client" },
        };
        const int draft = where[n / 4].draft;
        const bool server = where[n / 4].server;
        const bool uni = draft != 16;
        const bool refused = (n & 1u) != 0;
        const bool late_fin = (n & 2u) != 0;
        test_pair_t tp;
        uint8_t goaway[128];
        size_t goaway_len = 0;
        uint64_t sid = 0;

        if ((uni ? ctrl_end_uni_retaining_pair(&tp, draft)
                 : ctrl_end_retaining_pair(&tp, server)) < 0)
            return failures + 1;
        moq_session_t *s = server ? tp.server : tp.client;
        moq_transport_bridge_t *b =
            server ? tp.server_bridge : tp.client_bridge;
        fake_endpoint_t *ep = server ? &tp.server_ep : &tp.client_ep;
        snprintf(what, sizeof(what),
                 "%s, control input retained, close %s, late bytes%s",
                 where[n / 4].name, refused ? "refused" : "queued",
                 late_fin ? " with FIN" : "");

        MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(s->event_head != s->event_tail);
        MOQ_TEST_CHECK(ctrl_end_peer_goaway(&tp, server, goaway,
                                            sizeof(goaway), &goaway_len, &sid));
        if (uni) {
            MOQ_TEST_CHECK(b->peer_ctrl_uni_open);
            MOQ_TEST_CHECK_EQ_U64(b->peer_ctrl_uni_stream_id, sid);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)ctrl_end_feed(b, uni, sid, goaway, goaway_len, false),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(b->pending_control);
        MOQ_TEST_CHECK_EQ_U64(b->pending_control_stream_id, sid);
        MOQ_TEST_CHECK(!b->pending_control_fin);
        MOQ_TEST_CHECK(!s->goaway_received);

        if (refused) {
            ep->block_close = true;
            ep->block_count = 0;
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_close(s, CTRL_END_FIRST_CODE,
                                   ctrl_end_first_reason, 0),
            (int)MOQ_OK);
        if (refused) {
            MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
            MOQ_TEST_CHECK(ep->block_count > 0);
            MOQ_TEST_CHECK_EQ_SIZE((size_t)b->pending_count, (size_t)1);
            MOQ_TEST_CHECK(b->pending_control);
        }

        /* The late input is a GOAWAY too: delivered, it would be a second
         * one, and the session would have to answer it. */
        for (int again = 0; again < 2; again++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)ctrl_end_feed(b, uni, sid, goaway, goaway_len, late_fin),
                (int)MOQ_OK);
            MOQ_TEST_CHECK(b->pending_control);
            MOQ_TEST_CHECK_EQ_U64(b->pending_control_stream_id, sid);
            MOQ_TEST_CHECK(!b->pending_control_fin);
            MOQ_TEST_CHECK(!b->needs_close);
            MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
            MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(b));
            MOQ_TEST_CHECK(!s->goaway_received);
        }
        if (refused) {
            MOQ_TEST_CHECK_EQ_SIZE((size_t)b->pending_count, (size_t)1);
            if (b->pending_count == 1) {
                MOQ_TEST_CHECK(b->pending[0].kind == PENDING_CLOSE_TRANSPORT);
                MOQ_TEST_CHECK_EQ_U64(b->pending[0].error_code,
                                      CTRL_END_FIRST_CODE);
            }
            ep->block_close = false;
        }

        fake_endpoint_clear_ops(ep);
        MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(!s->goaway_received);
        failures += ctrl_end_expect_close(
            s, b, ep, CTRL_END_FIRST_CODE, CTRL_REASON_GIVEN,
            ctrl_end_first_reason, what);
        test_pair_destroy(&tp);
    }

    /* The later drafts share the rule: a unidirectional control stream that
     * ends after the session's own close changes nothing either. */
    for (size_t n = 0; n < CTRL_END_UNI_DRAFTS * CTRL_END_WAYS; n++) {
        int draft = ctrl_end_uni_drafts[n / CTRL_END_WAYS];
        int how = CTRL_END_FIN + (int)(n % CTRL_END_WAYS);
        test_pair_t tp;
        moq_transport_bridge_t *b;
        moq_result_t rc;

        if (ctrl_end_uni_pair(&tp, draft) < 0) return failures + 1;
        b = tp.client_bridge;
        snprintf(what, sizeof(what),
                 "draft-%d unidirectional control, local close queued, "
                 "then peer %s",
                 draft, ctrl_end_names[how]);
        MOQ_TEST_CHECK(moq_transport_bridge_uses_uni_control(b));
        MOQ_TEST_CHECK(moq_session_state(tp.client) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(b->peer_ctrl_uni_open);
        MOQ_TEST_CHECK(b->local_ctrl_uni_open);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_close(tp.client, CTRL_END_FIRST_CODE,
                                   ctrl_end_first_reason, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(!b->needs_close);
        for (int again = 0; again < 2; again++) {
            if (how == CTRL_END_FIN)
                rc = held_bridge_uni_bytes(b, b->peer_ctrl_uni_stream_id,
                                           NULL, 0, true, 0);
            else if (how == CTRL_END_RESET)
                rc = held_bridge_reset(b, b->peer_ctrl_uni_stream_id,
                                       CTRL_END_PEER_CODE, 0);
            else
                rc = moq_transport_bridge_on_peer_stop_sending(
                    b, b->local_ctrl_uni_stream_id, CTRL_END_PEER_CODE, 0);
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
            MOQ_TEST_CHECK(!b->needs_close);
            MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));
        }
        fake_endpoint_clear_ops(&tp.client_ep);
        MOQ_TEST_CHECK_EQ_INT((int)held_bridge_service(b, 0), (int)MOQ_OK);
        failures += ctrl_end_expect_close(
            tp.client, b, &tp.client_ep, CTRL_END_FIRST_CODE,
            CTRL_REASON_GIVEN, ctrl_end_first_reason, what);
        test_pair_destroy(&tp);
    }

    return failures;
}

#ifdef MOQ_BRIDGE_TEST_INJECT


/*
 * The deferred transport close is not retained work ownership: its record
 * carries only the close code the peer's protocol violation earned, and the
 * path has already cleared ordinary pending work. A failed retain there must
 * surface an error and keep THAT code as the fatal code, rather than being
 * rewritten to the internal retention code.
 *
 * Reached the same way as the established retry-would-block case: a peer
 * control FIN with the close blocked. The preconditions are asserted before
 * the injection, and the unblocked control below proves the FIN alone does
 * not satisfy this pin.
 */
int test_deferred_close_keeps_its_own_code(void)
{
    int failures = 0;
    test_pair_t tp;
    uint64_t expected_code;
    moq_result_t rc;

    if (test_pair_init(&tp) < 0) return 1;
    if (!setup_handshake(&tp)) { test_pair_destroy(&tp); return 1; }

    tp.client_ep.block_close = true;
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);

    /* preconditions: the deferred close is armed with a real protocol code,
     * nothing ordinary is queued, and the bridge is still healthy */
    MOQ_TEST_CHECK(tp.client_bridge->needs_close);
    expected_code = tp.client_bridge->needs_close_code;
    if (expected_code == 0 || expected_code == 0x5) {
        fprintf(stderr, "CLOSEPIN: precondition close code was %llu, which "
                "cannot distinguish the retention code\n",
                (unsigned long long)expected_code);
        failures++;
    }
    MOQ_TEST_CHECK(tp.client_bridge->pending_count == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));
    fake_endpoint_clear_ops(&tp.client_ep);

    /* with nothing else queued, the close is the only retention this pass */
    moq_bridge_test_fail_retain_after(0);
    rc = held_bridge_service(tp.client_bridge, 0);

    if (rc == MOQ_OK) {
        fprintf(stderr, "CLOSEPIN: a failed deferred-close retain was "
                "reported as success\n");
        failures++;
    }
    if (!moq_transport_bridge_is_fatal(tp.client_bridge)) {
        fprintf(stderr, "CLOSEPIN: a failed deferred-close retain left the "
                "bridge usable; the injection was not consumed here\n");
        failures++;
    }
    if (moq_transport_bridge_is_closed(tp.client_bridge)) {
        fprintf(stderr, "CLOSEPIN: the bridge reported a clean close after a "
                "failed retain\n");
        failures++;
    }
    if (tp.client_bridge->fatal_code != expected_code) {
        fprintf(stderr, "CLOSEPIN: fatal code was %llu, not the close code "
                "%llu the violation earned\n",
                (unsigned long long)tp.client_bridge->fatal_code,
                (unsigned long long)expected_code);
        failures++;
    }
    if (tp.client_bridge->pending_count != 0) {
        fprintf(stderr, "CLOSEPIN: %u item(s) queued after a failed "
                "deferred-close retain\n", tp.client_bridge->pending_count);
        failures++;
    }
    /* and no close escaped to the transport */
    MOQ_TEST_CHECK(fake_endpoint_find(&tp.client_ep, FAKE_OP_CLOSE) == NULL);
    test_pair_destroy(&tp);

    /* vacuity control: the same FIN with NO injection must reach the
     * established behaviour -- a pending close on a non-fatal bridge -- so the
     * protocol FIN alone cannot satisfy the assertions above */
    if (test_pair_init(&tp) < 0) return failures + 1;
    if (!setup_handshake(&tp)) { test_pair_destroy(&tp); return failures + 1; }
    tp.client_ep.block_close = true;
    moq_transport_bridge_on_peer_control_bytes(
        tp.client_bridge, 2000, NULL, 0, true, 0);
    (void)held_bridge_service(tp.client_bridge, 0);
    if (moq_transport_bridge_is_fatal(tp.client_bridge) ||
        !moq_transport_bridge_has_pending(tp.client_bridge)) {
        fprintf(stderr, "CLOSEPIN: the control FIN alone went fatal=%d "
                "pending=%d; the pin above proves nothing\n",
                (int)moq_transport_bridge_is_fatal(tp.client_bridge),
                (int)moq_transport_bridge_has_pending(tp.client_bridge));
        failures++;
    }
    test_pair_destroy(&tp);
    return failures;
}

#endif


/* -- Reset on unknown stream is no-op ------------------------------- */

int test_reset_on_unknown_stream(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Reset for an unknown stream should be a no-op (no stream to reset) */
    moq_result_t rc = held_bridge_reset(
        tp.server_bridge, 9999, 0x42, 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));

    test_pair_destroy(&tp);
    return failures;
}


/* -- Transport close clears all state ------------------------------- */

int test_transport_close_clears_state(void)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }

    /* Create real pending state: block writes then start (no handshake).
     * session_start emits SEND_CONTROL which the bridge tries to send.
     * With writes blocked, the control data goes to the pending queue. */
    tp.client_ep.block_write = true;
    moq_session_start(tp.client, 0);
    held_bridge_service(tp.client_bridge, 0);
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(tp.client_bridge));

    /* Transport close should clear everything */
    held_bridge_close(tp.client_bridge, 0x1, 0);

    MOQ_TEST_CHECK(moq_transport_bridge_is_closed(tp.client_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(tp.client_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.client_bridge) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_tombstone_count(tp.client_bridge) == 0);

    test_pair_destroy(&tp);
    return failures;
}


/* -- Regression: dropped inbound uni stream is discarded, not misparsed -----
 *
 * When the session drops a peer uni data stream -- here, a subgroup for a
 * track_alias nobody subscribed to -- it frees its rx entry and issues
 * STOP_DATA. Bytes already in flight on that stream must be DISCARDED by the
 * bridge: feeding them onward would have the session open a fresh rx entry and
 * parse mid-stream bytes as a leading stream type ("unknown data stream type",
 * 0x3), fataling the transport. This is the shape that previously closed the
 * connection during a live->VOD catalog conversion (the publisher finishes the
 * media subscription while its last objects are still arriving).
 *
 * The same contract holds on both inbound entry points (byte and rcbuf), so the
 * body is shared and run through each via this delivery shim. */
static moq_result_t deliver_uni(moq_transport_bridge_t *b, bool use_rcbuf,
                                uint64_t sid, const uint8_t *data, size_t len,
                                bool fin)
{
    if (!use_rcbuf)
        return held_bridge_uni_bytes(b, sid, data, len, fin, 0);

    moq_rcbuf_t *buf = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), data, len, &buf) < 0)
        return MOQ_ERR_NOMEM;
    moq_result_t rc = held_bridge_uni_rcbuf(b, sid, buf, fin, 0);
    moq_rcbuf_decref(buf);
    return rc;
}


static int run_inbound_uni_dropped_then_discarded(bool use_rcbuf)
{
    int failures = 0;
    test_pair_t tp;
    if (test_pair_init(&tp) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* A valid subgroup header for an unsubscribed track_alias: the session
     * classifies it, fails to bind, and stops the stream within this call. */
    uint8_t hdr[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, hdr, sizeof(hdr));
    moq_d16_subgroup_header_t sh;
    memset(&sh, 0, sizeof(sh));
    sh.type = 0x14;
    sh.subgroup_id_mode = MOQ_SUBGROUP_ID_MODE_PRESENT;
    sh.track_alias = 9999;
    sh.group_id = 0;
    sh.subgroup_id = 0;
    sh.publisher_priority = 128;
    MOQ_TEST_CHECK(moq_d16_encode_subgroup_header(&w, &sh) == MOQ_OK);
    size_t hdr_len = moq_buf_writer_offset(&w);

    const uint64_t sid = 5000;

    /* First delivery: the session drops the stream (STOP_DATA), so the bridge
     * marks it for discard. Not a protocol violation. */
    moq_result_t rc = deliver_uni(tp.server_bridge, use_rcbuf, sid,
                                  hdr, hdr_len, false);
    MOQ_TEST_CHECK(rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));

    /* More in-flight bytes on the same stream. Their leading byte (0x20)
     * classifies as an UNKNOWN data stream type: if the bridge re-fed them as a
     * fresh stream the session would fatal with 0x3. With the discard guard
     * they are swallowed and the bridge stays healthy. */
    uint8_t more[4] = { 0x20, 0x00, 0x00, 0x00 };
    rc = deliver_uni(tp.server_bridge, use_rcbuf, sid, more, sizeof(more), false);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));

    /* Final in-flight bytes with FIN: still no fatal, and the discard entry is
     * retired so it leaves no ghost stream behind. */
    size_t before = moq_transport_bridge_stream_count(tp.server_bridge);
    MOQ_TEST_CHECK(before >= 1);
    rc = deliver_uni(tp.server_bridge, use_rcbuf, sid, more, sizeof(more), true);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.server_bridge));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(tp.server_bridge));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_count(tp.server_bridge) ==
                   before - 1);

    test_pair_destroy(&tp);
    return failures;
}


int test_inbound_uni_dropped_then_discarded(void)
{
    return run_inbound_uni_dropped_then_discarded(false);
}


int test_inbound_uni_rcbuf_dropped_then_discarded(void)
{
    return run_inbound_uni_dropped_then_discarded(true);
}


/* -- Regression: bytes delivered during inbound pending_retry are kept ------
 *
 * A peer fills the client's event queue so a data stream backs up into
 * PENDING_EMIT (bridge pending_retry). The peer then delivers another object
 * (with FIN) on the SAME stream while pending_retry is set. The transport does
 * not re-deliver stream bytes, so the bridge/session must RETAIN those bytes
 * across the WOULD_BLOCK and deliver the object after the queue drains.
 *
 * Retrying the pending emit and returning WOULD_BLOCK before appending the
 * new bytes would silently drop the object.
 * Run through both inbound entry points via deliver_uni(). */
static int run_pending_retry_keeps_bytes(bool use_rcbuf_extra)
{
    int failures = 0;
    test_pair_t tp;
    /* max_events = 1: object 0 fills the queue, object 1 -> PENDING_EMIT. */
    if (test_pair_init_ex(&tp, 1) < 0) { failures++; return failures; }
    if (!setup_handshake(&tp)) { failures++; test_pair_destroy(&tp); return failures; }

    /* Client subscribes; server accepts (carry the control both ways via pump). */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { ns_parts, 1 };
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = ns;
    sub_cfg.track_name = MOQ_BYTES_LITERAL("video");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t sub;
    MOQ_TEST_CHECK(moq_session_subscribe(tp.client, &sub_cfg, 0, &sub) == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);

    moq_event_t ev;
    moq_subscription_t server_sub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(tp.server, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
            server_sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(server_sub));
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(tp.server, server_sub, &acc, 0)
                   == MOQ_OK);
    pump_until_quiescent(&tp, 20, 0);
    /* Drain the client's SUBSCRIBE_OK so the (size-1) event queue starts empty. */
    while (moq_session_poll_events(tp.client, &ev, 1) > 0)
        moq_event_cleanup(&ev);

    /* Server writes obj0+obj1 to one subgroup; capture the produced uni wire
     * bytes (header + obj0 + obj1) from the server endpoint into buf1. */
    moq_subgroup_cfg_t sg_cfg;
    moq_subgroup_cfg_init(&sg_cfg);
    sg_cfg.group_id = 0;
    sg_cfg.subgroup_id = 0;
    sg_cfg.publisher_priority = 200;
    moq_subgroup_handle_t sg;
    MOQ_TEST_CHECK(moq_session_open_subgroup(tp.server, server_sub, &sg_cfg, 0, &sg)
                   == MOQ_OK);
    const char *want[3] = { "AAA", "BBB", "CCC" };
    for (int i = 0; i < 2; i++) {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)want[i], 3, &p);
        MOQ_TEST_CHECK(moq_session_write_object(tp.server, sg, (uint64_t)i, p, 0)
                       == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    fake_endpoint_clear_ops(&tp.server_ep);
    held_bridge_service(tp.server_bridge, 0);

    uint8_t buf1[512]; size_t len1 = 0;
    uint64_t uni_sid = 0; bool have_uni = false;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_OPEN_UNI) { uni_sid = o->stream_id; have_uni = true; }
        else if (o->kind == FAKE_OP_WRITE && have_uni && o->stream_id == uni_sid &&
                 len1 + o->data_len <= sizeof(buf1)) {
            memcpy(buf1 + len1, o->data, o->data_len);
            len1 += o->data_len;
        }
    }
    MOQ_TEST_CHECK(have_uni && len1 > 0);
    fake_endpoint_clear_ops(&tp.server_ep);

    /* Server writes obj2 and closes; capture obj2's bytes into buf2. */
    {
        moq_rcbuf_t *p = NULL;
        moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)want[2], 3, &p);
        MOQ_TEST_CHECK(moq_session_write_object(tp.server, sg, 2, p, 0) == MOQ_OK);
        moq_rcbuf_decref(p);
    }
    MOQ_TEST_CHECK(moq_session_close_subgroup(tp.server, sg, 0) == MOQ_OK);
    held_bridge_service(tp.server_bridge, 0);

    uint8_t buf2[512]; size_t len2 = 0;
    for (size_t i = 0; i < tp.server_ep.count; i++) {
        fake_op_t *o = &tp.server_ep.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == uni_sid &&
            len2 + o->data_len <= sizeof(buf2)) {
            memcpy(buf2 + len2, o->data, o->data_len);
            len2 += o->data_len;
        }
    }
    MOQ_TEST_CHECK(len2 > 0);

    /* Deliver buf1 (header+obj0+obj1) to the CLIENT bridge as a peer uni stream.
     * obj0 emits and fills the size-1 event queue; obj1 backs up into
     * PENDING_EMIT, so the bridge returns WOULD_BLOCK (pending_retry). */
    const uint64_t client_sid = 5000;
    moq_result_t rc = held_bridge_uni_bytes(
        tp.client_bridge, client_sid, buf1, len1, false, 0);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* While pending_retry is set, deliver obj2 + FIN on the SAME stream. The
     * bytes must be retained (not dropped) even though this also WOULD_BLOCKs. */
    rc = deliver_uni(tp.client_bridge, use_rcbuf_extra, client_sid,
                     buf2, len2, true);
    MOQ_TEST_CHECK(rc == MOQ_ERR_WOULD_BLOCK);

    /* Drain + service-retry until quiescent, collecting delivered objects. */
    bool got[3] = { false, false, false };
    for (int iter = 0; iter < 16; iter++) {
        while (moq_session_poll_events(tp.client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                uint64_t oid = ev.u.object_received.object_id;
                moq_rcbuf_t *pl = ev.u.object_received.payload;
                if (oid < 3 && pl && moq_rcbuf_len(pl) == 3 &&
                    memcmp(moq_rcbuf_data(pl), want[oid], 3) == 0)
                    got[(size_t)oid] = true;
            }
            moq_event_cleanup(&ev);
        }
        held_bridge_service(tp.client_bridge, 0);
    }

    MOQ_TEST_CHECK(got[0]);
    MOQ_TEST_CHECK(got[1]);
    MOQ_TEST_CHECK(got[2]);   /* must not be dropped during pending_retry */
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(tp.client_bridge));

    test_pair_destroy(&tp);
    return failures;
}


int test_pending_retry_keeps_bytes(void)
{
    return run_pending_retry_keeps_bytes(false);
}


int test_pending_retry_keeps_rcbuf(void)
{
    return run_pending_retry_keeps_bytes(true);
}
