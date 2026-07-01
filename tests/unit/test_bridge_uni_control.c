/*
 * Focused white-box tests for the bridge unidirectional control-channel seam:
 *   - control-channel mode (default bidirectional vs uni-control-pair)
 *   - outbound MOQ_ACTION_OPEN_UNI_CONTROL / MOQ_ACTION_SEND_UNI_CONTROL
 *     dispatch through the bridge to a fake endpoint
 *   - bridge_route_peer_uni() classification-routing decision + single-vs-
 *     second peer control-channel acceptance state
 *
 * This is the bridge half of a version-neutral seam for profiles that carry
 * control on a local/peer unidirectional channel pair. Draft-16 uses a
 * bidirectional control channel, so its bridge stays in the default mode and
 * never emits the uni-control actions. The live inbound peer-uni path (with
 * NEED_MORE buffering and classification) is exercised here through a real
 * draft-18 session in section H; the deterministic real-transport coverage
 * lives in the adapter sim tests.
 */
#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include <moq/control_d18.h>
#include <moq/vi64.h>
#include "test_support.h"
#include "../support/fake_endpoint.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include "../../core/src/session/session_internal.h"

typedef struct {
    moq_session_t          *session;
    fake_endpoint_t         ep;
    moq_transport_bridge_t *bridge;
} uc_fixture_t;

static int uc_init(uc_fixture_t *f)
{
    memset(f, 0, sizeof(*f));
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    if (moq_session_create(&cfg, 0, &f->session) < 0) return -1;
    fake_endpoint_init(&f->ep, 1000, 2000);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&bcfg, f->session, &f->ep.vtable,
                                     &f->ep, &f->bridge) < 0) {
        moq_session_destroy(f->session);
        return -1;
    }
    return 0;
}

static void uc_destroy(uc_fixture_t *f)
{
    moq_transport_bridge_destroy(f->bridge);
    moq_session_destroy(f->session);
}

int main(void)
{
    int failures = 0;

    static const uint8_t setup_bytes[] = { 0xCA, 0xFE, 0xBA, 0xBE };
    static const uint8_t more_bytes[]  = { 0x11, 0x22 };

    /* == A. Draft-16 bridge defaults to bidirectional control mode ==== */
    {
        uc_fixture_t f;
        MOQ_TEST_CHECK_EQ_INT(uc_init(&f), 0);
        MOQ_TEST_CHECK_EQ_INT((int)f.bridge->control_mode,
                              (int)BRIDGE_CONTROL_BIDI);
        MOQ_TEST_CHECK(!f.bridge->local_ctrl_uni_open);
        MOQ_TEST_CHECK(!f.bridge->peer_ctrl_uni_open);
        uc_destroy(&f);
    }

    /* == B. Outbound OPEN_UNI_CONTROL + SEND_UNI_CONTROL dispatch ====== *
     *  Inject the actions into the session queue (no profile emits them
     *  yet) and service the bridge; the fake endpoint should see an
     *  open_uni followed by writes of the bytes, and the bridge should
     *  record the local control channel. */
    {
        uc_fixture_t f;
        MOQ_TEST_CHECK_EQ_INT(uc_init(&f), 0);

        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x9001);

        moq_action_t open_act;
        memset(&open_act, 0, sizeof(open_act));
        open_act.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
        open_act.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
        open_act.borrow_epoch = f.session->borrow_epoch;
        open_act.u.open_uni_control.stream_ref = ref;
        open_act.u.open_uni_control.data = setup_bytes;
        open_act.u.open_uni_control.len  = sizeof(setup_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &open_act),
                              (int)MOQ_OK);

        moq_action_t send_act;
        memset(&send_act, 0, sizeof(send_act));
        send_act.kind = MOQ_ACTION_SEND_UNI_CONTROL;
        send_act.detail_size = (uint32_t)sizeof(moq_send_uni_control_action_t);
        send_act.borrow_epoch = f.session->borrow_epoch;
        send_act.u.send_uni_control.stream_ref = ref;
        send_act.u.send_uni_control.data = more_bytes;
        send_act.u.send_uni_control.len  = sizeof(more_bytes);
        send_act.u.send_uni_control.fin  = false;
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &send_act),
                              (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.bridge));

        /* One uni opened, two writes recorded, in order. */
        MOQ_TEST_CHECK_EQ_SIZE(
            fake_endpoint_count_kind(&f.ep, FAKE_OP_OPEN_UNI), 1);
        MOQ_TEST_CHECK_EQ_SIZE(
            fake_endpoint_count_kind(&f.ep, FAKE_OP_WRITE), 2);
        MOQ_TEST_CHECK(f.ep.count >= 3);
        MOQ_TEST_CHECK_EQ_INT((int)f.ep.ops[0].kind, (int)FAKE_OP_OPEN_UNI);
        uint64_t uni_id = f.ep.ops[0].stream_id;
        MOQ_TEST_CHECK_EQ_INT((int)f.ep.ops[1].kind, (int)FAKE_OP_WRITE);
        MOQ_TEST_CHECK_EQ_U64(f.ep.ops[1].stream_id, uni_id);
        MOQ_TEST_CHECK_EQ_SIZE(f.ep.ops[1].data_len, sizeof(setup_bytes));
        MOQ_TEST_CHECK(memcmp(f.ep.ops[1].data, setup_bytes,
                              sizeof(setup_bytes)) == 0);
        MOQ_TEST_CHECK_EQ_INT((int)f.ep.ops[2].kind, (int)FAKE_OP_WRITE);
        MOQ_TEST_CHECK_EQ_U64(f.ep.ops[2].stream_id, uni_id);
        MOQ_TEST_CHECK_EQ_SIZE(f.ep.ops[2].data_len, sizeof(more_bytes));
        MOQ_TEST_CHECK(memcmp(f.ep.ops[2].data, more_bytes,
                              sizeof(more_bytes)) == 0);

        /* Bridge recorded the local control channel. */
        MOQ_TEST_CHECK(f.bridge->local_ctrl_uni_open);
        MOQ_TEST_CHECK_EQ_U64(f.bridge->local_ctrl_uni_stream_id, uni_id);
        MOQ_TEST_CHECK_EQ_U64(f.bridge->local_ctrl_uni_ref._v, 0x9001);

        /* Servicing again with no new actions produces no further ops. */
        fake_endpoint_clear_ops(&f.ep);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(f.ep.count, 0);

        uc_destroy(&f);
    }

    /* == C. bridge_route_peer_uni decision + control acceptance ======= */
    {
        uc_fixture_t f;
        MOQ_TEST_CHECK_EQ_INT(uc_init(&f), 0);
        moq_transport_bridge_t *b = f.bridge;

        MOQ_TEST_CHECK_EQ_INT(
            (int)bridge_route_peer_uni(b, MOQ_UNI_CLASS_DATA, 7),
            (int)BRIDGE_UNI_ROUTE_DATA);
        MOQ_TEST_CHECK_EQ_INT(
            (int)bridge_route_peer_uni(b, MOQ_UNI_CLASS_PADDING, 7),
            (int)BRIDGE_UNI_ROUTE_HANDLED);
        MOQ_TEST_CHECK_EQ_INT(
            (int)bridge_route_peer_uni(b, MOQ_UNI_CLASS_NEED_MORE, 7),
            (int)BRIDGE_UNI_ROUTE_NEED_MORE);
        MOQ_TEST_CHECK_EQ_INT(
            (int)bridge_route_peer_uni(b, MOQ_UNI_CLASS_UNKNOWN, 7),
            (int)BRIDGE_UNI_ROUTE_FATAL);

        /* First CONTROL accepted and recorded. */
        MOQ_TEST_CHECK(!b->peer_ctrl_uni_open);
        MOQ_TEST_CHECK_EQ_INT(
            (int)bridge_route_peer_uni(b, MOQ_UNI_CLASS_CONTROL, 10),
            (int)BRIDGE_UNI_ROUTE_HANDLED);
        MOQ_TEST_CHECK(b->peer_ctrl_uni_open);
        MOQ_TEST_CHECK_EQ_U64(b->peer_ctrl_uni_stream_id, 10);

        /* Same channel again: still handled. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)bridge_route_peer_uni(b, MOQ_UNI_CLASS_CONTROL, 10),
            (int)BRIDGE_UNI_ROUTE_HANDLED);

        /* A second, distinct control channel: fatal route. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)bridge_route_peer_uni(b, MOQ_UNI_CLASS_CONTROL, 20),
            (int)BRIDGE_UNI_ROUTE_FATAL);

        /* The helper is a pure decision: it never trips the bridge fatal. */
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(b));

        uc_destroy(&f);
    }

    /* == D. Pending SEND_UNI_CONTROL(fin) clears local control state == *
     *  Open the channel, then make the FIN write hit WOULD_BLOCK; on retry
     *  the local control state must clear and the stream must retire. */
    {
        uc_fixture_t f;
        MOQ_TEST_CHECK_EQ_INT(uc_init(&f), 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x9100);

        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
        a.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
        a.borrow_epoch = f.session->borrow_epoch;
        a.u.open_uni_control.stream_ref = ref;
        a.u.open_uni_control.data = setup_bytes;
        a.u.open_uni_control.len  = sizeof(setup_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &a), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(f.bridge->local_ctrl_uni_open);
        MOQ_TEST_CHECK_EQ_SIZE(moq_transport_bridge_stream_count(f.bridge), 1);
        fake_endpoint_clear_ops(&f.ep);

        /* FIN send blocks -> queued; channel still believed open. */
        f.ep.block_write = true;
        moq_action_t s;
        memset(&s, 0, sizeof(s));
        s.kind = MOQ_ACTION_SEND_UNI_CONTROL;
        s.detail_size = (uint32_t)sizeof(moq_send_uni_control_action_t);
        s.borrow_epoch = f.session->borrow_epoch;
        s.u.send_uni_control.stream_ref = ref;
        s.u.send_uni_control.data = more_bytes;
        s.u.send_uni_control.len  = sizeof(more_bytes);
        s.u.send_uni_control.fin  = true;
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &s), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(f.bridge->local_ctrl_uni_open);
        MOQ_TEST_CHECK(moq_transport_bridge_has_pending(f.bridge));

        /* Unblock and retry: FIN completes. */
        f.ep.block_write = false;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.bridge));
        const fake_op_t *w = fake_endpoint_find(&f.ep, FAKE_OP_WRITE);
        MOQ_TEST_CHECK(w != NULL);
        MOQ_TEST_CHECK(w && w->fin);
        MOQ_TEST_CHECK(!f.bridge->local_ctrl_uni_open);   /* state cleared */
        MOQ_TEST_CHECK_EQ_SIZE(moq_transport_bridge_stream_count(f.bridge), 0);

        uc_destroy(&f);
    }

    /* == E. Duplicate local OPEN_UNI_CONTROL is rejected, not silent == */
    {
        uc_fixture_t f;
        MOQ_TEST_CHECK_EQ_INT(uc_init(&f), 0);

        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
        a.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
        a.borrow_epoch = f.session->borrow_epoch;
        a.u.open_uni_control.stream_ref = moq_stream_ref_from_u64(0xA001);
        a.u.open_uni_control.data = setup_bytes;
        a.u.open_uni_control.len  = sizeof(setup_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &a), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(f.bridge->local_ctrl_uni_open);

        /* Second open attempt: must not open a second uni nor silently
         * replace the recorded channel — it is an internal error. */
        moq_action_t a2;
        memset(&a2, 0, sizeof(a2));
        a2.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
        a2.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
        a2.borrow_epoch = f.session->borrow_epoch;
        a2.u.open_uni_control.stream_ref = moq_stream_ref_from_u64(0xA002);
        a2.u.open_uni_control.data = more_bytes;
        a2.u.open_uni_control.len  = sizeof(more_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &a2), (int)MOQ_OK);
        moq_transport_bridge_service(f.bridge, 0);

        MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(f.bridge));
        /* Exactly one uni was ever opened (no silent second channel). */
        MOQ_TEST_CHECK_EQ_SIZE(
            fake_endpoint_count_kind(&f.ep, FAKE_OP_OPEN_UNI), 1);

        uc_destroy(&f);
    }

    /* == F. Bridge selects UNI_PAIR mode from the profile capability == *
     *  Bind a fake profile declaring uni-control topology before bridge
     *  creation; restore the real profile before session teardown so its
     *  co-allocated state is destroyed correctly. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s),
                              (int)MOQ_OK);

        const moq_profile_ops_t *real = s->profile;
        moq_profile_ops_t fake_ops;
        memset(&fake_ops, 0, sizeof(fake_ops));
        fake_ops.uses_uni_control_channel = true;  /* only field create reads */
        s->profile = &fake_ops;

        fake_endpoint_t ep;
        fake_endpoint_init(&ep, 1000, 2000);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *bridge = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                             &bridge), (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT((int)bridge->control_mode,
                              (int)BRIDGE_CONTROL_UNI_PAIR);

        s->profile = real;   /* restore before destroy */
        moq_transport_bridge_destroy(bridge);
        moq_session_destroy(s);
    }

    /* == G. RESET_BIDI_STREAM / STOP_BIDI_STREAM dispatch ============== *
     *  Open a request bidi, then inject the two teardown actions on its
     *  ref. The bridge must translate them to stop_sending and reset_stream
     *  on the bidi's transport stream (not fatal, not a data-stream op). */
    {
        uc_fixture_t f;
        MOQ_TEST_CHECK_EQ_INT(uc_init(&f), 0);

        static const uint8_t req_bytes[] = { 0xAA, 0xBB };
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0x7700);

        moq_action_t open_act;
        memset(&open_act, 0, sizeof(open_act));
        open_act.kind = MOQ_ACTION_OPEN_BIDI_STREAM;
        open_act.detail_size = (uint32_t)sizeof(moq_open_bidi_stream_action_t);
        open_act.borrow_epoch = f.session->borrow_epoch;
        open_act.u.open_bidi_stream.stream_ref = ref;
        open_act.u.open_bidi_stream.data = req_bytes;
        open_act.u.open_bidi_stream.len  = sizeof(req_bytes);
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &open_act),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        const fake_op_t *ob = fake_endpoint_find(&f.ep, FAKE_OP_OPEN_BIDI);
        MOQ_TEST_CHECK(ob != NULL);
        uint64_t bidi_id = ob->stream_id;
        fake_endpoint_clear_ops(&f.ep);

        /* STOP first (required cancellation signal), then RESET. */
        moq_action_t stop_act;
        memset(&stop_act, 0, sizeof(stop_act));
        stop_act.kind = MOQ_ACTION_STOP_BIDI_STREAM;
        stop_act.detail_size = (uint32_t)sizeof(moq_stop_bidi_stream_action_t);
        stop_act.borrow_epoch = f.session->borrow_epoch;
        stop_act.u.stop_bidi_stream.stream_ref = ref;
        stop_act.u.stop_bidi_stream.error_code = 0x1;
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &stop_act),
                              (int)MOQ_OK);

        moq_action_t reset_act;
        memset(&reset_act, 0, sizeof(reset_act));
        reset_act.kind = MOQ_ACTION_RESET_BIDI_STREAM;
        reset_act.detail_size = (uint32_t)sizeof(moq_reset_bidi_stream_action_t);
        reset_act.borrow_epoch = f.session->borrow_epoch;
        reset_act.u.reset_bidi_stream.stream_ref = ref;
        reset_act.u.reset_bidi_stream.error_code = 0x2;
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &reset_act),
                              (int)MOQ_OK);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.bridge));

        const fake_op_t *st = fake_endpoint_find(&f.ep, FAKE_OP_STOP);
        MOQ_TEST_CHECK(st != NULL);
        MOQ_TEST_CHECK_EQ_U64(st->stream_id, bidi_id);
        MOQ_TEST_CHECK_EQ_U64(st->error_code, 0x1);
        const fake_op_t *rs = fake_endpoint_find(&f.ep, FAKE_OP_RESET);
        MOQ_TEST_CHECK(rs != NULL);
        MOQ_TEST_CHECK_EQ_U64(rs->stream_id, bidi_id);
        MOQ_TEST_CHECK_EQ_U64(rs->error_code, 0x2);

        uc_destroy(&f);
    }

    /* == H. Peer RESET of its uni control channel terminates ========== *
     *  A real draft-18 session: an inbound peer uni carrying the SETUP
     *  stream type classifies as CONTROL through the live inbound path;
     *  a subsequent peer RESET_STREAM of that stream must terminate the
     *  session (the control channel lives for the session) -- never be
     *  swallowed as an unknown data-stream reset. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        cfg.version = MOQ_VERSION_DRAFT_18;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s),
                              (int)MOQ_OK);

        fake_endpoint_t ep;
        fake_endpoint_init(&ep, 1000, 2000);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *bridge = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                             &bridge), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_uses_uni_control(bridge));

        /* Peer opens its control channel: stream type alone is enough to
         * classify (the session retains it as a partial control message). */
        uint8_t type_buf[9];
        size_t tn = moq_vi64_encode(MOQ_D18_STREAM_SETUP, type_buf,
                                    sizeof(type_buf));
        MOQ_TEST_CHECK(tn > 0);
        moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
            bridge, 3, type_buf, tn, false, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(bridge));

        /* Peer resets its control stream: session over. The close is
         * recorded immediately (terminal) and dispatched to the endpoint
         * on the next service pass, exactly like a control-channel FIN. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_stream_reset(bridge, 3, 0x0, 0),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_has_pending(bridge));
        moq_transport_bridge_service(bridge, 0);
        MOQ_TEST_CHECK(moq_transport_bridge_is_closed(bridge));
        MOQ_TEST_CHECK_EQ_U64(moq_transport_bridge_close_code(bridge), 0x3);

        moq_transport_bridge_destroy(bridge);
        moq_session_destroy(s);
    }

    /* == I. Peer STOP_SENDING of our LOCAL uni control channel terminates = *
     *  A real, ESTABLISHED draft-18 client: start the session (the profile
     *  opens the local control uni and sends CLIENT SETUP), feed the peer SETUP
     *  on its uni control stream to reach MOQ_SESS_ESTABLISHED, then deliver a
     *  peer STOP_SENDING of our local control uni. That refuses our control
     *  channel and must be a fatal teardown (close code 0x3) -- it must not be
     *  routed through the generic local-origin uni path (moq_session_on_data_stop
     *  on the ESTABLISHED session returns MOQ_OK for the non-subgroup ref and
     *  would silently leave the session established). Mirrors section H's peer
     *  RESET. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        cfg.version = MOQ_VERSION_DRAFT_18;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s),
                              (int)MOQ_OK);

        fake_endpoint_t ep;
        fake_endpoint_init(&ep, 1000, 2000);
        moq_transport_bridge_cfg_t bcfg;
        moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
        moq_transport_bridge_t *bridge = NULL;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_create(&bcfg, s, &ep.vtable, &ep,
                                             &bridge), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_uses_uni_control(bridge));

        /* Start: the D18 profile opens our local control uni and writes CLIENT
         * SETUP; servicing flushes it to the transport. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(bridge, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(bridge->local_ctrl_uni_open);
        uint64_t local_uni_id = bridge->local_ctrl_uni_stream_id;

        /* Feed the peer SETUP on its uni control stream -> ESTABLISHED.
         * moq_d18_encode_setup() emits the full framed SETUP incl. the
         * MOQ_D18_STREAM_SETUP type, classifying the stream as control. */
        uint8_t setup[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_uni_bytes(
                bridge, 7000, setup, moq_buf_writer_offset(&w), false, 0),
            (int)MOQ_OK);
        { moq_event_t ev;
          while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev); }
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(bridge));

        /* Peer STOP_SENDING our control uni: session over (close code 0x3).
         * It must not be routed to the established session's data-stop (which
         * would return MOQ_OK and leave the bridge open / not pending). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_stop_sending(
                bridge, local_uni_id, 0x0, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_has_pending(bridge));
        moq_transport_bridge_service(bridge, 0);
        MOQ_TEST_CHECK(moq_transport_bridge_is_closed(bridge));
        MOQ_TEST_CHECK_EQ_U64(moq_transport_bridge_close_code(bridge), 0x3);

        moq_transport_bridge_destroy(bridge);
        moq_session_destroy(s);
    }

    /* == J. An ordinary LOCAL data uni is NOT marked control ============== *
     *  Guard against over-broadening: a local-origin uni DATA stream (opened
     *  for subgroup data) carries no control disposition, so the new
     *  STOP_SENDING control-teardown branch (gated on uni_disp == CONTROL) is
     *  skipped for it and it still routes through the generic data-stop path.
     *  Asserted white-box on the stream entry (the data-stop routing on an
     *  established session is covered by the transport_bridge suite). */
    {
        uc_fixture_t f;
        MOQ_TEST_CHECK_EQ_INT(uc_init(&f), 0);

        /* Open a local data uni by sending a subgroup data object on a fresh
         * ref; servicing opens the transport uni. */
        moq_rcbuf_t *payload = NULL;
        static const uint8_t obj[] = { 0xDE, 0xAD, 0xBE, 0xEF };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_rcbuf_create(moq_alloc_default(), obj, sizeof(obj),
                                  &payload), (int)MOQ_OK);
        moq_stream_ref_t dref = moq_stream_ref_from_u64(0x7301);
        moq_action_t sd;
        memset(&sd, 0, sizeof(sd));
        sd.kind = MOQ_ACTION_SEND_DATA;
        sd.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        sd.borrow_epoch = f.session->borrow_epoch;
        sd.u.send_data.stream_ref = dref;
        sd.u.send_data.header[0] = 0x04;     /* a subgroup header byte */
        sd.u.send_data.header_len = 1;
        sd.u.send_data.payload = payload;    /* OWNED ref, bridge cleans up */
        sd.u.send_data.fin = false;
        MOQ_TEST_CHECK_EQ_INT((int)push_action(f.session, &sd), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_service(f.bridge, 0), (int)MOQ_OK);
        const fake_op_t *ou = fake_endpoint_find(&f.ep, FAKE_OP_OPEN_UNI);
        MOQ_TEST_CHECK(ou != NULL);
        uint64_t data_uni_id = ou->stream_id;
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(f.bridge));

        /* The data uni is a local-origin UNI but NOT control -> STOP_SENDING
         * would take the data-stop path, never the control-teardown branch. */
        bridge_stream_entry_t *de = bridge_find_by_id(f.bridge, data_uni_id);
        MOQ_TEST_CHECK(de != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)de->kind, (int)BRIDGE_STREAM_UNI);
        MOQ_TEST_CHECK_EQ_INT((int)de->origin, (int)BRIDGE_ORIGIN_LOCAL);
        MOQ_TEST_CHECK(de->uni_disp != BRIDGE_UNI_DISP_CONTROL);

        uc_destroy(&f);
    }

    MOQ_TEST_PASS("bridge_uni_control");
    return failures != 0;
}
