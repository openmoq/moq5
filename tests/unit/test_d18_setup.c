/*
 * Draft-18 control-channel bring-up: SETUP over the unidirectional control
 * pair, through real transport bridges + fake endpoints.
 *
 * Covers: the D18 client/server reach SETUP_COMPLETE over local/peer uni
 * control channels (the bridge runs in uni-control-pair mode and never opens a
 * bidirectional control stream); a draft-16 pair stays in bidirectional
 * control mode; padding streams are discarded; and a SETUP whose stream type
 * is delivered one byte at a time still classifies and completes.
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
    moq_session_t          *sess;
    fake_endpoint_t         ep;
    moq_transport_bridge_t *bridge;
} d18_peer_t;

static int peer_init(d18_peer_t *p, moq_perspective_t persp,
                     moq_version_t version, uint64_t uni_base)
{
    memset(p, 0, sizeof(*p));
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = version;
    if (moq_session_create(&cfg, 0, &p->sess) < 0) return -1;
    fake_endpoint_init(&p->ep, uni_base, uni_base + 1000);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&bcfg, p->sess, &p->ep.vtable, &p->ep,
                                    &p->bridge) < 0) {
        moq_session_destroy(p->sess);
        return -1;
    }
    return 0;
}

static void peer_destroy(d18_peer_t *p)
{
    moq_transport_bridge_destroy(p->bridge);
    moq_session_destroy(p->sess);
}

/* Deliver a peer's recorded uni writes to the other bridge's uni inbound. */
static void deliver_uni(fake_endpoint_t *from, moq_transport_bridge_t *to,
                        uint64_t now)
{
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *o = &from->ops[i];
        if (o->kind == FAKE_OP_WRITE)
            moq_transport_bridge_on_peer_uni_bytes(to, o->stream_id, o->data,
                                                   o->data_len, o->fin, now);
    }
    fake_endpoint_clear_ops(from);
}

static bool drain_setup_complete(moq_session_t *s)
{
    moq_event_t ev;
    bool got = false;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) got = true;
        moq_event_cleanup(&ev);
    }
    return got;
}

int main(void)
{
    int failures = 0;

    /* == A. D18 SETUP handshake over the uni control pair ============= */
    {
        d18_peer_t c, sv;
        MOQ_TEST_CHECK_EQ_INT(peer_init(&c, MOQ_PERSPECTIVE_CLIENT,
                                        MOQ_VERSION_DRAFT_18, 1000), 0);
        MOQ_TEST_CHECK_EQ_INT(peer_init(&sv, MOQ_PERSPECTIVE_SERVER,
                                        MOQ_VERSION_DRAFT_18, 3000), 0);

        /* Both sides run in uni-control-pair mode. */
        MOQ_TEST_CHECK_EQ_INT((int)c.bridge->control_mode,
                              (int)BRIDGE_CONTROL_UNI_PAIR);
        MOQ_TEST_CHECK_EQ_INT((int)sv.bridge->control_mode,
                              (int)BRIDGE_CONTROL_UNI_PAIR);

        /* D18: both peers proactively open control + send SETUP. */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(c.sess, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv.sess, 0), (int)MOQ_OK);

        bool c_done = false, s_done = false;
        for (int i = 0; i < 8 && !(c_done && s_done); i++) {
            moq_transport_bridge_service(c.bridge, 0);
            moq_transport_bridge_service(sv.bridge, 0);
            deliver_uni(&c.ep, sv.bridge, 0);
            deliver_uni(&sv.ep, c.bridge, 0);
            if (drain_setup_complete(c.sess)) c_done = true;
            if (drain_setup_complete(sv.sess)) s_done = true;
        }

        MOQ_TEST_CHECK(c_done);
        MOQ_TEST_CHECK(s_done);
        MOQ_TEST_CHECK_EQ_INT((int)c.sess->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)sv.sess->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(c.bridge));
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(sv.bridge));

        /* == B. Control used the uni pair, never a bidi control stream. */
        MOQ_TEST_CHECK_EQ_SIZE(
            fake_endpoint_count_kind(&c.ep, FAKE_OP_OPEN_BIDI), 0);
        MOQ_TEST_CHECK_EQ_SIZE(
            fake_endpoint_count_kind(&sv.ep, FAKE_OP_OPEN_BIDI), 0);
        MOQ_TEST_CHECK(c.bridge->local_ctrl_uni_open);
        MOQ_TEST_CHECK(c.bridge->peer_ctrl_uni_open);

        peer_destroy(&c);
        peer_destroy(&sv);
    }

    /* == D16 stays in bidirectional control mode (unchanged) ========== */
    {
        d18_peer_t c;
        MOQ_TEST_CHECK_EQ_INT(peer_init(&c, MOQ_PERSPECTIVE_CLIENT,
                                        MOQ_VERSION_DRAFT_16, 1000), 0);
        MOQ_TEST_CHECK_EQ_INT((int)c.bridge->control_mode,
                              (int)BRIDGE_CONTROL_BIDI);
        peer_destroy(&c);
    }

    /* == C. Padding stream is recognized and discarded ================ */
    {
        d18_peer_t sv;
        MOQ_TEST_CHECK_EQ_INT(peer_init(&sv, MOQ_PERSPECTIVE_SERVER,
                                        MOQ_VERSION_DRAFT_18, 3000), 0);

        uint8_t pad[16];
        size_t n = moq_vi64_encode(MOQ_D18_STREAM_PADDING, pad, sizeof(pad));
        pad[n++] = 0x00;  /* a byte of padding payload (must be discarded) */

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_transport_bridge_on_peer_uni_bytes(sv.bridge, 5000,
                pad, n, false, 0), (int)MOQ_OK);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(sv.bridge));
        /* Padding must not generate any session event. */
        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv.sess, &ev, 1), 0);

        peer_destroy(&sv);
    }

    /* == D. SETUP stream type delivered one byte at a time ============ */
    {
        d18_peer_t sv;
        MOQ_TEST_CHECK_EQ_INT(peer_init(&sv, MOQ_PERSPECTIVE_SERVER,
                                        MOQ_VERSION_DRAFT_18, 3000), 0);
        /* Server sends its own SETUP first (so completion needs only the
         * peer SETUP we feed below). */
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv.sess, 0), (int)MOQ_OK);
        moq_transport_bridge_service(sv.bridge, 0);

        uint8_t setup[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_setup(&w), (int)MOQ_OK);
        size_t slen = moq_buf_writer_offset(&w);
        /* The 0x2F00 type is a 2-byte vi64, so the first byte alone must
         * classify as NEED_MORE and be retained without error. */
        for (size_t i = 0; i < slen; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_transport_bridge_on_peer_uni_bytes(sv.bridge, 7000,
                    &setup[i], 1, false, 0), (int)MOQ_OK);
            MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(sv.bridge));
        }

        MOQ_TEST_CHECK(drain_setup_complete(sv.sess));
        MOQ_TEST_CHECK_EQ_INT((int)sv.sess->state, (int)MOQ_SESS_ESTABLISHED);

        peer_destroy(&sv);
    }

    /* == E. A non-SETUP control message closes the session =========== */
    {
        d18_peer_t sv;
        MOQ_TEST_CHECK_EQ_INT(peer_init(&sv, MOQ_PERSPECTIVE_SERVER,
                                        MOQ_VERSION_DRAFT_18, 3000), 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv.sess, 0), (int)MOQ_OK);
        moq_transport_bridge_service(sv.bridge, 0);

        /* SETUP first establishes the peer control channel (stream 7100
         * classified as control). */
        uint8_t setup[16];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, setup, sizeof(setup));
        moq_d18_encode_setup(&sw);
        moq_transport_bridge_on_peer_uni_bytes(sv.bridge, 7100, setup,
            moq_buf_writer_offset(&sw), false, 0);
        MOQ_TEST_CHECK_EQ_INT((int)sv.sess->state, (int)MOQ_SESS_ESTABLISHED);

        /* A following non-SETUP control message on the control channel (vi64
         * type 0x10 + 16-bit zero length) is unsupported -> protocol close. */
        uint8_t ctrl[8];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ctrl, sizeof(ctrl));
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&w, 0x10), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_uint16(&w, 0), (int)MOQ_OK);
        moq_transport_bridge_on_peer_uni_bytes(sv.bridge, 7100, ctrl,
            moq_buf_writer_offset(&w), false, 0);

        MOQ_TEST_CHECK_EQ_INT((int)sv.sess->state, (int)MOQ_SESS_CLOSED);
        peer_destroy(&sv);
    }

    /* == F. FIN on the peer control channel closes the session ======== */
    {
        d18_peer_t sv;
        MOQ_TEST_CHECK_EQ_INT(peer_init(&sv, MOQ_PERSPECTIVE_SERVER,
                                        MOQ_VERSION_DRAFT_18, 3000), 0);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(sv.sess, 0), (int)MOQ_OK);
        moq_transport_bridge_service(sv.bridge, 0);

        /* Deliver a SETUP then FIN the control channel; the transport-level
         * close of a control stream terminates the session. */
        uint8_t setup[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        moq_d18_encode_setup(&w);
        moq_transport_bridge_on_peer_uni_bytes(sv.bridge, 7200, setup,
            moq_buf_writer_offset(&w), false, 0);
        MOQ_TEST_CHECK_EQ_INT((int)sv.sess->state, (int)MOQ_SESS_ESTABLISHED);

        moq_transport_bridge_on_peer_uni_bytes(sv.bridge, 7200, NULL, 0,
            true /* fin */, 0);
        /* The session was notified of the control-stream close. */
        MOQ_TEST_CHECK(moq_transport_bridge_is_terminal(sv.bridge) ||
                       sv.bridge->needs_close);
        peer_destroy(&sv);
    }

    MOQ_TEST_PASS("d18_setup");
    return failures != 0;
}
