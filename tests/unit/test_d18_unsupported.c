/*
 * After a draft-18 session establishes (control bring-up), the public request
 * and data APIs become reachable even though their profile ops are not yet
 * implemented. This test proves each such path fails cleanly -- returns an
 * error or closes the session -- rather than dereferencing a NULL op.
 *
 * Run under ASAN: a NULL-op crash would show up here.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* Build a draft-18 session and drive it to ESTABLISHED by sending our SETUP
 * (start) and feeding a peer SETUP on the control-bytes path. */
static moq_session_t *make_established_d18(void)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cfg.version = MOQ_VERSION_DRAFT_18;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;

    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

    uint8_t buf[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, buf, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    return s;
}

int main(void)
{
    int failures = 0;

    /* == Outbound request/control APIs fail cleanly, session survives = */
    {
        moq_session_t *s = make_established_d18();
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t ns = { ns_parts, 1 };

        /* SUBSCRIBE is implemented for draft-18 (opens its own bidi stream),
         * so it succeeds here; fetch/publish/goaway are still unimplemented. */
        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        sub.track_namespace = ns;
        sub.track_name = MOQ_BYTES_LITERAL("v");
        moq_subscription_t sh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &sub, 1, &sh),
                              (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* (SUBSCRIBE, FETCH, and PUBLISH are implemented for draft-18 and covered
         * by their own tests; the remaining outbound ops below are not yet.) */
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = ns;
        pc.track_name = MOQ_BYTES_LITERAL("v");
        moq_publication_t ph;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_publish(s, &pc, 1, &ph),
                              (int)MOQ_OK);
        { moq_action_t a; while (moq_session_poll_actions(s, &a, 1) > 0)
              moq_action_cleanup(&a); }

        /* (SUBSCRIBE / FETCH / PUBLISH / GOAWAY are implemented for draft-18 and
         * covered by their own tests; GOAWAY would drain the session, so it is
         * not exercised here.) */

        /* Capacity getters are NULL-safe stubs (return 0, no crash). */
        (void)moq_session_peer_request_capacity(s);
        (void)moq_session_local_request_capacity(s);

        /* None of the surviving outbound calls should have closed the session. */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(s);
    }

    /* == Inbound datagram on an unsupported profile: clean close ====== */
    {
        moq_session_t *s = make_established_d18();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t dg[] = { 0x00, 0x01, 0x02 };
        moq_result_t rc = moq_session_on_datagram(s, dg, sizeof(dg), 1);
        MOQ_TEST_CHECK(rc <= 0);  /* error or clean close, never a crash */
        moq_session_destroy(s);
    }

    /* == Inbound data stream: classify -> UNKNOWN -> clean close ====== */
    {
        moq_session_t *s = make_established_d18();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t hdr[] = { 0x05, 0x00 };  /* a data-stream-shaped header */
        (void)moq_session_on_data_bytes(s, moq_stream_ref_from_u64(1ULL << 63),
                                        hdr, sizeof(hdr), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Inbound unsupported request type on a bidi stream -> close === *
     * SUBSCRIBE is handled, but any other request type as the first message on
     * a request bidi is not yet accepted and must close cleanly. The envelope
     * is complete (vi64 type 0x20 + u16 length 0) so it is dispatched, not
     * buffered as incomplete. */
    {
        moq_session_t *s = make_established_d18();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t req[] = { 0x20, 0x00, 0x00 };  /* unsupported type, empty body */
        (void)moq_session_on_bidi_stream_bytes(s,
            moq_stream_ref_from_u64(1ULL << 63), req, sizeof(req), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_unsupported");
    return failures != 0;
}
