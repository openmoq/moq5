/*
 * Draft-18 inbound SUBSCRIBE on a request bidi stream (server side): the core
 * reserves a MOQ_SUB_RECVING_REQUEST slot, buffers the (possibly fragmented)
 * request, the profile decodes/validates/dispatches it, and a SUBSCRIBE_REQUEST
 * event is emitted. Covers single-read, fragmented, truncated-FIN cleanup,
 * fail-closed slot cleanup on malformed/unsupported/bad-parity requests,
 * post-commit fail-closed, and the backpressure retry contract.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static moq_session_t *make_server(uint32_t max_events)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    if (moq_session_start(s, 0) < 0) { moq_session_destroy(s); return NULL; }
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    return s;
}

static moq_session_t *make_established_d18_server(void) { return make_server(0); }

/* Encode a SUBSCRIBE message for namespace "live"/track into buf. */
static size_t make_subscribe(uint8_t *buf, size_t cap, uint64_t request_id,
                             const char *track)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_bytes_t tn = { (const uint8_t *)track, strlen(track) };
    moq_d18_msg_params_t mp = {0};
    moq_d18_encode_subscribe(&w, request_id, &ns, tn, &mp);
    return moq_buf_writer_offset(&w);
}

static bool got_subscribe_request(moq_session_t *s)
{
    moq_event_t ev;
    bool got = false;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) got = true;
        moq_event_cleanup(&ev);
    }
    return got;
}

/* Entry state behind a request stream_ref (or -1 if no slot is bound). */
static int slot_state_for(moq_session_t *s, moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind == MOQ_REQ_NONE) return -1;
    return (int)s->subs[ep.slot].state;
}

int main(void)
{
    int failures = 0;

    /* == A. Single-read inbound SUBSCRIBE -> SUBSCRIBE_REQUEST ======== */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB001);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        /* Registered by stream-ref, committed to PENDING_PUBLISHER. */
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_PENDING_PUBLISHER);
        moq_session_destroy(s);
    }

    /* == B. Fragmented inbound SUBSCRIBE (one byte at a time) ========= */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB002);
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1,
                    false, 1), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        MOQ_TEST_CHECK(got_subscribe_request(s));
        moq_session_destroy(s);
    }

    /* == C. Truncated FIN frees the reserved slot + stream-ref key ==== */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB003);
        /* Deliver only the first 2 bytes, then FIN: incomplete -> close. */
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            n > 2 ? 2 : 1, true /* fin */, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* key removed */
        moq_session_destroy(s);
    }

    /* == D. Duplicate track -> REQUEST_ERROR on the request bidi ====== */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t m1[128];
        size_t n1 = make_subscribe(m1, sizeof(m1), 0, "video");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s,
                moq_stream_ref_from_u64(0xB010), m1, n1, false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

        /* Same track, next parity request id, on a different bidi stream. */
        uint8_t m2[128];
        size_t n2 = make_subscribe(m2, sizeof(m2), 2, "video");
        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(0xB011);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, m2, n2, false, 1),
            (int)MOQ_OK);
        /* Session stays open; the rejection is a REQUEST_ERROR delivered on the
         * request bidi (not a control-channel error), and the reserved slot is
         * freed (stream-ref key removed). */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref2), -1);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK_EQ_U64(act.u.send_bidi_stream.stream_ref._v, ref2._v);
        MOQ_TEST_CHECK(act.u.send_bidi_stream.fin);
        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                            act.u.send_bidi_stream.len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_ERROR);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == E. Unsupported request type frees the reserved slot ========== */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        /* Complete envelope: vi64 type 0x20 (not SUBSCRIBE) + u16 length 0. */
        uint8_t msg[] = { 0x20, 0x00, 0x00 };
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB020);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, sizeof(msg),
                                               false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* slot freed */
        moq_session_destroy(s);
    }

    /* == F. Wrong request-ID parity frees the reserved slot =========== */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        /* Server expects even peer request ids; request id 1 is wrong parity. */
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 1, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB030);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* slot freed */
        moq_session_destroy(s);
    }

    /* == G. Post-commit bytes on the request stream fail closed ======= */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB040);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        /* One extra byte after the committed request is not yet handled. */
        uint8_t extra = 0x00;
        (void)moq_session_on_bidi_stream_bytes(s, ref, &extra, 1, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == H. Backpressure: WOULD_BLOCK keeps the slot; re-feed commits == */
    {
        moq_session_t *s = make_server(1);   /* event queue holds one entry */
        MOQ_TEST_CHECK(s != NULL);

        /* First inbound SUBSCRIBE commits and fills the event queue. */
        uint8_t m1[128];
        size_t n1 = make_subscribe(m1, sizeof(m1), 0, "video");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s,
                moq_stream_ref_from_u64(0xB050), m1, n1, false, 1), (int)MOQ_OK);

        /* Second inbound SUBSCRIBE: event queue full -> WOULD_BLOCK, slot stays
         * reserved in RECVING_REQUEST, no session close. */
        uint8_t m2[128];
        size_t n2 = make_subscribe(m2, sizeof(m2), 2, "audio");
        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(0xB051);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, m2, n2, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref2),
                              (int)MOQ_SUB_RECVING_REQUEST);

        /* Drain events to free queue capacity, then re-feed the retry signal
         * (NULL, 0, !fin) -- no bytes appended; the buffered request dispatches. */
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, NULL, 0, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref2),
                              (int)MOQ_SUB_PENDING_PUBLISHER);
        moq_session_destroy(s);
    }

    /* == I. Peer STOP_SENDING on the request bidi tears down the sub === *
     *  A peer cancelling its subscription STOP_SENDINGs our send half of the
     *  request bidi. That distinct input (not a RESET) must free the inbound
     *  subscription, emit UNSUBSCRIBED, and remove the stream-ref key, with no
     *  session close. */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB060);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_PENDING_PUBLISHER);

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_stop(s, ref, 0x1, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* freed */

        moq_event_t ev;
        bool unsub = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) unsub = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(unsub);
        moq_session_destroy(s);
    }

    /* == J. Peer RESET_STREAM on the request bidi tears down the sub === *
     *  The reset path reaches the same teardown through a separate input. */
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB070);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));

        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);

        moq_event_t ev;
        bool unsub = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) unsub = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(unsub);
        moq_session_destroy(s);
    }

    /* == K. Early arrival (§3.3) + tight event queue: the deferred refeed *
     *  retries on event drain. A request bidi delivered BEFORE the peer's
     *  SETUP is buffered; with max_events=1 the SETUP_COMPLETE pushed at
     *  establishment fills the only event slot, so the establishment-time
     *  refeed WOULD_BLOCKs. There is no bridge retry for the accepted bytes
     *  and the peer sends nothing more on the stream -- draining the event
     *  queue must re-dispatch the deferred request, or it is stranded in
     *  MOQ_SUB_RECVING_REQUEST forever. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_events = 1;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &s) >= 0);
        MOQ_TEST_CHECK(moq_session_start(s, 0) >= 0);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

        /* Request bidi BEFORE the peer SETUP: accepted + buffered (§3.3). */
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "early");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB080);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_RECVING_REQUEST);

        /* Peer SETUP establishes; SETUP_COMPLETE fills the single event
         * slot, so the refeed defers again (no event surfaced yet). */
        uint8_t setup[16];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        moq_d18_encode_setup(&w);
        MOQ_TEST_CHECK(moq_session_on_control_bytes(
            s, setup, moq_buf_writer_offset(&w), 1) >= 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_RECVING_REQUEST);

        /* Drain SETUP_COMPLETE: the freed capacity re-dispatches the
         * deferred request; SUBSCRIBE_REQUEST surfaces on the next poll. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(s, &ev, 1) == 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SETUP_COMPLETE);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_inbound_subscribe");
    return failures != 0;
}
