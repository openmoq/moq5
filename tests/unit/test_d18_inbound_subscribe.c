/*
 * Draft-18 inbound SUBSCRIBE on a request bidi stream (server side): the core
 * reserves a MOQ_SUB_RECVING_REQUEST slot, buffers the (possibly fragmented)
 * request, the profile decodes/validates/dispatches it, and a SUBSCRIBE_REQUEST
 * event is emitted. Covers single-read, fragmented, truncated-FIN cleanup,
 * fail-closed slot cleanup on malformed/unsupported/bad-parity requests,
 * post-commit fail-closed, backpressure retries, out-of-order Request IDs,
 * duplicate persistence, resource failure, and the GOAWAY gap boundary.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static int failures = 0;

static moq_session_t *make_peer_alloc(moq_perspective_t perspective,
                                      uint32_t max_events,
                                      const moq_alloc_t *alloc)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), alloc, perspective);
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

static moq_session_t *make_peer(moq_perspective_t perspective,
                                uint32_t max_events)
{
    return make_peer_alloc(perspective, max_events, moq_alloc_default());
}

static moq_session_t *make_server(uint32_t max_events)
{
    return make_peer(MOQ_PERSPECTIVE_SERVER, max_events);
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

static moq_result_t feed_subscribe(moq_session_t *s, uint64_t request_id,
                                   uint64_t stream_id, const char *track)
{
    uint8_t msg[128];
    size_t n = make_subscribe(msg, sizeof(msg), request_id, track);
    return moq_session_on_bidi_stream_bytes(s,
        moq_stream_ref_from_u64(stream_id), msg, n, false, 1);
}

static uint64_t poll_close_code(moq_session_t *s)
{
    uint64_t code = 0;
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_CLOSE_SESSION)
            code = a.u.close_session.code;
        moq_action_cleanup(&a);
    }
    return code;
}

static uint64_t send_goaway_request_id(moq_session_t *s)
{
    uint64_t request_id = UINT64_MAX;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_goaway(s, NULL, 0, 2), (int)MOQ_OK);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) {
        if (a.kind == MOQ_ACTION_SEND_CONTROL) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, a.u.send_control.data, a.u.send_control.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                env.msg_type == MOQ_D18_GOAWAY) {
                moq_d18_goaway_t ga = {0};
                moq_result_t rc = moq_d18_decode_goaway(
                    env.payload, env.payload_len, &ga);
                MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
                if (rc == MOQ_OK) request_id = ga.request_id;
            }
        }
        moq_action_cleanup(&a);
    }
    return request_id;
}

typedef struct fail_alloc_ctx {
    const moq_alloc_t *base;
    size_t fail_alloc_size;
    size_t fail_realloc_size;
    size_t refused;
} fail_alloc_ctx_t;

static void *test_alloc(size_t size, void *ctx)
{
    fail_alloc_ctx_t *f = (fail_alloc_ctx_t *)ctx;
    if (size == f->fail_alloc_size) {
        f->refused++;
        return NULL;
    }
    return f->base->alloc(size, f->base->ctx);
}

static void *test_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    fail_alloc_ctx_t *f = (fail_alloc_ctx_t *)ctx;
    if (new_size == f->fail_realloc_size) {
        f->refused++;
        return NULL;
    }
    return f->base->realloc(ptr, old_size, new_size, f->base->ctx);
}

static void test_free(void *ptr, size_t size, void *ctx)
{
    fail_alloc_ctx_t *f = (fail_alloc_ctx_t *)ctx;
    f->base->free(ptr, size, f->base->ctx);
}

int main(void)
{
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

    /* Requests use independent bidis. QUIC can deliver the server's later
     * streams first; the lowest not-yet-processed ID is the GOAWAY boundary. */
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 5, 0xC005, "five"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 3, 0xC003, "three"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_U64(send_goaway_request_id(s), 1);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 5, 0xC015, "five"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 1, 0xC011, "one"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_U64(send_goaway_request_id(s), 3);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 5, 0xC025, "five"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 1, 0xC021, "one"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 3, 0xC023, "three"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_U64(send_goaway_request_id(s), 7);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 131, 0xC131, "far"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 1, 0xC101, "one"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_U64(send_goaway_request_id(s), 3);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(
            s, UINT64_MAX, 0xC141, "last"), (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_U64(send_goaway_request_id(s), 1);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 5, 0xC035, "five"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        (void)feed_subscribe(s, 5, 0xC036, "duplicate");
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x4);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xC075);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 5, ref._v, "five"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_stop(
            s, ref, 0x1, 1), (int)MOQ_OK);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);
        (void)feed_subscribe(s, 5, 0xC076, "again");
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x4);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 5, 0xC085, "five"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 1, 0xC081, "one"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 3, 0xC083, "three"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        (void)feed_subscribe(s, 3, 0xC086, "again");
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x4);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        for (uint64_t id = 4; id <= 40; id += 4) {
            char track[16];
            (void)snprintf(track, sizeof(track), "track%llu",
                           (unsigned long long)id);
            MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(
                s, id, 0xD000 + id, track), (int)MOQ_OK);
            MOQ_TEST_CHECK(got_subscribe_request(s));
        }
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 0, 0xD000, "zero"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 2, 0xD002, "two"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        for (uint64_t id = 6; id <= 38; id += 4) {
            char track[16];
            (void)snprintf(track, sizeof(track), "track%llu",
                           (unsigned long long)id);
            MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(
                s, id, 0xD000 + id, track), (int)MOQ_OK);
            MOQ_TEST_CHECK(got_subscribe_request(s));
        }
        MOQ_TEST_CHECK_EQ_U64(send_goaway_request_id(s), 42);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 0);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 5, 0xC045, "five"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        (void)feed_subscribe(s, 4, 0xC044, "wrong-parity");
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x4);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_peer(MOQ_PERSPECTIVE_CLIENT, 1);
        MOQ_TEST_CHECK(s != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 5, 0xC055, "five"),
                              (int)MOQ_OK);
        /* The one event slot is full. ID 3 validates but cannot commit yet. */
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 3, "three");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xC053);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(
            s, ref, msg, n, false, 1), (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_RECVING_REQUEST);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(
            s, ref, NULL, 0, false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_U64(send_goaway_request_id(s), 1);
        moq_session_destroy(s);
    }
    {
        fail_alloc_ctx_t ctx = { .base = moq_alloc_default() };
        moq_alloc_t alloc = { &ctx, test_alloc, test_realloc, test_free };
        moq_session_t *s = make_peer_alloc(MOQ_PERSPECTIVE_CLIENT, 0, &alloc);
        MOQ_TEST_CHECK(s != NULL);
        ctx.fail_alloc_size = 8 * 2 * sizeof(uint64_t);
        MOQ_TEST_CHECK_EQ_INT((int)feed_subscribe(s, 1, 0xC061, "one"),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK(got_subscribe_request(s));
        MOQ_TEST_CHECK_EQ_SIZE(ctx.refused, 0);
        (void)feed_subscribe(s, 5, 0xC065, "five");
        MOQ_TEST_CHECK_EQ_SIZE(ctx.refused, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x1);
        MOQ_TEST_CHECK(!got_subscribe_request(s));
        moq_session_destroy(s);
    }
    {
        fail_alloc_ctx_t ctx = { .base = moq_alloc_default() };
        moq_alloc_t alloc = { &ctx, test_alloc, test_realloc, test_free };
        moq_session_t *s = make_peer_alloc(MOQ_PERSPECTIVE_SERVER, 0, &alloc);
        MOQ_TEST_CHECK(s != NULL);
        moq_request_endpoint_t ep;
        for (uint64_t i = 1; i <= 8; i++) {
            MOQ_TEST_CHECK_EQ_INT((int)s->profile->validate_inbound_request_stream(
                s, moq_stream_ref_from_u64(0xE100 + i), MOQ_D18_SUBSCRIBE,
                4 * i, &ep), (int)MOQ_OK);
            s->profile->commit_inbound_request(s, &ep);
        }
        ctx.fail_realloc_size = 16 * 2 * sizeof(uint64_t);
        (void)s->profile->validate_inbound_request_stream(
            s, moq_stream_ref_from_u64(0xE109), MOQ_D18_SUBSCRIBE, 36, &ep);
        MOQ_TEST_CHECK_EQ_SIZE(ctx.refused, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x1);
        moq_session_destroy(s);
    }
    {
        /* Exercise the private ledger limit directly; 1024 simultaneous
         * request bidis would hit unrelated session pools first. */
        moq_session_t *s = make_established_d18_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_request_endpoint_t ep;
        for (uint64_t i = 1; i <= 1024; i++) {
            uint64_t id = 4 * i;
            MOQ_TEST_CHECK_EQ_INT((int)s->profile->validate_inbound_request_stream(
                s, moq_stream_ref_from_u64(0xE000 + i), MOQ_D18_SUBSCRIBE,
                id, &ep), (int)MOQ_OK);
            s->profile->commit_inbound_request(s, &ep);
        }
        /* A bridge between two ranges frees a slot even at full capacity. */
        MOQ_TEST_CHECK_EQ_INT((int)s->profile->validate_inbound_request_stream(
            s, moq_stream_ref_from_u64(0xF006), MOQ_D18_SUBSCRIBE,
            6, &ep), (int)MOQ_OK);
        s->profile->commit_inbound_request(s, &ep);
        MOQ_TEST_CHECK_EQ_INT((int)s->profile->validate_inbound_request_stream(
            s, moq_stream_ref_from_u64(0xF100), MOQ_D18_SUBSCRIBE,
            4100, &ep), (int)MOQ_OK);
        s->profile->commit_inbound_request(s, &ep);
        (void)s->profile->validate_inbound_request_stream(
            s, moq_stream_ref_from_u64(0xF104), MOQ_D18_SUBSCRIBE,
            4104, &ep);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        MOQ_TEST_CHECK_EQ_U64(poll_close_code(s), 0x1);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_inbound_subscribe");
    return failures != 0;
}
