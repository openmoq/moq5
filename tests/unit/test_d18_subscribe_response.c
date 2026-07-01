/*
 * Draft-18 SUBSCRIBE responses on the request bidi stream (both directions):
 *   - Subscriber side: SUBSCRIBE_OK / REQUEST_ERROR arriving on our own outbound
 *     request bidi are decoded and surfaced; first-response enforcement closes on
 *     an unexpected first message.
 *   - Publisher side: accept/reject emit SUBSCRIBE_OK / REQUEST_ERROR on the
 *     inbound request bidi (not the control channel).
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static moq_session_t *make_established_ev(moq_perspective_t persp,
                                          uint32_t max_events)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
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

static moq_session_t *make_established(moq_perspective_t persp)
{
    return make_established_ev(persp, 0);
}

/* Outbound SUBSCRIBE for "live"/track; returns the request bidi stream_ref. */
static moq_stream_ref_t do_subscribe(moq_session_t *s, const char *track,
                                     moq_subscription_t *h)
{
    moq_subscribe_cfg_t sub;
    moq_subscribe_cfg_init(&sub);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    sub.track_namespace = ns;
    sub.track_name = (moq_bytes_t){ (const uint8_t *)track, strlen(track) };
    (void)moq_session_subscribe(s, &sub, 1, h);
    moq_action_t act;
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    if (moq_session_poll_actions(s, &act, 1) == 1) {
        ref = act.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&act);
    }
    return ref;
}

/* Inbound SUBSCRIBE request for "live"/track. */
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

static int event_kind_drain(moq_session_t *s, uint32_t want, moq_event_t *out)
{
    moq_event_t ev;
    int found = 0;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == want) {
            found = 1;
            if (out) { *out = ev; continue; }  /* caller cleans up */
        }
        moq_event_cleanup(&ev);
    }
    return found;
}

static int slot_state_for(moq_session_t *s, moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind == MOQ_REQ_NONE) return -1;
    return (int)s->subs[ep.slot].state;
}

int main(void)
{
    int failures = 0;

    /* == A. Subscriber: SUBSCRIBE_OK establishes the subscription ===== */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = do_subscribe(s, "video", &h);
        uint8_t ok[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_subscribe_ok(&w, 7 /* track_alias */, &(moq_d18_msg_params_t){0}, (moq_bytes_t){0});
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(event_kind_drain(s, MOQ_EVENT_SUBSCRIBE_OK, &ev));
        MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_ok.track_alias, 7);
        moq_event_cleanup(&ev);
        /* Established; the request bidi stays correlated for the subscription. */
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == B. Subscriber: REQUEST_ERROR with split FIN ================== *
     * Bytes first (no FIN): the error is surfaced and the entry drains; the slot
     * is freed only when the trailing FIN arrives in a later chunk. */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = do_subscribe(s, "video", &h);
        uint8_t er[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, 0x4, 0, MOQ_BYTES_LITERAL("denied"));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, er,
                moq_buf_writer_offset(&w), false /* fin */, 1), (int)MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(event_kind_drain(s, MOQ_EVENT_SUBSCRIBE_ERROR, &ev));
        MOQ_TEST_CHECK_EQ_U64((uint64_t)ev.u.subscribe_error.error_code, 0x4);
        moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* Draining: kept (not freed) until the FIN arrives. */
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_TERMINATED);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* freed on FIN */
        moq_session_destroy(s);
    }

    /* == B2. Subscriber: REQUEST_ERROR with same-chunk FIN frees now == */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = do_subscribe(s, "video", &h);
        uint8_t er[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, 0x4, 0, MOQ_BYTES_LITERAL("denied"));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, er,
                moq_buf_writer_offset(&w), true /* fin */, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(event_kind_drain(s, MOQ_EVENT_SUBSCRIBE_ERROR, NULL));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* freed now */
        moq_session_destroy(s);
    }

    /* == B3. Subscriber: extra bytes after REQUEST_ERROR -> close ===== */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = do_subscribe(s, "video", &h);
        uint8_t er[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, 0x4, 0, MOQ_BYTES_LITERAL("x"));
        size_t n = moq_buf_writer_offset(&w);
        er[n] = 0x00;   /* a stray trailing byte after the terminal response */
        (void)moq_session_on_bidi_stream_bytes(s, ref, er, n + 1, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == C. Subscriber: fragmented SUBSCRIBE_OK (byte by byte) ======== */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = do_subscribe(s, "video", &h);
        uint8_t ok[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_subscribe_ok(&w, 9, &(moq_d18_msg_params_t){0}, (moq_bytes_t){0});
        size_t n = moq_buf_writer_offset(&w);
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &ok[i], 1,
                    false, 1), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        MOQ_TEST_CHECK(event_kind_drain(s, MOQ_EVENT_SUBSCRIBE_OK, NULL));
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == D. Subscriber: unexpected first response closes ============== */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = do_subscribe(s, "video", &h);
        /* A SUBSCRIBE message is not a valid response on this stream. */
        uint8_t bad[128];
        size_t n = make_subscribe(bad, sizeof(bad), 1, "other");
        (void)moq_session_on_bidi_stream_bytes(s, ref, bad, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == E. Publisher: accept emits SUBSCRIBE_OK on the request bidi == */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xC001);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(event_kind_drain(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
        moq_subscription_t h = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, h, &acfg, 1),
                              (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK_EQ_U64(act.u.send_bidi_stream.stream_ref._v, ref._v);
        MOQ_TEST_CHECK(!act.u.send_bidi_stream.fin);  /* stays open */
        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                            act.u.send_bidi_stream.len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE_OK);
        moq_action_cleanup(&act);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref),
                              (int)MOQ_SUB_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == F. Publisher: reject emits REQUEST_ERROR (fin) on the bidi === */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xC002);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        moq_event_t ev;
        MOQ_TEST_CHECK(event_kind_drain(s, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev));
        moq_subscription_t h = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);

        moq_reject_subscribe_cfg_t rcfg;
        moq_reject_subscribe_cfg_init(&rcfg);
        rcfg.error_code = (moq_request_error_t)0x4;
        rcfg.reason = MOQ_BYTES_LITERAL("no");
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_reject_subscribe(s, h, &rcfg, 1),
                              (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK_EQ_U64(act.u.send_bidi_stream.stream_ref._v, ref._v);
        MOQ_TEST_CHECK(act.u.send_bidi_stream.fin);  /* terminal */
        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                            act.u.send_bidi_stream.len);
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&rr, &env),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_ERROR);
        moq_action_cleanup(&act);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* freed */
        moq_session_destroy(s);
    }

    /* == G. Subscriber: SUBSCRIBE_OK backpressure retry ============== *
     * Event queue full -> WOULD_BLOCK, slot kept PENDING_SUBSCRIBER; drain the
     * queue and re-feed the retry signal (NULL, 0, false) to commit. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 1);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h1, h2;
        moq_stream_ref_t r1 = do_subscribe(s, "video", &h1);
        moq_stream_ref_t r2 = do_subscribe(s, "audio", &h2);

        /* First SUBSCRIBE_OK commits and fills the (size-1) event queue. */
        uint8_t ok1[64];
        moq_buf_writer_t w1;
        moq_buf_writer_init(&w1, ok1, sizeof(ok1));
        moq_d18_encode_subscribe_ok(&w1, 11, &(moq_d18_msg_params_t){0}, (moq_bytes_t){0});
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r1, ok1,
                moq_buf_writer_offset(&w1), false, 1), (int)MOQ_OK);

        /* Second SUBSCRIBE_OK: queue full -> WOULD_BLOCK, slot stays pending. */
        uint8_t ok2[64];
        moq_buf_writer_t w2;
        moq_buf_writer_init(&w2, ok2, sizeof(ok2));
        moq_d18_encode_subscribe_ok(&w2, 13, &(moq_d18_msg_params_t){0}, (moq_bytes_t){0});
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, ok2,
                moq_buf_writer_offset(&w2), false, 1), (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, r2),
                              (int)MOQ_SUB_PENDING_SUBSCRIBER);

        /* Drain events, then re-feed the retry signal; the buffered OK commits. */
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, NULL, 0, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(event_kind_drain(s, MOQ_EVENT_SUBSCRIBE_OK, NULL));
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, r2),
                              (int)MOQ_SUB_ESTABLISHED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_subscribe_response");
    return failures != 0;
}
