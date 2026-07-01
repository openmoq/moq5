/*
 * Draft-18 PUBLISH_DONE on the request bidi (both perspectives). A publisher
 * terminates an established subscription by sending PUBLISH_DONE and FIN on the
 * request bidi (never the control channel); the subscriber surfaces
 * SUBSCRIBE_DONE, drains the trailing FIN, and frees its state. Also covers the
 * failed-update consequence: REQUEST_ERROR(update) followed by
 * PUBLISH_DONE(UPDATE_FAILED) terminates the subscription.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

static moq_session_t *make_established(moq_perspective_t persp)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
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

static moq_stream_ref_t establish_subscriber_alias(moq_session_t *s,
                                                   const char *track,
                                                   uint64_t alias,
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
    uint8_t ok[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok, sizeof(ok));
    moq_d18_encode_subscribe_ok(&w, alias, &(moq_d18_msg_params_t){0}, (moq_bytes_t){0});
    moq_session_on_bidi_stream_bytes(s, ref, ok, moq_buf_writer_offset(&w),
                                     false, 1);
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
    return ref;
}

static moq_stream_ref_t establish_subscriber(moq_session_t *s, const char *track,
                                             moq_subscription_t *h)
{
    return establish_subscriber_alias(s, track, 7, h);
}

static moq_subscription_t establish_publisher(moq_session_t *s, const char *track,
                                              uint64_t request_id,
                                              moq_stream_ref_t ref)
{
    uint8_t msg[128];
    size_t n = make_subscribe(msg, sizeof(msg), request_id, track);
    moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
    moq_subscription_t h = { 0 };
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    moq_accept_subscribe_cfg_t acfg;
    moq_accept_subscribe_cfg_init(&acfg);
    moq_session_accept_subscribe(s, h, &acfg, 1);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    return h;
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

    /* == A. PUBLISH_DONE codec round-trip ============================= */
    {
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_publish_done(&w, 0x3 /* SUBSCRIPTION_ENDED */, 5,
                MOQ_BYTES_LITERAL("bye")), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_PUBLISH_DONE);
        moq_d18_publish_done_t pd;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_publish_done(env.payload, env.payload_len, &pd),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(pd.status_code, 0x3);
        MOQ_TEST_CHECK_EQ_U64(pd.stream_count, 5);
        MOQ_TEST_CHECK_EQ_SIZE(pd.reason.len, 3);
        MOQ_TEST_CHECK(memcmp(pd.reason.data, "bye", 3) == 0);
    }

    /* == B. Publisher done_subscribe emits PUBLISH_DONE + FIN ========= */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xE001);
        moq_subscription_t h = establish_publisher(s, "video", 0, ref);

        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x2;   /* TRACK_ENDED */
        dcfg.stream_count = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(s, h, &dcfg, 1),
                              (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK_EQ_U64(act.u.send_bidi_stream.stream_ref._v, ref._v);
        MOQ_TEST_CHECK(act.u.send_bidi_stream.fin);   /* final message + FIN */
        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                            act.u.send_bidi_stream.len);
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&rr, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_PUBLISH_DONE);
        MOQ_TEST_CHECK(act.kind != MOQ_ACTION_SEND_CONTROL);
        moq_action_cleanup(&act);
        /* Publisher destroyed its subscription state. */
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == C. Subscriber receives PUBLISH_DONE, drains split FIN ========= */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);

        uint8_t dn[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, dn, sizeof(dn));
        moq_d18_encode_publish_done(&w, 0x3, 0, MOQ_BYTES_LITERAL("end"));
        /* Bytes first (no FIN): SUBSCRIBE_DONE surfaced, entry drains. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, dn,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool done = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                done = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x3);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(done);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), (int)MOQ_SUB_TERMINATED);
        /* FIN frees the subscription state. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == D. Subscriber PUBLISH_DONE with same-chunk FIN frees now ===== */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        uint8_t dn[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, dn, sizeof(dn));
        moq_d18_encode_publish_done(&w, 0x2, 0, MOQ_BYTES_LITERAL(""));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, dn,
                moq_buf_writer_offset(&w), true /* fin */, 1), (int)MOQ_OK);
        bool done = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) done = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(done);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* freed now */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == E. Failed update: REQUEST_ERROR then PUBLISH_DONE(UPDATE_FAILED) */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_forward = true; ucfg.forward = false;
        moq_session_update_subscription(s, h, &ucfg, 1);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act);
        MOQ_TEST_CHECK(s->subs[0].update_pending);

        /* REQUEST_ERROR (update failed) and PUBLISH_DONE(UPDATE_FAILED) +FIN
         * delivered together on the bidi: the update clears, then the
         * subscription terminates. */
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_error(&w, 0x8 /* UPDATE_FAILED */, 0,
                                     MOQ_BYTES_LITERAL("x"));
        moq_d18_encode_publish_done(&w, 0x8 /* UPDATE_FAILED */, 0,
                                    MOQ_BYTES_LITERAL("update failed"));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), true /* fin */, 1), (int)MOQ_OK);

        bool done = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                done = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x8);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(done);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* freed on FIN */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == F. Subscriber PUBLISH_DONE backpressure retry =============== *
     *  Event queue full -> WOULD_BLOCK, entry kept; drain + re-feed commits. */
    {
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        cfg.version = MOQ_VERSION_DRAFT_18;
        cfg.max_events = 1;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s), (int)MOQ_OK);
        moq_session_start(s, 0);
        moq_action_t a0;
        while (moq_session_poll_actions(s, &a0, 1) > 0) moq_action_cleanup(&a0);
        uint8_t setup[16];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, setup, sizeof(setup));
        moq_d18_encode_setup(&sw);
        moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&sw), 0);
        moq_event_t e0;
        while (moq_session_poll_events(s, &e0, 1) > 0) moq_event_cleanup(&e0);

        moq_subscription_t h, h2;
        moq_stream_ref_t ref = establish_subscriber_alias(s, "video", 7, &h);
        moq_stream_ref_t ref2 = establish_subscriber_alias(s, "audio", 9, &h2);
        (void)h; (void)h2;

        uint8_t dn[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, dn, sizeof(dn));
        moq_d18_encode_publish_done(&w, 0x3, 0, MOQ_BYTES_LITERAL("e"));
        size_t dlen = moq_buf_writer_offset(&w);

        /* First PUBLISH_DONE surfaces SUBSCRIBE_DONE, filling the size-1 queue. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, dn, dlen, false, 1),
            (int)MOQ_OK);
        /* Second PUBLISH_DONE on the other sub: event queue full -> WOULD_BLOCK,
         * entry kept established. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, dn, dlen, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref2), (int)MOQ_SUB_ESTABLISHED);

        /* Drain events, re-feed the retry signal: the buffered PUBLISH_DONE
         * commits. */
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, NULL, 0, false, 1),
            (int)MOQ_OK);
        bool done = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) done = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(done);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref2), (int)MOQ_SUB_TERMINATED);
        moq_session_destroy(s);
    }

    /* == G. End-to-end: publisher done -> subscriber SUBSCRIBE_DONE === */
    {
        moq_simpair_cfg_t scfg = MOQ_SIMPAIR_CFG_INIT;
        scfg.alloc = moq_alloc_default();
        scfg.version = MOQ_VERSION_DRAFT_18;
        moq_simpair_t *sp = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&scfg, &sp), (int)MOQ_OK);
        moq_session_t *client = moq_simpair_client(sp);
        moq_session_t *server = moq_simpair_server(sp);
        MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_start(sp), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        sub.track_namespace = (moq_namespace_t){ parts, 1 };
        sub.track_name = MOQ_BYTES_LITERAL("video");
        moq_subscription_t ch;
        moq_session_subscribe(client, &sub, 1, &ch);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_subscription_t sh = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) sh = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(server, sh, &acfg, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        while (moq_session_poll_events(client, &ev, 1) > 0) moq_event_cleanup(&ev);

        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x2;   /* TRACK_ENDED */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_done_subscribe(server, sh, &dcfg,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        bool done = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
                done = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_done.status_code, 0x2);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(done);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == H. PUBLISH_DONE as first response to a pending SUBSCRIBE closes */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        sub.track_namespace = (moq_namespace_t){ parts, 1 };
        sub.track_name = MOQ_BYTES_LITERAL("video");
        moq_subscription_t h;
        moq_session_subscribe(s, &sub, 1, &h);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        if (moq_session_poll_actions(s, &act, 1) == 1) {
            ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        /* No SUBSCRIBE_OK yet: PUBLISH_DONE is not a valid first response. */
        uint8_t dn[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, dn, sizeof(dn));
        moq_d18_encode_publish_done(&w, 0x2, 0, MOQ_BYTES_LITERAL(""));
        (void)moq_session_on_bidi_stream_bytes(s, ref, dn,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == I. Late REQUEST_UPDATE after done_subscribe is discarded ====== */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xE100);
        moq_subscription_t h = establish_publisher(s, "video", 0, ref);
        moq_done_subscribe_cfg_t dcfg;
        moq_done_subscribe_cfg_init(&dcfg);
        dcfg.status_code = 0x2;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_done_subscribe(s, h, &dcfg, 1),
                              (int)MOQ_OK);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), -1);  /* freed */

        /* A late in-flight REQUEST_UPDATE on the torn-down bidi is discarded,
         * not mistaken for a new inbound request. */
        moq_d18_msg_params_t p = { 0 };
        p.has_forward = true; p.forward = 0;
        uint8_t um[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, um, sizeof(um));
        moq_d18_encode_request_update(&w, 2, &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, um,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* FIN retires the drain ref without surfacing anything. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == J. After update failure: new update blocked; wrong status closes */
    {
        moq_session_t *s = make_established(MOQ_PERSPECTIVE_CLIENT);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_forward = true; ucfg.forward = false;
        moq_session_update_subscription(s, h, &ucfg, 1);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act);

        /* REQUEST_ERROR fails the update; the subscription awaits PUBLISH_DONE. */
        uint8_t er[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, 0x8, 0, MOQ_BYTES_LITERAL("x"));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, er,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);

        /* A new update is blocked until the terminal PUBLISH_DONE. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &ucfg, 1),
            (int)MOQ_ERR_WRONG_STATE);

        /* A PUBLISH_DONE with a non-UPDATE_FAILED status is a violation. */
        uint8_t dn[64];
        moq_buf_writer_t w2;
        moq_buf_writer_init(&w2, dn, sizeof(dn));
        moq_d18_encode_publish_done(&w2, 0x2 /* not UPDATE_FAILED */, 0,
                                    MOQ_BYTES_LITERAL(""));
        (void)moq_session_on_bidi_stream_bytes(s, ref, dn,
            moq_buf_writer_offset(&w2), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_publish_done");
    return failures != 0;
}
