/*
 * Draft-18 outbound SUBSCRIBE: a subscription opens its own bidirectional
 * stream (OPEN_BIDI_STREAM action carrying the SUBSCRIBE message), and the
 * request is registered in the stream-ref request index so its response can be
 * correlated by stream identity.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

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
    uint8_t setup[16];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, setup, sizeof(setup));
    moq_d18_encode_setup(&w);
    moq_session_on_control_bytes(s, setup, moq_buf_writer_offset(&w), 0);
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) > 0) moq_event_cleanup(&e);
    return s;
}

int main(void)
{
    int failures = 0;

    moq_session_t *s = make_established_d18();
    MOQ_TEST_CHECK(s != NULL);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_subscribe_cfg_t sub;
    moq_subscribe_cfg_init(&sub);
    sub.track_namespace = ns;
    sub.track_name = MOQ_BYTES_LITERAL("video");
    moq_subscription_t h;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &sub, 1, &h),
                          (int)MOQ_OK);

    /* The subscription emits OPEN_BIDI_STREAM carrying the SUBSCRIBE message,
     * not SEND_CONTROL. */
    moq_action_t act;
    size_t n = moq_session_poll_actions(s, &act, 1);
    MOQ_TEST_CHECK_EQ_SIZE(n, 1);
    MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_OPEN_BIDI_STREAM);
    moq_stream_ref_t ref = act.u.open_bidi_stream.stream_ref;

    /* The carried bytes decode as a SUBSCRIBE for the requested track. */
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, act.u.open_bidi_stream.data,
                        act.u.open_bidi_stream.len);
    moq_control_envelope_t env;
    MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE);
    moq_bytes_t dparts[8];
    moq_d18_subscribe_t dec;
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_d18_decode_subscribe(env.payload, env.payload_len,
            dparts, 8, &dec), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(dec.request_id, 0);   /* first client request */
    MOQ_TEST_CHECK_EQ_SIZE(dec.track_namespace.count, 1);
    MOQ_TEST_CHECK(dec.track_name.len == 5 &&
                   memcmp(dec.track_name.data, "video", 5) == 0);
    moq_action_cleanup(&act);

    /* The request is registered by its bidi stream_ref (response correlation),
     * and not by request-id (draft-18 responses carry no request id). */
    moq_request_endpoint_t by_ref =
        request_registry_find_by_streamref(s, ref);
    MOQ_TEST_CHECK_EQ_INT((int)by_ref.kind, (int)MOQ_REQ_SUBSCRIPTION);
    MOQ_TEST_CHECK(by_ref.has_stream_ref);
    MOQ_TEST_CHECK_EQ_U64(by_ref.stream_ref._v, ref._v);
    moq_request_endpoint_t by_id = request_registry_find_by_id(s, 0);
    MOQ_TEST_CHECK_EQ_INT((int)by_id.kind, (int)MOQ_REQ_NONE);

    /* The next request gets the next parity request id and a fresh stream. */
    moq_subscribe_cfg_t sub2;
    moq_subscribe_cfg_init(&sub2);
    moq_bytes_t parts2[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns2 = { parts2, 1 };
    sub2.track_namespace = ns2;
    sub2.track_name = MOQ_BYTES_LITERAL("audio");
    moq_subscription_t h2;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &sub2, 1, &h2),
                          (int)MOQ_OK);
    moq_action_t act2;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act2, 1), 1);
    MOQ_TEST_CHECK(act2.u.open_bidi_stream.stream_ref._v != ref._v);
    moq_action_cleanup(&act2);

    /* A non-default filter is now representable as a SUBSCRIPTION_FILTER
     * parameter: the subscribe succeeds and opens its own request bidi. Drain
     * that action so the unsubscribe assertions below see only the teardown. */
    moq_subscribe_cfg_t sub3;
    moq_subscribe_cfg_init(&sub3);
    moq_bytes_t parts3[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns3 = { parts3, 1 };
    sub3.track_namespace = ns3;
    sub3.track_name = MOQ_BYTES_LITERAL("hd");
    sub3.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    moq_subscription_t h3;
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(s, &sub3, 1, &h3),
                          (int)MOQ_OK);
    moq_action_t act3;
    MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act3, 1), 1);
    MOQ_TEST_CHECK_EQ_INT((int)act3.kind, (int)MOQ_ACTION_OPEN_BIDI_STREAM);
    moq_action_cleanup(&act3);

    /* Unsubscribe on a stream-correlated profile is transport teardown, not a
     * control message: it STOP_SENDINGs the request bidi (the required
     * cancellation signal) and RESETs our own send half. No SEND_CONTROL is
     * emitted, and the subscription is freed immediately. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_unsubscribe(s, h, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    moq_action_t tdn[4];
    size_t ntdn = moq_session_poll_actions(s, tdn, 4);
    MOQ_TEST_CHECK_EQ_SIZE(ntdn, 2);
    /* STOP is required and must come first; RESET terminates our send half. */
    MOQ_TEST_CHECK_EQ_INT((int)tdn[0].kind, (int)MOQ_ACTION_STOP_BIDI_STREAM);
    MOQ_TEST_CHECK_EQ_U64(tdn[0].u.stop_bidi_stream.stream_ref._v, ref._v);
    MOQ_TEST_CHECK_EQ_INT((int)tdn[1].kind, (int)MOQ_ACTION_RESET_BIDI_STREAM);
    MOQ_TEST_CHECK_EQ_U64(tdn[1].u.reset_bidi_stream.stream_ref._v, ref._v);
    for (size_t i = 0; i < ntdn; i++) {
        MOQ_TEST_CHECK(tdn[i].kind != MOQ_ACTION_SEND_CONTROL);
        moq_action_cleanup(&tdn[i]);
    }

    /* The subscription is gone: no longer correlated by its stream ref. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)request_registry_find_by_streamref(s, ref).kind,
        (int)MOQ_REQ_NONE);

    /* A late SUBSCRIBE_OK the peer sent before seeing our STOP arrives on the
     * cancelled bidi. It must be discarded (the bidi is draining), not mistaken
     * for a new inbound request -- which would close the session. A SUBSCRIBE_OK
     * envelope (type 0x07, length 0) is exactly such a non-request first byte. */
    uint8_t late_ok[] = { 0x07, 0x00, 0x00 };
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, late_ok, sizeof(late_ok),
                                              false, 1), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
    /* The trailing FIN retires the drain ref without surfacing anything. */
    MOQ_TEST_CHECK_EQ_INT(
        (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
        (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

    moq_session_destroy(s);

    /* == New-group request rides SUBSCRIBE both ways ================== *
     *  Outbound: allowed blind (no support foreknowledge); the queued
     *  SUBSCRIBE decodes with the parameter (value 0 is meaningful).
     *  Inbound: a SUBSCRIBE carrying it surfaces on the request event. */
    {
        moq_session_t *cs = make_established_d18();
        MOQ_TEST_CHECK(cs != NULL);
        moq_bytes_t nparts[] = { MOQ_BYTES_LITERAL("live") };
        moq_subscribe_cfg_t scfg;
        moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace = (moq_namespace_t){ nparts, 1 };
        scfg.track_name = MOQ_BYTES_LITERAL("video");
        scfg.has_new_group_request = true;
        scfg.new_group_request = 0;
        moq_subscription_t sh;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(cs, &scfg, 1, &sh),
                              (int)MOQ_OK);
        moq_action_t a;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(cs, &a, 1), 1);
        moq_buf_reader_t ar;
        moq_buf_reader_init(&ar, a.u.open_bidi_stream.data,
                            a.u.open_bidi_stream.len);
        moq_control_envelope_t aenv;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&ar, &aenv),
                              (int)MOQ_OK);
        moq_bytes_t adparts[8];
        moq_d18_subscribe_t adec;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(aenv.payload, aenv.payload_len,
                adparts, 8, &adec), (int)MOQ_OK);
        MOQ_TEST_CHECK(adec.params.has_new_group_request);
        MOQ_TEST_CHECK_EQ_U64(adec.params.new_group_request, 0);
        moq_action_cleanup(&a);
        moq_session_destroy(cs);
    }
    {
        moq_session_cfg_t pcfg;
        moq_session_cfg_init_sized(&pcfg, sizeof(pcfg), moq_alloc_default(),
                             MOQ_PERSPECTIVE_SERVER);
        pcfg.version = MOQ_VERSION_DRAFT_18;
        moq_session_t *ps = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&pcfg, 0, &ps),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(ps, 0), (int)MOQ_OK);
        { moq_action_t da;
          while (moq_session_poll_actions(ps, &da, 1) > 0)
              moq_action_cleanup(&da); }
        uint8_t setup[16];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, setup, sizeof(setup));
        moq_d18_encode_setup(&sw);
        moq_session_on_control_bytes(ps, setup, moq_buf_writer_offset(&sw), 0);
        { moq_event_t de;
          while (moq_session_poll_events(ps, &de, 1) > 0)
              moq_event_cleanup(&de); }

        moq_bytes_t nparts[] = { MOQ_BYTES_LITERAL("live") };
        moq_namespace_t nns = { nparts, 1 };
        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.has_new_group_request = true;
        mp.new_group_request = 9;
        uint8_t msg[128];
        moq_buf_writer_t mw;
        moq_buf_writer_init(&mw, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&mw, 0, &nns,
                MOQ_BYTES_LITERAL("video"), &mp), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(ps,
                moq_stream_ref_from_u64(1), msg, moq_buf_writer_offset(&mw),
                false, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(ps, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST &&
                ev.u.subscribe_request.has_new_group_request &&
                ev.u.subscribe_request.new_group_request == 9)
                got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_session_destroy(ps);
    }

    MOQ_TEST_PASS("d18_subscribe");
    return failures != 0;
}
