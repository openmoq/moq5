/*
 * Draft-18 REQUEST_UPDATE / REQUEST_OK lifecycle on the request bidi (both
 * perspectives). A subscriber sends REQUEST_UPDATE on its existing subscription
 * bidi; the publisher applies it (existing update semantics) and replies
 * REQUEST_OK on the same bidi -- never on the control channel. The update
 * carries FORWARD / SUBSCRIBER_PRIORITY and the delivery timeout (mapped to both
 * OBJECT_ and SUBGROUP_DELIVERY_TIMEOUT); a sub-millisecond timeout is rejected
 * locally before anything is emitted, and unknown inbound parameters fail closed.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Codec round-trip ---------------------------------------------- */

static int test_codec(void)
{
    int failures = 0;
    uint8_t buf[128];

    /* REQUEST_UPDATE with priority only. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true;
        p.subscriber_priority = 5;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, 4, &p), (int)MOQ_OK);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_decode_envelope(&r, &env), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_UPDATE);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(env.payload, env.payload_len, &u),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(u.request_id, 4);
        MOQ_TEST_CHECK(u.params.has_subscriber_priority);
        MOQ_TEST_CHECK_EQ_U64(u.params.subscriber_priority, 5);
        MOQ_TEST_CHECK(!u.params.has_forward);
    }

    /* REQUEST_UPDATE with forward only. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_forward = true;
        p.forward = 0;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, 6, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(env.payload, env.payload_len, &u),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(u.params.has_forward);
        MOQ_TEST_CHECK_EQ_U64(u.params.forward, 0);
        MOQ_TEST_CHECK(!u.params.has_subscriber_priority);
    }

    /* REQUEST_UPDATE with both, encoded in ascending parameter-type order. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_forward = true; p.forward = 1;
        p.has_subscriber_priority = true; p.subscriber_priority = 200;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, 8, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        /* Inspect the wire: count=2, then FORWARD (delta 0x10) before
         * SUBSCRIBER_PRIORITY (delta 0x10). */
        moq_buf_reader_t pr;
        moq_buf_reader_init(&pr, env.payload, env.payload_len);
        uint64_t rid, count, d0, d1;
        moq_buf_read_vi64(&pr, &rid);
        moq_buf_read_vi64(&pr, &count);
        MOQ_TEST_CHECK_EQ_U64(count, 2);
        moq_buf_read_vi64(&pr, &d0);
        MOQ_TEST_CHECK_EQ_U64(d0, MOQ_D18_PARAM_FORWARD);   /* first delta = type */
        pr.pos += 1;                                        /* forward value */
        moq_buf_read_vi64(&pr, &d1);
        MOQ_TEST_CHECK_EQ_U64(d1, MOQ_D18_PARAM_SUBSCRIBER_PRIORITY -
                                  MOQ_D18_PARAM_FORWARD);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(env.payload, env.payload_len, &u),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(u.params.has_forward && u.params.forward == 1);
        MOQ_TEST_CHECK(u.params.has_subscriber_priority &&
                       u.params.subscriber_priority == 200);
    }

    /* Decode rejects an unknown parameter type (count=1, type 0x11). */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_REQUEST_UPDATE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 4);          /* request id */
        moq_buf_write_vi64(&w, 1);          /* 1 parameter */
        moq_buf_write_vi64(&w, 0x11);       /* unknown type */
        uint8_t v = 0; moq_buf_write_raw(&w, &v, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(env.payload, env.payload_len, &u),
            (int)MOQ_ERR_PROTO);
    }

    /* Decode rejects FORWARD value > 1. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_REQUEST_UPDATE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_FORWARD);
        uint8_t v = 2; moq_buf_write_raw(&w, &v, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_update(env.payload, env.payload_len, &u),
            (int)MOQ_ERR_PROTO);
    }

    /* REQUEST_OK encode/decode (zero parameters). */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_request_ok(&w), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_request_ok(env.payload, env.payload_len),
            (int)MOQ_OK);
    }

    return failures;
}

/* -- Session helpers ----------------------------------------------- */

static moq_session_t *make_established_ev(moq_perspective_t persp,
                                          uint32_t max_events,
                                          uint32_t max_actions)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (max_actions) cfg.max_actions = max_actions;
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

/* Subscriber side: SUBSCRIBE then feed SUBSCRIBE_OK -> ESTABLISHED. */
static moq_stream_ref_t establish_subscriber_ex(moq_session_t *s,
                                                const char *track,
                                                moq_bytes_t ok_props,
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
    moq_d18_encode_subscribe_ok(&w, 7, &(moq_d18_msg_params_t){0}, ok_props);
    moq_session_on_bidi_stream_bytes(s, ref, ok, moq_buf_writer_offset(&w),
                                     false, 1);
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
    return ref;
}

static moq_stream_ref_t establish_subscriber(moq_session_t *s, const char *track,
                                             moq_subscription_t *h)
{
    return establish_subscriber_ex(s, track, (moq_bytes_t){0}, h);
}

/* Publisher side: feed SUBSCRIBE on `ref`, accept -> ESTABLISHED, with
 * optional track properties on the accept (e.g. DYNAMIC_GROUPS). */
static moq_subscription_t establish_publisher_ex(moq_session_t *s,
                                                 const char *track,
                                                 uint64_t request_id,
                                                 moq_stream_ref_t ref,
                                                 moq_bytes_t accept_props)
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
    acfg.track_properties = accept_props;
    moq_session_accept_subscribe(s, h, &acfg, 1);
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
    return h;
}

static moq_subscription_t establish_publisher(moq_session_t *s, const char *track,
                                              uint64_t request_id,
                                              moq_stream_ref_t ref)
{
    return establish_publisher_ex(s, track, request_id, ref, (moq_bytes_t){0});
}

static int slot_state_for(moq_session_t *s, moq_stream_ref_t ref)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (ep.kind == MOQ_REQ_NONE) return -1;
    return (int)s->subs[ep.slot].state;
}

/* Decode the REQUEST_UPDATE carried by a SEND_BIDI action into `u`. */
static int update_from_action(const moq_action_t *act, moq_stream_ref_t ref,
                              moq_d18_request_update_t *u)
{
    if (act->kind != MOQ_ACTION_SEND_BIDI_STREAM) return 0;
    if (act->u.send_bidi_stream.stream_ref._v != ref._v) return 0;
    if (act->u.send_bidi_stream.fin) return 0;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, act->u.send_bidi_stream.data,
                        act->u.send_bidi_stream.len);
    moq_control_envelope_t env;
    if (moq_d18_decode_envelope(&r, &env) < 0) return 0;
    if (env.msg_type != MOQ_D18_REQUEST_UPDATE) return 0;
    return moq_d18_decode_request_update(env.payload, env.payload_len, u) == MOQ_OK;
}

int main(void)
{
    int failures = 0;
    failures += test_codec();

    /* == Outbound: priority-only update on the request bidi =========== */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        MOQ_TEST_CHECK(s != NULL);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);

        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_subscriber_priority = true;
        cfg.subscriber_priority = 9;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(s->subs[0].update_pending);

        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK(update_from_action(&act, ref, &u));
        MOQ_TEST_CHECK(u.params.has_subscriber_priority &&
                       u.params.subscriber_priority == 9);
        MOQ_TEST_CHECK(!u.params.has_forward);
        MOQ_TEST_CHECK(act.kind != MOQ_ACTION_SEND_CONTROL);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == Outbound: forward-only update ================================ */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_forward = true;
        cfg.forward = false;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK(update_from_action(&act, ref, &u));
        MOQ_TEST_CHECK(u.params.has_forward && u.params.forward == 0);
        MOQ_TEST_CHECK(!u.params.has_subscriber_priority);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == Outbound: both params (ascending order on the wire) ========== */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_forward = true; cfg.forward = true;
        cfg.has_subscriber_priority = true; cfg.subscriber_priority = 3;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK(update_from_action(&act, ref, &u));
        MOQ_TEST_CHECK(u.params.has_forward && u.params.forward == 1);
        MOQ_TEST_CHECK(u.params.has_subscriber_priority &&
                       u.params.subscriber_priority == 3);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == Outbound: delivery_timeout emits both timeout parameters ===== *
     *  The single public value (us) maps to OBJECT_ and SUBGROUP_DELIVERY_-
     *  TIMEOUT (ms) with the same value. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_delivery_timeout = true;
        cfg.delivery_timeout_us = 5000000;   /* 5000 ms */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK(update_from_action(&act, ref, &u));
        MOQ_TEST_CHECK(u.params.has_object_delivery_timeout &&
                       u.params.object_delivery_timeout_ms == 5000);
        MOQ_TEST_CHECK(u.params.has_subgroup_delivery_timeout &&
                       u.params.subgroup_delivery_timeout_ms == 5000);
        moq_action_cleanup(&act);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Outbound: sub-millisecond delivery_timeout still rejected ===== */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        (void)establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_delivery_timeout = true;
        cfg.delivery_timeout_us = 500;   /* < 1000us, rejected like draft-16 */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(!s->subs[0].update_pending);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Outbound: auth token rides the update (auth-only is valid) === *
     *  Token refresh is the prime REQUEST_UPDATE auth use case, so an
     *  update carrying ONLY a token is a valid update. The token travels
     *  as a USE_VALUE structure among the message parameters. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        moq_auth_token_t tok = {
            .token_type = 11,
            .token_value = MOQ_BYTES_LITERAL("fresh"),
        };
        cfg.auth_tokens = &tok;
        cfg.auth_token_count = 1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(s->subs[0].update_pending);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK(update_from_action(&act, ref, &u));
        MOQ_TEST_CHECK_EQ_SIZE(u.params.auth_token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(u.params.auth_tokens[0].alias_type,
                              MOQ_AUTH_TOKEN_USE_VALUE);
        MOQ_TEST_CHECK_EQ_U64(u.params.auth_tokens[0].token_type, 11);
        MOQ_TEST_CHECK_EQ_SIZE(u.params.auth_tokens[0].token_value.len, 5);
        MOQ_TEST_CHECK(memcmp(u.params.auth_tokens[0].token_value.data,
                              "fresh", 5) == 0);
        MOQ_TEST_CHECK(!u.params.has_subscriber_priority);
        MOQ_TEST_CHECK(!u.params.has_forward);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == Outbound: an old-size cfg sends no tokens (ABI-additive) ===== *
     *  A caller compiled against the pre-auth layout passes the smaller
     *  struct_size; whatever happens to sit in the appended fields is
     *  never read. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.struct_size =
            (uint32_t)offsetof(moq_subscription_update_cfg_t, auth_tokens);
        cfg.has_subscriber_priority = true;
        cfg.subscriber_priority = 7;
        cfg.auth_tokens = (const moq_auth_token_t *)(uintptr_t)0x1;  /* junk */
        cfg.auth_token_count = 99;                                   /* junk */
        cfg.has_new_group_request = true;                            /* junk */
        cfg.new_group_request = 77;                                  /* junk */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK(update_from_action(&act, ref, &u));
        MOQ_TEST_CHECK_EQ_SIZE(u.params.auth_token_count, 0);
        MOQ_TEST_CHECK(!u.params.has_new_group_request);
        MOQ_TEST_CHECK(u.params.has_subscriber_priority &&
                       u.params.subscriber_priority == 7);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == Outbound: encode failure leaves the update uncommitted ======= *
     *  An over-cap token list fails the encode AFTER the request was
     *  reserved; the reservation must roll back: update_pending stays
     *  false and a follow-up valid update succeeds. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        static moq_auth_token_t many[MOQ_D18_MAX_AUTH_TOKENS + 1];
        for (size_t i = 0; i < MOQ_D18_MAX_AUTH_TOKENS + 1; i++) {
            many[i].token_type = i;
            many[i].token_value = MOQ_BYTES_LITERAL("v");
        }
        cfg.auth_tokens = many;
        cfg.auth_token_count = MOQ_D18_MAX_AUTH_TOKENS + 1;
        MOQ_TEST_CHECK(
            moq_session_update_subscription(s, h, &cfg, 1) < 0);
        MOQ_TEST_CHECK(!s->subs[0].update_pending);
        /* Capacity and state uncommitted: a valid update still works. */
        cfg.auth_token_count = 1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(s->subs[0].update_pending);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK(update_from_action(&act, ref, &u));
        MOQ_TEST_CHECK_EQ_SIZE(u.params.auth_token_count, 1);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == Outbound: new-group request requires dynamic-group support ===== *
     *  SUBSCRIBE_OK without DYNAMIC_GROUPS: an update carrying a new-group
     *  request is MOQ_ERR_INVAL before any mutation; with DYNAMIC_GROUPS=1
     *  it encodes (value 0 -- "no group info" -- round-trips distinctly
     *  from absent). */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        (void)establish_subscriber(s, "video", &h);   /* no DYNAMIC_GROUPS */
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_new_group_request = true;
        cfg.new_group_request = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1),
            (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(!s->subs[0].update_pending);
        moq_session_destroy(s);
    }
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        static const uint8_t dyn[] = { 0x30, 0x01 };  /* DYNAMIC_GROUPS=1 */
        moq_stream_ref_t ref = establish_subscriber_ex(s, "video",
            (moq_bytes_t){ dyn, 2 }, &h);
        MOQ_TEST_CHECK(s->subs[0].dynamic_groups);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_new_group_request = true;
        cfg.new_group_request = 0;            /* "no group info" is a value */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(s, h, &cfg, 1), (int)MOQ_OK);
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        moq_d18_request_update_t u;
        MOQ_TEST_CHECK(update_from_action(&act, ref, &u));
        MOQ_TEST_CHECK(u.params.has_new_group_request);
        MOQ_TEST_CHECK_EQ_U64(u.params.new_group_request, 0);
        moq_action_cleanup(&act);
        moq_session_destroy(s);
    }

    /* == Inbound: REQUEST_UPDATE new-group request surfaces ============ *
     *  Valid only because OUR accept advertised DYNAMIC_GROUPS=1. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        static const uint8_t dynp[] = { 0x30, 0x01 };
        (void)establish_publisher_ex(s, "video", 0, ref,
                                     (moq_bytes_t){ dynp, 2 });
        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.has_new_group_request = true;
        mp.new_group_request = 42;
        uint8_t upd[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, upd, sizeof(upd));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, 2, &mp), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, upd,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED &&
                ev.u.subscribe_updated.has_new_group_request &&
                ev.u.subscribe_updated.new_group_request == 42)
                got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound NGR without our advertisement closes 0x3 ============== *
     *  We accepted WITHOUT DYNAMIC_GROUPS; the peer's update carrying a
     *  new-group request violates the MUST NOT -- protocol violation,
     *  never delivered to the application. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        (void)establish_publisher(s, "video", 0, ref);   /* no DYNAMIC_GROUPS */
        moq_d18_msg_params_t mp;
        memset(&mp, 0, sizeof(mp));
        mp.has_new_group_request = true;
        mp.new_group_request = 42;
        uint8_t upd[96];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, upd, sizeof(upd));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, 2, &mp), (int)MOQ_OK);
        (void)moq_session_on_bidi_stream_bytes(s, ref, upd,
            moq_buf_writer_offset(&w), false, 1);
        bool surfaced = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) surfaced = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!surfaced);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        bool saw_close = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_CLOSE_SESSION) {
                saw_close = true;
                MOQ_TEST_CHECK_EQ_U64(act.u.close_session.code, 0x3);
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(saw_close);
        moq_session_destroy(s);
    }

    /* == DYNAMIC_GROUPS value above 1 closes PROTOCOL_VIOLATION ======== *
     *  The public encoder refuses such properties, so the malformed
     *  SUBSCRIBE_OK is hand-built: alias, zero params, then the property
     *  tail {0x30, 0x02}. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        /* Refused on encode: the outbound side cannot emit the violation. */
        {
            uint8_t tmp[32];
            moq_buf_writer_t tw;
            moq_buf_writer_init(&tw, tmp, sizeof(tmp));
            static const uint8_t bad_props[] = { 0x30, 0x02 };
            MOQ_TEST_CHECK(moq_d18_encode_subscribe_ok(&tw, 7,
                &(moq_d18_msg_params_t){0},
                (moq_bytes_t){ bad_props, 2 }) < 0);
        }
        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        sub.track_namespace = (moq_namespace_t){ parts, 1 };
        sub.track_name = MOQ_BYTES_LITERAL("video");
        (void)moq_session_subscribe(s, &sub, 1, &h);
        moq_action_t act;
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        if (moq_session_poll_actions(s, &act, 1) == 1) {
            ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        uint8_t ok[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_OK);
        size_t len_off;
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 7);            /* track alias */
        moq_buf_write_vi64(&w, 0);            /* zero params */
        moq_buf_write_raw(&w, (const uint8_t[]){ 0x30, 0x02 }, 2);
        moq_buf_patch_uint16(&w, len_off,
                             (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, ok,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        bool saw_close = false;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_CLOSE_SESSION) {
                saw_close = true;
                MOQ_TEST_CHECK_EQ_U64(act.u.close_session.code, 0x3);
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(saw_close);
        moq_session_destroy(s);
    }

    /* == Inbound: fragmented REQUEST_UPDATE applies + REQUEST_OK ====== */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xD001);
        (void)establish_publisher(s, "video", 0, ref);

        /* Build a REQUEST_UPDATE (request id 2 = next peer/client id). */
        moq_d18_msg_params_t p = { 0 };
        p.has_forward = true; p.forward = 0;
        p.has_subscriber_priority = true; p.subscriber_priority = 42;
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, 2, &p), (int)MOQ_OK);
        size_t n = moq_buf_writer_offset(&w);

        /* Feed it one byte at a time. */
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1,
                    false, 1), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }

        bool updated = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                updated = true;
                MOQ_TEST_CHECK(ev.u.subscribe_updated.has_forward &&
                               ev.u.subscribe_updated.forward == false);
                MOQ_TEST_CHECK(ev.u.subscribe_updated.has_subscriber_priority &&
                               ev.u.subscribe_updated.subscriber_priority == 42);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);
        /* REQUEST_OK emitted on the request bidi (not the control channel). */
        moq_action_t act;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &act, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(act.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        MOQ_TEST_CHECK_EQ_U64(act.u.send_bidi_stream.stream_ref._v, ref._v);
        MOQ_TEST_CHECK(!act.u.send_bidi_stream.fin);
        moq_buf_reader_t rr;
        moq_buf_reader_init(&rr, act.u.send_bidi_stream.data,
                            act.u.send_bidi_stream.len);
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&rr, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
        moq_action_cleanup(&act);
        /* The subscription remains established and correlated. */
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), (int)MOQ_SUB_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound: REQUEST_UPDATE with delivery timeout surfaces it ==== */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xD201);
        (void)establish_publisher(s, "video", 0, ref);
        /* Both timeout carriers equal (3000 ms) -> 3000000 us. */
        moq_d18_msg_params_t p = { 0 };
        p.has_object_delivery_timeout = true; p.object_delivery_timeout_ms = 3000;
        p.has_subgroup_delivery_timeout = true; p.subgroup_delivery_timeout_ms = 3000;
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool updated = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                updated = true;
                MOQ_TEST_CHECK(ev.u.subscribe_updated.has_delivery_timeout);
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.delivery_timeout_us,
                                      3000000);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound: only one timeout carrier present surfaces it ======== */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xD202);
        (void)establish_publisher(s, "video", 0, ref);
        moq_d18_msg_params_t p = { 0 };
        p.has_subgroup_delivery_timeout = true; p.subgroup_delivery_timeout_ms = 2000;
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool updated = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                updated = true;
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.delivery_timeout_us,
                                      2000000);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);
        moq_session_destroy(s);
    }

    /* == Inbound: mismatched timeout carriers fail closed ============= */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xD203);
        (void)establish_publisher(s, "video", 0, ref);
        moq_d18_msg_params_t p = { 0 };
        p.has_object_delivery_timeout = true; p.object_delivery_timeout_ms = 3000;
        p.has_subgroup_delivery_timeout = true; p.subgroup_delivery_timeout_ms = 4000;
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Inbound: unknown parameter fails closed ===================== */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xD002);
        (void)establish_publisher(s, "video", 0, ref);

        /* REQUEST_UPDATE with one unknown parameter type (0x11). */
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_REQUEST_UPDATE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 2);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 0x11);
        uint8_t v = 0; moq_buf_write_raw(&w, &v, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Inbound: REQUEST_UPDATE as the first message on a fresh bidi == *
     *  Only SUBSCRIBE/FETCH are valid first messages; a REQUEST_UPDATE on a
     *  brand-new request bidi must close (no event, no response action). */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 0);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xD0FF);
        moq_d18_msg_params_t p = { 0 };
        p.has_forward = true; p.forward = 0;
        uint8_t um[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, um, sizeof(um));
        moq_d18_encode_request_update(&w, 0, &p);
        (void)moq_session_on_bidi_stream_bytes(s, ref, um,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        /* No SUBSCRIBE_UPDATED surfaced and no REQUEST_OK queued (only the
         * session-close action remains). */
        bool updated = false, ok_action = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) updated = true;
            moq_event_cleanup(&ev);
        }
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM) ok_action = true;
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(!updated);
        MOQ_TEST_CHECK(!ok_action);
        moq_session_destroy(s);
    }

    /* == Inbound: WOULD_BLOCK keeps the buffer + update retryable ===== */
    {
        /* Action queue size 1: after accept, the SUBSCRIBE_OK action fills it,
         * so an inbound REQUEST_UPDATE cannot queue its REQUEST_OK yet. */
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_SERVER, 0, 1);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xD003);

        uint8_t msg[128];
        size_t n = make_subscribe(msg, sizeof(msg), 0, "video");
        moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
        moq_subscription_t h = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_subscribe(s, h, &acfg, 1),
                              (int)MOQ_OK);
        /* Do NOT drain the SUBSCRIBE_OK action: the action queue is now full. */

        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 7;
        uint8_t um[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, um, sizeof(um));
        moq_d18_encode_request_update(&w, 2, &p);
        size_t un = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, um, un, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), (int)MOQ_SUB_ESTABLISHED);
        /* Update not yet applied: no SUBSCRIBE_UPDATED surfaced. */
        bool updated = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) updated = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!updated);

        /* Drain the SUBSCRIBE_OK action to free queue space, then re-feed the
         * retry signal: the buffered update commits. */
        moq_action_t a;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &a, 1), 1);
        moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        updated = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) updated = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(s, &a, 1), 1);
        MOQ_TEST_CHECK_EQ_U64(a.kind, MOQ_ACTION_SEND_BIDI_STREAM);
        moq_action_cleanup(&a);
        moq_session_destroy(s);
    }

    /* == Subscriber: REQUEST_OK clears the pending update ============= */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_subscriber_priority = true; cfg.subscriber_priority = 1;
        moq_session_update_subscription(s, h, &cfg, 1);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act);
        MOQ_TEST_CHECK(s->subs[0].update_pending);

        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_request_ok(&w);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(!s->subs[0].update_pending);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* A REQUEST_OK with no pending update fails closed. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Subscriber: REQUEST_ERROR for an update clears pending ======= *
     *  A failed REQUEST_UPDATE clears the pending state; the subscription stays
     *  established and is terminated by the PUBLISH_DONE(UPDATE_FAILED) the
     *  publisher must follow with (exercised in the publish-done tests). A
     *  REQUEST_ERROR with no pending update is a protocol violation. */
    {
        moq_session_t *s = make_established_ev(MOQ_PERSPECTIVE_CLIENT, 0, 0);
        moq_subscription_t h;
        moq_stream_ref_t ref = establish_subscriber(s, "video", &h);
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init(&cfg);
        cfg.has_forward = true; cfg.forward = false;
        moq_session_update_subscription(s, h, &cfg, 1);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act);
        MOQ_TEST_CHECK(s->subs[0].update_pending);

        uint8_t er[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, er, sizeof(er));
        moq_d18_encode_request_error(&w, 0x8 /* UPDATE_FAILED */, 0,
                                     MOQ_BYTES_LITERAL("no"));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, er,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(!s->subs[0].update_pending);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT(slot_state_for(s, ref), (int)MOQ_SUB_ESTABLISHED);
        /* A second (unsolicited) REQUEST_ERROR has no pending update -> close. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, er,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == End-to-end through SimPair: update applied across the pair ==== */
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
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(client, &sub, 1, &ch),
                              (int)MOQ_OK);
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

        /* Subscriber updates; publisher applies and replies REQUEST_OK. */
        moq_subscription_update_cfg_t ucfg;
        moq_subscription_update_cfg_init(&ucfg);
        ucfg.has_subscriber_priority = true; ucfg.subscriber_priority = 17;
        ucfg.has_forward = true; ucfg.forward = false;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_update_subscription(client, ch, &ucfg,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 32, NULL);

        bool updated = false;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                updated = true;
                MOQ_TEST_CHECK(ev.u.subscribe_updated.subscriber_priority == 17);
                MOQ_TEST_CHECK(ev.u.subscribe_updated.has_forward &&
                               !ev.u.subscribe_updated.forward);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(updated);
        /* The subscriber's pending-update state cleared on REQUEST_OK. */
        MOQ_TEST_CHECK(!client->subs[0].update_pending);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_update");
    return failures != 0;
}
