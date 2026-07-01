/*
 * Draft-18 TRACK_STATUS (§10.14): a finite request -> single terminal response
 * (TRACK_STATUS_OK / REQUEST_ERROR) -> FIN, on its own request bidi via the
 * generic staging + handoff. Covers wire codecs, inbound surfacing (single-read
 * and fragmented), the outbound open-a-bidi path, accept (with LARGEST_OBJECT /
 * EXPIRES + a surfaced Track Properties tail) and reject, the requester-side
 * terminal response + split-FIN drain, auth reject, the protocol-violation
 * matrix, backpressure / pool exhaustion / handoff WOULD_BLOCK, peer teardown,
 * and SimPair end-to-end.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

/* -- Codec round-trips and malformed rejection --------------------- */

static int test_codec(void)
{
    int failures = 0;
    uint8_t buf[256];
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("example.com"),
                            MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 2 };
    moq_bytes_t name = MOQ_BYTES_LITERAL("video");

    /* TRACK_STATUS request round-trip with an auth token. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 5;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("t");
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_track_status(&w, 6, &ns, name, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_TRACK_STATUS);
        moq_bytes_t dp[8];
        moq_d18_track_status_t ts;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_track_status(env.payload, env.payload_len,
                                             dp, 8, &ts), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(ts.request_id, 6);
        MOQ_TEST_CHECK_EQ_SIZE(ts.track_namespace.count, 2);
        MOQ_TEST_CHECK(memcmp(ts.track_name.data, "video", 5) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(ts.params.auth_token_count, 1);
    }

    /* A Track-delivery parameter (SUBSCRIBER_PRIORITY) is rejected on the wire. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_TRACK_STATUS);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);             /* request id */
        moq_buf_write_vi64(&w, 1);             /* ns count */
        moq_buf_write_vi64(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);
        moq_buf_write_vi64(&w, 1);             /* track name len */
        moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
        moq_buf_write_vi64(&w, 1);             /* 1 param */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_SUBSCRIBER_PRIORITY);
        uint8_t pr = 1; moq_buf_write_raw(&w, &pr, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_track_status_t ts;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_track_status(env.payload, env.payload_len,
                                             dp, 8, &ts), (int)MOQ_ERR_PROTO);
    }

    /* TRACK_STATUS_OK round-trip: LARGEST_OBJECT + EXPIRES + Track Properties. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_largest = true; p.largest_group = 7; p.largest_object = 3;
        p.has_expires = true; p.expires_ms = 1000;
        uint8_t props[] = { 0x02, 0x00 };   /* one even property (type 2, value 0) */
        moq_bytes_t pr = { props, sizeof(props) };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_track_status_ok(&w, &p, pr), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_REQUEST_OK);
        moq_d18_track_status_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_track_status_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(ok.params.has_largest);
        MOQ_TEST_CHECK_EQ_U64(ok.params.largest_group, 7);
        MOQ_TEST_CHECK_EQ_U64(ok.params.largest_object, 3);
        MOQ_TEST_CHECK(ok.params.has_expires);
        MOQ_TEST_CHECK_EQ_U64(ok.params.expires_ms, 1000);
        MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, 2);
    }

    /* TRACK_STATUS_OK with no params and empty properties round-trips. */
    {
        moq_d18_msg_params_t p = { 0 };
        moq_bytes_t empty = { NULL, 0 };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_track_status_ok(&w, &p, empty), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_track_status_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_track_status_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(!ok.params.has_largest);
        MOQ_TEST_CHECK_EQ_SIZE(ok.track_properties.len, 0);
    }

    return failures;
}

/* -- Session helpers ----------------------------------------------- */

static moq_session_t *make_session_caps(moq_perspective_t persp,
                                        uint32_t max_events,
                                        uint32_t send_buffer_size,
                                        uint32_t max_track_status)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (send_buffer_size) cfg.send_buffer_size = send_buffer_size;
    if (max_track_status) cfg.max_track_statuses = max_track_status;
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
    moq_action_t a2;
    while (moq_session_poll_actions(s, &a2, 1) > 0) moq_action_cleanup(&a2);
    return s;
}

static moq_session_t *make_session(moq_perspective_t persp)
{
    return make_session_caps(persp, 0, 0, 0);
}

static int count_busy_subs(moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

static size_t encode_ts(uint8_t *buf, size_t cap, uint64_t request_id,
                        const char *ns_field, const char *name,
                        const moq_d18_msg_params_t *p)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)ns_field, strlen(ns_field) } };
    moq_namespace_t ns = { parts, 1 };
    moq_bytes_t tn = { (const uint8_t *)name, strlen(name) };
    moq_d18_encode_track_status(&w, request_id, &ns, tn, p);
    return moq_buf_writer_offset(&w);
}

/* Feed an inbound TRACK_STATUS (first + FIN) on `ref`, return the handle. */
static moq_track_status_handle_t feed_ts(moq_session_t *s, moq_stream_ref_t ref,
                                         uint64_t request_id, const char *field,
                                         const moq_d18_msg_params_t *p)
{
    uint8_t msg[160];
    size_t n = encode_ts(msg, sizeof(msg), request_id, field, "v", p);
    moq_session_on_bidi_stream_bytes(s, ref, msg, n, true, 1);
    moq_track_status_handle_t h = { 0 };
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST)
            h = ev.u.track_status_request.handle;
        moq_event_cleanup(&ev);
    }
    return h;
}

static bool action_has_msg(moq_session_t *s, moq_stream_ref_t ref,
                           uint64_t msg_type, bool *out_fin)
{
    bool seen = false;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
            act.u.send_bidi_stream.stream_ref._v == ref._v) {
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, act.u.send_bidi_stream.data,
                                act.u.send_bidi_stream.len);
            moq_control_envelope_t env;
            if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                env.msg_type == msg_type) {
                seen = true;
                if (out_fin) *out_fin = act.u.send_bidi_stream.fin;
            }
        }
        moq_action_cleanup(&act);
    }
    return seen;
}

/* Open an outbound track_status, returning the bidi ref. */
static moq_stream_ref_t open_ts(moq_session_t *s, moq_track_status_handle_t *h)
{
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    moq_session_track_status(s, &cfg, 1, h);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
            ref = act.u.open_bidi_stream.stream_ref;
        moq_action_cleanup(&act);
    }
    return ref;
}

int main(void)
{
    int failures = 0;
    failures += test_codec();

    /* == Inbound TRACK_STATUS surfaces the namespace/name + token ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 4;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("pk");
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[160];
        size_t n = encode_ts(msg, sizeof(msg), 0, "live", "v", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, true, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.track_status_request.track_namespace.count, 1);
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.track_status_request.token_count, 1);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_TRACK_STATUS);
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);   /* staging slot freed */
        moq_session_destroy(s);
    }

    /* == Inbound fragmented byte-by-byte -> one request event ========= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        uint8_t msg[160];
        size_t n = encode_ts(msg, sizeof(msg), 0, "live", "v", &p);
        for (size_t i = 0; i < n; i++) {
            bool last = (i + 1 == n);
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1, last, 1),
                (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        int reqs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        moq_session_destroy(s);
    }

    /* == Outbound track_status opens a request bidi =================== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_status_handle_t h;
        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("v");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_track_status(s, &cfg, 1, &h), (int)MOQ_OK);
        bool opened = false, open_fin = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.open_bidi_stream.data,
                                    act.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_TRACK_STATUS)
                    opened = true;
                open_fin = act.u.open_bidi_stream.fin;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(opened);
        /* The first-and-only message closes the send half (FIN) on open. */
        MOQ_TEST_CHECK(open_fin);
        moq_session_destroy(s);
    }

    /* == Accept -> TRACK_STATUS_OK (+largest/expires) + FIN =========== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_track_status_handle_t h = feed_ts(s, ref, 0, "live", &p);
        moq_accept_track_status_cfg_t ac;
        moq_accept_track_status_cfg_init(&ac);
        ac.has_largest = true; ac.largest_group = 9; ac.largest_object = 2;
        ac.has_expires = true; ac.expires_ms = 500;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_track_status(s, h, &ac, 1), (int)MOQ_OK);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_OK, &fin));
        MOQ_TEST_CHECK(fin);   /* terminal: FIN after TRACK_STATUS_OK */
        /* The requester's TRACK_STATUS arrived with its FIN (first and only
         * message), so accept leaves no drain ref. */
        MOQ_TEST_CHECK_EQ_SIZE(s->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Reject -> REQUEST_ERROR + FIN ================================ */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_track_status_handle_t h = feed_ts(s, ref, 0, "live", &p);
        moq_reject_track_status_cfg_t rc;
        moq_reject_track_status_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_track_status(s, h, &rc, 1), (int)MOQ_OK);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Auth reject: unknown USE_ALIAS -> REQUEST_ERROR + FIN, no event */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 77;
        uint8_t msg[160];
        size_t n = encode_ts(msg, sizeof(msg), 0, "live", "v", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, true, 1),
            (int)MOQ_OK);
        bool any = false, fin = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST) any = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any);
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Split-FIN pre-commit reject: trailing FIN is drained ========= *
     *  Auth-reject and pool-full reject without committing an entry; if the
     *  request arrived without its FIN, the request bidi must be retired via the
     *  drain ring so a separately-delivered trailing FIN is absorbed, not
     *  reclassified as an empty FIN without a request (session-fatal). */
    {
        /* Auth reject, request fed WITHOUT FIN. */
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 55;
        uint8_t msg[160];
        size_t n = encode_ts(msg, sizeof(msg), 0, "live", "v", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The separately-delivered trailing FIN is drained, session stays up. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);

        /* Pool-full reject, request fed WITHOUT FIN. */
        s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 0, 1);
        moq_d18_msg_params_t q = { 0 };
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        (void)feed_ts(s, r1, 0, "live", &q);          /* fills the 1-slot pool */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        uint8_t m2[160];
        size_t n2 = encode_ts(m2, sizeof(m2), 2, "vod", "v", &q);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, m2, n2, false, 1),
            (int)MOQ_OK);
        bool fin2 = false;
        MOQ_TEST_CHECK(action_has_msg(s, r2, MOQ_D18_REQUEST_ERROR, &fin2));
        MOQ_TEST_CHECK(fin2);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Malformed auth token -> session close 0x6 =================== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[160];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_TRACK_STATUS);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_raw(&w, (const uint8_t *)"v", 1);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 9);             /* invalid alias type */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), true, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Requester decodes the OK incl. Track Properties, drains FIN == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_status_handle_t h;
        moq_stream_ref_t ref = open_ts(s, &h);
        /* Build a TRACK_STATUS_OK with largest/expires + a 2-byte properties tail. */
        moq_d18_msg_params_t p = { 0 };
        p.has_largest = true; p.largest_group = 4; p.largest_object = 1;
        p.has_expires = true; p.expires_ms = 250;
        uint8_t props[] = { 0x02, 0x00 };
        moq_bytes_t pr = { props, sizeof(props) };
        uint8_t ok[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_track_status_ok(&w, &p, pr);
        /* Deliver OK then FIN in one chunk (publisher closes after the response). */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), true, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_OK) {
                got = true;
                MOQ_TEST_CHECK(ev.u.track_status_ok.has_largest);
                MOQ_TEST_CHECK_EQ_U64(ev.u.track_status_ok.largest_group, 4);
                MOQ_TEST_CHECK(ev.u.track_status_ok.has_expires);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.track_status_ok.track_properties.len, 2);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        /* The entry is freed (same-chunk FIN); the stream-ref key is gone. */
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Requester: split FIN after the OK is absorbed (drain) ======== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_status_handle_t h;
        moq_stream_ref_t ref = open_ts(s, &h);
        moq_d18_msg_params_t p = { 0 };
        moq_bytes_t empty = { NULL, 0 };
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_track_status_ok(&w, &p, empty);
        /* OK bytes (no FIN) -> event; entry kept drainable. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_OK) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        /* The trailing FIN frees the entry without reopening state. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Requester: a second response after the terminal OK closes ==== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_track_status_handle_t h;
        moq_stream_ref_t ref = open_ts(s, &h);
        moq_d18_msg_params_t p = { 0 };
        moq_bytes_t empty = { NULL, 0 };
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_track_status_ok(&w, &p, empty);
        size_t oklen = moq_buf_writer_offset(&w);
        moq_session_on_bidi_stream_bytes(s, ref, ok, oklen, false, 1);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        /* A second OK on the draining bidi is a protocol violation. */
        (void)moq_session_on_bidi_stream_bytes(s, ref, ok, oklen, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Handoff WOULD_BLOCK (event queue full): staging retained, retry === *
     *  TRACK_STATUS is reserve-then-commit (FETCH-shaped): on a full event queue
     *  no entry is created, the generic staging slot holds the buffered request
     *  for the re-feed, and nothing is orphaned. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1, 0, 0);
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        moq_d18_msg_params_t p = { 0 };
        uint8_t m1[160], m2[160];
        size_t n1 = encode_ts(m1, sizeof(m1), 0, "live", "v", &p);
        size_t n2 = encode_ts(m2, sizeof(m2), 2, "vod", "v", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r1, m1, n1, true, 1),
            (int)MOQ_OK);                  /* fills the 1-slot event queue */
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);   /* r1 committed + freed */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, m2, n2, true, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        /* r2 is buffered in its staging slot (not yet a track-status entry). */
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 1);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, r2).kind ==
                       MOQ_REQ_SUBSCRIPTION);
        /* Drain one event, retry empty -> the second request commits + frees. */
        moq_event_t ev; int reqs = 0;
        if (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, NULL, 0, false, 1),
            (int)MOQ_OK);
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 2);
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, r2).kind ==
                       MOQ_REQ_TRACK_STATUS);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == accept backpressure is WOULD_BLOCK, not BUFFER, retryable ==== */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 24, 8);
        moq_d18_msg_params_t p = { 0 };
        const char *fields = "abcdefgh";
        moq_track_status_handle_t handles[8];
        for (int i = 0; i < 8; i++) {
            moq_stream_ref_t ref = moq_stream_ref_from_u64((uint64_t)(1 + i));
            char f[2] = { fields[i], 0 };
            handles[i] = feed_ts(s, ref, (uint64_t)(i * 2), f, &p);
        }
        int blocked = -1;
        for (int i = 0; i < 8; i++) {
            moq_accept_track_status_cfg_t ac;
            moq_accept_track_status_cfg_init(&ac);
            ac.has_largest = true; ac.largest_group = 1; ac.largest_object = 1;
            moq_result_t rc = moq_session_accept_track_status(s, handles[i], &ac, 1);
            if (rc == MOQ_ERR_WOULD_BLOCK) { blocked = i; break; }
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }
        MOQ_TEST_CHECK(blocked >= 0);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        moq_accept_track_status_cfg_t ac;
        moq_accept_track_status_cfg_init(&ac);
        ac.has_largest = true; ac.largest_group = 1; ac.largest_object = 1;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_track_status(s, handles[blocked], &ac, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == track-status pool full -> REQUEST_ERROR rejection (no event) == *
     *  An inbound TRACK_STATUS with no free pool slot is rejected with
     *  REQUEST_ERROR on its bidi (not surfaced, not WOULD_BLOCK); the session
     *  stays up and the staging slot is reclaimed. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 0, 1);
        moq_d18_msg_params_t p = { 0 };
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_track_status_handle_t h = feed_ts(s, r1, 0, "live", &p);
        MOQ_TEST_CHECK(h._opaque != 0);
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        uint8_t m2[160];
        size_t n2 = encode_ts(m2, sizeof(m2), 2, "vod", "v", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, m2, n2, true, 1),
            (int)MOQ_OK);
        bool any = false, fin = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST) any = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any);
        MOQ_TEST_CHECK(action_has_msg(s, r2, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);   /* staging reclaimed */
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Publisher: peer RESET tears the entry down, session up ======= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        (void)feed_ts(s, ref, 0, "live", &p);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_TRACK_STATUS);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_reset(s, ref, 0x1, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(request_registry_find_by_streamref(s, ref).kind ==
                       MOQ_REQ_NONE);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == End-to-end accept over SimPair =============================== */
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

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("v");
        moq_track_status_handle_t ch;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_track_status(client, &cfg, 1, &ch), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_track_status_handle_t sh = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST)
                sh = ev.u.track_status_request.handle;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(sh._opaque != 0);
        moq_accept_track_status_cfg_t ac;
        moq_accept_track_status_cfg_init(&ac);
        ac.has_largest = true; ac.largest_group = 12; ac.largest_object = 5;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_track_status(server, sh, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool ok = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_OK) {
                ok = true;
                MOQ_TEST_CHECK(ev.u.track_status_ok.has_largest);
                MOQ_TEST_CHECK_EQ_U64(ev.u.track_status_ok.largest_group, 12);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        /* The requester opened with a FIN (first and only message), which SimPair
         * delivers, so the server retained no drain ref after responding. */
        MOQ_TEST_CHECK_EQ_SIZE(server->drain_ref_count, 0);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == End-to-end reject over SimPair =============================== */
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

        moq_track_status_cfg_t cfg;
        moq_track_status_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace = (moq_namespace_t){ parts, 1 };
        cfg.track_name = MOQ_BYTES_LITERAL("v");
        moq_track_status_handle_t ch;
        moq_session_track_status(client, &cfg, 1, &ch);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_track_status_handle_t sh = { 0 };
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_REQUEST)
                sh = ev.u.track_status_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_reject_track_status_cfg_t rc;
        moq_reject_track_status_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_track_status(server, sh, &rc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool rejected = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_TRACK_STATUS_ERROR) rejected = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(rejected);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    MOQ_TEST_PASS("d18_track_status");
    return failures != 0;
}
