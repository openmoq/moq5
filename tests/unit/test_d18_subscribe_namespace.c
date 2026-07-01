/*
 * Draft-18 SUBSCRIBE_NAMESPACE (§10.18) reusing the namespace-sub machinery via
 * the generic request-bidi staging + one-way handoff. Covers: wire codec
 * round-trips and rejection (SUBSCRIBE_NAMESPACE / NAMESPACE / NAMESPACE_DONE),
 * inbound surfacing (single-read and byte-by-byte fragmented), the handoff
 * registry invariant (idx_ns_by_ref owns the bidi, the stale staging key is
 * reclaimed), the outbound subscriber response path (REQUEST_OK then
 * NAMESPACE / NAMESPACE_DONE), accept/reject, PREFIX_OVERLAP, the session-close
 * protocol guards vs the bidi-only REQUEST_UPDATE guard, auth reject routing,
 * and end-to-end accept + delivery + reject over SimPair.
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
                            MOQ_BYTES_LITERAL("meeting=1") };
    moq_namespace_t ns = { parts, 2 };

    /* SUBSCRIBE_NAMESPACE round-trip with an auth token. */
    {
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 7;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tok");
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_namespace(&w, 4, &ns, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        MOQ_TEST_CHECK_EQ_U64(env.msg_type, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_bytes_t dp[8];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                                    dp, 8, &sn), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(sn.request_id, 4);
        MOQ_TEST_CHECK_EQ_SIZE(sn.track_namespace_prefix.count, 2);
        MOQ_TEST_CHECK(memcmp(sn.track_namespace_prefix.parts[0].data,
                              "example.com", 11) == 0);
        MOQ_TEST_CHECK_EQ_SIZE(sn.params.auth_token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(sn.params.auth_tokens[0].token_type, 7);
    }

    /* Zero-field prefix round-trips (draft-18 allows 0..32). */
    {
        moq_namespace_t empty = { NULL, 0 };
        moq_d18_msg_params_t p = { 0 };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe_namespace(&w, 0, &empty, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                                    dp, 8, &sn), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(sn.track_namespace_prefix.count, 0);
    }

    /* NAMESPACE / NAMESPACE_DONE suffix round-trip. */
    {
        for (int done = 0; done < 2; done++) {
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_encode_namespace_msg(&w, &ns, done != 0), (int)MOQ_OK);
            moq_buf_reader_t r;
            moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
            moq_control_envelope_t env;
            moq_d18_decode_envelope(&r, &env);
            MOQ_TEST_CHECK_EQ_U64(env.msg_type,
                done ? MOQ_D18_NAMESPACE_DONE : MOQ_D18_NAMESPACE);
            moq_bytes_t dp[8];
            moq_namespace_t suffix;
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_d18_decode_namespace_msg(env.payload, env.payload_len,
                                                  dp, 8, &suffix), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_SIZE(suffix.count, 2);
            MOQ_TEST_CHECK(memcmp(suffix.parts[1].data, "meeting=1", 9) == 0);
        }
    }

    /* A non-AUTHORIZATION_TOKEN parameter (FORWARD) is rejected. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);                       /* request id */
        moq_buf_write_vi64(&w, 1);                       /* ns count */
        moq_buf_write_vi64(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);
        moq_buf_write_vi64(&w, 1);                       /* 1 parameter */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_FORWARD);
        uint8_t one = 1; moq_buf_write_raw(&w, &one, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                                    dp, 8, &sn), (int)MOQ_ERR_PROTO);
    }

    /* A malformed auth Token structure -> MOQ_D18_ERR_KVP_FORMAT. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 3);
        moq_buf_write_raw(&w, (const uint8_t *)"abc", 3);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 1);                       /* span len */
        moq_buf_write_vi64(&w, 9);                       /* invalid alias type */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_namespace_t sn;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_namespace(env.payload, env.payload_len,
                                                    dp, 8, &sn),
            (int)MOQ_D18_ERR_KVP_FORMAT);
    }

    return failures;
}

/* -- Session helpers ----------------------------------------------- */

static moq_session_t *make_session(moq_perspective_t persp)
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

/* Session with explicit capacity knobs (for backpressure / pool-exhaustion). */
static moq_session_t *make_session_caps(moq_perspective_t persp,
                                        uint32_t max_events,
                                        uint32_t send_buffer_size,
                                        uint32_t max_ns_subs)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (max_events) cfg.max_events = max_events;
    if (send_buffer_size) cfg.send_buffer_size = send_buffer_size;
    if (max_ns_subs) cfg.max_namespace_subscriptions = max_ns_subs;
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

/* Count non-free generic subscription (staging) slots. */
static int count_busy_subs(moq_session_t *s)
{
    int n = 0;
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state != MOQ_SUB_FREE) n++;
    return n;
}

/* Encode a SUBSCRIBE_NAMESPACE with a single-field prefix. */
static size_t encode_sns(uint8_t *buf, size_t cap, uint64_t request_id,
                         const char *field, const moq_d18_msg_params_t *p)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { { (const uint8_t *)field, strlen(field) } };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_encode_subscribe_namespace(&w, request_id, &ns, p);
    return moq_buf_writer_offset(&w);
}

/* Feed an inbound SUBSCRIBE_NAMESPACE on `ref`, return the surfaced handle. */
static moq_ns_sub_handle_t feed_sns(moq_session_t *s, moq_stream_ref_t ref,
                                    uint64_t request_id, const char *field,
                                    const moq_d18_msg_params_t *p)
{
    uint8_t msg[128];
    size_t n = encode_sns(msg, sizeof(msg), request_id, field, p);
    moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
    moq_ns_sub_handle_t h = MOQ_NS_SUB_HANDLE_INVALID;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
            h = ev.u.ns_sub_request.handle;
        moq_event_cleanup(&ev);
    }
    return h;
}

/* Scan queued actions for a message of `msg_type` on `ref`. */
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

/* Count queued RESET/STOP actions on `ref`. */
static void count_teardown(moq_session_t *s, moq_stream_ref_t ref,
                           int *resets, int *stops)
{
    *resets = 0; *stops = 0;
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        if (act.kind == MOQ_ACTION_RESET_BIDI_STREAM &&
            act.u.reset_bidi_stream.stream_ref._v == ref._v) (*resets)++;
        if (act.kind == MOQ_ACTION_STOP_BIDI_STREAM &&
            act.u.stop_bidi_stream.stream_ref._v == ref._v) (*stops)++;
        moq_action_cleanup(&act);
    }
}

int main(void)
{
    int failures = 0;
    failures += test_codec();

    /* == Inbound SUBSCRIBE_NAMESPACE surfaces the prefix + token ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 9;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("pk");
        uint8_t msg[128];
        size_t n = encode_sns(msg, sizeof(msg), 0, "live", &p);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.ns_sub_request.track_namespace_prefix.count, 1);
                MOQ_TEST_CHECK(memcmp(
                    ev.u.ns_sub_request.track_namespace_prefix.parts[0].data,
                    "live", 4) == 0);
                MOQ_TEST_CHECK_EQ_U64(ev.u.ns_sub_request.namespace_interest,
                                      MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.ns_sub_request.token_count, 1);
                MOQ_TEST_CHECK_EQ_U64(ev.u.ns_sub_request.tokens[0].token_type, 9);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        /* Handoff registry invariant: idx_ns_by_ref owns the bidi and the stale
         * request-registry streamref key was reclaimed. */
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) >= 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, ref).kind,
            (int)MOQ_REQ_NONE);
        moq_session_destroy(s);
    }

    /* == Inbound fragmented byte-by-byte -> same event ================ */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        uint8_t msg[128];
        size_t n = encode_sns(msg, sizeof(msg), 0, "live", &p);
        for (size_t i = 0; i < n; i++) {
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, &msg[i], 1,
                    false, 1), (int)MOQ_OK);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        }
        int reqs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 1);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      ref._v) >= 0);
        moq_session_destroy(s);
    }

    /* == Outbound subscribe_namespace opens a request bidi ============ */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(s, &cfg, 1, &h), (int)MOQ_OK);
        bool opened = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.open_bidi_stream.data,
                                    act.u.open_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_SUBSCRIBE_NAMESPACE)
                    opened = true;
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(opened);
        moq_session_destroy(s);
    }

    /* == Inbound accept -> REQUEST_OK (fin=false), then NAMESPACE ====== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_ns_sub_handle_t h = feed_sns(s, ref, 0, "live", &p);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_ns_sub(s, h, &ac, 1), (int)MOQ_OK);
        bool fin = true;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_OK, &fin));
        MOQ_TEST_CHECK(!fin);
        /* send_namespace -> NAMESPACE on the bidi. */
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_namespace(s, h, &suffix, 1), (int)MOQ_OK);
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_NAMESPACE, NULL));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound reject -> REQUEST_ERROR + FIN; trailing FIN absorbed == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_ns_sub_handle_t h = feed_sns(s, ref, 0, "live", &p);
        moq_reject_ns_sub_cfg_t rc;
        moq_reject_ns_sub_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        rc.reason = MOQ_BYTES_LITERAL("nope");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_ns_sub(s, h, &rc, 1), (int)MOQ_OK);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The peer FINs its request half after the error; absorbed via drain. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == PREFIX_OVERLAP: a second overlapping request is rejected ===== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_ns_sub_handle_t h = feed_sns(s, r1, 0, "live", &p);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        moq_session_accept_ns_sub(s, h, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        /* Same prefix on a new bidi (request id 2): PREFIX_OVERLAP, no event. */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(2);
        uint8_t msg[128];
        size_t n = encode_sns(msg, sizeof(msg), 2, "live", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, msg, n, false, 1),
            (int)MOQ_OK);
        int reqs = 0;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 0);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, r2, MOQ_D18_REQUEST_ERROR, &fin));
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
        p.auth_tokens[0].alias = 99;
        uint8_t msg[128];
        size_t n = encode_sns(msg, sizeof(msg), 0, "live", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool any = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) any = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any);
        bool fin = false;
        MOQ_TEST_CHECK(action_has_msg(s, ref, MOQ_D18_REQUEST_ERROR, &fin));
        MOQ_TEST_CHECK(fin);
        /* The rejected bidi is drained: a trailing FIN is absorbed. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Malformed auth token closes the session with 0x6 ============= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 9);                       /* invalid alias type */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == A prefix with >32 fields closes the session ================== */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        uint8_t msg[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_NAMESPACE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 33);                      /* 33 > 32 fields */
        for (int i = 0; i < 33; i++) {
            moq_buf_write_vi64(&w, 1);
            uint8_t c = 'a'; moq_buf_write_raw(&w, &c, 1);
        }
        moq_buf_write_vi64(&w, 0);                       /* 0 params */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == REQUEST_UPDATE guard: bidi-only close, session stays up ======= *
     *  An inbound REQUEST_UPDATE on an established publisher-side ns_sub bidi is
     *  unmodelled; §10.9.1 closes only the bidi (RESET+STOP), not the session,
     *  and a second namespace subscription still works. */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        moq_d18_msg_params_t p = { 0 };
        moq_ns_sub_handle_t h = feed_sns(s, ref, 0, "live", &p);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        moq_session_accept_ns_sub(s, h, &ac, 1);
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);

        moq_d18_msg_params_t up = { 0 };
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &up);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        int resets = 0, stops = 0;
        count_teardown(s, ref, &resets, &stops);
        MOQ_TEST_CHECK_EQ_INT(resets, 1);
        MOQ_TEST_CHECK_EQ_INT(stops, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* The bidi is gone; a late in-flight byte is drained, not reopened. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
            (int)MOQ_OK);
        /* A second, fresh ns_sub on a new bidi still works. */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        moq_ns_sub_handle_t h2 = feed_sns(s, r2, 2, "vod", &p);
        MOQ_TEST_CHECK(h2._opaque != MOQ_NS_SUB_HANDLE_INVALID._opaque);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Subscriber response path (white-box): REQUEST_OK then NAMESPACE */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        moq_session_subscribe_namespace(s, &cfg, 1, &h);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        /* REQUEST_OK -> NS_SUB_OK. */
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_request_ok(&w);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, ok,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool ok_ev = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_OK) ok_ev = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok_ev);
        /* NAMESPACE (suffix) -> NAMESPACE_FOUND; then NAMESPACE_DONE -> GONE. */
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp, 1 };
        uint8_t nm[64];
        moq_buf_writer_init(&w, nm, sizeof(nm));
        moq_d18_encode_namespace_msg(&w, &suffix, false);
        moq_session_on_bidi_stream_bytes(s, ref, nm,
            moq_buf_writer_offset(&w), false, 1);
        bool found = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_FOUND) found = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(found);
        moq_buf_writer_init(&w, nm, sizeof(nm));
        moq_d18_encode_namespace_msg(&w, &suffix, true);
        moq_session_on_bidi_stream_bytes(s, ref, nm,
            moq_buf_writer_offset(&w), false, 1);
        bool gone = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NAMESPACE_GONE) gone = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(gone);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Subscriber: NAMESPACE_DONE before NAMESPACE closes the session  */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        moq_session_subscribe_namespace(s, &cfg, 1, &h);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        uint8_t ok[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, ok, sizeof(ok));
        moq_d18_encode_request_ok(&w);
        moq_session_on_bidi_stream_bytes(s, ref, ok,
            moq_buf_writer_offset(&w), false, 1);
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) moq_event_cleanup(&ev);
        /* NAMESPACE_DONE for a suffix never announced -> PROTOCOL_VIOLATION. */
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp, 1 };
        uint8_t nm[64];
        moq_buf_writer_init(&w, nm, sizeof(nm));
        moq_d18_encode_namespace_msg(&w, &suffix, true);
        (void)moq_session_on_bidi_stream_bytes(s, ref, nm,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Subscriber: a non-OK/ERROR first response closes the session == */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        moq_session_subscribe_namespace(s, &cfg, 1, &h);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_OPEN_BIDI_STREAM)
                ref = act.u.open_bidi_stream.stream_ref;
            moq_action_cleanup(&act);
        }
        /* NAMESPACE as the first response message is a PROTOCOL_VIOLATION. */
        moq_bytes_t sp[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp, 1 };
        uint8_t nm[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, nm, sizeof(nm));
        moq_d18_encode_namespace_msg(&w, &suffix, false);
        (void)moq_session_on_bidi_stream_bytes(s, ref, nm,
            moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Subscriber cancel -> CLOSE_BIDI (FIN), draft-18 §2249 ========= */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        moq_session_subscribe_namespace(s, &cfg, 1, &h);
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) moq_action_cleanup(&act);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_cancel_namespace_sub(s, h, 1), (int)MOQ_OK);
        bool closed = false;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_CLOSE_BIDI_STREAM) closed = true;
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(closed);
        moq_session_destroy(s);
    }

    /* == End-to-end accept + delivery over SimPair ==================== */
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

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t ch;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(client, &cfg, 1, &ch), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_ns_sub_handle_t shdl = MOQ_NS_SUB_HANDLE_INVALID;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
                shdl = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(shdl._opaque != MOQ_NS_SUB_HANDLE_INVALID._opaque);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_ns_sub(server, shdl, &ac,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_bytes_t sp2[] = { MOQ_BYTES_LITERAL("room=1") };
        moq_namespace_t suffix = { sp2, 1 };
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_send_namespace(server, shdl, &suffix,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool ok = false, found = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_OK) ok = true;
            if (ev.kind == MOQ_EVENT_NAMESPACE_FOUND) {
                found = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.namespace_found.track_namespace_suffix.count, 1);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(ok);
        MOQ_TEST_CHECK(found);
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

        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t ch;
        moq_session_subscribe_namespace(client, &cfg, 1, &ch);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        moq_ns_sub_handle_t shdl = MOQ_NS_SUB_HANDLE_INVALID;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST)
                shdl = ev.u.ns_sub_request.handle;
            moq_event_cleanup(&ev);
        }
        moq_reject_ns_sub_cfg_t rc;
        moq_reject_ns_sub_cfg_init(&rc);
        rc.error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        rc.reason = MOQ_BYTES_LITERAL("no");
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_reject_ns_sub(server, shdl, &rc,
                moq_simpair_now_us(sp)), (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool rejected = false;
        while (moq_session_poll_events(client, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_ERROR) rejected = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(rejected);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Unrepresentable namespace_interest returns INVAL (no silent drop) === */
    {
        moq_session_t *s = make_session(MOQ_PERSPECTIVE_CLIENT);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        moq_ns_sub_handle_t h;
        /* Default cfg interest is PUBLISHER_STATE(0); BOTH(2) is also track-bearing.
         * Both need SUBSCRIBE_TRACKS (deferred), so D18 rejects them. */
        moq_subscribe_namespace_cfg_t cfg;
        moq_subscribe_namespace_cfg_init(&cfg);
        cfg.track_namespace_prefix = (moq_namespace_t){ parts, 1 };
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_PUBLISHER_STATE;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(s, &cfg, 1, &h), (int)MOQ_ERR_INVAL);
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_BOTH;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(s, &cfg, 1, &h), (int)MOQ_ERR_INVAL);
        /* NAMESPACE_STATE is representable. */
        cfg.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_subscribe_namespace(s, &cfg, 1, &h), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Direct FIN on a publisher-side ns_sub tears the entry down ===== *
     *  A real bridge CLOSE_BIDI_STREAM delivers a FIN inline (not a reset);
     *  §2249 makes that a cancellation. The entry frees, session stays up. */
    {
        for (int established = 0; established < 2; established++) {
            moq_session_t *s = make_session(MOQ_PERSPECTIVE_SERVER);
            moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
            moq_d18_msg_params_t p = { 0 };
            moq_ns_sub_handle_t h = feed_sns(s, ref, 0, "live", &p);
            if (established) {
                moq_accept_ns_sub_cfg_t ac;
                moq_accept_ns_sub_cfg_init(&ac);
                moq_session_accept_ns_sub(s, h, &ac, 1);
            }
            moq_action_t a;
            while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
            MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                          ref._v) >= 0);
            /* FIN (len=0) directly on the publisher-side bidi. */
            MOQ_TEST_CHECK_EQ_INT(
                (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, true, 1),
                (int)MOQ_OK);
            MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                          ref._v) < 0);
            MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
            moq_session_destroy(s);
        }
    }

    /* == WOULD_BLOCK at the handoff event push: no orphaned staging slot = *
     *  max_events=1: the first request fills the event queue, so the second
     *  request's handoff event push WOULD_BLOCKs. The staging slot must be freed
     *  (no stuck sub slot, no stale registry key) and the empty re-feed must
     *  complete via the ns_sub path. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1, 0, 0);
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        moq_d18_msg_params_t p = { 0 };
        uint8_t m1[128], m2[128];
        size_t n1 = encode_sns(m1, sizeof(m1), 0, "live", &p);
        size_t n2 = encode_sns(m2, sizeof(m2), 2, "vod", &p);
        /* First request: emits NS_SUB_REQUEST, fills the 1-slot event queue. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r1, m1, n1, false, 1),
            (int)MOQ_OK);
        /* Second request: handoff created the ns_sub entry, event push blocks. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, m2, n2, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        /* The staging slot was freed despite WOULD_BLOCK: no stuck slot, and the
         * request registry's stale streamref key was reclaimed. */
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);
        MOQ_TEST_CHECK_EQ_INT(
            (int)request_registry_find_by_streamref(s, r2).kind, (int)MOQ_REQ_NONE);
        MOQ_TEST_CHECK(moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                      r2._v) >= 0);
        /* Drain one event, then retry empty -> the second request completes. */
        moq_event_t ev; int reqs = 0;
        if (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, NULL, 0, false, 1),
            (int)MOQ_OK);
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) reqs++;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK_EQ_INT(reqs, 2);
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Trailing bytes after the request are rejected on the blocked path = *
     *  Full event queue + (SUBSCRIBE_NAMESPACE followed by an extra byte in the
     *  same feed): the handoff reports the request consumed even though the event
     *  push blocks, so the trailing byte is a protocol violation -- it must close,
     *  not be silently dropped with the freed staging slot and then commit on the
     *  empty re-feed. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 1, 0, 0);
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        moq_d18_msg_params_t p = { 0 };
        uint8_t m1[128], m2[129];
        size_t n1 = encode_sns(m1, sizeof(m1), 0, "live", &p);
        size_t n2 = encode_sns(m2, sizeof(m2), 2, "vod", &p);
        m2[n2] = 0x00;                 /* one trailing byte after the envelope */
        /* Fill the 1-slot event queue. */
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r1, m1, n1, false, 1),
            (int)MOQ_OK);
        /* Request + trailing byte while the event queue is full: closes. */
        (void)moq_session_on_bidi_stream_bytes(s, r2, m2, n2 + 1, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Send-buffer backpressure on accept is WOULD_BLOCK, not BUFFER === *
     *  A temporary remaining-buffer shortfall must be retryable (queue_send_bidi),
     *  not collapse to a hard BUFFER that would lose the response. */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 16, 8);
        moq_d18_msg_params_t p = { 0 };
        const char *fields = "abcdefgh";
        moq_ns_sub_handle_t handles[8];
        for (int i = 0; i < 8; i++) {
            moq_stream_ref_t ref = moq_stream_ref_from_u64((uint64_t)(1 + i));
            char f[2] = { fields[i], 0 };
            handles[i] = feed_sns(s, ref, (uint64_t)(i * 2), f, &p);
        }
        /* Accept (REQUEST_OK) without polling actions until the send buffer is
         * exhausted: the blocking accept must be WOULD_BLOCK (retryable). */
        int blocked = -1;
        for (int i = 0; i < 8; i++) {
            moq_accept_ns_sub_cfg_t ac;
            moq_accept_ns_sub_cfg_init(&ac);
            moq_result_t rc = moq_session_accept_ns_sub(s, handles[i], &ac, 1);
            if (rc == MOQ_ERR_WOULD_BLOCK) { blocked = i; break; }
            MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
        }
        MOQ_TEST_CHECK(blocked >= 0);   /* WOULD_BLOCK occurred (not BUFFER) */
        /* Drain actions (reclaims the send buffer), retry -> succeeds. */
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_ns_sub(s, handles[blocked], &ac, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == ns_sub pool full -> WOULD_BLOCK, retryable, no orphaned staging == */
    {
        moq_session_t *s = make_session_caps(MOQ_PERSPECTIVE_SERVER, 0, 0, 1);
        moq_d18_msg_params_t p = { 0 };
        moq_stream_ref_t r1 = moq_stream_ref_from_u64(1);
        moq_ns_sub_handle_t h = feed_sns(s, r1, 0, "live", &p);
        MOQ_TEST_CHECK(h._opaque != MOQ_NS_SUB_HANDLE_INVALID._opaque);
        /* Pool (cap 1) is full; a second request WOULD_BLOCKs without orphaning
         * the staging slot (it is retained for the re-feed). */
        moq_stream_ref_t r2 = moq_stream_ref_from_u64(3);
        uint8_t m2[128];
        size_t n2 = encode_sns(m2, sizeof(m2), 2, "vod", &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, m2, n2, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* Free the first (subscriber cancels via FIN), then retry the second. */
        moq_action_t a;
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        moq_accept_ns_sub_cfg_t ac;
        moq_accept_ns_sub_cfg_init(&ac);
        moq_session_accept_ns_sub(s, h, &ac, 1);
        moq_session_on_bidi_stream_bytes(s, r1, NULL, 0, true, 1);  /* FIN cancel */
        while (moq_session_poll_actions(s, &a, 1) > 0) moq_action_cleanup(&a);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, r2, NULL, 0, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_NS_SUB_REQUEST) got = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT(count_busy_subs(s), 0);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_subscribe_namespace");
    return failures != 0;
}
