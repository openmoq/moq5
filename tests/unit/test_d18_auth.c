/*
 * Draft-18 AUTHORIZATION_TOKEN message parameter (§10.2.2) on SUBSCRIBE, FETCH
 * and REQUEST_UPDATE. Covers the Token-structure codec (round-trip of every
 * alias type, repeated tokens, malformed/over-cap/wrong-message rejection with
 * the right close code), inbound surfacing of resolved tokens on the request
 * events, the message-level reject and session-fatal routing, the ABI-additive
 * SUBSCRIBE_UPDATED event growth, and an end-to-end exchange over SimPair.
 */
#include <moq/moq.h>
#include <moq/control_d18.h>
#include <moq/sim.h>
#include "test_support.h"
#include <stdlib.h>
#include "../../core/src/session/session_internal.h"

/* -- Codec round-trips and malformed rejection --------------------- */

static int test_codec(void)
{
    int failures = 0;
    uint8_t buf[256];
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_bytes_t tn = MOQ_BYTES_LITERAL("video");

    /* SUBSCRIBE carrying two USE_VALUE tokens (the repeatable form) round-trips,
     * preserving order and values; AUTHORIZATION_TOKEN sorts before the other
     * params (0x03 < 0x20). */
    {
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 5;
        p.auth_token_count = 2;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 4;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("alpha");
        p.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[1].token_type = 9;
        p.auth_tokens[1].token_value = MOQ_BYTES_LITERAL("bravo!");
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, 4, &ns, tn, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp, 8,
                                          &sub), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(sub.params.auth_token_count, 2);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[0].alias_type,
                              MOQ_AUTH_TOKEN_USE_VALUE);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[0].token_type, 4);
        MOQ_TEST_CHECK_EQ_SIZE(sub.params.auth_tokens[0].token_value.len, 5);
        MOQ_TEST_CHECK(memcmp(sub.params.auth_tokens[0].token_value.data,
                              "alpha", 5) == 0);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[1].token_type, 9);
        MOQ_TEST_CHECK(memcmp(sub.params.auth_tokens[1].token_value.data,
                              "bravo!", 6) == 0);
        MOQ_TEST_CHECK(sub.params.has_subscriber_priority &&
                       sub.params.subscriber_priority == 5);
    }

    /* Each alias type round-trips its Token structure (REGISTER, DELETE,
     * USE_ALIAS, USE_VALUE). */
    {
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 4;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_REGISTER;
        p.auth_tokens[0].alias = 3; p.auth_tokens[0].token_type = 7;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("val");
        p.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_DELETE;
        p.auth_tokens[1].alias = 1;
        p.auth_tokens[2].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[2].alias = 2;
        p.auth_tokens[3].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[3].token_type = 0;
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_subscribe(&w, 0, &ns, tn, &p), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp, 8,
                                          &sub), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(sub.params.auth_token_count, 4);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[0].alias_type,
                              MOQ_AUTH_TOKEN_REGISTER);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[0].alias, 3);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[0].token_type, 7);
        MOQ_TEST_CHECK(memcmp(sub.params.auth_tokens[0].token_value.data,
                              "val", 3) == 0);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[1].alias_type,
                              MOQ_AUTH_TOKEN_DELETE);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[1].alias, 1);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[2].alias_type,
                              MOQ_AUTH_TOKEN_USE_ALIAS);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[2].alias, 2);
        MOQ_TEST_CHECK_EQ_U64(sub.params.auth_tokens[3].alias_type,
                              MOQ_AUTH_TOKEN_USE_VALUE);
    }

    /* FETCH also carries the token (its permitted set includes 0x03). */
    {
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        f.track_namespace = ns; f.track_name = tn;
        f.end = (moq_d18_location_t){ 10, 0 };
        f.params.auth_token_count = 1;
        f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        f.params.auth_tokens[0].token_type = 2;
        f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("tok");
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_d18_encode_fetch(&w, &f), (int)MOQ_OK);
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_fetch_t out;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_fetch(env.payload, env.payload_len, dp, 8, &out),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(out.params.auth_token_count, 1);
        MOQ_TEST_CHECK(memcmp(out.params.auth_tokens[0].token_value.data,
                              "tok", 3) == 0);
    }

    /* A malformed Token structure (alias type out of range) is reported as
     * MOQ_D18_ERR_KVP_FORMAT so the profile can close with 0x6. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);                       /* request id */
        moq_buf_write_vi64(&w, 1);                       /* ns count */
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 5);
        moq_buf_write_raw(&w, (const uint8_t *)"video", 5);
        moq_buf_write_vi64(&w, 1);                       /* 1 parameter */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        /* Length-prefixed value: alias type 9 (invalid). */
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 9);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp, 8,
                                          &sub), (int)MOQ_D18_ERR_KVP_FORMAT);
    }

    /* A Token structure with trailing bytes after the value is malformed
     * (DELETE has no value). */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 5);
        moq_buf_write_raw(&w, (const uint8_t *)"video", 5);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 3);          /* span length 3 */
        moq_buf_write_vi64(&w, MOQ_AUTH_TOKEN_DELETE);
        moq_buf_write_vi64(&w, 1);          /* alias */
        moq_buf_write_vi64(&w, 7);          /* trailing junk */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp, 8,
                                          &sub), (int)MOQ_D18_ERR_KVP_FORMAT);
    }

    /* AUTHORIZATION_TOKEN is not permitted on SUBSCRIBE_OK: a 0x03 parameter
     * there is a plain PROTOCOL_VIOLATION (not in the allowed mask). */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE_OK);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 7);                       /* track alias */
        moq_buf_write_vi64(&w, 1);                       /* 1 parameter */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 2);
        moq_buf_write_vi64(&w, MOQ_AUTH_TOKEN_USE_VALUE);
        moq_buf_write_vi64(&w, 0);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_d18_subscribe_ok_t ok;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe_ok(env.payload, env.payload_len, &ok),
            (int)MOQ_ERR_PROTO);
    }

    /* A zero Type-Delta on a non-repeatable parameter (two FORWARDs) is a
     * duplicate -> PROTOCOL_VIOLATION (the repeat exemption is auth-token only). */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 5);
        moq_buf_write_raw(&w, (const uint8_t *)"video", 5);
        moq_buf_write_vi64(&w, 2);                       /* 2 parameters */
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_FORWARD);   /* delta -> 0x10 */
        uint8_t one = 1; moq_buf_write_raw(&w, &one, 1);
        moq_buf_write_vi64(&w, 0);                       /* zero delta: repeat */
        moq_buf_write_raw(&w, &one, 1);
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp, 8,
                                          &sub), (int)MOQ_ERR_PROTO);
    }

    /* More than MOQ_D18_MAX_AUTH_TOKENS tokens -> PROTOCOL_VIOLATION. */
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 5);
        moq_buf_write_raw(&w, (const uint8_t *)"video", 5);
        moq_buf_write_vi64(&w, MOQ_D18_MAX_AUTH_TOKENS + 1);
        /* First token: full delta to 0x03; the rest: zero delta (repeat). */
        for (size_t i = 0; i < MOQ_D18_MAX_AUTH_TOKENS + 1; i++) {
            moq_buf_write_vi64(&w,
                i == 0 ? MOQ_D18_PARAM_AUTHORIZATION_TOKEN : 0);
            moq_buf_write_vi64(&w, 2);                   /* span len */
            moq_buf_write_vi64(&w, MOQ_AUTH_TOKEN_USE_VALUE);
            moq_buf_write_vi64(&w, 0);                   /* token type */
        }
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, moq_buf_writer_offset(&w));
        moq_control_envelope_t env;
        moq_d18_decode_envelope(&r, &env);
        moq_bytes_t dp[8];
        moq_d18_subscribe_t sub;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_decode_subscribe(env.payload, env.payload_len, dp, 8,
                                          &sub), (int)MOQ_ERR_PROTO);
    }

    return failures;
}

/* -- Session helpers ----------------------------------------------- */

static moq_session_t *make_server_ex(uint64_t cache_size)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    cfg.version = MOQ_VERSION_DRAFT_18;
    if (cache_size > 0) {
        cfg.send_auth_token_cache_size = true;
        cfg.auth_token_cache_size = cache_size;
    }
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

static moq_session_t *make_server(void) { return make_server_ex(0); }

/* Encode a SUBSCRIBE with the supplied parameters into buf. */
static size_t encode_subscribe(uint8_t *buf, size_t cap, uint64_t request_id,
                               const moq_d18_msg_params_t *p)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_namespace_t ns = { parts, 1 };
    moq_d18_encode_subscribe(&w, request_id, &ns, MOQ_BYTES_LITERAL("video"), p);
    return moq_buf_writer_offset(&w);
}

/* Publisher side: feed SUBSCRIBE on `ref`, accept -> ESTABLISHED. */
static void establish_publisher(moq_session_t *s, uint64_t request_id,
                                moq_stream_ref_t ref)
{
    uint8_t msg[128];
    moq_d18_msg_params_t mp = { 0 };
    size_t n = encode_subscribe(msg, sizeof(msg), request_id, &mp);
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
}

/* Drain any queued actions, returning whether a REQUEST_ERROR with `code` was
 * seen on the request bidi (`expect_fin`). */
static bool drain_request_error(moq_session_t *s, moq_stream_ref_t ref,
                                uint64_t code, bool expect_fin)
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
                env.msg_type == MOQ_D18_REQUEST_ERROR) {
                moq_d18_request_error_t err;
                if (moq_d18_decode_request_error(env.payload, env.payload_len,
                                                 &err) == MOQ_OK &&
                    err.error_code == code &&
                    act.u.send_bidi_stream.fin == expect_fin)
                    seen = true;
            }
        }
        moq_action_cleanup(&act);
    }
    return seen;
}


/* Configured client/server SETUP is copied before startup. Exercise both wire
 * profiles with real action delivery, including credentials beyond 256 bytes. */
static int test_owned_setup(void)
{
    int failures = 0;
    for (unsigned draft = 0; draft < 2; ++draft) {
        moq_version_t version = draft ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        uint8_t *value = malloc(1024);
        MOQ_TEST_CHECK(value != NULL);
        if (!value) continue;
        for (size_t i = 0; i < 1024; ++i) value[i] = (uint8_t)i;
        uint8_t authority[] = "relay.example:4433";
        uint8_t path[] = "/moq";
        moq_auth_token_t tokens[] = {{16, {value, 1024}},
                                    {1, MOQ_BYTES_LITERAL("client-cat")},
                                    {16, {NULL, 0}}};
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        cfg.version = version;
        cfg.setup_auth_tokens = tokens; cfg.setup_auth_token_count = 3;
        cfg.setup_authority = (moq_bytes_t){authority, sizeof(authority)-1};
        cfg.setup_path = (moq_bytes_t){path, sizeof(path)-1};
        cfg.send_request_capacity = true; cfg.initial_request_capacity = 128;
        cfg.send_auth_token_cache_size = true; cfg.auth_token_cache_size = 128;
        moq_session_t *peers[2] = {NULL, NULL};
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &peers[0]) == MOQ_OK);
        if (!peers[0]) { free(value); continue; }
        size_t exact = peers[0]->setup_wire_len;
        /* The complete frame, including route and cache/request options, is
         * accepted at exactly the budget and refused one byte below it. */
        cfg.send_buffer_size = (uint32_t)exact;
        moq_session_t *boundary = NULL;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &boundary) == MOQ_OK);
        moq_session_destroy(boundary); boundary = NULL;
        cfg.send_buffer_size--;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &boundary) == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(boundary == NULL);
        cfg.send_buffer_size = 4096;
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &boundary) == MOQ_ERR_INVAL);
        cfg.setup_authority = (moq_bytes_t){0}; cfg.setup_path = (moq_bytes_t){0};
        MOQ_TEST_CHECK(moq_session_create(&cfg, 0, &peers[1]) == MOQ_OK);
        memset(value, 0xcc, 1024); free(value);
        memset(tokens, 0xff, sizeof(tokens));
        memset(authority, 0xee, sizeof(authority)); memset(path, 0xdd, sizeof(path));
        MOQ_TEST_CHECK(moq_session_start(peers[0], 0) == MOQ_OK);
        if (draft) MOQ_TEST_CHECK(moq_session_start(peers[1], 0) == MOQ_OK);
        for (size_t side = 0; side < 2; ++side) {
            moq_action_t a;
            bool captured = false;
            while (moq_session_poll_actions(peers[side], &a, 1) == 1) {
                const uint8_t *wire = NULL; size_t len = 0;
                if (a.kind == MOQ_ACTION_SEND_CONTROL) {
                    wire = a.u.send_control.data; len = a.u.send_control.len;
                } else if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
                    wire = a.u.open_uni_control.data; len = a.u.open_uni_control.len;
                }
                if (wire) {
                    captured = true;
                    moq_buf_reader_t r; moq_control_envelope_t env;
                    moq_buf_reader_init(&r, wire, len);
                    MOQ_TEST_CHECK((draft ? moq_d18_decode_envelope(&r, &env) :
                        moq_control_decode_envelope(&r, &env)) == MOQ_OK);
                    bool got_path = false, got_authority = false;
                    if (draft) {
                        moq_buf_reader_init(&r, env.payload, env.payload_len);
                        uint64_t type = 0, delta, n;
                        while (moq_buf_reader_remaining(&r)) {
                            MOQ_TEST_CHECK(moq_buf_read_vi64(&r, &delta) == MOQ_OK);
                            type += delta;
                            MOQ_TEST_CHECK(moq_buf_read_vi64(&r, &n) == MOQ_OK);
                            if (!(type & 1)) continue;
                            if (type == 1) {
                                got_path = true;
                                MOQ_TEST_CHECK(n == 4 && memcmp(r.data + r.pos, "/moq", 4) == 0);
                            }
                            if (type == 5) {
                                got_authority = true;
                                MOQ_TEST_CHECK(n == 18 && memcmp(r.data + r.pos, "relay.example:4433", 18) == 0);
                            }
                            r.pos += (size_t)n;
                        }
                    } else {
                        moq_kvp_entry_t kv[16];
                        moq_d16_setup_t setup = {.params=kv, .params_cap=16};
                        MOQ_TEST_CHECK(moq_d16_decode_client_setup(env.payload, env.payload_len, &setup) == MOQ_OK);
                        for (size_t i = 0; i < setup.params_count; ++i) {
                            if (kv[i].type == 1) {
                                got_path = true;
                                MOQ_TEST_CHECK(kv[i].value_len == 4 && memcmp(kv[i].value, "/moq", 4) == 0);
                            }
                            if (kv[i].type == 5) {
                                got_authority = true;
                                MOQ_TEST_CHECK(kv[i].value_len == 18 && memcmp(kv[i].value, "relay.example:4433", 18) == 0);
                            }
                        }
                    }
                    MOQ_TEST_CHECK(got_path == (side == 0));
                    MOQ_TEST_CHECK(got_authority == (side == 0));
                    MOQ_TEST_CHECK(moq_session_on_control_bytes(peers[1-side], wire, len, 0) == MOQ_OK);
                }
                moq_action_cleanup(&a);
            }
            MOQ_TEST_CHECK(captured);
        }
        for (size_t side = 0; side < 2; ++side) {
            moq_event_t e;
            MOQ_TEST_CHECK(moq_session_state(peers[side]) == MOQ_SESS_ESTABLISHED);
            MOQ_TEST_CHECK(moq_session_poll_events(peers[side], &e, 1) == 1);
            MOQ_TEST_CHECK(e.kind == MOQ_EVENT_SETUP_COMPLETE);
            MOQ_TEST_CHECK(e.u.setup_complete.token_count == 3);
            if (e.u.setup_complete.token_count == 3) {
                const moq_resolved_token_t *t = e.u.setup_complete.tokens;
                MOQ_TEST_CHECK(t[0].token_type == 16 && t[0].token_value.len == 1024);
                for (size_t i = 0; i < t[0].token_value.len; ++i)
                    MOQ_TEST_CHECK(t[0].token_value.data[i] == (uint8_t)i);
                MOQ_TEST_CHECK(t[1].token_type == 1 && t[1].token_value.len == 10);
                MOQ_TEST_CHECK(memcmp(t[1].token_value.data, "client-cat", 10) == 0);
                MOQ_TEST_CHECK(t[2].token_type == 16 && t[2].token_value.len == 0);
            }
            moq_event_cleanup(&e);
            moq_session_destroy(peers[side]);
        }
    }
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_codec();
    failures += test_owned_setup();

    /* == Incoming SETUP preserves an opaque binary token ============== */
    {
        const uint8_t payload[] = {0xd2, 0x84, 0x40, 0x00, 0x80, 0xff};
        moq_session_cfg_t cfg;
        moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_SERVER);
        cfg.version = MOQ_VERSION_DRAFT_18;
        moq_session_t *s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_create(&cfg, 0, &s),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
        moq_action_t action;
        while (moq_session_poll_actions(s, &action, 1) > 0)
            moq_action_cleanup(&action);

        uint8_t setup[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, setup, sizeof(setup));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_STREAM_SETUP);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, MOQ_D18_SETUP_OPT_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 2 + sizeof(payload));
        moq_buf_write_vi64(&w, MOQ_AUTH_TOKEN_USE_VALUE);
        moq_buf_write_vi64(&w, 7);
        moq_buf_write_raw(&w, payload, sizeof(payload));
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_control_bytes(
            s, setup, moq_buf_writer_offset(&w), 0), (int)MOQ_OK);

        moq_event_t ev;
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(s, &ev, 1), 1);
        MOQ_TEST_CHECK_EQ_INT((int)ev.kind, (int)MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.setup_complete.token_count, 1);
        MOQ_TEST_CHECK_EQ_SIZE(
            ev.u.setup_complete.tokens[0].token_value.len, sizeof(payload));
        MOQ_TEST_CHECK(memcmp(
            ev.u.setup_complete.tokens[0].token_value.data,
            payload, sizeof(payload)) == 0);
        moq_event_cleanup(&ev);
        moq_session_destroy(s);
    }

    /* == Inbound SUBSCRIBE surfaces a resolved (USE_VALUE) token ====== */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 11;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("secret");
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 0, &p);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.token_count, 1);
                MOQ_TEST_CHECK_EQ_U64(
                    ev.u.subscribe_request.tokens[0].token_type, 11);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_request.tokens[0].token_value.len, 6);
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_request.tokens[0].token_value.data,
                    "secret", 6) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Inbound FETCH surfaces a resolved token ====================== */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        f.track_namespace = (moq_namespace_t){ parts, 1 };
        f.track_name = MOQ_BYTES_LITERAL("video");
        f.end = (moq_d18_location_t){ 10, 0 };
        f.params.auth_token_count = 1;
        f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        f.params.auth_tokens[0].token_type = 3;
        f.params.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("fk");
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_fetch(&w, &f);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.fetch_request.token_count, 1);
                MOQ_TEST_CHECK_EQ_U64(ev.u.fetch_request.tokens[0].token_type, 3);
                MOQ_TEST_CHECK(memcmp(ev.u.fetch_request.tokens[0].token_value.data,
                                      "fk", 2) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_session_destroy(s);
    }

    /* == Inbound REQUEST_UPDATE surfaces a token on SUBSCRIBE_UPDATED == */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xA001);
        establish_publisher(s, 0, ref);

        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 8;
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 12;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("upd");
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_d18_encode_request_update(&w, 2, &p), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_updated.token_count, 1);
                MOQ_TEST_CHECK_EQ_U64(
                    ev.u.subscribe_updated.tokens[0].token_type, 12);
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_updated.tokens[0].token_value.data,
                    "upd", 3) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Message-level reject: unknown USE_ALIAS on SUBSCRIBE ========= *
     *  REQUEST_ERROR(0x17) with FIN on the request bidi, no event, session up. */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 99;          /* never registered */
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 0, &p);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool any_event = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) any_event = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any_event);
        MOQ_TEST_CHECK(drain_request_error(s, ref, 0x17, true));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Message-level reject: unknown USE_ALIAS on REQUEST_UPDATE ===== *
     *  REQUEST_ERROR + PUBLISH_DONE(UPDATE_FAILED), no SUBSCRIBE_UPDATED. */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xB001);
        establish_publisher(s, 0, ref);

        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 77;
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool updated = false, err = false, done = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) updated = true;
            moq_event_cleanup(&ev);
        }
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                act.u.send_bidi_stream.stream_ref._v == ref._v) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, act.u.send_bidi_stream.data,
                                    act.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK) {
                    if (env.msg_type == MOQ_D18_REQUEST_ERROR) err = true;
                    if (env.msg_type == MOQ_D18_PUBLISH_DONE) {
                        moq_d18_publish_done_t pd;
                        if (moq_d18_decode_publish_done(env.payload,
                                env.payload_len, &pd) == MOQ_OK &&
                            pd.status_code == 0x8)
                            done = true;
                    }
                }
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(!updated);
        MOQ_TEST_CHECK(err);
        MOQ_TEST_CHECK(done);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Session-fatal: duplicate alias closes with 0x14 ============== */
    {
        moq_session_t *s = make_server_ex(4096);
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 2;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_REGISTER;
        p.auth_tokens[0].alias = 5; p.auth_tokens[0].token_type = 1;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("a");
        p.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_REGISTER;
        p.auth_tokens[1].alias = 5;          /* duplicate */
        p.auth_tokens[1].token_type = 1;
        p.auth_tokens[1].token_value = MOQ_BYTES_LITERAL("b");
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 0, &p);
        (void)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(1),
            msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Session-fatal: REGISTER exceeding the cache closes with 0x13 == *
     *  The default server advertises no cache (limit 0), so any registration
     *  overflows. */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_REGISTER;
        p.auth_tokens[0].alias = 1; p.auth_tokens[0].token_type = 1;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("value");
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 0, &p);
        (void)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(1),
            msg, n, false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        moq_session_destroy(s);
    }

    /* == Session-fatal: malformed Token structure closes with 0x6 ===== */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        uint8_t buf[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        size_t len_off;
        moq_buf_write_vi64(&w, MOQ_D18_SUBSCRIBE);
        moq_buf_reserve_uint16(&w, &len_off);
        size_t start = moq_buf_writer_offset(&w);
        moq_buf_write_vi64(&w, 0);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, 4);
        moq_buf_write_raw(&w, (const uint8_t *)"live", 4);
        moq_buf_write_vi64(&w, 5);
        moq_buf_write_raw(&w, (const uint8_t *)"video", 5);
        moq_buf_write_vi64(&w, 1);
        moq_buf_write_vi64(&w, MOQ_D18_PARAM_AUTHORIZATION_TOKEN);
        moq_buf_write_vi64(&w, 1);          /* span len 1 */
        moq_buf_write_vi64(&w, 42);         /* invalid alias type */
        moq_buf_patch_uint16(&w, len_off,
            (uint16_t)(moq_buf_writer_offset(&w) - start));
        (void)moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(1),
            buf, moq_buf_writer_offset(&w), false, 1);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_CLOSED);
        bool saw_close = false;
        moq_action_t act;
        while (moq_session_poll_actions(s, &act, 1) > 0) {
            if (act.kind == MOQ_ACTION_CLOSE_SESSION) {
                saw_close = true;
                MOQ_TEST_CHECK_EQ_U64(act.u.close_session.code, 0x6);
            }
            moq_action_cleanup(&act);
        }
        MOQ_TEST_CHECK(saw_close);
        moq_session_destroy(s);
    }

    /* == Empty USE_VALUE is left to the token-type verifier ============ */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 7;
        p.auth_tokens[0].token_value = (moq_bytes_t){ NULL, 0 };
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 0, &p);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_request.token_count, 1);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_request.tokens[0].token_value.len, 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Binary USE_VALUE is surfaced byte-for-byte =================== */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 7;
        const uint8_t payload[] = {0xd2, 0x84, 0x40, 0x00, 0x80, 0xff};
        p.auth_tokens[0].token_value =
            (moq_bytes_t){ payload, sizeof(payload) };
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 0, &p);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_request.token_count, 1);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_request.tokens[0].token_value.len,
                    sizeof(payload));
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_request.tokens[0].token_value.data,
                    payload, sizeof(payload)) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == Binary REGISTER remains resolvable through USE_ALIAS ========== */
    {
        const uint8_t payload[] = {0xd2, 0x84, 0x40, 0x00, 0x80, 0xff};
        moq_session_t *s = make_server_ex(1024);
        MOQ_TEST_CHECK(s != NULL);

        /* 1: REGISTER alias 5 with an opaque binary value. */
        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_REGISTER;
        p.auth_tokens[0].alias = 5;
        p.auth_tokens[0].token_type = 7;
        p.auth_tokens[0].token_value =
            (moq_bytes_t){ payload, sizeof(payload) };
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 0, &p);
        moq_stream_ref_t ref1 = moq_stream_ref_from_u64(1);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref1, msg, n, false, 1),
            (int)MOQ_OK);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_request.tokens[0].token_value.len,
                    sizeof(payload));
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_request.tokens[0].token_value.data,
                    payload, sizeof(payload)) == 0);
                moq_reject_subscribe_cfg_t rcfg;
                moq_reject_subscribe_cfg_init(&rcfg);
                moq_session_reject_subscribe(
                    s, ev.u.subscribe_request.sub, &rcfg, 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_action_t action;
        while (moq_session_poll_actions(s, &action, 1) > 0)
            moq_action_cleanup(&action);

        /* 2: USE_ALIAS 5 resolves the same bytes. */
        memset(&p, 0, sizeof(p));
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 5;
        n = encode_subscribe(msg, sizeof(msg), 2, &p);
        moq_stream_ref_t ref2 = moq_stream_ref_from_u64(3);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref2, msg, n, false, 1),
            (int)MOQ_OK);
        got = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_request.tokens[0].token_value.len,
                    sizeof(payload));
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_request.tokens[0].token_value.data,
                    payload, sizeof(payload)) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);

        moq_session_destroy(s);
    }

    /* == ABI: SUBSCRIBE_UPDATED is additive ========================== *
     *  An old caller (element_size truncated before the tokens fields) still
     *  drains the event and reads the pre-token fields; a full-size caller sees
     *  the tokens. */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xC001);
        establish_publisher(s, 0, ref);
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 21;
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 1;
        p.auth_tokens[0].token_value = MOQ_BYTES_LITERAL("x");
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);

        /* Old-caller element size: everything up to (but excluding) tokens. */
        size_t old_size = offsetof(moq_event_t, u) +
            offsetof(moq_subscribe_updated_event_t, tokens);
        uint8_t slot[sizeof(moq_event_t)];
        memset(slot, 0, sizeof(slot));
        size_t cnt = 0;
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_poll_events_ex(s, slot, 1, old_size, &cnt),
            (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_SIZE(cnt, 1);
        moq_event_t *oe = (moq_event_t *)slot;
        MOQ_TEST_CHECK_EQ_INT((int)oe->kind, (int)MOQ_EVENT_SUBSCRIBE_UPDATED);
        MOQ_TEST_CHECK_EQ_U64(oe->u.subscribe_updated.subscriber_priority, 21);
        moq_event_cleanup(oe);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* == A SUBSCRIBE_UPDATED carrying no tokens reports token_count 0 == */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xC101);
        establish_publisher(s, 0, ref);
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 3;
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        moq_session_on_bidi_stream_bytes(s, ref, msg,
            moq_buf_writer_offset(&w), false, 1);
        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_updated.token_count, 0);
                MOQ_TEST_CHECK(ev.u.subscribe_updated.tokens == NULL);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        moq_session_destroy(s);
    }

    /* == End-to-end: subscriber's auth tokens reach the publisher ===== */
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

        moq_auth_token_t tokens[2] = {
            { 4, MOQ_BYTES_LITERAL("one") },
            { 8, MOQ_BYTES_LITERAL("two!!") },
        };
        moq_subscribe_cfg_t sub;
        moq_subscribe_cfg_init(&sub);
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        sub.track_namespace = (moq_namespace_t){ parts, 1 };
        sub.track_name = MOQ_BYTES_LITERAL("video");
        sub.auth_tokens = tokens;
        sub.auth_token_count = 2;
        moq_subscription_t ch;
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_subscribe(client, &sub, 1, &ch),
                              (int)MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        bool got = false;
        moq_event_t ev;
        while (moq_session_poll_events(server, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_request.token_count, 2);
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.tokens[0].token_type, 4);
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_request.tokens[0].token_value.data,
                    "one", 3) == 0);
                MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_request.tokens[1].token_type, 8);
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_request.tokens[1].token_value.data,
                    "two!!", 5) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT((int)client->state, (int)MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK_EQ_INT((int)server->state, (int)MOQ_SESS_ESTABLISHED);
        moq_simpair_destroy(sp);
    }

    /* == Binary alias replacement and USE_ALIAS+DELETE ownership ====== */
    {
        const uint8_t original[] = {0x01, 0x00, 0x02};
        const uint8_t replacement[] = {
            0xd2, 0x84, 0x40, 0x00, 0x80, 0xff
        };
        moq_session_t *s = make_server_ex(1024);
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xE501);

        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_REGISTER;
        p.auth_tokens[0].alias = 7;
        p.auth_tokens[0].token_type = 3;
        p.auth_tokens[0].token_value =
            (moq_bytes_t){ original, sizeof(original) };
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 0, &p);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(
            s, ref, msg, n, false, 1), (int)MOQ_OK);

        moq_event_t ev;
        moq_subscription_t sub = { 0 };
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST)
                sub = ev.u.subscribe_request.sub;
            moq_event_cleanup(&ev);
        }
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_accept_subscribe(s, sub, &acfg, 1),
            (int)MOQ_OK);
        moq_action_t action;
        while (moq_session_poll_actions(s, &action, 1) > 0)
            moq_action_cleanup(&action);

        /* DELETE then REGISTER replaces the alias transactionally. */
        memset(&p, 0, sizeof(p));
        p.auth_token_count = 2;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_DELETE;
        p.auth_tokens[0].alias = 7;
        p.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_REGISTER;
        p.auth_tokens[1].alias = 7;
        p.auth_tokens[1].token_type = 9;
        p.auth_tokens[1].token_value =
            (moq_bytes_t){ replacement, sizeof(replacement) };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(
            s, ref, msg, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        bool got = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_updated.token_count, 1);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_updated.tokens[0].token_value.len,
                    sizeof(replacement));
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_updated.tokens[0].token_value.data,
                    replacement, sizeof(replacement)) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        while (moq_session_poll_actions(s, &action, 1) > 0)
            moq_action_cleanup(&action);

        /* The staged USE_ALIAS bytes survive deleting their cache entry. */
        memset(&p, 0, sizeof(p));
        p.auth_token_count = 2;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 7;
        p.auth_tokens[1].alias_type = MOQ_AUTH_TOKEN_DELETE;
        p.auth_tokens[1].alias = 7;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 4, &p);
        MOQ_TEST_CHECK_EQ_INT((int)moq_session_on_bidi_stream_bytes(
            s, ref, msg, moq_buf_writer_offset(&w), false, 1), (int)MOQ_OK);
        got = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                got = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_updated.token_count, 1);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_updated.tokens[0].token_value.len,
                    sizeof(replacement));
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_updated.tokens[0].token_value.data,
                    replacement, sizeof(replacement)) == 0);
            }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got);
        MOQ_TEST_CHECK_EQ_INT(moq_token_cache_lookup(
            &s->peer_token_cache, 7, NULL, NULL, NULL),
            MOQ_TOKEN_ERR_UNKNOWN);
        moq_session_destroy(s);
    }

    /* == Send-buffer backpressure: rejects are retryable, not dropped ==== *
     *  Leave one undrained action (so begin_advance keeps send_len) and mark the
     *  send buffer all but full. The reject's REQUEST_ERROR fits in send_cap but
     *  not the remaining 2 bytes, so it must surface as WOULD_BLOCK (retryable)
     *  rather than a hard error that frees the request slot and loses the
     *  required response. */

    /* SUBSCRIBE under exhausted send buffer: WOULD_BLOCK, then succeeds. */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        /* Filler: an accepted SUBSCRIBE whose SUBSCRIBE_OK stays queued. */
        moq_d18_msg_params_t mp = { 0 };
        uint8_t fmsg[128];
        size_t fn = encode_subscribe(fmsg, sizeof(fmsg), 0, &mp);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0xF001),
                                         fmsg, fn, false, 1);
        moq_subscription_t fh = { 0 };
        moq_event_t fev;
        while (moq_session_poll_events(s, &fev, 1) > 0) {
            if (fev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) fh = fev.u.subscribe_request.sub;
            moq_event_cleanup(&fev);
        }
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(s, fh, &acfg, 1);   /* leaves 1 pending act */
        s->send_len = s->send_cap - 2;                    /* nearly full */

        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 55;
        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 2, &p);   /* next req id */
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF002);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* The request slot is retained (not dropped). */
        moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_SUBSCRIPTION);
        bool any_event = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) any_event = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any_event);

        /* Drain the pending action (frees the buffer), then an empty re-feed
         * retries the buffered request: the REQUEST_ERROR now fits. */
        moq_action_t da;
        while (moq_session_poll_actions(s, &da, 1) > 0) moq_action_cleanup(&da);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(drain_request_error(s, ref, 0x17, true));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* FETCH under exhausted send buffer: WOULD_BLOCK, request retained. */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_d18_msg_params_t mp = { 0 };
        uint8_t fmsg[128];
        size_t fn = encode_subscribe(fmsg, sizeof(fmsg), 0, &mp);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0xF101),
                                         fmsg, fn, false, 1);
        moq_subscription_t fh = { 0 };
        moq_event_t fev;
        while (moq_session_poll_events(s, &fev, 1) > 0) {
            if (fev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) fh = fev.u.subscribe_request.sub;
            moq_event_cleanup(&fev);
        }
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(s, fh, &acfg, 1);
        s->send_len = s->send_cap - 2;

        moq_d18_fetch_t f;
        memset(&f, 0, sizeof(f));
        f.request_id = 2;
        f.fetch_type = MOQ_D18_FETCH_TYPE_STANDALONE;
        moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
        f.track_namespace = (moq_namespace_t){ parts, 1 };
        f.track_name = MOQ_BYTES_LITERAL("video");
        f.end = (moq_d18_location_t){ 10, 0 };
        f.params.auth_token_count = 1;
        f.params.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        f.params.auth_tokens[0].alias = 55;
        uint8_t msg[128];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_fetch(&w, &f);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF102);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool any_event = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) any_event = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!any_event);
        moq_action_t da;
        while (moq_session_poll_actions(s, &da, 1) > 0) moq_action_cleanup(&da);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(drain_request_error(s, ref, 0x17, true));
        moq_session_destroy(s);
    }

    /* REQUEST_UPDATE under exhausted send buffer: WOULD_BLOCK, sub retained. */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF201);
        /* Establish a publisher subscription but leave the SUBSCRIBE_OK queued so
         * an action stays pending (keeps send_len across the next advance). */
        {
            uint8_t msg[128];
            moq_d18_msg_params_t mp = { 0 };
            size_t n = encode_subscribe(msg, sizeof(msg), 0, &mp);
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
        }
        s->send_len = s->send_cap - 2;

        moq_d18_msg_params_t p = { 0 };
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_ALIAS;
        p.auth_tokens[0].alias = 55;
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        bool updated = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) updated = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!updated);
        /* Drain, retry: REQUEST_ERROR + PUBLISH_DONE now fit. */
        moq_action_t da;
        while (moq_session_poll_actions(s, &da, 1) > 0) moq_action_cleanup(&da);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        bool err = false, done = false;
        while (moq_session_poll_actions(s, &da, 1) > 0) {
            if (da.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                da.u.send_bidi_stream.stream_ref._v == ref._v) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, da.u.send_bidi_stream.data,
                                    da.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK) {
                    if (env.msg_type == MOQ_D18_REQUEST_ERROR) err = true;
                    if (env.msg_type == MOQ_D18_PUBLISH_DONE) done = true;
                }
            }
            moq_action_cleanup(&da);
        }
        MOQ_TEST_CHECK(err);
        MOQ_TEST_CHECK(done);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* Duplicate SUBSCRIBE reject under exhausted send buffer: WOULD_BLOCK,
     * retained, then the REQUEST_ERROR(0x19) is delivered on retry. */
    {
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        /* Filler: accept a "video" subscription (also makes the next one a
         * duplicate) and leave its SUBSCRIBE_OK queued. */
        moq_d18_msg_params_t mp = { 0 };
        uint8_t fmsg[128];
        size_t fn = encode_subscribe(fmsg, sizeof(fmsg), 0, &mp);
        moq_session_on_bidi_stream_bytes(s, moq_stream_ref_from_u64(0xF301),
                                         fmsg, fn, false, 1);
        moq_subscription_t fh = { 0 };
        moq_event_t fev;
        while (moq_session_poll_events(s, &fev, 1) > 0) {
            if (fev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) fh = fev.u.subscribe_request.sub;
            moq_event_cleanup(&fev);
        }
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(s, fh, &acfg, 1);
        s->send_len = s->send_cap - 2;

        uint8_t msg[128];
        size_t n = encode_subscribe(msg, sizeof(msg), 2, &mp);   /* same track */
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF302);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1),
            (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
        MOQ_TEST_CHECK_EQ_INT((int)ep.kind, (int)MOQ_REQ_SUBSCRIPTION);
        moq_action_t da;
        while (moq_session_poll_actions(s, &da, 1) > 0) moq_action_cleanup(&da);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        MOQ_TEST_CHECK(drain_request_error(s, ref, 0x19, true));
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    /* REQUEST_OK (successful update) under exhausted send buffer: WOULD_BLOCK,
     * no event, then on retry the REQUEST_OK + SUBSCRIBE_UPDATED are produced. */
    {
        const uint8_t payload[] = {0xd2, 0x84, 0x40, 0x00, 0x80, 0xff};
        moq_session_t *s = make_server();
        MOQ_TEST_CHECK(s != NULL);
        moq_stream_ref_t ref = moq_stream_ref_from_u64(0xF401);
        {
            uint8_t msg[128];
            moq_d18_msg_params_t mp = { 0 };
            size_t n = encode_subscribe(msg, sizeof(msg), 0, &mp);
            moq_session_on_bidi_stream_bytes(s, ref, msg, n, false, 1);
            moq_subscription_t h = { 0 };
            moq_event_t ev;
            while (moq_session_poll_events(s, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) h = ev.u.subscribe_request.sub;
                moq_event_cleanup(&ev);
            }
            moq_accept_subscribe_cfg_t acfg;
            moq_accept_subscribe_cfg_init(&acfg);
            moq_session_accept_subscribe(s, h, &acfg, 1);   /* leaves pending act */
        }
        s->send_len = s->send_cap - 2;

        /* Carry a USE_VALUE auth token so the success path stages token data into
         * output scratch before queuing REQUEST_OK; a retryable WOULD_BLOCK must
         * roll that staging back (no scratch leak) and surface no event. */
        moq_d18_msg_params_t p = { 0 };
        p.has_subscriber_priority = true; p.subscriber_priority = 4;
        p.auth_token_count = 1;
        p.auth_tokens[0].alias_type = MOQ_AUTH_TOKEN_USE_VALUE;
        p.auth_tokens[0].token_type = 13;
        p.auth_tokens[0].token_value =
            (moq_bytes_t){ payload, sizeof(payload) };
        uint8_t msg[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, msg, sizeof(msg));
        moq_d18_encode_request_update(&w, 2, &p);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, msg,
                moq_buf_writer_offset(&w), false, 1), (int)MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        /* Scratch was rolled back (begin_advance zeroes it at call entry, so a
         * leak would leave it non-zero here). */
        MOQ_TEST_CHECK_EQ_SIZE(s->output_scratch_len, 0);
        bool updated = false;
        moq_event_t ev;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) updated = true;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(!updated);

        moq_action_t da;
        while (moq_session_poll_actions(s, &da, 1) > 0) moq_action_cleanup(&da);
        MOQ_TEST_CHECK_EQ_INT(
            (int)moq_session_on_bidi_stream_bytes(s, ref, NULL, 0, false, 1),
            (int)MOQ_OK);
        bool got_ok = false, got_updated = false;
        while (moq_session_poll_events(s, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                got_updated = true;
                MOQ_TEST_CHECK_EQ_SIZE(ev.u.subscribe_updated.token_count, 1);
                MOQ_TEST_CHECK_EQ_U64(
                    ev.u.subscribe_updated.tokens[0].token_type, 13);
                MOQ_TEST_CHECK_EQ_SIZE(
                    ev.u.subscribe_updated.tokens[0].token_value.len,
                    sizeof(payload));
                MOQ_TEST_CHECK(memcmp(
                    ev.u.subscribe_updated.tokens[0].token_value.data,
                    payload, sizeof(payload)) == 0);
            }
            moq_event_cleanup(&ev);
        }
        while (moq_session_poll_actions(s, &da, 1) > 0) {
            if (da.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
                da.u.send_bidi_stream.stream_ref._v == ref._v) {
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, da.u.send_bidi_stream.data,
                                    da.u.send_bidi_stream.len);
                moq_control_envelope_t env;
                if (moq_d18_decode_envelope(&r, &env) == MOQ_OK &&
                    env.msg_type == MOQ_D18_REQUEST_OK)
                    got_ok = true;
            }
            moq_action_cleanup(&da);
        }
        MOQ_TEST_CHECK(got_ok);
        MOQ_TEST_CHECK(got_updated);
        MOQ_TEST_CHECK_EQ_INT((int)s->state, (int)MOQ_SESS_ESTABLISHED);
        moq_session_destroy(s);
    }

    MOQ_TEST_PASS("d18_auth");
    return failures != 0;
}
