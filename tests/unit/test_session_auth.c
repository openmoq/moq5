#include "test_session_support.h"
#include "../../core/src/session/session_internal.h"
#include <string.h>

/* -- Helper: build CLIENT_SETUP with AUTH_TOKEN params --------------- */

static size_t build_auth_token_value(uint8_t *out, size_t cap,
                                      const moq_d16_auth_token_t *tok)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, cap);
    moq_d16_auth_token_encode(&w, tok);
    return moq_buf_writer_offset(&w);
}

static moq_result_t feed_client_setup_with_tokens(
    moq_session_t *sv,
    const moq_d16_auth_token_t *tokens, size_t token_count,
    const moq_kvp_entry_t *extra_params, size_t extra_count)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));

    moq_kvp_entry_t params[16];
    size_t param_count = 0;

    /* Copy extra params first (e.g. MAX_AUTH_TOKEN_CACHE_SIZE). */
    for (size_t i = 0; i < extra_count && param_count < 16; i++)
        params[param_count++] = extra_params[i];

    /* Encode each auth token into value buffers and add as params. */
    uint8_t tok_bufs[16][64];
    for (size_t i = 0; i < token_count && param_count < 16; i++) {
        size_t vlen = build_auth_token_value(tok_bufs[i], sizeof(tok_bufs[i]),
                                              &tokens[i]);
        params[param_count].type      = MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN;
        params[param_count].value     = tok_bufs[i];
        params[param_count].value_len = vlen;
        params[param_count].is_varint = false;
        params[param_count].raw       = NULL;
        params[param_count].raw_len   = 0;
        param_count++;
    }

    moq_d16_encode_client_setup(&w, params, param_count);
    return moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
}

static moq_result_t feed_server_setup_with_tokens(
    moq_session_t *client,
    const moq_d16_auth_token_t *tokens, size_t token_count,
    const moq_kvp_entry_t *extra_params, size_t extra_count)
{
    uint8_t buf[512];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));

    moq_kvp_entry_t params[16];
    size_t param_count = 0;

    for (size_t i = 0; i < extra_count && param_count < 16; i++)
        params[param_count++] = extra_params[i];

    uint8_t tok_bufs[16][64];
    for (size_t i = 0; i < token_count && param_count < 16; i++) {
        size_t vlen = build_auth_token_value(tok_bufs[i], sizeof(tok_bufs[i]),
                                              &tokens[i]);
        params[param_count].type      = MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN;
        params[param_count].value     = tok_bufs[i];
        params[param_count].value_len = vlen;
        params[param_count].is_varint = false;
        params[param_count].raw       = NULL;
        params[param_count].raw_len   = 0;
        param_count++;
    }

    moq_d16_encode_server_setup(&w, params, param_count);
    return moq_session_on_control_bytes(client, buf, moq_buf_writer_offset(&w), 0);
}

int main(void)
{
    int failures = 0;

    /* == 1. Default max=0: REGISTER downgrades, token in event, cache empty */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        /* auth_token_cache_size defaults to 0 (no cache budget) */

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 42,
            .token_value = (const uint8_t *)"secret",
            .token_value_len = 6,
        };
        MOQ_TEST_CHECK(feed_client_setup_with_tokens(sv, &tok, 1, NULL, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Token appears in SETUP_COMPLETE event. */
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.token_count == 1);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.tokens != NULL);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.tokens[0].token_type == 42);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.tokens[0].token_value.len == 6);
        MOQ_TEST_CHECK(memcmp(evts[0].u.setup_complete.tokens[0].token_value.data,
                              "secret", 6) == 0);

        /* Cache is empty: REGISTER was downgraded to USE_VALUE. */
        uint64_t type;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 1,
            &type, NULL, NULL) == MOQ_TOKEN_ERR_UNKNOWN);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 2. Configured max: REGISTER succeeds, token in event =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_auth_token_cache_size = true;
        scfg.auth_token_cache_size = 1024;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 5,
            .token_type = 99,
            .token_value = (const uint8_t *)"token123",
            .token_value_len = 8,
        };
        MOQ_TEST_CHECK(feed_client_setup_with_tokens(sv, &tok, 1, NULL, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Token appears in event. */
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.token_count == 1);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.tokens[0].token_type == 99);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.tokens[0].token_value.len == 8);

        /* Alias is stored in cache. */
        uint64_t type;
        const uint8_t *val;
        size_t val_len;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 5,
            &type, &val, &val_len) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(type == 99);
        MOQ_TEST_CHECK(val_len == 8);
        MOQ_TEST_CHECK(memcmp(val, "token123", 8) == 0);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 3. CLIENT_SETUP DELETE -> PROTOCOL_VIOLATION =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_DELETE,
            .alias = 1,
        };
        feed_client_setup_with_tokens(sv, &tok, 1, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x3);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 4. CLIENT_SETUP USE_ALIAS -> PROTOCOL_VIOLATION ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 1,
        };
        feed_client_setup_with_tokens(sv, &tok, 1, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x3);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 5. Malformed token -> KEY_VALUE_FORMATTING_ERROR (0x6) ========= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        /* Feed a CLIENT_SETUP with a malformed AUTH_TOKEN value. */
        uint8_t buf[64];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        /* AUTH_TOKEN param with garbage value: just one byte 0xFF,
         * which should fail to decode as an auth token structure. */
        moq_kvp_entry_t params[1];
        params[0].type      = MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN;
        params[0].value     = (const uint8_t *)"\xFF";
        params[0].value_len = 1;
        params[0].is_varint = false;
        params[0].raw       = NULL;
        params[0].raw_len   = 0;
        moq_d16_encode_client_setup(&w, params, 1);

        moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x6);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 6. Duplicate REGISTER alias -> DUPLICATE_AUTH_TOKEN_ALIAS (0x14) */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_auth_token_cache_size = true;
        scfg.auth_token_cache_size = 1024;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tokens[2] = {
            {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER,
                .alias = 7,
                .token_type = 1,
                .token_value = (const uint8_t *)"a",
                .token_value_len = 1,
            },
            {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER,
                .alias = 7,  /* duplicate alias */
                .token_type = 2,
                .token_value = (const uint8_t *)"b",
                .token_value_len = 1,
            },
        };

        feed_client_setup_with_tokens(sv, tokens, 2, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x14);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 7. Accessor: peer_auth_token_cache_size ======================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        /* Before setup: defaults to 0. */
        MOQ_TEST_CHECK(moq_session_peer_auth_token_cache_size(sv) == 0);
        MOQ_TEST_CHECK(moq_session_peer_auth_token_cache_size(NULL) == 0);

        /* Feed a CLIENT_SETUP with MAX_AUTH_TOKEN_CACHE_SIZE=256. */
        uint8_t vbuf[8];
        size_t vlen = moq_quic_varint_encode(256, vbuf, sizeof(vbuf));
        moq_kvp_entry_t extra[1] = {{
            .type = MOQ_SETUP_PARAM_MAX_AUTH_TOKEN_CACHE_SIZE,
            .value = vbuf,
            .value_len = vlen,
            .is_varint = true,
            .raw = NULL,
            .raw_len = 0,
        }};
        feed_client_setup_with_tokens(sv, NULL, 0, extra, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_peer_auth_token_cache_size(sv) == 256);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 8. OOM on token value copy during setup ======================== */
    {
        /* Use fail-at-N allocator. The session itself takes N allocs,
         * then the cache init takes 1. We need to find the right N
         * to fail at the scratch copy (which doesn't alloc; it's in
         * the inline scratch). Instead, test that the token cache init
         * failure during session creation propagates correctly. */
        fail_alloc_state_t fas = { .fail_at = 2 };
        moq_alloc_t alloc = fail_allocator(&fas);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_auth_token_cache_size = true;
        scfg.auth_token_cache_size = 1024;

        moq_session_t *sv = NULL;
        moq_result_t rc = moq_session_create(&scfg, 0, &sv);
        /* First alloc succeeds (session blob), second fails (cache entries). */
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(sv == NULL);
        MOQ_TEST_CHECK(fas.balance == 0);
    }

    /* == 8b. SERVER_SETUP must not be queued before retryable allocs ===== */
    {
        /* If a retryable allocation needed to complete setup (here, the
         * token-cache preowned buffer) fails AFTER SERVER_SETUP has been
         * queued, the handler returns retryably with observable output
         * already escaped -- a retry then double-queues SERVER_SETUP.
         * SERVER_SETUP must be queued only once every retryable allocation
         * has succeeded. */
        fail_alloc_state_t fas = {0};
        moq_alloc_t alloc = fail_allocator(&fas);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_auth_token_cache_size = true;
        scfg.auth_token_cache_size = 1024;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 7,
            .token_value = (const uint8_t *)"regval",
            .token_value_len = 6,
        };

        /* Force the first heap allocation during CLIENT_SETUP handling to
         * fail. Token values are copied into inline output scratch (no heap),
         * so the first s->alloc.alloc is the cache preowned buffer -- the
         * allocation that previously ran after SERVER_SETUP was queued. */
        fas.fail_at = fas.call_count + 1;
        moq_result_t rc = feed_client_setup_with_tokens(sv, &tok, 1, NULL, 0);

        /* Retryable failure with NOTHING observable escaped. */
        MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(sv->send_len == 0);   /* no SERVER_SETUP bytes queued */

        moq_action_t acts[4];
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_actions(sv, acts, 4), 0);
        moq_event_t evts[4];
        MOQ_TEST_CHECK_EQ_SIZE(moq_session_poll_events(sv, evts, 4), 0);

        /* Retry with the allocator healed: exactly one SERVER_SETUP and one
         * SETUP_COMPLETE -- no stale duplicate from the first attempt. */
        fas.fail_at = 0;
        rc = moq_session_process_pending(sv, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        size_t na = moq_session_poll_actions(sv, acts, 4);
        MOQ_TEST_CHECK_EQ_SIZE(na, 1);
        MOQ_TEST_CHECK_EQ_INT((int)acts[0].kind, (int)MOQ_ACTION_SEND_CONTROL);
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK_EQ_SIZE(ne, 1);
        MOQ_TEST_CHECK_EQ_INT((int)evts[0].kind, (int)MOQ_EVENT_SETUP_COMPLETE);

        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(fas.balance == 0);
    }

    /* == 9. Advertised cache size appears in our setup =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_auth_token_cache_size = true;
        ccfg.auth_token_cache_size = 512;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.send_auth_token_cache_size = true;
        scfg.auth_token_cache_size = 256;

        moq_session_t *c = NULL, *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        pump_actions_to_peer(c, sv, 0);
        pump_actions_to_peer(sv, c, 0);

        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Server sees client's advertised cache size=512. */
        MOQ_TEST_CHECK(moq_session_peer_auth_token_cache_size(sv) == 512);
        /* Client sees server's advertised cache size=256. */
        MOQ_TEST_CHECK(moq_session_peer_auth_token_cache_size(c) == 256);

        /* Drain events. */
        moq_event_t evts[4];
        moq_session_poll_events(c, evts, 4);
        moq_session_poll_events(sv, evts, 4);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 10. USE_VALUE token (no alias) in CLIENT_SETUP ================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 7,
            .token_value = (const uint8_t *)"directval",
            .token_value_len = 9,
        };
        MOQ_TEST_CHECK(feed_client_setup_with_tokens(sv, &tok, 1, NULL, 0) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.token_count == 1);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.tokens[0].token_type == 7);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.tokens[0].token_value.len == 9);
        MOQ_TEST_CHECK(memcmp(evts[0].u.setup_complete.tokens[0].token_value.data,
                              "directval", 9) == 0);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 11. No tokens -> tokens=NULL, count=0 =========================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *c = NULL, *sv = NULL;
        moq_session_create(&ccfg, 0, &c);
        moq_session_create(&scfg, 0, &sv);
        moq_session_start(c, 0);
        pump_actions_to_peer(c, sv, 0);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.tokens == NULL);
        MOQ_TEST_CHECK(evts[0].u.setup_complete.token_count == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 12. SERVER_SETUP REGISTER overflow -> 0x13 ====================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        /* Client with auth_token_cache_size = 0 (default). */
        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;

        moq_session_t *c = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);

        /* Drain CLIENT_SETUP action. */
        moq_action_t drain[4];
        moq_session_poll_actions(c, drain, 4);

        /* Feed SERVER_SETUP with a REGISTER token. Client has no cache
         * budget, so this should close with AUTH_TOKEN_CACHE_OVERFLOW. */
        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 5,
            .token_value = (const uint8_t *)"x",
            .token_value_len = 1,
        };
        feed_server_setup_with_tokens(c, &tok, 1, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(c, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x13);

        moq_session_destroy(c);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================= */
    /* Phase 4: Request-level AUTH_TOKEN in SUBSCRIBE / PUBLISH_NAMESPACE */
    /* ================================================================= */

    /* == 13. SUBSCRIBE with USE_VALUE token ============================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 42,
            .token_value = (const uint8_t *)"hello",
            .token_value_len = 5,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &tok);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t", params, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens != NULL);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_type == 42);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_value.len == 5);
        MOQ_TEST_CHECK(memcmp(ev.u.subscribe_request.tokens[0].token_value.data,
                              "hello", 5) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 14. PUBLISH_NAMESPACE with USE_VALUE token ===================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 7,
            .token_value = (const uint8_t *)"abc",
            .token_value_len = 3,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &tok);

        /* Encode PUBLISH_NAMESPACE with AUTH_TOKEN param. */
        uint8_t buf[512];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns") };
        moq_namespace_t ns = { ns_parts, 1 };
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        moq_d16_encode_publish_namespace(&w, 0, &ns, params, 1);
        moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev.u.namespace_published.token_count == 1);
        MOQ_TEST_CHECK(ev.u.namespace_published.tokens != NULL);
        MOQ_TEST_CHECK(ev.u.namespace_published.tokens[0].token_type == 7);
        MOQ_TEST_CHECK(ev.u.namespace_published.tokens[0].token_value.len == 3);
        MOQ_TEST_CHECK(memcmp(ev.u.namespace_published.tokens[0].token_value.data,
                              "abc", 3) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 15. REGISTER then USE_ALIAS resolves correctly ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* First SUBSCRIBE: REGISTER alias=1 with token. */
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 99,
            .token_value = (const uint8_t *)"secret",
            .token_value_len = 6,
        };
        uint8_t tb1[64];
        moq_buf_writer_t tw1;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_d16_auth_token_encode(&tw1, &reg_tok);
        moq_kvp_entry_t p1[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t1", p1, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_type == 99);

        /* Accept the first subscribe so the slot is free. */
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acfg, 0);
        moq_action_t drain[8];
        moq_session_poll_actions(sv, drain, 8);

        /* Second SUBSCRIBE: USE_ALIAS alias=1. */
        moq_d16_auth_token_t use_tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 1,
        };
        uint8_t tb2[64];
        moq_buf_writer_t tw2;
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw2, &use_tok);
        moq_kvp_entry_t p2[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 2, "ns", "t2", p2, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev2;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev2, 1) == 1);
        MOQ_TEST_CHECK(ev2.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev2.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev2.u.subscribe_request.tokens[0].token_type == 99);
        MOQ_TEST_CHECK(ev2.u.subscribe_request.tokens[0].token_value.len == 6);
        MOQ_TEST_CHECK(memcmp(ev2.u.subscribe_request.tokens[0].token_value.data,
                              "secret", 6) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 16. Unknown alias -> REQUEST_ERROR 0x17, session stays open === */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        moq_d16_auth_token_t use_tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 999,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &use_tok);
        moq_kvp_entry_t p[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t", p, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* No SUBSCRIBE_REQUEST event (rejected). */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        /* Verify REQUEST_ERROR 0x17 was sent. */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        MOQ_TEST_CHECK(na >= 1);
        bool found_err = false;
        for (size_t i = 0; i < na; i++) {
            moq_d16_request_error_t err;
            if (decode_action_request_error(&acts[i], &err) == MOQ_OK) {
                MOQ_TEST_CHECK(err.error_code == 0x17);
                found_err = true;
            }
        }
        MOQ_TEST_CHECK(found_err);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 17. Duplicate resolved tokens -> REQUEST_ERROR 0x04 =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* Two USE_VALUE tokens with identical (type, value). */
        moq_d16_auth_token_t tok1 = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 5,
            .token_value = (const uint8_t *)"dup",
            .token_value_len = 3,
        };
        uint8_t tb1[64], tb2[64];
        moq_buf_writer_t tw1, tw2;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw1, &tok1);
        moq_d16_auth_token_encode(&tw2, &tok1);
        moq_kvp_entry_t p[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
        };
        feed_subscribe(sv, 0, "ns", "t", p, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* No event, request rejected. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        /* REQUEST_ERROR 0x04. */
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        bool found_err = false;
        for (size_t i = 0; i < na; i++) {
            moq_d16_request_error_t err;
            if (decode_action_request_error(&acts[i], &err) == MOQ_OK) {
                MOQ_TEST_CHECK(err.error_code == 0x4);
                found_err = true;
            }
        }
        MOQ_TEST_CHECK(found_err);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 18. Duplicate alias REGISTER -> session close 0x14 ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* First SUBSCRIBE registers alias=1. */
        moq_d16_auth_token_t reg1 = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 10,
            .token_value = (const uint8_t *)"x",
            .token_value_len = 1,
        };
        uint8_t tb1[64];
        moq_buf_writer_t tw1;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_d16_auth_token_encode(&tw1, &reg1);
        moq_kvp_entry_t p1[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t1", p1, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acfg, 0);
        moq_action_t drain[8];
        moq_session_poll_actions(sv, drain, 8);

        /* Second SUBSCRIBE tries to REGISTER same alias=1. */
        moq_d16_auth_token_t reg2 = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,  /* duplicate alias! */
            .token_type = 20,
            .token_value = (const uint8_t *)"y",
            .token_value_len = 1,
        };
        uint8_t tb2[64];
        moq_buf_writer_t tw2;
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw2, &reg2);
        moq_kvp_entry_t p2[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 2, "ns", "t2", p2, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x14);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 19. Overflow REGISTER -> session close 0x13 =================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        /* Tiny cache: 1 entry max (overhead=16 bytes, max_bytes=20). */
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 20;  /* enough for exactly 1 small entry */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* First SUBSCRIBE: REGISTER alias=1 (fits). */
        moq_d16_auth_token_t reg1 = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 1,
            .token_value = (const uint8_t *)"a",
            .token_value_len = 1,
        };
        uint8_t tb1[64];
        moq_buf_writer_t tw1;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_d16_auth_token_encode(&tw1, &reg1);
        moq_kvp_entry_t p1[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t1", p1, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acfg, 0);
        moq_action_t drain[8];
        moq_session_poll_actions(sv, drain, 8);

        /* Second SUBSCRIBE: REGISTER alias=2 (should overflow). */
        moq_d16_auth_token_t reg2 = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 2,
            .token_type = 2,
            .token_value = (const uint8_t *)"bb",
            .token_value_len = 2,
        };
        uint8_t tb2[64];
        moq_buf_writer_t tw2;
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw2, &reg2);
        moq_kvp_entry_t p2[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 2, "ns", "t2", p2, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x13);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 20. Malformed token in SUBSCRIBE -> session close 0x06 ======== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* Garbage AUTH_TOKEN value. */
        moq_kvp_entry_t p[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = (const uint8_t *)"\xFF", .value_len = 1,
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t", p, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x6);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 21. REGISTER persists after request reject ==================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* SUBSCRIBE with REGISTER alias=5 + USE_ALIAS for unknown alias=99. */
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 5,
            .token_type = 77,
            .token_value = (const uint8_t *)"val5",
            .token_value_len = 4,
        };
        moq_d16_auth_token_t use_tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 99,  /* not registered -> reject */
        };
        uint8_t tb1[64], tb2[64];
        moq_buf_writer_t tw1, tw2;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw1, &reg_tok);
        moq_d16_auth_token_encode(&tw2, &use_tok);
        moq_kvp_entry_t p[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
        };
        feed_subscribe(sv, 0, "ns", "t1", p, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Rejected with 0x17. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        bool found_err = false;
        for (size_t i = 0; i < na; i++) {
            moq_d16_request_error_t err;
            if (decode_action_request_error(&acts[i], &err) == MOQ_OK) {
                MOQ_TEST_CHECK(err.error_code == 0x17);
                found_err = true;
            }
        }
        MOQ_TEST_CHECK(found_err);

        /* Now feed a second SUBSCRIBE using alias=5 — should resolve. */
        moq_d16_auth_token_t use5 = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 5,
        };
        uint8_t tb3[64];
        moq_buf_writer_t tw3;
        moq_buf_writer_init(&tw3, tb3, sizeof(tb3));
        moq_d16_auth_token_encode(&tw3, &use5);
        moq_kvp_entry_t p3[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb3, .value_len = moq_buf_writer_offset(&tw3),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 2, "ns", "t2", p3, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev2;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev2, 1) == 1);
        MOQ_TEST_CHECK(ev2.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev2.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev2.u.subscribe_request.tokens[0].token_type == 77);
        MOQ_TEST_CHECK(ev2.u.subscribe_request.tokens[0].token_value.len == 4);
        MOQ_TEST_CHECK(memcmp(ev2.u.subscribe_request.tokens[0].token_value.data,
                              "val5", 4) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 22. Pool-full REGISTER persists =============================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        sx.max_subscriptions = 1;  /* pool can hold only 1 sub */
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* First SUBSCRIBE fills the pool. */
        feed_subscribe(sv, 0, "ns", "t1", NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev1;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev1, 1) == 1);
        MOQ_TEST_CHECK(ev1.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_subscription_t sub1 = ev1.u.subscribe_request.sub;

        /* Second SUBSCRIBE with REGISTER alias=3. Pool full -> REQUEST_ERROR. */
        moq_d16_auth_token_t reg3 = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 3,
            .token_type = 33,
            .token_value = (const uint8_t *)"val3",
            .token_value_len = 4,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &reg3);
        moq_kvp_entry_t p2[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 2, "ns", "t2", p2, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Pool full -> REQUEST_ERROR. */
        moq_event_t ev2;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev2, 1) == 0);
        moq_action_t acts[8];
        moq_session_poll_actions(sv, acts, 8);

        /* Free the first sub. */
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, sub1, &acfg, 0);
        moq_session_poll_actions(sv, acts, 8);

        /* Reject first sub to free slot. */
        moq_reject_subscribe_cfg_t rcfg;
        moq_reject_subscribe_cfg_init(&rcfg);
        rcfg.error_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        /* Verify alias=3 was registered in the cache despite pool-full reject. */
        uint64_t cached_type;
        const uint8_t *cached_val;
        size_t cached_len;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 3,
            &cached_type, &cached_val, &cached_len) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(cached_type == 33);
        MOQ_TEST_CHECK(cached_len == 4);
        MOQ_TEST_CHECK(memcmp(cached_val, "val3", 4) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* ================================================================= */
    /* Regression tests for AUTH_TOKEN control flow and lifetime bugs    */
    /* ================================================================= */

    /* == 23. USE_ALIAS(a) then DELETE(a) — resolved value must survive */
    /*     This is a UAF under ASan: USE_ALIAS borrows a pointer from   */
    /*     the cache, then DELETE frees that cache entry. The caller     */
    /*     later reads the freed pointer during scratch_copy.            */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* First SUBSCRIBE: REGISTER alias=1 with known value. */
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 42,
            .token_value = (const uint8_t *)"secret_value",
            .token_value_len = 12,
        };
        uint8_t tb0[64];
        moq_buf_writer_t tw0;
        moq_buf_writer_init(&tw0, tb0, sizeof(tb0));
        moq_d16_auth_token_encode(&tw0, &reg);
        moq_kvp_entry_t p0[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb0, .value_len = moq_buf_writer_offset(&tw0),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t0", p0, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_event_t ev0;
        moq_session_poll_events(sv, &ev0, 1);
        moq_accept_subscribe_cfg_t acfg0;
        moq_accept_subscribe_cfg_init(&acfg0);
        moq_session_accept_subscribe(sv, ev0.u.subscribe_request.sub, &acfg0, 0);
        moq_action_t drain[8];
        moq_session_poll_actions(sv, drain, 8);

        /* Second SUBSCRIBE: USE_ALIAS(1), then DELETE(1) in same message. */
        moq_d16_auth_token_t use = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 1,
        };
        moq_d16_auth_token_t del = {
            .alias_type = MOQ_AUTH_TOKEN_DELETE,
            .alias = 1,
        };
        uint8_t tb1[64], tb2[64];
        moq_buf_writer_t tw1, tw2;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw1, &use);
        moq_d16_auth_token_encode(&tw2, &del);
        moq_kvp_entry_t p1[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
        };
        feed_subscribe(sv, 2, "ns", "t1", p1, 2);
        /* Must not crash (UAF). Session should still be open. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Event must contain the original token value, not garbage. */
        moq_event_t ev1;
        size_t ne = moq_session_poll_events(sv, &ev1, 1);
        MOQ_TEST_CHECK(ne == 1);
        MOQ_TEST_CHECK(ev1.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev1.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev1.u.subscribe_request.tokens[0].token_type == 42);
        MOQ_TEST_CHECK(ev1.u.subscribe_request.tokens[0].token_value.len == 12);
        MOQ_TEST_CHECK(memcmp(ev1.u.subscribe_request.tokens[0].token_value.data,
                              "secret_value", 12) == 0);

        /* Cache entry should be deleted. */
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 1,
            NULL, NULL, NULL) == MOQ_TOKEN_ERR_UNKNOWN);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 24. Reject first, then REGISTER — alias must persist ========= */
    /*     [USE_ALIAS(unknown=99), REGISTER(alias=5)]                    */
    /*     Request is rejected (0x17), but REGISTER must still commit.   */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        moq_d16_auth_token_t use_bad = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 99,  /* unknown -> reject */
        };
        moq_d16_auth_token_t reg5 = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 5,
            .token_type = 77,
            .token_value = (const uint8_t *)"val5",
            .token_value_len = 4,
        };
        uint8_t tb1[64], tb2[64];
        moq_buf_writer_t tw1, tw2;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw1, &use_bad);
        moq_d16_auth_token_encode(&tw2, &reg5);
        moq_kvp_entry_t p[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
        };
        feed_subscribe(sv, 0, "ns", "t1", p, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* Request rejected with 0x17. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);
        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        bool found_err = false;
        for (size_t i = 0; i < na; i++) {
            moq_d16_request_error_t err;
            if (decode_action_request_error(&acts[i], &err) == MOQ_OK) {
                MOQ_TEST_CHECK(err.error_code == 0x17);
                found_err = true;
            }
        }
        MOQ_TEST_CHECK(found_err);

        /* REGISTER(5) must have committed despite the reject. */
        uint64_t cached_type;
        const uint8_t *cached_val;
        size_t cached_len;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 5,
            &cached_type, &cached_val, &cached_len) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(cached_type == 77);
        MOQ_TEST_CHECK(cached_len == 4);
        MOQ_TEST_CHECK(memcmp(cached_val, "val5", 4) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 25. Reject first, then duplicate REGISTER — session must close */
    /*     [USE_ALIAS(unknown), REGISTER(existing_alias)]                */
    /*     Session-fatal 0x14 must not be masked by the earlier reject.  */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* Pre-register alias=1. */
        moq_d16_auth_token_t pre_reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 10,
            .token_value = (const uint8_t *)"x",
            .token_value_len = 1,
        };
        uint8_t tb0[64];
        moq_buf_writer_t tw0;
        moq_buf_writer_init(&tw0, tb0, sizeof(tb0));
        moq_d16_auth_token_encode(&tw0, &pre_reg);
        moq_kvp_entry_t p0[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb0, .value_len = moq_buf_writer_offset(&tw0),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t0", p0, 1);
        moq_event_t ev0;
        moq_session_poll_events(sv, &ev0, 1);
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ev0.u.subscribe_request.sub, &acfg, 0);
        moq_action_t drain[8];
        moq_session_poll_actions(sv, drain, 8);

        /* Now: [USE_ALIAS(unknown=99), REGISTER(alias=1 duplicate)]. */
        moq_d16_auth_token_t use_bad = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 99,
        };
        moq_d16_auth_token_t dup_reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,  /* already registered — session-fatal */
            .token_type = 20,
            .token_value = (const uint8_t *)"y",
            .token_value_len = 1,
        };
        uint8_t tb1[64], tb2[64];
        moq_buf_writer_t tw1, tw2;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw1, &use_bad);
        moq_d16_auth_token_encode(&tw2, &dup_reg);
        moq_kvp_entry_t p1[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
        };
        feed_subscribe(sv, 2, "ns", "t1", p1, 2);

        /* Session-fatal 0x14 MUST override the request-level 0x17. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x14);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 26. Reject first, then overflow REGISTER — session must close  */
    /*     [USE_ALIAS(unknown), REGISTER(overflow)]                      */
    /*     Session-fatal 0x13 must not be masked by the earlier reject.  */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        /* Tiny cache: room for exactly 1 small entry. */
        moq_session_cfg_t sx = MOQ_SESSION_CFG_INIT;
        sx.send_auth_token_cache_size = true;
        sx.auth_token_cache_size = 20;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &sx);

        /* Fill the cache with alias=1. */
        moq_d16_auth_token_t fill = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 1,
            .token_type = 1,
            .token_value = (const uint8_t *)"a",
            .token_value_len = 1,
        };
        uint8_t tb0[64];
        moq_buf_writer_t tw0;
        moq_buf_writer_init(&tw0, tb0, sizeof(tb0));
        moq_d16_auth_token_encode(&tw0, &fill);
        moq_kvp_entry_t p0[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb0, .value_len = moq_buf_writer_offset(&tw0),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 0, "ns", "t0", p0, 1);
        moq_event_t ev0;
        moq_session_poll_events(sv, &ev0, 1);
        moq_accept_subscribe_cfg_t acfg;
        moq_accept_subscribe_cfg_init(&acfg);
        moq_session_accept_subscribe(sv, ev0.u.subscribe_request.sub, &acfg, 0);
        moq_action_t drain[8];
        moq_session_poll_actions(sv, drain, 8);

        /* Now: [USE_ALIAS(unknown=99), REGISTER(alias=2 overflow)]. */
        moq_d16_auth_token_t use_bad = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS,
            .alias = 99,
        };
        moq_d16_auth_token_t overflow_reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 2,
            .token_type = 2,
            .token_value = (const uint8_t *)"bb",
            .token_value_len = 2,
        };
        uint8_t tb1[64], tb2[64];
        moq_buf_writer_t tw1, tw2;
        moq_buf_writer_init(&tw1, tb1, sizeof(tb1));
        moq_buf_writer_init(&tw2, tb2, sizeof(tb2));
        moq_d16_auth_token_encode(&tw1, &use_bad);
        moq_d16_auth_token_encode(&tw2, &overflow_reg);
        moq_kvp_entry_t p1[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb1, .value_len = moq_buf_writer_offset(&tw1),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tb2, .value_len = moq_buf_writer_offset(&tw2),
              .is_varint = false, .raw = NULL, .raw_len = 0 },
        };
        feed_subscribe(sv, 2, "ns", "t1", p1, 2);

        /* Session-fatal 0x13 MUST override the request-level 0x17. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);
        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x13);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 23. USE_ALIAS then event queue full: staging freed ============ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.max_events = 1;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;
        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        /* Register alias=1 via first SUBSCRIBE (will succeed). */
        uint8_t tok_val[32];
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 1,
            .token_type = 42,
            .token_value = (const uint8_t *)"secret", .token_value_len = 6,
        };
        size_t tv_len = build_auth_token_value(tok_val, sizeof(tok_val), &reg_tok);

        moq_kvp_entry_t sub_params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_val, .value_len = tv_len,
            .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", sub_params, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        /* Accept to free the event slot. */
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        /* Now fill the event queue (max_events=1) with a SUBSCRIBE_OK. */
        moq_session_poll_events(c, &ev, 1); /* drain client SUBSCRIBE_OK */

        /* Feed second SUBSCRIBE with USE_ALIAS(1). DON'T drain server events
         * so the event queue stays full when the second subscribe arrives. */
        feed_subscribe(sv, 2, "ns", "t2", NULL, 0); /* fills event slot */

        /* Now feed third SUBSCRIBE with USE_ALIAS that will hit event_queue_full */
        moq_d16_auth_token_t use_tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 1,
        };
        size_t uv_len = build_auth_token_value(tok_val, sizeof(tok_val), &use_tok);
        moq_kvp_entry_t use_params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_val, .value_len = uv_len,
            .is_varint = false,
        }};
        feed_subscribe(sv, 4, "ns", "t3", use_params, 1);

        /* The SUBSCRIBE with USE_ALIAS should have hit event_queue_full
         * and returned WOULD_BLOCK. Staging must be freed. */
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 24. USE_ALIAS then subscription pool full: staging freed ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.max_subscriptions = 1;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        /* Register alias=1 via first SUBSCRIBE. */
        uint8_t tok_val[32];
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 1,
            .token_type = 7,
            .token_value = (const uint8_t *)"abc", .token_value_len = 3,
        };
        size_t tv_len = build_auth_token_value(tok_val, sizeof(tok_val), &reg_tok);

        moq_kvp_entry_t sub_params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_val, .value_len = tv_len,
            .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", sub_params, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        /* Don't accept — keep the slot occupied. max_subscriptions=1 is full. */

        /* Feed second SUBSCRIBE with USE_ALIAS(1). Pool is full → REQUEST_ERROR. */
        moq_d16_auth_token_t use_tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 1,
        };
        size_t uv_len = build_auth_token_value(tok_val, sizeof(tok_val), &use_tok);
        moq_kvp_entry_t use_params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_val, .value_len = uv_len,
            .is_varint = false,
        }};
        feed_subscribe(sv, 2, "ns", "t2", use_params, 1);

        /* Pool full → REQUEST_ERROR sent. Staging must be freed. */
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 25. USE_ALIAS in PUBLISH_NAMESPACE, pool full: staging freed == */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.max_announcements = 1;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        /* Register alias=1 via SUBSCRIBE on server. */
        uint8_t tok_val[32];
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 1,
            .token_type = 99,
            .token_value = (const uint8_t *)"xyz", .token_value_len = 3,
        };
        size_t tv_len = build_auth_token_value(tok_val, sizeof(tok_val), &reg_tok);
        moq_kvp_entry_t reg_params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_val, .value_len = tv_len,
            .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", reg_params, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);

        /* Fill announcement pool: feed PUBLISH_NAMESPACE without token. */
        {
            uint8_t buf[256];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns1") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_d16_encode_publish_namespace(&w, 2, &ns, NULL, 0);
            moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        }
        moq_session_poll_events(sv, &ev, 1);
        /* Don't accept — pool full. max_announcements=1. */

        /* Feed PUBLISH_NAMESPACE with USE_ALIAS(1). Pool full → REQUEST_ERROR. */
        {
            moq_d16_auth_token_t use_tok = {
                .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 1,
            };
            size_t uv_len = build_auth_token_value(tok_val, sizeof(tok_val), &use_tok);
            moq_kvp_entry_t use_params[1] = {{
                .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
                .value = tok_val, .value_len = uv_len,
                .is_varint = false,
            }};

            uint8_t buf[256];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_bytes_t ns_parts[] = { moq_bytes_cstr("ns2") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_d16_encode_publish_namespace(&w, 4, &ns, use_params, 1);
            moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        }

        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 26. USE_ALIAS + malformed FORWARD: staging freed on close ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        /* Register alias=1 via first SUBSCRIBE. */
        uint8_t tok_val[32];
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 1,
            .token_type = 7,
            .token_value = (const uint8_t *)"abc", .token_value_len = 3,
        };
        size_t tv_len = build_auth_token_value(tok_val, sizeof(tok_val), &reg_tok);
        moq_kvp_entry_t reg_params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_val, .value_len = tv_len, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", reg_params, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        /* Feed SUBSCRIBE with USE_ALIAS(1) + malformed FORWARD param.
         * FORWARD (0x10) is even → varint. Value 2 is invalid (must be 0 or 1). */
        moq_d16_auth_token_t use_tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 1,
        };
        size_t uv_len = build_auth_token_value(tok_val, sizeof(tok_val), &use_tok);
        uint8_t bad_fwd[1];
        size_t bflen = moq_quic_varint_encode(2, bad_fwd, sizeof(bad_fwd));
        moq_kvp_entry_t bad_params[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tok_val, .value_len = uv_len, .is_varint = false },
            { .type = MOQ_MSG_PARAM_FORWARD,
              .value = bad_fwd, .value_len = bflen, .is_varint = true },
        };
        feed_subscribe(sv, 2, "ns", "t2", bad_params, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 27. USE_ALIAS + DELIVERY_TIMEOUT=0: staging freed on close === */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;

        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        /* Register alias=2 via first SUBSCRIBE. */
        uint8_t tok_val[32];
        moq_d16_auth_token_t reg_tok = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 2,
            .token_type = 9,
            .token_value = (const uint8_t *)"xyz", .token_value_len = 3,
        };
        size_t tv_len = build_auth_token_value(tok_val, sizeof(tok_val), &reg_tok);
        moq_kvp_entry_t reg_params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tok_val, .value_len = tv_len, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", reg_params, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        /* Feed SUBSCRIBE with DELIVERY_TIMEOUT=0 + USE_ALIAS(2).
         * Params must be in non-decreasing type order for delta KVP.
         * DELIVERY_TIMEOUT=0x02 < AUTH_TOKEN=0x03. */
        moq_d16_auth_token_t use_tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 2,
        };
        size_t uv_len = build_auth_token_value(tok_val, sizeof(tok_val), &use_tok);
        uint8_t zero_timeout[1];
        size_t ztlen = moq_quic_varint_encode(0, zero_timeout, sizeof(zero_timeout));
        moq_kvp_entry_t bad_params[2] = {
            { .type = MOQ_MSG_PARAM_DELIVERY_TIMEOUT,
              .value = zero_timeout, .value_len = ztlen, .is_varint = true },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tok_val, .value_len = uv_len, .is_varint = false },
        };
        feed_subscribe(sv, 2, "ns", "t2", bad_params, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 28. Cross-request REGISTER then USE_ALIAS ==================== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        uint8_t tv[32];
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 10,
            .token_type = 5, .token_value = (const uint8_t *)"hello",
            .token_value_len = 5,
        };
        size_t tvl = build_auth_token_value(tv, sizeof(tv), &reg);
        moq_kvp_entry_t p1[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", p1, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_d16_auth_token_t use = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 10,
        };
        tvl = build_auth_token_value(tv, sizeof(tv), &use);
        moq_kvp_entry_t p2[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 2, "ns", "t2", p2, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_type == 5);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_value.len == 5);
        MOQ_TEST_CHECK(memcmp(ev.u.subscribe_request.tokens[0].token_value.data,
                               "hello", 5) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 29. REGISTER, DELETE, then USE_ALIAS rejects ================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        uint8_t tv[32];
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 3,
            .token_type = 1, .token_value = (const uint8_t *)"k",
            .token_value_len = 1,
        };
        size_t tvl = build_auth_token_value(tv, sizeof(tv), &reg);
        moq_kvp_entry_t p1[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", p1, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_d16_auth_token_t del = {
            .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 3,
        };
        tvl = build_auth_token_value(tv, sizeof(tv), &del);
        moq_kvp_entry_t p2[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 2, "ns", "t2", p2, 1);
        moq_session_poll_events(sv, &ev, 1);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_d16_auth_token_t use = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 3,
        };
        tvl = build_auth_token_value(tv, sizeof(tv), &use);
        moq_kvp_entry_t p3[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 4, "ns", "t3", p3, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        bool found_err = false;
        for (size_t i = 0; i < na; i++) {
            if (decode_action_msg_type(&acts[i]) == MOQ_D16_REQUEST_ERROR)
                found_err = true;
        }
        MOQ_TEST_CHECK(found_err);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 30. REGISTER, DELETE, re-REGISTER same alias ================= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        uint8_t tv[32];
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 7,
            .token_type = 1, .token_value = (const uint8_t *)"a",
            .token_value_len = 1,
        };
        size_t tvl = build_auth_token_value(tv, sizeof(tv), &reg);
        moq_kvp_entry_t p1[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", p1, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_d16_auth_token_t del = {
            .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 7,
        };
        tvl = build_auth_token_value(tv, sizeof(tv), &del);
        moq_kvp_entry_t p2[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 2, "ns", "t2", p2, 1);
        moq_session_poll_events(sv, &ev, 1);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_d16_auth_token_t rereg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 7,
            .token_type = 2, .token_value = (const uint8_t *)"bb",
            .token_value_len = 2,
        };
        tvl = build_auth_token_value(tv, sizeof(tv), &rereg);
        moq_kvp_entry_t p3[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 4, "ns", "t3", p3, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_type == 2);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_value.len == 2);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 31. PUBLISH_NAMESPACE: REGISTER then USE_ALIAS =============== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        uint8_t tv[32];
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 1,
            .token_type = 8, .token_value = (const uint8_t *)"nskey",
            .token_value_len = 5,
        };
        size_t tvl = build_auth_token_value(tv, sizeof(tv), &reg);
        moq_kvp_entry_t p1[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", p1, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_d16_auth_token_t use = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 1,
        };
        tvl = build_auth_token_value(tv, sizeof(tv), &use);
        moq_kvp_entry_t p2[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        {
            uint8_t buf[256];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_bytes_t ns_parts[] = { moq_bytes_cstr("myns") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_d16_encode_publish_namespace(&w, 2, &ns, p2, 1);
            moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        }
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK(ev.u.namespace_published.token_count == 1);
        MOQ_TEST_CHECK(ev.u.namespace_published.tokens[0].token_type == 8);
        MOQ_TEST_CHECK(ev.u.namespace_published.tokens[0].token_value.len == 5);
        MOQ_TEST_CHECK(memcmp(ev.u.namespace_published.tokens[0].token_value.data,
                               "nskey", 5) == 0);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 32. USE_VALUE + USE_ALIAS same token → MALFORMED_AUTH_TOKEN === */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        uint8_t tv1[32], tv2[32];
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 1,
            .token_type = 3, .token_value = (const uint8_t *)"dup",
            .token_value_len = 3,
        };
        size_t tvl1 = build_auth_token_value(tv1, sizeof(tv1), &reg);
        moq_kvp_entry_t rp[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv1, .value_len = tvl1, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", rp, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        moq_d16_auth_token_t val = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 3, .token_value = (const uint8_t *)"dup",
            .token_value_len = 3,
        };
        tvl1 = build_auth_token_value(tv1, sizeof(tv1), &val);
        moq_d16_auth_token_t use = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 1,
        };
        size_t tvl2 = build_auth_token_value(tv2, sizeof(tv2), &use);
        moq_kvp_entry_t dup_params[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tv1, .value_len = tvl1, .is_varint = false },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tv2, .value_len = tvl2, .is_varint = false },
        };
        feed_subscribe(sv, 2, "ns", "t2", dup_params, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        bool found_err = false;
        for (size_t i = 0; i < na; i++) {
            if (decode_action_msg_type(&acts[i]) == MOQ_D16_REQUEST_ERROR)
                found_err = true;
        }
        MOQ_TEST_CHECK(found_err);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 33. USE_ALIAS then DELETE same alias in one message (ASan) ==== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t sextra = MOQ_SESSION_CFG_INIT;
        sextra.send_auth_token_cache_size = true;
        sextra.auth_token_cache_size = 1024;
        moq_session_cfg_t cextra = MOQ_SESSION_CFG_INIT;
        cextra.send_auth_token_cache_size = true;
        cextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, &cextra, &sextra);

        uint8_t tv[32];
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER, .alias = 5,
            .token_type = 1, .token_value = (const uint8_t *)"val",
            .token_value_len = 3,
        };
        size_t tvl = build_auth_token_value(tv, sizeof(tv), &reg);
        moq_kvp_entry_t rp[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t1", rp, 1);
        moq_event_t ev;
        moq_session_poll_events(sv, &ev, 1);
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        moq_session_accept_subscribe(sv, ev.u.subscribe_request.sub, &acc, 0);
        pump_actions_to_peer(sv, c, 0);

        uint8_t tv1[32], tv2[32];
        moq_d16_auth_token_t use = {
            .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 5,
        };
        size_t tvl1 = build_auth_token_value(tv1, sizeof(tv1), &use);
        moq_d16_auth_token_t del = {
            .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 5,
        };
        size_t tvl2 = build_auth_token_value(tv2, sizeof(tv2), &del);
        moq_kvp_entry_t seq_params[2] = {
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tv1, .value_len = tvl1, .is_varint = false },
            { .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
              .value = tv2, .value_len = tvl2, .is_varint = false },
        };
        feed_subscribe(sv, 2, "ns", "t2", seq_params, 2);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_type == 1);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 33. CLIENT_SETUP zero-length USE_VALUE -> MALFORMED_AUTH_TOKEN  *
     *  Semantic validation of the resolved value applies to SETUP tokens
     *  too; with no request to reject, the session closes with the
     *  MALFORMED_AUTH_TOKEN session error (0x16) -- distinct from the
     *  structural 0x6 close. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tok = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 7,
            .token_value = NULL,
            .token_value_len = 0,
        };
        feed_client_setup_with_tokens(sv, &tok, 1, NULL, 0);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(sv, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x16);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == 34. NUL-containing USE_VALUE on SUBSCRIBE -> REQUEST_ERROR 0x4  *
     *  The shared semantic check is request-level on request messages:
     *  REQUEST_ERROR carrying MALFORMED_AUTH_TOKEN (0x4), session alive,
     *  no SUBSCRIBE_REQUEST surfaced. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, NULL);

        uint8_t tv[32];
        moq_d16_auth_token_t bad = {
            .alias_type = MOQ_AUTH_TOKEN_USE_VALUE,
            .token_type = 7,
            .token_value = (const uint8_t *)"a\0b",
            .token_value_len = 3,
        };
        size_t tvl = build_auth_token_value(tv, sizeof(tv), &bad);
        moq_kvp_entry_t params[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tv, .value_len = tvl, .is_varint = false,
        }};
        feed_subscribe(sv, 0, "ns", "t", params, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 0);

        bool found_err = false;
        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(sv, acts, 4);
        for (size_t i = 0; i < na; i++) {
            if (decode_action_msg_type(&acts[i]) == MOQ_D16_REQUEST_ERROR) {
                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.send_control.data,
                                    acts[i].u.send_control.len);
                if (moq_control_decode_envelope(&r, &env) == MOQ_OK) {
                    moq_d16_request_error_t err;
                    if (moq_d16_decode_request_error(env.payload,
                            env.payload_len, &err) == MOQ_OK &&
                        err.error_code == 0x4)
                        found_err = true;
                }
            }
            moq_action_cleanup(&acts[i]);
        }
        MOQ_TEST_CHECK(found_err);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SERVER_SETUP: DELETE then REGISTER same alias succeeds ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_auth_token_cache_size = true;
        ccfg.auth_token_cache_size = 1024;

        moq_session_t *c = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        moq_action_t drain[4];
        size_t nd = moq_session_poll_actions(c, drain, 4);
        MOQ_TEST_CHECK(nd >= 1);
        for (size_t i = 0; i < nd; i++) moq_action_cleanup(&drain[i]);

        MOQ_TEST_CHECK(moq_token_cache_register(&c->peer_token_cache, 1, 10,
            (const uint8_t *)"old", 3) == MOQ_TOKEN_OK);

        moq_d16_auth_token_t tokens[2] = {
            { .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 1 },
            {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER,
                .alias = 1,
                .token_type = 20,
                .token_value = (const uint8_t *)"new",
                .token_value_len = 3,
            },
        };
        moq_result_t rc = feed_server_setup_with_tokens(c, tokens, 2, NULL, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        uint64_t rtype;
        const uint8_t *rval;
        size_t rlen;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&c->peer_token_cache, 1,
            &rtype, &rval, &rlen) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(rtype == 20);
        MOQ_TEST_CHECK(rlen == 3);
        MOQ_TEST_CHECK(memcmp(rval, "new", 3) == 0);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(ev.u.setup_complete.token_count == 1);
        MOQ_TEST_CHECK(ev.u.setup_complete.tokens[0].token_type == 20);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SERVER_SETUP: DELETE then USE_ALIAS same alias closes ======= */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_auth_token_cache_size = true;
        ccfg.auth_token_cache_size = 1024;

        moq_session_t *c = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        moq_action_t drain[4];
        size_t nd = moq_session_poll_actions(c, drain, 4);
        MOQ_TEST_CHECK(nd >= 1);
        for (size_t i = 0; i < nd; i++) moq_action_cleanup(&drain[i]);

        MOQ_TEST_CHECK(moq_token_cache_register(&c->peer_token_cache, 1, 10,
            (const uint8_t *)"val", 3) == MOQ_TOKEN_OK);

        moq_d16_auth_token_t tokens[2] = {
            { .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 1 },
            { .alias_type = MOQ_AUTH_TOKEN_USE_ALIAS, .alias = 1 },
        };
        moq_result_t rc = feed_server_setup_with_tokens(c, tokens, 2, NULL, 0);
        (void)rc;
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(c, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x3);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SERVER_SETUP: DELETE then DELETE same alias closes =========== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_auth_token_cache_size = true;
        ccfg.auth_token_cache_size = 1024;

        moq_session_t *c = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        moq_action_t drain[4];
        size_t nd = moq_session_poll_actions(c, drain, 4);
        MOQ_TEST_CHECK(nd >= 1);
        for (size_t i = 0; i < nd; i++) moq_action_cleanup(&drain[i]);

        MOQ_TEST_CHECK(moq_token_cache_register(&c->peer_token_cache, 1, 10,
            (const uint8_t *)"val", 3) == MOQ_TOKEN_OK);

        moq_d16_auth_token_t tokens[2] = {
            { .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 1 },
            { .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 1 },
        };
        moq_result_t rc = feed_server_setup_with_tokens(c, tokens, 2, NULL, 0);
        (void)rc;
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_CLOSED);

        moq_event_t evts[4];
        size_t ne = moq_session_poll_events(c, evts, 4);
        MOQ_TEST_CHECK(ne >= 1);
        MOQ_TEST_CHECK(evts[0].kind == MOQ_EVENT_SESSION_CLOSED);
        MOQ_TEST_CHECK(evts[0].u.closed.code == 0x3);
        for (size_t i = 0; i < ne; i++) moq_event_cleanup(&evts[i]);

        moq_action_t acts[4];
        size_t na = moq_session_poll_actions(c, acts, 4);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SERVER_SETUP: DELETE frees space for REGISTER ================ */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t ccfg = MOQ_SESSION_CFG_INIT;
        ccfg.alloc = &alloc;
        ccfg.perspective = MOQ_PERSPECTIVE_CLIENT;
        ccfg.send_auth_token_cache_size = true;
        ccfg.auth_token_cache_size = 20;

        moq_session_t *c = NULL;
        MOQ_TEST_CHECK(moq_session_create(&ccfg, 0, &c) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_start(c, 0) == MOQ_OK);
        moq_action_t drain[4];
        size_t nd = moq_session_poll_actions(c, drain, 4);
        MOQ_TEST_CHECK(nd >= 1);
        for (size_t i = 0; i < nd; i++) moq_action_cleanup(&drain[i]);

        MOQ_TEST_CHECK(moq_token_cache_register(&c->peer_token_cache, 1, 10,
            (const uint8_t *)"a", 1) == MOQ_TOKEN_OK);

        moq_d16_auth_token_t tokens[2] = {
            { .alias_type = MOQ_AUTH_TOKEN_DELETE, .alias = 1 },
            {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER,
                .alias = 2,
                .token_type = 30,
                .token_value = (const uint8_t *)"b",
                .token_value_len = 1,
            },
        };
        moq_result_t rc = feed_server_setup_with_tokens(c, tokens, 2, NULL, 0);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);

        MOQ_TEST_CHECK(moq_token_cache_lookup(&c->peer_token_cache, 1,
            NULL, NULL, NULL) == MOQ_TOKEN_ERR_UNKNOWN);
        uint64_t rtype;
        const uint8_t *rval;
        size_t rlen;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&c->peer_token_cache, 2,
            &rtype, &rval, &rlen) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(rtype == 30);

        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(c, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SETUP_COMPLETE);
        MOQ_TEST_CHECK(ev.u.setup_complete.token_count == 1);
        MOQ_TEST_CHECK(ev.u.setup_complete.tokens[0].token_type == 30);
        moq_event_cleanup(&ev);

        moq_session_destroy(c);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == Setup REGISTER retry must not hit duplicate alias ============ */
    /*
     * Regression: d16_handle_setup_client called moq_token_cache_register
     * before scratch_copy. If scratch_copy failed for a later token, the
     * first token was already in the cache. On retry (recv_buf reprocessed),
     * the first REGISTER hit DUPLICATE → spurious session close 0x14.
     * After fix: no cache mutation before retryable failure point; retry
     * returns the same error without spurious close.
     */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.output_scratch_size = 16;
        scfg.send_auth_token_cache_size = true;
        scfg.auth_token_cache_size = 1024;

        moq_session_t *sv = NULL;
        MOQ_TEST_CHECK(moq_session_create(&scfg, 0, &sv) == MOQ_OK);

        moq_d16_auth_token_t tokens[2] = {
            {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER,
                .alias = 1,
                .token_type = 42,
                .token_value = (const uint8_t *)"ab",
                .token_value_len = 2,
            },
            {
                .alias_type = MOQ_AUTH_TOKEN_REGISTER,
                .alias = 2,
                .token_type = 43,
                .token_value = (const uint8_t *)"0123456789abcdef",
                .token_value_len = 16,
            },
        };

        moq_result_t rc = feed_client_setup_with_tokens(sv, tokens, 2, NULL, 0);
        MOQ_TEST_CHECK(rc < 0);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        rc = moq_session_on_control_bytes(sv, NULL, 0, 1);
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == SUBSCRIBE REGISTER retry must not hit duplicate alias ======== */
    /*
     * Regression: process_auth_tokens calls moq_token_cache_register
     * before event_queue_full / scratch exhaustion can fail retryably.
     * On retry, REGISTER hits DUPLICATE → spurious session close 0x14.
     */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_events = 1;
        svextra.send_auth_token_cache_size = true;
        svextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        /* establish_pair already drained SETUP_COMPLETE on both sides.
         * Fill the 1-slot event queue with a no-auth subscribe. */
        moq_event_t ev;
        feed_subscribe(sv, 0, "ns", "t_fill", NULL, 0);
        /* Event queue now full (max_events=1). */

        /* Feed SUBSCRIBE with REGISTER token — auth processes first,
         * then event_queue_full fires → WOULD_BLOCK. */
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 42,
            .token_type = 7,
            .token_value = (const uint8_t *)"secret",
            .token_value_len = 6,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &reg);
        moq_kvp_entry_t p[1] = {{
            .type = MOQ_MSG_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        feed_subscribe(sv, 2, "ns", "t_auth", p, 1);

        /* Session must NOT be closed with 0x14. */
        MOQ_TEST_CHECK(moq_session_state(sv) != MOQ_SESS_CLOSED);

        /* Drain the blocking event to make room. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&ev);

        /* Retry: reprocess pending bytes. Must succeed. */
        MOQ_TEST_CHECK(moq_session_process_pending(sv, 1) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);

        /* The retried SUBSCRIBE event must have the token. */
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        MOQ_TEST_CHECK(ev.u.subscribe_request.token_count == 1);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_type == 7);
        MOQ_TEST_CHECK(ev.u.subscribe_request.tokens[0].token_value.len == 6);
        MOQ_TEST_CHECK(memcmp(ev.u.subscribe_request.tokens[0].token_value.data,
                              "secret", 6) == 0);
        moq_event_cleanup(&ev);

        /* Alias must be committed exactly once. */
        uint64_t rtype;
        const uint8_t *rval;
        size_t rlen;
        MOQ_TEST_CHECK(moq_token_cache_lookup(&sv->peer_token_cache, 42,
            &rtype, &rval, &rlen) == MOQ_TOKEN_OK);
        MOQ_TEST_CHECK(rtype == 7);
        MOQ_TEST_CHECK(rlen == 6);
        MOQ_TEST_CHECK(memcmp(rval, "secret", 6) == 0);

        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK(as.balance == 0);
    }

    /* == PUBLISH_NAMESPACE REGISTER retry must not hit duplicate ====== */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);

        moq_session_cfg_t svextra = MOQ_SESSION_CFG_INIT;
        svextra.max_events = 1;
        svextra.send_auth_token_cache_size = true;
        svextra.auth_token_cache_size = 1024;
        moq_session_t *c = NULL, *sv = NULL;
        establish_pair(&alloc, 10, 10, &c, &sv, NULL, &svextra);

        /* Fill the 1-slot event queue with a no-auth PUBLISH_NAMESPACE. */
        {
            uint8_t buf[256];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_bytes_t ns_parts[] = { moq_bytes_cstr("fill") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_d16_encode_publish_namespace(&w, 0, &ns, NULL, 0);
            moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        }

        /* Feed PUBLISH_NAMESPACE with REGISTER token — event queue full. */
        moq_d16_auth_token_t reg = {
            .alias_type = MOQ_AUTH_TOKEN_REGISTER,
            .alias = 55,
            .token_type = 9,
            .token_value = (const uint8_t *)"nskey",
            .token_value_len = 5,
        };
        uint8_t tb[64];
        moq_buf_writer_t tw;
        moq_buf_writer_init(&tw, tb, sizeof(tb));
        moq_d16_auth_token_encode(&tw, &reg);
        moq_kvp_entry_t p[1] = {{
            .type = MOQ_SETUP_PARAM_AUTHORIZATION_TOKEN,
            .value = tb, .value_len = moq_buf_writer_offset(&tw),
            .is_varint = false, .raw = NULL, .raw_len = 0,
        }};
        {
            uint8_t buf[256];
            moq_buf_writer_t w;
            moq_buf_writer_init(&w, buf, sizeof(buf));
            moq_bytes_t ns_parts[] = { moq_bytes_cstr("auth") };
            moq_namespace_t ns = { ns_parts, 1 };
            moq_d16_encode_publish_namespace(&w, 2, &ns, p, 1);
            moq_session_on_control_bytes(sv, buf, moq_buf_writer_offset(&w), 0);
        }

        MOQ_TEST_CHECK_EQ_INT(moq_session_state(sv), MOQ_SESS_ESTABLISHED);

        /* Alias must NOT be in cache yet. */
        MOQ_TEST_CHECK_EQ_INT(moq_token_cache_lookup(&sv->peer_token_cache, 55,
            NULL, NULL, NULL), MOQ_TOKEN_ERR_UNKNOWN);

        /* Drain the blocking event. */
        moq_event_t ev;
        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        moq_event_cleanup(&ev);

        /* Retry. */
        MOQ_TEST_CHECK_EQ_INT(moq_session_process_pending(sv, 1), MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(moq_session_state(sv), MOQ_SESS_ESTABLISHED);

        MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
        MOQ_TEST_CHECK_EQ_INT(ev.kind, MOQ_EVENT_NAMESPACE_PUBLISHED);
        MOQ_TEST_CHECK_EQ_SIZE(ev.u.namespace_published.token_count, 1);
        MOQ_TEST_CHECK_EQ_U64(ev.u.namespace_published.tokens[0].token_type, 9);
        moq_event_cleanup(&ev);

        /* Alias committed exactly once. */
        uint64_t rtype;
        const uint8_t *rval;
        size_t rlen;
        MOQ_TEST_CHECK_EQ_INT(moq_token_cache_lookup(&sv->peer_token_cache, 55,
            &rtype, &rval, &rlen), MOQ_TOKEN_OK);
        MOQ_TEST_CHECK_EQ_U64(rtype, 9);
        MOQ_TEST_CHECK_EQ_SIZE(rlen, 5);

        moq_action_t acts[8];
        size_t na = moq_session_poll_actions(sv, acts, 8);
        for (size_t i = 0; i < na; i++) moq_action_cleanup(&acts[i]);

        moq_session_destroy(c);
        moq_session_destroy(sv);
        MOQ_TEST_CHECK_EQ_INT((int)as.balance, 0);
    }

    MOQ_TEST_PASS("test_session_auth");
    return failures;
}
