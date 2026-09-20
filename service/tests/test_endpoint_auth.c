/* Capture the production endpoint -> managed facade boundary before I/O. */
#include <moq/endpoint.h>
#include <moq/auth.h>
#include <moq/picoquic_threaded.h>
#include "test_session_support.h"
#include <stddef.h>
#include <string.h>
static int failures;
static unsigned creates, selections;
static bool deny, expect_auth;
static const uint8_t token_bytes[] = {0xd2, 0, 0x84, 0xff};
static const char *authority = "[::1]:4433";
static const char *path = "/moq?x=1";
typedef struct { test_alloc_state_t base; unsigned calls, fail; } oom_state_t;
static void *oom_alloc(size_t size, void *ctx) {
    oom_state_t *state = ctx;
    if (++state->calls == state->fail) return NULL;
    return test_alloc(size, &state->base);
}
static void *oom_realloc(void *ptr, size_t old, size_t size, void *ctx) {
    oom_state_t *state = ctx;
    if (++state->calls == state->fail) return NULL;
    return test_realloc(ptr, old, size, &state->base);
}
static void oom_free(void *ptr, size_t size, void *ctx) {
    oom_state_t *state = ctx;
    test_free(ptr, size, &state->base);
}

static moq_result_t select_setup(void *ctx, const moq_auth_request_t *r,
    moq_auth_token_t *tokens, size_t capacity, size_t *count)
{
    (void)ctx;
    selections++;
    MOQ_TEST_CHECK(r->action == MOQ_AUTH_CLIENT_SETUP);
    MOQ_TEST_CHECK(r->ns.count == 0 && r->name.len == 0 && capacity >= 1);
    if (deny) return MOQ_ERR_INVAL;
    tokens[0] = (moq_auth_token_t){16, {token_bytes, sizeof(token_bytes)}};
    *count = 1;
    return MOQ_OK;
}
moq_result_t moq_test_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                        moq_pq_threaded_t **out)
{
    creates++;
    *out = NULL;
    MOQ_TEST_CHECK(cfg->setup_auth_token_count == (expect_auth ? 1u : 0u));
    if (expect_auth && cfg->setup_auth_token_count == 1) {
        MOQ_TEST_CHECK(cfg->setup_auth_tokens[0].token_type == 16);
        MOQ_TEST_CHECK(cfg->setup_auth_tokens[0].token_value.len == sizeof(token_bytes));
        MOQ_TEST_CHECK(memcmp(cfg->setup_auth_tokens[0].token_value.data,
                              token_bytes, sizeof(token_bytes)) == 0);
        MOQ_TEST_CHECK(cfg->send_buffer_size >= 65536);
        MOQ_TEST_CHECK(cfg->recv_buffer_size >= 65536);
    }
    MOQ_TEST_CHECK(cfg->setup_authority.len == strlen(authority));
    MOQ_TEST_CHECK(memcmp(cfg->setup_authority.data, authority, strlen(authority)) == 0);
    MOQ_TEST_CHECK(cfg->setup_path.len == strlen(path));
    MOQ_TEST_CHECK(memcmp(cfg->setup_path.data, path, strlen(path)) == 0);
    /* Deterministic fake transport failure: no sockets/threads. */
    return MOQ_ERR_INTERNAL;
}
int main(void)
{
    test_alloc_state_t state = {0}; moq_alloc_t alloc = test_allocator(&state);
    moq_endpoint_cfg_t cfg; moq_endpoint_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.url = MOQ_BYTES_LITERAL("moqt://[::1]:4433/moq?x=1");
    cfg.insecure_skip_verify = true; cfg.alloc = &alloc;
    moq_auth_source_t src; moq_auth_source_init_sized(&src, sizeof(src));
    src.select = select_setup; cfg.setup_auth = &src;
    moq_endpoint_t *ep = NULL;
    deny = true;
    MOQ_TEST_CHECK(moq_endpoint_connect(&cfg, &ep) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(creates == 0 && selections == 1 && !ep && state.balance == 0);
    deny = false; expect_auth = true;
    MOQ_TEST_CHECK(moq_endpoint_connect(&cfg, &ep) == MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(creates == 1 && selections == 2 && !ep && state.balance == 0);
    src.select = NULL;
    moq_auth_token_t token = {16, {token_bytes, sizeof(token_bytes)}};
    src.tokens = &token; src.token_count = 1;
    MOQ_TEST_CHECK(moq_endpoint_connect(&cfg, &ep) == MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(creates == 2 && selections == 2 && !ep && state.balance == 0);
    /* Incomplete tail must never dereference a poison pointer. */
    expect_auth = false;
    cfg.setup_auth = (const moq_auth_source_t *)(uintptr_t)1;
    cfg.struct_size = offsetof(moq_endpoint_cfg_t, setup_auth) + sizeof(cfg.setup_auth) - 1;
    MOQ_TEST_CHECK(moq_endpoint_connect(&cfg, &ep) == MOQ_ERR_INTERNAL);
    MOQ_TEST_CHECK(creates == 3 && selections == 2 && !ep && state.balance == 0);
    /* Every construction allocation fails cleanly before reaching transport. */
    oom_state_t oom = {0};
    moq_alloc_t failing = {&oom, oom_alloc, oom_realloc, oom_free};
    cfg.alloc = &failing; cfg.struct_size = sizeof(cfg); cfg.setup_auth = &src;
    expect_auth = true;
    MOQ_TEST_CHECK(moq_endpoint_connect(&cfg, &ep) == MOQ_ERR_INTERNAL);
    unsigned allocations = oom.calls, baseline_creates = creates;
    MOQ_TEST_CHECK(allocations > 0 && oom.base.balance == 0);
    for (unsigned i = 1; i <= allocations; ++i) {
        oom.calls = 0; oom.fail = i;
        MOQ_TEST_CHECK(moq_endpoint_connect(&cfg, &ep) == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(!ep && oom.base.balance == 0 && creates == baseline_creates);
    }
    MOQ_TEST_PASS("endpoint_auth");
    return failures ? 1 : 0;
}
