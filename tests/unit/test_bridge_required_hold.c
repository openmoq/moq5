#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include "test_support.h"
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

static int failures;
static unsigned allocations, calls;

static void *fail_alloc(size_t n, void *ctx)
{ (void)n; (void)ctx; allocations++; return NULL; }
static void *fail_realloc(void *p, size_t old, size_t n, void *ctx)
{ (void)p; (void)old; return fail_alloc(n, ctx); }
static void counted_free(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; free(p); }
static moq_transport_result_t open_stream(void *ctx, uint64_t *id)
{ (void)ctx; (void)id; calls++; return MOQ_TRANSPORT_ERROR; }
static moq_transport_result_t write_stream(void *ctx, uint64_t id,
    const uint8_t *data, size_t len, bool fin)
{ (void)ctx; (void)id; (void)data; (void)len; (void)fin; calls++; return MOQ_TRANSPORT_ERROR; }
static moq_transport_result_t terminal(void *ctx, uint64_t id, uint64_t code)
{ (void)ctx; (void)id; (void)code; calls++; return MOQ_TRANSPORT_ERROR; }
static moq_transport_result_t close_transport(void *ctx, uint64_t code,
    const uint8_t *reason, size_t len)
{ (void)ctx; (void)code; (void)reason; (void)len; calls++; return MOQ_TRANSPORT_ERROR; }

static void check(moq_session_t *s, const moq_transport_bridge_cfg_t *cfg,
                  const moq_transport_endpoint_ops_t *ops, moq_result_t expected)
{
    moq_transport_bridge_t *out = (moq_transport_bridge_t *)(uintptr_t)1;
    allocations = calls = 0;
    MOQ_TEST_CHECK(moq_transport_bridge_create(cfg, s, ops, NULL, &out) == expected);
    MOQ_TEST_CHECK(out == NULL);
    MOQ_TEST_CHECK(allocations == (expected == MOQ_ERR_NOMEM ? 1u : 0u));
    MOQ_TEST_CHECK(calls == 0);
}

int main(void)
{
    moq_session_cfg_t sc = MOQ_SESSION_CFG_INIT;
    sc.alloc = moq_alloc_default();
    sc.perspective = MOQ_PERSPECTIVE_CLIENT;
    moq_session_t *s = NULL;
    MOQ_TEST_CHECK(moq_session_create(&sc, 0, &s) == MOQ_OK);
    moq_alloc_t allocator = {.alloc = fail_alloc, .realloc = fail_realloc, .free = counted_free};
    moq_transport_bridge_cfg_t cfg;
    moq_transport_bridge_cfg_init(&cfg, &allocator);
    moq_transport_endpoint_ops_t ops = MOQ_TRANSPORT_ENDPOINT_OPS_INIT;
    ops.open_uni = ops.open_bidi = open_stream;
    ops.write = write_stream;
    ops.reset_stream = ops.stop_sending = terminal;
    ops.close_transport = close_transport;
    check(s, &cfg, &ops, MOQ_ERR_UNSUPPORTED);
    ops.struct_size = offsetof(moq_transport_endpoint_ops_t, close_transport) + sizeof(ops.close_transport);
    check(s, &cfg, &ops, MOQ_ERR_UNSUPPORTED);
    ops.capabilities = MOQ_TRANSPORT_CAP_HOLD_INPUT;
    check(s, &cfg, &ops, MOQ_ERR_NOMEM);
    ops.capabilities = 0;
    ops.write = NULL;
    check(s, &cfg, &ops, MOQ_ERR_INVAL);
    ops.write = write_stream;
    cfg.struct_size = 0;
    check(s, &cfg, &ops, MOQ_ERR_INVAL);
    cfg.struct_size = sizeof(cfg);
    check(NULL, &cfg, &ops, MOQ_ERR_INVAL);
    check(s, &cfg, NULL, MOQ_ERR_INVAL);
    /* The allocation is genuinely shorter than capabilities: ASan detects
     * any attempted capability read before the vtable-size rejection. */
    uint32_t *short_ops = malloc(sizeof(*short_ops));
    MOQ_TEST_CHECK(short_ops != NULL);
    if (short_ops) {
        *short_ops = sizeof(*short_ops);
        check(s, &cfg, (const moq_transport_endpoint_ops_t *)short_ops, MOQ_ERR_INVAL);
        free(short_ops);
    }
    moq_session_destroy(s);
    MOQ_TEST_PASS("bridge_required_hold");
    return failures ? 1 : 0;
}
