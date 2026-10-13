/* Explicit fail-closed contract, not transport authentication parity. */
#include <moq/proxygen_wt_managed.h>
#include <assert.h>
#include <stddef.h>
#include <string.h>
static int pump(moq_proxygen_wt_managed_t *m, uint64_t now, void *ctx)
{ (void)m; (void)now; (void)ctx;  return 0; }
int main(void)
{
    moq_proxygen_wt_managed_cfg_t cfg;
    moq_proxygen_wt_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.host = "127.0.0.1"; cfg.port = 443;
    cfg.on_pump = pump;
    moq_auth_token_t token = {0x13, {(const uint8_t *)"token", 5}};
    cfg.setup_auth_tokens = &token; cfg.setup_auth_token_count = 1;
    moq_proxygen_wt_managed_t *fac = NULL;
    assert(moq_proxygen_wt_managed_create(&cfg, &fac) == MOQ_ERR_UNSUPPORTED);
    assert(!fac);
    cfg.setup_auth_tokens = NULL;
    assert(moq_proxygen_wt_managed_create(&cfg, &fac) == MOQ_ERR_UNSUPPORTED);
    cfg.setup_auth_token_count = 0; cfg.setup_auth_tokens = &token;
    assert(moq_proxygen_wt_managed_create(&cfg, &fac) == MOQ_ERR_UNSUPPORTED);
    union { moq_proxygen_wt_managed_cfg_t cfg; unsigned char bytes[sizeof(cfg) + 16]; } canary;
    for (size_t n = sizeof(uint32_t); n <= sizeof(cfg); ++n) {
        memset(canary.bytes, 0xa5, sizeof(canary.bytes));
        moq_proxygen_wt_managed_cfg_init_sized(&canary.cfg, n);
        for (size_t i = n; i < sizeof(canary.bytes); ++i) assert(canary.bytes[i] == 0xa5);
    }
    memset(canary.bytes, 0xa5, sizeof(canary.bytes));
    moq_proxygen_wt_managed_cfg_init(&canary.cfg);
    assert(canary.cfg.struct_size == offsetof(moq_proxygen_wt_managed_cfg_t, setup_auth_tokens));
    for (size_t i = canary.cfg.struct_size; i < sizeof(canary.bytes); ++i)
        assert(canary.bytes[i] == 0xa5);
    return 0;
}
