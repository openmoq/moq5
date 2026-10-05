/* Public Network policy contract. Mechanism-only deadline tests remain in
 * test_wtquic_network_managed_internal.c; no successful Network MoQ connection
 * is implied by those white-box controls. This test never starts a peer. */
#include <moq/wtquic_network_managed.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned allocs, frees, notifications;
static int failures;
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL %d: %s\n", __LINE__, #x); failures++; } } while (0)
static void *allocate(size_t n, void *ctx)
{ (void)ctx; allocs++; return malloc(n); }
static void *resize(void *p, size_t old, size_t n, void *ctx)
{ (void)old; (void)ctx; allocs++; return realloc(p, n); }
static void release(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; frees++; free(p); }
static int pump(moq_wtquic_network_managed_t *m,
                moq_wtquic_network_managed_lane_t *lane, uint64_t now, void *ctx)
{ (void)m; (void)lane; (void)now; (void)ctx; notifications++; return 0; }
static void activity(moq_wtquic_network_managed_t *m, void *ctx)
{ (void)m; (void)ctx; notifications++; }
static void stopped(void *ctx) { (void)ctx; notifications++; }
static uint64_t deadline(void *ctx)
{ (void)ctx; notifications++; return 1; }

static void expect(const char *name, const moq_wtquic_network_managed_cfg_t *cfg,
                   moq_result_t result)
{
    unsigned before = allocs;
    moq_wtquic_network_managed_t *m = (void *)&before;
    CHECK(moq_wtquic_network_managed_create(cfg, &m) == result);
    CHECK(m == NULL);
    CHECK(allocs == before && frees == 0 && notifications == 0);
    printf("policy row: %s\n", name);
}

int main(void)
{
    moq_alloc_t alloc = { .ctx = NULL, .alloc = allocate, .realloc = resize, .free = release };
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.host = "localhost";
    cfg.port = 443;
    cfg.on_lane_pump = pump;
    cfg.on_activity = activity;
    cfg.on_stopped = stopped;
    cfg.app_deadline_us = deadline;
    const char *offers[] = { NULL, "moqt-16", "moqt-18", "moqt-18,moqt-16",
                             " moqt-18 , moqt-16 " };
    /* Legacy, exact drafts and multi-offer all reject before negotiation.
     * Verification/trust, actor/deadline callbacks and ownership cannot change
     * that result. A second create is a fresh rejection, not async reconnect. */
    for (size_t i = 0; i < sizeof(offers) / sizeof(offers[0]); i++) {
        cfg.wt_protocols = offers[i];
        for (int verify = 0; verify < 2; verify++) {
            cfg.insecure_skip_verify = verify != 0;
            expect(offers[i] ? offers[i] : "legacy", &cfg, MOQ_ERR_UNSUPPORTED);
        }
    }
    for (unsigned i = 0; i < 64; i++)
        expect("repeat/create-free ownership", &cfg, MOQ_ERR_UNSUPPORTED);

    const char *bad[] = { "", " ", ",moqt-16", "moqt-16,", "moqt-16,,moqt-18",
        "moqt-16 moqt-18", "moqt-16,moqt-16", "unknown", "\"moqt-16\"" };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        cfg.wt_protocols = bad[i];
        expect("malformed offer precedes policy", &cfg, MOQ_ERR_INVAL);
    }
    cfg.wt_protocols = NULL;
    cfg.perspective = MOQ_PERSPECTIVE_SERVER;
    expect("server", &cfg, MOQ_ERR_UNSUPPORTED);
    cfg.perspective = (moq_perspective_t)99;
    expect("invalid perspective", &cfg, MOQ_ERR_INVAL);
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.lane_count = 2;
    expect("multiple lanes", &cfg, MOQ_ERR_UNSUPPORTED);
    cfg.lane_count = 1;
    cfg.port = 0;
    expect("invalid port", &cfg, MOQ_ERR_INVAL);
    cfg.port = 443;
    cfg.host = NULL;
    expect("missing host", &cfg, MOQ_ERR_INVAL);
    cfg.host = "localhost";
    cfg.alloc = NULL;
    expect("missing allocator", &cfg, MOQ_ERR_INVAL);
    cfg.alloc = &alloc;
    expect("null config", NULL, MOQ_ERR_INVAL);
    CHECK(moq_wtquic_network_managed_create(&cfg, NULL) == MOQ_ERR_INVAL);

    size_t floor = offsetof(moq_wtquic_network_managed_cfg_t, insecure_skip_verify)
        + sizeof(cfg.insecure_skip_verify);
    /* Exact allocations make accidental optional-tail reads causal under ASan. */
    for (size_t n = floor - 1; n <= sizeof(cfg); n++) {
        unsigned char *raw = malloc(n);
        CHECK(raw != NULL);
        if (!raw) break;
        memcpy(raw, &cfg, n);
        ((moq_wtquic_network_managed_cfg_t *)raw)->struct_size = (uint32_t)n;
        expect("exact prefix", (void *)raw, n < floor ? MOQ_ERR_INVAL : MOQ_ERR_UNSUPPORTED);
        free(raw);
    }
    struct { moq_wtquic_network_managed_cfg_t cfg; unsigned char tail[32]; } future;
    memset(&future, 0xA5, sizeof(future));
    future.cfg = cfg;
    future.cfg.struct_size = sizeof(future);
    expect("future-sized config", &future.cfg, MOQ_ERR_UNSUPPORTED);
    for (size_t i = 0; i < sizeof(future.tail); i++) CHECK(future.tail[i] == 0xA5);
    CHECK(allocs == 0 && frees == 0 && notifications == 0);
    if (!failures) puts("PASS: public Network policy (no allocation/startup/callbacks)");
    return failures ? 1 : 0;
}
