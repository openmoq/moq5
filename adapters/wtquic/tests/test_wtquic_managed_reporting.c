#include <moq/wtquic_network_managed.h>
#include <wtquic/wtquic_network.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef MOQ_TEST_SERVICE
#include <moq/endpoint.h>
#endif

static unsigned allocations, backend_calls, callbacks;
static wtq_result_t blocked_create(const wtq_nw_conn_cfg_t *cfg,
                                  wtq_nw_conn_t **out)
{
    (void)cfg;
    backend_calls++;
    *out = NULL;
    return WTQ_ERR_UNSUPPORTED;
}
#define wtq_nw_conn_create blocked_create
#include "../wtquic_network_managed.c"
#undef wtq_nw_conn_create

static void *deny_alloc(size_t n, void *ctx)
{ (void)n; (void)ctx; allocations++; return NULL; }
static void *deny_realloc(void *p, size_t old, size_t n, void *ctx)
{ (void)p; (void)old; return deny_alloc(n, ctx); }
static void deny_free(void *p, size_t n, void *ctx)
{ (void)p; (void)n; (void)ctx; abort(); }
static size_t live;
static void *count_alloc(size_t n, void *ctx)
{ (void)ctx; allocations++; void *p = malloc(n); if (p) live++; return p; }
static void *count_realloc(void *p, size_t old, size_t n, void *ctx)
{ (void)old; (void)ctx; allocations++; void *q = realloc(p, n); if (q && !p) live++; return q; }
static void count_free(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; if (p) { live--; free(p); } }
static void notified(void *ctx) { (void)ctx; callbacks++; }
static void activity(moq_wtquic_network_managed_t *m, void *ctx)
{ (void)m; (void)ctx; callbacks++; }
static int pumped(moq_wtquic_network_managed_t *m,
                  moq_wtquic_network_managed_lane_t *lane, uint64_t now, void *ctx)
{ (void)m; (void)lane; (void)now; (void)ctx; callbacks++; return 0; }
static int failures;
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL %d: %s\n", __LINE__, #x); failures++; } } while (0)

static void check_cfg(moq_wtquic_network_managed_cfg_t *cfg, moq_result_t want)
{
    allocations = backend_calls = callbacks = 0;
    moq_wtquic_network_managed_t *out = (void *)cfg;
    CHECK(moq_wtquic_network_managed_create(cfg, &out) == want);
    CHECK(out == NULL);
    CHECK(allocations == 0);
    CHECK(backend_calls == 0);
    CHECK(callbacks == 0);
}

int main(void)
{
    moq_alloc_t a = { .alloc = deny_alloc, .realloc = deny_realloc,
                          .free = deny_free, .ctx = NULL };
    moq_wtquic_network_managed_cfg_t cfg;
    moq_wtquic_network_managed_cfg_init(&cfg);
    cfg.alloc = &a;
    cfg.host = "localhost";
    cfg.port = 443;
    cfg.on_stopped = notified;
    cfg.on_lane_pump = pumped;
    cfg.on_activity = activity;
    check_cfg(&cfg, MOQ_ERR_UNSUPPORTED);
    moq_alloc_t counted = { .ctx = NULL, .alloc = count_alloc,
        .realloc = count_realloc, .free = count_free };
    cfg.alloc = &counted;
    check_cfg(&cfg, MOQ_ERR_UNSUPPORTED);
    CHECK(live == 0);
    cfg.alloc = &a;
    const char *valid[] = { "moqt-16", " moqt-18 , moqt-16 " };
    for (size_t i = 0; i < sizeof(valid) / sizeof(valid[0]); i++) {
        cfg.wt_protocols = valid[i];
        check_cfg(&cfg, MOQ_ERR_UNSUPPORTED);
    }
    const char *invalid[] = { "", " ", "moqt-16,", ",moqt-16",
        "moqt-16,,moqt-18", "moqt-16 moqt-18", "moqt-16,moqt-16", "unknown" };
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        cfg.wt_protocols = invalid[i];
        check_cfg(&cfg, MOQ_ERR_INVAL);
    }
    cfg.wt_protocols = NULL;
    cfg.perspective = (moq_perspective_t)99;
    check_cfg(&cfg, MOQ_ERR_INVAL);
    cfg.perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg.port = 0;
    check_cfg(&cfg, MOQ_ERR_INVAL);
    cfg.port = 443;
    check_cfg(NULL, MOQ_ERR_INVAL);
    size_t floor = offsetof(moq_wtquic_network_managed_cfg_t, insecure_skip_verify) + sizeof(cfg.insecure_skip_verify);
    for (size_t n = floor - 1; n <= sizeof(cfg); n++) {
        unsigned char *p = malloc(n);
        CHECK(p != NULL);
        if (!p) break;
        memcpy(p, &cfg, n);
        ((moq_wtquic_network_managed_cfg_t *)p)->struct_size = (uint32_t)n;
        check_cfg((void *)p, n < floor ? MOQ_ERR_INVAL : MOQ_ERR_UNSUPPORTED);
        free(p);
    }
#ifdef MOQ_TEST_SERVICE
    moq_endpoint_cfg_t ecfg;
    moq_endpoint_cfg_init_sized(&ecfg, sizeof(ecfg));
    static const char url[] = "https://localhost:443/moq";
    ecfg.url = (moq_bytes_t){ .data = (const uint8_t *)url, .len = sizeof(url) - 1 };
    ecfg.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
    ecfg.alloc = &counted;
    moq_endpoint_t *ep = (void *)&ecfg;
    CHECK(moq_endpoint_connect(&ecfg, &ep) == MOQ_ERR_UNSUPPORTED);
    CHECK(ep == NULL && backend_calls == 0 && callbacks == 0);
    CHECK(live == 0);
#endif
    return failures ? 1 : 0;
}
