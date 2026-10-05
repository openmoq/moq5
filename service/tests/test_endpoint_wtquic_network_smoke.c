/* Service selection remains public, but Network cannot satisfy the current
 * receive policy. Exercise real endpoint propagation, never a live peer. */
#include <moq/endpoint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static size_t live;
static int failures;
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "FAIL %d: %s\n", __LINE__, #x); failures++; } } while (0)
static void *allocate(size_t n, void *ctx)
{ (void)ctx; void *p = malloc(n); if (p) live++; return p; }
static void *resize(void *p, size_t old, size_t n, void *ctx)
{ (void)old; (void)ctx; void *q = realloc(p, n); if (q && !p) live++; return q; }
static void release(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; if (p) { live--; free(p); } }
static moq_bytes_t bytes(const char *s)
{ return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) }; }
static void expect(moq_endpoint_cfg_t *cfg, moq_result_t result)
{
    moq_endpoint_t *ep = (void *)cfg;
    CHECK(moq_endpoint_connect(cfg, &ep) == result);
    CHECK(ep == NULL && live == 0);
}
int main(void)
{
    moq_alloc_t a = { .ctx = NULL, .alloc = allocate, .realloc = resize, .free = release };
    moq_endpoint_cfg_t cfg;
    moq_endpoint_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &a;
    cfg.url = bytes("https://localhost:443/moq");
    cfg.backend = MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK;
    for (unsigned i = 0; i < 32; i++) {
        cfg.insecure_skip_verify = (i & 1) != 0;
        expect(&cfg, MOQ_ERR_UNSUPPORTED);
    }
    cfg.insecure_skip_verify = false;
    cfg.ca_file = bytes("/not-opened/policy-ca.pem");
    expect(&cfg, MOQ_ERR_UNSUPPORTED);
    cfg.ca_file = (moq_bytes_t){0};
    cfg.sni = bytes("different.example");
    expect(&cfg, MOQ_ERR_UNSUPPORTED);
    cfg.sni = (moq_bytes_t){0};
    cfg.url = bytes("moqt://localhost:443");
    expect(&cfg, MOQ_ERR_UNSUPPORTED);
    cfg.url = bytes("invalid://localhost");
    expect(&cfg, MOQ_ERR_INVAL);
    cfg.url = bytes("https://localhost:443/moq");
    cfg.struct_size = 0;
    expect(&cfg, MOQ_ERR_INVAL);
    if (!failures) puts("PASS: endpoint Network synchronous unsupported, balanced ownership");
    return failures ? 1 : 0;
}
