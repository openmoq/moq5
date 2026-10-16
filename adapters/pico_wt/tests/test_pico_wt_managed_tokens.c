/*
 * SETUP authorization tokens through the managed WebTransport facade
 * (moq_pico_wt_managed_cfg_t.setup_auth_tokens):
 *   - a token set whose SETUP cannot fit the default session send arena is
 *     refused at create, also when each value fits alone;
 *   - one just inside it reaches the server whole, on draft-16 and draft-18;
 *   - a client whose SETUP cannot be queued after its session was created
 *     (the SETUP buffer allocation fails) ends as a failed connection (fatal,
 *     wait() reports it), its session never established, and every
 *     allocation is returned at destroy.
 *
 * Usage: test_pico_wt_managed_tokens --cert <pem> --key <pem>
 */
#include <moq/moq.h>
#include <moq/pico_wt_managed.h>

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* No zero byte: a receiving session on this profile set refuses a SETUP token
 * value that contains one. */
static uint8_t big_byte(size_t i) { return (uint8_t)((i * 37u + 5u) % 255u + 1u); }

/* -- an allocator that counts, and can refuse one allocation ---------- */

/* Global state: the facade is given a NULL allocator context. */
static struct {
    atomic_long outstanding;      /* bytes allocated and not yet freed */
    atomic_int  armed;            /* refuse the next allocation in range */
    atomic_int  refused;
    size_t      lo, hi;           /* the size range refused when armed */
} g_ta;

static void *ta_alloc(size_t size, void *ctx)
{
    (void)ctx;
    if (size >= g_ta.lo && size <= g_ta.hi) {
        int one = 1;
        if (atomic_compare_exchange_strong(&g_ta.armed, &one, 0)) {
            atomic_fetch_add(&g_ta.refused, 1);
            return NULL;
        }
    }
    void *p = malloc(size);
    if (p) atomic_fetch_add(&g_ta.outstanding, (long)size);
    return p;
}

static void *ta_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    (void)ctx;
    void *p = realloc(ptr, new_size);
    if (p) atomic_fetch_add(&g_ta.outstanding, (long)new_size - (long)old_size);
    return p;
}

static void ta_free(void *ptr, size_t size, void *ctx)
{
    (void)ctx;
    if (!ptr) return;
    atomic_fetch_sub(&g_ta.outstanding, (long)size);
    free(ptr);
}

/* -- server: records the first SETUP_COMPLETE's tokens ----------------- */

typedef struct {
    atomic_int setups;
    size_t     token_count;
    uint64_t   type0;
    size_t     len0;
    bool       pattern0;
} server_app_t;

static int server_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)now;
    server_app_t *a = (server_app_t *)ctx;
    moq_session_t *s = moq_pico_wt_managed_session(m);
    if (!s) return 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SETUP_COMPLETE) {
            a->token_count = ev.u.setup_complete.token_count;
            if (a->token_count > 0) {
                const moq_resolved_token_t *k = ev.u.setup_complete.tokens;
                a->type0 = k[0].token_type;
                a->len0 = k[0].token_value.len;
                a->pattern0 = true;
                for (size_t i = 0; i < a->len0; i++)
                    if (k[0].token_value.data[i] != big_byte(i)) {
                        a->pattern0 = false;
                        break;
                    }
            }
            atomic_fetch_add(&a->setups, 1);
        }
        moq_event_cleanup(&ev);
    }
    return 0;
}

static int client_pump(moq_pico_wt_managed_t *m, uint64_t now, void *ctx)
{
    (void)m; (void)now; (void)ctx;
    return 0;
}

static void client_cfg(moq_pico_wt_managed_cfg_t *cfg, const moq_alloc_t *alloc, int port,
                       const char *wt_protocols, const moq_auth_token_t *tokens, size_t n)
{
    moq_pico_wt_managed_cfg_init_sized(cfg, sizeof(*cfg));
    cfg->alloc = alloc;
    cfg->perspective = MOQ_PERSPECTIVE_CLIENT;
    cfg->host = "127.0.0.1";
    cfg->port = port;
    cfg->insecure_skip_verify = true;
    cfg->on_pump = client_pump;
    cfg->wt_protocols = wt_protocols;
    cfg->setup_auth_tokens = tokens;
    cfg->setup_auth_token_count = n;
}

static uint8_t g_big[4096];

/* A token set the default arena cannot hold is refused at create. */
static void test_create_refusals(int port)
{
    moq_auth_token_t two[2] = { { 1, { g_big, sizeof(g_big) } }, { 2, { g_big, 2000 } } };
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg, moq_alloc_default(), port, NULL, two, 1);
    moq_pico_wt_managed_t *cli = NULL;
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_ERR_INVAL && !cli);
    two[0].token_value.len = 2000;
    cfg.setup_auth_token_count = 2;
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_ERR_INVAL && !cli);
}

/* 1: the server saw the token whole; 0: no setup reached the server. */
static int run_transmission(int port, server_app_t *sapp, const char *proto)
{
    int before = atomic_load(&sapp->setups);
    moq_auth_token_t near = { 12, { g_big, 3800 } };
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg, moq_alloc_default(), port, proto, &near, 1);
    moq_pico_wt_managed_t *cli = NULL;
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_OK);
    if (!cli) return 0;
    for (int i = 0; i < 200 && atomic_load(&sapp->setups) == before; i++) {
        if (moq_pico_wt_managed_is_fatal(cli)) break;
        moq_pico_wt_managed_wait(cli, 25000);
    }
    int got = atomic_load(&sapp->setups) > before;
    moq_pico_wt_managed_stop(cli);
    moq_pico_wt_managed_destroy(cli);
    if (!got) return 0;
    CHECK(sapp->token_count == 1);
    CHECK(sapp->type0 == 12 && sapp->len0 == 3800 && sapp->pattern0);
    return 1;
}

/* The SETUP buffer of a session already created cannot be allocated. */
static void run_setup_alloc_failure(int port, const char *proto)
{
    /* A 1000-byte value: draft-18 asks 32 + 1000 + 32 bytes for its SETUP,
     * draft-16 256 + the encoded token + 16; nothing else this client
     * allocates after create falls in the range. */
    atomic_store(&g_ta.outstanding, 0);
    atomic_store(&g_ta.armed, 0);
    atomic_store(&g_ta.refused, 0);
    g_ta.lo = 1060;
    g_ta.hi = 1300;
    moq_alloc_t alloc = { .alloc = ta_alloc, .realloc = ta_realloc, .free = ta_free,
                          .ctx = NULL };
    moq_auth_token_t tok = { 5, { g_big, 1000 } };
    moq_pico_wt_managed_cfg_t cfg;
    client_cfg(&cfg, &alloc, port, proto, &tok, 1);
    moq_pico_wt_managed_t *cli = NULL;
    CHECK(moq_pico_wt_managed_create(&cfg, &cli) == MOQ_OK);
    if (!cli) return;
    atomic_store(&g_ta.armed, 1);
    bool fatal = false;
    for (int i = 0; i < 200 && !fatal; i++) {
        moq_pico_wt_managed_wait(cli, 25000);
        fatal = moq_pico_wt_managed_is_fatal(cli);
    }
    moq_pico_wt_managed_stop(cli);
    if (atomic_load(&g_ta.refused) == 1) {
        /* A failed connection: never an established session. */
        CHECK(fatal);
        CHECK(moq_pico_wt_managed_wait(cli, 0) == MOQ_ERR_CLOSED);
        moq_session_t *s = moq_pico_wt_managed_session(cli);
        CHECK(!s || moq_session_state(s) != MOQ_SESS_ESTABLISHED);
    } else {
        fprintf(stderr, "setup allocation was not reached (%s)\n", proto);
        CHECK(0 && "the refused allocation was the SETUP buffer");
    }
    moq_pico_wt_managed_destroy(cli);
    CHECK(atomic_load(&g_ta.outstanding) == 0);
}

/* One server per client (a managed server serves one session): run
 * `scenario` against a fresh server, moving to another port when the server
 * cannot be created. */
typedef int (*scenario_fn)(int port, server_app_t *sapp, const char *proto);

static int scenario_transmission(int port, server_app_t *sapp, const char *proto)
{
    return run_transmission(port, sapp, proto);
}

static int scenario_alloc_failure(int port, server_app_t *sapp, const char *proto)
{
    (void)sapp;
    run_setup_alloc_failure(port, proto);
    return 1;
}

static int with_server(const char *cert, const char *key, unsigned pbase, scenario_fn fn,
                       const char *proto)
{
    for (int attempt = 0; attempt < 4; attempt++) {
        int port = (int)pbase + 97 + attempt * 311;
        server_app_t sapp;
        memset(&sapp, 0, sizeof(sapp));
        moq_pico_wt_managed_cfg_t scfg;
        moq_pico_wt_managed_cfg_init_sized(&scfg, sizeof(scfg));
        scfg.alloc = moq_alloc_default();
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        scfg.cert_path = cert;
        scfg.key_path = key;
        scfg.port = port;
        scfg.send_request_capacity = true;
        scfg.initial_request_capacity = 16;
        scfg.on_pump = server_pump;
        scfg.on_pump_ctx = &sapp;
        scfg.wt_protocols = "moqt-18, moqt-16";
        moq_pico_wt_managed_t *srv = NULL;
        if (moq_pico_wt_managed_create(&scfg, &srv) != MOQ_OK || !srv) continue;
        for (int i = 0; i < 3; i++) moq_pico_wt_managed_wait(srv, 100000);
        int ok = fn(moq_pico_wt_managed_local_port(srv), &sapp, proto);
        moq_pico_wt_managed_stop(srv);
        moq_pico_wt_managed_destroy(srv);
        if (ok) return 1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    const char *cert = NULL, *key = NULL;
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        fprintf(stderr, "usage: %s --cert <pem> --key <pem>\n", argv[0]);
        return 2;
    }
    for (size_t i = 0; i < sizeof(g_big); i++) g_big[i] = big_byte(i);
    unsigned pbase = 20000u + (unsigned)((uintptr_t)getpid() * 2654435761u) % 30000u;
    test_create_refusals((int)pbase);
    CHECK(with_server(cert, key, pbase, scenario_transmission, "moqt-16"));
    CHECK(with_server(cert, key, pbase + 7, scenario_transmission, "moqt-18"));
    CHECK(with_server(cert, key, pbase + 13, scenario_alloc_failure, "moqt-16"));
    CHECK(with_server(cert, key, pbase + 19, scenario_alloc_failure, "moqt-18"));
    if (failures) {
        fprintf(stderr, "test_pico_wt_managed_tokens: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_pico_wt_managed_tokens: PASS\n");
    return 0;
}
