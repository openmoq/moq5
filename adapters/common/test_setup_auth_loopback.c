/* Real peer SETUP capture, including server/deferred-session ownership. */
#if defined(TEST_PICO_WT)
#include <moq/pico_wt_managed.h>
#define CFG moq_pico_wt_managed_cfg_t
#define FAC moq_pico_wt_managed_t
#define POINTER_INIT moq_pico_wt_managed_cfg_init
#define INIT moq_pico_wt_managed_cfg_init_sized
#define CREATE moq_pico_wt_managed_create
#define STOP moq_pico_wt_managed_stop
#define DESTROY moq_pico_wt_managed_destroy
#elif defined(TEST_MSQUIC)
#include <moq/msquic_managed.h>
#define CFG moq_msquic_managed_cfg_t
#define FAC moq_msquic_managed_t
#define INIT moq_msquic_managed_cfg_init_sized
#define CREATE moq_msquic_managed_create
#define STOP moq_msquic_managed_stop
#define DESTROY moq_msquic_managed_destroy
#define LANE moq_msquic_managed_lane_t
#define CONN moq_msquic_managed_conn_t
#define NEXT moq_msquic_lane_next_conn
#define SESSION moq_msquic_managed_session
#define CONN_SESSION moq_msquic_managed_conn_session
#elif defined(TEST_MVFST)
#include <moq/mvfst.h>
#define CFG moq_mvfst_managed_cfg_t
#define FAC moq_mvfst_managed_t
#define POINTER_INIT moq_mvfst_managed_cfg_init
#define INIT moq_mvfst_managed_cfg_init_sized
#define CREATE moq_mvfst_managed_create
#define STOP moq_mvfst_managed_stop
#define DESTROY moq_mvfst_managed_destroy
#define LANE moq_mvfst_managed_lane_t
#define CONN moq_mvfst_conn_t
#define NEXT moq_mvfst_lane_next_conn
#define SESSION moq_mvfst_managed_session
#define CONN_SESSION moq_mvfst_conn_session
#else
#include <moq/picoquic_threaded.h>
#define CFG moq_pq_threaded_cfg_t
#define FAC moq_pq_threaded_t
#define POINTER_INIT moq_pq_threaded_cfg_init
#define INIT moq_pq_threaded_cfg_init_sized
#define CREATE moq_pq_threaded_create
#define STOP moq_pq_threaded_stop
#define DESTROY moq_pq_threaded_destroy
#define LANE moq_pq_threaded_lane_t
#define CONN moq_pq_threaded_conn_t
#define NEXT moq_pq_threaded_lane_next_conn
#define SESSION moq_pq_threaded_session
#define CONN_SESSION moq_pq_threaded_conn_session
#endif
#include <assert.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>

typedef struct capture { atomic_int seen; atomic_int finish; const char *expected; } capture_t;
static void capture(moq_session_t *s, capture_t *c)
{
    if (!s) return;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) {
        if (ev.kind != MOQ_EVENT_SETUP_COMPLETE) continue;
        if (!c->expected) {
            assert(ev.u.setup_complete.token_count == 0);
            atomic_store(&c->seen, 1);
            continue;
        }
        assert(ev.u.setup_complete.token_count == 1);
        const moq_resolved_token_t *t = ev.u.setup_complete.tokens;
        assert(t[0].token_type == 0x13);
        assert(t[0].token_value.len == strlen(c->expected));
        assert(memcmp(t[0].token_value.data, c->expected, t[0].token_value.len) == 0);
        atomic_store(&c->seen, 1);
    }
}
#if defined(TEST_PICO_WT)
static int pump(FAC *m, uint64_t now, void *ctx)
{
    (void)now;
    capture(moq_pico_wt_managed_session(m), ctx);
    return atomic_load(&((capture_t *)ctx)->finish);
}
#else
static int pump(FAC *m, LANE *lane, uint64_t now, void *ctx)
{
    (void)now;
    CONN *conn = NULL;
    while ((conn = NEXT(lane, conn)))
        capture(CONN_SESSION(conn), ctx);
    capture(SESSION(m), ctx);
    return 0;
}
#endif
static void configure(CFG *cfg, int server, int port, capture_t *c, int draft)
{
    INIT(cfg, sizeof(*cfg));
    cfg->alloc = moq_alloc_default();
    cfg->perspective = server ? MOQ_PERSPECTIVE_SERVER : MOQ_PERSPECTIVE_CLIENT;
    cfg->port = port;
    cfg->host = "127.0.0.1";
    cfg->cert_path = server ? MOQ_TEST_CERT_PATH : NULL;
    cfg->key_path = server ? MOQ_TEST_KEY_PATH : NULL;
    cfg->insecure_skip_verify = !server;
#if !defined(TEST_PICO_WT) && !defined(TEST_MSQUIC)
    cfg->send_buffer_size = 65536;
    cfg->recv_buffer_size = 65536;
#endif
#if defined(TEST_PICO_WT)
    cfg->on_pump = pump;
    cfg->on_pump_ctx = c;
    cfg->wt_protocols = draft == 18 ? "moqt-18" : draft == 16 ? "moqt-16" : NULL;
#else
    cfg->on_lane_pump = pump;
#if defined(TEST_MSQUIC)
    cfg->on_lane_pump_user = c;
    cfg->version = draft == 18 ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
    static const moq_version_t versions[] = {MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_16};
    if (server && draft) { cfg->version = 0; cfg->versions = versions; cfg->version_count = 2; }
#else
#if defined(TEST_MVFST)
    cfg->user_ctx = c;
#else
    cfg->on_lane_pump_ctx = c;
#endif
    static const char *v16[] = {"moqt-16"}, *v18[] = {"moqt-18"};
    cfg->alpn_list = draft == 18 ? v18 : draft == 16 ? v16 : NULL;
    cfg->alpn_count = draft ? 1 : 0;
#endif
#endif
}
static void run(int draft, int large, int auth_mask)
{
    capture_t server_capture = {ATOMIC_VAR_INIT(0), ATOMIC_VAR_INIT(0), "client-token"};
    capture_t client_capture = {ATOMIC_VAR_INIT(0), ATOMIC_VAR_INIT(0), "server-token"};
    CFG cfg;
    int port = 18000 + (getpid() % 15000) + draft;
    unsigned char server_bytes[16385], client_bytes[16385];
    size_t n = large ? 16384 : 12;
    memset(server_bytes, 's', n); memcpy(server_bytes, "server-token", 12); server_bytes[n] = 0;
    memset(client_bytes, 'c', n); memcpy(client_bytes, "client-token", 12); client_bytes[n] = 0;
    server_capture.expected = (auth_mask & 1) ? strdup((const char *)client_bytes) : NULL;
    client_capture.expected = (auth_mask & 2) ? strdup((const char *)server_bytes) : NULL;
    assert(!(auth_mask & 1) || server_capture.expected);
    assert(!(auth_mask & 2) || client_capture.expected);
    moq_auth_token_t token = {0x13, {server_bytes, n}};
    configure(&cfg, 1, port, &server_capture, draft);
    cfg.setup_auth_tokens = (auth_mask & 2) ? &token : NULL; cfg.setup_auth_token_count = (auth_mask & 2) ? 1 : 0;
    FAC *server = NULL, *client = NULL;
    assert(CREATE(&cfg, &server) == MOQ_OK);
    memset(server_bytes, 'X', sizeof(server_bytes));
    token.token_type = 99; token.token_value.data = NULL;
    token.token_type = 0x13;
    token.token_value.data = client_bytes; token.token_value.len = n;
    configure(&cfg, 0, port, &client_capture, draft);
    cfg.setup_auth_tokens = (auth_mask & 1) ? &token : NULL; cfg.setup_auth_token_count = (auth_mask & 1) ? 1 : 0;
    assert(CREATE(&cfg, &client) == MOQ_OK);
    memset(client_bytes, 'Y', sizeof(client_bytes));
    memset(&token, 0, sizeof(token));
    for (int i = 0; i < 1000 && (!atomic_load(&server_capture.seen) || !atomic_load(&client_capture.seen)); ++i)
        usleep(10000);
    assert(atomic_load(&server_capture.seen));
    assert(atomic_load(&client_capture.seen));
#if defined(TEST_PICO_WT)
    atomic_store(&client_capture.finish, 1);
    moq_pico_wt_managed_wake(client);
    for (int i = 0; i < 100 && moq_pico_wt_managed_wait(client, 10000) != MOQ_ERR_CLOSED; ++i) {}
    atomic_store(&server_capture.finish, 1);
    moq_pico_wt_managed_wake(server);
    for (int i = 0; i < 100 && moq_pico_wt_managed_wait(server, 10000) != MOQ_ERR_CLOSED; ++i) {}
#endif
    STOP(client); DESTROY(client);
    STOP(server); DESTROY(server);
    free((void *)server_capture.expected); free((void *)client_capture.expected);
}
typedef struct allocation_probe { size_t calls, fail_at, live; } allocation_probe_t;
static void *probe_alloc(size_t n, void *ctx)
{
    allocation_probe_t *p = ctx;
    if (++p->calls == p->fail_at) return NULL;
    void *v = malloc(n);
    if (v) ++p->live;
    return v;
}
static void probe_free(void *ptr, size_t n, void *ctx)
{
    (void)n;
    allocation_probe_t *p = ctx;
    if (ptr) { assert(p->live); --p->live; free(ptr); }
}
static void *probe_realloc(void *ptr, size_t old, size_t n, void *ctx)
{
    (void)old;
    allocation_probe_t *p = ctx;
    if (++p->calls == p->fail_at) return NULL;
    void *v = realloc(ptr, n);
    if (v && !ptr) ++p->live;
    return v;
}
static void failures(void)
{
    CFG cfg;
    capture_t c = {ATOMIC_VAR_INIT(0), ATOMIC_VAR_INIT(0), "unused"};
    unsigned char bytes[] = "owned-token";
    moq_auth_token_t token = {0x13, {bytes, sizeof(bytes)-1}};
    for (size_t failure = 1; failure <= 8; ++failure) {
        allocation_probe_t p = {0, failure, 0};
        moq_alloc_t allocator = {&p, probe_alloc, probe_realloc, probe_free};
        configure(&cfg, 1, 43000 + (getpid() % 1000), &c, 16);
        cfg.alloc = &allocator;
        cfg.setup_auth_tokens = &token; cfg.setup_auth_token_count = 1;
        FAC *fac = NULL;
        moq_result_t rc = CREATE(&cfg, &fac);
        assert(rc == MOQ_ERR_NOMEM || (failure > 6 && rc == MOQ_ERR_INTERNAL));
        assert(!fac && p.live == 0);
    }
    configure(&cfg, 1, 43000 + (getpid() % 1000), &c, 16);
    cfg.setup_auth_tokens = &token; cfg.setup_auth_token_count = 1;
    token.token_type = UINT64_MAX;
    FAC *fac = NULL;
    assert(CREATE(&cfg, &fac) == MOQ_ERR_INVAL && !fac);
    token.token_type = 0x13;
    token.token_value.len = SIZE_MAX;
    assert(CREATE(&cfg, &fac) == MOQ_ERR_BUFFER && !fac);

    /* A lone list pointer (including poison) is ignored as an incomplete pair.
     * The exact short allocation also makes ASan enforce the read boundary. */
    configure(&cfg, 1, 43000 + (getpid() % 1000), &c, 16);
    cfg.setup_auth_tokens = (const moq_auth_token_t *)(uintptr_t)1;
    size_t prefix = offsetof(CFG, setup_auth_token_count);
    cfg.struct_size = (uint32_t)prefix;
    void *short_cfg = malloc(prefix);
    assert(short_cfg);
    memcpy(short_cfg, &cfg, prefix);
    assert(CREATE(short_cfg, &fac) == MOQ_OK);
    STOP(fac); DESTROY(fac); free(short_cfg);

    /* Both initializers must leave the caller's first out-of-range byte alone. */
    union { CFG cfg; unsigned char bytes[sizeof(CFG) + 16]; } canary;
#ifdef POINTER_INIT
    memset(canary.bytes, 0xa5, sizeof(canary.bytes));
    POINTER_INIT(&canary.cfg);
    assert(canary.cfg.struct_size <= offsetof(CFG, setup_auth_tokens));
    for (size_t i = canary.cfg.struct_size; i < sizeof(canary.bytes); ++i)
        assert(canary.bytes[i] == 0xa5);
#endif
    for (size_t n = sizeof(uint32_t); n <= sizeof(CFG); ++n) {
        memset(canary.bytes, 0xa5, sizeof(canary.bytes));
        INIT(&canary.cfg, n);
        for (size_t i = n; i < sizeof(canary.bytes); ++i) assert(canary.bytes[i] == 0xa5);
    }
}
int main(void)
{
    failures();
    run(0, 0, 3); run(16, 0, 3); run(18, 0, 3);
    for (int mask = 1; mask <= 3; ++mask) {
        run(16, 1, mask); run(18, 1, mask);
    }
    puts("PASS: owned SETUP tokens captured at both peers (legacy, draft-16, draft-18)");
    return 0;
}
