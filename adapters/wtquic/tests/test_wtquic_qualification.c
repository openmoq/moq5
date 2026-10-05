#define main held_schedule_main
#include "test_wtquic_hold_input.c"
#undef main
#include <stdlib.h>

static size_t allocations, live, fail_at;
static void *count_alloc(size_t n, void *ctx)
{
    (void)ctx;
    if (++allocations == fail_at) return NULL;
    void *p = malloc(n);
    if (p) live++;
    return p;
}
static void count_free(void *p, size_t n, void *ctx)
{ (void)n; (void)ctx; if (p) { live--; free(p); } }
static void *count_realloc(void *p, size_t old_n, size_t n, void *ctx)
{
    (void)old_n; (void)ctx;
    if (++allocations == fail_at) return NULL;
    void *q = realloc(p, n);
    if (q && !p) live++;
    return q;
}

static bool steal_capacity;
static moq_wtquic_conn_t *service_conn;
static struct wtq_stream service_streams[2];
static unsigned service_calls, service_published, unsafe_service;
static wtq_result_t publish_one(wtq_session_t *s)
{
    service_calls++;
    if (!service_conn->in_service || service_conn->feed_depth) unsafe_service++;
    if (service_published < 2) {
        unsigned index = service_published++;
        ev_stream_opened(s, &service_streams[index], false, service_conn);
    }
    return WTQ_OK;
}
static void admission_hook(moq_wtquic_conn_t *c, void *user)
{
    (void)user;
    if (steal_capacity) c->session->rx_cap = 0;
}

static int retention_races(void)
{
    int failures = 0;
    for (int reset = 0; reset < 2; reset++) {
        moq_session_cfg_t scfg;
        moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
        scfg.version = MOQ_VERSION_DRAFT_16;
        moq_session_t *s = NULL;
        CHECK(moq_session_create(&scfg, 0, &s) == MOQ_OK);
        if (!s) return failures + 1;
        moq_wtquic_conn_cfg_t cfg;
        moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.alloc = moq_alloc_default(); cfg.session = s;
        cfg.wt_session = (wtq_session_t *)(uintptr_t)1;
        cfg.hook = admission_hook;
        moq_wtquic_conn_t *c = NULL;
        CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_OK);
        if (!c) { moq_session_destroy(s); return failures + 1; }
        size_t capacity = s->rx_cap;
        s->state = MOQ_SESS_ESTABLISHED;
        s->rx_cap = 0;
        expired = NULL;
        struct wtq_stream *st = new_stream(false);
        ev_stream_opened(c->ws, st, false, c);
        watched_key = stream_find_by_handle(c, st)->id;
        feeds = refused = exact_replays = accepted_replays = 0;
        expected_len = sizeof(expected); expected_fin = true;
        uint8_t bytes[sizeof(expected)]; memcpy(bytes, expected, sizeof(bytes));
        resume_conn = c;
        g_pause_rc = WTQ_OK;
        close_on_pause = !reset;
        ev_stream_data(c->ws, st, bytes, sizeof(bytes), true, c);
        memset(bytes, 0xa5, sizeof(bytes));
        struct moq_wtq_stream *ms = stream_find(c, watched_key);
        CHECK(ms != NULL && ms->held && ms->borrowed == 0);
        if (!reset) CHECK(ms->st == NULL); /* terminal inside pause, before unpin */
        steal_capacity = true;
        for (int retry = 0; retry < 3; retry++) {
            s->rx_cap = capacity;
            unsigned prior = feeds;
            conn_service(c); /* hook steals capacity after pending clears */
            CHECK(feeds == prior + 1 && ms->held && ms->held_fin);
            CHECK(memcmp(ms->held_bytes, expected, sizeof(expected)) == 0);
        }
        CHECK(refused == 4 && exact_replays == 3 && accepted_replays == 0);
        steal_capacity = false;
        if (reset) {
            ev_stream_reset(c->ws, st, 0, c);
            CHECK(!ms->held);
            ev_stream_closed(c->ws, st, c);
        }
        s->rx_cap = capacity;
        conn_service(c);
        CHECK(accepted_replays == (reset ? 0u : 1u));
        CHECK(stream_find(c, watched_key) == NULL);
        unsigned prior = feeds;
        conn_service(c);
        CHECK(feeds == prior && stale_calls == 0);
        moq_wtquic_conn_destroy(c); moq_session_destroy(s);
        expired = NULL;
    }
    return failures;
}

int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;
    int failures = retention_races();
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_t *session = NULL;
    CHECK(moq_session_create(&scfg, 0, &session) == MOQ_OK);
    if (!session) return 1;
    moq_alloc_t alloc = { .alloc = count_alloc, .realloc = count_realloc, .free = count_free };
    moq_wtquic_conn_cfg_t cfg;
    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &alloc;
    cfg.session = session;
    cfg.wt_session = (wtq_session_t *)(uintptr_t)1;
    moq_wtquic_conn_t *c = NULL;

    /* Every short tail byte remains poisoned; no partial pointer query. */
    for (size_t n = 0; n < sizeof(cfg); n++) {
        unsigned char *p = malloc(n ? n : 1);
        memset(p, 0xa5, n ? n : 1);
        if (n >= sizeof(uint32_t)) {
            uint32_t size = (uint32_t)n;
            memcpy(p, &size, sizeof(size));
            if (n >= sizeof(moq_wtquic_conn_cfg_v0_t)) {
                memcpy(p, &cfg, sizeof(moq_wtquic_conn_cfg_v0_t));
                memcpy(p, &size, sizeof(size));
            }
            allocations = query_calls = 0;
            moq_result_t rc = moq_wtquic_conn_create((const void *)p, &c);
            CHECK(rc == (n < sizeof(moq_wtquic_conn_cfg_v0_t) ? MOQ_ERR_INVAL : MOQ_ERR_UNSUPPORTED));
            CHECK(c == NULL && allocations == 0 && query_calls == 0);
        }
        free(p);
    }
    unsigned char init[sizeof(cfg) + 8];
    memset(init, 0xa5, sizeof(init));
    moq_wtquic_conn_cfg_init_sized((void *)init, sizeof(init));
    CHECK(((moq_wtquic_conn_cfg_t *)(void *)init)->struct_size == sizeof(cfg));
    for (size_t i = sizeof(cfg); i < sizeof(init); i++) CHECK(init[i] == 0xa5);
    cfg.wt_session = NULL;
    CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_ERR_UNSUPPORTED);
    cfg.wt_session = (wtq_session_t *)(uintptr_t)1;
    for (int unsupported = 0; unsupported < 5; unsupported++) {
        contract_rc = unsupported == 0 ? WTQ_ERR_UNSUPPORTED : WTQ_OK;
        contract_mode = unsupported == 1 ? WTQ_RECEIVE_PAUSE_DELIVERY_ONLY :
                        unsupported == 2 ? (wtq_receive_pause_mode_t)99 : WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED;
        contract_quantum = unsupported == 3 ? 0 : unsupported == 4 ? 65536 : 65535;
        allocations = 0;
        CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_ERR_UNSUPPORTED);
        CHECK(c == NULL && allocations == 0);
    }
    contract_rc = WTQ_OK; contract_mode = WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED; contract_quantum = 65535;
    bool succeeded = false;
    for (fail_at = 1; fail_at < 32; fail_at++) {
        allocations = 0;
        moq_result_t rc = moq_wtquic_conn_create(&cfg, &c);
        CHECK(rc == MOQ_ERR_NOMEM || rc == MOQ_OK);
        if (rc == MOQ_OK) {
            succeeded = true;
            CHECK(c->ops.capabilities & MOQ_TRANSPORT_CAP_HOLD_INPUT);
            moq_wtquic_conn_destroy(c); c = NULL;
        } else CHECK(c == NULL);
        CHECK(live == 0 && abort_calls == 0);
        if (succeeded) break;
    }
    CHECK(succeeded);
    fail_at = 0;
    CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_OK);
    if (!c) { moq_session_destroy(session); return 1; }
    size_t saved = session->rx_cap;
    session->state = MOQ_SESS_ESTABLISHED;
    session->rx_cap = 0;
    struct wtq_stream streams[MOQ_WTQ_MAX_STREAMS + 1] = {0};
    uint8_t *bytes = calloc(65536, 1);
    CHECK(bytes != NULL);
    if (!bytes) { moq_wtquic_conn_destroy(c); moq_session_destroy(session); return 1; }
    bytes[0] = 4;
    for (size_t i = 0; i < MOQ_WTQ_MAX_STREAMS; i++) {
        ev_stream_opened(c->ws, &streams[i], false, c);
        ev_stream_data(c->ws, &streams[i], bytes, 65535, true, c);
        ev_stream_closed(c->ws, &streams[i], c);
        CHECK(c->streams[i].in_use && c->streams[i].held && c->streams[i].st == NULL);
        CHECK(c->streams[i].held_len == 65535 && c->streams[i].held_fin);
    }
    memset(bytes, 0xa5, 65536);
    size_t held = 0;
    for (size_t i = 0; i < MOQ_WTQ_MAX_STREAMS; i++) {
        held += c->streams[i].held_len;
        CHECK(c->streams[i].held_bytes[0] == 4 && c->streams[i].held_bytes[65534] == 0);
    }
    CHECK(held == 1048560);
    unsigned before_abort = abort_calls;
    /* Qualified provider admission counts the retained API slots. Omitting
     * adapter retention makes this deliver the old lossy seventeenth callback.
     * The real provider's corresponding 16-handle schedule is msquic_ops. */
    if (retained < MOQ_WTQ_MAX_STREAMS)
        ev_stream_opened(c->ws, &streams[MOQ_WTQ_MAX_STREAMS], false, c);
    CHECK(retained == MOQ_WTQ_MAX_STREAMS);
    CHECK(abort_calls == before_abort);
    CHECK(!moq_wtquic_conn_is_fatal(c));
    for (size_t i = 0; i < MOQ_WTQ_MAX_STREAMS; i++) CHECK(c->streams[i].held);
    ev_closed(c->ws, 0, NULL, 0, true, c);
    for (size_t i = 0; i < MOQ_WTQ_MAX_STREAMS; i++) CHECK(!c->streams[i].held);
    session->rx_cap = saved;
    moq_wtquic_conn_destroy(c);
    CHECK(live == 0);
    moq_session_destroy(session);

    CHECK(moq_session_create(&scfg, 0, &session) == MOQ_OK);
    cfg.session = session;
    CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_OK);
    if (c) {
        ev_stream_opened(c->ws, &streams[0], false, c);
        ev_stream_data(c->ws, &streams[0], bytes, 65536, true, c);
        CHECK(moq_wtquic_conn_is_fatal(c));
        CHECK(!c->streams[0].held);
        moq_wtquic_conn_destroy(c);
    }
    moq_session_destroy(session);
    CHECK(moq_session_create(&scfg, 0, &session) == MOQ_OK);
    cfg.session = session;
    CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_OK);
    if (c) {
        ev_stream_opened((wtq_session_t *)(uintptr_t)2, &streams[0], false, c);
        CHECK(moq_wtquic_conn_is_fatal(c));
        CHECK(c->ws == cfg.wt_session && !c->streams[0].in_use);
        moq_wtquic_conn_destroy(c);
    }
    moq_session_destroy(session);
    CHECK(moq_session_create(&scfg, 0, &session) == MOQ_OK);
    cfg.session = session;
    CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_OK);
    service_conn = c;
    service_probe = publish_one;
    moq_wtquic_conn_service(c);
    CHECK(service_calls == 3 && service_published == 2 && unsafe_service == 0);
    CHECK(retained == 2 && session_refs == 0);
    service_probe = NULL;
    moq_wtquic_conn_destroy(c);
    moq_session_destroy(session);
    CHECK(retained == 0);
    free(bytes);
    CHECK(live == 0);
    printf("qualification failures=%d\n", failures);
    return failures != 0;
}
