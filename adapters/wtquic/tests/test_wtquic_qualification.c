#define main held_schedule_main
#include "test_wtquic_hold_input.c"
#undef main
#include <stdlib.h>

static size_t allocations, live, live_bytes, fail_at;
static void *count_alloc(size_t n, void *ctx)
{
    (void)ctx;
    if (++allocations == fail_at) return NULL;
    void *p = malloc(n);
    if (p) { live++; live_bytes += n; }
    return p;
}
static void count_free(void *p, size_t n, void *ctx)
{ (void)ctx; if (p) { live--; live_bytes -= n; free(p); } }
static void *count_realloc(void *p, size_t old_n, size_t n, void *ctx)
{
    (void)ctx;
    if (++allocations == fail_at) return NULL;
    bool had_pointer = p != NULL;
    void *q = realloc(p, n);
    if (q) {
        if (!had_pointer) live++;
        live_bytes = live_bytes - (had_pointer ? old_n : 0) + n;
    }
    return q;
}

static int budget_refusals(const moq_wtquic_conn_cfg_t *cfg)
{
    int failures = 0;
    wtq_stream_budget_t saved = test_budget;
    static const wtq_result_t errors[] = {
        WTQ_ERR_UNSUPPORTED, WTQ_ERR_INVALID_ARG, WTQ_ERR_NOMEM,
        WTQ_ERR_BACKEND, WTQ_ERR_STATE, WTQ_ERR_CLOSED,
    };
    /* Query failures, invalid sizes, inconsistent count plans and overflow-like
     * inputs all fail before any allocator, provider operation or callback. */
    for (size_t row = 0; row < sizeof(errors) / sizeof(errors[0]) + 15; row++) {
        test_budget = saved;
        test_budget_rc = WTQ_OK;
        if (row < sizeof(errors) / sizeof(errors[0])) test_budget_rc = errors[row];
        else switch (row - sizeof(errors) / sizeof(errors[0])) {
        case 0: test_budget.struct_size = 0; break;
        case 1: test_budget.struct_size = WTQ_STREAM_BUDGET_V1_SIZE - 1; break;
        case 2: test_budget.struct_size = sizeof(test_budget) + 1; break;
        case 3: test_budget.struct_size = UINT32_MAX; break;
        case 4: test_budget.api_capacity = 0; break;
        case 5: test_budget.api_capacity--; break;
        case 6: test_budget.api_capacity++; break;
        case 7: test_budget.api_capacity = UINT32_MAX; break;
        case 8:
            test_budget.peer_unidi_stream_count = 2;
            test_budget.api_capacity = 10;
            break;
        case 9: test_budget.peer_unidi_stream_count = 1025; break;
        case 10:
            test_budget.peer_bidi_stream_count = 0;
            test_budget.api_capacity = 9;
            break;
        case 11: test_budget.peer_bidi_stream_count = 1025; break;
        case 12:
            test_budget.peer_unidi_stream_count = 1024;
            test_budget.peer_bidi_stream_count = 1;
            test_budget.api_capacity = 1026;
            break;
        case 13:
            test_budget.max_local_streams = 0;
            test_budget.api_capacity = 15;
            break;
        case 14: test_budget.max_local_streams = 1025; break;
        }
        moq_wtquic_conn_t *c = NULL;
        allocations = test_budget_calls = 0;
        unsigned before_effects = transport_effects;
        CHECK(moq_wtquic_conn_create(cfg, &c) == MOQ_ERR_UNSUPPORTED);
        CHECK(c == NULL && allocations == 0 && test_budget_calls == 1);
        CHECK(transport_effects == before_effects && retained == 0 && session_refs == 0);
        /* An unexpected success must not leak while the oracle continues. */
        moq_wtquic_conn_destroy(c);
        CHECK(live == 0 && live_bytes == 0 && test_budget_bad_size == 0);
    }
    test_budget = saved;
    test_budget_rc = WTQ_OK;
    return failures;
}

static int budget_deferred_admission(const moq_wtquic_conn_cfg_t *cfg)
{
    int failures = 0;
    wtq_stream_budget_t saved = test_budget;
    /* Peer publication can wait for the current callback to return even
     * though its reservation is already backed. */
    test_budget.peer_uni_resident = 1;
    test_budget.peer_high_water = 1;
    test_budget.peer_wt_waiting = 1;
    moq_wtquic_conn_t *c = NULL;
    allocations = test_budget_calls = 0;
    unsigned before_effects = transport_effects;
    CHECK(moq_wtquic_conn_create(cfg, &c) == MOQ_OK);
    CHECK(c != NULL && allocations != 0 && test_budget_calls == 1);
    CHECK(transport_effects == before_effects && retained == 0 && session_refs == 0);
    moq_wtquic_conn_destroy(c);
    CHECK(live == 0 && live_bytes == 0 && test_budget_bad_size == 0);
    test_budget = saved;
    return failures;
}

static struct wtq_stream *budget_local_next;
static wtq_stream_t *budget_open(bool bidi)
{
    struct wtq_stream *st = budget_local_next++;
    st->bidi = bidi;
    return st;
}

static int budget_capacity(const moq_wtquic_conn_cfg_t *cfg, uint32_t capacity)
{
    int failures = 0;
    wtq_stream_budget_t saved = test_budget;
    uint32_t uni = capacity == 5 ? 3 : 8;
    uint32_t bidi = capacity == 5 ? 1 : 7;
    uint32_t peers = uni + bidi;
    test_budget.peer_unidi_stream_count = uni;
    test_budget.peer_bidi_stream_count = bidi;
    test_budget.max_local_streams = capacity - peers;
    test_budget.api_capacity = capacity;
    test_budget.engine_capacity = capacity;
    test_budget.native_capacity = capacity + 3;
    struct wtq_stream *streams = calloc(capacity + 1, sizeof(*streams));
    uint64_t *keys = calloc(capacity, sizeof(*keys));
    struct moq_wtq_stream **addresses = calloc(capacity, sizeof(*addresses));
    moq_wtquic_conn_t *c = NULL;
    allocations = test_budget_calls = 0;
    unsigned before_effects = transport_effects;
    CHECK(moq_wtquic_conn_create(cfg, &c) == MOQ_OK);
    CHECK(test_budget_calls == 1 && transport_effects == before_effects);
    if (!c || !streams || !keys || !addresses) { failures++; goto done; }
    size_t allocated = allocations;
    unsigned before_abort = abort_calls;
    budget_local_next = streams + peers;
    open_probe = budget_open;
    /* Fill the declared peer and local budgets together. Every API handle
     * fits, with a stable address and retained reference; no callback growth. */
    for (uint32_t i = 0; i < capacity; i++) {
        if (i < peers) {
            streams[i].bidi = i >= uni;
            ev_stream_opened(c->ws, &streams[i], streams[i].bidi, c);
        } else CHECK(ep_open_uni(c, &keys[i]) == MOQ_TRANSPORT_OK);
        struct moq_wtq_stream *ms = stream_find_by_handle(c, &streams[i]);
        CHECK(ms != NULL);
        if (ms) { keys[i] = ms->id; addresses[i] = ms; }
    }
    CHECK(retained == capacity && abort_calls == before_abort);
    CHECK(allocations == allocated);
    uint64_t extra_key = UINT64_MAX;
    before_effects = transport_effects;
    CHECK(ep_open_uni(c, &extra_key) == MOQ_TRANSPORT_WOULD_BLOCK);
    CHECK(extra_key == UINT64_MAX && transport_effects == before_effects);

    c->in_service = true;
    for (uint32_t i = 0; i < capacity; i++) {
        CHECK(stream_find(c, keys[i]) == addresses[i]);
        if (i >= uni) {
            ev_stream_stop(c->ws, &streams[i], i, c);
            ev_stream_stop(c->ws, &streams[i], i, c); /* duplicate outcome */
        }
        ev_stream_closed(c->ws, &streams[i], c);
        ev_stream_closed(c->ws, &streams[i], c);
        struct moq_wtq_stream *ms = stream_find(c, keys[i]);
        CHECK(ms && ms->st == NULL && ms->lease == &streams[i] &&
              ms->pending_events == (i >= uni ? 2u : 1u));
    }
    CHECK(!c->term_overflow && c->term_count == 2 * (size_t)capacity - uni);
    CHECK(retained == capacity && allocations == allocated);
    size_t at = 0;
    for (uint32_t i = 0; i < capacity; i++) {
        if (i >= uni) {
            CHECK(at < c->term_count);
            if (at < c->term_count) {
                CHECK(c->term_events[at].kind == STREAM_EVENT_STOP);
                CHECK(c->term_events[at].key == keys[i]);
                CHECK(c->term_events[at].code == i);
            }
            at++;
        }
        CHECK(at < c->term_count);
        if (at < c->term_count) {
            CHECK(c->term_events[at].kind == STREAM_EVENT_CLOSED);
            CHECK(c->term_events[at].key == keys[i]);
        }
        at++;
    }
    conn_feed_terminals(c);
    c->in_service = false;
    CHECK(c->term_count == 0 && !c->term_overflow && retained == 0);
    for (uint32_t i = 0; i < capacity; i++) CHECK(stream_find(c, keys[i]) == NULL);
    ev_stream_opened(c->ws, &streams[capacity], false, c);
    struct moq_wtq_stream *replacement = stream_find_by_handle(c, &streams[capacity]);
    CHECK(replacement && replacement->id > keys[capacity - 1]);
    CHECK(retained == 1 && allocations == allocated);
    CHECK(!moq_wtquic_conn_is_fatal(c));
done:
    open_probe = NULL;
    moq_wtquic_conn_destroy(c);
    free(addresses);
    free(keys);
    free(streams);
    test_budget = saved;
    CHECK(live == 0 && live_bytes == 0 && retained == 0 && test_budget_bad_size == 0);
    printf("budget capacity=%u failures=%d\n", capacity, failures);
    return failures;
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
    failures += budget_refusals(&cfg);
    failures += budget_deferred_admission(&cfg);
    failures += budget_capacity(&cfg, 5);
    failures += budget_capacity(&cfg, 31);
    failures += budget_capacity(&cfg, 65);

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
            allocations = query_calls = test_budget_calls = 0;
            moq_result_t rc = moq_wtquic_conn_create((const void *)p, &c);
            CHECK(rc == (n < sizeof(moq_wtquic_conn_cfg_v0_t) ? MOQ_ERR_INVAL : MOQ_ERR_UNSUPPORTED));
            CHECK(c == NULL && allocations == 0 && query_calls == 0 && test_budget_calls == 0);
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
        allocations = test_budget_calls = 0;
        CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_ERR_UNSUPPORTED);
        CHECK(c == NULL && allocations == 0 && test_budget_calls == 0);
    }
    contract_rc = WTQ_OK; contract_mode = WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED; contract_quantum = 65535;
    bool succeeded = false;
    for (fail_at = 1; fail_at < 32; fail_at++) {
        allocations = 0;
        unsigned before_effects = transport_effects;
        moq_result_t rc = moq_wtquic_conn_create(&cfg, &c);
        CHECK(rc == MOQ_ERR_NOMEM || rc == MOQ_OK);
        if (rc == MOQ_OK) {
            succeeded = true;
            CHECK(c->ops.capabilities & MOQ_TRANSPORT_CAP_HOLD_INPUT);
            moq_wtquic_conn_destroy(c); c = NULL;
        } else CHECK(c == NULL);
        CHECK(live == 0 && live_bytes == 0 && transport_effects == before_effects);
        if (succeeded) break;
    }
    CHECK(succeeded);
    fail_at = 0;
    CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_OK);
    if (!c) { moq_session_destroy(session); return 1; }
    size_t saved = session->rx_cap;
    session->state = MOQ_SESS_ESTABLISHED;
    session->rx_cap = 0;
    struct wtq_stream streams[TEST_STREAM_CAPACITY + 1] = {0};
    uint8_t *bytes = calloc(65536, 1);
    CHECK(bytes != NULL);
    if (!bytes) { moq_wtquic_conn_destroy(c); moq_session_destroy(session); return 1; }
    bytes[0] = 4;
    for (size_t i = 0; i < TEST_STREAM_CAPACITY; i++) {
        ev_stream_opened(c->ws, &streams[i], false, c);
        ev_stream_data(c->ws, &streams[i], bytes, 65535, true, c);
        ev_stream_closed(c->ws, &streams[i], c);
        CHECK(c->streams[i].in_use && c->streams[i].held && c->streams[i].st == NULL);
        CHECK(c->streams[i].held_len == 65535 && c->streams[i].held_fin);
    }
    memset(bytes, 0xa5, 65536);
    size_t held = 0;
    for (size_t i = 0; i < TEST_STREAM_CAPACITY; i++) {
        held += c->streams[i].held_len;
        CHECK(c->streams[i].held_bytes[0] == 4 && c->streams[i].held_bytes[65534] == 0);
    }
    CHECK(held == 1048560);
    unsigned before_abort = abort_calls;
    /* Qualified provider admission counts the retained API slots. Omitting
     * adapter retention makes this deliver the old lossy seventeenth callback.
     * The real provider's corresponding 16-handle schedule is msquic_ops. */
    if (retained < TEST_STREAM_CAPACITY)
        ev_stream_opened(c->ws, &streams[TEST_STREAM_CAPACITY], false, c);
    CHECK(retained == TEST_STREAM_CAPACITY);
    CHECK(abort_calls == before_abort);
    CHECK(!moq_wtquic_conn_is_fatal(c));
    for (size_t i = 0; i < TEST_STREAM_CAPACITY; i++) CHECK(c->streams[i].held);
    ev_closed(c->ws, 0, NULL, 0, true, c);
    for (size_t i = 0; i < TEST_STREAM_CAPACITY; i++) CHECK(!c->streams[i].held);
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
