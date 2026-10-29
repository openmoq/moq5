/*
 * The adapter's stream table follows the provider's queried api_capacity.
 *
 * Real adapter and bridge over a fake WT provider, MsQuic-free. Every stream
 * stays open (no terminal), so each one holds a table entry:
 *   - for each stream plan (U peer uni, B peer bidi, L local) the table
 *     admits exactly U+B+L concurrent peer streams; the next is aborted
 *     (nonfatal table pressure), and local opens fill the same count before
 *     WOULD_BLOCK, which reaches no provider;
 *   - opening up to capacity allocates nothing;
 *   - create() asks the allocator for exactly one block of the independently
 *     computed size, for the default plan and for the largest plan (2048);
 *   - a snapshot whose api_capacity disagrees with U+B+L, or whose counts
 *     are outside the provider's bounds, is refused before any allocation;
 *   - the conn layout arithmetic, checked against explicit size limits so
 *     a 32-bit size_t is exercised on any host: totals past the limit are
 *     refused (table product, terminal-queue product and the base each
 *     on its own), exact fits are accepted at the 64-bit-computed size.
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <moq/session.h>
#include <moq/transport_bridge.h>
#include <moq/wtquic.h>

static unsigned failures;
#define CHECK(expr)                                                       \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr);        \
            failures++;                                                   \
        }                                                                 \
    } while (0)

enum { MAX_CAPACITY = 2048, POOL = MAX_CAPACITY + 2 };

/* -- fake WT provider ------------------------------------------------------ */

struct wtq_stream {
    bool bidi;
    unsigned aborts;
    unsigned refs;
};
struct wtq_session {
    unsigned refs;
    struct wtq_stream streams[POOL];
    unsigned stream_count;
};

static struct wtq_stream *new_stream(wtq_session_t *s, bool bidi)
{
    if (s->stream_count == POOL)
        return NULL;
    struct wtq_stream *st = &s->streams[s->stream_count++];
    memset(st, 0, sizeof(*st));
    st->bidi = bidi;
    return st;
}

wtq_result_t wtq_session_receive_contract(const wtq_session_t *s,
    size_t *quantum, wtq_receive_pause_mode_t *mode)
{
    *quantum = s ? 65535 : 0;
    *mode = s ? WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED : WTQ_RECEIVE_PAUSE_UNSUPPORTED;
    return s ? WTQ_OK : WTQ_ERR_INVALID_ARG;
}
void wtq_session_add_ref(wtq_session_t *s) { s->refs++; }
void wtq_session_release(wtq_session_t *s) { s->refs--; }
/* The provider's stream plan for the next create(). */
static wtq_stream_budget_t plan;
static unsigned budget_calls;
static void set_plan(uint32_t uni, uint32_t bidi, uint32_t local)
{
    memset(&plan, 0, sizeof(plan));
    plan.struct_size = sizeof(plan);
    plan.peer_unidi_stream_count = uni;
    plan.peer_bidi_stream_count = bidi;
    plan.max_local_streams = local;
    plan.api_capacity = uni + bidi + local;
    plan.engine_capacity = plan.api_capacity;
    plan.native_capacity = plan.api_capacity + 3;
}
wtq_result_t wtq_session_stream_budget(const wtq_session_t *s,
                                       wtq_stream_budget_t *out)
{
    budget_calls++;
    if (!s || !out || out->struct_size < WTQ_STREAM_BUDGET_V1_SIZE)
        return WTQ_ERR_INVALID_ARG;
    uint32_t caller_size = out->struct_size;
    size_t n = caller_size < sizeof(*out) ? caller_size : sizeof(*out);
    memcpy(out, &plan, n);
    out->struct_size = caller_size;
    return WTQ_OK;
}
wtq_result_t wtq_session_service_stream_admission(wtq_session_t *s)
{ return s ? WTQ_OK : WTQ_ERR_INVALID_ARG; }
void wtq_stream_add_ref(wtq_stream_t *st) { st->refs++; }
void wtq_stream_release(wtq_stream_t *st) { st->refs--; }
wtq_result_t wtq_session_open_uni(wtq_session_t *s, wtq_stream_t **out)
{
    *out = new_stream(s, false);
    return *out ? WTQ_OK : WTQ_ERR_STREAM_LIMIT;
}
wtq_result_t wtq_session_open_bidi(wtq_session_t *s, wtq_stream_t **out)
{
    *out = new_stream(s, true);
    return *out ? WTQ_OK : WTQ_ERR_STREAM_LIMIT;
}
uint64_t wtq_stream_id(const wtq_stream_t *st)
{ (void)st; return WTQ_STREAM_ID_UNKNOWN; }
bool wtq_stream_is_bidi(const wtq_stream_t *st) { return st->bidi; }
wtq_result_t wtq_stream_reset(wtq_stream_t *st, uint32_t code)
{ (void)st; (void)code; return WTQ_OK; }
wtq_result_t wtq_stream_abort(wtq_stream_t *st, uint32_t code)
{ (void)code; st->aborts++; return WTQ_OK; }
wtq_result_t wtq_stream_stop_sending(wtq_stream_t *st, uint32_t code)
{ (void)st; (void)code; return WTQ_OK; }
wtq_result_t wtq_stream_pause_receive(wtq_stream_t *st)
{ (void)st; return WTQ_OK; }
wtq_result_t wtq_stream_resume_receive(wtq_stream_t *st)
{ (void)st; return WTQ_OK; }
wtq_result_t wtq_stream_send(wtq_stream_t *st, const wtq_span_t *spans,
                             size_t count, uint32_t flags, void *ctx)
{ (void)st; (void)spans; (void)count; (void)flags; (void)ctx; return WTQ_OK; }
void wtq_session_events_init_ex(wtq_session_events_t *e, size_t size)
{
    if (!e || size < sizeof(e->struct_size)) return;
    size_t n = size < sizeof(*e) ? size : sizeof(*e);
    memset(e, 0, n);
    e->struct_size = (uint32_t)n;
}
wtq_send_commit_mode_t wtq_session_send_commit_mode(const wtq_session_t *s)
{ (void)s; return WTQ_SEND_COMMIT_UNSUPPORTED; }
wtq_local_flush_mode_t wtq_session_local_flush_mode(const wtq_session_t *s)
{ (void)s; return WTQ_LOCAL_FLUSH_UNSUPPORTED; }
wtq_result_t wtq_session_local_flush(const wtq_session_t *s)
{ return s ? WTQ_ERR_UNSUPPORTED : WTQ_ERR_INVALID_ARG; }
wtq_result_t wtq_stream_enable_send_commit(wtq_stream_t *st)
{ (void)st; return WTQ_ERR_UNSUPPORTED; }
wtq_result_t wtq_session_close(wtq_session_t *s, uint32_t code,
                               const uint8_t *reason, size_t reason_len)
{ (void)s; (void)code; (void)reason; (void)reason_len; return WTQ_OK; }
wtq_result_t wtq_session_send_datagram(wtq_session_t *s, const wtq_span_t *spans,
                                       size_t count)
{ (void)s; (void)spans; (void)count; return WTQ_ERR_DGRAM_DISABLED; }
size_t wtq_session_datagram_max_size(const wtq_session_t *s) { (void)s; return 0; }

/* The adapter's static endpoint ops, reached through its own ops table. */
#include "../wtquic_adapter.c"

/* -- counting allocator ------------------------------------------------------ */

static unsigned alloc_calls;
static size_t last_request;
static bool refuse_alloc;

static void *count_alloc(size_t n, void *ctx)
{
    (void)ctx;
    alloc_calls++;
    last_request = n;
    return refuse_alloc ? NULL : malloc(n);
}
static void *count_realloc(void *p, size_t old, size_t n, void *ctx)
{
    (void)old; (void)ctx;
    alloc_calls++;
    return refuse_alloc ? NULL : realloc(p, n);
}
static void count_free(void *p, size_t n, void *ctx)
{
    (void)n; (void)ctx;
    free(p);
}
static const moq_alloc_t counting = {
    .alloc = count_alloc, .realloc = count_realloc, .free = count_free,
};

/* -- rows --------------------------------------------------------------------- */

/* Whether a 64-bit byte count is representable as size_t on this host. */
static bool fits_size(uint64_t v)
{
    return v == (uint64_t)(size_t)v;
}

/* Bytes one conn with `cap` entries needs, in 64-bit arithmetic. */
static uint64_t layout_need(uint64_t cap)
{
    return sizeof(moq_wtquic_conn_t) +
           cap * (sizeof(struct moq_wtq_stream) +
                  2u * sizeof(struct moq_wtq_stream_event));
}

static moq_session_t *new_session(void)
{
    moq_session_cfg_t scfg;
    moq_session_t *sess = NULL;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    CHECK(moq_session_create(&scfg, 0, &sess) == MOQ_OK);
    return sess;
}

/* Create a conn over `ws` under the current provider plan. */
static moq_wtquic_conn_t *new_conn(moq_session_t *sess, wtq_session_t *ws)
{
    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = &counting;
    ccfg.session = sess;
    ccfg.wt_session = ws;
    moq_wtquic_conn_t *c = NULL;
    CHECK(moq_wtquic_conn_create(&ccfg, &c) == MOQ_OK);
    return c;
}

/* Present `n` concurrent peer streams; return how many were admitted. */
static unsigned peer_admit(moq_wtquic_conn_t *c, wtq_session_t *ws, unsigned n)
{
    unsigned admitted = 0;
    for (unsigned i = 0; i < n; i++) {
        struct wtq_stream *st = new_stream(ws, false);
        if (st == NULL)
            break;
        moq_wtquic_conn_events()->on_stream_opened(ws, st, false, c);
        if (st->aborts == 0)
            admitted++;
    }
    return admitted;
}

static struct wtq_session ws_storage;

static void peer_row(uint32_t uni, uint32_t bidi, uint32_t local)
{
    set_plan(uni, bidi, local);
    unsigned expect = uni + bidi + local;
    moq_session_t *sess = new_session();
    struct wtq_session *ws = &ws_storage;
    memset(ws, 0, sizeof(*ws));
    moq_wtquic_conn_t *c = new_conn(sess, ws);
    if (c != NULL) {
        CHECK(c->stream_capacity == expect && c->term_capacity == 2u * expect);
        unsigned before = alloc_calls;
        unsigned admitted = peer_admit(c, ws, expect);
        CHECK(alloc_calls == before);      /* no steady-state allocation */
        unsigned past = peer_admit(c, ws, 1);
        printf("plan %u/%u/%u: admitted %u concurrent peer streams, next %s\n",
               uni, bidi, local, admitted, past ? "admitted" : "aborted");
        CHECK(admitted == expect);
        CHECK(past == 0);                  /* table pressure: aborted */
        CHECK(ws->streams[ws->stream_count - 1].aborts == 1);
        CHECK(!moq_wtquic_conn_is_fatal(c)); /* nonfatal */
        moq_wtquic_conn_destroy(c);
    }
    for (unsigned i = 0; i < ws->stream_count; i++)
        CHECK(ws->streams[i].refs == 0);
    moq_session_destroy(sess);
}

static void local_row(uint32_t uni, uint32_t bidi, uint32_t local)
{
    set_plan(uni, bidi, local);
    unsigned expect = uni + bidi + local;
    moq_session_t *sess = new_session();
    struct wtq_session *ws = &ws_storage;
    memset(ws, 0, sizeof(*ws));
    moq_wtquic_conn_t *c = new_conn(sess, ws);
    if (c != NULL) {
        unsigned before = alloc_calls;
        unsigned ok = 0;
        uint64_t id = 0;
        for (unsigned i = 0; i < expect; i++)
            if (c->ops.open_uni(c, &id) == MOQ_TRANSPORT_OK)
                ok++;
        CHECK(alloc_calls == before);
        CHECK(ok == expect);
        CHECK(c->ops.open_uni(c, &id) == MOQ_TRANSPORT_WOULD_BLOCK);
        CHECK(ws->stream_count == expect); /* the refused open reached no provider */
        moq_wtquic_conn_destroy(c);
    }
    moq_session_destroy(sess);
}

/* create() requests one block of the independently computed size; refused
 * here (NOMEM), so only the request is observed. */
static void request_row(uint32_t uni, uint32_t bidi, uint32_t local)
{
    set_plan(uni, bidi, local);
    moq_session_t *sess = new_session();
    struct wtq_session ws = {0};
    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = &counting;
    ccfg.session = sess;
    ccfg.wt_session = &ws;
    uint64_t need = layout_need((uint64_t)uni + bidi + local);
    unsigned before = alloc_calls, queried = budget_calls;
    refuse_alloc = true;
    last_request = 0;
    moq_wtquic_conn_t *c = (moq_wtquic_conn_t *)0x1;
    CHECK(fits_size(need));
    CHECK(moq_wtquic_conn_create(&ccfg, &c) == MOQ_ERR_NOMEM);
    CHECK(c == NULL);
    CHECK(alloc_calls == before + 1);
    CHECK(budget_calls == queried + 1);
    CHECK((uint64_t)last_request == need);
    printf("plan %u/%u/%u: one request of %llu bytes\n", uni, bidi, local,
           (unsigned long long)need);
    refuse_alloc = false;
    moq_session_destroy(sess);
}

/* An incoherent or out-of-bounds snapshot: refused before any allocation. */
static void refusal_row(void)
{
    moq_session_t *sess = new_session();
    struct wtq_session ws = {0};
    moq_wtquic_conn_cfg_t ccfg;
    moq_wtquic_conn_cfg_init_sized(&ccfg, sizeof(ccfg));
    ccfg.alloc = &counting;
    ccfg.session = sess;
    ccfg.wt_session = &ws;
    for (int row = 0; row < 6; row++) {
        set_plan(8, 7, 16);
        switch (row) {
        case 0: plan.api_capacity = 30; break;           /* fewer than U+B+L */
        case 1: plan.api_capacity = 32; break;           /* more than U+B+L */
        case 2: set_plan(8, 7, 1025); break;             /* L past 1024 */
        case 3: set_plan(1024, 1, 16); break;            /* U+B past 1024 */
        case 4: set_plan(2, 7, 16); break;               /* U below 3 */
        case 5: set_plan(1024, 1024, 1024); break;       /* every bound */
        }
        unsigned before = alloc_calls, queried = budget_calls;
        moq_wtquic_conn_t *c = (moq_wtquic_conn_t *)0x1;
        CHECK(moq_wtquic_conn_create(&ccfg, &c) == MOQ_ERR_UNSUPPORTED);
        CHECK(c == NULL);
        CHECK(alloc_calls == before);
        CHECK(budget_calls == queried + 1);
    }
    moq_session_destroy(sess);
}

/* The conn layout arithmetic under explicit limits. */
static void layout_row(void)
{
    const uint64_t base = sizeof(moq_wtquic_conn_t);
    const uint64_t entry = sizeof(struct moq_wtq_stream);
    const uint64_t per = entry + 2u * sizeof(struct moq_wtq_stream_event);
    size_t so = 7, eo = 7, total = 7;

    /* The hold buffers of the largest capacity alone exceed a 32-bit
     * size_t, so a 32-bit limit must refuse it, outputs untouched. */
    CHECK((uint64_t)65551 * MOQ_WTQ_RECEIVE_QUANTUM > UINT32_MAX);
    CHECK(!conn_layout(65551, UINT32_MAX, &so, &eo, &total));
    CHECK(so == 7 && eo == 7 && total == 7);

    /* The largest capacity a 32-bit size_t can hold fits; one more does not. */
    uint64_t max32 = (UINT32_MAX - base) / per;
    CHECK(conn_layout((size_t)max32, UINT32_MAX, &so, &eo, &total));
    CHECK((uint64_t)total == layout_need(max32) && total <= UINT32_MAX);
    CHECK(!conn_layout((size_t)max32 + 1, UINT32_MAX, &so, &eo, &total));

    static const uint64_t caps[] = { 1, 5, 31, 2048, 65551 };
    for (size_t i = 0; i < sizeof(caps) / sizeof(caps[0]); i++) {
        uint64_t cap = caps[i], need = layout_need(cap);
        if (!fits_size(need))
            continue;
        /* exact fit: offsets and total as computed independently */
        CHECK(conn_layout((size_t)cap, (size_t)need, &so, &eo, &total));
        CHECK(so == base && (uint64_t)eo == base + cap * entry &&
              (uint64_t)total == need);
        /* one byte short: the terminal-queue term no longer fits */
        CHECK(!conn_layout((size_t)cap, (size_t)need - 1, &so, &eo, &total));
        /* the stream table itself no longer fits */
        CHECK(!conn_layout((size_t)cap, (size_t)(base + cap * entry - 1),
                           &so, &eo, &total));
    }
    /* the conn alone */
    CHECK(conn_layout(0, (size_t)base, &so, &eo, &total) && total == base);
    CHECK(!conn_layout(0, (size_t)base - 1, &so, &eo, &total));
    /* a full-width limit refuses a product past it */
    CHECK(!conn_layout(SIZE_MAX / 2, SIZE_MAX, &so, &eo, &total));
}

int main(void)
{
    peer_row(8, 7, 16);    /* the provider default: 31 */
    peer_row(3, 1, 1);     /* the smallest plan: 5 */
    peer_row(9, 8, 48);    /* 65 */
    local_row(8, 7, 16);
    local_row(3, 1, 1);
    request_row(8, 7, 16);
    request_row(1023, 1, 1024); /* the largest plan: 2048 */
    refusal_row();
    layout_row();
    if (failures) {
        printf("%u check(s) failed\n", failures);
        return 1;
    }
    printf("wtquic_capacity: OK\n");
    return 0;
}
