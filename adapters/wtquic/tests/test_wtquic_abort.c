/* Classified stream aborts (wtq_stream_abort_t) through the adapter, over a
 * fake WebTransport provider and the real bridge and sessions.
 *
 * The MoQ control stream must stay open for the session's lifetime
 * (draft-ietf-moq-transport-16 section 3.3, -18 section 3.3, -22 section
 * 6.3), while every stream is aborted when the WebTransport session
 * terminates (draft-ietf-webtrans-http3-16 section 6). Rows:
 *
 *   1. attach is refused, before anything is allocated, installed, opened or
 *      sent, by a library whose event table does not reach on_stream_abort,
 *      and by a session that does not state each of RESET_CODE, STOP_EVENT
 *      and STOP_CODE. A backend that states none of them (Network.framework
 *      today) is refused the same way;
 *   2. on the MoQ control channel of drafts 16, 18 and 22 -- the draft-16
 *      control bidi; the peer's control uni for RESET and the client's own
 *      for STOP on 18/22 -- an APPLICATION code (0 or not) or a PROTOCOL code
 *      ends the control stream: an open session closes with 0x3, a close the
 *      session decided first stands. SESSION_GONE and UNKNOWN decide
 *      nothing; the WebTransport session terminal does;
 *   3. the same aborts on an ordinary request stream (drafts 18 and 22) do
 *      not end the session;
 *   5. a caller that forwards only the reset and stop events (no on_stream_abort) keeps their
 *      handling.
 * (Row 4, forwarding through the MsQuic managed facade, is in
 * test_wtquic_msquic_managed_internal.c.)
 *
 * Every session is established through a real setup exchange first. One
 * thread, no network. */
#include <moq/transport_bridge.h>
#include <moq/session.h>
#include <wtquic/wtquic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "wtquic_stream_budget_fake.h"

static int failures;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- the fake provider ------------------------------------------------------ */

struct wtq_stream {
    uint64_t native_id;
    bool bidi;
    bool local;
    unsigned refs;
    uint8_t out[512];       /* what the adapter sent on it */
    size_t out_len;
};

static struct wtq_stream g_streams[32];
static int g_stream_n;
static unsigned g_receive_contract_calls, g_sends, g_session_closes;
static uint32_t g_close_code;
static char g_close_reason[32];
static void *g_send_ctx[64];
static int g_send_ctx_n;
/* The extent wtq_session_events_init_ex stamps; 0 is this header's full
 * size. A smaller value is an older library. */
static size_t g_events_extent;

/* The local streams this fixture's client opens: its control stream (the
 * draft-16 bidi, or the draft-18/22 uni) and, in the request rows, one
 * request bidi. The provider declares exactly that many and refuses more. */
enum { FIXTURE_LOCAL_STREAMS = 2 };
static unsigned g_local_opens, g_local_refused;

static void fixture_budget(void)
{
    test_budget.max_local_streams = FIXTURE_LOCAL_STREAMS;
    test_budget.api_capacity = test_budget.peer_unidi_stream_count +
                               test_budget.peer_bidi_stream_count +
                               FIXTURE_LOCAL_STREAMS;
    test_budget.engine_capacity = test_budget.api_capacity;
    test_budget.native_capacity = test_budget.api_capacity + 3;
}

static bool local_open_allowed(void)
{
    if (g_local_opens >= test_budget.max_local_streams) {
        g_local_refused++;
        return false;
    }
    g_local_opens++;
    return true;
}

static struct wtq_stream *new_stream(bool bidi, bool local, uint64_t native)
{
    struct wtq_stream *st = &g_streams[g_stream_n++];
    memset(st, 0, sizeof(*st));
    st->bidi = bidi;
    st->local = local;
    st->native_id = native;
    return st;
}

wtq_result_t wtq_session_receive_contract(const wtq_session_t *s,
    size_t *quantum, wtq_receive_pause_mode_t *mode)
{
    g_receive_contract_calls++;
    *quantum = s ? 65535 : 0;
    *mode = s ? WTQ_RECEIVE_PAUSE_FLOW_CONTROLLED : WTQ_RECEIVE_PAUSE_UNSUPPORTED;
    return s ? WTQ_OK : WTQ_ERR_INVALID_ARG;
}
void wtq_session_add_ref(wtq_session_t *s) { (void)s; }
void wtq_session_release(wtq_session_t *s) { (void)s; }
wtq_result_t wtq_session_service_stream_admission(wtq_session_t *s)
{ return s ? WTQ_OK : WTQ_ERR_INVALID_ARG; }
void wtq_stream_add_ref(wtq_stream_t *st) { st->refs++; }
void wtq_stream_release(wtq_stream_t *st) { st->refs--; }
wtq_result_t wtq_session_open_uni(wtq_session_t *s, wtq_stream_t **out)
{
    (void)s;
    if (!local_open_allowed()) return WTQ_ERR_WOULD_BLOCK;
    *out = new_stream(false, true, WTQ_STREAM_ID_UNKNOWN);
    return WTQ_OK;
}
wtq_result_t wtq_session_open_bidi(wtq_session_t *s, wtq_stream_t **out)
{
    (void)s;
    if (!local_open_allowed()) return WTQ_ERR_WOULD_BLOCK;
    *out = new_stream(true, true, WTQ_STREAM_ID_UNKNOWN);
    return WTQ_OK;
}
uint64_t wtq_stream_id(const wtq_stream_t *st)
{ return st ? st->native_id : WTQ_STREAM_ID_UNKNOWN; }
wtq_result_t wtq_stream_abort(wtq_stream_t *st, uint32_t code)
{ (void)st; (void)code; return WTQ_OK; }
bool wtq_stream_is_bidi(const wtq_stream_t *st) { return st && st->bidi; }
wtq_result_t wtq_stream_reset(wtq_stream_t *st, uint32_t c) { (void)st; (void)c; return WTQ_OK; }
wtq_result_t wtq_stream_stop_sending(wtq_stream_t *st, uint32_t c) { (void)st; (void)c; return WTQ_OK; }
wtq_result_t wtq_stream_send(wtq_stream_t *st, const wtq_span_t *sp, size_t n,
                             uint32_t f, void *ctx)
{
    (void)f;
    g_sends++;
    for (size_t i = 0; i < n; i++) {
        if (sp[i].len > 0 && st->out_len + sp[i].len <= sizeof(st->out)) {
            memcpy(st->out + st->out_len, sp[i].data, sp[i].len);
            st->out_len += sp[i].len;
        }
    }
    if (g_send_ctx_n < 64) g_send_ctx[g_send_ctx_n++] = ctx;
    return WTQ_OK;
}
wtq_result_t wtq_stream_pause_receive(wtq_stream_t *st) { (void)st; return WTQ_OK; }
wtq_result_t wtq_stream_resume_receive(wtq_stream_t *st) { (void)st; return WTQ_OK; }
void wtq_session_events_init_ex(wtq_session_events_t *e, size_t size)
{
    if (!e || size < sizeof(e->struct_size)) return;
    size_t n = size < sizeof(*e) ? size : sizeof(*e);
    if (g_events_extent != 0 && g_events_extent < n) n = g_events_extent;
    memset(e, 0, n);
    e->struct_size = (uint32_t)n;
}
#include "wtquic_abort_fake.h"
wtq_send_commit_mode_t wtq_session_send_commit_mode(const wtq_session_t *s)
{ (void)s; return WTQ_SEND_COMMIT_UNSUPPORTED; }
wtq_local_flush_mode_t wtq_session_local_flush_mode(const wtq_session_t *s)
{ (void)s; return WTQ_LOCAL_FLUSH_UNSUPPORTED; }
wtq_result_t wtq_session_local_flush(const wtq_session_t *s)
{ return s ? WTQ_ERR_UNSUPPORTED : WTQ_ERR_INVALID_ARG; }
wtq_result_t wtq_stream_enable_send_commit(wtq_stream_t *st)
{ (void)st; return WTQ_ERR_UNSUPPORTED; }
wtq_result_t wtq_session_close(wtq_session_t *s, uint32_t c, const uint8_t *r,
                               size_t rl)
{
    (void)s;
    g_session_closes++;
    g_close_code = c;
    size_t n = rl < sizeof(g_close_reason) - 1 ? rl : sizeof(g_close_reason) - 1;
    if (r && n) memcpy(g_close_reason, r, n);
    g_close_reason[n] = '\0';
    return WTQ_OK;
}
wtq_result_t wtq_session_send_datagram(wtq_session_t *s, const wtq_span_t *sp,
                                       size_t n)
{ (void)s; (void)sp; (void)n; return WTQ_OK; }
size_t wtq_session_datagram_max_size(const wtq_session_t *s)
{ (void)s; return 1200; }

#include "../wtquic_adapter.c"

static wtq_session_t *const WS = (wtq_session_t *)(void *)0x1;

/* A counting allocator: a refused attach must not touch it. */
static unsigned g_allocs;
static void *count_alloc(size_t n, void *ctx)
{ (void)ctx; g_allocs++; return malloc(n); }
static void *count_realloc(void *p, size_t o, size_t n, void *ctx)
{ (void)o; (void)ctx; g_allocs++; return realloc(p, n); }
static void count_free(void *p, size_t n, void *ctx) { (void)n; (void)ctx; free(p); }
static const moq_alloc_t g_count_alloc = {
    NULL, count_alloc, count_realloc, count_free
};

/* -- rigs --------------------------------------------------------------------- */

/* The binding a WebTransport-over-HTTP/3 session carries when its profile
 * needs one: every fact known and supported, and the CONNECT request's
 * target on both sides -- the client names it, the server read it from the
 * request. */
static moq_result_t bind_webtransport(moq_session_t *s)
{
    if (moq_session_binding_requirements(s) == 0) return MOQ_OK;
    moq_transport_binding_t b;
    moq_transport_binding_init(&b);
    b.kind = MOQ_BINDING_WEBTRANSPORT_H3;
    b.facts_known = b.facts_supported = MOQ_BINDING_FACTS_ALL;
    b.target_present = MOQ_BINDING_TARGET_COMPLETE;
    b.authority = MOQ_BYTES_LITERAL("test.invalid");
    b.path_query = MOQ_BYTES_LITERAL("/moq");
    return moq_session_bind_transport(s, &b, sizeof(b));
}

static moq_session_t *new_session(moq_perspective_t persp, moq_version_t v)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(), persp);
    cfg.version = v;
    cfg.send_request_capacity = true;
    cfg.initial_request_capacity = 10;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) != MOQ_OK) return NULL;
    moq_result_t brc = bind_webtransport(s);
    CHECK(brc == MOQ_OK);
    if (brc != MOQ_OK) {
        moq_session_destroy(s);
        return NULL;
    }
    return s;
}

static void complete_sends(moq_wtquic_conn_t *c)
{
    while (g_send_ctx_n > 0)
        moq_wtquic_conn_events()->on_send_complete(
            WS, g_send_ctx[--g_send_ctx_n], false, c);
}

typedef struct {
    moq_version_t v;
    moq_session_t *s;
    moq_wtquic_conn_t *c;
    wtq_stream_t *ctrl_local;   /* the client's own control stream */
    wtq_stream_t *ctrl_peer;    /* drafts 18/22: the peer's control uni */
} rig_t;

static int closed_events(moq_session_t *s, uint64_t *code, char *reason,
                         size_t cap)
{
    int n = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) {
            n++;
            if (code) *code = ev.u.closed.code;
            if (reason && cap) {
                size_t l = ev.u.closed.reason.len < cap - 1
                         ? ev.u.closed.reason.len : cap - 1;
                if (l) memcpy(reason, ev.u.closed.reason.data, l);
                reason[l] = '\0';
            }
        }
        moq_event_cleanup(&ev);
    }
    return n;
}

/* The server's control bytes for the client of `v`, from a real server
 * session: its answer to the client's setup on draft-16, its own control
 * stream otherwise. */
static size_t server_control_bytes(moq_version_t v, const uint8_t *client,
                                   size_t client_len, uint8_t *out, size_t cap)
{
    size_t n = 0;
    moq_session_t *srv = new_session(MOQ_PERSPECTIVE_SERVER, v);
    if (!srv) return 0;
    if (v == MOQ_VERSION_DRAFT_16) {
        if (moq_session_on_control_bytes(srv, client, client_len, 0) < 0)
            goto out;
    } else if (moq_session_start(srv, 0) != MOQ_OK) {
        goto out;
    }
    moq_action_t a;
    while (moq_session_poll_actions(srv, &a, 1) == 1) {
        const uint8_t *d = NULL;
        size_t l = 0;
        if (a.kind == MOQ_ACTION_SEND_CONTROL) {
            d = a.u.send_control.data; l = a.u.send_control.len;
        } else if (a.kind == MOQ_ACTION_OPEN_UNI_CONTROL) {
            d = a.u.open_uni_control.data; l = a.u.open_uni_control.len;
        } else if (a.kind == MOQ_ACTION_SEND_UNI_CONTROL) {
            d = a.u.send_uni_control.data; l = a.u.send_uni_control.len;
        }
        if (d && l && n + l <= cap) { memcpy(out + n, d, l); n += l; }
        moq_action_cleanup(&a);
    }
out:
    moq_session_destroy(srv);
    return n;
}

static bool rig_up(rig_t *r, moq_version_t v)
{
    memset(r, 0, sizeof(*r));
    r->v = v;
    g_stream_n = 0; g_sends = 0; g_session_closes = 0; g_send_ctx_n = 0;
    g_close_code = UINT32_MAX; g_close_reason[0] = '\0';
    g_events_extent = 0;
    g_local_opens = 0; g_local_refused = 0;
    test_abort_observability = WTQ_ABORT_OBSERVES_RESET_CODE |
                               WTQ_ABORT_OBSERVES_STOP_EVENT |
                               WTQ_ABORT_OBSERVES_STOP_CODE;
    r->s = new_session(MOQ_PERSPECTIVE_CLIENT, v);
    if (!r->s) return false;
    /* draft-22 needs the binding; the others take none */
    CHECK((moq_session_binding_requirements(r->s) != 0) ==
          (v == MOQ_VERSION_DRAFT_22));
    moq_wtquic_conn_cfg_t cfg;
    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = moq_alloc_default();
    cfg.session = r->s;
    cfg.wt_session = WS;
    if (moq_wtquic_conn_create(&cfg, &r->c) != MOQ_OK) return false;
    if (moq_session_start(r->s, 0) != MOQ_OK) return false;
    moq_wtquic_conn_service(r->c);
    if (g_stream_n < 1) return false;
    r->ctrl_local = &g_streams[0];       /* the first stream the client opens */
    complete_sends(r->c);

    uint8_t bytes[512];
    size_t n = server_control_bytes(v, r->ctrl_local->out,
                                    r->ctrl_local->out_len, bytes, sizeof(bytes));
    if (n == 0) return false;
    if (v == MOQ_VERSION_DRAFT_16) {
        moq_wtquic_conn_events()->on_stream_data(WS, r->ctrl_local, bytes, n,
                                                 false, r->c);
    } else {
        r->ctrl_peer = new_stream(false, false, 3);
        moq_wtquic_conn_events()->on_stream_opened(WS, r->ctrl_peer, false, r->c);
        moq_wtquic_conn_events()->on_stream_data(WS, r->ctrl_peer, bytes, n,
                                                 false, r->c);
    }
    moq_wtquic_conn_service(r->c);
    complete_sends(r->c);
    (void)closed_events(r->s, NULL, NULL, 0);
    CHECK(g_local_refused == 0);
    return moq_session_state(r->s) == MOQ_SESS_ESTABLISHED &&
           !moq_wtquic_conn_is_fatal(r->c) && !moq_wtquic_conn_is_closed(r->c);
}

static void rig_down(rig_t *r)
{
    if (r->c) { complete_sends(r->c); moq_wtquic_conn_destroy(r->c); }
    if (r->s) moq_session_destroy(r->s);
    memset(r, 0, sizeof(*r));
}

static const char *draft_name(moq_version_t v)
{
    return v == MOQ_VERSION_DRAFT_22 ? "d22"
         : v == MOQ_VERSION_DRAFT_18 ? "d18" : "d16";
}

static void deliver_abort(rig_t *r, wtq_stream_t *st, uint32_t direction,
                          uint32_t origin, uint32_t app_code, uint64_t wire)
{
    wtq_stream_abort_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.struct_size = (uint32_t)sizeof(rec);
    rec.direction = direction;
    rec.origin = origin;
    rec.app_code = app_code;
    rec.wire_code = wire;
    moq_wtquic_conn_events()->on_stream_abort(WS, st, &rec, r->c);
}

/* -- 1. refusal before anything is committed ---------------------------------- */

static void t_refused(const char *what, size_t extent, uint32_t facts)
{
    int before = failures;
    moq_session_t *s = new_session(MOQ_PERSPECTIVE_CLIENT, MOQ_VERSION_DRAFT_16);
    CHECK(s != NULL);
    g_stream_n = 0; g_sends = 0; g_session_closes = 0; g_allocs = 0;
    g_receive_contract_calls = 0; test_budget_calls = 0;
    g_events_extent = extent;
    test_abort_observability = facts;
    moq_wtquic_conn_cfg_t cfg;
    moq_wtquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &g_count_alloc;
    cfg.session = s;
    cfg.wt_session = WS;
    moq_wtquic_conn_t *c = (moq_wtquic_conn_t *)(void *)0x1;
    CHECK(moq_wtquic_conn_create(&cfg, &c) == MOQ_ERR_UNSUPPORTED);
    CHECK(c == NULL);
    CHECK(g_allocs == 0);
    CHECK(g_receive_contract_calls == 0);   /* nothing asked of it further */
    CHECK(test_budget_calls == 0);
    CHECK(g_stream_n == 0);
    CHECK(g_sends == 0);
    CHECK(g_session_closes == 0);
    g_events_extent = 0;
    if (s) moq_session_destroy(s);
    if (failures == before) printf("PASS: attach refused: %s\n", what);
}

static void t_refusals(void)
{
    const uint32_t all = WTQ_ABORT_OBSERVES_RESET_CODE |
                         WTQ_ABORT_OBSERVES_STOP_EVENT |
                         WTQ_ABORT_OBSERVES_STOP_CODE;
    t_refused("event table ends before on_stream_abort",
              offsetof(wtq_session_events_t, on_stream_abort), all);
    t_refused("no RESET_CODE", 0, all & ~WTQ_ABORT_OBSERVES_RESET_CODE);
    t_refused("no STOP_EVENT (nor STOP_CODE, which implies it)", 0,
              WTQ_ABORT_OBSERVES_RESET_CODE);
    t_refused("no STOP_CODE", 0, all & ~WTQ_ABORT_OBSERVES_STOP_CODE);
    /* Network.framework states none of the three today: refused, so its
     * control-stream STOP cases are never claimed. */
    t_refused("no facts at all (Network.framework today)", 0, 0);

    /* Control: every fact stated, a library that reaches the member. */
    rig_t r;
    CHECK(rig_up(&r, MOQ_VERSION_DRAFT_16));
    rig_down(&r);
}

/* -- 2. routing on the MoQ control channel ------------------------------------ */

static const struct {
    const char *name;
    uint32_t origin;
    uint32_t app;
    uint64_t wire;
    bool decides;
} k_origins[] = {
    { "APPLICATION 0",    WTQ_STREAM_ABORT_ORIGIN_APPLICATION, 0,    0x52e4a40fa8dbull, true },
    { "APPLICATION 0x55", WTQ_STREAM_ABORT_ORIGIN_APPLICATION, 0x55, 0x52e4a40fa932ull, true },
    { "PROTOCOL",         WTQ_STREAM_ABORT_ORIGIN_PROTOCOL,    0,    0x10cull,          true },
    { "SESSION_GONE",     WTQ_STREAM_ABORT_ORIGIN_SESSION_GONE,0,    0x170d7b68ull,     false },
    { "UNKNOWN",          WTQ_STREAM_ABORT_ORIGIN_UNKNOWN,     0,    0,                 false },
};
#define N_ORIGINS (sizeof(k_origins) / sizeof(k_origins[0]))

static void t_control_abort(moq_version_t v, uint32_t direction, size_t k,
                            bool closed_first)
{
    int before = failures;
    rig_t r;
    CHECK(rig_up(&r, v));
    if (r.c) {
        /* RESET ends the direction the peer sends control on, STOP the one
         * the client sends it on */
        wtq_stream_t *st = v == MOQ_VERSION_DRAFT_16 ? r.ctrl_local
                         : direction == WTQ_STREAM_ABORT_RESET ? r.ctrl_peer
                         : r.ctrl_local;
        struct moq_wtq_stream *ms = stream_find_by_handle(r.c, st);
        CHECK(ms != NULL);
        CHECK(ms && ((r.c->ctrl_latched && ms->id == r.c->ctrl_id) ||
                     moq_transport_bridge_stream_is_control(r.c->bridge, ms->id)));
        if (closed_first)
            CHECK(moq_session_close(r.s, 0x2, "first", 0) == MOQ_OK);
        const bool decides = k_origins[k].decides;
        const uint64_t expect = closed_first ? 0x2u : 0x3u;

        CHECK(moq_wtquic_conn_events()->on_stream_abort != NULL);
        if (moq_wtquic_conn_events()->on_stream_abort) {
            for (int i = 0; i < 2; i++)
                deliver_abort(&r, st, direction, k_origins[k].origin,
                              k_origins[k].app, k_origins[k].wire);
        }
        moq_wtquic_conn_service(r.c);
        complete_sends(r.c);
        uint64_t code = UINT64_MAX;
        char reason[32] = "";
        int events = closed_events(r.s, &code, reason, sizeof(reason));
        if (decides || closed_first) {
            CHECK(events == 1);
            CHECK(code == expect);
            CHECK(strcmp(reason, closed_first ? "first" : "") == 0);
            CHECK(g_session_closes == 1);
            CHECK(g_close_code == expect);
            CHECK(moq_wtquic_conn_is_closed(r.c));
            CHECK(!moq_wtquic_conn_is_fatal(r.c));
        } else {
            /* nothing decided; the WebTransport session terminal does */
            CHECK(events == 0);
            CHECK(g_session_closes == 0);
            CHECK(moq_session_state(r.s) == MOQ_SESS_ESTABLISHED);
            moq_wtquic_conn_events()->on_closed(WS, 7, NULL, 0, true, r.c);
            events = closed_events(r.s, &code, NULL, 0);
            CHECK(events == 1);
            CHECK(code == 7);
            CHECK(g_session_closes == 0);
        }
    }
    if (failures != before)
        fprintf(stderr, "FAILED: %s control %s %s%s\n", draft_name(v),
                direction == WTQ_STREAM_ABORT_RESET ? "RESET" : "STOP",
                k_origins[k].name, closed_first ? ", session closed first" : "");
    rig_down(&r);
}

/* -- 3. an ordinary request stream ------------------------------------------- */

static void t_request_stream_abort(moq_version_t v, uint32_t direction, size_t k)
{
    int before = failures;
    rig_t r;
    CHECK(rig_up(&r, v));
    if (r.c) {
        int opened_before = g_stream_n;
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("abort") };
        sc.track_namespace = (moq_namespace_t){ ns, 1 };
        sc.track_name = MOQ_BYTES_LITERAL("t");
        sc.filter = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP;
        moq_subscription_t sub;
        CHECK(moq_session_subscribe(r.s, &sc, 0, &sub) == MOQ_OK);
        moq_wtquic_conn_service(r.c);
        complete_sends(r.c);
        CHECK(g_stream_n == opened_before + 1);
        CHECK(g_local_opens == FIXTURE_LOCAL_STREAMS);
        CHECK(g_local_refused == 0);
        wtq_stream_t *st = &g_streams[opened_before];
        CHECK(st->bidi);
        struct moq_wtq_stream *ms = stream_find_by_handle(r.c, st);
        CHECK(ms != NULL);
        CHECK(ms && !(r.c->ctrl_latched && ms->id == r.c->ctrl_id));
        CHECK(ms && !moq_transport_bridge_stream_is_control(r.c->bridge, ms->id));
        deliver_abort(&r, st, direction, k_origins[k].origin, k_origins[k].app,
                      k_origins[k].wire);
        moq_wtquic_conn_service(r.c);
        complete_sends(r.c);
        CHECK(closed_events(r.s, NULL, NULL, 0) == 0);
        CHECK(moq_session_state(r.s) == MOQ_SESS_ESTABLISHED);
        CHECK(g_session_closes == 0);
        CHECK(!moq_wtquic_conn_is_closed(r.c));
        CHECK(!moq_wtquic_conn_is_fatal(r.c));
    }
    if (failures != before)
        fprintf(stderr, "FAILED: %s request stream %s %s\n", draft_name(v),
                direction == WTQ_STREAM_ABORT_RESET ? "RESET" : "STOP",
                k_origins[k].name);
    rig_down(&r);
}

/* -- 5. reset and stop events only ----------------------------------------------------- */

/* A caller that forwards only on_stream_reset / on_stream_stop gets their
 * handling: on draft-16 a control-bidi RESET is left to the session terminal
 * and a STOP ends the control stream; on drafts 18/22 both end it. */
static void t_reset_stop_only(moq_version_t v, bool stop)
{
    int before = failures;
    rig_t r;
    CHECK(rig_up(&r, v));
    if (r.c) {
        wtq_stream_t *st = v == MOQ_VERSION_DRAFT_16 || stop ? r.ctrl_local
                                                             : r.ctrl_peer;
        if (stop) moq_wtquic_conn_events()->on_stream_stop(WS, st, 0, r.c);
        else moq_wtquic_conn_events()->on_stream_reset(WS, st, 0, r.c);
        moq_wtquic_conn_service(r.c);
        complete_sends(r.c);
        uint64_t code = UINT64_MAX;
        int events = closed_events(r.s, &code, NULL, 0);
        const bool ends = stop || v != MOQ_VERSION_DRAFT_16;
        if (ends) {
            CHECK(events == 1);
            CHECK(code == 0x3);
            CHECK(g_session_closes == 1);
        } else {
            CHECK(events == 0);
            CHECK(g_session_closes == 0);
            CHECK(moq_session_state(r.s) == MOQ_SESS_ESTABLISHED);
        }
    }
    if (failures != before)
        fprintf(stderr, "FAILED: %s reset/stop-only %s\n", draft_name(v),
                stop ? "on_stream_stop" : "on_stream_reset");
    rig_down(&r);
}

int main(void)
{
    fixture_budget();
    t_refusals();
    const moq_version_t drafts[3] = { MOQ_VERSION_DRAFT_16,
                                      MOQ_VERSION_DRAFT_18,
                                      MOQ_VERSION_DRAFT_22 };
    for (int d = 0; d < 3; d++) {
        for (size_t k = 0; k < N_ORIGINS; k++) {
            for (int closed_first = 0; closed_first < 2; closed_first++) {
                t_control_abort(drafts[d], WTQ_STREAM_ABORT_RESET, k,
                                closed_first != 0);
                t_control_abort(drafts[d], WTQ_STREAM_ABORT_STOP, k,
                                closed_first != 0);
            }
            if (drafts[d] != MOQ_VERSION_DRAFT_16) {
                t_request_stream_abort(drafts[d], WTQ_STREAM_ABORT_RESET, k);
                t_request_stream_abort(drafts[d], WTQ_STREAM_ABORT_STOP, k);
            }
        }
        t_reset_stop_only(drafts[d], false);
        t_reset_stop_only(drafts[d], true);
    }
    if (failures == 0) printf("PASS: test_wtquic_abort\n");
    else fprintf(stderr, "FAIL: test_wtquic_abort (%d)\n", failures);
    return failures ? 1 : 0;
}
