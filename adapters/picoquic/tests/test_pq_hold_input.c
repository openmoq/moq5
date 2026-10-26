/*
 * Raw picoquic adapter: receive admission (MOQ_TRANSPORT_CAP_HOLD_INPUT).
 *
 * The fetcher session is attached to the adapter, driven through the real
 * bridge with picoquic stubbed out (as test_backpressure.c does): inbound
 * bytes arrive through moq_pq_callback, outbound bytes are pulled through
 * the endpoint's prepare_to_send path. The publisher is a raw session over
 * the fake transport endpoint; this file relays between the two. Every
 * row runs on draft-16, draft-18 and draft-22 (the last two: uni-control
 * pair, request bidis).
 *
 * What the rows pin, for a response stream the session cannot admit yet:
 * the adapter keeps exactly the refused chunk (bytes and FIN) under the
 * stream's window budget, freezes credit, appends later bytes/FIN behind
 * it in order, redelivers it once (and holds it again, same allocation, if
 * another stream took the entry first), never replays a classification
 * prefix the bridge already owns, settles a FIN owed behind bytes the
 * session later retained without another transport callback, and releases
 * everything exactly once on reset/close/teardown with no credit granted
 * after a terminal. Allocation failure and the budget boundary report the
 * adapter's explicit failure contract instead of claiming acceptance.
 */

#include <moq/picoquic.h>
#include <moq/session.h>
#include <moq/rcbuf.h>
#include <moq/transport_bridge.h>
#include <moq/vi64.h>
#include "../../common/moq_pq_send_queue.h"
#include "../picoquic_endpoint.h"
#include "pq_test_seam.h"
#include "held_bridge_driver.h"
#include "../../../tests/unit/test_binding_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- picoquic stubs ------------------------------------------------- */

static uint64_t g_time = 1000;
static uint64_t g_next_uni = 2;     /* client-initiated uni: 2, 6, 10 .. */
static uint64_t g_next_bidi = 0;    /* client-initiated bidi: 0, 4, 8 .. */
static uint64_t g_reset_code = 0;   /* code the next stream_reset event carries */
static uint64_t g_peer_stop_code = 0;
static uint64_t g_rx_window = 65535;
static uint64_t g_tp_streams_uni = 64;   /* initial_max_stream_id_unidir */
static uint64_t g_tp_streams_bidi = 0;
static uint32_t g_client_max_events = 64;   /* the fetcher session's event queue */
static picoquic_state_enum g_cnx_state = picoquic_state_ready;

/* The order of the provider calls the endpoint makes, cleared per step:
 * C stream context set, M active mark, R reset, S stop. Every stub returns
 * at once and allocates nothing, so an ordering that would create streams
 * at a real provider is only recorded here. */
static char g_calls[32];
static int g_calls_n = 0;
static void call_log(char c)
    { if (g_calls_n < (int)sizeof(g_calls) - 1) { g_calls[g_calls_n++] = c; g_calls[g_calls_n] = 0; } }
static void calls_clear(void) { g_calls_n = 0; g_calls[0] = 0; }
static int g_reset_rc = 0;   /* what the next resets answer */
static int g_stop_rc = 0;    /* what the next stops answer, unless refused */
static uint64_t g_stop_once_sid = UINT64_MAX;
static int g_stop_once_rc = 0;

picoquic_quic_t *picoquic_get_quic_ctx(picoquic_cnx_t *c) { (void)c; return NULL; }
/* Per-stream context: the adapter watches stopped streams through it; no
 * release ever arrives here. */
int picoquic_set_app_stream_ctx(picoquic_cnx_t *c, uint64_t sid, void *ctx)
{
    (void)c; (void)ctx;
    call_log('C');
    if (sid < ((uint64_t)1 << 62) && (sid & 1u) == 0) {
        uint64_t *next = (sid & 2u) ? &g_next_uni : &g_next_bidi;
        if (sid >= *next) *next = sid + 4;
    }
    return 0;
}
void picoquic_unlink_app_stream_ctx(picoquic_cnx_t *c, uint64_t sid)
{ (void)c; (void)sid; }
#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
int picoquic_enable_stream_send_commit(picoquic_cnx_t *c, uint64_t sid)
{ (void)c; (void)sid; return 0; }
int picoquic_get_remote_stop_error(picoquic_cnx_t *c, uint64_t sid, uint64_t *out)
{ (void)c; (void)sid; *out = g_peer_stop_code; return 0; }
#endif
uint64_t picoquic_get_quic_time(picoquic_quic_t *q) { (void)q; return g_time; }
static int g_set_cb_n = 0;   /* callbacks installed (fn non-NULL) */
void picoquic_set_callback(picoquic_cnx_t *c, picoquic_stream_data_cb_fn fn, void *ctx)
    { (void)c; (void)ctx; if (fn) g_set_cb_n++; }
int picoquic_add_to_stream(picoquic_cnx_t *c, uint64_t sid, const uint8_t *d, size_t len, int fin)
    { (void)c; (void)sid; (void)d; (void)len; (void)fin; return 0; }
uint64_t picoquic_get_data_sent(picoquic_cnx_t *c) { (void)c; return 0; }
int picoquic_set_stream_priority(picoquic_cnx_t *c, uint64_t sid, uint8_t p)
    { (void)c; (void)sid; (void)p; return 0; }

/* Active-stream set: the endpoint marks a stream active when it has queued
 * bytes; the relay pulls every active stream. */
#define ACT_MAX 64
static uint64_t g_active[ACT_MAX];
static int g_active_n = 0;
static void act_set(uint64_t sid, int on)
{
    for (int i = 0; i < g_active_n; i++)
        if (g_active[i] == sid) {
            if (!on) { g_active[i] = g_active[--g_active_n]; }
            return;
        }
    if (on) {
        CHECK(g_active_n < ACT_MAX);
        if (g_active_n < ACT_MAX) g_active[g_active_n++] = sid;
    }
}
int picoquic_mark_active_stream(picoquic_cnx_t *c, uint64_t sid, int active, void *v)
    { (void)c; (void)v; call_log('M'); act_set(sid, active); return 0; }

static uint8_t g_provide_buf[4096];
static size_t g_provide_nb; static int g_provide_fin; static int g_provide_still;
uint8_t *picoquic_provide_stream_data_buffer(void *ctx, size_t nb, int is_fin, int is_still_active)
    { (void)ctx; g_provide_nb = nb; g_provide_fin = is_fin; g_provide_still = is_still_active;
      return nb <= sizeof(g_provide_buf) ? g_provide_buf : NULL; }
uint64_t picoquic_get_next_local_stream_id(picoquic_cnx_t *c, int uni)
    { (void)c; return uni ? g_next_uni : g_next_bidi; }
uint64_t picoquic_get_remote_stream_error(picoquic_cnx_t *c, uint64_t sid)
    { (void)c; (void)sid; return g_reset_code; }

/* STOP_SENDING / RESET issued by the adapter (the fetcher side). */
#define SIG_MAX 64
static struct { uint64_t sid; uint64_t code; bool accepted; } g_stops[SIG_MAX];
static int g_stop_n = 0, g_stop_seen = 0;
static int g_reset_n = 0;
static uint64_t g_last_reset_sid, g_last_reset_code, g_last_stop_sid;
int picoquic_reset_stream(picoquic_cnx_t *c, uint64_t sid, uint64_t ec)
{
    (void)c;
    call_log('R');
    g_last_reset_sid = sid;
    g_last_reset_code = ec;
    g_reset_n++;
    return g_reset_rc;
}
int picoquic_reset_stream_at(picoquic_cnx_t *c, uint64_t sid, uint64_t ec, uint64_t rs)
    { (void)rs; return picoquic_reset_stream(c, sid, ec); }
/* A genuine provider failure, separate from a terminal no-op. A refused call
 * is still counted. */
static bool g_stop_refused = false;
int picoquic_stop_sending(picoquic_cnx_t *c, uint64_t sid, uint64_t ec)
{
    (void)c; call_log('S'); g_last_stop_sid = sid;
    int rc = g_stop_rc;
    if (g_stop_once_sid != UINT64_MAX && sid == g_stop_once_sid) {
        rc = g_stop_once_rc;
        g_stop_once_sid = UINT64_MAX;
    }
    if (g_stop_refused) rc = -1;
    CHECK(g_stop_n < SIG_MAX);
    if (g_stop_n < SIG_MAX) {
        g_stops[g_stop_n].sid = sid;
        g_stops[g_stop_n].code = ec;
        g_stops[g_stop_n].accepted = rc == 0;
        g_stop_n++;
    }
    return rc;
}

/* Pair exact per-SID counts with an exact total to exclude every unexpected
 * stream, duplicate call, code change and false claim of provider acceptance. */
static void check_stop(uint64_t sid, uint64_t code, int expected, bool accepted)
{
    int count = 0;
    for (int i = 0; i < g_stop_n; i++) {
        if (g_stops[i].sid != sid) continue;
        count++;
        CHECK(g_stops[i].code == code && g_stops[i].accepted == accepted);
    }
    CHECK(count == expected);
}
int picoquic_close(picoquic_cnx_t *c, uint64_t ec) { (void)c; (void)ec; return 0; }
int picoquic_close_ex(picoquic_cnx_t *c, uint64_t ec, const char *r) { (void)c; (void)ec; (void)r; return 0; }
uint64_t picoquic_get_application_error(picoquic_cnx_t *c) { (void)c; return 0; }
int picoquic_queue_datagram_frame(picoquic_cnx_t *c, size_t len, const uint8_t *d)
    { (void)c; (void)len; (void)d; return 0; }
static bool g_tp_null = false;   /* the provider cannot read the parameters */
picoquic_tp_t const *picoquic_get_transport_parameters(picoquic_cnx_t *c, int get_local)
    { (void)c; (void)get_local; static picoquic_tp_t tp; memset(&tp, 0, sizeof(tp));
      if (g_tp_null) return NULL;
      tp.max_datagram_frame_size = 1252;
      tp.initial_max_stream_data_uni = g_rx_window;
      tp.initial_max_stream_data_bidi_remote = g_rx_window;
      tp.initial_max_stream_data_bidi_local = g_rx_window;
      tp.initial_max_stream_id_unidir = g_tp_streams_uni;
      tp.initial_max_stream_id_bidir = g_tp_streams_bidi;
      return &tp; }

/* Flow-control instrumentation: app-flow-control takeover and grants per stream. */
#define FC_MAX 256
static struct { uint64_t sid; int use; int open_calls; uint64_t open_total; } g_fc[FC_MAX];
static int g_fc_n = 0;
static int fc_slot(uint64_t sid)
    { for (int i = 0; i < g_fc_n; i++) if (g_fc[i].sid == sid) return i;
      if (g_fc_n < FC_MAX) { g_fc[g_fc_n].sid = sid; g_fc[g_fc_n].use = -1; return g_fc_n++; }
      CHECK(false && "flow-control recorder full"); return -1; }
static int fc_use(uint64_t sid)
    { for (int i = 0; i < g_fc_n; i++) if (g_fc[i].sid == sid) return g_fc[i].use; return -1; }
static int fc_open_calls(uint64_t sid)
    { for (int i = 0; i < g_fc_n; i++) if (g_fc[i].sid == sid) return g_fc[i].open_calls; return 0; }
int picoquic_set_app_flow_control(picoquic_cnx_t *c, uint64_t sid, int use)
    { (void)c; int s = fc_slot(sid); if (s < 0) return -1; g_fc[s].use = use; return 0; }
int picoquic_open_flow_control(picoquic_cnx_t *c, uint64_t sid, uint64_t sz)
    { (void)c; int s = fc_slot(sid); if (s < 0) return -1; g_fc[s].open_calls++; g_fc[s].open_total += sz; return 0; }
int picoquic_is_client(picoquic_cnx_t *c) { (void)c; return 1; }
picoquic_state_enum picoquic_get_cnx_state(picoquic_cnx_t *c) { (void)c; return g_cnx_state; }

/* -- Counting allocator with failure injection ------------------------- */

static long g_bal = 0;
static int g_fail_realloc_after = -1;   /* -1 off; n: the (n+1)th realloc fails once */
static int g_realloc_calls = 0;
static void *ca(size_t sz, void *c) { (void)c; g_bal++; return malloc(sz); }
static void *cr(void *p, size_t o, size_t n, void *c)
{
    (void)o; (void)c;
    if (g_fail_realloc_after >= 0 && g_realloc_calls++ == g_fail_realloc_after) {
        g_fail_realloc_after = -1;
        return NULL;
    }
    if (!p) g_bal++;
    return realloc(p, n);
}
static void cf(void *p, size_t sz, void *c) { (void)sz; (void)c; if (p) g_bal--; free(p); }

/* -- Observed events ----------------------------------------------------- */

typedef struct obs {
    int ok, object, complete, error, reset, closed, other;
    uint64_t seen[2];
    int duplicates, bad_payload;
} obs_t;

static void drain(moq_session_t *s, obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_OK: o->ok++; break;
        case MOQ_EVENT_FETCH_OBJECT: {
            o->object++;
            uint64_t id = ev.u.fetch_object.object_id;
            if (id < 128) {
                if (o->seen[id / 64] & (1ull << (id % 64))) o->duplicates++;
                o->seen[id / 64] |= 1ull << (id % 64);
            }
            moq_rcbuf_t *pl = ev.u.fetch_object.payload;
            bool good = pl != NULL && moq_rcbuf_len(pl) == 16;
            if (good) {
                const uint8_t *d = moq_rcbuf_data(pl);
                for (size_t i = 0; i < 16; i++) if (d[i] != (uint8_t)(0xd0 + id)) { good = false; break; }
            }
            if (!good) o->bad_payload++;
            break;
        }
        case MOQ_EVENT_FETCH_COMPLETE: o->complete++; break;
        case MOQ_EVENT_FETCH_ERROR: o->error++; break;
        case MOQ_EVENT_FETCH_RESET: o->reset++; break;
        case MOQ_EVENT_SESSION_CLOSED: o->closed++; break;
        default: o->other++; break;
        }
        moq_event_cleanup(&ev);
    }
}

/* ids [0, n) each exactly once, nothing else, every payload the writer's pattern */
static bool inventory_exact(const obs_t *o, int n)
{
    for (int i = 0; i < n; i++) if (!(o->seen[i / 64] & (1ull << (i % 64)))) return false;
    for (int i = n; i < 128; i++) if (o->seen[i / 64] & (1ull << (i % 64))) return false;
    return o->duplicates == 0 && o->bad_payload == 0 && o->object == n;
}

static void obs_merge(obs_t *t, const obs_t *c)
{
    t->ok += c->ok; t->object += c->object; t->complete += c->complete; t->error += c->error;
    t->reset += c->reset; t->closed += c->closed; t->other += c->other;
    for (int i = 0; i < 2; i++) {
        t->duplicates += (int)__builtin_popcountll(t->seen[i] & c->seen[i]);
        t->seen[i] |= c->seen[i];
    }
    t->duplicates += c->duplicates;
    t->bad_payload += c->bad_payload;
}

/* -- The rig --------------------------------------------------------------- */

#define UNI_MAX 96
typedef struct rig {
    moq_version_t   version;
    moq_alloc_t     alloc;
    moq_session_t  *c;        /* fetcher, adapter-attached */
    moq_pq_conn_t  *ad;
    moq_session_t  *sv;       /* raw publisher */
    moq_transport_bridge_t *sb;
    fake_endpoint_t sep;
    /* server uni streams: fake-endpoint op id -> peer stream id on the client */
    struct { uint64_t opid; uint64_t sid; } unis[UNI_MAX];
    int             n_unis;
    /* capture: server writes on this op id are collected, not delivered */
    uint64_t        capture_opid;
    uint8_t         cap[8192];
    size_t          cap_len;
    bool            cap_fin;
    bool            hold_client;   /* do not relay the fetcher's STOP_SENDING */
} rig_t;

static uint64_t rig_sid(rig_t *r, uint64_t opid)
{
    for (int i = 0; i < r->n_unis; i++) if (r->unis[i].opid == opid) return r->unis[i].sid;
    return UINT64_MAX;
}
static uint64_t rig_opid(rig_t *r, uint64_t sid)
{
    for (int i = 0; i < r->n_unis; i++) if (r->unis[i].sid == sid) return r->unis[i].opid;
    return UINT64_MAX;
}

static int deliver(rig_t *r, uint64_t sid, const uint8_t *data, size_t len, bool fin)
{
    return moq_pq_callback(NULL, sid, (uint8_t *)data, len,
                    fin ? picoquic_callback_stream_fin : picoquic_callback_stream_data,
                    r->ad, NULL);
}

/* Pull the fetcher's queued bytes for every active stream into the server. */
static void relay_client_sends(rig_t *r)
{
    for (int guard = 0; guard < 64 && g_active_n > 0; guard++) {
        uint64_t sid = g_active[0];
        g_provide_nb = 0; g_provide_fin = 0; g_provide_still = 0;
        moq_pq_callback(NULL, sid, (uint8_t *)(uintptr_t)0xABC, 1200,
                        picoquic_callback_prepare_to_send, r->ad, NULL);
        if (g_provide_nb == 0 && !g_provide_fin) { act_set(sid, 0); continue; }
        bool fin = g_provide_fin != 0;
        if (r->version != MOQ_VERSION_DRAFT_16) {
            if (sid & 2u)
                (void)held_bridge_uni_bytes(r->sb, sid, g_provide_buf, g_provide_nb, fin, 0);
            else
                (void)moq_transport_bridge_on_peer_bidi_bytes(r->sb, sid, g_provide_buf, g_provide_nb, fin, 0);
        } else {
            (void)moq_transport_bridge_on_peer_control_bytes(r->sb, 0, g_provide_buf, g_provide_nb, fin, 0);
        }
        if (!g_provide_still) act_set(sid, 0);
    }
}

/* Deliver the server's output: its own streams become peer streams on the
 * fetcher's adapter; writes addressed to the fetcher's streams land there. */
static void relay_server_ops(rig_t *r)
{
    (void)held_bridge_service(r->sb, 0);
    CHECK(!r->sep.overflowed && !r->sep.truncated);
    for (size_t i = 0; i < r->sep.count; i++) {
        fake_op_t *op = &r->sep.ops[i];
        if (op->kind == FAKE_OP_OPEN_UNI) {
            CHECK(r->n_unis < UNI_MAX);
            if (r->n_unis < UNI_MAX) {
                r->unis[r->n_unis].opid = op->stream_id;
                r->unis[r->n_unis].sid = 3u + 4u * (uint64_t)r->n_unis;   /* real server uni ids */
                r->n_unis++;
            }
        } else if (op->kind == FAKE_OP_WRITE) {
            uint64_t sid = rig_sid(r, op->stream_id);
            if (sid == UINT64_MAX) {
                /* a write on one of the FETCHER's streams (d16 control bidi 0,
                 * uni-pair request-bidi responses) */
                deliver(r, op->stream_id, op->data, op->data_len, op->fin);
            } else if (op->stream_id == r->capture_opid) {
                CHECK(op->data_len <= sizeof(r->cap) - r->cap_len);
                if (r->cap_len + op->data_len <= sizeof(r->cap)) {
                    memcpy(r->cap + r->cap_len, op->data, op->data_len);
                    r->cap_len += op->data_len;
                }
                if (op->fin) r->cap_fin = true;
            } else {
                deliver(r, sid, op->data, op->data_len, op->fin);
            }
        } else if (op->kind == FAKE_OP_RESET) {
            uint64_t sid = rig_sid(r, op->stream_id);
            if (sid != UINT64_MAX) {
                g_reset_code = op->error_code;
                moq_pq_callback(NULL, sid, NULL, 0, picoquic_callback_stream_reset, r->ad, NULL);
            }
        }
    }
    fake_endpoint_clear_ops(&r->sep);
}

static void relay_client_stops(rig_t *r)
{
    for (; g_stop_seen < g_stop_n; g_stop_seen++) {
        uint64_t opid = rig_opid(r, g_stops[g_stop_seen].sid);
        if (opid == UINT64_MAX || r->hold_client || !g_stops[g_stop_seen].accepted) continue;
        (void)moq_transport_bridge_on_peer_stop_sending(r->sb, opid, g_stops[g_stop_seen].code, 0);
    }
}

static void pump(rig_t *r, int rounds)
{
    for (int i = 0; i < rounds; i++) {
        (void)moq_pq_service(r->ad, g_time);
        relay_client_sends(r);
        relay_client_stops(r);
        relay_server_ops(r);
        (void)moq_pq_service(r->ad, g_time);
    }
}

static bool rig_up_config(rig_t *r, moq_version_t version,
                          uint64_t uni_credit, uint32_t outgoing_subgroups,
                          bool request_pressure)
{
    memset(r, 0, sizeof(*r));
    r->version = version;
    r->capture_opid = UINT64_MAX;
    r->alloc = (moq_alloc_t){ NULL, ca, cr, cf };
    g_next_uni = 2; g_next_bidi = 0; g_active_n = 0; g_fc_n = 0; memset(g_fc, 0, sizeof(g_fc));
    g_stop_n = 0; g_stop_seen = 0; g_reset_n = 0; g_realloc_calls = 0; g_fail_realloc_after = -1;
    g_stop_once_sid = UINT64_MAX; g_stop_once_rc = 0;
    g_stop_refused = false;
    g_tp_streams_uni = uni_credit;
    g_tp_streams_bidi = request_pressure ? 256 : 0;
    g_peer_stop_code = 0;
    g_cnx_state = picoquic_state_ready;

    moq_session_cfg_t cc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &r->alloc, MOQ_PERSPECTIVE_CLIENT);
    cc.version = version;
    cc.send_request_capacity = true;
    cc.initial_request_capacity = request_pressure ? 512 : 64;
    cc.max_subscriptions = request_pressure ? 256 : 0;
    cc.max_data_streams = 2;
    cc.max_fetches = 32;
    cc.max_events = g_client_max_events;
    if (moq_session_create(&cc, 0, &r->c) < 0) return false;
    if (test_bind_if_required(r->c) < 0) return false;

    moq_pq_conn_cfg_t ac;
    moq_pq_conn_cfg_init_sized(&ac, sizeof(ac));
    ac.session = r->c;
    ac.cnx = (picoquic_cnx_t *)(uintptr_t)0xDEAD;
    ac.alloc = &r->alloc;
    if (moq_pq_conn_create(&ac, &r->ad) != 0) return false;

    moq_session_cfg_t sc;
    moq_session_cfg_init_sized(&sc, sizeof(sc), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    sc.version = version;
    sc.send_request_capacity = true;
    sc.initial_request_capacity = request_pressure ? 512 : 64;
    sc.max_subscriptions = request_pressure ? 256 : 0;
    sc.max_fetches = 32;
    sc.max_open_subgroups = outgoing_subgroups;
    sc.max_events = 64;
    if (moq_session_create(&sc, 0, &r->sv) < 0) return false;
    if (test_bind_if_required(r->sv) < 0) return false;
    /* server op ids start at 1000: never one of the fetcher's own stream ids */
    held_endpoint_init(&r->sep, 1000, 1);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (request_pressure) bcfg.max_streams = 256;
    if (held_bridge_create(&bcfg, r->sv, &r->sep.vtable, &r->sep, &r->sb) != MOQ_OK) return false;

    if (moq_session_start(r->c, 0) < 0) return false;
    if (version != MOQ_VERSION_DRAFT_16 && moq_session_start(r->sv, 0) < 0) return false;
    pump(r, 8);
    moq_event_t ev;
    while (moq_session_poll_events(r->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(r->sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    return moq_session_state(r->c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(r->sv) == MOQ_SESS_ESTABLISHED && !moq_pq_conn_is_fatal(r->ad);
}

static bool rig_up_limits(rig_t *r, moq_version_t version,
                          uint64_t uni_credit, uint32_t outgoing_subgroups)
{
    return rig_up_config(r, version, uni_credit, outgoing_subgroups, false);
}

static bool rig_up(rig_t *r, moq_version_t version)
{
    return rig_up_limits(r, version, 64, 64);
}

static void rig_down(rig_t *r)
{
    if (r->ad) moq_pq_conn_destroy(r->ad);
    if (r->sb) held_bridge_destroy(r->sb);
    if (r->c) moq_session_destroy(r->c);
    if (r->sv) moq_session_destroy(r->sv);
    CHECK(g_bal == 0);
    g_bal = 0;
}

/* -- fetch helpers ---------------------------------------------------------- */

static uint64_t g_end_object = 3;

static moq_result_t issue_fetch(moq_session_t *s, moq_fetch_t *h)
{
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = MOQ_BYTES_LITERAL("t");
    fc.end_group = 0;
    fc.end_object = g_end_object;
    return moq_session_fetch(s, &fc, 0, h);
}

static moq_result_t write_obj(moq_session_t *pub, moq_fetch_t fh, uint64_t oid)
{
    uint8_t body[16];
    memset(body, (int)(0xd0 + oid), sizeof(body));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) != MOQ_OK) return MOQ_ERR_NOMEM;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0; oc.subgroup_id = 0; oc.object_id = oid; oc.publisher_priority = 100;
    oc.payload = pl;
    moq_result_t rc = moq_session_write_fetch_object(pub, fh, &oc, 0);
    moq_rcbuf_decref(pl);
    return rc;
}

static bool fetch_all(rig_t *r, int n, moq_fetch_t *fh, moq_fetch_t *sfh)
{
    for (int k = 0; k < n; k++) CHECK(issue_fetch(r->c, &fh[k]) == MOQ_OK);
    pump(r, 4);
    moq_event_t ev;
    int got = 0;
    while (moq_session_poll_events(r->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST && got < n) sfh[got++] = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);
    }
    return got == n;
}

static bool accept_and_write(rig_t *r, moq_fetch_t sfh, uint64_t oid)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = g_end_object;
    if (moq_session_accept_fetch(r->sv, sfh, &ac, 0) != MOQ_OK) return false;
    return write_obj(r->sv, sfh, oid) == MOQ_OK;
}

/* Two live responses occupy both receive entries. */
static void fill_pool(rig_t *r, moq_fetch_t *sfh, obs_t *co)
{
    for (int k = 0; k < 2; k++) {
        CHECK(accept_and_write(r, sfh[k], 0));
        pump(r, 3);
    }
    memset(co, 0, sizeof(*co));
    drain(r->c, co);
    CHECK(co->ok == 2 && co->object == 2 && co->complete == 0 && co->closed == 0);
    CHECK(!moq_session_can_admit_data_stream(r->c));
}

/* Answer request `sfh` with object 0 (and END when `end`), capturing the
 * response's wire bytes instead of delivering them. Returns the peer stream
 * id the response will arrive on. */
static uint64_t capture_response(rig_t *r, moq_fetch_t sfh, bool end)
{
    uint64_t opid = r->sep.next_uni_id;
    r->capture_opid = opid; r->cap_len = 0; r->cap_fin = false;
    CHECK(accept_and_write(r, sfh, 0));
    if (end) CHECK(moq_session_end_fetch(r->sv, sfh, 0) == MOQ_OK);
    pump(r, 2);
    uint64_t sid = rig_sid(r, opid);
    CHECK(sid != UINT64_MAX && r->cap_len > 0 && (!end || r->cap_fin));
    return sid;
}

/* Ordinary attach has no hold table on e92. This forward-behavior baseline
 * asks production admission to bound known-data holds at the existing policy
 * size, not to overflow an already allocated default table. One subscription
 * keeps request mappings out of the capacity prerequisite. */
#define PRODUCTION_HOLDS 64
#define PRODUCTION_STREAMS (2 + PRODUCTION_HOLDS + 1)

typedef struct production_obs {
    int ok, goaway, objects[PRODUCTION_STREAMS][2], fins[PRODUCTION_STREAMS];
    uint64_t order[4 * PRODUCTION_STREAMS];
    size_t n;
} production_obs_t;

static void production_body(uint8_t body[16], uint64_t group, uint64_t object)
{
    for (size_t i = 0; i < 16; i++)
        body[i] = (uint8_t)(3 * group + object + i);
}

static moq_result_t production_write(rig_t *r, moq_subgroup_handle_t sg,
                                      uint64_t group, uint64_t object)
{
    uint8_t body[16];
    production_body(body, group, object);
    moq_rcbuf_t *payload = NULL;
    moq_result_t rc = moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &payload);
    if (rc != MOQ_OK) return rc;
    rc = moq_session_write_object(r->sv, sg, object, payload, 0);
    moq_rcbuf_decref(payload);
    return rc;
}

static void production_drain(rig_t *r, moq_subscription_t sub,
                              production_obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(r->c, &ev, 1) > 0) {
        uint64_t identity = UINT64_MAX;
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            CHECK(ev.u.subscribe_ok.sub._opaque == sub._opaque);
            o->ok++;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *e = &ev.u.object_received;
            CHECK(e->sub._opaque == sub._opaque && !moq_publication_is_valid(e->pub));
            CHECK(e->group_id < PRODUCTION_STREAMS && e->subgroup_id == 0);
            CHECK(e->object_id < 2 && e->publisher_priority == 100);
            CHECK(e->status == MOQ_OBJECT_NORMAL && !e->datagram && !e->end_of_group);
            CHECK(e->first_object == (r->version != MOQ_VERSION_DRAFT_16 && e->object_id == 0));
            uint8_t body[16];
            production_body(body, e->group_id, e->object_id);
            CHECK(e->payload && moq_rcbuf_len(e->payload) == sizeof(body) &&
                  memcmp(moq_rcbuf_data(e->payload), body, sizeof(body)) == 0);
            if (e->group_id < PRODUCTION_STREAMS && e->object_id < 2)
                CHECK(++o->objects[e->group_id][e->object_id] == 1);
            identity = 4 * e->group_id + e->object_id;
        } else if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) {
            const moq_subgroup_finished_event_t *e = &ev.u.subgroup_finished;
            CHECK(e->sub._opaque == sub._opaque && !moq_publication_is_valid(e->pub));
            CHECK(e->group_id < PRODUCTION_STREAMS && e->subgroup_id == 0 && !e->end_of_group);
            if (e->group_id < PRODUCTION_STREAMS) {
                CHECK(o->objects[e->group_id][0] == 1);
                CHECK(++o->fins[e->group_id] == 1);
            }
            identity = 4 * e->group_id + 2;
        } else if (ev.kind == MOQ_EVENT_GOAWAY) {
            o->goaway++;
        } else {
            CHECK(ev.kind == MOQ_EVENT_REQUEST_READY);
        }
        if (identity != UINT64_MAX) {
            CHECK(o->n < sizeof(o->order) / sizeof(o->order[0]));
            if (o->n < sizeof(o->order) / sizeof(o->order[0]))
                o->order[o->n++] = identity;
        }
        moq_event_cleanup(&ev);
    }
}

static bool production_subscribe(rig_t *r, moq_subscription_t *client,
                                  moq_subscription_t *server)
{
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("capacity");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    if (moq_session_subscribe(r->c, &cfg, 0, client) != MOQ_OK) return false;
    pump(r, 4);
    int requests = 0;
    moq_event_t ev;
    while (moq_session_poll_events(r->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            *server = ev.u.subscribe_request.sub;
            requests++;
        } else {
            CHECK(ev.kind == MOQ_EVENT_REQUEST_READY);
        }
        moq_event_cleanup(&ev);
    }
    if (requests != 1) return false;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(r->sv, *server, &ac, 0) != MOQ_OK) return false;
    pump(r, 4);
    return true;
}

/* Capture only bytes actually produced for a real new subgroup. */
static bool production_capture(rig_t *r, moq_subscription_t server,
                                uint64_t group, bool fin,
                                moq_subgroup_handle_t *sg, uint64_t *sid)
{
    uint64_t opid = r->sep.next_uni_id;
    r->capture_opid = opid;
    r->cap_len = 0;
    r->cap_fin = false;
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    cfg.group_id = group;
    cfg.publisher_priority = 100;
    cfg.first_object = true;
    if (moq_session_open_subgroup(r->sv, server, &cfg, 0, sg) != MOQ_OK ||
        production_write(r, *sg, group, 0) != MOQ_OK ||
        (fin && moq_session_close_subgroup(r->sv, *sg, 0) != MOQ_OK))
        return false;
    pump(r, 2);
    r->capture_opid = UINT64_MAX;
    *sid = rig_sid(r, opid);
    return *sid != UINT64_MAX && r->cap_len > 0 && r->cap_fin == fin;
}

static int row_production_bounded_admission(moq_version_t v)
{
    int before = failures;
    rig_t r;
    /* Set permission before attach. At most 67 data + two controls + one
     * request mapping are live: well below the unmodified default 128 map. */
    bool up = rig_up_limits(&r, v, 96, 80);
    CHECK(up);
    if (!up) { rig_down(&r); return failures - before; }
    r.hold_client = true;       /* peer may deliver its terminal after our STOP */
    moq_subscription_t client = MOQ_SUBSCRIPTION_INVALID;
    moq_subscription_t server = MOQ_SUBSCRIPTION_INVALID;
    bool subscribed = production_subscribe(&r, &client, &server);
    CHECK(subscribed);
    if (!subscribed) { rig_down(&r); return failures - before; }
    production_obs_t o = { 0 };
    production_drain(&r, client, &o);
    CHECK(o.ok == 1 && o.n == 0);
    moq_subgroup_handle_t sg[PRODUCTION_STREAMS];
    uint64_t sid[PRODUCTION_STREAMS];
    int grants[PRODUCTION_STREAMS];
    moq_pq_test_rx_t held_at_arrival[PRODUCTION_HOLDS] = { 0 };
    const uint64_t code = v == MOQ_VERSION_DRAFT_16 ? MOQ_STREAM_ERROR_CANCELLED
                                                    : MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
    uint8_t oldest[8192];
    size_t oldest_len = 0;

    for (int i = 0; i < PRODUCTION_STREAMS; i++) {
        bool fin = i == 2;     /* oldest hold owns an exact chunk with FIN */
        bool captured = production_capture(&r, server, (uint64_t)i, fin, &sg[i], &sid[i]);
        CHECK(captured);
        if (!captured) { rig_down(&r); return failures - before; }
        CHECK(sid[i] == 3u + 4u * ((uint64_t)i + (v != MOQ_VERSION_DRAFT_16)));
        if (i == 2) {
            oldest_len = r.cap_len;
            memcpy(oldest, r.cap, oldest_len);
        }
        grants[i] = fc_open_calls(sid[i]);
        CHECK(deliver(&r, sid[i], r.cap, r.cap_len, fin) == 0);
        if (i >= 2) {
            CHECK(fc_use(sid[i]) == 1);
            CHECK(fc_open_calls(sid[i]) == grants[i]);
        }
        if (i < 2) {
            production_drain(&r, client, &o);
            CHECK(o.objects[i][0] == 1 && o.fins[i] == 0);
        } else if (i < PRODUCTION_STREAMS - 1) {
            moq_pq_test_rx_t st;
            bool tracked = moq_pq_test_rx_state(r.ad, sid[i], &st);
            CHECK(tracked);
            if (tracked) {
                CHECK(st.paused && st.held_input && st.blocked == 0);
                CHECK(st.held_len == r.cap_len && st.held_fin == fin && st.buf_fin == fin);
                CHECK(st.buf_len == r.cap_len && st.buf &&
                      memcmp(st.buf, r.cap, r.cap_len) == 0);
                held_at_arrival[i - 2] = st;
            }
            /* Raw services the preceding hold in production_capture's pump.
             * Equal priorities select the two admitted refs in order. */
            const int first = i >= 3 ? 1 : 0;
            const int second = i >= 4 ? 1 : 0;
            CHECK(g_stop_n == first + second && g_reset_n == 0);
            check_stop(sid[0], code, first, true);
            check_stop(sid[1], code, second, true);
        }
        CHECK(!moq_pq_conn_is_fatal(r.ad));
        CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        if (i >= 1) CHECK(!moq_session_can_admit_data_stream(r.c));
    }
    production_drain(&r, client, &o);
    CHECK(o.n == 2 && o.order[0] == 0 && o.order[1] == 4);

    const int rejected = PRODUCTION_STREAMS - 1;
    for (int n = 0; n < 3; n++) CHECK(moq_pq_service(r.ad, g_time) == 0);
    /* Two pressure victims still occupy RX; the 65th hold is refused. */
    CHECK(g_stop_n == 3 && g_reset_n == 0);
    check_stop(sid[0], code, 1, true);
    check_stop(sid[1], code, 1, true);
    check_stop(sid[rejected], code, 1, true);
    CHECK(!moq_session_can_admit_data_stream(r.c));

    /* Repeated service cannot grant held-stream credit or duplicate a STOP. */
    for (int n = 0; n < 3; n++) CHECK(moq_pq_service(r.ad, g_time) == 0);
    for (int i = 2; i < PRODUCTION_STREAMS; i++) {
        CHECK(fc_open_calls(sid[i]) == grants[i]);
        if (i == rejected) continue;
        moq_pq_test_rx_t held;
        bool present = moq_pq_test_rx_state(r.ad, sid[i], &held);
        CHECK(present);
        if (present) {
            const moq_pq_test_rx_t *saved = &held_at_arrival[i - 2];
            CHECK(held.paused && held.held_input && held.blocked == 0);
            CHECK(held.held_len == saved->held_len && held.held_fin == saved->held_fin &&
                  held.buf_fin == saved->buf_fin && held.buf_len == saved->buf_len &&
                  held.buf == saved->buf);
        }
    }
    CHECK(!moq_session_can_admit_data_stream(r.c));
    CHECK(g_stop_n == 3 && g_reset_n == 0);
    check_stop(sid[0], code, 1, true);
    check_stop(sid[1], code, 1, true);
    check_stop(sid[rejected], code, 1, true);
    CHECK(production_write(&r, sg[1], 1, 1) == MOQ_OK);
    pump(&r, 2);
    production_drain(&r, client, &o);
    /* Accepted pressure STOP discards this later object, without losing either
     * healthy object already delivered before cancellation. */
    CHECK(o.n == 2 && o.order[0] == 0 && o.order[1] == 4);
    CHECK(o.objects[0][0] == 1 && o.objects[1][0] == 1 && o.objects[1][1] == 0);

    /* The refused live stream's later FIN must discard, never deliver it.
     * No redundant STOP is demanded for a receive half already terminal. */
    CHECK(moq_session_close_subgroup(r.sv, sg[rejected], 0) == MOQ_OK);
    relay_server_ops(&r);
    for (int n = 0; n < 3; n++) CHECK(moq_pq_service(r.ad, g_time) == 0);
    moq_pq_test_rx_t rejected_state;
    bool tracked = moq_pq_test_rx_state(r.ad, sid[rejected], &rejected_state);
    CHECK(!tracked || (!rejected_state.held_input && rejected_state.buf_len == 0));
    CHECK(g_stop_n == 3 && g_reset_n == 0);
    check_stop(sid[0], code, 1, true);
    check_stop(sid[1], code, 1, true);
    check_stop(sid[rejected], code, 1, true);

    /* Releasing one admitted entry is the only new peer input. Neither held
     * stream is fed again by the fixture: service replays bytes and FIN. */
    moq_pq_test_rx_t oldest_state;
    tracked = moq_pq_test_rx_state(r.ad, sid[2], &oldest_state);
    CHECK(tracked);
    if (tracked) {
        CHECK(oldest_state.held_input && oldest_state.held_fin &&
              oldest_state.buf_len == oldest_len && oldest_state.buf &&
              memcmp(oldest_state.buf, oldest, oldest_len) == 0);
    }
    CHECK(moq_session_close_subgroup(r.sv, sg[0], 0) == MOQ_OK);
    relay_server_ops(&r);
    for (int n = 0; n < 8; n++) CHECK(moq_pq_service(r.ad, g_time) == 0);
    production_drain(&r, client, &o);
    CHECK(o.n == 5);
    CHECK(o.order[0] == 0 && o.order[1] == 4 && o.order[2] == 8 &&
          o.order[3] == 10 && o.order[4] == 12);
    CHECK(o.objects[2][0] == 1 && o.objects[3][0] == 1 && o.objects[1][1] == 0);
    CHECK(o.fins[0] == 0 && o.fins[1] == 0 && o.fins[2] == 1 && o.fins[3] == 0);
    /* The real terminal freed group 0. Oldest-held group 2 finishes, group 3
     * delivers its object, then becomes the next eligible pressure victim. */
    CHECK(g_stop_n == 4 && g_reset_n == 0);
    check_stop(sid[0], code, 1, true);
    check_stop(sid[1], code, 1, true);
    check_stop(sid[rejected], code, 1, true);
    check_stop(sid[3], code, 1, true);
    for (int i = 4; i < PRODUCTION_STREAMS; i++)
        CHECK(o.objects[i][0] == 0 && o.fins[i] == 0);
    tracked = moq_pq_test_rx_state(r.ad, sid[2], &oldest_state);
    CHECK(!tracked || (!oldest_state.held_input && oldest_state.buf_len == 0));
    moq_pq_test_rx_t next_state;
    tracked = moq_pq_test_rx_state(r.ad, sid[4], &next_state);
    CHECK(tracked && next_state.held_input);
    CHECK(!moq_session_can_admit_data_stream(r.c));
    CHECK(!moq_pq_conn_is_fatal(r.ad) && moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);

    /* The control channel still progresses with the remaining holds parked. */
    CHECK(moq_session_goaway(r.sv, NULL, 0, 0) == MOQ_OK);
    pump(&r, 3);
    production_drain(&r, client, &o);
    CHECK(o.ok == 1 && o.goaway == 1 && o.n == 5);
    CHECK(moq_session_state(r.c) == MOQ_SESS_DRAINING && !moq_pq_conn_is_fatal(r.ad));
    CHECK(g_stop_n == 4 && g_reset_n == 0);
    check_stop(sid[0], code, 1, true);
    check_stop(sid[1], code, 1, true);
    check_stop(sid[rejected], code, 1, true);
    check_stop(sid[3], code, 1, true);
    CHECK(fc_open_calls(sid[rejected]) == grants[rejected]);
    rig_down(&r);
    if (failures == before) printf("PASS: pq production bounded admission draft-%d\n",
                                  v == MOQ_VERSION_DRAFT_16 ? 16 : v == MOQ_VERSION_DRAFT_18 ? 18 : 22);
    return failures - before;
}

/* A full event queue defers GOAWAY. Incomplete UNI types still collect with
 * OK; completed types take one of the 64 holds or are refused immediately,
 * BEFORE control drains. Split input holds/refuses only its completing chunk,
 * never the already accepted prefix. */
static int row_deferred_admission(moq_version_t v, bool split, int stop_rc, bool refuse)
{
    int before = failures;
    rig_t r;
    g_client_max_events = 1;
    bool up = rig_up_limits(&r, v, 96, 80);
    g_client_max_events = 64;
    CHECK(up);
    if (!up) { rig_down(&r); return failures - before; }
    r.hold_client = true;
    moq_subscription_t client = MOQ_SUBSCRIPTION_INVALID;
    moq_subscription_t server = MOQ_SUBSCRIPTION_INVALID;
    bool subscribed = production_subscribe(&r, &client, &server);
    CHECK(subscribed);
    if (!subscribed) { rig_down(&r); return failures - before; }
    production_obs_t o = { 0 };
    production_drain(&r, client, &o);
    moq_subgroup_handle_t sg[PRODUCTION_STREAMS];
    uint64_t sid[PRODUCTION_STREAMS];
    int grants[PRODUCTION_STREAMS];
    const int streams = PRODUCTION_STREAMS - (refuse ? 0 : 1);
    const int last = streams - 1;
    const uint8_t *last_buf = NULL;
    const uint64_t code = MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
    for (int i = 0; i < streams; i++) {
        bool captured = production_capture(&r, server, (uint64_t)i, false, &sg[i], &sid[i]);
        CHECK(captured);
        if (!captured) goto out;
        grants[i] = fc_open_calls(sid[i]);
        if (split && i == last) {
            /* A valid non-minimal type exposes collection separately from
             * the bounded disposition of the completing callback. */
            uint64_t type = 0, expanded_type = 0;
            size_t type_len = moq_vi64_decode(r.cap, r.cap_len, &type);
            CHECK(r.cap_len > 2 && r.cap_len < sizeof(r.cap) &&
                  type_len == 1 && type <= 0x7f);
            if (r.cap_len <= 2 || r.cap_len >= sizeof(r.cap) ||
                type_len != 1 || type > 0x7f) goto out;
            memmove(r.cap + 1, r.cap, r.cap_len);
            r.cap[0] = 0x80;
            r.cap_len++;
            size_t expanded_len = moq_vi64_decode(r.cap, r.cap_len, &expanded_type);
            CHECK(expanded_len == 2 && expanded_type == type);
            if (expanded_len != 2 || expanded_type != type) goto out;
            size_t fragment_len = moq_vi64_decode(r.cap, 1, &expanded_type);
            CHECK(fragment_len == 0);
            if (fragment_len != 0) goto out;
            CHECK(deliver(&r, sid[i], r.cap, 1, false) == 0);
            for (int n = 0; n < 3; n++) {
                moq_pq_test_rx_t prefix;
                CHECK(moq_pq_service(r.ad, g_time) == 0);
                CHECK(moq_pq_test_rx_state(r.ad, sid[i], &prefix));
                CHECK(!prefix.paused && !prefix.held_input && prefix.prefix_len == 1 &&
                      prefix.delivered == 1 && prefix.blocked == 0 && prefix.buf_len == 0);
                CHECK(g_stop_n == 0 && g_reset_n == 0 &&
                      fc_open_calls(sid[i]) == grants[i]);
            }
            if (refuse) {
                g_stop_once_sid = sid[i];
                g_stop_once_rc = stop_rc;
            }
            CHECK(deliver(&r, sid[i], r.cap + 1, r.cap_len - 1, false) == 0);
        } else {
            if (refuse && i == last) {
                g_stop_once_sid = sid[i];
                g_stop_once_rc = stop_rc;
            }
            CHECK(deliver(&r, sid[i], r.cap, r.cap_len, false) == 0);
        }
        if (i == 0) production_drain(&r, client, &o);
        if (i == 1) {
            /* Keep object(1) queued: GOAWAY cannot emit its event yet. */
            CHECK(moq_session_goaway(r.sv, NULL, 0, 0) == MOQ_OK);
            relay_server_ops(&r);
            CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        }
        if (refuse && i == last) {
            CHECK(g_stop_once_sid == UINT64_MAX);
            CHECK(g_stop_n == 1 && g_reset_n == 0);
            check_stop(sid[last], code, 1, stop_rc == 0);
            check_stop(sid[0], code, 0, true);
            check_stop(sid[1], code, 0, true);
            CHECK(moq_pq_conn_is_fatal(r.ad) == (stop_rc == -1));
            CHECK(fc_use(sid[i]) == 1 && fc_open_calls(sid[i]) == grants[i]);
            CHECK(o.n == 1 && o.goaway == 0);
            if (stop_rc == -1) goto out;
            CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
            moq_pq_test_rx_t stopped;
            bool tracked = moq_pq_test_rx_state(r.ad, sid[i], &stopped);
            if (stop_rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) CHECK(!tracked);
            else CHECK(tracked && !stopped.paused && !stopped.held_input &&
                       stopped.buf_len == 0 && stopped.prefix_len == 0 && stopped.release_watch);
            continue;
        }
        if (i >= 2) {
            moq_pq_test_rx_t st;
            bool tracked = moq_pq_test_rx_state(r.ad, sid[i], &st);
            CHECK(tracked);
            if (tracked) {
                size_t prefix = split && i == last ? 1 : 0;
                size_t held = r.cap_len - prefix;
                CHECK(st.paused && st.held_input && st.held_len == held && st.blocked == 0);
                CHECK(!st.held_fin && !st.buf_fin && st.delivered == r.cap_len);
                CHECK(st.buf_len == held && st.buf &&
                      memcmp(st.buf, r.cap + prefix, held) == 0);
                if (i == last) last_buf = st.buf;
            }
            CHECK(fc_use(sid[i]) == 1 && fc_open_calls(sid[i]) == grants[i]);
        }
        CHECK(g_stop_n == 0 && g_reset_n == 0 && !moq_pq_conn_is_fatal(r.ad));
    }
    CHECK(o.n == 1 && o.goaway == 0);
    /* Pending GOAWAY must still suppress all F cancellation. */
    CHECK(g_stop_n == (refuse ? 1 : 0) && g_reset_n == 0);
    check_stop(sid[0], code, 0, true);
    check_stop(sid[1], code, 0, true);
    check_stop(sid[last], code, refuse ? 1 : 0, stop_rc == 0);
    production_drain(&r, client, &o);       /* free the queued object slot */
    CHECK(o.n == 2);
    for (int n = 0; n < 8; n++) {
        (void)moq_pq_service(r.ad, g_time);
        if (moq_pq_conn_is_fatal(r.ad)) break;
        production_drain(&r, client, &o);
    }
    /* Clearing control permits exactly two pressure STOPs. Acceptance does
     * not release either RX entry; the held input remains adapter-owned. */
    CHECK(g_stop_n == (refuse ? 3 : 2) && g_reset_n == 0);
    check_stop(sid[0], code, 1, true);
    check_stop(sid[1], code, 1, true);
    check_stop(sid[last], code, refuse ? 1 : 0, stop_rc == 0);
    CHECK(!moq_session_can_admit_data_stream(r.c));
    if (!refuse) {
        moq_pq_test_rx_t held;
        bool tracked = moq_pq_test_rx_state(r.ad, sid[last], &held);
        CHECK(tracked && split);
        if (tracked) {
            CHECK(held.held_input && held.held_len == r.cap_len - 1 && held.blocked == 0);
            CHECK(held.buf == last_buf && held.buf_len == r.cap_len - 1 &&
                  held.buf && memcmp(held.buf, r.cap + 1, r.cap_len - 1) == 0);
        }
        CHECK(g_stop_n == 2 && g_reset_n == 0 && o.n == 2 && o.goaway == 1);
        check_stop(sid[0], code, 1, true);
        check_stop(sid[1], code, 1, true);
        check_stop(sid[last], code, 0, true);
        CHECK(fc_open_calls(sid[last]) == grants[last]);
        g_reset_code = 0;       /* real zero-code peer RESET settles this hold */
        CHECK(moq_pq_callback(NULL, sid[last], NULL, 0, picoquic_callback_stream_reset,
                              r.ad, NULL) == 0);
        CHECK(moq_pq_service(r.ad, g_time) == 0);
        production_drain(&r, client, &o);
        CHECK(!moq_pq_test_rx_state(r.ad, sid[last], &held));
        CHECK(g_stop_n == 2 && g_reset_n == 0 && o.n == 2 && o.goaway == 1);
        check_stop(sid[0], code, 1, true);
        check_stop(sid[1], code, 1, true);
        check_stop(sid[last], code, 0, true);
        CHECK(!moq_pq_conn_is_fatal(r.ad));
        goto out;
    }
    CHECK(moq_pq_conn_is_fatal(r.ad) == (stop_rc == -1));
    if (stop_rc == -1) goto out;
    CHECK(o.n == 2 && o.goaway == 1 && moq_session_state(r.c) == MOQ_SESS_DRAINING);
    moq_pq_test_rx_t st;
    bool tracked = moq_pq_test_rx_state(r.ad, sid[last], &st);
    if (stop_rc == PICOQUIC_ERROR_STREAM_ALREADY_CLOSED) {
        CHECK(!tracked);
    } else {
        CHECK(tracked && !st.held_input && !st.paused && st.buf_len == 0 && st.release_watch);
    }
    CHECK(fc_open_calls(sid[last]) == grants[last]);
    if (stop_rc == 0) {
        /* A successful STOP still awaits a real native receive terminal.
         * Repeated input before that terminal is discarded, never re-admitted. */
        CHECK(deliver(&r, sid[last], r.cap, r.cap_len, false) == 0);
        CHECK(moq_pq_service(r.ad, g_time) == 0);
        production_drain(&r, client, &o);
        CHECK(o.n == 2 && g_stop_n == 3 && g_reset_n == 0);
        check_stop(sid[0], code, 1, true);
        check_stop(sid[1], code, 1, true);
        check_stop(sid[last], code, 1, true);
        CHECK(fc_open_calls(sid[last]) == grants[last]);
        CHECK(moq_session_close_subgroup(r.sv, sg[last], 0) == MOQ_OK);
        relay_server_ops(&r);
        CHECK(moq_pq_service(r.ad, g_time) == 0);
        CHECK(!moq_pq_test_rx_state(r.ad, sid[last], &st));
    }
    production_drain(&r, client, &o);
    CHECK(o.n == 2 && o.goaway == 1 && g_stop_n == 3 && g_reset_n == 0);
    check_stop(sid[0], code, 1, true);
    check_stop(sid[1], code, 1, true);
    check_stop(sid[last], code, 1, stop_rc == 0);
    CHECK(!moq_pq_conn_is_fatal(r.ad));
out:
    g_stop_once_sid = UINT64_MAX; g_stop_once_rc = 0;
    g_stop_rc = 0;
    rig_down(&r);
    if (failures == before) printf("PASS: pq deferred admission %s stop=%d draft-%d\n",
                                  split ? "split-prefix" : "whole", stop_rc,
                                  v == MOQ_VERSION_DRAFT_18 ? 18 : 22);
    return failures - before;
}

/* Request pressure is mapping pressure, never the core data pool. Two uni
 * controls plus 126 pending request bidis fill the ordinary 128-entry map. */
#define REQUEST_MAPPED 126
#define REQUEST_HELD 64
#define REQUEST_STREAMS (REQUEST_MAPPED + REQUEST_HELD + 1)

typedef struct request_obs {
    moq_subscription_t received[REQUEST_STREAMS];
    int seen[REQUEST_STREAMS], order[REQUEST_STREAMS], n, cancelled, goaway;
    bool allow_cancel;
} request_obs_t;

static void request_drain(rig_t *r, request_obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(r->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            const moq_subscribe_request_event_t *e = &ev.u.subscribe_request;
            CHECK(e->track_namespace.count == 1);
            if (e->track_namespace.count == 1) {
                CHECK(e->track_namespace.parts[0].len == 4 &&
                      memcmp(e->track_namespace.parts[0].data, "live", 4) == 0);
            }
            bool name_ok = e->track_name.len == 4 &&
                           e->track_name.data[0] == 'r' && e->track_name.data[3] == 'q';
            CHECK(name_ok);
            unsigned index = name_ok ? ((unsigned)e->track_name.data[1] << 8) |
                                       e->track_name.data[2] : REQUEST_STREAMS;
            CHECK(index < REQUEST_STREAMS && moq_subscription_is_valid(e->sub));
            if (index < REQUEST_STREAMS) {
                CHECK(++o->seen[index] == 1);
                for (int k = 0; k < REQUEST_STREAMS; k++)
                    if (o->seen[k] && (unsigned)k != index)
                        CHECK(o->received[k]._opaque != e->sub._opaque);
                o->received[index] = e->sub;
                CHECK(o->n < REQUEST_STREAMS);
                if (o->n < REQUEST_STREAMS) o->order[o->n++] = (int)index;
            }
        } else if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) {
            CHECK(o->allow_cancel && ev.u.unsubscribed.sub._opaque == o->received[0]._opaque);
            CHECK(ev.u.unsubscribed.cause == MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM);
            CHECK(ev.u.unsubscribed.stream_error_code == MOQ_STREAM_ERROR_CANCELLED);
            CHECK(++o->cancelled == 1);
        } else if (ev.kind == MOQ_EVENT_GOAWAY) {
            CHECK(++o->goaway == 1);
        } else {
            CHECK(ev.kind == MOQ_EVENT_REQUEST_READY);
        }
        moq_event_cleanup(&ev);
    }
}

/* Only this collector drives request output: fake endpoint IDs are opaque.
 * Each actual OPEN_BIDI is assigned one small server-initiated native ID by
 * the row; unlike the FETCH rig there are no locally initiated responses. */
static bool request_capture(rig_t *r, unsigned index,
                              moq_subscription_t *owner, uint64_t *peer_opid)
{
    uint8_t name[] = { 'r', (uint8_t)(index >> 8), (uint8_t)index, 'q' };
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ .parts = parts, .count = 1 };
    cfg.track_name = (moq_bytes_t){ .data = name, .len = sizeof(name) };
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_result_t rc = moq_session_subscribe(r->sv, &cfg, 0, owner);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return false;
    rc = held_bridge_service(r->sb, 0);
    CHECK(rc == MOQ_OK && !r->sep.overflowed && !r->sep.truncated);
    if (rc != MOQ_OK || r->sep.overflowed || r->sep.truncated) return false;
    int opened = 0;
    *peer_opid = UINT64_MAX;
    r->cap_len = 0;
    r->cap_fin = false;
    for (size_t k = 0; k < r->sep.count; k++) {
        const fake_op_t *op = &r->sep.ops[k];
        if (op->kind == FAKE_OP_OPEN_BIDI) {
            *peer_opid = op->stream_id;
            opened++;
        } else if (op->kind == FAKE_OP_WRITE && op->stream_id == *peer_opid) {
            CHECK(op->data_len <= sizeof(r->cap) - r->cap_len);
            if (op->data_len > sizeof(r->cap) - r->cap_len) return false;
            memcpy(r->cap + r->cap_len, op->data, op->data_len);
            r->cap_len += op->data_len;
            r->cap_fin |= op->fin;
        } else {
            CHECK(false && "unexpected request output");
            return false;
        }
    }
    fake_endpoint_clear_ops(&r->sep);
    return opened == 1 && *peer_opid == 1u + index && r->cap_len > 0 && !r->cap_fin;
}

/* D18 3.3.2 / D22 6.4.2.3 permit cancellation before acknowledgement.
 * Use the requester's public operation and its real emitted RESET/STOP,
 * delivering RESET first so the one terminal has an exact causal identity. */
static bool request_peer_cancel(rig_t *r, moq_subscription_t owner,
                                uint64_t peer_opid, uint64_t sid)
{
    moq_result_t rc = moq_session_unsubscribe(r->sv, owner, 0);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return false;
    rc = held_bridge_service(r->sb, 0);
    CHECK(rc == MOQ_OK && !r->sep.overflowed && !r->sep.truncated);
    if (rc != MOQ_OK || r->sep.overflowed || r->sep.truncated) return false;
    int resets = 0, stops = 0;
    for (size_t k = 0; k < r->sep.count; k++) {
        const fake_op_t *op = &r->sep.ops[k];
        CHECK(op->stream_id == peer_opid && op->error_code == MOQ_STREAM_ERROR_CANCELLED);
        if (op->kind == FAKE_OP_RESET) resets++;
        else if (op->kind == FAKE_OP_STOP) stops++;
        else CHECK(false && "unexpected peer cancellation output");
    }
    fake_endpoint_clear_ops(&r->sep);
    CHECK(resets == 1 && stops == 1);
    if (resets != 1 || stops != 1) return false;
    g_reset_code = MOQ_STREAM_ERROR_CANCELLED;
    g_peer_stop_code = MOQ_STREAM_ERROR_CANCELLED;
    CHECK(moq_pq_callback(NULL, sid, NULL, 0, picoquic_callback_stream_reset, r->ad, NULL) == 0);
    CHECK(moq_pq_callback(NULL, sid, NULL, 0, picoquic_callback_stop_sending, r->ad, NULL) == 0);
    return !moq_pq_conn_is_fatal(r->ad);
}

static int row_production_request_admission_mode(moq_version_t v, bool reset_failure)
{
    int before = failures;
    CHECK(v != MOQ_VERSION_DRAFT_16);    /* never cancel the shared control bidi */
    rig_t r;
    /* Credits/owners are configured before attach. The peer bridge has 256
     * maps so it can generate all 191 valid requests; the adapter stays 128. */
    bool up = rig_up_config(&r, v, 64, 64, true);
    CHECK(up);
    if (!up) { rig_down(&r); return failures - before; }
    CHECK(g_tp_streams_bidi >= REQUEST_STREAMS);
    /* These profiles use native bidi credit, not MAX_REQUEST_ID; the legacy
     * capacity observer deliberately reports zero (profile_d18.c). */
    CHECK(moq_session_local_request_capacity(r.c) == 0);
    CHECK(moq_session_peer_request_capacity(r.sv) == 0);
    request_obs_t o = { 0 };
    moq_subscription_t owner[REQUEST_STREAMS];
    uint64_t peer_opid[REQUEST_STREAMS], sid[REQUEST_STREAMS];
    int grants[REQUEST_STREAMS];
    uint8_t oldest[8192];
    size_t oldest_len = 0;

    for (unsigned i = 0; i < REQUEST_STREAMS; i++) {
        bool captured = request_capture(&r, i, &owner[i], &peer_opid[i]);
        CHECK(captured);
        if (!captured) goto out;
        sid[i] = 1u + 4u * i;
        if (i == REQUEST_MAPPED) {
            oldest_len = r.cap_len;
            memcpy(oldest, r.cap, oldest_len);
        }
        CHECK(moq_session_can_admit_data_stream(r.c)); /* not receive-pool pressure */
        grants[i] = fc_open_calls(sid[i]);
        if (reset_failure && i == REQUEST_STREAMS - 1) g_reset_rc = -1;
        int callback_rc = deliver(&r, sid[i], r.cap, r.cap_len, false);
        bool fatal = moq_pq_conn_is_fatal(r.ad);
        CHECK(fc_use(sid[i]) == 1 && fc_open_calls(sid[i]) == grants[i]);
        if (callback_rc != 0 || fatal) {
            if (reset_failure && i == REQUEST_STREAMS - 1) {
                CHECK(fatal && o.n == REQUEST_MAPPED && o.seen[i] == 0);
                CHECK(g_stop_n == 1 && g_reset_n == 1);
                if (g_stop_n > 0)
                    CHECK(g_stops[0].sid == sid[i] && g_stops[0].code == MOQ_STREAM_ERROR_EXCESSIVE_LOAD);
                CHECK(g_last_reset_sid == sid[i] && g_last_reset_code == MOQ_STREAM_ERROR_EXCESSIVE_LOAD);
                goto out;
            }
            /* Stop at the FIRST divergence, before any further arrival or
             * service can turn a known map failure into setup cascades. */
            fprintf(stderr, "BOUNDARY: pq request draft-%d index=%u sid=%llu received=%d callback=%d fatal=%d\n",
                    v == MOQ_VERSION_DRAFT_18 ? 18 : 22, i,
                    (unsigned long long)sid[i], o.n, callback_rc, (int)fatal);
            CHECK(i == REQUEST_MAPPED && o.n == REQUEST_MAPPED);
            CHECK(callback_rc == 0 && !fatal);   /* e92: first over-map request */
            goto out;
        }
        for (int n = 0; n < 2; n++) CHECK(moq_pq_service(r.ad, g_time) == 0);
        request_drain(&r, &o);
        CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        if (i < REQUEST_MAPPED) {
            CHECK(o.n == (int)i + 1 && o.order[i] == (int)i);
        } else {
            CHECK(o.n == REQUEST_MAPPED);
            if (i < REQUEST_STREAMS - 1) {
                moq_pq_test_rx_t st;
                bool tracked = moq_pq_test_rx_state(r.ad, sid[i], &st);
                CHECK(tracked);
                if (tracked) {
                    CHECK(st.held_input && st.paused && st.blocked == 0 &&
                          st.held_len == r.cap_len && !st.held_fin && !st.buf_fin);
                    CHECK(st.buf_len == r.cap_len && st.buf &&
                          memcmp(st.buf, r.cap, r.cap_len) == 0);
                }
            }
        }
        if (i < REQUEST_STREAMS - 1) CHECK(g_stop_n == 0 && g_reset_n == 0);
    }

    CHECK(!reset_failure);       /* a refused own RESET must fail the transport */
    uint64_t overload = MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
    CHECK(g_stop_n == 1 && g_reset_n == 1);
    if (g_stop_n > 0) {
        CHECK(g_stops[0].sid == sid[REQUEST_STREAMS - 1] && g_stops[0].code == overload);
    }
    if (g_reset_n > 0) {
        CHECK(g_last_reset_sid == sid[REQUEST_STREAMS - 1] && g_last_reset_code == overload);
    }
    moq_pq_test_rx_t st;
    bool tracked = moq_pq_test_rx_state(r.ad, sid[REQUEST_STREAMS - 1], &st);
    CHECK(!tracked || (!st.held_input && st.buf_len == 0));
    for (int n = 0; n < 3; n++) CHECK(moq_pq_service(r.ad, g_time) == 0);
    request_drain(&r, &o);
    CHECK(o.n == REQUEST_MAPPED && o.cancelled == 0);
    CHECK(g_stop_n == 1 && g_reset_n == 1);
    for (int i = REQUEST_MAPPED; i < REQUEST_STREAMS; i++)
        CHECK(fc_open_calls(sid[i]) == grants[i]);

    tracked = moq_pq_test_rx_state(r.ad, sid[REQUEST_MAPPED], &st);
    CHECK(tracked);
    if (tracked) {
        CHECK(st.held_input && st.buf_len == oldest_len && st.buf &&
              memcmp(st.buf, oldest, oldest_len) == 0);
    }
    o.allow_cancel = true;
    bool cancelled = request_peer_cancel(&r, owner[0], peer_opid[0], sid[0]);
    CHECK(cancelled);
    if (!cancelled) goto out;
    /* No held request is delivered again by the fixture: only the actual
     * peer's cancellation above and subsequent service make room. */
    for (int n = 0; n < 8; n++) CHECK(moq_pq_service(r.ad, g_time) == 0);
    request_drain(&r, &o);
    CHECK(o.cancelled == 1 && o.n == REQUEST_MAPPED + 1);
    for (int i = 0; i <= REQUEST_MAPPED; i++)
        CHECK(o.seen[i] == 1 && o.order[i] == i);
    for (int i = REQUEST_MAPPED + 1; i < REQUEST_STREAMS; i++) CHECK(o.seen[i] == 0);
    CHECK(g_stop_n == 1 && g_reset_n == 2);
    uint64_t cancel_code = MOQ_STREAM_ERROR_CANCELLED;
    CHECK(g_last_reset_sid == sid[0] && g_last_reset_code == cancel_code);
    tracked = moq_pq_test_rx_state(r.ad, sid[REQUEST_MAPPED + 1], &st);
    CHECK(tracked && st.held_input);
    CHECK(moq_session_can_admit_data_stream(r.c));
    CHECK(!moq_pq_conn_is_fatal(r.ad) && moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);

    /* The existing control stream needs no new map despite request pressure. */
    CHECK(moq_session_goaway(r.sv, NULL, 0, 0) == MOQ_OK);
    relay_server_ops(&r);
    for (int n = 0; n < 3; n++) CHECK(moq_pq_service(r.ad, g_time) == 0);
    request_drain(&r, &o);
    CHECK(o.n == REQUEST_MAPPED + 1 && o.cancelled == 1 && o.goaway == 1);
    CHECK(g_stop_n == 1 && g_reset_n == 2);
    CHECK(fc_open_calls(sid[REQUEST_STREAMS - 1]) == grants[REQUEST_STREAMS - 1]);
    CHECK(moq_session_state(r.c) == MOQ_SESS_DRAINING && !moq_pq_conn_is_fatal(r.ad));
out:
    g_reset_rc = 0;
    rig_down(&r);
    if (failures == before) printf("PASS: pq production request admission%s draft-%d\n",
                                  reset_failure ? " RESET-failure" : "",
                                  v == MOQ_VERSION_DRAFT_18 ? 18 : 22);
    return failures - before;
}

static int row_production_request_admission(moq_version_t v)
{
    return row_production_request_admission_mode(v, false);
}

/* -- rows -------------------------------------------------------------------- */

/* Pool exhausted: the whole first chunk of a new response (bytes + FIN) is
 * refused and kept exactly; credit stays frozen; unrelated streams progress;
 * once an entry frees the chunk is redelivered exactly once and the request
 * completes with an exact inventory. Two shapes: the response as ONE chunk
 * (FIN with the bytes), and header chunk first with the object + FIN arriving
 * behind the held chunk while paused (appended in order, replayed after). */
static int row_admission(moq_version_t v, bool split)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    rig_t r;
    if (!rig_up(&r, v)) {
        CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[4], sfh[4];
    CHECK(fetch_all(&r, 4, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);
    uint64_t sid = capture_response(&r, sfh[2], true);
    if (sid == UINT64_MAX) { rig_down(&r); return failures - before; }
    uint8_t all[8192]; size_t total = r.cap_len; memcpy(all, r.cap, total);
    r.capture_opid = UINT64_MAX;
    size_t first = split ? 2 : total;          /* header write is 2 bytes */
    deliver(&r, sid, all, first, !split);
    moq_pq_test_rx_t st;
    CHECK(moq_pq_test_rx_state(r.ad, sid, &st));
    int grants_at_hold = fc_open_calls(sid);
    printf("admission %s %s: total=%zu held_len=%zu held_fin=%d buf_len=%zu paused=%d blocked=%llu fatal=%d\n", lbl,
           split ? "split" : "single", total, st.held_len, (int)st.held_fin, st.buf_len, (int)st.paused,
           (unsigned long long)st.blocked, (int)moq_pq_conn_is_fatal(r.ad));
    CHECK(st.active && st.paused && st.held_input);
    CHECK(st.held_len == first && st.held_fin == !split);              /* exactly the refused chunk */
    CHECK(st.buf_len == first && st.buf && memcmp(st.buf, all, first) == 0);
    CHECK(st.blocked == 0);                                            /* not bridge-owned */
    CHECK(fc_use(sid) == 1);                                           /* window owned, frozen */
    CHECK(g_stop_n == 0 && g_reset_n == 0);                            /* nothing STOPped */
    CHECK(!moq_pq_conn_is_fatal(r.ad) && moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
    if (split) {
        /* the rest of the response arrives behind the held chunk */
        deliver(&r, sid, all + first, total - first, true);
        CHECK(moq_pq_test_rx_state(r.ad, sid, &st));
        CHECK(st.held_len == first && !st.held_fin && st.buf_fin);
        CHECK(st.buf_len == total && st.buf && memcmp(st.buf, all, total) == 0);  /* in order, byte-exact */
    }
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    CHECK(co.ok == 1 && co.object == 0 && co.complete == 0 && co.error == 0 && co.reset == 0);
    /* unrelated progress on live stream 0 */
    CHECK(write_obj(r.sv, sfh[0], 1) == MOQ_OK);
    pump(&r, 2);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    CHECK(co.object == 1 && co.seen[0] == 2u && co.bad_payload == 0);
    CHECK(moq_pq_test_rx_state(r.ad, sid, &st) && st.paused && st.held_input);
    CHECK(fc_open_calls(sid) == grants_at_hold);                       /* no credit while held */
    /* capacity returns: live stream 0 ends; the service pass redelivers */
    CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 3);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    printf("admission %s %s recovery: objects=%d complete=%d dup=%d bad=%d\n", lbl, split ? "split" : "single",
           co.object, co.complete, co.duplicates, co.bad_payload);
    CHECK(co.complete == 2 && inventory_exact(&co, 1) && co.error == 0 && co.reset == 0);
    CHECK(!moq_pq_test_rx_state(r.ad, sid, &st) || (!st.held_input && !st.paused && st.buf_len == 0));
    CHECK(moq_session_can_admit_data_stream(r.c));
    CHECK(!moq_pq_conn_is_fatal(r.ad));
    rig_down(&r);
    if (failures == before) printf("PASS: pq hold_input admission %s %s\n", lbl, split ? "split" : "single");
    return failures - before;
}

/* Repeated refusal: two held responses (the first stays LIVE after its
 * admission, so it keeps the entry); one entry frees; the first replayed
 * stream wins and the other is refused again -- same allocation, same
 * bytes, credit untouched -- then completes when the next entry frees. */
static int row_repeat(moq_version_t v)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    rig_t r;
    if (!rig_up(&r, v)) {
        CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[6], sfh[6];
    CHECK(fetch_all(&r, 4, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);
    uint64_t s2 = capture_response(&r, sfh[2], false);
    uint8_t a2[512]; size_t l2 = r.cap_len; memcpy(a2, r.cap, l2);
    uint64_t s3 = capture_response(&r, sfh[3], true);
    uint8_t a3[512]; size_t l3 = r.cap_len; memcpy(a3, r.cap, l3);
    r.capture_opid = UINT64_MAX;
    if (s2 == UINT64_MAX || s3 == UINT64_MAX) { rig_down(&r); return failures - before; }
    deliver(&r, s2, a2, l2, false);
    deliver(&r, s3, a3, l3, true);
    moq_pq_test_rx_t t2, t3;
    CHECK(moq_pq_test_rx_state(r.ad, s2, &t2) && t2.held_input && t2.held_len == l2);
    CHECK(moq_pq_test_rx_state(r.ad, s3, &t3) && t3.held_input && t3.held_len == l3);
    const uint8_t *buf3 = t3.buf;
    int grants3 = fc_open_calls(s3);
    /* one entry frees: the sweep replays s2 first (table order); s3 is refused again */
    CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 3);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    CHECK(moq_pq_test_rx_state(r.ad, s3, &t3));
    printf("repeat %s: after first free objects=%d complete=%d s3 held=%d held_len=%zu same_buf=%d grants=%d\n", lbl,
           co.object, co.complete, (int)t3.held_input, t3.held_len, (int)(t3.buf == buf3), fc_open_calls(s3));
    CHECK(co.complete == 1 && co.object == 1 && co.seen[0] == 1u && co.bad_payload == 0);   /* live 0 done; s2 live */
    CHECK(!moq_session_can_admit_data_stream(r.c));
    CHECK(t3.held_input && t3.paused && t3.held_len == l3 && t3.held_fin);
    CHECK(t3.buf == buf3 && t3.buf && memcmp(t3.buf, a3, l3) == 0);                /* ownership kept, no copy */
    CHECK(fc_open_calls(s3) == grants3);
    CHECK(!moq_pq_test_rx_state(r.ad, s2, &t2) || !t2.held_input);
    /* the next entry frees (s2 ends): s3 completes */
    CHECK(moq_session_end_fetch(r.sv, sfh[2], 0) == MOQ_OK);
    pump(&r, 3);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    CHECK(co.complete == 2 && co.object == 1 && co.seen[0] == 1u && co.bad_payload == 0);
    CHECK(!moq_pq_test_rx_state(r.ad, s3, &t3) || !t3.held_input);
    CHECK(!moq_pq_conn_is_fatal(r.ad));
    rig_down(&r);
    if (failures == before) printf("PASS: pq hold_input repeated refusal %s\n", lbl);
    return failures - before;
}

/* Drafts 18 and 22: the stream type arrives as the two-byte varint 0x80 0x05 split
 * across callbacks. The first byte is accepted into the bridge's retained
 * classification prefix (nothing paused); the chunk completing the type is
 * refused at admission and held exactly (the suffix only). The redelivery
 * must not replay the prefix: the response then decodes to its exact object. */
static int row_prefix(moq_version_t v)
{
    const char *vl = v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    rig_t r;
    if (!rig_up(&r, v)) {
        CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[4], sfh[4];
    CHECK(fetch_all(&r, 4, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);
    uint64_t sid = capture_response(&r, sfh[2], true);
    r.capture_opid = UINT64_MAX;
    if (sid == UINT64_MAX || r.cap_len < 2) { rig_down(&r); return failures - before; }
    CHECK(r.cap[0] == 0x05);
    uint8_t wide[8192]; size_t wtotal = r.cap_len + 1;
    wide[0] = 0x80; wide[1] = 0x05; memcpy(wide + 2, r.cap + 1, r.cap_len - 1);
    deliver(&r, sid, wide, 1, false);
    moq_pq_test_rx_t st;
    CHECK(moq_pq_test_rx_state(r.ad, sid, &st) && !st.paused && !st.held_input && st.delivered == 1);
    deliver(&r, sid, wide + 1, wtotal - 1, true);
    CHECK(moq_pq_test_rx_state(r.ad, sid, &st));
    printf("prefix %s: total=%zu held_len=%zu held_first=0x%02x held_fin=%d paused=%d\n", vl, wtotal, st.held_len,
           st.buf_len ? st.buf[0] : 0, (int)st.held_fin, (int)st.paused);
    CHECK(st.paused && st.held_input && st.held_len == wtotal - 1 && st.held_fin);
    CHECK(st.buf_len == wtotal - 1 && st.buf && memcmp(st.buf, wide + 1, wtotal - 1) == 0);   /* suffix only */
    CHECK(!moq_pq_conn_is_fatal(r.ad));
    CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 3);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    printf("prefix %s recovery: objects=%d complete=%d dup=%d bad=%d fatal=%d\n", vl, co.object, co.complete,
           co.duplicates, co.bad_payload, (int)moq_pq_conn_is_fatal(r.ad));
    CHECK(co.complete == 2 && inventory_exact(&co, 1) && co.error == 0 && co.reset == 0);
    CHECK(!moq_pq_conn_is_fatal(r.ad));
    rig_down(&r);
    if (failures == before) printf("PASS: pq hold_input fragmented prefix %s\n", vl);
    return failures - before;
}

/* Retained-input control: a LIVE response of 70 objects as one chunk hits the
 * fetcher's 64-deep event queue; the session retains it (bridge-owned
 * WOULD_BLOCK), the adapter accounts it as blocked -- not held -- and the
 * stream drains with an exact inventory. Then, with the pool exhausted, the
 * same response is refused at admission; 69 more objects and the FIN arrive
 * behind the held chunk (`fin_while_held`), or the FIN arrives after the
 * redelivery landed in retained WOULD_BLOCK (`!fin_while_held`). Either way
 * the stream must complete from service passes alone: no further transport
 * callback carries anything. */
static int row_retained_and_deferred_fin(moq_version_t v, bool fin_while_held)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    g_end_object = 100;
    rig_t r;
    if (!rig_up(&r, v)) {
        CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[4], sfh[4];
    CHECK(fetch_all(&r, 4, fh, sfh));
    obs_t co, tot;
    memset(&tot, 0, sizeof(tot));
    /* -- control: live stream, 70 objects, one chunk -- */
    uint64_t s0 = capture_response(&r, sfh[0], false);
    for (int i = 1; i < 70; i++) {
        CHECK(write_obj(r.sv, sfh[0], (uint64_t)i) == MOQ_OK);
        if (i % 16 == 15) pump(&r, 1);
    }
    pump(&r, 1);
    uint8_t all[8192]; size_t total = r.cap_len; memcpy(all, r.cap, total);
    r.capture_opid = UINT64_MAX;
    deliver(&r, s0, all, total, false);
    moq_pq_test_rx_t st;
    CHECK(moq_pq_test_rx_state(r.ad, s0, &st));
    printf("retained %s control: total=%zu paused=%d blocked=%llu held=%d\n", lbl, total, (int)st.paused,
           (unsigned long long)st.blocked, (int)st.held_input);
    CHECK(st.paused && !st.held_input && st.blocked == total && st.buf_len == 0);
    /* A provider-positive receive terminal with code zero must not discard
     * the chunk already owned by the core; its existing inventory still drains. */
    const moq_transport_endpoint_ops_t *ops = NULL;
    void *ep = NULL;
    moq_pq_test_endpoint(r.ad, &ops, &ep);
    g_stop_rc = PICOQUIC_ERROR_STREAM_ALREADY_CLOSED;
    CHECK(ops->stop_sending(ep, s0, 0) == MOQ_TRANSPORT_OK);
    g_stop_rc = 0;
    CHECK(moq_pq_test_rx_state(r.ad, s0, &st) && st.paused && !st.held_input && st.blocked == total);
    for (int i = 0; i < 12; i++) { memset(&co, 0, sizeof(co)); drain(r.c, &co); obs_merge(&tot, &co); pump(&r, 1); }
    CHECK(inventory_exact(&tot, 70) && tot.ok == 1 && tot.complete == 0);
    CHECK(moq_pq_test_rx_state(r.ad, s0, &st) && !st.paused && st.blocked == 0);
    /* -- pool exhausted: stream 1 live too -- */
    CHECK(accept_and_write(&r, sfh[1], 0));
    pump(&r, 2);
    memset(&co, 0, sizeof(co)); drain(r.c, &co);
    CHECK(co.ok == 1 && co.object == 1 && !moq_session_can_admit_data_stream(r.c));
    /* -- response 2: header+object 0 refused, 69 objects (+FIN) behind it -- */
    uint64_t s2 = capture_response(&r, sfh[2], false);
    uint8_t h2[512]; size_t hl = r.cap_len; memcpy(h2, r.cap, hl);
    r.cap_len = 0;
    deliver(&r, s2, h2, hl, false);
    CHECK(moq_pq_test_rx_state(r.ad, s2, &st) && st.held_input && st.held_len == hl && !st.held_fin);
    for (int i = 1; i < 70; i++) {
        CHECK(write_obj(r.sv, sfh[2], (uint64_t)i) == MOQ_OK);
        if (i % 16 == 15) pump(&r, 1);
    }
    pump(&r, 1);
    uint8_t rest[8192]; size_t rl = r.cap_len; memcpy(rest, r.cap, rl);
    r.capture_opid = UINT64_MAX;
    deliver(&r, s2, rest, rl, false);                      /* appended behind the held chunk */
    if (fin_while_held) deliver(&r, s2, NULL, 0, true);    /* bare FIN while held: owed behind bytes */
    CHECK(moq_pq_test_rx_state(r.ad, s2, &st));
    printf("deferred %s %s: held_len=%zu buf_len=%zu buf_fin=%d paused=%d blocked=%llu\n", lbl,
           fin_while_held ? "fin-while-held" : "fin-after-block", st.held_len, st.buf_len, (int)st.buf_fin,
           (int)st.paused, (unsigned long long)st.blocked);
    CHECK(st.held_input && st.held_len == hl && st.buf_len == hl + rl && st.buf_fin == fin_while_held);
    if (st.buf_len != hl + rl) { rig_down(&r); g_end_object = 3; return failures - before; }
    CHECK(st.buf && st.buf_len == hl + rl && memcmp(st.buf, h2, hl) == 0 && memcmp(st.buf + hl, rest, rl) == 0);
    memset(&co, 0, sizeof(co)); drain(r.c, &co);
    CHECK(co.ok == 1 && co.object == 0);
    /* capacity returns: the redelivery is admitted; the remainder lands in the
     * session's retained WOULD_BLOCK (64-deep queue) */
    CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 2);
    memset(&tot, 0, sizeof(tot));
    memset(&co, 0, sizeof(co)); drain(r.c, &co); obs_merge(&tot, &co);
    CHECK(moq_pq_test_rx_state(r.ad, s2, &st));
    CHECK(!st.held_input);
    if (!fin_while_held) {
        /* the FIN arrives now, as a bare callback, while the session still
         * retains input for the stream: it waits behind those bytes */
        CHECK(st.paused && st.blocked > 0);
        deliver(&r, s2, NULL, 0, true);
        CHECK(moq_pq_test_rx_state(r.ad, s2, &st) && st.buf_fin && st.buf_len == 0);
    }
    /* service passes only: no transport callback carries anything more */
    for (int i = 0; i < 12; i++) {
        (void)moq_pq_service(r.ad, g_time);
        memset(&co, 0, sizeof(co)); drain(r.c, &co); obs_merge(&tot, &co);
    }
    bool gone = !moq_pq_test_rx_state(r.ad, s2, &st);
    printf("deferred %s %s recovery: objects=%d complete=%d dup=%d bad=%d entry_retired=%d\n", lbl,
           fin_while_held ? "fin-while-held" : "fin-after-block", tot.object, tot.complete, tot.duplicates,
           tot.bad_payload, (int)gone);
    CHECK(inventory_exact(&tot, 70) && tot.complete == 2 && tot.error == 0 && tot.reset == 0);
    CHECK(gone);                                            /* retired on its FIN, exactly once */
    CHECK(!moq_pq_conn_is_fatal(r.ad));
    rig_down(&r);
    g_end_object = 3;
    if (failures == before) printf("PASS: pq hold_input retained + deferred fin %s %s\n", lbl,
                                   fin_while_held ? "fin-while-held" : "fin-after-block");
    return failures - before;
}

/* Terminals while a chunk is held or a FIN is owed: peer RESET before the
 * redelivery, peer RESET after the redelivery landed in retained WOULD_BLOCK
 * with the FIN owed behind it, and connection close while held. Each releases
 * the retained bytes exactly once (allocator balance) and grants no credit
 * afterwards. */
static int row_terminals(moq_version_t v)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    g_end_object = 100;
    {   /* reset before redelivery */
        rig_t r;
        if (!rig_up(&r, v)) {
            CHECK(false && "rig setup");
            rig_down(&r);
            return failures - before;
        }
        moq_fetch_t fh[4], sfh[4];
        CHECK(fetch_all(&r, 4, fh, sfh));
        obs_t co;
        fill_pool(&r, sfh, &co);
        uint64_t sid = capture_response(&r, sfh[2], true);
        r.capture_opid = UINT64_MAX;
        deliver(&r, sid, r.cap, r.cap_len, true);
        moq_pq_test_rx_t st;
        CHECK(moq_pq_test_rx_state(r.ad, sid, &st) && st.held_input);
        int grants = fc_open_calls(sid);
        long bal = g_bal;
        g_reset_code = 0x7;
        moq_pq_callback(NULL, sid, NULL, 0, picoquic_callback_stream_reset, r.ad, NULL);
        CHECK(!moq_pq_test_rx_state(r.ad, sid, &st));                   /* entry gone */
        CHECK(g_bal == bal - 1);                                         /* the retention buffer freed */
        CHECK(!moq_pq_conn_is_fatal(r.ad));
        CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
        pump(&r, 3);
        memset(&co, 0, sizeof(co)); drain(r.c, &co);
        CHECK(co.complete == 1 && co.object == 0);                       /* only live 0 completed */
        CHECK(fc_open_calls(sid) == grants);                             /* no post-terminal credit */
        CHECK(!moq_pq_test_rx_state(r.ad, sid, &st));
        rig_down(&r);
    }
    {   /* reset after the redelivery was admitted into retained WOULD_BLOCK with the FIN owed */
        rig_t r;
        if (!rig_up(&r, v)) {
            CHECK(false && "rig setup");
            rig_down(&r);
            return failures - before;
        }
        moq_fetch_t fh[4], sfh[4];
        CHECK(fetch_all(&r, 4, fh, sfh));
        obs_t co;
        fill_pool(&r, sfh, &co);
        uint64_t s2 = capture_response(&r, sfh[2], false);
        uint8_t h2[512]; size_t hl = r.cap_len; memcpy(h2, r.cap, hl);
        r.cap_len = 0;
        deliver(&r, s2, h2, hl, false);
        for (int i = 1; i < 70; i++) {
            CHECK(write_obj(r.sv, sfh[2], (uint64_t)i) == MOQ_OK);
            if (i % 16 == 15) pump(&r, 1);
        }
        pump(&r, 1);
        uint8_t rest[8192]; size_t rl = r.cap_len; memcpy(rest, r.cap, rl);
        r.capture_opid = UINT64_MAX;
        deliver(&r, s2, rest, rl, false);
        deliver(&r, s2, NULL, 0, true);
        CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
        pump(&r, 2);
        moq_pq_test_rx_t st;
        CHECK(moq_pq_test_rx_state(r.ad, s2, &st) && !st.held_input && st.paused && st.blocked > 0);
        int grants = fc_open_calls(s2);
        long bal = g_bal;
        memset(&co, 0, sizeof(co)); drain(r.c, &co);
        g_reset_code = 0x7;
        moq_pq_callback(NULL, s2, NULL, 0, picoquic_callback_stream_reset, r.ad, NULL);
        CHECK(!moq_pq_test_rx_state(r.ad, s2, &st));
        CHECK(g_bal <= bal);
        for (int i = 0; i < 6; i++) { (void)moq_pq_service(r.ad, g_time); memset(&co, 0, sizeof(co)); drain(r.c, &co); }
        CHECK(fc_open_calls(s2) == grants);
        CHECK(!moq_pq_test_rx_state(r.ad, s2, &st));
        CHECK(!moq_pq_conn_is_fatal(r.ad));
        rig_down(&r);
    }
    {   /* connection close while held: bridge closed, then teardown balances */
        rig_t r;
        if (!rig_up(&r, v)) {
            CHECK(false && "rig setup");
            rig_down(&r);
            return failures - before;
        }
        moq_fetch_t fh[4], sfh[4];
        CHECK(fetch_all(&r, 4, fh, sfh));
        obs_t co;
        fill_pool(&r, sfh, &co);
        uint64_t sid = capture_response(&r, sfh[2], true);
        r.capture_opid = UINT64_MAX;
        deliver(&r, sid, r.cap, r.cap_len, true);
        moq_pq_test_rx_t st;
        CHECK(moq_pq_test_rx_state(r.ad, sid, &st) && st.held_input);
        int grants = fc_open_calls(sid);
        moq_pq_callback(NULL, 0, NULL, 0, picoquic_callback_close, r.ad, NULL);
        CHECK(moq_pq_conn_is_closed(r.ad) || moq_pq_conn_is_fatal(r.ad));
        (void)moq_pq_service(r.ad, g_time);
        CHECK(fc_open_calls(sid) == grants);
        rig_down(&r);                                                   /* balance 0 */
    }
    g_end_object = 3;
    if (failures == before) printf("PASS: pq hold_input terminals %s\n", lbl);
    return failures - before;
}

/* Allocation failure while retaining the refused chunk, and the exact budget
 * boundary. Both report the adapter's explicit failure contract (a transport
 * error on the bridge, the connection torn down), never acceptance; the
 * chunk is not claimed and nothing leaks. */
static int row_alloc_and_budget(moq_version_t v)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    {   /* allocation failure on the retention buffer */
        rig_t r;
        if (!rig_up(&r, v)) {
            CHECK(false && "rig setup");
            rig_down(&r);
            return failures - before;
        }
        moq_fetch_t fh[4], sfh[4];
        CHECK(fetch_all(&r, 4, fh, sfh));
        obs_t co;
        fill_pool(&r, sfh, &co);
        uint64_t sid = capture_response(&r, sfh[2], true);
        r.capture_opid = UINT64_MAX;
        g_fail_realloc_after = g_realloc_calls;             /* the very next realloc fails */
        deliver(&r, sid, r.cap, r.cap_len, true);
        moq_pq_test_rx_t st;
        bool tracked = moq_pq_test_rx_state(r.ad, sid, &st);
        printf("alloc-fail %s: fatal=%d tracked=%d held=%d buf_len=%zu\n", lbl, (int)moq_pq_conn_is_fatal(r.ad),
               (int)tracked, tracked ? (int)st.held_input : -1, tracked ? st.buf_len : 0);
        CHECK(moq_pq_conn_is_fatal(r.ad));                               /* explicit, not silent */
        CHECK(!tracked || (!st.held_input && st.buf_len == 0));         /* nothing claimed as held */
        CHECK(g_fail_realloc_after == -1);                               /* the injected failure fired */
        rig_down(&r);
    }
    {   /* budget boundary: a refused chunk exactly the window is held; one byte past it is impossible */
        for (int over = 0; over < 2; over++) {
            rig_t r;
            if (!rig_up(&r, v)) {
                CHECK(false && "rig setup");
                rig_down(&r);
                return failures - before;
            }
            moq_fetch_t fh[4], sfh[4];
            CHECK(fetch_all(&r, 4, fh, sfh));
            obs_t co;
            fill_pool(&r, sfh, &co);
            uint64_t sid = capture_response(&r, sfh[2], true);
            r.capture_opid = UINT64_MAX;
            /* the stream's budget is read on its first sight: set it now */
            g_rx_window = over ? r.cap_len - 1 : r.cap_len;
            deliver(&r, sid, r.cap, r.cap_len, true);
            moq_pq_test_rx_t st;
            bool tracked = moq_pq_test_rx_state(r.ad, sid, &st);
            printf("budget %s %s: window=%llu chunk=%zu fatal=%d held=%d\n", lbl, over ? "over" : "exact",
                   (unsigned long long)g_rx_window, r.cap_len, (int)moq_pq_conn_is_fatal(r.ad),
                   tracked ? (int)st.held_input : -1);
            if (over) {
                CHECK(moq_pq_conn_is_fatal(r.ad));
                CHECK(!tracked || !st.held_input);
            } else {
                CHECK(!moq_pq_conn_is_fatal(r.ad));
                CHECK(tracked && st.held_input && st.held_len == r.cap_len && st.budget == r.cap_len);
                CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
                pump(&r, 3);
                memset(&co, 0, sizeof(co)); drain(r.c, &co);
                CHECK(co.complete == 2 && inventory_exact(&co, 1));
            }
            g_rx_window = 65535;
            rig_down(&r);
        }
    }
    if (failures == before) printf("PASS: pq hold_input alloc/budget %s\n", lbl);
    return failures - before;
}

/* Capability absence is rejected before bridge effects, not mutated after create. */
static int row_required_hold(moq_version_t v)
{
    int before = failures;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_SERVER);
    scfg.version = v;
    moq_session_t *session = NULL;
    moq_result_t rc = moq_session_create(&scfg, 0, &session);
    CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return failures - before;
    fake_endpoint_t ep;
    fake_endpoint_init(&ep, 3, 1);
    moq_transport_bridge_cfg_t cfg;
    moq_transport_bridge_cfg_init(&cfg, moq_alloc_default());
    moq_transport_bridge_t *bridge = NULL;
    CHECK(moq_transport_bridge_create(&cfg, session, &ep.vtable, &ep,
                                        &bridge) == MOQ_ERR_UNSUPPORTED);
    CHECK(bridge == NULL && ep.count == 0);
    moq_transport_bridge_destroy(bridge);
    moq_session_destroy(session);
    if (failures == before) printf("PASS: pq required HOLD_INPUT\n");
    return failures - before;
}

/* -- held-stream cancellation at the hold age (HOLD_BIDI_INPUT) ------------ *
 *
 * The bridge cancels a held response that reaches the hold age through
 * stop_sending. What the adapter does depends on its own record of the
 * stream's receive half:
 *   held FIN    -- the held chunk carried the FIN;
 *   later FIN   -- a FIN arrived behind the held chunk while it was held;
 *   peer RESET  -- the peer reset the stream while it was held;
 *   live        -- none of these (bytes queued behind the held chunk).
 * A stream terminal at the transport is discarded locally: no STOP_SENDING
 * reaches picoquic (which here refuses every STOP, as for a stream it already
 * released), the held bytes are dropped and never delivered, and nothing is
 * fatal. A live stream gets exactly one STOP_SENDING with the profile's code,
 * after which the held bytes and everything queued behind them are dropped
 * and never replayed; picoquic refusing that STOP is fatal. */
typedef enum { TC_HELD_FIN, TC_LATER_FIN, TC_PEER_RESET, TC_LIVE, TC_LIVE_REFUSED } tc_shape_t;

static const char *tc_name(tc_shape_t t)
{
    switch (t) {
    case TC_HELD_FIN:     return "held FIN";
    case TC_LATER_FIN:    return "later FIN";
    case TC_PEER_RESET:   return "peer RESET";
    case TC_LIVE:         return "live";
    case TC_LIVE_REFUSED: return "live, STOP refused";
    }
    return "?";
}

static int row_terminal_cancel(moq_version_t v, tc_shape_t shape)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    const uint64_t code = v == MOQ_VERSION_DRAFT_16 ? MOQ_STREAM_ERROR_CANCELLED
                                                    : MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
    rig_t r;
    const uint64_t t0 = g_time;              /* restored: later rows start where they did */
    moq_pq_test_create_with_hold_bidi(true);
    bool up = rig_up(&r, v);
    moq_pq_test_create_with_hold_bidi(false);
    if (!up) {
        CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[4], sfh[4];
    CHECK(fetch_all(&r, 4, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);
    uint64_t sid = capture_response(&r, sfh[2], true);
    if (sid == UINT64_MAX) { rig_down(&r); return failures - before; }
    uint8_t all[8192]; size_t total = r.cap_len; memcpy(all, r.cap, total);
    r.capture_opid = UINT64_MAX;
    r.hold_client = true;                    /* the publisher never sees the STOP */

    /* The refused chunk, and what arrives behind it while it is held. */
    const uint64_t held_at = g_time;
    if (shape == TC_HELD_FIN) {
        deliver(&r, sid, all, total, true);
    } else {
        deliver(&r, sid, all, 2, false);     /* the 2-byte header write */
        if (shape == TC_LATER_FIN)
            deliver(&r, sid, all + 2, total - 2, true);
        else if (shape == TC_LIVE || shape == TC_LIVE_REFUSED)
            deliver(&r, sid, all + 2, total - 2, false);
    }
    moq_pq_test_rx_t st;
    CHECK(moq_pq_test_rx_state(r.ad, sid, &st) && st.held_input);
    if (shape == TC_PEER_RESET) {
        g_reset_code = 0x2;
        moq_pq_callback(NULL, sid, NULL, 0, picoquic_callback_stream_reset, r.ad, NULL);
        CHECK(!moq_pq_test_rx_state(r.ad, sid, &st));
    }
    int stops_before = g_stop_n;
    g_stop_refused = shape != TC_LIVE;
    int grants_before = fc_open_calls(sid);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);

    /* The hold age passes. */
    g_time = held_at + 5000000;
    (void)moq_pq_service(r.ad, g_time);
    g_time += 10;
    (void)moq_pq_service(r.ad, g_time);
    obs_t after;
    memset(&after, 0, sizeof(after));
    drain(r.c, &after);
    int stops = g_stop_n - stops_before;
    bool tracked = moq_pq_test_rx_state(r.ad, sid, &st);
    printf("terminal cancel %s %s: stops=%d tracked=%d held=%d fatal=%d objects=%d complete=%d\n", lbl,
           tc_name(shape), stops, (int)tracked, tracked ? (int)st.held_input : -1,
           (int)moq_pq_conn_is_fatal(r.ad), after.object, after.complete);

    if (shape == TC_LIVE_REFUSED) {
        CHECK(stops == 1);
        CHECK(moq_pq_conn_is_fatal(r.ad));
    } else {
        CHECK(!moq_pq_conn_is_fatal(r.ad));
        CHECK(moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
        if (shape == TC_LIVE) {
            CHECK(stops == 1);
            CHECK(stops == 1 && g_stops[stops_before].sid == sid &&
                  g_stops[stops_before].code == code);
        } else {
            CHECK(stops == 0);               /* no STOP_SENDING reaches picoquic */
        }
        if (shape == TC_LIVE) {
            CHECK(tracked && !st.held_input && !st.paused && st.buf_len == 0 && st.release_watch);
            CHECK(fc_open_calls(sid) == grants_before);
            CHECK(deliver(&r, sid, all + 2, total - 2, false) == 0);
        } else {
            CHECK(!tracked);
        }
        CHECK(co.object == 0 && after.object == 0 && after.complete == 0);   /* never delivered */
        /* Later passes replay nothing and stop nothing more. */
        g_time += 10;
        pump(&r, 2);
        memset(&after, 0, sizeof(after));
        drain(r.c, &after);
        CHECK(after.object == 0 && after.complete == 0 && after.error == 0);
        if (shape == TC_LIVE) {
            CHECK(moq_pq_test_rx_state(r.ad, sid, &st) && !st.held_input && st.buf_len == 0);
            CHECK(fc_open_calls(sid) == grants_before);
            uint64_t terminals = moq_pq_test_release_terminals(r.ad);
            CHECK(moq_pq_callback(NULL, sid, NULL, 0, picoquic_callback_stream_released,
                                  r.ad, r.ad) == 0);
            CHECK(moq_pq_test_release_terminals(r.ad) == terminals + 1);
        }
        CHECK(!moq_pq_test_rx_state(r.ad, sid, &st));
        CHECK(g_stop_n - stops_before == stops);
        CHECK(!moq_pq_conn_is_fatal(r.ad));
    }
    g_stop_refused = false;
    rig_down(&r);
    g_time = t0;
    if (failures == before) printf("PASS: pq hold_input terminal cancel %s %s\n", lbl, tc_name(shape));
    return failures - before;
}

/* The transport parameters are fixed at attach: the adapter samples the
 * peer-unidirectional stream credit then, and a service pass that finds it
 * changed (a later picoquic_set_transport_parameters) fails the connection
 * closed. Unchanged parameters leave it running. */
static int row_tp_fixed(moq_version_t v, bool change)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    rig_t r;
    if (!rig_up(&r, v)) {
        CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    CHECK(!moq_pq_conn_is_fatal(r.ad));
    if (change) g_tp_streams_uni = 65;
    (void)moq_pq_service(r.ad, g_time);
    printf("tp fixed %s %s: fatal=%d\n", lbl, change ? "changed" : "unchanged", (int)moq_pq_conn_is_fatal(r.ad));
    CHECK(moq_pq_conn_is_fatal(r.ad) == change);
    g_tp_streams_uni = 64;
    rig_down(&r);
    if (failures == before)
        printf("PASS: pq hold_input transport parameters fixed at attach %s %s\n", lbl,
               change ? "changed" : "unchanged");
    return failures - before;
}

/* Unreadable local transport parameters refuse the attach before anything
 * is allocated or any callback is taken; the session is untouched and a
 * later attach succeeds. Defensive: picoquic returns the parameters for
 * every valid connection, so only the stub reaches this. */
static int row_tp_unreadable(moq_version_t v)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    moq_alloc_t al = (moq_alloc_t){ NULL, ca, cr, cf };
    moq_session_cfg_t cc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    cc.version = v;
    moq_session_t *sess = NULL;
    CHECK(moq_session_create(&cc, 0, &sess) == MOQ_OK);
    if (!sess) return failures - before;
    long bal0 = g_bal;
    int cb0 = g_set_cb_n;
    moq_pq_conn_cfg_t ac;
    moq_pq_conn_cfg_init_sized(&ac, sizeof(ac));
    ac.session = sess;
    ac.cnx = (picoquic_cnx_t *)(uintptr_t)0xDEAD;
    ac.alloc = &al;
    moq_pq_conn_t *ad = (moq_pq_conn_t *)(uintptr_t)1;
    g_tp_null = true;
    int rc = moq_pq_conn_create(&ac, &ad);
    g_tp_null = false;
    printf("tp unreadable %s: rc=%d out=%p callbacks=%d balance=%ld\n", lbl, rc, (void *)ad,
           g_set_cb_n - cb0, g_bal - bal0);
    CHECK(rc == -1);
    CHECK(ad == NULL);
    CHECK(g_set_cb_n == cb0);
    CHECK(g_bal == bal0);
    CHECK(moq_pq_conn_create(&ac, &ad) == 0 && ad != NULL);
    if (ad) moq_pq_conn_destroy(ad);
    moq_session_destroy(sess);
    CHECK(g_bal == bal0);
    if (failures == before) printf("PASS: pq hold_input unreadable transport parameters %s\n", lbl);
    return failures - before;
}

/* Drafts 18 and 22: the fetcher's one-slot event queue holds its unpolled
 * FETCH_OK, so the publisher's GOAWAY on the control stream is retained by
 * the session and the bridge waits for it to drain. A response arriving
 * meanwhile on another stream is refused whole: the adapter keeps exactly
 * that chunk (bytes and FIN) and redelivers it once the control input
 * drains, and the fetch then completes with its exact object. */
static int row_deferred_behind_control(moq_version_t v, bool with_fin)
{
    const char *vl = v == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    const char *lbl = with_fin ? "with FIN" : "no FIN";
    rig_t r;
    g_client_max_events = 1;
    bool up = rig_up(&r, v);
    g_client_max_events = 64;
    if (!up) {
        CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[1], sfh[1];
    bool fetched = fetch_all(&r, 1, fh, sfh);
    CHECK(fetched);
    if (!fetched) { rig_down(&r); return failures - before; }

    /* The response data stream is opened by the accept itself (with its
     * FETCH_HEADER), so the capture is armed on the next server uni id
     * before the accept: the header, the object and (with_fin) the FIN on
     * that stream are captured instead of delivered, while FETCH_OK flows
     * and fills the fetcher's single event slot, unpolled. */
    uint64_t opid = r.sep.next_uni_id;
    r.capture_opid = opid; r.cap_len = 0; r.cap_fin = false;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = g_end_object;
    moq_result_t arc = moq_session_accept_fetch(r.sv, sfh[0], &ac, 0);
    CHECK(arc == MOQ_OK);
    if (arc != MOQ_OK) { rig_down(&r); return failures - before; }
    pump(&r, 3);                              /* FETCH_OK fills the fetcher's queue */

    CHECK(write_obj(r.sv, sfh[0], 0) == MOQ_OK);
    if (with_fin) CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 2);
    uint64_t sid = rig_sid(&r, opid);
    uint8_t all[8192]; size_t total = r.cap_len; memcpy(all, r.cap, total);
    bool fin = r.cap_fin;
    r.capture_opid = UINT64_MAX;
    CHECK(sid != UINT64_MAX);
    CHECK(total > 1 && all[0] == 0x05);       /* FETCH_HEADER type */
    CHECK(fin == with_fin);
    if (sid == UINT64_MAX || total < 2 || all[0] != 0x05 || fin != with_fin) {
        rig_down(&r);
        return failures - before;
    }

    /* The GOAWAY reaches the fetcher while its queue is full. */
    CHECK(moq_session_goaway(r.sv, NULL, 0, 0) == MOQ_OK);
    pump(&r, 2);

    deliver(&r, sid, all, total, fin);
    moq_pq_test_rx_t st;
    bool tracked = moq_pq_test_rx_state(r.ad, sid, &st);
    printf("deferred behind control pq %s %s: tracked=%d held=%d held_len=%zu held_fin=%d blocked=%llu\n", vl, lbl,
           (int)tracked, tracked ? (int)st.held_input : -1, tracked ? st.held_len : 0,
           tracked ? (int)st.held_fin : -1, tracked ? (unsigned long long)st.blocked : 0ull);
    CHECK(tracked && st.held_input && st.held_len == total && st.held_fin == fin);
    CHECK(tracked && st.blocked == 0);       /* nothing claimed as bridge-owned */
    CHECK(tracked && st.buf_len == total && st.buf && memcmp(st.buf, all, total) == 0);
    CHECK(!moq_pq_conn_is_fatal(r.ad));

    /* The fetcher polls its events: the control input drains, then the
     * held chunk is redelivered exactly. */
    obs_t co, sum;
    memset(&sum, 0, sizeof(sum));
    for (int i = 0; i < 8; i++) {
        memset(&co, 0, sizeof(co));
        drain(r.c, &co);
        obs_merge(&sum, &co);
        pump(&r, 1);
    }
    printf("deferred behind control pq %s %s replay: objects=%d complete=%d dup=%d bad=%d fatal=%d\n", vl, lbl,
           sum.object, sum.complete, sum.duplicates, sum.bad_payload, (int)moq_pq_conn_is_fatal(r.ad));
    CHECK(inventory_exact(&sum, 1));
    CHECK(sum.complete == (with_fin ? 1 : 0));
    CHECK(sum.error == 0 && sum.reset == 0);
    CHECK(!moq_pq_test_rx_state(r.ad, sid, &st) || !st.held_input);
    CHECK(!moq_pq_conn_is_fatal(r.ad));
    rig_down(&r);
    if (failures == before) printf("PASS: pq hold_input deferred behind control %s %s\n", vl, lbl);
    return failures - before;
}

/* Draft 22 needs this picoquic's exact stream terminal outcomes
 * (PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1). Without them the attach
 * refuses a draft-22 session before allocating or taking any callback, while
 * a draft-18 session on the same configuration attaches. */
#if defined(PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION) && \
    PICOQUIC_STREAM_TERMINAL_CONTRACT_VERSION >= 1
#define HOSTS_D22 1
#else
#define HOSTS_D22 0
static int row_d22_attach_refused(void)
{
    int before = failures;
    for (int i = 0; i < 2; i++) {
        moq_version_t v = i ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_22;
        moq_session_cfg_t cc;
        moq_session_cfg_init_sized(&cc, sizeof(cc), moq_alloc_default(),
                                   MOQ_PERSPECTIVE_CLIENT);
        cc.version = v;
        moq_session_t *sess = NULL;
        CHECK(moq_session_create(&cc, 0, &sess) >= 0 && sess);
        if (!sess) continue;
        CHECK(test_bind_if_required(sess) >= 0);
        int calls = g_set_cb_n;
        moq_pq_conn_cfg_t ac;
        moq_pq_conn_cfg_init_sized(&ac, sizeof(ac));
        ac.session = sess;
        ac.cnx = (picoquic_cnx_t *)(uintptr_t)0xDEAD;
        ac.alloc = moq_alloc_default();
        moq_pq_conn_t *ad = (moq_pq_conn_t *)(uintptr_t)1;
        int rc = moq_pq_conn_create(&ac, &ad);
        if (v == MOQ_VERSION_DRAFT_22) {
            CHECK(rc != 0 && ad == NULL);
            CHECK(g_set_cb_n == calls);   /* no callback taken */
        } else {
            CHECK(rc == 0 && ad != NULL);
            if (ad) moq_pq_conn_destroy(ad);
        }
        moq_session_destroy(sess);
    }
    if (failures == before) printf("PASS: pq hold_input draft-22 attach refused without terminal contract\n");
    return failures - before;
}
#endif

/* The endpoint's reset and stop contract against the provider's answers,
 * with nothing real behind it: every refusal is classified by the provider
 * before the endpoint marks, binds or drops anything, so an ID the provider
 * never opened -- even one past the 62-bit range -- cannot be created by the
 * endpoint's own bookkeeping. A part already ended is a no-op; an accepted
 * reset abandons the stream's queue and active mark only afterwards. */
static int g_rx_stopped_n = 0;
static int count_rx_stopped(void *owner, uint64_t sid)
    { (void)owner; (void)sid; g_rx_stopped_n++; return 0; }

static int row_endpoint_classification(void)
{
    int before = failures;
    moq_transport_endpoint_ops_t ops;
    pq_endpoint_ctx_t ep;
    memset(&ops, 0, sizeof(ops));
    memset(&ep, 0, sizeof(ep));
    static int cnx_tag;
    CHECK(pq_endpoint_init(&ops, &ep, (picoquic_cnx_t *)&cnx_tag,
                           moq_alloc_default(), 0) == 0);
    ep.rx_stopped = count_rx_stopped;
    g_rx_stopped_n = 0;
    g_active_n = 0;
    g_reset_rc = g_stop_rc = 0;
    g_stop_refused = false;

    /* An open reserves its ID at the provider before returning it. */
    uint64_t uni = UINT64_MAX, bidi = UINT64_MAX, bidi2 = UINT64_MAX;
    calls_clear();
    CHECK(ops.open_uni(&ep, &uni) == MOQ_TRANSPORT_OK);
    CHECK(g_calls[0] == 'C');
    calls_clear();
    CHECK(ops.open_bidi(&ep, &bidi) == MOQ_TRANSPORT_OK);
    CHECK(strcmp(g_calls, "C") == 0);
    CHECK(ops.open_bidi(&ep, &bidi2) == MOQ_TRANSPORT_OK);
    CHECK(bidi2 != bidi);

    static const uint8_t bytes[3] = { 1, 2, 3 };
    CHECK(ops.write(&ep, uni, bytes, sizeof(bytes), false) == MOQ_TRANSPORT_OK);
    CHECK(moq_pq_send_queue_queued_bytes(ep.queue) == sizeof(bytes));
    CHECK(g_active_n == 1 && g_active[0] == uni);

    /* Past the 62-bit range, and refused: classified first, nothing marked
     * or bound. */
    const uint64_t huge = ((uint64_t)1 << 62) | 3;
    g_reset_rc = PICOQUIC_ERROR_INVALID_STREAM_ID;
    calls_clear();
    CHECK(ops.reset_stream(&ep, huge, 0x1) == MOQ_TRANSPORT_ERROR);
    CHECK(strcmp(g_calls, "R") == 0);
    CHECK(g_last_reset_sid == huge && g_last_reset_code == 0x1);
    CHECK(moq_pq_send_queue_queued_bytes(ep.queue) == sizeof(bytes));
    CHECK(g_active_n == 1 && g_active[0] == uni);
    /* A refused reset of a stream with queued bytes leaves them queued. */
    calls_clear();
    CHECK(ops.reset_stream(&ep, uni, 0x1) == MOQ_TRANSPORT_ERROR);
    CHECK(strcmp(g_calls, "R") == 0);
    CHECK(moq_pq_send_queue_has_data(ep.queue, uni));
    CHECK(moq_pq_send_queue_queued_bytes(ep.queue) == sizeof(bytes));
    CHECK(g_active_n == 1 && g_active[0] == uni);

    /* A sending part already ended: OK, and nothing else happens. */
    g_reset_rc = PICOQUIC_ERROR_STREAM_ALREADY_CLOSED;
    calls_clear();
    CHECK(ops.reset_stream(&ep, uni, 0x1) == MOQ_TRANSPORT_OK);
    CHECK(strcmp(g_calls, "R") == 0);
    CHECK(moq_pq_send_queue_has_data(ep.queue, uni));
    CHECK(moq_pq_send_queue_queued_bytes(ep.queue) == sizeof(bytes));
    CHECK(g_active_n == 1 && g_active[0] == uni);

    /* Accepted: only then are the queue and the active mark abandoned. */
    g_reset_rc = 0;
    calls_clear();
    CHECK(ops.reset_stream(&ep, uni, 0x1) == MOQ_TRANSPORT_OK);
    CHECK(strcmp(g_calls, "RM") == 0);
    CHECK(!moq_pq_send_queue_has_data(ep.queue, uni));
    CHECK(g_active_n == 0);

    CHECK(ops.write(&ep, bidi, bytes, sizeof(bytes), false) == MOQ_TRANSPORT_OK);

    /* STOP: a receiving part already terminal is a no-op with no watch; an
     * unknown, out-of-range or send-only ID, or a provider failure, stays an
     * ERROR with no watch; an accepted stop is watched. */
    g_stop_rc = PICOQUIC_ERROR_STREAM_ALREADY_CLOSED;
    calls_clear();
    CHECK(ops.stop_sending(&ep, bidi, 0x1) == MOQ_TRANSPORT_OK);
    CHECK(strcmp(g_calls, "S") == 0);
    CHECK(g_rx_stopped_n == 0);
    CHECK(moq_pq_send_queue_queued_bytes(ep.queue) == sizeof(bytes));
    CHECK(g_active_n == 1 && g_active[0] == bidi);
    g_stop_rc = PICOQUIC_ERROR_INVALID_STREAM_ID;
    calls_clear();
    CHECK(ops.stop_sending(&ep, huge, 0x1) == MOQ_TRANSPORT_ERROR);
    CHECK(strcmp(g_calls, "S") == 0);
    CHECK(g_last_stop_sid == huge);
    CHECK(g_rx_stopped_n == 0);
    CHECK(moq_pq_send_queue_queued_bytes(ep.queue) == sizeof(bytes));
    CHECK(g_active_n == 1 && g_active[0] == bidi);
    g_stop_rc = 0;
    g_stop_refused = true;
    CHECK(ops.stop_sending(&ep, bidi, 0x1) == MOQ_TRANSPORT_ERROR);
    CHECK(g_rx_stopped_n == 0);
    g_stop_refused = false;
    calls_clear();
    CHECK(ops.stop_sending(&ep, bidi, 0x1) == MOQ_TRANSPORT_OK);
    CHECK(strcmp(g_calls, "S") == 0);
    CHECK(g_rx_stopped_n == 1);
    CHECK(moq_pq_send_queue_queued_bytes(ep.queue) == sizeof(bytes));
    CHECK(g_active_n == 1 && g_active[0] == bidi);

    g_reset_rc = 0;
    g_stop_rc = 0;
    pq_endpoint_cleanup(&ep);
    g_active_n = 0;
    return failures - before;
}

int main(void)
{
    moq_version_t versions[3] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                  MOQ_VERSION_DRAFT_22 };
    const int nv = HOSTS_D22 ? 3 : 2;
#if !HOSTS_D22
    (void)row_d22_attach_refused();
#endif
    for (int i = 0; i < nv; i++) {
        (void)row_admission(versions[i], false);
        (void)row_admission(versions[i], true);
        (void)row_repeat(versions[i]);
        (void)row_retained_and_deferred_fin(versions[i], true);
        (void)row_retained_and_deferred_fin(versions[i], false);
        (void)row_terminals(versions[i]);
        (void)row_alloc_and_budget(versions[i]);
        (void)row_required_hold(versions[i]);
        for (int t = TC_HELD_FIN; t <= TC_LIVE_REFUSED; t++)
            (void)row_terminal_cancel(versions[i], (tc_shape_t)t);
        (void)row_tp_fixed(versions[i], false);
        (void)row_tp_fixed(versions[i], true);
        (void)row_tp_unreadable(versions[i]);
        (void)row_production_bounded_admission(versions[i]);
        if (versions[i] != MOQ_VERSION_DRAFT_16)
            (void)row_production_request_admission(versions[i]);
        if (versions[i] != MOQ_VERSION_DRAFT_16) {
            (void)row_deferred_admission(versions[i], false, 0, true);
            (void)row_deferred_admission(versions[i], true, 0, true);
        }
    }
    for (int i = 1; i < nv; i++) {
        (void)row_prefix(versions[i]);
        (void)row_deferred_behind_control(versions[i], false);
        (void)row_deferred_behind_control(versions[i], true);
    }
    (void)row_endpoint_classification();
    (void)row_deferred_admission(versions[nv - 1], false, -1, true);
    (void)row_deferred_admission(versions[nv - 1], true,
                                PICOQUIC_ERROR_STREAM_ALREADY_CLOSED, true);
    (void)row_deferred_admission(versions[nv - 1], true, 0, false);
    (void)row_production_request_admission_mode(versions[nv - 1], true);
    if (failures == 0) printf("PASS: test_pq_hold_input\n");
    else fprintf(stderr, "FAILED: test_pq_hold_input (%d)\n", failures);
    return failures ? 1 : 0;
}
