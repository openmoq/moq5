/*
 * picoquic WebTransport adapter: receive admission (MOQ_TRANSPORT_CAP_HOLD_INPUT).
 *
 * The fetcher session is attached to the adapter and driven through the
 * real bridge with picoquic/h3zero stubbed out: inbound WT bytes arrive
 * through moq_pico_wt_callback (post_data / post_fin / reset), outbound
 * bytes are pulled through the provide_data path. The publisher is a raw
 * session over the fake transport endpoint; this file relays between the
 * two. Every row runs on draft-16 and draft-18.
 *
 * The rows pin, for a response stream the session cannot admit yet: the
 * adapter keeps exactly the refused chunk (bytes and FIN) under the stream's
 * window budget, freezes credit, appends later bytes/FIN behind it in order,
 * redelivers it once (and holds it again, same allocation, when another
 * stream took the entry first), never replays a classification prefix the
 * bridge already owns, settles a FIN owed behind bytes the session later
 * retained from service passes alone, and releases everything exactly once
 * on reset/deregister/teardown with no credit granted after a terminal.
 * Allocation failure and the budget boundary report the adapter's explicit
 * failure contract instead of claiming acceptance.
 */

#include "../pico_wt_adapter.h"
#include "pico_wt_test_seam.h"
#include <moq/session.h>
#include <moq/rcbuf.h>
#include <moq/transport_bridge.h>
#include "held_bridge_driver.h"

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

/* -- picoquic / h3zero / picowt stubs ----------------------------------- */

static uint64_t g_time = 1000;
static uint64_t g_next_uni = 2;     /* client-initiated uni: 2, 6, 10 .. */
static uint64_t g_next_bidi = 4;    /* client-initiated bidi after the WT CONNECT stream 0 */
static uint64_t g_reset_code = 0;
static uint64_t g_rx_window = 65535;
static picoquic_state_enum g_cnx_state = picoquic_state_ready;

picoquic_quic_t *picoquic_get_quic_ctx(picoquic_cnx_t *c) { (void)c; return NULL; }
uint64_t picoquic_get_quic_time(picoquic_quic_t *q) { (void)q; return g_time; }
picoquic_state_enum picoquic_get_cnx_state(picoquic_cnx_t *c) { (void)c; return g_cnx_state; }
uint64_t picoquic_get_remote_stream_error(picoquic_cnx_t *c, uint64_t sid) { (void)c; (void)sid; return g_reset_code; }
int picoquic_is_client(picoquic_cnx_t *c) { (void)c; return 1; }
int picoquic_set_stream_priority(picoquic_cnx_t *c, uint64_t sid, uint8_t p) { (void)c; (void)sid; (void)p; return 0; }
size_t picoquic_frames_varint_encode_length(uint64_t n) { return n < 64 ? 1 : n < 16384 ? 2 : n < 1073741824ull ? 4 : 8; }
picoquic_tp_t const *picoquic_get_transport_parameters(picoquic_cnx_t *c, int get_local)
    { (void)c; (void)get_local; static picoquic_tp_t tp; memset(&tp, 0, sizeof(tp));
      tp.max_datagram_frame_size = 1252;
      tp.initial_max_stream_data_uni = g_rx_window;
      tp.initial_max_stream_data_bidi_remote = g_rx_window;
      tp.initial_max_stream_data_bidi_local = g_rx_window;
      return &tp; }

/* h3zero stream contexts: a pool keyed by stream id */
#define SC_MAX 96
static h3zero_stream_ctx_t g_sc[SC_MAX];
static int g_sc_n = 0;
static h3zero_stream_ctx_t *sc_get(uint64_t sid)
{
    for (int i = 0; i < g_sc_n; i++) if (g_sc[i].stream_id == sid) return &g_sc[i];
    if (g_sc_n >= SC_MAX) return NULL;
    h3zero_stream_ctx_t *sc = &g_sc[g_sc_n++];
    memset(sc, 0, sizeof(*sc));
    sc->stream_id = sid;
    return sc;
}
static h3zero_callback_ctx_t g_h3;
static h3zero_stream_prefix_t g_prefix;
h3zero_stream_ctx_t *picowt_create_local_stream(picoquic_cnx_t *cnx, int is_bidir, h3zero_callback_ctx_t *h3_ctx,
                                                uint64_t control_stream_id)
{
    (void)cnx; (void)h3_ctx; (void)control_stream_id;
    uint64_t id = is_bidir ? g_next_bidi : g_next_uni;
    if (is_bidir) g_next_bidi += 4; else g_next_uni += 4;
    return sc_get(id);
}
h3zero_stream_ctx_t *h3zero_find_stream(h3zero_callback_ctx_t *ctx, uint64_t stream_id)
{
    (void)ctx;
    for (int i = 0; i < g_sc_n; i++) if (g_sc[i].stream_id == stream_id) return &g_sc[i];
    return NULL;
}
h3zero_stream_prefix_t *h3zero_find_stream_prefix(h3zero_callback_ctx_t *ctx, uint64_t prefix)
    { (void)ctx; (void)prefix; return &g_prefix; }
void h3zero_delete_stream_prefix(picoquic_cnx_t *cnx, h3zero_callback_ctx_t *ctx, uint64_t prefix)
    { (void)cnx; (void)ctx; (void)prefix; }
int h3zero_set_datagram_ready(picoquic_cnx_t *cnx, uint64_t stream_id) { (void)cnx; (void)stream_id; return 0; }
static uint8_t g_dg_buf[2048];
uint8_t *h3zero_provide_datagram_buffer(void *context, size_t length, int ready_to_send)
    { (void)context; (void)ready_to_send; return length <= sizeof(g_dg_buf) ? g_dg_buf : NULL; }
void picowt_deregister(picoquic_cnx_t *cnx, h3zero_callback_ctx_t *h3_ctx, h3zero_stream_ctx_t *control_stream_ctx)
    { (void)cnx; (void)h3_ctx; (void)control_stream_ctx; }
int picowt_receive_capsule(picoquic_cnx_t *cnx, const uint8_t *bytes, const uint8_t *bytes_max, picowt_capsule_t *capsule)
    { (void)cnx; (void)bytes; (void)bytes_max; (void)capsule; return 0; }
void picowt_release_capsule(picowt_capsule_t *capsule) { (void)capsule; }
int picowt_send_close_session_message(picoquic_cnx_t *cnx, h3zero_stream_ctx_t *control_stream_ctx, uint32_t picowt_err,
                                      const char *err_msg)
    { (void)cnx; (void)control_stream_ctx; (void)picowt_err; (void)err_msg; return 0; }
picosplay_node_t *picosplay_first(picosplay_tree_t *tree) { (void)tree; return NULL; }
picosplay_node_t *picosplay_next(picosplay_node_t *node) { (void)node; return NULL; }

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
    if (on && g_active_n < ACT_MAX) g_active[g_active_n++] = sid;
}
int picoquic_mark_active_stream(picoquic_cnx_t *c, uint64_t sid, int active, void *v)
    { (void)c; (void)v; act_set(sid, active); return 0; }
static uint8_t g_provide_buf[4096];
static size_t g_provide_nb; static int g_provide_fin; static int g_provide_still;
uint8_t *picoquic_provide_stream_data_buffer(void *ctx, size_t nb, int is_fin, int is_still_active)
    { (void)ctx; g_provide_nb = nb; g_provide_fin = is_fin; g_provide_still = is_still_active;
      return nb <= sizeof(g_provide_buf) ? g_provide_buf : NULL; }

/* STOP_SENDING / RESET issued by the adapter (the fetcher side). */
#define SIG_MAX 64
static struct { uint64_t sid; uint64_t code; } g_stops[SIG_MAX];
static int g_stop_n = 0, g_stop_seen = 0;
static int g_reset_n = 0;
int picoquic_reset_stream(picoquic_cnx_t *c, uint64_t sid, uint64_t ec) { (void)c; (void)sid; (void)ec; g_reset_n++; return 0; }
int picowt_reset_stream(picoquic_cnx_t *cnx, h3zero_stream_ctx_t *stream_ctx, uint64_t local_stream_error)
    { (void)cnx; (void)stream_ctx; (void)local_stream_error; g_reset_n++; return 0; }
int picoquic_stop_sending(picoquic_cnx_t *c, uint64_t sid, uint64_t ec)
    { (void)c; if (g_stop_n < SIG_MAX) { g_stops[g_stop_n].sid = sid; g_stops[g_stop_n].code = ec; g_stop_n++; } return 0; }

/* Flow-control instrumentation: app-flow-control takeover and grants per stream. */
#define FC_MAX 64
static struct { uint64_t sid; int use; int open_calls; uint64_t open_total; } g_fc[FC_MAX];
static int g_fc_n = 0;
static int fc_slot(uint64_t sid)
    { for (int i = 0; i < g_fc_n; i++) if (g_fc[i].sid == sid) return i;
      if (g_fc_n < FC_MAX) { g_fc[g_fc_n].sid = sid; g_fc[g_fc_n].use = -1; return g_fc_n++; } return 0; }
static int fc_use(uint64_t sid)
    { for (int i = 0; i < g_fc_n; i++) if (g_fc[i].sid == sid) return g_fc[i].use; return -1; }
static int fc_open_calls(uint64_t sid)
    { for (int i = 0; i < g_fc_n; i++) if (g_fc[i].sid == sid) return g_fc[i].open_calls; return 0; }
int picoquic_set_app_flow_control(picoquic_cnx_t *c, uint64_t sid, int use)
    { (void)c; int s = fc_slot(sid); g_fc[s].use = use; return 0; }
int picoquic_open_flow_control(picoquic_cnx_t *c, uint64_t sid, uint64_t sz)
    { (void)c; int s = fc_slot(sid); g_fc[s].open_calls++; g_fc[s].open_total += sz; return 0; }

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

#define UNI_MAX 32
typedef struct rig {
    moq_version_t   version;
    moq_alloc_t     alloc;
    moq_session_t  *c;        /* fetcher, adapter-attached */
    moq_pico_wt_conn_t *ad;
    moq_session_t  *sv;       /* raw publisher */
    moq_transport_bridge_t *sb;
    fake_endpoint_t sep;
    /* server uni streams: fake-endpoint op id -> peer stream id on the fetcher */
    struct { uint64_t opid; uint64_t sid; } unis[UNI_MAX];
    int             n_unis;
    /* capture: server writes on this op id are collected, not delivered */
    uint64_t        capture_opid;
    uint8_t         cap[8192];
    size_t          cap_len;
    bool            cap_fin;
    bool            hold_client;   /* do not relay the fetcher's STOP_SENDING */
} rig_t;

/* receive-state snapshot, read from the adapter's own table */
typedef struct rxs {
    bool active, paused, held_input, buf_fin, fin_blocked, held_fin;
    uint64_t blocked, delivered, granted, budget;
    size_t held_len, buf_len;
    const uint8_t *buf;
} rxs_t;

static bool rx_state(rig_t *r, uint64_t sid, rxs_t *out)
{
    memset(out, 0, sizeof(*out));
    for (size_t i = 0; i < r->ad->rx_count; i++) {
        pw_rx_stream_t *st = &r->ad->rx[i];
        if (!st->active || st->stream_id != sid) continue;
        out->active = st->active; out->paused = st->paused; out->held_input = st->held_input;
        out->buf_fin = st->buf_fin; out->fin_blocked = st->fin_blocked; out->held_fin = st->held_fin;
        out->blocked = st->blocked; out->delivered = st->delivered; out->granted = st->granted;
        out->budget = st->budget; out->held_len = st->held_len; out->buf_len = st->buf_len; out->buf = st->buf;
        return true;
    }
    return false;
}

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

static picoquic_cnx_t *const CNX = (picoquic_cnx_t *)(uintptr_t)0xDEAD;

static void deliver(rig_t *r, uint64_t sid, const uint8_t *data, size_t len, bool fin)
{
    h3zero_stream_ctx_t *sc = sc_get(sid);
    (void)moq_pico_wt_callback(CNX, (uint8_t *)data, len,
                               fin ? picohttp_callback_post_fin : picohttp_callback_post_data, sc, r->ad);
}
static void reset_cb(rig_t *r, uint64_t sid)
{
    (void)moq_pico_wt_callback(CNX, NULL, 0, picohttp_callback_reset, sc_get(sid), r->ad);
}
static void deregister_cb(rig_t *r)
{
    (void)moq_pico_wt_callback(CNX, NULL, 0, picohttp_callback_deregister, NULL, r->ad);
}

/* Pull the fetcher's queued bytes for every active stream into the server. */
static void relay_client_sends(rig_t *r)
{
    for (int guard = 0; guard < 64 && g_active_n > 0; guard++) {
        uint64_t sid = g_active[0];
        g_provide_nb = 0; g_provide_fin = 0; g_provide_still = 0;
        (void)moq_pico_wt_callback(CNX, (uint8_t *)(uintptr_t)0xABC, 1200, picohttp_callback_provide_data,
                                   sc_get(sid), r->ad);
        if (g_provide_nb == 0 && !g_provide_fin) { act_set(sid, 0); continue; }
        bool fin = g_provide_fin != 0;
        if (r->version == MOQ_VERSION_DRAFT_18) {
            if (sid & 2u)
                (void)held_bridge_uni_bytes(r->sb, sid, g_provide_buf, g_provide_nb, fin, 0);
            else
                (void)moq_transport_bridge_on_peer_bidi_bytes(r->sb, sid, g_provide_buf, g_provide_nb, fin, 0);
        } else {
            (void)moq_transport_bridge_on_peer_control_bytes(r->sb, sid, g_provide_buf, g_provide_nb, fin, 0);
        }
        if (!g_provide_still) act_set(sid, 0);
    }
}

/* Deliver the server's output: its own streams become peer streams on the
 * fetcher's adapter; writes addressed to the fetcher's streams land there. */
static void relay_server_ops(rig_t *r)
{
    (void)held_bridge_service(r->sb, 0);
    for (size_t i = 0; i < r->sep.count; i++) {
        fake_op_t *op = &r->sep.ops[i];
        if (op->kind == FAKE_OP_OPEN_UNI) {
            if (r->n_unis < UNI_MAX) {
                r->unis[r->n_unis].opid = op->stream_id;
                r->unis[r->n_unis].sid = 3u + 4u * (uint64_t)r->n_unis;   /* real server uni ids */
                r->n_unis++;
            }
        } else if (op->kind == FAKE_OP_WRITE) {
            uint64_t sid = rig_sid(r, op->stream_id);
            if (sid == UINT64_MAX) {
                deliver(r, op->stream_id, op->data, op->data_len, op->fin);   /* one of the fetcher's streams */
            } else if (op->stream_id == r->capture_opid) {
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
            if (sid != UINT64_MAX) { g_reset_code = op->error_code; reset_cb(r, sid); }
        }
    }
    fake_endpoint_clear_ops(&r->sep);
}

static void relay_client_stops(rig_t *r)
{
    for (; g_stop_seen < g_stop_n; g_stop_seen++) {
        uint64_t opid = rig_opid(r, g_stops[g_stop_seen].sid);
        if (opid == UINT64_MAX || r->hold_client) continue;
        (void)moq_transport_bridge_on_peer_stop_sending(r->sb, opid, g_stops[g_stop_seen].code, 0);
    }
}

static void pump(rig_t *r, int rounds)
{
    for (int i = 0; i < rounds; i++) {
        (void)moq_pico_wt_service(r->ad, g_time);
        relay_client_sends(r);
        relay_client_stops(r);
        relay_server_ops(r);
        (void)moq_pico_wt_service(r->ad, g_time);
    }
}

static bool rig_up(rig_t *r, moq_version_t version)
{
    memset(r, 0, sizeof(*r));
    r->version = version;
    r->capture_opid = UINT64_MAX;
    r->alloc = (moq_alloc_t){ NULL, ca, cr, cf };
    g_next_uni = 2; g_next_bidi = 4; g_active_n = 0; g_fc_n = 0; memset(g_fc, 0, sizeof(g_fc));
    g_stop_n = 0; g_stop_seen = 0; g_reset_n = 0; g_realloc_calls = 0; g_fail_realloc_after = -1;
    g_cnx_state = picoquic_state_ready; g_sc_n = 0;
    memset(&g_h3, 0, sizeof(g_h3)); memset(&g_prefix, 0, sizeof(g_prefix));

    moq_session_cfg_t cc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &r->alloc, MOQ_PERSPECTIVE_CLIENT);
    cc.version = version;
    cc.send_request_capacity = true;
    cc.initial_request_capacity = 64;
    cc.max_data_streams = 2;
    cc.max_fetches = 32;
    cc.max_events = 64;
    if (moq_session_create(&cc, 0, &r->c) < 0) return false;

    h3zero_stream_ctx_t *ctrl = sc_get(0);           /* the WT CONNECT stream */
    moq_pico_wt_conn_cfg_t ac;
    moq_pico_wt_conn_cfg_init_sized(&ac, sizeof(ac));
    ac.session = r->c;
    ac.cnx = CNX;
    ac.h3_ctx = &g_h3;
    ac.ctrl_ctx = ctrl;
    ac.alloc = &r->alloc;
    if (moq_pico_wt_conn_create(&ac, &r->ad) != 0) return false;

    moq_session_cfg_t sc;
    moq_session_cfg_init_sized(&sc, sizeof(sc), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    sc.version = version;
    sc.send_request_capacity = true;
    sc.initial_request_capacity = 64;
    sc.max_fetches = 32;
    sc.max_events = 64;
    if (moq_session_create(&sc, 0, &r->sv) < 0) return false;
    /* server op ids start at 1000: never one of the fetcher's own stream ids */
    held_endpoint_init(&r->sep, 1000, 1);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (held_bridge_create(&bcfg, r->sv, &r->sep.vtable, &r->sep, &r->sb) != MOQ_OK) return false;

    if (moq_session_start(r->c, 0) < 0) return false;
    if (version == MOQ_VERSION_DRAFT_18 && moq_session_start(r->sv, 0) < 0) return false;
    pump(r, 8);
    moq_event_t ev;
    while (moq_session_poll_events(r->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(r->sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    return moq_session_state(r->c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(r->sv) == MOQ_SESS_ESTABLISHED && !moq_pico_wt_conn_is_fatal(r->ad);
}

static void rig_down(rig_t *r)
{
    if (r->ad) moq_pico_wt_conn_destroy(r->ad);
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
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
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
    rxs_t st;
    CHECK(rx_state(&r, sid, &st));
    int grants_at_hold = fc_open_calls(sid);
    printf("admission %s %s: total=%zu held_len=%zu held_fin=%d buf_len=%zu paused=%d blocked=%llu fatal=%d\n", lbl,
           split ? "split" : "single", total, st.held_len, (int)st.held_fin, st.buf_len, (int)st.paused,
           (unsigned long long)st.blocked, (int)moq_pico_wt_conn_is_fatal(r.ad));
    CHECK(st.active && st.paused && st.held_input);
    CHECK(st.held_len == first && st.held_fin == !split);              /* exactly the refused chunk */
    CHECK(st.buf_len == first && st.buf && memcmp(st.buf, all, first) == 0);
    CHECK(st.blocked == 0);                                            /* not bridge-owned */
    CHECK(fc_use(sid) == 1);                                           /* window owned, frozen */
    CHECK(g_stop_n == 0 && g_reset_n == 0);                            /* nothing STOPped */
    CHECK(!moq_pico_wt_conn_is_fatal(r.ad) && moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
    if (split) {
        /* the rest of the response arrives behind the held chunk */
        deliver(&r, sid, all + first, total - first, true);
        CHECK(rx_state(&r, sid, &st));
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
    CHECK(rx_state(&r, sid, &st) && st.paused && st.held_input);
    CHECK(fc_open_calls(sid) == grants_at_hold);                       /* no credit while held */
    /* capacity returns: live stream 0 ends; the service pass redelivers */
    CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 3);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    printf("admission %s %s recovery: objects=%d complete=%d dup=%d bad=%d\n", lbl, split ? "split" : "single",
           co.object, co.complete, co.duplicates, co.bad_payload);
    CHECK(co.complete == 2 && inventory_exact(&co, 1) && co.error == 0 && co.reset == 0);
    CHECK(!rx_state(&r, sid, &st) || (!st.held_input && !st.paused && st.buf_len == 0));
    CHECK(moq_session_can_admit_data_stream(r.c));
    CHECK(!moq_pico_wt_conn_is_fatal(r.ad));
    rig_down(&r);
    if (failures == before) printf("PASS: pw hold_input admission %s %s\n", lbl, split ? "split" : "single");
    return failures - before;
}

/* Repeated refusal: two held responses (the first stays LIVE after its
 * admission, so it keeps the entry); one entry frees; the first replayed
 * stream wins and the other is refused again -- same allocation, same
 * bytes, credit untouched -- then completes when the next entry frees. */
static int row_repeat(moq_version_t v)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
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
    rxs_t t2, t3;
    CHECK(rx_state(&r, s2, &t2) && t2.held_input && t2.held_len == l2);
    CHECK(rx_state(&r, s3, &t3) && t3.held_input && t3.held_len == l3);
    const uint8_t *buf3 = t3.buf;
    int grants3 = fc_open_calls(s3);
    /* one entry frees: the sweep replays s2 first (table order); s3 is refused again */
    CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 3);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    CHECK(rx_state(&r, s3, &t3));
    printf("repeat %s: after first free objects=%d complete=%d s3 held=%d held_len=%zu same_buf=%d grants=%d\n", lbl,
           co.object, co.complete, (int)t3.held_input, t3.held_len, (int)(t3.buf == buf3), fc_open_calls(s3));
    CHECK(co.complete == 1 && co.object == 1 && co.seen[0] == 1u && co.bad_payload == 0);   /* live 0 done; s2 live */
    CHECK(!moq_session_can_admit_data_stream(r.c));
    CHECK(t3.held_input && t3.paused && t3.held_len == l3 && t3.held_fin);
    CHECK(t3.buf == buf3 && t3.buf && memcmp(t3.buf, a3, l3) == 0);                /* ownership kept, no copy */
    CHECK(fc_open_calls(s3) == grants3);
    CHECK(!rx_state(&r, s2, &t2) || !t2.held_input);
    /* the next entry frees (s2 ends): s3 completes */
    CHECK(moq_session_end_fetch(r.sv, sfh[2], 0) == MOQ_OK);
    pump(&r, 3);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    CHECK(co.complete == 2 && co.object == 1 && co.seen[0] == 1u && co.bad_payload == 0);
    CHECK(!rx_state(&r, s3, &t3) || !t3.held_input);
    CHECK(!moq_pico_wt_conn_is_fatal(r.ad));
    rig_down(&r);
    if (failures == before) printf("PASS: pw hold_input repeated refusal %s\n", lbl);
    return failures - before;
}

/* Draft-18: the stream type arrives as the two-byte varint 0x80 0x05 split
 * across callbacks. The first byte is accepted into the bridge's retained
 * classification prefix (nothing paused); the chunk completing the type is
 * refused at admission and held exactly (the suffix only). The redelivery
 * must not replay the prefix: the response then decodes to its exact object. */
static int row_prefix(void)
{
    int before = failures;
    rig_t r;
    if (!rig_up(&r, MOQ_VERSION_DRAFT_18)) {
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
    rxs_t st;
    CHECK(rx_state(&r, sid, &st) && !st.paused && !st.held_input && st.delivered == 1);
    deliver(&r, sid, wide + 1, wtotal - 1, true);
    CHECK(rx_state(&r, sid, &st));
    printf("prefix v18: total=%zu held_len=%zu held_first=0x%02x held_fin=%d paused=%d\n", wtotal, st.held_len,
           st.buf_len ? st.buf[0] : 0, (int)st.held_fin, (int)st.paused);
    CHECK(st.paused && st.held_input && st.held_len == wtotal - 1 && st.held_fin);
    CHECK(st.buf_len == wtotal - 1 && st.buf && memcmp(st.buf, wide + 1, wtotal - 1) == 0);   /* suffix only */
    CHECK(!moq_pico_wt_conn_is_fatal(r.ad));
    CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 3);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    printf("prefix v18 recovery: objects=%d complete=%d dup=%d bad=%d fatal=%d\n", co.object, co.complete,
           co.duplicates, co.bad_payload, (int)moq_pico_wt_conn_is_fatal(r.ad));
    CHECK(co.complete == 2 && inventory_exact(&co, 1) && co.error == 0 && co.reset == 0);
    CHECK(!moq_pico_wt_conn_is_fatal(r.ad));
    rig_down(&r);
    if (failures == before) printf("PASS: pw hold_input fragmented prefix v18\n");
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
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
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
    rxs_t st;
    CHECK(rx_state(&r, s0, &st));
    printf("retained %s control: total=%zu paused=%d blocked=%llu held=%d\n", lbl, total, (int)st.paused,
           (unsigned long long)st.blocked, (int)st.held_input);
    CHECK(st.paused && !st.held_input && st.blocked == total && st.buf_len == 0);
    for (int i = 0; i < 12; i++) { memset(&co, 0, sizeof(co)); drain(r.c, &co); obs_merge(&tot, &co); pump(&r, 1); }
    CHECK(inventory_exact(&tot, 70) && tot.ok == 1 && tot.complete == 0);
    CHECK(rx_state(&r, s0, &st) && !st.paused && st.blocked == 0);
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
    CHECK(rx_state(&r, s2, &st) && st.held_input && st.held_len == hl && !st.held_fin);
    for (int i = 1; i < 70; i++) {
        CHECK(write_obj(r.sv, sfh[2], (uint64_t)i) == MOQ_OK);
        if (i % 16 == 15) pump(&r, 1);
    }
    pump(&r, 1);
    uint8_t rest[8192]; size_t rl = r.cap_len; memcpy(rest, r.cap, rl);
    r.capture_opid = UINT64_MAX;
    deliver(&r, s2, rest, rl, false);                      /* appended behind the held chunk */
    if (fin_while_held) deliver(&r, s2, NULL, 0, true);    /* bare FIN while held: owed behind bytes */
    CHECK(rx_state(&r, s2, &st));
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
    CHECK(rx_state(&r, s2, &st));
    CHECK(!st.held_input);
    if (!fin_while_held) {
        /* the FIN arrives now, as a bare callback, while the session still
         * retains input for the stream: it waits behind those bytes */
        CHECK(st.paused && st.blocked > 0);
        deliver(&r, s2, NULL, 0, true);
        CHECK(rx_state(&r, s2, &st) && st.buf_fin && st.buf_len == 0);
    }
    /* service passes only: no transport callback carries anything more */
    for (int i = 0; i < 12; i++) {
        (void)moq_pico_wt_service(r.ad, g_time);
        memset(&co, 0, sizeof(co)); drain(r.c, &co); obs_merge(&tot, &co);
    }
    bool gone = !rx_state(&r, s2, &st);
    printf("deferred %s %s recovery: objects=%d complete=%d dup=%d bad=%d entry_retired=%d\n", lbl,
           fin_while_held ? "fin-while-held" : "fin-after-block", tot.object, tot.complete, tot.duplicates,
           tot.bad_payload, (int)gone);
    CHECK(inventory_exact(&tot, 70) && tot.complete == 2 && tot.error == 0 && tot.reset == 0);
    CHECK(gone);                                            /* retired on its FIN, exactly once */
    CHECK(!moq_pico_wt_conn_is_fatal(r.ad));
    rig_down(&r);
    g_end_object = 3;
    if (failures == before) printf("PASS: pw hold_input retained + deferred fin %s %s\n", lbl,
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
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
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
        rxs_t st;
        CHECK(rx_state(&r, sid, &st) && st.held_input);
        int grants = fc_open_calls(sid);
        long bal = g_bal;
        g_reset_code = 0x7;
        reset_cb(&r, sid);
        CHECK(!rx_state(&r, sid, &st));                   /* entry gone */
        CHECK(g_bal == bal - 1);                                         /* the retention buffer freed */
        CHECK(!moq_pico_wt_conn_is_fatal(r.ad));
        CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
        pump(&r, 3);
        memset(&co, 0, sizeof(co)); drain(r.c, &co);
        CHECK(co.complete == 1 && co.object == 0);                       /* only live 0 completed */
        CHECK(fc_open_calls(sid) == grants);                             /* no post-terminal credit */
        CHECK(!rx_state(&r, sid, &st));
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
        rxs_t st;
        CHECK(rx_state(&r, s2, &st) && !st.held_input && st.paused && st.blocked > 0);
        int grants = fc_open_calls(s2);
        long bal = g_bal;
        memset(&co, 0, sizeof(co)); drain(r.c, &co);
        g_reset_code = 0x7;
        reset_cb(&r, s2);
        CHECK(!rx_state(&r, s2, &st));
        CHECK(g_bal <= bal);
        for (int i = 0; i < 6; i++) { (void)moq_pico_wt_service(r.ad, g_time); memset(&co, 0, sizeof(co)); drain(r.c, &co); }
        CHECK(fc_open_calls(s2) == grants);
        CHECK(!rx_state(&r, s2, &st));
        CHECK(!moq_pico_wt_conn_is_fatal(r.ad));
        rig_down(&r);
    }
    {   /* WebTransport session deregistered while held: everything released, no later credit */
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
        rxs_t st;
        CHECK(rx_state(&r, sid, &st) && st.held_input);
        int grants = fc_open_calls(sid);
        long bal = g_bal;
        deregister_cb(&r);                                              /* h3zero tears the session down */
        CHECK(!rx_state(&r, sid, &st));                                 /* every retained buffer released */
        CHECK(g_bal < bal);
        (void)moq_pico_wt_service(r.ad, g_time);
        CHECK(fc_open_calls(sid) == grants);
        rig_down(&r);                                                   /* balance 0 */
    }
    g_end_object = 3;
    if (failures == before) printf("PASS: pw hold_input terminals %s\n", lbl);
    return failures - before;
}

/* Allocation failure while retaining the refused chunk, and the exact budget
 * boundary. Both report the adapter's explicit failure contract (a transport
 * error on the bridge, the connection torn down), never acceptance; the
 * chunk is not claimed and nothing leaks. */
static int row_alloc_and_budget(moq_version_t v)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
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
        rxs_t st;
        bool tracked = rx_state(&r, sid, &st);
        printf("alloc-fail %s: fatal=%d tracked=%d held=%d buf_len=%zu\n", lbl, (int)moq_pico_wt_conn_is_fatal(r.ad),
               (int)tracked, tracked ? (int)st.held_input : -1, tracked ? st.buf_len : 0);
        CHECK(moq_pico_wt_conn_is_fatal(r.ad));                               /* explicit, not silent */
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
            rxs_t st;
            bool tracked = rx_state(&r, sid, &st);
            printf("budget %s %s: window=%llu chunk=%zu fatal=%d held=%d\n", lbl, over ? "over" : "exact",
                   (unsigned long long)g_rx_window, r.cap_len, (int)moq_pico_wt_conn_is_fatal(r.ad),
                   tracked ? (int)st.held_input : -1);
            if (over) {
                CHECK(moq_pico_wt_conn_is_fatal(r.ad));
                CHECK(!tracked || !st.held_input);
            } else {
                CHECK(!moq_pico_wt_conn_is_fatal(r.ad));
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
    if (failures == before) printf("PASS: pw hold_input alloc/budget %s\n", lbl);
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
    if (failures == before) printf("PASS: pw required HOLD_INPUT\n");
    return failures - before;
}

int main(void)
{
    moq_version_t versions[2] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (int i = 0; i < 2; i++) {
        (void)row_admission(versions[i], false);
        (void)row_admission(versions[i], true);
        (void)row_repeat(versions[i]);
        (void)row_retained_and_deferred_fin(versions[i], true);
        (void)row_retained_and_deferred_fin(versions[i], false);
        (void)row_terminals(versions[i]);
        (void)row_alloc_and_budget(versions[i]);
        (void)row_required_hold(versions[i]);
    }
    (void)row_prefix();
    if (failures == 0) printf("PASS: test_pico_wt_hold_input\n");
    else fprintf(stderr, "FAILED: test_pico_wt_hold_input (%d)\n", failures);
    return failures ? 1 : 0;
}
