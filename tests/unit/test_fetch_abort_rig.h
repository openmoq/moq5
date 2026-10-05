/*
 * Shared SimPair rig for the publisher-side FETCH abort tests: counting
 * allocator, a trace hook recording the server's RESET_DATA / CLOSE_SESSION /
 * SEND_DATA actions, an event ledger per side, and the issue / accept / write
 * helpers. Included by one translation unit per test binary.
 */
#ifndef TEST_FETCH_ABORT_RIG_H
#define TEST_FETCH_ABORT_RIG_H

#include "test_support.h"
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/wire.h>   /* MOQ_QUIC_VARINT_MAX: the reset code's domain bound */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(__GNUC__)
#define TSS_RIG_UNUSED __attribute__((unused))
#else
#define TSS_RIG_UNUSED
#endif

/* -- counting allocator ---------------------------------------------------- */
typedef struct cnt_alloc {
    moq_alloc_t vt;
    size_t      live;
} cnt_alloc_t;
static void *ca_alloc(size_t n, void *c) { cnt_alloc_t *a = c; void *p = malloc(n); if (p) a->live++; return p; }
static void *ca_realloc(void *p, size_t o, size_t n, void *c) { (void)o; (void)c; return realloc(p, n); }
static void  ca_free(void *p, size_t n, void *c) { (void)n; cnt_alloc_t *a = c; if (p) { a->live--; free(p); } }
TSS_RIG_UNUSED static void cnt_alloc_init(cnt_alloc_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.alloc = ca_alloc; a->vt.realloc = ca_realloc; a->vt.free = ca_free; a->vt.ctx = a;
}

/* -- trace capture: every action the SERVER (publisher) emits ---------------- */
typedef struct trace_cap {
    int      resets;             /* RESET_DATA actions from the server     */
    uint64_t reset_ref;          /* its stream ref                          */
    uint64_t reset_code;         /* its error code                          */
    int      close_session;      /* CLOSE_SESSION actions from the server   */
    int      data_streams;       /* distinct SEND_DATA refs seen from server */
    uint64_t data_refs[16];
} trace_cap_t;

TSS_RIG_UNUSED static void on_trace(void *ctx, const moq_sim_trace_record_t *rec)
{
    trace_cap_t *t = ctx;
    if (rec->kind != MOQ_SIM_TRACE_ACTION || rec->from != MOQ_PERSPECTIVE_SERVER)
        return;
    if (rec->action_kind == MOQ_ACTION_RESET_DATA) {
        t->resets++;
        t->reset_ref = rec->stream_ref._v;
        t->reset_code = rec->code;
    } else if (rec->action_kind == MOQ_ACTION_CLOSE_SESSION) {
        t->close_session++;
    } else if (rec->action_kind == MOQ_ACTION_SEND_DATA) {
        int i;
        for (i = 0; i < t->data_streams; i++)
            if (t->data_refs[i] == rec->stream_ref._v) break;
        if (i == t->data_streams && t->data_streams < 16)
            t->data_refs[t->data_streams++] = rec->stream_ref._v;
    }
}

/* -- event capture ---------------------------------------------------------- */
typedef struct ev_cap {
    int      requests, ok, objects, complete, error, cancelled, sub_objects, closed;
    int      redirect;           /* fetcher: REQUEST_REDIRECT for a fetch      */
    int      chunks;             /* OBJECT_CHUNK events (streaming subgroups)  */
    uint64_t err_code;
    moq_fetch_t last_request;    /* publisher: last FETCH_REQUEST           */
    uint64_t obj_handles[32];    /* fetcher: handle carried by each object  */
    int      obj_n;
    moq_subscription_t up_sub;   /* publisher: relay-style upstream sub     */
    bool     up_seen;
    /* fetcher: handle carried by each non-RESET request-local terminal
     * (FETCH_ERROR, REQUEST_REDIRECT for a fetch, FETCH_CANCELLED), in arrival
     * order; FETCH_RESET is ledgered separately below */
    uint64_t term_handles[32];
    int      term_n;
    /* fetcher: every FETCH_RESET, in arrival order, with its full detail */
    struct { uint64_t handle, code; bool data_stream, stop_sending; } resets[32];
    int      reset_n;
} ev_cap_t;

/* Every request-local terminal attributed to h: ERROR + REDIRECT + CANCELLED + RESET. */
TSS_RIG_UNUSED static int ev_all_terminals_for(const ev_cap_t *c, moq_fetch_t h)
{
    int n = 0;
    for (int i = 0; i < c->term_n && i < 32; i++) if (c->term_handles[i] == h._opaque) n++;
    for (int i = 0; i < c->reset_n && i < 32; i++) if (c->resets[i].handle == h._opaque) n++;
    return n;
}

/* FETCH_RESET records attributed to handle h: count, and the first one's detail. */
typedef struct reset_view { int n; uint64_t code; bool data_stream, stop_sending; } reset_view_t;
TSS_RIG_UNUSED static reset_view_t ev_resets_for(const ev_cap_t *c, moq_fetch_t h)
{
    reset_view_t v; memset(&v, 0, sizeof(v));
    for (int i = 0; i < c->reset_n && i < 32; i++) {
        if (c->resets[i].handle != h._opaque) continue;
        if (v.n == 0) { v.code = c->resets[i].code; v.data_stream = c->resets[i].data_stream; v.stop_sending = c->resets[i].stop_sending; }
        v.n++;
    }
    return v;
}

TSS_RIG_UNUSED static int ev_terminals_for(const ev_cap_t *c, moq_fetch_t h)
{
    int n = 0;
    for (int i = 0; i < c->term_n && i < 32; i++) if (c->term_handles[i] == h._opaque) n++;
    return n;
}

TSS_RIG_UNUSED static int ev_objects_for(const ev_cap_t *c, moq_fetch_t h)
{
    int n = 0;
    for (int i = 0; i < c->obj_n && i < 32; i++) if (c->obj_handles[i] == h._opaque) n++;
    return n;
}

TSS_RIG_UNUSED static void drain(moq_session_t *s, ev_cap_t *c, bool auto_accept_sub)
{
    moq_event_t evs[8];
    size_t n;
    while ((n = moq_session_poll_events(s, evs, 8)) > 0) {
        for (size_t i = 0; i < n; i++) {
            moq_event_t *ev = &evs[i];
            switch (ev->kind) {
            case MOQ_EVENT_FETCH_REQUEST: c->requests++; c->last_request = ev->u.fetch_request.fetch; break;
            case MOQ_EVENT_FETCH_OK:       c->ok++; break;
            case MOQ_EVENT_FETCH_OBJECT:
                if (c->obj_n < 32) c->obj_handles[c->obj_n] = ev->u.fetch_object.fetch._opaque;
                c->obj_n++; c->objects++; break;
            case MOQ_EVENT_FETCH_COMPLETE: c->complete++; break;
            case MOQ_EVENT_FETCH_ERROR:
                if (c->term_n < 32) c->term_handles[c->term_n] = ev->u.fetch_error.fetch._opaque;
                c->term_n++; c->error++; c->err_code = ev->u.fetch_error.error_code; break;
            case MOQ_EVENT_FETCH_CANCELLED:
                if (c->term_n < 32) c->term_handles[c->term_n] = ev->u.fetch_cancelled.fetch._opaque;
                c->term_n++; c->cancelled++; break;
            case MOQ_EVENT_FETCH_RESET:
                if (c->reset_n < 32) {
                    c->resets[c->reset_n].handle = ev->u.fetch_reset.fetch._opaque;
                    c->resets[c->reset_n].code = ev->u.fetch_reset.error_code;
                    c->resets[c->reset_n].data_stream = ev->u.fetch_reset.data_stream;
                    c->resets[c->reset_n].stop_sending = ev->u.fetch_reset.stop_sending;
                }
                c->reset_n++; break;
            case MOQ_EVENT_OBJECT_RECEIVED: c->sub_objects++; break;
            case MOQ_EVENT_OBJECT_CHUNK: c->chunks++; break;
            case MOQ_EVENT_REQUEST_REDIRECT:
                if (ev->u.request_redirect.family == MOQ_REQUEST_FAMILY_FETCH) {
                    if (c->term_n < 32) c->term_handles[c->term_n] = ev->u.request_redirect.handle.fetch._opaque;
                    c->term_n++; c->redirect++;
                }
                break;
            case MOQ_EVENT_SESSION_CLOSED: c->closed++; break;
            case MOQ_EVENT_SUBSCRIBE_REQUEST:
                if (auto_accept_sub) {
                    moq_accept_subscribe_cfg_t ac;
                    moq_accept_subscribe_cfg_init(&ac);
                    (void)moq_session_accept_subscribe(s, ev->u.subscribe_request.sub, &ac, 1);
                    c->up_sub = ev->u.subscribe_request.sub;
                    c->up_seen = true;
                }
                break;
            default: break;
            }
            moq_event_cleanup(ev);
        }
    }
}

typedef struct rig {
    cnt_alloc_t     alloc;
    moq_simpair_t  *sp;
    moq_session_t  *pub, *fet;
    uint64_t        now;
    trace_cap_t     tr;
    ev_cap_t        pe, fe;
} rig_t;

TSS_RIG_UNUSED static void cycle(rig_t *r)
{
    r->now += 1000;
    (void)moq_simpair_advance_to(r->sp, r->now);
    size_t steps = 0;
    (void)moq_simpair_run_until_quiescent(r->sp, 64, &steps);
}

TSS_RIG_UNUSED static bool rig_up_ex(rig_t *r, moq_version_t v, uint32_t max_actions, uint32_t max_events)
{
    memset(r, 0, sizeof(*r));
    cnt_alloc_init(&r->alloc);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc.vt;
    cfg.seed = 0xAB0C7u;
    cfg.version = v;
    cfg.max_actions = max_actions;
    cfg.max_events = max_events;
    cfg.trace_fn = on_trace;
    cfg.trace_ctx = &r->tr;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 1024;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 1024;
    if (moq_simpair_create(&cfg, &r->sp) != MOQ_OK) return false;
    if (moq_simpair_start(r->sp) != MOQ_OK) { moq_simpair_destroy(r->sp); return false; }
    r->fet = moq_simpair_client(r->sp);
    r->pub = moq_simpair_server(r->sp);
    r->now = 1;
    for (int i = 0; i < 8; i++) cycle(r);
    /* leave both event queues empty: a row sized to a tiny queue must start
     * without the setup events still occupying slots */
    drain(r->fet, &r->fe, false);
    drain(r->pub, &r->pe, false);
    return true;
}

TSS_RIG_UNUSED static bool rig_up(rig_t *r, moq_version_t v, uint32_t max_actions)
{
    return rig_up_ex(r, v, max_actions, 0);
}

TSS_RIG_UNUSED static void rig_down(rig_t *r) { moq_simpair_destroy(r->sp); }

TSS_RIG_UNUSED static moq_result_t fetch_issue(rig_t *r, const char *name, moq_fetch_t *out)
{
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("abort") };
    moq_fetch_cfg_t fc;
    memset(&fc, 0, sizeof(fc));
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    fc.start_group = 0; fc.start_object = 0; fc.end_group = 0; fc.end_object = 4;
    return moq_session_fetch(r->fet, &fc, r->now, out);
}

TSS_RIG_UNUSED static moq_result_t pub_write(rig_t *r, moq_fetch_t fh, uint64_t oid)
{
    uint8_t body[24];
    memset(body, (int)(0x60 + oid), sizeof(body));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&r->alloc.vt, body, sizeof(body), &pl) != MOQ_OK) return MOQ_ERR_NOMEM;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0; oc.subgroup_id = 0; oc.object_id = oid; oc.publisher_priority = 100;
    oc.payload = pl;
    moq_result_t rc = moq_session_write_fetch_object(r->pub, fh, &oc, r->now);
    moq_rcbuf_decref(pl);
    return rc;
}

/* Wait (bounded) until the publisher has seen `n` requests. */
TSS_RIG_UNUSED static bool pub_await_requests(rig_t *r, int n)
{
    for (int i = 0; i < 8 && r->pe.requests < n; i++) { cycle(r); drain(r->pub, &r->pe, true); }
    return r->pe.requests >= n;
}

TSS_RIG_UNUSED static moq_result_t pub_accept(rig_t *r, moq_fetch_t fh, uint64_t end_object)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_of_track = false; ac.end_group = 0; ac.end_object = end_object;
    moq_result_t rc = moq_session_accept_fetch(r->pub, fh, &ac, r->now);
    for (int i = 0; i < 8 && rc == MOQ_ERR_WOULD_BLOCK; i++) { cycle(r); rc = moq_session_accept_fetch(r->pub, fh, &ac, r->now); }
    return rc;
}


/* -- Raw pair with a manual, hold-capable pump ----------------------------- *
 * Two real sessions established by test_session_support's establish_pair, with
 * every action delivered by hand: a stream ref crossing the pair is XOR-mapped
 * (an involution, so a reply on the same bidi maps back), and a caller-supplied
 * filter can HOLD selected actions back; held actions are copied (borrowed
 * byte spans are snapshotted, owned payload refs retained) and released later
 * in order. ABORT_BIDI_STREAM reaches the peer as RESET then STOP, as the sim
 * delivers it. This is what the order / delay / capacity rows need and the
 * SimPair cannot arrange. */
#include "test_session_support.h"

#define RAW_REF_MAP (1ull << 40)
static inline moq_stream_ref_t raw_map_ref(moq_stream_ref_t r) { return moq_stream_ref_from_u64(r._v ^ RAW_REF_MAP); }

typedef struct held_action {
    uint32_t         kind;
    moq_stream_ref_t ref;      /* sender-side ref */
    uint8_t          bytes[512];
    size_t           len;
    bool             fin;
    uint64_t         code;
    moq_rcbuf_t     *payload;  /* SEND_DATA: retained */
    uint8_t          header[32];
    uint8_t          header_len;
} held_action_t;

typedef bool (*raw_hold_fn)(const moq_action_t *a, void *ctx);

typedef struct raw_pair {
    test_alloc_state_t ta;
    moq_alloc_t        alloc;
    moq_session_t     *fet, *pub;   /* fet = client, pub = server */
    uint64_t           now;
    held_action_t      held[16];
    int                held_n;
    moq_result_t       last_deliver_rc;  /* most negative delivery result seen */
    int                close_actions;    /* CLOSE_SESSION polled from either side */
    moq_stream_ref_t   last_reset_ref;   /* sender-side ref of the last RESET_DATA delivered */
} raw_t;

TSS_RIG_UNUSED static void raw_cycle(raw_t *r);

TSS_RIG_UNUSED static bool raw_up_ex(raw_t *r, moq_version_t v, uint32_t fet_max_events, bool fet_streaming)
{
    memset(r, 0, sizeof(*r));
    r->alloc = test_allocator(&r->ta);
    moq_session_cfg_t cc = MOQ_SESSION_CFG_INIT, sc = MOQ_SESSION_CFG_INIT;
    cc.alloc = &r->alloc; cc.perspective = MOQ_PERSPECTIVE_CLIENT; cc.version = v;
    cc.send_request_capacity = true; cc.initial_request_capacity = 1024;
    cc.max_events = fet_max_events;
    cc.streaming_objects = fet_streaming;
    sc.alloc = &r->alloc; sc.perspective = MOQ_PERSPECTIVE_SERVER; sc.version = v;
    sc.send_request_capacity = true; sc.initial_request_capacity = 1024;
    if (moq_session_create(&cc, 0, &r->fet) != MOQ_OK) return false;
    if (moq_session_create(&sc, 0, &r->pub) != MOQ_OK) { moq_session_destroy(r->fet); return false; }
    r->now = 1;
    (void)moq_session_start(r->fet, r->now);
    /* a profile whose setup is symmetric needs the server started too (the
     * sim does the same); on the others this is a benign WRONG_STATE */
    (void)moq_session_start(r->pub, r->now);
    for (int i = 0; i < 6 && (moq_session_state(r->fet) != MOQ_SESS_ESTABLISHED ||
                              moq_session_state(r->pub) != MOQ_SESS_ESTABLISHED); i++)
        raw_cycle(r);
    return moq_session_state(r->fet) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(r->pub) == MOQ_SESS_ESTABLISHED;
}

TSS_RIG_UNUSED static bool raw_up(raw_t *r, moq_version_t v, uint32_t fet_max_events)
{
    return raw_up_ex(r, v, fet_max_events, false);
}

TSS_RIG_UNUSED static void raw_down(raw_t *r)
{
    for (int i = 0; i < r->held_n; i++) if (r->held[i].payload) moq_rcbuf_decref(r->held[i].payload);
    r->held_n = 0;
    moq_session_destroy(r->pub);
    moq_session_destroy(r->fet);
}

/* Deliver one (possibly held) action to `to`. */
TSS_RIG_UNUSED static moq_result_t raw_deliver(raw_t *r, moq_session_t *to, const held_action_t *h)
{
    moq_stream_ref_t ref = raw_map_ref(h->ref);
    switch (h->kind) {
    case MOQ_ACTION_SEND_CONTROL:
    case MOQ_ACTION_OPEN_UNI_CONTROL:
    case MOQ_ACTION_SEND_UNI_CONTROL:
        /* a uni control pair carries the same bytes as the bidi control channel */
        return moq_session_on_control_bytes(to, h->bytes, h->len, r->now);
    case MOQ_ACTION_SEND_DATAGRAM:
        return moq_session_on_datagram(to, h->bytes, h->len, r->now);
    case MOQ_ACTION_OPEN_BIDI_STREAM:
    case MOQ_ACTION_SEND_BIDI_STREAM:
        return moq_session_on_bidi_stream_bytes(to, ref, h->len ? h->bytes : NULL, h->len, h->fin, r->now);
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        return moq_session_on_bidi_stream_bytes(to, ref, NULL, 0, true, r->now);
    case MOQ_ACTION_ABORT_BIDI_STREAM: {
        moq_result_t a = moq_session_on_bidi_stream_reset(to, ref, h->code, r->now);
        moq_result_t b = moq_session_on_bidi_stream_stop(to, ref, h->code, r->now);
        return a < 0 ? a : b;
    }
    case MOQ_ACTION_RESET_BIDI_STREAM:
        return moq_session_on_bidi_stream_reset(to, ref, h->code, r->now);
    case MOQ_ACTION_STOP_BIDI_STREAM:
        return moq_session_on_bidi_stream_stop(to, ref, h->code, r->now);
    case MOQ_ACTION_SEND_DATA: {
        bool hp = h->payload != NULL;
        moq_result_t rc = MOQ_OK;
        if (h->header_len > 0)
            rc = moq_session_on_data_bytes(to, ref, h->header, h->header_len, h->fin && !hp, r->now);
        if (hp && rc >= 0)
            rc = moq_session_on_data_bytes(to, ref, moq_rcbuf_data(h->payload), moq_rcbuf_len(h->payload), h->fin, r->now);
        if (!hp && h->header_len == 0 && h->fin)
            rc = moq_session_on_data_bytes(to, ref, NULL, 0, true, r->now);
        return rc;
    }
    case MOQ_ACTION_RESET_DATA:
        r->last_reset_ref = h->ref;
        return moq_session_on_data_reset(to, ref, h->code, r->now);
    case MOQ_ACTION_STOP_DATA:
        return moq_session_on_data_stop(to, ref, h->code, r->now);
    default:
        return MOQ_OK;
    }
}

TSS_RIG_UNUSED static void raw_snapshot(held_action_t *h, const moq_action_t *a)
{
    memset(h, 0, sizeof(*h));
    h->kind = a->kind;
    switch (a->kind) {
    case MOQ_ACTION_SEND_CONTROL:
        h->len = a->u.send_control.len < sizeof(h->bytes) ? a->u.send_control.len : sizeof(h->bytes);
        memcpy(h->bytes, a->u.send_control.data, h->len); break;
    case MOQ_ACTION_OPEN_UNI_CONTROL:
        h->len = a->u.open_uni_control.len < sizeof(h->bytes) ? a->u.open_uni_control.len : sizeof(h->bytes);
        memcpy(h->bytes, a->u.open_uni_control.data, h->len); break;
    case MOQ_ACTION_SEND_UNI_CONTROL:
        h->len = a->u.send_uni_control.len < sizeof(h->bytes) ? a->u.send_uni_control.len : sizeof(h->bytes);
        memcpy(h->bytes, a->u.send_uni_control.data, h->len); break;
    case MOQ_ACTION_SEND_DATAGRAM:
        h->len = a->u.send_datagram.len < sizeof(h->bytes) ? a->u.send_datagram.len : sizeof(h->bytes);
        memcpy(h->bytes, a->u.send_datagram.data, h->len); break;
    case MOQ_ACTION_OPEN_BIDI_STREAM:
        h->ref = a->u.open_bidi_stream.stream_ref; h->fin = a->u.open_bidi_stream.fin;
        h->len = a->u.open_bidi_stream.len < sizeof(h->bytes) ? a->u.open_bidi_stream.len : sizeof(h->bytes);
        memcpy(h->bytes, a->u.open_bidi_stream.data, h->len); break;
    case MOQ_ACTION_SEND_BIDI_STREAM:
        h->ref = a->u.send_bidi_stream.stream_ref; h->fin = a->u.send_bidi_stream.fin;
        h->len = a->u.send_bidi_stream.len < sizeof(h->bytes) ? a->u.send_bidi_stream.len : sizeof(h->bytes);
        memcpy(h->bytes, a->u.send_bidi_stream.data, h->len); break;
    case MOQ_ACTION_CLOSE_BIDI_STREAM: h->ref = a->u.close_bidi_stream.stream_ref; break;
    case MOQ_ACTION_ABORT_BIDI_STREAM: h->ref = a->u.abort_bidi_stream.stream_ref; h->code = a->u.abort_bidi_stream.error_code; break;
    case MOQ_ACTION_RESET_BIDI_STREAM: h->ref = a->u.reset_bidi_stream.stream_ref; h->code = a->u.reset_bidi_stream.error_code; break;
    case MOQ_ACTION_STOP_BIDI_STREAM:  h->ref = a->u.stop_bidi_stream.stream_ref;  h->code = a->u.stop_bidi_stream.error_code; break;
    case MOQ_ACTION_SEND_DATA:
        h->ref = a->u.send_data.stream_ref; h->fin = a->u.send_data.fin;
        h->header_len = a->u.send_data.header_len; memcpy(h->header, a->u.send_data.header, a->u.send_data.header_len);
        if (a->u.send_data.payload) { h->payload = a->u.send_data.payload; moq_rcbuf_incref(h->payload); }
        break;
    case MOQ_ACTION_RESET_DATA: h->ref = a->u.reset_data.stream_ref; h->code = a->u.reset_data.error_code; break;
    case MOQ_ACTION_STOP_DATA:  h->ref = a->u.stop_data.stream_ref;  h->code = a->u.stop_data.error_code; break;
    default: break;
    }
}

/* Poll every action of `from` and deliver it to `to`, except those the filter
 * holds (appended to the hold list). Returns the number delivered. */
TSS_RIG_UNUSED static int raw_pump(raw_t *r, moq_session_t *from, moq_session_t *to, raw_hold_fn hold, void *ctx)
{
    moq_action_t acts[16];
    size_t n;
    int delivered = 0;
    while ((n = moq_session_poll_actions(from, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_CLOSE_SESSION) { r->close_actions++; moq_action_cleanup(&acts[i]); continue; }
            held_action_t h;
            raw_snapshot(&h, &acts[i]);
            if (hold && hold(&acts[i], ctx) && r->held_n < 16) {
                r->held[r->held_n++] = h;
            } else {
                moq_result_t rc = raw_deliver(r, to, &h);
                if (h.payload) moq_rcbuf_decref(h.payload);
                if (rc < 0) r->last_deliver_rc = rc;
                delivered++;
            }
            moq_action_cleanup(&acts[i]);
        }
    }
    return delivered;
}

/* Release held actions in order to `to`; returns the number released. */
TSS_RIG_UNUSED static int raw_release(raw_t *r, moq_session_t *to)
{
    int n = r->held_n;
    for (int i = 0; i < n; i++) {
        moq_result_t rc = raw_deliver(r, to, &r->held[i]);
        if (r->held[i].payload) moq_rcbuf_decref(r->held[i].payload);
        if (rc < 0) r->last_deliver_rc = rc;
    }
    r->held_n = 0;
    return n;
}

/* Full exchange both ways (publisher first, then fetcher), no holds. */
TSS_RIG_UNUSED static void raw_cycle(raw_t *r)
{
    r->now += 1000;
    for (int i = 0; i < 3; i++) { raw_pump(r, r->pub, r->fet, NULL, NULL); raw_pump(r, r->fet, r->pub, NULL, NULL); }
}

#endif /* TEST_FETCH_ABORT_RIG_H */
