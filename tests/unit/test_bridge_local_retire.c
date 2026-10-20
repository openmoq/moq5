/*
 * Local receive retirement through the real transport bridge.
 *
 * A subscriber STOPs a peer data stream (cancelling its subscription, or a
 * done wait expiring). Some transports, once our receive side is aborted,
 * never report the peer's FIN or RESET: the only remaining signal is that the
 * transport finished with the stream (moq_transport_bridge_on_peer_stream_
 * terminal). The fixture models exactly that transport: after the client's
 * STOP for a stream, nothing more the publisher sends on it is delivered.
 *
 * Pinned, in draft-16, draft-18 and draft-22:
 *   - an accepted STOP plus the terminal report releases the bridge mapping and
 *     the session's stopped owner on the next service pass -- never from inside
 *     the report, and with no event (no SUBGROUP_FINISHED, no reset);
 *   - acceptance counts on the STOP's first dispatch and on its pending retry;
 *     a report that arrives while the STOP is still blocked is kept, and the
 *     release follows the retry; a refused STOP is not acceptance;
 *   - a report made inline, from inside the endpoint's stop_sending, is held
 *     for the service pass;
 *   - a live stream, the control stream and an admission-held stream are never
 *     released by a report; an unknown stream allocates nothing; a repeated
 *     report is harmless;
 *   - a real FIN the bridge still owes the session goes first and settles the
 *     stream as a FIN (the release path is not taken);
 *   - a stream stopped by an expired done wait is released the same way;
 *   - on a still-valid subscription or publication binding the release counts
 *     the stream once toward the done's Stream Count (and the done then
 *     surfaces exactly once); an identity-only record counts nothing;
 *   - teardown releases nothing on its own and fabricates no completion.
 *   - a cancelled fill of a live subscription, retired through the bridge, is
 *     reclaimed at the release with its CANCELLED reason and no second
 *     terminal, its stream counted once toward the parent.
 * (The fill child's table-level guards are pinned in test_fill_owner.c.)
 * Allocations balance in every case.
 */

#include "test_session_support.h"
#include "test_binding_support.h"
#include "../support/fake_endpoint.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include <moq/moq.h>
#include <moq/transport_bridge.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;
static const char *g_case = "";

#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", g_case, __LINE__, #x); failures++; } } while (0)

/* -- pair over fake endpoints ---------------------------------------- */

typedef struct pair {
    moq_session_t          *c, *sv;          /* c subscribes, sv publishes */
    moq_transport_bridge_t *cb, *sb;
    fake_endpoint_t         cep, sep;
    test_alloc_state_t      alloc_state;
    moq_alloc_t             alloc;
    uint64_t                now;
    uint64_t                client_control_sid, server_control_sid;
    uint64_t                swallowed[16];   /* client-stopped uni streams */
    int                     n_swallowed;
    bool                    hold_fins;       /* withhold publisher FINs */
    uint64_t                held_fin_sid[8];
    int                     n_held_fins;
    moq_version_t           version;
    /* refused (NOT_CONSUMED) uni input, kept and redelivered as a transport
     * holds it (MOQ_TRANSPORT_CAP_HOLD_INPUT) */
    struct { bool active; uint64_t sid; uint8_t data[256]; size_t len; bool fin; } holds[8];
    /* inline report from inside stop_sending */
    bool                    inline_report;
    bool                    inline_done;
    bool                    inline_entry_kept;
} pair_t;

static pair_t *g_pair;

static bool is_bidi_id(uint64_t id)
{
    return (id >= 2000 && id < 3000) || (id >= 4000 && id < 5000);
}

static bool swallowed(const pair_t *p, uint64_t sid)
{
    for (int i = 0; i < p->n_swallowed; i++)
        if (p->swallowed[i] == sid) return true;
    return false;
}

/* The client's stop_sending: the fake's, then (when armed) the transport's
 * terminal report made inline, while the bridge frame that called the op is
 * still on the stack. */
static moq_transport_result_t client_stop(void *ctx, uint64_t sid, uint64_t code)
{
    moq_transport_result_t r = fake_stop(ctx, sid, code);
    pair_t *p = g_pair;
    if (r == MOQ_TRANSPORT_OK && p->inline_report && !p->inline_done && !is_bidi_id(sid)) {
        p->inline_done = true;
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
        bridge_stream_entry_t *e = bridge_find_by_id(p->cb, sid);
        p->inline_entry_kept = e != NULL && e->active;
    }
    return r;
}

static int hold_find(pair_t *p, uint64_t sid)
{
    for (int i = 0; i < 8; i++)
        if (p->holds[i].active && p->holds[i].sid == sid) return i;
    return -1;
}

/* Feed one uni chunk to the client bridge, keeping what it refuses (and
 * anything after it on that stream) for redelivery. */
static void feed_uni(pair_t *p, moq_transport_bridge_t *to, uint64_t sid,
                     const uint8_t *data, size_t len, bool fin)
{
    int h = hold_find(p, sid);
    if (h >= 0) {
        CHECK(p->holds[h].len + len <= sizeof(p->holds[h].data));
        if (len) memcpy(p->holds[h].data + p->holds[h].len, data, len);
        p->holds[h].len += len;
        p->holds[h].fin |= fin;
        return;
    }
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(to, sid, data, len, fin, p->now);
    if (rc != MOQ_ERR_INPUT_NOT_CONSUMED) return;
    for (int i = 0; i < 8; i++) {
        if (p->holds[i].active) continue;
        p->holds[i].active = true;
        p->holds[i].sid = sid;
        CHECK(len <= sizeof(p->holds[i].data));
        if (len) memcpy(p->holds[i].data, data, len);
        p->holds[i].len = len;
        p->holds[i].fin = fin;
        return;
    }
    CHECK(!"hold slots exhausted");
}

static void hold_redeliver(pair_t *p)
{
    for (int i = 0; i < 8; i++) {
        if (!p->holds[i].active) continue;
        uint64_t sid = p->holds[i].sid;
        if (moq_transport_bridge_find_ref(p->cb, sid)._v == 0) { p->holds[i].active = false; continue; }
        if (moq_transport_bridge_stream_has_pending(p->cb, sid)) continue;
        uint8_t copy[256];
        size_t len = p->holds[i].len;
        bool fin = p->holds[i].fin;
        memcpy(copy, p->holds[i].data, len);
        p->holds[i].active = false;
        feed_uni(p, p->cb, sid, copy, len, fin);
    }
}

static void deliver(pair_t *p, fake_endpoint_t *from, moq_transport_bridge_t *to, bool from_client)
{
    bool uni_control = moq_transport_bridge_uses_uni_control(to);
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *o = &from->ops[i];
        switch (o->kind) {
        case FAKE_OP_WRITE:
            if (is_bidi_id(o->stream_id)) {
                bool control = false;
                if (!uni_control) {
                    uint64_t *sid = from_client ? &p->client_control_sid : &p->server_control_sid;
                    if (*sid == UINT64_MAX) *sid = o->stream_id;
                    control = o->stream_id == p->client_control_sid ||
                              o->stream_id == p->server_control_sid;
                }
                (void)(control
                    ? moq_transport_bridge_on_peer_control_bytes(to, o->stream_id, o->data,
                                                                o->data_len, o->fin, p->now)
                    : moq_transport_bridge_on_peer_bidi_bytes(to, o->stream_id, o->data,
                                                             o->data_len, o->fin, p->now));
            } else if (!from_client && swallowed(p, o->stream_id)) {
                /* aborted receive: the transport reports nothing more */
            } else {
                bool fin = o->fin;
                if (!from_client && fin && p->hold_fins && p->n_held_fins < 8) {
                    p->held_fin_sid[p->n_held_fins++] = o->stream_id;
                    fin = false;
                }
                if (o->data_len || fin) {
                    if (from_client)
                        (void)moq_transport_bridge_on_peer_uni_bytes(to, o->stream_id, o->data,
                                                                     o->data_len, fin, p->now);
                    else
                        feed_uni(p, to, o->stream_id, o->data, o->data_len, fin);
                }
            }
            break;
        case FAKE_OP_RESET:
            if (!from_client && swallowed(p, o->stream_id)) break;
            (void)moq_transport_bridge_on_peer_stream_reset(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_STOP:
            (void)moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
            if (from_client && !is_bidi_id(o->stream_id) && p->n_swallowed < 16)
                p->swallowed[p->n_swallowed++] = o->stream_id;
            break;
        case FAKE_OP_ABORT:
            (void)moq_transport_bridge_on_peer_stream_reset(to, o->stream_id, o->error_code, p->now);
            (void)moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_DATAGRAM:
            (void)moq_transport_bridge_on_peer_datagram(to, o->data, o->data_len, p->now);
            break;
        case FAKE_OP_CLOSE:
            (void)moq_transport_bridge_on_transport_close(to, o->error_code, p->now);
            break;
        default:
            break;
        }
    }
    fake_endpoint_clear_ops(from);
}

static void pump(pair_t *p, int rounds)
{
    for (int i = 0; i < rounds; i++) {
        p->now += 10;
        moq_transport_bridge_service(p->cb, p->now);
        hold_redeliver(p);
        CHECK(!p->cep.overflowed);
        deliver(p, &p->cep, p->sb, true);
        moq_transport_bridge_service(p->sb, p->now);
        CHECK(!p->sep.overflowed);
        deliver(p, &p->sep, p->cb, false);
    }
}

typedef struct opts {
    uint32_t client_max_events;
    uint32_t client_max_data_streams;
    bool     client_hold_input;
    uint64_t done_wait_us;
} opts_t;

static bool pair_open(pair_t *p, moq_version_t version, const opts_t *o)
{
    memset(p, 0, sizeof(*p));
    g_pair = p;
    p->version = version;
    p->alloc = test_allocator(&p->alloc_state);
    p->now = 1000;
    p->client_control_sid = UINT64_MAX;
    p->server_control_sid = UINT64_MAX;
    moq_session_cfg_t ccfg, scfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), &p->alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = version;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 64;
    ccfg.max_events = o && o->client_max_events ? o->client_max_events : 64;
    if (o && o->client_max_data_streams) ccfg.max_data_streams = o->client_max_data_streams;
    if (o && o->done_wait_us) ccfg.done_wait_timeout_us = o->done_wait_us;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.version = version;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;
    scfg.max_events = 64;
    moq_result_t crc = moq_session_create(&ccfg, p->now, &p->c);
    if (crc < 0) { fprintf(stderr, "client create %d\n", (int)crc); return false; }
    crc = moq_session_create(&scfg, p->now, &p->sv);
    if (crc < 0) { fprintf(stderr, "server create %d\n", (int)crc); return false; }
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.stop_sending = client_stop;
    (void)o;
    p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    crc = moq_transport_bridge_create(&bcfg, p->c, &p->cep.vtable, &p->cep, &p->cb);
    if (crc < 0) { fprintf(stderr, "client bridge %d\n", (int)crc); return false; }
    crc = moq_transport_bridge_create(&bcfg, p->sv, &p->sep.vtable, &p->sep, &p->sb);
    if (crc < 0) { fprintf(stderr, "server bridge %d\n", (int)crc); return false; }
    if (test_bind_if_required(p->c) != MOQ_OK || test_bind_if_required(p->sv) != MOQ_OK) {
        fprintf(stderr, "bind failed\n");
        return false;
    }
    if (moq_session_start(p->c, p->now) < 0) { fprintf(stderr, "client start failed\n"); return false; }
    if (moq_session_uses_uni_control(p->sv) && moq_session_start(p->sv, p->now) < 0) return false;
    pump(p, 6);
    moq_event_t ev;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    if (moq_session_state(p->c) != MOQ_SESS_ESTABLISHED || moq_session_state(p->sv) != MOQ_SESS_ESTABLISHED)
        fprintf(stderr, "states %d %d fatal %d %d\n", (int)moq_session_state(p->c), (int)moq_session_state(p->sv),
                (int)moq_transport_bridge_is_fatal(p->cb), (int)moq_transport_bridge_is_fatal(p->sb));
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(p->sv) == MOQ_SESS_ESTABLISHED &&
           !moq_transport_bridge_is_fatal(p->cb) && !moq_transport_bridge_is_fatal(p->sb);
}

static void pair_close(pair_t *p)
{
    moq_transport_bridge_destroy(p->cb);
    moq_transport_bridge_destroy(p->sb);
    moq_session_destroy(p->c);
    moq_session_destroy(p->sv);
    CHECK(p->alloc_state.balance == 0);
}

/* -- subscription and data helpers ------------------------------------- */

typedef struct track {
    moq_subscription_t csub, ssub;
    moq_subgroup_handle_t sg[4];
    uint64_t sid[4];
    int nsg;
} track_t;

static bool subscribe(pair_t *p, track_t *t)
{
    memset(t, 0, sizeof(*t));
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    if (moq_session_subscribe(p->c, &sc, p->now, &t->csub) != MOQ_OK) return false;
    pump(p, 4);
    moq_event_t ev;
    bool got = false;
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) { t->ssub = ev.u.subscribe_request.sub; got = true; }
        moq_event_cleanup(&ev);
    }
    if (!got) return false;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(p->sv, t->ssub, &ac, p->now) != MOQ_OK) return false;
    pump(p, 4);
    bool ok = false;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) ok = true;
        moq_event_cleanup(&ev);
    }
    return ok;
}

/* Publisher opens subgroup `g` and writes one object; its stream id. */
static bool open_group(pair_t *p, track_t *t, uint64_t g)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = g;
    int i = t->nsg;
    if (moq_session_open_subgroup(p->sv, t->ssub, &sc, p->now, &t->sg[i]) != MOQ_OK) return false;
    moq_rcbuf_t *b = NULL;
    uint8_t bytes[8] = { 1, 2, 3, 4, 5, 6, 7, (uint8_t)g };
    if (moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &b) != MOQ_OK) return false;
    moq_result_t rc = moq_session_write_object(p->sv, t->sg[i], 0, b, p->now);
    moq_rcbuf_decref(b);
    if (rc != MOQ_OK) return false;
    size_t before = p->sep.count;
    moq_transport_bridge_service(p->sb, p->now);
    t->sid[i] = UINT64_MAX;
    for (size_t k = before; k < p->sep.count; k++)
        if (p->sep.ops[k].kind == FAKE_OP_OPEN_UNI) t->sid[i] = p->sep.ops[k].stream_id;
    if (t->sid[i] == UINT64_MAX) {
        for (size_t k = 0; k < p->sep.count; k++)
            if (p->sep.ops[k].kind == FAKE_OP_WRITE && !is_bidi_id(p->sep.ops[k].stream_id) &&
                p->sep.ops[k].stream_id != p->server_control_sid &&
                bridge_find_by_id(p->sb, p->sep.ops[k].stream_id) &&
                bridge_find_by_id(p->sb, p->sep.ops[k].stream_id)->uni_disp != BRIDGE_UNI_DISP_CONTROL)
                t->sid[i] = p->sep.ops[k].stream_id;
    }
    t->nsg++;
    pump(p, 3);
    return t->sid[i] != UINT64_MAX;
}

typedef struct seen { int objects, finished, resets, done, other; } seen_t;

static void drain(moq_session_t *s, seen_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_OBJECT_RECEIVED: o->objects++; break;
        case MOQ_EVENT_SUBGROUP_FINISHED: o->finished++; break;
        case MOQ_EVENT_SUBGROUP_RESET: o->resets++; break;
        case MOQ_EVENT_SUBSCRIBE_DONE: o->done++; break;
        default: o->other++; break;
        }
        moq_event_cleanup(&ev);
    }
}

/* The session still owns `ref` as a STOPPED entry or a stopped record. */
static int session_owner(const moq_session_t *s, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->rx_cap; i++)
        if (s->rx_streams[i].active && s->rx_streams[i].stream_ref._v == ref._v)
            return s->rx_streams[i].parse_state == MOQ_RX_STOPPED ? 2 : 1;
    for (size_t i = 0; i < s->rx_stopped_count; i++)
        if (s->rx_stopped_refs[i] == ref._v) return 3;
    return 0;
}

static bool peer_fin_noted(const moq_session_t *s, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->rx_fin_count; i++)
        if (s->rx_finished[(s->rx_fin_head + i) % s->rx_fin_cap] == ref._v) return true;
    return false;
}

static void close_groups(pair_t *p, track_t *t)
{
    for (int i = 0; i < t->nsg; i++)
        (void)moq_session_close_subgroup(p->sv, t->sg[i], p->now);
    pump(p, 3);
}

/* -- cases ------------------------------------------------------------ */

enum { RETIRE, RETRY, INLINE, REFUSED, ACTIVE, CONTROL, HELD, FIN_FIRST, EXPIRY, TEARDOWN,
       COUNT_SUB, COUNT_PUB, FILL_RETIRE, CASE_COUNT };
static const char *const case_names[] = { "retire", "retry-order", "inline", "refused",
    "active", "control", "held", "fin-first", "expiry", "teardown", "count-subscription",
    "count-publication", "fill-retire" };

/* A subscriber cancels one fill of a live subscription: its stream is
 * STOPped (accepted), the publisher's answer is never reported, and the
 * transport then finishes the stream. The release through the bridge must
 * settle the fill child: it is reclaimed only then, with the cancellation
 * reason it already reported and no second terminal; the fill stream counts
 * once toward the parent subscription; no peer FIN is noted; a repeated or
 * late report changes nothing. */
static void run_fill(pair_t *p)
{
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ nsp, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("f");
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.fill = &fill;
    moq_subscription_t csub, ssub = MOQ_SUBSCRIPTION_INVALID;
    CHECK(moq_session_subscribe(p->c, &sc, p->now, &csub) == MOQ_OK);
    pump(p, 4);
    moq_event_t ev;
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    CHECK(moq_subscription_is_valid(ssub));
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = 2;
    ac.largest_object = 0;
    CHECK(moq_session_accept_subscribe(p->sv, ssub, &ac, p->now) == MOQ_OK);
    pump(p, 4);
    moq_fill_t cfill = { 0 }, sfill = { 0 };
    bool got_c = false, got_s = false;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_CREATED) { cfill = ev.u.fill_created.fill; got_c = true; }
        moq_event_cleanup(&ev);
    }
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_REQUEST) { sfill = ev.u.fill_request.fill; got_s = true; }
        moq_event_cleanup(&ev);
    }
    CHECK(got_c && got_s);
    if (!got_c || !got_s) return;
    CHECK(moq_session_open_fill(p->sv, sfill, p->now) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = 0;
    oc.publisher_priority = 5;
    CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"a", 1, &oc.payload) == MOQ_OK);
    CHECK(moq_session_write_fill_object(p->sv, sfill, &oc, p->now) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
    pump(p, 4);
    int objects = 0, terminals = 0;
    uint32_t reason = 0;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_OBJECT) objects++;
        moq_event_cleanup(&ev);
    }
    CHECK(objects == 1);
    /* The fill stream on the client. */
    uint64_t sid = UINT64_MAX;
    moq_stream_ref_t ref = { 0 };
    for (size_t i = 0; i < p->c->rx_cap; i++)
        if (p->c->rx_streams[i].active && p->c->rx_streams[i].fill_bound)
            ref = p->c->rx_streams[i].stream_ref;
    for (uint32_t i = 0; i < p->cb->max_streams; i++)
        if (p->cb->streams[i].active && p->cb->streams[i].ref._v == ref._v)
            sid = p->cb->streams[i].transport_id;
    CHECK(ref._v != 0 && sid != UINT64_MAX);
    int slot = sub_resolve_handle(p->c, csub);
    CHECK(slot >= 0);
    if (slot < 0 || sid == UINT64_MAX) return;
    uint64_t processed0 = p->c->subs[slot].processed_stream_count;

    CHECK(moq_session_cancel_fill(p->c, cfill, p->now) == MOQ_OK);
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_TERMINAL) { terminals++; reason = ev.u.fill_terminal.reason; }
        moq_event_cleanup(&ev);
    }
    CHECK(terminals == 1 && reason == MOQ_FILL_LOCAL_CANCELLED);
    pump(p, 4);                                 /* STOP accepted; the answer is never reported */
    bridge_stream_entry_t *e = bridge_find_by_id(p->cb, sid);
    CHECK(e != NULL && e->local_stop_accepted);
    CHECK(session_owner(p->c, ref) == 2);
    CHECK(p->c->fills.live == 1);               /* the child still owes its stream's end */
    CHECK(p->c->subs[slot].processed_stream_count == processed0);

    CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
    CHECK(p->c->fills.live == 1);               /* nothing inside the report */
    moq_transport_bridge_service(p->cb, p->now);
    CHECK(bridge_find_by_id(p->cb, sid) == NULL);
    CHECK(session_owner(p->c, ref) == 0);
    CHECK(p->c->fills.live == 0);               /* reclaimed at the release */
    CHECK(p->c->subs[slot].processed_stream_count == processed0 + 1);   /* counted once */
    CHECK(!peer_fin_noted(p->c, ref));
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FILL_TERMINAL) terminals++;
        moq_event_cleanup(&ev);
    }
    CHECK(terminals == 1);                      /* no second terminal, no COMPLETE */
    CHECK(moq_session_cancel_fill(p->c, cfill, p->now) == MOQ_ERR_STALE_HANDLE);
    /* A repeated and a late report change nothing. */
    CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
    pump(p, 3);
    CHECK(p->c->subs[slot].processed_stream_count == processed0 + 1);
    CHECK(p->c->fills.live == 0);
    CHECK(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    CHECK(p->c->subs[slot].state == MOQ_SUB_ESTABLISHED);
}

/* The session's half of the release on a stream whose binding is still valid:
 * the done gate is waiting for its second stream (stream count 2, one FIN in).
 * The waiting stream is put in the state a STOP leaves it in (white-box: no
 * real flow stops a stream while keeping its binding live and the gate in
 * direct count mode) and released. It counts once: the gate completes and the
 * done surfaces exactly once; a second release finds nothing and counts
 * nothing; an identity-only stopped record releases without counting. */
static void run_count(pair_t *p, bool publication)
{
    track_t t;
    memset(&t, 0, sizeof(t));
    moq_publication_t spub = MOQ_PUBLICATION_INVALID;
    if (!publication) {
        if (!subscribe(p, &t)) { CHECK(!"subscribe"); return; }
    } else {
        moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
        moq_publish_cfg_t pc;
        moq_publish_cfg_init(&pc);
        pc.track_namespace = (moq_namespace_t){ nsp, 1 };
        pc.track_name = MOQ_BYTES_LITERAL("p");
        CHECK(moq_session_publish(p->sv, &pc, p->now, &spub) == MOQ_OK);
        pump(p, 4);
        moq_publication_t cpub = MOQ_PUBLICATION_INVALID;
        moq_event_t ev;
        while (moq_session_poll_events(p->c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) cpub = ev.u.publish_request.pub;
            moq_event_cleanup(&ev);
        }
        CHECK(moq_publication_is_valid(cpub));
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init(&ac);
        CHECK(moq_session_accept_publish(p->c, cpub, &ac, p->now) == MOQ_OK);
        pump(p, 4);
        while (moq_session_poll_events(p->sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    }
    for (uint64_t g = 0; g < 2; g++) {
        moq_subgroup_cfg_t sc;
        moq_subgroup_cfg_init(&sc);
        sc.group_id = g;
        moq_result_t rc = publication
            ? moq_session_open_pub_subgroup(p->sv, spub, &sc, p->now, &t.sg[g])
            : moq_session_open_subgroup(p->sv, t.ssub, &sc, p->now, &t.sg[g]);
        CHECK(rc == MOQ_OK);
        moq_rcbuf_t *b = NULL;
        uint8_t bytes[4] = { 1, 2, 3, (uint8_t)g };
        CHECK(moq_rcbuf_create(moq_alloc_default(), bytes, 4, &b) == MOQ_OK);
        CHECK(moq_session_write_object(p->sv, t.sg[g], 0, b, p->now) == MOQ_OK);
        moq_rcbuf_decref(b);
        pump(p, 3);
    }
    seen_t seen = { 0 };
    CHECK(moq_session_close_subgroup(p->sv, t.sg[0], p->now) == MOQ_OK);
    pump(p, 3);
    p->hold_fins = true;
    CHECK(moq_session_close_subgroup(p->sv, t.sg[1], p->now) == MOQ_OK);
    pump(p, 3);
    if (publication) {
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        fc.stream_count = 2;
        CHECK(moq_session_finish_publish(p->sv, spub, &fc, p->now) == MOQ_OK);
    } else {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.stream_count = 2;
        CHECK(moq_session_done_subscribe(p->sv, t.ssub, &dc, p->now) == MOQ_OK);
    }
    pump(p, 3);
    int done = 0;
    moq_event_t ev;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE || ev.kind == MOQ_EVENT_PUBLISH_FINISHED) done++;
        if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED) seen.finished++;
        moq_event_cleanup(&ev);
    }
    CHECK(done == 0);                     /* gated: one of two streams processed */
    CHECK(seen.finished == 1);
    /* The waiting stream: the only active receive entry left. */
    int slot = -1;
    for (size_t i = 0; i < p->c->rx_cap; i++)
        if (p->c->rx_streams[i].active) slot = (int)i;
    CHECK(slot >= 0);
    if (slot < 0) return;
    moq_stream_ref_t ref = p->c->rx_streams[slot].stream_ref;
    CHECK(publication ? moq_publication_is_valid(p->c->rx_streams[slot].pub_handle)
                      : moq_subscription_is_valid(p->c->rx_streams[slot].sub));
    p->c->rx_streams[slot].parse_state = MOQ_RX_STOPPED;
    CHECK(bridge_session_on_data_retired(p->c, ref, p->now) == MOQ_OK);
    CHECK(session_owner(p->c, ref) == 0);
    CHECK(!peer_fin_noted(p->c, ref));
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE || ev.kind == MOQ_EVENT_PUBLISH_FINISHED) done++;
        if (ev.kind == MOQ_EVENT_SUBGROUP_FINISHED || ev.kind == MOQ_EVENT_SUBGROUP_RESET) seen.other++;
        moq_event_cleanup(&ev);
    }
    CHECK(done == 1);                     /* counted once: the gate completed */
    CHECK(seen.other == 0);
    CHECK(bridge_session_on_data_retired(p->c, ref, p->now) == MOQ_ERR_STALE_HANDLE);
    /* An identity-only record releases without counting anything. */
    CHECK(p->c->rx_stopped_count < p->c->rx_stopped_cap);
    p->c->rx_stopped_refs[p->c->rx_stopped_count++] = 0x7777;
    CHECK(bridge_session_on_data_retired(p->c, moq_stream_ref_from_u64(0x7777), p->now) == MOQ_OK);
    CHECK(session_owner(p->c, moq_stream_ref_from_u64(0x7777)) == 0);
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE || ev.kind == MOQ_EVENT_PUBLISH_FINISHED) done++;
        moq_event_cleanup(&ev);
    }
    CHECK(done == 1);
    /* The bridge mapping for the stream is the adapter's to report; drop it. */
    pump(p, 2);
}

static void run(moq_version_t v, int which)
{
    char label[64];
    snprintf(label, sizeof(label), "d%u/%s", (unsigned)v, case_names[which]);
    g_case = label;
    int before = failures;
    bool skipped = false;
    pair_t *p = calloc(1, sizeof(*p));
    if (!p) { failures++; return; }
    opts_t o = { 0 };
    if (which == HELD) { o.client_max_data_streams = 1; o.client_hold_input = true; }
    if (which == FIN_FIRST) o.client_max_events = 2;
    if (which == EXPIRY) o.done_wait_us = 50000;
    if (!pair_open(p, v, &o)) { CHECK(!"pair open"); goto out; }
    if (which == FILL_RETIRE) {
        if (moq_session_supports_fill(p->c)) run_fill(p);
        else skipped = true;              /* this profile has no fills */
        goto out;
    }
    if (which == COUNT_SUB || which == COUNT_PUB) {
        run_count(p, which == COUNT_PUB);
        goto out;
    }
    track_t t;
    if (!subscribe(p, &t)) { CHECK(!"subscribe"); goto out; }
    seen_t seen = { 0 };

    if (which == CONTROL) {
        uint64_t ctl = moq_transport_bridge_uses_uni_control(p->cb)
            ? p->cb->peer_ctrl_uni_stream_id : p->cb->control_stream_id;
        size_t mapped = moq_transport_bridge_stream_count(p->cb);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, ctl, p->now) == MOQ_OK);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, 0x7fff0, p->now) == MOQ_OK);
        pump(p, 3);
        CHECK(moq_transport_bridge_stream_count(p->cb) == mapped);
        CHECK(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
        CHECK(!moq_transport_bridge_is_fatal(p->cb));
        goto out;
    }
    if (!open_group(p, &t, 0)) { CHECK(!"open group"); goto out; }
    uint64_t sid = t.sid[0];
    bridge_stream_entry_t *e = bridge_find_by_id(p->cb, sid);
    CHECK(e != NULL && e->kind == BRIDGE_STREAM_UNI && e->origin == BRIDGE_ORIGIN_PEER);
    if (!e) goto out;
    moq_stream_ref_t ref = e->ref;
    drain(p->c, &seen);
    CHECK(seen.objects >= 1);

    if (which == ACTIVE) {
        /* A report for a live stream releases nothing; its data still flows. */
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
        pump(p, 3);
        e = bridge_find_by_id(p->cb, sid);
        CHECK(e != NULL && e->active);
        CHECK(session_owner(p->c, ref) == 1);
        close_groups(p, &t);
        drain(p->c, &seen);
        CHECK(seen.finished == 1);
        CHECK(bridge_find_by_id(p->cb, sid) == NULL);
        goto out;
    }
    if (which == HELD) {
        /* A second stream, refused at admission (one receive entry), is held. */
        if (!open_group(p, &t, 1)) { CHECK(!"open group 1"); goto out; }
        bridge_stream_entry_t *h = bridge_find_by_id(p->cb, t.sid[1]);
        CHECK(h != NULL && h->held_input);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, t.sid[1], p->now) == MOQ_OK);
        pump(p, 3);
        h = bridge_find_by_id(p->cb, t.sid[1]);
        CHECK(h != NULL && h->held_input);
        goto out;
    }
    if (which == FIN_FIRST) {
        /* The event queue is full, so the next object and the FIN stay
         * pending in the bridge; the transport then finishes the stream and
         * the subscriber cancels. The real FIN reaches the session first and
         * settles the stream as a FIN. */
        moq_rcbuf_t *b = NULL;
        uint8_t bytes[4] = { 9, 9, 9, 9 };
        CHECK(moq_rcbuf_create(moq_alloc_default(), bytes, 4, &b) == MOQ_OK);
        for (uint64_t k = 1; k <= 3; k++)
            CHECK(moq_session_write_object(p->sv, t.sg[0], k, b, p->now) == MOQ_OK);
        moq_rcbuf_decref(b);
        CHECK(moq_session_close_subgroup(p->sv, t.sg[0], p->now) == MOQ_OK);
        moq_transport_bridge_service(p->sb, p->now);
        deliver(p, &p->sep, p->cb, false);
        e = bridge_find_by_id(p->cb, sid);
        CHECK(e != NULL && (e->pending_retry || e->pending_fin));
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
        CHECK(moq_session_unsubscribe(p->c, t.csub, p->now) == MOQ_OK);
        for (int i = 0; i < 6; i++) { pump(p, 1); drain(p->c, &seen); }
        CHECK(bridge_find_by_id(p->cb, sid) == NULL);
        CHECK(session_owner(p->c, ref) == 0);
        CHECK(peer_fin_noted(p->c, ref));     /* settled by the real FIN */
        CHECK(seen.finished == 0 && seen.resets == 0);
        goto out;
    }
    if (which == EXPIRY) {
        /* Stream count 1: the done waits for this stream, whose FIN never
         * arrives. The wait expires (the done surfaces, the stream is
         * stopped); then the transport finishes the stream and its stopped
         * owner is released, with no further event. */
        p->hold_fins = true;
        close_groups(p, &t);
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.status_code = 0x2;
        dc.stream_count = 1;
        CHECK(moq_session_done_subscribe(p->sv, t.ssub, &dc, p->now) == MOQ_OK);
        pump(p, 3);
        drain(p->c, &seen);
        CHECK(seen.done == 0);
        int slot = -1;
        for (size_t i = 0; i < p->c->sub_cap; i++)
            if (p->c->subs[i].state != MOQ_SUB_FREE &&
                p->c->subs[i].role == MOQ_SUB_ROLE_SUBSCRIBER) slot = (int)i;
        CHECK(slot >= 0);
        uint64_t processed0 = slot >= 0 ? p->c->subs[slot].processed_stream_count : 0;
        p->now += 200000;
        for (int i = 0; i < 4; i++) { pump(p, 1); drain(p->c, &seen); }
        CHECK(swallowed(p, sid));                 /* the expiry stopped it */
        e = bridge_find_by_id(p->cb, sid);
        CHECK(e != NULL && e->local_stop_accepted);
        int done_before = seen.done;
        uint64_t processed1 = slot >= 0 ? p->c->subs[slot].processed_stream_count : 0;
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
        for (int i = 0; i < 4; i++) { pump(p, 1); drain(p->c, &seen); }
        CHECK(bridge_find_by_id(p->cb, sid) == NULL);
        CHECK(session_owner(p->c, ref) == 0);
        CHECK(!peer_fin_noted(p->c, ref));
        CHECK(seen.finished == 0 && seen.resets == 0);
        CHECK(seen.done == 1);
        (void)processed0; (void)processed1; (void)done_before;
        goto out;
    }

    /* Cancel: the subscriber STOPs the stream (with its subscription). */
    if (which == RETRY) p->cep.block_stop = true;
    if (which == INLINE) p->inline_report = true;
    if (which == REFUSED) p->cep.fail_stop = true;
    CHECK(moq_session_unsubscribe(p->c, t.csub, p->now) == MOQ_OK);
    moq_transport_bridge_service(p->cb, p->now);
    if (which == REFUSED) {
        /* A refused STOP is a transport failure, not acceptance: nothing is
         * released, and the report afterwards changes nothing. */
        CHECK(moq_transport_bridge_is_fatal(p->cb));
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_ERR_CLOSED);
        goto out;
    }
    e = bridge_find_by_id(p->cb, sid);
    if (which == INLINE) {
        CHECK(p->inline_done);
        CHECK(p->inline_entry_kept);              /* not released inside the op */
        CHECK(e == NULL);                         /* released by the same pass */
        CHECK(session_owner(p->c, ref) == 0);
        goto after;
    }
    CHECK(e != NULL && e->active);
    if (!e) goto out;
    if (which == RETRY) {
        CHECK(!e->local_stop_accepted);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
        moq_transport_bridge_service(p->cb, p->now);
        e = bridge_find_by_id(p->cb, sid);
        CHECK(e != NULL && e->transport_final && !e->local_stop_accepted);
        CHECK(session_owner(p->c, ref) != 0);
        p->cep.block_stop = false;
        moq_transport_bridge_service(p->cb, p->now);   /* the retry is accepted */
        CHECK(bridge_find_by_id(p->cb, sid) == NULL);
        CHECK(session_owner(p->c, ref) == 0);
        goto after;
    }
    CHECK(e->local_stop_accepted);
    CHECK(session_owner(p->c, ref) >= 2);
    pump(p, 2);
    close_groups(p, &t);                           /* the FIN is never reported */
    e = bridge_find_by_id(p->cb, sid);
    CHECK(e != NULL && e->active);
    if (which == TEARDOWN) {
        /* The connection ends: the bridge goes terminal and a later report is
         * refused; nothing is fabricated for the stream. */
        CHECK(moq_transport_bridge_on_transport_close(p->cb, 0, p->now) == MOQ_OK);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_ERR_CLOSED);
        drain(p->c, &seen);
        CHECK(seen.finished == 0 && seen.resets == 0);
        goto out;
    }
    CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
    e = bridge_find_by_id(p->cb, sid);
    CHECK(e != NULL && e->active);                 /* nothing inside the report */
    moq_transport_bridge_service(p->cb, p->now);
    CHECK(bridge_find_by_id(p->cb, sid) == NULL);
    CHECK(session_owner(p->c, ref) == 0);
after:
    CHECK(!peer_fin_noted(p->c, ref));            /* no FIN claimed */
    {
        /* A repeated and an unknown report allocate nothing and revive
         * nothing. Entries are compared by identity: while the pair pumps,
         * only an entry already aborting may retire (the peer's reset
         * of a stream we abandoned settles it). */
        uint64_t ids[64];
        bool aborting[64];
        size_t n = 0;
        for (uint32_t i = 0; i < p->cb->max_streams && n < 64; i++)
            if (p->cb->streams[i].active) {
                ids[n] = p->cb->streams[i].transport_id;
                aborting[n++] = p->cb->streams[i].aborting;
            }
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now) == MOQ_OK);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, 0x7fff0, p->now) == MOQ_OK);
        pump(p, 2);
        CHECK(bridge_find_by_id(p->cb, sid) == NULL);
        CHECK(bridge_find_by_id(p->cb, 0x7fff0) == NULL);
        for (uint32_t i = 0; i < p->cb->max_streams; i++) {
            if (!p->cb->streams[i].active) continue;
            bool known = false;
            for (size_t k = 0; k < n; k++)
                known = known || ids[k] == p->cb->streams[i].transport_id;
            CHECK(known);                          /* no entry appeared */
        }
        for (size_t k = 0; k < n; k++)
            if (!aborting[k]) CHECK(bridge_find_by_id(p->cb, ids[k]) != NULL);
    }
    drain(p->c, &seen);
    CHECK(seen.finished == 0 && seen.resets == 0);
    CHECK(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    CHECK(!moq_transport_bridge_is_fatal(p->cb));
    /* The session keeps working: a fresh subscription is answered. */
    {
        track_t t2;
        CHECK(subscribe(p, &t2));
    }
out:
    pair_close(p);
    free(p);
    printf("%s %s\n", failures != before ? "FAIL" : skipped ? "SKIP" : "PASS", label);
}

int main(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                       MOQ_VERSION_DRAFT_22 };
    for (size_t v = 0; v < 3; v++)
        for (int c = 0; c < CASE_COUNT; c++)
            run(versions[v], c);
    if (failures) {
        fprintf(stderr, "test_bridge_local_retire: %d failure(s)\n", failures);
        return 1;
    }
    printf("PASS: bridge_local_retire\n");
    return 0;
}
