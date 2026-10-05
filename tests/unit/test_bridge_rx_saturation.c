/*
 * Receive-capacity saturation through the real transport bridge.
 *
 * Two cancelled FETCH responses retain STOPPED receive entries. Further
 * response streams are refused without consumption until capacity returns.
 * The transport fixture retains exact chunks and FIN, then replays them.
 *
 * The historical schedules cover a live response beyond the former stopped
 * record bound (--live-request), within that bound (--live-record), and a
 * byte-fragmented response interleaved with a second stream (--live-fragmented).
 * --admission additionally exercises RESET of held input and unrelated progress.
 * --hold remains an alias: every receiving endpoint now retains refused input.
 *
 * Live requests must deliver exact payloads and complete once, without failure
 * terminals. Cancelled responses retain owned STOP/FIN lifetime semantics.
 * Both drafts, bounded fake transport storage, balanced allocations, no network.
 */

#include "test_session_support.h"
#include "../support/fake_endpoint.h"
#include "../support/held_input.h"
#include <moq/moq.h>
#include <moq/transport_bridge.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

/* -- bridge pair over fake endpoints ------------------------------------ */

typedef struct pair {
    moq_session_t          *c, *sv;
    moq_transport_bridge_t *cb, *sb;
    fake_endpoint_t         cep, sep;
    test_held_input_t       server_input;
    test_alloc_state_t      alloc_state;
    moq_alloc_t             alloc;
    uint64_t                now;
    uint64_t                client_control_sid, server_control_sid;
    bool                    hold_client;      /* client ops are counted, not delivered */
    bool                    hold_input;       /* endpoint declares HOLD_INPUT: chunks refused
                                                 NOT_CONSUMED are kept and redelivered */
    struct { bool active; uint64_t sid; uint8_t data[512]; size_t len, first_len; bool fin, first_fin; } holds[32];
    int                     refusals;         /* NOT_CONSUMED results seen */
    uint64_t                stopped_sids[64]; /* uni streams the fetcher STOPped (delivered to the publisher) */
    int                     n_stopped;
    bool                    drop_client_stops;/* only STOP/RESET ops are dropped */
    bool                    fragment;         /* uni writes delivered byte-interleaved */
    uint64_t                truncate_sid;     /* this uni stream: only its first byte is delivered */
    int                     client_stops;     /* STOP ops the client bridge issued */
    int                     client_resets;
    uint64_t                last_stop_sid;
    bool                    deliver_error;
} pair_t;

static bool
is_bidi_id(uint64_t id)
{
    return (id >= 2000 && id < 3000) || (id >= 4000 && id < 5000);
}

/* The adapter side of MOQ_TRANSPORT_CAP_HOLD_INPUT: a chunk refused with
 * NOT_CONSUMED is kept (later bytes of that stream queue behind it, as the
 * transport would hold them) and redelivered once the stream's pending
 * state clears. */
static moq_result_t
hold_feed(pair_t *p, uint64_t sid, const uint8_t *data, size_t len, bool fin)
{
    for (int i = 0; i < 32; i++) {
        if (!p->holds[i].active || p->holds[i].sid != sid) continue;
        MOQ_TEST_CHECK(p->holds[i].len + len <= sizeof(p->holds[i].data));
        if (p->holds[i].len + len <= sizeof(p->holds[i].data)) {
            if (len > 0) memcpy(p->holds[i].data + p->holds[i].len, data, len);
            p->holds[i].len += len;
            if (fin) p->holds[i].fin = true;
        }
        return MOQ_OK;
    }
    moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(p->cb, sid, data, len, fin, p->now);
    if (rc == MOQ_ERR_INPUT_NOT_CONSUMED) {
        p->refusals++;
        for (int i = 0; i < 32; i++) {
            if (p->holds[i].active) continue;
            p->holds[i].active = true;
            p->holds[i].sid = sid;
            MOQ_TEST_CHECK(len <= sizeof(p->holds[i].data));
            if (len > 0) memcpy(p->holds[i].data, data, len < sizeof(p->holds[i].data) ? len : sizeof(p->holds[i].data));
            p->holds[i].len = len;
            p->holds[i].fin = fin;
            p->holds[i].first_len = len;
            p->holds[i].first_fin = fin;
            return rc;
        }
        MOQ_TEST_CHECK(!"hold slots exhausted");
    }
    return rc;
}

static int
holds_active(const pair_t *p)
{
    int n = 0;
    for (int i = 0; i < 32; i++) if (p->holds[i].active) n++;
    return n;
}

/* After a service pass: redeliver every held chunk whose stream is no
 * longer pending (the adapter's resume); a stream the bridge retired
 * meanwhile (RESET) is dropped. */
static void
hold_redeliver(pair_t *p)
{
    for (int i = 0; i < 32; i++) {
        if (!p->holds[i].active) continue;
        if (moq_transport_bridge_find_ref(p->cb, p->holds[i].sid)._v == 0) { p->holds[i].active = false; continue; }
        if (moq_transport_bridge_stream_has_pending(p->cb, p->holds[i].sid)) continue;
        uint8_t copy[512];
        size_t len = p->holds[i].len;
        size_t first_len = p->holds[i].first_len;
        bool fin = p->holds[i].fin;
        bool first_fin = p->holds[i].first_fin;
        memcpy(copy, p->holds[i].data, len);
        p->holds[i].active = false;
        p->now += 1;
        uint64_t sid = p->holds[i].sid;
        moq_result_t rc = hold_feed(p, sid, copy, first_len, first_fin);
        if ((rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_INPUT_NOT_CONSUMED) &&
            (len > first_len || (fin && !first_fin))) {
            moq_result_t tail_rc = hold_feed(p, sid, copy + first_len, len - first_len, fin);
            if (rc != MOQ_ERR_INPUT_NOT_CONSUMED) rc = tail_rc;
        }
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_INPUT_NOT_CONSUMED && rc != MOQ_ERR_CLOSED)
            p->deliver_error = true;
    }
}

/* Route one side's recorded endpoint operations into the other side's bridge
 * (the harness's own routing: bidi control for draft-16, uni control pair for
 * draft-18 where every bidi is a request stream). */
static bool
deliver(pair_t *p, fake_endpoint_t *from, moq_transport_bridge_t *to, bool from_client)
{
    bool any = false;
    bool uni_control = moq_transport_bridge_uses_uni_control(to);
    for (size_t i = 0; i < from->count; i++) {
        fake_op_t *o = &from->ops[i];
        moq_result_t rc = MOQ_OK;
        switch (o->kind) {
        case FAKE_OP_WRITE:
            if (p->fragment && !from_client && !is_bidi_id(o->stream_id))
                break;   /* delivered byte-interleaved after this loop */
            if (!from_client && !is_bidi_id(o->stream_id) && o->stream_id == p->truncate_sid) {
                /* A partial header: only the stream's first byte ever arrives. */
                if (o->data_len > 0)
                    rc = moq_transport_bridge_on_peer_uni_bytes(to, o->stream_id, o->data, 1, false, p->now);
                p->truncate_sid = UINT64_MAX - 1;   /* later writes on it are withheld entirely */
                if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_CLOSED) p->deliver_error = true;
                any = true;
                break;
            }
            if (is_bidi_id(o->stream_id)) {
                bool control = false;
                if (!uni_control) {
                    uint64_t *sid = from_client ? &p->client_control_sid : &p->server_control_sid;
                    if (*sid == UINT64_MAX) *sid = o->stream_id;
                    control = o->stream_id == p->client_control_sid ||
                              o->stream_id == p->server_control_sid;
                }
                rc = control ? moq_transport_bridge_on_peer_control_bytes(to, o->stream_id, o->data, o->data_len,
                                                                          o->fin, p->now)
                             : moq_transport_bridge_on_peer_bidi_bytes(to, o->stream_id, o->data, o->data_len,
                                                                       o->fin, p->now);
            } else if (p->hold_input && !from_client) {
                rc = hold_feed(p, o->stream_id, o->data, o->data_len, o->fin);
            } else {
                rc = test_hold_uni(&p->server_input, to, o->stream_id, o->data, o->data_len, o->fin, p->now);
            }
            break;
        case FAKE_OP_RESET:
            rc = moq_transport_bridge_on_peer_stream_reset(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_STOP:
            rc = moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
            if (from_client && !is_bidi_id(o->stream_id) && p->n_stopped < 64)
                p->stopped_sids[p->n_stopped++] = o->stream_id;
            break;
        case FAKE_OP_ABORT:
            (void)moq_transport_bridge_on_peer_stream_reset(to, o->stream_id, o->error_code, p->now);
            rc = moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_DATAGRAM:
            rc = moq_transport_bridge_on_peer_datagram(to, o->data, o->data_len, p->now);
            break;
        case FAKE_OP_CLOSE:
            rc = moq_transport_bridge_on_transport_close(to, o->error_code, p->now);
            break;
        default:
            break;
        }
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_INPUT_NOT_CONSUMED)
            p->deliver_error = true;
        any = true;
    }
    /* Fragmented delivery (publisher side only): the uni writes of this pass
     * are delivered one byte per call, round-robin across STREAMS (a stream's
     * own writes stay in order), so a stream's header arrives interleaved
     * with other streams' bytes. Control/bidi writes of the pass were
     * delivered above, keeping the publisher's control-before-data order. */
    if (p->fragment && !from_client) {
        uint64_t sids[FAKE_EP_MAX_OPS];
        size_t   op_at[FAKE_EP_MAX_OPS];    /* current op index per stream */
        size_t   pos[FAKE_EP_MAX_OPS];      /* byte position within that op */
        size_t   nstreams = 0;
        for (size_t i = 0; i < from->count; i++) {
            fake_op_t *o = &from->ops[i];
            if (o->kind != FAKE_OP_WRITE || is_bidi_id(o->stream_id)) continue;
            size_t j = 0;
            while (j < nstreams && sids[j] != o->stream_id) j++;
            if (j == nstreams) { sids[nstreams] = o->stream_id; op_at[nstreams] = i; pos[nstreams] = 0; nstreams++; }
        }
        bool progress = true;
        while (progress) {
            progress = false;
            for (size_t j = 0; j < nstreams; j++) {
                /* advance to this stream's current undelivered op */
                while (op_at[j] < from->count) {
                    fake_op_t *o = &from->ops[op_at[j]];
                    if (o->kind == FAKE_OP_WRITE && o->stream_id == sids[j] &&
                        (pos[j] < o->data_len || (o->data_len == 0 && pos[j] == 0)))
                        break;
                    op_at[j]++;
                    pos[j] = 0;
                }
                if (op_at[j] >= from->count) continue;
                fake_op_t *o = &from->ops[op_at[j]];
                bool last = o->data_len == 0 || pos[j] + 1 >= o->data_len;
                moq_result_t rc;
                if (p->hold_input)
                    rc = hold_feed(p, o->stream_id, o->data_len ? o->data + pos[j] : NULL, o->data_len ? 1 : 0,
                                   last && o->fin);
                else
                    rc = moq_transport_bridge_on_peer_uni_bytes(
                        to, o->stream_id, o->data_len ? o->data + pos[j] : NULL, o->data_len ? 1 : 0,
                        last && o->fin, p->now);
                if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_INPUT_NOT_CONSUMED)
                    p->deliver_error = true;
                pos[j] = o->data_len ? pos[j] + 1 : 1;
                progress = true;
                any = true;
            }
        }
    }
    fake_endpoint_clear_ops(from);
    return any;
}

/* Service the client bridge; its STOP/RESET ops are counted. With the hold
 * on they are dropped instead of delivered (the publisher never learns of
 * them, so its streams stay open and the fetcher's pool stays saturated). */
static void
service_client(pair_t *p)
{
    moq_transport_bridge_service(p->cb, p->now);
    if (p->hold_input) hold_redeliver(p);
    for (size_t i = 0; i < p->cep.count; i++) {
        /* Only the publisher's uni data streams count: a draft-18 cancel also
         * STOPs/RESETs the fetcher's own request bidi. */
        if (is_bidi_id(p->cep.ops[i].stream_id)) continue;
        if (p->cep.ops[i].kind == FAKE_OP_STOP) { p->client_stops++; p->last_stop_sid = p->cep.ops[i].stream_id; }
        if (p->cep.ops[i].kind == FAKE_OP_RESET) p->client_resets++;
    }
    MOQ_TEST_CHECK(!p->cep.overflowed && !p->cep.truncated);
    if (p->hold_client) { fake_endpoint_clear_ops(&p->cep); return; }
    if (p->drop_client_stops) {
        size_t w = 0;
        for (size_t i = 0; i < p->cep.count; i++) {
            bool uni_stop = !is_bidi_id(p->cep.ops[i].stream_id) &&
                            (p->cep.ops[i].kind == FAKE_OP_STOP || p->cep.ops[i].kind == FAKE_OP_RESET);
            if (!uni_stop) p->cep.ops[w++] = p->cep.ops[i];
        }
        p->cep.count = w;
    }
    (void)deliver(p, &p->cep, p->sb, true);
}

static void
service_server(pair_t *p)
{
    moq_transport_bridge_service(p->sb, p->now);
    MOQ_TEST_CHECK(test_replay_uni(&p->server_input, p->sb, p->now) == MOQ_OK);
    MOQ_TEST_CHECK(!p->sep.overflowed && !p->sep.truncated);
    (void)deliver(p, &p->sep, p->cb, false);
}

static void
pump(pair_t *p, int rounds)
{
    for (int i = 0; i < rounds; i++) {
        p->now += 10;
        service_client(p);
        service_server(p);
    }
}

static bool g_hold_input = true;    /* Receiving endpoints always retain refused input. */

static bool
pair_open(pair_t *p, moq_version_t version, uint32_t max_data_streams, uint32_t max_fetches)
{
    memset(p, 0, sizeof(*p));
    p->hold_input = g_hold_input;
    p->alloc = test_allocator(&p->alloc_state);
    p->now = 1000;
    p->client_control_sid = UINT64_MAX;
    p->server_control_sid = UINT64_MAX;
    p->truncate_sid = UINT64_MAX;
    moq_session_cfg_t ccfg, scfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), &p->alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = version;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 64;
    ccfg.max_data_streams = max_data_streams;
    ccfg.max_fetches = max_fetches;
    ccfg.max_events = 64;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.version = version;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;
    scfg.max_fetches = 32;
    scfg.max_events = 64;
    if (moq_session_create(&ccfg, p->now, &p->c) < 0) return false;
    if (moq_session_create(&scfg, p->now, &p->sv) < 0) return false;
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    if (p->hold_input) p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&bcfg, p->c, &p->cep.vtable, &p->cep, &p->cb) < 0) return false;
    if (moq_transport_bridge_create(&bcfg, p->sv, &p->sep.vtable, &p->sep, &p->sb) < 0) return false;
    if (moq_session_start(p->c, p->now) < 0) return false;
    if (version == MOQ_VERSION_DRAFT_18 && moq_session_start(p->sv, p->now) < 0) return false;
    pump(p, 6);
    moq_event_t ev;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED && moq_session_state(p->sv) == MOQ_SESS_ESTABLISHED &&
           !moq_transport_bridge_is_fatal(p->cb) && !moq_transport_bridge_is_fatal(p->sb);
}

static void
pair_close(pair_t *p)
{
    moq_transport_bridge_destroy(p->cb);
    moq_transport_bridge_destroy(p->sb);
    moq_session_destroy(p->c);
    moq_session_destroy(p->sv);
    MOQ_TEST_CHECK_EQ_INT(p->alloc_state.balance, 0);   /* no ownership leak */
}

/* -- fetch helpers ------------------------------------------------------- */

typedef struct obs {
    int ok, object, complete, error, reset, cancelled, closed, other;
    uint64_t close_code;
    char close_reason[64];
} obs_t;


static void
drain(moq_session_t *s, obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_OK:        o->ok++; break;
        case MOQ_EVENT_FETCH_OBJECT:
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.fetch_object.payload) == 16);
            for (size_t j = 0; j < moq_rcbuf_len(ev.u.fetch_object.payload); j++)
                MOQ_TEST_CHECK(moq_rcbuf_data(ev.u.fetch_object.payload)[j] ==
                               (uint8_t)(0xd0 + ev.u.fetch_object.object_id));
            o->object++;
            break;
        case MOQ_EVENT_FETCH_COMPLETE:  o->complete++; break;
        case MOQ_EVENT_FETCH_ERROR:     o->error++; break;
        case MOQ_EVENT_FETCH_RESET:     o->reset++; break;
        case MOQ_EVENT_FETCH_CANCELLED: o->cancelled++; break;
        case MOQ_EVENT_SESSION_CLOSED: {
            o->closed++;
            o->close_code = ev.u.closed.code;
            size_t n = ev.u.closed.reason.len < sizeof(o->close_reason) - 1 ? ev.u.closed.reason.len
                                                                           : sizeof(o->close_reason) - 1;
            if (n) memcpy(o->close_reason, ev.u.closed.reason.data, n);
            o->close_reason[n] = '\0';
            break;
        }
        default:                        o->other++; break;
        }
        moq_event_cleanup(&ev);
    }
}

static moq_result_t
issue_fetch(moq_session_t *s, uint64_t now, moq_fetch_t *h)
{
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    fc.track_name = MOQ_BYTES_LITERAL("t");
    fc.end_group = 0;
    fc.end_object = 3;
    return moq_session_fetch(s, &fc, now, h);
}

static moq_result_t
write_obj(moq_session_t *pub, moq_fetch_t fh, uint64_t oid, uint64_t now)
{
    uint8_t body[16];
    memset(body, (int)(0xd0 + oid), sizeof(body));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) != MOQ_OK) return MOQ_ERR_NOMEM;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.subgroup_id = 0;
    oc.object_id = oid;
    oc.publisher_priority = 100;
    oc.payload = pl;
    moq_result_t rc = moq_session_write_fetch_object(pub, fh, &oc, now);
    moq_rcbuf_decref(pl);
    return rc;
}

/* The server-opened uni stream id carrying fetch k's response: the next id
 * the server endpoint hands out when the response is serviced. */
#define N_REQ      19          /* 2 stopped entries + 16 records + the saturating one */
#define N_HELD     18
#define RX_ENTRIES 2u

typedef enum mode {
    MODE_CANCELLED = 0,     /* the saturating stream answers a cancelled request */
    MODE_LIVE,              /* live response beyond the former stopped-record bound */
    MODE_LIVE_RECORD,       /* live response within that former bound */
    MODE_LIVE_FRAGMENTED,   /* MODE_LIVE with byte-interleaved delivery */
} mode_t_;

static const char *
mode_name(mode_t_ m)
{
    switch (m) {
    case MODE_CANCELLED:       return "cancelled";
    case MODE_LIVE:            return "live";
    case MODE_LIVE_RECORD:     return "live-record";
    default:                   return "live-fragmented";
    }
}

static int
run(moq_version_t version, mode_t_ mode)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    int before = failures;
    bool live_request = mode != MODE_CANCELLED;
    /* Index of the live request: the saturating stream, or a record-path one. */
    int live_idx = mode == MODE_LIVE_RECORD ? 2 : N_HELD;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, RX_ENTRIES, 32));
    p.fragment = mode == MODE_LIVE_FRAGMENTED;

    /* All requests reach the publisher. */
    moq_fetch_t fh[N_REQ + 1];
    for (int k = 0; k < N_REQ; k++) MOQ_TEST_CHECK(issue_fetch(p.c, p.now, &fh[k]) == MOQ_OK);
    pump(&p, 6);
    moq_fetch_t sfh[N_REQ];
    int req = 0;
    {
        moq_event_t ev;
        while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST && req < N_REQ) sfh[req++] = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK(req == N_REQ);
    obs_t co = {0};
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.ok == 0 && co.error == 0);

    /* The fetcher cancels the requests whose responses will be late; the
     * publisher never learns of it (its cancels and STOPs are held), so every
     * response it writes from here on is a late stream. */
    for (int k = 0; k < N_REQ; k++) {
        if (live_request && k == live_idx) continue;
        MOQ_TEST_CHECK(moq_session_fetch_cancel(p.c, fh[k], p.now) == MOQ_OK);
    }
    p.hold_client = true;
    p.now += 10;
    service_client(&p);
    MOQ_TEST_CHECK(p.client_stops == 0);

    /* The publisher answers every request with a header and one object, no
     * end: each response stream stays open. Delivered one response at a time
     * so the fetcher's view is observed after each. */
    uint64_t sid[N_REQ];
    size_t bridge_streams_before = moq_transport_bridge_stream_count(p.cb);
    for (int k = 0; k < N_REQ; k++) {
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 0;
        ac.end_object = 3;
        MOQ_TEST_CHECK(moq_session_accept_fetch(p.sv, sfh[k], &ac, p.now) == MOQ_OK);
        MOQ_TEST_CHECK(write_obj(p.sv, sfh[k], 0, p.now) == MOQ_OK);
        sid[k] = p.sep.next_uni_id;
        /* Fragmented: the saturating stream's header is interleaved with a
         * second object of the first held stream, written in the same pass. */
        if (mode == MODE_LIVE_FRAGMENTED && k == N_HELD)
            MOQ_TEST_CHECK(write_obj(p.sv, sfh[1], 1, p.now) == MOQ_OK);
        int stops_before = p.client_stops;
        p.now += 10;
        service_server(&p);
        service_client(&p);
        MOQ_TEST_CHECK(!p.deliver_error && !moq_transport_bridge_is_fatal(p.cb));
        MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
        moq_stream_ref_t ref = moq_transport_bridge_find_ref(p.cb, sid[k]);
        MOQ_TEST_CHECK(ref._v != 0);
        bool owned = moq_session_has_transport_stream(p.c, ref);
        /* Two parsed late responses are STOPped; later responses are held. */
        if (k < (int)RX_ENTRIES) MOQ_TEST_CHECK(p.client_stops == stops_before + 1 && owned);
        else MOQ_TEST_CHECK(p.client_stops == stops_before && !owned);
        MOQ_TEST_CHECK(moq_transport_bridge_stream_count(p.cb) == bridge_streams_before + (size_t)k + 1);
        if (p.hold_input && k >= (int)RX_ENTRIES) continue;
    }
    if (p.hold_input) {
        /* Every response the pool could not admit was
         * refused whole and is held by the adapter -- no STOP, no record, no
         * discard. Once the fetcher's STOPs reach the publisher (the hold on
         * client ops is released) its resets free entries, the held chunks
         * are admitted one by one, and the live request's response is
         * eventually parsed: its promised object arrives and the request
         * COMPLETES when the publisher ends it. No failure terminal. */
        drain(p.c, &co);
        MOQ_TEST_CHECK(co.ok == (live_request ? 1 : 0) && co.object == 0 && co.complete == 0 && co.closed == 0);
        printf("%s %s (hold): refusals=%d held=%d stops=%d\n", lbl, mode_name(mode), p.refusals, holds_active(&p),
               p.client_stops);
        MOQ_TEST_CHECK(p.refusals >= 1 && holds_active(&p) >= 1);
        MOQ_TEST_CHECK(p.client_stops == RX_ENTRIES);   /* only the two parsed late responses */
        p.hold_client = false;
        /* The fetcher's held STOPs were dropped, not delayed, so the two
         * parsed late responses must be ended by the publisher to free their
         * entries; from then on each admitted late response is STOPped, the
         * STOP reaches the publisher, its RESET frees the entry, and the next
         * held chunk is admitted. */
        for (int k = 0; k < (int)RX_ENTRIES; k++)
            MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[k], p.now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[live_idx], p.now) == MOQ_OK);
        obs_t tot = {0};
        int rounds = 0;
        bool ended[N_REQ + 1] = { false };
        int ended_count = 0;
        while (rounds++ < 200) {
            pump(&p, 1);
            /* A publisher that honours STOP_SENDING ends the stopped response
             * (the fetch): its FIN frees the fetcher's entry. */
            for (int i = 0; i < p.n_stopped; i++) {
                for (int k = 0; k < N_REQ; k++) {
                    if (sid[k] != p.stopped_sids[i] || ended[k]) continue;
                    ended[k] = true;
                    ended_count++;
                    (void)moq_session_end_fetch(p.sv, sfh[k], p.now);
                }
            }
            memset(&co, 0, sizeof(co));
            drain(p.c, &co);
            tot.ok += co.ok; tot.object += co.object; tot.complete += co.complete; tot.error += co.error;
            tot.reset += co.reset; tot.closed += co.closed;
            /* done once nothing is held, the live request (if any) completed
             * and the publisher has ended every stopped response (an entry is
             * free again) */
            bool settled = holds_active(&p) == 0 && moq_session_can_admit_data_stream(p.c);
            if (!live_request && settled) break;
            if (live_request && tot.complete >= 1 && settled) break;
        }
        printf("%s %s (hold): rounds=%d objects=%d complete=%d error=%d reset=%d holds=%d stops=%d ended=%d state=%d\n",
               lbl, mode_name(mode), rounds, tot.object, tot.complete, tot.error, tot.reset, holds_active(&p),
               p.client_stops, ended_count, (int)moq_session_state(p.c));
        MOQ_TEST_CHECK(holds_active(&p) == 0);
        MOQ_TEST_CHECK(tot.closed == 0 && moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(!p.deliver_error && !moq_transport_bridge_is_fatal(p.cb) && !moq_transport_bridge_is_fatal(p.sb));
        if (live_request) {
            MOQ_TEST_CHECK(tot.object == 1 && tot.complete == 1 && tot.error == 0 && tot.reset == 0);
            MOQ_TEST_CHECK(fetch_resolve_handle(p.c, fh[live_idx]) < 0);
            MOQ_TEST_CHECK(moq_session_fetch_cancel(p.c, fh[live_idx], p.now) == MOQ_ERR_STALE_HANDLE);
        } else {
            MOQ_TEST_CHECK(tot.object == 0 && tot.complete == 0 && tot.error == 0 && tot.reset == 0);
        }
        /* Unrelated progress afterwards. */
        MOQ_TEST_CHECK(issue_fetch(p.c, p.now, &fh[N_REQ]) == MOQ_OK);
        pump(&p, 4);
        {
            moq_event_t ev;
            moq_fetch_t sh = { 0 };
            int got = 0;
            while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
                if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sh = ev.u.fetch_request.fetch; got++; }
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK(got == 1);
            moq_accept_fetch_cfg_t ac;
            moq_accept_fetch_cfg_init(&ac);
            ac.end_group = 0;
            ac.end_object = 3;
            MOQ_TEST_CHECK(moq_session_accept_fetch(p.sv, sh, &ac, p.now) == MOQ_OK);
            MOQ_TEST_CHECK(write_obj(p.sv, sh, 0, p.now) == MOQ_OK);
            MOQ_TEST_CHECK(write_obj(p.sv, sh, 1, p.now) == MOQ_OK);
            MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sh, p.now) == MOQ_OK);
        }
        pump(&p, 12);
        memset(&co, 0, sizeof(co));
        drain(p.c, &co);
        MOQ_TEST_CHECK(co.ok == 1 && co.object == 2 && co.complete == 1 && co.closed == 0);
        pair_close(&p);
        if (failures == before) printf("PASS: bridge_rx_saturation %s %s (hold)\n", lbl, mode_name(mode));
        return failures - before;
    }
    return failures - before;
}


/* The admission schedule with the endpoint opted in: three live responses
 * are refused whole (NOT_CONSUMED), held by the adapter and nothing else
 * happens to them; unrelated streams progress; a RESET of a held stream
 * retires it; as the publisher ends live streams, held chunks are admitted
 * in turn and their requests complete with their objects. */
static int
run_admission(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    int before = failures;
    const int N_LIVE = 2, N_WAIT = 3, N = N_LIVE + N_WAIT;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, RX_ENTRIES, 32));
    moq_fetch_t fh[8], sfh[8];
    obs_t co = {0};
    for (int k = 0; k < N; k++) MOQ_TEST_CHECK(issue_fetch(p.c, p.now, &fh[k]) == MOQ_OK);
    pump(&p, 6);
    {
        moq_event_t ev;
        int got = 0;
        while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST && got < N) sfh[got++] = ev.u.fetch_request.fetch;
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got == N);
    }
    for (int k = 0; k < N_LIVE; k++) {
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 0;
        ac.end_object = 3;
        MOQ_TEST_CHECK(moq_session_accept_fetch(p.sv, sfh[k], &ac, p.now) == MOQ_OK);
        MOQ_TEST_CHECK(write_obj(p.sv, sfh[k], 0, p.now) == MOQ_OK);
        pump(&p, 4);
    }
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.ok == 2 && co.object == 2 && co.complete == 0 && co.closed == 0);
    uint64_t wsid[8];
    for (int i = 0; i < N_WAIT; i++) {
        int k = N_LIVE + i;
        moq_accept_fetch_cfg_t ac;
        moq_accept_fetch_cfg_init(&ac);
        ac.end_group = 0;
        ac.end_object = 3;
        MOQ_TEST_CHECK(moq_session_accept_fetch(p.sv, sfh[k], &ac, p.now) == MOQ_OK);
        MOQ_TEST_CHECK(write_obj(p.sv, sfh[k], 0, p.now) == MOQ_OK);
        wsid[i] = p.sep.next_uni_id;
        int refusals_before = p.refusals;
        p.now += 10;
        service_server(&p);
        service_client(&p);
        moq_stream_ref_t ref = moq_transport_bridge_find_ref(p.cb, wsid[i]);
        bool owned = ref._v != 0 && moq_session_has_transport_stream(p.c, ref);
        memset(&co, 0, sizeof(co));
        drain(p.c, &co);
        printf("%s admission (hold): waiting #%d refused=%d owned=%d stops=%d ok=%d objects=%d\n", lbl, i + 1,
               p.refusals - refusals_before, (int)owned, p.client_stops, co.ok, co.object);
        MOQ_TEST_CHECK(p.refusals == refusals_before + 1 && !owned && holds_active(&p) == i + 1);
        MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, wsid[i]));
        MOQ_TEST_CHECK(p.client_stops == 0);
        MOQ_TEST_CHECK(co.ok == 1 && co.object == 0 && co.complete == 0 && co.error == 0 && co.reset == 0);
        MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    }
    /* Unrelated traffic keeps flowing while the waiting streams are held. */
    MOQ_TEST_CHECK(write_obj(p.sv, sfh[0], 1, p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.object == 1 && co.closed == 0 && holds_active(&p) == N_WAIT);
    /* The publisher aborts request 4's response while it is held: retired. */
    {
        moq_result_t rc = moq_transport_bridge_on_peer_stream_reset(p.cb, wsid[2], 0x7, p.now);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, wsid[2])._v == 0);
        service_client(&p);   /* the adapter drops its held chunk */
        MOQ_TEST_CHECK(holds_active(&p) == N_WAIT - 1);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb) && moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    }
    /* Capacity returns twice: the publisher ends the live streams; each held
     * response is admitted in turn, delivers its object and completes once
     * the publisher ends it. */
    obs_t tot = {0};
    for (int k = 0; k < N_LIVE; k++) {
        MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[k], p.now) == MOQ_OK);
        pump(&p, 4);
        MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[N_LIVE + k], p.now) == MOQ_OK);
        pump(&p, 4);
        memset(&co, 0, sizeof(co));
        drain(p.c, &co);
        tot.object += co.object; tot.complete += co.complete; tot.error += co.error; tot.reset += co.reset;
        tot.closed += co.closed;
    }
    printf("%s admission (hold): objects=%d complete=%d error=%d reset=%d holds=%d stops=%d\n", lbl, tot.object,
           tot.complete, tot.error, tot.reset, holds_active(&p), p.client_stops);
    MOQ_TEST_CHECK(tot.object == 2 && tot.complete == 4 && tot.error == 0 && tot.reset == 0 && tot.closed == 0);
    MOQ_TEST_CHECK(holds_active(&p) == 0 && p.client_stops == 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(p.c, fh[N_LIVE]) < 0 && fetch_resolve_handle(p.c, fh[N_LIVE + 1]) < 0);
    MOQ_TEST_CHECK(!p.deliver_error && !moq_transport_bridge_is_fatal(p.cb));
    pair_close(&p);
    if (failures == before) printf("PASS: bridge_rx_saturation %s admission (hold)\n", lbl);
    return failures - before;
}

int main(int argc, char **argv)
{
    const char *arg = argc > 1 ? argv[1] : "";
    for (int i = 1; i < argc; i++) if (strcmp(argv[i], "--hold") == 0) g_hold_input = true;
    if (argc > 1 && strcmp(argv[1], "--hold") == 0) arg = argc > 2 ? argv[2] : "";
    if (strcmp(arg, "--admission") == 0) {
        (void)run_admission(MOQ_VERSION_DRAFT_16);
        (void)run_admission(MOQ_VERSION_DRAFT_18);
        if (failures == 0) MOQ_TEST_PASS("bridge_rx_saturation_admission");
        return failures ? 1 : 0;
    }
    mode_t_ mode = MODE_CANCELLED;
    if (strcmp(arg, "--live-request") == 0) mode = MODE_LIVE;
    else if (strcmp(arg, "--live-record") == 0) mode = MODE_LIVE_RECORD;
    else if (strcmp(arg, "--live-fragmented") == 0) mode = MODE_LIVE_FRAGMENTED;
    (void)run(MOQ_VERSION_DRAFT_16, mode);
    (void)run(MOQ_VERSION_DRAFT_18, mode);
    if (failures == 0) MOQ_TEST_PASS(mode == MODE_CANCELLED ? "bridge_rx_saturation" : "bridge_rx_saturation_live");
    return failures ? 1 : 0;
}
