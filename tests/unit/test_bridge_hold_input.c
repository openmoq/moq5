/*
 * Receive admission backpressure through the real transport bridge with an
 * endpoint that declares MOQ_TRANSPORT_CAP_HOLD_INPUT.
 *
 * A new peer uni data stream that the session cannot admit (no free receive
 * entry) is refused whole with MOQ_ERR_INPUT_NOT_CONSUMED: zero bytes and no
 * FIN consumed, nothing STOPped, nothing surfaced. The harness plays the
 * adapter: it keeps exactly that chunk and redelivers it once the stream's
 * pending state clears. Streams the session already owns keep the retained-
 * input contract (WOULD_BLOCK), control and padding are never gated, and an
 * accepted endpoint cannot withdraw the lossless lifetime contract.
 *
 * Rows: byte and rcbuf entry points; draft-16 (bidi control, every uni is
 * data), draft-18 and draft-22 (uni-control pair, classified first); a classification
 * prefix retained across an earlier call surviving the refusal; repeated
 * refusal; capacity taken by another stream before the redelivery; retained-
 * input pressure on an owned stream; FIN with payload and FIN-only while
 * pending; RESET while pending; teardown while pending; unrelated stream and
 * control progress; held refs and allocator balance returning to zero.
 */

#include "test_session_support.h"
#include "test_binding_support.h"
#include "../support/fake_endpoint.h"
#include "../support/held_input.h"
#include <moq/moq.h>
#include <moq/transport_bridge.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

/* Rows of the bounded-hold policy also advertise
 * MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT on the fetcher's endpoint. */
static bool g_hold_bidi = false;
#define HOLD_AGE_US 5000000ULL

/* -- pair over fake endpoints ------------------------------------------- */

#define HOLD_MAX 8

typedef struct held {
    bool     active;
    uint64_t sid;
    uint8_t  data[512];
    size_t   len;
    bool     fin;
    size_t   first_len;
    bool     first_fin;
    int      refusals;      /* NOT_CONSUMED results seen for this stream */
} held_t;

typedef struct pair {
    moq_session_t          *c, *sv;
    moq_transport_bridge_t *cb, *sb;
    fake_endpoint_t         cep, sep;
    test_held_input_t       server_input;
    test_alloc_state_t      alloc_state;
    moq_alloc_t             alloc;
    uint64_t                now;
    uint64_t                client_control_sid, server_control_sid;
    bool                    hold_client;          /* client ops counted, not delivered */
    bool                    use_rcbuf;            /* feed peer uni data through the rcbuf entry */
    int                     client_stops, client_resets;
    held_t                  held[HOLD_MAX];       /* chunks the "adapter" is holding */
    bool                    deliver_error;
    int                     unexpected_rc;
} pair_t;

static bool
is_bidi_id(uint64_t id)
{
    return (id >= 2000 && id < 3000) || (id >= 4000 && id < 5000);
}

static held_t *
held_find(pair_t *p, uint64_t sid)
{
    for (int i = 0; i < HOLD_MAX; i++)
        if (p->held[i].active && p->held[i].sid == sid) return &p->held[i];
    return NULL;
}

static int
held_count(pair_t *p)
{
    int n = 0;
    for (int i = 0; i < HOLD_MAX; i++) if (p->held[i].active) n++;
    return n;
}

/* Feed one peer uni data chunk to the client bridge the way an adapter with
 * the hold capability must: on NOT_CONSUMED keep the chunk (first refusal
 * stores it; a redelivery that is refused again keeps it as is). */
static moq_result_t
feed_uni(pair_t *p, uint64_t sid, const uint8_t *data, size_t len, bool fin)
{
    moq_result_t rc;
    if (p->use_rcbuf && len > 0) {
        moq_rcbuf_t *rb = NULL;
        if (moq_rcbuf_create(moq_alloc_default(), data, len, &rb) != MOQ_OK) return MOQ_ERR_NOMEM;
        rc = moq_transport_bridge_on_peer_uni_rcbuf(p->cb, sid, rb, fin, p->now);
        moq_rcbuf_decref(rb);
    } else {
        rc = moq_transport_bridge_on_peer_uni_bytes(p->cb, sid, data, len, fin, p->now);
    }
    if (rc == MOQ_ERR_INPUT_NOT_CONSUMED) {
        held_t *h = held_find(p, sid);
        if (!h) {
            for (int i = 0; i < HOLD_MAX && !h; i++) if (!p->held[i].active) h = &p->held[i];
            MOQ_TEST_CHECK(h != NULL);
            if (!h) return rc;
            memset(h, 0, sizeof(*h));
            h->active = true;
            h->sid = sid;
            if (len > sizeof(h->data)) { MOQ_TEST_CHECK(len <= sizeof(h->data)); return rc; }
            if (len > 0) memcpy(h->data, data, len);
            h->len = len;
            h->fin = fin;
            h->first_len = len;
            h->first_fin = fin;
        }
        h->refusals++;
    } else if (rc >= 0 || rc == MOQ_ERR_WOULD_BLOCK) {
        /* Bridge WOULD_BLOCK is owned input; only NOT_CONSUMED needs replay.
         * redeliver() snapshots any later tail/FIN before retiring this record. */
        held_t *h = held_find(p, sid);
        if (h) h->active = false;     /* the held chunk was taken */
    }
    return rc;
}

/* Redeliver the held chunk of `sid` exactly as it was refused. */
static moq_result_t
redeliver(pair_t *p, uint64_t sid)
{
    held_t *h = held_find(p, sid);
    MOQ_TEST_CHECK(h != NULL);
    if (!h) return MOQ_ERR_INVAL;
    uint8_t copy[512];
    size_t len = h->len, first_len = h->first_len;
    bool fin = h->fin, first_fin = h->first_fin;
    memcpy(copy, h->data, len);
    p->now += 10;
    moq_result_t rc = feed_uni(p, sid, copy, first_len, first_fin);
    if (rc == MOQ_ERR_INPUT_NOT_CONSUMED) return rc;
    if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK) return rc;
    if (len > first_len || (fin && !first_fin))
        return feed_uni(p, sid, copy + first_len, len - first_len, fin);
    return rc;
}

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
            if (is_bidi_id(o->stream_id)) {
                bool control = false;
                if (!uni_control) {
                    uint64_t *sid = from_client ? &p->client_control_sid : &p->server_control_sid;
                    if (*sid == UINT64_MAX) *sid = o->stream_id;
                    control = o->stream_id == p->client_control_sid || o->stream_id == p->server_control_sid;
                }
                rc = control ? moq_transport_bridge_on_peer_control_bytes(to, o->stream_id, o->data, o->data_len,
                                                                          o->fin, p->now)
                             : moq_transport_bridge_on_peer_bidi_bytes(to, o->stream_id, o->data, o->data_len,
                                                                       o->fin, p->now);
            } else if (!from_client) {
                /* publisher -> fetcher uni data: the adapter path under test.
                 * A stream with a held chunk gets nothing newer delivered; the
                 * transport keeps those bytes behind the held chunk, which
                 * the harness models by appending them to it. */
                held_t *h = held_find(p, o->stream_id);
                if (h) {
                    MOQ_TEST_CHECK(h->len + o->data_len <= sizeof(h->data));
                    if (h->len + o->data_len <= sizeof(h->data)) {
                        memcpy(h->data + h->len, o->data, o->data_len);
                        h->len += o->data_len;
                        if (o->fin) h->fin = true;
                    }
                    any = true;
                    break;
                }
                rc = feed_uni(p, o->stream_id, o->data, o->data_len, o->fin);
            } else {
                rc = test_hold_uni(&p->server_input, to, o->stream_id, o->data, o->data_len, o->fin, p->now);
            }
            break;
        case FAKE_OP_RESET:
            rc = moq_transport_bridge_on_peer_stream_reset(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_STOP:
            rc = moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
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
        if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_CLOSED && rc != MOQ_ERR_INPUT_NOT_CONSUMED) {
            p->deliver_error = true;
            p->unexpected_rc = (int)rc;
        }
        any = true;
    }
    fake_endpoint_clear_ops(from);
    return any;
}

static void
service_client(pair_t *p)
{
    moq_transport_bridge_service(p->cb, p->now);
    for (size_t i = 0; i < p->cep.count; i++) {
        if (is_bidi_id(p->cep.ops[i].stream_id)) continue;
        if (p->cep.ops[i].kind == FAKE_OP_STOP) p->client_stops++;
        if (p->cep.ops[i].kind == FAKE_OP_RESET) p->client_resets++;
    }
    MOQ_TEST_CHECK(!p->cep.overflowed && !p->cep.truncated);
    if (p->hold_client) fake_endpoint_clear_ops(&p->cep);
    else (void)deliver(p, &p->cep, p->sb, true);
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

static bool
pair_open_bounded(pair_t *p, moq_version_t version, uint32_t max_data_streams, bool hold_cap,
                  uint32_t max_events, uint32_t max_bridge_streams)
{
    memset(p, 0, sizeof(*p));
    p->alloc = test_allocator(&p->alloc_state);
    p->now = 1000;
    p->client_control_sid = UINT64_MAX;
    p->server_control_sid = UINT64_MAX;
    moq_session_cfg_t ccfg, scfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), &p->alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = version;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 64;
    ccfg.max_data_streams = max_data_streams;
    ccfg.max_fetches = 32;
    ccfg.max_events = max_events ? max_events : 64;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.version = version;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;
    scfg.max_fetches = 32;
    scfg.max_events = 64;
    if (moq_session_create(&ccfg, p->now, &p->c) < 0) return false;
    if (moq_session_create(&scfg, p->now, &p->sv) < 0) return false;
    if (test_bind_if_required(p->c) < 0 || test_bind_if_required(p->sv) < 0) return false;
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    if (hold_cap) p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    if (hold_cap && g_hold_bidi)
        p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, &p->alloc);
    if (max_bridge_streams) bcfg.max_streams = max_bridge_streams;
    if (moq_transport_bridge_create(&bcfg, p->c, &p->cep.vtable, &p->cep, &p->cb) < 0) return false;
    moq_transport_bridge_cfg_t sbcfg;
    moq_transport_bridge_cfg_init(&sbcfg, moq_alloc_default());
    if (moq_transport_bridge_create(&sbcfg, p->sv, &p->sep.vtable, &p->sep, &p->sb) < 0) return false;
    if (moq_session_start(p->c, p->now) < 0) return false;
    if (version != MOQ_VERSION_DRAFT_16 && moq_session_start(p->sv, p->now) < 0) return false;
    pump(p, 6);
    moq_event_t ev;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED && moq_session_state(p->sv) == MOQ_SESS_ESTABLISHED &&
           !moq_transport_bridge_is_fatal(p->cb) && !moq_transport_bridge_is_fatal(p->sb);
}

static bool
pair_open(pair_t *p, moq_version_t version, uint32_t max_data_streams, bool hold_cap, uint32_t max_events)
{
    return pair_open_bounded(p, version, max_data_streams, hold_cap, max_events, 0);
}

static void
pair_close(pair_t *p)
{
    moq_transport_bridge_destroy(p->cb);
    moq_transport_bridge_destroy(p->sb);
    moq_session_destroy(p->c);
    moq_session_destroy(p->sv);
    MOQ_TEST_CHECK_EQ_INT(p->alloc_state.balance, 0);
}

/* -- fetch helpers ------------------------------------------------------- */

typedef struct obs {
    int ok, object, complete, error, reset, cancelled, closed, other;
    uint64_t close_code;
} obs_t;

static void
drain(moq_session_t *s, obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_OK:        o->ok++; break;
        case MOQ_EVENT_FETCH_OBJECT:    o->object++; break;
        case MOQ_EVENT_FETCH_COMPLETE:  o->complete++; break;
        case MOQ_EVENT_FETCH_ERROR:     o->error++; break;
        case MOQ_EVENT_FETCH_RESET:     o->reset++; break;
        case MOQ_EVENT_FETCH_CANCELLED: o->cancelled++; break;
        case MOQ_EVENT_SESSION_CLOSED:  o->closed++; o->close_code = ev.u.closed.code; break;
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

/* Issue N fetches, collect the publisher's handles. */
static bool
fetch_all(pair_t *p, int n, moq_fetch_t *fh, moq_fetch_t *sfh)
{
    for (int k = 0; k < n; k++) MOQ_TEST_CHECK(issue_fetch(p->c, p->now, &fh[k]) == MOQ_OK);
    pump(p, 6);
    moq_event_t ev;
    int got = 0;
    while (moq_session_poll_events(p->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST && got < n) sfh[got++] = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);
    }
    return got == n;
}

static bool
accept_and_write(pair_t *p, moq_fetch_t sfh, uint64_t oid)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 3;
    if (moq_session_accept_fetch(p->sv, sfh, &ac, p->now) != MOQ_OK) return false;
    return write_obj(p->sv, sfh, oid, p->now) == MOQ_OK;
}

/* Fill the fetcher's 2-entry pool with two live, open responses. */
static void
fill_pool(pair_t *p, moq_fetch_t *sfh, obs_t *co)
{
    for (int k = 0; k < 2; k++) {
        MOQ_TEST_CHECK(accept_and_write(p, sfh[k], 0));
        pump(p, 4);
    }
    memset(co, 0, sizeof(*co));
    drain(p->c, co);
    MOQ_TEST_CHECK(co->ok == 2 && co->object == 2 && co->complete == 0 && co->closed == 0);
}

/* The publisher answers request k: deliver through the adapter path; return
 * the uni stream id. */
static uint64_t
answer(pair_t *p, moq_fetch_t sfh, uint64_t oid)
{
    MOQ_TEST_CHECK(accept_and_write(p, sfh, oid));
    uint64_t sid = p->sep.next_uni_id;
    p->now += 10;
    service_server(p);
    service_client(p);
    return sid;
}

static bool
owned(pair_t *p, uint64_t sid)
{
    moq_stream_ref_t ref = moq_transport_bridge_find_ref(p->cb, sid);
    return ref._v != 0 && moq_session_has_transport_stream(p->c, ref);
}

/* -- rows ----------------------------------------------------------------- */

/* Core row, both entry points, both drafts: refusal is whole and lossless,
 * repeated refusal is lossless, capacity return clears pending, redelivery
 * admits and completes; unrelated streams and control progress meanwhile. */
static int
row_admission(moq_version_t version, bool use_rcbuf)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, 2, true, 0));
    p.use_rcbuf = use_rcbuf;
    moq_fetch_t fh[6], sfh[6];
    MOQ_TEST_CHECK(fetch_all(&p, 4, fh, sfh));
    obs_t co;
    fill_pool(&p, sfh, &co);
    MOQ_TEST_CHECK(!moq_session_can_admit_data_stream(p.c));

    /* Request 3's response: refused whole. */
    uint64_t s3 = answer(&p, sfh[2], 0);
    held_t *h3 = held_find(&p, s3);
    MOQ_TEST_CHECK(h3 != NULL && h3->refusals == 1 && h3->len > 0);
    if (!h3) { printf("  %s: response not held -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(!owned(&p, s3));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3));
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p.cb));
    MOQ_TEST_CHECK(p.client_stops == 0 && p.client_resets == 0);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.ok == 1 && co.object == 0 && co.complete == 0 && co.error == 0 && co.reset == 0 &&
                   co.closed == 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));

    /* Repeated refusal before any capacity returns: still pending, still
     * held, nothing consumed. */
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(h3->active && h3->refusals == 2 && !owned(&p, s3));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3));
    /* A FIN-only notification while pending is refused too: the FIN belongs
     * with the held chunk, it cannot end the stream on its own. */
    p.now += 10;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, s3, NULL, 0, true, p.now) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v != 0);

    /* Unrelated progress: the live streams keep delivering and control keeps
     * flowing (a further request round-trips). */
    MOQ_TEST_CHECK(write_obj(p.sv, sfh[0], 1, p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.object == 1 && co.closed == 0);
    MOQ_TEST_CHECK(issue_fetch(p.c, p.now, &fh[4]) == MOQ_OK);
    pump(&p, 4);
    {
        moq_event_t ev;
        int got = 0;
        while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh[4] = ev.u.fetch_request.fetch; got++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got == 1);
    }
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3));   /* still waiting */

    /* Capacity returns: the publisher ends live stream 1; the next service
     * pass clears the pending mark (nothing is empty-fed to the session). */
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    MOQ_TEST_CHECK(moq_session_can_admit_data_stream(p.c));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3));
    MOQ_TEST_CHECK(h3->active && !owned(&p, s3));   /* the adapter still holds the chunk */

    /* Redelivery: admitted, parsed, completes when ended. */
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_OK);
    MOQ_TEST_CHECK(!h3->active && owned(&p, s3));
    service_client(&p);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.object == 1 && co.error == 0 && co.reset == 0);
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[2], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1 && co.closed == 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(p.c, fh[2]) < 0);
    MOQ_TEST_CHECK(p.client_stops == 0 && p.client_resets == 0 && !p.deliver_error);
    MOQ_TEST_CHECK(held_count(&p) == 0);
    printf("admission %s %s: refusals=%d stops=%d held_left=%d\n", lbl, use_rcbuf ? "rcbuf" : "bytes", h3->refusals,
           p.client_stops, held_count(&p));
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input admission %s %s\n", lbl, use_rcbuf ? "rcbuf" : "bytes");
    return failures - before;
}

/* Capacity taken by another stream before the redelivery: two held streams,
 * one entry frees, both pending marks clear; the first redelivered wins, the
 * second is refused again losslessly and admitted after the next free. */
static int
row_capacity_race(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, 2, true, 0));
    moq_fetch_t fh[6], sfh[6];
    MOQ_TEST_CHECK(fetch_all(&p, 4, fh, sfh));
    obs_t co;
    fill_pool(&p, sfh, &co);
    uint64_t s3 = answer(&p, sfh[2], 0);
    uint64_t s4 = answer(&p, sfh[3], 0);
    MOQ_TEST_CHECK(held_find(&p, s3) && held_find(&p, s4));
    if (!held_find(&p, s3) || !held_find(&p, s4)) { printf("  %s: responses not held -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3) && moq_transport_bridge_stream_has_pending(p.cb, s4));
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3) && !moq_transport_bridge_stream_has_pending(p.cb, s4));
    /* s4 redelivered first takes the entry. */
    MOQ_TEST_CHECK(redeliver(&p, s4) == MOQ_OK && owned(&p, s4));
    /* s3 loses the race: refused again, pending again, chunk still held. */
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(held_find(&p, s3) != NULL);
    if (!held_find(&p, s3)) { printf("  %s: s3 not held after re-refusal -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(held_find(&p, s3)->refusals == 2);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3) && !owned(&p, s3));
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    /* Next free entry: s3 admitted. */
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[3], p.now) == MOQ_OK);
    pump(&p, 3);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3));
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_OK && owned(&p, s3));
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[2], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.object == 2 && co.complete == 2 && co.error == 0 && co.reset == 0 && co.closed == 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(p.c, fh[2]) < 0 && fetch_resolve_handle(p.c, fh[3]) < 0);
    MOQ_TEST_CHECK(p.client_stops == 0 && held_count(&p) == 0 && !p.deliver_error);
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input capacity race %s\n", lbl);
    return failures - before;
}

/* FIN with payload refused: the FIN is held with the chunk; after admission
 * the stream ends and the request completes. RESET while pending retires the
 * stream (no fatal, no event for it, the adapter drops its chunk). Teardown
 * while pending leaks nothing. */
static int
row_fin_reset_teardown(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, 2, true, 0));
    moq_fetch_t fh[6], sfh[6];
    MOQ_TEST_CHECK(fetch_all(&p, 6, fh, sfh));
    obs_t co;
    fill_pool(&p, sfh, &co);
    /* Request 3: header + object + END in one pass -> the held chunk carries
     * (or is followed by) the FIN; everything of that stream stays unconsumed. */
    MOQ_TEST_CHECK(accept_and_write(&p, sfh[2], 0));
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[2], p.now) == MOQ_OK);
    uint64_t s3 = p.sep.next_uni_id;
    p.now += 10;
    service_server(&p);
    service_client(&p);
    held_t *h3 = held_find(&p, s3);
    MOQ_TEST_CHECK(h3 != NULL && !owned(&p, s3));
    if (!h3) { printf("  %s: response not held -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3));
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.ok == 1 && co.object == 0 && co.complete == 0);
    /* Request 4: refused, then RESET by the publisher while pending. */
    uint64_t s4 = answer(&p, sfh[3], 0);
    MOQ_TEST_CHECK(held_find(&p, s4) && !owned(&p, s4));
    if (!held_find(&p, s4)) { printf("  %s: s4 not held -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    p.now += 10;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, s4, 0x7, p.now) == MOQ_OK);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s4));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s4)._v == 0);   /* retired */
    held_find(&p, s4)->active = false;                                 /* the adapter drops it */
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb) && moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.reset == 0 && co.error == 0 && co.closed == 0);   /* never attributed: nothing reported */
    /* Capacity returns: stream 3 redelivered with its FIN completes. */
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3));
    bool fin_in_chunk = h3->fin;
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_OK);
    if (!fin_in_chunk) {
        /* the END rode a separate write: the harness kept delivery order by
         * withholding it; deliver it now */
        p.now += 10;
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, s3, NULL, 0, true, p.now) == MOQ_OK);
    }
    service_client(&p);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    printf("fin/reset %s: fin_in_chunk=%d objects=%d complete=%d\n", lbl, (int)fin_in_chunk, co.object, co.complete);
    MOQ_TEST_CHECK(co.object == 1 && co.complete == 1 && co.error == 0 && co.reset == 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(p.c, fh[2]) < 0);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v == 0);   /* ended */
    /* Refill the pool (request 5 takes the freed entry), then request 6's
     * response is pending admission at teardown. */
    uint64_t s5 = answer(&p, sfh[4], 0);
    MOQ_TEST_CHECK(owned(&p, s5) && held_find(&p, s5) == NULL);
    uint64_t s6 = answer(&p, sfh[5], 0);
    MOQ_TEST_CHECK(held_find(&p, s6) && moq_transport_bridge_stream_has_pending(p.cb, s6));
    MOQ_TEST_CHECK(p.client_stops == 0 && !p.deliver_error);
    pair_close(&p);   /* balance 0 with a pending-admission entry outstanding */
    if (failures == before) printf("PASS: hold_input fin/reset/teardown %s\n", lbl);
    return failures - before;
}

/* Owned-stream pressure keeps the retained-input contract: with a 1-deep
 * event queue the live stream's object blocks -> WOULD_BLOCK, retained, the
 * stream stays owned and pending_retry drains it; never NOT_CONSUMED. */
static int
row_retained_pressure(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, 4, true, 1));
    moq_fetch_t fh[4] = {{0}}, sfh[4] = {{0}};
    /* one request at a time: the tiny event queue holds one event */
    MOQ_TEST_CHECK(issue_fetch(p.c, p.now, &fh[0]) == MOQ_OK);
    pump(&p, 6);
    {
        moq_event_t ev;
        int got = 0;
        while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh[0] = ev.u.fetch_request.fetch; got++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got == 1);
    }
    MOQ_TEST_CHECK(accept_and_write(&p, sfh[0], 0));
    MOQ_TEST_CHECK(write_obj(p.sv, sfh[0], 1, p.now) == MOQ_OK);
    uint64_t s1 = p.sep.next_uni_id;
    p.now += 10;
    service_server(&p);   /* FETCH_OK fills the 1-deep queue; the data stream then blocks owned */
    service_client(&p);
    MOQ_TEST_CHECK(held_find(&p, s1) == NULL);                          /* never NOT_CONSUMED */
    MOQ_TEST_CHECK(owned(&p, s1));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s1));  /* retained input */
    obs_t co = {0};
    int objects = 0, rounds = 0;
    while (rounds++ < 12) {
        memset(&co, 0, sizeof(co));
        drain(p.c, &co);
        objects += co.object;
        p.now += 10;
        service_client(&p);
    }
    MOQ_TEST_CHECK(objects == 2);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s1));
    MOQ_TEST_CHECK(p.client_stops == 0 && !p.deliver_error && !moq_transport_bridge_is_fatal(p.cb));
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input retained pressure %s\n", lbl);
    return failures - before;
}

/* Drafts 18 and 22: classification prefix across calls: the response's stream type is
 * re-encoded as a three-byte vi64 and delivered one byte first (the bridge
 * retains it: NEED_MORE), then the rest while the pool is full. The refusal
 * must leave that prefix bridge-owned; the redelivered chunk re-enters
 * classification with it and the session parses the stream correctly. */
static int
row_prefix_fragment(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, 2, true, 0));
    p.hold_client = false;
    moq_fetch_t fh[6], sfh[6];
    MOQ_TEST_CHECK(fetch_all(&p, 3, fh, sfh));
    obs_t co;
    fill_pool(&p, sfh, &co);
    /* capture request 3's response bytes without delivering them */
    MOQ_TEST_CHECK(accept_and_write(&p, sfh[2], 0));
    uint64_t s3 = p.sep.next_uni_id;
    p.now += 10;
    moq_transport_bridge_service(p.sb, p.now);
    uint8_t raw[512];
    size_t raw_len = 0;
    bool raw_fin = false;
    for (size_t j = 0; j < p.sep.count; j++) {
        fake_op_t *o = &p.sep.ops[j];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == s3) {
            memcpy(raw + raw_len, o->data, o->data_len);
            raw_len += o->data_len;
            raw_fin = o->fin;
            o->kind = FAKE_OP_OPEN_UNI;   /* withheld from the ordinary delivery below */
        }
    }
    (void)deliver(&p, &p.sep, p.cb, false);   /* the request's FETCH_OK still flows */
    service_client(&p);
    MOQ_TEST_CHECK(raw_len > 1 && raw[0] == 0x05);
    /* Three-byte vi64 for type 0x05: 0xc0 0x00 0x05. Losing 0xc0 makes
     * the remainder start with padding, so this detects prefix loss rather
     * than accepting a shorter, equivalent encoding of the same type. */
    uint8_t rest[512];
    rest[0] = 0x00;
    rest[1] = 0x05;
    memcpy(rest + 2, raw + 1, raw_len - 1);
    size_t rest_len = raw_len + 1;
    uint8_t first = 0xc0;
    p.now += 10;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, s3, &first, 1, false, p.now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v != 0);   /* mapped, prefix retained */
    MOQ_TEST_CHECK(!owned(&p, s3));                                     /* nothing fed yet */
    /* The rest while the pool is full: refused whole; the prefix survives. */
    MOQ_TEST_CHECK(feed_uni(&p, s3, rest, rest_len, raw_fin) == MOQ_ERR_INPUT_NOT_CONSUMED);
    if (!held_find(&p, s3)) { printf("  prefix: rest not held -- unwinding\n"); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3) && !owned(&p, s3));
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_ERR_INPUT_NOT_CONSUMED);   /* repeated: still intact */
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3));
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_OK && owned(&p, s3));
    service_client(&p);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.object == 1 && co.closed == 0);   /* parsed with the prefix: a correct header */
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[2], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1 && co.closed == 0 && co.error == 0 && co.reset == 0);
    MOQ_TEST_CHECK(p.client_stops == 0 && held_count(&p) == 0 && !p.deliver_error);
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input prefix fragment %s\n", lbl);
    return failures - before;
}

/* FIN between readiness and redelivery. A refused response (bytes held);
 * capacity returns and a service pass clears the readiness mark; a FIN-only
 * notification then arrives BEFORE the adapter redelivers its held chunk.
 * It must be refused (NOT_CONSUMED, no fatal, mapping kept): the FIN cannot
 * overtake the held bytes nor trigger classification of an empty stand-in.
 * The redelivery then admits the chunk, and an ordinary FIN ends the stream.
 * Also: capacity available before any service pass (the redelivery itself is
 * admitted), RESET before and after readiness (retired, nothing reported). */
static int
row_fin_before_redelivery(moq_version_t version, bool use_rcbuf)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, 2, true, 0));
    p.use_rcbuf = use_rcbuf;
    moq_fetch_t fh[8], sfh[8];
    MOQ_TEST_CHECK(fetch_all(&p, 6, fh, sfh));
    obs_t co;
    fill_pool(&p, sfh, &co);

    /* s3: refused; ready after the live stream 1 ends; FIN-only overtakes. */
    uint64_t s3 = answer(&p, sfh[2], 0);
    held_t *h3 = held_find(&p, s3);
    MOQ_TEST_CHECK(h3 != NULL && h3->len > 0);
    if (!h3) { printf("  %s: s3 not held -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3) && h3->active);
    p.now += 10;
    moq_result_t frc = moq_transport_bridge_on_peer_uni_bytes(p.cb, s3, NULL, 0, true, p.now);
    printf("%s fin-before-redelivery %s: rc=%d fatal=%d held=%d\n", lbl, use_rcbuf ? "rcbuf" : "bytes", (int)frc,
           (int)moq_transport_bridge_is_fatal(p.cb), (int)h3->active);
    MOQ_TEST_CHECK(frc == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb) && moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v != 0 && !owned(&p, s3));
    MOQ_TEST_CHECK(h3->active && !h3->fin);
    /* Redelivery of the held bytes: admitted; then the FIN ends the stream. */
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_OK && owned(&p, s3));
    service_client(&p);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.object == 1);
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[2], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1 && co.error == 0 && co.reset == 0 && co.closed == 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(p.c, fh[2]) < 0);

    /* Refill the entry s3 released (request 5's response is admitted), then
     * s4: refused while full; capacity returns (live stream 2 ends) but the
     * adapter redelivers before any service pass: admitted on the spot. */
    uint64_t s5 = answer(&p, sfh[4], 0);
    MOQ_TEST_CHECK(owned(&p, s5) && held_find(&p, s5) == NULL);
    uint64_t s4 = answer(&p, sfh[3], 0);
    MOQ_TEST_CHECK(held_find(&p, s4) != NULL);
    if (!held_find(&p, s4)) { printf("  %s: s4 not held -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[1], p.now) == MOQ_OK);
    p.now += 10;
    moq_transport_bridge_service(p.sb, p.now);
    (void)deliver(&p, &p.sep, p.cb, false);   /* the FIN reaches the fetcher; no client service pass */
    MOQ_TEST_CHECK(moq_session_can_admit_data_stream(p.c));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s4));   /* readiness not yet observed */
    MOQ_TEST_CHECK(redeliver(&p, s4) == MOQ_OK && owned(&p, s4));
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s4));
    service_client(&p);
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[3], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    printf("%s capacity-before-service: ok=%d object=%d complete=%d error=%d reset=%d\n", lbl, co.ok, co.object,
           co.complete, co.error, co.reset);
    /* objects: the refill response (s5) and the redelivered s4; completes: live stream 2 and s4 */
    MOQ_TEST_CHECK(co.object == 2 && co.complete == 2 && co.error == 0 && co.reset == 0);

    /* Refill the entry s4 released; the pool is {s5, s6} again. */
    uint64_t s6 = answer(&p, sfh[5], 0);
    MOQ_TEST_CHECK(owned(&p, s6) && held_find(&p, s6) == NULL);
    /* Now the pool is full again: a seventh response is refused; RESET before
     * readiness retires it. */
    moq_fetch_t fh7, sfh7;
    MOQ_TEST_CHECK(issue_fetch(p.c, p.now, &fh7) == MOQ_OK);
    pump(&p, 4);
    {
        moq_event_t ev;
        int got = 0;
        while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh7 = ev.u.fetch_request.fetch; got++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got == 1);
        if (got != 1) { pair_close(&p); return failures - before; }
    }
    uint64_t s7 = answer(&p, sfh7, 0);
    MOQ_TEST_CHECK(held_find(&p, s7) && moq_transport_bridge_stream_has_pending(p.cb, s7));
    if (!held_find(&p, s7)) { printf("  %s: s7 not held -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    p.now += 10;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, s7, 0x9, p.now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s7)._v == 0 && !moq_transport_bridge_is_fatal(p.cb));
    held_find(&p, s7)->active = false;
    /* RESET after readiness: an eighth response refused, capacity returns
     * and is observed, then the publisher resets it before redelivery. */
    moq_fetch_t fh8, sfh8;
    MOQ_TEST_CHECK(issue_fetch(p.c, p.now, &fh8) == MOQ_OK);
    pump(&p, 4);
    {
        moq_event_t ev;
        int got = 0;
        while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_FETCH_REQUEST) { sfh8 = ev.u.fetch_request.fetch; got++; }
            moq_event_cleanup(&ev);
        }
        MOQ_TEST_CHECK(got == 1);
        if (got != 1) { pair_close(&p); return failures - before; }
    }
    uint64_t s8 = answer(&p, sfh8, 0);
    MOQ_TEST_CHECK(held_find(&p, s8) != NULL);
    if (!held_find(&p, s8)) { printf("  %s: s8 not held -- unwinding\n", lbl); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[4], p.now) == MOQ_OK);
    pump(&p, 3);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s8));   /* ready */
    p.now += 10;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, s8, 0x9, p.now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s8)._v == 0 && !moq_transport_bridge_is_fatal(p.cb));
    held_find(&p, s8)->active = false;
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.reset == 0 && co.error == 0 && co.closed == 0 && moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(p.client_stops == 0 && held_count(&p) == 0 && !p.deliver_error);
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input fin-before-redelivery %s %s\n", lbl, use_rcbuf ? "rcbuf" : "bytes");
    return failures - before;
}

/* A refused chunk that itself carried zero bytes and the FIN is a legitimate
 * retry when redelivered as FIN-only: it must not be refused forever.
 * draft-16: a stream whose only delivery is FIN-only (headerless graceful
 * FIN) arrives while the pool is full. Drafts 18 and 22: the stream's type byte was
 * accepted on an earlier call (retained prefix), and the zero-byte FIN that
 * followed was refused; the retained prefix is preserved and replayed. */
static int
row_zero_byte_fin_retry(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, 2, true, 0));
    moq_fetch_t fh[8], sfh[8];
    MOQ_TEST_CHECK(fetch_all(&p, 3, fh, sfh));
    obs_t co;
    fill_pool(&p, sfh, &co);
    const uint64_t sid = 3777;
    p.now += 10;
    if (version != MOQ_VERSION_DRAFT_16) {
        /* type byte of a FETCH_HEADER as a two-byte vi64 first byte: retained */
        uint8_t first = 0x80;
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, sid, &first, 1, false, p.now) == MOQ_OK);
        uint8_t second = 0x05;
        /* the rest of the type completes classification: DATA, refused */
        MOQ_TEST_CHECK(feed_uni(&p, sid, &second, 1, false) == MOQ_ERR_INPUT_NOT_CONSUMED);
        MOQ_TEST_CHECK(held_find(&p, sid) != NULL);
        if (!held_find(&p, sid)) { pair_close(&p); return failures - before; }
        /* FIN-only cannot overtake the held byte */
        p.now += 10;
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, sid, NULL, 0, true, p.now) == MOQ_ERR_INPUT_NOT_CONSUMED);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
        /* capacity returns; the held byte is redelivered (ownership passes to
         * the session: a FETCH_HEADER with no request id yet), then the FIN:
         * a truncated header at FIN is the session's existing protocol
         * close -- stated, not hidden; the point here is that neither the
         * prefix nor the held byte was lost or reordered. */
        MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
        pump(&p, 3);
        MOQ_TEST_CHECK(redeliver(&p, sid) == MOQ_OK && owned(&p, sid));
        MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    } else {
        /* a FIN-only new stream while full: refused as a zero-byte chunk */
        MOQ_TEST_CHECK(feed_uni(&p, sid, NULL, 0, true) == MOQ_ERR_INPUT_NOT_CONSUMED);
        held_t *h = held_find(&p, sid);
        MOQ_TEST_CHECK(h != NULL && h->len == 0 && h->fin);
        if (!h) { pair_close(&p); return failures - before; }
        MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, sid));
        /* repeated FIN-only retries while still full: refused, never fatal */
        MOQ_TEST_CHECK(redeliver(&p, sid) == MOQ_ERR_INPUT_NOT_CONSUMED);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
        /* capacity returns: the FIN-only retry is accepted (headerless
         * graceful FIN), the stream is gone, the session is unaffected */
        MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
        pump(&p, 3);
        MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, sid));
        MOQ_TEST_CHECK(redeliver(&p, sid) == MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, sid)._v == 0);
        MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    }
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.closed == 0 && p.client_stops == 0 && held_count(&p) == 0 && !p.deliver_error);
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input zero-byte fin retry %s\n", lbl);
    return failures - before;
}

/* The accepted contract cannot be withdrawn by mutating the borrowed vtable. */
static int
row_lifetime_contract(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    pair_t p;
    MOQ_TEST_CHECK(pair_open(&p, version, 2, true, 0));
    p.cep.vtable.capabilities &= ~(uint32_t)MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_fetch_t fh[6], sfh[6];
    MOQ_TEST_CHECK(fetch_all(&p, 3, fh, sfh));
    obs_t co;
    fill_pool(&p, sfh, &co);
    MOQ_TEST_CHECK(accept_and_write(&p, sfh[2], 0));
    uint64_t s3 = p.sep.next_uni_id;
    p.now += 10;
    service_server(&p);
    service_client(&p);
    MOQ_TEST_CHECK(held_count(&p) == 1);
    held_t *held = held_find(&p, s3);
    MOQ_TEST_CHECK(held && held->refusals > 0);
    MOQ_TEST_CHECK(!owned(&p, s3) && p.client_stops == 0);
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    if (held) MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[2], p.now) == MOQ_OK);
    pump(&p, 3);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.object == 1 && co.complete == 1 && co.reset == 0 && co.error == 0);
    MOQ_TEST_CHECK(held_count(&p) == 0 && p.client_stops == 0 && !p.deliver_error);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input lifetime contract %s\n", lbl);
    return failures - before;
}

/* A refused FIN is still an input obligation, not a reusable mapping. Bound
 * held identities by the configured bridge table, independently of transport
 * stream-credit replenishment. The identical input bytes remain owned here
 * until RESET or teardown; none of these synthetic streams is admitted. */
static void
row_mapping_bound(moq_version_t version)
{
    enum { LIMIT = 16 };
    int before = failures;
    pair_t p;
    if (!pair_open_bounded(&p, version, 2, true, 0, LIMIT)) {
        MOQ_TEST_CHECK(false);
        pair_close(&p);
        return;
    }
    moq_fetch_t fh[3], sfh[3];
    if (!fetch_all(&p, 3, fh, sfh)) {
        MOQ_TEST_CHECK(false);
        pair_close(&p);
        return;
    }
    obs_t co;
    fill_pool(&p, sfh, &co);
    uint64_t ids[LIMIT];
    moq_stream_ref_t refs[LIMIT];
    ids[0] = answer(&p, sfh[2], 0);
    held_t *h = held_find(&p, ids[0]);
    if (!h) {
        MOQ_TEST_CHECK(h != NULL);
        pair_close(&p);
        return;
    }
    refs[0] = moq_transport_bridge_find_ref(p.cb, ids[0]);
    MOQ_TEST_CHECK(refs[0]._v != 0);
    uint64_t next_id = 10000;
    for (int n = 0; n < LIMIT * 3; n++, next_id++) {
        moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
            p.cb, next_id, h->data, h->len, true, ++p.now);
        MOQ_TEST_CHECK(rc == MOQ_ERR_INPUT_NOT_CONSUMED);
        if (rc != MOQ_ERR_INPUT_NOT_CONSUMED) goto cleanup;
        MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, next_id)._v != 0);
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, next_id, 7, ++p.now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, next_id)._v == 0);
        MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, ids[0])._v == refs[0]._v);
    }

    size_t count = 1;
    bool exhausted = false;
    for (int n = 0; n < LIMIT; n++, next_id++) {
        moq_result_t rc = moq_transport_bridge_on_peer_uni_bytes(
            p.cb, next_id, h->data, h->len, true, ++p.now);
        if (rc != MOQ_ERR_INPUT_NOT_CONSUMED) {
            MOQ_TEST_CHECK(rc == MOQ_ERR_INTERNAL);
            MOQ_TEST_CHECK(moq_transport_bridge_is_fatal(p.cb));
            MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, next_id)._v == 0);
            exhausted = true;
            break;
        }
        MOQ_TEST_CHECK(count < LIMIT);
        if (count == LIMIT) break;
        ids[count] = next_id;
        refs[count] = moq_transport_bridge_find_ref(p.cb, next_id);
        MOQ_TEST_CHECK(refs[count]._v != 0);
        count++;
        service_client(&p);
        MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
        for (size_t i = 0; i < count; i++) {
            MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, ids[i])._v == refs[i]._v);
            MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, ids[i]));
            MOQ_TEST_CHECK(!owned(&p, ids[i]));
        }
    }
    MOQ_TEST_CHECK(exhausted && count > 1 && count < LIMIT);
    /* Two admitted data streams; a uni-pair profile (drafts 18 and 22)
     * additionally retains its two control streams and three outstanding
     * request streams. No other maps exist. */
    MOQ_TEST_CHECK(count == LIMIT - (version == MOQ_VERSION_DRAFT_16 ? 2u : 7u));
    printf("mapping bound d%d: limit=%d held=%zu reset_cycles=%d exhausted=%d\n",
           version == MOQ_VERSION_DRAFT_16 ? 16 : version == MOQ_VERSION_DRAFT_18 ? 18 : 22, LIMIT, count, LIMIT * 3, (int)exhausted);
cleanup:
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input mapping bound\n");
}

static void
drain_replay_pressure(pair_t *p, obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(p->c, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_OK: o->ok++; break;
        case MOQ_EVENT_FETCH_OBJECT: {
            uint8_t expected[16];
            memset(expected, 0xd0 + o->object, sizeof(expected));
            MOQ_TEST_CHECK(ev.u.fetch_object.object_id == (uint64_t)o->object);
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.fetch_object.payload) == sizeof(expected));
            if (moq_rcbuf_len(ev.u.fetch_object.payload) == sizeof(expected))
                MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.fetch_object.payload), expected, sizeof(expected)) == 0);
            o->object++;
            break;
        }
        case MOQ_EVENT_FETCH_COMPLETE: o->complete++; break;
        case MOQ_EVENT_FETCH_ERROR: o->error++; break;
        case MOQ_EVENT_FETCH_RESET: o->reset++; break;
        case MOQ_EVENT_SESSION_CLOSED: o->closed++; break;
        default: MOQ_TEST_CHECK(false); break;
        }
        moq_event_cleanup(&ev);
    }
}

/* Admission refusal followed by accepted, owned event pressure. Keep the
 * original coalesced call distinct from any later publisher tail and FIN. */
static void
row_replay_owned_pressure(moq_version_t version, bool use_rcbuf, bool queued_tail)
{
    int before = failures;
    pair_t p;
    if (!pair_open(&p, version, 1, true, 1)) {
        MOQ_TEST_CHECK(false);
        pair_close(&p);
        return;
    }
    p.use_rcbuf = use_rcbuf;
    moq_fetch_t fh[1], sfh[1];
    if (!fetch_all(&p, 1, fh, sfh)) {
        MOQ_TEST_CHECK(false);
        goto cleanup;
    }
    uint8_t type = 0x05;
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, 6000, &type, 1, false, p.now) == MOQ_OK);
    MOQ_TEST_CHECK(accept_and_write(&p, sfh[0], 0));
    uint64_t sid = p.sep.next_uni_id;
    MOQ_TEST_CHECK(moq_transport_bridge_service(p.sb, ++p.now) == MOQ_OK);
    uint8_t wire[512];
    size_t len = 0;
    for (size_t i = 0; i < p.sep.count; i++) {
        fake_op_t *op = &p.sep.ops[i];
        if (op->kind != FAKE_OP_WRITE || op->stream_id != sid) continue;
        if (len + op->data_len > sizeof(wire)) {
            MOQ_TEST_CHECK(false);
            goto cleanup;
        }
        memcpy(wire + len, op->data, op->data_len);
        len += op->data_len;
        MOQ_TEST_CHECK(!op->fin);
        op->kind = FAKE_OP_OPEN_UNI; /* Deliver control now, coalesced data below. */
    }
    MOQ_TEST_CHECK(len > 0);
    (void)deliver(&p, &p.sep, p.cb, false);
    MOQ_TEST_CHECK(feed_uni(&p, sid, wire, len, false) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(!owned(&p, sid) && held_count(&p) == 1);
    if (!held_find(&p, sid)) goto cleanup;
    if (queued_tail) {
        MOQ_TEST_CHECK(write_obj(p.sv, sfh[0], 1, ++p.now) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], ++p.now) == MOQ_OK);
        service_server(&p);
        held_t *h = held_find(&p, sid);
        MOQ_TEST_CHECK(h->len > h->first_len && h->fin && !h->first_fin);
    }
    MOQ_TEST_CHECK(redeliver(&p, sid) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(held_count(&p) == 1 && !owned(&p, sid));
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, 6000, 1, ++p.now) == MOQ_OK);
    MOQ_TEST_CHECK(redeliver(&p, sid) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(owned(&p, sid));
    MOQ_TEST_CHECK(held_count(&p) == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, sid));
    obs_t co = {0};
    for (int i = 0; i < 8; i++) {
        drain_replay_pressure(&p, &co);
        service_client(&p);
        /* A faulty driver retries its stale obligation once pending clears. */
        if (held_find(&p, sid) && !moq_transport_bridge_stream_has_pending(p.cb, sid))
            (void)redeliver(&p, sid);
    }
    if (!queued_tail)
        MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], ++p.now) == MOQ_OK);
    for (int i = 0; i < 8; i++) {
        pump(&p, 1);
        drain_replay_pressure(&p, &co);
    }
    MOQ_TEST_CHECK(co.ok == 1 && co.object == (queued_tail ? 2 : 1) && co.complete == 1);
    MOQ_TEST_CHECK(co.error == 0 && co.reset == 0 && co.closed == 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(p.c, fh[0]) < 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(held_count(&p) == 0 && !p.deliver_error && !moq_transport_bridge_is_fatal(p.cb));
    printf("owned replay d%d %s tail=%d: objects=%d complete=%d closed=%d held=%d\n",
           version == MOQ_VERSION_DRAFT_16 ? 16 : version == MOQ_VERSION_DRAFT_18 ? 18 : 22, use_rcbuf ? "rcbuf" : "bytes",
           (int)queued_tail, co.object, co.complete, co.closed, held_count(&p));
cleanup:
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input owned replay pressure\n");
}

/* -- bounded holding (MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT) ------------------ */

/* Service the fetcher's bridge and count, before delivering them, the
 * STOP_SENDING it emitted: on `sid` with `code`, and on any other stream. */
static void
service_client_stops(pair_t *p, uint64_t sid, uint64_t code, int *on_sid,
                     int *others)
{
    moq_transport_bridge_service(p->cb, p->now);
    for (size_t i = 0; i < p->cep.count; i++) {
        const fake_op_t *o = &p->cep.ops[i];
        if (o->kind != FAKE_OP_STOP || is_bidi_id(o->stream_id)) continue;
        if (o->stream_id == sid && o->error_code == code) (*on_sid)++;
        else (*others)++;
    }
    MOQ_TEST_CHECK(!p->cep.overflowed && !p->cep.truncated);
    (void)deliver(p, &p->cep, p->sb, true);
}

/* Fetcher events with the exact replay oracle: FETCH_OBJECT events for
 * `fetch` count as matching only with the expected Group/Subgroup/Object
 * identity, payload length and bytes; every FETCH_COMPLETE is recorded with
 * its fetch handle, in order. */
#define SEEN_MAX 8
typedef struct fetch_seen {
    int      objects, matching, mismatched;
    int      error, reset, closed, other;
    int      ncomplete;
    uint64_t complete[SEEN_MAX];
} fetch_seen_t;

static void
drain_fetch_exact(moq_session_t *s, moq_fetch_t fetch, uint64_t group, uint64_t object, const uint8_t *body,
                  size_t len, fetch_seen_t *o)
{
    memset(o, 0, sizeof(*o));
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_OBJECT: {
            const moq_fetch_object_event_t *fo = &ev.u.fetch_object;
            o->objects++;
            if (fo->fetch._opaque != fetch._opaque) break;
            size_t n = fo->payload ? moq_rcbuf_len(fo->payload) : 0;
            if (fo->group_id == group && fo->subgroup_id == 0 && fo->object_id == object && !fo->datagram &&
                n == len && (len == 0 || memcmp(moq_rcbuf_data(fo->payload), body, len) == 0))
                o->matching++;
            else
                o->mismatched++;
            break;
        }
        case MOQ_EVENT_FETCH_COMPLETE:
            if (o->ncomplete < SEEN_MAX) o->complete[o->ncomplete] = ev.u.fetch_complete.fetch._opaque;
            o->ncomplete++;
            break;
        case MOQ_EVENT_FETCH_ERROR:     o->error++; break;
        case MOQ_EVENT_FETCH_RESET:     o->reset++; break;
        case MOQ_EVENT_SESSION_CLOSED:  o->closed++; break;
        default:                        o->other++; break;
        }
        moq_event_cleanup(&ev);
    }
}

/* Completions of `fetch` among the drained events. */
static int
seen_completions(const fetch_seen_t *o, moq_fetch_t fetch)
{
    int n = 0;
    for (int i = 0; i < o->ncomplete && i < SEEN_MAX; i++)
        if (o->complete[i] == fetch._opaque) n++;
    return n;
}

/* The response a publisher wrote with write_obj(.., 0, ..): Group 0,
 * Object 0, sixteen bytes of 0xd0. A replayed response is judged against
 * this, never against what the harness held. */
static const uint8_t k_obj0_body[16] = {
    0xd0, 0xd0, 0xd0, 0xd0, 0xd0, 0xd0, 0xd0, 0xd0,
    0xd0, 0xd0, 0xd0, 0xd0, 0xd0, 0xd0, 0xd0, 0xd0,
};

/* Redeliver the held response `sid` of `fetch` and judge it with the exact
 * oracle: one matching object, nothing else, and no completion before the
 * publisher's FIN. Then the publisher ends the response: exactly one
 * completion, of `fetch`, after that FIN, and no further object. */
static void
replay_exact(pair_t *p, uint64_t sid, moq_fetch_t fetch, moq_fetch_t pub_fetch)
{
    fetch_seen_t seen;
    MOQ_TEST_CHECK(redeliver(p, sid) == MOQ_OK);
    MOQ_TEST_CHECK(owned(p, sid));
    drain_fetch_exact(p->c, fetch, 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.matching, 1);
    MOQ_TEST_CHECK_EQ_INT(seen.mismatched, 0);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 1);
    MOQ_TEST_CHECK_EQ_INT(seen.ncomplete, 0);        /* no completion before the FIN */
    MOQ_TEST_CHECK(seen.error == 0 && seen.reset == 0 && seen.closed == 0);

    MOQ_TEST_CHECK(moq_session_end_fetch(p->sv, pub_fetch, p->now) == MOQ_OK);
    pump(p, 3);
    drain_fetch_exact(p->c, fetch, 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 0);
    MOQ_TEST_CHECK_EQ_INT(seen.ncomplete, 1);
    MOQ_TEST_CHECK_EQ_INT(seen_completions(&seen, fetch), 1);
    MOQ_TEST_CHECK(seen.error == 0 && seen.reset == 0 && seen.closed == 0);
}

static uint64_t
hold_cancel_code(moq_version_t version)
{
    return version == MOQ_VERSION_DRAFT_16 ? MOQ_STREAM_ERROR_CANCELLED
                                           : MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
}

/* A response refused at admission (the receive pool is full) waits under a
 * hold record. Its age runs from the first refusal: a later re-refusal does
 * not restart it, and a FIN-only delivery cannot overtake the held bytes. At
 * the age the bridge cancels exactly that stream with STOP_SENDING carrying
 * the profile's code (draft-16 CANCELLED 0x1, drafts 18 and 22 EXCESSIVE_LOAD 0x9);
 * a blocked STOP stays owed -- the stream still reports pending and the
 * bridge reports outbound work -- and is retried until it is taken, once.
 * The other live responses and control keep working. */
static int
row_bounded_age_blocked_stop(moq_version_t version)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_16 ? "v16" : version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    g_hold_bidi = true;
    pair_t p;
    bool ok = pair_open(&p, version, 2, true, 0);
    g_hold_bidi = false;
    MOQ_TEST_CHECK(ok);
    if (!ok) { pair_close(&p); return failures - before; }
    moq_fetch_t fh[3], sfh[3];
    if (!fetch_all(&p, 3, fh, sfh)) { MOQ_TEST_CHECK(false); pair_close(&p); return failures - before; }
    obs_t co;
    fill_pool(&p, sfh, &co);
    uint64_t s3 = answer(&p, sfh[2], 0);
    uint64_t t3 = p.now;
    held_t *h3 = held_find(&p, s3);
    MOQ_TEST_CHECK(h3 != NULL);
    if (!h3) { pair_close(&p); return failures - before; }

    p.now += 1000000;
    MOQ_TEST_CHECK(redeliver(&p, s3) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, s3, NULL, 0, true, ++p.now) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(!owned(&p, s3));

    const uint64_t code = hold_cancel_code(version);
    int on_sid = 0, others = 0;
    p.now = t3 + HOLD_AGE_US;
    p.cep.block_stop = true;
    service_client_stops(&p, s3, code, &on_sid, &others);
    MOQ_TEST_CHECK(on_sid == 0 && others == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3));
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.cb));
    p.now += 10;
    service_client_stops(&p, s3, code, &on_sid, &others);
    MOQ_TEST_CHECK(on_sid == 0 && others == 0);
    p.cep.block_stop = false;
    p.now += 10;
    service_client_stops(&p, s3, code, &on_sid, &others);
    MOQ_TEST_CHECK_EQ_INT(on_sid, 1);
    MOQ_TEST_CHECK_EQ_INT(others, 0);
    h3->active = false;                     /* the adapter drops the chunk */
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3));
    p.now += 10;
    service_client_stops(&p, s3, code, &on_sid, &others);
    MOQ_TEST_CHECK_EQ_INT(on_sid, 1);       /* taken once */

    /* The other responses and control are unaffected. */
    MOQ_TEST_CHECK(write_obj(p.sv, sfh[0], 1, p.now) == MOQ_OK);
    pump(&p, 4);
    memset(&co, 0, sizeof(co));
    drain(p.c, &co);
    MOQ_TEST_CHECK(co.object >= 1 && co.closed == 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input bounded age and blocked stop %s\n", lbl);
    return failures - before;
}

/* Draft-16: a response that finds the stream map full (two live responses,
 * two refused ones) is held without a mapping. Borrowed rcbuf input is never
 * kept. A peer RESET settles a held stream. When a mapping frees, the
 * unmapped stream is offered; a FIN-only delivery still cannot overtake its
 * held bytes; redelivered, it maps but the receive pool is full, so it stays
 * held with its ORIGINAL age. When the receive pool frees, the older s4
 * (mapped since its first refusal) is offered first, replayed exactly, and
 * completes once at its FIN. Then either the transitioned stream s5
 * (unmapped, then mapped and held) is offered when s4's completion frees the
 * pool, replayed exactly and completes once at its FIN; or, never
 * redelivered, it is cancelled at its original age (CANCELLED 0x1), and
 * nothing else is. */
static int
row_bounded_unmapped_d16(bool use_rcbuf, bool replay_transitioned)
{
    const char *lbl = use_rcbuf ? "rcbuf" : "bytes";
    int before = failures;
    g_hold_bidi = true;
    pair_t p;
    bool ok = pair_open_bounded(&p, MOQ_VERSION_DRAFT_16, 2, true, 0, 4);
    g_hold_bidi = false;
    MOQ_TEST_CHECK(ok);
    if (!ok) { pair_close(&p); return failures - before; }
    p.use_rcbuf = use_rcbuf;
    moq_fetch_t fh[5], sfh[5];
    if (!fetch_all(&p, 5, fh, sfh)) { MOQ_TEST_CHECK(false); pair_close(&p); return failures - before; }
    obs_t co;
    fill_pool(&p, sfh, &co);
    uint64_t s3 = answer(&p, sfh[2], 0);
    uint64_t s4 = answer(&p, sfh[3], 0);
    MOQ_TEST_CHECK(held_find(&p, s3) && held_find(&p, s4));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s4)._v != 0);   /* mapped */
    uint64_t s5 = answer(&p, sfh[4], 0);
    uint64_t t5 = p.now;
    held_t *h5 = held_find(&p, s5);
    MOQ_TEST_CHECK(h5 != NULL);
    if (!h5) { pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s5)._v == 0); /* unmapped */
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s5));

    /* Borrowed rcbuf: refused and not kept. */
    {
        uint8_t d[4] = { 0x05, 0x01, 0x00, 0x00 };
        moq_rcbuf_t *rb = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), d, sizeof(d), &rb) == MOQ_OK);
        if (rb) {
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_rcbuf(p.cb, 30000, rb, false, ++p.now) ==
                           MOQ_ERR_INPUT_NOT_CONSUMED);
            MOQ_TEST_CHECK(moq_rcbuf_refcount(rb) == 1);
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, 30000, 7, ++p.now) == MOQ_OK);
            MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, 30000));
            moq_rcbuf_decref(rb);
        }
    }

    /* A peer RESET settles held s3 and frees its mapping. */
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, s3, 7, ++p.now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3));
    { held_t *x = held_find(&p, s3); if (x) x->active = false; }

    /* The unmapped stream is offered; its FIN cannot overtake its bytes. */
    p.now += 10;
    service_client(&p);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s5));
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, s5, NULL, 0, true, ++p.now) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s5)._v == 0);
    /* That refusal withdrew the offer; the next pass offers it again. */
    p.now += 10;
    service_client(&p);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s5));
    /* Redelivered: mapped, but the receive pool is full -- held again. */
    MOQ_TEST_CHECK(redeliver(&p, s5) == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s5)._v != 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s5));

    /* The receive pool frees: the older held s4 (mapped since its first
     * refusal) is offered first, replayed exactly, and completes once, at
     * its FIN. */
    fetch_seen_t seen;
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    drain_fetch_exact(p.c, fh[0], 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 0);
    MOQ_TEST_CHECK_EQ_INT(seen.ncomplete, 1);
    MOQ_TEST_CHECK_EQ_INT(seen_completions(&seen, fh[0]), 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s4));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s5));
    replay_exact(&p, s4, fh[3], sfh[3]);

    if (replay_transitioned) {
        /* s4's completion freed the receive pool: s5, which was held
         * unmapped and then mapped, is offered, replayed exactly, and
         * completes once, at its FIN. */
        MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s5));
        replay_exact(&p, s5, fh[4], sfh[4]);
        MOQ_TEST_CHECK(held_find(&p, s5) == NULL);
    } else {
        /* s5 is cancelled at its original age, and only s5. */
        int on_sid = 0, others = 0;
        p.now = t5 + HOLD_AGE_US;
        service_client_stops(&p, s5, MOQ_STREAM_ERROR_CANCELLED, &on_sid, &others);
        MOQ_TEST_CHECK_EQ_INT(on_sid, 1);
        MOQ_TEST_CHECK_EQ_INT(others, 0);
        h5->active = false;
        MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s5));
    }

    /* A live response still delivers; no replayed response completes again. */
    MOQ_TEST_CHECK(write_obj(p.sv, sfh[1], 1, p.now) == MOQ_OK);
    pump(&p, 4);
    drain_fetch_exact(p.c, fh[1], 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 1);
    MOQ_TEST_CHECK_EQ_INT(seen.ncomplete, 0);
    MOQ_TEST_CHECK_EQ_INT(seen.closed, 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    pair_close(&p);
    if (failures == before)
        printf("PASS: hold_input bounded unmapped v16 %s %s\n", lbl,
               replay_transitioned ? "transitioned replay" : "expiry");
    return failures - before;
}

/* Drafts 18 and 22 (uni control pair): the fetcher's stream map is exactly
 * full -- its two control streams, three outstanding request streams and two
 * live responses -- when a third response arrives. Its first chunk carries a
 * complete data stream type, so it is held without a mapping instead of
 * failing the session; a borrowed rcbuf of another such stream is refused and
 * not kept. When a live response ends, its mapping frees and the held
 * response is offered. Redelivered, it is mapped, admitted and replayed
 * exactly, completing once at its FIN; never redelivered, it is cancelled at
 * its age with EXCESSIVE_LOAD (0x9), and nothing else is. Control and the
 * other response keep working throughout. */
static int
row_bounded_unmapped_uni_pair(moq_version_t version, bool use_rcbuf, bool replay)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    g_hold_bidi = true;
    pair_t p;
    bool ok = pair_open_bounded(&p, version, 2, true, 0, 7);
    g_hold_bidi = false;
    MOQ_TEST_CHECK(ok);
    if (!ok) { pair_close(&p); return failures - before; }
    p.use_rcbuf = use_rcbuf;
    moq_fetch_t fh[3], sfh[3];
    if (!fetch_all(&p, 3, fh, sfh)) { MOQ_TEST_CHECK(false); pair_close(&p); return failures - before; }
    obs_t co;
    fill_pool(&p, sfh, &co);
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_stream_count(p.cb), 7);

    uint64_t s3 = answer(&p, sfh[2], 0);
    uint64_t t3 = p.now;
    held_t *h3 = held_find(&p, s3);
    MOQ_TEST_CHECK(h3 != NULL);
    if (!h3) { pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v == 0);   /* unmapped */
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s3));
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_stream_count(p.cb), 7);

    /* Borrowed rcbuf with a subgroup header type (0x10): refused, not kept. */
    {
        uint8_t d[4] = { 0x10, 0x01, 0x00, 0x00 };
        moq_rcbuf_t *rb = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), d, sizeof(d), &rb) == MOQ_OK);
        if (rb) {
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_rcbuf(p.cb, 30000, rb, false, ++p.now) ==
                           MOQ_ERR_INPUT_NOT_CONSUMED);
            MOQ_TEST_CHECK(moq_rcbuf_refcount(rb) == 1);
            MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, 30000)._v == 0);
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, 30000, 7, ++p.now) == MOQ_OK);
            MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, 30000));
            moq_rcbuf_decref(rb);
        }
    }

    /* A live response ends: its mapping frees and the held one is offered. */
    fetch_seen_t seen;
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    drain_fetch_exact(p.c, fh[0], 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 0);
    MOQ_TEST_CHECK_EQ_INT(seen_completions(&seen, fh[0]), 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3));
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v == 0);   /* offered, not mapped */

    if (replay) {
        replay_exact(&p, s3, fh[2], sfh[2]);
        MOQ_TEST_CHECK(held_find(&p, s3) == NULL);
    } else {
        int on_sid = 0, others = 0;
        p.now = t3 + HOLD_AGE_US;
        service_client_stops(&p, s3, MOQ_STREAM_ERROR_EXCESSIVE_LOAD, &on_sid, &others);
        MOQ_TEST_CHECK_EQ_INT(on_sid, 1);
        MOQ_TEST_CHECK_EQ_INT(others, 0);
        h3->active = false;                     /* the adapter drops the chunk */
        MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s3));
    }

    MOQ_TEST_CHECK(write_obj(p.sv, sfh[1], 1, p.now) == MOQ_OK);
    pump(&p, 4);
    drain_fetch_exact(p.c, fh[1], 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 1);
    MOQ_TEST_CHECK_EQ_INT(seen.closed, 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    pair_close(&p);
    if (failures == before)
        printf("PASS: hold_input bounded unmapped %s %s %s\n", lbl,
               use_rcbuf ? "rcbuf" : "bytes", replay ? "replay" : "expiry");
    return failures - before;
}

/* Drafts 18 and 22: a stream mapping and a receive entry are released
 * separately. The fetcher's map is full -- two control streams, four
 * outstanding request streams, two live responses and a third response held
 * mapped because the receive pool is full -- so a fourth response is held
 * without a mapping. The peer resets the mapped held response: a mapping
 * frees, but the receive pool is still full. The unmapped response is
 * offered for its mapping; redelivered, it is mapped and refused again at
 * admission, with zero bytes consumed and its FIN still behind its bytes,
 * and stays held. Only when a live response ends and frees a receive entry
 * is it offered again, replayed exactly, and completed once at its FIN. */
static int
row_unmapped_then_admission_refused_uni_pair(moq_version_t version, bool use_rcbuf)
{
    const char *lbl = version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    g_hold_bidi = true;
    pair_t p;
    bool ok = pair_open_bounded(&p, version, 2, true, 0, 9);
    g_hold_bidi = false;
    MOQ_TEST_CHECK(ok);
    if (!ok) { pair_close(&p); return failures - before; }
    p.use_rcbuf = use_rcbuf;
    moq_fetch_t fh[4], sfh[4];
    if (!fetch_all(&p, 4, fh, sfh)) { MOQ_TEST_CHECK(false); pair_close(&p); return failures - before; }
    obs_t co;
    fill_pool(&p, sfh, &co);
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_stream_count(p.cb), 8);

    /* The third response takes the last mapping and is held at admission. */
    uint64_t s3 = answer(&p, sfh[2], 0);
    MOQ_TEST_CHECK(held_find(&p, s3) != NULL);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v != 0);
    MOQ_TEST_CHECK(!owned(&p, s3));
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_stream_count(p.cb), 9);

    /* The fourth finds no mapping and is held without one. */
    uint64_t s4 = answer(&p, sfh[3], 0);
    held_t *h4 = held_find(&p, s4);
    MOQ_TEST_CHECK(h4 != NULL);
    if (!h4) { pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s4)._v == 0);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s4));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));

    /* The peer resets s3: its mapping frees, the receive pool stays full. */
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, s3, 7, ++p.now) == MOQ_OK);
    { held_t *x = held_find(&p, s3); if (x) x->active = false; }
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s3)._v == 0);
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_stream_count(p.cb), 8);
    MOQ_TEST_CHECK(!moq_session_can_admit_data_stream(p.c));

    /* Offered for the free mapping; its FIN cannot overtake its bytes, and
     * that refusal withdraws the offer until the next pass. */
    p.now += 10;
    service_client(&p);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s4));
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, s4, NULL, 0, true, ++p.now) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s4)._v == 0);
    p.now += 10;
    service_client(&p);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s4));

    /* Redelivered while admission still refuses: mapped, refused whole and
     * held again. A borrowed rcbuf keeps only its owner's reference. */
    {
        moq_result_t rc;
        p.now += 10;
        if (use_rcbuf) {
            moq_rcbuf_t *rb = NULL;
            MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), h4->data, h4->first_len, &rb) == MOQ_OK);
            rc = rb ? moq_transport_bridge_on_peer_uni_rcbuf(p.cb, s4, rb, h4->first_fin, p.now)
                    : MOQ_ERR_NOMEM;
            if (rb) {
                MOQ_TEST_CHECK(moq_rcbuf_refcount(rb) == 1);
                moq_rcbuf_decref(rb);
            }
        } else {
            rc = moq_transport_bridge_on_peer_uni_bytes(p.cb, s4, h4->data, h4->first_len,
                                                        h4->first_fin, p.now);
        }
        MOQ_TEST_CHECK(rc == MOQ_ERR_INPUT_NOT_CONSUMED);
    }
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(p.cb, s4)._v != 0);    /* mapped now */
    MOQ_TEST_CHECK(!owned(&p, s4));
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s4));
    MOQ_TEST_CHECK_EQ_INT((int)moq_transport_bridge_stream_count(p.cb), 9);
    MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, s4, NULL, 0, true, ++p.now) ==
                   MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(held_find(&p, s4) == h4);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));

    /* Another pass with the pool still full offers nothing. */
    p.now += 10;
    service_client(&p);
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, s4));
    MOQ_TEST_CHECK(!moq_session_can_admit_data_stream(p.c));

    /* A live response ends: a receive entry frees, s4 is offered, replayed
     * exactly and completes once at its FIN. */
    fetch_seen_t seen;
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    pump(&p, 3);
    drain_fetch_exact(p.c, fh[0], 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 0);
    MOQ_TEST_CHECK_EQ_INT(seen_completions(&seen, fh[0]), 1);
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, s4));
    replay_exact(&p, s4, fh[3], sfh[3]);
    MOQ_TEST_CHECK(held_find(&p, s4) == NULL);

    MOQ_TEST_CHECK(write_obj(p.sv, sfh[1], 1, p.now) == MOQ_OK);
    pump(&p, 4);
    drain_fetch_exact(p.c, fh[1], 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
    MOQ_TEST_CHECK_EQ_INT(seen.objects, 1);
    MOQ_TEST_CHECK_EQ_INT(seen.closed, 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    pair_close(&p);
    if (failures == before)
        printf("PASS: hold_input unmapped then admission refused %s %s\n", lbl,
               use_rcbuf ? "rcbuf" : "bytes");
    return failures - before;
}

/* Cancellation isolation on a live protocol pair (draft-16 section 12.1 asks
 * for the cancellation of one stream; subscriptions and other subgroups are
 * not affected). The fetcher holds a subscription whose subgroup is open and
 * one live response; a second response is held and cancelled at its age.
 * The subscription survives, and its next subgroup is delivered. */
static int
row_bounded_subscription_survives_d16(void)
{
    int before = failures;
    g_hold_bidi = true;
    pair_t p;
    bool ok = pair_open(&p, MOQ_VERSION_DRAFT_16, 2, true, 0);
    g_hold_bidi = false;
    MOQ_TEST_CHECK(ok);
    if (!ok) { pair_close(&p); return failures - before; }

    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = nsp, .count = 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    moq_subscription_t csub;
    MOQ_TEST_CHECK(moq_session_subscribe(p.c, &sc, p.now, &csub) == MOQ_OK);
    pump(&p, 4);
    moq_subscription_t ssub = { 0 };
    bool got = false;
    {
        moq_event_t ev;
        while (moq_session_poll_events(p.sv, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) { ssub = ev.u.subscribe_request.sub; got = true; }
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK(got);
    if (!got) { pair_close(&p); return failures - before; }
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    MOQ_TEST_CHECK(moq_session_accept_subscribe(p.sv, ssub, &ac, p.now) == MOQ_OK);
    pump(&p, 4);

    int objects = 0, ends = 0;
    moq_subgroup_cfg_t g;
    moq_subgroup_cfg_init(&g);
    g.group_id = 0; g.subgroup_id = 0; g.publisher_priority = 10;
    moq_subgroup_handle_t sg0;
    MOQ_TEST_CHECK(moq_session_open_subgroup(p.sv, ssub, &g, p.now, &sg0) == MOQ_OK);
    {
        uint8_t body[4] = { 1, 2, 3, 4 };
        moq_rcbuf_t *pl = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(p.sv, sg0, 0, pl, p.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    pump(&p, 4);

    moq_fetch_t fh[2], sfh[2];
    if (!fetch_all(&p, 2, fh, sfh)) { MOQ_TEST_CHECK(false); pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(accept_and_write(&p, sfh[0], 0));
    pump(&p, 4);
    MOQ_TEST_CHECK(!moq_session_can_admit_data_stream(p.c));
    uint64_t s2 = answer(&p, sfh[1], 0);
    uint64_t t2 = p.now;
    held_t *h2 = held_find(&p, s2);
    MOQ_TEST_CHECK(h2 != NULL);
    if (!h2) { pair_close(&p); return failures - before; }
    {
        moq_event_t ev;
        while (moq_session_poll_events(p.c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) objects++;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE || ev.kind == MOQ_EVENT_UNSUBSCRIBED) ends++;
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(objects, 1);

    int on_sid = 0, others = 0;
    p.now = t2 + HOLD_AGE_US;
    service_client_stops(&p, s2, MOQ_STREAM_ERROR_CANCELLED, &on_sid, &others);
    MOQ_TEST_CHECK_EQ_INT(on_sid, 1);
    MOQ_TEST_CHECK_EQ_INT(others, 0);
    h2->active = false;
    pump(&p, 4);

    /* The subscription's first subgroup ends and a later one is delivered. */
    MOQ_TEST_CHECK(moq_session_close_subgroup(p.sv, sg0, p.now) == MOQ_OK);
    pump(&p, 4);
    g.group_id = 1;
    moq_subgroup_handle_t sg1;
    MOQ_TEST_CHECK(moq_session_open_subgroup(p.sv, ssub, &g, p.now, &sg1) == MOQ_OK);
    {
        uint8_t body[4] = { 5, 6, 7, 8 };
        moq_rcbuf_t *pl = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), body, sizeof(body), &pl) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_object(p.sv, sg1, 0, pl, p.now) == MOQ_OK);
        moq_rcbuf_decref(pl);
    }
    pump(&p, 6);
    int later = 0;
    {
        moq_event_t ev;
        while (moq_session_poll_events(p.c, &ev, 1) > 0) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED && ev.u.object_received.group_id == 1) later++;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE || ev.kind == MOQ_EVENT_UNSUBSCRIBED) ends++;
            moq_event_cleanup(&ev);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(later, 1);
    MOQ_TEST_CHECK_EQ_INT(ends, 0);
    MOQ_TEST_CHECK(moq_session_state(p.c) == MOQ_SESS_ESTABLISHED && !moq_transport_bridge_is_fatal(p.cb));
    pair_close(&p);
    if (failures == before) printf("PASS: hold_input bounded subscription survives v16\n");
    return failures - before;
}

/* -- Peer uni input deferred behind pending control input (drafts 18, 22) -- *
 *
 * The fetcher's one-slot event queue holds its unpolled FETCH_OK, so the
 * publisher's GOAWAY on the control stream cannot be taken at once: the
 * session retains it and the bridge waits for it to drain. A response chunk
 * on another uni stream arriving meanwhile is refused whole -- zero bytes,
 * no FIN, the adapter keeps it -- and the stream reports pending until the
 * control input drains; redelivered then, it is parsed as if it had just
 * arrived. Shapes: the response chunk alone; the chunk carrying the
 * response's FIN; its stream type split across callbacks, the first byte
 * accepted before the control input blocked; a peer RESET before the
 * replay. */
typedef enum { DF_PLAIN, DF_FIN, DF_SPLIT, DF_RESET } df_shape_t;

static const char *df_name(df_shape_t t)
{
    switch (t) {
    case DF_PLAIN: return "chunk";
    case DF_FIN:   return "chunk with FIN";
    case DF_SPLIT: return "split type";
    case DF_RESET: return "RESET before replay";
    }
    return "?";
}

static int
row_deferred_behind_control(moq_version_t version, df_shape_t shape, bool use_rcbuf)
{
    const char *vl = version == MOQ_VERSION_DRAFT_18 ? "v18" : "v22";
    int before = failures;
    const char *lbl = use_rcbuf ? "rcbuf" : "bytes";
    pair_t p;
    bool ok = pair_open(&p, version, 4, true, 1);
    MOQ_TEST_CHECK(ok);
    if (!ok) { pair_close(&p); return failures - before; }
    p.use_rcbuf = use_rcbuf;
    moq_fetch_t fh[1], sfh[1];
    if (!fetch_all(&p, 1, fh, sfh)) { MOQ_TEST_CHECK(false); pair_close(&p); return failures - before; }

    /* The response data stream is opened by the accept itself (with its
     * FETCH_HEADER), so its id is taken before the accept. The accept, the
     * object and (DF_FIN) the end are then served together: writes on that
     * stream are captured instead of delivered, and everything else --
     * FETCH_OK, which fills the fetcher's single event slot and stays
     * unpolled -- is delivered. */
    const uint64_t sid = p.sep.next_uni_id;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 3;
    moq_result_t arc = moq_session_accept_fetch(p.sv, sfh[0], &ac, p.now);
    MOQ_TEST_CHECK(arc == MOQ_OK);
    if (arc != MOQ_OK) { pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(write_obj(p.sv, sfh[0], 0, p.now) == MOQ_OK);
    if (shape == DF_FIN) MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, sfh[0], p.now) == MOQ_OK);
    p.now += 10;
    moq_transport_bridge_service(p.sb, p.now);
    uint8_t raw[512];
    size_t raw_len = 0;
    bool raw_fin = false, opened = false, overflow = false;
    for (size_t j = 0; j < p.sep.count; j++) {
        fake_op_t *o = &p.sep.ops[j];
        if (o->kind == FAKE_OP_OPEN_UNI && o->stream_id == sid) opened = true;
        if (o->kind == FAKE_OP_WRITE && o->stream_id == sid) {
            if (raw_len + o->data_len <= sizeof(raw)) {
                memcpy(raw + raw_len, o->data, o->data_len);
                raw_len += o->data_len;
            } else {
                overflow = true;
            }
            if (o->fin) raw_fin = true;
            o->kind = FAKE_OP_OPEN_UNI;       /* withheld from the delivery below */
        }
    }
    (void)deliver(&p, &p.sep, p.cb, false);
    pump(&p, 3);                              /* FETCH_OK reaches the fetcher */
    MOQ_TEST_CHECK(opened);
    MOQ_TEST_CHECK(!overflow);
    MOQ_TEST_CHECK(raw_len > 1 && raw[0] == 0x05);
    MOQ_TEST_CHECK(raw_fin == (shape == DF_FIN));
    if (!opened || overflow || raw_len < 2 || raw[0] != 0x05 || raw_fin != (shape == DF_FIN)) {
        pair_close(&p);
        return failures - before;
    }

    /* Split type: three-byte vi64 for 0x05 (0xc0 0x00 0x05); its first byte
     * is accepted now, before the control input blocks. */
    uint8_t chunk[520];
    size_t chunk_len;
    if (shape == DF_SPLIT) {
        uint8_t first = 0xc0;
        p.now += 10;
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, sid, &first, 1, false, p.now) == MOQ_OK);
        chunk[0] = 0x00;
        chunk[1] = 0x05;
        memcpy(chunk + 2, raw + 1, raw_len - 1);
        chunk_len = raw_len + 1;
    } else {
        memcpy(chunk, raw, raw_len);
        chunk_len = raw_len;
    }

    /* The publisher's GOAWAY reaches the fetcher's full event queue. */
    MOQ_TEST_CHECK(moq_session_goaway(p.sv, NULL, 0, p.now) == MOQ_OK);
    pump(&p, 2);
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p.cb));

    /* The response chunk: refused whole and kept by the adapter. */
    p.now += 10;
    moq_result_t rc = feed_uni(&p, sid, chunk, chunk_len, raw_fin);
    held_t *h = held_find(&p, sid);
    printf("deferred behind control %s %s %s: rc=%d held=%d pending=%d\n", vl, df_name(shape), lbl, (int)rc,
           h != NULL, (int)moq_transport_bridge_stream_has_pending(p.cb, sid));
    MOQ_TEST_CHECK(rc == MOQ_ERR_INPUT_NOT_CONSUMED);
    MOQ_TEST_CHECK(h != NULL && h->len == chunk_len && h->fin == raw_fin);
    if (!h) { pair_close(&p); return failures - before; }
    MOQ_TEST_CHECK(moq_transport_bridge_stream_has_pending(p.cb, sid));
    MOQ_TEST_CHECK(!owned(&p, sid));
    if (shape == DF_PLAIN) {
        /* A FIN-only delivery cannot overtake the refused bytes, and a
         * borrowed rcbuf is never kept. */
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_bytes(p.cb, sid, NULL, 0, true, p.now) ==
                       MOQ_ERR_INPUT_NOT_CONSUMED);
        moq_rcbuf_t *rb = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), chunk, chunk_len, &rb) == MOQ_OK);
        if (rb) {
            MOQ_TEST_CHECK(moq_transport_bridge_on_peer_uni_rcbuf(p.cb, sid, rb, false, p.now) ==
                           MOQ_ERR_INPUT_NOT_CONSUMED);
            MOQ_TEST_CHECK(moq_rcbuf_refcount(rb) == 1);
            moq_rcbuf_decref(rb);
        }
        MOQ_TEST_CHECK(!owned(&p, sid));
    }
    if (shape == DF_RESET) {
        MOQ_TEST_CHECK(moq_transport_bridge_on_peer_stream_reset(p.cb, sid, 7, p.now) == MOQ_OK);
        h->active = false;                    /* the adapter drops the chunk */
    }

    /* The control input drains once the fetcher polls its event queue. */
    fetch_seen_t seen, total;
    memset(&total, 0, sizeof(total));
    for (int i = 0; i < 6 && moq_transport_bridge_stream_has_pending(p.cb, sid); i++) {
        drain_fetch_exact(p.c, fh[0], 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
        total.objects += seen.objects;
        p.now += 10;
        service_client(&p);
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_stream_has_pending(p.cb, sid));
    MOQ_TEST_CHECK(total.objects == 0);       /* nothing of the stream before its replay */

    int matching = 0, complete = 0, objects = 0;
    if (shape != DF_RESET) {
        rc = redeliver(&p, sid);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    }
    for (int i = 0; i < 6; i++) {
        drain_fetch_exact(p.c, fh[0], 0, 0, k_obj0_body, sizeof(k_obj0_body), &seen);
        matching += seen.matching;
        objects += seen.objects;
        complete += seen_completions(&seen, fh[0]);
        MOQ_TEST_CHECK(seen.mismatched == 0 && seen.error == 0 && seen.reset == 0 && seen.closed == 0);
        p.now += 10;
        service_client(&p);
    }
    printf("deferred behind control %s %s %s replay: objects=%d matching=%d complete=%d fatal=%d\n", vl, df_name(shape),
           lbl, objects, matching, complete, (int)moq_transport_bridge_is_fatal(p.cb));
    if (shape == DF_RESET) {
        MOQ_TEST_CHECK(objects == 0 && complete == 0);
    } else {
        MOQ_TEST_CHECK(matching == 1 && objects == 1);
        MOQ_TEST_CHECK(complete == (shape == DF_FIN ? 1 : 0));
        MOQ_TEST_CHECK(owned(&p, sid) || shape == DF_FIN);
    }
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p.cb));
    MOQ_TEST_CHECK(moq_session_state(p.c) != MOQ_SESS_CLOSED);
    pair_close(&p);
    if (failures == before)
        printf("PASS: hold_input deferred behind control %s %s %s\n", vl, df_name(shape), lbl);
    return failures - before;
}

int main(void)
{
    moq_version_t versions[3] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                  MOQ_VERSION_DRAFT_22 };
    for (int v = 0; v < 3; v++) {
        row_replay_owned_pressure(versions[v], false, false);
        row_replay_owned_pressure(versions[v], true, false);
        row_replay_owned_pressure(versions[v], false, true);
        row_replay_owned_pressure(versions[v], true, true);
        (void)row_admission(versions[v], false);
        (void)row_admission(versions[v], true);
        (void)row_capacity_race(versions[v]);
        (void)row_fin_reset_teardown(versions[v]);
        (void)row_retained_pressure(versions[v]);
        (void)row_fin_before_redelivery(versions[v], false);
        (void)row_fin_before_redelivery(versions[v], true);
        (void)row_zero_byte_fin_retry(versions[v]);
        (void)row_lifetime_contract(versions[v]);
        row_mapping_bound(versions[v]);
    }
    (void)row_prefix_fragment(MOQ_VERSION_DRAFT_18);
    (void)row_prefix_fragment(MOQ_VERSION_DRAFT_22);
    (void)row_bounded_age_blocked_stop(MOQ_VERSION_DRAFT_16);
    (void)row_bounded_age_blocked_stop(MOQ_VERSION_DRAFT_18);
    (void)row_bounded_age_blocked_stop(MOQ_VERSION_DRAFT_22);
    (void)row_bounded_unmapped_d16(false, false);
    (void)row_bounded_unmapped_d16(true, false);
    (void)row_bounded_unmapped_d16(false, true);
    (void)row_bounded_unmapped_d16(true, true);
    (void)row_bounded_subscription_survives_d16();
    for (int v = 1; v < 3; v++) {
        (void)row_bounded_unmapped_uni_pair(versions[v], false, false);
        (void)row_bounded_unmapped_uni_pair(versions[v], true, false);
        (void)row_bounded_unmapped_uni_pair(versions[v], false, true);
        (void)row_bounded_unmapped_uni_pair(versions[v], true, true);
        (void)row_unmapped_then_admission_refused_uni_pair(versions[v], false);
        (void)row_unmapped_then_admission_refused_uni_pair(versions[v], true);
    }
    for (int v = 1; v < 3; v++)
        for (int t = DF_PLAIN; t <= DF_RESET; t++) {
            (void)row_deferred_behind_control(versions[v], (df_shape_t)t, false);
            (void)row_deferred_behind_control(versions[v], (df_shape_t)t, true);
        }
    if (failures == 0) MOQ_TEST_PASS("bridge_hold_input");
    return failures ? 1 : 0;
}
