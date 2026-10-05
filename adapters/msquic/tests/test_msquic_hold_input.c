/*
 * Receive admission backpressure in the MsQuic adapter, over the fake
 * QUIC_API_TABLE and the production bridge.
 *
 * A fetcher (client session + the adapter under test) is paired with a raw
 * server session over a fake endpoint; the test relays wire bytes between
 * them on one thread. The fetcher's receive pool holds two entries. When the
 * bridge refuses a new response stream with MOQ_ERR_INPUT_NOT_CONSUMED, the
 * adapter must leave that buffer -- and every later one of the RECEIVE --
 * with MsQuic (accepted TotalBufferLength excludes it), pause only that
 * stream, re-enable it once the bridge reports it ready, and let MsQuic
 * redeliver the held bytes exactly once. A zero-byte FIN or a peer send
 * shutdown that arrives while bytes are held is owned by the adapter until
 * the held bytes are accepted. Refused-again redelivery stays lossless; a
 * peer abort or teardown releases everything exactly once.
 *
 * The fake table models MsQuic's default (non-multi) receive mode: a
 * partially accepted RECEIVE stashes the unconsumed tail and disables
 * delivery until StreamReceiveSetEnabled(TRUE); fake_msq_redeliver_held
 * re-indicates it. The fake never replays bytes the adapter accepted.
 */

#include "msquic_internal.h"
#include "support/fake_msq_table.h"
#include "support/held_bridge_driver.h"
#include "test_session_support.h"

#include <moq/msquic.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/transport_bridge.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

static int failures = 0;

/* -- rig: adapter client over the fake table + raw server over a fake endpoint */

typedef struct rig {
    moq_version_t       version;
    fake_msq_t          fake;
    test_alloc_state_t  alloc_state;
    moq_alloc_t         alloc;
    moq_session_t      *c;             /* fetcher session (adapter-attached) */
    moq_msquic_conn_t  *conn;
    moq_session_t      *sv;            /* raw publisher */
    moq_transport_bridge_t *sb;
    fake_endpoint_t     sep;
    fake_msq_stream_t  *ctrl;          /* the fetcher's control bidi */
    size_t              send_cur;      /* client sends relayed so far */
    /* server uni streams conjured as fake peer streams, by server stream id */
    struct { uint64_t sid; fake_msq_stream_t *st; } unis[32];
    int                 n_unis;
    bool                hold_client;   /* do not relay the client's STOPs/cancels */
    int                 relay_error;
    bool                started_done[FAKE_MSQ_MAX_STREAMS];
} rig_t;

static fake_msq_stream_t *
rig_uni(rig_t *r, uint64_t sid)
{
    for (int i = 0; i < r->n_unis; i++) if (r->unis[i].sid == sid) return r->unis[i].st;
    return NULL;
}

static void
deliver_conn_event(rig_t *r, QUIC_CONNECTION_EVENT_TYPE type)
{
    QUIC_CONNECTION_EVENT ev;
    memset(&ev, 0, sizeof(ev));
    ev.Type = type;
    if (type == QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE) {
        ev.STREAMS_AVAILABLE.BidirectionalCount = 8;
        ev.STREAMS_AVAILABLE.UnidirectionalCount = 8;
    }
    (void)moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn, &ev);
}

static fake_msq_stream_t *
deliver_peer_stream(rig_t *r, uint64_t id, bool uni)
{
    fake_msq_stream_t *st = fake_msq_peer_stream(&r->fake, id, uni);
    QUIC_CONNECTION_EVENT ev;
    if (st == NULL) return NULL;
    memset(&ev, 0, sizeof(ev));
    ev.Type = QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED;
    ev.PEER_STREAM_STARTED.Stream = (HQUIC)st;
    ev.PEER_STREAM_STARTED.Flags = uni ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL : QUIC_STREAM_OPEN_FLAG_NONE;
    (void)moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn, &ev);
    return st;
}

static void
deliver_stream_event(fake_msq_stream_t *st, QUIC_STREAM_EVENT_TYPE type, uint64_t code)
{
    QUIC_STREAM_EVENT ev;
    memset(&ev, 0, sizeof(ev));
    ev.Type = type;
    if (type == QUIC_STREAM_EVENT_PEER_SEND_ABORTED) ev.PEER_SEND_ABORTED.ErrorCode = code;
    st->cb((HQUIC)st, st->ctx, &ev);
}

/* A server write addressed to one of the CLIENT's own streams (draft-16
 * control bidi 0, draft-18 request bidi responses) lands on that fake
 * stream; returns false when the id is not a client stream. */
static bool
deliver_local_write(rig_t *r, const fake_op_t *op)
{
    for (int k = 0; k < r->fake.stream_count; k++) {
        fake_msq_stream_t *st = &r->fake.streams[k];
        if (st->in_use && st->id == op->stream_id && !st->uni) {
            fake_msq_deliver_receive(st, op->data, op->data_len, op->fin);
            return true;
        }
    }
    return false;
}

/* Relay the fetcher's new sends to the server (control bytes; STOP/RESET
 * ops arrive through the fake endpoint's op mapping on the server bridge --
 * the fetcher's StreamShutdown calls are recorded on the fake streams, and
 * a STOP_SENDING is forwarded to the server bridge as a peer stop), service
 * the server, and deliver the server's output as RECEIVEs. Server data
 * writes are delivered one write per RECEIVE unless `split` asks for a
 * multi-buffer event. */
typedef struct relay_opts {
    bool hold_server_writes;   /* leave server uni writes undelivered */
    uint64_t multi_sid;        /* this server uni stream: 3-buffer RECEIVE */
} relay_opts_t;

static void
relay(rig_t *r, const relay_opts_t *o)
{
    /* every local stream the adapter started gets its START_COMPLETE once */
    for (int i = 0; i < r->fake.stream_count; i++) {
        fake_msq_stream_t *st = &r->fake.streams[i];
        if (st->in_use && st->started && !r->started_done[i] && st->cb != NULL) {
            r->started_done[i] = true;
            fake_msq_deliver_start_complete(st, QUIC_STATUS_SUCCESS);
        }
    }
    for (; r->send_cur < (size_t)r->fake.send_count; r->send_cur++) {
        fake_msq_send_t *s = &r->fake.sends[r->send_cur];
        bool fin = (s->flags & QUIC_SEND_FLAG_FIN) != 0;
        if (r->version == MOQ_VERSION_DRAFT_18) {
            /* uni-control pair: the client's control uni and request bidis */
            if (s->stream->uni)
                (void)held_bridge_uni_bytes(r->sb, s->stream->id, s->bytes, s->bytes_len, fin, 0);
            else
                (void)moq_transport_bridge_on_peer_bidi_bytes(r->sb, s->stream->id, s->bytes, s->bytes_len, fin, 0);
        } else if (s->stream == r->ctrl) {
            (void)moq_transport_bridge_on_peer_control_bytes(r->sb, 0, s->bytes, s->bytes_len, fin, 0);
        }
    }
    while (fake_msq_deliver_send_complete(&r->fake, false)) { }
    if (!r->hold_client) {
        /* STOP_SENDING the fetcher issued on peer uni streams */
        for (int i = 0; i < r->n_unis; i++) {
            fake_msq_stream_t *st = r->unis[i].st;
            if (st == NULL || st->shutdown_calls == 0) continue;
            if (st->last_shutdown_flags & QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE) {
                (void)moq_transport_bridge_on_peer_stop_sending(r->sb, r->unis[i].sid, st->last_shutdown_code, 0);
                st->shutdown_calls = 0;
            }
        }
    }
    (void)held_bridge_service(r->sb, 0);
    for (size_t i = 0; i < r->sep.count; i++) {
        fake_op_t *op = &r->sep.ops[i];
        if (op->kind == FAKE_OP_OPEN_UNI) {
            if (r->n_unis < 32) {
                /* the server bridge's op id is only a key; the fake peer
                 * stream carries a real server-initiated uni id (3 + 4k) so
                 * it never collides with the client's own stream ids */
                r->unis[r->n_unis].sid = op->stream_id;
                r->unis[r->n_unis].st = deliver_peer_stream(r, 3u + 4u * (uint64_t)r->n_unis, true);
                r->n_unis++;
            }
        } else if (op->kind == FAKE_OP_WRITE) {
            if (!deliver_local_write(r, op)) {
                fake_msq_stream_t *st = rig_uni(r, op->stream_id);
                if (st == NULL || (o && o->hold_server_writes)) continue;
                if (st->recv_disabled) {
                    /* MsQuic indicates nothing on a disabled stream: the
                     * bytes queue behind the held tail (re-indicated
                     * together after StreamReceiveSetEnabled(TRUE)). */
                    MOQ_TEST_CHECK(st->held_len + op->data_len <= sizeof(st->held));
                    if (st->held_len + op->data_len <= sizeof(st->held)) {
                        memcpy(st->held + st->held_len, op->data, op->data_len);
                        st->held_len += (uint32_t)op->data_len;
                        if (op->fin) st->held_fin = true;
                    }
                    continue;
                }
                if (o && o->multi_sid == op->stream_id && op->data_len >= 6) {
                    size_t a = 2, b = (op->data_len - 2) / 2, c = op->data_len - 2 - b;
                    const uint8_t *bufs[3] = { op->data, op->data + a, op->data + a + b };
                    size_t lens[3] = { a, b, c };
                    fake_msq_deliver_receive_multi(st, bufs, lens, 3, op->fin);
                } else {
                    fake_msq_deliver_receive(st, op->data, op->data_len, op->fin);
                }
            }
        } else if (op->kind == FAKE_OP_RESET) {
            fake_msq_stream_t *st = rig_uni(r, op->stream_id);
            if (st) deliver_stream_event(st, QUIC_STREAM_EVENT_PEER_SEND_ABORTED, op->error_code);
        }
    }
    fake_endpoint_clear_ops(&r->sep);
}

static void
pump(rig_t *r, int rounds)
{
    for (int i = 0; i < rounds; i++) {
        relay(r, NULL);
        moq_msquic_conn_service(r->conn);
    }
}

static bool
rig_up(rig_t *r, moq_version_t version)
{
    memset(r, 0, sizeof(*r));
    r->version = version;
    r->alloc = test_allocator(&r->alloc_state);
    fake_msq_init(&r->fake, true);
    moq_session_cfg_t ccfg;
    moq_session_cfg_init_sized(&ccfg, sizeof(ccfg), &r->alloc, MOQ_PERSPECTIVE_CLIENT);
    ccfg.version = version;
    ccfg.send_request_capacity = true;
    ccfg.initial_request_capacity = 64;
    ccfg.max_data_streams = 2;
    ccfg.max_fetches = 32;
    ccfg.max_events = 64;
    if (moq_session_create(&ccfg, 0, &r->c) < 0) return false;
    moq_msquic_conn_cfg_t cfg;
    moq_msquic_conn_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.alloc = &r->alloc;
    cfg.session = r->c;
    cfg.api = fake_msq_table(&r->fake);
    if (moq_msquic_conn_create(&cfg, &r->conn) != MOQ_OK) return false;
    if (moq_msquic_conn_bind(r->conn, fake_msq_conn_handle(&r->fake)) != MOQ_OK) return false;
    moq_session_cfg_t scfg;
    moq_session_cfg_init_sized(&scfg, sizeof(scfg), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    scfg.version = version;
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 64;
    scfg.max_fetches = 32;
    scfg.max_events = 64;
    if (moq_session_create(&scfg, 0, &r->sv) < 0) return false;
    /* server-side uni op ids start at 1000 so they never match one of the
     * client's own stream ids (0,4,8.. bidi; 2,6.. uni) in the relay's
     * local-stream lookup */
    held_endpoint_init(&r->sep, 1000, 1);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (held_bridge_create(&bcfg, r->sv, &r->sep.vtable, &r->sep, &r->sb) != MOQ_OK) return false;
    /* draft-18: the server opens its own uni control stream at start */
    if (version == MOQ_VERSION_DRAFT_18 && moq_session_start(r->sv, 0) < 0) return false;
    deliver_conn_event(r, QUIC_CONNECTION_EVENT_STREAMS_AVAILABLE);
    deliver_conn_event(r, QUIC_CONNECTION_EVENT_CONNECTED);
    r->ctrl = fake_msq_stream_at(&r->fake, 0);
    if (r->ctrl == NULL) return false;
    pump(r, 8);
    moq_event_t ev;
    while (moq_session_poll_events(r->c, &ev, 1) > 0) moq_event_cleanup(&ev);
    while (moq_session_poll_events(r->sv, &ev, 1) > 0) moq_event_cleanup(&ev);
    return moq_session_state(r->c) == MOQ_SESS_ESTABLISHED && moq_session_state(r->sv) == MOQ_SESS_ESTABLISHED &&
           !moq_msquic_conn_is_fatal(r->conn);
}

static void
rig_down(rig_t *r)
{
    if (r->conn) {
        QUIC_CONNECTION_EVENT ev;
        memset(&ev, 0, sizeof(ev));
        ev.Type = QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE;
        (void)moq_msquic_conn_callback()(fake_msq_conn_handle(&r->fake), r->conn, &ev);
        moq_msquic_conn_destroy(r->conn);
    }
    if (r->sb) held_bridge_destroy(r->sb);
    if (r->c) moq_session_destroy(r->c);
    if (r->sv) moq_session_destroy(r->sv);
    MOQ_TEST_CHECK_EQ_INT(r->alloc_state.balance, 0);
}

/* -- fetch helpers ------------------------------------------------------- */

typedef struct obs {
    int ok, object, complete, error, reset, closed, other;
    uint64_t seen[2];     /* object ids 0..127 seen (bit per id) */
    int duplicates;       /* an id seen twice */
    int bad_payload;      /* payload not the writer's pattern (16 x (0xd0 + id)) */
} obs_t;

static void
drain(moq_session_t *s, obs_t *o)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_OK:       o->ok++; break;
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
        case MOQ_EVENT_FETCH_ERROR:    o->error++; break;
        case MOQ_EVENT_FETCH_RESET:    o->reset++; break;
        case MOQ_EVENT_SESSION_CLOSED: o->closed++; break;
        default: o->other++; break;
        }
        moq_event_cleanup(&ev);
    }
}

static uint64_t g_end_object = 3;

/* exact inventory: ids [0, n) each seen exactly once, every payload right */
static bool
inventory_exact(const obs_t *o, int n)
{
    for (int i = 0; i < n; i++) if (!(o->seen[i / 64] & (1ull << (i % 64)))) return false;
    for (int i = n; i < 128; i++) if (o->seen[i / 64] & (1ull << (i % 64))) return false;
    return o->duplicates == 0 && o->bad_payload == 0 && o->object == n;
}

static void
obs_merge(obs_t *tot, const obs_t *co)
{
    tot->ok += co->ok; tot->object += co->object; tot->complete += co->complete; tot->error += co->error;
    tot->reset += co->reset; tot->closed += co->closed; tot->other += co->other;
    for (int i = 0; i < 2; i++) {
        tot->duplicates += (int)__builtin_popcountll(tot->seen[i] & co->seen[i]);
        tot->seen[i] |= co->seen[i];
    }
    tot->duplicates += co->duplicates;
    tot->bad_payload += co->bad_payload;
}

static moq_result_t
issue_fetch(moq_session_t *s, moq_fetch_t *h)
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

static moq_result_t
write_obj(moq_session_t *pub, moq_fetch_t fh, uint64_t oid)
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
    moq_result_t rc = moq_session_write_fetch_object(pub, fh, &oc, 0);
    moq_rcbuf_decref(pl);
    return rc;
}

static bool
fetch_all(rig_t *r, int n, moq_fetch_t *fh, moq_fetch_t *sfh)
{
    for (int k = 0; k < n; k++) MOQ_TEST_CHECK(issue_fetch(r->c, &fh[k]) == MOQ_OK);
    moq_msquic_conn_service(r->conn);
    pump(r, 4);
    moq_event_t ev;
    int got = 0;
    while (moq_session_poll_events(r->sv, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_FETCH_REQUEST && got < n) sfh[got++] = ev.u.fetch_request.fetch;
        moq_event_cleanup(&ev);
    }
    return got == n;
}

static bool
accept_and_write(rig_t *r, moq_fetch_t sfh, uint64_t oid)
{
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = g_end_object;
    if (moq_session_accept_fetch(r->sv, sfh, &ac, 0) != MOQ_OK) return false;
    return write_obj(r->sv, sfh, oid) == MOQ_OK;
}

/* Answer request k; return the server uni stream id and its fake stream. */
static uint64_t
answer(rig_t *r, moq_fetch_t sfh, const relay_opts_t *o, fake_msq_stream_t **st_out)
{
    MOQ_TEST_CHECK(accept_and_write(r, sfh, 0));
    uint64_t sid = r->sep.next_uni_id;
    relay(r, o);
    moq_msquic_conn_service(r->conn);
    if (st_out) *st_out = rig_uni(r, sid);
    return sid;
}

static void
fill_pool(rig_t *r, moq_fetch_t *sfh, obs_t *co)
{
    for (int k = 0; k < 2; k++) {
        MOQ_TEST_CHECK(accept_and_write(r, sfh[k], 0));
        pump(r, 3);
    }
    memset(co, 0, sizeof(*co));
    drain(r->c, co);
    MOQ_TEST_CHECK(co->ok == 2 && co->object == 2 && co->complete == 0 && co->closed == 0);
    MOQ_TEST_CHECK(!moq_session_can_admit_data_stream(r->c));
}

/* -- rows ----------------------------------------------------------------- */

/* Accepted-prefix accounting: a response stream the bridge does not consume
 * leaves ALL its bytes with MsQuic (fake held_len == delivered), the stream
 * is paused and delivery disabled, nothing is STOPped, the session stays
 * established; a later redelivery after an entry frees is accepted exactly
 * once and the request completes. Single-buffer and three-buffer RECEIVEs. */
static int
row_admission(moq_version_t v, bool multi)
{
    int before = failures;
    rig_t r;
    if (!rig_up(&r, v)) {
        MOQ_TEST_CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[6], sfh[6];
    MOQ_TEST_CHECK(fetch_all(&r, 4, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);
    fake_msq_stream_t *st3 = NULL;
    relay_opts_t o = { .multi_sid = multi ? r.sep.next_uni_id : 0 };
    (void)answer(&r, sfh[2], &o, &st3);
    MOQ_TEST_CHECK(st3 != NULL);
    if (!st3) { rig_down(&r); return failures - before; }
    struct moq_msq_stream *ms3 = moq_msquic_test_stream_find(r.conn, st3->id);
    MOQ_TEST_CHECK(ms3 != NULL);
    if (!ms3) { rig_down(&r); return failures - before; }
    printf("admission %s: held_len=%u recv_disabled=%d paused=%d fin_fed=%d\n", multi ? "multi" : "single",
           st3->held_len, (int)st3->recv_disabled, (int)ms3->paused, (int)ms3->fin_fed);
    /* ZERO bytes accepted: MsQuic holds the whole event, delivery disabled. */
    MOQ_TEST_CHECK(st3->held_len > 0 && st3->recv_disabled);
    MOQ_TEST_CHECK(ms3->paused && ms3->recv_disabled);
    MOQ_TEST_CHECK(st3->shutdown_calls == 0);                       /* no STOP */
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn) && moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.ok == 1 && co.object == 0 && co.complete == 0 && co.error == 0 && co.reset == 0);
    /* Unrelated progress: the live streams keep delivering. */
    MOQ_TEST_CHECK(write_obj(r.sv, sfh[0], 1) == MOQ_OK);
    pump(&r, 2);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.object == 1 && co.seen[0] == 2u && co.duplicates == 0 && co.bad_payload == 0); /* id 1 only */
    MOQ_TEST_CHECK(st3->held_len > 0 && ms3->paused);              /* still held */
    /* Capacity returns: the publisher ends live stream 1; the adapter's
     * service pass sees the bridge ready and re-enables delivery. */
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 2);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    MOQ_TEST_CHECK(!ms3->paused && !ms3->recv_disabled && !st3->recv_disabled);
    MOQ_TEST_CHECK(st3->receive_enabled);
    /* MsQuic re-indicates the held bytes exactly once: accepted now. */
    MOQ_TEST_CHECK(fake_msq_redeliver_held(st3));
    MOQ_TEST_CHECK(st3->held_len == 0);
    moq_msquic_conn_service(r.conn);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(inventory_exact(&co, 1) && co.error == 0 && co.reset == 0);
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[2], 0) == MOQ_OK);
    pump(&r, 2);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.complete == 1 && co.closed == 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(r.c, fh[2]) < 0);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn));
    rig_down(&r);
    if (failures == before) printf("PASS: msquic hold_input admission %s %s\n", v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18", multi ? "multi" : "single");
    return failures - before;
}

/* Admission refusal after a nonzero accepted prefix. The response's stream
 * type is delivered as the non-minimal two-byte varint 0x80 0x05 split
 * across buffers: buffer 0 = {0x80}. In draft-18 the bridge classifies peer
 * uni streams itself: an incomplete type is retained bridge-owned
 * (NEED_MORE, the buffer is accepted) and the buffer completing it
 * classifies DATA and is refused at admission -- so the accepted prefix is
 * exactly one byte and MsQuic holds the rest. The refusal is in the middle
 * buffer ({0x80},{0x05},{header..object..FIN}) or in the last buffer
 * ({0x80},{0x05 header..object..FIN}). On redelivery the retained prefix
 * completes the type and the stream recovers: exact single-object inventory
 * and completion. Draft-18 only: the draft-16 bridge classifies nothing
 * (every peer uni stream is data, admission is decided at its first bytes,
 * so refusal is always at buffer 0 -- the admission rows) and the draft-16
 * session keys the stream kind on the raw first byte, so a widened type
 * byte is an unknown stream type there, not a fragment. */
static int
row_admission_prefix(bool refuse_last)
{
    int before = failures;
    const char *lbl = "v18";
    rig_t r;
    if (!rig_up(&r, MOQ_VERSION_DRAFT_18)) {
        MOQ_TEST_CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[6], sfh[6];
    MOQ_TEST_CHECK(fetch_all(&r, 4, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);
    MOQ_TEST_CHECK(accept_and_write(&r, sfh[2], 0));
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[2], 0) == MOQ_OK);
    uint64_t s3 = r.sep.next_uni_id;
    (void)held_bridge_service(r.sb, 0);
    uint8_t all[1024];
    size_t total = 0;
    bool fin = false;
    for (size_t i = 0; i < r.sep.count; i++) {
        fake_op_t *op = &r.sep.ops[i];
        if (op->kind == FAKE_OP_OPEN_UNI && op->stream_id == s3 && r.n_unis < 32) {
            r.unis[r.n_unis].sid = s3;
            r.unis[r.n_unis].st = deliver_peer_stream(&r, 3u + 4u * (uint64_t)r.n_unis, true);
            r.n_unis++;
        } else if (op->kind == FAKE_OP_WRITE && op->stream_id == s3) {
            MOQ_TEST_CHECK(total + op->data_len <= sizeof(all));
            if (total + op->data_len > sizeof(all)) break;
            memcpy(all + total, op->data, op->data_len);
            total += op->data_len;
            fin = fin || op->fin;
        } else if (op->kind == FAKE_OP_WRITE) {
            (void)deliver_local_write(&r, op);
        }
    }
    fake_endpoint_clear_ops(&r.sep);
    fake_msq_stream_t *st3 = rig_uni(&r, s3);
    MOQ_TEST_CHECK(st3 != NULL && fin && total > 1 && all[0] == 0x05);   /* FETCH stream type, minimal */
    if (!st3 || total < 2) { rig_down(&r); return failures - before; }
    /* the same bytes with the type widened to 0x80 0x05 */
    uint8_t wide[1026];
    wide[0] = 0x80; wide[1] = 0x05;
    memcpy(wide + 2, all + 1, total - 1);
    size_t wtotal = total + 1;
    const uint8_t *bufs[3];
    size_t lens[3];
    int nb;
    if (refuse_last) { bufs[0] = wide; lens[0] = 1; bufs[1] = wide + 1; lens[1] = wtotal - 1; nb = 2; }
    else { bufs[0] = wide; lens[0] = 1; bufs[1] = wide + 1; lens[1] = 1; bufs[2] = wide + 2; lens[2] = wtotal - 2; nb = 3; }
    fake_msq_deliver_receive_multi(st3, bufs, lens, nb, true);
    struct moq_msq_stream *ms3 = moq_msquic_test_stream_find(r.conn, st3->id);
    MOQ_TEST_CHECK(ms3 != NULL);
    if (!ms3) { rig_down(&r); return failures - before; }
    const size_t accepted = 1;   /* exactly the {0x80} buffer */
    printf("prefix %s %s: total=%zu accepted=%zu held_len=%u held_first=0x%02x held_fin=%d paused=%d fatal=%d\n", lbl,
           refuse_last ? "last" : "middle", wtotal, accepted, st3->held_len, st3->held_len ? st3->held[0] : 0,
           (int)st3->held_fin, (int)ms3->paused, (int)moq_msquic_conn_is_fatal(r.conn));
    MOQ_TEST_CHECK(st3->held_len == wtotal - accepted);                        /* exact held suffix */
    MOQ_TEST_CHECK(memcmp(st3->held, wide + accepted, wtotal - accepted) == 0);  /* exactly those bytes */
    MOQ_TEST_CHECK(st3->held_fin && st3->recv_disabled);
    MOQ_TEST_CHECK(ms3->held_input && ms3->paused && !ms3->fin_fed);
    MOQ_TEST_CHECK(st3->shutdown_calls == 0);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn) && moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.ok == 1 && co.object == 0 && co.complete == 0 && co.error == 0 && co.reset == 0);
    /* capacity returns: live stream 1 ends; the redelivery (type completed
     * from the retained prefix) is admitted, object + COMPLETE arrive. */
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 2);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    MOQ_TEST_CHECK(!ms3->paused && !st3->recv_disabled);
    MOQ_TEST_CHECK(fake_msq_redeliver_held(st3));
    moq_msquic_conn_service(r.conn);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    printf("prefix %s %s recovery: objects=%d complete=%d held=%u fin_fed=%d dup=%d bad=%d\n", lbl,
           refuse_last ? "last" : "middle", co.object, co.complete, st3->held_len, (int)ms3->fin_fed, co.duplicates,
           co.bad_payload);
    MOQ_TEST_CHECK(inventory_exact(&co, 1) && co.complete == 1 && co.error == 0 && co.reset == 0);
    MOQ_TEST_CHECK(st3->held_len == 0 && ms3->fin_fed && !ms3->held_input);
    MOQ_TEST_CHECK(fetch_resolve_handle(r.c, fh[2]) < 0);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn));
    rig_down(&r);
    if (failures == before) printf("PASS: msquic hold_input admission prefix %s %s\n", lbl, refuse_last ? "last" : "middle");
    return failures - before;
}

/* Legacy pending (retained input) keeps its accounting: with a 1-deep event
 * queue the live stream's second object blocks owned; the buffer that
 * caused it counts as accepted and only the buffers after it are held. */
/* Build the 70-object live response without delivering it; returns the
 * fake stream, the bytes and the byte offset at which each object starts.
 * The publisher writes the stream header first, then each object as a
 * header write followed by its 16-byte payload write: an object starts at
 * every non-payload write after the first. */
static bool
build_live_70(rig_t *r, moq_fetch_t sfh0, uint8_t *all, size_t *total_out, size_t *obj_off, int *n_obj_out,
              bool *fin_out, fake_msq_stream_t **st_out)
{
    MOQ_TEST_CHECK(accept_and_write(r, sfh0, 0));
    uint64_t sid = r->sep.next_uni_id;
    fake_msq_stream_t *st = NULL;
    size_t total = 0;
    int n_obj = 0, n_hdr = 0;
    bool fin = false;
    for (int i = 0; i < 70; i++) {
        if (i > 0) MOQ_TEST_CHECK(write_obj(r->sv, sfh0, (uint64_t)i) == MOQ_OK);
        if (i % 16 != 15 && i != 69) continue;
        (void)held_bridge_service(r->sb, 0);
        for (size_t j = 0; j < r->sep.count; j++) {
            fake_op_t *op = &r->sep.ops[j];
            if (op->kind == FAKE_OP_OPEN_UNI && op->stream_id == sid && r->n_unis < 32) {
                r->unis[r->n_unis].sid = sid;
                r->unis[r->n_unis].st = st = deliver_peer_stream(r, 3u + 4u * (uint64_t)r->n_unis, true);
                r->n_unis++;
            } else if (op->kind == FAKE_OP_WRITE && op->stream_id == sid) {
                if (total + op->data_len > 8192) break;
                if (op->data_len != 16) {
                    if (n_hdr > 0 && n_obj < 72) obj_off[n_obj++] = total;
                    n_hdr++;
                }
                memcpy(all + total, op->data, op->data_len);
                total += op->data_len;
                fin = op->fin;
            } else if (op->kind == FAKE_OP_WRITE) {
                (void)deliver_local_write(r, op);
            }
        }
        fake_endpoint_clear_ops(&r->sep);
    }
    *total_out = total; *n_obj_out = n_obj; *fin_out = fin; *st_out = st;
    return st != NULL && n_obj == 70 && n_hdr == 71;
}

/* Retained-input accounting on a LIVE stream with a 70-object response and
 * a 64-deep fetcher event queue. Control rig: the same bytes delivered as
 * three single-buffer RECEIVEs (split at object boundaries) find the first
 * buffer after which the bridge reports pending input (the session's own
 * retained WOULD_BLOCK) -- that boundary is the session's, not the adapter's.
 * Assertion rig: the same three buffers as ONE multi-buffer RECEIVE; the
 * adapter must accept exactly through that buffer and leave exactly the
 * bytes after it with MsQuic. Then everything drains with an exact object
 * inventory. Admission refusal is reachable only at a stream's first buffer
 * (nothing of a stream is admitted before its first bytes), so a nonzero
 * accepted prefix followed by NOT_CONSUMED cannot occur on a uni data
 * stream and is reported as unreachable rather than claimed. */
static int
row_retained_middle(moq_version_t v)
{
    int before = failures;
    const char *lbl = v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18";
    g_end_object = 100;
    uint8_t all[8192];
    size_t total = 0, obj_off[72];
    int n_obj = 0;
    bool fin = false;
    size_t a = 0, b = 0, c = 0;
    int pending_after = -1;   /* 0,1,2: the buffer after which the bridge first reported pending */
    {
        rig_t r;
        if (!rig_up(&r, v)) {
            MOQ_TEST_CHECK(false && "rig setup");
            rig_down(&r);
            return failures - before;
        }
        moq_fetch_t fh[4], sfh[4];
        MOQ_TEST_CHECK(fetch_all(&r, 1, fh, sfh));
        fake_msq_stream_t *st = NULL;
        MOQ_TEST_CHECK(build_live_70(&r, sfh[0], all, &total, obj_off, &n_obj, &fin, &st));
        if (!st || n_obj != 70) { rig_down(&r); g_end_object = 3; return failures - before; }
        a = obj_off[10]; b = obj_off[65] - obj_off[10]; c = total - obj_off[65];
        const uint8_t *bufs[3] = { all, all + a, all + a + b };
        size_t lens[3] = { a, b, c };
        for (int i = 0; i < 3 && pending_after < 0; i++) {
            fake_msq_deliver_receive(st, bufs[i], lens[i], fin && i == 2);
            if (moq_transport_bridge_stream_has_pending(moq_msquic_test_bridge(r.conn), st->id)) pending_after = i;
        }
        printf("retained %s control: total=%zu bufs=%zu/%zu/%zu pending_after_buffer=%d\n", lbl, total, a, b, c,
               pending_after);
        MOQ_TEST_CHECK(pending_after >= 0);
        rig_down(&r);
    }
    if (pending_after < 0) { g_end_object = 3; return failures - before; }
    rig_t r;
    if (!rig_up(&r, v)) {
        MOQ_TEST_CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    /* a multi-buffer RECEIVE on a LIVE stream whose later buffers hit the
     * fetcher's 64-deep event queue (the test does not drain mid-event) */
    moq_fetch_t fh[4], sfh[4];
    MOQ_TEST_CHECK(fetch_all(&r, 1, fh, sfh));
    fake_msq_stream_t *st = NULL;
    uint8_t all2[8192];
    size_t total2 = 0, obj_off2[72];
    int n_obj2 = 0;
    bool fin2 = false;
    MOQ_TEST_CHECK(build_live_70(&r, sfh[0], all2, &total2, obj_off2, &n_obj2, &fin2, &st));
    if (!st || n_obj2 != 70) { rig_down(&r); g_end_object = 3; return failures - before; }
    MOQ_TEST_CHECK(total2 == total && memcmp(all2, all, total) == 0);   /* identical bytes to the control rig */
    const uint8_t *bufs[3] = { all2, all2 + a, all2 + a + b };
    size_t lens[3] = { a, b, c };
    fake_msq_deliver_receive_multi(st, bufs, lens, 3, fin2);
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r.conn, st->id);
    MOQ_TEST_CHECK(ms != NULL);
    size_t expect_held = 0;
    for (int i = pending_after + 1; i < 3; i++) expect_held += lens[i];
    printf("retained %s multi: held_len=%u expect=%zu (suffix after buffer %d) paused=%d\n", lbl, st->held_len,
           expect_held, pending_after, ms ? (int)ms->paused : -1);
    MOQ_TEST_CHECK(moq_session_has_transport_stream(r.c, moq_transport_bridge_find_ref(
                       moq_msquic_test_bridge(r.conn), st->id)));
    MOQ_TEST_CHECK(st->held_len == expect_held);                                   /* exactly that suffix */
    MOQ_TEST_CHECK(memcmp(st->held, all2 + total2 - expect_held, expect_held) == 0); /* exactly those bytes */
    MOQ_TEST_CHECK(ms && ms->paused);
    /* drain events -> bridge pending clears -> resume -> redelivery of the held suffix */
    obs_t tot = {0}, co;
    int rounds = 0;
    while (rounds++ < 40) {
        memset(&co, 0, sizeof(co));
        drain(r.c, &co);
        obs_merge(&tot, &co);
        moq_msquic_conn_service(r.conn);
        if (!st->recv_disabled && st->held_len > 0) (void)fake_msq_redeliver_held(st);
        if (tot.object >= 70 && st->held_len == 0) break;
    }
    MOQ_TEST_CHECK(inventory_exact(&tot, 70));              /* ids 0..69 once each, payloads intact */
    MOQ_TEST_CHECK(st->held_len == 0 && !moq_msquic_conn_is_fatal(r.conn));
    rig_down(&r);
    g_end_object = 3;
    if (failures == before) printf("PASS: msquic hold_input retained middle\n");
    return failures - before;
}

/* FIN with payload refused: the event's FIN is held with its bytes by MsQuic
 * and the redelivery completes the request. Zero-byte FIN and
 * PEER_SEND_SHUTDOWN while bytes are held: owned by the adapter, fed only
 * after the held bytes are accepted; pause/disable state untouched. */
static int
row_fin_variants(moq_version_t v)
{
    int before = failures;
    rig_t r;
    if (!rig_up(&r, v)) {
        MOQ_TEST_CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[8], sfh[8];
    MOQ_TEST_CHECK(fetch_all(&r, 6, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);

    /* (a) header + object + END in one RECEIVE with FIN: refused whole. */
    MOQ_TEST_CHECK(accept_and_write(&r, sfh[2], 0));
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[2], 0) == MOQ_OK);
    uint64_t s3 = r.sep.next_uni_id;
    {
        /* coalesce the server's writes for this stream into one FIN event */
        (void)held_bridge_service(r.sb, 0);
        uint8_t all[1024];
        size_t total = 0;
        bool fin = false;
        for (size_t i = 0; i < r.sep.count; i++) {
            fake_op_t *op = &r.sep.ops[i];
            if (op->kind == FAKE_OP_OPEN_UNI && op->stream_id == s3 && r.n_unis < 32) {
                r.unis[r.n_unis].sid = s3;
                r.unis[r.n_unis].st = deliver_peer_stream(&r, 3u + 4u * (uint64_t)r.n_unis, true);
                r.n_unis++;
            } else if (op->kind == FAKE_OP_WRITE && op->stream_id == s3) {
                memcpy(all + total, op->data, op->data_len);
                total += op->data_len;
                fin = fin || op->fin;
            } else if (op->kind == FAKE_OP_WRITE) {
                (void)deliver_local_write(&r, op);
            }
        }
        fake_endpoint_clear_ops(&r.sep);
        fake_msq_stream_t *st3 = rig_uni(&r, s3);
        MOQ_TEST_CHECK(st3 != NULL && fin && total > 0);
        if (!st3) { rig_down(&r); return failures - before; }
        fake_msq_deliver_receive(st3, all, total, true);
        struct moq_msq_stream *ms3 = moq_msquic_test_stream_find(r.conn, st3->id);
        MOQ_TEST_CHECK(ms3 != NULL && ms3->paused && !ms3->fin_fed);
        MOQ_TEST_CHECK(st3->held_len == total && st3->held_fin);
        memset(&co, 0, sizeof(co));
        drain(r.c, &co);
        MOQ_TEST_CHECK(co.ok == 1 && co.complete == 0 && co.object == 0);
        /* capacity returns, redelivery carries the FIN: object + COMPLETE */
        MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
        pump(&r, 2);
        memset(&co, 0, sizeof(co));
        drain(r.c, &co);
        MOQ_TEST_CHECK(co.complete == 1);
        MOQ_TEST_CHECK(!st3->recv_disabled);
        MOQ_TEST_CHECK(fake_msq_redeliver_held(st3));
        moq_msquic_conn_service(r.conn);
        memset(&co, 0, sizeof(co));
        drain(r.c, &co);
        printf("fin-with-payload: objects=%d complete=%d held=%u fin_fed=%d\n", co.object, co.complete, st3->held_len,
               ms3 ? (int)ms3->fin_fed : -1);
        MOQ_TEST_CHECK(inventory_exact(&co, 1) && co.complete == 1 && st3->held_len == 0);
        MOQ_TEST_CHECK(fetch_resolve_handle(r.c, fh[2]) < 0);
    }

    /* Refill the pool: request 4's response is admitted. */
    fake_msq_stream_t *st4 = NULL;
    (void)answer(&r, sfh[3], NULL, &st4);
    MOQ_TEST_CHECK(st4 != NULL && st4->held_len == 0);
    MOQ_TEST_CHECK(!moq_session_can_admit_data_stream(r.c));

    /* (b) request 5's response refused (bytes held); then a zero-byte FIN
     * RECEIVE arrives while held: NOT fed as the stream's end -- owned by
     * the adapter -- and the pause/disable state is untouched. */
    fake_msq_stream_t *st5 = NULL;
    (void)answer(&r, sfh[4], NULL, &st5);
    MOQ_TEST_CHECK(st5 != NULL && st5->held_len > 0);
    if (!st5) { rig_down(&r); return failures - before; }
    struct moq_msq_stream *ms5 = moq_msquic_test_stream_find(r.conn, st5->id);
    MOQ_TEST_CHECK(ms5 != NULL);
    if (!ms5) { rig_down(&r); return failures - before; }
    fake_msq_deliver_receive(st5, NULL, 0, true);           /* zero-byte FIN while held */
    printf("zero-byte fin while held: fin_fed=%d paused=%d recv_disabled=%d held=%u\n", (int)ms5->fin_fed,
           (int)ms5->paused, (int)ms5->recv_disabled, st5->held_len);
    MOQ_TEST_CHECK(!ms5->fin_fed);                           /* not the stream's end yet */
    MOQ_TEST_CHECK(ms5->paused && ms5->recv_disabled && st5->held_len > 0);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn) && moq_session_state(r.c) == MOQ_SESS_ESTABLISHED);
    /* PEER_SEND_SHUTDOWN while held: same -- nothing cleared, FIN still owed. */
    deliver_stream_event(st5, QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN, 0);
    MOQ_TEST_CHECK(!ms5->fin_fed && ms5->paused && ms5->recv_disabled);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn));
    /* capacity returns (live stream 2 ends), held bytes redelivered and
     * accepted, THEN the owned FIN is fed: the request completes. */
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[1], 0) == MOQ_OK);
    pump(&r, 2);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.complete == 1);
    MOQ_TEST_CHECK(!st5->recv_disabled);
    MOQ_TEST_CHECK(fake_msq_redeliver_held(st5));
    moq_msquic_conn_service(r.conn);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    printf("after redelivery: objects=%d complete=%d fin_fed=%d held=%u\n", co.object, co.complete, (int)ms5->fin_fed,
           st5->held_len);
    MOQ_TEST_CHECK(inventory_exact(&co, 1) && co.complete == 1 && ms5->fin_fed && st5->held_len == 0);
    MOQ_TEST_CHECK(fetch_resolve_handle(r.c, fh[4]) < 0);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn));
    rig_down(&r);
    if (failures == before) printf("PASS: msquic hold_input fin variants\n");
    return failures - before;
}

/* Repeated refusal (capacity taken by another stream first), reset while
 * held, and teardown with held streams. */
static int
row_race_reset_teardown(moq_version_t v)
{
    int before = failures;
    rig_t r;
    if (!rig_up(&r, v)) {
        MOQ_TEST_CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[8], sfh[8];
    MOQ_TEST_CHECK(fetch_all(&r, 6, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);
    fake_msq_stream_t *st3 = NULL, *st4 = NULL, *st5 = NULL;
    (void)answer(&r, sfh[2], NULL, &st3);
    (void)answer(&r, sfh[3], NULL, &st4);
    MOQ_TEST_CHECK(st3 && st4 && st3->held_len > 0 && st4->held_len > 0);
    if (!st3 || !st4) { rig_down(&r); return failures - before; }
    struct moq_msq_stream *ms3 = moq_msquic_test_stream_find(r.conn, st3->id);
    struct moq_msq_stream *ms4 = moq_msquic_test_stream_find(r.conn, st4->id);
    MOQ_TEST_CHECK(ms3 && ms4 && ms3->paused && ms4->paused);
    /* one entry frees: both streams re-enabled; s4's redelivery wins */
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 2);
    MOQ_TEST_CHECK(!st3->recv_disabled && !st4->recv_disabled);
    MOQ_TEST_CHECK(fake_msq_redeliver_held(st4));
    moq_msquic_conn_service(r.conn);
    MOQ_TEST_CHECK(st4->held_len == 0 && !ms4->paused);
    /* s3 redelivered: refused again, held again, paused again -- lossless */
    printf("race: before s3 redelivery can_admit=%d s3_held=%u s4_held=%u\n", (int)moq_session_can_admit_data_stream(r.c),
           st3->held_len, st4->held_len);
    MOQ_TEST_CHECK(fake_msq_redeliver_held(st3));
    moq_msquic_conn_service(r.conn);
    printf("race: s3 held=%u paused=%d disabled=%d\n", st3->held_len, (int)ms3->paused, (int)st3->recv_disabled);
    MOQ_TEST_CHECK(st3->held_len > 0 && ms3->paused && st3->recv_disabled);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn));
    /* the publisher aborts request 3's response while held: the bridge
     * retires the stream, nothing is resumed, no fatal */
    deliver_stream_event(st3, QUIC_STREAM_EVENT_PEER_SEND_ABORTED, 0x7);
    moq_msquic_conn_service(r.conn);
    MOQ_TEST_CHECK(!ms3->paused && !ms3->recv_disabled);
    MOQ_TEST_CHECK(moq_transport_bridge_find_ref(moq_msquic_test_bridge(r.conn), st3->id)._v == 0);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn));
    deliver_stream_event(st3, QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE, 0);
    MOQ_TEST_CHECK(moq_msquic_test_stream_find(r.conn, st3->id) == NULL);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.reset == 0 && co.error == 0 && co.closed == 0);   /* never attributed */
    /* s4 completes (an entry frees); request 6's response refills it and
     * request 5's response is then held at teardown */
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[3], 0) == MOQ_OK);
    pump(&r, 2);
    fake_msq_stream_t *st6 = NULL;
    (void)answer(&r, sfh[5], NULL, &st6);
    MOQ_TEST_CHECK(st6 && st6->held_len == 0);
    (void)answer(&r, sfh[4], NULL, &st5);
    MOQ_TEST_CHECK(st5 && st5->held_len > 0);
    rig_down(&r);   /* SHUTDOWN_COMPLETE + destroy with a held stream: balance 0 */
    if (failures == before) printf("PASS: msquic hold_input race/reset/teardown\n");
    return failures - before;
}

/* Deferred FIN after retained pending. A held response carries 70 objects
 * (its later writes queue behind the held tail); a bare FIN arrives while
 * held (adapter-owned). Capacity returns and the held bytes are redelivered:
 * the bridge admits them and the session parses 63 objects before its event
 * queue fills, so the rest stays as bridge-owned retained pending (owned
 * WOULD_BLOCK). No further native callback ever comes for this stream. The
 * adapter must still settle the FIN it owns once the retained input drains:
 * exactly one FIN, in order, from the service pass -- the request then
 * COMPLETES with an exact object inventory. */
static int
row_deferred_fin_after_retained(moq_version_t v)
{
    int before = failures;
    rig_t r;
    g_end_object = 100;
    if (!rig_up(&r, v)) {
        MOQ_TEST_CHECK(false && "rig setup");
        rig_down(&r);
        return failures - before;
    }
    moq_fetch_t fh[6], sfh[6];
    MOQ_TEST_CHECK(fetch_all(&r, 3, fh, sfh));
    obs_t co;
    fill_pool(&r, sfh, &co);
    fake_msq_stream_t *st = NULL;
    (void)answer(&r, sfh[2], NULL, &st);
    MOQ_TEST_CHECK(st != NULL && st->held_len > 0 && st->recv_disabled);
    if (!st) { rig_down(&r); g_end_object = 3; return failures - before; }
    for (int i = 1; i < 70; i++) {
        MOQ_TEST_CHECK(write_obj(r.sv, sfh[2], (uint64_t)i) == MOQ_OK);
        if (i % 16 == 15 || i == 69) relay(&r, NULL);   /* queued behind the held tail */
    }
    fake_msq_deliver_receive(st, NULL, 0, true);          /* bare FIN while held */
    struct moq_msq_stream *ms = moq_msquic_test_stream_find(r.conn, st->id);
    MOQ_TEST_CHECK(ms != NULL);
    if (!ms) { rig_down(&r); g_end_object = 3; return failures - before; }
    MOQ_TEST_CHECK(ms->held_input && ms->fin_held && !ms->fin_fed);
    MOQ_TEST_CHECK(moq_session_end_fetch(r.sv, sfh[0], 0) == MOQ_OK);
    pump(&r, 2);
    memset(&co, 0, sizeof(co));
    drain(r.c, &co);
    MOQ_TEST_CHECK(co.complete == 1 && !st->recv_disabled);
    MOQ_TEST_CHECK(fake_msq_redeliver_held(st));          /* admitted; the session retains past 63 objects */
    obs_t tot = {0};
    int rounds = 0;
    while (rounds++ < 12) {
        memset(&co, 0, sizeof(co));
        drain(r.c, &co);
        obs_merge(&tot, &co);
        moq_msquic_conn_service(r.conn);
        if (tot.complete >= 1) break;
    }
    printf("deferred fin %s: objects=%d complete=%d held_len=%u fin_held=%d fin_fed=%d paused=%d pending=%d dup=%d bad=%d\n",
           v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18", tot.object, tot.complete, st->held_len, (int)ms->fin_held,
           (int)ms->fin_fed, (int)ms->paused, (int)moq_transport_bridge_stream_has_pending(moq_msquic_test_bridge(r.conn), st->id),
           tot.duplicates, tot.bad_payload);
    MOQ_TEST_CHECK(inventory_exact(&tot, 70));
    MOQ_TEST_CHECK(tot.complete == 1 && tot.error == 0 && tot.reset == 0 && tot.closed == 0);
    MOQ_TEST_CHECK(ms->fin_fed && !ms->fin_held);
    MOQ_TEST_CHECK(fetch_resolve_handle(r.c, fh[2]) < 0);
    MOQ_TEST_CHECK(!moq_msquic_conn_is_fatal(r.conn));
    rig_down(&r);
    g_end_object = 3;
    if (failures == before) printf("PASS: msquic hold_input deferred fin after retained %s\n", v == MOQ_VERSION_DRAFT_16 ? "v16" : "v18");
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
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return failures - before;
    fake_endpoint_t ep;
    fake_endpoint_init(&ep, 3, 1);
    moq_transport_bridge_cfg_t cfg;
    moq_transport_bridge_cfg_init(&cfg, moq_alloc_default());
    moq_transport_bridge_t *bridge = NULL;
    MOQ_TEST_CHECK(moq_transport_bridge_create(&cfg, session, &ep.vtable, &ep,
                                        &bridge) == MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(bridge == NULL && ep.count == 0);
    moq_transport_bridge_destroy(bridge);
    moq_session_destroy(session);
    if (failures == before) printf("PASS: msquic required HOLD_INPUT\n");
    return failures - before;
}

int main(void)
{
    moq_version_t versions[2] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (int i = 0; i < 2; i++) {
        (void)row_admission(versions[i], false);
        (void)row_admission(versions[i], true);
        (void)row_fin_variants(versions[i]);
        (void)row_race_reset_teardown(versions[i]);
        (void)row_retained_middle(versions[i]);
        (void)row_deferred_fin_after_retained(versions[i]);
        (void)row_required_hold(versions[i]);
    }
    (void)row_admission_prefix(false);
    (void)row_admission_prefix(true);
    if (failures == 0) MOQ_TEST_PASS("msquic_hold_input");
    return failures ? 1 : 0;
}
