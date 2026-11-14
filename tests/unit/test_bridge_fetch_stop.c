/*
 * The transport bridge and a peer STOP_SENDING on the response stream of a
 * FETCH the server serves.
 *
 * After the STOP the stream's remaining payload is obsolete: only a reset
 * whose reliable size covers its header follows. These rows pin that the
 * bridge freezes the stream's payload and FIN when the STOP arrives, before
 * and independent of the session's answer, and keeps the first STOP code
 * for the retry; that the teardown, refused while the server's event queue
 * is full, commits exactly once after relief; that the request stream's
 * reset is submitted once even when the client later stops the request
 * stream too; that a FETCH whose frozen FIN was never written is reset by
 * the bridge itself; and that the bridge's peer-opened refs never wrap.
 *
 * The stream's end custody, kept by the bridge until the outstanding
 * completion, is pinned state by state: a FETCH that ended before the STOP
 * with its FIN not accepted (held by the bridge, still queued in the
 * session, or never handed over because the FETCH ended before its header
 * was) is frozen and answered with one reset carrying the first STOP code
 * and, on drafts 18 and 22, one reset of its request stream with CANCELLED
 * in place of that stream's unaccepted FIN, dispatched by the service pass
 * and never inside the STOP callback, reported by both readiness queries
 * beforehand and retained through a refusal. The request FIN is the
 * FETCH's final message: it is handed to the endpoint only after the
 * response stream's FIN or reset is accepted (FETCH_OK itself goes at
 * once), so a STOP of an OPEN response stream always finds the request half
 * open to reset, and the ordinary FIN follows the response stream. A FIN
 * the provider accepted on the response stream, and a commit report after
 * it, leave the answer to the provider; an earlier reset keeps its code. A STOP
 * refused while the FETCH was live, followed by the FETCH's end or by a
 * different native terminal (a refused REQUEST_UPDATE) that commits first,
 * still resets both streams on drafts 18 and 22, the response with that
 * terminal's code when it has one, the request with CANCELLED and never
 * with a FIN. The request reset is reused
 * by a later STOP of the request while its drain keeps the mapping, whether
 * the endpoint accepted it or still refuses it. The live teardown's
 * reservation is all or nothing under action and event pressure, and with
 * drain pressure once the peer's request FIN was seen (handed over with the
 * FETCH, or arriving later), and always resets both streams on drafts 18
 * and 22. Rows that hold the client's return traffic run the server's
 * bridge alone (serve), so no client reaction settles anything for it.
 */
#include <moq/moq.h>
#include <moq/transport_bridge.h>
#include "test_support.h"
#include "test_binding_support.h"
#include "../support/held_bridge_driver.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include "../../core/src/wire/messages_d22_internal.h"
#include <moq/control_d18.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

static int failures;

#define CLIENT_UNI  1000u
#define CLIENT_BIDI 2000u
#define SERVER_UNI  3000u
#define SERVER_BIDI 4000u

typedef struct pair {
    moq_session_t          *c, *sv;
    fake_endpoint_t         cep, sep;
    moq_transport_bridge_t *cb, *sb;
    uint64_t                now;
    uint64_t                c_ctrl;
    /* What the server's endpoint was asked to do on the watched streams. */
    uint64_t                data_sid, req_sid;
    unsigned                data_resets, req_resets, data_writes, data_fins;
    uint64_t                data_reset_code;
    unsigned                req_fins, other_resets, req_writes;
    uint64_t                req_reset_code;
    /* Endpoint operation order: every tallied operation takes the next
     * number; the last data FIN, data reset and request FIN keep theirs. */
    unsigned                seq, data_fin_seq, data_reset_seq, req_fin_seq;
    /* fetch_open delivers the client's FETCH request with its FIN. */
    bool                    req_fin_on_open;
    uint64_t                fin_sid;
} pair_t;

static bool is_bidi(uint64_t sid)
{
    return (sid >= CLIENT_BIDI && sid < SERVER_UNI) || sid >= SERVER_BIDI;
}

/* Count the server endpoint's operations on the watched streams. Every
 * operation is evidence: a recorder that dropped one fails the row. */
static void tally(pair_t *p)
{
    MOQ_TEST_CHECK(!p->sep.overflowed);
    for (size_t i = 0; i < p->sep.count; i++) {
        const fake_op_t *o = &p->sep.ops[i];
        bool reset = o->kind == FAKE_OP_RESET || o->kind == FAKE_OP_RESET_AT ||
                     o->kind == FAKE_OP_ABORT;
        p->seq++;
        if (p->data_sid && o->stream_id == p->data_sid) {
            if (reset) {
                p->data_resets++;
                p->data_reset_code = o->error_code;
                p->data_reset_seq = p->seq;
            }
            if (o->kind == FAKE_OP_WRITE) {
                p->data_writes++;
                if (o->fin) { p->data_fins++; p->data_fin_seq = p->seq; }
            }
        }
        if (p->req_sid && o->stream_id == p->req_sid) {
            if (reset) { p->req_resets++; p->req_reset_code = o->error_code; }
            if (o->kind == FAKE_OP_WRITE) {
                p->req_writes++;
                if (o->fin) { p->req_fins++; p->req_fin_seq = p->seq; }
            }
        }
        if (reset && o->stream_id != p->data_sid && o->stream_id != p->req_sid)
            p->other_resets++;
    }
}

static void deliver(pair_t *p, fake_endpoint_t *from, moq_transport_bridge_t *to,
                    bool from_client)
{
    bool uni_control = moq_transport_bridge_uses_uni_control(to);
    for (size_t i = 0; i < from->count; i++) {
        const fake_op_t *o = &from->ops[i];
        switch (o->kind) {
        case FAKE_OP_WRITE:
            if (is_bidi(o->stream_id)) {
                if (!uni_control) {
                    if (from_client && p->c_ctrl == UINT64_MAX) p->c_ctrl = o->stream_id;
                    if (o->stream_id == p->c_ctrl) {
                        moq_transport_bridge_on_peer_control_bytes(to, o->stream_id, o->data,
                                                                   o->data_len, o->fin, p->now);
                        break;
                    }
                }
                moq_transport_bridge_on_peer_bidi_bytes(to, o->stream_id, o->data,
                                                        o->data_len,
                                                        o->fin || (from_client && p->fin_sid &&
                                                                   o->stream_id == p->fin_sid),
                                                        p->now);
                break;
            }
            held_bridge_uni_bytes(to, o->stream_id, o->data, o->data_len, o->fin, p->now);
            break;
        case FAKE_OP_RESET:
        case FAKE_OP_RESET_AT:
            held_bridge_reset(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_STOP:
            moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_ABORT:
            held_bridge_reset(to, o->stream_id, o->error_code, p->now);
            moq_transport_bridge_on_peer_stop_sending(to, o->stream_id, o->error_code, p->now);
            break;
        case FAKE_OP_CLOSE:
            held_bridge_close(to, o->error_code, p->now);
            break;
        default:
            break;
        }
    }
    fake_endpoint_clear_ops(from);
}

static void pump(pair_t *p)
{
    for (int i = 0; i < 16; i++) {
        held_bridge_service(p->cb, p->now);
        deliver(p, &p->cep, p->sb, true);
        held_bridge_service(p->sb, p->now);
        tally(p);
        deliver(p, &p->sep, p->cb, false);
    }
}

/* Count, and release, every event the session holds (bounded). */
static unsigned drain_count(moq_session_t *s, moq_event_kind_t kind, moq_event_t *last)
{
    unsigned n = 0;
    moq_event_t ev;
    for (int guard = 0; guard < 256; guard++) {
        if (moq_session_poll_events(s, &ev, 1) == 0) break;
        if (ev.kind == kind) {
            n++;
            if (last) *last = ev;
        }
        moq_event_cleanup(&ev);
    }
    return n;
}

/* server_events / server_actions: the server's event and action queue
 * sizes; 0 keeps the session's default. */
static bool pair_open_ex(pair_t *p, moq_version_t v, uint32_t server_events,
                         uint32_t server_actions)
{
    memset(p, 0, sizeof(*p));
    p->c_ctrl = UINT64_MAX;
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), moq_alloc_default(), MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), moq_alloc_default(), MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = v;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    if (server_events) sc.max_events = server_events;
    if (server_actions) sc.max_actions = server_actions;
    if (moq_session_create(&cc, 0, &p->c) < 0) return false;
    if (moq_session_create(&sc, 0, &p->sv) < 0) return false;
    if (test_bind_if_required(p->c) < 0 || test_bind_if_required(p->sv) < 0) return false;
    held_endpoint_init(&p->cep, CLIENT_UNI, CLIENT_BIDI);
    held_endpoint_init(&p->sep, SERVER_UNI, SERVER_BIDI);
    moq_transport_bridge_cfg_t bcfg;
    moq_transport_bridge_cfg_init(&bcfg, moq_alloc_default());
    if (held_bridge_create(&bcfg, p->c, &p->cep.vtable, &p->cep, &p->cb) < 0) return false;
    if (held_bridge_create(&bcfg, p->sv, &p->sep.vtable, &p->sep, &p->sb) < 0) return false;
    moq_session_start(p->c, 0);
    if (moq_session_uses_uni_control(p->sv)) moq_session_start(p->sv, 0);
    pump(p);
    (void)drain_count(p->c, MOQ_EVENT_SESSION_CLOSED, NULL);
    (void)drain_count(p->sv, MOQ_EVENT_SESSION_CLOSED, NULL);
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(p->sv) == MOQ_SESS_ESTABLISHED;
}

static bool pair_open(pair_t *p, moq_version_t v, uint32_t server_events)
{
    return pair_open_ex(p, v, server_events, 0);
}

static void pair_close(pair_t *p)
{
    if (p->cb) held_bridge_destroy(p->cb);
    if (p->sb) held_bridge_destroy(p->sb);
    if (p->c) moq_session_destroy(p->c);
    if (p->sv) moq_session_destroy(p->sv);
    memset(p, 0, sizeof(*p));
}

static moq_bytes_t ns_part = { (const uint8_t *)"live", 4 };

static moq_result_t write_fetch_object(pair_t *p, moq_fetch_t f, uint64_t id)
{
    moq_rcbuf_t *b = NULL;
    static const uint8_t body[] = "fetch-object";
    if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body) - 1, &b) < 0)
        return MOQ_ERR_NOMEM;
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = 0;
    oc.object_id = id;
    oc.publisher_priority = 100;
    oc.payload = b;
    moq_result_t rc = moq_session_write_fetch_object(p->sv, f, &oc, p->now);
    moq_rcbuf_decref(b);
    return rc;
}

/* A FETCH the server accepted, one object delivered; the server's response
 * stream and (drafts 18 and 22) the request stream are watched. */
static bool fetch_open(pair_t *p, moq_fetch_t *sfetch)
{
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = &ns_part, .count = 1 };
    fc.track_name = (moq_bytes_t){ (const uint8_t *)"past", 4 };
    fc.end_group = 0;
    fc.end_object = 9;
    moq_fetch_t cfetch;
    if (moq_session_fetch(p->c, &fc, p->now, &cfetch) != MOQ_OK) return false;
    held_bridge_service(p->cb, p->now);
    for (size_t i = 0; i < p->cep.count; i++)
        if (p->cep.ops[i].kind == FAKE_OP_OPEN_BIDI) p->req_sid = p->cep.ops[i].stream_id;
    if (p->req_fin_on_open) p->fin_sid = p->req_sid;
    pump(p);
    p->fin_sid = 0;
    moq_event_t ev;
    if (drain_count(p->sv, MOQ_EVENT_FETCH_REQUEST, &ev) != 1) return false;
    *sfetch = ev.u.fetch_request.fetch;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 9;
    if (moq_session_accept_fetch(p->sv, *sfetch, &ac, p->now) != MOQ_OK) return false;
    if (write_fetch_object(p, *sfetch, 0) != MOQ_OK) return false;
    held_bridge_service(p->sb, p->now);
    for (size_t i = 0; i < p->sep.count; i++)
        if (p->sep.ops[i].kind == FAKE_OP_OPEN_UNI) p->data_sid = p->sep.ops[i].stream_id;
    tally(p);
    deliver(p, &p->sep, p->cb, false);
    pump(p);
    (void)drain_count(p->c, MOQ_EVENT_FETCH_OBJECT, NULL);
    return p->data_sid != 0;
}

/* Fill the server's one-slot event queue with an event the test holds. */
static bool fill_server_events(pair_t *p)
{
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = &ns_part, .count = 1 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)"busy", 4 };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t sub;
    if (moq_session_subscribe(p->c, &sc, p->now, &sub) != MOQ_OK) return false;
    pump(p);
    return event_queue_full(p->sv);
}

/* Freeze at receipt, the first code kept, refused teardown changes
 * nothing, one commit after relief; then (drafts 18 and 22) a later STOP
 * of the request stream submits no second reset. */
static void test_freeze_and_retry(moq_version_t v)
{
    pair_t p;
    if (!pair_open(&p, v, 1)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f;
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    MOQ_TEST_CHECK(fill_server_events(&p));
    const size_t queued = p.sv->action_tail - p.sv->action_head;
    const unsigned writes0 = p.data_writes;

    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2B, p.now);
    /* Refused for now: the FETCH is live, its producer still writable; the
     * bridge holds the STOP with its first code and has frozen the stream. */
    const bridge_stream_entry_t *de = bridge_find_by_id_const(p.sb, p.data_sid);
    MOQ_TEST_CHECK(de != NULL);
    if (de) {
        MOQ_TEST_CHECK(bridge_session_is_outgoing_fetch(p.sv, de->ref));
        MOQ_TEST_CHECK(de->peer_stop_received);
        MOQ_TEST_CHECK(de->pending_stop && de->pending_stop_code == 0x2A);
    }
    MOQ_TEST_CHECK(p.sv->action_tail - p.sv->action_head == queued);
    MOQ_TEST_CHECK(write_fetch_object(&p, f, 1) == MOQ_OK);
    pump(&p);
    /* Frozen at receipt: nothing more of the stream reaches the transport. */
    MOQ_TEST_CHECK(p.data_writes == writes0);
    MOQ_TEST_CHECK(p.data_resets == 0);

    /* Relief: the stored STOP retries with its first code, once. */
    (void)drain_count(p.sv, MOQ_EVENT_SUBSCRIBE_REQUEST, NULL);
    pump(&p);
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, &ev) == 1);
    MOQ_TEST_CHECK(ev.u.fetch_cancelled.cause == MOQ_REQUEST_TERMINAL_PEER_STOP_DATA);
    MOQ_TEST_CHECK(ev.u.fetch_cancelled.stream_error_code == 0x2A);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == 0x2A);
    MOQ_TEST_CHECK(p.data_writes == writes0);
    MOQ_TEST_CHECK(write_fetch_object(&p, f, 2) == MOQ_ERR_STALE_HANDLE);

    if (v != MOQ_VERSION_DRAFT_16) {
        MOQ_TEST_CHECK(p.req_resets == 1);
        moq_transport_bridge_on_peer_stop_sending(p.sb, p.req_sid, 0x5, p.now);
        pump(&p);
        MOQ_TEST_CHECK(p.req_resets == 1);
        MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 0);
    }
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

/* The FETCH ends (its FIN queued and frozen) while its teardown waits:
 * no native reset follows, so the bridge answers the STOP with its own
 * reset, once, and the FIN never reaches the transport. */
static void test_self_reset_after_end(moq_version_t v)
{
    pair_t p;
    if (!pair_open(&p, v, 1)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f;
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    MOQ_TEST_CHECK(fill_server_events(&p));
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, f, p.now) == MOQ_OK);
    pump(&p);
    MOQ_TEST_CHECK(p.data_fins == 0);
    (void)drain_count(p.sv, MOQ_EVENT_SUBSCRIBE_REQUEST, NULL);
    pump(&p);
    MOQ_TEST_CHECK(p.data_fins == 0);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == 0x2A);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 0);
    pair_close(&p);
}

/* Run the server's bridge alone: its operations are tallied and dropped,
 * so the client never reacts and none of its resets, STOPs or FINs come
 * back to settle anything for the server. Bounded. */
static void serve(pair_t *p)
{
    for (int i = 0; i < 16; i++) {
        held_bridge_service(p->sb, p->now);
        tally(p);
        fake_endpoint_clear_ops(&p->sep);
    }
}

static const bridge_stream_entry_t *data_entry(const pair_t *p)
{
    return bridge_find_by_id_const(p->sb, p->data_sid);
}

/* The server's live FETCH on response stream ref, or NULL. */
static const moq_fetch_entry_t *server_fetch(const pair_t *p, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < p->sv->fetch_cap; i++)
        if (p->sv->fetches[i].state == MOQ_FETCH_ACCEPTED &&
            p->sv->fetches[i].data_stream_ref._v == ref._v)
            return &p->sv->fetches[i];
    return NULL;
}

/* A retained reset of the server bridge for transport stream sid. */
static bool reset_retained(const pair_t *p, uint64_t sid)
{
    for (uint32_t i = 0; i < p->sb->pending_count; i++)
        if (p->sb->pending[i].kind == PENDING_RESET_STREAM &&
            p->sb->pending[i].has_stream_id && p->sb->pending[i].stream_id == sid)
            return true;
    return false;
}

/* Another FETCH the server accepts, on streams no row watches: its
 * objects are queued output on a different stream. */
static bool fetch_other(pair_t *p, moq_fetch_t *sfetch)
{
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = &ns_part, .count = 1 };
    fc.track_name = (moq_bytes_t){ (const uint8_t *)"more", 4 };
    fc.end_group = 0;
    fc.end_object = 127;
    moq_fetch_t cfetch;
    if (moq_session_fetch(p->c, &fc, p->now, &cfetch) != MOQ_OK) return false;
    pump(p);
    moq_event_t ev;
    if (drain_count(p->sv, MOQ_EVENT_FETCH_REQUEST, &ev) != 1) return false;
    *sfetch = ev.u.fetch_request.fetch;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.end_group = 0;
    ac.end_object = 127;
    if (moq_session_accept_fetch(p->sv, *sfetch, &ac, p->now) != MOQ_OK) return false;
    pump(p);
    (void)drain_count(p->c, MOQ_EVENT_FETCH_OBJECT, NULL);
    return true;
}

/* A FETCH the server accepts and ends before its bridge hands anything of
 * it to the endpoint: empty (its header and FIN one write, the FETCH freed
 * at the accept) or with one object and then end_fetch. The first service
 * runs with every write refused (or, with data_only, every write of the
 * response stream about to open): the response stream is opened, nothing
 * of it is accepted, and the bridge consumed the FETCH header only after
 * the FETCH had already ended natively. */
static bool fetch_open_unconsumed_ex(pair_t *p, bool empty, bool data_only)
{
    moq_fetch_cfg_t fc;
    moq_fetch_cfg_init(&fc);
    fc.track_namespace = (moq_namespace_t){ .parts = &ns_part, .count = 1 };
    fc.track_name = (moq_bytes_t){ (const uint8_t *)"none", 4 };
    fc.end_group = 0;
    fc.end_object = 9;
    moq_fetch_t cfetch;
    if (moq_session_fetch(p->c, &fc, p->now, &cfetch) != MOQ_OK) return false;
    held_bridge_service(p->cb, p->now);
    for (size_t i = 0; i < p->cep.count; i++)
        if (p->cep.ops[i].kind == FAKE_OP_OPEN_BIDI) p->req_sid = p->cep.ops[i].stream_id;
    pump(p);
    moq_event_t ev;
    if (drain_count(p->sv, MOQ_EVENT_FETCH_REQUEST, &ev) != 1) return false;
    moq_fetch_t f = ev.u.fetch_request.fetch;
    moq_accept_fetch_cfg_t ac;
    moq_accept_fetch_cfg_init(&ac);
    ac.empty = empty;
    if (!empty) {
        ac.end_group = 0;
        ac.end_object = 9;
    }
    if (moq_session_accept_fetch(p->sv, f, &ac, p->now) != MOQ_OK) return false;
    if (!empty) {
        if (write_fetch_object(p, f, 0) != MOQ_OK) return false;
        if (moq_session_end_fetch(p->sv, f, p->now) != MOQ_OK) return false;
    }
    if (p->sv->fetch_occ_head >= 0) return false;   /* ended natively */
    if (data_only)
        fake_block_stream(&p->sep, p->sep.next_uni_id);   /* the stream it opens */
    else
        p->sep.block_write = true;
    held_bridge_service(p->sb, p->now);
    for (size_t i = 0; i < p->sep.count; i++)
        if (p->sep.ops[i].kind == FAKE_OP_OPEN_UNI) p->data_sid = p->sep.ops[i].stream_id;
    tally(p);
    fake_endpoint_clear_ops(&p->sep);
    return p->data_sid != 0;
}

/* Where FETCH_OK travels on the control stream (draft 16), a refused
 * control write holds back every stream's work, the response stream's open
 * included: the bridge orders connection-wide backlog ahead of stream work.
 * There only the response stream refuses its writes, which gives it the
 * same custody: opened, nothing of it accepted. */
static bool fetch_open_unconsumed(pair_t *p, bool empty)
{
    return fetch_open_unconsumed_ex(p, empty, !moq_session_uses_request_streams(p->sv));
}

typedef enum {
    END_HELD, END_QUEUED, END_EMPTY, END_UNCONSUMED, END_DATA_BLOCKED
} end_case_t;

/* Custody OPEN after the FETCH ended natively: its FIN held by the bridge
 * after the endpoint refused it, still queued in the session, or never
 * written because the FETCH ended before its header was handed over (an
 * empty FETCH, or one object then end_fetch). On drafts 18 and 22 the
 * request stream's FIN is unaccepted too: it is held, queued, or behind a
 * refused FETCH_OK. The client's return traffic is held. Two STOPs: the
 * callbacks freeze both send halves and call no endpoint operation; both
 * readiness queries report the bridge's own reset work before any service;
 * the endpoint refuses the resets once and they stay retained; after
 * relief exactly one reset of the response stream with the first STOP code
 * and, on drafts 18 and 22, one reset of the request stream with CANCELLED
 * reach the endpoint, with no further data and neither FIN, and nothing
 * stays pending. END_DATA_BLOCKED: only the response stream refuses
 * writes, so nothing stops the request stream's FIN but the ordering: the
 * FETCH's final message waits for the response stream, the request half is
 * still open at the STOP, and both are reset. */
static void test_end_before_stop(moq_version_t v, end_case_t how)
{
    pair_t p;
    if (!pair_open(&p, v, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    const bool paired = v != MOQ_VERSION_DRAFT_16;
    moq_fetch_t f = {0};
    if (how == END_EMPTY || how == END_UNCONSUMED) {
        MOQ_TEST_CHECK(fetch_open_unconsumed(&p, how == END_EMPTY));
    } else {
        MOQ_TEST_CHECK(fetch_open(&p, &f));
        if (how == END_HELD) p.sep.block_write = true;
        if (how == END_DATA_BLOCKED) fake_block_stream(&p.sep, p.data_sid);
        MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, f, p.now) == MOQ_OK);
        if (how == END_HELD || how == END_DATA_BLOCKED) serve(&p);
    }
    const unsigned writes0 = p.data_writes, req_fins0 = p.req_fins;
    const bridge_stream_entry_t *de = data_entry(&p);
    MOQ_TEST_CHECK(de != NULL);
    if (de) {
        MOQ_TEST_CHECK(!bridge_session_is_outgoing_fetch(p.sv, de->ref));
        MOQ_TEST_CHECK(de->fetch_end == BRIDGE_FETCH_END_OPEN);
    }
    MOQ_TEST_CHECK(p.data_fins == 0);
    MOQ_TEST_CHECK(p.data_resets == 0);
    if (v != MOQ_VERSION_DRAFT_16) {
        const bridge_stream_entry_t *re = bridge_find_by_id_const(p.sb, p.req_sid);
        MOQ_TEST_CHECK(re != NULL);
        if (re && de) MOQ_TEST_CHECK(de->fetch_req_ref._v == re->ref._v);
        if (re) MOQ_TEST_CHECK(!re->local_send_closed);
        MOQ_TEST_CHECK(p.req_fins == 0);
    }

    const size_t ops0 = p.sep.count;
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2B, p.now);
    MOQ_TEST_CHECK(p.sep.count == ops0);
    de = data_entry(&p);
    MOQ_TEST_CHECK(de != NULL);
    if (de) {
        MOQ_TEST_CHECK(de->peer_stop_received);
        MOQ_TEST_CHECK(de->fetch_reset_owed);
        MOQ_TEST_CHECK(de->pending_stop_code == 0x2A);
    }
    if (v != MOQ_VERSION_DRAFT_16) {
        const bridge_stream_entry_t *re = bridge_find_by_id_const(p.sb, p.req_sid);
        MOQ_TEST_CHECK(re != NULL);
        if (re) {
            MOQ_TEST_CHECK(re->fetch_req_stopped == paired);
            MOQ_TEST_CHECK(!re->local_cancel_queued);
        }
    }
    /* The bridge alone owes reset work now: both queries say so before any
     * service runs. */
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p.sb));
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.sb));

    /* Send capacity refuses the resets once: retained, nothing sent. */
    p.sep.block_write = false;
    fake_unblock_stream(&p.sep);
    p.sep.block_reset = true;
    serve(&p);
    MOQ_TEST_CHECK(p.data_resets == 0 && p.req_resets == 0);
    MOQ_TEST_CHECK(reset_retained(&p, p.data_sid));
    if (paired) MOQ_TEST_CHECK(reset_retained(&p, p.req_sid));
    MOQ_TEST_CHECK(moq_transport_bridge_has_outbound_pending(p.sb));

    p.sep.block_reset = false;
    serve(&p);
    MOQ_TEST_CHECK(p.data_fins == 0);
    MOQ_TEST_CHECK(p.data_writes == writes0);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == 0x2A);
    if (v != MOQ_VERSION_DRAFT_16) {
        MOQ_TEST_CHECK(p.req_fins == req_fins0);
        MOQ_TEST_CHECK(p.req_resets == (paired ? 1u : 0u));
        if (paired) MOQ_TEST_CHECK(p.req_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    }
    MOQ_TEST_CHECK(p.other_resets == 0);
    MOQ_TEST_CHECK(data_entry(&p) == NULL);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.sb));
    MOQ_TEST_CHECK(!moq_transport_bridge_has_pending(p.sb));
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

static void test_end_before_stop_held(moq_version_t v) { test_end_before_stop(v, END_HELD); }
static void test_end_before_stop_queued(moq_version_t v) { test_end_before_stop(v, END_QUEUED); }
static void test_end_before_stop_empty(moq_version_t v) { test_end_before_stop(v, END_EMPTY); }
static void test_end_before_stop_unconsumed(moq_version_t v)
{
    test_end_before_stop(v, END_UNCONSUMED);
}
static void test_end_before_stop_data_blocked(moq_version_t v)
{
    test_end_before_stop(v, END_DATA_BLOCKED);
}

/* Custody FIN_ACCEPTED, then the explicit all-ACKed completion. The
 * endpoint reports commits, so the mapping waits for the report after the
 * FIN is accepted. A STOP then is the provider's to answer: the bridge
 * resets nothing and owes nothing. The commit report ends the record; a
 * later STOP finds nothing. */
static void test_fin_accepted_stop(moq_version_t v)
{
    pair_t p;
    if (!pair_open(&p, v, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    p.sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_COMMIT_REPORT;
    moq_fetch_t f = {0};
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, f, p.now) == MOQ_OK);
    serve(&p);
    MOQ_TEST_CHECK(p.data_fins == 1);
    const bridge_stream_entry_t *de = data_entry(&p);
    MOQ_TEST_CHECK(de != NULL);
    if (de) {
        MOQ_TEST_CHECK(de->fetch_end == BRIDGE_FETCH_END_FIN_ACCEPTED);
        MOQ_TEST_CHECK(de->awaiting_commit);
    }
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    serve(&p);
    MOQ_TEST_CHECK(p.data_resets == 0);
    MOQ_TEST_CHECK(p.data_fins == 1);
    de = data_entry(&p);
    MOQ_TEST_CHECK(de != NULL);
    if (de) MOQ_TEST_CHECK(!de->fetch_reset_owed);
    MOQ_TEST_CHECK(moq_transport_bridge_on_local_stream_committed(p.sb, p.data_sid, p.now) == MOQ_OK);
    MOQ_TEST_CHECK(data_entry(&p) == NULL);
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2B, p.now);
    serve(&p);
    MOQ_TEST_CHECK(p.data_resets == 0);
    MOQ_TEST_CHECK(p.other_resets == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

/* Custody RESET from an earlier cause: the FETCH was aborted with 0x11
 * before the STOPs arrived, its reset still queued in the session or
 * already refused by the endpoint. The abort's reset is the one answer:
 * one reset per stream, with 0x11, never the STOP's code and never a
 * second one. */
static void test_earlier_reset_stop(moq_version_t v, bool refused)
{
    pair_t p;
    if (!pair_open(&p, v, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f = {0};
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    MOQ_TEST_CHECK(moq_session_abort_fetch(p.sv, f, 0x11, p.now) == MOQ_OK);
    if (refused) {
        p.sep.block_reset = true;
        serve(&p);
        MOQ_TEST_CHECK(p.data_resets == 0);
        MOQ_TEST_CHECK(reset_retained(&p, p.data_sid));
    }
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2B, p.now);
    p.sep.block_reset = false;
    serve(&p);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == 0x11);
    if (v != MOQ_VERSION_DRAFT_16) {
        MOQ_TEST_CHECK(p.req_resets == 1);
        MOQ_TEST_CHECK(p.req_reset_code == 0x11);
    }
    MOQ_TEST_CHECK(p.other_resets == 0);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

static void test_earlier_reset_stop_queued(moq_version_t v) { test_earlier_reset_stop(v, false); }
static void test_earlier_reset_stop_refused(moq_version_t v) { test_earlier_reset_stop(v, true); }

/* The STOP arrives while the FETCH is live and its teardown is refused
 * (the server's event queue is full); the application then ends the FETCH,
 * which queues the request stream's FIN and frees the FETCH. The paired
 * obligation the STOP created survives that end: exactly one reset of the
 * response stream with the first code and, on drafts 18 and 22, one reset
 * of the request stream with CANCELLED in place of its FIN, on exactly
 * those streams. The client's return traffic is held throughout. */
static void test_stop_refused_then_end(moq_version_t v)
{
    pair_t p;
    if (!pair_open(&p, v, 1)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f = {0};
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    MOQ_TEST_CHECK(fill_server_events(&p));
    const unsigned writes0 = p.data_writes, req_fins0 = p.req_fins;
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2B, p.now);
    const bridge_stream_entry_t *de = data_entry(&p);
    MOQ_TEST_CHECK(de != NULL);
    if (de) {
        MOQ_TEST_CHECK(bridge_session_is_outgoing_fetch(p.sv, de->ref));
        MOQ_TEST_CHECK(de->pending_stop && de->pending_stop_code == 0x2A);
    }
    if (v != MOQ_VERSION_DRAFT_16) {
        const bridge_stream_entry_t *re = bridge_find_by_id_const(p.sb, p.req_sid);
        MOQ_TEST_CHECK(re != NULL);
        if (re && de) {
            MOQ_TEST_CHECK(re->fetch_req_stopped);
            MOQ_TEST_CHECK(de->fetch_req_ref._v == re->ref._v);
        }
    }

    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, f, p.now) == MOQ_OK);
    serve(&p);
    MOQ_TEST_CHECK(p.data_fins == 0);
    MOQ_TEST_CHECK(p.data_writes == writes0);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == 0x2A);
    if (v != MOQ_VERSION_DRAFT_16) {
        MOQ_TEST_CHECK(p.req_fins == req_fins0);
        MOQ_TEST_CHECK(p.req_resets == 1);
        MOQ_TEST_CHECK(p.req_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    }
    MOQ_TEST_CHECK(p.other_resets == 0);

    /* Relief changes nothing more: the FETCH ended, so no cancellation. */
    (void)drain_count(p.sv, MOQ_EVENT_SUBSCRIBE_REQUEST, NULL);
    serve(&p);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 0);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.req_resets == (v != MOQ_VERSION_DRAFT_16 ? 1u : 0u));
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

/* A token alias the client never registered: an update carrying it is
 * refused per request. */
#define UNKNOWN_ALIAS 99u

/* The client's REQUEST_UPDATE of its FETCH, delivered on the request stream
 * straight to the server's bridge: its one parameter, an AUTHORIZATION_TOKEN
 * using an unregistered alias, makes the server refuse it. request_id is an
 * unused client id (arrival out of order is allowed). */
static moq_result_t deliver_refused_update(pair_t *p, moq_version_t v,
                                           uint64_t request_id)
{
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    if (v == MOQ_VERSION_DRAFT_22) {
        moq_d22_message_t m;
        memset(&m, 0, sizeof(m));
        m.type = MOQ_D22_REQUEST_UPDATE;
        m.request_id = request_id;
        moq_d22_param_t prm;
        memset(&prm, 0, sizeof(prm));
        prm.type = 0x03;
        prm.value.token.alias_type = 2;
        prm.value.token.alias = UNKNOWN_ALIAS;
        moq_d22_message_context_t ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.request_type = MOQ_D22_FETCH;
        ctx.update_from_subscriber = true;
        moq_result_t rc = moq_d22_encode_message(&w, &m, &prm, 1, &ctx);
        if (rc < 0) return rc;
    } else {
        moq_d18_msg_params_t prm;
        memset(&prm, 0, sizeof(prm));
        prm.auth_token_count = 1;
        prm.auth_tokens[0].alias_type = 2;
        prm.auth_tokens[0].alias = UNKNOWN_ALIAS;
        moq_result_t rc = moq_d18_encode_request_update(&w, request_id, &prm);
        if (rc < 0) return rc;
    }
    return moq_transport_bridge_on_peer_bidi_bytes(p->sb, p->req_sid, buf,
                                                   moq_buf_writer_offset(&w), false,
                                                   p->now);
}

/* Drafts 18 and 22. The data STOP arrives while the FETCH is live and its
 * teardown is refused (the server's event queue is full): both send halves
 * freeze, and while the FETCH lives that is no send-side work. The
 * application then drains its events and, before any service retries the
 * stored STOP, the client's REQUEST_UPDATE is refused: that different
 * native terminal commits first, with its own data reset (CANCELLED), a
 * REQUEST_ERROR with FIN on the request stream, its FETCH_CANCELLED and a
 * NORMAL drain, but no request reset. The response stream keeps that reset
 * and its code; the request half, whose error and FIN are obsolete, still
 * owes its reset and gets exactly one, with CANCELLED. One terminal event in
 * all. The client's return traffic is held until then; its later reset of
 * the request stream releases the drain and the mapping. */
static void test_update_refused_after_stop(moq_version_t v)
{
    pair_t p;
    if (!pair_open(&p, v, 1)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f = {0};
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    MOQ_TEST_CHECK(fill_server_events(&p));
    const unsigned req_fins0 = p.req_fins;
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    const bridge_stream_entry_t *de = data_entry(&p);
    const bridge_stream_entry_t *re = bridge_find_by_id_const(p.sb, p.req_sid);
    MOQ_TEST_CHECK(de != NULL);
    MOQ_TEST_CHECK(re != NULL);
    if (!de || !re) { pair_close(&p); return; }
    const moq_stream_ref_t data_ref = de->ref, req_ref = re->ref;
    MOQ_TEST_CHECK(bridge_session_is_outgoing_fetch(p.sv, data_ref));
    MOQ_TEST_CHECK(de->pending_stop && de->pending_stop_code == 0x2A);
    MOQ_TEST_CHECK(de->fetch_req_ref._v == req_ref._v);
    MOQ_TEST_CHECK(re->fetch_req_stopped);
    MOQ_TEST_CHECK(p.sep.count == 0);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.sb));
    MOQ_TEST_CHECK(moq_transport_bridge_has_pending(p.sb));

    (void)drain_count(p.sv, MOQ_EVENT_SUBSCRIBE_REQUEST, NULL);
    MOQ_TEST_CHECK(!event_queue_full(p.sv));
    MOQ_TEST_CHECK(deliver_refused_update(&p, v, 10) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(!bridge_session_is_outgoing_fetch(p.sv, data_ref));
    MOQ_TEST_CHECK(!bridge_session_fetch_request_live(p.sv, req_ref));
    MOQ_TEST_CHECK(bridge_session_has_queued_data_reset(p.sv, data_ref));
    MOQ_TEST_CHECK(!bridge_session_has_queued_request_reset(p.sv, req_ref));
    MOQ_TEST_CHECK(drain_ref_contains(p.sv, req_ref));
    MOQ_TEST_CHECK(p.sep.count == 0);

    serve(&p);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    MOQ_TEST_CHECK(p.req_resets == 1);
    MOQ_TEST_CHECK(p.req_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    MOQ_TEST_CHECK(p.req_fins == req_fins0);
    MOQ_TEST_CHECK(p.other_resets == 0);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 1);
    MOQ_TEST_CHECK(data_entry(&p) == NULL);
    MOQ_TEST_CHECK(!moq_transport_bridge_has_outbound_pending(p.sb));

    MOQ_TEST_CHECK(held_bridge_reset(p.sb, p.req_sid, MOQ_STREAM_ERROR_CANCELLED,
                                     p.now) == MOQ_OK);
    serve(&p);
    MOQ_TEST_CHECK(bridge_find_by_id_const(p.sb, p.req_sid) == NULL);
    MOQ_TEST_CHECK(!drain_ref_contains(p.sv, req_ref));
    MOQ_TEST_CHECK(p.req_resets == 1);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

/* Drafts 18 and 22. The FETCH's request FIN is its final message: it is
 * handed to the endpoint only after the response stream has its FIN
 * accepted, never while that stream can still be stopped. Here the response
 * stream refuses writes after its object: end_fetch queues both FINs, the
 * request FIN waits (the request half stays open, nothing reset), and once
 * the response stream's FIN is accepted the ordinary request FIN follows
 * it, with no reset of either stream. */
static void test_req_fin_after_data_fin(moq_version_t v)
{
    pair_t p;
    if (!pair_open(&p, v, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f = {0};
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    fake_block_stream(&p.sep, p.data_sid);
    MOQ_TEST_CHECK(moq_session_end_fetch(p.sv, f, p.now) == MOQ_OK);
    serve(&p);
    MOQ_TEST_CHECK(p.data_fins == 0);
    MOQ_TEST_CHECK(p.req_fins == 0);
    const bridge_stream_entry_t *re = bridge_find_by_id_const(p.sb, p.req_sid);
    MOQ_TEST_CHECK(re != NULL);
    if (re) MOQ_TEST_CHECK(!re->local_send_closed);

    fake_unblock_stream(&p.sep);
    serve(&p);
    MOQ_TEST_CHECK(p.data_fins == 1);
    MOQ_TEST_CHECK(p.req_fins == 1);
    MOQ_TEST_CHECK(p.data_fin_seq != 0 && p.data_fin_seq < p.req_fin_seq);
    MOQ_TEST_CHECK(p.data_resets == 0 && p.req_resets == 0);
    MOQ_TEST_CHECK(p.other_resets == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

/* Drafts 18 and 22. An empty FETCH accepted while its response stream
 * refuses writes: FETCH_OK still goes out at once, without the FIN; the
 * request FIN, queued after the header, waits until the header and its FIN
 * are accepted, then follows. */
static void test_req_fin_after_empty_header(moq_version_t v)
{
    pair_t p;
    if (!pair_open(&p, v, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    MOQ_TEST_CHECK(fetch_open_unconsumed_ex(&p, true, true));
    serve(&p);
    MOQ_TEST_CHECK(p.data_writes == 0);
    MOQ_TEST_CHECK(p.req_writes >= 1);
    MOQ_TEST_CHECK(p.req_fins == 0);
    const bridge_stream_entry_t *re = bridge_find_by_id_const(p.sb, p.req_sid);
    MOQ_TEST_CHECK(re != NULL);
    if (re) MOQ_TEST_CHECK(!re->local_send_closed);

    fake_unblock_stream(&p.sep);
    serve(&p);
    MOQ_TEST_CHECK(p.data_fins == 1);
    MOQ_TEST_CHECK(p.req_fins == 1);
    MOQ_TEST_CHECK(p.data_fin_seq != 0 && p.data_fin_seq < p.req_fin_seq);
    MOQ_TEST_CHECK(p.data_resets == 0 && p.req_resets == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

/* Drafts 18 and 22. A refused REQUEST_UPDATE ends the FETCH with a reset of
 * its response stream and REQUEST_ERROR with FIN on the request stream,
 * the FETCH's final message. While the endpoint refuses the reset that
 * final message waits (nothing written on the request stream); once the
 * reset is accepted it follows, and no request reset is sent. */
static void test_req_fin_after_data_reset(moq_version_t v)
{
    pair_t p;
    if (!pair_open(&p, v, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f = {0};
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    const unsigned req_writes0 = p.req_writes;
    p.sep.block_reset = true;
    MOQ_TEST_CHECK(deliver_refused_update(&p, v, 10) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    serve(&p);
    MOQ_TEST_CHECK(p.data_resets == 0);
    MOQ_TEST_CHECK(reset_retained(&p, p.data_sid));
    MOQ_TEST_CHECK(p.req_writes == req_writes0);
    MOQ_TEST_CHECK(p.req_fins == 0);

    p.sep.block_reset = false;
    serve(&p);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    MOQ_TEST_CHECK(p.req_fins == 1);
    MOQ_TEST_CHECK(p.data_reset_seq != 0 && p.data_reset_seq < p.req_fin_seq);
    MOQ_TEST_CHECK(p.req_resets == 0);
    MOQ_TEST_CHECK(p.other_resets == 0);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 1);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

/* Drafts 18 and 22. The live teardown commits at the STOP: the request
 * stream's reset with CANCELLED goes to the endpoint (or is refused and
 * retained), and the NORMAL drain keeps the request mapping. With the
 * client's traffic held, that prerequisite is asserted first; then the
 * client's STOP of the request stream reuses the reset already owed or
 * sent: one request reset in all, with CANCELLED, and no second event. */
static void test_companion_reuse(moq_version_t v, bool refused)
{
    pair_t p;
    if (!pair_open(&p, v, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f = {0};
    MOQ_TEST_CHECK(fetch_open(&p, &f));
    if (refused) p.sep.block_reset = true;
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    serve(&p);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 1);
    const bridge_stream_entry_t *re = bridge_find_by_id_const(p.sb, p.req_sid);
    MOQ_TEST_CHECK(re != NULL);
    if (re) MOQ_TEST_CHECK(drain_ref_contains(p.sv, re->ref));
    if (refused) {
        MOQ_TEST_CHECK(p.req_resets == 0);
        MOQ_TEST_CHECK(reset_retained(&p, p.req_sid));
    } else {
        MOQ_TEST_CHECK(p.req_resets == 1);
        MOQ_TEST_CHECK(p.req_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    }

    moq_transport_bridge_on_peer_stop_sending(p.sb, p.req_sid, 0x5, p.now);
    serve(&p);
    p.sep.block_reset = false;
    serve(&p);
    MOQ_TEST_CHECK(p.req_resets == 1);
    MOQ_TEST_CHECK(p.req_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == 0x2A);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

static void test_companion_reuse_sent(moq_version_t v) { test_companion_reuse(v, false); }
static void test_companion_reuse_refused(moq_version_t v) { test_companion_reuse(v, true); }

/* The server's view of the client's request FIN: unseen; delivered with
 * the FETCH request itself (handed over to the FETCH, whose handoff drive
 * latches it); delivered alone later; or handed over with its drive not yet
 * run, the handoff marker alone (set directly: the drive runs in the same
 * call, so no input sequence leaves the marker standing). */
typedef enum {
    PEER_FIN_NONE, PEER_FIN_HANDED, PEER_FIN_LATER, PEER_FIN_MARKER
} peer_fin_t;

static bool fetch_open_fin(pair_t *p, peer_fin_t fin, moq_fetch_t *f)
{
    p->req_fin_on_open = fin == PEER_FIN_HANDED;
    bool ok = fetch_open(p, f);
    p->req_fin_on_open = false;
    if (ok && fin == PEER_FIN_LATER) {
        ok = moq_transport_bridge_on_peer_bidi_bytes(p->sb, p->req_sid, NULL, 0, true,
                                                     p->now) == MOQ_OK;
        serve(p);
    }
    const bridge_stream_entry_t *de = data_entry(p);
    if (ok && fin == PEER_FIN_MARKER && de) {
        for (size_t i = 0; i < p->sv->fetch_cap; i++)
            if (p->sv->fetches[i].state == MOQ_FETCH_ACCEPTED &&
                p->sv->fetches[i].data_stream_ref._v == de->ref._v)
                p->sv->fetches[i].handoff_fin_pending = true;
    }
    return ok;
}

/* The peer FIN fact the reservation reads, as the row expects it. */
static void check_peer_fin(const pair_t *p, peer_fin_t fin)
{
    const bridge_stream_entry_t *de = data_entry(p);
    const moq_fetch_entry_t *fe = de ? server_fetch(p, de->ref) : NULL;
    MOQ_TEST_CHECK(fe != NULL);
    if (!fe) return;
    if (fin == PEER_FIN_NONE) MOQ_TEST_CHECK(!fetch_peer_fin_observed(fe));
    if (fin == PEER_FIN_HANDED || fin == PEER_FIN_LATER) MOQ_TEST_CHECK(fe->rs.recv_fin);
    if (fin == PEER_FIN_MARKER)
        MOQ_TEST_CHECK(fe->handoff_fin_pending && !fe->rs.recv_fin);
}

typedef enum { PRESSURE_ACTIONS, PRESSURE_EVENTS } pressure_t;

/* The server's action queue under action pressure: small, so the objects
 * that fill it, written at relief, stay well inside the endpoint's
 * operation record. */
#define PRESSURE_ACTION_SLOTS 16u

/* The live teardown's reservation is all or nothing: one event, and two
 * actions on drafts 18 and 22 (one on draft 16) whatever the peer's
 * request FIN. With one action slot short, or the event queue full, the
 * STOP is refused, the FETCH stays live and nothing is queued or reaches
 * the endpoint; after relief the stored STOP commits once and both streams
 * are reset, the request with CANCELLED, even when the request FIN was
 * seen. */
static void test_live_pressure(moq_version_t v, pressure_t kind, peer_fin_t fin)
{
    pair_t p;
    if (!pair_open_ex(&p, v, kind == PRESSURE_EVENTS ? 1 : 0,
                      kind == PRESSURE_ACTIONS ? PRESSURE_ACTION_SLOTS : 0)) {
        MOQ_TEST_CHECK(false);
        pair_close(&p);
        return;
    }
    moq_fetch_t f = {0}, other = {0};
    if (kind == PRESSURE_ACTIONS) MOQ_TEST_CHECK(fetch_other(&p, &other));
    MOQ_TEST_CHECK(fetch_open_fin(&p, fin, &f));
    check_peer_fin(&p, fin);
    const size_t need = v == MOQ_VERSION_DRAFT_16 ? 1u : 2u;
    if (kind == PRESSURE_ACTIONS) {
        uint64_t id = 0;
        for (int guard = 0; guard < 128 && action_queue_avail(p.sv) > need - 1; guard++)
            MOQ_TEST_CHECK(write_fetch_object(&p, other, id++) == MOQ_OK);
        MOQ_TEST_CHECK(action_queue_avail(p.sv) == need - 1);
    } else {
        MOQ_TEST_CHECK(fill_server_events(&p));
    }
    const size_t queued = p.sv->action_tail - p.sv->action_head;
    const size_t ops0 = p.sep.count;
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    const bridge_stream_entry_t *de = data_entry(&p);
    MOQ_TEST_CHECK(de != NULL);
    if (de) {
        MOQ_TEST_CHECK(bridge_session_is_outgoing_fetch(p.sv, de->ref));
        MOQ_TEST_CHECK(de->pending_stop && de->pending_stop_code == 0x2A);
    }
    MOQ_TEST_CHECK(p.sv->action_tail - p.sv->action_head == queued);
    MOQ_TEST_CHECK(p.sep.count == ops0);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 0);

    if (kind == PRESSURE_EVENTS)
        (void)drain_count(p.sv, MOQ_EVENT_SUBSCRIBE_REQUEST, NULL);
    serve(&p);
    de = data_entry(&p);
    MOQ_TEST_CHECK(de == NULL || !bridge_session_is_outgoing_fetch(p.sv, de->ref));
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, &ev) == 1);
    MOQ_TEST_CHECK(ev.u.fetch_cancelled.cause == MOQ_REQUEST_TERMINAL_PEER_STOP_DATA);
    MOQ_TEST_CHECK(ev.u.fetch_cancelled.stream_error_code == 0x2A);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == 0x2A);
    if (v != MOQ_VERSION_DRAFT_16) {
        MOQ_TEST_CHECK(p.req_resets == 1);
        MOQ_TEST_CHECK(p.req_reset_code == MOQ_STREAM_ERROR_CANCELLED);
        MOQ_TEST_CHECK(p.req_fins == 0);
    }
    MOQ_TEST_CHECK(p.other_resets == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

static void test_action_pressure(moq_version_t v)
{
    test_live_pressure(v, PRESSURE_ACTIONS, PEER_FIN_NONE);
}
static void test_action_pressure_fin_handed(moq_version_t v)
{
    test_live_pressure(v, PRESSURE_ACTIONS, PEER_FIN_HANDED);
}
static void test_action_pressure_fin_later(moq_version_t v)
{
    test_live_pressure(v, PRESSURE_ACTIONS, PEER_FIN_LATER);
}
static void test_event_pressure_fin_handed(moq_version_t v)
{
    test_live_pressure(v, PRESSURE_EVENTS, PEER_FIN_HANDED);
}
static void test_event_pressure_fin_later(moq_version_t v)
{
    test_live_pressure(v, PRESSURE_EVENTS, PEER_FIN_LATER);
}

/* Drafts 18 and 22, drain ring full. The drain is reserved only while the
 * request FIN is unseen: with it seen (handed over with the request, later
 * on its own, or as the handoff marker alone), the STOP commits at once,
 * takes no drain, and both streams are reset. */
static void test_drain_full_fin_seen(moq_version_t v, peer_fin_t fin)
{
    pair_t p;
    if (!pair_open(&p, v, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    moq_fetch_t f = {0};
    MOQ_TEST_CHECK(fetch_open_fin(&p, fin, &f));
    check_peer_fin(&p, fin);
    const size_t saved = p.sv->drain_ref_count;
    p.sv->drain_ref_count = p.sv->drain_ref_cap;
    moq_transport_bridge_on_peer_stop_sending(p.sb, p.data_sid, 0x2A, p.now);
    const bridge_stream_entry_t *de = data_entry(&p);
    MOQ_TEST_CHECK(de != NULL);
    if (de) {
        MOQ_TEST_CHECK(!bridge_session_is_outgoing_fetch(p.sv, de->ref));
        MOQ_TEST_CHECK(!de->pending_stop);
    }
    MOQ_TEST_CHECK(p.sv->drain_ref_count == p.sv->drain_ref_cap);
    p.sv->drain_ref_count = saved;
    serve(&p);
    MOQ_TEST_CHECK(drain_count(p.sv, MOQ_EVENT_FETCH_CANCELLED, NULL) == 1);
    MOQ_TEST_CHECK(p.data_resets == 1);
    MOQ_TEST_CHECK(p.data_reset_code == 0x2A);
    MOQ_TEST_CHECK(p.req_resets == 1);
    MOQ_TEST_CHECK(p.req_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    MOQ_TEST_CHECK(p.req_fins == 0);
    MOQ_TEST_CHECK(p.other_resets == 0);
    MOQ_TEST_CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
    pair_close(&p);
}

static void test_drain_full_fin_handed(moq_version_t v)
{
    test_drain_full_fin_seen(v, PEER_FIN_HANDED);
}
static void test_drain_full_fin_later(moq_version_t v)
{
    test_drain_full_fin_seen(v, PEER_FIN_LATER);
}
static void test_drain_full_fin_marker(moq_version_t v)
{
    test_drain_full_fin_seen(v, PEER_FIN_MARKER);
}
static void test_action_pressure_fin_marker(moq_version_t v)
{
    test_live_pressure(v, PRESSURE_ACTIONS, PEER_FIN_MARKER);
}

/* The bridge's peer-opened refs: issued from MOQ_STREAM_REF_LOCAL_END up,
 * and refused (fatal, ref 0) once UINT64_MAX was issued, never wrapping to
 * a ref already used. Synthetic: the counter is set, no huge stream id is
 * sent through a provider. */
static void test_peer_ref_wrap(void)
{
    pair_t p;
    if (!pair_open(&p, MOQ_VERSION_DRAFT_18, 0)) { MOQ_TEST_CHECK(false); pair_close(&p); return; }
    MOQ_TEST_CHECK(p.sb->next_inbound_ref >= MOQ_STREAM_REF_LOCAL_END);
    p.sb->next_inbound_ref = UINT64_MAX;
    const uint8_t padding[] = { 0x00 };
    (void)held_bridge_uni_bytes(p.sb, CLIENT_UNI + 900u, padding, 0, false, p.now);
    (void)held_bridge_uni_bytes(p.sb, CLIENT_UNI + 900u, padding, 1, false, p.now);
    MOQ_TEST_CHECK(p.sb->next_inbound_ref == 0);   /* UINT64_MAX was issued */
    (void)held_bridge_uni_bytes(p.sb, CLIENT_UNI + 901u, padding, 1, false, p.now);
    MOQ_TEST_CHECK(p.sb->next_inbound_ref == 0);   /* nothing wrapped */
    MOQ_TEST_CHECK(bridge_find_by_id_const(p.sb, CLIENT_UNI + 901u) == NULL);
    pair_close(&p);
}

/* MOQ_HOST_ROW=<row function>:<draft> runs that one row alone, in that
 * draft (a focused run of one case); unset, every row runs. */
static const char *only_row;
static bool row_matched;
static bool row_wanted(const char *fn, unsigned draft)
{
    if (!only_row) return true;
    char want[80];
    (void)snprintf(want, sizeof(want), "%s:%u", fn, draft);
    if (strcmp(want, only_row) != 0) return false;
    row_matched = true;
    return true;
}

int main(void)
{
    static const moq_version_t all[] = {
        MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22,
    };
    only_row = getenv("MOQ_HOST_ROW");
    /* Each row in each draft it applies to: the rows without d16 need a
     * request stream (its reset, drain or FIN). */
    static const struct {
        const char *name;
        void (*fn)(moq_version_t);
        bool d16;
    } rows[] = {
        { "test_freeze_and_retry", test_freeze_and_retry, true },
        { "test_self_reset_after_end", test_self_reset_after_end, true },
        { "test_end_before_stop_held", test_end_before_stop_held, true },
        { "test_end_before_stop_queued", test_end_before_stop_queued, true },
        { "test_end_before_stop_empty", test_end_before_stop_empty, true },
        { "test_end_before_stop_unconsumed", test_end_before_stop_unconsumed, true },
        { "test_fin_accepted_stop", test_fin_accepted_stop, true },
        { "test_earlier_reset_stop_queued", test_earlier_reset_stop_queued, true },
        { "test_earlier_reset_stop_refused", test_earlier_reset_stop_refused, true },
        { "test_stop_refused_then_end", test_stop_refused_then_end, true },
        { "test_action_pressure", test_action_pressure, true },
        { "test_end_before_stop_data_blocked", test_end_before_stop_data_blocked, false },
        { "test_req_fin_after_data_fin", test_req_fin_after_data_fin, false },
        { "test_req_fin_after_empty_header", test_req_fin_after_empty_header, false },
        { "test_req_fin_after_data_reset", test_req_fin_after_data_reset, false },
        { "test_update_refused_after_stop", test_update_refused_after_stop, false },
        { "test_companion_reuse_sent", test_companion_reuse_sent, false },
        { "test_companion_reuse_refused", test_companion_reuse_refused, false },
        { "test_action_pressure_fin_handed", test_action_pressure_fin_handed, false },
        { "test_action_pressure_fin_later", test_action_pressure_fin_later, false },
        { "test_event_pressure_fin_handed", test_event_pressure_fin_handed, false },
        { "test_event_pressure_fin_later", test_event_pressure_fin_later, false },
        { "test_drain_full_fin_handed", test_drain_full_fin_handed, false },
        { "test_drain_full_fin_later", test_drain_full_fin_later, false },
        { "test_drain_full_fin_marker", test_drain_full_fin_marker, false },
        { "test_action_pressure_fin_marker", test_action_pressure_fin_marker, false },
    };
    for (size_t i = 0; i < sizeof(all) / sizeof(all[0]); i++)
        for (size_t r = 0; r < sizeof(rows) / sizeof(rows[0]); r++)
            if ((rows[r].d16 || all[i] != MOQ_VERSION_DRAFT_16) &&
                row_wanted(rows[r].name, (unsigned)all[i]))
                rows[r].fn(all[i]);
    if (row_wanted("test_peer_ref_wrap", 18u)) test_peer_ref_wrap();
    if (only_row && !row_matched) {
        fprintf(stderr, "SETUP[selector] MOQ_HOST_ROW=%s names no row\n", only_row);
        return 2;
    }
    if (failures) {
        fprintf(stderr, "test_bridge_fetch_stop: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_bridge_fetch_stop: all passed\n");
    return 0;
}
