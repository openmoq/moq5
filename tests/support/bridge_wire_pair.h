#ifndef MOQ_TEST_BRIDGE_WIRE_PAIR_H
#define MOQ_TEST_BRIDGE_WIRE_PAIR_H

/*
 * Two sessions, each behind a real moq_transport_bridge_t over a fake
 * endpoint, joined by an ordered per-direction wire. The wire can hold one
 * stream's operations (QUIC gives no cross-stream ordering), drop them, or
 * take an injected peer RESET/STOP/FIN, and the pair ends every scenario with
 * the same quiescence oracle (check_quiet).
 *
 * Test infrastructure only. The including file defines `static int failures`
 * (MOQ_TEST_CHECK counts into it) before including this header.
 */

#include "../unit/test_session_support.h"
#include "fake_endpoint.h"
#include "held_input.h"
#include <moq/moq.h>
#include <moq/transport_bridge.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#ifdef __GNUC__
#define BWP_UNUSED __attribute__((unused))
#else
#define BWP_UNUSED
#endif

/* -- wire ----------------------------------------------------------------- */

#define WIRE_MAX   512
#define HOLD_MAX   4
#define FINLOG_MAX 64

typedef struct wire {
    fake_op_t q[WIRE_MAX];
    size_t    n;
    uint64_t  hold[HOLD_MAX];    /* stream ids whose operations stay queued */
    size_t    nhold;
    bool      hold_data;         /* hold every non-control uni */
    uint64_t  drop;              /* stream id whose operations are discarded */
} wire_t;

typedef struct finlog {
    uint64_t sid;
    bool     from_client;
} finlog_t;

typedef struct rl_opts {
    uint32_t client_max_events;
    uint32_t server_max_actions;
    uint32_t max_fetches;
    uint32_t max_track_statuses;
    /* Optional last word on each session's cfg before it is created. */
    void   (*tune)(moq_session_cfg_t *cfg, bool client, void *ctx);
    void    *tune_ctx;
} rl_opts_t;

typedef struct rl_pair {
    moq_version_t           ver;
    moq_session_t          *c, *s;
    fake_endpoint_t         cep, sep;
    moq_transport_bridge_t *cb, *sb;
    test_held_input_t       cin, sin;    /* refused uni input awaiting replay */
    test_alloc_state_t      cas, sas;
    moq_alloc_t             calloc, salloc;
    uint64_t                now;
    wire_t                  c2s, s2c;
    uint64_t                client_control_sid;   /* draft-16 control bidi */
    uint64_t                server_control_uni;   /* draft-18 control uni */
    finlog_t                fins[FINLOG_MAX];
    size_t                  nfins;
    bool                    deliver_error;
    int                     unexpected_rc;
    size_t                  base_cstreams, base_sstreams;
} rl_pair_t;

BWP_UNUSED static bool is_bidi_id(uint64_t id)
{
    return (id >= 2000 && id < 3000) || (id >= 4000 && id < 5000);
}

BWP_UNUSED static bool d18(const rl_pair_t *p) { return p->ver == MOQ_VERSION_DRAFT_18; }

BWP_UNUSED static bool wire_holds(const rl_pair_t *p, const wire_t *w, bool from_client,
                       uint64_t sid)
{
    for (size_t i = 0; i < w->nhold; i++)
        if (w->hold[i] == sid) return true;
    if (w->hold_data && !from_client && !is_bidi_id(sid) &&
        sid != p->server_control_uni)
        return true;
    return false;
}

BWP_UNUSED static void wire_capture(wire_t *w, fake_endpoint_t *ep)
{
    MOQ_TEST_CHECK(!ep->overflowed && !ep->truncated);
    for (size_t i = 0; i < ep->count; i++) {
        const fake_op_t *o = &ep->ops[i];
        if (o->kind == FAKE_OP_OPEN_UNI || o->kind == FAKE_OP_OPEN_BIDI) continue;
        MOQ_TEST_CHECK(w->n < WIRE_MAX);
        if (w->n < WIRE_MAX) w->q[w->n++] = *o;
    }
    fake_endpoint_clear_ops(ep);
}

BWP_UNUSED static void note_rc(rl_pair_t *p, moq_result_t rc)
{
    if (rc < 0 && rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_CLOSED) {
        p->deliver_error = true;
        p->unexpected_rc = (int)rc;
    }
}

BWP_UNUSED static void deliver_op(rl_pair_t *p, const fake_op_t *o, bool from_client)
{
    moq_transport_bridge_t *to = from_client ? p->sb : p->cb;
    moq_result_t rc = MOQ_OK;
    switch (o->kind) {
    case FAKE_OP_WRITE:
        if (is_bidi_id(o->stream_id)) {
            if (!d18(p) && from_client && p->client_control_sid == UINT64_MAX)
                p->client_control_sid = o->stream_id;
            bool control = !d18(p) && o->stream_id == p->client_control_sid;
            if (o->fin && !control && p->nfins < FINLOG_MAX)
                p->fins[p->nfins++] = (finlog_t){ o->stream_id, from_client };
            rc = control
                ? moq_transport_bridge_on_peer_control_bytes(
                      to, o->stream_id, o->data, o->data_len, o->fin, p->now)
                : moq_transport_bridge_on_peer_bidi_bytes(
                      to, o->stream_id, o->data, o->data_len, o->fin, p->now);
        } else {
            if (d18(p) && !from_client && p->server_control_uni == UINT64_MAX)
                p->server_control_uni = o->stream_id;
            rc = test_hold_uni(from_client ? &p->sin : &p->cin, to,
                               o->stream_id, o->data, o->data_len, o->fin, p->now);
        }
        break;
    case FAKE_OP_RESET:
        rc = moq_transport_bridge_on_peer_stream_reset(to, o->stream_id,
                                                       o->error_code, p->now);
        break;
    case FAKE_OP_STOP:
        rc = moq_transport_bridge_on_peer_stop_sending(to, o->stream_id,
                                                       o->error_code, p->now);
        break;
    case FAKE_OP_CLOSE:
        rc = moq_transport_bridge_on_transport_close(to, o->error_code, p->now);
        break;
    default:
        break;
    }
    note_rc(p, rc);
}

/* Deliver every queued operation whose stream is not held, in order. A held
 * stream keeps all of its operations queued, so per-stream order survives. */
BWP_UNUSED static size_t wire_flush(rl_pair_t *p, wire_t *w, bool from_client)
{
    size_t delivered = 0, keep = 0;
    for (size_t i = 0; i < w->n; i++) {
        fake_op_t o = w->q[i];
        if (w->drop != 0 && o.stream_id == w->drop) { delivered++; continue; }
        if (wire_holds(p, w, from_client, o.stream_id)) { w->q[keep++] = o; continue; }
        deliver_op(p, &o, from_client);
        delivered++;
    }
    w->n = keep;
    return delivered;
}

BWP_UNUSED static void wire_release(wire_t *w)
{
    w->nhold = 0;
    w->hold_data = false;
}

BWP_UNUSED static size_t shuttle_once(rl_pair_t *p)
{
    p->now += 10;
    moq_transport_bridge_service(p->cb, p->now);
    note_rc(p, test_replay_uni(&p->cin, p->cb, p->now));
    wire_capture(&p->c2s, &p->cep);
    size_t n = wire_flush(p, &p->c2s, true);
    moq_transport_bridge_service(p->sb, p->now);
    note_rc(p, test_replay_uni(&p->sin, p->sb, p->now));
    wire_capture(&p->s2c, &p->sep);
    n += wire_flush(p, &p->s2c, false);
    return n + p->cin.count + p->sin.count;
}

BWP_UNUSED static void shuttle(rl_pair_t *p)
{
    for (int i = 0; i < 64; i++)
        if (shuttle_once(p) == 0) return;
}

BWP_UNUSED static bool fin_seen(const rl_pair_t *p, uint64_t sid, bool from_client)
{
    for (size_t i = 0; i < p->nfins; i++)
        if (p->fins[i].sid == sid && p->fins[i].from_client == from_client)
            return true;
    return false;
}

/* -- events ---------------------------------------------------------------- */

typedef struct obs {
    int fetch_request, fetch_ok, fetch_object, fetch_complete, fetch_error;
    int fetch_reset, fetch_cancelled;
    int ts_request, ts_ok, ts_error;
    int closed;
    uint64_t close_code;
    int setup_complete;
    moq_fetch_t fetch;
    moq_track_status_handle_t ts;
} obs_t;

BWP_UNUSED static void drain_one(moq_session_t *s, obs_t *o, int limit)
{
    moq_event_t ev;
    int n = 0;
    while ((limit <= 0 || n < limit) && moq_session_poll_events(s, &ev, 1) > 0) {
        n++;
        switch (ev.kind) {
        case MOQ_EVENT_FETCH_REQUEST:
            o->fetch_request++; o->fetch = ev.u.fetch_request.fetch; break;
        case MOQ_EVENT_FETCH_OK:        o->fetch_ok++; break;
        case MOQ_EVENT_FETCH_OBJECT:    o->fetch_object++; break;
        case MOQ_EVENT_FETCH_COMPLETE:  o->fetch_complete++; break;
        case MOQ_EVENT_FETCH_ERROR:     o->fetch_error++; break;
        case MOQ_EVENT_FETCH_RESET:     o->fetch_reset++; break;
        case MOQ_EVENT_FETCH_CANCELLED: o->fetch_cancelled++; break;
        case MOQ_EVENT_TRACK_STATUS_REQUEST:
            o->ts_request++; o->ts = ev.u.track_status_request.handle; break;
        case MOQ_EVENT_TRACK_STATUS_OK:    o->ts_ok++; break;
        case MOQ_EVENT_TRACK_STATUS_ERROR: o->ts_error++; break;
        case MOQ_EVENT_SESSION_CLOSED:
            o->closed++; o->close_code = ev.u.closed.code; break;
        case MOQ_EVENT_SETUP_COMPLETE: o->setup_complete++; break;
        default: break;
        }
        moq_event_cleanup(&ev);
    }
}

BWP_UNUSED static void drain(moq_session_t *s, obs_t *o) { drain_one(s, o, 0); }

/* -- pair ------------------------------------------------------------------- */

BWP_UNUSED static bool pair_open(rl_pair_t *p, moq_version_t ver, const rl_opts_t *o)
{
    memset(p, 0, sizeof(*p));
    p->ver = ver;
    p->now = 1000;
    p->client_control_sid = UINT64_MAX;
    p->server_control_uni = UINT64_MAX;
    p->calloc = test_allocator(&p->cas);
    p->salloc = test_allocator(&p->sas);

    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &p->calloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), &p->salloc, MOQ_PERSPECTIVE_SERVER);
    cc.version = ver;
    sc.version = ver;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    if (o && o->client_max_events) cc.max_events = o->client_max_events;
    if (o && o->server_max_actions) sc.max_actions = o->server_max_actions;
    if (o && o->max_fetches) cc.max_fetches = sc.max_fetches = o->max_fetches;
    if (o && o->max_track_statuses)
        cc.max_track_statuses = sc.max_track_statuses = o->max_track_statuses;
    if (o && o->tune) {
        o->tune(&cc, true, o->tune_ctx);
        o->tune(&sc, false, o->tune_ctx);
    }
    if (moq_session_create(&cc, p->now, &p->c) < 0) return false;
    if (moq_session_create(&sc, p->now, &p->s) < 0) return false;

    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, moq_alloc_default());
    if (moq_transport_bridge_create(&bc, p->c, &p->cep.vtable, &p->cep, &p->cb) < 0)
        return false;
    if (moq_transport_bridge_create(&bc, p->s, &p->sep.vtable, &p->sep, &p->sb) < 0)
        return false;
    if (moq_session_start(p->c, p->now) < 0) return false;
    if (ver == MOQ_VERSION_DRAFT_18 && moq_session_start(p->s, p->now) < 0)
        return false;
    shuttle(p);
    obs_t ob;
    memset(&ob, 0, sizeof(ob));
    drain(p->c, &ob);
    drain(p->s, &ob);
    p->base_cstreams = moq_transport_bridge_stream_count(p->cb);
    p->base_sstreams = moq_transport_bridge_stream_count(p->sb);
    return moq_session_state(p->c) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(p->s) == MOQ_SESS_ESTABLISHED;
}

BWP_UNUSED static void pair_close(rl_pair_t *p)
{
    moq_transport_bridge_destroy(p->cb);
    moq_transport_bridge_destroy(p->sb);
    moq_session_destroy(p->c);
    moq_session_destroy(p->s);
    MOQ_TEST_CHECK_EQ_INT(p->cas.balance, 0);
    MOQ_TEST_CHECK_EQ_INT(p->sas.balance, 0);
}

BWP_UNUSED static size_t live_fetches(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state != MOQ_FETCH_FREE) n++;
    return n;
}

BWP_UNUSED static size_t live_track_statuses(const moq_session_t *s)
{
    size_t n = 0;
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state != MOQ_TS_FREE) n++;
    return n;
}

/* The quiescence oracle every row ends with. */
BWP_UNUSED static void check_quiet(rl_pair_t *p, const char *row)
{
    int before = failures;
    MOQ_TEST_CHECK(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(p->s) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p->cb));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_fatal(p->sb));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(p->cb));
    MOQ_TEST_CHECK(!moq_transport_bridge_is_closed(p->sb));
    MOQ_TEST_CHECK(!p->deliver_error);
    MOQ_TEST_CHECK_EQ_SIZE(moq_transport_bridge_stream_count(p->cb), p->base_cstreams);
    MOQ_TEST_CHECK_EQ_SIZE(moq_transport_bridge_stream_count(p->sb), p->base_sstreams);
    MOQ_TEST_CHECK_EQ_SIZE(p->c->drain_ref_count, 0);
    MOQ_TEST_CHECK_EQ_SIZE(p->s->drain_ref_count, 0);
    MOQ_TEST_CHECK_EQ_SIZE(live_fetches(p->c), 0);
    MOQ_TEST_CHECK_EQ_SIZE(live_fetches(p->s), 0);
    MOQ_TEST_CHECK_EQ_SIZE(live_track_statuses(p->c), 0);
    MOQ_TEST_CHECK_EQ_SIZE(live_track_statuses(p->s), 0);
    if (failures != before)
        fprintf(stderr, "  in row %s (draft-%d)\n", row, (int)p->ver);
}

#endif /* MOQ_TEST_BRIDGE_WIRE_PAIR_H */
