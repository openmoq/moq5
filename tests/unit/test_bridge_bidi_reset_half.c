/*
 * A peer RESET of a request bidi ends the peer's sending direction. When the
 * session accepts it and the request has no owner left, our own send half,
 * still open, is cancelled by us: one RESET_STREAM with CANCELLED (0x1),
 * issued from the service pass, never from the endpoint's callback.
 *
 * Real request bidis (a subscription's), in draft-18 and draft-22, seen from
 * both ends: the client that opened the stream and the server it was opened
 * to. Pinned:
 *   - the RESET reaches the session once with the peer's exact code; nothing
 *     is sent inside the callback; the service pass resets our half once with
 *     CANCELLED and the mapping retires; a later STOP or RESET of that id
 *     adds no reset, no event and no entry;
 *   - STOP then RESET: the STOP's reset answers both;
 *   - our half already finished (FIN): the RESET retires the mapping and
 *     nothing is reset;
 *   - a RESET the session cannot take yet keeps its first code, whether it is
 *     repeated while the queue is full or after room returns but before the
 *     service pass: the request's outcome carries the first code, once;
 *   - our cancellation that WOULD_BLOCKs stays owed, absorbs a STOP meanwhile,
 *     and is sent once;
 *   - a FIN the session already queued, or one the bridge holds while the
 *     endpoint refuses writes, goes out in its place and no reset is added;
 *   - connection teardown before the service pass fabricates no reset;
 *   - a cancellation needs a retention slot: with one slot, two ended
 *     requests are both cancelled, once each, without a fatal close; behind
 *     unrelated retained work they wait, mapped, and that work is kept and
 *     goes out first;
 *   - a FIN still queued at the session when a pass stops at the drain
 *     budget is never overtaken by a cancellation.
 * Allocations balance in every case.
 */

static int failures = 0;
#include "../support/bridge_wire_pair.h"
#include "test_binding_support.h"
#include "../../core/src/bridge/transport_bridge_internal.h"

static const char *g_case = "";
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL %s:%d: %s\n", g_case, __LINE__, #x); failures++; } } while (0)

/* Server-side bounds for the pressure rows; zero keeps the defaults. */
static uint32_t g_srv_max_pending, g_srv_max_actions;

static bool open_pair(rl_pair_t *p, moq_version_t ver, uint32_t client_max_events,
                      uint32_t server_max_events)
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
    cc.version = sc.version = ver;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    if (client_max_events) cc.max_events = client_max_events;
    if (server_max_events) sc.max_events = server_max_events;
    if (g_srv_max_actions) sc.max_actions = g_srv_max_actions;
    if (moq_session_create(&cc, p->now, &p->c) < 0) return false;
    if (moq_session_create(&sc, p->now, &p->s) < 0) return false;
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &p->calloc);
    moq_transport_bridge_cfg_t sbc;
    moq_transport_bridge_cfg_init(&sbc, &p->salloc);
    if (g_srv_max_pending) sbc.max_pending = g_srv_max_pending;
    if (moq_transport_bridge_create(&bc, p->c, &p->cep.vtable, &p->cep, &p->cb) < 0) return false;
    if (moq_transport_bridge_create(&sbc, p->s, &p->sep.vtable, &p->sep, &p->sb) < 0) return false;
    if (test_bind_if_required(p->c) != MOQ_OK || test_bind_if_required(p->s) != MOQ_OK) return false;
    if (moq_session_start(p->c, p->now) < 0) return false;
    if (moq_session_uses_uni_control(p->s) && moq_session_start(p->s, p->now) < 0) return false;
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

/* Client subscribes, server accepts; returns the request bidi's id. Leaves
 * the client's SUBSCRIBE_OK queued when keep_ok is set. */
static uint64_t subscribe(rl_pair_t *p, moq_subscription_t *ssub, bool keep_ok)
{
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ nsp, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("t");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t csub;
    if (moq_session_subscribe(p->c, &sc, p->now, &csub) != MOQ_OK) return 0;
    shuttle(p);
    moq_event_t ev;
    *ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(p->s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) *ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(*ssub)) return 0;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(p->s, *ssub, &ac, p->now) != MOQ_OK) return 0;
    shuttle(p);
    if (!keep_ok) {
        obs_t ob;
        memset(&ob, 0, sizeof(ob));
        drain(p->c, &ob);
    }
    for (uint32_t i = 0; i < p->cb->max_streams; i++) {
        bridge_stream_entry_t *e = &p->cb->streams[i];
        if (e->active && e->kind == BRIDGE_STREAM_BIDI && e->origin == BRIDGE_ORIGIN_LOCAL)
            return e->transport_id;
    }
    return 0;
}

/* Service one side and count what its endpoint did to `sid` (the endpoint's
 * operations are consumed here, never delivered). */
typedef struct side {
    moq_session_t *s;
    moq_transport_bridge_t *b;
    fake_endpoint_t *ep;
    int resets, fins, events, writes_after_reset, unsubscribed;
    uint64_t last_reset_code, unsub_code;
} side_t;

static void service(rl_pair_t *p, side_t *x, uint64_t sid)
{
    p->now += 10;
    moq_transport_bridge_service(x->b, p->now);
    for (size_t i = 0; i < x->ep->count; i++) {
        const fake_op_t *o = &x->ep->ops[i];
        if (o->stream_id != sid) continue;
        if (o->kind == FAKE_OP_RESET || o->kind == FAKE_OP_RESET_AT) {
            x->resets++;
            x->last_reset_code = o->error_code;
        }
        if (o->kind == FAKE_OP_WRITE && x->resets) x->writes_after_reset++;
        if (o->kind == FAKE_OP_WRITE && o->fin) x->fins++;
    }
    fake_endpoint_clear_ops(x->ep);
}

static void count_events(side_t *x)
{
    moq_event_t ev;
    while (moq_session_poll_events(x->s, &ev, 1) > 0) {
        x->events++;
        if (ev.kind == MOQ_EVENT_UNSUBSCRIBED) {
            x->unsubscribed++;
            x->unsub_code = ev.u.unsubscribed.stream_error_code;
        }
        moq_event_cleanup(&ev);
    }
}

enum { RESET_STOP, STOP_RESET, CLOSED_RESET_STOP, PRESSURE, PRESSURE_ROOM, OUT_BLOCK,
       DUPLICATES, ORDERED, FIN_BLOCKED, TEARDOWN, CASE_COUNT };
static const char *const names[] = { "reset-then-stop", "stop-then-reset",
    "closed-reset-then-stop", "reset-pressure", "reset-repeat-after-room", "cancel-would-block",
    "duplicates", "fin-queued-then-reset", "fin-blocked-then-reset", "teardown" };

static void run(moq_version_t v, bool server_side, int which)
{
    char label[96];
    snprintf(label, sizeof(label), "d%u/%s/%s", (unsigned)v,
             server_side ? "server(peer-opened)" : "client(opened)", names[which]);
    g_case = label;
    int before = failures;
    bool skipped = false;
    rl_pair_t *p = calloc(1, sizeof(*p));
    if (!p) { failures++; return; }
    bool pressure = which == PRESSURE || which == PRESSURE_ROOM;
    if (!open_pair(p, v, pressure && !server_side ? 1 : 0, pressure && server_side ? 1 : 0)) {
        CHECK(!"pair open");
        goto out;
    }
    moq_subscription_t ssub;
    uint64_t sid = subscribe(p, &ssub, pressure && !server_side);
    CHECK(sid != 0);
    if (!sid) goto out;
    side_t x;
    memset(&x, 0, sizeof(x));
    x.s = server_side ? p->s : p->c;
    x.b = server_side ? p->sb : p->cb;
    x.ep = server_side ? &p->sep : &p->cep;
    if (pressure && server_side) {
        /* A second request leaves its SUBSCRIBE_REQUEST queued: the server's
         * one-event queue is full. */
        moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
        moq_subscribe_cfg_t sc2;
        moq_subscribe_cfg_init(&sc2);
        sc2.track_namespace = (moq_namespace_t){ nsp, 1 };
        sc2.track_name = MOQ_BYTES_LITERAL("t2");
        sc2.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t csub2;
        CHECK(moq_session_subscribe(p->c, &sc2, p->now, &csub2) == MOQ_OK);
        shuttle(p);
    }
    if (!pressure) count_events(&x);
    size_t base = moq_transport_bridge_stream_count(x.b);
    bridge_stream_entry_t *e = bridge_find_by_id(x.b, sid);
    CHECK(e != NULL && e->kind == BRIDGE_STREAM_BIDI && !e->local_send_closed);
    if (!e) goto out;

    if (which == CLOSED_RESET_STOP) {
        if (!server_side) { skipped = true; goto out; }   /* the server ends its half */
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        CHECK(moq_session_done_subscribe(p->s, ssub, &dc, p->now) == MOQ_OK);
        service(p, &x, sid);
        e = bridge_find_by_id(x.b, sid);
        CHECK(x.fins == 1 && e != NULL && e->local_send_closed);
        CHECK(moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0x5, p->now) == MOQ_OK);
        service(p, &x, sid);
        CHECK(bridge_find_by_id(x.b, sid) == NULL);
        CHECK(moq_transport_bridge_on_peer_stop_sending(x.b, sid, 0x7, p->now) == MOQ_OK);
        service(p, &x, sid);
        CHECK(x.resets == 0);                  /* a finished half is not reset */
        goto out;
    }
    if (which == STOP_RESET) {
        CHECK(moq_transport_bridge_on_peer_stop_sending(x.b, sid, 0x7, p->now) == MOQ_OK);
        service(p, &x, sid);
        CHECK(x.resets == 1 && x.last_reset_code == 0x7);
        int ev_after_stop = (count_events(&x), x.events);
        CHECK(moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0x5, p->now) == MOQ_OK);
        service(p, &x, sid);
        count_events(&x);
        CHECK(x.resets == 1);
        CHECK(bridge_find_by_id(x.b, sid) == NULL);
        CHECK(moq_transport_bridge_stream_count(x.b) == base - 1);
        (void)ev_after_stop;
        goto out;
    }

    if (which == FIN_BLOCKED) {
        if (!server_side) { skipped = true; goto out; }   /* the server ends its half */
        /* PUBLISH_DONE and our FIN are held in the bridge (the endpoint
         * refuses writes) when the peer's RESET is accepted: that output
         * is our half's own close, and no cancellation is added after it. */
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        CHECK(moq_session_done_subscribe(p->s, ssub, &dc, p->now) == MOQ_OK);
        x.ep->block_write = true;
        service(p, &x, sid);
        CHECK(x.fins == 0 && moq_transport_bridge_has_pending(x.b));
        CHECK(moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0x5, p->now) == MOQ_OK);
        service(p, &x, sid);
        CHECK(x.resets == 0);
        x.ep->block_write = false;
        service(p, &x, sid);
        service(p, &x, sid);
        CHECK(x.fins == 1 && x.resets == 0);
        CHECK(bridge_find_by_id(x.b, sid) == NULL);
        CHECK(moq_transport_bridge_stream_count(x.b) == base - 1);
        CHECK(!moq_transport_bridge_has_pending(x.b));
        goto out;
    }
    if (which == ORDERED) {
        if (!server_side) { skipped = true; goto out; }   /* the server ends its half */
        /* PUBLISH_DONE and our FIN are queued at the session, not yet sent,
         * when the peer's RESET arrives: the FIN keeps its place and is never
         * replaced by a reset. */
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        CHECK(moq_session_done_subscribe(p->s, ssub, &dc, p->now) == MOQ_OK);
        CHECK(moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0x5, p->now) == MOQ_OK);
        CHECK(x.ep->count == 0);
        service(p, &x, sid);
        service(p, &x, sid);
        CHECK(x.fins == 1 && x.resets == 0);
        CHECK(bridge_find_by_id(x.b, sid) == NULL);
        CHECK(moq_transport_bridge_on_peer_stop_sending(x.b, sid, 0x7, p->now) == MOQ_OK);
        service(p, &x, sid);
        CHECK(x.resets == 0);
        CHECK(moq_transport_bridge_stream_count(x.b) == base - 1);
        CHECK(!moq_transport_bridge_has_pending(x.b));
        goto out;
    }

    /* The peer RESETs its half. */
    if (which == OUT_BLOCK) x.ep->block_reset = true;
    moq_result_t rrc = moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0x5, p->now);
    CHECK(x.ep->count == 0);                   /* nothing inside the callback */
    if (pressure) {
        CHECK(rrc == MOQ_ERR_WOULD_BLOCK);
        e = bridge_find_by_id(x.b, sid);
        CHECK(e != NULL && e->pending_reset && e->pending_reset_code == 0x5);
        /* repeated while the queue is full: the first code is kept */
        CHECK(moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0x9, p->now) ==
              MOQ_ERR_WOULD_BLOCK);
        e = bridge_find_by_id(x.b, sid);
        CHECK(e != NULL && e->pending_reset_code == 0x5);
        count_events(&x);                      /* room for the reset's event */
        CHECK(x.unsubscribed == 0);
        if (which == PRESSURE_ROOM) {
            /* repeated after room returns, before any service pass */
            CHECK(moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0xA, p->now) == MOQ_OK);
            count_events(&x);
            CHECK(x.unsubscribed == 1 && x.unsub_code == 0x5);
            e = bridge_find_by_id(x.b, sid);
            CHECK(e == NULL || !e->pending_reset);   /* delivered: nothing owed */
        }
        service(p, &x, sid);
        count_events(&x);
    } else {
        CHECK(rrc == MOQ_OK);
        count_events(&x);
        e = bridge_find_by_id(x.b, sid);
        /* Accepted: the mapping waits for the service pass to cancel our half. */
        CHECK(e != NULL && e->active && e->peer_send_closed && !e->local_send_closed);
        if (which == TEARDOWN) {
            CHECK(moq_transport_bridge_on_transport_close(x.b, 0, p->now) == MOQ_OK);
            service(p, &x, sid);
            CHECK(x.resets == 0);              /* teardown fabricates no reset */
            goto out;
        }
        if (which == DUPLICATES) {
            int ev0 = x.events;
            CHECK(moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0x6, p->now) == MOQ_OK);
            count_events(&x);
            CHECK(x.events == ev0 && x.ep->count == 0);
        }
        service(p, &x, sid);
        count_events(&x);
    }
    /* The request's outcome carries the peer's first code, once. */
    CHECK(x.unsubscribed == 1 && x.unsub_code == 0x5);
    int events_after_reset = x.events;
    CHECK(x.fins == 0);
    if (which == OUT_BLOCK) {
        CHECK(x.resets == 0);                  /* blocked: owed, mapping kept */
        e = bridge_find_by_id(x.b, sid);
        CHECK(e != NULL && e->active);
        CHECK(moq_transport_bridge_on_peer_stop_sending(x.b, sid, 0x7, p->now) == MOQ_OK);
        service(p, &x, sid);
        CHECK(x.resets == 0);
        x.ep->block_reset = false;
        service(p, &x, sid);
    }
    /* Our open half is cancelled, once, with CANCELLED. */
    CHECK(x.resets == 1 && x.last_reset_code == MOQ_STREAM_ERROR_CANCELLED);
    CHECK(x.writes_after_reset == 0);
    CHECK(bridge_find_by_id(x.b, sid) == NULL);
    CHECK(moq_transport_bridge_stream_count(x.b) == base - 1);

    /* Late STOP and RESET for that id: nothing more, nothing allocated. */
    CHECK(moq_transport_bridge_on_peer_stop_sending(x.b, sid, 0x8, p->now) == MOQ_OK);
    CHECK(moq_transport_bridge_on_peer_stream_reset(x.b, sid, 0xB, p->now) == MOQ_OK);
    service(p, &x, sid);
    service(p, &x, sid);
    count_events(&x);
    CHECK(x.resets == 1);
    CHECK(x.events == events_after_reset);       /* no second cancellation */
    CHECK(bridge_find_by_id(x.b, sid) == NULL);
    CHECK(moq_transport_bridge_stream_count(x.b) == base - 1);
    CHECK(!moq_transport_bridge_is_fatal(x.b));
    CHECK(!moq_transport_bridge_has_pending(x.b));
    CHECK(moq_session_state(x.s) == MOQ_SESS_ESTABLISHED);
out:
    pair_close(p);
    free(p);
    printf("%s %s\n", failures != before ? "FAIL" : skipped ? "SKIP" : "PASS", label);
}

/* -- Pressure rows: the server's own cancellations under bounded retention
 * and the drain budget. ------------------------------------------------- */

/* A further subscription; its request bidi's id (the newest local bidi on
 * the client, which the server sees under the same id). */
static uint64_t subscribe_track_handles(rl_pair_t *p, const char *name,
                                        moq_subscription_t *ssub,
                                        moq_subscription_t *client_sub)
{
    bool seen[4096] = { false };
    for (uint32_t i = 0; i < p->cb->max_streams && i < 4096; i++)
        seen[i] = p->cb->streams[i].active;
    moq_bytes_t nsp[1] = { MOQ_BYTES_LITERAL("live") };
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ nsp, 1 };
    sc.track_name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t csub;
    if (moq_session_subscribe(p->c, &sc, p->now, &csub) != MOQ_OK) return 0;
    if (client_sub) *client_sub = csub;
    shuttle(p);
    moq_event_t ev;
    *ssub = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(p->s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) *ssub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
    }
    if (!moq_subscription_is_valid(*ssub)) return 0;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    if (moq_session_accept_subscribe(p->s, *ssub, &ac, p->now) != MOQ_OK) return 0;
    shuttle(p);
    obs_t ob;
    memset(&ob, 0, sizeof(ob));
    drain(p->c, &ob);
    drain(p->s, &ob);
    for (uint32_t i = 0; i < p->cb->max_streams && i < 4096; i++) {
        const bridge_stream_entry_t *e = &p->cb->streams[i];
        if (e->active && !seen[i] && e->kind == BRIDGE_STREAM_BIDI &&
            e->origin == BRIDGE_ORIGIN_LOCAL)
            return e->transport_id;
    }
    return 0;
}

static uint64_t subscribe_track(rl_pair_t *p, const char *name, moq_subscription_t *ssub)
{
    return subscribe_track_handles(p, name, ssub, NULL);
}

/* Each op on the server's endpoint since the last clear, by stream. */
static int ops_of(const fake_endpoint_t *ep, fake_op_kind_t kind, uint64_t sid,
                  uint64_t code, bool fin_only)
{
    int n = 0;
    for (size_t i = 0; i < ep->count; i++) {
        const fake_op_t *o = &ep->ops[i];
        if (o->kind != kind || o->stream_id != sid) continue;
        if (kind == FAKE_OP_RESET && o->error_code != code) continue;
        if (fin_only && !o->fin) continue;
        n++;
    }
    return n;
}

static int first_op(const fake_endpoint_t *ep, fake_op_kind_t kind, uint64_t sid)
{
    for (size_t i = 0; i < ep->count; i++)
        if (ep->ops[i].kind == kind && ep->ops[i].stream_id == sid) return (int)i;
    return -1;
}

/* A streaming observer between the bridge and the fake endpoint for the
 * budget row: every accepted operation is counted as it happens, so nothing
 * is lost to the fake's bounded recorder. Only counters are kept (no log),
 * so the observer itself cannot overflow, and each op the fake records is
 * taken back out of its recorder once counted, so the fake's recorder cannot
 * overflow either; `writes` is cross-checked against the fake's own
 * monotonic write total. */
typedef struct obs_stream {
    uint64_t sid;
    uint64_t total, writes, resets, stops, closes;
    uint64_t fins, first_fin, first_reset;     /* sequence numbers, 0 = none */
} obs_stream_t;
static obs_stream_t g_obs;

static void obs_note(void *ctx, moq_transport_result_t r)
{
    fake_endpoint_t *ep = (fake_endpoint_t *)ctx;
    if (r != MOQ_TRANSPORT_OK) return;
    g_obs.total++;
    if (ep->count > 0) ep->count--;          /* counted here instead */
}

static moq_transport_result_t obs_write(void *ctx, uint64_t sid, const uint8_t *data,
                                        size_t len, bool fin)
{
    moq_transport_result_t r = fake_write(ctx, sid, data, len, fin);
    obs_note(ctx, r);
    if (r == MOQ_TRANSPORT_OK) {
        g_obs.writes++;
        if (sid == g_obs.sid && fin) {
            g_obs.fins++;
            if (!g_obs.first_fin) g_obs.first_fin = g_obs.total;
        }
    }
    return r;
}

static moq_transport_result_t obs_reset(void *ctx, uint64_t sid, uint64_t code)
{
    moq_transport_result_t r = fake_reset(ctx, sid, code);
    obs_note(ctx, r);
    if (r == MOQ_TRANSPORT_OK && sid == g_obs.sid) {
        g_obs.resets++;
        if (!g_obs.first_reset) g_obs.first_reset = g_obs.total;
    }
    return r;
}

static moq_transport_result_t obs_stop(void *ctx, uint64_t sid, uint64_t code)
{
    moq_transport_result_t r = fake_stop(ctx, sid, code);
    obs_note(ctx, r);
    if (r == MOQ_TRANSPORT_OK && sid == g_obs.sid) g_obs.stops++;
    return r;
}

static moq_transport_result_t obs_close(void *ctx, uint64_t code, const uint8_t *reason,
                                        size_t reason_len)
{
    moq_transport_result_t r = fake_close(ctx, code, reason, reason_len);
    obs_note(ctx, r);
    if (r == MOQ_TRANSPORT_OK) g_obs.closes++;
    return r;
}

enum { P_CAPACITY, P_UNRELATED, P_BUDGET, P_COUNT };
static const char *const pnames[] = { "two-cancels-one-slot",
    "cancels-wait-behind-unrelated-work", "fin-beyond-drain-budget" };

static void run_pressure(moq_version_t v, int which)
{
    char label[96];
    snprintf(label, sizeof(label), "d%u/server(peer-opened)/%s", (unsigned)v, pnames[which]);
    g_case = label;
    int before = failures;
    g_srv_max_pending = which == P_BUDGET ? 0 : 1;
    g_srv_max_actions = which == P_BUDGET ? 1024 : 0;
    rl_pair_t *p = calloc(1, sizeof(*p));
    if (!p) { failures++; return; }
    bool opened = open_pair(p, v, 0, 0);
    g_srv_max_pending = g_srv_max_actions = 0;
    if (!opened) { CHECK(!"pair open"); goto out; }
    moq_subscription_t s1, s2, s3;
    uint64_t a = subscribe_track(p, "a", &s1);
    uint64_t b = subscribe_track(p, "b", &s2);
    uint64_t c = subscribe_track(p, "c", &s3);
    CHECK(a && b && c && a != b && b != c && a != c);
    if (!a || !b || !c) goto out;
    moq_transport_bridge_t *sb = p->sb;
    fake_endpoint_t *ep = &p->sep;
    size_t base = moq_transport_bridge_stream_count(sb);
    fake_endpoint_clear_ops(ep);

    if (which == P_CAPACITY) {
        /* Two requests end with accepted peer RESETs before any service
         * pass; one retention slot. */
        CHECK(moq_transport_bridge_on_peer_stream_reset(sb, a, 0x5, p->now) == MOQ_OK);
        CHECK(moq_transport_bridge_on_peer_stream_reset(sb, b, 0x6, p->now) == MOQ_OK);
        CHECK(ep->count == 0);
        p->now += 10;
        CHECK(moq_transport_bridge_service(sb, p->now) == MOQ_OK);
        CHECK(!moq_transport_bridge_is_fatal(sb) && !moq_transport_bridge_is_closed(sb));
        CHECK(ops_of(ep, FAKE_OP_RESET, a, MOQ_STREAM_ERROR_CANCELLED, false) == 1);
        CHECK(ops_of(ep, FAKE_OP_RESET, b, MOQ_STREAM_ERROR_CANCELLED, false) == 1);
        CHECK(ep->count == 2);
        CHECK(bridge_find_by_id(sb, a) == NULL && bridge_find_by_id(sb, b) == NULL);
        CHECK(bridge_find_by_id(sb, c) != NULL);           /* unrelated owner kept */
        CHECK(moq_transport_bridge_stream_count(sb) == base - 2);
    }
    if (which == P_UNRELATED) {
        /* The third subscription's PUBLISH_DONE is retained (the endpoint
         * refuses writes) and fills the one slot; then two requests end. A
         * refused data object would wait in its own stream's queue, not in
         * the slot. */
        moq_done_subscribe_cfg_t dc3;
        moq_done_subscribe_cfg_init(&dc3);
        CHECK(moq_session_done_subscribe(p->s, s3, &dc3, p->now) == MOQ_OK);
        ep->block_write = true;
        p->now += 10;
        CHECK(moq_transport_bridge_service(sb, p->now) == MOQ_OK);
        CHECK(sb->pending_count == 1);
        bridge_pending_item_t held = sb->pending[0];
        uint64_t held_sid = held.canon_have_sid ? held.canon_sid : held.stream_id;
        fake_endpoint_clear_ops(ep);
        CHECK(moq_transport_bridge_on_peer_stream_reset(sb, a, 0x5, p->now) == MOQ_OK);
        CHECK(moq_transport_bridge_on_peer_stream_reset(sb, b, 0x6, p->now) == MOQ_OK);
        for (int k = 0; k < 2; k++) {
            p->now += 10;
            CHECK(moq_transport_bridge_service(sb, p->now) == MOQ_OK);
        }
        /* Waiting is backpressure: nonfatal, the held work unchanged, both
         * cancellations still owed with their mappings. */
        CHECK(!moq_transport_bridge_is_fatal(sb) && !moq_transport_bridge_is_closed(sb));
        CHECK(ep->count == 0);
        CHECK(sb->pending_count == 1 && sb->pending[0].kind == held.kind &&
              sb->pending[0].stream_ref._v == held.stream_ref._v &&
              sb->pending[0].stream_id == held.stream_id);
        bridge_stream_entry_t *ea = bridge_find_by_id(sb, a), *eb = bridge_find_by_id(sb, b);
        CHECK(ea && eb && !ea->local_cancel_queued && !eb->local_cancel_queued);
        CHECK(moq_transport_bridge_has_pending(sb));
        ep->block_write = false;
        p->now += 10;
        CHECK(moq_transport_bridge_service(sb, p->now) == MOQ_OK);
        /* The held PUBLISH_DONE goes first; then each cancellation, once. */
        int w = first_op(ep, FAKE_OP_WRITE, held_sid);
        int ra = first_op(ep, FAKE_OP_RESET, a), rb = first_op(ep, FAKE_OP_RESET, b);
        CHECK(w >= 0 && ra > w && rb > w);
        CHECK(ops_of(ep, FAKE_OP_RESET, a, MOQ_STREAM_ERROR_CANCELLED, false) == 1);
        CHECK(ops_of(ep, FAKE_OP_RESET, b, MOQ_STREAM_ERROR_CANCELLED, false) == 1);
        CHECK(bridge_find_by_id(sb, a) == NULL && bridge_find_by_id(sb, b) == NULL);
        CHECK(bridge_find_by_id(sb, c) != NULL);
        CHECK(!moq_transport_bridge_is_fatal(sb));
    }
    if (which == P_BUDGET) {
        /* More queued actions than one drain pass takes, then our FIN for
         * request `a`, then the peer's RESET of `a`: the first pass stops at
         * the drain budget with the FIN still queued at the session. No
         * cancellation may overtake that FIN. */
        moq_subgroup_cfg_t gc;
        moq_subgroup_cfg_init(&gc);
        gc.group_id = 0;
        moq_subgroup_handle_t sg;
        CHECK(moq_session_open_subgroup(p->s, s3, &gc, p->now, &sg) == MOQ_OK);
        uint8_t bytes[1] = { 7 };
        moq_rcbuf_t *obj = NULL;
        CHECK(moq_rcbuf_create(moq_alloc_default(), bytes, 1, &obj) == MOQ_OK);
        const uint64_t objects = BRIDGE_DRAIN_BUDGET + 40;
        for (uint64_t k = 0; k < objects; k++)
            CHECK(moq_session_write_object(p->s, sg, k, obj, p->now) == MOQ_OK);
        moq_rcbuf_decref(obj);
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        CHECK(moq_session_done_subscribe(p->s, s1, &dc, p->now) == MOQ_OK);
        CHECK(moq_transport_bridge_on_peer_stream_reset(sb, a, 0x5, p->now) == MOQ_OK);
        /* Observe every operation from here on; the bridge holds the vtable
         * by pointer, so the observer sits in front of the fake. Every op this
         * fake can be asked for is observed. */
        memset(&g_obs, 0, sizeof(g_obs));
        g_obs.sid = a;
        CHECK(ep->vtable.write == fake_write && ep->vtable.reset_stream == fake_reset);
        CHECK(ep->vtable.write_payload == NULL && ep->vtable.abort_stream == NULL &&
              ep->vtable.reset_stream_at == NULL);
        ep->vtable.write = obs_write;
        ep->vtable.reset_stream = obs_reset;
        ep->vtable.stop_sending = obs_stop;
        ep->vtable.close_transport = obs_close;
        fake_endpoint_clear_ops(ep);
        CHECK(!ep->overflowed && !ep->truncated);
        uint64_t w0 = ep->write_calls;
        p->now += 10;
        CHECK(moq_transport_bridge_service(sb, p->now) == MOQ_OK);
        /* First pass, before anything is cleared: nothing on `a`. */
        CHECK(g_obs.fins == 0 && g_obs.resets == 0 && g_obs.stops == 0 && g_obs.closes == 0);
        CHECK(g_obs.writes == ep->write_calls - w0);       /* the observer saw every write */
        /* Stopped at the budget (the subgroup open, its priority key, its
         * objects and PUBLISH_DONE with our FIN were queued): the FIN is
         * still the session's. */
        CHECK(p->s->action_tail - p->s->action_head + BRIDGE_DRAIN_BUDGET == objects + 3);
        bridge_stream_entry_t *ea = bridge_find_by_id(sb, a);
        CHECK(ea != NULL && !ea->local_send_closed && !ea->local_cancel_queued);
        for (int k = 0; k < 2; k++) {
            p->now += 10;
            CHECK(moq_transport_bridge_service(sb, p->now) == MOQ_OK);
        }
        /* Over the whole observed window: our FIN once, never a reset. */
        CHECK(g_obs.fins == 1 && g_obs.first_fin != 0);
        CHECK(g_obs.resets == 0 && g_obs.first_reset == 0 && g_obs.stops == 0);
        CHECK(g_obs.closes == 0);
        CHECK(g_obs.writes == ep->write_calls - w0);
        CHECK(g_obs.total == g_obs.writes);
        CHECK(!ep->overflowed && !ep->truncated);   /* nothing was lost */
        ep->vtable.write = fake_write;
        ep->vtable.reset_stream = fake_reset;
        ep->vtable.stop_sending = fake_stop;
        ep->vtable.close_transport = fake_close;
        CHECK(bridge_find_by_id(sb, a) == NULL);
        CHECK(p->s->action_tail == p->s->action_head);
        CHECK(!moq_transport_bridge_is_fatal(sb));
    }
    CHECK(!moq_transport_bridge_has_pending(sb));
    CHECK(moq_session_state(p->s) == MOQ_SESS_ESTABLISHED);
out:
    pair_close(p);
    free(p);
    printf("%s %s\n", failures != before ? "FAIL" : "PASS", label);
}

/* -- Locally cancelled SUBSCRIBEs: their exact request drain identities must
 * outlive our abort, but not the peer's true terminal. These are actual wire
 * requests; only the post-cancel transport signals are supplied explicitly.
 * Outgoing abort ops are inspected, not delivered, so the server cannot race
 * the particular terminal under test. ----------------------------------- */

enum { LC_FIN, LC_RESET, LC_TERMINAL };

static size_t cancel_drain_copies(const moq_session_t *s, moq_stream_ref_t ref)
{
    size_t n = 0;
    for (size_t i = 0; i < s->drain_ref_count; i++)
        if (s->drain_refs[i] == ref._v) n++;
    return n;
}

static moq_result_t cancel_terminal(rl_pair_t *p, uint64_t sid, int terminal)
{
    if (terminal == LC_FIN)
        return moq_transport_bridge_on_peer_bidi_bytes(p->cb, sid, NULL, 0,
                                                       true, p->now);
    if (terminal == LC_RESET)
        return moq_transport_bridge_on_peer_stream_reset(p->cb, sid, 0x59, p->now);
    return moq_transport_bridge_on_peer_stream_terminal(p->cb, sid, p->now);
}

static struct {
    moq_transport_bridge_t *b;
    moq_session_t *s;
    uint64_t sid;
    moq_stream_ref_t ref;
    int calls;
} g_abort_reentry;

static moq_transport_result_t cancel_reentrant_abort(void *ctx, uint64_t sid,
                                                    uint64_t code)
{
    moq_transport_result_t rc = fake_abort(ctx, sid, code);
    if (rc == MOQ_TRANSPORT_OK && sid == g_abort_reentry.sid &&
        g_abort_reentry.calls++ == 0) {
        bridge_stream_entry_t *e = bridge_find_by_id(g_abort_reentry.b, sid);
        size_t events = g_abort_reentry.s->event_tail;
        CHECK(e != NULL && e->aborting);
        CHECK(moq_transport_bridge_on_peer_stream_terminal(g_abort_reentry.b,
                                                            sid, 1000) == MOQ_OK);
        /* The endpoint's caller still has this frame/entry borrowed. A
         * terminal callback latches only; service settles it afterwards. */
        CHECK(bridge_find_by_id(g_abort_reentry.b, sid) == e);
        CHECK(e != NULL && e->active);
        CHECK(cancel_drain_copies(g_abort_reentry.s, g_abort_reentry.ref) == 1);
        CHECK(g_abort_reentry.s->event_tail == events);
    }
    return rc;
}

static void run_local_cancel(moq_version_t v, int terminal, bool reentrant,
                              bool blocked_abort)
{
    static const char *const terminal_names[] = { "peer-FIN", "peer-RESET", "transport-final" };
    char label[128];
    snprintf(label, sizeof(label), "d%u/local-unsubscribe/%s%s%s", (unsigned)v,
             terminal_names[terminal], reentrant ? "/reentrant-abort" : "",
             blocked_abort ? "/blocked-abort" : "");
    g_case = label;
    int before = failures;
    rl_pair_t *p = calloc(1, sizeof(*p));
    CHECK(p != NULL);
    if (!p) return;
    bool opened = open_pair(p, v, 0, 0);
    CHECK(opened);
    if (!opened) goto out;
    CHECK(!p->deliver_error);
    if (p->deliver_error) goto out;

    moq_subscription_t client[3] = { MOQ_SUBSCRIPTION_INVALID,
        MOQ_SUBSCRIPTION_INVALID, MOQ_SUBSCRIPTION_INVALID };
    moq_subscription_t server[3];
    uint64_t sid[3];
    moq_stream_ref_t ref[3];
    int slot[3];
    const char *const tracks[] = { "cancel-target", "other-drain", "live-owner" };
    for (size_t i = 0; i < 3; i++) {
        sid[i] = subscribe_track_handles(p, tracks[i], &server[i], &client[i]);
        slot[i] = test_sub_resolve_handle(p->c, client[i]);
        CHECK(sid[i] != 0 && moq_subscription_is_valid(server[i]) && slot[i] >= 0);
        if (sid[i] == 0 || !moq_subscription_is_valid(server[i]) || slot[i] < 0) goto out;
        bridge_stream_entry_t *e = bridge_find_by_id(p->cb, sid[i]);
        CHECK(e != NULL && e->kind == BRIDGE_STREAM_BIDI &&
              e->origin == BRIDGE_ORIGIN_LOCAL);
        if (!e) goto out;
        ref[i] = e->ref;
        CHECK(ref[i]._v != 0 && ref[i]._v == p->c->subs[slot[i]].rs.ref._v);
        CHECK(p->c->subs[slot[i]].state == MOQ_SUB_ESTABLISHED);
        CHECK(!p->c->subs[slot[i]].rs.recv_fin);
    }
    CHECK(sid[0] != sid[1] && sid[0] != sid[2] && sid[1] != sid[2]);
    CHECK(p->c->drain_ref_count == 0 && p->c->event_head == p->c->event_tail);
    if (failures != before) goto out;  /* no scenario on partial prerequisites */
    const size_t base = moq_transport_bridge_stream_count(p->cb);
    const uint64_t live_request_id = p->c->subs[slot[2]].request_id;
    const moq_request_lifetime_t live_lifetime = p->c->subs[slot[2]].rs.lifetime;
    fake_endpoint_t *ep = &p->cep;
    fake_endpoint_clear_ops(ep);
    if (reentrant || blocked_abort) fake_endpoint_enable_abort(ep);
    if (reentrant) {
        memset(&g_abort_reentry, 0, sizeof(g_abort_reentry));
        g_abort_reentry.b = p->cb;
        g_abort_reentry.s = p->c;
        g_abort_reentry.sid = sid[0];
        g_abort_reentry.ref = ref[0];
        ep->vtable.abort_stream = cancel_reentrant_abort;
    }
    ep->block_abort = blocked_abort;
    CHECK(moq_session_unsubscribe(p->c, client[0], p->now) == MOQ_OK);
    CHECK(cancel_drain_copies(p->c, ref[0]) == 1 && p->c->drain_ref_count == 1);
    CHECK(moq_session_unsubscribe(p->c, client[1], p->now) == MOQ_OK);
    CHECK(cancel_drain_copies(p->c, ref[1]) == 1 && p->c->drain_ref_count == 2);
    CHECK(test_sub_resolve_handle(p->c, client[0]) < 0);
    CHECK(test_sub_resolve_handle(p->c, client[1]) < 0);
    if (failures != before) goto out;
    p->now += 10;
    CHECK(moq_transport_bridge_service(p->cb, p->now) == MOQ_OK);
    if (blocked_abort) {
        CHECK(ep->count == 0 && moq_transport_bridge_has_pending(p->cb));
        CHECK(cancel_drain_copies(p->c, ref[0]) == 1);
        CHECK(cancel_terminal(p, sid[0], LC_TERMINAL) == MOQ_OK);
        p->now += 10;
        CHECK(moq_transport_bridge_service(p->cb, p->now) == MOQ_OK);
        CHECK(ep->count == 0 && cancel_drain_copies(p->c, ref[0]) == 1);
        CHECK(bridge_find_by_id(p->cb, sid[0]) != NULL);
        ep->block_abort = false;
        p->now += 10;
        CHECK(moq_transport_bridge_service(p->cb, p->now) == MOQ_OK);
    }
    CHECK(!ep->overflowed && !ep->truncated);
    CHECK(ep->count == (reentrant || blocked_abort ? 2u : 4u));
    for (size_t i = 0; i < ep->count; i++) {
        const fake_op_t *op = &ep->ops[i];
        CHECK(op->stream_id == sid[0] || op->stream_id == sid[1]);
        CHECK(op->error_code == MOQ_STREAM_ERROR_CANCELLED && !op->fin);
        CHECK(op->kind == (reentrant || blocked_abort ? FAKE_OP_ABORT : FAKE_OP_RESET) ||
              (!(reentrant || blocked_abort) && op->kind == FAKE_OP_STOP));
    }
    for (size_t i = 0; i < 2; i++) {
        if (reentrant || blocked_abort)
            CHECK(ops_of(ep, FAKE_OP_ABORT, sid[i], 0, false) == 1);
        else {
            CHECK(ops_of(ep, FAKE_OP_RESET, sid[i], MOQ_STREAM_ERROR_CANCELLED, false) == 1);
            CHECK(ops_of(ep, FAKE_OP_STOP, sid[i], 0, false) == 1);
        }
    }
    fake_endpoint_clear_ops(ep);

    if (!reentrant && !blocked_abort) {
        /* Accepting our abort is NOT evidence about the peer half. Neither
         * service, a peer STOP nor late non-FIN bytes may free its identity. */
        CHECK(moq_transport_bridge_stream_count(p->cb) == base);
        CHECK(cancel_drain_copies(p->c, ref[0]) == 1 && p->c->drain_ref_count == 2);
        CHECK(moq_transport_bridge_on_peer_stop_sending(p->cb, sid[0], 0x77, p->now) == MOQ_OK);
        const uint8_t late[] = { 0xff };
        CHECK(moq_transport_bridge_on_peer_bidi_bytes(p->cb, sid[0], late,
                                                      sizeof(late), false, p->now) == MOQ_OK);
        p->now += 10;
        CHECK(moq_transport_bridge_service(p->cb, p->now) == MOQ_OK);
        CHECK(ep->count == 0 && cancel_drain_copies(p->c, ref[0]) == 1);
        CHECK(p->c->drain_ref_count == 2);
        bridge_stream_entry_t *e = bridge_find_by_id(p->cb, sid[0]);
        CHECK(e != NULL && e->aborting);
        uint64_t epoch = p->c->borrow_epoch;
        CHECK(cancel_terminal(p, sid[0], terminal) == MOQ_OK);
        /* Repeat before service, while the true terminal is latched. */
        CHECK(cancel_terminal(p, sid[0], terminal) == MOQ_OK);
        CHECK(ep->count == 0 && p->c->event_head == p->c->event_tail);
        if (terminal == LC_TERMINAL) CHECK(p->c->borrow_epoch == epoch);
    }
    if (reentrant) CHECK(g_abort_reentry.calls == 1);
    for (int k = 0; k < 2; k++) {
        p->now += 10;
        CHECK(moq_transport_bridge_service(p->cb, p->now) == MOQ_OK);
    }
    CHECK(cancel_drain_copies(p->c, ref[0]) == 0);
    CHECK(cancel_drain_copies(p->c, ref[1]) == 1 && p->c->drain_ref_count == 1);
    CHECK(cancel_drain_copies(p->c, ref[2]) == 0);
    CHECK(bridge_find_by_id(p->cb, sid[0]) == NULL);
    CHECK(bridge_find_by_id(p->cb, sid[1]) != NULL);
    CHECK(moq_transport_bridge_stream_count(p->cb) == base - 1);
    CHECK(ep->count == 0 && !ep->overflowed && !ep->truncated);
    CHECK(p->c->event_head == p->c->event_tail);
    CHECK(p->c->action_head == p->c->action_tail);
    CHECK(test_sub_resolve_handle(p->c, client[2]) == slot[2]);
    CHECK(p->c->subs[slot[2]].state == MOQ_SUB_ESTABLISHED);
    CHECK(p->c->subs[slot[2]].request_id == live_request_id);
    CHECK(p->c->subs[slot[2]].rs.ref._v == ref[2]._v);
    CHECK(!p->c->subs[slot[2]].rs.recv_fin);
    CHECK(memcmp(&p->c->subs[slot[2]].rs.lifetime, &live_lifetime,
                 sizeof(live_lifetime)) == 0);

    /* Reports for an already-retired ID create no callback outcome, reset,
     * FIN, action, drain identity or new mapping. FIN is duplicated above,
     * while its original mapping is still latched, not as a new stream. */
    CHECK(moq_transport_bridge_on_peer_stream_terminal(p->cb, sid[0], p->now) == MOQ_OK);
    CHECK(moq_transport_bridge_on_peer_stream_reset(p->cb, sid[0], 0x99, p->now) == MOQ_OK);
    p->now += 10;
    CHECK(moq_transport_bridge_service(p->cb, p->now) == MOQ_OK);
    CHECK(ep->count == 0 && p->c->event_head == p->c->event_tail);
    CHECK(p->c->action_head == p->c->action_tail);
    CHECK(p->c->drain_ref_count == 1 && cancel_drain_copies(p->c, ref[1]) == 1);
    CHECK(bridge_find_by_id(p->cb, sid[0]) == NULL);
    CHECK(bridge_find_by_id(p->cb, sid[1]) != NULL);
    CHECK(bridge_find_by_id(p->cb, sid[2]) != NULL);
    CHECK(!moq_transport_bridge_is_fatal(p->cb) && !moq_transport_bridge_has_pending(p->cb));
    CHECK(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
out:
    pair_close(p);
    free(p);
    printf("%s %s\n", failures != before ? "FAIL" : "PASS", label);
}

int main(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t v = 0; v < 2; v++)
        for (int side = 0; side < 2; side++)
            for (int c = 0; c < CASE_COUNT; c++)
                run(versions[v], side != 0, c);
    for (size_t v = 0; v < 2; v++)
        for (int c = 0; c < P_COUNT; c++)
            run_pressure(versions[v], c);
    for (size_t v = 0; v < 2; v++) {
        for (int terminal = LC_FIN; terminal <= LC_TERMINAL; terminal++)
            run_local_cancel(versions[v], terminal, false, false);
        run_local_cancel(versions[v], LC_TERMINAL, true, false);
        run_local_cancel(versions[v], LC_TERMINAL, false, true);
    }
    if (failures) {
        fprintf(stderr, "test_bridge_bidi_reset_half: %d failure(s)\n", failures);
        return 1;
    }
    printf("PASS: bridge_bidi_reset_half\n");
    return 0;
}
