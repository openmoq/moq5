/* Admission readiness is consumed at service entry; the paired receive sweep
 * observes capacity released during the pass, including suspended service.
 * No injected actions/events, fabricated owners, or writes to bridge/session
 * state. Private fields observe the entry/return bracket and borrow_epoch
 * counts advancing session calls in otherwise idle intervals. The endpoint
 * observer only reads state: it never re-enters the bridge.
 *
 * Requires moq-core-test-internals and MOQ_SESSION_SWEEP_TESTING. The runner
 * must bound wall time: the sweep budget limits deferred transitions, not
 * iterations of the bridge deadline loop.
 */
static int failures;
#include "../support/bridge_wire_pair.h"
#include "test_binding_support.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include <moq/control_d18.h>
#include <moq/vi64.h>

static const char *row_name;
#define REQUIRE(x) do { if (!(x)) { ++failures; \
    fprintf(stderr, "FAIL %s:%d: %s\n", row_name, __LINE__, #x); \
    return false; } } while (0)

enum { HOLDS = 2 };
static const uint8_t data_type = 0x10;    /* known subgroup, incomplete header */

typedef struct fixture {
    rl_pair_t p;
    moq_subscription_t csub, ssub;
    uint64_t request_sid;
    moq_transport_bridge_t *observed_bridge;
    fake_endpoint_t *observed_endpoint;
    uint64_t observed_sid;
    unsigned observed_fins;
    bool fin_in_service, fin_ready_due, fin_may_wait;
} fixture_t;

static fixture_t *observing;

static moq_transport_result_t observe_write(void *ctx, uint64_t sid,
    const uint8_t *data, size_t len, bool fin)
{
    fixture_t *f = observing;
    if (f && ctx == f->observed_endpoint && sid == f->observed_sid && fin) {
        ++f->observed_fins;
        f->fin_in_service = f->observed_bridge->in_service;
        f->fin_ready_due = f->observed_bridge->ready_due;
        f->fin_may_wait = f->observed_bridge->may_wait;
    }
    return fake_write(ctx, sid, data, len, fin);
}

static const bridge_hold_t *held(const moq_transport_bridge_t *b, uint64_t sid)
{
    for (uint32_t i = 0; i < b->max_holds; ++i)
        if (b->holds[i].state != BRIDGE_HOLD_FREE &&
            b->holds[i].transport_id == sid) return &b->holds[i];
    return NULL;
}

static size_t hold_count(const moq_transport_bridge_t *b)
{
    size_t n = 0;
    for (uint32_t i = 0; i < b->max_holds; ++i)
        if (b->holds[i].state != BRIDGE_HOLD_FREE) ++n;
    return n;
}

static size_t op_count(const fake_endpoint_t *ep, fake_op_kind_t kind,
                       uint64_t sid)
{
    size_t n = 0;
    for (size_t i = 0; i < ep->count; ++i)
        if (ep->ops[i].kind == kind && ep->ops[i].stream_id == sid) ++n;
    return n;
}

static bool healthy(const fixture_t *f)
{
    const rl_pair_t *p = &f->p;
    REQUIRE(!p->deliver_error && p->cin.count == 0 && p->sin.count == 0);
    REQUIRE(!p->cep.overflowed && !p->cep.truncated);
    REQUIRE(!p->sep.overflowed && !p->sep.truncated);
    REQUIRE(!moq_transport_bridge_is_terminal(p->cb));
    REQUIRE(!moq_transport_bridge_is_terminal(p->sb));
    return true;
}

static bool setup(fixture_t *f, moq_version_t ver, uint32_t cmaps,
                   uint32_t smaps, bool cap, uint64_t done_wait,
                   uint64_t idle_timeout)
{
    rl_pair_t *p = &f->p;
    p->ver = ver;
    p->now = 1000;
    p->client_control_sid = p->server_control_uni = UINT64_MAX;
    p->calloc = test_allocator(&p->cas);
    p->salloc = test_allocator(&p->sas);
    moq_session_cfg_t cc, sc;
    moq_session_cfg_init_sized(&cc, sizeof(cc), &p->calloc, MOQ_PERSPECTIVE_CLIENT);
    moq_session_cfg_init_sized(&sc, sizeof(sc), &p->salloc, MOQ_PERSPECTIVE_SERVER);
    cc.version = sc.version = ver;
    cc.send_request_capacity = sc.send_request_capacity = true;
    cc.initial_request_capacity = sc.initial_request_capacity = 64;
    cc.max_data_streams = sc.max_data_streams = 16;
    cc.done_wait_timeout_us = done_wait;
    cc.idle_timeout_us = idle_timeout;
    sc.idle_timeout_us = 0;
    cc.goaway_timeout_us = sc.goaway_timeout_us = 0;
    REQUIRE(moq_session_create(&cc, p->now, &p->c) == MOQ_OK);
    REQUIRE(moq_session_create(&sc, p->now, &p->s) == MOQ_OK);
    REQUIRE(test_bind_if_required(p->c) == MOQ_OK);
    REQUIRE(test_bind_if_required(p->s) == MOQ_OK);
    fake_endpoint_init(&p->cep, 1000, 2000);
    fake_endpoint_init(&p->sep, 3000, 4000);
    p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    if (cap) {
        p->cep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
        p->sep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_BIDI_INPUT;
    }
    p->cep.vtable.write = p->sep.vtable.write = observe_write;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &p->calloc);
    bc.max_streams = cmaps;
    bc.max_pending = HOLDS;
    REQUIRE(moq_transport_bridge_create(&bc, p->c, &p->cep.vtable,
                                        &p->cep, &p->cb) == MOQ_OK);
    moq_transport_bridge_cfg_init(&bc, &p->salloc);
    bc.max_streams = smaps;
    bc.max_pending = HOLDS;
    REQUIRE(moq_transport_bridge_create(&bc, p->s, &p->sep.vtable,
                                        &p->sep, &p->sb) == MOQ_OK);
    REQUIRE(moq_session_start(p->c, p->now) == MOQ_OK);
    REQUIRE(moq_session_start(p->s, p->now) == MOQ_OK);
    shuttle(p);
    REQUIRE(healthy(f));
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(p->s) == MOQ_SESS_ESTABLISHED);
    obs_t o = {0};
    drain(p->c, &o);
    drain(p->s, &o);
    REQUIRE(o.setup_complete == 2 && o.closed == 0);
    REQUIRE(moq_transport_bridge_stream_count(p->cb) == 2);
    REQUIRE(moq_transport_bridge_stream_count(p->sb) == 2);
    REQUIRE(p->c2s.n == 0 && p->s2c.n == 0);
    REQUIRE((p->cb->holds != NULL) == cap);
    fake_endpoint_clear_ops(&p->cep);
    fake_endpoint_clear_ops(&p->sep);
    return true;
}

static bool service(moq_transport_bridge_t *b, uint64_t now, bool budgeted,
                     uint32_t budget, bool suspended)
{
    if (budgeted) {
        moq_bridge_budgeted_result_t out;
        REQUIRE(moq_transport_bridge_service_budgeted(b, now, budget, &out) == MOQ_OK);
        REQUIRE(out.suspended == suspended);
        REQUIRE(out.sweep_spent <= budget);
        REQUIRE(!b->session->budget_active);
    } else {
        REQUIRE(!suspended);
        REQUIRE(moq_transport_bridge_service(b, now) == MOQ_OK);
    }
    REQUIRE(!b->in_service && !b->ready_due);
    REQUIRE(!moq_transport_bridge_is_terminal(b));
    return true;
}

static bool padding(moq_transport_bridge_t *b, uint64_t sid, uint64_t now)
{
    size_t count = moq_transport_bridge_stream_count(b);
    uint8_t type[MOQ_VI64_MAX_LEN];
    size_t len = moq_vi64_encode(MOQ_D18_STREAM_PADDING, type, sizeof(type));
    REQUIRE(len > 0);
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(b, sid, type, len,
                                                  false, now) == MOQ_OK);
    const bridge_stream_entry_t *e = bridge_find_by_id_const(b, sid);
    REQUIRE(e && e->uni_disp == BRIDGE_UNI_DISP_DISCARD);
    REQUIRE(moq_transport_bridge_stream_count(b) == count + 1);
    return true;
}

static bool padding_fin(moq_transport_bridge_t *b, uint64_t sid, uint64_t now)
{
    REQUIRE(bridge_find_by_id_const(b, sid) != NULL);
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(b, sid, NULL, 0,
                                                  true, now) == MOQ_OK);
    REQUIRE(bridge_find_by_id_const(b, sid) == NULL);
    return true;
}

static bool hold_data(moq_transport_bridge_t *b, uint64_t sid, uint64_t now)
{
    REQUIRE(moq_transport_bridge_stream_count(b) == b->max_streams);
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(b, sid, &data_type, 1,
        false, now) == MOQ_ERR_INPUT_NOT_CONSUMED);
    const bridge_hold_t *h = held(b, sid);
    REQUIRE(h && !h->mapped && !h->offered && h->held_payload);
    REQUIRE(h->state == BRIDGE_HOLD_RECORDED && h->first_refusal_us == now);
    REQUIRE(bridge_find_by_id_const(b, sid) == NULL);
    REQUIRE(b->may_wait && !b->ready_due);
    return true;
}

/* Isolate the QUERY producer from the earlier INPUT_NOT_CONSUMED producer:
 * service first clears the earlier hint while the map is still full. */
static bool query_waiter(moq_transport_bridge_t *b, uint64_t sid, uint64_t now)
{
    REQUIRE(service(b, now, false, 0, false));
    REQUIRE(!b->may_wait);
    REQUIRE(moq_transport_bridge_stream_has_pending(b, sid));
    REQUIRE(b->may_wait && !b->ready_due);
    return true;
}

/* One explicit paired sweep, without hiding another service in a helper.
 * The exact refused byte is replayed once, after a fresh pending query. */
static bool replay_waiter(moq_transport_bridge_t *b, uint64_t sid, uint64_t now)
{
    REQUIRE(!moq_transport_bridge_stream_has_pending(b, sid));
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(b, sid, &data_type, 1,
                                                  false, now) == MOQ_OK);
    REQUIRE(held(b, sid) == NULL);
    moq_stream_ref_t ref = moq_transport_bridge_find_ref(b, sid);
    REQUIRE(ref._v != 0 && moq_session_has_transport_stream(b->session, ref));
    return true;
}

static bool row_25a(fixture_t *f, moq_version_t ver, bool budgeted)
{
    REQUIRE(setup(f, ver, 3, 8, true, 0, 0));
    rl_pair_t *p = &f->p;
    moq_transport_bridge_t *b = p->cb;
    REQUIRE(padding(b, 6000, ++p->now));
    REQUIRE(hold_data(b, 7000, ++p->now));
    uint64_t age = held(b, 7000)->first_refusal_us + BRIDGE_HOLD_AGE_US;
    REQUIRE(query_waiter(b, 7000, ++p->now));
    REQUIRE(padding_fin(b, 6000, ++p->now));
    REQUIRE(b->ready_due && b->ready_at == p->now);
    REQUIRE(moq_session_next_deadline_us(p->c) == p->now);
    REQUIRE(b->pending_count == 0 && b->dq_items == 0);
    REQUIRE(!moq_session_has_pending_output(p->c));
    uint64_t epoch = p->c->borrow_epoch;
    REQUIRE(service(b, p->now, budgeted, 0, false));
    REQUIRE(p->c->borrow_epoch == epoch);  /* zero ticks/advancing calls */
    REQUIRE(!b->may_wait);
    REQUIRE(held(b, 7000) && held(b, 7000)->offered);
    REQUIRE(moq_session_next_deadline_us(p->c) == age); /* genuine future age */
    REQUIRE(replay_waiter(b, 7000, ++p->now));
    REQUIRE(hold_count(b) == 0);
    REQUIRE(moq_session_next_deadline_us(p->c) == UINT64_MAX);
    epoch = p->c->borrow_epoch;
    for (unsigned i = 0; i < 2; ++i) {
        REQUIRE(service(b, ++p->now, budgeted, 0, false));
        REQUIRE(p->c->borrow_epoch == epoch);
        REQUIRE(moq_session_next_deadline_us(p->c) == UINT64_MAX);
    }
    return healthy(f);
}

static bool subscribe(fixture_t *f)
{
    rl_pair_t *p = &f->p;
    moq_bytes_t part = MOQ_BYTES_LITERAL("readiness");
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("track");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    f->request_sid = p->cep.next_bidi_id;
    REQUIRE(moq_session_subscribe(p->c, &cfg, p->now, &f->csub) == MOQ_OK);
    shuttle(p);
    moq_event_t ev;
    unsigned requests = 0;
    while (moq_session_poll_events(p->s, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            f->ssub = ev.u.subscribe_request.sub;
            ++requests;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(requests == 1 && moq_subscription_is_valid(f->ssub));
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    REQUIRE(moq_session_accept_subscribe(p->s, f->ssub, &accept, p->now) == MOQ_OK);
    shuttle(p);
    unsigned oks = 0, other = 0;
    while (moq_session_poll_events(p->c, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK &&
            ev.u.subscribe_ok.sub._opaque == f->csub._opaque) ++oks;
        else ++other;
        moq_event_cleanup(&ev);
    }
    REQUIRE(oks == 1 && other == 0);
    fake_endpoint_clear_ops(&p->cep);
    fake_endpoint_clear_ops(&p->sep);
    return healthy(f);
}

static bool open_group(fixture_t *f, moq_subgroup_handle_t *sg, uint64_t *sid)
{
    rl_pair_t *p = &f->p;
    moq_subgroup_cfg_t cfg;
    moq_subgroup_cfg_init(&cfg);
    *sid = p->sep.next_uni_id;
    REQUIRE(moq_session_open_subgroup(p->s, f->ssub, &cfg, p->now, sg) == MOQ_OK);
    REQUIRE(service(p->sb, ++p->now, false, 0, false));
    REQUIRE(op_count(&p->sep, FAKE_OP_OPEN_UNI, *sid) == 1);
    REQUIRE(op_count(&p->sep, FAKE_OP_WRITE, *sid) > 0);
    REQUIRE(bridge_find_by_id_const(p->sb, *sid) != NULL);
    return true;
}

/* FIN acceptance retires a map after initial hold maintenance. The paired
 * sweep must see A ready without another service call. Entry consumes
 * may_wait; the non-reentrant pass has no producer that sets it. The endpoint
 * observer checks the service bracket and absence of readiness rearm. */
static bool row_25b(fixture_t *f, moq_version_t ver, bool budgeted)
{
    REQUIRE(setup(f, ver, 8, 4, true, 0, 0));
    REQUIRE(subscribe(f));
    rl_pair_t *p = &f->p;
    moq_transport_bridge_t *b = p->sb;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    REQUIRE(open_group(f, &sg, &sid));
    REQUIRE(moq_transport_bridge_stream_count(b) == 4);
    REQUIRE(hold_data(b, 7000, ++p->now));
    uint64_t age = held(b, 7000)->first_refusal_us + BRIDGE_HOLD_AGE_US;
    REQUIRE(query_waiter(b, 7000, ++p->now));
    REQUIRE(moq_session_close_subgroup(p->s, sg, p->now) == MOQ_OK);
    REQUIRE(b->pending_count == 0 && !b->done_waiting);
    REQUIRE(b->dq_bytes < b->max_retained_bytes);
    f->observed_bridge = b;
    f->observed_endpoint = &p->sep;
    f->observed_sid = sid;
    observing = f;
    uint64_t epoch = p->s->borrow_epoch;
    REQUIRE(service(b, ++p->now, budgeted, 16, false));
    observing = NULL;
    REQUIRE(p->s->borrow_epoch == epoch);
    REQUIRE(f->observed_fins == 1 && f->fin_in_service);
    REQUIRE(!f->fin_ready_due && !f->fin_may_wait);
    REQUIRE(bridge_find_by_id_const(b, sid) == NULL);
    REQUIRE(!b->may_wait && !b->ready_due);
    REQUIRE(moq_session_next_deadline_us(p->s) == age);
    REQUIRE(replay_waiter(b, 7000, ++p->now));
    REQUIRE(moq_session_next_deadline_us(p->s) == UINT64_MAX);
    return healthy(f);
}

static const moq_sub_entry_t *pending_done(const fixture_t *f)
{
    const moq_session_t *s = f->p.c;
    for (size_t i = 0; i < s->sub_cap; ++i)
        if (s->subs[i].done_pending &&
            s->subs[i].handle._opaque == f->csub._opaque) return &s->subs[i];
    return NULL;
}

/* The peer really opened and FINished one subgroup. Deliver its production
 * PUBLISH_DONE request bytes but delay the data stream and the request FIN.
 * This arms a real Stream Count wait; there is no invented sweep owner. */
static bool defer_done(fixture_t *f)
{
    REQUIRE(subscribe(f));
    rl_pair_t *p = &f->p;
    moq_subgroup_handle_t sg;
    uint64_t sid;
    REQUIRE(open_group(f, &sg, &sid));
    REQUIRE(moq_session_close_subgroup(p->s, sg, p->now) == MOQ_OK);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.stream_count = 1;
    REQUIRE(moq_session_done_subscribe(p->s, f->ssub, &dc, p->now) == MOQ_OK);
    REQUIRE(service(p->sb, ++p->now, false, 0, false));
    unsigned data_fins = 0, request_fins = 0, done_bytes = 0;
    for (size_t i = 0; i < p->sep.count; ++i) {
        const fake_op_t *o = &p->sep.ops[i];
        if (o->kind != FAKE_OP_WRITE) continue;
        if (o->stream_id == sid && o->fin) ++data_fins;
        if (o->stream_id != f->request_sid) continue;
        if (o->fin) ++request_fins;
        if (!o->data_len) continue;
        ++done_bytes;
        REQUIRE(moq_transport_bridge_on_peer_bidi_bytes(p->cb, o->stream_id,
            o->data, o->data_len, false, p->now) == MOQ_OK);
    }
    REQUIRE(data_fins == 1 && request_fins == 1 && done_bytes > 0);
    REQUIRE(service(p->cb, ++p->now, false, 0, false)); /* local request FIN */
    const moq_sub_entry_t *e = pending_done(f);
    REQUIRE(e && !e->done_expired && e->done_stream_count == 1);
    REQUIRE(e->processed_stream_count == 0 && e->done_deadline_us > p->now);
    REQUIRE(p->c->event_head == p->c->event_tail);
    REQUIRE(p->cb->pending_count == 0 && !moq_session_has_pending_output(p->c));
    fake_endpoint_clear_ops(&p->cep);
    fake_endpoint_clear_ops(&p->sep);
    return healthy(f);
}

static bool done_events(fixture_t *f, unsigned expected)
{
    unsigned n = 0, other = 0;
    moq_event_t ev;
    while (moq_session_poll_events(f->p.c, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE &&
            ev.u.subscribe_done.sub._opaque == f->csub._opaque &&
            ev.u.subscribe_done.stream_count == 1) ++n;
        else ++other;
        moq_event_cleanup(&ev);
    }
    REQUIRE(n == expected && other == 0);
    return true;
}

static bool deadline_setup(fixture_t *f, moq_version_t ver, bool suspend,
                            uint64_t *due, uint64_t *age)
{
    REQUIRE(setup(f, ver, 4, 8, true,
        suspend ? 1000 : 2 * BRIDGE_HOLD_AGE_US, 6 * BRIDGE_HOLD_AGE_US));
    REQUIRE(defer_done(f));
    rl_pair_t *p = &f->p;
    *due = pending_done(f)->done_deadline_us;
    /* The request mapping may remain for its withheld peer FIN. Fill only
     * actual free slots, then release the last real padding stream. */
    uint64_t last = UINT64_MAX;
    for (uint64_t sid = 6000; moq_transport_bridge_stream_count(p->cb) < 4; ++sid) {
        REQUIRE(sid < 6004);
        REQUIRE(padding(p->cb, sid, ++p->now));
        last = sid;
    }
    REQUIRE(last != UINT64_MAX);
    REQUIRE(hold_data(p->cb, 7000, ++p->now));
    *age = held(p->cb, 7000)->first_refusal_us + BRIDGE_HOLD_AGE_US;
    REQUIRE(query_waiter(p->cb, 7000, ++p->now));
    REQUIRE(*due > p->now && p->c->idle_deadline_us > *due);
    REQUIRE(suspend ? *age > *due : *age < *due);
    if (!suspend) {
        /* Keep a distinct, younger waiter for the paired sweep. Its complete
         * type is held before session delivery, so this does not advance the
         * older hold or the deferred DONE to their deadlines. */
        p->now = *due - 1;
        REQUIRE(hold_data(p->cb, 7001, p->now));
        REQUIRE(moq_transport_bridge_stream_has_pending(p->cb, 7001));
    }
    /* Padding's discard FIN releases a map without advancing the session,
     * so the due deferred owner is still present at service entry. */
    p->now = *due;
    REQUIRE(padding_fin(p->cb, last, p->now));
    REQUIRE(p->cb->ready_due && p->cb->ready_at == *due);
    REQUIRE(pending_done(f) && !pending_done(f)->done_expired);
    return true;
}

static bool row_25c(fixture_t *f, moq_version_t ver, bool budgeted)
{
    uint64_t due, age;
    REQUIRE(deadline_setup(f, ver, false, &due, &age));
    rl_pair_t *p = &f->p;
    REQUIRE(moq_session_next_deadline_us(p->c) == age);
    uint64_t idle = p->c->idle_deadline_us;
    const bridge_hold_t *younger = held(p->cb, 7001);
    REQUIRE(younger != NULL);
    uint64_t younger_age = younger->first_refusal_us + BRIDGE_HOLD_AGE_US;
    REQUIRE(younger_age > due && younger_age < idle);
    uint64_t epoch = p->c->borrow_epoch;
    REQUIRE(service(p->cb, due, budgeted, 16, false));
    REQUIRE(p->c->borrow_epoch == epoch + 1); /* only Step 4 can advance */
    REQUIRE(op_count(&p->cep, FAKE_OP_STOP, 7000) == 1);
    REQUIRE(op_count(&p->cep, FAKE_OP_STOP, 7001) == 0);
    REQUIRE(held(p->cb, 7000) == NULL);
    REQUIRE(hold_count(p->cb) == 1 && held(p->cb, 7001) && held(p->cb, 7001)->offered);
    REQUIRE(p->c->has_owner_deadline && p->c->owner_deadline_us == younger_age);
    REQUIRE(pending_done(f) == NULL);
    REQUIRE(p->c->idle_deadline_us == idle);
    REQUIRE(moq_session_next_deadline_us(p->c) == younger_age);
    REQUIRE(done_events(f, 1));
    REQUIRE(replay_waiter(p->cb, 7001, due));
    REQUIRE(hold_count(p->cb) == 0 && !p->c->has_owner_deadline);
    REQUIRE(moq_session_next_deadline_us(p->c) == p->c->idle_deadline_us);
    epoch = p->c->borrow_epoch;
    REQUIRE(service(p->cb, due, budgeted, 16, false));
    REQUIRE(p->c->borrow_epoch == epoch);
    REQUIRE(op_count(&p->cep, FAKE_OP_STOP, 7000) == 1);
    REQUIRE(done_events(f, 0));
    return healthy(f);
}

static bool row_25d(fixture_t *f, moq_version_t ver, bool unused)
{
    (void)unused;
    uint64_t due, age;
    REQUIRE(deadline_setup(f, ver, true, &due, &age));
    rl_pair_t *p = &f->p;
    uint64_t epoch = p->c->borrow_epoch;
    uint64_t suspends = session_budget_suspend_count;
    REQUIRE(service(p->cb, due, true, 0, true));
    REQUIRE(p->c->borrow_epoch == epoch + 1);
    REQUIRE(session_budget_suspend_count == suspends + 1);
    REQUIRE(pending_done(f) && !pending_done(f)->done_expired);
    REQUIRE(p->c->has_owner_deadline && p->c->owner_deadline_us == age);
    REQUIRE(moq_session_next_deadline_us(p->c) == due); /* genuine debt survives */
    REQUIRE(held(p->cb, 7000) && held(p->cb, 7000)->offered);
    REQUIRE(!p->cb->may_wait);
    REQUIRE(done_events(f, 0));
    /* The sweep is outside the cleared bracket. Its ordinary ingress may
     * resume the real deferred owner, without inheriting a zero budget. */
    REQUIRE(replay_waiter(p->cb, 7000, p->now));
    REQUIRE(!p->c->budget_active && !p->cb->ready_due);
    REQUIRE(done_events(f, 1));
    epoch = p->c->borrow_epoch;
    REQUIRE(service(p->cb, due, true, 0, false));
    REQUIRE(p->c->borrow_epoch == epoch);
    REQUIRE(hold_count(p->cb) == 0 && !p->c->has_owner_deadline);
    REQUIRE(moq_session_next_deadline_us(p->c) == p->c->idle_deadline_us);
    REQUIRE(done_events(f, 0));
    return healthy(f);
}

static bool row_25e(fixture_t *f, moq_version_t ver, bool unused)
{
    (void)unused;
    REQUIRE(setup(f, ver, 3, 8, true, 0, 0));
    rl_pair_t *p = &f->p;
    moq_transport_bridge_t *b = p->cb;
    REQUIRE(padding(b, 6000, ++p->now));
    REQUIRE(hold_data(b, 7000, ++p->now));
    uint64_t first_age = held(b, 7000)->first_refusal_us + BRIDGE_HOLD_AGE_US;
    REQUIRE(hold_data(b, 7001, ++p->now));
    REQUIRE(moq_session_next_deadline_us(p->c) == first_age);
    REQUIRE(query_waiter(b, 7000, ++p->now));
    REQUIRE(padding_fin(b, 6000, ++p->now));
    uint64_t ready = p->now;
    REQUIRE(b->ready_due && b->ready_at == ready);
    REQUIRE(moq_session_next_deadline_us(p->c) == ready);
    /* A second release cannot postpone the already published due wake. */
    REQUIRE(padding(b, 6001, ++p->now));
    REQUIRE(padding_fin(b, 6001, ++p->now));
    REQUIRE(b->ready_due && b->ready_at == ready);
    REQUIRE(moq_session_next_deadline_us(p->c) == ready);
    /* Held UNI RESET settles the real held identity without provider ops. */
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(b, 7000, 0x9, ++p->now) == MOQ_OK);
    REQUIRE(hold_count(b) == 1 && held(b, 7001) != NULL);
    REQUIRE(b->ready_due && moq_session_next_deadline_us(p->c) == ready);
    REQUIRE(moq_transport_bridge_on_peer_stream_reset(b, 7001, 0x9, ++p->now) == MOQ_OK);
    REQUIRE(hold_count(b) == 0 && b->ready_due);
    REQUIRE(moq_session_next_deadline_us(p->c) == ready);
    uint64_t epoch = p->c->borrow_epoch;
    REQUIRE(service(b, p->now, false, 0, false));
    REQUIRE(p->c->borrow_epoch == epoch);
    REQUIRE(moq_session_next_deadline_us(p->c) == UINT64_MAX);
    REQUIRE(service(b, ++p->now, false, 0, false));
    REQUIRE(p->c->borrow_epoch == epoch && !b->may_wait);
    REQUIRE(p->cep.count == 0);
    return healthy(f);
}

static bool row_cap_off(fixture_t *f, moq_version_t ver, bool unused)
{
    (void)unused;
    REQUIRE(setup(f, ver, 4, 8, false, 0, 0));
    rl_pair_t *p = &f->p;
    REQUIRE(padding(p->cb, 6000, ++p->now));
    REQUIRE(moq_transport_bridge_on_peer_uni_bytes(p->cb, 7000, &data_type,
        1, false, ++p->now) == MOQ_OK);
    moq_stream_ref_t ref = moq_transport_bridge_find_ref(p->cb, 7000);
    REQUIRE(ref._v != 0 && moq_session_has_transport_stream(p->c, ref));
    REQUIRE(!moq_transport_bridge_stream_has_pending(p->cb, 7000));
    REQUIRE(padding_fin(p->cb, 6000, ++p->now));
    REQUIRE(!p->cb->may_wait && !p->cb->ready_due && !p->c->has_owner_deadline);
    uint64_t epoch = p->c->borrow_epoch;
    REQUIRE(service(p->cb, ++p->now, false, 0, false));
    REQUIRE(service(p->cb, ++p->now, true, 0, false));
    REQUIRE(p->c->borrow_epoch == epoch);
    REQUIRE(moq_session_next_deadline_us(p->c) == UINT64_MAX);
    return healthy(f);
}

static bool row_unknown_prefix(fixture_t *f, moq_version_t ver, bool unused)
{
    (void)unused;
    REQUIRE(setup(f, ver, 2, 8, true, 0, 0));
    rl_pair_t *p = &f->p;
    moq_transport_uni_prefix_t prefix = {0};
    /* Eight of a nine-byte vi64; the final byte may still identify control.
     * The adapter owns this state, and neither map nor hold owns this id. */
    const uint8_t incomplete[8] = { 0xff, 0, 0, 0, 0, 0, 0, 0 };
    REQUIRE(moq_transport_bridge_on_peer_uni_prefix_bytes(p->cb, 7000, &prefix,
        incomplete, sizeof(incomplete), false, ++p->now) == MOQ_OK);
    REQUIRE(prefix.len == sizeof(incomplete));
    REQUIRE(memcmp(prefix.bytes, incomplete, sizeof(incomplete)) == 0);
    REQUIRE(hold_count(p->cb) == 0 && moq_transport_bridge_stream_count(p->cb) == 2);
    REQUIRE(!moq_transport_bridge_stream_has_pending(p->cb, 7000));
    REQUIRE(!p->cb->may_wait && !p->cb->ready_due);
    uint64_t epoch = p->c->borrow_epoch;
    p->now += 2 * BRIDGE_HOLD_AGE_US;
    REQUIRE(service(p->cb, p->now, false, 0, false));
    REQUIRE(service(p->cb, p->now, true, 0, false));
    REQUIRE(p->c->borrow_epoch == epoch);
    REQUIRE(moq_session_next_deadline_us(p->c) == UINT64_MAX);
    REQUIRE(prefix.len == sizeof(incomplete));
    REQUIRE(hold_count(p->cb) == 0 && moq_transport_bridge_stream_count(p->cb) == 2);
    REQUIRE(p->cep.count == 0); /* no speculative STOP and no idle traffic */
    return healthy(f);
}

/* Capacity-only pending output is sticky, but an idle deadline must still
 * terminate a quiet peer. No provider refusal is involved: the bridge cannot
 * allocate the request's map, and its write-readiness query correctly says
 * false. This is a separate Step 4 oracle, not a readiness notification. */
static bool row_map_pending_idle(fixture_t *f, moq_version_t ver, bool unused)
{
    (void)unused;
    REQUIRE(setup(f, ver, 3, 8, true, 0, 1000000));
    rl_pair_t *p = &f->p;
    moq_transport_bridge_t *b = p->cb;
    REQUIRE(padding(b, 6000, ++p->now));
    moq_bytes_t part = MOQ_BYTES_LITERAL("readiness");
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("map-blocked");
    moq_track_status_handle_t request;
    REQUIRE(moq_session_track_status(p->c, &cfg, p->now, &request) == MOQ_OK);
    REQUIRE(service(b, ++p->now, false, 0, false));
    REQUIRE(b->pending_count == 1 && b->pending[0].kind == PENDING_OPEN_BIDI_DATA);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(b));
    REQUIRE(!moq_session_has_pending_output(p->c));
    REQUIRE(!b->ready_due && hold_count(b) == 0);
    REQUIRE(p->cep.count == 0 && p->cep.block_count == 0);
    uint64_t due = p->c->idle_deadline_us;
    REQUIRE(due > p->now && moq_session_next_deadline_us(p->c) == due);
    REQUIRE(moq_transport_bridge_service(b, due) == MOQ_OK);
    REQUIRE(!b->in_service && !b->ready_due);
    REQUIRE(!moq_transport_bridge_is_fatal(b));
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_CLOSED);
    REQUIRE(moq_transport_bridge_is_closed(b));
    REQUIRE(moq_transport_bridge_close_code(b) == MOQ_CLOSE_IDLE_TIMEOUT);
    REQUIRE(p->cep.count == 1 && p->cep.ops[0].kind == FAKE_OP_CLOSE);
    REQUIRE(p->cep.ops[0].error_code == MOQ_CLOSE_IDLE_TIMEOUT);
    REQUIRE(!p->cep.overflowed && !p->cep.truncated);
    REQUIRE(moq_session_next_deadline_us(p->c) == UINT64_MAX);
    return true;
}

/* The outgoing preamble may suspend before hold maintenance. An expired
 * FIFO head must remain unoffered at that exit, even with a free map; a
 * younger waiter cannot pass it until ordinary cancellation settles it. */
static bool row_expired_hold_early_suspension(fixture_t *f,
                                             moq_version_t ver, bool unused)
{
    (void)unused;
    REQUIRE(setup(f, ver, 8, 8, true,
                  2 * BRIDGE_HOLD_AGE_US, 6 * BRIDGE_HOLD_AGE_US));
    rl_pair_t *p = &f->p;
    moq_transport_bridge_t *b = p->cb;

    /* The reverse subscription gives the client real outgoing timed data,
     * independent of the incoming deferred DONE established below. */
    moq_bytes_t part = MOQ_BYTES_LITERAL("readiness");
    moq_subscribe_cfg_t sub_cfg;
    moq_subscribe_cfg_init(&sub_cfg);
    sub_cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    sub_cfg.track_name = MOQ_BYTES_LITERAL("outgoing-expiry");
    sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t receiver, publisher = MOQ_SUBSCRIPTION_INVALID;
    REQUIRE(moq_session_subscribe(p->s, &sub_cfg, p->now, &receiver) == MOQ_OK);
    shuttle(p);
    moq_event_t ev;
    unsigned requests = 0, other = 0;
    while (moq_session_poll_events(p->c, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            publisher = ev.u.subscribe_request.sub;
            ++requests;
        } else {
            ++other;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(requests == 1 && other == 0 && moq_subscription_is_valid(publisher));

    uint8_t props[2 * MOQ_VI64_MAX_LEN];
    size_t prop_len = moq_vi64_encode(MOQ_D18_PARAM_OBJECT_DELIVERY_TIMEOUT,
                                     props, sizeof(props));
    REQUIRE(prop_len > 0);
    size_t value_len = moq_vi64_encode(10, props + prop_len, sizeof(props) - prop_len);
    REQUIRE(value_len > 0);
    prop_len += value_len;
    moq_accept_subscribe_cfg_t accept;
    moq_accept_subscribe_cfg_init(&accept);
    accept.track_properties = (moq_bytes_t){ props, prop_len };
    REQUIRE(moq_session_accept_subscribe(p->c, publisher, &accept, p->now) == MOQ_OK);
    shuttle(p);
    unsigned oks = 0;
    other = 0;
    while (moq_session_poll_events(p->s, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK &&
            ev.u.subscribe_ok.sub._opaque == receiver._opaque) ++oks;
        else ++other;
        moq_event_cleanup(&ev);
    }
    REQUIRE(oks == 1 && other == 0);
    REQUIRE(defer_done(f));
    uint64_t due = pending_done(f)->done_deadline_us;

    uint64_t last = UINT64_MAX;
    for (uint64_t sid = 6000; moq_transport_bridge_stream_count(b) < b->max_streams; ++sid) {
        REQUIRE(sid < 6008);
        REQUIRE(padding(b, sid, ++p->now));
        last = sid;
    }
    REQUIRE(last != UINT64_MAX);
    REQUIRE(hold_data(b, 7000, ++p->now));
    uint64_t age = held(b, 7000)->first_refusal_us + BRIDGE_HOLD_AGE_US;
    REQUIRE(query_waiter(b, 7000, ++p->now));
    REQUIRE(p->now < age && age < due && due < p->c->idle_deadline_us);

    /* Queue, but do not service, one object with the negotiated 10 ms
     * deadline. At due this makes the first outgoing preamble advance. */
    moq_subgroup_cfg_t group_cfg;
    moq_subgroup_cfg_init(&group_cfg);
    group_cfg.first_object = true;
    moq_subgroup_handle_t group;
    ++p->now;
    REQUIRE(moq_session_open_subgroup(p->c, publisher, &group_cfg, p->now, &group) == MOQ_OK);
    static const uint8_t body[] = { 0x71, 0x72, 0x73 };
    moq_rcbuf_t *payload = NULL;
    REQUIRE(moq_rcbuf_create(&p->calloc, body, sizeof(body), &payload) == MOQ_OK);
    moq_result_t write_rc = moq_session_write_object(p->c, group, 0, payload, p->now);
    moq_rcbuf_decref(payload);
    REQUIRE(write_rc == MOQ_OK);
    uint64_t outgoing_due = session_queued_data_deadline(p->c);
    REQUIRE(outgoing_due == p->now + 10000 && outgoing_due < age);
    REQUIRE(p->c->action_timed > 0 && moq_session_has_pending_output(p->c));
    REQUIRE(b->pending_count == 0 && p->cep.count == 0);

    uint64_t epoch = p->c->borrow_epoch;
    p->now = due - 1;
    REQUIRE(hold_data(b, 7001, p->now));
    uint64_t younger_age = held(b, 7001)->first_refusal_us + BRIDGE_HOLD_AGE_US;
    REQUIRE(younger_age > due);
    p->now = due;
    REQUIRE(padding_fin(b, last, p->now));
    REQUIRE(p->c->borrow_epoch == epoch);
    REQUIRE(moq_transport_bridge_stream_count(b) == b->max_streams - 1);
    REQUIRE(held(b, 7000) && !held(b, 7000)->offered);
    REQUIRE(held(b, 7001) && !held(b, 7001)->offered);
    REQUIRE(b->ready_due && b->ready_at == due);

    uint64_t suspends = session_budget_suspend_count;
    moq_bridge_budgeted_result_t out;
    REQUIRE(moq_transport_bridge_service_budgeted(b, due, 0, &out) == MOQ_OK);
    REQUIRE(out.suspended && out.sweep_spent == 0);
    REQUIRE(session_budget_suspend_count == suspends + 1);
    REQUIRE(p->c->borrow_epoch == epoch + 1 && p->c->sweep_active);
    REQUIRE(!p->c->budget_active && !b->in_service && !b->ready_due);
    REQUIRE(pending_done(f) && !pending_done(f)->done_expired);
    REQUIRE(session_queued_data_deadline(p->c) == outgoing_due);
    REQUIRE(p->cep.count == 0); /* no hold maintenance or outgoing dispatch */
    REQUIRE(held(b, 7000) && held(b, 7000)->state == BRIDGE_HOLD_RECORDED);
    REQUIRE(!held(b, 7000)->offered && held(b, 7000)->first_refusal_us + BRIDGE_HOLD_AGE_US == age);
    REQUIRE(held(b, 7001) && !held(b, 7001)->offered);
    REQUIRE(moq_transport_bridge_stream_has_pending(b, 7000));
    REQUIRE(moq_transport_bridge_stream_has_pending(b, 7001));
    REQUIRE(p->c->has_owner_deadline && p->c->owner_deadline_us == age);
    REQUIRE(done_events(f, 0));

    REQUIRE(service(b, due, true, 16, false));
    REQUIRE(held(b, 7000) == NULL);
    REQUIRE(op_count(&p->cep, FAKE_OP_STOP, 7000) == 1);
    REQUIRE(op_count(&p->cep, FAKE_OP_STOP, 7001) == 0);
    REQUIRE(held(b, 7001) && held(b, 7001)->offered && hold_count(b) == 1);
    REQUIRE(p->c->has_owner_deadline && p->c->owner_deadline_us == younger_age);
    REQUIRE(pending_done(f) == NULL);
    unsigned dones = 0, expired = 0;
    other = 0;
    while (moq_session_poll_events(p->c, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_DONE &&
            ev.u.subscribe_done.sub._opaque == f->csub._opaque &&
            ev.u.subscribe_done.stream_count == 1) {
            ++dones;
        } else if (ev.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED &&
                   moq_subgroup_eq(ev.u.outgoing_subgroup_terminated.subgroup, group) &&
                   ev.u.outgoing_subgroup_terminated.sub._opaque == publisher._opaque &&
                   ev.u.outgoing_subgroup_terminated.cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT &&
                   ev.u.outgoing_subgroup_terminated.error_code == MOQ_RESET_DELIVERY_TIMEOUT) {
            ++expired;
        } else {
            ++other;
        }
        moq_event_cleanup(&ev);
    }
    REQUIRE(dones == 1 && expired == 1 && other == 0);
    REQUIRE(replay_waiter(b, 7001, due));
    REQUIRE(hold_count(b) == 0 && !p->c->has_owner_deadline);
    REQUIRE(service(b, due, true, 16, false));
    REQUIRE(op_count(&p->cep, FAKE_OP_STOP, 7000) == 1);
    REQUIRE(op_count(&p->cep, FAKE_OP_STOP, 7001) == 0);
    REQUIRE(done_events(f, 0));
    return healthy(f);
}

/* A real idle close supersedes a map-blocked request even when the endpoint
 * first refuses the close. The retained close keeps its exact cause; freeing
 * map capacity cannot let the superseded request open or write afterwards. */
static bool row_map_pending_idle_close_retry(fixture_t *f,
                                             moq_version_t ver, bool unused)
{
    (void)unused;
    REQUIRE(setup(f, ver, 3, 8, true, 0, 1000000));
    rl_pair_t *p = &f->p;
    moq_transport_bridge_t *b = p->cb;
    REQUIRE(padding(b, 6000, ++p->now));
    moq_bytes_t part = MOQ_BYTES_LITERAL("readiness");
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ &part, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("map-blocked");
    moq_track_status_handle_t request;
    REQUIRE(moq_session_track_status(p->c, &cfg, p->now, &request) == MOQ_OK);
    REQUIRE(service(b, ++p->now, false, 0, false));
    REQUIRE(b->pending_count == 1 && b->pending[0].kind == PENDING_OPEN_BIDI_DATA);
    REQUIRE(b->pending[0].data != NULL && b->pending[0].data_len > 0);
    REQUIRE(!moq_transport_bridge_has_outbound_pending(b));
    REQUIRE(!moq_session_has_pending_output(p->c));
    REQUIRE(!b->ready_due && hold_count(b) == 0);
    REQUIRE(p->cep.count == 0 && p->cep.block_count == 0);
    REQUIRE(b->max_pending == HOLDS);

    uint64_t due = p->c->idle_deadline_us;
    REQUIRE(due > p->now && moq_session_next_deadline_us(p->c) == due);
    uint64_t next_bidi = p->cep.next_bidi_id;
    uint64_t writes = p->cep.write_calls;
    uint64_t uni_opens = p->cep.open_uni_calls;
    static const uint8_t reason[] = "idle timeout";
    p->cep.block_close = true;
    REQUIRE(moq_transport_bridge_service(b, due) == MOQ_OK);
    REQUIRE(!b->in_service && !b->ready_due && !p->c->budget_active);
    REQUIRE(!moq_transport_bridge_is_fatal(b));
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_CLOSED);
    REQUIRE(!moq_transport_bridge_is_closed(b));
    REQUIRE(p->cep.block_count == 1 && p->cep.count == 0);
    REQUIRE(b->pending_count == 1 && b->pending_count <= b->max_pending);
    REQUIRE(b->pending[0].kind == PENDING_CLOSE_TRANSPORT);
    REQUIRE(b->pending[0].error_code == MOQ_CLOSE_IDLE_TIMEOUT);
    REQUIRE(b->pending[0].data && b->pending[0].data_len == sizeof(reason) - 1);
    REQUIRE(memcmp(b->pending[0].data, reason, sizeof(reason) - 1) == 0);
    REQUIRE(moq_transport_bridge_has_outbound_pending(b));

    if (bridge_find_by_id_const(b, 6000))
        REQUIRE(padding_fin(b, 6000, due));
    REQUIRE(moq_transport_bridge_stream_count(b) < b->max_streams);
    REQUIRE(moq_transport_bridge_service(b, due + 1) == MOQ_OK);
    REQUIRE(!b->in_service && !b->ready_due);
    REQUIRE(!moq_transport_bridge_is_fatal(b) && !moq_transport_bridge_is_closed(b));
    REQUIRE(p->cep.block_count == 2 && p->cep.count == 0);
    REQUIRE(b->pending_count == 1 && b->pending_count <= b->max_pending);
    REQUIRE(b->pending[0].kind == PENDING_CLOSE_TRANSPORT);
    REQUIRE(b->pending[0].error_code == MOQ_CLOSE_IDLE_TIMEOUT);
    REQUIRE(b->pending[0].data && b->pending[0].data_len == sizeof(reason) - 1);
    REQUIRE(memcmp(b->pending[0].data, reason, sizeof(reason) - 1) == 0);
    REQUIRE(p->cep.next_bidi_id == next_bidi && p->cep.write_calls == writes);
    REQUIRE(p->cep.open_uni_calls == uni_opens);

    p->cep.block_close = false;
    REQUIRE(moq_transport_bridge_service(b, due + 2) == MOQ_OK);
    REQUIRE(!moq_transport_bridge_is_fatal(b) && moq_transport_bridge_is_closed(b));
    REQUIRE(moq_transport_bridge_close_code(b) == MOQ_CLOSE_IDLE_TIMEOUT);
    REQUIRE(!b->in_service && !b->ready_due && !p->c->budget_active);
    REQUIRE(b->pending_count == 0 && !moq_session_has_pending_output(p->c));
    REQUIRE(p->cep.count == 1 && p->cep.ops[0].kind == FAKE_OP_CLOSE);
    REQUIRE(p->cep.ops[0].error_code == MOQ_CLOSE_IDLE_TIMEOUT);
    REQUIRE(p->cep.ops[0].data_len == sizeof(reason) - 1);
    REQUIRE(memcmp(p->cep.ops[0].data, reason, sizeof(reason) - 1) == 0);
    REQUIRE(moq_transport_bridge_service(b, due + 3) == MOQ_OK);
    REQUIRE(moq_transport_bridge_service(b, due + 4) == MOQ_OK);
    REQUIRE(p->cep.count == 1 && p->cep.block_count == 2);
    REQUIRE(p->cep.next_bidi_id == next_bidi && p->cep.write_calls == writes);
    REQUIRE(p->cep.open_uni_calls == uni_opens && b->pending_count == 0);
    REQUIRE(moq_session_state(p->c) == MOQ_SESS_CLOSED);
    REQUIRE(moq_session_next_deadline_us(p->c) == UINT64_MAX);
    obs_t events = {0};
    drain(p->c, &events);
    REQUIRE(events.closed == 1 && events.close_code == MOQ_CLOSE_IDLE_TIMEOUT);
    REQUIRE(!p->deliver_error && p->cin.count == 0 && p->sin.count == 0);
    REQUIRE(!p->cep.overflowed && !p->cep.truncated);
    return true;
}

int main(void)
{
    static const struct {
        const char *name;
        bool (*run)(fixture_t *, moq_version_t, bool);
        bool budgeted;
    } rows[] = {
        { "25a_ready_consumed_entry_ordinary", row_25a, false },
        { "25a_ready_consumed_entry_budgeted", row_25a, true },
        { "25b_fin_release_paired_sweep_ordinary", row_25b, false },
        { "25b_fin_release_paired_sweep_budgeted", row_25b, true },
        { "25c_real_done_and_hold_age_ordinary", row_25c, false },
        { "25c_real_done_and_hold_age_budgeted", row_25c, true },
        { "25d_suspension_paired_sweep_resume", row_25d, true },
        { "25e_minimum_survives_hold_release", row_25e, false },
        { "cap_off_release_stays_idle", row_cap_off, false },
        { "unknown_prefix_no_idle_spin", row_unknown_prefix, false },
        { "map_only_pending_preserves_idle_deadline", row_map_pending_idle, false },
        { "expired_hold_early_outgoing_suspension", row_expired_hold_early_suspension, true },
        { "map_only_pending_idle_close_retry", row_map_pending_idle_close_retry, false },
    };
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v) {
        for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); ++i) {
            row_name = rows[i].name;
            int before = failures;
            fixture_t *f = calloc(1, sizeof(*f));
            if (!f) {
                ++failures;
                fprintf(stderr, "FAIL %s: fixture allocation\n", row_name);
                continue;
            }
            bool ok = rows[i].run(f, versions[v], rows[i].budgeted);
            observing = NULL;
            pair_close(&f->p);  /* also balances partial setup failures */
            free(f);
            printf("D%d %s: %s\n", v == 0 ? 18 : 22, row_name,
                   ok && failures == before ? "PASS" : "FAIL");
        }
    }
    return failures ? 1 : 0;
}
