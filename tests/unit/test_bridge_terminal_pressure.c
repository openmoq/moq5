/* R1: terminal reset escape through a full bridge pending array and full
 * core action ring. All retained output is produced by real session/bridge
 * calls; only event-credit saturation uses the private queue producer. */
#include <moq/moq.h>
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include "../support/fake_endpoint.h"
#include "test_alloc_support.h"
#include <stdio.h>
#include <string.h>

enum { ACTIONS = 8, EVENTS = 4 };
static unsigned checks, failures;
#define CHECK(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "%s:%d: %s\n", __func__, __LINE__, #x); return false; } } while (0)

typedef struct group {
    moq_subgroup_handle_t handle;
    moq_stream_ref_t ref;
    uint64_t sid, id;
} group_t;

typedef struct endpoint {
    fake_endpoint_t fake; /* the other fake callbacks use this prefix */
    moq_session_t *session;
    group_t target;
    uint64_t owner, code, reliable;
    uint32_t cause;
    bool suppress_target, saw_event_before_reset, bad_reset;
    bool queued_reset_source;
    unsigned post_terminal_writes;
} endpoint_t;

typedef struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *sender, *peer;
    moq_subscription_t sub;
    moq_rcbuf_t *payload, *other_payload;
    moq_transport_bridge_t *bridge;
    endpoint_t ep;
    uint64_t now;
    size_t head, tail;
    moq_action_t actions[ACTIONS];
    moq_action_meta_t meta[ACTIONS];
} fixture_t;

static bool is_terminal(const moq_event_t *ev, const endpoint_t *ep)
{
    if (ev->kind != MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED ||
        ev->detail_size != MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE)
        return false;
    const moq_outgoing_subgroup_terminated_event_t *t = &ev->u.outgoing_subgroup_terminated;
    return t->sub._opaque == ep->owner && !moq_publication_is_valid(t->pub) &&
        t->subgroup._opaque == ep->target.handle._opaque &&
        t->stream_ref._v == ep->target.ref._v && t->group_id == ep->target.id &&
        t->subgroup_id == 0 && t->cause == ep->cause && t->error_code == ep->code;
}

static moq_transport_result_t write_checked(void *ctx, uint64_t sid,
                                            const uint8_t *data, size_t len, bool fin)
{
    endpoint_t *ep = ctx;
    if (ep->suppress_target && sid == ep->target.sid) {
        ep->post_terminal_writes++;
        return MOQ_TRANSPORT_ERROR;
    }
    return fake_write(&ep->fake, sid, data, len, fin);
}

static moq_transport_result_t reset_checked(void *ctx, uint64_t sid,
                                            uint64_t code, uint64_t reliable)
{
    endpoint_t *ep = ctx;
    if (sid == ep->target.sid) {
        ep->bad_reset |= code != ep->code || reliable != ep->reliable;
        if (ep->queued_reset_source) {
            /* This row has action credit: the typed reset stays in the core
             * ring through refusal, even after its subgroup entry reaps. */
            unsigned found = 0;
            for (size_t i = ep->session->action_head; i < ep->session->action_tail; i++) {
                size_t at = i % ep->session->action_cap;
                const moq_action_t *a = &ep->session->actions[at];
                if (a->kind == MOQ_ACTION_RESET_DATA &&
                    a->u.reset_data.stream_ref._v == ep->target.ref._v &&
                    a->u.reset_data.error_code == code &&
                    a->u.reset_data.reliable_size == reliable &&
                    ep->session->action_meta[at].outgoing_terminal) found++;
            }
            ep->bad_reset |= found != 1;
        } else {
            const moq_sg_entry_t *sg = &ep->session->subgroups[
                moq_handle_slot(ep->target.handle._opaque)];
            ep->bad_reset |= sg->state != MOQ_SG_RESET_OWED ||
                sg->stream_ref._v != ep->target.ref._v ||
                sg->owed_reset_code != code || sg->terminal_cause != ep->cause;
        }
        if (!ep->saw_event_before_reset) {
            unsigned found = 0;
            for (size_t i = ep->session->event_head; i < ep->session->event_tail; i++)
                found += is_terminal(&ep->session->events[i % ep->session->event_cap], ep);
            ep->bad_reset |= found != 1;
            ep->saw_event_before_reset = found == 1;
        }
        if (ep->bad_reset) return MOQ_TRANSPORT_ERROR;
    }
    return fake_reset_at(&ep->fake, sid, code, reliable);
}

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
}

static bool setup(fixture_t *f, bool object_timed)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->memory);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = MOQ_VERSION_DRAFT_18; /* this pressure path is profile-independent */
    cfg.max_actions = ACTIONS;
    cfg.max_events = EVENTS;
    cfg.initial_now_us = 1000000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 32;
    CHECK(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    CHECK(moq_simpair_start(f->pair) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    f->sender = moq_simpair_server(f->pair);
    f->peer = moq_simpair_client(f->pair);
    f->now = moq_simpair_now_us(f->pair);
    drain_events(f->sender);
    drain_events(f->peer);
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal-pressure");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.track_namespace = (moq_namespace_t){&ns, 1};
    sc.track_name = MOQ_BYTES_LITERAL("target");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t local;
    CHECK(moq_session_subscribe(f->peer, &sc, f->now, &local) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    moq_event_t ev;
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 1);
    CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    f->sub = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    static const uint8_t timeout[] = {0x06, 0x01};
    /* D18 delta-coded Track Properties: object=1ms, subgroup=1ms. */
    static const uint8_t both_timeouts[] = {0x02, 0x01, 0x04, 0x01};
    ac.track_properties = (moq_bytes_t){timeout, sizeof(timeout)};
    if (object_timed)
        ac.track_properties = (moq_bytes_t){both_timeouts, sizeof(both_timeouts)};
    CHECK(moq_session_accept_subscribe(f->sender, f->sub, &ac, f->now) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    drain_events(f->sender);
    drain_events(f->peer);
    CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"target", 6, &f->payload) == MOQ_OK);
    CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"other", 5, &f->other_payload) == MOQ_OK);
    fake_endpoint_init(&f->ep.fake, 1000, 2000);
    fake_endpoint_enable_reset_at(&f->ep.fake);
    f->ep.fake.vtable.write = write_checked;
    f->ep.fake.vtable.reset_stream_at = reset_checked;
    f->ep.fake.vtable.capabilities |= MOQ_TRANSPORT_CAP_COMMIT_REPORT | MOQ_TRANSPORT_CAP_HOLD_INPUT;
    f->ep.session = f->sender;
    f->ep.owner = f->sub._opaque;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &f->alloc);
    bc.max_pending = 1;
    CHECK(moq_transport_bridge_create(&bc, f->sender, &f->ep.fake.vtable, &f->ep, &f->bridge) == MOQ_OK);
    CHECK(f->sender->action_cap == ACTIONS && f->sender->event_cap == EVENTS);
    return true;
}

static bool cleanup(fixture_t *f)
{
    if (f->bridge) moq_transport_bridge_destroy(f->bridge);
    if (f->payload) moq_rcbuf_decref(f->payload);
    if (f->other_payload) moq_rcbuf_decref(f->other_payload);
    if (f->pair) moq_simpair_destroy(f->pair);
    CHECK(!f->ep.fake.overflowed && !f->ep.fake.truncated);
    CHECK(f->memory.balance == 0);
    return true;
}

static bool service(fixture_t *f)
{
    CHECK(moq_transport_bridge_service(f->bridge, f->now) == MOQ_OK);
    CHECK(!moq_transport_bridge_is_fatal(f->bridge));
    CHECK(!f->ep.bad_reset && f->ep.post_terminal_writes == 0);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool open_group(fixture_t *f, uint64_t id, group_t *g, bool send)
{
    memset(g, 0, sizeof(*g));
    g->id = id;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = id;
    CHECK(moq_session_open_subgroup(f->sender, f->sub, &sc, f->now, &g->handle) == MOQ_OK);
    g->ref = f->sender->subgroups[moq_handle_slot(g->handle._opaque)].stream_ref;
    if (send) {
        CHECK(service(f));
        bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g->ref);
        CHECK(e && e->subgroup_header_len > 0 && e->bytes_sent >= e->subgroup_header_len);
        g->sid = e->transport_id;
    }
    return true;
}

static bool has_queued_payload(const fixture_t *f, const group_t *g)
{
    for (uint32_t i = 0; i < f->bridge->dstream_cap; i++) {
        const bridge_dstream_t *d = &f->bridge->dstreams[i];
        if (!d->active || d->ref._v != g->ref._v || !d->has_sid || d->sid != g->sid) continue;
        for (uint32_t j = 0; j < d->q_len; j++) {
            const moq_action_t *a = &d->q[(d->q_head + j) % d->q_cap].action;
            if (a->kind == MOQ_ACTION_SEND_DATA && a->u.send_data.stream_ref._v == g->ref._v &&
                a->u.send_data.payload == f->payload) return true;
        }
    }
    return false;
}

static bool save_ring(fixture_t *f)
{
    f->head = f->sender->action_head;
    f->tail = f->sender->action_tail;
    CHECK(f->tail - f->head == ACTIONS);
    for (size_t i = 0; i < ACTIONS; i++) {
        size_t at = (f->head + i) % ACTIONS;
        f->actions[i] = f->sender->actions[at];
        f->meta[i] = f->sender->action_meta[at];
    }
    return true;
}

static bool same_ring(const fixture_t *f)
{
    CHECK(f->sender->action_head == f->head && f->sender->action_tail == f->tail);
    for (size_t i = 0; i < ACTIONS; i++) {
        size_t at = (f->head + i) % ACTIONS;
        CHECK(memcmp(&f->actions[i], &f->sender->actions[at], sizeof(f->actions[i])) == 0);
        CHECK(memcmp(&f->meta[i], &f->sender->action_meta[at], sizeof(f->meta[i])) == 0);
    }
    return true;
}

static bool fill_events(fixture_t *f)
{
    while (!event_queue_full(f->sender)) {
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_GOAWAY;
        CHECK(push_event(f->sender, &ev) == MOQ_OK);
    }
    return true;
}

static bool take_terminal(fixture_t *f)
{
    moq_event_t ev;
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 1);
    CHECK(is_terminal(&ev, &f->ep));
    moq_event_cleanup(&ev);
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);
    return true;
}

static unsigned accepted_resets(const fixture_t *f)
{
    unsigned n = 0;
    for (size_t i = 0; i < f->ep.fake.count; i++)
        if (f->ep.fake.ops[i].kind == FAKE_OP_RESET_AT &&
            f->ep.fake.ops[i].stream_id == f->ep.target.sid) n++;
    return n;
}

static bool pressure(fixture_t *f, bool timeout, bool object_timed)
{
    group_t target, sibling, blocked;
    CHECK(open_group(f, 10, &target, true));
    CHECK(open_group(f, 11, &sibling, true));
    f->ep.target = target;
    f->ep.code = timeout ? MOQ_RESET_DELIVERY_TIMEOUT : 61;
    f->ep.cause = timeout ? MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT
                         : MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING;
    f->ep.reliable = bridge_find_by_ref(f->bridge, target.ref)->subgroup_header_len;
    fake_endpoint_clear_ops(&f->ep.fake); /* record the pressured phase */
    fake_block_stream(&f->ep.fake, target.sid);
    CHECK(moq_session_write_object(f->sender, target.handle, 0, f->payload, f->now) == MOQ_OK);
    if (timeout) CHECK(moq_session_close_subgroup(f->sender, target.handle, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(has_queued_payload(f, &target) && moq_rcbuf_refcount(f->payload) > 1);
    /* The extra STOP row owns a timed DATA action in the bridge, not merely
     * in the core. The original subgroup-timeout rows remain untimed here. */
    CHECK(f->bridge->dq_timed == (object_timed ? 1u : 0u));
    if (object_timed) CHECK(!timeout && f->bridge->dq_items == 1);
    CHECK(f->bridge->dreset_count == 0);
#ifdef MOQ_SESSION_SWEEP_TESTING
    uint64_t healthy_slots = bridge_work_terminal_slots;
    for (unsigned i = 0; i < 3; i++) CHECK(service(f));
    CHECK(bridge_work_terminal_slots == healthy_slots);
    CHECK(has_queued_payload(f, &target));
#endif

    f->ep.fake.block_open_uni = true;
    CHECK(open_group(f, 12, &blocked, false));
    CHECK(service(f));
    CHECK(f->bridge->pending_count == 1 && f->bridge->max_pending == 1);
    CHECK(f->bridge->pending[0].kind == PENDING_OPEN_UNI_DATA);
    CHECK(f->bridge->pending[0].stream_ref._v == blocked.ref._v);
    CHECK(has_queued_payload(f, &target));
    CHECK(f->sender->action_head == f->sender->action_tail);
    for (size_t i = 0; i < ACTIONS; i++)
        CHECK(moq_session_write_object(f->sender, sibling.handle, i, f->other_payload, f->now) == MOQ_OK);
    CHECK(save_ring(f));
    for (size_t i = 0; i < ACTIONS; i++) {
        CHECK(f->actions[i].kind == MOQ_ACTION_SEND_DATA);
        CHECK(f->actions[i].u.send_data.stream_ref._v == sibling.ref._v);
    }

    moq_sg_entry_t *sg = &f->sender->subgroups[moq_handle_slot(target.handle._opaque)];
    f->ep.fake.block_reset_at = true;
    f->ep.suppress_target = true;
#ifdef MOQ_SESSION_SWEEP_TESTING
    uint64_t terminal_slots = bridge_work_terminal_slots;
#endif
    if (timeout) {
        CHECK(sg->delivery_deadline_us > f->now && sg->delivery_deadline_us != UINT64_MAX);
        f->now = sg->delivery_deadline_us;
        CHECK(moq_session_tick(f->sender, f->now) == MOQ_ERR_WOULD_BLOCK);
    } else {
        CHECK(fill_events(f));
        CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, target.sid, 61, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, target.sid, 99, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(f->bridge->dq_items == 0 && f->bridge->dq_bytes == 0);
        CHECK(f->bridge->dq_timed == 0 && f->bridge->dreset_count == 0);
        CHECK(moq_rcbuf_refcount(f->payload) == 1);
        CHECK(service(f));
        CHECK(f->ep.fake.reset_at_calls == 0 && !f->ep.saw_event_before_reset);
        moq_reset_data_action_t offer;
        CHECK(!bridge_session_offer_data_reset(f->sender, &offer));
        CHECK(same_ring(f));
        for (size_t i = 0; i < EVENTS; i++) {
            moq_event_t ev;
            CHECK(moq_session_poll_events(f->sender, &ev, 1) == 1);
            CHECK(ev.kind == MOQ_EVENT_GOAWAY);
            moq_event_cleanup(&ev);
        }
    }
    CHECK(sg->state == MOQ_SG_TERMINAL_OWED && sg->terminal_code == f->ep.code);
    moq_reset_data_action_t offer;
    bool offered = true;
    for (unsigned i = 0; i < 2; i++) {
        bool available = bridge_session_offer_data_reset(f->sender, &offer);
        offered &= available;
        if (available) {
            CHECK(offer.stream_ref._v == target.ref._v && offer.error_code == f->ep.code);
            CHECK(offer.reliable_size == f->ep.reliable);
        }
        CHECK(sg->state == MOQ_SG_TERMINAL_OWED);
        CHECK(f->sender->event_head == f->sender->event_tail);
        CHECK(same_ring(f)); /* offering cannot claim event/reset or poll output */
    }
    CHECK(service(f)); /* the actual bridge pressure escape must call claim */
    CHECK(f->ep.fake.reset_at_calls > 0 && f->ep.saw_event_before_reset);
    CHECK(offered); /* old product must reach the production escape before RED */
#ifdef MOQ_SESSION_SWEEP_TESTING
    CHECK(bridge_work_terminal_slots > terminal_slots);
#endif
    CHECK(f->bridge->dq_timed == 0);
    CHECK(accepted_resets(f) == 0 && sg->state == MOQ_SG_RESET_OWED);
    CHECK(sg->owed_reset_code == f->ep.code && sg->terminal_cause == f->ep.cause);
    CHECK(take_terminal(f));
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, target.sid, 99, f->now) == MOQ_OK);
    for (unsigned i = 0; i < 2; i++) {
        CHECK(service(f));
        bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, target.ref);
        CHECK(e && e->reset_source_held);
        CHECK(f->bridge->pending_count == 1 && f->bridge->pending[0].stream_ref._v == blocked.ref._v);
        CHECK(same_ring(f));
        CHECK(sg->state == MOQ_SG_RESET_OWED && sg->owed_reset_code == f->ep.code);
        CHECK(f->sender->event_head == f->sender->event_tail);
    }
    f->ep.fake.block_reset_at = false;
    CHECK(service(f));
    CHECK(accepted_resets(f) == 1);
    CHECK(moq_transport_bridge_find_ref(f->bridge, target.sid)._v == 0);
    CHECK(f->bridge->pending_count == 1 && same_ring(f));
    CHECK(f->sender->event_head == f->sender->event_tail);
    f->ep.fake.block_open_uni = false;
    CHECK(service(f));
    CHECK(f->bridge->pending_count == 0 && f->bridge->pending_opens == 0);
    CHECK(f->bridge->dq_items == 0 && f->bridge->dq_bytes == 0);
    CHECK(f->bridge->dq_timed == 0 && f->bridge->dreset_count == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1 && moq_rcbuf_refcount(f->other_payload) == 1);
    CHECK(bridge_find_by_ref(f->bridge, sibling.ref) != NULL);
    CHECK(accepted_resets(f) == 1);
    return true;
}

static bool request(fixture_t *f, unsigned id)
{
    char name[32];
    int n = snprintf(name, sizeof(name), "independent-%u", id);
    CHECK(n > 0 && (size_t)n < sizeof(name));
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal-pressure");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.track_namespace = (moq_namespace_t){&ns, 1};
    sc.track_name = (moq_bytes_t){(const uint8_t *)name, (size_t)n};
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    moq_subscription_t owner;
    CHECK(moq_session_subscribe(f->sender, &sc, f->now, &owner) == MOQ_OK);
    return true;
}

/* An OPEN subgroup has no close timer. Only its bridge-owned object's
 * deadline can create this terminal, with the core clock still stale. */
static bool retained_object_event_credit(fixture_t *f)
{
    group_t target, sibling, blocked;
    CHECK(open_group(f, 30, &target, true));
    CHECK(open_group(f, 31, &sibling, true));
    f->ep.target = target;
    f->ep.code = MOQ_RESET_DELIVERY_TIMEOUT;
    f->ep.cause = MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT;
    f->ep.reliable = bridge_find_by_ref(f->bridge, target.ref)->subgroup_header_len;
    f->ep.queued_reset_source = true;
    fake_endpoint_clear_ops(&f->ep.fake);
    fake_block_stream(&f->ep.fake, target.sid);
    CHECK(moq_session_write_object(f->sender, target.handle, 0, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(has_queued_payload(f, &target) && moq_rcbuf_refcount(f->payload) > 1);
    CHECK(f->bridge->dq_items == 1 && f->bridge->dq_timed == 1);
    uint64_t due = UINT64_MAX;
    for (uint32_t i = 0; i < f->bridge->dstream_cap; i++) {
        const bridge_dstream_t *d = &f->bridge->dstreams[i];
        if (!d->active || d->ref._v != target.ref._v) continue;
        CHECK(d->q_len == 1 && !d->has_reset);
        const bridge_data_item_t *item = &d->q[d->q_head];
        CHECK(item->action.u.send_data.payload == f->payload && !item->meta.subgroup_header);
        due = item->meta.deadline_us;
    }
    CHECK(due != UINT64_MAX && due > f->now);
    f->ep.fake.block_open_uni = true;
    CHECK(open_group(f, 32, &blocked, false));
    CHECK(service(f));
    CHECK(f->bridge->pending_count == 1 && f->bridge->pending_opens == 1);
    CHECK(f->bridge->pending[0].kind == PENDING_OPEN_UNI_DATA &&
          f->bridge->pending[0].stream_ref._v == blocked.ref._v);
    CHECK(f->sender->action_head == f->sender->action_tail && f->sender->action_timed == 0);
    moq_sg_entry_t *sg = &f->sender->subgroups[moq_handle_slot(target.handle._opaque)];
    CHECK(sg->state == MOQ_SG_OPEN && sg->delivery_deadline_us == UINT64_MAX);
    CHECK(f->sender->subgroup_deadline_us == UINT64_MAX);
    uint64_t old_now = f->sender->last_now_us;
    CHECK(old_now < due && old_now <= UINT64_MAX - 10000);
    CHECK(fill_events(f));
    size_t event_head = f->sender->event_head, event_tail = f->sender->event_tail;
    f->now = old_now + 10000; /* exceeds the 5000us terminal retry interval */
    CHECK(f->now > due);
    f->ep.suppress_target = true;
    f->ep.fake.block_reset_at = true;
    fake_unblock_stream(&f->ep.fake);
    /* No tick or other core call advances old_now before bridge expiry. */
    CHECK(service(f));
    CHECK(sg->state == MOQ_SG_TERMINAL_OWED && sg->stream_ref._v == target.ref._v);
    CHECK(sg->terminal_code == f->ep.code && sg->terminal_cause == f->ep.cause);
    CHECK(f->sender->event_head == event_head && f->sender->event_tail == event_tail);
    CHECK(event_queue_full(f->sender) && f->ep.fake.reset_at_calls == 0);
    CHECK(!f->ep.saw_event_before_reset && f->bridge->dreset_count == 0);
    CHECK(f->sender->action_head == f->sender->action_tail);
    CHECK(f->bridge->dq_items == 0 && f->bridge->dq_bytes == 0 && f->bridge->dq_timed == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(f->bridge->pending_count == 1 &&
          f->bridge->pending[0].stream_ref._v == blocked.ref._v);
    /* Check after debt/cleanup: stale-clock RED must reach the actual handoff. */
    CHECK(f->sender->subgroup_retry_deadline_us > f->now &&
          f->sender->subgroup_retry_deadline_us != UINT64_MAX);
    CHECK(service(f));
    CHECK(sg->state == MOQ_SG_TERMINAL_OWED && sg->terminal_code == f->ep.code);
    CHECK(f->sender->event_head == event_head && f->sender->event_tail == event_tail);
    CHECK(f->ep.fake.reset_at_calls == 0 && f->bridge->dreset_count == 0);
    CHECK(f->sender->subgroup_retry_deadline_us > f->now);

    /* Exactly one returned event slot is sufficient, without advancing time. */
    moq_event_t ev;
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 1 && ev.kind == MOQ_EVENT_GOAWAY);
    moq_event_cleanup(&ev);
    CHECK(service(f));
    CHECK(f->ep.saw_event_before_reset && f->ep.fake.reset_at_calls > 0);
    CHECK(accepted_resets(f) == 0 && event_queue_full(f->sender));
    for (size_t i = 1; i < EVENTS; i++) {
        CHECK(moq_session_poll_events(f->sender, &ev, 1) == 1 && ev.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&ev);
    }
    CHECK(take_terminal(f));
    const bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, target.ref);
    CHECK(e && e->data_terminal_received && e->reset_source_held);
    CHECK(f->bridge->pending_count == 1 && f->bridge->dreset_count == 0);
    f->ep.fake.block_open_uni = false;
    CHECK(service(f));
    CHECK(f->bridge->pending_count == 0);
    CHECK(moq_session_write_object(f->sender, sibling.handle, 0, f->other_payload, f->now) == MOQ_OK);
    CHECK(service(f));
    unsigned payload_writes = 0;
    for (size_t i = 0; i < f->ep.fake.count; i++) {
        const fake_op_t *op = &f->ep.fake.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == sibling.sid &&
            op->data_len == 5 && memcmp(op->data, "other", 5) == 0) {
            CHECK(!op->fin);
            payload_writes++;
        }
    }
    CHECK(payload_writes == 0);
    CHECK(moq_rcbuf_refcount(f->other_payload) == 2);
    unsigned retained = 0;
    for (size_t i = f->sender->action_head; i < f->sender->action_tail; i++) {
        const moq_action_t *a = &f->sender->actions[i % f->sender->action_cap];
        if (a->kind != MOQ_ACTION_SEND_DATA ||
            a->u.send_data.stream_ref._v != sibling.ref._v) continue;
        CHECK(a->u.send_data.payload == f->other_payload);
        CHECK(moq_rcbuf_len(a->u.send_data.payload) == 5 && !a->u.send_data.fin);
        retained++;
    }
    CHECK(retained == 1); /* later DATA stays behind the refused core RESET */
    CHECK(accepted_resets(f) == 0 && moq_session_poll_events(f->sender, &ev, 1) == 0);
    CHECK(bridge_find_by_ref(f->bridge, target.ref) != NULL);
    f->ep.fake.block_reset_at = false;
    CHECK(service(f));
    CHECK(accepted_resets(f) == 1 && bridge_find_by_ref(f->bridge, target.ref) == NULL);
    payload_writes = 0;
    for (size_t i = 0; i < f->ep.fake.count; i++) {
        const fake_op_t *op = &f->ep.fake.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == sibling.sid &&
            op->data_len == 5 && memcmp(op->data, "other", 5) == 0) {
            CHECK(!op->fin);
            payload_writes++;
        }
    }
    CHECK(payload_writes == 1);
    CHECK(moq_rcbuf_refcount(f->other_payload) == 1);
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);
    CHECK(f->bridge->dreset_count == 0 && f->bridge->dq_timed == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1 && f->ep.post_terminal_writes == 0);
    /* A normal close through the original handle proves it remains usable;
     * neither the target's terminal nor its reset terminated this sibling. */
    CHECK(moq_session_close_subgroup(f->sender, sibling.handle, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(accepted_resets(f) == 1 && moq_session_poll_events(f->sender, &ev, 1) == 0);
    return true;
}

static bool compact_before_done(fixture_t *f)
{
    group_t target;
    CHECK(open_group(f, 20, &target, true));
    CHECK(moq_session_close_subgroup(f->sender, target.handle, f->now) == MOQ_OK);
    CHECK(service(f));
    f->ep.target = target;
    f->ep.code = MOQ_RESET_DELIVERY_TIMEOUT;
    f->ep.cause = MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT;
    f->ep.reliable = bridge_find_by_ref(f->bridge, target.ref)->subgroup_header_len;
    f->ep.fake.block_open_bidi = true;
    CHECK(request(f, 0));
    CHECK(service(f));
    CHECK(f->bridge->pending_count == 1 && f->bridge->pending[0].kind == PENDING_OPEN_BIDI_DATA);
    moq_done_subscribe_cfg_t dc;
    moq_done_subscribe_cfg_init(&dc);
    dc.stream_count = 1;
    CHECK(moq_session_done_subscribe(f->sender, f->sub, &dc, f->now) == MOQ_OK);
    for (unsigned i = 1; i < ACTIONS; i++) CHECK(request(f, i));
    CHECK(save_ring(f));
    CHECK(f->meta[0].terminal_owner == f->sub._opaque);
    CHECK(f->actions[0].kind == MOQ_ACTION_SEND_BIDI_STREAM && f->actions[0].u.send_bidi_stream.fin);
    moq_sg_entry_t *sg = &f->sender->subgroups[moq_handle_slot(target.handle._opaque)];
    CHECK(sg->delivery_deadline_us > f->now && sg->delivery_deadline_us != UINT64_MAX);
    f->now = sg->delivery_deadline_us;
    f->ep.fake.block_reset_at = true;
    f->ep.suppress_target = true;
    CHECK(service(f));
    CHECK(f->ep.saw_event_before_reset && sg->state == MOQ_SG_RESET_OWED);
    CHECK(same_ring(f));
    CHECK(take_terminal(f));
    CHECK(fill_events(f)); /* a committed compact reset needs no second credit */
    moq_action_t a;
    moq_action_meta_t meta;
    CHECK(bridge_session_poll_action(f->sender, &a, &meta));
    CHECK(a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.stream_ref._v == target.ref._v);
    CHECK(a.u.reset_data.error_code == MOQ_RESET_DELIVERY_TIMEOUT && meta.outgoing_terminal);
    moq_action_cleanup(&a);
    CHECK(same_ring(f));
    CHECK(bridge_session_poll_action(f->sender, &a, &meta));
    CHECK(a.kind == MOQ_ACTION_SEND_BIDI_STREAM && a.u.send_bidi_stream.fin);
    CHECK(meta.terminal_owner == f->sub._opaque);
    moq_action_cleanup(&a);
    CHECK(event_queue_full(f->sender));
    for (size_t i = f->sender->event_head; i < f->sender->event_tail; i++)
        CHECK(f->sender->events[i % f->sender->event_cap].kind == MOQ_EVENT_GOAWAY);
    return true;
}

int main(void)
{
    fixture_t f;
    puts("terminal pressure: STOP, full events, refused reset retries");
    if (setup(&f, false)) (void)pressure(&f, false, false);
    (void)cleanup(&f);
    puts("terminal pressure: timeout, available event credit, refused reset retries");
    if (setup(&f, false)) (void)pressure(&f, true, false);
    (void)cleanup(&f);
    puts("terminal pressure: committed compact reset before full-ring owner DONE");
    if (setup(&f, false)) (void)compact_before_done(&f);
    (void)cleanup(&f);
    puts("terminal pressure: timed retained DATA pruned by STOP");
    if (setup(&f, true)) (void)pressure(&f, false, true);
    (void)cleanup(&f);
    puts("terminal pressure: live retained object, full events, stale-clock retry");
    if (setup(&f, true)) (void)retained_object_event_credit(&f);
    (void)cleanup(&f);
    printf("%u assertions, %u failures\n", checks, failures);
    return failures ? 1 : 0;
}
