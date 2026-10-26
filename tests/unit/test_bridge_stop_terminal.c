#include <moq/moq.h>
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include "../support/fake_endpoint.h"
#include "test_alloc_support.h"
#include <stdio.h>
#include <string.h>

static unsigned checks, failures;
#define CHECK(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "%s:%d: %s\n", __func__, __LINE__, #x); return false; } } while (0)

typedef struct endpoint {
    fake_endpoint_t fake;
    bool stopped, fail_reset;
    uint64_t stopped_sid;
    unsigned post_stop_writes;
    /* The budget row accepts more writes than fake.ops can hold. Count its
     * sibling's alternating header/payload writes without losing evidence. */
    bool count_budget_writes;
    uint64_t budget_sid;
    unsigned budget_headers, budget_payloads;
} endpoint_t;

static moq_transport_result_t write_checked(void *ctx, uint64_t sid,
                                            const uint8_t *data, size_t len, bool fin)
{
    endpoint_t *e = ctx;
    if (e->stopped && sid == e->stopped_sid) {
        e->post_stop_writes++;
        return MOQ_TRANSPORT_ERROR;
    }
    if (e->count_budget_writes && sid == e->budget_sid &&
        !e->fake.block_write && !fake_stream_blocked(&e->fake, sid) &&
        !e->fake.fail_write) {
        if (fin || !data || len == 0) return MOQ_TRANSPORT_ERROR;
        if (e->budget_headers == e->budget_payloads) {
            if (len > 16) return MOQ_TRANSPORT_ERROR;
            e->budget_headers++;
        } else {
            if (len != 6 || memcmp(data, "object", 6) != 0)
                return MOQ_TRANSPORT_ERROR;
            e->budget_payloads++;
        }
        e->fake.write_calls++;
        return MOQ_TRANSPORT_OK;
    }
    return fake_write(&e->fake, sid, data, len, fin);
}

static moq_transport_result_t reset_checked(void *ctx, uint64_t sid,
                                            uint64_t code, uint64_t reliable)
{
    endpoint_t *e = ctx;
    if (e->fail_reset) return MOQ_TRANSPORT_ERROR;
    return fake_reset_at(&e->fake, sid, code, reliable);
}

typedef struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *sender, *peer;
    moq_subscription_t sub;
    moq_publication_t pub;
    bool publication;
    moq_rcbuf_t *payload;
    moq_transport_bridge_t *bridge;
    endpoint_t ep;
    uint64_t now;
} fixture_t;

typedef struct group {
    moq_subgroup_handle_t handle;
    moq_stream_ref_t ref;
    uint64_t sid;
    uint64_t group_id, subgroup_id;
} group_t;

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
}

static bool setup_owner(fixture_t *f, moq_version_t version, bool client_sender,
                         bool timed, bool publication)
{
    memset(f, 0, sizeof(*f));
    f->publication = publication;
    f->alloc = test_allocator(&f->memory);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = version;
    cfg.max_actions = 8;
    cfg.max_events = 4;
    cfg.initial_now_us = 1000000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 32;
    CHECK(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    CHECK(moq_simpair_start(f->pair) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    f->sender = client_sender ? moq_simpair_client(f->pair) : moq_simpair_server(f->pair);
    f->peer = client_sender ? moq_simpair_server(f->pair) : moq_simpair_client(f->pair);
    drain_events(f->sender);
    drain_events(f->peer);
    f->now = moq_simpair_now_us(f->pair);
    moq_bytes_t ns = MOQ_BYTES_LITERAL("bridge-terminal");
    if (publication) {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        pc.track_namespace = (moq_namespace_t){&ns, 1};
        pc.track_name = MOQ_BYTES_LITERAL("v");
        CHECK(moq_session_publish(f->sender, &pc, f->now, &f->pub) == MOQ_OK);
        CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
        moq_event_t ev;
        CHECK(moq_session_poll_events(f->peer, &ev, 1) == 1);
        CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
        moq_publication_t owner = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        CHECK(moq_session_accept_publish(f->peer, owner, &ac, f->now) == MOQ_OK);
    } else {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
        sc.track_namespace = (moq_namespace_t){&ns, 1};
        sc.track_name = MOQ_BYTES_LITERAL("v");
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
        if (timed) ac.track_properties = (moq_bytes_t){timeout, sizeof(timeout)};
        CHECK(moq_session_accept_subscribe(f->sender, f->sub, &ac, f->now) == MOQ_OK);
    }
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    drain_events(f->sender);
    drain_events(f->peer);
    CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &f->payload) == MOQ_OK);
    fake_endpoint_init(&f->ep.fake, 1000, 2000);
    fake_endpoint_enable_reset_at(&f->ep.fake);
    f->ep.fake.vtable.write = write_checked;
    f->ep.fake.vtable.reset_stream_at = reset_checked;
    f->ep.fake.vtable.capabilities |= MOQ_TRANSPORT_CAP_COMMIT_REPORT | MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t bc;
    moq_transport_bridge_cfg_init(&bc, &f->alloc);
    CHECK(moq_transport_bridge_create(&bc, f->sender, &f->ep.fake.vtable, &f->ep, &f->bridge) == MOQ_OK);
    return true;
}

static bool setup(fixture_t *f, moq_version_t version, bool client_sender, bool timed)
{
    return setup_owner(f, version, client_sender, timed, false);
}

static bool cleanup(fixture_t *f)
{
    bool complete_log = !f->ep.fake.overflowed && !f->ep.fake.truncated;
    moq_transport_bridge_destroy(f->bridge);
    moq_rcbuf_decref(f->payload);
    moq_simpair_destroy(f->pair);
    CHECK(complete_log);
    CHECK(f->memory.balance == 0);
    return true;
}

static bool service(fixture_t *f)
{
    CHECK(moq_transport_bridge_service(f->bridge, f->now) == MOQ_OK);
    CHECK(!moq_transport_bridge_is_fatal(f->bridge));
    CHECK(f->ep.post_stop_writes == 0);
    return true;
}

/* Data retained by the scheduler lives in its exact stream queue, rather
 * than the generic retry queue. Establish ownership before injecting STOP. */
static bool retained_output(const fixture_t *f, const group_t *g, bool fin)
{
    for (uint32_t i = 0; i < f->bridge->dstream_cap; i++) {
        const bridge_dstream_t *d = &f->bridge->dstreams[i];
        if (!d->active || d->ref._v != g->ref._v) continue;
        if (!d->has_sid || d->sid != g->sid || d->q_len == 0) return false;
        for (uint32_t j = 0; j < d->q_len; j++) {
            const moq_action_t *a = &d->q[(d->q_head + j) % d->q_cap].action;
            if (a->kind != MOQ_ACTION_SEND_DATA ||
                a->u.send_data.stream_ref._v != g->ref._v) return false;
            if (fin ? a->u.send_data.fin : a->u.send_data.payload == f->payload)
                return true;
        }
    }
    return false;
}

static bool open_group(fixture_t *f, uint64_t group, uint64_t subgroup, group_t *g)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = group;
    sc.subgroup_id = subgroup;
    g->group_id = group;
    g->subgroup_id = subgroup;
    moq_result_t rc = f->publication
        ? moq_session_open_pub_subgroup(f->sender, f->pub, &sc, f->now, &g->handle)
        : moq_session_open_subgroup(f->sender, f->sub, &sc, f->now, &g->handle);
    CHECK(rc == MOQ_OK);
    size_t before = f->ep.fake.count;
    CHECK(service(f));
    CHECK(f->ep.fake.count == before + 2);
    CHECK(f->ep.fake.ops[before].kind == FAKE_OP_OPEN_UNI);
    CHECK(f->ep.fake.ops[before + 1].kind == FAKE_OP_WRITE);
    g->sid = f->ep.fake.ops[before].stream_id;
    g->ref = moq_transport_bridge_find_ref(f->bridge, g->sid);
    CHECK(g->ref._v != 0);
    return true;
}

static size_t fill_events(moq_session_t *s)
{
    size_t n = 0;
    while (s->event_tail - s->event_head < s->event_cap) {
        moq_event_t *e = &s->events[s->event_tail++ % s->event_cap];
        memset(e, 0, sizeof(*e));
        e->kind = MOQ_EVENT_GOAWAY;
        n++;
    }
    return n;
}

static bool release_events(fixture_t *f, size_t n)
{
    while (n--) {
        moq_event_t e;
        CHECK(moq_session_poll_events(f->sender, &e, 1) == 1);
        CHECK(e.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&e);
    }
    return true;
}

static unsigned resets(const fixture_t *f, const group_t *g, uint64_t code)
{
    unsigned n = 0;
    for (size_t i = 0; i < f->ep.fake.count; i++) {
        const fake_op_t *o = &f->ep.fake.ops[i];
        if ((o->kind == FAKE_OP_RESET_AT || o->kind == FAKE_OP_RESET) &&
            o->stream_id == g->sid && o->error_code == code) n++;
    }
    return n;
}

static bool terminal(fixture_t *f, const group_t *g, uint64_t code, uint32_t cause)
{
    moq_event_t e;
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 1);
    CHECK(e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
    CHECK(e.detail_size == MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE);
    CHECK(e.u.outgoing_subgroup_terminated.sub._opaque == f->sub._opaque);
    CHECK(e.u.outgoing_subgroup_terminated.pub._opaque == f->pub._opaque);
    CHECK(e.u.outgoing_subgroup_terminated.group_id == g->group_id);
    CHECK(e.u.outgoing_subgroup_terminated.subgroup_id == g->subgroup_id);
    CHECK(e.u.outgoing_subgroup_terminated.subgroup._opaque == g->handle._opaque);
    CHECK(e.u.outgoing_subgroup_terminated.stream_ref._v == g->ref._v);
    CHECK(e.u.outgoing_subgroup_terminated.error_code == code);
    CHECK(e.u.outgoing_subgroup_terminated.cause == cause);
    moq_event_cleanup(&e);
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 0);
    return true;
}

static bool pending_stop(fixture_t *f, unsigned mask)
{
    group_t g, sibling, later;
    CHECK(open_group(f, 10, 0, &g));
    CHECK(open_group(f, 10, 1, &sibling));
    CHECK(open_group(f, 11, 0, &later));
    fake_endpoint_clear_ops(&f->ep.fake);
    fake_block_stream(&f->ep.fake, g.sid);
    CHECK(moq_session_begin_object(f->sender, g.handle, 0, 12, f->now) == MOQ_OK);
    CHECK(moq_session_write_object_data(f->sender, g.handle, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(retained_output(f, &g, false) && moq_rcbuf_refcount(f->payload) > 1);
    size_t events = mask & 2u ? fill_events(f->sender) : 0;
    if (mask & 1u)
        for (size_t i = 0; i < f->sender->action_cap; i++)
            CHECK(moq_session_write_object(f->sender, later.handle, i, f->payload, f->now) == MOQ_OK);
    f->ep.stopped = true;
    f->ep.stopped_sid = g.sid;
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, g.sid, 42, f->now) ==
          (mask ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, g.sid, 88, f->now) ==
          (mask ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(release_events(f, events));
    CHECK(service(f));
    CHECK(resets(f, &g, 42) == 1 && resets(f, &g, 88) == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(terminal(f, &g, 42, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING));
    CHECK(moq_session_write_object(f->sender, sibling.handle, 99, f->payload, f->now) == MOQ_OK);
    CHECK(moq_session_write_object(f->sender, later.handle, 99, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool fin_timeout(fixture_t *f, bool ack_first, bool bridge_recognition)
{
    group_t g, sibling;
    CHECK(open_group(f, 20, 0, &g));
    CHECK(open_group(f, 21, 0, &sibling));
    CHECK(moq_session_close_subgroup(f->sender, g.handle, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(bridge_find_by_ref(f->bridge, g.ref)->awaiting_commit);
    fake_endpoint_clear_ops(&f->ep.fake);
    uint64_t due = moq_session_next_deadline_us(f->sender);
    CHECK(due > f->now && due != UINT64_MAX);
    size_t events = fill_events(f->sender);
    if (ack_first)
        CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    fake_block_stream(&f->ep.fake, sibling.sid);
    CHECK(moq_session_write_object(f->sender, sibling.handle, 0, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(retained_output(f, &sibling, false));
    f->now = due;
    if (bridge_recognition) CHECK(service(f));
    else CHECK(moq_session_tick(f->sender, f->now) == (ack_first ? MOQ_OK : MOQ_ERR_WOULD_BLOCK));
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == (ack_first ? 0 : g.ref._v));
    f->ep.fake.block_reset_at = true;
    CHECK(release_events(f, events));
    CHECK(service(f));
    CHECK(resets(f, &g, MOQ_RESET_DELIVERY_TIMEOUT) == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == (ack_first ? 0 : g.ref._v));
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    f->ep.fake.block_reset_at = false;
    CHECK(service(f));
    CHECK(resets(f, &g, MOQ_RESET_DELIVERY_TIMEOUT) == (ack_first ? 0u : 1u));
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == 0);
    if (!ack_first) CHECK(terminal(f, &g, MOQ_RESET_DELIVERY_TIMEOUT,
                                MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT));
    else {
        moq_event_t e;
        CHECK(moq_session_poll_events(f->sender, &e, 1) == 0);
    }
    fake_unblock_stream(&f->ep.fake);
    CHECK(service(f));
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    return true;
}

static bool retained_fallback(fixture_t *f)
{
    group_t g, replacement;
    CHECK(open_group(f, 30, 0, &g));
    CHECK(moq_session_close_subgroup(f->sender, g.handle, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(open_group(f, 31, 0, &replacement));
    CHECK(moq_handle_slot(g.handle._opaque) == moq_handle_slot(replacement.handle._opaque));
    CHECK(g.handle._opaque != replacement.handle._opaque);
    fake_endpoint_clear_ops(&f->ep.fake);
    f->ep.stopped = true;
    f->ep.stopped_sid = g.sid;
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, g.sid, 17, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, g.sid, 19, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(resets(f, &g, 17) == 1 && resets(f, &g, 19) == 0);
    CHECK(moq_session_write_object(f->sender, replacement.handle, 0, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(moq_transport_bridge_find_ref(f->bridge, replacement.sid)._v == replacement.ref._v);
    moq_event_t e;
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 0);
    return true;
}

static bool pending_fin(fixture_t *f)
{
    group_t g;
    CHECK(open_group(f, 40, 0, &g));
    fake_endpoint_clear_ops(&f->ep.fake);
    fake_block_stream(&f->ep.fake, g.sid);
    CHECK(moq_session_close_subgroup(f->sender, g.handle, f->now) == MOQ_OK);
    CHECK(service(f));
    bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g.ref);
    CHECK(e && !e->awaiting_commit && retained_output(f, &g, true));
    size_t events = fill_events(f->sender);
    f->ep.stopped = true;
    f->ep.stopped_sid = g.sid;
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, g.sid, 55, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(release_events(f, events));
    CHECK(service(f));
    CHECK(resets(f, &g, 55) == 1);
    CHECK(terminal(f, &g, 55, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING));
    return true;
}

static bool reset_error(fixture_t *f)
{
    group_t g;
    CHECK(open_group(f, 50, 0, &g));
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, g.sid, 42, f->now) == MOQ_OK);
    f->ep.fail_reset = true;
    CHECK(moq_transport_bridge_service(f->bridge, f->now) == MOQ_ERR_INTERNAL);
    CHECK(moq_transport_bridge_is_fatal(f->bridge));
    CHECK(resets(f, &g, 42) == 0);
    return true;
}

static bool publication_pending_terminal(fixture_t *f)
{
    group_t stopped, fresh;
    CHECK(f->publication && moq_publication_is_valid(f->pub));
    CHECK(open_group(f, 60, 2, &stopped));
    fake_endpoint_clear_ops(&f->ep.fake);
    fake_block_stream(&f->ep.fake, stopped.sid);
    CHECK(moq_session_begin_object(f->sender, stopped.handle, 0, 12, f->now) == MOQ_OK);
    CHECK(moq_session_write_object_data(f->sender, stopped.handle, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(retained_output(f, &stopped, false) && moq_rcbuf_refcount(f->payload) > 1);
    size_t events = fill_events(f->sender);
    f->ep.stopped = true;
    f->ep.stopped_sid = stopped.sid;
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, stopped.sid, 73, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, stopped.sid, 0, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    const moq_sg_entry_t *debt = &f->sender->subgroups[moq_handle_slot(stopped.handle._opaque)];
    CHECK(debt->state == MOQ_SG_TERMINAL_OWED);
    CHECK(moq_publication_eq(debt->pub, f->pub) && !moq_subscription_is_valid(debt->sub));
    CHECK(debt->terminal_code == 73 && debt->terminal_cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING);
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, stopped.sid, f->now) == MOQ_OK);
    f->ep.fake.block_reset_at = true;
    CHECK(release_events(f, events));
    CHECK(service(f));
    CHECK(resets(f, &stopped, 73) == 0);
    bridge_stream_entry_t *mapping = bridge_find_by_ref(f->bridge, stopped.ref);
    CHECK(mapping && mapping->active && mapping->fin_reset_pending);
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, stopped.sid, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_find_ref(f->bridge, stopped.sid)._v == stopped.ref._v);
    CHECK(terminal(f, &stopped, 73, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING));
    f->ep.fake.block_reset_at = false;
    CHECK(service(f));
    CHECK(resets(f, &stopped, 73) == 1 && resets(f, &stopped, 0) == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, stopped.sid)._v == 0);
    CHECK(pub_resolve_handle(f->sender, f->pub) >= 0);
    CHECK(open_group(f, 61, 2, &fresh));
    CHECK(moq_handle_slot(fresh.handle._opaque) == moq_handle_slot(stopped.handle._opaque));
    CHECK(fresh.handle._opaque != stopped.handle._opaque && fresh.ref._v != stopped.ref._v);
    const moq_sg_entry_t *live = &f->sender->subgroups[moq_handle_slot(fresh.handle._opaque)];
    CHECK(moq_publication_eq(live->pub, f->pub) && !moq_subscription_is_valid(live->sub));
    CHECK(live->terminal_cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE && live->terminal_code == 0);
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, stopped.sid, 91, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, stopped.sid, f->now) == MOQ_OK);
    CHECK(moq_session_write_object(f->sender, stopped.handle, 0, f->payload, f->now) == MOQ_ERR_STALE_HANDLE);
    CHECK(moq_session_write_object(f->sender, fresh.handle, 0, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    unsigned payloads = 0, old_resets = 0;
    for (size_t i = 0; i < f->ep.fake.count; i++) {
        const fake_op_t *o = &f->ep.fake.ops[i];
        if (o->kind == FAKE_OP_WRITE && o->stream_id == fresh.sid &&
            o->data_len == 6 && memcmp(o->data, "object", 6) == 0) ++payloads;
        if ((o->kind == FAKE_OP_RESET || o->kind == FAKE_OP_RESET_AT) &&
            o->stream_id == stopped.sid) ++old_resets;
    }
    CHECK(payloads == 1 && old_resets == 1 && f->ep.post_stop_writes == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    moq_event_t e;
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 0);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

static const bridge_dstream_t *queued_stream(const fixture_t *f, const group_t *g)
{
    for (uint32_t i = 0; i < f->bridge->dstream_cap; i++) {
        const bridge_dstream_t *d = &f->bridge->dstreams[i];
        if (d->active && d->ref._v == g->ref._v) return d;
    }
    return NULL;
}

static unsigned queued_resets(const fixture_t *f, const group_t *g)
{
    unsigned n = 0;
    for (size_t i = f->sender->action_head; i < f->sender->action_tail; i++) {
        const moq_action_t *a = &f->sender->actions[i % f->sender->action_cap];
        if (a->kind == MOQ_ACTION_RESET_DATA &&
            a->u.reset_data.stream_ref._v == g->ref._v) n++;
    }
    return n;
}

static unsigned stream_writes(const fixture_t *f, const group_t *g)
{
    unsigned n = 0;
    for (size_t i = 0; i < f->ep.fake.count; i++)
        if (f->ep.fake.ops[i].kind == FAKE_OP_WRITE &&
            f->ep.fake.ops[i].stream_id == g->sid) n++;
    return n;
}

static unsigned stream_resets(const fixture_t *f, const group_t *g)
{
    unsigned n = 0;
    for (size_t i = 0; i < f->ep.fake.count; i++) {
        const fake_op_t *o = &f->ep.fake.ops[i];
        if ((o->kind == FAKE_OP_RESET || o->kind == FAKE_OP_RESET_AT) &&
            o->stream_id == g->sid) n++;
    }
    return n;
}

static bool open_refused_prefix(fixture_t *f, group_t *g)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = g->group_id = 80;
    sc.subgroup_id = g->subgroup_id = 0;
    sc.publisher_priority = 255; /* the ordinary sibling runs first */
    f->ep.fake.block_write = true;
    size_t before = f->ep.fake.count;
    CHECK(moq_session_open_subgroup(f->sender, f->sub, &sc, f->now, &g->handle) == MOQ_OK);
    CHECK(service(f));
    CHECK(f->ep.fake.count == before + 1);
    CHECK(f->ep.fake.ops[before].kind == FAKE_OP_OPEN_UNI);
    g->sid = f->ep.fake.ops[before].stream_id;
    g->ref = moq_transport_bridge_find_ref(f->bridge, g->sid);
    CHECK(g->ref._v != 0);
    const bridge_dstream_t *d = queued_stream(f, g);
    CHECK(d && d->q_len == 1 && d->has_sid && d->sid == g->sid);
    CHECK(d->q[d->q_head].meta.subgroup_header);
    CHECK(d->q[d->q_head].action.u.send_data.header_len > 0);
    const bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g->ref);
    CHECK(e && e->bytes_sent == 0 && e->subgroup_header_len == 0);
    return true;
}

/* The first reset belongs to the bridge after the core entry has reaped.
 * A late STOP suppresses even an unsent prefix, without changing that reset. */
static bool deferred_reset_stop(fixture_t *f, bool prefix_ready, bool application)
{
    group_t g, sibling;
    const uint64_t first_code = application ? 73 : MOQ_RESET_DELIVERY_TIMEOUT;
    CHECK(open_group(f, 79, 0, &sibling));
    CHECK(open_refused_prefix(f, &g));
    if (application) {
        CHECK(moq_session_reset_subgroup(f->sender, g.handle, first_code, f->now) == MOQ_OK);
    } else {
        CHECK(moq_session_close_subgroup(f->sender, g.handle, f->now) == MOQ_OK);
        CHECK(service(f));
        uint64_t due = moq_session_next_deadline_us(f->sender);
        CHECK(due > f->now && due != UINT64_MAX);
        f->now = due;
        CHECK(moq_session_tick(f->sender, f->now) == MOQ_OK);
    }
    CHECK(service(f));
    const bridge_dstream_t *d = queued_stream(f, &g);
    CHECK(d && d->has_reset && d->q_len == 1 && d->reset_code == first_code);
    uint64_t reliable = d->reset_reliable;
    CHECK(reliable > 0 && reliable == d->q[d->q_head].action.u.send_data.header_len);
    CHECK(d->q[d->q_head].meta.subgroup_header);
    CHECK(queued_resets(f, &g) == 0 && stream_resets(f, &g) == 0);
    CHECK(moq_session_tick(f->sender, f->now) == MOQ_OK);
    CHECK(sg_find_by_stream_ref(f->sender, g.ref) < 0);
    if (!application)
        CHECK(terminal(f, &g, first_code, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT));
    else {
        moq_event_t e;
        CHECK(moq_session_poll_events(f->sender, &e, 1) == 0);
    }

    unsigned sibling_objects = 0;
    if (prefix_ready) {
        /* Fill the actual scheduler through ordinary calls, in action-ring
         * sized batches. No queue lengths or pump counters are patched. */
        sibling_objects = BRIDGE_DRAIN_BUDGET - 1;
        for (unsigned i = 0; i < sibling_objects; i++) {
            CHECK(moq_session_write_object(f->sender, sibling.handle, i, f->payload, f->now) == MOQ_OK);
            if ((i + 1) % f->sender->action_cap == 0 || i + 1 == sibling_objects)
                CHECK(service(f));
        }
        const bridge_dstream_t *sd = queued_stream(f, &sibling);
        CHECK(sd && sd->q_len == sibling_objects);
        d = queued_stream(f, &g);
        CHECK(d && d->has_reset && d->q_len == 1 && d->reset_code == first_code);
        CHECK(f->bridge->pending_count == 0 && f->bridge->dq_items == BRIDGE_DRAIN_BUDGET);
        CHECK(moq_rcbuf_refcount(f->payload) == sibling_objects + 1);
        fake_endpoint_clear_ops(&f->ep.fake);
        f->ep.count_budget_writes = true;
        f->ep.budget_sid = sibling.sid;
        f->ep.fake.block_write = false;
        CHECK(service(f));
        f->ep.count_budget_writes = false;
        CHECK(f->ep.budget_headers == sibling_objects && f->ep.budget_payloads == sibling_objects);
        CHECK(f->bridge->pump_budget == 0);
        d = queued_stream(f, &g);
        CHECK(d && d->has_reset && d->q_len == 0 && d->reset_code == first_code);
        CHECK(d->reset_reliable == reliable);
        CHECK(stream_writes(f, &g) == 1 && stream_resets(f, &g) == 0);
        CHECK(f->ep.fake.count == 1 && f->ep.fake.ops[0].data_len == reliable);
        const bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g.ref);
        CHECK(e && e->bytes_sent == reliable && e->subgroup_header_len == reliable);
        CHECK(f->bridge->dq_items == 0 && f->bridge->dq_bytes == 0);
        CHECK(moq_rcbuf_refcount(f->payload) == 1);
    }

    fake_endpoint_clear_ops(&f->ep.fake);
    f->ep.fake.block_write = false;
    f->ep.stopped = true;
    f->ep.stopped_sid = g.sid;
    f->ep.fake.block_reset = f->ep.fake.block_reset_at = true;
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, g.sid, 42, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_on_peer_stop_sending(f->bridge, g.sid, 88, f->now) == MOQ_OK);
    unsigned fallback_resets = queued_resets(f, &g);
    CHECK(service(f));
    CHECK(stream_writes(f, &g) == 0 && stream_resets(f, &g) == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == g.ref._v);
    const bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g.ref);
    CHECK(e && e->fin_reset_pending);
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == g.ref._v);
    CHECK(moq_session_write_object(f->sender, sibling.handle, sibling_objects, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(stream_writes(f, &sibling) == 2 && stream_resets(f, &g) == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == g.ref._v);
    f->ep.fake.block_reset = f->ep.fake.block_reset_at = false;
    CHECK(service(f));
    CHECK(resets(f, &g, first_code) == 1);
    CHECK(resets(f, &g, 42) == 0 && resets(f, &g, 88) == 0 && stream_resets(f, &g) == 1);
    CHECK(fallback_resets == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == 0);
    CHECK(f->bridge->dq_items == 0 && f->bridge->dq_bytes == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    moq_event_t ev;
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

/* Object ID delta 16 begins with 0x10, but is not a subgroup prefix.
 * Expiry must suppress it while event pressure prevents reset admission. */
static bool object_header_timeout(fixture_t *f)
{
    group_t g, sibling;
    CHECK(open_group(f, 90, 0, &g));
    CHECK(open_group(f, 91, 0, &sibling));
    fake_endpoint_clear_ops(&f->ep.fake);
    fake_block_stream(&f->ep.fake, g.sid);
    CHECK(moq_session_write_object(f->sender, g.handle, 16, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(retained_output(f, &g, false));
    const bridge_dstream_t *d = queued_stream(f, &g);
    CHECK(d && d->q_len == 1 && !d->q[d->q_head].meta.subgroup_header);
    const moq_send_data_action_t *data = &d->q[d->q_head].action.u.send_data;
    CHECK(data->header_len > 0 && data->header[0] == 0x10 && data->payload == f->payload);
    const bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g.ref);
    CHECK(e && e->subgroup_header_len > 0 && e->bytes_sent == e->subgroup_header_len);
    CHECK(moq_session_close_subgroup(f->sender, g.handle, f->now) == MOQ_OK);
    CHECK(service(f));
    uint64_t due = moq_session_next_deadline_us(f->sender);
    CHECK(due > f->now && due != UINT64_MAX);
    size_t events = fill_events(f->sender);
    CHECK(events == f->sender->event_cap);
    f->now = due;
    CHECK(moq_session_tick(f->sender, f->now) == MOQ_ERR_WOULD_BLOCK);
    int slot = sg_find_by_stream_ref(f->sender, g.ref);
    CHECK(slot >= 0 && f->sender->subgroups[slot].state == MOQ_SG_TERMINAL_OWED);
    CHECK(f->sender->subgroups[slot].terminal_code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(queued_resets(f, &g) == 0 && stream_resets(f, &g) == 0);
    fake_unblock_stream(&f->ep.fake);
    CHECK(service(f));
    CHECK(stream_writes(f, &g) == 0);
    CHECK(stream_resets(f, &g) == 0 && queued_resets(f, &g) == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(f->bridge->dq_items == 0 && f->bridge->dq_bytes == 0);
    CHECK(moq_session_write_object(f->sender, sibling.handle, 0, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(stream_writes(f, &sibling) == 2 && stream_writes(f, &g) == 0);
    CHECK(stream_resets(f, &g) == 0);
    CHECK(release_events(f, events));
    f->ep.fake.block_reset = f->ep.fake.block_reset_at = true;
    CHECK(service(f));
    CHECK(stream_resets(f, &g) == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == g.ref._v);
    CHECK(terminal(f, &g, MOQ_RESET_DELIVERY_TIMEOUT, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT));
    f->ep.fake.block_reset = f->ep.fake.block_reset_at = false;
    CHECK(service(f));
    CHECK(resets(f, &g, MOQ_RESET_DELIVERY_TIMEOUT) == 1 && stream_resets(f, &g) == 1);
    CHECK(stream_writes(f, &g) == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

/* No stream mapping exists when expiry moves its reset into the bridge.
 * Opening later must recover that terminal identity until reset acceptance. */
static bool timeout_refused_open(fixture_t *f)
{
    group_t g;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = g.group_id = 100;
    sc.subgroup_id = g.subgroup_id = 0;
    f->ep.fake.block_open_uni = true;
    CHECK(moq_session_open_subgroup(f->sender, f->sub, &sc, f->now, &g.handle) == MOQ_OK);
    g.ref = f->sender->subgroups[moq_handle_slot(g.handle._opaque)].stream_ref;
    CHECK(g.ref._v != 0);
    CHECK(service(f));
    CHECK(bridge_find_by_ref(f->bridge, g.ref) == NULL);
    CHECK(f->ep.fake.count == 0 && f->ep.fake.open_uni_calls == 0);
    CHECK(f->ep.fake.block_count > 0 && f->bridge->pending_count == 1);
    const bridge_pending_item_t *p = &f->bridge->pending[0];
    CHECK(p->kind == PENDING_OPEN_UNI_DATA && p->stream_ref._v == g.ref._v);
    CHECK(!p->has_stream_id && p->meta.subgroup_header);
    size_t prefix_len = p->act.u.send_data.header_len;
    uint8_t prefix[256];
    CHECK(prefix_len > 0 && prefix_len <= sizeof(prefix));
    memcpy(prefix, p->act.u.send_data.header, prefix_len);
    CHECK(moq_session_close_subgroup(f->sender, g.handle, f->now) == MOQ_OK);
    CHECK(service(f));
    uint64_t due = moq_session_next_deadline_us(f->sender);
    CHECK(due > f->now && due != UINT64_MAX);
    f->now = due;
    CHECK(moq_session_tick(f->sender, f->now) == MOQ_OK);
    CHECK(queued_resets(f, &g) == 1);
    CHECK(service(f));
    CHECK(bridge_find_by_ref(f->bridge, g.ref) == NULL);
    CHECK(f->bridge->pending_count == 2 && queued_resets(f, &g) == 0);
    p = &f->bridge->pending[0];
    CHECK(p->kind == PENDING_OPEN_UNI_DATA && p->stream_ref._v == g.ref._v);
    CHECK(!p->has_stream_id && p->meta.subgroup_header);
    CHECK(p->act.u.send_data.header_len == prefix_len);
    CHECK(memcmp(p->act.u.send_data.header, prefix, prefix_len) == 0);
    p = &f->bridge->pending[1];
    CHECK(p->kind == PENDING_RESET_STREAM && p->uni_reset && !p->has_stream_id);
    CHECK(p->stream_ref._v == g.ref._v && p->error_code == MOQ_RESET_DELIVERY_TIMEOUT);
    CHECK(p->reliable_size == prefix_len);
    CHECK(p->meta.outgoing_terminal && p->meta.owner == f->sub._opaque);
    CHECK(f->ep.fake.count == 0 && f->ep.fake.reset_at_calls == 0);
    CHECK(moq_session_tick(f->sender, f->now) == MOQ_OK);
    CHECK(sg_find_by_stream_ref(f->sender, g.ref) < 0);
    CHECK(terminal(f, &g, MOQ_RESET_DELIVERY_TIMEOUT, MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT));

    f->ep.fake.block_reset = f->ep.fake.block_reset_at = true;
    f->ep.fake.block_open_uni = false;
    CHECK(service(f));
    CHECK(f->ep.fake.count == 2 && f->ep.fake.open_uni_calls == 1);
    CHECK(f->ep.fake.ops[0].kind == FAKE_OP_OPEN_UNI);
    g.sid = f->ep.fake.ops[0].stream_id;
    CHECK(f->ep.fake.ops[1].kind == FAKE_OP_WRITE && f->ep.fake.ops[1].stream_id == g.sid);
    CHECK(!f->ep.fake.ops[1].fin && f->ep.fake.ops[1].data_len == prefix_len);
    CHECK(memcmp(f->ep.fake.ops[1].data, prefix, prefix_len) == 0);
    CHECK(f->ep.fake.reset_at_calls > 0 && stream_resets(f, &g) == 0);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == g.ref._v);
    const bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g.ref);
    CHECK(e && e->active && e->fin_reset_pending && e->transport_id == g.sid);
    CHECK(e->data_terminal_received && !e->peer_stop_received);
    CHECK(e->bytes_sent == prefix_len && e->subgroup_header_len == prefix_len);
    CHECK(f->bridge->pending_count == 1);
    p = &f->bridge->pending[0];
    CHECK(p->kind == PENDING_RESET_STREAM && p->uni_reset && p->has_stream_id);
    CHECK(p->stream_ref._v == g.ref._v && p->stream_id == g.sid);
    CHECK(p->error_code == MOQ_RESET_DELIVERY_TIMEOUT && p->reliable_size == prefix_len);
    CHECK(p->meta.outgoing_terminal && p->meta.owner == f->sub._opaque);
    uint64_t attempts = f->ep.fake.reset_at_calls;
    CHECK(service(f));
    CHECK(f->ep.fake.reset_at_calls > attempts && f->ep.fake.count == 2);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == g.ref._v);
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == g.ref._v);
    CHECK(sg_find_by_stream_ref(f->sender, g.ref) < 0 && queued_resets(f, &g) == 0);

    f->ep.fake.block_reset = f->ep.fake.block_reset_at = false;
    CHECK(service(f));
    CHECK(resets(f, &g, MOQ_RESET_DELIVERY_TIMEOUT) == 1 && stream_resets(f, &g) == 1);
    CHECK(f->ep.fake.count == 3 && f->ep.fake.ops[2].kind == FAKE_OP_RESET_AT);
    CHECK(f->ep.fake.ops[2].reliable_size == prefix_len && stream_writes(f, &g) == 1);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == 0);
    CHECK(bridge_find_by_ref(f->bridge, g.ref) == NULL);
    CHECK(f->bridge->pending_count == 0 && f->bridge->dq_items == 0 && f->bridge->dq_bytes == 0);
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(f->ep.fake.count == 3 && moq_rcbuf_refcount(f->payload) == 1);
    moq_event_t ev;
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

/* Current-main composition: a refused open on another stream keeps this
 * prefix in the scheduler until its core owner has reaped. The direct
 * deferred-reset retry must carry the typed terminal into the new mapping.
 * The application control deliberately uses the same wire error code. */
static bool deferred_reset_provenance(fixture_t *f, bool application)
{
    group_t blocked, g;
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = blocked.group_id = 110;
    blocked.subgroup_id = 0;
    f->ep.fake.block_open_uni = true;
    CHECK(moq_session_open_subgroup(f->sender, f->sub, &sc, f->now,
                                   &blocked.handle) == MOQ_OK);
    blocked.ref = f->sender->subgroups[moq_handle_slot(blocked.handle._opaque)].stream_ref;
    CHECK(service(f));
    CHECK(f->bridge->pending_count == 1 && f->bridge->pending_opens == 1);
    CHECK(f->bridge->pending[0].kind == PENDING_OPEN_UNI_DATA);
    CHECK(f->bridge->pending[0].stream_ref._v == blocked.ref._v);
    CHECK(f->ep.fake.block_count > 0 && f->ep.fake.open_uni_calls == 0);

    sc.group_id = g.group_id = 111;
    g.subgroup_id = 0;
    CHECK(moq_session_open_subgroup(f->sender, f->sub, &sc, f->now, &g.handle) == MOQ_OK);
    g.ref = f->sender->subgroups[moq_handle_slot(g.handle._opaque)].stream_ref;
    CHECK(service(f));
    const bridge_dstream_t *d = queued_stream(f, &g);
    CHECK(d && d->q_len == 1 && !d->has_sid && !d->has_reset);
    CHECK(d->q[d->q_head].meta.subgroup_header);
    CHECK(d->q[d->q_head].meta.owner == f->sub._opaque);
    CHECK(bridge_find_by_ref(f->bridge, g.ref) == NULL);
    size_t prefix_len = d->q[d->q_head].action.u.send_data.header_len;
    uint8_t prefix[256];
    CHECK(prefix_len > 0 && prefix_len <= sizeof(prefix));
    memcpy(prefix, d->q[d->q_head].action.u.send_data.header, prefix_len);

    if (application) {
        CHECK(moq_session_reset_subgroup(f->sender, g.handle,
                                        MOQ_RESET_DELIVERY_TIMEOUT, f->now) == MOQ_OK);
    } else {
        CHECK(moq_session_close_subgroup(f->sender, g.handle, f->now) == MOQ_OK);
        CHECK(service(f));
        uint64_t due = moq_session_next_deadline_us(f->sender);
        CHECK(due > f->now && due != UINT64_MAX);
        f->now = due;
        CHECK(moq_session_tick(f->sender, f->now) == MOQ_OK);
    }
    CHECK(service(f));
    CHECK(moq_session_tick(f->sender, f->now) == MOQ_OK);
    CHECK(sg_find_by_stream_ref(f->sender, g.ref) < 0 && queued_resets(f, &g) == 0);
    CHECK(bridge_find_by_ref(f->bridge, g.ref) == NULL);
    d = queued_stream(f, &g);
    CHECK(d && d->q_len == 1 && d->has_reset && d->reset_terminal == !application);
    CHECK(d->reset_code == MOQ_RESET_DELIVERY_TIMEOUT && d->reset_reliable == prefix_len);
    CHECK(d->key.owner == f->sub._opaque);
    CHECK(f->bridge->dreset_count == 1 && f->bridge->dq_timed == 0);
    if (!application)
        CHECK(terminal(f, &g, MOQ_RESET_DELIVERY_TIMEOUT,
                       MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT));
    else {
        moq_event_t ev;
        CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);
    }

    /* Open now succeeds, but the required prefix still cannot be written.
     * There is no core terminal or reset left to reconstruct the type. */
    f->ep.fake.block_write = true;
    f->ep.fake.block_reset_at = true;
    f->ep.fake.block_open_uni = false;
    CHECK(service(f));
    const bridge_stream_entry_t *e = bridge_find_by_ref(f->bridge, g.ref);
    CHECK(e && e->bytes_sent == 0 && e->subgroup_header_len == 0);
    g.sid = e->transport_id;
    d = queued_stream(f, &g);
    CHECK(d && d->has_sid && d->sid == g.sid && d->q_len == 1 && d->has_reset);
    CHECK(d->reset_terminal == !application && f->bridge->dreset_count == 1);
    CHECK(f->ep.fake.open_uni_calls == 2 && f->ep.fake.reset_at_calls == 0);
    CHECK(stream_writes(f, &g) == 0 && !e->peer_stop_received);

    f->ep.fake.block_write = false;
    CHECK(service(f));
    e = bridge_find_by_ref(f->bridge, g.ref);
    CHECK(e && e->fin_reset_pending && e->data_terminal_received == !application);
    CHECK(!e->peer_stop_received && e->bytes_sent == prefix_len &&
          e->subgroup_header_len == prefix_len);
    d = queued_stream(f, &g);
    CHECK(d && d->q_len == 0 && d->has_reset && d->reset_terminal == !application);
    CHECK(d->reset_code == MOQ_RESET_DELIVERY_TIMEOUT && d->reset_reliable == prefix_len);
    CHECK(f->bridge->pending_count == 0 && f->bridge->dq_items == 0 &&
          f->bridge->dq_bytes == 0 && f->bridge->dq_timed == 0);
    CHECK(f->bridge->dreset_count == 1 && f->ep.fake.reset_at_calls > 0);
    CHECK(stream_resets(f, &g) == 0 && stream_writes(f, &g) == 1);
    for (size_t i = 0; i < f->ep.fake.count; i++) {
        const fake_op_t *op = &f->ep.fake.ops[i];
        if (op->kind == FAKE_OP_WRITE && op->stream_id == g.sid) {
            CHECK(!op->fin && op->data_len == prefix_len);
            CHECK(memcmp(op->data, prefix, prefix_len) == 0);
        }
    }

    uint64_t attempts = f->ep.fake.reset_at_calls;
    CHECK(moq_transport_bridge_on_local_stream_committed(f->bridge, g.sid, f->now) == MOQ_OK);
    CHECK(moq_transport_bridge_find_ref(f->bridge, g.sid)._v == g.ref._v);
    e = bridge_find_by_ref(f->bridge, blocked.ref);
    CHECK(e != NULL);
    blocked.sid = e->transport_id;
    unsigned other_writes = stream_writes(f, &blocked);
    CHECK(moq_session_write_object(f->sender, blocked.handle, 0, f->payload, f->now) == MOQ_OK);
    CHECK(service(f));
    CHECK(f->ep.fake.reset_at_calls > attempts && stream_resets(f, &g) == 0);
    CHECK(stream_writes(f, &blocked) > other_writes);
    CHECK(f->bridge->dreset_count == 1 && moq_rcbuf_refcount(f->payload) == 1);
    moq_event_t ev;
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);

    f->ep.fake.block_reset_at = false;
    CHECK(service(f));
    CHECK(resets(f, &g, MOQ_RESET_DELIVERY_TIMEOUT) == 1 && stream_resets(f, &g) == 1);
    unsigned reliable_resets = 0;
    for (size_t i = 0; i < f->ep.fake.count; i++) {
        const fake_op_t *op = &f->ep.fake.ops[i];
        if (op->kind == FAKE_OP_RESET_AT && op->stream_id == g.sid) {
            CHECK(op->reliable_size == prefix_len);
            reliable_resets++;
        }
    }
    CHECK(reliable_resets == 1);
    CHECK(bridge_find_by_ref(f->bridge, g.ref) == NULL && queued_stream(f, &g) == NULL);
    CHECK(f->bridge->dreset_count == 0 && f->bridge->dq_timed == 0);
    CHECK(service(f));
    CHECK(stream_writes(f, &g) == 1 && stream_resets(f, &g) == 1);
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++)
        for (unsigned client = 0; client < 2; client++) {
            fixture_t f;
            for (unsigned mask = 0; mask < 4; mask++) {
                if (setup(&f, versions[v], client != 0, false)) (void)pending_stop(&f, mask);
                (void)cleanup(&f);
            }
            if (setup(&f, versions[v], client != 0, false)) (void)retained_fallback(&f);
            (void)cleanup(&f);
            if (setup(&f, versions[v], client != 0, false)) (void)reset_error(&f);
            (void)cleanup(&f);
            if (v != 0) {
                if (setup(&f, versions[v], client != 0, true)) (void)pending_fin(&f);
                (void)cleanup(&f);
                for (unsigned ack = 0; ack < 2; ack++)
                    for (unsigned bridge = 0; bridge < 2; bridge++) {
                        if (setup(&f, versions[v], client != 0, true))
                            (void)fin_timeout(&f, ack != 0, bridge != 0);
                        (void)cleanup(&f);
                    }
            }
        }
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++) {
        fixture_t f;
        printf("publication_pending_terminal/version%u\n", (unsigned)versions[v]);
        if (setup_owner(&f, versions[v], true, false, true)) (void)publication_pending_terminal(&f);
        (void)cleanup(&f);
    }
    for (unsigned row = 0; row < 3; row++) {
        fixture_t f;
        printf("deferred-reset/%s\n", row == 0 ? "timeout-before-prefix" :
               row == 1 ? "timeout-prefix-last-budget" : "application-prefix-last-budget");
        if (setup(&f, MOQ_VERSION_DRAFT_18, true, true))
            (void)deferred_reset_stop(&f, row != 0, row == 2);
        (void)cleanup(&f);
    }
    {
        fixture_t f;
        printf("object-header-0x10/event-pressure\n");
        if (setup(&f, MOQ_VERSION_DRAFT_18, true, true)) (void)object_header_timeout(&f);
        (void)cleanup(&f);
    }
    {
        fixture_t f;
        printf("deferred-reset/timeout-refused-open-reset-retry\n");
        if (setup(&f, MOQ_VERSION_DRAFT_18, true, true)) (void)timeout_refused_open(&f);
        (void)cleanup(&f);
    }
    for (unsigned application = 0; application < 2; application++) {
        fixture_t f;
        printf("integration/deferred-reset/%s/refused-open-prefix-reset\n",
               application ? "application-code-2" : "timeout");
        if (setup(&f, MOQ_VERSION_DRAFT_18, true, true))
            (void)deferred_reset_provenance(&f, application != 0);
        (void)cleanup(&f);
    }
    printf("%u assertions, %u failures\n", checks, failures);
    return failures ? 1 : 0;
}
