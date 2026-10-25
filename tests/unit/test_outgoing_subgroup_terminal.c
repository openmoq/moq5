#include <moq/moq.h>
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include "../../core/src/bridge/transport_bridge_internal.h"
#include "test_alloc_support.h"
#include <stdio.h>
#include <string.h>

static unsigned checks, failures;
#define CHECK(x) do { ++checks; if (!(x)) { ++failures; \
    fprintf(stderr, "%s:%d: %s\n", __func__, __LINE__, #x); return false; } } while (0)

typedef struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *sender, *peer;
    moq_subscription_t sub;
    moq_subscription_t peer_sub;
    moq_publication_t pub;
    moq_publication_t peer_pub;
    moq_rcbuf_t *payload;
    uint64_t now;
    bool publication;
} fixture_t;

static void drain_events(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
}

static bool setup_delivery_timeout(fixture_t *f, moq_version_t version,
                                     bool publication, unsigned timeout_kind)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->memory);
    f->publication = publication;
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
    moq_session_t *client = moq_simpair_client(f->pair);
    moq_session_t *server = moq_simpair_server(f->pair);
    f->sender = publication ? client : server;
    f->peer = publication ? server : client;
    drain_events(client);
    drain_events(server);
    f->now = moq_simpair_now_us(f->pair);
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    static const uint8_t object_timeout[] = {0x02, 0x01};
    static const uint8_t subgroup_timeout[] = {0x06, 0x01};
    moq_bytes_t props = timeout_kind == 2
        ? (moq_bytes_t){subgroup_timeout, sizeof(subgroup_timeout)}
        : timeout_kind == 1
        ? (moq_bytes_t){object_timeout, sizeof(object_timeout)}
        : (moq_bytes_t){NULL, 0};
    moq_event_t ev;
    if (publication) {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        pc.track_namespace = (moq_namespace_t){&ns, 1};
        pc.track_name = MOQ_BYTES_LITERAL("v");
        pc.track_properties = props;
        CHECK(moq_session_publish(client, &pc, f->now, &f->pub) == MOQ_OK);
        CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
        CHECK(moq_session_poll_events(server, &ev, 1) == 1);
        CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
        moq_publication_t owner = ev.u.publish_request.pub;
        f->peer_pub = owner;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        CHECK(moq_session_accept_publish(server, owner, &ac, f->now) == MOQ_OK);
    } else {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
        sc.track_namespace = (moq_namespace_t){&ns, 1};
        sc.track_name = MOQ_BYTES_LITERAL("v");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t local;
        CHECK(moq_session_subscribe(client, &sc, f->now, &local) == MOQ_OK);
        f->peer_sub = local;
        CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
        CHECK(moq_session_poll_events(server, &ev, 1) == 1);
        CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        f->sub = ev.u.subscribe_request.sub;
        moq_event_cleanup(&ev);
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        ac.track_properties = props;
        CHECK(moq_session_accept_subscribe(server, f->sub, &ac, f->now) == MOQ_OK);
    }
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    drain_events(client);
    drain_events(server);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &f->payload) == MOQ_OK);
    return true;
}

static bool setup(fixture_t *f, moq_version_t version, bool publication,
                   bool object_timeout)
{
    return setup_delivery_timeout(f, version, publication, object_timeout ? 1u : 0u);
}

static bool cleanup(fixture_t *f)
{
    moq_rcbuf_decref(f->payload);
    moq_simpair_destroy(f->pair);
    CHECK(f->memory.balance == 0);
    return true;
}

static bool open_group_priority(fixture_t *f, uint64_t group, uint8_t priority,
                                moq_subgroup_handle_t *h, moq_stream_ref_t *ref)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = group;
    sc.subgroup_id = 7;
    sc.publisher_priority = priority;
    moq_result_t rc = f->publication
        ? moq_session_open_pub_subgroup(f->sender, f->pub, &sc, f->now, h)
        : moq_session_open_subgroup(f->sender, f->sub, &sc, f->now, h);
    CHECK(rc == MOQ_OK);
    moq_action_t a;
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    CHECK(a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.header_len != 0);
    *ref = a.u.send_data.stream_ref;
    moq_action_cleanup(&a);
    return true;
}

static bool open_group(fixture_t *f, uint64_t group, moq_subgroup_handle_t *h,
                        moq_stream_ref_t *ref)
{
    return open_group_priority(f, group, 0, h, ref);
}

/* Fill only notification storage: scalar pre-existing events own no payload.
 * Public polls, not fixture writes, must release the pressure. */
static size_t fill_events(moq_session_t *s)
{
    size_t n = 0;
    while (s->event_tail - s->event_head < s->event_cap) {
        moq_event_t *e = &s->events[s->event_tail++ % s->event_cap];
        memset(e, 0, sizeof(*e));
        e->kind = MOQ_EVENT_GOAWAY;
        ++n;
    }
    return n;
}

static bool release_events(moq_session_t *s, size_t n)
{
    while (n--) {
        moq_event_t e;
        CHECK(moq_session_poll_events(s, &e, 1) == 1);
        CHECK(e.kind == MOQ_EVENT_GOAWAY);
        moq_event_cleanup(&e);
    }
    return true;
}

static bool terminal(fixture_t *f, moq_subgroup_handle_t old,
                       moq_stream_ref_t ref, uint64_t code, uint32_t cause)
{
    moq_event_t e;
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 1);
    CHECK(e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
    CHECK(e.detail_size == MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE);
    moq_outgoing_subgroup_terminated_event_t d = e.u.outgoing_subgroup_terminated;
    CHECK(d.subgroup._opaque == old._opaque && d.stream_ref._v == ref._v);
    CHECK(d.sub._opaque == f->sub._opaque && d.pub._opaque == f->pub._opaque);
    CHECK(d.subgroup_id == 7 && d.error_code == code && d.cause == cause);
    moq_event_cleanup(&e);
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 0);
    return true;
}

static bool pressure(fixture_t *f, unsigned mask)
{
    moq_subgroup_handle_t h, sibling;
    moq_stream_ref_t ref, sr;
    CHECK(open_group(f, 4, &h, &ref));
    CHECK(open_group(f, 5, &sibling, &sr));
    CHECK(moq_session_begin_object(f->sender, h, 0, 6, f->now) == MOQ_OK);
    moq_action_t a;
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    moq_action_cleanup(&a);
    size_t events = (mask & 2u) ? fill_events(f->sender) : 0;
    if (mask & 1u)
        for (size_t i = 0; i < f->sender->action_cap; i++)
            CHECK(moq_session_write_object(f->sender, sibling, i, f->payload, f->now) == MOQ_OK);
    CHECK(moq_session_on_data_stop(f->sender, ref, 0, f->now) ==
          (mask ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
    if (mask) {
        uint32_t refs = moq_rcbuf_refcount(f->payload);
        CHECK(moq_session_on_data_stop(f->sender, ref, 99, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_session_write_object_data(f->sender, h, f->payload, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_session_write_object(f->sender, h, 1, f->payload, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_session_end_object(f->sender, h, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_session_close_subgroup(f->sender, h, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_session_reset_subgroup(f->sender, h, 33, f->now) == MOQ_ERR_WOULD_BLOCK);
        if (f->publication) {
            moq_finish_publish_cfg_t fc;
            moq_finish_publish_cfg_init(&fc);
            CHECK(moq_session_finish_publish(f->sender, f->pub, &fc, f->now) == MOQ_ERR_WOULD_BLOCK);
        } else {
            moq_done_subscribe_cfg_t dc;
            moq_done_subscribe_cfg_init(&dc);
            CHECK(moq_session_done_subscribe(f->sender, f->sub, &dc, f->now) == MOQ_ERR_WOULD_BLOCK);
        }
        CHECK(moq_rcbuf_refcount(f->payload) == refs);
    }
    unsigned resets = 0;
    while (moq_session_poll_actions(f->sender, &a, 1)) {
        if (a.kind == MOQ_ACTION_RESET_DATA) {
            CHECK(a.u.reset_data.stream_ref._v == ref._v && a.u.reset_data.error_code == 0);
            ++resets;
        }
        moq_action_cleanup(&a);
    }
    CHECK(release_events(f->sender, events));
    while (moq_session_poll_actions(f->sender, &a, 1)) {
        CHECK(a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.stream_ref._v == ref._v);
        CHECK(a.u.reset_data.error_code == 0);
        ++resets;
        moq_action_cleanup(&a);
    }
    CHECK(resets == 1 && moq_rcbuf_refcount(f->payload) == 1);
    CHECK(terminal(f, h, ref, 0, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING));
    CHECK(moq_session_write_object(f->sender, sibling, 99, f->payload, f->now) == MOQ_OK);
    return true;
}

static bool expiry_order(fixture_t *f, bool stop_first)
{
    moq_subgroup_handle_t h;
    moq_stream_ref_t ref;
    CHECK(open_group(f, 8, &h, &ref));
    CHECK(moq_session_write_object(f->sender, h, 0, f->payload, f->now) == MOQ_OK);
    CHECK(moq_session_close_subgroup(f->sender, h, f->now) == MOQ_OK);
    if (!f->publication) {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.stream_count = 1;
        CHECK(moq_session_done_subscribe(f->sender, f->sub, &dc, f->now) == MOQ_OK);
    } else {
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        fc.stream_count = 1;
        CHECK(moq_session_finish_publish(f->sender, f->pub, &fc, f->now) == MOQ_OK);
    }
    size_t events = fill_events(f->sender);
    if (stop_first)
        CHECK(moq_session_on_data_stop(f->sender, ref, 37, f->now) == MOQ_ERR_WOULD_BLOCK);
    f->now += 1000;
    CHECK(moq_session_tick(f->sender, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_session_on_data_committed(f->sender, ref, f->now) == MOQ_OK);
    CHECK(moq_session_on_data_stop(f->sender, ref, 91, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    moq_action_t a;
    size_t n = 99;
    CHECK(moq_session_poll_actions_ex(f->sender, &a, 1, sizeof(a), &n) == MOQ_ERR_WOULD_BLOCK);
    CHECK(n == 0);
    CHECK(moq_session_next_deadline_us(f->sender) > f->now);
    CHECK(release_events(f->sender, events));
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    CHECK(a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.stream_ref._v == ref._v);
    CHECK(a.u.reset_data.error_code == (stop_first ? 37 : MOQ_RESET_DELIVERY_TIMEOUT));
    moq_action_cleanup(&a);
    unsigned done = 0;
    while (moq_session_poll_actions(f->sender, &a, 1)) {
        CHECK(a.kind != MOQ_ACTION_SEND_DATA && a.kind != MOQ_ACTION_RESET_DATA);
        ++done;
        moq_action_cleanup(&a);
    }
    CHECK(done == 1);
    CHECK(terminal(f, h, ref, stop_first ? 37 : MOQ_RESET_DELIVERY_TIMEOUT,
          stop_first ? MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING
                     : MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT));
    return true;
}

static bool poll_terminal_action(moq_session_t *s, moq_action_t *out,
                                  bool bridge_poll)
{
    moq_action_meta_t meta;
    return bridge_poll ? bridge_session_poll_action(s, out, &meta)
                       : moq_session_poll_actions(s, out, 1) == 1;
}

/* The transport already owns FIN. No residual target DATA can provide an
 * in-ring barrier when the still-uncommitted stream later ends by reset. */
static bool polled_fin_owner_done(fixture_t *f, bool timeout, bool full_actions,
                                  bool bridge_poll)
{
    moq_subgroup_handle_t h;
    moq_stream_ref_t ref;
    CHECK(open_group(f, 24, &h, &ref));
    CHECK(moq_session_close_subgroup(f->sender, h, f->now) == MOQ_OK);
    moq_action_t a;
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    CHECK(a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin);
    CHECK(a.u.send_data.stream_ref._v == ref._v);
    moq_action_cleanup(&a);
    CHECK(f->sender->action_head == f->sender->action_tail);
    uint64_t due = moq_session_next_deadline_us(f->sender);
    CHECK(due > f->now && due != UINT64_MAX);
    if (f->publication) {
        moq_finish_publish_cfg_t fc;
        moq_finish_publish_cfg_init(&fc);
        fc.stream_count = 1;
        CHECK(moq_session_finish_publish(f->sender, f->pub, &fc, f->now) == MOQ_OK);
    } else {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.stream_count = 1;
        CHECK(moq_session_done_subscribe(f->sender, f->sub, &dc, f->now) == MOQ_OK);
    }
    size_t head = f->sender->action_head;
    const moq_action_t *done = &f->sender->actions[head % f->sender->action_cap];
    CHECK(done->kind == MOQ_ACTION_SEND_BIDI_STREAM && done->u.send_bidi_stream.fin);
    moq_stream_ref_t owner_ref = done->u.send_bidi_stream.stream_ref;

    /* Real, independent requests occupy output behind DONE without leaving
     * target-stream bytes or expiring datagrams that could mask full pressure.
     * The first request also reuses the freed owner's pool slot. */
    size_t requests = full_actions ? f->sender->action_cap - 1 : 0;
    for (size_t i = 0; i < requests; i++) {
        char name[32];
        int len = snprintf(name, sizeof(name), "independent-%zu", i);
        CHECK(len > 0 && (size_t)len < sizeof(name));
        moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
        moq_bytes_t track_name = {(const uint8_t *)name, (size_t)len};
        uint64_t old_owner = f->publication ? f->pub._opaque : f->sub._opaque;
        uint64_t new_owner;
        if (f->publication) {
            moq_publish_cfg_t pc;
            moq_publish_cfg_init_sized(&pc, sizeof(pc));
            pc.track_namespace = (moq_namespace_t){&ns, 1};
            pc.track_name = track_name;
            moq_publication_t replacement;
            CHECK(moq_session_publish(f->sender, &pc, f->now, &replacement) == MOQ_OK);
            new_owner = replacement._opaque;
        } else {
            moq_subscribe_cfg_t sc;
            moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
            sc.track_namespace = (moq_namespace_t){&ns, 1};
            sc.track_name = track_name;
            sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
            moq_subscription_t replacement;
            CHECK(moq_session_subscribe(f->sender, &sc, f->now, &replacement) == MOQ_OK);
            new_owner = replacement._opaque;
        }
        if (i == 0) {
            CHECK(moq_handle_slot(old_owner) == moq_handle_slot(new_owner));
            CHECK(moq_handle_generation(old_owner) != moq_handle_generation(new_owner));
        }
    }
    size_t tail = f->sender->action_tail;
    CHECK(tail - head == 1 + requests);
    CHECK(moq_session_action_capacity(f->sender) ==
          (full_actions ? 0 : f->sender->action_cap - 1));
    for (size_t i = head; i < tail; i++) {
        const moq_action_t *queued = &f->sender->actions[i % f->sender->action_cap];
        CHECK(queued->kind != MOQ_ACTION_SEND_DATA && queued->kind != MOQ_ACTION_RESET_DATA);
    }
    size_t events = fill_events(f->sender);
    uint64_t code = timeout ? MOQ_RESET_DELIVERY_TIMEOUT : 61;
    uint32_t cause = timeout ? MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT
                            : MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING;
    if (timeout) {
        f->now = due;
        CHECK(moq_session_tick(f->sender, f->now) == MOQ_ERR_WOULD_BLOCK);
    } else {
        CHECK(moq_session_on_data_stop(f->sender, ref, code, f->now) == MOQ_ERR_WOULD_BLOCK);
    }
    CHECK(f->sender->subgroups[moq_handle_slot(h._opaque)].state == MOQ_SG_TERMINAL_OWED);
    CHECK(moq_session_on_data_committed(f->sender, ref, f->now) == MOQ_OK);
    size_t n = 99;
    memset(&a, 0xa5, sizeof(a));
    if (bridge_poll) {
        CHECK(!poll_terminal_action(f->sender, &a, true));
        n = 0;
    } else {
        CHECK(moq_session_poll_actions_ex(f->sender, &a, 1, sizeof(a), &n) == MOQ_ERR_WOULD_BLOCK);
    }
    CHECK(n == 0 && f->sender->action_head == head && f->sender->action_tail == tail);
    const uint8_t *bytes = (const uint8_t *)&a;
    for (size_t i = 0; i < sizeof(a); i++) CHECK(bytes[i] == 0xa5);
    CHECK(moq_session_next_deadline_us(f->sender) > f->now);
    CHECK(release_events(f->sender, events));
    CHECK(poll_terminal_action(f->sender, &a, bridge_poll));
    CHECK(a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.stream_ref._v == ref._v);
    CHECK(a.u.reset_data.error_code == code);
    moq_action_cleanup(&a);
    CHECK(poll_terminal_action(f->sender, &a, bridge_poll));
    CHECK(a.kind == MOQ_ACTION_SEND_BIDI_STREAM && a.u.send_bidi_stream.fin);
    CHECK(a.u.send_bidi_stream.stream_ref._v == owner_ref._v);
    moq_action_cleanup(&a);
    size_t drained = 0;
    while (moq_session_poll_actions(f->sender, &a, 1)) {
        CHECK(a.kind == MOQ_ACTION_OPEN_BIDI_STREAM);
        ++drained;
        moq_action_cleanup(&a);
    }
    CHECK(drained == requests);
    CHECK(terminal(f, h, ref, code, cause));
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool unrelated_done_progress(fixture_t *f, bool timeout)
{
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    moq_publish_cfg_t pc;
    moq_publish_cfg_init_sized(&pc, sizeof(pc));
    pc.track_namespace = (moq_namespace_t){&ns, 1};
    pc.track_name = MOQ_BYTES_LITERAL("independent-done");
    moq_publication_t other;
    CHECK(moq_session_publish(f->sender, &pc, f->now, &other) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    moq_event_t e;
    CHECK(moq_session_poll_events(f->peer, &e, 1) == 1);
    CHECK(e.kind == MOQ_EVENT_PUBLISH_REQUEST);
    moq_publication_t peer_owner = e.u.publish_request.pub;
    moq_event_cleanup(&e);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    CHECK(moq_session_accept_publish(f->peer, peer_owner, &ac, f->now) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    drain_events(f->sender);
    drain_events(f->peer);

    moq_subgroup_handle_t h;
    moq_stream_ref_t ref;
    CHECK(open_group(f, 25, &h, &ref));
    CHECK(moq_session_close_subgroup(f->sender, h, f->now) == MOQ_OK);
    moq_action_t a[2];
    CHECK(moq_session_poll_actions(f->sender, a, 1) == 1);
    CHECK(a[0].kind == MOQ_ACTION_SEND_DATA && a[0].u.send_data.fin);
    CHECK(a[0].u.send_data.stream_ref._v == ref._v);
    moq_action_cleanup(&a[0]);
    uint64_t due = moq_session_next_deadline_us(f->sender);
    CHECK(due > f->now && due != UINT64_MAX);
    moq_finish_publish_cfg_t fc;
    moq_finish_publish_cfg_init(&fc);
    CHECK(moq_session_finish_publish(f->sender, other, &fc, f->now) == MOQ_OK);
    const moq_action_t *first =
        &f->sender->actions[f->sender->action_head % f->sender->action_cap];
    CHECK(first->kind == MOQ_ACTION_SEND_BIDI_STREAM && first->u.send_bidi_stream.fin);
    moq_stream_ref_t other_ref = first->u.send_bidi_stream.stream_ref;
    if (f->publication) {
        fc.stream_count = 1;
        CHECK(moq_session_finish_publish(f->sender, f->pub, &fc, f->now) == MOQ_OK);
    } else {
        moq_done_subscribe_cfg_t dc;
        moq_done_subscribe_cfg_init(&dc);
        dc.stream_count = 1;
        CHECK(moq_session_done_subscribe(f->sender, f->sub, &dc, f->now) == MOQ_OK);
    }
    const moq_action_t *second =
        &f->sender->actions[(f->sender->action_tail - 1) % f->sender->action_cap];
    CHECK(second->kind == MOQ_ACTION_SEND_BIDI_STREAM && second->u.send_bidi_stream.fin);
    moq_stream_ref_t owner_ref = second->u.send_bidi_stream.stream_ref;
    CHECK(other_ref._v != owner_ref._v);
    size_t events = fill_events(f->sender);
    uint64_t code = timeout ? MOQ_RESET_DELIVERY_TIMEOUT : 62;
    if (timeout) {
        f->now = due;
        CHECK(moq_session_tick(f->sender, f->now) == MOQ_ERR_WOULD_BLOCK);
    } else {
        CHECK(moq_session_on_data_stop(f->sender, ref, code, f->now) == MOQ_ERR_WOULD_BLOCK);
    }
    CHECK(moq_session_on_data_committed(f->sender, ref, f->now) == MOQ_OK);
    memset(a, 0xa5, sizeof(a));
    size_t n = 99;
    CHECK(moq_session_poll_actions_ex(f->sender, a, 2, sizeof(a[0]), &n) == MOQ_OK);
    CHECK(n == 1);
    CHECK(a[0].kind == MOQ_ACTION_SEND_BIDI_STREAM && a[0].u.send_bidi_stream.fin);
    CHECK(a[0].u.send_bidi_stream.stream_ref._v == other_ref._v);
    moq_action_cleanup(&a[0]);
    const uint8_t *bytes = (const uint8_t *)&a[1];
    for (size_t i = 0; i < sizeof(a[1]); i++) CHECK(bytes[i] == 0xa5);
    CHECK(moq_session_poll_actions_ex(f->sender, a, 1, sizeof(a[0]), &n) == MOQ_ERR_WOULD_BLOCK);
    CHECK(n == 0);
    CHECK(release_events(f->sender, events));
    CHECK(moq_session_poll_actions(f->sender, a, 1) == 1);
    CHECK(a[0].kind == MOQ_ACTION_RESET_DATA && a[0].u.reset_data.stream_ref._v == ref._v);
    CHECK(a[0].u.reset_data.error_code == code);
    moq_action_cleanup(&a[0]);
    CHECK(moq_session_poll_actions(f->sender, a, 1) == 1);
    CHECK(a[0].kind == MOQ_ACTION_SEND_BIDI_STREAM && a[0].u.send_bidi_stream.fin);
    CHECK(a[0].u.send_bidi_stream.stream_ref._v == owner_ref._v);
    moq_action_cleanup(&a[0]);
    CHECK(moq_session_poll_actions(f->sender, a, 1) == 0);
    CHECK(terminal(f, h, ref, code,
          timeout ? MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT
                  : MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING));
    return true;
}

static bool sized_identity(fixture_t *f)
{
    moq_subgroup_handle_t h, replacement;
    moq_stream_ref_t ref, rr;
    CHECK(open_group(f, 12, &h, &ref));
    CHECK(moq_session_on_data_stop(f->sender, ref, 11, f->now) == MOQ_OK);
    moq_action_t a;
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    moq_action_cleanup(&a);
    CHECK(open_group(f, 13, &replacement, &rr));
    CHECK(moq_handle_slot(h._opaque) == moq_handle_slot(replacement._opaque));
    CHECK(h._opaque != replacement._opaque && ref._v != rr._v);
    size_t head = f->sender->event_head, n = 99;
    uint8_t bytes[sizeof(moq_event_t)];
    memset(bytes, 0xa5, sizeof(bytes));
    size_t short_size = offsetof(moq_event_t, u) + MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE - 1;
    CHECK(moq_session_poll_events_ex(f->sender, bytes, 1, short_size, &n) == MOQ_ERR_ABI_MISMATCH);
    CHECK(n == 0 && f->sender->event_head == head);
    for (size_t i = 0; i < sizeof(bytes); i++) CHECK(bytes[i] == 0xa5);
    CHECK(terminal(f, h, ref, 11, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING));
    moq_subgroup_handle_t foreign = {h._opaque ^ (UINT64_C(1) << 44)};
    CHECK(moq_session_write_object(f->sender, foreign, 0, f->payload, f->now) == MOQ_ERR_STALE_HANDLE);
    CHECK(moq_session_on_data_stop(f->sender, ref, 77, f->now) == MOQ_OK);
    CHECK(moq_session_write_object(f->sender, replacement, 0, f->payload, f->now) == MOQ_OK);
    moq_event_t ev;
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);
    return true;
}

static bool nonhead_priority_barrier(fixture_t *f, unsigned poll_mode)
{
    moq_subgroup_handle_t low, high;
    moq_stream_ref_t lr, hr;
    CHECK(open_group_priority(f, 20, 120, &low, &lr));
    CHECK(open_group_priority(f, 21, 0, &high, &hr));
    CHECK(moq_session_write_object(f->sender, low, 0, f->payload, f->now) == MOQ_OK);
    CHECK(moq_session_write_object(f->sender, high, 0, f->payload, f->now) == MOQ_OK);
    CHECK(moq_rcbuf_refcount(f->payload) == 3);
    size_t events = fill_events(f->sender);
    CHECK(moq_session_on_data_stop(f->sender, hr, 63, f->now) == MOQ_ERR_WOULD_BLOCK);
    size_t head = f->sender->action_head, tail = f->sender->action_tail;
    size_t cap = f->sender->action_cap;
    CHECK(tail - head == 2);
    CHECK(f->sender->actions[head % cap].u.send_data.stream_ref._v == lr._v);
    CHECK(!f->sender->action_meta[head % cap].terminal_barrier);
    CHECK(f->sender->action_meta[(head + 1) % cap].terminal_barrier);
    CHECK(f->sender->action_meta[(head + 1) % cap].publisher_priority <
          f->sender->action_meta[head % cap].publisher_priority);
    CHECK(moq_rcbuf_refcount(f->payload) == 2);
    moq_action_t a;
    memset(&a, 0xa5, sizeof(a));
    size_t n = 99;
    if (poll_mode == 0) {
        CHECK(moq_session_poll_actions_ex(f->sender, &a, 1, sizeof(a), &n) == MOQ_ERR_WOULD_BLOCK);
    } else {
        moq_action_meta_t meta;
        CHECK(!(poll_mode == 2
            ? bridge_session_poll_leading_data(f->sender, &a, &meta)
            : bridge_session_poll_action(f->sender, &a, &meta)));
        n = 0;
    }
    CHECK(n == 0 && f->sender->action_head == head && f->sender->action_tail == tail);
    const uint8_t *bytes = (const uint8_t *)&a;
    for (size_t i = 0; i < sizeof(a); i++) CHECK(bytes[i] == 0xa5);
    CHECK(release_events(f->sender, events));
    CHECK(poll_terminal_action(f->sender, &a, poll_mode != 0));
    CHECK(a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.stream_ref._v == lr._v);
    moq_action_cleanup(&a);
    CHECK(poll_terminal_action(f->sender, &a, poll_mode != 0));
    CHECK(a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.stream_ref._v == hr._v);
    CHECK(a.u.reset_data.error_code == 63);
    moq_action_cleanup(&a);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(terminal(f, high, hr, 63, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING));
    return true;
}

static bool local_terminals(fixture_t *f)
{
    moq_subgroup_handle_t h;
    moq_stream_ref_t ref;
    CHECK(open_group(f, 15, &h, &ref));
    CHECK(moq_session_reset_subgroup(f->sender, h, 44, f->now) == MOQ_OK);
    CHECK(moq_session_on_data_stop(f->sender, ref, 77, f->now) == MOQ_OK);
    moq_action_t a;
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    CHECK(a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.error_code == 44);
    moq_action_cleanup(&a);
    CHECK(open_group(f, 16, &h, &ref));
    CHECK(moq_session_close_subgroup(f->sender, h, f->now) == MOQ_OK);
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    CHECK(a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.fin);
    moq_action_cleanup(&a);
    CHECK(moq_session_on_data_committed(f->sender, ref, f->now) == MOQ_OK);
    CHECK(moq_session_on_data_stop(f->sender, ref, 99, f->now) == MOQ_OK);
    moq_event_t ev;
    CHECK(moq_session_poll_events(f->sender, &ev, 1) == 0);
    return true;
}

static bool owner_cancel(fixture_t *f)
{
    moq_subgroup_handle_t h;
    moq_stream_ref_t ref;
    CHECK(open_group(f, 18, &h, &ref));
    size_t slot = moq_handle_slot(f->sub._opaque);
    moq_stream_ref_t owner = f->sender->subs[slot].rs.ref;
    CHECK(owner._v != 0);
    size_t events = fill_events(f->sender);
    CHECK(moq_session_on_data_stop(f->sender, ref, 29, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_session_on_bidi_stream_stop(f->sender, owner, 1, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(release_events(f->sender, events));
    CHECK(moq_session_on_bidi_stream_stop(f->sender, owner, 1, f->now) == MOQ_OK);
    moq_event_t e;
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 1);
    CHECK(e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED);
    CHECK(e.u.outgoing_subgroup_terminated.subgroup._opaque == h._opaque);
    CHECK(e.u.outgoing_subgroup_terminated.sub._opaque == f->sub._opaque);
    CHECK(e.u.outgoing_subgroup_terminated.error_code == 29);
    moq_event_cleanup(&e);
    drain_events(f->sender);
    moq_action_t a;
    unsigned resets = 0;
    while (moq_session_poll_actions(f->sender, &a, 1)) {
        if (a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.stream_ref._v == ref._v) {
            CHECK(a.u.reset_data.error_code == 29);
            resets++;
        }
        moq_action_cleanup(&a);
    }
    CHECK(resets == 1 && moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

/* Route encoded peer output through the same public inputs a transport uses.
 * A new incoming request gets a fixture-local transport ref; a response uses
 * the opener's ref captured when that request action was polled. */
static bool deliver_request_bytes(moq_session_t *receiver, const moq_action_t *a,
                                   moq_stream_ref_t receiver_ref, uint64_t now)
{
    moq_result_t rc;
    if (a->kind == MOQ_ACTION_SEND_CONTROL) {
        rc = moq_session_on_control_bytes(receiver, a->u.send_control.data,
                                            a->u.send_control.len, now);
    } else if (a->kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
        rc = moq_session_on_bidi_stream_bytes(receiver, receiver_ref,
            a->u.open_bidi_stream.data, a->u.open_bidi_stream.len,
            a->u.open_bidi_stream.fin, now);
    } else {
        CHECK(a->kind == MOQ_ACTION_SEND_BIDI_STREAM);
        rc = moq_session_on_bidi_stream_bytes(receiver, receiver_ref,
            a->u.send_bidi_stream.data, a->u.send_bidi_stream.len,
            a->u.send_bidi_stream.fin, now);
    }
    CHECK(rc == MOQ_OK);
    CHECK(moq_session_state(receiver) == MOQ_SESS_ESTABLISHED);
    return true;
}

/* These requests precede the replacement on the wire. Receive each in FIFO
 * order (required by draft-16 request IDs), asserting its event rather than
 * dropping output or skipping notifications. They stay pending: no response
 * is needed to supply the fixture's action pressure. */
static bool receive_pressure_request(fixture_t *f, const moq_action_t *a,
                                      size_t index, bool streams)
{
    CHECK(f->peer->event_head == f->peer->event_tail);
    moq_stream_ref_t incoming = moq_stream_ref_from_u64(UINT64_C(0x74000000) + index);
    if (streams) {
        CHECK(a->kind == MOQ_ACTION_OPEN_BIDI_STREAM);
        CHECK(request_registry_find_by_streamref(f->peer, incoming).kind == MOQ_REQ_NONE);
    } else {
        CHECK(a->kind == MOQ_ACTION_SEND_CONTROL);
    }
    CHECK(deliver_request_bytes(f->peer, a, incoming, f->now));
    moq_event_t e;
    CHECK(moq_session_poll_events(f->peer, &e, 1) == 1);
    CHECK(e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    char name[32];
    int len = snprintf(name, sizeof(name), "lifetime-pressure-%zu", index);
    CHECK(len > 0 && (size_t)len < sizeof(name));
    CHECK(e.u.subscribe_request.track_name.len == (size_t)len);
    CHECK(memcmp(e.u.subscribe_request.track_name.data, name, (size_t)len) == 0);
    CHECK(e.u.subscribe_request.track_namespace.count == 1);
    CHECK(e.u.subscribe_request.track_namespace.parts[0].len == 8);
    CHECK(memcmp(e.u.subscribe_request.track_namespace.parts[0].data, "terminal", 8) == 0);
    CHECK(sub_resolve_handle(f->peer, e.u.subscribe_request.sub) >= 0);
    moq_event_cleanup(&e);
    CHECK(moq_session_poll_events(f->peer, &e, 1) == 0);
    return true;
}

static bool owner_cancel_reuse_debt(fixture_t *f, moq_version_t version)
{
    const uint64_t first_code = 73, cancel_code = 91;
    bool streams = version != MOQ_VERSION_DRAFT_16;
    moq_subgroup_handle_t old_sg;
    moq_stream_ref_t old_ref;
    CHECK(open_group(f, 28, &old_sg, &old_ref));
    size_t owner_slot = moq_handle_slot(f->publication ? f->pub._opaque : f->sub._opaque);
    moq_stream_ref_t owner_ref = f->publication
        ? f->sender->publishes[owner_slot].rs.ref : f->sender->subs[owner_slot].rs.ref;
    moq_stream_ref_t peer_ref = f->publication
        ? f->peer->publishes[moq_handle_slot(f->peer_pub._opaque)].rs.ref
        : f->peer->subs[moq_handle_slot(f->peer_sub._opaque)].rs.ref;
    uint64_t old_owner = f->publication ? f->pub._opaque : f->sub._opaque;

    /* No target DATA remains. Independent, real requests keep the action ring
     * full while cancellation and replacement admission use event storage. */
    for (size_t i = 0; i < f->sender->action_cap; i++) {
        char name[32];
        int len = snprintf(name, sizeof(name), "lifetime-pressure-%zu", i);
        CHECK(len > 0 && (size_t)len < sizeof(name));
        moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
        sc.track_namespace = (moq_namespace_t){&ns, 1};
        sc.track_name = (moq_bytes_t){(const uint8_t *)name, (size_t)len};
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t pressure;
        CHECK(moq_session_subscribe(f->sender, &sc, f->now, &pressure) == MOQ_OK);
    }
    CHECK(moq_session_action_capacity(f->sender) == 0);
    CHECK(moq_session_on_data_stop(f->sender, old_ref, first_code, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(moq_session_on_data_stop(f->sender, old_ref, 0, f->now) == MOQ_ERR_WOULD_BLOCK);
    moq_sg_entry_t *debt = &f->sender->subgroups[moq_handle_slot(old_sg._opaque)];
    CHECK(debt->state == MOQ_SG_TERMINAL_OWED);
    CHECK(debt->terminal_code == first_code);
    CHECK(debt->terminal_cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING);
    CHECK(f->sender->event_head == f->sender->event_tail);

    moq_result_t rc = f->publication
        ? moq_session_cancel_publication(f->peer, f->peer_pub, cancel_code, f->now)
        : moq_session_unsubscribe(f->peer, f->peer_sub, f->now);
    CHECK(rc == MOQ_OK);
    moq_action_t a;
    CHECK(moq_session_poll_actions(f->peer, &a, 1) == 1);
    uint64_t wire_cancel_code = 0;
    if (streams) {
        CHECK(a.kind == MOQ_ACTION_ABORT_BIDI_STREAM);
        CHECK(a.u.abort_bidi_stream.stream_ref._v == peer_ref._v);
        wire_cancel_code = a.u.abort_bidi_stream.error_code;
        if (f->publication) CHECK(wire_cancel_code == cancel_code);
        CHECK(owner_ref._v != 0);
        CHECK(moq_session_on_bidi_stream_reset(f->sender, owner_ref,
                  a.u.abort_bidi_stream.error_code, f->now) == MOQ_OK);
        CHECK(moq_session_on_bidi_stream_stop(f->sender, owner_ref,
                  a.u.abort_bidi_stream.error_code, f->now) == MOQ_OK);
    } else {
        CHECK(a.kind == MOQ_ACTION_SEND_CONTROL);
        CHECK(deliver_request_bytes(f->sender, &a, owner_ref, f->now));
    }
    moq_action_cleanup(&a);
    CHECK(moq_session_poll_actions(f->peer, &a, 1) == 0);
    rc = f->publication
        ? moq_session_cancel_publication(f->peer, f->peer_pub, 0, f->now)
        : moq_session_unsubscribe(f->peer, f->peer_sub, f->now);
    CHECK(rc == MOQ_ERR_STALE_HANDLE);
    CHECK(moq_session_poll_actions(f->peer, &a, 1) == 0);
    CHECK(f->publication ? pub_resolve_handle(f->sender, f->pub) < 0
                         : sub_resolve_handle(f->sender, f->sub) < 0);
    CHECK(debt->state == MOQ_SG_TERMINAL_OWED && debt->terminal_code == first_code);
    moq_event_t e;
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 1);
    if (f->publication) {
        CHECK(e.kind == MOQ_EVENT_PUBLISH_UNSUBSCRIBED);
        CHECK(e.u.publish_unsubscribed.pub._opaque == old_owner);
        CHECK(e.u.publish_unsubscribed.cause == (streams
              ? MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM : MOQ_REQUEST_TERMINAL_NONE));
        CHECK(e.u.publish_unsubscribed.stream_error_code == (streams ? cancel_code : 0));
    } else {
        CHECK(e.kind == MOQ_EVENT_UNSUBSCRIBED);
        CHECK(e.u.unsubscribed.sub._opaque == old_owner);
        CHECK(e.u.unsubscribed.cause == (streams
              ? MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM : MOQ_REQUEST_TERMINAL_NONE));
        CHECK(e.u.unsubscribed.stream_error_code == wire_cancel_code);
    }
    moq_event_cleanup(&e);
    CHECK(f->sender->event_head == f->sender->event_tail);
    CHECK(debt->state == MOQ_SG_TERMINAL_OWED);

    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    moq_subscription_t replacement_sub = MOQ_SUBSCRIPTION_INVALID;
    moq_publication_t replacement_pub = MOQ_PUBLICATION_INVALID;
    moq_stream_ref_t replacement_in = moq_stream_ref_from_u64(UINT64_C(0x73000001));
    moq_stream_ref_t replacement_out = moq_stream_ref_from_u64(0);
    if (!f->publication) {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
        sc.track_namespace = (moq_namespace_t){&ns, 1};
        sc.track_name = MOQ_BYTES_LITERAL("replacement");
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        CHECK(moq_session_subscribe(f->peer, &sc, f->now, &f->peer_sub) == MOQ_OK);
        CHECK(moq_session_poll_actions(f->peer, &a, 1) == 1);
        if (streams) {
            CHECK(a.kind == MOQ_ACTION_OPEN_BIDI_STREAM);
            replacement_out = a.u.open_bidi_stream.stream_ref;
            CHECK(request_registry_find_by_streamref(f->sender, replacement_in).kind == MOQ_REQ_NONE);
        }
        CHECK(deliver_request_bytes(f->sender, &a, replacement_in, f->now));
        moq_action_cleanup(&a);
        CHECK(moq_session_poll_events(f->sender, &e, 1) == 1);
        CHECK(e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
        replacement_sub = e.u.subscribe_request.sub;
        moq_event_cleanup(&e);
        CHECK(moq_handle_slot(replacement_sub._opaque) == owner_slot);
        CHECK(replacement_sub._opaque != old_owner);
        CHECK(debt->state == MOQ_SG_TERMINAL_OWED);
    }

    size_t events = fill_events(f->sender);
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    CHECK(a.kind == (streams ? MOQ_ACTION_OPEN_BIDI_STREAM : MOQ_ACTION_SEND_CONTROL));
    CHECK(receive_pressure_request(f, &a, 0, streams));
    moq_action_cleanup(&a);
    if (f->publication) {
        moq_publish_cfg_t pc;
        moq_publish_cfg_init_sized(&pc, sizeof(pc));
        pc.track_namespace = (moq_namespace_t){&ns, 1};
        pc.track_name = MOQ_BYTES_LITERAL("replacement");
        CHECK(moq_session_publish(f->sender, &pc, f->now, &replacement_pub) == MOQ_OK);
        CHECK(moq_handle_slot(replacement_pub._opaque) == owner_slot);
        CHECK(replacement_pub._opaque != old_owner);
        replacement_out = f->sender->publishes[owner_slot].rs.ref;
    } else {
        moq_accept_subscribe_cfg_t ac;
        moq_accept_subscribe_cfg_init(&ac);
        CHECK(moq_session_accept_subscribe(f->sender, replacement_sub, &ac, f->now) == MOQ_OK);
    }
    uint64_t new_owner = f->publication ? replacement_pub._opaque : replacement_sub._opaque;
    CHECK(moq_handle_generation(new_owner) != moq_handle_generation(old_owner));
    CHECK(debt->state == MOQ_SG_TERMINAL_OWED && debt->terminal_code == first_code);
    CHECK(debt->sub._opaque == f->sub._opaque && debt->pub._opaque == f->pub._opaque);
    CHECK(moq_session_on_data_committed(f->sender, old_ref, f->now) == MOQ_OK);
    CHECK(moq_session_on_data_stop(f->sender, old_ref, cancel_code, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(release_events(f->sender, events));
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    CHECK(a.kind == MOQ_ACTION_RESET_DATA && a.u.reset_data.stream_ref._v == old_ref._v);
    CHECK(a.u.reset_data.error_code == first_code && a.u.reset_data.error_code != cancel_code);
    moq_action_cleanup(&a);
    CHECK(f->sender->event_tail - f->sender->event_head == 1);
    CHECK(f->sender->events[f->sender->event_head % f->sender->event_cap]
              .u.outgoing_subgroup_terminated.group_id == 28);
    CHECK(terminal(f, old_sg, old_ref, first_code, MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING));
    for (size_t i = 1; i < f->sender->action_cap; i++) {
        CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
        CHECK(a.kind == (streams ? MOQ_ACTION_OPEN_BIDI_STREAM : MOQ_ACTION_SEND_CONTROL));
        CHECK(receive_pressure_request(f, &a, i, streams));
        moq_action_cleanup(&a);
    }
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    if (f->publication) {
        CHECK(a.kind == (streams ? MOQ_ACTION_OPEN_BIDI_STREAM : MOQ_ACTION_SEND_CONTROL));
        CHECK(deliver_request_bytes(f->peer, &a, replacement_in, f->now));
        moq_action_cleanup(&a);
        CHECK(moq_session_poll_events(f->peer, &e, 1) == 1);
        CHECK(e.kind == MOQ_EVENT_PUBLISH_REQUEST);
        f->peer_pub = e.u.publish_request.pub;
        moq_event_cleanup(&e);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        CHECK(moq_session_accept_publish(f->peer, f->peer_pub, &ac, f->now) == MOQ_OK);
        CHECK(moq_session_poll_actions(f->peer, &a, 1) == 1);
        CHECK(deliver_request_bytes(f->sender, &a, replacement_out, f->now));
        moq_action_cleanup(&a);
        CHECK(moq_session_poll_events(f->sender, &e, 1) == 1);
        CHECK(e.kind == MOQ_EVENT_PUBLISH_OK && e.u.publish_ok.pub._opaque == new_owner);
        moq_event_cleanup(&e);
        f->pub = replacement_pub;
    } else {
        CHECK(a.kind == (streams ? MOQ_ACTION_SEND_BIDI_STREAM : MOQ_ACTION_SEND_CONTROL));
        CHECK(deliver_request_bytes(f->peer, &a, replacement_out, f->now));
        moq_action_cleanup(&a);
        CHECK(moq_session_poll_events(f->peer, &e, 1) == 1);
        CHECK(e.kind == MOQ_EVENT_SUBSCRIBE_OK && moq_subscription_eq(e.u.subscribe_ok.sub, f->peer_sub));
        moq_event_cleanup(&e);
        f->sub = replacement_sub;
    }
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 0);
    moq_subgroup_handle_t fresh;
    moq_stream_ref_t fresh_ref;
    CHECK(open_group(f, 29, &fresh, &fresh_ref));
    CHECK(moq_handle_slot(fresh._opaque) == moq_handle_slot(old_sg._opaque));
    CHECK(fresh._opaque != old_sg._opaque && fresh_ref._v != old_ref._v);
    const moq_sg_entry_t *live = &f->sender->subgroups[moq_handle_slot(fresh._opaque)];
    CHECK(live->terminal_cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE && live->terminal_code == 0);
    CHECK(moq_session_on_data_stop(f->sender, old_ref, cancel_code, f->now) == MOQ_OK);
    CHECK(moq_session_on_data_committed(f->sender, old_ref, f->now) == MOQ_OK);
    CHECK(moq_session_write_object(f->sender, old_sg, 0, f->payload, f->now) == MOQ_ERR_STALE_HANDLE);
    CHECK(moq_session_write_object(f->sender, fresh, 0, f->payload, f->now) == MOQ_OK);
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 1);
    CHECK(a.kind == MOQ_ACTION_SEND_DATA && a.u.send_data.stream_ref._v == fresh_ref._v);
    CHECK(a.u.send_data.payload == f->payload);
    moq_action_cleanup(&a);
    CHECK(moq_session_poll_actions(f->sender, &a, 1) == 0);
    CHECK(moq_session_poll_events(f->sender, &e, 1) == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    return true;
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++) {
        for (unsigned pub = 0; pub < 2; pub++) {
            for (unsigned mask = 0; mask < 4; mask++) {
                fixture_t f;
                if (setup(&f, versions[v], pub != 0, false)) (void)pressure(&f, mask);
                (void)cleanup(&f);
            }
            fixture_t f;
            if (setup(&f, versions[v], pub != 0, false)) (void)sized_identity(&f);
            (void)cleanup(&f);
            for (unsigned mode = 0; mode < 3; mode++) {
                printf("nonhead_priority_barrier/version%u/publication%u/poll%u\n",
                       (unsigned)versions[v], pub, mode);
                if (setup(&f, versions[v], pub != 0, false))
                    (void)nonhead_priority_barrier(&f, mode);
                (void)cleanup(&f);
            }
            if (setup(&f, versions[v], pub != 0, false)) (void)local_terminals(&f);
            (void)cleanup(&f);
            printf("owner_cancel_reuse_debt/version%u/publication%u\n", (unsigned)versions[v], pub);
            if (setup(&f, versions[v], pub != 0, false)) (void)owner_cancel_reuse_debt(&f, versions[v]);
            (void)cleanup(&f);
            if (v != 0 && pub == 0) {
                if (setup(&f, versions[v], false, false)) (void)owner_cancel(&f);
                (void)cleanup(&f);
            }
            for (unsigned first = 0; first < 2; first++) {
                if (setup(&f, versions[v], pub != 0, true)) (void)expiry_order(&f, first != 0);
                (void)cleanup(&f);
            }
            if (v != 0)
                for (unsigned timeout = 0; timeout < 2; timeout++)
                    for (unsigned full = 0; full < 2; full++)
                        for (unsigned bridge = 0; bridge < 2; bridge++) {
                            printf("polled_fin_owner_done/version%u/publication%u/timeout%u/full%u/bridge%u\n",
                                   (unsigned)versions[v], pub, timeout, full, bridge);
                            if (setup_delivery_timeout(&f, versions[v], pub != 0, 2))
                                (void)polled_fin_owner_done(&f, timeout != 0,
                                                          full != 0, bridge != 0);
                            (void)cleanup(&f);
                        }
            if (v != 0)
                for (unsigned timeout = 0; timeout < 2; timeout++) {
                    printf("unrelated_done_progress/version%u/publication%u/timeout%u\n",
                           (unsigned)versions[v], pub, timeout);
                    if (setup_delivery_timeout(&f, versions[v], pub != 0, 2))
                        (void)unrelated_done_progress(&f, timeout != 0);
                    (void)cleanup(&f);
                }
        }
    }
    printf("%u assertions, %u failures\n", checks, failures);
    return failures ? 1 : 0;
}
