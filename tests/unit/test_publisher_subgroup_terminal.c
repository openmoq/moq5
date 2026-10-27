/* Real owners and wire delivery, with explicit virtual time and bounded output
 * pressure. */
#include <moq/publisher.h>
#include <moq/sim.h>
#include <moq/buf.h>
#include "../../core/src/session/session_internal.h"
#include "test_alloc_support.h"
#include <stdio.h>
#include <string.h>

static unsigned failures, rows;
static char case_name[96];
#define CHECK(x) do { if (!(x)) { ++failures; \
    fprintf(stderr, "%s/%s:%d: %s\n", case_name, __func__, __LINE__, #x); return false; } } while (0)

typedef struct property_prefix {
    bool captured, observed;
    uint8_t header[32];
    size_t header_len;
    const uint8_t *data; /* held by the fixture's properties reference */
    size_t data_len;
    moq_action_meta_t meta;
} property_prefix_t;

typedef struct peer_reply {
    moq_subscription_t sub;
    moq_fetch_t fetch;
    unsigned acks, oks, objects, completes, errors;
    bool replacement, properties;
} peer_reply_t;

typedef struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *sender, *peer;
    moq_publisher_t *publisher;
    moq_pub_track_t *track;
    moq_subscription_t peer_sub;
    moq_publication_t peer_pub;
    moq_subscription_t pending_peer_sub;
    moq_fetch_t pending_peer_fetch;
    moq_rcbuf_t *payload, *properties;
    uint64_t now;
    bool stop_publication, manual, watching;
    moq_stream_ref_t stopped_ref;
    moq_subgroup_handle_t stopped_handle;
    moq_stream_ref_t other_ref;
    uint64_t stopped_group, stopped_subgroup;
    unsigned received[2][40][20], reopened, post_stop_payloads, left;
    unsigned dones;
    unsigned done_received; /* actual application delivery, without a deadline tick */
    uint64_t done_streams, done_status;
    unsigned stopped_resets, pending_acks, pending_errors;
    unsigned stopped_fins, other_resets, other_fins;
    unsigned fetch_oks, fetch_objects, fetch_completes;
    property_prefix_t property_prefix;
    bool expiry_input;
    unsigned post_terminal_rcbufs, retained_property_prefixes;
    uint64_t stopped_reset_code;
    peer_reply_t replies[3];
    unsigned authorized, joined, updated, forward_changes;
    unsigned update_acks[2];
    bool update_has_largest[2];
    uint64_t update_largest_group[2], update_largest_object[2];
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
    moq_event_t terminal;
    unsigned terminals;
#endif
} fixture_t;

static void subscriber_left(void *ctx, moq_pub_track_t *track)
{
    (void)track;
    ++((fixture_t *)ctx)->left;
}

static void subscriber_joined(void *ctx, moq_pub_track_t *track)
{
    (void)track;
    ++((fixture_t *)ctx)->joined;
}

static void subscriber_updated(void *ctx, moq_pub_track_t *track,
                                const moq_pub_subscribe_update_info_t *info)
{
    (void)track; (void)info;
    ++((fixture_t *)ctx)->updated;
}

static void publication_forward(void *ctx, moq_pub_track_t *track, bool forward)
{
    (void)track; (void)forward;
    ++((fixture_t *)ctx)->forward_changes;
}

static moq_pub_accept_decision_t authorize(void *ctx, const moq_pub_subscribe_info_t *info,
                                           moq_request_error_t *error)
{
    (void)info; (void)error;
    ++((fixture_t *)ctx)->authorized;
    return MOQ_PUB_DECISION_ACCEPT;
}

static void trace(void *ctx, const moq_sim_trace_record_t *r)
{
    fixture_t *f = ctx;
    if (!f->watching || r->kind != MOQ_SIM_TRACE_ACTION ||
        r->from != MOQ_PERSPECTIVE_SERVER)
        return;
    if (r->action_kind == MOQ_ACTION_RESET_DATA && r->stream_ref._v == f->stopped_ref._v) {
        ++f->stopped_resets;
        f->stopped_reset_code = r->error_code;
    }
    if (r->action_kind == MOQ_ACTION_RESET_DATA && r->stream_ref._v == f->other_ref._v)
        ++f->other_resets;
    if (r->action_kind != MOQ_ACTION_SEND_DATA) return;
    if (r->fin && r->stream_ref._v == f->stopped_ref._v) ++f->stopped_fins;
    if (r->fin && r->stream_ref._v == f->other_ref._v) ++f->other_fins;
    if (r->stream_ref._v == f->stopped_ref._v && r->count) {
        ++f->post_terminal_rcbufs;
        property_prefix_t *p = &f->property_prefix;
        if (f->expiry_input && p->captured && !p->observed && !r->fin &&
            r->bytes.len == p->header_len && r->bytes.data &&
            memcmp(r->bytes.data, p->header, p->header_len) == 0 &&
            r->detail_bytes.data == p->data && r->detail_bytes.len == p->data_len &&
            r->count == p->data_len) {
            p->observed = true;
            ++f->retained_property_prefixes;
        } else {
            ++f->post_stop_payloads;
        }
    }
    for (size_t i = 0; i < f->sender->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->sender->subgroups[i];
        if (sg->state == MOQ_SG_FREE || sg->stream_ref._v != r->stream_ref._v)
            continue;
        if (moq_publication_is_valid(sg->pub) == f->stop_publication &&
            sg->group_id == f->stopped_group && sg->subgroup_id == f->stopped_subgroup &&
            sg->stream_ref._v != f->stopped_ref._v)
            ++f->reopened;
    }
}

static bool peer_events(fixture_t *f)
{
    moq_event_t e;
    while (moq_session_poll_events(f->peer, &e, 1)) {
        moq_result_t rc = MOQ_OK;
        if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t c;
            moq_accept_namespace_cfg_init(&c);
            rc = moq_session_accept_namespace(f->peer, e.u.namespace_published.ann, &c, f->now);
        } else if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            f->peer_pub = e.u.publish_request.pub;
            moq_accept_publish_cfg_t c;
            moq_accept_publish_cfg_init_sized(&c, sizeof(c));
            rc = moq_session_accept_publish(f->peer, e.u.publish_request.pub, &c, f->now);
        } else if (e.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &e.u.object_received;
            if (o->status == MOQ_OBJECT_NORMAL) {
                CHECK(o->group_id < 40 && o->object_id < 20);
                CHECK(o->payload && moq_rcbuf_len(o->payload) == 6);
                CHECK(memcmp(moq_rcbuf_data(o->payload), "object", 6) == 0);
                ++f->received[moq_publication_is_valid(o->pub)][o->group_id][o->object_id];
            }
        } else if (e.kind == MOQ_EVENT_SUBSCRIBE_DONE) {
            ++f->dones;
            f->done_received = 1;
            f->done_streams = e.u.subscribe_done.stream_count;
            f->done_status = e.u.subscribe_done.status_code;
        } else if (e.kind == MOQ_EVENT_SUBSCRIBE_OK &&
                   moq_subscription_eq(e.u.subscribe_ok.sub, f->pending_peer_sub)) {
            ++f->pending_acks;
        } else if (e.kind == MOQ_EVENT_SUBSCRIBE_ERROR &&
                   moq_subscription_eq(e.u.subscribe_error.sub, f->pending_peer_sub)) {
            ++f->pending_errors;
        } else if (e.kind == MOQ_EVENT_FETCH_OK &&
                   moq_fetch_eq(e.u.fetch_ok.fetch, f->pending_peer_fetch)) {
            CHECK(e.u.fetch_ok.end_group == 0 && e.u.fetch_ok.end_object == 1);
            ++f->fetch_oks;
        } else if (e.kind == MOQ_EVENT_FETCH_OBJECT &&
                   moq_fetch_eq(e.u.fetch_object.fetch, f->pending_peer_fetch)) {
            CHECK(e.u.fetch_object.group_id == 0 && e.u.fetch_object.object_id == 0);
            CHECK(e.u.fetch_object.payload && moq_rcbuf_len(e.u.fetch_object.payload) == 6);
            CHECK(memcmp(moq_rcbuf_data(e.u.fetch_object.payload), "object", 6) == 0);
            ++f->fetch_objects;
        } else if (e.kind == MOQ_EVENT_FETCH_COMPLETE &&
                   moq_fetch_eq(e.u.fetch_complete.fetch, f->pending_peer_fetch)) {
            ++f->fetch_completes;
        } else if (e.kind == MOQ_EVENT_FETCH_ERROR &&
                   moq_fetch_eq(e.u.fetch_error.fetch, f->pending_peer_fetch)) {
            ++f->pending_errors;
        } else if (e.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK &&
                   moq_subscription_eq(e.u.subscription_update_ok.sub, f->peer_sub)) {
            ++f->update_acks[0];
            f->update_has_largest[0] = e.u.subscription_update_ok.has_largest;
            f->update_largest_group[0] = e.u.subscription_update_ok.largest_group;
            f->update_largest_object[0] = e.u.subscription_update_ok.largest_object;
        } else if (e.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK &&
                   moq_publication_eq(e.u.publication_update_ok.pub, f->peer_pub)) {
            ++f->update_acks[1];
            f->update_has_largest[1] = e.u.publication_update_ok.has_largest;
            f->update_largest_group[1] = e.u.publication_update_ok.largest_group;
            f->update_largest_object[1] = e.u.publication_update_ok.largest_object;
        }
        for (size_t i = 0; i < 3; i++) {
            peer_reply_t *p = &f->replies[i];
            if (e.kind == MOQ_EVENT_SUBSCRIBE_OK && moq_subscription_eq(e.u.subscribe_ok.sub, p->sub))
                ++p->acks;
            if (e.kind == MOQ_EVENT_SUBSCRIBE_ERROR && moq_subscription_eq(e.u.subscribe_error.sub, p->sub))
                ++p->errors;
            if (e.kind == MOQ_EVENT_FETCH_OK && moq_fetch_eq(e.u.fetch_ok.fetch, p->fetch)) {
                CHECK(e.u.fetch_ok.end_group == 0 && e.u.fetch_ok.end_object == 1);
                ++p->oks;
            }
            if (e.kind == MOQ_EVENT_FETCH_OBJECT && moq_fetch_eq(e.u.fetch_object.fetch, p->fetch)) {
                const moq_fetch_object_event_t *o = &e.u.fetch_object;
                CHECK(o->group_id == 0 && o->object_id == 0 && o->payload);
                CHECK(moq_rcbuf_len(o->payload) == 6);
                CHECK(memcmp(moq_rcbuf_data(o->payload), p->replacement ? "second" : "object", 6) == 0);
                CHECK((o->properties != NULL) == p->properties);
                if (p->properties) {
                    CHECK(moq_rcbuf_len(o->properties) == moq_rcbuf_len(f->properties));
                    CHECK(memcmp(moq_rcbuf_data(o->properties), moq_rcbuf_data(f->properties),
                        moq_rcbuf_len(f->properties)) == 0);
                }
                ++p->objects;
            }
            if (e.kind == MOQ_EVENT_FETCH_COMPLETE && moq_fetch_eq(e.u.fetch_complete.fetch, p->fetch))
                ++p->completes;
            if (e.kind == MOQ_EVENT_FETCH_ERROR && moq_fetch_eq(e.u.fetch_error.fetch, p->fetch))
                ++p->errors;
        }
        bool closed = e.kind == MOQ_EVENT_SESSION_CLOSED;
        moq_event_cleanup(&e);
        CHECK(rc == MOQ_OK && !closed);
    }
    return true;
}

static bool sender_events(fixture_t *f)
{
    if (!f->manual) {
        moq_result_t rc = moq_pub_tick(f->publisher, f->now);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        return true;
    }
    moq_event_t e;
    size_t remaining = f->sender->event_cap;
    while (remaining-- && moq_session_poll_events(f->sender, &e, 1)) {
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
        if (e.kind == MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED) {
            f->terminal = e;
            ++f->terminals;
        }
#endif
        moq_pub_event_result_t result;
        moq_result_t rc = moq_pub_handle_event(f->publisher, &e, f->now, &result);
        moq_event_cleanup(&e);
        CHECK(rc == MOQ_OK || (rc == MOQ_ERR_WOULD_BLOCK && result == MOQ_PUB_EVENT_CONSUMED));
    }
    moq_result_t rc = moq_pub_flush(f->publisher, f->now);
    CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    return true;
}

static bool settle(fixture_t *f)
{
    for (unsigned i = 0; i < 8; i++) {
        CHECK(sender_events(f));
        CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
        CHECK(peer_events(f));
    }
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(f->peer) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool subscribe(fixture_t *f)
{
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    moq_subscribe_cfg_t c;
    moq_subscribe_cfg_init_sized(&c, sizeof(c));
    c.track_namespace = (moq_namespace_t){&ns, 1};
    c.track_name = MOQ_BYTES_LITERAL("v");
    c.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    CHECK(moq_session_subscribe(f->peer, &c, f->now, &f->peer_sub) == MOQ_OK);
    return settle(f);
}

static bool integer_property(moq_version_t version, uint64_t type, uint64_t value,
                              uint8_t *bytes, size_t capacity, size_t *length)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, bytes, capacity);
    if (version == MOQ_VERSION_DRAFT_16) {
        CHECK(moq_buf_write_varint(&w, type) == MOQ_OK);
        CHECK(moq_buf_write_varint(&w, value) == MOQ_OK);
    } else {
        CHECK(moq_buf_write_vi64(&w, type) == MOQ_OK);
        CHECK(moq_buf_write_vi64(&w, value) == MOQ_OK);
    }
    *length = moq_buf_writer_offset(&w);
    return true;
}

static bool setup_configured(fixture_t *f, moq_version_t version, bool publication,
                              bool timed, bool manual, uint32_t capacity,
                              bool callback, bool retained_before_admission)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->memory);
    f->stop_publication = publication;
    f->manual = manual;
    moq_simpair_cfg_t c = MOQ_SIMPAIR_CFG_INIT;
    c.alloc = &f->alloc;
    c.version = version;
    c.initial_now_us = 1000000;
    c.max_actions = capacity;
    c.max_events = 16;
    c.client_send_request_capacity = c.server_send_request_capacity = true;
    c.client_initial_request_capacity = c.server_initial_request_capacity = 64;
    c.trace_fn = trace;
    c.trace_ctx = f;
    CHECK(moq_simpair_create(&c, &f->pair) == MOQ_OK);
    CHECK(moq_simpair_start(f->pair) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    f->sender = moq_simpair_server(f->pair);
    f->peer = moq_simpair_client(f->pair);
    f->now = moq_simpair_now_us(f->pair);
    moq_event_t e;
    while (moq_session_poll_events(f->sender, &e, 1)) moq_event_cleanup(&e);
    while (moq_session_poll_events(f->peer, &e, 1)) moq_event_cleanup(&e);
    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    pc.accept_mode = callback ? MOQ_PUB_CALLBACK : MOQ_PUB_ACCEPT_ALL;
    if (callback) {
        pc.on_subscribe = authorize;
        pc.on_subscribe_ctx = f;
    }
    moq_pub_callbacks_init_sized(&pc.callbacks, sizeof(pc.callbacks));
    pc.callbacks.ctx = f;
    pc.callbacks.on_subscriber_left = subscriber_left;
    pc.callbacks.on_subscriber_joined = subscriber_joined;
    pc.callbacks.on_subscriber_updated = subscriber_updated;
    pc.callbacks.on_publish_forward_changed = publication_forward;
    CHECK(moq_pub_create(f->sender, &f->alloc, &pc, &f->publisher) == MOQ_OK);
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    tc.track_namespace = (moq_namespace_t){&ns, 1};
    tc.track_name = MOQ_BYTES_LITERAL("v");
    static const uint8_t timeout[] = {0x02, 0x01};
    uint8_t props[18];
    size_t props_len = 0;
    CHECK(integer_property(version, 64, 1, props, sizeof(props), &props_len));
    if (timed && !publication) tc.track_properties = (moq_bytes_t){timeout, sizeof(timeout)};
    CHECK(moq_pub_add_track(f->publisher, &tc, f->now, &f->track) == MOQ_OK);
    if (retained_before_admission) {
        CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &f->payload) == MOQ_OK);
        CHECK(moq_rcbuf_create(&f->alloc, props, props_len, &f->properties) == MOQ_OK);
        moq_pub_retained_object_t object = {
            .object_id = 0, .payload = f->payload, .properties = f->properties
        };
        moq_pub_retained_group_cfg_t retained;
        moq_pub_retained_group_cfg_init(&retained);
        retained.group_id = 0;
        retained.objects = &object;
        retained.object_count = 1;
        CHECK(moq_pub_set_retained_group(f->publisher, f->track, &retained) == MOQ_OK);
    }
    if (manual) CHECK(subscribe(f));
    moq_pub_publish_cfg_t pp;
    moq_pub_publish_cfg_init(&pp);
    if (timed) pp.track_properties = publication
        ? (moq_bytes_t){timeout, sizeof(timeout)}
        : (moq_bytes_t){props, props_len}; /* nonempty override: no timeout */
    CHECK(moq_pub_publish_track(f->publisher, f->track, &pp, f->now) == MOQ_OK);
    CHECK(settle(f));
    if (!manual) CHECK(subscribe(f));
    CHECK(moq_pub_active_subscriptions(f->publisher, f->track) == 1);
    CHECK(moq_pub_track_forward(f->publisher, f->track));
    if (!retained_before_admission) {
        CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"object", 6, &f->payload) == MOQ_OK);
        CHECK(moq_rcbuf_create(&f->alloc, props, props_len, &f->properties) == MOQ_OK);
    }
    return true;
}

static bool setup_authorized(fixture_t *f, moq_version_t version, bool publication,
                              bool timed, bool manual, uint32_t capacity, bool callback)
{
    return setup_configured(f, version, publication, timed, manual, capacity, callback, false);
}

static bool setup(fixture_t *f, moq_version_t version, bool publication,
                   bool timed, bool manual, uint32_t capacity)
{
    return setup_authorized(f, version, publication, timed, manual, capacity, false);
}

static void cleanup(fixture_t *f)
{
    moq_pub_destroy(f->publisher);
    moq_simpair_destroy(f->pair);
    if (f->payload && moq_rcbuf_refcount(f->payload) != 1) {
        ++failures;
        fprintf(stderr, "%s/cleanup: payload refs=%zu, expected 1\n", case_name, (size_t)moq_rcbuf_refcount(f->payload));
    }
    if (f->properties && moq_rcbuf_refcount(f->properties) != 1) {
        ++failures;
        fprintf(stderr, "%s/cleanup: property refs=%zu, expected 1\n", case_name, (size_t)moq_rcbuf_refcount(f->properties));
    }
    moq_rcbuf_decref(f->payload);
    moq_rcbuf_decref(f->properties);
    if (f->memory.balance != 0) {
        ++failures;
        fprintf(stderr, "%s/cleanup: allocator balance=%lld, expected 0\n", case_name, (long long)f->memory.balance);
    }
    ++rows;
}

static moq_result_t write_object(fixture_t *f, uint64_t group, uint64_t object, bool props)
{
    moq_pub_object_cfg_t c;
    moq_pub_object_cfg_init_sized(&c, sizeof(c));
    c.group_id = group;
    c.object_id = object;
    c.payload = f->payload;
    c.properties = props ? f->properties : NULL;
    return moq_pub_write_object_ex(f->publisher, f->track, &c, f->now);
}

static bool watch(fixture_t *f, uint64_t group)
{
    for (size_t i = 0; i < f->sender->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->sender->subgroups[i];
        if (sg->state == MOQ_SG_FREE || sg->group_id != group ||
            moq_publication_is_valid(sg->pub) != f->stop_publication)
            continue;
        f->stopped_ref = sg->stream_ref;
        f->stopped_handle._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP,
            f->sender->session_tag, sg->generation, (uint32_t)i);
        f->stopped_group = group;
        f->stopped_subgroup = sg->subgroup_id;
        f->watching = true;
        return true;
    }
    CHECK(false);
    return false;
}

static bool terminate(fixture_t *f, bool timed)
{
    if (timed) {
        f->now += 2000;
        moq_result_t rc = moq_simpair_advance_to(f->pair, f->now);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        f->expiry_input = true;
    } else {
        moq_result_t rc = moq_session_on_data_stop(f->sender, f->stopped_ref, 0, f->now);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    }
    return true;
}

static bool capture_property_prefix(fixture_t *f, bool expected)
{
    property_prefix_t *p = &f->property_prefix;
    for (size_t i = f->sender->action_head; i < f->sender->action_tail; i++) {
        const moq_action_t *a = &f->sender->actions[i % f->sender->action_cap];
        const moq_action_meta_t *m = &f->sender->action_meta[i % f->sender->action_cap];
        if (a->kind != MOQ_ACTION_SEND_DATA || a->u.send_data.stream_ref._v != f->stopped_ref._v ||
            a->u.send_data.payload != f->properties || m->deadline_us != UINT64_MAX)
            continue;
        CHECK(!p->captured && !a->u.send_data.fin);
        CHECK(a->u.send_data.header_len > 0 && a->u.send_data.header_len <= sizeof(p->header));
        CHECK(m->mode == MOQ_SCHED_MODE_SUBGROUP && !m->subgroup_header);
        CHECK(m->group_id == f->stopped_group && m->subgroup_id == f->stopped_subgroup);
        p->header_len = a->u.send_data.header_len;
        memcpy(p->header, a->u.send_data.header, p->header_len);
        p->data = moq_rcbuf_data(a->u.send_data.payload);
        p->data_len = moq_rcbuf_len(a->u.send_data.payload);
        p->meta = *m;
        p->captured = true;
    }
    CHECK(p->captured == expected);
    return true;
}

static bool isolation(fixture_t *f, bool timed, unsigned mode)
{
    bool streaming = (mode & 2u) != 0, props = (mode & 1u) != 0;
    if (streaming) {
        moq_pub_begin_object_cfg_t c;
        moq_pub_begin_object_cfg_init_sized(&c, sizeof(c));
        c.payload_length = 6;
        c.properties = props ? f->properties : NULL;
        CHECK(moq_pub_begin_object(f->publisher, f->track, &c, f->now) == MOQ_OK);
        if (timed) CHECK(moq_pub_write_data(f->publisher, f->track, f->payload, f->now) == MOQ_OK);
    } else {
        CHECK(write_object(f, 0, 0, props) == MOQ_OK);
    }
    if (!timed) CHECK(settle(f));
    CHECK(watch(f, 0));
    /* D18 streaming framing precedes the first-payload timer. Capture that
     * exact non-expiring property action, never an object payload exemption. */
    if (timed) CHECK(capture_property_prefix(f,
        streaming && props && !f->sender->profile->object_timer_at_header));
    CHECK(terminate(f, timed));
    /* Production before the normal notice pump must also respect the key. */
    if (streaming) {
        if (!timed) CHECK(moq_pub_write_data(f->publisher, f->track, f->payload, f->now) == MOQ_OK);
        CHECK(moq_pub_end_object(f->publisher, f->track, f->now) == MOQ_OK);
    }
    CHECK(settle(f));
    CHECK(moq_pub_active_subscriptions(f->publisher, f->track) == 1);
    CHECK(moq_pub_track_forward(f->publisher, f->track));
    CHECK(f->left == 0);
    CHECK(write_object(f, 0, 1, props) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[f->stop_publication][0][1] == 0);
    CHECK(f->received[!f->stop_publication][0][1] == 1);
    CHECK(f->received[!f->stop_publication][0][0] == 1);
    fprintf(stderr, "%s: reopened=%u obsolete_payload_actions=%u raw_rcbuf_actions=%u retained_property_prefixes=%u resets=%u reset_code=%llu\n",
        case_name, f->reopened, f->post_stop_payloads, f->post_terminal_rcbufs,
        f->retained_property_prefixes, f->stopped_resets, (unsigned long long)f->stopped_reset_code);
    CHECK(f->reopened == 0);
    CHECK(f->post_stop_payloads == 0);
    CHECK(f->stopped_resets == 1);
    CHECK(f->stopped_reset_code == (timed ? MOQ_RESET_DELIVERY_TIMEOUT : 0));
    CHECK(f->received[f->stop_publication][0][0] == (!timed && !streaming ? 1u : 0u));
    CHECK(f->retained_property_prefixes == (f->property_prefix.captured ? 1u : 0u));
    CHECK(moq_pub_reset_group(f->publisher, f->track, 0, f->now) == MOQ_OK);
    CHECK(settle(f));
    CHECK(write_object(f, 0, 2, props) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][0][2] == 1 && f->received[1][0][2] == 1);
    CHECK(write_object(f, 1, 0, props) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][1][0] == 1 && f->received[1][1][0] == 1);
    CHECK(f->left == 0 && f->reopened == 0);
    CHECK(moq_pub_finish_subscribers(f->publisher, f->track, 2, f->now) == MOQ_OK);
    CHECK(settle(f));
    /* SimPair observes each first admission: original group 0, reopened
     * group 0 after reset, and group 1. RESET retains the historical count;
     * even the timed row sends the identifying header before its reset. */
    CHECK(f->done_received == 1 && f->dones == 1);
    CHECK(f->done_streams == 3 && f->done_status == 2);
    return true;
}

static bool blocked_retry(fixture_t *f)
{
    CHECK(write_object(f, 0, 0, false) == MOQ_ERR_WOULD_BLOCK);
    CHECK(watch(f, 0));
    CHECK(f->sender->action_tail - f->sender->action_head == 3);
    unsigned target_actions = 0;
    for (size_t i = f->sender->action_head; i < f->sender->action_tail; i++) {
        const moq_action_t *a = &f->sender->actions[i % f->sender->action_cap];
        if (a->kind == MOQ_ACTION_SEND_DATA && a->u.send_data.stream_ref._v == f->stopped_ref._v)
            ++target_actions;
    }
    CHECK(target_actions == (f->stop_publication ? 1u : 2u));
    /* Notification credit alone is held; no fabricated terminal is injected. */
    while (f->sender->event_tail - f->sender->event_head < f->sender->event_cap) {
        moq_event_t *e = &f->sender->events[f->sender->event_tail++ % f->sender->event_cap];
        memset(e, 0, sizeof(*e));
        e->kind = MOQ_EVENT_SUBGROUP_FINISHED;
    }
    CHECK(terminate(f, false));
    CHECK(moq_session_action_capacity(f->sender) == target_actions - 1);
    /* Collapsing header + payload returns one slot; a header-only terminal
     * returns none. The original operation's healthy destination may finish. */
    moq_result_t retry = write_object(f, 0, 0, false);
    CHECK(retry == (f->stop_publication ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
    /* Return event credit before servicing the blocked action barrier. */
    CHECK(sender_events(f));
    CHECK(settle(f));
    if (retry == MOQ_ERR_WOULD_BLOCK) {
        CHECK(write_object(f, 0, 0, false) == MOQ_OK);
        CHECK(settle(f));
    }
    CHECK(f->received[!f->stop_publication][0][0] == 1);
    CHECK(f->reopened == 0 && f->post_stop_payloads == 0 && f->left == 0);
    CHECK(write_object(f, 1, 0, false) == MOQ_ERR_WOULD_BLOCK);
    CHECK(settle(f));
    CHECK(write_object(f, 1, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][1][0] == 1 && f->received[1][1][0] == 1);
    return true;
}

static bool history_reuse(fixture_t *f)
{
    CHECK(write_object(f, 0, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(watch(f, 0));
    CHECK(terminate(f, false));
    CHECK(settle(f));
    for (uint64_t g = 1; g < 35; g++) {
        CHECK(write_object(f, g, 0, false) == MOQ_OK);
        CHECK(settle(f));
        CHECK(f->received[0][g][0] == 1 && f->received[1][g][0] == 1);
    }
    CHECK(write_object(f, 0, 1, false) == MOQ_ERR_WRONG_STATE);
    CHECK(f->reopened == 0 && f->left == 0);
    return true;
}

static bool generation_bound(fixture_t *f)
{
    for (uint64_t generation = 0; generation < 8; generation++) {
        CHECK(write_object(f, 0, generation * 2, false) == MOQ_OK);
        CHECK(settle(f));
        CHECK(watch(f, 0));
        CHECK(f->stopped_subgroup == generation);
        CHECK(terminate(f, false));
        CHECK(settle(f));
        CHECK(write_object(f, 0, generation * 2 + 1, false) == MOQ_OK);
        CHECK(settle(f));
        CHECK(f->received[f->stop_publication][0][generation * 2 + 1] == 0);
        CHECK(f->received[!f->stop_publication][0][generation * 2 + 1] == 1);
        CHECK(moq_pub_reset_group(f->publisher, f->track, 0, f->now) == MOQ_OK);
        CHECK(settle(f));
    }
    CHECK(write_object(f, 0, 16, false) == MOQ_ERR_NOMEM);
    CHECK(f->reopened == 0 && f->left == 0);
    return true;
}

static bool serialized_retirement_credit(fixture_t *f)
{
    CHECK(write_object(f, 0, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(watch(f, 0));
    moq_subscription_update_cfg_t update;
    moq_subscription_update_cfg_init_sized(&update, sizeof(update));
    update.has_forward = true;
    update.forward = false;
    CHECK(moq_session_update_subscription(f->peer, f->peer_sub, &update, f->now) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    moq_publication_t healthy = MOQ_PUBLICATION_INVALID;
    for (size_t i = 0; i < f->sender->sg_cap; i++)
        if (f->sender->subgroups[i].state == MOQ_SG_OPEN &&
            moq_publication_is_valid(f->sender->subgroups[i].pub))
            healthy = f->sender->subgroups[i].pub;
    CHECK(moq_publication_is_valid(healthy));
    /* Real datagrams on the other destination hold shared action capacity.
     * Each carries a distinct status location, outside the measured group. */
    uint64_t g = 40;
    while (moq_session_action_capacity(f->sender)) {
        CHECK(moq_session_send_pub_status_datagram(f->sender, healthy, g++, 0,
            128, MOQ_OBJECT_END_OF_GROUP, f->now) == MOQ_OK);
    }
    CHECK(sender_events(f)); /* the real Forward update arms a blocked reset */
    while (f->sender->event_tail - f->sender->event_head < f->sender->event_cap) {
        moq_event_t *e = &f->sender->events[f->sender->event_tail++ % f->sender->event_cap];
        memset(e, 0, sizeof(*e));
        e->kind = MOQ_EVENT_SUBGROUP_FINISHED;
    }
    CHECK(terminate(f, false));
    CHECK(moq_pub_flush(f->publisher, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(sender_events(f));
    CHECK(settle(f));
    update.forward = true;
    CHECK(moq_session_update_subscription(f->peer, f->peer_sub, &update, f->now) == MOQ_OK);
    CHECK(settle(f));
    CHECK(write_object(f, 0, 1, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][0][1] == 0 && f->received[1][0][1] == 1);
    CHECK(write_object(f, 1, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][1][0] == 1 && f->received[1][1][0] == 1);
    CHECK(f->left == 0 && f->reopened == 0);
    return true;
}

static bool request_pending(const fixture_t *f, bool fetch, uint64_t identity)
{
    if (fetch) {
        for (size_t i = 0; i < f->sender->fetch_cap; i++) {
            const moq_fetch_entry_t *e = &f->sender->fetches[i];
            if (e->handle._opaque == identity)
                return e->state == MOQ_FETCH_PENDING_PUBLISHER && e->role == MOQ_FETCH_ROLE_PUBLISHER;
        }
    } else {
        for (size_t i = 0; i < f->sender->sub_cap; i++) {
            const moq_sub_entry_t *e = &f->sender->subs[i];
            if (e->handle._opaque == identity)
                return e->state == MOQ_SUB_PENDING_PUBLISHER && e->role == MOQ_SUB_ROLE_PUBLISHER;
        }
    }
    return false;
}

static bool fill_action_credit(fixture_t *f, bool stopped_owner)
{
    const moq_sg_entry_t *healthy = NULL;
    for (size_t i = 0; i < f->sender->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->sender->subgroups[i];
        if (sg->state == MOQ_SG_OPEN && sg->group_id == 0 &&
            (moq_publication_is_valid(sg->pub) == f->stop_publication) == stopped_owner)
            healthy = sg;
    }
    CHECK(healthy != NULL);
    uint64_t group = 40;
    while (moq_session_action_capacity(f->sender)) {
        moq_result_t rc = moq_publication_is_valid(healthy->pub)
            ? moq_session_send_pub_status_datagram(f->sender, healthy->pub, group++, 0,
                255, MOQ_OBJECT_END_OF_GROUP, f->now)
            : moq_session_send_status_datagram(f->sender, healthy->sub, group++, 0,
                255, MOQ_OBJECT_END_OF_GROUP, f->now);
        CHECK(rc == MOQ_OK);
    }
    return true;
}

static bool fill_event_credit(fixture_t *f)
{
    CHECK(f->sender->event_head == f->sender->event_tail && f->sender->event_reserve == 0);
    for (size_t i = 0; i < f->sender->event_cap; i++) {
        moq_event_t *e = &f->sender->events[f->sender->event_tail++ % f->sender->event_cap];
        memset(e, 0, sizeof(*e));
        e->kind = MOQ_EVENT_SUBGROUP_FINISHED;
    }
    CHECK(f->sender->event_tail - f->sender->event_head == f->sender->event_cap);
    return true;
}

static bool terminal_barrier(fixture_t *f)
{
    CHECK(f->sender->action_tail - f->sender->action_head == f->sender->action_cap);
    const moq_action_t *head = &f->sender->actions[f->sender->action_head % f->sender->action_cap];
    CHECK(head->kind == MOQ_ACTION_SEND_DATA || head->kind == MOQ_ACTION_RESET_DATA);
    if (head->kind == MOQ_ACTION_SEND_DATA) {
        CHECK(head->u.send_data.stream_ref._v == f->stopped_ref._v);
        CHECK(head->u.send_data.payload == NULL && head->u.send_data.header_len == 0);
    } else {
        CHECK(head->u.reset_data.stream_ref._v == f->stopped_ref._v && head->u.reset_data.error_code == 0);
    }
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
    const moq_action_meta_t *barrier = &f->sender->action_meta[f->sender->action_head % f->sender->action_cap];
    CHECK(barrier->terminal_barrier);
    /* Lower priorities cannot overtake; cross-owner ties keep queue order. */
    for (size_t i = f->sender->action_head + 1; i < f->sender->action_tail; i++) {
        const moq_action_meta_t *m = &f->sender->action_meta[i % f->sender->action_cap];
        CHECK(m->subscriber_priority > barrier->subscriber_priority ||
            (m->subscriber_priority == barrier->subscriber_priority &&
             (m->publisher_priority > barrier->publisher_priority ||
              (m->publisher_priority == barrier->publisher_priority && m->owner != barrier->owner))));
    }
    unsigned owed = 0;
    for (size_t i = 0; i < f->sender->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->sender->subgroups[i];
        if (sg->state != MOQ_SG_TERMINAL_OWED || sg->stream_ref._v != f->stopped_ref._v) continue;
        CHECK(sg->group_id == 0 && sg->subgroup_id == f->stopped_subgroup);
        CHECK(sg->terminal_cause == MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING && sg->terminal_code == 0);
        CHECK(moq_publication_is_valid(sg->pub) == f->stop_publication);
        ++owed;
    }
    CHECK(owed == 1);
#endif
    moq_action_t action;
    size_t n = 0;
    CHECK(moq_session_poll_actions_ex(f->sender, &action, 1, sizeof(action), &n) == MOQ_ERR_WOULD_BLOCK);
    CHECK(n == 0);
    return true;
}

static bool pressure_step(fixture_t *f, unsigned *blocked, size_t *handled)
{
    size_t delivered = 0;
    CHECK(moq_simpair_step(f->pair, &delivered) == MOQ_OK);
    *handled += delivered;
    CHECK(peer_events(f));
    moq_result_t rc = f->manual ? moq_pub_flush(f->publisher, f->now)
                                : moq_pub_tick(f->publisher, f->now);
    CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    if (rc == MOQ_ERR_WOULD_BLOCK) ++*blocked;
    if (f->manual) CHECK(sender_events(f));
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(f->peer) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool request_track(fixture_t *f, const char *name, moq_pub_track_t **track,
                           moq_subscription_t *sub)
{
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = (moq_namespace_t){&ns, 1};
    tc.track_name = (moq_bytes_t){(const uint8_t *)name, 1};
    tc.advertise_namespace = false;
    if (*track == NULL) CHECK(moq_pub_add_track(f->publisher, &tc, f->now, track) == MOQ_OK);
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.track_namespace = tc.track_namespace;
    sc.track_name = tc.track_name;
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    CHECK(moq_session_subscribe(f->peer, &sc, f->now, sub) == MOQ_OK);
    return true;
}

static bool retain_probe(fixture_t *f, bool replacement)
{
    moq_rcbuf_t *payload = f->payload;
    if (replacement)
        CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"second", 6, &payload) == MOQ_OK);
    moq_pub_retained_object_t object = {
        .object_id = 0, .payload = payload, .properties = replacement ? NULL : f->properties
    };
    moq_pub_retained_group_cfg_t c;
    moq_pub_retained_group_cfg_init(&c);
    c.group_id = 0;
    c.objects = &object;
    c.object_count = 1;
    moq_result_t rc = moq_pub_set_retained_group(f->publisher, f->track, &c);
    if (replacement) moq_rcbuf_decref(payload);
    CHECK(rc == MOQ_OK);
    return true;
}

static bool request_probe_fetch(fixture_t *f, unsigned index)
{
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    moq_fetch_cfg_t c;
    moq_fetch_cfg_init_sized(&c, sizeof(c));
    c.track_namespace = (moq_namespace_t){&ns, 1};
    c.track_name = MOQ_BYTES_LITERAL("v");
    c.start_group = c.start_object = c.end_group = 0;
    c.end_object = 1;
    CHECK(moq_session_fetch(f->peer, &c, f->now, &f->replies[index].fetch) == MOQ_OK);
    return true;
}

static bool fifo_snapshot_owners(fixture_t *f, bool cancel)
{
    CHECK(f->authorized == 1 && f->joined == 1);
    CHECK(write_object(f, 0, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(watch(f, 0));
    CHECK(retain_probe(f, false));
    moq_pub_track_t *w = NULL, *x = NULL;
    CHECK(request_track(f, "w", &w, &f->replies[0].sub));
    CHECK(request_probe_fetch(f, 0));
    CHECK(request_track(f, "x", &x, &f->replies[1].sub));
    CHECK(request_probe_fetch(f, 1));
    f->replies[0].properties = f->replies[1].properties = true;
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    CHECK(peer_events(f));
    CHECK(f->sender->event_tail - f->sender->event_head == 4);
    const moq_event_t *a = &f->sender->events[f->sender->event_head % f->sender->event_cap];
    const moq_event_t *b = &f->sender->events[(f->sender->event_head + 1) % f->sender->event_cap];
    const moq_event_t *c = &f->sender->events[(f->sender->event_head + 2) % f->sender->event_cap];
    const moq_event_t *d = &f->sender->events[(f->sender->event_head + 3) % f->sender->event_cap];
    CHECK(a->kind == MOQ_EVENT_SUBSCRIBE_REQUEST && b->kind == MOQ_EVENT_FETCH_REQUEST);
    CHECK(c->kind == MOQ_EVENT_SUBSCRIBE_REQUEST && d->kind == MOQ_EVENT_FETCH_REQUEST);
    moq_subscription_t old_sub = a->u.subscribe_request.sub;
    moq_fetch_t old_fetch = b->u.fetch_request.fetch;
    moq_subscription_t other_sub = c->u.subscribe_request.sub;
    moq_fetch_t other_fetch = d->u.fetch_request.fetch;
    CHECK(write_object(f, 0, 1, false) == MOQ_OK);
    CHECK(fill_action_credit(f, false));
    size_t refs = moq_rcbuf_refcount(f->payload);
    CHECK(sender_events(f));
    CHECK(f->sender->event_head == f->sender->event_tail);
    CHECK(f->authorized == 3 && f->joined == 1);
    CHECK(moq_pub_active_subscriptions(f->publisher, w) == 0);
    CHECK(moq_pub_active_subscriptions(f->publisher, x) == 0);
    CHECK(request_pending(f, false, old_sub._opaque) && request_pending(f, false, other_sub._opaque));
    CHECK(request_pending(f, true, old_fetch._opaque) && request_pending(f, true, other_fetch._opaque));
    CHECK(moq_rcbuf_refcount(f->payload) == refs + 2);
    CHECK(moq_rcbuf_refcount(f->properties) == 4); /* caller, cache, two snapshots */
    CHECK(retain_probe(f, true));
    CHECK(moq_rcbuf_refcount(f->payload) == refs + 1);
    CHECK(moq_rcbuf_refcount(f->properties) == 3);
    for (unsigned i = 0; i < 16; i++) {
        CHECK(moq_pub_flush(f->publisher, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(f->sender->action_tail - f->sender->action_head == f->sender->action_cap);
        CHECK(f->sender->event_head == f->sender->event_tail);
        CHECK(moq_rcbuf_refcount(f->payload) == refs + 1);
        CHECK(moq_rcbuf_refcount(f->properties) == 3);
        CHECK(f->authorized == 3 && f->joined == 1);
    }
    if (cancel) {
        CHECK(moq_session_unsubscribe(f->peer, f->replies[0].sub, f->now) == MOQ_OK);
        CHECK(moq_session_fetch_cancel(f->peer, f->replies[0].fetch, f->now) == MOQ_OK);
    }
    CHECK(settle(f));
    CHECK(f->replies[0].acks == (cancel ? 0u : 1u));
    CHECK(f->replies[0].oks == (cancel ? 0u : 1u));
    CHECK(f->replies[0].objects == (cancel ? 0u : 1u));
    CHECK(f->replies[0].completes == (cancel ? 0u : 1u));
    CHECK(f->replies[1].acks == 1 && f->replies[1].oks == 1);
    CHECK(f->replies[1].objects == 1 && f->replies[1].completes == 1);
    CHECK(f->replies[0].errors == 0 && f->replies[1].errors == 0);
    CHECK(f->joined == (cancel ? 2u : 3u) && f->authorized == 3 && f->left == 0);
    CHECK(moq_rcbuf_refcount(f->payload) == 1 && moq_rcbuf_refcount(f->properties) == 1);
    if (cancel) {
        f->replies[2].replacement = true;
        CHECK(request_track(f, "w", &w, &f->replies[2].sub));
        CHECK(request_probe_fetch(f, 2));
        CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
        CHECK(peer_events(f));
        CHECK(f->sender->event_tail - f->sender->event_head == 2);
        a = &f->sender->events[f->sender->event_head % f->sender->event_cap];
        b = &f->sender->events[(f->sender->event_head + 1) % f->sender->event_cap];
        CHECK(a->kind == MOQ_EVENT_SUBSCRIBE_REQUEST && b->kind == MOQ_EVENT_FETCH_REQUEST);
        CHECK(moq_handle_slot(a->u.subscribe_request.sub._opaque) == moq_handle_slot(old_sub._opaque));
        CHECK(a->u.subscribe_request.sub._opaque != old_sub._opaque);
        CHECK(moq_handle_slot(b->u.fetch_request.fetch._opaque) == moq_handle_slot(old_fetch._opaque));
        CHECK(b->u.fetch_request.fetch._opaque != old_fetch._opaque);
        CHECK(fill_action_credit(f, false));
        CHECK(sender_events(f));
        CHECK(f->authorized == 4 && f->joined == 2);
        moq_event_t stale;
        memset(&stale, 0, sizeof(stale));
        stale.kind = MOQ_EVENT_UNSUBSCRIBED;
        stale.detail_size = sizeof(stale.u.unsubscribed);
        stale.u.unsubscribed.sub = old_sub;
        moq_pub_event_result_t result;
        CHECK(moq_pub_handle_event(f->publisher, &stale, f->now, &result) == MOQ_OK);
        CHECK(result == MOQ_PUB_EVENT_IGNORED);
        stale.kind = MOQ_EVENT_FETCH_CANCELLED;
        stale.detail_size = sizeof(stale.u.fetch_cancelled);
        stale.u.fetch_cancelled.fetch = old_fetch;
        CHECK(moq_pub_handle_event(f->publisher, &stale, f->now, &result) == MOQ_OK);
        CHECK(result == MOQ_PUB_EVENT_IGNORED);
        CHECK(moq_pub_clear_retained_group(f->publisher, f->track) == MOQ_OK);
        CHECK(settle(f));
        CHECK(f->replies[2].acks == 1 && f->replies[2].oks == 1);
        CHECK(f->replies[2].objects == 1 && f->replies[2].completes == 1 && f->replies[2].errors == 0);
        CHECK(f->authorized == 4 && f->joined == 3 && f->left == 0);
    }
    CHECK(moq_pub_flush(f->publisher, f->now) == MOQ_OK);
    return true;
}

static bool send_legacy_update(fixture_t *f, bool publication, bool resume, bool joining)
{
    unsigned acks = f->update_acks[publication];
    if (publication) {
        moq_publication_update_cfg_t c;
        moq_publication_update_cfg_init_sized(&c, sizeof(c));
        c.has_forward = true;
        c.forward = resume;
        c.has_subscriber_priority = !resume;
        c.subscriber_priority = 17;
        c.has_delivery_timeout = resume;
        c.delivery_timeout_us = 5000;
        CHECK(moq_session_update_publication(f->peer, f->peer_pub, &c, f->now) == MOQ_OK);
    } else {
        moq_subscription_update_cfg_t c;
        moq_subscription_update_cfg_init_sized(&c, sizeof(c));
        c.has_forward = true;
        c.forward = resume;
        c.has_subscriber_priority = !resume;
        c.subscriber_priority = 17;
        c.has_delivery_timeout = resume;
        c.delivery_timeout_us = 5000;
        c.has_filter = !resume;
        c.filter = joining ? MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT : MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        c.start_group = c.start_object = 0;
        c.end_group = 3;
        CHECK(moq_session_update_subscription(f->peer, f->peer_sub, &c, f->now) == MOQ_OK);
    }
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    CHECK(peer_events(f));
    CHECK(f->update_acks[publication] == acks + 1);
    return true;
}

static bool legacy_auto_updates(fixture_t *f, bool publication, bool joining)
{
    CHECK(!f->sender->defer_subscription_updates);
    CHECK(write_object(f, 0, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(watch(f, 0));
    uint32_t sg_slot = moq_handle_slot(f->stopped_handle._opaque);
    CHECK(sg_slot < f->sender->sg_cap);
    moq_subscription_t sub = f->sender->subgroups[sg_slot].sub;
    moq_publication_t pub = f->sender->subgroups[sg_slot].pub;
    uint32_t slot = moq_handle_slot(publication ? pub._opaque : sub._opaque);
    uint32_t prior_filter = publication ? f->sender->publishes[slot].filter_type
                                        : f->sender->subs[slot].filter_type;
    if (joining && !publication) {
        /* This control seeds the initial Largest at the same Location as the
         * later UPDATE acknowledgements; it does not test a changed basis. */
        const moq_sub_entry_t *e = &f->sender->subs[slot];
        CHECK(e->has_largest && e->largest_group == 0 && e->largest_object == 0);
        uint32_t peer_slot = moq_handle_slot(f->peer_sub._opaque);
        CHECK(peer_slot < f->peer->sub_cap);
        const moq_sub_entry_t *pe = &f->peer->subs[peer_slot];
        CHECK(moq_subscription_eq(pe->handle, f->peer_sub));
        CHECK(pe->has_largest && pe->largest_group == 0 && pe->largest_object == 0);
    }
    unsigned updates = f->updated, forwards = f->forward_changes;
    if (joining) CHECK(retain_probe(f, false));
    /* Both wire ACKs complete without facade polling. Their ordinary FIFO
     * notifications still have to preserve the effective cumulative state. */
    CHECK(send_legacy_update(f, publication, false, joining));
    CHECK(send_legacy_update(f, publication, true, joining));
    CHECK(f->sender->event_tail - f->sender->event_head == 2);
    for (size_t i = f->sender->event_head; i < f->sender->event_tail; i++) {
        const moq_event_t *e = &f->sender->events[i % f->sender->event_cap];
        if (publication) {
            CHECK(e->kind == MOQ_EVENT_PUBLISH_UPDATED);
            CHECK(!e->u.publish_updated.answer_owed && e->u.publish_updated.options == NULL);
            CHECK(e->u.publish_updated.update_count == 1);
        } else {
            CHECK(e->kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
            CHECK(!e->u.subscribe_updated.answer_owed && e->u.subscribe_updated.options == NULL);
            CHECK(e->u.subscribe_updated.update_count == 1);
        }
    }
    if (publication) {
        const moq_pub_entry_t *e = &f->sender->publishes[slot];
        CHECK(e->handle._opaque == pub._opaque && e->send_allowed && e->subscriber_priority == 17);
        CHECK(e->filter_type == prior_filter && !e->facade_defer_updates && !e->owed_update.owed);
        CHECK(e->dt_sub_has_object && e->dt_sub_object_ms == 5);
        if (joining) CHECK(e->has_joining_loc && e->joining_group == 0 && e->joining_object == 0);
    } else {
        const moq_sub_entry_t *e = &f->sender->subs[slot];
        CHECK(e->handle._opaque == sub._opaque && e->forward && e->subscriber_priority == 17);
        CHECK(e->filter_type == (joining ? MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT : MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE));
        if (joining) CHECK(e->has_largest && e->largest_group == 0 && e->largest_object == 0);
        else CHECK(e->req_end_group == 3);
        CHECK(!e->facade_defer_updates && !e->owed_update.owed);
        CHECK(e->dt_sub_has_object && e->dt_sub_object_ms == 5);
    }
    if (joining) {
        CHECK(f->update_has_largest[publication]);
        CHECK(f->update_largest_group[publication] == 0 && f->update_largest_object[publication] == 0);
        moq_fetch_cfg_t c;
        moq_fetch_cfg_init_sized(&c, sizeof(c));
        c.is_joining = c.joining_relative = true;
        c.joining_start = 0;
        c.joining_sub = publication ? MOQ_SUBSCRIPTION_INVALID : f->peer_sub;
        c.joining_pub = publication ? f->peer_pub : MOQ_PUBLICATION_INVALID;
        f->replies[0].properties = true;
        CHECK(moq_session_fetch(f->peer, &c, f->now, &f->replies[0].fetch) == MOQ_OK);
        CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
        CHECK(peer_events(f));
        CHECK(f->sender->event_tail - f->sender->event_head == 3);
        const moq_event_t *e = &f->sender->events[(f->sender->event_tail - 1) % f->sender->event_cap];
        CHECK(e->kind == MOQ_EVENT_FETCH_REQUEST);
        CHECK(e->u.fetch_request.start_group == 0 && e->u.fetch_request.start_object == 0);
        CHECK(e->u.fetch_request.end_group == 0 && e->u.fetch_request.end_object == 1);
        CHECK(publication ? moq_publication_eq(e->u.fetch_request.joining_pub, pub)
                          : moq_subscription_eq(e->u.fetch_request.joining_sub, sub));
    }
    CHECK(fill_action_credit(f, false));
    CHECK(sender_events(f));
    CHECK(f->sender->event_head == f->sender->event_tail);
    CHECK(f->updated == updates + (publication ? 0u : 2u));
    CHECK(f->forward_changes == forwards + (publication ? 2u : 0u));
    for (unsigned i = 0; i < 16; i++) {
        CHECK(moq_pub_flush(f->publisher, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(f->updated == updates + (publication ? 0u : 2u));
        CHECK(f->forward_changes == forwards + (publication ? 2u : 0u));
    }
    CHECK(settle(f));
    CHECK(f->stopped_resets == 1 && f->stopped_reset_code == 0 && f->stopped_fins == 0);
    if (joining) {
        CHECK(f->replies[0].oks == 1 && f->replies[0].objects == 1);
        CHECK(f->replies[0].completes == 1 && f->replies[0].errors == 0);
        CHECK(moq_pub_clear_retained_group(f->publisher, f->track) == MOQ_OK);
    }
    CHECK(write_object(f, 1, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][1][0] == 1 && f->received[1][1][0] == 1);
    CHECK(f->left == 0 && f->reopened == 0 && f->joined == 1);
    CHECK(moq_pub_active_subscriptions(f->publisher, f->track) == 1);
    CHECK(moq_pub_track_forward(f->publisher, f->track));
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
    CHECK(f->terminals == 0);
#endif
    CHECK(moq_pub_flush(f->publisher, f->now) == MOQ_OK);
    return true;
}

static bool serialized_request_terminal_pressure(fixture_t *f, bool fetch, bool stop)
{
    CHECK(write_object(f, 0, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(watch(f, 0));
    CHECK(f->sender->action_tail == f->sender->action_head);
    CHECK(f->sender->event_tail == f->sender->event_head);
    moq_pub_track_t *pending_track = NULL;
    moq_bytes_t ns = MOQ_BYTES_LITERAL("terminal");
    if (fetch) {
        moq_pub_retained_object_t object = { .object_id = 0, .payload = f->payload };
        moq_pub_retained_group_cfg_t retained;
        moq_pub_retained_group_cfg_init(&retained);
        retained.group_id = 0;
        retained.objects = &object;
        retained.object_count = 1;
        CHECK(moq_pub_set_retained_group(f->publisher, f->track, &retained) == MOQ_OK);
        moq_fetch_cfg_t c;
        moq_fetch_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = (moq_namespace_t){&ns, 1};
        c.track_name = MOQ_BYTES_LITERAL("v");
        c.start_group = c.start_object = c.end_group = 0;
        c.end_object = 1; /* explicit end, not a whole-group seal */
        CHECK(moq_session_fetch(f->peer, &c, f->now, &f->pending_peer_fetch) == MOQ_OK);
    } else {
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
        tc.track_namespace = (moq_namespace_t){&ns, 1};
        tc.track_name = MOQ_BYTES_LITERAL("w");
        tc.advertise_namespace = false;
        CHECK(moq_pub_add_track(f->publisher, &tc, f->now, &pending_track) == MOQ_OK);
        moq_subscribe_cfg_t c;
        moq_subscribe_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = (moq_namespace_t){&ns, 1};
        c.track_name = MOQ_BYTES_LITERAL("w");
        c.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        CHECK(moq_session_subscribe(f->peer, &c, f->now, &f->pending_peer_sub) == MOQ_OK);
    }
    /* Decode the real request, without giving the facade a chance to answer. */
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    CHECK(peer_events(f));
    CHECK(f->sender->event_tail - f->sender->event_head == 1);
    const moq_event_t *request = &f->sender->events[f->sender->event_head % f->sender->event_cap];
    CHECK(request->kind == (fetch ? MOQ_EVENT_FETCH_REQUEST : MOQ_EVENT_SUBSCRIBE_REQUEST));
    uint64_t identity = fetch ? request->u.fetch_request.fetch._opaque
                              : request->u.subscribe_request.sub._opaque;
    CHECK(identity != 0 && request_pending(f, fetch, identity));
    if (fetch) {
        CHECK(moq_fetch_is_valid(f->pending_peer_fetch));
        CHECK(request->u.fetch_request.start_group == 0 && request->u.fetch_request.start_object == 0);
        CHECK(request->u.fetch_request.end_group == 0 && request->u.fetch_request.end_object == 1);
    } else {
        CHECK(moq_subscription_is_valid(f->pending_peer_sub));
        CHECK(request->u.subscribe_request.track_name.len == 1);
        CHECK(request->u.subscribe_request.track_name.data[0] == 'w');
    }

    /* Already-open streams make one queued object action per destination.
     * The first-installed destination must occupy the queue head. */
    CHECK(write_object(f, 0, 1, false) == MOQ_OK);
    CHECK(f->sender->action_tail - f->sender->action_head == 2);
    const moq_action_t *head = &f->sender->actions[f->sender->action_head % f->sender->action_cap];
    CHECK(head->kind == MOQ_ACTION_SEND_DATA);
    CHECK(head->u.send_data.stream_ref._v == f->stopped_ref._v);
    CHECK(head->u.send_data.payload == f->payload);
    CHECK(fill_action_credit(f, false));
    CHECK(f->sender->action_tail - f->sender->action_head == f->sender->action_cap);
    unsigned target_actions = 0;
    for (size_t i = f->sender->action_head; i < f->sender->action_tail; i++) {
        const moq_action_t *a = &f->sender->actions[i % f->sender->action_cap];
        if (a->kind == MOQ_ACTION_SEND_DATA && a->u.send_data.stream_ref._v == f->stopped_ref._v)
            ++target_actions;
    }
    CHECK(target_actions == 1); /* STOP must replace, not free, the sole slot. */
    size_t refs = moq_rcbuf_refcount(f->payload);
    if (f->manual) {
        moq_event_t event;
        size_t n = 0;
        CHECK(moq_session_poll_events_ex(f->sender, &event, 1, sizeof(event), &n) == MOQ_OK);
        CHECK(n == 1 && event.kind == (fetch ? MOQ_EVENT_FETCH_REQUEST : MOQ_EVENT_SUBSCRIBE_REQUEST));
        CHECK((fetch ? event.u.fetch_request.fetch._opaque : event.u.subscribe_request.sub._opaque) == identity);
        moq_pub_event_result_t result;
        moq_result_t rc = moq_pub_handle_event(f->publisher, &event, f->now, &result);
        moq_event_cleanup(&event);
        CHECK(rc == MOQ_ERR_WOULD_BLOCK && result == MOQ_PUB_EVENT_CONSUMED);
    } else {
        CHECK(moq_pub_tick(f->publisher, f->now) == MOQ_ERR_WOULD_BLOCK);
    }
    CHECK(f->sender->event_head == f->sender->event_tail);
    CHECK(request_pending(f, fetch, identity));
    if (fetch) {
        CHECK(moq_rcbuf_refcount(f->payload) == refs + 1); /* retained request snapshot */
        CHECK(moq_pub_clear_retained_group(f->publisher, f->track) == MOQ_OK);
        CHECK(moq_rcbuf_refcount(f->payload) == refs);
    } else {
        CHECK(moq_pub_active_subscriptions(f->publisher, pending_track) == 0);
    }
    CHECK(moq_pub_flush(f->publisher, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(f->pending_acks == 0 && f->fetch_oks == 0 && f->pending_errors == 0);

    /* Only event credit is synthetic: no request or outgoing notice is injected. */
    CHECK(fill_event_credit(f));
    if (stop) {
        CHECK(moq_session_on_data_stop(f->sender, f->stopped_ref, 0, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(terminal_barrier(f));
    } else {
        CHECK(f->sender->actions[f->sender->action_head % f->sender->action_cap].kind == MOQ_ACTION_SEND_DATA);
    }
    CHECK(request_pending(f, fetch, identity));
    CHECK(f->sender->event_tail - f->sender->event_head == f->sender->event_cap);
    size_t action_head = f->sender->action_head, action_tail = f->sender->action_tail;
    size_t event_head = f->sender->event_head, event_tail = f->sender->event_tail;
    size_t held_refs = moq_rcbuf_refcount(f->payload);
    unsigned blocked = 0;
    size_t handled = 0;
    bool completed = false;
    for (unsigned i = 0; i < 16; i++) {
        CHECK(pressure_step(f, &blocked, &handled));
        CHECK(f->pending_errors == 0);
        completed = (fetch ? f->fetch_oks == 1 && f->fetch_objects == 1 && f->fetch_completes == 1
                           : f->pending_acks == 1 && moq_pub_active_subscriptions(f->publisher, pending_track) == 1)
            && f->sender->action_head == f->sender->action_tail
            && f->sender->event_head == f->sender->event_tail
            && f->stopped_resets == (stop ? 1u : 0u);
        if (completed) break;
    }
    if (!completed && blocked == 16 && handled == 0) {
        CHECK(f->sender->action_head == action_head && f->sender->action_tail == action_tail);
        CHECK(f->sender->event_head == event_head && f->sender->event_tail == event_tail);
        CHECK(request_pending(f, fetch, identity));
        CHECK(moq_rcbuf_refcount(f->payload) == held_refs);
        CHECK(f->pending_acks == 0 && f->fetch_oks == 0 && f->fetch_objects == 0 && f->fetch_completes == 0);
        fprintf(stderr, "%s: no progress in 16 drivers; actions=%zu/%zu events=%zu/%zu request pending\n",
            case_name, action_tail - action_head, f->sender->action_cap,
            event_tail - event_head, f->sender->event_cap);
    }
    CHECK(completed); /* bounded completion, not acceptance of a stalled driver */
    CHECK(f->received[!f->stop_publication][0][1] == 1);
    CHECK(f->received[f->stop_publication][0][1] == (stop ? 0u : 1u));
    CHECK(f->post_stop_payloads == (stop ? 0u : 1u));
    CHECK(f->left == 0 && f->reopened == 0);
    CHECK(moq_pub_active_subscriptions(f->publisher, f->track) == 1);
    CHECK(moq_pub_track_forward(f->publisher, f->track));
    return true;
}

static bool finite_window_terminal_pressure(fixture_t *f, bool stop)
{
    moq_subscription_update_cfg_t update;
    moq_subscription_update_cfg_init_sized(&update, sizeof(update));
    update.has_filter = true;
    update.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    update.start_group = update.start_object = update.end_group = 0;
    CHECK(moq_session_update_subscription(f->peer, f->peer_sub, &update, f->now) == MOQ_OK);
    CHECK(settle(f));
    unsigned finite = 0;
    for (size_t i = 0; i < f->sender->sub_cap; i++) {
        const moq_sub_entry_t *e = &f->sender->subs[i];
        if (e->state != MOQ_SUB_ESTABLISHED || e->role != MOQ_SUB_ROLE_PUBLISHER) continue;
        CHECK(e->filter_type == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE);
        CHECK(e->req_start_group == 0 && e->req_start_object == 0 && e->req_end_group == 0);
        ++finite;
    }
    CHECK(finite == 1);
    CHECK(write_object(f, 0, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(watch(f, 0));
    CHECK(f->received[0][0][0] == 1 && f->received[1][0][0] == 1);
    CHECK(f->sender->action_head == f->sender->action_tail);
    CHECK(fill_event_credit(f));
    if (stop) {
        CHECK(moq_session_on_data_stop(f->sender, f->stopped_ref, 0, f->now) == MOQ_ERR_WOULD_BLOCK);
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
        uint32_t slot = moq_handle_slot(f->stopped_handle._opaque);
        CHECK(slot < f->sender->sg_cap);
        CHECK(f->sender->subgroups[slot].state == MOQ_SG_TERMINAL_OWED);
        CHECK(f->sender->subgroups[slot].generation == moq_handle_generation(f->stopped_handle._opaque));
#endif
    }
    bool d22 = moq_session_supports_fill(f->sender);
    moq_result_t rc = moq_pub_declare_groups_complete_through(f->publisher, f->track, 0, f->now);
    if (stop && !d22) {
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        CHECK(moq_pub_active_subscriptions(f->publisher, f->track) == 1);
    } else {
        CHECK(rc == MOQ_OK);
    }
    size_t action_head = f->sender->action_head, action_tail = f->sender->action_tail;
    size_t event_head = f->sender->event_head, event_tail = f->sender->event_tail;
    size_t refs = moq_rcbuf_refcount(f->payload);
    unsigned blocked = 0;
    size_t handled = 0;
    bool completed = false;
    for (unsigned i = 0; i < 16; i++) {
        CHECK(pressure_step(f, &blocked, &handled));
        completed = f->sender->action_head == f->sender->action_tail
            && f->sender->event_head == f->sender->event_tail
            && f->stopped_resets == (stop ? 1u : 0u)
            && (d22 ? f->done_received == 0 && moq_pub_active_subscriptions(f->publisher, f->track) == 1
                    : f->done_received == 1 && moq_pub_active_subscriptions(f->publisher, f->track) == 0);
        if (completed) break;
    }
    if (!completed && blocked == 16 && handled == 0) {
        CHECK(f->sender->action_head == action_head && f->sender->action_tail == action_tail);
        CHECK(f->sender->event_head == event_head && f->sender->event_tail == event_tail);
        CHECK(moq_pub_active_subscriptions(f->publisher, f->track) == 1);
        CHECK(moq_rcbuf_refcount(f->payload) == refs);
        CHECK(f->done_received == 0 && f->stopped_resets == 0 && f->stopped_fins == 0);
        fprintf(stderr, "%s: no progress in 16 drivers; finite owner retained, events=%zu/%zu\n",
            case_name, event_tail - event_head, f->sender->event_cap);
    }
    CHECK(completed);
    CHECK(f->stopped_fins == (!stop && !d22 ? 1u : 0u));
    /* Only group 0 was opened on the finite pull before the event pressure.
     * FIN or identifiable RESET completes that one observed stream. */
    if (!d22) CHECK(f->dones == 1 && f->done_streams == 1 && f->done_status == 0x3);
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
    if (f->manual) CHECK(f->terminals == (stop ? 1u : 0u));
#endif
    CHECK(f->left == 0 && f->post_stop_payloads == 0 && f->reopened == 0);
    CHECK(moq_pub_track_forward(f->publisher, f->track));
    CHECK(write_object(f, 1, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][1][0] == 0 && f->received[1][1][0] == 1);
    CHECK(f->done_received == (d22 ? 0u : 1u));
    return true;
}

static bool ordinary_retirement_terminal_pressure(fixture_t *f, bool stop)
{
    CHECK(write_object(f, 0, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(watch(f, 0));
    CHECK(f->stop_publication); /* A is the publication; B is the paused pull. */
    size_t other_slot = f->sender->sg_cap;
    for (size_t i = 0; i < f->sender->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->sender->subgroups[i];
        if (sg->state == MOQ_SG_OPEN && sg->group_id == 0 && moq_subscription_is_valid(sg->sub)) {
            f->other_ref = sg->stream_ref;
            other_slot = i;
        }
    }
    CHECK(other_slot < f->sender->sg_cap && f->other_ref._v != f->stopped_ref._v);
    moq_subscription_update_cfg_t update;
    moq_subscription_update_cfg_init_sized(&update, sizeof(update));
    update.has_forward = true;
    update.forward = false;
    CHECK(moq_session_update_subscription(f->peer, f->peer_sub, &update, f->now) == MOQ_OK);
    CHECK(moq_simpair_run_until_quiescent(f->pair, 32, NULL) == MOQ_OK);
    CHECK(peer_events(f));
    CHECK(f->sender->event_tail - f->sender->event_head == 1);
    const moq_event_t *e = &f->sender->events[f->sender->event_head % f->sender->event_cap];
    CHECK(e->kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
    CHECK(moq_subscription_eq(e->u.subscribe_updated.sub, f->sender->subgroups[other_slot].sub));
    CHECK(e->u.subscribe_updated.has_forward && !e->u.subscribe_updated.forward);
    CHECK(e->detail_size >= offsetof(moq_subscribe_updated_event_t, answer_owed) + sizeof(e->u.subscribe_updated.answer_owed));
    CHECK(!e->u.subscribe_updated.answer_owed); /* no ACK debt in this witness */

    /* Queue real output only on A: B's core Forward state is already false,
     * while its facade update remains undispatched. No ring action is forged. */
    CHECK(moq_session_write_object(f->sender, f->stopped_handle, 1, f->payload, f->now) == MOQ_OK);
    CHECK(f->sender->action_tail - f->sender->action_head == 1);
    CHECK(fill_action_credit(f, true));
    if (f->manual) {
        moq_event_t event;
        size_t n = 0;
        CHECK(moq_session_poll_events_ex(f->sender, &event, 1, sizeof(event), &n) == MOQ_OK);
        CHECK(n == 1 && event.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
        moq_pub_event_result_t result;
        moq_result_t rc = moq_pub_handle_event(f->publisher, &event, f->now, &result);
        moq_event_cleanup(&event);
        CHECK(rc == MOQ_ERR_WOULD_BLOCK && result == MOQ_PUB_EVENT_CONSUMED);
    } else {
        CHECK(moq_pub_tick(f->publisher, f->now) == MOQ_ERR_WOULD_BLOCK);
    }
    CHECK(f->sender->event_head == f->sender->event_tail);
    CHECK(f->sender->subgroups[other_slot].state == MOQ_SG_OPEN);
    CHECK(moq_pub_flush(f->publisher, f->now) == MOQ_ERR_WOULD_BLOCK);
    CHECK(fill_event_credit(f));
    if (stop) {
        CHECK(moq_session_on_data_stop(f->sender, f->stopped_ref, 0, f->now) == MOQ_ERR_WOULD_BLOCK);
        CHECK(terminal_barrier(f));
    }
    size_t action_head = f->sender->action_head, action_tail = f->sender->action_tail;
    size_t event_head = f->sender->event_head, event_tail = f->sender->event_tail;
    size_t refs = moq_rcbuf_refcount(f->payload);
    unsigned blocked = 0;
    size_t handled = 0;
    bool completed = false;
    for (unsigned i = 0; i < 16; i++) {
        CHECK(pressure_step(f, &blocked, &handled));
        completed = f->other_resets == 1 && f->stopped_resets == (stop ? 1u : 0u)
            && f->sender->action_head == f->sender->action_tail
            && f->sender->event_head == f->sender->event_tail;
        if (completed) break;
    }
    if (!completed && blocked == 16 && handled == 0) {
        CHECK(f->sender->action_head == action_head && f->sender->action_tail == action_tail);
        CHECK(f->sender->event_head == event_head && f->sender->event_tail == event_tail);
        CHECK(f->sender->subgroups[other_slot].state == MOQ_SG_OPEN);
        CHECK(moq_rcbuf_refcount(f->payload) == refs);
        CHECK(f->other_resets == 0 && f->stopped_resets == 0);
        fprintf(stderr, "%s: no progress in 16 drivers; ordinary retirement open, actions=%zu/%zu events=%zu/%zu\n",
            case_name, action_tail - action_head, f->sender->action_cap,
            event_tail - event_head, f->sender->event_cap);
    }
    CHECK(completed);
    CHECK(f->other_fins == 0 && f->stopped_fins == 0 && f->done_received == 0);
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
    if (f->manual) CHECK(f->terminals == (stop ? 1u : 0u));
#endif
    CHECK(f->received[1][0][1] == (stop ? 0u : 1u));
    CHECK(f->post_stop_payloads == (stop ? 0u : 1u));
    CHECK(f->left == 0 && f->reopened == 0);
    CHECK(moq_pub_active_subscriptions(f->publisher, f->track) == 1);
    CHECK(moq_pub_track_forward(f->publisher, f->track));
    update.forward = true;
    CHECK(moq_session_update_subscription(f->peer, f->peer_sub, &update, f->now) == MOQ_OK);
    CHECK(settle(f));
    CHECK(write_object(f, 1, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][1][0] == 1 && f->received[1][1][0] == 1);
    CHECK(moq_pub_finish_subscribers(f->publisher, f->track, 2, f->now) == MOQ_OK);
    CHECK(settle(f));
    /* The paused pull's original group 0 still counts after retirement;
     * resuming opens group 1. Both headers were admitted through SimPair. */
    CHECK(f->done_received == 1 && f->dones == 1);
    CHECK(f->done_streams == 2 && f->done_status == 2);
    CHECK(f->other_resets == 1 && f->stopped_resets == (stop ? 1u : 0u));
    return true;
}

#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
static bool identity_controls(fixture_t *f)
{
    CHECK(write_object(f, 0, 0, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(watch(f, 0));
    CHECK(terminate(f, false));
    CHECK(settle(f));
    CHECK(f->terminals == 1 && f->terminal.detail_size == 64);
    moq_event_t old = f->terminal, bad = old;
    moq_pub_event_result_t result;
    bad.detail_size = 63;
    CHECK(moq_pub_handle_event(f->publisher, &bad, f->now, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_IGNORED);
    bad = old;
    /* Both owners must be structurally valid; a raw nonzero integer is not. */
    for (size_t i = 0; i < f->sender->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->sender->subgroups[i];
        if (sg->state != MOQ_SG_OPEN) continue;
        if (f->stop_publication && moq_subscription_is_valid(sg->sub))
            bad.u.outgoing_subgroup_terminated.sub = sg->sub;
        if (!f->stop_publication && moq_publication_is_valid(sg->pub))
            bad.u.outgoing_subgroup_terminated.pub = sg->pub;
    }
    CHECK(moq_subscription_is_valid(bad.u.outgoing_subgroup_terminated.sub));
    CHECK(moq_publication_is_valid(bad.u.outgoing_subgroup_terminated.pub));
    CHECK(moq_pub_handle_event(f->publisher, &bad, f->now, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_IGNORED);
    bad = old;
    bad.u.outgoing_subgroup_terminated.subgroup._opaque = moq_handle_pack(
        MOQ_HANDLE_POOL_SUBGROUP, (uint16_t)(f->sender->session_tag ^ 1u),
        moq_handle_generation(old.u.outgoing_subgroup_terminated.subgroup._opaque),
        moq_handle_slot(old.u.outgoing_subgroup_terminated.subgroup._opaque));
    CHECK(moq_pub_handle_event(f->publisher, &bad, f->now, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_IGNORED);
    bad = old;
    bad.u.outgoing_subgroup_terminated.cause = UINT32_MAX;
    CHECK(moq_pub_handle_event(f->publisher, &bad, f->now, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_IGNORED);
    CHECK(moq_pub_reset_group(f->publisher, f->track, 0, f->now) == MOQ_OK);
    CHECK(settle(f));
    CHECK(write_object(f, 0, 2, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(moq_pub_handle_event(f->publisher, &old, f->now, &result) == MOQ_OK);
    CHECK(write_object(f, 0, 3, false) == MOQ_OK);
    CHECK(settle(f));
    CHECK(f->received[0][0][3] == 1 && f->received[1][0][3] == 1);
    if (f->stop_publication) {
        CHECK(moq_pub_unpublish_track(f->publisher, f->track, f->now) == MOQ_OK);
        CHECK(settle(f));
        moq_pub_publish_cfg_t c;
        moq_pub_publish_cfg_init(&c);
        CHECK(moq_pub_publish_track(f->publisher, f->track, &c, f->now) == MOQ_OK);
        CHECK(settle(f));
    } else {
        CHECK(moq_session_unsubscribe(f->peer, f->peer_sub, f->now) == MOQ_OK);
        CHECK(settle(f));
        CHECK(subscribe(f));
    }
    CHECK(moq_pub_handle_event(f->publisher, &old, f->now, &result) == MOQ_OK);
    CHECK(result == MOQ_PUB_EVENT_IGNORED);
    CHECK(write_object(f, 1, 0, false) == MOQ_OK);
    bool reused = false;
    for (size_t i = 0; i < f->sender->sg_cap; i++) {
        const moq_sg_entry_t *sg = &f->sender->subgroups[i];
        if (sg->state == MOQ_SG_FREE || sg->group_id != 1 ||
            moq_publication_is_valid(sg->pub) != f->stop_publication) continue;
        uint64_t before = f->stop_publication ? old.u.outgoing_subgroup_terminated.pub._opaque
                                              : old.u.outgoing_subgroup_terminated.sub._opaque;
        uint64_t after = f->stop_publication ? sg->pub._opaque : sg->sub._opaque;
        CHECK(moq_handle_slot(before) == moq_handle_slot(after));
        CHECK(moq_handle_generation(before) != moq_handle_generation(after));
        reused = true;
    }
    CHECK(reused);
    CHECK(settle(f));
    CHECK(f->received[0][1][0] == 1 && f->received[1][1][0] == 1);
    return true;
}
#endif

int main(int argc, char **argv)
{
    bool pressure_only = argc == 2 && strcmp(argv[1], "--terminal-credit-pressure") == 0;
    bool owners_only = argc == 2 && strcmp(argv[1], "--fifo-ownership") == 0;
    bool full = argc == 1;
    if (!full && !pressure_only && !owners_only) {
        fprintf(stderr, "usage: %s [--terminal-credit-pressure|--fifo-ownership]\n", argv[0]);
        return 2;
    }
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); v++) {
        if (full) {
            for (unsigned pub = 0; pub < 2; pub++) {
                for (unsigned timed = 0; timed < 2; timed++) {
                    for (unsigned mode = 0; mode < 4; mode++) {
                        snprintf(case_name, sizeof(case_name), "isolation/version%u/publication%u/timeout%u/mode%u",
                            (unsigned)versions[v], pub, timed, mode);
                        fixture_t f;
                        if (setup(&f, versions[v], pub != 0, timed != 0, (mode & 1u) != 0, 32))
                            (void)isolation(&f, timed != 0, mode);
                        cleanup(&f);
                    }
                }
                fixture_t f;
                snprintf(case_name, sizeof(case_name), "blocked_retry/version%u/publication%u",
                    (unsigned)versions[v], pub);
                if (setup(&f, versions[v], pub != 0, false, true, 3)) (void)blocked_retry(&f);
                cleanup(&f);
            }
        }
        fixture_t f;
        if (full) {
            snprintf(case_name, sizeof(case_name), "history_reuse/version%u", (unsigned)versions[v]);
            if (setup(&f, versions[v], false, false, true, 32)) (void)history_reuse(&f);
            cleanup(&f);
            snprintf(case_name, sizeof(case_name), "generation_bound/version%u", (unsigned)versions[v]);
            if (setup(&f, versions[v], false, false, true, 32)) (void)generation_bound(&f);
            cleanup(&f);
        }
        if (!owners_only) for (unsigned manual = 0; manual < 2; manual++) {
            for (unsigned stop = 0; stop < 2; stop++) {
                snprintf(case_name, sizeof(case_name), "finite_window_terminal_pressure/version%u/manual%u/stop%u",
                    (unsigned)versions[v], manual, stop);
                if (setup(&f, versions[v], false, false, manual != 0, 32))
                    (void)finite_window_terminal_pressure(&f, stop != 0);
                cleanup(&f);
                if (versions[v] != MOQ_VERSION_DRAFT_22) {
                    snprintf(case_name, sizeof(case_name), "ordinary_retirement_terminal_pressure/version%u/manual%u/stop%u",
                        (unsigned)versions[v], manual, stop);
                    /* Install publication A first, independently of the driver. */
                    if (setup(&f, versions[v], true, false, false, 32)) {
                        f.manual = manual != 0;
                        (void)ordinary_retirement_terminal_pressure(&f, stop != 0);
                    }
                    cleanup(&f);
                }
            }
        }
        if (versions[v] != MOQ_VERSION_DRAFT_22) {
            if (full) {
                snprintf(case_name, sizeof(case_name), "serialized_retirement_credit/version%u", (unsigned)versions[v]);
                if (setup(&f, versions[v], false, false, true, 32)) (void)serialized_retirement_credit(&f);
                cleanup(&f);
            }
            if (!owners_only) for (unsigned fetch = 0; fetch < 2; fetch++) {
                for (unsigned manual = 0; manual < 2; manual++) {
                    for (unsigned stop = 0; stop < 2; stop++) {
                        snprintf(case_name, sizeof(case_name), "%s_terminal_pressure/version%u/manual%u/stop%u",
                            fetch ? "retained_fetch" : "pending_ack", (unsigned)versions[v], manual, stop);
                        if (setup(&f, versions[v], manual == 0, false, manual != 0, 32))
                            (void)serialized_request_terminal_pressure(&f, fetch != 0, stop != 0);
                        cleanup(&f);
                    }
                }
            }
            if (!pressure_only) for (unsigned manual = 0; manual < 2; manual++) {
                for (unsigned cancel = 0; cancel < 2; cancel++) {
                    snprintf(case_name, sizeof(case_name), "fifo_snapshot_owners/version%u/manual%u/cancel%u",
                        (unsigned)versions[v], manual, cancel);
                    if (setup_authorized(&f, versions[v], manual == 0, false, manual != 0, 32, true))
                        (void)fifo_snapshot_owners(&f, cancel != 0);
                    cleanup(&f);
                }
                for (unsigned publication = 0; publication < 2; publication++) {
                    snprintf(case_name, sizeof(case_name), "legacy_auto_updates/version%u/manual%u/publication%u",
                        (unsigned)versions[v], manual, publication);
                    if (setup(&f, versions[v], publication != 0, false, manual != 0, 32))
                        (void)legacy_auto_updates(&f, publication != 0, false);
                    cleanup(&f);
                    if (versions[v] == MOQ_VERSION_DRAFT_18) {
                        snprintf(case_name, sizeof(case_name), "joining_fetch_after_updates/version%u/manual%u/publication%u",
                            (unsigned)versions[v], manual, publication);
                        if (setup_configured(&f, versions[v], publication != 0, false, manual != 0,
                            32, false, publication == 0))
                            (void)legacy_auto_updates(&f, publication != 0, true);
                        cleanup(&f);
                    }
                }
            }
        }
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
        if (full) {
            for (unsigned publication = 0; publication < 2; publication++) {
                snprintf(case_name, sizeof(case_name), "identity_controls/version%u/publication%u",
                    (unsigned)versions[v], publication);
                if (setup(&f, versions[v], publication != 0, false, true, 32)) (void)identity_controls(&f);
                cleanup(&f);
            }
        }
#endif
    }
    unsigned expected_rows = pressure_only ? 36u : owners_only ? 20u : 118u;
#ifdef MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED
    if (full) expected_rows = 124;
#endif
    if (rows != expected_rows) {
        ++failures;
        fprintf(stderr, "publisher_subgroup_terminal: rows=%u, expected %u\n", rows, expected_rows);
    }
    printf("publisher_subgroup_terminal: %u rows, %u failures, selection=%s\n",
        rows, failures, pressure_only ? "terminal-credit-pressure" : owners_only ? "fifo-ownership" : "full");
    return failures != 0;
}
