/* Bounded generic response ownership, real request generations and FIFO wire
 * answers. The directional sim pump leaves server output deliberately held;
 * session pools are observed, never edited. */
#include <moq/publisher.h>
#include <moq/sim.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include "../../core/src/session/session_internal.h"
#include "../../core/src/wire/control_d18_internal.h"
#include "../../sim/src/simpair_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned checks, failures, prerequisites, rows;
static char row_name[112];
#define CHECK(e) do { ++checks; if (!(e)) { ++failures; \
    fprintf(stderr, "%s:%d: %s\n", row_name, __LINE__, #e); } } while (0)
#define NEED(e) do { ++checks; if (!(e)) { ++failures; ++prerequisites; \
    fprintf(stderr, "%s:%d: prerequisite %s\n", row_name, __LINE__, #e); \
    goto cleanup; } } while (0)

enum { ACTIONS = 8, RECORDS = 24, CANCELS = 512 };
typedef union allocation_header {
    max_align_t alignment;
    size_t size;
} allocation_header_t;
typedef struct memory {
    size_t calls, fail_at, live, bytes, sizes[16];
} memory_t;

static void *allocate(size_t size, void *ctx)
{
    memory_t *m = ctx;
    m->sizes[m->calls++ % 16] = size;
    if (m->calls == m->fail_at || size > SIZE_MAX - sizeof(allocation_header_t)) return NULL;
    allocation_header_t *h = malloc(sizeof(*h) + size);
    if (!h) return NULL;
    h->size = size;
    ++m->live;
    m->bytes += size;
    return h + 1;
}

static void release(void *ptr, size_t size, void *ctx)
{
    if (!ptr) return;
    memory_t *m = ctx;
    allocation_header_t *h = (allocation_header_t *)ptr - 1;
    CHECK(h->size == size && m->live > 0 && m->bytes >= h->size);
    m->bytes -= h->size;
    --m->live;
    free(h);
}

static void *resize(void *ptr, size_t old_size, size_t size, void *ctx)
{
    if (!ptr) return allocate(size, ctx);
    if (!size) { release(ptr, old_size, ctx); return NULL; }
    allocation_header_t *h = (allocation_header_t *)ptr - 1;
    CHECK(h->size == old_size);
    void *p = allocate(size, ctx);
    if (!p) return NULL;
    memcpy(p, ptr, size < old_size ? size : old_size);
    release(ptr, old_size, ctx);
    return p;
}

static moq_alloc_t allocator(memory_t *m)
{ return (moq_alloc_t){.ctx = m, .alloc = allocate, .realloc = resize, .free = release}; }

typedef enum family { STATUS, SUBSCRIBE, PUBLISH, NAMESPACE, NS_SUB, TRACKS } family_t;
typedef struct request {
    family_t family;
    uint64_t peer, server, id;
    moq_stream_ref_t ref;
    bool known, accepted;
    unsigned replies;
    moq_request_error_t error;
} request_t;
typedef struct rig {
    memory_t core_memory, pub_memory;
    moq_alloc_t core_alloc, pub_alloc;
    moq_simpair_t *pair;
    moq_session_t *client, *server;
    moq_publisher_t *pub;
    moq_pub_track_t *track;
    moq_version_t version;
    moq_subscription_t wire_client, wire_server;
    moq_publication_t publication, peer_publication;
    moq_rcbuf_t *payload;
    request_t requests[RECORDS];
    size_t count, next;
    uint64_t cancelled_ids[CANCELS];
    moq_stream_ref_t cancelled_refs[CANCELS];
    size_t cancelled_count;
    unsigned stale_wire, resets, closed, authorized, joined, left, updated, forwarded;
    bool defer_next;
    moq_pub_deferred_t *deferred;
    uint64_t deferred_id;
    moq_pub_subscribe_update_info_t last_update;
    uint64_t externally_answered;
    unsigned external_acks;
    unsigned pressure_serial, peer_pressure_requests, namespace_wire;
    bool check_namespace_sequence;
    uint64_t next_namespace_id;
} rig_t;

static moq_bytes_t live_ns = {(const uint8_t *)"response", 8};
static moq_bytes_t absent_ns = {(const uint8_t *)"absent", 6};

static size_t generic_capacity(const moq_session_t *s)
{ return s->sub_cap + s->pub_cap + s->ann_cap + s->ns_sub_cap + s->track_sub_cap + s->ts_cap; }

static void trace(void *ctx, const moq_sim_trace_record_t *e)
{
    rig_t *r = ctx;
    if (e->kind == MOQ_SIM_TRACE_INPUT && e->result < 0 && e->result != MOQ_ERR_WOULD_BLOCK)
        fprintf(stderr, "%s: transport input=%u to=%u rc=%d\n", row_name,
            (unsigned)e->input_kind, (unsigned)e->to, (int)e->result);
    if (e->kind != MOQ_SIM_TRACE_ACTION || e->from != MOQ_PERSPECTIVE_SERVER) return;
    if (e->action_kind == MOQ_ACTION_RESET_DATA) ++r->resets;
    if (e->action_kind == MOQ_ACTION_SEND_BIDI_STREAM) {
        for (size_t i = 0; i < r->cancelled_count; i++)
            if (e->stream_ref._v == r->cancelled_refs[i]._v) ++r->stale_wire;
    }
    if (r->version == MOQ_VERSION_DRAFT_16 && e->action_kind == MOQ_ACTION_SEND_CONTROL) {
        moq_buf_reader_t reader;
        moq_buf_reader_init(&reader, e->bytes.data, e->bytes.len);
        while (reader.pos < reader.len) {
            moq_control_envelope_t env;
            if (moq_control_decode_envelope(&reader, &env) != MOQ_OK) break;
            if (r->check_namespace_sequence && env.msg_type == MOQ_D16_PUBLISH_NAMESPACE) {
                moq_kvp_entry_t params[8];
                moq_bytes_t parts[32];
                moq_d16_publish_namespace_t ns = {.params = params, .params_cap = 8};
                moq_result_t rc = moq_d16_decode_publish_namespace(env.payload, env.payload_len,
                    parts, 32, &ns);
                CHECK(rc == MOQ_OK);
                if (rc == MOQ_OK) {
                    CHECK(ns.request_id == r->next_namespace_id);
                    r->next_namespace_id += 2;
                    ++r->namespace_wire;
                }
            }
            if (env.msg_type != MOQ_D16_REQUEST_ERROR) continue;
            moq_d16_request_error_t error;
            if (moq_d16_decode_request_error(env.payload, env.payload_len, &error) != MOQ_OK) break;
            for (size_t i = 0; i < r->cancelled_count; i++)
                if (error.request_id == r->cancelled_ids[i]) ++r->stale_wire;
        }
    }
}

static void on_closed(void *ctx, uint64_t code)
{ rig_t *r = ctx; CHECK(code == 0x99); ++r->closed; }
static void on_joined(void *ctx, moq_pub_track_t *t)
{ (void)t; ++((rig_t *)ctx)->joined; }
static void on_left(void *ctx, moq_pub_track_t *t)
{ (void)t; ++((rig_t *)ctx)->left; }
static void on_updated(void *ctx, moq_pub_track_t *t, const moq_pub_subscribe_update_info_t *u)
{ (void)t; rig_t *r = ctx; ++r->updated; r->last_update = *u; }
static void on_forwarded(void *ctx, moq_pub_track_t *t, bool forward)
{ (void)t; CHECK(forward); ++((rig_t *)ctx)->forwarded; }
static moq_pub_accept_decision_t authorize(void *ctx, const moq_pub_subscribe_info_t *info,
                                           moq_request_error_t *error)
{
    (void)error;
    rig_t *r = ctx;
    ++r->authorized;
    if (!r->defer_next) return MOQ_PUB_DECISION_ACCEPT;
    r->defer_next = false;
    r->deferred = info->deferred;
    r->deferred_id = info->deferred_id;
    return MOQ_PUB_DECISION_DEFER;
}

static bool discard_events(moq_session_t *s)
{
    moq_event_t event;
    size_t count;
    do {
        if (moq_session_poll_events_ex(s, &event, 1, sizeof(event), &count) != MOQ_OK) return false;
        if (count) moq_event_cleanup(&event);
    } while (count);
    return true;
}

static bool open_rig(rig_t *r, moq_version_t version, bool callback)
{
    r->version = version;
    r->core_alloc = allocator(&r->core_memory);
    r->pub_alloc = allocator(&r->pub_memory);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->core_alloc;
    cfg.version = version;
    cfg.max_actions = ACTIONS;
    cfg.max_events = 16;
    cfg.server_max_subscriptions = 4;
    /* Pending D16 cancellations pin one requester tombstone apiece. The
     * requester budget is independent of the publisher's measured G. */
    cfg.client_max_subscriptions = version == MOQ_VERSION_DRAFT_16 ? CANCELS : 4;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 65536;
    cfg.initial_now_us = 1000;
    cfg.trace_fn = trace;
    cfg.trace_ctx = r;
    if (moq_simpair_create(&cfg, &r->pair) != MOQ_OK ||
        moq_simpair_start(r->pair) != MOQ_OK ||
        moq_simpair_run_until_quiescent(r->pair, 32, NULL) != MOQ_OK) return false;
    r->client = moq_simpair_client(r->pair);
    r->server = moq_simpair_server(r->pair);
    if (!discard_events(r->client) || !discard_events(r->server)) return false;
    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    pc.accept_mode = callback ? MOQ_PUB_CALLBACK : MOQ_PUB_ACCEPT_ALL;
    pc.on_subscribe = callback ? authorize : NULL;
    pc.on_subscribe_ctx = callback ? r : NULL;
    pc.callbacks.ctx = r;
    pc.callbacks.on_closed = on_closed;
    pc.callbacks.on_subscriber_joined = on_joined;
    pc.callbacks.on_subscriber_left = on_left;
    pc.callbacks.on_subscriber_updated = on_updated;
    pc.callbacks.on_publish_forward_changed = on_forwarded;
    if (moq_pub_create(r->server, &r->pub_alloc, &pc, &r->pub) != MOQ_OK) return false;
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = (moq_namespace_t){&live_ns, 1};
    tc.track_name = MOQ_BYTES_LITERAL("video");
    tc.advertise_namespace = false;
    return moq_pub_add_track(r->pub, &tc, 1000, &r->track) == MOQ_OK;
}

static void close_rig(rig_t *r)
{
    moq_pub_destroy(r->pub);
    moq_simpair_destroy(r->pair);
    moq_rcbuf_decref(r->payload);
    CHECK(r->pub_memory.live == 0 && r->pub_memory.bytes == 0);
    CHECK(r->core_memory.live == 0 && r->core_memory.bytes == 0);
}

static bool client_only(rig_t *r)
{
    size_t forward = 0, reverse = 0;
    return pump_direction(r->pair, r->client, r->server, MOQ_PERSPECTIVE_CLIENT,
        MOQ_PERSPECTIVE_SERVER, &forward, &reverse) == MOQ_OK;
}

static bool server_only(rig_t *r, size_t *delivered)
{
    size_t reverse = 0;
    *delivered = 0;
    return pump_direction(r->pair, r->server, r->client, MOQ_PERSPECTIVE_SERVER,
        MOQ_PERSPECTIVE_CLIENT, delivered, &reverse) == MOQ_OK;
}

static uint64_t request_identity(const moq_event_t *e)
{
    switch (e->kind) {
    case MOQ_EVENT_TRACK_STATUS_REQUEST: return e->u.track_status_request.handle._opaque;
    case MOQ_EVENT_SUBSCRIBE_REQUEST: return e->u.subscribe_request.sub._opaque;
    case MOQ_EVENT_PUBLISH_REQUEST: return e->u.publish_request.pub._opaque;
    case MOQ_EVENT_NAMESPACE_PUBLISHED: return e->u.namespace_published.ann._opaque;
    case MOQ_EVENT_NS_SUB_REQUEST: return e->u.ns_sub_request.handle._opaque;
    case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: return e->u.subscribe_tracks_request.handle._opaque;
    default: return 0;
    }
}

static moq_event_kind_t request_kind(family_t family)
{
    static const moq_event_kind_t kinds[] = {MOQ_EVENT_TRACK_STATUS_REQUEST, MOQ_EVENT_SUBSCRIBE_REQUEST,
        MOQ_EVENT_PUBLISH_REQUEST, MOQ_EVENT_NAMESPACE_PUBLISHED, MOQ_EVENT_NS_SUB_REQUEST,
        MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST};
    return kinds[family];
}

static bool request(rig_t *r, family_t family, bool known, request_t *out)
{
    memset(out, 0, sizeof(*out));
    out->family = family;
    out->known = known;
    moq_namespace_t ns = {known ? &live_ns : &absent_ns, 1};
    moq_result_t rc;
    switch (family) {
    case STATUS: {
        moq_track_status_cfg_t c;
        moq_track_status_cfg_init(&c);
        c.track_namespace = ns; c.track_name = MOQ_BYTES_LITERAL("video");
        moq_track_status_handle_t h;
        rc = moq_session_track_status(r->client, &c, 1000, &h); out->peer = h._opaque;
        break;
    }
    case SUBSCRIBE: {
        moq_subscribe_cfg_t c;
        moq_subscribe_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = ns; c.track_name = MOQ_BYTES_LITERAL("video");
        c.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        moq_subscription_t h;
        rc = moq_session_subscribe(r->client, &c, 1000, &h); out->peer = h._opaque;
        break;
    }
    case PUBLISH: {
        moq_publish_cfg_t c;
        moq_publish_cfg_init_sized(&c, sizeof(c));
        c.track_namespace = ns; c.track_name = MOQ_BYTES_LITERAL("pushed");
        moq_publication_t h;
        rc = moq_session_publish(r->client, &c, 1000, &h); out->peer = h._opaque;
        break;
    }
    case NAMESPACE: {
        moq_publish_namespace_cfg_t c;
        moq_publish_namespace_cfg_init(&c); c.track_namespace = ns;
        moq_announcement_t h;
        rc = moq_session_publish_namespace(r->client, &c, 1000, &h); out->peer = h._opaque;
        break;
    }
    case NS_SUB: {
        moq_subscribe_namespace_cfg_t c;
        moq_subscribe_namespace_cfg_init(&c); c.track_namespace_prefix = ns;
        c.namespace_interest = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE;
        moq_ns_sub_handle_t h;
        rc = moq_session_subscribe_namespace(r->client, &c, 1000, &h); out->peer = h._opaque;
        break;
    }
    case TRACKS: {
        moq_subscribe_tracks_cfg_t c;
        moq_subscribe_tracks_cfg_init(&c); c.track_namespace_prefix = (moq_namespace_t){&live_ns, 1};
        moq_track_sub_handle_t h;
        rc = moq_session_subscribe_tracks(r->client, &c, 1000, &h); out->peer = h._opaque;
        break;
    }
    default: return false;
    }
    if (rc != MOQ_OK || !client_only(r)) return false;
    /* Capture scalar identity before the facade advances. The event itself
     * stays in the ordinary FIFO, with its borrowed spans untouched. */
    moq_session_t *s = r->server;
    for (size_t i = s->event_head; i < s->event_tail; i++) {
        const moq_event_t *e = &s->events[i % s->event_cap];
        if (e->kind != request_kind(family)) continue;
        out->server = request_identity(e);
    }
    if (!out->server) return false;
    uint32_t slot = moq_handle_slot(out->server);
    switch (family) {
    case STATUS:
        if (slot >= s->ts_cap || s->track_statuses[slot].handle._opaque != out->server ||
            s->track_statuses[slot].role != MOQ_TS_ROLE_PUBLISHER ||
            s->track_statuses[slot].state != MOQ_TS_PENDING_PUBLISHER) return false;
        break;
    case SUBSCRIBE:
        if (slot >= s->sub_cap || s->subs[slot].handle._opaque != out->server ||
            s->subs[slot].role != MOQ_SUB_ROLE_PUBLISHER ||
            s->subs[slot].state != MOQ_SUB_PENDING_PUBLISHER) return false;
        break;
    case PUBLISH:
        if (slot >= s->pub_cap || s->publishes[slot].handle._opaque != out->server ||
            s->publishes[slot].role != MOQ_PUB_ROLE_SUBSCRIBER ||
            s->publishes[slot].state != MOQ_PUB_PENDING_SUBSCRIBER) return false;
        break;
    case NAMESPACE:
        if (slot >= s->ann_cap || s->announcements[slot].handle._opaque != out->server ||
            s->announcements[slot].role != MOQ_ANN_ROLE_RECEIVER ||
            s->announcements[slot].state != MOQ_ANN_PENDING_RECEIVER) return false;
        break;
    case NS_SUB:
        if (slot >= s->ns_sub_cap || s->ns_subs[slot].handle._opaque != out->server ||
            s->ns_subs[slot].local_subscriber || s->ns_subs[slot].state != MOQ_NS_SUB_PENDING_PUBLISHER)
            return false;
        break;
    case TRACKS:
        if (slot >= s->track_sub_cap || s->track_subs[slot].handle._opaque != out->server ||
            s->track_subs[slot].role != MOQ_TRACK_SUB_ROLE_PUBLISHER ||
            s->track_subs[slot].state != MOQ_TRACK_SUB_PENDING_PUBLISHER) return false;
        break;
    }
    if (family == SUBSCRIBE) {
        out->id = s->subs[slot].request_id;
        out->ref = s->subs[slot].rs.ref;
    }
    if (family == STATUS) out->ref = s->track_statuses[slot].rs.ref;
    return true;
}

static bool collect(rig_t *r)
{
    moq_event_t e;
    size_t count;
    for (;;) {
        if (moq_session_poll_events_ex(r->client, &e, 1, sizeof(e), &count) != MOQ_OK) return false;
        if (!count) break;
        uint64_t handle = 0;
        family_t family = STATUS;
        bool answer = true, accepted = false;
        moq_request_error_t error = 0;
        switch (e.kind) {
        case MOQ_EVENT_TRACK_STATUS_OK: handle = e.u.track_status_ok.handle._opaque; accepted = true; break;
        case MOQ_EVENT_TRACK_STATUS_ERROR: handle = e.u.track_status_error.handle._opaque; error = e.u.track_status_error.error_code; break;
        case MOQ_EVENT_SUBSCRIBE_OK: family = SUBSCRIBE; handle = e.u.subscribe_ok.sub._opaque; accepted = true; break;
        case MOQ_EVENT_SUBSCRIBE_ERROR: family = SUBSCRIBE; handle = e.u.subscribe_error.sub._opaque; error = e.u.subscribe_error.error_code; break;
        case MOQ_EVENT_PUBLISH_ERROR: family = PUBLISH; handle = e.u.publish_error.pub._opaque; error = e.u.publish_error.error_code; break;
        case MOQ_EVENT_NAMESPACE_REJECTED: family = NAMESPACE; handle = e.u.namespace_rejected.ann._opaque; error = e.u.namespace_rejected.error_code; break;
        case MOQ_EVENT_NAMESPACE_ACCEPTED: family = NAMESPACE; handle = e.u.namespace_accepted.ann._opaque; accepted = true; break;
        case MOQ_EVENT_NS_SUB_ERROR: family = NS_SUB; handle = e.u.ns_sub_error.handle._opaque; error = e.u.ns_sub_error.error_code; break;
        case MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR: family = TRACKS; handle = e.u.subscribe_tracks_error.handle._opaque; error = e.u.subscribe_tracks_error.error_code; break;
        default: answer = false; break;
        }
        if (answer && family == NAMESPACE && handle == r->externally_answered) {
            CHECK(accepted);
            ++r->external_acks;
        } else if (answer && !(family == SUBSCRIBE && handle == r->wire_client._opaque)) {
            bool found = false;
            for (size_t i = 0; i < r->count; i++) {
                request_t *q = &r->requests[i];
                if (q->family != family || q->peer != handle) continue;
                CHECK(i == r->next && q->replies == 0);
                CHECK(accepted == q->accepted && (accepted || error == q->error));
                ++q->replies;
                ++r->next;
                found = true;
                break;
            }
            CHECK(found);
        }
        if (e.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            moq_accept_publish_cfg_t c;
            moq_accept_publish_cfg_init_sized(&c, sizeof(c));
            r->peer_publication = e.u.publish_request.pub;
            moq_result_t rc = moq_session_accept_publish(r->client, r->peer_publication, &c, 1000);
            CHECK(rc == MOQ_OK);
        }
        if (e.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            const moq_namespace_t *ns = &e.u.namespace_published.track_namespace;
            CHECK(ns->count == 1 && ns->parts[0].len >= 5 &&
                memcmp(ns->parts[0].data, "held-", 5) == 0);
            ++r->peer_pressure_requests;
        }
        if (e.kind == MOQ_EVENT_SESSION_CLOSED)
            fprintf(stderr, "%s: peer closed code=%llu\n", row_name, (unsigned long long)e.u.closed.code);
        bool closed = e.kind == MOQ_EVENT_SESSION_CLOSED;
        moq_event_cleanup(&e);
        if (closed) return false;
    }
    return true;
}

static bool drive(rig_t *r, bool manual)
{
    if (!manual) {
        moq_result_t rc = moq_pub_tick(r->pub, 1000);
        return rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK;
    }
    moq_result_t rc = moq_pub_flush(r->pub, 1000);
    if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) return false;
    for (size_t i = 0; i < r->server->event_cap; i++) {
        moq_event_t e;
        size_t n;
        if (moq_session_poll_events_ex(r->server, &e, 1, sizeof(e), &n) != MOQ_OK) return false;
        if (!n) break;
        moq_pub_event_result_t result;
        rc = moq_pub_handle_event(r->pub, &e, 1000, &result);
        moq_event_cleanup(&e);
        if (rc != MOQ_OK && !(rc == MOQ_ERR_WOULD_BLOCK && result == MOQ_PUB_EVENT_CONSUMED)) return false;
    }
    rc = moq_pub_flush(r->pub, 1000);
    return rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK;
}

static bool settle(rig_t *r, bool manual)
{
    for (unsigned i = 0; i < 16; i++)
        if (!drive(r, manual) || moq_simpair_run_until_quiescent(r->pair, 32, NULL) != MOQ_OK ||
            !collect(r)) return false;
    return moq_session_state(r->client) == MOQ_SESS_ESTABLISHED &&
           moq_session_state(r->server) == MOQ_SESS_ESTABLISHED;
}

static bool add_expected(rig_t *r, family_t family, bool known)
{
    if (r->count == RECORDS) return false;
    request_t *q = &r->requests[r->count];
    if (!request(r, family, known, q)) return false;
    q->accepted = family == STATUS && known;
    q->error = family == SUBSCRIBE || family == STATUS
        ? MOQ_REQUEST_ERROR_DOES_NOT_EXIST : MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    ++r->count;
    return true;
}

static bool block_output(rig_t *r)
{
    while (moq_session_action_capacity(r->server)) {
        char name[32];
        int len = snprintf(name, sizeof(name), "held-%u", r->pressure_serial++);
        if (len < 0 || (size_t)len >= sizeof(name)) return false;
        moq_bytes_t ns = {(const uint8_t *)name, (size_t)len};
        moq_publish_namespace_cfg_t c;
        moq_publish_namespace_cfg_init(&c); c.track_namespace = (moq_namespace_t){&ns, 1};
        moq_announcement_t h;
        if (moq_session_publish_namespace(r->server, &c, 1000, &h) != MOQ_OK) return false;
    }
    return r->server->action_tail - r->server->action_head == ACTIONS;
}

static bool discard_pressure(rig_t *r)
{
    unsigned count = 0;
    for (;;) {
        moq_action_t a;
        size_t n;
        if (moq_session_poll_actions_ex(r->server, &a, 1, sizeof(a), &n) != MOQ_OK) return false;
        if (!n) break;
        bool pressure = a.kind == MOQ_ACTION_SEND_CONTROL || a.kind == MOQ_ACTION_OPEN_BIDI_STREAM;
        moq_action_cleanup(&a);
        if (!pressure) return false;
        ++count;
    }
    return count == ACTIONS;
}

static bool setup_wire(rig_t *r)
{
    request_t q;
    if (!request(r, SUBSCRIBE, true, &q)) return false;
    r->wire_client._opaque = q.peer; r->wire_server._opaque = q.server;
    if (!settle(r, false) || moq_pub_active_subscriptions(r->pub, r->track) != 1) return false;
    if (moq_rcbuf_create(&r->core_alloc, (const uint8_t *)"object", 6, &r->payload) != MOQ_OK) return false;
    return moq_pub_write_object(r->pub, r->track, 0, 0, r->payload, 1000) == MOQ_OK && settle(r, false);
}

static bool block_wire(rig_t *r, bool stop)
{
    if (moq_pub_write_object(r->pub, r->track, 0, 1, r->payload, 1000) != MOQ_OK) return false;
    moq_stream_ref_t ref = {0};
    for (size_t i = 0; i < r->server->sg_cap; i++)
        if (r->server->subgroups[i].state == MOQ_SG_OPEN &&
            moq_subscription_eq(r->server->subgroups[i].sub, r->wire_server))
            ref = r->server->subgroups[i].stream_ref;
    if (!ref._v || r->server->action_tail - r->server->action_head != 1) return false;
    for (uint64_t group = 40; moq_session_action_capacity(r->server); group++)
        if (moq_session_send_status_datagram(r->server, r->wire_server, group, 0, 255,
                MOQ_OBJECT_END_OF_GROUP, 1000) != MOQ_OK) return false;
    if (!stop) return true;
    moq_session_t *s = r->server;
    if (s->event_head != s->event_tail || s->event_reserve) return false;
    /* Only notification credit is synthetic; the queued object, datagrams,
     * STOP and every request/cancellation use real production entry points. */
    for (size_t i = 0; i < s->event_cap; i++) {
        moq_event_t *e = &s->events[s->event_tail++ % s->event_cap];
        memset(e, 0, sizeof(*e)); e->kind = MOQ_EVENT_SUBGROUP_FINISHED;
    }
    if (moq_session_on_data_stop(s, ref, 0, 1000) != MOQ_ERR_WOULD_BLOCK) return false;
    moq_action_t a;
    size_t n = 99;
    return s->event_tail - s->event_head == s->event_cap &&
        s->action_tail - s->action_head == ACTIONS &&
        moq_session_poll_actions_ex(s, &a, 1, sizeof(a), &n) == MOQ_ERR_WOULD_BLOCK && n == 0;
}

static void bounded_reuse(moq_version_t version, bool stop)
{
    ++rows;
    rig_t r = {0};
    NEED(open_rig(&r, version, false));
    NEED(setup_wire(&r));
    NEED(block_wire(&r, stop));
    /* Return scalar event credit, but never poll a server action. */
    NEED(drive(&r, false));
    NEED(add_expected(&r, STATUS, true));
    size_t factory_bytes = r.pub_memory.bytes, calls = r.pub_memory.calls;
    NEED(drive(&r, false));
    CHECK(r.pub_memory.calls == calls); /* no first-use vector allocation */
    NEED(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
    size_t action_head = r.server->action_head, action_tail = r.server->action_tail;
    size_t g = generic_capacity(r.server);
    NEED(g > 0 && g + 1 <= CANCELS);
    if (version == MOQ_VERSION_DRAFT_16) {
        fprintf(stderr, "%s: server_G=%zu client_sub=%zu tombstones=%zu/%zu needed=%zu\n",
            row_name, g, r.client->sub_cap, r.client->unsub_tomb_count, r.client->unsub_tomb_cap, g + 1);
        NEED(g + 1 <= r.client->unsub_tomb_cap - r.client->unsub_tomb_count);
    } else NEED(g + 1 < r.client->drain_ref_cap);
    uint64_t old = 0;
    for (size_t i = 0; i <= g; i++) {
        request_t q;
        NEED(request(&r, SUBSCRIBE, false, &q));
        if (old) {
            CHECK(moq_handle_slot(q.server) == moq_handle_slot(old));
            CHECK(q.server != old);
        }
        old = q.server;
        calls = r.pub_memory.calls;
        NEED(drive(&r, false));
        CHECK(r.pub_memory.calls == calls);
        CHECK(r.server->action_head == action_head && r.server->action_tail == action_tail && r.next == 0);
        uint32_t head_slot = moq_handle_slot(r.requests[0].server);
        CHECK(r.server->track_statuses[head_slot].handle._opaque == r.requests[0].server &&
            r.server->track_statuses[head_slot].state == MOQ_TS_PENDING_PUBLISHER);
        r.cancelled_ids[r.cancelled_count] = q.id;
        r.cancelled_refs[r.cancelled_count++] = q.ref;
        moq_result_t cancel = moq_session_unsubscribe(r.client, (moq_subscription_t){q.peer}, 1000);
        if (cancel != MOQ_OK)
            fprintf(stderr, "%s: cancel[%zu] rc=%d tombstones=%zu/%zu actions_free=%zu state=%u\n",
                row_name, i, (int)cancel, r.client->unsub_tomb_count, r.client->unsub_tomb_cap,
                moq_session_action_capacity(r.client), (unsigned)moq_session_state(r.client));
        NEED(cancel == MOQ_OK);
        NEED(client_only(&r));
        NEED(sub_resolve_handle(r.server, (moq_subscription_t){q.server}) < 0);
        NEED(drive(&r, false));
        CHECK(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
    }
    CHECK(r.cancelled_count > g && r.pub_memory.bytes == factory_bytes);
    if (version == MOQ_VERSION_DRAFT_16) CHECK(r.client->unsub_tomb_count == g + 1);
    NEED(add_expected(&r, SUBSCRIBE, false)); NEED(drive(&r, false));
    NEED(add_expected(&r, PUBLISH, false)); NEED(drive(&r, false));
    NEED(add_expected(&r, NAMESPACE, false)); NEED(drive(&r, false));
    NEED(add_expected(&r, NS_SUB, false)); NEED(drive(&r, false));
    if (version != MOQ_VERSION_DRAFT_16) { NEED(add_expected(&r, TRACKS, false)); NEED(drive(&r, false)); }
    NEED(add_expected(&r, STATUS, false)); NEED(drive(&r, false));
    CHECK(r.pub_memory.bytes == factory_bytes);
    calls = r.pub_memory.calls;
    for (unsigned i = 0; i < 16; i++) {
        CHECK(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
        CHECK(r.pub_memory.calls == calls && r.next == 0);
        CHECK(r.server->action_tail - r.server->action_head == ACTIONS);
    }
    NEED(settle(&r, false));
    CHECK(r.next == r.count && r.stale_wire == 0);
    for (size_t i = 0; i < r.count; i++) CHECK(r.requests[i].replies == 1);
    CHECK(r.resets == (stop ? 1u : 0u) && r.left == 0 && r.closed == 0);
    CHECK(moq_pub_flush(r.pub, 1000) == MOQ_OK);
cleanup:
    close_rig(&r);
}

static moq_result_t manual_status(rig_t *r, uint64_t handle, bool known,
                                  moq_pub_event_result_t *result)
{
    moq_event_t event;
    memset(&event, 0, sizeof(event));
    event.kind = MOQ_EVENT_TRACK_STATUS_REQUEST;
    event.detail_size = sizeof(event.u.track_status_request);
    event.u.track_status_request.handle._opaque = handle;
    event.u.track_status_request.track_namespace = (moq_namespace_t){known ? &live_ns : &absent_ns, 1};
    event.u.track_status_request.track_name = MOQ_BYTES_LITERAL("video");
    moq_result_t rc = moq_pub_handle_event(r->pub, &event, 1000, result);
    moq_event_cleanup(&event);
    return rc;
}

static void identity_controls(moq_version_t version)
{
    ++rows;
    rig_t r = {0}, foreign = {0};
    NEED(open_rig(&r, version, false));
    NEED(open_rig(&foreign, version, false));
    request_t other;
    NEED(request(&foreign, STATUS, true, &other));
    NEED(add_expected(&r, STATUS, true));
    uint64_t answered = r.requests[0].server;
    NEED(settle(&r, true));
    NEED(r.next == 1 && r.requests[0].replies == 1);
    NEED(add_expected(&r, STATUS, true));
    uint64_t live = r.requests[1].server;
    NEED(moq_handle_slot(answered) == moq_handle_slot(live) && answered != live);
    moq_track_status_cfg_t requester;
    moq_track_status_cfg_init(&requester);
    requester.track_namespace = (moq_namespace_t){&live_ns, 1};
    requester.track_name = MOQ_BYTES_LITERAL("video");
    moq_track_status_handle_t wrong_role;
    NEED(moq_session_track_status(r.server, &requester, 1000, &wrong_role) == MOQ_OK);
    NEED(r.server->track_statuses[moq_handle_slot(wrong_role._opaque)].role == MOQ_TS_ROLE_REQUESTER);
    NEED(block_output(&r));
    NEED(drive(&r, true));
    size_t calls = r.pub_memory.calls, bytes = r.pub_memory.bytes;
    for (unsigned i = 0; i < 24; i++) {
        moq_pub_event_result_t result;
        CHECK(manual_status(&r, live, true, &result) == MOQ_ERR_WOULD_BLOCK);
        CHECK(result == MOQ_PUB_EVENT_CONSUMED);
        moq_result_t rc = manual_status(&r, answered, true, &result);
        CHECK(rc == MOQ_OK); /* already answered, older generation */
        rc = manual_status(&r, other.server, true, &result);
        CHECK(rc == MOQ_OK); /* real foreign typed owner */
        uint64_t wrong_pool = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
            r.server->session_tag, moq_handle_generation(live), moq_handle_slot(live));
        rc = manual_status(&r, wrong_pool, true, &result);
        CHECK(rc == MOQ_OK);
        rc = manual_status(&r, wrong_role._opaque, true, &result);
        CHECK(rc == MOQ_OK); /* live same-session owner, but requester role */
        CHECK(r.pub_memory.calls == calls && r.pub_memory.bytes == bytes);
    }
    NEED(settle(&r, true));
    CHECK(r.next == 2 && r.requests[1].replies == 1 && r.closed == 0);
cleanup:
    close_rig(&foreign);
    close_rig(&r);
}

static void answered_namespace_tail(moq_version_t version)
{
    ++rows;
    rig_t r = {0};
    NEED(open_rig(&r, version, false));
    NEED(add_expected(&r, STATUS, true));
    NEED(block_output(&r));
    NEED(drive(&r, false));
    request_t namespace;
    NEED(request(&r, NAMESPACE, false, &namespace));
    /* Deliver every committed pressure request without driving the facade.
     * Dropping one D16 request would leave a gap on its ordered control
     * channel. The later refill holds the original answer head again. */
    r.check_namespace_sequence = version == MOQ_VERSION_DRAFT_16;
    r.next_namespace_id = 1;
    size_t delivered = 0;
    NEED(server_only(&r, &delivered));
    NEED(delivered == ACTIONS && collect(&r));
    NEED(r.peer_pressure_requests == ACTIONS && r.next == 0);
    uint32_t head = moq_handle_slot(r.requests[0].server);
    NEED(r.server->track_statuses[head].handle._opaque == r.requests[0].server &&
        r.server->track_statuses[head].state == MOQ_TS_PENDING_PUBLISHER);
    NEED(r.server->action_head == r.server->action_tail);
    moq_accept_namespace_cfg_t accept;
    moq_accept_namespace_cfg_init(&accept);
    NEED(moq_session_accept_namespace(r.server, (moq_announcement_t){namespace.server}, &accept, 1000) == MOQ_OK);
    uint32_t slot = moq_handle_slot(namespace.server);
    NEED(r.server->announcements[slot].state == MOQ_ANN_ESTABLISHED);
    NEED(r.server->announcements[slot].handle._opaque == namespace.server);
    r.externally_answered = namespace.peer;
    NEED(block_output(&r));
    NEED(r.server->action_tail - r.server->action_head == ACTIONS);
    size_t calls = r.pub_memory.calls;
    NEED(drive(&r, false));
    CHECK(r.pub_memory.calls == calls);
    CHECK(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
    CHECK(r.server->announcements[slot].state == MOQ_ANN_ESTABLISHED);
    NEED(settle(&r, false));
    CHECK(r.next == 1 && r.requests[0].replies == 1 && r.external_acks == 1);
    CHECK(r.peer_pressure_requests == 2 * ACTIONS - 1);
    if (version == MOQ_VERSION_DRAFT_16)
        CHECK(r.namespace_wire == 2 * ACTIONS - 1 && r.next_namespace_id == 1 + 2 * (2 * ACTIONS - 1));
    CHECK(r.server->announcements[slot].state == MOQ_ANN_ESTABLISHED);
    CHECK(moq_pub_flush(r.pub, 1000) == MOQ_OK && r.closed == 0);
cleanup:
    close_rig(&r);
}

static void status_lifetime(moq_version_t version, unsigned ending)
{
    ++rows;
    rig_t r = {0};
    uint8_t *properties = NULL;
    NEED(open_rig(&r, version, false));
    if (ending == 1) {
        size_t value = r.server->send_cap + 128;
        NEED(value < 16384);
        properties = malloc(value + 16);
        NEED(properties != NULL);
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, properties, value + 16);
        if (version == MOQ_VERSION_DRAFT_16) {
            NEED(moq_buf_write_varint(&w, 0x79) == MOQ_OK);
            NEED(moq_buf_write_varint(&w, value) == MOQ_OK);
        } else {
            NEED(moq_buf_write_vi64(&w, 0x79) == MOQ_OK);
            NEED(moq_buf_write_vi64(&w, value) == MOQ_OK);
        }
        size_t prefix = moq_buf_writer_offset(&w);
        memset(properties + prefix, 'p', value);
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
        tc.track_namespace = (moq_namespace_t){&live_ns, 1};
        tc.track_name = MOQ_BYTES_LITERAL("video"); tc.advertise_namespace = false;
        tc.track_properties = (moq_bytes_t){properties, prefix + value};
        NEED(moq_pub_remove_track(r.pub, r.track, 1000) == MOQ_OK);
        r.track = NULL;
        NEED(moq_pub_add_track(r.pub, &tc, 1000, &r.track) == MOQ_OK);
    }
    NEED(add_expected(&r, STATUS, true));
    NEED(block_output(&r));
    NEED(drive(&r, true));
    NEED(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
    if (ending == 0) {
        NEED(moq_pub_remove_track(r.pub, r.track, 1000) == MOQ_OK); r.track = NULL;
        r.requests[0].accepted = false; r.requests[0].error = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
    } else if (ending == 1 && version != MOQ_VERSION_DRAFT_16) {
        r.requests[0].accepted = false; r.requests[0].error = MOQ_REQUEST_ERROR_INTERNAL_ERROR;
    } else if (ending == 2) {
        NEED(moq_session_on_transport_close(r.server, 0x99, 1000) == MOQ_OK);
        moq_result_t rc = moq_pub_tick(r.pub, 1000);
        CHECK(rc == MOQ_OK || rc == MOQ_ERR_CLOSED);
        CHECK(r.closed == 1);
        moq_event_t e;
        memset(&e, 0, sizeof(e)); e.kind = MOQ_EVENT_SESSION_CLOSED; e.u.closed.code = 0x99;
        moq_pub_event_result_t result;
        CHECK(moq_pub_handle_event(r.pub, &e, 1000, &result) == MOQ_OK);
        CHECK(r.closed == 1);
        goto cleanup;
    } else if (ending == 3) goto cleanup; /* destruction while a live answer waits */
    NEED(settle(&r, true));
    CHECK(r.next == 1 && r.requests[0].replies == 1 && r.closed == 0);
cleanup:
    free(properties);
    close_rig(&r);
}

/* Mirror the private scalar answer record solely to check exact pool bytes;
 * this is not a public ABI. Fixed G records are the storage bound. */
typedef struct answer_storage {
    moq_event_kind_t kind;
    uint64_t handle;
    moq_pub_track_t *track;
    moq_request_error_t code;
} answer_storage_t;

static void constructor(moq_version_t version, uint32_t subscriptions)
{
    ++rows;
    memory_t core = {0}, memory = {0};
    moq_alloc_t a = allocator(&core), p = allocator(&memory);
    moq_session_t *s = NULL;
    moq_publisher_t *pub = NULL;
    moq_session_cfg_t sc = MOQ_SESSION_CFG_INIT;
    sc.alloc = &a; sc.version = version; sc.perspective = MOQ_PERSPECTIVE_SERVER;
    sc.max_subscriptions = subscriptions;
    NEED(moq_session_create(&sc, 1000, &s) == MOQ_OK);
    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    /* Publisher + optional fills + admissions + FETCH owners + generic G. */
    const unsigned allocations = version == MOQ_VERSION_DRAFT_22 ? 5u : 4u;
    NEED(moq_pub_create(s, &p, &pc, &pub) == MOQ_OK);
    CHECK(memory.calls == allocations);
    size_t expected = generic_capacity(s) * sizeof(answer_storage_t), matches = 0;
    for (size_t i = 0; i < memory.calls && i < 16; i++)
        matches += memory.sizes[i] == expected;
    CHECK(matches == 1);
    printf("%s: G=%zu generic_bytes=%zu factories=%zu expected=%u\n", row_name,
        generic_capacity(s), expected, memory.calls, allocations);
    moq_pub_destroy(pub); pub = NULL;
    CHECK(memory.live == 0 && memory.bytes == 0);
    for (unsigned i = 1; i <= allocations; i++) {
        memory.fail_at = memory.calls + i;
        moq_result_t rc = moq_pub_create(s, &p, &pc, &pub);
        CHECK(rc == MOQ_ERR_NOMEM && pub == NULL);
        if (pub) { moq_pub_destroy(pub); pub = NULL; }
        CHECK(memory.live == 0 && memory.bytes == 0);
        memory.fail_at = 0;
    }
cleanup:
    moq_pub_destroy(pub);
    moq_session_destroy(s);
    CHECK(memory.live == 0 && memory.bytes == 0 && core.live == 0 && core.bytes == 0);
}

static void decision_transfer(moq_version_t version, bool allocation_failure)
{
    ++rows;
    rig_t r = {0};
    NEED(open_rig(&r, version, true));
    unsigned prior = allocation_failure ? 2u : 0u;
    for (unsigned i = 0; i < prior; i++) {
        NEED(request(&r, SUBSCRIBE, true, &r.requests[r.count]));
        r.requests[r.count++].accepted = true;
        NEED(settle(&r, true));
    }
    r.defer_next = true;
    NEED(request(&r, SUBSCRIBE, true, &r.requests[r.count]));
    r.requests[r.count++].accepted = true;
    NEED(drive(&r, true));
    NEED(r.deferred != NULL && r.authorized == prior + 1 && r.joined == prior);
    CHECK(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
    NEED(block_output(&r));
    if (allocation_failure) r.pub_memory.fail_at = r.pub_memory.calls + 1;
    moq_result_t rc = moq_pub_resolve_deferred(r.pub, r.deferred, r.deferred_id, true, 0, 1000);
    CHECK(rc == (allocation_failure ? MOQ_ERR_NOMEM : MOQ_ERR_WOULD_BLOCK));
    CHECK(moq_pub_resolve_deferred(r.pub, r.deferred, r.deferred_id, true, 0, 1000) == MOQ_ERR_STALE_HANDLE);
    CHECK(r.joined == prior && r.authorized == prior + 1);
    r.pub_memory.fail_at = 0;
    NEED(settle(&r, true));
    CHECK(r.next == r.count && r.joined == prior + 1 && r.authorized == prior + 1 && r.left == 0);
    for (size_t i = 0; i < r.count; i++) CHECK(r.requests[i].replies == 1);
cleanup:
    close_rig(&r);
}

static bool parent_owed(const rig_t *r, bool publication, uint64_t parent,
                         uint64_t id, uint32_t count)
{
    uint32_t slot = moq_handle_slot(parent);
    const moq_owed_update_t *o = publication ? &r->server->publishes[slot].owed_update
                                            : &r->server->subs[slot].owed_update;
    return o->owed && o->rec.request_id == id && o->count == count;
}

static void cumulative_updates(moq_version_t version, bool publication, bool manual)
{
    ++rows;
    rig_t r = {0};
    NEED(open_rig(&r, version, false));
    NEED(setup_wire(&r));
    uint64_t parent = r.wire_server._opaque;
    moq_stream_ref_t parent_ref = r.server->subs[moq_handle_slot(parent)].rs.ref;
    uint64_t parent_id = r.server->subs[moq_handle_slot(parent)].request_id;
    if (publication) {
        moq_pub_publish_cfg_t pc;
        moq_pub_publish_cfg_init(&pc);
        NEED(moq_pub_publish_track(r.pub, r.track, &pc, 1000) == MOQ_OK);
        NEED(settle(&r, manual));
        NEED(moq_publication_is_valid(r.peer_publication));
        parent = 0;
        for (size_t i = 0; i < r.server->pub_cap; i++) {
            const moq_pub_entry_t *e = &r.server->publishes[i];
            if (e->state != MOQ_PUB_ESTABLISHED || e->role != MOQ_PUB_ROLE_PUBLISHER) continue;
            parent = e->handle._opaque; parent_ref = e->rs.ref; parent_id = e->request_id;
            NEED(!e->facade_defer_updates);
        }
        NEED(parent != 0);
        NEED(moq_pub_write_object(r.pub, r.track, 0, 1, r.payload, 1000) == MOQ_OK);
        NEED(settle(&r, manual));
    } else {
        NEED(!r.server->subs[moq_handle_slot(parent)].facade_defer_updates);
    }
    /* Explicit, existing opt-in only for this bounded two-update control.
     * No facade capability gate or session pool is edited. */
    NEED(moq_session_set_defer_subscription_updates(r.server, true) == MOQ_OK);
    NEED(block_output(&r));
    if (publication) {
        moq_publication_update_cfg_t c;
        moq_publication_update_cfg_init_sized(&c, sizeof(c));
        c.has_forward = true; c.forward = false;
        c.has_subscriber_priority = true; c.subscriber_priority = 17;
        NEED(moq_session_update_publication(r.client, r.peer_publication, &c, 1000) == MOQ_OK);
    } else {
        moq_subscription_update_cfg_t c;
        moq_subscription_update_cfg_init_sized(&c, sizeof(c));
        c.has_forward = true; c.forward = false;
        c.has_subscriber_priority = true; c.subscriber_priority = 17;
        c.has_filter = true; c.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
        c.start_group = c.start_object = 0; c.end_group = 3;
        NEED(moq_session_update_subscription(r.client, r.wire_client, &c, 1000) == MOQ_OK);
    }
    NEED(client_only(&r));
    NEED(r.server->event_tail - r.server->event_head == 1);
    const moq_event_t *first = &r.server->events[r.server->event_head % r.server->event_cap];
    NEED(first->kind == (publication ? MOQ_EVENT_PUBLISH_UPDATED : MOQ_EVENT_SUBSCRIBE_UPDATED));
    uint64_t id1 = publication ? first->u.publish_updated.update_request_id
                               : first->u.subscribe_updated.update_request_id;
    NEED(publication ? first->u.publish_updated.answer_owed && first->u.publish_updated.update_count == 1
                     : first->u.subscribe_updated.answer_owed && first->u.subscribe_updated.update_count == 1);
    NEED(drive(&r, manual));
    NEED(parent_owed(&r, publication, parent, id1, 1));
    CHECK(r.updated == 0 && r.forwarded == 0 && r.joined == 1);

    /* The public requester API serializes its own updates. Encode a legal
     * pipelined peer message with the shipping profile encoder, rather than
     * clearing update_pending or changing an owner pool for the test. Replies
     * are decoded below at the wire boundary (not fed to that serial client). */
    uint64_t id2 = id1 + 2;
    uint8_t bytes[256];
    moq_buf_writer_t writer;
    moq_buf_writer_init(&writer, bytes, sizeof(bytes));
    moq_request_update_encode_args_t args = {
        .request_id = id2, .existing_request_id = parent_id,
        .has_forward = true, .forward = true,
        .has_delivery_timeout = true, .delivery_timeout_us = 5000,
        .sender = MOQ_UPDATE_SENDER_SUBSCRIBER
    };
    NEED(r.client->profile->encode_request_update(r.client, &writer, &args) == MOQ_OK);
    moq_result_t rc = version == MOQ_VERSION_DRAFT_16
        ? moq_session_on_control_bytes(r.server, bytes, moq_buf_writer_offset(&writer), 1000)
        : moq_session_on_bidi_stream_bytes(r.server, parent_ref, bytes,
            moq_buf_writer_offset(&writer), false, 1000);
    NEED(rc == MOQ_OK);
    NEED(parent_owed(&r, publication, parent, id2, 2));
    NEED(r.server->event_tail - r.server->event_head == 1);
    const moq_event_t *second = &r.server->events[r.server->event_head % r.server->event_cap];
    if (publication) {
        const moq_publish_updated_event_t *u = &second->u.publish_updated;
        NEED(second->kind == MOQ_EVENT_PUBLISH_UPDATED && u->answer_owed && u->update_count == 2);
        CHECK(u->update_request_id == id2 && u->has_subscriber_priority && u->subscriber_priority == 17);
        CHECK(u->has_forward && u->forward && u->has_delivery_timeout && u->delivery_timeout_us == 5000);
    } else {
        const moq_subscribe_updated_event_t *u = &second->u.subscribe_updated;
        NEED(second->kind == MOQ_EVENT_SUBSCRIBE_UPDATED && u->answer_owed && u->update_count == 2);
        CHECK(u->update_request_id == id2 && u->has_subscriber_priority && u->subscriber_priority == 17);
        CHECK(u->has_forward && u->forward && u->has_delivery_timeout && u->delivery_timeout_us == 5000);
        CHECK(u->has_filter && u->filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE && u->end_group == 3);
    }
    moq_accept_subscription_update_cfg_t accept;
    moq_accept_subscription_update_cfg_init_sized(&accept, sizeof(accept));
    accept.update_request_id = id1;
    rc = publication ? moq_session_accept_publication_update(r.server, (moq_publication_t){parent}, &accept, 1000)
                     : moq_session_accept_subscription_update(r.server, (moq_subscription_t){parent}, &accept, 1000);
    CHECK(rc == MOQ_ERR_STALE_HANDLE);
    NEED(discard_pressure(&r));
    /* Even with empty actions the first scalar cell cannot ACK/apply its old
     * ID while the newer cumulative FIFO event has not reached the facade. */
    for (unsigned i = 0; i < 16; i++) {
        CHECK(moq_pub_flush(r.pub, 1000) == MOQ_ERR_WOULD_BLOCK);
        CHECK(r.server->action_head == r.server->action_tail && r.updated == 0 && r.forwarded == 0);
        CHECK(parent_owed(&r, publication, parent, id2, 2));
    }
    NEED(drive(&r, manual));
    CHECK(r.updated == (publication ? 0u : 1u) && r.forwarded == (publication ? 1u : 0u));
    if (!publication) {
        CHECK(r.last_update.has_forward && r.last_update.forward);
        CHECK(r.last_update.has_subscriber_priority && r.last_update.subscriber_priority == 17);
        CHECK(r.last_update.has_delivery_timeout && r.last_update.delivery_timeout_us == 5000);
    }
    uint32_t slot = moq_handle_slot(parent);
    if (publication) {
        const moq_pub_entry_t *e = &r.server->publishes[slot];
        CHECK(e->handle._opaque == parent && e->send_allowed && e->subscriber_priority == 17);
        CHECK(!e->owed_update.owed && e->dt_sub_has_object && e->dt_sub_object_ms == 5);
    } else {
        const moq_sub_entry_t *e = &r.server->subs[slot];
        CHECK(e->handle._opaque == parent && e->forward && e->subscriber_priority == 17);
        CHECK(!e->owed_update.owed && e->filter_type == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE && e->req_end_group == 3);
        CHECK(e->dt_sub_has_object && e->dt_sub_object_ms == 5);
    }
    unsigned acks = 0;
    for (;;) {
        moq_action_t action;
        size_t n;
        NEED(moq_session_poll_actions_ex(r.server, &action, 1, sizeof(action), &n) == MOQ_OK);
        if (!n) break;
        const uint8_t *data = NULL;
        size_t length = 0;
        if (version == MOQ_VERSION_DRAFT_16) {
            CHECK(action.kind == MOQ_ACTION_SEND_CONTROL);
            if (action.kind == MOQ_ACTION_SEND_CONTROL) { data = action.u.send_control.data; length = action.u.send_control.len; }
        } else {
            CHECK(action.kind == MOQ_ACTION_SEND_BIDI_STREAM);
            if (action.kind == MOQ_ACTION_SEND_BIDI_STREAM) {
                CHECK(action.u.send_bidi_stream.stream_ref._v == parent_ref._v && !action.u.send_bidi_stream.fin);
                data = action.u.send_bidi_stream.data; length = action.u.send_bidi_stream.len;
            }
        }
        if (data) {
            moq_buf_reader_t reader;
            moq_buf_reader_init(&reader, data, length);
            /* Several wire ACKs may share one transport output action. */
            while (reader.pos < reader.len) {
                moq_control_envelope_t env;
                rc = version == MOQ_VERSION_DRAFT_16 ? moq_control_decode_envelope(&reader, &env)
                                                    : moq_d18_decode_envelope(&reader, &env);
                CHECK(rc == MOQ_OK && env.msg_type == MOQ_D16_REQUEST_OK);
                if (rc != MOQ_OK || env.msg_type != MOQ_D16_REQUEST_OK) break;
                if (version == MOQ_VERSION_DRAFT_16) {
                    moq_kvp_entry_t params[8];
                    moq_d16_request_ok_t ok = {.params = params, .params_cap = 8};
                    CHECK(moq_d16_decode_request_ok(env.payload, env.payload_len, &ok) == MOQ_OK);
                    CHECK(ok.request_id == (acks == 0 ? id1 : id2));
                } else {
                    moq_d18_msg_params_t params;
                    memset(&params, 0, sizeof(params));
                    CHECK(moq_d18_decode_request_update_ok(env.payload, env.payload_len, &params) == MOQ_OK);
                }
                ++acks;
            }
            CHECK(reader.pos == reader.len);
        }
        moq_action_cleanup(&action);
    }
    CHECK(acks == 2 && r.resets == 0 && r.left == 0 && r.closed == 0);
    CHECK(moq_pub_flush(r.pub, 1000) == MOQ_OK);
    CHECK(r.updated == (publication ? 0u : 1u) && r.forwarded == (publication ? 1u : 0u));
cleanup:
    close_rig(&r);
}

int main(int argc, char **argv)
{
    bool full = argc == 1;
    bool bounds = argc == 2 && strcmp(argv[1], "--bounds") == 0;
    bool controls = argc == 2 && strcmp(argv[1], "--controls") == 0;
    bool cumulative = argc == 2 && strcmp(argv[1], "--cumulative-updates") == 0;
    if (!full && !bounds && !controls && !cumulative) {
        fprintf(stderr, "usage: %s [--bounds|--controls|--cumulative-updates]\n", argv[0]);
        return 2;
    }
#define RUN(call) do { unsigned before = failures, setup = prerequisites; call; \
    printf("%s: failures=%u prerequisites=%u\n", row_name, failures - before, prerequisites - setup); } while (0)
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t i = 0; i < 3; i++) {
        moq_version_t v = versions[i];
        if (full || bounds) {
            for (unsigned stop = 0; stop < 2; stop++) {
                snprintf(row_name, sizeof(row_name), "bounded_response_reuse/version%u/stop%u", (unsigned)v, stop);
                RUN(bounded_reuse(v, stop != 0));
            }
            snprintf(row_name, sizeof(row_name), "manual_response_identities/version%u", (unsigned)v);
            RUN(identity_controls(v));
            for (uint32_t cap = 1; cap <= 7; cap += 3) {
                snprintf(row_name, sizeof(row_name), "response_factory_storage/version%u/subscriptions%u", (unsigned)v, cap);
                RUN(constructor(v, cap));
            }
        }
        if (full || controls) {
            snprintf(row_name, sizeof(row_name), "answered_namespace_tail/version%u", (unsigned)v);
            RUN(answered_namespace_tail(v));
            for (unsigned ending = 0; ending < 4; ending++) {
                snprintf(row_name, sizeof(row_name), "status_response_lifetime/version%u/ending%u", (unsigned)v, ending);
                RUN(status_lifetime(v, ending));
            }
            snprintf(row_name, sizeof(row_name), "deferred_decision_transfer/version%u/output_blocked", (unsigned)v);
            RUN(decision_transfer(v, false));
            if (v == MOQ_VERSION_DRAFT_22) {
                snprintf(row_name, sizeof(row_name), "deferred_decision_transfer/version%u/destination_allocation", (unsigned)v);
                RUN(decision_transfer(v, true));
            }
        }
        if (v != MOQ_VERSION_DRAFT_22 && (full || controls || cumulative)) {
            for (unsigned publication = 0; publication < 2; publication++) {
                for (unsigned manual = 0; manual < 2; manual++) {
                    snprintf(row_name, sizeof(row_name), "cumulative_response_commit/version%u/publication%u/manual%u",
                        (unsigned)v, publication, manual);
                    RUN(cumulative_updates(v, publication != 0, manual != 0));
                }
            }
        }
    }
    unsigned expected = full ? 45u : bounds ? 18u : controls ? 27u : 8u;
    CHECK(rows == expected);
    printf("publisher_response_ownership: %u rows, %u checks, %u failures, %u prerequisite failures\n",
        rows, checks, failures, prerequisites);
    return failures != 0;
#undef RUN
}
