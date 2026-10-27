/* Shipping-core/SimPair boundary: actual virtual admission, never dequeue,
 * supplies owned DONE counts. No private core implementation is linked. */
#include <moq/moq.h>
#include <moq/sim.h>
#include <moq/control.h>
#include <moq/control_d18.h>
#include <moq/buf.h>
#include "../../core/src/session/session_internal.h"
#include "../../sim/src/simpair_internal.h"
#include "../support/fake_endpoint.h"
#include "test_alloc_support.h"
#include <stdio.h>
#include <string.h>

typedef struct pending_before_observer {
    uint64_t ref, owner;
    uint8_t mode;
    bool handed;
} pending_before_observer_t;
_Static_assert(sizeof(moq_pending_open_t) == sizeof(pending_before_observer_t),
               "virtual handoff must fit the existing pending-record padding");

static unsigned failures;
#define CHECK(x) do { if (!(x)) { ++failures; \
    fprintf(stderr, "%s:%d: %s\n", __func__, __LINE__, #x); return false; } } while (0)

typedef struct fixture {
    test_alloc_state_t memory;
    moq_alloc_t alloc;
    moq_simpair_t *sp;
    moq_session_t *sender, *peer;
    moq_publisher_t *facade;
    moq_pub_track_t *track;
    moq_subscription_t sub, peer_sub;
    moq_publication_t pub, peer_pub;
    moq_version_t version;
    bool publication, invalidate;
    size_t wires, headers, inputs, fins, drops;
    uint64_t wire_count, wire_status;
    bool decoded, wire_fin;
} fixture_t;

/* D22 §9: vi64 type, two-byte network-order payload length, then body.
 * Only public cursor primitives; the shipping DSO hides its D22 codec. */
static moq_result_t d22_envelope(moq_buf_reader_t *r, moq_control_envelope_t *env)
{
    moq_buf_reader_t cursor = *r;
    moq_result_t rc = moq_buf_read_vi64(&cursor, &env->msg_type);
    if (rc != MOQ_OK) return rc;
    rc = moq_buf_read_uint16(&cursor, &env->payload_len);
    if (rc != MOQ_OK) return rc;
    if (env->payload_len > moq_buf_reader_remaining(&cursor)) return MOQ_ERR_BUFFER;
    env->payload = moq_buf_reader_ptr(&cursor);
    cursor.pos += env->payload_len;
    *r = cursor;
    return MOQ_OK;
}

static void wire(fixture_t *f, const uint8_t *bytes, size_t len, bool fin)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, bytes, len);
    while (r.pos < r.len) {
        moq_control_envelope_t env;
        moq_result_t rc = f->version == MOQ_VERSION_DRAFT_16
            ? moq_control_decode_envelope(&r, &env)
            : f->version == MOQ_VERSION_DRAFT_18
            ? moq_d18_decode_envelope(&r, &env) : d22_envelope(&r, &env);
        if (rc != MOQ_OK) return;
        if (env.msg_type != 0x0b) continue;
        ++f->wires;
        f->wire_fin = fin && r.pos == r.len;
        if (f->version == MOQ_VERSION_DRAFT_16) {
            moq_d16_publish_done_t d;
            f->decoded = moq_d16_decode_publish_done(env.payload, env.payload_len, &d) == MOQ_OK;
            if (f->decoded) { f->wire_count = d.stream_count; f->wire_status = d.status_code; }
        } else {
            moq_d18_publish_done_t d;
            f->decoded = moq_d18_decode_publish_done(env.payload, env.payload_len, &d) == MOQ_OK;
            if (f->decoded) { f->wire_count = d.stream_count; f->wire_status = d.status_code; }
        }
    }
}

static void trace(void *ctx, const moq_sim_trace_record_t *r)
{
    fixture_t *f = ctx;
    if (r->kind == MOQ_SIM_TRACE_FAULT_DROP && r->action_kind == MOQ_ACTION_SEND_DATA)
        ++f->drops;
    if (r->kind == MOQ_SIM_TRACE_INPUT && r->input_kind == MOQ_SIM_INPUT_DATA_BYTES) {
        ++f->inputs;
        if (r->fin) ++f->fins;
    }
    if (r->kind != MOQ_SIM_TRACE_ACTION || r->from != MOQ_PERSPECTIVE_SERVER) return;
    if (r->action_kind == MOQ_ACTION_SEND_DATA) {
        ++f->headers;
        if (f->invalidate) {
            /* Deliberately violate the driver's epoch once, before it reads
             * spans or admits the stream, to exercise truthful error cleanup. */
            f->invalidate = false;
            (void)moq_session_tick(f->sender, moq_simpair_now_us(f->sp));
        }
    } else if (r->action_kind == MOQ_ACTION_SEND_CONTROL) {
        wire(f, r->bytes.data, r->bytes.len, false);
    } else if (r->action_kind == MOQ_ACTION_SEND_BIDI_STREAM) {
        wire(f, r->detail_bytes.data, r->detail_bytes.len, r->fin);
    }
}

static void drain(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1) == 1) moq_event_cleanup(&e);
}

static bool pump(fixture_t *f)
{
    CHECK(moq_simpair_run_until_quiescent(f->sp, 64, NULL) == MOQ_OK);
    CHECK(moq_session_state(f->sender) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(f->peer) == MOQ_SESS_ESTABLISHED);
    return true;
}

static bool server_events(fixture_t *f)
{
    moq_event_t e;
    for (unsigned i = 0; i < 128; ++i) {
        if (moq_session_poll_events(f->sender, &e, 1) != 1) return true;
        if (e.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) f->sub = e.u.subscribe_request.sub;
        moq_pub_event_result_t result;
        moq_result_t rc = moq_pub_handle_event(f->facade, &e,
            moq_simpair_now_us(f->sp), &result);
        moq_event_cleanup(&e);
        CHECK(rc == MOQ_OK);
    }
    CHECK(false);
    return false;
}

static bool setup(fixture_t *f, moq_version_t v, bool publication)
{
    memset(f, 0, sizeof(*f));
    f->version = v;
    f->publication = publication;
    f->alloc = test_allocator(&f->memory);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &f->alloc;
    cfg.version = v;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 64;
    cfg.fault_per_mille = 1000;
    cfg.fault_flags = MOQ_SIM_FAULT_DROP_DATA;
    cfg.trace_fn = trace;
    cfg.trace_ctx = f;
    CHECK(moq_simpair_create(&cfg, &f->sp) == MOQ_OK);
    f->sender = moq_simpair_server(f->sp);
    f->peer = moq_simpair_client(f->sp);
    CHECK(moq_simpair_start(f->sp) == MOQ_OK);
    CHECK(pump(f));
    drain(f->sender); drain(f->peer);
    moq_pub_cfg_t pc;
    moq_pub_cfg_init_sized(&pc, sizeof(pc));
    pc.accept_mode = MOQ_PUB_ACCEPT_ALL;
    CHECK(moq_pub_create(f->sender, &f->alloc, &pc, &f->facade) == MOQ_OK);
    moq_bytes_t ns = MOQ_BYTES_LITERAL("observed");
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    moq_track_delivery_timeouts_cfg_t timeouts;
    moq_track_delivery_timeouts_cfg_init(&timeouts);
    timeouts.has_object_delivery_timeout = timeouts.has_subgroup_delivery_timeout = true;
    timeouts.object_delivery_timeout_ms = timeouts.subgroup_delivery_timeout_ms = 1000;
    uint8_t properties[32];
    size_t properties_len = 0;
    CHECK(moq_session_track_properties_add_delivery_timeouts(f->sender, NULL, 0,
        &timeouts, properties, sizeof(properties), &properties_len) == MOQ_OK);
    tc.track_properties = (moq_bytes_t){properties, properties_len};
    tc.track_namespace = (moq_namespace_t){&ns, 1};
    tc.track_name = MOQ_BYTES_LITERAL("video");
    CHECK(moq_pub_add_track(f->facade, &tc, 1000, &f->track) == MOQ_OK);
    if (publication) {
        moq_pub_publish_cfg_t pp;
        moq_pub_publish_cfg_init(&pp);
        CHECK(moq_pub_publish_track(f->facade, f->track, &pp, 1000) == MOQ_OK);
        CHECK(pump(f));
        moq_event_t e;
        CHECK(moq_session_poll_events(f->peer, &e, 1) == 1);
        CHECK(e.kind == MOQ_EVENT_PUBLISH_REQUEST);
        f->peer_pub = e.u.publish_request.pub;
        moq_event_cleanup(&e);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        CHECK(moq_session_accept_publish(f->peer, f->peer_pub, &ac, 1000) == MOQ_OK);
    } else {
        moq_subscribe_cfg_t sc;
        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){&ns, 1};
        sc.track_name = MOQ_BYTES_LITERAL("video");
        CHECK(moq_session_subscribe(f->peer, &sc, 1000, &f->peer_sub) == MOQ_OK);
    }
    CHECK(pump(f));
    CHECK(server_events(f));
    CHECK(pump(f));
    drain(f->peer);
    if (publication) {
        CHECK(moq_pub_track_is_published(f->facade, f->track));
        for (size_t i = 0; i < f->sender->pub_cap; ++i)
            if (f->sender->publishes[i].handle._opaque)
                f->pub = f->sender->publishes[i].handle;
        CHECK(moq_publication_is_valid(f->pub));
    } else CHECK(moq_subscription_is_valid(f->sub));
    return true;
}

static void cleanup(fixture_t *f)
{
    if (f->facade) moq_pub_destroy(f->facade);
    if (f->sp) moq_simpair_destroy(f->sp);
    if (f->memory.balance != 0) {
        ++failures;
        fprintf(stderr, "allocator balance: %lld\n", (long long)f->memory.balance);
    }
}

static moq_open_counts_t *counts(fixture_t *f)
{
    for (size_t i = 0; i < f->sender->sub_cap; ++i)
        if (!f->publication && moq_subscription_eq(f->sender->subs[i].handle, f->sub))
            return &f->sender->subs[i].opened;
    for (size_t i = 0; i < f->sender->pub_cap; ++i)
        if (f->publication && moq_publication_eq(f->sender->publishes[i].handle, f->pub))
            return &f->sender->publishes[i].opened;
    return NULL;
}

static moq_result_t finish(fixture_t *f)
{
    return f->publication ? moq_pub_unpublish_track(f->facade, f->track, 1000)
        : moq_pub_finish_subscribers(f->facade, f->track, MOQ_PUB_DONE_TRACK_ENDED, 1000);
}

static bool write_one(fixture_t *f)
{
    moq_rcbuf_t *b = NULL;
    CHECK(moq_rcbuf_create(&f->alloc, (const uint8_t *)"x", 1, &b) == MOQ_OK);
    moq_result_t rc = moq_pub_write_object(f->facade, f->track, 1, 0, b, 1000);
    moq_rcbuf_decref(b);
    CHECK(rc == MOQ_OK);
    return true;
}

static size_t terminal_count(fixture_t *f, uint64_t count)
{
    size_t terminals = 0;
    moq_event_t e;
    while (moq_session_poll_events(f->peer, &e, 1) == 1) {
        bool valid = true;
        if (e.kind == MOQ_EVENT_SUBSCRIBE_DONE || e.kind == MOQ_EVENT_PUBLISH_FINISHED) {
            ++terminals;
            valid = f->publication
                ? e.kind == MOQ_EVENT_PUBLISH_FINISHED &&
                  moq_publication_eq(e.u.publish_finished.pub, f->peer_pub) &&
                  e.u.publish_finished.status_code == 2 && e.u.publish_finished.stream_count == count
                : e.kind == MOQ_EVENT_SUBSCRIBE_DONE &&
                  moq_subscription_eq(e.u.subscribe_done.sub, f->peer_sub) &&
                  e.u.subscribe_done.status_code == 2 && e.u.subscribe_done.stream_count == count;
        }
        moq_event_cleanup(&e);
        if (!valid) { ++failures; fprintf(stderr, "incorrect terminal\n"); }
    }
    return terminals;
}

static bool check_wire(fixture_t *f, uint64_t count)
{
    CHECK(f->wires == 1 && f->decoded && f->wire_count == count && f->wire_status == 2);
    CHECK(f->version == MOQ_VERSION_DRAFT_16 || f->wire_fin);
    return true;
}

static size_t queued_done(fixture_t *f)
{
    fixture_t observation = {0};
    observation.version = f->version;
    for (size_t i = f->sender->action_head; i < f->sender->action_tail; ++i) {
        const moq_action_t *a = &f->sender->actions[i % f->sender->action_cap];
        if (a->kind == MOQ_ACTION_SEND_CONTROL)
            wire(&observation, a->u.send_control.data, a->u.send_control.len, false);
        else if (a->kind == MOQ_ACTION_SEND_BIDI_STREAM)
            wire(&observation, a->u.send_bidi_stream.data, a->u.send_bidi_stream.len,
                 a->u.send_bidi_stream.fin);
    }
    return observation.wires;
}

static bool prompt(fixture_t *f)
{
    CHECK(write_one(f));
    CHECK(moq_pub_end_group(f->facade, f->track, 1000) == MOQ_OK);
    CHECK(f->sender->pending_open_count == 1);
    CHECK(finish(f) == MOQ_ERR_WOULD_BLOCK);
    CHECK(f->wires == 0 && queued_done(f) == 0 && terminal_count(f, 0) == 0);
    CHECK(pump(f));
    CHECK(f->sender->pending_open_count == 0);
    CHECK(counts(f) && counts(f)->subgroup == 1 && !counts(f)->subgroup_external);
    CHECK(f->fins == 1);
    CHECK(finish(f) == MOQ_OK);
    CHECK(pump(f));
    CHECK(check_wire(f, 1));
    CHECK(terminal_count(f, 1) == 1);
    CHECK(pump(f));
    CHECK(terminal_count(f, 1) == 0 && f->wires == 1);
    CHECK(moq_simpair_now_us(f->sp) == 1000 && f->peer->last_now_us == 1000);
    return true;
}

static bool private_feedback(fixture_t *f, bool abort_batch)
{
    uint64_t rx_before = f->sp->next_rx_ref;
    CHECK(write_one(f));
    /* Cancel the producer BEFORE the observing batch: suppression cannot
     * leave a live subgroup that later emits a FIN-only opening. */
    if (!abort_batch)
        CHECK(moq_pub_reset_group(f->facade, f->track, 0, 1000) == MOQ_OK);
    moq_action_t actions[16];
    moq_core_observed_open_t opens[16];
    size_t n = 0;
    CHECK(moq_core_internal_virtual_poll(f->sender, f->sp, actions, opens, 16, &n) == MOQ_OK);
    CHECK(n > 0 && f->sender->virtual_batch);
    uint64_t epoch = f->sender->borrow_epoch;
    size_t token = n;
    for (size_t i = 0; i < n; ++i) if (opens[i].ref) token = i;
    CHECK(token < n);
    moq_core_observed_open_t bad = opens[token];
    ++bad.owner;
    CHECK(!moq_core_internal_virtual_resolve(f->sender, f->sp, &bad, MOQ_CORE_VIRTUAL_OPENED));
    bad = opens[token]; ++bad.ref;
    CHECK(!moq_core_internal_virtual_resolve(f->sender, f->sp, &bad, MOQ_CORE_VIRTUAL_OPENED));
    bad = opens[token]; bad.mode ^= 0xff;
    CHECK(!moq_core_internal_virtual_resolve(f->sender, f->sp, &bad, MOQ_CORE_VIRTUAL_OPENED));
    CHECK(!moq_core_internal_virtual_resolve(f->peer, f->sp, &opens[token], MOQ_CORE_VIRTUAL_OPENED));
    size_t raw = 99;
    moq_action_t a;
    CHECK(moq_session_poll_actions_ex(f->sender, &a, 1, sizeof(a), &raw) == MOQ_ERR_WRONG_STATE && raw == 0);
    for (size_t i = 0; i < n; ++i) moq_action_cleanup(&actions[i]);
    if (abort_batch) {
        CHECK(moq_core_internal_virtual_finish(f->sender, f->sp, false) == MOQ_ERR_WRONG_STATE);
        CHECK(f->sender->virtual_batch); /* caller still owes explicit abort */
        CHECK(moq_core_internal_virtual_finish(f->sender, f->sp, true) == MOQ_OK);
        CHECK(counts(f) && counts(f)->subgroup_external && counts(f)->subgroup == 0);
    } else {
        /* Producer is permanently reset; no cleaned action was admitted. */
        CHECK(moq_core_internal_virtual_resolve(f->sender, f->sp, &opens[token], MOQ_CORE_VIRTUAL_SUPPRESSED));
        CHECK(!moq_core_internal_virtual_resolve(f->sender, f->sp, &opens[token], MOQ_CORE_VIRTUAL_OPENED));
        CHECK(moq_core_internal_virtual_finish(f->sender, f->sp, false) == MOQ_OK);
        CHECK(counts(f) && counts(f)->subgroup == 0 && !counts(f)->subgroup_external);
    }
    CHECK(f->sender->borrow_epoch == epoch && !f->sender->virtual_batch);
    CHECK(f->sender->pending_open_count == 0);
    CHECK(!moq_core_internal_virtual_resolve(f->sender, f->sp, &opens[token], MOQ_CORE_VIRTUAL_OPENED));
    if (!abort_batch) {
        CHECK(finish(f) == MOQ_OK);
        CHECK(pump(f));
        CHECK(check_wire(f, 0));
        CHECK(terminal_count(f, 0) == 1);
        CHECK(f->headers == 0);
        for (size_t i = 0; i < MOQ_SIM_MAX_DATA_STREAMS; ++i)
            CHECK(!f->sp->stream_map[i].active);
        CHECK(f->sp->next_rx_ref == rx_before); /* no virtual stream was ever admitted */
    }
    return true;
}

static bool failed_driver(fixture_t *f)
{
    CHECK(write_one(f));
    f->invalidate = true;
    CHECK(moq_simpair_step(f->sp, NULL) == MOQ_ERR_INTERNAL);
    size_t maps = 0;
    for (size_t i = 0; i < MOQ_SIM_MAX_DATA_STREAMS; ++i)
        if (f->sp->stream_map[i].active) ++maps;
    CHECK(maps == 1); /* admission succeeded before epoch feedback failed */
    CHECK(counts(f) && counts(f)->subgroup_external && counts(f)->subgroup == 0);
    CHECK(!f->sender->virtual_batch && !f->sp->borrowed_sender);
    CHECK(f->sender->pending_open_count == 0);
    size_t n = 99;
    CHECK(moq_core_internal_virtual_poll(f->sender, f->sp, NULL, NULL, 0, &n) == MOQ_OK && n == 0);
    return true;
}

static bool recover_wait(fixture_t *f, uint64_t count, uint64_t processed)
{
    uint64_t deadline = 0;
    bool attributable = false;
    if (f->publication) {
        for (size_t i = 0; i < f->peer->pub_cap; ++i) {
            moq_pub_entry_t *e = &f->peer->publishes[i];
            if (!moq_publication_eq(e->handle, f->peer_pub)) continue;
            attributable = e->done_pending && e->done_stream_count == count && e->processed_stream_count == processed;
            deadline = e->done_deadline_us;
        }
    } else {
        for (size_t i = 0; i < f->peer->sub_cap; ++i) {
            moq_sub_entry_t *e = &f->peer->subs[i];
            if (!moq_subscription_eq(e->handle, f->peer_sub)) continue;
            attributable = e->done_pending && e->done_stream_count == count && e->processed_stream_count == processed;
            deadline = e->done_deadline_us;
        }
    }
    CHECK(attributable && deadline > 1000 && deadline != UINT64_MAX);
    CHECK(moq_session_tick(f->peer, deadline - 1) == MOQ_OK);
    CHECK(terminal_count(f, count) == 0);
    CHECK(moq_session_tick(f->peer, deadline) == MOQ_OK);
    CHECK(terminal_count(f, count) == 1);
    CHECK(moq_session_tick(f->peer, deadline) == MOQ_OK);
    CHECK(terminal_count(f, count) == 0 && f->wires == 1);
    return true;
}

static bool raw_escape(fixture_t *f)
{
    CHECK(write_one(f));
    CHECK(moq_pub_end_group(f->facade, f->track, 1000) == MOQ_OK);
    moq_action_t actions[16];
    size_t n = moq_session_poll_actions(f->sender, actions, 16);
    CHECK(n > 0);
    moq_stream_ref_t rx = moq_stream_ref_from_u64(9000);
    size_t data = 0;
    for (size_t i = 0; i < n; ++i) {
        if (actions[i].kind == MOQ_ACTION_SEND_DATA) {
            ++data;
            moq_send_data_action_t *d = &actions[i].u.send_data;
            moq_result_t rc = MOQ_OK;
            if (d->header_len) rc = moq_session_on_data_bytes(f->peer, rx, d->header, d->header_len, false, 1000);
            if (rc == MOQ_OK && d->payload) rc = moq_session_on_data_bytes(f->peer, rx,
                moq_rcbuf_data(d->payload), moq_rcbuf_len(d->payload), false, 1000);
            if (rc == MOQ_OK && d->fin) rc = moq_session_on_data_bytes(f->peer, rx, NULL, 0, true, 1000);
            moq_action_cleanup(&actions[i]);
            CHECK(rc == MOQ_OK);
        } else moq_action_cleanup(&actions[i]);
    }
    CHECK(data > 0 && counts(f) && counts(f)->subgroup_external);
    CHECK(finish(f) == MOQ_OK);
    CHECK(pump(f));
    const uint64_t unknown = f->version == MOQ_VERSION_DRAFT_22
        ? UINT64_MAX : (UINT64_C(1) << 62) - 1;
    CHECK(f->sender->profile->stream_count_unknown == unknown);
    CHECK(f->peer->profile->stream_count_unknown == unknown);
    CHECK(check_wire(f, unknown));
    CHECK(terminal_count(f, unknown) == 0);
    CHECK(recover_wait(f, unknown, 1));
    return true;
}

static bool lost_header(fixture_t *f)
{
    CHECK(write_one(f));
    moq_simpair_enable_faults(f->sp);
    CHECK(pump(f));
    CHECK(f->drops > 0 && f->inputs == 0);
    CHECK(counts(f) && counts(f)->subgroup == 1 && !counts(f)->subgroup_external);

    CHECK(finish(f) == MOQ_OK);
    CHECK(pump(f));
    CHECK(check_wire(f, 1));
    CHECK(terminal_count(f, 1) == 0); /* physical open, missing attribution */
    CHECK(recover_wait(f, 1, 0));
    return true;
}

static bool native_priority(fixture_t *f)
{
    fake_endpoint_t ep;
    fake_endpoint_init(&ep, 100, 200);
    ep.vtable.capabilities |= MOQ_TRANSPORT_CAP_HOLD_INPUT;
    moq_transport_bridge_cfg_t cfg;
    moq_transport_bridge_cfg_init(&cfg, &f->alloc);
    moq_transport_bridge_t *bridge = NULL;
    CHECK(moq_transport_bridge_create(&cfg, f->sender, &ep.vtable, &ep, &bridge) == MOQ_OK);
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    moq_bytes_t ns = MOQ_BYTES_LITERAL("native-priority");
    sc.track_namespace = (moq_namespace_t){&ns, 1};
    sc.track_name = MOQ_BYTES_LITERAL("queued");
    moq_subscription_t queued;
    CHECK(moq_session_subscribe(f->peer, &sc, 1000, &queued) == MOQ_OK);
    uint64_t step = f->sp->step, epoch = f->sender->borrow_epoch;
    size_t ch = f->peer->action_head, sh = f->sender->action_head;
    size_t a = 77, b = 88;
    moq_result_t rc = moq_simpair_step_directional(f->sp, &a, &b);
    bool stable = rc == MOQ_ERR_WRONG_STATE && a == 77 && b == 88 &&
        f->sp->step == step && f->sender->borrow_epoch == epoch &&
        f->peer->action_head == ch && f->sender->action_head == sh;
    moq_transport_bridge_destroy(bridge);
    fake_endpoint_clear_ops(&ep);
    CHECK(stable);
    CHECK(pump(f)); /* no active virtual stream migration was requested */
    return true;
}

static bool map_refusal(fixture_t *f)
{
    for (size_t i = 0; i <= MOQ_SIM_MAX_DATA_STREAMS; ++i) {
        moq_subgroup_cfg_t cfg;
        moq_subgroup_cfg_init(&cfg);
        cfg.group_id = i + 1;
        moq_subgroup_handle_t sg;
        moq_result_t rc = f->publication
            ? moq_session_open_pub_subgroup(f->sender, f->pub, &cfg, 1000, &sg)
            : moq_session_open_subgroup(f->sender, f->sub, &cfg, 1000, &sg);
        CHECK(rc == MOQ_OK);
        if (i < MOQ_SIM_MAX_DATA_STREAMS) CHECK(pump(f));
        else CHECK(moq_simpair_step(f->sp, NULL) == MOQ_ERR_INTERNAL);
    }
    CHECK(counts(f) && counts(f)->subgroup == MOQ_SIM_MAX_DATA_STREAMS && counts(f)->subgroup_external);
    CHECK(f->sender->pending_open_count == 0 && !f->sender->virtual_batch && !f->sp->borrowed_sender);
    /* Failed reservation was never an open; abort is unknown because the
     * sender's live stream was not irrevocably cancelled on hard failure. */
    return true;
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    for (size_t v = 0; v < sizeof(versions) / sizeof(versions[0]); ++v) {
        for (unsigned p = 0; p < 2; ++p) {
            for (unsigned row = 0; row < 9; ++row) {
                fixture_t f;
                if (setup(&f, versions[v], p != 0)) {
                    bool ok = row == 0 ? prompt(&f) : row == 1 ? private_feedback(&f, false)
                        : row == 2 ? private_feedback(&f, true) : row == 3 ? failed_driver(&f)
                        : row == 4 ? raw_escape(&f) : row == 5 ? lost_header(&f)
                        : row == 6 ? native_priority(&f) : row == 7 ? map_refusal(&f)
                        : finish(&f) == MOQ_OK && pump(&f) && check_wire(&f, 0) && terminal_count(&f, 0) == 1;
                    if (!ok) { ++failures; fprintf(stderr, "v%d publication=%u row=%u\n", versions[v], p, row); }
                }
                cleanup(&f);
            }
        }
    }
    return failures ? 1 : 0;
}
