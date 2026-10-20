#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE: %s:%d: %s\n", __FILE__, __LINE__, #x); \
    exit(2); } } while (0)
#define CHECK(x) do { if (!(x)) { \
    fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #x); \
    failures++; } } while (0)

static int failures;
static unsigned rows;

typedef struct fixture {
    moq_simpair_t *pair;
    moq_session_t *c, *s;
    moq_subscription_t sub, peer_sub;
    int slot;
    moq_alloc_t original;
    bool fail_probe;
    size_t probe_size, probes, wiped, other_allocations;
    void *probe;
} fixture_t;

/* Track the exact temporary allocation, not arbitrary frees of the same size.
 * Poison its entire capacity so a partial wipe cannot pass accidentally. */
static void *probe_alloc(size_t n, void *ctx)
{
    fixture_t *f = ctx;
    if (n != f->probe_size) {
        f->other_allocations++;
        return f->original.alloc(n, f->original.ctx);
    }
    f->probes++;
    if (f->fail_probe) return NULL;
    REQUIRE(f->probe == NULL);
    f->probe = f->original.alloc(n, f->original.ctx);
    REQUIRE(f->probe != NULL);
    memset(f->probe, 0xa5, n);
    return f->probe;
}

static void *probe_realloc(void *p, size_t old_size, size_t n, void *ctx)
{
    fixture_t *f = ctx;
    f->other_allocations++;
    return f->original.realloc(p, old_size, n, f->original.ctx);
}

static void probe_free(void *p, size_t n, void *ctx)
{
    fixture_t *f = ctx;
    if (p && p == f->probe) {
        CHECK(n == f->probe_size);
        const uint8_t *bytes = p;
        bool zero = true;
        for (size_t i = 0; i < f->probe_size; i++)
            if (bytes[i] != 0) zero = false;
        CHECK(zero);
        f->wiped++;
        f->probe = NULL;
    }
    f->original.free(p, n, f->original.ctx);
}

static void drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
}

static void pump(fixture_t *f)
{
    REQUIRE(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
}

static void open_pair(fixture_t *f, moq_version_t version, bool with_fill)
{
    memset(f, 0, sizeof(*f));
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = moq_alloc_default();
    cfg.version = version;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 32;
    REQUIRE(moq_simpair_create(&cfg, &f->pair) == MOQ_OK);
    REQUIRE(moq_simpair_start(f->pair) == MOQ_OK);
    pump(f);
    f->c = moq_simpair_client(f->pair);
    f->s = moq_simpair_server(f->pair);
    REQUIRE(moq_session_state(f->c) == MOQ_SESS_ESTABLISHED);
    REQUIRE(moq_session_state(f->s) == MOQ_SESS_ESTABLISHED);
    drain(f->c); drain(f->s);

    moq_bytes_t part = MOQ_BYTES_LITERAL("capacity");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init_sized(&sc, sizeof(sc));
    sc.track_namespace = (moq_namespace_t){ &part, 1 };
    sc.track_name = MOQ_BYTES_LITERAL("track");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    sc.start_group = 7;
    sc.group_order = MOQ_GROUP_ORDER_ASCENDING;
    sc.has_forward = true;
    sc.forward = !with_fill;
    REQUIRE(moq_session_subscribe(f->c, &sc, 1000, &f->sub) == MOQ_OK);
    pump(f);
    moq_event_t ev;
    unsigned requests = 0, foreign = 0;
    while (moq_session_poll_events(f->s, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
            f->peer_sub = ev.u.subscribe_request.sub;
            requests++;
        } else foreign++;
        moq_event_cleanup(&ev);
    }
    REQUIRE(requests == 1 && foreign == 0);
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = 5; ac.largest_object = 2;
    REQUIRE(moq_session_accept_subscribe(f->s, f->peer_sub, &ac, 1000) == MOQ_OK);
    pump(f);
    drain(f->c); drain(f->s);
    REQUIRE(moq_session_set_defer_subscription_updates(f->s, true) == MOQ_OK);
    REQUIRE(moq_session_tick(f->c, 1000) == MOQ_OK);
    REQUIRE(f->c->send_len == 0);
    REQUIRE(f->c->action_head == f->c->action_tail);
    f->slot = sub_resolve_handle(f->c, f->sub);
    REQUIRE(f->slot >= 0 && f->c->subs[f->slot].state == MOQ_SUB_ESTABLISHED);
}

typedef struct saved_bytes { void *bytes; size_t len; } saved_bytes_t;

static saved_bytes_t save_bytes(const void *p, size_t n)
{
    saved_bytes_t saved = { NULL, n };
    if (n) {
        REQUIRE(p != NULL);
        saved.bytes = malloc(n);
        REQUIRE(saved.bytes != NULL);
        memcpy(saved.bytes, p, n);
    }
    return saved;
}

static void check_bytes(saved_bytes_t saved, const void *p)
{
    if (saved.len) CHECK(p && memcmp(saved.bytes, p, saved.len) == 0);
    free(saved.bytes);
}

typedef struct snapshot {
    moq_sub_entry_t sub;
    moq_token_cache_t cache;
    saved_bytes_t profile, ids, refs, send, fills, sides, tokens;
    size_t action_head, action_tail, event_head, event_tail, send_len, scratch_len;
    size_t fill_live;
    uint64_t next_ref, event_progress;
} snapshot_t;

static snapshot_t snapshot(fixture_t *f)
{
    moq_session_t *s = f->c;
    snapshot_t v;
    memset(&v, 0, sizeof(v));
    memcpy(&v.sub, &s->subs[f->slot], sizeof(v.sub));
    memcpy(&v.cache, &s->peer_token_cache, sizeof(v.cache));
    v.profile = save_bytes(s->profile_state, s->profile->state_size);
    v.ids = save_bytes(s->idx_req_by_rid, (s->idx_req_mask + 1) * sizeof(moq_index_entry_t));
    v.refs = save_bytes(s->idx_req_by_streamref,
        (s->idx_req_streamref_mask + 1) * sizeof(moq_index_entry_t));
    v.send = save_bytes(s->send_buf, s->send_len);
    v.fills = save_bytes(s->fill_storage,
        s->fill_storage ? s->fill_cap * sizeof(moq_fill_entry_t) : 0);
    v.sides = save_bytes(s->fill_side,
        s->fill_side ? s->fill_cap * sizeof(moq_fill_side_t) : 0);
    v.tokens = save_bytes(s->peer_token_cache.entries,
        s->peer_token_cache.cap * sizeof(moq_token_cache_entry_t));
    v.action_head = s->action_head; v.action_tail = s->action_tail;
    v.event_head = s->event_head; v.event_tail = s->event_tail;
    v.send_len = s->send_len; v.scratch_len = s->output_scratch_len;
    v.next_ref = s->next_stream_ref; v.fill_live = s->fills.live;
    v.event_progress = s->event_progress_token;
    return v;
}

static void unchanged(fixture_t *f, snapshot_t v)
{
    moq_session_t *s = f->c;
    CHECK(memcmp(&v.sub, &s->subs[f->slot], sizeof(v.sub)) == 0);
    CHECK(memcmp(&v.cache, &s->peer_token_cache, sizeof(v.cache)) == 0);
    CHECK(s->action_head == v.action_head && s->action_tail == v.action_tail);
    CHECK(s->event_head == v.event_head && s->event_tail == v.event_tail);
    CHECK(s->event_progress_token == v.event_progress);
    CHECK(s->send_len == v.send_len && s->output_scratch_len == v.scratch_len);
    CHECK(s->next_stream_ref == v.next_ref && s->fills.live == v.fill_live);
    check_bytes(v.profile, s->profile_state);
    check_bytes(v.ids, s->idx_req_by_rid);
    check_bytes(v.refs, s->idx_req_by_streamref);
    check_bytes(v.send, s->send_buf);
    check_bytes(v.fills, s->fill_storage);
    check_bytes(v.sides, s->fill_side);
    check_bytes(v.tokens, s->peer_token_cache.entries);
}

static void check_peer_update(fixture_t *f, const moq_subscribe_updated_event_t *u,
    const moq_auth_token_t *token, bool with_fill)
{
    CHECK(moq_subscription_eq(u->sub, f->peer_sub));
    CHECK(u->answer_owed && u->update_count == 1);
    CHECK(u->has_subscriber_priority && u->subscriber_priority == 42);
    CHECK(u->has_forward && u->forward == with_fill);
    CHECK(u->token_count == 1 && u->tokens != NULL);
    if (u->token_count == 1 && u->tokens) {
        CHECK(u->tokens[0].token_type == token->token_type);
        CHECK(u->tokens[0].token_value.len == token->token_value.len);
        if (u->tokens[0].token_value.len == token->token_value.len)
            CHECK(u->tokens[0].token_value.data &&
                memcmp(u->tokens[0].token_value.data, token->token_value.data,
                       token->token_value.len) == 0);
    }
    CHECK(u->fill_requested == with_fill);
    if (moq_session_version(f->c) == MOQ_VERSION_DRAFT_22) {
        CHECK(u->options != NULL);
        if (u->options) {
            const moq_subscription_update_options_t *o = u->options;
            CHECK(o->flags == (MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION |
                MOQ_SUBSCRIPTION_UPDATE_OPTIONS_OBJECT_DELIVERY_TIMEOUT |
                MOQ_SUBSCRIPTION_UPDATE_OPTIONS_SUBGROUP_DELIVERY_TIMEOUT));
            CHECK(o->location.kind == MOQ_LOCATION_FILTER_OBJECT_END);
            CHECK(o->location.start_group == 3 && o->location.start_object == 1);
            CHECK(o->location.end_group == 5 && o->location.end_object == 2);
            CHECK(o->object_delivery_timeout_ms == 7 && o->subgroup_delivery_timeout_ms == 11);
            CHECK(o->initiating_request_id == u->update_request_id);
        }
    } else {
        CHECK(u->options == NULL);
        CHECK(u->has_filter && u->filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE);
        CHECK(u->start_group == 3 && u->start_object == 1 && u->end_group == 5);
        CHECK(u->has_delivery_timeout && u->delivery_timeout_us == 5000);
    }
}

/* Exercise a real deferred response: local admission must pend selected fields
 * without latching them, and an outstanding UPDATE must not allocate a probe. */
static void acknowledge(fixture_t *f, const moq_subscription_update_cfg_t *cfg,
    const moq_auth_token_t *token, bool with_fill)
{
    moq_sub_entry_t *e = &f->c->subs[f->slot];
    CHECK(e->update_pending && e->update_has_forward && e->update_forward == with_fill);
    CHECK(e->forward == !with_fill);
    CHECK(e->filter_type == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START && e->req_start_group == 7);
    CHECK(e->update_has_filter && e->update_start_group == 3 && e->update_start_object == 1);
    CHECK(e->update_end_group == 5);
    CHECK(e->dt_upd_has_object && e->dt_upd_has_subgroup);
    uint64_t rid = e->update_request_id;
    moq_event_t ev;
    unsigned created = 0, foreign = 0;
    while (moq_session_poll_events(f->c, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_FILL_CREATED) {
            created++;
            CHECK(moq_subscription_eq(ev.u.fill_created.sub, f->sub));
            CHECK(ev.u.fill_created.initiating_request_id == rid);
        } else foreign++;
        moq_event_cleanup(&ev);
    }
    CHECK(created == (with_fill ? 1u : 0u) && foreign == 0);
    CHECK(f->c->fills.live == (with_fill ? 1u : 0u));
    if (with_fill) {
        unsigned live = 0;
        for (size_t i = 0; i < f->c->fills.capacity; i++) {
            const moq_fill_entry_t *child = &f->c->fill_storage[i];
            if (!child->active) continue;
            live++;
            CHECK(child->generation == 1 && child->initiating_id == rid);
            CHECK(child->control == MOQ_FILL_CONTROL_PENDING);
            const moq_fill_input_t *in = &f->c->fill_side[i].input;
            CHECK(in->start_group == 3 && in->start_object == 1);
            CHECK(in->end_group == 5 && in->end_object == 2);
            CHECK(in->subscriber_priority == 42);
            CHECK(in->has_fill_timeout && in->fill_timeout_ms == 19);
        }
        CHECK(live == 1);
    }
    pump(f);
    unsigned updates = 0;
    foreign = 0;
    while (moq_session_poll_events(f->s, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
            updates++;
            check_peer_update(f, &ev.u.subscribe_updated, token, with_fill);
            CHECK(ev.u.subscribe_updated.update_request_id == rid);
        } else foreign++;
        moq_event_cleanup(&ev);
    }
    CHECK(updates == 1 && foreign == 0);
    CHECK(e->update_pending && e->forward == !with_fill);
    REQUIRE(moq_session_tick(f->c, 1000) == MOQ_OK);
    snapshot_t v = snapshot(f);
    CHECK(moq_session_update_subscription(f->c, f->sub, cfg, 1000) == MOQ_ERR_WRONG_STATE);
    unchanged(f, v);
    moq_accept_subscription_update_cfg_t ac;
    moq_accept_subscription_update_cfg_init(&ac);
    ac.update_request_id = rid;
    REQUIRE(moq_session_accept_subscription_update(f->s, f->peer_sub, &ac, 1000) == MOQ_OK);
    pump(f);
    unsigned acks = 0;
    foreign = 0;
    while (moq_session_poll_events(f->c, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK) {
            acks++;
            CHECK(moq_subscription_eq(ev.u.subscription_update_ok.sub, f->sub));
        } else foreign++;
        moq_event_cleanup(&ev);
    }
    CHECK(acks == 1 && foreign == 0);
    CHECK(!e->update_pending && !e->update_has_forward && !e->update_has_filter);
    CHECK(!e->dt_upd_has_object && !e->dt_upd_has_subgroup);
    CHECK(e->forward == with_fill);
    CHECK(e->req_start_group == 3 && e->req_start_object == 1 && e->req_end_group == 5);
    if (moq_session_version(f->c) == MOQ_VERSION_DRAFT_22) {
        CHECK(e->filter_type == MOQ_SUBSCRIBE_FILTER_OBJECT_END_INTERNAL && e->req_end_object == 2);
        CHECK(e->subscriber_priority == 42);
        CHECK(e->dt_sub_object_ms == 7 && e->dt_sub_subgroup_ms == 11);
    } else {
        CHECK(e->filter_type == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE);
        CHECK(e->dt_sub_object_ms == 5 && e->dt_sub_subgroup_ms == 5);
    }
    unsigned requests = 0;
    foreign = 0;
    while (moq_session_poll_events(f->s, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_FILL_REQUEST) {
            requests++;
            CHECK(moq_subscription_eq(ev.u.fill_request.sub, f->peer_sub));
            CHECK(ev.u.fill_request.initiating_request_id == rid);
            CHECK(ev.u.fill_request.start_group == 3 && ev.u.fill_request.start_object == 1);
            CHECK(ev.u.fill_request.end_group == 5 && ev.u.fill_request.end_object == 2);
        } else foreign++;
        moq_event_cleanup(&ev);
    }
    CHECK(requests == (with_fill ? 1u : 0u) && foreign == 0);
    CHECK(moq_session_state(f->c) == MOQ_SESS_ESTABLISHED);
    CHECK(moq_session_state(f->s) == MOQ_SESS_ESTABLISHED);
}

enum capacity_case { EMPTY, OCCUPIED, TOO_LARGE, SCRATCH, ALLOC_FAIL };
static const char *const case_names[] = { "empty", "occupied", "too_large", "scratch", "alloc_fail" };

static void capacity_row(moq_version_t version, enum capacity_case mode, bool with_fill)
{
    fixture_t f;
    open_pair(&f, version, with_fill);
    int before = failures;
    rows++;
    size_t cap = f.c->send_cap, scratch_cap = f.c->output_scratch_cap;
    REQUIRE(cap >= 256);
    f.c->send_cap = 256;
    uint8_t bytes[300];
    memset(bytes, 0xa7, sizeof(bytes));
    /* Fixed opaque bytes exercise the encoder's variable-length and D16
     * scratch paths; no credential selection or token-cache policy is tested. */
    moq_auth_token_t token = { .token_type = 7,
        .token_value = { bytes, mode == TOO_LARGE ? sizeof(bytes) : 80 } };
    moq_subscription_update_cfg_t up;
    moq_subscription_update_cfg_init_sized(&up, sizeof(up));
    up.has_subscriber_priority = true; up.subscriber_priority = 42;
    up.has_forward = true; up.forward = with_fill;
    up.auth_tokens = &token; up.auth_token_count = 1;
    up.has_filter = true; up.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE;
    up.start_group = 3; up.start_object = 1; up.end_group = 5;
    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_OBJECT_END;
    loc.start_group = 3; loc.start_object = 1; loc.end_group = 5; loc.end_object = 2;
    if (version == MOQ_VERSION_DRAFT_22) {
        up.location = &loc;
        up.delivery_timeout_flags = MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT |
                                    MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
        up.object_delivery_timeout_ms = 7; up.subgroup_delivery_timeout_ms = 11;
    } else {
        up.has_delivery_timeout = true; up.delivery_timeout_us = 5000;
    }
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.has_fill_timeout = true; fill.fill_timeout_ms = 19;
    if (with_fill) {
        up.fill = &fill;
        /* Allocate the lazy pool before targeting the probe allocation. This
         * pure preparation neither consumes a generation nor creates a child. */
        moq_fill_input_t input, effective;
        moq_fill_reservation_t reservation;
        REQUIRE(moq_fill_input_normalize(&fill, &input) == MOQ_OK);
        REQUIRE(fill_local_prepare(f.c, fill_parent_sub(f.c, (size_t)f.slot),
            9999, &input, 42, MOQ_GROUP_ORDER_ASCENDING,
            MOQ_SUBSCRIBE_FILTER_OBJECT_END_INTERNAL, 3, 1, 5, 2,
            &reservation, &effective) == MOQ_OK);
        REQUIRE(f.c->fills.live == 0 && f.c->fill_storage != NULL);
    }
    bool occupied = mode != EMPTY;
    uint8_t filler[255];
    memset(filler, 0x5a, sizeof(filler));
    if (occupied) REQUIRE(queue_send_control(f.c, filler, sizeof(filler)) == MOQ_OK);
    if (mode == SCRATCH) f.c->output_scratch_cap = 1;
    snapshot_t v = snapshot(&f);
    f.original = f.c->alloc;
    f.probe_size = 256;
    f.fail_probe = mode == ALLOC_FAIL;
    f.c->alloc = (moq_alloc_t){ .ctx = &f, .alloc = probe_alloc,
        .realloc = probe_realloc, .free = probe_free };
    moq_result_t rc = moq_session_update_subscription(f.c, f.sub, &up, 1000);
    f.c->alloc = f.original;
    moq_result_t expected = mode == EMPTY ? MOQ_OK : mode == ALLOC_FAIL ? MOQ_ERR_NOMEM :
        (mode == TOO_LARGE || mode == SCRATCH) ? MOQ_ERR_BUFFER : MOQ_ERR_WOULD_BLOCK;
    CHECK(rc == expected);
    CHECK(f.other_allocations == 0 && f.probe == NULL);
    CHECK(f.probes == (occupied ? 1u : 0u));
    CHECK(f.wiped == (occupied && mode != ALLOC_FAIL ? 1u : 0u));
    if (occupied) unchanged(&f, v);
    else {
        /* This row committed, so release the snapshot without comparing it. */
        free(v.profile.bytes); free(v.ids.bytes); free(v.refs.bytes); free(v.send.bytes);
        free(v.fills.bytes); free(v.sides.bytes); free(v.tokens.bytes);
    }
    if (occupied) {
        /* Discard only fixture output; malformed filler never reaches a peer. */
        moq_action_t a;
        REQUIRE(moq_session_poll_actions(f.c, &a, 1) == 1);
        REQUIRE(a.kind == MOQ_ACTION_SEND_CONTROL);
        CHECK(a.u.send_control.len == sizeof(filler));
        CHECK(memcmp(a.u.send_control.data, filler, sizeof(filler)) == 0);
        moq_action_cleanup(&a);
        REQUIRE(moq_session_poll_actions(f.c, &a, 1) == 0);
        REQUIRE(moq_session_tick(f.c, 1000) == MOQ_OK);
        v = snapshot(&f);
        rc = moq_session_update_subscription(f.c, f.sub, &up, 1000);
        if (mode == TOO_LARGE || mode == SCRATCH) {
            CHECK(rc == MOQ_ERR_BUFFER);
            unchanged(&f, v);
        } else {
            CHECK(rc == MOQ_OK);
            free(v.profile.bytes); free(v.ids.bytes); free(v.refs.bytes); free(v.send.bytes);
            free(v.fills.bytes); free(v.sides.bytes); free(v.tokens.bytes);
        }
    }
    if (rc == MOQ_OK) acknowledge(&f, &up, &token, with_fill);
    f.c->send_cap = cap;
    f.c->output_scratch_cap = scratch_cap;
    moq_simpair_destroy(f.pair);
    printf("%s: update_capacity(draft=%u,case=%s,fill=%d)\n",
        failures == before ? "PASS" : "FAIL", (unsigned)version, case_names[mode], with_fill ? 1 : 0);
}

int main(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < sizeof(versions) / sizeof(versions[0]); i++)
        for (enum capacity_case c = EMPTY; c <= ALLOC_FAIL; c++)
            if (c != SCRATCH || versions[i] == MOQ_VERSION_DRAFT_16)
                capacity_row(versions[i], c, false);
    capacity_row(MOQ_VERSION_DRAFT_22, OCCUPIED, true);
    capacity_row(MOQ_VERSION_DRAFT_22, ALLOC_FAIL, true);
    printf("update_capacity: %u rows, %d failure(s)\n", rows, failures);
    return failures ? 1 : 0;
}
