/* Subscription update options. The v0 update configuration layout (through
 * end_group) is declared here independently of the current header and pinned
 * by size and offset, and every case compares literal wire bytes rather than
 * bytes produced by the encoder under test. */
#include <moq/session.h>
#include <moq/location.h>
#include <stddef.h>
#include <stdint.h>

typedef struct old_update {
    uint32_t struct_size;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    bool has_forward, forward, has_delivery_timeout;
    uint64_t delivery_timeout_us;
    const moq_auth_token_t *auth_tokens;
    size_t auth_token_count;
    bool has_new_group_request;
    uint64_t new_group_request;
    bool has_filter;
    moq_subscribe_filter_t filter;
    uint64_t start_group, start_object, end_group;
} old_update_t;

typedef struct expected_update {
    old_update_t old;
    const moq_location_filter_cfg_t *location;
    const moq_fill_cfg_t *fill;
    uint32_t delivery_timeout_flags, reserved;
    uint64_t object_delivery_timeout_ms, subgroup_delivery_timeout_ms;
} expected_update_t;

typedef struct old_detail {
    moq_subscription_t sub;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    bool has_forward, forward, has_delivery_timeout;
    uint8_t pad[5];
    uint64_t delivery_timeout_us;
    const moq_resolved_token_t *tokens;
    size_t token_count;
    bool has_new_group_request;
    uint64_t new_group_request;
    bool has_filter;
    moq_subscribe_filter_t filter;
    uint64_t start_group, start_object, end_group;
} old_detail_t;

typedef struct expected_options {
    uint32_t struct_size, flags;
    uint64_t initiating_request_id;
    moq_location_filter_cfg_t location;
    uint64_t object_delivery_timeout_ms, subgroup_delivery_timeout_ms;
} expected_options_t;

#ifdef MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION
#ifdef __cplusplus
#define PIN(c) static_assert(c, #c)
#else
#define PIN(c) _Static_assert(c, #c)
#endif
PIN(offsetof(moq_subscription_update_cfg_t, location) == sizeof(old_update_t));
PIN(offsetof(moq_subscription_update_cfg_t, fill) ==
    offsetof(expected_update_t, fill));
PIN(offsetof(moq_subscription_update_cfg_t, subgroup_delivery_timeout_ms) ==
    offsetof(expected_update_t, subgroup_delivery_timeout_ms));
PIN(sizeof(moq_subscription_update_cfg_t) == sizeof(expected_update_t));
PIN(offsetof(moq_subscribe_updated_event_t, options) == sizeof(old_detail_t));
PIN(sizeof(expected_options_t) == 80);
PIN(offsetof(moq_subscription_update_options_t, resolution) == sizeof(expected_options_t));
PIN(sizeof(moq_subscription_update_options_t) == 160);
PIN(sizeof(moq_subscribe_updated_event_t) <= 128);
PIN(sizeof(((moq_event_t *)0)->u) == 128);
PIN(sizeof(moq_event_t) == 144);
#if UINTPTR_MAX == UINT64_MAX
PIN(sizeof(old_update_t) == 88);
PIN(sizeof(moq_subscription_update_cfg_t) == 128);
PIN(offsetof(moq_subscribe_updated_event_t, options) == 96);
PIN(offsetof(moq_subscribe_updated_event_t, answer_owed) == 104);
PIN(sizeof(moq_subscribe_updated_event_t) == 120);
#elif defined(__arm__)
PIN(sizeof(old_update_t) == 80);
PIN(sizeof(moq_subscription_update_cfg_t) == 112);
PIN(offsetof(moq_subscribe_updated_event_t, options) == 88);
PIN(offsetof(moq_subscribe_updated_event_t, answer_owed) == 96);
PIN(sizeof(moq_subscribe_updated_event_t) == 112);
#elif defined(__i386__)
PIN(sizeof(old_update_t) == (sizeof(moq_subscribe_filter_t) == 1 ? 68 : 72));
PIN(sizeof(moq_subscription_update_cfg_t) == (sizeof(moq_subscribe_filter_t) == 1 ? 100 : 104));
PIN(offsetof(moq_subscribe_updated_event_t, options) == (sizeof(moq_subscribe_filter_t) == 1 ? 76 : 80));
PIN(offsetof(moq_subscribe_updated_event_t, answer_owed) == (sizeof(moq_subscribe_filter_t) == 1 ? 80 : 84));
PIN(sizeof(moq_subscribe_updated_event_t) == (sizeof(moq_subscribe_filter_t) == 1 ? 96 : 100));
#endif
#undef PIN
#endif

#ifndef MOQ_UPDATE_LAYOUT_ONLY
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include "../../core/src/session/request_stream_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned checks, failures;
#define CHECK(c) do { checks++; if (!(c)) { failures++; \
    fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); } } while (0)
#define REQUIRE(c) do { checks++; if (!(c)) { failures++; \
    fprintf(stderr, "%s:%d: prerequisite: %s\n", __FILE__, __LINE__, #c); \
    goto done; } } while (0)

typedef struct balance { int64_t bytes, blocks; bool bad_free; } balance_t;
static void *b_alloc(size_t n, void *ctx)
{
    balance_t *b = (balance_t *)ctx;
    if (n > SIZE_MAX - 16) return NULL;
    uint8_t *p = (uint8_t *)malloc(n + 16);
    if (!p) return NULL;
    memcpy(p, &n, sizeof(n));
    b->bytes += (int64_t)n; b->blocks++;
    return p + 16;
}
static void b_free(void *p, size_t n, void *ctx)
{
    if (!p) return;
    balance_t *b = (balance_t *)ctx;
    size_t actual;
    memcpy(&actual, (uint8_t *)p - 16, sizeof(actual));
    if (actual != n) b->bad_free = true;
    b->bytes -= (int64_t)actual; b->blocks--;
    free((uint8_t *)p - 16);
}
static void *b_realloc(void *p, size_t old, size_t n, void *ctx)
{
    void *q = b_alloc(n, ctx);
    if (!q) return NULL;
    if (p) memcpy(q, p, old < n ? old : n);
    b_free(p, old, ctx);
    return q;
}

typedef struct pair {
    moq_simpair_t *sp;
    moq_session_t *c, *sv;
    moq_sub_entry_t *ce, *se;
    moq_subscription_t cs, ss;
    balance_t balance;
    moq_alloc_t alloc;
} pair_t;

static void drain(moq_session_t *s)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) moq_event_cleanup(&ev);
}
static bool take(moq_session_t *s, moq_event_kind_t kind, moq_event_t *out)
{
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1)) {
        if (ev.kind == kind) { *out = ev; return true; }
        moq_event_cleanup(&ev);
    }
    return false;
}
static bool pump(pair_t *p)
{
    return moq_simpair_run_until_quiescent(p->sp, 64, NULL) >= 0;
}
static bool mk(pair_t *p, moq_version_t version)
{
    memset(p, 0, sizeof(*p));
    p->alloc = (moq_alloc_t){ &p->balance, b_alloc, b_realloc, b_free };
    moq_simpair_cfg_t sc = MOQ_SIMPAIR_CFG_INIT;
    sc.alloc = &p->alloc; sc.version = version;
    sc.client_send_request_capacity = sc.server_send_request_capacity = true;
    sc.client_initial_request_capacity = sc.server_initial_request_capacity = 64;
    sc.auth_token_cache_size = 256;
    if (moq_simpair_create(&sc, &p->sp) != MOQ_OK ||
        moq_simpair_start(p->sp) != MOQ_OK || !pump(p)) return false;
    p->c = moq_simpair_client(p->sp); p->sv = moq_simpair_server(p->sp);
    drain(p->c); drain(p->sv);
    moq_subscribe_cfg_t cfg; moq_subscribe_cfg_init(&cfg);
    moq_bytes_t parts[] = { MOQ_BYTES_LITERAL("u") };
    cfg.track_namespace = (moq_namespace_t){ parts, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("v");
    cfg.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    cfg.start_group = 1;
    if (moq_session_subscribe(p->c, &cfg, 1, &p->cs) != MOQ_OK || !pump(p))
        return false;
    moq_event_t ev;
    if (!take(p->sv, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    p->ss = ev.u.subscribe_request.sub; moq_event_cleanup(&ev);
    moq_accept_subscribe_cfg_t ac; moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true; ac.largest_group = 9; ac.largest_object = 7;
    if (moq_session_accept_subscribe(p->sv, p->ss, &ac, 2) != MOQ_OK || !pump(p))
        return false;
    drain(p->c); drain(p->sv);
    int ci = sub_resolve_handle(p->c, p->cs), si = sub_resolve_handle(p->sv, p->ss);
    if (ci < 0 || si < 0) return false;
    p->ce = &p->c->subs[ci]; p->se = &p->sv->subs[si];
    return true;
}
static void finish(pair_t *p)
{
    if (p->sp) moq_simpair_destroy(p->sp);
    CHECK(p->balance.bytes == 0 && p->balance.blocks == 0 && !p->balance.bad_free);
}
static moq_result_t update(pair_t *p, expected_update_t *u)
{
    union {
        moq_subscription_update_cfg_t align;
        uint8_t bytes[sizeof(expected_update_t) + 16];
    } storage;
    memset(&storage, 0, sizeof(storage));
    memcpy(storage.bytes, u, sizeof(*u));
    return moq_session_update_subscription(p->c, p->cs, &storage.align, 3);
}
static bool options_copy(moq_session_t *s, const moq_event_t *ev,
                         expected_options_t *out)
{
    const expected_options_t *ptr = NULL;
    if (ev->detail_size < sizeof(old_detail_t) + sizeof(ptr)) return false;
    memcpy(&ptr, (const uint8_t *)&ev->u + sizeof(old_detail_t), sizeof(ptr));
    uintptr_t lo = (uintptr_t)s->event_scratch, at = (uintptr_t)ptr;
    if (!ptr || at < lo || at - lo > s->event_scratch_len ||
        sizeof(*out) > s->event_scratch_len - (at - lo) ||
        at % _Alignof(expected_options_t)) return false;
    memcpy(out, ptr, sizeof(*out));
    return true;
}

static bool pressure_token_matches(const moq_session_t *s, const moq_event_t *ev)
{
    if (ev->u.subscribe_updated.token_count != 1) return false;
    const moq_resolved_token_t *token = ev->u.subscribe_updated.tokens;
    uintptr_t lo = (uintptr_t)s->event_scratch, at = (uintptr_t)token;
    if (!token || at < lo || at - lo > s->event_scratch_len ||
        sizeof(*token) > s->event_scratch_len - (at - lo) ||
        at % _Alignof(moq_resolved_token_t)) return false;
    if (token->token_type != 9 || token->token_value.len != 1) return false;
    const uint8_t *value = token->token_value.data;
    at = (uintptr_t)value;
    if (!value || at < lo || at - lo >= s->event_scratch_len) return false;
    return value[0] == 'a';
}

/* Bad observations must be refused by the test before any token dereference. */
static void token_storage_controls(void)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    static const uint8_t byte = 'a';
    uint8_t *value = event_scratch_copy(p.sv, &byte, 1);
    REQUIRE(value != NULL);
    moq_resolved_token_t *token = event_scratch_alloc_aligned(p.sv,
        sizeof(*token), _Alignof(moq_resolved_token_t));
    REQUIRE(token != NULL);
    memset(token, 0, sizeof(*token));
    token->token_type = 9;
    token->token_value = (moq_bytes_t){ value, 1 };
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_SUBSCRIBE_UPDATED;
    ev.u.subscribe_updated.token_count = 1;
    ev.u.subscribe_updated.tokens = token;
    CHECK(pressure_token_matches(p.sv, &ev));
    ev.u.subscribe_updated.token_count = 0;
    CHECK(!pressure_token_matches(p.sv, &ev));
    ev.u.subscribe_updated.token_count = 2;
    CHECK(!pressure_token_matches(p.sv, &ev));
    ev.u.subscribe_updated.token_count = 1;
    ev.u.subscribe_updated.tokens = NULL;
    CHECK(!pressure_token_matches(p.sv, &ev));
    ev.u.subscribe_updated.tokens = (const moq_resolved_token_t *)((uintptr_t)token + 1);
    CHECK(!pressure_token_matches(p.sv, &ev));
    ev.u.subscribe_updated.tokens = (const moq_resolved_token_t *)(
        p.sv->event_scratch + p.sv->event_scratch_len);
    CHECK(!pressure_token_matches(p.sv, &ev));
    ev.u.subscribe_updated.tokens = token;
    size_t used = p.sv->event_scratch_len;
    p.sv->event_scratch_len = (uintptr_t)token - (uintptr_t)p.sv->event_scratch + sizeof(*token) - 1;
    CHECK(!pressure_token_matches(p.sv, &ev));
    p.sv->event_scratch_len = used;
    token->token_value.data = NULL;
    CHECK(!pressure_token_matches(p.sv, &ev));
    token->token_value.data = p.sv->event_scratch + used;
    CHECK(!pressure_token_matches(p.sv, &ev));
    token->token_value.data = value;
    token->token_value.len = 2;
    CHECK(!pressure_token_matches(p.sv, &ev));
    token->token_value.len = 1;
    token->token_type = 8;
    CHECK(!pressure_token_matches(p.sv, &ev));
    token->token_type = 9;
    *value = 'b';
    CHECK(!pressure_token_matches(p.sv, &ev));
    *value = 'a';
    CHECK(pressure_token_matches(p.sv, &ev));
    moq_event_cleanup(&ev);
done:
    finish(&p);
}

static bool action_bytes(moq_session_t *s, const uint8_t *bytes, size_t len)
{
    moq_action_t a;
    if (moq_session_poll_actions(s, &a, 1) != 1) return false;
    bool ok = a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
        !a.u.send_bidi_stream.fin && a.u.send_bidi_stream.len == len &&
        memcmp(a.u.send_bidi_stream.data, bytes, len) == 0;
    moq_action_cleanup(&a);
    return ok;
}

static const uint8_t empty_wire[] = { 2, 0, 2, 2, 0 };
static const uint8_t object_zero[] = { 2, 0, 4, 2, 1, 2, 0 };
static const uint8_t ack_wire[] = { 7, 0, 4, 1, 9, 9, 7 };
static const uint8_t subgroup_zero[] = { 2, 0, 4, 2, 1, 6, 0 };
static const uint8_t object_max[] = {
    2, 0, 12, 2, 1, 2, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t subgroup_max[] = {
    2, 0, 12, 2, 1, 6, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t both_zero[] = { 2, 0, 6, 2, 2, 2, 0, 4, 0 };
static const uint8_t mixed[] = {
    2, 0, 14, 2, 2, 2, 0, 4, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t none_wire[] = { 2, 0, 4, 2, 1, 33, 0 };
static const uint8_t range_wire[] = { 2, 0, 8, 2, 1, 33, 4, 1, 2, 2, 3 };
static const uint8_t priority_wire[] = { 2, 0, 4, 2, 1, 32, 3 };

typedef struct timeout_case {
    uint32_t flags;
    uint64_t object, subgroup;
    const uint8_t *wire;
    size_t len;
} timeout_case_t;
#define ROW(f, o, g, w) { f, o, g, w, sizeof(w) }
static const timeout_case_t timeouts[] = {
    ROW(0, 0, 0, empty_wire), ROW(1, 0, 0, object_zero),
    ROW(2, 0, 0, subgroup_zero), ROW(1, UINT64_MAX, 0, object_max),
    ROW(2, 0, UINT64_MAX, subgroup_max), ROW(3, 0, 0, both_zero),
    ROW(3, 0, UINT64_MAX, mixed)
};
#undef ROW

static void roundtrip(size_t which, bool split, unsigned location_kind)
{
    const timeout_case_t *row = &timeouts[which];
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(u);
    u.delivery_timeout_flags = row->flags;
    u.object_delivery_timeout_ms = row->object;
    u.subgroup_delivery_timeout_ms = row->subgroup;
    moq_location_filter_cfg_t location; moq_location_filter_cfg_init(&location);
    const uint8_t *wire = row->wire; size_t n = row->len;
    if (location_kind) {
        u.location = &location;
        u.old.has_filter = true; u.old.filter = (moq_subscribe_filter_t)99;
        u.old.start_group = UINT64_MAX; u.old.end_group = 0;
        location.kind = location_kind == 1 ? MOQ_LOCATION_FILTER_NONE
                                           : MOQ_LOCATION_FILTER_OBJECT_END;
        location.start_group = 1; location.start_object = 2;
        location.end_group = 3; location.end_object = 3;
        wire = location_kind == 1 ? none_wire : range_wire;
        n = location_kind == 1 ? sizeof(none_wire) : sizeof(range_wire);
    }
    REQUIRE(update(&p, &u) == MOQ_OK);
    memset(&u, 0xA5, sizeof(u)); memset(&location, 0xA5, sizeof(location));
    REQUIRE(action_bytes(p.c, wire, n));
    size_t first = split ? n - 1 : n;
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, wire, first,
                                            false, 3) == MOQ_OK);
    if (split) {
        CHECK(p.sv->event_tail == p.sv->event_head);
        REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, wire + first,
                                                n - first, false, 3) == MOQ_OK);
    }
    moq_event_t ev;
    REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    expected_options_t opts;
    bool got = options_copy(p.sv, &ev, &opts);
    CHECK(!ev.u.subscribe_updated.has_filter && !ev.u.subscribe_updated.has_delivery_timeout);
    CHECK(ev.u.subscribe_updated.delivery_timeout_us == 0 &&
          ev.u.subscribe_updated.start_group == 0 && ev.u.subscribe_updated.end_group == 0);
    CHECK(ev.borrow_epoch == p.sv->borrow_epoch);
    moq_event_cleanup(&ev);
    REQUIRE(got);
    CHECK(opts.struct_size == sizeof(moq_subscription_update_options_t) &&
          opts.initiating_request_id == 2);
    CHECK(opts.flags == (location_kind ? 1u : row->flags << 1));
    CHECK(opts.object_delivery_timeout_ms == row->object &&
          opts.subgroup_delivery_timeout_ms == row->subgroup);
    if (location_kind) {
        CHECK(opts.location.struct_size == 48);
        CHECK(opts.location.kind == (location_kind == 1 ? MOQ_LOCATION_FILTER_NONE
                                                       : MOQ_LOCATION_FILTER_OBJECT_END));
        if (location_kind == 2) CHECK(opts.location.end_object == 3 && opts.location.end_group == 3);
        else CHECK(opts.location.start_group == 0 && opts.location.end_object == 0);
    } else {
        moq_location_filter_cfg_t zero; memset(&zero, 0, sizeof(zero));
        CHECK(memcmp(&opts.location, &zero, sizeof(zero)) == 0);
    }
    REQUIRE(action_bytes(p.sv, ack_wire, sizeof(ack_wire)));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.c, p.ce->rs.ref, ack_wire,
                                            sizeof(ack_wire), false, 4) == MOQ_OK);
    REQUIRE(take(p.c, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev));
    CHECK(ev.u.subscription_update_ok.has_largest &&
          ev.u.subscription_update_ok.largest_group == 9 &&
          ev.u.subscription_update_ok.largest_object == 7);
    CHECK(!ev.u.subscription_update_ok.has_expires && ev.u.subscription_update_ok.expires_ms == 0);
    moq_event_cleanup(&ev);
    CHECK(!p.ce->update_pending && p.ce->update_request_id == 0);
    CHECK(p.ce->dt_sub_has_object == ((row->flags & 1) != 0));
    CHECK(p.ce->dt_sub_has_subgroup == ((row->flags & 2) != 0));
    if (row->flags & 1) CHECK(p.ce->dt_sub_object_ms == row->object);
    if (row->flags & 2) CHECK(p.ce->dt_sub_subgroup_ms == row->subgroup);
    if (location_kind == 2) CHECK(p.ce->req_end_object == 3 && p.se->req_end_object == 3);
    if (location_kind == 1) CHECK(p.ce->filter_type == MOQ_SUBSCRIBE_FILTER_NONE);
    if (!location_kind) CHECK(p.ce->filter_type == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START);
    CHECK(p.sv->event_tail == p.sv->event_head && p.sv->action_tail == p.sv->action_head);
done:
    finish(&p);
}

static void no_effect_refusal(pair_t *p, expected_update_t *u, moq_result_t expected)
{
    moq_sub_entry_t before = *p->ce;
    uint64_t epoch = p->c->borrow_epoch, now = p->c->last_now_us;
    size_t ah = p->c->action_head, at = p->c->action_tail;
    size_t send = p->c->send_len, live = p->c->fills.live;
    CHECK(update(p, u) == expected);
    CHECK(memcmp(&before, p->ce, sizeof(before)) == 0);
    CHECK(p->c->borrow_epoch == epoch && p->c->last_now_us == now);
    CHECK(p->c->action_head == ah && p->c->action_tail == at &&
          p->c->send_len == send && p->c->fills.live == live);
}

static void validation(moq_version_t version)
{
    pair_t p;
    REQUIRE(mk(&p, version));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(u);
    u.old.has_forward = true;
    u.old.has_delivery_timeout = true;
    if (version == MOQ_VERSION_DRAFT_22) {
        no_effect_refusal(&p, &u, MOQ_ERR_UNSUPPORTED);
        u.old.delivery_timeout_us = UINT64_MAX;
        no_effect_refusal(&p, &u, MOQ_ERR_UNSUPPORTED);
    }
    u.old.has_delivery_timeout = false;
    u.delivery_timeout_flags = 4;
    no_effect_refusal(&p, &u, MOQ_ERR_INVAL);
    u.delivery_timeout_flags = 0; u.reserved = 1;
    no_effect_refusal(&p, &u, MOQ_ERR_INVAL);
    u.reserved = 0;
    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_GROUP_END; loc.start_group = 2; loc.end_group = 1;
    u.location = &loc;
    no_effect_refusal(&p, &u, version == MOQ_VERSION_DRAFT_22 ? MOQ_ERR_INVAL : MOQ_ERR_UNSUPPORTED);
    u.location = NULL;
    if (version != MOQ_VERSION_DRAFT_22) {
        /* Draft-16's single DELIVERY_TIMEOUT is never 0 (9.2.2.2); on
         * draft-18 a 0 means no timeout (8). */
        u.delivery_timeout_flags = 1;
        if (version == MOQ_VERSION_DRAFT_16)
            no_effect_refusal(&p, &u, MOQ_ERR_INVAL);
        /* Typed timeouts beside the generic one state two values. */
        u.old.has_delivery_timeout = true; u.old.delivery_timeout_us = 123000;
        no_effect_refusal(&p, &u, MOQ_ERR_INVAL);
        u.old.has_delivery_timeout = false; u.old.delivery_timeout_us = 0;
        u.delivery_timeout_flags = 0;
        moq_fill_cfg_t fill; moq_fill_cfg_init(&fill); u.fill = &fill;
        no_effect_refusal(&p, &u, MOQ_ERR_UNSUPPORTED);
        u.fill = NULL;
        u.old.has_delivery_timeout = true; u.old.delivery_timeout_us = 123000;
        REQUIRE(update(&p, &u) == MOQ_OK && pump(&p));
        moq_event_t ev;
        REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
        CHECK(ev.u.subscribe_updated.has_delivery_timeout &&
              ev.u.subscribe_updated.delivery_timeout_us == 123000 &&
              ev.u.subscribe_updated.options == NULL);
        moq_event_cleanup(&ev);
    }
done:
    finish(&p);
}

static void initializer_canaries(void)
{
    union { moq_subscription_update_cfg_t align; uint8_t bytes[sizeof(expected_update_t) + 16]; } mem;
    memset(&mem, 0xA5, sizeof(mem));
    moq_subscription_update_cfg_init(&mem.align);
    size_t floor = offsetof(old_update_t, auth_tokens);
    CHECK(mem.align.struct_size == floor);
    for (size_t i = floor; i < sizeof(mem); i++) CHECK(mem.bytes[i] == 0xA5);
    for (size_t cap = 0; cap <= sizeof(mem); cap++) {
        memset(&mem, 0xA5, sizeof(mem));
        moq_subscription_update_cfg_init_sized(&mem.align, cap);
        size_t n = cap < sizeof(uint32_t) ? 0 : cap < sizeof(expected_update_t) ? cap : sizeof(expected_update_t);
        if (n) CHECK(mem.align.struct_size == n);
        for (size_t i = sizeof(uint32_t); i < n; i++) CHECK(mem.bytes[i] == 0);
        for (size_t i = n; i < sizeof(mem); i++) CHECK(mem.bytes[i] == 0xA5);
    }
}

static void truncation(size_t cap, unsigned which)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)cap;
    u.old.has_subscriber_priority = true; u.old.subscriber_priority = 3;
    if (which == 1) u.location = (const moq_location_filter_cfg_t *)(uintptr_t)1;
    if (which == 2) u.fill = (const moq_fill_cfg_t *)(uintptr_t)1;
    if (which == 3) { u.delivery_timeout_flags = UINT32_MAX; u.reserved = UINT32_MAX; }
    if (which == 4) { u.object_delivery_timeout_ms = UINT64_MAX; u.subgroup_delivery_timeout_ms = UINT64_MAX; }
    if (which == 5) {
        u.old.has_filter = true; u.old.filter = (moq_subscribe_filter_t)99;
        u.old.start_group = UINT64_MAX; u.old.end_group = 0;
    }
    REQUIRE(update(&p, &u) == MOQ_OK);
    CHECK(action_bytes(p.c, priority_wire, sizeof(priority_wire)));
    CHECK(p.c->fills.live == 0);
done:
    finish(&p);
}

static void pointer_boundaries(bool fill_only)
{
    static const uint8_t fill_wire[] = { 2, 0, 5, 2, 1, 35, 1, 0 };
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_OBJECT_END;
    loc.start_group = 1; loc.start_object = 2; loc.end_group = 3; loc.end_object = 3;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    expected_update_t u; memset(&u, 0, sizeof(u));
    if (fill_only) {
        u.old.struct_size = (uint32_t)offsetof(expected_update_t, delivery_timeout_flags);
        u.fill = &fill;
        u.delivery_timeout_flags = UINT32_MAX; u.reserved = UINT32_MAX;
    } else {
        u.old.struct_size = (uint32_t)offsetof(expected_update_t, fill);
        u.location = &loc; u.fill = (const moq_fill_cfg_t *)(uintptr_t)1;
    }
    REQUIRE(update(&p, &u) == MOQ_OK);
    CHECK(action_bytes(p.c, fill_only ? fill_wire : range_wire,
                        fill_only ? sizeof(fill_wire) : sizeof(range_wire)));
    CHECK(p.c->fills.live == (fill_only ? 1u : 0u));
done:
    finish(&p);
}

static void old_event_prefix(void)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, empty_wire,
                                            sizeof(empty_wire), false, 3) == MOQ_OK);
    union { moq_event_t align; uint8_t bytes[sizeof(moq_event_t) + 16]; } mem;
    memset(&mem, 0xA5, sizeof(mem));
    size_t old_size = offsetof(moq_event_t, u) + sizeof(old_detail_t), count = 0;
    REQUIRE(moq_session_poll_events_ex(p.sv, mem.bytes, 1, old_size, &count) == MOQ_OK && count == 1);
    CHECK(mem.align.kind == MOQ_EVENT_SUBSCRIBE_UPDATED);
    CHECK(mem.align.detail_size == sizeof(moq_subscribe_updated_event_t));
    for (size_t i = old_size; i < sizeof(mem); i++) CHECK(mem.bytes[i] == 0xA5);
    REQUIRE(action_bytes(p.sv, ack_wire, sizeof(ack_wire)));
done:
    finish(&p);
}

/* Original receive owner, ID, authorization and fill view survive pressure. */
static void pressure(unsigned which)
{
    static const uint8_t original[] = {
        2, 0, 19, 2, 5, 2, 0, 1, 4, 1, 7, 9, 'a', 29, 3,
        1, 2, 1, 2, 2, 1, 0 };
    pair_t p;
    size_t saved_cap = 0;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    saved_cap = p.sv->event_scratch_cap;
    moq_event_t filler; memset(&filler, 0, sizeof(filler));
    filler.kind = MOQ_EVENT_SUBSCRIBE_OK;
    if (which == 1) {
        moq_action_t a; memset(&a, 0, sizeof(a)); a.kind = MOQ_ACTION_SEND_CONTROL;
        while (push_action(p.sv, &a) == MOQ_OK) { }
    } else if (which == 2) {
        while (push_event(p.sv, &filler) == MOQ_OK) { }
    } else {
        REQUIRE(push_event(p.sv, &filler) == MOQ_OK);
        /* With one occupied byte: align + options + one token byte + align
         * + token record needs sizeof(options)+40. Empty scratch fits. */
        p.sv->event_scratch_cap = sizeof(moq_subscription_update_options_t) + 39;
        uint8_t byte = 0x5C;
        REQUIRE(event_scratch_copy(p.sv, &byte, 1) != NULL);
    }
    uint8_t wire[sizeof(original)]; memcpy(wire, original, sizeof(wire));
    moq_result_t rc = moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, wire,
                                                      sizeof(wire), false, 3);
    REQUIRE(rc == (which == 3 ? MOQ_ERR_BUFFER : MOQ_ERR_WOULD_BLOCK));
    CHECK(moq_request_stream_peer_next_id(p.sv) == 2 && p.sv->fills.live == 0);
    CHECK(p.se->subscriber_priority == 128 && !p.se->dt_sub_has_object);
    CHECK(p.se->rs.recv_len == sizeof(original));
    CHECK(moq_token_cache_lookup(&p.sv->peer_token_cache, 7, NULL, NULL, NULL) != MOQ_TOKEN_OK);
    if (which == 3) CHECK(p.sv->event_scratch_len == 1 && p.sv->event_scratch[0] == 0x5C);
    memset(wire, 0xA5, sizeof(wire));
    moq_action_t a;
    while (moq_session_poll_actions(p.sv, &a, 1)) {
        CHECK(a.kind != MOQ_ACTION_SEND_BIDI_STREAM && a.kind != MOQ_ACTION_CLOSE_SESSION);
        moq_action_cleanup(&a);
    }
    drain(p.sv);
    p.se->hist->largest_group = 19; p.se->hist->largest_object = 17;
    p.se->subscriber_priority = 11;
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, NULL, 0, false, 30) == MOQ_OK);
    CHECK(moq_request_stream_peer_next_id(p.sv) == 4 && p.se->rs.recv_len == 0);
    CHECK(p.sv->fills.live == 1 && p.se->subscriber_priority == 3);
    moq_event_t ev;
    REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    expected_options_t opts;
    bool got = options_copy(p.sv, &ev, &opts);
    bool token_matches = pressure_token_matches(p.sv, &ev);
    moq_event_cleanup(&ev);
    REQUIRE(token_matches);
    REQUIRE(got);
    CHECK(opts.flags == 3 && opts.initiating_request_id == 2 &&
          opts.location.kind == MOQ_LOCATION_FILTER_ABSOLUTE_START && opts.location.start_object == 2);
    REQUIRE(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    CHECK(ev.u.fill_request.initiating_request_id == 2 && ev.u.fill_request.subscriber_priority == 3);
    CHECK(ev.u.fill_request.processed_at_us == 3 && ev.u.fill_request.end_group == 9 &&
          ev.u.fill_request.end_object == 7);
    moq_event_cleanup(&ev);
    REQUIRE(action_bytes(p.sv, ack_wire, sizeof(ack_wire)));
    uint64_t type = 0; const uint8_t *value = NULL; size_t len = 0;
    CHECK(moq_token_cache_lookup(&p.sv->peer_token_cache, 7, &type, &value, &len) == 0);
    CHECK(type == 9 && len == 1 && value && value[0] == 'a');
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, NULL, 0, false, 31) == MOQ_OK);
    CHECK(p.sv->event_tail == p.sv->event_head && p.sv->action_tail == p.sv->action_head);
    CHECK(p.sv->fills.live == 1 && moq_request_stream_peer_next_id(p.sv) == 4);
done:
    if (p.sv && saved_cap) p.sv->event_scratch_cap = saved_cap;
    finish(&p);
}

static void sticky_timeouts(void)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(u);
    u.delivery_timeout_flags = 3;
    u.subgroup_delivery_timeout_ms = UINT64_MAX;
    REQUIRE(update(&p, &u) == MOQ_OK && pump(&p));
    moq_event_t ev;
    REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    expected_options_t original, opts;
    bool got = options_copy(p.sv, &ev, &original); moq_event_cleanup(&ev);
    REQUIRE(got);
    CHECK(original.flags == 6 && original.subgroup_delivery_timeout_ms == UINT64_MAX);
    drain(p.c); drain(p.sv);
    CHECK(p.ce->dt_sub_has_object && p.ce->dt_sub_object_ms == 0 &&
          p.ce->dt_sub_has_subgroup && p.ce->dt_sub_subgroup_ms == UINT64_MAX);
    u.delivery_timeout_flags = 1; u.object_delivery_timeout_ms = 5;
    REQUIRE(update(&p, &u) == MOQ_OK && pump(&p));
    REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    got = options_copy(p.sv, &ev, &opts);
    moq_event_cleanup(&ev);
    REQUIRE(got);
    CHECK(opts.flags == 2 && opts.object_delivery_timeout_ms == 5 &&
          opts.subgroup_delivery_timeout_ms == 0 && opts.initiating_request_id == 4);
    CHECK(original.flags == 6 && original.object_delivery_timeout_ms == 0 &&
          original.subgroup_delivery_timeout_ms == UINT64_MAX && original.initiating_request_id == 2);
    CHECK(p.ce->dt_sub_object_ms == 5 && p.se->dt_sub_object_ms == 5 &&
          p.ce->dt_sub_subgroup_ms == UINT64_MAX && p.se->dt_sub_subgroup_ms == UINT64_MAX);
    drain(p.c); drain(p.sv);
    u.delivery_timeout_flags = 0;
    REQUIRE(update(&p, &u) == MOQ_OK && pump(&p));
    REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    got = options_copy(p.sv, &ev, &opts); moq_event_cleanup(&ev);
    REQUIRE(got);
    CHECK(opts.flags == 0 && opts.initiating_request_id == 6);
    CHECK(p.ce->dt_sub_object_ms == 5 && p.ce->dt_sub_subgroup_ms == UINT64_MAX);
done:
    finish(&p);
}

static void overlapping_fills(void)
{
    static const uint8_t first[] = { 2, 0, 13, 2, 3, 32, 3, 1, 4, 1, 2, 2, 3, 2, 1, 0 };
    static const uint8_t second[] = { 2, 0, 5, 4, 1, 35, 1, 0 };
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_OBJECT_END;
    loc.start_group = 1; loc.start_object = 2; loc.end_group = 3; loc.end_object = 3;
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(u);
    u.old.has_subscriber_priority = true; u.old.subscriber_priority = 3;
    u.location = &loc; u.fill = &fill;
    REQUIRE(update(&p, &u) == MOQ_OK);
    memset(&fill, 0xA5, sizeof(fill)); memset(&loc, 0xA5, sizeof(loc));
    REQUIRE(action_bytes(p.c, first, sizeof(first)));
    moq_event_t ev;
    REQUIRE(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t f1 = ev.u.fill_created.fill;
    CHECK(ev.u.fill_created.initiating_request_id == 2); moq_event_cleanup(&ev);
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, first, sizeof(first), false, 3) == MOQ_OK);
    drain(p.sv);
    REQUIRE(action_bytes(p.sv, ack_wire, sizeof(ack_wire)));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.c, p.ce->rs.ref, ack_wire, sizeof(ack_wire), false, 4) == MOQ_OK);
    drain(p.c);
    CHECK(p.ce->subscriber_priority == 3 && p.ce->req_end_object == 3);
    moq_fill_cfg_init(&fill);
    memset(&u, 0, sizeof(u)); u.old.struct_size = (uint32_t)sizeof(u); u.fill = &fill;
    REQUIRE(update(&p, &u) == MOQ_OK);
    memset(&fill, 0xA5, sizeof(fill));
    REQUIRE(action_bytes(p.c, second, sizeof(second)));
    REQUIRE(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
    moq_fill_t f2 = ev.u.fill_created.fill;
    CHECK(ev.u.fill_created.initiating_request_id == 4 && f1._opaque != f2._opaque);
    moq_event_cleanup(&ev);
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, second, sizeof(second), false, 5) == MOQ_OK);
    REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    expected_options_t opts;
    bool got = options_copy(p.sv, &ev, &opts); moq_event_cleanup(&ev);
    REQUIRE(got);
    CHECK(opts.flags == 0 && opts.initiating_request_id == 4);
    REQUIRE(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
    CHECK(ev.u.fill_request.initiating_request_id == 4 && ev.u.fill_request.subscriber_priority == 3);
    CHECK(ev.u.fill_request.start_group == 1 && ev.u.fill_request.start_object == 2 &&
          ev.u.fill_request.end_group == 3 && ev.u.fill_request.end_object == 3);
    moq_event_cleanup(&ev);
    REQUIRE(action_bytes(p.sv, ack_wire, sizeof(ack_wire)));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.c, p.ce->rs.ref, ack_wire, sizeof(ack_wire), false, 6) == MOQ_OK);
    drain(p.c);
    CHECK(p.c->fills.live == 2 && p.sv->fills.live == 2);
    moq_fill_entry_t info;
    moq_fill_ref_t ref1, ref2;
    REQUIRE(moq_fill_find(&p.c->fills, 2, &ref1) == MOQ_OK);
    REQUIRE(moq_fill_read(&p.c->fills, ref1, &info) == MOQ_OK);
    CHECK(info.initiating_id == 2 && info.delivery.end_object == 3 && info.delivery.subscriber_priority == 3);
    REQUIRE(moq_fill_find(&p.c->fills, 4, &ref2) == MOQ_OK);
    REQUIRE(moq_fill_read(&p.c->fills, ref2, &info) == MOQ_OK);
    CHECK(info.initiating_id == 4 && info.delivery.end_object == 3 && info.delivery.subscriber_priority == 3);
    REQUIRE(moq_session_cancel_fill(p.c, f1, 7) == MOQ_OK);
    REQUIRE(moq_fill_read(&p.c->fills, ref2, &info) == MOQ_OK);
    CHECK(info.initiating_id == 4 && !info.cancel_requested);
    REQUIRE(moq_session_cancel_fill(p.c, f2, 8) == MOQ_OK);
    CHECK(p.ce->state == MOQ_SUB_ESTABLISHED && p.se->state == MOQ_SUB_ESTABLISHED);
done:
    finish(&p);
}

static void outbound_update(void)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(u);
    u.delivery_timeout_flags = 1;
    REQUIRE(update(&p, &u) == MOQ_OK);
    CHECK(action_bytes(p.c, object_zero, sizeof(object_zero)));
done:
    finish(&p);
}

static void ingress_update(bool split)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    size_t first = split ? 4 : sizeof(object_zero);
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, object_zero, first,
                                       false, 3) == MOQ_OK);
    if (split) {
        CHECK(p.sv->event_tail == p.sv->event_head);
        REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.se->rs.ref, object_zero + first,
            sizeof(object_zero) - first, false, 3) == MOQ_OK);
    }
    moq_event_t ev;
    REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    expected_options_t opts;
    bool got = options_copy(p.sv, &ev, &opts);
    moq_event_cleanup(&ev);
    REQUIRE(got);
    CHECK(opts.struct_size == sizeof(moq_subscription_update_options_t) && opts.flags == 2);
    CHECK(opts.initiating_request_id == 2 && opts.object_delivery_timeout_ms == 0);
    CHECK(opts.subgroup_delivery_timeout_ms == 0 && opts.location.struct_size == 0);
    CHECK(action_bytes(p.sv, ack_wire, sizeof(ack_wire)));
done:
    finish(&p);
}

static void empty_update(void)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(old_update_t);
    REQUIRE(update(&p, &u) == MOQ_OK);
    CHECK(action_bytes(p.c, empty_wire, sizeof(empty_wire)));
done:
    finish(&p);
}

static void legacy_control(moq_version_t version)
{
    pair_t p;
    REQUIRE(mk(&p, version));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(old_update_t);
    CHECK(update(&p, &u) == MOQ_ERR_INVAL);
    u.old.has_forward = true;
    REQUIRE(update(&p, &u) == MOQ_OK && pump(&p));
    moq_event_t ev;
    REQUIRE(take(p.sv, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev));
    const void *ptr = (const void *)(uintptr_t)1;
    memcpy(&ptr, (const uint8_t *)&ev.u + sizeof(old_detail_t), sizeof(ptr));
    CHECK(ptr == NULL && ev.u.subscribe_updated.has_forward);
    moq_event_cleanup(&ev);
done:
    finish(&p);
}

int main(void)
{
    token_storage_controls();
    outbound_update(); ingress_update(false); ingress_update(true); empty_update();
    legacy_control(MOQ_VERSION_DRAFT_16); legacy_control(MOQ_VERSION_DRAFT_18);
    for (size_t i = 0; i < sizeof(timeouts) / sizeof(timeouts[0]); i++) {
        roundtrip(i, false, 0); roundtrip(i, true, 0);
    }
    roundtrip(0, false, 1); roundtrip(0, true, 1);
    roundtrip(0, false, 2); roundtrip(0, true, 2);
    validation(MOQ_VERSION_DRAFT_22); validation(MOQ_VERSION_DRAFT_16);
    validation(MOQ_VERSION_DRAFT_18); initializer_canaries();
    for (size_t i = sizeof(old_update_t); i < offsetof(expected_update_t, fill); i++)
        truncation(i, 1);
    for (size_t i = offsetof(expected_update_t, fill); i < offsetof(expected_update_t, delivery_timeout_flags); i++)
        truncation(i, 2);
    for (size_t i = offsetof(expected_update_t, delivery_timeout_flags); i < sizeof(expected_update_t); i++)
        truncation(i, 3);
    for (size_t i = offsetof(old_update_t, has_filter); i < sizeof(old_update_t); i++)
        truncation(i, 5);
    truncation(sizeof(expected_update_t) + 8, 4);
    pointer_boundaries(false); pointer_boundaries(true); old_event_prefix();
    pressure(1); pressure(2); pressure(3); sticky_timeouts(); overlapping_fills();
    printf("subscription_update_options: %u checks, %u failures\n", checks, failures);
    return failures ? 1 : 0;
}
#endif
