/* Independent old layouts and literal established-PUBLISH UPDATEs. */
#include <moq/session.h>
#include <stddef.h>
#include <stdint.h>

typedef struct old_update {
    uint32_t struct_size;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    bool has_forward, forward, has_delivery_timeout;
    uint8_t pad[5];
    uint64_t delivery_timeout_us;
    const moq_auth_token_t *auth_tokens;
    size_t auth_token_count;
    bool has_new_group_request;
    uint64_t new_group_request;
} old_update_t;

typedef struct expected_update {
    old_update_t old;
    const moq_location_filter_cfg_t *location;
    const moq_fill_cfg_t *fill;
    uint32_t delivery_timeout_flags, reserved;
    uint64_t object_delivery_timeout_ms, subgroup_delivery_timeout_ms;
} expected_update_t;

typedef struct old_detail {
    moq_publication_t pub;
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

typedef struct expected_detail {
    old_detail_t old;
    const moq_subscription_update_options_t *options;
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    bool answer_owed;
    bool fill_requested;
    uint32_t update_count;
    uint64_t update_request_id;
} expected_detail_t;

#ifdef __cplusplus
#define PIN(c) static_assert(c, #c)
#else
#define PIN(c) _Static_assert(c, #c)
#endif
PIN(offsetof(moq_subscription_update_options_t, resolution) == 80);
PIN(sizeof(moq_subscription_update_options_t) == 160);
PIN(sizeof(((moq_event_t *)0)->u) == 128);
PIN(sizeof(moq_event_t) == 144);
PIN(offsetof(expected_update_t, location) == sizeof(old_update_t));
PIN(offsetof(expected_detail_t, options) == sizeof(old_detail_t));
#if UINTPTR_MAX == UINT64_MAX
PIN(sizeof(old_update_t) == 56);
PIN(sizeof(expected_update_t) == 96);
PIN(sizeof(old_detail_t) == 96);
PIN(offsetof(expected_detail_t, answer_owed) == 104);
PIN(sizeof(expected_detail_t) == 120);
#elif defined(__arm__)
PIN(sizeof(old_update_t) == 48);
PIN(sizeof(expected_update_t) == 80);
PIN(sizeof(old_detail_t) == 88);
PIN(offsetof(expected_detail_t, answer_owed) == 96);
PIN(sizeof(expected_detail_t) == 112);
#elif defined(__i386__)
PIN(sizeof(old_update_t) == 44);
PIN(sizeof(expected_update_t) == 76);
PIN(sizeof(old_detail_t) == (sizeof(moq_subscribe_filter_t) == 1 ? 76 : 80));
PIN(offsetof(expected_detail_t, answer_owed) ==
    (sizeof(moq_subscribe_filter_t) == 1 ? 80 : 84));
PIN(sizeof(expected_detail_t) == (sizeof(moq_subscribe_filter_t) == 1 ? 96 : 100));
#endif

/* The baseline probe omits only compile-time checks for not-yet-added fields.
 * All runtime expectations stay enabled, including prerequisite failures. */
#ifndef MOQ_PUBLICATION_TYPED_BASELINE
PIN(offsetof(moq_publication_update_cfg_t, location) == sizeof(old_update_t));
PIN(offsetof(moq_publication_update_cfg_t, fill) == offsetof(expected_update_t, fill));
PIN(offsetof(moq_publication_update_cfg_t, delivery_timeout_flags) ==
    offsetof(expected_update_t, delivery_timeout_flags));
PIN(offsetof(moq_publication_update_cfg_t, reserved) == offsetof(expected_update_t, reserved));
PIN(offsetof(moq_publication_update_cfg_t, object_delivery_timeout_ms) ==
    offsetof(expected_update_t, object_delivery_timeout_ms));
PIN(offsetof(moq_publication_update_cfg_t, subgroup_delivery_timeout_ms) ==
    offsetof(expected_update_t, subgroup_delivery_timeout_ms));
PIN(sizeof(moq_publication_update_cfg_t) == sizeof(expected_update_t));
PIN(offsetof(moq_publish_updated_event_t, options) == sizeof(old_detail_t));
PIN(offsetof(moq_publish_updated_event_t, answer_owed) ==
    offsetof(expected_detail_t, answer_owed));
PIN(sizeof(moq_publish_updated_event_t) == sizeof(expected_detail_t));
#endif
#undef PIN

#ifndef MOQ_PUBLICATION_TYPED_LAYOUT_ONLY
#include <moq/sim.h>
#include "../../core/src/session/session_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned checks, failures, cases;
#define CHECK(c) do { checks++; if (!(c)) { failures++; \
    fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); } } while (0)
#define REQUIRE(c) do { checks++; if (!(c)) { failures++; \
    fprintf(stderr, "%s:%d: prerequisite: %s\n", __FILE__, __LINE__, #c); \
    goto done; } } while (0)
#define RUN(c) do { unsigned before_ = failures; cases++; c; \
    if (before_ != failures) fprintf(stderr, "  %s: %u failures\n", #c, failures - before_); \
    } while (0)

typedef struct balance { int64_t bytes, blocks; bool bad_free; } balance_t;
static void *b_alloc(size_t n, void *ctx)
{
    balance_t *b = ctx;
    if (n > SIZE_MAX - 16) return NULL;
    uint8_t *p = malloc(n + 16);
    if (!p) return NULL;
    memcpy(p, &n, sizeof(n));
    b->bytes += (int64_t)n; b->blocks++;
    return p + 16;
}
static void b_free(void *p, size_t n, void *ctx)
{
    if (!p) return;
    balance_t *b = ctx;
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
    moq_publication_t cp, pp;
    moq_pub_entry_t *ce, *pe;
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
static bool mk(pair_t *p, moq_version_t version, bool defer)
{
    memset(p, 0, sizeof(*p));
    p->alloc = (moq_alloc_t){ &p->balance, b_alloc, b_realloc, b_free };
    moq_simpair_cfg_t sc = MOQ_SIMPAIR_CFG_INIT;
    sc.alloc = &p->alloc; sc.version = version;
    sc.client_send_request_capacity = sc.server_send_request_capacity = true;
    sc.client_initial_request_capacity = sc.server_initial_request_capacity = 64;
    sc.auth_token_cache_size = 64;
    sc.server_defer_subscription_updates = defer;
    if (moq_simpair_create(&sc, &p->sp) != MOQ_OK ||
        moq_simpair_start(p->sp) != MOQ_OK || !pump(p)) return false;
    p->c = moq_simpair_client(p->sp); p->sv = moq_simpair_server(p->sp);
    drain(p->c); drain(p->sv);
    moq_publish_cfg_t cfg; moq_publish_cfg_init_sized(&cfg, sizeof(cfg));
    moq_bytes_t ns[] = { MOQ_BYTES_LITERAL("typed") };
    cfg.track_namespace = (moq_namespace_t){ ns, 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("publication");
    const uint8_t dynamic[] = { 0x30, 1 };
    moq_location_filter_cfg_t location; moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START; location.start_group = 1;
    if (version == MOQ_VERSION_DRAFT_22) {
        cfg.location = &location;
        cfg.track_properties = (moq_bytes_t){ dynamic, sizeof(dynamic) };
    }
    if (moq_session_publish(p->sv, &cfg, 1, &p->pp) != MOQ_OK || !pump(p)) return false;
    moq_event_t ev;
    if (!take(p->c, MOQ_EVENT_PUBLISH_REQUEST, &ev)) return false;
    p->cp = ev.u.publish_request.pub; moq_event_cleanup(&ev);
    moq_accept_publish_cfg_t ac; moq_accept_publish_cfg_init(&ac);
    if (moq_session_accept_publish(p->c, p->cp, &ac, 2) != MOQ_OK || !pump(p)) return false;
    drain(p->c); drain(p->sv);
    int ci = pub_resolve_handle(p->c, p->cp), pi = pub_resolve_handle(p->sv, p->pp);
    if (ci < 0 || pi < 0) return false;
    p->ce = &p->c->publishes[ci]; p->pe = &p->sv->publishes[pi];
    return p->ce->state == MOQ_PUB_ESTABLISHED && p->pe->state == MOQ_PUB_ESTABLISHED;
}
static void finish(pair_t *p)
{
    if (p->sp) moq_simpair_destroy(p->sp);
    CHECK(p->balance.bytes == 0 && p->balance.blocks == 0 && !p->balance.bad_free);
}
static moq_result_t update(pair_t *p, const expected_update_t *u, uint64_t now)
{
    union { moq_publication_update_cfg_t align; uint8_t bytes[sizeof(expected_update_t)]; } mem;
    memset(&mem, 0, sizeof(mem));
    memcpy(mem.bytes, u, sizeof(*u));
    return moq_session_update_publication(p->c, p->cp, &mem.align, now);
}
static bool input_layout_ready(void)
{
    /* An old layout interprets the new location pointer as fill. Refuse that
     * unsafe prerequisite before invoking it with nested location storage. */
    return sizeof(moq_publication_update_cfg_t) == sizeof(expected_update_t) &&
        offsetof(moq_publication_update_cfg_t, fill) == offsetof(expected_update_t, fill);
}
static const moq_subscription_update_options_t *options_pointer(const moq_event_t *ev)
{
    const moq_subscription_update_options_t *ptr = NULL;
    if (ev->detail_size >= sizeof(old_detail_t) + sizeof(ptr))
        memcpy(&ptr, (const uint8_t *)&ev->u + sizeof(old_detail_t), sizeof(ptr));
    return ptr;
}
static bool options_copy(const moq_session_t *s, const moq_event_t *ev,
                         moq_subscription_update_options_t *out)
{
    const moq_subscription_update_options_t *ptr = options_pointer(ev);
    uintptr_t lo = (uintptr_t)s->event_scratch, at = (uintptr_t)ptr;
    if (!ptr || at < lo || at - lo > s->event_scratch_len ||
        sizeof(*out) > s->event_scratch_len - (at - lo) ||
        at % _Alignof(moq_subscription_update_options_t)) return false;
    memcpy(out, ptr, sizeof(*out));
    return true;
}
static bool action_bytes(moq_session_t *s, moq_stream_ref_t ref,
                          const uint8_t *bytes, size_t len)
{
    moq_action_t a;
    if (moq_session_poll_actions(s, &a, 1) != 1) return false;
    bool ok = a.kind == MOQ_ACTION_SEND_BIDI_STREAM &&
        a.u.send_bidi_stream.stream_ref._v == ref._v &&
        !a.u.send_bidi_stream.fin && a.u.send_bidi_stream.len == len &&
        memcmp(a.u.send_bidi_stream.data, bytes, len) == 0;
    moq_action_cleanup(&a);
    return ok;
}

/* Draft22 sections9.5/9.20: the subscriber's first request ID is0. */
static const uint8_t empty_wire[] = { 2, 0, 2, 0, 0 };
static const uint8_t object_zero[] = { 2, 0, 4, 0, 1, 2, 0 };
static const uint8_t subgroup_zero[] = { 2, 0, 4, 0, 1, 6, 0 };
static const uint8_t object_max[] = {
    2, 0, 12, 0, 1, 2, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t subgroup_max[] = {
    2, 0, 12, 0, 1, 6, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t both_zero[] = { 2, 0, 6, 0, 2, 2, 0, 4, 0 };
static const uint8_t mixed_wire[] = {
    2, 0, 14, 0, 2, 2, 0, 4, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t opposite_wire[] = {
    2, 0, 14, 0, 2, 2, 255, 255, 255, 255, 255, 255, 255, 255, 255, 4, 0 };
static const uint8_t both_max[] = {
    2, 0, 22, 0, 2, 2, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    4, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t none_wire[] = { 2, 0, 4, 0, 1, 33, 0 };
static const uint8_t relative_wire[] = { 2, 0, 5, 0, 1, 33, 1, 2 };
static const uint8_t relative_max[] = {
    2, 0, 13, 0, 1, 33, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t range_wire[] = { 2, 0, 8, 0, 1, 33, 4, 1, 2, 2, 3 };
static const uint8_t range_max[] = {
    2, 0, 32, 0, 1, 33, 4,
    255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 0,
    255, 255, 255, 255, 255, 255, 255, 255, 255 };
static const uint8_t ack_wire[] = { 7, 0, 1, 0 };
static const uint8_t priority_wire[] = { 2, 0, 4, 0, 1, 32, 3 };

typedef struct row {
    const uint8_t *wire;
    size_t len;
    moq_subscription_update_options_t options;
} row_t;
#define TIMEOUT_ROW(w, f, o, g) { w, sizeof(w), { .struct_size = sizeof(moq_subscription_update_options_t), \
    .flags = f, .object_delivery_timeout_ms = o, .subgroup_delivery_timeout_ms = g } }
#define LOCATION_ROW(w, ...) { w, sizeof(w), { .struct_size = sizeof(moq_subscription_update_options_t), .flags = 1, \
    .location = { .struct_size = 48, __VA_ARGS__ } } }
static const row_t rows[] = {
    TIMEOUT_ROW(empty_wire, 0, 0, 0),
    TIMEOUT_ROW(object_zero, 2, 0, 0),
    TIMEOUT_ROW(subgroup_zero, 4, 0, 0),
    TIMEOUT_ROW(object_max, 2, UINT64_MAX, 0),
    TIMEOUT_ROW(subgroup_max, 4, 0, UINT64_MAX),
    TIMEOUT_ROW(both_zero, 6, 0, 0),
    TIMEOUT_ROW(mixed_wire, 6, 0, UINT64_MAX),
    LOCATION_ROW(none_wire, .kind = MOQ_LOCATION_FILTER_NONE),
    LOCATION_ROW(relative_wire, .kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP, .relative_group_offset = 2),
    LOCATION_ROW(relative_max, .kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP, .relative_group_offset = UINT64_MAX),
    LOCATION_ROW(range_wire, .kind = MOQ_LOCATION_FILTER_OBJECT_END,
        .start_group = 1, .start_object = 2, .end_group = 3, .end_object = 3),
    LOCATION_ROW(range_max, .kind = MOQ_LOCATION_FILTER_OBJECT_END,
        .start_group = UINT64_MAX, .start_object = UINT64_MAX,
        .end_group = UINT64_MAX, .end_object = UINT64_MAX),
    TIMEOUT_ROW(opposite_wire, 6, UINT64_MAX, 0),
    TIMEOUT_ROW(both_max, 6, UINT64_MAX, UINT64_MAX),
};
#undef TIMEOUT_ROW
#undef LOCATION_ROW

static void check_options(pair_t *p, const moq_event_t *ev, const row_t *row)
{
    moq_subscription_update_options_t out;
    CHECK(ev->kind == MOQ_EVENT_PUBLISH_UPDATED);
    CHECK(moq_publication_eq(ev->u.publish_updated.pub, p->pp));
    CHECK(!ev->u.publish_updated.has_filter && !ev->u.publish_updated.has_delivery_timeout);
    CHECK(ev->u.publish_updated.delivery_timeout_us == 0 &&
        ev->u.publish_updated.start_group == 0 && ev->u.publish_updated.start_object == 0 &&
        ev->u.publish_updated.end_group == 0);
    bool got = options_copy(p->sv, ev, &out);
    CHECK(got);
    if (got) {
        moq_subscription_update_options_t expected = row->options;
        expected.resolution.struct_size = sizeof(expected.resolution);
        if (expected.flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION) {
            expected.resolution.flags = MOQ_UPDATE_RESOLUTION_LOCATION_REPLACEMENT;
            expected.resolution.location = expected.location;
            if (expected.location.kind == MOQ_LOCATION_FILTER_RELATIVE_GROUP) {
                expected.resolution.location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
                expected.resolution.location.relative_group_offset = 0;
            }
        }
        CHECK(memcmp(&out, &expected, sizeof(out)) == 0);
    }
    CHECK(moq_session_borrow_valid(p->sv, ev->borrow_epoch));
}

static void ingress(size_t which, bool split, bool defer, bool reject, bool fin)
{
    const row_t *row = &rows[which];
    pair_t p;
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, defer));
    moq_location_filter_cfg_t initial, after;
    REQUIRE(moq_session_publication_filter(p.sv, p.pp, &initial) == MOQ_OK);
    size_t first = split ? row->len - 1 : row->len;
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
        row->wire, first, fin && !split, 3) == MOQ_OK);
    if (split) {
        CHECK(p.sv->event_head == p.sv->event_tail && p.sv->action_head == p.sv->action_tail);
        REQUIRE(moq_session_publication_filter(p.sv, p.pp, &after) == MOQ_OK);
        CHECK(memcmp(&initial, &after, sizeof(initial)) == 0);
        REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
            row->wire + first, row->len - first, fin, 3) == MOQ_OK);
    }
    if (defer) {
        uint64_t id = UINT64_MAX;
        bool selected = false;
        REQUIRE(moq_session_publication_owed_update(p.sv, p.pp, &id, &selected, &after) == MOQ_OK);
        CHECK(id == 0 && selected == ((row->options.flags & 1) != 0));
        if (selected) CHECK(memcmp(&after, &row->options.location, sizeof(after)) == 0);
        REQUIRE(moq_session_publication_filter(p.sv, p.pp, &after) == MOQ_OK);
        CHECK(memcmp(&initial, &after, sizeof(initial)) == 0);
        CHECK(p.sv->action_head == p.sv->action_tail);
    }
    REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    check_options(&p, &ev, row);
    CHECK(ev.u.publish_updated.answer_owed == defer);
    CHECK(ev.u.publish_updated.update_count == 1 && ev.u.publish_updated.update_request_id == 0);
    moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
    if (defer) {
        if (reject) {
            moq_reject_subscription_update_cfg_t cfg; moq_reject_subscription_update_cfg_init(&cfg);
            cfg.update_request_id = 0; cfg.error_code = (moq_request_error_t)5;
            REQUIRE(moq_session_reject_publication_update(p.sv, p.pp, &cfg, 4) == MOQ_OK);
            CHECK(moq_session_publication_owed_update(p.sv, p.pp, NULL, NULL, NULL) == MOQ_ERR_STALE_HANDLE);
            CHECK(moq_session_state(p.sv) == MOQ_SESS_ESTABLISHED);
            goto done;
        }
        moq_accept_subscription_update_cfg_t cfg; moq_accept_subscription_update_cfg_init(&cfg);
        cfg.update_request_id = 0;
        REQUIRE(moq_session_accept_publication_update(p.sv, p.pp, &cfg, 4) == MOQ_OK);
        CHECK(moq_session_publication_owed_update(p.sv, p.pp, NULL, NULL, NULL) == MOQ_ERR_WRONG_STATE);
    }
    CHECK(action_bytes(p.sv, p.pe->rs.ref, ack_wire, sizeof(ack_wire)));
    CHECK(p.sv->action_head == p.sv->action_tail && p.sv->event_head == p.sv->event_tail);
    REQUIRE(moq_session_publication_filter(p.sv, p.pp, &after) == MOQ_OK);
    CHECK(memcmp(&after, (row->options.flags & 1) ? &row->options.location : &initial, sizeof(after)) == 0);
done:
    moq_event_cleanup(&ev);
    finish(&p);
}

static void outbound(size_t which)
{
    const row_t *row = &rows[which];
    pair_t p;
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, false));
    if (row->options.flags & 1) REQUIRE(input_layout_ready());
    moq_publish_initial_parameters_t before, after;
    REQUIRE(moq_session_get_publish_initial_parameters(p.c, p.cp, &before, sizeof(before)) == MOQ_OK);
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(u);
    u.delivery_timeout_flags = row->options.flags >> 1;
    u.object_delivery_timeout_ms = row->options.object_delivery_timeout_ms;
    u.subgroup_delivery_timeout_ms = row->options.subgroup_delivery_timeout_ms;
    moq_location_filter_cfg_t loc = row->options.location;
    if (row->options.flags & 1) u.location = &loc;
    REQUIRE(update(&p, &u, 3) == MOQ_OK);
    memset(&u, 0xa5, sizeof(u)); memset(&loc, 0xa5, sizeof(loc));
    REQUIRE(action_bytes(p.c, p.ce->rs.ref, row->wire, row->len));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
        row->wire, row->len, false, 3) == MOQ_OK);
    REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    check_options(&p, &ev, row);
    moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
    REQUIRE(action_bytes(p.sv, p.pe->rs.ref, ack_wire, sizeof(ack_wire)));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.c, p.ce->rs.ref,
        ack_wire, sizeof(ack_wire), false, 4) == MOQ_OK);
    REQUIRE(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    CHECK(!p.ce->update_pending);
    CHECK(p.ce->dt_sub_has_object == ((row->options.flags & 2) != 0));
    CHECK(p.ce->dt_sub_has_subgroup == ((row->options.flags & 4) != 0));
    if (row->options.flags & 2) CHECK(p.ce->dt_sub_object_ms == row->options.object_delivery_timeout_ms);
    if (row->options.flags & 4) CHECK(p.ce->dt_sub_subgroup_ms == row->options.subgroup_delivery_timeout_ms);
    if (row->options.flags & 1) {
        moq_location_filter_cfg_t effective;
        REQUIRE(moq_session_publication_filter(p.c, p.cp, &effective) == MOQ_OK);
        CHECK(memcmp(&effective, &row->options.location, sizeof(effective)) == 0);
    }
    REQUIRE(moq_session_get_publish_initial_parameters(p.c, p.cp, &after, sizeof(after)) == MOQ_OK);
    CHECK(memcmp(&before, &after, sizeof(before)) == 0);
done:
    moq_event_cleanup(&ev);
    finish(&p);
}

static void no_effect_refusal(moq_version_t version, unsigned which)
{
    pair_t p;
    REQUIRE(mk(&p, version, false));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(u);
    u.old.has_forward = true;
    moq_result_t want = MOQ_ERR_INVAL;
    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
    switch (which) {
    case 0: u.old.has_delivery_timeout = true; want = MOQ_ERR_UNSUPPORTED; break;
    case 1: u.old.has_delivery_timeout = true; u.old.delivery_timeout_us = UINT64_MAX;
            want = MOQ_ERR_UNSUPPORTED; break;
    case 2: u.delivery_timeout_flags = 4; break;
    case 3: u.reserved = 1; break;
    case 4:
        /* Draft-16: its single DELIVERY_TIMEOUT is never 0 (9.2.2.2).
         * Draft-18: typed timeouts beside the generic one state two values. */
        u.delivery_timeout_flags = 1;
        if (version == MOQ_VERSION_DRAFT_18) {
            u.old.has_delivery_timeout = true;
            u.old.delivery_timeout_us = 123000;
        }
        break;
    case 5:
        REQUIRE(input_layout_ready());
        u.location = &loc;
        want = version == MOQ_VERSION_DRAFT_22 ? MOQ_ERR_INVAL : MOQ_ERR_UNSUPPORTED;
        loc.kind = MOQ_LOCATION_FILTER_OBJECT_END;
        loc.start_group = 2; loc.end_group = 1;
        break;
    case 6:
        REQUIRE(input_layout_ready());
        u.fill = &fill;
        want = version == MOQ_VERSION_DRAFT_22 ? MOQ_ERR_ABI_MISMATCH : MOQ_ERR_UNSUPPORTED;
        fill.struct_size = 4;
        break;
    case 7: u.old.auth_token_count = 1; break;
    default: REQUIRE(false);
    }
    moq_pub_entry_t before = *p.ce;
    uint64_t epoch = p.c->borrow_epoch, now = p.c->last_now_us;
    size_t ah = p.c->action_head, at = p.c->action_tail;
    size_t eh = p.c->event_head, et = p.c->event_tail;
    size_t send = p.c->send_len, live = p.c->fills.live;
    CHECK(update(&p, &u, 3) == want);
    CHECK(memcmp(&before, p.ce, sizeof(before)) == 0);
    CHECK(p.c->borrow_epoch == epoch && p.c->last_now_us == now);
    CHECK(p.c->action_head == ah && p.c->action_tail == at && p.c->send_len == send);
    CHECK(p.c->event_head == eh && p.c->event_tail == et && p.c->fills.live == live);
done:
    finish(&p);
}

static void legacy(moq_version_t version)
{
    pair_t p;
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    REQUIRE(mk(&p, version, false));
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(old_update_t);
    CHECK(update(&p, &u, 3) == MOQ_ERR_INVAL);
    u.old.has_forward = true; u.old.forward = false;
    u.old.has_subscriber_priority = true; u.old.subscriber_priority = 7;
    u.old.has_delivery_timeout = true; u.old.delivery_timeout_us = 123000;
    REQUIRE(update(&p, &u, 4) == MOQ_OK && pump(&p));
    REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    CHECK(ev.u.publish_updated.has_forward && !ev.u.publish_updated.forward);
    CHECK(ev.u.publish_updated.has_subscriber_priority && ev.u.publish_updated.subscriber_priority == 7);
    CHECK(ev.u.publish_updated.has_delivery_timeout && ev.u.publish_updated.delivery_timeout_us == 123000);
    CHECK(options_pointer(&ev) == NULL);
    moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
    REQUIRE(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    CHECK(!p.ce->update_pending);
done:
    moq_event_cleanup(&ev);
    finish(&p);
}

static void initializers(void)
{
    union { moq_publication_update_cfg_t align; uint8_t bytes[sizeof(expected_update_t) + 16]; } mem;
    void (*legacy_init)(moq_publication_update_cfg_t *) = moq_publication_update_cfg_init;
    memset(&mem, 0xa5, sizeof(mem));
    legacy_init(&mem.align);
    CHECK(mem.align.struct_size == sizeof(old_update_t));
    for (size_t i = sizeof(uint32_t); i < sizeof(old_update_t); i++) CHECK(mem.bytes[i] == 0);
    for (size_t i = sizeof(old_update_t); i < sizeof(mem); i++) CHECK(mem.bytes[i] == 0xa5);
    memset(&mem, 0xa5, sizeof(mem));
    moq_publication_update_cfg_init(&mem.align);
    CHECK(mem.align.struct_size == sizeof(expected_update_t));
    for (size_t i = sizeof(uint32_t); i < sizeof(expected_update_t); i++) CHECK(mem.bytes[i] == 0);
    for (size_t i = sizeof(expected_update_t); i < sizeof(mem); i++) CHECK(mem.bytes[i] == 0xa5);
    old_update_t *exact = malloc(sizeof(*exact));
    CHECK(exact != NULL);
    if (exact) {
        memset(exact, 0xa5, sizeof(old_update_t));
        legacy_init((moq_publication_update_cfg_t *)exact);
        CHECK(exact->struct_size == sizeof(old_update_t));
        free(exact);
    }
    for (size_t cap = 0; cap <= sizeof(mem); cap++) {
        memset(&mem, 0xa5, sizeof(mem));
        moq_publication_update_cfg_init_sized(&mem.align, cap);
        size_t n = cap < sizeof(uint32_t) ? 0 :
            cap < sizeof(expected_update_t) ? cap : sizeof(expected_update_t);
        if (n) CHECK(mem.align.struct_size == n);
        for (size_t i = sizeof(uint32_t); i < n; i++) CHECK(mem.bytes[i] == 0);
        for (size_t i = n; i < sizeof(mem); i++) CHECK(mem.bytes[i] == 0xa5);
    }
}

static void truncation(size_t cap)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, false));
    REQUIRE(input_layout_ready());
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)cap;
    u.old.has_subscriber_priority = true; u.old.subscriber_priority = 3;
    if (cap < offsetof(expected_update_t, fill))
        u.location = (const moq_location_filter_cfg_t *)(uintptr_t)1;
    else if (cap < offsetof(expected_update_t, delivery_timeout_flags))
        u.fill = (const moq_fill_cfg_t *)(uintptr_t)1;
    else {
        u.delivery_timeout_flags = UINT32_MAX; u.reserved = UINT32_MAX;
        u.object_delivery_timeout_ms = u.subgroup_delivery_timeout_ms = UINT64_MAX;
    }
    REQUIRE(update(&p, &u, 3) == MOQ_OK);
    CHECK(action_bytes(p.c, p.ce->rs.ref, priority_wire, sizeof(priority_wire)));
    CHECK(p.c->fills.live == 0);
done:
    finish(&p);
}

static void publisher_origin(bool forbidden)
{
    static const uint8_t forward[] = { 2, 0, 4, 3, 1, 16, 0 };
    /* Section3.3.2 Range Filters (0x25-0x29), not LOCATION_FILTER. */
    static const uint8_t subgroup_filter[] = { 2, 0, 4, 3, 1, 37, 0 };
    pair_t p;
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, false));
    const uint8_t *wire = forbidden ? subgroup_filter : forward;
    REQUIRE(moq_session_on_bidi_stream_bytes(p.c, p.ce->rs.ref,
        wire, sizeof(forward), false, 3) == MOQ_OK);
    if (forbidden) {
        CHECK(moq_session_state(p.c) == MOQ_SESS_CLOSED);
        CHECK(!take(p.c, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    } else {
        REQUIRE(take(p.c, MOQ_EVENT_PUBLISH_UPDATED, &ev));
        CHECK(ev.u.publish_updated.has_forward && !ev.u.publish_updated.forward);
        CHECK(!ev.u.publish_updated.answer_owed && ev.u.publish_updated.update_count == 1);
        moq_subscription_update_options_t opts;
        bool got = options_copy(p.c, &ev, &opts);
        CHECK(got);
        if (got) CHECK(opts.struct_size == sizeof(opts) && opts.flags == 0 && opts.initiating_request_id == 3);
        CHECK(action_bytes(p.c, p.ce->rs.ref, ack_wire, sizeof(ack_wire)));
        CHECK(!p.ce->send_allowed && moq_session_state(p.c) == MOQ_SESS_ESTABLISHED);
    }
done:
    moq_event_cleanup(&ev);
    finish(&p);
}

static void combined(void)
{
    /* OBJECT timeout, USE_VALUE authorization, SUBGROUP timeout, FORWARD,
     * priority, relative location and meaningful NEW_GROUP_REQUEST zero. */
    static const uint8_t wire[] = {
        2, 0, 28, 0, 7, 2, 0, 1, 3, 3, 9, 'a',
        3, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        10, 0, 16, 7, 1, 1, 2, 17, 0 };
    pair_t p;
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, true));
    REQUIRE(input_layout_ready());
    uint8_t token_value = 'a';
    moq_auth_token_t token = { .token_type = 9, .token_value = { &token_value, 1 } };
    moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP; loc.relative_group_offset = 2;
    expected_update_t u; memset(&u, 0, sizeof(u));
    u.old.struct_size = (uint32_t)sizeof(u);
    u.old.has_subscriber_priority = true; u.old.subscriber_priority = 7;
    u.old.has_forward = true; u.old.forward = false;
    u.old.has_new_group_request = true; u.old.new_group_request = 0;
    u.old.auth_tokens = &token; u.old.auth_token_count = 1;
    u.location = &loc; u.delivery_timeout_flags = 3;
    u.subgroup_delivery_timeout_ms = UINT64_MAX;
    REQUIRE(update(&p, &u, 3) == MOQ_OK);
    memset(&u, 0xa5, sizeof(u)); memset(&loc, 0xa5, sizeof(loc));
    memset(&token, 0xa5, sizeof(token)); token_value = 'z';
    REQUIRE(action_bytes(p.c, p.ce->rs.ref, wire, sizeof(wire)));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
        wire, sizeof(wire), false, 3) == MOQ_OK);
    CHECK(p.pe->send_allowed && p.pe->subscriber_priority == 128);
    CHECK(!p.pe->dt_sub_has_object && !p.pe->dt_sub_has_subgroup);
    uint64_t id = UINT64_MAX;
    bool selected = false;
    REQUIRE(moq_session_publication_owed_update(p.sv, p.pp, &id, &selected, &loc) == MOQ_OK);
    CHECK(id == 0 && selected && loc.kind == MOQ_LOCATION_FILTER_RELATIVE_GROUP &&
        loc.relative_group_offset == 2);
    REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    CHECK(ev.u.publish_updated.has_forward && !ev.u.publish_updated.forward);
    CHECK(ev.u.publish_updated.has_subscriber_priority && ev.u.publish_updated.subscriber_priority == 7);
    CHECK(ev.u.publish_updated.has_new_group_request && ev.u.publish_updated.new_group_request == 0);
    REQUIRE(ev.u.publish_updated.token_count == 1 && ev.u.publish_updated.tokens != NULL);
    CHECK(ev.u.publish_updated.tokens[0].token_type == 9);
    REQUIRE(ev.u.publish_updated.tokens[0].token_value.len == 1 &&
        ev.u.publish_updated.tokens[0].token_value.data != NULL);
    CHECK(ev.u.publish_updated.tokens[0].token_value.data[0] == 'a');
    moq_subscription_update_options_t opts;
    REQUIRE(options_copy(p.sv, &ev, &opts));
    CHECK(opts.struct_size == sizeof(opts) && opts.flags == 7 && opts.initiating_request_id == id);
    CHECK(opts.location.kind == MOQ_LOCATION_FILTER_RELATIVE_GROUP &&
        opts.location.relative_group_offset == 2);
    CHECK(opts.object_delivery_timeout_ms == 0 && opts.subgroup_delivery_timeout_ms == UINT64_MAX);
    CHECK(moq_session_borrow_valid(p.sv, ev.borrow_epoch));
    moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
    moq_accept_subscription_update_cfg_t answer; moq_accept_subscription_update_cfg_init(&answer);
    answer.update_request_id = id;
    REQUIRE(moq_session_accept_publication_update(p.sv, p.pp, &answer, 4) == MOQ_OK);
    REQUIRE(action_bytes(p.sv, p.pe->rs.ref, ack_wire, sizeof(ack_wire)));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.c, p.ce->rs.ref,
        ack_wire, sizeof(ack_wire), false, 4) == MOQ_OK);
    REQUIRE(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
    CHECK(!p.ce->send_allowed && !p.pe->send_allowed);
    CHECK(p.ce->subscriber_priority == 7 && p.pe->subscriber_priority == 7);
    CHECK(p.ce->dt_sub_has_object && p.pe->dt_sub_has_object &&
        p.ce->dt_sub_object_ms == 0 && p.pe->dt_sub_object_ms == 0);
    CHECK(p.ce->dt_sub_has_subgroup && p.pe->dt_sub_has_subgroup &&
        p.ce->dt_sub_subgroup_ms == UINT64_MAX && p.pe->dt_sub_subgroup_ms == UINT64_MAX);
done:
    moq_event_cleanup(&ev);
    finish(&p);
}

static void distinct_notifications(void)
{
    static const uint8_t second[] = {
        2, 0, 12, 2, 1, 6, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
    pair_t p;
    moq_event_t a, b; memset(&a, 0, sizeof(a)); memset(&b, 0, sizeof(b));
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, false));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
        object_zero, sizeof(object_zero), false, 3) == MOQ_OK);
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
        second, sizeof(second), false, 4) == MOQ_OK);
    REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &a));
    moq_subscription_update_options_t first, next;
    REQUIRE(options_copy(p.sv, &a, &first));
    const moq_subscription_update_options_t *first_ptr = options_pointer(&a);
    REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &b));
    REQUIRE(options_copy(p.sv, &b, &next));
    CHECK(first_ptr != options_pointer(&b));
    CHECK(first.flags == 2 && first.initiating_request_id == 0 &&
        first.object_delivery_timeout_ms == 0 && first.subgroup_delivery_timeout_ms == 0);
    CHECK(next.flags == 4 && next.initiating_request_id == 2 &&
        next.object_delivery_timeout_ms == 0 && next.subgroup_delivery_timeout_ms == UINT64_MAX);
    REQUIRE(moq_session_borrow_valid(p.sv, a.borrow_epoch));
    CHECK(memcmp(first_ptr, &first, sizeof(first)) == 0);
    CHECK(p.pe->dt_sub_has_object && p.pe->dt_sub_has_subgroup);
    CHECK(action_bytes(p.sv, p.pe->rs.ref, ack_wire, sizeof(ack_wire)));
    CHECK(action_bytes(p.sv, p.pe->rs.ref, ack_wire, sizeof(ack_wire)));
    CHECK(p.sv->event_head == p.sv->event_tail && p.sv->action_head == p.sv->action_tail);
    uint64_t epoch = a.borrow_epoch;
    moq_event_cleanup(&a); memset(&a, 0, sizeof(a));
    moq_event_cleanup(&b); memset(&b, 0, sizeof(b));
    REQUIRE(moq_session_tick(p.sv, 5) == MOQ_OK);
    CHECK(!moq_session_borrow_valid(p.sv, epoch));
    CHECK(first.flags == 2 && next.flags == 4);
done:
    moq_event_cleanup(&a); moq_event_cleanup(&b);
    finish(&p);
}

static void sticky_timeouts(void)
{
    static const uint8_t first[] = { 2, 0, 4, 0, 1, 2, 5 };
    static const uint8_t second[] = {
        2, 0, 12, 2, 1, 6, 255, 255, 255, 255, 255, 255, 255, 255, 255 };
    static const uint8_t third[] = { 2, 0, 2, 4, 0 };
    const uint8_t *wire[] = { first, second, third };
    const size_t len[] = { sizeof(first), sizeof(second), sizeof(third) };
    pair_t p;
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, false));
    moq_publish_initial_parameters_t initial, after;
    REQUIRE(moq_session_get_publish_initial_parameters(p.c, p.cp, &initial, sizeof(initial)) == MOQ_OK);
    for (size_t i = 0; i < 3; i++) {
        expected_update_t u; memset(&u, 0, sizeof(u));
        u.old.struct_size = (uint32_t)sizeof(u);
        u.delivery_timeout_flags = i == 0 ? 1 : i == 1 ? 2 : 0;
        u.object_delivery_timeout_ms = i == 0 ? 5 : UINT64_MAX;
        u.subgroup_delivery_timeout_ms = UINT64_MAX;
        REQUIRE(update(&p, &u, 3 + i * 2) == MOQ_OK);
        REQUIRE(action_bytes(p.c, p.ce->rs.ref, wire[i], len[i]));
        REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
            wire[i], len[i], false, 3 + i * 2) == MOQ_OK);
        REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
        moq_subscription_update_options_t opts;
        REQUIRE(options_copy(p.sv, &ev, &opts));
        CHECK(opts.flags == u.delivery_timeout_flags * 2 && opts.initiating_request_id == i * 2);
        CHECK(opts.object_delivery_timeout_ms == (i == 0 ? 5u : 0u));
        CHECK(opts.subgroup_delivery_timeout_ms == (i == 1 ? UINT64_MAX : 0));
        moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
        REQUIRE(action_bytes(p.sv, p.pe->rs.ref, ack_wire, sizeof(ack_wire)));
        REQUIRE(moq_session_on_bidi_stream_bytes(p.c, p.ce->rs.ref,
            ack_wire, sizeof(ack_wire), false, 4 + i * 2) == MOQ_OK);
        REQUIRE(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
        moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
        CHECK(p.ce->dt_sub_has_object && p.pe->dt_sub_has_object &&
            p.ce->dt_sub_object_ms == 5 && p.pe->dt_sub_object_ms == 5);
        CHECK(p.ce->dt_sub_has_subgroup == (i > 0) && p.pe->dt_sub_has_subgroup == (i > 0));
        if (i) CHECK(p.ce->dt_sub_subgroup_ms == UINT64_MAX && p.pe->dt_sub_subgroup_ms == UINT64_MAX);
    }
    REQUIRE(moq_session_get_publish_initial_parameters(p.c, p.cp, &after, sizeof(after)) == MOQ_OK);
    CHECK(memcmp(&initial, &after, sizeof(initial)) == 0);
done:
    moq_event_cleanup(&ev);
    finish(&p);
}

static bool seed_largest(pair_t *p)
{
    moq_subgroup_cfg_t sg; moq_subgroup_cfg_init(&sg);
    sg.group_id = 9; sg.publisher_priority = 1;
    moq_subgroup_handle_t h;
    if (moq_session_open_pub_subgroup(p->sv, p->pp, &sg, 2, &h) != MOQ_OK) return false;
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&p->alloc, (const uint8_t *)"x", 1, &payload) != MOQ_OK) return false;
    moq_result_t rc = moq_session_write_object(p->sv, h, 7, payload, 2);
    moq_rcbuf_decref(payload);
    if (rc != MOQ_OK || moq_session_close_subgroup(p->sv, h, 2) != MOQ_OK || !pump(p))
        return false;
    drain(p->c); drain(p->sv);
    return true;
}

static void independent_fills(bool defer)
{
    static const uint8_t first[] = { 2, 0, 13, 0, 3, 32, 3, 1, 4, 1, 2, 2, 3, 2, 1, 0 };
    static const uint8_t second[] = { 2, 0, 5, 2, 1, 35, 1, 0 };
    static const uint8_t third[] = { 2, 0, 6, 4, 2, 32, 9, 1, 0 };
    static const uint8_t ack[] = { 7, 0, 4, 1, 9, 9, 7 };
    const uint8_t *wire[] = { first, second, third };
    const size_t len[] = { sizeof(first), sizeof(second), sizeof(third) };
    pair_t p;
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, defer));
    REQUIRE(input_layout_ready());
    REQUIRE(seed_largest(&p));
    moq_fill_t children[2] = { MOQ_FILL_INVALID, MOQ_FILL_INVALID };
    for (size_t i = 0; i < 3; i++) {
        moq_location_filter_cfg_t loc; moq_location_filter_cfg_init(&loc);
        loc.kind = i == 2 ? MOQ_LOCATION_FILTER_NONE : MOQ_LOCATION_FILTER_OBJECT_END;
        loc.start_group = 1; loc.start_object = 2; loc.end_group = 3; loc.end_object = 3;
        moq_fill_cfg_t fill; moq_fill_cfg_init(&fill);
        expected_update_t u; memset(&u, 0, sizeof(u));
        u.old.struct_size = (uint32_t)sizeof(u);
        u.old.has_subscriber_priority = i != 1; u.old.subscriber_priority = i == 2 ? 9 : 3;
        u.location = i == 1 ? NULL : &loc; u.fill = i == 2 ? NULL : &fill;
        REQUIRE(update(&p, &u, 3 + i * 2) == MOQ_OK);
        memset(&loc, 0xa5, sizeof(loc)); memset(&fill, 0xa5, sizeof(fill));
        REQUIRE(action_bytes(p.c, p.ce->rs.ref, wire[i], len[i]));
        if (i < 2) {
            REQUIRE(take(p.c, MOQ_EVENT_FILL_CREATED, &ev));
            children[i] = ev.u.fill_created.fill;
            CHECK(ev.u.fill_created.initiating_request_id == i * 2);
            if (i) CHECK(!moq_fill_eq(children[0], children[1]));
            moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
        }
        REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
            wire[i], len[i], false, 3 + i * 2) == MOQ_OK);
        REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
        moq_subscription_update_options_t opts;
        REQUIRE(options_copy(p.sv, &ev, &opts));
        CHECK(opts.flags == (i == 1 ? 0u : 1u) && opts.initiating_request_id == i * 2);
        CHECK(ev.u.publish_updated.fill_requested == (i < 2));
        CHECK(ev.u.publish_updated.answer_owed == defer);
        moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
        if (defer) {
            CHECK(p.sv->fills.live == (i < 2 ? i : 2));
            moq_accept_subscription_update_cfg_t ac; moq_accept_subscription_update_cfg_init(&ac);
            ac.update_request_id = i * 2;
            REQUIRE(moq_session_accept_publication_update(p.sv, p.pp, &ac, 4 + i * 2) == MOQ_OK);
        }
        if (i < 2) {
            REQUIRE(take(p.sv, MOQ_EVENT_FILL_REQUEST, &ev));
            CHECK(ev.u.fill_request.initiating_request_id == i * 2);
            CHECK(ev.u.fill_request.subscriber_priority == 3);
            CHECK(ev.u.fill_request.start_group == 1 && ev.u.fill_request.start_object == 2 &&
                ev.u.fill_request.end_group == 3 && ev.u.fill_request.end_object == 3);
            moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
        }
        REQUIRE(action_bytes(p.sv, p.pe->rs.ref, ack, sizeof(ack)));
        REQUIRE(moq_session_on_bidi_stream_bytes(p.c, p.ce->rs.ref,
            ack, sizeof(ack), false, 4 + i * 2) == MOQ_OK);
        REQUIRE(take(p.c, MOQ_EVENT_PUBLICATION_UPDATE_OK, &ev));
        moq_event_cleanup(&ev); memset(&ev, 0, sizeof(ev));
        drain(p.c);
    }
    CHECK(p.ce->subscriber_priority == 9 && p.pe->subscriber_priority == 9);
    CHECK(p.ce->filter_type == MOQ_SUBSCRIBE_FILTER_NONE && p.pe->filter_type == MOQ_SUBSCRIBE_FILTER_NONE);
    CHECK(p.c->fills.live == 2 && p.sv->fills.live == 2);
    for (size_t i = 0; i < 2; i++) {
        moq_fill_ref_t ref;
        moq_fill_entry_t info;
        REQUIRE(moq_fill_find(&p.c->fills, i * 2, &ref) == MOQ_OK);
        REQUIRE(moq_fill_read(&p.c->fills, ref, &info) == MOQ_OK);
        CHECK(info.initiating_id == i * 2 && info.delivery.subscriber_priority == 3);
        CHECK(info.delivery.start_group == 1 && info.delivery.start_object == 2 &&
            info.delivery.end_group == 3 && info.delivery.end_object == 3);
    }
    REQUIRE(moq_session_cancel_fill(p.c, children[0], 9) == MOQ_OK);
    moq_fill_ref_t remaining;
    moq_fill_entry_t info;
    REQUIRE(moq_fill_find(&p.c->fills, 2, &remaining) == MOQ_OK);
    REQUIRE(moq_fill_read(&p.c->fills, remaining, &info) == MOQ_OK);
    CHECK(!info.cancel_requested);
    REQUIRE(moq_session_cancel_fill(p.c, children[1], 10) == MOQ_OK);
    CHECK(p.ce->state == MOQ_PUB_ESTABLISHED && p.pe->state == MOQ_PUB_ESTABLISHED);
done:
    moq_event_cleanup(&ev);
    finish(&p);
}

static void old_event_prefix(void)
{
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, false));
    REQUIRE(moq_session_on_bidi_stream_bytes(p.sv, p.pe->rs.ref,
        object_zero, sizeof(object_zero), false, 3) == MOQ_OK);
    union { moq_event_t align; uint8_t bytes[sizeof(moq_event_t) + 16]; } mem;
    memset(&mem, 0xa5, sizeof(mem));
    size_t old_size = offsetof(moq_event_t, u) + sizeof(old_detail_t), count = 0;
    REQUIRE(moq_session_poll_events_ex(p.sv, mem.bytes, 1, old_size, &count) == MOQ_OK && count == 1);
    CHECK(mem.align.kind == MOQ_EVENT_PUBLISH_UPDATED);
    CHECK(mem.align.detail_size == sizeof(moq_publish_updated_event_t));
    CHECK(!mem.align.u.publish_updated.has_delivery_timeout);
    for (size_t i = old_size; i < sizeof(mem); i++) CHECK(mem.bytes[i] == 0xa5);
    CHECK(action_bytes(p.sv, p.pe->rs.ref, ack_wire, sizeof(ack_wire)));
done:
    finish(&p);
}

static void pointer_boundaries(bool fill_only)
{
    static const uint8_t fill_wire[] = { 2, 0, 5, 0, 1, 35, 1, 0 };
    pair_t p;
    REQUIRE(mk(&p, MOQ_VERSION_DRAFT_22, false));
    REQUIRE(input_layout_ready());
    moq_location_filter_cfg_t loc = rows[10].options.location;
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
    REQUIRE(update(&p, &u, 3) == MOQ_OK);
    CHECK(action_bytes(p.c, p.ce->rs.ref, fill_only ? fill_wire : range_wire,
        fill_only ? sizeof(fill_wire) : sizeof(range_wire)));
    CHECK(p.c->fills.live == (fill_only ? 1u : 0u));
done:
    finish(&p);
}

static void exact_old_caller(moq_version_t version)
{
    pair_t p;
    moq_event_t ev; memset(&ev, 0, sizeof(ev));
    old_update_t *cfg = NULL;
    REQUIRE(mk(&p, version, false));
    cfg = malloc(sizeof(*cfg));
    REQUIRE(cfg != NULL);
    void (*init)(moq_publication_update_cfg_t *) = moq_publication_update_cfg_init;
    init((moq_publication_update_cfg_t *)cfg);
    CHECK(cfg->struct_size == sizeof(old_update_t));
    cfg->has_forward = true; cfg->forward = false;
    REQUIRE(moq_session_update_publication(p.c, p.cp,
        (const moq_publication_update_cfg_t *)cfg, 3) == MOQ_OK);
    free(cfg); cfg = NULL;
    REQUIRE(pump(&p));
    REQUIRE(take(p.sv, MOQ_EVENT_PUBLISH_UPDATED, &ev));
    CHECK(ev.u.publish_updated.has_forward && !ev.u.publish_updated.forward);
    CHECK(options_pointer(&ev) == NULL);
done:
    free(cfg);
    moq_event_cleanup(&ev);
    finish(&p);
}

int main(void)
{
    RUN(legacy(MOQ_VERSION_DRAFT_16));
    RUN(legacy(MOQ_VERSION_DRAFT_18));
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        RUN(outbound(i));
        RUN(ingress(i, false, false, false, false));
        RUN(ingress(i, true, false, false, false));
        RUN(ingress(i, false, true, false, false));
    }
    RUN(ingress(10, true, true, false, true));
    RUN(ingress(10, false, true, true, true));
    for (unsigned i = 0; i < 8; i++) {
        if (i != 4) RUN(no_effect_refusal(MOQ_VERSION_DRAFT_22, i));
        if (i > 1) {
            RUN(no_effect_refusal(MOQ_VERSION_DRAFT_16, i));
            RUN(no_effect_refusal(MOQ_VERSION_DRAFT_18, i));
        }
    }
    RUN(initializers());
    for (size_t i = sizeof(old_update_t); i < sizeof(expected_update_t); i++)
        RUN(truncation(i));
    RUN(publisher_origin(false));
    RUN(publisher_origin(true));
    RUN(combined());
    RUN(distinct_notifications());
    RUN(sticky_timeouts());
    RUN(independent_fills(false));
    RUN(independent_fills(true));
    RUN(old_event_prefix());
    RUN(pointer_boundaries(false));
    RUN(pointer_boundaries(true));
    RUN(exact_old_caller(MOQ_VERSION_DRAFT_16));
    RUN(exact_old_caller(MOQ_VERSION_DRAFT_18));
    printf("publication_typed_update: %u cases, %u checks, %u failures\n", cases, checks, failures);
    return failures ? 1 : 0;
}
#endif
