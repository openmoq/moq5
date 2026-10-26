#include <moq/publisher.h>
#include <moq/rcbuf.h>
#include <moq/sim.h>
#include "test_support.h"
#include "test_alloc_support.h"
#include <stdio.h>
#include <string.h>

static int failures;

typedef struct {
    test_alloc_state_t allocations;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_publisher_t *publisher;
    moq_pub_track_t *track;
    moq_bytes_t ns;
    size_t alloc_calls, fail_at, last_sizes[4], admission_bytes;
    moq_stream_ref_t publication_stream;
} rig_t;

static void trace_publication(void *ctx, const moq_sim_trace_record_t *record)
{
    rig_t *r = ctx;
    if (record->struct_size >= offsetof(moq_sim_trace_record_t, detail_bytes) +
                               sizeof(record->detail_bytes) &&
        record->kind == MOQ_SIM_TRACE_ACTION &&
        record->action_kind == MOQ_ACTION_OPEN_BIDI_STREAM &&
        record->detail_bytes.len && record->detail_bytes.data[0] == 0x1d)
        r->publication_stream = record->stream_ref;
}

static void *rig_alloc(size_t size, void *ctx)
{
    rig_t *r = ctx;
    r->last_sizes[r->alloc_calls++ % 4] = size;
    if (r->fail_at == r->alloc_calls) return NULL;
    return test_alloc(size, &r->allocations);
}

static void *rig_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    rig_t *r = ctx;
    return test_realloc(ptr, old_size, new_size, &r->allocations);
}

static void rig_free(void *ptr, size_t size, void *ctx)
{
    rig_t *r = ctx;
    test_free(ptr, size, &r->allocations);
}

static uint64_t now(const rig_t *r) { return moq_simpair_now_us(r->pair); }

static void discard_events(moq_session_t *s)
{
    moq_event_t e;
    while (moq_session_poll_events(s, &e, 1)) moq_event_cleanup(&e);
}

static void rig_open_cap(rig_t *r, const moq_pub_cfg_t *pcfg, moq_version_t version,
                          uint32_t actions, uint32_t subscriptions)
{
    memset(r, 0, sizeof(*r));
    r->alloc = (moq_alloc_t){.ctx = r, .alloc = rig_alloc,
                            .realloc = rig_realloc, .free = rig_free};
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &r->alloc;
    cfg.version = version;
    cfg.max_actions = actions;
    cfg.server_max_subscriptions = subscriptions;
    cfg.trace_fn = trace_publication;
    cfg.trace_ctx = r;
    cfg.seed = 7;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 32;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 32;
    MOQ_TEST_CHECK(moq_simpair_create(&cfg, &r->pair) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_start(r->pair) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r->pair, 8, NULL) == MOQ_OK);
    discard_events(moq_simpair_client(r->pair));
    discard_events(moq_simpair_server(r->pair));
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(r->pair), &r->alloc,
                                 pcfg, &r->publisher) == MOQ_OK);
    if (version == MOQ_VERSION_DRAFT_22)
        r->admission_bytes = r->last_sizes[(r->alloc_calls - 3) % 4];
    r->ns = MOQ_BYTES_LITERAL("publisher-fill-ownership");
    moq_pub_track_cfg_t tc;
    moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace.parts = &r->ns;
    tc.track_namespace.count = 1;
    tc.track_name = MOQ_BYTES_LITERAL("video");
    tc.max_retained_bytes = UINT64_MAX;
    MOQ_TEST_CHECK(moq_pub_add_track(r->publisher, &tc, now(r), &r->track) == MOQ_OK);
}

static void rig_open_ex(rig_t *r, const moq_pub_cfg_t *cfg, moq_version_t version,
                         uint32_t actions)
{ rig_open_cap(r, cfg, version, actions, 0); }

static void rig_open(rig_t *r, const moq_pub_cfg_t *cfg)
{ rig_open_ex(r, cfg, MOQ_VERSION_DRAFT_22, 0); }

static void rig_close(rig_t *r)
{
    moq_pub_destroy(r->publisher);
    discard_events(moq_simpair_server(r->pair));
    discard_events(moq_simpair_client(r->pair));
    moq_simpair_destroy(r->pair);
    MOQ_TEST_CHECK(r->allocations.balance == 0);
}

static moq_rcbuf_t *retain(rig_t *r, uint64_t group, const char *data)
{
    moq_rcbuf_t *buf = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&r->alloc, (const uint8_t *)data,
                                   strlen(data), &buf) == MOQ_OK);
    moq_pub_retained_object_t object = {
        .object_id = 0, .payload = buf, .end_of_group = true
    };
    moq_pub_retained_group_cfg_t cfg;
    moq_pub_retained_group_cfg_init(&cfg);
    cfg.group_id = group;
    cfg.objects = &object;
    cfg.object_count = 1;
    MOQ_TEST_CHECK(moq_pub_set_retained_group(r->publisher, r->track, &cfg) == MOQ_OK);
    return buf;
}

static moq_subscription_t request_ex(rig_t *r, const moq_fill_cfg_t *fill,
                                     bool forward)
{
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_NEXT_OBJECT;
    moq_subscribe_cfg_t cfg;
    moq_subscribe_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.track_namespace.parts = &r->ns;
    cfg.track_namespace.count = 1;
    cfg.track_name = MOQ_BYTES_LITERAL("video");
    cfg.location = &location;
    cfg.fill = fill;
    cfg.forward = forward;
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(r->pair), &cfg,
                                        now(r), &sub) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r->pair, 8, NULL) == MOQ_OK);
    return sub;
}

static moq_subscription_t request(rig_t *r)
{
    moq_location_filter_cfg_t history;
    moq_location_filter_cfg_init(&history);
    history.kind = MOQ_LOCATION_FILTER_RELATIVE_GROUP;
    history.relative_group_offset = 1;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &history;
    return request_ex(r, &fill, true);
}

typedef struct {
    rig_t *rig;
    unsigned authorized, requests, terminals;
    moq_pub_fill_request_t request[32];
    moq_pub_fill_terminal_t terminal[32];
    bool defer, check_reentrancy;
    moq_pub_track_t *second_track;
    bool defer_authorization;
    moq_pub_deferred_t *authorization;
    uint64_t authorization_id;
} probe_t;

static moq_pub_accept_decision_t authorize(void *ctx,
    const moq_pub_subscribe_info_v1_t *info, moq_request_error_t *error)
{
    probe_t *p = ctx;
    (void)error;
    MOQ_TEST_CHECK(info->struct_size >= sizeof(*info));
    p->authorized++;
    if (p->defer_authorization && info->deferred) {
        p->authorization = info->deferred;
        p->authorization_id = info->deferred_id;
        p->defer_authorization = false;
        return MOQ_PUB_DECISION_DEFER;
    }
    return MOQ_PUB_DECISION_ACCEPT;
}

static moq_pub_fill_decision_t fill_request(void *ctx, moq_pub_track_t *track,
                                           const moq_pub_fill_request_t *request_info)
{
    probe_t *p = ctx;
    MOQ_TEST_CHECK(request_info->struct_size == sizeof(*request_info));
    MOQ_TEST_CHECK(track == p->rig->track || track == p->second_track);
    MOQ_TEST_CHECK(p->requests < 32);
    if (p->requests < 32) p->request[p->requests++] = *request_info;
    if (p->check_reentrancy) {
        MOQ_TEST_CHECK(moq_pub_open_fill(p->rig->publisher, request_info->fill,
                                        now(p->rig)) == MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(moq_pub_tick(p->rig->publisher, now(p->rig)) == MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(moq_pub_flush(p->rig->publisher, now(p->rig)) == MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(moq_pub_clear_retained_group(p->rig->publisher, track) == MOQ_ERR_WRONG_STATE);
        moq_pub_destroy(p->rig->publisher); /* refused; owner must remain alive */
    }
    return p->defer ? MOQ_PUB_FILL_DEFER : MOQ_PUB_FILL_AUTO_RETAINED;
}

static void fill_terminal(void *ctx, moq_pub_track_t *track,
                           const moq_pub_fill_terminal_t *terminal)
{
    probe_t *p = ctx;
    MOQ_TEST_CHECK(terminal->struct_size == sizeof(*terminal));
    MOQ_TEST_CHECK(track == p->rig->track || track == p->second_track);
    MOQ_TEST_CHECK(p->terminals < 32);
    if (p->terminals < 32) p->terminal[p->terminals++] = *terminal;
    if (p->check_reentrancy)
        MOQ_TEST_CHECK(moq_pub_abort_fill(p->rig->publisher, terminal->fill, 0,
                                         now(p->rig)) == MOQ_ERR_WRONG_STATE);
}

static moq_pub_cfg_t probe_config(probe_t *p)
{
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_CALLBACK_V1;
    cfg.on_subscribe_v1 = authorize;
    cfg.on_subscribe_v1_ctx = p;
    cfg.on_fill_request = fill_request;
    cfg.on_fill_request_ctx = p;
    cfg.on_fill_terminal = fill_terminal;
    cfg.on_fill_terminal_ctx = p;
    return cfg;
}

static void settle(rig_t *r)
{
    for (unsigned n = 0; n < 12; n++) {
        moq_result_t rc = moq_pub_tick(r->publisher, now(r));
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r->pair, 8, NULL) == MOQ_OK);
    }
}

static void discard_actions(moq_session_t *s)
{
    moq_action_t a;
    while (moq_session_poll_actions(s, &a, 1)) moq_action_cleanup(&a);
}

static unsigned fill_actions(moq_session_t *s, uint64_t when)
{
    unsigned n = 0;
    for (unsigned i = 0; i < 16; i++) {
        uint8_t name = (uint8_t)('a' + i);
        moq_bytes_t part = {&name, 1};
        moq_publish_namespace_cfg_t cfg;
        moq_publish_namespace_cfg_init(&cfg);
        cfg.track_namespace = (moq_namespace_t){&part, 1};
        moq_announcement_t ann;
        moq_result_t rc = moq_session_publish_namespace(s, &cfg, when, &ann);
        if (rc == MOQ_ERR_WOULD_BLOCK) break;
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK) break;
        n++;
    }
    MOQ_TEST_CHECK(n > 0 && n < 16);
    MOQ_TEST_CHECK(moq_session_action_capacity(s) == 0);
    return n;
}

static void test_blocked_ack_snapshot(bool advance_largest)
{
    rig_t r;
    probe_t p = {.rig = &r};
    moq_pub_cfg_t cfg = probe_config(&p);
    rig_open_ex(&r, &cfg, MOQ_VERSION_DRAFT_22, 6);
    moq_rcbuf_t *old = retain(&r, 3, "old");
    (void)request(&r);
    moq_session_t *sv = moq_simpair_server(r.pair);
    moq_event_t ev;
    (void)fill_actions(sv, now(&r));
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_pub_event_result_t consumed;
    MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 1);
    moq_rcbuf_t *replacement = retain(&r, advance_largest ? 4 : 3, "new");
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 2);
    discard_actions(sv);
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 1);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].end_group, advance_largest ? 4 : 3);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason,
        advance_largest ? MOQ_FILL_LOCAL_ABORTED : MOQ_FILL_COMPLETE);
    if (advance_largest) MOQ_TEST_CHECK_EQ_INT(p.terminal[0].error_code, 6);
    unsigned objects = 0;
    while (moq_session_poll_events(moq_simpair_client(r.pair), &ev, 1)) {
        if (ev.kind == MOQ_EVENT_FILL_OBJECT) {
            objects++;
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.fill_object.payload) == 3);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.fill_object.payload), "old", 3) == 0);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(objects, advance_largest ? 0 : 1);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 1);
    moq_rcbuf_decref(old);
    moq_rcbuf_decref(replacement);
    rig_close(&r);
    MOQ_TEST_PASS(advance_largest ? "authoritative_range_outgrows_snapshot" : "blocked_ack_keeps_old_refs");
}

static void test_defer_and_credit(bool byte_limit)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true, .check_reentrancy = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = byte_limit ? 2 : 1;
    cfg.max_fill_snapshot_bytes = byte_limit ? 3 : 64;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    (void)request(&r);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    moq_pub_fill_t first = p.request[0].fill;
    (void)request(&r);
    MOQ_TEST_CHECK(moq_pub_tick(r.publisher, now(&r)) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 2);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    MOQ_TEST_CHECK(moq_pub_open_fill(r.publisher, first, now(&r)) == MOQ_OK);
    moq_fetch_object_cfg_t object;
    moq_fetch_object_cfg_init(&object);
    object.group_id = 3;
    object.payload = buf;
    MOQ_TEST_CHECK(moq_pub_write_fill_object(r.publisher, first, &object, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 4);
    MOQ_TEST_CHECK(moq_pub_end_fill(r.publisher, first, now(&r)) == MOQ_OK);
    /* FIN has not dispatched the real terminal, so credit is STILL held. */
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 2);
    MOQ_TEST_CHECK(!moq_pub_fill_eq(first, p.request[1].fill));
    MOQ_TEST_CHECK(moq_pub_open_fill(r.publisher, first, now(&r)) == MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[1].fill, 6, now(&r)) == MOQ_OK);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[1].error_code, 6);
    MOQ_TEST_CHECK(moq_pub_fill_eq(p.terminal[0].fill, first));
    MOQ_TEST_CHECK_EQ_U64(p.terminal[1].initiating_request_id, p.request[1].initiating_request_id);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS(byte_limit ? "shared_bytes_wait_for_terminal" : "job_limit_waits_for_terminal");
}

static void test_permanent_bytes(bool default_limit)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fill_snapshot_bytes = default_limit ? 0 : 2;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf;
    const uint8_t byte = 0;
    size_t length = default_limit ? 16u * 1024u * 1024u + 1u : 3u;
    MOQ_TEST_CHECK(moq_rcbuf_wrap(&r.alloc, &byte, length, NULL, NULL, &buf) == MOQ_OK);
    moq_pub_retained_object_t object = {.payload = buf, .end_of_group = true};
    moq_pub_retained_group_cfg_t retained;
    moq_pub_retained_group_cfg_init(&retained);
    retained.group_id = 3;
    retained.objects = &object;
    retained.object_count = 1;
    MOQ_TEST_CHECK(moq_pub_set_retained_group(r.publisher, r.track, &retained) == MOQ_OK);
    (void)request(&r);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason, MOQ_FILL_LOCAL_ABORTED);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].error_domain, MOQ_FILL_ERROR_STREAM);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].error_code, 0);
    MOQ_TEST_CHECK(moq_pub_has_subscriber(r.publisher, r.track));
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 2);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS(default_limit ? "zero_budget_is_exactly_16MiB" : "permanent_cost_aborts_without_refs");
}

static void test_no_stream(bool paused)
{
    rig_t r;
    probe_t p = {.rig = &r};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = paused ? retain(&r, 3, "old") : NULL;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    (void)request_ex(&r, &fill, !paused);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 0);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    if (buf) {
        MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 2);
        moq_rcbuf_decref(buf);
    }
    buf = retain(&r, 3, "new");
    (void)request(&r);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS(paused ? "paused_no_stream_releases_reservation" : "no_largest_no_stream_releases_reservation");
}

static void test_abort_pressure(void)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    rig_open_ex(&r, &cfg, MOQ_VERSION_DRAFT_22, 3);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    (void)request(&r);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    moq_session_t *sv = moq_simpair_server(r.pair);
    (void)fill_actions(sv, now(&r));
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[0].fill, 6,
                                     now(&r)) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_ERR_WOULD_BLOCK);
    discard_actions(sv);
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    moq_action_t action;
    unsigned headers = 0, resets = 0, fins = 0;
    while (moq_session_poll_actions(sv, &action, 1)) {
        if (action.kind == MOQ_ACTION_SEND_DATA) {
            headers++;
            fins += action.u.send_data.fin;
        }
        if (action.kind == MOQ_ACTION_RESET_DATA) {
            resets++;
            MOQ_TEST_CHECK_EQ_INT(action.u.reset_data.error_code, 6);
            MOQ_TEST_CHECK(action.u.reset_data.reliable_size > 0);
        }
        moq_action_cleanup(&action);
    }
    MOQ_TEST_CHECK_EQ_INT(headers, 1);
    MOQ_TEST_CHECK_EQ_INT(resets, 1);
    MOQ_TEST_CHECK_EQ_INT(fins, 0);
    MOQ_TEST_CHECK(moq_pub_tick(r.publisher, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason, MOQ_FILL_LOCAL_ABORTED);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 2);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS("unopened_abort_retries_header_and_reset");
}

static void test_parent_cause(void)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    moq_subscription_t sub = request(&r);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK(moq_pub_open_fill(r.publisher, p.request[0].fill, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_client(r.pair), sub, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    moq_result_t rc = moq_pub_abort_fill(r.publisher, p.request[0].fill, 0, now(&r));
    MOQ_TEST_CHECK(rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_WRONG_STATE);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason, MOQ_FILL_PARENT_TERMINATED);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 2);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS("original_parent_terminal_wins");
}

static void test_foreign_owner(void)
{
    rig_t a, b;
    probe_t pa = {.rig = &a, .defer = true}, pb = {.rig = &b, .defer = true};
    moq_pub_cfg_t ca = probe_config(&pa), cb = probe_config(&pb);
    rig_open(&a, &ca);
    rig_open(&b, &cb);
    moq_rcbuf_t *ba = retain(&a, 3, "a"), *bb = retain(&b, 3, "b");
    (void)request(&a); (void)request(&b);
    settle(&a); settle(&b);
    MOQ_TEST_CHECK(pa.requests == 1 && pb.requests == 1);
    MOQ_TEST_CHECK(!moq_pub_fill_eq(pa.request[0].fill, pb.request[0].fill));
    MOQ_TEST_CHECK(moq_pub_open_fill(b.publisher, pa.request[0].fill, now(&b)) == MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moq_pub_abort_fill(a.publisher, pb.request[0].fill, 0, now(&a)) == MOQ_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(pa.terminals == 0 && pb.terminals == 0);
    MOQ_TEST_CHECK(moq_pub_abort_fill(a.publisher, pa.request[0].fill, 0, now(&a)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_abort_fill(b.publisher, pb.request[0].fill, 0, now(&b)) == MOQ_OK);
    settle(&a); settle(&b);
    moq_rcbuf_decref(ba); moq_rcbuf_decref(bb);
    rig_close(&a); rig_close(&b);
    MOQ_TEST_PASS("foreign_owner_cannot_name_live_job");
}

static void test_full_width_defer(void)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    rig_open(&r, &cfg);
    moq_session_t *sv = moq_simpair_server(r.pair), *cl = moq_simpair_client(r.pair);
    /* A separate, raw-owned subscription seeds the public track history. */
    moq_subscription_t raw = request_ex(&r, NULL, true);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t raw_server = ev.u.subscribe_request.sub;
    moq_accept_subscribe_cfg_t ac;
    moq_accept_subscribe_cfg_init(&ac);
    ac.has_largest = true;
    ac.largest_group = UINT64_MAX;
    ac.largest_object = UINT64_MAX;
    MOQ_TEST_CHECK(moq_session_accept_subscribe(sv, raw_server, &ac, now(&r)) == MOQ_OK);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    discard_events(cl);

    moq_location_filter_cfg_t loc;
    moq_location_filter_cfg_init(&loc);
    loc.kind = MOQ_LOCATION_FILTER_OBJECT_END;
    loc.start_group = loc.end_group = UINT64_MAX;
    loc.start_object = UINT64_MAX - 1;
    loc.end_object = UINT64_MAX;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &loc;
    fill.has_group_order = true;
    fill.group_order = MOQ_GROUP_ORDER_DESCENDING;
    fill.has_subscriber_priority = true;
    fill.subscriber_priority = 7;
    fill.has_fill_timeout = true;
    fill.fill_timeout_ms = UINT64_MAX;
    (void)request_ex(&r, &fill, true);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].start_group, UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].end_object, UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].start_object, UINT64_MAX - 1);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].fill_timeout_ms, UINT64_MAX);
    MOQ_TEST_CHECK_EQ_INT(p.request[0].group_order, MOQ_GROUP_ORDER_DESCENDING);
    MOQ_TEST_CHECK_EQ_INT(p.request[0].subscriber_priority, 7);
    MOQ_TEST_CHECK(p.request[0].has_fill_timeout && p.request[0].processed_at_us > 0);
    moq_pub_fill_t token = p.request[0].fill;
    MOQ_TEST_CHECK(moq_pub_open_fill(r.publisher, token, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_write_fill_range(r.publisher, token, MOQ_FETCH_RANGE_NON_EXISTENT,
        UINT64_MAX, UINT64_MAX - 1, now(&r)) == MOQ_OK);
    moq_rcbuf_t *payload;
    MOQ_TEST_CHECK(moq_rcbuf_create(&r.alloc, (const uint8_t *)"z", 1, &payload) == MOQ_OK);
    moq_fetch_object_cfg_t object;
    moq_fetch_object_cfg_init(&object);
    object.group_id = UINT64_MAX;
    object.object_id = UINT64_MAX;
    object.payload = payload;
    MOQ_TEST_CHECK(moq_pub_write_fill_object(r.publisher, token, &object, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(payload), 2);
    MOQ_TEST_CHECK(moq_pub_end_fill(r.publisher, token, now(&r)) == MOQ_OK);
    settle(&r);
    unsigned objects = 0, gaps = 0;
    while (moq_session_poll_events(cl, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_FILL_GAP) {
            gaps++;
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_gap.object_id, UINT64_MAX - 1);
        }
        if (ev.kind == MOQ_EVENT_FILL_OBJECT) {
            objects++;
            MOQ_TEST_CHECK_EQ_U64(ev.u.fill_object.object_id, UINT64_MAX);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(objects == 1 && gaps == 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason, MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(payload), 1);
    moq_rcbuf_decref(payload);

    /* The facade did not change the global update policy for the raw parent. */
    moq_subscription_update_cfg_t update;
    moq_subscription_update_cfg_init_sized(&update, sizeof(update));
    update.has_subscriber_priority = true;
    update.subscriber_priority = 9;
    MOQ_TEST_CHECK(moq_session_update_subscription(cl, raw, &update, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    bool updated = false;
    while (moq_session_poll_events(sv, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
            MOQ_TEST_CHECK(!ev.u.subscribe_updated.answer_owed);
            updated = true;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(updated);
    rig_close(&r);
    MOQ_TEST_PASS("full_width_defer_range_and_raw_parent_progress");
}

static void test_update_snapshot(bool publication)
{
    rig_t r;
    probe_t p = {.rig = &r};
    moq_pub_cfg_t cfg = probe_config(&p);
    rig_open_ex(&r, &cfg, MOQ_VERSION_DRAFT_22, 6);
    moq_rcbuf_t *old = retain(&r, 3, "old");
    moq_session_t *sv = moq_simpair_server(r.pair), *cl = moq_simpair_client(r.pair);
    moq_subscription_t sub = MOQ_SUBSCRIPTION_INVALID;
    moq_publication_t pub = MOQ_PUBLICATION_INVALID;
    moq_event_t ev;
    if (publication) {
        moq_pub_publish_cfg_t pc;
        moq_pub_publish_cfg_init(&pc);
        MOQ_TEST_CHECK(moq_pub_publish_track(r.publisher, r.track, &pc, now(&r)) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_poll_events(cl, &ev, 1) == 1);
        MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_PUBLISH_REQUEST);
        pub = ev.u.publish_request.pub;
        moq_event_cleanup(&ev);
        moq_accept_publish_cfg_t ac;
        moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
        MOQ_TEST_CHECK(moq_session_accept_publish(cl, pub, &ac, now(&r)) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
        settle(&r);
        MOQ_TEST_CHECK(moq_pub_track_is_published(r.publisher, r.track));
    } else {
        sub = request_ex(&r, NULL, true);
        settle(&r);
    }
    MOQ_TEST_CHECK(p.requests == 0 && p.terminals == 0);
    discard_events(cl);
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    location.start_group = 3;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    fill.has_group_order = true;
    fill.group_order = MOQ_GROUP_ORDER_DESCENDING;
    fill.has_subscriber_priority = true;
    fill.subscriber_priority = 11;
    fill.has_fill_timeout = true;
    fill.fill_timeout_ms = UINT64_MAX;
    if (publication) {
        moq_publication_update_cfg_t update;
        moq_publication_update_cfg_init_sized(&update, sizeof(update));
        update.fill = &fill;
        MOQ_TEST_CHECK(moq_session_update_publication(cl, pub, &update, now(&r)) == MOQ_OK);
    } else {
        moq_subscription_update_cfg_t update;
        moq_subscription_update_cfg_init_sized(&update, sizeof(update));
        update.fill = &fill;
        MOQ_TEST_CHECK(moq_session_update_subscription(cl, sub, &update, now(&r)) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    uint64_t processed = now(&r);
    (void)fill_actions(sv, now(&r));
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == (publication ? MOQ_EVENT_PUBLISH_UPDATED : MOQ_EVENT_SUBSCRIBE_UPDATED));
    uint64_t initiating = publication ? ev.u.publish_updated.update_request_id
                                      : ev.u.subscribe_updated.update_request_id;
    MOQ_TEST_CHECK(publication ? ev.u.publish_updated.answer_owed : ev.u.subscribe_updated.answer_owed);
    moq_pub_event_result_t consumed;
    MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&ev);
    moq_rcbuf_t *replacement = retain(&r, 4, "new");
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 2);
    discard_actions(sv);
    MOQ_TEST_CHECK(moq_simpair_advance_to(r.pair, processed + 77) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_OK);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].initiating_request_id, initiating);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].processed_at_us, processed);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].start_group, 3);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].end_group, 3);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].fill_timeout_ms, UINT64_MAX);
    MOQ_TEST_CHECK_EQ_INT(p.request[0].group_order, MOQ_GROUP_ORDER_DESCENDING);
    MOQ_TEST_CHECK_EQ_INT(p.request[0].subscriber_priority, 11);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason, MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK(moq_publication_is_valid(p.request[0].pub) == publication);
    MOQ_TEST_CHECK(moq_subscription_is_valid(p.request[0].sub) != publication);
    unsigned objects = 0;
    while (moq_session_poll_events(cl, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_FILL_OBJECT) {
            objects++;
            MOQ_TEST_CHECK(moq_rcbuf_len(ev.u.fill_object.payload) == 3);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.fill_object.payload), "old", 3) == 0);
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(objects, 1);
    moq_rcbuf_decref(old);
    moq_rcbuf_decref(replacement);
    rig_close(&r);
    MOQ_TEST_PASS(publication ? "publication_update_keeps_original_delivery" : "subscription_update_keeps_original_delivery");
}

static void test_snapshot_held_until_terminal(void)
{
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    cfg.max_fills = 1;
    rig_t r;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    (void)request(&r);
    moq_session_t *server = moq_simpair_server(r.pair);
    moq_event_t event;
    bool saw_request = false;
    for (unsigned n = 0; n < 8 && moq_session_poll_events(server, &event, 1); n++) {
        moq_pub_event_result_t consumed;
        moq_result_t rc = moq_pub_handle_event(r.publisher, &event, now(&r), &consumed);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        saw_request = event.kind == MOQ_EVENT_FILL_REQUEST;
        moq_event_cleanup(&event);
        if (saw_request) break;
    }
    MOQ_TEST_CHECK(saw_request);
    MOQ_TEST_CHECK(moq_pub_clear_retained_group(r.publisher, r.track) == MOQ_OK);
    moq_action_t action;
    while (moq_session_poll_actions(server, &action, 1)) moq_action_cleanup(&action);
    /* The caller and the job still own refs, although FIN is already queued.
     * The facade has not consumed the real terminal event yet. */
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 2);
    unsigned terminals = 0;
    while (moq_session_poll_events(server, &event, 1)) {
        moq_pub_event_result_t consumed;
        MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &event, now(&r),
                                            &consumed) == MOQ_OK);
        if (event.kind == MOQ_EVENT_FILL_TERMINAL) {
            terminals++;
            MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
        }
        moq_event_cleanup(&event);
    }
    MOQ_TEST_CHECK_EQ_INT(terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 1);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS("snapshot_held_until_terminal");
}

static void settle_manual(rig_t *r)
{
    for (unsigned n = 0; n < 12; n++) {
        moq_event_t ev;
        while (moq_session_poll_events(moq_simpair_server(r->pair), &ev, 1)) {
            moq_pub_event_result_t consumed;
            moq_result_t rc = moq_pub_handle_event(r->publisher, &ev, now(r), &consumed);
            MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST ||
                ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED ||
                ev.kind == MOQ_EVENT_PUBLISH_UPDATED || ev.kind == MOQ_EVENT_FILL_REQUEST)
                MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
            /* A cancelled, unaccepted fill can terminate without ever owning
             * a facade job. Its core terminal is correctly left IGNORED. */
            moq_event_cleanup(&ev);
            rc = moq_pub_flush(r->publisher, now(r));
            MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        }
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r->pair, 8, NULL) == MOQ_OK);
        moq_result_t rc = moq_pub_flush(r->publisher, now(r));
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    }
}

static void test_batched_admission(bool manual)
{
    rig_t r;
    probe_t p = {.rig = &r};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    /* All three ordinary requests arrive before the facade starts its pump.
     * The third must not be discarded behind the single pending admission. */
    (void)request(&r);
    (void)request(&r);
    (void)request(&r);
    if (manual) settle_manual(&r); else settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 3);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 3);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 3);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_active_subscriptions(r.publisher, r.track), 3);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS(manual ? "manual_batched_admission_preserves_all_requests"
                         : "batched_admission_preserves_all_requests");
}

typedef struct {
    moq_subscription_t sub;
    moq_publication_t pub;
} parent_t;

static parent_t establish(rig_t *r, moq_pub_track_t *track, bool publication,
                           bool leave_ok_queued)
{
    parent_t parent = {0};
    if (!publication) {
        parent.sub = request_ex(r, NULL, true);
        settle(r);
        return parent;
    }
    moq_pub_publish_cfg_t cfg;
    moq_pub_publish_cfg_init(&cfg);
    MOQ_TEST_CHECK(moq_pub_publish_track(r->publisher, track, &cfg, now(r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r->pair, 8, NULL) == MOQ_OK);
    moq_event_t ev;
    unsigned requests = 0;
    while (moq_session_poll_events(moq_simpair_client(r->pair), &ev, 1)) {
        if (ev.kind == MOQ_EVENT_PUBLISH_REQUEST) {
            parent.pub = ev.u.publish_request.pub;
            requests++;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(requests, 1);
    moq_accept_publish_cfg_t ac;
    moq_accept_publish_cfg_init_sized(&ac, sizeof(ac));
    MOQ_TEST_CHECK(moq_session_accept_publish(moq_simpair_client(r->pair), parent.pub,
                                              &ac, now(r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r->pair, 8, NULL) == MOQ_OK);
    if (!leave_ok_queued) settle(r);
    return parent;
}

static void update_parent(rig_t *r, parent_t parent, bool fill_requested)
{
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    location.start_group = 3;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    fill.location = &location;
    fill.has_subscriber_priority = true;
    fill.subscriber_priority = 11;
    if (moq_publication_is_valid(parent.pub)) {
        moq_publication_update_cfg_t cfg;
        moq_publication_update_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.has_forward = true;
        cfg.forward = true;
        cfg.fill = fill_requested ? &fill : NULL;
        MOQ_TEST_CHECK(moq_session_update_publication(moq_simpair_client(r->pair),
                         parent.pub, &cfg, now(r)) == MOQ_OK);
    } else {
        moq_subscription_update_cfg_t cfg;
        moq_subscription_update_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.has_forward = true;
        cfg.forward = true;
        cfg.fill = fill_requested ? &fill : NULL;
        MOQ_TEST_CHECK(moq_session_update_subscription(moq_simpair_client(r->pair),
                         parent.sub, &cfg, now(r)) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r->pair, 8, NULL) == MOQ_OK);
}

static void test_queued_updates(unsigned origins, bool manual)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    cfg.max_fill_snapshot_bytes = 3;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    moq_rcbuf_t *other = NULL;
    if (origins == 3) {
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init_sized(&tc, sizeof(tc));
        tc.track_namespace = (moq_namespace_t){&r.ns, 1};
        tc.track_name = MOQ_BYTES_LITERAL("audio");
        MOQ_TEST_CHECK(moq_pub_add_track(r.publisher, &tc, now(&r), &p.second_track) == MOQ_OK);
        moq_pub_track_t *first = r.track;
        r.track = p.second_track;
        other = retain(&r, 3, "old");
        r.track = first;
    }
    parent_t a = establish(&r, r.track, (origins & 1) != 0, false);
    parent_t b = establish(&r, p.second_track ? p.second_track : r.track,
                            (origins & 2) != 0, false);
    (void)request(&r);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    update_parent(&r, a, true);
    update_parent(&r, b, true);
    size_t active = moq_pub_active_subscriptions(r.publisher, r.track);
    (void)request_ex(&r, NULL, true); /* Must pass both waiting fill answers. */
    if (manual) settle_manual(&r); else settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_active_subscriptions(r.publisher, r.track), active + 1);
    MOQ_TEST_CHECK(moq_pub_remove_track(r.publisher, r.track, now(&r)) == MOQ_ERR_WOULD_BLOCK);
    for (unsigned i = 0; i < 3; i++) {
        MOQ_TEST_CHECK_EQ_INT(p.requests, i + 1);
        MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[i].fill, 6, now(&r)) == MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT(p.terminals, i);
        MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) ==
                       (i < 2 ? MOQ_ERR_WOULD_BLOCK : MOQ_OK));
        MOQ_TEST_CHECK_EQ_INT(p.requests, i + 1);
        if (manual) settle_manual(&r); else settle(&r);
        MOQ_TEST_CHECK_EQ_INT(p.terminals, i + 1);
    }
    MOQ_TEST_CHECK_EQ_INT(p.requests, 3);
    for (unsigned i = 1; i < 3; i++) {
        MOQ_TEST_CHECK_EQ_INT(p.request[i].subscriber_priority, 11);
        MOQ_TEST_CHECK_EQ_U64(p.request[i].start_group, 3);
        MOQ_TEST_CHECK_EQ_U64(p.request[i].end_group, 3);
        MOQ_TEST_CHECK(moq_publication_is_valid(p.request[i].pub) ==
                       ((origins & (1u << (i - 1))) != 0));
        MOQ_TEST_CHECK(p.request[i].initiating_request_id != p.request[i - 1].initiating_request_id);
        MOQ_TEST_CHECK_EQ_U64(p.terminal[i].initiating_request_id, p.request[i].initiating_request_id);
        MOQ_TEST_CHECK(moq_pub_fill_eq(p.terminal[i].fill, p.request[i].fill));
    }
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 2);
    moq_rcbuf_decref(buf);
    if (other) {
        MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(other), 2);
        moq_rcbuf_decref(other);
    }
    rig_close(&r);
    char name[80];
    snprintf(name, sizeof(name), "queued_updates_origins%u_%s", origins,
             manual ? "manual" : "automatic");
    MOQ_TEST_PASS(name);
}

static void test_sequential_updates(bool publication)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    /* PUBLISH_OK remains ahead of UPDATE in the facade FIFO. */
    parent_t parent = establish(&r, r.track, publication, publication);
    update_parent(&r, parent, true);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[0].fill, 6, now(&r)) == MOQ_OK);
    /* The first answer committed, but its terminal is still queued. */
    update_parent(&r, parent, true);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK(p.request[0].initiating_request_id != p.request[1].initiating_request_id);
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[1].fill, 6, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 2);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS(publication ? "publish_ok_and_sequential_updates" : "subscribe_sequential_updates");
}

static void test_authorizer_defer_progress(void)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer_authorization = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    (void)request(&r);
    (void)request(&r);
    (void)request(&r);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 3);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 2);
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_pub_resolve_deferred(r.publisher, p.authorization,
                     p.authorization_id, true, 0, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 3);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 3);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 3);
    MOQ_TEST_CHECK(moq_pub_resolve_deferred(r.publisher, p.authorization,
                     p.authorization_id, true, 0, now(&r)) == MOQ_ERR_STALE_HANDLE);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS("single_authorizer_defer_does_not_block_other_parents");
}

/* Deliberately repeat an owned scalar observation, never a saved borrowed
 * event. The core's live handle/owed operation remains authoritative. */
static moq_event_t owned_request(rig_t *r, moq_subscription_t sub,
                                const moq_subscription_admission_options_t *options)
{
    moq_event_t ev = {0};
    ev.kind = MOQ_EVENT_SUBSCRIBE_REQUEST;
    ev.detail_size = sizeof(ev.u.subscribe_request);
    ev.u.subscribe_request.sub = sub;
    ev.u.subscribe_request.track_namespace = (moq_namespace_t){&r->ns, 1};
    ev.u.subscribe_request.track_name = MOQ_BYTES_LITERAL("video");
    ev.u.subscribe_request.forward = true;
    ev.u.subscribe_request.options = options;
    return ev;
}

static void test_stale_generation(bool before_dispatch)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    (void)request(&r);
    settle(&r);
    moq_subscription_t peer_old = request(&r);
    moq_session_t *sv = moq_simpair_server(r.pair);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t old = ev.u.subscribe_request.sub;
    moq_pub_event_result_t consumed;
    if (!before_dispatch) {
        MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
    }
    moq_event_cleanup(&ev);
    moq_subscription_admission_options_t options = {0};
    options.struct_size = sizeof(options);
    options.flags = MOQ_SUBSCRIPTION_ADMISSION_FILL;
    moq_event_t stale = owned_request(&r, old, &options);
    if (!before_dispatch) {
        MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &stale, now(&r), &consumed) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT(p.authorized, 2);
    }
    MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_client(r.pair), peer_old, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    (void)request(&r);
    moq_subscription_t replacement = MOQ_SUBSCRIPTION_INVALID;
    while (moq_session_poll_events(sv, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) replacement = ev.u.subscribe_request.sub;
        moq_result_t rc = moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_subscription_is_valid(replacement));
    MOQ_TEST_CHECK(!moq_subscription_eq(old, replacement));
    MOQ_TEST_CHECK_EQ_INT(moq_handle_slot(old._opaque), moq_handle_slot(replacement._opaque));
    MOQ_TEST_CHECK_EQ_INT(p.authorized, before_dispatch ? 2 : 3);
    MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &stale, now(&r), &consumed) == MOQ_OK);
    moq_event_t late = {0};
    late.kind = MOQ_EVENT_UNSUBSCRIBED;
    late.u.unsubscribed.sub = old;
    MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &late, now(&r), &consumed) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[0].fill, 6, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 2);
    MOQ_TEST_CHECK(moq_subscription_eq(p.request[1].sub, replacement));
    MOQ_TEST_CHECK_EQ_INT(p.authorized, before_dispatch ? 2 : 3);
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[1].fill, 6, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 2);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS(before_dispatch ? "stale_before_authorization_slot_reuse"
                                 : "stale_staged_decision_and_duplicate_slot_reuse");
}

static void test_destination_allocation_retry(void)
{
    rig_t r;
    probe_t p = {.rig = &r};
    moq_pub_cfg_t cfg = probe_config(&p);
    rig_open(&r, &cfg);
    (void)request_ex(&r, NULL, true);
    settle(&r);
    (void)request_ex(&r, NULL, true);
    settle(&r); /* Tracks start with two slots, including publication space. */
    (void)request_ex(&r, NULL, true);
    moq_session_t *sv = moq_simpair_server(r.pair);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    size_t action_space = moq_session_action_capacity(sv);
    r.fail_at = r.alloc_calls + 1;
    moq_pub_event_result_t consumed;
    moq_result_t rc = moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed);
    MOQ_TEST_CHECK(rc == MOQ_ERR_NOMEM);
    MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&ev);
    if (rc != MOQ_ERR_NOMEM) { r.fail_at = 0; rig_close(&r); return; }
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 3);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_active_subscriptions(r.publisher, r.track), 2);
    MOQ_TEST_CHECK_EQ_INT(moq_session_action_capacity(sv), action_space);
    MOQ_TEST_CHECK(moq_pub_remove_track(r.publisher, r.track, now(&r)) == MOQ_ERR_WOULD_BLOCK);
    r.fail_at = 0;
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 3);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_active_subscriptions(r.publisher, r.track), 3);
    rig_close(&r);
    MOQ_TEST_PASS("destination_oom_keeps_decision_and_no_ack_or_reject");
}

static void test_creation_allocations(moq_version_t version)
{
    rig_t r;
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    rig_open_ex(&r, &cfg, version, 0);
    /* Publisher, optional fills, admissions, FETCH owners, generic answers. */
    unsigned allocations = version == MOQ_VERSION_DRAFT_22 ? 5 : 4;
    for (unsigned failure = 1; failure <= allocations; failure++) {
        int64_t balance = r.allocations.balance;
        r.fail_at = r.alloc_calls + failure;
        moq_publisher_t *pub = NULL;
        MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(r.pair), &r.alloc, &cfg, &pub) == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(pub == NULL);
        MOQ_TEST_CHECK_EQ_INT(r.allocations.balance, balance);
    }
    r.fail_at = 0;
    size_t calls = r.alloc_calls;
    moq_publisher_t *pub = NULL;
    MOQ_TEST_CHECK(moq_pub_create(moq_simpair_server(r.pair), &r.alloc, &cfg, &pub) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(r.alloc_calls - calls, allocations);
    if (version == MOQ_VERSION_DRAFT_22)
        MOQ_TEST_CHECK_EQ_INT(r.last_sizes[(r.alloc_calls - 3) % 4],
                             80 * (sizeof(void *) == 8 ? 64 : 56));
    moq_pub_destroy(pub);
    rig_close(&r);
    char name[64];
    snprintf(name, sizeof(name), "creation_allocation_and_unwind_draft%u", (unsigned)version);
    MOQ_TEST_PASS(name);
}

static void test_early_subscribe_update(void)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    moq_session_t *sv = moq_simpair_server(r.pair);
    moq_stream_ref_t ref = moq_stream_ref_from_u64(0x7800);
    /* Public ingress, hand-authored D22: SUBSCRIBE id0, one namespace part,
     * video, FILL_PARAMETERS(Number of Parameters 0); then UPDATE id2. */
    uint8_t wire[96] = {3, 0, 0, 0, 1};
    size_t n = 5;
    wire[n++] = (uint8_t)r.ns.len;
    memcpy(wire + n, r.ns.data, r.ns.len); n += r.ns.len;
    wire[n++] = 5;
    memcpy(wire + n, "video", 5); n += 5;
    wire[n++] = 1; wire[n++] = 0x23; wire[n++] = 1; wire[n++] = 0;
    wire[2] = (uint8_t)(n - 3);
    const uint8_t update[] = {2, 0, 5, 2, 1, 0x23, 1, 0};
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, wire, n, false, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, update, sizeof(update), false,
                                                  now(&r)) == MOQ_ERR_WOULD_BLOCK);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_pub_event_result_t consumed;
    MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed) == MOQ_OK);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_bytes(sv, ref, NULL, 0, false, now(&r)) == MOQ_OK);
    unsigned updates = 0;
    while (moq_session_poll_events(sv, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
            updates++;
            MOQ_TEST_CHECK(ev.u.subscribe_updated.answer_owed);
            MOQ_TEST_CHECK_EQ_INT(ev.u.subscribe_updated.update_count, 1);
            MOQ_TEST_CHECK_EQ_U64(ev.u.subscribe_updated.update_request_id, 2);
        }
        moq_result_t rc = moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(updates, 1);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 1);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[0].fill, 6, now(&r)) == MOQ_OK);
    for (unsigned round = 0; round < 4; round++) {
        discard_actions(sv); /* This raw ingress stream has no SimPair peer. */
        moq_result_t rc = moq_pub_tick(r.publisher, now(&r));
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
    }
    MOQ_TEST_CHECK_EQ_INT(p.requests, 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_U64(p.request[0].initiating_request_id, 0);
    MOQ_TEST_CHECK_EQ_U64(p.request[1].initiating_request_id, 2);
    MOQ_TEST_CHECK(moq_subscription_eq(p.request[0].sub, p.request[1].sub));
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[1].fill, 6, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_tick(r.publisher, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 2);
    MOQ_TEST_CHECK(moq_session_state(sv) == MOQ_SESS_ESTABLISHED);
    discard_actions(sv);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS("early_subscribe_update_reuses_committed_initial_cell");
}

static void test_small_parent_capacity(unsigned capacity)
{
    rig_t r;
    probe_t p = {.rig = &r};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open_cap(&r, &cfg, MOQ_VERSION_DRAFT_22, 0, capacity);
    MOQ_TEST_CHECK_EQ_INT(r.admission_bytes, (capacity + 16) * (sizeof(void *) == 8 ? 64 : 56));
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    for (unsigned i = 0; i < capacity; i++) (void)request(&r);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, capacity);
    MOQ_TEST_CHECK_EQ_INT(p.requests, capacity);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, capacity);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_active_subscriptions(r.publisher, r.track), capacity);
    discard_events(moq_simpair_client(r.pair));
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 2);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    char name[64];
    snprintf(name, sizeof(name), "bounded_parent_capacity_%u", capacity);
    MOQ_TEST_PASS(name);
}

static void test_cancel_unbound_reservation(void)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open_ex(&r, &cfg, MOQ_VERSION_DRAFT_22, 6);
    moq_rcbuf_t *old = retain(&r, 3, "old");
    moq_subscription_t peer_old = request(&r);
    moq_session_t *sv = moq_simpair_server(r.pair);
    (void)fill_actions(sv, now(&r));
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
    moq_subscription_t old_parent = ev.u.subscribe_request.sub;
    moq_pub_event_result_t consumed;
    MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed) == MOQ_ERR_WOULD_BLOCK);
    moq_event_cleanup(&ev);
    moq_rcbuf_t *replacement = retain(&r, 3, "new");
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 2);
    discard_actions(sv);
    MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_client(r.pair), peer_old, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    (void)request(&r);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 2);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 1);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(replacement), 3);
    MOQ_TEST_CHECK_EQ_INT(moq_handle_slot(p.request[0].sub._opaque), moq_handle_slot(old_parent._opaque));
    MOQ_TEST_CHECK(!moq_subscription_eq(p.request[0].sub, old_parent));
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[0].fill, 6, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    moq_rcbuf_decref(old);
    moq_rcbuf_decref(replacement);
    rig_close(&r);
    MOQ_TEST_PASS("cancel_releases_unbound_candidate_before_generation_reuse");
}

static void test_destroy_waiting_admissions(void)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    for (unsigned i = 0; i < 3; i++) (void)request(&r);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.authorized, 3);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    moq_pub_destroy(r.publisher);
    r.publisher = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS("destroy_unwinds_bound_job_and_waiting_scalar_cells_once");
}

static void test_publication_generation_reuse(void)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    parent_t old = establish(&r, r.track, true, false);
    update_parent(&r, old, true);
    settle_manual(&r);
    update_parent(&r, old, true);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    /* Peer RESET, not local clean finish (which correctly refuses an owed
     * answer). Do not dispatch the old terminal before reusing the core slot. */
    MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(moq_simpair_server(r.pair),
                     r.publication_stream, 0, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    /* The core parent is now stale: local unpublish clears the facade marker
     * without polling the old child's queued terminal. */
    MOQ_TEST_CHECK(moq_pub_unpublish_track(r.publisher, r.track, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    parent_t replacement = establish(&r, r.track, true, true);
    update_parent(&r, replacement, true);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason, MOQ_FILL_PARENT_TERMINATED);
    MOQ_TEST_CHECK_EQ_INT(moq_handle_slot(p.request[0].pub._opaque), moq_handle_slot(p.request[1].pub._opaque));
    MOQ_TEST_CHECK(!moq_publication_eq(p.request[0].pub, p.request[1].pub));
    update_parent(&r, replacement, true);
    settle_manual(&r);
    moq_event_t late = {0};
    late.kind = MOQ_EVENT_PUBLISH_UNSUBSCRIBED;
    late.u.publish_unsubscribed.pub = p.request[0].pub;
    moq_pub_event_result_t consumed;
    MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &late, now(&r), &consumed) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[1].fill, 6, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 3);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 2);
    MOQ_TEST_CHECK(moq_publication_eq(p.request[1].pub, p.request[2].pub));
    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[2].fill, 6, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 3);
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS("publication_generation_reuse_keeps_old_child_terminal_independent");
}

static moq_result_t goaway_owed(rig_t *r, const moq_pub_fill_request_t *first,
                                uint64_t *id)
{
    moq_session_t *sv = moq_simpair_server(r->pair);
    return moq_publication_is_valid(first->pub)
        ? moq_session_publication_owed_update(sv, first->pub, id, NULL, NULL)
        : moq_session_subscription_owed_update(sv, first->sub, id, NULL, NULL);
}

static void goaway_peer_events(rig_t *r, parent_t parent, unsigned acks,
                                unsigned objects, unsigned goaways)
{
    unsigned na = 0, no = 0, ng = 0;
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(r->pair), &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIPTION_UPDATE_OK) {
            na++;
            MOQ_TEST_CHECK(moq_subscription_eq(ev.u.subscription_update_ok.sub, parent.sub));
        } else if (ev.kind == MOQ_EVENT_PUBLICATION_UPDATE_OK) {
            na++;
            MOQ_TEST_CHECK(moq_publication_eq(ev.u.publication_update_ok.pub, parent.pub));
        } else if (ev.kind == MOQ_EVENT_FILL_OBJECT) {
            no++;
            MOQ_TEST_CHECK_EQ_SIZE(moq_rcbuf_len(ev.u.fill_object.payload), 3);
            MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(ev.u.fill_object.payload), "old", 3) == 0);
        } else if (ev.kind == MOQ_EVENT_REQUEST_GOAWAY) {
            ng++;
        }
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(na, acks);
    MOQ_TEST_CHECK_EQ_INT(no, objects);
    MOQ_TEST_CHECK_EQ_INT(ng, goaways);
}

/* Consume GOAWAY at the mock transport: model a peer that keeps the old
 * request alive, rather than the stock peer's immediate migration FIN.
 * All UPDATE, ACK, fill and cancellation traffic still uses real core APIs. */
static bool stage_goaway_update(rig_t *r, probe_t *p, parent_t parent,
                                 uint64_t *id)
{
    moq_session_t *sv = moq_simpair_server(r->pair);
    moq_request_goaway_cfg_t gc;
    moq_request_goaway_cfg_init(&gc);
    moq_result_t rc = moq_publication_is_valid(parent.pub)
        ? moq_session_request_goaway_publish(sv, p->request[0].pub, &gc, now(r))
        : moq_session_request_goaway_subscribe(sv, p->request[0].sub, &gc, now(r));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    moq_action_t action;
    unsigned sent = 0;
    const uint8_t expected[] = {0x10, 0, 2, 0, 0};
    while (moq_session_poll_actions(sv, &action, 1)) {
        sent++;
        MOQ_TEST_CHECK(action.kind == MOQ_ACTION_SEND_BIDI_STREAM);
        if (action.kind == MOQ_ACTION_SEND_BIDI_STREAM) {
            MOQ_TEST_CHECK(!action.u.send_bidi_stream.fin);
            MOQ_TEST_CHECK_EQ_SIZE(action.u.send_bidi_stream.len, sizeof(expected));
            if (action.u.send_bidi_stream.len == sizeof(expected))
                MOQ_TEST_CHECK(memcmp(action.u.send_bidi_stream.data, expected, sizeof(expected)) == 0);
        }
        moq_action_cleanup(&action);
    }
    MOQ_TEST_CHECK_EQ_INT(sent, 1);
    update_parent(r, parent, true);
    unsigned updates = 0, children = 0;
    bool owed = false;
    moq_event_t ev;
    while (moq_session_poll_events(sv, &ev, 1)) {
        if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED || ev.kind == MOQ_EVENT_PUBLISH_UPDATED) {
            bool pub = ev.kind == MOQ_EVENT_PUBLISH_UPDATED;
            updates++;
            owed = pub ? ev.u.publish_updated.answer_owed : ev.u.subscribe_updated.answer_owed;
            *id = pub ? ev.u.publish_updated.update_request_id : ev.u.subscribe_updated.update_request_id;
            MOQ_TEST_CHECK(owed);
            MOQ_TEST_CHECK_EQ_INT(pub ? ev.u.publish_updated.update_count
                                      : ev.u.subscribe_updated.update_count, 1);
        }
        if (ev.kind == MOQ_EVENT_FILL_REQUEST) children++;
        moq_pub_event_result_t consumed;
        rc = moq_pub_handle_event(r->publisher, &ev, now(r), &consumed);
        MOQ_TEST_CHECK(rc == MOQ_OK || rc == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK_EQ_INT(updates, 1);
    MOQ_TEST_CHECK_EQ_INT(children, 0);
    uint64_t queried = 0;
    MOQ_TEST_CHECK(goaway_owed(r, &p->request[0], &queried) == MOQ_OK);
    if (owed) MOQ_TEST_CHECK_EQ_U64(queried, *id);
    goaway_peer_events(r, parent, 0, 0, 0);
    MOQ_TEST_CHECK_EQ_INT(p->requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p->terminals, 0);
    return owed && children == 0;
}

static void test_goaway_reservation(bool publication, bool session_defer)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    cfg.max_fill_snapshot_bytes = 3;
    rig_open_ex(&r, &cfg, MOQ_VERSION_DRAFT_22, 6);
    moq_rcbuf_t *old = retain(&r, 3, "old"), *replacement = NULL;
    parent_t parent = establish(&r, r.track, publication, false);
    update_parent(&r, parent, true);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 3);
    discard_events(moq_simpair_client(r.pair));
    MOQ_TEST_CHECK(moq_session_set_defer_subscription_updates(
        moq_simpair_server(r.pair), session_defer) == MOQ_OK);
    uint64_t id = 0;
    if (!stage_goaway_update(&r, &p, parent, &id)) goto done;
    uint64_t processed = now(&r);
    for (unsigned i = 0; i < 3; i++) {
        MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
        MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 3);
    }
    size_t active = moq_pub_active_subscriptions(r.publisher, r.track);
    (void)request_ex(&r, NULL, true);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(moq_pub_active_subscriptions(r.publisher, r.track), active + 1);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    goaway_peer_events(&r, parent, 0, 0, 0);
    MOQ_TEST_CHECK(moq_pub_remove_track(r.publisher, r.track, now(&r)) == MOQ_ERR_WOULD_BLOCK);

    MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[0].fill, 6, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 3);
    goaway_peer_events(&r, parent, 0, 0, 0);

    moq_session_t *sv = moq_simpair_server(r.pair);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(sv, &ev, 1) == 1);
    MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_FILL_TERMINAL);
    moq_pub_event_result_t consumed;
    MOQ_TEST_CHECK(moq_pub_handle_event(r.publisher, &ev, now(&r), &consumed) == MOQ_OK);
    MOQ_TEST_CHECK(consumed == MOQ_PUB_EVENT_CONSUMED);
    moq_event_cleanup(&ev);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason, MOQ_FILL_LOCAL_ABORTED);
    MOQ_TEST_CHECK_EQ_U64(p.terminal[0].error_code, 6);

    /* Credit is now available, but ACK output is not. Reserve the old cache,
     * then replace it and advance Largest before the ACK can commit. */
    (void)fill_actions(sv, now(&r));
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 3);
    replacement = retain(&r, 4, "new");
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 2);
    MOQ_TEST_CHECK(goaway_owed(&r, &p.request[0], NULL) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    discard_actions(sv);
    MOQ_TEST_CHECK(moq_simpair_advance_to(r.pair, processed + 77) == MOQ_OK);
    p.defer = false;
    MOQ_TEST_CHECK(moq_pub_flush(r.publisher, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 2);
    MOQ_TEST_CHECK_EQ_U64(p.request[1].initiating_request_id, id);
    MOQ_TEST_CHECK_EQ_U64(p.request[1].processed_at_us, processed);
    MOQ_TEST_CHECK_EQ_U64(p.request[1].start_group, 3);
    MOQ_TEST_CHECK_EQ_U64(p.request[1].end_group, 3);
    MOQ_TEST_CHECK_EQ_INT(p.request[1].subscriber_priority, 11);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[1].reason, MOQ_FILL_COMPLETE);
    MOQ_TEST_CHECK(moq_pub_fill_eq(p.terminal[1].fill, p.request[1].fill));
    MOQ_TEST_CHECK_EQ_U64(p.terminal[1].initiating_request_id, id);
    MOQ_TEST_CHECK(goaway_owed(&r, &p.request[0], NULL) == MOQ_ERR_WRONG_STATE);
    goaway_peer_events(&r, parent, 1, 1, 0);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(old), 1);
    settle(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 2);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 2);
    goaway_peer_events(&r, parent, 0, 0, 0);
    /* Clean local finish remains forbidden after GOAWAY. Once the peer
     * cancels, neither the answered admission nor its terminal pins removal. */
    MOQ_TEST_CHECK(moq_pub_remove_track(r.publisher, r.track, now(&r)) == MOQ_ERR_WRONG_STATE);
    if (publication)
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(sv, r.publication_stream,
                                                       0, now(&r)) == MOQ_OK);
    else
        MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_client(r.pair),
                                                parent.sub, now(&r)) == MOQ_OK);
    settle_manual(&r);
    MOQ_TEST_CHECK(moq_pub_remove_track(r.publisher, r.track, now(&r)) == MOQ_OK);
    r.track = NULL;
done:
    moq_rcbuf_decref(old);
    if (replacement) moq_rcbuf_decref(replacement);
    rig_close(&r);
    MOQ_TEST_PASS(publication ? "goaway_publication_preserves_pre_ack_reservation"
                             : "goaway_subscription_preserves_pre_ack_reservation");
}

static void test_goaway_cancel(bool publication, bool abort_first)
{
    rig_t r;
    probe_t p = {.rig = &r, .defer = true};
    moq_pub_cfg_t cfg = probe_config(&p);
    cfg.max_fills = 1;
    rig_open(&r, &cfg);
    moq_rcbuf_t *buf = retain(&r, 3, "old");
    parent_t parent = establish(&r, r.track, publication, false);
    update_parent(&r, parent, true);
    settle_manual(&r);
    discard_events(moq_simpair_client(r.pair));
    uint64_t id = 0;
    if (!stage_goaway_update(&r, &p, parent, &id)) goto done;
    if (abort_first)
        MOQ_TEST_CHECK(moq_pub_abort_fill(r.publisher, p.request[0].fill, 6, now(&r)) == MOQ_OK);
    else {
        MOQ_TEST_CHECK(moq_pub_open_fill(r.publisher, p.request[0].fill, now(&r)) == MOQ_OK);
        MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    }
    if (publication)
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(moq_simpair_server(r.pair),
                         r.publication_stream, 0, now(&r)) == MOQ_OK);
    else
        MOQ_TEST_CHECK(moq_session_unsubscribe(moq_simpair_client(r.pair),
                                                parent.sub, now(&r)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(r.pair, 8, NULL) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 0);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 3);
    MOQ_TEST_CHECK(goaway_owed(&r, &p.request[0], NULL) == MOQ_ERR_STALE_HANDLE);
    moq_result_t rc = moq_pub_abort_fill(r.publisher, p.request[0].fill, 0, now(&r));
    MOQ_TEST_CHECK(rc == MOQ_ERR_STALE_HANDLE || rc == MOQ_ERR_WRONG_STATE);
    settle_manual(&r);
    MOQ_TEST_CHECK_EQ_INT(p.requests, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminals, 1);
    MOQ_TEST_CHECK_EQ_INT(p.terminal[0].reason,
        abort_first ? MOQ_FILL_LOCAL_ABORTED : MOQ_FILL_PARENT_TERMINATED);
    MOQ_TEST_CHECK_EQ_U64(p.terminal[0].initiating_request_id, p.request[0].initiating_request_id);
    if (abort_first) MOQ_TEST_CHECK_EQ_U64(p.terminal[0].error_code, 6);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 2);
    goaway_peer_events(&r, parent, 0, 0, 0);
    MOQ_TEST_CHECK(moq_pub_remove_track(r.publisher, r.track, now(&r)) == MOQ_OK);
    r.track = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_refcount(buf), 1);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(r.pair)) == MOQ_SESS_ESTABLISHED);
done:
    moq_rcbuf_decref(buf);
    rig_close(&r);
    MOQ_TEST_PASS(publication ? "goaway_publication_cancel_keeps_first_cause"
                             : "goaway_subscription_cancel_keeps_first_cause");
}

int main(void)
{
    test_snapshot_held_until_terminal();
    test_blocked_ack_snapshot(false);
    test_blocked_ack_snapshot(true);
    test_defer_and_credit(false);
    test_defer_and_credit(true);
    test_permanent_bytes(false);
    test_permanent_bytes(true);
    test_no_stream(false);
    test_no_stream(true);
    test_abort_pressure();
    test_parent_cause();
    test_foreign_owner();
    test_full_width_defer();
    test_update_snapshot(false);
    test_update_snapshot(true);
    test_batched_admission(false);
    test_batched_admission(true);
    for (unsigned origins = 0; origins < 4; origins++) {
        test_queued_updates(origins, false);
        test_queued_updates(origins, true);
    }
    test_sequential_updates(false);
    test_sequential_updates(true);
    test_authorizer_defer_progress();
    test_stale_generation(false);
    test_stale_generation(true);
    test_destination_allocation_retry();
    test_creation_allocations(MOQ_VERSION_DRAFT_16);
    test_creation_allocations(MOQ_VERSION_DRAFT_18);
    test_creation_allocations(MOQ_VERSION_DRAFT_22);
    test_early_subscribe_update();
    for (unsigned capacity = 1; capacity <= 3; capacity++)
        test_small_parent_capacity(capacity);
    test_cancel_unbound_reservation();
    test_destroy_waiting_admissions();
    test_publication_generation_reuse();
    for (unsigned origin = 0; origin < 2; origin++) {
        test_goaway_reservation(origin != 0, false);
        test_goaway_reservation(origin != 0, true);
        test_goaway_cancel(origin != 0, false);
        test_goaway_cancel(origin != 0, true);
    }
    printf("publisher fill ownership: %d failures\n", failures);
    return failures ? 1 : 0;
}
