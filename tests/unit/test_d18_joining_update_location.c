/*
 * Draft-18 sections 3.3.1 and 10.12.2: a Forward 0 -> 1 acknowledgement
 * supplies the Joining Location. Later history and a Forward 1 -> 1 refresh
 * do not replace that saved location. All state is established over SimPair
 * through public publisher/session APIs; no subscription-pool edits.
 */
#include <moq/publisher.h>
#include <moq/sim.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;
static unsigned setup_failures;

#define SETUP(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "SETUP FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        ++failures; \
        ++setup_failures; \
        goto cleanup; \
    } \
} while (0)

typedef struct allocation_count {
    int64_t live;
} allocation_count_t;

static void *count_alloc(size_t size, void *ctx)
{
    allocation_count_t *count = ctx;
    void *p = malloc(size);
    if (p != NULL) ++count->live;
    return p;
}

static void *count_realloc(void *p, size_t old_size, size_t size, void *ctx)
{
    allocation_count_t *count = ctx;
    (void)old_size;
    if (size == 0) {
        if (p != NULL) --count->live;
        free(p);
        return NULL;
    }
    bool was_null = p == NULL;
    void *next = realloc(p, size);
    if (next != NULL && was_null) ++count->live;
    return next;
}

static void count_free(void *p, size_t size, void *ctx)
{
    allocation_count_t *count = ctx;
    (void)size;
    if (p != NULL) --count->live;
    free(p);
}

typedef struct joining_fixture {
    allocation_count_t allocations;
    moq_alloc_t alloc;
    moq_simpair_t *pair;
    moq_session_t *client_session;
    moq_publisher_t *publisher;
    moq_pub_track_t *track;
    moq_subscription_t client_sub, server_sub;
    moq_fetch_t fetch;
    unsigned subscribe_oks;
    bool initial_has_largest;
    uint64_t initial_group, initial_object;
    unsigned updates, update_oks, forward_zero, forward_one;
    bool ack_has_largest;
    uint64_t ack_group, ack_object;
    unsigned live_fresh, live_later;
    unsigned requests, oks, objects, completes, errors, invalid_ranges;
    unsigned closed, other_errors;
    uint64_t request_start_group, request_start_object;
    uint64_t request_end_group, request_end_object;
    uint64_t ok_end_group, ok_end_object;
    uint64_t object_group, object_id;
    bool fetch_owner_matches, payload_matches;
    uint64_t expected_group;
    const char *expected_payload;
    bool manual_updates;
    uint64_t update_id;
    unsigned blocked_acks, done;
    uint64_t done_status;
    bool rejected_seen, rejected_has_largest, rejected_forward, rejected_pending;
    uint64_t rejected_group, rejected_object;
} joining_fixture_t;

/* Read-only observers: no queue, subscription or history state is fabricated. */
static const moq_sub_entry_t *entry_for(const moq_session_t *s, moq_subscription_t sub)
{
    if (!s) return NULL;
    for (size_t i = 0; i < s->sub_cap; ++i)
        if (s->subs[i].state != MOQ_SUB_FREE &&
            moq_subscription_eq(s->subs[i].handle, sub)) return &s->subs[i];
    return NULL;
}

static void trace_ack(void *ctx, const moq_sim_trace_record_t *r)
{
    joining_fixture_t *f = ctx;
    if (r->kind != MOQ_SIM_TRACE_INPUT || r->to != MOQ_PERSPECTIVE_CLIENT ||
        r->input_kind != MOQ_SIM_INPUT_BIDI_BYTES ||
        r->struct_size < offsetof(moq_sim_trace_record_t, stream_ref) + sizeof(r->stream_ref))
        return;
    const moq_sub_entry_t *e = entry_for(f->client_session, f->client_sub);
    if (!e || e->rs.ref._v != r->stream_ref._v) return;
    if (r->result == MOQ_ERR_WOULD_BLOCK) ++f->blocked_acks;
    /* REQUEST_ERROR is delivered before the separate PUBLISH_DONE action.
     * Observe its effect without reentering SimPair or advancing a session. */
    if (r->result == MOQ_OK && e->update_failed) {
        f->rejected_seen = true;
        f->rejected_has_largest = e->has_largest;
        f->rejected_group = e->largest_group;
        f->rejected_object = e->largest_object;
        f->rejected_forward = e->forward;
        f->rejected_pending = e->update_pending;
    }
}

static uint64_t now(const joining_fixture_t *f)
{
    return moq_simpair_now_us(f->pair);
}

static bool payload_is(const moq_rcbuf_t *payload, const char *expected)
{
    if (expected == NULL) return false;
    size_t len = strlen(expected);
    return payload != NULL && moq_rcbuf_len(payload) == len &&
           memcmp(moq_rcbuf_data(payload), expected, len) == 0;
}

static void observe_client(joining_fixture_t *f, const moq_event_t *event)
{
    switch (event->kind) {
    case MOQ_EVENT_SUBSCRIBE_OK:
        ++f->subscribe_oks;
        MOQ_TEST_CHECK(moq_subscription_eq(event->u.subscribe_ok.sub, f->client_sub));
        f->initial_has_largest = event->u.subscribe_ok.has_largest;
        f->initial_group = event->u.subscribe_ok.largest_group;
        f->initial_object = event->u.subscribe_ok.largest_object;
        break;
    case MOQ_EVENT_SUBSCRIPTION_UPDATE_OK:
        ++f->update_oks;
        MOQ_TEST_CHECK(moq_subscription_eq(event->u.subscription_update_ok.sub,
                                           f->client_sub));
        f->ack_has_largest = event->u.subscription_update_ok.has_largest;
        f->ack_group = event->u.subscription_update_ok.largest_group;
        f->ack_object = event->u.subscription_update_ok.largest_object;
        break;
    case MOQ_EVENT_OBJECT_RECEIVED:
        MOQ_TEST_CHECK(moq_subscription_eq(event->u.object_received.sub, f->client_sub));
        if (event->u.object_received.group_id == 4) {
            ++f->live_fresh;
            MOQ_TEST_CHECK(event->u.object_received.object_id == 0);
            MOQ_TEST_CHECK(payload_is(event->u.object_received.payload, "fresh"));
        } else if (event->u.object_received.group_id == 6) {
            ++f->live_later;
            MOQ_TEST_CHECK(event->u.object_received.object_id == 0);
            MOQ_TEST_CHECK(payload_is(event->u.object_received.payload, "later"));
        } else {
            MOQ_TEST_CHECK(false);
        }
        break;
    case MOQ_EVENT_FETCH_OK:
        ++f->oks;
        MOQ_TEST_CHECK(moq_fetch_eq(event->u.fetch_ok.fetch, f->fetch));
        f->ok_end_group = event->u.fetch_ok.end_group;
        f->ok_end_object = event->u.fetch_ok.end_object;
        break;
    case MOQ_EVENT_FETCH_OBJECT:
        ++f->objects;
        MOQ_TEST_CHECK(moq_fetch_eq(event->u.fetch_object.fetch, f->fetch));
        f->object_group = event->u.fetch_object.group_id;
        f->object_id = event->u.fetch_object.object_id;
        f->payload_matches = payload_is(event->u.fetch_object.payload,
                                         f->expected_payload);
        break;
    case MOQ_EVENT_FETCH_COMPLETE:
        ++f->completes;
        MOQ_TEST_CHECK(moq_fetch_eq(event->u.fetch_complete.fetch, f->fetch));
        break;
    case MOQ_EVENT_FETCH_ERROR:
        ++f->errors;
        f->invalid_ranges += event->u.fetch_error.error_code == MOQ_REQUEST_ERROR_INVALID_RANGE;
        MOQ_TEST_CHECK(moq_fetch_eq(event->u.fetch_error.fetch, f->fetch));
        break;
    case MOQ_EVENT_SESSION_CLOSED:
        ++f->closed;
        break;
    case MOQ_EVENT_SUBSCRIBE_ERROR:
    case MOQ_EVENT_FETCH_RESET:
        ++f->other_errors;
        break;
    case MOQ_EVENT_SUBSCRIBE_DONE:
        ++f->done;
        f->done_status = event->u.subscribe_done.status_code;
        ++f->other_errors;
        break;
    default:
        break;
    }
}

/* Inspect actual inbound requests before handing each event to its facade
 * owner. The matching client events are decoded from the peer's real output. */
static bool drive(joining_fixture_t *f)
{
    for (unsigned round = 0; round < 12; ++round) {
        if (moq_simpair_run_until_quiescent(f->pair, 16, NULL) != MOQ_OK) return false;
        moq_result_t rc = moq_pub_flush(f->publisher, now(f));
        if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) return false;
        moq_event_t event;
        while (moq_session_poll_events(moq_simpair_server(f->pair), &event, 1)) {
            if (event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                f->server_sub = event.u.subscribe_request.sub;
                MOQ_TEST_CHECK(event.u.subscribe_request.filter ==
                               MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
            } else if (event.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
                ++f->updates;
                f->update_id = event.u.subscribe_updated.update_request_id;
                MOQ_TEST_CHECK(moq_subscription_eq(event.u.subscribe_updated.sub,
                                                   f->server_sub));
                MOQ_TEST_CHECK(event.u.subscribe_updated.has_forward);
                if (event.u.subscribe_updated.forward) ++f->forward_one;
                else ++f->forward_zero;
            } else if (event.kind == MOQ_EVENT_FETCH_REQUEST) {
                const moq_fetch_request_event_t *request = &event.u.fetch_request;
                ++f->requests;
                f->fetch_owner_matches = moq_subscription_eq(request->joining_sub,
                                                              f->server_sub) &&
                                        !moq_publication_is_valid(request->joining_pub);
                f->request_start_group = request->start_group;
                f->request_start_object = request->start_object;
                f->request_end_group = request->end_group;
                f->request_end_object = request->end_object;
            } else if (event.kind == MOQ_EVENT_SESSION_CLOSED) {
                ++f->closed;
            }
            if (f->manual_updates) {
                /* Session APIs own these requests. The facade only supplies
                 * real retained data to the shared track history. */
                if (event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                    moq_accept_subscribe_cfg_t accept;
                    moq_accept_subscribe_cfg_init(&accept);
                    rc = moq_session_accept_subscribe(moq_simpair_server(f->pair),
                                                      f->server_sub, &accept, now(f));
                } else {
                    if (event.kind == MOQ_EVENT_SUBSCRIBE_UPDATED)
                        MOQ_TEST_CHECK(event.u.subscribe_updated.answer_owed);
                    rc = MOQ_OK;
                }
                moq_event_cleanup(&event);
                if (rc != MOQ_OK) return false;
                continue;
            }
            moq_pub_event_result_t result;
            rc = moq_pub_handle_event(f->publisher, &event, now(f), &result);
            moq_event_cleanup(&event);
            if (rc != MOQ_OK && rc != MOQ_ERR_WOULD_BLOCK) return false;
        }
        while (moq_session_poll_events(moq_simpair_client(f->pair), &event, 1)) {
            observe_client(f, &event);
            moq_event_cleanup(&event);
        }
    }
    return true;
}

static bool install_retained(joining_fixture_t *f, uint64_t group, const char *body)
{
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&f->alloc, (const uint8_t *)body, strlen(body),
                         &payload) != MOQ_OK) return false;
    moq_pub_retained_object_t object = { .object_id = 0, .payload = payload };
    moq_pub_retained_group_cfg_t retained;
    moq_pub_retained_group_cfg_init(&retained);
    retained.group_id = group;
    retained.objects = &object;
    retained.object_count = 1;
    moq_result_t rc = moq_pub_set_retained_group(f->publisher, f->track, &retained);
    moq_rcbuf_decref(payload);
    return rc == MOQ_OK;
}

static bool publish_object(joining_fixture_t *f, uint64_t group, const char *body)
{
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&f->alloc, (const uint8_t *)body, strlen(body),
                         &payload) != MOQ_OK) return false;
    moq_result_t rc = moq_pub_write_object(f->publisher, f->track, group, 0,
                                           payload, now(f));
    moq_rcbuf_decref(payload);
    return rc == MOQ_OK;
}

static bool update_forward(joining_fixture_t *f, bool forward)
{
    moq_subscription_update_cfg_t update;
    moq_subscription_update_cfg_init_sized(&update, sizeof(update));
    update.has_forward = true;
    update.forward = forward;
    if (moq_session_update_subscription(moq_simpair_client(f->pair), f->client_sub,
                                        &update, now(f)) != MOQ_OK) return false;
    unsigned before = f->update_oks;
    if (!drive(f)) return false;
    return f->update_oks == before + 1 && f->ack_has_largest &&
           f->ack_group == 4 && f->ack_object == 0;
}

static bool fixture_open_options(joining_fixture_t *f, bool initially_nonempty,
                                  moq_version_t version, bool manual_updates)
{
    memset(f, 0, sizeof(*f));
    f->manual_updates = manual_updates;
    f->alloc = (moq_alloc_t){ .ctx = &f->allocations, .alloc = count_alloc,
                             .realloc = count_realloc, .free = count_free };
    moq_simpair_cfg_t sim = MOQ_SIMPAIR_CFG_INIT;
    sim.alloc = &f->alloc;
    sim.version = version;
    sim.initial_now_us = 1000;
    sim.max_actions = manual_updates ? 8 : 64;
    sim.max_events = manual_updates ? 4 : 64;
    sim.server_defer_subscription_updates = manual_updates;
    sim.trace_fn = trace_ack;
    sim.trace_ctx = f;
    sim.client_send_request_capacity = true;
    sim.client_initial_request_capacity = 32;
    sim.server_send_request_capacity = true;
    sim.server_initial_request_capacity = 32;
    if (moq_simpair_create(&sim, &f->pair) != MOQ_OK) return false;
    f->client_session = moq_simpair_client(f->pair);
    if (moq_simpair_start(f->pair) != MOQ_OK ||
        moq_simpair_run_until_quiescent(f->pair, 16, NULL) != MOQ_OK) return false;
    moq_event_t event;
    while (moq_session_poll_events(moq_simpair_client(f->pair), &event, 1))
        moq_event_cleanup(&event);
    while (moq_session_poll_events(moq_simpair_server(f->pair), &event, 1))
        moq_event_cleanup(&event);
    moq_pub_cfg_t cfg;
    moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
    if (moq_pub_create(moq_simpair_server(f->pair), &f->alloc, &cfg,
                        &f->publisher) != MOQ_OK) return false;
    moq_bytes_t part = MOQ_BYTES_LITERAL("joining-location");
    moq_pub_track_cfg_t track;
    moq_pub_track_cfg_init_sized(&track, sizeof(track));
    track.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    track.track_name = MOQ_BYTES_LITERAL("video");
    if (moq_pub_add_track(f->publisher, &track, now(f), &f->track) != MOQ_OK) return false;
    if (initially_nonempty && !install_retained(f, 2, "old")) return false;
    moq_subscribe_cfg_t request;
    moq_subscribe_cfg_init(&request);
    request.track_namespace = track.track_namespace;
    request.track_name = track.track_name;
    request.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    request.has_forward = true;
    request.forward = true;
    return moq_session_subscribe(moq_simpair_client(f->pair), &request, now(f),
                                  &f->client_sub) == MOQ_OK && drive(f);
}

static bool fixture_open(joining_fixture_t *f, bool initially_nonempty)
{
    return fixture_open_options(f, initially_nonempty, MOQ_VERSION_DRAFT_18, false);
}

static void fixture_close(joining_fixture_t *f)
{
    moq_pub_destroy(f->publisher);
    moq_simpair_destroy(f->pair);
}

static void run_case(const char *name, bool initially_nonempty, bool resume)
{
    int before = failures;
    joining_fixture_t f;
    bool opened = fixture_open(&f, initially_nonempty);
    printf("CASE %s\n", name);
    SETUP(opened);
    SETUP(f.subscribe_oks == 1 && f.initial_has_largest == initially_nonempty);
    if (initially_nonempty) SETUP(f.initial_group == 2 && f.initial_object == 0);
    SETUP(moq_subscription_is_valid(f.server_sub));
    SETUP(moq_pub_active_subscriptions(f.publisher, f.track) == 1);
    SETUP(publish_object(&f, 4, "fresh") && drive(&f));
    SETUP(f.live_fresh == 1 && f.closed == 0 && f.other_errors == 0);
    if (resume) {
        SETUP(install_retained(&f, 4, "fresh"));
        SETUP(update_forward(&f, false));
        SETUP(f.forward_zero == 1 && f.update_oks == 1);
    }
    /* On a same-state refresh the ACK may advertise new history, but it is
     * not a 0 -> 1 transition and must not replace the initial Joining Location. */
    SETUP(update_forward(&f, true));
    SETUP(f.forward_one == 1 && f.updates == (resume ? 2u : 1u));
    SETUP(f.update_oks == f.updates);
    /* Make a live-history shortcut wrong too: the joining end is the ACK's
     * group 4, not the later group 6. The retained target stays unchanged. */
    SETUP(publish_object(&f, 6, "later") && drive(&f));
    SETUP(f.live_later == 1 && f.closed == 0 && f.other_errors == 0);
    f.expected_group = resume ? 4 : 2;
    f.expected_payload = resume ? "fresh" : "old";
    moq_fetch_cfg_t fetch;
    moq_fetch_cfg_init_sized(&fetch, sizeof(fetch));
    fetch.is_joining = true;
    fetch.joining_relative = true;
    fetch.joining_start = 0;
    fetch.joining_sub = f.client_sub;
    moq_result_t rc = moq_session_fetch(moq_simpair_client(f.pair), &fetch,
                                         now(&f), &f.fetch);
    printf("joining-fetch command=%d ack-largest=(%llu,%llu) initial-present=%d\n",
           (int)rc, (unsigned long long)f.ack_group,
           (unsigned long long)f.ack_object, f.initial_has_largest ? 1 : 0);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    /* Do not invent a Largest to bypass an outbound refusal. Keep that
     * refusal visible; the nonempty row independently reaches the wire. */
    if (rc != MOQ_OK) goto cleanup;
    SETUP(drive(&f));
    printf("joining-fetch requests=%u range=(%llu,%llu)-(%llu,%llu) "
           "ok=%u objects=%u complete=%u errors=%u invalid-range=%u\n",
           f.requests, (unsigned long long)f.request_start_group,
           (unsigned long long)f.request_start_object,
           (unsigned long long)f.request_end_group,
           (unsigned long long)f.request_end_object, f.oks, f.objects,
           f.completes, f.errors, f.invalid_ranges);
    MOQ_TEST_CHECK(f.requests == 1 && f.fetch_owner_matches);
    MOQ_TEST_CHECK(f.request_start_group == f.expected_group && f.request_start_object == 0);
    MOQ_TEST_CHECK(f.request_end_group == f.expected_group && f.request_end_object == 1);
    MOQ_TEST_CHECK(f.oks == 1 && f.objects == 1 && f.completes == 1);
    MOQ_TEST_CHECK(f.errors == 0 && f.invalid_ranges == 0);
    MOQ_TEST_CHECK(f.ok_end_group == f.expected_group && f.ok_end_object == 1);
    MOQ_TEST_CHECK(f.object_group == f.expected_group && f.object_id == 0 && f.payload_matches);
    MOQ_TEST_CHECK(f.closed == 0 && f.other_errors == 0);
    SETUP(drive(&f));
    MOQ_TEST_CHECK(f.requests == 1 && f.oks == 1 && f.objects == 1 && f.completes == 1);
    MOQ_TEST_CHECK(f.update_oks == (resume ? 2u : 1u) && f.errors == 0);
cleanup:
    if (opened) {
        MOQ_TEST_CHECK(moq_pub_active_subscriptions(f.publisher, f.track) == 1);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_client(f.pair)) == MOQ_SESS_ESTABLISHED);
        MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(f.pair)) == MOQ_SESS_ESTABLISHED);
    }
    fixture_close(&f);
    MOQ_TEST_CHECK(f.allocations.live == 0);
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", name);
}

static void check_basis(const moq_session_t *s, moq_subscription_t sub,
                        uint64_t group, bool forward)
{
    const moq_sub_entry_t *e = entry_for(s, sub);
    MOQ_TEST_CHECK(e != NULL);
    if (!e) return;
    MOQ_TEST_CHECK(e->has_largest && e->largest_group == group && e->largest_object == 0);
    MOQ_TEST_CHECK(e->forward == forward);
}

static void check_both(joining_fixture_t *f, uint64_t group, bool forward)
{
    check_basis(moq_simpair_server(f->pair), f->server_sub, group, forward);
    check_basis(f->client_session, f->client_sub, group, forward);
}

static bool manual_submit(joining_fixture_t *f, bool forward)
{
    moq_subscription_update_cfg_t cfg;
    moq_subscription_update_cfg_init_sized(&cfg, sizeof(cfg));
    cfg.has_forward = true;
    cfg.forward = forward;
    unsigned updates = f->updates, acks = f->update_oks;
    return moq_session_update_subscription(f->client_session, f->client_sub,
                                            &cfg, now(f)) == MOQ_OK && drive(f) &&
        f->updates == updates + 1 && f->update_oks == acks;
}

static moq_result_t manual_accept(joining_fixture_t *f)
{
    moq_accept_subscription_update_cfg_t cfg;
    moq_accept_subscription_update_cfg_init(&cfg);
    cfg.update_request_id = f->update_id;
    return moq_session_accept_subscription_update(moq_simpair_server(f->pair),
                                                 f->server_sub, &cfg, now(f));
}

static bool manual_update(joining_fixture_t *f, bool forward)
{
    unsigned acks = f->update_oks;
    return manual_submit(f, forward) && manual_accept(f) == MOQ_OK && drive(f) &&
           f->update_oks == acks + 1;
}

/* One real TRACK_STATUS request occupies one sender action and, after
 * delivery, one receiver event. Requests use a different bidi from the ACK.
 * They need no answer in these cases and are released on session teardown. */
static bool queue_status_requests(joining_fixture_t *f, size_t count)
{
    moq_bytes_t ns = MOQ_BYTES_LITERAL("pressure");
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    cfg.track_namespace = (moq_namespace_t){ .parts = &ns, .count = 1 };
    cfg.track_name = MOQ_BYTES_LITERAL("video");
    for (size_t i = 0; i < count; ++i) {
        moq_track_status_handle_t handle;
        if (moq_session_track_status(moq_simpair_server(f->pair), &cfg, now(f),
                                     &handle) != MOQ_OK) return false;
    }
    return true;
}

static void check_manual_alive(joining_fixture_t *f)
{
    MOQ_TEST_CHECK(f->closed == 0 && f->other_errors == 0);
    MOQ_TEST_CHECK(moq_session_state(f->client_session) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(f->pair)) == MOQ_SESS_ESTABLISHED);
}

static void run_publisher_ack_pressure(void)
{
    const char *name = "publisher-ack-pressure-retry-snapshot";
    int before = failures;
    joining_fixture_t f;
    bool opened = fixture_open_options(&f, true, MOQ_VERSION_DRAFT_18, true);
    printf("CASE %s\n", name);
    SETUP(opened);
    moq_session_t *server = moq_simpair_server(f.pair);
    SETUP(f.subscribe_oks == 1 && f.initial_has_largest && f.initial_group == 2);
    SETUP(install_retained(&f, 4, "fresh") && manual_update(&f, false));
    check_both(&f, 2, false);
    SETUP(manual_submit(&f, true));
    check_both(&f, 2, false);
    SETUP(queue_status_requests(&f, server->action_cap));
    SETUP(server->action_tail - server->action_head == server->action_cap);
    size_t queued = server->action_tail - server->action_head;
    MOQ_TEST_CHECK(manual_accept(&f) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(manual_accept(&f) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(server->action_tail - server->action_head == queued);
    check_both(&f, 2, false);
    const moq_sub_entry_t *e = entry_for(server, f.server_sub);
    MOQ_TEST_CHECK(e && e->owed_update.owed && e->owed_update.count == 1);
    MOQ_TEST_CHECK(f.update_oks == 1);
    /* The failed attempt commits nothing. A successful retry advertises the
     * new processing snapshot, not the history at the failed attempt. */
    SETUP(install_retained(&f, 6, "retry") && drive(&f));
    check_both(&f, 2, false);
    SETUP(manual_accept(&f) == MOQ_OK);
    check_basis(server, f.server_sub, 6, true);
    check_basis(f.client_session, f.client_sub, 2, false);
    SETUP(install_retained(&f, 8, "later") && drive(&f));
    MOQ_TEST_CHECK(f.update_oks == 2 && f.ack_has_largest &&
                   f.ack_group == 6 && f.ack_object == 0);
    check_both(&f, 6, true);
    e = entry_for(server, f.server_sub);
    MOQ_TEST_CHECK(e && !e->owed_update.owed && e->hist && e->hist->largest_group == 8);
    MOQ_TEST_CHECK(manual_accept(&f) == MOQ_ERR_WRONG_STATE);
    SETUP(drive(&f));
    MOQ_TEST_CHECK(f.update_oks == 2 && f.updates == 2);
    check_manual_alive(&f);
cleanup:
    fixture_close(&f);
    MOQ_TEST_CHECK(f.allocations.live == 0);
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", name);
}

static void run_receiver_ack_pressure(void)
{
    const char *name = "receiver-ack-event-pressure-retry";
    int before = failures;
    joining_fixture_t f;
    bool opened = fixture_open_options(&f, true, MOQ_VERSION_DRAFT_18, true);
    printf("CASE %s\n", name);
    SETUP(opened);
    moq_session_t *server = moq_simpair_server(f.pair);
    moq_session_t *client = f.client_session;
    SETUP(install_retained(&f, 4, "fresh") && manual_update(&f, false));
    SETUP(manual_submit(&f, true));
    check_both(&f, 2, false);
    SETUP(queue_status_requests(&f, client->event_cap));
    SETUP(moq_simpair_run_until_quiescent(f.pair, 16, NULL) == MOQ_OK);
    SETUP(client->event_tail - client->event_head == client->event_cap);
    f.blocked_acks = 0;
    SETUP(manual_accept(&f) == MOQ_OK);
    check_basis(server, f.server_sub, 4, true);
    /* Let SimPair deliver and retry the real ACK while the event queue stays
     * full. Its trace must confirm refusal on this subscription's bidi. */
    SETUP(moq_simpair_step(f.pair, NULL) == MOQ_OK);
    SETUP(moq_simpair_step(f.pair, NULL) == MOQ_OK);
    MOQ_TEST_CHECK(f.blocked_acks > 0);
    check_basis(client, f.client_sub, 2, false);
    const moq_sub_entry_t *e = entry_for(client, f.client_sub);
    MOQ_TEST_CHECK(e && e->update_pending && e->update_has_forward && e->update_forward);
    MOQ_TEST_CHECK(f.update_oks == 1);
    MOQ_TEST_CHECK(client->event_tail - client->event_head == client->event_cap);
    SETUP(install_retained(&f, 6, "later") && drive(&f));
    MOQ_TEST_CHECK(f.update_oks == 2 && f.ack_has_largest &&
                   f.ack_group == 4 && f.ack_object == 0);
    check_both(&f, 4, true);
    e = entry_for(client, f.client_sub);
    MOQ_TEST_CHECK(e && !e->update_pending && !e->update_has_forward);
    SETUP(drive(&f));
    MOQ_TEST_CHECK(f.update_oks == 2 && f.updates == 2);
    check_manual_alive(&f);
cleanup:
    fixture_close(&f);
    MOQ_TEST_CHECK(f.allocations.live == 0);
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", name);
}

static void run_rejected_resume(void)
{
    const char *name = "rejected-resume-keeps-basis-until-terminal";
    int before = failures;
    joining_fixture_t f;
    bool opened = fixture_open_options(&f, true, MOQ_VERSION_DRAFT_18, true);
    printf("CASE %s\n", name);
    SETUP(opened);
    moq_session_t *server = moq_simpair_server(f.pair);
    SETUP(install_retained(&f, 4, "fresh") && manual_update(&f, false));
    SETUP(manual_submit(&f, true));
    check_both(&f, 2, false);
    moq_reject_subscription_update_cfg_t reject;
    moq_reject_subscription_update_cfg_init(&reject);
    reject.update_request_id = f.update_id;
    reject.error_code = MOQ_REQUEST_ERROR_REDIRECT;
    MOQ_TEST_CHECK(moq_session_reject_subscription_update(server, f.server_sub,
                                                          &reject, now(&f)) == MOQ_ERR_INVAL);
    check_both(&f, 2, false);
    reject.error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
    SETUP(moq_session_reject_subscription_update(server, f.server_sub,
                                                  &reject, now(&f)) == MOQ_OK);
    MOQ_TEST_CHECK(entry_for(server, f.server_sub) == NULL);
    check_basis(f.client_session, f.client_sub, 2, false);
    SETUP(drive(&f));
    MOQ_TEST_CHECK(f.rejected_seen && f.rejected_has_largest);
    MOQ_TEST_CHECK(f.rejected_group == 2 && f.rejected_object == 0);
    MOQ_TEST_CHECK(!f.rejected_forward && !f.rejected_pending);
    MOQ_TEST_CHECK(f.done == 1 && f.done_status == 0x8); /* UPDATE_FAILED */
    MOQ_TEST_CHECK(f.update_oks == 1 && f.updates == 2 && f.other_errors == 1);
    MOQ_TEST_CHECK(entry_for(f.client_session, f.client_sub) == NULL);
    MOQ_TEST_CHECK(manual_accept(&f) == MOQ_ERR_STALE_HANDLE);
    SETUP(drive(&f));
    MOQ_TEST_CHECK(f.done == 1 && f.update_oks == 1 && f.closed == 0);
    MOQ_TEST_CHECK(moq_session_state(f.client_session) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(server) == MOQ_SESS_ESTABLISHED);
cleanup:
    fixture_close(&f);
    MOQ_TEST_CHECK(f.allocations.live == 0);
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", name);
}

static void run_other_profile(moq_version_t version, const char *name)
{
    int before = failures;
    joining_fixture_t f;
    bool opened = fixture_open_options(&f, true, version, true);
    printf("CASE %s\n", name);
    SETUP(opened);
    SETUP(f.subscribe_oks == 1 && f.initial_has_largest && f.initial_group == 2);
    SETUP(install_retained(&f, 4, "fresh"));
    SETUP(manual_update(&f, false));
    check_both(&f, 2, false);
    SETUP(manual_update(&f, true));
    MOQ_TEST_CHECK(f.update_oks == 2 && f.ack_has_largest &&
                   f.ack_group == 4 && f.ack_object == 0);
    check_both(&f, 2, true);
    SETUP(drive(&f));
    MOQ_TEST_CHECK(f.update_oks == 2 && f.updates == 2);
    check_manual_alive(&f);
cleanup:
    fixture_close(&f);
    MOQ_TEST_CHECK(f.allocations.live == 0);
    printf("%s %s\n", failures == before ? "PASS" : "FAIL", name);
}

int main(void)
{
    run_case("empty-basis-resume-ack-largest", false, true);
    run_case("advanced-basis-resume-ack-largest", true, true);
    run_case("same-state-refresh-keeps-joining-location", true, false);
    run_publisher_ack_pressure();
    run_receiver_ack_pressure();
    run_rejected_resume();
    run_other_profile(MOQ_VERSION_DRAFT_16, "d16-resume-keeps-initial-basis");
    run_other_profile(MOQ_VERSION_DRAFT_22, "d22-resume-keeps-initial-basis");
    printf("joining-location: failures=%d setup-failures=%u\n", failures, setup_failures);
    return failures ? 1 : 0;
}
