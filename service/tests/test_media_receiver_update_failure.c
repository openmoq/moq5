/* Selected error rows inject only the bad track's UPDATE return. Native probe
 * rows instead refuse a real core allocation or exercise ordinary pressure;
 * the retry/ACK controls also use the native implementation. */
#include <moq/subscriber.h>
#include "../../core/src/session/session_internal.h"
static moq_result_t update_boundary(moq_subscriber_t *, moq_sub_track_t *,
    const moq_sub_update_cfg_t *, uint64_t);
#define moq_sub_update_subscription update_boundary
#define main receiver_update_ack_controls_main
#include "test_media_receiver_ack_failure.c"
#undef main
#undef moq_sub_update_subscription

/* An independent event value also permits running this fixture against a
 * receiver that does not yet report UPDATE admission failures. */
enum { UPDATE_LOCAL_FAILURE = 11, UPDATE_REPEATS = 4 };

#define UPDATE_NEED(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; goto done; \
    } \
} while (0)

static struct {
    moq_subscriber_t *sub;
    moq_sub_track_t *track;
    moq_result_t inject, last;
    unsigned attempts, admissions, injected;
} update_probe;

static moq_result_t update_boundary(moq_subscriber_t *sub, moq_sub_track_t *track,
    const moq_sub_update_cfg_t *cfg, uint64_t now_us)
{
    if (sub != update_probe.sub || track != update_probe.track)
        return moq_sub_update_subscription(sub, track, cfg, now_us);
    update_probe.attempts++;
    if (update_probe.inject != MOQ_OK) {
        update_probe.injected++;
        return update_probe.last = update_probe.inject;
    }
    moq_result_t rc = moq_sub_update_subscription(sub, track, cfg, now_us);
    update_probe.last = rc;
    if (rc == MOQ_OK) update_probe.admissions++;
    return rc;
}

typedef struct update_events {
    unsigned added, ready, failures, ends, acks;
    moq_result_t reason;
} update_events_t;

static void update_drain(fixture_t *f, update_events_t *seen)
{
    for (unsigned i = 0; i < 32; i++) {
        moq_media_track_event_t event;
        memset(&event, 0xa5, sizeof(event));
        moq_result_t rc = moq_media_receiver_poll_track(f->r, &event, sizeof(event));
        if (rc != MOQ_OK) { MOQ_TEST_CHECK(rc == MOQ_DONE); return; }
        if (event.kind == MOQ_MEDIA_TRACK_ADDED) seen->added++;
        if (event.kind == MOQ_MEDIA_CATALOG_READY) seen->ready++;
        if (event.kind == MOQ_MEDIA_TRACK_ENDED) seen->ends++;
        if (event.kind == MOQ_MEDIA_TRACK_UPDATE_OK) {
            seen->acks++;
            MOQ_TEST_CHECK(event.track == f->bad);
        }
        if ((unsigned)event.kind == UPDATE_LOCAL_FAILURE) {
            seen->failures++;
            MOQ_TEST_CHECK(seen->added == 2 && seen->ready == 1);
            moq_media_track_event_t expected;
            memset(&expected, 0, sizeof(expected));
            expected.struct_size = sizeof(expected);
            expected.kind = (moq_media_track_event_kind_t)UPDATE_LOCAL_FAILURE;
            expected.track = f->bad;
            expected.desc = &f->bad->desc;
            expected.local_result = seen->reason;
            /* Exact result, identity, stamped extent and zero inactive tails;
             * no UPDATE_OK, peer-end or delivery/ACK-failure projection. */
            MOQ_TEST_CHECK(memcmp(&event, &expected, sizeof(event)) == 0);
        } else {
            MOQ_TEST_CHECK(event.local_result == MOQ_OK);
            MOQ_TEST_CHECK((unsigned)event.kind != LOCAL_FAILURE &&
                event.kind != MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE);
        }
    }
    MOQ_TEST_CHECK(false); /* A bounded fixture must drain in this budget. */
}

static bool update_start(fixture_t *f, moq_version_t version,
    update_events_t *seen, bool keep_discovery)
{
    memset(&update_probe, 0, sizeof(update_probe));
    if (!start(f, version, 0) || !bootstrap(f)) return false;
    if (!keep_discovery) update_drain(f, seen);
    accept(f, 1, true);
    accept(f, 2, false);
    settle(f);
    update_probe.sub = f->r->sub;
    update_probe.track = f->probe;
    return f->bad->established && f->good->established;
}

static void update_state(fixture_t *f, moq_media_track_t *track,
    moq_media_track_state_t expected)
{
    moq_media_track_state_t state = MOQ_MEDIA_TRACK_STATE_DISCOVERED;
    MOQ_TEST_CHECK(moq_media_receiver_track_state(f->r, track, &state) == MOQ_OK);
    MOQ_TEST_CHECK(state == expected);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(f->r));
    MOQ_TEST_CHECK(!moq_endpoint_is_closed(f->ep) && !moq_endpoint_is_fatal(f->ep));
    MOQ_TEST_CHECK(moq_session_state(client(f)) == MOQ_SESS_ESTABLISHED);
    MOQ_TEST_CHECK(moq_session_state(server(f)) == MOQ_SESS_ESTABLISHED);
}

static void update_good(fixture_t *f)
{
    update_state(f, f->good, MOQ_MEDIA_TRACK_STATE_ACTIVE);
    send_payload(f, 2, 1, "good", 4, true);
    settle(f);
    moq_media_object_t object = {0};
    moq_result_t rc = moq_media_receiver_poll_object(f->r, &object, sizeof(object));
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK(object.track == f->good && object.payload.len == 4);
        MOQ_TEST_CHECK(memcmp(object.payload.data, "good", 4) == 0);
        MOQ_TEST_CHECK(object.has_capture_time && object.capture_time_us == 12345);
    }
    moq_media_object_cleanup(&object);
}

static void update_finish(fixture_t *f, moq_media_object_t *held)
{
    memset(&update_probe, 0, sizeof(update_probe));
    /* Prove the app's transferred refs survive queue purge AND owner teardown
     * before the inherited allocator balance check. Do not dereference track. */
    if (held->payload_ref) {
        f->probe = NULL;
        /* Release deliberate action pressure before the synchronous test
         * destructor, which cannot requeue a blocked endpoint teardown task. */
        if (f->pair) network(f);
        if (f->r) {
            f->r->ep = NULL;
            moq_media_receiver_test_destroy_with_session(f->r, client(f), now(f));
            f->r = NULL;
        }
        if (f->pair) { moq_simpair_destroy(f->pair); f->pair = NULL; }
        MOQ_TEST_CHECK(held->payload.len == 4 &&
            memcmp(held->payload.data, "held", 4) == 0 && held->properties_ref);
        MOQ_TEST_CHECK((held->track_properties_ref != NULL) ==
            (f->version == MOQ_VERSION_DRAFT_22));
        moq_media_object_cleanup(held);
        moq_media_object_cleanup(held);
    }
    finish(f);
}

static void update_failure(moq_version_t version, moq_result_t reason, bool pressure)
{
    int case_failures = failures;
    fixture_t f;
    moq_media_object_t held = {0}, empty = {0};
    update_events_t seen = {.reason = reason};
    UPDATE_NEED(update_start(&f, version, &seen, pressure));
    if (pressure) UPDATE_NEED(f.r->ev_tail - f.r->ev_head == f.r->ev_cap);
    send_payload(&f, 1, 1, "held", 4, true); pump(&f);
    UPDATE_NEED(moq_media_receiver_poll_object(f.r, &held, sizeof(held)) == MOQ_OK);
    send_payload(&f, 1, 2, "queued", 6, true); pump(&f);
    UPDATE_NEED(f.r->obj_tail - f.r->obj_head == 1);
    /* App pause purges only library-owned queue entries. Its UPDATE then
     * fails before admission; no pending initial ACK or earlier UPDATE exists. */
    UPDATE_NEED(moq_sub_track_get_state(f.probe) == MOQ_SUB_TRACK_ACTIVE);
    UPDATE_NEED(!f.bad->upd_notify_app && f.bad->upd_sent_app_gen == 0);
    moq_sub_track_result_t ack;
    UPDATE_NEED(moq_sub_get_track_result(f.r->sub, f.probe, &ack, sizeof(ack)) == MOQ_OK);
    MOQ_TEST_CHECK(ack.flags & MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN);
    UPDATE_NEED(moq_media_receiver_unsubscribe_track(f.r, f.bad) == MOQ_OK);
    UPDATE_NEED(moq_media_receiver_poll_object(f.r, &empty, sizeof(empty)) == MOQ_DONE);
    update_probe.inject = reason;
    if (pressure) fill_action_queue(&f);
    unsigned wakes = f.wakes;
    pump(&f);
    UPDATE_NEED(update_probe.attempts == 1 && update_probe.injected == 1 &&
        update_probe.admissions == 0 && update_probe.last == reason);
    MOQ_TEST_CHECK(f.wakes > wakes);
    update_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_FAILED);
    MOQ_TEST_CHECK(!f.bad->ended && !f.bad->upd_notify_app && f.bad->upd_sent_app_gen == 0);
    if (f.bad->sub_track == f.probe) {
        MOQ_TEST_CHECK(moq_sub_get_track_result(f.r->sub, f.probe, &ack, sizeof(ack)) == MOQ_OK);
        MOQ_TEST_CHECK(ack.flags & MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN);
    }
    /* Empty object queue: the local observation alone must make wait ready
     * after discovery drains, including when the ordinary event ring is full. */
    if (pressure) {
        for (unsigned i = 0; i < 3; i++) {
            moq_media_track_event_t event;
            UPDATE_NEED(moq_media_receiver_poll_track(f.r, &event, sizeof(event)) == MOQ_OK);
            MOQ_TEST_CHECK(event.kind == (i < 2 ? MOQ_MEDIA_TRACK_ADDED : MOQ_MEDIA_CATALOG_READY));
            if (i < 2) seen.added++; else seen.ready++;
        }
    }
    MOQ_TEST_CHECK(moq_media_receiver_wait(f.r, 0) == MOQ_OK);
    update_drain(&f, &seen);
    UPDATE_NEED(seen.failures == 1 && seen.ends == 0 && seen.acks == 0);
    MOQ_TEST_CHECK(f.r->local_failure_pending_count == 0);
    unsigned cancels = f.unsubscribes;
    for (unsigned i = 0; i < UPDATE_REPEATS; i++) pump(&f);
    MOQ_TEST_CHECK(update_probe.attempts == 1 && update_probe.admissions == 0);
    MOQ_TEST_CHECK(f.unsubscribes - cancels <= UPDATE_REPEATS);
    if (pressure) {
        MOQ_TEST_CHECK(f.unsubscribes > cancels &&
            (f.unsubscribe_result == MOQ_ERR_WOULD_BLOCK || f.unsubscribe_result == MOQ_ERR_BUFFER));
        MOQ_TEST_CHECK(f.bad->sub_track == f.probe && f.releases == 0);
        MOQ_TEST_CHECK(moq_sub_fill_is_valid(moq_sub_track_fill(f.probe)));
    }
    settle(&f);
    MOQ_TEST_CHECK(f.unsubscribes > 0 && f.unsubscribe_result == MOQ_OK);
    MOQ_TEST_CHECK(f.bad->sub_track == NULL && f.releases == 1);
    expect_failed(&f);
    update_good(&f);
    send_payload(&f, 0, 1, vod_catalog, sizeof(vod_catalog) - 1, false);
    settle(&f); update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.failures == 1 && seen.ends == 0 && seen.acks == 0);
    MOQ_TEST_CHECK(update_probe.attempts == 1 && f.peer[1].requests == 1 && f.peer[2].requests == 1);
    update_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_FAILED);
done:
    moq_media_object_cleanup(&empty);
    update_finish(&f, &held);
    printf("%s: receiver_update_failure(draft=%u,result=%d,pressure=%d)\n",
        failures == case_failures ? "PASS" : "FAIL", (unsigned)version,
        (int)reason, (int)pressure);
}

static void update_would_block(moq_version_t version)
{
    int case_failures = failures;
    fixture_t f;
    moq_media_object_t held = {0};
    update_events_t seen = {0};
    UPDATE_NEED(update_start(&f, version, &seen, false));
    UPDATE_NEED(moq_media_receiver_unsubscribe_track(f.r, f.bad) == MOQ_OK);
    uint64_t reconciled = f.bad->reconciled_app_gen;
    /* Real native action capacity, with no injected status and no ACK in
     * flight. No network drain until the retry assertions have completed. */
    fill_action_queue(&f);
    for (unsigned i = 0; i < UPDATE_REPEATS; i++) {
        pump(&f);
        UPDATE_NEED(update_probe.last == MOQ_ERR_WOULD_BLOCK);
        MOQ_TEST_CHECK(update_probe.attempts == i + 1 && update_probe.admissions == 0);
        MOQ_TEST_CHECK(f.bad->reconciled_app_gen == reconciled && !f.bad->upd_notify_app);
    }
    update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.failures == 0 && seen.ends == 0 && seen.acks == 0);
    update_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_PAUSED_APP);
    MOQ_TEST_CHECK(f.r->local_failure_pending_count == 0);
    network(&f); pump(&f); /* Capacity restored, not yet acknowledged. */
    UPDATE_NEED(update_probe.admissions == 1 && !f.bad->forward_sent);
    update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.acks == 0);
    unsigned attempts = update_probe.attempts;
    settle(&f); update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.acks == 1 && seen.failures == 0 && seen.ends == 0);
    MOQ_TEST_CHECK(update_probe.attempts == attempts && update_probe.injected == 0);
    MOQ_TEST_CHECK(f.peer[1].requests == 1 && f.unsubscribes == 0 && f.releases == 0);
    update_good(&f);
done:
    update_finish(&f, &held);
    printf("%s: receiver_update_capacity_retry(draft=%u)\n",
        failures == case_failures ? "PASS" : "FAIL", (unsigned)version);
}

/* Capture only real deferred UPDATE events. Unlike network(), retain their
 * correlation for answering later. No new SUBSCRIBEs are expected here. */
static bool update_owed(fixture_t *f, uint64_t *request_id, bool forward)
{
    MOQ_TEST_CHECK(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
    unsigned found = 0;
    moq_event_t event;
    for (unsigned i = 0; i < 32; i++) {
        if (!moq_session_poll_events(server(f), &event, 1)) break;
        if (event.kind == MOQ_EVENT_SUBSCRIBE_UPDATED) {
            found++;
            MOQ_TEST_CHECK(moq_subscription_eq(event.u.subscribe_updated.sub, f->peer[1].sub));
            MOQ_TEST_CHECK(event.u.subscribe_updated.answer_owed &&
                event.u.subscribe_updated.update_count == 1);
            MOQ_TEST_CHECK(event.u.subscribe_updated.has_forward &&
                event.u.subscribe_updated.forward == forward);
            *request_id = event.u.subscribe_updated.update_request_id;
        }
        MOQ_TEST_CHECK(event.kind != MOQ_EVENT_SUBSCRIBE_REQUEST);
        moq_event_cleanup(&event);
    }
    return found == 1;
}

static void update_answer(fixture_t *f, uint64_t request_id)
{
    moq_accept_subscription_update_cfg_t cfg;
    moq_accept_subscription_update_cfg_init(&cfg);
    cfg.update_request_id = request_id;
    MOQ_TEST_CHECK(moq_session_accept_subscription_update(server(f), f->peer[1].sub,
        &cfg, now(f)) == MOQ_OK);
    network(f); pump(f);
}

static void update_pending(moq_version_t version)
{
    int case_failures = failures;
    fixture_t f;
    moq_media_object_t held = {0};
    update_events_t seen = {0};
    uint64_t request_id = 0;
    UPDATE_NEED(update_start(&f, version, &seen, false));
    UPDATE_NEED(moq_session_set_defer_subscription_updates(server(&f), true) == MOQ_OK);
    UPDATE_NEED(moq_media_receiver_unsubscribe_track(f.r, f.bad) == MOQ_OK);
    pump(&f);
    UPDATE_NEED(update_probe.admissions == 1 && !f.bad->forward_sent);
    UPDATE_NEED(update_owed(&f, &request_id, false));
    uint64_t sent = f.bad->upd_sent_app_gen, reconciled = f.bad->reconciled_app_gen;
    UPDATE_NEED(f.bad->upd_notify_app && sent == f.bad->app_gen);
    UPDATE_NEED(moq_media_receiver_subscribe_track(f.r, f.bad, NULL) == MOQ_OK);
    uint64_t next = f.bad->app_gen;
    MOQ_TEST_CHECK(next > sent);
    unsigned attempts = update_probe.attempts;
    for (unsigned i = 0; i < UPDATE_REPEATS; i++) {
        unsigned before = update_probe.attempts;
        pump(&f); /* ACK withheld; any attempted native call really returns WRONG_STATE. */
        if (update_probe.attempts != before)
            MOQ_TEST_CHECK(update_probe.last == MOQ_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(update_probe.admissions == 1 && update_probe.injected == 0);
        MOQ_TEST_CHECK(f.bad->upd_notify_app && f.bad->upd_sent_app_gen == sent &&
            f.bad->reconciled_app_gen == reconciled && !f.bad->forward_sent);
    }
    MOQ_TEST_CHECK(update_probe.attempts - attempts <= UPDATE_REPEATS);
    update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.failures == 0 && seen.acks == 0 && seen.ends == 0);
    update_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_ACTIVE);
    update_answer(&f, request_id);
    update_drain(&f, &seen);
    UPDATE_NEED(seen.acks == 1 && update_probe.admissions == 2);
    MOQ_TEST_CHECK(f.bad->upd_notify_app && f.bad->upd_sent_app_gen == next && f.bad->forward_sent);
    UPDATE_NEED(update_owed(&f, &request_id, true));
    update_answer(&f, request_id);
    settle(&f); update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.acks == 2 && seen.failures == 0 && seen.ends == 0);
    MOQ_TEST_CHECK(update_probe.admissions == 2 && !f.bad->upd_notify_app);
    MOQ_TEST_CHECK(f.peer[1].requests == 1 && f.unsubscribes == 0 && f.releases == 0);
    update_good(&f);
done:
    update_finish(&f, &held);
    printf("%s: receiver_update_ack_wait(draft=%u)\n",
        failures == case_failures ? "PASS" : "FAIL", (unsigned)version);
}

static void update_initial_ack_wait(moq_version_t version, bool keep_disabled)
{
    int case_failures = failures;
    fixture_t f;
    moq_media_object_t held = {0};
    update_events_t seen = {0};
    memset(&update_probe, 0, sizeof(update_probe));
    UPDATE_NEED(start(&f, version, 0) && bootstrap(&f));
    update_drain(&f, &seen);
    update_probe.sub = f.r->sub;
    update_probe.track = f.probe;
    update_probe.inject = MOQ_ERR_WRONG_STATE;
    UPDATE_NEED(moq_media_receiver_unsubscribe_track(f.r, f.bad) == MOQ_OK);
    for (unsigned i = 0; i < UPDATE_REPEATS; i++) pump(&f);
    MOQ_TEST_CHECK(update_probe.attempts == 0); /* Known initial ACK wait skips admission. */
    update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.failures == 0 && seen.acks == 0 && seen.ends == 0);
    MOQ_TEST_CHECK(f.bad->sub_track == f.probe && f.releases == 0);
    update_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_PAUSED_APP);
    if (!keep_disabled)
        UPDATE_NEED(moq_media_receiver_subscribe_track(f.r, f.bad, NULL) == MOQ_OK);
    update_probe.inject = MOQ_OK;
    accept(&f, 1, true); accept(&f, 2, false);
    settle(&f); update_drain(&f, &seen);
    MOQ_TEST_CHECK(update_probe.attempts == (keep_disabled ? 1u : 0u) &&
        update_probe.admissions == (keep_disabled ? 1u : 0u));
    MOQ_TEST_CHECK(seen.failures == 0 && seen.ends == 0 &&
        seen.acks == (keep_disabled ? 1u : 0u));
    MOQ_TEST_CHECK(f.peer[1].requests == 1 && f.unsubscribes == 0 && f.releases == 0);
    update_state(&f, f.bad, keep_disabled ? MOQ_MEDIA_TRACK_STATE_PAUSED_APP
                                        : MOQ_MEDIA_TRACK_STATE_ACTIVE);
    update_good(&f);
done:
    update_finish(&f, &held);
    printf("%s: receiver_initial_ack_wait(draft=%u,paused=%d)\n",
        failures == case_failures ? "PASS" : "FAIL", (unsigned)version,
        keep_disabled ? 1 : 0);
}

typedef struct probe_allocator {
    moq_alloc_t original;
    unsigned calls, refusals;
    bool fail;
} probe_allocator_t;

static void *probe_alloc(size_t size, void *ctx)
{
    probe_allocator_t *p = ctx;
    p->calls++;
    if (p->fail) { p->refusals++; return NULL; }
    return p->original.alloc(size, p->original.ctx);
}

static void probe_free(void *ptr, size_t size, void *ctx)
{
    probe_allocator_t *p = ctx;
    p->original.free(ptr, size, p->original.ctx);
}

static void *probe_realloc(void *ptr, size_t old_size, size_t size, void *ctx)
{
    probe_allocator_t *p = ctx;
    return p->original.realloc(ptr, old_size, size, p->original.ctx);
}

/* Real core encoding with one byte left in an occupied send arena. No return
 * is injected: only the probe's native allocator is allowed to refuse. */
static void update_native_probe(moq_version_t version, bool fail)
{
    int case_failures = failures;
    fixture_t f;
    moq_media_object_t held = {0};
    update_events_t seen = {.reason = MOQ_ERR_NOMEM};
    probe_allocator_t probe = {0};
    size_t original_capacity = 0;
    bool wrapped = false;
    UPDATE_NEED(update_start(&f, version, &seen, false));
    UPDATE_NEED(moq_media_receiver_unsubscribe_track(f.r, f.bad) == MOQ_OK);
    moq_session_t *session = client(&f);
    moq_track_status_cfg_t cfg;
    moq_track_status_cfg_init(&cfg);
    moq_bytes_t ns[] = {MOQ_BYTES_LITERAL("occupied")};
    cfg.track_namespace = (moq_namespace_t){ns, 1};
    cfg.track_name = MOQ_BYTES_LITERAL("other");
    moq_track_status_handle_t status;
    UPDATE_NEED(moq_session_track_status(session, &cfg, now(&f), &status) == MOQ_OK);
    UPDATE_NEED(session->send_len > 0 && session->send_len < session->send_cap);
    UPDATE_NEED(session->action_tail - session->action_head < session->action_cap);
    original_capacity = session->send_cap;
    session->send_cap = session->send_len + 1;
    probe.original = session->alloc;
    probe.fail = fail;
    session->alloc = (moq_alloc_t){.alloc = probe_alloc, .free = probe_free,
        .realloc = probe_realloc, .ctx = &probe};
    wrapped = true;
    pump(&f);
    session->alloc = probe.original;
    session->send_cap = original_capacity;
    wrapped = false;
    MOQ_TEST_CHECK(probe.calls == 1 && probe.refusals == (fail ? 1u : 0u));
    MOQ_TEST_CHECK(update_probe.attempts == 1 && update_probe.injected == 0 &&
        update_probe.admissions == 0);
    MOQ_TEST_CHECK(update_probe.last == (fail ? MOQ_ERR_NOMEM : MOQ_ERR_WOULD_BLOCK));
    update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.failures == (fail ? 1u : 0u) && seen.acks == 0 && seen.ends == 0);
    update_state(&f, f.bad, fail ? MOQ_MEDIA_TRACK_STATE_FAILED : MOQ_MEDIA_TRACK_STATE_PAUSED_APP);
    settle(&f);
    update_drain(&f, &seen);
    if (fail) {
        MOQ_TEST_CHECK(update_probe.attempts == 1 && f.bad->sub_track == NULL && f.releases == 1);
        MOQ_TEST_CHECK(seen.failures == 1 && seen.acks == 0);
    } else {
        MOQ_TEST_CHECK(update_probe.admissions == 1 && seen.failures == 0 && seen.acks == 1);
        MOQ_TEST_CHECK(f.bad->sub_track == f.probe && f.releases == 0);
    }
    MOQ_TEST_CHECK(f.peer[1].requests == 1);
    update_good(&f);
done:
    if (wrapped) {
        client(&f)->alloc = probe.original;
        client(&f)->send_cap = original_capacity;
    }
    update_finish(&f, &held);
    printf("%s: receiver_native_update_probe(draft=%u,fail=%d)\n",
        failures == case_failures ? "PASS" : "FAIL", (unsigned)version, fail ? 1 : 0);
}

static void answer_flow_updates(fixture_t *f)
{
    for (unsigned i = 1; i <= 2; i++) {
        uint64_t request_id = 0;
        MOQ_TEST_CHECK(moq_session_subscription_owed_update(server(f), f->peer[i].sub,
            &request_id, NULL, NULL) == MOQ_OK);
        moq_accept_subscription_update_cfg_t cfg;
        moq_accept_subscription_update_cfg_init(&cfg);
        cfg.update_request_id = request_id;
        MOQ_TEST_CHECK(moq_session_accept_subscription_update(server(f), f->peer[i].sub,
            &cfg, now(f)) == MOQ_OK);
    }
}

static void update_flow_ack_wait(moq_version_t version)
{
    int case_failures = failures;
    fixture_t f;
    moq_media_object_t held = {0};
    update_events_t seen = {0};
    memset(&update_probe, 0, sizeof(update_probe));
    UPDATE_NEED(start_configured(&f, version, 0, true) && bootstrap(&f));
    update_drain(&f, &seen);
    accept(&f, 1, true); accept(&f, 2, false);
    settle(&f);
    update_probe.sub = f.r->sub;
    update_probe.track = f.probe;
    UPDATE_NEED(moq_session_set_defer_subscription_updates(server(&f), true) == MOQ_OK);
    for (unsigned i = 1; i <= 3; i++) {
        send_payload(&f, 1, i, "flow", 4, true);
        pump(&f);
    }
    UPDATE_NEED(f.r->paused && f.r->obj_tail - f.r->obj_head == 3);
    UPDATE_NEED(update_probe.admissions == 1 && update_probe.attempts == 1);
    MOQ_TEST_CHECK(f.bad->update_inflight && !f.bad->upd_notify_app);
    network(&f); /* Peer retains both tracks' unacknowledged pause updates. */
    for (unsigned i = 0; i < 3; i++) {
        UPDATE_NEED(moq_media_receiver_poll_object(f.r, &held, sizeof(held)) == MOQ_OK);
        moq_media_object_cleanup(&held);
    }
    for (unsigned i = 0; i < UPDATE_REPEATS; i++) pump(&f);
    MOQ_TEST_CHECK(!f.r->paused && f.bad->update_inflight && !f.bad->upd_notify_app);
    MOQ_TEST_CHECK(update_probe.attempts == 1 && update_probe.admissions == 1);
    update_drain(&f, &seen);
    MOQ_TEST_CHECK(seen.failures == 0 && seen.acks == 0 && seen.ends == 0);
    MOQ_TEST_CHECK(f.unsubscribes == 0 && f.releases == 0);
    answer_flow_updates(&f);
    network(&f); pump(&f);
    update_drain(&f, &seen);
    MOQ_TEST_CHECK(update_probe.attempts == 2 && update_probe.admissions == 2);
    MOQ_TEST_CHECK(f.bad->update_inflight && !f.bad->upd_notify_app && f.bad->forward_sent);
    MOQ_TEST_CHECK(seen.failures == 0 && seen.acks == 0 && seen.ends == 0);
    network(&f);
    answer_flow_updates(&f);
    settle(&f); update_drain(&f, &seen);
    MOQ_TEST_CHECK(!f.bad->update_inflight && !f.bad->upd_notify_app);
    MOQ_TEST_CHECK(update_probe.attempts == 2 && f.peer[1].requests == 1);
    MOQ_TEST_CHECK(seen.failures == 0 && seen.acks == 0 && seen.ends == 0);
    update_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_ACTIVE);
    update_good(&f);
done:
    update_finish(&f, &held);
    printf("%s: receiver_flow_update_ack_wait(draft=%u)\n",
        failures == case_failures ? "PASS" : "FAIL", (unsigned)version);
}

int main(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22};
    const moq_result_t errors[] = {MOQ_ERR_NOMEM, MOQ_ERR_WRONG_STATE, MOQ_ERR_BUFFER};
    for (unsigned i = 0; i < sizeof(versions) / sizeof(versions[0]); i++) {
        for (unsigned j = 0; j < sizeof(errors) / sizeof(errors[0]); j++)
            update_failure(versions[i], errors[j], false);
        update_would_block(versions[i]);
        update_pending(versions[i]);
        update_initial_ack_wait(versions[i], false);
        update_initial_ack_wait(versions[i], true);
        update_native_probe(versions[i], false);
        update_native_probe(versions[i], true);
        update_flow_ack_wait(versions[i]);
    }
    update_failure(MOQ_VERSION_DRAFT_22, MOQ_ERR_NOMEM, true);
    /* Keep inherited static controls live, as in the delivery fixture. These
     * test ACK retention/intake/sizing separately, not UPDATE refusal causes. */
    int update_failures = failures;
    failures = 0;
    (void)receiver_update_ack_controls_main();
    int ack_failures = failures;
    failures += update_failures;
    fprintf(stderr, "receiver UPDATE failure: %d failures; inherited ACK controls: %d failures\n",
        update_failures, ack_failures);
    return failures ? 1 : 0;
}
