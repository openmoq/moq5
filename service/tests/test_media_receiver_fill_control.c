/* Current receiver TU and real fill wire paths; no production testing API. */
#ifdef MOQ_RECEIVER_FILL_LAYOUT_ONLY
#include <moq/media_receiver.h>
#include <stddef.h>
#else
#include <moq/subscriber.h>
#ifndef MOQ_RECEIVER_FILL_BASELINE
static moq_result_t observed_fill_cancel(moq_subscriber_t *, moq_sub_fill_t, uint64_t);
static moq_result_t observed_update(moq_subscriber_t *, moq_sub_track_t *,
                                    const moq_sub_update_cfg_t *, uint64_t);
#define moq_sub_cancel_fill observed_fill_cancel
#define moq_sub_update_subscription observed_update
#endif
#define MOQ_RECEIVER_ORIGIN_MAIN receiver_fill_origin_controls_main
#include "test_media_receiver_origin.c"
#ifndef MOQ_RECEIVER_FILL_BASELINE
#undef moq_sub_cancel_fill
#undef moq_sub_update_subscription

/* When armed, runs once just after a fill-bearing UPDATE the facade
 * accepted, before the receiver's admission step returns. */
static void (*update_admitted_hook)(void *);
static void *update_admitted_ctx;
static moq_result_t observed_update(moq_subscriber_t *sub, moq_sub_track_t *track,
                                    const moq_sub_update_cfg_t *cfg, uint64_t now_us)
{
    moq_result_t rc = moq_sub_update_subscription(sub, track, cfg, now_us);
    void (*hook)(void *) = update_admitted_hook;
    if (rc == MOQ_OK && cfg->fill && hook) {
        update_admitted_hook = NULL;
        hook(update_admitted_ctx);
    }
    return rc;
}

static moq_result_t cancel_retry_result;
static unsigned cancel_calls;
static uint64_t cancel_now;
static moq_result_t observed_fill_cancel(moq_subscriber_t *sub, moq_sub_fill_t fill,
                                        uint64_t now_us)
{
    cancel_calls++;
    cancel_now = now_us;
    moq_result_t rc = cancel_retry_result;
    cancel_retry_result = MOQ_OK;
    return rc == MOQ_OK ? moq_sub_cancel_fill(sub, fill, now_us) : rc;
}
#endif
#endif

#ifdef __cplusplus
#define FILL_ASSERT static_assert
#else
#define FILL_ASSERT _Static_assert
#endif

typedef struct fill_previous_event {
    uint32_t struct_size;
    moq_media_track_event_kind_t kind;
    moq_media_track_t *track;
    const moq_media_track_desc_t *desc;
    uint32_t config_generation;
    uint8_t reserved[4];
    bool has_largest;
    uint64_t largest_group, largest_object;
    bool has_expires;
    uint64_t expires_ms;
    moq_media_parse_drop_class_t parse_drop_class;
    uint64_t parse_drops_total, parse_drops_delta;
    int32_t local_result;
    uint32_t reserved_local_failure;
} fill_previous_event_t;

typedef struct fill_expected_event {
    fill_previous_event_t previous;
    uint64_t initiating_request_id;
    uint32_t fill_reason, fill_error_domain;
    uint64_t fill_error_code;
} fill_expected_event_t;

enum { EXPECTED_FILL_TERMINAL = 9 };
#ifndef MOQ_RECEIVER_FILL_BASELINE
FILL_ASSERT((int)MOQ_MEDIA_TRACK_FILL_TERMINAL == EXPECTED_FILL_TERMINAL, "event value");
FILL_ASSERT(offsetof(moq_media_track_event_t, initiating_request_id) ==
    sizeof(fill_previous_event_t), "complete old allocation preserved");
FILL_ASSERT(offsetof(moq_media_track_event_t, delivery_origin) ==
    sizeof(fill_expected_event_t), "complete fill event extent preserved");
FILL_ASSERT(sizeof(((moq_media_track_event_t *)0)->fill_reason) == 4, "reason width");
FILL_ASSERT(sizeof(((moq_media_track_event_t *)0)->fill_error_domain) == 4, "domain width");
FILL_ASSERT(offsetof(moq_media_track_event_t, fill_error_code) ==
    offsetof(fill_expected_event_t, fill_error_code), "code offset");
FILL_ASSERT((int)MOQ_MEDIA_TRACK_FILL_CREATED == 12, "next unused event value");
#endif

#ifdef MOQ_RECEIVER_FILL_LAYOUT_ONLY
int main(void) { return 0; }
#else
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *);
void moq_endpoint_test_pump_cycle(moq_endpoint_t *, moq_session_t *, uint64_t);
void moq_endpoint_test_drain_posted(moq_endpoint_t *, moq_session_t *, uint64_t);
size_t moq_endpoint_test_task_count(moq_endpoint_t *);

static bool fill_attach(origin_fixture_t *f)
{
    f->receiver->ep = moq_endpoint_test_make_bare();
    ORIGIN_REQUIRE(f->receiver->ep);
    moq_endpoint_hook_ops_t ops = { .pump = receiver_hook };
    ORIGIN_REQUIRE(moq_endpoint_attach_hook(f->receiver->ep, MOQ_ENDPOINT_HOOK_RECEIVER,
        &ops, f->receiver) == MOQ_OK);
    moq_endpoint_test_pump_cycle(f->receiver->ep, moq_simpair_client(f->pair), origin_now(f));
    return true;
}

static bool fill_start(origin_fixture_t *f)
{
    ORIGIN_REQUIRE(origin_wire_start(f, false));
    return fill_attach(f);
}

static void fill_pump(origin_fixture_t *f)
{
    moq_endpoint_test_pump_cycle(f->receiver->ep, moq_simpair_client(f->pair), origin_now(f));
}

static void fill_finish(origin_fixture_t *f)
{
    moq_endpoint_t *ep = f->receiver ? f->receiver->ep : NULL;
    if (ep) moq_endpoint_detach_hook(ep, MOQ_ENDPOINT_HOOK_RECEIVER, f->receiver);
    origin_finish(f);
    moq_endpoint_test_free_bare(ep);
}

#ifndef MOQ_RECEIVER_FILL_BASELINE
static bool fill_event(origin_fixture_t *f, uint64_t id, uint32_t reason,
                       uint32_t domain, uint64_t code)
{
    fill_expected_event_t event;
    memset(&event, 0xa5, sizeof(event));
    ORIGIN_REQUIRE(moq_media_receiver_poll_track(f->receiver,
        (moq_media_track_event_t *)&event, sizeof(event)) == MOQ_OK);
    ORIGIN_REQUIRE(event.previous.struct_size == sizeof(event));
    ORIGIN_REQUIRE((unsigned)event.previous.kind == EXPECTED_FILL_TERMINAL);
    ORIGIN_REQUIRE(event.previous.track == f->track && event.previous.desc == &f->track->desc);
    ORIGIN_REQUIRE(event.initiating_request_id == id && event.fill_reason == reason &&
        event.fill_error_domain == domain && event.fill_error_code == code);
    return true;
}
#endif

static void fill_terminal_delivery(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    ORIGIN_NEED(origin_send_fill(&f, 0));
    fill_pump(&f);
    fill_expected_event_t event;
    memset(&event, 0xa5, sizeof(event));
    moq_result_t rc = moq_media_receiver_poll_track(f.receiver,
        (moq_media_track_event_t *)&event, sizeof(event));
    printf("ORACLE terminal poll=%d kind=%u expected=%u\n", rc,
        (unsigned)event.previous.kind, EXPECTED_FILL_TERMINAL);
    ORIGIN_NEED(rc == MOQ_OK && (unsigned)event.previous.kind == EXPECTED_FILL_TERMINAL);
    ORIGIN_NEED(event.previous.struct_size == sizeof(event));
    ORIGIN_NEED(event.previous.track == f.track && event.initiating_request_id == 0);
    ORIGIN_NEED(event.fill_reason == MOQ_FILL_COMPLETE &&
        event.fill_error_domain == MOQ_FILL_ERROR_NONE && event.fill_error_code == 0);
    moq_media_track_state_t state;
    ORIGIN_NEED(moq_media_receiver_track_state(f.receiver, f.track, &state) == MOQ_OK);
    ORIGIN_NEED(state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    ORIGIN_NEED(moq_media_receiver_poll_track(f.receiver,
        (moq_media_track_event_t *)&event, sizeof(event)) == MOQ_DONE);
done:
    fill_finish(&f);
}

#ifndef MOQ_RECEIVER_FILL_BASELINE
static bool fill_active(origin_fixture_t *f)
{
    moq_media_track_state_t state;
    ORIGIN_REQUIRE(moq_media_receiver_track_state(f->receiver, f->track, &state) == MOQ_OK);
    ORIGIN_REQUIRE(state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
    ORIGIN_REQUIRE(!moq_media_receiver_is_fatal(f->receiver));
    ORIGIN_REQUIRE(moq_session_state(moq_simpair_client(f->pair)) == MOQ_SESS_ESTABLISHED);
    return true;
}

typedef struct fill_app_call {
    moq_media_receiver_t *receiver;
    moq_media_track_t *track;
    uint64_t id;
    moq_result_t result;
} fill_app_call_t;

static void *fill_app_cancel(void *ctx)
{
    fill_app_call_t *call = ctx;
    call->result = moq_media_receiver_cancel_fill(call->receiver, call->track, call->id);
    return NULL;
}

static void fill_cancel_overlap(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    ORIGIN_NEED(moq_media_receiver_cancel_fill(NULL, f.track, 0) == MOQ_ERR_INVAL);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, NULL, 0) == MOQ_ERR_INVAL);
    moq_media_track_t foreign = {0};
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, &foreign, 0) == MOQ_ERR_INVAL);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, UINT64_MAX) == MOQ_ERR_STALE_HANDLE);
    void *(*saved_alloc)(size_t, void *) = f.receiver->alloc.alloc;
    f.receiver->alloc.alloc = refuse_alloc;
    unsigned before_alloc = alloc_calls;
    fill_app_call_t call = {f.receiver, f.track, 0, MOQ_ERR_INTERNAL};
    pthread_t app;
    int started = pthread_create(&app, NULL, fill_app_cancel, &call);
    if (started == 0) ORIGIN_CHECK(pthread_join(app, NULL) == 0);
    f.receiver->alloc.alloc = saved_alloc;
    ORIGIN_NEED(started == 0 && call.result == MOQ_OK && alloc_calls == before_alloc);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_OK);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_OK);
    ORIGIN_NEED(moq_endpoint_test_task_count(f.receiver->ep) == 0);
    /* Both children are preheader. No packet/clock advance is needed for the
     * recorded cancellation and its local terminal to progress. */
    fill_pump(&f);
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 0, MOQ_FILL_LOCAL_CANCELLED, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_ERR_STALE_HANDLE);
    ORIGIN_NEED(origin_send_fill(&f, 1));
    ORIGIN_NEED(origin_send_live(&f));
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 4, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(fill_active(&f));
    moq_media_track_event_t event;
    ORIGIN_NEED(moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_DONE);
    unsigned objects = 0;
    moq_media_object_t object;
    while (moq_media_receiver_poll_object(f.receiver, &object, sizeof(object)) == MOQ_OK) {
        ORIGIN_CHECK(object.delivery_origin == MOQ_MEDIA_DELIVERY_LIVE ||
            (object.delivery_origin == MOQ_MEDIA_DELIVERY_FILL && object.initiating_request_id == 4));
        objects++;
        moq_media_object_cleanup(&object);
    }
    ORIGIN_NEED(objects == 2);
done:
    fill_finish(&f);
}

static void fill_peer_wins_race(void)
{
    origin_fixture_t f;
    moq_media_object_t object = {0};
    ORIGIN_NEED(fill_start(&f));
    ORIGIN_NEED(origin_send_fill(&f, 0));
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_OK);
    fill_pump(&f);
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 0, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_ERR_STALE_HANDLE);
    ORIGIN_NEED(moq_media_receiver_poll_object(f.receiver, &object, sizeof(object)) == MOQ_OK);
    ORIGIN_NEED(object.delivery_origin == MOQ_MEDIA_DELIVERY_FILL && object.initiating_request_id == 0);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 4) == MOQ_OK);
    fill_pump(&f);
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 4, MOQ_FILL_LOCAL_CANCELLED, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(fill_active(&f));
done:
    {
        moq_endpoint_t *ep = f.receiver ? f.receiver->ep : NULL;
        if (ep) moq_endpoint_detach_hook(ep, MOQ_ENDPOINT_HOOK_RECEIVER, f.receiver);
        origin_destroy_sources(&f);
        moq_endpoint_test_free_bare(ep);
    }
    if (object.payload_ref) ORIGIN_CHECK(bytes_equal(object.payload, origin_body, sizeof(origin_body)));
    moq_media_object_cleanup(&object);
    moq_media_object_cleanup(&object);
    ORIGIN_CHECK(f.state.balance == 0);
}

static void fill_reset_values(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    const uint64_t codes[] = {0, MOQ_QUIC_VARINT_MAX};
    for (unsigned i = 0; i < 2; i++) {
        ORIGIN_NEED(moq_session_open_fill(origin_server(&f), f.fill[i], origin_now(&f)) == MOQ_OK);
        ORIGIN_NEED(moq_session_abort_fill(origin_server(&f), f.fill[i], codes[i], origin_now(&f)) == MOQ_OK);
        ORIGIN_NEED(origin_network(&f));
        fill_pump(&f);
        ORIGIN_NEED(fill_event(&f, f.initiator[i], MOQ_FILL_PEER_CANCELLED_OR_RESET,
            MOQ_FILL_ERROR_STREAM, codes[i]));
        ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, f.initiator[i]) == MOQ_ERR_STALE_HANDLE);
    }
    ORIGIN_NEED(fill_active(&f));
done:
    fill_finish(&f);
}

static void fill_event_pressure(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    f.receiver->ev_cap = 2;
    ORIGIN_NEED(rq_push(f.receiver, MOQ_MEDIA_TRACK_UPDATED, f.track));
    ORIGIN_NEED(rq_push(f.receiver, MOQ_MEDIA_TRACK_UPDATED, f.track));
    ORIGIN_NEED(origin_send_fill(&f, 0));
    ORIGIN_NEED(origin_send_fill(&f, 1));
    fill_pump(&f);
    moq_sub_stats_t before, after;
    ORIGIN_NEED(moq_sub_get_stats(f.receiver->sub, &before, sizeof(before)) == MOQ_OK);
    ORIGIN_NEED(before.fill_items_queued == 4 && f.receiver->fill_blocked);
    fill_pump(&f);
    ORIGIN_NEED(moq_sub_get_stats(f.receiver->sub, &after, sizeof(after)) == MOQ_OK);
    ORIGIN_NEED(after.fill_items_polled == before.fill_items_polled);
    moq_media_track_event_t event;
    for (unsigned i = 0; i < 2; i++) {
        ORIGIN_NEED(moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_OK);
        ORIGIN_NEED(event.kind == MOQ_MEDIA_TRACK_UPDATED);
        fill_pump(&f); /* no new network delivery */
    }
    ORIGIN_NEED(fill_event(&f, 0, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 4, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_DONE);
    ORIGIN_NEED(fill_active(&f));
done:
    fill_finish(&f);
}

static void fill_event_sizes(void)
{
    const size_t sizes[] = {0, MEDIA_TRACK_EVENT_V0_SIZE - 1, MEDIA_TRACK_EVENT_V0_SIZE,
        sizeof(fill_previous_event_t), sizeof(fill_previous_event_t) + 3,
        offsetof(moq_media_track_event_t, fill_error_code) + 1,
        sizeof(moq_media_track_event_t) - 1, sizeof(moq_media_track_event_t),
        sizeof(moq_media_track_event_t) + 17};
    for (size_t i = 0; i < sizeof(sizes) / sizeof(*sizes); i++) {
        origin_fixture_t f;
        uint8_t *out = NULL;
        ORIGIN_NEED(fill_start(&f));
        ORIGIN_NEED(origin_send_fill(&f, 0));
        fill_pump(&f);
        size_t size = sizes[i];
        out = malloc(size ? size : 1);
        ORIGIN_NEED(out);
        memset(out, 0xa5, size ? size : 1);
        uint32_t head = f.receiver->ev_head;
        moq_result_t rc = moq_media_receiver_poll_track(f.receiver, (moq_media_track_event_t *)out, size);
        if (size < MEDIA_TRACK_EVENT_V0_SIZE) {
            ORIGIN_NEED(rc == MOQ_ERR_INVAL && f.receiver->ev_head == head);
            for (size_t j = 0; j < (size ? size : 1); j++) ORIGIN_CHECK(out[j] == 0xa5);
        } else {
            ORIGIN_NEED(rc == MOQ_OK && f.receiver->ev_head == head + 1);
            uint32_t stamp;
            memcpy(&stamp, out, sizeof(stamp));
            ORIGIN_NEED(stamp == (size < sizeof(moq_media_track_event_t) ? size : sizeof(moq_media_track_event_t)));
            moq_media_track_event_t expected = {0};
            expected.struct_size = stamp;
            expected.kind = MOQ_MEDIA_TRACK_FILL_TERMINAL;
            expected.track = f.track;
            expected.desc = &f.track->desc;
            expected.fill_reason = MOQ_FILL_COMPLETE;
            ORIGIN_NEED(memcmp(out, &expected, stamp) == 0);
            for (size_t j = stamp; j < size; j++) ORIGIN_CHECK(out[j] == 0xa5);
        }
done:
        free(out);
        fill_finish(&f);
    }
}

static void fill_legacy(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    for (unsigned i = 0; i < 2; i++) {
        origin_fixture_t f;
        ORIGIN_NEED(origin_start(&f, versions[i]));
        ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_ERR_UNSUPPORTED);
        ORIGIN_NEED(rq_push(f.receiver, MOQ_MEDIA_TRACK_UPDATED, f.track));
        fill_previous_event_t *old = malloc(sizeof(*old));
        ORIGIN_NEED(old);
        moq_result_t rc = moq_media_receiver_poll_track(f.receiver, (moq_media_track_event_t *)old, sizeof(*old));
        ORIGIN_CHECK(rc == MOQ_OK && old->struct_size == sizeof(*old) && old->kind == MOQ_MEDIA_TRACK_UPDATED);
        free(old);
done:
        fill_finish(&f);
    }
}

static moq_result_t fill_noop_task(moq_endpoint_t *ep, moq_session_t *session,
                                  uint64_t now_us, void *ctx)
{
    (void)ep; (void)session; (void)now_us; (void)ctx;
    return MOQ_OK;
}

static void fill_interrupt_close_teardown(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    moq_endpoint_t *ep = f.receiver->ep;
    moq_endpoint_set_interrupted(ep, true);
    ORIGIN_NEED(moq_media_receiver_wait(f.receiver, 0) == MOQ_ERR_INTERRUPTED);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_OK);
    fill_pump(&f);
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 0, MOQ_FILL_LOCAL_CANCELLED, MOQ_FILL_ERROR_NONE, 0));
    moq_endpoint_set_interrupted(ep, false);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 4) == MOQ_OK);
    moq_endpoint_detach_hook(ep, MOQ_ENDPOINT_HOOK_RECEIVER, f.receiver);
    ORIGIN_NEED(moq_endpoint_stop(ep) == MOQ_OK);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 4) == MOQ_ERR_CLOSED);
    ORIGIN_NEED(moq_endpoint_post(ep, fill_noop_task, NULL) == MOQ_ERR_CLOSED);
    /* Real destroy attempts the existing teardown post; a closed endpoint
     * refuses it and destroys the independent teardown context locally. */
    moq_media_receiver_destroy(f.receiver);
    f.receiver = NULL;
    ORIGIN_NEED(moq_endpoint_test_task_count(ep) == 0);
    moq_endpoint_test_free_bare(ep);
done:
    fill_finish(&f);
}

static void fill_pending_destroy(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_OK);
    moq_endpoint_t *ep = f.receiver->ep;
    moq_media_receiver_destroy(f.receiver);
    f.receiver = NULL;
    ORIGIN_NEED(moq_endpoint_test_task_count(ep) == 1); /* only normal teardown */
    moq_endpoint_test_drain_posted(ep, moq_simpair_client(f.pair), origin_now(&f));
    ORIGIN_NEED(moq_endpoint_test_task_count(ep) == 0);
    moq_endpoint_test_free_bare(ep);
done:
    fill_finish(&f);
}

static void fill_late_header(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    /* Commit a peer header but do not deliver it until after local cancel. */
    ORIGIN_NEED(moq_session_open_fill(origin_server(&f), f.fill[0], origin_now(&f)) == MOQ_OK);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_OK);
    fill_pump(&f);
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 0, MOQ_FILL_LOCAL_CANCELLED, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(origin_network(&f));
    fill_pump(&f);
    moq_media_track_event_t event;
    ORIGIN_NEED(moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_DONE);
    ORIGIN_NEED(origin_send_fill(&f, 1));
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 4, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(fill_active(&f));
done:
    fill_finish(&f);
}

static void fill_actual_capacity(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    moq_location_filter_cfg_t location;
    moq_fill_cfg_t fill;
    origin_fill_config(&location, &fill);
    moq_sub_update_cfg_t update;
    moq_sub_update_cfg_init_sized(&update, sizeof(update));
    update.fill = &fill;
    for (unsigned i = 2; i < RECEIVER_SUB_MAX_FILLS; i++) {
        ORIGIN_NEED(moq_sub_update_subscription(f.receiver->sub, f.track->sub_track,
            &update, origin_now(&f)) == MOQ_OK);
        ORIGIN_NEED(moq_simpair_run_until_quiescent(f.pair, 64, NULL) == MOQ_OK);
        moq_event_t event;
        unsigned requests = 0;
        while (moq_session_poll_events(origin_server(&f), &event, 1)) {
            if (event.kind == MOQ_EVENT_FILL_REQUEST) requests++;
            moq_event_cleanup(&event);
        }
        ORIGIN_NEED(requests == 1);
        fill_pump(&f);
    }
    ORIGIN_NEED(moq_sub_update_subscription(f.receiver->sub, f.track->sub_track,
        &update, origin_now(&f)) == MOQ_ERR_WOULD_BLOCK);
    uint64_t ids[RECEIVER_SUB_MAX_FILLS];
    for (unsigned i = 0; i < RECEIVER_SUB_MAX_FILLS; i++) {
        ORIGIN_NEED(moq_sub_fill_is_valid(f.receiver->fills[i].token));
        ids[i] = f.receiver->fills[i].initiating_request_id;
        for (unsigned j = 0; j < i; j++) ORIGIN_NEED(ids[i] != ids[j]);
        ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, ids[i]) == MOQ_OK);
    }
    fill_pump(&f);
    fill_pump(&f);
    for (unsigned i = 0; i < RECEIVER_SUB_MAX_FILLS; i++) {
        ORIGIN_NEED(fill_event(&f, ids[i], MOQ_FILL_LOCAL_CANCELLED, MOQ_FILL_ERROR_NONE, 0));
        ORIGIN_NEED(!moq_sub_fill_is_valid(f.receiver->fills[i].token));
    }
    ORIGIN_NEED(fill_active(&f));
done:
    fill_finish(&f);
}

static void fill_generation_pressure(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    moq_sub_track_t *generation = f.track->sub_track;
    f.receiver->ev_cap = 1;
    ORIGIN_NEED(origin_send_fill(&f, 0));
    ORIGIN_NEED(origin_send_fill(&f, 1));
    /* Confirm conversion through catalog ingress while both original fill
     * terminals are queued. Its real TRACK_UPDATED fills the event lane;
     * confirmation must not bypass either child or release the old slot. */
    static const char conversion[] =
        "{\"version\":1,\"generatedAt\":2,\"tracks\":["
        "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":false,"
        "\"trackDuration\":5000,\"role\":\"video\",\"codec\":\"av01\"}]}";
    moq_media_receiver_test_ingest(f.receiver, 1, 0, conversion, sizeof(conversion) - 1);
    moq_media_track_desc_t desc;
    ORIGIN_NEED(moq_media_receiver_track_desc_copy(f.receiver, f.track, &desc, sizeof(desc)) == MOQ_OK);
    ORIGIN_NEED(!desc.is_live && desc.has_track_duration && desc.track_duration_ms == 5000);
    moq_done_subscribe_cfg_t done_cfg;
    moq_done_subscribe_cfg_init(&done_cfg);
    done_cfg.status_code = RECEIVER_DONE_TRACK_ENDED;
    ORIGIN_NEED(moq_session_done_subscribe(origin_server(&f), f.peer_sub,
        &done_cfg, origin_now(&f)) == MOQ_OK);
    ORIGIN_NEED(origin_network(&f));
    fill_pump(&f);
    ORIGIN_NEED(f.track->done_pending && f.track->sub_track == generation && !f.track->vod_rearmed);
    moq_media_track_event_t event;
    ORIGIN_NEED(moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_OK);
    ORIGIN_NEED(event.kind == MOQ_MEDIA_TRACK_UPDATED && event.track == f.track);
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 0, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(f.track->sub_track == generation && !f.track->vod_rearmed);
    fill_pump(&f);
    ORIGIN_NEED(fill_event(&f, 4, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    fill_pump(&f);
    ORIGIN_NEED(f.track->vod_rearmed && !f.track->done_pending);
    ORIGIN_NEED(f.track->sub_track != NULL);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_ERR_STALE_HANDLE);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 4) == MOQ_ERR_STALE_HANDLE);
    ORIGIN_NEED(!moq_media_receiver_is_fatal(f.receiver));
done:
    fill_finish(&f);
}

static void fill_parent_outcomes(void)
{
    for (unsigned outcome = 0; outcome < 3; outcome++) {
        origin_fixture_t f;
        ORIGIN_NEED(origin_start(&f, MOQ_VERSION_DRAFT_22));
        f.pair = pair(&f.alloc, MOQ_VERSION_DRAFT_22);
        ORIGIN_NEED(f.pair);
        moq_sub_cfg_t cfg;
        moq_sub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.max_fills = RECEIVER_SUB_MAX_FILLS;
        cfg.callbacks.ctx = f.receiver;
        cfg.callbacks.on_subscribed = receiver_on_subscribed;
        cfg.callbacks.on_subscribe_error = receiver_on_subscribe_error;
        cfg.on_subscribe_done = receiver_on_subscribe_done;
        ORIGIN_NEED(moq_sub_create(moq_simpair_client(f.pair), &f.alloc, &cfg, &f.receiver->sub) == MOQ_OK);
        f.receiver->joins_by_fill = true;
        receiver_reconcile_subscriptions(f.receiver, origin_now(&f));
        moq_sub_track_cfg_t catalog;
        moq_sub_track_cfg_init_sized(&catalog, sizeof(catalog));
        catalog.track_namespace = f.receiver->namespace_;
        catalog.track_name = MOQ_BYTES_LITERAL("catalog");
        ORIGIN_NEED(moq_sub_subscribe(f.receiver->sub, &catalog, origin_now(&f),
            &f.receiver->catalog_sub) == MOQ_OK);
        ORIGIN_NEED(fill_attach(&f));
        ORIGIN_NEED(moq_simpair_run_until_quiescent(f.pair, 64, NULL) == MOQ_OK);
        moq_event_t event;
        bool requested = false;
        while (moq_session_poll_events(origin_server(&f), &event, 1)) {
            if (event.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                if (event.u.subscribe_request.track_name.len == 1) {
                    f.peer_sub = event.u.subscribe_request.sub;
                    requested = true;
                } else {
                    moq_accept_subscribe_cfg_t ac;
                    moq_accept_subscribe_cfg_init(&ac);
                    ORIGIN_CHECK(moq_session_accept_subscribe(origin_server(&f),
                        event.u.subscribe_request.sub, &ac, origin_now(&f)) == MOQ_OK);
                }
            }
            moq_event_cleanup(&event);
        }
        ORIGIN_NEED(requested);
        if (outcome == 0) {
            moq_reject_subscribe_cfg_t reject;
            moq_reject_subscribe_cfg_init(&reject);
            reject.error_code = 0;
            ORIGIN_NEED(moq_session_reject_subscribe(origin_server(&f), f.peer_sub,
                &reject, origin_now(&f)) == MOQ_OK);
        } else {
            moq_accept_subscribe_cfg_t accept;
            moq_accept_subscribe_cfg_init(&accept);
            accept.has_largest = outcome == 2;
            accept.largest_group = 1;
            ORIGIN_NEED(moq_session_accept_subscribe(origin_server(&f), f.peer_sub,
                &accept, origin_now(&f)) == MOQ_OK);
            if (outcome == 2) {
                moq_done_subscribe_cfg_t done_cfg;
                moq_done_subscribe_cfg_init(&done_cfg);
                done_cfg.status_code = 0x12345;
                ORIGIN_NEED(moq_session_done_subscribe(origin_server(&f), f.peer_sub,
                    &done_cfg, origin_now(&f)) == MOQ_OK);
            }
        }
        ORIGIN_NEED(moq_simpair_run_until_quiescent(f.pair, 64, NULL) == MOQ_OK);
        fill_pump(&f);
        /* Peer rejection can notify its ordinary parent ENDED before the child;
         * that real parent outcome must not be confused with the child's code. */
        unsigned child_count = 0, parent_count = 0;
        moq_media_track_event_t output;
        while (moq_media_receiver_poll_track(f.receiver, &output, sizeof(output)) == MOQ_OK) {
            if (output.kind == MOQ_MEDIA_TRACK_FILL_TERMINAL) {
                child_count++;
                ORIGIN_NEED(output.track == f.track && output.initiating_request_id == 0);
                ORIGIN_NEED(output.fill_reason == (outcome == 0 ? MOQ_FILL_PARENT_REJECTED :
                    outcome == 1 ? MOQ_FILL_NO_STREAM : MOQ_FILL_PARENT_TERMINATED));
                ORIGIN_NEED(output.fill_error_domain == (outcome == 0 ? MOQ_FILL_ERROR_PARENT_REQUEST :
                    outcome == 1 ? MOQ_FILL_ERROR_NONE : MOQ_FILL_ERROR_PARENT_DONE));
                ORIGIN_NEED(output.fill_error_code == (outcome == 2 ? 0x12345 : 0));
            } else if (output.kind == MOQ_MEDIA_TRACK_ENDED) parent_count++;
            else ORIGIN_NEED(false);
        }
        printf("ORACLE parent outcome=%u child=%u parent=%u\n", outcome, child_count, parent_count);
        ORIGIN_NEED(child_count == 1 && parent_count == (outcome == 1 ? 0u : 1u));
        ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_ERR_STALE_HANDLE);
        ORIGIN_NEED(!moq_media_receiver_is_fatal(f.receiver));
done:
        fill_finish(&f);
    }
}

static void fill_catalog_private(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(fill_start(&f));
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill);
    moq_sub_update_cfg_t update;
    moq_sub_update_cfg_init_sized(&update, sizeof(update));
    update.fill = &fill;
    ORIGIN_NEED(moq_sub_update_subscription(f.receiver->sub, f.receiver->catalog_sub,
        &update, origin_now(&f)) == MOQ_OK);
    ORIGIN_NEED(origin_network(&f));
    fill_pump(&f);
    moq_media_track_event_t event;
    ORIGIN_NEED(moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_DONE);
    ORIGIN_NEED(!moq_sub_fill_is_valid(f.receiver->catalog_fill));
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 6) == MOQ_ERR_STALE_HANDLE);
    ORIGIN_NEED(fill_active(&f));
done:
    fill_finish(&f);
}

static void fill_cancel_retry(void)
{
    const moq_result_t retry[] = {MOQ_ERR_WOULD_BLOCK, MOQ_ERR_BUFFER};
    for (unsigned i = 0; i < 2; i++) {
        origin_fixture_t f;
        ORIGIN_NEED(fill_start(&f));
        ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_OK);
        unsigned calls = cancel_calls;
        cancel_retry_result = retry[i];
        fill_pump(&f);
        ORIGIN_NEED(cancel_calls == calls + 1 && cancel_now == origin_now(&f));
        moq_media_track_event_t event;
        ORIGIN_NEED(moq_media_receiver_poll_track(f.receiver, &event, sizeof(event)) == MOQ_DONE);
        ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_OK);
        fill_pump(&f);
        fill_pump(&f);
        ORIGIN_NEED(cancel_calls == calls + 2);
        ORIGIN_NEED(fill_event(&f, 0, MOQ_FILL_LOCAL_CANCELLED, MOQ_FILL_ERROR_NONE, 0));
        ORIGIN_NEED(moq_media_receiver_cancel_fill(f.receiver, f.track, 0) == MOQ_ERR_STALE_HANDLE);
        ORIGIN_NEED(fill_active(&f));
done:
        cancel_retry_result = MOQ_OK;
        fill_finish(&f);
    }
}

/* -- moq_media_receiver_fill_track ------------------------------------------
 * The peer records every fill request on the wire, and the order of the media
 * subscription's UPDATEs: 'P' pause, 'R' resume, 'F' a fill-bearing UPDATE. */
typedef struct rf_fixture {
    origin_fixture_t o;
    moq_fill_request_event_t req[24];
    unsigned reqs;
    char order[16];
    unsigned order_n;
} rf_fixture_t;

static bool rf_network(rf_fixture_t *rf)
{
    origin_fixture_t *f = &rf->o;
    for (unsigned round = 0; round < 3; round++) {
        ORIGIN_REQUIRE(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
        moq_event_t ev;
        while (moq_session_poll_events(origin_server(f), &ev, 1)) {
            moq_result_t rc = MOQ_OK;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                bool media = ev.u.subscribe_request.track_name.len == 1;
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                moq_rcbuf_t *properties = NULL;
                if (media) {
                    f->peer_sub = ev.u.subscribe_request.sub;
                    ac.has_largest = true;
                    ac.largest_group = origin_group;
                    ac.largest_object = origin_object + 10;
                }
                if (media && moq_session_version(origin_server(f)) == MOQ_VERSION_DRAFT_22) {
                    moq_loc_metadata_t loc;
                    moq_loc_metadata_init_sized(&loc, sizeof(loc));
                    loc.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_TIMESCALE;
                    loc.video_config = (moq_bytes_t){origin_video, sizeof(origin_video)};
                    loc.timescale = 1000;
                    rc = moq_loc_encode_metadata(&f->alloc, MOQ_VERSION_DRAFT_22,
                        MOQ_LOC_PROFILE_04, &loc, &properties);
                    if (rc == MOQ_OK)
                        ac.track_properties = (moq_bytes_t){moq_rcbuf_data(properties),
                            moq_rcbuf_len(properties)};
                }
                if (rc == MOQ_OK)
                    rc = moq_session_accept_subscribe(origin_server(f),
                        ev.u.subscribe_request.sub, &ac, origin_now(f));
                moq_rcbuf_decref(properties);
            } else if (ev.kind == MOQ_EVENT_FILL_REQUEST) {
                if (rf->reqs < sizeof(rf->req) / sizeof(*rf->req))
                    rf->req[rf->reqs++] = ev.u.fill_request;
                else rc = MOQ_ERR_LIMIT;
            } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_UPDATED &&
                       moq_subscription_eq(ev.u.subscribe_updated.sub, f->peer_sub) &&
                       rf->order_n < sizeof(rf->order) - 1) {
                rf->order[rf->order_n++] = ev.u.subscribe_updated.fill_requested ? 'F' :
                    !ev.u.subscribe_updated.has_forward ? '?' :
                    ev.u.subscribe_updated.forward ? 'R' : 'P';
            }
            moq_event_cleanup(&ev);
            ORIGIN_REQUIRE(rc == MOQ_OK);
        }
    }
    return true;
}

/* An established media track. On draft 22 its subscribe_track options carried
 * a fill (initiator 0), which stays private: no FILL_CREATED. Drafts 16 and
 * 18 subscribe without one. */
static bool rf_start_version(rf_fixture_t *rf, moq_version_t version)
{
    memset(rf, 0, sizeof(*rf));
    origin_fixture_t *f = &rf->o;
    bool d22 = version == MOQ_VERSION_DRAFT_22;
    ORIGIN_REQUIRE(origin_start(f, version));
    f->pair = pair(&f->alloc, version);
    ORIGIN_REQUIRE(f->pair != NULL);
    moq_sub_cfg_t sc;
    moq_sub_cfg_init_sized(&sc, sizeof(sc));
    if (d22) sc.max_fills = RECEIVER_SUB_MAX_FILLS;
    sc.callbacks.ctx = f->receiver;
    sc.callbacks.on_subscribed = receiver_on_subscribed;
    sc.callbacks.on_subscribe_error = receiver_on_subscribe_error;
    sc.on_subscribe_done = receiver_on_subscribe_done;
    sc.on_update_ok = receiver_on_update_ok;
    ORIGIN_REQUIRE(moq_sub_create(moq_simpair_client(f->pair), &f->alloc, &sc,
        &f->receiver->sub) == MOQ_OK);
    moq_location_filter_cfg_t location;
    moq_fill_cfg_t fill;
    origin_fill_config(&location, &fill);
    moq_media_receiver_track_subscribe_cfg_t media;
    moq_media_receiver_track_subscribe_cfg_init_sized(&media, sizeof(media));
    media.location = &location;
    media.fill = &fill;
    ORIGIN_REQUIRE(moq_media_receiver_subscribe_track(f->receiver, f->track,
        d22 ? &media : NULL) == MOQ_OK);
    f->receiver->joins_by_fill = d22;
    receiver_reconcile_subscriptions(f->receiver, origin_now(f));
    ORIGIN_REQUIRE(f->track->sub_track != NULL);
    moq_sub_track_cfg_t tc;
    moq_sub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = f->receiver->namespace_;
    tc.track_name = MOQ_BYTES_LITERAL("catalog");
    ORIGIN_REQUIRE(moq_sub_subscribe(f->receiver->sub, &tc, origin_now(f),
        &f->receiver->catalog_sub) == MOQ_OK);
    ORIGIN_REQUIRE(rf_network(rf));
    receiver_hook(NULL, moq_simpair_client(f->pair), origin_now(f), f->receiver);
    ORIGIN_REQUIRE(d22 ? rf->reqs == 1 && rf->req[0].initiating_request_id == 0
                       : rf->reqs == 0);
    ORIGIN_REQUIRE(moq_sub_track_is_active(f->track->sub_track));
    ORIGIN_REQUIRE(fill_attach(f));
    moq_media_track_event_t event;
    ORIGIN_REQUIRE(moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) == MOQ_DONE);
    return true;
}

static bool rf_start(rf_fixture_t *rf)
{
    return rf_start_version(rf, MOQ_VERSION_DRAFT_22);
}

/* A fill of objects [o, o+3] of group g, with every selectable override. */
static void rf_fill(moq_location_filter_cfg_t *location, moq_fill_cfg_t *fill,
                    uint64_t g, uint64_t o)
{
    moq_location_filter_cfg_init(location);
    location->kind = MOQ_LOCATION_FILTER_OBJECT_END;
    location->start_group = g;
    location->start_object = o;
    location->end_group = g;
    location->end_object = o + 3;
    moq_fill_cfg_init(fill);
    fill->location = location;
    fill->has_subscriber_priority = true;
    fill->subscriber_priority = 77;
    fill->has_group_order = true;
    fill->group_order = MOQ_GROUP_ORDER_DESCENDING;
    fill->has_fill_timeout = true;
    fill->fill_timeout_ms = 1234;
}

static bool rf_request_is(const moq_fill_request_event_t *q, const origin_fixture_t *f,
                          uint64_t id, uint64_t g, uint64_t o)
{
    ORIGIN_REQUIRE(moq_subscription_eq(q->sub, f->peer_sub));
    ORIGIN_REQUIRE(q->initiating_request_id == id);
    ORIGIN_REQUIRE(q->start_group == g && q->start_object == o &&
                   q->end_group == g && q->end_object == o + 3);
    ORIGIN_REQUIRE(q->subscriber_priority == 77 && q->has_fill_timeout &&
                   q->fill_timeout_ms == 1234 &&
                   q->group_order == (uint32_t)MOQ_GROUP_ORDER_DESCENDING);
    return true;
}

/* Poll the next event as this track's FILL_CREATED; every other field zero. */
static bool rf_created(rf_fixture_t *rf, uint64_t *id)
{
    moq_media_track_event_t event;
    memset(&event, 0xa5, sizeof(event));
    ORIGIN_REQUIRE(moq_media_receiver_poll_track(rf->o.receiver, &event, sizeof(event)) == MOQ_OK);
    ORIGIN_REQUIRE(event.struct_size == sizeof(event));
    ORIGIN_REQUIRE(event.kind == MOQ_MEDIA_TRACK_FILL_CREATED);
    ORIGIN_REQUIRE(event.track == rf->o.track && event.desc == &rf->o.track->desc);
    ORIGIN_REQUIRE(event.fill_reason == 0 && event.fill_error_domain == 0 &&
                   event.fill_error_code == 0 && event.local_result == 0 &&
                   event.delivery_origin == 0 && event.delivery_initiating_request_id == 0 &&
                   !event.has_largest && !event.has_done_status);
    *id = event.initiating_request_id;
    return true;
}

static bool rf_none(rf_fixture_t *rf)
{
    moq_media_track_event_t event;
    ORIGIN_REQUIRE(moq_media_receiver_poll_track(rf->o.receiver, &event, sizeof(event)) == MOQ_DONE);
    return true;
}

/* The peer serves request `i`: one object in its range, then its end. */
static bool rf_serve(rf_fixture_t *rf, unsigned i)
{
    origin_fixture_t *f = &rf->o;
    moq_fill_t child = rf->req[i].fill;
    ORIGIN_REQUIRE(moq_session_open_fill(origin_server(f), child, origin_now(f)) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = rf->req[i].start_group;
    oc.subgroup_id = origin_subgroup;
    oc.object_id = rf->req[i].start_object;
    oc.publisher_priority = 213;
    ORIGIN_REQUIRE(origin_buffers(f, &oc.payload, &oc.properties));
    moq_result_t rc = moq_session_write_fill_object(origin_server(f), child, &oc, origin_now(f));
    moq_rcbuf_decref(oc.payload);
    moq_rcbuf_decref(oc.properties);
    ORIGIN_REQUIRE(rc == MOQ_OK);
    ORIGIN_REQUIRE(moq_session_end_fill(origin_server(f), child, origin_now(f)) == MOQ_OK);
    ORIGIN_REQUIRE(rf_network(rf));
    return true;
}

/* The pass that admits a command, then the one its wake runs, which takes
 * the child's CREATED. No packet moves. */
static void rf_admit(origin_fixture_t *f)
{
    fill_pump(f);
    fill_pump(f);
}

static void rf_finish(rf_fixture_t *rf)
{
    fill_finish(&rf->o);
}

/* The command sends exactly one fill-bearing UPDATE on the existing parent,
 * with the values copied at the call, and its receipt carries the actual
 * initiating ID before any packet moves. The peer then serves that range. */
static void rf_exact_range(void)
{
    rf_fixture_t rf;
    origin_fixture_t *f = &rf.o;
    moq_media_object_t object = {0};
    ORIGIN_NEED(rf_start(&rf));
    moq_sub_track_t *parent = f->track->sub_track;
    moq_location_filter_cfg_t location;
    moq_fill_cfg_t fill;
    rf_fill(&location, &fill, origin_group, origin_object);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &fill) == MOQ_OK);
    /* The caller's records are not retained. */
    location.start_group = location.end_group = 1;
    fill.subscriber_priority = 1;
    fill.location = NULL;
    rf_admit(f);
    uint64_t id = UINT64_MAX;
    ORIGIN_NEED(rf_created(&rf, &id));
    ORIGIN_NEED(rf.reqs == 1 && rf.order_n == 0);   /* nothing delivered yet */
    ORIGIN_NEED(rf_network(&rf));
    printf("ORACLE running fill id=%llu requests=%u order=%s\n",
           (unsigned long long)id, rf.reqs, rf.order);
    ORIGIN_NEED(rf.reqs == 2 && rf.order_n == 1 && rf.order[0] == 'F');
    ORIGIN_NEED(rf_request_is(&rf.req[1], f, id, origin_group, origin_object));
    ORIGIN_NEED(rf_serve(&rf, 1));
    fill_pump(f);
    ORIGIN_NEED(moq_media_receiver_poll_object(f->receiver, &object, sizeof(object)) == MOQ_OK);
    ORIGIN_NEED(object.delivery_origin == MOQ_MEDIA_DELIVERY_FILL &&
                object.initiating_request_id == id && object.group_id == origin_group &&
                object.object_id == origin_object);
    ORIGIN_NEED(fill_event(f, id, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(f->track->sub_track == parent && fill_active(f));
    ORIGIN_NEED(rf_none(&rf));
done:
    moq_media_object_cleanup(&object);
    rf_finish(&rf);
}

/* One unobserved command per track: B waits for A's receipt to be polled,
 * not for A to end. A is cancellable by its receipt's ID before any packet,
 * and B is admitted, after A's UPDATE is answered, as an independent child. */
static void rf_receipt_slot_and_cancel(void)
{
    rf_fixture_t rf;
    origin_fixture_t *f = &rf.o;
    ORIGIN_NEED(rf_start(&rf));
    moq_location_filter_cfg_t la, lb;
    moq_fill_cfg_t a, b;
    rf_fill(&la, &a, origin_group, origin_object);
    rf_fill(&lb, &b, origin_group, origin_object + 4);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_ERR_WOULD_BLOCK);
    rf_admit(f);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_ERR_WOULD_BLOCK);
    uint64_t ida = UINT64_MAX, idb = UINT64_MAX;
    ORIGIN_NEED(rf_created(&rf, &ida));
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_OK);
    fill_pump(f);
    ORIGIN_NEED(rf_none(&rf));                       /* A's UPDATE is unanswered */
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f->receiver, f->track, ida) == MOQ_OK);
    fill_pump(f);
    fill_pump(f);
    ORIGIN_NEED(fill_event(f, ida, MOQ_FILL_LOCAL_CANCELLED, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(rf_network(&rf));
    rf_admit(f);
    ORIGIN_NEED(rf_created(&rf, &idb));
    ORIGIN_NEED(rf_network(&rf));
    printf("ORACLE receipts a=%llu b=%llu requests=%u order=%s\n",
           (unsigned long long)ida, (unsigned long long)idb, rf.reqs, rf.order);
    ORIGIN_NEED(idb != ida && rf.reqs == 3 && strcmp(rf.order, "FF") == 0);
    ORIGIN_NEED(rf_request_is(&rf.req[1], f, ida, origin_group, origin_object));
    ORIGIN_NEED(rf_request_is(&rf.req[2], f, idb, origin_group, origin_object + 4));
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f->receiver, f->track, ida) == MOQ_ERR_STALE_HANDLE);
    ORIGIN_NEED(rf_serve(&rf, 2));
    fill_pump(f);
    {
        moq_media_object_t object;
        ORIGIN_NEED(moq_media_receiver_poll_object(f->receiver, &object, sizeof(object)) == MOQ_OK);
        bool ok = object.initiating_request_id == idb;
        moq_media_object_cleanup(&object);
        ORIGIN_NEED(ok);
    }
    ORIGIN_NEED(fill_event(f, idb, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(fill_active(f));
done:
    rf_finish(&rf);
}

typedef struct rf_claim_race {
    moq_media_track_t *track;
    const moq_fill_cfg_t *b;
    unsigned calls;
    moq_result_t disable, enable, refill;
} rf_claim_race_t;

/* Runs once the executor has taken A, before its facade call. */
static void rf_claimed_interleave(moq_media_receiver_t *r, moq_media_track_t *t, void *ctx)
{
    rf_claim_race_t *race = ctx;
    if (t != race->track || race->calls++) return;
    race->disable = moq_media_receiver_unsubscribe_track(r, t);
    race->enable = moq_media_receiver_subscribe_track(r, t, NULL);
    race->refill = moq_media_receiver_fill_track(r, t, race->b);
}

/* A / disable / resume / B. Before the executor takes A, the disable withdraws
 * it and B is the only fill sent. Once A is taken, B is refused until A is
 * settled; the withdrawn A neither goes out nor frees or sends B. */
static void rf_disable_interleaves(void)
{
    for (unsigned claimed = 0; claimed < 2; claimed++) {
        rf_fixture_t rf;
        origin_fixture_t *f = &rf.o;
        ORIGIN_NEED(rf_start(&rf));
        moq_location_filter_cfg_t la, lb;
        moq_fill_cfg_t a, b;
        rf_fill(&la, &a, origin_group, origin_object);
        rf_fill(&lb, &b, origin_group, origin_object + 4);
        ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
        rf_claim_race_t race = {f->track, &b, 0, MOQ_ERR_INTERNAL, MOQ_ERR_INTERNAL,
                                MOQ_ERR_INTERNAL};
        if (!claimed) {
            ORIGIN_NEED(moq_media_receiver_unsubscribe_track(f->receiver, f->track) == MOQ_OK);
            ORIGIN_NEED(moq_media_receiver_subscribe_track(f->receiver, f->track, NULL) == MOQ_OK);
            ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_OK);
        } else {
            /* The app calls run on this thread inside the pass, as from
             * another thread: drive the pass without the endpoint mutex those
             * calls' wakes take. */
            moq_media_receiver_test_set_auth_hook(rf_claimed_interleave, &race);
            receiver_hook(NULL, moq_simpair_client(f->pair), origin_now(f), f->receiver);
            moq_media_receiver_test_set_auth_hook(NULL, NULL);
            printf("ORACLE claimed interleave calls=%u disable=%d enable=%d refill=%d\n",
                   race.calls, race.disable, race.enable, race.refill);
            ORIGIN_NEED(race.calls == 1 && race.disable == MOQ_OK && race.enable == MOQ_OK);
            ORIGIN_NEED(race.refill == MOQ_ERR_WOULD_BLOCK);
            ORIGIN_NEED(rf_none(&rf));
            ORIGIN_NEED(rf_network(&rf));
            ORIGIN_NEED(rf.reqs == 1 && rf.order_n == 0);   /* A never went out */
            ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_OK);
        }
        rf_admit(f);
        uint64_t id = UINT64_MAX;
        ORIGIN_NEED(rf_created(&rf, &id));
        ORIGIN_NEED(rf_none(&rf));
        ORIGIN_NEED(rf_network(&rf));
        printf("ORACLE interleave claimed=%u requests=%u order=%s\n", claimed, rf.reqs, rf.order);
        ORIGIN_NEED(rf.reqs == 2 && strcmp(rf.order, "F") == 0);
        ORIGIN_NEED(rf_request_is(&rf.req[1], f, id, origin_group, origin_object + 4));
        ORIGIN_NEED(fill_active(f));
done:
        moq_media_receiver_test_set_auth_hook(NULL, NULL);
        rf_finish(&rf);
    }
}

typedef struct rf_win_race {
    moq_media_receiver_t *receiver;
    moq_media_track_t *track;
    const moq_fill_cfg_t *b;
    unsigned calls;
    moq_result_t disable, enable, refill;
} rf_win_race_t;

static void rf_after_admission(void *ctx)
{
    rf_win_race_t *race = ctx;
    race->calls++;
    race->disable = moq_media_receiver_unsubscribe_track(race->receiver, race->track);
    race->enable = moq_media_receiver_subscribe_track(race->receiver, race->track, NULL);
    race->refill = moq_media_receiver_fill_track(race->receiver, race->track, race->b);
}

/* A disable that lands after A passed its final recheck and the facade
 * accepted its UPDATE loses: A is admitted and its receipt carries the real
 * initiating ID; B, requested meanwhile, is refused until that receipt is
 * polled; A stays cancellable by its ID; B then goes out as its own child. */
static void rf_admission_wins(void)
{
    rf_fixture_t rf;
    origin_fixture_t *f = &rf.o;
    ORIGIN_NEED(rf_start(&rf));
    moq_location_filter_cfg_t la, lb;
    moq_fill_cfg_t a, b;
    rf_fill(&la, &a, origin_group, origin_object);
    rf_fill(&lb, &b, origin_group, origin_object + 4);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
    rf_win_race_t race = {f->receiver, f->track, &b, 0, MOQ_ERR_INTERNAL,
                          MOQ_ERR_INTERNAL, MOQ_ERR_INTERNAL};
    update_admitted_hook = rf_after_admission;
    update_admitted_ctx = &race;
    /* The app calls run on this thread inside the pass, as from another
     * thread: drive the pass without the endpoint mutex their wakes take. */
    receiver_hook(NULL, moq_simpair_client(f->pair), origin_now(f), f->receiver);
    update_admitted_hook = NULL;
    printf("ORACLE admission wins calls=%u disable=%d enable=%d refill=%d\n",
           race.calls, race.disable, race.enable, race.refill);
    ORIGIN_NEED(race.calls == 1 && race.disable == MOQ_OK && race.enable == MOQ_OK);
    ORIGIN_NEED(race.refill == MOQ_ERR_WOULD_BLOCK);
    ORIGIN_NEED(f->track->cmd_state == FILL_CMD_ADMITTED);
    rf_admit(f);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_ERR_WOULD_BLOCK);
    uint64_t ida = UINT64_MAX, idb = UINT64_MAX;
    ORIGIN_NEED(rf_created(&rf, &ida));
    ORIGIN_NEED(rf_none(&rf));
    ORIGIN_NEED(rf_network(&rf));
    ORIGIN_NEED(rf.reqs == 2 && strcmp(rf.order, "F") == 0);
    ORIGIN_NEED(rf_request_is(&rf.req[1], f, ida, origin_group, origin_object));
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_OK);
    ORIGIN_NEED(moq_media_receiver_cancel_fill(f->receiver, f->track, ida) == MOQ_OK);
    fill_pump(f);
    fill_pump(f);
    ORIGIN_NEED(fill_event(f, ida, MOQ_FILL_LOCAL_CANCELLED, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(rf_network(&rf));
    rf_admit(f);
    ORIGIN_NEED(rf_created(&rf, &idb));
    ORIGIN_NEED(rf_network(&rf));
    printf("ORACLE admission wins a=%llu b=%llu requests=%u order=%s\n",
           (unsigned long long)ida, (unsigned long long)idb, rf.reqs, rf.order);
    ORIGIN_NEED(idb != ida && rf.reqs == 3 && strcmp(rf.order, "FF") == 0);
    ORIGIN_NEED(rf_request_is(&rf.req[2], f, idb, origin_group, origin_object + 4));
    ORIGIN_NEED(fill_active(f));
done:
    update_admitted_hook = NULL;
    rf_finish(&rf);
}

/* FLOW_CONTROL: no fill while paused; the resume is sent and answered before
 * the retained fill; a later pause goes ahead of a queued one. */
static void rf_flow_pause_first(void)
{
    rf_fixture_t rf;
    origin_fixture_t *f = &rf.o;
    moq_media_object_t object = {0};
    ORIGIN_NEED(rf_start(&rf));
    moq_media_receiver_t *r = f->receiver;
    r->overflow.policy = MOQ_MEDIA_OVERFLOW_FLOW_CONTROL;
    r->obj_cap = 1;
    ORIGIN_NEED(origin_live_input(f));     /* one queued object: above low-water */
    pthread_mutex_lock(&r->mu);
    r->paused = true;
    pthread_mutex_unlock(&r->mu);
    fill_pump(f);
    moq_location_filter_cfg_t la, lb;
    moq_fill_cfg_t a, b;
    rf_fill(&la, &a, origin_group, origin_object);
    rf_fill(&lb, &b, origin_group, origin_object + 4);
    ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &a) == MOQ_OK);
    ORIGIN_NEED(rf_network(&rf));
    fill_pump(f);
    fill_pump(f);
    ORIGIN_NEED(rf_none(&rf));
    ORIGIN_NEED(rf_network(&rf));
    ORIGIN_NEED(rf.reqs == 1 && strcmp(rf.order, "P") == 0);
    ORIGIN_NEED(moq_media_receiver_poll_object(r, &object, sizeof(object)) == MOQ_OK);
    moq_media_object_cleanup(&object);
    fill_pump(f);                          /* low-water: the resume goes out alone */
    ORIGIN_NEED(rf_none(&rf));
    ORIGIN_NEED(rf_network(&rf));
    ORIGIN_NEED(rf.reqs == 1 && strcmp(rf.order, "PR") == 0);
    rf_admit(f);                           /* resume answered: now the fill */
    uint64_t id = UINT64_MAX;
    ORIGIN_NEED(rf_created(&rf, &id));
    ORIGIN_NEED(rf_network(&rf));
    ORIGIN_NEED(rf.reqs == 2 && strcmp(rf.order, "PRF") == 0);
    ORIGIN_NEED(rf_request_is(&rf.req[1], f, id, origin_group, origin_object));
    ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &b) == MOQ_OK);
    ORIGIN_NEED(origin_live_input(f));
    pthread_mutex_lock(&r->mu);
    r->paused = true;
    pthread_mutex_unlock(&r->mu);
    fill_pump(f);
    ORIGIN_NEED(rf_none(&rf));
    ORIGIN_NEED(rf_network(&rf));
    printf("ORACLE flow order=%s requests=%u\n", rf.order, rf.reqs);
    ORIGIN_NEED(rf.reqs == 2 && strcmp(rf.order, "PRFP") == 0);
done:
    moq_media_object_cleanup(&object);
    rf_finish(&rf);
}

/* A full facade child table refuses admission transiently: the same command
 * is retained and becomes exactly one child once a slot frees. */
static void rf_capacity_retry(void)
{
    rf_fixture_t rf;
    origin_fixture_t *f = &rf.o;
    ORIGIN_NEED(rf_start(&rf));
    moq_location_filter_cfg_t extra_location;
    moq_fill_cfg_t extra;
    origin_fill_config(&extra_location, &extra);
    moq_sub_update_cfg_t update;
    moq_sub_update_cfg_init_sized(&update, sizeof(update));
    update.fill = &extra;
    for (unsigned i = 1; i < RECEIVER_SUB_MAX_FILLS; i++) {
        ORIGIN_NEED(moq_sub_update_subscription(f->receiver->sub, f->track->sub_track,
            &update, origin_now(f)) == MOQ_OK);
        ORIGIN_NEED(rf_network(&rf));
        fill_pump(f);
    }
    ORIGIN_NEED(rf.reqs == RECEIVER_SUB_MAX_FILLS);
    ORIGIN_NEED(moq_sub_update_subscription(f->receiver->sub, f->track->sub_track,
        &update, origin_now(f)) == MOQ_ERR_WOULD_BLOCK);
    unsigned before = rf.reqs;
    moq_location_filter_cfg_t la;
    moq_fill_cfg_t a;
    rf_fill(&la, &a, origin_group, origin_object);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
    fill_pump(f);
    fill_pump(f);
    ORIGIN_NEED(rf_none(&rf));
    ORIGIN_NEED(rf_network(&rf));
    ORIGIN_NEED(rf.reqs == before);
    /* The peer completes the oldest child; its slot frees when the
     * receiver takes that terminal. */
    ORIGIN_NEED(rf_serve(&rf, 0));
    fill_pump(f);
    {
        moq_media_object_t object;
        ORIGIN_NEED(moq_media_receiver_poll_object(f->receiver, &object, sizeof(object)) == MOQ_OK);
        moq_media_object_cleanup(&object);
    }
    ORIGIN_NEED(fill_event(f, 0, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    unsigned settled = rf.reqs;
    rf_admit(f);
    uint64_t id = UINT64_MAX;
    ORIGIN_NEED(rf_created(&rf, &id));
    ORIGIN_NEED(rf_none(&rf));
    ORIGIN_NEED(rf_network(&rf));
    ORIGIN_NEED(settled == before);
    printf("ORACLE capacity retry id=%llu requests=%u/%u\n", (unsigned long long)id,
           rf.reqs, before);
    ORIGIN_NEED(rf.reqs == before + 1);
    ORIGIN_NEED(rf_request_is(&rf.req[before], f, id, origin_group, origin_object));
    ORIGIN_NEED(fill_active(f));
done:
    rf_finish(&rf);
}

/* A full event ring delays the receipt without losing it; draining the ring
 * alone lets it through. The child's terminal reaches the facade first and
 * still follows the receipt. An untaken command is withdrawn by a disable. */
static void rf_pressure_and_terminal_first(void)
{
    rf_fixture_t rf;
    origin_fixture_t *f = &rf.o;
    moq_media_object_t object = {0};
    ORIGIN_NEED(rf_start(&rf));
    f->receiver->ev_cap = 1;
    ORIGIN_NEED(rq_push(f->receiver, MOQ_MEDIA_TRACK_UPDATED, f->track));
    moq_location_filter_cfg_t la, lb;
    moq_fill_cfg_t a, b;
    rf_fill(&la, &a, origin_group, origin_object);
    rf_fill(&lb, &b, origin_group, origin_object + 4);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
    fill_pump(f);
    ORIGIN_NEED(f->receiver->fill_blocked);
    ORIGIN_NEED(rf_network(&rf));
    ORIGIN_NEED(rf.reqs == 2);
    ORIGIN_NEED(rf_serve(&rf, 1));                 /* the terminal reaches the facade */
    moq_media_track_event_t event;
    ORIGIN_NEED(moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) == MOQ_OK);
    ORIGIN_NEED(event.kind == MOQ_MEDIA_TRACK_UPDATED);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_ERR_WOULD_BLOCK);
    fill_pump(f);                                  /* no new packet */
    uint64_t id = UINT64_MAX;
    ORIGIN_NEED(rf_created(&rf, &id));
    ORIGIN_NEED(id == rf.req[1].initiating_request_id);
    ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &b) == MOQ_OK);
    ORIGIN_NEED(moq_media_receiver_unsubscribe_track(f->receiver, f->track) == MOQ_OK);
    fill_pump(f);
    fill_pump(f);
    ORIGIN_NEED(fill_event(f, id, MOQ_FILL_COMPLETE, MOQ_FILL_ERROR_NONE, 0));
    ORIGIN_NEED(moq_media_receiver_subscribe_track(f->receiver, f->track, NULL) == MOQ_OK);
    fill_pump(f);
    fill_pump(f);
    ORIGIN_NEED(rf_network(&rf));
    ORIGIN_NEED(rf_none(&rf));
    printf("ORACLE pressure order=%s requests=%u\n", rf.order, rf.reqs);
    ORIGIN_NEED(rf.reqs == 2 && strchr(rf.order, 'F') == strrchr(rf.order, 'F'));
done:
    moq_media_object_cleanup(&object);
    rf_finish(&rf);
}

/* Every output size at or above the v0 floor consumes the receipt and frees
 * the slot; a short one leaves the ID unobserved. Below the floor nothing is
 * consumed and the command stays unobserved. */
static void rf_receipt_sizes(void)
{
    const size_t sizes[] = {MEDIA_TRACK_EVENT_V0_SIZE - 1, MEDIA_TRACK_EVENT_V0_SIZE,
        sizeof(fill_previous_event_t), offsetof(moq_media_track_event_t, initiating_request_id) + 4,
        offsetof(moq_media_track_event_t, initiating_request_id) + 8,
        sizeof(moq_media_track_event_t), sizeof(moq_media_track_event_t) + 17};
    for (size_t i = 0; i < sizeof(sizes) / sizeof(*sizes); i++) {
        rf_fixture_t rf;
        origin_fixture_t *f = &rf.o;
        uint8_t *out = NULL;
        ORIGIN_NEED(rf_start(&rf));
        moq_location_filter_cfg_t la;
        moq_fill_cfg_t a;
        rf_fill(&la, &a, origin_group, origin_object);
        ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
        rf_admit(f);
        size_t size = sizes[i];
        out = malloc(size);
        ORIGIN_NEED(out);
        memset(out, 0xa5, size);
        moq_result_t rc = moq_media_receiver_poll_track(f->receiver,
            (moq_media_track_event_t *)out, size);
        if (size < MEDIA_TRACK_EVENT_V0_SIZE) {
            ORIGIN_NEED(rc == MOQ_ERR_INVAL);
            ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) ==
                        MOQ_ERR_WOULD_BLOCK);
        } else {
            ORIGIN_NEED(rc == MOQ_OK);
            uint32_t stamp;
            memcpy(&stamp, out, sizeof(stamp));
            ORIGIN_NEED(stamp == (size < sizeof(moq_media_track_event_t) ?
                                  size : sizeof(moq_media_track_event_t)));
            moq_media_track_event_t expected = {0};
            expected.struct_size = stamp;
            expected.kind = MOQ_MEDIA_TRACK_FILL_CREATED;
            expected.track = f->track;
            expected.desc = &f->track->desc;
            expected.initiating_request_id = 4;
            ORIGIN_NEED(memcmp(out, &expected, stamp) == 0);
            for (size_t j = stamp; j < size; j++) ORIGIN_CHECK(out[j] == 0xa5);
            ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
        }
done:
        free(out);
        rf_finish(&rf);
    }
}

/* A command is withdrawn, with no event or wire effect, when its track ends;
 * one recorded for a subscription that a live-to-VOD re-subscription replaced
 * never goes out on the replacement. Teardown with commands pending in each
 * state leaks nothing. */
static void rf_end_rearm_teardown(void)
{
    for (unsigned mode = 0; mode < 2; mode++) {
        rf_fixture_t rf;
        origin_fixture_t *f = &rf.o;
        ORIGIN_NEED(rf_start(&rf));
        moq_location_filter_cfg_t la;
        moq_fill_cfg_t a;
        rf_fill(&la, &a, origin_group, origin_object);
        if (mode == 1) {
            static const char conversion[] =
                "{\"version\":1,\"generatedAt\":2,\"tracks\":["
                "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":false,"
                "\"trackDuration\":5000,\"role\":\"video\",\"codec\":\"av01\"}]}";
            moq_media_receiver_test_ingest(f->receiver, 1, 0, conversion, sizeof(conversion) - 1);
        }
        /* The facade may give the replacement the same slot: the peer's
         * subscription identifies the parent. */
        moq_subscription_t generation = f->peer_sub;
        ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
        moq_done_subscribe_cfg_t done_cfg;
        moq_done_subscribe_cfg_init(&done_cfg);
        done_cfg.status_code = mode == 1 ? RECEIVER_DONE_TRACK_ENDED : 0x12345;
        ORIGIN_NEED(moq_session_done_subscribe(origin_server(f), f->peer_sub,
            &done_cfg, origin_now(f)) == MOQ_OK);
        ORIGIN_NEED(rf_network(&rf));
        for (unsigned n = 0; n < 6; n++) {
            fill_pump(f);
            ORIGIN_NEED(rf_network(&rf));
        }
        bool created = false, ended = false;
        moq_media_track_event_t event;
        while (moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) == MOQ_OK) {
            if (event.kind == MOQ_MEDIA_TRACK_FILL_CREATED) created = true;
            if (event.kind == MOQ_MEDIA_TRACK_ENDED) ended = true;
        }
        printf("ORACLE command after done mode=%u created=%d ended=%d rearmed=%d order=%s\n",
               mode, created, ended, f->track->vod_rearmed, rf.order);
        ORIGIN_NEED(!created && rf.order_n == 0);
        if (mode == 0) {
            ORIGIN_NEED(ended);
            ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) ==
                        MOQ_ERR_WRONG_STATE);
        } else {
            ORIGIN_NEED(f->track->vod_rearmed && f->track->sub_track &&
                        !moq_subscription_eq(f->peer_sub, generation) &&
                        moq_sub_track_is_active(f->track->sub_track));
            /* The slot is free for a command on the replacement. */
            ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
            rf_admit(f);
            uint64_t id = UINT64_MAX;
            ORIGIN_NEED(rf_created(&rf, &id));
            ORIGIN_NEED(rf_network(&rf));
            ORIGIN_NEED(strcmp(rf.order, "F") == 0);
            ORIGIN_NEED(rf_request_is(&rf.req[rf.reqs - 1], f, id, origin_group, origin_object));
        }
done:
        rf_finish(&rf);
    }
}

/* Teardown with a command queued, and with one admitted whose receipt is
 * unread: the finish checks the allocator balance. */
static void rf_teardown_pending(void)
{
    for (unsigned stage = 0; stage < 2; stage++) {
        rf_fixture_t rf;
        origin_fixture_t *f = &rf.o;
        ORIGIN_NEED(rf_start(&rf));
        moq_location_filter_cfg_t la;
        moq_fill_cfg_t a;
        rf_fill(&la, &a, origin_group, origin_object);
        ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &a) == MOQ_OK);
        if (stage == 1) rf_admit(f);
done:
        rf_finish(&rf);
    }
}

/* Synchronous refusals change nothing and send nothing. Drafts 16 and 18 have
 * no fills. The interrupt latch does not refuse the command. */
static void rf_refusals(void)
{
    {
        rf_fixture_t rf;
        origin_fixture_t *f = &rf.o;
        ORIGIN_NEED(rf_start(&rf));
        moq_media_receiver_t *r = f->receiver;
        moq_location_filter_cfg_t loc;
        moq_fill_cfg_t fill;
        rf_fill(&loc, &fill, origin_group, origin_object);
        moq_media_track_t foreign = {0};
        ORIGIN_NEED(moq_media_receiver_fill_track(NULL, f->track, &fill) == MOQ_ERR_INVAL);
        ORIGIN_NEED(moq_media_receiver_fill_track(r, NULL, &fill) == MOQ_ERR_INVAL);
        ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, NULL) == MOQ_ERR_INVAL);
        ORIGIN_NEED(moq_media_receiver_fill_track(r, &foreign, &fill) == MOQ_ERR_INVAL);
        moq_fill_cfg_t small = fill;
        small.struct_size = MOQ_FILL_CFG_V0_SIZE - 1;
        ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &small) == MOQ_ERR_ABI_MISMATCH);
        moq_location_filter_cfg_t bad = loc;
        bad.struct_size = MOQ_LOCATION_FILTER_CFG_V0_SIZE - 1;
        small = fill;
        small.location = &bad;
        ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &small) == MOQ_ERR_ABI_MISMATCH);
        bad = loc;
        bad.end_group = loc.start_group - 1;
        ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &small) == MOQ_ERR_INVAL);
        small = fill;
        small.group_order = 99;
        ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &small) == MOQ_ERR_INVAL);
        ORIGIN_NEED(moq_media_receiver_unsubscribe_track(r, f->track) == MOQ_OK);
        ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &fill) == MOQ_ERR_WRONG_STATE);
        ORIGIN_NEED(moq_media_receiver_subscribe_track(r, f->track, NULL) == MOQ_OK);
        ORIGIN_NEED(f->track->cmd_state == 0);
        fill_pump(f);
        ORIGIN_NEED(rf_network(&rf));
        ORIGIN_NEED(rf.reqs == 1 && strchr(rf.order, 'F') == NULL);
        moq_endpoint_set_interrupted(r->ep, true);
        ORIGIN_NEED(moq_media_receiver_wait(r, 0) == MOQ_ERR_INTERRUPTED);
        ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &fill) == MOQ_OK);
        moq_endpoint_set_interrupted(r->ep, false);
        moq_endpoint_t *ep = r->ep;
        moq_endpoint_detach_hook(ep, MOQ_ENDPOINT_HOOK_RECEIVER, r);
        ORIGIN_NEED(moq_endpoint_stop(ep) == MOQ_OK);
        ORIGIN_NEED(moq_media_receiver_fill_track(r, f->track, &fill) == MOQ_ERR_CLOSED);
done:
        rf_finish(&rf);
    }
}

/* Drafts 16 and 18 have no fills: on an established subscription the call is
 * refused before anything is recorded, nothing reaches the peer, and the
 * track's ordinary pause and resume still go out and are answered. */
static void rf_legacy(void)
{
    const moq_version_t versions[] = {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18};
    for (unsigned i = 0; i < 2; i++) {
        rf_fixture_t rf;
        origin_fixture_t *f = &rf.o;
        ORIGIN_NEED(rf_start_version(&rf, versions[i]));
        moq_location_filter_cfg_t loc;
        moq_fill_cfg_t fill;
        rf_fill(&loc, &fill, origin_group, origin_object);
        ORIGIN_NEED(moq_media_receiver_fill_track(f->receiver, f->track, &fill) == MOQ_ERR_UNSUPPORTED);
        ORIGIN_NEED(f->track->cmd_state == 0 && f->receiver->fill_cmd_next == 0);
        fill_pump(f);
        fill_pump(f);
        ORIGIN_NEED(rf_network(&rf));
        ORIGIN_NEED(rf.reqs == 0 && rf.order_n == 0);
        ORIGIN_NEED(rf_none(&rf));
        ORIGIN_NEED(moq_media_receiver_unsubscribe_track(f->receiver, f->track) == MOQ_OK);
        for (unsigned n = 0; n < 3 && rf.order_n < 1; n++) {
            fill_pump(f);
            ORIGIN_NEED(rf_network(&rf));
        }
        ORIGIN_NEED(moq_media_receiver_subscribe_track(f->receiver, f->track, NULL) == MOQ_OK);
        for (unsigned n = 0; n < 4 && rf.order_n < 2; n++) {
            fill_pump(f);
            ORIGIN_NEED(rf_network(&rf));
        }
        printf("ORACLE legacy v=%d requests=%u order=%s\n", (int)versions[i], rf.reqs, rf.order);
        ORIGIN_NEED(rf.reqs == 0 && strcmp(rf.order, "PR") == 0);
        moq_media_track_state_t state;
        ORIGIN_NEED(moq_media_receiver_track_state(f->receiver, f->track, &state) == MOQ_OK &&
                    state == MOQ_MEDIA_TRACK_STATE_ACTIVE);
        ORIGIN_NEED(f->track->cmd_state == 0 && f->receiver->fill_cmd_next == 0);
done:
        rf_finish(&rf);
    }
}
#endif

int main(void)
{
    origin_run("fill_terminal_delivery", fill_terminal_delivery);
#ifndef MOQ_RECEIVER_FILL_BASELINE
    origin_run("preheader_overlap_cancel_zero_and_parent_health", fill_cancel_overlap);
    origin_run("peer_terminal_wins_race_and_owned_object_survives", fill_peer_wins_race);
    origin_run("exact_reset_zero_and_max_domains", fill_reset_values);
    origin_run("event_pressure_retains_facade_items_without_new_packet", fill_event_pressure);
    origin_run("short_full_future_physical_event_outputs", fill_event_sizes);
    origin_run("d16_d18_old_event_and_cancel_refusal", fill_legacy);
    origin_run("interrupt_close_post_refusal_and_teardown", fill_interrupt_close_teardown);
    origin_run("pending_cancel_destroy_has_no_deferred_receiver", fill_pending_destroy);
    origin_run("late_header_does_not_reopen_cancelled_child", fill_late_header);
    origin_run("actual_facade_capacity_bounds_child_tracking", fill_actual_capacity);
    origin_run("old_generation_terminals_precede_vod_rearm", fill_generation_pressure);
    origin_run("parent_request_done_domains_and_no_stream", fill_parent_outcomes);
    origin_run("catalog_child_terminal_stays_private", fill_catalog_private);
    origin_run("executor_transient_cancel_retry_keeps_original_identity", fill_cancel_retry);
    origin_run("running_fill_exact_range_on_existing_parent", rf_exact_range);
    origin_run("running_fill_receipt_slot_and_preack_cancel", rf_receipt_slot_and_cancel);
    origin_run("running_fill_disable_resume_interleaves", rf_disable_interleaves);
    origin_run("running_fill_admission_wins_over_late_disable", rf_admission_wins);
    origin_run("running_fill_flow_pause_and_resume_first", rf_flow_pause_first);
    origin_run("running_fill_capacity_retry_one_child", rf_capacity_retry);
    origin_run("running_fill_event_pressure_terminal_first", rf_pressure_and_terminal_first);
    origin_run("running_fill_receipt_output_sizes", rf_receipt_sizes);
    origin_run("running_fill_end_and_vod_rearm_withdraw", rf_end_rearm_teardown);
    origin_run("running_fill_teardown_with_pending_command", rf_teardown_pending);
    origin_run("running_fill_synchronous_refusals", rf_refusals);
    origin_run("running_fill_d16_d18_unsupported", rf_legacy);
#endif
    printf("receiver_fill_control: %d failures\n", failures);
    return failures ? 1 : 0;
}
#endif
