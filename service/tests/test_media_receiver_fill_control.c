/* Current receiver TU and real fill wire paths; no production testing API. */
#ifdef MOQ_RECEIVER_FILL_LAYOUT_ONLY
#include <moq/media_receiver.h>
#include <stddef.h>
#else
#include <moq/subscriber.h>
#ifndef MOQ_RECEIVER_FILL_BASELINE
static moq_result_t observed_fill_cancel(moq_subscriber_t *, moq_sub_fill_t, uint64_t);
#define moq_sub_cancel_fill observed_fill_cancel
#endif
#define MOQ_RECEIVER_ORIGIN_MAIN receiver_fill_origin_controls_main
#include "test_media_receiver_origin.c"
#ifndef MOQ_RECEIVER_FILL_BASELINE
#undef moq_sub_cancel_fill

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
#endif
    printf("receiver_fill_control: %d failures\n", failures);
    return failures ? 1 : 0;
}
#endif
