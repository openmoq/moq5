/* Reuse the private real-session receiver fixture. Only configured facade
 * limits are changed; BUFFER must come from its actual pre-dequeue gate. */
#define main receiver_delivery_ack_controls_main
#include "test_media_receiver_ack_failure.c"
#undef main

typedef struct delivery_previous_event {
    expected_track_event_t ack;
    uint64_t initiating_request_id;
    uint32_t fill_reason, fill_error_domain;
    uint64_t fill_error_code;
} delivery_previous_event_t;

typedef struct delivery_expected_event {
    delivery_previous_event_t previous;
    uint32_t delivery_origin, reserved;
    uint64_t delivery_initiating_request_id;
} delivery_expected_event_t;

enum { DELIVERY_FAILURE = 10, DELIVERY_BUDGET = 1024 };
#ifndef MOQ_RECEIVER_DELIVERY_BASELINE
_Static_assert((int)MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE == (int)DELIVERY_FAILURE,
    "additive event value");
_Static_assert(offsetof(moq_media_track_event_t, delivery_origin) ==
    sizeof(delivery_previous_event_t), "complete previous allocation preserved");
_Static_assert(offsetof(moq_media_track_event_t, initiating_request_id) ==
    offsetof(delivery_previous_event_t, initiating_request_id), "fill ID unchanged");
_Static_assert(offsetof(moq_media_track_event_t, fill_error_code) ==
    offsetof(delivery_previous_event_t, fill_error_code), "fill cause unchanged");
_Static_assert(offsetof(moq_media_track_event_t, delivery_initiating_request_id) ==
    offsetof(delivery_expected_event_t, delivery_initiating_request_id), "delivery ID");
_Static_assert(offsetof(moq_media_track_event_t, fill_unreported_loss_causes) ==
    sizeof(delivery_expected_event_t), "complete delivery failure allocation preserved");
#endif

#define DELIVERY_NEED(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; goto done; \
    } \
} while (0)

static unsigned delivery_notices, delivery_terminals, delivery_ends;
static uint64_t delivery_last_id;
static uint32_t delivery_last_origin, delivery_last_reason, delivery_last_domain;
static uint64_t delivery_last_code;

static void delivery_drain(fixture_t *f)
{
    delivery_expected_event_t event;
    for (unsigned n = 0; n < 32; n++) {
        memset(&event, 0xa5, sizeof(event));
        if (moq_media_receiver_poll_track(f->r, (moq_media_track_event_t *)&event,
            sizeof(event)) != MOQ_OK) break;
        old_track_event_t *base = &event.previous.ack.old;
        if ((unsigned)base->kind == DELIVERY_FAILURE) {
            delivery_notices++;
            MOQ_TEST_CHECK(base->track == f->bad && base->desc == &f->bad->desc);
            MOQ_TEST_CHECK(base->struct_size == sizeof(event));
            MOQ_TEST_CHECK(event.previous.ack.local_result == MOQ_ERR_BUFFER);
            MOQ_TEST_CHECK(event.previous.ack.reserved == 0 && event.reserved == 0);
            MOQ_TEST_CHECK(event.previous.initiating_request_id == 0 &&
                event.previous.fill_reason == 0 && event.previous.fill_error_domain == 0 &&
                event.previous.fill_error_code == 0);
            delivery_last_origin = event.delivery_origin;
            delivery_last_id = event.delivery_initiating_request_id;
        } else if (base->kind == MOQ_MEDIA_TRACK_FILL_TERMINAL && base->track == f->bad) {
            delivery_terminals++;
            delivery_last_reason = event.previous.fill_reason;
            delivery_last_domain = event.previous.fill_error_domain;
            delivery_last_code = event.previous.fill_error_code;
        } else if (base->kind == MOQ_MEDIA_TRACK_ENDED) {
            delivery_ends++;
        }
    }
}

static bool delivery_start(fixture_t *f, moq_version_t version, bool discovery_pending)
{
    delivery_notices = delivery_terminals = delivery_ends = 0;
    delivery_last_id = delivery_last_origin = 0;
    delivery_last_reason = delivery_last_domain = 0;
    delivery_last_code = 0;
    if (!start(f, version, DELIVERY_BUDGET) || !bootstrap(f)) return false;
    if (!discovery_pending) delivery_drain(f);
    accept(f, 1, true);
    accept(f, 2, false);
    settle(f);
    return f->bad->established && f->good->established;
}

static void delivery_big_live(fixture_t *f, unsigned peer, uint64_t group)
{
    char payload[DELIVERY_BUDGET + 1];
    memset(payload, 'x', sizeof(payload));
    send_payload(f, peer, group, payload, sizeof(payload), peer != 0);
}

static void delivery_fill_object(fixture_t *f, unsigned peer, uint64_t object,
                                  size_t bytes)
{
    char payload[DELIVERY_BUDGET + 1];
    memset(payload, 'f', sizeof(payload));
    MOQ_TEST_CHECK(bytes <= sizeof(payload));
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.object_id = object;
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
    loc.timestamp = 12345;
    if (peer != 0)
        MOQ_TEST_CHECK(moq_loc_encode_metadata(moq_alloc_default(), f->version,
            MOQ_LOC_PROFILE_04, &loc, &oc.properties) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)payload,
        bytes, &oc.payload) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_write_fill_object(server(f), f->peer[peer].fill,
        &oc, now(f)) == MOQ_OK);
    moq_rcbuf_decref(oc.payload);
    moq_rcbuf_decref(oc.properties);
    network(f);
}

static void delivery_state(fixture_t *f, moq_media_track_t *track,
                            moq_media_track_state_t expected)
{
    moq_media_track_state_t state;
    MOQ_TEST_CHECK(moq_media_receiver_track_state(f->r, track, &state) == MOQ_OK);
    MOQ_TEST_CHECK(state == expected);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(f->r));
    MOQ_TEST_CHECK(!moq_endpoint_is_closed(f->ep) && !moq_endpoint_is_fatal(f->ep));
    MOQ_TEST_CHECK(moq_session_state(client(f)) == MOQ_SESS_ESTABLISHED);
}

static void delivery_live(void)
{
    fixture_t f;
    moq_media_object_t held = {0}, queued = {0};
    DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, false));
    send_payload(&f, 1, 1, "held", 4, true); pump(&f);
    DELIVERY_NEED(moq_media_receiver_poll_object(f.r, &held, sizeof(held)) == MOQ_OK);
    send_payload(&f, 1, 2, "queued", 6, true); pump(&f);
    delivery_big_live(&f, 1, 3);
    pump(&f);
    DELIVERY_NEED(f.tick_result == MOQ_ERR_BUFFER);
    MOQ_TEST_CHECK(f.query_result == MOQ_OK); /* ACK is still immutable/successful. */
    /* The notice follows the failed subscription's local cancellation, which
     * also settles its fill: network and pump, no peer action. */
    settle(&f);
    MOQ_TEST_CHECK(moq_media_receiver_wait(f.r, 0) == MOQ_OK);
    delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_last_origin == MOQ_MEDIA_DELIVERY_LIVE &&
        delivery_last_id == 0 && delivery_ends == 0);
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_FAILED);
    DELIVERY_NEED(moq_media_receiver_poll_object(f.r, &queued, sizeof(queued)) == MOQ_OK);
    MOQ_TEST_CHECK(queued.track == f.bad && queued.payload.len == 6 &&
        memcmp(queued.payload.data, "queued", 6) == 0);
    settle(&f); delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_ends == 0);
    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(f.r, f.bad, NULL) == MOQ_ERR_WRONG_STATE);
    send_payload(&f, 2, 1, "good", 4, true); settle(&f);
    moq_media_object_t good = {0};
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &good, sizeof(good)) == MOQ_OK);
    MOQ_TEST_CHECK(good.track == f.good && good.payload.len == 4);
    moq_media_object_cleanup(&good);
done:
    if (f.r) {
        f.r->ep = NULL;
        moq_media_receiver_test_destroy_with_session(f.r, client(&f), now(&f));
        f.r = NULL;
    }
    if (f.pair) { moq_simpair_destroy(f.pair); f.pair = NULL; }
    if (held.payload_ref) MOQ_TEST_CHECK(held.payload.len == 4 &&
        memcmp(held.payload.data, "held", 4) == 0 && held.track_properties_ref);
    if (queued.payload_ref) MOQ_TEST_CHECK(queued.payload.len == 6 && queued.track_properties_ref);
    moq_media_object_cleanup(&held);
    moq_media_object_cleanup(&queued);
    finish(&f);
}

static void delivery_facade_retained(void)
{
    fixture_t f;
    DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, false));
    send_payload(&f, 1, 1, "facade", 6, true);
    delivery_big_live(&f, 1, 2);
    /* The facade has accepted the first object when the next head fails. */
    moq_result_t rc = moq_sub_tick(f.r->sub, now(&f));
    DELIVERY_NEED(rc == MOQ_ERR_BUFFER);
    moq_sub_stats_t stats;
    MOQ_TEST_CHECK(moq_sub_get_stats(f.r->sub, &stats, sizeof(stats)) == MOQ_OK);
    DELIVERY_NEED(stats.objects_queued == 1);
    pump(&f);
    delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1);
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_FAILED);
    moq_media_object_t object = {0};
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_OK);
    MOQ_TEST_CHECK(object.track == f.bad && object.payload.len == 6 &&
        memcmp(object.payload.data, "facade", 6) == 0);
    moq_media_object_cleanup(&object);
done:
    finish(&f);
}

static void delivery_fill(void)
{
    fixture_t f;
    DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, false));
    DELIVERY_NEED(moq_fill_is_valid(f.peer[1].fill));
    moq_sub_fill_t token = moq_sub_track_fill(f.bad->sub_track);
    uint64_t id = 0;
    bool found = false;
    for (size_t i = 0; i < RECEIVER_SUB_MAX_FILLS; i++)
        if (moq_sub_fill_eq(f.r->fills[i].token, token)) {
            id = f.r->fills[i].initiating_request_id; found = true;
        }
    DELIVERY_NEED(found);
    DELIVERY_NEED(moq_session_open_fill(server(&f), f.peer[1].fill, now(&f)) == MOQ_OK);
    delivery_fill_object(&f, 1, 0, 7);
    delivery_fill_object(&f, 1, 1, DELIVERY_BUDGET + 1);
    DELIVERY_NEED(moq_sub_tick(f.r->sub, now(&f)) == MOQ_ERR_BUFFER);
    pump(&f); delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_last_origin == MOQ_MEDIA_DELIVERY_FILL &&
        delivery_last_id == id && delivery_ends == 0);
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_ACTIVE);
    moq_media_object_t object = {0};
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_OK);
    MOQ_TEST_CHECK(object.track == f.bad && object.payload.len == 7 &&
        object.delivery_origin == MOQ_MEDIA_DELIVERY_FILL && object.initiating_request_id == id);
    moq_media_object_cleanup(&object);
    settle(&f); delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_terminals == 0 && delivery_ends == 0);
    send_payload(&f, 1, 1, "live", 4, true); pump(&f);
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_OK);
    MOQ_TEST_CHECK(object.track == f.bad && object.delivery_origin == MOQ_MEDIA_DELIVERY_LIVE);
    moq_media_object_cleanup(&object);
done:
    finish(&f);
}

static void delivery_pressure(void)
{
    fixture_t f;
    DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, true));
    DELIVERY_NEED(f.r->ev_tail - f.r->ev_head == f.r->ev_cap);
    DELIVERY_NEED(moq_session_open_fill(server(&f), f.peer[1].fill, now(&f)) == MOQ_OK);
    delivery_fill_object(&f, 1, 0, DELIVERY_BUDGET + 1);
    pump(&f);
    DELIVERY_NEED(f.tick_result == MOQ_ERR_BUFFER);
    fill_action_queue(&f);
    moq_sub_fill_t token = moq_sub_track_fill(f.bad->sub_track);
    moq_result_t cancel = moq_sub_cancel_fill(f.r->sub, token, now(&f));
    MOQ_TEST_CHECK(cancel == MOQ_ERR_WOULD_BLOCK || cancel == MOQ_ERR_BUFFER);
    pump(&f);
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(moq_sub_track_fill(f.bad->sub_track)));
    moq_media_track_event_t event;
    for (unsigned i = 0; i < 3; i++) {
        MOQ_TEST_CHECK(moq_media_receiver_poll_track(f.r, &event, sizeof(event)) == MOQ_OK);
        MOQ_TEST_CHECK(event.kind == (i < 2 ? MOQ_MEDIA_TRACK_ADDED : MOQ_MEDIA_CATALOG_READY));
    }
    unsigned wakes = f.wakes;
    pump(&f); /* No network input: freed event credit publishes the retained notice. */
    MOQ_TEST_CHECK(f.wakes > wakes);
    delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_last_origin == MOQ_MEDIA_DELIVERY_FILL);
    MOQ_TEST_CHECK(moq_sub_fill_eq(moq_sub_track_fill(f.bad->sub_track), token));
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_ACTIVE);
    settle(&f); delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_terminals == 0);
done:
    finish(&f);
}

static void delivery_peer_terminal(void)
{
    const uint64_t codes[] = {0, MOQ_QUIC_VARINT_MAX};
    for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); i++) {
        fixture_t f;
        DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, false));
        DELIVERY_NEED(moq_session_open_fill(server(&f), f.peer[1].fill, now(&f)) == MOQ_OK);
        delivery_fill_object(&f, 1, 0, DELIVERY_BUDGET + 1);
        DELIVERY_NEED(moq_sub_tick(f.r->sub, now(&f)) == MOQ_ERR_BUFFER);
        DELIVERY_NEED(moq_session_abort_fill(server(&f), f.peer[1].fill, codes[i], now(&f)) == MOQ_OK);
        network(&f);
        pump(&f); delivery_drain(&f);
        settle(&f); delivery_drain(&f);
        MOQ_TEST_CHECK(delivery_notices == 1 && delivery_terminals == 1 && delivery_ends == 0);
        MOQ_TEST_CHECK(delivery_last_origin == MOQ_MEDIA_DELIVERY_FILL);
        MOQ_TEST_CHECK(delivery_last_reason == MOQ_FILL_PEER_CANCELLED_OR_RESET &&
            delivery_last_domain == MOQ_FILL_ERROR_STREAM && delivery_last_code == codes[i]);
        delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_ACTIVE);
done:
        finish(&f);
    }
}

static void delivery_temporary_pressure(void)
{
    fixture_t f;
    char payload[600];
    memset(payload, 'p', sizeof(payload));
    DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, false));
    send_payload(&f, 1, 1, payload, sizeof(payload), true);
    send_payload(&f, 1, 2, payload, sizeof(payload), true);
    DELIVERY_NEED(moq_sub_tick(f.r->sub, now(&f)) == MOQ_ERR_WOULD_BLOCK);
    moq_sub_stats_t stats;
    MOQ_TEST_CHECK(moq_sub_get_stats(f.r->sub, &stats, sizeof(stats)) == MOQ_OK);
    MOQ_TEST_CHECK(stats.objects_queued == 1 && stats.bytes_queued < DELIVERY_BUDGET);
    pump(&f);
    DELIVERY_NEED(f.tick_result == MOQ_ERR_WOULD_BLOCK);
    pump(&f); /* App/service polling restored facade credit without network. */
    delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 0);
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_ACTIVE);
    for (unsigned i = 0; i < 2; i++) {
        moq_media_object_t object = {0};
        MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_OK);
        MOQ_TEST_CHECK(object.track == f.bad && object.group_id == i + 1 &&
            object.payload.len == sizeof(payload));
        moq_media_object_cleanup(&object);
    }
done:
    finish(&f);
}

static void delivery_metadata_floor(void)
{
    fixture_t f;
    char payload[DELIVERY_BUDGET - 1];
    memset(payload, 'm', sizeof(payload));
    DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, false));
    send_payload(&f, 1, 1, payload, sizeof(payload), false);
    size_t calls = f.ledger.calls;
    f.ledger.fail = true;
    pump(&f);
    f.ledger.fail = false;
    DELIVERY_NEED(f.tick_result == MOQ_ERR_BUFFER && f.query_result == MOQ_OK);
    MOQ_TEST_CHECK(f.ledger.calls == calls);
    settle(&f);   /* local cancellation settles the fill; then the notice */
    MOQ_TEST_CHECK(moq_media_receiver_wait(f.r, 0) == MOQ_OK);
    delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_last_origin == MOQ_MEDIA_DELIVERY_LIVE);
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_FAILED);
done:
    finish(&f);
}

/* Bytes carrying the late fill Object's marker that reached the client. */
static const char late_marker[] = "late-fill-object-marker";
static unsigned late_ingress;
static void count_late_ingress(void *ctx, const moq_sim_trace_record_t *r)
{
    (void)ctx;
    if (r->kind != MOQ_SIM_TRACE_INPUT || r->to != MOQ_PERSPECTIVE_CLIENT) return;
    size_t m = sizeof(late_marker) - 1;
    for (size_t i = 0; i + m <= r->bytes.len; i++)
        if (memcmp(r->bytes.data + i, late_marker, m) == 0) { late_ingress++; return; }
}

/* A failed subscription's notice waits for everything it could still
 * deliver, and the existing local cancellation settles that: a real D22
 * capacity failure (an oversize Object) after an earlier Object, with the
 * track's join fill live (opened, or never opened) and the action queue
 * full, so cancellation is refused for credit. The facade then holds
 * nothing for the track, yet its fill could still deliver: no notice. Once
 * credit returns the failed subscription is cancelled, its fill with it
 * (locally: no peer FIN or RESET). With the child opened, the publisher
 * then writes a valid fill Object: it can, not having seen the cancel yet,
 * and its bytes reach the client only after the cancellation committed.
 * They arrive, and deliver nothing: no Object, no second notice, no parent
 * failure. Then the child's terminal is reported, then the one LIVE
 * notice. The earlier Object was kept, and the healthy sibling plays on. */
static void delivery_parent_cancellation_frontier(bool open_child)
{
    fixture_t f;
    moq_media_object_t early = {0}, late = {0}, good = {0};
    late_ingress = 0;
    fixture_trace = count_late_ingress;
    fixture_trace_ctx = NULL;
    bool started = delivery_start(&f, MOQ_VERSION_DRAFT_22, false);
    fixture_trace = NULL;
    DELIVERY_NEED(started);
    moq_fill_t child = f.peer[1].fill;
    DELIVERY_NEED(moq_fill_is_valid(child));
    if (open_child)
        DELIVERY_NEED(moq_session_open_fill(server(&f), child, now(&f)) == MOQ_OK);
    send_payload(&f, 1, 1, "early", 5, true);
    delivery_big_live(&f, 1, 2);
    fill_action_queue(&f);
    pump(&f);
    DELIVERY_NEED(f.tick_result == MOQ_ERR_BUFFER);
    pump(&f);
    delivery_drain(&f);
    MOQ_TEST_CHECK(!moq_sub_track_has_queued_input(f.r->sub, f.bad->sub_track));
    MOQ_TEST_CHECK(moq_sub_fill_is_valid(moq_sub_track_fill(f.bad->sub_track)));
    MOQ_TEST_CHECK(delivery_notices == 0 && delivery_terminals == 0);
    DELIVERY_NEED(moq_media_receiver_poll_object(f.r, &early, sizeof(early)) == MOQ_OK);
    MOQ_TEST_CHECK(early.track == f.bad && early.payload.len == 5 &&
        memcmp(early.payload.data, "early", 5) == 0);
    /* Credit back (the queued requests go out; no fill bytes exist yet),
     * then the facade's retry cancels the failed subscription locally. */
    network(&f);
    pump(&f);
    MOQ_TEST_CHECK(f.bad->sub_track == NULL ||
                   moq_sub_track_get_state(f.bad->sub_track) != MOQ_SUB_TRACK_ACTIVE);
    if (open_child) {
        /* The publisher has not seen the cancel: the child is writable. */
        moq_fetch_object_cfg_t oc;
        moq_fetch_object_cfg_init(&oc);
        oc.object_id = 1;
        moq_loc_metadata_t loc;
        moq_loc_metadata_init_sized(&loc, sizeof(loc));
        loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP;
        loc.timestamp = 12346;
        MOQ_TEST_CHECK(moq_loc_encode_metadata(moq_alloc_default(), f.version,
            MOQ_LOC_PROFILE_04, &loc, &oc.properties) == MOQ_OK);
        MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)late_marker,
            sizeof(late_marker) - 1, &oc.payload) == MOQ_OK);
        MOQ_TEST_CHECK(moq_session_write_fill_object(server(&f), child, &oc, now(&f)) ==
                       MOQ_OK);
        moq_rcbuf_decref(oc.payload);
        moq_rcbuf_decref(oc.properties);
        MOQ_TEST_CHECK(late_ingress == 0);
    }
    settle(&f); delivery_drain(&f);
    if (open_child) MOQ_TEST_CHECK(late_ingress > 0);   /* the bytes did arrive */
    MOQ_TEST_CHECK(delivery_terminals == 1);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_last_origin == MOQ_MEDIA_DELIVERY_LIVE &&
        delivery_ends == 0);
    MOQ_TEST_CHECK(!moq_sub_fill_is_valid(moq_sub_track_fill(f.bad->sub_track)));
    if (moq_media_receiver_poll_object(f.r, &late, sizeof(late)) == MOQ_OK)
        MOQ_TEST_CHECK(late.track != f.bad);
    send_payload(&f, 2, 1, "good", 4, true); settle(&f);
    moq_media_object_t any = {0};
    while (moq_media_receiver_poll_object(f.r, &any, sizeof(any)) == MOQ_OK) {
        if (any.track == f.good) good = any;
        else { MOQ_TEST_CHECK(any.track != f.bad); moq_media_object_cleanup(&any); }
        memset(&any, 0, sizeof(any));
    }
    MOQ_TEST_CHECK(good.track == f.good && good.payload.len == 4);
done:
    moq_media_object_cleanup(&early);
    moq_media_object_cleanup(&late);
    moq_media_object_cleanup(&good);
    finish(&f);
}
static void delivery_parent_cancellation_opened(void) { delivery_parent_cancellation_frontier(true); }
static void delivery_parent_cancellation_unopened(void) { delivery_parent_cancellation_frontier(false); }

static void delivery_live_cancel_refusal(void)
{
    fixture_t f;
    DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, false));
    delivery_big_live(&f, 1, 1);
    send_payload(&f, 2, 1, "sibling", 7, true);
    fill_action_queue(&f);
    pump(&f);
    DELIVERY_NEED(f.tick_result == MOQ_ERR_BUFFER);
    delivery_drain(&f);
    pump(&f);
    MOQ_TEST_CHECK(f.bad->sub_track == f.probe);
    MOQ_TEST_CHECK(moq_sub_release_track(f.r->sub, f.probe) == MOQ_ERR_WRONG_STATE);
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_FAILED);
    moq_media_object_t object = {0};
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_OK);
    MOQ_TEST_CHECK(object.track == f.good && object.payload.len == 7);
    moq_media_object_cleanup(&object);
    settle(&f); delivery_drain(&f);
    MOQ_TEST_CHECK(f.bad->sub_track == NULL && delivery_notices == 1 && delivery_ends == 0);
done:
    finish(&f);
}

static void delivery_catalog(void)
{
    fixture_t f;
    DELIVERY_NEED(start(&f, MOQ_VERSION_DRAFT_22, DELIVERY_BUDGET));
    accept(&f, 0, false); pump(&f);
    DELIVERY_NEED(moq_fill_is_valid(f.peer[0].fill));
    DELIVERY_NEED(moq_session_open_fill(server(&f), f.peer[0].fill, now(&f)) == MOQ_OK);
    delivery_fill_object(&f, 0, 0, DELIVERY_BUDGET + 1);
    pump(&f);
    MOQ_TEST_CHECK(f.tick_result == MOQ_ERR_BUFFER);
    MOQ_TEST_CHECK(moq_media_receiver_is_fatal(f.r));
    MOQ_TEST_CHECK(moq_media_receiver_fatal_code(f.r) == MOQ_MEDIA_RECEIVER_FATAL_CATALOG_UNUSABLE);
    MOQ_TEST_CHECK(moq_media_receiver_wait(f.r, 0) == MOQ_ERR_CLOSED);
    settle(&f);
    MOQ_TEST_CHECK(f.r->catalog_sub == NULL);
    MOQ_TEST_CHECK(!moq_endpoint_is_fatal(f.ep) && !moq_endpoint_is_closed(f.ep));
    MOQ_TEST_CHECK(moq_session_state(client(&f)) == MOQ_SESS_ESTABLISHED);
done:
    finish(&f);
}

static void delivery_zero_initiator(void)
{
    fixture_t f = {0};
    current = &f;
    f.version = MOQ_VERSION_DRAFT_22;
    f.facade_budget = DELIVERY_BUDGET;
    f.facade_objects = 4;
    delivery_notices = delivery_terminals = delivery_ends = 0;
    moq_simpair_cfg_t pc = MOQ_SIMPAIR_CFG_INIT;
    pc.alloc = moq_alloc_default(); pc.version = f.version;
    pc.client_send_request_capacity = pc.server_send_request_capacity = true;
    pc.client_initial_request_capacity = pc.server_initial_request_capacity = 32;
    DELIVERY_NEED(moq_simpair_create(&pc, &f.pair) == MOQ_OK);
    DELIVERY_NEED(moq_simpair_start(f.pair) == MOQ_OK);
    network(&f);
    moq_alloc_t alloc = {.ctx = &f.ledger, .alloc = ledger_alloc, .free = ledger_free};
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    moq_bytes_t ns[] = {MOQ_BYTES_LITERAL("capacity-zero")};
    cfg.namespace_ = (moq_namespace_t){ns, 1};
    cfg.auto_subscribe = true;
    cfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    moq_media_receiver_test_set_alloc(&alloc);
    f.r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    f.ep = moq_endpoint_test_make_bare();
    DELIVERY_NEED(f.r && f.ep);
    f.r->ep = f.ep;
    f.r->transport_version = f.version;
    /* Discover through the existing catalog seam so the first wire request
     * can be the media subscription, with the real initiating ID zero. */
    moq_media_receiver_test_ingest(f.r, 0, 0, catalog, sizeof(catalog) - 1);
    DELIVERY_NEED(f.r->track_count == 2);
    f.bad = f.r->tracks[0]; f.good = f.r->tracks[1];
    delivery_drain(&f);
    moq_sub_cfg_t sc;
    moq_sub_cfg_init_sized(&sc, sizeof(sc));
    sc.callbacks.ctx = f.r;
    sc.callbacks.on_subscribed = receiver_on_subscribed;
    sc.callbacks.on_subscribe_error = receiver_on_subscribe_error;
    sc.on_subscribe_done = receiver_on_subscribe_done;
    DELIVERY_NEED(observed_create(client(&f), &alloc, &sc, &f.r->sub) == MOQ_OK);
    f.r->joins_by_fill = true;
    receiver_reconcile_subscriptions(f.r, now(&f));
    f.probe = f.bad->sub_track;
    moq_sub_track_cfg_t tc;
    moq_sub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = f.r->namespace_;
    tc.track_name = MOQ_BYTES_LITERAL("catalog");
    DELIVERY_NEED(moq_sub_subscribe(f.r->sub, &tc, now(&f), &f.r->catalog_sub) == MOQ_OK);
    network(&f);
    accept(&f, 0, false); accept(&f, 1, true); accept(&f, 2, false);
    pump(&f);
    moq_fill_t failed_child = f.peer[1].fill;
    moq_location_filter_cfg_t location;
    moq_location_filter_cfg_init(&location);
    location.kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    moq_fill_cfg_t fill;
    moq_fill_cfg_init(&fill); fill.location = &location;
    moq_sub_update_cfg_t update;
    moq_sub_update_cfg_init_sized(&update, sizeof(update)); update.fill = &fill;
    DELIVERY_NEED(moq_sub_update_subscription(f.r->sub, f.bad->sub_track, &update, now(&f)) == MOQ_OK);
    network(&f); pump(&f);
    moq_fill_t healthy_child = f.peer[1].fill;
    DELIVERY_NEED(!moq_fill_eq(failed_child, healthy_child));
    f.peer[1].fill = failed_child;
    DELIVERY_NEED(moq_session_open_fill(server(&f), failed_child, now(&f)) == MOQ_OK);
    delivery_fill_object(&f, 1, 0, DELIVERY_BUDGET + 1);
    pump(&f); delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_last_origin == MOQ_MEDIA_DELIVERY_FILL &&
        delivery_last_id == 0);
    settle(&f); delivery_drain(&f);
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_ACTIVE);
    f.peer[1].fill = healthy_child;
    DELIVERY_NEED(moq_session_open_fill(server(&f), healthy_child, now(&f)) == MOQ_OK);
    delivery_fill_object(&f, 1, 0, 4);
    DELIVERY_NEED(moq_session_end_fill(server(&f), healthy_child, now(&f)) == MOQ_OK);
    network(&f); pump(&f); delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 1 && delivery_terminals == 1 &&
        delivery_last_reason == MOQ_FILL_COMPLETE);
    moq_media_object_t object = {0};
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_OK);
    MOQ_TEST_CHECK(object.track == f.bad && object.delivery_origin == MOQ_MEDIA_DELIVERY_FILL &&
        object.initiating_request_id != 0 && object.payload.len == 4);
    moq_media_object_cleanup(&object);
done:
    finish(&f);
}

static void delivery_sizes(void)
{
    const size_t floor = offsetof(old_track_event_t, config_generation) + sizeof(uint32_t);
    for (size_t size = floor; size <= sizeof(moq_media_track_event_t) + 17; size++) {
        fixture_t f;
        DELIVERY_NEED(delivery_start(&f, MOQ_VERSION_DRAFT_22, false));
        /* The track's join fill ends first, so the failure notice is the one
         * event left to poll at each size. */
        DELIVERY_NEED(moq_session_open_fill(server(&f), f.peer[1].fill, now(&f)) == MOQ_OK);
        DELIVERY_NEED(moq_session_end_fill(server(&f), f.peer[1].fill, now(&f)) == MOQ_OK);
        settle(&f); delivery_drain(&f);
        DELIVERY_NEED(!moq_sub_fill_is_valid(moq_sub_track_fill(f.bad->sub_track)));
        delivery_big_live(&f, 1, 1); settle(&f);
        unsigned char *storage = malloc(size + 16);
        DELIVERY_NEED(storage != NULL);
        memset(storage, 0xa5, size + 16);
        MOQ_TEST_CHECK(moq_media_receiver_poll_track(f.r,
            (moq_media_track_event_t *)storage, floor - 1) == MOQ_ERR_INVAL);
        for (size_t i = 0; i < size + 16; i++) MOQ_TEST_CHECK(storage[i] == 0xa5);
        moq_result_t rc = moq_media_receiver_poll_track(f.r,
            (moq_media_track_event_t *)storage, size);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc == MOQ_OK) {
            /* The current event: this block, then the fill loss summary,
             * zero for this kind. */
            size_t n = size < sizeof(moq_media_track_event_t) ? size
                                                              : sizeof(moq_media_track_event_t);
            unsigned char bytes[sizeof(moq_media_track_event_t)] = {0};
            delivery_expected_event_t expected = {0};
            expected.previous.ack.old.struct_size = (uint32_t)n;
            expected.previous.ack.old.kind = (moq_media_track_event_kind_t)DELIVERY_FAILURE;
            expected.previous.ack.old.track = f.bad;
            expected.previous.ack.old.desc = &f.bad->desc;
            expected.previous.ack.local_result = MOQ_ERR_BUFFER;
            expected.delivery_origin = MOQ_MEDIA_DELIVERY_LIVE;
            memcpy(bytes, &expected, sizeof(expected));
            MOQ_TEST_CHECK(memcmp(storage, bytes, n) == 0);
            for (size_t i = n; i < size + 16; i++) MOQ_TEST_CHECK(storage[i] == 0xa5);
        }
        free(storage);
        delivery_drain(&f);
        MOQ_TEST_CHECK(delivery_notices == 0);
done:
        finish(&f);
    }
}

static void delivery_legacy(moq_version_t version)
{
    fixture_t f;
    DELIVERY_NEED(delivery_start(&f, version, false));
    delivery_big_live(&f, 1, 1); pump(&f);
    delivery_drain(&f);
    MOQ_TEST_CHECK(delivery_notices == 0);
    delivery_state(&f, f.bad, MOQ_MEDIA_TRACK_STATE_ACTIVE);
    moq_media_object_t object = {0};
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &object, sizeof(object)) == MOQ_OK);
    MOQ_TEST_CHECK(object.payload.len == DELIVERY_BUDGET + 1);
    moq_media_object_cleanup(&object);
done:
    finish(&f);
}
static void delivery_d16(void) { delivery_legacy(MOQ_VERSION_DRAFT_16); }
static void delivery_d18(void) { delivery_legacy(MOQ_VERSION_DRAFT_18); }

static void delivery_run(const char *name, void (*test)(void))
{
    int before = failures;
    test();
    printf("%s: %s\n", failures == before ? "PASS" : "FAIL", name);
}

int main(void)
{
    delivery_run("live_notice_retained_ownership_healthy_sibling", delivery_live);
    delivery_run("admitted_facade_object_survives_failed_generation", delivery_facade_retained);
    delivery_run("fill_notice_parent_active_no_duplicate_cancel_terminal", delivery_fill);
    delivery_run("discovery_ring_pressure_refused_cancel_recovery", delivery_pressure);
    delivery_run("real_peer_terminal_cause_after_local_failure", delivery_peer_terminal);
    delivery_run("temporary_byte_pressure_without_new_input", delivery_temporary_pressure);
    delivery_run("retained_ack_floor_is_causal", delivery_metadata_floor);
    delivery_run("live_cancel_refusal_keeps_identity_and_sibling_progress", delivery_live_cancel_refusal);
    delivery_run("failed_parent_waits_for_its_open_child", delivery_parent_cancellation_opened);
    delivery_run("failed_parent_waits_for_its_unopened_child", delivery_parent_cancellation_unopened);
    delivery_run("catalog_dependency_capacity_failure", delivery_catalog);
    delivery_run("zero_initiator_and_healthy_sibling_fill", delivery_zero_initiator);
    delivery_run("old_partial_future_output_guards", delivery_sizes);
    delivery_run("d16_capacity_policy_unchanged", delivery_d16);
    delivery_run("d18_capacity_policy_unchanged", delivery_d18);
    printf("receiver delivery failure: %d failures\n", failures);
    return failures ? 1 : 0;
}
