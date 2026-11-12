/* The measured output is independent of the current public object sizeof.
 * Layout-only mode has no dependency on the C-only runtime harness. */
#if defined(MOQ_RECEIVER_ORIGIN_LAYOUT_ONLY)
#include <moq/media_receiver.h>
#include <stddef.h>
typedef struct frozen_media_object {
    uint32_t struct_size;
    moq_media_track_t *track;
    uint32_t config_generation;
    moq_media_packaging_t packaging;
    moq_object_status_t status;
    bool end_of_group, datagram, keyframe, has_capture_time;
    uint64_t capture_time_us, decode_time_us;
    int64_t composition_offset_us;
    uint64_t presentation_time_us;
    moq_bytes_t payload, fragment;
    size_t mdat_offset, mdat_len, sample_count;
    const moq_cmaf_sample_t *samples;
    moq_rcbuf_t *payload_ref, *properties_ref;
    moq_cmaf_sample_t *samples_owned;
    uint64_t group_id, subgroup_id, object_id;
    uint8_t publisher_priority;
    bool has_subgroup;
} frozen_media_object_t;
typedef struct loss_report_media_object {
    uint32_t struct_size;
    moq_media_track_t *track;
    uint32_t config_generation;
    moq_media_packaging_t packaging;
    moq_object_status_t status;
    bool end_of_group, datagram, keyframe, has_capture_time;
    uint64_t capture_time_us, decode_time_us;
    int64_t composition_offset_us;
    uint64_t presentation_time_us;
    moq_bytes_t payload, fragment;
    size_t mdat_offset, mdat_len, sample_count;
    const moq_cmaf_sample_t *samples;
    moq_rcbuf_t *payload_ref, *properties_ref;
    moq_cmaf_sample_t *samples_owned;
    uint64_t group_id, subgroup_id, object_id;
    uint8_t publisher_priority;
    bool has_subgroup;
    uint8_t reserved_obj[6];
    bool has_loss_report, gap;
    uint64_t gap_group, gap_object;
    bool gap_tail_unknown, gap_duration_known;
    uint64_t gap_duration_27mhz;
    bool discontinuous;
} loss_report_media_object_t;
#else
/* Reuse the existing private receiver harness without a production seam. */
#define main receiver_origin_companion_controls_main
#include "test_media_receiver_companion.c"
#undef main
#endif

#ifdef __cplusplus
#define ORIGIN_ASSERT static_assert
#else
#define ORIGIN_ASSERT _Static_assert
#endif

typedef struct origin_previous_object {
    loss_report_media_object_t base;
    moq_media_metadata_t metadata;
    moq_rcbuf_t *track_properties_ref;
} origin_previous_object_t;

typedef struct origin_expected_object {
    origin_previous_object_t previous;
    uint32_t delivery_origin;
    uint64_t initiating_request_id;
} origin_expected_object_t;

enum { EXPECTED_LIVE = 1, EXPECTED_FILL = 2 };
ORIGIN_ASSERT(offsetof(moq_media_object_t, metadata) ==
    offsetof(origin_previous_object_t, metadata), "old metadata offset");
ORIGIN_ASSERT(offsetof(moq_media_object_t, track_properties_ref) ==
    offsetof(origin_previous_object_t, track_properties_ref), "old ownership offset");
#ifndef MOQ_RECEIVER_ORIGIN_BASELINE
ORIGIN_ASSERT(MOQ_MEDIA_DELIVERY_LIVE == EXPECTED_LIVE &&
    MOQ_MEDIA_DELIVERY_FILL == EXPECTED_FILL, "fixed origin values");
ORIGIN_ASSERT(sizeof(((moq_media_object_t *)0)->delivery_origin) == 4, "origin width");
ORIGIN_ASSERT(sizeof(((moq_media_object_t *)0)->initiating_request_id) == 8, "initiator width");
ORIGIN_ASSERT(offsetof(moq_media_object_t, delivery_origin) ==
    sizeof(origin_previous_object_t), "no previous allocation padding reuse");
ORIGIN_ASSERT(offsetof(moq_media_object_t, initiating_request_id) ==
    offsetof(origin_expected_object_t, initiating_request_id), "initiator offset");
/* The extent through initiating_request_id is complete: the LOC-05 block
 * starts after it. */
ORIGIN_ASSERT(offsetof(moq_media_object_t, loc_ext_present) ==
    sizeof(origin_expected_object_t), "output extent through initiating_request_id");
#endif

#ifdef MOQ_RECEIVER_ORIGIN_LAYOUT_ONLY
int main(void) { return 0; }
#else
static bool origin_check(bool ok, const char *expr, int line)
{
    if (!ok) {
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, line, expr);
        failures++;
    }
    return ok;
}
#define ORIGIN_CHECK(x) origin_check((x), #x, __LINE__)
#define ORIGIN_REQUIRE(x) do { if (!ORIGIN_CHECK(x)) return false; } while (0)
#define ORIGIN_NEED(x) do { if (!ORIGIN_CHECK(x)) goto done; } while (0)

static const char origin_catalog[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\"}]}";
static const uint8_t origin_body[] = {0x31, 0, 0x33};
static const uint64_t origin_group = (UINT64_C(1) << 40) + 7;
static const uint64_t origin_subgroup = (UINT64_C(1) << 42) + 9;
static const uint64_t origin_object = (UINT64_C(1) << 36) + 2;
static const uint8_t origin_video[] = {0xa4, 0, 0xb5};
static const uint8_t origin_fragment[] = {
    0,0,0,100,'m','o','o','f', 0,0,0,92,'t','r','a','f',
    0,0,0,16,'t','f','h','d', 0,0,0,0, 0,0,0,1,
    0,0,0,20,'t','f','d','t', 1,0,0,0, 0,0,0,0, 0,1,0x5f,0x90,
    0,0,0,48,'t','r','u','n', 0,0,0x0f,0, 0,0,0,2,
    0,0,0x11,0x94, 0,0,0,2, 2,0,0,0, 0xff,0xff,0xee,0x6c,
    0,0,0x11,0x94, 0,0,0,2, 2,0,0,0, 0,0,0,0,
    0,0,0,12,'m','d','a','t', 0x41,0x42,0x43,0x44
};

typedef struct origin_fixture {
    test_alloc_state_t state;
    moq_alloc_t alloc;
    moq_media_receiver_t *receiver;
    moq_media_track_t *track;
    moq_simpair_t *pair;
    moq_subscription_t peer_sub;
    moq_fill_t fill[2];
    uint64_t initiator[2];
    unsigned fills;
    bool cmaf;
} origin_fixture_t;

static bool origin_start(origin_fixture_t *f, moq_version_t version)
{
    memset(f, 0, sizeof(*f));
    f->alloc = test_allocator(&f->state);
    moq_media_receiver_cfg_t cfg;
    init_cfg(&cfg, version == MOQ_VERSION_DRAFT_22 ?
        MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01);
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->receiver = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    ORIGIN_REQUIRE(f->receiver != NULL);
    f->receiver->transport_version = version;
    moq_media_receiver_test_ingest(f->receiver, 0, 0, origin_catalog, sizeof(origin_catalog) - 1);
    moq_media_track_event_t event;
    for (unsigned n = 0; n < 8; n++) {
        if (moq_media_receiver_poll_track(f->receiver, &event, sizeof(event)) != MOQ_OK) break;
        if (event.kind == MOQ_MEDIA_TRACK_ADDED) f->track = event.track;
    }
    ORIGIN_REQUIRE(f->track && !moq_media_receiver_is_fatal(f->receiver));
    return true;
}

static void origin_destroy_sources(origin_fixture_t *f)
{
    if (f->receiver) moq_media_receiver_test_destroy_with_session(f->receiver,
        f->pair ? moq_simpair_client(f->pair) : NULL,
        f->pair ? moq_simpair_now_us(f->pair) : 0);
    f->receiver = NULL;
    if (f->pair) moq_simpair_destroy(f->pair);
    f->pair = NULL;
}

static void origin_finish(origin_fixture_t *f)
{
    origin_destroy_sources(f);
    ORIGIN_CHECK(f->state.balance == 0);
}

static bool origin_live_input(origin_fixture_t *f)
{
    moq_sub_object_t input = {0};
    input.status = MOQ_OBJECT_NORMAL;
    input.group_id = (UINT64_C(1) << 40) + 7;
    input.subgroup_id = (UINT64_C(1) << 42) + 9;
    input.object_id = (UINT64_C(1) << 36) + 2;
    input.publisher_priority = 213;
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
    loc.timestamp = 12345;
    loc.marking_len = 1;
    loc.marking[0] = 0x20;
    moq_version_t version = f->receiver->transport_version;
    ORIGIN_REQUIRE(moq_loc_encode_metadata(&f->alloc, version,
        version == MOQ_VERSION_DRAFT_22 ? MOQ_LOC_PROFILE_04 : MOQ_LOC_PROFILE_01,
        &loc, &input.properties) == MOQ_OK);
    moq_result_t rc = moq_rcbuf_create(&f->alloc, origin_body, sizeof(origin_body), &input.payload);
    if (rc != MOQ_OK) moq_sub_object_cleanup(&input);
    ORIGIN_REQUIRE(rc == MOQ_OK);
    receiver_on_media_object(f->receiver, f->track, &input, 1000);
    ORIGIN_REQUIRE(f->receiver->obj_tail - f->receiver->obj_head == 1);
    return true;
}

static void origin_old_extent_refused(void)
{
    origin_fixture_t f;
    ORIGIN_NEED(origin_start(&f, MOQ_VERSION_DRAFT_22));
    ORIGIN_NEED(origin_live_input(&f));
    origin_expected_object_t out, before;
    memset(&out, 0xa5, sizeof(out));
    memcpy(&before, &out, sizeof(before));
    moq_result_t rc = moq_media_receiver_poll_object(f.receiver,
        (moq_media_object_t *)&out, sizeof(origin_previous_object_t));
    printf("ORACLE prior_full_extent=%zu poll=%d\n", sizeof(origin_previous_object_t), rc);
    if (rc == MOQ_OK) moq_media_object_cleanup((moq_media_object_t *)&out);
    ORIGIN_NEED(rc == MOQ_ERR_UNSUPPORTED);
    ORIGIN_NEED(memcmp(&out, &before, sizeof(out)) == 0);
    ORIGIN_NEED(f.receiver->obj_tail - f.receiver->obj_head == 1);
done:
    origin_finish(&f);
}

static void origin_live_observation(void)
{
    origin_fixture_t f;
    origin_expected_object_t out;
    memset(&out, 0, sizeof(out));
    ORIGIN_NEED(origin_start(&f, MOQ_VERSION_DRAFT_22));
    ORIGIN_NEED(origin_live_input(&f));
    ORIGIN_NEED(moq_media_receiver_poll_object(f.receiver, (moq_media_object_t *)&out,
        sizeof(out)) == MOQ_OK);
    printf("ORACLE live_stamp=%u expected=%zu origin=%u\n", out.previous.base.struct_size,
        sizeof(out), out.delivery_origin);
    ORIGIN_NEED(out.previous.base.struct_size == sizeof(out));
    ORIGIN_NEED(out.delivery_origin == EXPECTED_LIVE && out.initiating_request_id == 0);
    ORIGIN_NEED(bytes_equal(out.previous.base.payload, origin_body, sizeof(origin_body)));
done:
    moq_media_object_cleanup((moq_media_object_t *)&out);
    moq_media_object_cleanup((moq_media_object_t *)&out);
    origin_finish(&f);
}

static uint64_t origin_now(origin_fixture_t *f) { return moq_simpair_now_us(f->pair); }
static moq_session_t *origin_server(origin_fixture_t *f) { return moq_simpair_server(f->pair); }

static bool origin_network(origin_fixture_t *f)
{
    for (unsigned round = 0; round < 3; round++) {
        ORIGIN_REQUIRE(moq_simpair_run_until_quiescent(f->pair, 64, NULL) == MOQ_OK);
        unsigned n;
        for (n = 0; n < 64; n++) {
            moq_event_t ev;
            if (moq_session_poll_events(origin_server(f), &ev, 1) == 0) break;
            moq_result_t rc = MOQ_OK;
            if (ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST) {
                bool media = ev.u.subscribe_request.track_name.len == 1 &&
                    ev.u.subscribe_request.track_name.data[0] == 'v';
                moq_accept_subscribe_cfg_t ac;
                moq_accept_subscribe_cfg_init(&ac);
                moq_rcbuf_t *properties = NULL;
                if (media) {
                    f->peer_sub = ev.u.subscribe_request.sub;
                    ac.has_largest = true;
                    ac.largest_group = origin_group;
                    ac.largest_object = origin_object + 10;
                    moq_loc_metadata_t loc;
                    moq_loc_metadata_init_sized(&loc, sizeof(loc));
                    loc.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_TIMESCALE;
                    loc.video_config = (moq_bytes_t){origin_video, sizeof(origin_video)};
                    loc.timescale = 1000;
                    rc = moq_loc_encode_metadata(&f->alloc, MOQ_VERSION_DRAFT_22,
                        MOQ_LOC_PROFILE_04, &loc, &properties);
                    if (rc == MOQ_OK)
                        ac.track_properties = (moq_bytes_t){moq_rcbuf_data(properties), moq_rcbuf_len(properties)};
                }
                if (rc == MOQ_OK)
                    rc = moq_session_accept_subscribe(origin_server(f), ev.u.subscribe_request.sub,
                        &ac, origin_now(f));
                moq_rcbuf_decref(properties);
            } else if (ev.kind == MOQ_EVENT_FILL_REQUEST) {
                if (f->fills >= 2) rc = MOQ_ERR_LIMIT;
                else {
                    f->fill[f->fills] = ev.u.fill_request.fill;
                    f->initiator[f->fills++] = ev.u.fill_request.initiating_request_id;
                }
            }
            moq_event_cleanup(&ev);
            ORIGIN_REQUIRE(rc == MOQ_OK);
        }
        ORIGIN_REQUIRE(n < 64);
    }
    return true;
}

static void origin_fill_config(moq_location_filter_cfg_t *location, moq_fill_cfg_t *fill)
{
    moq_location_filter_cfg_init(location);
    location->kind = MOQ_LOCATION_FILTER_ABSOLUTE_START;
    location->start_group = origin_group;
    location->start_object = origin_object;
    moq_fill_cfg_init(fill);
    fill->location = location;
}

static bool origin_wire_start(origin_fixture_t *f, bool cmaf)
{
    ORIGIN_REQUIRE(origin_start(f, MOQ_VERSION_DRAFT_22));
    f->cmaf = cmaf;
    if (cmaf) {
        f->track->desc.info.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        f->track->desc.info.timescale = 90000;
    }
    f->pair = pair(&f->alloc, MOQ_VERSION_DRAFT_22);
    ORIGIN_REQUIRE(f->pair != NULL);
    moq_sub_cfg_t sc;
    moq_sub_cfg_init_sized(&sc, sizeof(sc));
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
    /* Put the media request first to exercise the valid initiator zero. The
     * catalog was ingested via the existing test seam; bootstrap policy is not
     * being qualified here. All media objects below traverse real wire events. */
    ORIGIN_REQUIRE(moq_media_receiver_subscribe_track(f->receiver, f->track, &media) == MOQ_OK);
    f->receiver->joins_by_fill = true;
    receiver_reconcile_subscriptions(f->receiver, origin_now(f));
    ORIGIN_REQUIRE(f->track->sub_track != NULL);
    moq_sub_track_cfg_t tc;
    moq_sub_track_cfg_init_sized(&tc, sizeof(tc));
    tc.track_namespace = f->receiver->namespace_;
    tc.track_name = MOQ_BYTES_LITERAL("catalog");
    ORIGIN_REQUIRE(moq_sub_subscribe(f->receiver->sub, &tc, origin_now(f),
        &f->receiver->catalog_sub) == MOQ_OK);
    ORIGIN_REQUIRE(origin_network(f));
    receiver_hook(NULL, moq_simpair_client(f->pair), origin_now(f), f->receiver);
    ORIGIN_REQUIRE(f->fills == 1 && f->initiator[0] == 0);
    ORIGIN_REQUIRE(moq_sub_track_is_active(f->track->sub_track));
    moq_sub_update_cfg_t update;
    moq_sub_update_cfg_init_sized(&update, sizeof(update));
    update.fill = &fill;
    ORIGIN_REQUIRE(moq_sub_update_subscription(f->receiver->sub, f->track->sub_track,
        &update, origin_now(f)) == MOQ_OK);
    ORIGIN_REQUIRE(origin_network(f));
    ORIGIN_REQUIRE(f->fills == 2 && f->initiator[1] == 4);
    return true;
}

static bool origin_buffers(origin_fixture_t *f, moq_rcbuf_t **payload, moq_rcbuf_t **properties)
{
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING;
    loc.timestamp = 9000;
    loc.marking_len = 1;
    loc.marking[0] = 0x20;
    ORIGIN_REQUIRE(moq_loc_encode_metadata(&f->alloc, MOQ_VERSION_DRAFT_22,
        MOQ_LOC_PROFILE_04, &loc, properties) == MOQ_OK);
    moq_result_t rc = moq_rcbuf_create(&f->alloc,
        f->cmaf ? origin_fragment : origin_body,
        f->cmaf ? sizeof(origin_fragment) : sizeof(origin_body), payload);
    if (rc != MOQ_OK) { moq_rcbuf_decref(*properties); *properties = NULL; }
    ORIGIN_REQUIRE(rc == MOQ_OK);
    return true;
}

static bool origin_send_fill(origin_fixture_t *f, unsigned child)
{
    ORIGIN_REQUIRE(moq_session_open_fill(origin_server(f), f->fill[child], origin_now(f)) == MOQ_OK);
    moq_fetch_object_cfg_t oc;
    moq_fetch_object_cfg_init(&oc);
    oc.group_id = origin_group;
    oc.subgroup_id = origin_subgroup;
    oc.object_id = origin_object;
    oc.publisher_priority = 213;
    ORIGIN_REQUIRE(origin_buffers(f, &oc.payload, &oc.properties));
    moq_result_t rc = moq_session_write_fill_object(origin_server(f), f->fill[child], &oc, origin_now(f));
    moq_rcbuf_decref(oc.payload);
    moq_rcbuf_decref(oc.properties);
    ORIGIN_REQUIRE(rc == MOQ_OK);
    ORIGIN_REQUIRE(moq_session_end_fill(origin_server(f), f->fill[child], origin_now(f)) == MOQ_OK);
    ORIGIN_REQUIRE(origin_network(f));
    return true;
}

static bool origin_send_live(origin_fixture_t *f)
{
    moq_subgroup_cfg_t sc;
    moq_subgroup_cfg_init(&sc);
    sc.group_id = origin_group;
    sc.subgroup_id = origin_subgroup;
    sc.publisher_priority = 213;
    sc.object_properties = true;
    moq_subgroup_handle_t subgroup_handle;
    ORIGIN_REQUIRE(moq_session_open_subgroup(origin_server(f), f->peer_sub, &sc,
        origin_now(f), &subgroup_handle) == MOQ_OK);
    moq_object_cfg_t oc;
    moq_object_cfg_init(&oc);
    oc.object_id = origin_object;
    ORIGIN_REQUIRE(origin_buffers(f, &oc.payload, &oc.properties));
    moq_result_t rc = moq_session_write_object_ex(origin_server(f), subgroup_handle, &oc, origin_now(f));
    moq_rcbuf_decref(oc.payload);
    moq_rcbuf_decref(oc.properties);
    ORIGIN_REQUIRE(rc == MOQ_OK);
    ORIGIN_REQUIRE(moq_session_close_subgroup(origin_server(f), subgroup_handle, origin_now(f)) == MOQ_OK);
    ORIGIN_REQUIRE(origin_network(f));
    return true;
}

/* `stamp` is the struct_size the caller's size yields: the size through
 * initiating_request_id for that exact caller, the current size for any
 * larger one. */
static bool origin_output_valid(const origin_expected_object_t *out, size_t stamp,
                                bool cmaf, uint32_t origin, uint64_t initiator)
{
    const loss_report_media_object_t *base = &out->previous.base;
    ORIGIN_REQUIRE(base->struct_size == stamp);
    ORIGIN_REQUIRE(out->delivery_origin == origin && out->initiating_request_id == initiator);
    ORIGIN_REQUIRE(base->group_id == origin_group && base->subgroup_id == origin_subgroup &&
        base->object_id == origin_object && base->publisher_priority == 213 &&
        base->has_subgroup && !base->datagram);
    ORIGIN_REQUIRE(out->previous.track_properties_ref &&
        moq_rcbuf_refcount(out->previous.track_properties_ref) == 1);
    ORIGIN_REQUIRE(bytes_equal(out->previous.metadata.loc.video_config, origin_video, sizeof(origin_video)));
    ORIGIN_REQUIRE(out->previous.metadata.loc.timescale == 1000);
    if (cmaf) {
        ORIGIN_REQUIRE(bytes_equal(base->fragment, origin_fragment, sizeof(origin_fragment)));
        ORIGIN_REQUIRE(base->sample_count == 2 && base->samples && base->samples_owned);
        ORIGIN_REQUIRE(base->samples[0].size == 2 && base->samples[1].size == 2 &&
            base->samples[0].duration == 4500 && base->samples[1].duration == 4500);
        ORIGIN_REQUIRE(base->mdat_len == 4 && base->mdat_offset + base->mdat_len <= base->fragment.len);
        ORIGIN_REQUIRE(memcmp(base->fragment.data + base->mdat_offset, "ABCD", 4) == 0);
        ORIGIN_REQUIRE(base->decode_time_us == 1000000 && base->presentation_time_us == 950000);
    } else {
        ORIGIN_REQUIRE(bytes_equal(base->payload, origin_body, sizeof(origin_body)));
        ORIGIN_REQUIRE(base->sample_count == 0 && base->samples == NULL);
    }
    return true;
}

/* The prefix through initiating_request_id of a held current record, copied
 * for validation. The copy only borrows the record's resources and is never
 * cleaned up. */
static origin_expected_object_t origin_prefix(const moq_media_object_t *held)
{
    origin_expected_object_t prefix;
    memcpy(&prefix, held, sizeof(prefix));
    return prefix;
}

static void origin_wire_overlap(bool cmaf)
{
    origin_fixture_t f;
    /* Full current records: a caller sized through initiating_request_id fills
     * that extent of one, and a larger caller fills all of it. */
    moq_media_object_t held[3] = {0};
    size_t stamp[3] = {0};
    ORIGIN_NEED(origin_wire_start(&f, cmaf));
    ORIGIN_NEED(origin_send_fill(&f, 0));
    ORIGIN_NEED(origin_send_live(&f));
    ORIGIN_NEED(origin_send_fill(&f, 1));
    /* Sample tables use the production default allocator used by public
     * cleanup. Keep the counting allocator for receiver construction/teardown. */
    moq_alloc_t saved = f.receiver->alloc;
    if (cmaf) f.receiver->alloc = *moq_alloc_default();
    receiver_hook(NULL, moq_simpair_client(f.pair), origin_now(&f), f.receiver);
    f.receiver->alloc = saved;
    ORIGIN_NEED(!moq_media_receiver_is_fatal(f.receiver));
    ORIGIN_NEED(moq_sub_track_is_active(f.track->sub_track));
    ORIGIN_NEED(!moq_sub_fill_is_valid(moq_sub_track_fill(f.track->sub_track)));
    ORIGIN_NEED(f.receiver->obj_tail - f.receiver->obj_head == 3);
    uint64_t bytes = f.receiver->obj_sizes_total, budget = f.receiver->obj_budget_total;
    moq_rcbuf_t *head = f.receiver->objs[f.receiver->obj_head % f.receiver->ring_cap].obj.payload_ref;
    uint32_t refs = moq_rcbuf_refcount(head);
    for (size_t size = sizeof(origin_previous_object_t); size < sizeof(origin_expected_object_t); size++) {
        origin_expected_object_t refused, before;
        memset(&refused, 0xa5, sizeof(refused));
        memcpy(&before, &refused, sizeof(before));
        moq_result_t rc = moq_media_receiver_poll_object(f.receiver, (moq_media_object_t *)&refused, size);
        if (rc == MOQ_OK) moq_media_object_cleanup((moq_media_object_t *)&refused);
        ORIGIN_NEED(rc == MOQ_ERR_UNSUPPORTED);
        ORIGIN_NEED(memcmp(&refused, &before, sizeof(refused)) == 0);
        ORIGIN_NEED(f.receiver->obj_tail - f.receiver->obj_head == 3 &&
            f.receiver->obj_sizes_total == bytes && f.receiver->obj_budget_total == budget);
        ORIGIN_NEED(moq_rcbuf_refcount(head) == refs);
    }
    /* Objects 0 and 2 go to callers sized exactly through
     * initiating_request_id; object 1 to a caller larger than the current
     * record. Nothing past each stamp is written. */
    for (unsigned i = 0; i < 3; i++) {
        struct { moq_media_object_t object; uint8_t tail[17]; } future;
        memset(&future, 0xa5, sizeof(future));
        size_t size = i == 1 ? sizeof(future) : sizeof(origin_expected_object_t);
        stamp[i] = i == 1 ? sizeof(future.object) : sizeof(origin_expected_object_t);
        ORIGIN_NEED(moq_media_receiver_poll_object(f.receiver, &future.object, size) == MOQ_OK);
        held[i] = future.object;
        for (size_t byte = stamp[i]; byte < sizeof(future); byte++)
            ORIGIN_NEED(((const uint8_t *)&future)[byte] == 0xa5);
        origin_expected_object_t prefix = origin_prefix(&held[i]);
        ORIGIN_NEED(origin_output_valid(&prefix, stamp[i], cmaf,
            i == 0 ? EXPECTED_LIVE : EXPECTED_FILL, i == 2 ? f.initiator[1] : 0));
    }
    /* The larger caller's LOC-05 block: no Codec String on this track. */
    ORIGIN_NEED(held[1].loc_ext_present == 0 && held[1].codec_string.data == NULL);
    ORIGIN_NEED(held[0].track_properties_ref != held[1].track_properties_ref &&
        held[1].track_properties_ref != held[2].track_properties_ref &&
        held[0].track_properties_ref != held[2].track_properties_ref);
    ORIGIN_NEED(f.receiver->obj_sizes_total == 0 && f.receiver->obj_budget_total == 0);
    origin_destroy_sources(&f);
    for (unsigned i = 0; i < 3; i++) {
        origin_expected_object_t prefix = origin_prefix(&held[i]);
        ORIGIN_NEED(origin_output_valid(&prefix, stamp[i], cmaf,
            i == 0 ? EXPECTED_LIVE : EXPECTED_FILL, i == 2 ? 4 : 0));
    }
done:
    for (unsigned i = 0; i < 3; i++) {
        moq_media_object_cleanup(&held[i]);
        moq_media_object_cleanup(&held[i]);
    }
    origin_finish(&f);
}

static void origin_raw_overlap(void) { origin_wire_overlap(false); }
static void origin_cmaf_overlap(void) { origin_wire_overlap(true); }

static void origin_legacy(moq_version_t version)
{
    origin_fixture_t f;
    ORIGIN_NEED(origin_start(&f, version));
    const size_t sizes[] = {MEDIA_OBJECT_V0_SIZE, sizeof(frozen_media_object_t),
        sizeof(origin_previous_object_t), sizeof(origin_expected_object_t)};
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); i++) {
        ORIGIN_NEED(origin_live_input(&f));
        struct { origin_expected_object_t object; uint8_t tail[17]; } future;
        memset(&future, 0xa5, sizeof(future));
        moq_result_t rc = moq_media_receiver_poll_object(f.receiver, (moq_media_object_t *)&future, sizes[i]);
        ORIGIN_NEED(rc == MOQ_OK);
        bool ok = future.object.previous.base.struct_size == sizes[i] &&
            bytes_equal(future.object.previous.base.payload, origin_body, sizeof(origin_body));
        if (sizes[i] == sizeof(origin_expected_object_t))
            ok = ok && future.object.delivery_origin == EXPECTED_LIVE &&
                future.object.initiating_request_id == 0;
        for (size_t byte = sizes[i]; byte < sizeof(future); byte++)
            ok = ok && ((const uint8_t *)&future)[byte] == 0xa5;
        moq_media_object_cleanup((moq_media_object_t *)&future);
        moq_media_object_cleanup((moq_media_object_t *)&future);
        for (size_t byte = sizes[i]; byte < sizeof(future); byte++)
            ok = ok && ((const uint8_t *)&future)[byte] == 0xa5;
        ORIGIN_NEED(ok);
    }
done:
    origin_finish(&f);
}
static void origin_d16(void) { origin_legacy(MOQ_VERSION_DRAFT_16); }
static void origin_d18(void) { origin_legacy(MOQ_VERSION_DRAFT_18); }

static void origin_run(const char *name, void (*test)(void))
{
    int before = failures;
    test();
    printf("%s: %s\n", failures == before ? "PASS" : "FAIL", name);
}

#ifndef MOQ_RECEIVER_ORIGIN_MAIN
#define MOQ_RECEIVER_ORIGIN_MAIN main
#endif
int MOQ_RECEIVER_ORIGIN_MAIN(void)
{
    origin_run("prior_full_D22_extent_refused_without_transfer", origin_old_extent_refused);
    origin_run("complete_live_observation", origin_live_observation);
    origin_run("raw_real_wire_overlapping_origins_and_owned_lifetime", origin_raw_overlap);
    origin_run("cmaf_two_samples_keep_origin_and_owned_lifetime", origin_cmaf_overlap);
    origin_run("d16_old_output_and_cleanup", origin_d16);
    origin_run("d18_old_output_and_cleanup", origin_d18);
    printf("receiver_origin: %d failures\n", failures);
    return failures ? 1 : 0;
}
#endif
