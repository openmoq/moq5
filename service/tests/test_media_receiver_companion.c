/* Receiver companion sizing and ownership. The production conversion is
 * compiled here so the test can feed a real facade object without exporting
 * another seam. Link endpoint support, not a second media_receiver object. */
#include <moq/media_receiver.h>
#include <stddef.h>
#include "receiver_legacy_ts_caller.h"

typedef struct frozen_receiver_cfg {
    uint32_t struct_size;
    const moq_endpoint_cfg_t *endpoint;
    moq_namespace_t namespace_;
    moq_bytes_t catalog_track;
    bool auto_subscribe;
    moq_media_time_mode_t time_mode;
    moq_media_overflow_cfg_t overflow;
    uint32_t max_track_events;
} frozen_receiver_cfg_t;

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

/* The object as it stood before metadata: the frozen fields, the reserved
 * slot over their tail padding, then the loss report. */
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

_Static_assert(offsetof(moq_media_receiver_cfg_t, max_track_events) ==
               offsetof(frozen_receiver_cfg_t, max_track_events), "old cfg fields unchanged");
_Static_assert(offsetof(moq_media_receiver_cfg_t, companion_profile) >=
               sizeof(frozen_receiver_cfg_t), "no old cfg padding reuse");
_Static_assert(offsetof(moq_media_receiver_cfg_t, mpeg2ts_join_psi) ==
               offsetof(ts_receiver_cfg_t, mpeg2ts_join_psi), "MPEG2TS block unchanged");
_Static_assert(offsetof(moq_media_receiver_cfg_t, companion_profile) >=
               sizeof(ts_receiver_cfg_t),
               "companion follows the complete MPEG2TS-era allocation");
_Static_assert(offsetof(moq_media_object_t, has_loss_report) >=
               sizeof(frozen_media_object_t), "loss report after complete frozen object");
_Static_assert(offsetof(moq_media_object_t, discontinuous) ==
               offsetof(loss_report_media_object_t, discontinuous), "loss report unchanged");
_Static_assert(offsetof(moq_media_object_t, metadata) == sizeof(loss_report_media_object_t),
               "metadata starts after complete loss-report object sizeof");
_Static_assert(offsetof(moq_media_object_t, has_subgroup) ==
               offsetof(frozen_media_object_t, has_subgroup), "old identity unchanged");

#ifdef MOQ_RECEIVER_COMPANION_LAYOUT_ONLY
int main(void) { return 0; }
#else
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"
#include <moq/sim.h>
#include "test_support.h"
#include "test_alloc_support.h"

static int failures;
static moq_bytes_t ns[] = {
    { .data = (const uint8_t *)"companion", .len = sizeof("companion") - 1 }
};

static bool bytes_equal(moq_bytes_t bytes, const uint8_t *expected, size_t len)
{
    return bytes.len == len && bytes.data && memcmp(bytes.data, expected, len) == 0;
}

_Static_assert(MOQ_MEDIA_RECEIVER_CFG_V0_SIZE == sizeof(frozen_receiver_cfg_t),
               "initializer and validation use the complete frozen floor");

static void init_cfg(moq_media_receiver_cfg_t *cfg, uint32_t profile)
{
    memset(cfg, 0, sizeof(*cfg));
    moq_media_receiver_cfg_init_live(cfg);
    cfg->namespace_ = (moq_namespace_t){ ns, 1 };
    cfg->auto_subscribe = true;
    cfg->companion_profile = profile;
    cfg->struct_size = sizeof(*cfg);
}

static void test_old_initializers(void)
{
    void (*const init[])(moq_media_receiver_cfg_t *) = {
        moq_media_receiver_cfg_init, moq_media_receiver_cfg_init_live,
        moq_media_receiver_cfg_init_flow_control
    };
    const moq_media_overflow_policy_t policies[] = {
        MOQ_MEDIA_OVERFLOW_UNSET, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME,
        MOQ_MEDIA_OVERFLOW_FLOW_CONTROL
    };
    for (size_t i = 0; i < sizeof(init) / sizeof(*init); i++) {
        frozen_receiver_cfg_t *old = malloc(sizeof(*old));
        MOQ_TEST_CHECK(old != NULL);
        if (!old) return;
        memset(old, 0xa5, sizeof(*old));
        init[i]((moq_media_receiver_cfg_t *)old);
        MOQ_TEST_CHECK(old->struct_size == sizeof(*old));
        MOQ_TEST_CHECK(old->overflow.policy == policies[i]);
        MOQ_TEST_CHECK(old->endpoint == NULL && old->namespace_.parts == NULL);
        old->namespace_ = (moq_namespace_t){ ns, 1 };
        moq_media_receiver_t *r = NULL;
        moq_result_t rc = moq_media_receiver_test_construct((moq_media_receiver_cfg_t *)old, &r);
        MOQ_TEST_CHECK(rc == (i == 0 ? MOQ_ERR_INVAL : MOQ_OK));
        if (r) {
            MOQ_TEST_CHECK(r->companion_profile == MOQ_MEDIA_COMPANION_AUTO);
            moq_media_receiver_test_free(r);
        }
        free(old);
        moq_media_receiver_cfg_t full;
        memset(&full, 0xa5, sizeof(full));
        init[i](&full);
        const uint8_t *bytes = (const uint8_t *)&full;
        for (size_t j = sizeof(frozen_receiver_cfg_t); j < sizeof(full); j++)
            MOQ_TEST_CHECK(bytes[j] == 0xa5);
    }
    MOQ_TEST_PASS("receiver_old_initializers");
}

/* The old caller is in another TU; these private implementation observations
 * remain here, without exporting a product seam. */
static unsigned ts_probe_calls;
static void probe_ts_sized_caller(const void *old)
{
    ts_probe_calls++;
    MOQ_TEST_CHECK(receiver_cfg_companion(old) == MOQ_MEDIA_COMPANION_AUTO);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_test_construct(
        (const moq_media_receiver_cfg_t *)old, &r);
    MOQ_TEST_CHECK(rc == MOQ_OK && r != NULL);
    if (r) {
        MOQ_TEST_CHECK(r->companion_profile == MOQ_MEDIA_COMPANION_AUTO);
        moq_media_receiver_test_free(r);
    }
}

/* Exact old-size allocation and both padding patterns cross the real C ABI. */
static void test_ts_sized_caller(void)
{
    ts_probe_calls = 0;
    int caller_failures = moq_test_receiver_legacy_ts_caller(
        (moq_namespace_t){ ns, 1 }, probe_ts_sized_caller);
    failures += caller_failures;
    MOQ_TEST_CHECK(ts_probe_calls == 2);
    MOQ_TEST_PASS("receiver_ts_sized_caller_is_auto");
}

static unsigned alloc_calls, connect_calls;
static void *refuse_alloc(size_t n, void *ctx)
{
    (void)n; (void)ctx;
    alloc_calls++;
    return NULL;
}
static moq_result_t refuse_connect(const moq_endpoint_cfg_t *cfg,
                                   moq_endpoint_t **out, void *ctx)
{
    (void)cfg; (void)out; (void)ctx;
    connect_calls++;
    return MOQ_ERR_INTERNAL;
}

static void test_config(void)
{
    moq_media_receiver_cfg_t cfg;
    const size_t field = offsetof(moq_media_receiver_cfg_t, companion_profile);
    for (size_t n = sizeof(frozen_receiver_cfg_t); n < field + sizeof(cfg.companion_profile); n++) {
        init_cfg(&cfg, UINT32_MAX);
        cfg.struct_size = (uint32_t)n;
        moq_media_receiver_t *r = NULL;
        moq_result_t rc = moq_media_receiver_test_construct(&cfg, &r);
        MOQ_TEST_CHECK(rc == MOQ_OK && r != NULL);
        if (r) {
            MOQ_TEST_CHECK(r->companion_profile == MOQ_MEDIA_COMPANION_AUTO);
            moq_media_receiver_test_free(r);
        }
    }
    /* Every known value is accepted as given; AUTO with SHARED_EPOCH is
     * accepted at creation (it may resolve to legacy). */
    const moq_media_companion_profile_t known[] = {
        MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
        MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, MOQ_MEDIA_COMPANION_AUTO
    };
    for (size_t k = 0; k < sizeof(known) / sizeof(*known); k++) {
        for (unsigned epoch = 0; epoch < 2; epoch++) {
            init_cfg(&cfg, known[k]);
            if (epoch) cfg.time_mode = MOQ_MEDIA_TIME_SHARED_EPOCH;
            moq_media_receiver_t *r = NULL;
            moq_result_t rc = moq_media_receiver_test_construct(&cfg, &r);
            bool refused = epoch &&
                known[k] == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
            MOQ_TEST_CHECK(rc == (refused ? MOQ_ERR_UNSUPPORTED : MOQ_OK));
            if (r) {
                MOQ_TEST_CHECK(r->companion_profile == known[k]);
                moq_media_receiver_test_free(r);
            }
        }
    }
    /* Unknown, undersized and unanchored D22 SHARED_EPOCH refuse before
     * either allocation or an owning endpoint connect is attempted. */
    moq_alloc_t refusal = *moq_alloc_default();
    refusal.alloc = refuse_alloc;
    moq_media_receiver_test_set_alloc(&refusal);
    moq_media_receiver_test_set_connect(refuse_connect, NULL);
    moq_endpoint_cfg_t endpoint;
    memset(&endpoint, 0, sizeof(endpoint));
    for (unsigned i = 0; i < 4; i++) {
        init_cfg(&cfg, i == 0 ? UINT32_MAX : i == 3 ? MOQ_MEDIA_COMPANION_AUTO + 1u
                                : MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC);
        cfg.endpoint = &endpoint;
        if (i == 1) cfg.struct_size = sizeof(frozen_receiver_cfg_t) - 1;
        if (i == 2) cfg.time_mode = MOQ_MEDIA_TIME_SHARED_EPOCH;
        moq_media_receiver_t *r = NULL;
        moq_result_t rc = moq_media_receiver_create(&cfg, &r);
        MOQ_TEST_CHECK(rc == (i == 2 ? MOQ_ERR_UNSUPPORTED : MOQ_ERR_INVAL));
        MOQ_TEST_CHECK(r == NULL && alloc_calls == 0 && connect_calls == 0);
    }
    moq_media_receiver_test_set_alloc(NULL);
    moq_media_receiver_test_set_connect(NULL, NULL);
    MOQ_TEST_PASS("receiver_config_whole_field_and_preflight");
}

static moq_simpair_t *pair(const moq_alloc_t *alloc, moq_version_t version)
{
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = alloc;
    cfg.version = version;
    cfg.initial_now_us = 1000;
    cfg.client_send_request_capacity = cfg.server_send_request_capacity = true;
    cfg.client_initial_request_capacity = cfg.server_initial_request_capacity = 32;
    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&cfg, &sp) != MOQ_OK) return NULL;
    if (moq_simpair_start(sp) != MOQ_OK ||
        moq_simpair_run_until_quiescent(sp, 32, NULL) != MOQ_OK) goto fail;
    moq_session_t *ends[] = { moq_simpair_client(sp), moq_simpair_server(sp) };
    for (size_t i = 0; i < 2; i++) {
        if (moq_session_state(ends[i]) != MOQ_SESS_ESTABLISHED ||
            moq_session_version(ends[i]) != version) goto fail;
        moq_event_t ev;
        for (unsigned j = 0; j < 32 && moq_session_poll_events(ends[i], &ev, 1) == 1; j++)
            moq_event_cleanup(&ev);
    }
    return sp;
fail:
    moq_simpair_destroy(sp);
    return NULL;
}

/* Explicit companions accept only their own drafts; AUTO resolves to the one
 * matching the session (none for D22 with SHARED_EPOCH). A mismatch is a
 * receiver-local fatal with nothing sent. Rows: legacy, D22, AUTO, then AUTO
 * with SHARED_EPOCH. */
static void test_profiles(void)
{
    const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
    for (size_t i = 0; i < 3; i++) {
        for (unsigned p = 0; p < 4; p++) {
            test_alloc_state_t state = {0};
            moq_alloc_t alloc = test_allocator(&state);
            moq_simpair_t *sp = pair(&alloc, versions[i]);
            MOQ_TEST_CHECK(sp != NULL);
            if (!sp) { MOQ_TEST_CHECK(state.balance == 0); continue; }
            moq_media_receiver_cfg_t cfg;
            bool d22 = versions[i] == MOQ_VERSION_DRAFT_22;
            init_cfg(&cfg, p < 2 ? p : MOQ_MEDIA_COMPANION_AUTO);
            if (p == 3) cfg.time_mode = MOQ_MEDIA_TIME_SHARED_EPOCH;
            moq_media_receiver_test_set_alloc(&alloc);
            moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&cfg);
            moq_media_receiver_test_set_alloc(NULL);
            MOQ_TEST_CHECK(r != NULL);
            if (r) {
                moq_session_t *c = moq_simpair_client(sp);
                MOQ_TEST_CHECK(r->companion_profile == cfg.companion_profile);
                receiver_hook(NULL, c, moq_simpair_now_us(sp), r);
                bool usable = p == 2 ? true : p == 3 ? !d22 :
                    (p == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC) == d22;
                moq_media_companion_profile_t resolved =
                    p < 2 ? p : !usable ? MOQ_MEDIA_COMPANION_AUTO :
                    d22 ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
                        : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
                MOQ_TEST_CHECK(r->companion_profile == resolved);
                MOQ_TEST_CHECK(r->transport_version == moq_session_version(c));
                MOQ_TEST_CHECK(moq_media_receiver_is_fatal(r) == !usable);
                if (!usable) {
                    MOQ_TEST_CHECK(moq_media_receiver_fatal_code(r) == MOQ_MEDIA_RECEIVER_FATAL_SETUP_FAILED);
                    moq_action_t a;
                    size_t n = moq_session_poll_actions(c, &a, 1);
                    MOQ_TEST_CHECK(n == 0);
                    if (n) moq_action_cleanup(&a);
                }
                MOQ_TEST_CHECK(moq_session_state(c) == MOQ_SESS_ESTABLISHED);
                MOQ_TEST_CHECK(moq_session_state(moq_simpair_server(sp)) == MOQ_SESS_ESTABLISHED);
                moq_media_receiver_test_destroy_with_session(r, c, moq_simpair_now_us(sp));
            }
            moq_simpair_destroy(sp);
            MOQ_TEST_CHECK(state.balance == 0);
        }
    }
    MOQ_TEST_PASS("receiver_actual_profiles_auto_resolution_local_mismatch");
}

static void test_objects(moq_version_t version, bool auto_companion)
{
    static const char catalog[] = "{\"version\":1,\"generatedAt\":1,\"tracks\":["
        "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\","
        "\"codec\":\"av01\",\"timescale\":90000}]}";
    static const uint8_t video[] = { 0xa1, 0xa2 }, audio[] = { 0xb1, 0xb2, 0xb3 };
    static const uint8_t payload[] = { 1, 2, 3, 4 };
    /* Independent LOC-04 property block: types 9,12,13,15,16 as deltas;
     * marking 0x20, audio level 0x85, video/audio config, timestamp 2000000.
     * The D22 receiver must not rely solely on our encoder's own output. */
    static const uint8_t loc04_unix[] = {
        0x09, 0x01, 0x20, 0x03, 0x80, 0x85, 0x01, 0x02, 0xa1, 0xa2,
        0x02, 0x03, 0xb1, 0xb2, 0xb3, 0x01, 0xde, 0x84, 0x80
    };
    test_alloc_state_t state = {0};
    moq_alloc_t alloc = test_allocator(&state);
    moq_simpair_t *sp = pair(&alloc, version);
    moq_media_receiver_t *r = NULL;
    moq_media_object_t held[4] = {{0}};
    size_t held_count = 0;
    MOQ_TEST_CHECK(sp != NULL);
    if (!sp) goto cleanup;
    moq_media_receiver_cfg_t cfg;
    bool d22 = version == MOQ_VERSION_DRAFT_22;
    init_cfg(&cfg, auto_companion ? MOQ_MEDIA_COMPANION_AUTO :
                   d22 ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC :
                         MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01);
    moq_media_receiver_test_set_alloc(&alloc);
    r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) goto cleanup;
    receiver_hook(NULL, moq_simpair_client(sp), moq_simpair_now_us(sp), r);
    MOQ_TEST_CHECK(!moq_media_receiver_is_fatal(r));
    if (moq_media_receiver_is_fatal(r)) goto cleanup;
    MOQ_TEST_CHECK(r->companion_profile == (d22 ? MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
                                                : MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01));
    moq_media_receiver_test_ingest(r, 0, 0, catalog, sizeof(catalog) - 1);
    moq_media_track_t *track = NULL;
    moq_media_track_event_t ev;
    for (unsigned i = 0; i < 16 && moq_media_receiver_poll_track(r, &ev, sizeof(ev)) == MOQ_OK; i++)
        if (ev.kind == MOQ_MEDIA_TRACK_ADDED) track = ev.track;
    MOQ_TEST_CHECK(track != NULL);
    if (!track) goto cleanup;
    MOQ_TEST_CHECK(track->desc.info.transport_version == version);
    for (unsigned i = 0; i < (d22 ? 4u : 1u); i++) {
        moq_sub_object_t so = {0};
        so.group_id = 7; so.subgroup_id = 3; so.object_id = i;
        so.publisher_priority = 27;
        so.status = MOQ_OBJECT_NORMAL;
        moq_loc_metadata_t loc;
        moq_loc_metadata_init_sized(&loc, sizeof(loc));
        loc.present = MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING |
                      MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
        loc.timestamp = d22 ? (i == 0 ? 2000000 : i == 3 ? UINT64_MAX : 96000) : 12345;
        loc.marking_len = d22 ? (uint8_t)(i + 1) : 1;
        loc.marking[0] = 0x20; loc.marking[1] = 0x11;
        loc.marking[2] = 0x22; loc.marking[3] = 0x33;
        loc.video_config = (moq_bytes_t){ video, sizeof(video) };
        if (d22) {
            loc.present |= MOQ_LOC_METADATA_HAS_AUDIO_CONFIG | MOQ_LOC_METADATA_HAS_AUDIO_LEVEL;
            loc.audio_level = 0x85;
            loc.audio_config = (moq_bytes_t){ audio, sizeof(audio) };
            if (i != 0) {
                loc.present |= MOQ_LOC_METADATA_HAS_TIMESCALE;
                loc.timescale = i == 2 ? 0 : i == 3 ? 1 : 48000;
            }
            if (i == 2) {
                loc.video_config = (moq_bytes_t){ NULL, 0 };
                loc.audio_config = (moq_bytes_t){ NULL, 0 };
            }
        }
        moq_result_t rc;
        if (d22 && i == 0) {
            rc = moq_rcbuf_create(&alloc, loc04_unix, sizeof(loc04_unix), &so.properties);
        } else {
            rc = moq_loc_encode_metadata(&alloc, version,
                d22 ? MOQ_LOC_PROFILE_04 : MOQ_LOC_PROFILE_01, &loc, &so.properties);
        }
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK) goto cleanup;
        rc = moq_rcbuf_create(&alloc, payload, sizeof(payload), &so.payload);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK) { moq_sub_object_cleanup(&so); goto cleanup; }
        receiver_on_media_object(r, track, &so, moq_simpair_now_us(sp));
        if (d22 && i == 0) {
            const size_t sizes[] = { 0, MEDIA_OBJECT_V0_SIZE, sizeof(frozen_media_object_t),
                offsetof(moq_media_object_t, metadata) + sizeof(moq_media_metadata_t) - 1,
                MEDIA_OBJECT_METADATA_SIZE - 1 };
            for (size_t j = 0; j < sizeof(sizes) / sizeof(*sizes); j++) {
                moq_media_object_t out, before;
                memset(&out, 0xa5, sizeof(out)); before = out;
                rc = moq_media_receiver_poll_object(r, &out, sizes[j]);
                MOQ_TEST_CHECK(rc == MOQ_ERR_UNSUPPORTED);
                MOQ_TEST_CHECK(memcmp(&out, &before, sizeof(out)) == 0);
            }
        }
        moq_media_object_t *out = &held[held_count];
        size_t out_size = d22 ? sizeof(*out) : MEDIA_OBJECT_V0_SIZE;
        rc = moq_media_receiver_poll_object(r, out, out_size);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        if (rc != MOQ_OK) goto cleanup;
        held_count++;
        MOQ_TEST_CHECK(out->struct_size == out_size && out->track == track);
        MOQ_TEST_CHECK(out->payload.len == sizeof(payload));
        if (!d22) {
            MOQ_TEST_CHECK(out->has_capture_time && out->capture_time_us == 12345);
            continue;
        }
        const moq_media_metadata_t *m = &out->metadata;
        MOQ_TEST_CHECK(out->object_id == i && out->group_id == 7 && out->subgroup_id == 3);
        MOQ_TEST_CHECK(out->publisher_priority == 27 && out->has_subgroup);
        MOQ_TEST_CHECK(out->track_properties_ref == NULL);
        MOQ_TEST_CHECK(m->struct_size == sizeof(*m) && m->loc.struct_size == sizeof(m->loc));
        MOQ_TEST_CHECK(m->loc.present == loc.present && m->loc.timestamp == loc.timestamp);
        MOQ_TEST_CHECK(m->loc.timescale == loc.timescale && m->loc.audio_level == 0x85);
        MOQ_TEST_CHECK(m->loc.marking_len == i + 1 && memcmp(m->loc.marking, loc.marking, i + 1) == 0);
        for (size_t j = i + 1; j < sizeof(m->loc.marking); j++)
            MOQ_TEST_CHECK(m->loc.marking[j] == 0);
        for (size_t j = 0; j < sizeof(m->loc.reserved); j++)
            MOQ_TEST_CHECK(m->loc.reserved[j] == 0);
        for (size_t j = 0; j < sizeof(m->reserved); j++)
            MOQ_TEST_CHECK(m->reserved[j] == 0);
        MOQ_TEST_CHECK(m->independence_known == (i != 3) && m->independent == (i != 3));
        MOQ_TEST_CHECK(out->keyframe == (i != 3));
        MOQ_TEST_CHECK(m->time_domain == (i == 0 ? MOQ_MEDIA_TIME_DOMAIN_UNIX_US : MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA));
        MOQ_TEST_CHECK(m->has_time_us == (i < 2));
        MOQ_TEST_CHECK(m->time_us == (i < 2 ? 2000000u : 0));
        MOQ_TEST_CHECK(out->has_capture_time == (i == 0));
        MOQ_TEST_CHECK(out->capture_time_us == (i == 0 ? 2000000u : 0));
        MOQ_TEST_CHECK(out->decode_time_us == (i < 2 ? 2000000u : 0));
        MOQ_TEST_CHECK(out->presentation_time_us == out->decode_time_us);
        MOQ_TEST_CHECK(m->loc.video_config.len == (i == 2 ? 0 : sizeof(video)));
        MOQ_TEST_CHECK(m->loc.audio_config.len == (i == 2 ? 0 : sizeof(audio)));
    }
    /* The returned object's refs, not the receiver/session/track, own spans. */
    moq_media_receiver_test_destroy_with_session(r, moq_simpair_client(sp), moq_simpair_now_us(sp));
    r = NULL;
    moq_simpair_destroy(sp); sp = NULL;
    for (size_t i = 0; i < held_count; i++) {
        MOQ_TEST_CHECK(bytes_equal(held[i].payload, payload, sizeof(payload)));
        if (d22 && i != 2) {
            MOQ_TEST_CHECK(bytes_equal(held[i].metadata.loc.video_config, video, sizeof(video)));
            MOQ_TEST_CHECK(bytes_equal(held[i].metadata.loc.audio_config, audio, sizeof(audio)));
        }
    }
cleanup:
    if (r) moq_media_receiver_test_destroy_with_session(r, moq_simpair_client(sp), moq_simpair_now_us(sp));
    if (sp) moq_simpair_destroy(sp);
    for (size_t i = 0; i < held_count; i++) moq_media_object_cleanup(&held[i]);
    MOQ_TEST_CHECK(state.balance == 0);
    MOQ_TEST_PASS(version == MOQ_VERSION_DRAFT_22
        ? (auto_companion ? "receiver_auto_d22_metadata_and_size_floor" : "receiver_d22_metadata_and_backing")
        : (auto_companion ? "receiver_auto_legacy_v0_poll" : "receiver_legacy_v0_poll"));
}

static void test_cleanup(void)
{
    test_alloc_state_t state = {0};
    moq_alloc_t alloc = test_allocator(&state);
    moq_rcbuf_t *backing = NULL;
    static const uint8_t byte = 1;
    moq_result_t rc = moq_rcbuf_create(&alloc, &byte, 1, &backing);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;
    moq_media_object_t obj = {0};
    obj.struct_size = MEDIA_OBJECT_METADATA_SIZE - 1;
    obj.track_properties_ref = backing;
    int64_t before = state.balance;
    moq_media_object_cleanup(&obj);
    MOQ_TEST_CHECK(state.balance == before);
    moq_rcbuf_decref(backing);
    MOQ_TEST_CHECK(state.balance == 0);
    memset(&obj, 0, sizeof(obj));
    rc = moq_rcbuf_create(&alloc, &byte, 1, &obj.track_properties_ref);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return;
    obj.struct_size = MEDIA_OBJECT_METADATA_SIZE;
    moq_media_object_cleanup(&obj);
    moq_media_object_cleanup(&obj);
    MOQ_TEST_CHECK(state.balance == 0);
    frozen_media_object_t *old = calloc(1, sizeof(*old));
    MOQ_TEST_CHECK(old != NULL);
    if (old) {
        old->struct_size = sizeof(*old);
        moq_media_object_cleanup((moq_media_object_t *)old);
        free(old);
    }
    MOQ_TEST_PASS("receiver_owned_tail_cleanup_guards");
}

int main(void)
{
    test_old_initializers();
    test_ts_sized_caller();
    test_config();
    test_profiles();
    test_objects(MOQ_VERSION_DRAFT_22, false);
    test_objects(MOQ_VERSION_DRAFT_18, false);
    test_objects(MOQ_VERSION_DRAFT_22, true);
    test_objects(MOQ_VERSION_DRAFT_18, true);
    test_cleanup();
    printf("receiver_companion: %d failures\n", failures);
    return failures ? 1 : 0;
}
#endif
