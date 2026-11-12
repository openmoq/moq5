#include <moq/media_object.h>
#include <moq/loc.h>
#include "../../../tests/unit/test_support.h"
#include "media_object_layout_probe.h"
#include <stdlib.h>
#include <string.h>

static moq_result_t parse_companion(const moq_media_track_info_t *track,
    moq_media_companion_profile_t companion, const moq_media_object_input_t *in,
    moq_cmaf_sample_t *samples, size_t sample_cap, moq_media_parsed_object_t *out,
    size_t out_size, moq_media_drop_reason_t *reason)
{
    moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    cfg.companion_profile = companion;
    return moq_media_object_parse_sized(track, in, &cfg, samples, sample_cap,
                                      out, out_size, reason);
}

/* Literal LOC04 properties: scale 1,000,000, marking I=1, timestamp 7.
 * No codec-generated oracle and no RTP ID/length envelope. */
static int explicit_application_clock(void)
{
    int failures = 0;
    const uint8_t literal[] = {8, 0xcf, 0x42, 0x40, 1, 1, 0xe0, 7, 7};
    const uint8_t byte = 0x41;
    moq_rcbuf_t *payload = NULL, *properties = NULL;
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), &byte, 1, &payload), MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT(moq_rcbuf_create(moq_alloc_default(), literal, sizeof(literal), &properties), MOQ_OK);
    MOQ_TEST_CHECK(payload && properties);
    if (!payload || !properties) {
        moq_rcbuf_decref(payload); moq_rcbuf_decref(properties);
        return failures;
    }
    moq_media_track_info_t track;
    moq_media_track_info_init_sized(&track, sizeof(track));
    track.media_type = MOQ_MEDIA_TYPE_VIDEO;
    track.packaging = MOQ_MEDIA_PACKAGING_RAW;
    track.transport_version = MOQ_VERSION_DRAFT_22;
    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    in.payload = payload;
    in.properties = properties;
    moq_media_parsed_object_t out;
    moq_media_drop_reason_t reason = 0;
    moq_result_t rc = parse_companion(&track,
        MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, &in, NULL, 0,
        &out, sizeof(out), &reason);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK(reason == 0 && !out.has_capture_time);
        MOQ_TEST_CHECK(out.metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA);
        MOQ_TEST_CHECK(out.metadata.has_time_us && out.metadata.time_us == 7);
        MOQ_TEST_CHECK(out.metadata.loc.timescale == 1000000 && out.metadata.loc.timestamp == 7);
        MOQ_TEST_CHECK(out.metadata.independence_known && out.metadata.independent && out.keyframe);
        MOQ_TEST_CHECK(out.decode_time_us == 7 && out.presentation_time_us == 7);
        MOQ_TEST_CHECK(out.payload.data == moq_rcbuf_data(payload));
    }
    MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1 && moq_rcbuf_refcount(properties) == 1);
    moq_rcbuf_decref(payload);
    moq_rcbuf_decref(properties);
    return failures;
}

static moq_media_track_info_t make_track(moq_version_t version,
                                         moq_media_packaging_t packaging)
{
    moq_media_track_info_t track;
    moq_media_track_info_init_sized(&track, sizeof(track));
    track.media_type = MOQ_MEDIA_TYPE_VIDEO;
    track.packaging = packaging;
    track.transport_version = version;
    track.timescale = 90000;
    return track;
}

static moq_rcbuf_t *literal_buffer(const uint8_t *data, size_t len)
{
    moq_rcbuf_t *out = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), data, len, &out) != MOQ_OK) return NULL;
    return out;
}

static moq_media_object_input_t make_input(moq_rcbuf_t *payload, moq_rcbuf_t *properties)
{
    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    in.payload = payload;
    in.properties = properties;
    return in;
}

static moq_result_t parse22(const moq_media_track_info_t *track,
    const moq_media_object_input_t *in, moq_media_parsed_object_t *out,
    moq_media_drop_reason_t *reason)
{
    return parse_companion(track, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC,
                                       in, NULL, 0, out, sizeof(*out), reason);
}

/* Independently assembled non-minimal vi64 values. No production encoder. */
static void literal_u64(uint8_t bytes[9], uint64_t value)
{
    bytes[0] = 0xff;
    for (size_t i = 0; i < 8; ++i) bytes[i + 1] = (uint8_t)(value >> ((7 - i) * 8));
}

static int exact_clocks(void)
{
    int failures = 0;
    static const struct {
        uint64_t timestamp, scale, expected;
        bool scale_present, converted;
    } cases[] = {
        {0, 0, 0, false, true},
        {UINT64_MAX, 0, UINT64_MAX, false, true},
        {2, 3, 666666, true, true},
        {7, 1000000, 7, true, true},
        {UINT64_MAX, 1000000, UINT64_MAX, true, true},
        {UINT64_MAX - 1, UINT64_MAX, 999999, true, true},
        {UINT64_C(1) << 63, UINT64_MAX, 500000, true, true},
        {UINT64_MAX, UINT64_MAX, 1000000, true, true},
        {UINT64_MAX, UINT64_MAX - 1, 1000000, true, true},
        {UINT64_C(18446744073709), 1, UINT64_C(18446744073709000000), true, true},
        {UINT64_C(18446744073710), 1, 0, true, false},
        {UINT64_MAX, 1, 0, true, false},
        {UINT64_MAX, 999999, 0, true, false},
        {5, 0, 0, true, false},
    };
    const uint8_t byte = 0x41;
    moq_rcbuf_t *payload = literal_buffer(&byte, 1);
    MOQ_TEST_CHECK(payload != NULL);
    if (!payload) return failures;
    moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        uint8_t literal[20];
        size_t n = cases[i].scale_present ? 20 : 10;
        literal[0] = cases[i].scale_present ? 8 : 0x10;
        literal_u64(literal + 1, cases[i].scale_present ? cases[i].scale : cases[i].timestamp);
        if (cases[i].scale_present) { literal[10] = 8; literal_u64(literal + 11, cases[i].timestamp); }
        moq_rcbuf_t *props = literal_buffer(literal, n);
        MOQ_TEST_CHECK(props != NULL);
        if (!props) continue;
        moq_media_object_input_t in = make_input(payload, props);
        moq_media_parsed_object_t out;
        moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
        moq_result_t rc = parse22(&track, &in, &out, &reason);
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        if (rc == MOQ_OK) {
            MOQ_TEST_CHECK(reason == 0);
            MOQ_TEST_CHECK(out.metadata.loc.timestamp == cases[i].timestamp);
            MOQ_TEST_CHECK(out.metadata.loc.timescale == cases[i].scale);
            MOQ_TEST_CHECK(out.metadata.time_domain == (cases[i].scale_present ?
                MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA : MOQ_MEDIA_TIME_DOMAIN_UNIX_US));
            MOQ_TEST_CHECK((out.metadata.has_time_us != 0) == cases[i].converted);
            MOQ_TEST_CHECK_EQ_U64(out.metadata.time_us, cases[i].expected);
            MOQ_TEST_CHECK_EQ_U64(out.decode_time_us, cases[i].expected);
            MOQ_TEST_CHECK_EQ_U64(out.presentation_time_us, cases[i].expected);
            MOQ_TEST_CHECK(out.has_capture_time == !cases[i].scale_present);
            MOQ_TEST_CHECK_EQ_U64(out.capture_time_us, cases[i].scale_present ? 0 : cases[i].expected);
        }
        MOQ_TEST_CHECK(moq_rcbuf_refcount(props) == 1 && moq_rcbuf_refcount(payload) == 1);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(props), literal, n) == 0);
        moq_rcbuf_decref(props);
    }
    moq_media_object_input_t in = make_input(payload, NULL);
    moq_media_parsed_object_t out;
    moq_result_t rc = parse22(&track, &in, &out, NULL);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc == MOQ_OK) {
        MOQ_TEST_CHECK(out.metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_NONE && !out.metadata.has_time_us);
        MOQ_TEST_CHECK(out.metadata.loc.present == 0 && !out.has_capture_time && out.decode_time_us == 0);
    }
    moq_rcbuf_decref(payload);
    return failures;
}

static int marking_and_policy(void)
{
    int failures = 0;
    const uint8_t byte = 0x41;
    moq_rcbuf_t *payload = literal_buffer(&byte, 1);
    MOQ_TEST_CHECK(payload != NULL);
    if (!payload) return failures;
    moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW);
    for (uint8_t n = 1; n <= 4; ++n) {
        for (size_t independent = 0; independent < 2; ++independent) {
            const uint8_t literal[] = {9, n, independent ? 0xe8 : 0xc8, 7, 0x11, 0x3e};
            moq_rcbuf_t *props = literal_buffer(literal, (size_t)n + 2);
            MOQ_TEST_CHECK(props != NULL);
            if (!props) continue;
            moq_media_object_input_t in = make_input(payload, props);
            moq_media_parsed_object_t out;
            moq_result_t rc = parse22(&track, &in, &out, NULL);
            MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
            if (rc == MOQ_OK) {
                MOQ_TEST_CHECK(out.metadata.loc.marking_len == n &&
                               memcmp(out.metadata.loc.marking, literal + 2, n) == 0);
                MOQ_TEST_CHECK((out.metadata.independence_known != 0) == (n < 4));
                MOQ_TEST_CHECK(out.keyframe == (n < 4 && independent != 0));
                MOQ_TEST_CHECK((out.metadata.independent != 0) == (n < 4 && independent != 0));
            }
            MOQ_TEST_CHECK(moq_rcbuf_refcount(props) == 1);
            moq_rcbuf_decref(props);
        }
    }
    const uint8_t audio[] = {0x0c, 0x80, 0xaa};
    moq_rcbuf_t *props = literal_buffer(audio, sizeof(audio));
    MOQ_TEST_CHECK(props != NULL);
    if (!props) { moq_rcbuf_decref(payload); return failures; }
    moq_media_object_input_t in = make_input(payload, props);
    moq_media_parsed_object_t out;
    track.media_type = MOQ_MEDIA_TYPE_AUDIO;
    moq_result_t rc = parse22(&track, &in, &out, NULL);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc == MOQ_OK) MOQ_TEST_CHECK(out.keyframe && out.metadata.loc.audio_level == 0xaa);
    moq_rcbuf_decref(props);
    static const struct { moq_result_t result; size_t len; uint8_t data[8]; } vectors[] = {
        {MOQ_ERR_UNSUPPORTED, 4, {0x10, 1, 0, 2}},
        {MOQ_ERR_UNSUPPORTED, 6, {0x0b, 2, 0x10, 1, 5, 2}},
        {MOQ_ERR_PROTO, 2, {9, 0}},
        {MOQ_ERR_PROTO, 2, {0x10, 0xff}},
        {MOQ_OK, 4, {0x20, 1, 0, 2}},
    };
    for (size_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); ++i) {
        props = literal_buffer(vectors[i].data, vectors[i].len);
        MOQ_TEST_CHECK(props != NULL);
        if (!props) continue;
        in = make_input(payload, props);
        moq_media_parsed_object_t saved;
        memset(&out, 0xa5, sizeof(out)); memcpy(&saved, &out, sizeof(saved));
        moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
        rc = parse22(&track, &in, &out, &reason);
        MOQ_TEST_CHECK_EQ_INT(rc, vectors[i].result);
        if (rc == vectors[i].result) {
            MOQ_TEST_CHECK(reason == (vectors[i].result == MOQ_ERR_PROTO ? MOQ_MEDIA_DROP_MALFORMED_LOC : 0));
            if (vectors[i].result != MOQ_OK) MOQ_TEST_CHECK(memcmp(&saved, &out, sizeof(out)) == 0);
            else MOQ_TEST_CHECK(out.metadata.loc.present == 0);
        }
        MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1 && moq_rcbuf_refcount(props) == 1);
        moq_rcbuf_decref(props);
    }
    moq_rcbuf_decref(payload);
    return failures;
}

/* One literal fragment: tfdt=90000, duration=4500, size=4, sync sample,
 * composition=-4500 at catalog scale90000 => DTS1e6 / PTS950000 / duration50000. */
static const uint8_t fragment[] = {
    0,0,0,84,'m','o','o','f', 0,0,0,76,'t','r','a','f',
    0,0,0,16,'t','f','h','d', 0,0,0,0, 0,0,0,1,
    0,0,0,20,'t','f','d','t', 1,0,0,0, 0,0,0,0, 0,1,0x5f,0x90,
    0,0,0,32,'t','r','u','n', 0,0,0x0f,0, 0,0,0,1,
    0,0,0x11,0x94, 0,0,0,4, 2,0,0,0, 0xff,0xff,0xee,0x6c,
    0,0,0,12,'m','d','a','t', 0x41,0x42,0x43,0x44
};

static int object_config_spans(void)
{
    int failures = 0;
    const uint8_t outer[] = {0x0d, 2, 0xaa, 0xbb, 2, 1, 0xcc, 1, 7};
    const uint8_t immutable[] = {0x0b, 6, 0x0d, 1, 0xaa, 2, 1, 0xbb, 5, 7};
    const uint8_t empty[] = {0x0d, 0, 2, 0, 1, 7};
    const uint8_t byte = 0x41;
    moq_rcbuf_t *payload = literal_buffer(&byte, 1);
    MOQ_TEST_CHECK(payload != NULL);
    if (!payload) return failures;
    moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW);
    for (size_t i = 0; i < 3; ++i) {
        const uint8_t *literal = i == 0 ? outer : i == 1 ? immutable : empty;
        size_t len = i == 0 ? sizeof(outer) : i == 1 ? sizeof(immutable) : sizeof(empty);
        moq_rcbuf_t *props = literal_buffer(literal, len);
        MOQ_TEST_CHECK(props != NULL);
        if (!props) continue;
        moq_media_object_input_t in = make_input(payload, props);
        moq_media_parsed_object_t out;
        moq_result_t rc = parse22(&track, &in, &out, NULL);
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        if (rc == MOQ_OK) {
            MOQ_TEST_CHECK(out.metadata.loc.present == (MOQ_LOC_METADATA_HAS_TIMESTAMP |
                MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_AUDIO_CONFIG));
            MOQ_TEST_CHECK(out.metadata.loc.video_config.len == (i == 0 ? 2u : i == 1 ? 1u : 0u));
            MOQ_TEST_CHECK(out.metadata.loc.audio_config.len == (i == 2 ? 0u : 1u));
            if (i != 2) {
                MOQ_TEST_CHECK(out.metadata.loc.video_config.data == moq_rcbuf_data(props) + (i == 0 ? 2 : 4));
                MOQ_TEST_CHECK(out.metadata.loc.audio_config.data == moq_rcbuf_data(props) + (i == 0 ? 6 : 7));
            }
            MOQ_TEST_CHECK(out.metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_UNIX_US &&
                out.metadata.time_us == 7 && out.capture_time_us == 7);
            MOQ_TEST_CHECK(out.payload.data == moq_rcbuf_data(payload));
        }
        MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1 && moq_rcbuf_refcount(props) == 1);
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(props), literal, len) == 0);
        moq_rcbuf_decref(props);
    }
    moq_rcbuf_decref(payload);
    return failures;
}

static int cmaf_and_terminal(void)
{
    int failures = 0;
    moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_CMAF);
    moq_rcbuf_t *payload = literal_buffer(fragment, sizeof(fragment));
    const uint8_t app[] = {8, 0xcf, 0x42, 0x40, 8, 7};
    const uint8_t unix_time[] = {0x10, 42};
    const uint8_t dependent[] = {9, 1, 0xc0};
    const uint8_t opaque[] = {9, 4, 0xe0, 7, 0x11, 0x3e};
    MOQ_TEST_CHECK(payload != NULL);
    if (!payload) return failures;
    for (size_t i = 0; i < 4; ++i) {
        const uint8_t *literal = i == 0 ? app : i == 1 ? unix_time : i == 2 ? dependent : opaque;
        size_t len = i == 0 ? sizeof(app) : i == 1 ? sizeof(unix_time) : i == 2 ? sizeof(dependent) : sizeof(opaque);
        moq_rcbuf_t *props = literal_buffer(literal, len);
        MOQ_TEST_CHECK(props != NULL);
        if (!props) continue;
        moq_media_object_input_t in = make_input(payload, props);
        moq_cmaf_sample_t samples[1];
        moq_media_parsed_object_t out;
        moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
        moq_result_t rc = parse_companion(&track,
            MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, &in, samples, 1,
            &out, sizeof(out), &reason);
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        if (rc == MOQ_OK) {
            MOQ_TEST_CHECK(reason == 0 && out.decode_time_us == 1000000 && out.presentation_time_us == 950000);
            MOQ_TEST_CHECK(out.composition_offset_us == -50000 && out.sample_duration_us == 50000);
            MOQ_TEST_CHECK(out.fragment.data == moq_rcbuf_data(payload) && out.fragment.len == sizeof(fragment));
            MOQ_TEST_CHECK(out.samples == samples && out.sample_count == 1 && out.mdat_offset == 92 && out.mdat_len == 4);
            MOQ_TEST_CHECK(out.keyframe == (i != 2));
            MOQ_TEST_CHECK(out.has_capture_time == (i == 1));
            if (i == 0) MOQ_TEST_CHECK(out.metadata.time_us == 7 &&
                out.metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA);
            if (i == 1) MOQ_TEST_CHECK(out.capture_time_us == 42 &&
                out.metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_UNIX_US);
            if (i == 3) MOQ_TEST_CHECK(!out.metadata.independence_known && out.metadata.loc.marking_len == 4);
        }
        MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1 && moq_rcbuf_refcount(props) == 1);
        moq_rcbuf_decref(props);
    }
    moq_media_object_input_t in = make_input(payload, NULL);
    moq_media_parsed_object_t out;
    moq_result_t rc = parse22(&track, &in, &out, NULL);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_BUFFER);
    if (rc == MOQ_ERR_BUFFER) MOQ_TEST_CHECK(out.sample_count == 1 && out.struct_size == sizeof(out));
    MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1);
    moq_rcbuf_decref(payload);
    const uint8_t bad[] = {0x41};
    payload = literal_buffer(bad, sizeof(bad));
    MOQ_TEST_CHECK(payload != NULL);
    if (!payload) return failures;
    in = make_input(payload, NULL);
    moq_media_parsed_object_t saved;
    memset(&out, 0xa5, sizeof(out)); memcpy(&saved, &out, sizeof(saved));
    moq_cmaf_sample_t samples[1];
    moq_media_drop_reason_t reason = 0;
    rc = parse_companion(&track,
        MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, &in, samples, 1,
        &out, sizeof(out), &reason);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_PROTO);
    if (rc == MOQ_ERR_PROTO)
        MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_CMAF && memcmp(&out, &saved, sizeof(out)) == 0);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1);
    moq_rcbuf_decref(payload);
    in = make_input(NULL, NULL);
    for (size_t i = 0; i < 2; ++i) {
        in.status = i ? MOQ_OBJECT_END_OF_TRACK : MOQ_OBJECT_END_OF_GROUP;
        rc = parse22(&track, &in, &out, NULL);
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        if (rc == MOQ_OK)
            MOQ_TEST_CHECK(out.status == in.status && out.sample_count == 0 &&
                out.metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_NONE && !out.has_capture_time);
    }
    return failures;
}

static bool bytes_are(const uint8_t *bytes, size_t len, uint8_t value)
{
    for (size_t i = 0; i < len; ++i) if (bytes[i] != value) return false;
    return true;
}

static int track_context(void)
{
    int failures = 0;
    const uint8_t track_video[] = {0xaa, 0xbb}, track_audio[] = {0xcc, 0xdd};
    const uint8_t timestamp[] = {0x10, 10};
    const uint8_t overrides[] = {8, 2, 5, 1, 0x33, 2, 0, 1, 10};
    const uint8_t zero_scale[] = {8, 0, 8, 10};
    const uint8_t byte = 0x41;
    for (unsigned cmaf = 0; cmaf < 2; ++cmaf) {
        moq_rcbuf_t *payload = literal_buffer(cmaf ? fragment : &byte,
                                               cmaf ? sizeof(fragment) : 1);
        MOQ_TEST_CHECK(payload != NULL);
        if (!payload) continue;
        moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_22,
            cmaf ? MOQ_MEDIA_PACKAGING_CMAF : MOQ_MEDIA_PACKAGING_RAW);
        for (unsigned row = 0; row < 4; ++row) {
            const uint8_t *literal = row == 0 ? NULL : row == 1 ? timestamp :
                row == 2 ? overrides : zero_scale;
            size_t len = row == 0 ? 0 : row == 1 ? sizeof(timestamp) :
                row == 2 ? sizeof(overrides) : sizeof(zero_scale);
            moq_rcbuf_t *props = len ? literal_buffer(literal, len) : NULL;
            MOQ_TEST_CHECK(!len || props != NULL);
            if (len && !props) continue;
            moq_loc_metadata_t context;
            moq_loc_metadata_init_sized(&context, sizeof(context));
            context.present = MOQ_LOC_METADATA_HAS_TIMESCALE |
                MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_AUDIO_CONFIG |
                MOQ_LOC_METADATA_HAS_TIMESTAMP | MOQ_LOC_METADATA_HAS_MARKING |
                MOQ_LOC_METADATA_HAS_AUDIO_LEVEL;
            context.timescale = 1000;
            context.video_config = (moq_bytes_t){track_video, sizeof(track_video)};
            context.audio_config = (moq_bytes_t){track_audio, sizeof(track_audio)};
            context.timestamp = 123;
            context.marking_len = 1; context.marking[0] = 0xe0;
            context.audio_level = 0xaa;
            moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
            cfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
            cfg.track_loc_metadata = &context;
            moq_media_object_input_t in = make_input(payload, props);
            moq_cmaf_sample_t samples[1];
            moq_media_parsed_object_t out;
            moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
            moq_result_t rc = moq_media_object_parse_sized(&track, &in, &cfg,
                samples, 1, &out, sizeof(out), &reason);
            MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
            if (rc == MOQ_OK) {
                const uint32_t flags = MOQ_LOC_METADATA_HAS_TIMESCALE |
                    MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_AUDIO_CONFIG |
                    (row ? MOQ_LOC_METADATA_HAS_TIMESTAMP : 0);
                MOQ_TEST_CHECK_EQ_U64(out.metadata.loc.present, flags);
                MOQ_TEST_CHECK_EQ_U64(out.metadata.loc.timescale, row == 2 ? 2 : row == 3 ? 0 : 1000);
                MOQ_TEST_CHECK(out.metadata.loc.timestamp == (row ? 10 : 0));
                MOQ_TEST_CHECK(!out.metadata.independence_known && out.metadata.loc.audio_level == 0);
                MOQ_TEST_CHECK(!out.has_capture_time && out.capture_time_us == 0 && reason == 0);
                MOQ_TEST_CHECK(out.metadata.time_domain == (row ? MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA : MOQ_MEDIA_TIME_DOMAIN_NONE));
                MOQ_TEST_CHECK((out.metadata.has_time_us != 0) == (row == 1 || row == 2));
                MOQ_TEST_CHECK_EQ_U64(out.metadata.time_us, row == 1 ? 10000 : row == 2 ? 5000000 : 0);
                MOQ_TEST_CHECK(out.metadata.loc.video_config.data ==
                    (row == 2 ? moq_rcbuf_data(props) + 4 : track_video));
                MOQ_TEST_CHECK_EQ_U64(out.metadata.loc.video_config.len, row == 2 ? 1 : sizeof(track_video));
                MOQ_TEST_CHECK_EQ_U64(out.metadata.loc.audio_config.len, row == 2 ? 0 : sizeof(track_audio));
                if (row != 2) MOQ_TEST_CHECK(out.metadata.loc.audio_config.data == track_audio);
                if (cmaf) {
                    MOQ_TEST_CHECK(out.decode_time_us == 1000000 && out.presentation_time_us == 950000);
                } else {
                    MOQ_TEST_CHECK(out.decode_time_us == out.metadata.time_us &&
                                   out.presentation_time_us == out.metadata.time_us && !out.keyframe);
                }
                context.timescale = 77;
                context.video_config = (moq_bytes_t){NULL, 0};
                MOQ_TEST_CHECK(out.metadata.loc.timescale != context.timescale &&
                               out.metadata.loc.video_config.data != NULL);
            }
            MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1 && (!props || moq_rcbuf_refcount(props) == 1));
            if (props) MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(props), literal, len) == 0);
            moq_rcbuf_decref(props);
        }
        moq_rcbuf_decref(payload);
    }
    return failures;
}

static int context_sizes_and_refusals(void)
{
    int failures = 0;
    const uint8_t byte = 0x41, timestamp[] = {0x10, 10};
    moq_rcbuf_t *payload = literal_buffer(&byte, 1);
    moq_rcbuf_t *props = literal_buffer(timestamp, sizeof(timestamp));
    MOQ_TEST_CHECK(payload && props);
    if (!payload || !props) {
        moq_rcbuf_decref(payload); moq_rcbuf_decref(props);
        return failures;
    }
    moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW);
    moq_media_object_input_t in = make_input(payload, props);
    moq_loc_metadata_t context;
    moq_loc_metadata_init_sized(&context, sizeof(context));
    context.present = MOQ_LOC_METADATA_HAS_TIMESCALE;
    context.timescale = 1000;
    const size_t pointer_end = offsetof(moq_media_object_parse_cfg_t, track_loc_metadata) + sizeof(&context);
    /* A covered track_loc_ext is the caller's own field: this caller names
     * none. A size that covers only part of it leaves it unread. */
    const size_t ext_end = offsetof(moq_media_object_parse_cfg_t, track_loc_ext) +
                           sizeof(const moq_loc_ext_t *);
    for (size_t size = 0; size <= sizeof(moq_media_object_parse_cfg_t) + 8; ++size) {
        size_t allocated = size < sizeof(uint32_t) ? sizeof(uint32_t) : size;
        uint8_t *bytes = malloc(allocated);
        MOQ_TEST_CHECK(bytes != NULL);
        if (!bytes) continue;
        memset(bytes, 0xa5, allocated);
        uint32_t stamp = (uint32_t)size;
        memcpy(bytes, &stamp, sizeof(stamp));
        if (size >= MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE) {
            uint32_t companion = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
            memcpy(bytes + offsetof(moq_media_object_parse_cfg_t, companion_profile), &companion, sizeof(companion));
        }
        if (size >= pointer_end) {
            const moq_loc_metadata_t *ptr = &context;
            memcpy(bytes + offsetof(moq_media_object_parse_cfg_t, track_loc_metadata), &ptr, sizeof(ptr));
        }
        if (size >= ext_end) {
            const moq_loc_ext_t *none = NULL;
            memcpy(bytes + offsetof(moq_media_object_parse_cfg_t, track_loc_ext), &none, sizeof(none));
        }
        moq_media_parsed_object_t out;
        memset(&out, 0xa5, sizeof(out));
        moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
        moq_result_t rc = moq_media_object_parse_sized(&track, &in,
            (const moq_media_object_parse_cfg_t *)bytes, NULL, 0, &out, sizeof(out), &reason);
        MOQ_TEST_CHECK_EQ_INT(rc, size < MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE ? MOQ_ERR_INVAL : MOQ_OK);
        if (size < MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE) {
            MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_STALE && bytes_are((const uint8_t *)&out, sizeof(out), 0xa5));
        } else if (rc == MOQ_OK) {
            MOQ_TEST_CHECK_EQ_U64(out.metadata.time_us, size < pointer_end ? 10 : 10000);
            MOQ_TEST_CHECK(out.has_capture_time == (size < pointer_end));
        }
        free(bytes);
    }
    for (unsigned row = 0; row < 7; ++row) {
        moq_loc_metadata_t bad = context;
        moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
        cfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
        cfg.track_loc_metadata = &bad;
        if (row == 0) bad.struct_size = MOQ_LOC_METADATA_V0_SIZE - 1;
        if (row == 1) bad.present |= UINT32_C(0x80000000);
        if (row == 2) bad.reserved[0] = 1;
        if (row == 3) { bad.present |= MOQ_LOC_METADATA_HAS_VIDEO_CONFIG; bad.video_config = (moq_bytes_t){NULL, 1}; }
        if (row == 4) { bad.present |= MOQ_LOC_METADATA_HAS_AUDIO_CONFIG; bad.audio_config = (moq_bytes_t){NULL, 1}; }
        if (row == 5) cfg.companion_profile = UINT32_MAX;
        moq_media_parsed_object_t out;
        memset(&out, 0xa5, sizeof(out));
        moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
        moq_result_t rc = moq_media_object_parse_sized(&track, &in, row == 6 ? NULL : &cfg,
            NULL, 0, &out, sizeof(out), &reason);
        MOQ_TEST_CHECK_EQ_INT(rc, row == 6 ? MOQ_ERR_UNSUPPORTED : MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_STALE && bytes_are((const uint8_t *)&out, sizeof(out), 0xa5));
    }
    uint32_t *short_record = malloc(sizeof(*short_record));
    MOQ_TEST_CHECK(short_record != NULL);
    if (short_record) {
        *short_record = sizeof(*short_record);
        moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
        cfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
        cfg.track_loc_metadata = (const moq_loc_metadata_t *)short_record;
        moq_media_parsed_object_t out;
        memset(&out, 0xa5, sizeof(out));
        moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
        MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&track, &in, &cfg,
            NULL, 0, &out, sizeof(out), &reason), MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_STALE && bytes_are((const uint8_t *)&out, sizeof(out), 0xa5));
        free(short_record);
    }
    MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1 && moq_rcbuf_refcount(props) == 1);
    moq_rcbuf_decref(payload); moq_rcbuf_decref(props);
    return failures;
}

/* The sized parser's legacy pairs (LEGACY_MSF01_LOC01 with draft 16/18) run
 * the metadata parser, so their results differ from the frozen parser's in
 * exactly these cases, all intended:
 *   1. a duplicate known LOC property is UNSUPPORTED, with no drop reason
 *      (the frozen parser keeps the last value);
 *   2. known properties inside an Immutable Properties list are read, and a
 *      nested list is PROTO / MALFORMED_LOC (the frozen parser skips it);
 *   3. a 3- or 4-byte video frame marking is kept raw with independence
 *      unknown (the frozen parser refuses it as PROTO / MALFORMED_LOC);
 *   4. a wholly covered track LOC record is read: malformed is INVAL, track
 *      timescale or audio configuration is UNSUPPORTED, otherwise its video
 *      configuration is inherited into metadata only;
 *   5. a draft-22 track with the legacy companion is a known mismatched pair,
 *      UNSUPPORTED;
 *   6. output that covers the metadata tail receives it.
 * The frozen moq_media_object_parse is unchanged; rows 4 and 6 are pinned
 * below, rows 1 and 5 in old_allocations and explicit_pairs_and_inputs. */
static int legacy_context_defaults(void)
{
    int failures = 0;
    const uint8_t byte = 0x41, timestamp[] = {2, 10}, config_byte = 0xaa;
    moq_rcbuf_t *payload = literal_buffer(&byte, 1);
    moq_rcbuf_t *props = literal_buffer(timestamp, sizeof(timestamp));
    MOQ_TEST_CHECK(payload && props);
    if (!payload || !props) {
        moq_rcbuf_decref(payload); moq_rcbuf_decref(props);
        return failures;
    }
    moq_media_object_input_t in = make_input(payload, props);
    for (unsigned v = 0; v < 2; ++v) {
        moq_media_track_info_t track = make_track(v ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16,
                                                MOQ_MEDIA_PACKAGING_RAW);
        for (unsigned row = 0; row < 5; ++row) {
            moq_loc_metadata_t context;
            moq_loc_metadata_init_sized(&context, sizeof(context));
            context.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
            context.video_config = (moq_bytes_t){&config_byte, 1};
            context.timescale = 7; /* Inactive: must not change the clock. */
            if (row == 3) context.present |= MOQ_LOC_METADATA_HAS_TIMESCALE;
            if (row == 4) context.present |= MOQ_LOC_METADATA_HAS_AUDIO_CONFIG;
            moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
            if (row >= 2) cfg.track_loc_metadata = &context;
            moq_media_parsed_object_t out;
            memset(&out, 0xa5, sizeof(out));
            moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
            moq_result_t rc = moq_media_object_parse_sized(&track, &in, row == 0 ? NULL : &cfg,
                NULL, 0, &out, sizeof(out), &reason);
            MOQ_TEST_CHECK_EQ_INT(rc, row < 3 ? MOQ_OK : MOQ_ERR_UNSUPPORTED);
            if (row < 3 && rc == MOQ_OK) {
                MOQ_TEST_CHECK(reason == 0 && out.has_capture_time && out.capture_time_us == 10);
                MOQ_TEST_CHECK(out.metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_UNIX_US &&
                               out.metadata.time_us == 10);
                MOQ_TEST_CHECK_EQ_U64(out.metadata.loc.present, MOQ_LOC_METADATA_HAS_TIMESTAMP |
                    (row == 2 ? MOQ_LOC_METADATA_HAS_VIDEO_CONFIG : 0));
                if (row == 2) MOQ_TEST_CHECK(out.metadata.loc.video_config.data == &config_byte &&
                                           out.metadata.loc.video_config.len == 1);
            } else if (row >= 3) {
                MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_STALE && bytes_are((const uint8_t *)&out, sizeof(out), 0xa5));
            }
        }
    }
    MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1 && moq_rcbuf_refcount(props) == 1);
    moq_rcbuf_decref(payload); moq_rcbuf_decref(props);
    return failures;
}

static int output_sizes(void)
{
    int failures = 0;
    const uint8_t byte = 0x41, literal[] = {0x10, 7};
    moq_rcbuf_t *payload = literal_buffer(&byte, 1);
    moq_rcbuf_t *properties = literal_buffer(literal, sizeof(literal));
    MOQ_TEST_CHECK(payload && properties);
    if (!payload || !properties) {
        moq_rcbuf_decref(payload); moq_rcbuf_decref(properties);
        return failures;
    }
    moq_media_object_input_t in = make_input(payload, properties);
    moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW);
    for (size_t size = 0; size <= sizeof(moq_media_parsed_object_t) + 16; ++size) {
        size_t allocated = size ? size : 1;
        uint8_t *bytes = malloc(allocated);
        MOQ_TEST_CHECK(bytes != NULL);
        if (!bytes) continue;
        memset(bytes, 0xa5, allocated);
        moq_media_parsed_object_init_sized((moq_media_parsed_object_t *)bytes, size);
        uint8_t expected[sizeof(moq_media_parsed_object_t) + 16];
        memset(expected, 0xa5, sizeof(expected));
        size_t n = size < sizeof(moq_media_parsed_object_t) ? size : sizeof(moq_media_parsed_object_t);
        if (size >= sizeof(uint32_t)) {
            memset(expected, 0, n);
            uint32_t stamp = (uint32_t)n;
            memcpy(expected, &stamp, sizeof(stamp));
            if (n >= offsetof(moq_media_parsed_object_t, sap_type) + sizeof(moq_sap_type_t)) {
                moq_sap_type_t sap = MOQ_SAP_UNKNOWN;
                memcpy(expected + offsetof(moq_media_parsed_object_t, sap_type), &sap, sizeof(sap));
            }
            if (n >= MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE) {
                moq_media_metadata_t meta;
                memset(&meta, 0, sizeof(meta));
                meta.struct_size = sizeof(meta);
                meta.loc.struct_size = sizeof(meta.loc);
                memcpy(expected + offsetof(moq_media_parsed_object_t, metadata), &meta, sizeof(meta));
            }
        }
        MOQ_TEST_CHECK(memcmp(bytes, expected, allocated) == 0);

        memset(bytes, 0xa5, allocated);
        moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
        moq_result_t rc = parse_companion(&track,
            MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, &in, NULL, 0,
            (moq_media_parsed_object_t *)bytes, size, &reason);
        if (size < MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE) {
            MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_UNSUPPORTED);
            MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_STALE && bytes_are(bytes, allocated, 0xa5));
        } else {
            MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
            if (rc == MOQ_OK) {
                moq_media_parsed_object_t *out = (moq_media_parsed_object_t *)bytes;
                MOQ_TEST_CHECK(reason == 0 && out->struct_size == n && out->capture_time_us == 7);
                MOQ_TEST_CHECK(out->metadata.struct_size == sizeof(out->metadata) &&
                    out->metadata.loc.struct_size == sizeof(out->metadata.loc));
            }
            MOQ_TEST_CHECK(bytes_are(bytes + n, allocated - n, 0xa5));
        }
        free(bytes);
    }
    moq_rcbuf_decref(properties);
    const uint8_t legacy_literal[] = {2, 7};
    properties = literal_buffer(legacy_literal, sizeof(legacy_literal));
    MOQ_TEST_CHECK(properties != NULL);
    if (!properties) { moq_rcbuf_decref(payload); return failures; }
    in.properties = properties;
    for (size_t v = 0; v < 2; ++v) {
        track.transport_version = v ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        for (size_t size = 0; size <= sizeof(moq_media_parsed_object_t) + 16; ++size) {
            size_t allocated = size ? size : 1;
            uint8_t *bytes = malloc(allocated);
            MOQ_TEST_CHECK(bytes != NULL);
            if (!bytes) continue;
            memset(bytes, 0xa5, allocated);
            moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
            moq_result_t rc = parse_companion(&track,
                MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, &in, NULL, 0,
                (moq_media_parsed_object_t *)bytes, size, &reason);
            if (size < MOQ_MEDIA_PARSED_OBJECT_V0_SIZE) {
                MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_INVAL);
                MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_STALE && bytes_are(bytes, allocated, 0xa5));
            } else {
                size_t n = size < sizeof(moq_media_parsed_object_t) ? size : sizeof(moq_media_parsed_object_t);
                MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
                if (rc == MOQ_OK) {
                    moq_media_parsed_object_t *out = (moq_media_parsed_object_t *)bytes;
                    MOQ_TEST_CHECK(out->struct_size == n && reason == 0 && out->has_capture_time &&
                        out->capture_time_us == 7 && out->decode_time_us == 7 && out->presentation_time_us == 7);
                }
                MOQ_TEST_CHECK(bytes_are(bytes + n, allocated - n, 0xa5));
            }
            free(bytes);
        }
    }
    MOQ_TEST_CHECK(moq_rcbuf_refcount(payload) == 1 && moq_rcbuf_refcount(properties) == 1);
    moq_rcbuf_decref(payload); moq_rcbuf_decref(properties);
    moq_media_parsed_object_init_sized(NULL, sizeof(moq_media_parsed_object_t));
    return failures;
}

static int explicit_pairs_and_inputs(void)
{
    int failures = 0;
    static const struct {
        moq_version_t version;
        moq_media_companion_profile_t companion;
        moq_result_t result;
    } pairs[] = {
        {MOQ_VERSION_DRAFT_22, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, MOQ_ERR_UNSUPPORTED},
        {MOQ_VERSION_DRAFT_16, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, MOQ_ERR_UNSUPPORTED},
        {MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, MOQ_ERR_UNSUPPORTED},
        {(moq_version_t)0, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, MOQ_ERR_INVAL},
        {(moq_version_t)17, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, MOQ_ERR_INVAL},
        {MOQ_VERSION_DRAFT_22, UINT32_MAX, MOQ_ERR_INVAL},
        /* AUTO is a service config value; the parser takes a resolved one. */
        {MOQ_VERSION_DRAFT_22, MOQ_MEDIA_COMPANION_AUTO, MOQ_ERR_INVAL},
        {MOQ_VERSION_DRAFT_18, MOQ_MEDIA_COMPANION_AUTO, MOQ_ERR_INVAL},
    };
    moq_media_object_input_t in = make_input(NULL, NULL);
    for (size_t i = 0; i < sizeof(pairs) / sizeof(pairs[0]); ++i) {
        moq_media_track_info_t track = make_track(pairs[i].version, MOQ_MEDIA_PACKAGING_CMAF);
        for (size_t terminal = 0; terminal < 2; ++terminal) {
            in.status = terminal ? MOQ_OBJECT_END_OF_TRACK : MOQ_OBJECT_NORMAL;
            moq_media_parsed_object_t out;
            memset(&out, 0xa5, sizeof(out));
            moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
            MOQ_TEST_CHECK_EQ_INT(parse_companion(&track, pairs[i].companion,
                &in, NULL, 0, &out, sizeof(out), &reason), pairs[i].result);
            MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_STALE && bytes_are((uint8_t *)&out, sizeof(out), 0xa5));
        }
    }
    moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_22, MOQ_MEDIA_PACKAGING_RAW);
    moq_media_parsed_object_t out;
    for (size_t i = 0; i < 8; ++i) {
        moq_media_track_info_t bad_track = track;
        moq_media_object_input_t bad_in = in;
        bad_in.status = MOQ_OBJECT_NORMAL;
        memset(&out, 0xa5, sizeof(out));
        if (i == 0) bad_track.struct_size = MOQ_MEDIA_TRACK_INFO_V0_SIZE - 1;
        if (i == 1) bad_in.struct_size = 0;
        if (i == 2) bad_track.media_type = (moq_media_type_t)0;
        if (i == 3) bad_track.packaging = (moq_media_packaging_t)0;
        if (i == 4) bad_in.status = (moq_object_status_t)UINT8_MAX;
        moq_result_t rc = parse_companion(i == 5 ? NULL : &bad_track,
            MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC, i == 6 ? NULL : &bad_in,
            NULL, 0, i == 7 ? NULL : &out, sizeof(out), NULL);
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(bytes_are((uint8_t *)&out, sizeof(out), 0xa5));
    }
    return failures;
}

static int old_allocations(void)
{
    int failures = 0;
    const uint8_t byte = 0x41, wire16[] = {2, 0x43, 0xe8}, wire18[] = {2, 0x83, 0xe8};
    moq_rcbuf_t *payload = literal_buffer(&byte, 1);
    MOQ_TEST_CHECK(payload != NULL);
    if (!payload) return failures;
    const size_t version_end = offsetof(media_old_track_t, transport_version) + sizeof(moq_version_t);
    for (size_t size = MOQ_MEDIA_TRACK_INFO_V0_SIZE; size <= sizeof(media_old_track_t); ++size) {
        uint8_t *track_bytes = malloc(size);
        uint8_t *out_bytes = malloc(sizeof(media_old_parsed_t));
        MOQ_TEST_CHECK(track_bytes && out_bytes);
        if (!track_bytes || !out_bytes) {
            free(track_bytes); free(out_bytes);
            continue;
        }
        media_old_track_t old_track;
        memset(&old_track, 0xa5, sizeof(old_track));
        old_track.struct_size = (uint32_t)size;
        old_track.media_type = MOQ_MEDIA_TYPE_VIDEO;
        old_track.packaging = MOQ_MEDIA_PACKAGING_RAW;
        old_track.timescale = 90000;
        if (size >= version_end) old_track.transport_version = MOQ_VERSION_DRAFT_18;
        memcpy(track_bytes, &old_track, size);
        moq_rcbuf_t *props = literal_buffer(size >= version_end ? wire18 : wire16, sizeof(wire16));
        MOQ_TEST_CHECK(props != NULL);
        if (!props) { free(track_bytes); free(out_bytes); continue; }
        moq_media_object_input_t in = make_input(payload, props);
        moq_media_parsed_object_init((moq_media_parsed_object_t *)out_bytes);
        moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
        moq_result_t rc = moq_media_object_parse((moq_media_track_info_t *)track_bytes,
            &in, NULL, 0, (moq_media_parsed_object_t *)out_bytes, &reason);
        MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
        if (rc == MOQ_OK) {
            media_old_parsed_t old_out;
            memcpy(&old_out, out_bytes, sizeof(old_out));
            MOQ_TEST_CHECK(old_out.struct_size == sizeof(old_out) && old_out.capture_time_us == 1000 &&
                old_out.decode_time_us == 1000 && old_out.presentation_time_us == 1000 && reason == 0);
            MOQ_TEST_CHECK(old_out.payload.data == moq_rcbuf_data(payload) && old_out.payload.len == 1);
        }
        MOQ_TEST_CHECK(moq_rcbuf_refcount(props) == 1 && memcmp(track_bytes, &old_track, size) == 0);
        free(track_bytes); free(out_bytes); moq_rcbuf_decref(props);
    }
    uint8_t *prefix = malloc(MOQ_MEDIA_TRACK_INFO_V0_SIZE);
    MOQ_TEST_CHECK(prefix != NULL);
    if (!prefix) { moq_rcbuf_decref(payload); return failures; }
    memset(prefix, 0xa5, MOQ_MEDIA_TRACK_INFO_V0_SIZE);
    moq_media_track_info_init((moq_media_track_info_t *)prefix);
    uint32_t stamp;
    memcpy(&stamp, prefix, sizeof(stamp));
    MOQ_TEST_CHECK(stamp == MOQ_MEDIA_TRACK_INFO_V0_SIZE &&
        bytes_are(prefix + sizeof(stamp), MOQ_MEDIA_TRACK_INFO_V0_SIZE - sizeof(stamp), 0));
    free(prefix);

    moq_media_track_info_t track = make_track(MOQ_VERSION_DRAFT_18, MOQ_MEDIA_PACKAGING_RAW);
    const uint8_t duplicate[] = {2, 1, 0, 2};
    moq_rcbuf_t *props = literal_buffer(duplicate, sizeof(duplicate));
    MOQ_TEST_CHECK(props != NULL);
    if (!props) { moq_rcbuf_decref(payload); return failures; }
    moq_media_object_input_t in = make_input(payload, props);
    union { max_align_t alignment; uint8_t bytes[sizeof(moq_media_parsed_object_t) + 16]; } storage;
    memset(storage.bytes, 0xa5, sizeof(storage.bytes));
    moq_media_parsed_object_t *out = (moq_media_parsed_object_t *)storage.bytes;
    moq_media_drop_reason_t reason = MOQ_MEDIA_DROP_STALE;
    moq_result_t rc = moq_media_object_parse(&track, &in, NULL, 0, out, &reason);
    MOQ_TEST_CHECK_EQ_INT(rc, MOQ_OK);
    if (rc == MOQ_OK)
        MOQ_TEST_CHECK(out->struct_size == sizeof(media_old_parsed_t) && out->capture_time_us == 2 && reason == 0);
    MOQ_TEST_CHECK(bytes_are(storage.bytes + sizeof(media_old_parsed_t),
        sizeof(storage.bytes) - sizeof(media_old_parsed_t), 0xa5));
    memset(storage.bytes, 0xa5, sizeof(storage.bytes));
    MOQ_TEST_CHECK_EQ_INT(parse_companion(&track,
        MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, &in, NULL, 0, out, sizeof(*out), &reason), MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(reason == 0 && bytes_are(storage.bytes, sizeof(storage.bytes), 0xa5));
    moq_rcbuf_decref(props); moq_rcbuf_decref(payload);
    return failures;
}

int main(void)
{
    static const struct { const char *name; int (*run)(void); } cases[] = {
        {"explicit_application_clock", explicit_application_clock},
        {"exact_clocks", exact_clocks},
        {"marking_and_policy", marking_and_policy},
        {"object_config_spans", object_config_spans},
        {"cmaf_and_terminal", cmaf_and_terminal},
        {"track_context", track_context},
        {"context_sizes_and_refusals", context_sizes_and_refusals},
        {"legacy_context_defaults", legacy_context_defaults},
        {"output_sizes", output_sizes},
        {"explicit_pairs_and_inputs", explicit_pairs_and_inputs},
        {"old_allocations", old_allocations},
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
        int n = cases[i].run();
        printf("CASE %s: %s (%d failed checks)\n", cases[i].name, n ? "FAIL" : "PASS", n);
        failures += n;
    }
    printf("%s: %zu cases, %d failures\n", failures ? "FAIL" : "PASS",
           sizeof(cases) / sizeof(cases[0]), failures);
    return failures != 0;
}
