/*
 * The media sender's catalog revision (moq_media_sender_cfg_t.catalog_revision).
 *
 * A sender publishes MSF-01 (draft-ietf-moq-msf-01) unless it is configured
 * for MSF-02 (draft-ietf-moq-msf-02). These cases pin:
 *   - the default: catalog and delta bytes are exactly those an MSF-01
 *     sender has always written;
 *   - REVISION_02: "version":"draft-02" (MSF-02 5.1.1), generatedAt and no
 *     other root field on every delta (5.3), SAP timeline records keyed "L"
 *     (8.2), the 5.2.4.1 required fields and the 11.2.1 naming rule refused
 *     at add_track, a generated timeline published only with the track its
 *     "depends" names (5.2.15), and the track supplement record taken from
 *     the track configuration;
 *   - the cfg reader: an old or partial struct_size is MSF-01, an unknown
 *     revision fails creation.
 *
 * What a catalog says is read back with the public parser and check, not
 * compared against bytes copied from the implementation, except for the
 * default bytes, which are the regression pin.
 */
#include <moq/media_sender.h>
#include <moq/endpoint.h>
#include <moq/msf.h>
#include <moq/rcbuf.h>
#include <moq/session.h>
#include <moq/sim.h>
#include "test_support.h"
#include "media_sender_cmaf_init.h"

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Test seams (MOQ_MEDIA_SENDER_TESTING). No header declares them. */
moq_media_sender_t *moq_media_sender_test_new_cfg(const moq_media_sender_cfg_t *cfg);
moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_ready(moq_media_sender_t *s);
moq_result_t moq_media_sender_test_build_catalog(moq_media_sender_t *s,
                                                 moq_rcbuf_t **out);
void moq_media_sender_test_mark_published(moq_media_sender_t *s);
void moq_media_sender_test_mark_registered(moq_media_sender_t *s);
void moq_media_sender_test_register_track(moq_media_sender_t *s,
                                          moq_media_track_t *track);
size_t moq_media_sender_test_stage(moq_media_sender_t *s,
                                   moq_rcbuf_t **objs, size_t cap);
void moq_media_sender_test_set_clock_ms(moq_media_sender_t *s,
                                        bool set, uint64_t ms);
void moq_media_sender_test_pump(moq_media_sender_t *s,
                                moq_session_t *session, uint64_t now_us);
moq_media_track_t *moq_media_sender_test_timeline_track(
    const moq_media_track_t *track, bool media_timeline);

#define STAMP_A 1700000000123ull
#define STAMP_B 1700000000456ull

static moq_bytes_t g_ns[2];

static moq_bytes_t lit(const char *s)
{
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

static bool bytes_is(moq_bytes_t b, const char *s)
{
    size_t n = strlen(s);
    return b.len == n && (n == 0 || (b.data && memcmp(b.data, s, n) == 0));
}

static bool rc_starts(const moq_rcbuf_t *j, const char *prefix)
{
    size_t n = strlen(prefix);
    return j && moq_rcbuf_len(j) >= n && memcmp(moq_rcbuf_data(j), prefix, n) == 0;
}

static bool mem_contains(const uint8_t *d, size_t len, const char *needle)
{
    size_t n = strlen(needle);
    for (size_t i = 0; n <= len && i <= len - n; i++)
        if (memcmp(d + i, needle, n) == 0) return true;
    return false;
}

static bool rc_contains(const moq_rcbuf_t *j, const char *needle)
{
    return j && mem_contains(moq_rcbuf_data(j), moq_rcbuf_len(j), needle);
}

/* Print a buffer as a C string literal, so a changed byte is visible. */
static void print_literal(const char *what, const uint8_t *d, size_t len)
{
    fprintf(stderr, "  %s (%zu bytes):\n  \"", what, len);
    for (size_t i = 0; i < len; i++) {
        uint8_t c = d[i];
        if (c == '"' || c == '\\') fprintf(stderr, "\\%c", c);
        else if (c >= 0x20 && c < 0x7f) fputc(c, stderr);
        else fprintf(stderr, "\\x%02x", c);
    }
    fprintf(stderr, "\"\n");
}

static void expect_bytes(const moq_rcbuf_t *j, const char *want, const char *what)
{
    size_t n = strlen(want);
    bool eq = j && moq_rcbuf_len(j) == n &&
              memcmp(moq_rcbuf_data(j), want, n) == 0;
    if (!eq) {
        fprintf(stderr, "FAIL: %s differs\n", what);
        if (j) print_literal("got", moq_rcbuf_data(j), moq_rcbuf_len(j));
        failures++;
    }
}

/* A sender with no endpoint, its catalog clock pinned at STAMP_A. */
static moq_media_sender_t *fx_sender_cfg(moq_media_sender_cfg_t *cfg)
{
    g_ns[0] = lit("svc");
    g_ns[1] = lit("msf02");
    cfg->namespace_ = (moq_namespace_t){ g_ns, 2 };
    moq_media_sender_t *s = moq_media_sender_test_new_cfg(cfg);
    MOQ_TEST_CHECK(s != NULL);
    if (s) moq_media_sender_test_set_clock_ms(s, true, STAMP_A);
    return s;
}

static moq_media_sender_t *fx_sender_default(void)
{
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    return fx_sender_cfg(&cfg);
}

static moq_media_sender_t *fx_sender_rev(moq_msf_revision_t rev)
{
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.catalog_revision = rev;
    return fx_sender_cfg(&cfg);
}

static void video_cfg(moq_media_track_cfg_t *tc, const char *name)
{
    moq_media_track_cfg_init_sized(tc, sizeof(*tc));
    tc->name = lit(name);
    tc->media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc->packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc->codec = lit("av01.0.08M.08");
    tc->bitrate = 2000000;
    tc->width = 1280;
    tc->height = 720;
    tc->framerate_millis = 30000;
}

static void audio_cfg(moq_media_track_cfg_t *tc, const char *name)
{
    moq_media_track_cfg_init_sized(tc, sizeof(*tc));
    tc->name = lit(name);
    tc->media_type = MOQ_MEDIA_TYPE_AUDIO;
    tc->packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc->codec = lit("opus");
    tc->bitrate = 128000;
    tc->samplerate = 48000;
    tc->channel_config = lit("2");
}

static moq_media_track_t *add_ok(moq_media_sender_t *s,
                                 const moq_media_track_cfg_t *tc)
{
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, tc, &t), (int)MOQ_OK);
    MOQ_TEST_CHECK(t != NULL);
    return t;
}

/* add_track's refusal: the result, a cleared handle, nothing registered. */
static void add_refused(moq_media_sender_t *s, const moq_media_track_cfg_t *tc,
                        moq_result_t want, const char *what)
{
    moq_media_track_t *t = (moq_media_track_t *)(uintptr_t)1;
    moq_result_t rc = moq_media_sender_add_track(s, tc, &t);
    if (rc != want || t != NULL) {
        fprintf(stderr, "FAIL: add_track (%s) returned %d, expected %d\n",
                what, (int)rc, (int)want);
        failures++;
    }
}

static const uint8_t g_video_init[8] = { 0x0a, 0x0b, 0x0c, 0x0d, 1, 2, 3, 4 };
static uint8_t g_cmaf_init[256];

/* The catalog fixture: a RAW video track with init data and both generated
 * timelines, a RAW audio track, a CMAF track and a VOD clip. Returns the
 * clip, which the delta cases remove. */
static moq_media_track_t *fx_tracks(moq_media_sender_t *s)
{
    moq_media_track_cfg_t tc;
    video_cfg(&tc, "video");
    tc.init_data = (moq_bytes_t){ g_video_init, sizeof(g_video_init) };
    tc.timescale = 90000;
    tc.emit_sap_timeline = true;
    tc.emit_media_timeline = true;
    tc.has_render_group = true;
    tc.render_group = 1;
    tc.has_label = true;
    tc.label = lit("Main");
    (void)add_ok(s, &tc);

    audio_cfg(&tc, "audio");
    tc.lang = lit("en");
    tc.has_render_group = true;
    tc.render_group = 1;
    (void)add_ok(s, &tc);

    moq_media_track_cfg_init_sized(&tc, sizeof(tc));
    tc.name = lit("cmaf");
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    tc.codec = lit("avc1.42c01e");
    tc.bitrate = 1000000;
    tc.framerate_millis = 25000;
    tc.has_max_grp_sap = true;
    tc.max_grp_sap = 2;
    tc.init_data = (moq_bytes_t){ g_cmaf_init, sender_test_cmaf_init(g_cmaf_init, 1) };
    (void)add_ok(s, &tc);

    video_cfg(&tc, "clip");
    tc.is_live = false;
    tc.has_track_duration = true;
    tc.track_duration_ms = 60000;
    return add_ok(s, &tc);
}

/* Publish the fixture as the baseline at STAMP_A, then, at STAMP_B, remove
 * the clip and add an init-less video track, and stage that generation:
 * objs[0] the base, objs[1] the remove delta, objs[2] the add delta. */
static size_t fx_stage_deltas(moq_media_sender_t *s, moq_media_track_t *clip,
                              moq_rcbuf_t **objs)
{
    moq_media_sender_test_set_ready(s);
    moq_media_sender_test_mark_registered(s);
    moq_media_sender_test_mark_published(s);
    moq_media_sender_test_set_clock_ms(s, true, STAMP_B);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_remove_track(s, clip), (int)MOQ_OK);
    moq_media_track_cfg_t tc;
    video_cfg(&tc, "late");
    (void)add_ok(s, &tc);
    moq_media_sender_test_mark_registered(s);
    return moq_media_sender_test_stage(s, objs, 3);
}

/* Parse through the full extent (both revisions, supplement records). */
static bool parse_full(const moq_rcbuf_t *j, moq_msf_catalog_t *c)
{
    memset(c, 0, sizeof(*c));
    if (!j) return false;
    moq_result_t rc = moq_msf_catalog_parse_sized(moq_alloc_default(),
        (moq_bytes_t){ moq_rcbuf_data(j), moq_rcbuf_len(j) }, c, sizeof(*c));
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    return rc == MOQ_OK;
}

static const moq_msf_track_t *find_track(const moq_msf_catalog_t *c,
                                         const char *name)
{
    for (size_t i = 0; i < c->track_count; i++)
        if (bytes_is(c->tracks[i].name, name)) return &c->tracks[i];
    return NULL;
}

static void free_objs(moq_rcbuf_t **objs, size_t n)
{
    for (size_t i = 0; i < n; i++)
        if (objs[i]) { moq_rcbuf_decref(objs[i]); objs[i] = NULL; }
}

/* -- default_bytes_unchanged ------------------------------------------- *
 * A sender configured without catalog_revision writes the MSF-01 catalog and
 * deltas byte for byte as before: "draft-01", the timelines' mimeType and
 * depends, no generatedAt on a delta. An explicit REVISION_01 is the same
 * sender. */
static const char GOLDEN_CATALOG[] =
    "{\"version\":\"draft-01\",\"generatedAt\":1700000000123,"
    "\"tracks\":[{\"name\":\"video\",\"packaging\":\"loc\","
    "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01.0.08M.08\","
    "\"width\":1280,\"height\":720,\"bitrate\":2000000,"
    "\"framerate\":30,\"renderGroup\":1,\"timescale\":90000,"
    "\"label\":\"Main\",\"initRef\":\"video\"},{\"name\":\"video.sap\","
    "\"packaging\":\"eventtimeline\",\"isLive\":true,\"eventType\":\"org.ietf"
    ".moq.cmsf.sap\",\"mimeType\":\"application/json\","
    "\"depends\":[\"video\"]},{\"name\":\"video.timeline\","
    "\"packaging\":\"mediatimeline\",\"isLive\":true,\"mimeType\":\"applicati"
    "on/json\",\"depends\":[\"video\"]},{\"name\":\"audio\","
    "\"packaging\":\"loc\",\"isLive\":true,\"role\":\"audio\","
    "\"codec\":\"opus\",\"samplerate\":48000,\"bitrate\":128000,"
    "\"renderGroup\":1,\"timescale\":1000000,\"channelConfig\":\"2\","
    "\"lang\":\"en\"},{\"name\":\"cmaf\",\"packaging\":\"cmaf\","
    "\"isLive\":true,\"role\":\"video\",\"codec\":\"avc1.42c01e\","
    "\"bitrate\":1000000,\"framerate\":25,\"timescale\":1000000,"
    "\"initRef\":\"cmaf\",\"maxGrpSapStartingType\":2},"
    "{\"name\":\"clip\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"role\":\"video\",\"codec\":\"av01.0.08M.08\",\"width\":1280,"
    "\"height\":720,\"bitrate\":2000000,\"framerate\":30,"
    "\"timescale\":1000000,\"trackDuration\":60000}],\"initDataList\":[{\"id\""
    ":\"video\",\"type\":\"inline\",\"data\":\"CgsMDQECAwQ=\"},"
    "{\"id\":\"cmaf\",\"type\":\"inline\",\"data\":\"AAAAFGZ0eXBpc29tAAAAAGlz"
    "b20AAADHbW9vdgAAAL90cmFrAAAAGHRraGQAAAAAAAAAAAAAAAAAAAABAAAAn21kaWEAAAAY"
    "bWRoZAAAAAAAAAAAAAAAAAABX5AAAAB/bWluZgAAAHdzdGJsAAAAb3N0c2QAAAAAAAAAAQAA"
    "AF9hdmMxAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACWF2Y0MB\"}]}";

static const char GOLDEN_REMOVE[] =
    "{\"deltaUpdate\":[{\"op\":\"remove\",\"tracks\":[{\"name\":\"clip\"}]}]}";

static const char GOLDEN_ADD[] =
    "{\"deltaUpdate\":[{\"op\":\"add\",\"tracks\":[{\"name\":\"late\","
    "\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\","
    "\"codec\":\"av01.0.08M.08\",\"width\":1280,\"height\":720,"
    "\"bitrate\":2000000,\"framerate\":30,\"timescale\":1000000}]}]}";

static void default_bytes_case(bool explicit_01)
{
    moq_media_sender_t *s = explicit_01 ? fx_sender_rev(MOQ_MSF_REVISION_01)
                                        : fx_sender_default();
    if (!s) return;
    moq_media_track_t *clip = fx_tracks(s);
    moq_rcbuf_t *cat = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &cat),
                          (int)MOQ_OK);
    expect_bytes(cat, GOLDEN_CATALOG, "independent catalog");

    moq_rcbuf_t *objs[3] = { NULL, NULL, NULL };
    size_t n = fx_stage_deltas(s, clip, objs);
    MOQ_TEST_CHECK_EQ_SIZE(n, 3u);
    expect_bytes(objs[0], GOLDEN_CATALOG, "delta base");
    expect_bytes(objs[1], GOLDEN_REMOVE, "remove delta");
    expect_bytes(objs[2], GOLDEN_ADD, "add delta");
    free_objs(objs, 3);
    if (cat) moq_rcbuf_decref(cat);
    moq_media_sender_test_free(s);
}

static void test_default_bytes_unchanged(void)
{
    int before = failures;
    default_bytes_case(false);
    default_bytes_case(true);
    if (failures == before) printf("PASS: msf02.default_bytes_unchanged\n");
}

/* -- r02_catalog -------------------------------------------------------- *
 * A REVISION_02 sender's catalog is "draft-02", parses through the full
 * extent as REVISION_02 and passes the inbound check; the generated SAP
 * timeline carries eventType (the eventtimeline row of 5.2.4.1, which no
 * add_track configuration can leave out) and its "depends" resolves; a
 * configured supplement is emitted. */
static void test_r02_catalog(void)
{
    int before = failures;
    moq_media_sender_t *s = fx_sender_rev(MOQ_MSF_REVISION_02);
    if (!s) return;
    (void)fx_tracks(s);
    moq_msf_track_supplement_t sp;
    moq_msf_track_supplement_init(&sp);
    sp.has_packaging_metadata = true;
    sp.packaging_metadata = lit("streamMapping=group");
    sp.has_keyframe_spacing = true;
    sp.keyframe_spacing = 30;
    sp.has_accessible_window_ms = true;
    sp.accessible_window_ms = 30000;
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_SUPPLEMENT,
                                .u = { .supplement = &sp } };
    moq_media_track_cfg_t tc;
    video_cfg(&tc, "alt");
    tc.has_alt_group = true;
    tc.alt_group = 7;
    tc.extensions = &ext;
    tc.extension_count = 1;
    (void)add_ok(s, &tc);

    moq_rcbuf_t *cat = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &cat),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(rc_starts(cat, "{\"version\":\"draft-02\","
                                  "\"generatedAt\":1700000000123,"));
    moq_msf_catalog_t c;
    if (parse_full(cat, &c)) {
        MOQ_TEST_CHECK_EQ_INT((int)c.revision, (int)MOQ_MSF_REVISION_02);
        MOQ_TEST_CHECK(c.has_generated_at && c.generated_at == STAMP_A);
        MOQ_TEST_CHECK_EQ_SIZE(c.track_count, 7u);
        MOQ_TEST_CHECK_EQ_SIZE(c.init_data_count, 2u);
        moq_msf_catalog_error_t why = UINT32_MAX;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_check(moq_alloc_default(),
                                                         &c, NULL, &why),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)why, (int)MOQ_MSF_CATALOG_OK);
        const moq_msf_track_t *sap = find_track(&c, "video.sap");
        MOQ_TEST_CHECK(sap != NULL);
        if (sap) {
            MOQ_TEST_CHECK(bytes_is(sap->packaging, "eventtimeline"));
            MOQ_TEST_CHECK(sap->has_event_type &&
                           bytes_is(sap->event_type, "org.ietf.moq.cmsf.sap"));
            MOQ_TEST_CHECK(sap->depends_count == 1 &&
                           bytes_is(sap->depends[0], "video"));
        }
        const moq_msf_track_t *alt = find_track(&c, "alt");
        const moq_msf_track_supplement_t *got =
            alt ? moq_msf_track_supplement(alt) : NULL;
        MOQ_TEST_CHECK(got != NULL);
        if (got) {
            MOQ_TEST_CHECK(got->has_packaging_metadata &&
                           bytes_is(got->packaging_metadata, "streamMapping=group"));
            MOQ_TEST_CHECK(got->has_keyframe_spacing && got->keyframe_spacing == 30);
            MOQ_TEST_CHECK(got->has_accessible_window_ms &&
                           got->accessible_window_ms == 30000);
            MOQ_TEST_CHECK(!got->has_encryption_scheme && !got->has_cipher_suite &&
                           !got->has_key_id && !got->has_track_base_key);
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_track_stream_mapping(alt),
                                  (int)MOQ_MSF_STREAM_MAPPING_GROUP);
        }
        /* The parsed catalog re-encodes strictly as MSF-02. */
        moq_rcbuf_t *again = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_encode(moq_alloc_default(),
                                                          &c, &again),
                              (int)MOQ_OK);
        if (again) moq_rcbuf_decref(again);
        moq_msf_catalog_cleanup(moq_alloc_default(), &c);
    }
    if (cat) moq_rcbuf_decref(cat);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: msf02.r02_catalog\n");
}

/* -- r02_delta_generated_at --------------------------------------------- *
 * MSF-02 5.3: every delta carries generatedAt -- the stamp of the generation
 * it belongs to -- and no other root field. The library's MSF-02 apply
 * enforces the second half (any other root key a parsed delta held is
 * MOQ_ERR_PROTO), so applying each delta in order onto the base, the way a
 * receiver does, is the proof; the negative control shows that apply refuses
 * an MSF-01 delta (no generatedAt) on the same base. */
static void test_r02_delta_generated_at(void)
{
    int before = failures;
    const moq_alloc_t *al = moq_alloc_default();
    moq_media_sender_t *s = fx_sender_rev(MOQ_MSF_REVISION_02);
    if (!s) return;
    moq_media_track_t *clip = fx_tracks(s);
    moq_rcbuf_t *objs[3] = { NULL, NULL, NULL };
    size_t n = fx_stage_deltas(s, clip, objs);
    MOQ_TEST_CHECK_EQ_SIZE(n, 3u);
    MOQ_TEST_CHECK(rc_starts(objs[0], "{\"version\":\"draft-02\","
                                      "\"generatedAt\":1700000000123,"));
    MOQ_TEST_CHECK(rc_starts(objs[1], "{\"generatedAt\":1700000000456,"
                                      "\"deltaUpdate\":[{\"op\":\"remove\","));
    MOQ_TEST_CHECK(rc_starts(objs[2], "{\"generatedAt\":1700000000456,"
                                      "\"deltaUpdate\":[{\"op\":\"add\","));
    moq_msf_catalog_t eff;
    if (n == 3 && parse_full(objs[0], &eff)) {
        for (size_t i = 1; i < n; i++) {
            moq_msf_catalog_t d, out;
            if (!parse_full(objs[i], &d)) continue;
            MOQ_TEST_CHECK(d.delta_update != NULL && d.delta_update_count == 1);
            MOQ_TEST_CHECK(d.has_generated_at && d.generated_at == STAMP_B);
            MOQ_TEST_CHECK(d.track_count == 0 && d.init_data_count == 0 &&
                           d.content_protection_count == 0 && !d.is_complete &&
                           d.publish_track_count == 0);
            moq_result_t arc = moq_msf_catalog_apply_delta_sized(al, &eff, &d,
                                                                 NULL, &out,
                                                                 sizeof(out));
            MOQ_TEST_CHECK_EQ_INT((int)arc, (int)MOQ_OK);
            moq_msf_catalog_cleanup(al, &d);
            if (arc == MOQ_OK) {
                moq_msf_catalog_cleanup(al, &eff);
                eff = out;
            }
        }
        MOQ_TEST_CHECK_EQ_INT((int)eff.revision, (int)MOQ_MSF_REVISION_02);
        MOQ_TEST_CHECK_EQ_SIZE(eff.track_count, 6u);
        MOQ_TEST_CHECK(find_track(&eff, "clip") == NULL);
        MOQ_TEST_CHECK(find_track(&eff, "late") != NULL);
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_check(al, &eff, NULL, NULL),
                              (int)MOQ_OK);

        /* Negative control: the same removal as an MSF-01 delta, without
         * generatedAt, is refused on this base. */
        moq_msf_catalog_t base, d01, out01;
        if (parse_full(objs[0], &base)) {
            MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_parse_sized(al,
                lit(GOLDEN_REMOVE), &d01, sizeof(d01)), (int)MOQ_OK);
            moq_result_t nrc = moq_msf_catalog_apply_delta_sized(
                al, &base, &d01, NULL, &out01, sizeof(out01));
            MOQ_TEST_CHECK_EQ_INT((int)nrc, (int)MOQ_ERR_PROTO);
            if (nrc == MOQ_OK) moq_msf_catalog_cleanup(al, &out01);
            moq_msf_catalog_cleanup(al, &d01);
            moq_msf_catalog_cleanup(al, &base);
        }
        moq_msf_catalog_cleanup(al, &eff);
    }
    free_objs(objs, 3);
    moq_media_sender_test_free(s);
    if (failures == before) printf("PASS: msf02.r02_delta_generated_at\n");
}

/* -- r02_sap_key -------------------------------------------------------- *
 * The generated SAP timeline's records carry their Location under "L"
 * (MSF-02 8.2) from a REVISION_02 sender and under "l" (CMSF-01 3.6.1)
 * otherwise. Driven through the production hook over a SimPair, with a real
 * subscriber on the media track and on its timeline. */
typedef struct {
    moq_simpair_t      *sp;
    moq_media_sender_t *s;
    moq_subscription_t  sub_v, sub_sap;
    int                 sub_ok;
    int                 sap_n;
    uint8_t             sap[256];   /* the first timeline payload */
    size_t              sap_len;
    bool                sap_truncated;
} sapx_t;

static void sapx_collect(sapx_t *x, uint64_t now)
{
    moq_session_t *srv = moq_simpair_server(x->sp);
    moq_event_t ev;
    while (moq_session_poll_events(srv, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_NAMESPACE_PUBLISHED) {
            moq_accept_namespace_cfg_t ac;
            moq_accept_namespace_cfg_init(&ac);
            MOQ_TEST_CHECK_EQ_INT((int)moq_session_accept_namespace(
                srv, ev.u.namespace_published.ann, &ac, now), (int)MOQ_OK);
        } else if (ev.kind == MOQ_EVENT_SUBSCRIBE_OK) {
            x->sub_ok++;
        } else if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            const moq_object_received_event_t *o = &ev.u.object_received;
            if (o->sub._opaque == x->sub_sap._opaque && o->payload &&
                x->sap_n++ == 0) {
                size_t len = moq_rcbuf_len(o->payload);
                x->sap_truncated = len > sizeof(x->sap);
                x->sap_len = x->sap_truncated ? sizeof(x->sap) : len;
                memcpy(x->sap, moq_rcbuf_data(o->payload), x->sap_len);
            }
        }
        moq_event_cleanup(&ev);
    }
}

static void sapx_drive(sapx_t *x)
{
    uint64_t now = moq_simpair_now_us(x->sp);
    moq_media_sender_test_pump(x->s, moq_simpair_client(x->sp), now);
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_run_until_quiescent(x->sp, 64, NULL),
                          (int)MOQ_OK);
    sapx_collect(x, now);
}

static bool sapx_subscribe(sapx_t *x, const char *name, moq_subscription_t *sub)
{
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ g_ns, 2 };
    sc.track_name = lit(name);
    sc.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
    sc.has_forward = true;
    sc.forward = true;
    if (moq_session_subscribe(moq_simpair_server(x->sp), &sc,
                              moq_simpair_now_us(x->sp), sub) != MOQ_OK)
        return false;
    int want = x->sub_ok + 1;
    for (int i = 0; i < 16 && x->sub_ok < want; i++) sapx_drive(x);
    return x->sub_ok == want;
}

static moq_result_t sapx_write(sapx_t *x, moq_media_track_t *v, bool starts,
                               moq_sap_type_t sap, uint64_t pts_us)
{
    uint8_t d[16];
    memset(d, starts ? 0x11 : 0x22, sizeof(d));
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(moq_alloc_default(), d, sizeof(d), &b) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = b;
    o.is_sync = starts;
    o.starts_group = starts;
    o.presentation_time_us = pts_us;
    o.has_sap_type = true;
    o.sap_type = sap;
    moq_result_t rc = moq_media_sender_write(x->s, v, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(b);
    return rc;
}

static void sap_key_case(moq_msf_revision_t rev)
{
    sapx_t x;
    memset(&x, 0, sizeof(x));
    moq_simpair_cfg_t sc = MOQ_SIMPAIR_CFG_INIT;
    sc.alloc = moq_alloc_default();
    sc.seed = 42;
    sc.initial_now_us = 1000;
    sc.client_send_request_capacity = true;
    sc.client_initial_request_capacity = 32;
    sc.server_send_request_capacity = true;
    sc.server_initial_request_capacity = 32;
    sc.version = MOQ_VERSION_DRAFT_18;
    sc.max_events = 256;
    MOQ_TEST_CHECK_EQ_INT((int)moq_simpair_create(&sc, &x.sp), (int)MOQ_OK);
    if (!x.sp) return;
    moq_simpair_start(x.sp);
    (void)moq_simpair_run_until_quiescent(x.sp, 64, NULL);
    moq_event_t ev;
    while (moq_session_poll_events(moq_simpair_client(x.sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(x.sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);

    x.s = fx_sender_rev(rev);
    moq_media_track_t *v = NULL;
    if (x.s) {
        moq_media_track_cfg_t tc;
        video_cfg(&tc, "v");
        tc.timescale = 1000000;
        tc.emit_sap_timeline = true;
        v = add_ok(x.s, &tc);
    }
    if (x.s && v) {
        for (int c = 0; c < 16 && !moq_media_sender_is_ready(x.s); c++)
            sapx_drive(&x);
        MOQ_TEST_CHECK(moq_media_sender_is_ready(x.s));
        MOQ_TEST_CHECK(sapx_subscribe(&x, "v", &x.sub_v));
        MOQ_TEST_CHECK(sapx_subscribe(&x, "v.sap", &x.sub_sap));
        MOQ_TEST_CHECK_EQ_INT((int)sapx_write(&x, v, true, MOQ_SAP_TYPE_1, 0),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)sapx_write(&x, v, false, MOQ_SAP_TYPE_3, 33000),
                              (int)MOQ_OK);
        for (int d = 0; d < 32 && x.sap_n == 0; d++) sapx_drive(&x);
        MOQ_TEST_CHECK(x.sap_n >= 1 && !x.sap_truncated);
        const char *want = rev == MOQ_MSF_REVISION_02 ? "[{\"L\":[" : "[{\"l\":[";
        const char *other = rev == MOQ_MSF_REVISION_02 ? "\"l\":" : "\"L\":";
        bool ok = x.sap_len >= strlen(want) &&
                  memcmp(x.sap, want, strlen(want)) == 0 &&
                  !mem_contains(x.sap, x.sap_len, other) &&
                  mem_contains(x.sap, x.sap_len, "\"data\":[1,0]");
        if (!ok) {
            fprintf(stderr, "FAIL: SAP timeline payload (revision %u)\n",
                    (unsigned)rev);
            print_literal("got", x.sap, x.sap_len);
            failures++;
        }
    }
    if (x.s) moq_media_sender_test_free(x.s);
    while (moq_session_poll_events(moq_simpair_client(x.sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    while (moq_session_poll_events(moq_simpair_server(x.sp), &ev, 1) == 1)
        moq_event_cleanup(&ev);
    moq_simpair_destroy(x.sp);
}

static void test_r02_sap_key(void)
{
    int before = failures;
    sap_key_case(MOQ_MSF_REVISION_02);
    sap_key_case(MOQ_MSF_REVISION_01);
    if (failures == before) printf("PASS: msf02.r02_sap_key\n");
}

/* -- r02_required_fields ------------------------------------------------ *
 * MSF-02 5.2.4.1 Table 6, keyed on (packaging, role): one row per cell with
 * that field left out, refused by a REVISION_02 sender. REVISION_01 keeps the
 * sender's MSF-01 rules, keyed on media type (codec and bitrate for audio and
 * video, samplerate and channelConfig for audio): a cell those rules already
 * cover is refused there too, every other row is accepted. The eventtimeline
 * row (eventType) has no add_track configuration -- packaging is RAW, CMAF or
 * MPEG2TS -- and the generated SAP timeline always carries it (r02_catalog). */
enum { B_AUDIO, B_VIDEO, B_AD, B_SL, B_AUDIO_ON_VIDEO, B_AD_ON_VIDEO, B_CMAF };
enum { F_NONE, F_CODEC, F_BITRATE, F_SAMPLERATE, F_CHANNELS, F_FRAMERATE, F_LANG };

typedef struct {
    int base, drop;
    moq_result_t rc01, rc02;
} req_row_t;

#define OK_ MOQ_OK
#define BAD MOQ_ERR_INVAL
static const req_row_t g_req_rows[] = {
    { B_AUDIO, F_NONE,       OK_, OK_ },
    { B_AUDIO, F_CODEC,      BAD, BAD },
    { B_AUDIO, F_BITRATE,    BAD, BAD },
    { B_AUDIO, F_SAMPLERATE, BAD, BAD },
    { B_AUDIO, F_CHANNELS,   BAD, BAD },
    { B_VIDEO, F_NONE,       OK_, OK_ },
    { B_VIDEO, F_CODEC,      BAD, BAD },
    { B_VIDEO, F_BITRATE,    BAD, BAD },
    { B_VIDEO, F_FRAMERATE,  OK_, BAD },
    { B_AD,    F_NONE,       OK_, OK_ },
    { B_AD,    F_CODEC,      BAD, BAD },
    { B_AD,    F_SAMPLERATE, BAD, BAD },
    { B_AD,    F_CHANNELS,   BAD, BAD },
    { B_AD,    F_LANG,       OK_, BAD },
    { B_SL,    F_NONE,       OK_, OK_ },
    { B_SL,    F_CODEC,      BAD, BAD },
    { B_SL,    F_FRAMERATE,  OK_, BAD },
    /* The table is keyed on role, not media type. */
    { B_AUDIO_ON_VIDEO, F_NONE,       OK_, OK_ },
    { B_AUDIO_ON_VIDEO, F_SAMPLERATE, OK_, BAD },
    { B_AUDIO_ON_VIDEO, F_CHANNELS,   OK_, BAD },
    { B_AD_ON_VIDEO,    F_NONE,       OK_, OK_ },
    { B_AD_ON_VIDEO,    F_SAMPLERATE, OK_, BAD },
    { B_AD_ON_VIDEO,    F_CHANNELS,   OK_, BAD },
    { B_AD_ON_VIDEO,    F_LANG,       OK_, BAD },
    /* The table has no cmaf rows. */
    { B_CMAF,  F_FRAMERATE,  OK_, OK_ },
};
#undef OK_
#undef BAD

static void req_cfg(moq_media_track_cfg_t *tc, int base, int drop,
                    const char *name)
{
    switch (base) {
    case B_AUDIO:
        audio_cfg(tc, name);
        break;
    case B_AD:
        audio_cfg(tc, name);
        tc->role = lit("audiodescription");
        tc->lang = lit("en");
        break;
    case B_SL:
        video_cfg(tc, name);
        tc->role = lit("signlanguage");
        break;
    case B_AUDIO_ON_VIDEO:
    case B_AD_ON_VIDEO:
        video_cfg(tc, name);
        tc->framerate_millis = 0;
        tc->role = lit(base == B_AUDIO_ON_VIDEO ? "audio" : "audiodescription");
        tc->samplerate = 48000;
        tc->channel_config = lit("2");
        if (base == B_AD_ON_VIDEO) tc->lang = lit("en");
        break;
    case B_CMAF:
        video_cfg(tc, name);
        tc->packaging = MOQ_MEDIA_PACKAGING_CMAF;
        tc->codec = lit("avc1.42c01e");
        tc->init_data = (moq_bytes_t){ g_cmaf_init,
                                       sender_test_cmaf_init(g_cmaf_init, 1) };
        break;
    default:
        video_cfg(tc, name);
        break;
    }
    switch (drop) {
    case F_CODEC:      tc->codec = (moq_bytes_t){ NULL, 0 }; break;
    case F_BITRATE:    tc->bitrate = 0; break;
    case F_SAMPLERATE: tc->samplerate = 0; break;
    case F_CHANNELS:   tc->channel_config = (moq_bytes_t){ NULL, 0 }; break;
    case F_FRAMERATE:  tc->framerate_millis = 0; break;
    case F_LANG:       tc->lang = (moq_bytes_t){ NULL, 0 }; break;
    default: break;
    }
}

static void test_r02_required_fields(void)
{
    int before = failures;
    moq_media_sender_t *s01 = fx_sender_rev(MOQ_MSF_REVISION_01);
    moq_media_sender_t *s02 = fx_sender_rev(MOQ_MSF_REVISION_02);
    if (!s01 || !s02) goto out;
    for (size_t i = 0; i < sizeof(g_req_rows) / sizeof(g_req_rows[0]); i++) {
        const req_row_t *r = &g_req_rows[i];
        char name[24];
        snprintf(name, sizeof(name), "r%zu", i);
        moq_media_track_cfg_t tc;
        req_cfg(&tc, r->base, r->drop, name);
        for (int rev = 0; rev < 2; rev++) {
            moq_media_sender_t *s = rev ? s02 : s01;
            moq_result_t want = rev ? r->rc02 : r->rc01;
            moq_media_track_t *t = (moq_media_track_t *)(uintptr_t)1;
            moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
            bool cleared = want == MOQ_OK ? t != NULL : t == NULL;
            if (rc != want || !cleared) {
                fprintf(stderr, "FAIL: required-fields row %zu (base %d, drop %d), "
                        "revision %d: add_track %d, expected %d\n",
                        i, r->base, r->drop, rev ? 2 : 1, (int)rc, (int)want);
                failures++;
            }
        }
    }
    /* What was accepted still makes a valid MSF-02 catalog. */
    moq_rcbuf_t *cat = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s02, &cat),
                          (int)MOQ_OK);
    moq_msf_catalog_t c;
    if (parse_full(cat, &c)) {
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_check(moq_alloc_default(),
                                                         &c, NULL, NULL),
                              (int)MOQ_OK);
        moq_msf_catalog_cleanup(moq_alloc_default(), &c);
    }
    if (cat) moq_rcbuf_decref(cat);
out:
    if (s01) moq_media_sender_test_free(s01);
    if (s02) moq_media_sender_test_free(s02);
    if (failures == before) printf("PASS: msf02.r02_required_fields\n");
}

/* -- r02_track_naming ---------------------------------------------------- *
 * MSF-02 11.2.1: a Track Name maps to bytes as UTF-8, and an author never
 * writes NUL. A REVISION_02 sender refuses such a name at add_track; an
 * MSF-01 catalog has no such rule and keeps accepting it. */
static void test_r02_track_naming(void)
{
    int before = failures;
    static const uint8_t bad_utf8[] = { 'v', 0xff };
    static const uint8_t with_nul[] = { 'v', 0, 'w' };
    const moq_bytes_t names[] = {
        { bad_utf8, sizeof(bad_utf8) }, { with_nul, sizeof(with_nul) } };
    moq_media_sender_t *s01 = fx_sender_rev(MOQ_MSF_REVISION_01);
    moq_media_sender_t *s02 = fx_sender_rev(MOQ_MSF_REVISION_02);
    for (size_t i = 0; s01 && s02 && i < 2; i++) {
        moq_media_track_cfg_t tc;
        video_cfg(&tc, "x");
        tc.name = names[i];
        add_refused(s02, &tc, MOQ_ERR_INVAL, "MSF-02 naming");
        (void)add_ok(s01, &tc);
    }
    if (s01) moq_media_sender_test_free(s01);
    if (s02) moq_media_sender_test_free(s02);
    if (failures == before) printf("PASS: msf02.r02_track_naming\n");
}

/* -- r02_timeline_follows_media ----------------------------------------- *
 * MSF-02 5.2.15: a catalog whose "depends" entry names no track is invalid.
 * A generated timeline names its media track, so a REVISION_02 sender keeps
 * the timeline out of the catalog until the media track is in it, however
 * their registrations interleave. MSF-01 has no such rule and keeps
 * publishing a registered timeline on its own. */
static void timeline_case(moq_msf_revision_t rev)
{
    moq_media_sender_t *s = fx_sender_rev(rev);
    if (!s) return;
    moq_media_track_cfg_t tc;
    video_cfg(&tc, "v");
    tc.emit_sap_timeline = true;
    moq_media_track_t *v = add_ok(s, &tc);
    moq_media_track_t *tl = v ? moq_media_sender_test_timeline_track(v, false) : NULL;
    MOQ_TEST_CHECK(tl != NULL);
    if (!tl) { moq_media_sender_test_free(s); return; }
    moq_media_sender_test_set_ready(s);
    moq_media_sender_test_register_track(s, tl);
    moq_rcbuf_t *objs[3] = { NULL, NULL, NULL };
    size_t n = moq_media_sender_test_stage(s, objs, 3);
    MOQ_TEST_CHECK_EQ_SIZE(n, 1u);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
    moq_msf_catalog_t c;
    if (n == 1 && parse_full(objs[0], &c)) {
        if (rev == MOQ_MSF_REVISION_02) {
            MOQ_TEST_CHECK_EQ_SIZE(c.track_count, 0u);
        } else {
            MOQ_TEST_CHECK_EQ_SIZE(c.track_count, 1u);
            MOQ_TEST_CHECK(find_track(&c, "v.sap") != NULL);
        }
        moq_msf_catalog_cleanup(moq_alloc_default(), &c);
    }
    free_objs(objs, 3);
    moq_media_sender_test_register_track(s, v);
    n = moq_media_sender_test_stage(s, objs, 3);
    MOQ_TEST_CHECK(n >= 1);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
    if (n >= 1 && parse_full(objs[0], &c)) {
        MOQ_TEST_CHECK_EQ_SIZE(c.track_count, 2u);
        MOQ_TEST_CHECK(find_track(&c, "v") != NULL);
        MOQ_TEST_CHECK(find_track(&c, "v.sap") != NULL);
        moq_msf_catalog_cleanup(moq_alloc_default(), &c);
    }
    free_objs(objs, 3);
    moq_media_sender_test_free(s);
}

static void test_r02_timeline_follows_media(void)
{
    int before = failures;
    timeline_case(MOQ_MSF_REVISION_02);
    timeline_case(MOQ_MSF_REVISION_01);
    if (failures == before) printf("PASS: msf02.r02_timeline_follows_media\n");
}

/* -- supplement_cfg ------------------------------------------------------ *
 * The track supplement (MOQ_MSF_EXT_SUPPLEMENT) on a track configuration:
 * deep-copied at add_track, emitted in the catalog, and refused where the
 * sender cannot honour it -- a streamMapping other than "group" (the sender
 * sends one subgroup per group), any encryption field (it does not encrypt,
 * and MSF-02 13.2 forbids trackBaseKey in a catalog that is not encrypted
 * end to end), "depends" entries (it cannot keep a renderGroup dependency
 * resolved across remove_track), malformed or undersized records, and
 * MSF-02 fields on an MSF-01 sender. */
static void supp_base(moq_msf_track_supplement_t *sp)
{
    moq_msf_track_supplement_init(sp);
    sp->has_keyframe_spacing = true;
    sp->keyframe_spacing = 30;
}

static void supp_refused(moq_media_sender_t *s, const char *what,
                         const moq_msf_track_supplement_t *sp)
{
    /* A fresh name per row, so one wrongly accepted row cannot make the
     * next one fail on a name collision instead. */
    static unsigned row;
    char name[24];
    snprintf(name, sizeof(name), "refused%u", row++);
    moq_msf_track_ext_t ext = { .kind = MOQ_MSF_EXT_SUPPLEMENT,
                                .u = { .supplement = sp } };
    moq_media_track_cfg_t tc;
    video_cfg(&tc, name);
    tc.extensions = &ext;
    tc.extension_count = 1;
    add_refused(s, &tc, MOQ_ERR_INVAL, what);
}

static void test_supplement_cfg(void)
{
    int before = failures;
    const moq_alloc_t *al = moq_alloc_default();
    moq_media_sender_t *s = fx_sender_rev(MOQ_MSF_REVISION_02);
    if (!s) return;

    /* Deep copy: every caller buffer is overwritten and freed after add. */
    static const char pm_text[] = "streamMapping=group;vendor=x";
    char *pm = (char *)malloc(sizeof(pm_text));
    moq_msf_track_supplement_t *sp =
        (moq_msf_track_supplement_t *)malloc(sizeof(*sp));
    moq_msf_track_ext_t *ext = (moq_msf_track_ext_t *)malloc(sizeof(*ext));
    MOQ_TEST_CHECK(pm && sp && ext);
    if (pm && sp && ext) {
        memcpy(pm, pm_text, sizeof(pm_text));
        moq_msf_track_supplement_init(sp);
        sp->has_packaging_metadata = true;
        sp->packaging_metadata = (moq_bytes_t){ (const uint8_t *)pm,
                                                sizeof(pm_text) - 1 };
        sp->has_keyframe_spacing = true;
        sp->keyframe_spacing = 30;
        sp->has_accessible_window_ms = true;
        sp->accessible_window_ms = 30000;
        ext->kind = MOQ_MSF_EXT_SUPPLEMENT;
        ext->u.supplement = sp;
        moq_media_track_cfg_t tc;
        video_cfg(&tc, "v");
        tc.extensions = ext;
        tc.extension_count = 1;
        (void)add_ok(s, &tc);
        memset(pm, 'Z', sizeof(pm_text));
        memset(sp, 0xA5, sizeof(*sp));
        memset(ext, 0xA5, sizeof(*ext));
    }
    free(pm);
    free(sp);
    free(ext);
    moq_rcbuf_t *cat = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &cat),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(rc_contains(cat, "\"packagingMetadata\":"
                                    "\"streamMapping=group;vendor=x\","
                                    "\"keyframeSpacing\":30,"
                                    "\"accessibleWindowTime\":30000"));
    moq_msf_catalog_t c;
    if (parse_full(cat, &c)) {
        const moq_msf_track_t *t = find_track(&c, "v");
        const moq_msf_track_supplement_t *got = t ? moq_msf_track_supplement(t) : NULL;
        MOQ_TEST_CHECK(got != NULL);
        if (got) {
            MOQ_TEST_CHECK(bytes_is(got->packaging_metadata,
                                    "streamMapping=group;vendor=x"));
            MOQ_TEST_CHECK(got->keyframe_spacing == 30);
            MOQ_TEST_CHECK(got->accessible_window_ms == 30000);
        }
        moq_msf_catalog_cleanup(al, &c);
    }
    if (cat) moq_rcbuf_decref(cat);

    /* Refusals on an MSF-02 sender. */
    moq_msf_track_supplement_t r;
    static const char *const bad_mappings[] = {
        "streamMapping=object", "streamMapping=bogus",
        "x=1;streamMapping=object", "streamMapping=" };
    for (size_t i = 0; i < sizeof(bad_mappings) / sizeof(bad_mappings[0]); i++) {
        supp_base(&r);
        r.has_packaging_metadata = true;
        r.packaging_metadata = lit(bad_mappings[i]);
        supp_refused(s, bad_mappings[i], &r);
    }
    supp_base(&r);
    r.has_packaging_metadata = true;
    r.packaging_metadata = lit("streamMapping=group;");   /* 5.2.5 syntax */
    supp_refused(s, "packagingMetadata syntax", &r);
    supp_base(&r);
    r.has_packaging_metadata = true;
    r.packaging_metadata = (moq_bytes_t){ NULL, 3 };
    supp_refused(s, "packagingMetadata without bytes", &r);
    supp_base(&r);
    r.keyframe_spacing = 0;                                /* 5.2.46 */
    supp_refused(s, "keyframeSpacing 0", &r);
    supp_base(&r);
    r.has_encryption_scheme = true;
    r.encryption_scheme = lit("moq-secure-objects");
    supp_refused(s, "encryptionScheme", &r);
    supp_base(&r);
    r.has_cipher_suite = true;
    r.cipher_suite = lit("aes-128-gcm-sha256");
    supp_refused(s, "cipherSuite", &r);
    supp_base(&r);
    r.has_key_id = true;
    r.key_id = lit("k1");
    supp_refused(s, "keyId", &r);
    supp_base(&r);
    r.has_track_base_key = true;
    r.track_base_key = lit("AAAA");
    supp_refused(s, "trackBaseKey", &r);
    static const int groups[] = { 1 };
    supp_base(&r);
    r.depends_render_groups = groups;
    r.depends_render_group_count = 1;
    supp_refused(s, "depends renderGroup", &r);
    supp_base(&r);
    r.has_depends = true;
    supp_refused(s, "has_depends", &r);
    supp_base(&r);
    r.malformed = MOQ_MSF_SUPPLEMENT_F_NAMESPACE;
    supp_refused(s, "malformed bit", &r);
    supp_base(&r);
    r.struct_size = (uint32_t)MOQ_MSF_TRACK_SUPPLEMENT_V0_SIZE - 1u;
    supp_refused(s, "below the v0 size", &r);
    supp_refused(s, "NULL record", NULL);
    {
        supp_base(&r);
        moq_msf_track_ext_t two[2] = {
            { .kind = MOQ_MSF_EXT_SUPPLEMENT, .u = { .supplement = &r } },
            { .kind = MOQ_MSF_EXT_SUPPLEMENT, .u = { .supplement = &r } } };
        moq_media_track_cfg_t tc;
        video_cfg(&tc, "twice");
        tc.extensions = two;
        tc.extension_count = 2;
        add_refused(s, &tc, MOQ_ERR_INVAL, "supplement listed twice");
    }

    /* One accessibleWindowTime per altGroup (MSF-02 5.2.45). */
    {
        moq_msf_track_supplement_t w5, w6;
        moq_msf_track_supplement_init(&w5);
        w5.has_accessible_window_ms = true;
        w5.accessible_window_ms = 5000;
        w6 = w5;
        w6.accessible_window_ms = 6000;
        moq_msf_track_ext_t e5 = { .kind = MOQ_MSF_EXT_SUPPLEMENT,
                                   .u = { .supplement = &w5 } };
        moq_msf_track_ext_t e6 = { .kind = MOQ_MSF_EXT_SUPPLEMENT,
                                   .u = { .supplement = &w6 } };
        moq_media_track_cfg_t tc;
        video_cfg(&tc, "a1");
        tc.has_alt_group = true;
        tc.alt_group = 3;
        tc.extensions = &e5;
        tc.extension_count = 1;
        (void)add_ok(s, &tc);
        tc.name = lit("a2");
        tc.extensions = &e6;
        add_refused(s, &tc, MOQ_ERR_INVAL, "another window in the altGroup");
        tc.name = lit("a3");
        tc.extensions = NULL;
        tc.extension_count = 0;
        add_refused(s, &tc, MOQ_ERR_INVAL, "no window in the altGroup");
        tc.name = lit("a4");
        tc.extensions = &e5;
        tc.extension_count = 1;
        (void)add_ok(s, &tc);
        tc.name = lit("b1");
        tc.alt_group = 4;
        tc.extensions = NULL;
        tc.extension_count = 0;
        (void)add_ok(s, &tc);
    }
    cat = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &cat),
                          (int)MOQ_OK);
    MOQ_TEST_CHECK(!moq_media_sender_is_fatal(s));
    if (parse_full(cat, &c)) {
        MOQ_TEST_CHECK_EQ_SIZE(c.track_count, 4u);
        MOQ_TEST_CHECK_EQ_INT((int)moq_msf_catalog_check(al, &c, NULL, NULL),
                              (int)MOQ_OK);
        moq_msf_catalog_cleanup(al, &c);
    }
    if (cat) moq_rcbuf_decref(cat);
    moq_media_sender_test_free(s);

#ifdef MOQ_TEST_HAVE_MPEG2TS
    /* Both kinds on an MPEG2TS track, listed out of kind order. */
    s = fx_sender_rev(MOQ_MSF_REVISION_02);
    if (s) {
        moq_msf_mpeg2ts_t ts;
        moq_msf_mpeg2ts_init(&ts);
        ts.has_mode = true;
        ts.mode = MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM;
        ts.has_packet_size = true;
        ts.packet_size = 188;
        supp_base(&r);
        moq_msf_track_ext_t both[2] = {
            { .kind = MOQ_MSF_EXT_SUPPLEMENT, .u = { .supplement = &r } },
            { .kind = MOQ_MSF_EXT_MPEG2TS, .u = { .mpeg2ts = &ts } } };
        moq_media_track_cfg_t tc;
        moq_media_track_cfg_init_sized(&tc, sizeof(tc));
        tc.name = lit("ts");
        tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
        tc.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
        tc.bitrate = 3000000;
        tc.extensions = both;
        tc.extension_count = 2;
        (void)add_ok(s, &tc);
        cat = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_build_catalog(s, &cat),
                              (int)MOQ_OK);
        if (parse_full(cat, &c)) {
            const moq_msf_track_t *t = find_track(&c, "ts");
            MOQ_TEST_CHECK(t && moq_msf_track_mpeg2ts(t) != NULL &&
                           moq_msf_track_mpeg2ts(t)->packet_size == 188);
            const moq_msf_track_supplement_t *got =
                t ? moq_msf_track_supplement(t) : NULL;
            MOQ_TEST_CHECK(got && got->has_keyframe_spacing &&
                           got->keyframe_spacing == 30);
            moq_msf_catalog_cleanup(al, &c);
        }
        if (cat) moq_rcbuf_decref(cat);
        moq_media_sender_test_free(s);
    }
#endif

    /* An MSF-01 sender: the MSF-02 fields are refused; an empty record
     * changes no byte. */
    moq_media_sender_t *s01 = fx_sender_rev(MOQ_MSF_REVISION_01);
    moq_media_sender_t *plain = fx_sender_rev(MOQ_MSF_REVISION_01);
    if (s01 && plain) {
        supp_base(&r);
        supp_refused(s01, "keyframeSpacing on MSF-01", &r);
        moq_msf_track_supplement_init(&r);
        r.has_accessible_window_ms = true;
        r.accessible_window_ms = 1;
        supp_refused(s01, "accessibleWindowTime on MSF-01", &r);
        moq_msf_track_supplement_init(&r);
        r.has_packaging_metadata = true;
        r.packaging_metadata = lit("streamMapping=group");
        supp_refused(s01, "packagingMetadata on MSF-01", &r);
        moq_msf_track_supplement_init(&r);
        moq_msf_track_ext_t e = { .kind = MOQ_MSF_EXT_SUPPLEMENT,
                                  .u = { .supplement = &r } };
        moq_media_track_cfg_t tc;
        video_cfg(&tc, "v");
        (void)add_ok(plain, &tc);
        tc.extensions = &e;
        tc.extension_count = 1;
        (void)add_ok(s01, &tc);
        moq_rcbuf_t *a = NULL, *b = NULL;
        MOQ_TEST_CHECK(moq_media_sender_test_build_catalog(s01, &a) == MOQ_OK);
        MOQ_TEST_CHECK(moq_media_sender_test_build_catalog(plain, &b) == MOQ_OK);
        MOQ_TEST_CHECK(a && b && moq_rcbuf_len(a) == moq_rcbuf_len(b) &&
                       memcmp(moq_rcbuf_data(a), moq_rcbuf_data(b),
                              moq_rcbuf_len(a)) == 0);
        if (a) moq_rcbuf_decref(a);
        if (b) moq_rcbuf_decref(b);
    }
    if (s01) moq_media_sender_test_free(s01);
    if (plain) moq_media_sender_test_free(plain);
    if (failures == before) printf("PASS: msf02.supplement_cfg\n");
}

/* -- cfg_old_size -------------------------------------------------------- *
 * catalog_revision is read only when struct_size covers the whole field: a
 * caller built before it existed (struct_size the sizeof through
 * initial_group_id, its storage ending there) and a struct_size covering
 * part of it publish MSF-01. The reserved bytes after it are never read. An
 * unknown value fails creation. */
static bool catalog_is(moq_media_sender_t *s, const char *version)
{
    moq_media_track_cfg_t tc;
    video_cfg(&tc, "v");
    if (!add_ok(s, &tc)) return false;
    moq_rcbuf_t *cat = NULL;
    bool ok = moq_media_sender_test_build_catalog(s, &cat) == MOQ_OK &&
              rc_starts(cat, version);
    if (cat) moq_rcbuf_decref(cat);
    return ok;
}

static void test_cfg_old_size(void)
{
    int before = failures;
    const size_t at = offsetof(moq_media_sender_cfg_t, catalog_revision);
    const char *v01 = "{\"version\":\"draft-01\"";
    const char *v02 = "{\"version\":\"draft-02\"";

    moq_media_sender_cfg_t full;
    moq_media_sender_cfg_init_live_sized(&full, sizeof(full));
    MOQ_TEST_CHECK_EQ_INT((int)full.catalog_revision, (int)MOQ_MSF_REVISION_01);
    g_ns[0] = lit("svc");
    g_ns[1] = lit("msf02");
    full.namespace_ = (moq_namespace_t){ g_ns, 2 };
    full.catalog_revision = MOQ_MSF_REVISION_02;

    /* The old caller: its storage ends where the field begins. */
    {
        uint8_t *old = (uint8_t *)malloc(at);
        MOQ_TEST_CHECK(old != NULL);
        if (old) {
            memcpy(old, &full, at);
            uint32_t size = (uint32_t)at;
            memcpy(old, &size, sizeof(size));
            moq_media_sender_t *s = moq_media_sender_test_new_cfg(
                (const moq_media_sender_cfg_t *)(const void *)old);
            MOQ_TEST_CHECK(s != NULL);
            if (s) {
                MOQ_TEST_CHECK(catalog_is(s, v01));
                moq_media_sender_test_free(s);
            }
            free(old);
        }
    }
    /* Partial cover, the field exactly, and the whole struct with its
     * reserved bytes set. */
    static const struct { size_t size; bool reserved; bool r02; } rows[] = {
        { 1, false, false }, { 2, false, false }, { 3, false, false },
        { 4, false, true }, { 4, true, true }, { 0, true, true } };
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        moq_media_sender_cfg_t c = full;
        c.struct_size = rows[i].size ? (uint32_t)(at + rows[i].size)
                                     : (uint32_t)sizeof(c);
        if (rows[i].reserved)
            memset(c._reserved_catalog_revision, 0xFF,
                   sizeof(c._reserved_catalog_revision));
        moq_media_sender_t *s = moq_media_sender_test_new_cfg(&c);
        MOQ_TEST_CHECK(s != NULL);
        if (s) {
            if (!catalog_is(s, rows[i].r02 ? v02 : v01)) {
                fprintf(stderr, "FAIL: cfg_old_size row %zu\n", i);
                failures++;
            }
            moq_media_sender_test_free(s);
        }
    }
    /* Unknown values fail creation; a partial cover of one is not read. */
    static const uint32_t unknown[] = { 2u, 3u, UINT32_MAX };
    for (size_t i = 0; i < sizeof(unknown) / sizeof(unknown[0]); i++) {
        moq_media_sender_cfg_t c = full;
        c.catalog_revision = unknown[i];
        moq_media_sender_t *s = (moq_media_sender_t *)(uintptr_t)1;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_construct(&c, &s),
                              (int)MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(s == NULL);
        if (s && s != (moq_media_sender_t *)(uintptr_t)1)
            moq_media_sender_test_free(s);
        c.struct_size = (uint32_t)(at + 2);
        s = NULL;
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_construct(&c, &s),
                              (int)MOQ_OK);
        if (s) moq_media_sender_test_free(s);
    }
    /* Every sized initializer leaves the default. */
    {
        moq_media_sender_cfg_t c;
        memset(&c, 0xA5, sizeof(c));
        moq_media_sender_cfg_init_sized(&c, sizeof(c));
        MOQ_TEST_CHECK_EQ_INT((int)c.catalog_revision, (int)MOQ_MSF_REVISION_01);
        memset(&c, 0xA5, sizeof(c));
        moq_media_sender_cfg_init_lossless_sized(&c, sizeof(c));
        MOQ_TEST_CHECK_EQ_INT((int)c.catalog_revision, (int)MOQ_MSF_REVISION_01);
    }
    if (failures == before) printf("PASS: msf02.cfg_old_size\n");
}

int main(void)
{
    test_default_bytes_unchanged();
    test_r02_catalog();
    test_r02_delta_generated_at();
    test_r02_sap_key();
    test_r02_required_fields();
    test_r02_track_naming();
    test_r02_timeline_follows_media();
    test_supplement_cfg();
    test_cfg_old_size();
    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
