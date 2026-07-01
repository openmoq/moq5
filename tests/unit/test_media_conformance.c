/*
 * Cross-format media conformance vectors, driven by the shared harness
 * (tests/support/media_conformance.{h,c}). Each format registers an ops table
 * + a vector of cases; the driver runs the round-trip discipline and returns
 * its failure count, which we fold into `failures`.
 *
 * Shapes exercised:
 *   - MSF/CMSF  catalog round-trip (parse -> encode -> reparse -> semantic eq)
 *   - LOC       typed round-trip (encode -> reparse -> field eq) + malformed
 *   - CMAF      object validation (bytes -> report; report-field asserts)
 *
 * m2ts is intentionally NOT registered yet: the MSF model does not store the
 * m2ts* fields, so a catalog round-trip would silently drop them. When the TS
 * production slice lands it implements catalog ops (after moq_msf_track_t gains
 * the m2ts* fields) and an object validator mirroring moq_cmaf_validate_object's
 * pure (bytes -> report) shape, then adds vectors here.
 */
#include "../support/media_conformance.h"
#include "../support/media_builders.h"
#include "test_support.h"

#include <moq/types.h>
#include <moq/rcbuf.h>
#include <moq/msf.h>
#include <moq/loc.h>
#include <moq/cmaf.h>

#include <stdint.h>
#include <string.h>

static int failures = 0;   /* MOQ_TEST_PASS reads this; run() returns are folded in */

static bool beq(moq_bytes_t a, moq_bytes_t b)
{
    if (a.len != b.len) return false;
    if (a.len == 0) return true;
    return a.data && b.data && memcmp(a.data, b.data, a.len) == 0;
}

/* ===================================================================== *
 * MSF / CMSF catalog round-trip
 * ===================================================================== */

static moq_result_t msf_parse(const moq_alloc_t *al, moq_bytes_t json,
                              void **model)
{
    moq_msf_catalog_t *cat = (moq_msf_catalog_t *)al->alloc(sizeof(*cat),
                                                            al->ctx);
    if (!cat) return MOQ_ERR_NOMEM;
    moq_result_t rc = moq_msf_catalog_parse(al, json, cat);
    if (rc != MOQ_OK) { al->free(cat, sizeof(*cat), al->ctx); return rc; }
    *model = cat;
    return MOQ_OK;
}

static moq_result_t msf_encode(const moq_alloc_t *al, const void *m,
                               moq_rcbuf_t **out)
{
    return moq_msf_catalog_encode(al, (const moq_msf_catalog_t *)m, out);
}

static void msf_free(const moq_alloc_t *al, void *m)
{
    moq_msf_catalog_cleanup(al, (moq_msf_catalog_t *)m);
    al->free(m, sizeof(moq_msf_catalog_t), al->ctx);
}

static bool url_equal(const moq_cmsf_url_t *a, const moq_cmsf_url_t *b)
{
    if (a->present != b->present) return false;
    if (!a->present) return true;
    if (!beq(a->url, b->url)) return false;
    if (a->has_type != b->has_type) return false;
    if (a->has_type && !beq(a->type, b->type)) return false;
    return true;
}

static bool cp_equal(const moq_cmsf_content_protection_t *a,
                     const moq_cmsf_content_protection_t *b)
{
    if (!beq(a->ref_id, b->ref_id)) return false;
    if (!beq(a->scheme, b->scheme)) return false;
    if (a->default_kid_count != b->default_kid_count) return false;
    for (size_t i = 0; i < a->default_kid_count; i++)
        if (!beq(a->default_kids[i], b->default_kids[i])) return false;
    const moq_cmsf_drm_system_t *da = &a->drm_system, *db = &b->drm_system;
    if (!beq(da->system_id, db->system_id)) return false;
    if (!url_equal(&da->la_url, &db->la_url)) return false;
    if (!url_equal(&da->cert_url, &db->cert_url)) return false;
    if (!url_equal(&da->auth_url, &db->auth_url)) return false;
    if (da->has_pssh != db->has_pssh || !beq(da->pssh, db->pssh)) return false;
    if (da->has_robustness != db->has_robustness ||
        !beq(da->robustness, db->robustness)) return false;
    return true;
}

static bool track_equal(const moq_msf_track_t *a, const moq_msf_track_t *b)
{
    if (!beq(a->name, b->name) || !beq(a->packaging, b->packaging)) return false;
    if (a->is_live != b->is_live) return false;
    if (a->has_namespace != b->has_namespace ||
        !beq(a->namespace_, b->namespace_)) return false;
    if (a->has_role != b->has_role || !beq(a->role, b->role)) return false;
    if (a->has_codec != b->has_codec || !beq(a->codec, b->codec)) return false;
    if (a->has_init_track != b->has_init_track ||
        !beq(a->init_track, b->init_track)) return false;
    if (a->has_render_group != b->has_render_group ||
        a->render_group != b->render_group) return false;
    if (a->has_alt_group != b->has_alt_group ||
        a->alt_group != b->alt_group) return false;
    if (a->has_target_latency != b->has_target_latency ||
        a->target_latency != b->target_latency) return false;
    if (a->has_track_duration != b->has_track_duration ||
        a->track_duration_ms != b->track_duration_ms) return false;
    if (a->has_label != b->has_label || !beq(a->label, b->label)) return false;
    if (a->has_event_type != b->has_event_type ||
        !beq(a->event_type, b->event_type)) return false;
    if (a->has_mime_type != b->has_mime_type ||
        !beq(a->mime_type, b->mime_type)) return false;
    if (a->has_width != b->has_width || a->width != b->width) return false;
    if (a->has_height != b->has_height || a->height != b->height) return false;
    if (a->has_samplerate != b->has_samplerate ||
        a->samplerate != b->samplerate) return false;
    if (a->has_bitrate != b->has_bitrate || a->bitrate != b->bitrate)
        return false;
    if (a->has_framerate != b->has_framerate ||
        a->framerate_millis != b->framerate_millis) return false;
    if (a->has_timescale != b->has_timescale || a->timescale != b->timescale)
        return false;
    if (a->has_channel_config != b->has_channel_config ||
        !beq(a->channel_config, b->channel_config)) return false;
    if (a->has_lang != b->has_lang || !beq(a->lang, b->lang)) return false;
    if (a->has_init_ref != b->has_init_ref || !beq(a->init_ref, b->init_ref))
        return false;
    if (a->has_init_data != b->has_init_data ||
        !beq(a->init_data, b->init_data)) return false;
    if (a->has_max_grp_sap != b->has_max_grp_sap ||
        a->max_grp_sap != b->max_grp_sap) return false;
    if (a->has_max_obj_sap != b->has_max_obj_sap ||
        a->max_obj_sap != b->max_obj_sap) return false;
    if (a->cp_ref_id_count != b->cp_ref_id_count) return false;
    for (size_t i = 0; i < a->cp_ref_id_count; i++)
        if (!beq(a->cp_ref_ids[i], b->cp_ref_ids[i])) return false;
    if (a->depends_count != b->depends_count) return false;
    for (size_t i = 0; i < a->depends_count; i++)
        if (!beq(a->depends[i], b->depends[i])) return false;
    if (a->has_template != b->has_template) return false;
    if (a->has_template &&
        (a->template_.start_media_ms != b->template_.start_media_ms ||
         a->template_.delta_media_ms != b->template_.delta_media_ms ||
         a->template_.start_group != b->template_.start_group ||
         a->template_.start_object != b->template_.start_object ||
         a->template_.delta_group != b->template_.delta_group ||
         a->template_.delta_object != b->template_.delta_object ||
         a->template_.start_wallclock_ms != b->template_.start_wallclock_ms ||
         a->template_.delta_wallclock_ms != b->template_.delta_wallclock_ms))
        return false;
    return true;
}

/* Curated semantic compare of the MSF / MSF-01 / CMSF surface (the fields the
 * encoder emits). Non-emitted parser-only fields are intentionally not
 * compared; the harness vectors only use round-trip-safe fields. */
static bool msf_equal(const void *pa, const void *pb)
{
    const moq_msf_catalog_t *a = (const moq_msf_catalog_t *)pa;
    const moq_msf_catalog_t *b = (const moq_msf_catalog_t *)pb;
    if (a->version != b->version) return false;
    if (a->has_generated_at != b->has_generated_at ||
        a->generated_at != b->generated_at) return false;
    if (a->is_complete != b->is_complete) return false;

    if (a->track_count != b->track_count) return false;
    for (size_t i = 0; i < a->track_count; i++)
        if (!track_equal(&a->tracks[i], &b->tracks[i])) return false;

    if (a->init_data_count != b->init_data_count) return false;
    for (size_t i = 0; i < a->init_data_count; i++) {
        if (!beq(a->init_data_list[i].id, b->init_data_list[i].id)) return false;
        if (!beq(a->init_data_list[i].type, b->init_data_list[i].type))
            return false;
        if (!beq(a->init_data_list[i].data, b->init_data_list[i].data))
            return false;
    }

    if (a->content_protection_count != b->content_protection_count) return false;
    for (size_t i = 0; i < a->content_protection_count; i++)
        if (!cp_equal(&a->content_protections[i], &b->content_protections[i]))
            return false;
    return true;
}

static const moq_test_media_format_ops_t MSF_OPS = {
    .name = "msf",
    .catalog_parse = msf_parse,
    .catalog_encode = msf_encode,
    .catalog_equal = msf_equal,
    .catalog_free = msf_free,
};

#define JSON(s) { (const uint8_t *)(s), sizeof(s) - 1 }

static const char k_min[] =
    "{\"version\":1,\"tracks\":[{\"name\":\"video\",\"packaging\":\"loc\","
    "\"isLive\":true}]}";

static const char k_av[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"v\",\"namespace\":\"ns/cam1\",\"packaging\":\"loc\","
    "\"isLive\":true,\"role\":\"video\",\"codec\":\"av01\",\"width\":1920,"
    "\"height\":1080,\"framerate\":30,\"bitrate\":1500000,\"timescale\":90000,"
    "\"targetLatency\":2000,\"renderGroup\":1,\"altGroup\":2,\"label\":\"Main\"},"
    "{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"audio\","
    "\"codec\":\"opus\",\"samplerate\":48000,\"channelConfig\":\"2\","
    "\"lang\":\"en\",\"bitrate\":32000,\"renderGroup\":1}]}";

/* CMSF: cmaf track with initRef -> initDataList, contentProtections (cenc) +
 * contentProtectionRefIDs, max SAP starting types; version as the string "1". */
static const char k_cmsf[] =
    "{\"version\":\"1\",\"contentProtections\":[{\"refID\":\"cp1\","
    "\"defaultKID\":[\"01234567-89ab-cdef-0123-456789abcdef\"],"
    "\"scheme\":\"cenc\",\"drmSystem\":{"
    "\"systemID\":\"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed\","
    "\"laURL\":{\"url\":\"https://la.example/x\",\"type\":\"application/json\"},"
    "\"certURL\":{\"url\":\"https://cert.example/c\"},"
    "\"authURL\":{\"url\":\"https://auth.example/a\"},"
    "\"pssh\":\"AAAB\",\"robustness\":\"SW_SECURE_CRYPTO\"}}],"
    "\"tracks\":[{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.640028\",\"width\":1280,"
    "\"height\":720,\"initRef\":\"i1\",\"renderGroup\":1,\"altGroup\":2,"
    "\"label\":\"HD\",\"contentProtectionRefIDs\":[\"cp1\"],"
    "\"maxGrpSapStartingType\":1,\"maxObjSapStartingType\":2}],"
    "\"initDataList\":[{\"id\":\"i1\",\"type\":\"inline\",\"data\":\"AAAB\"}]}";

/* MSF §7: a media track carrying an inline template (5.2.15) plus an explicit
 * mediatimeline track (§7.2: mimeType application/json + non-empty depends). */
static const char k_mtl[] =
    "{\"version\":\"1\",\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\","
    "\"template\":[0,2002,[0,0],[1,0],1759924158381,2002]},"
    "{\"name\":\"history\",\"packaging\":\"mediatimeline\",\"isLive\":true,"
    "\"mimeType\":\"application/json\",\"depends\":[\"video\"]}]}";

/* MSF §5.6.7 VOD: non-live tracks with trackDuration. §5.6.13 termination:
 * isComplete + empty tracks. */
static const char k_vod[] =
    "{\"version\":\"1\",\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"trackDuration\":8072340,\"codec\":\"av01\",\"width\":1920,"
    "\"height\":1080},"
    "{\"name\":\"audio\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"trackDuration\":8072340,\"codec\":\"opus\",\"samplerate\":48000}]}";

static const char k_term[] =
    "{\"version\":\"1\",\"generatedAt\":1746104606044,\"isComplete\":true,"
    "\"tracks\":[]}";

static const moq_test_media_case_t MSF_CASES[] = {
    { "msf-minimal", MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON(k_min), NULL, true, NULL },
    { "msf-av",      MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON(k_av),  NULL, true, NULL },
    { "cmsf-cmaf",   MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON(k_cmsf),NULL, true, NULL },
    { "msf-mediatimeline", MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON(k_mtl), NULL, true, NULL },
    { "msf-vod",     MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON(k_vod), NULL, true, NULL },
    { "msf-complete",MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON(k_term),NULL, true, NULL },
    { "msf-bad-json",     MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON("{ not json"),    NULL, false, NULL },
    { "msf-truncated",    MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON("{\"version\":1,\"tracks\":"), NULL, false, NULL },
    { "msf-no-version",   MOQ_TEST_MEDIA_CATALOG_ROUNDTRIP, JSON("{\"tracks\":[]}"), NULL, false, NULL },
};

/* ===================================================================== *
 * LOC typed round-trip
 * ===================================================================== */

static moq_result_t loc_encode(const moq_alloc_t *al, const void *m,
                               moq_rcbuf_t **out)
{
    return moq_loc_encode(al, MOQ_LOC_PROFILE_01,
                          (const moq_loc_headers_t *)m, out);
}

static bool loc_reparse_equal(moq_bytes_t bytes, const void *m)
{
    const moq_loc_headers_t *want = (const moq_loc_headers_t *)m;
    moq_loc_headers_t got;
    moq_loc_headers_init(&got);
    if (moq_loc_parse(MOQ_LOC_PROFILE_01, bytes, &got) != MOQ_OK) return false;

    if (got.has_timestamp != want->has_timestamp ||
        got.timestamp != want->timestamp) return false;
    if (got.has_video_frame_marking != want->has_video_frame_marking)
        return false;
    if (want->has_video_frame_marking) {
        const moq_loc_video_frame_marking_t *g = &got.video_frame_marking;
        const moq_loc_video_frame_marking_t *w = &want->video_frame_marking;
        if (g->independent != w->independent ||
            g->start_of_frame != w->start_of_frame ||
            g->end_of_frame != w->end_of_frame ||
            g->discardable != w->discardable ||
            g->base_layer_sync != w->base_layer_sync ||
            g->temporal_id != w->temporal_id ||
            g->has_layer_id != w->has_layer_id ||
            (w->has_layer_id && g->layer_id != w->layer_id))
            return false;
    }
    if (got.has_audio_level != want->has_audio_level) return false;
    if (want->has_audio_level &&
        (got.audio_level.voice_activity != want->audio_level.voice_activity ||
         got.audio_level.level != want->audio_level.level))
        return false;
    if (got.has_video_config != want->has_video_config ||
        !beq(got.video_config, want->video_config))
        return false;
    return true;
}

static moq_result_t loc_parse_only(moq_bytes_t bytes)
{
    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    return moq_loc_parse(MOQ_LOC_PROFILE_01, bytes, &h);
}

static const moq_test_media_format_ops_t LOC_OPS = {
    .name = "loc",
    .typed_encode = loc_encode,
    .typed_reparse_equal = loc_reparse_equal,
    .typed_parse_only = loc_parse_only,
};

/* A leading QUIC varint whose 2-bit length prefix (0b11) promises 8 bytes but
 * only 1 is present -> truncated -> moq_loc_parse rejects (malformed). */
static const uint8_t k_loc_malformed[] = { 0xC0 };

/* ===================================================================== *
 * CMAF object validation
 * ===================================================================== */

typedef struct {
    moq_cmaf_validity_t reason;       /* expected report reason */
    moq_sap_type_t      sap;          /* checked only when reason == OK */
    uint32_t            track_id;     /* checked only when reason == OK */
    uint32_t            chunk_count;  /* checked only when reason == OK */
    bool                use_init;     /* validate against an init with track_id */
    uint32_t            init_track_id;
} cmaf_expect_t;

static bool cmaf_validate(moq_bytes_t bytes, const moq_test_media_case_t *c,
                          bool *reported_valid)
{
    const cmaf_expect_t *e = (const cmaf_expect_t *)c->expect;
    moq_cmaf_init_info_t init;
    moq_cmaf_init_info_t *ip = NULL;
    if (e->use_init) {
        moq_cmaf_init_info_init(&init);
        init.track_id = e->init_track_id;
        ip = &init;
    }
    moq_cmaf_object_report_t rep;
    moq_cmaf_object_report_init(&rep);
    moq_result_t rc = moq_cmaf_validate_object(ip, bytes, &rep);

    *reported_valid = rep.valid;
    bool ok = (rep.reason == e->reason) && ((rc == MOQ_OK) == rep.valid);
    if (e->reason == MOQ_CMAF_OK) {       /* best-effort fields: assert only on valid */
        ok = ok && rep.sap_type == e->sap &&
             rep.track_id == e->track_id && rep.chunk_count == e->chunk_count;
    }
    return ok;
}

static const moq_test_media_format_ops_t CMAF_OPS = {
    .name = "cmaf",
    .object_validate = cmaf_validate,
};

int main(void)
{
    /* MSF / CMSF catalog round-trip + malformed negatives. */
    failures += moq_test_run_media_cases(&MSF_OPS, MSF_CASES,
                                         sizeof(MSF_CASES) / sizeof(MSF_CASES[0]));

    /* LOC typed round-trip. Models authored here; bytes produced by the encoder. */
    moq_loc_headers_t loc_ts;
    moq_loc_headers_init(&loc_ts);
    loc_ts.has_timestamp = true;
    loc_ts.timestamp = 123456789ull;

    moq_loc_headers_t loc_kf;
    moq_loc_headers_init(&loc_kf);
    loc_kf.has_timestamp = true;
    loc_kf.timestamp = 90000ull;
    loc_kf.has_video_frame_marking = true;
    loc_kf.video_frame_marking.independent = true;
    loc_kf.video_frame_marking.start_of_frame = true;
    loc_kf.video_frame_marking.end_of_frame = true;

    moq_loc_headers_t loc_au;
    moq_loc_headers_init(&loc_au);
    loc_au.has_timestamp = true;
    loc_au.timestamp = 42ull;
    loc_au.has_audio_level = true;
    loc_au.audio_level.voice_activity = true;
    loc_au.audio_level.level = 30;

    static const uint8_t vcfg[] = { 0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1 };
    moq_loc_headers_t loc_vc;
    moq_loc_headers_init(&loc_vc);
    loc_vc.has_timestamp = true;
    loc_vc.timestamp = 7ull;
    loc_vc.has_video_config = true;
    loc_vc.video_config = (moq_bytes_t){ vcfg, sizeof(vcfg) };

    const moq_test_media_case_t loc_cases[] = {
        { "loc-timestamp",    MOQ_TEST_MEDIA_TYPED_ROUNDTRIP, {NULL,0}, &loc_ts, true, NULL },
        { "loc-keyframe",     MOQ_TEST_MEDIA_TYPED_ROUNDTRIP, {NULL,0}, &loc_kf, true, NULL },
        { "loc-audio",        MOQ_TEST_MEDIA_TYPED_ROUNDTRIP, {NULL,0}, &loc_au, true, NULL },
        { "loc-video-config", MOQ_TEST_MEDIA_TYPED_ROUNDTRIP, {NULL,0}, &loc_vc, true, NULL },
        { "loc-malformed",    MOQ_TEST_MEDIA_TYPED_ROUNDTRIP,
          { k_loc_malformed, sizeof(k_loc_malformed) }, NULL, false, NULL },
    };
    failures += moq_test_run_media_cases(&LOC_OPS, loc_cases,
                                         sizeof(loc_cases) / sizeof(loc_cases[0]));

    /* CMAF object validation. Bytes built deterministically; reports asserted. */
    static uint8_t buf_sync[256], buf_nonsync[256], buf_nomfhd[256];
    static uint8_t buf_multi[256], buf_nosamp[256], buf_mismatch[256];
    size_t n_sync     = moq_test_build_cmaf_chunk(buf_sync,     7, 0x02000000, true,  false, 0xAA);
    size_t n_nonsync  = moq_test_build_cmaf_chunk(buf_nonsync,  7, 0x01010000, true,  false, 0xBB);
    size_t n_nomfhd   = moq_test_build_cmaf_chunk(buf_nomfhd,   7, 0x02000000, false, false, 0xCC);
    size_t n_multi    = moq_test_build_cmaf_chunk(buf_multi,    7, 0x02000000, true,  true,  0xDD);
    size_t n_nosamp   = moq_test_build_cmaf_chunk_no_samples(buf_nosamp, 7, 0xEE);
    size_t n_mismatch = moq_test_build_cmaf_chunk(buf_mismatch, 7, 0x02000000, true,  false, 0xFF);

    static const cmaf_expect_t exp_sync     = { MOQ_CMAF_OK, MOQ_SAP_UNKNOWN, 7, 1, false, 0 };
    static const cmaf_expect_t exp_nonsync  = { MOQ_CMAF_OK, MOQ_SAP_NONE,    7, 1, false, 0 };
    static const cmaf_expect_t exp_nomfhd   = { MOQ_CMAF_ERR_MISSING_MFHD,   0, 0, 0, false, 0 };
    static const cmaf_expect_t exp_multi    = { MOQ_CMAF_ERR_MULTI_TRACK,    0, 0, 0, false, 0 };
    static const cmaf_expect_t exp_nosamp   = { MOQ_CMAF_ERR_NO_SAMPLES,     0, 0, 0, false, 0 };
    static const cmaf_expect_t exp_mismatch = { MOQ_CMAF_ERR_TRACK_ID_MISMATCH, 0, 0, 0, true, 9 };

    const moq_test_media_case_t cmaf_cases[] = {
        { "cmaf-sync",     MOQ_TEST_MEDIA_OBJECT_VALIDATE, { buf_sync, n_sync },         NULL, true,  &exp_sync },
        { "cmaf-nonsync",  MOQ_TEST_MEDIA_OBJECT_VALIDATE, { buf_nonsync, n_nonsync },   NULL, true,  &exp_nonsync },
        { "cmaf-no-mfhd",  MOQ_TEST_MEDIA_OBJECT_VALIDATE, { buf_nomfhd, n_nomfhd },     NULL, false, &exp_nomfhd },
        { "cmaf-multi",    MOQ_TEST_MEDIA_OBJECT_VALIDATE, { buf_multi, n_multi },       NULL, false, &exp_multi },
        { "cmaf-nosamples",MOQ_TEST_MEDIA_OBJECT_VALIDATE, { buf_nosamp, n_nosamp },     NULL, false, &exp_nosamp },
        { "cmaf-mismatch", MOQ_TEST_MEDIA_OBJECT_VALIDATE, { buf_mismatch, n_mismatch }, NULL, false, &exp_mismatch },
    };
    failures += moq_test_run_media_cases(&CMAF_OPS, cmaf_cases,
                                         sizeof(cmaf_cases) / sizeof(cmaf_cases[0]));

    MOQ_TEST_PASS("media_conformance");
    return failures != 0;
}
