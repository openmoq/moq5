/*
 * Seeded MSF/CMSF catalog property test (generate-only). See media_fuzz.h.
 *
 * Generates VALID catalog models covering the big MSF/CMSF surface --
 * initDataList/initRef (shared entries), eventtimeline depends/mimeType/
 * eventType, contentProtections (refID/defaultKID/scheme/drmSystem +
 * optional URLs/pssh/robustness), per-track contentProtectionRefIDs, and the
 * maxGrp/maxObj SAP fields -- then exercises both sides:
 *
 *   encode(model) -> json1 -> parse(json1) -> parsed
 *
 * Two oracles. PRIMARY: msf_equal(model, parsed) -- every generated/emitted
 * field survived encode->parse. This is the semantic round-trip; byte equality
 * alone cannot prove it, since a field the encoder silently dropped would be
 * absent from both sides. SECONDARY: encode(parsed) -> json2, assert
 * json1 == json2 -- canonical stability (a faithful parse re-encodes to
 * identical bytes). A generated "valid" the encoder rejects is a generator bug,
 * reported with the seed+step.
 *
 * Ownership: string SPANS borrow from static literal pools or from per-case
 * stack buffers (names / id strings) that outlive the encode; only the ARRAYS
 * (tracks, per-track depends[]/cp_ref_ids[], initDataList[], contentProtections[]
 * + per-cp default_kids[]) are heap-allocated via the run's counting allocator
 * and freed here after the oracle. The PARSED catalog borrows from its own DOM
 * and is released by moq_msf_catalog_cleanup -- a separate path.
 */

#include "media_fuzz.h"

#include <moq/types.h>
#include <moq/rcbuf.h>
#include <moq/msf.h>

#include "../unit/test_support.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MF_MAX_TRACKS 5
#define MF_MAX_IDL    4
#define MF_MAX_CP     3
#define MF_MAX_DEP    3
#define MF_MAX_CPREF  3
#define MF_MAX_KID    2
#define MF_IDLEN      16

#define NPOOL(a)   (sizeof(a) / sizeof((a)[0]))
#define LITSPAN(s) ((moq_bytes_t){ (const uint8_t *)(s), strlen(s) })

static const char *const POOL_PACKAGING[] = { "loc", "cmaf" };
static const char *const POOL_CODEC[] = {
    "avc1.640028", "hev1.1.6.L93.B0", "av01.0.04M.08", "mp4a.40.2", "opus" };
static const char *const POOL_ROLE[]  = { "video", "audio", "subtitle" };
static const char *const POOL_LANG[]  = { "en", "fr", "de", "es" };
static const char *const POOL_LABEL[] = { "Main", "Alt", "Low", "High" };
static const char *const POOL_NS[]    = { "ns/a", "ns/b", "stream/main" };
static const char *const POOL_EVENT[] = {
    "org.ietf.moq.cmsf.sap", "org.example.scte35" };
static const char *const POOL_SCHEME[] = { "cenc", "cbcs" };
static const char *const POOL_SYSID[]  = {
    "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed",
    "9a04f079-9840-4286-ab92-e65be0885f95" };
/* CMSF §4.1.1.2: defaultKID values are UUID strings -- the public encoder
 * (which this generator feeds and treats rejection as a generator bug) now
 * enforces that syntax. */
static const char *const POOL_KID[]   = {
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222",
    "33333333-3333-3333-3333-333333333333" };
static const char *const POOL_URL[]   = {
    "https://drm.example/la", "https://drm.example/cert",
    "https://auth.example/t" };
static const char *const POOL_URLTYPE[] = { "EME-1.0", "PlayReady" };
static const char *const POOL_PSSH[]  = { "AAAANHBzc2gAAAAA", "AAAAQHBzc2gBAAAA" };
static const char *const POOL_ROB[]   = { "SW_SECURE_CRYPTO", "HW_SECURE_ALL" };
static const char *const POOL_INITB64[] = { "AAAB", "AAAC", "AAAD", "AAAE" };

static moq_bytes_t pick(moq_fuzz_rng_t *r, const char *const *pool, size_t n)
{
    /* Evaluate the random index ONCE: LITSPAN double-evaluates its argument
     * (pointer + strlen), so passing an rng-laden index would draw twice and
     * mismatch the pointer against a different entry's length. */
    const char *s = pool[moq_fuzz_rng_below(r, n)];
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

static moq_bytes_t buf_span(const char *s)
{
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

static void gen_url(moq_fuzz_rng_t *rng, moq_cmsf_url_t *u)
{
    u->present = true;
    u->url = pick(rng, POOL_URL, NPOOL(POOL_URL));
    if (moq_fuzz_rng_bool(rng)) {
        u->has_type = true;
        u->type = pick(rng, POOL_URLTYPE, NPOOL(POOL_URLTYPE));
    }
}

/* -- Generated-model -> parsed-model semantic comparator ------------------ *
 * Proves every generated/emitted field survived encode->parse, which byte
 * equality alone does NOT (a field the encoder dropped is absent from both
 * json1 and json2). Covers the full surface the generator exercises. */

static bool b_eq(moq_bytes_t a, moq_bytes_t b)
{
    if (a.len != b.len) return false;
    if (a.len == 0) return true;
    return a.data && b.data && memcmp(a.data, b.data, a.len) == 0;
}
static bool opt_b_eq(bool ha, moq_bytes_t a, bool hb, moq_bytes_t b)
{
    if (ha != hb) return false;
    return !ha || b_eq(a, b);
}
static bool url_eq(const moq_cmsf_url_t *a, const moq_cmsf_url_t *b)
{
    if (a->present != b->present) return false;
    if (!a->present) return true;
    if (!b_eq(a->url, b->url)) return false;
    if (a->has_type != b->has_type) return false;
    return !a->has_type || b_eq(a->type, b->type);
}

static bool track_eq(const moq_msf_track_t *a, const moq_msf_track_t *b)
{
    if (!b_eq(a->name, b->name) || !b_eq(a->packaging, b->packaging) ||
        a->is_live != b->is_live) return false;
    if (!opt_b_eq(a->has_namespace, a->namespace_, b->has_namespace, b->namespace_)) return false;
    if (!opt_b_eq(a->has_role, a->role, b->has_role, b->role)) return false;
    if (!opt_b_eq(a->has_codec, a->codec, b->has_codec, b->codec)) return false;
    if (!opt_b_eq(a->has_init_data, a->init_data, b->has_init_data, b->init_data)) return false;
    if (!opt_b_eq(a->has_init_track, a->init_track, b->has_init_track, b->init_track)) return false;
    if (a->has_width != b->has_width || (a->has_width && a->width != b->width)) return false;
    if (a->has_height != b->has_height || (a->has_height && a->height != b->height)) return false;
    if (a->has_samplerate != b->has_samplerate || (a->has_samplerate && a->samplerate != b->samplerate)) return false;
    if (a->has_bitrate != b->has_bitrate || (a->has_bitrate && a->bitrate != b->bitrate)) return false;
    if (a->has_framerate != b->has_framerate || (a->has_framerate && a->framerate_millis != b->framerate_millis)) return false;
    if (a->has_render_group != b->has_render_group || (a->has_render_group && a->render_group != b->render_group)) return false;
    if (a->has_alt_group != b->has_alt_group || (a->has_alt_group && a->alt_group != b->alt_group)) return false;
    if (a->has_timescale != b->has_timescale || (a->has_timescale && a->timescale != b->timescale)) return false;
    if (a->has_target_latency != b->has_target_latency || (a->has_target_latency && a->target_latency != b->target_latency)) return false;
    if (a->has_track_duration != b->has_track_duration || (a->has_track_duration && a->track_duration_ms != b->track_duration_ms)) return false;
    if (!opt_b_eq(a->has_channel_config, a->channel_config, b->has_channel_config, b->channel_config)) return false;
    if (!opt_b_eq(a->has_lang, a->lang, b->has_lang, b->lang)) return false;
    if (!opt_b_eq(a->has_label, a->label, b->has_label, b->label)) return false;
    if (!opt_b_eq(a->has_init_ref, a->init_ref, b->has_init_ref, b->init_ref)) return false;
    if (!opt_b_eq(a->has_event_type, a->event_type, b->has_event_type, b->event_type)) return false;
    if (!opt_b_eq(a->has_mime_type, a->mime_type, b->has_mime_type, b->mime_type)) return false;
    if (a->has_max_grp_sap != b->has_max_grp_sap || (a->has_max_grp_sap && a->max_grp_sap != b->max_grp_sap)) return false;
    if (a->has_max_obj_sap != b->has_max_obj_sap || (a->has_max_obj_sap && a->max_obj_sap != b->max_obj_sap)) return false;
    if (a->depends_count != b->depends_count) return false;
    for (size_t i = 0; i < a->depends_count; i++)
        if (!b_eq(a->depends[i], b->depends[i])) return false;
    if (a->cp_ref_id_count != b->cp_ref_id_count) return false;
    for (size_t i = 0; i < a->cp_ref_id_count; i++)
        if (!b_eq(a->cp_ref_ids[i], b->cp_ref_ids[i])) return false;
    if (a->has_template != b->has_template) return false;
    if (a->has_template) {
        const moq_msf_media_template_t *x = &a->template_, *y = &b->template_;
        if (x->start_media_ms != y->start_media_ms ||
            x->delta_media_ms != y->delta_media_ms ||
            x->start_group != y->start_group ||
            x->start_object != y->start_object ||
            x->delta_group != y->delta_group ||
            x->delta_object != y->delta_object ||
            x->start_wallclock_ms != y->start_wallclock_ms ||
            x->delta_wallclock_ms != y->delta_wallclock_ms) return false;
    }
    return true;
}

static bool cp_eq(const moq_cmsf_content_protection_t *a,
                  const moq_cmsf_content_protection_t *b)
{
    if (!b_eq(a->ref_id, b->ref_id) || !b_eq(a->scheme, b->scheme)) return false;
    if (a->default_kid_count != b->default_kid_count) return false;
    for (size_t i = 0; i < a->default_kid_count; i++)
        if (!b_eq(a->default_kids[i], b->default_kids[i])) return false;
    const moq_cmsf_drm_system_t *da = &a->drm_system, *db = &b->drm_system;
    if (!b_eq(da->system_id, db->system_id)) return false;
    if (!url_eq(&da->la_url, &db->la_url)) return false;
    if (!url_eq(&da->cert_url, &db->cert_url)) return false;
    if (!url_eq(&da->auth_url, &db->auth_url)) return false;
    if (!opt_b_eq(da->has_pssh, da->pssh, db->has_pssh, db->pssh)) return false;
    if (!opt_b_eq(da->has_robustness, da->robustness, db->has_robustness, db->robustness)) return false;
    return true;
}

static bool msf_equal(const moq_msf_catalog_t *a, const moq_msf_catalog_t *b)
{
    if (a->version != b->version) return false;
    if (a->has_generated_at != b->has_generated_at) return false;
    if (a->has_generated_at && a->generated_at != b->generated_at) return false;
    if (a->is_complete != b->is_complete) return false;
    if (a->track_count != b->track_count) return false;
    for (size_t i = 0; i < a->track_count; i++)
        if (!track_eq(&a->tracks[i], &b->tracks[i])) return false;
    if (a->init_data_count != b->init_data_count) return false;
    for (size_t i = 0; i < a->init_data_count; i++) {
        const moq_msf_init_data_entry_t *x = &a->init_data_list[i];
        const moq_msf_init_data_entry_t *y = &b->init_data_list[i];
        if (!b_eq(x->id, y->id) || !b_eq(x->type, y->type) || !b_eq(x->data, y->data))
            return false;
    }
    if (a->content_protection_count != b->content_protection_count) return false;
    for (size_t i = 0; i < a->content_protection_count; i++)
        if (!cp_eq(&a->content_protections[i], &b->content_protections[i])) return false;
    return true;
}

/* -- Byte mutation ------------------------------------------------------- */

static bool find_sub(const uint8_t *hay, size_t hlen, const char *needle,
                     size_t *at)
{
    size_t nl = strlen(needle);
    if (hlen < nl) return false;
    for (size_t i = 0; i + nl <= hlen; i++)
        if (memcmp(hay + i, needle, nl) == 0) { *at = i; return true; }
    return false;
}

/* Soft oracle for a mutated catalog: never crash/leak; a reject is fine. If it
 * parses, the round trip must be coherent -- but because the MSF parser is
 * intentionally MORE LENIENT than the encoder (validation is encode-side), a
 * leniently-parsed mutant may not re-encode; that is acceptable, not a finding.
 * Only when it DOES re-encode must parse(encode(parsed)) equal parsed. */
static int msf_soft(const moq_alloc_t *alloc, const uint8_t *buf, size_t len)
{
    int failures = 0;
    moq_msf_catalog_t p1;
    if (moq_msf_catalog_parse(alloc, (moq_bytes_t){ buf, len }, &p1) != MOQ_OK)
        return 0;
    moq_rcbuf_t *j2 = NULL;
    if (moq_msf_catalog_encode(alloc, &p1, &j2) == MOQ_OK && j2) {
        moq_msf_catalog_t p2;
        moq_result_t r2 = moq_msf_catalog_parse(alloc,
            (moq_bytes_t){ moq_rcbuf_data(j2), moq_rcbuf_len(j2) }, &p2);
        MOQ_TEST_CHECK(r2 == MOQ_OK);
        if (r2 == MOQ_OK) {
            MOQ_TEST_CHECK(msf_equal(&p1, &p2));
            moq_msf_catalog_cleanup(alloc, &p2);
        }
    }
    if (j2) moq_rcbuf_decref(j2);
    moq_msf_catalog_cleanup(alloc, &p1);
    return failures;
}

/* Assert a malformed/wrong-shape document is rejected (parse fails). */
static int msf_reject(const moq_alloc_t *alloc, const uint8_t *buf, size_t len)
{
    int failures = 0;
    moq_msf_catalog_t p;
    moq_result_t rc = moq_msf_catalog_parse(alloc, (moq_bytes_t){ buf, len }, &p);
    MOQ_TEST_CHECK(rc != MOQ_OK);
    if (rc == MOQ_OK) moq_msf_catalog_cleanup(alloc, &p);
    return failures;
}

/* Assert a document parses (a wrong-type OPTIONAL field is leniently ignored)
 * and round-trips coherently (here the parsed model is valid so encode succeeds). */
static int msf_accept(const moq_alloc_t *alloc, const char *json)
{
    int failures = 0;
    moq_bytes_t b = { (const uint8_t *)json, strlen(json) };
    moq_msf_catalog_t p1;
    moq_result_t rc = moq_msf_catalog_parse(alloc, b, &p1);
    MOQ_TEST_CHECK(rc == MOQ_OK);
    if (rc != MOQ_OK) return failures;
    moq_rcbuf_t *j2 = NULL;
    moq_result_t e = moq_msf_catalog_encode(alloc, &p1, &j2);
    MOQ_TEST_CHECK(e == MOQ_OK && j2 != NULL);
    if (e == MOQ_OK && j2) {
        moq_msf_catalog_t p2;
        moq_result_t r2 = moq_msf_catalog_parse(alloc,
            (moq_bytes_t){ moq_rcbuf_data(j2), moq_rcbuf_len(j2) }, &p2);
        MOQ_TEST_CHECK(r2 == MOQ_OK);
        if (r2 == MOQ_OK) {
            MOQ_TEST_CHECK(msf_equal(&p1, &p2));
            moq_msf_catalog_cleanup(alloc, &p2);
        }
    }
    if (j2) moq_rcbuf_decref(j2);
    moq_msf_catalog_cleanup(alloc, &p1);
    return failures;
}

/* Apply one structured mutation to a valid catalog's encoded JSON (json/jlen)
 * and assert the appropriate oracle. Scratch uses malloc (not the counting
 * allocator). */
static int msf_mutate(const moq_alloc_t *alloc, moq_fuzz_rng_t *rng,
                      uint64_t seed, int step, const uint8_t *json, size_t jlen)
{
    int failures = 0;
    int kind = (int)moq_fuzz_rng_below(rng, 9);
    const char *kname = "?";

    if (kind == 0 || kind == 1 || kind == 2 || kind == 3) {
        uint8_t *buf = (uint8_t *)malloc(jlen ? jlen : 1);
        if (!buf) { MOQ_TEST_CHECK(false); return failures; }  /* never false-green */
        memcpy(buf, json, jlen);
        if (kind == 0) {                       /* truncate */
            kname = "truncate";
            size_t len = (size_t)moq_fuzz_rng_below(rng, jlen + 1);
            failures += msf_soft(alloc, buf, len);
        } else if (kind == 1) {                /* flip a byte */
            kname = "flip";
            moq_fuzz_flip_byte(buf, jlen, rng);
            failures += msf_soft(alloc, buf, jlen);
        } else {                               /* rename a required key -> reject */
            const char *needle = (kind == 2) ? "\"version\"" : "\"tracks\"";
            kname = (kind == 2) ? "rename-version" : "rename-tracks";
            size_t at;
            if (find_sub(buf, jlen, needle, &at)) {
                buf[at + 1] = 'X';             /* corrupt the key -> required missing */
                failures += msf_reject(alloc, buf, jlen);
            }
        }
        free(buf);
    } else if (kind == 4) {                    /* tracks non-array -> reject */
        kname = "tracks-nonarray";
        const char *j = "{\"version\":1,\"tracks\":123}";
        failures += msf_reject(alloc, (const uint8_t *)j, strlen(j));
    } else if (kind == 5) {                    /* contentProtections non-array */
        kname = "cp-nonarray";
        const char *j = "{\"version\":1,\"tracks\":[{\"name\":\"v\","
            "\"packaging\":\"loc\",\"isLive\":true}],\"contentProtections\":5}";
        failures += msf_reject(alloc, (const uint8_t *)j, strlen(j));
    } else if (kind == 6) {                    /* initDataList non-array */
        kname = "idl-nonarray";
        const char *j = "{\"version\":1,\"tracks\":[{\"name\":\"v\","
            "\"packaging\":\"loc\",\"isLive\":true}],\"initDataList\":\"x\"}";
        failures += msf_reject(alloc, (const uint8_t *)j, strlen(j));
    } else if (kind == 7) {                    /* eventtimeline depends non-array */
        kname = "depends-nonarray";
        const char *j = "{\"version\":1,\"tracks\":[{\"name\":\"e\","
            "\"packaging\":\"eventtimeline\",\"isLive\":true,\"eventType\":\"x\","
            "\"mimeType\":\"application/json\",\"depends\":5}]}";
        failures += msf_reject(alloc, (const uint8_t *)j, strlen(j));
    } else {                                   /* mimeType wrong type -> ignored */
        kname = "mimetype-wrongtype";
        failures += msf_accept(alloc, "{\"version\":1,\"tracks\":[{\"name\":\"v\","
            "\"packaging\":\"loc\",\"isLive\":true,\"mimeType\":123}]}");
    }

    if (failures)
        fprintf(stderr, "  (seed=0x%llx step=%d: msf mutate '%s')\n",
                (unsigned long long)seed, step, kname);
    return failures;
}

/* Run one MSF/CMSF catalog case. Returns the local failure count. */
static int msf_one_case(const moq_alloc_t *alloc, moq_fuzz_rng_t *rng,
                        uint64_t seed, int step, uint32_t mutate_permille)
{
    int failures = 0;

    /* Per-case stable id/name storage (spans point in here; lives through the
     * encode of the generated model). */
    char names[MF_MAX_TRACKS][MF_IDLEN];
    char idl_ids[MF_MAX_IDL][MF_IDLEN];
    char cp_ids[MF_MAX_CP][MF_IDLEN];

    size_t track_n = 1 + (size_t)moq_fuzz_rng_below(rng, MF_MAX_TRACKS);
    size_t idl_n   = (size_t)moq_fuzz_rng_below(rng, MF_MAX_IDL + 1);
    size_t cp_n    = (size_t)moq_fuzz_rng_below(rng, MF_MAX_CP + 1);

    /* %u (<=10 digits) keeps the formatted id within MF_IDLEN -- the counts are
     * tiny, but this bound is one the compiler can prove (-Wformat-truncation). */
    for (size_t i = 0; i < track_n; i++) snprintf(names[i], MF_IDLEN, "t%u", (unsigned)i);
    for (size_t i = 0; i < idl_n; i++)   snprintf(idl_ids[i], MF_IDLEN, "i%u", (unsigned)i);
    for (size_t i = 0; i < cp_n; i++)    snprintf(cp_ids[i], MF_IDLEN, "c%u", (unsigned)i);

    /* Decide track kinds up front (track 0 is always media so a timeline track
     * always has a real media track to depend on). */
    enum { TK_MEDIA = 0, TK_EVENT, TK_MEDIATL };
    int kind[MF_MAX_TRACKS] = { 0 };
    moq_bytes_t media_names[MF_MAX_TRACKS];
    size_t media_n = 0;
    for (size_t i = 0; i < track_n; i++) {
        if (i == 0) {
            kind[i] = TK_MEDIA;
        } else {
            uint64_t r = moq_fuzz_rng_below(rng, 6);
            kind[i] = (r == 0) ? TK_EVENT : (r == 1) ? TK_MEDIATL : TK_MEDIA;
        }
        if (kind[i] == TK_MEDIA) media_names[media_n++] = buf_span(names[i]);
    }

    /* -- Allocate the catalog's owned arrays (counting allocator) -------- */
    moq_msf_track_t *tracks = (moq_msf_track_t *)alloc->alloc(
        track_n * sizeof(moq_msf_track_t), alloc->ctx);
    moq_msf_init_data_entry_t *idl = idl_n
        ? (moq_msf_init_data_entry_t *)alloc->alloc(
              idl_n * sizeof(moq_msf_init_data_entry_t), alloc->ctx) : NULL;
    moq_cmsf_content_protection_t *cps = cp_n
        ? (moq_cmsf_content_protection_t *)alloc->alloc(
              cp_n * sizeof(moq_cmsf_content_protection_t), alloc->ctx) : NULL;
    if (!tracks || (idl_n && !idl) || (cp_n && !cps)) {
        MOQ_TEST_CHECK(false);   /* allocation failure in the harness itself */
        if (tracks) alloc->free(tracks, track_n * sizeof(*tracks), alloc->ctx);
        if (idl) alloc->free(idl, idl_n * sizeof(*idl), alloc->ctx);
        if (cps) alloc->free(cps, cp_n * sizeof(*cps), alloc->ctx);
        return failures;
    }
    memset(tracks, 0, track_n * sizeof(*tracks));
    if (idl) memset(idl, 0, idl_n * sizeof(*idl));
    if (cps) memset(cps, 0, cp_n * sizeof(*cps));

    /* -- initDataList ---------------------------------------------------- */
    for (size_t j = 0; j < idl_n; j++) {
        idl[j].id   = buf_span(idl_ids[j]);
        idl[j].type = LITSPAN("inline");
        idl[j].data = pick(rng, POOL_INITB64, NPOOL(POOL_INITB64));
    }

    /* -- contentProtections --------------------------------------------- */
    for (size_t k = 0; k < cp_n; k++) {
        moq_cmsf_content_protection_t *cp = &cps[k];
        cp->ref_id = buf_span(cp_ids[k]);
        cp->scheme = pick(rng, POOL_SCHEME, NPOOL(POOL_SCHEME));
        cp->drm_system.system_id = pick(rng, POOL_SYSID, NPOOL(POOL_SYSID));
        size_t kn = 1 + (size_t)moq_fuzz_rng_below(rng, MF_MAX_KID);
        cp->default_kids = (moq_bytes_t *)alloc->alloc(
            kn * sizeof(moq_bytes_t), alloc->ctx);
        if (!cp->default_kids) { MOQ_TEST_CHECK(false); cp->default_kid_count = 0; }
        else {
            cp->default_kid_count = kn;
            for (size_t i = 0; i < kn; i++)
                cp->default_kids[i] = pick(rng, POOL_KID, NPOOL(POOL_KID));
        }
        if (moq_fuzz_rng_bool(rng)) gen_url(rng, &cp->drm_system.la_url);
        if (moq_fuzz_rng_bool(rng)) gen_url(rng, &cp->drm_system.cert_url);
        if (moq_fuzz_rng_bool(rng)) gen_url(rng, &cp->drm_system.auth_url);
        if (moq_fuzz_rng_bool(rng)) {
            cp->drm_system.has_pssh = true;
            cp->drm_system.pssh = pick(rng, POOL_PSSH, NPOOL(POOL_PSSH));
        }
        if (moq_fuzz_rng_bool(rng)) {
            cp->drm_system.has_robustness = true;
            cp->drm_system.robustness = pick(rng, POOL_ROB, NPOOL(POOL_ROB));
        }
    }

    /* -- tracks --------------------------------------------------------- */
    for (size_t i = 0; i < track_n; i++) {
        moq_msf_track_t *t = &tracks[i];
        t->struct_size = (uint32_t)sizeof(*t);
        t->name = buf_span(names[i]);
        t->is_live = moq_fuzz_rng_bool(rng);

        if (kind[i] == TK_EVENT || kind[i] == TK_MEDIATL) {
            /* Timeline tracks (eventtimeline §8.2 / mediatimeline §7.2):
             * application/json mimeType + a non-empty depends list referencing
             * media tracks. eventtimeline additionally carries eventType;
             * mediatimeline MUST NOT. */
            if (kind[i] == TK_EVENT) {
                t->packaging = LITSPAN("eventtimeline");
                t->has_event_type = true;
                t->event_type = pick(rng, POOL_EVENT, NPOOL(POOL_EVENT));
            } else {
                t->packaging = LITSPAN("mediatimeline");
            }
            t->has_mime_type = true;
            t->mime_type = LITSPAN("application/json");
            size_t dn = 1 + (size_t)moq_fuzz_rng_below(rng, MF_MAX_DEP);
            if (dn > media_n) dn = media_n;
            t->depends = (moq_bytes_t *)alloc->alloc(
                dn * sizeof(moq_bytes_t), alloc->ctx);
            if (!t->depends) { MOQ_TEST_CHECK(false); t->depends_count = 0; }
            else {
                t->depends_count = dn;
                for (size_t d = 0; d < dn; d++)
                    t->depends[d] = media_names[moq_fuzz_rng_below(rng, media_n)];
            }
            continue;
        }

        /* Media track. */
        t->packaging = pick(rng, POOL_PACKAGING, NPOOL(POOL_PACKAGING));
        if (moq_fuzz_rng_bool(rng)) { t->has_namespace = true; t->namespace_ = pick(rng, POOL_NS, NPOOL(POOL_NS)); }
        if (moq_fuzz_rng_bool(rng)) { t->has_role = true; t->role = pick(rng, POOL_ROLE, NPOOL(POOL_ROLE)); }
        if (moq_fuzz_rng_bool(rng)) { t->has_codec = true; t->codec = pick(rng, POOL_CODEC, NPOOL(POOL_CODEC)); }
        /* init: inline base64 and/or an initRef into a shared initDataList id. */
        if (moq_fuzz_rng_bool(rng)) { t->has_init_data = true; t->init_data = pick(rng, POOL_INITB64, NPOOL(POOL_INITB64)); }
        if (idl_n && moq_fuzz_rng_bool(rng)) {
            t->has_init_ref = true;
            t->init_ref = buf_span(idl_ids[moq_fuzz_rng_below(rng, idl_n)]);
        }
        if (moq_fuzz_rng_bool(rng)) { t->has_width = true;  t->width  = (uint32_t)moq_fuzz_rng_below(rng, 7680); }
        if (moq_fuzz_rng_bool(rng)) { t->has_height = true; t->height = (uint32_t)moq_fuzz_rng_below(rng, 4320); }
        if (moq_fuzz_rng_bool(rng)) { t->has_samplerate = true; t->samplerate = (uint32_t)moq_fuzz_rng_below(rng, 192001); }
        if (moq_fuzz_rng_bool(rng)) { t->has_bitrate = true; t->bitrate = moq_fuzz_rng_below(rng, 50000001); }
        if (moq_fuzz_rng_bool(rng)) {
            /* integer fps * 1000 so the framerate decimal round-trips exactly */
            static const uint32_t fps[] = { 24, 25, 30, 48, 50, 60 };
            t->has_framerate = true;
            t->framerate_millis = (uint64_t)fps[moq_fuzz_rng_below(rng, NPOOL(fps))] * 1000u;
        }
        if (moq_fuzz_rng_bool(rng)) { t->has_render_group = true; t->render_group = (int)moq_fuzz_rng_below(rng, 16); }
        if (moq_fuzz_rng_bool(rng)) { t->has_alt_group = true;   t->alt_group   = (int)moq_fuzz_rng_below(rng, 16); }
        if (moq_fuzz_rng_bool(rng)) { t->has_timescale = true;   t->timescale   = moq_fuzz_rng_below(rng, 1000000) + 1; }
        if (moq_fuzz_rng_bool(rng)) { t->has_target_latency = true; t->target_latency = moq_fuzz_rng_below(rng, 10001); }
        /* trackDuration (5.2.35): valid only on a non-live track. */
        if (!t->is_live && moq_fuzz_rng_bool(rng)) {
            t->has_track_duration = true;
            t->track_duration_ms = moq_fuzz_rng_below(rng, 100000000ULL) + 1;
        }
        if (moq_fuzz_rng_bool(rng)) { t->has_channel_config = true; t->channel_config = LITSPAN("2"); }
        if (moq_fuzz_rng_bool(rng)) { t->has_lang = true;  t->lang  = pick(rng, POOL_LANG, NPOOL(POOL_LANG)); }
        if (moq_fuzz_rng_bool(rng)) { t->has_label = true; t->label = pick(rng, POOL_LABEL, NPOOL(POOL_LABEL)); }
        if (moq_fuzz_rng_bool(rng)) { t->has_mime_type = true; t->mime_type = LITSPAN("video/mp4"); }
        if (moq_fuzz_rng_bool(rng)) { t->has_max_grp_sap = true; t->max_grp_sap = (uint32_t)moq_fuzz_rng_below(rng, 4); }
        if (moq_fuzz_rng_bool(rng)) { t->has_max_obj_sap = true; t->max_obj_sap = (uint32_t)moq_fuzz_rng_below(rng, 4); }
        /* per-track contentProtectionRefIDs into existing refIDs. */
        if (cp_n && moq_fuzz_rng_bool(rng)) {
            size_t rn = 1 + (size_t)moq_fuzz_rng_below(rng, MF_MAX_CPREF);
            if (rn > cp_n) rn = cp_n;
            t->cp_ref_ids = (moq_bytes_t *)alloc->alloc(
                rn * sizeof(moq_bytes_t), alloc->ctx);
            if (!t->cp_ref_ids) { MOQ_TEST_CHECK(false); t->cp_ref_id_count = 0; }
            else {
                t->cp_ref_id_count = rn;
                for (size_t r = 0; r < rn; r++)
                    t->cp_ref_ids[r] = buf_span(cp_ids[moq_fuzz_rng_below(rng, cp_n)]);
            }
        }
        /* media timeline template (5.2.15 / §7.4): inline 6-tuple of
         * non-negative integers. */
        if (moq_fuzz_rng_bool(rng)) {
            t->has_template = true;
            t->template_.start_media_ms     = moq_fuzz_rng_below(rng, 100000);
            t->template_.delta_media_ms     = moq_fuzz_rng_below(rng, 10000);
            t->template_.start_group        = moq_fuzz_rng_below(rng, 1000);
            t->template_.start_object       = moq_fuzz_rng_below(rng, 100);
            t->template_.delta_group        = moq_fuzz_rng_below(rng, 10);
            t->template_.delta_object       = moq_fuzz_rng_below(rng, 10);
            t->template_.start_wallclock_ms = moq_fuzz_rng_below(rng, 2000000000000ULL);
            t->template_.delta_wallclock_ms = moq_fuzz_rng_below(rng, 10000);
        }
    }

    /* -- Assemble the catalog ------------------------------------------- */
    moq_msf_catalog_t cat;
    memset(&cat, 0, sizeof(cat));
    cat.struct_size = (uint32_t)sizeof(cat);
    cat.version = MOQ_MSF_VERSION;
    cat.tracks = tracks;
    cat.track_count = track_n;
    if (moq_fuzz_rng_bool(rng)) {
        cat.has_generated_at = true;
        cat.generated_at = moq_fuzz_rng_below(rng, 2000000000000ULL);
    }
    /* isComplete (5.1.3): only the TRUE state is emitted; encodable with any
     * tracks (the codec does not cross-check completion against per-track state). */
    if (moq_fuzz_rng_below(rng, 8) == 0) cat.is_complete = true;
    cat.init_data_list = idl;
    cat.init_data_count = idl_n;
    cat.content_protections = cps;
    cat.content_protection_count = cp_n;

    /* -- Oracle: encode -> parse -> semantic equality + canonical re-encode -- */
    moq_rcbuf_t *json1 = NULL;
    moq_result_t e1 = moq_msf_catalog_encode(alloc, &cat, &json1);
    MOQ_TEST_CHECK(e1 == MOQ_OK && json1 != NULL);  /* generated valid must encode */
    if (e1 != MOQ_OK || !json1) {
        fprintf(stderr, "  (seed=0x%llx step=%d: msf encode rc=%d, tracks=%zu "
                "idl=%zu cp=%zu)\n", (unsigned long long)seed, step, (int)e1,
                track_n, idl_n, cp_n);
        if (json1) moq_rcbuf_decref(json1);
    } else {
        moq_bytes_t b1 = { moq_rcbuf_data(json1), moq_rcbuf_len(json1) };
        moq_msf_catalog_t back;
        moq_result_t prc = moq_msf_catalog_parse(alloc, b1, &back);
        MOQ_TEST_CHECK(prc == MOQ_OK);
        if (prc == MOQ_OK) {
            MOQ_TEST_CHECK_EQ_SIZE(back.track_count, track_n);
            MOQ_TEST_CHECK_EQ_SIZE(back.init_data_count, idl_n);
            MOQ_TEST_CHECK_EQ_SIZE(back.content_protection_count, cp_n);

            /* Primary oracle: every generated/emitted field survived
             * encode->parse (catches an encoder that silently drops a field --
             * which byte equality below cannot, since it would be absent from
             * both json1 and json2). */
            MOQ_TEST_CHECK(msf_equal(&cat, &back));

            /* Secondary invariant: canonical stability -- a faithful parse
             * re-encodes to identical bytes. */
            moq_rcbuf_t *json2 = NULL;
            moq_result_t e2 = moq_msf_catalog_encode(alloc, &back, &json2);
            MOQ_TEST_CHECK(e2 == MOQ_OK && json2 != NULL);
            if (e2 == MOQ_OK && json2) {
                size_t n1 = moq_rcbuf_len(json1), n2 = moq_rcbuf_len(json2);
                MOQ_TEST_CHECK(n1 == n2 &&
                    (n1 == 0 || memcmp(moq_rcbuf_data(json1),
                                       moq_rcbuf_data(json2), n1) == 0));
                if (failures) {
                    fprintf(stderr, "  json1=%.*s\n  json2=%.*s\n",
                            (int)n1, (const char *)moq_rcbuf_data(json1),
                            (int)n2, (const char *)moq_rcbuf_data(json2));
                }
            }
            if (json2) moq_rcbuf_decref(json2);
            moq_msf_catalog_cleanup(alloc, &back);
        }
        /* Optional structured mutation of the just-encoded JSON. */
        if (moq_fuzz_hit_permille(rng, mutate_permille))
            failures += msf_mutate(alloc, rng, seed, step,
                                   moq_rcbuf_data(json1), moq_rcbuf_len(json1));
        moq_rcbuf_decref(json1);
    }

    /* -- Free the generated catalog's owned arrays ---------------------- */
    for (size_t i = 0; i < track_n; i++) {
        if (tracks[i].depends)
            alloc->free(tracks[i].depends,
                        tracks[i].depends_count * sizeof(moq_bytes_t), alloc->ctx);
        if (tracks[i].cp_ref_ids)
            alloc->free(tracks[i].cp_ref_ids,
                        tracks[i].cp_ref_id_count * sizeof(moq_bytes_t), alloc->ctx);
    }
    alloc->free(tracks, track_n * sizeof(*tracks), alloc->ctx);
    for (size_t k = 0; k < cp_n; k++) {
        if (cps[k].default_kids)
            alloc->free(cps[k].default_kids,
                        cps[k].default_kid_count * sizeof(moq_bytes_t), alloc->ctx);
    }
    if (cps) alloc->free(cps, cp_n * sizeof(*cps), alloc->ctx);
    if (idl) alloc->free(idl, idl_n * sizeof(*idl), alloc->ctx);

    if (failures)
        fprintf(stderr, "  (seed=0x%llx step=%d: msf case)\n",
                (unsigned long long)seed, step);
    return failures;
}

int moq_fuzz_msf_run_seed(uint64_t seed, int steps, uint32_t mutate_permille,
                          const char *argv0)
{
    int failures = 0;
    moq_fuzz_rng_t rng = { seed };
    for (int step = 0; step < steps; step++) {
        moq_fuzz_alloc_state_t as = { 0 };
        moq_alloc_t alloc;
        moq_fuzz_alloc_init(&alloc, &as);
        failures += msf_one_case(&alloc, &rng, seed, step, mutate_permille);
        if (as.balance != 0) {
            fprintf(stderr, "FAIL seed=0x%llx step=%d: msf alloc balance=%lld\n",
                    (unsigned long long)seed, step, (long long)as.balance);
            failures++;
        }
    }
    if (failures) moq_fuzz_print_replay(argv0, seed, steps, mutate_permille);
    return failures;
}
