/*
 * Track-description generations across the live->VOD conversion (MSF §11.3),
 * judged on the PUBLIC surfaces: the TRACK_ADDED event's desc pointer, the
 * TRACK_UPDATED event's desc pointer, and moq_media_track_desc_get().
 *
 * The contract under test (1530 R10): a description handed to the app is a
 * coherent, immutable snapshot of ONE catalog generation for the receiver's
 * lifetime. The conversion produces a NEW generation -- TRACK_UPDATED and a
 * fresh desc_get() return it, complete -- while the pointer the app took at
 * TRACK_ADDED keeps reading the OLD generation, unchanged. No observation may
 * combine is_live, has_track_duration and track_duration_ms from two
 * generations; every other field and span is identical across generations;
 * and the update cannot become visible before the new snapshot is complete.
 *
 * Two independently registered rows from one binary:
 *
 *   --mode interleaving   drain discovery first, then apply the conversion
 *                         with the receiver's test-only descriptor-update
 *                         observer reporting at PRE / MID / POST / PUBLISHED
 *                         on the reconciling thread (deterministic: the MID
 *                         observation is exactly what a concurrent reader
 *                         could see at that instant); the public queue is
 *                         inventoried at every phase;
 *   --mode delayed        ingest discovery AND the conversion before any
 *                         poll, then drain the complete public queue and
 *                         classify every event in order. This row judges the
 *                         queue design itself: an entry that only names a
 *                         track, with desc derived at poll time from the
 *                         track's current descriptor, cannot hand TRACK_ADDED
 *                         its own generation -- atomic writes alone would not
 *                         make this row pass.
 *
 * Event inventories are bounded, sticky and complete: every polled event is
 * recorded with its kind, and any unexpected kind, count or order fails.
 *
 * The comparator covers the ENTIRE active descriptor prefix, field by field:
 * every scalar and presence flag, every span by length and content, every
 * depends[] and content-protection-ref entry, every moq_media_track_info_t
 * field, every moq_cmaf_init_info_t field and nested span, and all eight
 * template values -- everything except the three sanctioned-mutable fields.
 * Its own discrimination is proved in-process on a copied image before it
 * judges the product. The fixture is an MSF-01/CMSF-01 catalog (version "1",
 * root contentProtections with UUID KID and system IDs, initialization data
 * referenced by initRef into the root initDataList) with five tracks, so that
 * every legally coexisting immutable class is non-empty on the handle whose
 * codec kind can carry it:
 *
 *   v    CENC-protected CMAF AVC video: a parsed init (codec kind,
 *        1280x720, a complete avcC with SPS and PPS as codec_config, track_id,
 *        "cenc" scheme, a non-zero per-sample IV size, the 16-byte KID),
 *        lang, label, width/height/framerate/bitrate, both maxSap fields and
 *        a content-protection reference;
 *   ac   CMAF AAC audio: a parsed init (samplerate, channel_count and the
 *        AudioSpecificConfig as codec_config -- the audio-exclusive nested
 *        fields), plus the catalog-level samplerate/channelConfig;
 *   a    LOC Opus audio carrying the inline media-timeline template
 *        (MSF-01 §5.2.15 / §7.4; no separate media timeline names it);
 *   sap  a CMSF SAP event timeline (CMSF §3.6.1 eventType
 *        org.ietf.moq.cmsf.sap, mimeType application/json, depends[]);
 *   mt   an explicit media timeline for v (mediatimeline packaging,
 *        mimeType, depends[]).
 *
 * init.width/height are visual-only and init.samplerate/channel_count are
 * audio-only, so each is judged live on the handle that can carry it. The
 * conversion catalog follows MSF-01 §11.3: for EACH track, isLive false and a
 * trackDuration, so all five handles convert and five TRACK_UPDATED events
 * are owed in declared order; the interleaving observer watches the video
 * handle's phases. A preflight proves both catalogs parse and pass the
 * library's strict authoring encode, resolve their initRefs, spell the
 * current version, carry no track-level init field, and that both decoder
 * configurations are structurally valid by the independent codec-signaling
 * validator.
 */

#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/codec_signaling.h>
#include "test_support.h"
#include "media_builders.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* -- seams (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING) ---------------- */
moq_media_receiver_t *moq_media_receiver_test_new_cfg_alloc(
    const moq_media_receiver_cfg_t *cfg, const moq_alloc_t *alloc);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json, size_t len);
void moq_media_receiver_test_set_desc_update_hook(
    void (*fn)(const moq_media_receiver_t *, const moq_media_track_t *, int, void *),
    void *ctx);
void moq_media_receiver_test_push_object(moq_media_receiver_t *r, moq_media_track_t *track,
                                         uint64_t group, uint64_t object, moq_object_status_t status,
                                         bool end_of_group, bool datagram,
                                         const uint8_t *payload, size_t len,
                                         const uint8_t *properties, size_t plen);
bool moq_media_receiver_test_is_fatal(const moq_media_receiver_t *r);
void moq_media_receiver_test_mark_desired(moq_media_receiver_t *r, moq_media_track_t *track);
void moq_media_receiver_test_set_transport_version(moq_media_receiver_t *r, moq_version_t v);
enum { DESC_PRE = 0, DESC_MID = 1, DESC_POST = 2, DESC_PUBLISHED = 3 };

/* -- counting allocator --------------------------------------------------- */
typedef struct { long live; long total; } astats_t;
static void *c_alloc(size_t n, void *ctx)
{ astats_t *s = ctx; void *p = malloc(n ? n : 1); if (p) { s->live++; s->total++; } return p; }
static void *c_realloc(void *p, size_t o, size_t n, void *ctx)
{ astats_t *s = ctx; (void)o; void *q = realloc(p, n ? n : 1); if (q && !p) { s->live++; s->total++; } return q; }
static void c_free(void *p, size_t n, void *ctx)
{ astats_t *s = ctx; (void)n; if (p) { free(p); s->live--; } }

/* -- fixture catalogs ------------------------------------------------------ */
#define VOD_DURATION_MS 8072340ull

/* Fixture identities, in declared catalog order. Every name is pinned by the
 * discovery inventory below. */
static const char *const k_track_names[] = { "v", "ac", "a", "sap", "mt" };
#define K_TRACKS 5
/* MSF-01 §11.3: converting a live broadcast to VOD publishes an independent
 * catalog which, FOR EACH TRACK, sets isLive false and adds trackDuration.
 * Every declared track therefore converts, timelines included, and one
 * TRACK_UPDATED per track is owed in declared order. */
static const bool k_converts[K_TRACKS] = { true, true, true, true, true };

/* Authoritative fixture values (checked-in CMSF fixtures elsewhere in the
 * tree use these UUIDs): the root defaultKID corresponds to the 16-byte tenc
 * KID; the drmSystem ID is the Widevine system UUID. */
static const uint8_t k_kid[16] = { 0x01,0x23,0x45,0x67,0x89,0xab,0xcd,0xef,0x01,0x23,0x45,0x67,0x89,0xab,0xcd,0xef };
#define K_KID_UUID   "01234567-89ab-cdef-0123-456789abcdef"
#define K_SYSTEM_UUID "edef8ba9-79d6-4ace-a3c8-27dcd51d21ed"
/* A structurally complete AVCDecoderConfigurationRecord: one SPS and one PPS
 * (the codec-signaling spec fixture's exact bytes). */
static const uint8_t k_avcc[] = {
    0x01, 0x42, 0x00, 0x1e, 0xff, 0xe1, 0x00, 0x06,
    0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
    0x01, 0x00, 0x04, 0x68, 0xce, 0x3c, 0x80,
};
/* AAC-LC AudioSpecificConfig: object type 2, 48 kHz (index 3), 2 channels. */
static const uint8_t k_asc[] = { 0x11, 0x90 };
#define K_IV_SIZE 8u
#define K_AUDIO_TRACK_ID 9u

static char g_live[8192], g_vod[8192];

/* A plain (unprotected) AAC CMAF init: ftyp + moov(trak(tkhd mdia(mdhd
 * minf(stbl(stsd(mp4a(esds))))))). The parser reads channel_count and
 * samplerate from the AudioSampleEntry and the AudioSpecificConfig from the
 * esds DecoderSpecificInfo (tag 0x05). */
static size_t build_aac_init(uint8_t *out, uint32_t track_id, uint32_t timescale,
                             uint16_t channels, uint16_t samplerate,
                             const uint8_t *asc, size_t asc_len)
{
    /* esds body: ver/flags(4) + ES_Descriptor(0x03, 3 bytes) +
     * DecoderConfigDescriptor(0x04, 13 bytes) + DecoderSpecificInfo(0x05, asc) */
    size_t dsi = 2 + asc_len;
    size_t dcd = 2 + 13 + dsi;
    size_t esd = 2 + 3 + dcd;
    size_t esds_box = 8 + 4 + esd;
    size_t mp4a_box = 8 + 28 + esds_box;
    size_t stsd_box = 8 + 8 + mp4a_box;
    size_t stbl_box = 8 + stsd_box;
    size_t minf_box = 8 + stbl_box;
    size_t mdhd_box = 28;
    size_t mdia_box = 8 + mdhd_box + minf_box;
    size_t tkhd_box = 24;
    size_t trak_box = 8 + tkhd_box + mdia_box;
    size_t moov_box = 8 + trak_box;
    size_t p = 0;
    p += moq_test_box_hdr(out + p, 20, "ftyp");
    memcpy(out + p, "iso6", 4); p += 4; memset(out + p, 0, 4); p += 4; memcpy(out + p, "iso6", 4); p += 4;
    p += moq_test_box_hdr(out + p, (uint32_t)moov_box, "moov");
    p += moq_test_box_hdr(out + p, (uint32_t)trak_box, "trak");
    p += moq_test_box_hdr(out + p, (uint32_t)tkhd_box, "tkhd");
    memset(out + p, 0, 12); p += 12;                                   /* ver/flags, creation, modification */
    out[p++] = (uint8_t)(track_id >> 24); out[p++] = (uint8_t)(track_id >> 16);
    out[p++] = (uint8_t)(track_id >> 8);  out[p++] = (uint8_t)track_id;
    p += moq_test_box_hdr(out + p, (uint32_t)mdia_box, "mdia");
    p += moq_test_box_hdr(out + p, (uint32_t)mdhd_box, "mdhd");
    memset(out + p, 0, 12); p += 12;                                   /* v0: ver/flags, creation, modification */
    out[p++] = (uint8_t)(timescale >> 24); out[p++] = (uint8_t)(timescale >> 16);
    out[p++] = (uint8_t)(timescale >> 8);  out[p++] = (uint8_t)timescale;
    memset(out + p, 0, 4); p += 4;                                     /* duration */
    p += moq_test_box_hdr(out + p, (uint32_t)minf_box, "minf");
    p += moq_test_box_hdr(out + p, (uint32_t)stbl_box, "stbl");
    p += moq_test_box_hdr(out + p, (uint32_t)stsd_box, "stsd");
    memset(out + p, 0, 4); p += 4; memset(out + p, 0, 3); p += 3; out[p++] = 1;   /* ver/flags, entry_count 1 */
    p += moq_test_box_hdr(out + p, (uint32_t)mp4a_box, "mp4a");
    memset(out + p, 0, 6); p += 6; out[p++] = 0; out[p++] = 1;          /* reserved, data_reference_index */
    memset(out + p, 0, 8); p += 8;                                     /* reserved */
    out[p++] = (uint8_t)(channels >> 8); out[p++] = (uint8_t)channels;
    out[p++] = 0; out[p++] = 16;                                       /* samplesize */
    memset(out + p, 0, 4); p += 4;                                     /* pre_defined, reserved */
    out[p++] = (uint8_t)(samplerate >> 8); out[p++] = (uint8_t)samplerate; out[p++] = 0; out[p++] = 0;
    p += moq_test_box_hdr(out + p, (uint32_t)esds_box, "esds");
    memset(out + p, 0, 4); p += 4;                                     /* ver/flags */
    out[p++] = 0x03; out[p++] = (uint8_t)(esd - 2);
    out[p++] = 0; out[p++] = 1; out[p++] = 0;                          /* ES_ID, flags */
    out[p++] = 0x04; out[p++] = (uint8_t)(dcd - 2);
    out[p++] = 0x40; out[p++] = 0x15; memset(out + p, 0, 11); p += 11; /* AAC, audio stream, sizes */
    out[p++] = 0x05; out[p++] = (uint8_t)asc_len;
    memcpy(out + p, asc, asc_len); p += asc_len;
    return p;
}

/* The fixture preflight: the catalog text is the current-version spelling,
 * the library's STRICT authoring path (parse, then encode) accepts it, and
 * each decoder configuration is structurally valid by the independent
 * codec-signaling validator. None of this is what the product under test
 * decides; it is what makes the fixture a legitimate catalog. */
static void preflight_catalog(const moq_alloc_t *al, const char *json, const char *label)
{
    MOQ_TEST_CHECK(strstr(json, "\"version\":\"1\"") != NULL);
    /* MSF-01 has no track-level init field: initialization data is referenced
     * (§5.2.13 initRef) into the root initDataList (§5.1.7). */
    MOQ_TEST_CHECK(strstr(json, "\"initData\"") == NULL);
    MOQ_TEST_CHECK(strstr(json, "\"initRef\":\"init-v\"") != NULL && strstr(json, "\"initRef\":\"init-ac\"") != NULL);
    MOQ_TEST_CHECK(strstr(json, "\"initDataList\":[") != NULL);
    /* The root initDataList follows the tracks in the literal input. */
    MOQ_TEST_CHECK(strstr(json, "\"tracks\":[") != NULL &&
                   strstr(json, "\"initDataList\":[") > strstr(json, "\"tracks\":["));
    /* §5.1.2: the live catalog carries generatedAt, the all-VOD catalog does not. */
    MOQ_TEST_CHECK((strstr(json, "\"isLive\":true") != NULL) == (strstr(json, "\"generatedAt\"") != NULL));
    moq_msf_catalog_t cat;
    memset(&cat, 0, sizeof(cat));
    moq_result_t prc = moq_msf_catalog_parse(al, (moq_bytes_t){ (const uint8_t *)json, strlen(json) }, &cat);
    MOQ_TEST_CHECK_EQ_INT((int)prc, (int)MOQ_OK);
    if (prc != MOQ_OK) { fprintf(stderr, "  %s: parse refused\n", label); return; }
    moq_rcbuf_t *re = NULL;
    moq_result_t erc = moq_msf_catalog_encode(al, &cat, &re);
    MOQ_TEST_CHECK_EQ_INT((int)erc, (int)MOQ_OK);      /* strict authoring accepts it */
    if (re) moq_rcbuf_decref(re);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)cat.track_count, K_TRACKS);
    MOQ_TEST_CHECK(moq_msf_catalog_find_init_data(&cat, MOQ_BYTES_LITERAL("init-v")) != NULL);
    MOQ_TEST_CHECK(moq_msf_catalog_find_init_data(&cat, MOQ_BYTES_LITERAL("init-ac")) != NULL);
    for (size_t i = 0; i < cat.track_count && i < K_TRACKS; i++) {
        const moq_msf_track_t *t = &cat.tracks[i];
        MOQ_TEST_CHECK(t->name.len == strlen(k_track_names[i]) &&
                       memcmp(t->name.data, k_track_names[i], t->name.len) == 0);
    }
    moq_msf_catalog_cleanup(al, &cat);
    printf("preflight %s: version \"1\", strict encode OK, %zu tracks, initRef -> initDataList resolved\n", label, (size_t)K_TRACKS);
}
static void preflight_decoder_configs(void)
{
    uint8_t buf[256]; size_t n = 0;
    moq_codec_init_data_cfg_t c;
    moq_codec_init_data_cfg_init(&c);
    c.source_format = MOQ_CODEC_SOURCE_AVC_AVCC;
    c.source = (moq_bytes_t){ k_avcc, sizeof(k_avcc) };
    MOQ_TEST_CHECK_EQ_INT((int)moq_codec_init_data_build(&c, buf, sizeof(buf), &n), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n, (uint64_t)sizeof(k_avcc));
    moq_codec_init_data_cfg_init(&c);
    c.source_format = MOQ_CODEC_SOURCE_AAC_ASC;
    c.source = (moq_bytes_t){ k_asc, sizeof(k_asc) };
    MOQ_TEST_CHECK_EQ_INT((int)moq_codec_init_data_build(&c, buf, sizeof(buf), &n), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)n, (uint64_t)sizeof(k_asc));
    printf("preflight decoder configs: avcC (SPS+PPS) and AudioSpecificConfig valid\n");
}

static void build_catalogs(const moq_alloc_t *al)
{
    static uint8_t vinit[2048], ainit[1024];
    size_t vn = moq_test_build_cmaf_init_cenc(vinit, 7, 90000, 1280, 720, k_avcc, sizeof(k_avcc),
                                              "cenc", 1, K_IV_SIZE, k_kid, 0, 0);
    size_t an = build_aac_init(ainit, K_AUDIO_TRACK_ID, 48000, 2, 48000, k_asc, sizeof(k_asc));
    MOQ_TEST_CHECK(vn > 0 && vn <= sizeof(vinit) && an > 0 && an <= sizeof(ainit));
    moq_rcbuf_t *vb = NULL, *ab = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_msf_encode_init_data(al, (moq_bytes_t){ vinit, vn }, &vb), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)moq_msf_encode_init_data(al, (moq_bytes_t){ ainit, an }, &ab), (int)MOQ_OK);
    const char *vs = (const char *)moq_rcbuf_data(vb); int vl = (int)moq_rcbuf_len(vb);
    const char *as = (const char *)moq_rcbuf_data(ab); int alen = (int)moq_rcbuf_len(ab);
    const char *fmt =
        "{\"version\":\"1\"%s"
        "\"contentProtections\":[{\"refID\":\"cp1\",\"defaultKID\":[\"" K_KID_UUID "\"],"
        "\"scheme\":\"cenc\",\"drmSystem\":{\"systemID\":\"" K_SYSTEM_UUID "\"}}],"
        "\"tracks\":["
        /* v: protected CMAF video, every video-class field live */
        "{\"name\":\"v\",\"packaging\":\"cmaf\",\"isLive\":%s,\"role\":\"video\","
        "\"codec\":\"avc1.42001e\",\"timescale\":90000,\"width\":1280,\"height\":720,"
        "\"framerate\":30,\"bitrate\":2000000,\"lang\":\"en\",\"label\":\"Main\","
        "\"maxGrpSapStartingType\":2,\"maxObjSapStartingType\":3,"
        "\"contentProtectionRefIDs\":[\"cp1\"],\"initRef\":\"init-v\"%s},"
        /* ac: CMAF AAC audio with a parsed init (nested samplerate/channels/ASC) */
        "{\"name\":\"ac\",\"packaging\":\"cmaf\",\"isLive\":%s,\"role\":\"audio\","
        "\"codec\":\"mp4a.40.2\",\"timescale\":48000,\"samplerate\":48000,\"channelConfig\":\"2\","
        "\"bitrate\":128000,\"lang\":\"en\",\"label\":\"AAC\",\"initRef\":\"init-ac\"%s},"
        /* a: LOC audio carrying the inline media-timeline template (MSF 5.2.15/7.4) */
        "{\"name\":\"a\",\"packaging\":\"loc\",\"isLive\":%s,\"role\":\"audio\","
        "\"codec\":\"opus\",\"timescale\":48000,\"samplerate\":48000,\"channelConfig\":\"2\","
        "\"bitrate\":96000,\"lang\":\"fr\",\"label\":\"Opus\","
        "\"template\":[1000,2002,[3,4],[1,2],1700000000000,2002]%s},"
        /* sap: a real CMSF SAP event timeline (CMSF §3.6.1: packaging
         * eventtimeline + eventType org.ietf.moq.cmsf.sap; MSF §8 mimeType
         * application/json; depends on the media it indexes) */
        "{\"name\":\"sap\",\"packaging\":\"eventtimeline\",\"isLive\":%s,"
        "\"eventType\":\"org.ietf.moq.cmsf.sap\",\"mimeType\":\"application/json\","
        "\"depends\":[\"v\",\"ac\"]%s},"
        /* mt: an explicit media timeline for v (MSF §7: mediatimeline packaging,
         * application/json, depends) -- v carries no template, so §5.2.15's
         * SHOULD NOT is respected; the template lives on `a`. */
        "{\"name\":\"mt\",\"packaging\":\"mediatimeline\",\"isLive\":%s,"
        "\"mimeType\":\"application/json\",\"depends\":[\"v\"]%s}"
        "],"
        /* MSF-01 §5.1.7: initialization data lives in the root initDataList
         * (type "inline", Base64 data) and tracks point at it by §5.2.13
         * initRef; there is no track-level init field in MSF-01. */
        "\"initDataList\":[{\"id\":\"init-v\",\"type\":\"inline\",\"data\":\"%.*s\"},"
        "{\"id\":\"init-ac\",\"type\":\"inline\",\"data\":\"%.*s\"}]}";
    const char *dur = ",\"trackDuration\":8072340";
    /* MSF-01 §5.1.2: generatedAt SHOULD NOT be included when isLive is false,
     * so only the live catalog carries it. Both formats are checked for
     * truncation: a cut-off catalog must never be described as valid. */
    int nl = snprintf(g_live, sizeof(g_live), fmt, ",\"generatedAt\":1,", "true", "", "true", "", "true", "", "true", "", "true", "", vl, vs, alen, as);
    int nv = snprintf(g_vod, sizeof(g_vod), fmt, ",", "false", dur, "false", dur, "false", dur, "false", dur, "false", dur, vl, vs, alen, as);
    MOQ_TEST_CHECK(nl > 0 && (size_t)nl < sizeof(g_live));
    MOQ_TEST_CHECK(nv > 0 && (size_t)nv < sizeof(g_vod));
    moq_rcbuf_decref(vb); moq_rcbuf_decref(ab);
    preflight_catalog(al, g_live, "live");
    preflight_catalog(al, g_vod, "vod");
    preflight_decoder_configs();
}

/* -- the three mutable fields, classified --------------------------------- */
typedef struct { bool is_live; bool has_duration; uint64_t duration_ms; } gen_t;
static gen_t read_gen(const moq_media_track_desc_t *d)
{ gen_t g = { d->is_live, d->has_track_duration, d->track_duration_ms }; return g; }
static bool is_old(gen_t g) { return g.is_live && !g.has_duration && g.duration_ms == 0; }
/* The row-declared NEW duration: the five-track fixture's unless a row
 * declares its own (the initial-VOD control does). */
static uint64_t g_new_duration_ms = VOD_DURATION_MS;
static bool is_new(gen_t g) { return !g.is_live && g.has_duration && g.duration_ms == g_new_duration_ms; }
static const char *classify(gen_t g) { return is_old(g) ? "OLD" : is_new(g) ? "NEW" : "TORN"; }

/* -- the complete immutable comparator ------------------------------------- */
/* Returns the name of the FIRST differing immutable member, or NULL. */
static bool span_eq(moq_bytes_t a, moq_bytes_t b)
{ return a.len == b.len && (a.len == 0 || (a.data && b.data && memcmp(a.data, b.data, a.len) == 0)); }
#define DIFF(cond, name) do { if (!(cond)) return (name); } while (0)
static const char *immutable_diff(const moq_media_track_desc_t *a, const moq_media_track_desc_t *b)
{
    DIFF(a->struct_size == b->struct_size, "struct_size");
    DIFF(span_eq(a->name, b->name), "name");
    DIFF(span_eq(a->role, b->role), "role");
    DIFF(span_eq(a->codec, b->codec), "codec");
    DIFF(span_eq(a->lang, b->lang), "lang");
    DIFF(span_eq(a->label, b->label), "label");
    DIFF(a->info.struct_size == b->info.struct_size, "info.struct_size");
    DIFF(a->info.media_type == b->info.media_type, "info.media_type");
    DIFF(a->info.packaging == b->info.packaging, "info.packaging");
    DIFF(a->info.timescale == b->info.timescale, "info.timescale");
    DIFF(a->info.transport_version == b->info.transport_version, "info.transport_version");
    DIFF(a->has_init == b->has_init, "has_init");
    DIFF(a->init.struct_size == b->init.struct_size, "init.struct_size");
    DIFF(a->init.codec_kind == b->init.codec_kind, "init.codec_kind");
    DIFF(a->init.timescale == b->init.timescale, "init.timescale");
    DIFF(a->init.width == b->init.width, "init.width");
    DIFF(a->init.height == b->init.height, "init.height");
    DIFF(a->init.samplerate == b->init.samplerate, "init.samplerate");
    DIFF(a->init.channel_count == b->init.channel_count, "init.channel_count");
    DIFF(span_eq(a->init.codec_config, b->init.codec_config), "init.codec_config");
    DIFF(a->init.track_id == b->init.track_id, "init.track_id");
    DIFF(a->init.has_cenc == b->init.has_cenc, "init.has_cenc");
    DIFF(span_eq(a->init.scheme, b->init.scheme), "init.scheme");
    DIFF(a->init.default_is_protected == b->init.default_is_protected, "init.default_is_protected");
    DIFF(a->init.default_per_sample_iv_size == b->init.default_per_sample_iv_size, "init.default_per_sample_iv_size");
    DIFF(span_eq(a->init.default_kid, b->init.default_kid), "init.default_kid");
    DIFF(span_eq(a->init_data, b->init_data), "init_data");
    DIFF(a->has_width == b->has_width && a->width == b->width, "width");
    DIFF(a->has_height == b->has_height && a->height == b->height, "height");
    DIFF(a->has_samplerate == b->has_samplerate && a->samplerate == b->samplerate, "samplerate");
    DIFF(span_eq(a->channel_config, b->channel_config), "channel_config");
    DIFF(a->has_framerate == b->has_framerate && a->framerate_millis == b->framerate_millis, "framerate");
    DIFF(a->has_bitrate == b->has_bitrate && a->bitrate == b->bitrate, "bitrate");
    DIFF(a->has_max_grp_sap == b->has_max_grp_sap && a->max_grp_sap == b->max_grp_sap, "max_grp_sap");
    DIFF(a->has_max_obj_sap == b->has_max_obj_sap && a->max_obj_sap == b->max_obj_sap, "max_obj_sap");
    DIFF(a->content_protection_ref_id_count == b->content_protection_ref_id_count, "cp_ref_count");
    for (size_t i = 0; i < a->content_protection_ref_id_count; i++)
        DIFF(span_eq(a->content_protection_ref_ids[i], b->content_protection_ref_ids[i]), "cp_ref[i]");
    DIFF(span_eq(a->packaging, b->packaging), "packaging");
    DIFF(span_eq(a->event_type, b->event_type), "event_type");
    DIFF(span_eq(a->mime_type, b->mime_type), "mime_type");
    DIFF(a->depends_count == b->depends_count, "depends_count");
    for (size_t i = 0; i < a->depends_count; i++)
        DIFF(span_eq(a->depends[i], b->depends[i]), "depends[i]");
    DIFF(a->has_template == b->has_template, "has_template");
    DIFF(a->template_.start_media_ms == b->template_.start_media_ms, "template.start_media_ms");
    DIFF(a->template_.delta_media_ms == b->template_.delta_media_ms, "template.delta_media_ms");
    DIFF(a->template_.start_group == b->template_.start_group, "template.start_group");
    DIFF(a->template_.start_object == b->template_.start_object, "template.start_object");
    DIFF(a->template_.delta_group == b->template_.delta_group, "template.delta_group");
    DIFF(a->template_.delta_object == b->template_.delta_object, "template.delta_object");
    DIFF(a->template_.start_wallclock_ms == b->template_.start_wallclock_ms, "template.start_wallclock_ms");
    DIFF(a->template_.delta_wallclock_ms == b->template_.delta_wallclock_ms, "template.delta_wallclock_ms");
    return NULL;
}

/* A deep IMAGE of a descriptor: the struct plus copies of every span and
 * array, so a later in-place mutation of the handle cannot alter the image. */
typedef struct {
    moq_media_track_desc_t d;
    uint8_t   bytes[8192];
    moq_bytes_t deps[8], cps[8];
} desc_image_t;

static moq_bytes_t img_span(desc_image_t *img, size_t *off, moq_bytes_t s)
{
    if (s.len == 0) return (moq_bytes_t){ NULL, 0 };
    MOQ_TEST_CHECK(*off + s.len <= sizeof(img->bytes));
    if (*off + s.len > sizeof(img->bytes)) return (moq_bytes_t){ NULL, 0 };
    memcpy(img->bytes + *off, s.data, s.len);
    moq_bytes_t r = { img->bytes + *off, s.len };
    *off += s.len;
    return r;
}
static void image_of(desc_image_t *img, const moq_media_track_desc_t *d)
{
    memset(img, 0, sizeof(*img));
    img->d = *d;
    size_t off = 0;
    img->d.name = img_span(img, &off, d->name);
    img->d.role = img_span(img, &off, d->role);
    img->d.codec = img_span(img, &off, d->codec);
    img->d.lang = img_span(img, &off, d->lang);
    img->d.label = img_span(img, &off, d->label);
    img->d.init.codec_config = img_span(img, &off, d->init.codec_config);
    img->d.init.scheme = img_span(img, &off, d->init.scheme);
    img->d.init.default_kid = img_span(img, &off, d->init.default_kid);
    img->d.init_data = img_span(img, &off, d->init_data);
    img->d.channel_config = img_span(img, &off, d->channel_config);
    img->d.packaging = img_span(img, &off, d->packaging);
    img->d.event_type = img_span(img, &off, d->event_type);
    img->d.mime_type = img_span(img, &off, d->mime_type);
    MOQ_TEST_CHECK(d->depends_count <= 8 && d->content_protection_ref_id_count <= 8);
    for (size_t i = 0; i < d->depends_count && i < 8; i++) img->deps[i] = img_span(img, &off, d->depends[i]);
    for (size_t i = 0; i < d->content_protection_ref_id_count && i < 8; i++) img->cps[i] = img_span(img, &off, d->content_protection_ref_ids[i]);
    img->d.depends = img->deps;
    img->d.content_protection_ref_ids = img->cps;
}

/* Every class the fixture makes live must actually be non-empty on the handle
 * it belongs to, or that part of the comparator is not an oracle. Fields that
 * are codec-kind exclusive are stated as such: init.width/height belong to a
 * visual entry and init.samplerate/channel_count to an audio entry, so each
 * is judged on the handle whose codec kind can carry it. */
static void check_fixture_classes(const moq_media_track_desc_t *v, const moq_media_track_desc_t *ac,
                                  const moq_media_track_desc_t *a, const moq_media_track_desc_t *sap,
                                  const moq_media_track_desc_t *mt)
{
    /* v: protected AVC video init, fully parsed. */
    MOQ_TEST_CHECK(v->has_init && v->init.codec_kind == MOQ_CMAF_CODEC_AVC);
    MOQ_TEST_CHECK(v->init.width == 1280 && v->init.height == 720 && v->init.track_id == 7 && v->init.timescale == 90000);
    MOQ_TEST_CHECK(v->init.codec_config.len == sizeof(k_avcc) && memcmp(v->init.codec_config.data, k_avcc, sizeof(k_avcc)) == 0);
    MOQ_TEST_CHECK(v->init.has_cenc && v->init.scheme.len == 4 && memcmp(v->init.scheme.data, "cenc", 4) == 0);
    MOQ_TEST_CHECK(v->init.default_kid.len == 16 && memcmp(v->init.default_kid.data, k_kid, 16) == 0);
    MOQ_TEST_CHECK(v->init.default_is_protected == 1 && v->init.default_per_sample_iv_size == K_IV_SIZE);
    MOQ_TEST_CHECK(v->init_data.len > 0 && v->lang.len == 2 && v->label.len == 4 && v->role.len == 5);
    MOQ_TEST_CHECK(v->has_width && v->has_height && v->has_framerate && v->has_bitrate);
    MOQ_TEST_CHECK(v->has_max_grp_sap && v->max_grp_sap == 2 && v->has_max_obj_sap && v->max_obj_sap == 3);
    MOQ_TEST_CHECK(v->content_protection_ref_id_count == 1 && v->content_protection_ref_ids[0].len == 3);
    MOQ_TEST_CHECK(v->info.timescale == 90000 && v->info.packaging == MOQ_MEDIA_PACKAGING_CMAF);
    /* ac: AAC audio init, fully parsed -- the audio-exclusive nested fields. */
    MOQ_TEST_CHECK(ac->has_init && ac->init.codec_kind == MOQ_CMAF_CODEC_AAC);
    MOQ_TEST_CHECK(ac->init.samplerate == 48000 && ac->init.channel_count == 2 && ac->init.track_id == K_AUDIO_TRACK_ID);
    MOQ_TEST_CHECK(ac->init.codec_config.len == sizeof(k_asc) && memcmp(ac->init.codec_config.data, k_asc, sizeof(k_asc)) == 0);
    MOQ_TEST_CHECK(!ac->init.has_cenc && ac->init.width == 0 && ac->init.height == 0);   /* visual-only fields: inactive by codec kind */
    MOQ_TEST_CHECK(ac->has_samplerate && ac->samplerate == 48000 && ac->channel_config.len == 1);
    /* a: catalog-level audio fields plus the inline template. */
    MOQ_TEST_CHECK(a->has_samplerate && a->samplerate == 48000 && a->channel_config.len == 1 && a->lang.len == 2 && a->label.len == 4);
    MOQ_TEST_CHECK(a->has_template && a->template_.start_media_ms == 1000 && a->template_.delta_media_ms == 2002 &&
                   a->template_.start_group == 3 && a->template_.start_object == 4 &&
                   a->template_.delta_group == 1 && a->template_.delta_object == 2 &&
                   a->template_.start_wallclock_ms == 1700000000000ull && a->template_.delta_wallclock_ms == 2002);
    /* sap: a real CMSF SAP timeline; mt: explicit media timeline. */
    MOQ_TEST_CHECK(sap->event_type.len == 21 && memcmp(sap->event_type.data, "org.ietf.moq.cmsf.sap", 21) == 0);
    MOQ_TEST_CHECK(sap->mime_type.len == 16 && sap->depends_count == 2);
    MOQ_TEST_CHECK(mt->packaging.len == 13 && mt->depends_count == 1 && !mt->has_template && mt->mime_type.len == 16);
}

/* The comparator's own discrimination, on copied images of live handles. Every
 * mutation here is same-count / same-shape: a scalar changed in place, one
 * byte of a span of unchanged length, one element of an array of unchanged
 * count. */
static void expect_diff(const moq_media_track_desc_t *base, const moq_media_track_desc_t *m, const char *want)
{
    const char *got = immutable_diff(base, m);
    if (!got || strcmp(got, want) != 0) {
        fprintf(stderr, "FAIL: comparator self-check expected '%s', got '%s'\n", want, got ? got : "(equal)");
        failures++;
    }
}
static void comparator_selfcheck(const moq_media_track_desc_t *v, const moq_media_track_desc_t *ac,
                                 const moq_media_track_desc_t *sap, const moq_media_track_desc_t *a)
{
    desc_image_t base, m;
    image_of(&base, v);
    MOQ_TEST_CHECK(immutable_diff(&base.d, v) == NULL);          /* image == handle */
    /* The self-checks flip bytes inside spans; a handle that does not carry
     * them (a perturbed fixture) is a named failure here, never a crash. */
    MOQ_TEST_CHECK(base.d.codec.len > 0 && base.d.init.default_kid.len == 16 && base.d.init_data.len > 3 &&
                   base.d.init.codec_config.len > 8 && base.d.content_protection_ref_id_count == 1);
    if (!(base.d.codec.len > 0 && base.d.init.default_kid.len == 16 && base.d.init_data.len > 3 &&
          base.d.init.codec_config.len > 8 && base.d.content_protection_ref_id_count == 1)) return;
    image_of(&m, v); m.d.width += 1;                              expect_diff(&base.d, &m.d, "width");
    image_of(&m, v); ((uint8_t *)m.d.codec.data)[0] ^= 1;         expect_diff(&base.d, &m.d, "codec");
    image_of(&m, v); ((uint8_t *)m.d.init.default_kid.data)[15] ^= 1; expect_diff(&base.d, &m.d, "init.default_kid");
    image_of(&m, v); m.d.init.track_id = 8;                       expect_diff(&base.d, &m.d, "init.track_id");
    image_of(&m, v); m.d.init.default_per_sample_iv_size = 16;    expect_diff(&base.d, &m.d, "init.default_per_sample_iv_size");
    image_of(&m, v); ((uint8_t *)m.d.init.codec_config.data)[8] ^= 1; expect_diff(&base.d, &m.d, "init.codec_config");
    image_of(&m, v); ((uint8_t *)m.d.init_data.data)[3] ^= 1;     expect_diff(&base.d, &m.d, "init_data");
    image_of(&m, v); ((uint8_t *)m.d.content_protection_ref_ids[0].data)[0] ^= 1; expect_diff(&base.d, &m.d, "cp_ref[i]");
    desc_image_t abase, am;
    image_of(&abase, ac);
    MOQ_TEST_CHECK(abase.d.init.codec_config.len >= 2 && abase.d.channel_config.len >= 1);
    if (!(abase.d.init.codec_config.len >= 2 && abase.d.channel_config.len >= 1)) return;
    image_of(&am, ac); am.d.init.samplerate = 44100;              expect_diff(&abase.d, &am.d, "init.samplerate");
    image_of(&am, ac); am.d.init.channel_count = 1;               expect_diff(&abase.d, &am.d, "init.channel_count");
    image_of(&am, ac); ((uint8_t *)am.d.init.codec_config.data)[1] ^= 1; expect_diff(&abase.d, &am.d, "init.codec_config");
    image_of(&am, ac); ((uint8_t *)am.d.channel_config.data)[0] ^= 1;    expect_diff(&abase.d, &am.d, "channel_config");
    desc_image_t sbase, sm;
    image_of(&sbase, sap);
    MOQ_TEST_CHECK(sbase.d.depends_count == 2 && sbase.d.depends[1].len > 0 && sbase.d.event_type.len > 0);
    if (!(sbase.d.depends_count == 2 && sbase.d.depends[1].len > 0 && sbase.d.event_type.len > 0)) return;
    image_of(&sm, sap); ((uint8_t *)sm.d.depends[1].data)[0] ^= 1;   expect_diff(&sbase.d, &sm.d, "depends[i]");
    image_of(&sm, sap); ((uint8_t *)sm.d.event_type.data)[0] ^= 1;   expect_diff(&sbase.d, &sm.d, "event_type");
    desc_image_t tbase, tm;
    image_of(&tbase, a);
    MOQ_TEST_CHECK(tbase.d.has_template);
    if (!tbase.d.has_template) return;
    image_of(&tm, a); tm.d.template_.delta_object += 1;           expect_diff(&tbase.d, &tm.d, "template.delta_object");
    image_of(&tm, a); tm.d.template_.start_wallclock_ms += 1;     expect_diff(&tbase.d, &tm.d, "template.start_wallclock_ms");
    /* The mutable three are deliberately NOT compared. */
    image_of(&m, v); m.d.is_live = !m.d.is_live; m.d.has_track_duration = true; m.d.track_duration_ms = 1;
    MOQ_TEST_CHECK(immutable_diff(&base.d, &m.d) == NULL);
}

/* -- bounded, sticky, complete event inventory ----------------------------- */
#define INV_MAX 32
typedef struct {
    int n;
    moq_media_track_event_kind_t kind[INV_MAX];
    moq_media_track_t *track[INV_MAX];
    const moq_media_track_desc_t *desc[INV_MAX];
    gen_t gen[INV_MAX];
    bool overflow;
} inventory_t;

/* Drain EVERYTHING pollable now. Every event is recorded; nothing is discarded. */
static void drain_all(moq_media_receiver_t *r, inventory_t *inv)
{
    memset(inv, 0, sizeof(*inv));
    for (;;) {
        moq_media_track_event_t ev;
        moq_result_t rc = moq_media_receiver_poll_track(r, &ev, sizeof(ev));
        if (rc != MOQ_OK) {
            /* Empty. A receiver with no endpoint reports CLOSED for "no
             * endpoint" on the empty path; that is only acceptable while the
             * receiver itself is not fatal, which is asserted explicitly. */
            MOQ_TEST_CHECK(rc == MOQ_DONE || rc == MOQ_ERR_CLOSED);
            MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
            break;
        }
        if (inv->n >= INV_MAX) { inv->overflow = true; break; }
        inv->kind[inv->n] = ev.kind; inv->track[inv->n] = ev.track; inv->desc[inv->n] = ev.desc;
        inv->gen[inv->n] = ev.desc ? read_gen(ev.desc) : (gen_t){ false, false, 0 };
        inv->n++;
    }
    MOQ_TEST_CHECK(!inv->overflow);
}
static const char *kind_name(moq_media_track_event_kind_t k)
{
    switch (k) {
    case MOQ_MEDIA_TRACK_ADDED: return "ADDED";
    case MOQ_MEDIA_TRACK_UPDATED: return "UPDATED";
    case MOQ_MEDIA_TRACK_REMOVED: return "REMOVED";
    case MOQ_MEDIA_TRACK_ENDED: return "ENDED";
    case MOQ_MEDIA_CATALOG_READY: return "CATALOG_READY";
    case MOQ_MEDIA_TRACK_PARSE_DROP: return "PARSE_DROP";
    default: return "OTHER";
    }
}
static void print_inventory(const char *label, const inventory_t *inv)
{
    printf("%s:", label);
    for (int i = 0; i < inv->n; i++)
        printf(" %s%s%s", kind_name(inv->kind[i]),
               inv->desc[i] ? "/" : "", inv->desc[i] ? classify(inv->gen[i]) : "");
    printf("%s\n", inv->n == 0 ? " (empty)" : "");
}
/* The one discovery shape both modes expect: exactly the K_TRACKS declared
 * names, each ADDED once with a desc, in declared order, then exactly one
 * CATALOG_READY with no track and no desc. Duplicates, missing names and
 * extras all fail; identity is by name bytes. Returns the index after
 * CATALOG_READY, or -1. */
static int expect_discovery(const inventory_t *inv, int from, int idx_out[K_TRACKS])
{
    bool seen[K_TRACKS]; memset(seen, 0, sizeof(seen));
    for (int i = 0; i < K_TRACKS; i++) {
        int k = from + i;
        MOQ_TEST_CHECK(k < inv->n && inv->kind[k] == MOQ_MEDIA_TRACK_ADDED);
        if (!(k < inv->n && inv->kind[k] == MOQ_MEDIA_TRACK_ADDED)) return -1;
        MOQ_TEST_CHECK(inv->track[k] != NULL && inv->desc[k] != NULL);
        if (!inv->desc[k]) return -1;
        int which = -1;
        for (int j = 0; j < K_TRACKS; j++)
            if (inv->desc[k]->name.len == strlen(k_track_names[j]) &&
                memcmp(inv->desc[k]->name.data, k_track_names[j], inv->desc[k]->name.len) == 0) which = j;
        if (which < 0) { fprintf(stderr, "FAIL: ADDED #%d names an undeclared track\n", i); failures++; return -1; }
        if (seen[which]) { fprintf(stderr, "FAIL: ADDED #%d duplicates '%s'\n", i, k_track_names[which]); failures++; return -1; }
        if (which != i) { fprintf(stderr, "FAIL: ADDED #%d is '%s', declared order expects '%s'\n", i, k_track_names[which], k_track_names[i]); failures++; }
        seen[which] = true;
        idx_out[which] = k;
    }
    for (int j = 0; j < K_TRACKS; j++) MOQ_TEST_CHECK(seen[j]);
    int r = from + K_TRACKS;
    MOQ_TEST_CHECK(r < inv->n && inv->kind[r] == MOQ_MEDIA_CATALOG_READY);
    MOQ_TEST_CHECK(r < inv->n && inv->track[r] == NULL && inv->desc[r] == NULL);
    return r + 1;
}

/* -- the interleaving observer -------------------------------------------- */
typedef struct {
    moq_media_receiver_t *r;
    const moq_media_track_t *watched;          /* the video handle */
    const moq_media_track_desc_t *added_desc;
    gen_t added_at[4], get_at[4];
    inventory_t queue_at[4];
    int calls[4];
} observer_t;
static void on_desc_update(const moq_media_receiver_t *r, const moq_media_track_t *t, int phase, void *ctx)
{
    observer_t *o = ctx; (void)r;
    if (phase < 0 || phase > 3) { failures++; return; }
    o->calls[phase]++;
    if (t != o->watched) return;               /* other tuples convert too; only v is observed */
    o->added_at[phase] = read_gen(o->added_desc);
    o->get_at[phase] = read_gen(moq_media_track_desc_get(t));
    /* The queue at PRE, MID and POST, drained completely and recorded: whatever
     * is here IS the inventory at that phase, and it must be empty. At
     * PUBLISHED nothing is consumed -- the update is drained by the declared
     * post-ingest inventory, never "to peek". */
    if (phase != DESC_PUBLISHED) drain_all(o->r, &o->queue_at[phase]);
}

/* -- common setup ------------------------------------------------------------ */
typedef struct {
    astats_t st; moq_alloc_t alloc; moq_media_receiver_t *r;
} rig_t;
static bool rig_open(rig_t *g)
{
    memset(g, 0, sizeof(*g));
    g->alloc = (moq_alloc_t){ &g->st, c_alloc, c_realloc, c_free };
    build_catalogs(&g->alloc);
    static moq_bytes_t ns[2] = { MOQ_BYTES_LITERAL("svc"), MOQ_BYTES_LITERAL("demo") };
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live(&cfg);
    cfg.namespace_ = (moq_namespace_t){ ns, 2 };
    cfg.auto_subscribe = false;
    g->r = moq_media_receiver_test_new_cfg_alloc(&cfg, &g->alloc);
    MOQ_TEST_CHECK(g->r != NULL);
    if (!g->r) return false;
    /* A descriptor carries the session's draft or is not built at all; the
     * harness has no session, so the draft is supplied as production's pump
     * would read it from the live session. */
    moq_media_receiver_test_set_transport_version(g->r, MOQ_VERSION_DRAFT_16);
    return true;
}
static void rig_close(rig_t *g)
{
    moq_media_receiver_test_free(g->r);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)g->st.live, 0);
    printf("allocator: %ld allocations, %ld live after free\n", g->st.total, g->st.live);
}

/* Judge the post-conversion state shared by both modes. */
static void judge_after(const moq_media_track_desc_t *added_v, const desc_image_t *old_img,
                        const moq_media_track_desc_t *updated_v, moq_media_track_t *tv)
{
    gen_t g_added = read_gen(added_v), g_get = read_gen(moq_media_track_desc_get(tv));
    gen_t g_upd = updated_v ? read_gen(updated_v) : (gen_t){ false, false, 0 };
    printf("after conversion: added-pointer=%s updated-event=%s desc_get=%s\n",
           classify(g_added), updated_v ? classify(g_upd) : "none", classify(g_get));
    /* The retained TRACK_ADDED pointer is the OLD generation, immutable. */
    MOQ_TEST_CHECK(is_old(g_added));
    MOQ_TEST_CHECK(immutable_diff(&old_img->d, added_v) == NULL);
    /* TRACK_UPDATED and the getter are the complete NEW generation. */
    MOQ_TEST_CHECK(updated_v != NULL);
    if (updated_v) {
        MOQ_TEST_CHECK(is_new(g_upd));
        const char *dd = immutable_diff(&old_img->d, updated_v);
        if (dd) fprintf(stderr, "  immutable member differs across generations: %s\n", dd);
        MOQ_TEST_CHECK(dd == NULL);
        MOQ_TEST_CHECK(moq_media_track_desc_get(tv) == updated_v);
    }
    MOQ_TEST_CHECK(is_new(g_get));
}

/* -- mode: interleaving ---------------------------------------------------- */
static void mode_interleaving(void)
{
    rig_t g; if (!rig_open(&g)) return;
    moq_media_receiver_test_ingest(g.r, 0, 0, g_live, strlen(g_live));
    inventory_t disc; drain_all(g.r, &disc);
    print_inventory("discovery", &disc);
    int idx[K_TRACKS] = { -1, -1, -1, -1, -1 };
    int next = expect_discovery(&disc, 0, idx);
    MOQ_TEST_CHECK_EQ_INT(next, disc.n);   /* nothing else */
    if (next < 0) { rig_close(&g); return; }
    const int iv = idx[0], iac = idx[1], ia = idx[2], is = idx[3], im = idx[4];
    check_fixture_classes(disc.desc[iv], disc.desc[iac], disc.desc[ia], disc.desc[is], disc.desc[im]);
    comparator_selfcheck(disc.desc[iv], disc.desc[iac], disc.desc[is], disc.desc[ia]);
    for (int j = 0; j < K_TRACKS; j++) MOQ_TEST_CHECK(is_old(read_gen(disc.desc[idx[j]])));
    desc_image_t img[K_TRACKS];
    for (int j = 0; j < K_TRACKS; j++) image_of(&img[j], disc.desc[idx[j]]);

    /* Observe the video track's conversion at every phase. */
    observer_t o; memset(&o, 0, sizeof(o)); o.r = g.r; o.watched = disc.track[iv]; o.added_desc = disc.desc[iv];
    moq_media_receiver_test_set_desc_update_hook(on_desc_update, &o);
    moq_media_receiver_test_ingest(g.r, 1, 0, g_vod, strlen(g_vod));
    moq_media_receiver_test_set_desc_update_hook(NULL, NULL);
    inventory_t post; drain_all(g.r, &post);

    /* The hook fires once per converted tuple (every declared track); the
     * observer records only the video track's phases and counts all. */
    static const char *ph[4] = { "PRE", "MID", "POST", "PUBLISHED" };
    for (int p = 0; p < 4; p++) {
        MOQ_TEST_CHECK_EQ_INT(o.calls[p], K_TRACKS);
        printf("%s (v): added-pointer=%s desc_get=%s", ph[p], classify(o.added_at[p]), classify(o.get_at[p]));
        if (p != DESC_PUBLISHED) print_inventory("  queue", &o.queue_at[p]); else printf("  (queue not consumed here)\n");
    }
    print_inventory("post-ingest queue", &post);
    for (int p = 0; p < 4; p++) {
        MOQ_TEST_CHECK(strcmp(classify(o.added_at[p]), "TORN") != 0);
        MOQ_TEST_CHECK(strcmp(classify(o.get_at[p]), "TORN") != 0);
    }
    for (int p = 0; p < 4; p++) MOQ_TEST_CHECK(is_old(o.added_at[p]));
    for (int p = 0; p < 3; p++) MOQ_TEST_CHECK(is_old(o.get_at[p]));
    MOQ_TEST_CHECK(is_new(o.get_at[DESC_PUBLISHED]));
    for (int p = 0; p < 3; p++) MOQ_TEST_CHECK_EQ_INT(o.queue_at[p].n, 0);

    /* Post-ingest: exactly one TRACK_UPDATED per converted tuple, in declared
     * order, each with exact handle identity; nothing else. */
    int want_updates = 0; for (int j = 0; j < K_TRACKS; j++) if (k_converts[j]) want_updates++;
    MOQ_TEST_CHECK_EQ_INT(post.n, want_updates);
    const moq_media_track_desc_t *updated[K_TRACKS] = { NULL, NULL, NULL, NULL, NULL };
    int u = 0;
    for (int j = 0; j < K_TRACKS && u < post.n; j++) {
        if (!k_converts[j]) continue;
        MOQ_TEST_CHECK(post.kind[u] == MOQ_MEDIA_TRACK_UPDATED);
        MOQ_TEST_CHECK(post.track[u] == disc.track[idx[j]]);
        if (post.track[u] == disc.track[idx[j]]) updated[j] = post.desc[u];
        u++;
    }
    judge_after(disc.desc[iv], &img[0], updated[0], disc.track[iv]);
    /* Every other converted tuple: new generation on the getter, immutable
     * members equal; every unconverted tuple: untouched and still OLD. */
    for (int j = 1; j < K_TRACKS; j++) {
        if (k_converts[j]) {
            MOQ_TEST_CHECK(updated[j] != NULL && is_new(read_gen(moq_media_track_desc_get(disc.track[idx[j]]))));
            if (updated[j]) MOQ_TEST_CHECK(immutable_diff(&img[j].d, updated[j]) == NULL);
            MOQ_TEST_CHECK(is_old(read_gen(disc.desc[idx[j]])));           /* retained pointer */
        } else {
            MOQ_TEST_CHECK(immutable_diff(&img[j].d, disc.desc[idx[j]]) == NULL);
            MOQ_TEST_CHECK(read_gen(disc.desc[idx[j]]).is_live);
        }
    }
    rig_close(&g);
}

/* -- mode: delayed poll ----------------------------------------------------- */
static void mode_delayed(void)
{
    rig_t g; if (!rig_open(&g)) return;
    moq_media_receiver_test_ingest(g.r, 0, 0, g_live, strlen(g_live));
    moq_media_receiver_test_ingest(g.r, 1, 0, g_vod, strlen(g_vod));
    inventory_t inv; drain_all(g.r, &inv);
    print_inventory("delayed drain", &inv);
    /* Exact identity, order and multiplicity: K_TRACKS ADDED by name in
     * declared order, one CATALOG_READY, then one UPDATED per converted
     * tuple in declared order with exact handle identity -- nothing else. */
    int idx[K_TRACKS] = { -1, -1, -1, -1, -1 };
    int next = expect_discovery(&inv, 0, idx);
    if (next < 0) { rig_close(&g); return; }
    int want_updates = 0; for (int j = 0; j < K_TRACKS; j++) if (k_converts[j]) want_updates++;
    MOQ_TEST_CHECK_EQ_INT(inv.n, K_TRACKS + 1 + want_updates);
    check_fixture_classes(inv.desc[idx[0]], inv.desc[idx[1]], inv.desc[idx[2]], inv.desc[idx[3]], inv.desc[idx[4]]);
    int u = next;
    for (int j = 0; j < K_TRACKS; j++) {
        if (!k_converts[j]) { MOQ_TEST_CHECK(is_old(inv.gen[idx[j]]) || read_gen(inv.desc[idx[j]]).is_live); continue; }
        /* TRACK_ADDED carries the immutable OLD generation even though it was
         * polled after the conversion; TRACK_UPDATED the complete NEW one. */
        MOQ_TEST_CHECK(is_old(inv.gen[idx[j]]));
        MOQ_TEST_CHECK(u < inv.n && inv.kind[u] == MOQ_MEDIA_TRACK_UPDATED && inv.track[u] == inv.track[idx[j]]);
        if (u < inv.n) {
            MOQ_TEST_CHECK(is_new(inv.gen[u]));
            const char *dd = immutable_diff(inv.desc[idx[j]], inv.desc[u]);
            if (dd) fprintf(stderr, "  '%s': immutable member differs between ADDED and UPDATED: %s\n", k_track_names[j], dd);
            MOQ_TEST_CHECK(dd == NULL);
            /* Both pointers keep reading their own generations. */
            MOQ_TEST_CHECK(is_old(read_gen(inv.desc[idx[j]])));
            MOQ_TEST_CHECK(is_new(read_gen(inv.desc[u])));
            MOQ_TEST_CHECK(moq_media_track_desc_get(inv.track[idx[j]]) == inv.desc[u]);
        }
        u++;
    }
    MOQ_TEST_CHECK_EQ_INT(u, inv.n);
    rig_close(&g);
}

/* -- control: a duplicate VOD catalog publishes nothing new ---------------- */
static void mode_duplicate(void)
{
    rig_t g; if (!rig_open(&g)) return;
    moq_media_receiver_test_ingest(g.r, 0, 0, g_live, strlen(g_live));
    moq_media_receiver_test_ingest(g.r, 1, 0, g_vod, strlen(g_vod));
    inventory_t inv; drain_all(g.r, &inv);
    int idx[K_TRACKS] = { -1, -1, -1, -1, -1 };
    int next = expect_discovery(&inv, 0, idx);
    if (next < 0) { rig_close(&g); return; }
    MOQ_TEST_CHECK_EQ_INT(inv.n, K_TRACKS + 1 + K_TRACKS);
    const moq_media_track_desc_t *cur[K_TRACKS];
    /* Deep images of BOTH generations, taken before the duplicate: the
     * complete comparator judges every immutable member and span afterwards,
     * not just addresses and the three mutable fields. */
    static desc_image_t old_img[K_TRACKS], cur_img[K_TRACKS];
    for (int j = 0; j < K_TRACKS; j++) {
        cur[j] = moq_media_track_desc_get(inv.track[idx[j]]);
        image_of(&old_img[j], inv.desc[idx[j]]);
        image_of(&cur_img[j], cur[j]);
    }
    /* The same all-VOD catalog again, as a later generation. */
    moq_media_receiver_test_ingest(g.r, 2, 0, g_vod, strlen(g_vod));
    inventory_t again; drain_all(g.r, &again);
    print_inventory("duplicate VOD drain", &again);
    MOQ_TEST_CHECK_EQ_INT(again.n, 0);               /* no second update */
    for (int j = 0; j < K_TRACKS; j++) {
        /* The published snapshot is neither replaced nor rewritten: same
         * address, same generation, every immutable member and backing byte
         * identical to the image; generation 1 likewise. */
        MOQ_TEST_CHECK(moq_media_track_desc_get(inv.track[idx[j]]) == cur[j]);
        MOQ_TEST_CHECK(is_new(read_gen(cur[j])));
        const char *dc = immutable_diff(&cur_img[j].d, cur[j]);
        if (dc) fprintf(stderr, "  '%s': current snapshot rewritten across the duplicate: %s\n", k_track_names[j], dc);
        MOQ_TEST_CHECK(dc == NULL);
        MOQ_TEST_CHECK(is_old(read_gen(inv.desc[idx[j]])));   /* gen 1 pointer still OLD */
        const char *dO = immutable_diff(&old_img[j].d, inv.desc[idx[j]]);
        if (dO) fprintf(stderr, "  '%s': generation 1 rewritten across the duplicate: %s\n", k_track_names[j], dO);
        MOQ_TEST_CHECK(dO == NULL);
    }
    rig_close(&g);
}

/* -- control: an initially non-live track acquires a duration once --------- */
static const char k_vod0[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"x\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000}]}";
static const char k_vod0_dur[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"x\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000,\"trackDuration\":5000}]}";
static const char k_vod0_dur2[] =
    "{\"version\":\"1\",\"tracks\":[{\"name\":\"x\",\"packaging\":\"loc\",\"isLive\":false,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000,\"trackDuration\":6000}]}";
static void mode_initial_vod(void)
{
    rig_t g; if (!rig_open(&g)) return;
    g_new_duration_ms = 5000;   /* this row's declared NEW generation */
    moq_media_receiver_test_ingest(g.r, 0, 0, k_vod0, sizeof(k_vod0) - 1);
    inventory_t d1; drain_all(g.r, &d1);
    print_inventory("initial-vod discovery", &d1);   /* labels: ADDED reads a non-live, no-duration gen 1 */
    MOQ_TEST_CHECK_EQ_INT(d1.n, 2);
    MOQ_TEST_CHECK(d1.n == 2 && d1.kind[0] == MOQ_MEDIA_TRACK_ADDED && d1.kind[1] == MOQ_MEDIA_CATALOG_READY);
    if (d1.n < 2 || !d1.desc[0]) { rig_close(&g); return; }
    const moq_media_track_desc_t *g1 = d1.desc[0];
    MOQ_TEST_CHECK(!g1->is_live && !g1->has_track_duration);
    /* The one allowed change: a previously absent duration appears. */
    moq_media_receiver_test_ingest(g.r, 1, 0, k_vod0_dur, sizeof(k_vod0_dur) - 1);
    inventory_t d2; drain_all(g.r, &d2);
    print_inventory("duration-added drain", &d2);
    MOQ_TEST_CHECK_EQ_INT(d2.n, 1);
    MOQ_TEST_CHECK(d2.n == 1 && d2.kind[0] == MOQ_MEDIA_TRACK_UPDATED && d2.track[0] == d1.track[0]);
    const moq_media_track_desc_t *g2 = d2.n == 1 ? d2.desc[0] : NULL;
    MOQ_TEST_CHECK(g2 != NULL && g2 != g1);
    if (g2) {
        MOQ_TEST_CHECK(!g2->is_live && g2->has_track_duration && g2->track_duration_ms == 5000);
        MOQ_TEST_CHECK(moq_media_track_desc_get(d1.track[0]) == g2);
        MOQ_TEST_CHECK(immutable_diff(g1, g2) == NULL);
    }
    MOQ_TEST_CHECK(!g1->is_live && !g1->has_track_duration);   /* gen 1 unchanged */
    /* Changing an existing duration is an immutable-tuple violation: dropped,
     * nothing published, nothing rewritten. */
    moq_media_receiver_test_ingest(g.r, 2, 0, k_vod0_dur2, sizeof(k_vod0_dur2) - 1);
    inventory_t d3; drain_all(g.r, &d3);
    print_inventory("duration-change drain", &d3);
    MOQ_TEST_CHECK_EQ_INT(d3.n, 0);
    MOQ_TEST_CHECK(moq_media_track_desc_get(d1.track[0]) == g2);
    MOQ_TEST_CHECK(g2 && g2->track_duration_ms == 5000);
    g_new_duration_ms = VOD_DURATION_MS;
    rig_close(&g);
}

/* -- control: removal after conversion ------------------------------------- */
static void mode_removal(void)
{
    rig_t g; if (!rig_open(&g)) return;
    moq_media_receiver_test_ingest(g.r, 0, 0, g_live, strlen(g_live));
    moq_media_receiver_test_ingest(g.r, 1, 0, g_vod, strlen(g_vod));
    inventory_t inv; drain_all(g.r, &inv);
    int idx[K_TRACKS] = { -1, -1, -1, -1, -1 };
    int next = expect_discovery(&inv, 0, idx);
    if (next < 0) { rig_close(&g); return; }
    MOQ_TEST_CHECK_EQ_INT(inv.n, K_TRACKS + 1 + K_TRACKS);
    const int iv = idx[0];
    const moq_media_track_desc_t *g1 = inv.desc[iv];
    const moq_media_track_desc_t *g2 = moq_media_track_desc_get(inv.track[iv]);
    MOQ_TEST_CHECK(g2 != g1 && is_new(read_gen(g2)) && is_old(read_gen(g1)));
    /* Frozen BEFORE the removal: the exact handle and current snapshot each
     * removed tuple must report (v, sap, mt, one each), plus deep images of
     * both of v's generations. */
    const int removed_idx[3] = { idx[0], idx[3], idx[4] };
    const char *const removed_name[3] = { "v", "sap", "mt" };
    const moq_media_track_desc_t *expect_snap[3];
    for (int k = 0; k < 3; k++) expect_snap[k] = moq_media_track_desc_get(inv.track[removed_idx[k]]);
    static desc_image_t img_g1, img_g2;
    image_of(&img_g1, g1); image_of(&img_g2, g2);
    /* Generation 3 drops `v` (and the timelines that depend on it) but keeps
     * the two audio tracks exactly as converted. */
    static char cat3[8192];
    const char *vs = strstr(g_vod, "{\"name\":\"ac\"");
    const char *ve = strstr(g_vod, ",{\"name\":\"sap\"");
    MOQ_TEST_CHECK(vs && ve && ve > vs);
    if (!(vs && ve && ve > vs)) { rig_close(&g); return; }
    const char *tail = strstr(g_vod, "],\"initDataList\"");
    MOQ_TEST_CHECK(tail != NULL);
    if (!tail) { rig_close(&g); return; }
    int n3 = snprintf(cat3, sizeof(cat3), "{\"version\":\"1\",\"tracks\":[%.*s%s", (int)(ve - vs), vs, tail);
    MOQ_TEST_CHECK(n3 > 0 && (size_t)n3 < sizeof(cat3));
    moq_media_receiver_test_ingest(g.r, 2, 0, cat3, strlen(cat3));
    inventory_t rem; drain_all(g.r, &rem);
    print_inventory("removal drain", &rem);
    /* The exact declared multiset: one REMOVED each for v, sap and mt, each
     * carrying the snapshot frozen above for that handle; no duplicate, no
     * foreign entry, no leftover, no other kind. */
    int seen[3] = { 0, 0, 0 }; int foreign = 0, other = 0;
    for (int i = 0; i < rem.n; i++) {
        if (rem.kind[i] != MOQ_MEDIA_TRACK_REMOVED) { other++; continue; }
        int which = -1;
        for (int k = 0; k < 3; k++) if (rem.track[i] == inv.track[removed_idx[k]]) which = k;
        if (which < 0) { foreign++; continue; }
        seen[which]++;
        if (rem.desc[i] != expect_snap[which]) {
            fprintf(stderr, "FAIL: REMOVED '%s' carries a snapshot other than its frozen current one\n", removed_name[which]);
            failures++;
        }
    }
    for (int k = 0; k < 3; k++) {
        if (seen[k] != 1) { fprintf(stderr, "FAIL: REMOVED '%s' seen %d time(s), expected exactly 1\n", removed_name[k], seen[k]); failures++; }
    }
    MOQ_TEST_CHECK_EQ_INT(foreign, 0);
    MOQ_TEST_CHECK_EQ_INT(other, 0);
    MOQ_TEST_CHECK_EQ_INT(rem.n, 3);
    /* Both of v's snapshots stay readable and byte-identical to their frozen
     * images after removal; the getter still returns generation 2. */
    MOQ_TEST_CHECK(is_old(read_gen(g1)) && is_new(read_gen(g2)));
    MOQ_TEST_CHECK(moq_media_track_desc_get(inv.track[iv]) == g2);
    MOQ_TEST_CHECK(immutable_diff(&img_g1.d, g1) == NULL);
    MOQ_TEST_CHECK(immutable_diff(&img_g2.d, g2) == NULL);
    MOQ_TEST_CHECK(immutable_diff(g1, g2) == NULL);
    rig_close(&g);   /* exact cleanup: balance asserted inside */
}

/* -- control: the synthesized PARSE_DROP carries the poll-time snapshot ------ */
/* A malformed SAP timeline record on `sap` is pushed BEFORE the conversion and
 * its coalesced diagnostic is polled AFTER it. The diagnostic is synthesized at
 * poll time (it never passes through rq_push), so it must carry the CURRENT
 * (generation 2) snapshot; `sap`'s retained generation-1 pointer, taken from
 * its ordinary rq_push ADDED, must still read generation 1. */
static void mode_parse_drop(void)
{
    rig_t g; if (!rig_open(&g)) return;
    moq_media_receiver_test_ingest(g.r, 0, 0, g_live, strlen(g_live));
    /* Ordering: drain discovery (to learn the `sap` handle and keep its
     * generation-1 pointer), push the malformed object, convert, then poll.
     * The queued-ADDED-versus-UPDATED ordering is proved by mode_delayed;
     * here the subject is the synthesized PARSE_DROP path alone. */
    inventory_t disc; drain_all(g.r, &disc);
    int idx[K_TRACKS] = { -1, -1, -1, -1, -1 };
    if (expect_discovery(&disc, 0, idx) < 0) { rig_close(&g); return; }
    /* The SAP timeline's intake parses a JSON record; a payload that is not
     * JSON is the same class of malformed object the timeline-loss fixture
     * uses, and it is coalesced into one PARSE_DROP for the track. The intake
     * parses only objects of a track the app asked for, so that intent is
     * recorded exactly as a subscribe would. */
    moq_media_track_t *tsap = disc.track[idx[3]];
    const moq_media_track_desc_t *sap_g1 = disc.desc[idx[3]];
    moq_media_receiver_test_mark_desired(g.r, tsap);
    static const uint8_t garbage[] = "not a sap record";
    moq_media_receiver_test_push_object(g.r, tsap, 0, 0, MOQ_OBJECT_NORMAL, false, false,
                                        garbage, sizeof(garbage) - 1, NULL, 0);
    /* Convert, then poll: the UPDATED events (queued) precede the synthesized
     * PARSE_DROP (only surfaced once the ordinary queue is empty). */
    moq_media_receiver_test_ingest(g.r, 1, 0, g_vod, strlen(g_vod));
    inventory_t inv; drain_all(g.r, &inv);
    print_inventory("parse-drop drain", &inv);
    int drops = 0, updates = 0, other = 0;
    for (int i = 0; i < inv.n; i++) {
        if (inv.kind[i] == MOQ_MEDIA_TRACK_UPDATED) { updates++; continue; }
        if (inv.kind[i] == MOQ_MEDIA_TRACK_PARSE_DROP) {
            drops++;
            MOQ_TEST_CHECK(inv.track[i] == tsap);
            MOQ_TEST_CHECK(inv.desc[i] != NULL);
            MOQ_TEST_CHECK(inv.desc[i] == moq_media_track_desc_get(tsap));   /* poll-time current */
            MOQ_TEST_CHECK(inv.desc[i] != sap_g1);
            if (inv.desc[i]) MOQ_TEST_CHECK(is_new(read_gen(inv.desc[i])));  /* a NULL is a named failure above, never a crash */
            continue;
        }
        other++;
    }
    MOQ_TEST_CHECK_EQ_INT(updates, K_TRACKS);
    MOQ_TEST_CHECK_EQ_INT(drops, 1);
    MOQ_TEST_CHECK_EQ_INT(other, 0);
    MOQ_TEST_CHECK(is_old(read_gen(sap_g1)));   /* the retained generation-1 pointer */
    rig_close(&g);
}

int main(int argc, char **argv)
{
    const char *mode = argc >= 3 && !strcmp(argv[1], "--mode") ? argv[2] : NULL;
    if (!mode) { fprintf(stderr, "usage: %s --mode interleaving|delayed|duplicate|initial-vod|removal|parse-drop\n", argv[0]); return 2; }
    if (!strcmp(mode, "interleaving")) mode_interleaving();
    else if (!strcmp(mode, "delayed")) mode_delayed();
    else if (!strcmp(mode, "duplicate")) mode_duplicate();
    else if (!strcmp(mode, "initial-vod")) mode_initial_vod();
    else if (!strcmp(mode, "removal")) mode_removal();
    else if (!strcmp(mode, "parse-drop")) mode_parse_drop();
    else { fprintf(stderr, "unknown mode %s\n", mode); return 2; }
    char name[80]; snprintf(name, sizeof(name), "media_receiver_desc_generation_%s", mode);
    MOQ_TEST_PASS(name);
    if (failures) fprintf(stderr, "%s: %d failure(s)\n", name, failures);
    return failures ? 1 : 0;
}
