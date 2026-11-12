/* The media-object parser on D22_MSF01_LOC05_PUBLIC: the effective Codec
 * String (object's own, else the track's), its borrowed backing, the output
 * floor, the track record's checks, and LOC04/legacy staying as they were.
 * Property bytes are literal spec vectors. */
#include <moq/media_object.h>
#include <moq/loc.h>
#include "../../../tests/unit/test_support.h"
#include "media_object_layout_probe.h"
#include <stdlib.h>
#include <string.h>

#define LOC05 MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC
#define LOC04 MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC

typedef struct {
    moq_rcbuf_t *payload;
    moq_rcbuf_t *properties;
    moq_media_track_info_t track;
    moq_media_object_input_t in;
} fixture_t;

static bool fixture_init(fixture_t *f, moq_version_t version,
                         const uint8_t *props, size_t props_len)
{
    memset(f, 0, sizeof(*f));
    static const uint8_t byte = 0x41;
    if (moq_rcbuf_create(moq_alloc_default(), &byte, 1, &f->payload) != MOQ_OK)
        return false;
    if (props_len &&
        moq_rcbuf_create(moq_alloc_default(), props, props_len, &f->properties) != MOQ_OK)
        return false;
    moq_media_track_info_init_sized(&f->track, sizeof(f->track));
    f->track.media_type = MOQ_MEDIA_TYPE_VIDEO;
    f->track.packaging = MOQ_MEDIA_PACKAGING_RAW;
    f->track.transport_version = version;
    moq_media_object_input_init(&f->in);
    f->in.payload = f->payload;
    f->in.properties = f->properties;
    return true;
}

static void fixture_free(fixture_t *f)
{
    moq_rcbuf_decref(f->payload);
    moq_rcbuf_decref(f->properties);
}

static moq_result_t parse(fixture_t *f, moq_media_companion_profile_t companion,
                          const moq_loc_ext_t *track_ext,
                          moq_media_parsed_object_t *out, size_t out_size,
                          moq_media_drop_reason_t *reason)
{
    moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    cfg.companion_profile = companion;
    cfg.track_loc_ext = track_ext;
    return moq_media_object_parse_sized(&f->track, &f->in, &cfg, NULL, 0,
                                        out, out_size, reason);
}

static moq_loc_ext_t track_codec(const char *text, size_t len)
{
    moq_loc_ext_t e;
    moq_loc_ext_init_sized(&e, sizeof(e));
    e.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
    e.codec_string = (moq_bytes_t){(const uint8_t *)text, len};
    return e;
}

/* Timestamp 7 then Codec String "avc1.640028" (a mid-track change). */
static const uint8_t object_codec[] = {0x10, 0x07, 0x01, 0x0b, 'a', 'v', 'c', '1',
                                       '.', '6', '4', '0', '0', '2', '8'};
static const uint8_t object_plain[] = {0x10, 0x07};

static int effective_codec(void)
{
    int failures = 0;
    static const char track_text[] = "avc1.64001f";
    moq_loc_ext_t track = track_codec(track_text, 11);
    fixture_t f;
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_codec, sizeof(object_codec)));
    moq_media_parsed_object_t out;
    moq_media_drop_reason_t reason = 0;
    /* The object's own wins, borrowing its property bytes. */
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, &track, &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(out.loc_ext_present == MOQ_LOC_EXT_HAS_CODEC_STRING);
    MOQ_TEST_CHECK(out.codec_string.data == moq_rcbuf_data(f.properties) + 4);
    MOQ_TEST_CHECK_EQ_SIZE(out.codec_string.len, 11);
    MOQ_TEST_CHECK_EQ_SIZE(out.struct_size, sizeof(out));
    fixture_free(&f);

    /* Without one, the track's, borrowing the caller's track backing. */
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_plain, sizeof(object_plain)));
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, &track, &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(out.loc_ext_present == MOQ_LOC_EXT_HAS_CODEC_STRING);
    MOQ_TEST_CHECK(out.codec_string.data == (const uint8_t *)track_text);
    MOQ_TEST_CHECK(out.metadata.loc.timestamp == 7);
    /* Neither: absent, never a remembered value. */
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, NULL, &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(out.loc_ext_present == 0 && out.codec_string.len == 0);
    fixture_free(&f);

    /* Without properties at all, the track's still applies. */
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, NULL, 0));
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, &track, &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(out.codec_string.data == (const uint8_t *)track_text);
    fixture_free(&f);
    return failures;
}

static int floors_and_old_callers(void)
{
    int failures = 0;
    fixture_t f;
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_codec, sizeof(object_codec)));
    /* LOC05 needs the codec block: an output through metadata is refused before
     * any store, drop reason included. */
    struct { media_through_metadata_parsed_t old; uint8_t canary[32]; } small;
    memset(&small, 0xa5, sizeof(small));
    moq_media_drop_reason_t reason = 0x77;
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, NULL, (moq_media_parsed_object_t *)(void *)&small,
                                sizeof(small.old), &reason), MOQ_ERR_UNSUPPORTED);
    MOQ_TEST_CHECK(reason == 0x77);
    for (size_t i = 0; i < sizeof(small); i++)
        MOQ_TEST_CHECK(((const uint8_t *)&small)[i] == 0xa5);
    /* The same output through metadata on LOC04 works, writes nothing past its
     * extent, and the Codec String stays an unknown property there. */
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC04, NULL, (moq_media_parsed_object_t *)(void *)&small,
                                sizeof(small.old), &reason), MOQ_OK);
    MOQ_TEST_CHECK_EQ_SIZE(small.old.base.struct_size, sizeof(small.old));
    MOQ_TEST_CHECK(small.old.metadata.loc.timestamp == 7);
    for (size_t i = 0; i < sizeof(small.canary); i++) MOQ_TEST_CHECK(small.canary[i] == 0xa5);
    /* On LOC04 a full output reports no Codec String. */
    moq_media_parsed_object_t out;
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC04, NULL, &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(out.loc_ext_present == 0 && out.codec_string.data == NULL);

    /* An older caller's config ends before track_loc_ext: the pointer field
     * it does not own (and its own padding) is never read. */
    struct { media_old_parse_cfg_t old; const void *garbage; } cfg;
    memset(&cfg, 0xa5, sizeof(cfg));
    cfg.old.struct_size = (uint32_t)sizeof(cfg.old);
    cfg.old.companion_profile = LOC05;
    cfg.old.track_loc_metadata = NULL;
    cfg.old.mpeg2ts_packet_size = 0;
    cfg.garbage = (const void *)(uintptr_t)0x10;
    MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&f.track, &f.in,
        (const moq_media_object_parse_cfg_t *)(const void *)&cfg, NULL, 0,
        &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(out.codec_string.data == moq_rcbuf_data(f.properties) + 4);
    fixture_free(&f);

    /* A struct_size one byte short of the whole pointer leaves the track
     * Codec String absent; the current caller's is inherited. */
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_plain, sizeof(object_plain)));
    static const char text[] = "av01";
    moq_loc_ext_t e = track_codec(text, 4);
    moq_media_object_parse_cfg_t cur = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    cur.companion_profile = LOC05;
    cur.track_loc_ext = &e;
    cur.struct_size = (uint32_t)(offsetof(moq_media_object_parse_cfg_t, track_loc_ext) +
                                 sizeof(cur.track_loc_ext) - 1);
    MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&f.track, &f.in, &cur, NULL, 0,
        &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(out.loc_ext_present == 0 && out.codec_string.data == NULL);
    cur.struct_size = (uint32_t)sizeof(cur);
    MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&f.track, &f.in, &cur, NULL, 0,
        &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(out.loc_ext_present == MOQ_LOC_EXT_HAS_CODEC_STRING &&
                   out.codec_string.data == (const uint8_t *)text);
    fixture_free(&f);
    return failures;
}

static int track_record_checks(void)
{
    int failures = 0;
    fixture_t f;
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_plain, sizeof(object_plain)));
    moq_media_parsed_object_t out;
    moq_media_drop_reason_t reason = 0;
    moq_loc_ext_t e = track_codec("opus", 4);
    e.struct_size = MOQ_LOC_EXT_V0_SIZE - 1;
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, &e, &out, sizeof(out), &reason), MOQ_ERR_INVAL);
    e = track_codec("opus", 4);
    e.present |= 0x2;
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, &e, &out, sizeof(out), &reason), MOQ_ERR_INVAL);
    static const struct { const char *text; size_t len; } unusable[] = {
        {"", 0}, {"  ", 2}, {"op\0us", 5}, {"\xed\xa0\x80", 3},
    };
    for (size_t i = 0; i < sizeof(unusable) / sizeof(unusable[0]); i++) {
        e = track_codec(unusable[i].text, unusable[i].len);
        MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, &e, &out, sizeof(out), &reason), MOQ_ERR_INVAL);
    }
    e = track_codec(NULL, 4);
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, &e, &out, sizeof(out), &reason), MOQ_ERR_INVAL);
    /* A present track Codec String on a companion without it is refused,
     * never ignored; an empty record is fine. */
    e = track_codec("opus", 4);
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC04, &e, &out, sizeof(out), &reason),
                          MOQ_ERR_UNSUPPORTED);
    moq_loc_ext_t none;
    moq_loc_ext_init_sized(&none, sizeof(none));
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC04, &none, &out, sizeof(out), &reason), MOQ_OK);
    fixture_free(&f);
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_16, NULL, 0));
    MOQ_TEST_CHECK_EQ_INT(parse(&f, MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, &e, &out,
                                sizeof(out), &reason), MOQ_ERR_UNSUPPORTED);
    /* LOC05 is draft 22 only. */
    MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, NULL, &out, sizeof(out), &reason),
                          MOQ_ERR_UNSUPPORTED);
    fixture_free(&f);
    return failures;
}

static int malformed_objects(void)
{
    int failures = 0;
    static const uint8_t bad_utf8[] = {0x10, 0x07, 0x01, 0x02, 0xc0, 0xaf};
    static const uint8_t empty[] = {0x10, 0x07, 0x01, 0x00};
    static const uint8_t repeated[] = {0x10, 0x07, 0x01, 0x01, 'a', 0x00, 0x01, 'b'};
    fixture_t f;
    moq_media_parsed_object_t out;
    moq_media_drop_reason_t reason;
    const struct { const uint8_t *bytes; size_t len; moq_result_t loc05; } rows[] = {
        {bad_utf8, sizeof(bad_utf8), MOQ_ERR_PROTO},
        {empty, sizeof(empty), MOQ_ERR_PROTO},
        {repeated, sizeof(repeated), MOQ_ERR_UNSUPPORTED},
    };
    for (size_t i = 0; i < 3; i++) {
        MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, rows[i].bytes, rows[i].len));
        reason = 0;
        MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC05, NULL, &out, sizeof(out), &reason),
                              rows[i].loc05);
        if (rows[i].loc05 == MOQ_ERR_PROTO)
            MOQ_TEST_CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_LOC);
        /* LOC04 skips the unknown 0x11. */
        MOQ_TEST_CHECK_EQ_INT(parse(&f, LOC04, NULL, &out, sizeof(out), &reason), MOQ_OK);
        MOQ_TEST_CHECK(out.metadata.loc.timestamp == 7 && out.loc_ext_present == 0);
        fixture_free(&f);
    }
    return failures;
}

/* Timestamp 7 then Codec String "hev1.1.6.L93", and the same codec with the
 * object's own Video Config b2 05 06 ahead of them. */
static const uint8_t object_hevc[] = {0x10, 0x07, 0x01, 0x0c, 'h', 'e', 'v', '1', '.',
                                      '1', '.', '6', '.', 'L', '9', '3'};
static const uint8_t object_hevc_config[] = {0x0d, 0x03, 0xb2, 0x05, 0x06, 0x03, 0x07,
                                             0x01, 0x0c, 'h', 'e', 'v', '1', '.', '1',
                                             '.', '6', '.', 'L', '9', '3'};
/* Timestamp 7 then Codec String "avc1.64001f", the catalog's below. */
static const uint8_t object_catalog[] = {0x10, 0x07, 0x01, 0x0b, 'a', 'v', 'c', '1',
                                         '.', '6', '4', '0', '0', '1', 'f'};

#define CATALOG "avc1.64001f"
#define HEVC "hev1.1.6.L93"
static const uint8_t track_config[] = {0xa1, 0x01, 0x02};

static moq_result_t parse_track(fixture_t *f, moq_media_companion_profile_t companion,
                                const moq_loc_metadata_t *track_metadata,
                                const moq_loc_ext_t *track_ext, const char *catalog,
                                moq_media_parsed_object_t *out)
{
    moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    cfg.companion_profile = companion;
    cfg.track_loc_metadata = track_metadata;
    cfg.track_loc_ext = track_ext;
    if (catalog)
        cfg.catalog_codec = (moq_bytes_t){(const uint8_t *)catalog, strlen(catalog)};
    moq_media_drop_reason_t reason = 0;
    return moq_media_object_parse_sized(&f->track, &f->in, &cfg, NULL, 0, out,
                                        sizeof(*out), &reason);
}

static bool config_is_track(const moq_media_parsed_object_t *out)
{
    return (out->metadata.loc.present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG) &&
           out->metadata.loc.video_config.data == track_config &&
           out->metadata.loc.video_config.len == sizeof(track_config);
}

static bool config_absent(const moq_media_parsed_object_t *out)
{
    return !(out->metadata.loc.present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG);
}

/* A Codec String and a Video Config are the two halves of one decoder
 * configuration (draft-ietf-moq-loc-05 2.3.1.3, 2.3.2.1). The track's
 * configuration belongs to the track's codec: its own Codec String, else the
 * catalog's. An object naming that codec, or none, inherits it; an object
 * naming another codec, or one the track's cannot be compared with, does not.
 * Timescale is inherited regardless. */
static int track_configuration_pairing(void)
{
    int failures = 0;
    moq_loc_metadata_t meta;
    moq_loc_metadata_init_sized(&meta, sizeof(meta));
    meta.present = MOQ_LOC_METADATA_HAS_TIMESCALE | MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
    meta.timescale = 90000;
    meta.video_config = (moq_bytes_t){track_config, sizeof(track_config)};
    static const char named_text[] = "avc1.640028";
    moq_loc_ext_t named = track_codec(named_text, 11);
    moq_media_parsed_object_t out;
    fixture_t f;

    /* The track names avc1.640028. */
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_hevc, sizeof(object_hevc)));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, &named, CATALOG, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_absent(&out));
    MOQ_TEST_CHECK(out.metadata.loc.present & MOQ_LOC_METADATA_HAS_TIMESCALE);
    MOQ_TEST_CHECK(out.metadata.loc.timescale == 90000);
    MOQ_TEST_CHECK(out.codec_string.len == 12 &&
                   out.codec_string.data == moq_rcbuf_data(f.properties) + 4);
    /* The track's own Codec String decides, not the catalog's. */
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, &named, HEVC, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_absent(&out));
    fixture_free(&f);
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_codec, sizeof(object_codec)));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, &named, CATALOG, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_is_track(&out));
    fixture_free(&f);
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_plain, sizeof(object_plain)));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, &named, CATALOG, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_is_track(&out));
    MOQ_TEST_CHECK(out.codec_string.data == (const uint8_t *)named_text);
    fixture_free(&f);

    /* The track names no codec: the catalog's is the track's. */
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_catalog, sizeof(object_catalog)));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, NULL, CATALOG, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_is_track(&out));
    /* Without a catalog codec the pair cannot be shown. */
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, NULL, NULL, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_absent(&out));
    MOQ_TEST_CHECK(out.metadata.loc.timescale == 90000);
    fixture_free(&f);
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_hevc, sizeof(object_hevc)));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, NULL, CATALOG, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_absent(&out));
    fixture_free(&f);
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_plain, sizeof(object_plain)));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, NULL, CATALOG, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_is_track(&out));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, NULL, NULL, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_is_track(&out));
    fixture_free(&f);

    /* The object's own configuration is its own, whatever it names. */
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_hevc_config,
                                sizeof(object_hevc_config)));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC05, &meta, &named, CATALOG, &out), MOQ_OK);
    MOQ_TEST_CHECK((out.metadata.loc.present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG) &&
                   out.metadata.loc.video_config.len == 3 &&
                   out.metadata.loc.video_config.data == moq_rcbuf_data(f.properties) + 2);
    fixture_free(&f);

    /* LOC04 skips 0x11, so every object decodes under the catalog codec and
     * inherits whatever catalog_codec says. */
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_hevc, sizeof(object_hevc)));
    MOQ_TEST_CHECK_EQ_INT(parse_track(&f, LOC04, &meta, NULL, HEVC, &out), MOQ_OK);
    MOQ_TEST_CHECK(config_is_track(&out));
    fixture_free(&f);
    return failures;
}

/* catalog_codec is read only when struct_size covers all of it, and a
 * nonempty one needs data. */
static int catalog_codec_field(void)
{
    int failures = 0;
    moq_loc_metadata_t meta;
    moq_loc_metadata_init_sized(&meta, sizeof(meta));
    meta.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
    meta.video_config = (moq_bytes_t){track_config, sizeof(track_config)};
    moq_media_parsed_object_t out;
    moq_media_drop_reason_t reason = 0;
    fixture_t f;
    MOQ_TEST_CHECK(fixture_init(&f, MOQ_VERSION_DRAFT_22, object_catalog, sizeof(object_catalog)));
    moq_media_object_parse_cfg_t cfg = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    cfg.companion_profile = LOC05;
    cfg.track_loc_metadata = &meta;
    cfg.catalog_codec = (moq_bytes_t){(const uint8_t *)CATALOG, 11};
    cfg.struct_size = (uint32_t)(offsetof(moq_media_object_parse_cfg_t, catalog_codec) +
                                 sizeof(cfg.catalog_codec) - 1);
    MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&f.track, &f.in, &cfg, NULL, 0,
        &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(config_absent(&out));
    cfg.struct_size = (uint32_t)sizeof(cfg);
    MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&f.track, &f.in, &cfg, NULL, 0,
        &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(config_is_track(&out));
    /* Nonempty without data is INVAL on every companion, output untouched;
     * empty is absent. */
    memset(&out, 0xa5, sizeof(out));
    cfg.catalog_codec = (moq_bytes_t){NULL, 11};
    MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&f.track, &f.in, &cfg, NULL, 0,
        &out, sizeof(out), &reason), MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(((const uint8_t *)&out)[0] == 0xa5);
    cfg.companion_profile = LOC04;
    MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&f.track, &f.in, &cfg, NULL, 0,
        &out, sizeof(out), &reason), MOQ_ERR_INVAL);
    cfg.companion_profile = LOC05;
    cfg.catalog_codec = (moq_bytes_t){NULL, 0};
    MOQ_TEST_CHECK_EQ_INT(moq_media_object_parse_sized(&f.track, &f.in, &cfg, NULL, 0,
        &out, sizeof(out), &reason), MOQ_OK);
    MOQ_TEST_CHECK(config_absent(&out));
    fixture_free(&f);
    return failures;
}

int main(void)
{
    static const struct { const char *name; int (*run)(void); } cases[] = {
        {"effective_codec", effective_codec},
        {"floors_and_old_callers", floors_and_old_callers},
        {"track_record_checks", track_record_checks},
        {"malformed_objects", malformed_objects},
        {"track_configuration_pairing", track_configuration_pairing},
        {"catalog_codec_field", catalog_codec_field},
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
