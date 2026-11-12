/* The receiver on draft 22 with LOC-05: the effective Codec String (the
 * object's own, else its track's) in the object's codec_string block, its
 * borrowed backing through cleanup, the explicit-LOC-05 output floor beside
 * AUTO's floor through initiating_request_id, and a malformed Codec String
 * as a parse drop.
 * Reuses the real ACK/session harness with the companion named here. */
#include <moq/media_object.h>

static moq_media_companion_profile_t g_d22_companion =
    MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
#define ACK_FAILURE_D22_COMPANION g_d22_companion
#define RECEIVER_METADATA_COPY_NO_MAIN
#include "test_media_receiver_metadata_copy.c"

/* The object through initiating_request_id, declared on its own: the
 * complete layout an older caller allocates. */
typedef struct through_origin_object {
    uint8_t bytes[MEDIA_OBJECT_ORIGIN_SIZE];
} through_origin_object_t;
_Static_assert(sizeof(through_origin_object_t) ==
               offsetof(moq_media_object_t, loc_ext_present),
               "the extent through initiating_request_id ends where the LOC-05 block starts");

#define TRACK_CODEC "avc1.64001f"
#define OBJECT_CODEC "avc1.640028"

/* LOC-05 Track Properties: Timescale `scale` and the Codec String. */
static moq_rcbuf_t *track_block05(uint64_t scale)
{
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_TIMESCALE;
    loc.timescale = scale;
    moq_loc_ext_t ext;
    moq_loc_ext_init_sized(&ext, sizeof(ext));
    ext.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
    ext.codec_string = (moq_bytes_t){(const uint8_t *)TRACK_CODEC, 11};
    moq_rcbuf_t *out = NULL;
    MOQ_TEST_CHECK(moq_loc_encode_metadata_ex(moq_alloc_default(), MOQ_VERSION_DRAFT_22,
        MOQ_LOC_PROFILE_05, &loc, &ext, &out) == MOQ_OK);
    return out;
}

/* One object on `t` with raw LOC `properties`, delivered as the facade would. */
static void input_raw(fixture_t *f, moq_media_track_t *t, uint64_t group,
                      const uint8_t *properties, size_t len)
{
    static const uint8_t byte = 0x41;
    moq_sub_object_t so = {0};
    so.track = t->sub_track; so.group_id = group; so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), &byte, 1, &so.payload) == MOQ_OK);
    MOQ_TEST_CHECK(moq_rcbuf_create(moq_alloc_default(), properties, len, &so.properties) == MOQ_OK);
    receiver_on_media_object(f->r, t, &so, now(f));
}

/* Timestamp 2000, marking I=1, and optionally the object's own Codec String. */
static const uint8_t plain_object[] = {0x09, 0x01, 0x20, 0x07, 0x87, 0xd0};
static const uint8_t codec_object[] = {0x09, 0x01, 0x20, 0x07, 0x87, 0xd0,
                                       0x01, 0x0b, 'a', 'v', 'c', '1', '.', '6', '4',
                                       '0', '0', '2', '8'};

static bool within(moq_bytes_t span, const moq_rcbuf_t *owner)
{
    const uint8_t *p = moq_rcbuf_data(owner);
    size_t n = moq_rcbuf_len(owner);
    return span.data && (uintptr_t)span.data >= (uintptr_t)p &&
           (uintptr_t)span.data + span.len <= (uintptr_t)p + n;
}

static void explicit_loc05(void)
{
    g_d22_companion = MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
    fixture_t f;
    moq_rcbuf_t *block = track_block05(1000);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    input_raw(&f, f.bad, 1, plain_object, sizeof(plain_object));
    input_raw(&f, f.bad, 2, codec_object, sizeof(codec_object));
    MOQ_TEST_CHECK(f.r->obj_tail - f.r->obj_head == 2);
    /* An output through initiating_request_id is refused on the explicit
     * companion: nothing consumed or stored. */
    struct { through_origin_object_t old; uint8_t canary[64]; } small;
    memset(&small, 0xa5, sizeof(small));
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, (moq_media_object_t *)(void *)&small,
                                                  sizeof(small.old)) == MOQ_ERR_UNSUPPORTED);
    for (size_t i = 0; i < sizeof(small); i++)
        MOQ_TEST_CHECK(((const uint8_t *)&small)[i] == 0xa5);
    MOQ_TEST_CHECK(f.r->obj_tail - f.r->obj_head == 2);
    /* The full retry gets the same objects. */
    moq_media_object_t a = {0}, b = {0};
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &a, sizeof(a)) == MOQ_OK);
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &b, sizeof(b)) == MOQ_OK);
    MOQ_TEST_CHECK(a.group_id == 1 && b.group_id == 2);
    /* Object 1 inherits the track's, from its own private track copy;
     * object 2 has its own, from its properties. */
    MOQ_TEST_CHECK(a.loc_ext_present == MOQ_LOC_EXT_HAS_CODEC_STRING);
    MOQ_TEST_CHECK(a.codec_string.len == 11 && within(a.codec_string, a.track_properties_ref));
    MOQ_TEST_CHECK(b.loc_ext_present == MOQ_LOC_EXT_HAS_CODEC_STRING);
    MOQ_TEST_CHECK(b.codec_string.len == 11 && within(b.codec_string, b.properties_ref));
    MOQ_TEST_CHECK(a.metadata.loc.timescale == 1000);
    /* The spans outlive the receiver, the session and the facade borrow. */
    f.probe = NULL; f.r->ep = NULL;
    moq_media_receiver_test_destroy_with_session(f.r, client(&f), now(&f)); f.r = NULL;
    moq_endpoint_test_free_bare(f.ep); f.ep = NULL;
    moq_simpair_destroy(f.pair); f.pair = NULL;
    MOQ_TEST_CHECK(memcmp(a.codec_string.data, TRACK_CODEC, 11) == 0);
    MOQ_TEST_CHECK(memcmp(b.codec_string.data, OBJECT_CODEC, 11) == 0);
    moq_media_object_cleanup(&a); moq_media_object_cleanup(&b);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("explicit_loc05_inheritance_floor_and_lifetime");
}

static void auto_resolved(void)
{
    g_d22_companion = MOQ_MEDIA_COMPANION_AUTO;
    fixture_t f;
    moq_rcbuf_t *block = track_block05(1000);
    if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
    MOQ_TEST_CHECK(f.r->companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC);
    MOQ_TEST_CHECK(f.r->companion_auto);
    input_raw(&f, f.bad, 1, codec_object, sizeof(codec_object));
    input_raw(&f, f.bad, 2, codec_object, sizeof(codec_object));
    /* AUTO keeps the floor through initiating_request_id: an older caller
     * still receives, and nothing is written past its extent. */
    struct { through_origin_object_t old; uint8_t canary[64]; } small;
    memset(&small, 0xa5, sizeof(small));
    moq_media_object_t *as_object = (moq_media_object_t *)(void *)&small;
    moq_result_t origin_poll = moq_media_receiver_poll_object(f.r, as_object, sizeof(small.old));
    MOQ_TEST_CHECK(origin_poll == MOQ_OK);
    if (origin_poll == MOQ_OK) {
        MOQ_TEST_CHECK(as_object->struct_size == sizeof(small.old));
        moq_media_object_cleanup(as_object);
    }
    for (size_t i = 0; i < sizeof(small.canary); i++) MOQ_TEST_CHECK(small.canary[i] == 0xa5);
    moq_media_object_t full = {0};
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &full, sizeof(full)) == MOQ_OK);
    MOQ_TEST_CHECK(full.loc_ext_present == MOQ_LOC_EXT_HAS_CODEC_STRING &&
                   within(full.codec_string, full.properties_ref));
    moq_media_object_cleanup(&full);
    drain_objects(&f);
    finish(&f); moq_rcbuf_decref(block);
    MOQ_TEST_PASS("auto_loc05_keeps_origin_floor");
}

/* A malformed Codec String drops the object on LOC-05; LOC-04 skips it. */
static void malformed_codec(void)
{
    static const uint8_t bad[] = {0x09, 0x01, 0x20, 0x07, 0x87, 0xd0, 0x01, 0x02, 0xc0, 0xaf};
    const moq_media_companion_profile_t rows[] = {
        MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC, MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC,
    };
    for (size_t i = 0; i < 2; i++) {
        g_d22_companion = rows[i];
        fixture_t f;
        if (!metadata_start(&f, NULL)) { finish(&f); continue; }
        uint64_t drops = f.r->stats.parse_drops;
        input_raw(&f, f.bad, 1, bad, sizeof(bad));
        bool loc05 = rows[i] == MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
        MOQ_TEST_CHECK((f.r->obj_tail - f.r->obj_head) == (loc05 ? 0u : 1u));
        MOQ_TEST_CHECK(f.r->stats.parse_drops == drops + (loc05 ? 1u : 0u));
        moq_media_object_t out = {0};
        if (!loc05) {
            MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &out, sizeof(out)) == MOQ_OK);
            MOQ_TEST_CHECK(out.loc_ext_present == 0 && out.codec_string.data == NULL);
            moq_media_object_cleanup(&out);
        }
        finish(&f);
    }
    g_d22_companion = MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
    MOQ_TEST_PASS("malformed_codec_drop_on_loc05_only");
}

/* LOC-05 Track Properties: Video Config b2 05 06 and optionally a Codec
 * String. */
static moq_rcbuf_t *track_block_config(const char *codec)
{
    static const uint8_t config[] = {0xb2, 0x05, 0x06};
    moq_loc_metadata_t loc;
    moq_loc_metadata_init_sized(&loc, sizeof(loc));
    loc.present = MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
    loc.video_config = (moq_bytes_t){config, sizeof(config)};
    moq_loc_ext_t ext;
    moq_loc_ext_init_sized(&ext, sizeof(ext));
    if (codec) {
        ext.present = MOQ_LOC_EXT_HAS_CODEC_STRING;
        ext.codec_string = (moq_bytes_t){(const uint8_t *)codec, strlen(codec)};
    }
    moq_rcbuf_t *out = NULL;
    MOQ_TEST_CHECK(moq_loc_encode_metadata_ex(moq_alloc_default(), MOQ_VERSION_DRAFT_22,
        MOQ_LOC_PROFILE_05, &loc, codec ? &ext : NULL, &out) == MOQ_OK);
    return out;
}

static bool track_config_of(const moq_media_object_t *o)
{
    return (o->metadata.loc.present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG) &&
           o->metadata.loc.video_config.len == 3 &&
           within(o->metadata.loc.video_config, o->track_properties_ref) &&
           o->metadata.loc.video_config.data[0] == 0xb2;
}

/* The track's configuration belongs to the track's codec: its own Codec
 * String, else the catalog entry's ("av01" here). A delivered object pairs
 * its codec with a configuration declared for it, never another codec's
 * (draft-ietf-moq-loc-05 2.3.1.3, 2.3.2.1). */
static void track_configuration_pairing(void)
{
    static const uint8_t catalog_object[] = {0x09, 0x01, 0x20, 0x07, 0x87, 0xd0,
                                             0x01, 0x04, 'a', 'v', '0', '1'};
    g_d22_companion = MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
    for (int named = 0; named < 2; named++) {
        fixture_t f;
        moq_rcbuf_t *block = track_block_config(named ? TRACK_CODEC : NULL);
        if (!metadata_start(&f, block)) { finish(&f); moq_rcbuf_decref(block); return; }
        input_raw(&f, f.bad, 1, plain_object, sizeof(plain_object));
        input_raw(&f, f.bad, 2, codec_object, sizeof(codec_object));
        input_raw(&f, f.bad, 3, catalog_object, sizeof(catalog_object));
        moq_media_object_t o[3];
        memset(o, 0, sizeof(o));
        for (size_t i = 0; i < 3; i++)
            MOQ_TEST_CHECK(moq_media_receiver_poll_object(f.r, &o[i], sizeof(o[i])) == MOQ_OK);
        /* No Codec String of its own: the track's codec and configuration. */
        MOQ_TEST_CHECK(track_config_of(&o[0]));
        /* avc1.640028 is neither the track's nor the catalog's codec. */
        MOQ_TEST_CHECK(o[1].codec_string.len == 11 &&
                       !(o[1].metadata.loc.present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG));
        /* av01 is the track's codec only when the track names none. */
        if (named)
            MOQ_TEST_CHECK(!(o[2].metadata.loc.present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG));
        else
            MOQ_TEST_CHECK(track_config_of(&o[2]));
        for (size_t i = 0; i < 3; i++) moq_media_object_cleanup(&o[i]);
        finish(&f); moq_rcbuf_decref(block);
    }
    MOQ_TEST_PASS("track_configuration_stays_with_its_codec");
}

int main(void)
{
    explicit_loc05();
    auto_resolved();
    malformed_codec();
    track_configuration_pairing();
    fprintf(stderr, "receiver loc05: %d failures\n", failures);
    return failures ? 1 : 0;
}
