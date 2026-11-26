/*
 * The media object normalizer on "mpeg2ts" objects (draft-gregoire-moq-msfts).
 * Built with and without moq::mpeg2ts: without it, the mpeg2ts packaging is
 * refused as unsupported by this build.
 */

#include <moq/media_object.h>
#include <moq/rcbuf.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ts_builders.h"
#include "ts_layout_probe.h"

static int failures = 0;
int media_object_legacy_client(void);

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static void *t_alloc(size_t n, void *ctx) { (void)ctx; return malloc(n ? n : 1); }
static void *t_realloc(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    return realloc(p, n ? n : 1);
}
static void t_free(void *p, size_t n, void *ctx) { (void)n; (void)ctx; free(p); }
static moq_alloc_t g_alloc = {
    .ctx = NULL, .alloc = t_alloc, .realloc = t_realloc, .free = t_free,
};

static moq_rcbuf_t *buf(const uint8_t *d, size_t n)
{
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(&g_alloc, d, n, &b) != MOQ_OK) {
        fprintf(stderr, "FAIL: rcbuf\n");
        exit(1);
    }
    return b;
}

/* An MPEG2TS track of `packet_size` octet packets. */
static void track(moq_media_track_info_t *t, moq_media_object_parse_cfg_t *cfg,
                  uint32_t packet_size,
                  moq_media_type_t mt)
{
    moq_media_track_info_init_sized(t, sizeof(*t));
    t->packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    *cfg = (moq_media_object_parse_cfg_t)MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    cfg->mpeg2ts_packet_size = packet_size;
    t->media_type = mt;
}

/* PAT, PMT, an audio PES, a video PES at a random access point with PCR, and
 * a continuation packet. */
static size_t build_object(uint8_t *out, bool prefix, bool rap)
{
    size_t o = 0;
    uint8_t sec[256], pl[188], pes[64];
    tb_prog_t tp[] = { { 1, 0x0100 } };
    size_t n = tb_pat(sec, 1, 0, tp, 1);
    size_t pn = tb_psi_payload(pl, sec, n);
    tb_pkt_t p0 = { .pid = 0, .pusi = true, .payload = pl, .payload_len = pn,
                    .prefix = prefix };
    o += tb_packet(out + o, &p0);
    tb_es_t es[] = { { 0x1B, 0x0101, NULL, 0 }, { 0x0F, 0x0102, NULL, 0 } };
    n = tb_pmt(sec, 1, 0, 0x0101, NULL, 0, es, 2);
    pn = tb_psi_payload(pl, sec, n);
    tb_pkt_t p1 = { .pid = 0x0100, .pusi = true, .payload = pl,
                    .payload_len = pn, .prefix = prefix };
    o += tb_packet(out + o, &p1);
    n = tb_pes(pes, 0xC0, true, 180000, false, 0);
    tb_pkt_t p2 = { .pid = 0x0102, .pusi = true, .payload = pes,
                    .payload_len = n, .prefix = prefix };
    o += tb_packet(out + o, &p2);
    /* PTS 2 s, DTS 2 s - 100 ms. */
    n = tb_pes(pes, 0xE0, true, 180000 + 9000, true, 180000);
    tb_pkt_t p3 = { .pid = 0x0101, .pusi = true, .rai = rap, .pcr = true,
                    .pcr_base = 179000, .payload = pes, .payload_len = n,
                    .prefix = prefix };
    o += tb_packet(out + o, &p3);
    tb_pkt_t p4 = { .pid = 0x0101, .cc = 1, .prefix = prefix };
    o += tb_packet(out + o, &p4);
    return o;
}

static moq_result_t run(const moq_media_track_info_t *t,
                        const moq_media_object_parse_cfg_t *cfg, const uint8_t *d,
                        size_t n, moq_media_parsed_object_t *out,
                        moq_media_drop_reason_t *why)
{
    moq_rcbuf_t *b = buf(d, n);
    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    in.status = MOQ_OBJECT_NORMAL;
    in.payload = b;
    moq_media_parsed_object_init(out);
    moq_result_t rc = moq_media_object_parse_sized(t, &in, cfg, NULL, 0, out, sizeof(*out), why);
    moq_rcbuf_decref(b);
    return rc;
}

#ifdef MOQ_TEST_HAVE_MPEG2TS

static void test_timing_and_keyframe(void)
{
    for (int pf = 0; pf < 2; pf++) {
        bool prefix = pf == 1;
        uint8_t obj[192 * 5];
        size_t n = build_object(obj, prefix, true);
        moq_media_track_info_t t;
    moq_media_object_parse_cfg_t cfg;
        track(&t, &cfg, prefix ? 192 : 188, MOQ_MEDIA_TYPE_VIDEO);
        moq_media_parsed_object_t o;
        moq_media_drop_reason_t why = 0;
        CHECK(run(&t, &cfg, obj, n, &o, &why) == MOQ_OK);
        CHECK(o.packaging == t.packaging);
        CHECK(o.keyframe);
        /* Timing from the PES at the random access point, not the audio PES
         * before it. */
        CHECK(o.track_id == 0x0101);
        CHECK(o.decode_time_us == 2000000);
        CHECK(o.presentation_time_us == 2100000);
        CHECK(o.composition_offset_us == 100000);
        CHECK(!o.has_capture_time);
        CHECK(o.chunk_count == 5);
        CHECK(o.sap_type == MOQ_SAP_UNKNOWN);
        CHECK(o.payload.len == n);
        CHECK(o.sample_count == 0 && o.samples == NULL);
    }

    /* No random access point: not a keyframe; a video track still times
     * from the video PES, not the audio PES before it. */
    {
        uint8_t obj[188 * 5];
        size_t n = build_object(obj, false, false);
        moq_media_track_info_t t;
    moq_media_object_parse_cfg_t cfg;
        track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);
        moq_media_parsed_object_t o;
        CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_OK);
        CHECK(!o.keyframe);
        CHECK(o.track_id == 0x0101);
        CHECK(o.decode_time_us == 2000000 && o.presentation_time_us == 2100000);
        CHECK(o.composition_offset_us == 100000);
        /* An audio track times from the first PES with a PTS. */
        track(&t, &cfg, 188, MOQ_MEDIA_TYPE_AUDIO);
        CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_OK);
        CHECK(o.track_id == 0x0102);
        CHECK(o.decode_time_us == 2000000 && o.presentation_time_us == 2000000);
    }

    /* random_access_indicator on the audio PES only: a video track's Object
     * is not a keyframe. */
    {
        uint8_t obj[188 * 5];
        size_t n = build_object(obj, false, false);
        uint8_t pes[64];
        size_t pn = tb_pes(pes, 0xC0, true, 180000, false, 0);
        tb_pkt_t a = { .pid = 0x0102, .pusi = true, .rai = true,
                       .payload = pes, .payload_len = pn };
        tb_packet(obj + 2 * 188, &a);
        moq_media_track_info_t t;
    moq_media_object_parse_cfg_t cfg;
        track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);
        moq_media_parsed_object_t o;
        CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_OK);
        CHECK(!o.keyframe);
    }

    /* No PES at all: accepted, without timing (track_id 0). */
    {
        uint8_t obj[188 * 5];
        build_object(obj, false, false);
        moq_media_track_info_t t;
    moq_media_object_parse_cfg_t cfg;
        track(&t, &cfg, 188, MOQ_MEDIA_TYPE_AUDIO);
        moq_media_parsed_object_t o;
        /* Only the PAT and PMT packets. */
        CHECK(run(&t, &cfg, obj, 2 * 188, &o, NULL) == MOQ_OK);
        CHECK(o.track_id == 0);
        CHECK(o.decode_time_us == 0 && o.presentation_time_us == 0);
        CHECK(o.keyframe);                 /* audio, as for LOC and CMAF */
    }
}

/* Object Payload Format: the subscriber MUST reject an Object that is not
 * whole source packets; the sync byte must be where the packet size says. */
static void test_rejections(void)
{
    uint8_t obj[192 * 5];
    size_t n = build_object(obj, false, true);
    moq_media_track_info_t t;
    moq_media_object_parse_cfg_t cfg;
    track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);
    moq_media_parsed_object_t o;
    moq_media_drop_reason_t why = 0;

    CHECK(run(&t, &cfg, obj, n - 1, &o, &why) == MOQ_ERR_PROTO);
    CHECK(why == MOQ_MEDIA_DROP_MALFORMED_MPEG2TS);
    why = 0;
    CHECK(run(&t, &cfg, obj, 100, &o, &why) == MOQ_ERR_PROTO);
    CHECK(why == MOQ_MEDIA_DROP_MALFORMED_MPEG2TS);

    /* 188-octet packets declared as 192. */
    track(&t, &cfg, 192, MOQ_MEDIA_TYPE_VIDEO);
    why = 0;
    CHECK(run(&t, &cfg, obj, n, &o, &why) == MOQ_ERR_PROTO);
    CHECK(why == MOQ_MEDIA_DROP_MALFORMED_MPEG2TS);

    /* Sync byte. */
    track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);
    obj[188 * 2] = 0x48;
    why = 0;
    CHECK(run(&t, &cfg, obj, n, &o, &why) == MOQ_ERR_PROTO);
    CHECK(why == MOQ_MEDIA_DROP_MALFORMED_MPEG2TS);

    /* An empty normal Object. */
    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    in.status = MOQ_OBJECT_NORMAL;
    moq_rcbuf_t *empty = NULL;
    CHECK(moq_rcbuf_create(&g_alloc, NULL, 0, &empty) == MOQ_OK);
    in.payload = empty;
    why = 0;
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0, &o, sizeof(o), &why) == MOQ_ERR_PROTO);
    CHECK(why == MOQ_MEDIA_DROP_MALFORMED_MPEG2TS);
    moq_rcbuf_decref(empty);

    /* The packet size is 188 or 192, in a struct that holds it. */
    uint32_t bad_sizes[] = { 0, 187, 190, 204 };
    for (size_t i = 0; i < sizeof(bad_sizes) / sizeof(bad_sizes[0]); i++) {
        track(&t, &cfg, bad_sizes[i], MOQ_MEDIA_TYPE_VIDEO);
        CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_ERR_INVAL);
    }
    cfg.struct_size = (uint32_t)MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE;
    moq_media_track_info_init(&t);           /* the v0 prefix only */
    t.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    t.media_type = MOQ_MEDIA_TYPE_VIDEO;
    CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_ERR_INVAL);
    track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);

    /* Terminal statuses pass as for the other packagings. */
    moq_media_object_input_init(&in);
    in.status = MOQ_OBJECT_END_OF_GROUP;
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0, &o, sizeof(o), NULL) == MOQ_OK);
    CHECK(o.status == MOQ_OBJECT_END_OF_GROUP);
}

#else

static void test_unsupported(void)
{
    uint8_t obj[188 * 5];
    size_t n = build_object(obj, false, true);
    moq_media_track_info_t t;
    moq_media_object_parse_cfg_t cfg;
    track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);
    moq_media_parsed_object_t o;
    CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_ERR_UNSUPPORTED);
    track(&t, &cfg, 192, MOQ_MEDIA_TYPE_VIDEO);
    CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_ERR_UNSUPPORTED);
}

#endif

static void test_sized_contract(void)
{
    moq_media_track_info_t t;
    moq_media_object_parse_cfg_t cfg;
    track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);
    moq_media_object_input_t in;
    moq_media_object_input_init(&in);
    in.status = MOQ_OBJECT_END_OF_GROUP;
    union {
        moq_media_parsed_object_t aligner;
        unsigned char bytes[sizeof(ts_old_parsed_t) + 16];
    } box, before;
    memset(&box, 0xA5, sizeof(box));
    before = box;
    moq_media_drop_reason_t why = 99;
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0,
          &box.aligner, MOQ_MEDIA_PARSED_OBJECT_V0_SIZE - 1, &why) == MOQ_ERR_INVAL);
    CHECK(memcmp(&box, &before, sizeof(box)) == 0 && why == 99);
    CHECK(moq_media_object_parse(&t, &in, NULL, 0, &box.aligner, NULL) == MOQ_ERR_INVAL);
    CHECK(memcmp(&box, &before, sizeof(box)) == 0);
    CHECK(moq_media_object_parse_sized(&t, &in, NULL, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_INVAL);
    CHECK(memcmp(&box, &before, sizeof(box)) == 0 && why == 99);
    for (size_t n = MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE;
         n < offsetof(moq_media_object_parse_cfg_t, mpeg2ts_packet_size) + sizeof(uint32_t);
         n++) {
        cfg.struct_size = (uint32_t)n;
        CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0,
              &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_INVAL);
        CHECK(memcmp(&box, &before, sizeof(box)) == 0 && why == 99);
    }
    cfg = (moq_media_object_parse_cfg_t)MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    t.packaging = MOQ_MEDIA_PACKAGING_RAW;
    cfg.struct_size = (uint32_t)MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE - 1;
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_INVAL);
    CHECK(memcmp(&box, &before, sizeof(box)) == 0 && why == 99);
    cfg.struct_size = sizeof(cfg);
    cfg.companion_profile = 99;
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_INVAL);
    cfg.companion_profile = MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_UNSUPPORTED);
    cfg.companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    /* A covered track record is read: one that is not a LOC metadata record
     * is INVAL, and track timescale on the legacy companion is UNSUPPORTED,
     * both before any output store. */
    cfg.track_loc_metadata = (const moq_loc_metadata_t *)(const void *)&t;
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_INVAL);
    CHECK(memcmp(&box, &before, sizeof(box)) == 0 && why == 99);
    moq_loc_metadata_t track_loc;
    moq_loc_metadata_init_sized(&track_loc, sizeof(track_loc));
    track_loc.present = MOQ_LOC_METADATA_HAS_TIMESCALE;
    track_loc.timescale = 90000;
    cfg.track_loc_metadata = &track_loc;
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_UNSUPPORTED);
    CHECK(memcmp(&box, &before, sizeof(box)) == 0 && why == 99);
    /* A partially covered non-NULL pointer must not be consulted. */
    cfg.struct_size = (uint32_t)(offsetof(moq_media_object_parse_cfg_t,
                               track_loc_metadata) + sizeof(void *) - 1);
    CHECK(moq_media_object_parse_sized(&t, &in, &cfg, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_OK);
    CHECK(box.aligner.struct_size == sizeof(ts_old_parsed_t) && why == 0);
    for (size_t i = sizeof(ts_old_parsed_t); i < sizeof(box); i++)
        CHECK(box.bytes[i] == 0xA5);
    /* NULL config and the unchanged legacy entry have the same RAW result. */
    moq_media_parsed_object_t legacy;
    CHECK(moq_media_object_parse(&t, &in, NULL, 0, &legacy, NULL) == MOQ_OK);
    CHECK(moq_media_object_parse_sized(&t, &in, NULL, NULL, 0,
          &box.aligner, sizeof(box), NULL) == MOQ_OK);
    CHECK(legacy.struct_size == MOQ_MEDIA_PARSED_OBJECT_V0_SIZE);
    CHECK(box.aligner.struct_size == sizeof(moq_media_parsed_object_t));
    CHECK(memcmp((const unsigned char *)&legacy + sizeof(uint32_t),
                 (const unsigned char *)&box.aligner + sizeof(uint32_t),
                 MOQ_MEDIA_PARSED_OBJECT_V0_SIZE - sizeof(uint32_t)) == 0);
    t.transport_version = (moq_version_t)0;
    before = box;
    why = 99;
    CHECK(moq_media_object_parse_sized(&t, &in, NULL, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_INVAL);
    CHECK(memcmp(&box, &before, sizeof(box)) == 0 && why == 99);
    /* A legacy prefix cannot express a version and retains draft-16. */
    t.struct_size = MOQ_MEDIA_TRACK_INFO_V0_SIZE;
    CHECK(moq_media_object_parse_sized(&t, &in, NULL, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), NULL) == MOQ_OK);
    /* Legacy malformed-CMAF failure initializes output; sized failure must
     * retain caller output. Sharing the parser must not erase this difference. */
    uint8_t bad = 0;
    in.status = MOQ_OBJECT_NORMAL;
    in.payload = buf(&bad, 1);
    t.packaging = MOQ_MEDIA_PACKAGING_CMAF;
    t.timescale = 90000;
    memset(&box, 0xA5, sizeof(box));
    before = box;
    why = 99;
    CHECK(moq_media_object_parse_sized(&t, &in, NULL, NULL, 0,
          &box.aligner, sizeof(ts_old_parsed_t), &why) == MOQ_ERR_PROTO);
    CHECK(memcmp(&box, &before, sizeof(box)) == 0);
    CHECK(why == MOQ_MEDIA_DROP_MALFORMED_CMAF);
    CHECK(moq_media_object_parse(&t, &in, NULL, 0, &box.aligner, &why) == MOQ_ERR_PROTO);
    CHECK(box.aligner.struct_size == sizeof(ts_old_parsed_t));
    CHECK(box.aligner.packaging == MOQ_MEDIA_PACKAGING_CMAF);
    CHECK(memcmp(&box, &before, sizeof(box)) != 0);
    moq_rcbuf_decref(in.payload);
}

static void test_unknown_packaging(void)
{
    uint8_t obj[188 * 5];
    size_t n = build_object(obj, false, true);
    moq_media_track_info_t t;
    moq_media_object_parse_cfg_t cfg;
    moq_media_parsed_object_t o;
    for (int pk = 5; pk <= 6; pk++) {
        track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);
        t.packaging = (moq_media_packaging_t)pk;
        CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_ERR_INVAL);
    }
    /* LOCMAF is known, and needs a stateful decoder. */
    track(&t, &cfg, 188, MOQ_MEDIA_TYPE_VIDEO);
    t.packaging = MOQ_MEDIA_PACKAGING_LOCMAF;
    CHECK(run(&t, &cfg, obj, n, &o, NULL) == MOQ_ERR_UNSUPPORTED);
}

int main(void)
{
#ifdef MOQ_TEST_HAVE_MPEG2TS
    test_timing_and_keyframe();
    test_rejections();
#else
    test_unsupported();
#endif
    test_unknown_packaging();
    test_sized_contract();
    CHECK(media_object_legacy_client() == (sizeof(moq_media_type_t) == 4 ? 0 : 5));
    if (failures) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("media_object_mpeg2ts: all tests passed\n");
    return 0;
}
