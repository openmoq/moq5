#include <moq/media_object.h>
#include <moq/subscriber.h>
#include <moq/loc.h>
#include <moq/cmaf.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- LOC property builder -------------------------------------------- */

static moq_rcbuf_t *make_loc_props(const moq_alloc_t *alloc,
                                    uint64_t ts, bool kf)
{
    moq_loc_headers_t lh;
    moq_loc_headers_init(&lh);
    lh.has_timestamp = true;
    lh.timestamp = ts;
    if (kf) {
        lh.has_video_frame_marking = true;
        lh.video_frame_marking.independent = true;
        lh.video_frame_marking.start_of_frame = true;
        lh.video_frame_marking.end_of_frame = true;
    }
    moq_rcbuf_t *out = NULL;
    if (moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &lh, &out) != MOQ_OK)
        return NULL;
    return out;
}

/* -- CMAF fragment builder ------------------------------------------- */

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static size_t box_hdr(uint8_t *p, uint32_t size, const char *type)
{
    wr32(p, size);
    memcpy(p + 4, type, 4);
    return 8;
}

static size_t build_fragment(uint8_t *buf, uint64_t base_time,
                              uint32_t duration, uint32_t sample_size,
                              uint32_t flags, int32_t comp_offset,
                              const uint8_t *mdat_data, size_t mdat_len)
{
    size_t p = 0;
    uint32_t trun_flags = 0x100 | 0x200 | 0x400 | 0x800;
    size_t trun_size = 8 + 8 + 16;
    size_t tfdt_size = 8 + 4 + 8;
    size_t tfhd_size = 8 + 8;
    size_t traf_size = 8 + tfhd_size + tfdt_size + trun_size;
    size_t moof_size = 8 + traf_size;

    p += box_hdr(buf + p, (uint32_t)moof_size, "moof");
    p += box_hdr(buf + p, (uint32_t)traf_size, "traf");
    p += box_hdr(buf + p, (uint32_t)tfhd_size, "tfhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)tfdt_size, "tfdt");
    wr32(buf + p, 0x01000000); p += 4;
    wr32(buf + p, (uint32_t)(base_time >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base_time); p += 4;
    p += box_hdr(buf + p, (uint32_t)trun_size, "trun");
    wr32(buf + p, trun_flags); p += 4;
    wr32(buf + p, 1); p += 4;
    wr32(buf + p, duration); p += 4;
    wr32(buf + p, sample_size); p += 4;
    wr32(buf + p, flags); p += 4;
    wr32(buf + p, (uint32_t)comp_offset); p += 4;

    p += box_hdr(buf + p, (uint32_t)(8 + mdat_len), "mdat");
    memcpy(buf + p, mdat_data, mdat_len);
    p += mdat_len;
    return p;
}

static size_t build_multi_sample_fragment(uint8_t *buf, uint64_t base_time,
                                           uint32_t duration,
                                           const uint32_t *sizes, size_t count,
                                           const uint8_t *mdat_data,
                                           size_t mdat_len)
{
    size_t p = 0;
    uint32_t trun_flags = 0x100 | 0x200;
    size_t per_sample = 8;
    size_t trun_size = 8 + 8 + count * per_sample;
    size_t tfdt_size = 8 + 4 + 8;
    size_t tfhd_size = 8 + 8;
    size_t traf_size = 8 + tfhd_size + tfdt_size + trun_size;
    size_t moof_size = 8 + traf_size;

    p += box_hdr(buf + p, (uint32_t)moof_size, "moof");
    p += box_hdr(buf + p, (uint32_t)traf_size, "traf");
    p += box_hdr(buf + p, (uint32_t)tfhd_size, "tfhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)tfdt_size, "tfdt");
    wr32(buf + p, 0x01000000); p += 4;
    wr32(buf + p, (uint32_t)(base_time >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base_time); p += 4;
    p += box_hdr(buf + p, (uint32_t)trun_size, "trun");
    wr32(buf + p, trun_flags); p += 4;
    wr32(buf + p, (uint32_t)count); p += 4;
    for (size_t i = 0; i < count; i++) {
        wr32(buf + p, duration); p += 4;
        wr32(buf + p, sizes[i]); p += 4;
    }

    p += box_hdr(buf + p, (uint32_t)(8 + mdat_len), "mdat");
    memcpy(buf + p, mdat_data, mdat_len);
    p += mdat_len;
    return p;
}

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();

    /* -- init functions null-safe ------------------------------------- */
    {
        moq_media_track_info_init(NULL);
        moq_media_object_input_init(NULL);
        moq_media_parsed_object_init(NULL);
    }

    /* -- init sets struct_size --------------------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        CHECK(ti.struct_size == sizeof(moq_media_track_info_t));

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        CHECK(oi.struct_size == sizeof(moq_media_object_input_t));

        moq_media_parsed_object_t po;
        moq_media_parsed_object_init(&po);
        CHECK(po.struct_size == sizeof(moq_media_parsed_object_t));
    }

    /* -- NULL args return INVAL -------------------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        moq_media_parsed_object_t po;

        CHECK(moq_media_object_parse(NULL, &oi, NULL, 0, &po, NULL) == MOQ_ERR_INVAL);
        CHECK(moq_media_object_parse(&ti, NULL, NULL, 0, &po, NULL) == MOQ_ERR_INVAL);
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* ================================================================ */
    /*  RAW tests                                                      */
    /* ================================================================ */

    /* -- RAW timestamp+keyframe exact normalization ------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        uint8_t frame[] = { 0x00, 0x00, 0x01, 0x65 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, frame, 4, &payload) == MOQ_OK);

        moq_rcbuf_t *props = make_loc_props(alloc, 1000000, true);
        CHECK(props != NULL);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.group_id = 1;
        oi.object_id = 0;
        oi.payload = payload;
        oi.properties = props;

        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, &reason) == MOQ_OK);
        CHECK(reason == 0);
        CHECK(po.struct_size == sizeof(moq_media_parsed_object_t));
        CHECK(po.packaging == MOQ_MEDIA_PACKAGING_RAW);
        CHECK(po.has_capture_time == true);
        CHECK(po.capture_time_us == 1000000);
        CHECK(po.decode_time_us == 1000000);
        CHECK(po.composition_offset_us == 0);
        CHECK(po.presentation_time_us == 1000000);
        CHECK(po.keyframe == true);
        CHECK(po.payload.data == moq_rcbuf_data(payload));
        CHECK(po.payload.len == 4);

        CHECK(moq_rcbuf_refcount(payload) == 1);
        CHECK(moq_rcbuf_refcount(props) == 1);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
    }

    /* -- RAW timestamp without VFM: keyframe=false ------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        moq_loc_headers_t lh;
        moq_loc_headers_init(&lh);
        lh.has_timestamp = true;
        lh.timestamp = 5000;
        moq_rcbuf_t *props = NULL;
        CHECK(moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &lh, &props) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;
        oi.properties = props;

        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, NULL) == MOQ_OK);
        CHECK(po.keyframe == false);
        CHECK(po.decode_time_us == 5000);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
    }

    /* -- RAW missing timestamp -> surfaced with has_capture_time=false ---- *
     * The LOC capture timestamp is optional metadata (draft-ietf-moq-loc-01):
     * a missing one is not a protocol error, so the object surfaces (no capture
     * time; decode/presentation default 0) with its payload intact, rather than
     * being dropped -- for interop with peers that omit the extension. */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, &reason) == MOQ_OK);
        CHECK(po.has_capture_time == false);
        CHECK(po.decode_time_us == 0);
        CHECK(po.presentation_time_us == 0);
        CHECK(po.payload.len == 1);

        moq_rcbuf_decref(payload);
    }

    /* -- RAW malformed LOC -> PROTO + MALFORMED_LOC ------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        uint8_t bad[] = { 0xFF };
        moq_rcbuf_t *props = NULL;
        CHECK(moq_rcbuf_create(alloc, bad, 1, &props) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;
        oi.properties = props;

        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, &reason) == MOQ_ERR_PROTO);
        CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_LOC);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
    }

    /* ================================================================ */
    /*  Terminal tests                                                  */
    /* ================================================================ */

    /* -- END_OF_GROUP/END_OF_TRACK without payload -------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.status = MOQ_OBJECT_END_OF_GROUP;

        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, NULL) == MOQ_OK);
        CHECK(po.status == MOQ_OBJECT_END_OF_GROUP);

        oi.status = MOQ_OBJECT_END_OF_TRACK;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, NULL) == MOQ_OK);
        CHECK(po.status == MOQ_OBJECT_END_OF_TRACK);
    }

    /* -- terminal with payload -> INVAL ------------------------------ */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.status = MOQ_OBJECT_END_OF_GROUP;
        oi.payload = p;

        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, NULL) == MOQ_ERR_INVAL);
        CHECK(moq_rcbuf_refcount(p) == 1);

        moq_rcbuf_decref(p);
    }

    /* -- normal without payload -> INVAL ----------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);

        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, NULL) == MOQ_ERR_INVAL);
    }

    /* -- invalid packaging/media_type -> INVAL ----------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = (moq_media_type_t)99;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *p = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &p) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = p;

        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, NULL) == MOQ_ERR_INVAL);
        moq_rcbuf_decref(p);

        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = (moq_media_packaging_t)99;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, NULL) == MOQ_ERR_INVAL);
    }

    /* ================================================================ */
    /*  CMAF tests                                                     */
    /* ================================================================ */

    /* -- CMAF one-sample fragment: exact timing ---------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t mdat[] = { 0xCA, 0xFE };
        uint8_t buf[512];
        size_t len = build_fragment(buf, 180000, 3000, 2,
                                     0x00000000, 1000, mdat, 2);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_OK);
        CHECK(reason == 0);
        CHECK(po.packaging == MOQ_MEDIA_PACKAGING_CMAF);
        CHECK(po.sample_count == 1);
        /* 180000 / 90000 = 2.0s = 2000000us */
        CHECK(po.decode_time_us == 2000000);
        /* comp_offset = 1000 / 90000 ≈ 11111us */
        CHECK(po.composition_offset_us == 11111);
        CHECK(po.presentation_time_us == 2000000 + 11111);
        /* sample_duration = 3000/90000 = 33333us */
        CHECK(po.sample_duration_us == 33333);
        CHECK(po.keyframe == true); /* flags=0 → not non-sync */
        CHECK(po.mdat_len == 2);
        CHECK(po.fragment.data == moq_rcbuf_data(payload));
        CHECK(po.fragment.len == len);
        CHECK(moq_rcbuf_refcount(payload) == 1);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF + LOC VFM: LOC keyframe marking wins over CMAF sample flags -- *
     * A CMAF fragment that omits per-sample flags parses as a sync sample
     * (flags == 0 -> keyframe). When LOC video frame marking is present it is
     * authoritative, so an independent=false marking must yield keyframe=false
     * even though the CMAF flags say sync -- and vice versa. */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t mdat[] = { 0xCA, 0xFE };
        uint8_t buf[512];
        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;

        /* CMAF flags=0 (reads as sync/keyframe) but LOC says delta -> false. */
        moq_loc_headers_t lh;
        moq_loc_headers_init(&lh);
        lh.has_timestamp = true;
        lh.timestamp = 1000000;
        lh.has_video_frame_marking = true;
        lh.video_frame_marking.independent = false;
        moq_rcbuf_t *props = NULL;
        CHECK(moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &lh, &props) == MOQ_OK);

        size_t len = build_fragment(buf, 90000, 3000, 2, 0x00000000, 0, mdat, 2);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;
        oi.properties = props;

        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.keyframe == false);   /* LOC independent=false wins */

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(payload);

        /* Inverse: CMAF flags say non-sync (delta) but LOC says key -> true. */
        moq_loc_headers_init(&lh);
        lh.has_timestamp = true;
        lh.timestamp = 1000000;
        lh.has_video_frame_marking = true;
        lh.video_frame_marking.independent = true;
        props = NULL;
        CHECK(moq_loc_encode(alloc, MOQ_LOC_PROFILE_01, &lh, &props) == MOQ_OK);

        len = build_fragment(buf, 90000, 3000, 2, 0x00010000, 0, mdat, 2);
        payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_init(&oi);
        oi.payload = payload;
        oi.properties = props;

        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.keyframe == true);    /* LOC independent=true wins */

        moq_rcbuf_decref(props);
        moq_rcbuf_decref(payload);
    }

    /* -- CMAF: SAP type / chunk_count / track_id surfaced ------------ */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t mdat[] = { 0xCA, 0xFE };
        uint8_t buf[512];
        /* flags=0 -> sync sample (closed-GOP SAP, exact type UNKNOWN) */
        size_t len = build_fragment(buf, 0, 3000, 2, 0x00000000, 0, mdat, 2);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.sap_type == MOQ_SAP_UNKNOWN);
        CHECK(po.chunk_count == 1);
        CHECK(po.track_id == 1);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF: non-sync depends-on-others -> SAP NONE ---------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t mdat[] = { 0x01 };
        uint8_t buf[512];
        /* non_sync bit + depends_on=1 -> a P/B frame, not a SAP */
        size_t len = build_fragment(buf, 0, 3000, 1, 0x01010000, 0, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.sap_type == MOQ_SAP_NONE);
        CHECK(po.keyframe == false);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF: multi-chunk object -> chunk_count, first mdat, full frag */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t m1[] = { 0xA1 };
        uint8_t m2[] = { 0xB2, 0xB3 };
        uint8_t buf[1024];
        size_t n = 0;
        n += build_fragment(buf + n, 90000, 3000, 1, 0x00000000, 0, m1, 1);
        n += build_fragment(buf + n, 93000, 3000, 2, 0x00000000, 0, m2, 2);

        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, n, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.chunk_count == 2);            /* both chunks detected */
        CHECK(po.mdat_len == 1);               /* first chunk's mdat normalized */
        CHECK(po.fragment.len == n);           /* full object preserved */
        CHECK(po.track_id == 1);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF with LOC capture timestamp ----------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t mdat[] = { 0x01 };
        uint8_t buf[512];
        size_t len = build_fragment(buf, 90000, 3000, 1,
                                     0x00000000, 0, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_rcbuf_t *props = make_loc_props(alloc, 9999999, false);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;
        oi.properties = props;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.has_capture_time == true);
        CHECK(po.capture_time_us == 9999999);
        CHECK(po.decode_time_us == 1000000);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
    }

    /* -- CMAF sample buffer too small -> MOQ_ERR_BUFFER -------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t mdat[] = { 0x01 };
        uint32_t sizes3[] = { 1, 0, 0 };
        uint8_t buf[512];
        size_t len = build_multi_sample_fragment(buf, 90000, 3000,
                                                  sizes3, 3, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[1];
        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 1, &po, NULL) == MOQ_ERR_BUFFER);
        CHECK(po.sample_count == 3);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF video multi-sample succeeds with enough scratch --------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t mdat[] = { 0x01, 0x02, 0x03 };
        uint32_t sizes2[] = { 1, 2 };
        uint8_t buf[512];
        size_t len = build_multi_sample_fragment(buf, 90000, 3000,
                                                  sizes2, 2, mdat, 3);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_OK);
        CHECK(reason == 0);
        CHECK(po.sample_count == 2);
        CHECK(po.samples == samples);
        CHECK(po.decode_time_us == 1000000);
        CHECK(samples[0].size == 1);
        CHECK(samples[1].size == 2);
        CHECK(po.mdat_len == 3);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF malformed -> PROTO + MALFORMED_CMAF -------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 90000;

        uint8_t bad[] = { 0x00, 0x00, 0x00, 0x01, 'b', 'a', 'd', '!' };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, bad, 8, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_ERR_PROTO);
        CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_CMAF);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF timescale zero -> INVAL -------------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 0;

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_ERR_INVAL);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF keyframe flags ----------------------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 1000;

        uint8_t mdat[] = { 0x01 };
        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_object_input_t oi;
        uint8_t buf[512];
        size_t len;
        moq_rcbuf_t *payload;

        /* depends_on=2 (0x02000000): independent → keyframe */
        len = build_fragment(buf, 1000, 33, 1, 0x02000000, 0, mdat, 1);
        payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);
        moq_media_object_input_init(&oi);
        oi.payload = payload;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.keyframe == true);
        moq_rcbuf_decref(payload);

        /* non_sync bit (0x00010000): not keyframe */
        len = build_fragment(buf, 1000, 33, 1, 0x00010000, 0, mdat, 1);
        payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);
        moq_media_object_input_init(&oi);
        oi.payload = payload;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.keyframe == false);
        moq_rcbuf_decref(payload);

        /* depends_on=1 (0x01000000): depends on others → not keyframe */
        len = build_fragment(buf, 1000, 33, 1, 0x01000000, 0, mdat, 1);
        payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);
        moq_media_object_input_init(&oi);
        oi.payload = payload;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.keyframe == false);
        moq_rcbuf_decref(payload);

        /* flags=0: default/permissive → keyframe */
        len = build_fragment(buf, 1000, 33, 1, 0x00000000, 0, mdat, 1);
        payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);
        moq_media_object_input_init(&oi);
        oi.payload = payload;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_OK);
        CHECK(po.keyframe == true);
        moq_rcbuf_decref(payload);
    }

    /* -- drop_reason set to 0 on success ----------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.status = MOQ_OBJECT_END_OF_GROUP;

        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 99;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, &reason) == MOQ_OK);
        CHECK(reason == 0);
    }

    /* ================================================================ */
    /*  Timestamp overflow tests                                       */
    /* ================================================================ */

    /* -- base_decode_time overflow at timescale=1 --------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 1;

        uint8_t mdat[] = { 0x01 };
        uint8_t buf[512];
        /* base_decode_time near UINT64_MAX / timescale=1 → sec * 1000000 overflows */
        size_t len = build_fragment(buf, UINT64_MAX / 2, 1, 1,
                                     0x00000000, 0, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_ERR_PROTO);
        CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_CMAF);

        moq_rcbuf_decref(payload);
    }

    /* -- positive composition offset causing presentation overflow ---- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 1000;

        uint8_t mdat[] = { 0x01 };
        uint8_t buf[512];
        /* base_decode_time yields ~UINT64_MAX in us, comp_offset > 0 → overflow */
        uint64_t bdt = UINT64_MAX / 1000u - 1;
        size_t len = build_fragment(buf, bdt, 1, 1,
                                     0x00000000, 2000, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_ERR_PROTO);
        CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_CMAF);

        moq_rcbuf_decref(payload);
    }

    /* -- negative composition offset causing presentation underflow --- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 1000;

        uint8_t mdat[] = { 0x01 };
        uint8_t buf[512];
        /* base_decode_time=0, comp_offset=-1000 → presentation underflow */
        size_t len = build_fragment(buf, 0, 1, 1,
                                     0x00000000, -1000, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_ERR_PROTO);
        CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_CMAF);

        moq_rcbuf_decref(payload);
    }

    /* -- decode_time > INT64_MAX with zero offset succeeds ------------ */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 1000000;

        uint8_t mdat[] = { 0x01 };
        uint8_t buf[512];
        /* base_decode_time yields decode_time_us > INT64_MAX but < UINT64_MAX */
        uint64_t bdt = (uint64_t)INT64_MAX + 1000000u;
        size_t len = build_fragment(buf, bdt, 1000000, 1,
                                     0x00000000, 0, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_OK);
        CHECK(reason == 0);
        CHECK(po.decode_time_us == bdt);
        CHECK(po.presentation_time_us == bdt);

        moq_rcbuf_decref(payload);
    }

    /* -- sample duration > UINT32_MAX returns PROTO + MALFORMED_CMAF -- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 1;

        uint8_t mdat[] = { 0x01 };
        uint8_t buf[512];
        /* duration=5000 at timescale=1 → 5000000000us > UINT32_MAX */
        size_t len = build_fragment(buf, 0, 5000, 1,
                                     0x00000000, 0, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_ERR_PROTO);
        CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_CMAF);

        moq_rcbuf_decref(payload);
    }

    /* ================================================================ */
    /*  Audio support                                                  */
    /* ================================================================ */

    /* -- RAW audio with LOC timestamp succeeds ----------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_AUDIO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        uint8_t data[] = { 0x01, 0x02 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &payload) == MOQ_OK);

        moq_rcbuf_t *props = make_loc_props(alloc, 5000000, false);
        CHECK(props != NULL);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload; oi.properties = props;

        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, &reason) == MOQ_OK);
        CHECK(reason == 0);
        CHECK(po.keyframe == true);
        CHECK(po.decode_time_us == 5000000);
        CHECK(po.payload.len == 2);

        moq_rcbuf_decref(payload); moq_rcbuf_decref(props);
    }

    /* -- RAW audio missing timestamp → surfaced with has_capture_time=false - *
     * Same leniency as RAW video: optional LOC timestamp, surfaced not dropped
     * (audio is always a keyframe). */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_AUDIO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, &reason) == MOQ_OK);
        CHECK(po.has_capture_time == false);
        CHECK(po.keyframe == true);
        CHECK(po.payload.len == 1);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF audio one-sample succeeds with exact timing ------------ */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_AUDIO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 48000;

        uint8_t mdat[] = { 0xAA, 0xBB, 0xCC };
        uint8_t buf[512];
        size_t len = build_fragment(buf, 48000, 1024, 3,
                                     0x00000000, 0, mdat, 3);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_OK);
        CHECK(reason == 0);
        CHECK(po.keyframe == true);
        CHECK(po.sample_count == 1);
        CHECK(po.decode_time_us == 1000000);
        /* duration = 1024/48000 = 21333us */
        CHECK(po.sample_duration_us == 21333);
        CHECK(po.mdat_len == 3);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF audio multi-sample succeeds ---------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_AUDIO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 48000;

        uint8_t mdat[] = { 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 };
        uint32_t sizes3[] = { 2, 3, 1 };
        uint8_t buf[512];
        size_t len = build_multi_sample_fragment(buf, 48000, 1024,
                                                  sizes3, 3, mdat, 6);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[8];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 8, &po, &reason) == MOQ_OK);
        CHECK(reason == 0);
        CHECK(po.keyframe == true);
        CHECK(po.sample_count == 3);
        CHECK(po.samples == samples);
        CHECK(po.decode_time_us == 1000000);
        CHECK(po.sample_duration_us == 21333);
        CHECK(samples[0].size == 2);
        CHECK(samples[1].size == 3);
        CHECK(samples[2].size == 1);
        CHECK(po.mdat_len == 6);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF audio multi-sample too-small scratch → BUFFER ---------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_AUDIO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 48000;

        uint8_t mdat[] = { 0x01 };
        uint32_t sizes4[] = { 1, 0, 0, 0 };
        uint8_t buf[512];
        size_t len = build_multi_sample_fragment(buf, 48000, 1024,
                                                  sizes4, 4, mdat, 1);
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, buf, len, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[2];
        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 2, &po, NULL) == MOQ_ERR_BUFFER);
        CHECK(po.sample_count == 4);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF audio malformed → PROTO + MALFORMED_CMAF --------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_AUDIO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 48000;

        uint8_t bad[] = { 0x00, 0x00, 0x00, 0x01, 'b', 'a', 'd', '!' };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, bad, 8, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        moq_media_drop_reason_t reason = 0;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, &reason) == MOQ_ERR_PROTO);
        CHECK(reason == MOQ_MEDIA_DROP_MALFORMED_CMAF);

        moq_rcbuf_decref(payload);
    }

    /* -- CMAF audio zero timescale → INVAL --------------------------- */
    {
        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_AUDIO;
        ti.packaging = MOQ_MEDIA_PACKAGING_CMAF;
        ti.timescale = 0;

        uint8_t data[] = { 0x01 };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 1, &payload) == MOQ_OK);

        moq_media_object_input_t oi;
        moq_media_object_input_init(&oi);
        oi.payload = payload;

        moq_cmaf_sample_t samples[4];
        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, samples, 4, &po, NULL) == MOQ_ERR_INVAL);

        moq_rcbuf_decref(payload);
    }

    /* ================================================================ */
    /*  Subscriber input helper                                        */
    /* ================================================================ */

    /* -- NULL src/dst → INVAL ---------------------------------------- */
    {
        moq_media_object_input_t dst;
        CHECK(moq_media_object_input_from_sub_object(NULL, &dst) == MOQ_ERR_INVAL);

        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        CHECK(moq_media_object_input_from_sub_object(&src, NULL) == MOQ_ERR_INVAL);
    }

    /* -- Exact field mapping ----------------------------------------- */
    {
        uint8_t data[] = { 0xDE, 0xAD };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &payload) == MOQ_OK);

        uint8_t prop_data[] = { 0x01 };
        moq_rcbuf_t *props = NULL;
        CHECK(moq_rcbuf_create(alloc, prop_data, 1, &props) == MOQ_OK);

        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        src.group_id = 42;
        src.object_id = 7;
        src.status = MOQ_OBJECT_NORMAL;
        src.end_of_group = true;
        src.datagram = true;
        src.payload = payload;
        src.properties = props;

        moq_media_object_input_t dst;
        CHECK(moq_media_object_input_from_sub_object(&src, &dst) == MOQ_OK);

        CHECK(dst.struct_size == sizeof(moq_media_object_input_t));
        CHECK(dst.group_id == 42);
        CHECK(dst.object_id == 7);
        CHECK(dst.status == MOQ_OBJECT_NORMAL);
        CHECK(dst.end_of_group == true);
        CHECK(dst.datagram == true);
        CHECK(dst.payload == payload);
        CHECK(dst.properties == props);

        CHECK(moq_rcbuf_refcount(payload) == 1);
        CHECK(moq_rcbuf_refcount(props) == 1);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
    }

    /* -- Helper output feeds directly into parse ---------------------- */
    {
        moq_rcbuf_t *props = make_loc_props(alloc, 8000, true);
        CHECK(props != NULL);

        uint8_t data[] = { 0xCA, 0xFE };
        moq_rcbuf_t *payload = NULL;
        CHECK(moq_rcbuf_create(alloc, data, 2, &payload) == MOQ_OK);

        moq_sub_object_t src;
        memset(&src, 0, sizeof(src));
        src.group_id = 1;
        src.object_id = 0;
        src.status = MOQ_OBJECT_NORMAL;
        src.payload = payload;
        src.properties = props;

        moq_media_object_input_t oi;
        CHECK(moq_media_object_input_from_sub_object(&src, &oi) == MOQ_OK);

        moq_media_track_info_t ti;
        moq_media_track_info_init(&ti);
        ti.media_type = MOQ_MEDIA_TYPE_VIDEO;
        ti.packaging = MOQ_MEDIA_PACKAGING_RAW;

        moq_media_parsed_object_t po;
        CHECK(moq_media_object_parse(&ti, &oi, NULL, 0, &po, NULL) == MOQ_OK);
        CHECK(po.decode_time_us == 8000);
        CHECK(po.keyframe == true);
        CHECK(po.payload.len == 2);

        CHECK(moq_rcbuf_refcount(payload) == 1);
        CHECK(moq_rcbuf_refcount(props) == 1);

        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
