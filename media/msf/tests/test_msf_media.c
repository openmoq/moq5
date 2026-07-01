#include <moq/msf_media.h>
#include <moq/rcbuf.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define CHECK_EQ_INT(a, b) do { \
    int _a = (a), _b = (b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %d, expected %s == %d\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        failures++; \
    } \
} while (0)

#define CHECK_EQ_U32(a, b) do { \
    uint32_t _a = (a), _b = (b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %u, expected %s == %u\n", \
                __FILE__, __LINE__, #a, (unsigned)_a, #b, (unsigned)_b); \
        failures++; \
    } \
} while (0)

static moq_bytes_t lit(const char *s)
{
    return (moq_bytes_t){ (const uint8_t *)s, strlen(s) };
}

/* -- Box builder helpers (from test_cmaf.c) --------------------------- */

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v;
}

static size_t box_hdr(uint8_t *p, uint32_t size, const char *type)
{
    wr32(p, size);
    memcpy(p + 4, type, 4);
    return 8;
}

static size_t build_avc_init(uint8_t *buf, uint32_t timescale,
                              uint16_t w, uint16_t h,
                              const uint8_t *avcc_data, size_t avcc_len)
{
    size_t p = 0;
    p += box_hdr(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    size_t avcc_box_size = 8 + avcc_len;
    size_t avc1_size = 8 + 78 + avcc_box_size;
    size_t stsd_size = 8 + 8 + avc1_size;
    size_t stbl_size = 8 + stsd_size;
    size_t minf_size = 8 + stbl_size;
    size_t mdhd_size = 8 + 4 + 12;
    size_t mdia_size = 8 + mdhd_size + minf_size;
    size_t trak_size = 8 + mdia_size;
    size_t moov_size = 8 + trak_size;

    p += box_hdr(buf + p, (uint32_t)moov_size, "moov");
    p += box_hdr(buf + p, (uint32_t)trak_size, "trak");
    p += box_hdr(buf + p, (uint32_t)mdia_size, "mdia");
    p += box_hdr(buf + p, (uint32_t)mdhd_size, "mdhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, timescale); p += 4;
    p += box_hdr(buf + p, (uint32_t)minf_size, "minf");
    p += box_hdr(buf + p, (uint32_t)stbl_size, "stbl");
    p += box_hdr(buf + p, (uint32_t)stsd_size, "stsd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)avc1_size, "avc1");
    memset(buf + p, 0, 78);
    wr16(buf + p + 24, w);
    wr16(buf + p + 26, h);
    p += 78;
    p += box_hdr(buf + p, (uint32_t)avcc_box_size, "avcC");
    memcpy(buf + p, avcc_data, avcc_len);
    p += avcc_len;
    return p;
}

static size_t build_aac_init(uint8_t *buf, uint32_t timescale,
                              uint16_t samplerate, uint16_t channels,
                              const uint8_t *asc, size_t asc_len)
{
    size_t p = 0;
    p += box_hdr(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    size_t dec_spec_len = 2 + asc_len;
    size_t dec_cfg_len = 2 + 13 + dec_spec_len;
    size_t es_desc_len = 2 + 3 + dec_cfg_len;
    size_t esds_body = 4 + es_desc_len;
    size_t esds_size = 8 + esds_body;
    size_t mp4a_size = 8 + 28 + esds_size;
    size_t stsd_size = 8 + 8 + mp4a_size;
    size_t stbl_size = 8 + stsd_size;
    size_t minf_size = 8 + stbl_size;
    size_t mdhd_size = 8 + 4 + 12;
    size_t mdia_size = 8 + mdhd_size + minf_size;
    size_t trak_size = 8 + mdia_size;
    size_t moov_size = 8 + trak_size;

    p += box_hdr(buf + p, (uint32_t)moov_size, "moov");
    p += box_hdr(buf + p, (uint32_t)trak_size, "trak");
    p += box_hdr(buf + p, (uint32_t)mdia_size, "mdia");
    p += box_hdr(buf + p, (uint32_t)mdhd_size, "mdhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, timescale); p += 4;
    p += box_hdr(buf + p, (uint32_t)minf_size, "minf");
    p += box_hdr(buf + p, (uint32_t)stbl_size, "stbl");
    p += box_hdr(buf + p, (uint32_t)stsd_size, "stsd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)mp4a_size, "mp4a");
    memset(buf + p, 0, 28);
    wr16(buf + p + 8, channels);
    wr16(buf + p + 24, samplerate);
    p += 28;
    p += box_hdr(buf + p, (uint32_t)esds_size, "esds");
    wr32(buf + p, 0); p += 4;
    buf[p++] = 0x03;
    buf[p++] = (uint8_t)(es_desc_len - 2);
    wr16(buf + p, 0); p += 2;
    buf[p++] = 0;
    buf[p++] = 0x04;
    buf[p++] = (uint8_t)(dec_cfg_len - 2);
    buf[p++] = 0x40;
    buf[p++] = 0x15;
    buf[p++] = 0; buf[p++] = 0; buf[p++] = 0;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    buf[p++] = 0x05;
    buf[p++] = (uint8_t)asc_len;
    memcpy(buf + p, asc, asc_len);
    p += asc_len;
    return p;
}

/* -- Helper: encode raw bytes to base64 ------------------------------- */

typedef struct {
    moq_rcbuf_t *buf;
    moq_bytes_t  bytes;
} b64_result_t;

static b64_result_t encode_b64(const moq_alloc_t *alloc,
                                const uint8_t *data, size_t len)
{
    b64_result_t r = { NULL, { NULL, 0 } };
    moq_msf_encode_init_data(alloc,
        (moq_bytes_t){ data, len }, &r.buf);
    if (r.buf) {
        r.bytes.data = moq_rcbuf_data(r.buf);
        r.bytes.len  = moq_rcbuf_len(r.buf);
    }
    return r;
}

/* -- OOM allocator ---------------------------------------------------- */

typedef struct {
    int64_t  balance;
    uint64_t alloc_count;
    uint64_t fail_at;
} oom_state_t;

static void *oom_alloc(size_t sz, void *ctx)
{
    oom_state_t *s = (oom_state_t *)ctx;
    if (sz == 0) return NULL;
    s->alloc_count++;
    if (s->fail_at > 0 && s->alloc_count == s->fail_at)
        return NULL;
    void *p = malloc(sz);
    if (p) s->balance++;
    return p;
}

static void oom_free(void *p, size_t sz, void *ctx)
{
    oom_state_t *s = (oom_state_t *)ctx;
    (void)sz;
    if (p) s->balance--;
    free(p);
}

static moq_alloc_t oom_allocator(oom_state_t *s)
{
    return (moq_alloc_t){ s, oom_alloc, NULL, oom_free };
}

int main(void)
{
    const moq_alloc_t *alloc = moq_alloc_default();

    /* -- CMAF video: out_init + out_init_buf -------------------------- */
    {
        uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1E };
        uint8_t init_raw[512];
        size_t init_len = build_avc_init(init_raw, 90000, 1920, 1080,
                                          avcc, sizeof(avcc));
        b64_result_t b64 = encode_b64(alloc, init_raw, init_len);
        CHECK(b64.buf != NULL);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_codec = true;
        t.codec = lit("avc1.64001e");
        t.has_init_data = true;
        t.init_data = b64.bytes;

        moq_media_track_info_t info;
        moq_cmaf_init_info_t cinit;
        moq_rcbuf_t *init_buf = NULL;
        moq_result_t rc = moq_msf_track_to_media_info(
            alloc, &t, &info, &cinit, &init_buf);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_VIDEO);
        CHECK_EQ_INT(info.packaging, MOQ_MEDIA_PACKAGING_CMAF);
        CHECK_EQ_U32(info.timescale, 90000);
        CHECK_EQ_INT(cinit.codec_kind, MOQ_CMAF_CODEC_AVC);
        CHECK_EQ_U32(cinit.width, 1920);
        CHECK_EQ_U32(cinit.height, 1080);
        CHECK(cinit.codec_config.len == sizeof(avcc));
        CHECK(memcmp(cinit.codec_config.data, avcc, sizeof(avcc)) == 0);
        CHECK(init_buf != NULL);

        moq_rcbuf_decref(init_buf);
        moq_rcbuf_decref(b64.buf);
    }

    /* -- CMAF audio: out_init + out_init_buf -------------------------- */
    {
        uint8_t asc[] = { 0x12, 0x10 };
        uint8_t init_raw[512];
        size_t init_len = build_aac_init(init_raw, 48000, 48000, 2,
                                          asc, sizeof(asc));
        b64_result_t b64 = encode_b64(alloc, init_raw, init_len);
        CHECK(b64.buf != NULL);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("audio");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("audio");
        t.has_codec = true;
        t.codec = lit("mp4a.40.2");
        t.has_init_data = true;
        t.init_data = b64.bytes;

        moq_media_track_info_t info;
        moq_cmaf_init_info_t cinit;
        moq_rcbuf_t *init_buf = NULL;
        moq_result_t rc = moq_msf_track_to_media_info(
            alloc, &t, &info, &cinit, &init_buf);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_AUDIO);
        CHECK_EQ_INT(info.packaging, MOQ_MEDIA_PACKAGING_CMAF);
        CHECK_EQ_U32(info.timescale, 48000);
        CHECK_EQ_INT(cinit.codec_kind, MOQ_CMAF_CODEC_AAC);
        CHECK_EQ_U32(cinit.samplerate, 48000);
        CHECK_EQ_U32(cinit.channel_count, 2);
        CHECK(init_buf != NULL);

        moq_rcbuf_decref(init_buf);
        moq_rcbuf_decref(b64.buf);
    }

    /* -- CMAF video: out_init == NULL still derives timescale ---------- */
    {
        uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1E };
        uint8_t init_raw[512];
        size_t init_len = build_avc_init(init_raw, 90000, 1920, 1080,
                                          avcc, sizeof(avcc));
        b64_result_t b64 = encode_b64(alloc, init_raw, init_len);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_init_data = true;
        t.init_data = b64.bytes;

        moq_media_track_info_t info;
        moq_result_t rc = moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_U32(info.timescale, 90000);

        moq_rcbuf_decref(b64.buf);
    }

    /* -- CMAF: out_init != NULL but out_init_buf == NULL → INVAL ------- */
    {
        uint8_t avcc[] = { 0x01 };
        uint8_t init_raw[512];
        size_t init_len = build_avc_init(init_raw, 90000, 640, 480,
                                          avcc, sizeof(avcc));
        b64_result_t b64 = encode_b64(alloc, init_raw, init_len);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("v");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_init_data = true;
        t.init_data = b64.bytes;

        moq_media_track_info_t info;
        moq_cmaf_init_info_t cinit;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, &cinit, NULL) == MOQ_ERR_INVAL);

        moq_rcbuf_decref(b64.buf);
    }

    /* -- LOC video track with timescale ------------------------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_timescale = true;
        t.timescale = 90000;

        moq_media_track_info_t info;
        moq_result_t rc = moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_VIDEO);
        CHECK_EQ_INT(info.packaging, MOQ_MEDIA_PACKAGING_RAW);
        CHECK_EQ_U32(info.timescale, 90000);
    }

    /* -- LOC audio without timescale → 0 ------------------------------ */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("audio");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("audio");

        moq_media_track_info_t info;
        moq_result_t rc = moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_AUDIO);
        CHECK_EQ_U32(info.timescale, 0);
    }

    /* -- LOC audio with timescale ------------------------------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("audio");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("audio");
        t.has_timescale = true;
        t.timescale = 48000;

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_OK);
        CHECK_EQ_U32(info.timescale, 48000);
    }

    /* -- roleless LOC video classified by codec → VIDEO ------- *
     * MSF-01 makes `role` Optional (§5.2.6); `codec` is required for
     * audio/video tracks (§5.2.18). A conformant catalog may omit role, so
     * the media type must fall back to the codec prefix. */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("loc");
        t.is_live = true;
        /* no role */
        t.has_codec = true;
        t.codec = lit("avc1.42e01e");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_VIDEO);
        CHECK_EQ_INT(info.packaging, MOQ_MEDIA_PACKAGING_RAW);
    }

    /* -- roleless LOC audio classified by codec (aac) → AUDIO - */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("audio");
        t.packaging = lit("loc");
        t.is_live = true;
        /* no role */
        t.has_codec = true;
        t.codec = lit("mp4a.40.2");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_AUDIO);
        CHECK_EQ_INT(info.packaging, MOQ_MEDIA_PACKAGING_RAW);
    }

    /* -- roleless LOC audio classified by codec (opus) → AUDIO */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("audio");
        t.packaging = lit("loc");
        t.is_live = true;
        /* no role */
        t.has_codec = true;
        t.codec = lit("opus");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_AUDIO);
    }

    /* -- no role AND an unclassifiable codec → INVAL ---------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("misc");
        t.packaging = lit("loc");
        t.is_live = true;
        /* no role */
        t.has_codec = true;
        t.codec = lit("application/mystery");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- non-CMAF with out_init: init zeroed, buf NULL ---------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("audio");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("audio");
        t.has_timescale = true;
        t.timescale = 48000;

        moq_media_track_info_t info;
        moq_cmaf_init_info_t cinit;
        moq_rcbuf_t *init_buf = (moq_rcbuf_t *)0xDEAD;
        moq_result_t rc = moq_msf_track_to_media_info(
            alloc, &t, &info, &cinit, &init_buf);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_U32(cinit.struct_size, sizeof(moq_cmaf_init_info_t));
        CHECK_EQ_INT(cinit.codec_kind, MOQ_CMAF_CODEC_UNKNOWN);
        CHECK_EQ_U32(cinit.timescale, 0);
        CHECK(init_buf == NULL);
    }

    /* -- LOC with initData: decoded extradata surfaces as codec_config -- *
     * A RAW/LOC track's initData is the encoder's decoder config verbatim.
     * It must be decoded and exposed as codec_config (aliasing the returned
     * buffer) so consumers read extradata from codec_config for any
     * packaging -- this is the receiver-side bug the contract fixes. */
    {
        static const uint8_t extradata[] = {
            0x01, 0x64, 0x00, 0x1f, 0xff, 0xe1, 0x00, 0x09, 0xCA, 0xFE };
        b64_result_t b64 = encode_b64(alloc, extradata, sizeof(extradata));
        CHECK(b64.buf != NULL);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("audio");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("audio");
        t.has_timescale = true;
        t.timescale = 48000;
        t.has_init_data = true;
        t.init_data = b64.bytes;

        moq_media_track_info_t info;
        moq_cmaf_init_info_t cinit;
        moq_rcbuf_t *init_buf = NULL;
        moq_result_t rc = moq_msf_track_to_media_info(
            alloc, &t, &info, &cinit, &init_buf);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_INT(info.packaging, MOQ_MEDIA_PACKAGING_RAW);
        CHECK_EQ_U32(info.timescale, 48000);
        /* decoded buffer returned, and codec_config == the original extradata */
        CHECK(init_buf != NULL);
        CHECK(cinit.codec_config.len == sizeof(extradata));
        CHECK(memcmp(cinit.codec_config.data, extradata, sizeof(extradata)) == 0);
        CHECK(cinit.codec_config.data == moq_rcbuf_data(init_buf));

        moq_rcbuf_decref(init_buf);
        moq_rcbuf_decref(b64.buf);
    }

    /* -- LOC with initData but out_init_buf == NULL: no decode --------- *
     * Without a buffer to own the bytes, do not decode (no allocation may
     * outlive the call); codec_config stays empty. */
    {
        static const uint8_t extradata[] = { 0xDE, 0xAD, 0xBE, 0xEF };
        b64_result_t b64 = encode_b64(alloc, extradata, sizeof(extradata));
        CHECK(b64.buf != NULL);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("audio");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("audio");
        t.has_init_data = true;
        t.init_data = b64.bytes;

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(alloc, &t, &info, NULL, NULL) == MOQ_OK);
        moq_rcbuf_decref(b64.buf);
    }

    /* -- no role AND no codec → INVAL (nothing to classify from) ------- *
     * role is Optional (§5.2.6), but with neither a role nor a codec there is
     * no signal to derive the media type, so classification must fail. */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("cmaf");
        t.is_live = true;
        /* no role, no codec */

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- reserved non-media role → INVAL even WITH a media codec ------- *
     * A denylisted role (subtitle/caption/timeline/log/metrics) is never media;
     * the codec must NOT promote it. */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("subs");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("subtitle");
        t.has_codec = true;
        t.codec = lit("avc1.42e01e");   /* a video codec must not override the role */

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- custom role + media codec → classified by codec ------ *
     * §5.2.6 allows custom roles; a custom role with a media codec is media. */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("main");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("main");           /* custom role */
        t.has_codec = true;
        t.codec = lit("avc1.42e01e");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_VIDEO);
    }

    /* -- media-like reserved role (audiodescription) + codec -- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("ad");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("audiodescription");
        t.has_codec = true;
        t.codec = lit("mp4a.40.2");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_AUDIO);
    }

    /* -- media-like reserved role (signlanguage) + codec ------ */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("sign");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("signlanguage");
        t.has_codec = true;
        t.codec = lit("hvc1.1.6.L93.B0");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_OK);
        CHECK_EQ_INT(info.media_type, MOQ_MEDIA_TYPE_VIDEO);
    }

    /* -- custom role but NO classifiable codec → INVAL -------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("weird");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("main");
        t.has_codec = true;
        t.codec = lit("application/mystery");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- unsupported packaging → INVAL -------------------------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("custom");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- NULL track → INVAL ------------------------------------------- */
    {
        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, NULL, &info, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- NULL out_info → INVAL ---------------------------------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("v");
        t.packaging = lit("loc");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");

        CHECK(moq_msf_track_to_media_info(
            alloc, &t, NULL, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- malformed CMAF initData → PROTO ------------------------------ */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_init_data = true;
        t.init_data = lit("AAAB");

        moq_media_track_info_t info;
        moq_cmaf_init_info_t cinit;
        moq_rcbuf_t *init_buf = NULL;
        moq_result_t rc = moq_msf_track_to_media_info(
            alloc, &t, &info, &cinit, &init_buf);
        CHECK(rc == MOQ_ERR_PROTO);
        CHECK(init_buf == NULL);
    }

    /* -- CMAF without initData: fallback to MSF timescale ------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_timescale = true;
        t.timescale = 90000;

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_OK);
        CHECK_EQ_U32(info.timescale, 90000);
    }

    /* -- CMAF without initData AND without timescale → INVAL ---------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, NULL, NULL) == MOQ_ERR_INVAL);
    }

    /* -- CMAF without initData + out_init != NULL → INVAL ------------- */
    {
        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("video");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_timescale = true;
        t.timescale = 90000;

        moq_media_track_info_t info;
        moq_cmaf_init_info_t cinit;
        moq_rcbuf_t *init_buf = NULL;
        CHECK(moq_msf_track_to_media_info(
            alloc, &t, &info, &cinit, &init_buf) == MOQ_ERR_INVAL);
    }

    /* -- CMAF alloc == NULL with initData → INVAL --------------------- */
    {
        uint8_t avcc[] = { 0x01 };
        uint8_t init_raw[512];
        size_t init_len = build_avc_init(init_raw, 90000, 640, 480,
                                          avcc, sizeof(avcc));
        b64_result_t b64 = encode_b64(alloc, init_raw, init_len);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("v");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_init_data = true;
        t.init_data = b64.bytes;

        moq_media_track_info_t info;
        CHECK(moq_msf_track_to_media_info(
            NULL, &t, &info, NULL, NULL) == MOQ_ERR_INVAL);

        moq_rcbuf_decref(b64.buf);
    }

    /* -- allocator balance: out_init path ----------------------------- */
    {
        uint8_t avcc[] = { 0x01, 0x64 };
        uint8_t init_raw[512];
        size_t init_len = build_avc_init(init_raw, 30000, 320, 240,
                                          avcc, sizeof(avcc));
        b64_result_t b64 = encode_b64(alloc, init_raw, init_len);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("v");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_init_data = true;
        t.init_data = b64.bytes;

        oom_state_t oom = { 0, 0, 0 };
        moq_alloc_t fa = oom_allocator(&oom);

        moq_media_track_info_t info;
        moq_cmaf_init_info_t cinit;
        moq_rcbuf_t *init_buf = NULL;
        moq_result_t rc = moq_msf_track_to_media_info(
            &fa, &t, &info, &cinit, &init_buf);
        CHECK(rc == MOQ_OK);
        CHECK(init_buf != NULL);
        CHECK(oom.balance > 0);
        moq_rcbuf_decref(init_buf);
        CHECK(oom.balance == 0);

        moq_rcbuf_decref(b64.buf);
    }

    /* -- allocator balance: out_init == NULL path --------------------- */
    {
        uint8_t avcc[] = { 0x01 };
        uint8_t init_raw[512];
        size_t init_len = build_avc_init(init_raw, 90000, 640, 480,
                                          avcc, sizeof(avcc));
        b64_result_t b64 = encode_b64(alloc, init_raw, init_len);

        moq_msf_track_t t;
        memset(&t, 0, sizeof(t));
        t.struct_size = sizeof(t);
        t.name = lit("v");
        t.packaging = lit("cmaf");
        t.is_live = true;
        t.has_role = true;
        t.role = lit("video");
        t.has_init_data = true;
        t.init_data = b64.bytes;

        oom_state_t oom = { 0, 0, 0 };
        moq_alloc_t fa = oom_allocator(&oom);

        moq_media_track_info_t info;
        moq_result_t rc = moq_msf_track_to_media_info(
            &fa, &t, &info, NULL, NULL);
        CHECK(rc == MOQ_OK);
        CHECK_EQ_U32(info.timescale, 90000);
        CHECK(oom.balance == 0);

        moq_rcbuf_decref(b64.buf);
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
