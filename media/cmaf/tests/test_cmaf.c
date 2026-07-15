#include <moq/cmaf.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "media_builders.h"  /* shared CENC-protected init builder */

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

/* -- Helpers: build synthetic ISO BMFF boxes ------------------------- */

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v;
}

/* Write a box header: [size:4][type:4]. Returns 8. */
static size_t box_hdr(uint8_t *p, uint32_t size, const char *type)
{
    wr32(p, size);
    memcpy(p + 4, type, 4);
    return 8;
}

/*
 * Build a minimal AVC init segment:
 * ftyp + moov(trak(mdia(mdhd + minf(stbl(stsd(avc1(avcC)))))))
 *
 * Returns total size written to buf.
 */
static size_t build_avc_init(uint8_t *buf, size_t cap,
                              uint32_t timescale, uint16_t w, uint16_t h,
                              const uint8_t *avcc_data, size_t avcc_len)
{
    (void)cap;
    size_t p = 0;

    /* ftyp */
    p += box_hdr(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    /* Start building inner boxes from the inside out. */
    /* avcC box */
    size_t avcc_box_size = 8 + avcc_len;

    /* avc1 sample entry: 8 (box hdr) + 78 (VisualSampleEntry fixed) + avcC */
    size_t avc1_size = 8 + 78 + avcc_box_size;

    /* stsd: 8 (hdr) + 8 (version+flags+entry_count) + avc1 */
    size_t stsd_size = 8 + 8 + avc1_size;
    size_t stbl_size = 8 + stsd_size;
    size_t minf_size = 8 + stbl_size;

    /* mdhd: version 0 = 8 (hdr) + 4 (ver/flags) + 12 (fields with timescale at +8) */
    size_t mdhd_size = 8 + 4 + 12;
    size_t mdia_size = 8 + mdhd_size + minf_size;
    size_t trak_size = 8 + mdia_size;
    size_t moov_size = 8 + trak_size;

    /* moov */
    p += box_hdr(buf + p, (uint32_t)moov_size, "moov");

    /* trak */
    p += box_hdr(buf + p, (uint32_t)trak_size, "trak");

    /* mdia */
    p += box_hdr(buf + p, (uint32_t)mdia_size, "mdia");

    /* mdhd (version 0) */
    p += box_hdr(buf + p, (uint32_t)mdhd_size, "mdhd");
    wr32(buf + p, 0); p += 4; /* version + flags */
    wr32(buf + p, 0); p += 4; /* creation_time */
    wr32(buf + p, 0); p += 4; /* modification_time */
    wr32(buf + p, timescale); p += 4; /* timescale */

    /* minf */
    p += box_hdr(buf + p, (uint32_t)minf_size, "minf");

    /* stbl */
    p += box_hdr(buf + p, (uint32_t)stbl_size, "stbl");

    /* stsd */
    p += box_hdr(buf + p, (uint32_t)stsd_size, "stsd");
    wr32(buf + p, 0); p += 4; /* version + flags */
    wr32(buf + p, 1); p += 4; /* entry_count */

    /* avc1 */
    p += box_hdr(buf + p, (uint32_t)avc1_size, "avc1");
    memset(buf + p, 0, 78);
    wr16(buf + p + 24, w);
    wr16(buf + p + 26, h);
    p += 78;

    /* avcC */
    p += box_hdr(buf + p, (uint32_t)avcc_box_size, "avcC");
    memcpy(buf + p, avcc_data, avcc_len);
    p += avcc_len;

    return p;
}

/*
 * Build a minimal AAC init segment:
 * ftyp + moov(trak(mdia(mdhd + minf(stbl(stsd(mp4a(esds)))))))
 */
static size_t build_aac_init(uint8_t *buf, size_t cap,
                              uint32_t timescale, uint16_t samplerate,
                              uint16_t channels,
                              const uint8_t *asc, size_t asc_len)
{
    (void)cap;
    size_t p = 0;

    /* ftyp */
    p += box_hdr(buf + p, 20, "ftyp");
    memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    memcpy(buf + p, "isom", 4); p += 4;

    /* esds body: version(4) + ES_Descriptor wrapper around ASC.
     * Minimal: tag=3, len, ES_ID(2), flags(1),
     *   tag=4, len, objectType(1), streamType(1), bufferSize(3), maxBr(4), avgBr(4),
     *     tag=5, len, ASC data */
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

    /* mdhd */
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

    /* mp4a */
    p += box_hdr(buf + p, (uint32_t)mp4a_size, "mp4a");
    memset(buf + p, 0, 28);
    wr16(buf + p + 8, channels);
    wr16(buf + p + 24, samplerate);
    p += 28;

    /* esds */
    p += box_hdr(buf + p, (uint32_t)esds_size, "esds");
    wr32(buf + p, 0); p += 4; /* version+flags */

    /* ES_Descriptor tag=3 */
    buf[p++] = 0x03;
    buf[p++] = (uint8_t)(es_desc_len - 2);
    wr16(buf + p, 0); p += 2; /* ES_ID */
    buf[p++] = 0; /* flags */

    /* DecoderConfigDescriptor tag=4 */
    buf[p++] = 0x04;
    buf[p++] = (uint8_t)(dec_cfg_len - 2);
    buf[p++] = 0x40; /* objectTypeIndication: AAC */
    buf[p++] = 0x15; /* streamType: audio */
    buf[p++] = 0; buf[p++] = 0; buf[p++] = 0; /* bufferSizeDB */
    wr32(buf + p, 0); p += 4; /* maxBitrate */
    wr32(buf + p, 0); p += 4; /* avgBitrate */

    /* DecoderSpecificInfo tag=5 */
    buf[p++] = 0x05;
    buf[p++] = (uint8_t)asc_len;
    memcpy(buf + p, asc, asc_len);
    p += asc_len;

    return p;
}

/*
 * Build a minimal fragment: moof(traf(tfhd + tfdt + trun)) + mdat.
 */
static size_t build_fragment(uint8_t *buf, size_t cap,
                              uint64_t base_time, uint32_t trun_flags,
                              const moq_cmaf_sample_t *samples,
                              size_t sample_count,
                              const uint8_t *mdat_payload,
                              size_t mdat_len)
{
    size_t p = 0;

    /* Compute trun size. */
    size_t per_sample = 0;
    if (trun_flags & 0x100) per_sample += 4;
    if (trun_flags & 0x200) per_sample += 4;
    if (trun_flags & 0x400) per_sample += 4;
    if (trun_flags & 0x800) per_sample += 4;
    size_t trun_size = 8 + 8 + sample_count * per_sample;

    size_t tfdt_size = 8 + 4 + 8; /* version 1: 8-byte time */
    size_t tfhd_size = 8 + 8;     /* minimal: version+flags + track_id */
    size_t traf_size = 8 + tfhd_size + tfdt_size + trun_size;
    size_t moof_size = 8 + traf_size;

    /* Guard the caller's buffer: total = moof + (mdat box header + payload).
     * Previously `cap` was ignored, which let an undersized fixture buffer
     * silently overflow (caught by ASAN). Assert instead of overflowing. */
    assert(moof_size + 8 + mdat_len <= cap &&
           "build_fragment: output buffer too small");

    /* moof */
    p += box_hdr(buf + p, (uint32_t)moof_size, "moof");
    p += box_hdr(buf + p, (uint32_t)traf_size, "traf");

    /* tfhd: minimal */
    p += box_hdr(buf + p, (uint32_t)tfhd_size, "tfhd");
    wr32(buf + p, 0); p += 4; /* version+flags */
    wr32(buf + p, 1); p += 4; /* track_id */

    /* tfdt: version 1 */
    p += box_hdr(buf + p, (uint32_t)tfdt_size, "tfdt");
    wr32(buf + p, 0x01000000); p += 4; /* version=1, flags=0 */
    wr32(buf + p, (uint32_t)(base_time >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base_time); p += 4;

    /* trun */
    p += box_hdr(buf + p, (uint32_t)trun_size, "trun");
    wr32(buf + p, trun_flags); p += 4; /* version+flags */
    wr32(buf + p, (uint32_t)sample_count); p += 4;

    for (size_t i = 0; i < sample_count; i++) {
        if (trun_flags & 0x100) { wr32(buf + p, samples[i].duration); p += 4; }
        if (trun_flags & 0x200) { wr32(buf + p, samples[i].size); p += 4; }
        if (trun_flags & 0x400) { wr32(buf + p, samples[i].flags); p += 4; }
        if (trun_flags & 0x800) { wr32(buf + p, (uint32_t)samples[i].composition_offset); p += 4; }
    }

    /* mdat */
    p += box_hdr(buf + p, (uint32_t)(8 + mdat_len), "mdat");
    memcpy(buf + p, mdat_payload, mdat_len);
    p += mdat_len;

    return p;
}

/* -- CMSF §3.3 builders (include mfhd; for validate_object tests) ---- */

/* A 16-byte fullbox carrying a single u32 payload word (mfhd seq, tfhd
 * track_ID, tfdt base time). */
static size_t put_box_u32(uint8_t *b, const char *type, uint32_t v)
{
    size_t p = box_hdr(b, 16, type);
    wr32(b + p, 0); p += 4;   /* version + flags */
    wr32(b + p, v); p += 4;   /* payload word */
    return p;                 /* 16 */
}

/*
 * Build a moof: moof( mfhd? traf[+traf] ).
 *   include_mfhd=false omits the mandatory mfhd (CMSF §3.3);
 *   two_traf=true adds a second track fragment (multi-track);
 *   include_trun=false omits the sample table;
 *   first_sample_flags drives SAP classification of the chunk's sample.
 */
static size_t build_moof(uint8_t *out, uint32_t track_id,
                         uint32_t first_sample_flags,
                         int include_mfhd, int two_traf, int include_trun)
{
    uint8_t traf_inner[64]; size_t ti = 0;
    ti += put_box_u32(traf_inner + ti, "tfhd", track_id);
    ti += put_box_u32(traf_inner + ti, "tfdt", 0);
    if (include_trun) {
        ti += box_hdr(traf_inner + ti, 20, "trun");  /* flags 0x400, 1 sample */
        wr32(traf_inner + ti, 0x00000400); ti += 4;
        wr32(traf_inner + ti, 1); ti += 4;
        wr32(traf_inner + ti, first_sample_flags); ti += 4;
    }

    uint8_t traf[80]; size_t tp = 0;
    tp += box_hdr(traf + tp, (uint32_t)(8 + ti), "traf");
    memcpy(traf + tp, traf_inner, ti); tp += ti;

    uint8_t moofc[256]; size_t mc = 0;
    if (include_mfhd) mc += put_box_u32(moofc + mc, "mfhd", 1);
    memcpy(moofc + mc, traf, tp); mc += tp;
    if (two_traf) { memcpy(moofc + mc, traf, tp); mc += tp; }

    size_t p = 0;
    p += box_hdr(out + p, (uint32_t)(8 + mc), "moof");
    memcpy(out + p, moofc, mc); p += mc;
    return p;
}

static size_t build_mdat(uint8_t *out, uint8_t byte)
{
    size_t p = box_hdr(out, 9, "mdat");
    out[p++] = byte;
    return p;
}

/* A structurally-well-formed moof whose trun declares zero samples. */
static size_t build_moof_zero_samples(uint8_t *out, uint32_t track_id)
{
    uint8_t traf_inner[64]; size_t ti = 0;
    ti += put_box_u32(traf_inner + ti, "tfhd", track_id);
    ti += put_box_u32(traf_inner + ti, "tfdt", 0);
    ti += box_hdr(traf_inner + ti, 16, "trun");   /* flags 0x400, count 0 */
    wr32(traf_inner + ti, 0x00000400); ti += 4;
    wr32(traf_inner + ti, 0); ti += 4;            /* sample_count = 0 */

    uint8_t traf[80]; size_t tp = 0;
    tp += box_hdr(traf + tp, (uint32_t)(8 + ti), "traf");
    memcpy(traf + tp, traf_inner, ti); tp += ti;

    uint8_t moofc[256]; size_t mc = 0;
    mc += put_box_u32(moofc + mc, "mfhd", 1);
    memcpy(moofc + mc, traf, tp); mc += tp;

    size_t p = 0;
    p += box_hdr(out + p, (uint32_t)(8 + mc), "moof");
    memcpy(out + p, moofc, mc); p += mc;
    return p;
}

/* One complete CMAF chunk: moof(mfhd traf(tfhd tfdt trun)) + mdat(1 byte). */
static size_t build_cmaf_chunk(uint8_t *out, uint32_t track_id,
                               uint32_t first_sample_flags,
                               int include_mfhd, int two_traf,
                               uint8_t mdat_byte)
{
    size_t p = build_moof(out, track_id, first_sample_flags,
                          include_mfhd, two_traf, 1 /*trun*/);
    p += build_mdat(out + p, mdat_byte);
    return p;
}

/* A moof whose trun declares `declared` samples (with per-sample flags, 0x400)
 * but carries ZERO sample entries -- it lies about its sample count. A full
 * parse must reject it; a buffer-limited parse returns MOQ_ERR_BUFFER after
 * only recording the count. */
static size_t build_moof_oversized_trun(uint8_t *out, uint32_t track_id,
                                        uint32_t declared)
{
    uint32_t trun_size  = 16;                 /* ver/flags + count, NO entries */
    uint32_t traf_inner = 16 + 16 + trun_size;/* tfhd + tfdt + trun */
    uint32_t traf_size  = 8 + traf_inner;
    uint32_t moofc      = 16 /*mfhd*/ + traf_size;
    uint32_t moof_size  = 8 + moofc;

    size_t p = 0;
    p += box_hdr(out + p, moof_size, "moof");
    p += put_box_u32(out + p, "mfhd", 1);
    p += box_hdr(out + p, traf_size, "traf");
    p += put_box_u32(out + p, "tfhd", track_id);
    p += put_box_u32(out + p, "tfdt", 0);
    p += box_hdr(out + p, trun_size, "trun");
    wr32(out + p, 0x00000400); p += 4;        /* version/flags: sample-flags */
    wr32(out + p, declared);   p += 4;        /* sample_count (lie) */
    return p;
}

/* A well-formed moof with exactly `n` samples (flags 0x400): sample 0 carries
 * `first_flags`, the rest are non-sync (0x00010000). Used for the static-buffer
 * boundary test (n == 512). */
static size_t build_moof_n_samples(uint8_t *out, uint32_t track_id,
                                   uint32_t n, uint32_t first_flags)
{
    uint32_t trun_size  = 16 + 4u * n;        /* ver/flags + count + n*flags */
    uint32_t traf_inner = 16 + 16 + trun_size;
    uint32_t traf_size  = 8 + traf_inner;
    uint32_t moofc      = 16 + traf_size;
    uint32_t moof_size  = 8 + moofc;

    size_t p = 0;
    p += box_hdr(out + p, moof_size, "moof");
    p += put_box_u32(out + p, "mfhd", 1);
    p += box_hdr(out + p, traf_size, "traf");
    p += put_box_u32(out + p, "tfhd", track_id);
    p += put_box_u32(out + p, "tfdt", 0);
    p += box_hdr(out + p, trun_size, "trun");
    wr32(out + p, 0x00000400); p += 4;
    wr32(out + p, n);          p += 4;
    for (uint32_t i = 0; i < n; i++) {
        wr32(out + p, i == 0 ? first_flags : 0x00010000u); p += 4;
    }
    return p;
}

/* AVC init segment that includes a tkhd carrying track_ID. */
static size_t build_avc_init_tkhd(uint8_t *buf, uint32_t timescale,
                                  uint32_t track_id,
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
    size_t tkhd_size = 24;            /* hdr + ver/flags + creation + mod + track_ID */
    size_t trak_size = 8 + tkhd_size + mdia_size;
    size_t moov_size = 8 + trak_size;

    p += box_hdr(buf + p, (uint32_t)moov_size, "moov");
    p += box_hdr(buf + p, (uint32_t)trak_size, "trak");

    /* tkhd v0: track_ID at body offset 12 */
    p += box_hdr(buf + p, (uint32_t)tkhd_size, "tkhd");
    wr32(buf + p, 0); p += 4;          /* version + flags */
    wr32(buf + p, 0); p += 4;          /* creation_time */
    wr32(buf + p, 0); p += 4;          /* modification_time */
    wr32(buf + p, track_id); p += 4;   /* track_ID */

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
    memset(buf + p, 0, 78); p += 78;
    p += box_hdr(buf + p, (uint32_t)avcc_box_size, "avcC");
    memcpy(buf + p, avcc_data, avcc_len); p += avcc_len;
    return p;
}

/* ================================================================== */

int main(void)
{
    /* -- 1. init functions null-safe ---------------------------------- */
    {
        moq_cmaf_init_info_init(NULL);
        moq_cmaf_fragment_info_init(NULL, NULL, 0);
    }

    /* -- 2. parse minimal AVC init ----------------------------------- */
    {
        uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1E, 0xFF };
        uint8_t buf[512];
        size_t len = build_avc_init(buf, sizeof(buf), 90000, 1920, 1080,
                                     avcc, sizeof(avcc));

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        moq_result_t rc = moq_cmaf_parse_init(
            (moq_bytes_t){ buf, len }, &info);
        CHECK(rc == MOQ_OK);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AVC);
        CHECK(info.timescale == 90000);
        CHECK(info.width == 1920);
        CHECK(info.height == 1080);
        CHECK(info.codec_config.len == sizeof(avcc));
        CHECK(memcmp(info.codec_config.data, avcc, sizeof(avcc)) == 0);
        /* Borrowed: pointer within buf. */
        CHECK(info.codec_config.data >= buf);
        CHECK(info.codec_config.data < buf + len);
    }

    /* -- 3. parse minimal AAC init ----------------------------------- */
    {
        uint8_t asc[] = { 0x11, 0x90 }; /* AAC-LC 48000Hz stereo */
        uint8_t buf[512];
        size_t len = build_aac_init(buf, sizeof(buf), 48000, 48000, 2,
                                     asc, sizeof(asc));

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        moq_result_t rc = moq_cmaf_parse_init(
            (moq_bytes_t){ buf, len }, &info);
        CHECK(rc == MOQ_OK);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AAC);
        CHECK(info.timescale == 48000);
        CHECK(info.samplerate == 48000);
        CHECK(info.channel_count == 2);
        CHECK(info.codec_config.len == sizeof(asc));
        CHECK(memcmp(info.codec_config.data, asc, sizeof(asc)) == 0);
    }

    /* -- 4. AV1/Opus init: TODO when test synthesis is straightforward */

    /* -- 5. parse fragment with one sample ---------------------------- */
    {
        moq_cmaf_sample_t in_samples[] = {{ 1000, 500, 0, 0 }};
        uint8_t payload[] = { 0xCA, 0xFE };
        uint8_t buf[256];
        size_t len = build_fragment(buf, sizeof(buf), 90000,
            0x300, in_samples, 1, payload, sizeof(payload));

        moq_cmaf_sample_t out_samples[8];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out_samples, 8);

        moq_result_t rc = moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, len }, &frag);
        CHECK(rc == MOQ_OK);
        CHECK(frag.has_base_decode_time == true);
        CHECK(frag.base_decode_time == 90000);
        CHECK(frag.sample_count == 1);
        CHECK(frag.samples[0].duration == 1000);
        CHECK(frag.samples[0].size == 500);
        CHECK(frag.mdat.len == sizeof(payload));
        CHECK(memcmp(frag.mdat.data, payload, sizeof(payload)) == 0);
    }

    /* -- 6. fragment with three samples ------------------------------ */
    {
        moq_cmaf_sample_t in[] = {
            { 1000, 100, 0x02000000, 50 },
            { 1000, 200, 0, -25 },
            { 1000, 150, 0, 0 },
        };
        uint8_t payload[450];
        memset(payload, 0xAB, sizeof(payload));
        uint8_t buf[1024];  /* 450B payload + ~124B boxes > 512 */
        size_t len = build_fragment(buf, sizeof(buf), 0,
            0xF00, in, 3, payload, sizeof(payload));

        moq_cmaf_sample_t out[8];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 8);

        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, len }, &frag) == MOQ_OK);
        CHECK(frag.sample_count == 3);
        CHECK(out[0].duration == 1000);
        CHECK(out[0].size == 100);
        CHECK(out[0].flags == 0x02000000);
        CHECK(out[0].composition_offset == 50);
        CHECK(out[1].size == 200);
        CHECK(out[1].composition_offset == -25);
        CHECK(out[2].size == 150);
    }

    /* -- 7. fragment using tfhd defaults ----------------------------- */
    {
        moq_cmaf_sample_t in[] = {{ 0, 0, 0, 0 }};
        uint8_t payload[] = { 0x01 };
        uint8_t buf[256];

        /* Build manually: moof(traf(tfhd with defaults + tfdt + trun no per-sample)) */
        size_t p = 0;
        size_t tfhd_sz = 8 + 8 + 4 + 4; /* flags 0x18: default_duration + default_size */
        size_t tfdt_sz = 8 + 4 + 4;
        size_t trun_sz = 8 + 8; /* no per-sample data */
        size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
        size_t moof_sz = 8 + traf_sz;

        p += box_hdr(buf + p, (uint32_t)moof_sz, "moof");
        p += box_hdr(buf + p, (uint32_t)traf_sz, "traf");

        /* tfhd: flags = 0x18 (default_duration + default_size) */
        p += box_hdr(buf + p, (uint32_t)tfhd_sz, "tfhd");
        wr32(buf + p, 0x00000018); p += 4;
        wr32(buf + p, 1); p += 4;
        wr32(buf + p, 512); p += 4; /* default_sample_duration */
        wr32(buf + p, 64); p += 4;  /* default_sample_size */

        /* tfdt v0 */
        p += box_hdr(buf + p, (uint32_t)tfdt_sz, "tfdt");
        wr32(buf + p, 0); p += 4;
        wr32(buf + p, 1000); p += 4;

        /* trun: 1 sample, no per-sample fields */
        p += box_hdr(buf + p, (uint32_t)trun_sz, "trun");
        wr32(buf + p, 0); p += 4;
        wr32(buf + p, 1); p += 4;

        /* mdat */
        p += box_hdr(buf + p, 8 + 1, "mdat");
        buf[p++] = 0x01;

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);

        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, p }, &frag) == MOQ_OK);
        CHECK(frag.sample_count == 1);
        CHECK(frag.default_sample_duration == 512);
        CHECK(frag.default_sample_size == 64);
        CHECK(out[0].duration == 512);
        CHECK(out[0].size == 64);
    }

    /* -- 8. sample_count > cap -> ERR_BUFFER ------------------------- */
    {
        moq_cmaf_sample_t in[] = {
            { 100, 10, 0, 0 }, { 100, 10, 0, 0 }, { 100, 10, 0, 0 },
        };
        uint8_t payload[] = { 0x00 };
        uint8_t buf[256];
        size_t len = build_fragment(buf, sizeof(buf), 0,
            0x300, in, 3, payload, sizeof(payload));

        moq_cmaf_sample_t out[2]; /* only room for 2 */
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 2);

        moq_result_t rc = moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, len }, &frag);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK(frag.sample_count == 3);
    }

    /* -- 9. unknown boxes skipped ------------------------------------ */
    {
        uint8_t avcc[] = { 0x01 };
        uint8_t buf[512];
        size_t len = build_avc_init(buf, sizeof(buf), 30000, 640, 480,
                                     avcc, sizeof(avcc));
        /* Prepend an unknown box. */
        uint8_t buf2[600];
        size_t unk = box_hdr(buf2, 16, "xunk");
        wr32(buf2 + unk, 0); unk += 4;
        wr32(buf2 + unk, 0); unk += 4;
        memcpy(buf2 + unk, buf, len);

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(
            (moq_bytes_t){ buf2, unk + len }, &info) == MOQ_OK);
        CHECK(info.timescale == 30000);
    }

    /* -- 10. box size < header -> PROTO ------------------------------ */
    {
        uint8_t buf[] = { 0x00, 0x00, 0x00, 0x04, 'm', 'o', 'o', 'v' };
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(
            (moq_bytes_t){ buf, sizeof(buf) }, &info) == MOQ_ERR_PROTO);
    }

    /* -- 11. box size > remaining -> PROTO --------------------------- */
    {
        uint8_t buf[] = { 0x00, 0x00, 0x01, 0x00, 'm', 'o', 'o', 'v' };
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(
            (moq_bytes_t){ buf, sizeof(buf) }, &info) == MOQ_ERR_PROTO);
    }

    /* -- 12. truncated mdhd -> PROTO --------------------------------- */
    {
        uint8_t buf[128];
        size_t p = 0;
        p += box_hdr(buf + p, 60, "moov");
        p += box_hdr(buf + p, 52, "trak");
        p += box_hdr(buf + p, 44, "mdia");
        p += box_hdr(buf + p, 12, "mdhd"); /* too small: needs 4+12 */
        wr32(buf + p, 0); p += 4;

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(
            (moq_bytes_t){ buf, p }, &info) == MOQ_ERR_PROTO);
    }

    /* -- 13. truncated trun -> PROTO --------------------------------- */
    {
        uint8_t buf[128];
        size_t p = 0;
        size_t moof_sz = 8 + 8 + 8 + 12; /* traf(tfhd + trun(truncated)) */
        p += box_hdr(buf + p, (uint32_t)(moof_sz), "moof");
        p += box_hdr(buf + p, (uint32_t)(moof_sz - 8), "traf");
        p += box_hdr(buf + p, 8 + 8, "tfhd");
        wr32(buf + p, 0); p += 4;
        wr32(buf + p, 1); p += 4;
        /* trun with size claiming more than available */
        p += box_hdr(buf + p, 12, "trun"); /* only 4 bytes after hdr, need 8 */

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);
        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, p }, &frag) == MOQ_ERR_PROTO);
    }

    /* -- 14. moof without trun -> PROTO ------------------------------ */
    {
        uint8_t buf[64];
        size_t p = 0;
        p += box_hdr(buf + p, 24, "moof");
        p += box_hdr(buf + p, 16, "traf");
        p += box_hdr(buf + p, 8, "tfhd"); /* minimal, no trun */
        p += box_hdr(buf + p, 8, "mdat");

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);
        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, p }, &frag) == MOQ_ERR_PROTO);
    }

    /* -- 15. NULL args -> INVAL -------------------------------------- */
    {
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ NULL, 0 }, &info)
            == MOQ_ERR_INVAL);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ (uint8_t*)"x", 1 }, NULL)
            == MOQ_ERR_INVAL);

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ NULL, 0 }, &frag)
            == MOQ_ERR_INVAL);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ (uint8_t*)"x", 1 }, NULL)
            == MOQ_ERR_INVAL);
    }

    /* -- 16. no sample buffer with samples -> ERR_BUFFER ------------- */
    {
        moq_cmaf_sample_t in[] = {{ 100, 10, 0, 0 }};
        uint8_t payload[] = { 0x00 };
        uint8_t buf[256];
        size_t len = build_fragment(buf, sizeof(buf), 0,
            0x300, in, 1, payload, sizeof(payload));

        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, NULL, 0);

        moq_result_t rc = moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, len }, &frag);
        CHECK(rc == MOQ_ERR_BUFFER);
        CHECK(frag.sample_count == 1);
    }

    /* -- 17. missing mdat -> PROTO ------------------------------------ */
    {
        uint8_t buf[128];
        size_t p = 0;
        moq_cmaf_sample_t in[] = {{ 100, 10, 0, 0 }};

        /* Build moof only, no mdat. */
        size_t trun_sz = 8 + 8 + 8;
        size_t tfhd_sz = 8 + 8;
        size_t traf_sz = 8 + tfhd_sz + trun_sz;
        size_t moof_sz = 8 + traf_sz;
        p += box_hdr(buf + p, (uint32_t)moof_sz, "moof");
        p += box_hdr(buf + p, (uint32_t)traf_sz, "traf");
        p += box_hdr(buf + p, (uint32_t)tfhd_sz, "tfhd");
        wr32(buf + p, 0); p += 4;
        wr32(buf + p, 1); p += 4;
        p += box_hdr(buf + p, (uint32_t)trun_sz, "trun");
        wr32(buf + p, 0x00000300); p += 4;
        wr32(buf + p, 1); p += 4;
        wr32(buf + p, in[0].duration); p += 4;
        wr32(buf + p, in[0].size); p += 4;

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);
        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, p }, &frag) == MOQ_ERR_PROTO);
    }

    /* -- 18. truncated tfhd flagged field -> PROTO --------------------- */
    {
        uint8_t buf[64];
        size_t p = 0;
        /* tfhd with flag 0x08 (default_duration) but no bytes for it. */
        size_t tfhd_sz = 8 + 8; /* version+flags + track_id only */
        size_t traf_sz = 8 + tfhd_sz;
        size_t moof_sz = 8 + traf_sz;
        p += box_hdr(buf + p, (uint32_t)moof_sz, "moof");
        p += box_hdr(buf + p, (uint32_t)traf_sz, "traf");
        p += box_hdr(buf + p, (uint32_t)tfhd_sz, "tfhd");
        wr32(buf + p, 0x00000008); p += 4; /* flag: default_duration present */
        wr32(buf + p, 1); p += 4; /* track_id */
        /* no bytes for default_duration */
        p += box_hdr(buf + p, 8, "mdat");

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);
        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, p }, &frag) == MOQ_ERR_PROTO);
    }

    /* -- 19. esds without tag 0x05 -> PROTO --------------------------- */
    {
        /* AAC init with esds that has no DecoderSpecificInfo */
        uint8_t buf[512];
        size_t p = 0;
        /* ftyp */
        p += box_hdr(buf + p, 20, "ftyp");
        memcpy(buf + p, "isom", 4); p += 4;
        wr32(buf + p, 0); p += 4;
        memcpy(buf + p, "isom", 4); p += 4;

        /* Build esds with only tag 3 + tag 4, no tag 5 */
        size_t esds_body = 4 + 2 + 3 + 2 + 13; /* ver + ES(tag+len+3) + DC(tag+len+13) */
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
        wr32(buf + p, 48000); p += 4;
        p += box_hdr(buf + p, (uint32_t)minf_size, "minf");
        p += box_hdr(buf + p, (uint32_t)stbl_size, "stbl");
        p += box_hdr(buf + p, (uint32_t)stsd_size, "stsd");
        wr32(buf + p, 0); p += 4;
        wr32(buf + p, 1); p += 4;
        p += box_hdr(buf + p, (uint32_t)mp4a_size, "mp4a");
        memset(buf + p, 0, 28);
        wr16(buf + p + 8, 2);
        wr16(buf + p + 24, 48000);
        p += 28;
        p += box_hdr(buf + p, (uint32_t)esds_size, "esds");
        wr32(buf + p, 0); p += 4;
        buf[p++] = 0x03; buf[p++] = (uint8_t)(3 + 2 + 13);
        wr16(buf + p, 0); p += 2;
        buf[p++] = 0;
        buf[p++] = 0x04; buf[p++] = 13;
        memset(buf + p, 0, 13); p += 13;
        /* no tag 0x05 */

        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init(
            (moq_bytes_t){ buf, p }, &info) == MOQ_ERR_PROTO);
    }

    /* -- 20. malformed box inside moof -> PROTO ----------------------- */
    {
        uint8_t buf[32];
        size_t p = 0;
        /* moof containing a child with size < 8. */
        p += box_hdr(buf + p, 24, "moof");
        /* Bad child: size=4. */
        wr32(buf + p, 4); p += 4;
        memcpy(buf + p, "traf", 4); p += 4;
        wr32(buf + p, 0); p += 4;
        wr32(buf + p, 0); p += 4;

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);
        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, p }, &frag) == MOQ_ERR_PROTO);
    }

    /* -- 21. reuse: defaults not inherited between parses --------------- */
    {
        /* Fragment A: tfhd with default_duration=512, default_size=64. */
        uint8_t bufA[256];
        size_t pA = 0;
        size_t tfhd_szA = 8 + 8 + 4 + 4;
        size_t trun_szA = 8 + 8;
        size_t traf_szA = 8 + tfhd_szA + trun_szA;
        size_t moof_szA = 8 + traf_szA;
        pA += box_hdr(bufA + pA, (uint32_t)moof_szA, "moof");
        pA += box_hdr(bufA + pA, (uint32_t)traf_szA, "traf");
        pA += box_hdr(bufA + pA, (uint32_t)tfhd_szA, "tfhd");
        wr32(bufA + pA, 0x00000018); pA += 4;
        wr32(bufA + pA, 1); pA += 4;
        wr32(bufA + pA, 512); pA += 4;
        wr32(bufA + pA, 64); pA += 4;
        pA += box_hdr(bufA + pA, (uint32_t)trun_szA, "trun");
        wr32(bufA + pA, 0); pA += 4;
        wr32(bufA + pA, 1); pA += 4;
        pA += box_hdr(bufA + pA, 9, "mdat");
        bufA[pA++] = 0x01;

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);

        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ bufA, pA }, &frag) == MOQ_OK);
        CHECK(frag.default_sample_duration == 512);
        CHECK(out[0].duration == 512);
        CHECK(out[0].size == 64);

        /* Fragment B: tfhd with no defaults, trun with per-sample. */
        moq_cmaf_sample_t inB[] = {{ 100, 10, 0, 0 }};
        uint8_t bufB[256];
        size_t lenB = build_fragment(bufB, sizeof(bufB), 0,
            0x300, inB, 1, (uint8_t[]){0x02}, 1);

        /* Re-parse using the SAME frag struct without reinit.
         * parse_fragment must reset defaults internally. */
        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ bufB, lenB }, &frag) == MOQ_OK);
        CHECK(frag.default_sample_duration == 0);
        CHECK(frag.default_sample_size == 0);
        CHECK(out[0].duration == 100);
        CHECK(out[0].size == 10);
    }

    /* -- 22. trailing bytes after valid boxes -> PROTO ----------------- */
    {
        moq_cmaf_sample_t in[] = {{ 100, 10, 0, 0 }};
        uint8_t payload[] = { 0x00 };
        uint8_t buf[256];
        size_t len = build_fragment(buf, sizeof(buf), 0,
            0x300, in, 1, payload, sizeof(payload));
        buf[len++] = 0xFF; /* trailing junk byte */

        moq_cmaf_sample_t out[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, out, 4);
        CHECK(moq_cmaf_parse_fragment(
            (moq_bytes_t){ buf, len }, &frag) == MOQ_ERR_PROTO);
    }

    /* -- 23. init track_ID surfaced from tkhd ------------------------ */
    {
        uint8_t avcc[] = { 0x01 };
        uint8_t buf[512];
        size_t len = build_avc_init_tkhd(buf, 90000, 7, avcc, sizeof(avcc));
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info) == MOQ_OK);
        CHECK(info.track_id == 7);
        CHECK(info.timescale == 90000);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AVC);
    }

    /* -- 24. SAP classification from sample flags --------------------- */
    {
        CHECK(moq_cmaf_sap_from_sample_flags(0x00000000) == MOQ_SAP_UNKNOWN); /* sync */
        CHECK(moq_cmaf_sap_from_sample_flags(0x02000000) == MOQ_SAP_UNKNOWN); /* sync, indep */
        CHECK(moq_cmaf_sap_from_sample_flags(0x01010000) == MOQ_SAP_NONE);    /* non-sync, depends */
        CHECK(moq_cmaf_sap_from_sample_flags(0x02010000) == MOQ_SAP_UNKNOWN); /* non-sync, indep */
    }

    /* -- 25. validate_object: valid single chunk --------------------- */
    {
        uint8_t obj[128];
        size_t len = build_cmaf_chunk(obj, 1, 0x02000000, 1, 0, 0xAA);
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, len }, &rep)
              == MOQ_OK);
        CHECK(rep.valid == true);
        CHECK(rep.reason == MOQ_CMAF_OK);
        CHECK(rep.chunk_count == 1);
        CHECK(rep.track_id == 1);
        CHECK(rep.starts_with_sync == true);
        CHECK(rep.sap_type == MOQ_SAP_UNKNOWN);
    }

    /* -- 26. validate_object + parse_fragment: multi-chunk ----------- */
    {
        uint8_t obj[256]; size_t n = 0;
        n += build_cmaf_chunk(obj + n, 1, 0x02000000, 1, 0, 0xAA);
        n += build_cmaf_chunk(obj + n, 1, 0x00000000, 1, 0, 0xBB);

        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_OK);
        CHECK(rep.valid == true);
        CHECK(rep.chunk_count == 2);

        moq_cmaf_sample_t s[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, s, 4);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &frag) == MOQ_OK);
        CHECK(frag.chunk_count == 2);   /* first chunk normalized, count reports 2 */
        CHECK(frag.track_id == 1);
    }

    /* -- 27. validate_object: missing mfhd --------------------------- */
    {
        uint8_t obj[128];
        size_t len = build_cmaf_chunk(obj, 1, 0x02000000, 0 /*no mfhd*/, 0, 0xAA);
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, len }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.valid == false);
        CHECK(rep.reason == MOQ_CMAF_ERR_MISSING_MFHD);
    }

    /* -- 28. validate_object: two traf -> multi-track ---------------- */
    {
        uint8_t obj[200];
        size_t len = build_cmaf_chunk(obj, 1, 0x02000000, 1, 1 /*two traf*/, 0xAA);
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, len }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_MULTI_TRACK);
    }

    /* -- 29. validate_object: track_ID mismatch vs init -------------- */
    {
        uint8_t obj[128];
        size_t len = build_cmaf_chunk(obj, 1, 0x02000000, 1, 0, 0xAA);
        moq_cmaf_init_info_t init;
        moq_cmaf_init_info_init(&init);
        init.track_id = 42;   /* object carries track_ID 1 */
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(&init, (moq_bytes_t){ obj, len }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_TRACK_ID_MISMATCH);
        CHECK(rep.track_id == 1);

        init.track_id = 1;    /* matching track_ID passes */
        CHECK(moq_cmaf_validate_object(&init, (moq_bytes_t){ obj, len }, &rep)
              == MOQ_OK);
        CHECK(rep.valid == true);
    }

    /* -- 30. validate_object: no chunk / stray box / NULL args ------- */
    {
        moq_cmaf_object_report_t rep;
        /* empty (non-NULL) payload -> no chunk */
        uint8_t any[8];
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ any, 0 }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_NO_CHUNK);
        /* a stray box where a moof is expected -> malformed ordering */
        uint8_t freebox[8];
        box_hdr(freebox, 8, "free");
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ freebox, 8 }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_MALFORMED);
        /* NULL args */
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ NULL, 0 }, &rep)
              == MOQ_ERR_INVAL);
    }

    /* -- 31. validate_object: moof/moof/mdat (not successive pairs) --- */
    {
        uint8_t obj[256]; size_t n = 0;
        n += build_moof(obj + n, 1, 0x02000000, 1, 0, 1);
        n += build_moof(obj + n, 1, 0x02000000, 1, 0, 1);
        n += build_mdat(obj + n, 0xAA);
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_MALFORMED);
    }

    /* -- 32. validate_object: mdat before moof ----------------------- */
    {
        uint8_t obj[160]; size_t n = 0;
        n += build_mdat(obj + n, 0xAA);
        n += build_moof(obj + n, 1, 0x02000000, 1, 0, 1);
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_MALFORMED);
    }

    /* -- 33. validate_object: moof without trun (no sample table) ----- */
    {
        uint8_t obj[160]; size_t n = 0;
        n += build_moof(obj + n, 1, 0x02000000, 1, 0, 0 /*no trun*/);
        n += build_mdat(obj + n, 0xAA);
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_NO_SAMPLES);
    }

    /* -- 34. validate_object: trailing moof with no mdat ------------- */
    {
        uint8_t obj[256]; size_t n = 0;
        n += build_cmaf_chunk(obj + n, 1, 0x02000000, 1, 0, 0xAA);
        n += build_moof(obj + n, 1, 0x02000000, 1, 0, 1);  /* dangling moof */
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_MALFORMED);
    }

    /* -- 35. validate_object: second chunk has zero-sample trun ------ */
    {
        uint8_t obj[256]; size_t n = 0;
        n += build_cmaf_chunk(obj + n, 1, 0x02000000, 1, 0, 0xAA); /* valid chunk 1 */
        n += build_moof_zero_samples(obj + n, 1);                  /* chunk 2: 0 samples */
        n += build_mdat(obj + n, 0xBB);
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_NO_SAMPLES);
        CHECK(rep.chunk_count == 1);   /* only the first chunk completed validly */
    }

    /* -- 35a. oversized trun (declares 513 samples, carries none) is rejected
     *    as MALFORMED before any required count is surfaced. The structural
     *    sufficiency check (declared count vs trun/fragment bytes) fires before
     *    sample_cap is consulted, so the result is PROTO regardless of cap, and
     *    the attacker-controlled count is NEVER exposed as a required size: a
     *    tiny malformed fragment must not surface an oversized-allocation
     *    signal such as a 513-sample BUFFER result at cap 512. */
    {
        uint8_t obj[256]; size_t n = 0;
        n += build_moof_oversized_trun(obj + n, 1, 513);
        n += build_mdat(obj + n, 0xAA);

        /* cap below the declared count: now PROTO, and no count surfaced. */
        moq_cmaf_sample_t small[512];
        moq_cmaf_fragment_info_t f512;
        moq_cmaf_fragment_info_init(&f512, small, 512);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &f512)
              == MOQ_ERR_PROTO);
        CHECK(f512.sample_count == 0);

        /* cap above the declared count: also PROTO, no count surfaced. */
        moq_cmaf_sample_t big[600];
        moq_cmaf_fragment_info_t f600;
        moq_cmaf_fragment_info_init(&f600, big, 600);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &f600)
              == MOQ_ERR_PROTO);
        CHECK(f600.sample_count == 0);

        /* Validator: must still reject. */
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.valid == false);
        CHECK(rep.reason == MOQ_CMAF_ERR_NO_SAMPLES);
    }

    /* -- 35a-i. tiny malformed trun with a HUGE count + insufficient
     *    per-sample bytes returns PROTO (not BUFFER) even with a tiny cap, and
     *    never surfaces the huge count as a required allocation size: a
     *    ~60-byte fragment must not advertise ~1e9 samples. */
    {
        uint8_t obj[256]; size_t n = 0;
        n += build_moof_oversized_trun(obj + n, 1, 1073741824u); /* 2^30 */
        n += build_mdat(obj + n, 0xAA);

        moq_cmaf_sample_t one[1];
        moq_cmaf_fragment_info_t f1;
        moq_cmaf_fragment_info_init(&f1, one, 1);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &f1)
              == MOQ_ERR_PROTO);
        CHECK(f1.sample_count == 0);   /* huge count NOT exposed */

        /* NULL sample buffer must behave the same (no required-count leak). */
        moq_cmaf_fragment_info_t f0;
        moq_cmaf_fragment_info_init(&f0, NULL, 0);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &f0)
              == MOQ_ERR_PROTO);
        CHECK(f0.sample_count == 0);
    }

    /* -- 35a-ii. malformed fragment: structurally-impossible trun with a huge
     *    count AND no mdat returns PROTO, never BUFFER. The BUFFER signal is
     *    withheld until the whole fragment validates. */
    {
        uint8_t obj[256]; size_t n = 0;
        n += build_moof_oversized_trun(obj + n, 1, 1073741824u);
        /* no mdat */

        moq_cmaf_sample_t one[1];
        moq_cmaf_fragment_info_t f1;
        moq_cmaf_fragment_info_init(&f1, one, 1);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &f1)
              == MOQ_ERR_PROTO);
        CHECK(f1.sample_count == 0);
    }

    /* -- 35a-iii. deferred BUFFER: a STRUCTURALLY VALID trun whose count
     *    exceeds sample_cap, but whose fragment is otherwise malformed (no
     *    mdat), returns PROTO -- the BUFFER signal is withheld until the whole
     *    fragment validates, so it cannot mask the missing mdat. */
    {
        static uint8_t obj[256]; size_t n = 0;
        n += build_moof_n_samples(obj + n, 1, 3, 0x02000000); /* valid 3-sample moof */
        /* no mdat */

        moq_cmaf_sample_t two[2];   /* cap below the real count of 3 */
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, two, 2);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &frag)
              == MOQ_ERR_PROTO);
        CHECK(frag.sample_count == 0);   /* no required count leaked on PROTO */
    }

    /* -- 35a-iv. malformed-after-trun with a SUFFICIENT buffer: a
     *    structurally valid trun whose fragment then breaks moof->mdat
     *    ordering (a second mdat where a moof is required) returns PROTO and
     *    must not leave the parsed count in out. The required count is
     *    committed only after the whole fragment validates. (RED: before the
     *    fix parse_trun stamped out->sample_count, leaking it on this PROTO.) */
    {
        static uint8_t obj[256]; size_t n = 0;
        n += build_moof_n_samples(obj + n, 1, 3, 0x02000000);
        n += build_mdat(obj + n, 0xAA);
        n += build_mdat(obj + n, 0xBB);   /* mdat where the next moof is expected */

        moq_cmaf_sample_t s[4];           /* cap >= the real count of 3 */
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, s, 4);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &frag)
              == MOQ_ERR_PROTO);
        CHECK(frag.sample_count == 0);
    }

    /* -- 35b. positive boundary: a well-formed 512-sample object (the static
     *    buffer maximum) still validates. */
    {
        static uint8_t obj[4096]; size_t n = 0;
        n += build_moof_n_samples(obj + n, 1, 512, 0x02000000); /* sync first */
        n += build_mdat(obj + n, 0xAA);
        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_OK);
        CHECK(rep.valid == true);
        CHECK(rep.reason == MOQ_CMAF_OK);
        CHECK(rep.chunk_count == 1);
        CHECK(rep.starts_with_sync == true);   /* sample 0 flags 0x02000000: sync */
    }

    /* -- 36. parse_fragment: rejects malformed moof/moof/mdat ordering */
    {
        uint8_t obj[256]; size_t n = 0;
        n += build_moof(obj + n, 1, 0x02000000, 1, 0, 1);
        n += build_moof(obj + n, 1, 0x02000000, 1, 0, 1);
        n += build_mdat(obj + n, 0xAA);
        moq_cmaf_sample_t s[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, s, 4);
        /* normal media path (parse_fragment) must reject, not just validate */
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &frag)
              == MOQ_ERR_PROTO);
    }

    /* -- 37. CENC protected init (encv): positive parse, tenc v0 ------ */
    {
        const uint8_t avcc[] = { 0x01, 0x42, 0xe0, 0x1e };
        const uint8_t kid[16] = {
            0x10, 0x77, 0xef, 0xec, 0xc0, 0xb2, 0x4d, 0x02,
            0xac, 0xe3, 0x3c, 0x1e, 0x52, 0xe2, 0xfb, 0x4b };
        uint8_t buf[1024];
        size_t len = moq_test_build_cmaf_init_cenc(
            buf, 9, 90000, 1920, 1080, avcc, sizeof(avcc),
            "cenc", 1, 8, kid, 0, MOQ_TEST_CENC_OK);
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info) == MOQ_OK);
        /* Original codec recovered via frma -> first-class protected track. */
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AVC);
        CHECK(info.timescale == 90000);
        CHECK(info.width == 1920);
        CHECK(info.height == 1080);
        CHECK(info.track_id == 9);
        CHECK(info.codec_config.len == sizeof(avcc));
        CHECK(memcmp(info.codec_config.data, avcc, sizeof(avcc)) == 0);
        /* codec_config borrows from the input buffer. */
        CHECK(info.codec_config.data >= buf &&
              info.codec_config.data < buf + len);
        /* CENC protection params. */
        CHECK(info.has_cenc == true);
        CHECK(info.scheme.len == 4);
        CHECK(memcmp(info.scheme.data, "cenc", 4) == 0);
        CHECK(info.default_is_protected == 1);
        CHECK(info.default_per_sample_iv_size == 8);
        CHECK(info.default_kid.len == 16);
        CHECK(memcmp(info.default_kid.data, kid, 16) == 0);
        /* default_KID borrows from the input buffer. */
        CHECK(info.default_kid.data >= buf &&
              info.default_kid.data < buf + len);
    }

    /* -- 38. CENC protected init: tenc v1 (pattern byte) parses same -- */
    {
        const uint8_t avcc[] = { 0x01 };
        const uint8_t kid[16] = {
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f };
        uint8_t buf[1024];
        size_t len = moq_test_build_cmaf_init_cenc(
            buf, 1, 48000, 0, 0, avcc, sizeof(avcc),
            "cbcs", 1, 0, kid, 1 /*tenc v1*/, MOQ_TEST_CENC_OK);
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info) == MOQ_OK);
        CHECK(info.has_cenc == true);
        CHECK(memcmp(info.scheme.data, "cbcs", 4) == 0);
        CHECK(info.default_is_protected == 1);
        CHECK(info.default_per_sample_iv_size == 0);
        CHECK(memcmp(info.default_kid.data, kid, 16) == 0);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AVC);
    }

    /* -- 39. CENC negatives: each missing/short box -> PROTO ---------- */
    {
        const uint8_t avcc[] = { 0x01 };
        const uint8_t kid[16] = { 0 };
        const uint32_t bad[] = {
            MOQ_TEST_CENC_OMIT_SINF,
            MOQ_TEST_CENC_OMIT_FRMA,
            MOQ_TEST_CENC_OMIT_SCHM,
            MOQ_TEST_CENC_OMIT_SCHI,
            MOQ_TEST_CENC_OMIT_TENC,
            MOQ_TEST_CENC_SHORT_TENC,
        };
        for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
            uint8_t buf[1024];
            size_t len = moq_test_build_cmaf_init_cenc(
                buf, 1, 90000, 64, 64, avcc, sizeof(avcc),
                "cenc", 1, 8, kid, 0, bad[i]);
            moq_cmaf_init_info_t info;
            moq_cmaf_init_info_init(&info);
            CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info)
                  == MOQ_ERR_PROTO);
        }
    }

    /* -- 40. clear avc1 init still parses with has_cenc == false ------ */
    {
        uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1f };
        uint8_t buf[512];
        size_t len = build_avc_init(buf, sizeof(buf), 90000, 640, 480,
                                    avcc, sizeof(avcc));
        moq_cmaf_init_info_t info;
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info) == MOQ_OK);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AVC);
        CHECK(info.has_cenc == false);
        CHECK(info.scheme.len == 0);
        CHECK(info.default_kid.len == 0);
    }

    /* -- 41. CENC edge cases: schm/tenc bounds, frma codec resolution -- */
    {
        const uint8_t avcc[] = { 0x01, 0x64, 0x00, 0x1f };
        const uint8_t kid[16] = {
            0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7,
            0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf };
        uint8_t buf[1024];
        size_t len;
        moq_cmaf_init_info_t info;

        /* (a) short schm (body < 12, lacks scheme_version) -> PROTO. */
        len = moq_test_build_cmaf_init_cenc(buf, 1, 90000, 64, 64,
            avcc, sizeof(avcc), "cenc", 1, 8, kid, 0,
            MOQ_TEST_CENC_SHORT_SCHM);
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info)
              == MOQ_ERR_PROTO);

        /* (b) trailing tenc (body 27 > 23) -> OK, same fields extracted. */
        len = moq_test_build_cmaf_init_cenc(buf, 1, 90000, 64, 64,
            avcc, sizeof(avcc), "cenc", 1, 8, kid, 0,
            MOQ_TEST_CENC_TRAILING_TENC);
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info) == MOQ_OK);
        CHECK(info.has_cenc == true);
        CHECK(info.default_is_protected == 1);
        CHECK(info.default_per_sample_iv_size == 8);
        CHECK(info.default_kid.len == 16);
        CHECK(memcmp(info.default_kid.data, kid, 16) == 0);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_AVC);

        /* (c) unknown frma ("zzzz") -> OK, protected, codec stays unknown. */
        len = moq_test_build_cmaf_init_cenc(buf, 1, 90000, 64, 64,
            avcc, sizeof(avcc), "cenc", 1, 8, kid, 0,
            MOQ_TEST_CENC_FRMA_UNKNOWN);
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info) == MOQ_OK);
        CHECK(info.has_cenc == true);
        CHECK(info.codec_kind == MOQ_CMAF_CODEC_UNKNOWN);
        CHECK(info.codec_config.len == 0);

        /* (d) known frma ("avc1") but missing avcC config -> PROTO. */
        len = moq_test_build_cmaf_init_cenc(buf, 1, 90000, 64, 64,
            avcc, sizeof(avcc), "cenc", 1, 8, kid, 0,
            MOQ_TEST_CENC_OMIT_AVCC);
        moq_cmaf_init_info_init(&info);
        CHECK(moq_cmaf_parse_init((moq_bytes_t){ buf, len }, &info)
              == MOQ_ERR_PROTO);
    }

    /* -- 42. prft/emsg/styp ahead of a chunk's moof are skipped ------- *
     * ISO/IEC 23000-19 permits prft and emsg boxes in CMAF chunks ahead
     * of the moof (and a fragment may lead with styp); real publishers
     * emit them (ffmpeg -write_prft, moqtail's testsrc prefixes every
     * chunk with a prft). They are metadata, not chunk-ordering
     * violations -- skip them in parse_fragment and validate_object.
     * Unknown boxes (e.g. free) still reject. */
    {
        /* prft: fullbox, reference_track_ID + ntp (8) + media time (4). */
        uint8_t obj[256]; size_t n = 0;
        n += box_hdr(obj + n, 8 + 4 + 4 + 8 + 4, "prft");
        wr32(obj + n, 0); n += 4;              /* version + flags */
        wr32(obj + n, 1); n += 4;              /* reference_track_ID */
        wr32(obj + n, 0); n += 4;              /* ntp_timestamp hi */
        wr32(obj + n, 0); n += 4;              /* ntp_timestamp lo */
        wr32(obj + n, 0); n += 4;              /* media_time (v0) */
        size_t prefix = n;
        n += build_cmaf_chunk(obj + n, 1, 0x02000000, 1, 0, 0xAA);

        moq_cmaf_object_report_t rep;
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ obj, n }, &rep)
              == MOQ_OK);
        CHECK(rep.valid == true);
        CHECK(rep.chunk_count == 1);
        CHECK(rep.starts_with_sync == true);

        moq_cmaf_sample_t s[4];
        moq_cmaf_fragment_info_t frag;
        moq_cmaf_fragment_info_init(&frag, s, 4);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ obj, n }, &frag)
              == MOQ_OK);
        CHECK(frag.chunk_count == 1);
        CHECK(frag.track_id == 1);
        CHECK(frag.sample_count == 1);

        /* emsg between two chunks of a multi-chunk object also skips. */
        uint8_t multi[512]; size_t m = 0;
        m += build_cmaf_chunk(multi + m, 1, 0x02000000, 1, 0, 0xAA);
        m += box_hdr(multi + m, 12, "emsg");
        wr32(multi + m, 0x01000000); m += 4;   /* version 1, flags 0 */
        m += build_cmaf_chunk(multi + m, 1, 0x00000000, 1, 0, 0xBB);
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ multi, m }, &rep)
              == MOQ_OK);
        CHECK(rep.chunk_count == 2);
        moq_cmaf_fragment_info_init(&frag, s, 4);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ multi, m }, &frag)
              == MOQ_OK);
        CHECK(frag.chunk_count == 2);

        /* An unknown top-level box is still not a chunk: reject. */
        uint8_t bad[256]; size_t bn = 0;
        bn += box_hdr(bad + bn, 12, "free");
        wr32(bad + bn, 0); bn += 4;
        bn += build_cmaf_chunk(bad + bn, 1, 0x02000000, 1, 0, 0xAA);
        CHECK(moq_cmaf_validate_object(NULL, (moq_bytes_t){ bad, bn }, &rep)
              == MOQ_ERR_PROTO);
        CHECK(rep.reason == MOQ_CMAF_ERR_MALFORMED);
        moq_cmaf_fragment_info_init(&frag, s, 4);
        CHECK(moq_cmaf_parse_fragment((moq_bytes_t){ bad, bn }, &frag)
              == MOQ_ERR_PROTO);

        (void)prefix;
    }

    printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
