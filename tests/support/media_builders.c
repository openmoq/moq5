/*
 * Shared CMAF object builders for media-format conformance tests.
 * See media_builders.h. Mirrors the proven builders in
 * media/cmaf/tests/test_cmaf.c so the bytes parse identically to what the
 * CMAF validator is exercised against.
 */
#include "media_builders.h"

#include <string.h>

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

size_t moq_test_box_hdr(uint8_t *p, uint32_t size, const char *type)
{
    wr32(p, size);
    memcpy(p + 4, type, 4);
    return 8;
}

/* A fullbox carrying a single u32 payload word: [size][type][ver+flags=0][v]. */
static size_t put_box_u32(uint8_t *b, const char *type, uint32_t v)
{
    size_t p = moq_test_box_hdr(b, 16, type);
    wr32(b + p, 0); p += 4;   /* version + flags */
    wr32(b + p, v); p += 4;   /* payload word */
    return p;                 /* 16 */
}

/* moof( mfhd? traf[+traf]( tfhd tfdt trun[count,first_sample_flags] ) ). */
static size_t build_moof(uint8_t *out, uint32_t track_id,
                         uint32_t first_sample_flags,
                         bool include_mfhd, bool two_traf,
                         bool include_trun, bool zero_samples)
{
    uint8_t traf_inner[64]; size_t ti = 0;
    ti += put_box_u32(traf_inner + ti, "tfhd", track_id);
    ti += put_box_u32(traf_inner + ti, "tfdt", 0);
    if (include_trun) {
        if (zero_samples) {
            ti += moq_test_box_hdr(traf_inner + ti, 16, "trun");
            wr32(traf_inner + ti, 0x00000400); ti += 4;   /* sample-flags */
            wr32(traf_inner + ti, 0);          ti += 4;   /* sample_count = 0 */
        } else {
            ti += moq_test_box_hdr(traf_inner + ti, 20, "trun");
            wr32(traf_inner + ti, 0x00000400);        ti += 4; /* sample-flags */
            wr32(traf_inner + ti, 1);                 ti += 4; /* sample_count */
            wr32(traf_inner + ti, first_sample_flags); ti += 4;
        }
    }

    uint8_t traf[80]; size_t tp = 0;
    tp += moq_test_box_hdr(traf + tp, (uint32_t)(8 + ti), "traf");
    memcpy(traf + tp, traf_inner, ti); tp += ti;

    uint8_t moofc[256]; size_t mc = 0;
    if (include_mfhd) mc += put_box_u32(moofc + mc, "mfhd", 1);
    memcpy(moofc + mc, traf, tp); mc += tp;
    if (two_traf) { memcpy(moofc + mc, traf, tp); mc += tp; }

    size_t p = moq_test_box_hdr(out, (uint32_t)(8 + mc), "moof");
    memcpy(out + p, moofc, mc); p += mc;
    return p;
}

static size_t build_mdat(uint8_t *out, uint8_t byte)
{
    size_t p = moq_test_box_hdr(out, 9, "mdat");
    out[p++] = byte;
    return p;
}

size_t moq_test_build_cmaf_chunk(uint8_t *out, uint32_t track_id,
                                 uint32_t first_sample_flags,
                                 bool include_mfhd, bool two_traf,
                                 uint8_t mdat_byte)
{
    size_t p = build_moof(out, track_id, first_sample_flags,
                          include_mfhd, two_traf, true, false);
    p += build_mdat(out + p, mdat_byte);
    return p;
}

size_t moq_test_build_cmaf_chunk_no_samples(uint8_t *out, uint32_t track_id,
                                            uint8_t mdat_byte)
{
    size_t p = build_moof(out, track_id, 0, true, false, true, true);
    p += build_mdat(out + p, mdat_byte);
    return p;
}

size_t moq_test_build_cmaf_init_cenc(uint8_t *out, uint32_t track_id,
                                     uint32_t timescale,
                                     uint16_t w, uint16_t h,
                                     const uint8_t *avcc, size_t avcc_len,
                                     const char scheme[4],
                                     uint8_t default_is_protected,
                                     uint8_t iv_size, const uint8_t kid[16],
                                     uint8_t tenc_version, uint32_t omit)
{
    /* Bottom-up sizes (0 = box omitted). */
    size_t avcc_box = (omit & MOQ_TEST_CENC_OMIT_AVCC) ? 0 : (8 + avcc_len);
    size_t frma_box = (omit & MOQ_TEST_CENC_OMIT_FRMA) ? 0 : 12;     /* hdr + 4cc */
    size_t schm_box = (omit & MOQ_TEST_CENC_OMIT_SCHM) ? 0
                    : ((omit & MOQ_TEST_CENC_SHORT_SCHM) ? 16 : 20); /* hdr+8 / hdr+12 */
    size_t tenc_body = (omit & MOQ_TEST_CENC_OMIT_TENC) ? 0
                     : ((omit & MOQ_TEST_CENC_SHORT_TENC) ? 10
                       : ((omit & MOQ_TEST_CENC_TRAILING_TENC) ? 27 : 23));
    size_t tenc_box = (omit & MOQ_TEST_CENC_OMIT_TENC) ? 0 : (8 + tenc_body);
    size_t schi_box = (omit & MOQ_TEST_CENC_OMIT_SCHI) ? 0 : (8 + tenc_box);
    size_t sinf_box = (omit & MOQ_TEST_CENC_OMIT_SINF)
                    ? 0 : (8 + frma_box + schm_box + schi_box);
    size_t encv_box = 8 + 78 + avcc_box + sinf_box;
    size_t stsd_box = 8 + 8 + encv_box;
    size_t stbl_box = 8 + stsd_box;
    size_t minf_box = 8 + stbl_box;
    size_t mdhd_box = 28;
    size_t mdia_box = 8 + mdhd_box + minf_box;
    size_t tkhd_box = 24;
    size_t trak_box = 8 + tkhd_box + mdia_box;
    size_t moov_box = 8 + trak_box;

    size_t p = 0;

    /* ftyp */
    p += moq_test_box_hdr(out + p, 20, "ftyp");
    memcpy(out + p, "isom", 4); p += 4;
    wr32(out + p, 0); p += 4;
    memcpy(out + p, "isom", 4); p += 4;

    p += moq_test_box_hdr(out + p, (uint32_t)moov_box, "moov");
    p += moq_test_box_hdr(out + p, (uint32_t)trak_box, "trak");

    /* tkhd: ver/flags + creation + modification + track_ID. */
    p += moq_test_box_hdr(out + p, (uint32_t)tkhd_box, "tkhd");
    wr32(out + p, 0); p += 4;
    wr32(out + p, 0); p += 4;
    wr32(out + p, 0); p += 4;
    wr32(out + p, track_id); p += 4;

    p += moq_test_box_hdr(out + p, (uint32_t)mdia_box, "mdia");

    /* mdhd v0: ver/flags + creation + modification + timescale + duration. */
    p += moq_test_box_hdr(out + p, (uint32_t)mdhd_box, "mdhd");
    wr32(out + p, 0); p += 4;
    wr32(out + p, 0); p += 4;
    wr32(out + p, 0); p += 4;
    wr32(out + p, timescale); p += 4;
    wr32(out + p, 0); p += 4;

    p += moq_test_box_hdr(out + p, (uint32_t)minf_box, "minf");
    p += moq_test_box_hdr(out + p, (uint32_t)stbl_box, "stbl");

    /* stsd: ver/flags + entry_count + encv. */
    p += moq_test_box_hdr(out + p, (uint32_t)stsd_box, "stsd");
    wr32(out + p, 0); p += 4;
    wr32(out + p, 1); p += 4;

    /* encv: 78-byte VisualSampleEntry header, then avcC + sinf. */
    p += moq_test_box_hdr(out + p, (uint32_t)encv_box, "encv");
    memset(out + p, 0, 78);
    wr16(out + p + 24, w);
    wr16(out + p + 26, h);
    p += 78;

    /* avcC (original decoder config, sibling of sinf). */
    if (avcc_box) {
        p += moq_test_box_hdr(out + p, (uint32_t)avcc_box, "avcC");
        if (avcc_len) { memcpy(out + p, avcc, avcc_len); p += avcc_len; }
    }

    /* sinf( frma schm schi(tenc) ). */
    if (sinf_box) {
        p += moq_test_box_hdr(out + p, (uint32_t)sinf_box, "sinf");
        if (frma_box) {
            p += moq_test_box_hdr(out + p, (uint32_t)frma_box, "frma");
            memcpy(out + p,
                   (omit & MOQ_TEST_CENC_FRMA_UNKNOWN) ? "zzzz" : "avc1", 4);
            p += 4;                               /* original format */
        }
        if (schm_box) {
            p += moq_test_box_hdr(out + p, (uint32_t)schm_box, "schm");
            wr32(out + p, 0); p += 4;             /* ver/flags */
            memcpy(out + p, scheme, 4); p += 4;   /* scheme_type 4cc */
            if (!(omit & MOQ_TEST_CENC_SHORT_SCHM)) {
                wr32(out + p, 0); p += 4;         /* scheme_version */
            }
        }
        if (schi_box) {
            p += moq_test_box_hdr(out + p, (uint32_t)schi_box, "schi");
            if (tenc_box) {
                p += moq_test_box_hdr(out + p, (uint32_t)tenc_box, "tenc");
                out[p++] = tenc_version;          /* version */
                out[p++] = 0; out[p++] = 0; out[p++] = 0;  /* flags */
                if (omit & MOQ_TEST_CENC_SHORT_TENC) {
                    memset(out + p, 0, 6); p += 6; /* truncated body (<23) */
                } else {
                    out[p++] = 0;                  /* reserved / pattern byte */
                    out[p++] = default_is_protected;
                    out[p++] = iv_size;
                    memcpy(out + p, kid, 16); p += 16;  /* default_KID */
                    if (omit & MOQ_TEST_CENC_TRAILING_TENC) {
                        memset(out + p, 0, 4); p += 4;  /* trailing data */
                    }
                }
            }
        }
    }

    return p;
}
