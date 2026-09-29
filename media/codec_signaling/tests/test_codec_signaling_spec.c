/*
 * Conformance vectors constructed from the primary specifications rather than
 * from producer samples, so a parser can be wrong in a band the samples never
 * visit and still be caught.
 *
 * Rows are of two kinds. Every API row pins one exact public result plus the
 * full buffer contract: on a non-buffer failure the destination is untouched
 * and *out_len is 0; on MOQ_ERR_BUFFER after a valid parse *out_len is the
 * exact required length and the destination is untouched; on success the bytes
 * are exact. Guard bands sit on both sides of every destination. The remaining
 * rows are internal ORACLE rows: they exercise only this file's own helpers,
 * drive no public entry point, and pin no public contract. The two kinds are
 * counted and reported separately.
 *
 * Bit-level sources are built by a local writer whose field packing is
 * cross-checked before it is trusted -- against a known configuration for the
 * non-escaped path, and against an independent bit reader for the escaped
 * path, for which no producer-derived vector is available (see
 * oracle_selfchecks() and the PROVENANCE note there).
 */

#include <moq/codec_signaling.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ---- accounting ---------------------------------------------------- */

static const char *g_row = "?";
static int g_rows = 0;      /* named rows executed */
static int g_asserts = 0;   /* assertions evaluated */
static int g_diags = 0;     /* diagnostics emitted */

static int g_rows_with_diags = 0;
static int g_row_diag_mark = 0;

/*
 * Rows are of two kinds and are counted separately. An API row drives a public
 * entry point and pins a public result plus the buffer contract. An ORACLE row
 * exercises only this file's own helpers and pins nothing about the library.
 */
static int  g_oracle_rows = 0;
static int  g_oracle_diags = 0;
static bool g_row_is_oracle = false;

static void close_row(void)
{
    if (g_rows == 0) {
        return;
    }
    int d = g_diags - g_row_diag_mark;
    if (d > 0) {
        g_rows_with_diags++;
    }
    if (g_row_is_oracle) {
        g_oracle_rows++;
        g_oracle_diags += d;
    }
}

static void row(const char *name)
{
    close_row();
    g_row_diag_mark = g_diags;
    g_row_is_oracle = false;
    g_row = name;
    g_rows++;
}

/* An internal oracle self-check: no public API, no public contract. */
static void row_oracle(const char *name)
{
    row(name);
    g_row_is_oracle = true;
}

static void rows_done(void) { close_row(); }

#define CHECK(cond)                                                          \
    do {                                                                     \
        g_asserts++;                                                         \
        if (!(cond)) {                                                       \
            g_diags++;                                                       \
            printf("FAIL: %s:%d: %s: %s\n", __FILE__, __LINE__, g_row,       \
                   #cond);                                                   \
        }                                                                    \
    } while (0)

#define FAILF(...)                                                           \
    do {                                                                     \
        g_asserts++;                                                         \
        g_diags++;                                                           \
        printf("FAIL: %s:%d: %s: ", __FILE__, __LINE__, g_row);              \
        printf(__VA_ARGS__);                                                 \
        printf("\n");                                                        \
    } while (0)

#define CHECKF(cond, ...)                                                    \
    do {                                                                     \
        g_asserts++;                                                         \
        if (!(cond)) {                                                       \
            g_diags++;                                                       \
            printf("FAIL: %s:%d: %s: ", __FILE__, __LINE__, g_row);          \
            printf(__VA_ARGS__);                                             \
            printf("\n");                                                    \
        }                                                                    \
    } while (0)

/* ---- guarded destination ------------------------------------------- */

#define GUARD  16
#define CANARY 0xCD
#define DESTMAX 80000

typedef struct {
    uint8_t *raw;
    size_t   cap;
} dest_t;

static uint8_t g_dest_store[GUARD + DESTMAX + GUARD];

static void dest_init(dest_t *d, size_t cap)
{
    if (cap > DESTMAX) {
        abort();
    }
    memset(g_dest_store, CANARY, sizeof(g_dest_store));
    d->raw = g_dest_store;
    d->cap = cap;
}

static uint8_t *dest_ptr(const dest_t *d) { return d->raw + GUARD; }

static bool guards_intact(const dest_t *d)
{
    for (size_t i = 0; i < GUARD; i++) {
        if (d->raw[i] != CANARY) return false;
        if (d->raw[GUARD + d->cap + i] != CANARY) return false;
    }
    return true;
}

static bool dest_unwritten(const dest_t *d)
{
    for (size_t i = 0; i < d->cap; i++) {
        if (dest_ptr(d)[i] != CANARY) return false;
    }
    return true;
}

/* A value *out_len can never legitimately hold, so "left untouched" is
 * distinguishable from "set to 0". */
#define OUTLEN_SENTINEL ((size_t)0x5a5a5a5au)

static const char *rcname(moq_result_t rc)
{
    switch (rc) {
    case MOQ_OK:                 return "MOQ_OK";
    case MOQ_ERR_INVAL:          return "MOQ_ERR_INVAL";
    case MOQ_ERR_BUFFER:         return "MOQ_ERR_BUFFER";
    case MOQ_ERR_PROTO:          return "MOQ_ERR_PROTO";
    case MOQ_ERR_UNSUPPORTED:    return "MOQ_ERR_UNSUPPORTED";
    default:                     return "other";
    }
}

static moq_bytes_t bytes(const uint8_t *p, size_t n)
{
    moq_bytes_t b;
    b.data = p;
    b.len = n;
    return b;
}

static moq_bytes_t entry(const char *s)
{
    return bytes((const uint8_t *)s, 4);
}

/* ---- contract pins -------------------------------------------------- */

/*
 * A non-buffer failure: exact result, *out_len == 0, destination untouched,
 * guards intact. `why` names what the row is proving.
 */
static void pin_build_fail(const moq_codec_init_data_cfg_t *cfg,
                           moq_result_t want, size_t cap, const char *why)
{
    dest_t d;
    dest_init(&d, cap);
    size_t out_len = OUTLEN_SENTINEL;
    moq_result_t rc = moq_codec_init_data_build(cfg, dest_ptr(&d), cap, &out_len);

    CHECKF(rc == want, "%s: got %s, the contract is %s", why, rcname(rc),
           rcname(want));
    CHECKF(out_len == 0,
           "%s: *out_len was left at %s after a non-buffer failure; the "
           "contract is 0",
           why, out_len == OUTLEN_SENTINEL ? "its caller value (untouched)"
                                           : "a non-zero value");
    CHECKF(dest_unwritten(&d), "%s: the destination was written on failure", why);
    CHECKF(guards_intact(&d), "%s: a destination guard band was overwritten", why);
}

static void pin_string_fail(const moq_codec_string_cfg_t *cfg,
                            moq_result_t want, size_t cap, const char *why)
{
    dest_t d;
    dest_init(&d, cap);
    size_t out_len = OUTLEN_SENTINEL;
    moq_result_t rc = moq_codec_string_format(cfg, dest_ptr(&d), cap, &out_len);

    CHECKF(rc == want, "%s: got %s, the contract is %s", why, rcname(rc),
           rcname(want));
    CHECKF(out_len == 0,
           "%s: *out_len was left at %s after a non-buffer failure; the "
           "contract is 0",
           why, out_len == OUTLEN_SENTINEL ? "its caller value (untouched)"
                                           : "a non-zero value");
    CHECKF(dest_unwritten(&d), "%s: the destination was written on failure", why);
    CHECKF(guards_intact(&d), "%s: a destination guard band was overwritten", why);
}

/*
 * A success: the size query reports the exact length and writes nothing, an
 * undersized buffer reports the same length and writes nothing, and an
 * exact-size buffer produces exactly the expected bytes.
 */
static void pin_build_ok(const moq_codec_init_data_cfg_t *cfg,
                         const uint8_t *expect, size_t elen, const char *why)
{
    /* size query */
    {
        size_t out_len = OUTLEN_SENTINEL;
        moq_result_t rc = moq_codec_init_data_build(cfg, NULL, 0, &out_len);
        CHECKF(rc == MOQ_ERR_BUFFER, "%s: size query returned %s, want MOQ_ERR_BUFFER",
               why, rcname(rc));
        CHECKF(out_len == elen, "%s: size query reported %zu, the record is %zu bytes",
               why, out_len == OUTLEN_SENTINEL ? (size_t)0 : out_len, elen);
    }
    /* undersized buffer: same required length, no write */
    if (elen > 0) {
        dest_t d;
        dest_init(&d, elen - 1);
        size_t out_len = OUTLEN_SENTINEL;
        moq_result_t rc = moq_codec_init_data_build(cfg, dest_ptr(&d), elen - 1, &out_len);
        CHECKF(rc == MOQ_ERR_BUFFER, "%s: undersized buffer returned %s", why, rcname(rc));
        CHECKF(out_len == elen, "%s: undersized buffer reported required length %zu, want %zu",
               why, out_len == OUTLEN_SENTINEL ? (size_t)0 : out_len, elen);
        CHECKF(dest_unwritten(&d), "%s: partial output written into a too-small buffer", why);
        CHECKF(guards_intact(&d), "%s: guard overwritten on the undersized call", why);
    }
    /* exact buffer */
    {
        dest_t d;
        dest_init(&d, elen);
        size_t out_len = OUTLEN_SENTINEL;
        moq_result_t rc = moq_codec_init_data_build(cfg, dest_ptr(&d), elen, &out_len);
        CHECKF(rc == MOQ_OK, "%s: exact buffer returned %s", why, rcname(rc));
        CHECKF(out_len == elen, "%s: produced %zu bytes, want %zu", why,
               out_len == OUTLEN_SENTINEL ? (size_t)0 : out_len, elen);
        CHECKF(out_len == elen && memcmp(dest_ptr(&d), expect, elen) == 0,
               "%s: record bytes differ from the expected record", why);
        CHECKF(guards_intact(&d), "%s: guard overwritten on the successful call", why);
    }
}

static void pin_string_ok(const moq_codec_string_cfg_t *cfg,
                          const char *expect, const char *why)
{
    size_t elen = strlen(expect);
    /* size query */
    {
        size_t out_len = OUTLEN_SENTINEL;
        moq_result_t rc = moq_codec_string_format(cfg, NULL, 0, &out_len);
        CHECKF(rc == MOQ_ERR_BUFFER, "%s: size query returned %s, want MOQ_ERR_BUFFER",
               why, rcname(rc));
        CHECKF(out_len == elen, "%s: size query reported %zu, \"%s\" is %zu bytes",
               why, out_len == OUTLEN_SENTINEL ? (size_t)0 : out_len, expect, elen);
    }
    /* undersized buffer: same required length, no write */
    if (elen > 0) {
        dest_t d;
        dest_init(&d, elen - 1);
        size_t out_len = OUTLEN_SENTINEL;
        moq_result_t rc = moq_codec_string_format(cfg, dest_ptr(&d), elen - 1, &out_len);
        CHECKF(rc == MOQ_ERR_BUFFER, "%s: undersized buffer returned %s", why, rcname(rc));
        CHECKF(out_len == elen,
               "%s: undersized buffer reported required length %zu, want %zu",
               why, out_len == OUTLEN_SENTINEL ? (size_t)0 : out_len, elen);
        CHECKF(dest_unwritten(&d), "%s: partial output written into a too-small buffer", why);
        CHECKF(guards_intact(&d), "%s: guard overwritten on the undersized call", why);
    }
    /* exact buffer */
    {
        dest_t d;
        dest_init(&d, elen);
        size_t out_len = OUTLEN_SENTINEL;
        moq_result_t rc = moq_codec_string_format(cfg, dest_ptr(&d), elen, &out_len);
        CHECKF(rc == MOQ_OK, "%s: returned %s", why, rcname(rc));
        if (rc == MOQ_OK) {
            /*
             * Every diagnostic read is bounded by the destination capacity the
             * call was actually given, never by the length it reported: a
             * defect that returns MOQ_OK with an over-long length must not be
             * able to walk this test past the buffer.
             */
            if (out_len > d.cap) {
                FAILF("%s: reported %zu bytes from a %zu-byte destination",
                      why, out_len, d.cap);
            } else {
                char got[80];
                size_t n = out_len < sizeof(got) - 1 ? out_len : sizeof(got) - 1;
                memcpy(got, dest_ptr(&d), n);
                got[n] = 0;
                CHECKF(out_len == elen && memcmp(dest_ptr(&d), expect, elen) == 0,
                       "got \"%s\", the contract is exactly \"%s\"", got, expect);
            }
        }
        CHECKF(guards_intact(&d), "%s: guard overwritten", why);
    }
}

/* ---- independent MSB-first bit writer ------------------------------- */

typedef struct {
    uint8_t b[256];
    size_t  nbits;
} bw_t;

static void bw_init(bw_t *w) { memset(w->b, 0, sizeof(w->b)); w->nbits = 0; }

static void bw_u(bw_t *w, uint32_t v, int n)
{
    for (int i = n - 1; i >= 0; i--) {
        uint32_t bit = (v >> i) & 1u;
        size_t byte = w->nbits >> 3;
        int off = 7 - (int)(w->nbits & 7u);
        if (byte >= sizeof(w->b)) abort();
        w->b[byte] |= (uint8_t)(bit << off);
        w->nbits++;
    }
}

static void bw_ue(bw_t *w, uint32_t v)
{
    uint32_t code = v + 1u;
    int bits = 0;
    for (uint32_t t = code; t != 0; t >>= 1) {
        bits++;
    }
    for (int i = 0; i < bits - 1; i++) {
        bw_u(w, 0, 1);
    }
    bw_u(w, code, bits);
}

static size_t bw_len(const bw_t *w) { return (w->nbits + 7) / 8; }

/* ---- AudioSpecificConfig leading fields (ISO/IEC 14496-3) ----------- */

/*
 * Writes AudioSpecificConfig's mandatory leading fields: GetAudioObjectType(),
 * samplingFrequencyIndex, channelConfiguration. GetAudioObjectType() is a
 * 5-bit field, and when that field reads 31 the value is 32 plus a 6-bit
 * extension that IMMEDIATELY follows it.
 *
 * This is NOT a complete AudioSpecificConfig for an arbitrary object type:
 * what follows channelConfiguration is the object-type-specific payload, whose
 * syntax differs per AOT. Rows built with this helper assert only the
 * AudioObjectType the formatter extracts, which is the sole field the codec
 * string depends on.
 */
static void asc_leading(bw_t *w, uint32_t aot, uint32_t freq_idx, uint32_t chan_cfg)
{
    bw_init(w);
    if (aot < 32) {
        bw_u(w, aot, 5);
    } else {
        bw_u(w, 31, 5);
        bw_u(w, aot - 32, 6);
    }
    bw_u(w, freq_idx, 4);
    bw_u(w, chan_cfg, 4);
}

/* ---- AV1 sequence header (AV1 1.0.0 sections 5.5.1 / 5.5.2) --------- */

typedef struct {
    uint32_t seq_profile;
    uint32_t seq_level_idx;
    uint32_t high_bitdepth;
    uint32_t twelve_bit;
    uint32_t mono_chrome;
    uint32_t color_range;
    uint32_t ssx, ssy;
    uint32_t csp;              /* chroma_sample_position, when read */
    bool     write_csp;        /* whether the bitstream carries csp */
} av1_seq_t;

/* Builds sequence_header_obu() in its reduced_still_picture_header form. */
static void av1_seqhdr(bw_t *w, const av1_seq_t *s)
{
    bw_init(w);
    bw_u(w, s->seq_profile, 3);
    bw_u(w, 1, 1);                    /* still_picture */
    bw_u(w, 1, 1);                    /* reduced_still_picture_header */
    bw_u(w, s->seq_level_idx, 5);     /* seq_level_idx[0] */

    bw_u(w, 0, 4);                    /* frame_width_bits_minus_1 */
    bw_u(w, 0, 4);                    /* frame_height_bits_minus_1 */
    bw_u(w, 0, 1);                    /* max_frame_width_minus_1 */
    bw_u(w, 0, 1);                    /* max_frame_height_minus_1 */

    bw_u(w, 0, 1);                    /* use_128x128_superblock */
    bw_u(w, 0, 1);                    /* enable_filter_intra */
    bw_u(w, 0, 1);                    /* enable_intra_edge_filter */
    bw_u(w, 0, 1);                    /* enable_superres */
    bw_u(w, 0, 1);                    /* enable_cdef */
    bw_u(w, 0, 1);                    /* enable_restoration */

    /* color_config() */
    bw_u(w, s->high_bitdepth, 1);
    if (s->seq_profile == 2 && s->high_bitdepth) {
        bw_u(w, s->twelve_bit, 1);
    }
    if (s->seq_profile != 1) {
        bw_u(w, s->mono_chrome, 1);
    }
    bw_u(w, 0, 1);                    /* color_description_present_flag */
    if (s->mono_chrome) {
        bw_u(w, s->color_range, 1);
    } else {
        /* cp/tc/mc default to UNSPECIFIED, so the general else branch is
         * taken and color_range is read BEFORE the subsampling decision. */
        bw_u(w, s->color_range, 1);
        if (s->seq_profile == 2) {
            uint32_t bit_depth = s->high_bitdepth ? (s->twelve_bit ? 12u : 10u) : 8u;
            if (bit_depth == 12) {
                bw_u(w, s->ssx, 1);
                if (s->ssx) {
                    bw_u(w, s->ssy, 1);
                }
            }
        }
        if (s->ssx && s->ssy) {
            bw_u(w, s->csp, 2);
        }
    }
    bw_u(w, 0, 1);                    /* separate_uv_delta_q */
    bw_u(w, 0, 1);                    /* film_grain_params_present */
}

/* The av1C configuration bytes the record must carry for that sequence. */
static void av1c_expect(const av1_seq_t *s, uint8_t out[2])
{
    uint32_t mono = s->mono_chrome;
    uint32_t ssx = mono ? 1u : s->ssx;
    uint32_t ssy = mono ? 1u : s->ssy;
    uint32_t csp = (!mono && s->ssx && s->ssy) ? s->csp : 0u;

    out[0] = (uint8_t)((s->seq_profile << 5) | (s->seq_level_idx & 0x1fu));
    out[1] = (uint8_t)((0u << 7) |                       /* seq_tier_0 */
                       (s->high_bitdepth << 6) |
                       (((s->seq_profile == 2 && s->high_bitdepth) ? s->twelve_bit : 0u) << 5) |
                       (mono << 4) |
                       (ssx << 3) |
                       (ssy << 2) |
                       (csp & 3u));
}

/* Wrap a payload as one OBU. */
static size_t obu_wrap(uint8_t *out, uint32_t type, bool has_size,
                       const uint8_t *payload, size_t plen)
{
    size_t o = 0;
    out[o++] = (uint8_t)((type << 3) | (has_size ? 0x02u : 0x00u));
    if (has_size) {
        size_t v = plen;
        do {
            uint8_t byte = (uint8_t)(v & 0x7fu);
            v >>= 7;
            if (v) byte |= 0x80u;
            out[o++] = byte;
        } while (v);
    }
    memcpy(out + o, payload, plen);
    return o + plen;
}

/* ==================================================================== */

/*
 * Vectors carried over from the donor suite and reused as positives. The donor
 * attributes two of them to a specific encoder -- the HEVC Annex B stream to
 * x265 and the AV1 OBU stream to SVT-AV1. The others carry no attribution
 * there, so none is described here as producer-derived.
 */

/* AAC-LC AudioSpecificConfig (complete: AOT 2 + freq idx + channels +
 * GASpecificConfig's three flags = 16 bits). */
static const uint8_t k_asc_lc[] = { 0x12, 0x10 };

/* AVC Annex B SPS (Baseline) + PPS, and the avcC they assemble into. */
static const uint8_t k_avc_annexb[] = {
    0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
    0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80,
};
static const uint8_t k_avcc[] = {
    0x01, 0x42, 0x00, 0x1e, 0xff, 0xe1, 0x00, 0x06,
    0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
    0x01, 0x00, 0x04, 0x68, 0xce, 0x3c, 0x80,
};

/* HEVC Annex B VPS + SPS + PPS produced by x265. */
static const uint8_t k_hevc_annexb[] = {
    0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, 0x01, 0xff, 0xff, 0x04, 0x08,
    0x00, 0x00, 0x03, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x03, 0x00, 0x00, 0x1e,
    0x95, 0x98, 0x09, 0x00, 0x00, 0x00, 0x01, 0x42, 0x01, 0x01, 0x04, 0x08,
    0x00, 0x00, 0x03, 0x00, 0x9e, 0x08, 0x00, 0x00, 0x03, 0x00, 0x00, 0x1e,
    0x90, 0x04, 0x10, 0x20, 0xb2, 0xca, 0xcd, 0x24, 0x99, 0x5e, 0x02, 0xdc,
    0x08, 0x08, 0x00, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00, 0x00, 0x03,
    0x00, 0x10, 0x80, 0x00, 0x00, 0x00, 0x01, 0x44, 0x01, 0xc1, 0x72, 0x86,
    0x0c, 0x42, 0x24,
};

/* HEVC Annex B VPS + SPS + PPS with sps_max_sub_layers_minus1 = 1. */
static const uint8_t k_hevc_temporal_annexb[] = {
    0x00, 0x00, 0x00, 0x01, 0x40, 0x01, 0x0c, 0x03, 0xff, 0xff, 0x01, 0x60,
    0x00, 0x00, 0x03, 0x00, 0xb0, 0x00, 0x00, 0x03, 0x00, 0x00, 0x03, 0x00,
    0x5d, 0x00, 0x00, 0x1b, 0x02, 0x40, 0x00, 0x00, 0x00, 0x01, 0x42, 0x01,
    0x03, 0x01, 0x60, 0x00, 0x00, 0x03, 0x00, 0xb0, 0x00, 0x00, 0x03, 0x00,
    0x00, 0x03, 0x00, 0x5d, 0x00, 0x00, 0xa0, 0x02, 0x80, 0x80, 0x2d, 0x16,
    0x20, 0x6e, 0xe4, 0x52, 0x32, 0xe7, 0xe1, 0x3d, 0x0b, 0xea, 0x1b, 0xd5,
    0x29, 0xa8, 0x10, 0x10, 0x10, 0x1f, 0xc2, 0x01, 0x04, 0x00, 0x00, 0x00,
    0x01, 0x44, 0x01, 0xc0, 0x72, 0xf0, 0x5b, 0x24,
};

static const uint8_t k_hevc_temporal_ptl[] = {
    0x01, 0x60, 0x00, 0x00, 0x00, 0xb0,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x5d,
};

/* dOps for a stereo 48 kHz stream, mapping family 0. */
static const uint8_t k_dops[] = {
    0x00, 0x02, 0x01, 0x38, 0x00, 0x00, 0xbb, 0x80, 0x00, 0x00, 0x00,
};

static uint8_t g_big_src[80000];

/* ==================================================================== */

/* ---- independent MSB-first bit reader (cross-checks the writer) ----- */

typedef struct {
    const uint8_t *d;
    size_t         nbits;
    size_t         pos;
} br_t;

static void br_init(br_t *r, const uint8_t *d, size_t nbits)
{
    r->d = d;
    r->nbits = nbits;
    r->pos = 0;
}

static uint32_t br_read(br_t *r, int n)
{
    uint32_t v = 0;
    for (int i = 0; i < n; i++) {
        uint32_t bit = 0;
        if (r->pos < r->nbits) {
            bit = (uint32_t)((r->d[r->pos >> 3] >> (7 - (r->pos & 7u))) & 1u);
        }
        v = (v << 1) | bit;
        r->pos++;
    }
    return v;
}

/*
 * Internal oracle self-checks. These exercise no public API and pin no public
 * contract; they exist so a bug in the local writer cannot masquerade as a
 * finding about the library.
 *
 * PROVENANCE, stated precisely: there is no producer-derived escaped-AOT
 * vector available to anchor against. The donor suite's `f8 08 00` is NOT one
 * -- its comment (tests/test_codec_signaling.c:102-105) constructs it from the
 * same byte1[2:0]/byte2[7:5] reading the implementation uses, so agreeing with
 * it would be circular. The escaped construction is therefore cross-checked by
 * an INDEPENDENT reader instead, and the non-escaped packing is checked
 * against a known AAC-LC configuration decoded field by field.
 */
static void oracle_selfchecks(void)
{
    row_oracle("oracle: writer packs the non-escaped leading fields");
    {
        /*
         * A known AAC-LC AudioSpecificConfig: audioObjectType 2,
         * samplingFrequencyIndex 4, channelConfiguration 2 (two channels),
         * then GASpecificConfig's three flags -- 16 bits, 0x12 0x10. The
         * expectation is derived field by field, not adopted from any parser.
         */
        bw_t w;
        asc_leading(&w, 2, 4, 2);
        bw_u(&w, 0, 3);
        CHECKF(bw_len(&w) == sizeof(k_asc_lc) &&
                   memcmp(w.b, k_asc_lc, sizeof(k_asc_lc)) == 0,
               "the writer produced %02x %02x for AOT 2 / freq idx 4 / two "
               "channels; the AAC-LC configuration is %02x %02x",
               w.b[0], w.b[1], k_asc_lc[0], k_asc_lc[1]);
    }

    row_oracle("oracle: escaped leading fields are exactly f9 48 00");
    {
        /*
         * GetAudioObjectType(): a 5-bit field, and when it reads 31 the value
         * is 32 plus a 6-bit extension that IMMEDIATELY follows. Written by
         * bw_u, read back by br_read -- two separate code paths.
         */
        bw_t w;
        asc_leading(&w, 42, 4, 0);

        /*
         * Hand-derived from the field widths and values alone -- not from the
         * donor suite and not from the implementation:
         *
         *   audioObjectType escape   11111
         *   extension (42 - 32)      001010
         *   samplingFrequencyIndex 4 0100
         *   channelConfiguration 0   0000
         *
         * 19 bits, zero-padded to 24: 11111001 01001000 00000000.
         *
         * This is checked BEFORE the reader round trip, because a writer and a
         * reader that share a bit-order mistake would agree with each other.
         */
        static const uint8_t k_aot42_leading[] = { 0xf9, 0x48, 0x00 };
        CHECKF(bw_len(&w) == sizeof(k_aot42_leading),
               "the escaped leading fields occupy %zu bytes, want %zu",
               bw_len(&w), sizeof(k_aot42_leading));
        CHECKF(bw_len(&w) == sizeof(k_aot42_leading) &&
                   memcmp(w.b, k_aot42_leading, sizeof(k_aot42_leading)) == 0,
               "the writer produced %02x %02x %02x for the AOT 42 leading "
               "fields; the field widths and values give %02x %02x %02x",
               w.b[0], w.b[1], w.b[2], k_aot42_leading[0],
               k_aot42_leading[1], k_aot42_leading[2]);

        br_t r;
        br_init(&r, w.b, w.nbits);
        uint32_t first = br_read(&r, 5);
        uint32_t ext = br_read(&r, 6);
        uint32_t freq = br_read(&r, 4);
        uint32_t chan = br_read(&r, 4);
        CHECKF(first == 31, "the 5-bit field reads %u, want 31 (escape)", first);
        CHECKF(ext == 10, "the 6-bit extension reads %u, want 10 (42 - 32)", ext);
        CHECKF(freq == 4, "samplingFrequencyIndex reads %u, want 4", freq);
        CHECKF(chan == 0, "channelConfiguration reads %u, want 0", chan);
    }

    row_oracle("oracle: bit placement, no codec-validity claim");
    {
        /*
         * Pure bit-placement discrimination. These extension values are NOT
         * asserted to name registered audio object types and are never fed to
         * the public formatter as positive vectors; they exist only to show
         * that the escape extension changes the bytes and that a reading which
         * ignores it cannot tell these apart.
         */
        bw_t a, b, c;
        asc_leading(&a, 32, 4, 0);
        asc_leading(&b, 33, 4, 0);
        asc_leading(&c, 42, 4, 0);
        CHECKF(memcmp(a.b, b.b, 3) != 0 && memcmp(a.b, c.b, 3) != 0,
               "extension values 0, 1 and 10 produced identical bytes, so no "
               "vector built this way could discriminate anything");
        /* The implementation's reading looks at byte1[2:0] and byte2[7:5],
         * which are identical across all three. */
        CHECKF((a.b[1] & 0x07u) == (b.b[1] & 0x07u) &&
                   (a.b[2] & 0xe0u) == (b.b[2] & 0xe0u),
               "the bytes the implementation reads already differ, so these "
               "vectors would not isolate the placement defect");
    }
}

static void rows_aac(void)
{
    /*
     * Positive vectors are limited to codec strings the accessible WebCodecs
     * AAC registration lists: mp4a.40.2, mp4a.40.5, mp4a.40.29, mp4a.40.42 and
     * mp4a.67. AOT 32 and 33 are NOT listed there and are not used as public
     * formatter inputs.
     *
     * The formatter's job here is to extract AudioObjectType from the common
     * leading fields. These rows assert that extraction and make no claim
     * about any object-type-specific payload.
     */
    static const struct {
        uint32_t    aot;
        const char *want;
        bool        complete_asc;
    } cases[] = {
        { 2,  "mp4a.40.2",  true  },  /* complete AAC-LC configuration */
        { 5,  "mp4a.40.5",  false },  /* leading fields only */
        { 29, "mp4a.40.29", false },
        { 42, "mp4a.40.42", false },  /* escaped: 31 + 6-bit extension */
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        char name[112];
        snprintf(name, sizeof(name),
                 "mp4a OTI 0x40, AudioObjectType %u (%s)", cases[i].aot,
                 cases[i].complete_asc ? "complete configuration"
                                       : "leading fields only");
        row(name);

        bw_t w;
        asc_leading(&w, cases[i].aot, 4, cases[i].complete_asc ? 2u : 0u);
        if (cases[i].complete_asc) {
            bw_u(&w, 0, 3);           /* GASpecificConfig */
        }

        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        cfg.sample_entry = entry("mp4a");
        cfg.has_mp4_object_type_indication = true;
        cfg.mp4_object_type_indication = 0x40;
        cfg.decoder_config = bytes(w.b, bw_len(&w));

        pin_string_ok(&cfg, cases[i].want, "mp4a.40");
    }

    /*
     * RFC 6381 section 3.3 scopes the third element to OTI 40: "One of the OTI
     * values for 'mp4a' is 40 ... For this value, the third element identifies
     * the audio ObjectTypeIndication". The grammar makes it optional
     * (iso-mpega := mp4a "." oti [ "." aud-oti ]), and the WebCodecs AAC
     * registration lists mp4a.67 with no third element.
     */
    row("mp4a OTI 0x67 carries no third element");
    {
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        cfg.sample_entry = entry("mp4a");
        cfg.has_mp4_object_type_indication = true;
        cfg.mp4_object_type_indication = 0x67;
        cfg.decoder_config = bytes(k_asc_lc, sizeof(k_asc_lc));

        pin_string_ok(&cfg, "mp4a.67", "mp4a.67");
    }
}

static void rows_case(void)
{
    /*
     * RFC 6381 uses RFC 5234 ABNF, whose quoted strings are case-insensitive,
     * so lowercase hexadecimal is conformant. Lowercase is the canonical
     * emitter form and is pinned exactly. The four-character sample entry is
     * separately case-SENSITIVE (RFC 6381 section 3.3 says so, and defines the
     * entries with explicit numeric octets), so it must be matched by exact
     * byte equality rather than case-folded.
     */
    row("codec-string hexadecimal is emitted lowercase");
    {
        static const uint8_t avcc[] = { 0x01, 0xab, 0xcd, 0xef };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc1");
        cfg.decoder_config = bytes(avcc, sizeof(avcc));
        pin_string_ok(&cfg, "avc1.abcdef", "lowercase avcoti");
    }

    /*
     * This row proves only that a second legitimate entry is carried through
     * unchanged -- avc3 is a distinct registered entry, not a case variant of
     * avc1. It is NOT evidence of case-sensitive validation; the row below is.
     */
    row("a distinct registered sample entry is carried through");
    {
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc3");
        cfg.decoder_config = bytes(k_avcc, sizeof(k_avcc));
        pin_string_ok(&cfg, "avc3.42001e", "avc3 entry");
    }

    /*
     * Sample-entry values are case-sensitive, so "Avc1" is not the registered
     * entry "avc1" -- it is an unregistered four-character code paired with an
     * avcC. A compatibility check by exact byte equality refuses it.
     */
    row("a mixed-case sample entry is not the registered entry");
    {
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("Avc1");
        cfg.decoder_config = bytes(k_avcc, sizeof(k_avcc));
        pin_string_fail(&cfg, MOQ_ERR_INVAL, 64, "mixed-case sample entry");
    }
}


static void rows_annexb_avc(void)
{
    row("AVC Annex B SPS+PPS assembles the expected avcC");
    {
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(k_avc_annexb, sizeof(k_avc_annexb));
        pin_build_ok(&cfg, k_avcc, sizeof(k_avcc), "avcC build");
    }

    row("AVC Annex B: an empty NAL between start codes");
    {
        /* The empty NAL sits AFTER the SPS, so a parser that stops early
         * silently loses the PPS that follows instead of failing. */
        static const uint8_t src[] = {
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
            0x00, 0x00, 0x00, 0x01,             /* empty NAL */
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(src, sizeof(src));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "empty Annex B NAL");
    }

    row("AVC Annex B: no SPS");
    {
        static const uint8_t src[] = {
            0x00, 0x00, 0x00, 0x01, 0x68, 0xce, 0x3c, 0x80,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(src, sizeof(src));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "avcC with no SPS");
    }

    row("AVC Annex B: no PPS");
    {
        /*
         * An avcC declaring numOfPictureParameterSets = 0 configures no
         * decoder. Whether that must be refused depends on the target sample
         * entry (out-of-band avc1 versus in-band avc3), which the builder
         * config does not currently name; see the report.
         */
        static const uint8_t src[] = {
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab, 0xcd,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(src, sizeof(src));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "avcC with no PPS");
    }

    row("AVC Annex B: a parameter set the 16-bit record field cannot hold");
    {
        /*
         * ISO/IEC 14496-15 gives sequenceParameterSetLength 16 bits. A source
         * that is valid but larger than the destination record can represent
         * is an implementation/record limit, not malformed input.
         */
        size_t n = 0;
        static const uint8_t start[] = { 0x00, 0x00, 0x00, 0x01 };
        memcpy(g_big_src + n, start, 4); n += 4;
        g_big_src[n++] = 0x67; g_big_src[n++] = 0x42;
        g_big_src[n++] = 0x00; g_big_src[n++] = 0x1e;
        /* Filler that cannot be mistaken for a start code. */
        while (n < 70000) g_big_src[n++] = 0xaa;
        memcpy(g_big_src + n, start, 4); n += 4;
        g_big_src[n++] = 0x68; g_big_src[n++] = 0xce;
        g_big_src[n++] = 0x3c; g_big_src[n++] = 0x80;

        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_ANNEXB;
        cfg.source = bytes(g_big_src, n);
        pin_build_fail(&cfg, MOQ_ERR_UNSUPPORTED, DESTMAX,
                       "AVC parameter set over 65535 bytes");
    }
}

/* Offset of the nth (0-indexed) 4-byte Annex B start code. */
static size_t nal_start(const uint8_t *d, size_t len, int nth)
{
    int seen = 0;
    for (size_t i = 0; i + 4 <= len; i++) {
        if (d[i] == 0 && d[i + 1] == 0 && d[i + 2] == 0 && d[i + 3] == 1) {
            if (seen == nth) return i;
            seen++;
        }
    }
    abort();
}

static uint16_t rd16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

static size_t append_start_code(uint8_t *dst, size_t n)
{
    static const uint8_t start[] = { 0x00, 0x00, 0x00, 0x01 };
    memcpy(dst + n, start, sizeof(start));
    return n + sizeof(start);
}

static size_t append_escaped(uint8_t *dst, size_t n, const uint8_t *src, size_t len)
{
    int zeros = 0;
    for (size_t i = 0; i < len; i++) {
        uint8_t b = src[i];
        if (zeros >= 2 && b <= 0x03) {
            dst[n++] = 0x03;
            zeros = 0;
        }
        dst[n++] = b;
        zeros = (b == 0) ? zeros + 1 : 0;
    }
    return n;
}

static size_t build_hevc_fixture(uint8_t *dst, uint32_t max_sub,
                                 uint32_t flagged_sub,
                                 bool sub_profile_present,
                                 bool sub_level_present,
                                 uint32_t chroma, uint32_t bd_luma,
                                 uint32_t bd_chroma)
{
    static const uint8_t vps[] = { 0x40, 0x01 };
    static const uint8_t pps[] = { 0x44, 0x01 };
    bw_t rbsp;
    bw_init(&rbsp);

    bw_u(&rbsp, 0, 4);                   /* sps_video_parameter_set_id */
    bw_u(&rbsp, max_sub, 3);             /* sps_max_sub_layers_minus1 */
    bw_u(&rbsp, 1, 1);                   /* sps_temporal_id_nesting */
    for (size_t i = 0; i < sizeof(k_hevc_temporal_ptl); i++) {
        bw_u(&rbsp, k_hevc_temporal_ptl[i], 8);
    }
    if (max_sub > 0) {
        for (uint32_t i = 0; i < max_sub && i < 7; i++) {
            bw_u(&rbsp, (i == flagged_sub && sub_profile_present) ? 1u : 0u, 1);
            bw_u(&rbsp, (i == flagged_sub && sub_level_present) ? 1u : 0u, 1);
        }
        for (uint32_t i = max_sub; i < 8; i++) {
            bw_u(&rbsp, 0, 2);           /* reserved_zero_2bits */
        }
        if (sub_profile_present) {
            bw_u(&rbsp, 0x9b, 8);
            bw_u(&rbsp, 0x11223344u, 32);
            bw_u(&rbsp, 0x556677u, 24);
            bw_u(&rbsp, 0x8899aau, 24);
        }
        if (sub_level_present) {
            bw_u(&rbsp, 0x5d, 8);
        }
    }
    bw_ue(&rbsp, 0);                     /* sps_seq_parameter_set_id */
    bw_ue(&rbsp, chroma);
    bw_ue(&rbsp, 128);                   /* pic_width_in_luma_samples */
    bw_ue(&rbsp, 72);                    /* pic_height_in_luma_samples */
    bw_u(&rbsp, 0, 1);                   /* conformance_window_flag */
    bw_ue(&rbsp, bd_luma);
    bw_ue(&rbsp, bd_chroma);

    size_t n = 0;
    n = append_start_code(dst, n);
    memcpy(dst + n, vps, sizeof(vps)); n += sizeof(vps);
    n = append_start_code(dst, n);
    dst[n++] = 0x42; dst[n++] = 0x01;
    n = append_escaped(dst, n, rbsp.b, bw_len(&rbsp));
    n = append_start_code(dst, n);
    memcpy(dst + n, pps, sizeof(pps)); n += sizeof(pps);
    return n;
}

static void expect_hvcc_temporal_sub_layers(const moq_codec_init_data_cfg_t *cfg)
{
    size_t vps_start = nal_start(k_hevc_temporal_annexb,
                                 sizeof(k_hevc_temporal_annexb), 0) + 4;
    size_t sps_start = nal_start(k_hevc_temporal_annexb,
                                 sizeof(k_hevc_temporal_annexb), 1) + 4;
    size_t pps_start = nal_start(k_hevc_temporal_annexb,
                                 sizeof(k_hevc_temporal_annexb), 2) + 4;
    const uint8_t *nal_p[3] = {
        k_hevc_temporal_annexb + vps_start,
        k_hevc_temporal_annexb + sps_start,
        k_hevc_temporal_annexb + pps_start,
    };
    size_t nal_len[3] = {
        sps_start - 4 - vps_start,
        pps_start - 4 - sps_start,
        sizeof(k_hevc_temporal_annexb) - pps_start,
    };
    const uint8_t nal_type[3] = { 32, 33, 34 };
    const size_t want_len = 23 + 3 * 3 + 3 * 2 +
                            nal_len[0] + nal_len[1] + nal_len[2];

    dest_t d;
    dest_init(&d, want_len);
    size_t out_len = OUTLEN_SENTINEL;
    moq_result_t rc =
        moq_codec_init_data_build(cfg, dest_ptr(&d), d.cap, &out_len);
    CHECKF(rc == MOQ_OK, "HEVC sub-layer PTL build returned %s", rcname(rc));
    CHECKF(out_len == want_len, "HEVC sub-layer PTL hvcC length is %zu, want %zu",
           out_len == OUTLEN_SENTINEL ? (size_t)0 : out_len, want_len);
    CHECKF(guards_intact(&d), "HEVC sub-layer PTL build overwrote a guard band");
    if (rc != MOQ_OK || out_len != want_len) {
        return;
    }

    const uint8_t *out = dest_ptr(&d);
    CHECKF(out[0] == 1, "HEVC sub-layer PTL hvcC version is %u, want 1", out[0]);
    CHECKF(memcmp(out + 1, k_hevc_temporal_ptl,
                  sizeof(k_hevc_temporal_ptl)) == 0,
           "HEVC sub-layer PTL general profile_tier_level was not copied");
    CHECKF(out[21] == 0x17,
           "HEVC sub-layer PTL byte 21 is 0x%02x, want 0x17 "
           "(two temporal layers, temporalIdNested, 4-byte NAL lengths)",
           out[21]);
    CHECKF(out[22] == 3, "HEVC sub-layer PTL hvcC array count is %u, want 3",
           out[22]);

    size_t o = 23;
    for (size_t i = 0; i < 3; i++) {
        CHECKF(o + 5 <= out_len, "HEVC sub-layer PTL array %zu header overruns", i);
        CHECKF(out[o] == (uint8_t)(0x80 | nal_type[i]),
               "HEVC sub-layer PTL array %zu type is 0x%02x, want 0x%02x",
               i, out[o], (uint8_t)(0x80 | nal_type[i]));
        o++;
        CHECKF(rd16(out + o) == 1,
               "HEVC sub-layer PTL array %zu count is %u, want 1",
               i, rd16(out + o));
        o += 2;
        CHECKF(rd16(out + o) == nal_len[i],
               "HEVC sub-layer PTL array %zu NAL length is %u, want %zu",
               i, rd16(out + o), nal_len[i]);
        o += 2;
        CHECKF(o + nal_len[i] <= out_len,
               "HEVC sub-layer PTL array %zu payload overruns", i);
        if (o + nal_len[i] <= out_len) {
            CHECKF(memcmp(out + o, nal_p[i], nal_len[i]) == 0,
                   "HEVC sub-layer PTL array %zu payload differs", i);
        }
        o += nal_len[i];
    }
    CHECKF(o == out_len, "HEVC sub-layer PTL hvcC ended at %zu, want %zu",
           o, out_len);
}

static void expect_hvcc_generated_profile_level(const uint8_t *src, size_t src_len,
                                                uint32_t max_sub)
{
    size_t vps_start = nal_start(src, src_len, 0) + 4;
    size_t sps_start = nal_start(src, src_len, 1) + 4;
    size_t pps_start = nal_start(src, src_len, 2) + 4;
    const uint8_t *nal_p[3] = {
        src + vps_start,
        src + sps_start,
        src + pps_start,
    };
    size_t nal_len[3] = {
        sps_start - 4 - vps_start,
        pps_start - 4 - sps_start,
        src_len - pps_start,
    };
    const uint8_t nal_type[3] = { 32, 33, 34 };
    const size_t want_len = 23 + 3 * 3 + 3 * 2 +
                            nal_len[0] + nal_len[1] + nal_len[2];

    moq_codec_init_data_cfg_t cfg;
    moq_codec_init_data_cfg_init(&cfg);
    cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
    cfg.source = bytes(src, src_len);

    dest_t d;
    dest_init(&d, want_len);
    size_t out_len = OUTLEN_SENTINEL;
    moq_result_t rc =
        moq_codec_init_data_build(&cfg, dest_ptr(&d), d.cap, &out_len);
    CHECKF(rc == MOQ_OK, "HEVC sub-layer profile/level build returned %s",
           rcname(rc));
    CHECKF(out_len == want_len,
           "HEVC sub-layer profile/level hvcC length is %zu, want %zu",
           out_len == OUTLEN_SENTINEL ? (size_t)0 : out_len, want_len);
    CHECKF(guards_intact(&d),
           "HEVC sub-layer profile/level build overwrote a guard band");
    if (rc != MOQ_OK || out_len != want_len) {
        return;
    }

    const uint8_t *out = dest_ptr(&d);
    CHECKF(memcmp(out + 1, k_hevc_temporal_ptl,
                  sizeof(k_hevc_temporal_ptl)) == 0,
           "HEVC sub-layer profile/level general PTL was not copied");
    CHECKF(out[15] == 0xfc, "HEVC parallelismType byte is 0x%02x, want 0xfc",
           out[15]);
    CHECKF(out[16] == 0xfe, "HEVC chromaFormat byte is 0x%02x, want 0xfe",
           out[16]);
    CHECKF(out[17] == 0xfb, "HEVC bitDepthLumaMinus8 byte is 0x%02x, want 0xfb",
           out[17]);
    CHECKF(out[18] == 0xfa,
           "HEVC bitDepthChromaMinus8 byte is 0x%02x, want 0xfa", out[18]);
    uint8_t want_temporal = (uint8_t)(0x07u | ((max_sub + 1u) << 3));
    CHECKF(out[21] == want_temporal,
           "HEVC sub-layer profile/level byte 21 is 0x%02x, want 0x%02x",
           out[21], want_temporal);

    size_t o = 23;
    for (size_t i = 0; i < 3; i++) {
        CHECKF(o + 5 <= out_len, "HEVC profile/level array %zu header overruns", i);
        CHECKF(out[o] == (uint8_t)(0x80 | nal_type[i]),
               "HEVC profile/level array %zu type is 0x%02x, want 0x%02x",
               i, out[o], (uint8_t)(0x80 | nal_type[i]));
        o++;
        CHECKF(rd16(out + o) == 1,
               "HEVC profile/level array %zu count is %u, want 1",
               i, rd16(out + o));
        o += 2;
        CHECKF(rd16(out + o) == nal_len[i],
               "HEVC profile/level array %zu NAL length is %u, want %zu",
               i, rd16(out + o), nal_len[i]);
        o += 2;
        CHECKF(o + nal_len[i] <= out_len,
               "HEVC profile/level array %zu payload overruns", i);
        if (o + nal_len[i] <= out_len) {
            CHECKF(memcmp(out + o, nal_p[i], nal_len[i]) == 0,
                   "HEVC profile/level array %zu payload differs", i);
        }
        o += nal_len[i];
    }
    CHECKF(o == out_len, "HEVC profile/level hvcC ended at %zu, want %zu",
           o, out_len);
}

static void rows_annexb_hevc(void)
{
    row("HEVC Annex B: SPS temporal sub-layer PTL is skipped into hvcC");
    {
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        cfg.source = bytes(k_hevc_temporal_annexb,
                           sizeof(k_hevc_temporal_annexb));
        expect_hvcc_temporal_sub_layers(&cfg);
    }

    row("HEVC Annex B: sub-layer profile and level PTL bodies are skipped");
    {
        uint8_t src[512];
        size_t src_len = build_hevc_fixture(src, 1, 0, true, true, 2, 3, 2);
        expect_hvcc_generated_profile_level(src, src_len, 1);
    }

    row("HEVC Annex B: last of six sub-layer profiles is skipped");
    {
        uint8_t src[512];
        size_t src_len = build_hevc_fixture(src, 6, 5, true, false, 2, 3, 2);
        expect_hvcc_generated_profile_level(src, src_len, 6);
    }

    row("HEVC Annex B: last of six sub-layer levels is skipped");
    {
        uint8_t src[512];
        size_t src_len = build_hevc_fixture(src, 6, 5, false, true, 2, 3, 2);
        expect_hvcc_generated_profile_level(src, src_len, 6);
    }

    row("HEVC Annex B: sps_max_sub_layers_minus1 7 is malformed");
    {
        uint8_t src[512];
        size_t src_len = build_hevc_fixture(src, 7, 0, false, false, 1, 0, 0);
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        cfg.source = bytes(src, src_len);
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 512,
                       "sps_max_sub_layers_minus1 outside HEVC range");
    }

    row("HEVC Annex B: no VPS");
    {
        /* SPS + PPS only. */
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        size_t sps_off = nal_start(k_hevc_annexb, sizeof(k_hevc_annexb), 1);
        cfg.source = bytes(k_hevc_annexb + sps_off,
                           sizeof(k_hevc_annexb) - sps_off);
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 512, "hvcC with no VPS");
    }

    row("HEVC Annex B: no PPS");
    {
        /* VPS + SPS only: drop the trailing PPS NAL. */
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        size_t pps_off = nal_start(k_hevc_annexb, sizeof(k_hevc_annexb), 2);
        cfg.source = bytes(k_hevc_annexb, pps_off);
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 512, "hvcC with no PPS");
    }

    row("HEVC Annex B: a parameter set the 16-bit record field cannot hold");
    {
        static const uint8_t start[] = { 0x00, 0x00, 0x00, 0x01 };
        size_t n = 0;
        /* Real VPS + SPS, then an oversized PPS. */
        size_t pps_off = nal_start(k_hevc_annexb, sizeof(k_hevc_annexb), 2);
        memcpy(g_big_src, k_hevc_annexb, pps_off);
        n = pps_off;
        memcpy(g_big_src + n, start, 4); n += 4;
        g_big_src[n++] = 0x44; g_big_src[n++] = 0x01;
        while (n < 70000) g_big_src[n++] = 0xaa;

        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_ANNEXB;
        cfg.source = bytes(g_big_src, n);
        pin_build_fail(&cfg, MOQ_ERR_UNSUPPORTED, DESTMAX,
                       "HEVC parameter set over 65535 bytes");
    }
}

static void rows_av1(void)
{
    static const struct {
        const char *name;
        av1_seq_t   s;
    } cases[] = {
        { "AV1 profile 0, full range, chroma_sample_position UNKNOWN",
          { 0, 4, 0, 0, 0, 1, 1, 1, 0, true } },
        { "AV1 profile 0, full range, chroma_sample_position COLOCATED",
          { 0, 4, 0, 0, 0, 1, 1, 1, 2, true } },
        { "AV1 profile 0, limited range, chroma_sample_position COLOCATED",
          { 0, 4, 0, 0, 0, 0, 1, 1, 2, true } },
        { "AV1 profile 2, 12-bit, full range, 4:2:0",
          { 2, 8, 1, 1, 0, 1, 1, 1, 1, true } },
        { "AV1 profile 2, 12-bit, limited range, 4:2:2",
          { 2, 8, 1, 1, 0, 0, 1, 0, 0, false } },
        { "AV1 monochrome",
          { 0, 4, 0, 0, 1, 1, 1, 1, 0, false } },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        row(cases[i].name);

        bw_t w;
        av1_seqhdr(&w, &cases[i].s);

        uint8_t obu[512];
        size_t obu_len = obu_wrap(obu, 1 /* OBU_SEQUENCE_HEADER */, true,
                                  w.b, bw_len(&w));

        uint8_t want_cfg[2];
        av1c_expect(&cases[i].s, want_cfg);

        /* Complete expected record: 4 header bytes then the OBU verbatim. */
        uint8_t expect[512];
        expect[0] = 0x81;
        expect[1] = want_cfg[0];
        expect[2] = want_cfg[1];
        expect[3] = 0x00;
        memcpy(expect + 4, obu, obu_len);

        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_OBU;
        cfg.source = bytes(obu, obu_len);

        /* Full output inventory: exact length, exact header, and the single
         * sequence-header OBU copied byte for byte with its size field. */
        pin_build_ok(&cfg, expect, 4 + obu_len, "av1C build");

        dest_t d;
        dest_init(&d, 4 + obu_len);
        size_t out_len = OUTLEN_SENTINEL;
        if (moq_codec_init_data_build(&cfg, dest_ptr(&d), 4 + obu_len, &out_len) == MOQ_OK) {
            const uint8_t *o = dest_ptr(&d);
            CHECKF(o[0] == 0x81, "av1C marker/version byte is %02x, want 81", o[0]);
            CHECKF(o[1] == want_cfg[0] && o[2] == want_cfg[1],
                   "av1C config bytes are %02x %02x, the sequence header gives %02x %02x",
                   o[1], o[2], want_cfg[0], want_cfg[1]);
            CHECKF(o[3] == 0x00, "av1C byte 3 is %02x, want 00", o[3]);
            CHECKF(out_len == 4 + obu_len,
                   "record is %zu bytes, want %zu", out_len, 4 + obu_len);
            CHECKF((o[4] & 0x02u) != 0,
                   "the copied configOBU has no size field (header %02x)", o[4]);
            CHECKF((o[4] >> 3 & 0x0fu) == 1,
                   "the copied configOBU is type %u, want 1 (sequence header)",
                   (unsigned)(o[4] >> 3 & 0x0fu));
            CHECKF(o[5] == (uint8_t)bw_len(&w),
                   "the configOBU declares size %u but the payload is %zu bytes",
                   o[5], bw_len(&w));
            CHECKF(memcmp(o + 6, w.b, bw_len(&w)) == 0,
                   "the configOBU payload is not the sequence header byte for byte");
        }
    }

    row("AV1 configOBUs: obu_has_size_field == 0");
    {
        av1_seq_t s = { 0, 4, 0, 0, 0, 1, 1, 1, 0, true };
        bw_t w;
        av1_seqhdr(&w, &s);

        uint8_t obu[512];
        size_t obu_len = obu_wrap(obu, 1, false, w.b, bw_len(&w));

        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_OBU;
        cfg.source = bytes(obu, obu_len);
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 512, "OBU without a size field");
    }

    row("AV1 configOBUs: unterminated LEB128 size");
    {
        static const uint8_t obu[] = { 0x0a, 0x80, 0x80, 0x80 };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_OBU;
        cfg.source = bytes(obu, sizeof(obu));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 512, "unterminated LEB128");
    }

    /*
     * A LEB128 size field that never terminates, followed by a payload that
     * WOULD be a valid sequence header. The earlier row runs out of buffer
     * inside the size field, so it is caught by the bounds check rather than
     * by a termination check; this one supplies all eight continuation bytes
     * and a real payload, so only a parser that requires the field to end
     * rejects it.
     */
    row("AV1 configOBUs: eight unterminated LEB128 bytes, valid payload after");
    {
        av1_seq_t s = { 0, 4, 0, 0, 0, 1, 1, 1, 0, true };
        bw_t w;
        av1_seqhdr(&w, &s);
        size_t plen = bw_len(&w);

        uint8_t obu[64];
        size_t o = 0;
        obu[o++] = 0x0a;                             /* seq header, has_size */
        obu[o++] = (uint8_t)(0x80u | (plen & 0x7fu));/* value, continuing    */
        for (int k = 0; k < 7; k++) {
            obu[o++] = 0x80u;                        /* still continuing     */
        }
        memcpy(obu + o, w.b, plen);
        o += plen;

        /* Fixture self-check: the size field must really be eight bytes with
         * every continuation bit set, and the payload must really follow. */
        CHECKF(plen > 0 && plen < 0x80,
               "the sequence header payload is %zu bytes, which this vector "
               "cannot encode in one LEB128 value byte", plen);
        int cont = 0;
        for (size_t k = 1; k <= 8; k++) {
            if (obu[k] & 0x80u) cont++;
        }
        CHECKF(cont == 8, "only %d of the eight size-field bytes continue", cont);
        CHECKF(o == 9 + plen, "vector is %zu bytes, expected %zu", o, 9 + plen);

        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_OBU;
        cfg.source = bytes(obu, o);
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 512, "unterminated LEB128, payload present");
    }

    row("av1C pass-through: eight unterminated LEB128 bytes, valid payload after");
    {
        av1_seq_t s = { 0, 4, 0, 0, 0, 1, 1, 1, 0, true };
        bw_t w;
        av1_seqhdr(&w, &s);
        size_t plen = bw_len(&w);

        uint8_t rec[64];
        size_t o = 0;
        rec[o++] = 0x81; rec[o++] = 0x04; rec[o++] = 0x0c; rec[o++] = 0x00;
        rec[o++] = 0x0a;
        rec[o++] = (uint8_t)(0x80u | (plen & 0x7fu));
        for (int k = 0; k < 7; k++) {
            rec[o++] = 0x80u;
        }
        memcpy(rec + o, w.b, plen);
        o += plen;

        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_AV1C;
        cfg.source = bytes(rec, o);
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 512,
                       "av1C unterminated LEB128, payload present");
    }

    row("AV1: a sequence header truncated inside required syntax");
    {
        av1_seq_t s = { 0, 4, 0, 0, 0, 1, 1, 1, 0, true };
        bw_t w;
        av1_seqhdr(&w, &s);
        /* Keep only the first two bytes: seq_profile and the level are there,
         * every field color_config() needs is gone. */
        uint8_t obu[64];
        size_t obu_len = obu_wrap(obu, 1, true, w.b, 2);

        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_OBU;
        cfg.source = bytes(obu, obu_len);
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 512, "truncated sequence header");
    }
}

/*
 * Pass-through validation. The boundary being proposed is structural
 * self-consistency: every count and length a record declares about its own
 * bytes must be satisfiable from those bytes. Decoder semantics are out of
 * scope in this phase.
 */
static void rows_passthrough(void)
{
    row("avcC pass-through: a declared SPS length beyond the record");
    {
        static const uint8_t rec[] = {
            0x01, 0x42, 0x00, 0x1e, 0xff, 0xe1,
            0x00, 0x40,                   /* declares a 64-byte SPS */
            0x67, 0x42, 0x00,             /* only 3 bytes follow */
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_AVCC;
        cfg.source = bytes(rec, sizeof(rec));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "avcC internal length overrun");
    }

    row("avcC pass-through: an SPS count with no parameter sets");
    {
        static const uint8_t rec[] = {
            0x01, 0x42, 0x00, 0x1e, 0xff,
            0xe3,                         /* numOfSequenceParameterSets = 3 */
            0x00,                         /* record ends immediately */
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AVC_AVCC;
        cfg.source = bytes(rec, sizeof(rec));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "avcC internal count overrun");
    }

    row("hvcC pass-through: numOfArrays with no arrays present");
    {
        uint8_t rec[23];
        memset(rec, 0, sizeof(rec));
        rec[0] = 0x01;
        rec[22] = 0x03;                   /* numOfArrays = 3, none follow */
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_HVCC;
        cfg.source = bytes(rec, sizeof(rec));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "hvcC array count overrun");
    }

    row("hvcC pass-through: a declared NAL length beyond the record");
    {
        uint8_t rec[29];
        memset(rec, 0, sizeof(rec));
        rec[0] = 0x01;
        rec[22] = 0x01;                   /* numOfArrays = 1 */
        rec[23] = 0x20;                   /* array_completeness/NAL_unit_type */
        rec[24] = 0x00; rec[25] = 0x01;   /* numNalus = 1 */
        rec[26] = 0x00; rec[27] = 0x40;   /* nalUnitLength = 64 */
        rec[28] = 0x40;                   /* one byte follows */
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_HEVC_HVCC;
        cfg.source = bytes(rec, sizeof(rec));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "hvcC NAL length overrun");
    }

    row("av1C pass-through: a configOBU without a size field");
    {
        static const uint8_t rec[] = {
            0x81, 0x04, 0x0c, 0x00,
            0x08,                          /* obu_has_size_field == 0 */
            0x00, 0x00, 0x00, 0x02,
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_AV1C;
        cfg.source = bytes(rec, sizeof(rec));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "av1C configOBU framing");
    }

    row("av1C pass-through: a configOBU size beyond the record");
    {
        static const uint8_t rec[] = {
            0x81, 0x04, 0x0c, 0x00,
            0x0a, 0x40,                    /* declares a 64-byte OBU */
            0x00, 0x00,                    /* two bytes follow */
        };
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AV1_AV1C;
        cfg.source = bytes(rec, sizeof(rec));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "av1C configOBU length overrun");
    }

    row("dOps pass-through: a mapping family with no ChannelMappingTable");
    {
        uint8_t rec[11];
        memcpy(rec, k_dops, sizeof(rec));
        rec[10] = 0x01;                   /* ChannelMappingFamily = 1 */
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_OPUS_DOPS;
        cfg.source = bytes(rec, sizeof(rec));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "dOps missing mapping table");
    }

    row("dOps pass-through: a truncated ChannelMappingTable");
    {
        /* family 1, 2 channels: StreamCount + CoupledCount + 2 mapping bytes
         * are required; only two of the four are present. */
        uint8_t rec[13];
        memcpy(rec, k_dops, 11);
        rec[10] = 0x01;
        rec[11] = 0x02;
        rec[12] = 0x00;
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_OPUS_DOPS;
        cfg.source = bytes(rec, sizeof(rec));
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "dOps truncated mapping table");
    }

    row("ASC pass-through: an explicit-frequency escape without its 24 bits");
    {
        /*
         * samplingFrequencyIndex 0xf means a 24-bit samplingFrequency follows.
         * AOT(5) + index(4) + 24 + channelConfiguration(4) needs 37 bits;
         * two bytes cannot carry it.
         */
        bw_t w;
        bw_init(&w);
        bw_u(&w, 2, 5);
        bw_u(&w, 0xf, 4);
        bw_u(&w, 0, 7);                   /* 16 bits total, then nothing */
        moq_codec_init_data_cfg_t cfg;
        moq_codec_init_data_cfg_init(&cfg);
        cfg.source_format = MOQ_CODEC_SOURCE_AAC_ASC;
        cfg.source = bytes(w.b, 2);
        pin_build_fail(&cfg, MOQ_ERR_PROTO, 256, "ASC truncated mandatory field");
    }
}

/*
 * The formatter carries the same failure contract as the builder: a malformed
 * decoder configuration is MOQ_ERR_PROTO with *out_len == 0 and no write.
 */
static void rows_string_fail(void)
{
    row("codec string: truncated avcC");
    {
        static const uint8_t rec[] = { 0x01, 0x42, 0x00 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AVCC;
        cfg.sample_entry = entry("avc1");
        cfg.decoder_config = bytes(rec, sizeof(rec));
        pin_string_fail(&cfg, MOQ_ERR_PROTO, 64, "truncated avcC");
    }

    row("codec string: truncated hvcC");
    {
        static const uint8_t rec[] = { 0x01, 0x01, 0x60 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_HVCC;
        cfg.sample_entry = entry("hvc1");
        cfg.decoder_config = bytes(rec, sizeof(rec));
        pin_string_fail(&cfg, MOQ_ERR_PROTO, 64, "truncated hvcC");
    }

    row("codec string: av1C with an unknown version");
    {
        static const uint8_t rec[] = { 0x82, 0x04, 0x00, 0x00 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AV1C;
        cfg.sample_entry = entry("av01");
        cfg.decoder_config = bytes(rec, sizeof(rec));
        pin_string_fail(&cfg, MOQ_ERR_PROTO, 64, "av1C version");
    }

    row("codec string: truncated AudioSpecificConfig");
    {
        static const uint8_t rec[] = { 0x12 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        cfg.sample_entry = entry("mp4a");
        cfg.has_mp4_object_type_indication = true;
        cfg.mp4_object_type_indication = 0x40;
        cfg.decoder_config = bytes(rec, sizeof(rec));
        pin_string_fail(&cfg, MOQ_ERR_PROTO, 64, "truncated ASC");
    }

    row("codec string: escaped ASC truncated before channelConfiguration");
    {
        /*
         * The 11-bit GetAudioObjectType() is complete in these two bytes; what
         * is missing is samplingFrequencyIndex and channelConfiguration, so
         * the mandatory leading fields need 19 bits and only 16 are present.
         */
        static const uint8_t rec[] = { 0xf8, 0x08 };
        moq_codec_string_cfg_t cfg;
        moq_codec_string_cfg_init(&cfg);
        cfg.config_format = MOQ_CODEC_CONFIG_AAC_ASC;
        cfg.sample_entry = entry("mp4a");
        cfg.has_mp4_object_type_indication = true;
        cfg.mp4_object_type_indication = 0x40;
        cfg.decoder_config = bytes(rec, sizeof(rec));
        pin_string_fail(&cfg, MOQ_ERR_PROTO, 64, "escaped ASC truncated");
    }
}

int main(void)
{
    oracle_selfchecks();
    rows_aac();
    rows_case();
    rows_annexb_avc();
    rows_annexb_hevc();
    rows_av1();
    rows_passthrough();
    rows_string_fail();
    rows_done();

    {
        int api_rows = g_rows - g_oracle_rows;
        int api_diags = g_diags - g_oracle_diags;
        printf("spec suite: %d API rows + %d internal oracle rows; "
               "%d rows with diagnostics; "
               "%d assertions (%d passed, %d diagnostics: %d API, %d oracle)\n",
               api_rows, g_oracle_rows, g_rows_with_diags,
               g_asserts, g_asserts - g_diags, g_diags, api_diags,
               g_oracle_diags);
    }
    return g_diags ? 1 : 0;
}
