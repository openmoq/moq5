#ifndef MOQ_LOCMAF_FIXTURES_H
#define MOQ_LOCMAF_FIXTURES_H

/*
 * Shared material for the moq-locmaf tests: check macros, a hex reader,
 * track presets, the hand-built golden fixtures (Objects and their canonical
 * CMAF chunks), decoder and encoder setup over malloc'd storage, a CMAF
 * chunk builder for encoder sources, and a seeded chunk generator.
 */

#include <moq/locmaf.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int lm_failures;

#define LM_FOURCC_T(a, b, c, d) \
    (((uint32_t)(a) << 24) | ((uint32_t)(b) << 16) | \
     ((uint32_t)(c) << 8) | (uint32_t)(d))

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        lm_failures++; \
    } \
} while (0)

/* A result and, when the call reported one, its reason. */
#define CHECK_RC(actual, expected) do { \
    moq_result_t _a = (actual), _e = (expected); \
    if (_a != _e) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %d (%s), expected %d (%s)\n", \
                __FILE__, __LINE__, #actual, _a, moq_strerror(_a), _e, \
                moq_strerror(_e)); \
        lm_failures++; \
    } \
} while (0)

#define CHECK_WHY(actual, expected) do { \
    moq_locmaf_reason_t _a = (actual), _e = (expected); \
    if (_a != _e) { \
        fprintf(stderr, "FAIL: %s:%d: reason %s (%u), expected %s (%u)\n", \
                __FILE__, __LINE__, moq_locmaf_reason_name(_a), (unsigned)_a, \
                moq_locmaf_reason_name(_e), (unsigned)_e); \
        lm_failures++; \
    } \
} while (0)

#define CHECK_EQ(actual, expected) do { \
    unsigned long long _a = (unsigned long long)(actual); \
    unsigned long long _e = (unsigned long long)(expected); \
    if (_a != _e) { \
        fprintf(stderr, "FAIL: %s:%d: %s == %llu (0x%llx), expected %llu (0x%llx)\n", \
                __FILE__, __LINE__, #actual, _a, _a, _e, _e); \
        lm_failures++; \
    } \
} while (0)

/* Byte equality, with the first differing offset on failure. */
#define CHECK_BYTES(a, alen, b, blen) \
    lm_check_bytes(__FILE__, __LINE__, #a, (a), (alen), (b), (blen))
void lm_check_bytes(const char *file, int line, const char *what,
                    const uint8_t *a, size_t alen, const uint8_t *b, size_t blen);

int lm_finish(const char *name);

/* Hex digits (whitespace ignored) into out; exits on a malformed string or
 * overflow. Returns the bytes written. */
size_t lm_hex(const char *hex, uint8_t *out, size_t cap);

/* A deterministic payload: byte i is (seed + i). */
void lm_fill(uint8_t *out, size_t len, uint8_t seed);

/* -- Track presets --------------------------------------------------------- */

/* track_id 1. A: audio, clear, trex sdi 1 / dur 1024 / size 0 / flags
 * 0x02000000. V: video, clear, trex sdi 1 / dur 0 / size 0 / flags
 * 0x01010000. P8: A's trex, protected, IV 8. P0: A's trex, protected,
 * IV 0 (cbcs constant IV). P16: P8 with IV 16. */
moq_locmaf_track_t lm_track_a(void);
moq_locmaf_track_t lm_track_v(void);
moq_locmaf_track_t lm_track_p8(void);
moq_locmaf_track_t lm_track_p0(void);
moq_locmaf_track_t lm_track_p16(void);

/* -- Golden fixtures ------------------------------------------------------- */

typedef enum {
    LM_TRACK_A, LM_TRACK_V, LM_TRACK_P8, LM_TRACK_P0
} lm_track_kind_t;

typedef struct lm_fixture {
    const char     *name;
    lm_track_kind_t track;
    uint64_t        object_id;       /* in group 0, after its predecessors */
    const char     *object_hex;      /* the Object without its mdat payload */
    const char     *canonical_hex;   /* the canonical chunk through the mdat header */
    size_t          payload_len;
    const char     *payload_hex;     /* NULL: lm_fill(payload, len, seed) */
    uint8_t         payload_seed;
} lm_fixture_t;

/* The fixture sequences, each one group in Object order. */
typedef struct lm_sequence {
    const char         *name;
    const lm_fixture_t *items;
    size_t              count;
} lm_sequence_t;

extern const lm_sequence_t lm_sequences[];
extern const size_t lm_sequence_count;

moq_locmaf_track_t lm_fixture_track(const lm_fixture_t *f);

/* The fixture's Object and canonical chunk into caller buffers. */
size_t lm_fixture_object(const lm_fixture_t *f, uint8_t *out, size_t cap);
size_t lm_fixture_canonical(const lm_fixture_t *f, uint8_t *out, size_t cap);

/* -- Decoder and encoder over malloc'd storage ------------------------------ */

typedef struct lm_dec {
    moq_locmaf_decoder_t d;
    void                *storage;
    size_t               storage_size;
} lm_dec_t;

/* Limits 0 select the defaults. Returns the init result; on failure the
 * storage is already freed. */
moq_result_t lm_dec_open(lm_dec_t *dec, const moq_locmaf_track_t *track,
                         uint32_t max_samples, uint32_t max_subsamples,
                         uint32_t max_gen_boxes);
void lm_dec_close(lm_dec_t *dec);

typedef struct lm_enc {
    moq_locmaf_encoder_t e;
    void                *storage;
    size_t               storage_size;
} lm_enc_t;

moq_result_t lm_enc_open(lm_enc_t *enc, const moq_locmaf_track_t *track,
                         uint32_t max_samples, uint32_t max_subsamples,
                         uint32_t max_gen_boxes);
void lm_enc_close(lm_enc_t *enc);

/* Decode one Object and write its canonical chunk into out (cap bytes).
 * Returns the push result; *out_len is the written length on MOQ_OK. */
moq_result_t lm_decode_write(lm_dec_t *dec, const moq_locmaf_track_t *track,
                             uint64_t g, uint64_t o, const uint8_t *obj,
                             size_t obj_len, uint8_t *out, size_t cap,
                             size_t *out_len, moq_locmaf_reason_t *why);

/* -- CMAF chunk builder (encoder sources) ---------------------------------- */

typedef struct lm_gen_box {
    uint32_t       type;
    const uint8_t *payload;
    size_t         len;
} lm_gen_box_t;

/* Everything a muxer chooses about one chunk. Per-sample arrays are read
 * only when their trun flag is set (sizes also feed the mdat length check
 * of a source the test builds by hand). */
typedef struct lm_chunk_spec {
    uint32_t        track_id;
    uint32_t        mfhd_seq;
    uint8_t         tfdt_version;        /* 0 or 1 */
    uint64_t        bmdt;
    uint32_t        tfhd_flags;          /* 0x02, 0x08, 0x10, 0x20, 0x020000 ... */
    uint32_t        tfhd_sdi, tfhd_dur, tfhd_size, tfhd_flags_value;
    uint8_t         trun_version;
    uint32_t        trun_flags;          /* 0x001 is always written */
    uint32_t        first_flags;
    uint32_t        n;
    const uint32_t *durs;
    const uint32_t *sizes;
    const uint32_t *flags;
    const int64_t  *ctos;
    /* CENC: a v0 senc with flags 0x2 when sub_counts is set. */
    bool            senc;
    uint8_t         iv;
    const uint8_t  *ivs;
    const uint16_t *sub_counts;
    const uint16_t *clear;
    const uint32_t *prot;
    bool            saiz_saio;
    size_t          gen_box_count;
    const lm_gen_box_t *gen_boxes;
    const uint8_t  *payload;
    size_t          payload_len;
    /* Shape overrides for refusal cases (0 = the natural value). */
    bool            data_offset_override;
    uint32_t        data_offset;
    const uint8_t  *moof_extra;          /* whole boxes appended inside moof */
    size_t          moof_extra_len;
    const uint8_t  *traf_extra;          /* whole boxes appended inside traf */
    size_t          traf_extra_len;
} lm_chunk_spec_t;

/* Build the chunk; exits when cap is too small. Returns its length. */
size_t lm_build_chunk(const lm_chunk_spec_t *spec, uint8_t *out, size_t cap);

/* -- Seeded generator ------------------------------------------------------- */

typedef struct lm_rng { uint64_t s; } lm_rng_t;
uint64_t lm_rng_next(lm_rng_t *r);
uint32_t lm_rng_below(lm_rng_t *r, uint32_t n);

/* One generated chunk: effective values, the bytes of a muxer-style source
 * whose layout choices are drawn from the generator, and the canonical
 * expectation is computed by the library itself under test. */
typedef struct lm_gen_chunk {
    uint32_t n;
    uint64_t bmdt;
    uint32_t durs[8], sizes[8], flags[8];
    int64_t  ctos[8];
    uint16_t sub_counts[8], clear[32];
    uint32_t prot[32];
    uint8_t  ivs[8 * 16];
    uint8_t  payload[8 * 64];
    size_t   payload_len;
    uint8_t  genbox_bytes[64];
    lm_gen_box_t gen_boxes[2];
    size_t   gen_box_count;
} lm_gen_chunk_t;

/* Draw chunk number k of a group (k = 0 starts it) for the track; next_bmdt
 * is the continuation of the previous chunk, which the generator follows
 * most of the time. Writes the source chunk into out and returns its
 * length. */
size_t lm_gen_source(lm_rng_t *rng, const moq_locmaf_track_t *track,
                     uint64_t next_bmdt, bool first, lm_gen_chunk_t *g,
                     uint8_t *out, size_t cap);

#endif /* MOQ_LOCMAF_FIXTURES_H */
