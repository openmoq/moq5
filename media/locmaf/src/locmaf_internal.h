#ifndef MOQ_LOCMAF_INTERNAL_H
#define MOQ_LOCMAF_INTERNAL_H

/*
 * Private types and helpers of moq-locmaf, shared by the wire split, the
 * decoder, the canonical writer, the chunk reader and the encoder. Every
 * helper here is pure; none allocates.
 */

#include <moq/locmaf.h>
#include <moq/vi64.h>
#include <string.h>

#define LM_FOURCC(a, b, c, d) \
    (((uint32_t)(a) << 24) | ((uint32_t)(b) << 16) | \
     ((uint32_t)(c) << 8) | (uint32_t)(d))

/* The ceilings on the configured limits (moq_locmaf_*_cfg_t). */
#define LM_CEIL_SAMPLES     65536u
#define LM_CEIL_SUBSAMPLES  1048576u
#define LM_CEIL_GEN_BOXES   256u

/* Distinct unknown field IDs one header may carry (section 7.3 skips them;
 * the bound keeps duplicate detection in a fixed table). */
#define LM_UNKNOWN_FIELDS   64u

/* Section 15.7: the largest mdat payload with a 32-bit mdat size. */
#define LM_MDAT_MAX         0xFFFFFFF7u
/* Section 8.3: the largest box_size whose wrapped box fits 32 bits. */
#define LM_GEN_BOX_SIZE_MAX 0xFFFFFFFBu

/* Field IDs are bit positions: the known IDs are 1..16 and 27. */
#define LM_IDS              28u
#define LM_BIT(id)          (1u << (id))
#define LM_KNOWN_IDS        (0x0001FFFEu | LM_BIT(27))
#define LM_CENC_IDS         (LM_BIT(9) | LM_BIT(11) | LM_BIT(13) | \
                             LM_BIT(15) | LM_BIT(16))
#define LM_SCALAR_IDS       (LM_BIT(2) | LM_BIT(4) | LM_BIT(6) | LM_BIT(8) | \
                             LM_BIT(12) | LM_BIT(14) | LM_BIT(16))

/* The configured limits, defaults applied. */
typedef struct locmaf_limits {
    uint32_t samples;
    uint32_t subsamples;
    uint32_t gen_boxes;
} locmaf_limits_t;

/* -- Bytes -------------------------------------------------------------- */

static inline uint16_t lm_rd16(const uint8_t *p)
{
    return (uint16_t)(((uint32_t)p[0] << 8) | p[1]);
}

static inline uint32_t lm_rd32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

static inline uint64_t lm_rd64(const uint8_t *p)
{
    return ((uint64_t)lm_rd32(p) << 32) | lm_rd32(p + 4);
}

static inline void lm_wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

static inline void lm_wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static inline void lm_wr64(uint8_t *p, uint64_t v)
{
    lm_wr32(p, (uint32_t)(v >> 32));
    lm_wr32(p + 4, (uint32_t)v);
}

/* One vi64 at p (n bytes available): the bytes it takes, 0 when short. */
static inline size_t locmaf_vi64(const uint8_t *p, size_t n, uint64_t *v)
{
    return n ? moq_vi64_decode(p, n, v) : 0;
}

/* -- Arithmetic (each pure and total) ------------------------------------ */

/* zigzag, defined on uint64_t: no left shift of a negative value, no
 * implementation-defined signed conversion. */
static inline uint64_t locmaf_zz_enc(int64_t n)
{
    uint64_t u = (uint64_t)n;                  /* modulo 2^64: well defined */
    return (u << 1) ^ (0u - (u >> 63));
}

static inline int64_t locmaf_zz_dec(uint64_t z)
{
    uint64_t m = z >> 1;                       /* 0 .. 2^63-1: fits int64_t */
    return (z & 1u) ? -(int64_t)m - 1 : (int64_t)m;   /* -2^63 .. 2^63-1 */
}

/* prev in [lo, hi]; lo <= 0 <= hi; |lo|, hi < 2^62. On success *out =
 * prev + d, else false. Neither hi - prev nor lo - prev can overflow. */
static inline bool locmaf_add_in_range(int64_t prev, int64_t d,
                                       int64_t lo, int64_t hi, int64_t *out)
{
    if (d >= 0 ? d > hi - prev : d < lo - prev) return false;
    *out = prev + d;
    return true;
}

/* acc + x for unsigned accumulation, checked first. */
static inline bool locmaf_u64_add(uint64_t acc, uint64_t x, uint64_t *out)
{
    if (x > UINT64_MAX - acc) return false;
    *out = acc + x;
    return true;
}

/* size_t a + b, checked first. */
static inline bool lm_size_add(size_t a, size_t b, size_t *out)
{
    if (b > SIZE_MAX - a) return false;
    *out = a + b;
    return true;
}

/* -- Track facts and limits --------------------------------------------- */

/* Copy a caller's track record (borrowed for the call) into *out. INVAL for
 * NULL, a record below its one extent, or an IV size other than 0, 8, 16. */
moq_result_t locmaf_track_read(const moq_locmaf_track_t *track,
                               moq_locmaf_track_t *out);

/* Resolve configured limits (0 = default). INVAL above a ceiling. */
moq_result_t locmaf_limits_resolve(uint32_t samples, uint32_t subsamples,
                                   uint32_t gen_boxes, locmaf_limits_t *out);

/* -- Storage layout ------------------------------------------------------ */

/*
 * The caller storage is carved into 8-aligned regions, each used only at its
 * own element type. A reference slot holds the represented lists of one
 * chunk; the tables hold one chunk's samples, subsamples and genBoxes.
 */
typedef struct locmaf_carve {
    uint8_t *base;        /* NULL: measure only */
    size_t   off;
    bool     ok;
} locmaf_carve_t;

/* The next region of count elements of elem bytes: its address (NULL when
 * measuring or after an overflow). */
void *locmaf_carve_take(locmaf_carve_t *c, size_t count, size_t elem);

/* Carve one reference slot's lists for the limits. */
void locmaf_carve_ref(locmaf_carve_t *c, const locmaf_limits_t *lim,
                      moq_locmaf_ref_state_t *ref);

/* Clear a reference's presence and counts (its lists keep their regions). */
void locmaf_ref_clear(moq_locmaf_ref_state_t *ref);

/* Copy src's represented fields into dst's own regions; dst's regions hold
 * every list src has (the caller checked the limits). */
void locmaf_ref_copy(moq_locmaf_ref_state_t *dst, const moq_locmaf_ref_state_t *src);

/*
 * Which represented fields a chunk leaves as the in-group reference
 * (section 12.1): its own, whatever the header kind, so every delta applies
 * against the previous chunk of its group (sections 1, 3, 12.1 to 12.4, 17).
 * Returns the state that becomes the reference for the next delta: cur.
 * The encoder adopts the returned slot at commit; the decoder applies every
 * header to its one reference in place, which is this same rule.
 */
static inline const moq_locmaf_ref_state_t *
locmaf_ref_next(const moq_locmaf_ref_state_t *prev,
                const moq_locmaf_ref_state_t *cur,
                moq_locmaf_object_kind_t kind)
{
    (void)prev;
    (void)kind;
    return cur;
}

/* Element i of list field id (1, 3, 5, 7, 11, 13, 15) of a reference. */
int64_t locmaf_ref_elem(const moq_locmaf_ref_state_t *ref, unsigned id,
                        size_t i);

/* -- Wire split (locmaf_wire.c) ------------------------------------------ */

typedef struct locmaf_split {
    moq_locmaf_object_kind_t kind;
    size_t                   gen_box_count;
    moq_bytes_t              properties;
    moq_bytes_t              mdat_payload;
    moq_bytes_t              raw_boxes;
} locmaf_split_t;

/*
 * Split one Object into its elements and check its framing and boxes
 * (sections 7.1, 7.2, 7.3 framing, 8, 9.1, 9.2, 18). track may be NULL (no
 * track facts: a senc inside rawBoxes is then accepted under any IV size a
 * track can have). Stores the first cap
 * genBoxes in boxes and counts all of them; the caller judges a count above
 * its capacity. Returns OK, or PROTO / UNSUPPORTED with *why set, with
 * *out unspecified.
 */
moq_result_t locmaf_split(const moq_locmaf_track_t *track, moq_bytes_t payload,
                          locmaf_split_t *out, moq_locmaf_gen_box_t *boxes,
                          size_t cap, moq_locmaf_reason_t *why);

/* True iff block is an exact sequence of (id, value) tuples by the parity
 * rule (section 7.3). */
bool locmaf_tuples_framing(moq_bytes_t block);

/* True iff bytes is one or more complete boxes with 32-bit sizes of at least
 * 8 that sum to its length (sections 9.1, 18). */
bool locmaf_raw_boxes_ok(moq_bytes_t bytes);

/* rawBoxes accepted only as one CMAF chunk whose every box has a validator
 * (section 18): PROTO RAW_BOXES_MALFORMED for a structural failure,
 * UNSUPPORTED RAW_BOXES_UNSUPPORTED for any other shape or box type. The
 * bytes already passed locmaf_raw_boxes_ok(). track may be NULL, as for
 * locmaf_split(). */
moq_result_t locmaf_raw_chunk_check(const moq_locmaf_track_t *track,
                                    moq_bytes_t bytes, moq_locmaf_reason_t *why);

/* False for the box types that are never genBoxes: moof, mdat, ftyp, moov. */
bool locmaf_gen_box_type_ok(uint32_t type);

/* A genBox payload against the structural rules of its type (section 18):
 * OK, PROTO GEN_BOX_STRUCTURE, or UNSUPPORTED GEN_BOX_UNSUPPORTED for a type
 * with no validator here. */
moq_result_t locmaf_gen_box_check(uint32_t type, moq_bytes_t payload,
                                  moq_locmaf_reason_t *why);

/* -- Tuple pass 1 (locmaf_wire.c) ---------------------------------------- */

typedef struct locmaf_tuples {
    uint32_t    carried;            /* LM_BIT(id) of every known ID present */
    uint64_t    scalar[LM_IDS];     /* even IDs: the vi64 value as read */
    moq_bytes_t bytes[LM_IDS];      /* odd IDs: the value bytes */
    uint64_t    unknown;            /* unknown fields skipped */
} locmaf_tuples_t;

/*
 * Index the tuples of a framed property block (section 7.3): duplicates of
 * a known or unknown ID, the kind restrictions of IDs 10 and 27, and CENC
 * IDs on an unprotected track (section 10.3). PROTO or LIMIT with *why.
 */
moq_result_t locmaf_tuples(moq_bytes_t block, bool is_full,
                           const moq_locmaf_track_t *track,
                           locmaf_tuples_t *out, moq_locmaf_reason_t *why);

/* -- Canonical decisions (locmaf_canon.c) -------------------------------- */

/*
 * The canonical field set (section 11.1) and box flags (sections 15.4,
 * 15.6) of one chunk's effective values. Writer and encoder both use this
 * one function, so they cannot disagree.
 */
typedef struct locmaf_canon {
    uint32_t n;
    bool     sdi;              /* field 2, tfhd 0x000002 */
    bool     dur_list;         /* field 3, trun 0x000100 */
    bool     dur_default;      /* field 4, tfhd 0x000008 */
    bool     size_list;        /* field 1, trun 0x000200 */
    bool     size_wire;        /* field 6 */
    bool     size_box;         /* tfhd 0x000010 */
    bool     first_flags;      /* field 12, trun 0x000004 */
    bool     flags_list;       /* field 7, trun 0x000400 */
    bool     flags_default;    /* field 8, tfhd 0x000020 */
    bool     cto;              /* field 5, trun 0x000800 */
    bool     cto_signed;       /* trun version 1 */
    uint32_t dur;              /* the default duration when dur_default */
    uint32_t size;             /* the uniform size when size_wire/size_box */
    uint32_t flags;            /* the default flags when flags_default */
    uint32_t first;            /* the first-sample flags when first_flags */
} locmaf_canon_t;

void locmaf_decide(const moq_locmaf_track_t *track, uint32_t sdi,
                   const moq_locmaf_sample_t *samples, uint32_t n,
                   uint64_t payload_len, locmaf_canon_t *out);

/* -- Canonical writer (locmaf_write.c) ----------------------------------- */

/* Section 15.7: a moof-carrying chunk with an mdat payload above
 * 0xFFFFFFF7 bytes cannot be packaged. */
bool locmaf_check_mdat_len(uint64_t payload_len, moq_locmaf_reason_t *why);

/* The moof layout limit (section 15.7): data_offset = moof_size + 8 must
 * fit the signed 32-bit trun field. */
bool locmaf_check_moof_size(uint64_t moof_size);

/* -- Chunk reader (locmaf_chunk.c) --------------------------------------- */

/* One CMAF chunk read losslessly into effective values (encoder input). */
typedef struct locmaf_src {
    size_t                  gen_box_count;
    uint64_t                bmdt;
    uint32_t                sdi;
    uint32_t                n;
    bool                    has_aux_info;  /* senc carries IVs or subsamples */
    bool                    has_subsamples;
    uint8_t                 iv;            /* the senc IV size (tenc default) */
    size_t                  subsample_total;
    moq_bytes_t             mdat_payload;
} locmaf_src_t;

/* Scratch the reader fills: limits-sized tables in encoder storage. */
typedef struct locmaf_scratch {
    moq_locmaf_sample_t    *samples;
    moq_locmaf_subsample_t *subsamples;
    moq_locmaf_gen_box_t   *gen_boxes;
    uint8_t                *ivs;
} locmaf_scratch_t;

moq_result_t locmaf_read_chunk(const moq_locmaf_track_t *track,
                               moq_bytes_t bytes, const locmaf_limits_t *lim,
                               locmaf_src_t *src, const locmaf_scratch_t *scr,
                               moq_locmaf_reason_t *why);

/* The encode bound of section 3.4 for a chunk the reader accepted; false
 * when it does not fit size_t. */
bool locmaf_encode_bound(const locmaf_src_t *src,
                         const moq_locmaf_gen_box_t *gen_boxes, size_t *out);

/* The bound for synthetic counts (genBox bytes, n, U, P), for tests at the
 * limits. */
bool locmaf_encode_bound_counts(uint64_t gen_box_bytes, size_t gen_box_count,
                                uint64_t n, uint64_t subsamples,
                                uint64_t payload_len, size_t *out);

static inline void lm_why(moq_locmaf_reason_t *why, moq_locmaf_reason_t r)
{
    if (why) *why = r;
}

#endif /* MOQ_LOCMAF_INTERNAL_H */
