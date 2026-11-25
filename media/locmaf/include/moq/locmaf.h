#ifndef MOQ_LOCMAF_H
#define MOQ_LOCMAF_H

/*
 * LOCMAF: Low Overhead CMAF for Media over QUIC
 * (draft-einarsson-moq-locmaf-01; Object encoding version "0.3").
 *
 * A LOCMAF Object carries one CMAF chunk (section 7): the boxes before its
 * moof as generic box elements, the moof as tagged fields -- absolute in a
 * full header, differences against the previous chunk of the same MOQT group
 * in a delta header -- and the mdat payload unchanged. This library encodes a
 * CMAF chunk into a LOCMAF Object, decodes a LOCMAF Object into the chunk's
 * effective values (section 15.1), and writes the canonical CMAF chunk those
 * values define (section 15).
 *
 * Sans-I/O and allocation-free. The encoder and decoder are caller-owned
 * state over caller storage sized by their _storage_size function; every
 * other output is written into a caller buffer or borrows from the caller's
 * input. One encoder or decoder serves one track; neither is thread-safe, and
 * calls on one instance must be serialized. The canonical writer and the
 * object parser are pure.
 *
 * Integers in a LOCMAF Object are the MoQT vi64 of
 * draft-ietf-moq-transport-18 section 1.4.1, whatever draft the session
 * carrying the Object negotiated: the payload is end to end and is never
 * re-encoded by a relay. Decoders accept any valid vi64 length; encoders
 * write the shortest.
 *
 * Link against moq::locmaf (depends on moq::core and moq::cmaf).
 */

#include <stddef.h>
#include <moq/types.h>
#include <moq/cmaf.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The catalog locmafVersion this build encodes and decodes (section 5). */
#define MOQ_LOCMAF_VERSION "0.3"

/* -- Wire vocabulary (sections 7.1 and 10) -------------------------------- */

#define MOQ_LOCMAF_ELEMENT_GEN_BOX       1u
#define MOQ_LOCMAF_ELEMENT_FULL_HEADER   2u
#define MOQ_LOCMAF_ELEMENT_DELTA_HEADER  3u
#define MOQ_LOCMAF_ELEMENT_RAW_BOXES     4u

#define MOQ_LOCMAF_FIELD_TRUN_SAMPLE_SIZES          1u  /* list, n-1 entries */
#define MOQ_LOCMAF_FIELD_TFHD_SAMPLE_DESC_INDEX     2u
#define MOQ_LOCMAF_FIELD_TRUN_SAMPLE_DURATIONS      3u  /* list */
#define MOQ_LOCMAF_FIELD_TFHD_DEFAULT_DURATION      4u
#define MOQ_LOCMAF_FIELD_TRUN_CTO                   5u  /* signed list */
#define MOQ_LOCMAF_FIELD_TFHD_DEFAULT_SIZE          6u
#define MOQ_LOCMAF_FIELD_TRUN_SAMPLE_FLAGS          7u  /* list */
#define MOQ_LOCMAF_FIELD_TFHD_DEFAULT_FLAGS         8u
#define MOQ_LOCMAF_FIELD_SENC_IVS                   9u  /* raw bytes */
#define MOQ_LOCMAF_FIELD_TFDT_BMDT                 10u  /* full header only */
#define MOQ_LOCMAF_FIELD_SENC_SUBSAMPLE_COUNTS     11u  /* list */
#define MOQ_LOCMAF_FIELD_TRUN_FIRST_SAMPLE_FLAGS   12u
#define MOQ_LOCMAF_FIELD_SENC_CLEAR_BYTES          13u  /* list */
#define MOQ_LOCMAF_FIELD_TRUN_SAMPLE_COUNT         14u
#define MOQ_LOCMAF_FIELD_SENC_PROTECTED_BYTES      15u  /* list */
#define MOQ_LOCMAF_FIELD_SENC_PER_SAMPLE_IV_SIZE   16u
#define MOQ_LOCMAF_FIELD_DELETED_IDS               27u  /* delta header only */

/* -- Object kinds and reasons --------------------------------------------- */

/* The shape of a LOCMAF Object. Fixed-width uint32_t for ABI stability.
 * Values are never renumbered. Unknown kinds must be ignored. */
typedef uint32_t moq_locmaf_object_kind_t;
#define MOQ_LOCMAF_OBJECT_NONE       0u
#define MOQ_LOCMAF_OBJECT_FULL       1u  /* genBox* locmafFullHeader mdat */
#define MOQ_LOCMAF_OBJECT_DELTA      2u  /* genBox* locmafDeltaHeader mdat */
#define MOQ_LOCMAF_OBJECT_RAW_BOXES  3u  /* one rawBoxes element */

/* Why an Object or a CMAF chunk was refused. Fixed-width uint32_t for ABI
 * stability. Values are never renumbered. Unknown reasons must be ignored. */
typedef uint32_t moq_locmaf_reason_t;
#define MOQ_LOCMAF_REASON_NONE                   0u
/* Decoding a LOCMAF Object (MOQ_ERR_PROTO unless noted). */
#define MOQ_LOCMAF_REASON_EMPTY_OBJECT           1u  /* no element */
#define MOQ_LOCMAF_REASON_UNKNOWN_ELEMENT        2u  /* section 7.1 */
#define MOQ_LOCMAF_REASON_RAW_BOXES_NOT_FIRST    3u  /* sections 7.1, 9.2 */
#define MOQ_LOCMAF_REASON_TRUNCATED              4u  /* an element or vi64 runs past the Object */
#define MOQ_LOCMAF_REASON_GEN_BOX_SIZE           5u  /* box_size < 4 or > 0xFFFFFFFB (8.1, 8.3) */
#define MOQ_LOCMAF_REASON_GEN_BOX_TYPE           6u  /* genBox carrying moof, mdat, ftyp or moov */
#define MOQ_LOCMAF_REASON_NO_HEADER              7u  /* genBoxes with no header (7.1) */
#define MOQ_LOCMAF_REASON_RAW_BOXES_MALFORMED    8u  /* sections 9.1, 18 */
#define MOQ_LOCMAF_REASON_PROPERTY_FRAMING       9u  /* a tuple crosses properties_length (7.2, 7.3) */
#define MOQ_LOCMAF_REASON_DUPLICATE_FIELD       10u  /* section 7.3 */
#define MOQ_LOCMAF_REASON_FIELD_KIND            11u  /* ID 10 in a delta, ID 27 in a full header */
#define MOQ_LOCMAF_REASON_MISSING_FIELD         12u  /* full header without ID 14 or ID 10 */
#define MOQ_LOCMAF_REASON_LIST_LENGTH           13u  /* list length differs from what n requires */
#define MOQ_LOCMAF_REASON_VALUE_RANGE           14u  /* a value outside its ISO field */
#define MOQ_LOCMAF_REASON_DELETION              15u  /* an ID 27 entry that cannot be deleted */
#define MOQ_LOCMAF_REASON_SAMPLE_SIZES          16u  /* sections 11.2, 15.6.1 */
#define MOQ_LOCMAF_REASON_CLEAR_TRACK_CENC      17u  /* section 10.3 */
#define MOQ_LOCMAF_REASON_IV_LENGTH             18u  /* ID 9 is not n * IV size bytes */
#define MOQ_LOCMAF_REASON_CENC_LAYOUT           19u  /* subsample lists without counts; aux with n = 0 */
#define MOQ_LOCMAF_REASON_SUBSAMPLE_SUM         20u  /* section 18 */
#define MOQ_LOCMAF_REASON_AUX_INFO_SIZE         21u  /* sections 15.8, 18 */
#define MOQ_LOCMAF_REASON_MDAT_TOO_LARGE        22u  /* section 15.7 */
#define MOQ_LOCMAF_REASON_DELTA_AFTER_RAW_BOXES 23u  /* section 9.3 */
#define MOQ_LOCMAF_REASON_NO_REFERENCE          24u  /* MOQ_ERR_WRONG_STATE: delta without reference */
#define MOQ_LOCMAF_REASON_LIMIT                 25u  /* MOQ_ERR_LIMIT: beyond the configured limits */
#define MOQ_LOCMAF_REASON_GEN_BOX_REPEAT        26u  /* a second styp or prft genBox (section 2) */
#define MOQ_LOCMAF_REASON_GEN_BOX_STRUCTURE     27u  /* a genBox whose contents break its box
                                                        definition (section 18) */
#define MOQ_LOCMAF_REASON_GEN_BOX_UNSUPPORTED   28u  /* MOQ_ERR_UNSUPPORTED: a genBox type with no
                                                        validator in this library (section 18) */
#define MOQ_LOCMAF_REASON_RAW_BOXES_UNSUPPORTED 29u  /* MOQ_ERR_UNSUPPORTED: rawBoxes that are not
                                                        one CMAF chunk of validated boxes (18) */
/* Encoding a CMAF chunk. */
#define MOQ_LOCMAF_REASON_MALFORMED_CMAF        40u  /* MOQ_ERR_PROTO */
#define MOQ_LOCMAF_REASON_TRACK_ID              41u  /* MOQ_ERR_PROTO: tfhd track_ID != the header's */
#define MOQ_LOCMAF_REASON_MULTI_CHUNK           42u  /* MOQ_ERR_UNSUPPORTED: more than one moof+mdat */
#define MOQ_LOCMAF_REASON_STRUCTURE             43u  /* UNSUPPORTED: not one traf with one tfhd, tfdt, trun */
#define MOQ_LOCMAF_REASON_DATA_LAYOUT           44u  /* UNSUPPORTED: samples do not fill the mdat payload */
#define MOQ_LOCMAF_REASON_SAMPLE_GROUPS         45u  /* UNSUPPORTED: sgpd/sbgp (4.1, 13.2) */
#define MOQ_LOCMAF_REASON_FRAGMENT_PSSH         46u  /* UNSUPPORTED: pssh in moof (13.2) */
#define MOQ_LOCMAF_REASON_SUBS                  47u  /* UNSUPPORTED: subs (13.2) */
#define MOQ_LOCMAF_REASON_AUX_INFO              48u  /* UNSUPPORTED: CENC aux info not in a v0 senc */
#define MOQ_LOCMAF_REASON_UNKNOWN_BOX           49u  /* UNSUPPORTED: a moof/traf box outside the field model */
#define MOQ_LOCMAF_REASON_SIZE_ESCAPE           50u  /* UNSUPPORTED: box size 0 or 64-bit largesize */

/* A stable name for diagnostics ("" for an unknown value). Pure. */
MOQ_API const char *moq_locmaf_reason_name(moq_locmaf_reason_t reason);

/* -- Track facts (section 6) ------------------------------------------------ */

/*
 * The CMAF Header facts the encoding is relative to: the single trak's
 * track_ID, the trex defaults, and the track encryption defaults. Usually
 * filled by moq_locmaf_track_from_cmaf(); a caller may fill it by hand.
 */
typedef struct moq_locmaf_track {
    uint32_t struct_size;
    uint32_t track_id;                              /* tkhd track_ID, nonzero */
    uint32_t trex_default_sample_description_index;
    uint32_t trex_default_sample_duration;          /* timescale units */
    uint32_t trex_default_sample_size;              /* bytes */
    uint32_t trex_default_sample_flags;             /* sample_flags */
    bool     is_protected;                          /* tenc default_isProtected == 1 */
    uint8_t  default_per_sample_iv_size;            /* tenc: 0, 8 or 16 */
    uint8_t  _reserved[2];                          /* zero; never read */
} moq_locmaf_track_t;

/* The record's one capability extent: every reader requires all of it. A
 * later field is appended as a new extent, read only when covered whole. */
#define MOQ_LOCMAF_TRACK_V0_SIZE \
    (offsetof(moq_locmaf_track_t, _reserved) + 2u * sizeof(uint8_t))

/* Clear and stamp the largest extent track_size holds; writes nothing when
 * track_size is below MOQ_LOCMAF_TRACK_V0_SIZE. */
MOQ_API void moq_locmaf_track_init_sized(moq_locmaf_track_t *track,
                                         size_t track_size);
#define moq_locmaf_track_init(track) \
    moq_locmaf_track_init_sized((track), sizeof(*(track)))

/*
 * Take the track facts from a parsed CMAF Header (moq_cmaf_parse_init_sized).
 * Reads init only; nothing is retained.
 *
 * Returns MOQ_OK, writing the largest extent out_size holds and stamping it.
 * Returns MOQ_ERR_INVAL, writing nothing, for NULL arguments, out_size below
 * MOQ_LOCMAF_TRACK_V0_SIZE, or an init record whose struct_size does not
 * cover MOQ_CMAF_INIT_INFO_SIZE_THROUGH_CONSTANT_IV.
 * Returns MOQ_ERR_PROTO, writing nothing, when the header is not one LOCMAF
 * can use: trak_count is not 1 (sections 4.1, 6), there is no trex for the
 * track, track_id is 0, or a protected track's default per-sample IV size is
 * not 0, 8 or 16.
 */
MOQ_API moq_result_t moq_locmaf_track_from_cmaf(const moq_cmaf_init_info_t *init,
                                                moq_locmaf_track_t *out,
                                                size_t out_size);

/* -- Stateless object split (section 7.1) ----------------------------------- */

/* One generic box element (section 8): a box before the moof. Frozen leaf. */
typedef struct moq_locmaf_gen_box {
    uint32_t    type;        /* box FourCC, big-endian (a<<24|b<<16|c<<8|d) */
    uint32_t    _reserved;   /* zero */
    moq_bytes_t payload;     /* box contents after the 8-byte header; a uuid
                                box's 16-byte usertype comes first */
} moq_locmaf_gen_box_t;

typedef struct moq_locmaf_object_view {
    uint32_t                 struct_size;
    moq_locmaf_object_kind_t kind;
    size_t                   gen_box_count;   /* genBox elements in the Object */
    moq_bytes_t              properties;      /* the header's property block */
    moq_bytes_t              mdat_payload;    /* bytes after the header (may be empty) */
    moq_bytes_t              raw_boxes;       /* RAW_BOXES only: the boxes, verbatim */
} moq_locmaf_object_view_t;

#define MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE \
    (offsetof(moq_locmaf_object_view_t, raw_boxes) + sizeof(moq_bytes_t))

/*
 * Split one LOCMAF Object payload into its elements and check its framing:
 * element types (7.1), genBox sizes (8.1, 8.3), the header's property framing
 * by the parity rule (7.3), and rawBoxes structure (9.1). Every box is
 * validated against the structural rules for its type before it can be used
 * (section 18): genBoxes of type styp, prft, emsg, sidx, uuid, free and skip,
 * and, inside rawBoxes, one CMAF chunk whose every box has a validator here.
 * A box of any other type is refused (MOQ_ERR_UNSUPPORTED), never carried
 * unvalidated. moof, mdat, ftyp and moov are malformed as genBoxes, and so
 * is a second styp or prft (section 2 allows at most one each in a CMAF
 * chunk). Unknown header field IDs are a different matter: they are skipped
 * by the parity rule (7.3). Field values are not judged here;
 * moq_locmaf_decoder_push() does that.
 * Pure: no state, no allocation.
 *
 * Every span in *out and in gen_boxes BORROWS from payload.
 *
 * Returns MOQ_OK with *out stamped (its one extent, MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE),
 * *gen_box_count = out->gen_box_count, and that many entries in gen_boxes.
 * Returns MOQ_ERR_BUFFER when gen_box_cap is smaller than the Object's genBox
 * count: *out is filled, *gen_box_count is the count, and the first
 * gen_box_cap entries are written. gen_boxes may be NULL with cap 0 (probe).
 * Returns MOQ_ERR_PROTO with *reason set when the framing or a box is
 * malformed, and MOQ_ERR_UNSUPPORTED with *reason GEN_BOX_UNSUPPORTED or
 * RAW_BOXES_UNSUPPORTED for a box this library cannot validate; in both
 * cases *gen_box_count is 0 and *out is not written.
 * Returns MOQ_ERR_INVAL for out or gen_box_count NULL, out_size below
 * MOQ_LOCMAF_OBJECT_VIEW_V0_SIZE, payload.data NULL with a nonzero length, or
 * gen_boxes NULL with gen_box_cap nonzero. reason may be NULL.
 */
MOQ_API moq_result_t moq_locmaf_parse_object(moq_bytes_t payload,
                                             moq_locmaf_object_view_t *out,
                                             size_t out_size,
                                             moq_locmaf_gen_box_t *gen_boxes,
                                             size_t gen_box_cap,
                                             size_t *gen_box_count,
                                             moq_locmaf_reason_t *reason);

/* -- Effective values (section 15.1) ---------------------------------------- */

/* One sample's effective values. Frozen leaf. */
typedef struct moq_locmaf_sample {
    uint32_t duration;            /* timescale units */
    uint32_t size;                /* bytes */
    uint32_t flags;               /* ISO/IEC 14496-12 sample_flags */
    uint32_t subsample_count;     /* senc subsample_count; 0 without a map */
    int64_t  composition_offset;  /* timescale units, -2^31 .. 2^32-1 */
    uint64_t offset;              /* first byte of the sample in mdat_payload */
    uint32_t subsample_index;     /* its first entry in the chunk's subsamples */
    uint32_t _reserved;           /* zero */
} moq_locmaf_sample_t;

/* One senc subsample entry. Frozen leaf. */
typedef struct moq_locmaf_subsample {
    uint32_t clear_bytes;         /* BytesOfClearData, 0 .. 65535 */
    uint32_t protected_bytes;     /* BytesOfProtectedData */
} moq_locmaf_subsample_t;

/*
 * One decoded chunk: the effective values of section 15.1, the genBoxes in
 * Object order, and the mdat payload -- everything the canonical CMAF chunk
 * is a function of, together with the track facts. For RAW_BOXES only kind
 * and raw_boxes are set.
 */
typedef struct moq_locmaf_chunk {
    uint32_t                      struct_size;
    moq_locmaf_object_kind_t      kind;
    uint64_t                      base_media_decode_time; /* tfdt, timescale units */
    uint32_t                      sample_description_index;
    uint32_t                      sample_count;
    const moq_locmaf_sample_t    *samples;          /* sample_count entries */
    uint8_t                       per_sample_iv_size;/* effective; 0, 8 or 16 */
    bool                          has_aux_info;     /* senc, saiz and saio are rebuilt */
    bool                          has_subsamples;   /* senc flags 0x000002 */
    uint8_t                       _reserved[5];     /* zero */
    moq_bytes_t                   ivs;              /* sample_count * per_sample_iv_size
                                                       bytes when has_aux_info */
    const moq_locmaf_subsample_t *subsamples;
    size_t                        subsample_total;  /* sum of samples[i].subsample_count */
    const moq_locmaf_gen_box_t   *gen_boxes;
    size_t                        gen_box_count;
    moq_bytes_t                   mdat_payload;
    moq_bytes_t                   raw_boxes;
} moq_locmaf_chunk_t;

#define MOQ_LOCMAF_CHUNK_V0_SIZE \
    (offsetof(moq_locmaf_chunk_t, raw_boxes) + sizeof(moq_bytes_t))

/* -- Decoder ---------------------------------------------------------------- */

/* Limits: 0 selects the default. Storage grows linearly with them. */
#define MOQ_LOCMAF_DEFAULT_MAX_SAMPLES    1024u    /* per chunk; at most 65536 */
#define MOQ_LOCMAF_DEFAULT_MAX_SUBSAMPLES 4096u    /* per chunk; at most 1048576 */
#define MOQ_LOCMAF_DEFAULT_MAX_GEN_BOXES  16u      /* per Object; at most 256 */

typedef struct moq_locmaf_decoder_cfg {
    uint32_t                  struct_size;
    uint32_t                  max_samples;
    uint32_t                  max_subsamples;
    uint32_t                  max_gen_boxes;
    const moq_locmaf_track_t *track;        /* borrowed for the call; copied */
    void                     *storage;      /* see MOQ_LOCMAF_STORAGE_ALIGN */
    size_t                    storage_size; /* bytes at storage */
} moq_locmaf_decoder_cfg_t;

/* Decoder and encoder storage.
 *
 * Source: raw memory returned by an allocation function -- malloc,
 * aligned_alloc, a moq_alloc_t, C++ ::operator new, Rust std::alloc::alloc
 * -- so that it holds no object of a declared type (ISO C11 6.5p6). A
 * declared array (static, automatic or a struct member) and the elements of
 * a typed container (a std::vector<T>, a Box<[T]>, a Vec<T>) are not
 * suitable: they already hold live objects of another type.
 * Alignment: at least MOQ_LOCMAF_STORAGE_ALIGN; init refuses less.
 * Exclusive use: from init until the caller stops using the decoder or
 * encoder, nothing else reads or writes the storage. The library carves it
 * into typed arrays and accesses each only at its own type; a later init
 * over the same storage may carve it differently.
 * Lifetime: the storage must stay allocated while the decoder or encoder is
 * used and while any chunk view it returned is read. There is no fini: the
 * caller frees the storage with the deallocator matching its allocation once
 * it stops using the decoder or encoder.
 * The state struct: moq_locmaf_decoder_t and moq_locmaf_encoder_t record
 * pointers into the storage. Do not copy one by assignment or memcpy (the
 * copy would share the storage); use moq_locmaf_decoder_copy(), which copies
 * into the destination's own storage. Keep the struct at one address while
 * it is in use.
 * Threads: a decoder or encoder has no thread affinity. It may be used from
 * one thread and then another, but calls on one decoder or encoder must not
 * overlap. */
#define MOQ_LOCMAF_STORAGE_ALIGN 8u

/* The cfg's one capability extent. moq_locmaf_decoder_init() refuses a cfg
 * below it before reading any field; a later field is appended as a new
 * extent, read only when struct_size covers it whole. */
#define MOQ_LOCMAF_DECODER_CFG_V0_SIZE \
    (offsetof(moq_locmaf_decoder_cfg_t, storage_size) + sizeof(size_t))

/* Clear and stamp the largest extent cfg_size holds (limits 0 = defaults);
 * writes nothing below MOQ_LOCMAF_DECODER_CFG_V0_SIZE. */
MOQ_API void moq_locmaf_decoder_cfg_init_sized(moq_locmaf_decoder_cfg_t *cfg,
                                               size_t cfg_size);
#define moq_locmaf_decoder_cfg_init(cfg) \
    moq_locmaf_decoder_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * The storage a decoder with cfg's limits needs (track and storage are not
 * read). Returns MOQ_OK with *out_bytes set, or MOQ_ERR_INVAL for NULL
 * arguments, a cfg below MOQ_LOCMAF_DECODER_CFG_V0_SIZE, or a limit above its
 * maximum.
 */
MOQ_API moq_result_t moq_locmaf_decoder_storage_size(
    const moq_locmaf_decoder_cfg_t *cfg, size_t *out_bytes);

/* Counters of one decoder since its init; never reset by
 * moq_locmaf_decoder_reset(). Sized record, one extent. */
typedef struct moq_locmaf_decoder_stats {
    uint32_t struct_size;
    moq_locmaf_reason_t last_reason;    /* of the last refused Object; 0 if none */
    uint64_t objects_decoded;           /* MOQ_OK, any kind */
    uint64_t full_headers_decoded;
    uint64_t delta_headers_decoded;
    uint64_t raw_boxes_decoded;
    uint64_t objects_unreferenced;      /* MOQ_ERR_WRONG_STATE */
    uint64_t objects_malformed;         /* MOQ_ERR_PROTO */
    uint64_t objects_limited;           /* MOQ_ERR_LIMIT */
    uint64_t references_discarded;      /* a held reference dropped: new group,
                                           gap, reset, rawBoxes or refusal */
    uint64_t unknown_fields_skipped;    /* section 7.3 */
    uint64_t objects_unsupported;       /* MOQ_ERR_UNSUPPORTED (unvalidatable box) */
} moq_locmaf_decoder_stats_t;

#define MOQ_LOCMAF_DECODER_STATS_V0_SIZE \
    (offsetof(moq_locmaf_decoder_stats_t, objects_unsupported) + sizeof(uint64_t))

/* The in-group reference (section 12.1), a building block of the decoder
 * and encoder states. Its members are private and may change in any release;
 * the type is public only so those states are complete types. */
typedef struct moq_locmaf_ref_state {
    uint32_t  _present;              /* bit n: field n is in the reference */
    uint32_t  _count[16];            /* odd field ID: elements; [9]: IV bytes */
    uint64_t  _scalar[17];           /* even field ID; [10]: that chunk's BMDT */
    uint32_t *_sizes, *_durations, *_flags, *_subsample_counts;
    uint32_t *_clear, *_protected;
    int64_t  *_ctos;
    uint8_t  *_ivs;
} moq_locmaf_ref_state_t;

/* Caller-owned decoder state, a complete type: declare or allocate one and
 * pass sizeof(moq_locmaf_decoder_t) as d_size. Every member after
 * struct_size is private; names and layout may change in any release, and
 * the reserved tail lets that happen without changing the size. */
typedef struct moq_locmaf_decoder {
    uint32_t                   struct_size;
    moq_locmaf_track_t         _track;
    uint32_t                   _max_samples, _max_subsamples, _max_gen_boxes;
    bool                       _has_position, _position_at_end;
    bool                       _has_reference, _after_raw_boxes;
    uint64_t                   _group, _next_object;
    moq_locmaf_ref_state_t     _ref;
    uint64_t                   _bmdt;          /* the last chunk's BMDT */
    uint64_t                   _duration_sum;  /* its effective durations (12.2) */
    moq_locmaf_sample_t       *_samples;
    moq_locmaf_subsample_t    *_subsamples;
    moq_locmaf_gen_box_t      *_gen_boxes;
    moq_locmaf_decoder_stats_t _stats;
    uint64_t                   _reserved[16];  /* never read */
} moq_locmaf_decoder_t;

/* Observing: copy the decoder's counters. Writes the stats extent and stamps
 * it. Returns MOQ_OK, or MOQ_ERR_INVAL for NULL arguments or out_size below
 * MOQ_LOCMAF_DECODER_STATS_V0_SIZE (nothing written). */
MOQ_API moq_result_t moq_locmaf_decoder_get_stats(
    const moq_locmaf_decoder_t *d, moq_locmaf_decoder_stats_t *out,
    size_t out_size);

/*
 * Initialize a decoder over caller storage. The decoder keeps a pointer to
 * storage until it is re-initialized or no longer used; storage must stay
 * valid and unshared that long. It starts with no position and no reference.
 *
 * Returns MOQ_OK, or MOQ_ERR_INVAL, writing nothing, for NULL arguments,
 * d_size below sizeof(moq_locmaf_decoder_t), a cfg below its v0 size, a track
 * below MOQ_LOCMAF_TRACK_V0_SIZE or with an IV size other than 0, 8 or 16, a
 * limit above its maximum, storage not 8-byte aligned, or storage_size below
 * what moq_locmaf_decoder_storage_size() reports.
 */
MOQ_API moq_result_t moq_locmaf_decoder_init(moq_locmaf_decoder_t *d,
                                             size_t d_size,
                                             const moq_locmaf_decoder_cfg_t *cfg);

/* Forget the position and the reference: the transport reported the
 * subgroup's stream reset, or Objects were lost (section 3). NULL is a no-op.
 * Invalidates the last chunk's borrows. */
MOQ_API void moq_locmaf_decoder_reset(moq_locmaf_decoder_t *d);

/* Observing: whether a delta header at (group_id, object_id) would apply --
 * the decoder holds a reference and expects exactly that Location next.
 * False after a rawBoxes Object until a full header is decoded (section
 * 9.3), and after any reset, gap, new group or refused Object. */
MOQ_API bool moq_locmaf_decoder_expects(const moq_locmaf_decoder_t *d,
                                        uint64_t group_id, uint64_t object_id);

/*
 * Make dst continue where src stands: its position, reference and derived
 * BMDT. Used when two delivery paths carry one group (a fill and the live
 * tail): the decoder that saw the earlier Objects hands the chain on.
 * Invalidates dst's last chunk's borrows; src is unchanged.
 *
 * Returns MOQ_OK, or MOQ_ERR_INVAL, leaving dst unchanged, for NULL
 * arguments, dst == src, differing track facts, or dst limits smaller than
 * the lists src's reference holds.
 */
MOQ_API moq_result_t moq_locmaf_decoder_copy(moq_locmaf_decoder_t *dst,
                                             const moq_locmaf_decoder_t *src);

/*
 * Decode the LOCMAF Object at (group_id, object_id). Objects of one group are
 * pushed in ascending Object ID order; a different group_id starts a new
 * group (sections 3, 7.1). A gap in Object IDs within the group discards the
 * reference first (section 3), as moq_locmaf_decoder_reset() does for a
 * stream reset. Dispatch is on the header element type alone (7.1).
 *
 * Datagram delivery, subgroup identity and stream terminals are the caller's
 * to judge (section 3 binds publishers); the caller reports a loss with
 * moq_locmaf_decoder_reset().
 *
 * On MOQ_OK, *out holds the chunk (its one extent, MOQ_LOCMAF_CHUNK_V0_SIZE,
 * written and stamped). samples, subsamples, gen_boxes and ivs BORROW from the
 * decoder's storage until the next push, reset or copy into this decoder;
 * mdat_payload, raw_boxes and every genBox payload BORROW from payload.
 *
 * Results (reason, when non-NULL, is set on every return):
 *   MOQ_OK                a full or delta chunk, or a rawBoxes Object (which
 *                         also discards the reference, section 9.3);
 *   MOQ_ERR_WRONG_STATE   a delta header with no reference (joined mid-group,
 *                         or after loss): reason NO_REFERENCE. Not malformed;
 *                         the Object cannot be decoded. The position advances;
 *   MOQ_ERR_PROTO         malformed (reason says which section): the
 *                         reference is discarded and the position advances;
 *   MOQ_ERR_UNSUPPORTED   a genBox or rawBoxes box this library cannot
 *                         validate (section 18): as PROTO;
 *   MOQ_ERR_LIMIT         the chunk exceeds the configured limits: as PROTO;
 *   MOQ_ERR_INVAL         NULL d or out, out_size below MOQ_LOCMAF_CHUNK_V0_SIZE,
 *                         payload.data NULL with a nonzero length, or an
 *                         object_id below the next expected one in the same
 *                         group (a duplicate or out of order): nothing changes.
 * *out is written only on MOQ_OK.
 */
MOQ_API moq_result_t moq_locmaf_decoder_push(moq_locmaf_decoder_t *d,
                                             uint64_t group_id,
                                             uint64_t object_id,
                                             moq_bytes_t payload,
                                             moq_locmaf_chunk_t *out,
                                             size_t out_size,
                                             moq_locmaf_reason_t *reason);

/* -- Canonical CMAF (section 15) -------------------------------------------- */

/*
 * Write the canonical CMAF chunk for a decoded chunk: its genBoxes wrapped
 * in payload order, then moof and mdat in the pinned layout of sections
 * 15.2-15.8; for RAW_BOXES, the boxes verbatim (9.4). Every conformant
 * writer produces the same bytes for the same track facts and chunk. Pure.
 *
 * *out_len is always written with the chunk's length when the arguments are
 * valid. Returns MOQ_OK when out_cap holds it, MOQ_ERR_BUFFER when it does
 * not (out may be NULL with out_cap 0 to ask), MOQ_ERR_INVAL for NULL track,
 * chunk or out_len, struct sizes below their v0, or a chunk that is not
 * self-consistent (sample sizes not summing to the mdat payload, offsets or
 * subsample indexes that disagree with the table, ivs of the wrong length,
 * aux info on an unprotected track, no aux info on a protected track whose
 * per_sample_iv_size is nonzero, a value outside its box field, a chunk
 * whose moof or mdat does not fit its 32-bit size and signed data_offset,
 * or RAW_BOXES that are not one CMAF chunk of valid boxes, as
 * moq_locmaf_parse_object() requires). A chunk from
 * moq_locmaf_decoder_push() is always consistent.
 */
MOQ_API moq_result_t moq_locmaf_write_cmaf(const moq_locmaf_track_t *track,
                                           const moq_locmaf_chunk_t *chunk,
                                           uint8_t *out, size_t out_cap,
                                           size_t *out_len);

/* -- Encoder ---------------------------------------------------------------- */

typedef struct moq_locmaf_encoder_cfg {
    uint32_t                  struct_size;
    uint32_t                  max_samples;
    uint32_t                  max_subsamples;
    uint32_t                  max_gen_boxes;
    const moq_locmaf_track_t *track;        /* borrowed for the call; copied */
    void                     *storage;      /* see MOQ_LOCMAF_STORAGE_ALIGN */
    size_t                    storage_size;
} moq_locmaf_encoder_cfg_t;

/* The cfg's one capability extent (as for the decoder cfg). */
#define MOQ_LOCMAF_ENCODER_CFG_V0_SIZE \
    (offsetof(moq_locmaf_encoder_cfg_t, storage_size) + sizeof(size_t))

MOQ_API void moq_locmaf_encoder_cfg_init_sized(moq_locmaf_encoder_cfg_t *cfg,
                                               size_t cfg_size);
#define moq_locmaf_encoder_cfg_init(cfg) \
    moq_locmaf_encoder_cfg_init_sized((cfg), sizeof(*(cfg)))

/* As moq_locmaf_decoder_storage_size(), for an encoder: it keeps the
 * committed reference and a staged one. */
MOQ_API moq_result_t moq_locmaf_encoder_storage_size(
    const moq_locmaf_encoder_cfg_t *cfg, size_t *out_bytes);

/* Counters of one encoder since its init. Sized record, one extent. */
typedef struct moq_locmaf_encoder_stats {
    uint32_t struct_size;
    moq_locmaf_reason_t last_reason;    /* of the last refused chunk; 0 if none */
    uint64_t chunks_encoded;            /* staged by encode_chunk */
    uint64_t full_headers_written;
    uint64_t delta_headers_written;
    uint64_t raw_boxes_written;
    uint64_t chunks_refused;            /* PROTO, UNSUPPORTED or LIMIT, check included */
    uint64_t stages_committed;
    uint64_t stages_replaced;           /* an uncommitted stage overwritten */
} moq_locmaf_encoder_stats_t;

#define MOQ_LOCMAF_ENCODER_STATS_V0_SIZE \
    (offsetof(moq_locmaf_encoder_stats_t, stages_replaced) + sizeof(uint64_t))

/* One position-and-continuation snapshot of an encoder (private, as for
 * moq_locmaf_ref_state_t). */
typedef struct moq_locmaf_enc_position {
    bool     _has_position, _position_at_end, _has_reference, _after_raw_boxes;
    uint64_t _group, _next_object;
    uint64_t _bmdt, _duration_sum;
} moq_locmaf_enc_position_t;

/* Caller-owned encoder state, a complete type (as moq_locmaf_decoder_t). The
 * two reference slots hold the committed reference and the staged one. */
typedef struct moq_locmaf_encoder {
    uint32_t                   struct_size;
    moq_locmaf_track_t         _track;
    uint32_t                   _max_samples, _max_subsamples, _max_gen_boxes;
    uint8_t                    _committed_slot;    /* index into _ref */
    bool                       _has_stage, _stage_raw_boxes;
    moq_locmaf_enc_position_t  _committed, _staged;
    moq_locmaf_ref_state_t     _ref[2];
    moq_locmaf_sample_t       *_src_samples;       /* source scratch table */
    moq_locmaf_subsample_t    *_src_subsamples;
    moq_locmaf_gen_box_t      *_src_gen_boxes;
    uint8_t                   *_src_ivs;
    moq_locmaf_encoder_stats_t _stats;
    uint64_t                   _reserved[16];      /* never read */
} moq_locmaf_encoder_t;

/* Observing: as moq_locmaf_decoder_get_stats(), for an encoder. */
MOQ_API moq_result_t moq_locmaf_encoder_get_stats(
    const moq_locmaf_encoder_t *e, moq_locmaf_encoder_stats_t *out,
    size_t out_size);

/* As moq_locmaf_decoder_init(), for an encoder. */
MOQ_API moq_result_t moq_locmaf_encoder_init(moq_locmaf_encoder_t *e,
                                             size_t e_size,
                                             const moq_locmaf_encoder_cfg_t *cfg);

/* Forget the committed and staged position and reference: the next encode
 * writes a full header. NULL is a no-op. */
MOQ_API void moq_locmaf_encoder_reset(moq_locmaf_encoder_t *e);

/*
 * Check that a CMAF chunk can be encoded, without encoding it: one chunk
 * (genBox-able boxes, then one moof and one mdat), inside the field model of
 * section 10, consistent with the track facts and within the limits. Does
 * not disturb a staged encode.
 *
 * On MOQ_OK, *out_bound (when non-NULL) is a length no encoding of this
 * chunk exceeds at any Location, full or delta: a caller that reserves it
 * once never sees MOQ_ERR_BUFFER from moq_locmaf_encode_chunk() for the
 * chunk. Returns MOQ_OK, or MOQ_ERR_PROTO, MOQ_ERR_UNSUPPORTED or
 * MOQ_ERR_LIMIT with *reason set (see the reasons), or MOQ_ERR_INVAL for NULL
 * e or chunk.data with a nonzero length, or a bound that does not fit size_t.
 */
MOQ_API moq_result_t moq_locmaf_encoder_check(moq_locmaf_encoder_t *e,
                                              moq_bytes_t cmaf_chunk,
                                              size_t *out_bound,
                                              moq_locmaf_reason_t *reason);

/*
 * Encode one CMAF chunk as the LOCMAF Object at (group_id, object_id) and
 * stage the reference it leaves. A full header is written at a new group, a
 * gap in Object IDs, after a rawBoxes Object, with no reference, and where
 * the chunk's BMDT does not continue the previous chunk's (section 12.2);
 * otherwise a delta header. The output is canonical (section 15.9): the
 * section 11.1 fields only, IDs ascending, shortest vi64.
 *
 * Nothing becomes the reference until moq_locmaf_encoder_commit(). Encoding
 * the same chunk at the same Location again before a commit writes the same
 * bytes, so a refused send can be retried unchanged.
 *
 * *out_len is written with the Object's length on MOQ_OK and MOQ_ERR_BUFFER.
 * Returns MOQ_OK (staged), MOQ_ERR_BUFFER when out_cap is too small (out may
 * be NULL with out_cap 0; nothing staged), MOQ_ERR_PROTO / UNSUPPORTED /
 * LIMIT with *reason set (nothing staged), or MOQ_ERR_INVAL for NULL
 * arguments, or an object_id at or below the committed position in the same
 * group (nothing staged). reason may be NULL. Reads cmaf_chunk only during
 * the call.
 */
MOQ_API moq_result_t moq_locmaf_encode_chunk(moq_locmaf_encoder_t *e,
                                             uint64_t group_id,
                                             uint64_t object_id,
                                             moq_bytes_t cmaf_chunk,
                                             uint8_t *out, size_t out_cap,
                                             size_t *out_len,
                                             moq_locmaf_reason_t *reason);

/*
 * Encode complete ISO BMFF boxes verbatim as a rawBoxes Object (section 9):
 * for a chunk outside the field model, or a CMAF Header carried in band.
 * Stages a reference reset: after its commit, the next chunk of the group is
 * a full header. Results as moq_locmaf_encode_chunk(); MOQ_ERR_PROTO with
 * reason RAW_BOXES_MALFORMED for boxes that break section 9.1.
 */
MOQ_API moq_result_t moq_locmaf_encode_raw_boxes(moq_locmaf_encoder_t *e,
                                                 uint64_t group_id,
                                                 uint64_t object_id,
                                                 moq_bytes_t boxes,
                                                 uint8_t *out, size_t out_cap,
                                                 size_t *out_len,
                                                 moq_locmaf_reason_t *reason);

/* Adopt what the last successful encode staged; a later encode replaces an
 * uncommitted stage. No-op when nothing is staged or e is NULL. */
MOQ_API void moq_locmaf_encoder_commit(moq_locmaf_encoder_t *e);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_LOCMAF_H */
