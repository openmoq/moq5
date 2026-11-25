#ifndef MOQ_CMAF_H
#define MOQ_CMAF_H

/*
 * CMAF (Common Media Application Format) segment parser.
 *
 * Parse-only: extracts codec configuration from init segments
 * (ftyp+moov) and sample tables from media fragments (moof+mdat).
 * No muxer, no decoder, no session integration.
 *
 * All string/byte fields in parsed output borrow from the input
 * buffer. The input must remain valid while the output is in use.
 * Parsing never allocates.
 *
 * Link against moq::cmaf (depends only on moq::core).
 */

#include <moq/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Codec kind ------------------------------------------------------ */

typedef enum moq_cmaf_codec_kind {
    MOQ_CMAF_CODEC_UNKNOWN = 0,
    MOQ_CMAF_CODEC_AVC     = 1,
    MOQ_CMAF_CODEC_HEVC    = 2,
    MOQ_CMAF_CODEC_AV1     = 3,
    MOQ_CMAF_CODEC_AAC     = 4,
    MOQ_CMAF_CODEC_OPUS    = 5,
} moq_cmaf_codec_kind_t;

/* -- Init segment info ----------------------------------------------- */

/*
 * The record parsed from a CMAF header (ftyp + moov): its first trak. A sized
 * output record: fields are appended at the tail, and a reader uses a field
 * only when the stamped struct_size covers all of it. The same record is the
 * standalone parser's output and the receiver's handle-owned observation
 * (moq_media_track_desc_t.cmaf).
 */
typedef struct moq_cmaf_init_info {
    uint32_t              struct_size;
    moq_cmaf_codec_kind_t codec_kind;
    uint32_t              timescale;
    uint32_t              width;
    uint32_t              height;
    uint32_t              samplerate;
    uint32_t              channel_count;
    moq_bytes_t           codec_config; /* BORROWED from input */
    uint32_t              track_id;     /* init trak tkhd.track_ID (0 if absent) */

    /* CMSF §4.2 / CENC [ISO 23001-7]: protection parameters read from an
     * encrypted sample entry (encv/enca) in the init segment -- the sinf box's
     * schm (scheme) and schi.tenc (track encryption). has_cenc is false for a
     * clear (unencrypted) track, and the four fields below are then zeroed. The
     * original codec is recovered from the sinf frma box, so codec_kind and
     * codec_config stay populated for protected tracks. libmoq surfaces these
     * for license acquisition only -- it performs no decryption. */
    bool                  has_cenc;
    moq_bytes_t           scheme;       /* schm scheme_type 4cc ("cenc"/"cbcs"),
                                           BORROWED; len 4 when has_cenc */
    uint8_t               default_is_protected;       /* tenc default_isProtected */
    uint8_t               default_per_sample_iv_size;  /* tenc default_Per_Sample_IV_Size */
    moq_bytes_t           default_kid;  /* tenc default_KID, 16 bytes, BORROWED */

    /* Appended after default_kid. The sample entry's format as its four
     * bytes read big-endian, (a<<24)|(b<<16)|(c<<8)|d: the entry type of a
     * clear entry ('avc1', 'avc3', 'hvc1', 'hev1', ...), or the sinf frma
     * original format of an encrypted one ('encv'/'enca' are never reported).
     * A value the parser does not recognize, including zero, is reported
     * exactly as read: whether it was reported is the stamp's coverage, never
     * the value. */
    uint32_t              sample_entry;

    /* Covers the tail padding after sample_entry on targets that have it.
     * Never written with meaning and never read. */
    uint32_t              _reserved_sample_entry;

    /* Appended after sample_entry, as one block: read any of these only when
     * struct_size covers the whole block, through default_constant_iv
     * (MOQ_CMAF_INIT_INFO_SIZE_THROUGH_CONSTANT_IV). */

    /* trak boxes in moov. A CMAF header has exactly one; 0 when moov's
     * children do not parse to its end (counted, never a parse failure). */
    uint32_t              trak_count;
    /* ISO/IEC 14496-12 8.8.3 TrackExtendsBox: the mvex/trex whose track_ID is
     * track_id. has_trex is false (and the four values zero) when moov has no
     * mvex, no matching trex, track_id is 0, or the trex is shorter than its
     * 24-byte body. */
    bool                  has_trex;
    uint32_t              trex_default_sample_description_index;
    uint32_t              trex_default_sample_duration;   /* timescale units */
    uint32_t              trex_default_sample_size;       /* bytes */
    uint32_t              trex_default_sample_flags;      /* sample_flags */
    /* CENC (ISO/IEC 23001-7 8.2) TrackEncryptionBox beyond the v0 fields,
     * zero unless has_cenc: the tenc version (0 or 1); the version-1 pattern
     * (default_crypt_byte_block, default_skip_byte_block, zero for version 0);
     * and the default constant IV, present (8 or 16 bytes, BORROWED) only when
     * default_isProtected is 1, default_Per_Sample_IV_Size is 0 and the box
     * carries a well-formed constant IV; otherwise empty. */
    uint8_t               tenc_version;
    uint8_t               default_crypt_byte_block;
    uint8_t               default_skip_byte_block;
    moq_bytes_t           default_constant_iv;
} moq_cmaf_init_info_t;

/* Frozen v0 prefix: everything through default_kid. A caller's record must
 * hold at least this much; fields past it are written only when the
 * caller's size covers them. */
#define MOQ_CMAF_INIT_INFO_V0_SIZE \
    (offsetof(moq_cmaf_init_info_t, default_kid) + sizeof(moq_bytes_t))

/* Capability extents: the only sizes the parser stamps. A reader may rely on
 * a field only when struct_size covers its extent. */
#define MOQ_CMAF_INIT_INFO_SIZE_THROUGH_SAMPLE_ENTRY \
    (offsetof(moq_cmaf_init_info_t, sample_entry) + sizeof(uint32_t))
#define MOQ_CMAF_INIT_INFO_SIZE_THROUGH_RESERVED \
    (offsetof(moq_cmaf_init_info_t, _reserved_sample_entry) + sizeof(uint32_t))
/* The record through default_constant_iv: what a reader of the header facts
 * block above must see covered. */
#define MOQ_CMAF_INIT_INFO_SIZE_THROUGH_CONSTANT_IV \
    (offsetof(moq_cmaf_init_info_t, default_constant_iv) + sizeof(moq_bytes_t))

/*
 * Clear and stamp min(info_size, sizeof(current)) bytes. Writes nothing when
 * info_size cannot hold struct_size. The macro passes the caller's own size.
 */
MOQ_API void moq_cmaf_init_info_init_sized(moq_cmaf_init_info_t *info,
                                           size_t info_size);
#define moq_cmaf_init_info_init(info) \
    moq_cmaf_init_info_init_sized((info), sizeof(*(info)))

/*
 * Parse the first trak from a CMAF init segment into the caller's record of
 * out_size bytes (sizeof the caller's record).
 *
 * On MOQ_OK, the largest capability extent that out_size holds is written
 * and stamped in out->struct_size: the v0 prefix, then through sample_entry,
 * through _reserved_sample_entry, through default_constant_iv, and the whole
 * current record. Bytes past the stamp are never touched, so a size that
 * cuts a field gets the extent before it. Only the first sample entry of the
 * first trak is read; trak_count counts every trak in moov.
 *
 * Returns MOQ_ERR_INVAL, writing nothing, if init_segment.data or out is
 * NULL, or out_size is below MOQ_CMAF_INIT_INFO_V0_SIZE.
 * Returns MOQ_ERR_PROTO on malformed boxes or missing required data; the
 * record is then left as moq_cmaf_init_info_init_sized(out, out_size) leaves
 * it, carrying no partial observation.
 */
MOQ_API moq_result_t moq_cmaf_parse_init_sized(moq_bytes_t init_segment,
                                               moq_cmaf_init_info_t *out,
                                               size_t out_size);

/* -- Stream Access Point (SAP) type ---------------------------------- */

/*
 * Values 0..3 are CMSF/ISO-BMFF spec-aligned (CMSF §3.6: 0 = object does not
 * start with a SAP; 1/2/3 = SAP type 1/2/3), so emitting the §3.6 SAP-type
 * timeline is a direct value map. UNKNOWN is a DISTINCT sentinel meaning "could
 * not classify the exact type" -- NOT the same as NONE ("known to not be a
 * SAP"). Exact type-1-vs-2 and open-GOP type-3 detection needs codec bitstream
 * parsing (HEVC CRA/BLA, AVC recovery-point SEI); from ISO-BMFF sample flags
 * alone a sync sample is reported UNKNOWN (it IS a closed-GOP SAP of type 1 or
 * 2, but which is indeterminate).
 */
typedef enum moq_sap_type {
    MOQ_SAP_NONE    = 0,
    MOQ_SAP_TYPE_1  = 1,
    MOQ_SAP_TYPE_2  = 2,
    MOQ_SAP_TYPE_3  = 3,
    MOQ_SAP_UNKNOWN = 0xFF,
} moq_sap_type_t;

/*
 * Classify a sample's SAP type from its ISO-BMFF sample_flags
 * (sample_is_non_sync_sample bit 0x00010000, sample_depends_on bits 24-25),
 * with no codec parsing:
 *   - non-sync sample that depends on others  -> NONE  (P/B frame)
 *   - sync sample                             -> UNKNOWN (closed-GOP SAP 1/2)
 *   - non-sync but independent                -> UNKNOWN (possible open-GOP SAP-3)
 * Never fabricates an exact 1/2/3 from flags alone.
 */
MOQ_API moq_sap_type_t moq_cmaf_sap_from_sample_flags(uint32_t sample_flags);

/* -- Fragment info --------------------------------------------------- */

typedef struct moq_cmaf_sample {
    uint32_t duration;
    uint32_t size;
    uint32_t flags;
    int32_t  composition_offset;
} moq_cmaf_sample_t;

typedef struct moq_cmaf_fragment_info {
    uint32_t            struct_size;
    bool                has_base_decode_time;
    uint64_t            base_decode_time;
    uint32_t            default_sample_duration;
    uint32_t            default_sample_size;
    uint32_t            default_sample_flags;
    moq_cmaf_sample_t  *samples;
    size_t              sample_count;
    size_t              sample_cap;
    moq_bytes_t         mdat;  /* BORROWED from input: FIRST chunk's mdat */
    uint32_t            track_id;    /* tfhd.track_ID of the first chunk (0 if none) */
    uint32_t            chunk_count; /* number of CMAF chunks (moof boxes) in the object */
} moq_cmaf_fragment_info_t;

/*
 * Initialize a fragment_info with a caller-provided sample buffer.
 */
MOQ_API void moq_cmaf_fragment_info_init(moq_cmaf_fragment_info_t *info,
                                          moq_cmaf_sample_t *samples,
                                          size_t sample_cap);

/*
 * Parse a CMAF media fragment (moof+mdat).
 *
 * When the object holds multiple CMAF chunks (successive moof+mdat pairs,
 * CMSF §3.3), the sample table, base decode time, track_id, and mdat are taken
 * from the FIRST chunk; out->chunk_count reports how many chunks the object
 * contains so callers can fall back to the full object bytes when they need the
 * later chunks (a per-chunk mdat-range table is future API).
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL if fragment.data or out is NULL.
 * A trun whose sample_count is 0 (an event-only chunk) is valid: MOQ_OK with
 *   sample_count 0; the mdat is still required and may be empty.
 * Returns MOQ_ERR_PROTO on malformed boxes, missing trun, or a structurally
 *   invalid sample table -- including a trun that declares more samples than
 *   its per-sample records (or the fragment) can hold. A
 *   malformed fragment NEVER surfaces a required count: out->sample_count
 *   stays 0 on PROTO, so an attacker cannot drive an oversized allocation via
 *   the resize-and-retry contract below.
 * Returns MOQ_ERR_BUFFER ONLY for a structurally valid fragment whose sample
 *   count exceeds sample_cap; out->sample_count is then the (trusted, fragment-
 *   bounded) required count. The BUFFER signal is withheld until the whole
 *   fragment has been validated (moof->mdat ordering, mdat presence, no
 *   trailing bytes), so it always means "valid fragment, grow the buffer".
 */
MOQ_API moq_result_t moq_cmaf_parse_fragment(moq_bytes_t fragment,
                                              moq_cmaf_fragment_info_t *out);

/* -- Object packaging validation (CMSF §3.3) ------------------------- */

typedef enum moq_cmaf_validity {
    MOQ_CMAF_OK                 = 0,
    MOQ_CMAF_ERR_NO_CHUNK       = 1, /* no complete moof+mdat chunk */
    MOQ_CMAF_ERR_MISSING_MFHD   = 2, /* a moof lacks the mandatory mfhd */
    MOQ_CMAF_ERR_MULTI_TRACK    = 3, /* >1 traf in a moof, or track_ID varies */
    MOQ_CMAF_ERR_TRACK_ID_MISMATCH = 4, /* track_ID != init track_id */
    MOQ_CMAF_ERR_MALFORMED      = 5, /* broken structure or out-of-order boxes */
    MOQ_CMAF_ERR_NO_SAMPLES     = 6, /* a moof lacks trun, or its sample table did
                                      * not fully parse/validate (incl. a trun
                                      * declaring more samples than the validator
                                      * can hold) */
} moq_cmaf_validity_t;

typedef struct moq_cmaf_object_report {
    uint32_t            struct_size;
    uint32_t            chunk_count;      /* CMAF chunks (moof boxes) seen */
    uint32_t            track_id;         /* track_ID from the first tfhd (0 if none) */
    bool                starts_with_sync; /* first sample of first chunk is a sync sample */
    moq_sap_type_t      sap_type;         /* SAP type of the object's first sample */
    bool                valid;            /* passes CMSF §3.3 object packaging */
    moq_cmaf_validity_t reason;           /* first failure (MOQ_CMAF_OK when valid) */
} moq_cmaf_object_report_t;

MOQ_API void moq_cmaf_object_report_init(moq_cmaf_object_report_t *r);

/*
 * Validate one MoQ object payload against CMSF §3.3 object packaging:
 *   - the object is one or more successive, ordered CMAF chunks, each a moof
 *     immediately followed by an mdat (no other or out-of-order top-level
 *     boxes -- e.g. moof/moof/mdat or mdat/moof are rejected);
 *   - each moof has the mandatory mfhd;
 *   - a single track (one traf per moof, consistent tfhd.track_ID);
 *   - each moof carries a trun, and EVERY completed chunk's sample table fully
 *     parses to >= 1 sample. A trun declaring more samples than the validator's
 *     internal cap (512) cannot be fully parsed/validated and is rejected
 *     (MOQ_CMAF_ERR_NO_SAMPLES), not accepted on an incomplete parse;
 *   - when init is non-NULL with a known track_id, the object's track_ID
 *     matches it. Only track_id, inside the v0 prefix, is read from init.
 * The report is filled best-effort in all cases (chunk_count/track_id/SAP).
 *
 * Returns MOQ_OK when the object is valid (out->valid = true).
 * Returns MOQ_ERR_PROTO when it violates §3.3 (out->valid = false,
 *   out->reason set to the first failure).
 * Returns MOQ_ERR_INVAL if object.data or out is NULL.
 * Pure: no allocation, no MSF, no retained refs.
 */
MOQ_API moq_result_t moq_cmaf_validate_object(const moq_cmaf_init_info_t *init,
                                              moq_bytes_t object,
                                              moq_cmaf_object_report_t *out);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_CMAF_H */
