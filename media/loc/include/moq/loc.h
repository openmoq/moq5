#ifndef MOQ_LOC_H
#define MOQ_LOC_H

/*
 * LOC (Low Overhead Container) metadata parse/encode helpers.
 *
 * Parses and encodes LOC metadata carried in raw object property
 * bytes for the selected LOC profile. Each profile maps typed C
 * fields to the property IDs defined by that LOC draft version.
 *
 * Pure helper library: no sessions, no I/O, no threads. A
 * moq_version_t is taken by value to select the wire codec; no
 * session is touched.
 * Link against moq::loc (depends only on moq::core).
 *
 * Unknown well-formed properties are silently ignored.
 *
 * -- Transport version, LOC revision, and the KVP integer codec --------
 *
 * The caller explicitly selects the companion LOC profile, separately from
 * the transport version. Transport selects only the KVP integer encoding:
 *     MOQ_VERSION_DRAFT_16  section 1.4/1.4.2: fields marked (i) use the
 *       RFC 9000 QUIC variable-length integer (1/2/4/8 bytes, max 2^62-1);
 *     MOQ_VERSION_DRAFT_18  section 1.4.1/1.4.3 and
 *     MOQ_VERSION_DRAFT_22  section 8.3: fields marked (vi64) use MOQT's
 *       own encoding (1..9 bytes, max 2^64-1).
 *
 * The Delta Type, the odd-type Length, and the even-type value are all
 * encoded with that draft's integer. Values 0..63 encode identically in
 * both, so a codec confusion is invisible below 64 and produces wrong
 * bytes at or above it.
 *
 * Supported explicit pairs are LOC-01 with D16/18, and LOC-04 or LOC-05
 * with D22. LOC-05 is LOC-04 plus the Codec String property (0x11); its
 * other property definitions are unchanged, and its Key-Value-Pair grammar
 * is draft-22 section 8.3's. Pairing it with draft 22 claims nothing about
 * draft 23. The LOC revision is not negotiated, so the caller names it.
 * There is no default: an unsupported or unknown
 * version -- including 0 -- FAILS CLOSED with MOQ_ERR_INVAL rather than
 * guessing a codec, and so does a revision paired with a version it is not
 * registered for (LOC-01 ids on draft 22, LOC-04 or LOC-05 ids on draft 16
 * or 18). A future draft becomes usable here only when its row is
 * deliberately registered in the table.
 */

#include <stddef.h>
#include <moq/types.h>
#include <moq/rcbuf.h>
#include <moq/session.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- LOC profile selection -------------------------------------------- */

typedef enum moq_loc_profile {
    MOQ_LOC_PROFILE_01 = 1,  /* draft-ietf-moq-loc-01: drafts 16 and 18 */
    MOQ_LOC_PROFILE_02 = 2,  /* draft-ietf-moq-loc-02; reserved, never selected */
    MOQ_LOC_PROFILE_04 = 4,  /* draft-ietf-moq-loc-04: draft 22 */
    MOQ_LOC_PROFILE_05 = 5,  /* draft-ietf-moq-loc-05: draft 22 */
} moq_loc_profile_t;

/*
 * Compatibility selector retained pending migration of existing callers:
 * MOQ_LOC_PROFILE_01 for
 * drafts 16 and 18, MOQ_LOC_PROFILE_04 for draft 22, and 0 for a version
 * the table does not register (including 0). This is the single place
 * the compatibility mapping lives. This is not companion opt-in or proof that
 * an unconverted media/service caller supports D22. It is not the latest
 * revision for a draft either: LOC-05 is selected only by naming it. Pure
 * function.
 */
MOQ_API moq_loc_profile_t moq_loc_profile_for_version(
    moq_version_t transport_version);

/*
 * The largest integer property value (timestamp, timescale) that the
 * integer encoding of transport_version can carry: UINT64_MAX on drafts
 * 18 and 22 (vi64), 2^62-1 on draft 16, and 0 for a version the table
 * does not register (including 0). Read from the same table
 * moq_loc_encode_metadata uses for a supported explicit pair. The legacy
 * encoder remains LOC01+D16/18 only. Pure function, no session touched.
 */
MOQ_API uint64_t moq_loc_value_max_for_version(
    moq_version_t transport_version);

/* -- Video Frame Marking (RFC 9626 §3.1) ------------------------------ */

typedef struct moq_loc_video_frame_marking {
    bool    start_of_frame;
    bool    end_of_frame;
    bool    independent;
    bool    discardable;
    bool    base_layer_sync;
    uint8_t temporal_id;  /* 0-7 */
    bool    has_layer_id;
    uint8_t layer_id;     /* 0-255; valid when has_layer_id */
} moq_loc_video_frame_marking_t;

/* -- Audio Level (RFC 6464 §3) ---------------------------------------- */
/*
 * LOC-01 property ID 0x06; LOC-04 property ID 0x0C. Both carry the RFC
 * 6464 byte (V bit + 7-bit level) in the low 8 bits of an even-type
 * integer value.
 */

typedef struct moq_loc_audio_level {
    bool    voice_activity;
    uint8_t level;  /* 0-127; 0 = loudest, 127 = silence */
} moq_loc_audio_level_t;

/* -- Parsed LOC headers ----------------------------------------------- */

typedef struct moq_loc_headers {
    uint32_t struct_size;

    bool     has_timestamp;
    uint64_t timestamp;  /* LOC-01 ID 0x02: microseconds since the Unix epoch. */ /* api-boundary-exempt */

    bool     has_timescale;
    uint64_t timescale;  /* Reserved legacy field; LOC-01 encode refuses it. */

    bool     has_video_frame_marking;
    moq_loc_video_frame_marking_t video_frame_marking; /* LOC-01 0x04 integer */

    bool     has_audio_level;
    moq_loc_audio_level_t audio_level;  /* LOC-01 0x06 */

    bool        has_video_config;
    moq_bytes_t video_config;  /* LOC-01 ID 0x0D; BORROWED from
                                * the input property bytes */

} moq_loc_headers_t;

/* Complete frozen legacy layout. New metadata never grows this record. */
#define MOQ_LOC_HEADERS_V0_SIZE \
    (offsetof(moq_loc_headers_t, video_config) + sizeof(moq_bytes_t))

/* Clears/stamps the complete frozen legacy record. */
MOQ_API void moq_loc_headers_init(moq_loc_headers_t *h);

/*
 * Parse LOC metadata from raw object property bytes.
 *
 * Empty input parses successfully with no fields set.
 * Unknown well-formed properties are ignored.
 * Duplicate known properties: last value wins.
 *
 * video_config.data is BORROWED from the input property bytes.
 *
 * Legacy LOC-01 on D16/18 only. New D22 callers use parse_metadata below.
 * The Key-Value-Pair integer codec is selected by transport_version.
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL if out is NULL, data is NULL with len > 0,
 *   transport_version is not D16/18, or profile is not LOC-01. The version is
 *   checked even for an EMPTY property block: a caller that cannot name
 *   its codec is refused whether or not there is anything to decode.
 * Returns MOQ_ERR_PROTO on malformed data.
 */
MOQ_API moq_result_t moq_loc_parse(moq_version_t transport_version,
                                    moq_loc_profile_t profile,
                                    moq_bytes_t properties,
                                    moq_loc_headers_t *out);

/*
 * Encode LOC metadata into raw object property bytes.
 *
 * Fields are emitted in ascending property ID order. Allocates one
 * rcbuf via alloc.
 *
 * Legacy LOC-01 on D16/18 only. New D22 callers use encode_metadata below.
 * The Key-Value-Pair integer codec is selected by transport_version.
 * A value the selected codec cannot represent -- a timestamp beyond
 * what the draft-16 integer encoding reaches, say -- is MOQ_ERR_INVAL,
 * not a truncation.
 *
 * If no fields are set, returns MOQ_OK with *out_properties = NULL.
 * Returns MOQ_ERR_INVAL on invalid input (audio level > 127,
 *   temporal_id > 7, NULL alloc or out_properties, an unregistered
 *   transport_version, a profile other than the one the table selects
 *   for it, a value the selected codec cannot represent, or a field the
 *   selected revision has no id for: timescale on LOC-01).
 *   The version is checked BEFORE the field set, so a caller
 *   with nothing to encode is still refused an unknown codec.
 * Returns MOQ_ERR_NOMEM on allocation failure.
 */
MOQ_API moq_result_t moq_loc_encode(const moq_alloc_t *alloc,
                                     moq_version_t transport_version,
                                     moq_loc_profile_t profile,
                                     const moq_loc_headers_t *headers,
                                     moq_rcbuf_t **out_properties);

/* -- Lossless, explicitly selected companion metadata ---------------- */

#define MOQ_LOC_METADATA_HAS_TIMESTAMP    UINT32_C(0x01)
#define MOQ_LOC_METADATA_HAS_TIMESCALE    UINT32_C(0x02)
#define MOQ_LOC_METADATA_HAS_MARKING      UINT32_C(0x04)
#define MOQ_LOC_METADATA_HAS_AUDIO_LEVEL  UINT32_C(0x08)
#define MOQ_LOC_METADATA_HAS_VIDEO_CONFIG UINT32_C(0x10)
#define MOQ_LOC_METADATA_HAS_AUDIO_CONFIG UINT32_C(0x20)
#define MOQ_LOC_METADATA_PRESENT_MASK     UINT32_C(0x3f)

/* Complete and frozen: embedded by value in moq_media_metadata_t, so it never
 * grows. Properties registered after LOC-04 are reported in moq_loc_ext_t. */
typedef struct moq_loc_metadata {
    uint32_t struct_size;
    uint32_t present;
    uint64_t timestamp;       /* Raw ticks; no clock conversion. */
    uint64_t timescale;       /* Raw units/second, including zero. */
    uint8_t marking_len;
    uint8_t marking[4];       /* Exact marking payload; no normalization. */
    uint8_t audio_level;      /* Raw V bit and seven-bit level. */
    uint8_t reserved[2];      /* Must be zero on encode. */
    moq_bytes_t video_config;
    moq_bytes_t audio_config;
} moq_loc_metadata_t;

/* Full frozen v0 extent; includes BOTH complete borrowed spans. */
#define MOQ_LOC_METADATA_V0_SIZE \
    (offsetof(moq_loc_metadata_t, audio_config) + sizeof(moq_bytes_t))

/* Clears/stamps min(size, library size); size < 4 writes nothing. */
MOQ_API void moq_loc_metadata_init_sized(moq_loc_metadata_t *out, size_t size);

/* Explicit LOC01+D16/18, or LOC04 or LOC05 with D22; unknown/mismatched pairs
 * are INVAL. Exactly moq_loc_parse_metadata_ex with ext NULL: on LOC05 the
 * Codec String is still a known property, validated (and a duplicate refused)
 * although this call does not report it.
 * No input output-stamp is required. Size below the full v0 floor or invalid
 * arguments => INVAL. Malformed KVP/known values => PROTO. A known field
 * repeated in one block (outer or inner immutable lists) => UNSUPPORTED,
 * even for equal values. Unknown well-formed fields are skipped.
 * Success alone copies/stamps min(out_size, library size); any failure leaves
 * the entire caller output untouched. Configs borrow ORIGINAL property bytes,
 * including immutable inner offsets; their presence may have length zero.
 * The caller retains that backing for as long as either span is used.
 * LOC01 integer marking is represented as minimal big-endian payload bytes;
 * LOC04 preserves all 1..4 bytes verbatim. No owned resource or cleanup. */
MOQ_API moq_result_t moq_loc_parse_metadata(moq_version_t transport_version,
    moq_loc_profile_t profile, moq_bytes_t properties,
    moq_loc_metadata_t *out, size_t out_size);

/* Exactly moq_loc_encode_metadata_ex with ext NULL.
 * Requires input struct_size >= full v0; reads only the covered library prefix.
 * Unknown present bits, reserved bytes, invalid spans/marking lengths or an
 * unrepresentable integer => INVAL, before allocation. LOC01 timescale/audio
 * config, or multi-byte marking with an unrepresentable leading zero, =>
 * UNSUPPORTED (no lossy fallback). Raw timestamp/scale zero/MAX remain metadata,
 * not a usable service-clock promise. Emits sorted mutable keys into one owned
 * rcbuf; does not concatenate/rewrite raw immutable blocks. On success only,
 * publishes *out_properties (NULL for no fields); NOMEM or any other refusal
 * leaves it untouched. Configs are borrowed for this call only. */
MOQ_API moq_result_t moq_loc_encode_metadata(const moq_alloc_t *alloc,
    moq_version_t transport_version, moq_loc_profile_t profile,
    const moq_loc_metadata_t *in, moq_rcbuf_t **out_properties);

/* -- Properties beyond the frozen metadata record --------------------- */

#define MOQ_LOC_EXT_HAS_CODEC_STRING UINT32_C(0x01)
#define MOQ_LOC_EXT_PRESENT_MASK     UINT32_C(0x01)

/*
 * LOC properties registered after LOC-04. Never embedded by value in another
 * public record, so it grows by appending; every field is read or written only
 * when struct_size covers all of it.
 *
 * codec_string: the LOC-05 Codec String (draft-ietf-moq-loc-05 2.3.1.3), the
 * WebCodecs codec string of the track or object. Exact bytes, BORROWED, not
 * NUL-terminated, never normalized or trimmed. A present value is well-formed
 * UTF-8 (RFC 3629) and a usable identifier: not empty, not only ASCII
 * whitespace (U+0009, U+000A, U+000C, U+000D, U+0020), and free of U+0000.
 * Nothing else is checked: an identifier the library accepts is not a claim
 * that any decoder recognizes or supports it, and there is no codec list.
 */
typedef struct moq_loc_ext {
    uint32_t    struct_size;
    uint32_t    present;        /* MOQ_LOC_EXT_HAS_* */
    moq_bytes_t codec_string;
} moq_loc_ext_t;

/* Complete first-published extent, including codec_string. Future fields
 * never raise this floor. */
#define MOQ_LOC_EXT_V0_SIZE \
    (offsetof(moq_loc_ext_t, codec_string) + sizeof(moq_bytes_t))

/* Clears/stamps min(size, library size); NULL or size < 4 writes nothing. */
MOQ_API void moq_loc_ext_init_sized(moq_loc_ext_t *out, size_t size);

/*
 * moq_loc_parse_metadata, also reporting the properties moq_loc_ext_t holds.
 *
 * One bounded walk validates the whole block for the selected revision, as
 * moq_loc_parse_metadata does. On LOC05 the Codec String (0x11) is known in
 * both the mutable list and the Immutable Properties list: malformed UTF-8 or
 * an unusable identifier is PROTO, and a second occurrence anywhere in the
 * block is UNSUPPORTED. On LOC01/04 0x11 is an unknown property and is
 * skipped, as before; ext then reports nothing.
 *
 * ext NULL omits that report and ext_size is not read; validation is the same.
 * A non-NULL ext needs ext_size >= MOQ_LOC_EXT_V0_SIZE and must not overlap the
 * bytes out receives; otherwise INVAL. On success only, out receives
 * min(out_size, library size) and ext min(ext_size, library size), each
 * stamped; any failure leaves both untouched. codec_string borrows the ORIGINAL
 * property bytes, including an immutable inner offset; the caller keeps that
 * backing for as long as the span is used. No allocation or cleanup.
 */
MOQ_API moq_result_t moq_loc_parse_metadata_ex(moq_version_t transport_version,
    moq_loc_profile_t profile, moq_bytes_t properties,
    moq_loc_metadata_t *out, size_t out_size,
    moq_loc_ext_t *ext, size_t ext_size);

/*
 * moq_loc_encode_metadata, also emitting the properties moq_loc_ext_t holds.
 *
 * ext NULL is exactly moq_loc_encode_metadata. A non-NULL ext needs
 * struct_size >= MOQ_LOC_EXT_V0_SIZE and only known present bits, or INVAL. A
 * present Codec String on LOC01 or LOC04 is UNSUPPORTED: it is never dropped.
 * On LOC05 it must be a usable identifier as moq_loc_ext_t describes, at most
 * 65535 bytes, with a coherent span; otherwise INVAL. Every refusal happens
 * before allocation and leaves *out_properties untouched. The Codec String
 * follows the moq_loc_metadata_t keys (0x11 is the highest LOC id), with its
 * bytes copied exactly; spans are borrowed for this call only.
 */
MOQ_API moq_result_t moq_loc_encode_metadata_ex(const moq_alloc_t *alloc,
    moq_version_t transport_version, moq_loc_profile_t profile,
    const moq_loc_metadata_t *in, const moq_loc_ext_t *ext,
    moq_rcbuf_t **out_properties);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_LOC_H */
