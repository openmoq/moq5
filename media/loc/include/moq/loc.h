#ifndef MOQ_LOC_H
#define MOQ_LOC_H

/*
 * LOC (Low Overhead Container) metadata parse/encode helpers.
 *
 * Parses and encodes LOC metadata carried in raw object property
 * bytes for the selected LOC profile. Each profile maps typed C
 * fields to the property IDs defined by that LOC draft version.
 *
 * The LOC profile fixes the property IDs and their semantics; the
 * negotiated MoQ transport draft fixes how the property block encodes
 * integers, so both parse and encode take it (see the `draft`
 * parameter below).
 *
 * Pure helper library: no sessions, no I/O, no threads.
 * Link against moq::loc (depends only on moq::core).
 *
 * Unknown well-formed properties are silently ignored.
 */

#include <moq/types.h>
#include <moq/rcbuf.h>
#include <moq/session.h>   /* moq_version_t: the negotiated transport draft */

#ifdef __cplusplus
extern "C" {
#endif

/* -- LOC profile selection -------------------------------------------- */

typedef enum moq_loc_profile {
    MOQ_LOC_PROFILE_01 = 1,  /* draft-ietf-moq-loc-01 */
    MOQ_LOC_PROFILE_02 = 2,  /* draft-ietf-moq-loc-02; reserved, its property
                              * mapping is not implemented (ID 0x06 collides
                              * with LOC-01 Audio Level) */
} moq_loc_profile_t;

/* -- Property block encoding ------------------------------------------ *
 * A property block is a list of typed properties whose integer encoding is
 * fixed by the negotiated MoQ draft (draft-ietf-moq-transport-18 §1.4.1 and
 * §1.4.3, draft-16 §1.4.2). Draft-16 and draft-18 encode integers differently
 * and agree only for values up to 63, so a block written for one draft is
 * rejected on a session that negotiated the other. Callers pass the version
 * the session negotiated (moq_endpoint_negotiated_version); MOQ_ERR_INVAL for
 * any other value. */

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
 * Supported in LOC-01 at property ID 0x06. In LOC-02 this ID is
 * reassigned to Timestamp; Audio Level encoding under LOC-02 is
 * blocked until the draft resolves the ID collision.
 */

typedef struct moq_loc_audio_level {
    bool    voice_activity;
    uint8_t level;  /* 0-127; 0 = loudest, 127 = silence */
} moq_loc_audio_level_t;

/* -- Parsed LOC headers ----------------------------------------------- */

typedef struct moq_loc_headers {
    uint32_t struct_size;

    bool     has_timestamp;
    uint64_t timestamp;  /* LOC-01: microseconds since Unix epoch (ID 0x02) */ /* api-boundary-exempt */
                         /* LOC-02: units per timescale (ID 0x06) */

    bool     has_timescale;
    uint64_t timescale;  /* LOC-02 only (ID 0x08); units per second */

    bool     has_video_frame_marking;
    moq_loc_video_frame_marking_t video_frame_marking;

    bool     has_audio_level;
    moq_loc_audio_level_t audio_level;  /* LOC-01 only */

    bool        has_video_config;
    moq_bytes_t video_config;  /* BORROWED from input property bytes */
} moq_loc_headers_t;

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
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL if out is NULL, data is NULL with len > 0,
 *   or profile / draft is not a supported value.
 * Returns MOQ_ERR_PROTO on malformed data.
 */
MOQ_API moq_result_t moq_loc_parse(moq_loc_profile_t profile,
                                    moq_version_t draft,
                                    moq_bytes_t properties,
                                    moq_loc_headers_t *out);

/*
 * Encode LOC metadata into raw object property bytes.
 *
 * Fields are emitted in ascending property ID order. Allocates one
 * rcbuf via alloc.
 *
 * If no fields are set, returns MOQ_OK with *out_properties = NULL.
 * Returns MOQ_ERR_INVAL on invalid input (audio level > 127,
 *   temporal_id > 7, NULL alloc or out_properties, unsupported
 *   profile or draft, a field unsupported by the selected profile, or
 *   a value the draft's integer encoding cannot represent).
 * Returns MOQ_ERR_NOMEM on allocation failure.
 */
MOQ_API moq_result_t moq_loc_encode(const moq_alloc_t *alloc,
                                     moq_loc_profile_t profile,
                                     moq_version_t draft,
                                     const moq_loc_headers_t *headers,
                                     moq_rcbuf_t **out_properties);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_LOC_H */
