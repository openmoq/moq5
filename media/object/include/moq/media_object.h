#ifndef MOQ_MEDIA_OBJECT_H
#define MOQ_MEDIA_OBJECT_H

/*
 * Stateless MoQ media object normalizer.
 *
 * Parses one MoQ media object into timing, keyframe, and payload
 * metadata. Handles RAW (LOC-01 properties) and CMAF (fragment
 * parsing) packaging. No state, no heap allocation, no retained
 * refs — output spans borrow from input rcbufs.
 *
 * Usable standalone by downstream integrations without requiring
 * moq_playback_t. Link against moq::media-object (depends on
 * moq::core, moq::loc, moq::cmaf).
 */

#include <moq/types.h>
#include <moq/session.h>
#include <moq/rcbuf.h>
#include <moq/cmaf.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Enums ----------------------------------------------------------- */

typedef enum moq_media_packaging {
    MOQ_MEDIA_PACKAGING_RAW  = 1,
    MOQ_MEDIA_PACKAGING_CMAF = 2,
} moq_media_packaging_t;

typedef enum moq_media_drop_reason {
    MOQ_MEDIA_DROP_MALFORMED_LOC            = 1,
    MOQ_MEDIA_DROP_MALFORMED_CMAF           = 2,
    MOQ_MEDIA_DROP_MISSING_TIMESTAMP        = 3,
    MOQ_MEDIA_DROP_NON_MONOTONIC_DTS        = 4,
    MOQ_MEDIA_DROP_UNSUPPORTED_MULTI_SAMPLE = 5,
    MOQ_MEDIA_DROP_STALE                    = 6,
    MOQ_MEDIA_DROP_KEYFRAME_WAIT            = 7,
} moq_media_drop_reason_t;

typedef enum moq_media_type {
    MOQ_MEDIA_TYPE_VIDEO = 1,
    MOQ_MEDIA_TYPE_AUDIO = 2,
} moq_media_type_t;

/* -- Track info (caller-supplied context) ----------------------------- */

typedef struct moq_media_track_info {
    uint32_t             struct_size;
    moq_media_type_t     media_type;
    moq_media_packaging_t packaging;
    uint32_t             timescale;
} moq_media_track_info_t;

MOQ_API void moq_media_track_info_init(moq_media_track_info_t *info);

/* -- Object input ---------------------------------------------------- */

typedef struct moq_media_object_input {
    uint32_t            struct_size;
    uint64_t            group_id;
    uint64_t            object_id;
    moq_object_status_t status;
    bool                end_of_group;
    bool                datagram;
    moq_rcbuf_t        *payload;      /* borrowed */
    moq_rcbuf_t        *properties;   /* borrowed */
} moq_media_object_input_t;

MOQ_API void moq_media_object_input_init(moq_media_object_input_t *in);

/* -- Parsed output --------------------------------------------------- */

typedef struct moq_media_parsed_object {
    uint32_t              struct_size;
    moq_media_packaging_t packaging;
    moq_object_status_t   status;
    bool                  end_of_group;
    bool                  datagram;

    bool                  has_capture_time;
    uint64_t              capture_time_us;

    uint64_t              decode_time_us;
    int64_t               composition_offset_us;
    uint64_t              presentation_time_us;

    bool                  keyframe;

    moq_bytes_t           payload;     /* RAW: borrowed payload bytes */
    moq_bytes_t           fragment;    /* CMAF: borrowed full fragment */
    size_t                mdat_offset;
    size_t                mdat_len;

    size_t                      sample_count;
    const moq_cmaf_sample_t    *samples;
    uint32_t                    sample_duration_us;

    /* CMAF object structure (RAW: sap_type = UNKNOWN, others 0). sap_type is
     * derived from the first sample's flags -- NONE / UNKNOWN only in this
     * version; exact TYPE_1/2/3 awaits codec-bitstream classification. For a
     * multi-chunk object, mdat_offset/mdat_len cover the FIRST chunk while
     * `fragment` holds all chunks; chunk_count > 1 flags the fallback. */
    moq_sap_type_t              sap_type;
    uint32_t                    chunk_count;  /* CMAF chunks in the object */
    uint32_t                    track_id;     /* tfhd.track_ID (0 if none) */
} moq_media_parsed_object_t;

MOQ_API void moq_media_parsed_object_init(moq_media_parsed_object_t *out);

/* -- Parse ----------------------------------------------------------- */

/*
 * Parse and normalize one MoQ media object.
 *
 * Stateless, allocation-free, no retained refs.
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL on bad struct_size, NULL required pointers,
 *   invalid packaging/media_type, terminal with payload, normal
 *   without payload, or CMAF with zero timescale.
 * Returns MOQ_ERR_PROTO on media-level parse failure; if drop_reason
 *   is non-NULL, it is set to the specific reason.
 * Returns MOQ_ERR_BUFFER if the CMAF fragment has more samples than
 *   sample_cap; out->sample_count is set to the required count.
 *
 * Terminal statuses (END_OF_GROUP, END_OF_TRACK) return MOQ_OK with
 * status set and no timing/payload required.
 *
 * Capture timestamp is OPTIONAL (draft-ietf-moq-loc-01): a RAW/LOC object with
 * no LOC capture timestamp is NOT a parse failure -- it returns MOQ_OK with
 * has_capture_time=false and the timing fields (decode/composition/presentation)
 * left at 0. (It is therefore never dropped with MOQ_MEDIA_DROP_MISSING_TIMESTAMP
 * at this layer; a consumer that requires a presentation time enforces that
 * itself via has_capture_time -- e.g. the playback pipeline drops such objects.)
 * When the timestamp is present it drives decode/presentation time as usual.
 */
MOQ_API moq_result_t moq_media_object_parse(
    const moq_media_track_info_t *track,
    const moq_media_object_input_t *in,
    moq_cmaf_sample_t *samples,
    size_t sample_cap,
    moq_media_parsed_object_t *out,
    moq_media_drop_reason_t *drop_reason);

/* -- Subscriber input helper ----------------------------------------- */

/*
 * Populate a media object input from a subscriber object.
 *
 * For integrations using moq_sub_poll_object(). Copies field values
 * only — payload and properties remain borrowed from the source and
 * are valid only as long as the source object has not been cleaned up.
 * No allocation, no rcbuf incref, no cleanup needed.
 */
struct moq_sub_object;
MOQ_API moq_result_t moq_media_object_input_from_sub_object(
    const struct moq_sub_object *src,
    moq_media_object_input_t *dst);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MEDIA_OBJECT_H */
