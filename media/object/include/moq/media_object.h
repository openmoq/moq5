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

#include <stddef.h>

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

/*
 * The FROZEN v0 PREFIX runs from struct_size through `timescale`. Those
 * four fields and their order will not change; anything appended after
 * `timescale` is a tail field that only a caller whose struct_size
 * covers it may be assumed to own.
 *
 * `transport_version` is the first such tail field. It names the
 * negotiated MoQ transport draft, which selects the integer codec the
 * object's LOC Key-Value-Pair properties are written in (see <moq/loc.h>).
 */
typedef struct moq_media_track_info {
    /* -- frozen v0 prefix ------------------------------------------- */
    uint32_t             struct_size;
    moq_media_type_t     media_type;
    moq_media_packaging_t packaging;
    uint32_t             timescale;
    /* -- appended after v0 ------------------------------------------ */
    moq_version_t        transport_version;
} moq_media_track_info_t;

/* Size of the frozen v0 prefix: struct_size .. timescale inclusive. A
 * struct_size equal to this is an OLD caller that does not own
 * transport_version. */
#define MOQ_MEDIA_TRACK_INFO_V0_SIZE \
    (offsetof(moq_media_track_info_t, timescale) + \
     sizeof(((moq_media_track_info_t *)0)->timescale))

/*
 * Pointer-only initializer, SAFE FOR AN OLD, SMALLER CALLER.
 *
 * It cannot know how large the caller's struct is, so it touches ONLY
 * the frozen v0 prefix and stamps struct_size = MOQ_MEDIA_TRACK_INFO_V0_SIZE.
 * transport_version is deliberately NOT written -- a caller who wants it
 * must use moq_media_track_info_init_sized, which is told the size.
 *
 * An object parsed with an info initialized this way therefore takes the
 * legacy standalone old-prefix contract (draft-16), preserved for callers
 * compiled against the pre-version ABI.
 */
MOQ_API void moq_media_track_info_init(moq_media_track_info_t *info);

/*
 * Sized initializer for a caller that knows its own struct size.
 *
 * Clears exactly min(info_size, sizeof(current)) bytes -- never past an
 * old caller's allocation, never past the fields this build knows --
 * and stamps struct_size with the number of bytes actually written. When
 * that covers transport_version, it is set to MOQ_VERSION_DRAFT_16, the
 * behaviour-preserving default for a caller that does not override it;
 * a caller on a draft-18 session MUST assign the negotiated version.
 *
 * info_size below MOQ_MEDIA_TRACK_INFO_V0_SIZE writes nothing.
 */
MOQ_API void moq_media_track_info_init_sized(moq_media_track_info_t *info,
                                             size_t info_size);

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
 * The LOC property codec is selected by track->transport_version, and
 * ONLY when track->struct_size FULLY COVERS that field.
 *
 * Any valid struct_size that stops short of covering the whole field --
 * the frozen v0 prefix itself, or any partial prefix in between -- comes
 * from a caller that cannot express a version, so the legacy standalone
 * contract, draft-16, applies. A partially covered tail field is never
 * read: half a version is not a version.
 *
 * When the field IS fully covered the caller owns it and must name a
 * supported version: 0 is INVALID and does not masquerade as draft-16,
 * so a caller who cleared the struct by hand is refused rather than
 * silently given a codec.
 *
 * Returns MOQ_OK on success.
 * Returns MOQ_ERR_INVAL on bad struct_size, NULL required pointers,
 *   a covered-but-unsupported transport_version,
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
 *
 * When the timestamp IS present, a RAW/LOC object's whole timing comes from
 * that ONE scalar: it is COPIED into capture_time_us, decode_time_us and
 * presentation_time_us -- reinterpreted in each output, not unit-converted --
 * and composition_offset_us is 0. Two consequences a consumer should know:
 *
 *   - has_capture_time=true does not mean the publisher supplied a capture
 *     instant. The LOC field is DEFINED as an epoch capture timestamp
 *     (LOC-01 2.3.1.1), but a sender given no capture time copies its media
 *     presentation scalar into it (moq_media_send_object_t, media_sender.h),
 *     and which source domain produced the value cannot be identified here.
 *   - a publisher's own media-timeline values do not arrive alongside an
 *     application-supplied capture instant; the single wire value produced all
 *     three fields.
 *
 * This is RAW/LOC behaviour only. A CMAF object's timing is derived from the
 * fragment (base media decode time and the sample table), not from this field.
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
