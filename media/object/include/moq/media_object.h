#ifndef MOQ_MEDIA_OBJECT_H
#define MOQ_MEDIA_OBJECT_H

/*
 * Stateless MoQ media object normalizer.
 *
 * Parses one MoQ media object into timing, keyframe, and payload
 * metadata. Handles RAW (LOC properties), CMAF (fragment parsing) and, in a
 * build with moq::mpeg2ts, MPEG-2 TS packaging. No state, no heap allocation, no retained
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
#include <moq/loc.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Enums ----------------------------------------------------------- */

/* Packaging. Fixed-width uint32_t for ABI stability. Values are never
 * renumbered. Unknown values must be ignored (a parse refuses them). */
typedef uint32_t moq_media_packaging_t;

#define MOQ_MEDIA_PACKAGING_RAW     1u
#define MOQ_MEDIA_PACKAGING_CMAF    2u
/* MSF "mpeg2ts" packaging (draft-gregoire-moq-msfts): whole source packets
 * of moq_media_object_parse_cfg_t.mpeg2ts_packet_size octets. Parsed only by a
 * build that includes moq::mpeg2ts; any other build returns
 * MOQ_ERR_UNSUPPORTED. */
#define MOQ_MEDIA_PACKAGING_MPEG2TS 3u

/* Why an object was dropped. Fixed-width uint32_t for ABI stability. Values
 * are never renumbered. Unknown reasons must be ignored. */
typedef uint32_t moq_media_drop_reason_t;

#define MOQ_MEDIA_DROP_MALFORMED_LOC            1u
#define MOQ_MEDIA_DROP_MALFORMED_CMAF           2u
#define MOQ_MEDIA_DROP_MISSING_TIMESTAMP        3u
#define MOQ_MEDIA_DROP_NON_MONOTONIC_DTS        4u
#define MOQ_MEDIA_DROP_UNSUPPORTED_MULTI_SAMPLE 5u
#define MOQ_MEDIA_DROP_STALE                    6u
#define MOQ_MEDIA_DROP_KEYFRAME_WAIT            7u
#define MOQ_MEDIA_DROP_MALFORMED_MPEG2TS        8u

typedef enum moq_media_type {
    MOQ_MEDIA_TYPE_VIDEO = 1,
    MOQ_MEDIA_TYPE_AUDIO = 2,
} moq_media_type_t;

/* Fixed-width shared types. LEGACY_MSF01_LOC01 pairs with drafts 16/18 and
 * D22_MSF01_LOC04_PUBLIC with draft 22. AUTO is for the service sender and
 * receiver configs (their default): it resolves to whichever of the two
 * matches the session's negotiated draft. The object parser takes a resolved
 * companion and refuses AUTO. Values are never renumbered. */
typedef uint32_t moq_media_companion_profile_t;
#define MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01     UINT32_C(0)
#define MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC UINT32_C(1)
#define MOQ_MEDIA_COMPANION_AUTO                   UINT32_C(2)

typedef uint32_t moq_media_time_domain_t;
#define MOQ_MEDIA_TIME_DOMAIN_NONE              UINT32_C(0)
#define MOQ_MEDIA_TIME_DOMAIN_UNIX_US           UINT32_C(1)
#define MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA UINT32_C(2)

/* Sized, value-only metadata. LOC is the effective snapshot: object presence
 * overrides track presence for timescale/video/audio config, including empty
 * config. Object spans borrow object properties; inherited config spans borrow
 * the caller's distinct track-properties backing. Neither is retained here.
 * time_us is available only when has_time_us; raw zero/overflow survives in loc.
 * Independence describes normalized LOC marking, not a guessed opaque form. */
typedef struct moq_media_metadata {
    uint32_t struct_size;
    moq_media_time_domain_t time_domain;
    uint64_t time_us;
    moq_loc_metadata_t loc;
    uint8_t has_time_us;
    uint8_t independence_known;
    uint8_t independent;
    uint8_t reserved[5];
} moq_media_metadata_t;

#define MOQ_MEDIA_METADATA_V0_SIZE \
    (offsetof(moq_media_metadata_t, reserved) + \
     sizeof(((moq_media_metadata_t *)0)->reserved))

/* -- Track info (caller-supplied context) ----------------------------- */

/*
 * The FROZEN v0 PREFIX runs from struct_size through `timescale`. Those
 * four fields and their order will not change. The existing transport-version
 * tail is read only when covered. This complete type is embedded in service
 * descriptors and must not grow; new parser context uses a separate config.
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
 * MPEG2TS packet size belongs in the separate parser config, from the catalog.
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
    uint64_t              capture_time_us;   /* UNIX microseconds only */

    /* RAW (LOC): decode_time_us and presentation_time_us both copy the
     * object's LOC media time (its capture timestamp on drafts 16/18, the
     * LOC-04 time on draft 22) and composition_offset_us is 0. LOC carries
     * no decode timestamp: decode order is object order, so decode_time_us
     * may decrease in decode order and is not evidence of a known decode
     * clock, and a zero offset does not mean the codec has no reordered
     * frames. CMAF: decode_time_us is the tfdt/trun decode time. */
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

    /* MPEG2TS: `payload` is the whole Object (its source
     * packets); `fragment`, mdat and samples are empty. For a video track the
     * program's video stream decides (its PID from a PMT in the Object, else
     * a PES with a video stream_id, see MOQ_MPEG2TS_PID_AUTO): keyframe is
     * true when the Object holds a random access point of that stream
     * (random_access_indicator announcing a PES start), and timing is taken
     * from its first PES with a PTS. For an audio track keyframe is always
     * true and timing comes from the PES at the first random access point,
     * else the first PES that carries a PTS. decode_time_us is the DTS (the
     * PTS when there is none), presentation_time_us the PTS, both converted
     * from the 90 kHz clock without unwrapping its 2^33 wrap. track_id is the
     * PID of that PES, or 0 when there is none (PID 0 carries the PAT, never
     * a PES), in which case the timing fields are 0. One Object of a
     * multiplex can hold several streams and several frames: these fields
     * describe the one PES chosen, not every frame. chunk_count is the number
     * of source packets (saturating). sap_type is UNKNOWN:
     * random_access_indicator does not give a SAP type. Continuity counters
     * and the es-packets PID rules are not judged here; see <moq/mpeg2ts.h>. */

    /* Starts after the complete baseline parsed-object sizeof. Read only
     * when the outer struct_size covers this entire field. */
    moq_media_metadata_t         metadata;    /* borrowed spans, no owned refs */
} moq_media_parsed_object_t;

/* Original padded sizeof through track_id, independent of future tails: the
 * complete frozen baseline record, including its tail padding. metadata
 * starts exactly here. */
#ifdef __cplusplus
#define MOQ_MEDIA_PARSED_OBJECT_V0_ALIGNMENT \
    (alignof(void *) > alignof(uint64_t) ? alignof(void *) : alignof(uint64_t))
#else
#define MOQ_MEDIA_PARSED_OBJECT_V0_ALIGNMENT \
    (_Alignof(void *) > _Alignof(uint64_t) ? _Alignof(void *) : _Alignof(uint64_t))
#endif
#define MOQ_MEDIA_PARSED_OBJECT_V0_SIZE \
    ((offsetof(moq_media_parsed_object_t, track_id) + \
      sizeof(((moq_media_parsed_object_t *)0)->track_id) + \
      MOQ_MEDIA_PARSED_OBJECT_V0_ALIGNMENT - 1) & \
     ~(size_t)(MOQ_MEDIA_PARSED_OBJECT_V0_ALIGNMENT - 1))

/* D22 requires the whole metadata field, not a partial tail. */
#define MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE \
    (offsetof(moq_media_parsed_object_t, metadata) + sizeof(moq_media_metadata_t))

/* Pointer-only initializer clears/stamps ONLY the complete baseline record,
 * including its tail padding, and sets sap_type to MOQ_SAP_UNKNOWN. Metadata
 * and any later bytes are untouched, including through a function pointer.
 * NULL writes nothing. */
MOQ_API void moq_media_parsed_object_init(moq_media_parsed_object_t *out);

/* Clears min(out_size, library size) bytes and stamps struct_size with that
 * count, without reading its previous value. NULL or fewer than four bytes
 * writes nothing. A wholly covered sap_type is set to MOQ_SAP_UNKNOWN.
 * A wholly covered metadata field receives its own and its LOC struct_size
 * stamps; all other metadata bytes stay zero. Partial metadata is only cleared
 * within the supplied prefix. Bytes beyond the written count are untouched.
 * No allocation, retained refs, or cleanup is needed. */
MOQ_API void moq_media_parsed_object_init_sized(moq_media_parsed_object_t *out,
                                              size_t out_size);

/* -- Parse ----------------------------------------------------------- */

/*
 * Parse and normalize one MoQ media object.
 *
 * Stateless, allocation-free, no retained refs.
 *
 * Legacy LOC01+D16/18 only. Stores only the complete frozen output prefix;
 * D22 requires parse_sized below.
 * The LOC integer codec is selected by track->transport_version, and ONLY
 * when track->struct_size FULLY COVERS that field.
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
 * MPEG2TS requires parse_sized below, with explicit packet-size context.
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
 * Capture timestamp is OPTIONAL (draft-ietf-moq-loc-01): a RAW/LOC
 * object with no LOC timestamp is NOT a parse failure -- it returns MOQ_OK with
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

/* Separate from the frozen embedded track record. NULL cfg selects legacy with
 * no track context; it never opts into draft-22. The frozen input floor covers
 * companion_profile. Each tail (track_loc_metadata, mpeg2ts_packet_size) is
 * read only when its whole field is covered. Config/metadata records are
 * borrowed for the call; inherited byte spans borrow their original backing
 * for the output's lifetime. No ownership transfer or cleanup. */
typedef struct moq_media_object_parse_cfg {
    uint32_t struct_size;
    uint32_t companion_profile;
    const moq_loc_metadata_t *track_loc_metadata;
    uint32_t mpeg2ts_packet_size;
} moq_media_object_parse_cfg_t;

#define MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE \
    (offsetof(moq_media_object_parse_cfg_t, companion_profile) + \
     sizeof(((moq_media_object_parse_cfg_t *)0)->companion_profile))
#define MOQ_MEDIA_OBJECT_PARSE_CFG_INIT \
    { (uint32_t)sizeof(moq_media_object_parse_cfg_t), \
      MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01, NULL, 0 }

/* Explicit LEGACY_MSF01_LOC01+D16/18 or D22_MSF01_LOC04_PUBLIC+D22.
 * Unknown companion/version (AUTO included) or malformed sized config is
 * INVAL; a known but
 * mismatched pair is UNSUPPORTED. D22 requires METADATA_SIZE bytes; shorter output returns
 * UNSUPPORTED before any output/drop-reason store. Legacy pairs require the
 * old V0_SIZE floor. No caller input output-stamp is required.
 * Success copies/stamps min(out_size, library size). Other failures leave out
 * untouched, except BUFFER publishes the required sample_count as on the old
 * parser. Caller sample storage is scratch during CMAF parsing, even on error.
 * PROTO sets MALFORMED_LOC/CMAF; duplicate known LOC is UNSUPPORTED, not a drop.
 * No refs are taken or transferred on any result. Payload/fragment/samples and
 * object config borrow in/sample backing. These must outlive use of out;
 * no cleanup is needed. Only absent timescale/video/audio configuration inherits
 * from track_loc_metadata. Present object fields, including empty config, win.
 * Track timestamp/marking/audio-level fields do not become object metadata.
 * A supplied track record needs its complete v0 floor, known presence bits,
 * zero reserved bytes, and coherent selected config spans; otherwise INVAL.
 * Track timescale/audio configuration on the legacy companion is UNSUPPORTED.
 * Timestamp without effective LOC timescale is UNIX/capture time; with any
 * scale it is application time, never capture. Zero scale/overflow keeps raw
 * metadata with has_time_us=false. RAW timing uses available time_us; CMAF
 * tfdt/trun timing stays authoritative. Catalog timescale is not a LOC default.
 * LOC04 marking1..3 uses the first payload byte's I bit; marking4 is opaque.
 * Well-formed unknown properties are skipped; original property bytes stay
 * unchanged and separate. No automatic companion or service eligibility.
 *
 * MPEG2TS requires a covered packet size of 188 or 192 (otherwise INVAL);
 * other packaging ignores it. The unsized parser cannot supply TS context and
 * returns INVAL for TS. Valid TS context in a build without moq::mpeg2ts
 * returns UNSUPPORTED. Empty, partial or invalid-sync TS packets return PROTO
 * with drop reason MOQ_MEDIA_DROP_MALFORMED_MPEG2TS. An MPEG2TS object's
 * properties are not parsed as LOC, so its metadata stays empty. */
MOQ_API moq_result_t moq_media_object_parse_sized(
    const moq_media_track_info_t *track,
    const moq_media_object_input_t *in,
    const moq_media_object_parse_cfg_t *cfg,
    moq_cmaf_sample_t *samples,
    size_t sample_cap,
    moq_media_parsed_object_t *out,
    size_t out_size,
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
