#include <moq/media_object.h>
#include <moq/subscriber.h>
#include <moq/loc.h>
#include <moq/cmaf.h>
#include <moq/rcbuf.h>
#include <stddef.h>
#include <string.h>
#include "loc_codec_string.h"
#ifdef MOQ_MEDIA_OBJECT_HAVE_MPEG2TS
#include <moq/mpeg2ts.h>
#endif

/* -- Init functions -------------------------------------------------- */

#define TRACK_INFO_MIN_SIZE MOQ_MEDIA_TRACK_INFO_V0_SIZE

/* struct_size at or above this covers transport_version, so the caller
 * owns that field and must have named a supported version. */
#define TRACK_INFO_VERSION_SIZE \
    (offsetof(moq_media_track_info_t, transport_version) + \
     sizeof(((moq_media_track_info_t *)0)->transport_version))

_Static_assert(MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE == 8,
               "config floor through companion_profile must remain eight bytes");
_Static_assert(MOQ_MEDIA_PARSED_OBJECT_V0_SIZE ==
                   offsetof(moq_media_parsed_object_t, metadata),
               "parsed output floor must include original tail padding");
_Static_assert(offsetof(moq_media_parsed_object_t, loc_ext_present) ==
                   MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE,
               "the LOC-05 block starts at the complete sizeof through metadata");
_Static_assert(offsetof(moq_media_object_parse_cfg_t, track_loc_ext) >=
                   offsetof(moq_media_object_parse_cfg_t, mpeg2ts_packet_size) +
                   sizeof(uint32_t),
               "track_loc_ext follows mpeg2ts_packet_size");
_Static_assert(offsetof(moq_media_object_parse_cfg_t, catalog_codec) >=
                   offsetof(moq_media_object_parse_cfg_t, track_loc_ext) +
                   sizeof(const moq_loc_ext_t *),
               "catalog_codec follows track_loc_ext");

/*
 * Which transport draft's integer codec this track's LOC properties are
 * written in. An old caller -- struct_size exactly the frozen v0 prefix --
 * cannot express one, and keeps the legacy standalone draft-16 contract.
 * A caller whose struct_size COVERS the field owns it: 0 or any
 * unsupported value is rejected rather than being read as draft-16.
 */
static bool track_transport_version(const moq_media_track_info_t *track,
                                    moq_version_t *out)
{
    if (track->struct_size < TRACK_INFO_VERSION_SIZE) {
        *out = MOQ_VERSION_DRAFT_16;
        return true;
    }
    if (track->transport_version != MOQ_VERSION_DRAFT_16 &&
        track->transport_version != MOQ_VERSION_DRAFT_18 &&
        track->transport_version != MOQ_VERSION_DRAFT_22)
        return false;
    *out = track->transport_version;
    return true;
}

/* Exact floor(ticks * 1e6 / scale), without a wide-integer target dependency.
 * The fractional loop tracks quotient/remainder of a binary multiplier prefix;
 * subtract-before-add keeps each remainder below scale even near UINT64_MAX. */
static bool loc_time_us(uint64_t ticks, uint64_t scale, uint64_t *out_us)
{
    if (scale == 0) return false;
    const uint64_t whole = ticks / scale;
    const uint64_t rem = ticks % scale;
    if (whole > UINT64_MAX / 1000000u) return false;
    uint64_t us = whole * 1000000u;
    uint64_t frac = 0, r = 0;
    for (uint32_t bit = UINT32_C(1) << 19; bit != 0; bit >>= 1) {
        frac *= 2;
        if (r >= scale - r) { r -= scale - r; ++frac; }
        else r += r;
        if (UINT32_C(1000000) & bit) {
            if (r >= scale - rem) { r -= scale - rem; ++frac; }
            else r += rem;
        }
    }
    if (frac > UINT64_MAX - us) return false;
    *out_us = us + frac;
    return true;
}


void moq_media_track_info_init(moq_media_track_info_t *info)
{
    if (!info) return;
    /* Pointer-only: the caller's struct may be an OLD, SMALLER one, so
     * only the frozen v0 prefix may be written. transport_version is
     * deliberately left alone -- see the header. */
    memset(info, 0, MOQ_MEDIA_TRACK_INFO_V0_SIZE);
    info->struct_size = (uint32_t)MOQ_MEDIA_TRACK_INFO_V0_SIZE;
}

void moq_media_track_info_init_sized(moq_media_track_info_t *info,
                                     size_t info_size)
{
    if (!info) return;
    if (info_size < MOQ_MEDIA_TRACK_INFO_V0_SIZE) return;

    /* Never write past the caller's allocation, and never past the
     * fields this build knows about. */
    size_t n = info_size < sizeof(*info) ? info_size : sizeof(*info);
    memset(info, 0, n);
    info->struct_size = (uint32_t)n;
    if (n >= TRACK_INFO_VERSION_SIZE)
        info->transport_version = MOQ_VERSION_DRAFT_16;
}

void moq_media_object_input_init(moq_media_object_input_t *in)
{
    if (!in) return;
    memset(in, 0, sizeof(*in));
    in->struct_size = sizeof(*in);
}

void moq_media_parsed_object_init(moq_media_parsed_object_t *out)
{
    if (!out) return;
    memset(out, 0, MOQ_MEDIA_PARSED_OBJECT_V0_SIZE);
    out->struct_size = (uint32_t)MOQ_MEDIA_PARSED_OBJECT_V0_SIZE;
    out->sap_type = MOQ_SAP_UNKNOWN;  /* NONE (0) means "known not a SAP" */
}

void moq_media_parsed_object_init_sized(moq_media_parsed_object_t *out,
                                       size_t out_size)
{
    if (!out || out_size < sizeof(out->struct_size)) return;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    memset(out, 0, n);
    out->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_media_parsed_object_t, sap_type) + sizeof(out->sap_type))
        out->sap_type = MOQ_SAP_UNKNOWN;
    if (n >= MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE) {
        out->metadata.struct_size = (uint32_t)sizeof(out->metadata);
        moq_loc_metadata_init_sized(&out->metadata.loc, sizeof(out->metadata.loc));
    }
}

_Static_assert(MOQ_MEDIA_METADATA_V0_SIZE == sizeof(moq_media_metadata_t),
               "complete media metadata v0 floor");

static moq_result_t media_track_metadata_validate(const moq_loc_metadata_t *track,
                                                  bool d22)
{
    if (!track) return MOQ_OK;
    if (track->struct_size < MOQ_LOC_METADATA_V0_SIZE ||
        (track->present & ~MOQ_LOC_METADATA_PRESENT_MASK) ||
        track->reserved[0] || track->reserved[1]) return MOQ_ERR_INVAL;
    if (((track->present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG) &&
         track->video_config.len && !track->video_config.data) ||
        ((track->present & MOQ_LOC_METADATA_HAS_AUDIO_CONFIG) &&
         track->audio_config.len && !track->audio_config.data)) return MOQ_ERR_INVAL;
    if (!d22 && (track->present & (MOQ_LOC_METADATA_HAS_TIMESCALE |
                                  MOQ_LOC_METADATA_HAS_AUDIO_CONFIG)))
        return MOQ_ERR_UNSUPPORTED;
    return MOQ_OK;
}

/* A caller-supplied track extension record: complete, known bits, and a
 * present Codec String only on LOC05, where it must be usable. */
static moq_result_t media_track_ext_validate(const moq_loc_ext_t *track,
                                             bool loc05)
{
    if (!track) return MOQ_OK;
    if (track->struct_size < MOQ_LOC_EXT_V0_SIZE ||
        (track->present & ~MOQ_LOC_EXT_PRESENT_MASK)) return MOQ_ERR_INVAL;
    if (!(track->present & MOQ_LOC_EXT_HAS_CODEC_STRING)) return MOQ_OK;
    if (!loc05) return MOQ_ERR_UNSUPPORTED;
    if (track->codec_string.len > 0xFFFFu ||
        !moq_loc_internal_codec_string_ok(track->codec_string.data,
                                          track->codec_string.len))
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}

static moq_result_t media_metadata_parse(moq_version_t version,
    moq_loc_profile_t profile, moq_bytes_t properties,
    const moq_loc_metadata_t *track, const moq_loc_ext_t *track_ext,
    moq_bytes_t catalog_codec,
    moq_media_metadata_t *out, moq_loc_ext_t *ext_out)
{
    moq_result_t rc = moq_loc_parse_metadata_ex(version, profile, properties,
                                                &out->loc, sizeof(out->loc),
                                                ext_out, sizeof(*ext_out));
    if (rc != MOQ_OK) return rc;
    /* A Codec String and a Video/Audio Config are the two halves of one
     * decoder configuration (draft-ietf-moq-loc-05 2.3.1.3, 2.3.2.1,
     * 2.3.3.1). The track's configuration belongs to the track's codec: its
     * own Codec String, else the catalog's. An object that names its own
     * codec takes the track's configuration only when that codec is byte-equal
     * to the track's; otherwise, or when the track's codec is unknown here,
     * the configuration would be another codec's, so none is inherited. */
    uint32_t inheritable = MOQ_LOC_METADATA_HAS_TIMESCALE |
        MOQ_LOC_METADATA_HAS_VIDEO_CONFIG | MOQ_LOC_METADATA_HAS_AUDIO_CONFIG;
    if (ext_out->present & MOQ_LOC_EXT_HAS_CODEC_STRING) {
        moq_bytes_t track_codec = catalog_codec;
        if (track_ext && (track_ext->present & MOQ_LOC_EXT_HAS_CODEC_STRING))
            track_codec = track_ext->codec_string;
        moq_bytes_t own = ext_out->codec_string;
        if (track_codec.len == 0 || own.len != track_codec.len ||
            memcmp(own.data, track_codec.data, own.len) != 0)
            inheritable = MOQ_LOC_METADATA_HAS_TIMESCALE;
    }
    if (track_ext && !(ext_out->present & MOQ_LOC_EXT_HAS_CODEC_STRING) &&
        (track_ext->present & MOQ_LOC_EXT_HAS_CODEC_STRING)) {
        ext_out->codec_string = track_ext->codec_string;
        ext_out->present |= MOQ_LOC_EXT_HAS_CODEC_STRING;
    }
    if (track) {
        uint32_t inherited = track->present & ~out->loc.present & inheritable;
        if (inherited & MOQ_LOC_METADATA_HAS_TIMESCALE)
            out->loc.timescale = track->timescale;
        if (inherited & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG)
            out->loc.video_config = track->video_config;
        if (inherited & MOQ_LOC_METADATA_HAS_AUDIO_CONFIG)
            out->loc.audio_config = track->audio_config;
        out->loc.present |= inherited;
    }
    out->struct_size = (uint32_t)sizeof(*out);
    if (out->loc.present & MOQ_LOC_METADATA_HAS_TIMESTAMP) {
        if (out->loc.present & MOQ_LOC_METADATA_HAS_TIMESCALE) {
            out->time_domain = MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA;
            out->has_time_us = loc_time_us(out->loc.timestamp, out->loc.timescale, &out->time_us);
        } else {
            out->time_domain = MOQ_MEDIA_TIME_DOMAIN_UNIX_US;
            out->has_time_us = 1;
            out->time_us = out->loc.timestamp;
        }
    }
    bool loc04_family = profile == MOQ_LOC_PROFILE_04 ||
                        profile == MOQ_LOC_PROFILE_05;
    if ((out->loc.present & MOQ_LOC_METADATA_HAS_MARKING) &&
        out->loc.marking_len <= (loc04_family ? 3 : 2)) {
        out->independence_known = 1;
        out->independent = (out->loc.marking[0] & 0x20u) != 0;
    }
    return MOQ_OK;
}

/* -- Checked timestamp scaling --------------------------------------- */

static moq_result_t scale_time_checked(uint64_t ticks, uint32_t timescale,
                                        uint64_t *out)
{
    uint64_t sec = ticks / timescale;
    uint64_t rem = ticks % timescale;
    if (sec > UINT64_MAX / 1000000u) return MOQ_ERR_PROTO;
    uint64_t us_sec = sec * 1000000u;
    uint64_t us_rem = rem * 1000000u / timescale;
    if (us_sec > UINT64_MAX - us_rem) return MOQ_ERR_PROTO;
    *out = us_sec + us_rem;
    return MOQ_OK;
}

static moq_result_t scale_time_signed_checked(int32_t ticks,
                                               uint32_t timescale,
                                               int64_t *out)
{
    bool neg = ticks < 0;
    uint32_t abs_ticks = neg ? (uint32_t)(-(int64_t)ticks) : (uint32_t)ticks;
    uint64_t us;
    moq_result_t rc = scale_time_checked(abs_ticks, timescale, &us);
    if (rc != MOQ_OK) return rc;
    if (us > (uint64_t)INT64_MAX) return MOQ_ERR_PROTO;
    *out = neg ? -(int64_t)us : (int64_t)us;
    return MOQ_OK;
}

/* -- CMAF keyframe detection ----------------------------------------- */

static bool cmaf_sample_is_keyframe(uint32_t flags)
{
    if (flags & 0x00010000u) return false;
    uint32_t depends_on = (flags >> 24) & 0x3u;
    if (depends_on == 1) return false;
    if (depends_on == 2) return true;
    return (flags == 0);
}

/* -- MPEG-2 TS ------------------------------------------------------- */

#ifdef MOQ_MEDIA_OBJECT_HAVE_MPEG2TS
/* 90 kHz ticks (below 2^33) to microseconds; exact for whole ticks. */
static uint64_t ts90k_to_us(uint64_t t)
{
    return t / 9u * 100u + (t % 9u) * 100u / 9u;
}

static moq_result_t parse_mpeg2ts(const moq_media_track_info_t *track,
                                  const moq_media_object_input_t *in,
                                  uint32_t ps,
                                  moq_media_parsed_object_t *out,
                                  moq_media_drop_reason_t *drop_reason)
{
    moq_bytes_t payload = { moq_rcbuf_data(in->payload),
                            moq_rcbuf_len(in->payload) };
    if (!payload.data) payload.len = 0;

    /* Object Payload Format: whole source packets only. */
    moq_mpeg2ts_object_rules_t rules;
    moq_mpeg2ts_object_rules_init(&rules);
    rules.packet_size = ps;
    moq_mpeg2ts_object_report_t rep;
    moq_mpeg2ts_object_report_init(&rep);
    if (moq_mpeg2ts_validate_object(&rules, payload, &rep, sizeof(rep)) != MOQ_OK) {
        if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_MPEG2TS;
        return MOQ_ERR_PROTO;
    }

    moq_mpeg2ts_inspect_cfg_t ip;
    moq_mpeg2ts_inspect_cfg_init(&ip);
    ip.packet_size = ps;
    /* A video track's keyframe and timing come from the program's video
     * stream: muxers commonly set random_access_indicator on every audio
     * frame too. */
    if (track->media_type == MOQ_MEDIA_TYPE_VIDEO) {
        ip.random_access_pid = MOQ_MPEG2TS_PID_AUTO;
        ip.timing_pid = MOQ_MPEG2TS_PID_AUTO;
    }
    moq_mpeg2ts_object_info_t info;
    moq_mpeg2ts_object_info_init(&info);
    if (moq_mpeg2ts_inspect_object(&ip, payload, &info, sizeof(info)) != MOQ_OK) {
        if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_MPEG2TS;
        return MOQ_ERR_PROTO;
    }

    out->payload = payload;
    out->chunk_count = info.packet_count > UINT32_MAX
                     ? UINT32_MAX : (uint32_t)info.packet_count;
    out->sap_type = MOQ_SAP_UNKNOWN;
    out->keyframe = track->media_type == MOQ_MEDIA_TYPE_AUDIO ||
                    info.has_random_access;
    if (info.has_pts) {
        uint64_t dts = info.has_dts ? info.dts : info.pts;
        out->track_id = info.timing_pid;
        out->decode_time_us = ts90k_to_us(dts);
        out->presentation_time_us = ts90k_to_us(info.pts);
        out->composition_offset_us =
            (int64_t)ts90k_to_us(moq_mpeg2ts_pts_delta(dts, info.pts));
    }
    return MOQ_OK;
}
#endif

/* -- Parse ----------------------------------------------------------- */

#define OBJECT_INPUT_MIN_SIZE \
    (offsetof(moq_media_object_input_t, properties) + \
     sizeof(((moq_media_object_input_t *)0)->properties))

static moq_result_t media_object_parse_into(
    const moq_media_track_info_t *track,
    const moq_media_object_input_t *in,
    moq_cmaf_sample_t *samples,
    size_t sample_cap,
    moq_media_parsed_object_t *out,
    moq_media_drop_reason_t *drop_reason,
    moq_loc_profile_t metadata_profile,
    const moq_loc_metadata_t *track_metadata,
    const moq_loc_ext_t *track_ext,
    moq_bytes_t catalog_codec,
    uint32_t packet_size)
{
    /* The sized parser names the LOC revision its companion selects; the
     * legacy unsized parser passes 0 and has no metadata output. */
    bool metadata = metadata_profile != 0;
    if (drop_reason) *drop_reason = 0;
    if (!track || !in || !out) return MOQ_ERR_INVAL;

    if (track->struct_size < TRACK_INFO_MIN_SIZE) return MOQ_ERR_INVAL;
    if (in->struct_size < OBJECT_INPUT_MIN_SIZE) return MOQ_ERR_INVAL;

    /* Resolved before any packaging branch, so a CMAF object with no
     * properties is refused an unsupported version too -- the check must
     * not depend on whether a LOC block happens to be present. */
    moq_version_t transport_version;
    if (!track_transport_version(track, &transport_version))
        return MOQ_ERR_INVAL;
    if (!metadata && transport_version == MOQ_VERSION_DRAFT_22)
        return MOQ_ERR_INVAL;

    if (track->media_type != MOQ_MEDIA_TYPE_VIDEO &&
        track->media_type != MOQ_MEDIA_TYPE_AUDIO)
        return MOQ_ERR_INVAL;
    bool is_mpeg2ts = track->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS;
    if (track->packaging != MOQ_MEDIA_PACKAGING_RAW &&
        track->packaging != MOQ_MEDIA_PACKAGING_CMAF && !is_mpeg2ts)
        return MOQ_ERR_INVAL;
    if (is_mpeg2ts && packet_size != 188 && packet_size != 192)
        return MOQ_ERR_INVAL;
#ifndef MOQ_MEDIA_OBJECT_HAVE_MPEG2TS
    if (is_mpeg2ts) return MOQ_ERR_UNSUPPORTED;
#endif

    if (in->status != MOQ_OBJECT_NORMAL &&
        in->status != MOQ_OBJECT_END_OF_GROUP &&
        in->status != MOQ_OBJECT_END_OF_TRACK)
        return MOQ_ERR_INVAL;

    if (in->status == MOQ_OBJECT_NORMAL && !in->payload)
        return MOQ_ERR_INVAL;
    if (in->status != MOQ_OBJECT_NORMAL && in->payload)
        return MOQ_ERR_INVAL;

    moq_media_parsed_object_init(out);
    out->packaging = track->packaging;
    out->status = in->status;
    out->end_of_group = in->end_of_group;
    out->datagram = in->datagram;

    if (in->status != MOQ_OBJECT_NORMAL)
        return MOQ_OK;

#ifdef MOQ_MEDIA_OBJECT_HAVE_MPEG2TS
    if (is_mpeg2ts)
        return parse_mpeg2ts(track, in, packet_size, out, drop_reason);
#endif

    /* -- Parse LOC properties (both RAW and CMAF) -------------------- */
    moq_loc_headers_t loc;
    bool loc_parsed = false;
    {
        moq_bytes_t props = { NULL, 0 };
        if (in->properties) {
            props.data = moq_rcbuf_data(in->properties);
            props.len = moq_rcbuf_len(in->properties);
        }

        if (metadata) {
            moq_loc_ext_t ext;
            moq_loc_ext_init_sized(&ext, sizeof(ext));
            moq_result_t lr = media_metadata_parse(transport_version,
                metadata_profile, props, track_metadata, track_ext,
                catalog_codec, &out->metadata, &ext);
            if (lr == MOQ_ERR_PROTO && drop_reason)
                *drop_reason = MOQ_MEDIA_DROP_MALFORMED_LOC;
            if (lr != MOQ_OK) return lr;
            out->loc_ext_present = ext.present;
            out->codec_string = ext.codec_string;
            if (out->metadata.time_domain == MOQ_MEDIA_TIME_DOMAIN_UNIX_US &&
                out->metadata.has_time_us) {
                out->has_capture_time = true;
                out->capture_time_us = out->metadata.time_us;
            }
        } else if (props.len > 0 || track->packaging == MOQ_MEDIA_PACKAGING_RAW) {
            moq_result_t lr = moq_loc_parse(transport_version,
                MOQ_LOC_PROFILE_01, props, &loc);
            if (lr == MOQ_ERR_PROTO) {
                if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_LOC;
                return MOQ_ERR_PROTO;
            }
            if (lr != MOQ_OK) return MOQ_ERR_INVAL;
            loc_parsed = true;
        }
    }

    if (loc_parsed && loc.has_timestamp) {
        out->capture_time_us = loc.timestamp;
        out->has_capture_time = true;
    }

    /* LOC video frame marking is authoritative for keyframe/independence when
     * present; the CMAF sample sync flag (parsed below) is only a fallback.
     * Tracked so the CMAF block does not overwrite an explicit LOC marking --
     * a fragment that omits per-sample flags parses as a keyframe (flags == 0
     * reads as a sync sample), which would otherwise lose independent=false. */
    bool keyframe_from_loc = false;
    if (track->media_type == MOQ_MEDIA_TYPE_AUDIO) {
        out->keyframe = true;
    } else if (metadata && out->metadata.independence_known) {
        out->keyframe = out->metadata.independent != 0;
        keyframe_from_loc = true;
    } else if (loc_parsed && loc.has_video_frame_marking) {
        out->keyframe = loc.video_frame_marking.independent;
        keyframe_from_loc = true;
    }

    /* -- RAW --------------------------------------------------------- */
    if (track->packaging == MOQ_MEDIA_PACKAGING_RAW) {
        /* The LOC capture timestamp is OPTIONAL metadata (draft-ietf-moq-loc-01):
         * an object MAY omit it, and decoders fall back to delivery order when it
         * cannot be obtained. So a missing timestamp is NOT a media-object
         * protocol error -- surface the object with has_capture_time=false rather
         * than dropping it, for interop with peers that deliver LOC objects
         * without the capture-timestamp extension. Any
         * stricter per-frame timing requirement belongs to playback-level code,
         * which sees has_capture_time. When the timestamp IS present it drives
         * decode/presentation time as before. */
        if (metadata && out->metadata.has_time_us) {
            out->decode_time_us = out->metadata.time_us;
            out->composition_offset_us = 0;
            out->presentation_time_us = out->metadata.time_us;
        } else if (out->has_capture_time) {
            out->decode_time_us = out->capture_time_us;
            out->composition_offset_us = 0;
            out->presentation_time_us = out->capture_time_us;
        }

        out->payload.data = moq_rcbuf_data(in->payload);
        out->payload.len = moq_rcbuf_len(in->payload);

        return MOQ_OK;
    }

    /* -- CMAF -------------------------------------------------------- */
    if (track->timescale == 0) return MOQ_ERR_INVAL;

    moq_bytes_t frag_bytes;
    frag_bytes.data = moq_rcbuf_data(in->payload);
    frag_bytes.len = moq_rcbuf_len(in->payload);

    moq_cmaf_fragment_info_t frag;
    moq_cmaf_fragment_info_init(&frag, samples, sample_cap);
    moq_result_t fr = moq_cmaf_parse_fragment(frag_bytes, &frag);

    if (fr == MOQ_ERR_BUFFER) {
        out->sample_count = frag.sample_count;
        return MOQ_ERR_BUFFER;
    }
    if (fr != MOQ_OK) {
        if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
        return MOQ_ERR_PROTO;
    }

    if (frag.sample_count == 0) {
        if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
        return MOQ_ERR_PROTO;
    }

    {
        uint64_t total_size = 0;
        for (size_t i = 0; i < frag.sample_count; i++) {
            if (frag.samples[i].size > UINT64_MAX - total_size) {
                if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
                return MOQ_ERR_PROTO;
            }
            total_size += frag.samples[i].size;
        }
        if (total_size > frag.mdat.len) {
            if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
            return MOQ_ERR_PROTO;
        }
    }

    {
        moq_result_t rc = scale_time_checked(
            frag.base_decode_time, track->timescale, &out->decode_time_us);
        if (rc != MOQ_OK) {
            if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
            return MOQ_ERR_PROTO;
        }
    }
    {
        moq_result_t rc = scale_time_signed_checked(
            frag.samples[0].composition_offset, track->timescale,
            &out->composition_offset_us);
        if (rc != MOQ_OK) {
            if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
            return MOQ_ERR_PROTO;
        }
    }
    if (out->composition_offset_us >= 0) {
        uint64_t off = (uint64_t)out->composition_offset_us;
        if (out->decode_time_us > UINT64_MAX - off) {
            if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
            return MOQ_ERR_PROTO;
        }
        out->presentation_time_us = out->decode_time_us + off;
    } else {
        uint64_t off = (uint64_t)(-out->composition_offset_us);
        if (off > out->decode_time_us) {
            if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
            return MOQ_ERR_PROTO;
        }
        out->presentation_time_us = out->decode_time_us - off;
    }

    out->sample_count = frag.sample_count;
    out->samples = frag.samples;
    {
        uint64_t dur_us;
        moq_result_t rc = scale_time_checked(
            frag.samples[0].duration, track->timescale, &dur_us);
        if (rc != MOQ_OK || dur_us > UINT32_MAX) {
            if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_CMAF;
            return MOQ_ERR_PROTO;
        }
        out->sample_duration_us = (uint32_t)dur_us;
    }

    if (track->media_type == MOQ_MEDIA_TYPE_AUDIO) {
        out->keyframe = true;
    } else if (!keyframe_from_loc) {
        /* Fallback only: an explicit LOC video frame marking already set it. */
        out->keyframe = cmaf_sample_is_keyframe(frag.samples[0].flags);
    }

    out->sap_type = moq_cmaf_sap_from_sample_flags(frag.samples[0].flags);
    out->chunk_count = frag.chunk_count;
    out->track_id = frag.track_id;

    out->fragment.data = frag_bytes.data;
    out->fragment.len = frag_bytes.len;
    out->mdat_offset = (size_t)(frag.mdat.data - frag_bytes.data);
    out->mdat_len = frag.mdat.len;

    return MOQ_OK;
}

moq_result_t moq_media_object_parse(
    const moq_media_track_info_t *track,
    const moq_media_object_input_t *in,
    moq_cmaf_sample_t *samples,
    size_t sample_cap,
    moq_media_parsed_object_t *out,
    moq_media_drop_reason_t *drop_reason)
{
    return media_object_parse_into(track, in, samples, sample_cap, out,
                                   drop_reason, (moq_loc_profile_t)0, NULL,
                                   NULL, (moq_bytes_t){ NULL, 0 }, 0);
}

/* Read a caller's parse config into a full current struct by byte offset
 * (the caller's object may be an older, smaller struct, never read through
 * this type): the v0 prefix, then each appended field only when struct_size
 * covers all of it. Fields left out keep their zero default (NULL, 0). The
 * caller has checked struct_size >= MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE. */
static void media_parse_cfg_read(const moq_media_object_parse_cfg_t *in,
                                 moq_media_object_parse_cfg_t *out)
{
    const uint8_t *src = (const uint8_t *)in;
    uint32_t size;
    memcpy(&size, src, sizeof(size));
    memset(out, 0, sizeof(*out));
    memcpy(out, src, MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE);
#define PARSE_CFG_COPY(f) do { \
    const size_t offset = offsetof(moq_media_object_parse_cfg_t, f); \
    if (size >= offset + sizeof(out->f)) \
        memcpy(&out->f, src + offset, sizeof(out->f)); \
} while (0)
    PARSE_CFG_COPY(track_loc_metadata);
    PARSE_CFG_COPY(mpeg2ts_packet_size);
    PARSE_CFG_COPY(track_loc_ext);
    PARSE_CFG_COPY(catalog_codec);
#undef PARSE_CFG_COPY
    out->struct_size = (uint32_t)sizeof(*out);
}

moq_result_t moq_media_object_parse_sized(
    const moq_media_track_info_t *track,
    const moq_media_object_input_t *in,
    const moq_media_object_parse_cfg_t *cfg,
    moq_cmaf_sample_t *samples,
    size_t sample_cap,
    moq_media_parsed_object_t *out,
    size_t out_size,
    moq_media_drop_reason_t *drop_reason)
{
    if (!track || !in || !out || track->struct_size < TRACK_INFO_MIN_SIZE ||
        in->struct_size < OBJECT_INPUT_MIN_SIZE) return MOQ_ERR_INVAL;
    /* Every config read below uses the full local copy; no config is the
     * all-default one (legacy companion, nothing appended). */
    moq_media_object_parse_cfg_t c = MOQ_MEDIA_OBJECT_PARSE_CFG_INIT;
    if (cfg) {
        uint32_t cfg_size;
        memcpy(&cfg_size, cfg, sizeof(cfg_size));
        if (cfg_size < MOQ_MEDIA_OBJECT_PARSE_CFG_V0_SIZE) return MOQ_ERR_INVAL;
        media_parse_cfg_read(cfg, &c);
    }
    moq_media_companion_profile_t companion_profile = c.companion_profile;
    moq_version_t version;
    if (!track_transport_version(track, &version) ||
        (companion_profile != MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 &&
         companion_profile != MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC &&
         companion_profile != MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC))
        return MOQ_ERR_INVAL;
    bool loc05 = companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC;
    bool d22 = loc05 ||
               companion_profile == MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC;
    if (d22 != (version == MOQ_VERSION_DRAFT_22)) return MOQ_ERR_UNSUPPORTED;
    if (d22 && out_size < MOQ_MEDIA_PARSED_OBJECT_METADATA_SIZE)
        return MOQ_ERR_UNSUPPORTED;
    if (loc05 && out_size < MOQ_MEDIA_PARSED_OBJECT_LOC_EXT_SIZE)
        return MOQ_ERR_UNSUPPORTED;
    if (out_size < MOQ_MEDIA_PARSED_OBJECT_V0_SIZE) return MOQ_ERR_INVAL;
    moq_loc_profile_t profile = loc05 ? MOQ_LOC_PROFILE_05 :
        d22 ? MOQ_LOC_PROFILE_04 : MOQ_LOC_PROFILE_01;
    const moq_loc_metadata_t *track_metadata = c.track_loc_metadata;
    uint32_t packet_size = c.mpeg2ts_packet_size;
    const moq_loc_ext_t *track_ext = c.track_loc_ext;
    moq_result_t rc = media_track_metadata_validate(track_metadata, d22);
    if (rc != MOQ_OK) return rc;
    rc = media_track_ext_validate(track_ext, loc05);
    if (rc != MOQ_OK) return rc;
    if (c.catalog_codec.len > 0 && !c.catalog_codec.data) return MOQ_ERR_INVAL;
    if (track->packaging == MOQ_MEDIA_PACKAGING_MPEG2TS &&
        packet_size != 188 && packet_size != 192)
        return MOQ_ERR_INVAL;
    moq_media_parsed_object_t tmp;
    moq_media_parsed_object_init_sized(&tmp, sizeof(tmp));
    rc = media_object_parse_into(track, in, samples, sample_cap,
                                 &tmp, drop_reason, profile, track_metadata,
                                 track_ext, c.catalog_codec, packet_size);
    if (rc == MOQ_OK || rc == MOQ_ERR_BUFFER) {
        size_t n = out_size < sizeof(tmp) ? out_size : sizeof(tmp);
        tmp.struct_size = (uint32_t)n;
        memcpy(out, &tmp, n);
    }
    return rc;
}

/* -- Subscriber input helper ----------------------------------------- */

moq_result_t moq_media_object_input_from_sub_object(
    const moq_sub_object_t *src,
    moq_media_object_input_t *dst)
{
    if (!src || !dst) return MOQ_ERR_INVAL;
    moq_media_object_input_init(dst);
    dst->group_id    = src->group_id;
    dst->object_id   = src->object_id;
    dst->status      = src->status;
    dst->end_of_group = src->end_of_group;
    dst->datagram    = src->datagram;
    dst->payload     = src->payload;
    dst->properties  = src->properties;
    return MOQ_OK;
}
