#include <moq/media_object.h>
#include <moq/subscriber.h>
#include <moq/loc.h>
#include <moq/cmaf.h>
#include <moq/rcbuf.h>
#include <stddef.h>
#include <string.h>

/* -- Init functions -------------------------------------------------- */

void moq_media_track_info_init(moq_media_track_info_t *info)
{
    if (!info) return;
    memset(info, 0, sizeof(*info));
    info->struct_size = sizeof(*info);
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
    memset(out, 0, sizeof(*out));
    out->struct_size = sizeof(*out);
    out->sap_type = MOQ_SAP_UNKNOWN;  /* NONE (0) means "known not a SAP" */
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

/* -- Parse ----------------------------------------------------------- */

#define TRACK_INFO_MIN_SIZE \
    (offsetof(moq_media_track_info_t, timescale) + \
     sizeof(((moq_media_track_info_t *)0)->timescale))

#define OBJECT_INPUT_MIN_SIZE \
    (offsetof(moq_media_object_input_t, properties) + \
     sizeof(((moq_media_object_input_t *)0)->properties))

moq_result_t moq_media_object_parse(
    const moq_media_track_info_t *track,
    const moq_media_object_input_t *in,
    moq_cmaf_sample_t *samples,
    size_t sample_cap,
    moq_media_parsed_object_t *out,
    moq_media_drop_reason_t *drop_reason)
{
    if (drop_reason) *drop_reason = 0;
    if (!track || !in || !out) return MOQ_ERR_INVAL;

    if (track->struct_size < TRACK_INFO_MIN_SIZE) return MOQ_ERR_INVAL;
    if (in->struct_size < OBJECT_INPUT_MIN_SIZE) return MOQ_ERR_INVAL;

    if (track->media_type != MOQ_MEDIA_TYPE_VIDEO &&
        track->media_type != MOQ_MEDIA_TYPE_AUDIO)
        return MOQ_ERR_INVAL;
    if (track->packaging != MOQ_MEDIA_PACKAGING_RAW &&
        track->packaging != MOQ_MEDIA_PACKAGING_CMAF)
        return MOQ_ERR_INVAL;

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

    /* -- Parse LOC properties (both RAW and CMAF) -------------------- */
    moq_loc_headers_t loc;
    bool loc_parsed = false;
    {
        moq_bytes_t props = { NULL, 0 };
        if (in->properties) {
            props.data = moq_rcbuf_data(in->properties);
            props.len = moq_rcbuf_len(in->properties);
        }

        if (props.len > 0 || track->packaging == MOQ_MEDIA_PACKAGING_RAW) {
            moq_result_t lr = moq_loc_parse(MOQ_LOC_PROFILE_01, props, &loc);
            if (lr == MOQ_ERR_PROTO) {
                if (drop_reason) *drop_reason = MOQ_MEDIA_DROP_MALFORMED_LOC;
                return MOQ_ERR_PROTO;
            }
            if (lr != MOQ_OK) return MOQ_ERR_INVAL;
            loc_parsed = true;
        }
    }

    if (loc_parsed && loc.has_timestamp) {
        out->has_capture_time = true;
        out->capture_time_us = loc.timestamp;
    }

    /* LOC video frame marking is authoritative for keyframe/independence when
     * present; the CMAF sample sync flag (parsed below) is only a fallback.
     * Tracked so the CMAF block does not overwrite an explicit LOC marking --
     * a fragment that omits per-sample flags parses as a keyframe (flags == 0
     * reads as a sync sample), which would otherwise lose independent=false. */
    bool keyframe_from_loc = false;
    if (track->media_type == MOQ_MEDIA_TYPE_AUDIO) {
        out->keyframe = true;
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
        if (out->has_capture_time) {
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
