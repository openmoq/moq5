#include <moq/msf_media.h>
#include "msf_internal.h"
#include <moq/loc.h>
#include <stddef.h>
#include <string.h>

static bool bytes_eq(moq_bytes_t b, const char *s)
{
    size_t len = strlen(s);
    return b.len == len && memcmp(b.data, s, len) == 0;
}

static bool codec_has_prefix(moq_bytes_t codec, const char *prefix)
{
    size_t plen = strlen(prefix);
    return codec.len >= plen && memcmp(codec.data, prefix, plen) == 0;
}

/* Classify media type from a codec string prefix (WebCodecs Codec Registry,
 * MSF-01 §5.2.18). Conservative, audio/video only. Returns false when the codec
 * is not a recognized audio/video codec. */
static bool media_type_from_codec(moq_bytes_t codec, moq_media_type_t *out)
{
    static const char *const video[] = {
        "avc1", "avc3", "hev1", "hvc1", "av01", "vp8", "vp09",
    };
    static const char *const audio[] = {
        "mp4a", "opus",
    };
    for (size_t i = 0; i < sizeof(video) / sizeof(video[0]); i++)
        if (codec_has_prefix(codec, video[i])) {
            *out = MOQ_MEDIA_TYPE_VIDEO;
            return true;
        }
    for (size_t i = 0; i < sizeof(audio) / sizeof(audio[0]); i++)
        if (codec_has_prefix(codec, audio[i])) {
            *out = MOQ_MEDIA_TYPE_AUDIO;
            return true;
        }
    return false;
}

/* Reserved MSF roles (Table 4/5) that are NOT audio/video media: an MSF/event
 * timeline, text tracks, or out-of-band publishing tracks. These are never
 * classified as media even if they carry a codec. Other reserved roles
 * (audiodescription, signlanguage) and custom roles (§5.2.6 allows them) are
 * media-capable and fall through to codec classification. */
static bool role_is_non_media(moq_bytes_t role)
{
    static const char *const non_media[] = {
        "mediatimeline", "eventtimeline", "caption",
        "subtitle", "log", "metrics",
    };
    for (size_t i = 0; i < sizeof(non_media) / sizeof(non_media[0]); i++)
        if (bytes_eq(role, non_media[i]))
            return true;
    return false;
}

/* Track roles of draft-gregoire-moq-msfts that name a PSI/SI table or the
 * SCTE-35 stream of an es-packets track: not media. */
static bool role_is_ts_table(moq_bytes_t role)
{
    static const char *const tables[] = {
        "pat", "pmt", "nit", "sdt", "eit", "tdt", "scte35",
    };
    for (size_t i = 0; i < sizeof(tables) / sizeof(tables[0]); i++)
        if (bytes_eq(role, tables[i]))
            return true;
    return false;
}

/* The "mpeg2ts" mapping (draft-gregoire-moq-msfts). out_info and out_init
 * are already initialized by the caller. Packet size remains in the typed
 * catalog extension, not in the frozen track info. */
static moq_result_t msf_mpeg2ts_to_media_info(
    const moq_alloc_t *alloc,
    const moq_msf_track_t *track,
    moq_media_track_info_t *out_info,
    moq_rcbuf_t **out_init_buf)
{
    /* Catalog: a subscriber MUST reject a track that breaks the rules. */
    if (moq_msf_track_mpeg2ts_check(track, NULL) != MOQ_OK)
        return MOQ_ERR_INVAL;
    const moq_msf_mpeg2ts_t *ts = moq_msf_track_mpeg2ts(track);
    if (!ts)
        return MOQ_ERR_INVAL;

    /* A TS track is a program (or one PID of it) whose random access is
     * defined by its video stream; an audio role makes it audio. PSI/SI
     * table roles and the reserved non-media roles are not media. */
    if (track->has_role && bytes_eq(track->role, "audio"))
        out_info->media_type = MOQ_MEDIA_TYPE_AUDIO;
    else if (track->has_role &&
             (role_is_ts_table(track->role) || role_is_non_media(track->role)))
        return MOQ_ERR_INVAL;
    else
        out_info->media_type = MOQ_MEDIA_TYPE_VIDEO;

    out_info->packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    if (track->has_timescale && track->timescale <= UINT32_MAX)
        out_info->timescale = (uint32_t)track->timescale;

    /* The initialization data is the PAT/PMT source packets (Use of MSF
     * Initialization Data). It is not decoder extradata, so codec_config
     * stays empty. Data that is not whole source packets is not surfaced;
     * the track stays usable, since the PSI also travels in band. */
    if (out_init_buf && track->has_init_data && track->init_data.len > 0) {
        if (!alloc)
            return MOQ_ERR_INVAL;
        moq_rcbuf_t *decoded = NULL;
        moq_result_t rc = moq_msf_decode_init_data(alloc, track->init_data,
                                                   &decoded);
        if (rc == MOQ_ERR_NOMEM)
            return rc;
        if (rc == MOQ_OK) {
            if (moq_msf_ts_whole_packets(moq_rcbuf_data(decoded),
                                         moq_rcbuf_len(decoded),
                                         ts->packet_size))
                *out_init_buf = decoded;
            else
                moq_rcbuf_decref(decoded);
        }
    }
    return MOQ_OK;
}

/* struct_size at or above this covers transport_version. Spelled from
 * offsetof rather than assumed, so it tracks the struct. */
#define MSF_TRACK_INFO_VERSION_SIZE \
    (offsetof(moq_media_track_info_t, transport_version) + \
     sizeof(((moq_media_track_info_t *)0)->transport_version))

moq_result_t moq_msf_track_to_media_info_sized(
    const moq_alloc_t *alloc,
    const moq_msf_track_t *track,
    moq_media_track_info_t *out_info,
    size_t out_info_size,
    moq_version_t transport_version,
    moq_cmaf_init_info_t *out_init,
    size_t out_init_size,
    moq_rcbuf_t **out_init_buf)
{
    if (!track || !out_info)
        return MOQ_ERR_INVAL;
    /* An undersized request is REFUSED, never quietly answered with a v0
     * output: a caller that cannot hold the version would silently fall back
     * to the legacy draft-16 contract at parse time. */
    if (out_info_size < MSF_TRACK_INFO_VERSION_SIZE)
        return MOQ_ERR_INVAL;
    /* Zero and unsupported versions are refused rather than downgraded; the
     * LOC table names the drafts whose property codec and revision are
     * registered. */
    if (moq_loc_profile_for_version(transport_version) == 0)
        return MOQ_ERR_INVAL;
    if (out_init && out_init_size < MOQ_CMAF_INIT_INFO_V0_SIZE)
        return MOQ_ERR_INVAL;

    moq_media_track_info_init_sized(out_info, out_info_size);
    out_info->transport_version = transport_version;
    if (out_init)
        moq_cmaf_init_info_init_sized(out_init, out_init_size);
    if (out_init_buf)
        *out_init_buf = NULL;

    if (bytes_eq(track->packaging, "mpeg2ts"))
        return msf_mpeg2ts_to_media_info(alloc, track, out_info, out_init_buf);

    /* Map media type. role is OPTIONAL (MSF-01 §5.2.6) and custom roles are
     * allowed, so role alone cannot gate media classification:
     *   - "video"/"audio"            -> that media type;
     *   - a reserved NON-media role  -> MOQ_ERR_INVAL (never media, even with a
     *     codec): MSF/event timelines, text, log/metrics;
     *   - any other role (custom, or media-like reserved roles such as
     *     audiodescription / signlanguage) OR an absent role -> classify from
     *     the codec prefix (required for audio/video tracks, §5.2.18), and fail
     *     only when no role and no classifiable codec give a media type. */
    if (track->has_role && bytes_eq(track->role, "video")) {
        out_info->media_type = MOQ_MEDIA_TYPE_VIDEO;
    } else if (track->has_role && bytes_eq(track->role, "audio")) {
        out_info->media_type = MOQ_MEDIA_TYPE_AUDIO;
    } else if (track->has_role && role_is_non_media(track->role)) {
        return MOQ_ERR_INVAL;
    } else if (!(track->has_codec &&
                 media_type_from_codec(track->codec, &out_info->media_type))) {
        return MOQ_ERR_INVAL;
    }

    /* Map packaging. */
    if (bytes_eq(track->packaging, "cmaf"))
        out_info->packaging = MOQ_MEDIA_PACKAGING_CMAF;
    else if (bytes_eq(track->packaging, "loc"))
        out_info->packaging = MOQ_MEDIA_PACKAGING_RAW;
    else
        return MOQ_ERR_INVAL;

    /* Derive timescale. */
    if (out_info->packaging == MOQ_MEDIA_PACKAGING_CMAF) {
        bool has_init = (track->has_init_data && track->init_data.len > 0);

        if (has_init) {
            if (!alloc)
                return MOQ_ERR_INVAL;
            if (out_init && !out_init_buf)
                return MOQ_ERR_INVAL;

            moq_rcbuf_t *decoded = NULL;
            moq_result_t rc = moq_msf_decode_init_data(
                alloc, track->init_data, &decoded);
            if (rc < 0)
                return rc;

            moq_cmaf_init_info_t parsed;
            moq_cmaf_init_info_init(&parsed);
            moq_bytes_t raw = {
                moq_rcbuf_data(decoded),
                moq_rcbuf_len(decoded)
            };
            rc = moq_cmaf_parse_init_sized(raw, &parsed, sizeof(parsed));
            if (rc < 0) {
                moq_rcbuf_decref(decoded);
                return rc;
            }

            out_info->timescale = parsed.timescale;

            if (out_init) {
                /* The caller's record is out_init_size bytes, which may be
                 * smaller than this build's: hand over only its prefix. */
                size_t n = out_init_size < sizeof(parsed) ?
                           out_init_size : sizeof(parsed);
                memcpy(out_init, &parsed, n);
                out_init->struct_size = (uint32_t)n;
                *out_init_buf = decoded;
            } else {
                moq_rcbuf_decref(decoded);
            }
        } else {
            /* CMAF without initData. */
            if (out_init)
                return MOQ_ERR_INVAL;
            if (track->has_timescale && track->timescale <= UINT32_MAX)
                out_info->timescale = (uint32_t)track->timescale;
            else
                return MOQ_ERR_INVAL;
        }
    } else {
        /* Non-CMAF (RAW/LOC): use the MSF timescale if present, 0 otherwise. */
        if (track->has_timescale && track->timescale <= UINT32_MAX)
            out_info->timescale = (uint32_t)track->timescale;

        /* The catalog initData for a RAW/LOC track is the encoder's decoder
         * config (extradata) verbatim -- there is no container to parse. Decode
         * it (when the caller wants the buffer) and expose it as codec_config so
         * consumers read decoder extradata from codec_config uniformly across
         * packagings; out_init_buf owns the bytes codec_config borrows. */
        if (out_init_buf && track->has_init_data && track->init_data.len > 0) {
            if (!alloc)
                return MOQ_ERR_INVAL;
            moq_rcbuf_t *decoded = NULL;
            moq_result_t rc = moq_msf_decode_init_data(
                alloc, track->init_data, &decoded);
            if (rc < 0)
                return rc;
            *out_init_buf = decoded;
            if (out_init) {
                out_init->codec_config = (moq_bytes_t){
                    moq_rcbuf_data(decoded), moq_rcbuf_len(decoded) };
            }
        }
    }

    return MOQ_OK;
}
