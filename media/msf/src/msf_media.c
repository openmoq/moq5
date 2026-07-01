#include <moq/msf_media.h>
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

moq_result_t moq_msf_track_to_media_info(
    const moq_alloc_t *alloc,
    const moq_msf_track_t *track,
    moq_media_track_info_t *out_info,
    moq_cmaf_init_info_t *out_init,
    moq_rcbuf_t **out_init_buf)
{
    if (!track || !out_info)
        return MOQ_ERR_INVAL;

    moq_media_track_info_init(out_info);

    if (out_init)
        moq_cmaf_init_info_init(out_init);
    if (out_init_buf)
        *out_init_buf = NULL;

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
            rc = moq_cmaf_parse_init(raw, &parsed);
            if (rc < 0) {
                moq_rcbuf_decref(decoded);
                return rc;
            }

            out_info->timescale = parsed.timescale;

            if (out_init) {
                *out_init = parsed;
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
