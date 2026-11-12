#ifndef MOQ_LOC_HPP
#define MOQ_LOC_HPP

#include <moq/loc.h>
#include <moq/buffer.hpp>
#include <moq/result.hpp>
#include <moq/types.hpp>

#include <array>
#include <optional>
#include <string_view>

namespace moq::loc {

/*
 * The negotiated transport draft selects the Key-Value-Pair integer codec
 * the LOC properties are written in; see <moq/loc.h>. It is spelled with
 * the C type deliberately -- a parallel C++ enum would be a second name
 * for one wire fact and could drift from it. There is NO default: every
 * call names its version, and an unsupported one is errc::inval.
 */
using transport_version = moq_version_t;

enum class profile {
    loc01 = MOQ_LOC_PROFILE_01,
    loc02 = MOQ_LOC_PROFILE_02,
};

struct video_frame_marking {
    bool    start_of_frame  = false;
    bool    end_of_frame    = false;
    bool    independent     = false;
    bool    discardable     = false;
    bool    base_layer_sync = false;
    uint8_t temporal_id     = 0;
    bool    has_layer_id    = false;
    uint8_t layer_id        = 0;
};

struct audio_level {
    bool    voice_activity = false;
    uint8_t level          = 0;
};

struct headers {
    bool     has_timestamp          = false;
    uint64_t timestamp              = 0;

    bool     has_timescale          = false;
    uint64_t timescale              = 0;

    bool                has_video_frame_marking = false;
    moq::loc::video_frame_marking video_frame_marking{};

    bool        has_audio_level = false;
    moq::loc::audio_level audio_level{};

    bool       has_video_config = false;
    bytes_view video_config{};
};

inline result<headers> parse(transport_version tv, profile p,
                             bytes_view properties)
{
    moq_loc_headers_t ch;
    moq_result_t rc = moq_loc_parse(
        tv, static_cast<moq_loc_profile_t>(p), properties.raw(), &ch);
    if (rc < 0)
        return errc_from_result(rc);

    headers h;
    h.has_timestamp = ch.has_timestamp;
    h.timestamp     = ch.timestamp;
    h.has_timescale = ch.has_timescale;
    h.timescale     = ch.timescale;

    if (ch.has_video_frame_marking) {
        h.has_video_frame_marking                  = true;
        h.video_frame_marking.start_of_frame       = ch.video_frame_marking.start_of_frame;
        h.video_frame_marking.end_of_frame         = ch.video_frame_marking.end_of_frame;
        h.video_frame_marking.independent           = ch.video_frame_marking.independent;
        h.video_frame_marking.discardable           = ch.video_frame_marking.discardable;
        h.video_frame_marking.base_layer_sync       = ch.video_frame_marking.base_layer_sync;
        h.video_frame_marking.temporal_id           = ch.video_frame_marking.temporal_id;
        h.video_frame_marking.has_layer_id          = ch.video_frame_marking.has_layer_id;
        h.video_frame_marking.layer_id              = ch.video_frame_marking.layer_id;
    }

    if (ch.has_audio_level) {
        h.has_audio_level              = true;
        h.audio_level.voice_activity   = ch.audio_level.voice_activity;
        h.audio_level.level            = ch.audio_level.level;
    }

    h.has_video_config = ch.has_video_config;
    h.video_config     = bytes_view(ch.video_config);

    return h;
}

inline result<buffer> encode(transport_version tv, profile p, const headers &h,
                              const moq_alloc_t *alloc = moq_alloc_default())
{
    moq_loc_headers_t ch;
    moq_loc_headers_init(&ch);

    ch.has_timestamp = h.has_timestamp;
    ch.timestamp     = h.timestamp;
    ch.has_timescale = h.has_timescale;
    ch.timescale     = h.timescale;

    if (h.has_video_frame_marking) {
        ch.has_video_frame_marking                  = true;
        ch.video_frame_marking.start_of_frame       = h.video_frame_marking.start_of_frame;
        ch.video_frame_marking.end_of_frame         = h.video_frame_marking.end_of_frame;
        ch.video_frame_marking.independent           = h.video_frame_marking.independent;
        ch.video_frame_marking.discardable           = h.video_frame_marking.discardable;
        ch.video_frame_marking.base_layer_sync       = h.video_frame_marking.base_layer_sync;
        ch.video_frame_marking.temporal_id           = h.video_frame_marking.temporal_id;
        ch.video_frame_marking.has_layer_id          = h.video_frame_marking.has_layer_id;
        ch.video_frame_marking.layer_id              = h.video_frame_marking.layer_id;
    }

    if (h.has_audio_level) {
        ch.has_audio_level              = true;
        ch.audio_level.voice_activity   = h.audio_level.voice_activity;
        ch.audio_level.level            = h.audio_level.level;
    }

    if (h.has_video_config) {
        ch.has_video_config = true;
        ch.video_config     = h.video_config.raw();
    }

    moq_rcbuf_t *raw = nullptr;
    moq_result_t rc = moq_loc_encode(alloc, tv,
        static_cast<moq_loc_profile_t>(p), &ch, &raw);
    if (rc < 0)
        return errc_from_result(rc);

    return buffer::adopt(raw);
}

/*
 * Lossless LOC companion metadata (moq_loc_parse_metadata_ex /
 * moq_loc_encode_metadata_ex). The overloads that take no profile use the
 * compatibility selection (moq_loc_profile_for_version: LOC-01 on drafts 16
 * and 18, LOC-04 on draft 22); name MOQ_LOC_PROFILE_05 to read or write the
 * LOC-05 Codec String. Values are raw: timestamp ticks and timescale units,
 * the marking payload's 1..4 bytes verbatim, the audio level byte with its V
 * bit. The configs and the Codec String borrow the parsed property bytes; on
 * encode they are borrowed for the call. A Codec String is its exact UTF-8
 * bytes: never normalized, trimmed or NUL-terminated. Encoding one under a
 * profile without it (LOC-01, LOC-04, or a no-profile overload) is refused,
 * not dropped.
 */
struct metadata {
    std::optional<uint64_t>   timestamp    = {};
    std::optional<uint64_t>   timescale    = {};
    uint8_t                   marking_len  = 0;   /* 0: no marking */
    std::array<uint8_t, 4>    marking      = {};
    std::optional<uint8_t>    audio_level  = {};
    std::optional<bytes_view> video_config = {};
    std::optional<bytes_view> audio_config = {};
    std::optional<std::string_view> codec_string = {};   /* LOC-05 only */
};

inline result<metadata> parse_metadata(transport_version tv, moq_loc_profile_t profile,
                                       bytes_view properties)
{
    moq_loc_metadata_t c;
    moq_loc_metadata_init_sized(&c, sizeof(c));
    moq_loc_ext_t e;
    moq_loc_ext_init_sized(&e, sizeof(e));
    moq_result_t rc = moq_loc_parse_metadata_ex(tv, profile, properties.raw(),
                                                &c, sizeof(c), &e, sizeof(e));
    if (rc < 0)
        return errc_from_result(rc);
    metadata m;
    if (c.present & MOQ_LOC_METADATA_HAS_TIMESTAMP) m.timestamp = c.timestamp;
    if (c.present & MOQ_LOC_METADATA_HAS_TIMESCALE) m.timescale = c.timescale;
    if (c.present & MOQ_LOC_METADATA_HAS_MARKING) {
        m.marking_len = c.marking_len;
        for (size_t i = 0; i < c.marking_len && i < m.marking.size(); i++)
            m.marking[i] = c.marking[i];
    }
    if (c.present & MOQ_LOC_METADATA_HAS_AUDIO_LEVEL) m.audio_level = c.audio_level;
    if (c.present & MOQ_LOC_METADATA_HAS_VIDEO_CONFIG) m.video_config = bytes_view(c.video_config);
    if (c.present & MOQ_LOC_METADATA_HAS_AUDIO_CONFIG) m.audio_config = bytes_view(c.audio_config);
    if (e.present & MOQ_LOC_EXT_HAS_CODEC_STRING)
        m.codec_string = std::string_view(
            reinterpret_cast<const char *>(e.codec_string.data), e.codec_string.len);
    return m;
}

inline result<metadata> parse_metadata(transport_version tv, bytes_view properties)
{
    moq_loc_profile_t p = moq_loc_profile_for_version(tv);
    if (p == 0)
        return errc::invalid;
    return parse_metadata(tv, p, properties);
}

inline result<buffer> encode_metadata(transport_version tv, moq_loc_profile_t profile,
                                      const metadata &m,
                                      const moq_alloc_t *alloc = moq_alloc_default())
{
    if (m.marking_len > m.marking.size())
        return errc::invalid;
    moq_loc_metadata_t c;
    moq_loc_metadata_init_sized(&c, sizeof(c));
    if (m.timestamp) { c.present |= MOQ_LOC_METADATA_HAS_TIMESTAMP; c.timestamp = *m.timestamp; }
    if (m.timescale) { c.present |= MOQ_LOC_METADATA_HAS_TIMESCALE; c.timescale = *m.timescale; }
    if (m.marking_len) {
        c.present |= MOQ_LOC_METADATA_HAS_MARKING;
        c.marking_len = m.marking_len;
        for (size_t i = 0; i < m.marking_len; i++) c.marking[i] = m.marking[i];
    }
    if (m.audio_level) { c.present |= MOQ_LOC_METADATA_HAS_AUDIO_LEVEL; c.audio_level = *m.audio_level; }
    if (m.video_config) { c.present |= MOQ_LOC_METADATA_HAS_VIDEO_CONFIG; c.video_config = m.video_config->raw(); }
    if (m.audio_config) { c.present |= MOQ_LOC_METADATA_HAS_AUDIO_CONFIG; c.audio_config = m.audio_config->raw(); }
    moq_loc_ext_t e;
    moq_loc_ext_init_sized(&e, sizeof(e));
    if (m.codec_string) {
        e.present |= MOQ_LOC_EXT_HAS_CODEC_STRING;
        e.codec_string = moq_bytes_t{
            reinterpret_cast<const uint8_t *>(m.codec_string->data()),
            m.codec_string->size()};
    }
    moq_rcbuf_t *raw = nullptr;
    moq_result_t rc = moq_loc_encode_metadata_ex(alloc, tv, profile, &c,
                                                 m.codec_string ? &e : nullptr, &raw);
    if (rc < 0)
        return errc_from_result(rc);
    return buffer::adopt(raw);
}

inline result<buffer> encode_metadata(transport_version tv, const metadata &m,
                                      const moq_alloc_t *alloc = moq_alloc_default())
{
    moq_loc_profile_t p = moq_loc_profile_for_version(tv);
    if (p == 0)
        return errc::invalid;
    return encode_metadata(tv, p, m, alloc);
}

} // namespace moq::loc

#endif // MOQ_LOC_HPP
