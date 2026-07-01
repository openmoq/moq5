#ifndef MOQ_LOC_HPP
#define MOQ_LOC_HPP

#include <moq/loc.h>
#include <moq/buffer.hpp>
#include <moq/result.hpp>
#include <moq/types.hpp>

namespace moq::loc {

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
    video_frame_marking video_frame_marking{};

    bool        has_audio_level = false;
    audio_level audio_level{};

    bool       has_video_config = false;
    bytes_view video_config{};
};

inline result<headers> parse(profile p, bytes_view properties)
{
    moq_loc_headers_t ch;
    moq_result_t rc = moq_loc_parse(
        static_cast<moq_loc_profile_t>(p), properties.raw(), &ch);
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

inline result<buffer> encode(profile p, const headers &h,
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
    moq_result_t rc = moq_loc_encode(alloc,
        static_cast<moq_loc_profile_t>(p), &ch, &raw);
    if (rc < 0)
        return errc_from_result(rc);

    return buffer::adopt(raw);
}

} // namespace moq::loc

#endif // MOQ_LOC_HPP
