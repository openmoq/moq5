#ifndef MOQ_CMAF_HPP
#define MOQ_CMAF_HPP

#include <moq/cmaf.h>
#include <moq/result.hpp>
#include <moq/types.hpp>

#include <cstddef>
#include <optional>
#include <span>

namespace moq::cmaf {

enum class codec_kind {
    unknown = MOQ_CMAF_CODEC_UNKNOWN,
    avc     = MOQ_CMAF_CODEC_AVC,
    hevc    = MOQ_CMAF_CODEC_HEVC,
    av1     = MOQ_CMAF_CODEC_AV1,
    aac     = MOQ_CMAF_CODEC_AAC,
    opus    = MOQ_CMAF_CODEC_OPUS,
};

struct init_info {
    codec_kind codec      = codec_kind::unknown;
    uint32_t   timescale   = 0;
    uint32_t   width       = 0;
    uint32_t   height      = 0;
    uint32_t   samplerate  = 0;
    uint32_t   channel_count = 0;
    bytes_view codec_config;
};

using sample = moq_cmaf_sample_t;

struct fragment_info {
    std::optional<uint64_t> base_decode_time;
    uint32_t default_sample_duration = 0;
    uint32_t default_sample_size     = 0;
    uint32_t default_sample_flags    = 0;
    std::span<const sample> samples;
    bytes_view mdat;
};

inline result<init_info> parse_init(bytes_view data)
{
    moq_cmaf_init_info_t ci;
    moq_cmaf_init_info_init(&ci);
    moq_result_t rc = moq_cmaf_parse_init(data.raw(), &ci);
    if (rc < 0)
        return errc_from_result(rc);

    init_info out;
    out.codec         = static_cast<codec_kind>(ci.codec_kind);
    out.timescale     = ci.timescale;
    out.width         = ci.width;
    out.height        = ci.height;
    out.samplerate    = ci.samplerate;
    out.channel_count = ci.channel_count;
    out.codec_config  = bytes_view(ci.codec_config);
    return out;
}

inline result<fragment_info> parse_fragment(bytes_view data,
                                            std::span<sample> buf,
                                            size_t *required_count = nullptr)
{
    moq_cmaf_fragment_info_t cf;
    moq_cmaf_fragment_info_init(&cf, buf.data(), buf.size());

    moq_result_t rc = moq_cmaf_parse_fragment(data.raw(), &cf);
    if (rc < 0) {
        if (rc == MOQ_ERR_BUFFER && required_count)
            *required_count = cf.sample_count;
        return errc_from_result(rc);
    }

    fragment_info out;
    if (cf.has_base_decode_time)
        out.base_decode_time = cf.base_decode_time;
    out.default_sample_duration = cf.default_sample_duration;
    out.default_sample_size     = cf.default_sample_size;
    out.default_sample_flags    = cf.default_sample_flags;
    out.samples = std::span<const sample>(buf.data(), cf.sample_count);
    out.mdat    = bytes_view(cf.mdat);
    return out;
}

} // namespace moq::cmaf

#endif // MOQ_CMAF_HPP
