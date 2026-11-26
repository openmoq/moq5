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

// ISO/IEC 14496-12 8.8.3 TrackExtendsBox defaults.
struct trex_defaults {
    uint32_t sample_description_index = 0;
    uint32_t sample_duration = 0;   // timescale units
    uint32_t sample_size = 0;       // bytes
    uint32_t sample_flags = 0;      // sample_flags

    bool operator==(const trex_defaults &) const = default;
};

// ISO/IEC 23001-7 8.2 TrackEncryptionBox version-1 pattern: encrypted and
// skipped 16-byte blocks.
struct encryption_pattern {
    uint8_t crypt_byte_block = 0;
    uint8_t skip_byte_block = 0;

    bool operator==(const encryption_pattern &) const = default;
};

struct init_info {
    codec_kind codec      = codec_kind::unknown;
    uint32_t   timescale   = 0;
    uint32_t   width       = 0;
    uint32_t   height      = 0;
    uint32_t   samplerate  = 0;
    uint32_t   channel_count = 0;
    bytes_view codec_config;
    // The sample entry's format, (a<<24)|(b<<16)|(c<<8)|d: a clear entry's
    // own type, an encrypted entry's frma original format. Any value,
    // including zero, is reported as read; nullopt when the library did not
    // report it.
    std::optional<uint32_t> sample_entry;

    // The header facts. Every one of them is nullopt when the library did
    // not report the block (a record stamped short of
    // MOQ_CMAF_INIT_INFO_SIZE_THROUGH_CONSTANT_IV).
    //
    // trak boxes in moov: a CMAF header has exactly one; 0 when moov's
    // children do not parse to its end.
    std::optional<uint32_t> trak_count;
    // The mvex/trex of the header's track; also nullopt when there is none.
    std::optional<trex_defaults> trex;
    // A version-1 tenc's pattern; also nullopt for a clear track or a
    // version-0 tenc.
    std::optional<encryption_pattern> pattern;
    // The tenc default constant IV (8 or 16 bytes), borrowed from the input;
    // also nullopt unless the track is protected with no per-sample IV and
    // the box carries a well-formed one.
    std::optional<bytes_view> constant_iv;
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

namespace detail {

// The C++ view of a record the library stamped: each appended field is read
// only when the stamp covers its whole extent.
inline init_info init_info_from(const moq_cmaf_init_info_t &ci)
{
    init_info out;
    out.codec         = static_cast<codec_kind>(ci.codec_kind);
    out.timescale     = ci.timescale;
    out.width         = ci.width;
    out.height        = ci.height;
    out.samplerate    = ci.samplerate;
    out.channel_count = ci.channel_count;
    out.codec_config  = bytes_view(ci.codec_config);
    if (ci.struct_size >= MOQ_CMAF_INIT_INFO_SIZE_THROUGH_SAMPLE_ENTRY)
        out.sample_entry = ci.sample_entry;
    if (ci.struct_size >= MOQ_CMAF_INIT_INFO_SIZE_THROUGH_CONSTANT_IV) {
        out.trak_count = ci.trak_count;
        if (ci.has_trex) {
            trex_defaults t;
            t.sample_description_index = ci.trex_default_sample_description_index;
            t.sample_duration          = ci.trex_default_sample_duration;
            t.sample_size              = ci.trex_default_sample_size;
            t.sample_flags             = ci.trex_default_sample_flags;
            out.trex = t;
        }
        if (ci.has_cenc && ci.tenc_version == 1) {
            encryption_pattern p;
            p.crypt_byte_block = ci.default_crypt_byte_block;
            p.skip_byte_block  = ci.default_skip_byte_block;
            out.pattern = p;
        }
        if (ci.default_constant_iv.len > 0)
            out.constant_iv = bytes_view(ci.default_constant_iv);
    }
    return out;
}

} // namespace detail

inline result<init_info> parse_init(bytes_view data)
{
    moq_cmaf_init_info_t ci;
    moq_cmaf_init_info_init(&ci);
    moq_result_t rc = moq_cmaf_parse_init_sized(data.raw(), &ci, sizeof(ci));
    if (rc < 0)
        return errc_from_result(rc);
    return detail::init_info_from(ci);
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
