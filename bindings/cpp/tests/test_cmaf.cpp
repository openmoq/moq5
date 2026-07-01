#include <moq/cmaf.hpp>
#include "test_support.hpp"

#include <cassert>
#include <cstring>

/* -- Synthetic box helpers (same logic as C tests) ------------------- */

static void wr32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
}

static void wr16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v;
}

static size_t box_hdr(uint8_t *p, uint32_t size, const char *type)
{
    wr32(p, size);
    std::memcpy(p + 4, type, 4);
    return 8;
}

static size_t build_avc_init(uint8_t *buf, uint32_t timescale,
                              uint16_t w, uint16_t h,
                              const uint8_t *avcc, size_t avcc_len)
{
    size_t p = 0;
    p += box_hdr(buf + p, 20, "ftyp");
    std::memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    std::memcpy(buf + p, "isom", 4); p += 4;

    size_t avcc_box_sz = 8 + avcc_len;
    size_t avc1_sz = 8 + 78 + avcc_box_sz;
    size_t stsd_sz = 8 + 8 + avc1_sz;
    size_t stbl_sz = 8 + stsd_sz;
    size_t minf_sz = 8 + stbl_sz;
    size_t mdhd_sz = 8 + 4 + 12;
    size_t mdia_sz = 8 + mdhd_sz + minf_sz;
    size_t trak_sz = 8 + mdia_sz;
    size_t moov_sz = 8 + trak_sz;

    p += box_hdr(buf + p, (uint32_t)moov_sz, "moov");
    p += box_hdr(buf + p, (uint32_t)trak_sz, "trak");
    p += box_hdr(buf + p, (uint32_t)mdia_sz, "mdia");
    p += box_hdr(buf + p, (uint32_t)mdhd_sz, "mdhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, timescale); p += 4;
    p += box_hdr(buf + p, (uint32_t)minf_sz, "minf");
    p += box_hdr(buf + p, (uint32_t)stbl_sz, "stbl");
    p += box_hdr(buf + p, (uint32_t)stsd_sz, "stsd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)avc1_sz, "avc1");
    std::memset(buf + p, 0, 78);
    wr16(buf + p + 24, w);
    wr16(buf + p + 26, h);
    p += 78;
    p += box_hdr(buf + p, (uint32_t)avcc_box_sz, "avcC");
    std::memcpy(buf + p, avcc, avcc_len);
    p += avcc_len;
    return p;
}

static size_t build_aac_init(uint8_t *buf, uint32_t timescale,
                              uint16_t samplerate, uint16_t channels,
                              const uint8_t *asc, size_t asc_len)
{
    size_t p = 0;
    p += box_hdr(buf + p, 20, "ftyp");
    std::memcpy(buf + p, "isom", 4); p += 4;
    wr32(buf + p, 0); p += 4;
    std::memcpy(buf + p, "isom", 4); p += 4;

    size_t dec_spec = 2 + asc_len;
    size_t dec_cfg = 2 + 13 + dec_spec;
    size_t es_desc = 2 + 3 + dec_cfg;
    size_t esds_body = 4 + es_desc;
    size_t esds_sz = 8 + esds_body;
    size_t mp4a_sz = 8 + 28 + esds_sz;
    size_t stsd_sz = 8 + 8 + mp4a_sz;
    size_t stbl_sz = 8 + stsd_sz;
    size_t minf_sz = 8 + stbl_sz;
    size_t mdhd_sz = 8 + 4 + 12;
    size_t mdia_sz = 8 + mdhd_sz + minf_sz;
    size_t trak_sz = 8 + mdia_sz;
    size_t moov_sz = 8 + trak_sz;

    p += box_hdr(buf + p, (uint32_t)moov_sz, "moov");
    p += box_hdr(buf + p, (uint32_t)trak_sz, "trak");
    p += box_hdr(buf + p, (uint32_t)mdia_sz, "mdia");
    p += box_hdr(buf + p, (uint32_t)mdhd_sz, "mdhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, timescale); p += 4;
    p += box_hdr(buf + p, (uint32_t)minf_sz, "minf");
    p += box_hdr(buf + p, (uint32_t)stbl_sz, "stbl");
    p += box_hdr(buf + p, (uint32_t)stsd_sz, "stsd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)mp4a_sz, "mp4a");
    std::memset(buf + p, 0, 28);
    wr16(buf + p + 8, channels);
    wr16(buf + p + 24, samplerate);
    p += 28;
    p += box_hdr(buf + p, (uint32_t)esds_sz, "esds");
    wr32(buf + p, 0); p += 4;
    buf[p++] = 0x03; buf[p++] = (uint8_t)(es_desc - 2);
    wr16(buf + p, 0); p += 2;
    buf[p++] = 0;
    buf[p++] = 0x04; buf[p++] = (uint8_t)(dec_cfg - 2);
    buf[p++] = 0x40; buf[p++] = 0x15;
    std::memset(buf + p, 0, 11); p += 11;
    buf[p++] = 0x05; buf[p++] = (uint8_t)asc_len;
    std::memcpy(buf + p, asc, asc_len);
    p += asc_len;
    return p;
}

static size_t build_fragment(uint8_t *buf, size_t cap, uint64_t base_time,
                              uint32_t trun_flags,
                              const moq::cmaf::sample *samples,
                              size_t sample_count,
                              const uint8_t *mdat_payload, size_t mdat_len)
{
    size_t per = 0;
    if (trun_flags & 0x100) per += 4;
    if (trun_flags & 0x200) per += 4;
    if (trun_flags & 0x400) per += 4;
    if (trun_flags & 0x800) per += 4;
    size_t trun_sz = 8 + 8 + sample_count * per;
    size_t tfdt_sz = 8 + 4 + 8;
    size_t tfhd_sz = 8 + 8;
    size_t traf_sz = 8 + tfhd_sz + tfdt_sz + trun_sz;
    size_t moof_sz = 8 + traf_sz;

    /* Fail loudly if the caller's buffer cannot hold the moof + mdat box rather
     * than silently writing past it (caught by ASan, not by a plain run). */
    assert(moof_sz + 8 + mdat_len <= cap &&
           "build_fragment: destination buffer too small");

    size_t p = 0;
    p += box_hdr(buf + p, (uint32_t)moof_sz, "moof");
    p += box_hdr(buf + p, (uint32_t)traf_sz, "traf");
    p += box_hdr(buf + p, (uint32_t)tfhd_sz, "tfhd");
    wr32(buf + p, 0); p += 4;
    wr32(buf + p, 1); p += 4;
    p += box_hdr(buf + p, (uint32_t)tfdt_sz, "tfdt");
    wr32(buf + p, 0x01000000); p += 4;
    wr32(buf + p, (uint32_t)(base_time >> 32)); p += 4;
    wr32(buf + p, (uint32_t)base_time); p += 4;
    p += box_hdr(buf + p, (uint32_t)trun_sz, "trun");
    wr32(buf + p, trun_flags); p += 4;
    wr32(buf + p, (uint32_t)sample_count); p += 4;
    for (size_t i = 0; i < sample_count; i++) {
        if (trun_flags & 0x100) { wr32(buf + p, samples[i].duration); p += 4; }
        if (trun_flags & 0x200) { wr32(buf + p, samples[i].size); p += 4; }
        if (trun_flags & 0x400) { wr32(buf + p, samples[i].flags); p += 4; }
        if (trun_flags & 0x800) { wr32(buf + p, (uint32_t)samples[i].composition_offset); p += 4; }
    }
    p += box_hdr(buf + p, (uint32_t)(8 + mdat_len), "mdat");
    std::memcpy(buf + p, mdat_payload, mdat_len);
    p += mdat_len;
    return p;
}

/* ================================================================== */

int main()
{
    int failures = 0;

    // Parse AVC init
    {
        uint8_t avcc[] = {0x01, 0x64, 0x00, 0x1E, 0xFF};
        uint8_t buf[512];
        size_t len = build_avc_init(buf, 90000, 1920, 1080, avcc, sizeof(avcc));

        auto r = moq::cmaf::parse_init(moq::bytes_view(buf, len));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->codec == moq::cmaf::codec_kind::avc);
        MOQ_CHECK(r->timescale == 90000);
        MOQ_CHECK(r->width == 1920);
        MOQ_CHECK(r->height == 1080);
        MOQ_CHECK(r->codec_config.size() == sizeof(avcc));
        MOQ_CHECK(std::memcmp(r->codec_config.data(), avcc, sizeof(avcc)) == 0);
        // Borrowed: pointer into input
        MOQ_CHECK(r->codec_config.data() >= buf);
        MOQ_CHECK(r->codec_config.data() < buf + len);
    }

    // Parse AAC init
    {
        uint8_t asc[] = {0x11, 0x90};
        uint8_t buf[512];
        size_t len = build_aac_init(buf, 48000, 48000, 2, asc, sizeof(asc));

        auto r = moq::cmaf::parse_init(moq::bytes_view(buf, len));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->codec == moq::cmaf::codec_kind::aac);
        MOQ_CHECK(r->timescale == 48000);
        MOQ_CHECK(r->samplerate == 48000);
        MOQ_CHECK(r->channel_count == 2);
        MOQ_CHECK(r->codec_config.size() == sizeof(asc));
    }

    // Parse fragment with 1 sample
    {
        moq::cmaf::sample in[] = {{1000, 500, 0, 0}};
        uint8_t payload[] = {0xCA, 0xFE};
        uint8_t buf[256];
        size_t len = build_fragment(buf, sizeof(buf), 90000, 0x300, in, 1,
                                     payload, sizeof(payload));

        moq::cmaf::sample out[8];
        auto r = moq::cmaf::parse_fragment(
            moq::bytes_view(buf, len), std::span(out));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->base_decode_time.has_value());
        MOQ_CHECK(*r->base_decode_time == 90000);
        MOQ_CHECK(r->samples.size() == 1);
        MOQ_CHECK(r->samples[0].duration == 1000);
        MOQ_CHECK(r->samples[0].size == 500);
        MOQ_CHECK(r->mdat.size() == sizeof(payload));
        MOQ_CHECK(std::memcmp(r->mdat.data(), payload, sizeof(payload)) == 0);
        // mdat borrowed from input
        MOQ_CHECK(r->mdat.data() >= buf);
        MOQ_CHECK(r->mdat.data() < buf + len);
    }

    // Parse fragment with caller span (3 samples)
    {
        moq::cmaf::sample in[] = {
            {1000, 100, 0x02000000, 50},
            {1000, 200, 0, -25},
            {1000, 150, 0, 0},
        };
        uint8_t payload[450];
        std::memset(payload, 0xAB, sizeof(payload));
        uint8_t buf[640];   /* moof(116) + mdat(8) + payload(450) = 574 */
        size_t len = build_fragment(buf, sizeof(buf), 0, 0xF00, in, 3,
                                     payload, sizeof(payload));

        moq::cmaf::sample out[8];
        auto r = moq::cmaf::parse_fragment(
            moq::bytes_view(buf, len), std::span(out));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->samples.size() == 3);
        MOQ_CHECK(r->samples[0].flags == 0x02000000);
        MOQ_CHECK(r->samples[0].composition_offset == 50);
        MOQ_CHECK(r->samples[1].composition_offset == -25);
    }

    // ERR_BUFFER when span too small, with required_count
    {
        moq::cmaf::sample in[] = {{100, 10, 0, 0}, {100, 10, 0, 0}, {100, 10, 0, 0}};
        uint8_t payload[] = {0x00};
        uint8_t buf[256];
        size_t len = build_fragment(buf, sizeof(buf), 0, 0x300, in, 3,
                                     payload, sizeof(payload));

        moq::cmaf::sample out[2]; // too small
        size_t needed = 0;
        auto r = moq::cmaf::parse_fragment(
            moq::bytes_view(buf, len), std::span(out), &needed);
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::buffer);
        MOQ_CHECK(needed == 3);
    }

    // ERR_BUFFER without required_count pointer
    {
        moq::cmaf::sample in[] = {{100, 10, 0, 0}, {100, 10, 0, 0}};
        uint8_t payload[] = {0x00};
        uint8_t buf[256];
        size_t len = build_fragment(buf, sizeof(buf), 0, 0x300, in, 2,
                                     payload, sizeof(payload));

        moq::cmaf::sample out[1];
        auto r = moq::cmaf::parse_fragment(
            moq::bytes_view(buf, len), std::span(out));
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::buffer);
    }

    // Malformed input returns error
    {
        uint8_t bad[] = {0x00, 0x00, 0x00, 0x04, 'm', 'o', 'o', 'v'};
        auto r = moq::cmaf::parse_init(moq::bytes_view(bad, sizeof(bad)));
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::protocol);
    }

    // codec_config borrowed from input
    {
        uint8_t avcc[] = {0xAA, 0xBB, 0xCC};
        uint8_t buf[512];
        size_t len = build_avc_init(buf, 30000, 640, 480, avcc, sizeof(avcc));

        auto r = moq::cmaf::parse_init(moq::bytes_view(buf, len));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->codec_config.data() >= buf);
        MOQ_CHECK(r->codec_config.data() + r->codec_config.size() <= buf + len);
    }

    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
