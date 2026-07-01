#include <moq/loc.hpp>
#include "test_support.hpp"

#include <cstring>

int main()
{
    int failures = 0;

    // Parse empty LOC-01 — success, no fields
    {
        auto r = moq::loc::parse(moq::loc::profile::loc01, {});
        MOQ_CHECK(r.ok());
        MOQ_CHECK(!r->has_timestamp);
        MOQ_CHECK(!r->has_timescale);
        MOQ_CHECK(!r->has_video_frame_marking);
        MOQ_CHECK(!r->has_audio_level);
        MOQ_CHECK(!r->has_video_config);
    }

    // Encode empty LOC-01 — success, empty/default buffer
    {
        moq::loc::headers h;
        auto r = moq::loc::encode(moq::loc::profile::loc01, h);
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->empty());
    }

    // Timestamp LOC-01 encode byte oracle matches C test
    {
        moq::loc::headers h;
        h.has_timestamp = true;
        h.timestamp     = 1000000;

        auto r = moq::loc::encode(moq::loc::profile::loc01, h);
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->size() == 5);
        auto d = r->data();
        MOQ_CHECK(d[0] == 0x02);
        MOQ_CHECK(d[1] == 0x80);
        MOQ_CHECK(d[2] == 0x0F);
        MOQ_CHECK(d[3] == 0x42);
        MOQ_CHECK(d[4] == 0x40);
    }

    // Audio level LOC-01 encode byte oracle matches C test
    {
        moq::loc::headers h;
        h.has_audio_level          = true;
        h.audio_level.voice_activity = true;
        h.audio_level.level          = 30;

        auto r = moq::loc::encode(moq::loc::profile::loc01, h);
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->size() == 3);
        auto d = r->data();
        MOQ_CHECK(d[0] == 0x06);
        MOQ_CHECK(d[1] == 0x40);
        MOQ_CHECK(d[2] == 0x9E);
    }

    // Parse video_config returns borrowed bytes matching input
    {
        // Build wire: delta=0x0d, length=3, data={0xAA,0xBB,0xCC}
        uint8_t wire[] = {0x0d, 0x03, 0xAA, 0xBB, 0xCC};
        auto r = moq::loc::parse(moq::loc::profile::loc01,
                                  moq::bytes_view(wire, sizeof(wire)));
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->has_video_config);
        MOQ_CHECK(r->video_config.size() == 3);
        MOQ_CHECK(r->video_config.data() == wire + 2);
        MOQ_CHECK(r->video_config.data()[0] == 0xAA);
        MOQ_CHECK(r->video_config.data()[2] == 0xCC);
    }

    // LOC-02 parse returns errc::invalid
    {
        auto r = moq::loc::parse(moq::loc::profile::loc02, {});
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::invalid);
    }

    // LOC-02 encode returns errc::invalid
    {
        moq::loc::headers h;
        h.has_timestamp = true;
        h.timestamp     = 48000;
        auto r = moq::loc::encode(moq::loc::profile::loc02, h);
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::invalid);
    }

    // Oversized audio level from raw bytes returns errc::protocol
    {
        // delta=6, value=0x100 (2-byte QUIC varint: 0x41 0x00)
        uint8_t wire[] = {0x06, 0x41, 0x00};
        auto r = moq::loc::parse(moq::loc::profile::loc01,
                                  moq::bytes_view(wire, sizeof(wire)));
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::protocol);
    }

    // Oversized video frame marking from raw bytes returns errc::protocol
    {
        // delta=4, value=0x10000 (4-byte QUIC varint: 0x80 0x01 0x00 0x00)
        uint8_t wire[] = {0x04, 0x80, 0x01, 0x00, 0x00};
        auto r = moq::loc::parse(moq::loc::profile::loc01,
                                  moq::bytes_view(wire, sizeof(wire)));
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::protocol);
    }

    // Encode with video_config uses caller bytes and round-trips
    {
        uint8_t cfg_data[] = {0x01, 0x64, 0x00, 0x1E};

        moq::loc::headers h;
        h.has_video_config = true;
        h.video_config     = moq::bytes_view(cfg_data, sizeof(cfg_data));

        auto enc = moq::loc::encode(moq::loc::profile::loc01, h);
        MOQ_CHECK(enc.ok());
        MOQ_CHECK(!enc->empty());

        auto dec = moq::loc::parse(moq::loc::profile::loc01,
            moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->has_video_config);
        MOQ_CHECK(dec->video_config.size() == sizeof(cfg_data));
        MOQ_CHECK(std::memcmp(dec->video_config.data(), cfg_data,
                               sizeof(cfg_data)) == 0);
    }

    // Full roundtrip: timestamp + vfm + audio + config
    {
        uint8_t cfg[] = {0xDE, 0xAD};

        moq::loc::headers h;
        h.has_timestamp = true;
        h.timestamp     = 5000;
        h.has_video_frame_marking                  = true;
        h.video_frame_marking.start_of_frame       = true;
        h.video_frame_marking.end_of_frame         = true;
        h.video_frame_marking.independent           = true;
        h.has_audio_level                           = true;
        h.audio_level.voice_activity                = true;
        h.audio_level.level                         = 30;
        h.has_video_config                          = true;
        h.video_config = moq::bytes_view(cfg, sizeof(cfg));

        auto enc = moq::loc::encode(moq::loc::profile::loc01, h);
        MOQ_CHECK(enc.ok());

        auto dec = moq::loc::parse(moq::loc::profile::loc01,
            moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->timestamp == 5000);
        MOQ_CHECK(dec->video_frame_marking.independent);
        MOQ_CHECK(dec->audio_level.voice_activity);
        MOQ_CHECK(dec->audio_level.level == 30);
        MOQ_CHECK(dec->video_config.size() == 2);
    }

    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
