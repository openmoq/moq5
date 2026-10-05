#include <moq/loc.hpp>
#include "test_support.hpp"

#include <cstring>
#include <cstdint>

int main()
{
    int failures = 0;

    // Parse empty LOC-01 — success, no fields
    {
        auto r = moq::loc::parse(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01, {});
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
        auto r = moq::loc::encode(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01, h);
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->empty());
    }

    // Timestamp LOC-01 encode byte oracle matches C test
    {
        moq::loc::headers h;
        h.has_timestamp = true;
        h.timestamp     = 1000000;

        auto r = moq::loc::encode(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01, h);
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

        auto r = moq::loc::encode(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01, h);
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
        auto r = moq::loc::parse(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01,
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
        auto r = moq::loc::parse(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc02, {});
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::invalid);
    }

    // LOC-02 encode returns errc::invalid
    {
        moq::loc::headers h;
        h.has_timestamp = true;
        h.timestamp     = 48000;
        auto r = moq::loc::encode(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc02, h);
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::invalid);
    }

    // Oversized audio level from raw bytes returns errc::protocol
    {
        // delta=6, value=0x100 (2-byte QUIC varint: 0x41 0x00)
        uint8_t wire[] = {0x06, 0x41, 0x00};
        auto r = moq::loc::parse(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01,
                                  moq::bytes_view(wire, sizeof(wire)));
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::protocol);
    }

    // Oversized video frame marking from raw bytes returns errc::protocol
    {
        // delta=4, value=0x10000 (4-byte QUIC varint: 0x80 0x01 0x00 0x00)
        uint8_t wire[] = {0x04, 0x80, 0x01, 0x00, 0x00};
        auto r = moq::loc::parse(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01,
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

        auto enc = moq::loc::encode(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01, h);
        MOQ_CHECK(enc.ok());
        MOQ_CHECK(!enc->empty());

        auto dec = moq::loc::parse(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01,
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

        auto enc = moq::loc::encode(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01, h);
        MOQ_CHECK(enc.ok());

        auto dec = moq::loc::parse(MOQ_VERSION_DRAFT_16, moq::loc::profile::loc01,
            moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        MOQ_CHECK(dec->timestamp == 5000);
        MOQ_CHECK(dec->video_frame_marking.independent);
        MOQ_CHECK(dec->audio_level.voice_activity);
        MOQ_CHECK(dec->audio_level.level == 30);
        MOQ_CHECK(dec->video_config.size() == 2);
    }

    // ---- Draft-18 differential coverage through the C++ WRAPPER --------
    //
    // Everything above proves the new argument compiles and that draft-16
    // still works. What follows proves the WRAPPER FORWARDS the version:
    // the expected bytes are declared here independently, from
    // draft-ietf-moq-transport-16 section 1.4 ((i), the RFC 9000 QUIC
    // varint) and draft-ietf-moq-transport-18 section 1.4.1 (vi64), and a
    // Capture Timestamp is LOC-01 property 0x02 -- the first entry, so its
    // Delta Type is one byte in both drafts and only the value differs.
    {
        // Shared band: 63 is the LAST value the two encodings spell
        // identically, so it is the control. A failure here is the fixture.
        static const unsigned char want_63[] = { 0x02, 0x3f };

        // Divergent: 33333.
        static const unsigned char want_33333_d16[] =
            { 0x02, 0x80, 0x00, 0x82, 0x35 };
        static const unsigned char want_33333_d18[] =
            { 0x02, 0xc0, 0x82, 0x35 };

        struct vec {
            moq_version_t        version;
            std::uint64_t        value;
            const unsigned char *want;
            std::size_t          want_len;
            const char          *name;
        };
        const vec vecs[] = {
            { MOQ_VERSION_DRAFT_16, 63,    want_63, sizeof(want_63),
              "draft-16 timestamp 63" },
            { MOQ_VERSION_DRAFT_18, 63,    want_63, sizeof(want_63),
              "draft-18 timestamp 63" },
            { MOQ_VERSION_DRAFT_16, 33333, want_33333_d16,
              sizeof(want_33333_d16), "draft-16 timestamp 33333" },
            { MOQ_VERSION_DRAFT_18, 33333, want_33333_d18,
              sizeof(want_33333_d18), "draft-18 timestamp 33333" },
        };

        for (const auto &v : vecs) {
            moq::loc::headers h;
            h.has_timestamp = true;
            h.timestamp     = v.value;

            auto enc = moq::loc::encode(v.version, moq::loc::profile::loc01, h);
            MOQ_CHECK(enc.ok());
            if (!enc.ok()) continue;

            const bool bytes_match =
                enc->size() == v.want_len &&
                std::memcmp(enc->data(), v.want, v.want_len) == 0;
            if (!bytes_match) {
                std::fprintf(stderr,
                    "FAIL: %s:%d: %s: the C++ wrapper produced",
                    __FILE__, __LINE__, v.name);
                for (std::size_t i = 0; i < enc->size(); i++)
                    std::fprintf(stderr, " %02x", enc->data()[i]);
                std::fprintf(stderr, ", the draft requires");
                for (std::size_t i = 0; i < v.want_len; i++)
                    std::fprintf(stderr, " %02x", v.want[i]);
                std::fprintf(stderr, "\n");
                failures++;
                continue;
            }

            // Matching-family parse recovers the value exactly.
            auto same = moq::loc::parse(v.version, moq::loc::profile::loc01,
                moq::bytes_view(enc->data(), enc->size()));
            MOQ_CHECK(same.ok());
            if (same.ok()) {
                MOQ_CHECK(same->has_timestamp);
                MOQ_CHECK(same->timestamp == v.value);
            }

            // Cross-family NON-EQUIVALENCE, for the divergent value only.
            // The guarantee is that the wrong codec does not reproduce the
            // value -- it may reject the block OR decode something else, and
            // either is a visible mismatch. It is deliberately NOT a claim
            // that the wrong codec always refuses: no such protocol property
            // exists. Below 64 the two encodings agree and either codec
            // legitimately reads it, which is what makes 63 the control
            // rather than a case.
            if (v.value >= 64) {
                const moq_version_t other =
                    (v.version == MOQ_VERSION_DRAFT_16) ? MOQ_VERSION_DRAFT_18
                                                        : MOQ_VERSION_DRAFT_16;
                auto cross = moq::loc::parse(other, moq::loc::profile::loc01,
                    moq::bytes_view(enc->data(), enc->size()));
                const bool reproduced =
                    cross.ok() && cross->has_timestamp &&
                    cross->timestamp == v.value;
                if (reproduced) {
                    std::fprintf(stderr,
                        "FAIL: %s:%d: %s was reproduced exactly by the other "
                        "draft's codec through the C++ wrapper\n",
                        __FILE__, __LINE__, v.name);
                    failures++;
                }
            }
        }
    }

    // Unsupported and unset transport versions fail closed through the
    // wrapper, on encode and on parse, empty property block included.
    {
        const moq_version_t bad[] = {
            static_cast<moq_version_t>(0),  static_cast<moq_version_t>(1),
            static_cast<moq_version_t>(15), static_cast<moq_version_t>(17),
            // The enum's C++ value range is 0..31 (largest enumerator 18).
            static_cast<moq_version_t>(19), static_cast<moq_version_t>(31),
        };
        static const unsigned char some_bytes[] = { 0x02, 0x21 };

        for (const auto v : bad) {
            moq::loc::headers h;
            h.has_timestamp = true;
            h.timestamp     = 33;
            auto enc = moq::loc::encode(v, moq::loc::profile::loc01, h);
            MOQ_CHECK(!enc.ok());

            auto empty = moq::loc::parse(v, moq::loc::profile::loc01, {});
            MOQ_CHECK(!empty.ok());

            auto some = moq::loc::parse(v, moq::loc::profile::loc01,
                moq::bytes_view(some_bytes, sizeof(some_bytes)));
            MOQ_CHECK(!some.ok());
        }
    }

    std::printf("%s: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
