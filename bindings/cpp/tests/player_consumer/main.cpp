/* A cold C++ consumer of moq::media_player through public targets only. It
 * builds a player whose receiver points at a port nothing listens on and
 * runs the application loop shape: tick, wait, poll events and outputs,
 * report the decoder queue. Nothing arrives; the point is that the public
 * surface builds, links and runs warning-clean against the SDK. */
#include <moq/media_player.hpp>
#include <moq/endpoint.h>

#include <cstdio>
#include <cstring>
#include <variant>

#define CHECK(expr, code)                                                     \
    do {                                                                      \
        if (!(expr)) {                                                        \
            std::fprintf(stderr, "FAIL(%d): %s:%d: %s\n", code, __FILE__,     \
                         __LINE__, #expr);                                    \
            return code;                                                      \
        }                                                                     \
    } while (0)

int main()
{
    static const char url[] = "moqt://127.0.0.1:1";
    moq_endpoint_cfg_t ec;
    moq_endpoint_cfg_init(&ec);
    ec.url = moq_bytes_t{reinterpret_cast<const uint8_t *>(url), std::strlen(url)};
    moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("consumer")};
    moq_media_receiver_cfg_t rc;
    moq_media_receiver_cfg_init_live_sized(&rc, sizeof(rc));
    rc.namespace_ = moq_namespace_t{parts, 1};
    rc.endpoint = &ec;

    moq::media_player_config cfg;
    cfg.selection = moq::selection_mode::automatic;
    auto created = moq::media_player::create(rc, cfg);
    CHECK(created.ok(), 1);
    moq::media_player player = std::move(*created);

    for (uint64_t now = 1; now < 4; now++) {
        auto t = player.tick(now);
        CHECK(t.ok() || t.error().code() == moq::errc::would_block ||
                  t.error().code() == moq::errc::closed, 2);
        while (true) {
            auto ev = player.poll_event();
            if (!ev.ok() || !ev->has_value()) break;
        }
        while (true) {
            auto out = player.poll_output();
            if (!out.ok() || !out->has_value()) break;
            moq::polled_output rec = std::move(**out);
            if (std::holds_alternative<moq::output::unit>(rec.variant())) {
                auto fb = moq::player_feedback::decoder_queue(
                    rec.track(), rec.configuration_generation(), 1, 4);
                (void)player.handle_feedback(fb, now);
            }
        }
        auto w = player.wait(now, 1000);
        CHECK(w.ok() || w.error().code() == moq::errc::closed, 3);
    }
    auto st = player.stats();
    CHECK(st.ok() && st->units_output == 0, 4);
    std::printf("moq_player_consumer_test: OK\n");
    return 0;
}
