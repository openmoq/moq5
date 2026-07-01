#include <moq/moq.hpp>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <variant>
#include <vector>

#define CHECK(expr, code)        \
    do {                         \
        if (!(expr)) {           \
            std::fprintf(stderr, \
                "FAIL(%d): %s:%d: %s\n", code, __FILE__, __LINE__, #expr); \
            return code;         \
        }                        \
    } while (0)

// Pump helper — same pattern as in-tree tests but uses only public API
static void pump(moq::session &from, moq::session &to, uint64_t now)
{
    while (auto a = from.poll_action()) {
        a->visit(
            [&](const moq::action::send_control &sc) {
                to.on_control_bytes(sc.data, now);
            },
            [&](const moq::action::send_data &sd) {
                std::vector<uint8_t> buf;
                buf.insert(buf.end(), sd.header.begin(), sd.header.end());
                buf.insert(buf.end(), sd.payload.begin(), sd.payload.end());
                to.on_data_bytes(sd.ref, buf, sd.fin, now);
            },
            [&](const moq::action::send_datagram &dg) {
                to.on_datagram(dg.data, now);
            },
            [](const auto &) {});
    }
}

int main()
{
    // -- 1. Foundation types (existing checks) -------------------------

    moq::session_config cfg{};
    cfg.perspective = moq::perspective::client;

    auto buf = moq::buffer::create(
        reinterpret_cast<const uint8_t *>("test"), 4);
    CHECK(buf.ok(), 1);
    CHECK(buf->size() == 4, 2);

    moq::subscription sub;
    CHECK(!sub.valid(), 3);

    CHECK(moq::to_c(moq::perspective::server) == MOQ_PERSPECTIVE_SERVER, 4);

    moq::result<void> ok_result{};
    CHECK(ok_result.ok(), 5);

    moq::result<void> err_result{moq::errc::nomem};
    CHECK(!err_result.ok(), 6);

    using var_t = std::variant<int, double>;
    var_t v     = 42;
    int   visited = 0;
    moq::visit(v,
               [&](int i) { visited = i; },
               [&](double) { visited = -1; });
    CHECK(visited == 42, 7);

    moq::namespace_name ns({"live", "camera-1"});
    CHECK(ns.count() == 2, 8);

    moq::bytes_view bv("hello");
    CHECK(bv.size() == 5, 9);

    // -- 2. buffer::wrap — zero-copy external ownership ----------------

    static uint8_t ext_payload[] = {0xDE, 0xAD, 0xC0, 0xDE};
    int release_count = 0;
    {
        auto wrapped = moq::buffer::wrap(ext_payload, 4,
            [](void *ctx, const uint8_t *, size_t) {
                ++*static_cast<int *>(ctx);
            }, &release_count);
        CHECK(wrapped.ok(), 10);
        CHECK(wrapped->data() == ext_payload, 11);
        CHECK(wrapped->size() == 4, 12);
        CHECK(release_count == 0, 13);
    }
    CHECK(release_count == 1, 14);

    // -- 3. Full pub/sub round-trip with subgroup properties -----------

    moq::session_config ccfg{};
    ccfg.perspective              = moq::perspective::client;
    ccfg.send_request_capacity    = true;
    ccfg.initial_request_capacity = 10;

    moq::session_config scfg{};
    scfg.perspective              = moq::perspective::server;
    scfg.send_request_capacity    = true;
    scfg.initial_request_capacity = 10;

    auto cr = moq::session::create(ccfg);
    auto sr = moq::session::create(scfg);
    CHECK(cr.ok(), 20);
    CHECK(sr.ok(), 21);
    auto &client = *cr;
    auto &server = *sr;

    client.start(0);
    pump(client, server, 0);
    server.poll_event();
    pump(server, client, 0);
    client.poll_event();
    CHECK(server.state() == moq::session_state::established, 22);
    CHECK(client.state() == moq::session_state::established, 23);

    // Client subscribes
    auto sub_r = client.subscribe(
        {.ns = {"media"}, .track = "video"}, 0);
    CHECK(sub_r.ok(), 24);
    pump(client, server, 0);

    moq::subscription srv_sub;
    {
        auto ev = server.poll_event();
        CHECK(ev.has_value(), 25);
        ev->visit(
            [&](const moq::event::subscribe_request &req) {
                srv_sub = req.sub;
            },
            [](const auto &) {});
    }
    server.accept_subscribe(srv_sub, {}, 0);
    pump(server, client, 0);
    client.poll_event();

    // Server opens subgroup with extensions
    auto sg = server.open_subgroup(srv_sub,
        {.object_properties = true}, 0);
    CHECK(sg.ok(), 26);

    // Wrap external payload — no copy
    static uint8_t media_frame[] = {0xCA, 0xFE, 0xBA, 0xBE};
    int frame_released = 0;
    auto payload = moq::buffer::wrap(media_frame, 4,
        [](void *ctx, const uint8_t *, size_t) {
            ++*static_cast<int *>(ctx);
        }, &frame_released).value();

    CHECK(payload.data() == media_frame, 27);

    // Valid KVP properties: type=1 (odd=length-prefixed), len=2, value
    uint8_t prop_bytes[] = {0x01, 0x02, 0x42, 0x43};
    auto props = moq::buffer::create(prop_bytes, 4).value();

    // Write object with properties
    auto wr = server.write_object_ex(*sg, 0, payload, &props, 0);
    CHECK(wr.ok(), 28);
    server.close_subgroup(*sg, 0);

    // Pump data to client
    pump(server, client, 0);
    CHECK(frame_released == 0, 29);

    // Client receives object with exact payload + properties
    bool got_object = false;
    size_t rx_payload_len = 0, rx_props_len = 0;
    uint8_t rx_payload_first = 0, rx_payload_last = 0;
    uint8_t rx_props_first = 0, rx_props_last = 0;
    {
        auto ev = client.poll_event();
        CHECK(ev.has_value(), 30);
        ev->visit(
            [&](const moq::event::object_received &obj) {
                got_object = true;
                rx_payload_len = obj.payload_data.size();
                if (rx_payload_len > 0) {
                    rx_payload_first = obj.payload_data[0];
                    rx_payload_last  = obj.payload_data[rx_payload_len - 1];
                }
                rx_props_len = obj.properties_data.size();
                if (rx_props_len > 0) {
                    rx_props_first = obj.properties_data[0];
                    rx_props_last  = obj.properties_data[rx_props_len - 1];
                }
            },
            [](const auto &) {});
    }
    CHECK(got_object, 31);
    CHECK(rx_payload_len == 4, 32);
    CHECK(rx_payload_first == 0xCA, 33);
    CHECK(rx_payload_last == 0xBE, 34);
    CHECK(rx_props_len == 4, 35);
    CHECK(rx_props_first == 0x01, 36);
    CHECK(rx_props_last == 0x43, 37);

    // Release wrapped buffers — release callback should fire
    payload = moq::buffer();
    CHECK(frame_released == 1, 38);

#ifdef MOQ_HAS_LOC
    // -- 4. LOC parse/encode smoke ------------------------------------
    {
        moq::loc::headers lh;
        lh.has_timestamp = true;
        lh.timestamp     = 42;
        auto enc = moq::loc::encode(moq::loc::profile::loc01, lh);
        CHECK(enc.ok(), 40);
        CHECK(!enc->empty(), 41);

        auto dec = moq::loc::parse(moq::loc::profile::loc01,
            moq::bytes_view(enc->data(), enc->size()));
        CHECK(dec.ok(), 42);
        CHECK(dec->timestamp == 42, 43);
    }
#endif

#ifdef MOQ_HAS_MSF
    // -- 5. MSF catalog parse/encode smoke ----------------------------
    {
        moq::msf::catalog cat;
        auto enc = moq::msf::encode(cat);
        CHECK(enc.ok(), 50);
        CHECK(!enc->empty(), 51);

        auto dec = moq::msf::parse(
            moq::bytes_view(enc->data(), enc->size()));
        CHECK(dec.ok(), 52);
        CHECK(dec->track_count() == 0, 53);
    }

    // -- 5b. MSF base64 smoke ----------------------------------------
    {
        auto enc = moq::msf::encode_init_data(moq::bytes_view("hi"));
        CHECK(enc.ok(), 54);
        auto dec = moq::msf::decode_init_data(
            moq::bytes_view(enc->data(), enc->size()));
        CHECK(dec.ok(), 55);
        CHECK(dec->size() == 2, 56);
    }
#endif

#ifdef MOQ_HAS_CMAF
    // -- 6. CMAF parse smoke -----------------------------------------
    {
        // Malformed init should return error, not crash
        uint8_t bad[] = {0x00, 0x00, 0x00, 0x04, 'm', 'o', 'o', 'v'};
        auto r = moq::cmaf::parse_init(moq::bytes_view(bad, sizeof(bad)));
        CHECK(!r.ok(), 60);
    }
#endif

    std::printf("PASS: moq_consumer_test\n");
    return 0;
}
