#include <moq/moq.hpp>
#include <algorithm>
#include "test_support.hpp"

#include <vector>

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
            [&](const moq::action::open_bidi_stream &bs) {
                to.on_bidi_stream_bytes(bs.ref, bs.data, false, now);
            },
            [&](const moq::action::send_bidi_stream &bs) {
                to.on_bidi_stream_bytes(bs.ref, bs.data, bs.fin, now);
            },
            [](const auto &) {});
    }
}

struct session_pair {
    moq::session client;
    moq::session server;
};

static session_pair establish(int &failures)
{
    moq::session_config ccfg{};
    ccfg.perspective              = moq::perspective::client;
    ccfg.send_request_capacity    = true;
    ccfg.initial_request_capacity = 16;

    moq::session_config scfg{};
    scfg.perspective              = moq::perspective::server;
    scfg.send_request_capacity    = true;
    scfg.initial_request_capacity = 16;

    auto cr = moq::session::create(ccfg);
    auto sr = moq::session::create(scfg);
    MOQ_CHECK(cr.ok());
    MOQ_CHECK(sr.ok());
    auto c = std::move(*cr);
    auto s = std::move(*sr);

    c.start(0);
    pump(c, s, 0);
    s.poll_event();
    pump(s, c, 0);
    c.poll_event();

    return {std::move(c), std::move(s)};
}

int main()
{
    int failures = 0;

    // -- 1. Create/destroy publisher -----------------------------------
    {
        auto [c, s] = establish(failures);

        auto pr = moq::publisher::create(s);
        MOQ_CHECK(pr.ok());
        MOQ_CHECK(pr->raw() != nullptr);
    }

    // -- 2. Add track + accept_all subscribe ---------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();

        auto track = pub.add_track(
            {.ns = {"live"}, .track = "video"}, 0);
        MOQ_CHECK(track.ok());
        MOQ_CHECK(track->valid());

        auto sub = c.subscribe(
            {.ns = {"live"}, .track = "video"}, 0);
        MOQ_CHECK(sub.ok());

        pump(c, s, 0);
        pub.tick(0);
        MOQ_CHECK(pub.active_subscriptions(*track) == 1);

        pump(s, c, 0);
        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::subscribe_ok &) {},
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 3. Write object, client receives exact payload ----------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"live"}, .track = "video"}, 0).value();

        c.subscribe({.ns = {"live"}, .track = "video"}, 0);
        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        c.poll_event(); // drain subscribe_ok

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xCA\xFE"), 2).value();

        auto wr = pub.write_object(track, 0, 0, payload, 0);
        MOQ_CHECK(wr.ok());
        pub.end_group(track, 0);
        pump(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.payload_data.size() == 2);
                MOQ_CHECK(obj.payload_data[0] == 0xCA);
                MOQ_CHECK(obj.payload_data[1] == 0xFE);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 4. Refcount: const buffer& — no extra retain ------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        c.poll_event();

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xAB"), 1).value();
        uint32_t rc_before = moq_rcbuf_refcount(payload.raw());

        pub.write_object(track, 0, 0, payload, 0);

        bool found_payload = false;
        while (auto a = s.poll_action()) {
            a->visit(
                [&](const moq::action::send_data &sd) {
                    if (!sd.payload.empty()) {
                        found_payload = true;
                        MOQ_CHECK(sd.payload_rcbuf == payload.raw());
                        /* +1 for the session's queued send_data ref, +1 for the
                         * action_variant `sd` that visit() built and holds for
                         * its lifetime (the rcbuf-lifetime fix). Both released
                         * after the visit -- see the == rc_before check below. */
                        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) ==
                                  rc_before + 2);
                    }
                },
                [](const auto &) {});
        }
        MOQ_CHECK(found_payload);
        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) == rc_before);
    }

    // -- 5. Move semantics ---------------------------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        MOQ_CHECK(pub.raw() != nullptr);

        auto pub2 = std::move(pub);
        MOQ_CHECK(pub.raw() == nullptr);
        MOQ_CHECK(pub2.raw() != nullptr);

        auto track = pub2.add_track(
            {.ns = {"ns"}, .track = "t"}, 0);
        MOQ_CHECK(track.ok());
    }

    // -- 6. has_subscriber before/after subscribe ----------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        MOQ_CHECK(!pub.has_subscriber(track));
        MOQ_CHECK(pub.active_subscriptions(track) == 0);

        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump(c, s, 0);
        pub.tick(0);

        MOQ_CHECK(pub.has_subscriber(track));
        MOQ_CHECK(pub.active_subscriptions(track) == 1);
    }

    // -- 7. Streaming object -------------------------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        c.poll_event();

        MOQ_CHECK(pub.begin_object(track, 0, 0, 5, 0).ok());
        auto chunk = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("hello"), 5).value();
        MOQ_CHECK(pub.write_data(track, chunk, 0).ok());
        MOQ_CHECK(pub.end_object(track, 0).ok());
        pub.end_group(track, 0);
        pump(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.payload_data.size() == 5);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 8. pub_object_config: status datagram (no payload) -------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        c.poll_event();

        auto wr = pub.write_object(track,
            {.group_id = 0, .object_id = 0,
             .datagram = true,
             .has_status = true,
             .status = moq::object_status::end_of_group},
            0);
        MOQ_CHECK(wr.ok());
        pump(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.datagram);
                MOQ_CHECK(obj.status == moq::object_status::end_of_group);
                MOQ_CHECK(obj.payload_data.empty());
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 8b. Appended-field discriminators: the facade cfg builders must
    //        use SIZED init, or these appended fields silently vanish. ----
    {
        auto [c, s] = establish(failures);
        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();
        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        c.poll_event();

        // end_of_group must reach the wire (appended tail of the C cfg).
        std::vector<uint8_t> pdat{1, 2, 3};
        auto payload = moq::buffer::create(pdat.data(), pdat.size()).value();
        auto wr = pub.write_object(track,
            {.group_id = 0, .object_id = 0,
             .payload = &payload,
             .end_of_group = true},
            0);
        MOQ_CHECK(wr.ok());
        pump(s, c, 0);
        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.end_of_group);   // dropped by a frozen init
            },
            [&](const auto &) { MOQ_CHECK(false); });

    }


    // -- 8c. Streaming begin-properties discriminator: the appended
    //        properties field of the begin cfg must reach the wire. The
    //        properties bytes are VALID draft KVP ({key=1, len=1, 0xBB} --
    //        the same fixture the C session test uses); a fresh session
    //        pair with a streaming-enabled client surfaces them on the
    //        first chunk. Dropped by a frozen begin init. -----------------
    {
        moq::session_config ccfg{};
        ccfg.perspective              = moq::perspective::client;
        ccfg.send_request_capacity    = true;
        ccfg.initial_request_capacity = 16;
        ccfg.streaming_objects        = true;

        moq::session_config scfg{};
        scfg.perspective              = moq::perspective::server;
        scfg.send_request_capacity    = true;
        scfg.initial_request_capacity = 16;

        auto cr = moq::session::create(ccfg);
        auto sr = moq::session::create(scfg);
        MOQ_CHECK(cr.ok() && sr.ok());
        auto c = std::move(*cr);
        auto s = std::move(*sr);
        c.start(0);
        pump(c, s, 0); s.poll_event();
        pump(s, c, 0); c.poll_event();

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();
        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        c.poll_event();

        static const uint8_t kvp[] = { 0x01, 0x01, 0xBB };  /* valid KVP */
        auto props = moq::buffer::create(kvp, sizeof(kvp)).value();
        MOQ_CHECK(pub.begin_object(track, 0, 0, 2, props, 0).ok());
        std::vector<uint8_t> cdat{7, 8};
        auto chunk = moq::buffer::create(cdat.data(), cdat.size()).value();
        MOQ_CHECK(pub.write_data(track, chunk, 0).ok());
        MOQ_CHECK(pub.end_object(track, 0).ok());
        pump(s, c, 0);
        bool saw_props = false;
        while (auto e2 = c.poll_event()) {
            e2->visit(
                [&](const moq::event::object_chunk &obj) {
                    if (obj.properties_data.size() == sizeof(kvp) &&
                        std::equal(obj.properties_data.begin(),
                                   obj.properties_data.end(), kvp))
                        saw_props = true;
                },
                [&](const moq::event::object_received &obj) {
                    if (obj.properties_data.size() == sizeof(kvp) &&
                        std::equal(obj.properties_data.begin(),
                                   obj.properties_data.end(), kvp))
                        saw_props = true;
                },
                [&](const auto &) {});
        }
        MOQ_CHECK(saw_props);
    }

    // -- 9. pub_object_config: datagram with properties ----------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        c.poll_event();

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\x42"), 1).value();
        auto props = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xAA\xBB"), 2).value();

        auto wr = pub.write_object(track,
            {.group_id = 0, .object_id = 0,
             .payload = &payload, .properties = &props,
             .datagram = true},
            0);
        MOQ_CHECK(wr.ok());
        pump(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.datagram);
                MOQ_CHECK(obj.payload_data.size() == 1);
                MOQ_CHECK(obj.payload_data[0] == 0x42);
                MOQ_CHECK(obj.properties_data.size() == 2);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 10. Wrapped buffer write: zero-copy + release proof ------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        c.poll_event();

        static uint8_t ext_data[] = {0xDE, 0xAD, 0xC0, 0xDE};
        int released = 0;
        auto payload = moq::buffer::wrap(ext_data, 4,
            [](void *ctx, const uint8_t *, size_t) {
                ++*static_cast<int *>(ctx);
            }, &released).value();

        MOQ_CHECK(payload.data() == ext_data);

        MOQ_CHECK(pub.write_object(track, 0, 0, payload, 0).ok());

        bool found = false;
        while (auto a = s.poll_action()) {
            a->visit(
                [&](const moq::action::send_data &sd) {
                    if (!sd.payload.empty()) {
                        found = true;
                        MOQ_CHECK(sd.payload_rcbuf == payload.raw());
                        MOQ_CHECK(released == 0);
                    }
                },
                [](const auto &) {});
        }
        MOQ_CHECK(found);
        MOQ_CHECK(released == 0);

        payload = moq::buffer();
        MOQ_CHECK(released == 1);
    }

    // -- 11. Retained group: set/clear refcount -------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("catalog"), 7).value();
        uint32_t rc_before = moq_rcbuf_refcount(payload.raw());

        moq::pub_retained_object obj{.object_id = 0, .payload = &payload};
        MOQ_CHECK(pub.set_retained_group(track,
            {.group_id = 0, .objects = &obj, .object_count = 1}).ok());
        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) == rc_before + 1);

        MOQ_CHECK(pub.clear_retained_group(track).ok());
        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) == rc_before);
    }

    // -- 12. Retained group: max_retained_bytes rejection ---------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto track = pub.add_track(
            {.ns = {"ns"}, .track = "t", .max_retained_bytes = 4}, 0).value();

        auto big = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("toolarge"), 8).value();
        auto small = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("ok"), 2).value();

        moq::pub_retained_object big_obj{.object_id = 0, .payload = &big};
        MOQ_CHECK(!pub.set_retained_group(track,
            {.group_id = 0, .objects = &big_obj, .object_count = 1}).ok());
        MOQ_CHECK(moq_rcbuf_refcount(big.raw()) == 1);

        moq::pub_retained_object small_obj{.object_id = 0, .payload = &small};
        MOQ_CHECK(pub.set_retained_group(track,
            {.group_id = 0, .objects = &small_obj, .object_count = 1}).ok());
        MOQ_CHECK(moq_rcbuf_refcount(small.raw()) == 2);
    }

    // -- 13. Default publisher is fail-closed: rejects matching subscribe
    {
        auto [c, s] = establish(failures);

        // No accept_mode opt-in: the default publisher must reject remote
        // subscribers (fail-closed), not silently accept them.
        auto pub = moq::publisher::create(s).value();

        auto track = pub.add_track(
            {.ns = {"live"}, .track = "video"}, 0);
        MOQ_CHECK(track.ok());

        auto sub = c.subscribe(
            {.ns = {"live"}, .track = "video"}, 0);
        MOQ_CHECK(sub.ok());

        pump(c, s, 0);
        pub.tick(0);
        MOQ_CHECK(pub.active_subscriptions(*track) == 0);

        pump(s, c, 0);
        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::subscribe_error &err) {
                MOQ_CHECK(err.error_code ==
                          moq::request_error::unauthorized);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    MOQ_PASS("test_cpp_publisher");
    return failures;
}
