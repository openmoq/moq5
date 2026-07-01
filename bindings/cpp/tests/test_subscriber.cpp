#include <moq/moq.hpp>
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

    // -- 1. Create/destroy subscriber ---------------------------------
    {
        auto [c, s] = establish(failures);

        auto sr = moq::subscriber::create(c);
        MOQ_CHECK(sr.ok());
        MOQ_CHECK(sr->raw() != nullptr);
    }

    // -- 2. Subscribe + tick → track active ----------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        pub.add_track({.ns = {"test"}, .track = "video"}, 0);

        auto sub = moq::subscriber::create(c).value();
        auto track = sub.subscribe(
            {.ns = {"test"}, .track = "video"}, 0);
        MOQ_CHECK(track.ok());
        MOQ_CHECK(track->valid());

        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        sub.tick(0);

        MOQ_CHECK(track->is_active());
    }

    // -- 3. Receive object via poll_object -----------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto ptrack = pub.add_track(
            {.ns = {"test"}, .track = "video"}, 0).value();

        auto sub = moq::subscriber::create(c).value();
        auto track = sub.subscribe(
            {.ns = {"test"}, .track = "video"}, 0).value();

        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        sub.tick(0);
        MOQ_CHECK(track.is_active());

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xDE\xAD"), 2).value();
        pub.write_object(ptrack, 0, 0, payload, 0);
        pub.end_group(ptrack, 0);

        pump(s, c, 0);
        sub.tick(0);

        auto obj = sub.poll_object();
        MOQ_CHECK(obj.has_value());
        MOQ_CHECK(obj->group_id() == 0);
        MOQ_CHECK(obj->object_id() == 0);
        MOQ_CHECK(obj->payload().size() == 2);
        MOQ_CHECK(obj->payload()[0] == 0xDE);
        MOQ_CHECK(obj->payload()[1] == 0xAD);
        MOQ_CHECK(obj->status() == moq::object_status::normal);

        auto obj2 = sub.poll_object();
        MOQ_CHECK(!obj2.has_value());
    }

    // -- 4. payload_owned survives polled_object destruction -----------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto ptrack = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        auto sub = moq::subscriber::create(c).value();
        [[maybe_unused]] auto track = sub.subscribe(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        sub.tick(0);

        auto pl = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\x01\x02"), 2).value();
        pub.write_object(ptrack, 0, 0, pl, 0);
        pub.end_group(ptrack, 0);
        pump(s, c, 0);
        sub.tick(0);

        moq::buffer owned;
        {
            auto obj = sub.poll_object();
            MOQ_CHECK(obj.has_value());
            owned = obj->payload_owned();
        }
        MOQ_CHECK(owned.size() == 2);
        MOQ_CHECK(owned.data()[0] == 0x01);
        MOQ_CHECK(owned.data()[1] == 0x02);
    }

    // -- 5. Move semantics --------------------------------------------
    {
        auto [c, s] = establish(failures);

        auto sub = moq::subscriber::create(c).value();
        MOQ_CHECK(sub.raw() != nullptr);

        auto sub2 = std::move(sub);
        MOQ_CHECK(sub.raw() == nullptr);
        MOQ_CHECK(sub2.raw() != nullptr);
    }

    // -- 6. Unsubscribe -----------------------------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto ptrack = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        auto sub = moq::subscriber::create(c).value();
        auto track = sub.subscribe(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        sub.tick(0);
        MOQ_CHECK(track.is_active());
        MOQ_CHECK(pub.has_subscriber(ptrack));

        auto ur = sub.unsubscribe(track, 0);
        MOQ_CHECK(ur.ok());

        pump(c, s, 0);
        pub.tick(0);
        MOQ_CHECK(!pub.has_subscriber(ptrack));
    }

    // -- 7. Multiple objects ------------------------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        auto ptrack = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        auto sub = moq::subscriber::create(c).value();
        sub.subscribe({.ns = {"ns"}, .track = "t"}, 0);

        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        sub.tick(0);

        auto p1 = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("A"), 1).value();
        auto p2 = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("B"), 1).value();
        pub.write_object(ptrack, 0, 0, p1, 0);
        pub.write_object(ptrack, 0, 1, p2, 0);
        pub.end_group(ptrack, 0);
        pump(s, c, 0);
        sub.tick(0);

        auto obj1 = sub.poll_object();
        MOQ_CHECK(obj1.has_value());
        MOQ_CHECK(obj1->object_id() == 0);
        MOQ_CHECK(obj1->payload()[0] == 'A');

        auto obj2 = sub.poll_object();
        MOQ_CHECK(obj2.has_value());
        MOQ_CHECK(obj2->object_id() == 1);
        MOQ_CHECK(obj2->payload()[0] == 'B');

        MOQ_CHECK(!sub.poll_object().has_value());
    }

    // -- 8. Streaming poll_chunk ---------------------------------------
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
        auto ptrack = pub.add_track(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        auto sub = moq::subscriber::create(c,
            {.streaming_objects = true}).value();
        sub.subscribe({.ns = {"ns"}, .track = "t"}, 0);

        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        sub.tick(0);

        pub.begin_object(ptrack, 0, 0, 3, 0);
        auto chunk = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("xyz"), 3).value();
        pub.write_data(ptrack, chunk, 0);
        pub.end_object(ptrack, 0);
        pub.end_group(ptrack, 0);
        pump(s, c, 0);
        sub.tick(0);

        bool got_begin = false;
        size_t total_bytes = 0;
        while (auto ch = sub.poll_chunk()) {
            if (ch->begin()) {
                got_begin = true;
                MOQ_CHECK(ch->object_id() == 0);
            }
            total_bytes += ch->chunk_data().size();
        }
        MOQ_CHECK(got_begin);
        MOQ_CHECK(total_bytes == 3);
    }

    // -- 9. Fetch round-trip (raw session server) -----------------------
    {
        auto [c, s] = establish(failures);

        // Client subscribes via raw session API so server can accept
        [[maybe_unused]] auto sub_h = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump(c, s, 0);
        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) { srv_sub = r.sub; },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump(s, c, 0);
        c.poll_event(); // drain subscribe_ok

        // Client fetches via subscriber facade
        auto sub = moq::subscriber::create(c).value();
        auto fr = sub.fetch(
            {.ns = {"ns"}, .track = "t",
             .start_group = 0, .end_group = 1}, 0);
        MOQ_CHECK(fr.ok());
        pump(c, s, 0);

        // Server accepts fetch + writes object via raw session API
        moq::fetch_handle srv_fetch;
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::fetch_request &r) {
                    srv_fetch = r.fetch;
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }
        s.accept_fetch(srv_fetch, {.end_group = 0, .end_object = 1}, 0);
        auto fp = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("FD"), 2).value();
        s.write_fetch_object(srv_fetch,
            {.group_id = 0, .subgroup_id = 0, .object_id = 0}, fp, 0);
        s.end_fetch(srv_fetch, 0);
        pump(s, c, 0);
        sub.tick(0);

        bool got_ok = false, got_obj = false, got_complete = false;
        while (auto item = sub.poll_fetch()) {
            switch (item->kind()) {
            case moq::sub_fetch_item_kind::ok:
                got_ok = true;
                break;
            case moq::sub_fetch_item_kind::object:
                got_obj = true;
                MOQ_CHECK(item->obj_payload().size() == 2);
                MOQ_CHECK(item->obj_payload()[0] == 'F');
                MOQ_CHECK(item->obj_payload()[1] == 'D');
                break;
            case moq::sub_fetch_item_kind::complete:
                got_complete = true;
                break;
            default:
                break;
            }
        }
        MOQ_CHECK(got_ok);
        MOQ_CHECK(got_obj);
        MOQ_CHECK(got_complete);
    }

    // -- 10. Track status round-trip (raw session server) ---------------
    {
        auto [c, s] = establish(failures);

        auto sub = moq::subscriber::create(c).value();
        auto sr = sub.track_status({.ns = {"ns"}, .track = "t"}, 0);
        MOQ_CHECK(sr.ok());

        pump(c, s, 0);

        // Server accepts track status via raw session API
        moq::track_status_handle srv_ts;
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::track_status_request &r) {
                    srv_ts = r.handle;
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }
        s.accept_track_status(srv_ts, 0);
        pump(s, c, 0);
        sub.tick(0);

        auto result = sub.poll_status();
        MOQ_CHECK(result.has_value());
        MOQ_CHECK(result->kind == moq::sub_status_result_kind::ok);
    }

    // -- 11. update_subscription round-trip -----------------------------
    {
        auto [c, s] = establish(failures);

        auto pub = moq::publisher::create(
            s, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
        pub.add_track({.ns = {"ns"}, .track = "t"}, 0);

        auto sub = moq::subscriber::create(c).value();
        auto track = sub.subscribe(
            {.ns = {"ns"}, .track = "t"}, 0).value();

        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        sub.tick(0);
        MOQ_CHECK(track.is_active());

        auto ur = sub.update_subscription(track,
            {.has_subscriber_priority = true, .subscriber_priority = 5},
            0);
        MOQ_CHECK(ur.ok());

        pump(c, s, 0);
        pub.tick(0);
        pump(s, c, 0);
        sub.tick(0);
    }

    MOQ_PASS("test_cpp_subscriber");
    return failures;
}
