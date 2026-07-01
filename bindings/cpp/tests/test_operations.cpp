#include <moq/moq.hpp>
#include "test_support.hpp"

#include <vector>

// -- Pump helpers (duplicated from test_session.cpp) --------------------

static void pump_control(moq::session &from, moq::session &to, uint64_t now)
{
    while (auto a = from.poll_action()) {
        a->visit(
            [&](const moq::action::send_control &sc) {
                to.on_control_bytes(sc.data, now);
            },
            [&](const moq::action::send_datagram &dg) {
                to.on_datagram(dg.data, now);
            },
            [](const auto &) {});
    }
}

static void pump_all(moq::session &from, moq::session &to, uint64_t now)
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

struct session_pair {
    moq::session client;
    moq::session server;
};

static session_pair establish(int &failures)
{
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
    MOQ_CHECK(cr.ok());
    MOQ_CHECK(sr.ok());
    auto c = std::move(*cr);
    auto s = std::move(*sr);

    c.start(0);
    pump_control(c, s, 0);
    s.poll_event();
    pump_control(s, c, 0);
    c.poll_event();

    return {std::move(c), std::move(s)};
}

int main()
{
    int failures = 0;

    // -- 1. C++ subscribe produces SUBSCRIBE_REQUEST on server ----------
    {
        auto [c, s] = establish(failures);

        auto r = c.subscribe(
            {.ns = {"live", "camera-1"},
             .track = "video",
             .filter = moq::subscribe_filter::largest_object},
            0);
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->valid());
        pump_control(c, s, 0);

        auto ev = s.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::subscribe_request &req) {
                auto ns = req.track_namespace();
                MOQ_CHECK(ns.count() == 2);
                MOQ_CHECK(ns[0] == "live");
                MOQ_CHECK(ns[1] == "camera-1");
                MOQ_CHECK(req.track_name.string_view() == "video");
                MOQ_CHECK(req.filter ==
                          moq::subscribe_filter::largest_object);
                MOQ_CHECK(req.forward == true);
                MOQ_CHECK(req.subscriber_priority == 128);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 2. C++ accept_subscribe produces SUBSCRIBE_OK ------------------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe(
            {.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &req) {
                    srv_sub = req.sub;
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }

        auto ar = s.accept_subscribe(srv_sub,
            {.has_track_alias = true, .track_alias = 42,
             .has_largest = true, .largest_group = 5, .largest_object = 99},
            0);
        MOQ_CHECK(ar.ok());
        pump_control(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::subscribe_ok &ok) {
                MOQ_CHECK(ok.track_alias == 42);
                MOQ_CHECK(ok.has_largest);
                MOQ_CHECK(ok.largest_group == 5);
                MOQ_CHECK(ok.largest_object == 99);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 3. C++ reject_subscribe produces SUBSCRIBE_ERROR ---------------
    {
        auto [c, s] = establish(failures);

        c.subscribe({.ns = {"ns"}, .track = "t"}, 0);
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &req) {
                    srv_sub = req.sub;
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }

        auto rr = s.reject_subscribe(srv_sub,
            {.error_code = moq::request_error::does_not_exist,
             .reason = "not found",
             .can_retry = true,
             .retry_after_ms = 500},
            0);
        MOQ_CHECK(rr.ok());
        pump_control(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::subscribe_error &err) {
                MOQ_CHECK(err.error_code ==
                          moq::request_error::does_not_exist);
                MOQ_CHECK(err.reason == "not found");
                MOQ_CHECK(err.can_retry);
                MOQ_CHECK(err.retry_after_ms == 500);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 4. open_subgroup + write_object: SEND_DATA, no payload copy ---
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &req) {
                    srv_sub = req.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event(); // drain subscribe_ok

        auto sg = s.open_subgroup(srv_sub,
            {.group_id = 1, .subgroup_id = 0, .publisher_priority = 64},
            0);
        MOQ_CHECK(sg.ok());
        MOQ_CHECK(sg->valid());

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xDE\xAD\xBE\xEF"), 4).value();
        uint32_t rc_before = moq_rcbuf_refcount(payload.raw());

        auto wr = s.write_object(*sg, 0, payload, 0);
        MOQ_CHECK(wr.ok());

        bool found_payload = false;
        while (auto a = s.poll_action()) {
            a->visit(
                [&](const moq::action::send_data &sd) {
                    if (!sd.payload.empty()) {
                        found_payload = true;
                        MOQ_CHECK(sd.payload.size() == 4);
                        MOQ_CHECK(sd.payload[0] == 0xDE);
                        MOQ_CHECK(sd.payload_rcbuf == payload.raw());
                        /* +1 for the session's queued send_data ref, +1 for the
                         * action_variant `sd` that visit() built and holds for
                         * its lifetime (the rcbuf-lifetime fix). Both are
                         * released after the visit -- see the == rc_before check
                         * below. */
                        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) ==
                                  rc_before + 2);
                    }
                },
                [](const auto &) {});
        }
        MOQ_CHECK(found_payload);
        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) == rc_before);
    }

    // -- 5. Pump SEND_DATA to client → OBJECT_RECEIVED exact payload ---
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto sg = s.open_subgroup(srv_sub, {}, 0).value();
        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xCA\xFE"), 2).value();
        s.write_object(sg, 0, payload, 0);
        s.close_subgroup(sg, 0);
        pump_all(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.payload_data.size() == 2);
                MOQ_CHECK(obj.payload_data[0] == 0xCA);
                MOQ_CHECK(obj.payload_data[1] == 0xFE);
                MOQ_CHECK(obj.status == moq::object_status::normal);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 6. payload_owned survives polled_event destruction -------------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto sg = s.open_subgroup(srv_sub, {}, 0).value();
        auto pl = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\x01\x02\x03"), 3).value();
        s.write_object(sg, 0, pl, 0);
        s.close_subgroup(sg, 0);
        pump_all(s, c, 0);

        moq::buffer owned;
        {
            auto ev = c.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::object_received &obj) {
                    owned = obj.payload_owned();
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }
        MOQ_CHECK(owned.size() == 3);
        MOQ_CHECK(owned.data()[0] == 0x01);
        MOQ_CHECK(owned.data()[2] == 0x03);
    }

    // -- 7. close_subgroup queues FIN send_data action ------------------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto sg = s.open_subgroup(srv_sub, {}, 0).value();
        s.poll_action(); // drain open action

        auto cr = s.close_subgroup(sg, 0);
        MOQ_CHECK(cr.ok());

        bool found_fin = false;
        while (auto a = s.poll_action()) {
            a->visit(
                [&](const moq::action::send_data &sd) {
                    if (sd.fin) found_fin = true;
                },
                [](const auto &) {});
        }
        MOQ_CHECK(found_fin);
    }

    // -- 8. reset_subgroup queues RESET_DATA action --------------------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto sg = s.open_subgroup(srv_sub, {}, 0).value();
        s.poll_action(); // drain open

        auto rr = s.reset_subgroup(sg, 0x42, 0);
        MOQ_CHECK(rr.ok());

        bool found_reset = false;
        while (auto a = s.poll_action()) {
            a->visit(
                [&](const moq::action::reset_data &rd) {
                    found_reset = true;
                    MOQ_CHECK(rd.error_code == 0x42);
                },
                [](const auto &) {});
        }
        MOQ_CHECK(found_reset);
    }

    // -- 9. send_object_datagram round-trip -----------------------------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xAB\xCD"), 2).value();
        auto dr = s.send_object_datagram(srv_sub,
            {.group_id = 7, .object_id = 3, .publisher_priority = 64},
            payload, 0);
        MOQ_CHECK(dr.ok());

        pump_all(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.datagram);
                MOQ_CHECK(obj.group_id == 7);
                MOQ_CHECK(obj.object_id == 3);
                MOQ_CHECK(obj.payload_data.size() == 2);
                MOQ_CHECK(obj.payload_data[0] == 0xAB);
                MOQ_CHECK(obj.payload_data[1] == 0xCD);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 10. Namespace overflow returns errc::invalid -------------------
    {
        auto [c, s] = establish(failures);

        std::string_view parts[33];
        for (int i = 0; i < 33; ++i) parts[i] = "x";

        auto overflow_ns = moq::namespace_name(
            std::span<const std::string_view>(parts, 33));
        MOQ_CHECK(overflow_ns.overflowed());

        auto r = c.subscribe(
            {.ns = overflow_ns, .track = "t"}, 0);
        MOQ_CHECK(!r.ok());
        MOQ_CHECK(r.error().code() == moq::errc::invalid);
    }

    // -- 11. WOULD_BLOCK with max_actions=1 ----------------------------
    {
        moq::session_config ccfg{};
        ccfg.perspective              = moq::perspective::client;
        ccfg.send_request_capacity    = true;
        ccfg.initial_request_capacity = 10;

        moq::session_config scfg{};
        scfg.perspective              = moq::perspective::server;
        scfg.send_request_capacity    = true;
        scfg.initial_request_capacity = 10;
        scfg.max_actions              = 1;

        auto cr = moq::session::create(ccfg);
        auto sr = moq::session::create(scfg);
        MOQ_CHECK(cr.ok());
        MOQ_CHECK(sr.ok());
        auto &c = *cr;
        auto &s = *sr;

        c.start(0);
        pump_control(c, s, 0);
        s.poll_event();
        pump_control(s, c, 0);
        c.poll_event();

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto sg = s.open_subgroup(srv_sub, {}, 0).value();
        // action queue has the SEND_DATA for subgroup header — queue is full (cap=1)
        // Next write should get WOULD_BLOCK
        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("x"), 1).value();
        auto wr = s.write_object(sg, 0, payload, 0);
        MOQ_CHECK(!wr.ok());
        MOQ_CHECK(wr.error().code() == moq::errc::would_block);

        // Drain and retry
        s.poll_action();
        wr = s.write_object(sg, 0, payload, 0);
        MOQ_CHECK(wr.ok());
    }

    // -- 12. write_object: const buffer& — no extra incref -------------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto sg = s.open_subgroup(srv_sub, {}, 0).value();
        s.poll_action(); // drain open

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xFF"), 1).value();
        uint32_t rc_before = moq_rcbuf_refcount(payload.raw());
        s.write_object(sg, 0, payload, 0);
        // After queueing, C takes its own ref, but our const& didn't add one
        // Drain action to release C's ref
        while (auto a = s.poll_action()) {
            (void)a;
        }
        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) == rc_before);
    }

    // -- 13. datagram: const buffer& — no extra incref -----------------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xAA"), 1).value();
        uint32_t rc_before = moq_rcbuf_refcount(payload.raw());
        s.send_object_datagram(srv_sub, {}, payload, 0);
        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) == rc_before);
        s.poll_action(); // drain
        MOQ_CHECK(moq_rcbuf_refcount(payload.raw()) == rc_before);
    }

    // -- 14. send_status_datagram round-trip ----------------------------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto dr = s.send_status_datagram(srv_sub,
            {.group_id = 2,
             .object_id = 0,
             .status = moq::object_status::end_of_group},
            0);
        MOQ_CHECK(dr.ok());
        pump_all(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.datagram);
                MOQ_CHECK(obj.group_id == 2);
                MOQ_CHECK(obj.object_id == 0);
                MOQ_CHECK(obj.status == moq::object_status::end_of_group);
                MOQ_CHECK(obj.payload_data.empty());
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 15. request_error enum static asserts --------------------------
    {
        static_assert(moq::to_c(moq::request_error::internal_error) ==
                      MOQ_REQUEST_ERROR_INTERNAL_ERROR);
        static_assert(moq::to_c(moq::request_error::unauthorized) ==
                      MOQ_REQUEST_ERROR_UNAUTHORIZED);
        static_assert(moq::to_c(moq::request_error::does_not_exist) ==
                      MOQ_REQUEST_ERROR_DOES_NOT_EXIST);
        static_assert(moq::to_c(moq::request_error::duplicate_subscription) ==
                      MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION);
        static_assert(moq::request_error_from_c(MOQ_REQUEST_ERROR_TIMEOUT) ==
                      moq::request_error::timeout);
        static_assert(
            moq::request_error_from_c(MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID) ==
            moq::request_error::invalid_joining_request_id);
    }

    // -- 16. Subgroup stream object with properties round-trip ----------
    {
        auto [c, s] = establish(failures);

        [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
        pump_control(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &r) {
                    srv_sub = r.sub;
                },
                [](const auto &) {});
        }
        s.accept_subscribe(srv_sub, {}, 0);
        pump_control(s, c, 0);
        c.poll_event();

        auto sg = s.open_subgroup(srv_sub,
            {.object_properties = true}, 0).value();

        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("\xCA\xFE"), 2).value();
        /* Valid KVP: key=1, value_len=1, value=0xAA */
        uint8_t prop_bytes[] = {0x01, 0x01, 0xAA};
        auto props = moq::buffer::create(prop_bytes, 3).value();

        MOQ_CHECK(s.write_object_ex(sg, 0, payload, &props, 0).ok());
        s.close_subgroup(sg, 0);
        pump_all(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.payload_data.size() == 2);
                MOQ_CHECK(obj.payload_data[0] == 0xCA);
                MOQ_CHECK(obj.properties_data.size() == 3);
                MOQ_CHECK(obj.properties_data[2] == 0xAA);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    MOQ_PASS("test_cpp_operations");
    return failures;
}
