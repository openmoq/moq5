#include <moq/moq.hpp>
#include "test_support.hpp"

#include <cstring>
#include <optional>
#include <vector>

// Release callback for a wrapped rcbuf: flips *ctx to true on final decref.
// Lets a test deterministically observe whether a buffer was freed -- no ASan.
static void rcbuf_freed_cb(void *ctx, const uint8_t *, size_t)
{
    *static_cast<bool *>(ctx) = true;
}

// -- Pump helpers ------------------------------------------------------

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

static session_pair establish_pair(int &failures)
{
    moq::session_config ccfg{};
    ccfg.perspective            = moq::perspective::client;
    ccfg.send_request_capacity  = true;
    ccfg.initial_request_capacity = 10;

    moq::session_config scfg{};
    scfg.perspective            = moq::perspective::server;
    scfg.send_request_capacity  = true;
    scfg.initial_request_capacity = 10;

    auto cr = moq::session::create(ccfg);
    auto sr = moq::session::create(scfg);
    MOQ_CHECK(cr.ok());
    MOQ_CHECK(sr.ok());
    auto c = std::move(*cr);
    auto s = std::move(*sr);

    c.start(0);
    pump_control(c, s, 0);
    s.poll_event(); // drain setup_complete
    pump_control(s, c, 0);
    c.poll_event(); // drain setup_complete

    return {std::move(c), std::move(s)};
}

int main()
{
    int failures = 0;

    // -- 1. Create/destroy client session ------------------------------
    {
        auto r = moq::session::create({.perspective = moq::perspective::client});
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->state() == moq::session_state::idle);
    }

    // -- 2. Create/destroy server session ------------------------------
    {
        auto r = moq::session::create({.perspective = moq::perspective::server});
        MOQ_CHECK(r.ok());
        MOQ_CHECK(r->state() == moq::session_state::idle);
    }

    // -- 3. Start client + poll send_control ---------------------------
    {
        auto r = moq::session::create({.perspective = moq::perspective::client});
        MOQ_CHECK(r.ok());
        auto &s = *r;
        MOQ_CHECK(s.start(0).ok());
        MOQ_CHECK(s.state() == moq::session_state::setup_sent);

        auto a = s.poll_action();
        MOQ_CHECK(a.has_value());
        MOQ_CHECK(a->kind() == MOQ_ACTION_SEND_CONTROL);

        bool found_control = false;
        a->visit(
            [&](const moq::action::send_control &sc) {
                found_control = true;
                MOQ_CHECK(!sc.data.empty());
            },
            [](const auto &) {});
        MOQ_CHECK(found_control);

        auto a2 = s.poll_action();
        MOQ_CHECK(!a2.has_value());
    }

    // -- 4. Full setup handshake ---------------------------------------
    {
        moq::session_config ccfg{};
        ccfg.perspective = moq::perspective::client;
        moq::session_config scfg{};
        scfg.perspective = moq::perspective::server;

        auto cr = moq::session::create(ccfg);
        auto sr = moq::session::create(scfg);
        MOQ_CHECK(cr.ok());
        MOQ_CHECK(sr.ok());
        auto &c = *cr;
        auto &s = *sr;

        c.start(0);
        pump_control(c, s, 0);
        MOQ_CHECK(s.state() == moq::session_state::established);

        // Server setup_complete event
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            bool setup_ok = false;
            ev->visit(
                [&](const moq::event::setup_complete &sc) {
                    setup_ok = true;
                    MOQ_CHECK(sc.local_perspective == moq::perspective::server);
                    MOQ_CHECK(sc.peer_perspective == moq::perspective::client);
                },
                [](const auto &) {});
            MOQ_CHECK(setup_ok);
        }

        pump_control(s, c, 0);
        MOQ_CHECK(c.state() == moq::session_state::established);

        // Client setup_complete event
        {
            auto ev = c.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::setup_complete &sc) {
                    MOQ_CHECK(sc.local_perspective == moq::perspective::client);
                    MOQ_CHECK(sc.peer_perspective == moq::perspective::server);
                },
                [](const auto &) {});
        }
    }

    // -- 5. Poll empty returns nullopt ---------------------------------
    {
        auto r = moq::session::create({.perspective = moq::perspective::client});
        MOQ_CHECK(r.ok());
        MOQ_CHECK(!r->poll_action().has_value());
        MOQ_CHECK(!r->poll_event().has_value());
    }

    // -- 6. Server start → wrong_state error --------------------------
    {
        auto r = moq::session::create({.perspective = moq::perspective::server});
        MOQ_CHECK(r.ok());
        auto rc = r->start(0);
        MOQ_CHECK(!rc.ok());
        MOQ_CHECK(rc.error().code() == moq::errc::wrong_state);
    }

    // -- 7. Move-only session ------------------------------------------
    {
        auto r = moq::session::create({.perspective = moq::perspective::client});
        MOQ_CHECK(r.ok());
        auto s1 = std::move(*r);
        s1.start(0);

        auto s2 = std::move(s1);
        MOQ_CHECK(s1.raw() == nullptr);
        MOQ_CHECK(s2.raw() != nullptr);
        MOQ_CHECK(s2.state() == moq::session_state::setup_sent);

        auto a = s2.poll_action();
        MOQ_CHECK(a.has_value());
    }

    // -- 8. Subscribe + datagram → event cleanup proof -----------------
    {
        auto [c, s] = establish_pair(failures);

        // Client subscribes via C API
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        moq_bytes_t     ns_parts[] = {MOQ_BYTES_LITERAL("ns")};
        moq_namespace_t ns         = {ns_parts, 1};
        sub_cfg.track_namespace    = ns;
        sub_cfg.track_name         = MOQ_BYTES_LITERAL("video");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub;
        MOQ_CHECK(moq_session_subscribe(c.raw(), &sub_cfg, 0, &sub) == MOQ_OK);
        pump_control(c, s, 0);

        // Server receives subscribe_request via C++
        moq_subscription_t srv_sub{};
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::subscribe_request &req) {
                    srv_sub = req.sub.raw();
                    MOQ_CHECK(req.track_name.string_view() == "video");
                    MOQ_CHECK(req.filter == moq::subscribe_filter::largest_object);
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }

        // Server accepts via C API
        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        MOQ_CHECK(
            moq_session_accept_subscribe(s.raw(), srv_sub, &accept, 0) ==
            MOQ_OK);
        pump_control(s, c, 0);

        // Verify subscribe_ok carries track_alias
        {
            auto ok_ev = c.poll_event();
            MOQ_CHECK(ok_ev.has_value());
            ok_ev->visit(
                [&](const moq::event::subscribe_ok &ok) {
                    MOQ_CHECK(ok.track_alias > 0);
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }

        // Server sends datagram
        moq_rcbuf_t  *payload = nullptr;
        uint8_t       payload_data[] = {0xCA, 0xFE, 0xBA, 0xBE};
        moq_rcbuf_create(moq_alloc_default(), payload_data, 4, &payload);
        moq_session_send_object_datagram(s.raw(), srv_sub, 0, 0, 128, false,
                                         payload, nullptr, 0, 0);
        moq_rcbuf_decref(payload);
        pump_all(s, c, 0);

        // Client receives object — test payload_owned survives event
        moq::buffer owned;
        {
            auto ev = c.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::object_received &obj) {
                    MOQ_CHECK(obj.datagram);
                    MOQ_CHECK(obj.payload_data.size() == 4);
                    MOQ_CHECK(obj.payload_data[0] == 0xCA);
                    owned = obj.payload_owned();
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }
        // polled_event destroyed — owned buffer survives
        MOQ_CHECK(owned.size() == 4);
        MOQ_CHECK(owned.data()[0] == 0xCA);
        MOQ_CHECK(owned.data()[3] == 0xBE);
    }

    // -- 9. Send_data action cleanup proof -----------------------------
    {
        auto [c, s] = establish_pair(failures);

        // Client subscribes via C API
        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        moq_bytes_t     ns_parts[] = {MOQ_BYTES_LITERAL("ns")};
        moq_namespace_t ns_val     = {ns_parts, 1};
        sub_cfg.track_namespace    = ns_val;
        sub_cfg.track_name         = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub;
        moq_session_subscribe(c.raw(), &sub_cfg, 0, &sub);
        pump_control(c, s, 0);

        moq_subscription_t srv_sub{};
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &req) {
                    srv_sub = req.sub.raw();
                },
                [](const auto &) {});
        }

        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        moq_session_accept_subscribe(s.raw(), srv_sub, &accept, 0);
        pump_control(s, c, 0);
        c.poll_event(); // drain subscribe_ok

        // Server opens subgroup + writes object
        moq_subgroup_cfg_t sg_cfg;
        moq_subgroup_cfg_init(&sg_cfg);
        moq_subgroup_handle_t sg;
        moq_session_open_subgroup(s.raw(), srv_sub, &sg_cfg, 0, &sg);

        uint8_t      obj_data[] = {0xDE, 0xAD};
        moq_rcbuf_t *obj_payload = nullptr;
        moq_rcbuf_create(moq_alloc_default(), obj_data, 2, &obj_payload);
        moq_session_write_object(s.raw(), sg, 0, obj_payload, 0);
        moq_rcbuf_decref(obj_payload);

        // Poll send_data actions — capture payload_owned before cleanup
        moq::buffer owned;
        while (auto a = s.poll_action()) {
            a->visit(
                [&](const moq::action::send_data &sd) {
                    if (!sd.payload.empty())
                        owned = sd.payload_owned();
                },
                [](const auto &) {});
        }
        // All polled_actions destroyed (rcbuf cleanup'd). Owned survives.
        MOQ_CHECK(owned.size() == 2);
        MOQ_CHECK(owned.data()[0] == 0xDE);
        MOQ_CHECK(owned.data()[1] == 0xAD);
    }

    // -- 10. polled_action move semantics ------------------------------
    {
        auto r = moq::session::create({.perspective = moq::perspective::client});
        MOQ_CHECK(r.ok());
        auto &s = *r;
        s.start(0);

        auto a1 = s.poll_action();
        MOQ_CHECK(a1.has_value());
        MOQ_CHECK(a1->kind() == MOQ_ACTION_SEND_CONTROL);

        auto a2 = std::move(*a1);
        MOQ_CHECK(a2.kind() == MOQ_ACTION_SEND_CONTROL);
    }

    // -- 11. session_closed event from transport close -------------------
    {
        auto r = moq::session::create({.perspective = moq::perspective::client});
        MOQ_CHECK(r.ok());
        auto &s = *r;
        s.start(0);
        s.poll_action(); // drain CLIENT_SETUP

        s.on_transport_close(0x1, 100);
        MOQ_CHECK(s.state() == moq::session_state::closed);

        auto ev = s.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::session_closed &sc) {
                MOQ_CHECK(sc.code == 0x1);
            },
            [&](const auto &) { MOQ_CHECK(false); });

        // No CLOSE_SESSION action (transport already closed)
        MOQ_CHECK(!s.poll_action().has_value());
    }

    // -- 12. Header compile smoke: all major symbols used --------------
    {
        (void)sizeof(moq::action_variant);
        (void)sizeof(moq::event_variant);
        (void)sizeof(moq::polled_action);
        (void)sizeof(moq::polled_event);
        (void)sizeof(moq::session);
        (void)sizeof(moq::session_config);
        (void)sizeof(moq::action::send_data);
        (void)sizeof(moq::event::object_received);
    }

    // -- 13. next_deadline_us ------------------------------------------
    {
        moq::session_config cfg{};
        cfg.perspective = moq::perspective::client;
        cfg.idle_timeout_us = 5000000;
        auto r = moq::session::create(cfg);
        MOQ_CHECK(r.ok());
        auto &s = *r;
        s.start(1000);
        MOQ_CHECK(s.next_deadline_us() != UINT64_MAX);
    }

    // -- 14. Enum conversion static asserts -----------------------------
    {
        static_assert(moq::to_c(moq::perspective::client) == MOQ_PERSPECTIVE_CLIENT);
        static_assert(moq::to_c(moq::perspective::server) == MOQ_PERSPECTIVE_SERVER);
        static_assert(moq::to_c(moq::session_state::idle) == MOQ_SESS_IDLE);
        static_assert(moq::to_c(moq::session_state::established) == MOQ_SESS_ESTABLISHED);
        static_assert(moq::to_c(moq::session_state::closed) == MOQ_SESS_CLOSED);
        static_assert(moq::to_c(moq::group_order::ascending) == MOQ_GROUP_ORDER_ASCENDING);
        static_assert(moq::to_c(moq::subscribe_filter::largest_object) ==
                      MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT);
        static_assert(moq::to_c(moq::object_status::end_of_track) == MOQ_OBJECT_END_OF_TRACK);
        static_assert(moq::to_c(moq::object_terminal::reset) == MOQ_OBJECT_TERMINAL_RESET);

        static_assert(moq::perspective_from_c(MOQ_PERSPECTIVE_CLIENT) ==
                      moq::perspective::client);
        static_assert(moq::session_state_from_c(MOQ_SESS_DRAINING) ==
                      moq::session_state::draining);
        static_assert(moq::group_order_from_c(MOQ_GROUP_ORDER_DESCENDING) ==
                      moq::group_order::descending);
        static_assert(moq::subscribe_filter_from_c(MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE) ==
                      moq::subscribe_filter::absolute_range);
        static_assert(moq::object_status_from_c(MOQ_OBJECT_END_OF_GROUP) ==
                      moq::object_status::end_of_group);
        static_assert(moq::object_terminal_from_c(MOQ_OBJECT_TERMINAL_STOP) ==
                      moq::object_terminal::stop);
    }

    // -- 15. Object received exposes object_status ---------------------
    {
        auto [c, s] = establish_pair(failures);

        moq_subscribe_cfg_t sub_cfg;
        moq_subscribe_cfg_init(&sub_cfg);
        moq_bytes_t     ns_parts[] = {MOQ_BYTES_LITERAL("ns")};
        moq_namespace_t ns         = {ns_parts, 1};
        sub_cfg.track_namespace    = ns;
        sub_cfg.track_name         = MOQ_BYTES_LITERAL("t");
        sub_cfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;

        moq_subscription_t sub;
        moq_session_subscribe(c.raw(), &sub_cfg, 0, &sub);
        pump_control(c, s, 0);

        moq_subscription_t srv_sub{};
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &req) {
                    srv_sub = req.sub.raw();
                },
                [](const auto &) {});
        }

        moq_accept_subscribe_cfg_t accept;
        moq_accept_subscribe_cfg_init(&accept);
        moq_session_accept_subscribe(s.raw(), srv_sub, &accept, 0);
        pump_control(s, c, 0);
        c.poll_event(); // drain subscribe_ok

        moq_rcbuf_t  *payload = nullptr;
        uint8_t       pd[]    = {0x01};
        moq_rcbuf_create(moq_alloc_default(), pd, 1, &payload);
        moq_session_send_object_datagram(s.raw(), srv_sub, 0, 0, 128, false,
                                         payload, nullptr, 0, 0);
        moq_rcbuf_decref(payload);
        pump_all(s, c, 0);

        auto ev = c.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::object_received &obj) {
                MOQ_CHECK(obj.status == moq::object_status::normal);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 16. on_data_rcbuf with const buffer& (no extra incref) --------
    {
        auto r = moq::session::create({.perspective = moq::perspective::client});
        MOQ_CHECK(r.ok());
        auto &sess = *r;
        sess.start(0);
        sess.poll_action(); // drain

        auto br = moq::buffer::create(nullptr, 0);
        MOQ_CHECK(br.ok());

        // lvalue const-ref — compiles, no extra incref
        const auto &buf = *br;
        (void)sess.on_data_rcbuf(moq::stream_ref(uint64_t(1)), buf, false, 0);
        MOQ_CHECK(moq_rcbuf_refcount(buf.raw()) == 1);

        // rvalue buffer also binds to const-ref
        moq::buffer tmp = std::move(*br);
        (void)sess.on_data_rcbuf(moq::stream_ref(uint64_t(2)), tmp, true, 0);
    }

    // -- Saved value-variant outlives polled_event/action (rcbuf lifetime) --
    // A value variant copied out of poll_event()/poll_action() must keep its
    // rcbuf-backed buffers alive after the polled_* RAII object is destroyed
    // (which calls moq_event_cleanup / moq_action_cleanup). White-box: build the
    // polled object over a WRAPPED rcbuf whose release callback flips `freed`,
    // save the variant past the polled object's scope, and assert the buffer is
    // NOT freed -- a deterministic lifetime check (independent of ASan). Pre-fix
    // the variant copied an unowned raw pointer, so `freed` would already be true.
    {
        const moq_alloc_t *al = moq_alloc_default();
        static const uint8_t data[4] = {0xCA, 0xFE, 0xBA, 0xBE};

        // OBJECT_RECEIVED (payload + properties)
        {
            bool freed = false, pfreed = false;
            moq_rcbuf_t *buf = nullptr, *pbuf = nullptr;
            MOQ_CHECK(moq_rcbuf_wrap(al, data, 4, rcbuf_freed_cb, &freed, &buf)
                      == MOQ_OK);
            MOQ_CHECK(moq_rcbuf_wrap(al, data, 4, rcbuf_freed_cb, &pfreed, &pbuf)
                      == MOQ_OK);
            std::optional<moq::event_variant> saved;
            {
                moq_event_t e{};
                e.kind = MOQ_EVENT_OBJECT_RECEIVED;
                e.u.object_received.payload = buf;   // transfer the wraps' refs
                e.u.object_received.properties = pbuf;
                moq::polled_event pe(e);
                saved = pe.variant();
            }
            MOQ_CHECK(!freed);    // saved variant retained payload
            MOQ_CHECK(!pfreed);   // ...and properties
            auto &obj = std::get<moq::event::object_received>(*saved);
            MOQ_CHECK(obj.payload_data.size() == 4);
            MOQ_CHECK(obj.payload_data[0] == 0xCA && obj.payload_data[3] == 0xBE);
            MOQ_CHECK(obj.properties_data.size() == 4);
            moq::buffer owned = obj.payload_owned();
            moq::buffer powned = obj.properties_owned();
            MOQ_CHECK(owned.size() == 4 && owned.data()[0] == 0xCA);
            MOQ_CHECK(powned.size() == 4 && powned.data()[3] == 0xBE);
            // copy coverage: copying the saved variant retains; both stay valid
            std::optional<moq::event_variant> copy = saved;
            MOQ_CHECK(std::get<moq::event::object_received>(*copy)
                          .payload_owned().size() == 4);
            MOQ_CHECK(std::get<moq::event::object_received>(*copy)
                          .properties_owned().size() == 4);
            saved.reset();
            copy.reset();
            owned = moq::buffer();
            powned = moq::buffer();
            MOQ_CHECK(freed);     // all refs dropped -> released exactly once
            MOQ_CHECK(pfreed);
        }

        // FETCH_OBJECT (payload + properties)
        {
            bool freed = false, pfreed = false;
            moq_rcbuf_t *buf = nullptr, *pbuf = nullptr;
            MOQ_CHECK(moq_rcbuf_wrap(al, data, 4, rcbuf_freed_cb, &freed, &buf)
                      == MOQ_OK);
            MOQ_CHECK(moq_rcbuf_wrap(al, data, 4, rcbuf_freed_cb, &pfreed, &pbuf)
                      == MOQ_OK);
            std::optional<moq::event_variant> saved;
            {
                moq_event_t e{};
                e.kind = MOQ_EVENT_FETCH_OBJECT;
                e.u.fetch_object.payload = buf;
                e.u.fetch_object.properties = pbuf;
                moq::polled_event pe(e);
                saved = pe.variant();
            }
            MOQ_CHECK(!freed);
            MOQ_CHECK(!pfreed);
            auto &obj = std::get<moq::event::fetch_object>(*saved);
            MOQ_CHECK(obj.payload_data.size() == 4 && obj.payload_data[3] == 0xBE);
            MOQ_CHECK(obj.properties_data.size() == 4);
            MOQ_CHECK(obj.payload_owned().size() == 4);
            MOQ_CHECK(obj.properties_owned().size() == 4);
            saved.reset();
            MOQ_CHECK(freed);
            MOQ_CHECK(pfreed);
        }

        // OBJECT_CHUNK (chunk + properties)
        {
            bool freed = false, pfreed = false;
            moq_rcbuf_t *buf = nullptr, *pbuf = nullptr;
            MOQ_CHECK(moq_rcbuf_wrap(al, data, 4, rcbuf_freed_cb, &freed, &buf)
                      == MOQ_OK);
            MOQ_CHECK(moq_rcbuf_wrap(al, data, 4, rcbuf_freed_cb, &pfreed, &pbuf)
                      == MOQ_OK);
            std::optional<moq::event_variant> saved;
            {
                moq_event_t e{};
                e.kind = MOQ_EVENT_OBJECT_CHUNK;
                e.u.object_chunk.chunk = buf;
                e.u.object_chunk.properties = pbuf;
                moq::polled_event pe(e);
                saved = pe.variant();
            }
            MOQ_CHECK(!freed);
            MOQ_CHECK(!pfreed);
            auto &ch = std::get<moq::event::object_chunk>(*saved);
            MOQ_CHECK(ch.chunk_data.size() == 4 && ch.chunk_data[0] == 0xCA);
            MOQ_CHECK(ch.properties_data.size() == 4);
            MOQ_CHECK(ch.chunk_owned().size() == 4);
            MOQ_CHECK(ch.properties_owned().size() == 4);
            saved.reset();
            MOQ_CHECK(freed);
            MOQ_CHECK(pfreed);
        }

        // SEND_DATA (action variant)
        {
            bool freed = false;
            moq_rcbuf_t *buf = nullptr;
            MOQ_CHECK(moq_rcbuf_wrap(al, data, 4, rcbuf_freed_cb, &freed, &buf)
                      == MOQ_OK);
            std::optional<moq::action_variant> saved;
            {
                moq_action_t a{};
                a.kind = MOQ_ACTION_SEND_DATA;
                a.u.send_data.payload = buf;
                moq::polled_action pa(a);
                saved = pa.variant();
            }
            MOQ_CHECK(!freed);
            auto &sd = std::get<moq::action::send_data>(*saved);
            MOQ_CHECK(sd.payload.size() == 4 && sd.payload[3] == 0xBE);
            moq::buffer owned = sd.payload_owned();
            MOQ_CHECK(owned.size() == 4 && owned.data()[0] == 0xCA);
            std::optional<moq::action_variant> copy = saved;
            MOQ_CHECK(std::get<moq::action::send_data>(*copy)
                          .payload_owned().size() == 4);
            saved.reset();
            copy.reset();
            owned = moq::buffer();
            MOQ_CHECK(freed);
        }
    }

    MOQ_PASS("test_cpp_session");
    return failures;
}
