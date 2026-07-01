#include <moq/moq.hpp>
#include "test_support.hpp"

#include <cstring>
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
            [&](const moq::action::close_bidi_stream &bs) {
                to.on_bidi_stream_reset(bs.ref, 0, now);
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

static moq::subscription subscribe_and_accept(moq::session &c,
                                              moq::session &s,
                                              int &failures)
{
    [[maybe_unused]] auto sub = c.subscribe({.ns = {"ns"}, .track = "t"}, 0).value();
    pump(c, s, 0);

    moq::subscription srv_sub;
    {
        auto ev = s.poll_event();
        MOQ_CHECK(ev.has_value());
        ev->visit(
            [&](const moq::event::subscribe_request &req) {
                srv_sub = req.sub;
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    MOQ_CHECK(s.accept_subscribe(srv_sub, {}, 0).ok());
    pump(s, c, 0);
    c.poll_event();
    return srv_sub;
}

int main()
{
    int failures = 0;

    // -- 1. Subscription update + publisher-side done -------------------
    {
        auto [c, s] = establish(failures);
        /* SUBSCRIBE may carry a new-group request blind (no support
         * foreknowledge). */
        auto sub = c.subscribe({.ns = {"ns"}, .track = "t",
                                .has_new_group_request = true,
                                .new_group_request = 0}, 0).value();
        pump(c, s, 0);

        moq::subscription srv_sub;
        {
            auto ev = s.poll_event();
            ev->visit(
                [&](const moq::event::subscribe_request &req) {
                    srv_sub = req.sub;
                    MOQ_CHECK(req.has_new_group_request);
                    MOQ_CHECK(req.new_group_request == 0);
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }
        /* Accept advertising DYNAMIC_GROUPS=1 (KVP 0x30, varint 1) so the
         * subscriber may put new-group requests on later updates. */
        static const uint8_t dyn_props[] = { 0x30, 0x01 };
        MOQ_CHECK(s.accept_subscribe(srv_sub,
            {.track_properties = moq::bytes_view(dyn_props, 2)}, 0).ok());
        pump(s, c, 0);
        {
            auto ok_ev = c.poll_event();
            MOQ_CHECK(ok_ev.has_value());
            ok_ev->visit(
                [&](const moq::event::subscribe_ok &ok) {
                    MOQ_CHECK(ok.dynamic_groups);
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }

        const moq_auth_token_t upd_tok = {
            .token_type = 11,
            .token_value = { (const uint8_t *)"fresh", 5 },
        };
        auto ur = c.update_subscription(sub,
            {.has_subscriber_priority = true, .subscriber_priority = 7,
             .has_forward = true, .forward = false,
             .has_delivery_timeout = true, .delivery_timeout_us = 9000,
             .auth_tokens = {&upd_tok, 1},
             .has_new_group_request = true, .new_group_request = 5},
            0);
        MOQ_CHECK(ur.ok());
        pump(c, s, 0);

        auto uev = s.poll_event();
        MOQ_CHECK(uev.has_value());
        uev->visit(
            [&](const moq::event::subscribe_updated &up) {
                MOQ_CHECK(up.sub == srv_sub);
                MOQ_CHECK(up.has_subscriber_priority);
                MOQ_CHECK(up.subscriber_priority == 7);
                MOQ_CHECK(up.has_forward);
                MOQ_CHECK(!up.forward);
                MOQ_CHECK(up.has_delivery_timeout);
                MOQ_CHECK(up.delivery_timeout_us == 9000);
                MOQ_CHECK(up.tokens.size() == 1);
                MOQ_CHECK(up.tokens[0].token_type == 11);
                MOQ_CHECK(up.tokens[0].token_value.len == 5);
                MOQ_CHECK(std::memcmp(up.tokens[0].token_value.data,
                                      "fresh", 5) == 0);
                MOQ_CHECK(up.has_new_group_request);
                MOQ_CHECK(up.new_group_request == 5);
            },
            [&](const auto &) { MOQ_CHECK(false); });
        pump(s, c, 0); // REQUEST_OK for the update.

        auto dr = s.done_subscribe(srv_sub,
            {.status_code = 9, .stream_count = 2, .reason = "done"},
            0);
        MOQ_CHECK(dr.ok());
        pump(s, c, 0);

        auto dev = c.poll_event();
        MOQ_CHECK(dev.has_value());
        dev->visit(
            [&](const moq::event::subscribe_done &done) {
                MOQ_CHECK(done.sub == sub);
                MOQ_CHECK(done.status_code == 9);
                MOQ_CHECK(done.stream_count == 2);
                MOQ_CHECK(done.reason == "done");
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 2. FETCH request, accept, object, completion --------------------
    {
        auto [c, s] = establish(failures);

        auto fh = c.fetch({.ns = {"archive"}, .track = "video",
                           .start_group = 1, .start_object = 0,
                           .end_group = 2, .end_object = 0},
                          0);
        MOQ_CHECK(fh.ok());
        pump(c, s, 0);

        moq::fetch_handle srv_fetch;
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::fetch_request &fr) {
                    srv_fetch = fr.fetch;
                    MOQ_CHECK(fr.track_namespace()[0] == "archive");
                    MOQ_CHECK(fr.track_name.string_view() == "video");
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }

        MOQ_CHECK(s.accept_fetch(srv_fetch,
            {.end_group = 1, .end_object = 1}, 0).ok());
        auto payload = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("xy"), 2).value();
        MOQ_CHECK(s.write_fetch_object(srv_fetch,
            {.group_id = 1, .subgroup_id = 0, .object_id = 0},
            payload, 0).ok());
        MOQ_CHECK(s.end_fetch(srv_fetch, 0).ok());
        pump(s, c, 0);

        bool got_ok = false;
        bool got_obj = false;
        bool got_complete = false;
        while (auto ev = c.poll_event()) {
            ev->visit(
                [&](const moq::event::fetch_ok &ok) {
                    got_ok = true;
                    MOQ_CHECK(ok.fetch == *fh);
                },
                [&](const moq::event::fetch_object &obj) {
                    got_obj = true;
                    MOQ_CHECK(obj.fetch == *fh);
                    MOQ_CHECK(obj.payload_data.size() == 2);
                    MOQ_CHECK(obj.payload_data[0] == 'x');
                    MOQ_CHECK(obj.payload_data[1] == 'y');
                },
                [&](const moq::event::fetch_complete &done) {
                    got_complete = true;
                    MOQ_CHECK(done.fetch == *fh);
                },
                [](const auto &) {});
        }
        MOQ_CHECK(got_ok);
        MOQ_CHECK(got_obj);
        MOQ_CHECK(got_complete);
    }

    // -- 3. PUBLISH accept, update, datagram, finish --------------------
    {
        auto [c, s] = establish(failures);

        auto pub = c.publish({.ns = {"live"}, .track = "video",
                              .has_track_alias = true, .track_alias = 88,
                              .has_forward = true, .forward = true},
                             0);
        MOQ_CHECK(pub.ok());
        pump(c, s, 0);

        moq::publication srv_pub;
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::publish_request &pr) {
                    srv_pub = pr.pub;
                    MOQ_CHECK(pr.track_alias == 88);
                    MOQ_CHECK(pr.track_namespace()[0] == "live");
                    MOQ_CHECK(pr.track_name.string_view() == "video");
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }

        MOQ_CHECK(s.accept_publish(srv_pub,
            {.has_subscriber_priority = true,
             .subscriber_priority = 33,
             .group_order = moq::group_order::ascending},
            0).ok());
        pump(s, c, 0);
        auto ok_ev = c.poll_event();
        MOQ_CHECK(ok_ev.has_value());
        ok_ev->visit(
            [&](const moq::event::publish_ok &ok) {
                MOQ_CHECK(ok.pub == *pub);
                MOQ_CHECK(ok.subscriber_priority == 33);
                MOQ_CHECK(ok.group_order == moq::group_order::ascending);
            },
            [&](const auto &) { MOQ_CHECK(false); });

        const moq_auth_token_t pub_tok = {
            .token_type = 9,
            .token_value = { (const uint8_t *)"renew", 5 },
        };
        MOQ_CHECK(s.update_publication(srv_pub,
            {.has_forward = true, .forward = false,
             .auth_tokens = {&pub_tok, 1}}, 0).ok());
        pump(s, c, 0);
        auto up_ev = c.poll_event();
        MOQ_CHECK(up_ev.has_value());
        up_ev->visit(
            [&](const moq::event::publish_updated &up) {
                MOQ_CHECK(up.pub == *pub);
                MOQ_CHECK(up.has_forward);
                MOQ_CHECK(!up.forward);
                MOQ_CHECK(up.tokens.size() == 1);
                MOQ_CHECK(up.tokens[0].token_type == 9);
                MOQ_CHECK(up.tokens[0].token_value.len == 5);
                MOQ_CHECK(std::memcmp(up.tokens[0].token_value.data,
                                      "renew", 5) == 0);
            },
            [&](const auto &) { MOQ_CHECK(false); });
        pump(c, s, 0); // REQUEST_OK for the update.

        /* This publication opened no data streams (datagram-only path), so its
         * PUBLISH_DONE Stream Count is 0, which finalizes immediately. A non-zero
         * count would (correctly) defer PUBLISH_FINISHED until that many data
         * streams were processed -- streams this publication never opens. */
        MOQ_CHECK(c.finish_publish(*pub,
            {.status_code = 5, .stream_count = 0, .reason = "bye"}, 0).ok());
        pump(c, s, 0);
        auto fin_ev = s.poll_event();
        MOQ_CHECK(fin_ev.has_value());
        fin_ev->visit(
            [&](const moq::event::publish_finished &fin) {
                MOQ_CHECK(fin.pub == srv_pub);
                MOQ_CHECK(fin.status_code == 5);
                MOQ_CHECK(fin.stream_count == 0);
                MOQ_CHECK(fin.reason == "bye");
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 4. Namespace advertisement + track status ----------------------
    {
        auto [c, s] = establish(failures);

        auto ann = c.publish_namespace({.ns = {"live"}}, 0);
        MOQ_CHECK(ann.ok());
        pump(c, s, 0);

        moq::announcement srv_ann;
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::namespace_published &np) {
                    srv_ann = np.ann;
                    MOQ_CHECK(np.track_namespace()[0] == "live");
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }
        MOQ_CHECK(s.accept_namespace(srv_ann, 0).ok());
        pump(s, c, 0);
        auto nev = c.poll_event();
        MOQ_CHECK(nev.has_value());
        nev->visit(
            [&](const moq::event::namespace_accepted &na) {
                MOQ_CHECK(na.ann == *ann);
            },
            [&](const auto &) { MOQ_CHECK(false); });

        auto ts = c.track_status({.ns = {"live"}, .track = "video"}, 0);
        MOQ_CHECK(ts.ok());
        pump(c, s, 0);
        moq::track_status_handle srv_ts;
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::track_status_request &req) {
                    srv_ts = req.handle;
                    MOQ_CHECK(req.track_namespace()[0] == "live");
                    MOQ_CHECK(req.track_name.string_view() == "video");
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }
        MOQ_CHECK(s.accept_track_status(srv_ts, 0).ok());
        pump(s, c, 0);
        auto tsev = c.poll_event();
        MOQ_CHECK(tsev.has_value());
        tsev->visit(
            [&](const moq::event::track_status_ok &ok) {
                MOQ_CHECK(ok.handle == *ts);
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 5. Namespace subscription round-trip ---------------------------
    {
        auto [c, s] = establish(failures);

        auto ns = c.subscribe_namespace(
            {.ns_prefix = {"live"},
             .interest = moq::namespace_interest::both},
            0);
        MOQ_CHECK(ns.ok());
        pump(c, s, 0);

        moq::ns_sub_handle srv_ns;
        {
            auto ev = s.poll_event();
            MOQ_CHECK(ev.has_value());
            ev->visit(
                [&](const moq::event::ns_sub_request &req) {
                    srv_ns = req.handle;
                    MOQ_CHECK(req.track_namespace_prefix()[0] == "live");
                },
                [&](const auto &) { MOQ_CHECK(false); });
        }
        MOQ_CHECK(s.accept_ns_sub(srv_ns, 0).ok());
        pump(s, c, 0);
        auto ok = c.poll_event();
        MOQ_CHECK(ok.has_value());
        ok->visit(
            [&](const moq::event::ns_sub_ok &ev) {
                MOQ_CHECK(ev.handle == *ns);
            },
            [&](const auto &) { MOQ_CHECK(false); });

        MOQ_CHECK(s.send_namespace(srv_ns, {"camera"}, 0).ok());
        pump(s, c, 0);
        auto found = c.poll_event();
        MOQ_CHECK(found.has_value());
        found->visit(
            [&](const moq::event::namespace_found &ev) {
                MOQ_CHECK(ev.handle == *ns);
                MOQ_CHECK(ev.track_namespace_suffix()[0] == "camera");
            },
            [&](const auto &) { MOQ_CHECK(false); });

        MOQ_CHECK(s.send_namespace_done(srv_ns, {"camera"}, 0).ok());
        pump(s, c, 0);
        auto gone = c.poll_event();
        MOQ_CHECK(gone.has_value());
        gone->visit(
            [&](const moq::event::namespace_gone &ev) {
                MOQ_CHECK(ev.handle == *ns);
                MOQ_CHECK(ev.track_namespace_suffix()[0] == "camera");
            },
            [&](const auto &) { MOQ_CHECK(false); });
    }

    // -- 6. Streaming object wrapper ------------------------------------
    {
        auto [c, s] = establish(failures);
        auto srv_sub = subscribe_and_accept(c, s, failures);
        auto sg = s.open_subgroup(srv_sub, {}, 0).value();

        MOQ_CHECK(s.begin_object(sg, {.object_id = 10, .payload_length = 3}, 0).ok());
        auto chunk = moq::buffer::create(
            reinterpret_cast<const uint8_t *>("abc"), 3).value();
        MOQ_CHECK(s.write_object_data(sg, chunk, 0).ok());
        MOQ_CHECK(s.end_object(sg, 0).ok());
        MOQ_CHECK(s.close_subgroup(sg, 0).ok());
        pump(s, c, 0);

        bool got_object = false;
        while (auto ev = c.poll_event()) {
            ev->visit(
                [&](const moq::event::object_received &obj) {
                    got_object = true;
                    MOQ_CHECK(obj.object_id == 10);
                    MOQ_CHECK(obj.payload_data.size() == 3);
                    MOQ_CHECK(obj.payload_data[0] == 'a');
                    MOQ_CHECK(obj.payload_data[2] == 'c');
                },
                [](const auto &) {});
        }
        MOQ_CHECK(got_object);
    }

    // -- 7. New enum wrappers -------------------------------------------
    {
        static_assert(moq::to_c(moq::namespace_interest::publisher_state) ==
                      MOQ_NAMESPACE_INTEREST_PUBLISHER_STATE);
        static_assert(moq::to_c(moq::namespace_interest::namespace_state) ==
                      MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE);
        static_assert(moq::to_c(moq::namespace_interest::both) ==
                      MOQ_NAMESPACE_INTEREST_BOTH);
        static_assert(moq::namespace_interest_from_c(MOQ_NAMESPACE_INTEREST_BOTH) ==
                      moq::namespace_interest::both);
        static_assert(moq::to_c(moq::fetch_range_kind::non_existent) ==
                      MOQ_FETCH_RANGE_NON_EXISTENT);
        static_assert(moq::to_c(moq::fetch_range_kind::unknown) ==
                      MOQ_FETCH_RANGE_UNKNOWN);
        static_assert(moq::fetch_range_kind_from_c(MOQ_FETCH_RANGE_UNKNOWN) ==
                      moq::fetch_range_kind::unknown);
    }

    MOQ_PASS("test_cpp_advanced_operations");
    return failures;
}
