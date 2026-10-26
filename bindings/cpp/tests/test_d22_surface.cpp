// The draft-22 session surface through the C++ binding, beyond fills:
// session config fields reach the C session; update answers owed under
// defer_subscription_updates (both the SUBSCRIBE and the PUBLISH side, with
// the owed-update and filter queries and the runtime setter); FETCH Location
// Filter, FILL_TIMEOUT and the options a deferring publisher sees; the
// Timed-Out range writer; the capability and version queries; request
// terminal causes; and fills through the subscriber facade. Every config is
// a plain value copied for the call; every event field is copied out.
#include <moq/moq.hpp>
#include "test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

static constexpr uint64_t k_peer_bit[2] = {UINT64_C(1) << 40, UINT64_C(1) << 41};

static moq::stream_ref to_peer(moq::stream_ref ref, int from)
{
    const uint64_t v = ref.value();
    const uint64_t back = k_peer_bit[from ^ 1];
    if (v & back) return moq::stream_ref(v & ~back);
    return moq::stream_ref(v | k_peer_bit[from]);
}

// Request streams each direction opened, in order, as the opener names them.
struct opened_streams {
    std::vector<moq::stream_ref> by_dir[2];
};

static void pump_all(moq::session &from, moq::session &to, uint64_t now, int dir,
                     opened_streams *opened)
{
    while (auto a = from.poll_action()) {
        a->visit_extended(
            [&](const moq::action::send_control &sc) { to.on_control_bytes(sc.data, now); },
            [&](const moq::action::open_uni_control &uc) { to.on_control_bytes(uc.data, now); },
            [&](const moq::action::send_uni_control &uc) { to.on_control_bytes(uc.data, now); },
            [&](const moq::action::send_data &sd) {
                std::vector<uint8_t> buf;
                buf.insert(buf.end(), sd.header.begin(), sd.header.end());
                buf.insert(buf.end(), sd.payload.begin(), sd.payload.end());
                to.on_data_bytes(to_peer(sd.ref, dir), buf, sd.fin, now);
            },
            [&](const moq::extended_action::reset_data &rd) {
                to.on_data_reset(to_peer(rd.ref, dir), rd.error_code, now);
            },
            [&](const moq::action::stop_data &sd) {
                to.on_data_stop(to_peer(sd.ref, dir), sd.error_code, now);
            },
            [&](const moq::action::open_bidi_stream &bs) {
                if (opened) opened->by_dir[dir].push_back(bs.ref);
                to.on_bidi_stream_bytes(to_peer(bs.ref, dir), bs.data, bs.fin, now);
            },
            [&](const moq::action::send_bidi_stream &bs) {
                to.on_bidi_stream_bytes(to_peer(bs.ref, dir), bs.data, bs.fin, now);
            },
            [&](const moq::action::reset_bidi_stream &rb) {
                to.on_bidi_stream_reset(to_peer(rb.ref, dir), rb.error_code, now);
            },
            [&](const moq::action::stop_bidi_stream &sb) {
                to.on_bidi_stream_stop(to_peer(sb.ref, dir), sb.error_code, now);
            },
            [&](const moq::action::abort_bidi_stream &ab) {
                to.on_bidi_stream_reset(to_peer(ab.ref, dir), ab.error_code, now);
                to.on_bidi_stream_stop(to_peer(ab.ref, dir), ab.error_code, now);
            },
            [&](const moq::action::send_datagram &dg) { to.on_datagram(dg.data, now); },
            [](const auto &) {});
    }
}

static void settle(moq::session &c, moq::session &s, uint64_t now,
                   opened_streams *opened = nullptr)
{
    for (int i = 0; i < 6; i++) {
        pump_all(c, s, now, 0, opened);
        pump_all(s, c, now, 1, opened);
    }
}

struct session_pair {
    moq::session client;
    moq::session server;
};

static uint32_t accept_any_uri(void *, uint32_t, moq_bytes_t, moq_bytes_t)
{
    return MOQ_URI_ACCEPT;
}

static void bind_if_required(int &failures, moq::session &s, bool client)
{
    if (s.binding_requirements() == 0) return;
    moq::transport_binding_config b{};
    b.kind            = MOQ_BINDING_RAW_QUIC;
    b.facts_known     = MOQ_BINDING_FACTS_ALL;
    b.facts_supported = MOQ_BINDING_FACTS_ALL;
    if (client) {
        b.target_present = MOQ_BINDING_TARGET_COMPLETE;
        b.authority      = "sim.invalid";
    } else {
        b.accept_uri = accept_any_uri;
    }
    MOQ_CHECK(s.bind_transport(b).ok());
}

static session_pair establish_pair(int &failures, moq::session_config ccfg,
                                   moq::session_config scfg,
                                   moq::session_options coptions = {},
                                   moq::session_options soptions = {})
{
    ccfg.perspective              = moq::perspective::client;
    ccfg.send_request_capacity    = true;
    ccfg.initial_request_capacity = 16;
    scfg.perspective              = moq::perspective::server;
    scfg.send_request_capacity    = true;
    scfg.initial_request_capacity = 16;

    auto cr = moq::session::create(ccfg, coptions, 0);
    auto sr = moq::session::create(scfg, soptions, 0);
    MOQ_CHECK(cr.ok());
    MOQ_CHECK(sr.ok());
    auto c = std::move(*cr);
    auto s = std::move(*sr);
    bind_if_required(failures, c, true);
    bind_if_required(failures, s, false);
    MOQ_CHECK(c.start(0).ok());
    if (ccfg.version != MOQ_VERSION_DRAFT_16) MOQ_CHECK(s.start(0).ok());
    settle(c, s, 0);
    MOQ_CHECK(c.state() == moq::session_state::established);
    MOQ_CHECK(s.state() == moq::session_state::established);
    while (c.poll_event()) {}
    while (s.poll_event()) {}
    return {std::move(c), std::move(s)};
}

static session_pair establish_pair(int &failures, moq_version_t ver)
{
    moq::session_config c{}, s{};
    c.version = ver;
    s.version = ver;
    return establish_pair(failures, c, s);
}

// Subscribe from the client and accept on the server with Largest {3,2}.
static std::pair<moq::subscription, moq::subscription>
subscribe_accepted(int &failures, session_pair &p,
                   std::optional<moq::location_filter_config> location = {},
                   opened_streams *opened = nullptr)
{
    moq::subscribe_config sc{};
    sc.ns       = {"d22"};
    sc.track    = "v";
    moq::subscribe_options options{};
    options.location = location;
    auto r = p.client.subscribe(sc, options, 1);
    MOQ_CHECK(r.ok());
    if (!r.ok()) return {};
    settle(p.client, p.server, 1, opened);
    moq::subscription ssub{};
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::extended_event::subscribe_request &sr) { ssub = sr.sub; },
                  [](const auto &) {});
    MOQ_CHECK(ssub.valid());
    moq::accept_subscribe_config ac{};
    ac.has_largest    = true;
    ac.largest_group  = 3;
    ac.largest_object = 2;
    MOQ_CHECK(p.server.accept_subscribe(ssub, ac, 2).ok());
    settle(p.client, p.server, 2, opened);
    while (p.client.poll_event()) {}
    return {*r, ssub};
}

// -- Capability and version queries ----------------------------------------

static void test_capabilities_and_versions(int &failures)
{
    auto p22 = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    MOQ_CHECK(p22.client.supports_fill());
    MOQ_CHECK(!p22.client.supports_joining_fetch());
    MOQ_CHECK(p22.client.supports_concurrent_subscriptions());

    auto p18 = establish_pair(failures, MOQ_VERSION_DRAFT_18);
    MOQ_CHECK(!p18.client.supports_fill());
    MOQ_CHECK(p18.client.supports_joining_fetch());
    MOQ_CHECK(!p18.client.supports_concurrent_subscriptions());

    // The list is the library's, newest first, and agrees with the
    // per-version query.
    auto vs = moq::session::versions_supported();
    std::vector<moq_version_t> c(moq_versions_supported(nullptr, 0));
    moq_versions_supported(c.data(), c.size());
    MOQ_CHECK(!vs.empty());
    MOQ_CHECK(vs == c);
    for (auto v : vs) MOQ_CHECK(moq::session::version_supported(v));
    /* Unsupported, but inside the public enum's representable range. */
    MOQ_CHECK(!moq::session::version_supported(static_cast<moq_version_t>(17)));
    MOQ_CHECK(std::is_sorted(vs.begin(), vs.end(), [](moq_version_t a, moq_version_t b) {
        return static_cast<uint32_t>(a) > static_cast<uint32_t>(b);
    }));
    MOQ_PASS("capabilities_and_versions");
}

// -- Session config fields reach the session ---------------------------------

static void test_session_config_fields(int &failures)
{
    // max_fills: a pool beyond 65536 is refused at create, so the field is
    // read, not dropped.
    moq::session_config bad{};
    bad.version   = MOQ_VERSION_DRAFT_22;
    moq::session_options options{};
    options.max_fills = 0x10001;
    auto r = moq::session::create(bad, options, 0);
    MOQ_CHECK(!r.ok());
    if (!r.ok()) MOQ_CHECK(r.error().code() == moq::errc::invalid);
    options.max_fills = 0x10000;
    MOQ_CHECK(moq::session::create(bad, options, 0).ok());

    MOQ_PASS("session_config_fields");
}

// -- Subscription update answers --------------------------------------------

static void test_subscription_update_deferral(int &failures)
{
    moq::session_config c{}, s{};
    c.version = s.version = MOQ_VERSION_DRAFT_22;
    auto p = establish_pair(failures, c, s, {}, {.defer_subscription_updates = true});
    moq::location_filter_config next{moq::location_filter_kind::next_object};
    auto [csub, ssub] = subscribe_accepted(failures, p, next);

    // Both sides read the filter in effect.
    auto cf = p.client.subscription_filter(csub);
    auto sf = p.server.subscription_filter(ssub);
    MOQ_CHECK(cf.ok() && sf.ok());
    if (cf.ok()) MOQ_CHECK(cf->kind == moq::location_filter_kind::next_object);
    if (sf.ok()) MOQ_CHECK(sf->kind == moq::location_filter_kind::next_object);
    MOQ_CHECK(p.server.subscription_owed_update(ssub).error().code() ==
              moq::errc::wrong_state);

    // An update with a new filter is surfaced with its answer owed.
    moq::subscription_update_config uc{};
    uc.has_forward  = true;
    uc.forward      = false;
    uc.has_filter   = true;
    uc.filter       = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    uc.start_group  = 5;
    uc.start_object = 1;
    MOQ_CHECK(p.client.update_subscription(csub, uc, 3).ok());
    settle(p.client, p.server, 3);
    std::optional<moq::extended_event::subscribe_updated> upd;
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::extended_event::subscribe_updated &u) { upd = u; },
                  [](const auto &) {});
    MOQ_CHECK(upd.has_value());
    if (!upd) return;
    MOQ_CHECK(upd->sub == ssub);
    MOQ_CHECK(upd->answer_owed);
    MOQ_CHECK(!upd->fill_requested);
    MOQ_CHECK(upd->update_count == 1);
    MOQ_CHECK(upd->has_forward && !upd->forward);

    // The owed-update query names the same update and its filter; the
    // filter in effect is still the old one until the answer.
    auto owed = p.server.subscription_owed_update(ssub);
    MOQ_CHECK(owed.ok());
    if (owed.ok()) {
        MOQ_CHECK(owed->update_request_id == upd->update_request_id);
        MOQ_CHECK(owed->filter.has_value());
        if (owed->filter) {
            MOQ_CHECK(owed->filter->kind == moq::location_filter_kind::absolute_start);
            MOQ_CHECK(owed->filter->start_group == 5);
            MOQ_CHECK(owed->filter->start_object == 1);
        }
    }
    sf = p.server.subscription_filter(ssub);
    if (sf.ok()) MOQ_CHECK(sf->kind == moq::location_filter_kind::next_object);
    // Nothing reaches the subscriber before the answer.
    int early = 0;
    while (auto ev = p.client.poll_event())
        ev->visit_extended([&](const moq::event::subscription_update_ok &) { early++; },
                  [](const auto &) {});
    MOQ_CHECK(early == 0);

    // An answer naming another update is refused and changes nothing.
    moq::accept_subscription_update_config acc{};
    acc.update_request_id = upd->update_request_id + 2;
    MOQ_CHECK(!p.server.accept_subscription_update(ssub, acc, 4).ok());
    MOQ_CHECK(p.server.subscription_owed_update(ssub).ok());

    acc.update_request_id = upd->update_request_id;
    MOQ_CHECK(p.server.accept_subscription_update(ssub, acc, 4).ok());
    settle(p.client, p.server, 4);
    int oks = 0;
    while (auto ev = p.client.poll_event())
        ev->visit_extended([&](const moq::event::subscription_update_ok &o) {
                      oks++;
                      MOQ_CHECK(o.sub == csub);
                  },
                  [](const auto &) {});
    MOQ_CHECK(oks == 1);
    sf = p.server.subscription_filter(ssub);
    if (sf.ok()) {
        MOQ_CHECK(sf->kind == moq::location_filter_kind::absolute_start);
        MOQ_CHECK(sf->start_group == 5);
    }
    MOQ_CHECK(p.server.subscription_owed_update(ssub).error().code() ==
              moq::errc::wrong_state);

    // The setter switches the session back to answering by itself.
    MOQ_CHECK(p.server.set_defer_subscription_updates(false).ok());
    moq::subscription_update_config uc2{};
    uc2.has_subscriber_priority = true;
    uc2.subscriber_priority     = 7;
    MOQ_CHECK(p.client.update_subscription(csub, uc2, 5).ok());
    settle(p.client, p.server, 5);
    upd.reset();
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::extended_event::subscribe_updated &u) { upd = u; },
                  [](const auto &) {});
    MOQ_CHECK(upd.has_value());
    if (upd) {
        MOQ_CHECK(!upd->answer_owed);
        MOQ_CHECK(upd->update_count == 1);
    }
    oks = 0;
    while (auto ev = p.client.poll_event())
        ev->visit_extended([&](const moq::event::subscription_update_ok &) { oks++; },
                  [](const auto &) {});
    MOQ_CHECK(oks == 1);

    // Deferred again, then rejected: the subscription ends and its handle
    // is stale on the publisher side.
    MOQ_CHECK(p.server.set_defer_subscription_updates(true).ok());
    MOQ_CHECK(p.client.update_subscription(csub, uc2, 6).ok());
    settle(p.client, p.server, 6);
    upd.reset();
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::extended_event::subscribe_updated &u) { upd = u; },
                  [](const auto &) {});
    MOQ_CHECK(upd && upd->answer_owed);
    if (!upd) return;
    moq::reject_subscription_update_config rej{};
    rej.update_request_id = upd->update_request_id;
    rej.error_code        = moq::request_errors::invalid_filter;
    rej.reason            = "no";
    MOQ_CHECK(p.server.reject_subscription_update(ssub, rej, 7).ok());
    MOQ_CHECK(p.server.subscription_filter(ssub).error().code() ==
              moq::errc::stale_handle);
    settle(p.client, p.server, 7);
    MOQ_CHECK(p.client.state() == moq::session_state::established);
    MOQ_PASS("subscription_update_deferral");
}

// -- Publication update answers ---------------------------------------------

static void test_publication_update_deferral(int &failures)
{
    moq::session_config c{}, s{};
    c.version = s.version = MOQ_VERSION_DRAFT_22;
    auto p = establish_pair(failures, c, s, {.defer_subscription_updates = true});

    moq::publish_config pc{};
    pc.ns    = {"d22"};
    pc.track = "p";
    auto pr = p.client.publish(pc, 1);
    MOQ_CHECK(pr.ok());
    if (!pr.ok()) return;
    auto cpub = *pr;
    settle(p.client, p.server, 1);
    std::optional<moq::event::publish_request> req;
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::event::publish_request &r) { req = r; },
                  [](const auto &) {});
    MOQ_CHECK(req.has_value());
    if (!req) return;
    // The default publisher policy explicitly advertises middle priority.
    auto spub = req->pub;
    auto initial = p.server.get_publish_initial_parameters(spub);
    MOQ_CHECK(initial.ok());
    if (initial.ok()) {
        MOQ_CHECK(initial->has_subscriber_priority);
        MOQ_CHECK(initial->subscriber_priority == 128);
        MOQ_CHECK(!initial->location);
    }
    auto pf = p.server.publication_filter(spub);
    MOQ_CHECK(pf.ok());
    if (pf.ok()) MOQ_CHECK(pf->kind == moq::location_filter_kind::none);
    MOQ_CHECK(p.server.accept_publish(spub, moq::accept_publish_config{}, 2).ok());
    settle(p.client, p.server, 2);
    while (p.client.poll_event()) {}

    moq::publication_update_config uc{};
    uc.has_forward = true;
    uc.forward     = false;
    MOQ_CHECK(p.server.update_publication(spub, uc, 3).ok());
    settle(p.client, p.server, 3);
    std::optional<moq::extended_event::publish_updated> upd;
    while (auto ev = p.client.poll_event())
        ev->visit_extended([&](const moq::extended_event::publish_updated &u) { upd = u; },
                  [](const auto &) {});
    MOQ_CHECK(upd.has_value());
    if (!upd) return;
    MOQ_CHECK(upd->pub == cpub);
    MOQ_CHECK(upd->answer_owed);
    MOQ_CHECK(upd->update_count == 1);
    MOQ_CHECK(upd->has_forward && !upd->forward);

    moq::accept_subscription_update_config acc{};
    acc.update_request_id = upd->update_request_id;
    MOQ_CHECK(p.client.accept_publication_update(cpub, acc, 4).ok());
    settle(p.client, p.server, 4);
    int oks = 0;
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::event::publication_update_ok &o) {
                      oks++;
                      MOQ_CHECK(o.pub == spub);
                  },
                  [](const auto &) {});
    MOQ_CHECK(oks == 1);

    // A second update, rejected: the publication ends and its handle is
    // stale on the PUBLISH side.
    MOQ_CHECK(p.server.update_publication(spub, uc, 5).ok());
    settle(p.client, p.server, 5);
    upd.reset();
    while (auto ev = p.client.poll_event())
        ev->visit_extended([&](const moq::extended_event::publish_updated &u) { upd = u; },
                  [](const auto &) {});
    MOQ_CHECK(upd && upd->answer_owed);
    if (!upd) return;
    moq::reject_subscription_update_config rej{};
    rej.update_request_id = upd->update_request_id;
    rej.error_code        = moq::request_error::internal_error;
    MOQ_CHECK(p.client.reject_publication_update(cpub, rej, 6).ok());
    MOQ_CHECK(p.client.publication_filter(cpub).error().code() ==
              moq::errc::stale_handle);
    MOQ_PASS("publication_update_deferral");
}

// -- FETCH: Location Filter, FILL_TIMEOUT, options, Timed-Out ------------------

static std::optional<moq::extended_event::fetch_request> next_fetch_request(session_pair &p)
{
    std::optional<moq::extended_event::fetch_request> out;
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::extended_event::fetch_request &r) {
                      // The value outlives the polled event: options are copied.
                      out = r;
                  },
                  [](const auto &) {});
    return out;
}

static void test_fetch_options_and_timed_out(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    MOQ_CHECK(p.server.set_defer_fetch_ranges(true).ok());

    // A relative start with FILL_TIMEOUT 0 reaches the publisher unresolved.
    moq::fetch_config fc{};
    moq::fetch_options fo{};
    fc.ns    = {"d22"};
    fc.track = "f";
    fo.location = moq::location_filter_config{};
    fo.location->kind                  = moq::location_filter_kind::relative_group;
    fo.location->relative_group_offset = 2;
    fo.has_fill_timeout = true;
    fo.fill_timeout_ms  = 0;
    auto fr = p.client.fetch(fc, fo, 1);
    MOQ_CHECK(fr.ok());
    if (!fr.ok()) return;
    auto cfetch = *fr;
    settle(p.client, p.server, 1);
    auto req = next_fetch_request(p);
    MOQ_CHECK(req.has_value());
    if (!req) return;
    MOQ_CHECK(req->options.has_value());
    if (!req->options) return;
    MOQ_CHECK(req->options->range_unresolved);
    MOQ_CHECK(req->options->has_fill_timeout);
    MOQ_CHECK(req->options->fill_timeout_ms == 0);
    MOQ_CHECK(req->options->include_properties);
    MOQ_CHECK(req->options->location.has_value());
    if (req->options->location) {
        MOQ_CHECK(req->options->location->kind ==
                  moq::location_filter_kind::relative_group);
        MOQ_CHECK(req->options->location->relative_group_offset == 2);
    }

    // The publisher resolves it against its Largest {5,3}: start {4,0}.
    moq::accept_fetch_config ac{};
    ac.end_group  = 5;
    ac.end_object = 4;   // exclusive: through {5,3}
    MOQ_CHECK(p.server.accept_fetch(req->fetch, ac, 2).ok());
    // Timed-Out marks a range the publisher waited for and gave up on.
    MOQ_CHECK(p.server.write_fetch_range(req->fetch, moq::fetch_ranges::timed_out,
                                         4, 0, 2).ok());
    MOQ_CHECK(p.server.end_fetch(req->fetch, 3).ok());
    settle(p.client, p.server, 3);
    int oks = 0, gaps = 0, done = 0;
    while (auto ev = p.client.poll_event())
        ev->visit_extended(
            [&](const moq::event::fetch_ok &o) {
                oks++;
                MOQ_CHECK(o.fetch == cfetch);
                MOQ_CHECK(o.end_group == 5);
            },
            [&](const moq::event::fetch_gap &g) {
                gaps++;
                MOQ_CHECK(g.range_kind == moq::fetch_ranges::timed_out);
            },
            [&](const moq::event::fetch_complete &) { done++; },
            [](const auto &) {});
    MOQ_CHECK(oks == 1);
    MOQ_CHECK(gaps >= 1);
    MOQ_CHECK(done == 1);

    // An open absolute start: an End before the start contradicts the
    // request and is refused with the fetch still pending.
    moq::fetch_config abs = fc;
    moq::fetch_options abs_options = fo;
    abs_options.location = moq::location_filter_config{};
    abs_options.location->kind         = moq::location_filter_kind::absolute_start;
    abs_options.location->start_group  = 4;
    abs_options.location->start_object = 0;
    abs_options.has_fill_timeout       = false;
    MOQ_CHECK(p.client.fetch(abs, abs_options, 3).ok());
    settle(p.client, p.server, 3);
    req = next_fetch_request(p);
    MOQ_CHECK(req && req->options && req->options->range_unresolved);
    if (req && req->options) {
        MOQ_CHECK(req->options->location &&
                  req->options->location->kind ==
                      moq::location_filter_kind::absolute_start &&
                  req->options->location->start_group == 4);
        MOQ_CHECK(req->start_group == 4);
        moq::accept_fetch_config bad{};
        bad.end_group  = 2;
        bad.end_object = 1;
        MOQ_CHECK(p.server.accept_fetch(req->fetch, bad, 3).error().code() ==
                  moq::errc::invalid);
        moq::reject_fetch_config rj{};
        rj.error_code = moq::request_error::invalid_range;
        MOQ_CHECK(p.server.reject_fetch(req->fetch, rj, 3).ok());
    }
    settle(p.client, p.server, 3);
    while (p.client.poll_event()) {}

    // No filter at all is the whole track; the options say so with no
    // location.
    moq::fetch_config whole{};
    moq::fetch_options whole_options{};
    whole.ns    = {"d22"};
    whole.track = "f";
    whole_options.location = moq::location_filter_config{moq::location_filter_kind::none};
    MOQ_CHECK(p.client.fetch(whole, whole_options, 4).ok());
    settle(p.client, p.server, 4);
    req = next_fetch_request(p);
    MOQ_CHECK(req && req->options && req->options->range_unresolved);
    if (req && req->options) {
        MOQ_CHECK(!req->options->location.has_value());
        MOQ_CHECK(!req->options->has_fill_timeout);
        moq::reject_fetch_config rj{};
        rj.error_code = moq::request_error::invalid_range;
        MOQ_CHECK(p.server.reject_fetch(req->fetch, rj, 4).ok());
    }
    settle(p.client, p.server, 4);
    while (p.client.poll_event()) {}

    // Switched back, the session resolves on its own: it published nothing
    // on the track, so the relative fetch is refused with INVALID_RANGE and
    // never surfaced.
    MOQ_CHECK(p.server.set_defer_fetch_ranges(false).ok());
    MOQ_CHECK(p.client.fetch(fc, fo, 5).ok());
    settle(p.client, p.server, 5);
    MOQ_CHECK(!next_fetch_request(p).has_value());
    int errors = 0;
    while (auto ev = p.client.poll_event())
        ev->visit_extended([&](const moq::event::fetch_error &e) {
                      errors++;
                      MOQ_CHECK(e.error_code == moq::request_error::invalid_range);
                  },
                  [](const auto &) {});
    MOQ_CHECK(errors == 1);
    MOQ_PASS("fetch_options_and_timed_out");
}

static void test_fetch_defer_from_config(int &failures)
{
    // The config field, not only the setter, defers resolution.
    moq::session_config c{}, s{};
    c.version = s.version = MOQ_VERSION_DRAFT_22;
    auto p = establish_pair(failures, c, s, {}, {.defer_fetch_ranges = true});
    moq::fetch_config fc{};
    moq::fetch_options fo{};
    fc.ns    = {"d22"};
    fc.track = "f";
    fo.location = moq::location_filter_config{moq::location_filter_kind::next_object};
    MOQ_CHECK(p.client.fetch(fc, fo, 1).ok());
    settle(p.client, p.server, 1);
    auto req = next_fetch_request(p);
    MOQ_CHECK(req && req->options && req->options->range_unresolved);
    if (req && req->options)
        MOQ_CHECK(req->options->location &&
                  req->options->location->kind == moq::location_filter_kind::next_object);
    MOQ_PASS("fetch_defer_from_config");
}

static void test_fetch_draft_18(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_18);
    moq::fetch_config fc{};
    moq::fetch_options fo{};
    fc.ns    = {"d18"};
    fc.track = "f";
    fo.location = moq::location_filter_config{moq::location_filter_kind::none};
    MOQ_CHECK(p.client.fetch(fc, fo, 1).error().code() == moq::errc::unsupported);
    fo.location.reset();
    fo.has_fill_timeout = true;
    MOQ_CHECK(p.client.fetch(fc, fo, 1).error().code() == moq::errc::unsupported);

    // An explicit range: no options on this draft, and no Timed-Out writer.
    fo.has_fill_timeout = false;
    fc.start_group = 1;
    fc.end_group   = 2;
    MOQ_CHECK(p.client.fetch(fc, fo, 1).ok());
    settle(p.client, p.server, 1);
    auto req = next_fetch_request(p);
    MOQ_CHECK(req.has_value());
    if (!req) return;
    MOQ_CHECK(!req->options.has_value());
    moq::accept_fetch_config ac{};
    ac.end_group = 2;
    MOQ_CHECK(p.server.accept_fetch(req->fetch, ac, 2).ok());
    MOQ_CHECK(p.server.write_fetch_range(req->fetch, moq::fetch_ranges::timed_out,
                                         1, 0, 2).error().code() == moq::errc::invalid);
    MOQ_CHECK(p.server.write_fetch_range(req->fetch, moq::fetch_range_kind::unknown,
                                         1, 0, 2).ok());
    MOQ_PASS("fetch_draft_18");
}

// -- Request terminal causes -------------------------------------------------

static void test_request_terminal_causes(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    opened_streams opened;

    // A real request-local terminal reaches the typed extended view only.
    moq::track_status_config tc{};
    tc.ns    = {"d22"};
    tc.track = "ts";
    auto th = p.client.track_status(tc, 1);
    MOQ_CHECK(th.ok());
    if (!th.ok()) return;
    settle(p.client, p.server, 1, &opened);
    while (p.server.poll_event()) {}
    MOQ_CHECK(!opened.by_dir[0].empty());
    if (opened.by_dir[0].empty()) return;
    MOQ_CHECK(p.client.on_bidi_stream_reset(opened.by_dir[0].back(), 7, 2).ok());
    int terminals = 0;
    while (auto ev = p.client.poll_event()) {
        if (ev->kind() != MOQ_EVENT_TRACK_STATUS_TERMINATED) continue;
        ++terminals;
        const auto value = ev->variant_extended();
        const auto *typed = std::get_if<moq::extended_event::track_status_terminated>(&value);
        MOQ_CHECK(typed != nullptr);
        if (typed) {
            MOQ_CHECK(typed->handle == *th);
            MOQ_CHECK(typed->cause == moq::track_status_terminal_cause::peer_reset_stream);
            MOQ_CHECK(typed->local_role == moq::request_role::requester);
            MOQ_CHECK(typed->stream_error_code == 7);
        }
        MOQ_CHECK(std::holds_alternative<moq::event::unknown>(ev->variant()));
        const auto &term = ev->raw().u.track_status_terminated;
        MOQ_CHECK(term.cause == MOQ_TRACK_STATUS_PEER_RESET_STREAM);
        MOQ_CHECK(term.local_role == MOQ_TRACK_STATUS_REQUESTER);
        MOQ_CHECK(term.stream_error_code == 7);
        MOQ_CHECK(moq::track_status_handle(term.handle) == *th);
    }
    MOQ_CHECK(terminals == 1);

    // A subscriber that resets its request stream unsubscribes: the
    // publisher reads the cause and the code.
    opened.by_dir[0].clear();
    auto [csub, ssub] = subscribe_accepted(failures, p, {}, &opened);
    MOQ_CHECK(!opened.by_dir[0].empty());
    if (opened.by_dir[0].empty()) return;
    MOQ_CHECK(p.server.on_bidi_stream_reset(to_peer(opened.by_dir[0].front(), 0), 9, 3).ok());
    std::optional<moq::extended_event::unsubscribed> uns;
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::extended_event::unsubscribed &u) { uns = u; },
                  [](const auto &) {});
    MOQ_CHECK(uns.has_value());
    if (uns) {
        MOQ_CHECK(uns->sub == ssub);
        MOQ_CHECK(uns->cause == moq::request_terminal_cause::peer_reset_stream);
        MOQ_CHECK(uns->stream_error_code == 9);
    }

    // A fetcher that resets its request stream cancels the fetch.
    opened.by_dir[0].clear();
    MOQ_CHECK(p.server.set_defer_fetch_ranges(true).ok());
    moq::fetch_config fc{};
    moq::fetch_options fo{};
    fc.ns    = {"d22"};
    fc.track = "f";
    fo.location = moq::location_filter_config{moq::location_filter_kind::none};
    MOQ_CHECK(p.client.fetch(fc, fo, 4).ok());
    settle(p.client, p.server, 4, &opened);
    auto req = next_fetch_request(p);
    MOQ_CHECK(req.has_value() && !opened.by_dir[0].empty());
    if (!req || opened.by_dir[0].empty()) return;
    MOQ_CHECK(p.server.on_bidi_stream_reset(to_peer(opened.by_dir[0].front(), 0), 11, 5).ok());
    std::optional<moq::extended_event::fetch_cancelled> fcan;
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::extended_event::fetch_cancelled &f) { fcan = f; },
                  [](const auto &) {});
    MOQ_CHECK(fcan.has_value());
    if (fcan) {
        MOQ_CHECK(fcan->fetch == req->fetch);
        MOQ_CHECK(fcan->cause == moq::request_terminal_cause::peer_reset_stream);
        MOQ_CHECK(fcan->stream_error_code == 11);
    }
    MOQ_PASS("request_terminal_causes");
}

// -- Fills through the subscriber facade ---------------------------------------

static void test_subscriber_facade_fills(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    auto pub = moq::publisher::create(
        p.server, {.accept_mode = moq::pub_accept_mode::accept_all}).value();
    auto ptrack = pub.add_track({.ns = {"fill"}, .track = "v"}, 0).value();
    auto b0 = moq::buffer::create(reinterpret_cast<const uint8_t *>("g3o0"), 4).value();
    auto b1 = moq::buffer::create(reinterpret_cast<const uint8_t *>("g3o1"), 4).value();
    moq::pub_retained_object objs[2] = {{.object_id = 0, .payload = &b0},
                                        {.object_id = 1, .payload = &b1}};
    MOQ_CHECK(pub.set_retained_group(ptrack, {.group_id = 3, .objects = objs,
                                              .object_count = 2}).ok());

    auto sub = moq::subscriber::create(p.client, {}, {.max_fills = 1}).value();

    // Join at the current group: Next Object plus a fill of group 3.
    moq::sub_track_config tc{};
    moq::sub_track_options options{};
    tc.ns       = {"fill"};
    tc.track    = "v";
    options.location = moq::location_filter_config{moq::location_filter_kind::next_object};
    options.fill     = moq::fill_config{};
    options.fill->location = moq::location_filter_config{};
    options.fill->location->kind                  = moq::location_filter_kind::relative_group;
    options.fill->location->relative_group_offset = 1;
    auto tr = sub.subscribe(tc, options, 1);
    MOQ_CHECK(tr.ok());
    if (!tr.ok()) return;
    auto track = *tr;
    auto fill = sub.track_fill(track);
    MOQ_CHECK(fill.valid());

    // max_fills = 1: a second request while the first is held is refused.
    moq::sub_track_config tc2 = tc;
    tc2.track = "w";
    MOQ_CHECK(sub.subscribe(tc2, options, 1).error().code() == moq::errc::would_block);

    for (int i = 0; i < 6; i++) {
        settle(p.client, p.server, 2 + i);
        MOQ_CHECK(sub.tick(2 + i).ok());
        MOQ_CHECK(pub.tick(2 + i).ok());
    }

    std::vector<uint64_t> objects;
    std::optional<moq::buffer> kept;
    int created = 0;
    int terminals = 0;
    while (auto item = sub.poll_fill()) {
        MOQ_CHECK(item->request() == fill);
        MOQ_CHECK(item->track().raw() == track.raw());
        MOQ_CHECK(item->initiating_request_id() == 0);
        switch (item->kind()) {
        case moq::sub_fill_item_kind::created:
            MOQ_CHECK(objects.empty() && terminals == 0);
            created++;
            break;
        case moq::sub_fill_item_kind::object:
            objects.push_back(item->obj_object_id());
            MOQ_CHECK(item->obj_group_id() == 3);
            if (item->obj_object_id() == 1) kept = item->obj_payload_owned();
            break;
        case moq::sub_fill_item_kind::gap:
            break;
        case moq::sub_fill_item_kind::terminal:
            terminals++;
            MOQ_CHECK(item->terminal_reason() == moq::fill_reason::complete);
            MOQ_CHECK(item->terminal_error_domain() == moq::fill_error_domain::none);
            break;
        }
    }
    MOQ_CHECK((objects == std::vector<uint64_t>{0, 1}));
    MOQ_CHECK(created == 1);
    MOQ_CHECK(terminals == 1);
    // The owned payload outlives its item.
    MOQ_CHECK(kept.has_value());
    if (kept) MOQ_CHECK(std::memcmp(moq_rcbuf_data(kept->raw()), "g3o1", 4) == 0);
    // The polled terminal released the slot: the handle is stale, the track
    // has no fill, and the slot takes a new request with a new handle.
    MOQ_CHECK(!sub.track_fill(track).valid());
    MOQ_CHECK(sub.cancel_fill(fill, 9).error().code() == moq::errc::stale_handle);
    moq::subscription_update_config uc{};
    moq::update_options uoptions{};
    uoptions.fill = moq::fill_config{};
    MOQ_CHECK(sub.update_subscription(track, uc, uoptions, 9).ok());
    auto fill2 = sub.track_fill(track);
    MOQ_CHECK(fill2.valid());
    MOQ_CHECK(fill2 != fill);
    MOQ_PASS("subscriber_facade_fills");
}

static void test_fill_item_moves_and_fixed_width_values(int &failures)
{
    static_assert(!std::is_copy_constructible_v<moq::polled_fill_item>);
    static_assert(!std::is_copy_assignable_v<moq::polled_fill_item>);
    static_assert(std::is_nothrow_move_constructible_v<moq::polled_fill_item>);
    static_assert(std::is_nothrow_move_assignable_v<moq::polled_fill_item>);
    moq_sub_fill_item_t raw{};
    raw.struct_size = sizeof(raw);
    raw.kind = UINT32_MAX;
    raw.initiating_request_id = UINT64_MAX;
    {
        moq::polled_fill_item item(raw);
        MOQ_CHECK(static_cast<uint32_t>(item.kind()) == UINT32_MAX);
        MOQ_CHECK(item.initiating_request_id() == UINT64_MAX);
    }
    raw.kind = MOQ_SUB_FILL_GAP;
    raw.u.gap.range_kind = UINT32_MAX;
    raw.u.gap.group_id = UINT64_MAX;
    raw.u.gap.object_id = UINT64_MAX - 1;
    {
        moq::polled_fill_item item(raw);
        MOQ_CHECK(static_cast<uint32_t>(item.gap_range_kind()) == UINT32_MAX);
        MOQ_CHECK(item.gap_group_id() == UINT64_MAX);
        MOQ_CHECK(item.gap_object_id() == UINT64_MAX - 1);
    }
    raw.kind = MOQ_SUB_FILL_TERMINAL;
    raw.u.terminal.reason = UINT32_MAX;
    raw.u.terminal.error_domain = UINT32_MAX;
    raw.u.terminal.error_code = UINT64_MAX;
    {
        moq::polled_fill_item item(raw);
        MOQ_CHECK(static_cast<uint32_t>(item.terminal_reason()) == UINT32_MAX);
        MOQ_CHECK(static_cast<uint32_t>(item.terminal_error_domain()) == UINT32_MAX);
        MOQ_CHECK(item.terminal_error_code() == UINT64_MAX);
    }

    int releases[3]{};
    const uint8_t bytes[] = {0, 0x42, 0xff};
    const auto release = [](void *ctx, const uint8_t *, size_t) {
        ++*static_cast<int *>(ctx);
    };
    std::optional<moq::buffer> kept_payload, kept_properties;
    {
        raw = {};
        raw.struct_size = sizeof(raw);
        raw.kind = MOQ_SUB_FILL_OBJECT;
        auto payload = moq::buffer::wrap(bytes, sizeof(bytes), release, &releases[0]).value();
        auto props = moq::buffer::wrap(bytes, sizeof(bytes), release, &releases[1]).value();
        raw.u.object.payload = payload.release();
        raw.u.object.properties = props.release();
        moq::polled_fill_item source(raw);
        kept_payload = source.obj_payload_owned();
        kept_properties = source.obj_properties_owned();
        MOQ_CHECK(moq_rcbuf_refcount(kept_payload->raw()) == 2);
        moq::polled_fill_item moved(std::move(source));
        MOQ_CHECK(source.raw().struct_size == 0);
        MOQ_CHECK(moq_rcbuf_refcount(kept_payload->raw()) == 2);
        raw.u.object.payload = moq::buffer::wrap(bytes, sizeof(bytes), release, &releases[2]).value().release();
        raw.u.object.properties = nullptr;
        moq::polled_fill_item destination(raw);
        destination = std::move(moved);
        MOQ_CHECK(moved.raw().struct_size == 0);
        MOQ_CHECK(releases[0] == 0 && releases[1] == 0 && releases[2] == 1);
        MOQ_CHECK(moq_rcbuf_refcount(kept_payload->raw()) == 2);
    }
    MOQ_CHECK(moq_rcbuf_refcount(kept_payload->raw()) == 1);
    MOQ_CHECK(moq_rcbuf_refcount(kept_properties->raw()) == 1);
    {
        auto copy = *kept_payload;
        kept_payload.reset();
        MOQ_CHECK(releases[0] == 0 && copy.size() == sizeof(bytes));
        MOQ_CHECK(std::memcmp(copy.data(), bytes, sizeof(bytes)) == 0);
    }
    kept_properties.reset();
    MOQ_CHECK(releases[0] == 1 && releases[1] == 1 && releases[2] == 1);
    MOQ_PASS("fill_item_moves_and_fixed_width_values");
}

static void test_overlapping_facade_fill_observations(int &failures)
{
    constexpr uint64_t group = (UINT64_C(1) << 40) + 7;
    constexpr uint64_t object = (UINT64_C(1) << 36) + 2;
    constexpr uint64_t subgroup = (UINT64_C(1) << 42) + 9;
    constexpr uint64_t wide_code = (UINT64_C(1) << 54) + 13;
    const uint8_t body[] = {0, 0x42, 0xff};
    const uint8_t props[] = {2, 0x41};
    std::optional<moq::polled_fill_item> held;
    std::optional<moq::buffer> kept_payload, kept_properties;
    {
        auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
        auto sub = moq::subscriber::create(p.client, {}, {.max_fills = 3}).value();
        moq::fill_config fill{};
        fill.location = moq::location_filter_config{};
        fill.location->kind = moq::location_filter_kind::absolute_start;
        fill.location->start_group = group;
        fill.location->start_object = object;
        moq::sub_track_options opts{};
        opts.fill = fill;
        auto track = sub.subscribe({.ns = {"sized-fill"}, .track = "v"}, opts, 0).value();
        std::vector<moq::event::fill_request> requests;
        const auto collect = [&] {
            for (int step = 0; step < 2; ++step) {
                settle(p.client, p.server, 0);
                while (auto ev = p.server.poll_event()) {
                    ev->visit_extended([&](const moq::extended_event::subscribe_request &r) {
                        moq::accept_subscribe_config ac{};
                        ac.has_largest = true;
                        ac.largest_group = group + 1;
                        MOQ_CHECK(p.server.accept_subscribe(r.sub, ac, 0).ok());
                    }, [&](const moq::event::fill_request &r) { requests.push_back(r); },
                    [](const auto &) {});
                }
            }
            MOQ_CHECK(sub.tick(0).ok());
        };
        collect();
        for (int n = 1; n < 3; ++n) {
            moq::update_options update{};
            update.fill = fill;
            MOQ_CHECK(sub.update_subscription(track, {}, update, 0).ok());
            collect();
        }
        MOQ_CHECK(requests.size() == 3);
        if (requests.size() != 3) return;
        // All three children coexist before any CREATED is polled. The latest
        // query cannot recover their identities or initiating request IDs.
        std::vector<moq::sub_fill_handle> children;
        for (size_t n = 0; n < 3; ++n) {
            auto it = sub.poll_fill();
            MOQ_CHECK(it.has_value());
            if (!it) return;
            MOQ_CHECK(it->kind() == moq::sub_fill_item_kind::created);
            MOQ_CHECK(it->raw().struct_size == sizeof(moq_sub_fill_item_t));
            MOQ_CHECK(it->track().raw() == track.raw());
            MOQ_CHECK(it->initiating_request_id() == 2 * n);
            MOQ_CHECK(requests[n].initiating_request_id == 2 * n);
            children.push_back(it->request());
        }
        MOQ_CHECK(children[0] != children[1] && children[1] != children[2] && children[0] != children[2]);
        MOQ_CHECK(sub.track_fill(track) == children[2]);
        MOQ_CHECK(!sub.poll_fill());
        auto payload = moq::buffer::create(body, sizeof(body)).value();
        auto properties = moq::buffer::create(props, sizeof(props)).value();
        moq::fetch_object_config oc{};
        oc.group_id = group;
        oc.subgroup_id = subgroup;
        oc.object_id = object;
        oc.publisher_priority = 213;
        oc.properties = &properties;
        MOQ_CHECK(p.server.open_fill(requests[0].fill, 0).ok());
        MOQ_CHECK(p.server.write_fill_object(requests[0].fill, oc, payload, 0).ok());
        oc.datagram = true;
        oc.object_id++;
        MOQ_CHECK(p.server.write_fill_object(requests[0].fill, oc, payload, 0).ok());
        MOQ_CHECK(p.server.write_fill_range(requests[0].fill, moq::fetch_range_kind::non_existent,
                                            group, object + 2, 0).ok());
        MOQ_CHECK(p.server.end_fill(requests[0].fill, 0).ok());
        oc.datagram = false;
        oc.object_id = object;
        MOQ_CHECK(p.server.open_fill(requests[1].fill, 0).ok());
        MOQ_CHECK(p.server.write_fill_object(requests[1].fill, oc, payload, 0).ok());
        MOQ_CHECK(p.server.write_fill_range(requests[1].fill, moq::fetch_range_kind::non_existent,
                                            group, object + 1, 0).ok());
        // Deliver the UPDATE child's data before its reset, without relying on
        // transport buffering of data subsequently discarded by RESET.
        settle(p.client, p.server, 0);
        MOQ_CHECK(sub.tick(0).ok());
        MOQ_CHECK(p.server.abort_fill(requests[1].fill, 0, 0).ok());
        MOQ_CHECK(p.server.abort_fill(requests[2].fill, wide_code, 0).ok());
        settle(p.client, p.server, 0);
        MOQ_CHECK(sub.tick(0).ok());
        int objects[3]{}, gaps[3]{}, terminals[3]{};
        while (auto it = sub.poll_fill()) {
            const auto found = std::find(children.begin(), children.end(), it->request());
            MOQ_CHECK(found != children.end());
            if (found == children.end()) return;
            const auto n = static_cast<size_t>(found - children.begin());
            MOQ_CHECK(it->initiating_request_id() == 2 * n);
            MOQ_CHECK(it->track().raw() == track.raw());
            switch (it->kind()) {
            case moq::sub_fill_item_kind::created:
                MOQ_CHECK(false);
                break;
            case moq::sub_fill_item_kind::object:
                MOQ_CHECK(n < 2 && objects[n] < (n == 0 ? 2 : 1));
                MOQ_CHECK(it->obj_group_id() == group && it->obj_object_id() == object + objects[n]);
                MOQ_CHECK(it->obj_subgroup_id() == (objects[n] == 0 ? subgroup : 0));
                MOQ_CHECK(it->obj_datagram() == (objects[n] == 1) && it->obj_publisher_priority() == 213);
                MOQ_CHECK(it->obj_payload().size() == sizeof(body));
                MOQ_CHECK(it->obj_properties().size() == sizeof(props));
                MOQ_CHECK(std::memcmp(it->obj_payload().data(), body, sizeof(body)) == 0);
                MOQ_CHECK(std::memcmp(it->obj_properties().data(), props, sizeof(props)) == 0);
                if (n == 0 && objects[n] == 0) held.emplace(std::move(*it));
                else { kept_payload = it->obj_payload_owned(); kept_properties = it->obj_properties_owned(); }
                objects[n]++;
                break;
            case moq::sub_fill_item_kind::gap:
                MOQ_CHECK(n < 2 && objects[n] == (n == 0 ? 2 : 1));
                MOQ_CHECK(it->gap_range_kind() == moq::fetch_range_kind::non_existent);
                MOQ_CHECK(it->gap_group_id() == group && it->gap_object_id() == object + (n == 0 ? 2 : 1));
                gaps[n]++;
                break;
            case moq::sub_fill_item_kind::terminal:
                terminals[n]++;
                MOQ_CHECK(it->terminal_reason() == (n == 0 ? moq::fill_reason::complete : moq::fill_reason::peer_cancelled_or_reset));
                MOQ_CHECK(it->terminal_error_domain() == (n == 0 ? moq::fill_error_domain::none : moq::fill_error_domain::stream));
                MOQ_CHECK(it->terminal_error_code() == (n == 2 ? wide_code : 0));
                MOQ_CHECK(sub.cancel_fill(it->request(), 0).error().code() == moq::errc::stale_handle);
                break;
            }
        }
        MOQ_CHECK(objects[0] == 2 && objects[1] == 1 && objects[2] == 0);
        MOQ_CHECK(gaps[0] == 1 && gaps[1] == 1 && gaps[2] == 0);
        MOQ_CHECK(terminals[0] == 1 && terminals[1] == 1 && terminals[2] == 1);
        MOQ_CHECK(!sub.track_fill(track).valid());
    }
    // Neither an item nor retained buffers depend on the now-destroyed facade
    // or sessions. The borrowed track must no longer be dereferenced.
    MOQ_CHECK(held.has_value() && kept_payload.has_value() && kept_properties.has_value());
    if (held) {
        MOQ_CHECK(held->initiating_request_id() == 0);
        MOQ_CHECK(std::memcmp(held->obj_payload().data(), body, sizeof(body)) == 0);
        MOQ_CHECK(std::memcmp(held->obj_properties().data(), props, sizeof(props)) == 0);
    }
    if (kept_payload) MOQ_CHECK(std::memcmp(kept_payload->data(), body, sizeof(body)) == 0);
    if (kept_properties) MOQ_CHECK(std::memcmp(kept_properties->data(), props, sizeof(props)) == 0);
    MOQ_PASS("overlapping_facade_fill_observations");
}

static void test_fill_refused_by_facade_on_draft_18(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_18);
    auto sub = moq::subscriber::create(p.client).value();
    moq::sub_track_config tc{};
    moq::sub_track_options options{};
    tc.ns   = {"fill"};
    tc.track = "v";
    options.fill = moq::fill_config{};
    MOQ_CHECK(sub.subscribe(tc, options, 1).error().code() == moq::errc::unsupported);
    // A relative start more than one group back has no draft-18 form.
    options.fill.reset();
    options.location = moq::location_filter_config{};
    options.location->kind                  = moq::location_filter_kind::relative_group;
    options.location->relative_group_offset = 3;
    MOQ_CHECK(sub.subscribe(tc, options, 1).error().code() == moq::errc::unsupported);
    MOQ_CHECK(!p.client.poll_action().has_value());
    MOQ_PASS("fill_refused_by_facade_on_draft_18");
}

// -- Publisher facade fill slots ------------------------------------------------

struct counting_alloc {
    size_t bytes = 0;
    static void *alloc(size_t n, void *ctx)
    {
        static_cast<counting_alloc *>(ctx)->bytes += n;
        return std::malloc(n);
    }
    static void *realloc(void *ptr, size_t old_n, size_t n, void *ctx)
    {
        auto *c = static_cast<counting_alloc *>(ctx);
        c->bytes += n - (n > old_n ? old_n : n);
        return std::realloc(ptr, n);
    }
    static void free(void *ptr, size_t, void *) { std::free(ptr); }
};

static size_t publisher_bytes(int &failures, moq::session &s, uint32_t max_fills)
{
    counting_alloc counter;
    moq_alloc_t a{&counter, counting_alloc::alloc, counting_alloc::realloc,
                  counting_alloc::free};
    moq::publisher_config cfg{};
    cfg.alloc     = &a;
    auto pub = moq::publisher::create(s, cfg, {.max_fills = max_fills});
    MOQ_CHECK(pub.ok());
    return counter.bytes;
}

static void test_publisher_facade_max_fills(int &failures)
{
    // The publisher reserves one fill slot per request it can hold: a larger
    // max_fills reserves more than the default, so the field is read.
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    const size_t dflt = publisher_bytes(failures, p.server, 0);
    const size_t big  = publisher_bytes(failures, p.server, 1000);
    MOQ_CHECK(big > dflt);
    // No fills on draft 18: nothing is reserved whatever the value.
    auto q = establish_pair(failures, MOQ_VERSION_DRAFT_18);
    MOQ_CHECK(publisher_bytes(failures, q.server, 1000) ==
              publisher_bytes(failures, q.server, 0));
    MOQ_PASS("publisher_facade_max_fills");
}

static size_t subscriber_bytes(int &failures, moq::session &s, uint32_t max_fills,
                               uint32_t max_fill_items)
{
    counting_alloc counter;
    moq_alloc_t a{&counter, counting_alloc::alloc, counting_alloc::realloc,
                  counting_alloc::free};
    moq::subscriber_config cfg{};
    cfg.alloc          = &a;
    auto sub = moq::subscriber::create(s, cfg, {.max_fills = max_fills,
                                               .max_fill_items = max_fill_items});
    MOQ_CHECK(sub.ok());
    return counter.bytes;
}

static void test_subscriber_facade_fill_capacity(int &failures)
{
    // Both capacities size the facade's reservations on a draft with fills.
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    const size_t dflt = subscriber_bytes(failures, p.client, 0, 0);
    MOQ_CHECK(subscriber_bytes(failures, p.client, 1000, 0) > dflt);
    MOQ_CHECK(subscriber_bytes(failures, p.client, 0, 5000) > dflt);
    MOQ_PASS("subscriber_facade_fill_capacity");
}

// -- Appended event fields, read only when the detail covers them -------------

// A C event built by hand, as a producer of either era would fill it. The
// kinds used here own no references, so the polled_event's cleanup is a no-op.
template <class D>
static moq::extended_event_variant convert(moq_event_kind_t kind, const D &d, size_t detail)
{
    moq_event_t e{};
    e.kind        = kind;
    e.detail_size = static_cast<uint32_t>(detail);
    std::memcpy(&e.u, &d, sizeof(d));
    return moq::polled_event(e).variant_extended();
}

#define END_OF(type, field) (offsetof(type, field) + sizeof(((type *)nullptr)->field))

static void test_event_field_gates(int &failures)
{
    const auto reset = moq::request_terminal_cause::peer_reset_stream;
    const auto none  = moq::request_terminal_cause::none;

    {   // namespace_done
        moq_namespace_done_event_t d{};
        d.ann = moq_announcement_t{0x51};
        MOQ_CHECK(std::holds_alternative<moq::event::namespace_done>(
            convert(MOQ_EVENT_NAMESPACE_DONE, d, sizeof(d))));
        MOQ_CHECK(std::holds_alternative<moq::event::unknown>(
            convert(MOQ_EVENT_NAMESPACE_DONE, d, sizeof(d) - 1)));
    }
    {   // namespace_cancelled
        moq_namespace_cancelled_event_t d{};
        d.ann = moq_announcement_t{0x52};
        d.error_code = 6;
        auto value = convert(MOQ_EVENT_NAMESPACE_CANCELLED, d, sizeof(d));
        const auto *cancelled = std::get_if<moq::event::namespace_cancelled>(&value);
        MOQ_CHECK(cancelled && cancelled->ann == moq::announcement(d.ann));
        MOQ_CHECK(cancelled && cancelled->error_code == moq::request_error_from_c(6));
        MOQ_CHECK(std::holds_alternative<moq::event::unknown>(
            convert(MOQ_EVENT_NAMESPACE_CANCELLED, d,
                    END_OF(moq_namespace_cancelled_event_t, reason) - 1)));
    }
    {   // publish_finished
        moq_publish_finished_event_t d{};
        d.cause = MOQ_REQUEST_TERMINAL_MISSING_RESPONSE;
        auto v = std::get<moq::extended_event::publish_finished>(
            convert(MOQ_EVENT_PUBLISH_FINISHED, d, sizeof(d)));
        MOQ_CHECK(v.cause == moq::request_terminal_cause::missing_response);
        v = std::get<moq::extended_event::publish_finished>(convert(
            MOQ_EVENT_PUBLISH_FINISHED, d, offsetof(moq_publish_finished_event_t, cause)));
        MOQ_CHECK(v.cause == none);
    }
    {   // publish_unsubscribed
        moq_publish_unsubscribed_event_t d{};
        d.cause = MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM;
        d.stream_error_code = 8;
        auto v = std::get<moq::extended_event::publish_unsubscribed>(
            convert(MOQ_EVENT_PUBLISH_UNSUBSCRIBED, d, sizeof(d)));
        MOQ_CHECK(v.cause == reset && v.stream_error_code == 8);
        v = std::get<moq::extended_event::publish_unsubscribed>(convert(
            MOQ_EVENT_PUBLISH_UNSUBSCRIBED, d, offsetof(moq_publish_unsubscribed_event_t, cause)));
        MOQ_CHECK(v.cause == none && v.stream_error_code == 0);
    }
    {   // subscribe_tracks_cancelled
        moq_subscribe_tracks_cancelled_event_t d{};
        d.cause = MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE;
        auto v = std::get<moq::extended_event::subscribe_tracks_cancelled>(
            convert(MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED, d, sizeof(d)));
        MOQ_CHECK(v.cause == moq::request_terminal_cause::truncated_response);
        v = std::get<moq::extended_event::subscribe_tracks_cancelled>(
            convert(MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED, d,
                    offsetof(moq_subscribe_tracks_cancelled_event_t, cause)));
        MOQ_CHECK(v.cause == none);
    }
    {   // fetch_reset
        moq_fetch_reset_event_t d{};
        d.error_code = 0;
        d.cause = MOQ_REQUEST_TERMINAL_MISSING_RESPONSE;
        auto v = std::get<moq::extended_event::fetch_reset>(
            convert(MOQ_EVENT_FETCH_RESET, d, sizeof(d)));
        MOQ_CHECK(v.cause == moq::request_terminal_cause::missing_response);
        v = std::get<moq::extended_event::fetch_reset>(
            convert(MOQ_EVENT_FETCH_RESET, d, offsetof(moq_fetch_reset_event_t, cause)));
        MOQ_CHECK(v.cause == none);
        // This side's own cancel of a fetch it could not take.
        d.error_code = 0x1;
        d.data_stream = true;
        d.cause = MOQ_REQUEST_TERMINAL_LOCAL_CANCEL;
        v = std::get<moq::extended_event::fetch_reset>(
            convert(MOQ_EVENT_FETCH_RESET, d, sizeof(d)));
        MOQ_CHECK(v.cause == moq::request_terminal_cause::local_cancel);
        MOQ_CHECK(v.error_code == 0x1 && v.data_stream);
    }
    {   // subscribe_updated / publish_updated
        moq_subscribe_updated_event_t d{};
        moq_subscription_update_options_t options{};
        options.struct_size = sizeof(options);
        options.flags = MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION;
        moq_location_filter_cfg_init(&options.location);
        options.location.kind = MOQ_LOCATION_FILTER_OBJECT_END;
        options.location.end_object = 4;
        d.options = &options;
        d.answer_owed = true;
        d.fill_requested = true;
        d.update_count = 3;
        d.update_request_id = 12;
        auto v = std::get<moq::extended_event::subscribe_updated>(
            convert(MOQ_EVENT_SUBSCRIBE_UPDATED, d, sizeof(d)));
        MOQ_CHECK(v.options && v.options->location && v.options->location->end_object == 4);
        MOQ_CHECK(v.answer_owed && v.fill_requested);
        MOQ_CHECK(v.update_count == 3 && v.update_request_id == 12);
        v = std::get<moq::extended_event::subscribe_updated>(convert(
            MOQ_EVENT_SUBSCRIBE_UPDATED, d, offsetof(moq_subscribe_updated_event_t, options)));
        MOQ_CHECK(!v.options && !v.answer_owed && !v.fill_requested);
        MOQ_CHECK(v.update_count == 0 && v.update_request_id == 0);

        moq_publish_updated_event_t q{};
        q.answer_owed = true;
        q.fill_requested = true;
        q.update_count = 2;
        q.update_request_id = 14;
        auto w = std::get<moq::extended_event::publish_updated>(
            convert(MOQ_EVENT_PUBLISH_UPDATED, q, sizeof(q)));
        MOQ_CHECK(w.answer_owed && w.fill_requested);
        MOQ_CHECK(w.update_count == 2 && w.update_request_id == 14);
        w = std::get<moq::extended_event::publish_updated>(convert(
            MOQ_EVENT_PUBLISH_UPDATED, q, offsetof(moq_publish_updated_event_t, options)));
        MOQ_CHECK(!w.answer_owed && w.update_request_id == 0);
    }
    {   // Initial typed PUBLISH values are queried, never fabricated from
        // absent generic event fields. The legacy request keeps its old shape.
        moq_publish_request_event_t d{};
        d.has_expires = true;
        d.expires_ms = 7;
        auto v = std::get<moq::event::publish_request>(
            convert(MOQ_EVENT_PUBLISH_REQUEST, d, sizeof(d)));
        MOQ_CHECK(v.has_expires && v.expires_ms == 7);
    }
    {   // request_redirect: the two handle families added to its union
        moq_request_redirect_event_t d{};
        d.family = MOQ_REQUEST_FAMILY_PUBLISH;
        d.handle.publication = moq_publication_t{0x1234};
        auto v = std::get<moq::event::request_redirect>(
            convert(MOQ_EVENT_REQUEST_REDIRECT, d, sizeof(d)));
        MOQ_CHECK(v.family == moq::request_family::publish);
        MOQ_CHECK(v.as_publication().raw()._opaque == 0x1234);
        d.family = MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS;
        d.handle.track_sub = moq_track_sub_handle_t{0x5678};
        v = std::get<moq::event::request_redirect>(
            convert(MOQ_EVENT_REQUEST_REDIRECT, d, sizeof(d)));
        MOQ_CHECK(v.as_track_sub().raw()._opaque == 0x5678);
    }
    {   // fetch_request options: copied by value, and absent when the
        // detail or the options record predates a field.
        moq_location_filter_cfg_t loc;
        moq_location_filter_cfg_init(&loc);
        loc.kind = MOQ_LOCATION_FILTER_GROUP_END;
        loc.start_group = 2;
        loc.end_group = 6;
        moq_fetch_request_options_t o{};
        o.struct_size = sizeof(o);
        o.range_unresolved = true;
        o.flags = MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT | MOQ_FETCH_REQUEST_INCLUDE_PROPERTIES;
        o.fill_timeout_ms = 250;
        o.location = loc;
        moq_fetch_request_event_t d{};
        d.options = &o;
        auto v = std::get<moq::extended_event::fetch_request>(
            convert(MOQ_EVENT_FETCH_REQUEST, d, sizeof(d)));
        o.location.kind = MOQ_LOCATION_FILTER_NONE;   // the value no longer borrows it
        MOQ_CHECK(v.options.has_value());
        if (v.options) {
            MOQ_CHECK(v.options->range_unresolved);
            MOQ_CHECK(v.options->has_fill_timeout && v.options->fill_timeout_ms == 250);
            MOQ_CHECK(v.options->include_properties);
            MOQ_CHECK(v.options->location.has_value());
            if (v.options->location) {
                MOQ_CHECK(v.options->location->kind == moq::location_filter_kind::group_end);
                MOQ_CHECK(v.options->location->start_group == 2);
                MOQ_CHECK(v.options->location->end_group == 6);
            }
        }
        v = std::get<moq::extended_event::fetch_request>(convert(
            MOQ_EVENT_FETCH_REQUEST, d, offsetof(moq_fetch_request_event_t, options)));
        MOQ_CHECK(!v.options.has_value());
        o.struct_size = offsetof(moq_fetch_request_options_t, location);
        v = std::get<moq::extended_event::fetch_request>(
            convert(MOQ_EVENT_FETCH_REQUEST, d, sizeof(d)));
        MOQ_CHECK(!v.options.has_value());
        o.struct_size = sizeof(o);
        v = std::get<moq::extended_event::fetch_request>(
            convert(MOQ_EVENT_FETCH_REQUEST, d, sizeof(d)));
        MOQ_CHECK(v.options && !v.options->location);
    }
    {   // Each new scalar terminal requires its own whole 24-byte detail.
        moq_ns_subscription_terminated_event_t d{};
        d.cause = MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING;
        d.local_role = MOQ_REQUEST_ROLE_RESPONDER;
        d.stream_error_code = UINT64_MAX;
        d.handle = moq_ns_sub_handle_t{0x99};
        auto value = convert(MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED, d, sizeof(d));
        const auto *ns = std::get_if<moq::extended_event::namespace_subscription_terminated>(&value);
        MOQ_CHECK(ns && ns->handle == moq::ns_sub_handle(d.handle));
        MOQ_CHECK(ns && ns->cause == moq::request_terminal_cause::peer_stop_sending);
        MOQ_CHECK(ns && ns->local_role == moq::request_role::responder);
        MOQ_CHECK(ns && ns->stream_error_code == UINT64_MAX);

        moq_track_status_terminated_event_t ts{};
        ts.handle = moq_track_status_handle_t{0x77};
        ts.cause = MOQ_TRACK_STATUS_PEER_RESET_STREAM;
        ts.local_role = MOQ_TRACK_STATUS_REQUESTER;
        auto tv = convert(MOQ_EVENT_TRACK_STATUS_TERMINATED, ts, sizeof(ts));
        const auto *status = std::get_if<moq::extended_event::track_status_terminated>(&tv);
        MOQ_CHECK(status && status->handle == moq::track_status_handle(ts.handle));
        MOQ_CHECK(status && status->cause == moq::track_status_terminal_cause::peer_reset_stream);
        MOQ_CHECK(status && status->local_role == moq::request_role::requester);
        MOQ_CHECK(status && status->stream_error_code == 0);

        moq_namespace_terminated_event_t ann{};
        ann.ann = moq_announcement_t{0x88};
        ann.cause = MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY;
        ann.local_role = MOQ_NAMESPACE_ROLE_RESPONDER;
        auto av = convert(MOQ_EVENT_NAMESPACE_TERMINATED, ann, sizeof(ann));
        const auto *announcement = std::get_if<moq::extended_event::namespace_terminated>(&av);
        MOQ_CHECK(announcement && announcement->ann == moq::announcement(ann.ann));
        MOQ_CHECK(announcement && announcement->cause == moq::namespace_terminal_cause::local_capacity);
        MOQ_CHECK(announcement && announcement->local_role == moq::request_role::responder);
        MOQ_CHECK(announcement && announcement->stream_error_code == 0);

        for (size_t extent = 0; extent < 24; ++extent) {
            MOQ_CHECK(std::holds_alternative<moq::event::unknown>(
                convert(MOQ_EVENT_TRACK_STATUS_TERMINATED, ts, extent)));
            MOQ_CHECK(std::holds_alternative<moq::event::unknown>(
                convert(MOQ_EVENT_NAMESPACE_TERMINATED, ann, extent)));
            MOQ_CHECK(std::holds_alternative<moq::event::unknown>(
                convert(MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED, d, extent)));
        }
        ts.cause = UINT32_MAX;
        ts.local_role = UINT32_MAX;
        tv = convert(MOQ_EVENT_TRACK_STATUS_TERMINATED, ts, sizeof(ts) + 8);
        status = std::get_if<moq::extended_event::track_status_terminated>(&tv);
        MOQ_CHECK(status && static_cast<uint32_t>(status->cause) == UINT32_MAX);
        MOQ_CHECK(status && static_cast<uint32_t>(status->local_role) == UINT32_MAX);
    }
    MOQ_PASS("event_field_gates");
}

#undef END_OF

static void test_typed_terminal_domains(int &failures)
{
    using ts_cause = moq::track_status_terminal_cause;
    using ann_cause = moq::namespace_terminal_cause;
    using ns_cause = moq::request_terminal_cause;
    const std::pair<uint32_t, ts_cause> track_causes[] = {
        {MOQ_TRACK_STATUS_PEER_RESET_STREAM, ts_cause::peer_reset_stream},
        {MOQ_TRACK_STATUS_PEER_STOP_SENDING, ts_cause::peer_stop_sending},
        {MOQ_TRACK_STATUS_MISSING_RESPONSE, ts_cause::missing_response},
        {MOQ_TRACK_STATUS_TRUNCATED_RESPONSE, ts_cause::truncated_response},
        {UINT32_MAX, static_cast<ts_cause>(UINT32_MAX)}};
    const std::pair<uint32_t, ann_cause> announcement_causes[] = {
        {MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE, ann_cause::peer_fin_complete},
        {MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM, ann_cause::peer_reset_stream},
        {MOQ_NAMESPACE_TERMINAL_PEER_STOP_SENDING, ann_cause::peer_stop_sending},
        {MOQ_NAMESPACE_TERMINAL_MISSING_RESPONSE, ann_cause::missing_response},
        {MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE, ann_cause::truncated_message},
        {MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY, ann_cause::local_capacity},
        {UINT32_MAX, static_cast<ann_cause>(UINT32_MAX)}};
    const std::pair<uint32_t, ns_cause> subscription_causes[] = {
        {MOQ_REQUEST_TERMINAL_NONE, ns_cause::none},
        {MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM, ns_cause::peer_reset_stream},
        {MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING, ns_cause::peer_stop_sending},
        {MOQ_REQUEST_TERMINAL_MISSING_RESPONSE, ns_cause::missing_response},
        {MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE, ns_cause::truncated_response},
        {MOQ_REQUEST_TERMINAL_PEER_FIN_COMPLETE, ns_cause::peer_fin_complete},
        {UINT32_MAX, static_cast<ns_cause>(UINT32_MAX)}};
    const std::pair<uint32_t, moq::request_role> roles[] = {
        {MOQ_REQUEST_ROLE_REQUESTER, moq::request_role::requester},
        {MOQ_REQUEST_ROLE_RESPONDER, moq::request_role::responder},
        {UINT32_MAX, static_cast<moq::request_role>(UINT32_MAX)}};
    for (const auto &[native_role, role] : roles) {
        for (uint64_t code : {UINT64_C(0), UINT64_MAX}) {
            for (size_t extent : {24u, 32u}) {
                for (const auto &[native_cause, cause] : track_causes) {
                    moq_track_status_terminated_event_t d{};
                    d.handle = moq_track_status_handle_t{0x77};
                    d.cause = native_cause;
                    d.local_role = native_role;
                    d.stream_error_code = code;
                    const auto value = convert(MOQ_EVENT_TRACK_STATUS_TERMINATED, d, extent);
                    const auto *v = std::get_if<moq::extended_event::track_status_terminated>(&value);
                    MOQ_CHECK(v && v->handle == moq::track_status_handle(d.handle));
                    MOQ_CHECK(v && v->cause == cause && v->local_role == role);
                    MOQ_CHECK(v && v->stream_error_code == code);
                }
                for (const auto &[native_cause, cause] : announcement_causes) {
                    moq_namespace_terminated_event_t d{};
                    d.ann = moq_announcement_t{0x88};
                    d.cause = native_cause;
                    d.local_role = native_role;
                    d.stream_error_code = code;
                    const auto value = convert(MOQ_EVENT_NAMESPACE_TERMINATED, d, extent);
                    const auto *v = std::get_if<moq::extended_event::namespace_terminated>(&value);
                    MOQ_CHECK(v && v->ann == moq::announcement(d.ann));
                    MOQ_CHECK(v && v->cause == cause && v->local_role == role);
                    MOQ_CHECK(v && v->stream_error_code == code);
                }
                for (const auto &[native_cause, cause] : subscription_causes) {
                    moq_ns_subscription_terminated_event_t d{};
                    d.handle = moq_ns_sub_handle_t{0x99};
                    d.cause = native_cause;
                    d.local_role = native_role;
                    d.stream_error_code = code;
                    const auto value = convert(MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED, d, extent);
                    const auto *v = std::get_if<moq::extended_event::namespace_subscription_terminated>(&value);
                    MOQ_CHECK(v && v->handle == moq::ns_sub_handle(d.handle));
                    MOQ_CHECK(v && v->cause == cause && v->local_role == role);
                    MOQ_CHECK(v && v->stream_error_code == code);
                    for (size_t short_extent : {0u, 8u, 12u, 16u, 23u}) {
                        const auto short_value = convert(MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED,
                                                         d, short_extent);
                        MOQ_CHECK(std::holds_alternative<moq::event::unknown>(short_value));
                    }
                }
            }
        }
    }
    MOQ_PASS("typed_terminal_domains");
}

static void test_fetch_typed_locations(int &failures)
{
    const moq::location_filter_config forms[] = {
        {}, {moq::location_filter_kind::relative_group, 2},
        {moq::location_filter_kind::absolute_start, 0, 2, 3},
        {moq::location_filter_kind::group_end, 0, 2, 3, 7},
        {moq::location_filter_kind::object_end, 0, 2, 3, 7, 9},
        {moq::location_filter_kind::next_object}};
    for (const auto &form : forms) {
        auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
        MOQ_CHECK(p.server.set_defer_fetch_ranges(true).ok());
        moq::fetch_config cfg{};
        cfg.ns = {"typed"};
        cfg.track = "fetch";
        moq::fetch_options options{};
        options.location = form;
        MOQ_CHECK(p.client.fetch(cfg, options, 1).ok());
        options = {};
        settle(p.client, p.server, 1);
        auto req = next_fetch_request(p);
        MOQ_CHECK(req && req->options);
        if (!req || !req->options) continue;
        const auto &value = *req->options;
        MOQ_CHECK(value.range_unresolved && value.include_properties && !value.has_fill_timeout);
        MOQ_CHECK(value.location.has_value() == (form.kind != moq::location_filter_kind::none));
        if (value.location) {
            MOQ_CHECK(value.location->kind == form.kind);
            MOQ_CHECK(value.location->relative_group_offset == form.relative_group_offset);
            MOQ_CHECK(value.location->start_group == form.start_group && value.location->start_object == form.start_object);
            MOQ_CHECK(value.location->end_group == form.end_group && value.location->end_object == form.end_object);
        }
        moq::reject_fetch_config reject{};
        reject.error_code = moq::request_error::invalid_range;
        MOQ_CHECK(p.server.reject_fetch(req->fetch, reject, 2).ok());
    }
    MOQ_PASS("fetch_typed_locations");
}

static void test_fetch_options_prefix(int &failures)
{
    static_assert(MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE == 64);
    static_assert(offsetof(moq_fetch_request_options_t, range_unresolved) == 64);
    const moq::event::fetch_request_options legacy{true, std::nullopt, true, 99};
    MOQ_CHECK(legacy.range_unresolved && legacy.has_fill_timeout &&
              legacy.fill_timeout_ms == 99 && legacy.include_properties);
    auto extract = [](const void *storage, size_t detail = sizeof(moq_fetch_request_event_t)) {
        moq_fetch_request_event_t d{};
        d.options = static_cast<const moq_fetch_request_options_t *>(storage);
        return std::get<moq::extended_event::fetch_request>(
            convert(MOQ_EVENT_FETCH_REQUEST, d, detail)).options;
    };
    const uint32_t kinds[] = {MOQ_LOCATION_FILTER_NONE, MOQ_LOCATION_FILTER_RELATIVE_GROUP,
        MOQ_LOCATION_FILTER_ABSOLUTE_START, MOQ_LOCATION_FILTER_GROUP_END,
        MOQ_LOCATION_FILTER_OBJECT_END, MOQ_LOCATION_FILTER_NEXT_OBJECT, 99};
    for (size_t size : {64u, 65u, 71u, 72u, 88u}) {
        for (uint32_t kind : kinds) {
            for (bool properties : {false, true}) {
                for (int timeout = 0; timeout < 3; ++timeout) {
                    moq_fetch_request_options_t o{};
                    o.struct_size = static_cast<uint32_t>(size);
                    moq_location_filter_cfg_init(&o.location);
                    o.location.kind = kind;
                    o.location.relative_group_offset = UINT64_MAX;
                    o.location.start_group = 2;
                    o.location.start_object = 3;
                    o.location.end_group = 7;
                    o.location.end_object = UINT64_MAX;
                    o.flags = properties ? MOQ_FETCH_REQUEST_INCLUDE_PROPERTIES : 0;
                    if (timeout != 0) o.flags |= MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT;
                    o.fill_timeout_ms = timeout == 1 ? 0 : UINT64_MAX;
                    o.range_unresolved = true;
                    // Actual allocation ends at the advertised extent, including exact64.
                    void *storage = std::malloc(size);
                    if (!storage) std::abort();
                    std::memset(storage, 0xa5, size);
                    std::memcpy(storage, &o, std::min(size, sizeof(o)));
                    auto value = extract(storage);
                    std::memset(storage, 0xa5, size);
                    std::free(storage);
                    MOQ_CHECK(value.has_value());
                    if (!value) continue;
                    MOQ_CHECK(value->range_unresolved == (size > 64));
                    MOQ_CHECK(value->include_properties == properties);
                    MOQ_CHECK(value->has_fill_timeout == (timeout != 0));
                    MOQ_CHECK(value->fill_timeout_ms == (timeout == 2 ? UINT64_MAX : 0));
                    MOQ_CHECK(value->location.has_value() == (kind != MOQ_LOCATION_FILTER_NONE));
                    if (value->location) {
                        MOQ_CHECK(value->location->kind == moq::location_filter_kind_from_c(kind));
                        MOQ_CHECK(value->location->relative_group_offset == UINT64_MAX);
                        MOQ_CHECK(value->location->start_group == 2 && value->location->start_object == 3);
                        MOQ_CHECK(value->location->end_group == 7 && value->location->end_object == UINT64_MAX);
                    }
                }
            }
        }
    }
    for (uint32_t size : {0u, 4u, 8u, 56u, 63u}) {
        void *storage = std::malloc(std::max(size, uint32_t{4}));
        if (!storage) std::abort();
        std::memcpy(storage, &size, sizeof(size));
        MOQ_CHECK(!extract(storage));
        std::free(storage);
    }
    for (size_t size = offsetof(moq_fetch_request_event_t, options);
         size < sizeof(moq_fetch_request_event_t); ++size)
        MOQ_CHECK(!extract(reinterpret_cast<const void *>(uintptr_t{1}), size));
    MOQ_CHECK(!extract(nullptr));
    moq_fetch_request_options_t o{};
    o.struct_size = sizeof(o);
    o.flags = MOQ_FETCH_REQUEST_INCLUDE_PROPERTIES;
    for (uint32_t size : {0u, 4u, 47u, 48u, 64u}) {
        o.location.struct_size = size;
        auto value = extract(&o);
        MOQ_CHECK(value.has_value() == (size >= MOQ_LOCATION_FILTER_CFG_V0_SIZE));
        if (value) MOQ_CHECK(!value->range_unresolved && !value->location && value->include_properties);
    }
    MOQ_PASS("fetch_options_prefix");
}

static void test_typed_publish_parameters(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    moq::publish_config cfg{};
    cfg.ns = {"typed"};
    cfg.track = "publish";
    cfg.has_forward = true;
    cfg.forward = false;
    moq::publish_options options{};
    options.subscriber_priority = 7;
    options.group_order = moq::group_order::descending;
    options.location = moq::location_filter_config{};
    options.object_delivery_timeout_ms = 0;
    options.subgroup_delivery_timeout_ms = UINT64_MAX;
    options.expires_ms = 0;
    auto sent = p.client.publish(cfg, options, 1);
    MOQ_CHECK(sent.ok());
    if (!sent.ok()) return;
    options = {}; // native storage cannot borrow the options passed above
    settle(p.client, p.server, 1);
    moq::publication peer{};
    std::optional<moq::publish_initial_parameters> saved;
    while (auto ev = p.server.poll_event()) {
        auto view = ev->variant();
        const auto *request = std::get_if<moq::event::publish_request>(&view);
        if (!request) continue;
        peer = request->pub;
        MOQ_CHECK(!request->forward && request->has_expires && request->expires_ms == 0);
        auto initial = p.server.get_publish_initial_parameters(peer);
        MOQ_CHECK(initial.ok());
        MOQ_CHECK(moq_session_borrow_valid(p.server.raw(), ev->raw().borrow_epoch));
        if (initial.ok()) saved = *initial;
    }
    MOQ_CHECK(peer.valid() && saved.has_value());
    if (!saved) return;
    MOQ_CHECK(saved->has_subscriber_priority && saved->subscriber_priority == 7);
    MOQ_CHECK(saved->has_group_order && saved->group_order == moq::group_order::descending);
    MOQ_CHECK(saved->location && saved->location->kind == moq::location_filter_kind::none);
    MOQ_CHECK(saved->object_delivery_timeout_ms == 0);
    MOQ_CHECK(saved->subgroup_delivery_timeout_ms == UINT64_MAX);
    MOQ_CHECK(p.server.accept_publish(peer, {}, {.expires_ms = UINT64_MAX}, 2).ok());
    settle(p.client, p.server, 2);
    int responses = 0;
    while (auto ev = p.client.poll_event()) {
        auto view = ev->variant();
        if (auto ok = std::get_if<moq::event::publish_ok>(&view)) {
            ++responses;
            MOQ_CHECK(!ok->send_allowed);
            MOQ_CHECK(ok->has_expires && ok->expires_ms == UINT64_MAX);
        }
    }
    MOQ_CHECK(responses == 1);
    moq::publication_update_config update{};
    update.has_subscriber_priority = true;
    update.subscriber_priority = 99;
    MOQ_CHECK(p.server.update_publication(peer, update, 3).ok());
    settle(p.client, p.server, 3);
    auto initial = p.server.get_publish_initial_parameters(peer);
    MOQ_CHECK(initial.ok());
    if (initial.ok()) {
        MOQ_CHECK(initial->subscriber_priority == 7);
        MOQ_CHECK(initial->object_delivery_timeout_ms == 0);
        MOQ_CHECK(initial->subgroup_delivery_timeout_ms == UINT64_MAX);
    }
    MOQ_CHECK(p.client.get_publish_initial_parameters(*sent).error().code() == moq::errc::wrong_state);

    auto legacy = establish_pair(failures, MOQ_VERSION_DRAFT_18);
    MOQ_CHECK(legacy.client.publish(cfg, {.expires_ms = 0}, 1).error().code() == moq::errc::unsupported);
    MOQ_CHECK(!legacy.client.poll_action());
    MOQ_PASS("typed_publish_parameters");
}

static void test_typed_update_options(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    auto [client, server] = subscribe_accepted(failures, p);
    moq::update_options options{};
    options.location = moq::location_filter_config{moq::location_filter_kind::object_end};
    options.location->start_group = 1;
    options.location->end_group = 3;
    options.location->end_object = UINT64_MAX;
    options.object_delivery_timeout_ms = 0;
    options.subgroup_delivery_timeout_ms = UINT64_MAX;
    MOQ_CHECK(p.client.update_subscription(client, {}, options, 3).ok());
    options = {};
    settle(p.client, p.server, 3);
    std::optional<moq::extended_event::subscription_update_options> saved;
    while (auto ev = p.server.poll_event()) {
        auto extended = ev->variant_extended();
        if (auto update = std::get_if<moq::extended_event::subscribe_updated>(&extended)) {
            MOQ_CHECK(update->sub == server);
            MOQ_CHECK(!update->has_filter && !update->has_delivery_timeout);
            saved = update->options;
        }
    }
    MOQ_CHECK(saved && saved->location);
    if (!saved || !saved->location) return;
    const auto first_id = saved->initiating_request_id;
    MOQ_CHECK(saved->location->end_object == UINT64_MAX);
    MOQ_CHECK(saved->object_delivery_timeout_ms == 0);
    MOQ_CHECK(saved->subgroup_delivery_timeout_ms == UINT64_MAX);
    auto current = p.server.subscription_filter(server);
    MOQ_CHECK(current.ok() && current->end_object == UINT64_MAX);
    while (p.client.poll_event()) {}

    // Empty refresh and explicit NONE are distinct value snapshots.
    MOQ_CHECK(p.client.update_subscription(client, {}, {}, 4).ok());
    settle(p.client, p.server, 4);
    while (auto ev = p.server.poll_event()) {
        auto view = ev->variant_extended();
        if (auto update = std::get_if<moq::extended_event::subscribe_updated>(&view)) {
            MOQ_CHECK(update->options && update->options->flags == 0);
            MOQ_CHECK(update->options && update->options->initiating_request_id != first_id);
        }
    }
    while (p.client.poll_event()) {}
    options.location = moq::location_filter_config{};
    MOQ_CHECK(p.client.update_subscription(client, {}, options, 5).ok());
    settle(p.client, p.server, 5);
    current = p.server.subscription_filter(server);
    MOQ_CHECK(current.ok() && current->kind == moq::location_filter_kind::none);
    MOQ_CHECK(saved->location->end_object == UINT64_MAX);

    auto legacy = establish_pair(failures, MOQ_VERSION_DRAFT_18);
    auto [old_client, old_server] = subscribe_accepted(failures, legacy);
    MOQ_CHECK(old_server.valid());
    MOQ_CHECK(legacy.client.update_subscription(old_client, {}, options, 3).error().code() == moq::errc::unsupported);
    MOQ_CHECK(!legacy.client.poll_action());
    // Through the facade: an active draft-18 track refuses the typed
    // options before anything is sent.
    auto facade = moq::subscriber::create(legacy.client).value();
    moq::sub_track_config ftc{};
    ftc.ns    = {"legacy"};
    ftc.track = "f";
    auto ftrack = facade.subscribe(ftc, {}, 3);
    MOQ_CHECK(ftrack.ok());
    settle(legacy.client, legacy.server, 3);
    while (auto ev = legacy.server.poll_event()) {
        auto view = ev->variant_extended();
        if (auto req = std::get_if<moq::extended_event::subscribe_request>(&view))
            MOQ_CHECK(legacy.server.accept_subscribe(req->sub, {}, 3).ok());
    }
    settle(legacy.client, legacy.server, 4);
    MOQ_CHECK(facade.tick(4).ok());
    if (ftrack.ok()) {
        while (legacy.client.poll_action()) {}
        MOQ_CHECK(facade.update_subscription(*ftrack, {}, options, 5).error().code() ==
                  moq::errc::unsupported);
        MOQ_CHECK(!legacy.client.poll_action());
    }
    MOQ_PASS("typed_update_options");
}

static void test_typed_publication_update(int &failures)
{
    for (unsigned row = 0; row < 8; ++row) {
        auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
        moq::publish_config publish{};
        publish.ns = {"a08"}; publish.track = "p";
        auto sent = p.client.publish(publish, 1);
        MOQ_CHECK(sent.ok());
        if (!sent.ok()) return;
        settle(p.client, p.server, 1);
        moq::publication peer{};
        while (auto ev = p.server.poll_event()) {
            auto view = ev->variant();
            if (auto request = std::get_if<moq::event::publish_request>(&view)) peer = request->pub;
        }
        MOQ_CHECK(peer.valid());
        if (!peer.valid()) return;
        MOQ_CHECK(p.server.accept_publish(peer, {}, 2).ok());
        settle(p.client, p.server, 2);
        while (p.client.poll_event()) {}
        auto initial = p.server.get_publish_initial_parameters(peer);
        MOQ_CHECK(initial.ok());

        moq::publication_update_options options{};
        if (row > 0 && row < 4) {
            options.location = moq::location_filter_config{};
            if (row == 2) {
                options.location->kind = moq::location_filter_kind::relative_group;
                options.location->relative_group_offset = UINT64_MAX;
                options.fill = moq::fill_config{};
                options.fill->location = moq::location_filter_config{};
            } else if (row == 3) {
                options.location->kind = moq::location_filter_kind::object_end;
                options.location->start_group = 1;
                options.location->start_object = 2;
                options.location->end_group = 3;
                options.location->end_object = UINT64_MAX;
            }
        }
        if (row == 4 || row == 6 || row == 7)
            options.object_delivery_timeout_ms = row == 7 ? UINT64_MAX : 0;
        if (row == 5 || row == 6 || row == 7)
            options.subgroup_delivery_timeout_ms = row == 7 ? 0 : UINT64_MAX;
        const auto expected = options;
        auto rc = p.server.update_publication(peer, {}, options, 3);
        MOQ_CHECK(rc.ok());
        if (!rc.ok()) return;
        options = {};
        settle(p.client, p.server, 3);
        std::optional<moq::extended_event::subscription_update_options> saved;
        unsigned updates = 0;
        while (auto ev = p.client.poll_event()) {
            auto view = ev->variant_extended();
            if (auto update = std::get_if<moq::extended_event::publish_updated>(&view)) {
                ++updates;
                MOQ_CHECK(update->pub == *sent && update->update_count == 1);
                MOQ_CHECK(!update->has_filter && !update->has_delivery_timeout);
                MOQ_CHECK(update->fill_requested == expected.fill.has_value());
                saved = update->options;
            }
        }
        MOQ_CHECK(updates == 1 && saved.has_value());
        if (!saved) return;
        MOQ_CHECK(saved->location.has_value() == expected.location.has_value());
        if (saved->location && expected.location) {
            MOQ_CHECK(saved->location->kind == expected.location->kind);
            MOQ_CHECK(saved->location->relative_group_offset == expected.location->relative_group_offset);
            MOQ_CHECK(saved->location->end_object == expected.location->end_object);
        }
        MOQ_CHECK(saved->object_delivery_timeout_ms == expected.object_delivery_timeout_ms);
        MOQ_CHECK(saved->subgroup_delivery_timeout_ms == expected.subgroup_delivery_timeout_ms);
        auto after = p.server.get_publish_initial_parameters(peer);
        MOQ_CHECK(after.ok());
        if (initial.ok() && after.ok()) {
            MOQ_CHECK(after->object_delivery_timeout_ms == initial->object_delivery_timeout_ms);
            MOQ_CHECK(after->subgroup_delivery_timeout_ms == initial->subgroup_delivery_timeout_ms);
            MOQ_CHECK(after->location.has_value() == initial->location.has_value());
        }
        moq::publication_update_config generic{};
        generic.has_delivery_timeout = true;
        generic.delivery_timeout_us = 1000;
        MOQ_CHECK(p.server.update_publication(peer, generic, 4).error().code() == moq::errc::unsupported);
        MOQ_CHECK(!p.server.poll_action());
    }
    for (auto version : {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18}) {
        auto p = establish_pair(failures, version);
        moq::publication_update_options options{};
        options.location = moq::location_filter_config{};
        MOQ_CHECK(p.server.update_publication({}, {}, options, 1).error().code() == moq::errc::unsupported);
        options = {};
        options.object_delivery_timeout_ms = 0;
        MOQ_CHECK(p.server.update_publication({}, {}, options, 1).error().code() == moq::errc::unsupported);
        options = {};
        options.subgroup_delivery_timeout_ms = UINT64_MAX;
        MOQ_CHECK(p.server.update_publication({}, {}, options, 1).error().code() == moq::errc::unsupported);
        MOQ_CHECK(!p.server.poll_action());
    }
    MOQ_PASS("typed_publication_update");
}

static void test_initial_admission_and_timeouts(int &failures)
{
    const std::optional<uint64_t> values[] = {std::nullopt, 0, UINT64_MAX};
    std::vector<moq::extended_event::subscription_admission_options> held;
    for (unsigned row = 0; row < 9; ++row) {
        auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
        moq::subscribe_options options{};
        moq::location_filter_config location{};
        location.kind = static_cast<moq::location_filter_kind>(row % 6);
        location.relative_group_offset = 3;
        location.start_group = 7; location.start_object = 9;
        location.end_group = 10; location.end_object = UINT64_MAX;
        options.location = location;
        if (row % 2) options.fill = moq::fill_config{};
        options.object_delivery_timeout_ms = values[row / 3];
        options.subgroup_delivery_timeout_ms = values[row % 3];
        MOQ_CHECK(p.client.subscribe({.ns = {"admission"}, .track = "v"}, options, 0).ok());
        options = {};
        settle(p.client, p.server, 0);
        auto ev = p.server.poll_event();
        MOQ_CHECK(ev.has_value());
        if (!ev) return;
        const auto view = std::get<moq::extended_event::subscribe_request>(ev->variant_extended());
        MOQ_CHECK(view.options.has_value());
        if (!view.options) return;
        auto observed = *view.options;
        MOQ_CHECK(observed.location && observed.location->kind == location.kind);
        if (row % 6 == 1) MOQ_CHECK(observed.location->relative_group_offset == 3);
        if (row % 6 >= 2 && row % 6 <= 4) {
            MOQ_CHECK(observed.location->start_group == 7 && observed.location->start_object == 9);
            if (row % 6 >= 3) MOQ_CHECK(observed.location->end_group == 10);
            if (row % 6 == 4) MOQ_CHECK(observed.location->end_object == UINT64_MAX);
        }
        const uint32_t flags = 1u | (row % 2 ? 2u : 0u) | (row / 3 ? 4u : 0u) | (row % 3 ? 8u : 0u);
        MOQ_CHECK(observed.flags == flags && observed.fill_requested == (row % 2 != 0));
        MOQ_CHECK(observed.object_delivery_timeout_ms == values[row / 3]);
        MOQ_CHECK(observed.subgroup_delivery_timeout_ms == values[row % 3]);
        held.push_back(observed);
        ev.reset();
        MOQ_CHECK(p.server.tick(1).ok());
    }
    for (unsigned row = 0; row < held.size(); ++row) {
        MOQ_CHECK(held[row].location->kind == static_cast<moq::location_filter_kind>(row % 6));
        MOQ_CHECK(held[row].object_delivery_timeout_ms == values[row / 3]);
        MOQ_CHECK(held[row].subgroup_delivery_timeout_ms == values[row % 3]);
    }
    // A literal empty parameter set distinguishes absent LOCATION from NONE.
    {
        auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
        uint8_t empty[] = {3, 0, 7, 0, 1, 1, 'n', 1, 'v', 0};
        MOQ_CHECK(p.server.on_bidi_stream_bytes(moq::stream_ref(0x7100), empty, false, 0).ok());
        auto ev = p.server.poll_event();
        MOQ_CHECK(ev.has_value());
        if (!ev) return;
        const auto request = std::get<moq::extended_event::subscribe_request>(ev->variant_extended());
        MOQ_CHECK(request.options && request.options->flags == 0 && !request.options->location);
        MOQ_CHECK(!request.options->fill_requested && !request.options->object_delivery_timeout_ms &&
                  !request.options->subgroup_delivery_timeout_ms);
    }
    for (auto version : {MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18}) {
        auto p = establish_pair(failures, version);
        for (unsigned selected = 1; selected <= 3; ++selected) {
            moq::subscribe_options options{};
            if (selected & 1) options.object_delivery_timeout_ms = 0;
            if (selected & 2) options.subgroup_delivery_timeout_ms = UINT64_MAX;
            auto refused = p.client.subscribe({.ns = {"admission"}, .track = "v"}, options, 9);
            MOQ_CHECK(!refused.ok() && refused.error().code() == moq::errc::unsupported);
            MOQ_CHECK(!p.client.poll_action());
        }
        MOQ_CHECK(p.client.subscribe({.ns = {"admission"}, .track = "v"}, 0).ok());
        settle(p.client, p.server, 0);
        auto ev = p.server.poll_event();
        MOQ_CHECK(ev.has_value());
        if (ev) MOQ_CHECK(!std::get<moq::extended_event::subscribe_request>(ev->variant_extended()).options);
    }
    MOQ_PASS("initial admission, all locations, independent nil-zero-MAX and legacy refusal");
}

static void test_historical_state_snapshots(int &failures)
{
    for (bool publish : {false, true}) {
        std::vector<moq::extended_event::publish_state> held;
        {
            auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
            opened_streams streams;
            moq::location_filter_config a{};
            a.kind = moq::location_filter_kind::group_end;
            a.end_group = 5;
            moq::subscription sub, server_sub;
            moq::publication client_pub, pub;
            if (publish) {
                moq::publish_options options{};
                options.location = a;
                client_pub = p.client.publish({.ns = {"history"}, .track = "v"}, options, 0).value();
            } else {
                moq::subscribe_options options{};
                options.location = a;
                sub = p.client.subscribe({.ns = {"history"}, .track = "v"}, options, 0).value();
            }
            settle(p.client, p.server, 0, &streams);
            while (auto ev = p.server.poll_event()) {
                ev->visit_extended([&](const moq::extended_event::subscribe_request &r) { server_sub = r.sub; },
                    [&](const moq::event::publish_request &r) { pub = r.pub; }, [](const auto &) {});
            }
            if (publish) MOQ_CHECK(p.server.accept_publish(pub, {}, 0).ok());
            else MOQ_CHECK(p.server.accept_subscribe(server_sub, {}, 0).ok());
            settle(p.client, p.server, 0, &streams);
            while (p.client.poll_event()) {}
            while (p.server.poll_event()) {}
            MOQ_CHECK(streams.by_dir[0].size() == 1);
            if (streams.by_dir[0].size() != 1) return;
            auto &receiver = publish ? p.server : p.client;
            const auto ref = publish ? to_peer(streams.by_dir[0][0], 0) : streams.by_dir[0][0];
            uint8_t notification[] = {0x22, 0, 6, 1, 0x21, 3, 0, 0, 5};
            MOQ_CHECK(receiver.on_bidi_stream_bytes(ref, notification, false, 0).ok());
            MOQ_CHECK(!receiver.poll_action());
            auto b = a; b.end_group = 10;
            moq::update_options update{};
            update.location = b;
            if (publish) {
                moq::publication_update_options pub_update{};
                pub_update.location = b;
                MOQ_CHECK(receiver.update_publication(pub, {}, pub_update, 0).ok());
            } else MOQ_CHECK(receiver.update_subscription(sub, {}, update, 0).ok());
            // Do not poll either notification until the genuine UPDATE/ACK is done.
            settle(p.client, p.server, 0);
            notification[8] = 10;
            MOQ_CHECK(receiver.on_bidi_stream_bytes(ref, notification, false, 0).ok());
            MOQ_CHECK(!receiver.poll_action());
            std::memset(notification, 0xa5, sizeof(notification));
            unsigned ack = 0;
            while (auto ev = receiver.poll_event()) {
                ev->visit_extended([&](const moq::extended_event::publish_state &state) {
                    MOQ_CHECK(state.family == (publish ? moq::request_family::publish : moq::request_family::subscribe));
                    MOQ_CHECK(publish ? state.pub == pub && !state.sub.valid() : state.sub == sub && !state.pub.valid());
                    MOQ_CHECK(state.has_filter && !state.has_forward && !state.has_largest);
                    held.push_back(state);
                }, [&](const moq::event::subscription_update_ok &) { MOQ_CHECK(!publish); ++ack; },
                   [&](const moq::event::publication_update_ok &) { MOQ_CHECK(publish); ++ack; },
                   [&](const auto &) { MOQ_CHECK(false); });
            }
            MOQ_CHECK(ack == 1 && held.size() == 2);
            MOQ_CHECK(receiver.tick(1).ok());
        }
        MOQ_CHECK(held.size() == 2);
        if (held.size() != 2) return;
        MOQ_CHECK(held[0].location && held[1].location);
        MOQ_CHECK(held[0].location->kind == moq::location_filter_kind::group_end);
        MOQ_CHECK(held[0].location->end_group == 5 && held[1].location->end_group == 10);
    }
    MOQ_PASS("two historical state snapshots across real UPDATE/ACK for both owners");
}

static void test_data_committed(int &failures)
{
    // A report for a stream the session is not holding changes nothing.
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    MOQ_CHECK(p.client.on_data_committed(moq::stream_ref(UINT64_C(77)), 1).ok());
    MOQ_CHECK(!p.client.poll_action().has_value());
    MOQ_CHECK(!p.client.poll_event().has_value());
    MOQ_PASS("data_committed");
}

// -- Value pins ----------------------------------------------------------------

static_assert(static_cast<int>(moq::errc::limit) == MOQ_ERR_LIMIT);
static_assert(MOQ_ERR_LIMIT == -16);
static_assert(static_cast<int>(moq::errc::capacity) == MOQ_ERR_CAPACITY);
static_assert(MOQ_ERR_CAPACITY == -17);
static_assert(static_cast<uint32_t>(moq::fetch_ranges::timed_out) ==
              MOQ_FETCH_RANGE_TIMED_OUT);
static_assert(static_cast<uint64_t>(moq::request_errors::conflicting_filters) ==
              MOQ_REQUEST_ERROR_CONFLICTING_FILTERS);
static_assert(static_cast<uint64_t>(moq::request_errors::invalid_filter) ==
              MOQ_REQUEST_ERROR_INVALID_FILTER);
static_assert(static_cast<uint32_t>(moq::request_terminal_cause::truncated_response) ==
              MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE);
static_assert(static_cast<uint32_t>(moq::request_role::responder) ==
              MOQ_REQUEST_ROLE_RESPONDER);
static_assert(moq::stream_error::unknown_object_status ==
              MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS);
static_assert(moq::stream_error::d16_unknown_object_status ==
              MOQ_STREAM_ERROR_D16_UNKNOWN_OBJECT_STATUS);
static_assert(static_cast<int>(moq::sub_fill_item_kind::terminal) ==
              MOQ_SUB_FILL_TERMINAL);

// -- FIRST_OBJECT through the binding ---------------------------------------
// subgroup_config.first_object reaches the wire (draft-22 §2.2) and the
// receiver reports it on the stream's first object only.
static void test_first_object(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    auto subs = subscribe_accepted(failures, p);
    for (int variant = 0; variant < 2; variant++) {
        moq::subgroup_config sg{};
        sg.group_id     = 10 + static_cast<uint64_t>(variant);
        sg.first_object = variant == 0;
        auto h = p.server.open_subgroup(subs.second, sg, 3);
        MOQ_CHECK(h.ok());
        if (!h.ok()) return;
        for (uint64_t o = 0; o < 2; o++) {
            const uint8_t byte = static_cast<uint8_t>(o);
            auto pl = moq::buffer::create(&byte, 1);
            MOQ_CHECK(pl.ok());
            if (!pl.ok()) return;
            MOQ_CHECK(p.server.write_object(*h, o, *pl, 3).ok());
        }
        MOQ_CHECK(p.server.close_subgroup(*h, 3).ok());
        settle(p.client, p.server, 3);
        std::vector<bool> flags;
        while (auto ev = p.client.poll_event())
            ev->visit([&](const moq::event::object_received &obj) {
                          if (obj.group_id == sg.group_id) flags.push_back(obj.first_object);
                      },
                      [](const auto &) {});
        MOQ_CHECK(flags.size() == 2);
        if (flags.size() == 2) {
            MOQ_CHECK(flags[0] == (variant == 0));
            MOQ_CHECK(!flags[1]);
        }
    }
}

// -- Prior Group/Object ID Gaps and Malformed Tracks -------------------------

// write_object_ex with gaps puts them on the wire; the receiver's views read
// them (and the subgroup's PROPERTIES form) only where the C detail covers
// them. A Subgroup reopened at another Publisher Priority makes the Track
// malformed: the subscriber's own cancel surfaces as UNSUBSCRIBED with the
// malformed_track cause and MALFORMED_TRACK (draft-22 §12.1).
static void test_prior_gaps_and_malformed(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    auto subs = subscribe_accepted(failures, p);
    moq::subgroup_config sg{};
    sg.group_id          = 10;
    sg.object_properties = true;
    auto h = p.server.open_subgroup(subs.second, sg, 3);
    MOQ_CHECK(h.ok());
    if (!h.ok()) return;
    const uint8_t byte = 7;
    auto pl = moq::buffer::create(&byte, 1);
    MOQ_CHECK(pl.ok());
    if (!pl.ok()) return;
    MOQ_CHECK(p.server.write_object_ex(*h, 4, *pl, nullptr, 3u, 2u, 3).ok());
    MOQ_CHECK(!p.server.write_object_ex(*h, 5, *pl, nullptr, 11u, std::nullopt, 3).ok());
    MOQ_CHECK(p.server.close_subgroup(*h, 3).ok());
    settle(p.client, p.server, 3);
    int seen = 0;
    while (auto ev = p.client.poll_event())
        ev->visit([&](const moq::event::object_received &obj) {
                      ++seen;
                      MOQ_CHECK(obj.prior_group_gap == std::optional<uint64_t>(3));
                      MOQ_CHECK(obj.prior_object_gap == std::optional<uint64_t>(2));
                      MOQ_CHECK(obj.subgroup_properties);
                  },
                  [](const auto &) {});
    MOQ_CHECK(seen == 1);

    {   // detail_size gate on a hand-built event
        moq_object_received_event_t d{};
        d.has_prior_group_gap = true;
        d.prior_group_gap = 9;
        d.subgroup_properties = true;
        auto full = std::get<moq::event::object_received>(
            convert(MOQ_EVENT_OBJECT_RECEIVED, d, sizeof(d)));
        MOQ_CHECK(full.prior_group_gap == std::optional<uint64_t>(9));
        MOQ_CHECK(!full.prior_object_gap && full.subgroup_properties);
        auto old = std::get<moq::event::object_received>(convert(
            MOQ_EVENT_OBJECT_RECEIVED, d, offsetof(moq_object_received_event_t,
                                                   has_prior_group_gap)));
        MOQ_CHECK(!old.prior_group_gap && !old.subgroup_properties);
    }

    for (uint8_t k = 0; k < 2; k++) {
        moq::subgroup_config re{};
        re.group_id           = 11;
        re.publisher_priority = static_cast<uint8_t>(20 + k);
        auto rh = p.server.open_subgroup(subs.second, re, 4);
        MOQ_CHECK(rh.ok());
        if (!rh.ok()) return;
        MOQ_CHECK(p.server.write_object(*rh, k, *pl, 4).ok());
        if (k == 0) MOQ_CHECK(p.server.close_subgroup(*rh, 4).ok());
        settle(p.client, p.server, 4);
    }
    int malformed = 0;
    while (auto ev = p.client.poll_event()) {
        if (ev->kind() != MOQ_EVENT_UNSUBSCRIBED) continue;
        const auto value = ev->variant_extended();
        const auto *u = std::get_if<moq::extended_event::unsubscribed>(&value);
        MOQ_CHECK(u != nullptr);
        if (u && u->cause == moq::request_terminal_cause::malformed_track &&
            u->stream_error_code == MOQ_STREAM_ERROR_MALFORMED_TRACK)
            ++malformed;
    }
    MOQ_CHECK(malformed == 1);
}

// -- Track Properties and INCLUDE_PROPERTIES ---------------------------------

static const uint8_t k_cpp_props[] = {0x78, 0x01};

static bool same_props(std::span<const uint8_t> got)
{
    return got.size() == sizeof(k_cpp_props) &&
           std::memcmp(got.data(), k_cpp_props, sizeof(k_cpp_props)) == 0;
}

// The binding's INCLUDE_PROPERTIES reaches the wire and empties the answers;
// absent, the answers carry what the responder supplied (TRACK_STATUS_OK,
// SUBSCRIBE_OK, FETCH_OK).
static void test_track_properties_and_include(int &failures, std::optional<bool> include)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    const bool want = !include.has_value() || *include;

    moq::track_status_config tc{};
    tc.ns    = {"d22"};
    tc.track = "v";
    tc.include_properties = include;
    MOQ_CHECK(p.client.track_status(tc, 1).ok());
    settle(p.client, p.server, 1);
    std::optional<moq::track_status_handle> th;
    while (auto ev = p.server.poll_event())
        ev->visit([&](const moq::event::track_status_request &r) { th = r.handle; },
                  [](const auto &) {});
    MOQ_CHECK(th.has_value());
    if (!th) return;
    moq::accept_track_status_config ac{};
    ac.has_largest      = true;
    ac.largest_group    = 9;
    ac.track_properties = moq::bytes_view(k_cpp_props, sizeof(k_cpp_props));
    MOQ_CHECK(p.server.accept_track_status(*th, ac, 2).ok());
    settle(p.client, p.server, 2);
    int oks = 0;
    while (auto ev = p.client.poll_event())
        ev->visit([&](const moq::event::track_status_ok &ok) {
                      ++oks;
                      MOQ_CHECK(ok.has_largest && ok.largest_group == 9);
                      MOQ_CHECK(want ? same_props(ok.track_properties)
                                     : ok.track_properties.empty());
                  },
                  [](const auto &) {});
    MOQ_CHECK(oks == 1);

    moq::subscribe_config sc{};
    sc.ns    = {"d22"};
    sc.track = "v";
    moq::subscribe_options so{};
    so.include_properties = include;
    MOQ_CHECK(p.client.subscribe(sc, so, 3).ok());
    settle(p.client, p.server, 3);
    moq::subscription ssub{};
    while (auto ev = p.server.poll_event())
        ev->visit([&](const moq::event::subscribe_request &r) { ssub = r.sub; },
                  [](const auto &) {});
    moq::accept_subscribe_config sac{};
    sac.track_properties = moq::bytes_view(k_cpp_props, sizeof(k_cpp_props));
    MOQ_CHECK(p.server.accept_subscribe(ssub, sac, 4).ok());
    settle(p.client, p.server, 4);
    int sub_oks = 0;
    while (auto ev = p.client.poll_event())
        ev->visit([&](const moq::event::subscribe_ok &ok) {
                      ++sub_oks;
                      MOQ_CHECK(want ? same_props(ok.track_properties)
                                     : ok.track_properties.empty());
                  },
                  [](const auto &) {});
    MOQ_CHECK(sub_oks == 1);

    moq::fetch_config fc{};
    fc.ns          = {"d22"};
    fc.track       = "v";
    fc.start_group = 1;
    fc.end_group   = 1;
    fc.end_object  = 1;
    moq::fetch_options fo{};
    fo.include_properties = include;
    MOQ_CHECK(p.client.fetch(fc, fo, 5).ok());
    settle(p.client, p.server, 5);
    std::optional<moq::fetch_handle> fh;
    while (auto ev = p.server.poll_event())
        ev->visit([&](const moq::event::fetch_request &r) { fh = r.fetch; },
                  [](const auto &) {});
    MOQ_CHECK(fh.has_value());
    if (!fh) return;
    moq::accept_fetch_config fac{};
    fac.end_group = 1;
    fac.end_object = 1;
    fac.track_properties = moq::bytes_view(k_cpp_props, sizeof(k_cpp_props));
    MOQ_CHECK(p.server.accept_fetch(*fh, fac, 6).ok());
    settle(p.client, p.server, 6);
    int fetch_oks = 0;
    while (auto ev = p.client.poll_event())
        ev->visit([&](const moq::event::fetch_ok &ok) {
                      ++fetch_oks;
                      MOQ_CHECK(want ? same_props(ok.track_properties)
                                     : ok.track_properties.empty());
                  },
                  [](const auto &) {});
    MOQ_CHECK(fetch_oks == 1);
}

// Draft-18 has no INCLUDE_PROPERTIES: selecting it is unsupported.
static void test_include_properties_unsupported(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_18);
    moq::track_status_config tc{};
    tc.ns    = {"d22"};
    tc.track = "v";
    tc.include_properties = false;
    auto r = p.client.track_status(tc, 1);
    MOQ_CHECK(!r.ok() && r.error().code() == moq::errc::unsupported);
}

struct new_group_seen {
    int calls = 0;
    uint64_t requested = UINT64_MAX;
    bool has_current = true;
};

// The publisher facade states its track's properties with DYNAMIC_GROUPS
// added, reports a subscriber's NEW_GROUP_REQUEST through the observer, and
// answers a TRACK_STATUS for its track on tick.
static void test_publisher_track_properties(int &failures)
{
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    new_group_seen seen;
    moq::publisher_observer obs{};
    obs.ctx = &seen;
    obs.on_new_group_request = [](void *ctx, moq::publisher_track,
                                  moq::result<moq::pub_new_group_request_view> v) noexcept {
        auto &s = *static_cast<new_group_seen *>(ctx);
        ++s.calls;
        if (v.ok()) {
            s.requested = v->requested_group;
            s.has_current = v->current_group.has_value();
        }
    };
    moq::publisher_config pc{};
    pc.accept_mode = moq::pub_accept_mode::accept_all;
    auto pub = moq::publisher::create_observed(p.server, obs, pc);
    MOQ_CHECK(pub.ok());
    if (!pub.ok()) return;
    moq::pub_track_config tc{};
    tc.ns    = {"d22"};
    tc.track = "v";
    tc.track_properties = moq::bytes_view(k_cpp_props, sizeof(k_cpp_props));
    tc.dynamic_groups = true;
    MOQ_CHECK(pub->add_track(tc, 1).ok());

    moq::subscribe_config sc{};
    sc.ns    = {"d22"};
    sc.track = "v";
    sc.has_new_group_request = true;
    sc.new_group_request     = 0;
    MOQ_CHECK(p.client.subscribe(sc, 2).ok());
    moq::track_status_config ts{};
    ts.ns    = {"d22"};
    ts.track = "v";
    MOQ_CHECK(p.client.track_status(ts, 2).ok());
    for (int i = 0; i < 4; i++) {
        settle(p.client, p.server, 3);
        (void)pub->tick(3);
    }
    settle(p.client, p.server, 3);
    static const uint8_t stated[] = {0x30, 0x01, 0x48, 0x01};
    int sub_oks = 0, ts_oks = 0;
    while (auto ev = p.client.poll_event())
        ev->visit([&](const moq::event::subscribe_ok &ok) {
                      ++sub_oks;
                      MOQ_CHECK(ok.dynamic_groups);
                      MOQ_CHECK(ok.track_properties.size() == sizeof(stated) &&
                                std::memcmp(ok.track_properties.data(), stated,
                                            sizeof(stated)) == 0);
                  },
                  [&](const moq::event::track_status_ok &ok) {
                      ++ts_oks;
                      MOQ_CHECK(ok.track_properties.size() == sizeof(stated));
                  },
                  [](const auto &) {});
    MOQ_CHECK(sub_oks == 1);
    MOQ_CHECK(ts_oks == 1);
    MOQ_CHECK(seen.calls == 1);
    MOQ_CHECK(seen.requested == 0 && !seen.has_current);
}

struct goaway_seen {
    int calls = 0;
    std::string uri;
};

static void on_goaway_seen(void *ctx, std::span<const uint8_t> uri) noexcept
{
    auto &s = *static_cast<goaway_seen *>(ctx);
    ++s.calls;
    s.uri.assign(reinterpret_cast<const char *>(uri.data()), uri.size());
}

// The observer's on_goaway is read when the GOAWAY arrives, like its other
// callbacks: one present at creation and cleared before the GOAWAY is not
// called; one absent at creation and set before it is, once, with the URI
// (also an empty one).
static void test_publisher_observer_goaway(int &failures)
{
    struct row { bool initially; bool later; const char *uri; };
    const row rows[] = {
        {true, false, "moqt://next.example/"},
        {false, true, "moqt://next.example/"},
        {false, true, ""},
    };
    for (const row &r : rows) {
        auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
        goaway_seen seen;
        moq::publisher_observer obs{};
        obs.ctx = &seen;
        obs.on_goaway = r.initially ? on_goaway_seen : nullptr;
        auto pub = moq::publisher::create_observed(p.client, obs, moq::publisher_config{});
        MOQ_CHECK(pub.ok());
        if (!pub.ok()) continue;
        obs.on_goaway = r.later ? on_goaway_seen : nullptr;
        std::string_view uri(r.uri);
        MOQ_CHECK(p.server.goaway(moq::bytes_view(reinterpret_cast<const uint8_t *>(uri.data()),
                                                  uri.size()), 2).ok());
        settle(p.client, p.server, 3);
        (void)pub->tick(3);
        MOQ_CHECK(seen.calls == (r.later ? 1 : 0));
        if (r.later) MOQ_CHECK(seen.uri == r.uri);
    }
}

int main()
{
    int failures = 0;
    test_track_properties_and_include(failures, std::nullopt);
    test_track_properties_and_include(failures, true);
    test_track_properties_and_include(failures, false);
    test_include_properties_unsupported(failures);
    test_publisher_track_properties(failures);
    test_capabilities_and_versions(failures);
    test_session_config_fields(failures);
    test_subscription_update_deferral(failures);
    test_publication_update_deferral(failures);
    test_fetch_options_and_timed_out(failures);
    test_fetch_draft_18(failures);
    test_request_terminal_causes(failures);
    test_subscriber_facade_fills(failures);
    test_fill_item_moves_and_fixed_width_values(failures);
    test_overlapping_facade_fill_observations(failures);
    test_fill_refused_by_facade_on_draft_18(failures);
    test_publisher_facade_max_fills(failures);
    test_subscriber_facade_fill_capacity(failures);
    test_fetch_defer_from_config(failures);
    test_event_field_gates(failures);
    test_typed_terminal_domains(failures);
    test_fetch_options_prefix(failures);
    test_fetch_typed_locations(failures);
    test_typed_publish_parameters(failures);
    test_typed_update_options(failures);
    test_typed_publication_update(failures);
    test_data_committed(failures);
    test_initial_admission_and_timeouts(failures);
    test_historical_state_snapshots(failures);
    test_first_object(failures);
    test_prior_gaps_and_malformed(failures);
    test_publisher_observer_goaway(failures);
    if (failures) {
        std::fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    std::printf("PASS: cpp_d22_surface\n");
    return 0;
}
