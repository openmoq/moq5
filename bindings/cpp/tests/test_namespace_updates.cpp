// Draft-22 namespace updates, rendezvous and SUBSCRIBE_TRACKS options through
// the C++ binding: every config is a plain value copied for the call, the
// update events and owed-update queries are borrowed views copied out here,
// and every field reaches the wire and back.
#include <moq/moq.hpp>
#include "test_support.hpp"

#include <cstdint>
#include <cstdio>
#include <optional>
#include <string_view>
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


static std::string_view sv(moq_bytes_t b)
{
    return {reinterpret_cast<const char *>(b.data), b.len};
}

// An announcement update carrying a token reaches the deferring receiver as
// one request event naming its own Request ID; the owed query reports the
// same record; the acceptance concludes the update once on the announcer.
static void test_announcement_update(int &failures)
{
    const int before = failures;
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    MOQ_CHECK(p.server.set_defer_namespace_updates(true).ok());
    moq::publish_namespace_config pc{};
    pc.ns = {"n"};
    auto ann = p.client.publish_namespace(pc, 1);
    MOQ_CHECK(ann.ok());
    settle(p.client, p.server, 1);
    moq::announcement sann{};
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::event::namespace_published &np) { sann = np.ann; },
                           [](const auto &) {});
    MOQ_CHECK(sann.valid());
    MOQ_CHECK(p.server.accept_namespace(sann, 2).ok());
    settle(p.client, p.server, 2);
    while (p.client.poll_event()) {}

    static const uint8_t token[] = {0xaa};
    moq_auth_token_t tok{0, {token, 1}};
    moq::namespace_update_config uc{};
    uc.auth_tokens = {&tok, 1};
    auto id = p.client.update_namespace(*ann, uc, 3);
    MOQ_CHECK(id.ok());
    settle(p.client, p.server, 3);
    int requests = 0;
    while (auto ev = p.server.poll_event())
        ev->visit_extended(
            [&](const moq::extended_event::namespace_update_request &r) {
                ++requests;
                MOQ_CHECK(r.ann == sann);
                MOQ_CHECK(r.update_request_id == *id);
                MOQ_CHECK(r.update.update_request_id == *id);
                MOQ_CHECK(r.update.has_authorization);
                MOQ_CHECK(r.update.tokens.size() == 1);
                MOQ_CHECK(!r.update.prefix_raw && !r.update.forward);
            },
            [](const auto &) {});
    MOQ_CHECK(requests == 1);
    auto owed = p.server.namespace_owed_update(sann);
    MOQ_CHECK(owed.ok());
    MOQ_CHECK(owed.ok() && owed->update_request_id == *id &&
              owed->has_authorization && owed->tokens.size() == 1);
    moq::accept_subscription_update_config ac{};
    ac.update_request_id = *id;
    MOQ_CHECK(p.server.accept_namespace_update(sann, ac, 4).ok());
    MOQ_CHECK(p.server.namespace_owed_update(sann).error().code() == moq::errc::wrong_state);
    settle(p.client, p.server, 4);
    int results = 0;
    while (auto ev = p.client.poll_event())
        ev->visit_extended(
            [&](const moq::extended_event::namespace_update_result &r) {
                ++results;
                MOQ_CHECK(r.ann == *ann);
                MOQ_CHECK(r.update_request_id == *id);
                MOQ_CHECK(r.outcome == moq::extended_event::namespace_update_outcome::ok);
                MOQ_CHECK(r.local_role == moq::request_role::requester);
            },
            [](const auto &) {});
    MOQ_CHECK(results == 1);
    if (failures == before) std::printf("PASS: cpp_namespace_announcement_update\n");
}

// A namespace-subscription prefix update reaches the receiver with its new
// prefix; a refusal concludes it on the requester with the error code.
static void test_ns_sub_prefix_refused(int &failures)
{
    const int before = failures;
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    MOQ_CHECK(p.server.set_defer_namespace_updates(true).ok());
    moq::subscribe_namespace_config nc{};
    nc.ns_prefix = {"n"};
    nc.interest = moq::namespace_interest::namespace_state;
    auto h = p.client.subscribe_namespace(nc, 1);
    MOQ_CHECK(h.ok());
    settle(p.client, p.server, 1);
    moq::ns_sub_handle sh{};
    while (auto ev = p.server.poll_event())
        ev->visit_extended([&](const moq::event::ns_sub_request &r) { sh = r.handle; },
                           [](const auto &) {});
    MOQ_CHECK(sh.valid());
    MOQ_CHECK(p.server.accept_ns_sub(sh, 2).ok());
    settle(p.client, p.server, 2);
    while (p.client.poll_event()) {}

    moq::namespace_update_config uc{};
    uc.prefix = moq::namespace_name{"m"};
    auto id = p.client.update_ns_sub(*h, uc, 3);
    MOQ_CHECK(id.ok());
    // FORWARD is not a namespace-subscription parameter.
    moq::namespace_update_config bad{};
    bad.forward = false;
    MOQ_CHECK(p.client.update_ns_sub(*h, bad, 3).error().code() == moq::errc::invalid);
    settle(p.client, p.server, 3);
    int requests = 0;
    while (auto ev = p.server.poll_event())
        ev->visit_extended(
            [&](const moq::extended_event::ns_sub_update_request &r) {
                ++requests;
                MOQ_CHECK(r.update_request_id == *id);
                auto prefix = r.update.prefix();
                MOQ_CHECK(prefix.has_value());
                if (prefix) {
                    MOQ_CHECK(prefix->c_namespace().count == 1);
                    MOQ_CHECK(sv(prefix->c_namespace().parts[0]) == "m");
                }
            },
            [](const auto &) {});
    MOQ_CHECK(requests == 1);
    moq::reject_subscription_update_config rc{};
    rc.update_request_id = *id;
    rc.error_code = moq::request_error::unauthorized;
    MOQ_CHECK(p.server.reject_ns_sub_update(sh, rc, 4).ok());
    settle(p.client, p.server, 4);
    int results = 0;
    while (auto ev = p.client.poll_event())
        ev->visit_extended(
            [&](const moq::extended_event::ns_sub_update_result &r) {
                ++results;
                MOQ_CHECK(r.handle == *h);
                MOQ_CHECK(r.outcome == moq::extended_event::namespace_update_outcome::error);
                MOQ_CHECK(r.error_code == moq::request_error::unauthorized);
            },
            [](const auto &) {});
    MOQ_CHECK(results == 1);
    if (failures == before) std::printf("PASS: cpp_namespace_ns_sub_prefix_refused\n");
}

// RENDEZVOUS_TIMEOUT rides subscribe_options and reaches the publisher's
// admission view exactly; an older draft refuses it before sending.
static void test_rendezvous_option(int &failures)
{
    const int before = failures;
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    moq::subscribe_config sc{};
    sc.ns = {"r"};
    sc.track = "v";
    moq::subscribe_options so{};
    so.rendezvous_timeout_ms = 25;
    MOQ_CHECK(p.client.subscribe(sc, so, 1).ok());
    settle(p.client, p.server, 1);
    int seen = 0;
    while (auto ev = p.server.poll_event())
        ev->visit_extended(
            [&](const moq::extended_event::subscribe_request &r) {
                ++seen;
                MOQ_CHECK(r.options.has_value());
                MOQ_CHECK(r.options && r.options->rendezvous_timeout_ms == 25u);
            },
            [](const auto &) {});
    MOQ_CHECK(seen == 1);

    auto p18 = establish_pair(failures, MOQ_VERSION_DRAFT_18);
    MOQ_CHECK(p18.client.subscribe(sc, so, 1).error().code() == moq::errc::unsupported);
    if (failures == before) std::printf("PASS: cpp_rendezvous_option\n");
}

// The SUBSCRIBE_TRACKS options of a draft-22 request are a copied view on
// the polled event; other events report none.
static void test_subscribe_tracks_options(int &failures)
{
    const int before = failures;
    auto p = establish_pair(failures, MOQ_VERSION_DRAFT_22);
    moq_bytes_t part{reinterpret_cast<const uint8_t *>("t"), 1};
    moq_subscribe_tracks_cfg_t c;
    moq_subscribe_tracks_cfg_init(&c);
    c.track_namespace_prefix = {&part, 1};
    c.options_flags = MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER |
                      MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES;
    c.group_order = MOQ_GROUP_ORDER_DESCENDING;
    c.include_properties = false;
    moq_track_sub_handle_t h{};
    MOQ_CHECK(moq_session_subscribe_tracks(const_cast<moq_session_t *>(p.client.raw()),
                                           &c, 1, &h) == MOQ_OK);
    settle(p.client, p.server, 1);
    int seen = 0;
    while (auto ev = p.server.poll_event()) {
        auto o = ev->subscribe_tracks_options();
        if (ev->kind() != MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST) {
            MOQ_CHECK(!o.has_value());
            continue;
        }
        ++seen;
        MOQ_CHECK(o.has_value());
        if (!o) continue;
        MOQ_CHECK(o->flags == (MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER |
                               MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES));
        MOQ_CHECK(o->group_order == moq::group_order::descending);
        MOQ_CHECK(!o->include_properties);
    }
    MOQ_CHECK(seen == 1);
    if (failures == before) std::printf("PASS: cpp_subscribe_tracks_options\n");
}

static void test_historical_result_extent(int &failures)
{
    moq_event_t raw{};
    raw.kind = MOQ_EVENT_NAMESPACE_UPDATE_RESULT;
    auto &d = raw.u.namespace_update_result;
    d.ann._opaque = 27;
    d.update_request_id = UINT64_MAX;
    d.outcome = MOQ_NAMESPACE_UPDATE_OUTCOME_OK;
    d.local_role = MOQ_REQUEST_ROLE_REQUESTER;
    d.has_expires = true;
    d.expires_ms = 0;
    const size_t v0 = offsetof(moq_namespace_update_result_event_t, expires_ms) + sizeof(d.expires_ms);
    for (size_t size : {v0 - 1, v0, sizeof(d)}) {
        raw.detail_size = static_cast<uint32_t>(size);
        const auto value = moq::polled_event(raw).variant_extended();
        const auto *result = std::get_if<moq::extended_event::namespace_update_result>(&value);
        if (size < v0) {
            MOQ_CHECK(std::holds_alternative<moq::event::unknown>(value));
        } else {
            MOQ_CHECK(result != nullptr);
            if (result != nullptr) {
                MOQ_CHECK(result->ann == moq::announcement(d.ann));
                MOQ_CHECK(result->update_request_id == UINT64_MAX);
                MOQ_CHECK(result->outcome == moq::extended_event::namespace_update_outcome::ok);
                MOQ_CHECK(result->local_role == moq::request_role::requester);
                MOQ_CHECK(result->expires_ms == 0u);
            }
        }
    }
}

int main()
{
    int failures = 0;
    test_historical_result_extent(failures);
    test_announcement_update(failures);
    test_ns_sub_prefix_refused(failures);
    test_rendezvous_option(failures);
    test_subscribe_tracks_options(failures);
    if (failures) {
        std::fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    std::printf("PASS: cpp_namespace_updates\n");
    return 0;
}
