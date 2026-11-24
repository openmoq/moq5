// Session control surfaces through the C++ binding: SETUP authorization
// tokens, the GOAWAY Timeout, SUBSCRIBE_TRACKS with its answers and
// PUBLISH_BLOCKED, closing with a code, the fetcher's and the publisher's own
// updates, the resolved filter window, LOC metadata by transport version, and
// the facades' delivery timeouts, migration targets and GOAWAY URI.
#include <moq/moq.hpp>
#include <moq/loc.hpp>
#include "test_support.hpp"

#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
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


static session_pair make_pair(int &failures, moq_version_t v,
                              std::span<const moq_auth_token_t> ctokens = {},
                              uint64_t server_goaway_us = 0)
{
    moq::session_config ccfg{}, scfg{};
    ccfg.version = v; scfg.version = v;
    ccfg.perspective = moq::perspective::client;
    scfg.perspective = moq::perspective::server;
    ccfg.send_request_capacity = scfg.send_request_capacity = true;
    ccfg.initial_request_capacity = scfg.initial_request_capacity = 16;
    scfg.goaway_timeout_us = server_goaway_us;
    ccfg.setup_auth_tokens = ctokens;
    auto cr = moq::session::create(ccfg, moq::session_options{}, 0);
    auto sr = moq::session::create(scfg, moq::session_options{}, 0);
    MOQ_CHECK(cr.ok());
    MOQ_CHECK(sr.ok());
    auto c = std::move(*cr);
    auto s = std::move(*sr);
    bind_if_required(failures, c, true);
    bind_if_required(failures, s, false);
    MOQ_CHECK(c.start(0).ok());
    if (v != MOQ_VERSION_DRAFT_16) MOQ_CHECK(s.start(0).ok());
    settle(c, s, 0);
    return session_pair{std::move(c), std::move(s)};
}

template <typename T, typename F>
static bool take(moq::session &s, F &&check)
{
    bool found = false;
    while (auto ev = s.poll_event()) {
        auto value = ev->variant_extended();
        if (const auto *t = std::get_if<T>(&value); t && !found) {
            found = true;
            check(*t);
        }
    }
    return found;
}

static int test_setup_tokens_and_close()
{
    int failures = 0;
    uint8_t value[] = {0x0a, 0x0b};
    moq_auth_token_t tok{3, {value, sizeof(value)}};
    auto p = make_pair(failures, MOQ_VERSION_DRAFT_22,
                       std::span<const moq_auth_token_t>(&tok, 1));
    bool seen = take<moq::event::setup_complete>(p.server, [&](const auto &e) {
        MOQ_CHECK(e.tokens.size() == 1);
        if (e.tokens.size() == 1) {
            MOQ_CHECK(e.tokens[0].token_type == 3);
            MOQ_CHECK(e.tokens[0].token_value.len == 2);
        }
    });
    MOQ_CHECK(seen);
    while (p.client.poll_event()) {}
    MOQ_CHECK(p.client.close(0x2, "bye", 1).ok());
    MOQ_CHECK(p.client.state() == moq::session_state::closed);
    seen = take<moq::event::session_closed>(p.client, [&](const auto &e) {
        MOQ_CHECK(e.code == 0x2);
    });
    MOQ_CHECK(seen);
    MOQ_PASS("setup_tokens_and_close");
    return failures;
}

static int test_goaway_timeout()
{
    int failures = 0;
    auto p = make_pair(failures, MOQ_VERSION_DRAFT_22, {}, 4000000);
    while (p.client.poll_event()) {}
    MOQ_CHECK(p.server.goaway({}, 1).ok());
    settle(p.client, p.server, 2);
    bool seen = take<moq::event::goaway>(p.client, [&](const auto &e) {
        MOQ_CHECK(e.timeout_ms.has_value() && *e.timeout_ms == 4000);
    });
    MOQ_CHECK(seen);
    auto q = make_pair(failures, MOQ_VERSION_DRAFT_16);
    while (q.client.poll_event()) {}
    MOQ_CHECK(q.server.goaway({}, 1).ok());
    settle(q.client, q.server, 2);
    seen = take<moq::event::goaway>(q.client, [&](const auto &e) {
        MOQ_CHECK(!e.timeout_ms.has_value());
    });
    MOQ_CHECK(seen);
    MOQ_PASS("goaway_timeout");
    return failures;
}

static int test_subscribe_tracks()
{
    int failures = 0;
    auto p = make_pair(failures, MOQ_VERSION_DRAFT_22);
    while (p.client.poll_event()) {}
    while (p.server.poll_event()) {}
    moq::subscribe_tracks_config cfg;
    cfg.ns_prefix = moq::namespace_name{"live"};
    cfg.forward = false;
    auto h = p.client.subscribe_tracks(cfg, 1);
    MOQ_CHECK(h.ok());
    settle(p.client, p.server, 2);
    std::optional<moq::track_sub_handle> sh;
    MOQ_CHECK((take<moq::event::subscribe_tracks_request>(p.server, [&](const auto &e) {
        sh = e.handle;
        MOQ_CHECK(!e.forward);
    })));
    MOQ_CHECK(sh.has_value());
    if (!sh) return failures;
    MOQ_CHECK(p.server.accept_subscribe_tracks(*sh, 3).ok());
    settle(p.client, p.server, 4);
    MOQ_CHECK(take<moq::event::subscribe_tracks_ok>(p.client, [&](const auto &) {}));
    MOQ_CHECK(p.server.send_publish_blocked(*sh, moq::namespace_name{"cam"},
                                            moq::bytes_view("v"), 5).ok());
    settle(p.client, p.server, 6);
    MOQ_CHECK(take<moq::event::publish_blocked>(p.client, [&](const auto &e) {
        MOQ_CHECK(e.track_name.size() == 1);
    }));

    /* A second request, for another prefix, refused. */
    cfg.forward.reset();
    cfg.ns_prefix = moq::namespace_name{"other"};
    auto h2 = p.client.subscribe_tracks(cfg, 7);
    MOQ_CHECK(h2.ok());
    settle(p.client, p.server, 8);
    sh.reset();
    MOQ_CHECK((take<moq::event::subscribe_tracks_request>(p.server, [&](const auto &e) {
        sh = e.handle;
    })));
    if (sh) {
        moq::reject_subscribe_tracks_config rj;
        rj.error_code = moq::request_error::not_supported;
        rj.reason = "no";
        MOQ_CHECK(p.server.reject_subscribe_tracks(*sh, rj, 9).ok());
        settle(p.client, p.server, 10);
        MOQ_CHECK(take<moq::event::subscribe_tracks_error>(p.client, [&](const auto &e) {
            MOQ_CHECK(e.error_code == moq::request_error::not_supported);
        }));
    }
    MOQ_PASS("subscribe_tracks");
    return failures;
}

static int test_updates_and_window()
{
    int failures = 0;
    auto p = make_pair(failures, MOQ_VERSION_DRAFT_22);
    while (p.client.poll_event()) {}
    while (p.server.poll_event()) {}
    /* The filter window on both sides of a subscription. */
    moq::subscribe_config sc;
    sc.ns = moq::namespace_name{"live"};
    sc.track = moq::bytes_view("v");
    auto sub = p.client.subscribe(sc, 1);
    MOQ_CHECK(sub.ok());
    if (sub.ok()) {
        auto w0 = p.client.subscription_window(*sub);
        MOQ_CHECK(w0.ok() && !w0->resolved);
    }
    settle(p.client, p.server, 2);
    std::optional<moq::subscription> ssub;
    MOQ_CHECK(take<moq::extended_event::subscribe_request>(p.server, [&](const auto &e) { ssub = e.sub; }));
    if (ssub) {
        moq::accept_subscribe_config ac;
        ac.has_largest = true; ac.largest_group = 5; ac.largest_object = 2;
        MOQ_CHECK(p.server.accept_subscribe(*ssub, ac, 3).ok());
        auto w = p.server.subscription_window(*ssub);
        MOQ_CHECK(w.ok() && w->resolved && w->start_group == 5 && w->start_object == 3);
    }
    settle(p.client, p.server, 4);
    while (p.client.poll_event()) {}

    /* The fetcher's own update. */
    moq::fetch_config fc;
    fc.ns = moq::namespace_name{"live"};
    fc.track = moq::bytes_view("v");
    fc.end_group = 4;
    auto f = p.client.fetch(fc, 5);
    MOQ_CHECK(f.ok());
    if (f.ok()) {
        moq::fetch_update_config fu;
        MOQ_CHECK(p.client.update_fetch(*f, fu, 6).error().code() == moq::errc::invalid);
        fu.subscriber_priority = 9;
        MOQ_CHECK(p.client.update_fetch(*f, fu, 6).ok());
        MOQ_CHECK(p.client.update_fetch(*f, fu, 6).error().code() == moq::errc::wrong_state);
    }
    /* The publisher's own update of its PUBLISH. */
    moq::publish_config pc;
    pc.ns = moq::namespace_name{"live"};
    pc.track = moq::bytes_view("p");
    auto pub = p.client.publish(pc, 7);
    MOQ_CHECK(pub.ok());
    settle(p.client, p.server, 8);
    std::optional<moq::publication> spub;
    MOQ_CHECK(take<moq::event::publish_request>(p.server, [&](const auto &e) { spub = e.pub; }));
    if (spub) MOQ_CHECK(p.server.accept_publish(*spub, moq::accept_publish_config{}, 9).ok());
    settle(p.client, p.server, 10);
    while (p.client.poll_event()) {}
    if (pub.ok()) {
        uint8_t tv[] = {1};
        moq_auth_token_t tok{1, {tv, 1}};
        moq::publication_update_config u;
        u.auth_tokens = std::span<const moq_auth_token_t>(&tok, 1);
        MOQ_CHECK(p.client.update_publication(*pub, u, 11).ok());
        settle(p.client, p.server, 12);
        MOQ_CHECK(take<moq::event::publication_update_ok>(p.client, [](const auto &) {}));
        auto pw = p.client.publication_window(*pub);
        MOQ_CHECK(pw.ok());
    }
    MOQ_PASS("updates_and_window");
    return failures;
}

static int test_loc_metadata()
{
    int failures = 0;
    for (moq_version_t v : {MOQ_VERSION_DRAFT_22, MOQ_VERSION_DRAFT_16}) {
        moq::loc::metadata m;
        m.timestamp = 1234;
        m.audio_level = 0x85;
        if (v == MOQ_VERSION_DRAFT_22) m.timescale = 90000;
        auto enc = moq::loc::encode_metadata(v, m);
        MOQ_CHECK(enc.ok());
        if (!enc.ok()) continue;
        auto dec = moq::loc::parse_metadata(v, moq::bytes_view(enc->data(), enc->size()));
        MOQ_CHECK(dec.ok());
        if (dec.ok()) {
            MOQ_CHECK(dec->timestamp == std::optional<uint64_t>(1234));
            MOQ_CHECK(dec->audio_level == std::optional<uint8_t>(0x85));
            MOQ_CHECK(dec->timescale == m.timescale);
        }
    }
    /* LOC-01 has no timescale: refused rather than dropped. */
    moq::loc::metadata m;
    m.timescale = 1000;
    MOQ_CHECK(moq::loc::encode_metadata(MOQ_VERSION_DRAFT_16, m).error().code() ==
              moq::errc::unsupported);
    MOQ_CHECK(moq::loc::parse_metadata(static_cast<moq_version_t>(0), {}).error().code() == moq::errc::invalid);

    /* LOC-05 by name: the Codec String's exact bytes round trip, after the
     * other keys; the no-profile overloads stay LOC-04, which refuses to
     * encode one and skips it on parse. */
    moq::loc::metadata c;
    c.timestamp = 7;
    c.codec_string = std::string_view("avc1.64001f");
    auto enc5 = moq::loc::encode_metadata(MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_05, c);
    MOQ_CHECK(enc5.ok());
    if (enc5.ok()) {
        static const uint8_t wire[] = {0x10, 0x07, 0x01, 0x0b, 'a', 'v', 'c', '1', '.',
                                       '6', '4', '0', '0', '1', 'f'};
        MOQ_CHECK(enc5->size() == sizeof(wire) &&
                  std::memcmp(enc5->data(), wire, sizeof(wire)) == 0);
        moq::bytes_view bytes(enc5->data(), enc5->size());
        auto dec5 = moq::loc::parse_metadata(MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_05, bytes);
        MOQ_CHECK(dec5.ok() && dec5->codec_string &&
                  *dec5->codec_string == std::string_view("avc1.64001f") &&
                  dec5->codec_string->data() == reinterpret_cast<const char *>(enc5->data()) + 4);
        auto dec4 = moq::loc::parse_metadata(MOQ_VERSION_DRAFT_22, bytes);
        MOQ_CHECK(dec4.ok() && !dec4->codec_string && dec4->timestamp == std::optional<uint64_t>(7));
    }
    MOQ_CHECK(moq::loc::encode_metadata(MOQ_VERSION_DRAFT_22, c).error().code() ==
              moq::errc::unsupported);
    c.codec_string = std::string_view("op\0us", 5);
    MOQ_CHECK(moq::loc::encode_metadata(MOQ_VERSION_DRAFT_22, MOQ_LOC_PROFILE_05, c).error().code() ==
              moq::errc::invalid);
    MOQ_PASS("loc_metadata");
    return failures;
}

static int test_facades()
{
    int failures = 0;
    auto p = make_pair(failures, MOQ_VERSION_DRAFT_22);
    while (p.client.poll_event()) {}
    while (p.server.poll_event()) {}
    auto sr = moq::subscriber::create(p.client);
    MOQ_CHECK(sr.ok());
    if (!sr.ok()) return failures;
    auto sub = std::move(*sr);
    moq::sub_track_config tc;
    tc.ns = moq::namespace_name{"live"};
    tc.track = moq::bytes_view("v");
    moq::sub_track_options to;
    to.object_delivery_timeout_ms = 100;
    to.forward = false;
    auto t = sub.subscribe(tc, to, 1);
    MOQ_CHECK(t.ok());
    settle(p.client, p.server, 2);
    std::optional<moq::subscription> ssub;
    std::optional<uint64_t> asked;
    std::optional<bool> forward;
    MOQ_CHECK((take<moq::extended_event::subscribe_request>(p.server, [&](const auto &e) {
        ssub = e.sub;
        forward = e.forward;
        if (e.options) asked = e.options->object_delivery_timeout_ms;
    })));
    MOQ_CHECK(asked && *asked == 100);
    MOQ_CHECK(forward && !*forward);
    if (ssub && t.ok()) {
        moq::reject_subscribe_config rj;
        rj.error_code = moq::request_error::redirect;
        rj.redirect.connect_uri = "moqt://other/";
        MOQ_CHECK(p.server.reject_subscribe(*ssub, rj, 3).ok());
        settle(p.client, p.server, 4);
        MOQ_CHECK(sub.tick(4).ok());
        auto m = sub.get_track_migration(*t);
        MOQ_CHECK(m.ok() && m->has_value() && (*m)->redirect &&
                  (*m)->connect_uri.size() == std::strlen("moqt://other/"));
    }
    MOQ_PASS("facades");
    return failures;
}

int main()
{
    int failures = 0;
    failures += test_setup_tokens_and_close();
    failures += test_goaway_timeout();
    failures += test_subscribe_tracks();
    failures += test_updates_and_window();
    failures += test_loc_metadata();
    failures += test_facades();
    return failures ? 1 : 0;
}
