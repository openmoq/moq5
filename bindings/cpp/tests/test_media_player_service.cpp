/* moq::media_player over the real public service path: a player that owns
 * its receiver plays a finite RAW/LOC video track from a picoquic loopback
 * server that publishes a catalog, six LOC objects in two groups and the
 * track's end. The loop is the application's: tick, poll outputs and
 * events, report the decoder queue, until END.
 *
 * Pinned: CONFIGURE (codec av01, RAW: decode time is not a decode clock)
 * precedes TIMELINE, which precedes the units; the six units arrive in
 * location order with their exact bytes and timestamps; END is the peer's
 * end of track, COMPLETE; READY, STARTED and ENDED are reported; the outputs
 * kept past the player's destruction stay readable; a SHARED_EPOCH receiver
 * is refused at create, and attaching to an auto-subscribing receiver is
 * refused without touching it; a borrowed attach succeeds, and destroying
 * that player leaves the application's receiver usable. The publishing
 * peer's facade is released in the lane pump that sees its session close,
 * while the adapter still holds that session (picoquic_threaded.h's
 * terminal-connection window); only a session still open at shutdown has
 * it released between the adapter's stop and destroy. Its allocator
 * balances. */
#include <moq/media_player.hpp>
#include <moq/endpoint.h>
#include <moq/loc.h>
#include <moq/msf.h>
#include <moq/picoquic_threaded.h>
#include <moq/publisher.h>

#include "test_support.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unistd.h>
#include <vector>

static int failures = 0;

/* The peer publisher's allocator: every block it takes is returned by
 * moq_pub_destroy. Allocated on the network thread, freed on this one. */
static std::atomic<long> g_pub_live{0};
static void *pub_alloc(size_t n, void *) { ++g_pub_live; return std::malloc(n); }
static void *pub_realloc(void *p, size_t, size_t n, void *)
{
    if (!p) ++g_pub_live;
    return std::realloc(p, n);
}
static void pub_free(void *p, size_t, void *) { if (p) --g_pub_live; std::free(p); }
static const moq_alloc_t g_pub_alloc = {nullptr, pub_alloc, pub_realloc, pub_free};

static const char k_catalog[] =
    "{\"version\":1,\"generatedAt\":1,\"tracks\":["
    "{\"name\":\"v\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"av01\",\"timescale\":1000000,"
    "\"width\":640,\"height\":360}]}";

enum { OBJECTS = 6, PER_GROUP = 3, PAYLOAD = 16 };

/* -- The publishing peer (network thread) -------------------------------- */

struct server_state {
    moq_publisher_t *pub = nullptr;
    moq_session_t   *pub_session = nullptr;   /* the session pub was built on */
    bool             pub_done = false;        /* published once; never again */
    moq_pub_track_t *catalog = nullptr;
    moq_pub_track_t *media = nullptr;
    int published = 0;
    bool ended = false;
    std::atomic<bool> failed{false};
};

static bool publish_media(server_state *st, uint64_t now_us)
{
    if (!moq_pub_has_subscriber(st->pub, st->media)) return true;
    while (st->published < OBJECTS) {
        int i = st->published;
        moq_loc_headers_t h;
        moq_loc_headers_init(&h);
        h.has_timestamp = true;
        h.timestamp = 1000000u + static_cast<uint64_t>(i) * 33333u;
        h.has_video_frame_marking = true;
        h.video_frame_marking.independent = (i % PER_GROUP) == 0;
        moq_rcbuf_t *props = nullptr;
        if (moq_loc_encode(moq_alloc_default(), MOQ_VERSION_DRAFT_16, MOQ_LOC_PROFILE_01, &h,
                           &props) != MOQ_OK)
            return false;
        uint8_t bytes[PAYLOAD];
        std::memset(bytes, i + 1, sizeof(bytes));
        moq_rcbuf_t *payload = nullptr;
        if (moq_rcbuf_create(moq_alloc_default(), bytes, sizeof(bytes), &payload) < 0) {
            moq_rcbuf_decref(props);
            return false;
        }
        moq_pub_object_cfg_t o;
        moq_pub_object_cfg_init_sized(&o, sizeof(o));
        o.group_id = static_cast<uint64_t>(i / PER_GROUP);
        o.object_id = static_cast<uint64_t>(i % PER_GROUP);
        o.payload = payload;
        o.properties = props;
        o.end_of_group = true;
        moq_result_t rc = moq_pub_write_object_ex(st->pub, st->media, &o, now_us);
        moq_rcbuf_decref(payload);
        moq_rcbuf_decref(props);
        if (rc == MOQ_ERR_WOULD_BLOCK || rc == MOQ_ERR_WRONG_STATE) return true;
        if (rc != MOQ_OK) return false;
        st->published++;
    }
    if (!st->ended) {
        moq_result_t rc = moq_pub_end_track(st->pub, st->media, now_us);
        if (rc == MOQ_OK) st->ended = true;
        else if (rc != MOQ_ERR_WOULD_BLOCK && rc != MOQ_ERR_WRONG_STATE) return false;
    }
    return true;
}

/* Release the publisher with its session still alive: a connection whose
 * peer closed stays in lane iteration for one more pump so its facades can
 * go before the adapter destroys the session (picoquic_threaded.h). */
static void release_publisher(server_state *st)
{
    moq_pub_destroy(st->pub);
    st->pub = nullptr;
    st->pub_session = nullptr;
}

static int server_pump(moq_pq_threaded_t *, moq_pq_threaded_lane_t *lane, uint64_t now_us,
                       void *ctx)
{
    auto *st = static_cast<server_state *>(ctx);
    moq_session_t *session = nullptr;
    bool pub_session_live = false;
    for (moq_pq_threaded_conn_t *c = moq_pq_threaded_lane_next_conn(lane, nullptr); c;
         c = moq_pq_threaded_lane_next_conn(lane, c)) {
        moq_session_t *cs = moq_pq_threaded_conn_session(c);
        if (cs && cs == st->pub_session) pub_session_live = true;
        if (cs && !session && moq_session_state(cs) != MOQ_SESS_CLOSED) session = cs;
    }
    if (st->pub && (!pub_session_live || moq_session_state(st->pub_session) == MOQ_SESS_CLOSED)) {
        if (pub_session_live) {
            release_publisher(st);
        } else {
            /* Its session vanished without a pump that saw it: the contract
             * above was not met, and destroying pub now would touch it. */
            st->pub = nullptr;
            st->pub_session = nullptr;
            st->failed = true;
        }
        return 0;
    }
    if (st->pub_done || !session) {      /* finished: keep flushing what is owed */
        if (st->pub) (void)moq_pub_tick(st->pub, now_us);
        return 0;
    }
    if (!st->pub) {
        moq_pub_cfg_t pcfg;
        moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        if (moq_pub_create(session, &g_pub_alloc, &pcfg, &st->pub) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        st->pub_session = session;
        moq_bytes_t parts[] = {MOQ_BYTES_LITERAL("cpp"), MOQ_BYTES_LITERAL("player")};
        moq_pub_track_cfg_t tc;
        moq_pub_track_cfg_init(&tc);
        tc.track_namespace = moq_namespace_t{parts, 2};
        tc.track_name = MOQ_BYTES_LITERAL(MOQ_MSF_CATALOG_TRACK_NAME);
        moq_pub_track_cfg_t mc = tc;
        mc.track_name = MOQ_BYTES_LITERAL("v");
        if (moq_pub_add_track(st->pub, &tc, now_us, &st->catalog) != MOQ_OK ||
            moq_pub_add_track(st->pub, &mc, now_us, &st->media) != MOQ_OK) {
            st->failed = true;
            return -1;
        }
        moq_rcbuf_t *payload = nullptr;
        if (moq_rcbuf_create(moq_alloc_default(), reinterpret_cast<const uint8_t *>(k_catalog),
                             std::strlen(k_catalog), &payload) < 0) {
            st->failed = true;
            return -1;
        }
        moq_pub_retained_object_t robj{};
        robj.object_id = 0;
        robj.payload = payload;
        moq_pub_retained_group_cfg_t rg;
        moq_pub_retained_group_cfg_init(&rg);
        rg.group_id = 0;
        rg.objects = &robj;
        rg.object_count = 1;
        moq_result_t rc = moq_pub_set_retained_group(st->pub, st->catalog, &rg);
        moq_rcbuf_decref(payload);
        if (rc != MOQ_OK) {
            st->failed = true;
            return -1;
        }
    }
    (void)moq_pub_tick(st->pub, now_us);
    if (!publish_media(st, now_us)) {
        st->failed = true;
        return -1;
    }
    if (st->ended) st->pub_done = true;
    return 0;
}

static moq_pq_threaded_t *start_server(const char *cert, const char *key, server_state *st,
                                       int *out_port)
{
    int base = 19300 + static_cast<int>(getpid() % 991);
    for (int attempt = 0; attempt < 8; attempt++) {
        int port = base + attempt * 17;
        moq_pq_threaded_cfg_t cfg;
        moq_pq_threaded_cfg_init(&cfg);
        cfg.alloc = moq_alloc_default();
        cfg.perspective = MOQ_PERSPECTIVE_SERVER;
        cfg.cert_path = cert;
        cfg.key_path = key;
        cfg.port = port;
        cfg.send_request_capacity = true;
        cfg.initial_request_capacity = 16;
        cfg.on_lane_pump = server_pump;
        cfg.on_lane_pump_ctx = st;
        moq_pq_threaded_t *srv = nullptr;
        if (moq_pq_threaded_create(&cfg, &srv) == MOQ_OK) {
            *out_port = port;
            return srv;
        }
    }
    return nullptr;
}

/* -- The application ---------------------------------------------------- */

static uint64_t now_us()
{
    using namespace std::chrono;
    return static_cast<uint64_t>(
        duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count());
}

struct receiver_setup {
    char url[64];
    moq_bytes_t parts[2];
    moq_endpoint_cfg_t ec;
    moq_media_receiver_cfg_t rc;

    explicit receiver_setup(int port)
    {
        std::snprintf(url, sizeof(url), "moqt://127.0.0.1:%d", port);
        moq_endpoint_cfg_init(&ec);
        ec.url = moq_bytes_t{reinterpret_cast<const uint8_t *>(url), std::strlen(url)};
        ec.insecure_skip_verify = true;
        moq_media_receiver_cfg_init_live_sized(&rc, sizeof(rc));
        parts[0] = MOQ_BYTES_LITERAL("cpp");
        parts[1] = MOQ_BYTES_LITERAL("player");
        rc.namespace_ = moq_namespace_t{parts, 2};
        rc.endpoint = &ec;
    }
};

/* Refusals need receivers, not a peer: they use a port nothing listens on,
 * so the publishing server keeps its one session for the player below. */
static void refusals(int port)
{
    receiver_setup shared(port);
    shared.rc.time_mode = MOQ_MEDIA_TIME_SHARED_EPOCH;
    auto refused = moq::media_player::create(shared.rc);
    MOQ_CHECK(!refused.ok() && refused.error().code() == moq::errc::unsupported);

    receiver_setup subscribing(port);
    subscribing.rc.auto_subscribe = true;
    moq_media_receiver_t *r = nullptr;
    MOQ_CHECK(moq_media_receiver_create(&subscribing.rc, &r) == MOQ_OK);
    if (r) {
        auto attached = moq::media_player::attach(r);
        MOQ_CHECK(!attached.ok() && attached.error().code() == moq::errc::wrong_state);
        /* Untouched: its queues are still the application's to poll. */
        moq_media_track_event_t ev{};
        moq_result_t prc = moq_media_receiver_poll_track(r, &ev, sizeof(ev));
        MOQ_CHECK(prc == MOQ_DONE || prc == MOQ_OK || prc == MOQ_ERR_CLOSED);
        moq_media_receiver_destroy(r);
    }
    MOQ_PASS("media_player_service_refusals");
}

/* A borrowed attach: the player is the receiver's sole consumer while it
 * lives; destroying it returns the queues, and the receiver -- still the
 * application's -- is observed, polled and destroyed afterwards. */
static void borrowed_attach(int port)
{
    receiver_setup b(port);
    b.rc.auto_subscribe = false;
    b.rc.surface_stream_terminals = true;
    moq_media_receiver_t *r = nullptr;
    MOQ_CHECK(moq_media_receiver_create(&b.rc, &r) == MOQ_OK);
    if (!r) return;
    {
        auto attached = moq::media_player::attach(r);
        MOQ_CHECK(attached.ok());
        if (attached.ok()) {
            MOQ_CHECK(attached->receiver() == r);
            auto t = attached->tick(now_us());
            MOQ_CHECK(t.ok() || t.error().code() == moq::errc::closed);
            auto st = attached->stats();
            MOQ_CHECK(st.ok() && st->tracks_selected == 0);
        }
    }                                                /* the player, not r */
    moq_media_receiver_stats_t rs{};
    MOQ_CHECK(moq_media_receiver_get_stats(r, &rs, sizeof(rs)) == MOQ_OK);
    moq_media_track_event_t ev{};
    moq_result_t prc = moq_media_receiver_poll_track(r, &ev, sizeof(ev));
    MOQ_CHECK(prc == MOQ_DONE || prc == MOQ_OK || prc == MOQ_ERR_CLOSED);
    moq_media_receiver_destroy(r);
    MOQ_PASS("media_player_service_borrowed_attach");
}

static void plays_to_end(int port)
{
    receiver_setup setup(port);
    auto created = moq::media_player::create(setup.rc);
    MOQ_CHECK(created.ok());
    if (!created.ok()) return;
    std::optional<moq::media_player> player(std::move(*created));

    std::vector<moq::polled_output> kept;
    std::vector<std::string> order;
    bool ready = false, started = false, ended = false, end_seen = false;
    moq::output::end end_value{};
    uint32_t units = 0;
    uint64_t deadline = now_us() + 20000000u;
    while (!end_seen && now_us() < deadline) {
        uint64_t now = now_us();
        auto t = player->tick(now);
        MOQ_CHECK(t.ok() || t.error().code() == moq::errc::would_block);
        for (;;) {
            auto ev = player->poll_event();
            MOQ_CHECK(ev.ok());
            if (!ev.ok() || !ev->has_value()) break;
            auto v = (**ev).variant();
            ready |= std::holds_alternative<moq::player_event::ready>(v);
            started |= std::holds_alternative<moq::player_event::started>(v);
            ended |= std::holds_alternative<moq::player_event::ended>(v);
        }
        for (;;) {
            auto out = player->poll_output();
            MOQ_CHECK(out.ok());
            if (!out.ok() || !out->has_value()) break;
            moq::polled_output rec = std::move(**out);
            auto v = rec.variant();
            if (auto *c = std::get_if<moq::output::configure>(&v)) {
                order.push_back("configure");
                MOQ_CHECK(c->codec_string() == "av01");
                MOQ_CHECK(c->packaging == MOQ_MEDIA_PACKAGING_RAW);
                MOQ_CHECK(!c->decode_time_is_decode_clock());
            } else if (std::holds_alternative<moq::output::timeline>(v)) {
                order.push_back("timeline");
            } else if (auto *u = std::get_if<moq::output::unit>(&v)) {
                order.push_back("unit");
                MOQ_CHECK(u->group_id == units / PER_GROUP && u->object_id == units % PER_GROUP);
                MOQ_CHECK(u->presentation_time_us == 1000000u + units * 33333u);
                MOQ_CHECK(u->data.size() == PAYLOAD && u->data[0] == units + 1);
                ++units;
                /* The application's decoder took it: one in its queue. */
                auto fb = moq::player_feedback::decoder_queue(
                    rec.track(), rec.configuration_generation(), 1, 4);
                MOQ_CHECK(player->handle_feedback(fb, now).ok());
                kept.push_back(std::move(rec));
            } else if (auto *e = std::get_if<moq::output::end>(&v)) {
                order.push_back("end");
                end_value = *e;
                end_seen = true;
            }
        }
        if (!end_seen) {
            auto w = player->wait(now_us(), 20000);
            MOQ_CHECK(w.ok());
        }
    }
    for (int i = 0; i < 8 && !ended; i++) {     /* ENDED follows the last END */
        (void)player->tick(now_us());
        auto ev = player->poll_event();
        if (ev.ok() && ev->has_value())
            ended |= std::holds_alternative<moq::player_event::ended>((**ev).variant());
        else
            (void)player->wait(now_us(), 20000);
    }

    MOQ_CHECK(end_seen && units == OBJECTS);
    MOQ_CHECK(end_value.cause == moq::end_cause::peer_end_of_track);
    MOQ_CHECK(end_value.disposition == moq::end_disposition::complete);
    MOQ_CHECK(ready && started && ended);
    MOQ_CHECK(order.size() >= 3 && order[0] == "configure" && order[1] == "timeline" &&
              order[2] == "unit");
    auto st = player->stats();
    MOQ_CHECK(st.ok() && st->units_output == OBJECTS && st->ends_output == 1);

    player.reset();                                  /* the player and its receiver */
    MOQ_CHECK(kept.size() == OBJECTS);
    for (size_t i = 0; i < kept.size(); i++) {
        auto u = std::get<moq::output::unit>(kept[i].variant());
        MOQ_CHECK(u.data.size() == PAYLOAD && u.data[PAYLOAD - 1] == i + 1);
    }
    MOQ_PASS("media_player_service_plays_to_end");
}

int main(int argc, char **argv)
{
    const char *cert = nullptr, *key = nullptr;
    for (int i = 1; i < argc; i++) {
        if (!std::strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
        else if (!std::strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
    }
    if (!cert || !key) {
        std::fprintf(stderr, "usage: --cert <pem> --key <pem>\n");
        return 2;
    }
    server_state st;
    int port = 0;
    moq_pq_threaded_t *srv = start_server(cert, key, &st, &port);
    MOQ_CHECK(srv != nullptr);
    if (!srv) return 1;
    refusals(1);
    borrowed_attach(1);
    plays_to_end(port);
    MOQ_CHECK(!st.failed);
    /* The publisher normally went in the pump that saw its session close;
     * if the connection outlived the player, it goes here, between stop and
     * destroy, while the session is still the adapter's. */
    MOQ_CHECK(moq_pq_threaded_stop(srv) == MOQ_OK);
    if (st.pub) release_publisher(&st);
    moq_pq_threaded_destroy(srv);
    MOQ_CHECK(!st.failed);
    MOQ_CHECK(g_pub_live == 0);
    if (failures) {
        std::fprintf(stderr, "FAIL: %d media_player service failures\n", failures);
        return 1;
    }
    std::printf("PASS: media_player service\n");
    return 0;
}
