/* moq::media_player over a scripted stand-in for the C player, in one TU:
 * this binary defines the moq_media_player_* functions itself and links no
 * service library, so it can drive every record shape and result exactly.
 *
 * Pinned:
 *   - config, select options and feedback reach C through sized init, field
 *     for field; create passes the receiver config, attach the receiver;
 *   - a refused create or attach is its error and destroys nothing; a player
 *     is destroyed exactly once across moves, and move assignment destroys
 *     the player it replaces;
 *   - every polled output -- each kind, unknown and short ones included --
 *     is cleaned up exactly once across moves and move assignment over a
 *     live record, and its refs (and retained buffers taken from it) outlive
 *     the player; the rcbuf allocator balances;
 *   - typed views are lossless: optional capture time, independence (known
 *     true, known false, unknown) and sample duration, the unit's byte
 *     range, video and audio configuration fields, the timeline, END cause
 *     and disposition (unnamed numeric values too), and the decode-time
 *     semantics follow packaging;
 *   - every event kind maps to its alternative with its values (a crossed
 *     gap's hole, a sync report with and without a render group, a RESET
 *     and a RECEIVER_DROP loss), and unknown or short ones stay unknown;
 *   - empty polls are empty optionals, native errors stay errors; wait maps
 *     OK/DONE to ready/idle and keeps its errors; no deadline is empty;
 *   - the destructor sends no feedback.
 */
#include <moq/media_player.hpp>

#include "test_support.hpp"

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

static int failures = 0;

/* -- A counting allocator for the rcbufs the records carry ------------- */

static long g_live_allocs = 0;
static void *a_alloc(size_t n, void *) { ++g_live_allocs; return std::malloc(n); }
static void *a_realloc(void *p, size_t, size_t n, void *)
{
    if (!p) ++g_live_allocs;
    return std::realloc(p, n);
}
static void a_free(void *p, size_t, void *) { if (p) --g_live_allocs; std::free(p); }
static const moq_alloc_t g_alloc = {nullptr, a_alloc, a_realloc, a_free};

static moq_rcbuf_t *mkbuf(const char *s)
{
    moq_rcbuf_t *b = nullptr;
    moq_rcbuf_create(&g_alloc, reinterpret_cast<const uint8_t *>(s), std::strlen(s), &b);
    return b;
}

/* -- The scripted C player ------------------------------------------------- */

struct moq_media_player { int id; };

static moq_media_player g_player_storage[8];
static int g_players_made = 0;
static int g_destroys = 0;
static moq_result_t g_create_rc = MOQ_OK;
static moq_media_player_cfg_t g_last_cfg{};
static const moq_media_receiver_cfg_t *g_last_receiver_cfg = nullptr;
static moq_media_receiver_t *g_last_attach_receiver = nullptr;
static moq_media_player_select_cfg_t g_last_select{};
static moq_media_track_t *g_last_select_track = nullptr;
static moq_media_player_feedback_t g_last_fb{};
static std::string g_last_fb_message;
static int g_feedbacks = 0;
static std::vector<moq_media_player_output_t> g_outputs;
static moq_result_t g_output_tail = MOQ_DONE;
static std::vector<moq_media_player_event_t> g_events;
static moq_result_t g_event_tail = MOQ_DONE;
static int g_cleanups = 0;           /* cleanups of a record holding a kind */
static moq_result_t g_wait_rc = MOQ_OK;
static moq_result_t g_tick_rc = MOQ_OK;
static uint64_t g_deadline = UINT64_MAX;
static size_t g_stats_size = 0;

static moq_media_player_t *make_player()
{
    moq_media_player_t *p = &g_player_storage[g_players_made % 8];
    p->id = ++g_players_made;
    return p;
}

extern "C" {

void moq_media_player_cfg_init_sized(moq_media_player_cfg_t *cfg, size_t n)
{
    if (!cfg || n < sizeof(uint32_t)) return;
    size_t k = n < sizeof(*cfg) ? n : sizeof(*cfg);
    std::memset(cfg, 0, k);
    cfg->struct_size = static_cast<uint32_t>(k);
}
void moq_media_player_select_cfg_init_sized(moq_media_player_select_cfg_t *cfg, size_t n)
{
    size_t k = n < sizeof(*cfg) ? n : sizeof(*cfg);
    std::memset(cfg, 0, k);
    cfg->struct_size = static_cast<uint32_t>(k);
}
void moq_media_player_feedback_init_sized(moq_media_player_feedback_t *fb, size_t n)
{
    size_t k = n < sizeof(*fb) ? n : sizeof(*fb);
    std::memset(fb, 0, k);
    fb->struct_size = static_cast<uint32_t>(k);
}
moq_result_t moq_media_player_create(const moq_media_player_cfg_t *cfg, moq_media_player_t **out)
{
    g_last_cfg = *cfg;
    g_last_receiver_cfg = cfg->receiver;
    if (g_create_rc != MOQ_OK) return g_create_rc;
    *out = make_player();
    return MOQ_OK;
}
moq_result_t moq_media_player_attach(moq_media_receiver_t *r, const moq_media_player_cfg_t *cfg,
                                     moq_media_player_t **out)
{
    g_last_cfg = *cfg;
    g_last_attach_receiver = r;
    if (g_create_rc != MOQ_OK) return g_create_rc;
    *out = make_player();
    return MOQ_OK;
}
void moq_media_player_destroy(moq_media_player_t *p) { if (p) ++g_destroys; }
moq_media_receiver_t *moq_media_player_receiver(const moq_media_player_t *)
{
    return reinterpret_cast<moq_media_receiver_t *>(0x10);
}
moq_result_t moq_media_player_select_track(moq_media_player_t *, moq_media_track_t *t,
                                           const moq_media_player_select_cfg_t *cfg)
{
    g_last_select_track = t;
    g_last_select = *cfg;
    return t ? MOQ_OK : MOQ_ERR_INVAL;
}
moq_result_t moq_media_player_deselect_track(moq_media_player_t *, moq_media_track_t *t)
{
    return t ? MOQ_OK : MOQ_ERR_INVAL;
}
moq_result_t moq_media_player_tick(moq_media_player_t *, uint64_t) { return g_tick_rc; }
moq_result_t moq_media_player_wait(moq_media_player_t *, uint64_t, uint64_t) { return g_wait_rc; }
uint64_t moq_media_player_next_deadline_us(const moq_media_player_t *) { return g_deadline; }
moq_result_t moq_media_player_poll_output(moq_media_player_t *, moq_media_player_output_t *out,
                                          size_t n)
{
    if (n < sizeof(*out)) return MOQ_ERR_ABI_MISMATCH;
    if (g_outputs.empty()) return g_output_tail;
    *out = g_outputs.front();
    g_outputs.erase(g_outputs.begin());
    return MOQ_OK;
}
void moq_media_player_output_cleanup(moq_media_player_output_t *out)
{
    if (!out || out->kind == 0) return;     /* a moved-from or cleared record */
    ++g_cleanups;
    if (out->kind == MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE) {
        moq_rcbuf_decref(out->u.configure.codec);
        moq_rcbuf_decref(out->u.configure.codec_config);
        out->u.configure.codec = out->u.configure.codec_config = nullptr;
    } else if (out->kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT || out->kind == 99) {
        /* kind 99 stands for a kind newer than this header: the library
         * still knows its refs (here, in the unit position). */
        moq_rcbuf_decref(out->u.unit.payload);
        out->u.unit.payload = nullptr;
    }
    out->kind = 0;
}
moq_result_t moq_media_player_poll_event(moq_media_player_t *, moq_media_player_event_t *ev,
                                         size_t n)
{
    if (n < sizeof(*ev)) return MOQ_ERR_ABI_MISMATCH;
    if (g_events.empty()) return g_event_tail;
    *ev = g_events.front();
    g_events.erase(g_events.begin());
    return MOQ_OK;
}
moq_result_t moq_media_player_handle_feedback(moq_media_player_t *,
                                              const moq_media_player_feedback_t *fb, uint64_t)
{
    ++g_feedbacks;
    g_last_fb = *fb;
    if (fb->kind == MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR)
        g_last_fb_message.assign(reinterpret_cast<const char *>(fb->u.decode_error.message.data),
                                 fb->u.decode_error.message.len);
    return fb->track ? MOQ_OK : MOQ_ERR_INVAL;
}
bool moq_media_player_is_closed(const moq_media_player_t *) { return false; }
bool moq_media_player_is_fatal(const moq_media_player_t *) { return true; }
uint64_t moq_media_player_fatal_code(const moq_media_player_t *) { return 42; }
moq_result_t moq_media_player_get_stats(const moq_media_player_t *, moq_media_player_stats_t *out,
                                        size_t n)
{
    g_stats_size = n;
    std::memset(out, 0, n);
    out->struct_size = static_cast<uint32_t>(n);
    out->units_output = 7;
    out->feedback_stale = 3;
    return MOQ_OK;
}

} // extern "C"

/* -- Record builders --------------------------------------------------------- */

static moq_media_track_t *const k_track = reinterpret_cast<moq_media_track_t *>(0x40);

static moq_media_player_output_t base_output(uint32_t kind, uint32_t detail)
{
    moq_media_player_output_t o{};
    o.struct_size = sizeof(o);
    o.kind = kind;
    o.detail_size = detail;
    o.timeline_id = 3;
    o.configuration_generation = 9;
    o.track = k_track;
    return o;
}

static moq_media_player_output_t configure_output(uint32_t packaging, bool with_config)
{
    auto o = base_output(MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE,
                         sizeof(moq_media_player_output_t{}.u.configure));
    o.u.configure.media_type = MOQ_MEDIA_TYPE_VIDEO;
    o.u.configure.packaging = packaging;
    o.u.configure.codec = mkbuf("av01.0.08M.08");
    o.u.configure.codec_config = with_config ? mkbuf("\x81\x08\x0c") : nullptr;
    o.u.configure.timescale = 1000000;
    o.u.configure.width = 1280;
    o.u.configure.height = 720;
    return o;
}

static moq_media_player_output_t unit_output(bool capture, bool known, uint32_t dur,
                                             bool independent = true)
{
    auto o = base_output(MOQ_MEDIA_PLAYER_OUTPUT_UNIT, sizeof(moq_media_player_output_t{}.u.unit));
    auto &u = o.u.unit;
    u.group_id = 4;
    u.object_id = 2;
    u.decode_time_us = 1000;            /* below the previous unit's: B-frame order */
    u.presentation_time_us = 1000;
    u.composition_offset_us = -40;
    u.has_capture_time = capture;
    u.capture_time_us = 1700000000000123ull;
    u.independence_known = known;
    u.independent = known && independent;
    u.present = true;
    u.sample_duration_us = dur;
    u.payload = mkbuf("xxPAYLOADyy");
    u.offset = 2;
    u.len = 7;
    return o;
}

/* -- Rows ------------------------------------------------------------------------ */

static void config_and_lifecycle()
{
    moq_media_receiver_cfg_t rcfg{};
    moq_playback_cfg_t pb{};
    moq::media_player_config cfg;
    cfg.selection = moq::selection_mode::manual;
    cfg.timing = moq::timing_policy::allow_independent;
    cfg.playback = &pb;
    cfg.preferred_lang = "fr";
    cfg.end_tail_wait_us = 11;
    cfg.cushion_us = 12;
    cfg.max_outputs = 13;
    cfg.max_events = 14;
    cfg.max_held_bytes = 15;
    cfg.assume_shared_epoch = true;
    g_destroys = 0;
    {
        auto p = moq::media_player::create(rcfg, cfg);
        MOQ_CHECK(p.ok());
        MOQ_CHECK(g_last_cfg.struct_size == sizeof(moq_media_player_cfg_t));
        MOQ_CHECK(g_last_receiver_cfg == &rcfg);
        MOQ_CHECK(g_last_cfg.selection == MOQ_MEDIA_SELECTION_MANUAL);
        MOQ_CHECK(g_last_cfg.timing_policy == MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT);
        MOQ_CHECK(g_last_cfg.playback == &pb);
        MOQ_CHECK(g_last_cfg.preferred_lang.len == 2 &&
                  std::memcmp(g_last_cfg.preferred_lang.data, "fr", 2) == 0);
        MOQ_CHECK(g_last_cfg.end_tail_wait_us == 11 && g_last_cfg.cushion_us == 12);
        MOQ_CHECK(g_last_cfg.max_outputs == 13 && g_last_cfg.max_events == 14);
        MOQ_CHECK(g_last_cfg.max_held_bytes == 15 && g_last_cfg.assume_shared_epoch);
        MOQ_CHECK(p->receiver() == reinterpret_cast<moq_media_receiver_t *>(0x10));
        MOQ_CHECK(p->is_fatal() && p->fatal_code() == 42 && !p->is_closed());

        moq::media_player a = std::move(*p);         /* move construct */
        auto q = moq::media_player::create(rcfg);
        MOQ_CHECK(q.ok());
        MOQ_CHECK(g_last_cfg.selection == MOQ_MEDIA_SELECTION_AUTO);
        MOQ_CHECK(g_last_cfg.preferred_lang.len == 0);
        a = std::move(*q);                           /* destroys a's player */
        MOQ_CHECK(g_destroys == 1);
    }
    MOQ_CHECK(g_destroys == 2);                      /* a, once; moved-from: none */

    /* Refusals are errors and destroy nothing. */
    g_destroys = 0;
    g_create_rc = MOQ_ERR_INVAL;
    auto bad = moq::media_player::create(rcfg);
    MOQ_CHECK(!bad.ok() && bad.error().code() == moq::errc::invalid);
    g_create_rc = MOQ_ERR_WRONG_STATE;
    auto r = reinterpret_cast<moq_media_receiver_t *>(0x20);
    auto refused = moq::media_player::attach(r);
    MOQ_CHECK(!refused.ok() && refused.error().code() == moq::errc::wrong_state);
    MOQ_CHECK(g_last_attach_receiver == r && g_last_cfg.receiver == nullptr);
    g_create_rc = MOQ_OK;
    MOQ_CHECK(g_destroys == 0);
    MOQ_PASS("media_player_config_and_lifecycle");
}

static void outputs_views_and_ownership()
{
    long base_allocs = g_live_allocs;
    g_cleanups = 0;
    g_outputs.clear();
    g_outputs.push_back(configure_output(MOQ_MEDIA_PACKAGING_RAW, true));
    g_outputs.push_back(configure_output(MOQ_MEDIA_PACKAGING_CMAF, false));
    {
        auto t = base_output(MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE, sizeof(moq_media_timeline_t));
        t.u.timeline = moq_media_timeline_t{MOQ_MEDIA_TIME_DOMAIN_UNIX_US, 3, 5, 1000, 2000, 1.0};
        g_outputs.push_back(t);
    }
    g_outputs.push_back(unit_output(true, true, 40000));
    g_outputs.push_back(unit_output(false, false, 0));
    g_outputs.push_back(unit_output(false, true, 0, false));   /* known, not independent */
    {
        auto a = base_output(MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE,
                             sizeof(moq_media_player_output_t{}.u.configure));
        a.u.configure.media_type = MOQ_MEDIA_TYPE_AUDIO;
        a.u.configure.packaging = MOQ_MEDIA_PACKAGING_RAW;
        a.u.configure.codec = mkbuf("mp4a.40.2");
        a.u.configure.codec_config = mkbuf("\x11\x90");
        a.u.configure.timescale = 48000;
        a.u.configure.samplerate = 48000;
        a.u.configure.channel_count = 2;
        g_outputs.push_back(a);
    }
    {
        auto d = base_output(MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY,
                             sizeof(moq_media_player_output_t{}.u.discontinuity));
        d.u.discontinuity.reason = MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR;
        d.u.discontinuity.decoder_reset_required = true;
        g_outputs.push_back(d);
    }
    {
        auto e = base_output(MOQ_MEDIA_PLAYER_OUTPUT_END, sizeof(moq_media_player_output_t{}.u.end));
        e.u.end.cause = MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK;
        e.u.end.disposition = MOQ_MEDIA_PLAYER_END_COMPLETE;
        g_outputs.push_back(e);
        e.u.end.cause = 77;              /* unnamed values pass through */
        e.u.end.disposition = 9;
        g_outputs.push_back(e);
    }
    {
        auto u = base_output(99, sizeof(moq_media_player_output_t{}.u.unit));   /* newer kind */
        u.u.unit.payload = mkbuf("future");
        g_outputs.push_back(u);
        auto s = unit_output(true, true, 1);
        s.detail_size = 8;               /* too short for a unit: unknown, still cleaned */
        g_outputs.push_back(s);
    }

    auto p = moq::media_player::create(moq_media_receiver_cfg_t{});
    MOQ_CHECK(p.ok());
    std::vector<moq::polled_output> held;
    for (;;) {
        auto r = p->poll_output();
        MOQ_CHECK(r.ok());
        if (!r.ok() || !r->has_value()) break;
        held.push_back(std::move(**r));
    }
    MOQ_CHECK(held.size() == 12);
    if (held.size() != 12) return;

    auto c0 = std::get<moq::output::configure>(held[0].variant());
    MOQ_CHECK(c0.codec_string() == "av01.0.08M.08");
    MOQ_CHECK(c0.has_codec_config && c0.codec_config.size() == 3);
    MOQ_CHECK(c0.width == 1280 && c0.height == 720 && c0.timescale == 1000000);
    MOQ_CHECK(c0.media_type == MOQ_MEDIA_TYPE_VIDEO);
    MOQ_CHECK(!c0.decode_time_is_decode_clock());
    auto c1 = std::get<moq::output::configure>(held[1].variant());
    MOQ_CHECK(!c1.has_codec_config && c1.codec_config.empty());
    MOQ_CHECK(c1.decode_time_is_decode_clock());
    MOQ_CHECK(held[0].track() == k_track && held[0].timeline_id() == 3);
    MOQ_CHECK(held[0].configuration_generation() == 9);

    auto tl = std::get<moq::output::timeline>(held[2].variant());
    MOQ_CHECK(tl.domain == MOQ_MEDIA_TIME_DOMAIN_UNIX_US && tl.timeline_id == 3 &&
              tl.timeline_epoch == 5 && tl.anchor_media_us == 1000 &&
              tl.anchor_local_us == 2000 && tl.rate == 1.0);

    auto u0 = std::get<moq::output::unit>(held[3].variant());
    MOQ_CHECK(u0.group_id == 4 && u0.object_id == 2);
    MOQ_CHECK(u0.decode_time_us == 1000 && u0.presentation_time_us == 1000);
    MOQ_CHECK(u0.composition_offset_us == -40 && u0.present);
    MOQ_CHECK(u0.capture_time_us == 1700000000000123ull);
    MOQ_CHECK(u0.independent == true && u0.sample_duration_us == 40000u);
    MOQ_CHECK(std::string(reinterpret_cast<const char *>(u0.data.data()), u0.data.size()) ==
              "PAYLOAD");
    auto u1 = std::get<moq::output::unit>(held[4].variant());
    MOQ_CHECK(!u1.capture_time_us && !u1.independent && !u1.sample_duration_us);
    auto u2 = std::get<moq::output::unit>(held[5].variant());
    MOQ_CHECK(u2.independent.has_value() && *u2.independent == false);
    auto au = std::get<moq::output::configure>(held[6].variant());
    MOQ_CHECK(au.media_type == MOQ_MEDIA_TYPE_AUDIO && au.codec_string() == "mp4a.40.2");
    MOQ_CHECK(au.samplerate == 48000 && au.channel_count == 2 && au.timescale == 48000);
    MOQ_CHECK(au.has_codec_config && au.codec_config.size() == 2 &&
              au.codec_config[0] == 0x11 && au.codec_config[1] == 0x90);
    MOQ_CHECK(au.width == 0 && au.height == 0);

    auto d = std::get<moq::output::discontinuity>(held[7].variant());
    MOQ_CHECK(d.reason == moq::discontinuity_reason::decode_error && d.decoder_reset_required);
    auto e0 = std::get<moq::output::end>(held[8].variant());
    MOQ_CHECK(e0.cause == moq::end_cause::peer_end_of_track &&
              e0.disposition == moq::end_disposition::complete);
    auto e1 = std::get<moq::output::end>(held[9].variant());
    MOQ_CHECK(static_cast<uint32_t>(e1.cause) == 77 &&
              static_cast<uint32_t>(e1.disposition) == 9);
    MOQ_CHECK(std::get<moq::output::unknown>(held[10].variant()).kind == 99);
    MOQ_CHECK(std::get<moq::output::unknown>(held[11].variant()).kind ==
              MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    MOQ_CHECK(held[11].payload().raw() == nullptr);  /* not exposed when short */

    /* Retained buffers and the records themselves outlive the player. */
    moq::buffer kept = held[3].payload();
    moq::buffer codec = held[0].codec();
    moq::buffer extradata = held[6].codec_config();
    MOQ_CHECK(kept.size() == 11 && codec.size() == 13 && extradata.size() == 2);
    MOQ_CHECK(held[2].payload().raw() == nullptr && held[3].codec().raw() == nullptr);
    *p = moq::media_player::create(moq_media_receiver_cfg_t{}).value();   /* destroy the first */
    MOQ_CHECK(g_cleanups == 0);

    /* Exactly once, across move construction and move assignment over a
     * live record. */
    moq::polled_output moved = std::move(held[0]);
    MOQ_CHECK(g_cleanups == 0);
    moved = std::move(held[1]);                     /* cleans held[0]'s record */
    MOQ_CHECK(g_cleanups == 1);
    held.clear();                                   /* the other ten; moved-from: none */
    MOQ_CHECK(g_cleanups == 11);
    {
        moq::polled_output gone = std::move(moved);
    }
    MOQ_CHECK(g_cleanups == 12);
    /* Retained past every record and the player. */
    MOQ_CHECK(std::memcmp(kept.data(), "xxPAYLOADyy", 11) == 0);
    MOQ_CHECK(extradata.data()[0] == 0x11 && extradata.data()[1] == 0x90);
    kept = moq::buffer();
    codec = moq::buffer();
    extradata = moq::buffer();
    MOQ_CHECK(g_live_allocs == base_allocs);
    MOQ_PASS("media_player_outputs_views_and_ownership");
}

static void empty_errors_and_timing()
{
    auto p = moq::media_player::create(moq_media_receiver_cfg_t{});
    MOQ_CHECK(p.ok());
    g_outputs.clear();
    g_output_tail = MOQ_DONE;
    auto empty = p->poll_output();
    MOQ_CHECK(empty.ok() && !empty->has_value());
    g_output_tail = MOQ_ERR_CLOSED;
    auto closed = p->poll_output();
    MOQ_CHECK(!closed.ok() && closed.error().code() == moq::errc::closed);
    g_output_tail = MOQ_ERR_ABI_MISMATCH;
    auto abi = p->poll_output();
    MOQ_CHECK(!abi.ok() && abi.error().code() == moq::errc::abi_mismatch);
    g_output_tail = MOQ_DONE;

    g_events.clear();
    g_event_tail = MOQ_ERR_CLOSED;
    auto ev_closed = p->poll_event();
    MOQ_CHECK(!ev_closed.ok() && ev_closed.error().code() == moq::errc::closed);
    g_event_tail = MOQ_DONE;
    auto ev_empty = p->poll_event();
    MOQ_CHECK(ev_empty.ok() && !ev_empty->has_value());

    g_wait_rc = MOQ_OK;
    MOQ_CHECK(p->wait(1, 2).value() == moq::wait_status::ready);
    g_wait_rc = MOQ_DONE;
    MOQ_CHECK(p->wait(1, 2).value() == moq::wait_status::idle);
    g_wait_rc = MOQ_ERR_INTERRUPTED;
    MOQ_CHECK(p->wait(1, 2).error().code() == moq::errc::interrupted);
    g_wait_rc = MOQ_ERR_CLOSED;
    MOQ_CHECK(p->wait(1, 2).error().code() == moq::errc::closed);
    g_tick_rc = MOQ_ERR_WOULD_BLOCK;
    MOQ_CHECK(p->tick(5).error().code() == moq::errc::would_block);
    g_tick_rc = MOQ_OK;
    MOQ_CHECK(p->tick(5).ok());
    g_deadline = UINT64_MAX;
    MOQ_CHECK(!p->next_deadline_us());
    g_deadline = 1234;
    MOQ_CHECK(p->next_deadline_us() == 1234u);

    auto st = p->stats();
    MOQ_CHECK(st.ok() && g_stats_size == sizeof(moq_media_player_stats_t));
    MOQ_CHECK(st->struct_size == sizeof(moq_media_player_stats_t) && st->units_output == 7 &&
              st->feedback_stale == 3);

    moq::select_options so;
    so.start = moq::start_mode::next_group;
    so.independent_timing = true;
    MOQ_CHECK(p->select_track(k_track, so).ok());
    MOQ_CHECK(g_last_select_track == k_track);
    MOQ_CHECK(g_last_select.struct_size == sizeof(moq_media_player_select_cfg_t));
    MOQ_CHECK(g_last_select.start == MOQ_MEDIA_START_NEXT_GROUP);
    MOQ_CHECK(g_last_select.independent_timing && !g_last_select.shared_epoch);
    MOQ_CHECK(p->select_track(nullptr).error().code() == moq::errc::invalid);
    MOQ_CHECK(p->deselect_track(nullptr).error().code() == moq::errc::invalid);
    MOQ_PASS("media_player_empty_errors_and_timing");
}

static void events_and_feedback()
{
    g_events.clear();
    auto mk = [](uint32_t kind, uint32_t detail) {
        moq_media_player_event_t e{};
        e.struct_size = sizeof(e);
        e.kind = kind;
        e.detail_size = detail;
        e.track = k_track;
        return e;
    };
    moq_media_player_event_t e;
    g_events.push_back(mk(MOQ_MEDIA_PLAYER_EVENT_READY, 0));                 /* 0 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_GAP, sizeof(e.u.gap));
    e.u.gap = {7, 3, 2, false, 0, 0};
    g_events.push_back(e);                                                   /* 1 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_LOSS, sizeof(e.u.loss));
    e.u.loss.kind = MOQ_MEDIA_STREAM_RESET;
    e.u.loss.group_id = 5;
    e.u.loss.last_group_id = 5;
    e.u.loss.error_code = 0x10;
    g_events.push_back(e);                                                   /* 2 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE, sizeof(e.u.sync));
    e.u.sync.has_render_group = true;
    e.u.sync.render_group = -2;
    e.u.sync.reason = MOQ_MEDIA_PLAYER_SYNC_NO_SHARED_EPOCH;
    g_events.push_back(e);                                                   /* 3 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_STARTED, sizeof(e.u.started));
    e.u.started.timeline_id = 3;
    g_events.push_back(e);                                                   /* 4 */
    g_events.push_back(mk(50, 0));                       /* a newer kind:   5 */
    g_events.push_back(mk(MOQ_MEDIA_PLAYER_EVENT_DROP, 2)); /* short:       6 */
    g_events.push_back(mk(MOQ_MEDIA_PLAYER_EVENT_TRACK_AVAILABLE, 0));       /* 7 */
    g_events.push_back(mk(MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED, 0));         /* 8 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_GAP, sizeof(e.u.gap));
    e.u.gap = {9, 0, 0, true, 5000000, 40000};          /* crossed */
    g_events.push_back(e);                                                   /* 9 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_SKIP, sizeof(e.u.skip));
    e.u.skip = {10, 12};
    g_events.push_back(e);                                                   /* 10 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_SHED, sizeof(e.u.shed));
    e.u.shed = {3, 1};
    g_events.push_back(e);                                                   /* 11 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_DROP, sizeof(e.u.drop));
    e.u.drop = {MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN, 13, 2};
    g_events.push_back(e);                                                   /* 12 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_LOSS, sizeof(e.u.loss));
    e.u.loss.kind = MOQ_MEDIA_STREAM_RECEIVER_DROP;
    e.u.loss.group_id = 14;
    e.u.loss.last_group_id = 16;
    g_events.push_back(e);                                                   /* 13 */
    g_events.push_back(mk(MOQ_MEDIA_PLAYER_EVENT_STALL, 0));                 /* 14 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT, sizeof(e.u.sync));
    e.u.sync.has_render_group = false;
    e.u.sync.render_group = 99;                         /* not meaningful */
    e.u.sync.reason = MOQ_MEDIA_PLAYER_SYNC_MIXED_DOMAINS;
    g_events.push_back(e);                                                   /* 15 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN, sizeof(e.u.knowledge_forgotten));
    e.u.knowledge_forgotten = {2, 6};
    g_events.push_back(e);                                                   /* 16 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_ENDED, 0);
    e.track = nullptr;                                  /* player-wide */
    g_events.push_back(e);                                                   /* 17 */
    e = mk(MOQ_MEDIA_PLAYER_EVENT_CLOSED, 0);
    e.track = nullptr;
    g_events.push_back(e);                                                   /* 18 */

    auto p = moq::media_player::create(moq_media_receiver_cfg_t{});
    std::vector<moq::polled_player_event> evs;
    for (;;) {
        auto r = p->poll_event();
        if (!r.ok() || !r->has_value()) break;
        evs.push_back(**r);
    }
    MOQ_CHECK(evs.size() == 19);
    if (evs.size() != 19) return;
    MOQ_CHECK(std::holds_alternative<moq::player_event::ready>(evs[0].variant()));
    auto g = std::get<moq::player_event::gap>(evs[1].variant());
    MOQ_CHECK(g.group_id == 7 && g.object_id == 3 && g.cause == 2 && !g.crossed);
    auto l = std::get<moq::player_event::loss>(evs[2].variant());
    MOQ_CHECK(l.kind == MOQ_MEDIA_STREAM_RESET && l.group_id == 5 && l.last_group_id == 5 &&
              l.error_code == 0x10);
    auto s = std::get<moq::player_event::sync_unavailable>(evs[3].variant());
    MOQ_CHECK(s.render_group == -2 && s.reason == MOQ_MEDIA_PLAYER_SYNC_NO_SHARED_EPOCH);
    MOQ_CHECK(std::get<moq::player_event::started>(evs[4].variant()).timeline_id == 3);
    MOQ_CHECK(std::get<moq::player_event::unknown>(evs[5].variant()).kind == 50);
    MOQ_CHECK(std::get<moq::player_event::unknown>(evs[6].variant()).kind ==
              MOQ_MEDIA_PLAYER_EVENT_DROP);
    MOQ_CHECK(evs[1].track() == k_track);
    namespace pe = moq::player_event;
    MOQ_CHECK(std::holds_alternative<pe::track_available>(evs[7].variant()));
    MOQ_CHECK(std::holds_alternative<pe::track_removed>(evs[8].variant()));
    auto gx = std::get<pe::gap>(evs[9].variant());
    MOQ_CHECK(gx.crossed && gx.group_id == 9 && gx.cause == 0 &&
              gx.hole_start_us == 5000000 && gx.hole_duration_us == 40000);
    auto sk = std::get<pe::skip>(evs[10].variant());
    MOQ_CHECK(sk.from_group_id == 10 && sk.to_group_id == 12);
    auto sh = std::get<pe::shed>(evs[11].variant());
    MOQ_CHECK(sh.dropped_groups == 3 && sh.remaining_groups == 1);
    auto dr = std::get<pe::drop>(evs[12].variant());
    MOQ_CHECK(dr.reason == MOQ_MEDIA_PLAYER_DROP_TIME_DOMAIN && dr.group_id == 13 &&
              dr.object_id == 2);
    auto rd = std::get<pe::loss>(evs[13].variant());
    MOQ_CHECK(rd.kind == MOQ_MEDIA_STREAM_RECEIVER_DROP && rd.group_id == 14 &&
              rd.last_group_id == 16);
    MOQ_CHECK(std::holds_alternative<pe::stall>(evs[14].variant()));
    auto ti = std::get<pe::timing_independent>(evs[15].variant());
    MOQ_CHECK(!ti.render_group && ti.reason == MOQ_MEDIA_PLAYER_SYNC_MIXED_DOMAINS);
    auto kf = std::get<pe::knowledge_forgotten>(evs[16].variant());
    MOQ_CHECK(kf.first_group_id == 2 && kf.last_group_id == 6);
    MOQ_CHECK(std::holds_alternative<pe::ended>(evs[17].variant()) && !evs[17].track());
    MOQ_CHECK(std::holds_alternative<pe::closed>(evs[18].variant()) && !evs[18].track());

    g_feedbacks = 0;
    auto q = moq::player_feedback::decoder_queue(k_track, 9, 4, 8);
    MOQ_CHECK(p->handle_feedback(q, 100).ok());
    MOQ_CHECK(g_last_fb.struct_size == sizeof(moq_media_player_feedback_t));
    MOQ_CHECK(g_last_fb.kind == MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE);
    MOQ_CHECK(g_last_fb.track == k_track && g_last_fb.configuration_generation == 9);
    MOQ_CHECK(g_last_fb.u.decoder_queue.depth == 4 &&
              g_last_fb.u.decoder_queue.max_recommended == 8);
    std::string msg = "bad slice";
    MOQ_CHECK(p->handle_feedback(moq::player_feedback::decode_error(k_track, 10, msg), 101).ok());
    MOQ_CHECK(g_last_fb.kind == MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR &&
              g_last_fb_message == "bad slice");
    MOQ_CHECK(p->handle_feedback(moq::player_feedback::flush_complete(k_track, 11), 102).ok());
    MOQ_CHECK(g_last_fb.kind == MOQ_MEDIA_PLAYER_FEEDBACK_FLUSH_COMPLETE &&
              g_last_fb.configuration_generation == 11);
    auto bad = p->handle_feedback(moq::player_feedback::flush_complete(nullptr, 1), 103);
    MOQ_CHECK(!bad.ok() && bad.error().code() == moq::errc::invalid);
    int before = g_feedbacks;
    {
        auto gone = moq::media_player::create(moq_media_receiver_cfg_t{});
        (void)gone;
    }
    MOQ_CHECK(g_feedbacks == before);                    /* no destructor feedback */
    MOQ_PASS("media_player_events_and_feedback");
}

int main()
{
    config_and_lifecycle();
    outputs_views_and_ownership();
    empty_errors_and_timing();
    events_and_feedback();
    if (failures) {
        std::fprintf(stderr, "FAIL: %d media_player fake failures\n", failures);
        return 1;
    }
    std::printf("PASS: media_player fake\n");
    return 0;
}
