#ifndef MOQ_MEDIA_PLAYER_HPP
#define MOQ_MEDIA_PLAYER_HPP

/*
 * moq::media_player: a thin, header-only C++ owner over <moq/media_player.h>.
 *
 * The player hands an application decode-order media for its own decoders:
 * per selected track, CONFIGURE, UNIT, DISCONTINUITY, TIMELINE and END
 * outputs, and diagnostics on a separate event queue. The decoder, renderer,
 * audio output and presentation clock stay with the application; this
 * wrapper adds RAII, typed records and result<T>, and nothing else -- no
 * thread, no scheduling, no state of its own. Every C contract holds as
 * written in <moq/media_player.h>: one thread at a time for every call on a
 * player, now_us on the application's monotonic clock, and the receiver kept
 * for observing calls only while a player exists.
 *
 * Opt-in: include this header and link moq::service (which carries the
 * playback engine) next to moq::cpp. <moq/moq.hpp> does not include it, so a
 * core-only moq::cpp consumer never needs the service tier.
 */

#include <moq/media_player.h>
#include <moq/buffer.hpp>
#include <moq/result.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string_view>
#include <variant>

namespace moq {

/* -- Kinds and policies: open sets (unknown numeric values pass through) -- */

enum class selection_mode : uint32_t {
    automatic = MOQ_MEDIA_SELECTION_AUTO,
    manual    = MOQ_MEDIA_SELECTION_MANUAL,
};

enum class timing_policy : uint32_t {
    require_sync      = MOQ_MEDIA_TIMING_REQUIRE_SYNC,
    allow_independent = MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT,
};

enum class start_mode : uint32_t {
    current    = MOQ_MEDIA_START_CURRENT,
    next_group = MOQ_MEDIA_START_NEXT_GROUP,
};

enum class discontinuity_reason : uint32_t {
    gap          = MOQ_MEDIA_PLAYER_DISCONTINUITY_GAP,
    decode_error = MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR,
};

enum class end_cause : uint32_t {
    peer_end_of_track = MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK,
    peer_done         = MOQ_MEDIA_PLAYER_END_PEER_DONE,  // delivery ended, no END_OF_TRACK
    removed           = MOQ_MEDIA_PLAYER_END_REMOVED,
    local_failure     = MOQ_MEDIA_PLAYER_END_LOCAL_FAILURE,
    closed            = MOQ_MEDIA_PLAYER_END_CLOSED,
    fatal             = MOQ_MEDIA_PLAYER_END_FATAL,
};

enum class end_disposition : uint32_t {
    complete    = MOQ_MEDIA_PLAYER_END_COMPLETE,
    tail_cutoff = MOQ_MEDIA_PLAYER_END_TAIL_CUTOFF,
    immediate   = MOQ_MEDIA_PLAYER_END_IMMEDIATE,
};

/* -- Inputs ---------------------------------------------------------------- */

/* Copied into a sized-initialized moq_media_player_cfg_t at create/attach.
 * Zero values select the C defaults. `playback` and `preferred_lang` are
 * borrowed for the call (the C side copies them). */
struct media_player_config {
    selection_mode            selection = selection_mode::automatic;
    timing_policy             timing    = timing_policy::require_sync;
    const moq_playback_cfg_t *playback  = nullptr;
    std::string_view          preferred_lang = {};
    uint64_t                  end_tail_wait_us = 0;   /* 0 = 2 s */
    uint64_t                  cushion_us       = 0;   /* 0 = 100 ms */
    uint32_t                  max_outputs      = 0;   /* 0 = 128 */
    uint32_t                  max_events       = 0;   /* 0 = 64 */
    uint64_t                  max_held_bytes   = 0;   /* 0 = 8 MiB */
    bool                      assume_shared_epoch = false;
};

struct select_options {
    start_mode start              = start_mode::current;
    bool       independent_timing = false;
    bool       shared_epoch       = false;
};

/* Decoder feedback for one track, naming the configuration_generation of the
 * outputs it observed (feedback for an older generation is ignored and
 * counted as stale). Build one with the factories; a decode_error message is
 * borrowed until handle_feedback returns. */
class player_feedback {
public:
    static player_feedback decoder_queue(moq_media_track_t *track, uint64_t generation,
                                         uint32_t depth, uint32_t max_recommended) noexcept
    {
        player_feedback f(MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE, track, generation);
        f.fb_.u.decoder_queue.depth           = depth;
        f.fb_.u.decoder_queue.max_recommended = max_recommended;
        return f;
    }

    static player_feedback decode_error(moq_media_track_t *track, uint64_t generation,
                                        std::string_view message = {}) noexcept
    {
        player_feedback f(MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR, track, generation);
        f.fb_.u.decode_error.message = moq_bytes_t{
            reinterpret_cast<const uint8_t *>(message.data()), message.size()};
        return f;
    }

    static player_feedback flush_complete(moq_media_track_t *track,
                                          uint64_t generation) noexcept
    {
        return player_feedback(MOQ_MEDIA_PLAYER_FEEDBACK_FLUSH_COMPLETE, track, generation);
    }

    const moq_media_player_feedback_t &raw() const noexcept { return fb_; }

private:
    player_feedback(uint32_t kind, moq_media_track_t *track, uint64_t generation) noexcept
    {
        moq_media_player_feedback_init(&fb_);
        fb_.kind                     = kind;
        fb_.track                    = track;
        fb_.configuration_generation = generation;
    }

    moq_media_player_feedback_t fb_{};
};

/* -- Outputs ----------------------------------------------------------------- */

/* The output alternatives are non-owning views into the polled_output they
 * came from: valid while that polled_output lives, and only there. To keep a
 * payload, codec string or configuration longer, take its buffer (a retained
 * reference, which also outlives the player). `track` is an opaque
 * correlation key: comparing it is fine; dereferencing it is not permission
 * to touch a receiver that has been destroyed. */
namespace output {

struct configure {
    uint32_t              media_type;   /* a moq_media_type_t value, kept as is */
    moq_media_packaging_t packaging;
    std::span<const uint8_t> codec;         /* the codec string's bytes */
    std::span<const uint8_t> codec_config;  /* decoder extradata; empty when none */
    bool                  has_codec_config;
    uint32_t              timescale;
    uint32_t              width, height;
    uint32_t              samplerate, channel_count;

    std::string_view codec_string() const noexcept
    {
        return {reinterpret_cast<const char *>(codec.data()), codec.size()};
    }
    /* Only on CMAF is a unit's decode_time_us a decode clock. On RAW/LOC it
     * copies the media time: decode order is output order, and with frame
     * reordering it can decrease from one unit to the next. */
    bool decode_time_is_decode_clock() const noexcept
    {
        return packaging == MOQ_MEDIA_PACKAGING_CMAF;
    }
};

struct unit {
    uint64_t group_id, object_id;
    /* See configure::decode_time_is_decode_clock for what this value is. */
    uint64_t decode_time_us;
    uint64_t presentation_time_us;
    int64_t  composition_offset_us;
    std::optional<uint64_t> capture_time_us;
    std::optional<bool>     independent;      /* empty: not marked */
    bool                    present;
    std::optional<uint32_t> sample_duration_us;
    std::span<const uint8_t> data;            /* the unit's bytes */
};

struct discontinuity {
    discontinuity_reason reason;
    bool                 decoder_reset_required;
};

/* A presentation mapping: a unit of this timeline presents at
 *   anchor_local_us + (presentation_time_us - anchor_media_us) / rate
 * on the application's clock. Carried losslessly. */
struct timeline {
    moq_media_time_domain_t domain;
    uint32_t timeline_id;
    uint64_t timeline_epoch;
    uint64_t anchor_media_us;
    uint64_t anchor_local_us;
    double   rate;
};

struct end {
    end_cause       cause;        /* numeric values beyond the named ones pass through */
    end_disposition disposition;
};

/* A kind this header does not know: its refs are still released exactly
 * once by the polled_output that holds it. */
struct unknown {
    moq_media_player_output_kind_t kind;
};

} // namespace output

using output_variant = std::variant<output::configure, output::unit, output::discontinuity,
                                    output::timeline, output::end, output::unknown>;

/* One polled output record: move-only, and it gives its native refs back
 * exactly once (moq_media_player_output_cleanup) when destroyed or assigned
 * over -- for every kind, unknown ones included. It may outlive the player:
 * a record owns rcbuf refs only. */
class polled_output {
public:
    explicit polled_output(const moq_media_player_output_t &out) noexcept : out_(out) {}

    ~polled_output() { moq_media_player_output_cleanup(&out_); }

    polled_output(polled_output &&o) noexcept : out_(o.out_)
    {
        std::memset(&o.out_, 0, sizeof(o.out_));
    }

    polled_output &operator=(polled_output &&o) noexcept
    {
        if (this != &o) {
            moq_media_player_output_cleanup(&out_);
            out_ = o.out_;
            std::memset(&o.out_, 0, sizeof(o.out_));
        }
        return *this;
    }

    polled_output(const polled_output &)            = delete;
    polled_output &operator=(const polled_output &) = delete;

    moq_media_player_output_kind_t kind() const noexcept { return out_.kind; }
    moq_media_track_t *track() const noexcept { return out_.track; }
    uint32_t timeline_id() const noexcept { return out_.timeline_id; }
    uint64_t configuration_generation() const noexcept { return out_.configuration_generation; }

    /* Views into this record (see namespace output). */
    output_variant variant() const noexcept
    {
        const auto &u = out_.u;
        switch (out_.kind) {
        case MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE:
            if (!covers(sizeof(u.configure))) break;
            return output::configure{
                u.configure.media_type,
                u.configure.packaging,
                span_of(u.configure.codec),
                span_of(u.configure.codec_config),
                u.configure.codec_config != nullptr,
                u.configure.timescale,
                u.configure.width, u.configure.height,
                u.configure.samplerate, u.configure.channel_count};
        case MOQ_MEDIA_PLAYER_OUTPUT_UNIT: {
            if (!covers(sizeof(u.unit))) break;
            const auto &d = u.unit;
            std::span<const uint8_t> bytes;
            if (d.payload && d.offset <= moq_rcbuf_len(d.payload) &&
                d.len <= moq_rcbuf_len(d.payload) - d.offset)
                bytes = {moq_rcbuf_data(d.payload) + d.offset, d.len};
            return output::unit{
                d.group_id, d.object_id,
                d.decode_time_us, d.presentation_time_us, d.composition_offset_us,
                d.has_capture_time ? std::optional<uint64_t>(d.capture_time_us) : std::nullopt,
                d.independence_known ? std::optional<bool>(d.independent) : std::nullopt,
                d.present,
                d.sample_duration_us ? std::optional<uint32_t>(d.sample_duration_us)
                                     : std::nullopt,
                bytes};
        }
        case MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY:
            if (!covers(sizeof(u.discontinuity))) break;
            return output::discontinuity{
                static_cast<discontinuity_reason>(u.discontinuity.reason),
                u.discontinuity.decoder_reset_required};
        case MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE:
            if (!covers(sizeof(u.timeline))) break;
            return output::timeline{
                u.timeline.domain, u.timeline.timeline_id, u.timeline.timeline_epoch,
                u.timeline.anchor_media_us, u.timeline.anchor_local_us, u.timeline.rate};
        case MOQ_MEDIA_PLAYER_OUTPUT_END:
            if (!covers(sizeof(u.end))) break;
            return output::end{static_cast<end_cause>(u.end.cause),
                               static_cast<end_disposition>(u.end.disposition)};
        default:
            break;
        }
        return output::unknown{out_.kind};
    }

    /* Retained references that outlive this record (and the player); empty
     * buffers when the record carries none. */
    buffer payload() const noexcept
    {
        return out_.kind == MOQ_MEDIA_PLAYER_OUTPUT_UNIT && covers(sizeof(out_.u.unit))
                   ? buffer::retain(out_.u.unit.payload) : buffer();
    }
    buffer codec() const noexcept
    {
        return out_.kind == MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE && covers(sizeof(out_.u.configure))
                   ? buffer::retain(out_.u.configure.codec) : buffer();
    }
    buffer codec_config() const noexcept
    {
        return out_.kind == MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE && covers(sizeof(out_.u.configure))
                   ? buffer::retain(out_.u.configure.codec_config) : buffer();
    }

    const moq_media_player_output_t &raw() const noexcept { return out_; }

private:
    bool covers(size_t member_size) const noexcept { return out_.detail_size >= member_size; }

    static std::span<const uint8_t> span_of(const moq_rcbuf_t *b) noexcept
    {
        if (!b) return {};
        return {moq_rcbuf_data(b), moq_rcbuf_len(b)};
    }

    moq_media_player_output_t out_{};
};

/* -- Events: values (an event owns nothing) ---------------------------------- */

namespace player_event {

struct track_available {};
struct track_removed {};
struct ready {};
struct started { uint32_t timeline_id; };
struct gap {
    uint64_t group_id, object_id;     /* the missing unit */
    uint32_t cause;                   /* moq_playback_gap_cause_t; 0 when crossed */
    bool     crossed;
    uint64_t hole_start_us, hole_duration_us;
};
struct skip { uint64_t from_group_id, to_group_id; };
struct shed { uint32_t dropped_groups, remaining_groups; };
struct drop { uint32_t reason; uint64_t group_id, object_id; };
struct loss {
    moq_media_stream_terminal_kind_t kind;
    uint64_t group_id, last_group_id, error_code;
};
struct stall {};
struct ended {};
struct closed {};
struct sync_report {
    std::optional<int32_t> render_group;
    uint32_t               reason;    /* MOQ_MEDIA_PLAYER_SYNC_* */
};
struct timing_independent : sync_report {};
struct sync_unavailable : sync_report {};
struct knowledge_forgotten { uint64_t first_group_id, last_group_id; };
struct unknown { moq_media_player_event_kind_t kind; };

} // namespace player_event

using player_event_variant = std::variant<
    player_event::track_available, player_event::track_removed, player_event::ready,
    player_event::started, player_event::gap, player_event::skip, player_event::shed,
    player_event::drop, player_event::loss, player_event::stall, player_event::ended,
    player_event::closed, player_event::timing_independent, player_event::sync_unavailable,
    player_event::knowledge_forgotten, player_event::unknown>;

/* One polled event, copied whole: copyable, and it holds no native refs.
 * `track` (NULL for player-wide events) is an opaque correlation key, as on
 * outputs. */
class polled_player_event {
public:
    explicit polled_player_event(const moq_media_player_event_t &ev) noexcept : ev_(ev) {}

    moq_media_player_event_kind_t kind() const noexcept { return ev_.kind; }
    moq_media_track_t *track() const noexcept { return ev_.track; }

    player_event_variant variant() const noexcept
    {
        namespace pe = player_event;
        const auto &u = ev_.u;
        switch (ev_.kind) {
        case MOQ_MEDIA_PLAYER_EVENT_TRACK_AVAILABLE: return pe::track_available{};
        case MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED:   return pe::track_removed{};
        case MOQ_MEDIA_PLAYER_EVENT_READY:           return pe::ready{};
        case MOQ_MEDIA_PLAYER_EVENT_STALL:           return pe::stall{};
        case MOQ_MEDIA_PLAYER_EVENT_ENDED:           return pe::ended{};
        case MOQ_MEDIA_PLAYER_EVENT_CLOSED:          return pe::closed{};
        case MOQ_MEDIA_PLAYER_EVENT_STARTED:
            if (!covers(sizeof(u.started))) break;
            return pe::started{u.started.timeline_id};
        case MOQ_MEDIA_PLAYER_EVENT_GAP:
            if (!covers(sizeof(u.gap))) break;
            return pe::gap{u.gap.group_id, u.gap.object_id, u.gap.cause, u.gap.crossed,
                           u.gap.hole_start_us, u.gap.hole_duration_us};
        case MOQ_MEDIA_PLAYER_EVENT_SKIP:
            if (!covers(sizeof(u.skip))) break;
            return pe::skip{u.skip.from_group_id, u.skip.to_group_id};
        case MOQ_MEDIA_PLAYER_EVENT_SHED:
            if (!covers(sizeof(u.shed))) break;
            return pe::shed{u.shed.dropped_groups, u.shed.remaining_groups};
        case MOQ_MEDIA_PLAYER_EVENT_DROP:
            if (!covers(sizeof(u.drop))) break;
            return pe::drop{u.drop.reason, u.drop.group_id, u.drop.object_id};
        case MOQ_MEDIA_PLAYER_EVENT_LOSS:
            if (!covers(sizeof(u.loss))) break;
            return pe::loss{u.loss.kind, u.loss.group_id, u.loss.last_group_id,
                            u.loss.error_code};
        case MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT:
            if (!covers(sizeof(u.sync))) break;
            return pe::timing_independent{sync()};
        case MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE:
            if (!covers(sizeof(u.sync))) break;
            return pe::sync_unavailable{sync()};
        case MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN:
            if (!covers(sizeof(u.knowledge_forgotten))) break;
            return pe::knowledge_forgotten{u.knowledge_forgotten.first_group_id,
                                           u.knowledge_forgotten.last_group_id};
        default:
            break;
        }
        return pe::unknown{ev_.kind};
    }

    const moq_media_player_event_t &raw() const noexcept { return ev_; }

private:
    bool covers(size_t member_size) const noexcept { return ev_.detail_size >= member_size; }

    player_event::sync_report sync() const noexcept
    {
        const auto &s = ev_.u.sync;
        return {s.has_render_group ? std::optional<int32_t>(s.render_group) : std::nullopt,
                s.reason};
    }

    moq_media_player_event_t ev_{};
};

/* -- The player ---------------------------------------------------------------- */

/* wait(): ready -- something to do now (poll, or tick); idle -- the timeout
 * elapsed, or the only work left is paused by DECODER_QUEUE feedback (wait on
 * the decoder, not on the player). */
enum class wait_status { ready, idle };

class media_player {
public:
    /* A player that creates and owns its receiver from `receiver` (borrowed
     * for the call). Destroying the player destroys that receiver. */
    static result<media_player> create(const moq_media_receiver_cfg_t &receiver,
                                       const media_player_config &cfg = {})
    {
        moq_media_player_cfg_t c = to_c(cfg);
        c.receiver = &receiver;
        moq_media_player_t *p = nullptr;
        moq_result_t rc = moq_media_player_create(&c, &p);
        if (rc < 0) return errc_from_result(rc);
        return media_player(p);
    }

    /* A player attached to an existing receiver: it becomes the receiver's
     * sole consumer. The receiver must outlive the player and is only
     * observed meanwhile; destroying the player returns its queues. A
     * refused attach leaves the receiver untouched. */
    static result<media_player> attach(moq_media_receiver_t *receiver,
                                       const media_player_config &cfg = {})
    {
        moq_media_player_cfg_t c = to_c(cfg);
        moq_media_player_t *p = nullptr;
        moq_result_t rc = moq_media_player_attach(receiver, &c, &p);
        if (rc < 0) return errc_from_result(rc);
        return media_player(p);
    }

    media_player(media_player &&o) noexcept : p_(o.p_) { o.p_ = nullptr; }
    media_player &operator=(media_player &&o) noexcept
    {
        if (this != &o) {
            moq_media_player_destroy(p_);
            p_   = o.p_;
            o.p_ = nullptr;
        }
        return *this;
    }
    media_player(const media_player &)            = delete;
    media_player &operator=(const media_player &) = delete;

    /* Unsubscribes the selected tracks and releases what the player holds;
     * sends no feedback. Polled outputs stay valid. */
    ~media_player() { moq_media_player_destroy(p_); }

    /* The receiver, for observing calls only (never poll it). */
    moq_media_receiver_t *receiver() const noexcept { return moq_media_player_receiver(p_); }

    result<void> select_track(moq_media_track_t *track, const select_options &opt = {})
    {
        moq_media_player_select_cfg_t c;
        moq_media_player_select_cfg_init(&c);
        c.start              = static_cast<uint32_t>(opt.start);
        c.independent_timing = opt.independent_timing;
        c.shared_epoch       = opt.shared_epoch;
        return result_from_rc(moq_media_player_select_track(p_, track, &c));
    }

    result<void> deselect_track(moq_media_track_t *track)
    {
        return result_from_rc(moq_media_player_deselect_track(p_, track));
    }

    /* errc::would_block: the output queue is full and the engine holds more
     * (poll outputs and tick again; work done before stands). */
    result<void> tick(uint64_t now_us)
    {
        return result_from_rc(moq_media_player_tick(p_, now_us));
    }

    /* Errors: errc::interrupted, errc::closed, as the C wait. */
    result<wait_status> wait(uint64_t now_us, uint64_t timeout_us)
    {
        moq_result_t rc = moq_media_player_wait(p_, now_us, timeout_us);
        if (rc < 0) return errc_from_result(rc);
        return rc == MOQ_OK ? wait_status::ready : wait_status::idle;
    }

    /* The earliest now_us at which tick has time-driven work; empty when
     * there is none. */
    std::optional<uint64_t> next_deadline_us() const noexcept
    {
        uint64_t d = moq_media_player_next_deadline_us(p_);
        if (d == UINT64_MAX) return std::nullopt;
        return d;
    }

    /* Empty when none is queued; errc::closed once none is queued and the
     * player is closed; other native errors stay errors. */
    result<std::optional<polled_output>> poll_output()
    {
        moq_media_player_output_t out{};
        moq_result_t rc = moq_media_player_poll_output(p_, &out, sizeof(out));
        if (rc == MOQ_DONE) return std::optional<polled_output>{};
        if (rc < 0) return errc_from_result(rc);
        return std::optional<polled_output>{std::in_place, out};
    }

    result<std::optional<polled_player_event>> poll_event()
    {
        moq_media_player_event_t ev{};
        moq_result_t rc = moq_media_player_poll_event(p_, &ev, sizeof(ev));
        if (rc == MOQ_DONE) return std::optional<polled_player_event>{};
        if (rc < 0) return errc_from_result(rc);
        return std::optional<polled_player_event>{std::in_place, ev};
    }

    result<void> handle_feedback(const player_feedback &fb, uint64_t now_us)
    {
        return result_from_rc(moq_media_player_handle_feedback(p_, &fb.raw(), now_us));
    }

    bool     is_closed() const noexcept { return moq_media_player_is_closed(p_); }
    bool     is_fatal() const noexcept { return moq_media_player_is_fatal(p_); }
    uint64_t fatal_code() const noexcept { return moq_media_player_fatal_code(p_); }

    result<moq_media_player_stats_t> stats() const
    {
        moq_media_player_stats_t s{};
        moq_result_t rc = moq_media_player_get_stats(p_, &s, sizeof(s));
        if (rc < 0) return errc_from_result(rc);
        return s;
    }

    moq_media_player_t *raw() const noexcept { return p_; }

private:
    explicit media_player(moq_media_player_t *p) noexcept : p_(p) {}

    static moq_media_player_cfg_t to_c(const media_player_config &cfg) noexcept
    {
        moq_media_player_cfg_t c;
        moq_media_player_cfg_init(&c);
        c.selection        = static_cast<uint32_t>(cfg.selection);
        c.timing_policy    = static_cast<uint32_t>(cfg.timing);
        c.playback         = cfg.playback;
        c.preferred_lang   = moq_bytes_t{
            reinterpret_cast<const uint8_t *>(cfg.preferred_lang.data()),
            cfg.preferred_lang.size()};
        c.end_tail_wait_us = cfg.end_tail_wait_us;
        c.cushion_us       = cfg.cushion_us;
        c.max_outputs      = cfg.max_outputs;
        c.max_events       = cfg.max_events;
        c.max_held_bytes   = cfg.max_held_bytes;
        c.assume_shared_epoch = cfg.assume_shared_epoch;
        return c;
    }

    moq_media_player_t *p_ = nullptr;
};

} // namespace moq

#endif // MOQ_MEDIA_PLAYER_HPP
