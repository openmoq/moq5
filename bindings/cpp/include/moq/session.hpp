#ifndef MOQ_SESSION_HPP
#define MOQ_SESSION_HPP

#include <moq/action.hpp>
#include <moq/event.hpp>
#include <moq/operations.hpp>
#include <moq/result.hpp>

#include <array>
#include <concepts>
#include <cstddef>
#include <functional>
#include <optional>
#include <utility>
#include <span>
#include <vector>

namespace moq {

namespace detail {

/*
 * Owns EVERY raw record of a batched poll while armed. Each record is moved out
 * with std::exchange as it is adopted, so a visited slot is left value-
 * initialised and owns nothing. On the normal path release() disarms the guard,
 * which also avoids N cleanup calls over empty records; if the callback throws,
 * the guard is still armed and cleans the unvisited suffix exactly once.
 *
 * This is the single ownership mechanism: there is no second, redundant defence,
 * so removing either the transfer or the armed cleanup is independently fatal
 * and independently detectable.
 */
struct action_batch_guard {
    moq_action_t *recs;
    std::size_t   count;
    bool          armed = true;
    action_batch_guard(moq_action_t *r, std::size_t n) noexcept : recs(r), count(n) {}
    void release() noexcept { armed = false; }
    ~action_batch_guard()
    {
        if (!armed) return;
        for (std::size_t i = 0; i < count; i++) moq_action_cleanup(&recs[i]);
    }
    action_batch_guard(const action_batch_guard &)            = delete;
    action_batch_guard &operator=(const action_batch_guard &) = delete;
};

struct event_batch_guard {
    moq_event_t *recs;
    std::size_t  count;
    bool         armed = true;
    event_batch_guard(moq_event_t *r, std::size_t n) noexcept : recs(r), count(n) {}
    void release() noexcept { armed = false; }
    ~event_batch_guard()
    {
        if (!armed) return;
        for (std::size_t i = 0; i < count; i++) moq_event_cleanup(&recs[i]);
    }
    event_batch_guard(const event_batch_guard &)            = delete;
    event_batch_guard &operator=(const event_batch_guard &) = delete;
};

}  // namespace detail


struct session_config {
    moq::perspective   perspective              = moq::perspective::client;
    const moq_alloc_t *alloc                    = nullptr;
    bool               send_request_capacity    = false;
    uint64_t           initial_request_capacity = 0;
    uint32_t           max_actions              = 0;
    uint32_t           max_events               = 0;
    uint32_t           max_subscriptions        = 0;
    uint32_t           max_publishes            = 0;
    uint32_t           max_fetches              = 0;
    uint32_t           max_announcements        = 0;
    uint32_t           max_namespace_subscriptions = 0;
    uint32_t           max_track_statuses       = 0;
    uint32_t           max_open_subgroups       = 0;
    uint32_t           max_data_streams         = 0;
    uint32_t           max_object_payload_size  = 0;
    uint32_t           max_receive_buffer_bytes = 0;
    uint32_t           send_buffer_size         = 0;
    uint32_t           recv_buffer_size         = 0;
    uint32_t           output_scratch_size      = 0;
    uint64_t           idle_timeout_us          = 0;
    // §9.8: bound on the deferred-terminal wait (0 = 30s library default).
    uint64_t           done_wait_timeout_us     = 0;
    uint64_t           goaway_timeout_us        = 0;
    bool               streaming_objects        = false;
    bool               send_auth_token_cache_size = false;
    uint64_t           auth_token_cache_size    = 0;
    moq_version_t      version                  = {};
    // Authorization tokens presented in this side's SETUP, sent by value
    // (moq_session_cfg_t.setup_auth_tokens); create copies them, so the
    // storage need only outlive the create call.
    std::span<const moq_auth_token_t> setup_auth_tokens = {};
};

struct session_options {
    /* The fill pool (children a subscription may have open at once on either
     * side); 0 selects the library default. */
    uint32_t           max_fills                = 0;
    /* Surface subscribers' updates with answer_owed and answer them with
     * accept_/reject_subscription_update (or the publication pair) instead of
     * the session acknowledging them itself. */
    bool               defer_subscription_updates = false;
    /* Surface every standalone FETCH whose range is stated relative to the
     * Largest Object unresolved (extended_event::fetch_request::options) and answer it
     * with the End the application resolved. */
    bool               defer_fetch_ranges       = false;
    /* Use session_config::done_wait_timeout_us as given, without the floor
     * the delivery timeouts set on it (draft 22 Section 9.9 lets a subscriber
     * discard subscription state earlier); ignored when that wait is 0. */
    bool               done_wait_exact          = false;
};

namespace detail {

/* Copy an optional location filter into C storage; returns the pointer the C
 * config borrows for the call, or nullptr when absent. */
inline const moq_location_filter_cfg_t *
location_to_c(const std::optional<location_filter_config> &in,
              moq_location_filter_cfg_t &out) noexcept
{
    if (!in) return nullptr;
    moq_location_filter_cfg_init(&out);
    out.kind                  = to_c(in->kind);
    out.relative_group_offset = in->relative_group_offset;
    out.start_group           = in->start_group;
    out.start_object          = in->start_object;
    out.end_group             = in->end_group;
    out.end_object            = in->end_object;
    return &out;
}

/* Copy an optional fill request into C storage (its location into `loc`). */
inline const moq_fill_cfg_t *
fill_to_c(const std::optional<fill_config> &in, moq_fill_cfg_t &out,
          moq_location_filter_cfg_t &loc) noexcept
{
    if (!in) return nullptr;
    moq_fill_cfg_init(&out);
    out.has_subscriber_priority = in->has_subscriber_priority;
    out.subscriber_priority     = in->subscriber_priority;
    out.has_group_order         = in->has_group_order;
    out.group_order             = to_c(in->group_order);
    out.has_fill_timeout        = in->has_fill_timeout;
    out.fill_timeout_ms         = in->fill_timeout_ms;
    out.location                = location_to_c(in->location, loc);
    return &out;
}

inline void reject_update_to_c(const reject_subscription_update_config &in,
                               moq_reject_subscription_update_cfg_t &out) noexcept
{
    moq_reject_subscription_update_cfg_init(&out);
    out.update_request_id = in.update_request_id;
    out.error_code        = to_c(in.error_code);
    out.reason            = bytes_view(in.reason).raw();
}

inline namespace_update_record
namespace_update_record_from_c(const moq_namespace_update_record_t &r) noexcept
{
    namespace_update_record out{};
    out.update_request_id = r.update_request_id;
    if (r.flags & MOQ_NAMESPACE_UPDATE_PREFIX)
        out.prefix_raw = r.track_namespace_prefix;
    if (r.flags & MOQ_NAMESPACE_UPDATE_FORWARD)
        out.forward = r.forward;
    out.has_authorization = (r.flags & MOQ_NAMESPACE_UPDATE_AUTHORIZATION) != 0;
    out.tokens = {r.tokens, r.token_count};
    return out;
}

}  // namespace detail

class session {
    moq_session_t *s_;

    template <class Send>
    static result<uint64_t> namespace_update_send(
        const namespace_update_config &cfg, Send &&send)
    {
        if (cfg.prefix && cfg.prefix->overflowed())
            return errc::invalid;
        moq_namespace_update_cfg_t c;
        moq_namespace_update_cfg_init_sized(&c, sizeof(c));
        if (cfg.prefix) {
            c.flags |= MOQ_NAMESPACE_UPDATE_PREFIX;
            c.track_namespace_prefix = cfg.prefix->c_namespace();
        }
        if (cfg.forward) {
            c.flags |= MOQ_NAMESPACE_UPDATE_FORWARD;
            c.forward = *cfg.forward;
        }
        c.auth_tokens = cfg.auth_tokens.data();
        c.auth_token_count = cfg.auth_tokens.size();
        uint64_t id = 0;
        auto rc = send(c, &id);
        if (rc < 0)
            return errc_from_result(rc);
        return id;
    }

    explicit session(moq_session_t *s) noexcept : s_(s) {}

    static filter_window window_from_c(const moq_filter_window_t &w) noexcept
    {
        return filter_window{w.resolved, w.empty, w.has_end, w.has_end_object,
                             w.start_group, w.start_object, w.end_group,
                             w.end_object};
    }

public:
    session() noexcept : s_(nullptr) {}

    static result<session> create(session_config cfg, uint64_t now = 0)
    {
        return create(cfg, session_options{}, now);
    }

    static result<session> create(session_config cfg, const session_options &options,
                                  uint64_t now)
    {
        moq_session_cfg_t c{};
        moq_session_cfg_init_sized(&c, sizeof(c),
                             cfg.alloc ? cfg.alloc : moq_alloc_default(),
                             to_c(cfg.perspective));

        c.send_request_capacity    = cfg.send_request_capacity;
        c.initial_request_capacity = cfg.initial_request_capacity;
        c.max_actions              = cfg.max_actions;
        c.max_events               = cfg.max_events;
        c.max_subscriptions        = cfg.max_subscriptions;
        c.max_publishes            = cfg.max_publishes;
        c.max_fetches              = cfg.max_fetches;
        c.max_announcements        = cfg.max_announcements;
        c.max_namespace_subscriptions = cfg.max_namespace_subscriptions;
        c.max_track_statuses       = cfg.max_track_statuses;
        c.max_open_subgroups       = cfg.max_open_subgroups;
        c.max_data_streams         = cfg.max_data_streams;
        c.max_object_payload_size  = cfg.max_object_payload_size;
        c.max_receive_buffer_bytes = cfg.max_receive_buffer_bytes;
        c.send_buffer_size         = cfg.send_buffer_size;
        c.recv_buffer_size         = cfg.recv_buffer_size;
        c.output_scratch_size      = cfg.output_scratch_size;
        c.idle_timeout_us          = cfg.idle_timeout_us;
        c.done_wait_timeout_us     = cfg.done_wait_timeout_us;
        c.goaway_timeout_us        = cfg.goaway_timeout_us;
        c.streaming_objects        = cfg.streaming_objects;
        c.send_auth_token_cache_size = cfg.send_auth_token_cache_size;
        c.auth_token_cache_size    = cfg.auth_token_cache_size;
        c.version                  = cfg.version;
        c.setup_auth_tokens        = cfg.setup_auth_tokens.data();
        c.setup_auth_token_count   = cfg.setup_auth_tokens.size();
        c.max_fills                = options.max_fills;
        c.defer_subscription_updates = options.defer_subscription_updates;
        c.defer_fetch_ranges       = options.defer_fetch_ranges;
        c.done_wait_exact          = options.done_wait_exact;

        moq_session_t *s = nullptr;
        auto rc = moq_session_create(&c, now, &s);
        if (rc < 0)
            return errc_from_result(rc);
        return session(s);
    }

    ~session()
    {
        if (s_)
            moq_session_destroy(s_);
    }

    session(session &&o) noexcept : s_(o.s_) { o.s_ = nullptr; }

    session &operator=(session &&o) noexcept
    {
        if (this != &o) {
            if (s_)
                moq_session_destroy(s_);
            s_   = o.s_;
            o.s_ = nullptr;
        }
        return *this;
    }

    session(const session &)            = delete;
    session &operator=(const session &) = delete;

    // -- Transport input -----------------------------------------------

    result<void> start(uint64_t now)
    {
        return result_from_rc(moq_session_start(s_, now));
    }

    result<void> tick(uint64_t now)
    {
        return result_from_rc(moq_session_tick(s_, now));
    }

    result<void> process_pending(uint64_t now)
    {
        return result_from_rc(moq_session_process_pending(s_, now));
    }

    result<void> on_control_bytes(std::span<const uint8_t> data, uint64_t now)
    {
        return result_from_rc(
            moq_session_on_control_bytes(s_, data.data(), data.size(), now));
    }

    result<void> on_data_bytes(stream_ref ref, std::span<const uint8_t> data,
                               bool fin, uint64_t now)
    {
        return result_from_rc(moq_session_on_data_bytes(
            s_, ref.raw(), data.data(), data.size(), fin, now));
    }

    result<void> on_data_rcbuf(stream_ref ref, const buffer &data, bool fin,
                               uint64_t now)
    {
        return result_from_rc(
            moq_session_on_data_rcbuf(s_, ref.raw(), data.raw(), fin, now));
    }

    result<void> on_data_reset(stream_ref ref, uint64_t error_code,
                               uint64_t now)
    {
        return result_from_rc(
            moq_session_on_data_reset(s_, ref.raw(), error_code, now));
    }

    result<void> on_data_stop(stream_ref ref, uint64_t error_code,
                              uint64_t now)
    {
        return result_from_rc(
            moq_session_on_data_stop(s_, ref.raw(), error_code, now));
    }

    result<void> on_bidi_stream_bytes(stream_ref ref,
                                      std::span<const uint8_t> data, bool fin,
                                      uint64_t now)
    {
        return result_from_rc(moq_session_on_bidi_stream_bytes(
            s_, ref.raw(), data.data(), data.size(), fin, now));
    }

    result<void> on_bidi_stream_reset(stream_ref ref, uint64_t error_code,
                                      uint64_t now)
    {
        return result_from_rc(
            moq_session_on_bidi_stream_reset(s_, ref.raw(), error_code, now));
    }

    result<void> on_bidi_stream_stop(stream_ref ref, uint64_t error_code,
                                     uint64_t now)
    {
        return result_from_rc(
            moq_session_on_bidi_stream_stop(s_, ref.raw(), error_code, now));
    }

    result<void> on_datagram(std::span<const uint8_t> data, uint64_t now)
    {
        return result_from_rc(
            moq_session_on_datagram(s_, data.data(), data.size(), now));
    }

    result<void> on_transport_close(uint64_t code, uint64_t now)
    {
        return result_from_rc(
            moq_session_on_transport_close(s_, code, now));
    }

    /* A local data stream reached "all data committed" (every byte through
     * its FIN acknowledged): stops a FIN-started subgroup delivery timer. */
    result<void> on_data_committed(stream_ref ref, uint64_t now)
    {
        return result_from_rc(
            moq_session_on_data_committed(s_, ref.raw(), now));
    }

    // -- Polling -------------------------------------------------------

    std::optional<polled_action> poll_action()
    {
        moq_action_t a{};
        if (moq_session_poll_actions(s_, &a, 1) == 0)
            return std::nullopt;
        return polled_action(a);
    }

    std::optional<polled_event> poll_event()
    {
        moq_event_t e{};
        if (moq_session_poll_events(s_, &e, 1) == 0)
            return std::nullopt;
        return polled_event(e);
    }

    /*
     * Bounded batched polling: ONE underlying C poll of capacity N, then
     * `consume` is invoked once per returned record, in queue order.
     *
     * Returns the number of records polled and visited; 0 means `consume` was
     * never invoked. There is no internal drain loop and no early-stop
     * protocol -- call again to drain further.
     *
     * OWNERSHIP. An internal guard owns every raw record while armed. Each
     * record is MOVED into the existing move-only polled_action / polled_event
     * owner (std::exchange leaves the slot value-initialised, owning nothing),
     * so ownership lives in exactly one place at every instant. On normal
     * completion the guard is released; if `consume` throws, the still-armed
     * guard cleans the unvisited suffix exactly once while the current owner
     * cleans itself during unwinding. Moving the owner out of `consume` works
     * through the wrapper's existing move contract.
     *
     * LIFETIME PRECONDITION. Non-payload fields of a polled record are borrowed
     * (see the contract at the top of action.hpp / event.hpp): they are valid
     * only while their owner is alive, and an advancing call on THIS session
     * can invalidate them sooner. Therefore:
     *   - a borrowed variant or reference must NOT be used after `consume`
     *     returns for that record;
     *   - `consume` must NOT make an advancing call on THIS session while the
     *     batch is dispatching -- doing so can invalidate the current record and
     *     every record not yet visited. Advancing a DIFFERENT session (the usual
     *     pump pattern) is fine;
     *   - variants of the rcbuf-owning alternatives keep their retained buffers
     *     and remain valid after `consume` returns and after the owner is
     *     destroyed. Those are action::send_data (payload) and, for events,
     *     object_received (payload + properties), fetch_object (payload +
     *     properties), fill_object (payload + properties) and object_chunk
     *     (chunk + properties).
     *
     * COST. Storage is a value-initialised std::array on the stack; the
     * batching layer performs no heap allocation. Stack use scales with N:
     * N * sizeof(moq_action_t) or N * sizeof(moq_event_t) (96 and 144 bytes per
     * record respectively at the time of writing). The default N = 16 matches
     * the measured batching target, NOT any queue depth -- the action queue
     * defaults to 64 and the event queue to 16.
     */
    template <std::size_t N = 16, class F>
        requires std::invocable<F &, polled_action &>
    std::size_t poll_actions(F &&consume)
    {
        static_assert(N > 0, "batched poll capacity must be at least 1");
        std::array<moq_action_t, N> raw{};
        const std::size_t n = moq_session_poll_actions(s_, raw.data(), N);
        detail::action_batch_guard guard{raw.data(), n};
        for (std::size_t i = 0; i < n; i++) {
            polled_action owner(std::exchange(raw[i], moq_action_t{}));
            std::invoke(consume, owner);
        }
        guard.release();
        return n;
    }

    template <std::size_t N = 16, class F>
        requires std::invocable<F &, polled_event &>
    std::size_t poll_events(F &&consume)
    {
        static_assert(N > 0, "batched poll capacity must be at least 1");
        std::array<moq_event_t, N> raw{};
        const std::size_t n = moq_session_poll_events(s_, raw.data(), N);
        detail::event_batch_guard guard{raw.data(), n};
        for (std::size_t i = 0; i < n; i++) {
            polled_event owner(std::exchange(raw[i], moq_event_t{}));
            std::invoke(consume, owner);
        }
        guard.release();
        return n;
    }

    // -- Subscribe operations ------------------------------------------

    result<subscription> subscribe(const subscribe_config &cfg, uint64_t now)
    {
        return subscribe(cfg, subscribe_options{}, now);
    }

    result<subscription> subscribe(const subscribe_config &cfg,
                                    const subscribe_options &options, uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_subscribe_cfg_t c;
        moq_subscribe_cfg_init_sized(&c, sizeof(c));
        c.track_namespace        = cfg.ns.c_namespace();
        c.track_name             = cfg.track.raw();
        c.filter                 = to_c(cfg.filter);
        c.start_group            = cfg.start_group;
        c.start_object           = cfg.start_object;
        c.end_group              = cfg.end_group;
        c.has_subscriber_priority = true;
        c.subscriber_priority    = cfg.priority;
        c.group_order            = to_c(cfg.group_order);
        c.has_forward            = true;
        c.forward                = cfg.forward;
        c.has_new_group_request  = cfg.has_new_group_request;
        c.new_group_request      = cfg.new_group_request;
        /* Location and fill live in wrapper storage for the duration of the
         * call; the C config borrows them and is NULL when absent. */
        moq_location_filter_cfg_t loc_c, fill_loc_c;
        moq_fill_cfg_t            fill_c;
        c.location = detail::location_to_c(options.location, loc_c);
        c.fill     = detail::fill_to_c(options.fill, fill_c, fill_loc_c);
        if (options.object_delivery_timeout_ms) {
            c.delivery_timeout_flags |= MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT;
            c.object_delivery_timeout_ms = *options.object_delivery_timeout_ms;
        }
        if (options.subgroup_delivery_timeout_ms) {
            c.delivery_timeout_flags |= MOQ_SUBSCRIBE_SUBGROUP_DELIVERY_TIMEOUT;
            c.subgroup_delivery_timeout_ms = *options.subgroup_delivery_timeout_ms;
        }
        c.has_rendezvous_timeout = options.rendezvous_timeout_ms.has_value();
        c.rendezvous_timeout_ms = options.rendezvous_timeout_ms.value_or(0);
        c.has_include_properties = options.include_properties.has_value();
        c.include_properties = options.include_properties.value_or(true);

        moq_subscription_t h{};
        auto rc = moq_session_subscribe(s_, &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return subscription(h);
    }

    result<void> accept_subscribe(subscription sub,
                                  const accept_subscribe_config &cfg,
                                  uint64_t now)
    {
        moq_accept_subscribe_cfg_t c;
        moq_accept_subscribe_cfg_init(&c);
        c.has_track_alias   = cfg.has_track_alias;
        c.track_alias       = cfg.track_alias;
        c.has_largest       = cfg.has_largest;
        c.largest_group     = cfg.largest_group;
        c.largest_object    = cfg.largest_object;
        c.has_expires       = cfg.has_expires;
        c.expires_ms        = cfg.expires_ms;
        c.track_properties  = cfg.track_properties.raw();
        return result_from_rc(
            moq_session_accept_subscribe(s_, sub.raw(), &c, now));
    }

    result<void> reject_subscribe(subscription sub,
                                  const reject_subscribe_config &cfg,
                                  uint64_t now)
    {
        moq_reject_subscribe_cfg_t c;
        moq_reject_subscribe_cfg_init(&c);
        c.error_code     = to_c(cfg.error_code);
        c.reason         = bytes_view(cfg.reason).raw();
        c.can_retry      = cfg.can_retry;
        c.retry_after_ms = cfg.retry_after_ms;
        c.redirect.connect_uri     = bytes_view(cfg.redirect.connect_uri).raw();
        c.redirect.track_namespace = cfg.redirect.ns.c_namespace();
        c.redirect.track_name      = cfg.redirect.track.raw();
        return result_from_rc(
            moq_session_reject_subscribe(s_, sub.raw(), &c, now));
    }

    result<void> unsubscribe(subscription sub, uint64_t now)
    {
        return result_from_rc(moq_session_unsubscribe(s_, sub.raw(), now));
    }

    // -- Subgroup operations -------------------------------------------

    result<subgroup_handle> open_subgroup(subscription sub,
                                          const subgroup_config &cfg,
                                          uint64_t now)
    {
        moq_subgroup_cfg_t c;
        moq_subgroup_cfg_init(&c);
        c.group_id           = cfg.group_id;
        c.subgroup_id        = cfg.subgroup_id;
        c.publisher_priority = cfg.publisher_priority;
        c.object_properties  = cfg.object_properties;
        c.end_of_group       = cfg.end_of_group;
        c.first_object       = cfg.first_object;

        moq_subgroup_handle_t h{};
        auto rc = moq_session_open_subgroup(s_, sub.raw(), &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return subgroup_handle(h);
    }

    result<void> write_object(subgroup_handle sg, uint64_t object_id,
                              const buffer &payload, uint64_t now)
    {
        return result_from_rc(
            moq_session_write_object(s_, sg.raw(), object_id,
                                     payload.raw(), now));
    }

    result<void> write_object_ex(subgroup_handle sg, uint64_t object_id,
                                 const buffer &payload,
                                 const buffer *properties, uint64_t now)
    {
        moq_object_cfg_t c;
        moq_object_cfg_init(&c);
        c.object_id  = object_id;
        c.payload    = payload.raw();
        c.properties = properties ? properties->raw() : nullptr;
        return result_from_rc(
            moq_session_write_object_ex(s_, sg.raw(), &c, now));
    }

    /* As above, with the Prior Group ID Gap / Prior Object ID Gap Object
     * Properties added to the object's properties (draft-22 §10.8, §10.9;
     * see moq_object_cfg_t). */
    result<void> write_object_ex(subgroup_handle sg, uint64_t object_id,
                                 const buffer &payload,
                                 const buffer *properties,
                                 std::optional<uint64_t> prior_group_gap,
                                 std::optional<uint64_t> prior_object_gap,
                                 uint64_t now)
    {
        moq_object_cfg_t c;
        moq_object_cfg_init_sized(&c, sizeof(c));
        c.object_id  = object_id;
        c.payload    = payload.raw();
        c.properties = properties ? properties->raw() : nullptr;
        c.has_prior_group_gap = prior_group_gap.has_value();
        c.prior_group_gap = prior_group_gap.value_or(0);
        c.has_prior_object_gap = prior_object_gap.has_value();
        c.prior_object_gap = prior_object_gap.value_or(0);
        return result_from_rc(
            moq_session_write_object_ex(s_, sg.raw(), &c, now));
    }

    result<void> close_subgroup(subgroup_handle sg, uint64_t now)
    {
        return result_from_rc(
            moq_session_close_subgroup(s_, sg.raw(), now));
    }

    result<void> reset_subgroup(subgroup_handle sg, uint64_t error_code,
                                uint64_t now)
    {
        return result_from_rc(
            moq_session_reset_subgroup(s_, sg.raw(), error_code, now));
    }

    // -- Datagram operations -------------------------------------------

    result<void> send_object_datagram(subscription sub,
                                      const datagram_config &cfg,
                                      const buffer &payload,
                                      uint64_t now)
    {
        return result_from_rc(moq_session_send_object_datagram(
            s_, sub.raw(), cfg.group_id, cfg.object_id,
            cfg.publisher_priority, cfg.end_of_group, payload.raw(),
            cfg.properties.data(), cfg.properties.size(), now));
    }

    result<void> send_status_datagram(subscription sub,
                                      const status_datagram_config &cfg,
                                      uint64_t now)
    {
        return result_from_rc(moq_session_send_status_datagram(
            s_, sub.raw(), cfg.group_id, cfg.object_id,
            cfg.publisher_priority, to_c(cfg.status), now));
    }

    result<void> done_subscribe(subscription sub,
                                const done_subscribe_config &cfg,
                                uint64_t now)
    {
        moq_done_subscribe_cfg_t c;
        moq_done_subscribe_cfg_init(&c);
        c.status_code  = cfg.status_code;
        c.stream_count = cfg.stream_count;
        c.reason       = bytes_view(cfg.reason).raw();
        return result_from_rc(
            moq_session_done_subscribe(s_, sub.raw(), &c, now));
    }

    result<void> update_subscription(subscription sub,
                                     const subscription_update_config &cfg,
                                     uint64_t now)
    {
        return update_subscription(sub, cfg, update_options{}, now);
    }

    result<void> update_subscription(subscription sub,
                                     const subscription_update_config &cfg,
                                     const update_options &options, uint64_t now)
    {
        moq_subscription_update_cfg_t c;
        /* Sized init: this wrapper sets appended tail fields (auth tokens,
         * new-group request, filter); the frozen pointer-only init would
         * stamp the v0 prefix and silently drop them. */
        moq_subscription_update_cfg_init_sized(&c, sizeof(c));
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority     = cfg.subscriber_priority;
        c.has_forward             = cfg.has_forward;
        c.forward                 = cfg.forward;
        c.has_delivery_timeout    = cfg.has_delivery_timeout;
        c.delivery_timeout_us     = cfg.delivery_timeout_us;
        c.auth_tokens             = cfg.auth_tokens.data();
        c.auth_token_count        = cfg.auth_tokens.size();
        c.has_new_group_request   = cfg.has_new_group_request;
        c.new_group_request       = cfg.new_group_request;
        c.has_filter              = cfg.has_filter;
        c.filter                  = cfg.filter;
        c.start_group             = cfg.start_group;
        c.start_object            = cfg.start_object;
        c.end_group               = cfg.end_group;
        moq_location_filter_cfg_t loc_c, fill_loc_c;
        moq_fill_cfg_t            fill_c;
        c.location = detail::location_to_c(options.location, loc_c);
        c.fill = detail::fill_to_c(options.fill, fill_c, fill_loc_c);
        if (options.object_delivery_timeout_ms) {
            c.delivery_timeout_flags |= MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT;
            c.object_delivery_timeout_ms = *options.object_delivery_timeout_ms;
        }
        if (options.subgroup_delivery_timeout_ms) {
            c.delivery_timeout_flags |= MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
            c.subgroup_delivery_timeout_ms = *options.subgroup_delivery_timeout_ms;
        }
        return result_from_rc(
            moq_session_update_subscription(s_, sub.raw(), &c, now));
    }

    // -- Fills (draft-22 section 3.4) -----------------------------------
    //
    // Publisher side, on a child surfaced by event::fill_request: open_fill
    // opens the fill fetch stream with its header; write_fill_object keeps
    // the caller's buffer refs (queued actions hold their own); write_fill_range
    // marks a gap; end_fill finishes with a FIN; abort_fill resets it (an
    // unopened child is opened first, section 3.4.1). Subscriber side:
    // cancel_fill stops one fill without touching the subscription.

    result<void> open_fill(fill_handle fill, uint64_t now)
    {
        return result_from_rc(moq_session_open_fill(s_, fill.raw(), now));
    }

    result<void> write_fill_object(fill_handle fill,
                                   const fetch_object_config &cfg,
                                   const buffer &payload, uint64_t now)
    {
        moq_fetch_object_cfg_t c;
        moq_fetch_object_cfg_init(&c);
        c.group_id           = cfg.group_id;
        c.subgroup_id        = cfg.subgroup_id;
        c.object_id          = cfg.object_id;
        c.publisher_priority = cfg.publisher_priority;
        c.payload            = payload.raw();
        c.properties         = cfg.properties ? cfg.properties->raw() : nullptr;
        c.datagram           = cfg.datagram;
        return result_from_rc(
            moq_session_write_fill_object(s_, fill.raw(), &c, now));
    }

    result<void> write_fill_range(fill_handle fill, fetch_range_kind kind,
                                  uint64_t group_id, uint64_t object_id,
                                  uint64_t now)
    {
        return result_from_rc(moq_session_write_fill_range(
            s_, fill.raw(), to_c(kind), group_id, object_id, now));
    }

    result<void> end_fill(fill_handle fill, uint64_t now)
    {
        return result_from_rc(moq_session_end_fill(s_, fill.raw(), now));
    }

    result<void> abort_fill(fill_handle fill, uint64_t error_code, uint64_t now)
    {
        return result_from_rc(
            moq_session_abort_fill(s_, fill.raw(), error_code, now));
    }

    result<void> cancel_fill(fill_handle fill, uint64_t now)
    {
        return result_from_rc(moq_session_cancel_fill(s_, fill.raw(), now));
    }

    // -- Deferred update answers (session_options::defer_subscription_updates)

    result<void> accept_subscription_update(
        subscription sub, const accept_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_accept_subscription_update_cfg_t c;
        moq_accept_subscription_update_cfg_init(&c);
        c.update_request_id = cfg.update_request_id;
        return result_from_rc(
            moq_session_accept_subscription_update(s_, sub.raw(), &c, now));
    }

    result<void> reject_subscription_update(
        subscription sub, const reject_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_reject_subscription_update_cfg_t c;
        detail::reject_update_to_c(cfg, c);
        return result_from_rc(
            moq_session_reject_subscription_update(s_, sub.raw(), &c, now));
    }

    result<void> accept_publication_update(
        publication pub, const accept_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_accept_subscription_update_cfg_t c;
        moq_accept_subscription_update_cfg_init(&c);
        c.update_request_id = cfg.update_request_id;
        return result_from_rc(
            moq_session_accept_publication_update(s_, pub.raw(), &c, now));
    }

    result<void> reject_publication_update(
        publication pub, const reject_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_reject_subscription_update_cfg_t c;
        detail::reject_update_to_c(cfg, c);
        return result_from_rc(
            moq_session_reject_publication_update(s_, pub.raw(), &c, now));
    }

    /* The update whose answer is owed on a publisher-role subscription.
     * errc::wrong_state when none is owed. Not an advancing call. */
    result<owed_update> subscription_owed_update(subscription sub)
    {
        uint64_t id = 0;
        bool has_filter = false;
        moq_location_filter_cfg_t f;
        moq_location_filter_cfg_init(&f);
        auto rc = moq_session_subscription_owed_update(s_, sub.raw(), &id,
                                                       &has_filter, &f);
        if (rc < 0)
            return errc_from_result(rc);
        owed_update out{};
        out.update_request_id = id;
        if (has_filter)
            out.filter = location_from_c(f);
        return out;
    }

    result<void> set_defer_subscription_updates(bool defer)
    {
        return result_from_rc(
            moq_session_set_defer_subscription_updates(s_, defer));
    }

    result<void> set_defer_fetch_ranges(bool defer)
    {
        return result_from_rc(moq_session_set_defer_fetch_ranges(s_, defer));
    }

    // -- Draft-22 namespace updates (§9.5) ----------------------------------

    /* Send an update of a request this side made; the value is the update's
     * own Request ID, which its result event names. errc::request_blocked
     * while an earlier update of the same request is unanswered. */
    result<uint64_t> update_namespace(announcement ann,
                                      const namespace_update_config &cfg,
                                      uint64_t now)
    {
        return namespace_update_send(cfg, [&](const moq_namespace_update_cfg_t &c,
                                              uint64_t *id) {
            return moq_session_update_namespace(s_, ann.raw(), &c, now, id);
        });
    }

    result<uint64_t> update_ns_sub(ns_sub_handle handle,
                                   const namespace_update_config &cfg,
                                   uint64_t now)
    {
        return namespace_update_send(cfg, [&](const moq_namespace_update_cfg_t &c,
                                              uint64_t *id) {
            return moq_session_update_ns_sub(s_, handle.raw(), &c, now, id);
        });
    }

    result<uint64_t> update_subscribe_tracks(track_sub_handle handle,
                                             const namespace_update_config &cfg,
                                             uint64_t now)
    {
        return namespace_update_send(cfg, [&](const moq_namespace_update_cfg_t &c,
                                              uint64_t *id) {
            return moq_session_update_subscribe_tracks(s_, handle.raw(), &c, now,
                                                       id);
        });
    }

    /* Leave the answer to every peer namespace update to the application
     * (the *_update_request events). Off, they are refused. */
    result<void> set_defer_namespace_updates(bool defer)
    {
        return result_from_rc(
            moq_session_set_defer_namespace_updates(s_, defer));
    }

    result<void> accept_namespace_update(
        announcement ann, const accept_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_accept_subscription_update_cfg_t c;
        moq_accept_subscription_update_cfg_init(&c);
        c.update_request_id = cfg.update_request_id;
        return result_from_rc(
            moq_session_accept_namespace_update(s_, ann.raw(), &c, now));
    }

    result<void> reject_namespace_update(
        announcement ann, const reject_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_reject_subscription_update_cfg_t c;
        detail::reject_update_to_c(cfg, c);
        return result_from_rc(
            moq_session_reject_namespace_update(s_, ann.raw(), &c, now));
    }

    result<void> accept_ns_sub_update(
        ns_sub_handle handle, const accept_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_accept_subscription_update_cfg_t c;
        moq_accept_subscription_update_cfg_init(&c);
        c.update_request_id = cfg.update_request_id;
        return result_from_rc(
            moq_session_accept_ns_sub_update(s_, handle.raw(), &c, now));
    }

    result<void> reject_ns_sub_update(
        ns_sub_handle handle, const reject_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_reject_subscription_update_cfg_t c;
        detail::reject_update_to_c(cfg, c);
        return result_from_rc(
            moq_session_reject_ns_sub_update(s_, handle.raw(), &c, now));
    }

    result<void> accept_subscribe_tracks_update(
        track_sub_handle handle, const accept_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_accept_subscription_update_cfg_t c;
        moq_accept_subscription_update_cfg_init(&c);
        c.update_request_id = cfg.update_request_id;
        return result_from_rc(
            moq_session_accept_subscribe_tracks_update(s_, handle.raw(), &c, now));
    }

    result<void> reject_subscribe_tracks_update(
        track_sub_handle handle, const reject_subscription_update_config &cfg,
        uint64_t now)
    {
        moq_reject_subscription_update_cfg_t c;
        detail::reject_update_to_c(cfg, c);
        return result_from_rc(
            moq_session_reject_subscribe_tracks_update(s_, handle.raw(), &c, now));
    }

    /* The peer update whose answer is owed, as a borrowed record.
     * errc::wrong_state when none is owed. Not advancing calls. */
    result<namespace_update_record> namespace_owed_update(announcement ann)
    {
        moq_namespace_update_record_t r;
        auto rc = moq_session_namespace_owed_update(s_, ann.raw(), &r, sizeof(r));
        if (rc < 0) return errc_from_result(rc);
        return detail::namespace_update_record_from_c(r);
    }

    result<namespace_update_record> ns_sub_owed_update(ns_sub_handle handle)
    {
        moq_namespace_update_record_t r;
        auto rc = moq_session_ns_sub_owed_update(s_, handle.raw(), &r, sizeof(r));
        if (rc < 0) return errc_from_result(rc);
        return detail::namespace_update_record_from_c(r);
    }

    result<namespace_update_record> subscribe_tracks_owed_update(
        track_sub_handle handle)
    {
        moq_namespace_update_record_t r;
        auto rc = moq_session_subscribe_tracks_owed_update(s_, handle.raw(), &r,
                                                           sizeof(r));
        if (rc < 0) return errc_from_result(rc);
        return detail::namespace_update_record_from_c(r);
    }

    /* The filter in effect on a subscription / publication as this side
     * knows it; kind `none` when unfiltered. Not advancing calls. */
    result<location_filter_config> subscription_filter(subscription sub)
    {
        moq_location_filter_cfg_t f;
        moq_location_filter_cfg_init(&f);
        auto rc = moq_session_subscription_filter(s_, sub.raw(), &f);
        if (rc < 0)
            return errc_from_result(rc);
        return location_from_c(f);
    }

    /* The Location window each side resolved the filter to. */
    result<filter_window> subscription_window(subscription sub)
    {
        moq_filter_window_t w;
        auto rc = moq_session_subscription_window(s_, sub.raw(), &w, sizeof(w));
        if (rc < 0)
            return errc_from_result(rc);
        return window_from_c(w);
    }

    result<filter_window> publication_window(publication pub)
    {
        moq_filter_window_t w;
        auto rc = moq_session_publication_window(s_, pub.raw(), &w, sizeof(w));
        if (rc < 0)
            return errc_from_result(rc);
        return window_from_c(w);
    }

    result<location_filter_config> publication_filter(publication pub)
    {
        moq_location_filter_cfg_t f;
        moq_location_filter_cfg_init(&f);
        auto rc = moq_session_publication_filter(s_, pub.raw(), &f);
        if (rc < 0)
            return errc_from_result(rc);
        return location_from_c(f);
    }

    // -- Publisher state notifications (PUBLISH_STATE_NOTIFY) -----------

    result<void> notify_subscription_state(subscription sub,
                                           const publish_state_config &cfg,
                                           uint64_t now)
    {
        moq_publish_state_cfg_t c;
        moq_publish_state_cfg_init_sized(&c, sizeof(c));
        moq_location_filter_cfg_t filter_c;
        c.has_forward = cfg.has_forward;
        c.forward     = cfg.forward;
        c.filter      = detail::location_to_c(cfg.filter, filter_c);
        return result_from_rc(
            moq_session_notify_subscription_state(s_, sub.raw(), &c, now));
    }

    result<void> notify_publication_state(publication pub,
                                          const publish_state_config &cfg,
                                          uint64_t now)
    {
        moq_publish_state_cfg_t c;
        moq_publish_state_cfg_init_sized(&c, sizeof(c));
        moq_location_filter_cfg_t filter_c;
        c.has_forward = cfg.has_forward;
        c.forward     = cfg.forward;
        c.filter      = detail::location_to_c(cfg.filter, filter_c);
        return result_from_rc(
            moq_session_notify_publication_state(s_, pub.raw(), &c, now));
    }

    // -- Transport binding (transport_binding.h) ------------------------

    /* The fact mask this session's profile requires before it starts; zero
     * when the profile starts without a binding and accepts one optionally. */
    uint32_t binding_requirements() const noexcept
    {
        return moq_session_binding_requirements(s_);
    }

    static uint32_t version_binding_requirements(moq_version_t version) noexcept
    {
        return moq_version_binding_requirements(version);
    }

    /* Bind the transport before start. The target bytes are copied by the
     * session; the policy context is borrowed until the session is destroyed. */
    result<void> bind_transport(const transport_binding_config &cfg)
    {
        moq_transport_binding_t b;
        moq_transport_binding_init(&b);
        b.kind            = cfg.kind;
        b.facts_known     = cfg.facts_known;
        b.facts_supported = cfg.facts_supported;
        b.target_present  = cfg.target_present;
        b.authority       = cfg.authority.raw();
        b.path_query      = cfg.path_query.raw();
        b.accept_uri      = cfg.accept_uri;
        b.accept_uri_ctx  = cfg.accept_uri_ctx;
        return result_from_rc(moq_session_bind_transport(s_, &b, sizeof(b)));
    }

    // -- Fetch operations -----------------------------------------------

    result<fetch_handle> fetch(const fetch_config &cfg, uint64_t now)
    {
        return fetch(cfg, fetch_options{}, now);
    }

    result<fetch_handle> fetch(const fetch_config &cfg,
                               const fetch_options &options, uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_fetch_cfg_t c;
        /* sized init: the binding always exposes the full struct, including
         * the appended joining_pub discriminator */
        moq_fetch_cfg_init_sized(&c, sizeof c);
        c.track_namespace        = cfg.ns.c_namespace();
        c.track_name             = cfg.track.raw();
        c.start_group            = cfg.start_group;
        c.start_object           = cfg.start_object;
        c.end_group              = cfg.end_group;
        c.end_object             = cfg.end_object;
        c.group_order            = to_c(cfg.group_order);
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority    = cfg.subscriber_priority;
        c.is_joining             = cfg.is_joining;
        c.joining_relative       = cfg.joining_relative;
        c.joining_sub            = cfg.joining_sub.raw();
        c.joining_pub            = cfg.joining_pub.raw();
        c.joining_start          = cfg.joining_start;
        c.auth_tokens            = cfg.auth_tokens.data();
        c.auth_token_count       = cfg.auth_tokens.size();
        moq_location_filter_cfg_t loc_c;
        c.location               = detail::location_to_c(options.location, loc_c);
        c.has_fill_timeout       = options.has_fill_timeout;
        c.fill_timeout_ms        = options.fill_timeout_ms;
        c.has_include_properties = options.include_properties.has_value();
        c.include_properties     = options.include_properties.value_or(true);

        moq_fetch_t h{};
        auto rc = moq_session_fetch(s_, &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return fetch_handle(h);
    }

    result<void> accept_fetch(fetch_handle fetch,
                              const accept_fetch_config &cfg,
                              uint64_t now)
    {
        moq_accept_fetch_cfg_t c;
        moq_accept_fetch_cfg_init(&c);
        c.end_of_track = cfg.end_of_track;
        c.end_group    = cfg.end_group;
        c.end_object   = cfg.end_object;
        c.empty        = cfg.empty;
        c.track_properties = cfg.track_properties.raw();
        return result_from_rc(
            moq_session_accept_fetch(s_, fetch.raw(), &c, now));
    }

    result<void> reject_fetch(fetch_handle fetch,
                              const reject_fetch_config &cfg,
                              uint64_t now)
    {
        moq_reject_fetch_cfg_t c;
        moq_reject_fetch_cfg_init(&c);
        c.error_code     = to_c(cfg.error_code);
        c.reason         = bytes_view(cfg.reason).raw();
        c.can_retry      = cfg.can_retry;
        c.retry_after_ms = cfg.retry_after_ms;
        c.redirect.connect_uri     = bytes_view(cfg.redirect.connect_uri).raw();
        c.redirect.track_namespace = cfg.redirect.ns.c_namespace();
        c.redirect.track_name      = cfg.redirect.track.raw();
        return result_from_rc(
            moq_session_reject_fetch(s_, fetch.raw(), &c, now));
    }

    result<void> write_fetch_object(fetch_handle fetch,
                                    const fetch_object_config &cfg,
                                    const buffer &payload,
                                    uint64_t now)
    {
        moq_fetch_object_cfg_t c;
        moq_fetch_object_cfg_init(&c);
        c.group_id           = cfg.group_id;
        c.subgroup_id        = cfg.subgroup_id;
        c.object_id          = cfg.object_id;
        c.publisher_priority = cfg.publisher_priority;
        c.datagram           = cfg.datagram;
        c.payload            = payload.raw();
        c.properties         = cfg.properties ? cfg.properties->raw() : nullptr;
        return result_from_rc(
            moq_session_write_fetch_object(s_, fetch.raw(), &c, now));
    }

    result<void> write_fetch_range(fetch_handle fetch, fetch_range_kind kind,
                                   uint64_t group_id, uint64_t object_id,
                                   uint64_t now)
    {
        return result_from_rc(moq_session_write_fetch_range(
            s_, fetch.raw(), to_c(kind), group_id, object_id, now));
    }

    result<void> end_fetch(fetch_handle fetch, uint64_t now)
    {
        return result_from_rc(moq_session_end_fetch(s_, fetch.raw(), now));
    }

    /* Update a FETCH this side sent (draft-22 §9.5). */
    result<void> update_fetch(fetch_handle fetch, const fetch_update_config &cfg,
                              uint64_t now)
    {
        moq_fetch_update_cfg_t c;
        moq_fetch_update_cfg_init(&c);
        c.has_subscriber_priority = cfg.subscriber_priority.has_value();
        c.subscriber_priority     = cfg.subscriber_priority.value_or(0);
        c.auth_tokens             = cfg.auth_tokens.data();
        c.auth_token_count        = cfg.auth_tokens.size();
        return result_from_rc(moq_session_update_fetch(s_, fetch.raw(), &c, now));
    }

    result<void> fetch_cancel(fetch_handle fetch, uint64_t now)
    {
        return result_from_rc(moq_session_fetch_cancel(s_, fetch.raw(), now));
    }

    // -- Publish operations ---------------------------------------------

    result<publication> publish(const publish_config &cfg, uint64_t now)
    {
        return publish(cfg, publish_options{}, now);
    }

    result<publication> publish(const publish_config &cfg,
                                 const publish_options &options, uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_publish_cfg_t c;
        /* Sets the auth-token tail past the frozen v0 prefix, so the sized
         * init is required (the pointer init would leave them reader-ignored). */
        moq_publish_cfg_init_sized(&c, sizeof(c));
        c.track_namespace  = cfg.ns.c_namespace();
        c.track_name       = cfg.track.raw();
        c.has_track_alias  = cfg.has_track_alias;
        c.track_alias      = cfg.track_alias;
        c.has_forward      = cfg.has_forward;
        c.forward          = cfg.forward;
        c.track_properties = cfg.track_properties.raw();
        c.auth_tokens      = cfg.auth_tokens.data();
        c.auth_token_count = cfg.auth_tokens.size();
        moq_location_filter_cfg_t loc_c;
        c.location = detail::location_to_c(options.location, loc_c);
        c.has_subscriber_priority = options.subscriber_priority.has_value();
        c.subscriber_priority = options.subscriber_priority.value_or(128);
        c.has_group_order = options.group_order.has_value();
        c.group_order = to_c(options.group_order.value_or(group_order::default_order));
        c.has_object_delivery_timeout = options.object_delivery_timeout_ms.has_value();
        c.object_delivery_timeout_ms = options.object_delivery_timeout_ms.value_or(0);
        c.has_subgroup_delivery_timeout = options.subgroup_delivery_timeout_ms.has_value();
        c.subgroup_delivery_timeout_ms = options.subgroup_delivery_timeout_ms.value_or(0);
        c.has_expires = options.expires_ms.has_value();
        c.expires_ms = options.expires_ms.value_or(0);

        moq_publication_t h{};
        auto rc = moq_session_publish(s_, &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return publication(h);
    }

    result<void> accept_publish(publication pub,
                                const accept_publish_config &cfg,
                                uint64_t now)
    {
        return accept_publish(pub, cfg, accept_publish_options{}, now);
    }

    result<void> accept_publish(publication pub, const accept_publish_config &cfg,
                                const accept_publish_options &options, uint64_t now)
    {
        moq_accept_publish_cfg_t c;
        /* Sets tails past the frozen v0 prefix (new-group, filter, forward),
         * so the sized init is required. */
        moq_accept_publish_cfg_init_sized(&c, sizeof(c));
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority     = cfg.subscriber_priority;
        c.group_order             = to_c(cfg.group_order);
        c.has_new_group_request   = cfg.has_new_group_request;
        c.new_group_request       = cfg.new_group_request;
        c.filter                  = to_c(cfg.filter);
        c.start_group             = cfg.start_group;
        c.start_object            = cfg.start_object;
        c.end_group               = cfg.end_group;
        c.has_forward             = cfg.has_forward;
        c.forward                 = cfg.forward;
        c.has_expires             = options.expires_ms.has_value();
        c.expires_ms              = options.expires_ms.value_or(0);
        return result_from_rc(
            moq_session_accept_publish(s_, pub.raw(), &c, now));
    }

    result<publish_initial_parameters> get_publish_initial_parameters(publication pub) const
    {
        moq_publish_initial_parameters_t c{};
        auto rc = moq_session_get_publish_initial_parameters(s_, pub.raw(), &c, sizeof(c));
        if (rc < 0)
            return errc_from_result(rc);
        publish_initial_parameters out{};
        out.has_subscriber_priority = (c.flags & MOQ_PUBLISH_INITIAL_HAS_SUBSCRIBER_PRIORITY) != 0;
        out.subscriber_priority = c.subscriber_priority;
        out.has_group_order = (c.flags & MOQ_PUBLISH_INITIAL_HAS_GROUP_ORDER) != 0;
        out.group_order = group_order_from_c(static_cast<moq_group_order_t>(c.group_order));
        if (c.flags & MOQ_PUBLISH_INITIAL_HAS_LOCATION)
            out.location = location_from_c(c.location);
        if (c.flags & MOQ_PUBLISH_INITIAL_HAS_OBJECT_TIMEOUT)
            out.object_delivery_timeout_ms = c.object_delivery_timeout_ms;
        if (c.flags & MOQ_PUBLISH_INITIAL_HAS_SUBGROUP_TIMEOUT)
            out.subgroup_delivery_timeout_ms = c.subgroup_delivery_timeout_ms;
        return out;
    }

    result<void> reject_publish(publication pub,
                                const reject_publish_config &cfg,
                                uint64_t now)
    {
        moq_reject_publish_cfg_t c;
        moq_reject_publish_cfg_init(&c);
        c.error_code     = to_c(cfg.error_code);
        c.reason         = bytes_view(cfg.reason).raw();
        c.can_retry      = cfg.can_retry;
        c.retry_after_ms = cfg.retry_after_ms;
        return result_from_rc(
            moq_session_reject_publish(s_, pub.raw(), &c, now));
    }

    /* Cancel an accepted publication (subscriber side), resetting its request
     * stream with `error_code` (moq_session_cancel_publication). */
    result<void> cancel_publication(publication pub, uint64_t error_code,
                                    uint64_t now)
    {
        return result_from_rc(
            moq_session_cancel_publication(s_, pub.raw(), error_code, now));
    }

    result<subgroup_handle> open_pub_subgroup(publication pub,
                                              const subgroup_config &cfg,
                                              uint64_t now)
    {
        moq_subgroup_cfg_t c;
        moq_subgroup_cfg_init(&c);
        c.group_id           = cfg.group_id;
        c.subgroup_id        = cfg.subgroup_id;
        c.publisher_priority = cfg.publisher_priority;
        c.object_properties  = cfg.object_properties;
        c.end_of_group       = cfg.end_of_group;
        c.first_object       = cfg.first_object;

        moq_subgroup_handle_t h{};
        auto rc = moq_session_open_pub_subgroup(s_, pub.raw(), &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return subgroup_handle(h);
    }

    result<void> send_pub_object_datagram(publication pub,
                                          const datagram_config &cfg,
                                          const buffer &payload,
                                          uint64_t now)
    {
        return result_from_rc(moq_session_send_pub_object_datagram(
            s_, pub.raw(), cfg.group_id, cfg.object_id,
            cfg.publisher_priority, cfg.end_of_group, payload.raw(),
            cfg.properties.data(), cfg.properties.size(), now));
    }

    result<void> send_pub_status_datagram(publication pub,
                                          const status_datagram_config &cfg,
                                          uint64_t now)
    {
        return result_from_rc(moq_session_send_pub_status_datagram(
            s_, pub.raw(), cfg.group_id, cfg.object_id,
            cfg.publisher_priority, to_c(cfg.status), now));
    }

    result<void> update_publication(publication pub,
                                    const publication_update_config &cfg,
                                    uint64_t now)
    {
        return update_publication(pub, cfg, publication_update_options{}, now);
    }

    result<void> update_publication(publication pub,
                                    const publication_update_config &cfg,
                                    const publication_update_options &options, uint64_t now)
    {
        moq_publication_update_cfg_t c;
        moq_publication_update_cfg_init(&c);
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority     = cfg.subscriber_priority;
        c.has_forward             = cfg.has_forward;
        c.forward                 = cfg.forward;
        c.has_delivery_timeout    = cfg.has_delivery_timeout;
        c.delivery_timeout_us     = cfg.delivery_timeout_us;
        c.auth_tokens             = cfg.auth_tokens.data();
        c.auth_token_count        = cfg.auth_tokens.size();
        c.has_new_group_request   = cfg.has_new_group_request;
        c.new_group_request       = cfg.new_group_request;
        moq_location_filter_cfg_t loc_c, fill_loc_c;
        moq_fill_cfg_t            fill_c;
        c.location = detail::location_to_c(options.location, loc_c);
        c.fill = detail::fill_to_c(options.fill, fill_c, fill_loc_c);
        if (options.object_delivery_timeout_ms) {
            c.delivery_timeout_flags |= MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT;
            c.object_delivery_timeout_ms = *options.object_delivery_timeout_ms;
        }
        if (options.subgroup_delivery_timeout_ms) {
            c.delivery_timeout_flags |= MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
            c.subgroup_delivery_timeout_ms = *options.subgroup_delivery_timeout_ms;
        }
        return result_from_rc(
            moq_session_update_publication(s_, pub.raw(), &c, now));
    }

    result<void> finish_publish(publication pub,
                                const finish_publish_config &cfg,
                                uint64_t now)
    {
        moq_finish_publish_cfg_t c;
        moq_finish_publish_cfg_init(&c);
        c.status_code  = cfg.status_code;
        c.stream_count = cfg.stream_count;
        c.reason       = bytes_view(cfg.reason).raw();
        return result_from_rc(
            moq_session_finish_publish(s_, pub.raw(), &c, now));
    }

    // -- SUBSCRIBE_TRACKS (draft-22 §9.18) -------------------------------

    result<track_sub_handle> subscribe_tracks(const subscribe_tracks_config &cfg,
                                              uint64_t now)
    {
        if (cfg.ns_prefix.overflowed())
            return errc::invalid;
        moq_subscribe_tracks_cfg_t c;
        moq_subscribe_tracks_cfg_init(&c);
        c.track_namespace_prefix = cfg.ns_prefix.c_namespace();
        c.has_forward      = cfg.forward.has_value();
        c.forward          = cfg.forward.value_or(true);
        c.auth_tokens      = cfg.auth_tokens.data();
        c.auth_token_count = cfg.auth_tokens.size();
        if (cfg.group_order) {
            c.options_flags |= MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER;
            c.group_order = to_c(*cfg.group_order);
        }
        if (cfg.include_properties) {
            c.options_flags |= MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES;
            c.include_properties = *cfg.include_properties;
        }
        moq_track_sub_handle_t h{};
        auto rc = moq_session_subscribe_tracks(s_, &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return track_sub_handle(h);
    }

    result<void> accept_subscribe_tracks(track_sub_handle handle, uint64_t now)
    {
        moq_accept_subscribe_tracks_cfg_t c;
        moq_accept_subscribe_tracks_cfg_init(&c);
        return result_from_rc(
            moq_session_accept_subscribe_tracks(s_, handle.raw(), &c, now));
    }

    result<void> reject_subscribe_tracks(track_sub_handle handle,
                                         const reject_subscribe_tracks_config &cfg,
                                         uint64_t now)
    {
        moq_reject_subscribe_tracks_cfg_t c;
        moq_reject_subscribe_tracks_cfg_init(&c);
        c.error_code = to_c(cfg.error_code);
        c.reason     = moq_bytes_t{reinterpret_cast<const uint8_t *>(cfg.reason.data()),
                                   cfg.reason.size()};
        return result_from_rc(
            moq_session_reject_subscribe_tracks(s_, handle.raw(), &c, now));
    }

    /* PUBLISH_BLOCKED (§3.6.3, §9.19): a track under the prefix that this
     * side could not publish, named by its namespace suffix and name. */
    result<void> send_publish_blocked(track_sub_handle handle,
                                      const namespace_name &suffix,
                                      bytes_view track_name, uint64_t now)
    {
        if (suffix.overflowed())
            return errc::invalid;
        moq_namespace_t ns = suffix.c_namespace();
        return result_from_rc(moq_session_send_publish_blocked(
            s_, handle.raw(), &ns, track_name.raw(), now));
    }

    // -- Session close --------------------------------------------------

    /* Close the session with `code` (a session termination code, §12.2).
     * `reason` is not copied: it must outlive the queued close outputs (a
     * string literal is the safe choice); nullptr sends none. */
    result<void> close(uint64_t code, const char *reason, uint64_t now)
    {
        return result_from_rc(moq_session_close(s_, code, reason, now));
    }

    // -- Namespace and track status operations --------------------------

    result<void> goaway(bytes_view uri, uint64_t now)
    {
        return result_from_rc(
            moq_session_goaway(s_, uri.data(), uri.size(), now));
    }

    result<announcement> publish_namespace(
        const publish_namespace_config &cfg, uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_publish_namespace_cfg_t c;
        moq_publish_namespace_cfg_init(&c);
        c.track_namespace  = cfg.ns.c_namespace();
        c.auth_tokens      = cfg.auth_tokens.data();
        c.auth_token_count = cfg.auth_tokens.size();

        moq_announcement_t h{};
        auto rc = moq_session_publish_namespace(s_, &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return announcement(h);
    }

    result<void> publish_namespace_done(announcement ann, uint64_t now)
    {
        return result_from_rc(
            moq_session_publish_namespace_done(s_, ann.raw(), now));
    }

    result<void> accept_namespace(announcement ann, uint64_t now)
    {
        moq_accept_namespace_cfg_t c;
        moq_accept_namespace_cfg_init(&c);
        return result_from_rc(
            moq_session_accept_namespace(s_, ann.raw(), &c, now));
    }

    result<void> reject_namespace(announcement ann,
                                  const reject_namespace_config &cfg,
                                  uint64_t now)
    {
        moq_reject_namespace_cfg_t c;
        moq_reject_namespace_cfg_init(&c);
        c.error_code     = to_c(cfg.error_code);
        c.reason         = bytes_view(cfg.reason).raw();
        c.can_retry      = cfg.can_retry;
        c.retry_after_ms = cfg.retry_after_ms;
        c.redirect.connect_uri     = bytes_view(cfg.redirect.connect_uri).raw();
        c.redirect.track_namespace = cfg.redirect.ns.c_namespace();
        c.redirect.track_name      = cfg.redirect.track.raw();
        return result_from_rc(
            moq_session_reject_namespace(s_, ann.raw(), &c, now));
    }

    result<void> cancel_namespace(announcement ann,
                                  const cancel_namespace_config &cfg,
                                  uint64_t now)
    {
        moq_cancel_namespace_cfg_t c;
        moq_cancel_namespace_cfg_init(&c);
        c.error_code = to_c(cfg.error_code);
        c.reason     = bytes_view(cfg.reason).raw();
        return result_from_rc(
            moq_session_cancel_namespace(s_, ann.raw(), &c, now));
    }

    result<track_status_handle> track_status(const track_status_config &cfg,
                                             uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_track_status_cfg_t c;
        moq_track_status_cfg_init(&c);
        c.track_namespace  = cfg.ns.c_namespace();
        c.track_name       = cfg.track.raw();
        c.auth_tokens      = cfg.auth_tokens.data();
        c.auth_token_count = cfg.auth_tokens.size();
        c.has_include_properties = cfg.include_properties.has_value();
        c.include_properties     = cfg.include_properties.value_or(true);

        moq_track_status_handle_t h{};
        auto rc = moq_session_track_status(s_, &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return track_status_handle(h);
    }

    result<void> accept_track_status(track_status_handle handle,
                                     uint64_t now)
    {
        return accept_track_status(handle, accept_track_status_config{}, now);
    }

    result<void> accept_track_status(track_status_handle handle,
                                     const accept_track_status_config &cfg,
                                     uint64_t now)
    {
        moq_accept_track_status_cfg_t c;
        moq_accept_track_status_cfg_init(&c);
        c.has_largest      = cfg.has_largest;
        c.largest_group    = cfg.largest_group;
        c.largest_object   = cfg.largest_object;
        c.has_expires      = cfg.has_expires;
        c.expires_ms       = cfg.expires_ms;
        c.track_properties = cfg.track_properties.raw();
        return result_from_rc(
            moq_session_accept_track_status(s_, handle.raw(), &c, now));
    }

    result<void> reject_track_status(track_status_handle handle,
                                     const reject_track_status_config &cfg,
                                     uint64_t now)
    {
        moq_reject_track_status_cfg_t c;
        moq_reject_track_status_cfg_init(&c);
        c.error_code     = to_c(cfg.error_code);
        c.reason         = bytes_view(cfg.reason).raw();
        c.can_retry      = cfg.can_retry;
        c.retry_after_ms = cfg.retry_after_ms;
        c.redirect.connect_uri     = bytes_view(cfg.redirect.connect_uri).raw();
        c.redirect.track_namespace = cfg.redirect.ns.c_namespace();
        c.redirect.track_name      = cfg.redirect.track.raw();
        return result_from_rc(
            moq_session_reject_track_status(s_, handle.raw(), &c, now));
    }

    // -- Namespace subscription operations ------------------------------

    result<ns_sub_handle> subscribe_namespace(
        const subscribe_namespace_config &cfg, uint64_t now)
    {
        if (cfg.ns_prefix.overflowed())
            return errc::invalid;

        moq_subscribe_namespace_cfg_t c;
        moq_subscribe_namespace_cfg_init(&c);
        c.track_namespace_prefix = cfg.ns_prefix.c_namespace();
        c.namespace_interest     = to_c(cfg.interest);
        c.auth_tokens            = cfg.auth_tokens.data();
        c.auth_token_count       = cfg.auth_tokens.size();

        moq_ns_sub_handle_t h{};
        auto rc = moq_session_subscribe_namespace(s_, &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return ns_sub_handle(h);
    }

    result<void> accept_ns_sub(ns_sub_handle handle, uint64_t now)
    {
        moq_accept_ns_sub_cfg_t c;
        moq_accept_ns_sub_cfg_init(&c);
        return result_from_rc(
            moq_session_accept_ns_sub(s_, handle.raw(), &c, now));
    }

    result<void> reject_ns_sub(ns_sub_handle handle,
                               const reject_ns_sub_config &cfg,
                               uint64_t now)
    {
        moq_reject_ns_sub_cfg_t c;
        moq_reject_ns_sub_cfg_init(&c);
        c.error_code     = to_c(cfg.error_code);
        c.reason         = bytes_view(cfg.reason).raw();
        c.can_retry      = cfg.can_retry;
        c.retry_after_ms = cfg.retry_after_ms;
        c.redirect.connect_uri     = bytes_view(cfg.redirect.connect_uri).raw();
        c.redirect.track_namespace = cfg.redirect.ns.c_namespace();
        c.redirect.track_name      = cfg.redirect.track.raw();
        return result_from_rc(
            moq_session_reject_ns_sub(s_, handle.raw(), &c, now));
    }

    // -- Per-request GOAWAY send (draft-18 §10.4 migration) -------------
#define MOQ_CPP_REQUEST_GOAWAY(method, htype, cfn)                            \
    result<void> method(htype h, const request_goaway_config &cfg,            \
                        uint64_t now)                                         \
    {                                                                         \
        moq_request_goaway_cfg_t c;                                          \
        moq_request_goaway_cfg_init(&c);                                     \
        c.new_session_uri = bytes_view(cfg.new_session_uri).raw();          \
        c.timeout_ms      = cfg.timeout_ms;                                  \
        return result_from_rc(cfn(s_, h.raw(), &c, now));                    \
    }
    MOQ_CPP_REQUEST_GOAWAY(request_goaway_subscribe, subscription,
                           moq_session_request_goaway_subscribe)
    MOQ_CPP_REQUEST_GOAWAY(request_goaway_fetch, fetch_handle,
                           moq_session_request_goaway_fetch)
    MOQ_CPP_REQUEST_GOAWAY(request_goaway_track_status, track_status_handle,
                           moq_session_request_goaway_track_status)
    MOQ_CPP_REQUEST_GOAWAY(request_goaway_namespace, announcement,
                           moq_session_request_goaway_namespace)
    MOQ_CPP_REQUEST_GOAWAY(request_goaway_ns_sub, ns_sub_handle,
                           moq_session_request_goaway_ns_sub)
    MOQ_CPP_REQUEST_GOAWAY(request_goaway_publish, publication,
                           moq_session_request_goaway_publish)
    MOQ_CPP_REQUEST_GOAWAY(request_goaway_subscribe_tracks, track_sub_handle,
                           moq_session_request_goaway_subscribe_tracks)
#undef MOQ_CPP_REQUEST_GOAWAY

    result<void> send_namespace(ns_sub_handle handle,
                                const namespace_name &suffix,
                                uint64_t now)
    {
        if (suffix.overflowed())
            return errc::invalid;
        auto ns = suffix.c_namespace();
        return result_from_rc(
            moq_session_send_namespace(s_, handle.raw(), &ns, now));
    }

    result<void> send_namespace_done(ns_sub_handle handle,
                                     const namespace_name &suffix,
                                     uint64_t now)
    {
        if (suffix.overflowed())
            return errc::invalid;
        auto ns = suffix.c_namespace();
        return result_from_rc(
            moq_session_send_namespace_done(s_, handle.raw(), &ns, now));
    }

    result<void> cancel_namespace_sub(ns_sub_handle handle, uint64_t now)
    {
        return result_from_rc(
            moq_session_cancel_namespace_sub(s_, handle.raw(), now));
    }

    // -- Streaming object operations ------------------------------------

    result<void> begin_object(subgroup_handle sg,
                              const stream_object_config &cfg,
                              uint64_t now)
    {
        return result_from_rc(moq_session_begin_object(
            s_, sg.raw(), cfg.object_id, cfg.payload_length, now));
    }

    result<void> begin_object_ex(subgroup_handle sg, uint64_t object_id,
                                 uint64_t payload_length,
                                 const buffer *properties, uint64_t now)
    {
        moq_begin_object_cfg_t c;
        moq_begin_object_cfg_init(&c);
        c.object_id      = object_id;
        c.payload_length = payload_length;
        c.properties     = properties ? properties->raw() : nullptr;
        return result_from_rc(
            moq_session_begin_object_ex(s_, sg.raw(), &c, now));
    }

    result<void> write_object_data(subgroup_handle sg, const buffer &data,
                                   uint64_t now)
    {
        return result_from_rc(
            moq_session_write_object_data(s_, sg.raw(), data.raw(), now));
    }

    result<void> end_object(subgroup_handle sg, uint64_t now)
    {
        return result_from_rc(moq_session_end_object(s_, sg.raw(), now));
    }

    // -- Observation ---------------------------------------------------

    session_state state() const noexcept
    {
        return session_state_from_c(moq_session_state(s_));
    }

    uint64_t next_deadline_us() const noexcept
    {
        return moq_session_next_deadline_us(s_);
    }

    /* What the negotiated draft allows, asked instead of testing a version:
     * a fill on a subscription, the joining forms of FETCH, and several
     * concurrent subscriptions to one track. */
    bool supports_fill() const noexcept { return moq_session_supports_fill(s_); }
    bool supports_joining_fetch() const noexcept
    {
        return moq_session_supports_joining_fetch(s_);
    }
    bool supports_concurrent_subscriptions() const noexcept
    {
        return moq_session_supports_concurrent_subscriptions(s_);
    }

    /* Versions this build can offer a peer, newest first. */
    static bool version_supported(moq_version_t v) noexcept
    {
        return moq_version_supported(v);
    }
    static std::vector<moq_version_t> versions_supported()
    {
        std::vector<moq_version_t> out(moq_versions_supported(nullptr, 0));
        out.resize(moq_versions_supported(out.data(), out.size()));
        return out;
    }

    // The wire profile this session was created for; fixed for its lifetime.
    // Not a negotiator -- see moq_session_version() in <moq/session.h>.
    // Zero on a moved-from session (raw handle null).
    moq_version_t version() const noexcept
    {
        return moq_session_version(s_);
    }

    // -- Raw access ----------------------------------------------------

    moq_session_t       *raw() noexcept { return s_; }
    const moq_session_t *raw() const noexcept { return s_; }
};

} // namespace moq

#endif // MOQ_SESSION_HPP
