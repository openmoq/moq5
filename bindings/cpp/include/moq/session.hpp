#ifndef MOQ_SESSION_HPP
#define MOQ_SESSION_HPP

#include <moq/action.hpp>
#include <moq/event.hpp>
#include <moq/operations.hpp>
#include <moq/result.hpp>

#include <optional>
#include <span>

namespace moq {

struct session_config {
    perspective        perspective              = perspective::client;
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
    uint64_t           goaway_timeout_us        = 0;
    bool               streaming_objects        = false;
    bool               send_auth_token_cache_size = false;
    uint64_t           auth_token_cache_size    = 0;
    moq_version_t      version                  = {};
};

class session {
    moq_session_t *s_;

    explicit session(moq_session_t *s) noexcept : s_(s) {}

public:
    session() noexcept : s_(nullptr) {}

    static result<session> create(session_config cfg, uint64_t now = 0)
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
        c.goaway_timeout_us        = cfg.goaway_timeout_us;
        c.streaming_objects        = cfg.streaming_objects;
        c.send_auth_token_cache_size = cfg.send_auth_token_cache_size;
        c.auth_token_cache_size    = cfg.auth_token_cache_size;
        c.version                  = cfg.version;

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

    // -- Subscribe operations ------------------------------------------

    result<subscription> subscribe(const subscribe_config &cfg, uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_subscribe_cfg_t c;
        moq_subscribe_cfg_init(&c);
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
        moq_subscription_update_cfg_t c;
        moq_subscription_update_cfg_init(&c);
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
        return result_from_rc(
            moq_session_update_subscription(s_, sub.raw(), &c, now));
    }

    // -- Fetch operations -----------------------------------------------

    result<fetch_handle> fetch(const fetch_config &cfg, uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_fetch_cfg_t c;
        moq_fetch_cfg_init(&c);
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
        c.joining_start          = cfg.joining_start;
        c.auth_tokens            = cfg.auth_tokens.data();
        c.auth_token_count       = cfg.auth_tokens.size();

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

    result<void> fetch_cancel(fetch_handle fetch, uint64_t now)
    {
        return result_from_rc(moq_session_fetch_cancel(s_, fetch.raw(), now));
    }

    // -- Publish operations ---------------------------------------------

    result<publication> publish(const publish_config &cfg, uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_publish_cfg_t c;
        moq_publish_cfg_init(&c);
        c.track_namespace  = cfg.ns.c_namespace();
        c.track_name       = cfg.track.raw();
        c.has_track_alias  = cfg.has_track_alias;
        c.track_alias      = cfg.track_alias;
        c.has_forward      = cfg.has_forward;
        c.forward          = cfg.forward;
        c.track_properties = cfg.track_properties.raw();
        c.auth_tokens      = cfg.auth_tokens.data();
        c.auth_token_count = cfg.auth_tokens.size();

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
        moq_accept_publish_cfg_t c;
        moq_accept_publish_cfg_init(&c);
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority     = cfg.subscriber_priority;
        c.group_order             = to_c(cfg.group_order);
        c.has_new_group_request   = cfg.has_new_group_request;
        c.new_group_request       = cfg.new_group_request;
        return result_from_rc(
            moq_session_accept_publish(s_, pub.raw(), &c, now));
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

        moq_track_status_handle_t h{};
        auto rc = moq_session_track_status(s_, &c, now, &h);
        if (rc < 0)
            return errc_from_result(rc);
        return track_status_handle(h);
    }

    result<void> accept_track_status(track_status_handle handle,
                                     uint64_t now)
    {
        moq_accept_track_status_cfg_t c;
        moq_accept_track_status_cfg_init(&c);
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

    // -- Raw access ----------------------------------------------------

    moq_session_t       *raw() noexcept { return s_; }
    const moq_session_t *raw() const noexcept { return s_; }
};

} // namespace moq

#endif // MOQ_SESSION_HPP
