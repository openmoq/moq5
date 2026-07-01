#ifndef MOQ_SUBSCRIBER_HPP
#define MOQ_SUBSCRIBER_HPP

#include <moq/subscriber.h>

#include <moq/buffer.hpp>
#include <moq/operations.hpp>
#include <moq/result.hpp>
#include <moq/session.hpp>

#include <cstring>
#include <optional>
#include <span>

namespace moq {

class subscriber_track {
    moq_sub_track_t *ptr_ = nullptr;

public:
    subscriber_track() = default;
    explicit subscriber_track(moq_sub_track_t *p) noexcept : ptr_(p) {}

    bool valid() const noexcept { return ptr_ != nullptr; }

    bool is_active() const noexcept
    {
        return ptr_ && moq_sub_track_is_active(ptr_);
    }

    moq_sub_track_t *raw() const noexcept { return ptr_; }
};

struct subscriber_config {
    const moq_alloc_t *alloc             = nullptr;
    uint32_t           max_tracks        = 0;
    uint32_t           max_objects       = 0;
    uint32_t           max_fetches       = 0;
    uint32_t           max_fetch_items   = 0;
    bool               streaming_objects = false;
    uint32_t           max_chunks        = 0;
};

struct sub_track_config {
    namespace_name   ns;
    bytes_view       track;
    subscribe_filter filter       = subscribe_filter::largest_object;
    bool             has_priority = false;
    uint8_t          priority     = 128;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

class polled_object {
    moq_sub_object_t obj_{};

public:
    explicit polled_object(moq_sub_object_t o) noexcept : obj_(o) {}

    ~polled_object() { moq_sub_object_cleanup(&obj_); }

    polled_object(polled_object &&o) noexcept : obj_(o.obj_)
    {
        std::memset(&o.obj_, 0, sizeof(o.obj_));
    }

    polled_object &operator=(polled_object &&o) noexcept
    {
        if (this != &o) {
            moq_sub_object_cleanup(&obj_);
            obj_ = o.obj_;
            std::memset(&o.obj_, 0, sizeof(o.obj_));
        }
        return *this;
    }

    polled_object(const polled_object &)            = delete;
    polled_object &operator=(const polled_object &) = delete;

    subscriber_track track() const noexcept
    {
        return subscriber_track(obj_.track);
    }

    uint64_t group_id() const noexcept { return obj_.group_id; }
    uint64_t subgroup_id() const noexcept { return obj_.subgroup_id; }
    uint64_t object_id() const noexcept { return obj_.object_id; }
    uint8_t  publisher_priority() const noexcept
    {
        return obj_.publisher_priority;
    }
    object_status status() const noexcept
    {
        return object_status_from_c(obj_.status);
    }
    bool end_of_group() const noexcept { return obj_.end_of_group; }
    bool datagram() const noexcept { return obj_.datagram; }

    std::span<const uint8_t> payload() const noexcept
    {
        return {moq_rcbuf_data(obj_.payload), moq_rcbuf_len(obj_.payload)};
    }

    buffer payload_owned() const noexcept
    {
        return obj_.payload ? buffer::retain(obj_.payload) : buffer();
    }

    std::span<const uint8_t> properties() const noexcept
    {
        return {moq_rcbuf_data(obj_.properties),
                moq_rcbuf_len(obj_.properties)};
    }

    buffer properties_owned() const noexcept
    {
        return obj_.properties ? buffer::retain(obj_.properties) : buffer();
    }

    const moq_sub_object_t &raw() const noexcept { return obj_; }
};

class polled_chunk {
    moq_sub_chunk_t chunk_{};

public:
    explicit polled_chunk(moq_sub_chunk_t c) noexcept : chunk_(c) {}

    ~polled_chunk() { moq_sub_chunk_cleanup(&chunk_); }

    polled_chunk(polled_chunk &&o) noexcept : chunk_(o.chunk_)
    {
        std::memset(&o.chunk_, 0, sizeof(o.chunk_));
    }

    polled_chunk &operator=(polled_chunk &&o) noexcept
    {
        if (this != &o) {
            moq_sub_chunk_cleanup(&chunk_);
            chunk_ = o.chunk_;
            std::memset(&o.chunk_, 0, sizeof(o.chunk_));
        }
        return *this;
    }

    polled_chunk(const polled_chunk &)            = delete;
    polled_chunk &operator=(const polled_chunk &) = delete;

    subscriber_track track() const noexcept
    {
        return subscriber_track(chunk_.track);
    }

    uint64_t        group_id() const noexcept { return chunk_.group_id; }
    uint64_t        subgroup_id() const noexcept { return chunk_.subgroup_id; }
    uint64_t        object_id() const noexcept { return chunk_.object_id; }
    uint64_t        payload_length() const noexcept { return chunk_.payload_length; }
    uint8_t         publisher_priority() const noexcept { return chunk_.publisher_priority; }
    object_status   status() const noexcept { return object_status_from_c(chunk_.status); }
    object_terminal terminal() const noexcept { return object_terminal_from_c(chunk_.terminal); }
    bool            begin() const noexcept { return chunk_.begin; }
    bool            end() const noexcept { return chunk_.end; }
    bool            end_of_group() const noexcept { return chunk_.end_of_group; }

    std::span<const uint8_t> chunk_data() const noexcept
    {
        return {moq_rcbuf_data(chunk_.chunk), moq_rcbuf_len(chunk_.chunk)};
    }

    buffer chunk_owned() const noexcept
    {
        return chunk_.chunk ? buffer::retain(chunk_.chunk) : buffer();
    }

    std::span<const uint8_t> properties() const noexcept
    {
        return {moq_rcbuf_data(chunk_.properties),
                moq_rcbuf_len(chunk_.properties)};
    }

    buffer properties_owned() const noexcept
    {
        return chunk_.properties ? buffer::retain(chunk_.properties)
                                : buffer();
    }

    const moq_sub_chunk_t &raw() const noexcept { return chunk_; }
};

// -- Fetch request handle (non-owning) ---------------------------------

class sub_fetch_request {
    moq_sub_fetch_req_t *ptr_ = nullptr;

public:
    sub_fetch_request() = default;
    explicit sub_fetch_request(moq_sub_fetch_req_t *p) noexcept : ptr_(p) {}
    bool                  valid() const noexcept { return ptr_ != nullptr; }
    moq_sub_fetch_req_t  *raw() const noexcept { return ptr_; }
};

struct sub_fetch_config {
    namespace_name ns;
    bytes_view     track;
    uint64_t       start_group  = 0;
    uint64_t       start_object = 0;
    uint64_t       end_group    = 0;
    uint64_t       end_object   = 0;
    group_order    group_order  = group_order::default_order;
    bool           has_subscriber_priority = false;
    uint8_t        subscriber_priority     = 128;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct sub_joining_fetch_config {
    subscriber_track track;
    bool             relative         = false;
    uint64_t         joining_start    = 0;
    group_order      group_order      = group_order::default_order;
    bool             has_subscriber_priority = false;
    uint8_t          subscriber_priority     = 128;
};

class polled_fetch_item {
    moq_sub_fetch_item_t item_{};

public:
    explicit polled_fetch_item(moq_sub_fetch_item_t i) noexcept : item_(i) {}

    ~polled_fetch_item() { moq_sub_fetch_item_cleanup(&item_); }

    polled_fetch_item(polled_fetch_item &&o) noexcept : item_(o.item_)
    {
        std::memset(&o.item_, 0, sizeof(o.item_));
    }
    polled_fetch_item &operator=(polled_fetch_item &&o) noexcept
    {
        if (this != &o) {
            moq_sub_fetch_item_cleanup(&item_);
            item_ = o.item_;
            std::memset(&o.item_, 0, sizeof(o.item_));
        }
        return *this;
    }
    polled_fetch_item(const polled_fetch_item &)            = delete;
    polled_fetch_item &operator=(const polled_fetch_item &) = delete;

    sub_fetch_item_kind kind() const noexcept
    {
        return sub_fetch_item_kind_from_c(item_.kind);
    }
    sub_fetch_request request() const noexcept
    {
        return sub_fetch_request(item_.request);
    }

    bool     ok_end_of_track() const noexcept { return item_.u.ok.end_of_track; }
    uint64_t ok_end_group() const noexcept { return item_.u.ok.end_group; }
    uint64_t ok_end_object() const noexcept { return item_.u.ok.end_object; }

    request_error error_code() const noexcept
    {
        return request_error_from_c(item_.u.error.error_code);
    }
    bool     error_can_retry() const noexcept { return item_.u.error.can_retry; }
    uint64_t error_retry_after_ms() const noexcept
    {
        return item_.u.error.retry_after_ms;
    }

    uint64_t obj_group_id() const noexcept { return item_.u.object.group_id; }
    uint64_t obj_object_id() const noexcept { return item_.u.object.object_id; }
    uint8_t  obj_publisher_priority() const noexcept
    {
        return item_.u.object.publisher_priority;
    }
    std::span<const uint8_t> obj_payload() const noexcept
    {
        return {moq_rcbuf_data(item_.u.object.payload),
                moq_rcbuf_len(item_.u.object.payload)};
    }
    buffer obj_payload_owned() const noexcept
    {
        return item_.u.object.payload
                   ? buffer::retain(item_.u.object.payload)
                   : buffer();
    }
    std::span<const uint8_t> obj_properties() const noexcept
    {
        return {moq_rcbuf_data(item_.u.object.properties),
                moq_rcbuf_len(item_.u.object.properties)};
    }
    buffer obj_properties_owned() const noexcept
    {
        return item_.u.object.properties
                   ? buffer::retain(item_.u.object.properties)
                   : buffer();
    }

    fetch_range_kind gap_range_kind() const noexcept
    {
        return fetch_range_kind_from_c(item_.u.gap.range_kind);
    }
    uint64_t gap_group_id() const noexcept { return item_.u.gap.group_id; }
    uint64_t gap_object_id() const noexcept { return item_.u.gap.object_id; }

    const moq_sub_fetch_item_t &raw() const noexcept { return item_; }
};

// -- Track status request handle (non-owning) --------------------------

class sub_status_request {
    moq_sub_status_req_t *ptr_ = nullptr;

public:
    sub_status_request() = default;
    explicit sub_status_request(moq_sub_status_req_t *p) noexcept : ptr_(p) {}
    bool                   valid() const noexcept { return ptr_ != nullptr; }
    moq_sub_status_req_t  *raw() const noexcept { return ptr_; }
};

struct sub_status_config {
    namespace_name ns;
    bytes_view     track;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct sub_status_result {
    sub_status_result_kind       kind;
    sub_status_request           request;
    bool                         has_largest    = false;
    uint64_t                     largest_group  = 0;
    uint64_t                     largest_object = 0;
    bool                         has_expires    = false;
    uint64_t                     expires_ms     = 0;
    request_error                error_code     = request_error::internal_error;
    bool                         can_retry      = false;
    uint64_t                     retry_after_ms = 0;
};

// -- Subscriber --------------------------------------------------------

class subscriber {
    moq_subscriber_t *s_;

    explicit subscriber(moq_subscriber_t *s) noexcept : s_(s) {}

public:
    subscriber() noexcept : s_(nullptr) {}

    static result<subscriber> create(session &s, subscriber_config cfg = {});

    ~subscriber()
    {
        if (s_)
            moq_sub_destroy(s_);
    }

    subscriber(subscriber &&o) noexcept : s_(o.s_) { o.s_ = nullptr; }

    subscriber &operator=(subscriber &&o) noexcept
    {
        if (this != &o) {
            if (s_)
                moq_sub_destroy(s_);
            s_   = o.s_;
            o.s_ = nullptr;
        }
        return *this;
    }

    subscriber(const subscriber &)            = delete;
    subscriber &operator=(const subscriber &) = delete;

    // -- Subscribe -----------------------------------------------------

    result<subscriber_track> subscribe(const sub_track_config &cfg,
                                       uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_sub_track_cfg_t c;
        moq_sub_track_cfg_init(&c);
        c.track_namespace        = cfg.ns.c_namespace();
        c.track_name             = cfg.track.raw();
        c.filter                 = to_c(cfg.filter);
        c.has_subscriber_priority = cfg.has_priority;
        c.subscriber_priority    = cfg.priority;
        c.auth_tokens            = cfg.auth_tokens.data();
        c.auth_token_count       = cfg.auth_tokens.size();

        moq_sub_track_t *t = nullptr;
        auto rc = moq_sub_subscribe(s_, &c, now, &t);
        if (rc < 0)
            return errc_from_result(rc);
        return subscriber_track(t);
    }

    result<void> unsubscribe(subscriber_track track, uint64_t now)
    {
        return result_from_rc(
            moq_sub_unsubscribe(s_, track.raw(), now));
    }

    // -- Event processing ----------------------------------------------

    result<void> tick(uint64_t now)
    {
        return result_from_rc(moq_sub_tick(s_, now));
    }

    result<void> update_subscription(subscriber_track track,
                                     const subscription_update_config &cfg,
                                     uint64_t now)
    {
        moq_sub_update_cfg_t c;
        moq_sub_update_cfg_init(&c);
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority     = cfg.subscriber_priority;
        c.has_forward             = cfg.has_forward;
        c.forward                 = cfg.forward;
        c.has_delivery_timeout    = cfg.has_delivery_timeout;
        c.delivery_timeout_us     = cfg.delivery_timeout_us;
        return result_from_rc(
            moq_sub_update_subscription(s_, track.raw(), &c, now));
    }

    // -- Object poll ---------------------------------------------------

    std::optional<polled_object> poll_object()
    {
        moq_sub_object_t o{};
        auto rc = moq_sub_poll_object(s_, &o);
        if (rc != MOQ_OK)
            return std::nullopt;
        return polled_object(o);
    }

    std::optional<polled_chunk> poll_chunk()
    {
        moq_sub_chunk_t c{};
        auto rc = moq_sub_poll_chunk(s_, &c);
        if (rc != MOQ_OK)
            return std::nullopt;
        return polled_chunk(c);
    }

    // -- Fetch ---------------------------------------------------------

    result<sub_fetch_request> fetch(const sub_fetch_config &cfg,
                                    uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_sub_fetch_cfg_t c;
        moq_sub_fetch_cfg_init(&c);
        c.track_namespace         = cfg.ns.c_namespace();
        c.track_name              = cfg.track.raw();
        c.start_group             = cfg.start_group;
        c.start_object            = cfg.start_object;
        c.end_group               = cfg.end_group;
        c.end_object              = cfg.end_object;
        c.group_order             = to_c(cfg.group_order);
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority     = cfg.subscriber_priority;
        c.auth_tokens             = cfg.auth_tokens.data();
        c.auth_token_count        = cfg.auth_tokens.size();

        moq_sub_fetch_req_t *req = nullptr;
        auto rc = moq_sub_fetch(s_, &c, now, &req);
        if (rc < 0)
            return errc_from_result(rc);
        return sub_fetch_request(req);
    }

    result<sub_fetch_request> joining_fetch(
        const sub_joining_fetch_config &cfg, uint64_t now)
    {
        moq_sub_joining_fetch_cfg_t c;
        moq_sub_joining_fetch_cfg_init(&c);
        c.track                   = cfg.track.raw();
        c.relative                = cfg.relative;
        c.joining_start           = cfg.joining_start;
        c.group_order             = to_c(cfg.group_order);
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority     = cfg.subscriber_priority;

        moq_sub_fetch_req_t *req = nullptr;
        auto rc = moq_sub_joining_fetch(s_, &c, now, &req);
        if (rc < 0)
            return errc_from_result(rc);
        return sub_fetch_request(req);
    }

    result<void> cancel_fetch(sub_fetch_request req, uint64_t now)
    {
        return result_from_rc(moq_sub_cancel_fetch(s_, req.raw(), now));
    }

    std::optional<polled_fetch_item> poll_fetch()
    {
        moq_sub_fetch_item_t item{};
        auto rc = moq_sub_poll_fetch(s_, &item);
        if (rc != MOQ_OK)
            return std::nullopt;
        return polled_fetch_item(item);
    }

    // -- Track status --------------------------------------------------

    result<sub_status_request> track_status(const sub_status_config &cfg,
                                            uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_sub_status_cfg_t c;
        moq_sub_status_cfg_init(&c);
        c.track_namespace  = cfg.ns.c_namespace();
        c.track_name       = cfg.track.raw();
        c.auth_tokens      = cfg.auth_tokens.data();
        c.auth_token_count = cfg.auth_tokens.size();

        moq_sub_status_req_t *req = nullptr;
        auto rc = moq_sub_track_status(s_, &c, now, &req);
        if (rc < 0)
            return errc_from_result(rc);
        return sub_status_request(req);
    }

    std::optional<sub_status_result> poll_status()
    {
        moq_sub_status_result_t r{};
        auto rc = moq_sub_poll_status(s_, &r);
        if (rc != MOQ_OK)
            return std::nullopt;
        return sub_status_result{
            sub_status_result_kind_from_c(r.kind),
            sub_status_request(r.request),
            r.has_largest,
            r.largest_group,
            r.largest_object,
            r.has_expires,
            r.expires_ms,
            request_error_from_c(r.error_code),
            r.can_retry,
            r.retry_after_ms};
    }

    // -- Query ---------------------------------------------------------

    bool is_draining() const noexcept
    {
        return moq_sub_is_draining(s_);
    }

    // -- Raw access ----------------------------------------------------

    moq_subscriber_t       *raw() noexcept { return s_; }
    const moq_subscriber_t *raw() const noexcept { return s_; }
};

inline result<subscriber> subscriber::create(session &s,
                                              subscriber_config cfg)
{
    moq_sub_cfg_t c;
    moq_sub_cfg_init(&c);
    c.max_tracks        = cfg.max_tracks;
    c.max_objects       = cfg.max_objects;
    c.max_fetches       = cfg.max_fetches;
    c.max_fetch_items   = cfg.max_fetch_items;
    c.streaming_objects = cfg.streaming_objects;
    c.max_chunks        = cfg.max_chunks;

    moq_subscriber_t *sub = nullptr;
    auto alloc = cfg.alloc ? cfg.alloc : moq_alloc_default();
    auto rc = moq_sub_create(s.raw(), alloc, &c, &sub);
    if (rc < 0)
        return errc_from_result(rc);
    return subscriber(sub);
}

} // namespace moq

#endif // MOQ_SUBSCRIBER_HPP
