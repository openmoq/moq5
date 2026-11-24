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

enum class sub_track_state : uint32_t {
    pending = MOQ_SUB_TRACK_PENDING,
    active  = MOQ_SUB_TRACK_ACTIVE,
    error   = MOQ_SUB_TRACK_ERROR,
    done    = MOQ_SUB_TRACK_DONE,
};

struct sub_track_location {
    uint64_t group = 0;
    uint64_t object = 0;
};

struct sub_track_result {
    sub_track_state state = sub_track_state::pending;
    bool response_seen = false;
    std::optional<sub_track_location> largest = {};
    std::optional<uint64_t> expires_ms = {};
    buffer track_properties;
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

struct subscriber_options {
    /* Fill request slots and fill-item queue depth; 0 selects the defaults.
     * Reserved only on a draft with fills. */
    uint32_t           max_fills         = 0;
    uint32_t           max_fill_items    = 0;
};

struct sub_track_config {
    namespace_name   ns;
    bytes_view       track;
    subscribe_filter filter       = subscribe_filter::largest_object;
    bool             has_priority = false;
    uint8_t          priority     = 128;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct sub_track_options {
    /* The filter in its semantic form; when set it replaces `filter`. A form
     * the session's draft cannot carry is errc::unsupported. */
    std::optional<location_filter_config> location = {};
    /* A fill carried on the SUBSCRIBE; its items arrive through poll_fill and
     * its handle through track_fill(). errc::unsupported without fills. */
    std::optional<fill_config> fill = {};
    /* The delivery timeouts the subscriber asks for, exact milliseconds
     * (zero included); errc::unsupported on a draft without them. */
    std::optional<uint64_t> object_delivery_timeout_ms   = {};
    std::optional<uint64_t> subgroup_delivery_timeout_ms = {};
    /* The SUBSCRIBE's Forward state; unset sends Forward 1. */
    std::optional<bool> forward = {};
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
    moq::group_order group_order = moq::group_order::default_order;
    bool           has_subscriber_priority = false;
    uint8_t        subscriber_priority     = 128;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct sub_joining_fetch_config {
    subscriber_track track;
    bool             relative         = false;
    uint64_t         joining_start    = 0;
    moq::group_order group_order      = moq::group_order::default_order;
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

    // kind() == reset: the peer's STREAM error code (not a request error) and
    // which stream of the fetch carried the terminal.
    uint64_t reset_error_code() const noexcept { return item_.u.reset.error_code; }
    bool     reset_data_stream() const noexcept { return item_.u.reset.data_stream; }
    bool     reset_stop_sending() const noexcept { return item_.u.reset.stop_sending; }

    const moq_sub_fetch_item_t &raw() const noexcept { return item_; }
};

// -- Fill items (draft-22 section 3.4) ----------------------------------

/* One item of a fill the subscriber requested. Move-only RAII over
 * moq_sub_fill_item_t: the object payload/properties refs are owned by the
 * item until it is destroyed (the *_owned() accessors retain them past it).
 * The track is borrowed until track release or subscriber destruction; owning
 * an item does not extend that lifetime. Buffers survive both. A created item
 * is the authoritative child observation, not the track's latest-fill query.
 * After a local delivery failure, slot retirement also waits for the local
 * notice, accepted items and native retirement. Poll local failures as well
 * as fill items; the handle becomes stale only when the slot is released. */
class polled_fill_item {
    moq_sub_fill_item_t item_{};

public:
    explicit polled_fill_item(moq_sub_fill_item_t i) noexcept : item_(i) {}

    ~polled_fill_item() { moq_sub_fill_item_cleanup(&item_); }

    polled_fill_item(polled_fill_item &&o) noexcept : item_(o.item_)
    {
        std::memset(&o.item_, 0, sizeof(o.item_));
    }
    polled_fill_item &operator=(polled_fill_item &&o) noexcept
    {
        if (this != &o) {
            moq_sub_fill_item_cleanup(&item_);
            item_ = o.item_;
            std::memset(&o.item_, 0, sizeof(o.item_));
        }
        return *this;
    }
    polled_fill_item(const polled_fill_item &)            = delete;
    polled_fill_item &operator=(const polled_fill_item &) = delete;

    sub_fill_item_kind kind() const noexcept
    {
        return sub_fill_item_kind_from_c(item_.kind);
    }
    sub_fill_handle  request() const noexcept { return sub_fill_handle(item_.request); }
    subscriber_track track() const noexcept { return subscriber_track(item_.track); }
    // Present on every kind; zero is a valid initiating request ID.
    uint64_t initiating_request_id() const noexcept { return item_.initiating_request_id; }

    uint64_t obj_group_id() const noexcept { return item_.u.object.group_id; }
    uint64_t obj_subgroup_id() const noexcept { return item_.u.object.subgroup_id; }
    uint64_t obj_object_id() const noexcept { return item_.u.object.object_id; }
    uint8_t  obj_publisher_priority() const noexcept
    {
        return item_.u.object.publisher_priority;
    }
    bool     obj_datagram() const noexcept { return item_.u.object.datagram; }
    std::span<const uint8_t> obj_payload() const noexcept
    {
        return {moq_rcbuf_data(item_.u.object.payload),
                moq_rcbuf_len(item_.u.object.payload)};
    }
    buffer obj_payload_owned() const noexcept
    {
        return item_.u.object.payload ? buffer::retain(item_.u.object.payload)
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
        // Convert directly to the fixed-underlying C++ enum, not a C enum
        // whose valid range may exclude a future fixed-width value.
        return static_cast<fetch_range_kind>(item_.u.gap.range_kind);
    }
    uint64_t gap_group_id() const noexcept { return item_.u.gap.group_id; }
    uint64_t gap_object_id() const noexcept { return item_.u.gap.object_id; }

    // kind() == terminal: the session's reason, the domain its code belongs
    // to, and the code (zero is a valid code when the domain is not none).
    fill_reason       terminal_reason() const noexcept
    {
        return fill_reason_from_c(item_.u.terminal.reason);
    }
    fill_error_domain terminal_error_domain() const noexcept
    {
        return fill_error_domain_from_c(item_.u.terminal.error_domain);
    }
    uint64_t          terminal_error_code() const noexcept
    {
        return item_.u.terminal.error_code;
    }

    const moq_sub_fill_item_t &raw() const noexcept { return item_; }
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

enum class sub_status_outcome_kind : uint32_t {
    ok = MOQ_SUB_STATUS_OUTCOME_OK,
    error = MOQ_SUB_STATUS_OUTCOME_ERROR,
    terminated = MOQ_SUB_STATUS_OUTCOME_TERMINATED,
};

/* Identifier-only request correlation has the native slot lifetime. Polling
 * retires the slot; do not use it as a persistent unique operation identity. */
struct sub_status_outcome {
    sub_status_outcome_kind kind{};
    sub_status_request request{};
    std::optional<sub_track_location> largest{};
    std::optional<uint64_t> expires_ms{};
    request_error error_code{};
    bool can_retry = false;
    uint64_t retry_after_ms = 0;
    std::optional<track_status_terminal_cause> cause{};
    std::optional<request_role> local_role{};
    uint64_t stream_error_code = 0;

    static result<sub_status_outcome> from_native(const void *data, size_t size)
    {
        if (!data || size < sizeof(uint32_t)) return errc::abi_mismatch;
        uint32_t stamped = 0;
        std::memcpy(&stamped, data, sizeof(stamped));
        if (size < MOQ_SUB_STATUS_OUTCOME_V0_SIZE || stamped < MOQ_SUB_STATUS_OUTCOME_V0_SIZE)
            return errc::abi_mismatch;
        moq_sub_status_outcome_t c{};
        std::memcpy(&c, data, MOQ_SUB_STATUS_OUTCOME_V0_SIZE);
        sub_status_outcome v;
        v.kind = static_cast<sub_status_outcome_kind>(c.kind);
        v.request = sub_status_request(c.request);
        if (c.kind == MOQ_SUB_STATUS_OUTCOME_OK) {
            if (c.flags & MOQ_SUB_STATUS_OUTCOME_HAS_LARGEST)
                v.largest = sub_track_location{c.largest_group, c.largest_object};
            if (c.flags & MOQ_SUB_STATUS_OUTCOME_HAS_EXPIRES) v.expires_ms = c.expires_ms;
        } else if (c.kind == MOQ_SUB_STATUS_OUTCOME_ERROR) {
            v.error_code = request_error_from_c(c.error_code);
            v.can_retry = (c.flags & MOQ_SUB_STATUS_OUTCOME_CAN_RETRY) != 0;
            v.retry_after_ms = c.retry_after_ms;
        } else if (c.kind == MOQ_SUB_STATUS_OUTCOME_TERMINATED) {
            v.cause = static_cast<track_status_terminal_cause>(c.cause);
            v.local_role = static_cast<request_role>(c.local_role);
            v.stream_error_code = c.stream_error_code;
        }
        return v;
    }
};

enum class sub_local_failure_owner_kind : uint32_t {
    track = MOQ_SUB_LOCAL_FAILURE_TRACK,
    fetch = MOQ_SUB_LOCAL_FAILURE_FETCH,
    fill = MOQ_SUB_LOCAL_FAILURE_FILL,
};

/* Value-only local delivery observation, not a peer error or owner transfer.
 * Track/FILL correlation does not extend native lifetime. FETCH is only a
 * slot identifier, which a later request may reuse; never dereference it.
 * Inactive alternatives are absent, and unknown owner/result values survive. */
struct sub_local_failure {
    sub_local_failure_owner_kind owner_kind{};
    int32_t local_result = 0;
    std::optional<subscriber_track> track{};
    std::optional<uintptr_t> fetch_id{};
    std::optional<sub_fill_handle> fill{};
    std::optional<uint64_t> initiating_request_id{};

    static result<sub_local_failure> from_native(const void *data, size_t size)
    {
        if (!data || size < sizeof(uint32_t)) return errc::abi_mismatch;
        uint32_t stamped = 0;
        std::memcpy(&stamped, data, sizeof(stamped));
        if (size < MOQ_SUB_LOCAL_FAILURE_V0_SIZE || stamped < MOQ_SUB_LOCAL_FAILURE_V0_SIZE)
            return errc::abi_mismatch;
        moq_sub_local_failure_t c{};
        std::memcpy(&c, data, MOQ_SUB_LOCAL_FAILURE_V0_SIZE);
        sub_local_failure v;
        v.owner_kind = static_cast<sub_local_failure_owner_kind>(c.owner_kind);
        v.local_result = c.local_result;
        if (c.owner_kind == MOQ_SUB_LOCAL_FAILURE_TRACK || c.owner_kind == MOQ_SUB_LOCAL_FAILURE_FILL) {
            if (c.track) v.track = subscriber_track(c.track);
        }
        if (c.owner_kind == MOQ_SUB_LOCAL_FAILURE_FETCH && c.fetch)
            v.fetch_id = reinterpret_cast<uintptr_t>(c.fetch);
        if (c.owner_kind == MOQ_SUB_LOCAL_FAILURE_FILL) {
            if (moq_sub_fill_is_valid(c.fill)) v.fill = sub_fill_handle(c.fill);
            v.initiating_request_id = c.initiating_request_id;
        }
        return v;
    }
};

// -- Subscriber --------------------------------------------------------

class subscriber {
    moq_subscriber_t *s_;
    moq_alloc_t alloc_{};

    subscriber(moq_subscriber_t *s, const moq_alloc_t &alloc) noexcept
        : s_(s), alloc_(alloc) {}

public:
    subscriber() noexcept : s_(nullptr) {}

    static result<subscriber> create(session &s, subscriber_config cfg = {});
    static result<subscriber> create(session &s, subscriber_config cfg,
                                      const subscriber_options &options);

    ~subscriber()
    {
        if (s_)
            moq_sub_destroy(s_);
    }

    subscriber(subscriber &&o) noexcept : s_(o.s_), alloc_(o.alloc_) { o.s_ = nullptr; }

    subscriber &operator=(subscriber &&o) noexcept
    {
        if (this != &o) {
            if (s_)
                moq_sub_destroy(s_);
            s_   = o.s_;
            alloc_ = o.alloc_;
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
        return subscribe(cfg, sub_track_options{}, now);
    }

    result<subscriber_track> subscribe(const sub_track_config &cfg,
                                        const sub_track_options &options, uint64_t now)
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
        /* Location and fill live in wrapper storage for the call. */
        moq_location_filter_cfg_t loc_c, fill_loc_c;
        moq_fill_cfg_t            fill_c;
        c.location = detail::location_to_c(options.location, loc_c);
        c.fill     = detail::fill_to_c(options.fill, fill_c, fill_loc_c);
        /* Borrowed for the call from the options' own storage. */
        if (options.object_delivery_timeout_ms)
            c.object_delivery_timeout_ms = &*options.object_delivery_timeout_ms;
        if (options.subgroup_delivery_timeout_ms)
            c.subgroup_delivery_timeout_ms = &*options.subgroup_delivery_timeout_ms;
        if (options.forward)
            c.forward = &*options.forward;

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
        return update_subscription(track, cfg, update_options{}, now);
    }

    result<void> update_subscription(subscriber_track track,
                                      const subscription_update_config &cfg,
                                      const update_options &options, uint64_t now)
    {
        moq_sub_update_cfg_t c;
        /* Sized init: the filter block is an appended tail; the frozen
         * pointer-only init would drop it. Result callbacks are
         * deliberately NOT exposed by this binding (it has no callback
         * plumbing); the facade drains acknowledgment events internally,
         * and a rejected update surfaces through the C on_subscribe_done
         * path with the update-failure status. This binding supports
         * FILTER SENDING only. */
        moq_sub_update_cfg_init_sized(&c, sizeof(c));
        c.has_subscriber_priority = cfg.has_subscriber_priority;
        c.subscriber_priority     = cfg.subscriber_priority;
        c.has_forward             = cfg.has_forward;
        c.forward                 = cfg.forward;
        c.has_delivery_timeout    = cfg.has_delivery_timeout;
        c.delivery_timeout_us     = cfg.delivery_timeout_us;
        c.has_filter              = cfg.has_filter;
        c.filter                  = cfg.filter;
        c.start_group             = cfg.start_group;
        c.start_object            = cfg.start_object;
        c.end_group               = cfg.end_group;
        /* A fill carried on the update; its handle is then track_fill(). */
        moq_location_filter_cfg_t fill_loc_c, loc_c;
        moq_fill_cfg_t            fill_c;
        c.fill = detail::fill_to_c(options.fill, fill_c, fill_loc_c);
        /* The replacement filter in its semantic form and the independent
         * delivery timeouts (errc::unsupported where the draft lacks them). */
        c.location = detail::location_to_c(options.location, loc_c);
        if (options.object_delivery_timeout_ms) {
            c.delivery_timeout_flags |= MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT;
            c.object_delivery_timeout_ms = *options.object_delivery_timeout_ms;
        }
        if (options.subgroup_delivery_timeout_ms) {
            c.delivery_timeout_flags |= MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT;
            c.subgroup_delivery_timeout_ms = *options.subgroup_delivery_timeout_ms;
        }
        return result_from_rc(
            moq_sub_update_subscription(s_, track.raw(), &c, now));
    }

    // -- Fills (draft-22 section 3.4) ------------------------------------

    /* The newest fill requested on `track` whose slot is not yet released.
     * A local notice or unsettled native retirement can keep it present after
     * terminal polling. Read right after the request for that request's handle.
     * Invalid when none. */
    sub_fill_handle track_fill(subscriber_track track) const noexcept
    {
        return sub_fill_handle(moq_sub_track_fill(track.raw()));
    }

    /* Stop one fill without touching the subscription; its terminal item
     * (reason local_cancelled) still arrives through poll_fill. */
    result<void> cancel_fill(sub_fill_handle fill, uint64_t now)
    {
        return result_from_rc(moq_sub_cancel_fill(s_, fill.raw(), now));
    }

    std::optional<polled_fill_item> poll_fill()
    {
        moq_sub_fill_item_t item{};
        auto rc = moq_sub_poll_fill(s_, &item, sizeof(item));
        if (rc != MOQ_OK)
            return std::nullopt;
        return polled_fill_item(item);
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

    /* Same bounded ring as poll_status(). The legacy method returns an empty
     * optional at a TERMINATED head without consuming it; use this method for
     * explicit D22 operation. Empty is distinct from a native/ABI error. */
    result<std::optional<sub_status_outcome>> poll_status_outcome()
    {
        moq_sub_status_outcome_t c{};
        auto rc = moq_sub_poll_status_outcome(s_, &c, sizeof(c));
        if (rc == MOQ_DONE) return std::optional<sub_status_outcome>{};
        if (rc < 0) return errc_from_result(rc);
        auto view = sub_status_outcome::from_native(&c, sizeof(c));
        if (!view) return view.error();
        return std::optional<sub_status_outcome>{std::move(view).value()};
    }

    /* Consume one notice on the session executor, even after a terminal item.
     * Empty is distinct from native/ABI error. This non-advancing poll can
     * finish FETCH/FILL retirement immediately; returned correlation never
     * authorizes dereferencing a retired owner. No refs or extra queue. */
    result<std::optional<sub_local_failure>> poll_local_failure()
    {
        moq_sub_local_failure_t c{};
        auto rc = moq_sub_poll_local_failure(s_, &c, sizeof(c));
        if (rc == MOQ_DONE) return std::optional<sub_local_failure>{};
        if (rc < 0) return errc_from_result(rc);
        auto view = sub_local_failure::from_native(&c, sizeof(c));
        if (!view) return view.error();
        return std::optional<sub_local_failure>{std::move(view).value()};
    }

    // -- Query ---------------------------------------------------------

    /* Observes current state and immutable initial ACK facts without ticking
     * or polling. An ACK with empty properties has response_seen == true;
     * before ACK it is false. Native query and local copy failures are errors.
     *
     * Nonempty properties are independently copied with the subscriber's
     * configured allocator (or the default). Empty properties allocate nothing.
     * The result outlives track release and subscriber destruction, provided the
     * allocator context outlives every result buffer. Call on the session
     * executor; buffer copies/refcounts remain same-shard, not a thread-safe
     * ownership transfer. */
    result<sub_track_result> get_track_result(subscriber_track track) const
    {
        moq_sub_track_result_t c{};
        auto rc = moq_sub_get_track_result(s_, track.raw(), &c, sizeof(c));
        if (rc < 0)
            return errc_from_result(rc);

        sub_track_result out{};
        out.state = static_cast<sub_track_state>(c.state);
        out.response_seen = (c.flags & MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN) != 0;
        if (c.flags & MOQ_SUB_TRACK_RESULT_HAS_LARGEST)
            out.largest = sub_track_location{c.largest_group, c.largest_object};
        if (c.flags & MOQ_SUB_TRACK_RESULT_HAS_EXPIRES)
            out.expires_ms = c.expires_ms;
        if (c.track_properties.len != 0) {
            auto copy = buffer::create(c.track_properties.data, c.track_properties.len, &alloc_);
            if (!copy)
                return copy.error();
            out.track_properties = std::move(*copy);
        }
        return out;
    }

    /* Where the track went when a REDIRECT or a per-request GOAWAY ended
     * it; nullopt when it did not end by migration. */
    result<std::optional<request_migration>> get_track_migration(
        subscriber_track track) const
    {
        moq_request_migration_t m;
        auto rc = moq_sub_get_track_migration(s_, track.raw(), &m, sizeof(m));
        if (rc == MOQ_DONE)
            return std::optional<request_migration>{};
        if (rc < 0)
            return errc_from_result(rc);
        return std::optional<request_migration>{migration_from_c(m)};
    }

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
    return create(s, cfg, subscriber_options{});
}

inline result<subscriber> subscriber::create(session &s, subscriber_config cfg,
                                              const subscriber_options &options)
{
    moq_sub_cfg_t c;
    /* Sized init: max_fills / max_fill_items are past the frozen prefix. */
    moq_sub_cfg_init_sized(&c, sizeof(c));
    c.max_tracks        = cfg.max_tracks;
    c.max_objects       = cfg.max_objects;
    c.max_fetches       = cfg.max_fetches;
    c.max_fetch_items   = cfg.max_fetch_items;
    c.streaming_objects = cfg.streaming_objects;
    c.max_chunks        = cfg.max_chunks;
    c.max_fills         = options.max_fills;
    c.max_fill_items    = options.max_fill_items;

    moq_subscriber_t *sub = nullptr;
    auto alloc = cfg.alloc ? cfg.alloc : moq_alloc_default();
    auto rc = moq_sub_create(s.raw(), alloc, &c, &sub);
    if (rc < 0)
        return errc_from_result(rc);
    return subscriber(sub, *alloc);
}

} // namespace moq

#endif // MOQ_SUBSCRIBER_HPP
