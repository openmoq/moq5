#ifndef MOQ_PUBLISHER_HPP
#define MOQ_PUBLISHER_HPP

#include <moq/publisher.h>

#include <moq/buffer.hpp>
#include <moq/result.hpp>
#include <moq/session.hpp>

#include <cstring>
#include <optional>
#include <span>

namespace moq {

enum class pub_accept_mode {
    reject_all = MOQ_PUB_REJECT_ALL,
    accept_all = MOQ_PUB_ACCEPT_ALL,
};

class publisher_track {
    moq_pub_track_t *ptr_ = nullptr;

public:
    publisher_track() = default;
    explicit publisher_track(moq_pub_track_t *p) noexcept : ptr_(p) {}

    bool              valid() const noexcept { return ptr_ != nullptr; }
    moq_pub_track_t  *raw() const noexcept { return ptr_; }
};

struct publisher_config {
    const moq_alloc_t *alloc                      = nullptr;
    // Fail-closed by default (matches the C facade's moq_pub_cfg_init): a
    // default publisher rejects remote subscribers. Public broadcasters must
    // opt in explicitly with pub_accept_mode::accept_all.
    pub_accept_mode    accept_mode                 = pub_accept_mode::reject_all;
    uint8_t            default_publisher_priority  = 128;
};

struct publisher_options {
    /* Fill requests the facade can hold in flight at once; 0 selects 16.
     * Never fewer than the session's fill pool. */
    uint32_t           max_fills                   = 0;
};

enum class pub_namespace_terminal_kind : uint32_t {
    rejected = MOQ_PUB_NAMESPACE_REJECTED,
    cancelled = MOQ_PUB_NAMESPACE_CANCELLED,
    terminated = MOQ_PUB_NAMESPACE_TERMINATED,
};

/* Namespace parts and reason are borrowed only for the native callback.
 * The physical extent and producer stamp must cover every observed scalar. */
struct pub_namespace_terminal_view {
    pub_namespace_terminal_kind kind{};
    std::span<const moq_bytes_t> namespace_parts{};
    request_error error_code{};
    bool can_retry = false;
    uint64_t retry_after_ms = 0;
    bytes_view reason{};
    std::optional<namespace_terminal_cause> cause{};
    std::optional<request_role> local_role{};
    uint64_t stream_error_code = 0;

    static result<pub_namespace_terminal_view> from_native(const void *data, size_t size)
    {
        if (!data || size < sizeof(uint32_t)) return errc::abi_mismatch;
        uint32_t stamped = 0;
        std::memcpy(&stamped, data, sizeof(stamped));
        size_t n = size < stamped ? size : stamped;
        if (n < MOQ_PUB_NAMESPACE_TERMINAL_INFO_V0_SIZE) return errc::abi_mismatch;
        moq_pub_namespace_terminal_info_t c{};
        std::memcpy(&c, data, n < sizeof(c) ? n : sizeof(c));
        if (c.kind == MOQ_PUB_NAMESPACE_TERMINATED &&
            n < offsetof(moq_pub_namespace_terminal_info_t, stream_error_code) + sizeof(c.stream_error_code))
            return errc::abi_mismatch;
        pub_namespace_terminal_view v;
        v.kind = static_cast<pub_namespace_terminal_kind>(c.kind);
        v.namespace_parts = {c.namespace_.parts, c.namespace_.count};
        if (c.kind == MOQ_PUB_NAMESPACE_TERMINATED) {
            v.cause = static_cast<namespace_terminal_cause>(c.cause);
            v.local_role = static_cast<request_role>(c.local_role);
            v.stream_error_code = c.stream_error_code;
        } else {
            v.error_code = request_error_from_c(c.error_code);
            v.can_retry = c.can_retry;
            v.retry_after_ms = c.retry_after_ms;
            v.reason = bytes_view(c.reason);
        }
        return v;
    }
};

/* A stable, caller-owned observer. It and ctx must outlive the publisher;
 * do not move it or mutate it during a callback. Callbacks must not reenter
 * libmoq. Moving the publisher itself does not move this context. */
/* A subscriber asked for a new group on a track that advertises
 * DYNAMIC_GROUPS, with a value that warrants one (draft-22 §9.20.19): the
 * value it sent (0: it knows no group) and the track's current group, when
 * the track has published one. */
struct pub_new_group_request_view {
    uint64_t                requested_group = 0;
    std::optional<uint64_t> current_group   = {};

    static result<pub_new_group_request_view> from_native(const void *data, size_t size)
    {
        if (!data || size < sizeof(uint32_t)) return errc::abi_mismatch;
        uint32_t stamped = 0;
        std::memcpy(&stamped, data, sizeof(stamped));
        size_t n = size < stamped ? size : stamped;
        if (n < offsetof(moq_pub_new_group_request_info_t, current_group) +
                sizeof(uint64_t))
            return errc::abi_mismatch;
        moq_pub_new_group_request_info_t c{};
        std::memcpy(&c, data, n < sizeof(c) ? n : sizeof(c));
        pub_new_group_request_view v;
        v.requested_group = c.requested_group;
        if (c.has_current_group) v.current_group = c.current_group;
        return v;
    }
};

struct publisher_observer {
    void *ctx = nullptr;
    void (*on_namespace_terminal)(void *, result<pub_namespace_terminal_view>) noexcept = nullptr;
    void (*on_new_group_request)(void *, publisher_track,
                                 result<pub_new_group_request_view>) noexcept = nullptr;
    /* The session-wide GOAWAY's New Session URI, borrowed for the call
     * (empty: reconnect to the current URI). */
    void (*on_goaway)(void *, std::span<const uint8_t>) noexcept = nullptr;
};

struct pub_track_config {
    namespace_name ns;
    bytes_view     track;
    bool           advertise_namespace     = false;
    bool           has_publisher_priority   = false;
    uint8_t        publisher_priority      = 128;
    uint64_t       max_retained_bytes      = 0;  // retained-group budget; 0 = 1 MiB
    /* The track's Track Properties in the session's encoding, stated on
     * every answer for it; dynamic_groups adds DYNAMIC_GROUPS = 1. */
    bytes_view     track_properties        = {};
    bool           dynamic_groups          = false;
};

// One object of a retained catalog group (origin-local Joining-FETCH cache).
struct pub_retained_object {
    uint64_t       object_id    = 0;
    const buffer  *payload      = nullptr;  // required; retained via incref
    const buffer  *properties   = nullptr;  // optional; retained via incref
    bool           end_of_group = false;
};

struct pub_retained_group_config {
    uint64_t                   group_id     = 0;   // shared by all objects
    const pub_retained_object *objects      = nullptr;  // ascending 0..N
    size_t                     object_count = 0;
};

struct pub_object_config {
    uint64_t       group_id           = 0;
    uint64_t       object_id          = 0;
    const buffer  *payload            = nullptr;
    const buffer  *properties         = nullptr;
    bool           datagram           = false;
    bool           has_status         = false;
    object_status  status             = object_status::normal;
    bool           end_of_group       = false;
};

class publisher {
    moq_publisher_t *p_;

    explicit publisher(moq_publisher_t *p) noexcept : p_(p) {}

public:
    publisher() noexcept : p_(nullptr) {}

    static result<publisher> create(session &s, publisher_config cfg = {});
    static result<publisher> create(session &s, publisher_config cfg,
                                     const publisher_options &options);
    static result<publisher> create_observed(session &s, publisher_observer &observer,
        publisher_config cfg = {}, const publisher_options &options = {});

    ~publisher()
    {
        if (p_)
            moq_pub_destroy(p_);
    }

    publisher(publisher &&o) noexcept : p_(o.p_) { o.p_ = nullptr; }

    publisher &operator=(publisher &&o) noexcept
    {
        if (this != &o) {
            if (p_)
                moq_pub_destroy(p_);
            p_   = o.p_;
            o.p_ = nullptr;
        }
        return *this;
    }

    publisher(const publisher &)            = delete;
    publisher &operator=(const publisher &) = delete;

    // -- Track management ----------------------------------------------

    result<publisher_track> add_track(const pub_track_config &cfg,
                                      uint64_t now)
    {
        if (cfg.ns.overflowed())
            return errc::invalid;

        moq_pub_track_cfg_t c;
        /* Sized init: this binding always copies the appended fields
         * (has_publisher_priority, max_retained_bytes), so the cfg must
         * advertise the full current size for add_track to honor them. The
         * later-appended monotonic_groups stays at its zeroed default (no
         * C++ knob; opting in is a C-level cfg decision). */
        moq_pub_track_cfg_init_sized(&c, sizeof(c));
        c.track_namespace        = cfg.ns.c_namespace();
        c.track_name             = cfg.track.raw();
        c.advertise_namespace    = cfg.advertise_namespace;
        c.has_publisher_priority = cfg.has_publisher_priority;
        c.publisher_priority     = cfg.publisher_priority;
        c.max_retained_bytes     = cfg.max_retained_bytes;
        c.track_properties       = cfg.track_properties.raw();
        c.dynamic_groups         = cfg.dynamic_groups;

        moq_pub_track_t *t = nullptr;
        auto rc = moq_pub_add_track(p_, &c, now, &t);
        if (rc < 0)
            return errc_from_result(rc);
        return publisher_track(t);
    }

    result<void> remove_track(publisher_track track, uint64_t now)
    {
        return result_from_rc(moq_pub_remove_track(p_, track.raw(), now));
    }

    // -- Retained group (origin-local Joining-FETCH cache) -------------

    result<void> set_retained_group(publisher_track track,
                                    const pub_retained_group_config &cfg)
    {
        if (cfg.object_count == 0 ||
            cfg.object_count > MOQ_PUB_RETAINED_MAX_OBJECTS)
            return errc::invalid;
        moq_pub_retained_object_t objs[MOQ_PUB_RETAINED_MAX_OBJECTS];
        for (size_t i = 0; i < cfg.object_count; ++i) {
            objs[i].object_id    = cfg.objects[i].object_id;
            objs[i].payload      = cfg.objects[i].payload
                                       ? cfg.objects[i].payload->raw() : nullptr;
            objs[i].properties   = cfg.objects[i].properties
                                       ? cfg.objects[i].properties->raw() : nullptr;
            objs[i].end_of_group = cfg.objects[i].end_of_group;
        }
        moq_pub_retained_group_cfg_t c;
        moq_pub_retained_group_cfg_init(&c);
        c.group_id     = cfg.group_id;
        c.objects      = objs;
        c.object_count = cfg.object_count;
        return result_from_rc(
            moq_pub_set_retained_group(p_, track.raw(), &c));
    }

    result<void> clear_retained_group(publisher_track track)
    {
        return result_from_rc(
            moq_pub_clear_retained_group(p_, track.raw()));
    }

    // -- Object writing ------------------------------------------------

    result<void> write_object(publisher_track track, uint64_t group_id,
                              uint64_t object_id, const buffer &payload,
                              uint64_t now)
    {
        return result_from_rc(moq_pub_write_object(
            p_, track.raw(), group_id, object_id, payload.raw(), now));
    }

    result<void> write_object(publisher_track track,
                              const pub_object_config &cfg,
                              uint64_t now)
    {
        moq_pub_object_cfg_t c;
        /* Sized init: end_of_group is an appended tail field. */
        moq_pub_object_cfg_init_sized(&c, sizeof(c));
        c.group_id   = cfg.group_id;
        c.object_id  = cfg.object_id;
        c.payload    = cfg.payload ? cfg.payload->raw() : nullptr;
        c.properties = cfg.properties ? cfg.properties->raw() : nullptr;
        c.datagram   = cfg.datagram;
        c.has_status    = cfg.has_status;
        c.status        = to_c(cfg.status);
        c.end_of_group  = cfg.end_of_group;
        return result_from_rc(
            moq_pub_write_object_ex(p_, track.raw(), &c, now));
    }

    result<void> end_group(publisher_track track, uint64_t now)
    {
        return result_from_rc(moq_pub_end_group(p_, track.raw(), now));
    }

    // -- Streaming objects ---------------------------------------------

    result<void> begin_object(publisher_track track, uint64_t group_id,
                              uint64_t object_id, uint64_t payload_length,
                              uint64_t now)
    {
        moq_pub_begin_object_cfg_t c;
        moq_pub_begin_object_cfg_init(&c);
        c.group_id       = group_id;
        c.object_id      = object_id;
        c.payload_length = payload_length;
        return result_from_rc(
            moq_pub_begin_object(p_, track.raw(), &c, now));
    }

    result<void> begin_object(publisher_track track, uint64_t group_id,
                              uint64_t object_id, uint64_t payload_length,
                              const buffer &properties, uint64_t now)
    {
        moq_pub_begin_object_cfg_t c;
        /* Sized init: properties is an appended tail field. */
        moq_pub_begin_object_cfg_init_sized(&c, sizeof(c));
        c.group_id       = group_id;
        c.object_id      = object_id;
        c.payload_length = payload_length;
        c.properties     = properties.raw();
        return result_from_rc(
            moq_pub_begin_object(p_, track.raw(), &c, now));
    }

    result<void> write_data(publisher_track track, const buffer &chunk,
                            uint64_t now)
    {
        return result_from_rc(
            moq_pub_write_data(p_, track.raw(), chunk.raw(), now));
    }

    result<void> end_object(publisher_track track, uint64_t now)
    {
        return result_from_rc(moq_pub_end_object(p_, track.raw(), now));
    }

    // -- Event processing ----------------------------------------------

    /* Sole event consumer for this session: one bounded FIFO pass with
     * staged-work progress before and after it, on every draft. would_block
     * means work remains; drain actions and retry in driver rounds, yielding
     * when nothing progresses. Do not also poll session events directly. */
    result<void> tick(uint64_t now)
    {
        return result_from_rc(moq_pub_tick(p_, now));
    }

    /* Retry owned work without polling events. A manual C event dispatcher
     * calls this once per progress round, then keeps routing FIFO events even
     * after would_block. Never spin waiting for flush-to-OK before polling;
     * a consumed event is owned and must not be replayed. See publisher.h. */
    result<void> flush(uint64_t now)
    {
        return result_from_rc(moq_pub_flush(p_, now));
    }

    // -- Query ---------------------------------------------------------

    size_t active_subscriptions(publisher_track track) const noexcept
    {
        return moq_pub_active_subscriptions(p_, track.raw());
    }

    bool has_subscriber(publisher_track track) const noexcept
    {
        return moq_pub_has_subscriber(p_, track.raw());
    }

    /* Where the track's publication went when a REDIRECT or a GOAWAY on its
     * request stream ended it; nullopt when it did not end by migration. */
    result<std::optional<request_migration>> get_track_migration(
        publisher_track track) const
    {
        moq_request_migration_t m;
        auto rc = moq_pub_get_track_migration(p_, track.raw(), &m, sizeof(m));
        if (rc == MOQ_DONE)
            return std::optional<request_migration>{};
        if (rc < 0)
            return errc_from_result(rc);
        return std::optional<request_migration>{migration_from_c(m)};
    }

    bool is_draining() const noexcept
    {
        return moq_pub_is_draining(p_);
    }

    // -- Raw access ----------------------------------------------------

    moq_publisher_t       *raw() noexcept { return p_; }
    const moq_publisher_t *raw() const noexcept { return p_; }
};

inline result<publisher> publisher::create(session &s, publisher_config cfg)
{
    return create(s, cfg, publisher_options{});
}

inline result<publisher> publisher::create(session &s, publisher_config cfg,
                                            const publisher_options &options)
{
    moq_pub_cfg_t c;
    /* Sized init: max_fills is past the frozen prefix. */
    moq_pub_cfg_init_sized(&c, sizeof(c));
    c.accept_mode                 = static_cast<moq_pub_accept_mode_t>(cfg.accept_mode);
    c.default_publisher_priority  = cfg.default_publisher_priority;
    c.max_fills                   = options.max_fills;

    moq_publisher_t *p = nullptr;
    auto alloc = cfg.alloc ? cfg.alloc : moq_alloc_default();
    auto rc = moq_pub_create(s.raw(), alloc, &c, &p);
    if (rc < 0)
        return errc_from_result(rc);
    return publisher(p);
}

inline result<publisher> publisher::create_observed(session &s, publisher_observer &observer,
    publisher_config cfg, const publisher_options &options)
{
    moq_pub_cfg_t c;
    moq_pub_cfg_init_sized(&c, sizeof(c));
    c.accept_mode = static_cast<moq_pub_accept_mode_t>(cfg.accept_mode);
    c.default_publisher_priority = cfg.default_publisher_priority;
    c.max_fills = options.max_fills;
    c.callbacks.ctx = &observer;
    c.callbacks.on_namespace_terminal = [](void *ctx, const moq_pub_namespace_terminal_info_t *info) {
        auto &o = *static_cast<publisher_observer *>(ctx);
        if (o.on_namespace_terminal)
            o.on_namespace_terminal(o.ctx, pub_namespace_terminal_view::from_native(info, info->struct_size));
    };
    c.on_new_group_request_ctx = &observer;
    c.on_new_group_request = [](void *ctx, moq_pub_track_t *track,
                                const moq_pub_new_group_request_info_t *info) {
        auto &o = *static_cast<publisher_observer *>(ctx);
        if (o.on_new_group_request)
            o.on_new_group_request(o.ctx, publisher_track(track),
                pub_new_group_request_view::from_native(info, info->struct_size));
    };
    c.on_goaway = [](void *ctx, moq_bytes_t uri) {
        auto &o = *static_cast<publisher_observer *>(ctx);
        if (o.on_goaway)
            o.on_goaway(o.ctx, std::span<const uint8_t>(uri.data, uri.len));
    };
    moq_publisher_t *p = nullptr;
    auto rc = moq_pub_create(s.raw(), cfg.alloc ? cfg.alloc : moq_alloc_default(), &c, &p);
    if (rc < 0) return errc_from_result(rc);
    return publisher(p);
}

} // namespace moq

#endif // MOQ_PUBLISHER_HPP
