#ifndef MOQ_PUBLISHER_HPP
#define MOQ_PUBLISHER_HPP

#include <moq/publisher.h>

#include <moq/buffer.hpp>
#include <moq/result.hpp>
#include <moq/session.hpp>

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

struct pub_track_config {
    namespace_name ns;
    bytes_view     track;
    bool           advertise_namespace     = false;
    bool           has_publisher_priority   = false;
    uint8_t        publisher_priority      = 128;
    uint64_t       max_retained_bytes      = 0;  // retained-group budget; 0 = 1 MiB
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
         * advertise the full current size for add_track to honor them. */
        moq_pub_track_cfg_init_sized(&c, sizeof(c));
        c.track_namespace        = cfg.ns.c_namespace();
        c.track_name             = cfg.track.raw();
        c.advertise_namespace    = cfg.advertise_namespace;
        c.has_publisher_priority = cfg.has_publisher_priority;
        c.publisher_priority     = cfg.publisher_priority;
        c.max_retained_bytes     = cfg.max_retained_bytes;

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
        moq_pub_object_cfg_init(&c);
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
        moq_pub_begin_object_cfg_init(&c);
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

    result<void> tick(uint64_t now)
    {
        return result_from_rc(moq_pub_tick(p_, now));
    }

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
    moq_pub_cfg_t c;
    moq_pub_cfg_init(&c);
    c.accept_mode                 = static_cast<moq_pub_accept_mode_t>(cfg.accept_mode);
    c.default_publisher_priority  = cfg.default_publisher_priority;

    moq_publisher_t *p = nullptr;
    auto alloc = cfg.alloc ? cfg.alloc : moq_alloc_default();
    auto rc = moq_pub_create(s.raw(), alloc, &c, &p);
    if (rc < 0)
        return errc_from_result(rc);
    return publisher(p);
}

} // namespace moq

#endif // MOQ_PUBLISHER_HPP
