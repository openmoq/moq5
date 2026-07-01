#ifndef MOQ_MSF_HPP
#define MOQ_MSF_HPP

#include <moq/msf.h>
#include <moq/buffer.hpp>
#include <moq/result.hpp>
#include <moq/types.hpp>

#include <cstring>
#include <optional>
#include <string_view>

namespace moq::msf {

/// MSF-01 media timeline template (§5.2.15 / §7.4): a fully decoded 6-tuple
/// describing a fixed-duration-segment timeline. Mirrors moq_msf_media_template_t.
struct media_template {
    uint64_t start_media_ms = 0;
    uint64_t delta_media_ms = 0;
    uint64_t start_group = 0;
    uint64_t start_object = 0;
    uint64_t delta_group = 0;
    uint64_t delta_object = 0;
    uint64_t start_wallclock_ms = 0;
    uint64_t delta_wallclock_ms = 0;
};

/// Non-owning view over a C array of moq_bytes_t string fields (e.g. depends[],
/// contentProtectionRefIDs[], defaultKID[]). Each element is materialized as a
/// std::string_view borrowing from the parsed_catalog's DOM, so the span is
/// valid only while the owning parsed_catalog is alive -- the same contract as
/// every other string_view a track_view exposes.
class string_span {
    const moq_bytes_t *items_ = nullptr;
    size_t count_ = 0;

public:
    string_span() = default;
    string_span(const moq_bytes_t *items, size_t count)
        : items_(items), count_(count) {}

    size_t size() const { return count_; }
    bool empty() const { return count_ == 0; }

    std::string_view operator[](size_t i) const {
        return {reinterpret_cast<const char *>(items_[i].data), items_[i].len};
    }

    class iterator {
        const moq_bytes_t *p_ = nullptr;
    public:
        explicit iterator(const moq_bytes_t *p) : p_(p) {}
        std::string_view operator*() const {
            return {reinterpret_cast<const char *>(p_->data), p_->len};
        }
        iterator &operator++() { ++p_; return *this; }
        bool operator!=(const iterator &o) const { return p_ != o.p_; }
    };
    iterator begin() const { return iterator(items_); }
    // Null-aware: a default/empty span has items_ == nullptr; avoid pointer
    // arithmetic on a null pointer (UB) -- begin() == end() either way.
    iterator end() const { return iterator(items_ ? items_ + count_ : nullptr); }
};

struct track_view {
    std::string_view name;
    std::string_view packaging;
    bool             is_live = false;

    /// Presence of the otherwise-required packaging / isLive keys. Always true
    /// for an independent track. On a delta add/clone track object (MSF-01
    /// 5.1.6) false means the key was omitted -- so `packaging` is empty / not
    /// meaningful, and `is_live` is inherited from the parent (clone) rather
    /// than an explicit false. Delta consumers MUST gate on these.
    bool             has_packaging = true;
    bool             has_is_live   = true;

    std::optional<std::string_view> ns;
    std::optional<std::string_view> role;
    std::optional<std::string_view> codec;
    std::optional<std::string_view> init_data;
    std::optional<std::string_view> init_track;
    std::optional<std::string_view> channel_config;
    std::optional<std::string_view> lang;
    std::optional<std::string_view> label;
    std::optional<std::string_view> init_ref;     // MSF-01 5.2.13
    std::optional<std::string_view> event_type;   // MSF-01 5.2.5
    std::optional<std::string_view> mime_type;    // MSF-01 5.2.19

    std::optional<uint32_t> width;
    std::optional<uint32_t> height;
    std::optional<uint32_t> samplerate;
    std::optional<uint64_t> bitrate;
    std::optional<uint64_t> framerate_millis;
    std::optional<int>      render_group;
    std::optional<int>      alt_group;
    std::optional<uint64_t> timescale;
    std::optional<uint64_t> target_latency;
    std::optional<uint64_t> track_duration_ms;    // MSF-01 5.2.35
    std::optional<uint32_t> max_grp_sap;          // CMSF 3.5.2.1
    std::optional<uint32_t> max_obj_sap;          // CMSF 3.5.2.2

    /// MSF-01 5.2.14 depends[] -- track names this track depends on / applies to.
    string_span depends;
    /// CMSF 4.1.2 contentProtectionRefIDs[] -- resolves via find_content_protection.
    string_span content_protection_ref_ids;
    /// MSF-01 5.2.15 / §7.4 inline media-timeline template.
    std::optional<media_template> timeline_template;

    /// MSF-01 5.2.33/5.2.34 -- present only on a delta "clone" op track object.
    std::optional<std::string_view> parent_name;
    std::optional<std::string_view> parent_namespace;
};

struct init_data_view {
    std::string_view id;     // MSF-01 5.1.7 (unique within the catalog)
    std::string_view type;   // "inline" in this MSF version
    std::string_view data;   // base64 for type "inline"
};

struct url_view {
    std::string_view                url;
    std::optional<std::string_view> type;
};

struct drm_system_view {
    std::string_view             system_id;   // UUID string (not interpreted)
    std::optional<url_view>      la_url;
    std::optional<url_view>      cert_url;
    std::optional<url_view>      auth_url;
    std::optional<std::string_view> pssh;
    std::optional<std::string_view> robustness;
};

struct content_protection_view {
    std::string_view  ref_id;       // CMSF 4.1.1.1
    string_span       default_kids; // CMSF 4.1.1.2 (UUID strings; not interpreted)
    std::string_view  scheme;       // "cenc" | "cbcs"
    drm_system_view   drm_system;
};

/// MSF-01 5.1.6 delta operation kind. Values match moq_msf_delta_op_kind_t.
enum class delta_op_kind {
    add    = MOQ_MSF_DELTA_OP_ADD,
    remove = MOQ_MSF_DELTA_OP_REMOVE,
    clone  = MOQ_MSF_DELTA_OP_CLONE,
};

namespace detail {

inline std::string_view sv(moq_bytes_t b) {
    return {reinterpret_cast<const char *>(b.data), b.len};
}

inline url_view url_view_from(const moq_cmsf_url_t &u) {
    url_view v;
    v.url = sv(u.url);
    if (u.has_type) v.type = sv(u.type);
    return v;
}

inline drm_system_view drm_view_from(const moq_cmsf_drm_system_t &d) {
    drm_system_view v;
    v.system_id = sv(d.system_id);
    if (d.la_url.present)   v.la_url   = url_view_from(d.la_url);
    if (d.cert_url.present) v.cert_url = url_view_from(d.cert_url);
    if (d.auth_url.present) v.auth_url = url_view_from(d.auth_url);
    if (d.has_pssh)        v.pssh        = sv(d.pssh);
    if (d.has_robustness)  v.robustness  = sv(d.robustness);
    return v;
}

inline content_protection_view cp_view_from(const moq_cmsf_content_protection_t &c) {
    content_protection_view v;
    v.ref_id       = sv(c.ref_id);
    v.default_kids = string_span(c.default_kids, c.default_kid_count);
    v.scheme       = sv(c.scheme);
    v.drm_system   = drm_view_from(c.drm_system);
    return v;
}

inline init_data_view init_view_from(const moq_msf_init_data_entry_t &e) {
    return {sv(e.id), sv(e.type), sv(e.data)};
}

inline track_view view_from(const moq_msf_track_t &t) {
    track_view v;
    v.name      = sv(t.name);
    v.packaging = sv(t.packaging);
    v.is_live   = t.is_live;
    v.has_packaging = t.has_packaging;
    v.has_is_live   = t.has_is_live;
    if (t.has_namespace)      v.ns             = sv(t.namespace_);
    if (t.has_role)           v.role           = sv(t.role);
    if (t.has_codec)          v.codec          = sv(t.codec);
    if (t.has_init_data)      v.init_data      = sv(t.init_data);
    if (t.has_init_track)     v.init_track     = sv(t.init_track);
    if (t.has_channel_config) v.channel_config = sv(t.channel_config);
    if (t.has_lang)           v.lang           = sv(t.lang);
    if (t.has_label)          v.label          = sv(t.label);
    if (t.has_width)          v.width          = t.width;
    if (t.has_height)         v.height         = t.height;
    if (t.has_samplerate)     v.samplerate     = t.samplerate;
    if (t.has_bitrate)        v.bitrate        = t.bitrate;
    if (t.has_framerate)      v.framerate_millis = t.framerate_millis;
    if (t.has_render_group)   v.render_group   = t.render_group;
    if (t.has_alt_group)      v.alt_group      = t.alt_group;
    if (t.has_timescale)      v.timescale      = t.timescale;
    if (t.has_target_latency) v.target_latency = t.target_latency;
    if (t.has_init_ref)       v.init_ref       = sv(t.init_ref);
    if (t.has_event_type)     v.event_type     = sv(t.event_type);
    if (t.has_mime_type)      v.mime_type      = sv(t.mime_type);
    if (t.has_track_duration) v.track_duration_ms = t.track_duration_ms;
    if (t.has_max_grp_sap)    v.max_grp_sap    = t.max_grp_sap;
    if (t.has_max_obj_sap)    v.max_obj_sap    = t.max_obj_sap;
    v.depends                   = string_span(t.depends, t.depends_count);
    v.content_protection_ref_ids = string_span(t.cp_ref_ids, t.cp_ref_id_count);
    if (t.has_template) {
        media_template mt;
        mt.start_media_ms     = t.template_.start_media_ms;
        mt.delta_media_ms     = t.template_.delta_media_ms;
        mt.start_group        = t.template_.start_group;
        mt.start_object       = t.template_.start_object;
        mt.delta_group        = t.template_.delta_group;
        mt.delta_object       = t.template_.delta_object;
        mt.start_wallclock_ms = t.template_.start_wallclock_ms;
        mt.delta_wallclock_ms = t.template_.delta_wallclock_ms;
        v.timeline_template = mt;
    }
    if (t.has_parent_name)      v.parent_name      = sv(t.parent_name);
    if (t.has_parent_namespace) v.parent_namespace = sv(t.parent_namespace);
    return v;
}

} // namespace detail

/// MSF-01 5.1.6 view over one delta operation (op kind + its track objects).
/// Borrows from the owning parsed_catalog's DOM.
class delta_op_view {
    const moq_msf_delta_op_t *op_ = nullptr;
public:
    explicit delta_op_view(const moq_msf_delta_op_t *op) : op_(op) {}
    delta_op_kind kind() const { return static_cast<delta_op_kind>(op_->op); }
    size_t track_count() const { return op_->track_count; }
    track_view track(size_t i) const { return detail::view_from(op_->tracks[i]); }
};

class parsed_catalog {
    moq_msf_catalog_t cat_{};
    moq_alloc_t alloc_{};
    bool valid_ = false;

public:
    parsed_catalog() = default;

    parsed_catalog(moq_msf_catalog_t cat, moq_alloc_t alloc)
        : cat_(cat), alloc_(alloc), valid_(true) {}

    ~parsed_catalog() {
        if (valid_)
            moq_msf_catalog_cleanup(&alloc_, &cat_);
    }

    parsed_catalog(parsed_catalog &&o) noexcept
        : cat_(o.cat_), alloc_(o.alloc_), valid_(o.valid_) {
        o.valid_ = false;
    }

    parsed_catalog &operator=(parsed_catalog &&o) noexcept {
        if (this != &o) {
            if (valid_) moq_msf_catalog_cleanup(&alloc_, &cat_);
            cat_ = o.cat_;
            alloc_ = o.alloc_;
            valid_ = o.valid_;
            o.valid_ = false;
        }
        return *this;
    }

    parsed_catalog(const parsed_catalog &) = delete;
    parsed_catalog &operator=(const parsed_catalog &) = delete;

    int version() const { return cat_.version; }

    std::optional<uint64_t> generated_at() const {
        if (cat_.has_generated_at) return cat_.generated_at;
        return std::nullopt;
    }

    /// MSF-01 5.1.3 broadcast completion. Only the TRUE state is meaningful.
    bool is_complete() const { return cat_.is_complete; }

    size_t track_count() const { return cat_.track_count; }

    track_view track(size_t i) const {
        return detail::view_from(cat_.tracks[i]);
    }

    std::optional<track_view> find_role(std::string_view role) const {
        for (size_t i = 0; i < cat_.track_count; i++) {
            if (!cat_.tracks[i].has_role) continue;
            if (detail::sv(cat_.tracks[i].role) == role)
                return detail::view_from(cat_.tracks[i]);
        }
        return std::nullopt;
    }

    // MSF-01 5.1.7 root Initialization Data List.
    size_t init_data_count() const { return cat_.init_data_count; }
    init_data_view init_data(size_t i) const {
        return detail::init_view_from(cat_.init_data_list[i]);
    }
    /// Resolve a track's initRef (5.2.13) to its init-data entry, or nullopt.
    std::optional<init_data_view> find_init_data(std::string_view id) const {
        const moq_msf_init_data_entry_t *e =
            moq_msf_catalog_find_init_data(&cat_, bytes_view(id).raw());
        if (!e) return std::nullopt;
        return detail::init_view_from(*e);
    }

    // CMSF 4.1.1 root content protections.
    size_t content_protection_count() const {
        return cat_.content_protection_count;
    }
    content_protection_view content_protection(size_t i) const {
        return detail::cp_view_from(cat_.content_protections[i]);
    }
    /// Resolve a track's contentProtectionRefIDs entry (4.1.2), or nullopt.
    std::optional<content_protection_view>
    find_content_protection(std::string_view ref_id) const {
        const moq_cmsf_content_protection_t *c =
            moq_msf_catalog_find_content_protection(&cat_, bytes_view(ref_id).raw());
        if (!c) return std::nullopt;
        return detail::cp_view_from(*c);
    }

    // MSF-01 5.1.6 delta update. is_delta() is true for a partial catalog
    // (version 0, no root tracks) carrying add/remove/clone operations.
    bool is_delta() const { return cat_.delta_update != nullptr; }
    size_t delta_op_count() const { return cat_.delta_update_count; }
    delta_op_view delta_op(size_t i) const {
        return delta_op_view(&cat_.delta_update[i]);
    }
};

inline result<parsed_catalog> parse(bytes_view json,
                                     const moq_alloc_t *alloc = moq_alloc_default())
{
    moq_msf_catalog_t cat;
    moq_result_t rc = moq_msf_catalog_parse(alloc, json.raw(), &cat);
    if (rc < 0)
        return errc_from_result(rc);
    return parsed_catalog(cat, *alloc);
}

namespace detail {

/// Owning scratch region for the transient C mirror arrays that encode() builds
/// from the borrowed authoring views before handing them to
/// moq_msf_catalog_encode. Every block is freed together when the arena is
/// destroyed -- which happens after the encode call returns -- so the C
/// catalog's array pointers stay valid for exactly the encode call and not a
/// cycle longer. Allocation failure is latched (failed()) rather than thrown,
/// matching the binding's result<> error model (the binding uses no exceptions).
class encode_arena {
    const moq_alloc_t *alloc_;
    struct block { void *p; size_t size; block *next; };
    block *head_ = nullptr;
    bool   failed_ = false;

public:
    explicit encode_arena(const moq_alloc_t *a) : alloc_(a) {}
    ~encode_arena() {
        for (block *b = head_; b; ) {
            block *next = b->next;
            alloc_->free(b->p, b->size, alloc_->ctx);
            alloc_->free(b, sizeof(block), alloc_->ctx);
            b = next;
        }
    }
    encode_arena(const encode_arena &) = delete;
    encode_arena &operator=(const encode_arena &) = delete;

    bool failed() const { return failed_; }

    /// Allocate n zero-initialized T from the arena. Returns nullptr and latches
    /// failed() on allocation failure or size overflow. n == 0 returns nullptr
    /// WITHOUT failing (an absent array is not an error).
    template <class T>
    T *calloc_n(size_t n) {
        if (failed_ || n == 0) return nullptr;
        if (n > static_cast<size_t>(-1) / sizeof(T)) { failed_ = true; return nullptr; }
        size_t sz = n * sizeof(T);
        void *p = alloc_->alloc(sz, alloc_->ctx);
        block *node = p ? static_cast<block *>(
            alloc_->alloc(sizeof(block), alloc_->ctx)) : nullptr;
        if (!p || !node) {
            if (p) alloc_->free(p, sz, alloc_->ctx);
            failed_ = true;
            return nullptr;
        }
        std::memset(p, 0, sz);
        node->p = p; node->size = sz; node->next = head_; head_ = node;
        return static_cast<T *>(p);
    }
};

/// Copy a borrowed bytes_view array into an arena-owned moq_bytes_t array.
/// Leaves *out NULL / *out_count 0 for an empty input (not an error). Returns
/// false ONLY on a genuine allocation failure (arena.failed() latched).
inline bool dup_bytes_array(encode_arena &arena, const bytes_view *src, size_t n,
                            moq_bytes_t **out, size_t *out_count) {
    *out = nullptr; *out_count = 0;
    if (n == 0) return true;
    moq_bytes_t *arr = arena.calloc_n<moq_bytes_t>(n);
    if (!arr) return false;
    for (size_t i = 0; i < n; i++) arr[i] = src[i].raw();
    *out = arr; *out_count = n;
    return true;
}

} // namespace detail

struct track {
    bytes_view name;
    bytes_view packaging;
    bool       is_live = false;

    bool       has_namespace      = false;  bytes_view ns;
    bool       has_role           = false;  bytes_view role;
    bool       has_codec          = false;  bytes_view codec;
    bool       has_init_data      = false;  bytes_view init_data;
    bool       has_init_track     = false;  bytes_view init_track;
    bool       has_channel_config = false;  bytes_view channel_config;
    bool       has_lang           = false;  bytes_view lang;
    bool       has_label          = false;  bytes_view label;
    bool       has_init_ref       = false;  bytes_view init_ref;
    bool       has_event_type     = false;  bytes_view event_type;
    bool       has_mime_type      = false;  bytes_view mime_type;

    bool       has_width          = false;  uint32_t width = 0;
    bool       has_height         = false;  uint32_t height = 0;
    bool       has_samplerate     = false;  uint32_t samplerate = 0;
    bool       has_bitrate        = false;  uint64_t bitrate = 0;
    bool       has_framerate      = false;  uint64_t framerate_millis = 0;
    bool       has_render_group   = false;  int      render_group = 0;
    bool       has_alt_group      = false;  int      alt_group = 0;
    bool       has_timescale      = false;  uint64_t timescale = 0;
    bool       has_target_latency = false;  uint64_t target_latency = 0;
    bool       has_track_duration = false;  uint64_t track_duration_ms = 0;
    bool       has_max_grp_sap    = false;  uint32_t max_grp_sap = 0;
    bool       has_max_obj_sap    = false;  uint32_t max_obj_sap = 0;

    // MSF-01 5.2.15 / §7.4 inline media-timeline template (value, no alloc).
    bool       has_timeline_template = false;  media_template timeline_template;

    // MSF-01 5.2.14 / CMSF 4.1.2 borrowed arrays: the bytes_view elements (and
    // the backing array) must outlive encode().
    const bytes_view *depends = nullptr;  size_t depends_count = 0;
    const bytes_view *content_protection_ref_ids = nullptr;
    size_t            content_protection_ref_id_count = 0;

    // Delta-op authoring (MSF-01 5.1.6). Ignored for an independent catalog
    // track -- there name/packaging/isLive are always emitted. On a delta
    // add/clone op track, packaging/isLive are emitted only when has_packaging /
    // has_is_live are set, so a partial track round-trips. On a clone op track
    // parent_name is REQUIRED and parent_namespace optional; on a remove op
    // track set name (+ optional namespace) only.
    bool       has_packaging      = false;
    bool       has_is_live        = false;
    bool       has_parent_name    = false;  bytes_view parent_name;
    bool       has_parent_namespace = false;  bytes_view parent_namespace;
};

// MSF-01 5.1.7 root Initialization Data List entry (authoring; borrowed views).
struct init_data_entry {
    bytes_view id;
    bytes_view type;
    bytes_view data;
};

// CMSF 4.1.1.4 drmSystem URL (authoring). Set has_type to emit the "type" key.
struct drm_url {
    bytes_view url;
    bool       has_type = false;  bytes_view type;
};

// CMSF 4.1.1.4 drmSystem (authoring). systemID required; the rest optional.
struct drm_system {
    bytes_view system_id;
    bool       has_la_url     = false;  drm_url la_url;
    bool       has_cert_url   = false;  drm_url cert_url;
    bool       has_auth_url   = false;  drm_url auth_url;
    bool       has_pssh       = false;  bytes_view pssh;
    bool       has_robustness = false;  bytes_view robustness;
};

// CMSF 4.1.1 content protection (authoring). The public encoder enforces the
// §4 syntax (scheme enum, UUID systemID/defaultKID, unique refIDs). default_kids
// and the nested views are borrowed and must outlive encode(). The drmSystem is
// named `drm` to avoid a member/type name clash.
struct content_protection {
    bytes_view        ref_id;
    const bytes_view *default_kids = nullptr;  size_t default_kid_count = 0;
    bytes_view        scheme;
    drm_system        drm;
};

// MSF-01 5.1.6 delta operation (authoring): a kind plus its op track objects.
struct delta_op {
    delta_op_kind kind = delta_op_kind::add;
    const track  *tracks = nullptr;
    size_t        track_count = 0;
};

struct catalog {
    int                  version = MOQ_MSF_VERSION;
    const track         *tracks  = nullptr;
    size_t               track_count = 0;
    bool                 has_generated_at = false;
    uint64_t             generated_at = 0;
    bool                 is_complete = false;   // MSF-01 5.1.3 (independent only)

    // MSF-01 5.1.7 / CMSF 4.1.1 / MSF-01 5.1.6 root arrays (authoring). Borrowed;
    // must outlive encode(). A delta catalog sets delta_update (+ count) and
    // leaves tracks empty (the C encoder requires version/tracks absent there).
    const init_data_entry    *init_data_list = nullptr;  size_t init_data_count = 0;
    const content_protection *content_protections = nullptr;
    size_t                    content_protection_count = 0;
    const delta_op           *delta_update = nullptr;  size_t delta_update_count = 0;
};

namespace detail {
// A track's borrowed array fields must be coherent: a non-zero count requires a
// non-null pointer. Checked before any copy so a malformed caller gets
// errc::invalid rather than a null dereference inside encode().
inline bool track_arrays_coherent(const track &t) {
    if (t.depends_count > 0 && !t.depends) return false;
    if (t.content_protection_ref_id_count > 0 && !t.content_protection_ref_ids)
        return false;
    return true;
}
} // namespace detail

inline void fill_c_track_(detail::encode_arena &arena, moq_msf_track_t &d,
                          const track &s)
{
    std::memset(&d, 0, sizeof(d));
    d.struct_size = sizeof(d);
    d.name = s.name.raw();
    d.packaging = s.packaging.raw();
    d.is_live = s.is_live;
    if (s.has_namespace)      { d.has_namespace = true;      d.namespace_ = s.ns.raw(); }
    if (s.has_role)           { d.has_role = true;           d.role = s.role.raw(); }
    if (s.has_codec)          { d.has_codec = true;          d.codec = s.codec.raw(); }
    if (s.has_init_data)      { d.has_init_data = true;      d.init_data = s.init_data.raw(); }
    if (s.has_init_track)     { d.has_init_track = true;     d.init_track = s.init_track.raw(); }
    if (s.has_channel_config) { d.has_channel_config = true; d.channel_config = s.channel_config.raw(); }
    if (s.has_lang)           { d.has_lang = true;           d.lang = s.lang.raw(); }
    if (s.has_label)          { d.has_label = true;          d.label = s.label.raw(); }
    if (s.has_width)          { d.has_width = true;          d.width = s.width; }
    if (s.has_height)         { d.has_height = true;         d.height = s.height; }
    if (s.has_samplerate)     { d.has_samplerate = true;     d.samplerate = s.samplerate; }
    if (s.has_bitrate)        { d.has_bitrate = true;        d.bitrate = s.bitrate; }
    if (s.has_framerate)      { d.has_framerate = true;      d.framerate_millis = s.framerate_millis; }
    if (s.has_render_group)   { d.has_render_group = true;   d.render_group = s.render_group; }
    if (s.has_alt_group)      { d.has_alt_group = true;      d.alt_group = s.alt_group; }
    if (s.has_timescale)      { d.has_timescale = true;      d.timescale = s.timescale; }
    if (s.has_target_latency) { d.has_target_latency = true; d.target_latency = s.target_latency; }
    if (s.has_init_ref)       { d.has_init_ref = true;       d.init_ref = s.init_ref.raw(); }
    if (s.has_event_type)     { d.has_event_type = true;     d.event_type = s.event_type.raw(); }
    if (s.has_mime_type)      { d.has_mime_type = true;      d.mime_type = s.mime_type.raw(); }
    if (s.has_track_duration) { d.has_track_duration = true; d.track_duration_ms = s.track_duration_ms; }
    if (s.has_max_grp_sap)    { d.has_max_grp_sap = true;    d.max_grp_sap = s.max_grp_sap; }
    if (s.has_max_obj_sap)    { d.has_max_obj_sap = true;    d.max_obj_sap = s.max_obj_sap; }
    if (s.has_timeline_template) {
        d.has_template = true;
        d.template_.start_media_ms     = s.timeline_template.start_media_ms;
        d.template_.delta_media_ms     = s.timeline_template.delta_media_ms;
        d.template_.start_group        = s.timeline_template.start_group;
        d.template_.start_object       = s.timeline_template.start_object;
        d.template_.delta_group        = s.timeline_template.delta_group;
        d.template_.delta_object       = s.timeline_template.delta_object;
        d.template_.start_wallclock_ms = s.timeline_template.start_wallclock_ms;
        d.template_.delta_wallclock_ms = s.timeline_template.delta_wallclock_ms;
    }
    // Delta-op authoring fields (no-ops for an independent catalog track).
    if (s.has_packaging) d.has_packaging = true;
    if (s.has_is_live)   d.has_is_live   = true;
    if (s.has_parent_name)      { d.has_parent_name = true;      d.parent_name = s.parent_name.raw(); }
    if (s.has_parent_namespace) { d.has_parent_namespace = true; d.parent_namespace = s.parent_namespace.raw(); }
    // Borrowed arrays -> arena-owned moq_bytes_t arrays. On allocation failure
    // dup_bytes_array leaves the field NULL/0 and latches arena.failed(), which
    // encode() checks before calling moq_msf_catalog_encode.
    detail::dup_bytes_array(arena, s.depends, s.depends_count,
                            &d.depends, &d.depends_count);
    detail::dup_bytes_array(arena, s.content_protection_ref_ids,
                            s.content_protection_ref_id_count,
                            &d.cp_ref_ids, &d.cp_ref_id_count);
}

inline result<buffer> encode(const catalog &cat,
                              const moq_alloc_t *alloc = moq_alloc_default())
{
    if (cat.track_count > 0 && !cat.tracks) return errc::invalid;
    if (cat.init_data_count > 0 && !cat.init_data_list) return errc::invalid;
    if (cat.content_protection_count > 0 && !cat.content_protections)
        return errc::invalid;
    if (cat.delta_update_count > 0 && !cat.delta_update) return errc::invalid;

    // Owns every transient C array; frees them all when this scope exits (after
    // the encode call), so each early return below is leak-safe.
    detail::encode_arena arena(alloc);

    moq_msf_catalog_t cc;
    std::memset(&cc, 0, sizeof(cc));
    cc.struct_size = sizeof(cc);
    cc.version = cat.version;
    cc.has_generated_at = cat.has_generated_at;
    cc.generated_at = cat.generated_at;
    cc.is_complete = cat.is_complete;

    // Tracks: stack fast-path for <= 8, arena otherwise. stack_ct lives for the
    // whole call (incl. the encode below), so no manual free is needed.
    constexpr size_t stack_max = 8;
    moq_msf_track_t stack_ct[stack_max];
    moq_msf_track_t *ct = stack_ct;
    if (cat.track_count > 0) {
        if (cat.track_count > stack_max) {
            ct = arena.calloc_n<moq_msf_track_t>(cat.track_count);
            if (!ct) return errc::nomem;
        }
        for (size_t i = 0; i < cat.track_count; i++) {
            if (!detail::track_arrays_coherent(cat.tracks[i]))
                return errc::invalid;
            fill_c_track_(arena, ct[i], cat.tracks[i]);
        }
        cc.tracks = ct;
        cc.track_count = cat.track_count;
    }

    // MSF-01 5.1.7 initDataList.
    if (cat.init_data_count > 0) {
        auto *cidl =
            arena.calloc_n<moq_msf_init_data_entry_t>(cat.init_data_count);
        if (!cidl) return errc::nomem;
        for (size_t i = 0; i < cat.init_data_count; i++) {
            cidl[i].id   = cat.init_data_list[i].id.raw();
            cidl[i].type = cat.init_data_list[i].type.raw();
            cidl[i].data = cat.init_data_list[i].data.raw();
        }
        cc.init_data_list = cidl;
        cc.init_data_count = cat.init_data_count;
    }

    // CMSF 4.1.1 contentProtections (+ nested defaultKID[] / drmSystem URLs).
    if (cat.content_protection_count > 0) {
        auto fill_url = [](moq_cmsf_url_t &d, const drm_url &s) {
            d.present = true; d.url = s.url.raw();
            if (s.has_type) { d.has_type = true; d.type = s.type.raw(); }
        };
        auto *ccp = arena.calloc_n<moq_cmsf_content_protection_t>(
            cat.content_protection_count);
        if (!ccp) return errc::nomem;
        for (size_t i = 0; i < cat.content_protection_count; i++) {
            const content_protection &s = cat.content_protections[i];
            if (s.default_kid_count > 0 && !s.default_kids) return errc::invalid;
            ccp[i].ref_id = s.ref_id.raw();
            if (!detail::dup_bytes_array(arena, s.default_kids,
                                         s.default_kid_count,
                                         &ccp[i].default_kids,
                                         &ccp[i].default_kid_count))
                return errc::nomem;
            ccp[i].scheme = s.scheme.raw();
            ccp[i].drm_system.system_id = s.drm.system_id.raw();
            if (s.drm.has_la_url)   fill_url(ccp[i].drm_system.la_url,   s.drm.la_url);
            if (s.drm.has_cert_url) fill_url(ccp[i].drm_system.cert_url, s.drm.cert_url);
            if (s.drm.has_auth_url) fill_url(ccp[i].drm_system.auth_url, s.drm.auth_url);
            if (s.drm.has_pssh) {
                ccp[i].drm_system.has_pssh = true;
                ccp[i].drm_system.pssh = s.drm.pssh.raw();
            }
            if (s.drm.has_robustness) {
                ccp[i].drm_system.has_robustness = true;
                ccp[i].drm_system.robustness = s.drm.robustness.raw();
            }
        }
        cc.content_protections = ccp;
        cc.content_protection_count = cat.content_protection_count;
    }

    // MSF-01 5.1.6 deltaUpdate (+ per-op track arrays via fill_c_track_).
    if (cat.delta_update_count > 0) {
        auto *cops = arena.calloc_n<moq_msf_delta_op_t>(cat.delta_update_count);
        if (!cops) return errc::nomem;
        for (size_t i = 0; i < cat.delta_update_count; i++) {
            const delta_op &op = cat.delta_update[i];
            cops[i].op = static_cast<moq_msf_delta_op_kind_t>(op.kind);
            if (op.track_count > 0) {
                if (!op.tracks) return errc::invalid;
                auto *cts = arena.calloc_n<moq_msf_track_t>(op.track_count);
                if (!cts) return errc::nomem;
                for (size_t j = 0; j < op.track_count; j++) {
                    if (!detail::track_arrays_coherent(op.tracks[j]))
                        return errc::invalid;
                    fill_c_track_(arena, cts[j], op.tracks[j]);
                }
                cops[i].tracks = cts;
                cops[i].track_count = op.track_count;
            }
        }
        cc.delta_update = cops;
        cc.delta_update_count = cat.delta_update_count;
    }

    // A nested-array allocation inside fill_c_track_ may have failed without an
    // immediate return; catch it before handing cc to the C encoder.
    if (arena.failed()) return errc::nomem;

    moq_rcbuf_t *raw = nullptr;
    moq_result_t rc = moq_msf_catalog_encode(alloc, &cc, &raw);
    if (rc < 0)
        return errc_from_result(rc);
    return buffer::adopt(raw);
}

inline result<buffer> decode_init_data(bytes_view b64,
                                       const moq_alloc_t *alloc = moq_alloc_default())
{
    moq_rcbuf_t *raw = nullptr;
    moq_result_t rc = moq_msf_decode_init_data(alloc, b64.raw(), &raw);
    if (rc < 0) return errc_from_result(rc);
    return buffer::adopt(raw);
}

inline result<buffer> encode_init_data(bytes_view data,
                                       const moq_alloc_t *alloc = moq_alloc_default())
{
    moq_rcbuf_t *raw = nullptr;
    moq_result_t rc = moq_msf_encode_init_data(alloc, data.raw(), &raw);
    if (rc < 0) return errc_from_result(rc);
    return buffer::adopt(raw);
}

} // namespace moq::msf

#endif // MOQ_MSF_HPP
