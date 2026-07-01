#ifndef MOQ_TYPES_HPP
#define MOQ_TYPES_HPP

#include <moq/types.h>
#include <moq/session.h>
#include <moq/subscriber.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <string_view>
#include <type_traits>

namespace moq {

// -- stream_ref --------------------------------------------------------

class stream_ref {
    moq_stream_ref_t ref_{};

public:
    stream_ref() = default;
    explicit stream_ref(moq_stream_ref_t r) noexcept : ref_(r) {}
    explicit stream_ref(uint64_t v) noexcept : ref_(moq_stream_ref_from_u64(v)) {}

    bool     operator==(stream_ref other) const noexcept { return ref_._v == other.ref_._v; }
    bool     operator!=(stream_ref other) const noexcept { return !(*this == other); }
    uint64_t value() const noexcept { return ref_._v; }

    moq_stream_ref_t raw() const noexcept { return ref_; }
};

// -- bytes_view --------------------------------------------------------

class bytes_view {
    moq_bytes_t bytes_{};

public:
    bytes_view() = default;

    bytes_view(const uint8_t *data, size_t len) noexcept
        : bytes_{data, len} {}

    bytes_view(moq_bytes_t b) noexcept : bytes_(b) {}

    // String-literal / char-array convenience. The length is known at compile
    // time, so this never scans memory -- it is bounded by the array. A single
    // trailing NUL is dropped ONLY when actually present (the literal case), so
    // a non-NUL-terminated char buffer keeps its full length rather than
    // silently losing its last byte.
    template <std::size_t N>
    bytes_view(const char (&lit)[N]) noexcept
        : bytes_{reinterpret_cast<const uint8_t *>(lit),
                 N > 0 && lit[N - 1] == '\0' ? N - 1 : N} {}

    bytes_view(std::string_view sv) noexcept
        : bytes_{reinterpret_cast<const uint8_t *>(sv.data()), sv.size()} {}

    // A bare char pointer must NOT construct a bytes_view: it would strlen-scan
    // a possibly non-NUL-terminated, attacker-influenced buffer past its end
    // (directly, or via the std::string_view overload). Force an explicit choice
    // -- std::string_view(s) for a runtime C string, or (data, len) for
    // arbitrary bytes. This is a deleted *template* (not a plain const char*
    // overload) so that the more-specialized array-reference overload above
    // still wins partial ordering for string literals; a plain const char*
    // overload would instead steal literals via array-to-pointer decay.
    template <class P,
              std::enable_if_t<
                  std::is_same_v<std::remove_cv_t<P>, char *> ||
                  std::is_same_v<std::remove_cv_t<P>, const char *>, int> = 0>
    bytes_view(P) = delete;
    bytes_view(std::nullptr_t) = delete;

    const uint8_t            *data() const noexcept { return bytes_.data; }
    size_t                    size() const noexcept { return bytes_.len; }
    bool                      empty() const noexcept { return bytes_.len == 0; }
    std::span<const uint8_t>  span() const noexcept { return {data(), size()}; }

    std::string_view string_view() const noexcept
    {
        return {reinterpret_cast<const char *>(bytes_.data), bytes_.len};
    }

    moq_bytes_t raw() const noexcept { return bytes_; }
};

// -- namespace_name ----------------------------------------------------

class namespace_name {
public:
    static constexpr size_t max_parts = 32;

    namespace_name() = default;

    namespace_name(std::initializer_list<std::string_view> parts)
    {
        for (auto sv : parts) {
            if (count_ >= max_parts) {
                overflow_ = true;
                return;
            }
            parts_[count_++] = moq_bytes_t{
                reinterpret_cast<const uint8_t *>(sv.data()), sv.size()};
        }
    }

    namespace_name(std::span<const std::string_view> parts)
    {
        for (auto sv : parts) {
            if (count_ >= max_parts) {
                overflow_ = true;
                return;
            }
            parts_[count_++] = moq_bytes_t{
                reinterpret_cast<const uint8_t *>(sv.data()), sv.size()};
        }
    }

    namespace_name(moq_namespace_t ns)
    {
        size_t n = ns.count < max_parts ? ns.count : max_parts;
        for (size_t i = 0; i < n; ++i)
            parts_[i] = ns.parts[i];
        count_    = n;
        overflow_ = ns.count > max_parts;
    }

    size_t count() const noexcept { return count_; }
    bool   overflowed() const noexcept { return overflow_; }

    std::string_view operator[](size_t i) const noexcept
    {
        if (i >= count_)
            return {};
        return {reinterpret_cast<const char *>(parts_[i].data), parts_[i].len};
    }

    moq_namespace_t c_namespace() const noexcept
    {
        return {parts_.data(), count_};
    }

private:
    std::array<moq_bytes_t, max_parts> parts_{};
    size_t                             count_    = 0;
    bool                               overflow_ = false;
};

// -- Enum wrappers -----------------------------------------------------

enum class perspective {
    client = MOQ_PERSPECTIVE_CLIENT,
    server = MOQ_PERSPECTIVE_SERVER,
};

constexpr moq_perspective_t to_c(perspective v) noexcept
{
    return static_cast<moq_perspective_t>(v);
}
constexpr perspective perspective_from_c(moq_perspective_t v) noexcept
{
    return static_cast<perspective>(v);
}

enum class session_state {
    idle        = MOQ_SESS_IDLE,
    setup_sent  = MOQ_SESS_SETUP_SENT,
    established = MOQ_SESS_ESTABLISHED,
    draining    = MOQ_SESS_DRAINING,
    closed      = MOQ_SESS_CLOSED,
};

constexpr moq_session_state_t to_c(session_state v) noexcept
{
    return static_cast<moq_session_state_t>(v);
}
constexpr session_state session_state_from_c(moq_session_state_t v) noexcept
{
    return static_cast<session_state>(v);
}

enum class group_order : uint32_t {
    default_order = MOQ_GROUP_ORDER_DEFAULT,
    ascending     = MOQ_GROUP_ORDER_ASCENDING,
    descending    = MOQ_GROUP_ORDER_DESCENDING,
};

constexpr moq_group_order_t to_c(group_order v) noexcept
{
    return static_cast<moq_group_order_t>(v);
}
constexpr group_order group_order_from_c(moq_group_order_t v) noexcept
{
    return static_cast<group_order>(v);
}

enum class subscribe_filter : uint32_t {
    none           = MOQ_SUBSCRIBE_FILTER_NONE,
    next_group     = MOQ_SUBSCRIBE_FILTER_NEXT_GROUP,
    largest_object = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT,
    absolute_start = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START,
    absolute_range = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE,
};

constexpr moq_subscribe_filter_t to_c(subscribe_filter v) noexcept
{
    return static_cast<moq_subscribe_filter_t>(v);
}
constexpr subscribe_filter subscribe_filter_from_c(moq_subscribe_filter_t v) noexcept
{
    return static_cast<subscribe_filter>(v);
}

enum class object_status : uint8_t {
    normal       = MOQ_OBJECT_NORMAL,
    end_of_group = MOQ_OBJECT_END_OF_GROUP,
    end_of_track = MOQ_OBJECT_END_OF_TRACK,
};

constexpr moq_object_status_t to_c(object_status v) noexcept
{
    return static_cast<moq_object_status_t>(v);
}
constexpr object_status object_status_from_c(moq_object_status_t v) noexcept
{
    return static_cast<object_status>(v);
}

enum class object_terminal {
    normal = MOQ_OBJECT_TERMINAL_NORMAL,
    reset  = MOQ_OBJECT_TERMINAL_RESET,
    stop   = MOQ_OBJECT_TERMINAL_STOP,
};

constexpr moq_object_terminal_t to_c(object_terminal v) noexcept
{
    return static_cast<moq_object_terminal_t>(v);
}
constexpr object_terminal object_terminal_from_c(moq_object_terminal_t v) noexcept
{
    return static_cast<object_terminal>(v);
}

enum class request_error : uint32_t {
    internal_error             = MOQ_REQUEST_ERROR_INTERNAL_ERROR,
    unauthorized               = MOQ_REQUEST_ERROR_UNAUTHORIZED,
    timeout                    = MOQ_REQUEST_ERROR_TIMEOUT,
    not_supported              = MOQ_REQUEST_ERROR_NOT_SUPPORTED,
    going_away                 = MOQ_REQUEST_ERROR_GOING_AWAY,
    does_not_exist             = MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
    invalid_range              = MOQ_REQUEST_ERROR_INVALID_RANGE,
    duplicate_subscription     = MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION,
    prefix_overlap             = MOQ_REQUEST_ERROR_PREFIX_OVERLAP,
    invalid_joining_request_id = MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID,
    redirect                   = MOQ_REQUEST_ERROR_REDIRECT,
};

constexpr moq_request_error_t to_c(request_error v) noexcept
{
    return static_cast<moq_request_error_t>(v);
}
constexpr request_error request_error_from_c(moq_request_error_t v) noexcept
{
    return static_cast<request_error>(v);
}

// The request family a migration signal applies to (redirect §10.6, GOAWAY §10.4).
enum class request_family : uint32_t {
    subscribe        = MOQ_REQUEST_FAMILY_SUBSCRIBE,
    fetch            = MOQ_REQUEST_FAMILY_FETCH,
    track_status     = MOQ_REQUEST_FAMILY_TRACK_STATUS,
    announcement     = MOQ_REQUEST_FAMILY_ANNOUNCEMENT,
    ns_sub           = MOQ_REQUEST_FAMILY_NS_SUB,
    publish          = MOQ_REQUEST_FAMILY_PUBLISH,
    subscribe_tracks = MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS,
};

constexpr request_family request_family_from_c(moq_request_family_t v) noexcept
{
    return static_cast<request_family>(v);
}

enum class namespace_interest : uint32_t {
    publisher_state = MOQ_NAMESPACE_INTEREST_PUBLISHER_STATE,
    namespace_state = MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE,
    both            = MOQ_NAMESPACE_INTEREST_BOTH,
};

constexpr moq_namespace_interest_t to_c(namespace_interest v) noexcept
{
    return static_cast<moq_namespace_interest_t>(v);
}
constexpr namespace_interest namespace_interest_from_c(
    moq_namespace_interest_t v) noexcept
{
    return static_cast<namespace_interest>(v);
}

enum class fetch_range_kind {
    non_existent = MOQ_FETCH_RANGE_NON_EXISTENT,
    unknown      = MOQ_FETCH_RANGE_UNKNOWN,
};

constexpr moq_fetch_range_kind_t to_c(fetch_range_kind v) noexcept
{
    return static_cast<moq_fetch_range_kind_t>(v);
}
constexpr fetch_range_kind fetch_range_kind_from_c(
    moq_fetch_range_kind_t v) noexcept
{
    return static_cast<fetch_range_kind>(v);
}

enum class sub_fetch_item_kind {
    ok       = MOQ_SUB_FETCH_OK,
    error    = MOQ_SUB_FETCH_ERROR,
    object   = MOQ_SUB_FETCH_OBJECT,
    gap      = MOQ_SUB_FETCH_GAP,
    complete = MOQ_SUB_FETCH_COMPLETE,
};

constexpr moq_sub_fetch_item_kind_t to_c(sub_fetch_item_kind v) noexcept
{
    return static_cast<moq_sub_fetch_item_kind_t>(v);
}
constexpr sub_fetch_item_kind sub_fetch_item_kind_from_c(
    moq_sub_fetch_item_kind_t v) noexcept
{
    return static_cast<sub_fetch_item_kind>(v);
}

enum class sub_status_result_kind {
    ok    = MOQ_SUB_STATUS_OK,
    error = MOQ_SUB_STATUS_ERROR,
};

constexpr moq_sub_status_result_kind_t to_c(sub_status_result_kind v) noexcept
{
    return static_cast<moq_sub_status_result_kind_t>(v);
}
constexpr sub_status_result_kind sub_status_result_kind_from_c(
    moq_sub_status_result_kind_t v) noexcept
{
    return static_cast<sub_status_result_kind>(v);
}

// -- Handle wrappers ---------------------------------------------------

#define MOQ_DETAIL_HANDLE_(CppName, CType, c_prefix)                                    \
    class CppName {                                                                     \
        CType handle_{};                                                                \
                                                                                        \
    public:                                                                             \
        CppName() = default;                                                            \
        explicit CppName(CType h) noexcept : handle_(h) {}                              \
                                                                                        \
        bool     valid() const noexcept { return c_prefix##_is_valid(handle_); }        \
        bool     operator==(CppName o) const noexcept                                   \
        {                                                                               \
            return c_prefix##_eq(handle_, o.handle_);                                   \
        }                                                                               \
        bool     operator!=(CppName o) const noexcept { return !(*this == o); }         \
        uint64_t hash() const noexcept { return c_prefix##_hash(handle_); }             \
        uint64_t id_for_trace() const noexcept                                          \
        {                                                                               \
            return c_prefix##_id_for_trace(handle_);                                    \
        }                                                                               \
        CType raw() const noexcept { return handle_; }                                  \
    }

MOQ_DETAIL_HANDLE_(subscription,        moq_subscription_t,         moq_subscription);
MOQ_DETAIL_HANDLE_(publication,          moq_publication_t,          moq_publication);
MOQ_DETAIL_HANDLE_(fetch_handle,         moq_fetch_t,               moq_fetch);
MOQ_DETAIL_HANDLE_(announcement,         moq_announcement_t,        moq_announcement);
MOQ_DETAIL_HANDLE_(ns_sub_handle,        moq_ns_sub_handle_t,       moq_ns_sub_handle);
MOQ_DETAIL_HANDLE_(track_status_handle,  moq_track_status_handle_t, moq_track_status_handle);
MOQ_DETAIL_HANDLE_(track_sub_handle,     moq_track_sub_handle_t,    moq_track_sub_handle);
MOQ_DETAIL_HANDLE_(subgroup_handle,      moq_subgroup_handle_t,     moq_subgroup);

#undef MOQ_DETAIL_HANDLE_

} // namespace moq

// -- std::hash specializations -----------------------------------------

template<>
struct std::hash<moq::subscription> {
    size_t operator()(moq::subscription h) const noexcept
    {
        return static_cast<size_t>(h.hash());
    }
};

template<>
struct std::hash<moq::publication> {
    size_t operator()(moq::publication h) const noexcept
    {
        return static_cast<size_t>(h.hash());
    }
};

template<>
struct std::hash<moq::fetch_handle> {
    size_t operator()(moq::fetch_handle h) const noexcept
    {
        return static_cast<size_t>(h.hash());
    }
};

template<>
struct std::hash<moq::announcement> {
    size_t operator()(moq::announcement h) const noexcept
    {
        return static_cast<size_t>(h.hash());
    }
};

template<>
struct std::hash<moq::ns_sub_handle> {
    size_t operator()(moq::ns_sub_handle h) const noexcept
    {
        return static_cast<size_t>(h.hash());
    }
};

template<>
struct std::hash<moq::track_status_handle> {
    size_t operator()(moq::track_status_handle h) const noexcept
    {
        return static_cast<size_t>(h.hash());
    }
};

template<>
struct std::hash<moq::subgroup_handle> {
    size_t operator()(moq::subgroup_handle h) const noexcept
    {
        return static_cast<size_t>(h.hash());
    }
};

template<>
struct std::hash<moq::stream_ref> {
    size_t operator()(moq::stream_ref r) const noexcept
    {
        return std::hash<uint64_t>{}(r.value());
    }
};

#endif // MOQ_TYPES_HPP
