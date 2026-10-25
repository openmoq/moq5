#ifndef MOQ_TYPES_HPP
#define MOQ_TYPES_HPP

#include <moq/types.h>
#include <moq/session.h>
#include <moq/subscriber.h>
#include <moq/transport_binding.h>

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
        if (i >= count_ || i >= parts_.size())
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

enum class request_error : uint64_t {
    internal_error             = MOQ_REQUEST_ERROR_INTERNAL_ERROR,
    unauthorized               = MOQ_REQUEST_ERROR_UNAUTHORIZED,
    timeout                    = MOQ_REQUEST_ERROR_TIMEOUT,
    not_supported              = MOQ_REQUEST_ERROR_NOT_SUPPORTED,
    malformed_auth_token       = MOQ_REQUEST_ERROR_MALFORMED_AUTH_TOKEN,
    expired_auth_token         = MOQ_REQUEST_ERROR_EXPIRED_AUTH_TOKEN,
    excessive_load             = MOQ_REQUEST_ERROR_EXCESSIVE_LOAD,
    malformed_track            = MOQ_REQUEST_ERROR_MALFORMED_TRACK,
    uninterested               = MOQ_REQUEST_ERROR_UNINTERESTED,
    namespace_too_large        = MOQ_REQUEST_ERROR_NAMESPACE_TOO_LARGE,
    going_away                 = MOQ_REQUEST_ERROR_GOING_AWAY,
    does_not_exist             = MOQ_REQUEST_ERROR_DOES_NOT_EXIST,
    invalid_range              = MOQ_REQUEST_ERROR_INVALID_RANGE,
    duplicate_subscription     = MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION,
    prefix_overlap             = MOQ_REQUEST_ERROR_PREFIX_OVERLAP,
    invalid_joining_request_id = MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID,
    unsupported_extension      = MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION,
    redirect                   = MOQ_REQUEST_ERROR_REDIRECT,
};

// Keep the legacy enum's case set stable for exhaustive switch clients.
namespace request_errors {
inline constexpr request_error conflicting_filters =
    static_cast<request_error>(MOQ_REQUEST_ERROR_CONFLICTING_FILTERS);
inline constexpr request_error invalid_filter =
    static_cast<request_error>(MOQ_REQUEST_ERROR_INVALID_FILTER);
} // namespace request_errors

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

/* Why a request ended by a stream signal rather than by a message
 * (MOQ_REQUEST_TERMINAL_*); `none` when a message ended it or the producer
 * predates the field. */
enum class request_terminal_cause : uint32_t {
    none               = MOQ_REQUEST_TERMINAL_NONE,
    peer_reset_stream  = MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM,
    peer_stop_sending  = MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING,
    missing_response   = MOQ_REQUEST_TERMINAL_MISSING_RESPONSE,
    truncated_response = MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE,
    peer_fin_complete  = MOQ_REQUEST_TERMINAL_PEER_FIN_COMPLETE,
    /* This side cancelled a fetch it could not take (FETCH_RESET only). */
    local_cancel       = MOQ_REQUEST_TERMINAL_LOCAL_CANCEL,
    /* This side cancelled the request: its Track is malformed (draft-22
     * §12.1). UNSUBSCRIBED, PUBLISH_FINISHED and FETCH_RESET. */
    malformed_track    = MOQ_REQUEST_TERMINAL_MALFORMED_TRACK,
};

constexpr request_terminal_cause request_terminal_cause_from_c(
    moq_request_terminal_cause_t v) noexcept
{
    return static_cast<request_terminal_cause>(v);
}

/* An asynchronous termination of an outgoing subgroup. Unknown causes retain
 * their raw value; the event never implies that the entire request ended. */
enum class outgoing_subgroup_terminal_cause : uint32_t {
    none              = MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE,
    peer_stop_sending = MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING,
    delivery_timeout  = MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT,
};

enum class track_status_terminal_cause : uint32_t {
    peer_reset_stream  = MOQ_TRACK_STATUS_PEER_RESET_STREAM,
    peer_stop_sending  = MOQ_TRACK_STATUS_PEER_STOP_SENDING,
    missing_response   = MOQ_TRACK_STATUS_MISSING_RESPONSE,
    truncated_response = MOQ_TRACK_STATUS_TRUNCATED_RESPONSE,
};

enum class namespace_terminal_cause : uint32_t {
    peer_fin_complete  = MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE,
    peer_reset_stream  = MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM,
    peer_stop_sending  = MOQ_NAMESPACE_TERMINAL_PEER_STOP_SENDING,
    missing_response   = MOQ_NAMESPACE_TERMINAL_MISSING_RESPONSE,
    truncated_message  = MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE,
    local_capacity     = MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY,
};

/* This endpoint's side of a request (MOQ_REQUEST_ROLE_*). */
enum class request_role : uint32_t {
    requester = MOQ_REQUEST_ROLE_REQUESTER,
    responder = MOQ_REQUEST_ROLE_RESPONDER,
};

constexpr request_role request_role_from_c(moq_request_role_t v) noexcept
{
    return static_cast<request_role>(v);
}

/* The application error codes a data stream carries in RESET_STREAM or
 * STOP_SENDING (session.h MOQ_STREAM_ERROR_*): wire values passed through
 * unchanged, so a caller uses the value its session's draft registers. */
namespace stream_error {
inline constexpr uint64_t internal_error        = MOQ_STREAM_ERROR_INTERNAL_ERROR;
inline constexpr uint64_t cancelled             = MOQ_STREAM_ERROR_CANCELLED;
inline constexpr uint64_t delivery_timeout      = MOQ_STREAM_ERROR_DELIVERY_TIMEOUT;
inline constexpr uint64_t session_closed        = MOQ_STREAM_ERROR_SESSION_CLOSED;
inline constexpr uint64_t malformed_track       = MOQ_STREAM_ERROR_MALFORMED_TRACK;
/* Drafts 18 and 22 only. */
inline constexpr uint64_t going_away            = MOQ_STREAM_ERROR_GOING_AWAY;
inline constexpr uint64_t too_far_behind        = MOQ_STREAM_ERROR_TOO_FAR_BEHIND;
inline constexpr uint64_t unknown_object_status = MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS;
inline constexpr uint64_t expired_auth_token    = MOQ_STREAM_ERROR_EXPIRED_AUTH_TOKEN;
inline constexpr uint64_t excessive_load        = MOQ_STREAM_ERROR_EXCESSIVE_LOAD;
/* Draft 16's value for UNKNOWN_OBJECT_STATUS. */
inline constexpr uint64_t d16_unknown_object_status =
    MOQ_STREAM_ERROR_D16_UNKNOWN_OBJECT_STATUS;
}  // namespace stream_error

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

namespace fetch_ranges {
inline constexpr fetch_range_kind timed_out =
    static_cast<fetch_range_kind>(MOQ_FETCH_RANGE_TIMED_OUT);
} // namespace fetch_ranges

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
    reset    = MOQ_SUB_FETCH_RESET,
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

/* Items of a subscriber-facade fill (subscriber.h moq_sub_fill_item_kind_t). */
enum class sub_fill_item_kind : uint32_t {
    object   = MOQ_SUB_FILL_OBJECT,
    gap      = MOQ_SUB_FILL_GAP,
    terminal = MOQ_SUB_FILL_TERMINAL,
    created  = MOQ_SUB_FILL_CREATED,
};

constexpr sub_fill_item_kind sub_fill_item_kind_from_c(
    uint32_t v) noexcept
{
    return static_cast<sub_fill_item_kind>(v);
}

enum class sub_status_result_kind {
    ok    = MOQ_SUB_STATUS_OK,
    error = MOQ_SUB_STATUS_ERROR,
};

/* Semantic location filter kinds (location.h); the enclosing operation
 * defines omission and inheritance. */
enum class location_filter_kind : uint32_t {
    none           = MOQ_LOCATION_FILTER_NONE,
    relative_group = MOQ_LOCATION_FILTER_RELATIVE_GROUP,
    absolute_start = MOQ_LOCATION_FILTER_ABSOLUTE_START,
    group_end      = MOQ_LOCATION_FILTER_GROUP_END,
    object_end     = MOQ_LOCATION_FILTER_OBJECT_END,
    next_object    = MOQ_LOCATION_FILTER_NEXT_OBJECT,
};

constexpr moq_location_filter_kind_t to_c(location_filter_kind v) noexcept
{
    return static_cast<moq_location_filter_kind_t>(v);
}
constexpr location_filter_kind location_filter_kind_from_c(
    moq_location_filter_kind_t v) noexcept
{
    return static_cast<location_filter_kind>(v);
}

/* Why a fill ended (MOQ_FILL_*) and the domain its code belongs to
 * (MOQ_FILL_ERROR_*), from session.h. */
enum class fill_reason : uint32_t {
    complete                = MOQ_FILL_COMPLETE,
    no_stream               = MOQ_FILL_NO_STREAM,
    local_cancelled         = MOQ_FILL_LOCAL_CANCELLED,
    peer_cancelled_or_reset = MOQ_FILL_PEER_CANCELLED_OR_RESET,
    local_aborted           = MOQ_FILL_LOCAL_ABORTED,
    parent_rejected         = MOQ_FILL_PARENT_REJECTED,
    parent_terminated       = MOQ_FILL_PARENT_TERMINATED,
};

enum class fill_error_domain : uint32_t {
    none           = MOQ_FILL_ERROR_NONE,
    stream         = MOQ_FILL_ERROR_STREAM,
    parent_request = MOQ_FILL_ERROR_PARENT_REQUEST,
    parent_done    = MOQ_FILL_ERROR_PARENT_DONE,
};

constexpr fill_reason fill_reason_from_c(uint32_t v) noexcept
{
    return static_cast<fill_reason>(v);
}
constexpr fill_error_domain fill_error_domain_from_c(uint32_t v) noexcept
{
    return static_cast<fill_error_domain>(v);
}

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
MOQ_DETAIL_HANDLE_(fill_handle,          moq_fill_t,                moq_fill);

#undef MOQ_DETAIL_HANDLE_

/* A fill the subscriber facade requested (subscriber.h moq_sub_fill_t): a
 * plain value, all-zero when absent, meaningful only to its issuing subscriber.
 * Stale after native retirement and consumption of all accepted items,
 * terminal items and local-failure notices release its slot. Never reused
 * within one linked core library instance, even across subscriber lifetimes.
 * Other library copies, unload/reload and processes have separate domains:
 * do not persist or interchange their tokens. Issuance safety does not make a subscriber
 * thread-safe. */
class sub_fill_handle {
    moq_sub_fill_t handle_{};

public:
    sub_fill_handle() = default;
    explicit sub_fill_handle(moq_sub_fill_t h) noexcept : handle_(h) {}

    bool     valid() const noexcept { return moq_sub_fill_is_valid(handle_); }
    bool     operator==(sub_fill_handle o) const noexcept
    {
        return moq_sub_fill_eq(handle_, o.handle_);
    }
    bool     operator!=(sub_fill_handle o) const noexcept { return !(*this == o); }
    uint64_t value() const noexcept { return handle_._opaque; }
    moq_sub_fill_t raw() const noexcept { return handle_; }
};

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
struct std::hash<moq::fill_handle> {
    size_t operator()(moq::fill_handle h) const noexcept
    {
        return static_cast<size_t>(h.hash());
    }
};

template<>
struct std::hash<moq::sub_fill_handle> {
    size_t operator()(moq::sub_fill_handle h) const noexcept
    {
        return std::hash<uint64_t>{}(h.value());
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
