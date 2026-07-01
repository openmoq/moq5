#ifndef MOQ_EVENT_HPP
#define MOQ_EVENT_HPP

#include <moq/buffer.hpp>
#include <moq/types.hpp>
#include <moq/visit.hpp>

#include <cstring>
#include <span>
#include <string_view>
#include <variant>

namespace moq {

/* Lifetime contract for event value alternatives:
 *
 * rcbuf-backed fields (payload/properties/chunk) are SELF-OWNING. Each such
 * alternative holds a retained moq::buffer (payload_buf / properties_buf /
 * chunk_buf), so its *_data spans, *_rcbuf raw pointers, and *_owned() stay
 * valid for as long as the value (or any copy of it) is alive -- even after the
 * polled_event it came from is destroyed and moq_event_cleanup() decrefs the C
 * event's refs. Copying an alternative increfs; moving steals.
 *
 * All OTHER fields remain BORROWED: spans, string_views, and token arrays point
 * into session/event scratch owned by the polled_event and are valid only while
 * it is alive. This slice does not make those owning. */
namespace event {

struct setup_complete {
    perspective                           local_perspective;
    perspective                           peer_perspective;
    std::span<const moq_resolved_token_t> tokens;
};

struct session_closed {
    uint64_t         code;
    std::string_view reason;
};

struct subscribe_request {
    subscription                          sub;
    moq_namespace_t                       ns_raw;
    bytes_view                            track_name;
    subscribe_filter                      filter;
    uint8_t                               subscriber_priority;
    group_order                           group_order;
    bool                                  forward;
    uint64_t                              start_group;
    uint64_t                              start_object;
    uint64_t                              end_group;
    uint64_t                              delivery_timeout_us;
    std::span<const moq_resolved_token_t> tokens;
    bool                                  has_new_group_request;
    uint64_t                              new_group_request;

    namespace_name track_namespace() const { return {ns_raw}; }
};

struct subscribe_ok {
    subscription             sub;
    uint64_t                 track_alias;
    bool                     has_largest;
    uint64_t                 largest_group;
    uint64_t                 largest_object;
    bool                     has_expires;
    uint64_t                 expires_ms;
    std::span<const uint8_t> track_properties;
    bool                     dynamic_groups;
};

struct subscribe_error {
    subscription     sub;
    request_error    error_code;
    bool             can_retry;
    uint64_t         retry_after_ms;
    std::string_view reason;
};

struct request_ready {
    uint64_t available_requests;
};

struct object_received {
    subscription             sub;
    publication              pub;
    uint64_t                 group_id;
    uint64_t                 subgroup_id;
    uint64_t                 object_id;
    uint8_t                  publisher_priority;
    object_status            status;
    bool                     end_of_group;
    bool                     datagram;
    std::span<const uint8_t> payload_data;
    std::span<const uint8_t> properties_data;
    moq_rcbuf_t             *payload_rcbuf;
    moq_rcbuf_t             *properties_rcbuf;
    /* Owners retained at variant() construction so the payload/properties buffers
     * (and the spans + raw pointers above, which alias them) stay valid after the
     * source polled_event is destroyed and moq_event_cleanup() decrefs the C
     * event's refs. Copying the variant increfs; moving steals -- no double-free. */
    buffer payload_buf;
    buffer properties_buf;

    buffer payload_owned() const noexcept { return payload_buf; }
    buffer properties_owned() const noexcept { return properties_buf; }
};

struct namespace_published {
    announcement                          ann;
    moq_namespace_t                       ns_raw;
    std::span<const moq_resolved_token_t> tokens;

    namespace_name track_namespace() const { return {ns_raw}; }
};

struct namespace_done {
    announcement ann;
};

struct namespace_rejected {
    announcement     ann;
    request_error    error_code;
    bool             can_retry;
    uint64_t         retry_after_ms;
    std::string_view reason;
};

struct namespace_accepted {
    announcement ann;
};

struct namespace_cancelled {
    announcement     ann;
    request_error    error_code;
    std::string_view reason;
};

struct goaway {
    std::span<const uint8_t> new_session_uri;
};

/* A relay redirected a request (REQUEST_ERROR REDIRECT, §10.6.1). The request is
 * terminal; retry it on connect_uri (empty => reuse current) for the redirect
 * track name (empty => same as original). Select the typed handle by `family`. */
struct request_redirect {
    request_family           family;
    request_error            error_code;
    bool                     can_retry;
    uint64_t                 retry_after_ms;
    std::span<const uint8_t> connect_uri;
    moq_namespace_t          ns_raw;
    std::string_view         track_name;
    std::string_view         reason;
    uint64_t                 handle_raw;   // opaque handle; select by family

    namespace_name track_namespace() const { return {ns_raw}; }
    moq::subscription        as_subscription() const
        { return moq::subscription(moq_subscription_t{handle_raw}); }
    moq::fetch_handle        as_fetch() const
        { return moq::fetch_handle(moq_fetch_t{handle_raw}); }
    moq::track_status_handle as_track_status() const
        { return moq::track_status_handle(moq_track_status_handle_t{handle_raw}); }
    moq::announcement        as_announcement() const
        { return moq::announcement(moq_announcement_t{handle_raw}); }
    moq::ns_sub_handle       as_ns_sub() const
        { return moq::ns_sub_handle(moq_ns_sub_handle_t{handle_raw}); }
};

/* A peer sent a GOAWAY on this request's bidi (§10.4): migrate the single request
 * to new_session_uri (empty => reuse current) and re-issue it. The request is
 * terminal; select the typed handle by `family`. */
struct request_goaway {
    request_family           family;
    std::span<const uint8_t> new_session_uri;
    uint64_t                 timeout_ms;
    uint64_t                 handle_raw;   // opaque handle; select by family

    moq::subscription        as_subscription() const
        { return moq::subscription(moq_subscription_t{handle_raw}); }
    moq::fetch_handle        as_fetch() const
        { return moq::fetch_handle(moq_fetch_t{handle_raw}); }
    moq::track_status_handle as_track_status() const
        { return moq::track_status_handle(moq_track_status_handle_t{handle_raw}); }
    moq::announcement        as_announcement() const
        { return moq::announcement(moq_announcement_t{handle_raw}); }
    moq::ns_sub_handle       as_ns_sub() const
        { return moq::ns_sub_handle(moq_ns_sub_handle_t{handle_raw}); }
    moq::publication         as_publication() const
        { return moq::publication(moq_publication_t{handle_raw}); }
    moq::track_sub_handle    as_track_sub() const
        { return moq::track_sub_handle(moq_track_sub_handle_t{handle_raw}); }
};

struct unsubscribed {
    subscription sub;
};

struct subscribe_updated {
    subscription sub;
    bool         has_subscriber_priority;
    uint8_t      subscriber_priority;
    bool         has_forward;
    bool         forward;
    bool         has_delivery_timeout;
    uint64_t     delivery_timeout_us;
    std::span<const moq_resolved_token_t> tokens;
    bool         has_new_group_request;
    uint64_t     new_group_request;
};

struct object_chunk {
    subscription             sub;
    publication              pub;
    bool                     begin;
    bool                     end;
    object_terminal          terminal;
    uint64_t                 group_id;
    uint64_t                 subgroup_id;
    uint64_t                 object_id;
    uint8_t                  publisher_priority;
    object_status            status;
    bool                     end_of_group;
    uint64_t                 payload_length;
    std::span<const uint8_t> chunk_data;
    std::span<const uint8_t> properties_data;
    moq_rcbuf_t             *chunk_rcbuf;
    moq_rcbuf_t             *properties_rcbuf;
    /* Owners retained at variant() construction (see object_received) so the
     * chunk/properties buffers, spans, and raw pointers stay valid after the
     * polled_event is destroyed. Copy increfs; move steals. */
    buffer chunk_buf;
    buffer properties_buf;

    buffer chunk_owned() const noexcept { return chunk_buf; }
    buffer properties_owned() const noexcept { return properties_buf; }
};

struct ns_sub_request {
    ns_sub_handle                         handle;
    moq_namespace_t                       ns_raw;
    namespace_interest                    interest;
    bool                                  forward;
    std::span<const moq_resolved_token_t> tokens;

    namespace_name track_namespace_prefix() const { return {ns_raw}; }
};

struct ns_sub_ok {
    ns_sub_handle handle;
};

struct ns_sub_error {
    ns_sub_handle    handle;
    request_error    error_code;
    std::string_view reason;
};

struct namespace_found {
    ns_sub_handle   handle;
    moq_namespace_t ns_raw;

    namespace_name track_namespace_suffix() const { return {ns_raw}; }
};

struct namespace_gone {
    ns_sub_handle   handle;
    moq_namespace_t ns_raw;

    namespace_name track_namespace_suffix() const { return {ns_raw}; }
};

struct fetch_request {
    fetch_handle                          fetch;
    moq_namespace_t                       ns_raw;
    bytes_view                            track_name;
    subscription                          joining_sub;
    uint64_t                              start_group;
    uint64_t                              start_object;
    uint64_t                              end_group;
    uint64_t                              end_object;
    uint8_t                               subscriber_priority;
    group_order                           group_order;
    std::span<const moq_resolved_token_t> tokens;

    namespace_name track_namespace() const { return {ns_raw}; }
};

struct fetch_error {
    fetch_handle     fetch;
    request_error    error_code;
    bool             can_retry;
    uint64_t         retry_after_ms;
    std::string_view reason;
};

struct fetch_cancelled {
    fetch_handle fetch;
};

struct fetch_ok {
    fetch_handle             fetch;
    bool                     end_of_track;
    uint64_t                 end_group;
    uint64_t                 end_object;
    std::span<const uint8_t> track_properties;
};

struct fetch_complete {
    fetch_handle fetch;
};

struct fetch_object {
    fetch_handle             fetch;
    uint64_t                 group_id;
    uint64_t                 subgroup_id;   // 0 when datagram is true (no subgroup)
    uint64_t                 object_id;
    uint8_t                  publisher_priority;
    bool                     datagram;      // original forwarding preference was Datagram
    std::span<const uint8_t> payload_data;
    std::span<const uint8_t> properties_data;
    moq_rcbuf_t             *payload_rcbuf;
    moq_rcbuf_t             *properties_rcbuf;
    /* Owners retained at variant() construction (see object_received) so the
     * buffers, spans, and raw pointers stay valid after the polled_event is
     * destroyed. Copy increfs; move steals. */
    buffer payload_buf;
    buffer properties_buf;

    buffer payload_owned() const noexcept { return payload_buf; }
    buffer properties_owned() const noexcept { return properties_buf; }
};

struct fetch_gap {
    fetch_handle           fetch;
    fetch_range_kind       range_kind;
    uint64_t               group_id;
    uint64_t               object_id;
};

struct publish_request {
    publication                           pub;
    moq_namespace_t                       ns_raw;
    bytes_view                            track_name;
    uint64_t                              track_alias;
    bool                                  forward;
    std::span<const uint8_t>              track_properties;
    std::span<const moq_resolved_token_t> tokens;
    bool                                  dynamic_groups;

    namespace_name track_namespace() const { return {ns_raw}; }
};

struct publish_ok {
    publication       pub;
    bool              send_allowed;
    uint8_t           subscriber_priority;
    group_order       group_order;
    bool              has_delivery_timeout;
    uint64_t          delivery_timeout_ms;
    bool              has_expires;
    uint64_t          expires_ms;
    bool              has_new_group_request;
    uint64_t          new_group_request;
};

struct publish_error {
    publication      pub;
    request_error    error_code;
    bool             can_retry;
    uint64_t         retry_after_ms;
    std::string_view reason;
};

struct publish_finished {
    publication      pub;
    uint64_t         status_code;
    uint64_t         stream_count;
    std::string_view reason;
};

struct track_status_request {
    track_status_handle                   handle;
    moq_namespace_t                       ns_raw;
    bytes_view                            track_name;
    std::span<const moq_resolved_token_t> tokens;

    namespace_name track_namespace() const { return {ns_raw}; }
};

struct track_status_ok {
    track_status_handle handle;
    bool                has_largest;
    uint64_t            largest_group;
    uint64_t            largest_object;
    bool                has_expires;
    uint64_t            expires_ms;
};

struct track_status_error {
    track_status_handle handle;
    request_error       error_code;
    bool                can_retry;
    uint64_t            retry_after_ms;
    std::string_view    reason;
};

struct subscribe_tracks_request {
    track_sub_handle                      handle;
    moq_namespace_t                       ns_raw;
    bool                                  forward;
    std::span<const moq_resolved_token_t> tokens;

    namespace_name track_namespace_prefix() const { return {ns_raw}; }
};

struct subscribe_tracks_ok {
    track_sub_handle handle;
};

struct subscribe_tracks_error {
    track_sub_handle handle;
    request_error    error_code;
    bool             can_retry;
    uint64_t         retry_after_ms;
    std::string_view reason;
};

struct publish_blocked {
    track_sub_handle handle;
    moq_namespace_t  ns_raw;
    bytes_view       track_name;

    namespace_name track_namespace_suffix() const { return {ns_raw}; }
};

struct subscribe_tracks_cancelled {
    track_sub_handle handle;
};

struct subscribe_done {
    subscription     sub;
    uint64_t         status_code;
    uint64_t         stream_count;
    std::string_view reason;
};

struct publish_unsubscribed {
    publication pub;
};

struct publish_updated {
    publication pub;
    bool        has_subscriber_priority;
    uint8_t     subscriber_priority;
    bool        has_forward;
    bool        forward;
    bool        has_delivery_timeout;
    uint64_t    delivery_timeout_us;
    std::span<const moq_resolved_token_t> tokens;
    bool        has_new_group_request;
    uint64_t    new_group_request;
};

struct unknown {
    moq_event_kind_t kind;
};

} // namespace event

// clang-format off
using event_variant = std::variant<
    event::setup_complete,
    event::session_closed,
    event::subscribe_request,
    event::subscribe_ok,
    event::subscribe_error,
    event::request_ready,
    event::object_received,
    event::namespace_published,
    event::namespace_done,
    event::namespace_rejected,
    event::namespace_accepted,
    event::namespace_cancelled,
    event::goaway,
    event::request_redirect,
    event::request_goaway,
    event::unsubscribed,
    event::subscribe_updated,
    event::object_chunk,
    event::ns_sub_request,
    event::ns_sub_ok,
    event::ns_sub_error,
    event::namespace_found,
    event::namespace_gone,
    event::fetch_request,
    event::fetch_error,
    event::fetch_cancelled,
    event::fetch_ok,
    event::fetch_complete,
    event::fetch_object,
    event::fetch_gap,
    event::publish_request,
    event::publish_ok,
    event::publish_error,
    event::publish_finished,
    event::track_status_request,
    event::track_status_ok,
    event::track_status_error,
    event::subscribe_tracks_request,
    event::subscribe_tracks_ok,
    event::subscribe_tracks_error,
    event::publish_blocked,
    event::subscribe_tracks_cancelled,
    event::subscribe_done,
    event::publish_unsubscribed,
    event::publish_updated,
    event::unknown>;
// clang-format on

namespace detail {

inline std::string_view sv_from_bytes(moq_bytes_t b) noexcept
{
    return {reinterpret_cast<const char *>(b.data), b.len};
}

inline std::span<const uint8_t> span_from_rcbuf(moq_rcbuf_t *r) noexcept
{
    return {moq_rcbuf_data(r), moq_rcbuf_len(r)};
}

} // namespace detail

class polled_event {
    moq_event_t event_{};

public:
    explicit polled_event(moq_event_t e) noexcept : event_(e) {}

    ~polled_event() { moq_event_cleanup(&event_); }

    polled_event(polled_event &&o) noexcept : event_(o.event_)
    {
        std::memset(&o.event_, 0, sizeof(o.event_));
    }

    polled_event &operator=(polled_event &&o) noexcept
    {
        if (this != &o) {
            moq_event_cleanup(&event_);
            event_ = o.event_;
            std::memset(&o.event_, 0, sizeof(o.event_));
        }
        return *this;
    }

    polled_event(const polled_event &)            = delete;
    polled_event &operator=(const polled_event &) = delete;

    moq_event_kind_t kind() const noexcept { return event_.kind; }

    event_variant variant() const noexcept
    {
        using namespace detail;
        auto &e = event_;
        switch (e.kind) {

        case MOQ_EVENT_SETUP_COMPLETE: {
            auto &d = e.u.setup_complete;
            return event::setup_complete{
                perspective_from_c(d.local_perspective),
                perspective_from_c(d.peer_perspective),
                {d.tokens, d.token_count}};
        }
        case MOQ_EVENT_SESSION_CLOSED: {
            auto &d = e.u.closed;
            return event::session_closed{d.code, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_SUBSCRIBE_REQUEST: {
            auto &d = e.u.subscribe_request;
            return event::subscribe_request{
                subscription(d.sub),
                d.track_namespace,
                bytes_view(d.track_name),
                subscribe_filter_from_c(d.filter),
                d.subscriber_priority,
                group_order_from_c(d.group_order),
                d.forward,
                d.start_group,
                d.start_object,
                d.end_group,
                d.delivery_timeout_us,
                {d.tokens, d.token_count},
                d.has_new_group_request, d.new_group_request};
        }
        case MOQ_EVENT_SUBSCRIBE_OK: {
            auto &d = e.u.subscribe_ok;
            return event::subscribe_ok{
                subscription(d.sub), d.track_alias, d.has_largest,
                d.largest_group, d.largest_object, d.has_expires,
                d.expires_ms,
                {d.track_properties.data, d.track_properties.len},
                d.dynamic_groups};
        }
        case MOQ_EVENT_SUBSCRIBE_ERROR: {
            auto &d = e.u.subscribe_error;
            return event::subscribe_error{
                subscription(d.sub), request_error_from_c(d.error_code), d.can_retry,
                d.retry_after_ms, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_REQUEST_READY: {
            return event::request_ready{e.u.request_ready.available_requests};
        }
        case MOQ_EVENT_OBJECT_RECEIVED: {
            auto &d = e.u.object_received;
            return event::object_received{
                subscription(d.sub), publication(d.pub),
                d.group_id, d.subgroup_id, d.object_id,
                d.publisher_priority, object_status_from_c(d.status),
                d.end_of_group, d.datagram,
                span_from_rcbuf(d.payload), span_from_rcbuf(d.properties),
                d.payload, d.properties,
                buffer::retain(d.payload), buffer::retain(d.properties)};
        }
        case MOQ_EVENT_NAMESPACE_PUBLISHED: {
            auto &d = e.u.namespace_published;
            return event::namespace_published{
                announcement(d.ann), d.track_namespace,
                {d.tokens, d.token_count}};
        }
        case MOQ_EVENT_NAMESPACE_DONE:
            return event::namespace_done{announcement(e.u.namespace_done.ann)};

        case MOQ_EVENT_NAMESPACE_REJECTED: {
            auto &d = e.u.namespace_rejected;
            return event::namespace_rejected{
                announcement(d.ann), request_error_from_c(d.error_code), d.can_retry,
                d.retry_after_ms, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_NAMESPACE_ACCEPTED:
            return event::namespace_accepted{
                announcement(e.u.namespace_accepted.ann)};

        case MOQ_EVENT_NAMESPACE_CANCELLED: {
            auto &d = e.u.namespace_cancelled;
            return event::namespace_cancelled{
                announcement(d.ann), request_error_from_c(d.error_code), sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_GOAWAY:
            return event::goaway{
                {e.u.goaway.new_session_uri.data,
                 e.u.goaway.new_session_uri.len}};

        case MOQ_EVENT_REQUEST_REDIRECT: {
            auto &d = e.u.request_redirect;
            return event::request_redirect{
                request_family_from_c(d.family),
                request_error_from_c(d.error_code),
                d.can_retry, d.retry_after_ms,
                {d.connect_uri.data, d.connect_uri.len},
                d.track_namespace, sv_from_bytes(d.track_name),
                sv_from_bytes(d.reason), d.handle.raw};
        }

        case MOQ_EVENT_REQUEST_GOAWAY: {
            auto &d = e.u.request_goaway;
            return event::request_goaway{
                request_family_from_c(d.family),
                {d.new_session_uri.data, d.new_session_uri.len},
                d.timeout_ms, d.handle.raw};
        }

        case MOQ_EVENT_UNSUBSCRIBED:
            return event::unsubscribed{subscription(e.u.unsubscribed.sub)};

        case MOQ_EVENT_SUBSCRIBE_UPDATED: {
            auto &d = e.u.subscribe_updated;
            return event::subscribe_updated{
                subscription(d.sub), d.has_subscriber_priority,
                d.subscriber_priority, d.has_forward, d.forward,
                d.has_delivery_timeout, d.delivery_timeout_us,
                {d.tokens, d.token_count},
                d.has_new_group_request, d.new_group_request};
        }
        case MOQ_EVENT_OBJECT_CHUNK: {
            auto &d = e.u.object_chunk;
            return event::object_chunk{
                subscription(d.sub), publication(d.pub),
                d.begin, d.end, object_terminal_from_c(d.terminal),
                d.group_id, d.subgroup_id, d.object_id,
                d.publisher_priority, object_status_from_c(d.status),
                d.end_of_group,
                d.payload_length,
                span_from_rcbuf(d.chunk), span_from_rcbuf(d.properties),
                d.chunk, d.properties,
                buffer::retain(d.chunk), buffer::retain(d.properties)};
        }
        case MOQ_EVENT_NS_SUB_REQUEST: {
            auto &d = e.u.ns_sub_request;
            return event::ns_sub_request{
                ns_sub_handle(d.handle), d.track_namespace_prefix,
                namespace_interest_from_c(d.namespace_interest), d.forward,
                {d.tokens, d.token_count}};
        }
        case MOQ_EVENT_NS_SUB_OK:
            return event::ns_sub_ok{ns_sub_handle(e.u.ns_sub_ok.handle)};

        case MOQ_EVENT_NS_SUB_ERROR: {
            auto &d = e.u.ns_sub_error;
            return event::ns_sub_error{
                ns_sub_handle(d.handle), request_error_from_c(d.error_code),
                sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_NAMESPACE_FOUND: {
            auto &d = e.u.namespace_found;
            return event::namespace_found{
                ns_sub_handle(d.handle), d.track_namespace_suffix};
        }
        case MOQ_EVENT_NAMESPACE_GONE: {
            auto &d = e.u.namespace_gone;
            return event::namespace_gone{
                ns_sub_handle(d.handle), d.track_namespace_suffix};
        }
        case MOQ_EVENT_FETCH_REQUEST: {
            auto &d = e.u.fetch_request;
            return event::fetch_request{
                fetch_handle(d.fetch), d.track_namespace,
                bytes_view(d.track_name), subscription(d.joining_sub),
                d.start_group, d.start_object, d.end_group, d.end_object,
                d.subscriber_priority, group_order_from_c(d.group_order),
                {d.tokens, d.token_count}};
        }
        case MOQ_EVENT_FETCH_ERROR: {
            auto &d = e.u.fetch_error;
            return event::fetch_error{
                fetch_handle(d.fetch), request_error_from_c(d.error_code), d.can_retry,
                d.retry_after_ms, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_FETCH_CANCELLED:
            return event::fetch_cancelled{fetch_handle(e.u.fetch_cancelled.fetch)};

        case MOQ_EVENT_FETCH_OK: {
            auto &d = e.u.fetch_ok;
            return event::fetch_ok{
                fetch_handle(d.fetch), d.end_of_track,
                d.end_group, d.end_object,
                {d.track_properties.data, d.track_properties.len}};
        }
        case MOQ_EVENT_FETCH_COMPLETE:
            return event::fetch_complete{fetch_handle(e.u.fetch_complete.fetch)};

        case MOQ_EVENT_FETCH_OBJECT: {
            auto &d = e.u.fetch_object;
            return event::fetch_object{
                fetch_handle(d.fetch), d.group_id, d.subgroup_id,
                d.object_id, d.publisher_priority, d.datagram,
                span_from_rcbuf(d.payload), span_from_rcbuf(d.properties),
                d.payload, d.properties,
                buffer::retain(d.payload), buffer::retain(d.properties)};
        }
        case MOQ_EVENT_FETCH_GAP: {
            auto &d = e.u.fetch_gap;
            return event::fetch_gap{
                fetch_handle(d.fetch), fetch_range_kind_from_c(d.range_kind),
                d.group_id, d.object_id};
        }
        case MOQ_EVENT_PUBLISH_REQUEST: {
            auto &d = e.u.publish_request;
            return event::publish_request{
                publication(d.pub), d.track_namespace,
                bytes_view(d.track_name), d.track_alias, d.forward,
                {d.track_properties.data, d.track_properties.len},
                {d.tokens, d.token_count},
                d.dynamic_groups};
        }
        case MOQ_EVENT_PUBLISH_OK: {
            auto &d = e.u.publish_ok;
            return event::publish_ok{
                publication(d.pub), d.send_allowed, d.subscriber_priority,
                group_order_from_c(d.group_order),
                d.has_delivery_timeout, d.delivery_timeout_ms,
                d.has_expires, d.expires_ms,
                d.has_new_group_request, d.new_group_request};
        }
        case MOQ_EVENT_PUBLISH_ERROR: {
            auto &d = e.u.publish_error;
            return event::publish_error{
                publication(d.pub), request_error_from_c(d.error_code), d.can_retry,
                d.retry_after_ms, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_PUBLISH_FINISHED: {
            auto &d = e.u.publish_finished;
            return event::publish_finished{
                publication(d.pub), d.status_code, d.stream_count,
                sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_TRACK_STATUS_REQUEST: {
            auto &d = e.u.track_status_request;
            return event::track_status_request{
                track_status_handle(d.handle), d.track_namespace,
                bytes_view(d.track_name),
                {d.tokens, d.token_count}};
        }
        case MOQ_EVENT_TRACK_STATUS_OK: {
            auto &d = e.u.track_status_ok;
            return event::track_status_ok{
                track_status_handle(d.handle), d.has_largest,
                d.largest_group, d.largest_object,
                d.has_expires, d.expires_ms};
        }
        case MOQ_EVENT_TRACK_STATUS_ERROR: {
            auto &d = e.u.track_status_error;
            return event::track_status_error{
                track_status_handle(d.handle), request_error_from_c(d.error_code), d.can_retry,
                d.retry_after_ms, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST: {
            auto &d = e.u.subscribe_tracks_request;
            return event::subscribe_tracks_request{
                track_sub_handle(d.handle), d.track_namespace_prefix,
                d.forward, {d.tokens, d.token_count}};
        }
        case MOQ_EVENT_SUBSCRIBE_TRACKS_OK:
            return event::subscribe_tracks_ok{
                track_sub_handle(e.u.subscribe_tracks_ok.handle)};

        case MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR: {
            auto &d = e.u.subscribe_tracks_error;
            return event::subscribe_tracks_error{
                track_sub_handle(d.handle), request_error_from_c(d.error_code),
                d.can_retry, d.retry_after_ms, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_PUBLISH_BLOCKED: {
            auto &d = e.u.publish_blocked;
            return event::publish_blocked{
                track_sub_handle(d.handle), d.track_namespace_suffix,
                bytes_view(d.track_name)};
        }
        case MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED:
            return event::subscribe_tracks_cancelled{
                track_sub_handle(e.u.subscribe_tracks_cancelled.handle)};

        case MOQ_EVENT_SUBSCRIBE_DONE: {
            auto &d = e.u.subscribe_done;
            return event::subscribe_done{
                subscription(d.sub), d.status_code, d.stream_count,
                sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_PUBLISH_UNSUBSCRIBED:
            return event::publish_unsubscribed{
                publication(e.u.publish_unsubscribed.pub)};

        case MOQ_EVENT_PUBLISH_UPDATED: {
            auto &d = e.u.publish_updated;
            return event::publish_updated{
                publication(d.pub), d.has_subscriber_priority,
                d.subscriber_priority, d.has_forward, d.forward,
                d.has_delivery_timeout, d.delivery_timeout_us,
                {d.tokens, d.token_count},
                d.has_new_group_request, d.new_group_request};
        }
        default:
            return event::unknown{e.kind};
        }
    }

    template<typename... Visitors>
    decltype(auto) visit(Visitors &&...visitors) const
    {
        return std::visit(moq::overloaded{std::forward<Visitors>(visitors)...},
                          variant());
    }

    const moq_event_t &raw() const noexcept { return event_; }
};

} // namespace moq

#endif // MOQ_EVENT_HPP
