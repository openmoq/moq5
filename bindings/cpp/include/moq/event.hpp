#ifndef MOQ_EVENT_HPP
#define MOQ_EVENT_HPP

#include <cstddef>
#include <moq/buffer.hpp>
#include <moq/types.hpp>
#include <moq/visit.hpp>

#include <cstring>
#include <moq/operations.hpp>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <variant>

namespace moq {

/* Lifetime contract for event value alternatives:
 *
 * rcbuf-backed fields (payload/properties/chunk) are SELF-OWNING. Each such
 * alternative -- object_received, object_chunk, fetch_object and fill_object --
 * holds a retained moq::buffer (payload_buf / properties_buf / chunk_buf), so
 * its *_data spans, *_rcbuf raw pointers, and *_owned() stay valid for as long
 * as the value (or any copy of it) is alive -- even after the polled_event it
 * came from is destroyed and moq_event_cleanup() decrefs the C event's refs.
 * Copying an alternative increfs; moving steals.
 *
 * All OTHER fields remain BORROWED: spans, string_views, and token arrays point
 * into session output scratch and expire at the next advancing session call
 * even if the polled_event is still alive. Extended options are copied values;
 * they do not borrow the native options record. */
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
    moq::group_order                      group_order;
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

    /* Appended at the aggregate tail so shorter existing aggregate
     * initialization stays source-compatible. The first object ever
     * published in its subgroup (moq_object_received_event_t.first_object). */
    bool first_object = false;
    /* Appended. The Prior Group ID Gap / Prior Object ID Gap Object
     * Properties as received (draft-22 §10.8, §10.9); empty when absent or
     * when the C event predates them. */
    std::optional<uint64_t> prior_group_gap;
    std::optional<uint64_t> prior_object_gap;
    /* Appended. The subgroup header carried PROPERTIES
     * (moq_object_received_event_t.subgroup_properties). */
    bool subgroup_properties = false;

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
    /* The sender's Timeout in milliseconds, on drafts whose GOAWAY carries
     * one (18, 22; zero names no specific timeout); absent on draft-16 and
     * from a library that predates the field. */
    std::optional<uint64_t>  timeout_ms = {};
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
    moq::publication         as_publication() const
        { return moq::publication(moq_publication_t{handle_raw}); }
    moq::track_sub_handle    as_track_sub() const
        { return moq::track_sub_handle(moq_track_sub_handle_t{handle_raw}); }
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
    bool         has_filter;
    moq_subscribe_filter_t filter;
    uint64_t     start_group;
    uint64_t     start_object;
    uint64_t     end_group;
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

    /* Appended at the aggregate tail so shorter existing aggregate
     * initialization stays source-compatible. The peer's RESET_STREAM cause,
     * valid only when terminal == object_terminal::reset; zero otherwise,
     * including when the C event predates the field (see the detail_size
     * gate in the conversion). */
    uint64_t error_code = 0;
    /* Appended. Valid when begin: the first object ever published in its
     * subgroup (moq_object_chunk_event_t.first_object). */
    bool first_object = false;
    /* Appended. The Prior Group ID Gap / Prior Object ID Gap Object
     * Properties as received (draft-22 §10.8, §10.9); empty when absent or
     * when the C event predates them. */
    std::optional<uint64_t> prior_group_gap;
    std::optional<uint64_t> prior_object_gap;
    bool subgroup_properties = false;

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

/* What a FETCH request states beyond its range fields, on a draft whose
 * FETCH carries a Location Filter (draft-22); copied by value. With
 * range_unresolved (session_options::defer_fetch_ranges) start/end are the
 * request's explicit values and `location` is the form to resolve against
 * the Largest Object (moq_fetch_request_options_t documents the rule); an
 * absent location means the whole track through the Largest Object. */
struct fetch_request_options {
    bool                                  range_unresolved = false;
    std::optional<location_filter_config> location         = {};
    bool                                  has_fill_timeout = false;
    uint64_t                              fill_timeout_ms  = 0;
    bool                                  include_properties = true;
};

struct fetch_request {
    fetch_handle                          fetch;
    moq_namespace_t                       ns_raw;
    bytes_view                            track_name;
    subscription                          joining_sub;
    publication                           joining_pub;
    uint64_t                              start_group;
    uint64_t                              start_object;
    uint64_t                              end_group;
    uint64_t                              end_object;
    uint8_t                               subscriber_priority;
    moq::group_order                      group_order;
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

// Fetcher-side stream terminal (MOQ_EVENT_FETCH_RESET): error_code is the
// peer's QUIC STREAM application error code, not a request error.
struct fetch_reset {
    fetch_handle fetch;
    uint64_t     error_code;
    bool         data_stream;    // true: response data stream; false: request stream
    bool         stop_sending;   // request stream only: STOP_SENDING rather than RESET_STREAM
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
    /* Appended. The Prior Group ID Gap / Prior Object ID Gap Object
     * Properties as received (draft-22 §10.8, §10.9); empty when absent or
     * when the C event predates them. */
    std::optional<uint64_t> prior_group_gap;
    std::optional<uint64_t> prior_object_gap;

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
    /* Publisher-advertised Largest Object / EXPIRES (absent = nothing
     * published / no expiry advertised). */
    bool                                  has_largest;
    uint64_t                              largest_group;
    uint64_t                              largest_object;
    bool                                  has_expires;
    uint64_t                              expires_ms;

    namespace_name track_namespace() const { return {ns_raw}; }
};

struct publish_ok {
    publication       pub;
    bool              send_allowed;
    uint8_t           subscriber_priority;
    moq::group_order  group_order;
    bool              has_delivery_timeout;
    uint64_t          delivery_timeout_ms;
    bool              has_expires;
    uint64_t          expires_ms;
    bool              has_new_group_request;
    uint64_t          new_group_request;
    /* The subscriber's raw subscription filter (absent = unfiltered). */
    bool              has_filter;
    subscribe_filter  filter;
    uint64_t          start_group;
    uint64_t          start_object;
    uint64_t          end_group;
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
    /* The answer's Track Properties (borrowed; empty on draft-16, whose
     * answer has none, or when the producer's detail predates them). */
    std::span<const uint8_t> track_properties = {};
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
    bool        has_filter;
    moq_subscribe_filter_t filter;
    uint64_t    start_group;
    uint64_t    start_object;
    uint64_t    end_group;
};

struct subscription_update_ok {
    subscription sub;
    bool         has_largest;
    uint64_t     largest_group;
    uint64_t     largest_object;
    bool         has_expires;
    uint64_t     expires_ms;
};

struct publication_update_ok {
    publication pub;
    bool        has_largest;
    uint64_t    largest_group;
    uint64_t    largest_object;
    bool        has_expires;
    uint64_t    expires_ms;
};

/* Fill children (fill.h, draft-22 section 3.4). Exactly one parent handle is
 * valid: `sub` for a SUBSCRIBE-initiated subscription, `pub` for a
 * PUBLISH-initiated one. */
struct fill_created {
    fill_handle  fill;
    subscription sub;
    publication  pub;
    uint64_t     initiating_request_id;
};

/* The publisher owes a fill: the resolved inclusive range. */
struct fill_request {
    fill_handle  fill;
    subscription sub;
    publication  pub;
    uint64_t     initiating_request_id;
    uint64_t     start_group;
    uint64_t     start_object;
    uint64_t     end_group;
    uint64_t     end_object;
    uint64_t     processed_at_us;
    bool         has_fill_timeout;
    uint64_t     fill_timeout_ms;
    moq::group_order group_order;
    uint8_t      subscriber_priority;
};

struct fill_object {
    fill_handle              fill;
    subscription             sub;
    publication              pub;
    uint64_t                 initiating_request_id;
    uint64_t                 group_id;
    uint64_t                 subgroup_id;
    uint64_t                 object_id;
    uint8_t                  publisher_priority;
    bool                     datagram;
    std::span<const uint8_t> payload_data;
    std::span<const uint8_t> properties_data;
    moq_rcbuf_t             *payload_rcbuf;
    moq_rcbuf_t             *properties_rcbuf;
    /* Owners retained at variant() construction (see object_received) so the
     * buffers, spans, and raw pointers stay valid after the polled_event is
     * destroyed. Copy increfs; move steals. */
    buffer payload_buf;
    buffer properties_buf;
    /* Appended. The Prior Group ID Gap / Prior Object ID Gap Object
     * Properties as received (draft-22 §10.8, §10.9); empty when absent or
     * when the C event predates them. */
    std::optional<uint64_t> prior_group_gap;
    std::optional<uint64_t> prior_object_gap;

    buffer payload_owned() const noexcept { return payload_buf; }
    buffer properties_owned() const noexcept { return properties_buf; }
};

struct fill_gap {
    fill_handle      fill;
    subscription     sub;
    publication      pub;
    uint64_t         initiating_request_id;
    uint64_t         group_id;
    uint64_t         object_id;
    fetch_range_kind range_kind;
};

/* The child's end, exactly once; error_code is meaningful when the domain is
 * not `none` (zero is then a valid code). */
struct fill_terminal {
    fill_handle       fill;
    subscription      sub;
    publication       pub;
    uint64_t          initiating_request_id;
    fill_reason       reason;
    fill_error_domain error_domain;
    uint64_t          error_code;
};

/* A publisher's state notification on a subscription this side requested
 * (family subscribe, `sub`) or a publication it accepted (family publish,
 * `pub`). The extended view carries the notification's own location snapshot;
 * a latest-state query is not a historical observation. */
struct publish_state {
    request_family family;
    subscription   sub;
    publication    pub;
    bool           has_forward;
    bool           forward;
    bool           has_largest;
    bool           has_filter;
    uint64_t       largest_group;
    uint64_t       largest_object;
};

struct unknown {
    moq_event_kind_t kind;
};

} // namespace event

namespace extended_event {

/* Copied identity of an outgoing subgroup whose reset was committed locally.
 * Exactly one of sub/pub is valid. The handles identify a possibly retired
 * generation; no provider acceptance, peer ACK or owner liveness is implied. */
struct outgoing_subgroup_terminated {
    subgroup_handle subgroup;
    subscription sub;
    publication pub;
    uint64_t group_id;
    uint64_t subgroup_id;
    stream_ref stream;
    uint64_t error_code; /* Exact reset code; zero is valid. */
    outgoing_subgroup_terminal_cause cause;
};

/* Copied, pointer-free selected parameters, independent of the native epoch.
 * Flags preserve presence (including explicit NONE) and future flag bits;
 * a selected but short nested location is unavailable, not reconstructed. */
struct subscription_admission_options {
    uint32_t flags = 0;
    std::optional<location_filter_config> location = {};
    bool fill_requested = false;
    std::optional<uint64_t> object_delivery_timeout_ms = {};
    std::optional<uint64_t> subgroup_delivery_timeout_ms = {};
    /* The exact RENDEZVOUS_TIMEOUT milliseconds, including 0, when the
     * SUBSCRIBE carried it (§9.20.6). */
    std::optional<uint64_t> rendezvous_timeout_ms = {};
};

/* The draft-22 SUBSCRIBE_TRACKS options beyond its prefix and FORWARD
 * (polled_event::subscribe_tracks_options): the effective GROUP_ORDER and
 * INCLUDE_PROPERTIES, and which of them the request carried (flags). */
struct subscribe_tracks_options {
    uint32_t         flags              = 0;
    moq::group_order group_order        = moq::group_order::default_order;
    bool             include_properties = true;
};

struct subscribe_request : event::subscribe_request {
    std::optional<subscription_admission_options> options = {};
};

struct publish_state : event::publish_state {
    std::optional<location_filter_config> location = {};
};

struct unsubscribed : event::unsubscribed {
    request_terminal_cause cause = request_terminal_cause::none;
    uint64_t stream_error_code = 0;
};

struct fetch_cancelled : event::fetch_cancelled {
    request_terminal_cause cause = request_terminal_cause::none;
    uint64_t stream_error_code = 0;
};

struct fetch_reset : event::fetch_reset {
    request_terminal_cause cause = request_terminal_cause::none;
};

struct publish_finished : event::publish_finished {
    request_terminal_cause cause = request_terminal_cause::none;
    uint64_t stream_error_code = 0;
};

struct publish_unsubscribed : event::publish_unsubscribed {
    request_terminal_cause cause = request_terminal_cause::none;
    uint64_t stream_error_code = 0;
};

struct subscribe_tracks_cancelled : event::subscribe_tracks_cancelled {
    request_terminal_cause cause = request_terminal_cause::none;
    uint64_t stream_error_code = 0;
};

struct subscription_update_options {
    uint32_t flags = 0;
    uint64_t initiating_request_id = 0;
    std::optional<location_filter_config> location = {};
    std::optional<uint64_t> object_delivery_timeout_ms = {};
    std::optional<uint64_t> subgroup_delivery_timeout_ms = {};
};

struct subscribe_updated : event::subscribe_updated {
    std::optional<subscription_update_options> options = {};
    bool answer_owed = false;
    bool fill_requested = false;
    uint32_t update_count = 0;
    uint64_t update_request_id = 0;
};

struct publish_updated : event::publish_updated {
    bool answer_owed = false;
    bool fill_requested = false;
    uint32_t update_count = 0;
    uint64_t update_request_id = 0;
    std::optional<subscription_update_options> options = {};
};

struct fetch_request : event::fetch_request {
    std::optional<event::fetch_request_options> options = {};
};

/* Scalar terminal observations. Handles are identifiers after commitment;
 * stream_error_code has peer-error meaning only for RESET/STOP causes. */
struct track_status_terminated {
    track_status_handle handle;
    track_status_terminal_cause cause;
    request_role local_role;
    uint64_t stream_error_code;
};

struct namespace_terminated {
    announcement ann;
    namespace_terminal_cause cause;
    request_role local_role;
    uint64_t stream_error_code;
};

struct namespace_subscription_terminated {
    ns_sub_handle handle;
    request_terminal_cause cause;
    request_role local_role;
    uint64_t stream_error_code;
};

/* Draft-22 namespace updates (§9.5). *_update_request: the peer updated a
 * request this side answers and the answer is owed
 * (session::set_defer_namespace_updates). *_update_result: an update
 * concluded -- the peer's answer to this side's update, or this side's own
 * refusal (outcome refused; the handle is stale after it). */
struct namespace_update_request {
    announcement            ann;
    uint64_t                update_request_id;
    namespace_update_record update;
};

struct ns_sub_update_request {
    ns_sub_handle           handle;
    uint64_t                update_request_id;
    namespace_update_record update;
};

struct subscribe_tracks_update_request {
    track_sub_handle        handle;
    uint64_t                update_request_id;
    namespace_update_record update;
};

enum class namespace_update_outcome : uint32_t {
    ok      = MOQ_NAMESPACE_UPDATE_OUTCOME_OK,
    error   = MOQ_NAMESPACE_UPDATE_OUTCOME_ERROR,
    refused = MOQ_NAMESPACE_UPDATE_OUTCOME_REFUSED,
};

struct namespace_update_conclusion {
    uint64_t                 update_request_id;
    namespace_update_outcome outcome;
    request_role             local_role;
    request_error            error_code;
    std::string_view         reason;
    std::optional<uint64_t>  expires_ms;
};

struct namespace_update_result : namespace_update_conclusion {
    announcement ann;
};

struct ns_sub_update_result : namespace_update_conclusion {
    ns_sub_handle handle;
};

struct subscribe_tracks_update_result : namespace_update_conclusion {
    track_sub_handle handle;
};

} // namespace extended_event

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
    event::subscription_update_ok,
    event::publication_update_ok,
    event::fetch_reset,
    event::unknown>;

using extended_event_variant = std::variant<
    event::setup_complete,
    event::session_closed,
    extended_event::subscribe_request,
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
    extended_event::unsubscribed,
    extended_event::subscribe_updated,
    event::object_chunk,
    event::ns_sub_request,
    event::ns_sub_ok,
    event::ns_sub_error,
    event::namespace_found,
    event::namespace_gone,
    extended_event::fetch_request,
    event::fetch_error,
    extended_event::fetch_cancelled,
    event::fetch_ok,
    event::fetch_complete,
    event::fetch_object,
    event::fetch_gap,
    event::publish_request,
    event::publish_ok,
    event::publish_error,
    extended_event::publish_finished,
    event::track_status_request,
    event::track_status_ok,
    event::track_status_error,
    event::subscribe_tracks_request,
    event::subscribe_tracks_ok,
    event::subscribe_tracks_error,
    event::publish_blocked,
    extended_event::subscribe_tracks_cancelled,
    event::subscribe_done,
    extended_event::publish_unsubscribed,
    extended_event::publish_updated,
    event::subscription_update_ok,
    event::publication_update_ok,
    extended_event::fetch_reset,
    event::fill_created,
    event::fill_request,
    event::fill_object,
    event::fill_gap,
    event::fill_terminal,
    extended_event::publish_state,
    extended_event::track_status_terminated,
    extended_event::namespace_terminated,
    extended_event::namespace_subscription_terminated,
    extended_event::namespace_update_request,
    extended_event::ns_sub_update_request,
    extended_event::subscribe_tracks_update_request,
    extended_event::namespace_update_result,
    extended_event::ns_sub_update_result,
    extended_event::subscribe_tracks_update_result,
    extended_event::outgoing_subgroup_terminated,
    event::unknown>;
// clang-format on

namespace detail {

inline std::string_view sv_from_bytes(moq_bytes_t b) noexcept
{
    return {reinterpret_cast<const char *>(b.data), b.len};
}

/* The C detail covers a field that ends at `end_off + 8` (the appended
 * uint64_t tails are read only when covered). */
inline bool covers(const moq_event_t &e, size_t end_off) noexcept
{
    return e.detail_size >= end_off + sizeof(uint64_t);
}

inline std::optional<uint64_t> gap_of(const moq_event_t &e, size_t end_off,
                                      bool has, uint64_t value) noexcept
{
    if (!covers(e, end_off) || !has) return std::nullopt;
    return value;
}

inline std::span<const uint8_t> span_from_rcbuf(moq_rcbuf_t *r) noexcept
{
    return {moq_rcbuf_data(r), moq_rcbuf_len(r)};
}

/* An appended event field is read only when the producer's detail covers
 * all of it; an older producer's event reads it as absent (zero). */
#define MOQ_CPP_DETAIL_COVERS_(e, type, field)                                 \
    ((e).detail_size >= offsetof(type, field) + sizeof(((type *)nullptr)->field))

inline std::optional<location_filter_config>
location_snapshot_from_c(const moq_location_filter_cfg_t *pointer) noexcept
{
    if (!pointer) return std::nullopt;
    uint32_t size = 0;
    std::memcpy(&size, pointer, sizeof(size));
    if (size < MOQ_LOCATION_FILTER_CFG_V0_SIZE) return std::nullopt;
    moq_location_filter_cfg_t value{};
    std::memcpy(&value, pointer, MOQ_LOCATION_FILTER_CFG_V0_SIZE);
    return location_from_c(value);
}

inline std::optional<extended_event::subscription_admission_options>
admission_options_from_c(const moq_event_t &e) noexcept
{
    const auto &d = e.u.subscribe_request;
    if (!MOQ_CPP_DETAIL_COVERS_(e, moq_subscribe_request_event_t, options) || !d.options)
        return std::nullopt;
    uint32_t size = 0;
    std::memcpy(&size, d.options, sizeof(size));
    if (size < MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE) return std::nullopt;
    moq_subscription_admission_options_t value{};
    std::memcpy(&value, d.options, MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE);
    extended_event::subscription_admission_options out{};
    out.flags = value.flags;
    if (value.flags & MOQ_SUBSCRIPTION_ADMISSION_LOCATION)
        out.location = location_snapshot_from_c(&value.location);
    out.fill_requested = (value.flags & MOQ_SUBSCRIPTION_ADMISSION_FILL) != 0;
    if (value.flags & MOQ_SUBSCRIPTION_ADMISSION_OBJECT_DELIVERY_TIMEOUT)
        out.object_delivery_timeout_ms = value.object_delivery_timeout_ms;
    if (value.flags & MOQ_SUBSCRIPTION_ADMISSION_SUBGROUP_DELIVERY_TIMEOUT)
        out.subgroup_delivery_timeout_ms = value.subgroup_delivery_timeout_ms;
    constexpr size_t rv_off =
        offsetof(moq_subscription_admission_options_t, rendezvous_timeout_ms);
    if ((value.flags & MOQ_SUBSCRIPTION_ADMISSION_RENDEZVOUS_TIMEOUT) &&
        size >= rv_off + sizeof(uint64_t)) {
        uint64_t ms = 0;
        std::memcpy(&ms, reinterpret_cast<const unsigned char *>(d.options) + rv_off,
                    sizeof(ms));
        out.rendezvous_timeout_ms = ms;
    }
    return out;
}

inline namespace_update_record
update_record_view(const moq_namespace_update_record_t *r) noexcept
{
    namespace_update_record out{};
    if (r == nullptr || r->struct_size < MOQ_NAMESPACE_UPDATE_RECORD_V0_SIZE)
        return out;
    out.update_request_id = r->update_request_id;
    if (r->flags & MOQ_NAMESPACE_UPDATE_PREFIX)
        out.prefix_raw = r->track_namespace_prefix;
    if (r->flags & MOQ_NAMESPACE_UPDATE_FORWARD)
        out.forward = r->forward;
    out.has_authorization = (r->flags & MOQ_NAMESPACE_UPDATE_AUTHORIZATION) != 0;
    out.tokens = {r->tokens, r->token_count};
    return out;
}

template <class R>
inline extended_event::namespace_update_conclusion
update_conclusion_from_c(const R &d) noexcept
{
    extended_event::namespace_update_conclusion out{};
    out.update_request_id = d.update_request_id;
    out.outcome = static_cast<extended_event::namespace_update_outcome>(d.outcome);
    out.local_role = request_role_from_c(d.local_role);
    out.error_code = request_error_from_c(d.error_code);
    out.reason = sv_from_bytes(d.reason);
    if (d.has_expires)
        out.expires_ms = d.expires_ms;
    return out;
}

template <class D>
inline request_terminal_cause cause_of(const moq_event_t &e, const D &d,
                                       std::size_t end) noexcept
{
    return e.detail_size >= end ? request_terminal_cause_from_c(d.cause)
                                : request_terminal_cause::none;
}

inline std::optional<event::fetch_request_options>
fetch_options_from_c(const moq_event_t &e) noexcept
{
    const auto &d = e.u.fetch_request;
    if (!MOQ_CPP_DETAIL_COVERS_(e, moq_fetch_request_event_t, options) ||
        d.options == nullptr)
        return std::nullopt;
    uint32_t size = 0;
    std::memcpy(&size, d.options, sizeof(size));
    if (size < MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE)
        return std::nullopt;
    // An older producer may have allocated only the frozen value prefix.
    moq_fetch_request_options_t o{};
    std::memcpy(&o, d.options, MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE);
    if (o.location.struct_size < MOQ_LOCATION_FILTER_CFG_V0_SIZE)
        return std::nullopt;
    constexpr size_t deferred_offset = offsetof(moq_fetch_request_options_t, range_unresolved);
    if (size >= deferred_offset + sizeof(o.range_unresolved))
        std::memcpy(&o.range_unresolved,
                    reinterpret_cast<const unsigned char *>(d.options) + deferred_offset,
                    sizeof(o.range_unresolved));
    event::fetch_request_options out{};
    out.range_unresolved = o.range_unresolved;
    out.has_fill_timeout = (o.flags & MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT) != 0;
    out.fill_timeout_ms = out.has_fill_timeout ? o.fill_timeout_ms : 0;
    out.include_properties = (o.flags & MOQ_FETCH_REQUEST_INCLUDE_PROPERTIES) != 0;
    if (o.location.kind != MOQ_LOCATION_FILTER_NONE)
        out.location = location_from_c(o.location);
    return out;
}

inline std::optional<extended_event::subscription_update_options>
update_options_from_c(const moq_subscription_update_options_t *options) noexcept
{
    if (!options) return std::nullopt;
    const auto &o = *options;
    if (o.struct_size < offsetof(moq_subscription_update_options_t, subgroup_delivery_timeout_ms) +
                            sizeof(o.subgroup_delivery_timeout_ms))
        return std::nullopt;
    extended_event::subscription_update_options out{};
    out.flags = o.flags;
    out.initiating_request_id = o.initiating_request_id;
    if ((o.flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION) &&
        o.location.struct_size >= MOQ_LOCATION_FILTER_CFG_V0_SIZE)
        out.location = location_from_c(o.location);
    if (o.flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_OBJECT_DELIVERY_TIMEOUT)
        out.object_delivery_timeout_ms = o.object_delivery_timeout_ms;
    if (o.flags & MOQ_SUBSCRIPTION_UPDATE_OPTIONS_SUBGROUP_DELIVERY_TIMEOUT)
        out.subgroup_delivery_timeout_ms = o.subgroup_delivery_timeout_ms;
    return out;
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

    extended_event_variant variant_extended() const noexcept
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
            if (e.detail_size < offsetof(moq_subscribe_request_event_t, options))
                return event::unknown{e.kind};
            auto &d = e.u.subscribe_request;
            return extended_event::subscribe_request{event::subscribe_request{
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
                d.has_new_group_request, d.new_group_request}, admission_options_from_c(e)};
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
                buffer::retain(d.payload), buffer::retain(d.properties),
                d.first_object,
                detail::gap_of(e, offsetof(moq_object_received_event_t, prior_object_gap),
                               d.has_prior_group_gap, d.prior_group_gap),
                detail::gap_of(e, offsetof(moq_object_received_event_t, prior_object_gap),
                               d.has_prior_object_gap, d.prior_object_gap),
                detail::covers(e, offsetof(moq_object_received_event_t, prior_object_gap)) &&
                    d.subgroup_properties};
        }
        case MOQ_EVENT_NAMESPACE_PUBLISHED: {
            auto &d = e.u.namespace_published;
            return event::namespace_published{
                announcement(d.ann), d.track_namespace,
                {d.tokens, d.token_count}};
        }
        case MOQ_EVENT_NAMESPACE_DONE: {
            if (e.detail_size < sizeof(moq_namespace_done_event_t))
                return event::unknown{e.kind};
            auto &d = e.u.namespace_done;
            return event::namespace_done{announcement(d.ann)};
        }

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
            if (e.detail_size < offsetof(moq_namespace_cancelled_event_t, reason) +
                                    sizeof(moq_bytes_t))
                return event::unknown{e.kind};
            auto &d = e.u.namespace_cancelled;
            return event::namespace_cancelled{
                announcement(d.ann), request_error_from_c(d.error_code), sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_GOAWAY: {
            auto &d = e.u.goaway;
            std::optional<uint64_t> timeout;
            if (MOQ_CPP_DETAIL_COVERS_(e, moq_goaway_event_t, timeout_ms) && d.has_timeout)
                timeout = d.timeout_ms;
            return event::goaway{{d.new_session_uri.data, d.new_session_uri.len},
                                 timeout};
        }

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

        case MOQ_EVENT_UNSUBSCRIBED: {
            auto &d = e.u.unsubscribed;
            return extended_event::unsubscribed{
                {subscription(d.sub)},
                cause_of(e, d, offsetof(moq_unsubscribed_event_t, cause) + sizeof(d.cause)),
                MOQ_CPP_DETAIL_COVERS_(e, moq_unsubscribed_event_t, stream_error_code)
                    ? d.stream_error_code : 0u};
        }

        case MOQ_EVENT_SUBSCRIBE_UPDATED: {
            auto &d = e.u.subscribe_updated;
            return extended_event::subscribe_updated{{
                subscription(d.sub), d.has_subscriber_priority,
                d.subscriber_priority, d.has_forward, d.forward,
                d.has_delivery_timeout, d.delivery_timeout_us,
                {d.tokens, d.token_count},
                d.has_new_group_request, d.new_group_request,
                d.has_filter, d.filter,
                d.start_group, d.start_object, d.end_group},
                update_options_from_c(
                    MOQ_CPP_DETAIL_COVERS_(e, moq_subscribe_updated_event_t, options)
                        ? d.options : nullptr),
                MOQ_CPP_DETAIL_COVERS_(e, moq_subscribe_updated_event_t, answer_owed)
                    && d.answer_owed,
                MOQ_CPP_DETAIL_COVERS_(e, moq_subscribe_updated_event_t, fill_requested)
                    && d.fill_requested,
                MOQ_CPP_DETAIL_COVERS_(e, moq_subscribe_updated_event_t, update_count)
                    ? d.update_count : 0u,
                MOQ_CPP_DETAIL_COVERS_(e, moq_subscribe_updated_event_t, update_request_id)
                    ? d.update_request_id : 0u};
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
                buffer::retain(d.chunk), buffer::retain(d.properties),
                /* The cause is exposed only when it MEANS something: the
                 * terminal must be a RESET, and the C detail must cover the
                 * whole appended field. Otherwise zero -- so neither an older
                 * event's tail nor a NORMAL/STOP terminal's physical bytes
                 * escape as a cause. */
                (d.terminal == MOQ_OBJECT_TERMINAL_RESET &&
                 e.detail_size >= offsetof(moq_object_chunk_event_t, error_code) +
                                      sizeof(d.error_code))
                    ? d.error_code
                    : 0u,
                d.first_object,
                detail::gap_of(e, offsetof(moq_object_chunk_event_t, prior_object_gap),
                               d.begin && d.has_prior_group_gap, d.prior_group_gap),
                detail::gap_of(e, offsetof(moq_object_chunk_event_t, prior_object_gap),
                               d.begin && d.has_prior_object_gap, d.prior_object_gap),
                detail::covers(e, offsetof(moq_object_chunk_event_t, prior_object_gap)) &&
                    d.begin && d.subgroup_properties};
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
            return extended_event::fetch_request{{
                fetch_handle(d.fetch), d.track_namespace,
                bytes_view(d.track_name), subscription(d.joining_sub),
                publication(d.joining_pub), d.start_group, d.start_object, d.end_group, d.end_object,
                d.subscriber_priority, group_order_from_c(d.group_order),
                {d.tokens, d.token_count}},
                fetch_options_from_c(e)};
        }
        case MOQ_EVENT_FETCH_ERROR: {
            auto &d = e.u.fetch_error;
            return event::fetch_error{
                fetch_handle(d.fetch), request_error_from_c(d.error_code), d.can_retry,
                d.retry_after_ms, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_FETCH_CANCELLED: {
            auto &d = e.u.fetch_cancelled;
            return extended_event::fetch_cancelled{
                {fetch_handle(d.fetch)},
                cause_of(e, d, offsetof(moq_fetch_cancelled_event_t, cause) + sizeof(d.cause)),
                MOQ_CPP_DETAIL_COVERS_(e, moq_fetch_cancelled_event_t, stream_error_code)
                    ? d.stream_error_code : 0u};
        }

        case MOQ_EVENT_FETCH_OK: {
            auto &d = e.u.fetch_ok;
            return event::fetch_ok{
                fetch_handle(d.fetch), d.end_of_track,
                d.end_group, d.end_object,
                {d.track_properties.data, d.track_properties.len}};
        }
        case MOQ_EVENT_FETCH_COMPLETE:
            return event::fetch_complete{fetch_handle(e.u.fetch_complete.fetch)};
        case MOQ_EVENT_FETCH_RESET: {
            auto &d = e.u.fetch_reset;
            return extended_event::fetch_reset{
                {fetch_handle(d.fetch), d.error_code, d.data_stream, d.stop_sending},
                cause_of(e, d, offsetof(moq_fetch_reset_event_t, cause) + sizeof(d.cause))};
        }

        case MOQ_EVENT_FETCH_OBJECT: {
            auto &d = e.u.fetch_object;
            return event::fetch_object{
                fetch_handle(d.fetch), d.group_id, d.subgroup_id,
                d.object_id, d.publisher_priority, d.datagram,
                span_from_rcbuf(d.payload), span_from_rcbuf(d.properties),
                d.payload, d.properties,
                buffer::retain(d.payload), buffer::retain(d.properties),
                detail::gap_of(e, offsetof(moq_fetch_object_event_t, prior_object_gap),
                               d.has_prior_group_gap, d.prior_group_gap),
                detail::gap_of(e, offsetof(moq_fetch_object_event_t, prior_object_gap),
                               d.has_prior_object_gap, d.prior_object_gap)};
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
                d.dynamic_groups,
                d.has_largest, d.largest_group, d.largest_object,
                d.has_expires, d.expires_ms};
        }
        case MOQ_EVENT_PUBLISH_OK: {
            auto &d = e.u.publish_ok;
            return event::publish_ok{
                publication(d.pub), d.send_allowed, d.subscriber_priority,
                group_order_from_c(d.group_order),
                d.has_delivery_timeout, d.delivery_timeout_ms,
                d.has_expires, d.expires_ms,
                d.has_new_group_request, d.new_group_request,
                d.has_filter, subscribe_filter_from_c(d.filter),
                d.start_group, d.start_object, d.end_group};
        }
        case MOQ_EVENT_PUBLISH_ERROR: {
            auto &d = e.u.publish_error;
            return event::publish_error{
                publication(d.pub), request_error_from_c(d.error_code), d.can_retry,
                d.retry_after_ms, sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_PUBLISH_FINISHED: {
            auto &d = e.u.publish_finished;
            return extended_event::publish_finished{{
                publication(d.pub), d.status_code, d.stream_count,
                sv_from_bytes(d.reason)},
                cause_of(e, d, offsetof(moq_publish_finished_event_t, cause) + sizeof(d.cause)),
                MOQ_CPP_DETAIL_COVERS_(e, moq_publish_finished_event_t, stream_error_code)
                    ? d.stream_error_code : 0u};
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
            const bool props = e.detail_size >=
                offsetof(moq_track_status_ok_event_t, track_properties) +
                sizeof(d.track_properties);
            return event::track_status_ok{
                track_status_handle(d.handle), d.has_largest,
                d.largest_group, d.largest_object,
                d.has_expires, d.expires_ms,
                props ? std::span<const uint8_t>{d.track_properties.data,
                                                 d.track_properties.len}
                      : std::span<const uint8_t>{}};
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
        case MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED: {
            auto &d = e.u.subscribe_tracks_cancelled;
            return extended_event::subscribe_tracks_cancelled{
                {track_sub_handle(d.handle)},
                cause_of(e, d, offsetof(moq_subscribe_tracks_cancelled_event_t, cause) +
                                   sizeof(d.cause)),
                MOQ_CPP_DETAIL_COVERS_(e, moq_subscribe_tracks_cancelled_event_t,
                                       stream_error_code)
                    ? d.stream_error_code : 0u};
        }

        case MOQ_EVENT_SUBSCRIBE_DONE: {
            auto &d = e.u.subscribe_done;
            return event::subscribe_done{
                subscription(d.sub), d.status_code, d.stream_count,
                sv_from_bytes(d.reason)};
        }
        case MOQ_EVENT_PUBLISH_UNSUBSCRIBED: {
            auto &d = e.u.publish_unsubscribed;
            return extended_event::publish_unsubscribed{
                {publication(d.pub)},
                cause_of(e, d, offsetof(moq_publish_unsubscribed_event_t, cause) + sizeof(d.cause)),
                MOQ_CPP_DETAIL_COVERS_(e, moq_publish_unsubscribed_event_t, stream_error_code)
                    ? d.stream_error_code : 0u};
        }

        case MOQ_EVENT_PUBLISH_UPDATED: {
            auto &d = e.u.publish_updated;
            return extended_event::publish_updated{{
                publication(d.pub), d.has_subscriber_priority,
                d.subscriber_priority, d.has_forward, d.forward,
                d.has_delivery_timeout, d.delivery_timeout_us,
                {d.tokens, d.token_count},
                d.has_new_group_request, d.new_group_request,
                d.has_filter, d.filter,
                d.start_group, d.start_object, d.end_group},
                MOQ_CPP_DETAIL_COVERS_(e, moq_publish_updated_event_t, answer_owed)
                    && d.answer_owed,
                MOQ_CPP_DETAIL_COVERS_(e, moq_publish_updated_event_t, fill_requested)
                    && d.fill_requested,
                MOQ_CPP_DETAIL_COVERS_(e, moq_publish_updated_event_t, update_count)
                    ? d.update_count : 0u,
                MOQ_CPP_DETAIL_COVERS_(e, moq_publish_updated_event_t, update_request_id)
                    ? d.update_request_id : 0u,
                update_options_from_c(
                    MOQ_CPP_DETAIL_COVERS_(e, moq_publish_updated_event_t, options)
                        ? d.options : nullptr)};
        }

        case MOQ_EVENT_SUBSCRIPTION_UPDATE_OK: {
            auto &d = e.u.subscription_update_ok;
            return event::subscription_update_ok{
                subscription(d.sub), d.has_largest,
                d.largest_group, d.largest_object,
                d.has_expires, d.expires_ms};
        }
        case MOQ_EVENT_PUBLICATION_UPDATE_OK: {
            auto &d = e.u.publication_update_ok;
            return event::publication_update_ok{
                publication(d.pub), d.has_largest,
                d.largest_group, d.largest_object,
                d.has_expires, d.expires_ms};
        }
        /* Fill events need their whole frozen prefix (session.h); a shorter
         * detail is an older producer and surfaces as unknown rather than as
         * a half-read record. */
        case MOQ_EVENT_FILL_CREATED: {
            if (e.detail_size < MOQ_FILL_CREATED_EVENT_V0_SIZE)
                return event::unknown{e.kind};
            auto &d = e.u.fill_created;
            return event::fill_created{
                fill_handle(d.fill), subscription(d.sub), publication(d.pub),
                d.initiating_request_id};
        }
        case MOQ_EVENT_FILL_REQUEST: {
            if (e.detail_size < MOQ_FILL_REQUEST_EVENT_V0_SIZE)
                return event::unknown{e.kind};
            auto &d = e.u.fill_request;
            return event::fill_request{
                fill_handle(d.fill), subscription(d.sub), publication(d.pub),
                d.initiating_request_id,
                d.start_group, d.start_object, d.end_group, d.end_object,
                d.processed_at_us, d.has_fill_timeout, d.fill_timeout_ms,
                group_order_from_c(d.group_order), d.subscriber_priority};
        }
        case MOQ_EVENT_FILL_OBJECT: {
            if (e.detail_size < MOQ_FILL_OBJECT_EVENT_V0_SIZE)
                return event::unknown{e.kind};
            auto &d = e.u.fill_object;
            return event::fill_object{
                fill_handle(d.fill), subscription(d.sub), publication(d.pub),
                d.initiating_request_id,
                d.group_id, d.subgroup_id, d.object_id,
                d.publisher_priority, d.datagram,
                span_from_rcbuf(d.payload), span_from_rcbuf(d.properties),
                d.payload, d.properties,
                buffer::retain(d.payload), buffer::retain(d.properties),
                detail::gap_of(e, offsetof(moq_fill_object_event_t, prior_object_gap),
                               d.has_prior_group_gap, d.prior_group_gap),
                detail::gap_of(e, offsetof(moq_fill_object_event_t, prior_object_gap),
                               d.has_prior_object_gap, d.prior_object_gap)};
        }
        case MOQ_EVENT_FILL_GAP: {
            if (e.detail_size < MOQ_FILL_GAP_EVENT_V0_SIZE)
                return event::unknown{e.kind};
            auto &d = e.u.fill_gap;
            return event::fill_gap{
                fill_handle(d.fill), subscription(d.sub), publication(d.pub),
                d.initiating_request_id, d.group_id, d.object_id,
                fetch_range_kind_from_c(static_cast<moq_fetch_range_kind_t>(d.kind))};
        }
        case MOQ_EVENT_FILL_TERMINAL: {
            if (e.detail_size < MOQ_FILL_TERMINAL_EVENT_V0_SIZE)
                return event::unknown{e.kind};
            auto &d = e.u.fill_terminal;
            return event::fill_terminal{
                fill_handle(d.fill), subscription(d.sub), publication(d.pub),
                d.initiating_request_id,
                fill_reason_from_c(d.reason),
                fill_error_domain_from_c(d.error_domain), d.error_code};
        }
        case MOQ_EVENT_TRACK_STATUS_TERMINATED: {
            if (e.detail_size < sizeof(moq_track_status_terminated_event_t))
                return event::unknown{e.kind};
            const auto &d = e.u.track_status_terminated;
            return extended_event::track_status_terminated{
                track_status_handle(d.handle),
                static_cast<track_status_terminal_cause>(d.cause),
                static_cast<request_role>(d.local_role), d.stream_error_code};
        }
        case MOQ_EVENT_NAMESPACE_TERMINATED: {
            if (e.detail_size < sizeof(moq_namespace_terminated_event_t))
                return event::unknown{e.kind};
            const auto &d = e.u.namespace_terminated;
            return extended_event::namespace_terminated{
                announcement(d.ann), static_cast<namespace_terminal_cause>(d.cause),
                static_cast<request_role>(d.local_role), d.stream_error_code};
        }
        case MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED: {
            if (e.detail_size < sizeof(moq_ns_subscription_terminated_event_t))
                return event::unknown{e.kind};
            const auto &d = e.u.ns_subscription_terminated;
            return extended_event::namespace_subscription_terminated{
                ns_sub_handle(d.handle), request_terminal_cause_from_c(d.cause),
                request_role_from_c(d.local_role), d.stream_error_code};
        }
        case MOQ_EVENT_PUBLISH_STATE: {
            if (e.detail_size < offsetof(moq_publish_state_event_t, location))
                return event::unknown{e.kind};
            auto &d = e.u.publish_state;
            if (d.family != MOQ_REQUEST_FAMILY_SUBSCRIBE && d.family != MOQ_REQUEST_FAMILY_PUBLISH)
                return event::unknown{e.kind};
            std::optional<location_filter_config> location;
            if (d.has_filter) {
                if (!MOQ_CPP_DETAIL_COVERS_(e, moq_publish_state_event_t, location))
                    return event::unknown{e.kind};
                location = location_snapshot_from_c(d.location);
                if (!location) return event::unknown{e.kind};
            }
            const bool is_sub = d.family == MOQ_REQUEST_FAMILY_SUBSCRIBE;
            return extended_event::publish_state{event::publish_state{
                request_family_from_c(d.family),
                is_sub ? subscription(d.handle.subscription) : subscription{},
                is_sub ? publication{} : publication(d.handle.publication),
                d.has_forward, d.forward, d.has_largest, d.has_filter,
                d.largest_group, d.largest_object}, location};
        }
        case MOQ_EVENT_NAMESPACE_UPDATE_REQUEST: {
            if (e.detail_size < sizeof(moq_namespace_update_request_event_t))
                return event::unknown{e.kind};
            const auto &d = e.u.namespace_update_request;
            return extended_event::namespace_update_request{
                announcement(d.ann), d.update_request_id,
                update_record_view(d.update)};
        }
        case MOQ_EVENT_NS_SUB_UPDATE_REQUEST: {
            if (e.detail_size < sizeof(moq_ns_sub_update_request_event_t))
                return event::unknown{e.kind};
            const auto &d = e.u.ns_sub_update_request;
            return extended_event::ns_sub_update_request{
                ns_sub_handle(d.handle), d.update_request_id,
                update_record_view(d.update)};
        }
        case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST: {
            if (e.detail_size < sizeof(moq_subscribe_tracks_update_request_event_t))
                return event::unknown{e.kind};
            const auto &d = e.u.subscribe_tracks_update_request;
            return extended_event::subscribe_tracks_update_request{
                track_sub_handle(d.handle), d.update_request_id,
                update_record_view(d.update)};
        }
        case MOQ_EVENT_NAMESPACE_UPDATE_RESULT: {
            if (e.detail_size < offsetof(moq_namespace_update_result_event_t, expires_ms) +
                                sizeof(e.u.namespace_update_result.expires_ms))
                return event::unknown{e.kind};
            const auto &d = e.u.namespace_update_result;
            return extended_event::namespace_update_result{
                update_conclusion_from_c(d), announcement(d.ann)};
        }
        case MOQ_EVENT_NS_SUB_UPDATE_RESULT: {
            if (e.detail_size < sizeof(moq_ns_sub_update_result_event_t))
                return event::unknown{e.kind};
            const auto &d = e.u.ns_sub_update_result;
            return extended_event::ns_sub_update_result{
                update_conclusion_from_c(d), ns_sub_handle(d.handle)};
        }
        case MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT: {
            if (e.detail_size < sizeof(moq_subscribe_tracks_update_result_event_t))
                return event::unknown{e.kind};
            const auto &d = e.u.subscribe_tracks_update_result;
            return extended_event::subscribe_tracks_update_result{
                update_conclusion_from_c(d), track_sub_handle(d.handle)};
        }
        case MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED: {
            if (e.detail_size < MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE)
                return event::unknown{e.kind};
            const auto &d = e.u.outgoing_subgroup_terminated;
            return extended_event::outgoing_subgroup_terminated{
                subgroup_handle(d.subgroup), subscription(d.sub),
                publication(d.pub), d.group_id, d.subgroup_id,
                stream_ref(d.stream_ref), d.error_code,
                static_cast<outgoing_subgroup_terminal_cause>(d.cause)};
        }
        default:
            return event::unknown{e.kind};
        }
    }

    /* The draft-22 options of a SUBSCRIBE_TRACKS request event; empty for
     * any other event or an older draft. A copied value. */
    std::optional<extended_event::subscribe_tracks_options>
    subscribe_tracks_options() const noexcept
    {
        const auto &e = event_;
        if (e.kind != MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST ||
            !MOQ_CPP_DETAIL_COVERS_(e, moq_subscribe_tracks_request_event_t, options) ||
            e.u.subscribe_tracks_request.options == nullptr)
            return std::nullopt;
        moq_subscribe_tracks_options_t o{};
        std::memcpy(&o, e.u.subscribe_tracks_request.options,
                    MOQ_SUBSCRIBE_TRACKS_OPTIONS_V0_SIZE);
        if (o.struct_size < MOQ_SUBSCRIBE_TRACKS_OPTIONS_V0_SIZE)
            return std::nullopt;
        extended_event::subscribe_tracks_options out{};
        out.flags = o.flags;
        out.group_order = group_order_from_c(o.group_order);
        out.include_properties = o.include_properties;
        return out;
    }

    event_variant variant() const noexcept
    {
        return std::visit([this](const auto &value) -> event_variant {
            using T = std::remove_cvref_t<decltype(value)>;
            if constexpr (requires { value.cause; }) {
                if constexpr (std::is_same_v<decltype(value.cause), request_terminal_cause>) {
                    if constexpr (std::is_same_v<T, extended_event::fetch_reset>) {
                        if (value.cause != request_terminal_cause::none &&
                            value.cause != request_terminal_cause::peer_reset_stream &&
                            value.cause != request_terminal_cause::peer_stop_sending)
                            return event::unknown{kind()};
                    } else if (value.cause != request_terminal_cause::none) {
                        return event::unknown{kind()};
                    }
                }
            }
            if constexpr (std::is_constructible_v<event_variant, decltype(value)>)
                return value;
            else
                return event::unknown{kind()};
        }, variant_extended());
    }

    template<typename... Visitors>
    decltype(auto) visit_extended(Visitors &&...visitors) const
    {
        return std::visit(moq::overloaded{std::forward<Visitors>(visitors)...},
                          variant_extended());
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

#undef MOQ_CPP_DETAIL_COVERS_

#endif // MOQ_EVENT_HPP
