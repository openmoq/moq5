#ifndef MOQ_OPERATIONS_HPP
#define MOQ_OPERATIONS_HPP

#include <moq/buffer.hpp>
#include <moq/types.hpp>

#include <cstdint>
#include <span>
#include <string_view>

namespace moq {

struct subscribe_config {
    namespace_name   ns;
    bytes_view       track;
    subscribe_filter filter   = subscribe_filter::largest_object;
    uint8_t          priority = 128;
    group_order      group_order = group_order::default_order;
    bool             forward    = true;
    uint64_t         start_group  = 0;
    uint64_t         start_object = 0;
    uint64_t         end_group    = 0;
    bool             has_new_group_request = false;
    uint64_t         new_group_request     = 0;
};

struct accept_subscribe_config {
    bool             has_track_alias  = false;
    uint64_t         track_alias      = 0;
    bool             has_largest      = false;
    uint64_t         largest_group    = 0;
    uint64_t         largest_object   = 0;
    bool             has_expires      = false;
    uint64_t         expires_ms       = 0;
    bytes_view       track_properties = {};
};

/* REDIRECT (§10.6.1) target on a reject; set error_code to request_error::redirect
 * to send it. All fields may stay empty (an all-empty Redirect reuses the session
 * and the original namespace/name). A client must leave connect_uri empty;
 * namespace-scoped families must leave track empty. */
struct redirect_target {
    std::string_view connect_uri = {};
    namespace_name   ns          = {};
    bytes_view       track       = {};
};

struct reject_subscribe_config {
    request_error    error_code      = request_error::internal_error;
    std::string_view reason          = {};
    bool             can_retry       = false;
    uint64_t         retry_after_ms  = 0;
    redirect_target  redirect        = {};
};

struct subgroup_config {
    uint64_t group_id           = 0;
    uint64_t subgroup_id        = 0;
    uint8_t  publisher_priority = 128;
    bool     object_properties  = false;
    bool     end_of_group       = false;
};

struct datagram_config {
    uint64_t  group_id           = 0;
    uint64_t  object_id          = 0;
    uint8_t   publisher_priority = 128;
    bool      end_of_group       = false;
    bytes_view properties        = {};
};

struct status_datagram_config {
    uint64_t      group_id           = 0;
    uint64_t      object_id          = 0;
    uint8_t       publisher_priority = 128;
    object_status status             = object_status::end_of_group;
};

struct subscription_update_config {
    bool     has_subscriber_priority = false;
    uint8_t  subscriber_priority     = 128;
    bool     has_forward             = false;
    bool     forward                 = true;
    bool     has_delivery_timeout    = false;
    uint64_t delivery_timeout_us     = 0;
    std::span<const moq_auth_token_t> auth_tokens = {};
    bool     has_new_group_request   = false;   /* requires dynamic_groups */
    uint64_t new_group_request       = 0;
};

struct done_subscribe_config {
    uint64_t         status_code  = 0;
    uint64_t         stream_count = 0;
    std::string_view reason       = {};
};

struct fetch_config {
    namespace_name ns;
    bytes_view     track;
    uint64_t       start_group     = 0;
    uint64_t       start_object    = 0;
    uint64_t       end_group       = 0;
    uint64_t       end_object      = 0;
    group_order    group_order     = group_order::default_order;
    bool           has_subscriber_priority = false;
    uint8_t        subscriber_priority     = 128;
    bool           is_joining      = false;
    bool           joining_relative = false;
    subscription   joining_sub     = {};
    uint64_t       joining_start   = 0;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct accept_fetch_config {
    bool     end_of_track = false;
    uint64_t end_group    = 0;
    uint64_t end_object   = 0;
    bool     empty        = false;
};

struct reject_fetch_config {
    request_error    error_code     = request_error::internal_error;
    std::string_view reason         = {};
    bool             can_retry      = false;
    uint64_t         retry_after_ms = 0;
    redirect_target  redirect       = {};
};

/* Per-request GOAWAY (§10.4): migrate one active request to new_session_uri
 * (empty => current session; a client must leave it empty). */
struct request_goaway_config {
    std::string_view new_session_uri = {};
    uint64_t         timeout_ms      = 0;
};

struct fetch_object_config {
    uint64_t      group_id           = 0;
    uint64_t      subgroup_id        = 0;   // ignored when datagram is true
    uint64_t      object_id          = 0;
    uint8_t       publisher_priority = 128;
    const buffer *properties         = nullptr;
    // Appended after properties to keep positional aggregate init stable.
    bool          datagram           = false;  // forwarding preference Datagram (draft-18)
};

struct publish_config {
    namespace_name ns;
    bytes_view     track;
    bool           has_track_alias = false;
    uint64_t       track_alias     = 0;
    bool           has_forward     = false;
    bool           forward         = true;
    bytes_view     track_properties = {};
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct accept_publish_config {
    bool        has_subscriber_priority = false;
    uint8_t     subscriber_priority     = 128;
    group_order group_order             = group_order::default_order;
    bool        has_new_group_request   = false;   /* requires dynamic_groups */
    uint64_t    new_group_request       = 0;
};

struct reject_publish_config {
    request_error    error_code     = request_error::internal_error;
    std::string_view reason         = {};
    bool             can_retry      = false;
    uint64_t         retry_after_ms = 0;
};

struct publish_namespace_config {
    namespace_name ns;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct reject_namespace_config {
    request_error    error_code     = request_error::internal_error;
    std::string_view reason         = {};
    bool             can_retry      = false;
    uint64_t         retry_after_ms = 0;
    redirect_target  redirect       = {};  /* namespace-scoped: leave track empty */
};

struct cancel_namespace_config {
    request_error    error_code = request_error::internal_error;
    std::string_view reason     = {};
};

struct track_status_config {
    namespace_name ns;
    bytes_view     track;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct reject_track_status_config {
    request_error    error_code     = request_error::internal_error;
    std::string_view reason         = {};
    bool             can_retry      = false;
    uint64_t         retry_after_ms = 0;
    redirect_target  redirect       = {};
};

struct subscribe_namespace_config {
    namespace_name      ns_prefix;
    namespace_interest  interest = namespace_interest::both;
    std::span<const moq_auth_token_t> auth_tokens = {};
};

struct reject_ns_sub_config {
    request_error    error_code     = request_error::internal_error;
    std::string_view reason         = {};
    bool             can_retry      = false;
    uint64_t         retry_after_ms = 0;
    redirect_target  redirect       = {};  /* namespace-scoped: leave track empty */
};

struct publication_update_config {
    bool     has_subscriber_priority = false;
    uint8_t  subscriber_priority     = 128;
    bool     has_forward             = false;
    bool     forward                 = true;
    bool     has_delivery_timeout    = false;
    uint64_t delivery_timeout_us     = 0;
    std::span<const moq_auth_token_t> auth_tokens = {};
    bool     has_new_group_request   = false;   /* requires dynamic_groups */
    uint64_t new_group_request       = 0;
};

struct finish_publish_config {
    uint64_t         status_code  = 0;
    uint64_t         stream_count = 0;
    std::string_view reason       = {};
};

struct stream_object_config {
    uint64_t object_id      = 0;
    uint64_t payload_length = 0;
};

} // namespace moq

#endif // MOQ_OPERATIONS_HPP
