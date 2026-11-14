#ifndef TEST_CFG_INIT_FROZEN_LAYOUT_H
#define TEST_CFG_INIT_FROZEN_LAYOUT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* The configs whose exported initializer stamps a frozen size, declared as a
 * binary built when that size was fixed declares them. Plain C types only,
 * and no library header: a scalar typedef or an embedded record that changes
 * in the library cannot change these. Pointers the caller never dereferences
 * are untyped, and callbacks keep only their pointer width. */

typedef struct test_frozen_bytes {
    const uint8_t *data;
    size_t len;
} test_frozen_bytes_t;

typedef struct test_frozen_namespace {
    const test_frozen_bytes_t *parts;
    size_t count;
} test_frozen_namespace_t;

typedef struct test_frozen_redirect {
    test_frozen_bytes_t connect_uri;
    test_frozen_namespace_t track_namespace;
    test_frozen_bytes_t track_name;
} test_frozen_redirect_t;

typedef struct test_frozen_accept_subscribe_cfg {
    uint32_t struct_size;
    bool has_track_alias;
    uint64_t track_alias;
    bool has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool has_expires;
    uint64_t expires_ms;
    test_frozen_bytes_t track_properties;
} test_frozen_accept_subscribe_cfg_t;

typedef struct test_frozen_reject_subscribe_cfg {
    uint32_t struct_size;
    uint64_t error_code;
    test_frozen_bytes_t reason;
    bool can_retry;
    uint64_t retry_after_ms;
    test_frozen_redirect_t redirect;
} test_frozen_reject_subscribe_cfg_t;

typedef struct test_frozen_done_subscribe_cfg {
    uint32_t struct_size;
    uint64_t status_code;
    uint64_t stream_count;
    test_frozen_bytes_t reason;
} test_frozen_done_subscribe_cfg_t;

typedef struct test_frozen_accept_fetch_cfg {
    uint32_t struct_size;
    bool end_of_track;
    uint64_t end_group;
    uint64_t end_object;
    bool empty;
    test_frozen_bytes_t track_properties;
} test_frozen_accept_fetch_cfg_t;

typedef struct test_frozen_reject_fetch_cfg {
    uint32_t struct_size;
    uint64_t error_code;
    test_frozen_bytes_t reason;
    bool can_retry;
    uint64_t retry_after_ms;
    test_frozen_redirect_t redirect;
} test_frozen_reject_fetch_cfg_t;

typedef struct test_frozen_fetch_object_cfg {
    uint32_t struct_size;
    uint64_t group_id;
    uint64_t subgroup_id;
    uint64_t object_id;
    uint8_t publisher_priority;
    bool datagram;
    uint8_t _pad[6];
    void *payload;
    void *properties;
} test_frozen_fetch_object_cfg_t;

typedef struct test_frozen_fetch_update_cfg {
    uint32_t struct_size;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    uint8_t _reserved[2];
    const void *auth_tokens;
    size_t auth_token_count;
} test_frozen_fetch_update_cfg_t;

typedef struct test_frozen_request_goaway_cfg {
    uint32_t struct_size;
    test_frozen_bytes_t new_session_uri;
    uint64_t timeout_ms;
} test_frozen_request_goaway_cfg_t;

typedef struct test_frozen_reject_publish_cfg {
    uint32_t struct_size;
    uint64_t error_code;
    test_frozen_bytes_t reason;
    bool can_retry;
    uint64_t retry_after_ms;
} test_frozen_reject_publish_cfg_t;

typedef struct test_frozen_accept_namespace_cfg {
    uint32_t struct_size;
} test_frozen_accept_namespace_cfg_t;

typedef struct test_frozen_reject_namespace_cfg {
    uint32_t struct_size;
    uint64_t error_code;
    test_frozen_bytes_t reason;
    bool can_retry;
    uint64_t retry_after_ms;
    test_frozen_redirect_t redirect;
} test_frozen_reject_namespace_cfg_t;

typedef struct test_frozen_cancel_namespace_cfg {
    uint32_t struct_size;
    uint64_t error_code;
    test_frozen_bytes_t reason;
} test_frozen_cancel_namespace_cfg_t;

typedef struct test_frozen_reject_track_status_cfg {
    uint32_t struct_size;
    uint64_t error_code;
    test_frozen_bytes_t reason;
    bool can_retry;
    uint64_t retry_after_ms;
    test_frozen_redirect_t redirect;
} test_frozen_reject_track_status_cfg_t;

typedef struct test_frozen_subscribe_namespace_cfg {
    uint32_t struct_size;
    test_frozen_namespace_t track_namespace_prefix;
    uint32_t namespace_interest;
    const void *auth_tokens;
    size_t auth_token_count;
} test_frozen_subscribe_namespace_cfg_t;

typedef struct test_frozen_accept_ns_sub_cfg {
    uint32_t struct_size;
} test_frozen_accept_ns_sub_cfg_t;

typedef struct test_frozen_reject_ns_sub_cfg {
    uint32_t struct_size;
    uint64_t error_code;
    test_frozen_bytes_t reason;
    bool can_retry;
    uint64_t retry_after_ms;
    test_frozen_redirect_t redirect;
} test_frozen_reject_ns_sub_cfg_t;

typedef struct test_frozen_accept_subscribe_tracks_cfg {
    uint32_t struct_size;
} test_frozen_accept_subscribe_tracks_cfg_t;

typedef struct test_frozen_reject_subscribe_tracks_cfg {
    uint32_t struct_size;
    uint64_t error_code;
    test_frozen_bytes_t reason;
} test_frozen_reject_subscribe_tracks_cfg_t;

typedef struct test_frozen_finish_publish_cfg {
    uint32_t struct_size;
    uint64_t status_code;
    uint64_t stream_count;
    test_frozen_bytes_t reason;
} test_frozen_finish_publish_cfg_t;

typedef struct test_frozen_begin_object_cfg {
    uint32_t struct_size;
    uint64_t object_id;
    uint64_t payload_length;
    void *properties;
} test_frozen_begin_object_cfg_t;

typedef struct test_frozen_pub_publish_cfg {
    uint32_t struct_size;
    bool has_track_alias;
    uint64_t track_alias;
    bool has_forward;
    bool forward;
    test_frozen_bytes_t track_properties;
    const void *auth_tokens;
    size_t auth_token_count;
} test_frozen_pub_publish_cfg_t;

typedef struct test_frozen_pub_retained_group_cfg {
    uint32_t struct_size;
    uint64_t group_id;
    const void *objects;
    size_t object_count;
} test_frozen_pub_retained_group_cfg_t;

typedef struct test_frozen_sub_callbacks {
    uint32_t struct_size;
    void *ctx;
    void (*on_subscribed)(void);
    void (*on_subscribe_error)(void);
    void (*on_draining)(void);
    void (*on_closed)(void);
} test_frozen_sub_callbacks_t;

typedef struct test_frozen_sub_status_cfg {
    uint32_t struct_size;
    test_frozen_namespace_t track_namespace;
    test_frozen_bytes_t track_name;
    const void *auth_tokens;
    size_t auth_token_count;
} test_frozen_sub_status_cfg_t;

typedef struct test_frozen_sub_fetch_cfg {
    uint32_t struct_size;
    test_frozen_namespace_t track_namespace;
    test_frozen_bytes_t track_name;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;
    uint32_t group_order;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    const void *auth_tokens;
    size_t auth_token_count;
} test_frozen_sub_fetch_cfg_t;

#endif
