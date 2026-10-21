/*
 * Layout pins for the public structs the draft-22 surface extended.
 *
 * Each extended struct is pinned at the boundary of the layout it extends:
 * the last field that layout had, by name, and its sizeof -- the size a
 * caller built against it passes as struct_size. Every appended field is
 * pinned to start at or past that sizeof. A field that started inside it
 * would sit in that caller's tail padding, which the caller is free to leave
 * unset, and struct_size would claim to cover it. PUBLISH_REQUEST keeps its
 * original layout and reserved bytes; new initial parameters use a separate
 * observing query.
 *
 * The pins hold on every data model: compiled here for the host, and
 * freestanding by scripts/check_ilp32_headers.sh for two ILP32 targets, one
 * that aligns uint64_t to 4 inside a struct and one that aligns it to 8.
 * Headers of the other tiers are pinned when their include directories are
 * reachable; the adapters whose headers need a transport SDK pin their
 * configs in their own tests.
 */
#include <moq/moq.h>
#include <moq/subscriber.h>
#include <moq/publisher.h>
#include <moq/transport_bridge.h>
#include <moq/control_d18.h>
#include "publisher_old_layout.h"

#if defined(__has_include)
#  if __has_include(<moq/loc.h>)
#    include <moq/loc.h>
#    define ABI_HAVE_LOC 1
#  endif
#  if __has_include(<moq/endpoint.h>)
#    include <moq/endpoint.h>
#    define ABI_HAVE_ENDPOINT 1
#  endif
#  if __has_include(<moq/media_receiver.h>) && __has_include(<moq/media_object.h>) && \
      __has_include(<moq/msf.h>) && __has_include(<moq/cmaf.h>) && __has_include(<moq/loc.h>)
#    include <moq/media_receiver.h>
#    define ABI_HAVE_MEDIA_RECEIVER 1
#  endif
#  if __has_include(<moq/media_sender.h>) && __has_include(<moq/endpoint.h>) && \
      __has_include(<moq/media_object.h>) && __has_include(<moq/msf.h>)
#    include <moq/media_sender.h>
#    define ABI_HAVE_MEDIA_SENDER 1
#  endif
#  if __has_include(<moq/sim.h>)
#    include <moq/sim.h>
#    define ABI_HAVE_SIM 1
#  endif
#  if __has_include(<moq/relay/moqr_bind.h>)
#    include <moq/relay/moqr_bind.h>
#    include <moq/relay/moqr_shards.h>
#    define ABI_HAVE_RELAY 1
#  endif
#  if __has_include(<moq/mvfst.h>)
#    include <moq/mvfst.h>
#    define ABI_HAVE_MVFST 1
#  endif
#  if __has_include(<moq/proxygen_wt_managed.h>)
#    include <moq/proxygen_wt_managed.h>
#    define ABI_HAVE_PROXYGEN 1
#  endif
#endif

/* -- Data model ------------------------------------------------------- */

/* The alignment uint64_t receives inside a struct: 8 on LP64 and on ARM
 * ILP32, 4 on i386 ILP32. With the pointer width it selects the expected
 * value of each pin. */
typedef struct abi_u64_probe { char c; uint64_t v; } abi_u64_probe_t;
#define ABI_U64_ALIGN offsetof(abi_u64_probe_t, v)
#define ABI_PICK(lp64, ilp32_a4, ilp32_a8) \
    (sizeof(void *) == 8 ? (size_t)(lp64) \
     : ABI_U64_ALIGN == 4 ? (size_t)(ilp32_a4) : (size_t)(ilp32_a8))

_Static_assert(sizeof(void *) == 8 || sizeof(void *) == 4, "pointer width");
_Static_assert(sizeof(void *) == 4 || ABI_U64_ALIGN == 8,
               "64-bit pointers come with 8-aligned uint64_t");

/* -- Pin vocabulary ----------------------------------------------------- */

#define ABI_FIELD_END(T, f) (offsetof(T, f) + sizeof(((T *)0)->f))
/* The sizeof of the layout that ends at `last`: its end rounded up to the
 * struct's alignment, which the PIN_THROUGH alignment pin holds fixed. */
#define ABI_SIZEOF_THROUGH(T, last) \
    ((ABI_FIELD_END(T, last) + _Alignof(T) - 1) / _Alignof(T) * _Alignof(T))

/* The extended layout: it ends at `last`, has this sizeof and alignment. */
#define PIN_THROUGH(T, last, sz64, sz32a4, sz32a8, al64, al32a4, al32a8)      \
    _Static_assert(ABI_SIZEOF_THROUGH(T, last) == ABI_PICK(sz64, sz32a4, sz32a8), \
                   #T ": sizeof through " #last);                            \
    _Static_assert(_Alignof(T) == ABI_PICK(al64, al32a4, al32a8),            \
                   #T ": alignment")

/* An appended field starts at or past the sizeof through `last`. */
#define PIN_AFTER(T, last, f)                                                \
    _Static_assert(offsetof(T, f) >= ABI_SIZEOF_THROUGH(T, last),            \
                   #T "." #f ": starts past the sizeof through " #last)

/* An appended field's exact offset. */
#define PIN_AT(T, f, o64, o32a4, o32a8)                                      \
    _Static_assert(offsetof(T, f) == ABI_PICK(o64, o32a4, o32a8),            \
                   #T "." #f ": offset")

/* The current sizeof. */
#define PIN_SIZE(T, s64, s32a4, s32a8)                                       \
    _Static_assert(sizeof(T) == ABI_PICK(s64, s32a4, s32a8), #T ": sizeof")

/* An event detail fits the event's fixed detail reserve. */
#define PIN_DETAIL(T)                                                        \
    _Static_assert(sizeof(T) <= MOQ_EVENT_DETAIL_MAX, #T ": fits the event")

/* -- Session configuration --------------------------------------------- */

/* The baseline perspective enum saves four bytes on i386 with short enums;
 * the 8-byte-aligned models absorb that change before initial capacity. */
#define ABI_SESSION_ENUM_SHIFT (sizeof(moq_perspective_t) < 4 ? 4 : 0)
PIN_THROUGH(moq_session_cfg_t, max_namespace_suffixes_per_subscription,
            160, 144 - ABI_SESSION_ENUM_SHIFT, 152, 8, 4, 8);
PIN_AFTER(moq_session_cfg_t, max_namespace_suffixes_per_subscription,
          setup_auth_tokens);
PIN_THROUGH(moq_session_cfg_t, setup_auth_token_count,
            176, 156 - ABI_SESSION_ENUM_SHIFT, 160, 8, 4, 8);
PIN_AFTER(moq_session_cfg_t, setup_auth_token_count, max_fills);
PIN_AFTER(moq_session_cfg_t, setup_auth_token_count, defer_subscription_updates);
PIN_AFTER(moq_session_cfg_t, setup_auth_token_count, defer_fetch_ranges);

PIN_THROUGH(moq_subscribe_cfg_t, new_group_request, 120, 80, 80, 8, 4, 8);
PIN_AFTER(moq_subscribe_cfg_t, new_group_request, location);
PIN_AFTER(moq_subscribe_cfg_t, new_group_request, fill);

PIN_THROUGH(moq_subscription_update_cfg_t, end_group, 88, 72, 80, 8, 4, 8);
PIN_AFTER(moq_subscription_update_cfg_t, end_group, location);
PIN_AFTER(moq_subscription_update_cfg_t, end_group, fill);
PIN_AFTER(moq_subscription_update_cfg_t, end_group, delivery_timeout_flags);
PIN_AFTER(moq_subscription_update_cfg_t, end_group, object_delivery_timeout_ms);
PIN_AFTER(moq_subscription_update_cfg_t, end_group, subgroup_delivery_timeout_ms);

PIN_THROUGH(moq_publication_update_cfg_t, new_group_request, 56, 44, 48, 8, 4, 8);
PIN_AFTER(moq_publication_update_cfg_t, new_group_request, location);
PIN_AFTER(moq_publication_update_cfg_t, new_group_request, fill);
PIN_AFTER(moq_publication_update_cfg_t, new_group_request, delivery_timeout_flags);
PIN_AFTER(moq_publication_update_cfg_t, new_group_request, subgroup_delivery_timeout_ms);

PIN_THROUGH(moq_fetch_cfg_t, joining_pub, 120, 92, 96, 8, 4, 8);
PIN_AFTER(moq_fetch_cfg_t, joining_pub, location);
PIN_AFTER(moq_fetch_cfg_t, joining_pub, has_fill_timeout);
PIN_AFTER(moq_fetch_cfg_t, joining_pub, fill_timeout_ms);

/* -- Facades ------------------------------------------------------------ */

PIN_THROUGH(moq_sub_cfg_t, on_update_ok, 112, 72, 80, 8, 4, 8);
PIN_AFTER(moq_sub_cfg_t, on_update_ok, max_fills);
PIN_AFTER(moq_sub_cfg_t, on_update_ok, max_fill_items);

PIN_THROUGH(moq_sub_track_cfg_t, auth_token_count, 64, 36, 36, 8, 4, 4);
PIN_AFTER(moq_sub_track_cfg_t, auth_token_count, location);
PIN_AFTER(moq_sub_track_cfg_t, auth_token_count, fill);
PIN_THROUGH(moq_sub_track_cfg_t, fill, 80, 44, 44, 8, 4, 4);
PIN_AFTER(moq_sub_track_cfg_t, fill, rendezvous_timeout_ms);

/* The subscriber's delivery timeouts on an accepted PUBLISH start past the
 * layout that ends at the expiry pair. */
PIN_AFTER(moq_accept_publish_cfg_t, has_expires, delivery_timeout_flags);
PIN_AFTER(moq_accept_publish_cfg_t, has_expires, subgroup_delivery_timeout_ms);

/* SUBSCRIBE_TRACKS options and the rendezvous wait extend these layouts. */
PIN_THROUGH(moq_subscribe_cfg_t, subgroup_delivery_timeout_ms, 160, 112, 112, 8, 4, 8);
PIN_AFTER(moq_subscribe_cfg_t, subgroup_delivery_timeout_ms, has_rendezvous_timeout);
PIN_AFTER(moq_subscribe_cfg_t, subgroup_delivery_timeout_ms, rendezvous_timeout_ms);
PIN_THROUGH(moq_subscription_admission_options_t, subgroup_delivery_timeout_ms,
            72, 72, 72, 8, 4, 8);
PIN_AFTER(moq_subscription_admission_options_t, subgroup_delivery_timeout_ms,
          rendezvous_timeout_ms);
PIN_THROUGH(moq_subscribe_tracks_cfg_t, auth_token_count, 48, 24, 24, 8, 4, 4);
PIN_AFTER(moq_subscribe_tracks_cfg_t, auth_token_count, options_flags);
PIN_AFTER(moq_subscribe_tracks_cfg_t, auth_token_count, group_order);
PIN_AFTER(moq_subscribe_tracks_cfg_t, auth_token_count, include_properties);
PIN_THROUGH(moq_subscribe_tracks_request_event_t, token_count, 48, 32, 32, 8, 4, 8);
PIN_AFTER(moq_subscribe_tracks_request_event_t, token_count, options);

PIN_THROUGH(moq_sub_update_cfg_t, end_group, 56, 52, 56, 8, 4, 8);
PIN_AFTER(moq_sub_update_cfg_t, end_group, fill);

/* The baseline accept_mode enum shrinks under -fshort-enums, moving its first
 * callback pointer by one pointer slot on all three supported data models. */
#define ABI_PUB_ENUM_SHIFT (sizeof(moq_pub_accept_mode_t) < 4 ? sizeof(void *) : 0)
PIN_THROUGH(test_pub_cfg_callbacks_t, callbacks, 128 - ABI_PUB_ENUM_SHIFT,
            68 - ABI_PUB_ENUM_SHIFT, 68 - ABI_PUB_ENUM_SHIFT, 8, 4, 4);
_Static_assert(_Alignof(moq_pub_cfg_t) == ABI_PICK(8, 4, 8), "current publisher alignment");
_Static_assert(offsetof(moq_pub_cfg_t, max_fills) >= sizeof(test_pub_cfg_callbacks_t),
               "max_fills follows the complete callbacks-prefix allocation");
_Static_assert(offsetof(moq_pub_cfg_t, on_subscribe_v1) >= sizeof(test_pub_cfg_pre_v1_t),
               "V1 follows the complete pre-V1 allocation, including padding");
_Static_assert(offsetof(moq_pub_cfg_t, max_fill_snapshot_bytes) >= sizeof(test_pub_cfg_v1_t),
               "fill budget follows the complete V1 allocation");
#define PIN_PUB_OLD_FIELD(f) \
    _Static_assert(offsetof(moq_pub_cfg_t, f) == offsetof(test_pub_cfg_v1_t, f), \
                   "publisher old offset " #f); \
    _Static_assert(sizeof(((moq_pub_cfg_t *)0)->f) == sizeof(((test_pub_cfg_v1_t *)0)->f), \
                   "publisher old width " #f)
PIN_PUB_OLD_FIELD(struct_size);
PIN_PUB_OLD_FIELD(accept_mode);
PIN_PUB_OLD_FIELD(default_publisher_priority);
PIN_PUB_OLD_FIELD(on_subscribe);
PIN_PUB_OLD_FIELD(on_subscribe_ctx);
PIN_PUB_OLD_FIELD(callbacks);
PIN_PUB_OLD_FIELD(max_fills);
PIN_PUB_OLD_FIELD(on_subscribe_v1);
PIN_PUB_OLD_FIELD(on_subscribe_v1_ctx);
#undef PIN_PUB_OLD_FIELD
#define PIN_PUB_OLD_CALLBACK(f) \
    _Static_assert(offsetof(moq_pub_callbacks_t, f) == offsetof(test_pub_old_callbacks_t, f), \
                   "publisher old callback offset " #f); \
    _Static_assert(sizeof(((moq_pub_callbacks_t *)0)->f) == \
                   sizeof(((test_pub_old_callbacks_t *)0)->f), "publisher callback width " #f)
PIN_PUB_OLD_CALLBACK(struct_size);
PIN_PUB_OLD_CALLBACK(ctx);
PIN_PUB_OLD_CALLBACK(on_subscriber_joined);
PIN_PUB_OLD_CALLBACK(on_subscriber_left);
PIN_PUB_OLD_CALLBACK(on_draining);
PIN_PUB_OLD_CALLBACK(on_closed);
PIN_PUB_OLD_CALLBACK(on_subscriber_updated);
PIN_PUB_OLD_CALLBACK(on_publish_ok);
PIN_PUB_OLD_CALLBACK(on_publish_error);
PIN_PUB_OLD_CALLBACK(on_publish_forward_changed);
PIN_PUB_OLD_CALLBACK(on_publish_finished);
PIN_PUB_OLD_CALLBACK(on_namespace_terminal);
#undef PIN_PUB_OLD_CALLBACK

/* A snapshot the library writes min(out_size, current) bytes of. */
PIN_THROUGH(moq_sub_stats_t, closed, 192, 184, 192, 8, 4, 8);
PIN_AFTER(moq_sub_stats_t, closed, fill_objects);
PIN_AFTER(moq_sub_stats_t, closed, fill_items_queued);

/* -- Events -------------------------------------------------------------- */

/* The event itself never grows: a fixed prefix and a fixed detail reserve,
 * so a caller's element size is the same on every revision. */
_Static_assert(offsetof(moq_event_t, u) == 16, "event prefix");
_Static_assert(sizeof(((moq_event_t *)0)->u) == MOQ_EVENT_DETAIL_MAX,
               "event detail reserve");
_Static_assert(MOQ_EVENT_DETAIL_MAX == 128, "event detail reserve size");
_Static_assert(sizeof(moq_event_t) == 144, "event size");

PIN_THROUGH(moq_namespace_done_event_t, ann, 8, 8, 8, 8, 4, 8);

PIN_THROUGH(moq_namespace_cancelled_event_t, reason, 32, 24, 24, 8, 4, 8);

PIN_THROUGH(moq_unsubscribed_event_t, sub, 8, 8, 8, 8, 4, 8);
PIN_AFTER(moq_unsubscribed_event_t, sub, cause);
PIN_AFTER(moq_unsubscribed_event_t, sub, stream_error_code);

PIN_THROUGH(moq_subscribe_updated_event_t, end_group, 96, 80, 88, 8, 4, 8);
PIN_AFTER(moq_subscribe_updated_event_t, end_group, options);
PIN_THROUGH(moq_subscribe_updated_event_t, options, 104, 84, 96, 8, 4, 8);
PIN_AFTER(moq_subscribe_updated_event_t, options, answer_owed);
PIN_AFTER(moq_subscribe_updated_event_t, end_group, fill_requested);
PIN_AFTER(moq_subscribe_updated_event_t, end_group, update_count);
PIN_AFTER(moq_subscribe_updated_event_t, end_group, update_request_id);

PIN_THROUGH(moq_publish_updated_event_t, end_group, 96, 80, 88, 8, 4, 8);
PIN_AFTER(moq_publish_updated_event_t, end_group, options);
PIN_THROUGH(moq_publish_updated_event_t, options, 104, 84, 96, 8, 4, 8);
PIN_AFTER(moq_publish_updated_event_t, options, answer_owed);
PIN_AFTER(moq_publish_updated_event_t, end_group, answer_owed);
PIN_AFTER(moq_publish_updated_event_t, end_group, fill_requested);
PIN_AFTER(moq_publish_updated_event_t, end_group, update_count);
PIN_AFTER(moq_publish_updated_event_t, end_group, update_request_id);

PIN_THROUGH(moq_fetch_cancelled_event_t, fetch, 8, 8, 8, 8, 4, 8);
PIN_AFTER(moq_fetch_cancelled_event_t, fetch, cause);
PIN_AFTER(moq_fetch_cancelled_event_t, fetch, stream_error_code);

PIN_THROUGH(moq_fetch_reset_event_t, _pad, 24, 24, 24, 8, 4, 8);
PIN_AFTER(moq_fetch_reset_event_t, _pad, cause);

PIN_THROUGH(moq_publish_finished_event_t, reason, 40, 32, 32, 8, 4, 8);
PIN_AFTER(moq_publish_finished_event_t, reason, cause);
PIN_AFTER(moq_publish_finished_event_t, reason, stream_error_code);

PIN_THROUGH(moq_publish_unsubscribed_event_t, pub, 8, 8, 8, 8, 4, 8);
PIN_AFTER(moq_publish_unsubscribed_event_t, pub, cause);
PIN_AFTER(moq_publish_unsubscribed_event_t, pub, stream_error_code);

PIN_THROUGH(moq_subscribe_tracks_cancelled_event_t, handle, 8, 8, 8, 8, 4, 8);
PIN_AFTER(moq_subscribe_tracks_cancelled_event_t, handle, cause);
PIN_AFTER(moq_subscribe_tracks_cancelled_event_t, handle, stream_error_code);

/* PUBLISH_REQUEST's seven explicit padding bytes remain reserved. */
_Static_assert(offsetof(moq_publish_request_event_t, _pad) ==
               ABI_FIELD_END(moq_publish_request_event_t, forward),
               "publish request padding starts immediately after forward");
_Static_assert(sizeof(((moq_publish_request_event_t *)0)->_pad) == 7,
               "publish request keeps all seven reserved bytes");
_Static_assert(ABI_FIELD_END(moq_publish_request_event_t, _pad) ==
               ABI_FIELD_END(moq_publish_request_event_t, forward) + 7,
               "publish request padding window keeps its end");
_Static_assert(offsetof(moq_publish_request_event_t, tokens) ==
               ABI_PICK(56, 40, 40), "publish request tokens unmoved");
PIN_SIZE(moq_publish_request_event_t, 128, 88, 96);

/* REDIRECT names two more request kinds through the same 8-byte handle. */
PIN_SIZE(moq_request_redirect_event_t, 104, 64, 72);
_Static_assert(sizeof(((moq_request_redirect_event_t *)0)->handle) == 8,
               "redirect handle stays one handle wide");

PIN_DETAIL(moq_namespace_done_event_t);
PIN_DETAIL(moq_namespace_cancelled_event_t);
PIN_DETAIL(moq_unsubscribed_event_t);
PIN_DETAIL(moq_subscribe_updated_event_t);
PIN_DETAIL(moq_publish_updated_event_t);
PIN_DETAIL(moq_fetch_cancelled_event_t);
PIN_DETAIL(moq_fetch_reset_event_t);
PIN_DETAIL(moq_publish_finished_event_t);
PIN_DETAIL(moq_publish_unsubscribed_event_t);
PIN_DETAIL(moq_subscribe_tracks_cancelled_event_t);
PIN_DETAIL(moq_publish_request_event_t);
PIN_DETAIL(moq_request_redirect_event_t);
PIN_DETAIL(moq_track_status_terminated_event_t);
PIN_DETAIL(moq_namespace_terminated_event_t);
PIN_DETAIL(moq_ns_subscription_terminated_event_t);
PIN_DETAIL(moq_publish_state_event_t);
PIN_DETAIL(moq_fill_created_event_t);
PIN_DETAIL(moq_fill_request_event_t);
PIN_DETAIL(moq_fill_object_event_t);
PIN_DETAIL(moq_fill_gap_event_t);
PIN_DETAIL(moq_fill_terminal_event_t);
PIN_DETAIL(moq_fetch_request_event_t);
PIN_DETAIL(moq_subscribe_tracks_request_event_t);
PIN_DETAIL(moq_namespace_update_request_event_t);
PIN_DETAIL(moq_ns_sub_update_request_event_t);
PIN_DETAIL(moq_subscribe_tracks_update_request_event_t);
PIN_DETAIL(moq_namespace_update_result_event_t);
PIN_DETAIL(moq_ns_sub_update_result_event_t);
PIN_DETAIL(moq_subscribe_tracks_update_result_event_t);

PIN_THROUGH(moq_publish_state_event_t, largest_object, 40, 36, 40, 8, 4, 8);
PIN_AFTER(moq_publish_state_event_t, largest_object, location);
PIN_AT(moq_publish_state_event_t, location, 40, 36, 40);
PIN_SIZE(moq_publish_state_event_t, 48, 40, 48);

/* FETCH_REQUEST grew a borrowed pointer past joining_pub; the record it
 * points to is written by the library and carries its own struct_size. */
PIN_THROUGH(moq_fetch_request_event_t, joining_pub, 120, 92, 96, 8, 4, 8);
PIN_AFTER(moq_fetch_request_event_t, joining_pub, options);
PIN_AT(moq_fetch_request_options_t, flags, 4, 4, 4);
PIN_AT(moq_fetch_request_options_t, location, 8, 8, 8);
PIN_AT(moq_fetch_request_options_t, fill_timeout_ms, 56, 56, 56);
PIN_AT(moq_fetch_request_options_t, range_unresolved, 64, 64, 64);
PIN_SIZE(moq_fetch_request_options_t, 72, 72, 72);
_Static_assert(MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE == 64, "FETCH options v0 floor");

/* Each fill event's frozen prefix covers the whole record through its last
 * field and never exceeds the struct: a producer's detail_size (the full
 * sizeof) always satisfies a consumer that requires the prefix. */
#define PIN_FILL_PREFIX(T, last, V0)                                         \
    _Static_assert(ABI_FIELD_END(T, last) <= (V0) && (V0) <= sizeof(T),      \
                   #T ": frozen prefix through " #last " within the struct")
PIN_FILL_PREFIX(moq_fill_created_event_t, initiating_request_id,
                MOQ_FILL_CREATED_EVENT_V0_SIZE);
PIN_FILL_PREFIX(moq_fill_request_event_t, has_fill_timeout,
                MOQ_FILL_REQUEST_EVENT_V0_SIZE);
PIN_FILL_PREFIX(moq_fill_object_event_t, properties,
                MOQ_FILL_OBJECT_EVENT_V0_SIZE);
PIN_FILL_PREFIX(moq_fill_gap_event_t, kind, MOQ_FILL_GAP_EVENT_V0_SIZE);
PIN_FILL_PREFIX(moq_fill_terminal_event_t, error_code,
                MOQ_FILL_TERMINAL_EVENT_V0_SIZE);

/* -- LOC ------------------------------------------------------------------ */

#ifdef ABI_HAVE_LOC
PIN_THROUGH(moq_loc_headers_t, video_config, 64, 52, 56, 8, 4, 8);
_Static_assert(MOQ_LOC_HEADERS_V0_SIZE ==
               ABI_FIELD_END(moq_loc_headers_t, video_config),
               "LOC headers v0 size ends at video_config");
_Static_assert(MOQ_LOC_HEADERS_V0_SIZE == sizeof(moq_loc_headers_t),
               "legacy LOC is a complete frozen record, not an appended prefix");
PIN_THROUGH(moq_loc_metadata_t, audio_config, 64, 48, 48, 8, 4, 8);
_Static_assert(MOQ_LOC_METADATA_V0_SIZE == sizeof(moq_loc_metadata_t),
               "lossless LOC v0 covers both complete borrowed config spans");
_Static_assert(sizeof(((moq_loc_metadata_t *)0)->present) == 4 &&
               sizeof(((moq_loc_metadata_t *)0)->marking) == 4 &&
               sizeof(((moq_loc_metadata_t *)0)->reserved) == 2,
               "lossless LOC masks and raw payload fields have fixed widths");
#endif

/* -- Service endpoint ----------------------------------------------------- */

#ifdef ABI_HAVE_ENDPOINT
/* Short baseline transport enums save one ILP32 word; the final uint64_t
 * alignment adds another word on ARM. LP64 pointer padding absorbs both. */
#define ABI_ENDPOINT_ENUM_SHIFT (sizeof(moq_transport_protocol_t) < 4 ? 4 : 0)
PIN_THROUGH(moq_endpoint_cfg_t, handshake_timeout_us, 144,
            84 - ABI_ENDPOINT_ENUM_SHIFT, 88 - 2 * ABI_ENDPOINT_ENUM_SHIFT, 8, 4, 8);
PIN_AFTER(moq_endpoint_cfg_t, handshake_timeout_us, setup_auth_tokens);
PIN_THROUGH(moq_endpoint_cfg_t, verify_certificate_ctx, 176,
            100 - ABI_ENDPOINT_ENUM_SHIFT, 104 - 2 * ABI_ENDPOINT_ENUM_SHIFT, 8, 4, 8);
PIN_AFTER(moq_endpoint_cfg_t, verify_certificate_ctx, uri_mode);
PIN_AFTER(moq_endpoint_cfg_t, verify_certificate_ctx, preferred_version);
PIN_AFTER(moq_endpoint_cfg_t, verify_certificate_ctx, accept_uri);
PIN_AFTER(moq_endpoint_cfg_t, verify_certificate_ctx, accept_uri_ctx);
PIN_THROUGH(moq_endpoint_cfg_t, accept_uri_ctx, 200,
            116 - ABI_ENDPOINT_ENUM_SHIFT, 120 - 2 * ABI_ENDPOINT_ENUM_SHIFT, 8, 4, 8);
PIN_AFTER(moq_endpoint_cfg_t, accept_uri_ctx, send_queue_cap_bytes);
#endif

/* -- Simulator ------------------------------------------------------------- */

#ifdef ABI_HAVE_SIM
PIN_THROUGH(moq_simpair_cfg_t, server_max_open_subgroups, 168, 136, 152, 8, 4, 8);
PIN_AFTER(moq_simpair_cfg_t, server_max_open_subgroups,
          server_defer_subscription_updates);
PIN_AFTER(moq_simpair_cfg_t, server_max_open_subgroups,
          client_defer_subscription_updates);
PIN_AFTER(moq_simpair_cfg_t, server_max_open_subgroups,
          client_max_data_streams);
/* The receive-pool field starts past the sizeof of the layout that ended at
 * client_defer_subscription_updates (its tail padding holds the reserved
 * slot, never read). */
PIN_AFTER(moq_simpair_cfg_t, client_defer_subscription_updates,
          client_max_data_streams);
#endif

/* -- Relay ------------------------------------------------------------------ */

#ifdef ABI_HAVE_RELAY
/* Embedded by value in moqr_shards_cfg_t, so it never grows: its sizeof is
 * the one it ends at (pinned exactly below). */
PIN_THROUGH(moqr_bind_cfg_t, fetch_forward_bytes, 88, 64, 64, 8, 4, 8);

/* The shards config embeds the core and bind templates by value and appends
 * after them, so every field of the layout through pump_subgroup_slots is
 * pinned at its offset on each data model, not just the boundary: a template
 * that grew would move them all. */
PIN_THROUGH(moqr_shards_cfg_t, pump_subgroup_slots, 352, 272, 296, 8, 4, 8);
PIN_AFTER(moqr_shards_cfg_t, pump_subgroup_slots, fill_timeout_default_ms);
PIN_AFTER(moqr_shards_cfg_t, pump_subgroup_slots, fill_timeout_max_ms);

PIN_THROUGH(moqr_subscribe_req_t, forward, 96, 68, 80, 8, 4, 8);
PIN_AFTER(moqr_subscribe_req_t, forward, range_end_holds);

PIN_THROUGH(moqr_fetch_req_t, cookie, 88, 68, 72, 8, 4, 8);
PIN_AFTER(moqr_fetch_req_t, cookie, report_gaps);

/* No struct_size: the appended field is reached only through the _sized
 * entry points, which take the caller's sizeof. */
PIN_THROUGH(moqr_park_req_t, sub_group_order, 112, 80, 88, 8, 4, 8);
PIN_AFTER(moqr_park_req_t, sub_group_order, sub_forward_off);
PIN_AFTER(moqr_fetch_req_t, report_unknown, relative_start);
PIN_AFTER(moqr_fetch_req_t, report_unknown, range_form);
PIN_AFTER(moqr_fetch_req_t, report_unknown, report_pending);

/* The subgroup PROPERTIES form joins the append descriptor at or past its
 * previous sizeof (the extent through first_object); the record view, a
 * caller-sized output with no size field, does not change. */
PIN_THROUGH(moqr_log_append_desc_t, first_object, 88, 64, 80, 8, 4, 8);
PIN_AFTER(moqr_log_append_desc_t, first_object, subgroup_properties);
PIN_AT(moqr_log_append_desc_t, subgroup_properties, 88, 68, 80);
PIN_SIZE(moqr_log_append_desc_t, 96, 72, 88);
PIN_SIZE(moqr_record_view_t, 104, 84, 96);
#endif

/* -- Adapters without a transport SDK in their public header -------------- */

#ifdef ABI_HAVE_MVFST
PIN_THROUGH(moq_mvfst_managed_cfg_t, app_deadline_ctx, 192, 120, 128, 8, 4, 8);
PIN_AFTER(moq_mvfst_managed_cfg_t, app_deadline_ctx, target);
#endif

#ifdef ABI_HAVE_PROXYGEN
PIN_THROUGH(moq_proxygen_wt_managed_cfg_t, goaway_timeout_us, 136, 76, 80, 8, 4, 8);
PIN_AFTER(moq_proxygen_wt_managed_cfg_t, goaway_timeout_us, target);
#endif

/* -- Exact positions of the appended fields ------------------------------- */

/* The reserved slots fill the tail padding the extended layout left where
 * its last field ends short of the struct's alignment; they are never read,
 * so the first readable appended field lands at or past the old sizeof. */
/* The SETUP token block follows the slot covering the tail padding after
 * max_namespace_suffixes_per_subscription; it starts at or past that
 * layout's sizeof (160/144/152). */
PIN_AT(moq_session_cfg_t, _reserved_setup_auth, 156, 144 - ABI_SESSION_ENUM_SHIFT, 148);
PIN_AT(moq_session_cfg_t, setup_auth_tokens, 160, 148 - ABI_SESSION_ENUM_SHIFT, 152);
PIN_AT(moq_session_cfg_t, setup_auth_token_count, 168, 152 - ABI_SESSION_ENUM_SHIFT, 156);
/* The fill pool and the deferral switches follow the token block, at its
 * sizeof (176/156/160); the block ends on the struct's alignment, so no
 * reserved slot is needed. */
PIN_AT(moq_session_cfg_t, max_fills, 176, 156 - ABI_SESSION_ENUM_SHIFT, 160);
PIN_AT(moq_session_cfg_t, defer_subscription_updates, 180, 160 - ABI_SESSION_ENUM_SHIFT, 164);
PIN_AT(moq_session_cfg_t, defer_fetch_ranges, 181, 161 - ABI_SESSION_ENUM_SHIFT, 165);
PIN_SIZE(moq_session_cfg_t, 184, 164 - ABI_SESSION_ENUM_SHIFT, 168);
/* GOAWAY Timeout: at the previous sizeof (16/8/8). */
PIN_AT(moq_goaway_event_t, has_timeout, 16, 8, 8);
PIN_AT(moq_goaway_event_t, timeout_ms, 24, 16, 16);
PIN_SIZE(moq_goaway_event_t, 32, 24, 24);
PIN_AT(moq_subscribe_cfg_t, location, 120, 80, 80);
PIN_AT(moq_subscribe_cfg_t, fill, 128, 84, 84);
PIN_THROUGH(moq_subscribe_cfg_t, fill, 136, 88, 88, 8, 4, 8);
PIN_AT(moq_subscribe_cfg_t, delivery_timeout_flags, 136, 88, 88);
PIN_AT(moq_subscribe_cfg_t, reserved, 140, 92, 92);
PIN_AT(moq_subscribe_cfg_t, object_delivery_timeout_ms, 144, 96, 96);
PIN_AT(moq_subscribe_cfg_t, subgroup_delivery_timeout_ms, 152, 104, 104);
PIN_AT(moq_subscribe_cfg_t, has_rendezvous_timeout, 160, 112, 112);
PIN_AT(moq_subscribe_cfg_t, rendezvous_timeout_ms, 168, 116, 120);
PIN_THROUGH(moq_subscribe_cfg_t, rendezvous_timeout_ms, 176, 124, 128, 8, 4, 8);
PIN_AFTER(moq_subscribe_cfg_t, rendezvous_timeout_ms, has_include_properties);
PIN_AT(moq_subscribe_cfg_t, has_include_properties, 176, 124, 128);
PIN_AT(moq_subscribe_cfg_t, include_properties, 177, 125, 129);
PIN_SIZE(moq_subscribe_cfg_t, 184, 128, 136);
PIN_AT(moq_subscribe_request_event_t, options, 120, 92, 96);
_Static_assert(MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE == 72,
               "admission options v0 floor");
PIN_AT(moq_subscription_admission_options_t, rendezvous_timeout_ms, 72, 72, 72);
PIN_SIZE(moq_subscription_admission_options_t, 80, 80, 80);
PIN_AT(moq_subscription_admission_options_t, location, 8, 8, 8);
PIN_AT(moq_subscription_admission_options_t, object_delivery_timeout_ms, 56, 56, 56);
PIN_AT(moq_subscription_admission_options_t, subgroup_delivery_timeout_ms, 64, 64, 64);
PIN_AT(moq_subscription_update_cfg_t, location, 88, 72, 80);
PIN_AT(moq_subscription_update_cfg_t, fill, 96, 76, 84);
PIN_AT(moq_subscription_update_cfg_t, delivery_timeout_flags, 104, 80, 88);
PIN_AT(moq_subscription_update_cfg_t, reserved, 108, 84, 92);
PIN_AT(moq_subscription_update_cfg_t, object_delivery_timeout_ms, 112, 88, 96);
PIN_AT(moq_subscription_update_cfg_t, subgroup_delivery_timeout_ms, 120, 96, 104);
PIN_SIZE(moq_subscription_update_cfg_t, 128, 104, 112);
PIN_AT(moq_subscription_update_options_t, initiating_request_id, 8, 8, 8);
PIN_AT(moq_subscription_update_options_t, location, 16, 16, 16);
PIN_AT(moq_subscription_update_options_t, object_delivery_timeout_ms, 64, 64, 64);
PIN_AT(moq_subscription_update_options_t, subgroup_delivery_timeout_ms, 72, 72, 72);
PIN_AT(moq_subscription_update_options_t, resolution, 80, 80, 80);
PIN_SIZE(moq_subscription_update_options_t, 160, 160, 160);
PIN_AT(moq_update_resolution_t, update_request_id, 8, 8, 8);
PIN_AT(moq_update_resolution_t, largest_group, 16, 16, 16);
PIN_AT(moq_update_resolution_t, largest_object, 24, 24, 24);
PIN_AT(moq_update_resolution_t, location, 32, 32, 32);
PIN_SIZE(moq_update_resolution_t, 80, 80, 80);
PIN_AT(moq_publication_update_cfg_t, location, 56, 44, 48);
PIN_AT(moq_publication_update_cfg_t, fill, 64, 48, 52);
PIN_AT(moq_publication_update_cfg_t, delivery_timeout_flags, 72, 52, 56);
PIN_AT(moq_publication_update_cfg_t, reserved, 76, 56, 60);
PIN_AT(moq_publication_update_cfg_t, object_delivery_timeout_ms, 80, 60, 64);
PIN_AT(moq_publication_update_cfg_t, subgroup_delivery_timeout_ms, 88, 68, 72);
PIN_SIZE(moq_publication_update_cfg_t, 96, 76, 80);
PIN_AT(moq_sub_cfg_t, _reserved_group_ends, 112, 72, 76);
PIN_AT(moq_sub_cfg_t, surface_group_ends, 116, 76, 80);
/* The fill limits start at the complete sizeof through surface_group_ends
 * (120/80/88), not in its tail padding. */
PIN_THROUGH(moq_sub_cfg_t, surface_group_ends, 120, 80, 88, 8, 4, 8);
PIN_AFTER(moq_sub_cfg_t, surface_group_ends, max_fills);
PIN_AT(moq_sub_cfg_t, max_fills, 120, 80, 88);
PIN_AT(moq_sub_cfg_t, max_fill_items, 124, 84, 92);
PIN_SIZE(moq_sub_cfg_t, 128, 88, 96);
PIN_AT(moq_sub_track_cfg_t, location, 64, 36, 36);
PIN_AT(moq_sub_track_cfg_t, fill, 72, 40, 40);
PIN_AT(moq_sub_track_cfg_t, rendezvous_timeout_ms, 80, 44, 44);
/* The delivery-timeout pointers start at the previous sizeof (88/48/48)
 * and keep the struct's alignment. */
PIN_AT(moq_sub_track_cfg_t, object_delivery_timeout_ms, 88, 48, 48);
PIN_AT(moq_sub_track_cfg_t, subgroup_delivery_timeout_ms, 96, 52, 52);
PIN_SIZE(moq_sub_track_cfg_t, 104, 56, 56);
PIN_AT(moq_subscribe_tracks_cfg_t, options_flags, 48, 24, 24);
PIN_AT(moq_subscribe_tracks_cfg_t, group_order, 52, 28, 28);
PIN_AT(moq_subscribe_tracks_cfg_t, include_properties, 56, 32, 32);
PIN_SIZE(moq_subscribe_tracks_cfg_t, 64, 36, 36);
PIN_AT(moq_subscribe_tracks_request_event_t, options, 48, 32, 32);
PIN_SIZE(moq_subscribe_tracks_request_event_t, 56, 36, 40);
_Static_assert(MOQ_SUBSCRIBE_TRACKS_OPTIONS_V0_SIZE ==
               sizeof(moq_subscribe_tracks_options_t),
               "SUBSCRIBE_TRACKS options v0 is the whole record");
PIN_SIZE(moq_subscribe_tracks_options_t, 16, 16, 16);

/* Namespace updates: new records, each v0 the whole layout. */
_Static_assert(MOQ_NAMESPACE_UPDATE_RECORD_V0_SIZE ==
               sizeof(moq_namespace_update_record_t), "update record v0");
_Static_assert(MOQ_NAMESPACE_UPDATE_CFG_V0_SIZE ==
               sizeof(moq_namespace_update_cfg_t), "update cfg v0");
PIN_AT(moq_namespace_update_record_t, update_request_id, 8, 8, 8);
PIN_AT(moq_namespace_update_record_t, track_namespace_prefix, 16, 16, 16);
PIN_AT(moq_namespace_update_record_t, forward, 32, 24, 24);
PIN_AT(moq_namespace_update_record_t, tokens, 40, 32, 32);
PIN_AT(moq_namespace_update_record_t, token_count, 48, 36, 36);
PIN_SIZE(moq_namespace_update_record_t, 56, 40, 40);
PIN_AT(moq_namespace_update_cfg_t, track_namespace_prefix, 8, 8, 8);
PIN_AT(moq_namespace_update_cfg_t, forward, 24, 16, 16);
PIN_AT(moq_namespace_update_cfg_t, auth_tokens, 32, 24, 24);
PIN_AT(moq_namespace_update_cfg_t, auth_token_count, 40, 28, 28);
PIN_SIZE(moq_namespace_update_cfg_t, 48, 32, 32);
PIN_AT(moq_namespace_update_request_event_t, update_request_id, 8, 8, 8);
PIN_AT(moq_namespace_update_request_event_t, update, 16, 16, 16);
PIN_SIZE(moq_namespace_update_request_event_t, 24, 20, 24);
PIN_SIZE(moq_ns_sub_update_request_event_t, 24, 20, 24);
PIN_SIZE(moq_subscribe_tracks_update_request_event_t, 24, 20, 24);
PIN_AT(moq_namespace_update_result_event_t, update_request_id, 8, 8, 8);
PIN_AT(moq_namespace_update_result_event_t, outcome, 16, 16, 16);
PIN_AT(moq_namespace_update_result_event_t, local_role, 20, 20, 20);
PIN_AT(moq_namespace_update_result_event_t, error_code, 24, 24, 24);
PIN_AT(moq_namespace_update_result_event_t, reason, 32, 32, 32);
PIN_AT(moq_namespace_update_result_event_t, has_expires, 48, 40, 40);
PIN_AT(moq_namespace_update_result_event_t, expires_ms, 56, 48, 48);
PIN_SIZE(moq_namespace_update_result_event_t, 64, 56, 56);
PIN_SIZE(moq_ns_sub_update_result_event_t, 64, 56, 56);
PIN_SIZE(moq_subscribe_tracks_update_result_event_t, 64, 56, 56);
PIN_AT(moq_sub_update_cfg_t, fill, 56, 52, 56);
/* The never-read slot covers the eight-aligned old tail padding; the first
 * readable appended field sits at or past the previous sizeof (64/56/64). */
PIN_AT(moq_sub_update_cfg_t, _reserved_location, 64, 56, 60);
PIN_AT(moq_sub_update_cfg_t, location, 72, 60, 64);
PIN_AT(moq_sub_update_cfg_t, delivery_timeout_flags, 80, 64, 72);
PIN_AT(moq_sub_update_cfg_t, subgroup_delivery_timeout_ms, 96, 80, 88);
PIN_SIZE(moq_sub_update_cfg_t, 104, 88, 96);
PIN_SIZE(moq_fetch_update_cfg_t, 24, 16, 16);
PIN_SIZE(moq_filter_window_t, 40, 40, 40);
PIN_AT(moq_pub_cfg_t, max_fills, 128 - ABI_PUB_ENUM_SHIFT,
        68 - ABI_PUB_ENUM_SHIFT, 68 - ABI_PUB_ENUM_SHIFT);
PIN_AT(moq_pub_cfg_t, on_subscribe_v1, 136 - ABI_PUB_ENUM_SHIFT,
        72 - ABI_PUB_ENUM_SHIFT, 72 - ABI_PUB_ENUM_SHIFT);
PIN_AT(moq_pub_cfg_t, on_subscribe_v1_ctx, 144 - ABI_PUB_ENUM_SHIFT,
        76 - ABI_PUB_ENUM_SHIFT, 76 - ABI_PUB_ENUM_SHIFT);
PIN_AT(moq_pub_cfg_t, max_fill_snapshot_bytes, 152 - ABI_PUB_ENUM_SHIFT,
        80 - ABI_PUB_ENUM_SHIFT, 80);
PIN_AT(moq_pub_cfg_t, on_fill_request, 160 - ABI_PUB_ENUM_SHIFT,
        88 - ABI_PUB_ENUM_SHIFT, 88);
PIN_AT(moq_pub_cfg_t, on_fill_request_ctx, 168 - ABI_PUB_ENUM_SHIFT,
        92 - ABI_PUB_ENUM_SHIFT, 92);
PIN_AT(moq_pub_cfg_t, on_fill_terminal, 176 - ABI_PUB_ENUM_SHIFT,
        96 - ABI_PUB_ENUM_SHIFT, 96);
PIN_AT(moq_pub_cfg_t, on_fill_terminal_ctx, 184 - ABI_PUB_ENUM_SHIFT,
        100 - ABI_PUB_ENUM_SHIFT, 100);
PIN_THROUGH(moq_pub_cfg_t, on_fill_terminal_ctx, 192 - ABI_PUB_ENUM_SHIFT,
            104 - ABI_PUB_ENUM_SHIFT, 104, 8, 4, 8);
PIN_AFTER(moq_pub_cfg_t, on_fill_terminal_ctx, on_new_group_request);
PIN_AT(moq_pub_cfg_t, on_new_group_request, 192 - ABI_PUB_ENUM_SHIFT,
        104 - ABI_PUB_ENUM_SHIFT, 104);
PIN_AT(moq_pub_cfg_t, on_new_group_request_ctx, 200 - ABI_PUB_ENUM_SHIFT,
        108 - ABI_PUB_ENUM_SHIFT, 108);
/* on_goaway starts at the previous sizeof (208/112/112). */
PIN_AT(moq_pub_cfg_t, on_goaway, 208 - ABI_PUB_ENUM_SHIFT,
        112 - ABI_PUB_ENUM_SHIFT, 112);
PIN_SIZE(moq_pub_cfg_t, 216 - ABI_PUB_ENUM_SHIFT,
          116 - ABI_PUB_ENUM_SHIFT, 120);
PIN_AT(moq_pub_subscribe_info_v1_t, struct_size, 0, 0, 0);
PIN_AT(moq_pub_subscribe_info_v1_t, options, 128, 92, 96);
PIN_SIZE(moq_pub_subscribe_info_v1_t, 136, 96, 104);
PIN_AT(moq_sub_stats_t, fill_objects, 192, 184, 192);
PIN_AT(moq_sub_stats_t, fill_items_queued, 224, 216, 224);
PIN_AT(moq_sub_stats_t, malformed_tracks, 232, 224, 232);
PIN_SIZE(moq_sub_stats_t, 240, 232, 240);

/* Prior Group/Object ID Gaps and the subgroup PROPERTIES form on object
 * events, the gaps on the object write cfg: every new field starts at or
 * past the original sizeof (the fill event and the cfg through a never-read
 * reserved word where that sizeof ends in tail padding). */
PIN_AFTER(moq_object_received_event_t, properties, has_prior_group_gap);
PIN_AT(moq_object_received_event_t, has_prior_group_gap, 64, 56, 56);
PIN_AT(moq_object_received_event_t, subgroup_properties, 66, 58, 58);
PIN_AT(moq_object_received_event_t, prior_object_gap, 80, 72, 72);
PIN_SIZE(moq_object_received_event_t, 88, 80, 80);
PIN_DETAIL(moq_object_received_event_t);
/* The chunk event's terminal enum changes its sizeof under short enums, so
 * its tail is pinned relative to the sizeof through error_code. */
PIN_AFTER(moq_object_chunk_event_t, error_code, has_prior_group_gap);
_Static_assert(offsetof(moq_object_chunk_event_t, has_prior_group_gap) ==
               ABI_SIZEOF_THROUGH(moq_object_chunk_event_t, error_code),
               "moq_object_chunk_event_t.has_prior_group_gap: at the old sizeof");
_Static_assert(offsetof(moq_object_chunk_event_t, subgroup_properties) ==
               offsetof(moq_object_chunk_event_t, has_prior_group_gap) + 2,
               "moq_object_chunk_event_t.subgroup_properties: offset");
_Static_assert(offsetof(moq_object_chunk_event_t, prior_object_gap) ==
               offsetof(moq_object_chunk_event_t, prior_group_gap) + 8,
               "moq_object_chunk_event_t.prior_object_gap: offset");
_Static_assert(sizeof(moq_object_chunk_event_t) ==
               offsetof(moq_object_chunk_event_t, prior_object_gap) + 8,
               "moq_object_chunk_event_t: sizeof");
PIN_DETAIL(moq_object_chunk_event_t);
PIN_AFTER(moq_fetch_object_event_t, properties, has_prior_group_gap);
PIN_AT(moq_fetch_object_event_t, has_prior_group_gap, 56, 48, 48);
PIN_AT(moq_fetch_object_event_t, prior_object_gap, 72, 64, 64);
PIN_SIZE(moq_fetch_object_event_t, 80, 72, 72);
PIN_AFTER(moq_fill_object_event_t, properties, has_prior_group_gap);
PIN_AT(moq_fill_object_event_t, has_prior_group_gap, 84, 72, 72);
PIN_AT(moq_fill_object_event_t, prior_object_gap, 96, 84, 88);
PIN_SIZE(moq_fill_object_event_t, 104, 92, 96);
PIN_AFTER(moq_object_cfg_t, properties, has_prior_group_gap);
PIN_AT(moq_object_cfg_t, has_prior_group_gap, 36, 24, 28);
PIN_AT(moq_object_cfg_t, prior_group_gap, 40, 28, 32);
PIN_AT(moq_object_cfg_t, prior_object_gap, 48, 36, 40);
PIN_SIZE(moq_object_cfg_t, 56, 44, 48);

PIN_SIZE(moq_namespace_done_event_t, 8, 8, 8);
PIN_SIZE(moq_namespace_cancelled_event_t, 32, 24, 24);
#define PIN_TYPED_TERMINAL(T, h) \
    PIN_AT(T, h, 0, 0, 0); \
    PIN_AT(T, cause, 8, 8, 8); \
    PIN_AT(T, local_role, 12, 12, 12); \
    PIN_AT(T, stream_error_code, 16, 16, 16); \
    PIN_SIZE(T, 24, 24, 24)
PIN_TYPED_TERMINAL(moq_track_status_terminated_event_t, handle);
PIN_TYPED_TERMINAL(moq_namespace_terminated_event_t, ann);
PIN_TYPED_TERMINAL(moq_ns_subscription_terminated_event_t, handle);
_Static_assert(MOQ_REQUEST_TERMINAL_NONE == 0 &&
               MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM == 1 &&
               MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING == 2 &&
               MOQ_REQUEST_TERMINAL_MISSING_RESPONSE == 3 &&
               MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE == 4 &&
               MOQ_REQUEST_TERMINAL_PEER_FIN_COMPLETE == 5,
               "request terminal cause values preserve the frozen domain");
#undef PIN_TYPED_TERMINAL
_Static_assert(MOQ_EVENT_TRACK_STATUS_TERMINATED == 51 && MOQ_EVENT_FILL_CREATED == 52 &&
               MOQ_EVENT_FILL_REQUEST == 53 && MOQ_EVENT_FILL_OBJECT == 54 &&
               MOQ_EVENT_FILL_GAP == 55 && MOQ_EVENT_FILL_TERMINAL == 56 &&
               MOQ_EVENT_NAMESPACE_TERMINATED == 57 && MOQ_EVENT_PUBLISH_STATE == 58 &&
               MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED == 59, "typed event registry");
PIN_AT(moq_unsubscribed_event_t, cause, 8, 8, 8);
PIN_AT(moq_unsubscribed_event_t, stream_error_code, 16, 16, 16);
PIN_SIZE(moq_unsubscribed_event_t, 24, 24, 24);
PIN_AT(moq_subscribe_updated_event_t, options, 96, 80, 88);
PIN_AT(moq_subscribe_updated_event_t, answer_owed, 104, 84, 96);
PIN_AT(moq_subscribe_updated_event_t, fill_requested, 105, 85, 97);
PIN_AT(moq_subscribe_updated_event_t, update_count, 108, 88, 100);
PIN_AT(moq_subscribe_updated_event_t, update_request_id, 112, 92, 104);
PIN_SIZE(moq_subscribe_updated_event_t, 120, 100, 112);
PIN_AT(moq_publish_updated_event_t, options, 96, 80, 88);
PIN_AT(moq_publish_updated_event_t, answer_owed, 104, 84, 96);
PIN_AT(moq_publish_updated_event_t, fill_requested, 105, 85, 97);
PIN_AT(moq_publish_updated_event_t, update_count, 108, 88, 100);
PIN_AT(moq_publish_updated_event_t, update_request_id, 112, 92, 104);
PIN_SIZE(moq_publish_updated_event_t, 120, 100, 112);
PIN_AT(moq_fetch_cancelled_event_t, cause, 8, 8, 8);
PIN_AT(moq_fetch_cancelled_event_t, stream_error_code, 16, 16, 16);
PIN_SIZE(moq_fetch_cancelled_event_t, 24, 24, 24);
PIN_AT(moq_fetch_reset_event_t, cause, 24, 24, 24);
PIN_SIZE(moq_fetch_reset_event_t, 32, 32, 32);
PIN_AT(moq_publish_finished_event_t, cause, 40, 32, 32);
PIN_AT(moq_publish_finished_event_t, stream_error_code, 48, 40, 40);
PIN_SIZE(moq_publish_finished_event_t, 56, 48, 48);
PIN_AT(moq_publish_unsubscribed_event_t, cause, 8, 8, 8);
PIN_AT(moq_publish_unsubscribed_event_t, stream_error_code, 16, 16, 16);
PIN_SIZE(moq_publish_unsubscribed_event_t, 24, 24, 24);
PIN_AT(moq_subscribe_tracks_cancelled_event_t, cause, 8, 8, 8);
PIN_AT(moq_subscribe_tracks_cancelled_event_t, stream_error_code, 16, 16, 16);
PIN_SIZE(moq_subscribe_tracks_cancelled_event_t, 24, 24, 24);

#ifdef ABI_HAVE_LOC
PIN_AT(moq_loc_headers_t, timestamp, 8, 8, 8);
PIN_AT(moq_loc_headers_t, timescale, 24, 20, 24);
PIN_AT(moq_loc_headers_t, video_config, 48, 44, 48);
PIN_SIZE(moq_loc_headers_t, 64, 52, 56);
PIN_AT(moq_loc_metadata_t, struct_size, 0, 0, 0);
PIN_AT(moq_loc_metadata_t, present, 4, 4, 4);
PIN_AT(moq_loc_metadata_t, timestamp, 8, 8, 8);
PIN_AT(moq_loc_metadata_t, timescale, 16, 16, 16);
PIN_AT(moq_loc_metadata_t, marking_len, 24, 24, 24);
PIN_AT(moq_loc_metadata_t, marking, 25, 25, 25);
PIN_AT(moq_loc_metadata_t, audio_level, 29, 29, 29);
PIN_AT(moq_loc_metadata_t, reserved, 30, 30, 30);
PIN_AT(moq_loc_metadata_t, video_config, 32, 32, 32);
PIN_AT(moq_loc_metadata_t, audio_config, 48, 40, 40);
PIN_SIZE(moq_loc_metadata_t, 64, 48, 48);
#endif
#ifdef ABI_HAVE_ENDPOINT
/* The SETUP token pair and the certificate callback pair start at the
 * sizeof through handshake_timeout_us. */
PIN_AT(moq_endpoint_cfg_t, setup_auth_tokens, 144,
       84 - ABI_ENDPOINT_ENUM_SHIFT, 88 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
PIN_AT(moq_endpoint_cfg_t, setup_auth_token_count, 152,
       88 - ABI_ENDPOINT_ENUM_SHIFT, 92 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
PIN_AT(moq_endpoint_cfg_t, verify_certificate, 160,
       92 - ABI_ENDPOINT_ENUM_SHIFT, 96 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
PIN_AT(moq_endpoint_cfg_t, verify_certificate_ctx, 168,
       96 - ABI_ENDPOINT_ENUM_SHIFT, 100 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
/* The URI fields start at the sizeof through verify_certificate_ctx. */
PIN_AT(moq_endpoint_cfg_t, uri_mode, 176,
       100 - ABI_ENDPOINT_ENUM_SHIFT, 104 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
PIN_AT(moq_endpoint_cfg_t, preferred_version, 180,
       104 - ABI_ENDPOINT_ENUM_SHIFT, 108 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
PIN_AT(moq_endpoint_cfg_t, accept_uri, 184,
       108 - ABI_ENDPOINT_ENUM_SHIFT, 112 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
PIN_AT(moq_endpoint_cfg_t, accept_uri_ctx, 192,
       112 - ABI_ENDPOINT_ENUM_SHIFT, 116 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
/* The adapter queue bound starts at the sizeof through accept_uri_ctx. */
PIN_AT(moq_endpoint_cfg_t, send_queue_cap_bytes, 200,
       116 - ABI_ENDPOINT_ENUM_SHIFT, 120 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
PIN_SIZE(moq_endpoint_cfg_t, 208,
         124 - ABI_ENDPOINT_ENUM_SHIFT, 128 - 2 * ABI_ENDPOINT_ENUM_SHIFT);
#endif

#ifdef ABI_HAVE_MEDIA_RECEIVER
PIN_THROUGH(moq_media_track_event_t, _reserved_local_failure, 104, 84, 96, 8, 4, 8);
PIN_AFTER(moq_media_track_event_t, _reserved_local_failure, initiating_request_id);
PIN_AT(moq_media_track_event_t, initiating_request_id, 104, 84, 96);
PIN_AT(moq_media_track_event_t, fill_reason, 112, 92, 104);
PIN_AT(moq_media_track_event_t, fill_error_domain, 116, 96, 108);
PIN_AT(moq_media_track_event_t, fill_error_code, 120, 100, 112);
PIN_THROUGH(moq_media_track_event_t, fill_error_code, 128, 108, 120, 8, 4, 8);
PIN_AFTER(moq_media_track_event_t, fill_error_code, delivery_origin);
PIN_AT(moq_media_track_event_t, delivery_origin, 128, 108, 120);
PIN_AT(moq_media_track_event_t, _reserved_delivery, 132, 112, 124);
PIN_AT(moq_media_track_event_t, delivery_initiating_request_id, 136, 116, 128);
PIN_THROUGH(moq_media_track_event_t, delivery_initiating_request_id,
            144, 124, 136, 8, 4, 8);
PIN_AFTER(moq_media_track_event_t, delivery_initiating_request_id,
          fill_unreported_loss_causes);
PIN_AT(moq_media_track_event_t, fill_unreported_loss_causes, 144, 124, 136);
PIN_AT(moq_media_track_event_t, _reserved_fill_loss, 148, 128, 140);
PIN_AT(moq_media_track_event_t, fill_unreported_loss_group, 152, 132, 144);
PIN_AT(moq_media_track_event_t, fill_unreported_loss_object, 160, 140, 152);
PIN_SIZE(moq_media_track_event_t, 168, 148, 160);
#endif
#ifdef ABI_HAVE_SIM
PIN_AT(moq_simpair_cfg_t, server_defer_subscription_updates, 168, 136, 152);
PIN_AT(moq_simpair_cfg_t, client_defer_subscription_updates, 169, 137, 153);
PIN_AT(moq_simpair_cfg_t, _reserved_cfg_tail, 172, 140, 156);
PIN_AT(moq_simpair_cfg_t, client_max_data_streams, 176, 144, 160);
PIN_SIZE(moq_simpair_cfg_t, 184, 148, 168);
#endif
#ifdef ABI_HAVE_RELAY
PIN_SIZE(moqr_bind_cfg_t, 88, 64, 64);
PIN_SIZE(moqr_core_relay_cfg_t, 168, 132, 144);
PIN_AT(moqr_shards_cfg_t, struct_size, 0, 0, 0);
PIN_AT(moqr_shards_cfg_t, alloc, 8, 4, 4);
PIN_AT(moqr_shards_cfg_t, shards, 16, 8, 8);
PIN_AT(moqr_shards_cfg_t, trace_ring_records, 20, 12, 12);
PIN_AT(moqr_shards_cfg_t, placement, 24, 16, 16);
PIN_AT(moqr_shards_cfg_t, permute_seed, 32, 20, 24);
PIN_AT(moqr_shards_cfg_t, core_cfg, 40, 28, 32);
PIN_AT(moqr_shards_cfg_t, bind_cfg, 208, 160, 176);
PIN_AT(moqr_shards_cfg_t, mailbox_entries, 296, 224, 240);
PIN_AT(moqr_shards_cfg_t, journal_entries, 300, 228, 244);
PIN_AT(moqr_shards_cfg_t, pending_demand_entries, 304, 232, 248);
PIN_AT(moqr_shards_cfg_t, live_visibility, 308, 236, 252);
PIN_AT(moqr_shards_cfg_t, demand_channel_entries, 312, 240, 256);
PIN_AT(moqr_shards_cfg_t, admit_remote_demand, 316, 244, 260);
PIN_AT(moqr_shards_cfg_t, demand_channel_bytes, 320, 248, 264);
PIN_AT(moqr_shards_cfg_t, pump_turn_messages, 328, 256, 272);
PIN_AT(moqr_shards_cfg_t, pump_turn_bytes, 336, 260, 280);
PIN_AT(moqr_shards_cfg_t, pump_subgroup_slots, 344, 268, 288);
PIN_AT(moqr_shards_cfg_t, fill_timeout_default_ms, 352, 272, 296);
PIN_AT(moqr_shards_cfg_t, fill_timeout_max_ms, 360, 280, 304);
PIN_SIZE(moqr_shards_cfg_t, 368, 288, 312);
PIN_AT(moqr_subscribe_req_t, _reserved_range_end_holds, 92, 68, 76);
PIN_AT(moqr_subscribe_req_t, range_end_holds, 96, 72, 80);
PIN_THROUGH(moqr_subscribe_req_t, range_end_holds, 104, 76, 88, 8, 4, 8);
PIN_AFTER(moqr_subscribe_req_t, range_end_holds, location);
PIN_AT(moqr_subscribe_req_t, _reserved_location, 100, 76, 84);
PIN_AT(moqr_subscribe_req_t, location, 104, 80, 88);
/* The rendezvous pair starts past the layout through location (its tail
 * padding included), deadline first. */
PIN_AT(moqr_subscribe_req_t, rendezvous_deadline_us, 112, 84, 96);
PIN_AT(moqr_subscribe_req_t, rendezvous, 120, 92, 104);
PIN_SIZE(moqr_subscribe_req_t, 128, 96, 112);
PIN_AT(moqr_fetch_req_t, report_gaps, 88, 68, 72);
PIN_AT(moqr_fetch_req_t, report_unknown, 89, 69, 73);
/* The range form starts past the layout through report_unknown, so no field
 * of it sits in that layout's tail padding. */
PIN_AT(moqr_fetch_req_t, relative_start, 96, 72, 80);
PIN_AT(moqr_fetch_req_t, range_form, 104, 80, 88);
PIN_AT(moqr_fetch_req_t, report_pending, 108, 84, 92);
PIN_SIZE(moqr_fetch_req_t, 112, 88, 96);
PIN_AT(moqr_park_req_t, sub_forward_off, 112, 80, 88);
PIN_AT(moqr_park_req_t, sub_rendezvous_deadline_us, 120, 84, 96);
PIN_AT(moqr_park_req_t, sub_rendezvous, 128, 92, 104);
PIN_SIZE(moqr_park_req_t, 136, 96, 112);
PIN_AT(moqr_tracks_req_t, struct_size, 0, 0, 0);
PIN_AT(moqr_tracks_req_t, prefix, 8, 4, 4);
PIN_AT(moqr_tracks_req_t, cookie, 24, 12, 16);
PIN_AT(moqr_tracks_req_t, forward, 32, 20, 24);
PIN_AT(moqr_tracks_req_t, include_properties, 33, 21, 25);
PIN_AT(moqr_tracks_req_t, group_order, 36, 24, 28);
PIN_AT(moqr_tracks_req_t, held, 40, 28, 32);
PIN_SIZE(moqr_tracks_req_t, 48, 32, 40);
PIN_AT(moqr_discovery_t, struct_size, 0, 0, 0);
PIN_AT(moqr_discovery_t, cookie, 8, 4, 8);
PIN_AT(moqr_discovery_t, ns_parts, 48, 36, 40);
PIN_AT(moqr_discovery_t, name, 560, 292, 296);
PIN_AT(moqr_discovery_t, group_order, 584, 308, 312);
PIN_AT(moqr_discovery_t, largest_object, 600, 324, 328);
PIN_AT(moqr_discovery_t, authorized, 608, 332, 336);
PIN_AT(moqr_discovery_t, input_gen, 612, 336, 340);
PIN_SIZE(moqr_discovery_t, 616, 340, 344);
_Static_assert(MOQR_DISCOVERY_V0_SIZE == ABI_PICK(608, 332, 336),
               "MOQR_DISCOVERY_V0_SIZE: frozen");
#endif
#ifdef ABI_HAVE_MVFST
PIN_AT(moq_mvfst_managed_cfg_t, _reserved_target, 192, 120, 124);
PIN_AT(moq_mvfst_managed_cfg_t, target, 200, 124, 128);
PIN_SIZE(moq_mvfst_managed_cfg_t, 208, 128, 136);
#endif
#ifdef ABI_HAVE_PROXYGEN
PIN_AT(moq_proxygen_wt_managed_cfg_t, target, 136, 76, 80);
PIN_SIZE(moq_proxygen_wt_managed_cfg_t, 144, 80, 88);
#endif

/* -- New structs with a frozen v0 layout -------------------------------- *
 * A consuming call measures a caller's struct_size against these, so they
 * never change; an appended field starts at or past them. */

#define PIN_V0(V0, s64, s32a4, s32a8)                                         \
    _Static_assert((V0) == ABI_PICK(s64, s32a4, s32a8), #V0 ": frozen")
PIN_V0(MOQ_PUBLISH_STATE_CFG_V0_SIZE, 16, 12, 12);
PIN_V0(MOQ_ACCEPT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE, 16, 12, 16);
PIN_V0(MOQ_REJECT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE, 40, 28, 32);
PIN_V0(MOQ_LOCATION_FILTER_CFG_V0_SIZE, 48, 48, 48);
/* The sized draft-18 SETUP options codec record (tooling tier): struct_size
 * leads and v0 is frozen through `authority`. Both codec calls measure the
 * caller's allocation against MOQ_D18_SETUP_OPTS_V0_SIZE. */
PIN_AT(moq_d18_setup_opts_t, struct_size, 0, 0, 0);
PIN_AT(moq_d18_setup_opts_t, has_path, 4, 4, 4);
PIN_AT(moq_d18_setup_opts_t, max_auth_token_cache_size, 8, 8, 8);
PIN_AT(moq_d18_setup_opts_t, auth_token_count, 16, 16, 16);
PIN_AT(moq_d18_setup_opts_t, auth_tokens, 24, 20, 24);
PIN_AT(moq_d18_setup_opts_t, path, 664, 468, 536);
PIN_AT(moq_d18_setup_opts_t, authority, 680, 476, 544);
PIN_SIZE(moq_d18_setup_opts_t, 696, 484, 552);
PIN_V0(MOQ_D18_SETUP_OPTS_V0_SIZE, 696, 484, 552);
PIN_AT(moq_publish_state_cfg_t, filter, 8, 8, 8);
PIN_AT(moq_sub_fill_item_t, struct_size, 0, 0, 0);
PIN_AT(moq_sub_fill_item_t, kind, 4, 4, 4);
PIN_AT(moq_sub_fill_item_t, request, 8, 8, 8);
PIN_AT(moq_sub_fill_item_t, track, 16, 16, 16);
PIN_AT(moq_sub_fill_item_t, initiating_request_id, 24, 20, 24);
PIN_AT(moq_sub_fill_item_t, u, 32, 28, 32);
PIN_AT(moq_sub_fill_item_t, u.object.payload, 64, 56, 60);
PIN_AT(moq_sub_fill_item_t, u.object.properties, 72, 60, 64);
PIN_SIZE(moq_sub_fill_item_t, 80, 64, 72);
PIN_V0(MOQ_SUB_FILL_ITEM_V0_SIZE, 80, 64, 72);
PIN_AT(moq_sub_local_failure_t, struct_size, 0, 0, 0);
PIN_AT(moq_sub_local_failure_t, owner_kind, 4, 4, 4);
PIN_AT(moq_sub_local_failure_t, local_result, 8, 8, 8);
PIN_AT(moq_sub_local_failure_t, reserved, 12, 12, 12);
PIN_AT(moq_sub_local_failure_t, track, 16, 16, 16);
PIN_AT(moq_sub_local_failure_t, fetch, 24, 20, 20);
PIN_AT(moq_sub_local_failure_t, fill, 32, 24, 24);
PIN_AT(moq_sub_local_failure_t, initiating_request_id, 40, 32, 32);
PIN_SIZE(moq_sub_local_failure_t, 48, 40, 40);
PIN_V0(MOQ_SUB_LOCAL_FAILURE_V0_SIZE, 48, 40, 40);
_Static_assert(sizeof(((moq_sub_local_failure_t *)0)->owner_kind) == 4,
               "local failure kind is enum-mode independent");
_Static_assert(sizeof(((moq_sub_local_failure_t *)0)->local_result) == 4,
               "local result has signed 32-bit storage");

/* -- INCLUDE_PROPERTIES and TRACK_STATUS_OK Track Properties ------------ *
 * Each request config gains the INCLUDE_PROPERTIES pair after its previous
 * last field; the TRACK_STATUS answer gains its Track Properties after
 * expires_ms. None of them starts inside the layout it extends. */
PIN_THROUGH(moq_fetch_cfg_t, fill_timeout_ms, 144, 108, 112, 8, 4, 8);
PIN_AFTER(moq_fetch_cfg_t, fill_timeout_ms, has_include_properties);
PIN_AT(moq_fetch_cfg_t, has_include_properties, 144, 108, 112);
PIN_AT(moq_fetch_cfg_t, include_properties, 145, 109, 113);
PIN_SIZE(moq_fetch_cfg_t, 152, 112, 120);
PIN_THROUGH(moq_track_status_cfg_t, auth_token_count, 56, 28, 28, 8, 4, 4);
PIN_AFTER(moq_track_status_cfg_t, auth_token_count, has_include_properties);
PIN_AT(moq_track_status_cfg_t, has_include_properties, 56, 28, 28);
PIN_AT(moq_track_status_cfg_t, include_properties, 57, 29, 29);
PIN_SIZE(moq_track_status_cfg_t, 64, 32, 32);
PIN_THROUGH(moq_accept_track_status_cfg_t, expires_ms, 40, 36, 40, 8, 4, 8);
PIN_AFTER(moq_accept_track_status_cfg_t, expires_ms, track_properties);
PIN_AT(moq_accept_track_status_cfg_t, track_properties, 40, 36, 40);
PIN_SIZE(moq_accept_track_status_cfg_t, 56, 44, 48);

/* The publisher track's Track Properties and DYNAMIC_GROUPS follow a
 * never-read reservation past the layout that ends at monotonic_groups; the
 * new-group callback pair follows the fill callbacks. */
PIN_THROUGH(moq_pub_track_cfg_t, monotonic_groups, 72, 44, 48, 8, 4, 8);
PIN_AFTER(moq_pub_track_cfg_t, monotonic_groups, track_properties);
PIN_AT(moq_pub_track_cfg_t, track_properties, 72, 48, 48);
PIN_AT(moq_pub_track_cfg_t, dynamic_groups, 88, 56, 56);
/* group_order follows another reservation past the layout that ends at
 * dynamic_groups. */
PIN_THROUGH(moq_pub_track_cfg_t, dynamic_groups, 96, 60, 64, 8, 4, 8);
PIN_AFTER(moq_pub_track_cfg_t, dynamic_groups, group_order);
PIN_AT(moq_pub_track_cfg_t, group_order, 96, 64, 64);
PIN_SIZE(moq_pub_track_cfg_t, 104, 68, 72);
PIN_AT(moq_pub_new_group_request_info_t, requested_group, 8, 8, 8);
PIN_AT(moq_pub_new_group_request_info_t, current_group, 16, 16, 16);
PIN_SIZE(moq_pub_new_group_request_info_t, 24, 24, 24);

#ifdef ABI_HAVE_MEDIA_RECEIVER
PIN_AFTER(moq_media_receiver_stats_t, catalog_complete, tracks_malformed);
/* The receiver companion starts at the complete sizeof through the MPEG2TS
 * block (104 LP64, 72 i386, 80 ARM; i386 short enums shift it by 4), never
 * in that layout's tail padding. */
#define ABI_RECEIVER_ENUM_SHIFT (sizeof(moq_media_time_mode_t) < 4 ? 4 : 0)
PIN_THROUGH(moq_media_receiver_cfg_t, mpeg2ts_join_psi,
            104, 72 - ABI_RECEIVER_ENUM_SHIFT, 80, 8, 4, 8);
PIN_AFTER(moq_media_receiver_cfg_t, mpeg2ts_join_psi, companion_profile);
PIN_AT(moq_media_receiver_cfg_t, companion_profile,
       104, 72 - ABI_RECEIVER_ENUM_SHIFT, 80);
PIN_SIZE(moq_media_receiver_cfg_t, 112, 76 - ABI_RECEIVER_ENUM_SHIFT, 88);
/* The other appends after the MPEG2TS fields start past the complete
 * sizeof of the layout before them. */
PIN_AFTER(moq_media_receiver_track_subscribe_cfg_t, mpeg2ts_program_number, location);
PIN_AFTER(moq_media_object_t, discontinuous, metadata);
PIN_AFTER(moq_media_receiver_stats_t, reorder_refused, tracks_malformed);
#endif

#ifdef ABI_HAVE_MEDIA_SENDER
/* The media track's Track Properties start exactly at the sizeof of the
 * layout that ends at the MPEG2TS block (its enum members change that sizeof
 * under short enums, so the pin is relational), never in its tail padding.
 * The MPEG2TS block starts past the layout that ends at the publisher
 * priority pair, which itself starts past the layout that ends at
 * alt_group. */
PIN_AFTER(moq_media_track_cfg_t, alt_group, has_publisher_priority);
PIN_AFTER(moq_media_track_cfg_t, publisher_priority, extensions);
PIN_AFTER(moq_media_track_cfg_t, mpeg2ts_forward_before_random_access,
          track_properties);
PIN_AFTER(moq_media_track_cfg_t, mpeg2ts_forward_before_random_access,
          dynamic_groups);
PIN_AFTER(moq_media_track_cfg_t, label, group_order);
_Static_assert(offsetof(moq_media_track_cfg_t, track_properties) ==
               ABI_SIZEOF_THROUGH(moq_media_track_cfg_t,
                                  mpeg2ts_forward_before_random_access),
               "moq_media_track_cfg_t.track_properties: at the MPEG2TS-era sizeof");
#endif

int main(void)
{
    return 0;
}
