#ifndef MOQ_SUBSCRIBER_H
#define MOQ_SUBSCRIBER_H

/*
 * Subscriber facade.
 *
 * Manages track subscriptions and object delivery for a subscriber.
 * Hides session event plumbing; the caller uses subscribe + tick + poll.
 *
 * Event ownership: the facade owns session events. The caller must
 * NOT also poll session events. Call moq_sub_tick() to process events
 * and moq_sub_poll_object() to drain received objects.
 *
 * Supports whole-object delivery, streaming chunks, track status
 * queries, and fetch.
 *
 * Thread safety: none. All calls serialized on the session's thread.
 */

#include "session.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_subscriber moq_subscriber_t;
typedef struct moq_sub_track  moq_sub_track_t;

/* -- Callbacks ---------------------------------------------------- */

typedef struct moq_sub_callbacks {
    uint32_t struct_size;
    void    *ctx;
    void (*on_subscribed)(void *ctx, moq_sub_track_t *track);
    void (*on_subscribe_error)(void *ctx, moq_sub_track_t *track,
                                moq_request_error_t error_code,
                                moq_bytes_t reason);
    void (*on_draining)(void *ctx);
    void (*on_closed)(void *ctx, uint64_t error_code);
} moq_sub_callbacks_t;

MOQ_API void moq_sub_callbacks_init(moq_sub_callbacks_t *cb);

/* -- Configuration ------------------------------------------------ */

typedef struct moq_sub_cfg {
    uint32_t            struct_size;
    uint32_t            max_tracks;
    uint32_t            max_objects;
    moq_sub_callbacks_t callbacks;
    uint32_t            max_fetches;
    uint32_t            max_fetch_items;
    bool                streaming_objects;
    uint32_t            max_chunks;
    /* Appended: fired on GOAWAY with the new-session URI. The URI is
     * borrowed and valid only for the duration of the callback. Uses
     * callbacks.ctx. If set, takes priority over callbacks.on_draining. */
    void (*on_goaway)(void *ctx, moq_bytes_t new_session_uri);
    /* Appended: fired when a subscription completes (SUBSCRIBE_DONE) with the
     * peer's status code; the track has transitioned to DONE. The status lets
     * the app distinguish a non-terminal "Track Ended" (live->VOD step 1, status
     * 0x2: the publisher keeps the track joinable, so the app may re-subscribe /
     * Joining-FETCH the VOD content) from a terminal completion. Fired
     * synchronously inside moq_sub_tick; uses callbacks.ctx. After it returns the
     * track handle is DONE -- release it with moq_sub_release_track() to reuse
     * the slot. */
    void (*on_subscribe_done)(void *ctx, moq_sub_track_t *track,
                              uint64_t status_code);
    /* Appended: byte budget for payload+properties rcbufs the facade retains
     * in its object and fetch-object queues. The session releases its own
     * receive-buffer budget once an event is polled, so without this cap a peer
     * could park unbounded bytes in the facade's count-only queues. 0 selects
     * the default (16 MiB, matching the session's default receive budget).
     * Values larger than SIZE_MAX are clamped to SIZE_MAX. */
    uint64_t max_queued_object_bytes;
} moq_sub_cfg_t;

/* Pointer-only initializer. Clears and stamps ONLY the frozen v0 prefix (the
 * layout before the first appended field). It cannot know the caller's storage
 * size, so it must not write the full current sizeof -- that would overflow a
 * caller compiled against the original (smaller) struct. Appended fields
 * (on_goaway, on_subscribe_done, max_queued_object_bytes) default to disabled;
 * to set any of them, or to initialize the full current struct, use
 * moq_sub_cfg_init_sized(). */
MOQ_API void moq_sub_cfg_init(moq_sub_cfg_t *cfg);

/* Size-aware initializer. Clears and stamps min(cfg_size, sizeof current
 * struct): an older caller passes its smaller sizeof (prefix init), a newer
 * caller's extra trailing fields are left to its own initializer. Pass
 * sizeof(moq_sub_cfg_t) to initialize the full current struct with all appended
 * fields enabled. No-op if cfg is NULL or cfg_size is too small to hold
 * struct_size. */
MOQ_API void moq_sub_cfg_init_sized(moq_sub_cfg_t *cfg, size_t cfg_size);

/* -- Lifecycle ---------------------------------------------------- */

MOQ_API moq_result_t moq_sub_create(
    moq_session_t *session,
    const moq_alloc_t *alloc,
    const moq_sub_cfg_t *cfg,
    moq_subscriber_t **out);

MOQ_API void moq_sub_destroy(moq_subscriber_t *sub);

/*
 * Process session events. Dispatches subscribe, object, fetch,
 * track status, GOAWAY, and SESSION_CLOSED events.
 * Fires callbacks synchronously.
 *
 * Caller must NOT also call moq_session_poll_events.
 */
MOQ_API moq_result_t moq_sub_tick(moq_subscriber_t *sub, uint64_t now_us);

/* -- Subscribe ---------------------------------------------------- */

typedef struct moq_sub_track_cfg {
    uint32_t               struct_size;
    moq_namespace_t        track_namespace;
    moq_bytes_t            track_name;
    moq_subscribe_filter_t filter;
    uint8_t                subscriber_priority;
    bool                   has_subscriber_priority;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_sub_track_cfg_t;

MOQ_API void moq_sub_track_cfg_init(moq_sub_track_cfg_t *cfg);

MOQ_API moq_result_t moq_sub_subscribe(
    moq_subscriber_t *sub,
    const moq_sub_track_cfg_t *cfg,
    uint64_t now_us,
    moq_sub_track_t **out);

MOQ_API moq_result_t moq_sub_unsubscribe(
    moq_subscriber_t *sub,
    moq_sub_track_t *track,
    uint64_t now_us);

/* Release a terminal track's facade slot back to the pool so a later subscribe
 * can reuse it. Valid only for a track in DONE or ERROR state (e.g. after
 * SUBSCRIBE_DONE / on_subscribe_done, or a subscribe error); a still-live
 * (PENDING/ACTIVE) track must be torn down with moq_sub_unsubscribe() first and
 * returns MOQ_ERR_WRONG_STATE here. The track handle MUST NOT be used after
 * release. No session message is sent (the subscription is already complete). */
MOQ_API moq_result_t moq_sub_release_track(
    moq_subscriber_t *sub,
    moq_sub_track_t *track);

/* -- Subscription update ------------------------------------------ */

typedef struct moq_sub_update_cfg {
    uint32_t struct_size;
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_forward;
    bool     forward;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_us;
} moq_sub_update_cfg_t;

MOQ_API void moq_sub_update_cfg_init(moq_sub_update_cfg_t *cfg);

/*
 * Update an active subscription's parameters (priority, forward,
 * delivery timeout).  Only one update may be outstanding per track;
 * a second call before the peer responds returns MOQ_ERR_WRONG_STATE.
 *
 * The peer's response (accept or reject) is consumed internally by
 * the session core.  This slice does not surface the response to
 * the facade caller.
 */
MOQ_API moq_result_t moq_sub_update_subscription(
    moq_subscriber_t *sub,
    moq_sub_track_t *track,
    const moq_sub_update_cfg_t *cfg,
    uint64_t now_us);

/* -- Object poll -------------------------------------------------- */

typedef struct moq_sub_object {
    moq_sub_track_t    *track;
    uint64_t            group_id;
    uint64_t            subgroup_id;
    uint64_t            object_id;
    uint8_t             publisher_priority;
    moq_object_status_t status;
    bool                end_of_group;
    bool                datagram;
    moq_rcbuf_t        *payload;
    moq_rcbuf_t        *properties;
} moq_sub_object_t;

/*
 * Poll the next received object. Returns MOQ_OK and fills out.
 * Returns MOQ_DONE when no objects are queued.
 * Caller must call moq_sub_object_cleanup after processing.
 */
MOQ_API moq_result_t moq_sub_poll_object(
    moq_subscriber_t *sub,
    moq_sub_object_t *out);

MOQ_API void moq_sub_object_cleanup(moq_sub_object_t *obj);

/* -- Chunk poll (streaming mode) ---------------------------------- */

typedef struct moq_sub_chunk {
    moq_sub_track_t       *track;
    uint64_t               group_id;
    uint64_t               subgroup_id;
    uint64_t               object_id;
    uint64_t               payload_length;
    uint8_t                publisher_priority;
    moq_object_status_t    status;
    moq_object_terminal_t  terminal;
    bool                   begin;
    bool                   end;
    bool                   end_of_group;
    moq_rcbuf_t           *chunk;
    moq_rcbuf_t           *properties;
} moq_sub_chunk_t;

/*
 * Poll the next streaming chunk. Returns MOQ_OK and fills out.
 * Returns MOQ_DONE when no chunks are queued.
 * Only produces results when streaming_objects is true.
 * Caller must call moq_sub_chunk_cleanup after processing.
 */
MOQ_API moq_result_t moq_sub_poll_chunk(
    moq_subscriber_t *sub,
    moq_sub_chunk_t *out);

MOQ_API void moq_sub_chunk_cleanup(moq_sub_chunk_t *chunk);

/* -- Track status ------------------------------------------------- */

typedef struct moq_sub_status_req moq_sub_status_req_t;

typedef struct moq_sub_status_cfg {
    uint32_t        struct_size;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_sub_status_cfg_t;

MOQ_API void moq_sub_status_cfg_init(moq_sub_status_cfg_t *cfg);

MOQ_API moq_result_t moq_sub_track_status(
    moq_subscriber_t *sub,
    const moq_sub_status_cfg_t *cfg,
    uint64_t now_us,
    moq_sub_status_req_t **out);

typedef enum moq_sub_status_result_kind {
    MOQ_SUB_STATUS_OK    = 1,
    MOQ_SUB_STATUS_ERROR = 2,
} moq_sub_status_result_kind_t;

typedef struct moq_sub_status_result {
    moq_sub_status_result_kind_t kind;
    moq_sub_status_req_t        *request;
    bool                has_largest;
    uint64_t            largest_group;
    uint64_t            largest_object;
    bool                has_expires;
    uint64_t            expires_ms;
    moq_request_error_t error_code;
    bool                can_retry;
    uint64_t            retry_after_ms;
} moq_sub_status_result_t;

/*
 * Poll the next track status result. Returns MOQ_OK and fills out.
 * Returns MOQ_DONE when no results are queued.
 *
 * result.request is an opaque correlation token, not a live object.
 * The slot is released by this call and may be reused by a later
 * moq_sub_track_status call. Compare pointer values for correlation
 * but do not dereference or retain as live state.
 */
MOQ_API moq_result_t moq_sub_poll_status(
    moq_subscriber_t *sub,
    moq_sub_status_result_t *out);

/* -- Fetch -------------------------------------------------------- */

typedef struct moq_sub_fetch_req moq_sub_fetch_req_t;

typedef struct moq_sub_fetch_cfg {
    uint32_t               struct_size;
    moq_namespace_t        track_namespace;
    moq_bytes_t            track_name;
    uint64_t               start_group;
    uint64_t               start_object;
    uint64_t               end_group;
    uint64_t               end_object;
    moq_group_order_t      group_order;
    bool                   has_subscriber_priority;
    uint8_t                subscriber_priority;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_sub_fetch_cfg_t;

MOQ_API void moq_sub_fetch_cfg_init(moq_sub_fetch_cfg_t *cfg);

MOQ_API moq_result_t moq_sub_fetch(
    moq_subscriber_t *sub,
    const moq_sub_fetch_cfg_t *cfg,
    uint64_t now_us,
    moq_sub_fetch_req_t **out);

/* -- Joining fetch ------------------------------------------------ */

typedef struct moq_sub_joining_fetch_cfg {
    uint32_t          struct_size;
    moq_sub_track_t  *track;
    bool              relative;
    uint64_t          joining_start;
    moq_group_order_t group_order;
    bool              has_subscriber_priority;
    uint8_t           subscriber_priority;
} moq_sub_joining_fetch_cfg_t;

MOQ_API void moq_sub_joining_fetch_cfg_init(moq_sub_joining_fetch_cfg_t *cfg);

MOQ_API moq_result_t moq_sub_joining_fetch(
    moq_subscriber_t *sub,
    const moq_sub_joining_fetch_cfg_t *cfg,
    uint64_t now_us,
    moq_sub_fetch_req_t **out);

/*
 * Cancel a pending or active fetch. On success the request slot is
 * freed immediately — req is invalid after this call and may be
 * reused by a later moq_sub_fetch. Do not dereference or retain.
 */
MOQ_API moq_result_t moq_sub_cancel_fetch(
    moq_subscriber_t *sub,
    moq_sub_fetch_req_t *req,
    uint64_t now_us);

typedef enum moq_sub_fetch_item_kind {
    MOQ_SUB_FETCH_OK       = 1,
    MOQ_SUB_FETCH_ERROR    = 2,
    MOQ_SUB_FETCH_OBJECT   = 3,
    MOQ_SUB_FETCH_GAP      = 4,
    MOQ_SUB_FETCH_COMPLETE = 5,
} moq_sub_fetch_item_kind_t;

typedef struct moq_sub_fetch_item {
    moq_sub_fetch_item_kind_t kind;
    moq_sub_fetch_req_t      *request;
    union {
        struct {
            bool        end_of_track;
            uint64_t    end_group;
            uint64_t    end_object;
        } ok;
        struct {
            moq_request_error_t error_code;
            bool                can_retry;
            uint64_t            retry_after_ms;
        } error;
        struct {
            uint64_t    group_id;
            uint64_t    object_id;
            uint8_t     publisher_priority;
            moq_rcbuf_t *payload;
            moq_rcbuf_t *properties;
        } object;
        struct {
            moq_fetch_range_kind_t range_kind;
            uint64_t               group_id;
            uint64_t               object_id;
        } gap;
    } u;
} moq_sub_fetch_item_t;

/*
 * Poll the next fetch item. Returns MOQ_OK and fills out.
 * Returns MOQ_DONE when no items are queued.
 * Caller must call moq_sub_fetch_item_cleanup after processing.
 *
 * For terminal items (ERROR, COMPLETE), request is an opaque
 * correlation token. The slot is released when the terminal item
 * is polled and may be reused by a later moq_sub_fetch call.
 * Do not dereference or retain as live state.
 */
MOQ_API moq_result_t moq_sub_poll_fetch(
    moq_subscriber_t *sub,
    moq_sub_fetch_item_t *out);

MOQ_API void moq_sub_fetch_item_cleanup(moq_sub_fetch_item_t *item);

/* -- Query -------------------------------------------------------- */

typedef enum moq_sub_track_state {
    MOQ_SUB_TRACK_PENDING = 1,
    MOQ_SUB_TRACK_ACTIVE  = 2,
    MOQ_SUB_TRACK_ERROR   = 3,
    MOQ_SUB_TRACK_DONE    = 4,
} moq_sub_track_state_t;

MOQ_API moq_sub_track_state_t moq_sub_track_get_state(
    const moq_sub_track_t *track);

MOQ_API bool moq_sub_track_is_active(const moq_sub_track_t *track);

/*
 * Returns true and writes the error code if the track is in ERROR state.
 * Returns false if the track is not in ERROR state.
 */
MOQ_API bool moq_sub_track_get_error(
    const moq_sub_track_t *track,
    moq_request_error_t *out_code);

MOQ_API bool moq_sub_is_draining(const moq_subscriber_t *sub);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_SUBSCRIBER_H */
