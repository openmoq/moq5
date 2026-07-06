#ifndef MOQ_PUBLISHER_H
#define MOQ_PUBLISHER_H

#include "session.h"

#ifdef __cplusplus
extern "C" {
#endif

/* -- Opaque types ------------------------------------------------- */

typedef struct moq_publisher moq_publisher_t;
typedef struct moq_pub_track moq_pub_track_t;
typedef struct moq_pub_deferred moq_pub_deferred_t;

/* -- Accept policy ------------------------------------------------ */

typedef enum moq_pub_accept_mode {
    MOQ_PUB_REJECT_ALL = 0,
    MOQ_PUB_ACCEPT_ALL = 1,
    MOQ_PUB_CALLBACK   = 2,
} moq_pub_accept_mode_t;

typedef enum moq_pub_accept_decision {
    MOQ_PUB_DECISION_ACCEPT = 0,
    MOQ_PUB_DECISION_REJECT = 1,
    MOQ_PUB_DECISION_DEFER  = 2,
} moq_pub_accept_decision_t;

typedef struct moq_pub_subscribe_info {
    moq_pub_track_t            *track;
    moq_namespace_t             track_namespace;  /* BORROWED, callback-duration only */
    moq_bytes_t                 track_name;       /* BORROWED, callback-duration only */
    moq_subscribe_filter_t      filter;
    uint8_t                     subscriber_priority;
    moq_group_order_t           group_order;
    bool                        forward;
    uint64_t                    start_group;
    uint64_t                    start_object;
    uint64_t                    end_group;
    uint64_t                    delivery_timeout_us;
    const moq_resolved_token_t *tokens;           /* BORROWED, callback-duration only */
    size_t                      token_count;
    moq_pub_deferred_t         *deferred;         /* opaque handle; NULL if defer slot occupied */
    uint64_t                    deferred_id;     /* pass back to resolve_deferred */
} moq_pub_subscribe_info_t;

/*
 * Subscribe authorization callback. Return ACCEPT, REJECT, or DEFER.
 * DEFER is only valid when info->deferred is non-NULL; if the defer
 * slot is occupied by a prior unresolved request, deferred is NULL
 * and the callback must return ACCEPT or REJECT.
 */
typedef moq_pub_accept_decision_t (*moq_pub_subscribe_cb)(
    void *ctx,
    const moq_pub_subscribe_info_t *info,
    moq_request_error_t *out_error_code);

/* -- Subscription update info ------------------------------------- */

typedef struct moq_pub_subscribe_update_info {
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_forward;
    bool     forward;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_us;
} moq_pub_subscribe_update_info_t;

/* -- Callbacks ---------------------------------------------------- */

/*
 * Publisher callbacks are non-reentrant: do not call moq_pub_*,
 * moq_session_*, or moq_pq_* mutation APIs from within a callback.
 * The session and publisher are mid-advance when callbacks fire;
 * reentrant calls may corrupt state or deadlock.
 *
 * Recommended pattern: record state changes (e.g. subscriber joined)
 * in the callback, then publish objects or update configuration from
 * the application's normal loop after the callback returns.
 */

typedef struct moq_pub_callbacks {
    uint32_t struct_size;
    void    *ctx;
    void (*on_subscriber_joined)(void *ctx, moq_pub_track_t *track);
    void (*on_subscriber_left)(void *ctx, moq_pub_track_t *track);
    void (*on_draining)(void *ctx);
    void (*on_closed)(void *ctx, uint64_t error_code);
    /* Appended: fired when a subscriber updates priority/forward/timeout. */
    void (*on_subscriber_updated)(void *ctx, moq_pub_track_t *track,
                                   const moq_pub_subscribe_update_info_t *info);
    void (*on_publish_ok)(void *ctx, moq_pub_track_t *track, bool forward);
    void (*on_publish_error)(void *ctx, moq_pub_track_t *track,
                              moq_request_error_t error_code);
    void (*on_publish_forward_changed)(void *ctx, moq_pub_track_t *track,
                                        bool forward);
    void (*on_publish_finished)(void *ctx, moq_pub_track_t *track);
} moq_pub_callbacks_t;

MOQ_API void moq_pub_callbacks_init(moq_pub_callbacks_t *cb);

/* -- Configuration ------------------------------------------------ */

typedef struct moq_pub_cfg {
    uint32_t              struct_size;
    moq_pub_accept_mode_t accept_mode;
    uint8_t               default_publisher_priority;
    moq_pub_subscribe_cb  on_subscribe;
    void                 *on_subscribe_ctx;
    moq_pub_callbacks_t   callbacks;
} moq_pub_cfg_t;

/* Pointer-only initializer. Clears and stamps ONLY the frozen prefix (the
 * original layout: struct_size, accept_mode, default_publisher_priority). It
 * cannot know the caller's storage size, so it must not write the full current
 * sizeof -- that would overflow a caller compiled against the original (smaller)
 * struct. The appended fields (on_subscribe, on_subscribe_ctx, callbacks)
 * default to disabled; to set any of them, or to initialize the full current
 * struct, use moq_pub_cfg_init_sized(). */
MOQ_API void moq_pub_cfg_init(moq_pub_cfg_t *cfg);

/* Size-aware initializer. Clears and stamps min(cfg_size, sizeof current
 * struct): an older caller passes its smaller sizeof (prefix init), a newer
 * caller's extra trailing fields are left to its own initializer. Pass
 * sizeof(moq_pub_cfg_t) to initialize the full current struct with all appended
 * fields enabled. No-op if cfg is NULL or cfg_size is too small to hold
 * struct_size. */
MOQ_API void moq_pub_cfg_init_sized(moq_pub_cfg_t *cfg, size_t cfg_size);

/* -- Publisher lifecycle ------------------------------------------ */

MOQ_API moq_result_t moq_pub_create(
    moq_session_t *session,
    const moq_alloc_t *alloc,
    const moq_pub_cfg_t *cfg,
    moq_publisher_t **out);

MOQ_API void moq_pub_destroy(moq_publisher_t *pub);

/*
 * Process session events and retry pending operations.
 * Polls all session events internally: dispatches subscribe requests,
 * unsubscribe, namespace lifecycle, goaway, and session closed.
 * Fires callbacks synchronously. Retries pending accept/reject.
 *
 * The caller must NOT also call moq_session_poll_events on the same
 * session when using tick.
 */
MOQ_API moq_result_t moq_pub_tick(moq_publisher_t *pub, uint64_t now_us);

MOQ_API bool moq_pub_is_draining(const moq_publisher_t *pub);

/* -- Track management --------------------------------------------- */

typedef struct moq_pub_track_cfg {
    uint32_t        struct_size;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    bool            advertise_namespace;
    uint8_t         publisher_priority;
    uint64_t        max_retained_bytes; /* retained-group byte budget; 0 = default (1 MiB) */
    /* Appended AFTER max_retained_bytes, beyond the original frozen layout.
     * It must not live in the original struct's trailing padding: an old
     * caller's uninitialised padding byte there would be misread as an
     * explicit priority. Set it (and publisher_priority) only when the cfg
     * was initialised with moq_pub_track_cfg_init_sized(); the pointer-only
     * moq_pub_track_cfg_init() stamps the frozen prefix, which excludes this
     * field, so it is ignored. */
    bool            has_publisher_priority;
} moq_pub_track_cfg_t;

/* Pointer-only initializer: zeroes and stamps ONLY the frozen original
 * prefix (struct_size .. publisher_priority, i.e. before the appended
 * max_retained_bytes and has_publisher_priority). Both appended fields stay
 * disabled — callers that want an explicit priority or retained-byte budget
 * must use moq_pub_track_cfg_init_sized(). Mirrors moq_pub_cfg_init(). */
MOQ_API void moq_pub_track_cfg_init(moq_pub_track_cfg_t *cfg);

/* Size-aware initializer: zeroes and stamps min(cfg_size, sizeof) so all
 * appended fields the caller's struct covers are active. Pass
 * sizeof(moq_pub_track_cfg_t). No-op if cfg is NULL or cfg_size cannot hold
 * struct_size. Use this whenever you set has_publisher_priority. */
MOQ_API void moq_pub_track_cfg_init_sized(moq_pub_track_cfg_t *cfg,
                                          size_t cfg_size);

MOQ_API moq_result_t moq_pub_add_track(
    moq_publisher_t *pub,
    const moq_pub_track_cfg_t *cfg,
    uint64_t now_us,
    moq_pub_track_t **out);

MOQ_API moq_result_t moq_pub_remove_track(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t now_us);

/*
 * Configuration for publishing a track. Publishing is a per-track OPERATION,
 * not a publisher mode: a track added with moq_pub_add_track may be advertised
 * (advertise_namespace) and receive SUBSCRIBE, AND/OR be published with
 * moq_pub_publish_track.
 */
typedef struct moq_pub_publish_cfg {
    uint32_t    struct_size;
    bool        has_track_alias;
    uint64_t    track_alias;        /* has_track_alias==false: session assigns */
    bool        has_forward;
    bool        forward;            /* initial forward intent (default true) */
    moq_bytes_t track_properties;   /* opaque track properties (e.g. DYNAMIC_GROUPS) */
    const moq_auth_token_t *auth_tokens;   /* borrowed for the call */
    size_t                  auth_token_count;
} moq_pub_publish_cfg_t;

MOQ_API void moq_pub_publish_cfg_init(moq_pub_publish_cfg_t *cfg);

/*
 * Send PUBLISH for an existing track (publisher-initiated). The track's
 * namespace and name from moq_pub_add_track are used.
 *
 * Advancing call. Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity and
 * MOQ_ERR_WOULD_BLOCK if the action queue is full (retry via moq_pub_tick /
 * moq_pub_flush). Idempotent: a second call while the publication is live
 * returns MOQ_OK. Returns MOQ_ERR_WRONG_STATE if the track is ended,
 * MOQ_ERR_CLOSED if the publisher is closed.
 *
 * Objects are written with the same moq_pub_write_object[_ex] API -- the
 * facade routes them to the publication once it is established. Acceptance is
 * surfaced via moq_pub_track_is_published / on_publish_ok.
 */
MOQ_API moq_result_t moq_pub_publish_track(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    const moq_pub_publish_cfg_t *cfg,
    uint64_t now_us);

/* Inverse of moq_pub_publish_track: end the publication cleanly (PUBLISH_DONE),
 * leaving the track and its subscriptions intact. No-op MOQ_OK if never
 * published. WOULD_BLOCK (retry via tick/flush) / WRONG_STATE (object
 * mid-stream) / CLOSED. */
MOQ_API moq_result_t moq_pub_unpublish_track(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t now_us);

/* True once the peer has accepted this track's publication. False for NULL
 * inputs, a foreign track, a track never published, or while the publication
 * is still pending / errored / finished. */
MOQ_API bool moq_pub_track_is_published(
    const moq_publisher_t *pub,
    const moq_pub_track_t *track);

/* True when the publication is established AND the peer's forward state is 1. 
 * Objects written while this is false are still accepted by the facade*/
MOQ_API bool moq_pub_track_forward(
    const moq_publisher_t *pub,
    const moq_pub_track_t *track);

/* -- Retained group (origin-local explicit-FETCH cache) ------------- */

/*
 * Retain a GROUP of objects for a track so the publisher can answer an EXPLICIT
 * FETCH for them: an independent object 0 plus the deltaUpdate objects 1..N of
 * the latest catalog group (MSF-01 §5). Setting replaces any previous retained
 * set atomically; the publisher increfs each payload/properties.
 *
 * Scope: this is an ORIGIN-LOCAL CACHE for an explicit FETCH of the retained
 * group — both a Joining FETCH(offset 0) (matched by its joining subscription)
 * and a bounded standalone FETCH (matched by explicit namespace/name, the shape
 * a relay such as moqx emits to pull a catalog) are served, PROVIDED the FETCH
 * range covers object 0 through the last retained object (and, for a standalone
 * FETCH, the request is authorized — see the authorization note below). It is
 * NOT pushed to
 * plain SUBSCRIBE joiners (a subscriber that wants the retained group MUST issue
 * a FETCH for it), it is NOT a relay-safe catalog solution — a late subscriber
 * behind a relay is not served by this (the origin never sees that joiner) — and
 * it is NOT a general object store: only the latest retained dense group is held,
 * and only a range covering the whole group is answered. It just lets an origin
 * answer a FETCH it receives directly while it still holds the objects.
 *
 * The retained group is the cache an explicit FETCH is answered from (objects
 * 0..N, End Location = last object_id + 1). Only the LATEST group is retained —
 * setting a new group drops the old (the receiver ignores any group below the
 * latest). Setting the retained group advertises the last object as the
 * subscription's Largest Location so the joiner issues the correct FETCH. A
 * FETCH whose range omits object 0 (or any retained object) is rejected
 * NOT_SUPPORTED; a FETCH for an unknown/ended/non-retained track is rejected
 * DOES_NOT_EXIST.
 *
 * Authorization (standalone FETCH only): because a standalone FETCH names the
 * track directly, it is served only when the publisher's accept_mode is
 * MOQ_PUB_ACCEPT_ALL, or the track already has an accepted subscription on this
 * session (e.g. a relay that accepted a SUBSCRIBE via callback and then pulls
 * the retained catalog). Otherwise it is rejected UNAUTHORIZED — and protected
 * tracks return UNAUTHORIZED whether or not they exist, so their existence is
 * not leaked. A Joining FETCH carries a joining subscription (itself proof of an
 * accepted subscription) and is not gated. This check never invokes on_subscribe.
 *
 * Objects MUST form a dense catalog group: objects[i].object_id == i (objects[0]
 * the independent base, no gaps), so a FETCH reconstructs the catalog from
 * object 0 through the last delta. object_count >= 1; each payload non-NULL. The
 * retained set is bounded: at most MOQ_PUB_RETAINED_MAX_OBJECTS objects and a
 * total payload+properties byte budget (the track's max_retained_bytes) —
 * exceeding either is MOQ_ERR_INVAL and leaves the prior retained set intact.
 *
 * Returns MOQ_OK, MOQ_ERR_INVAL (NULL/foreign, bad struct_size, empty/oversize/
 * out-of-order objects, NULL payload), MOQ_ERR_WRONG_STATE (track ended), or
 * MOQ_ERR_NOMEM.
 */
#define MOQ_PUB_RETAINED_MAX_OBJECTS 64u

typedef struct moq_pub_retained_object {
    uint64_t     object_id;
    moq_rcbuf_t *payload;       /* retained via incref; required */
    moq_rcbuf_t *properties;    /* retained via incref; NULL OK */
    bool         end_of_group;  /* sets END_OF_GROUP on the last object */
} moq_pub_retained_object_t;

typedef struct moq_pub_retained_group_cfg {
    uint32_t struct_size;
    uint64_t group_id;                          /* shared by all objects */
    const moq_pub_retained_object_t *objects;   /* ascending object_id, 0..N */
    size_t   object_count;
} moq_pub_retained_group_cfg_t;

MOQ_API void moq_pub_retained_group_cfg_init(
    moq_pub_retained_group_cfg_t *cfg);

MOQ_API moq_result_t moq_pub_set_retained_group(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    const moq_pub_retained_group_cfg_t *cfg);

/* Clear the retained group, releasing its retained refs. */
MOQ_API moq_result_t moq_pub_clear_retained_group(
    moq_publisher_t *pub,
    moq_pub_track_t *track);

/* -- Object writing ----------------------------------------------- */

/*
 * In moq_pub_write_object_ex() below, a status object (has_status) requires
 * datagram=true; arbitrary non-datagram status objects are not supported on
 * this cfg path. The reliable, stream-based terminal END_OF_TRACK is exposed
 * separately as moq_pub_end_track() (no datagram dependency).
 */
typedef struct moq_pub_object_cfg {
    uint32_t            struct_size;
    uint64_t            group_id;
    uint64_t            object_id;
    moq_rcbuf_t        *payload;        /* required for non-status; NULL for status */
    moq_rcbuf_t        *properties;     /* NULL if none; stream or datagram */
    bool                datagram;
    bool                has_status;      /* datagram only */
    moq_object_status_t status;         /* NORMAL, END_OF_GROUP, or END_OF_TRACK */
    uint8_t             _reserved_obj[5];
    bool                end_of_group;   /* stream: set END_OF_GROUP in subgroup header */
} moq_pub_object_cfg_t;

MOQ_API void moq_pub_object_cfg_init(moq_pub_object_cfg_t *cfg);

MOQ_API moq_result_t moq_pub_write_object_ex(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    const moq_pub_object_cfg_t *obj,
    uint64_t now_us);

MOQ_API moq_result_t moq_pub_write_object(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t group_id,
    uint64_t object_id,
    moq_rcbuf_t *payload,
    uint64_t now_us);

/* End a track reliably: emit a terminal END_OF_TRACK status object on a fresh
 * subgroup (NOT a datagram), then close it. Subscribers surface the track
 * end without the session closing. When the track has no active subscriber the
 * track is still marked ended locally (MOQ_OK). Single active slot, like the
 * write fan-out (v0 limit).
 *
 * After end_track succeeds the track is terminal: moq_pub_write_object[_ex],
 * moq_pub_begin_object, and moq_pub_set_retained_group all return
 * MOQ_ERR_WRONG_STATE, and a repeated end_track is an idempotent MOQ_OK.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full (retry; the track is
 * NOT marked ended), or MOQ_ERR_WRONG_STATE if an object is mid-stream (finish
 * it first, then retry). */
MOQ_API moq_result_t moq_pub_end_track(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t now_us);

/* Subscription-done status code "Track Ended" (MSF section 11.3 step 1). Tells
 * active subscribers the live phase of the track has ended without terminating
 * the track itself -- distinct from the terminal end emitted by
 * moq_pub_end_track(). */
#define MOQ_PUB_DONE_TRACK_ENDED 0x2u

/* Finish every active subscriber of `track` by completing each subscription with
 * `status_code` (typically MOQ_PUB_DONE_TRACK_ENDED), WITHOUT terminalizing the
 * track. This is the MSF section 11.3 "convert live to VOD" step-1 primitive:
 * live subscribers are told the track ended, but the track stays registered, its
 * retained group is preserved, and it remains joinable -- a later subscribe (then
 * an explicit Joining FETCH) can still pull the retained group.
 *
 * Contrast with moq_pub_end_track(), which is terminal: end_track emits a
 * terminal status object and marks the track ended so subsequent writes,
 * subscribes, and fetches are rejected. finish_subscribers does none of that --
 * it does NOT mark the track ended, does NOT remove the track, and does NOT
 * clear the retained group.
 *
 * For each active subscriber: any open live subgroup is closed first (completing
 * a subscription requires no open data stream), then the subscription is
 * completed and its state freed. The
 * reported stream count is the unknown sentinel (2^62 - 1) since the facade does
 * not track per-subscription stream counts; subscribers fall back to a timeout
 * per the transport spec.
 *
 * Idempotent and WOULD_BLOCK-safe: a finished subscriber is never completed
 * twice, and a retry after MOQ_ERR_WOULD_BLOCK resumes without duplicating or
 * skipping any subscriber. With no active subscribers this is a no-op MOQ_OK.
 *
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full (retry), or
 * MOQ_ERR_WRONG_STATE if a subscriber has an object mid-stream (finish it,
 * then retry). MOQ_ERR_CLOSED if the publisher is closed, MOQ_ERR_INVAL on
 * bad arguments. */
MOQ_API moq_result_t moq_pub_finish_subscribers(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t status_code,
    uint64_t now_us);

/* -- Object writing (streaming) ----------------------------------- */

typedef struct moq_pub_begin_object_cfg {
    uint32_t     struct_size;
    uint64_t     group_id;
    uint64_t     object_id;
    uint64_t     payload_length;
    moq_rcbuf_t *properties;  /* NULL if none; requires object_properties on subgroup */
} moq_pub_begin_object_cfg_t;

MOQ_API void moq_pub_begin_object_cfg_init(moq_pub_begin_object_cfg_t *cfg);

MOQ_API moq_result_t moq_pub_begin_object(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    const moq_pub_begin_object_cfg_t *cfg,
    uint64_t now_us);

MOQ_API moq_result_t moq_pub_write_data(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    moq_rcbuf_t *chunk,
    uint64_t now_us);

MOQ_API moq_result_t moq_pub_end_object(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t now_us);

/* -- Group end ---------------------------------------------------- */

MOQ_API moq_result_t moq_pub_end_group(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t now_us);

/* Abandon the track's currently-open group on the wire: RESET_STREAM the
 * open subgroup (across every active subscriber slot) with error_code,
 * rather than closing it cleanly. Use this to drop a partially-sent group
 * under backpressure -- a clean close would imply a complete group, and
 * just discarding local state would leave the subscriber a truncated
 * subgroup it cannot decode. The next write to the track opens a fresh
 * subgroup. No-op (MOQ_OK) when no group is open. MOQ_ERR_WOULD_BLOCK if
 * the reset action cannot be queued yet (retryable; slots already reset
 * are not reset again). */
MOQ_API moq_result_t moq_pub_reset_group(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t error_code,
    uint64_t now_us);

/* -- Event forwarding --------------------------------------------- */

typedef enum moq_pub_event_result {
    MOQ_PUB_EVENT_CONSUMED = 0,
    MOQ_PUB_EVENT_IGNORED  = 1,
    MOQ_PUB_EVENT_ERROR    = 2,
} moq_pub_event_result_t;

MOQ_API moq_result_t moq_pub_handle_event(
    moq_publisher_t *pub,
    const moq_event_t *event,
    uint64_t now_us,
    moq_pub_event_result_t *result);

/*
 * Retry pending accept/reject after caller drains session actions.
 * Returns MOQ_OK when no pending work or retry succeeded.
 * Returns MOQ_ERR_WOULD_BLOCK if still blocked — drain and retry.
 * Caller must resolve pending work (flush until OK or error)
 * before destroying the publisher.
 */
MOQ_API moq_result_t moq_pub_flush(
    moq_publisher_t *pub,
    uint64_t now_us);

/* -- Deferred authorization --------------------------------------- */

/*
 * Resolve a previously deferred subscribe decision.
 * accept=true sends SUBSCRIBE_OK; accept=false sends SUBSCRIBE_ERROR
 * with the given error_code.
 * Returns MOQ_ERR_WOULD_BLOCK if the session action queue is full;
 * moq_pub_flush/moq_pub_tick will retry.
 * Returns MOQ_ERR_STALE_HANDLE if the deferred handle is no longer
 * valid (session closed or track removed).
 */
MOQ_API moq_result_t moq_pub_resolve_deferred(
    moq_publisher_t *pub,
    moq_pub_deferred_t *deferred,
    uint64_t deferred_id,
    bool accept,
    moq_request_error_t error_code,
    uint64_t now_us);

/* -- Query -------------------------------------------------------- */

MOQ_API size_t moq_pub_active_subscriptions(
    const moq_publisher_t *pub,
    const moq_pub_track_t *track);

MOQ_API bool moq_pub_has_subscriber(
    const moq_publisher_t *pub,
    const moq_pub_track_t *track);

/* True once the peer has accepted the namespace advertisement for this
 * track -- i.e. the facade has consumed MOQ_EVENT_NAMESPACE_ACCEPTED for
 * the track's announcement. False for NULL inputs, a track not owned by
 * pub, a track added without advertise_namespace, and while the
 * advertisement is pending, rejected, or cancelled. Surfaces existing
 * state only; no behavior change. Lets a higher tier gate "publish ready"
 * on the relay actually accepting the namespace. */
MOQ_API bool moq_pub_namespace_accepted(
    const moq_publisher_t *pub,
    const moq_pub_track_t *track);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PUBLISHER_H */
