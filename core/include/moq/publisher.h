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

/* Why an advertised namespace reached a terminal state. A REJECTED
 * advertisement was refused by the peer at request time; a CANCELLED
 * advertisement was accepted and later withdrawn by the peer. */
typedef enum moq_pub_namespace_terminal_kind {
    MOQ_PUB_NAMESPACE_REJECTED  = 1,
    MOQ_PUB_NAMESPACE_CANCELLED = 2,
} moq_pub_namespace_terminal_kind_t;

/* Detail for on_namespace_terminal. Library-produced: struct_size is stamped
 * with this library's sizeof so a newer caller can gate any future field.
 * REJECTED carries error_code, can_retry, retry_after_ms, and reason;
 * CANCELLED carries error_code and reason with can_retry=false and
 * retry_after_ms=0. reason is BORROWED for the callback's duration only. */
typedef struct moq_pub_namespace_terminal_info {
    uint32_t                          struct_size;
    moq_pub_namespace_terminal_kind_t kind;
    /* The advertised namespace that reached the terminal state. This is a
     * NAMESPACE-level event (one advertisement can back several tracks), so
     * the identity is the namespace, not any one track. Its parts are
     * BORROWED for the callback's duration only. */
    moq_namespace_t                   namespace_;
    moq_request_error_t               error_code;
    bool                              can_retry;
    uint64_t                          retry_after_ms;
    moq_bytes_t                       reason;
} moq_pub_namespace_terminal_info_t;

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
    /* Appended: fired exactly once PER NAMESPACE when an advertised
     * namespace enters a terminal state (peer REJECTED the advertisement,
     * or peer CANCELLED a previously accepted one). Namespace-level: the
     * advertisement can back several tracks, so info->namespace_ carries
     * the identity rather than any one track. `info` and its borrowed
     * namespace/reason are valid only for the callback's duration.
     * Non-reentrant: do not call back into the publisher from here.
     * Uses ctx. */
    void (*on_namespace_terminal)(void *ctx,
                                  const moq_pub_namespace_terminal_info_t *info);
} moq_pub_callbacks_t;

/* Pointer-only initializer: clears and stamps ONLY the frozen v0 prefix
 * (through on_subscriber_updated). It cannot know the caller's storage size,
 * so it never touches the appended fields (on_publish_* and
 * on_namespace_terminal) -- they stay disabled
 * (struct_size == the v0 prefix). To SET any appended field, initialize with
 * moq_pub_callbacks_init_sized(cb, sizeof *cb) instead. */
MOQ_API void moq_pub_callbacks_init(moq_pub_callbacks_t *cb);

/* Sized initializer: clears and stamps min(cb_size, this library's struct
 * size). Pass sizeof(*cb) to enable every field your build knows about,
 * including the appended on_publish_* callbacks. Safe for a caller compiled
 * against an older, smaller header too. Enables the appended callbacks
 * (on_publish_* and on_namespace_terminal). */
MOQ_API void moq_pub_callbacks_init_sized(moq_pub_callbacks_t *cb,
                                          size_t cb_size);

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
 * Fires callbacks synchronously. Retries pending accept/reject, deferred
 * retained-FETCH serving, lazy subgroup retirements, and finite-window
 * completion work unlocked by moq_pub_declare_groups_complete_through.
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
    /* Permanently reserved, never read. The pre-monotonic struct ended in
     * seven bytes of trailing padding after has_publisher_priority; a field
     * placed there would sit INSIDE an old full-size caller's storage --
     * that caller's struct_size would appear to cover it and its
     * uninitialized padding would be misread as an explicit declaration.
     * This tail pushes the next append to the old sizeof boundary. */
    uint8_t         _reserved_track_tail[7];
    /* Immutable per-track declaration: this track's group ids are
     * non-decreasing for its whole lifetime, and a group is never written
     * again once a higher group has begun or once its End-of-Group evidence
     * is recorded. The facade enforces the promise (violating writes return
     * MOQ_ERR_WRONG_STATE before any state changes) and uses the evidence
     * to auto-complete finite subscription filters: a destination whose
     * filter End Group is provably complete is terminated with
     * SUBSCRIPTION_ENDED and the exact stream count, the same completion
     * flow as moq_pub_declare_groups_complete_through. Declaring it opts
     * the track out of honoring a later End-widening at or below the
     * completion watermark (subscribers fill gaps via FETCH). Completion is
     * staged work: evidence created by a write is acted on by
     * moq_pub_flush / moq_pub_tick, never inside the write itself --
     * manual-mode apps must keep flushing after evidence-producing writes.
     * Read only when struct_size covers the whole field: set it only with
     * moq_pub_track_cfg_init_sized(). */
    bool            monotonic_groups;
} moq_pub_track_cfg_t;

/* Pointer-only initializer: zeroes and stamps ONLY the frozen original
 * prefix (struct_size .. publisher_priority, i.e. before the appended
 * max_retained_bytes, has_publisher_priority, and monotonic_groups). All
 * appended fields stay disabled — callers that want an explicit priority, a
 * retained-byte budget, or the monotonic-groups declaration (default OFF)
 * must use moq_pub_track_cfg_init_sized(). Mirrors moq_pub_cfg_init(). */
MOQ_API void moq_pub_track_cfg_init(moq_pub_track_cfg_t *cfg);

/* Size-aware initializer: zeroes and stamps min(cfg_size, sizeof) so all
 * appended fields the caller's struct covers are active. Pass
 * sizeof(moq_pub_track_cfg_t). No-op if cfg is NULL or cfg_size cannot hold
 * struct_size. Use this whenever you set has_publisher_priority or
 * monotonic_groups (both default off; monotonic_groups is read only when
 * struct_size covers the whole field). */
MOQ_API void moq_pub_track_cfg_init_sized(moq_pub_track_cfg_t *cfg,
                                          size_t cfg_size);

MOQ_API moq_result_t moq_pub_add_track(
    moq_publisher_t *pub,
    const moq_pub_track_cfg_t *cfg,
    uint64_t now_us,
    moq_pub_track_t **out);

/* Remove a track, tearing down every destination. This is the ABANDONMENT
 * path: it releases any pending write operation. The publication's open
 * subgroup is RESET when abandoning would omit an admitted-but-unsent object
 * or a Forward/filter cut already armed a reset (an armed reset is never
 * converted into a clean close, and both cases proceed even mid-stream);
 * otherwise it is closed cleanly (FIN). Subscriptions are completed with the
 * track-ended status. Every terminal carries the EXACT count of streams the
 * facade opened for that destination, whether it ended by FIN or RESET
 * (identifiable resets satisfy the peer's completed-stream gate like FINs).
 * A CLEAN mid-stream object (nothing armed, nothing omitted) refuses with
 * MOQ_ERR_WRONG_STATE -- finish it or abandon via moq_pub_reset_group first.
 * Retryable: WOULD_BLOCK at any step resumes without re-sending or skipping
 * (a queued RESET clears the mid-stream state, so a later blocked step never
 * wedges the retry). */
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
 * MOQ_ERR_WOULD_BLOCK if the action queue is full -- nothing is staged: drain
 * session actions and retry moq_pub_publish_track() itself with the same
 * arguments (tick/flush do not resume it). Idempotent: a second call while the publication is live
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

/* Inverse of moq_pub_publish_track: end the publication cleanly, leaving the
 * track and its subscriptions intact. No-op MOQ_OK if never published.
 *
 * Clean-end contract: an admitted-but-unsent object for the publication is
 * REFUSED with MOQ_ERR_WRONG_STATE and no mutation -- complete the write or
 * abandon explicitly (moq_pub_reset_group / moq_pub_remove_track) first;
 * truncated work is never closed as finished. An armed reset (Forward/filter
 * cut) PROGRESSES first, even mid-stream: the affected subgroup is RESET --
 * never FIN'd -- before the clean close/finish, so only a CLEAN mid-stream
 * object still returns WRONG_STATE. The finish reports the EXACT count of
 * streams the facade opened for the publication (a reset stream counts at
 * the peer like a completed one).
 * On MOQ_ERR_WOULD_BLOCK the operation is INCOMPLETE (publish_requested
 * remains set): drain session actions and retry moq_pub_unpublish_track()
 * itself with the same arguments. moq_pub_tick / moq_pub_flush may progress
 * an armed retirement, but they do NOT resume the close or the finish -- only
 * the retry completes the unpublish.
 * WOULD_BLOCK (as above) / WRONG_STATE (as above) / CLOSED. */
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

/* Pointer-only initializer: clears/stamps ONLY the frozen v0 prefix (through
 * `status`); the appended end_of_group stays disabled. Use the _sized form to
 * set it. */
MOQ_API void moq_pub_object_cfg_init(moq_pub_object_cfg_t *cfg);
MOQ_API void moq_pub_object_cfg_init_sized(moq_pub_object_cfg_t *cfg,
                                           size_t cfg_size);

/* Fan-out ownership and retry contract (write_object[_ex], begin_object,
 * write_data, end_object, end_group, end_track):
 *
 * Writes fan out to every eligible destination: each accepted subscription
 * with Forward 1 -- and, when the subscriber negotiated a subscription
 * filter, whose RESOLVED WINDOW admits the object's location (at or after
 * the window start, and within the end group when one is set; drafts 16/18
 * §5.1.2 forbid sending outside the requested range) -- plus an established
 * publication with Forward 1 under the same window rule. A fully filtered
 * write still succeeds, delivers nothing, and advances track history. On
 * MOQ_ERR_WOULD_BLOCK the operation is PARTIAL: the facade RETAINS the
 * payload/properties (rcbuf references) until the operation completes or is
 * abandoned, and the caller MUST retry the same call. A retry may pass a
 * DIFFERENT buffer with byte-identical content (re-encoding per attempt is
 * fine); a retry whose wire-visible identity or bytes DIFFER from the pending
 * operation returns MOQ_ERR_WRONG_STATE without disturbing it -- an unsent
 * write_data chunk is never silently skipped. Destinations (including their
 * window decision) are snapshotted when the operation starts: a destination
 * appearing, returning to Forward 1, or gaining a wider window mid-operation
 * joins the NEXT operation (for a streaming bracket, the next begin_object). Retained buffers are
 * released exactly once -- on completion, moq_pub_reset_group (the abandon
 * path), moq_pub_remove_track, session close, or moq_pub_destroy -- so a
 * wrapped buffer's release callback may fire later than the caller's own
 * decref, within normal refcount semantics. */
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

/* End a track reliably. When a fresh terminal Location is representable, emit
 * a terminal END_OF_TRACK status object on a fresh subgroup (NOT a datagram),
 * then close it; subscribers surface the track end without the session
 * closing. When the track has no active subscriber the track is still marked
 * ended locally (MOQ_OK). Fans out over every destination slot. The terminal
 * Location is TRACK-WIDE -- {last published group + 1, 0} from track history,
 * retained across retries and merged into history exactly once -- never a
 * per-destination cursor. CEILING FALLBACK: when no fresh terminal Location
 * is representable through the data plane (history's last group is at or
 * beyond the 2^62-1 group-id cap, where +1 is unencodable -- or, under
 * draft-18 history, would wrap past UINT64_MAX), NO status object is emitted
 * for ANY destination and history is left UNCHANGED; streams are closed and
 * every subscription is terminated with a done carrying TRACK_ENDED instead,
 * with the chosen mode preserved across WOULD_BLOCK retries. The synthetic
 * status is an Object and the drafts make no status-object exception to the
 * range rule, so a subscription whose resolved window excludes the terminal
 * Location receives no status object; and a Forward-0 subscription cannot
 * receive it either -- but BOTH still get the terminal control message the
 * drafts require regardless of Forward state: any open subgroup is closed
 * first (streams close before the done), then the subscription is ended
 * with SUBSCRIPTION_ENDED when its finite filter end was genuinely passed,
 * TRACK_ENDED otherwise, carrying the EXACT count of streams the facade
 * opened for it -- retryably and idempotently across WOULD_BLOCK.
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
 * terminal status object (when a fresh terminal Location is representable --
 * see its ceiling fallback) and marks the track ended so subsequent writes,
 * subscribes, and fetches are rejected. finish_subscribers does none of that --
 * it does NOT mark the track ended, does NOT remove the track, and does NOT
 * clear the retained group.
 *
 * For each active subscriber: any open live subgroup is closed first (completing
 * a subscription requires no open data stream), then the subscription is
 * completed and its state freed. The reported stream count is EXACT -- the
 * facade opened every stream for the subscription itself, so a datagram-only
 * or never-delivered subscriber reports zero and a peer never waits on streams
 * that will not arrive.
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

/* Pointer-only initializer: clears/stamps ONLY the frozen v0 prefix (through
 * payload_length); the appended properties field stays disabled. Use the
 * _sized form to set it. */
MOQ_API void moq_pub_begin_object_cfg_init(moq_pub_begin_object_cfg_t *cfg);
MOQ_API void moq_pub_begin_object_cfg_init_sized(
    moq_pub_begin_object_cfg_t *cfg, size_t cfg_size);

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

/*
 * §10: declare that no object with Group ID <= `group` will ever be
 * published on this track again. Idempotent and monotone: repeated
 * declarations max-merge; a lower or equal `group` merges nothing and skips
 * the pending-work check (those groups are already sealed) but STILL runs
 * the completion sweep and returns its result -- redeclaration (any value at
 * or below the committed watermark, including the same one) is the
 * documented resume path after MOQ_ERR_WOULD_BLOCK. Never a rollback.
 *
 * Enforcement: after a successful declaration, any write (one-shot,
 * datagram, status, or streaming begin) whose Group ID <= the declared
 * watermark is rejected with MOQ_ERR_WRONG_STATE before any state mutation.
 *
 * Completion: every destination whose subscription/publication filter has a
 * finite End Group <= the watermark terminates -- its open subgroup is
 * closed cleanly first, then the terminal done is sent with status
 * SUBSCRIPTION_ENDED (0x3) and the exact per-destination stream count.
 * This includes destinations still installed after moq_pub_end_track (the
 * terminal-status recipients keep their slots). moq_pub_end_track honors the
 * watermark: its terminal Location is chosen strictly above both the
 * published history and the declared watermark (degrading to its no-status
 * mode at the numeric ceiling), and a raising declaration that would cover
 * a blocked end_track's pending terminal is refused WRONG_STATE.
 *
 * Returns:
 *   MOQ_ERR_INVAL        NULL args / track not owned by pub
 *   MOQ_ERR_CLOSED       publisher closed
 *   MOQ_ERR_WRONG_STATE  the declaration would cover an object currently
 *                        admitted but not fully sent: a pending object
 *                        operation with group <= `group`, or an open
 *                        streaming bracket whose group <= `group`. The
 *                        watermark is NOT set (zero mutation); finish or
 *                        abandon (moq_pub_reset_group) it, then redeclare.
 *   MOQ_OK               watermark merged; all covered destinations done.
 *   MOQ_ERR_WOULD_BLOCK  the watermark is COMMITTED (never rolled back),
 *                        but completion work hit a full action queue.
 *                        Drain session actions, then redeclare (idempotent)
 *                        or call moq_pub_flush / moq_pub_tick -- the sweep
 *                        resumes exactly where it stopped.
 */
MOQ_API moq_result_t moq_pub_declare_groups_complete_through(
    moq_publisher_t *pub,
    moq_pub_track_t *track,
    uint64_t group,
    uint64_t now_us);

/* Abandon the track's currently-open group on the wire: RESET_STREAM the
 * open subgroup (across every active destination slot -- subscriptions
 * and the publication alike) with error_code,
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

/*
 * Result of handing one session event to the publisher facade. A caller that
 * multiplexes one moq_session_t across several facades uses this to decide
 * whether an event still needs forwarding to the others.
 *
 *   MOQ_PUB_EVENT_CONSUMED  a facade-exclusive event matched one of this
 *                           publisher's tracks/slots and was taken by this
 *                           publisher (subscribe request, subscribe/publish
 *                           update, publish ok/error/unsubscribed, unsubscribed,
 *                           namespace lifecycle for an advertised track). Fully
 *                           handled on a MOQ_OK return; with WOULD_BLOCK the
 *                           event's owned state is staged but not yet flushed.
 *   MOQ_PUB_EVENT_IGNORED   the event did not match this publisher (or is not
 *                           a publisher-facing event, e.g. PUBLISH_FINISHED,
 *                           which the session emits only to the subscriber
 *                           role). ALSO returned for shared session
 *                           BROADCASTS -- SESSION_CLOSED and GOAWAY -- which
 *                           the facade observes locally (tears down state /
 *                           fires on_closed / on_draining) yet leaves IGNORED
 *                           so a multiplexing caller keeps forwarding them.
 *   MOQ_PUB_EVENT_ERROR     handling failed (paired with a negative return).
 */
typedef enum moq_pub_event_result {
    MOQ_PUB_EVENT_CONSUMED = 0,
    MOQ_PUB_EVENT_IGNORED  = 1,
    MOQ_PUB_EVENT_ERROR    = 2,
} moq_pub_event_result_t;

/*
 * Feed one polled session event to the publisher facade. This is the manual
 * event-forwarding entry point; it runs the SAME event state machine as
 * moq_pub_tick, so a consumer that polls moq_session_poll_events itself and
 * forwards each event here gets behavior identical to the tick pump.
 *
 * SERIALIZED usage. Event namespace/name/token spans are borrowed and go
 * invalid at the next advancing session call (see moq/session.h). Therefore a
 * manual consumer MUST process one event to completion before polling the
 * next: on MOQ_ERR_WOULD_BLOCK, drain session actions and call moq_pub_flush
 * until it returns MOQ_OK, THEN poll/forward the next event. A polled event is
 * NEVER held across a flush, and is NEVER re-submitted (its borrowed spans
 * would be dead). For a correctly serialized event the single call fully
 * consumes its borrowed data (resolving the track, running the accept callback)
 * and stages only owned state, so flush completes from that owned state with no
 * event access.
 *
 * Returns MOQ_OK (see *result for CONSUMED/IGNORED), MOQ_ERR_INVAL on NULL
 * args, or MOQ_ERR_WOULD_BLOCK against a full action queue:
 *
 *   WOULD_BLOCK + result == CONSUMED: THIS event was taken and staged owned
 *     work (a Forward-0 subgroup retirement, or a blocked accept/reject).
 *     Drain actions + moq_pub_flush to completion; do NOT replay the event.
 *
 *   WOULD_BLOCK + result == IGNORED: the single pending slot is still occupied
 *     by earlier staged work, so this event was NOT taken. This only happens
 *     if the caller broke serialization (polled a new event before flushing
 *     the previous one to MOQ_OK). Recovery: flush to completion; the event's
 *     borrowed spans are now invalid, so it cannot be resubmitted -- structure
 *     the loop to flush before polling the next event and this never arises.
 */
MOQ_API moq_result_t moq_pub_handle_event(
    moq_publisher_t *pub,
    const moq_event_t *event,
    uint64_t now_us,
    moq_pub_event_result_t *result);

/*
 * Progress ALL staged manual-mode work after the caller drains session
 * actions: a pending subscribe accept/reject, a deferred retained FETCH,
 * lazy subgroup retirements armed by a Forward-0 update, and finite-window
 * completion work unlocked by moq_pub_declare_groups_complete_through.
 * This is the required follow-up to a MOQ_ERR_WOULD_BLOCK from
 * moq_pub_handle_event (and the manual-mode drive for completion work
 * staged by a declaration that returned MOQ_ERR_WOULD_BLOCK).
 * Returns MOQ_OK when no staged work remains or it all completed.
 * Returns MOQ_ERR_WOULD_BLOCK if still blocked — drain actions and call again.
 * Idempotent under retry: callbacks and wire actions never double.
 * Caller must resolve pending work (flush until OK or error) before destroying
 * the publisher.
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
