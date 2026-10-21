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
    MOQ_PUB_CALLBACK_V1 = 3,
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

/* Versioned subscribe authorization observation. Library-produced: struct_size
 * is stamped with this library's sizeof. Before reading a field, require
 * struct_size >= offsetof(moq_pub_subscribe_info_v1_t, FIELD) + sizeof(FIELD).
 * The complete initial prefix extends through options; future appended fields
 * require their own complete-field checks, not a check against current sizeof.
 *
 * info, namespace/name bytes, tokens (including token values), and options are
 * BORROWED for the callback only. Copy any needed data before returning; a
 * shallow copy does not retain the pointed-to observations. track and deferred
 * are opaque handles with the same lifetime/resolution rules as the legacy
 * callback. No publisher/session mutation may reenter from this callback.
 *
 * The legacy filter/location/timeout fields remain projections. options carries
 * lossless selected parameters, including explicit NONE versus absence, initial
 * fill presence, and independent timeouts. It is NULL if the event producer or
 * negotiated profile supplies no admission record (including draft-16/18).
 * Check its own struct_size before reading/copying its fields; its complete
 * frozen prefix is MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE bytes. */
typedef struct moq_pub_subscribe_info_v1 {
    uint32_t                    struct_size;
    moq_pub_track_t            *track;
    moq_namespace_t             track_namespace;
    moq_bytes_t                 track_name;
    moq_subscribe_filter_t      filter;
    uint8_t                     subscriber_priority;
    moq_group_order_t           group_order;
    bool                        forward;
    uint64_t                    start_group;
    uint64_t                    start_object;
    uint64_t                    end_group;
    uint64_t                    delivery_timeout_us;
    const moq_resolved_token_t *tokens;
    size_t                      token_count;
    moq_pub_deferred_t         *deferred;
    uint64_t                    deferred_id;
    const moq_subscription_admission_options_t *options;
} moq_pub_subscribe_info_v1_t;

/* Same ACCEPT/REJECT/DEFER decisions and out_error_code contract as the legacy
 * callback. DEFER requires a non-NULL info->deferred and is resolved through
 * moq_pub_resolve_deferred, never by retaining this borrowed info record. */
typedef moq_pub_accept_decision_t (*moq_pub_subscribe_v1_cb)(
    void *ctx,
    const moq_pub_subscribe_info_v1_t *info,
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
 * advertisement was accepted and later withdrawn by the peer. TERMINATED is
 * D22 namespace-operation closure, not a peer rejection. */
typedef enum moq_pub_namespace_terminal_kind {
    MOQ_PUB_NAMESPACE_REJECTED  = 1,
    MOQ_PUB_NAMESPACE_CANCELLED = 2,
    MOQ_PUB_NAMESPACE_TERMINATED = 3,
} moq_pub_namespace_terminal_kind_t;

/* Detail for on_namespace_terminal. Library-produced: struct_size is stamped
 * with this library's sizeof so a newer caller can gate any future field.
 * REJECTED carries error_code, can_retry, retry_after_ms, and reason;
 * CANCELLED carries error_code and reason with can_retry=false and
 * retry_after_ms=0. TERMINATED carries cause/local_role/stream_error_code;
 * error_code, can_retry, retry_after_ms and reason are zero/false/empty.
 * reason is BORROWED for the callback's duration only.
 *
 * Explicit D22 clients must handle kind 3: struct_size describes available
 * fields, not which kinds the caller understands. D16/18 retain kinds 1/2. */
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
    /* Appended after the complete original extent. Announcement-specific
     * MOQ_NAMESPACE_TERMINAL_* and MOQ_NAMESPACE_ROLE_* from session.h (not
     * the namespace-subscription cause domain). RESET/STOP preserve the exact
     * peer uint64 code, including zero; other causes carry zero. */
    uint32_t                          cause;
    uint32_t                          local_role;
    uint64_t                          stream_error_code;
} moq_pub_namespace_terminal_info_t;

#define MOQ_PUB_NAMESPACE_TERMINAL_INFO_V0_SIZE \
    offsetof(moq_pub_namespace_terminal_info_t, cause)

/* Detail for on_new_group_request. Library-produced: struct_size is stamped
 * with this library's sizeof; gate any future field on it.
 *
 *   requested_group    the request's value: the largest Group ID the
 *                      subscriber knows, plus one; 0 when it knows none.
 *   has_current_group  the track has published an object; current_group is
 *                      the largest Group ID it has published. */
typedef struct moq_pub_new_group_request_info {
    uint32_t struct_size;
    bool     has_current_group;
    uint64_t requested_group;
    uint64_t current_group;
} moq_pub_new_group_request_info_t;

/* A subscriber asked this Original Publisher to start a new group on a track
 * that advertises DYNAMIC_GROUPS (moq_pub_track_cfg_t.dynamic_groups). Fired
 * for a NEW_GROUP_REQUEST on an accepted SUBSCRIBE and on a subscription or
 * publication update, and only when the request warrants one (draft-22
 * §9.20.19; drafts 16 and 18 say the same): its value is 0 or larger than the
 * track's current group. The publisher "SHOULD end the current Group and
 * begin a new Group as soon as practical" and chooses that group's ID, so
 * record the request and start the group from the application's loop. A
 * track that does not advertise DYNAMIC_GROUPS ignores the parameter, as the
 * draft says, and fires nothing. Non-reentrant; `info` is valid for the
 * callback only. */
typedef void (*moq_pub_new_group_request_cb)(
    void *ctx, moq_pub_track_t *track,
    const moq_pub_new_group_request_info_t *info);

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

/* Facade-local value identity. Never convert to a core fill handle. Tokens are
 * not reused within one linked library instance; a foreign/stale token is
 * STALE_HANDLE. Exhaustion is LIMIT, distinct from temporary job pressure. */
typedef struct moq_pub_fill { uint64_t _opaque; } moq_pub_fill_t;
#ifdef __cplusplus
#define MOQ_PUB_FILL_INVALID moq_pub_fill_t{0}
#else
#define MOQ_PUB_FILL_INVALID ((moq_pub_fill_t){0})
#endif
static inline bool moq_pub_fill_is_valid(moq_pub_fill_t fill) { return fill._opaque != 0; }
static inline bool moq_pub_fill_eq(moq_pub_fill_t a, moq_pub_fill_t b)
{ return a._opaque == b._opaque; }

typedef enum moq_pub_fill_decision {
    MOQ_PUB_FILL_AUTO_RETAINED = 0,
    MOQ_PUB_FILL_DEFER = 1
} moq_pub_fill_decision_t;

/* Borrowed, library-stamped records. Copy plain values during the callback;
 * never keep the record pointer or read beyond struct_size. Exactly one parent
 * handle is valid. Range endpoints are inclusive, with full uint64_t width. */
typedef struct moq_pub_fill_request {
    uint32_t struct_size;
    moq_pub_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating_request_id;
    uint64_t start_group, start_object, end_group, end_object;
    uint64_t processed_at_us;
    uint64_t fill_timeout_ms;
    uint8_t group_order;
    uint8_t subscriber_priority;
    bool has_fill_timeout;
} moq_pub_fill_request_t;

typedef struct moq_pub_fill_terminal {
    uint32_t struct_size;
    moq_pub_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating_request_id;
    uint32_t reason;       /* MOQ_FILL_* terminal reason */
    uint32_t error_domain; /* MOQ_FILL_ERROR_* */
    uint64_t error_code;
} moq_pub_fill_terminal_t;

typedef moq_pub_fill_decision_t (*moq_pub_fill_request_cb)(void *ctx,
    moq_pub_track_t *track, const moq_pub_fill_request_t *request);
typedef void (*moq_pub_fill_terminal_cb)(void *ctx, moq_pub_track_t *track,
    const moq_pub_fill_terminal_t *terminal);

typedef struct moq_pub_cfg {
    uint32_t              struct_size;
    moq_pub_accept_mode_t accept_mode;
    uint8_t               default_publisher_priority;
    moq_pub_subscribe_cb  on_subscribe;
    void                 *on_subscribe_ctx;
    moq_pub_callbacks_t   callbacks;
    /* Appended: fill requests the facade can hold in flight at once (each
     * is answered from a snapshot of the track's retained group, or failed
     * on the wire); 0 selects exactly 16. This bound is never enlarged to
     * match the session pool. Jobs remain charged through terminal dispatch.
     * Reserved only on a draft with fills (moq_session_supports_fill). Read
     * only when struct_size covers it. */
    uint32_t              max_fills;
    /* Appended beyond the complete old max_fills layout. Explicit mode
     * MOQ_PUB_CALLBACK_V1 requires this WHOLE callback/context block and a
     * non-NULL hook; a NULL context is valid. Missing/partial block, a selected
     * V1 hook or context under another mode, or both legacy and V1 authorizers
     * are INVAL before allocation. No implicit callback-mode selection.
     * Older libraries reject mode3 rather than falling back to ACCEPT_ALL. */
    moq_pub_subscribe_v1_cb on_subscribe_v1;
    void                  *on_subscribe_v1_ctx;
    /* Beyond the complete V1 layout (through on_subscribe_v1_ctx). Aggregate
     * AND per-candidate retained
     * payload+serialized-property byte ceiling; zero is exactly 16 MiB.
     * Temporary shared credit delays acceptance. A candidate larger than the
     * ceiling is accepted without refs and failed with INTERNAL_ERROR. */
    uint64_t               max_fill_snapshot_bytes;
    /* Each callback/context pair must be wholly present or absent. Request
     * defaults to AUTO_RETAINED; DEFER leaves the bounded job usable after
     * callback return. Callbacks are non-reentrant: no publisher/session
     * mutation, destruction, tick or flush within either callback. */
    moq_pub_fill_request_cb on_fill_request;
    void                  *on_fill_request_ctx;
    moq_pub_fill_terminal_cb on_fill_terminal;
    void                  *on_fill_terminal_ctx;
    /* Appended: see moq_pub_new_group_request_cb. The pair is read only when
     * struct_size covers both; a torn pair is MOQ_ERR_INVAL. */
    moq_pub_new_group_request_cb on_new_group_request;
    void                  *on_new_group_request_ctx;
    /* Appended: fired on a session-wide GOAWAY with the new session URI
     * (borrowed for the callback; empty means reuse the current URI). Uses
     * callbacks.ctx. When set it is called instead of callbacks.on_draining.
     * Read only when struct_size covers it. */
    void (*on_goaway)(void *ctx, moq_bytes_t new_session_uri);
} moq_pub_cfg_t;

/* Single-owner, non-reentrant fill operations, after a DEFER callback has
 * returned. The object cfg has its own struct_size. Caller rcbuf ownership is
 * unchanged on EVERY result; accepted actions hold independent refs. Queued
 * FIN/reset does not release job credit: the real terminal is dispatched once
 * before refs/identity are released. A blocked abort is retained for flush.
 * No operation exposes session access or creates a new FETCH request. */
MOQ_API moq_result_t moq_pub_open_fill(moq_publisher_t *pub, moq_pub_fill_t fill,
                                      uint64_t now_us);
MOQ_API moq_result_t moq_pub_write_fill_object(moq_publisher_t *pub, moq_pub_fill_t fill,
    const moq_fetch_object_cfg_t *cfg, uint64_t now_us);
MOQ_API moq_result_t moq_pub_write_fill_range(moq_publisher_t *pub, moq_pub_fill_t fill,
    moq_fetch_range_kind_t kind, uint64_t group_id, uint64_t object_id, uint64_t now_us);
MOQ_API moq_result_t moq_pub_end_fill(moq_publisher_t *pub, moq_pub_fill_t fill,
                                     uint64_t now_us);
MOQ_API moq_result_t moq_pub_abort_fill(moq_publisher_t *pub, moq_pub_fill_t fill,
                                       uint64_t error_code, uint64_t now_us);

/* Pointer-only initializer. Clears and stamps ONLY the frozen prefix (the
 * original layout: struct_size, accept_mode, default_publisher_priority). It
 * cannot know the caller's storage size, so it must not write the full current
 * sizeof -- that would overflow a caller compiled against the original (smaller)
 * struct. The appended fields (on_subscribe, on_subscribe_ctx, callbacks,
 * max_fills, on_subscribe_v1/on_subscribe_v1_ctx) default to disabled;
 * to set any of them, or to initialize the full
 * current struct, use moq_pub_cfg_init_sized(). */
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
 * D22 keeps draining the normal FIFO while admissions wait for fill credit or
 * retained-FETCH responses wait for output credit. Each retained FETCH has a
 * bounded owner and immutable snapshot; progress visits owners in arrival
 * order and skips blocked owners so independent work can proceed. D16/18 keep
 * one retained-FETCH slot and serialize polling until that response completes.
 * Drain actions and retry tick; WOULD_BLOCK reports remaining staged work.
 *
 * Because tick is the session's only event consumer, it answers every request
 * nothing else will: a SUBSCRIBE or TRACK_STATUS for a track this publisher
 * does not have is refused with DOES_NOT_EXIST, and a request kind the facade
 * does not serve (a peer's publication or namespace advertisement, a
 * namespace or track-discovery subscription) with NOT_SUPPORTED ("Limited endpoints SHOULD respond to
 * any unsupported messages with the appropriate NOT_SUPPORTED error code,
 * rather than ignoring them", draft-22 §1.6; the same in drafts 16 and 18). An
 * answer the session cannot take yet is retried by later ticks and flushes.
 * moq_pub_handle_event leaves those requests IGNORED for another owner.
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
    /* Permanently reserved, never read. Covers the pre-monotonic struct's
     * trailing padding after has_publisher_priority (its size varies by
     * target), plus any additional bytes needed for the frozen placement.
     * A field in old padding would misread an old caller's uninitialized
     * bytes as an explicit declaration. These seven reserved bytes keep
     * the next append at or beyond the historical sizeof boundary. */
    uint8_t         _reserved_track_tail[7];
    /* Immutable per-track declaration: this track's group ids are
     * non-decreasing for its whole lifetime, and a group is never written
     * again once a higher group has begun or once its End-of-Group evidence
     * is recorded. The facade enforces the promise (violating writes return
     * MOQ_ERR_WRONG_STATE before any state changes) and uses the evidence
     * to auto-complete finite subscription filters: on drafts 16 and 18 a
     * destination whose filter End Group is provably complete is terminated
     * with SUBSCRIPTION_ENDED and the exact stream count, the same
     * completion flow as moq_pub_declare_groups_complete_through (draft-22
     * keeps it established; see there). Declaring it opts
     * the track out of honoring a later End-widening at or below the
     * completion watermark (subscribers fill gaps via FETCH). Completion is
     * staged work: evidence created by a write is acted on by
     * moq_pub_flush / moq_pub_tick, never inside the write itself --
     * manual-mode apps must keep flushing after evidence-producing writes.
     * Read only when struct_size covers the whole field: set it only with
     * moq_pub_track_cfg_init_sized(). */
    bool            monotonic_groups;
    /* Permanently reserved, never read: keeps the fields below past the
     * sizeof of the layout that ends at monotonic_groups on every target. */
    uint8_t         _reserved_track_tail2[7];
    /* Appended: the track's Track Properties (draft-22 §10) in the
     * session's encoding, copied by add_track. Every answer the facade gives for the track carries
     * them: SUBSCRIBE_OK, TRACK_STATUS_OK, the retained-group fetch answer
     * and, when the publish cfg supplies none, the publication (each request
     * that asked for none with INCLUDE_PROPERTIES = 0 gets none). Validated as
     * moq_session_accept_subscribe() validates them: MOQ_ERR_INVAL from
     * add_track. Read only when struct_size covers the field; empty sends
     * none. */
    moq_bytes_t     track_properties;
    /* Appended: advertise DYNAMIC_GROUPS = 1 (draft-22 §10.6), added to
     * track_properties in the session's encoding unless they already carry
     * it. Subscribers may then ask for new groups, which surface through
     * on_new_group_request. A track_properties set that carries
     * DYNAMIC_GROUPS = 0 is MOQ_ERR_INVAL. Read only when struct_size covers
     * the field. */
    bool            dynamic_groups;
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
 * wedges the retry).
 *
 * Removal ends this publisher's memory of the track's group history. Adding
 * the same Full Track Name again does not reset its wire identity: the
 * application must not publish again any Location the track published
 * before, and the facade does not detect it. */
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

/* Where the track's publication went when the peer moved it elsewhere: a
 * REDIRECT refusal of the PUBLISH (on_publish_error reported
 * MOQ_REQUEST_ERROR_REDIRECT) or a GOAWAY on its request stream
 * (on_publish_finished reported the end). The track is unpublished either
 * way and may be published again (here, or at the target). MOQ_OK writes
 * min(out_size, library size) bytes and stamps struct_size; spans borrow
 * facade storage until the track is published again, removed or the
 * publisher destroyed. MOQ_DONE: the last publication did not end by
 * migration. MOQ_ERR_NOMEM: the migration was recorded but its target could
 * not be kept. MOQ_ERR_INVAL for NULL arguments, a foreign track, or an
 * out_size short of the fields through `target`. Not an advancing call. */
MOQ_API moq_result_t moq_pub_get_track_migration(
    const moq_publisher_t *pub,
    const moq_pub_track_t *track,
    moq_request_migration_t *out,
    size_t out_size);

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
 * D22 can retain one immutable response snapshot per session FETCH slot. Each
 * snapshot pins at most MOQ_PUB_RETAINED_MAX_OBJECTS payload/properties pairs
 * and its source track's max_retained_bytes at admission. The total pinned
 * response bytes are bounded by the sum of those per-owner limits, separately
 * from the current retained caches, fill budgets and transport send buffers.
 * Replacing/clearing a cache or removing its track does not change an already
 * admitted response. Completion, cancellation or publisher destruction releases
 * its snapshot. D16/18 retain only one such response snapshot at a time.
 *
 * Fills: on a draft with fills (draft-22 section 3.4), a subscription on the
 * track may carry a fill request, surfaced by the session as
 * MOQ_EVENT_FILL_REQUEST with the resolved inclusive range. AUTO_RETAINED
 * answers from an immutable retained-group snapshot when that snapshot covers
 * the entire resolved inclusive range. Only the requested objects are sent,
 * followed by FIN. Missing requested coverage, including an empty snapshot,
 * fails with FETCH_HEADER followed by RESET_STREAM
 * (MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS), leaving the subscription untouched
 * (section 3.4.1); a strict subset never finishes successfully. Unlike the
 * explicit FETCH policy above, a fill need not request object 0. This does not
 * change the service catalog's independent-base requirement. The stream count
 * the facade reports when it completes a subscription or publication stays
 * the count of streams the facade itself opened for the destination; the
 * session adds the fill fetch streams it opened (section 9.9 counts them).
 *
 * Identity (group history, see the write contract below): an install
 * assigns Objects 0..N Subgroup ID 0 of the group, and every FETCH or fill
 * response carries the Subgroup ID each Object was assigned. It is allowed
 * for a group with no history, and for a group whose only assignments are
 * one subgroup (no datagram, no revisit) holding Objects 0..M without a gap:
 * when N <= M it assigns nothing new and is allowed even after a FIN;
 * Objects above M extend that subgroup only when none of them is assigned
 * and none of its streams has ended with a FIN. Anything else -- a sealed
 * group, a final Object below N, a datagram, a revisit, an assigned Object
 * above M, or Objects above M after a FIN -- is MOQ_ERR_WRONG_STATE before
 * any effect; an install needing a seventeenth range is MOQ_ERR_NOMEM.
 * Clearing or replacing the retained group never removes identity.
 *
 * Draft-16 whole-group FETCH: a FETCH whose range requests the entire group
 * (End Location Object 0, or an end group past it) makes the last Object it
 * sends the group's final Object (draft-16 section 2.4.2). When the track
 * assigned an Object above the retained ones, the request is refused with
 * MOQ_REQUEST_ERROR_NOT_SUPPORTED (the session stays open). The response is
 * committed only with its snapshot: once staged, an Object above the last
 * retained one is refused MOQ_ERR_WRONG_STATE, while a staging that failed to
 * allocate its snapshot fixes nothing.
 *
 * Returns MOQ_OK, MOQ_ERR_INVAL (NULL/foreign, bad struct_size, empty/oversize/
 * out-of-order objects, NULL payload), MOQ_ERR_WRONG_STATE (track ended, or
 * the group history refuses the install), or MOQ_ERR_NOMEM (allocation, or
 * the group needs a record and none can be freed).
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
    /* Stream: the END_OF_GROUP bit of the subgroup header, which says the
     * subgroup holds the Group's largest Object (drafts 16 and 18). It is
     * fixed when the subgroup opens, so every Object written into one
     * subgroup carries the same value: a publisher that puts a whole Group in
     * one subgroup sets it on every Object of the Group. */
    bool                end_of_group;
} moq_pub_object_cfg_t;

/* Pointer-only initializer: clears/stamps ONLY the frozen v0 prefix (through
 * `status`); the appended end_of_group stays disabled. Use the _sized form to
 * set it. */
MOQ_API void moq_pub_object_cfg_init(moq_pub_object_cfg_t *cfg);
MOQ_API void moq_pub_object_cfg_init_sized(moq_pub_object_cfg_t *cfg,
                                           size_t cfg_size);

/* Subgroup header contract (write_object[_ex], begin_object): the header a
 * destination sent when it opened the subgroup for a Group fixes two things
 * for every later object of that Group on that destination (drafts 16 and
 * 18). end_of_group must equal the header's END_OF_GROUP bit. Properties are
 * asymmetric: an object with properties needs a header with the properties
 * field, while an object without them (NULL) fits either header -- under a
 * header with the field it is written with an empty list. An object that does
 * not fit is refused with MOQ_ERR_WRONG_STATE before anything is committed;
 * no operation is left pending, and the same object written to fit can be
 * written next. A new Group opens new subgroups with the header its first
 * object asks for.
 *
 * Fan-out ownership and retry contract (write_object[_ex], begin_object,
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
 * decref, within normal refcount semantics.
 *
 * Group history (every draft). Each Object the track admits -- a stream
 * write or begin, a datagram, a status, or a retained install, with or
 * without eligible destinations -- is assigned its (Group, Subgroup ID,
 * delivery mode) at admission and keeps it for the track's life; a write
 * that would change it is refused. Groups need not increase. The stream
 * Objects the track writes while it stays in one group share one subgroup.
 * When the track leaves the group (a stream write to another group, or
 * moq_pub_end_group) or abandons it (moq_pub_reset_group), that subgroup
 * ends; a later stream Object in the group is a revisit, which opens the
 * next Subgroup ID (1, 2, ...) and must be larger than every Object already
 * assigned in the group. Once one destination has ended the open subgroup
 * with a FIN, its final Object is fixed for that receiver: replaying an
 * Object already in it still continues it, but a new Object above every
 * Object assigned in the group opens the next Subgroup ID instead (and one
 * at or below that maximum is refused); the other destinations finish the
 * old subgroup. FIRST_OBJECT (drafts 18 and 22, never draft 16) is
 * set on a stream exactly when the stream's first Object is its subgroup's
 * first Object. A destination's stream ends with a FIN only when it carried
 * every Object of its subgroup except those below the destination's Start
 * Location; otherwise it is RESET. An Object past the end of the
 * destination's window is never sent to it, and a stream that ends without
 * it ends with a RESET.
 *
 * Bounds (local policy, not wire limits): the track holds exact records for
 * 16 groups. A revisited group keeps its record for the track's life, at
 * most 15 groups may be revisited, and a group may be revisited at most 7
 * times (Subgroup IDs 0..7). When a newer group needs a record, the least
 * recently admitted group that was never revisited and is not the group
 * being written is sealed; a datagram in a new group may seal the group
 * being written when no other record can go (its destinations' streams still
 * end correctly, and later Objects of it are refused). Each group record
 * keeps its assigned Objects exactly, as at most 16 ranges of consecutive
 * Objects of one subgroup or of the datagram mode; replaying an assigned
 * Object, extending a range or joining two ranges always fits. Freeing
 * streams never restores these budgets.
 *
 * Refusals, all before any effect (nothing sent, payload stays the caller's):
 *   MOQ_ERR_WRONG_STATE  an Object in a sealed group (moq_pub_end_group, or
 *                        displaced from the records); a revisit at or below
 *                        the group's largest assigned Object, and any later
 *                        Object of a revisit subgroup at or below the highest
 *                        Object of the subgroup before it; an Object
 *                        above the group's final Object (fixed by a FIN of a
 *                        subgroup whose header carried END_OF_GROUP, or by a
 *                        staged draft-16 whole-group FETCH), at or above an
 *                        admitted END_OF_GROUP status, or at or beyond an
 *                        admitted END_OF_TRACK status; an Object already
 *                        assigned in the other delivery mode or to another
 *                        subgroup; a new Object after a subgroup whose header
 *                        carried END_OF_GROUP.
 *   MOQ_ERR_NOMEM        an eighth revisit of a group, a revisit that would
 *                        make a sixteenth revisited group, a displaced group
 *                        the sealed record cannot hold, or an Object that
 *                        would need a seventeenth range in its group. */
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
 * with SUBSCRIPTION_ENDED when its finite filter end was genuinely passed
 * on a draft that defines that status (16 and 18), TRACK_ENDED otherwise
 * (always on draft-22), carrying the EXACT count of streams the facade
 * opened for it -- retryably and idempotently across WOULD_BLOCK.
 *
 * A published track (moq_pub_publish_track) is ended the same way on its
 * publication: the END_OF_TRACK status object first, on a fresh subgroup of
 * the publication, then the publication's terminal done (TRACK_ENDED) whose
 * stream count includes that stream. The publication receives the status
 * object only when it was accepted and forwarding (Forward 1) when the
 * operation began, and its window admits the terminal Location; otherwise --
 * and under the ceiling fallback -- the done alone ends it. A destination a
 * Forward or filter cut reset mid-terminal is ended by its done (never a
 * clean close of the reset stream). Each step of the terminal (open, status
 * object, FIN, done) is taken once, so end_track completes through any
 * action queue size by retrying on MOQ_ERR_WOULD_BLOCK.
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

/* End the track's current group: every destination's open subgroup ends
 * (FIN, or RESET for a destination that omitted an Object it owed -- any
 * at or after its Start Location), and the group is sealed -- no further stream, datagram or
 * retained Object is admitted in it (MOQ_ERR_WRONG_STATE). Sealing applies
 * with no destinations too and repeats harmlessly. It is a local
 * declaration: the wire carries an END_OF_GROUP only when the subgroup
 * header carried it. MOQ_ERR_WRONG_STATE mid-object or while another
 * operation is pending; MOQ_ERR_WOULD_BLOCK is retryable. */
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
 * Completion (drafts 16 and 18): every destination whose
 * subscription/publication filter has a finite End Group <= the watermark
 * terminates -- its open subgroup is closed cleanly first, then the terminal
 * done is sent with status SUBSCRIPTION_ENDED (0x3) and the exact
 * per-destination stream count. Draft-22 has no SUBSCRIPTION_ENDED, and its
 * publisher does not end a subscription because its filter end was passed
 * (draft-22 §3.3.1): such a destination stays established, receives nothing
 * outside its window, and ends with the track or with the subscriber.
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
 * subgroup it cannot decode. The group's Objects keep their assignments
 * and no final Object is fixed: the next stream Object in the same group
 * opens a new subgroup and must be larger than every Object already
 * assigned there (a revisit; see the write contract). Abandoning needs no
 * capacity and never fails on the history bounds. No wire effect (MOQ_OK)
 * when no subgroup is open. MOQ_ERR_WOULD_BLOCK if
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
 *                           a fill request or fill terminal on a served
 *                           subscription, namespace lifecycle for an
 *                           advertised track, a TRACK_STATUS for one of its
 *                           tracks, answered as a SUBSCRIBE would be: the
 *                           Largest Object and Track Properties a SUBSCRIBE_OK
 *                           would carry, or the refusal it would get; in a
 *                           callback accept mode a track with no accepted
 *                           subscription on this session is UNAUTHORIZED,
 *                           as a standalone FETCH is, and the callback is not
 *                           consulted). All required borrowed input has
 *                           been consumed; this does NOT promise completed
 *                           output. With WOULD_BLOCK, owned work remains staged.
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
 * Event namespace/name/token/options spans are borrowed and go invalid at the
 * next advancing session call (see moq/session.h). Dispatch while they are live,
 * clean up each event exactly once, and NEVER retain/re-submit it across flush.
 * CONSUMED means required state is owned, not that ACK/output has completed.
 *
 * D22: drain actions and call flush once per progress round, then continue
 * ordinary FIFO polling/dispatch even after WOULD_BLOCK + CONSUMED. In
 * particular, FILL_REQUEST and FILL_TERMINAL must reach this publisher: only
 * terminal dispatch returns a bound job's credit. Flush never polls events.
 * Retry flush after terminal dispatch; yield to the driver when no event,
 * action or application progress exists (an intentionally deferred fill or
 * authorizer may remain pending). Do not spin waiting for flush-to-OK.
 * Retained FETCHes follow the same D22 polling rule: each has a fixed owner
 * indexed by the session FETCH slot and distinguished by its full generation.
 * CONSUMED owns the complete response snapshot even if output is blocked.
 * Admissions use fixed per-parent scalar cells; the authorizer runs once and
 * a reserved candidate remains immutable until its ACK commits. This relies
 * on D22's one-unanswered-UPDATE-per-parent bound.
 * If destination allocation fails after staging, NOMEM + CONSUMED leaves the
 * decision owned for a later flush; no ACK or peer rejection has been sent.
 *
 * D16/18 retain the SERIALIZED rule: after WOULD_BLOCK, drain actions and flush
 * until OK before polling the next event. On every profile, let other owners
 * inspect IGNORED events before an advancing call invalidates borrowed spans.
 *
 * Returns MOQ_OK (see *result for CONSUMED/IGNORED), MOQ_ERR_INVAL on NULL
 * args, or MOQ_ERR_WOULD_BLOCK for pending output/admission credit:
 *
 *   WOULD_BLOCK + result == CONSUMED: THIS event was taken and staged owned
 *     work (a Forward-0 subgroup retirement, or a blocked accept/reject).
 *     Drain actions + moq_pub_flush; use the profile-specific pump rule above.
 *     Do NOT replay the event.
 *
 *   WOULD_BLOCK + result == IGNORED: this event was NOT taken. D16/18's single
 *     pending admission and retained-FETCH response require serialization.
 *     D22 SUBSCRIBE/UPDATE admissions and retained FETCHes never return this
 *     combination merely because another owner is waiting for credit.
 */
MOQ_API moq_result_t moq_pub_handle_event(
    moq_publisher_t *pub,
    const moq_event_t *event,
    uint64_t now_us,
    moq_pub_event_result_t *result);

/*
 * Progress ALL staged manual-mode work after the caller drains session
 * actions: pending subscribe accept/reject or UPDATE answers, retained FETCHes,
 * lazy subgroup retirements armed by a Forward-0 update, and finite-window
 * completion work unlocked by moq_pub_declare_groups_complete_through.
 * This is the required follow-up to a MOQ_ERR_WOULD_BLOCK from
 * moq_pub_handle_event (and the manual-mode drive for completion work
 * staged by a declaration that returned MOQ_ERR_WOULD_BLOCK).
 * Returns MOQ_OK when no staged work remains or it all completed.
 * Returns MOQ_ERR_WOULD_BLOCK if still blocked. D22 callers must keep routing
 * FIFO events for admission/output-credit waits as described above, not require
 * flush-to-OK before another poll. D16/18 retained-FETCH serving remains
 * serialized. Each D22 admission or retained-FETCH pass visits its occupied
 * owners once in arrival order, skipping blocked entries so independent work
 * can progress. Flush never polls or takes the next request.
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

/* The subscriptions the facade currently serves on `track`. Where the
 * session allows several subscriptions to one track (draft-22), each
 * accepted one counts and every write reaches each of them whose filter
 * admits the object; on drafts 16 and 18 a second one is refused. */
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
