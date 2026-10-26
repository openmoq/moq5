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
 * queries, fetch, and fills (draft-22 section 3.4: past objects a
 * subscription asks for on SUBSCRIBE or on a later update, delivered on
 * fill fetch streams and polled with moq_sub_poll_fill).
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

/* The peer accepted an outbound subscription update . Produced by the
 * library and BORROWED for the duration of the on_update_ok callback --
 * plain scalars, nothing retained. struct_size is stamped by the LIBRARY
 * with its own sizeof: gate any future appended field on
 * struct_size >= offsetof + sizeof before reading it. */
typedef struct moq_sub_update_result {
    uint32_t struct_size;
    bool     has_largest;      /* the response carried the peer's Largest */
    uint64_t largest_group;    /* valid iff has_largest */
    uint64_t largest_object;   /* valid iff has_largest */
    bool     has_expires;      /* draft-18 only */
    uint64_t expires_ms;       /* valid iff has_expires */
} moq_sub_update_result_t;

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
     * Values larger than SIZE_MAX are clamped to SIZE_MAX. Initial ACK property
     * snapshots also debit this cap, separately from bytes_queued, until the
     * corresponding track is released. */
    uint64_t max_queued_object_bytes;
    /* Appended: fired when the peer ACCEPTS an outbound
     * moq_sub_update_subscription for this track -- success only; a
     * REJECTED update terminates the subscription and surfaces through
     * on_subscribe_done with the update-failure status code (0x8).
     * *result is borrowed for the duration of the callback. Fired
     * synchronously inside moq_sub_tick AFTER the session has applied the
     * update (the acknowledgment is what latches Forward/timeout/filter
     * changes), and always refers to the track's single outstanding
     * update. Uses callbacks.ctx. */
    void (*on_update_ok)(void *ctx, moq_sub_track_t *track,
                         const moq_sub_update_result_t *result);
    /* Never read: protects the old tail padding on 32-bit ABIs with
     * eight-byte uint64_t alignment. */
    uint32_t _reserved_group_ends;
    /* Appended after on_update_ok: queue a moq_sub_group_end_t for every
     * subgroup that ends with a FIN and whose header carried END_OF_GROUP,
     * for moq_sub_poll_group_end(). false (the default) queues none. */
    bool surface_group_ends;
    /* Appended: fill request slots (one per fill a track asked for, retained
     * until native retirement and all accepted items, terminal items and local
     * failure notices are consumed; see moq_sub_poll_fill/local_failure below)
     * and the fill-item queue depth. 0 selects 16 request slots and the
     * configured/defaulted max_fetch_items, respectively. FETCH and FILL
     * share one tagged backing arena of their summed item depths, with
     * separate FIFO quotas. Metadata uses item credit too; each family keeps
     * its existing one-deep pending slot, included in queued-item stats.
     * Fill credit is reserved only on a draft with fills
     * (moq_session_supports_fill). Read only when struct_size covers the field.
     * Aligned as uint64_t (the struct's own alignment), so max_fills starts
     * at the complete sizeof of the layout that ends with surface_group_ends
     * on every target, never in its tail padding. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    uint32_t max_fills;
    uint32_t max_fill_items;
    /* Appended after max_fill_items, which ends the struct on every target:
     * queue a moq_sub_stream_terminal_t for every terminal of a stream
     * carrying a subgroup of an active track, for
     * moq_sub_poll_stream_terminal(), in a lane of max_stream_terminals
     * records (0 selects max_objects). false (the default) queues none and
     * changes nothing else. Read as one block, only when struct_size covers
     * surface_stream_terminals. */
    uint32_t max_stream_terminals;
    bool     surface_stream_terminals;
} moq_sub_cfg_t;

/* Pointer-only initializer. Clears and stamps ONLY the frozen v0 prefix (the
 * layout before the first appended field). It cannot know the caller's storage
 * size, so it must not write the full current sizeof -- that would overflow a
 * caller compiled against the original (smaller) struct. Appended fields
 * (on_goaway, on_subscribe_done, max_queued_object_bytes, on_update_ok,
 * surface_group_ends, max_fills, max_fill_items) default to disabled;
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
 * Draft-22 checks complete retained-byte ownership before dequeuing each
 * delivery. Temporary pressure returns WOULD_BLOCK without taking the event;
 * polling queued items restores credit. A delivery that cannot fit beside the
 * retained ACK metadata returns BUFFER once and latches an owner-local outcome
 * for moq_sub_poll_local_failure. Later ticks service cancellation and other
 * owners. Draft-16/18 retain their existing high-water admission policy.
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
    /* Appended: the subscription's filter in its semantic form (borrowed
     * for the call; see location.h). When set it replaces `filter` and
     * admits the forms it cannot express -- a NEXT_OBJECT start, a relative
     * start more than one group back, an absolute end object. A form the
     * session's draft cannot carry is refused with MOQ_ERR_UNSUPPORTED
     * before anything is sent. */
    const moq_location_filter_cfg_t *location;
    /* Appended: a fill request carried on the SUBSCRIBE (borrowed for the
     * call; see fill.h). The fill's items arrive through moq_sub_poll_fill
     * and its request handle through moq_sub_track_fill(). On a draft
     * without fills the subscribe is refused with MOQ_ERR_UNSUPPORTED
     * before anything is sent. */
    const moq_fill_cfg_t            *fill;
    /* Appended: RENDEZVOUS_TIMEOUT (draft-22 §9.20.6), the milliseconds the
     * subscription may wait at a relay for a publisher to appear, borrowed for
     * the call. Non-NULL selects it with the exact value, zero included (no
     * wait); NULL leaves it absent. A pointer keeps the struct's alignment.
     * On another draft a selection is MOQ_ERR_UNSUPPORTED before anything is
     * sent. Read only when struct_size covers the whole pointer. */
    const uint64_t                  *rendezvous_timeout_ms;
    /* Appended: OBJECT_DELIVERY_TIMEOUT and SUBGROUP_DELIVERY_TIMEOUT the
     * subscriber asks for, each exact milliseconds borrowed for the call.
     * Non-NULL selects one with its value, zero included; NULL leaves it
     * absent. Pointers keep the struct's alignment. Each is read only when
     * struct_size covers that whole pointer. Draft-16 has no independent
     * carriers; on draft-18, which defines both (§10.2.3, §10.2.4), this
     * library does not send them yet. Either way a selection is
     * MOQ_ERR_UNSUPPORTED before anything is sent. */
    const uint64_t                  *object_delivery_timeout_ms;
    const uint64_t                  *subgroup_delivery_timeout_ms;
} moq_sub_track_cfg_t;

/*
 * Both initializers default the filter to LARGEST_OBJECT. The symbol clears
 * and stamps the layout through auth_token_count -- the struct as a caller
 * built before `location` was appended holds it -- so it never writes past
 * such a caller's storage. A caller built against this header reaches the
 * whole struct: the function-like macro below passes its sizeof to
 * moq_sub_track_cfg_init_sized(), which clears and stamps min(cfg_size,
 * sizeof current), setting the filter when cfg_size reaches it.
 * A binding that calls the symbol instead of compiling the macro (an FFI, a
 * dlsym lookup) gets only that layout: fields it sets past it (location,
 * fill) are ignored. Such a binding calls moq_sub_track_cfg_init_sized()
 * with the size of its struct.
 */
MOQ_API void moq_sub_track_cfg_init(moq_sub_track_cfg_t *cfg);
MOQ_API void moq_sub_track_cfg_init_sized(moq_sub_track_cfg_t *cfg,
                                          size_t cfg_size);
#define moq_sub_track_cfg_init(cfg) \
    moq_sub_track_cfg_init_sized((cfg), sizeof(*(cfg)))

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
 * returns MOQ_ERR_WRONG_STATE here. On D22, also returns MOQ_ERR_WRONG_STATE
 * while a fill child or a queued/pending object, fill item, streaming chunk
 * or Group end still names the track, or a local delivery failure has an unpolled notice
 * or unsettled native cancellation. Continue ticking, polling facade items
 * and calling moq_sub_poll_local_failure before retrying release. Session
 * close settles native retirement without fabricating peer terminals; pending
 * local notices and already queued items remain pollable and prevent release.
 * D16/18 keep their existing terminal-state-only release behavior. Items
 * transferred to the caller no longer pin the facade slot; clean them and
 * finish using their track pointers before release.
 * Release does not purge the queues: records already queued for the track
 * (objects, chunks, Group ends) keep its handle, which a later subscribe may
 * reuse, so poll them before releasing the track (D22 refuses the release
 * until they are polled; D16/18 do not check).
 * The track handle MUST NOT be used after
 * release. No session message is sent (the subscription is already complete). */
MOQ_API moq_result_t moq_sub_release_track(
    moq_subscriber_t *sub,
    moq_sub_track_t *track);

/* Immutable initial SUBSCRIBE_OK values; state is the current
 * moq_sub_track_state_t value. Flags describe presence, so zero locations
 * and zero expiry remain distinct from omission. No ownership is transferred. */
#define MOQ_SUB_TRACK_RESULT_RESPONSE_SEEN 0x1u
#define MOQ_SUB_TRACK_RESULT_HAS_LARGEST   0x2u
#define MOQ_SUB_TRACK_RESULT_HAS_EXPIRES   0x4u

typedef struct moq_sub_track_result {
    uint32_t    struct_size;
    uint32_t    state;
    uint32_t    flags;
    uint64_t    largest_group;
    uint64_t    largest_object;
    uint64_t    expires_ms;
    moq_bytes_t track_properties;
} moq_sub_track_result_t;

/* Frozen full v0 extent: 56 bytes on LP64, 48 on eight-aligned ILP32,
 * 44 on four-aligned ILP32. Future tails must not change this floor. */
#define MOQ_SUB_TRACK_RESULT_V0_SIZE \
    (offsetof(moq_sub_track_result_t, track_properties) + sizeof(moq_bytes_t))

/* Observational, nonallocating, and callable from on_subscribed. No caller
 * stamp is required. Copies/stamps min(out_size, library size); NULL args,
 * a foreign/released track or a size below the full v0 floor return INVAL.
 * Every refusal leaves output untouched. Before the initial ACK, flags and
 * values are zero. Later objects and update ACKs never alter these values.
 * Properties borrow facade-owned storage until track release or sub destroy;
 * all access remains on the session executor. Copy bytes for another executor.
 *
 * If initial ACK retention fails, tick and this query report NOMEM (allocation)
 * or BUFFER (byte capacity). The consumed subscription becomes ACTIVE unless
 * already locally unsubscribed, with no on_subscribed or fabricated peer-error
 * callback. The failing tick stops after that ACK. Query failure stays latched
 * until release; later ticks process events normally (including terminal events
 * and unrelated tracks) and, absent queue backpressure, report the retained
 * error while its track is still ACTIVE. Unsubscribe then release the affected
 * track, or release after an observed terminal event. No retry reads an expired
 * session borrow and no protocol session is closed by this failure. */
/* Where a track went when the peer moved its subscription elsewhere: a
 * REDIRECT refusal (the track is in error; on_subscribe_error reported
 * MOQ_REQUEST_ERROR_REDIRECT) or a per-request GOAWAY (the track is done;
 * on_subscribe_done reported the GOING_AWAY status, 0x4). A fetch ended by
 * either surfaces its error item (REDIRECT, or GOING_AWAY), a track status
 * request its error outcome. MOQ_OK writes min(out_size, library size) bytes
 * and stamps struct_size; spans borrow facade storage until the track is
 * released. MOQ_DONE: the track did not end by migration. MOQ_ERR_NOMEM: the
 * migration was recorded but its target could not be kept. MOQ_ERR_INVAL for
 * NULL arguments, a foreign or released track, or an out_size short of the
 * fields through `target`. Not an advancing call. */
MOQ_API moq_result_t moq_sub_get_track_migration(
    const moq_subscriber_t *sub,
    const moq_sub_track_t *track,
    moq_request_migration_t *out,
    size_t out_size);

MOQ_API moq_result_t moq_sub_get_track_result(
    const moq_subscriber_t *sub,
    const moq_sub_track_t *track,
    moq_sub_track_result_t *out,
    size_t out_size);

/* -- Subscription update ------------------------------------------ */

typedef struct moq_sub_update_cfg {
    uint32_t struct_size;
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_forward;
    bool     forward;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_us;
    /* Appended: update the subscription's filter. Whole-BLOCK gate:
     * read only when struct_size covers THROUGH end_group, so a caller
     * whose struct ends mid-block can never send a torn filter. Location
     * fields apply to the ABSOLUTE_* forms only; values set on the
     * relative forms are IGNORED (the wire carries none for them);
     * ABSOLUTE_RANGE with end_group < start_group is MOQ_ERR_INVAL. */
    bool                   has_filter;
    moq_subscribe_filter_t filter;
    uint64_t               start_group;
    uint64_t               start_object;
    uint64_t               end_group;
    /* Appended: a fill request carried on this update (borrowed for the
     * call; see fill.h). A new fill never cancels an earlier one; its
     * handle is the track's newest, moq_sub_track_fill(). Read only when
     * struct_size covers the whole field (moq_sub_update_cfg_init_sized).
     * Refused with MOQ_ERR_UNSUPPORTED on a draft without fills. */
    const moq_fill_cfg_t  *fill;
    /* Reserved: covers the tail padding the layout that ended at `fill` left
     * on targets that align it to eight bytes, so `location` lands at or
     * past that old sizeof. Never read. */
    uint32_t               _reserved_location;
    /* Appended: the replacement filter in its semantic form (borrowed for
     * the call; see location.h), as moq_subscription_update_cfg_t carries
     * it: non-NULL replaces the filter block above, and NONE removes the
     * restriction. Read only when struct_size covers the pointer. A draft
     * without semantic filter updates refuses it with MOQ_ERR_UNSUPPORTED
     * before anything is sent. */
    const moq_location_filter_cfg_t *location;
    /* Appended: the delivery timeouts, as moq_subscription_update_cfg_t
     * carries them: MOQ_SUBSCRIPTION_UPDATE_{OBJECT,SUBGROUP}_DELIVERY_TIMEOUT
     * select object_delivery_timeout_ms / subgroup_delivery_timeout_ms, each
     * exact milliseconds; an unselected one keeps its value; reserved must be
     * zero. Read as one block, only when struct_size covers
     * subgroup_delivery_timeout_ms. On draft-22 these replace
     * has_delivery_timeout / delivery_timeout_us, which that draft refuses
     * (MOQ_ERR_UNSUPPORTED); drafts 16 and 18 refuse a selection here. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    uint32_t               delivery_timeout_flags;
    uint32_t               reserved;
    uint64_t               object_delivery_timeout_ms;
    uint64_t               subgroup_delivery_timeout_ms;
    /* Appended after the complete D22 timeout block: subscription-update
     * credentials (D16/D18/D22), borrowed for the call and encoded before
     * success. Use the sized initializer. An absent pair means anonymous;
     * a size ending inside the pointer/count pair is INVAL. */
    const moq_auth_token_t *auth_tokens;
    size_t                 auth_token_count;
} moq_sub_update_cfg_t;

/* Pointer-only initializer: zeroes and stamps ONLY the frozen v0 prefix
 * (struct_size .. delivery_timeout_us -- the layout before the filter
 * append). The appended filter block stays disabled; to set it, or to
 * initialize the full current struct, use moq_sub_update_cfg_init_sized. */
MOQ_API void moq_sub_update_cfg_init(moq_sub_update_cfg_t *cfg);

/* Size-aware initializer: zeroes and stamps min(cfg_size, sizeof current
 * struct). Pass sizeof(moq_sub_update_cfg_t) for the full current struct.
 * No-op if cfg is NULL or cfg_size cannot hold struct_size. */
MOQ_API void moq_sub_update_cfg_init_sized(moq_sub_update_cfg_t *cfg,
                                           size_t cfg_size);

/*
 * Update an active subscription's parameters (priority, forward,
 * delivery timeout, filter).  Only one update may be outstanding per
 * track; a second call before the peer responds returns
 * MOQ_ERR_WRONG_STATE.
 *
 * The peer's ACCEPTANCE is surfaced through the appended
 * moq_sub_cfg_t.on_update_ok callback (success only). A REJECTED update
 * terminates the subscription: it surfaces through on_subscribe_done
 * with the update-failure status code (0x8), never through on_update_ok.
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

/*
 * Whether the facade still retains received input for `track` that a poll
 * will return: a queued or pending LIVE object or chunk, or a queued or
 * pending FILL object of one of its fills. Fill CREATED, GAP and TERMINAL
 * items, Group ends, stream terminals, fill request slots and outstanding
 * requests are not input. A snapshot of what is retained now, not a
 * promise about what may still arrive, and not a test of whether the
 * track may be released (moq_sub_release_track decides that).
 *
 * False for NULL, for a track this subscriber does not own, and for a
 * released track. Observes only: no mutation, allocation, callback or
 * session advancement. Called on the subscriber's executor, as every call
 * here.
 */
MOQ_API bool moq_sub_track_has_queued_input(const moq_subscriber_t *sub,
                                            const moq_sub_track_t *track);

/* -- Group ends --------------------------------------------------- */

/* The end of a Group on a subscription: a subgroup whose header carried the
 * END_OF_GROUP bit ended with a FIN, so the last Object the subgroup carried
 * is the Group's largest (drafts 16 and 18). A subgroup that is reset, or
 * that ends with a FIN but without the bit, says nothing about where its
 * Group ends and produces no record. Plain values; nothing to clean up. */
typedef struct moq_sub_group_end {
    moq_sub_track_t *track;
    uint64_t         group_id;
    uint64_t         subgroup_id;
} moq_sub_group_end_t;

/*
 * Poll the next Group end. Group ends are queued only when
 * moq_sub_cfg_t.surface_group_ends is set, and only for an active track.
 *
 * Ordering: a Group end is queued in session event order, behind the objects
 * of its subgroup, and becomes pollable only once every object and chunk the
 * facade queued before it has been returned by moq_sub_poll_object() or
 * moq_sub_poll_chunk(). So when this returns a Group end, every object of
 * that subgroup the facade received has already been handed to the caller.
 * A caller that enables the option polls this queue too: like the object
 * queue, a full Group-end queue holds moq_sub_tick() at MOQ_ERR_WOULD_BLOCK.
 *
 * Returns MOQ_OK and fills out, MOQ_DONE when no Group end is pollable, and
 * MOQ_ERR_INVAL for a NULL argument.
 */
MOQ_API moq_result_t moq_sub_poll_group_end(
    moq_subscriber_t *sub,
    moq_sub_group_end_t *out);

/* -- Stream terminals --------------------------------------------- */

/* How the stream carrying a subgroup of a subscription ended. */
typedef uint32_t moq_sub_stream_terminal_kind_t;
/* FIN on a subgroup whose header carried END_OF_GROUP: the stream's last
 * Object is the Group's largest, so no Object above it exists. */
#define MOQ_SUB_STREAM_FIN_END_OF_GROUP 1u
/* FIN without END_OF_GROUP: the stream ended cleanly; nothing follows
 * about the Group. */
#define MOQ_SUB_STREAM_FIN              2u
/* RESET_STREAM: delivery ended incomplete. The Objects of the subgroup
 * after the last one the stream completed were not delivered by it; they
 * may still arrive another way. */
#define MOQ_SUB_STREAM_RESET            3u

/* One terminal of a stream carrying a subgroup. Plain values; nothing to
 * clean up. */
typedef struct moq_sub_stream_terminal {
    uint32_t                       struct_size;
    moq_sub_stream_terminal_kind_t kind;
    moq_sub_track_t               *track;
    uint64_t                       group_id;
    uint64_t                       subgroup_id;
    /* RESET only: the peer's RESET_STREAM code. */
    uint64_t                       error_code;
    /* The last Object the stream carried (completely, for RESET); absent
     * when it carried none, or for a RESET that cut an Object short whose
     * predecessor on the stream is not known. */
    bool                           has_last_object;
    /* RESET only: the reset cut this Object's payload short, so neither it
     * nor any later Object of the subgroup was delivered by the stream. */
    bool                           has_cut_object;
    uint8_t                        _reserved[6];
    uint64_t                       last_object_id;
    uint64_t                       cut_object_id;
} moq_sub_stream_terminal_t;

/* Frozen v0 floor: the complete layout through cut_object_id. */
#define MOQ_SUB_STREAM_TERMINAL_V0_SIZE \
    (offsetof(moq_sub_stream_terminal_t, cut_object_id) + sizeof(uint64_t))

/*
 * Poll the next stream terminal. Terminals are queued only when
 * moq_sub_cfg_t.surface_stream_terminals is set, and only for an active
 * track: one record per terminal of a stream carrying a subgroup, in
 * session event order, never coalesced.
 *
 * Ordering: a terminal becomes pollable only once every object and chunk
 * the facade queued before it has been returned by moq_sub_poll_object()
 * or moq_sub_poll_chunk(), so every object the stream delivered has been
 * handed to the caller first. A caller that applies facts in order polls
 * terminals after each object or chunk it polls; one that stores polled
 * objects for later settles those before applying a later terminal. A full
 * lane holds moq_sub_tick() at MOQ_ERR_WOULD_BLOCK, like the object queue;
 * no terminal is dropped.
 *
 * Retirement: while a terminal naming a track is queued, or held for a
 * full lane, moq_sub_release_track() refuses that track with
 * MOQ_ERR_WRONG_STATE, so a record never names a reused slot.
 *
 * `out_size` is the caller's sizeof; one below the v0 floor is
 * MOQ_ERR_ABI_MISMATCH and dequeues nothing. Returns MOQ_OK and fills out,
 * MOQ_DONE when no terminal is pollable, MOQ_ERR_INVAL for a NULL argument.
 */
MOQ_API moq_result_t moq_sub_poll_stream_terminal(
    moq_subscriber_t *sub,
    moq_sub_stream_terminal_t *out,
    size_t out_size);

/* -- Stats ----------------------------------------------------------- */

typedef struct moq_sub_stats {
    uint32_t struct_size;
    uint64_t subscribe_ok;            /* SUBSCRIBE_OK accepted by the facade */
    uint64_t subscribe_errors;        /* SUBSCRIBE_ERROR for this subscriber */
    uint64_t subscribe_done;          /* SUBSCRIBE_DONE for this subscriber */
    uint64_t subscription_update_ok;  /* SUBSCRIBE_UPDATE_OK acknowledgments */
    uint64_t unsubscribed;            /* UNSUBSCRIBED events for live tracks */
    uint64_t objects_received;        /* whole objects retained by facade */
    uint64_t objects_polled;          /* whole objects returned to caller */
    uint64_t chunks_received;         /* streaming chunks retained by facade */
    uint64_t chunks_polled;           /* streaming chunks returned to caller */
    uint64_t fetch_ok;
    uint64_t fetch_errors;
    uint64_t fetch_objects;
    uint64_t fetch_gaps;
    uint64_t fetch_complete;
    uint64_t fetch_items_polled;      /* every item returned by poll_fetch */
    uint64_t subgroup_finished;       /* graceful subgroup terminals */
    uint64_t subgroup_resets;         /* reset terminals, both event surfaces */
    uint64_t tick_would_blocks;       /* facade queue/budget backpressure */
    uint64_t objects_queued;          /* retained whole objects incl pending */
    uint64_t chunks_queued;           /* retained streaming chunks incl pending */
    uint64_t fetch_items_queued;      /* retained fetch items incl pending */
    uint64_t bytes_queued;            /* retained payload/properties bytes */
    bool     draining;
    bool     closed;
    /* Appended: fill counters (the v0 snapshot ends at `closed`). */
    uint64_t fill_objects;            /* fill-delivered objects retained */
    uint64_t fill_gaps;               /* fill range markers retained */
    uint64_t fill_terminals;          /* FILL_TERMINAL per fill, exactly once */
    uint64_t fill_items_polled;       /* every item returned by poll_fill */
    uint64_t fill_items_queued;       /* retained fill items incl pending */
    /* Appended: subscriptions this side cancelled because their Track was
     * malformed (draft-22 §12.1). Each also ends through on_subscribe_done
     * with MOQ_STREAM_ERROR_MALFORMED_TRACK (0x12) as its status. */
    uint64_t malformed_tracks;
} moq_sub_stats_t;

/* Snapshot subscriber-facade counters. Pass `out_size = sizeof(*out)`.
 * Retained queue gauges include the one-deep pending item that caused a prior
 * MOQ_ERR_WOULD_BLOCK, even though that item is not pollable until the app
 * drains and calls moq_sub_tick() again. bytes_queued counts retained
 * payload/properties bytes across whole-object, fetch-object, and streaming
 * chunk and fill-item queues. Same ABI contract as other size-aware libmoq snapshots:
 * requires the v0 stats size, writes min(out_size, current), and stamps
 * out->struct_size with the bytes written. */
MOQ_API moq_result_t moq_sub_get_stats(
    const moq_subscriber_t *sub,
    moq_sub_stats_t *out,
    size_t out_size);

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
 * A draft-22 TERMINATED outcome at the head returns MOQ_ERR_UNSUPPORTED
 * without writing or consuming it. Explicit draft-22 callers must use
 * moq_sub_poll_status_outcome to observe and consume all outcome kinds.
 *
 * result.request is an opaque correlation token, not a live object.
 * The slot is released by this call and may be reused by a later
 * moq_sub_track_status call. Compare pointer values for correlation
 * but do not dereference or retain as live state.
 */
MOQ_API moq_result_t moq_sub_poll_status(
    moq_subscriber_t *sub,
    moq_sub_status_result_t *out);

#define MOQ_SUB_STATUS_OUTCOME_OK         1u
#define MOQ_SUB_STATUS_OUTCOME_ERROR      2u
#define MOQ_SUB_STATUS_OUTCOME_TERMINATED 3u

#define MOQ_SUB_STATUS_OUTCOME_HAS_LARGEST 0x1u
#define MOQ_SUB_STATUS_OUTCOME_HAS_EXPIRES 0x2u
#define MOQ_SUB_STATUS_OUTCOME_CAN_RETRY   0x4u

/* Output-only status outcome. Flags describe presence, including zero values.
 * OK uses largest/expiry; ERROR uses native peer error/retry values only,
 * preserving the existing profile's moq_request_error_t normalization.
 * TERMINATED uses MOQ_TRACK_STATUS_* cause/role and the exact RESET/STOP
 * stream code (including zero); FIN diagnoses carry zero with no peer-code
 * meaning. All inactive fields and reserved are zero. */
typedef struct moq_sub_status_outcome {
    uint32_t struct_size;
    uint32_t kind;
    moq_sub_status_req_t *request;
    uint32_t flags;
    uint32_t reserved;
    uint64_t largest_group;
    uint64_t largest_object;
    uint64_t expires_ms;
    uint64_t error_code;
    uint64_t retry_after_ms;
    uint32_t cause;
    uint32_t local_role;
    uint64_t stream_error_code;
} moq_sub_status_outcome_t;

/* Frozen full v0 extent: 80 bytes on LP64/eight-aligned ILP32, 76 on
 * four-aligned ILP32. Future tails must not change this physical floor. */
#define MOQ_SUB_STATUS_OUTCOME_V0_SIZE \
    (offsetof(moq_sub_status_outcome_t, stream_error_code) + sizeof(uint64_t))

/* Nonblocking, nonallocating poll over the SAME bounded ring as the legacy
 * poll. No caller stamp, initializer or cleanup is required. NULL arguments
 * or out_size below the full v0 floor return INVAL; empty returns DONE.
 * Every refusal leaves output, queue and request slots untouched. Success
 * copies/stamps min(out_size, library size), leaving any future bytes untouched,
 * and only then releases the request slot. request is identifier-only and may
 * be reused by the next request; do not dereference it or retain as live state.
 * Access remains on the session executor. D16/18 behavior is unchanged. */
MOQ_API moq_result_t moq_sub_poll_status_outcome(
    moq_subscriber_t *sub,
    moq_sub_status_outcome_t *out,
    size_t out_size);

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
    /* FETCH credentials (D16/D18), borrowed for the call and encoded before
     * success. D22 retains its UNSUPPORTED joining-fetch result. The aligned
     * append preserves the previous allocation including its tail padding.
     * An absent pair means anonymous; a partial pair is INVAL. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    const moq_auth_token_t *auth_tokens;
    size_t                 auth_token_count;
} moq_sub_joining_fetch_cfg_t;

#define MOQ_SUB_JOINING_FETCH_CFG_V0_SIZE \
    offsetof(moq_sub_joining_fetch_cfg_t, auth_tokens)

/* Zero/stamp only the frozen prefix through subscriber_priority, including
 * its trailing padding. Auth fields stay disabled. Safe for old allocations. */
MOQ_API void moq_sub_joining_fetch_cfg_init(moq_sub_joining_fetch_cfg_t *cfg);

/* Zero/stamp min(cfg_size, sizeof current struct). No write if NULL or size
 * cannot hold struct_size. Required to enable the credential tail. */
MOQ_API void moq_sub_joining_fetch_cfg_init_sized(moq_sub_joining_fetch_cfg_t *cfg,
                                               size_t cfg_size);

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
    /* Terminal: the peer abruptly terminated a stream of this fetch (see
     * MOQ_EVENT_FETCH_RESET). u.reset carries the peer's STREAM error code --
     * not a request error -- and which stream carried it. */
    MOQ_SUB_FETCH_RESET    = 6,
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
        struct {
            uint64_t error_code;    /* peer STREAM application error code */
            bool     data_stream;   /* true: response data stream; false: request stream */
            bool     stop_sending;  /* request stream only: STOP_SENDING, not RESET_STREAM */
        } reset;
    } u;
} moq_sub_fetch_item_t;

/*
 * Poll the next fetch item. Returns MOQ_OK and fills out.
 * Returns MOQ_DONE when no items are queued.
 * Caller must call moq_sub_fetch_item_cleanup after processing.
 *
 * For terminal items (ERROR, COMPLETE, RESET), request is an opaque
 * correlation token. Normally polling the terminal releases its slot for a
 * later moq_sub_fetch call. After a D22 local delivery failure, reuse also
 * waits for moq_sub_poll_local_failure, accepted queued items and native
 * retirement to settle. A successful local cancellation or session close can
 * settle retirement without producing a peer terminal item. Continue ticking
 * and polling both APIs; do not dereference or retain request as live state.
 */
MOQ_API moq_result_t moq_sub_poll_fetch(
    moq_subscriber_t *sub,
    moq_sub_fetch_item_t *out);

MOQ_API void moq_sub_fetch_item_cleanup(moq_sub_fetch_item_t *item);

/* -- Fills (draft-22 section 3.4) --------------------------------- */

/*
 * A fill is a child of a track's subscription: requested on the SUBSCRIBE
 * (moq_sub_track_cfg_t.fill) or on a later update (moq_sub_update_cfg_t.fill),
 * answered by the publisher on a fill fetch stream, and ended exactly once.
 * Its request slot is allocated when the request is made and normally released
 * when its TERMINAL item is polled. After a local delivery failure, reuse also
 * waits for its local notice, accepted queued items and native retirement to
 * settle. Continue ticking and polling moq_sub_poll_local_failure as well as
 * fill items. Session close settles native retirement without fabricating a
 * peer terminal; pending notices and accepted items remain pollable.
 * Fill-delivered objects carry the same ownership as fetch objects: the
 * payload/properties refs are OWNED by the item until
 * moq_sub_fill_item_cleanup, count toward max_queued_object_bytes while
 * queued, and are released on moq_sub_destroy if never polled.
 */

/*
 * A fill this subscriber requested: an opaque value handle, all-zero when
 * absent. It names one request for good: once the slot is released under the
 * lifetime rules above the handle is stale, and a later request that takes the
 * same slot gets a different handle. A pending local-failure notice can keep
 * the handle present after its TERMINAL item is polled; it cannot reach a
 * different fill or restart the failed one.
 * Compare handles with moq_sub_fill_eq (FFI: compare the 64-bit values).
 * A handle is meaningful only to the subscriber that issued it.
 * Identities are never reused within one linked core library instance, even
 * after subscriber/address reuse. Separate library copies, unload/reload and
 * processes are separate domains; do not interchange or persist their tokens.
 * Exhausting the non-wrapping identity space returns MOQ_ERR_LIMIT from a
 * fill-bearing subscribe/update before owner or wire effects. Temporary full
 * request slots still return MOQ_ERR_WOULD_BLOCK. Issuance is thread-safe across
 * independent subscribers, not wait-free or signal/ISR-safe; it does not make
 * concurrent access to a single subscriber safe.
 */
typedef struct moq_sub_fill { uint64_t _opaque; } moq_sub_fill_t;

static inline bool moq_sub_fill_is_valid(moq_sub_fill_t f)
{
    return f._opaque != 0;
}

static inline bool moq_sub_fill_eq(moq_sub_fill_t a, moq_sub_fill_t b)
{
    return a._opaque == b._opaque;
}

/* Draft-22 local delivery capacity outcomes, never peer error codes. */
enum {
    MOQ_SUB_LOCAL_FAILURE_TRACK = 1,
    MOQ_SUB_LOCAL_FAILURE_FETCH = 2,
    MOQ_SUB_LOCAL_FAILURE_FILL = 3
};

typedef struct moq_sub_local_failure {
    uint32_t struct_size;
    uint32_t owner_kind;
    int32_t local_result;
    uint32_t reserved;
    moq_sub_track_t *track;
    moq_sub_fetch_req_t *fetch;
    moq_sub_fill_t fill;
    uint64_t initiating_request_id;
} moq_sub_local_failure_t;

#define MOQ_SUB_LOCAL_FAILURE_V0_SIZE \
    (offsetof(moq_sub_local_failure_t, initiating_request_id) + sizeof(uint64_t))

/* Non-advancing, allocation-free; all calls stay on the session executor.
 * OK transfers one value-only outcome; DONE leaves output untouched if none.
 * INVAL for NULL arguments or output shorter than the frozen full v0 floor.
 * Copies/stamps min(out_size, library_size), leaving a future suffix untouched.
 * TRACK selects track; FETCH selects fetch; FILL selects track/fill/initiator.
 * All inactive fields and reserved are zero. local_result is MOQ_ERR_BUFFER.
 * No references or cleanup; identities are observations, not permission to
 * dereference after a later call retires/reuses their owner. One notice per
 * failed owner; already accepted queued or polled items keep their ownership.
 * Poll every notice even if a terminal was already observed: an unpolled
 * notice pins its owner. Polling may release a FETCH/FILL slot immediately if
 * its other retirement obligations are settled. Cancellation retries through
 * tick independently, without closing the session. */
MOQ_API moq_result_t moq_sub_poll_local_failure(moq_subscriber_t *sub,
    moq_sub_local_failure_t *out, size_t out_size);

/* The newest fill requested on `track` whose slot has not been released, or
 * the all-zero handle. A local-failure notice or unsettled native retirement
 * can keep it present after its terminal is polled. Requests are answered in
 * order, so the handle read
 * right after the subscribe or update that carried the request is that
 * request's. */
MOQ_API moq_sub_fill_t moq_sub_track_fill(const moq_sub_track_t *track);

/*
 * Cancel one fill (STOP_SENDING on its stream) without touching the
 * subscription. Normally the slot stays allocated until the fill's TERMINAL
 * item (reason MOQ_FILL_LOCAL_CANCELLED) is polled, so queued items keep their
 * correlation. A local delivery failure also retains it until its notice and
 * native retirement settle. MOQ_ERR_STALE_HANDLE for a released handle (or
 * one this subscriber never issued); MOQ_ERR_WRONG_STATE before the session
 * has created the child, once its terminal is queued/polled but the slot is
 * retained, or for a fill already cancelled; MOQ_ERR_WOULD_BLOCK when the
 * session cannot queue the cancel yet (retry).
 */
MOQ_API moq_result_t moq_sub_cancel_fill(
    moq_subscriber_t *sub,
    moq_sub_fill_t fill,
    uint64_t now_us);

typedef enum moq_sub_fill_item_kind {
    MOQ_SUB_FILL_OBJECT   = 1,
    MOQ_SUB_FILL_GAP      = 2,
    /* Terminal: exactly once per fill. u.terminal carries the session's
     * reason (MOQ_FILL_*), the domain its code belongs to
     * (MOQ_FILL_ERROR_*), and the code (0 is a valid code when the domain
     * is not NONE). Polling releases the slot unless a local delivery failure
     * still has a notice, queued items or native retirement to settle. */
    MOQ_SUB_FILL_TERMINAL = 3,
    /* Actual core child creation, before its other items. No union payload. */
    MOQ_SUB_FILL_CREATED  = 4,
} moq_sub_fill_item_kind_t;

typedef struct moq_sub_fill_item {
    uint32_t                struct_size; /* bytes written by successful poll */
    uint32_t                kind;        /* MOQ_SUB_FILL_* */
    moq_sub_fill_t           request;   /* the fill this item belongs to */
    moq_sub_track_t         *track;
    uint64_t                initiating_request_id; /* initial SUBSCRIBE or UPDATE */
    union {
        struct {
            uint64_t     group_id;
            uint64_t     subgroup_id;
            uint64_t     object_id;
            uint8_t      publisher_priority;
            bool         datagram;
            moq_rcbuf_t *payload;
            moq_rcbuf_t *properties;
        } object;
        struct {
            uint32_t               range_kind; /* MOQ_FETCH_RANGE_* */
            uint64_t               group_id;
            uint64_t               object_id;
        } gap;
        struct {
            uint32_t reason;
            uint32_t error_domain;
            uint64_t error_code;
        } terminal;
    } u;
} moq_sub_fill_item_t;

/* Frozen full v0 extent, including its eight-byte-rounded trailing padding:
 * LP64 80, eight-aligned ILP32 72, four-aligned ILP32 64. Future tails must
 * start beyond this extent and must not change the floor. */
#define MOQ_SUB_FILL_ITEM_V0_SIZE \
    ((offsetof(moq_sub_fill_item_t, u.object.properties) + sizeof(moq_rcbuf_t *) + 7u) \
     & ~(size_t)7u)

/*
 * Poll the next fill item. No input stamp or initializer is required.
 * NULL arguments or out_size below the frozen full v0 floor return INVAL before
 * any write, dequeue or reference transfer. DONE (empty queue) also leaves out
 * untouched. On OK, copies/stamps min(out_size, library size), leaves later
 * caller bytes untouched, and transfers owned refs exactly once. Caller must
 * call moq_sub_fill_item_cleanup after processing.
 * CREATED and all later items carry the actual initiating request ID, including
 * zero; the latest-child convenience query is not an every-child observation.
 * Queued items pin their track. The returned track pointer is borrowed until
 * track release/subscriber destruction; polled payload/properties refs survive
 * child, track and subscriber retirement independently.
 * Polling a TERMINAL item releases the fill's slot: its handle (the item's
 * request) is identifier-only from then on, and stays distinct from every later
 * request's. CREATED consumes the same history item credit as other fill items.
 */
MOQ_API moq_result_t moq_sub_poll_fill(
    moq_subscriber_t *sub,
    moq_sub_fill_item_t *out,
    size_t out_size);

/* Uses the stamped prefix; the struct_size field itself must be accessible.
 * Only whole owned fields of an OBJECT are read/released and nulled. Metadata
 * owns no refs. NULL is harmless; repeated cleanup is safe. */
MOQ_API void moq_sub_fill_item_cleanup(moq_sub_fill_item_t *item);

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
