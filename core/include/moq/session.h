#ifndef MOQ_SESSION_H
#define MOQ_SESSION_H

/*
 * Sans-I/O session API.
 *
 * The session is a deterministic state machine: same inputs + same time
 * produce identical outputs. No sockets, no threads, no wall clock
 * reads, no global mutable state.
 *
 * Public input functions normalize into internal tagged records.
 * Outputs are pulled via poll_actions (adapter I/O) and poll_events
 * (application notifications).
 *
 * This header is included by moq/moq.h (the stable umbrella).
 *
 * -- ABI policy --
 *
 * Output records (moq_action_t, moq_event_t):
 *   - Append-only: new fields added to the end of per-kind structs.
 *   - Kind enum values are never renumbered or removed.
 *   - Unknown kinds must be ignored/logged by the caller.
 *   - Reserved inline capacity absorbs new variants without changing
 *     sizeof. If a future variant exceeds the reserve, a SONAME bump
 *     is required.
 *   - The size-aware _ex poll functions are the ABI safety boundary.
 *     The library writes min(library_sizeof, caller_element_size) per
 *     element; callers compiled against older headers are safe.
 *
 * Borrowed pointers:
 *   - Pointers in polled records are valid until the next advancing
 *     call on the same session. Advancing calls are: start,
 *     on_control_bytes, process_pending, tick, on_data_bytes,
 *     on_data_reset, on_data_stop, subscribe, accept_subscribe,
 *     reject_subscribe, unsubscribe, open_subgroup, write_object,
 *     begin_object, write_object_data, end_object,
 *     close_subgroup, reset_subgroup, goaway, close.
 *     publish_namespace, publish_namespace_done, accept_namespace,
 *     reject_namespace, cancel_namespace, grant_request_capacity,
 *     subscribe_namespace, on_bidi_stream_bytes, accept_ns_sub,
 *     reject_ns_sub, send_namespace, send_namespace_done,
 *     cancel_namespace_sub, on_bidi_stream_reset,
 *     fetch, accept_fetch, reject_fetch, fetch_cancel,
 *     write_fetch_object, write_fetch_range, end_fetch.
 *     Resolved tokens (moq_resolved_token_t) in SETUP_COMPLETE,
 *     SUBSCRIBE_REQUEST, and NAMESPACE_PUBLISHED are BORROWED from
 *     output scratch and follow the same borrow epoch.
 *     The borrow_epoch field, stamped at poll time, can be checked
 *     with moq_session_borrow_valid().
 *
 * Terminal close:
 *   - A fatal close supersedes all previously queued but undrained
 *     actions and events. After close, the queues contain only the
 *     CLOSE_SESSION action and SESSION_CLOSED event. This is by
 *     design: stale outputs from a dead session are not meaningful.
 */

#include "export.h"
#include "types.h"
#include "location.h"
#include "fill.h"

/* Forward declaration; full definition in rcbuf.h (included by moq.h). */
typedef struct moq_rcbuf moq_rcbuf_t;

#ifdef __cplusplus
extern "C" {
#endif

/* -- Protocol version ---------------------------------------------- */

typedef enum moq_version {
    MOQ_VERSION_DRAFT_16 = 16,
    /*
     * Recognized protocol version. A session can be created only for a version
     * whose profile is available in this build; when the draft-18 profile is
     * not present, moq_session_create() refuses this version (returns an
     * error). The constant and its ALPN are registered so transports can name
     * and negotiate the version independently of profile availability.
     */
    MOQ_VERSION_DRAFT_18 = 18,
    /*
     * Draft-22 wire profile. Registered so a session can be created for it;
     * offered to peers only once moq_version_supported() reports it.
     */
    MOQ_VERSION_DRAFT_22 = 22,
} moq_version_t;

/*
 * Versions this build can offer to a peer: those whose wire profile is
 * complete. moq_versions_supported() writes them newest first and returns
 * the full count even when cap is smaller, so a caller can size its buffer;
 * out may be NULL when cap is 0. The order is a listing, not a preference:
 * the component building an offer chooses which version goes first.
 */
MOQ_API bool   moq_version_supported(moq_version_t version);

MOQ_API size_t moq_versions_supported(moq_version_t *out, size_t cap);

/* Whether a Track Property set encoded for `version` is one this side may
 * publish: the checks the publisher facade applies when a track is added
 * (moq_pub_add_track), before any session exists. With dynamic_groups the
 * set is checked as published with DYNAMIC_GROUPS 1 added. MOQ_OK;
 * MOQ_ERR_INVAL when the set does not parse, states a property this side
 * may not publish, or carries an unreadable delivery timeout or default
 * publisher priority; MOQ_ERR_UNSUPPORTED for a version this build does not
 * support; MOQ_ERR_NOMEM if the DYNAMIC_GROUPS copy cannot be allocated. */
MOQ_API moq_result_t moq_track_properties_validate(moq_version_t version,
                                                   const uint8_t *props,
                                                   size_t len,
                                                   bool dynamic_groups);

/* -- Request terminal outcomes ------------------------------------- */

/* Why a request ended by a stream signal rather than by a message. Zero
 * means a message ended it (or the field is absent on an older producer).
 * Fixed-width discriminants, so a new cause never changes layout. */
typedef uint32_t moq_request_terminal_cause_t;
#define MOQ_REQUEST_TERMINAL_NONE                0u
#define MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM   1u  /* peer RESET_STREAM; stream_error_code is the peer's code */
#define MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING   2u  /* peer STOP_SENDING; stream_error_code is the peer's code */
#define MOQ_REQUEST_TERMINAL_MISSING_RESPONSE    3u  /* peer FIN before any required message */
#define MOQ_REQUEST_TERMINAL_TRUNCATED_RESPONSE  4u  /* peer FIN inside a partial message */
/* Lawful responder FIN after an established namespace subscription's required
 * messages. Emitted only by NAMESPACE_SUBSCRIPTION_TERMINATED, with local role
 * REQUESTER and stream_error_code zero (not a peer error). */
#define MOQ_REQUEST_TERMINAL_PEER_FIN_COMPLETE   5u
/* This side cancelled the request because it could not take what the
 * response delivered (a FETCH object over the local object-size limit).
 * Emitted only by FETCH_RESET; stream_error_code is the CANCELLED code this
 * side sent, not a peer code. */
#define MOQ_REQUEST_TERMINAL_LOCAL_CANCEL        6u
/* This side cancelled the request because the Track it carries is malformed
 * (draft-22 §12.1, draft-18 §2.4.2: the subscriber "MUST cancel any
 * corresponding subscription or fetches for that Track from that publisher,
 * and SHOULD deliver an error to the application"; draft-16 §2.4.2 has it
 * unsubscribe and cancel the fetch). The failure is the Track's, wherever it
 * was found: every request this side holds for that Track from the peer ends
 * this way -- each SUBSCRIBE, pending or established, under any alias; each
 * PUBLISH; each FETCH -- while requests for other Tracks go on. A request
 * whose terminal could not be queued at once ends on a later call, without
 * further input. stream_error_code (error_code on FETCH_RESET) is the code
 * this side sent, MALFORMED_TRACK (0x12), or zero on draft-16, whose cancel
 * messages carry no code. Emitted by UNSUBSCRIBED for a SUBSCRIBE,
 * PUBLISH_FINISHED for a PUBLISH and FETCH_RESET for a FETCH; a PUBLISH still
 * awaiting this side's answer is refused with UNINTERESTED
 * (MOQ_REQUEST_ERROR_UNINTERESTED, every draft) and its PUBLISH_FINISHED
 * carries that code. */
#define MOQ_REQUEST_TERMINAL_MALFORMED_TRACK     7u

/* This endpoint's side of the request the outcome names. */
typedef uint32_t moq_request_role_t;
#define MOQ_REQUEST_ROLE_REQUESTER 1u
#define MOQ_REQUEST_ROLE_RESPONDER 2u

/* -- Perspective --------------------------------------------------- */
/*
 * Transport-level endpoint perspective: who initiated the connection.
 * This is NOT a pub/sub role. MoQ allows both endpoints to publish
 * and subscribe; perspective only determines who initiates the
 * session setup handshake and the parity of locally minted
 * Request IDs.
 */

typedef enum moq_perspective {
    MOQ_PERSPECTIVE_CLIENT = 1,
    MOQ_PERSPECTIVE_SERVER = 2,
} moq_perspective_t;

/* -- Session state ------------------------------------------------- */

typedef enum moq_session_state {
    MOQ_SESS_IDLE          = 0,
    MOQ_SESS_SETUP_SENT    = 1,
    /* value 2 reserved */
    MOQ_SESS_ESTABLISHED   = 3,
    MOQ_SESS_DRAINING      = 4,
    MOQ_SESS_CLOSED        = 5,
} moq_session_state_t;

/* -- Session configuration ----------------------------------------- */

typedef struct moq_session_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;        /* required; copied into session */
    moq_perspective_t  perspective;   /* CLIENT or SERVER */
    bool               send_request_capacity;
    uint64_t           initial_request_capacity;

    /* Session resource limits. 0 = use library defaults. */
    uint32_t           max_actions;         /* action queue entries   (default 64;
                                             * below 4 the transport bridge
                                             * keys no data stream) */
    uint32_t           max_events;          /* event queue entries    (default 16) */
    uint32_t           send_buffer_size;    /* outbound bytes         (default 4096) */
    uint32_t           recv_buffer_size;    /* inbound bytes          (default 4096) */
    uint32_t           max_subscriptions;   /* subscription pool      (default 64) */
    uint32_t           output_scratch_size; /* event borrow scratch   (default 65536) */
    uint32_t           max_open_subgroups;  /* outgoing subgroup pool (default 64) */
    uint32_t           max_data_streams;    /* incoming data stream pool (default 64) */
    uint32_t           max_object_payload_size;  /* per-object limit bytes (default 4 MiB) */
    uint32_t           max_receive_buffer_bytes; /* retained rx buffer budget (default 16 MiB) */
    uint32_t           max_announcements;   /* announcement pool       (default 64) */

    bool               send_auth_token_cache_size;
    uint64_t           auth_token_cache_size;    /* advertise in setup; 0 = no aliases */
    bool               streaming_objects;  /* false=OBJECT_RECEIVED, true=OBJECT_CHUNK */

    uint64_t           goaway_timeout_us;  /* after this side sends GOAWAY: close with GOAWAY_TIMEOUT; 0 = disabled */

    uint32_t           max_namespace_subscriptions;  /* ns_sub pool (default 16) */

    /*
     * Reserved: covers the pre-version layout's tail padding after
     * max_namespace_subscriptions on targets where it had any.
     * `version` is placed AFTER this slot so its struct_size gate threshold lands
     * past that old sizeof -- otherwise an old caller passing sizeof(pre-version
     * struct) would have its uninitialized tail bytes read as `version`. This
     * field must never become a readable config field for the same reason. */
    uint32_t           _reserved_version;

    /*
     * MoQ wire profile for this session. 0 selects the draft-16 default.
     * The profile is fixed at create time and immutable thereafter. It is
     * chosen by transport version negotiation (ALPN for native MoQ-over-
     * QUIC and WebTransport-over-QUIC; the WebTransport protocol for H3
     * WebTransport) — the host/adapter sets this to match the negotiated
     * transport version. An unsupported value fails moq_session_create
     * with MOQ_ERR_INVAL; it is never silently downgraded. See
     * docs/transport-integration-guide.md ("MoQ version & ALPN selection").
     */
    moq_version_t      version;

    uint32_t           max_fetches;  /* fetch pool (default 16) */
    uint32_t           max_publishes;       /* publish pool (default 16) */
    uint32_t           max_track_statuses;  /* track status pool (default 8) */

    uint64_t           idle_timeout_us;    /* close after no activity; 0 = disabled */

    /* Appended (ABI-additive, draft-18 only): SUBSCRIBE_TRACKS pool size
     * (default 16). Old callers (smaller struct_size) get the default. */
    uint32_t           max_track_subscriptions;

    /* Appended (ABI-additive): capacity of the per-track largest-location
     * history registry, which records the largest published/received object
     * location per track (keyed by full track name) so the library can report
     * each track's largest object to subscribers and resolve relative
     * subscription filters correctly. Records are refcounted while empty
     * (unobserved) and reclaimed when their last referencing request releases;
     * once a largest has been observed the record persists until the session
     * is destroyed. When the registry is full a new record cannot be created:
     * a local caller gets MOQ_ERR_NOMEM, an inbound peer request that needs a
     * new record is rejected without terminating the session.
     * 0 = default (max_subscriptions + max_publishes + max_track_statuses).
     * Old callers (smaller struct_size) get the default. */
    uint32_t           max_track_history_records;

    /* Appended (ABI-safe, §9.8): bound on how long a deferred terminal done
     * (Stream Count gating) waits for its advertised data streams before
     * the session expires the wait, discards the remaining streams and
     * surfaces the terminal event. 0 selects the library default (30
     * seconds). The effective wait is never below the subscription's
     * negotiated delivery timeouts. Read only when struct_size covers this
     * field; older callers get the library default. */
    uint64_t           done_wait_timeout_us;

    /* Appended (ABI-additive): the maximum number of simultaneously active
     * peer-announced namespace suffixes tracked for one namespace subscription.
     * This is a per-subscription active-suffix cap; it does not replace the
     * session-wide receive-byte budget -- both apply. When a peer announces a
     * new unique suffix after this cap is full, the library does NOT close the
     * session: it surfaces NAMESPACE_GONE for the already-active suffixes,
     * retires only the offending namespace-subscription stream, and the session
     * stays ESTABLISHED. 0 selects the library default (4096). Read only when
     * struct_size covers this field; older callers get the default. */
    uint32_t           max_namespace_suffixes_per_subscription;

    /*
     * Reserved: the 4 bytes of tail padding the layout that ended at
     * max_namespace_suffixes_per_subscription left before its old sizeof on
     * ABIs that align this struct to 8 bytes. setup_auth_tokens is placed AFTER
     * this slot so its struct_size gate lands at or past that old sizeof on
     * every ABI. This field must never become a readable config field.
     */
    uint32_t           _reserved_setup_auth;

    /* Appended (ABI-additive): authorization tokens this side presents in
     * its SETUP, the AUTHORIZATION TOKEN Setup Option (draft-22 §9.1.4,
     * draft-18 §10.3.1.4) or setup parameter (draft-16 §9.3.1.5), which the
     * peer can use to authorize the session. Each is sent by value: nothing
     * can be registered before the session exists. The tokens and their values are
     * copied by moq_session_create; the caller's storage need not outlive
     * the call. At most 16 tokens, each token_type within the version's
     * encoding (at most 2^62-1 on draft-16, any uint64 on draft-18 and
     * draft-22) and each value at most 65535 bytes, and together they must
     * fit the setup message
     * (MOQ_ERR_INVAL at create otherwise). Read only when struct_size covers
     * setup_auth_token_count; older callers send none.
     *
     * Send-arena rule: the SETUP is queued in the send arena
     * (send_buffer_size) when the session starts, and each token takes its
     * value plus up to 32 bytes there, beside the other setup fields. The
     * facades that carry these tokens check that rule when they are created,
     * so a set that cannot be queued fails there rather than at start. */
    const moq_auth_token_t *setup_auth_tokens;
    size_t                  setup_auth_token_count;

    /* Appended (ABI-additive): the fill pool, the children a subscription may
     * have open on either side at once, independent of the FETCH pool. 0
     * selects the library default (16); the maximum is 65536. Read only when
     * struct_size covers this field; older callers get the default. */
    uint32_t           max_fills;

    /* Appended (ABI-additive): leave the answer to a subscriber's update of
     * a subscription this side publishes to the application. By default the
     * session acknowledges every update it can apply as it surfaces
     * MOQ_EVENT_SUBSCRIBE_UPDATED. When set, such an update is surfaced with
     * answer_owed and nothing it asks for takes effect until
     * moq_session_accept_subscription_update() applies and acknowledges it,
     * or moq_session_reject_subscription_update() refuses it and ends the
     * subscription with status UPDATE_FAILED. The same holds for a
     * subscription this side's PUBLISH began: MOQ_EVENT_PUBLISH_UPDATED
     * carries answer_owed, answered with
     * moq_session_accept_publication_update() or
     * moq_session_reject_publication_update(). Updates the session refuses
     * on its own (an unsupported parameter, a rejected authorization token)
     * are still refused immediately, and on either kind of subscription the
     * refusal ends it with status UPDATE_FAILED, failing any answer still
     * owed with it; the handle is then stale and no event names the
     * refused update. Read only when struct_size covers this
     * field; older callers get the automatic answer. Can also be switched
     * after creation with moq_session_set_defer_subscription_updates(). */
    bool               defer_subscription_updates;

    /* Appended (ABI-additive): leave the resolution of a FETCH range to the
     * application. By default the session resolves a range stated relative
     * to the Largest Object -- a relative or Next Object start, an open end,
     * no Location Filter at all -- against what this session itself
     * published on the track, and refuses the request with INVALID_RANGE
     * when it published nothing there or the start lies past it. A
     * component whose Largest Object lives outside the session (a relay,
     * whose Largest is its own log) sets this. Every standalone FETCH whose
     * profile carries its range as a Location Filter is then surfaced
     * unresolved (moq_fetch_request_options_t.range_unresolved, with the
     * request's form in .location), and the application answers it:
     * moq_session_accept_fetch() with the End Location it resolved -- an End
     * that contradicts the request is refused -- or
     * moq_session_reject_fetch() with INVALID_RANGE. Profiles whose FETCH
     * range is explicit are unaffected. Read only when struct_size covers
     * this field; older callers get the session's resolution. Can also be
     * switched after creation with moq_session_set_defer_fetch_ranges(). */
    bool               defer_fetch_ranges;
} moq_session_cfg_t;

#ifdef __cplusplus
#define MOQ_SESSION_CFG_INIT \
    ([]{moq_session_cfg_t c{}; c.struct_size=sizeof(c); return c;}())
#else
#define MOQ_SESSION_CFG_INIT \
    ((moq_session_cfg_t){ .struct_size = sizeof(moq_session_cfg_t) })
#endif

/*
 * Initialize a session config.
 *
 * moq_session_cfg_init() is pointer-only: it cannot know the caller's struct
 * size, so it clears and stamps ONLY the frozen v0 prefix (struct_size, alloc,
 * perspective) and leaves every later field untouched. A config produced this
 * way creates a session with library defaults for everything past the prefix --
 * any field beyond it (resource caps, streaming_objects, GOAWAY/idle timeouts,
 * version, fetch/publish pools, ...) requires moq_session_cfg_init_sized(),
 * which records the real struct_size so moq_session_create() reads those fields.
 *
 * moq_session_cfg_init_sized() clears and stamps min(cfg_size, current sizeof)
 * and is the initializer to use whenever you set anything beyond alloc /
 * perspective. Pass sizeof(moq_session_cfg_t) for cfg_size. (The
 * MOQ_SESSION_CFG_INIT compile-time initializer is equivalent to the sized form
 * for a caller compiled against this header.)
 */
MOQ_API void moq_session_cfg_init(moq_session_cfg_t *cfg,
                                   const moq_alloc_t *alloc,
                                   moq_perspective_t perspective);

MOQ_API void moq_session_cfg_init_sized(moq_session_cfg_t *cfg,
                                         size_t cfg_size,
                                         const moq_alloc_t *alloc,
                                         moq_perspective_t perspective);

/* -- Session lifecycle --------------------------------------------- */

/*
 * Create a session.
 *
 * Sessions start in IDLE. A draft-16 client calls moq_session_start() to
 * begin the setup handshake, and a draft-16 server is ready for the peer's
 * setup right after create. Draft-18 and draft-22 sessions call
 * moq_session_start() in both perspectives.
 *
 * Allocates: session state via cfg->alloc.
 * Ownership: caller owns *out until moq_session_destroy().
 */
MOQ_API moq_result_t moq_session_create(const moq_session_cfg_t *cfg,
                                         uint64_t now_us,
                                         moq_session_t **out);

/*
 * Destroy a session and free all internal state.
 * destroy(NULL) is a no-op. Double-destroy of non-NULL is misuse.
 * If the session is not CLOSED, this is an unclean shutdown.
 */
MOQ_API void moq_session_destroy(moq_session_t *s);

/* Current session state. Observing (does not invalidate borrows). */
MOQ_API moq_session_state_t moq_session_state(const moq_session_t *s);

/* Session perspective (CLIENT or SERVER). Returns 0 if s is NULL. */
MOQ_API moq_perspective_t moq_session_perspective(const moq_session_t *s);

/*
 * True when the negotiated draft permits a Track Namespace with ZERO fields —
 * the root namespace (draft-18 yes, draft-16 no, which requires 1..32). Pure
 * query: a caller forwarding a namespace onto this session asks the capability
 * instead of testing a draft version, so version details never leak into caller
 * logic. Observing — no advancement, no output, no borrow invalidation.
 *
 * This describes the Track Namespace only. It says nothing about Track
 * Namespace PREFIX cardinality: an empty prefix on a namespace subscription
 * stays legal in draft-16.
 *
 * False for a NULL session, and for a session that has no profile bound.
 */
MOQ_API bool moq_session_supports_zero_field_track_namespace(
    const moq_session_t *s);

/*
 * The wire profile this session was created for, fixed at creation and
 * immutable for its lifetime -- reading it before start, mid-handshake, at
 * ESTABLISHED and after close all yield the same value.
 *
 * This is NOT a negotiator. Transport version negotiation belongs to the
 * layer that owns ALPN / the WebTransport protocol token; a service endpoint
 * reports its outcome through moq_endpoint_negotiated_version() and creates
 * the session with the version it selected. This accessor reports what the
 * session is actually speaking, which is the same authority that validates
 * the bytes on the wire.
 *
 * Returns (moq_version_t)0 if s is NULL. Observing (does not invalidate
 * borrows, does not advance the session).
 */
MOQ_API moq_version_t moq_session_version(const moq_session_t *s);

/*
 * Record that an Object at {group_id, object_id} has been published or
 * received on the given track, advancing that track's largest-location
 * history (see moq_session_cfg_t.max_track_history_records). The library
 * feeds this automatically for objects written/received through the session;
 * a relay or a publisher restoring history across sessions calls it directly
 * to seed or advance the track's Largest Object.
 *
 * The largest is used to report each track's largest object to subscribers
 * and to resolve relative subscription filters (Largest Object / Next Group)
 * at message-processing time.
 *
 * `ns` parts and `track_name` are BORROWED for the duration of the call
 * (copied into the record's owned key). NOT an advancing call: it records
 * history only -- it does not advance the borrow epoch, consume time, or
 * produce actions/events (so it never invalidates borrowed spans from a prior
 * poll). Must run on the session's owning shard; NOT thread-safe against
 * concurrent session calls.
 *
 * Merge is monotonic in location order: a location not greater than the
 * track's current largest is a no-op success (idempotent under reorder /
 * retransmit). group_id and object_id are object-location components; the
 * maximum value is PROFILE-SPECIFIC (the session's wire version fixes it):
 * draft-16 caps a location at 2^62-1, draft-18/22 at 2^64-1 (their full wire
 * range). A component above the session's limit is rejected. Opening a raw
 * subgroup honors these limits for group/subgroup IDs. The publisher facade
 * and raw object-write paths still have 62-bit object-ID limits; full-width
 * object writes remain a separate follow-up. This history API and the
 * subscribe/update negotiation honor the profile-specific location bound.
 *
 * Returns:
 *   MOQ_OK              merged, or no-op (already <= current largest)
 *   MOQ_ERR_INVAL       NULL args, a malformed full track name (namespace
 *                       cardinality invalid for the selected profile, a
 *                       component or name with nonzero length but NULL data,
 *                       or an over-long full name), or a
 *                       location component above the session's profile limit
 *   MOQ_ERR_NOMEM       a new record is required but the registry is full
 *                       (max_track_history_records reached) or allocation
 *                       failed; nothing is recorded
 *   MOQ_ERR_WRONG_STATE the session is not active (before setup / closed)
 * On any error no record is created and no largest is changed.
 */
MOQ_API moq_result_t moq_session_note_object_published(
    moq_session_t *s,
    const moq_namespace_t *ns,
    moq_bytes_t track_name,
    uint64_t group_id,
    uint64_t object_id);

/* -- Inputs (advancing calls) -------------------------------------- */

/*
 * Begin the setup handshake. Transitions IDLE -> SETUP_SENT.
 *
 * Draft-16 is client only. Under draft-18 and draft-22 both perspectives
 * call it, and each endpoint begins its own setup without waiting for the
 * peer. When moq_session_binding_requirements() is nonzero (draft-22), the
 * session must first be bound with moq_session_bind_transport(); on draft-16
 * and draft-18 a binding is optional, and a raw-QUIC client bound before
 * start names its connection target in its setup.
 *
 * Returns MOQ_ERR_WRONG_STATE when the session is not IDLE, when a draft-16
 * server calls it, or when a required transport binding is missing.
 */
MOQ_API moq_result_t moq_session_start(moq_session_t *s, uint64_t now_us);

/*
 * Feed bytes received on the control channel.
 * The session parses control message envelopes, dispatches to the
 * appropriate handler, and queues actions/events.
 *
 * buf is borrowed for the duration of this call only.
 *
 * Calling with buf == NULL and len == 0 is valid: it processes any
 * already-buffered input without feeding new bytes. Prefer
 * moq_session_process_pending() for clarity.
 */
MOQ_API moq_result_t moq_session_on_control_bytes(moq_session_t *s,
                                                   const uint8_t *buf,
                                                   size_t len,
                                                   uint64_t now_us);

/*
 * Process already-buffered control input without feeding new bytes.
 * Use after draining outputs to retry a previously blocked operation.
 * Advancing call: invalidates borrows, takes now_us.
 */
MOQ_API moq_result_t moq_session_process_pending(moq_session_t *s,
                                                  uint64_t now_us);

/*
 * Advance virtual time without feeding transport bytes.
 *
 * This is the explicit "timer/tick" input. It invalidates existing
 * borrows just like every other advancing call. Fires any internal
 * deadlines whose time has arrived (e.g., GOAWAY drain timeout).
 *
 * The adapter should call this when moq_session_next_deadline_us()
 * returns a value <= current time.
 */
MOQ_API moq_result_t moq_session_tick(moq_session_t *s, uint64_t now_us);

/*
 * Feed bytes received on a data stream.
 *
 * stream_ref is an adapter-assigned opaque identifier for this QUIC
 * unidirectional stream. The session uses it to look up parse state.
 *
 * buf is borrowed for the duration of this call only. fin == true
 * means the stream has been cleanly closed after these bytes.
 *
 * Returns MOQ_ERR_WOULD_BLOCK in two distinguishable cases:
 *   - the session OWNS the stream (moq_session_has_transport_stream is
 *     true): the bytes (and FIN) were retained; drain the full queue and
 *     retry with
 *       moq_session_on_data_bytes(s, stream_ref, NULL, 0, false, now_us)
 *     which resumes the pending emission for the stream;
 *   - the session does NOT own the stream (a new stream it could not
 *     admit): nothing was retained; the caller must redeliver the SAME
 *     bytes and FIN later (an empty retry on an unknown ref is a no-op).
 *     moq_session_can_admit_data_stream() says when that is worthwhile.
 *
 * Advancing call: invalidates borrows, takes now_us.
 */
MOQ_API moq_result_t moq_session_on_data_bytes(moq_session_t *s,
                                                moq_stream_ref_t stream_ref,
                                                const uint8_t *buf,
                                                size_t len,
                                                bool fin,
                                                uint64_t now_us);

/*
 * Feed data via a refcounted buffer for zero-copy streaming receive.
 *
 * Like on_data_bytes, but takes a caller-owned moq_rcbuf_t instead of
 * a raw pointer. The session increfs or slices the rcbuf if it needs
 * to retain data; the caller may decref after this call returns.
 *
 * When eligible (streaming continuation chunks, no buffered data),
 * emitted OBJECT_CHUNK events carry slices of the input rcbuf
 * instead of copies. The adapter's transport buffer is released when
 * the last slice is decreffed via moq_event_cleanup.
 *
 * data may be NULL for zero-length or retry calls.
 *
 * Advancing call: invalidates borrows, takes now_us.
 */
MOQ_API moq_result_t moq_session_on_data_rcbuf(moq_session_t *s,
                                                moq_stream_ref_t stream_ref,
                                                moq_rcbuf_t *data,
                                                bool fin,
                                                uint64_t now_us);

/*
 * Notify the session that a data stream was reset by the peer.
 *
 * Releases internal parse state for stream_ref. Does not close the
 * session unless the reset violates an invariant.
 *
 * A reset owes the application a visible closure: OBJECT_CHUNK with a
 * RESET terminal when a streaming object was in flight, otherwise
 * SUBGROUP_RESET for a bound subgroup whose id is resolved. When that
 * output cannot be queued -- a full event queue, or a complete object
 * still owed ahead of it -- this returns MOQ_ERR_WOULD_BLOCK and the
 * obligation is RETAINED. The session is not closed and the stream is
 * not released.
 *
 * To complete it: drain output (poll events, freeing a slot) and CALL
 * THIS FUNCTION AGAIN with the SAME stream_ref and the SAME error_code,
 * until it returns something other than MOQ_ERR_WOULD_BLOCK. Any later
 * advancing drive on that stream -- including an empty
 * moq_session_on_data_bytes(ref, NULL, 0, false) re-feed -- also
 * completes the owed closure, but repeating this call is the recovery to
 * write: it is the same operation with its own arguments, and it does
 * not depend on which drive routes happen to service the obligation.
 *
 * MOQ_ERR_WOULD_BLOCK implies the reset was already RECORDED: the
 * obligation and its cause are captured before anything that can block.
 * So the FIRST error_code seen for a stream is the cause that is
 * reported, and a retry passing a different code does not replace it --
 * pass the original anyway, rather than relying on that.
 *
 * Advancing call: invalidates borrows, takes now_us.
 */
MOQ_API moq_result_t moq_session_on_data_reset(moq_session_t *s,
                                                moq_stream_ref_t stream_ref,
                                                uint64_t error_code,
                                                uint64_t now_us);

/*
 * Notify the session that the peer sent STOP_SENDING on a data stream
 * we opened. Queues RESET_DATA (the adapter must send RESET_STREAM)
 * and transitions the outgoing subgroup to RESETTING.
 *
 * Advancing call: invalidates borrows, takes now_us.
 */
MOQ_API moq_result_t moq_session_on_data_stop(moq_session_t *s,
                                               moq_stream_ref_t stream_ref,
                                               uint64_t error_code,
                                               uint64_t now_us);

/*
 * Report that a data stream we opened has reached the transport's "all data
 * committed" state: every byte up to and including its FIN has been
 * acknowledged by the peer. A subgroup whose SUBGROUP_DELIVERY_TIMEOUT runs
 * from its FIN stops its timer here instead of resetting the stream when it
 * expires. The transport, not the session, knows this fact; adapters report
 * it through moq_transport_bridge_on_local_stream_committed().
 *
 * Advancing when a subgroup was awaiting the report; a report for any other
 * stream changes nothing and returns MOQ_OK.
 */
MOQ_API moq_result_t moq_session_on_data_committed(moq_session_t *s,
                                                    moq_stream_ref_t stream_ref,
                                                    uint64_t now_us);

/*
 * Feed a received QUIC datagram to the session.
 *
 * The session decodes the datagram and emits an OBJECT_RECEIVED
 * event with datagram=true. buf is borrowed for the duration of the
 * call only.
 *
 * Advancing call: invalidates borrows, takes now_us.
 */
MOQ_API moq_result_t moq_session_on_datagram(moq_session_t *s,
                                              const uint8_t *data, size_t len,
                                              uint64_t now_us);

/*
 * Notify the session that the transport has closed externally.
 * Queues SESSION_CLOSED event without queuing a CLOSE_SESSION action
 * (the transport is already gone). Advancing call.
 */
MOQ_API moq_result_t moq_session_on_transport_close(moq_session_t *s,
                                                      uint64_t code,
                                                      uint64_t now_us);

/*
 * Hard-close the session locally (e.g. deny an application-level setup
 * auth check when only the session handle is in hand). NOT a graceful
 * drain -- for that use moq_session_goaway(). Transitions to CLOSED,
 * supersedes every still-queued action/event and outbound byte (queued
 * payload refs are released, rx stream buffers freed, subgroup/deadline
 * state cleared), then queues exactly one MOQ_ACTION_CLOSE_SESSION (the
 * adapter closes the transport with `code`/`reason`) and one
 * MOQ_EVENT_SESSION_CLOSED (the app observes the close).
 *
 * `reason` is NOT copied: if non-NULL it must point to storage that
 * remains valid until the CLOSE_SESSION action and SESSION_CLOSED event
 * have been polled (their reason spans borrow it) -- static/literal
 * strings are recommended. NULL means no reason phrase.
 *
 * Idempotent: on an already-CLOSED session returns MOQ_OK and leaves any
 * still-queued close outputs (their original code/reason) untouched.
 * MOQ_ERR_INVAL for NULL s. Advancing call: invalidates borrows, takes
 * now_us.
 */
MOQ_API moq_result_t moq_session_close(moq_session_t *s,
                                        uint64_t code,
                                        const char *reason,
                                        uint64_t now_us);

/* -- Borrow validation --------------------------------------------- */

/*
 * Returns true if the given borrow_epoch is still current for this
 * session (no advancing call has been made since the epoch was stamped).
 * Observing (does not invalidate borrows).
 */
MOQ_API bool moq_session_borrow_valid(const moq_session_t *s,
                                       uint64_t borrow_epoch);

/* -- Actions (outbound I/O instructions) --------------------------- */

/*
 * Action kind discriminator. Fixed-width uint32_t for ABI stability.
 * Values are never renumbered. Unknown kinds must be ignored.
 */
typedef uint32_t moq_action_kind_t;

#define MOQ_ACTION_SEND_CONTROL  1u
#define MOQ_ACTION_CLOSE_SESSION 2u
#define MOQ_ACTION_SEND_DATA     3u
#define MOQ_ACTION_RESET_DATA    4u
#define MOQ_ACTION_STOP_DATA          5u
#define MOQ_ACTION_OPEN_BIDI_STREAM   6u
#define MOQ_ACTION_SEND_BIDI_STREAM   7u
#define MOQ_ACTION_CLOSE_BIDI_STREAM  8u
#define MOQ_ACTION_SEND_DATAGRAM      9u
/*
 * Unidirectional control actions. Some protocol profiles carry control
 * messages on a dedicated unidirectional stream that the local endpoint opens,
 * rather than on a single shared bidirectional stream. OPEN_UNI_CONTROL opens
 * that stream and writes its first bytes; SEND_UNI_CONTROL appends further
 * control bytes to the already-open stream. (Draft-16 carries control on a
 * shared bidirectional stream and emits neither of these.)
 */
#define MOQ_ACTION_OPEN_UNI_CONTROL  10u
#define MOQ_ACTION_SEND_UNI_CONTROL  11u
/*
 * Bidirectional request-stream teardown. Profiles that carry each request on
 * its own bidirectional stream cancel a request by resetting and/or sending
 * STOP_SENDING on it (with a stream error code), rather than by a control
 * message. RESET_BIDI_STREAM resets the local send half; STOP_BIDI_STREAM asks
 * the peer to stop sending. These are distinct transport signals and are kept
 * separate from RESET_DATA / STOP_DATA (which target unidirectional data
 * streams). Profiles that carry requests on a shared control channel emit
 * neither.
 */
#define MOQ_ACTION_RESET_BIDI_STREAM 12u
#define MOQ_ACTION_STOP_BIDI_STREAM  13u
/*
 * Whole-request teardown: abort BOTH directions of a request bidi with
 * ONE error code. This is what request cancellation means on the wire —
 * the profiles emit it instead of an adjacent STOP+RESET pair, and the
 * bridge dispatches it as one transaction (a native endpoint
 * abort_stream when available, else reset+stop with explicit pending
 * state). Kinds 12/13 remain defined for future genuinely
 * single-half-only semantics; current profiles no longer emit them.
 */
#define MOQ_ACTION_ABORT_BIDI_STREAM 14u
/*
 * A data stream's scheduling key (moq_data_priority_t), right before its
 * first SEND_DATA. stream_ref 0 is an owner-wide subscriber priority update:
 * every stream of priority.owner takes priority.subscriber_priority. Only
 * after moq_session_set_data_priority_updates(s, true).
 */
#define MOQ_ACTION_SET_DATA_PRIORITY 15u

typedef struct moq_send_control_action {
    const uint8_t *data;  /* BORROWED until next advancing call */
    size_t         len;
} moq_send_control_action_t;

typedef struct moq_close_session_action {
    uint64_t    code;
    moq_bytes_t reason;  /* BORROWED: internal closes use static strings;
                            moq_session_close() borrows the caller's
                            reason (which must outlive this action's
                            poll). Valid until the next advancing call. */
} moq_close_session_action_t;

typedef struct moq_send_data_action {
    moq_stream_ref_t stream_ref;
    uint8_t          header[32];
    uint8_t          header_len;
    moq_rcbuf_t     *payload;       /* OWNED ref; adapter must cleanup */
    bool             fin;
    /* Scheduling snapshot, independent of the wire bytes. Zero means absent
     * (legacy producer). Otherwise 0x10000 | subscriber_priority << 8 |
     * publisher_priority. Lower subscriber priority takes precedence. This
     * snapshot survives subgroup retirement and transport backpressure.
     * Appended at uint64 alignment, beyond the old complete struct prefix. */
    uint64_t         scheduling_priority;
} moq_send_data_action_t;

typedef struct moq_set_data_priority_action {
    moq_stream_ref_t    stream_ref;
    moq_data_priority_t priority;
} moq_set_data_priority_action_t;

typedef struct moq_reset_data_action {
    moq_stream_ref_t stream_ref;
    uint64_t         error_code;
    /* Appended (read when detail_size covers it): how many leading bytes of
     * the stream the reset must still deliver -- the stream header as encoded
     * on this stream, so the receiver can tell which subscription, fetch or
     * fill the reset ends. The adapter uses it as far as its transport
     * allows: RESET_STREAM_AT with this reliable size where the connection
     * negotiated it, or a plain reset sent once these bytes are acknowledged;
     * otherwise, or when this is 0, the stream is reset with a plain
     * RESET_STREAM at once. Never more than the bytes the session queued on
     * the stream before the reset. */
    uint64_t         reliable_size;
} moq_reset_data_action_t;

typedef struct moq_stop_data_action {
    moq_stream_ref_t stream_ref;
    uint64_t         error_code;
} moq_stop_data_action_t;

typedef struct moq_open_bidi_stream_action {
    moq_stream_ref_t stream_ref;
    const uint8_t   *data;  /* BORROWED from send_buf */
    size_t           len;
    bool             fin;   /* close the local send half after the data (e.g. a
                             * TRACK_STATUS request, the first and only message) */
} moq_open_bidi_stream_action_t;

typedef struct moq_send_bidi_stream_action {
    moq_stream_ref_t stream_ref;
    const uint8_t   *data;  /* BORROWED from send_buf */
    size_t           len;
    bool             fin;
} moq_send_bidi_stream_action_t;

typedef struct moq_close_bidi_stream_action {
    moq_stream_ref_t stream_ref;
} moq_close_bidi_stream_action_t;

typedef struct moq_reset_bidi_stream_action {
    moq_stream_ref_t stream_ref;
    uint64_t         error_code;
} moq_reset_bidi_stream_action_t;

typedef struct moq_stop_bidi_stream_action {
    moq_stream_ref_t stream_ref;
    uint64_t         error_code;
} moq_stop_bidi_stream_action_t;

typedef struct moq_abort_bidi_stream_action {
    moq_stream_ref_t stream_ref;
    uint64_t         error_code;
} moq_abort_bidi_stream_action_t;

typedef struct moq_send_datagram_action {
    const uint8_t *data;
    size_t         len;
} moq_send_datagram_action_t;

/*
 * Open the local unidirectional control channel and write its first bytes.
 * Mirrors open_bidi_stream but for a dedicated unidirectional stream that
 * carries control messages. data is BORROWED from the session send buffer
 * (valid until the next advancing call).
 */
typedef struct moq_open_uni_control_action {
    moq_stream_ref_t stream_ref;
    const uint8_t   *data;  /* BORROWED from send_buf */
    size_t           len;
} moq_open_uni_control_action_t;

/*
 * Append control bytes to the already-open local unidirectional control
 * channel identified by stream_ref. fin closes the send direction (this
 * channel is normally long-lived; fin is only set on teardown). data is
 * BORROWED from the session send buffer (valid until the next advancing call).
 */
typedef struct moq_send_uni_control_action {
    moq_stream_ref_t stream_ref;
    const uint8_t   *data;  /* BORROWED from send_buf */
    size_t           len;
    bool             fin;
} moq_send_uni_control_action_t;

/*
 * Inline detail capacity frozen at v1.
 */
#define MOQ_ACTION_DETAIL_MAX 80

typedef struct moq_action {
    moq_action_kind_t kind;
    uint32_t          detail_size;
    uint64_t          borrow_epoch;
    union moq_action_detail {
        moq_send_control_action_t      send_control;
        moq_close_session_action_t     close_session;
        moq_send_data_action_t         send_data;
        moq_reset_data_action_t        reset_data;
        moq_stop_data_action_t         stop_data;
        moq_open_bidi_stream_action_t  open_bidi_stream;
        moq_send_bidi_stream_action_t  send_bidi_stream;
        moq_close_bidi_stream_action_t close_bidi_stream;
        moq_send_datagram_action_t     send_datagram;
        moq_open_uni_control_action_t  open_uni_control;
        moq_send_uni_control_action_t  send_uni_control;
        moq_reset_bidi_stream_action_t reset_bidi_stream;
        moq_stop_bidi_stream_action_t  stop_bidi_stream;
        moq_abort_bidi_stream_action_t abort_bidi_stream;
        moq_set_data_priority_action_t set_data_priority;
        uint8_t                        _reserved[MOQ_ACTION_DETAIL_MAX];
    } u;
} moq_action_t;

/*
 * Release owned resources in a polled action. Idempotent.
 * SEND_DATA: decrefs payload and sets it NULL.
 * All other kinds: no-op.
 * Adapters MUST call this after processing or dropping every polled action.
 */
MOQ_API void moq_action_cleanup(moq_action_t *action);

/*
 * Size-aware drain of pending actions.
 *
 * Returns MOQ_OK on success (including 0 actions and partial drains).
 * Returns MOQ_ERR_INVAL if element_size < fixed prefix.
 * Returns MOQ_ERR_ABI_MISMATCH if the head action carries owned
 *   resources (e.g. SEND_DATA with rcbuf payload) and element_size
 *   is too small to hold it. Queue is left untouched.
 *
 * If a prefix of non-owned actions can be drained but the next action
 * is too large, returns MOQ_OK with the partial count and leaves the
 * owned action queued for a later full-size poll.
 *
 * Owned SEND_DATA payload refs transfer to the caller on poll.
 * Caller MUST call moq_action_cleanup after processing each action.
 *
 * Order: data actions (SEND_DATA, SEND_DATAGRAM) queued ahead of the next
 * action of any other kind come out as the scheduling algorithm places
 * them: the higher subscriber priority first, then the higher publisher
 * priority, then within one request the subscription's group order between
 * groups, and within a group the fill-delivered object, a datagram before a
 * subgroup object, and the lowest Subgroup ID or Object ID. Actions on one
 * stream keep their relative order; ties keep queue order; every other kind
 * of action holds its place and data queued after it does not overtake it.
 *
 * A queued object whose OBJECT_DELIVERY_TIMEOUT has elapsed at the session's
 * current time is never handed over: a datagram is dropped, and a subgroup
 * object is dropped together with the rest of its stream's queued data and
 * the stream is reset with DELIVERY_TIMEOUT (a RESET_DATA is queued and
 * polled in turn).
 *
 * Observing (does not invalidate borrows).
 */
MOQ_API moq_result_t moq_session_poll_actions_ex(moq_session_t *s,
                                                  void *out,
                                                  size_t cap,
                                                  size_t element_size,
                                                  size_t *out_count);

/* Convenience wrapper. */
static inline size_t
moq_session_poll_actions(moq_session_t *s, moq_action_t *out, size_t cap)
{
    size_t n = 0;
    moq_session_poll_actions_ex(s, out, cap, sizeof(moq_action_t), &n);
    return n;
}

/*
 * Number of action slots currently available for queuing.
 */
MOQ_API size_t moq_session_action_capacity(const moq_session_t *s);

/*
 * Opt in to MOQ_ACTION_SET_DATA_PRIORITY (default off, so older action
 * consumers never see it); the transport bridge turns it on. Call it before
 * any data stream opens. A key rides beside the actions a call already
 * queues, so MOQ_ERR_INVAL (and no change) when max_actions is below 4.
 */
MOQ_API moq_result_t moq_session_set_data_priority_updates(moq_session_t *s,
                                                           bool enabled);

/* -- Events (application notifications) ---------------------------- */

/*
 * Event kind discriminator. Fixed-width uint32_t for ABI stability.
 * Values are never renumbered. Unknown kinds must be ignored.
 */
typedef uint32_t moq_event_kind_t;

#define MOQ_EVENT_SETUP_COMPLETE    1u
#define MOQ_EVENT_SESSION_CLOSED    2u
#define MOQ_EVENT_SUBSCRIBE_REQUEST 3u
#define MOQ_EVENT_SUBSCRIBE_OK      4u
#define MOQ_EVENT_SUBSCRIBE_ERROR   5u
#define MOQ_EVENT_REQUEST_READY     6u
#define MOQ_EVENT_OBJECT_RECEIVED   7u
#define MOQ_EVENT_NAMESPACE_PUBLISHED   8u
#define MOQ_EVENT_NAMESPACE_DONE        9u
#define MOQ_EVENT_NAMESPACE_REJECTED   10u
#define MOQ_EVENT_NAMESPACE_ACCEPTED   11u
#define MOQ_EVENT_NAMESPACE_CANCELLED  12u
#define MOQ_EVENT_GOAWAY               13u
#define MOQ_EVENT_UNSUBSCRIBED         14u
#define MOQ_EVENT_SUBSCRIBE_UPDATED    15u
#define MOQ_EVENT_OBJECT_CHUNK         16u
#define MOQ_EVENT_NS_SUB_REQUEST       17u
#define MOQ_EVENT_NS_SUB_OK            18u
#define MOQ_EVENT_NS_SUB_ERROR         19u
#define MOQ_EVENT_NAMESPACE_FOUND      20u
#define MOQ_EVENT_NAMESPACE_GONE       21u
#define MOQ_EVENT_FETCH_REQUEST        22u
#define MOQ_EVENT_FETCH_ERROR          23u
#define MOQ_EVENT_FETCH_CANCELLED      24u
#define MOQ_EVENT_FETCH_OK             25u
#define MOQ_EVENT_FETCH_COMPLETE       26u
#define MOQ_EVENT_FETCH_OBJECT         27u
#define MOQ_EVENT_FETCH_GAP            28u
#define MOQ_EVENT_PUBLISH_REQUEST      29u
#define MOQ_EVENT_PUBLISH_OK           30u
#define MOQ_EVENT_PUBLISH_ERROR        31u
#define MOQ_EVENT_PUBLISH_FINISHED     32u
#define MOQ_EVENT_TRACK_STATUS_REQUEST 33u
#define MOQ_EVENT_TRACK_STATUS_OK      34u
#define MOQ_EVENT_TRACK_STATUS_ERROR   35u
#define MOQ_EVENT_SUBSCRIBE_DONE       36u
#define MOQ_EVENT_PUBLISH_UNSUBSCRIBED 37u
#define MOQ_EVENT_PUBLISH_UPDATED      38u
#define MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST   39u
#define MOQ_EVENT_SUBSCRIBE_TRACKS_OK        40u
#define MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR     41u
#define MOQ_EVENT_PUBLISH_BLOCKED            42u
#define MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED 43u
#define MOQ_EVENT_REQUEST_REDIRECT           44u
#define MOQ_EVENT_REQUEST_GOAWAY             45u
#define MOQ_EVENT_SUBGROUP_FINISHED          46u
/*
 * Response counterparts to the inbound update-request events
 * (MOQ_EVENT_SUBSCRIBE_UPDATED 15 / MOQ_EVENT_PUBLISH_UPDATED 38, which fire on
 * the *publisher* side when a peer's update request arrives). These fire on the
 * *original requester* side when the peer accepts an outbound update we sent.
 * Success only -- a rejected update drives the existing terminal subscribe-done
 * path, never a *_UPDATE_OK. Emitted exactly once per accepted update. Detail is
 * plain scalars (no borrowed spans), so safe to read after the next advancing
 * call.
 */
#define MOQ_EVENT_SUBSCRIPTION_UPDATE_OK     47u
#define MOQ_EVENT_PUBLICATION_UPDATE_OK      48u
#define MOQ_EVENT_SUBGROUP_RESET             49u
/*
 * Fetcher-side request-local stream terminal: the peer abruptly terminated a
 * stream of an identified, uncompleted FETCH -- its response data stream
 * (data_stream = true, always a RESET_STREAM) or, on stream-correlated request
 * profiles, its request stream (data_stream = false; stop_sending tells
 * STOP_SENDING from RESET_STREAM). RESET names the terminal category, not a
 * claim that every input was RESET_STREAM. error_code is the peer's QUIC
 * STREAM application error code exactly as received; it is NOT a MoQT
 * request error. Terminal: the fetch handle is retired before this event is
 * polled (later cancel/use -> MOQ_ERR_STALE_HANDLE); objects queued earlier
 * for the same handle remain pollable. Emitted exactly once per fetch and
 * never after a FETCH_ERROR, REQUEST_REDIRECT or FETCH_COMPLETE for the same
 * handle. A data-stream reset is attributed only through the FETCH_HEADER the
 * stream presented: a reset data stream whose header never arrived carries no
 * request identity, so nothing is invented and nothing is reported for it.
 * The request-stream terminal needs no data header: it is attributed by the
 * request stream itself and may arrive before any data.
 *
 * The same event reports this side's own cancel of the fetch when the
 * response delivered an object it cannot take (over the local object-size
 * limit): cause MOQ_REQUEST_TERMINAL_LOCAL_CANCEL, data_stream = true,
 * error_code the CANCELLED code this side sent. The session continues.
 */
#define MOQ_EVENT_FETCH_RESET                50u
/*
 * Draft-22 TRACK_STATUS ended abnormally before a response or migration.
 * Request-local, not a peer error response. See the typed scalar detail below.
 */
#define MOQ_EVENT_TRACK_STATUS_TERMINATED    51u
/* Fill children of a subscription (see fill.h). FILL_CREATED: the subscriber
 * side's child exists for a fill it requested, before any stream. FILL_REQUEST:
 * the publisher side owes a fill; the application opens, writes and ends it, or
 * aborts it. FILL_OBJECT / FILL_GAP: fill-delivered content on the subscriber
 * side. FILL_TERMINAL: the child's end, exactly once per child, after which its
 * handle is identifier-only. */
#define MOQ_EVENT_FILL_CREATED               52u
#define MOQ_EVENT_FILL_REQUEST               53u
#define MOQ_EVENT_FILL_OBJECT                54u
#define MOQ_EVENT_FILL_GAP                   55u
#define MOQ_EVENT_FILL_TERMINAL              56u
/* Draft-22 announcement terminal; distinct from legacy DONE/CANCELLED. */
#define MOQ_EVENT_NAMESPACE_TERMINATED        57u
/* The publisher reports selected subscription state and its known Largest
 * Object. Informative; nothing is owed back. A notification does not authorize
 * changing subscriber-controlled Forward or Location Filter parameters. */
#define MOQ_EVENT_PUBLISH_STATE               58u
/* Namespace-subscription terminal, not an announcement or track status. */
#define MOQ_EVENT_NAMESPACE_SUBSCRIPTION_TERMINATED 59u
/* Draft-22 updates of the three namespace-scoped requests (§9.5, §9.5.2),
 * one kind per typed owner. *_UPDATE_REQUEST: the peer updated a
 * request this side answers and the application owes the decision
 * (moq_session_set_defer_namespace_updates). *_UPDATE_RESULT: an update
 * concluded -- the peer's answer to an update this side sent, or this side's
 * automatic refusal of a peer update. See moq_namespace_update_record_t. */
#define MOQ_EVENT_NAMESPACE_UPDATE_REQUEST          60u
#define MOQ_EVENT_NS_SUB_UPDATE_REQUEST             61u
#define MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST   62u
#define MOQ_EVENT_NAMESPACE_UPDATE_RESULT           63u
#define MOQ_EVENT_NS_SUB_UPDATE_RESULT              64u
#define MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT    65u

/* Resolved authorization token (stable app API, NOT wire).
 * token_value is BORROWED from output scratch, follows borrow epoch. */
typedef struct moq_resolved_token {
    uint64_t    token_type;
    moq_bytes_t token_value;  /* BORROWED from output scratch */
} moq_resolved_token_t;

typedef struct moq_setup_complete_event {
    moq_perspective_t local_perspective;
    moq_perspective_t peer_perspective;
    const moq_resolved_token_t *tokens;  /* BORROWED; NULL if none */
    size_t                       token_count;
} moq_setup_complete_event_t;

typedef struct moq_session_closed_event {
    uint64_t    code;
    moq_bytes_t reason;  /* BORROWED; .data may be NULL if .len == 0 */
} moq_session_closed_event_t;

/* -- Object status (semantic, not wire codes) ---------------------- */
/*
 * Semantic object status codes for MOQ_EVENT_OBJECT_RECEIVED.
 * These are NOT wire codes (which live behind <moq/codec.h>).
 */

typedef uint8_t moq_object_status_t;

#define MOQ_OBJECT_NORMAL        ((moq_object_status_t)0)
#define MOQ_OBJECT_END_OF_GROUP  ((moq_object_status_t)1)
#define MOQ_OBJECT_END_OF_TRACK  ((moq_object_status_t)2)

/* -- Subscribe filter (semantic, not wire codes) ------------------- */

typedef uint32_t moq_subscribe_filter_t;

#define MOQ_SUBSCRIBE_FILTER_NONE            0u
#define MOQ_SUBSCRIBE_FILTER_NEXT_GROUP      1u
#define MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT  2u
#define MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START  3u
#define MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE  4u

/* -- Group order --------------------------------------------------- */

typedef uint32_t moq_group_order_t;

#define MOQ_GROUP_ORDER_DEFAULT    0u
#define MOQ_GROUP_ORDER_ASCENDING  1u
#define MOQ_GROUP_ORDER_DESCENDING 2u

/* -- Request error codes (semantic, for reject_subscribe) ---------- */

typedef uint64_t moq_request_error_t;

/*
 * The codes below are the drafts' own registries -- draft-16 §13.4.2,
 * draft-18 §15.10.2 and draft-22 §16.11.2 -- not an abbreviated convenience
 * list. This type is 64
 * bits wide because the protocol's error-code domain is, and the largest
 * value a given protocol version can actually carry differs between
 * versions; a reject/cancel call refuses a code its session cannot express
 * rather than silently altering it.
 *
 * Codes present in the draft-16 and draft-18 registries; draft-22 drops two
 * of them (see below).
 */
#define MOQ_REQUEST_ERROR_INTERNAL_ERROR         0x0u
#define MOQ_REQUEST_ERROR_UNAUTHORIZED           0x1u
#define MOQ_REQUEST_ERROR_TIMEOUT                0x2u
#define MOQ_REQUEST_ERROR_NOT_SUPPORTED          0x3u
#define MOQ_REQUEST_ERROR_MALFORMED_AUTH_TOKEN   0x4u
#define MOQ_REQUEST_ERROR_EXPIRED_AUTH_TOKEN     0x5u
#define MOQ_REQUEST_ERROR_DOES_NOT_EXIST         0x10u
#define MOQ_REQUEST_ERROR_INVALID_RANGE          0x11u
#define MOQ_REQUEST_ERROR_MALFORMED_TRACK        0x12u
#define MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION     0x19u
#define MOQ_REQUEST_ERROR_UNINTERESTED               0x20u
#define MOQ_REQUEST_ERROR_PREFIX_OVERLAP             0x30u
#define MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID 0x32u

/*
 * Registered by draft-18 and draft-22, not by draft-16. A draft-16 peer does
 * not know them, so a draft-16 session treats them like any other unknown
 * value.
 */
#define MOQ_REQUEST_ERROR_GOING_AWAY                 0x6u
#define MOQ_REQUEST_ERROR_EXCESSIVE_LOAD             0x9u
#define MOQ_REQUEST_ERROR_NAMESPACE_TOO_LARGE        0x31u
#define MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION      0x33u
#define MOQ_REQUEST_ERROR_REDIRECT                   0x34u

/*
 * Registered by draft-22 (§16.11.2), which also drops DUPLICATE_SUBSCRIPTION
 * (0x19) and INVALID_JOINING_REQUEST_ID (0x32): a draft-22 session treats
 * those two as unknown. CONFLICTING_FILTERS answers a SUBSCRIBE_TRACKS whose
 * filters conflict among too many subscribers to aggregate upstream;
 * INVALID_FILTER answers a filter parameter the receiver cannot honour (a
 * Range Filter beyond its advertised limit, an undecodable range).
 */
#define MOQ_REQUEST_ERROR_CONFLICTING_FILTERS        0x35u
#define MOQ_REQUEST_ERROR_INVALID_FILTER             0x36u

/*
 * Draft-18 §14 also reserves 0x7f*N + 0x9D for greasing. Those are
 * deliberately NOT exposed as semantic constants: they carry no meaning, and
 * a draft-18 session treats them as unknown per §15's unknown-code rule.
 */

/* -- Stream reset error codes --------------------------------------- */

/*
 * The application error code a data stream carries in RESET_STREAM or
 * STOP_SENDING: draft-16 §10.4, draft-18 §3.3.3, draft-22 §12.5. These are
 * wire values passed through unchanged, so a caller resetting a stream uses
 * the value its session's draft registers.
 *
 * Codes with the same value in all three registries.
 */
#define MOQ_STREAM_ERROR_INTERNAL_ERROR         0x0u
#define MOQ_STREAM_ERROR_CANCELLED              0x1u
#define MOQ_STREAM_ERROR_DELIVERY_TIMEOUT       0x2u
#define MOQ_STREAM_ERROR_SESSION_CLOSED         0x3u
#define MOQ_STREAM_ERROR_MALFORMED_TRACK        0x12u

/*
 * Registered by draft-18 and draft-22. Draft-16 registers none of these, and
 * gives 0x4 a different meaning: its UNKNOWN_OBJECT_STATUS is
 * MOQ_STREAM_ERROR_D16_UNKNOWN_OBJECT_STATUS below.
 */
#define MOQ_STREAM_ERROR_GOING_AWAY             0x4u
#define MOQ_STREAM_ERROR_TOO_FAR_BEHIND         0x5u
#define MOQ_STREAM_ERROR_UNKNOWN_OBJECT_STATUS  0x6u
#define MOQ_STREAM_ERROR_EXPIRED_AUTH_TOKEN     0x7u
#define MOQ_STREAM_ERROR_EXCESSIVE_LOAD         0x9u

/* Draft-16's value for UNKNOWN_OBJECT_STATUS. */
#define MOQ_STREAM_ERROR_D16_UNKNOWN_OBJECT_STATUS 0x4u

/* -- Subscribe event detail structs -------------------------------- */

#define MOQ_SUBSCRIPTION_ADMISSION_LOCATION                  0x01u
#define MOQ_SUBSCRIPTION_ADMISSION_FILL                      0x02u
#define MOQ_SUBSCRIPTION_ADMISSION_OBJECT_DELIVERY_TIMEOUT   0x04u
#define MOQ_SUBSCRIPTION_ADMISSION_SUBGROUP_DELIVERY_TIMEOUT 0x08u
#define MOQ_SUBSCRIPTION_ADMISSION_RENDEZVOUS_TIMEOUT         0x10u
#define MOQ_SUBSCRIPTION_ADMISSION_OPTIONS_V0_SIZE           72u

/* Borrowed for the containing event's epoch. The complete frozen prefix can
 * be copied by value; it contains no pointers or owned resources. Flags name
 * fields actually selected by the SUBSCRIBE, not effective defaults. Location
 * is always initialized; explicit NONE retains LOCATION, omission does not.
 * FILL reports the request even if bounded child admission later refuses it.
 * Independent timeouts are exact milliseconds (including 0 and UINT64_MAX);
 * an absent timeout is zero with its flag clear. No unknown flags are emitted. */
typedef struct moq_subscription_admission_options {
    uint32_t struct_size;
    uint32_t flags;
    moq_location_filter_cfg_t location;
    uint64_t object_delivery_timeout_ms;
    uint64_t subgroup_delivery_timeout_ms;
    /* After the frozen72-byte prefix. Exact peer milliseconds, including zero
     * and UINT64_MAX, when RENDEZVOUS_TIMEOUT is selected; otherwise zero.
     * This is source-discovery intent, not an object/FILL delivery timeout. */
    uint64_t rendezvous_timeout_ms;
} moq_subscription_admission_options_t;

typedef struct moq_subscribe_request_event {
    moq_subscription_t     sub;
    moq_namespace_t        track_namespace;  /* BORROWED from output scratch */
    moq_bytes_t            track_name;       /* BORROWED from output scratch */
    moq_subscribe_filter_t filter;
    uint8_t                subscriber_priority;
    moq_group_order_t      group_order;
    bool                   forward;
    uint8_t                _pad[3];
    uint64_t               start_group;
    uint64_t               start_object;
    uint64_t               end_group;
    uint64_t               delivery_timeout_us;
    const moq_resolved_token_t *tokens;  /* BORROWED; NULL if none */
    size_t                       token_count;
    /* Appended: the subscriber asks the original publisher to start a new
     * group (largest known Group ID + 1; 0 = "no group info"). Surfaced
     * only -- whether and when to start a group is application policy. */
    bool                         has_new_group_request;
    uint64_t                     new_group_request;
    /* After the complete legacy detail. NULL on draft-16/18, non-NULL on
     * draft-22 including an empty parameter set. Read only when detail_size
     * covers this pointer; options are the lossless parameter observation,
     * whereas the legacy filter/timeout fields above are projections. */
    const moq_subscription_admission_options_t *options;
} moq_subscribe_request_event_t;

typedef struct moq_subscribe_ok_event {
    moq_subscription_t sub;
    uint64_t           track_alias;
    bool               has_largest;
    uint8_t            _pad1[7];
    uint64_t           largest_group;
    uint64_t           largest_object;
    bool               has_expires;
    uint8_t            _pad2[7];
    uint64_t           expires_ms;
    moq_bytes_t        track_properties;  /* BORROWED from output scratch */
    /* Appended: the track supports dynamically started groups (Track
     * Property DYNAMIC_GROUPS == 1); omitted on the wire means false. When
     * true, new-group requests may ride this subscription's updates. */
    bool               dynamic_groups;
    /* Appended: the publisher's DEFAULT_PUBLISHER_GROUP_ORDER, or
     * MOQ_GROUP_ORDER_DEFAULT if it advertised none. */
    moq_group_order_t  publisher_group_order;
} moq_subscribe_ok_event_t;

typedef struct moq_subscribe_error_event {
    moq_subscription_t  sub;
    moq_request_error_t error_code;
    bool                can_retry;
    uint8_t             _pad[7];
    uint64_t            retry_after_ms;
    moq_bytes_t         reason;  /* BORROWED from output scratch */
} moq_subscribe_error_event_t;

typedef struct moq_request_ready_event {
    uint64_t available_requests;
} moq_request_ready_event_t;

typedef struct moq_object_received_event {
    moq_subscription_t sub;
    moq_publication_t  pub;   /* set when object is from a publisher-initiated subscription */
    uint64_t           group_id;
    uint64_t           subgroup_id;
    uint64_t           object_id;
    uint8_t            publisher_priority;
    moq_object_status_t status;
    bool               end_of_group;  /* subgroup header END_OF_GROUP bit */
    bool               datagram;      /* true if delivered via datagram */
    /* The subgroup header set FIRST_OBJECT and this is the first object its
     * stream carried: the first object ever published in the subgroup, so no
     * earlier object of the subgroup exists. False on every later object of
     * that stream, on datagrams, and on drafts without the bit. */
    bool               first_object;
    uint8_t            _pad[3];
    moq_rcbuf_t       *payload;     /* OWNED ref transferred on poll; NULL for status objects */
    moq_rcbuf_t       *properties;  /* OWNED ref transferred on poll; NULL if none */
    /* Appended: the Prior Group ID Gap (0x3C) and Prior Object ID Gap (0x3E)
     * Object Properties (draft-22 §10.8, §10.9; draft-18 §12.8, §12.9), as
     * received. A gap of N says the N Groups before this Group, or the N
     * Objects before this Object in its Group, do not and will never exist.
     * Read only when detail_size covers prior_object_gap; the raw properties
     * still carry them. */
    bool               has_prior_group_gap;
    bool               has_prior_object_gap;
    /* Appended with the gaps: the subgroup header that carried this object set
     * PROPERTIES, so every object of its stream had a Properties field (a
     * zero length for none). False for a datagram. Lets a relay mirror the
     * upstream stream's form. */
    bool               subgroup_properties;
    uint8_t            _pad_gap[5];
    uint64_t           prior_group_gap;
    uint64_t           prior_object_gap;
} moq_object_received_event_t;

/*
 * A subgroup data stream ended gracefully with a FIN. Emitted after the
 * subgroup header has been parsed and every buffered object on the stream has
 * been delivered -- i.e. strictly AFTER the final OBJECT_RECEIVED / OBJECT_CHUNK
 * of that subgroup. Lets a receiver/relay reclaim per-subgroup state on a clean
 * end without inferring it from object IDs.
 *
 * Exactly one of `sub` / `pub` is valid, matching the OBJECT_RECEIVED events of
 * the same subgroup (sub for a subscriber-role stream, pub for a
 * publisher-initiated one). Not emitted for FETCH streams, for streams reset via
 * RESET_STREAM / STOP_SENDING, or for streams that FIN before a usable subgroup
 * header was parsed. Owns no resources (moq_event_cleanup is a no-op).
 */
typedef struct moq_subgroup_finished_event {
    moq_subscription_t sub;
    moq_publication_t  pub;
    uint64_t           group_id;
    uint64_t           subgroup_id;
    bool               end_of_group;  /* subgroup header END_OF_GROUP bit */
    uint8_t            _pad[7];
} moq_subgroup_finished_event_t;

/*
 * A subgroup data stream ended abnormally with a RESET_STREAM: the abnormal
 * counterpart of SUBGROUP_FINISHED, which a receiver/relay uses to close its
 * per-subgroup state.
 *
 * A reset has exactly TWO non-overlapping surfaces, chosen by whether an
 * application-visible object terminal can carry the closure -- for one reset
 * exactly one of them fires:
 *
 *   - A streaming object is in flight AND its object events are surfaced:
 *     the closure IS that object's OBJECT_CHUNK with
 *     terminal=MOQ_OBJECT_TERMINAL_RESET, carrying the peer's code in the
 *     chunk's error_code. This event is NOT emitted.
 *   - Otherwise, for a bound subgroup whose ID is resolved, no visible object
 *     terminal can carry the closure, so THIS event reports it. That covers
 *     whole-object mode (a partial payload is dropped; a complete one is
 *     delivered as OBJECT_RECEIVED first, in order, and this event follows),
 *     a streaming stream between objects or after its last object completed,
 *     and a streaming object whose events are suppressed by Forward State 0
 *     (its parsed header resolves the subgroup identity even though no object
 *     event is surfaced for it).
 *
 * Exactly one of `sub` / `pub` is valid, matching the OBJECT_RECEIVED events of
 * the same subgroup. `error_code` is the RESET_STREAM application error code:
 * the FIRST code the peer sent for this stream, retained across event-queue
 * backpressure so a re-driven terminal reports the original cause. Emitted at
 * most once per stream.
 *
 * Not emitted for FETCH streams, for gracefully FINed streams (those emit
 * SUBGROUP_FINISHED), or for a stream reset before its subgroup ID was
 * resolved -- a FIRST_OBJECT-mode header that never carried an object has only
 * the decoder default, and reporting it would let a relay seal subgroup 0 by
 * mistake. A suppressed object's parsed header DOES resolve the identity, so
 * that case still reports. Owns no resources (moq_event_cleanup is a no-op).
 */
typedef struct moq_subgroup_reset_event {
    moq_subscription_t sub;
    moq_publication_t  pub;
    uint64_t           group_id;
    uint64_t           subgroup_id;
    uint64_t           error_code;    /* RESET_STREAM application error code */
    bool               end_of_group;  /* subgroup header END_OF_GROUP bit */
    uint8_t            _pad[7];
} moq_subgroup_reset_event_t;

/* -- Namespace event detail structs -------------------------------- */

typedef struct moq_namespace_published_event {
    moq_announcement_t ann;
    moq_namespace_t    track_namespace;  /* BORROWED from output scratch */
    const moq_resolved_token_t *tokens;  /* BORROWED; NULL if none */
    size_t                       token_count;
} moq_namespace_published_event_t;

typedef struct moq_namespace_done_event {
    moq_announcement_t ann;
} moq_namespace_done_event_t;

typedef struct moq_namespace_rejected_event {
    moq_announcement_t  ann;
    moq_request_error_t error_code;
    bool                can_retry;
    uint8_t             _pad[7];
    uint64_t            retry_after_ms;
    moq_bytes_t         reason;  /* BORROWED from output scratch */
} moq_namespace_rejected_event_t;

typedef struct moq_namespace_accepted_event {
    moq_announcement_t ann;
} moq_namespace_accepted_event_t;

typedef struct moq_namespace_cancelled_event {
    moq_announcement_t  ann;
    moq_request_error_t error_code;
    moq_bytes_t         reason;  /* BORROWED from output scratch */
} moq_namespace_cancelled_event_t;

typedef uint32_t moq_namespace_terminal_cause_t;
#define MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE 1u
#define MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM 2u
#define MOQ_NAMESPACE_TERMINAL_PEER_STOP_SENDING 3u
#define MOQ_NAMESPACE_TERMINAL_MISSING_RESPONSE 4u
#define MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE 5u
#define MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY 6u
typedef uint32_t moq_namespace_role_t;
#define MOQ_NAMESPACE_ROLE_REQUESTER 1u
#define MOQ_NAMESPACE_ROLE_RESPONDER 2u

/* Frozen 24-byte scalar detail, offsets 0/8/12/16. RESET/STOP carry the exact
 * peer stream error, including zero; other causes carry zero without error
 * meaning. LOCAL_CAPACITY is inability to project a complete required local
 * observation, not peer rejection. Terminal commitment makes ann stale;
 * private stream cleanup may remain. No pointers or owned resources.
 * Full reading needs a 40-byte event element AND detail_size >= 24: the latter
 * describes the producer extent, not the number of bytes copied on poll. */
typedef struct moq_namespace_terminated_event {
    moq_announcement_t ann;
    moq_namespace_terminal_cause_t cause;
    moq_namespace_role_t local_role;
    uint64_t stream_error_code;
} moq_namespace_terminated_event_t;

/*
 * The peer sent a session-wide GOAWAY. The session is DRAINING; it
 * never closes itself because of a GOAWAY it received: GOAWAY_TIMEOUT is the
 * code of the GOAWAY's sender, which closes the session once its Timeout has
 * passed (draft-22 §9.2, draft-18 §10.4, draft-16 §3.5). The receiver
 * migrates and closes with NO_ERROR when it is done.
 *
 * has_timeout / timeout_ms (appended; read when detail_size covers
 * timeout_ms): the Timeout the sender stated, in milliseconds, on the
 * profiles whose GOAWAY carries one (draft-18 and draft-22). Zero means the
 * sender named no specific timeout and the receiver should migrate as
 * quickly as possible. has_timeout is false on draft-16.
 */
typedef struct moq_goaway_event {
    moq_bytes_t new_session_uri;  /* BORROWED from output scratch; empty if reuse current URI */
    bool        has_timeout;
    uint8_t     _pad[7];
    uint64_t    timeout_ms;
} moq_goaway_event_t;

/* The request family a migration signal applies to. Redirect (§10.6) permits only
 * the first five (subscribe, fetch, track-status, namespace announcement, namespace
 * subscription); a per-request GOAWAY (§10.4) applies to any request bidi, adding
 * publish and subscribe-tracks. */
typedef enum moq_request_family {
    MOQ_REQUEST_FAMILY_SUBSCRIBE        = 1,
    MOQ_REQUEST_FAMILY_FETCH            = 2,
    MOQ_REQUEST_FAMILY_TRACK_STATUS     = 3,
    MOQ_REQUEST_FAMILY_ANNOUNCEMENT     = 4,   /* moq_session_publish_namespace */
    MOQ_REQUEST_FAMILY_NS_SUB           = 5,   /* moq_session_subscribe_namespace */
    MOQ_REQUEST_FAMILY_PUBLISH          = 6,   /* moq_session_publish */
    MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS = 7,   /* moq_session_subscribe_tracks */
} moq_request_family_t;

/* Fixed-width TRACK_STATUS discriminants. Zero is reserved. */
typedef uint32_t moq_track_status_terminal_cause_t;
#define MOQ_TRACK_STATUS_PEER_RESET_STREAM  1u
#define MOQ_TRACK_STATUS_PEER_STOP_SENDING  2u
#define MOQ_TRACK_STATUS_MISSING_RESPONSE   3u
#define MOQ_TRACK_STATUS_TRUNCATED_RESPONSE 4u
typedef uint32_t moq_track_status_role_t;
#define MOQ_TRACK_STATUS_REQUESTER 1u
#define MOQ_TRACK_STATUS_RESPONDER 2u

/* Draft-22: one abnormal outcome for an outstanding public TRACK_STATUS.
 * Legal requester FIN permits a delayed response. A validated complete response
 * precedes a later stream signal; committed response/migration and session
 * destruction do not generate another terminal. First cause/code survive
 * event/action refusal. Once failure is known, accept/reject cannot revive it;
 * event commitment makes the handle identifier-only/stale even if private
 * directional cleanup remains. Legacy profiles keep their existing behavior.
 * RESET/STOP carry the exact peer stream error, including zero. FIN diagnoses
 * carry zero with no error meaning, never peer rejection/retry metadata.
 * Frozen 24-byte scalar detail at offsets 0/8/12/16, no resources. Scalar polls
 * may copy a shorter prefix: read the full record only if element_size >= 40
 * AND detail_size >= 24. detail_size is the producer extent, not copied bytes. */
typedef struct moq_track_status_terminated_event {
    moq_track_status_handle_t handle;
    moq_track_status_terminal_cause_t cause;
    moq_track_status_role_t local_role;
    uint64_t stream_error_code;
} moq_track_status_terminated_event_t;

/* Frozen 24-byte scalar detail, offsets 0/8/12/16; same scalar poll rule as
 * TRACK_STATUS_TERMINATED. The typed handle belongs to a namespace subscription;
 * its initial OK does not end an established namespace subscription.
 * RESET/STOP use the exact peer stream error, including zero; FIN causes use
 * zero without error meaning. PEER_FIN_COMPLETE follows every remaining GONE;
 * a requester's FIN does not complete the subscription. No pointer/resource or
 * announcement capacity cause. Terminal commitment retires the public handle, not necessarily all
 * private directional cleanup. */
typedef struct moq_ns_subscription_terminated_event {
    moq_ns_sub_handle_t handle;
    moq_request_terminal_cause_t cause;
    moq_request_role_t           local_role;
    uint64_t stream_error_code;
} moq_ns_subscription_terminated_event_t;

/* Fill events. Exactly one parent handle is valid (sub for a SUBSCRIBE-initiated
 * subscription, pub for a PUBLISH-initiated one). Terminal commitment
 * invalidates the child's public operations; queued events may still carry the
 * identifier-only handle. Every fill event needs its full frozen prefix on poll. */
typedef struct moq_fill_created_event {
    moq_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating_request_id;
} moq_fill_created_event_t;

typedef struct moq_fill_request_event {
    moq_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating_request_id;
    uint64_t start_group, start_object, end_group, end_object; /* inclusive */
    uint64_t processed_at_us;
    uint64_t fill_timeout_ms;
    uint32_t group_order;
    uint8_t subscriber_priority;
    bool has_fill_timeout;
} moq_fill_request_event_t;

typedef struct moq_fill_object_event {
    moq_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating_request_id;
    uint64_t group_id, subgroup_id, object_id;
    uint8_t publisher_priority;
    bool datagram;
    moq_rcbuf_t *payload;    /* OWNED ref transferred on full poll */
    moq_rcbuf_t *properties; /* OWNED ref transferred on full poll */
    /* Appended: the Prior Group/Object ID Gaps as received (see
     * moq_object_received_event_t). Read only when detail_size covers
     * prior_object_gap. _reserved_gap is never read: it keeps the new
     * fields past the original sizeof where that ends in tail padding. */
    uint32_t _reserved_gap;
    bool     has_prior_group_gap;
    bool     has_prior_object_gap;
    uint8_t  _pad_gap[2];
    uint64_t prior_group_gap;
    uint64_t prior_object_gap;
} moq_fill_object_event_t;

typedef struct moq_fill_gap_event {
    moq_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating_request_id;
    uint64_t group_id, object_id;
    uint32_t kind; /* MOQ_FETCH_RANGE_* */
} moq_fill_gap_event_t;

/* Why a fill ended, and the error domain its code belongs to. */
#define MOQ_FILL_COMPLETE                 1u
#define MOQ_FILL_NO_STREAM                2u
#define MOQ_FILL_LOCAL_CANCELLED          3u
#define MOQ_FILL_PEER_CANCELLED_OR_RESET  4u
#define MOQ_FILL_LOCAL_ABORTED            5u
#define MOQ_FILL_PARENT_REJECTED          6u
#define MOQ_FILL_PARENT_TERMINATED        7u
#define MOQ_FILL_ERROR_NONE               0u
#define MOQ_FILL_ERROR_STREAM             1u
#define MOQ_FILL_ERROR_PARENT_REQUEST     2u
#define MOQ_FILL_ERROR_PARENT_DONE        3u
typedef struct moq_fill_terminal_event {
    moq_fill_t fill;
    moq_subscription_t sub;
    moq_publication_t pub;
    uint64_t initiating_request_id;
    uint32_t reason;
    uint32_t error_domain; /* error_code=0 is valid when domain != NONE */
    uint64_t error_code;
} moq_fill_terminal_event_t;

#define MOQ_FILL_CREATED_EVENT_V0_SIZE 32u
#define MOQ_FILL_REQUEST_EVENT_V0_SIZE 88u
#define MOQ_FILL_TERMINAL_EVENT_V0_SIZE 48u
/* These two end at their last field: their tail padding depends on the data
 * model (where uint64_t is 4-aligned inside a struct they have none), so a
 * size that counted it would exceed the struct there. */
#define MOQ_FILL_GAP_EVENT_V0_SIZE \
    (offsetof(moq_fill_gap_event_t, kind) + sizeof(uint32_t))
#define MOQ_FILL_OBJECT_EVENT_V0_SIZE \
    (offsetof(moq_fill_object_event_t, properties) + sizeof(moq_rcbuf_t *))

typedef struct moq_publish_state_event {
    moq_request_family_t family;             /* SUBSCRIBE or PUBLISH */
    union {
        moq_subscription_t subscription;     /* a subscription this side requested */
        moq_publication_t  publication;      /* a publication this side accepted */
        uint64_t           raw;
    } handle;
    bool     has_forward;
    bool     forward;
    bool     has_largest;
    bool     has_filter;
    uint8_t  _pad[4];
    uint64_t largest_group;
    uint64_t largest_object;
    /* Appended after the complete previous detail. A whole-field-covered
     * pointer is non-NULL exactly when has_filter is true, including explicit
     * NONE. The sized location is this notification's immutable observation,
     * BORROWED for the event's borrow_epoch, not the latest *_filter() result.
     * Copy covered fields before advancing the session. */
    const moq_location_filter_cfg_t *location;
} moq_publish_state_event_t;

typedef struct moq_request_redirect_event {
    moq_request_family_t family;
    union {                                  /* select by family */
        moq_subscription_t        subscription;     /* subscribe */
        moq_fetch_t               fetch;            /* fetch */
        moq_track_status_handle_t track_status;     /* track status */
        moq_announcement_t        announcement;     /* namespace announcement */
        moq_ns_sub_handle_t       ns_sub;           /* namespace subscription */
        moq_publication_t         publication;      /* publish */
        moq_track_sub_handle_t    track_sub;        /* subscribe tracks */
        uint64_t                  raw;
    } handle;
    moq_request_error_t error_code;          /* REDIRECT */
    bool                can_retry;           /* retry interval was non-zero */
    uint8_t             _pad[3];
    uint64_t            retry_after_ms;      /* §10.6.1: relays may cache up to this */
    moq_bytes_t         connect_uri;         /* empty ⇒ reuse current session URI */
    /* The Redirect target exactly as received. On draft-18, when both are
     * empty the redirected request uses the original request's namespace and
     * name; when either is not, both are the literal values to use
     * (draft-18 §10.6.1). On draft-22 they are the literal target, empty
     * values included (§9.4.1). */
    moq_namespace_t     track_namespace;
    moq_bytes_t         track_name;
    moq_bytes_t         reason;              /* the error reason phrase */
} moq_request_redirect_event_t;

/* A peer sent a GOAWAY on this request's bidi (§10.4): migrate the single request
 * to `new_session_uri` (empty ⇒ reuse the current session URI) and re-issue it.
 * The request is terminal; `handle` identifies it (select by `family`). The
 * new_session_uri is BORROWED from output scratch. */
typedef struct moq_request_goaway_event {
    moq_request_family_t family;
    union {                                  /* select by family */
        moq_subscription_t        subscription;
        moq_fetch_t               fetch;
        moq_track_status_handle_t track_status;
        moq_announcement_t        announcement;
        moq_ns_sub_handle_t       ns_sub;
        moq_publication_t         publication;
        moq_track_sub_handle_t    track_sub;
        uint64_t                  raw;
    } handle;
    moq_bytes_t new_session_uri;             /* empty ⇒ reuse current session URI */
    uint64_t    timeout_ms;
} moq_request_goaway_event_t;


typedef struct moq_unsubscribed_event {
    moq_subscription_t sub;  /* identifier only; stale after polling */
    /* Appended: how the subscription ended when a stream signal, not a
     * message, ended it (read when detail_size covers the field). */
    moq_request_terminal_cause_t cause;
    uint8_t                      _pad[4];
    uint64_t                     stream_error_code;
} moq_unsubscribed_event_t;

/* The publisher's terminal done for an ordinary subscription. When the done
 * advertises a finite, nonzero stream count, this event is DELAYED until that
 * many of the subscription's data streams have been processed (completed, or
 * identifiably reset after their header) -- objects on late-arriving streams
 * are still delivered in the interim. A count of 0 surfaces the event
 * immediately. The wait -- including for the maximum representable count
 * (2^62-1, "unknown") -- is bounded by the deadline from
 * moq_session_cfg_t.done_wait_timeout_us (floored by the negotiated
 * delivery timeouts): on expiry the remaining streams are discarded and
 * the event surfaces with the publisher's original values. The
 * subscription handle is stale after polling. */
typedef struct moq_subscribe_done_event {
    moq_subscription_t sub;
    uint64_t           status_code;
    uint64_t           stream_count;
    moq_bytes_t        reason;  /* BORROWED from output scratch */
} moq_subscribe_done_event_t;

/* Value-only D22 UPDATE processing snapshot. The optional Largest is exactly
 * the basis used by the response, parent filter and fill, not a later query of
 * track history. A selected replacement is resolved to NONE or an absolute
 * window; an empty window uses OBJECT_END with equal groups and end_object
 * below start_object. Unselected fields, including location, are all zero. */
#define MOQ_UPDATE_RESOLUTION_HAS_LARGEST          UINT32_C(1)
#define MOQ_UPDATE_RESOLUTION_LOCATION_REPLACEMENT UINT32_C(2)
typedef struct moq_update_resolution {
    uint32_t struct_size;
    uint32_t flags;
    uint64_t update_request_id;
    uint64_t largest_group;
    uint64_t largest_object;
    moq_location_filter_cfg_t location;
} moq_update_resolution_t;

#define MOQ_UPDATE_RESOLUTION_V0_SIZE 80u

/* Selected, lossless UPDATE parameters. Output-only, value-only and BORROWED
 * for the event's epoch; copy before the next advancing call. Inactive values
 * are zero. location has its own struct_size when selected. */
#define MOQ_SUBSCRIPTION_UPDATE_OPTIONS_LOCATION                  UINT32_C(1)
#define MOQ_SUBSCRIPTION_UPDATE_OPTIONS_OBJECT_DELIVERY_TIMEOUT   UINT32_C(2)
#define MOQ_SUBSCRIPTION_UPDATE_OPTIONS_SUBGROUP_DELIVERY_TIMEOUT UINT32_C(4)
typedef struct moq_subscription_update_options {
    uint32_t struct_size;
    uint32_t flags;
    uint64_t initiating_request_id;
    moq_location_filter_cfg_t location;
    uint64_t object_delivery_timeout_ms;
    uint64_t subgroup_delivery_timeout_ms;
    /* Appended after the complete original 80-byte extent. Read only when
     * struct_size covers this entire member. Raw location above is unchanged. */
    moq_update_resolution_t resolution;
} moq_subscription_update_options_t;

typedef struct moq_subscribe_updated_event {
    moq_subscription_t sub;
    bool               has_subscriber_priority;
    uint8_t            subscriber_priority;
    bool               has_forward;
    bool               forward;
    bool               has_delivery_timeout;
    uint8_t            _pad[5];
    uint64_t           delivery_timeout_us;
    /* Authorization tokens carried on the update, resolved against the session
     * token cache. BORROWED from output scratch; NULL when none. (Appended:
     * callers built against an older layout still drain the event via the
     * size-negotiated poll and simply do not see these fields.) */
    const moq_resolved_token_t *tokens;
    size_t                       token_count;
    /* Appended: the subscriber asks the original publisher to start a new
     * group (largest known Group ID + 1; 0 = "no group info"). Surfaced
     * only -- whether and when to start a group is application policy. */
    bool                         has_new_group_request;
    uint64_t                     new_group_request;
    /* Appended: a replacement Subscription Filter carried on the update
     * (omitted on the wire means unchanged). start/end fields are valid
     * per the filter type, as on the SUBSCRIBE_REQUEST event. Which
     * objects to deliver under the new window is application policy. */
    bool                         has_filter;
    moq_subscribe_filter_t       filter;
    uint64_t                     start_group;
    uint64_t                     start_object;
    uint64_t                     end_group;
    /* Appended AFTER the complete old detail extent. Non-NULL on every
     * typed-options UPDATE, including an empty refresh; NULL on legacy
     * profiles. Describes selected fields, not effective defaults. When
     * present, the legacy timeout/filter projections above are inactive. */
    const moq_subscription_update_options_t *options;
    /* Appended: the session is waiting for the application's answer
     * (moq_session_cfg_t.defer_subscription_updates). Until it answers,
     * nothing this update asks for is in effect: forward state, priority,
     * delivery timeouts, filter and fill are those of the subscription
     * before the update. Answer with moq_session_accept_subscription_update()
     * or moq_session_reject_subscription_update(), naming
     * update_request_id. Always false when the session answered itself. */
    /* Do not reuse padding in the complete options-pointer tail on ILP32. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    bool                         answer_owed;
    /* Appended: the update carries a fill request (FILL_PARAMETERS). Under
     * answer_owed no fill exists yet; accepting admits it. */
    bool                         fill_requested;
    /* Appended: how many updates this event's values and its answer cover.
     * More than one when the subscriber sent further updates before the owed
     * answer: the session coalesces them (later values override earlier
     * ones, has_* fields accumulate), surfaces this event with the
     * cumulative values, and the answer then covers all of them -- one
     * acknowledgement each on accept, a single refusal on reject. A profile
     * that advertises a bound on unanswered updates (draft-22) closes the
     * session instead when the subscriber exceeds it. Always 1 when the
     * session answered itself. tokens are those of the newest update only. */
    uint32_t                     update_count;
    /* Appended: the Request ID of the newest update covered; the
     * correlation the answer names. */
    uint64_t                     update_request_id;
} moq_subscribe_updated_event_t;

typedef enum moq_object_terminal {
    MOQ_OBJECT_TERMINAL_NORMAL = 0,
    MOQ_OBJECT_TERMINAL_RESET  = 1,
    MOQ_OBJECT_TERMINAL_STOP   = 2,
} moq_object_terminal_t;

typedef struct moq_object_chunk_event {
    moq_subscription_t    sub;
    moq_publication_t     pub;
    moq_rcbuf_t          *chunk;       /* OWNED; payload bytes, NULL if no data */
    bool                  begin;       /* first chunk: metadata fields valid */
    bool                  end;         /* last chunk or only chunk */
    /* valid when end=true. MOQ_OBJECT_TERMINAL_RESET means the peer reset the
     * stream while THIS object was in flight and surfaced; the cause is in
     * error_code below. When no visible object terminal can carry the closure
     * (whole-object mode, between objects, or a suppressed object), the reset
     * reports MOQ_EVENT_SUBGROUP_RESET instead -- for one reset exactly one of
     * the two surfaces fires. */
    moq_object_terminal_t terminal;
    uint8_t               _pad[3];
    /* Routing identity: valid on EVERY chunk of the object -- begin,
     * continuation and end -- so a consumer multiplexing concurrent subgroups
     * routes each chunk to the record it belongs to. The chunks of one
     * identity never interleave from two streams: a copy of an object that
     * begins on another stream of the same Track alias while the object is
     * still being surfaced (a reset-and-reopen race, or concurrent
     * subscriptions sharing the alias) is consumed without events. */
    uint64_t              group_id;
    uint64_t              subgroup_id;
    uint64_t              object_id;
    uint8_t               publisher_priority; /* valid when begin=true */
    moq_object_status_t   status;          /* valid when begin=true */
    bool                  end_of_group;    /* valid when begin=true */
    /* Valid when begin=true: the object is the first object ever published
     * in its subgroup (see moq_object_received_event_t.first_object). */
    bool                  first_object;
    uint8_t               _pad2[3];
    uint64_t              payload_length;  /* declared wire length; valid when begin=true */
    moq_rcbuf_t          *properties;      /* OWNED; valid when begin=true, NULL if none */
    /* Appended. The peer's RESET_STREAM application code, valid ONLY on a
     * terminal chunk whose terminal is MOQ_OBJECT_TERMINAL_RESET; zero on
     * every other chunk, including NORMAL and STOP terminals (zero is also a
     * legal peer code, so read it only under the RESET terminal). */
    uint64_t              error_code;
    /* Appended: the Prior Group ID Gap (0x3C) and Prior Object ID Gap (0x3E)
     * Object Properties (draft-22 §10.8, §10.9; draft-18 §12.8, §12.9), as
     * received. A gap of N says the N Groups before this Group, or the N
     * Objects before this Object in its Group, do not and will never exist.
     * Read only when detail_size covers prior_object_gap; the raw properties
     * still carry them. Valid when begin=true. */
    bool               has_prior_group_gap;
    bool               has_prior_object_gap;
    /* Valid when begin=true: see moq_object_received_event_t. */
    bool               subgroup_properties;
    uint8_t            _pad_gap[5];
    uint64_t           prior_group_gap;
    uint64_t           prior_object_gap;
} moq_object_chunk_event_t;

typedef uint32_t moq_namespace_interest_t;

typedef struct moq_ns_sub_request_event {
    moq_ns_sub_handle_t      handle;
    moq_namespace_t          track_namespace_prefix;  /* BORROWED from output scratch */
    moq_namespace_interest_t namespace_interest;
    bool                     forward;
    uint8_t                  _pad[3];
    const moq_resolved_token_t *tokens;    /* BORROWED from output scratch; NULL if none */
    size_t                       token_count;
} moq_ns_sub_request_event_t;

typedef struct moq_ns_sub_ok_event {
    moq_ns_sub_handle_t handle;
} moq_ns_sub_ok_event_t;

typedef struct moq_ns_sub_error_event {
    moq_ns_sub_handle_t  handle;
    moq_request_error_t  error_code;
    moq_bytes_t          reason;  /* BORROWED from output scratch */
} moq_ns_sub_error_event_t;

typedef struct moq_namespace_found_event {
    moq_ns_sub_handle_t handle;
    moq_namespace_t     track_namespace_suffix;  /* BORROWED from output scratch */
} moq_namespace_found_event_t;

typedef struct moq_namespace_gone_event {
    moq_ns_sub_handle_t handle;
    moq_namespace_t     track_namespace_suffix;  /* BORROWED from output scratch */
} moq_namespace_gone_event_t;

/*
 * What a FETCH request states beyond the range fields of
 * moq_fetch_request_event_t, on a profile whose FETCH carries its range as a
 * Location Filter. Written by the library into output scratch with the event
 * and borrowed for as long as the event is (see `options` below); struct_size
 * is the size the library wrote, so a field appended later is present only
 * when struct_size covers it.
 *
 * range_unresolved: the event's start_* / end_* fields are the request's own
 * explicit values, zero where its form leaves them to the Largest Object, and
 * `location` is the form to resolve (moq_session_cfg_t.defer_fetch_ranges).
 * Resolving it against a Largest Object {G, O}: no filter starts at {0, 0}; a
 * RELATIVE_GROUP offset N starts at {G + 1 - N, 0} (offset 0 is the next group,
 * one the current group, clamped at group 0); NEXT_OBJECT starts at {G, O + 1};
 * ABSOLUTE_START and the bounded forms start where they say. A form without an
 * end ends at {G, O}; a bounded form ends at its own end or at {G, O},
 * whichever is smaller. No Objects, or a start past {G, O}, is INVALID_RANGE
 * (moq_session_reject_fetch()). False: the session resolved the range (or it
 * was explicit), and start_* / end_* are final.
 *
 * location: the request's semantic Location Filter, copied by value. NONE
 * represents no filter (the whole track through the Largest Object). It is
 * always initialized, including when the parameter was omitted. Copying the
 * complete prefix retains all its facts independently of the event's epoch.
 *
 * HAS_FILL_TIMEOUT / fill_timeout_ms: the request's FILL_TIMEOUT, the most it
 * wants a relay to spend waiting for Objects that are not immediately
 * available before reporting them as Timed-Out gaps; zero asks for no wait.
 * INCLUDE_PROPERTIES carries the effective value, including its default true.
 * No flag distinguishes omitted and explicit NONE, which have the same FETCH
 * semantics. Unknown flags are not emitted; inactive timeout is zero.
 */
#define MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT   0x01u
#define MOQ_FETCH_REQUEST_INCLUDE_PROPERTIES 0x02u
#define MOQ_FETCH_REQUEST_OPTIONS_V0_SIZE    64u

typedef struct moq_fetch_request_options {
    uint32_t struct_size;
    uint32_t flags;
    moq_location_filter_cfg_t location;
    uint64_t fill_timeout_ms;
    /* Appended after the complete v0 prefix. Read only when struct_size
     * covers this field; absent means the range has been resolved. */
    bool     range_unresolved;
    uint8_t  _reserved[7];
} moq_fetch_request_options_t;

typedef struct moq_fetch_request_event {
    moq_fetch_t        fetch;
    moq_namespace_t    track_namespace;  /* BORROWED from output scratch; empty for joining fetch */
    moq_bytes_t        track_name;       /* BORROWED from output scratch; empty for joining fetch */
    moq_subscription_t joining_sub;      /* valid for joining fetch; zero for standalone */
    uint64_t           start_group;
    uint64_t           start_object;
    uint64_t           end_group;
    uint64_t           end_object;
    uint8_t            subscriber_priority;
    moq_group_order_t  group_order;
    uint8_t            _pad[2];
    const moq_resolved_token_t *tokens;  /* BORROWED; NULL if none */
    size_t                       token_count;
    /* Appended: the joined owner when the Joining Request ID named a
     * PUBLISH-initiated subscription. Surfaced on BOTH draft profiles.
     * Mutually exclusive with joining_sub -- for any joining fetch exactly one
     * of the two is valid, and both are zero for a standalone fetch. */
    moq_publication_t  joining_pub;
    /* Appended: the request's Location Filter, FILL_TIMEOUT and, under
     * moq_session_cfg_t.defer_fetch_ranges, whether the range is still to be
     * resolved (see moq_fetch_request_options_t). BORROWED from output
     * scratch, like the tokens. NULL on a profile whose FETCH carries no
     * Location Filter, and read only when detail_size covers it. */
    const moq_fetch_request_options_t *options;
} moq_fetch_request_event_t;

typedef struct moq_fetch_error_event {
    moq_fetch_t         fetch;
    moq_request_error_t error_code;
    bool                can_retry;
    uint8_t             _pad[7];
    uint64_t            retry_after_ms;
    /* BORROWED from output scratch. For the refusal of this side's update
     * of the fetch, empty when the peer's reason is longer than the whole
     * event arena (output_scratch_size): the refusal still ends only this
     * fetch, with its code. */
    moq_bytes_t         reason;
} moq_fetch_error_event_t;

typedef struct moq_fetch_cancelled_event {
    moq_fetch_t fetch;
    /* Appended: zero when a cancel message ended the request; otherwise the
     * stream signal that cancelled it and the peer's code. */
    moq_request_terminal_cause_t cause;
    uint8_t                      _pad[4];
    uint64_t                     stream_error_code;
} moq_fetch_cancelled_event_t;

typedef struct moq_fetch_ok_event {
    moq_fetch_t fetch;
    bool        end_of_track;
    uint8_t     _pad[7];
    /* The end of the range served, in the FETCH range's own form on every
     * draft: end_object is exclusive and zero means the whole end_group
     * (draft-22's inclusive End Location {G, O} surfaces as (G, O + 1)). */
    uint64_t    end_group;
    uint64_t    end_object;
    moq_bytes_t track_properties;  /* BORROWED from output scratch */
} moq_fetch_ok_event_t;

typedef struct moq_fetch_complete_event {
    moq_fetch_t fetch;
} moq_fetch_complete_event_t;

typedef struct moq_fetch_reset_event {
    moq_fetch_t fetch;
    uint64_t    error_code;    /* peer STREAM application error code; the
                                * code this side sent for LOCAL_CANCEL */
    bool        data_stream;   /* true: response data stream; false: request stream */
    bool        stop_sending;  /* request stream only: STOP_SENDING rather than RESET_STREAM */
    uint8_t     _pad[6];
    /* Appended: the same outcome as a cause discriminant, so a FIN that
     * failed the request is distinguishable from a RESET with code zero. */
    moq_request_terminal_cause_t cause;
    uint8_t     _pad2[4];
} moq_fetch_reset_event_t;

typedef enum moq_fetch_range_kind {
    MOQ_FETCH_RANGE_NON_EXISTENT = 1,
    MOQ_FETCH_RANGE_UNKNOWN      = 2,
    MOQ_FETCH_RANGE_TIMED_OUT    = 3, /* profiles with an End of Timed-Out Range */
} moq_fetch_range_kind_t;

typedef struct moq_fetch_object_event {
    moq_fetch_t   fetch;
    uint64_t      group_id;
    uint64_t      subgroup_id;   /* 0 (not applicable) when datagram is true */
    uint64_t      object_id;
    uint8_t       publisher_priority;
    bool          datagram;      /* original object's forwarding preference was
                                  * Datagram (§11.4.4.1, 0x40): no subgroup */
    uint8_t       _pad[6];
    moq_rcbuf_t  *payload;     /* OWNED ref transferred on poll; NULL for zero-length */
    moq_rcbuf_t  *properties;  /* OWNED ref transferred on poll; NULL if none */
    /* Appended: the Prior Group ID Gap (0x3C) and Prior Object ID Gap (0x3E)
     * Object Properties (draft-22 §10.8, §10.9; draft-18 §12.8, §12.9), as
     * received. A gap of N says the N Groups before this Group, or the N
     * Objects before this Object in its Group, do not and will never exist.
     * Read only when detail_size covers prior_object_gap; the raw properties
     * still carry them. */
    bool               has_prior_group_gap;
    bool               has_prior_object_gap;
    uint8_t            _pad_gap[6];
    uint64_t           prior_group_gap;
    uint64_t           prior_object_gap;
} moq_fetch_object_event_t;

/* Draft-22's additional initial parameters are observed through
 * moq_session_get_publish_initial_parameters(), not this event's padding. */
typedef struct moq_publish_request_event {
    moq_publication_t  pub;
    moq_namespace_t    track_namespace;
    moq_bytes_t        track_name;
    uint64_t           track_alias;
    bool               forward;
    uint8_t            _pad[7];
    const moq_resolved_token_t *tokens;
    size_t                       token_count;
    moq_bytes_t        track_properties;  /* BORROWED from output scratch */
    /* Appended: the track supports dynamically started groups (Track
     * Property DYNAMIC_GROUPS == 1); omitted on the wire means false. When
     * true, a new-group request may ride the accept (and later updates). */
    bool               dynamic_groups;
    /* Appended: the publisher's advertised Largest Object for this track. The
     * publisher MUST include it once Objects have been published, so absence
     * means "nothing published yet". Also max-merged into this session's track
     * history at request commit, so the registry is current even if the app
     * ignores this field. */
    bool               has_largest;
    /* Appended into the padding before largest_group (the union is at its
     * size limit): the publisher's DEFAULT_PUBLISHER_GROUP_ORDER, or
     * MOQ_GROUP_ORDER_DEFAULT if it advertised none. */
    uint8_t            publisher_group_order;
    uint64_t           largest_group;
    uint64_t           largest_object;
    /* Appended: the publisher's advertised EXPIRES (milliseconds). Surfaced
     * only -- the session does not run an expiry timer. */
    bool               has_expires;
    uint64_t           expires_ms;
} moq_publish_request_event_t;

typedef struct moq_publish_ok_event {
    moq_publication_t  pub;
    bool               send_allowed;
    uint8_t            subscriber_priority;
    moq_group_order_t  group_order;
    uint8_t            _pad[2];
    bool               has_delivery_timeout;
    uint64_t           delivery_timeout_ms;
    bool               has_expires;
    uint8_t            _pad2[7];
    uint64_t           expires_ms;
    /* Appended: the subscriber asks the original publisher to start a new
     * group (largest known Group ID + 1; 0 = "no group info"). Surfaced
     * only -- whether and when to start a group is application policy. */
    bool                         has_new_group_request;
    uint64_t                     new_group_request;
    /* Appended: the subscription filter the subscriber chose, surfaced RAW
     * exactly as it arrived (same shape as moq_publish_updated_event_t) --
     * absent means unfiltered. The session separately resolves this against the
     * track's largest into an internal window; that resolved window is not
     * exposed here. */
    bool                   has_filter;
    moq_subscribe_filter_t filter;
    uint64_t               start_group;
    uint64_t               start_object;
    uint64_t               end_group;
} moq_publish_ok_event_t;

typedef struct moq_publish_error_event {
    moq_publication_t   pub;
    moq_request_error_t error_code;
    bool                can_retry;
    uint8_t             _pad[7];
    uint64_t            retry_after_ms;
    /* BORROWED from output scratch. For the refusal of this side's update
     * of its own PUBLISH, empty when the peer's reason is longer than the
     * whole event arena (output_scratch_size): the publication still owes
     * its UPDATE_FAILED terminal, and the session is unaffected. */
    moq_bytes_t         reason;
} moq_publish_error_event_t;

/* The publisher's terminal done for a publication we subscribed to. Same
 * delayed-delivery contract as moq_subscribe_done_event_t: with a finite,
 * nonzero stream count the event is DELAYED until that many of the
 * publication's data streams have been processed (completed, or identifiably
 * reset after their header) -- objects on late-arriving streams are still
 * delivered in the interim. A count of 0 surfaces the event immediately.
 * The wait -- including for the maximum representable count (2^62-1,
 * "unknown") -- is bounded by the deadline from
 * moq_session_cfg_t.done_wait_timeout_us (floored by the negotiated
 * delivery timeouts): on expiry the remaining streams are discarded and
 * the event surfaces with the publisher's original values. The publication
 * handle is stale after polling. */
typedef struct moq_publish_finished_event {
    moq_publication_t pub;
    uint64_t          status_code;
    uint64_t          stream_count;
    moq_bytes_t       reason;  /* BORROWED from output scratch */
    /* Appended: non-zero when no completion message arrived and a stream
     * signal ended the publication instead; status_code is then zero and
     * carries no meaning. */
    moq_request_terminal_cause_t cause;
    uint8_t                      _pad[4];
    uint64_t                     stream_error_code;
} moq_publish_finished_event_t;

typedef struct moq_publish_unsubscribed_event {
    moq_publication_t pub;
    /* Appended: stream signal that ended the publication, if one did. */
    moq_request_terminal_cause_t cause;
    uint8_t                      _pad[4];
    uint64_t                     stream_error_code;
} moq_publish_unsubscribed_event_t;

typedef struct moq_publish_updated_event {
    moq_publication_t pub;
    bool               has_subscriber_priority;
    uint8_t            subscriber_priority;
    bool               has_forward;
    bool               forward;
    bool               has_delivery_timeout;
    uint8_t            _pad[5];
    uint64_t           delivery_timeout_us;
    /* Authorization tokens carried on the update, resolved against the session
     * token cache. BORROWED from output scratch; NULL when none. (Appended:
     * callers built against an older layout still drain the event via the
     * size-negotiated poll and simply do not see these fields.) */
    const moq_resolved_token_t *tokens;
    size_t                       token_count;
    /* Appended: the subscriber asks the original publisher to start a new
     * group (largest known Group ID + 1; 0 = "no group info"). Surfaced
     * only -- whether and when to start a group is application policy. */
    bool                         has_new_group_request;
    uint64_t                     new_group_request;
    /* Appended: a replacement Subscription Filter carried on the update
     * (omitted on the wire means unchanged). start/end fields are valid
     * per the filter type. Which objects to deliver under the new window
     * is application policy. */
    bool                         has_filter;
    moq_subscribe_filter_t       filter;
    uint64_t                     start_group;
    uint64_t                     start_object;
    uint64_t                     end_group;
    /* Appended AFTER the complete old detail extent. Non-NULL on every
     * draft-22 UPDATE, including an empty refresh; NULL on legacy profiles.
     * Selected lossless values for this notification, not later effective
     * state. BORROWED for the event epoch, as on SUBSCRIBE_UPDATED. When
     * present, the legacy timeout/filter projections above are inactive. */
    const moq_subscription_update_options_t *options;
    /* Appended: the session is waiting for the application's answer
     * (moq_session_cfg_t.defer_subscription_updates), exactly as for
     * moq_subscribe_updated_event_t.answer_owed. Answer with
     * moq_session_accept_publication_update() or
     * moq_session_reject_publication_update(), naming update_request_id.
     * Always false when the session answered itself. */
    /* Preserve the complete options-pointer tail, including ILP32 padding. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    bool                         answer_owed;
    /* Appended: the update carries a fill request (FILL_PARAMETERS). Under
     * answer_owed no fill exists yet; accepting admits it. */
    bool                         fill_requested;
    /* Appended: how many updates this event's values and its answer cover,
     * as moq_subscribe_updated_event_t.update_count. Always 1 when the
     * session answered itself. */
    uint32_t                     update_count;
    /* Appended: the Request ID of the newest update covered; the
     * correlation the answer names. */
    uint64_t                     update_request_id;
} moq_publish_updated_event_t;

/*
 * MOQ_EVENT_SUBSCRIPTION_UPDATE_OK (47): the peer accepted an outbound update we
 * sent on a subscription. Fires exactly once per accepted update, on the
 * requester side, when the pending-update state clears. `has_largest` is true
 * iff the peer carried a Largest Object (both drafts); the app uses it to decide
 * a gap-FETCH after widening an End (§10.9.1). No value is synthesised when
 * absent (absence means the peer published/received nothing). `has_expires` may
 * be set only under draft-18 (a draft-16 acknowledgment does not carry EXPIRES
 * on this path); always false under draft-16, and reflects whatever the PEER sent
 * -- this library's own outbound acks do not currently emit EXPIRES. Plain
 * scalars only -- no borrowed spans, safe to read after the next advancing call.
 */
typedef struct moq_subscription_update_ok_event {
    moq_subscription_t sub;            /* the subscription we updated */
    bool     has_largest;
    uint64_t largest_group;            /* valid iff has_largest */
    uint64_t largest_object;           /* valid iff has_largest */
    bool     has_expires;              /* EXPIRES present -- d18 only */
    uint64_t expires_ms;               /* valid iff has_expires */
} moq_subscription_update_ok_event_t;

/*
 * MOQ_EVENT_PUBLICATION_UPDATE_OK (48): the peer accepted an outbound update we
 * sent on a publish-established subscription. Same semantics as
 * moq_subscription_update_ok_event_t; see MOQ_EVENT_SUBSCRIPTION_UPDATE_OK.
 */
typedef struct moq_publication_update_ok_event {
    moq_publication_t pub;             /* the PUBLISH-established sub we updated */
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;
    uint64_t expires_ms;
} moq_publication_update_ok_event_t;

typedef struct moq_track_status_request_event {
    moq_track_status_handle_t handle;
    moq_namespace_t           track_namespace;
    moq_bytes_t               track_name;
    const moq_resolved_token_t *tokens;
    size_t                    token_count;
} moq_track_status_request_event_t;

typedef struct moq_track_status_ok_event {
    moq_track_status_handle_t handle;
    bool               has_largest;
    uint8_t            _pad1[7];
    uint64_t           largest_group;
    uint64_t           largest_object;
    bool               has_expires;
    uint8_t            _pad2[7];
    uint64_t           expires_ms;
    /* Appended (ABI-additive): the opaque Track Properties tail received in
     * TRACK_STATUS_OK (draft-18). BORROWED from output scratch; empty under
     * draft-16 (no properties tail). */
    moq_bytes_t        track_properties;
} moq_track_status_ok_event_t;

typedef struct moq_track_status_error_event {
    moq_track_status_handle_t handle;
    moq_request_error_t       error_code;
    bool                      can_retry;
    uint8_t                   _pad[7];
    uint64_t                  retry_after_ms;
    moq_bytes_t               reason;
} moq_track_status_error_event_t;

/* -- SUBSCRIBE_TRACKS events (draft-18) ---------------------------- */

#define MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER        0x01u
#define MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES 0x02u
#define MOQ_SUBSCRIBE_TRACKS_OPTIONS_V0_SIZE    16u

/* The draft-22 SUBSCRIBE_TRACKS options beyond its prefix and FORWARD
 * (§9.18), borrowed for the containing event's epoch. GROUP_ORDER and
 * INCLUDE_PROPERTIES flag a parameter the request carried; the values are
 * always the effective ones: MOQ_GROUP_ORDER_DEFAULT (the publisher's order)
 * and include_properties true when omitted (§9.20.8, §9.20.21). They apply to
 * the PUBLISH messages the request leads to. No Range Filter capacity is
 * implied: this side advertises MAX_FILTER_RANGES 0. struct_size is the size
 * the library wrote; MOQ_SUBSCRIBE_TRACKS_OPTIONS_V0_SIZE is the frozen v0. */
typedef struct moq_subscribe_tracks_options {
    uint32_t struct_size;
    uint32_t flags;
    moq_group_order_t group_order;
    bool include_properties;
    uint8_t _pad[3];
} moq_subscribe_tracks_options_t;

/* Publisher side: a peer's SUBSCRIBE_TRACKS arrived. Reply with
 * moq_session_accept_subscribe_tracks / _reject_subscribe_tracks. */
typedef struct moq_subscribe_tracks_request_event {
    moq_track_sub_handle_t handle;
    moq_namespace_t        track_namespace_prefix;  /* BORROWED from output scratch */
    bool                   forward;
    uint8_t                _pad[7];
    const moq_resolved_token_t *tokens;   /* BORROWED from output scratch; NULL if none */
    size_t                 token_count;
    /* Appended after the complete legacy detail; NULL on draft-18. */
    const moq_subscribe_tracks_options_t *options;
} moq_subscribe_tracks_request_event_t;

/* Subscriber side: the publisher accepted; the bidi is now established and may
 * carry PUBLISH_BLOCKED (and, later, PUBLISH on separate bidis). */
typedef struct moq_subscribe_tracks_ok_event {
    moq_track_sub_handle_t handle;
} moq_subscribe_tracks_ok_event_t;

/* Subscriber side: the publisher rejected; the bidi is closed. */
typedef struct moq_subscribe_tracks_error_event {
    moq_track_sub_handle_t handle;
    moq_request_error_t    error_code;
    bool                   can_retry;
    uint8_t                _pad[7];
    uint64_t               retry_after_ms;
    moq_bytes_t            reason;  /* BORROWED from output scratch */
} moq_subscribe_tracks_error_event_t;

/* Subscriber side: the publisher signalled it cannot open a PUBLISH for a
 * matching track (the subscriber may SUBSCRIBE directly instead). */
typedef struct moq_publish_blocked_event {
    moq_track_sub_handle_t handle;
    moq_namespace_t        track_namespace_suffix;  /* BORROWED from output scratch */
    moq_bytes_t            track_name;               /* BORROWED from output scratch */
} moq_publish_blocked_event_t;

/* Publisher side: the subscriber cancelled (FIN/RESET on the request bidi). */
typedef struct moq_subscribe_tracks_cancelled_event {
    moq_track_sub_handle_t handle;  /* identifier only; stale after polling */
    /* Appended: the stream signal that ended the request, if one did. Both
     * roles receive this event. */
    moq_request_terminal_cause_t cause;
    uint8_t                      _pad[4];
    uint64_t                     stream_error_code;
} moq_subscribe_tracks_cancelled_event_t;

/* -- Namespace request updates (draft-22 §9.5, §9.5.2) -------------- */

/* Parameters a namespace update selects. PREFIX is TRACK_NAMESPACE_PREFIX
 * (§9.20.20): namespace subscriptions and SUBSCRIBE_TRACKS only. FORWARD
 * (§9.20.18): SUBSCRIBE_TRACKS only, and it governs only the subscriptions
 * later discovery creates. AUTHORIZATION marks, in an observed record, that
 * the update's AUTHORIZATION_TOKEN parameters resolved to at least one token
 * (REGISTER, USE_ALIAS or USE_VALUE; a DELETE only retires an alias and
 * resolves to none, §8.9); a configuration carries tokens through
 * auth_tokens instead. An unselected parameter keeps its value
 * (§9.5). An update selecting nothing is a valid refresh. */
#define MOQ_NAMESPACE_UPDATE_PREFIX        0x01u
#define MOQ_NAMESPACE_UPDATE_FORWARD       0x02u
#define MOQ_NAMESPACE_UPDATE_AUTHORIZATION 0x04u

/*
 * One namespace update as this side observes it: the record of a peer
 * update owed a decision, or of one this side is answering. flags selects
 * the fields that are meaningful. A selected prefix of zero parts is the
 * empty prefix, which matches every namespace. tokens are the update's
 * resolved authorization tokens in wire order. update_request_id is the
 * update's own full 64-bit Request ID, not the original request's.
 *
 * Borrowed: from an event, for the event's borrow epoch; from an owed-update
 * query, until the next advancing call on the session. struct_size is the
 * size the library wrote; a field past it is absent.
 */
typedef struct moq_namespace_update_record {
    uint32_t                    struct_size;
    uint32_t                    flags;
    uint64_t                    update_request_id;
    moq_namespace_t             track_namespace_prefix;
    bool                        forward;
    uint8_t                     _reserved[7];
    const moq_resolved_token_t *tokens;
    size_t                      token_count;
} moq_namespace_update_record_t;

/* Frozen v0 layout: everything through token_count. */
#define MOQ_NAMESPACE_UPDATE_RECORD_V0_SIZE \
    (offsetof(moq_namespace_update_record_t, token_count) + sizeof(size_t))

/* Responder side: the peer updated a request this side answers; the answer
 * is owed (moq_session_accept_namespace_update and its siblings). Emitted
 * once per update, with the typed owner and the update's Request ID. */
typedef struct moq_namespace_update_request_event {
    moq_announcement_t                   ann;
    uint64_t                             update_request_id;
    const moq_namespace_update_record_t *update;   /* BORROWED (event epoch) */
} moq_namespace_update_request_event_t;

typedef struct moq_ns_sub_update_request_event {
    moq_ns_sub_handle_t                  handle;
    uint64_t                             update_request_id;
    const moq_namespace_update_record_t *update;   /* BORROWED (event epoch) */
} moq_ns_sub_update_request_event_t;

typedef struct moq_subscribe_tracks_update_request_event {
    moq_track_sub_handle_t               handle;
    uint64_t                             update_request_id;
    const moq_namespace_update_record_t *update;   /* BORROWED (event epoch) */
} moq_subscribe_tracks_update_request_event_t;

/* How a namespace update concluded. OK: the responder accepted it; on the
 * requester a selected prefix takes effect here -- namespace notifications
 * after the acceptance name suffixes of the new prefix, earlier ones of the
 * old (the event is the barrier), and active namespaces outside the new
 * prefix leave the requester's set without a withdrawal. ERROR: the
 * responder refused it and closes the request (its FIN follows and the
 * request's own terminal reports the end). REFUSED: this side refused a peer
 * update without asking the application (an authorization token failure,
 * PREFIX_OVERLAP, or an unservable filter) and closed the request. */
typedef uint32_t moq_namespace_update_outcome_t;
#define MOQ_NAMESPACE_UPDATE_OUTCOME_OK      1u
#define MOQ_NAMESPACE_UPDATE_OUTCOME_ERROR   2u
#define MOQ_NAMESPACE_UPDATE_OUTCOME_REFUSED 3u

/* The conclusion of one update, emitted once. local_role is REQUESTER for
 * OK/ERROR (the peer answered this side's update) and RESPONDER for REFUSED.
 * error_code is the request error code (zero for OK); reason is BORROWED for
 * the event's epoch. has_expires / expires_ms report an EXPIRES the peer's
 * acceptance carried. After REFUSED the owner's handle is stale. Scalars
 * except reason; no owned resources. */
typedef struct moq_namespace_update_result_event {
    moq_announcement_t             ann;
    uint64_t                       update_request_id;
    moq_namespace_update_outcome_t outcome;
    moq_request_role_t             local_role;
    moq_request_error_t            error_code;
    moq_bytes_t                    reason;
    bool                           has_expires;
    uint8_t                        _reserved[7];
    uint64_t                       expires_ms;
} moq_namespace_update_result_event_t;

typedef struct moq_ns_sub_update_result_event {
    moq_ns_sub_handle_t            handle;
    uint64_t                       update_request_id;
    moq_namespace_update_outcome_t outcome;
    moq_request_role_t             local_role;
    moq_request_error_t            error_code;
    moq_bytes_t                    reason;
    bool                           has_expires;
    uint8_t                        _reserved[7];
    uint64_t                       expires_ms;
} moq_ns_sub_update_result_event_t;

typedef struct moq_subscribe_tracks_update_result_event {
    moq_track_sub_handle_t         handle;
    uint64_t                       update_request_id;
    moq_namespace_update_outcome_t outcome;
    moq_request_role_t             local_role;
    moq_request_error_t            error_code;
    moq_bytes_t                    reason;
    bool                           has_expires;
    uint8_t                        _reserved[7];
    uint64_t                       expires_ms;
} moq_subscribe_tracks_update_result_event_t;

typedef struct moq_fetch_gap_event {
    moq_fetch_t              fetch;
    moq_fetch_range_kind_t   range_kind;
    uint8_t                  _pad[4];
    uint64_t                 group_id;
    uint64_t                 object_id;
} moq_fetch_gap_event_t;

#define MOQ_STREAM_CHUNK_MAX 16384u

/* Reset code for delivery timeout on a data stream. */
#define MOQ_RESET_DELIVERY_TIMEOUT 0x2u

/* Close code for GOAWAY timeout (§3.4). */
#define MOQ_CLOSE_GOAWAY_TIMEOUT 0x10u
#define MOQ_CLOSE_IDLE_TIMEOUT   0x11u

/*
 * Inline detail capacity frozen at v1. Sized for near-term variants
 * (subscribe request/ok/error, object received, fetch, announce,
 * goaway, publish ready) with headroom.
 */
#define MOQ_EVENT_DETAIL_MAX 128

typedef struct moq_event {
    moq_event_kind_t kind;
    uint32_t         detail_size;  /* meaningful bytes in u */
    uint64_t         borrow_epoch;
    union moq_event_detail {
        moq_setup_complete_event_t  setup_complete;
        moq_session_closed_event_t  closed;
        moq_subscribe_request_event_t subscribe_request;
        moq_subscribe_ok_event_t    subscribe_ok;
        moq_subscribe_error_event_t subscribe_error;
        moq_request_ready_event_t  request_ready;
        moq_object_received_event_t object_received;
        moq_namespace_published_event_t  namespace_published;
        moq_namespace_done_event_t       namespace_done;
        moq_namespace_rejected_event_t   namespace_rejected;
        moq_namespace_accepted_event_t   namespace_accepted;
        moq_namespace_cancelled_event_t  namespace_cancelled;
        moq_goaway_event_t              goaway;
        moq_request_redirect_event_t    request_redirect;
        moq_request_goaway_event_t      request_goaway;
        moq_unsubscribed_event_t        unsubscribed;
        moq_subscribe_done_event_t      subscribe_done;
        moq_subscribe_updated_event_t   subscribe_updated;
        moq_object_chunk_event_t        object_chunk;
        moq_ns_sub_request_event_t      ns_sub_request;
        moq_ns_sub_ok_event_t          ns_sub_ok;
        moq_ns_sub_error_event_t       ns_sub_error;
        moq_namespace_found_event_t    namespace_found;
        moq_namespace_gone_event_t     namespace_gone;
        moq_fetch_request_event_t     fetch_request;
        moq_fetch_error_event_t       fetch_error;
        moq_fetch_cancelled_event_t   fetch_cancelled;
        moq_fetch_ok_event_t          fetch_ok;
        moq_fetch_complete_event_t    fetch_complete;
        moq_fetch_object_event_t      fetch_object;
        moq_fetch_gap_event_t         fetch_gap;
        moq_publish_request_event_t   publish_request;
        moq_publish_ok_event_t        publish_ok;
        moq_publish_error_event_t     publish_error;
        moq_publish_finished_event_t  publish_finished;
        moq_publish_unsubscribed_event_t publish_unsubscribed;
        moq_publish_updated_event_t     publish_updated;
        moq_subscription_update_ok_event_t subscription_update_ok;
        moq_publication_update_ok_event_t  publication_update_ok;
        moq_track_status_request_event_t track_status_request;
        moq_track_status_ok_event_t      track_status_ok;
        moq_track_status_error_event_t   track_status_error;
        moq_subscribe_tracks_request_event_t   subscribe_tracks_request;
        moq_subscribe_tracks_ok_event_t        subscribe_tracks_ok;
        moq_subscribe_tracks_error_event_t     subscribe_tracks_error;
        moq_publish_blocked_event_t            publish_blocked;
        moq_subscribe_tracks_cancelled_event_t subscribe_tracks_cancelled;
        moq_subgroup_finished_event_t          subgroup_finished;
        moq_subgroup_reset_event_t             subgroup_reset;
        moq_fetch_reset_event_t                fetch_reset;
        moq_track_status_terminated_event_t    track_status_terminated;
        moq_namespace_terminated_event_t       namespace_terminated;
        moq_ns_subscription_terminated_event_t ns_subscription_terminated;
        moq_publish_state_event_t              publish_state;
        moq_fill_created_event_t               fill_created;
        moq_fill_request_event_t               fill_request;
        moq_fill_object_event_t                fill_object;
        moq_fill_gap_event_t                   fill_gap;
        moq_fill_terminal_event_t              fill_terminal;
        moq_namespace_update_request_event_t        namespace_update_request;
        moq_ns_sub_update_request_event_t           ns_sub_update_request;
        moq_subscribe_tracks_update_request_event_t subscribe_tracks_update_request;
        moq_namespace_update_result_event_t         namespace_update_result;
        moq_ns_sub_update_result_event_t            ns_sub_update_result;
        moq_subscribe_tracks_update_result_event_t  subscribe_tracks_update_result;
        uint8_t                     _reserved[MOQ_EVENT_DETAIL_MAX];
    } u;
} moq_event_t;

/*
 * Size-aware drain of pending events. Same ABI semantics as
 * moq_session_poll_actions_ex.
 */
MOQ_API moq_result_t moq_session_poll_events_ex(moq_session_t *s,
                                                 void *out,
                                                 size_t cap,
                                                 size_t element_size,
                                                 size_t *out_count);

/* Convenience wrapper. */
static inline size_t
moq_session_poll_events(moq_session_t *s, moq_event_t *out, size_t cap)
{
    size_t n = 0;
    moq_session_poll_events_ex(s, out, cap, sizeof(moq_event_t), &n);
    return n;
}

/*
 * Release owned resources in a polled event. Idempotent.
 * OBJECT_RECEIVED, FETCH_OBJECT and FILL_OBJECT: decrefs payload and
 * properties, sets both NULL. OBJECT_CHUNK: decrefs chunk and properties,
 * sets both NULL. All other kinds (including SUBGROUP_FINISHED): no-op.
 * Callers MUST call this after processing or dropping every polled event.
 */
MOQ_API void moq_event_cleanup(moq_event_t *event);

/* -- Setup observation --------------------------------------------- */
/*
 * Peer's request capacity as received in setup. Defaults to 0
 * if the peer did not advertise it. Observing.
 */
MOQ_API uint64_t moq_session_peer_request_capacity(const moq_session_t *s);

/* D16 compatibility: equivalent to moq_session_peer_request_capacity.
 * Prefer the semantic name in new code. */
MOQ_API uint64_t moq_session_peer_max_request_id(const moq_session_t *s); /* api-boundary-exempt */

/*
 * Local request capacity as advertised in our setup. Observing.
 */
MOQ_API uint64_t moq_session_local_request_capacity(const moq_session_t *s);

/* D16 compatibility: equivalent to moq_session_local_request_capacity. */
MOQ_API uint64_t moq_session_local_max_request_id(const moq_session_t *s); /* api-boundary-exempt */

/*
 * Increase the local request capacity and notify the peer.
 * Advancing call.
 *
 * new_capacity must be greater than the current local capacity.
 * Commits the new capacity only after the notification is
 * successfully queued.
 *
 * Returns MOQ_ERR_INVAL if new_capacity <= current capacity.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full;
 * capacity is NOT advanced.
 */
MOQ_API moq_result_t moq_session_grant_request_capacity(
    moq_session_t *s,
    uint64_t new_capacity,
    uint64_t now_us);

/* D16 compatibility: equivalent to moq_session_grant_request_capacity. */
MOQ_API moq_result_t moq_session_update_max_request_id( /* api-boundary-exempt */
    moq_session_t *s,
    uint64_t max_request_id, /* api-boundary-exempt */
    uint64_t now_us);

/*
 * Peer's MAX_AUTH_TOKEN_CACHE_SIZE as received in setup.
 * Returns 0 if the peer did not send it. Observing.
 */
MOQ_API uint64_t moq_session_peer_auth_token_cache_size(const moq_session_t *s);

/* -- Subscribe configuration --------------------------------------- */

#define MOQ_SUBSCRIBE_OBJECT_DELIVERY_TIMEOUT   0x01u
#define MOQ_SUBSCRIBE_SUBGROUP_DELIVERY_TIMEOUT 0x02u

/*
 * Subscribe configuration.
 *
 * Required: track_namespace (draft-16: 1..32 non-empty parts; draft-18:
 *   0..32 non-empty parts), track_name,
 *   filter (must not be NONE).
 * Filter-dependent: start_group/start_object (ABSOLUTE_START,
 *   ABSOLUTE_RANGE), end_group (ABSOLUTE_RANGE only).
 * Optional with has_* flag: subscriber_priority (0 is valid,
 *   default 128), forward (false is valid, default true).
 * Default-means-unset: group_order (DEFAULT=0 uses publisher order).
 */
typedef struct moq_subscribe_cfg {
    uint32_t               struct_size;
    moq_namespace_t        track_namespace;  /* borrowed for the call */
    moq_bytes_t            track_name;       /* borrowed for the call */
    moq_subscribe_filter_t filter;
    uint64_t               start_group;
    uint64_t               start_object;
    uint64_t               end_group;
    bool                   has_subscriber_priority;
    uint8_t                subscriber_priority;
    moq_group_order_t      group_order;
    bool                   has_forward;
    bool                   forward;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    /* Appended (ABI-safe): request the original publisher start a new
     * group (largest known Group ID + 1; the value 0 -- "no group info" --
     * is meaningful, hence the has_ flag). Callers compiled against the
     * smaller struct send nothing. */
    bool                    has_new_group_request;
    uint64_t                new_group_request;
    /* Appended (ABI-safe): the subscription's filter in its semantic form
     * (borrowed for the call). When set it replaces the filter/start/end
     * fields above and admits the forms they cannot express: a relative start
     * more than one group back and an absolute end object. A form the
     * session's profile cannot carry on the wire is refused with
     * MOQ_ERR_UNSUPPORTED before anything is sent. */
    const moq_location_filter_cfg_t *location;
    /* Appended (ABI-safe): one fill request carried on the SUBSCRIBE (borrowed
     * for the call; see fill.h). NULL requests no fill. Refused with
     * MOQ_ERR_UNSUPPORTED on a profile without fills. */
    const moq_fill_cfg_t            *fill;
    /* Appended after the complete location/fill prefix. Read as one block
     * through subgroup_delivery_timeout_ms; a torn block is ignored. Unknown
     * flags or nonzero reserved are INVAL. Selected fields are exact
     * milliseconds, including 0 and UINT64_MAX. An absent field is omitted;
     * its value is ignored. Draft-16 has one DELIVERY_TIMEOUT (9.2.2.2):
     * both selected values must be equal and neither 0, or MOQ_ERR_INVAL
     * before advancing the session or consuming request/stream credit. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    uint32_t delivery_timeout_flags;
    uint32_t reserved;
    uint64_t object_delivery_timeout_ms;
    uint64_t subgroup_delivery_timeout_ms;
    /* Read together only when struct_size covers rendezvous_timeout_ms.
     * Selected presence, including an explicit zero, is D22-only and is
     * refused on older profiles before consuming request/output credit.
     * An absent value is ignored; a torn tail means absent. */
    bool has_rendezvous_timeout;
    uint64_t rendezvous_timeout_ms;
    /* INCLUDE_PROPERTIES (draft-22 §9.20.21), read together only when
     * struct_size covers include_properties; a torn pair is absent. Selected
     * (has_include_properties), the SUBSCRIBE carries the parameter: false
     * asks for a SUBSCRIBE_OK with empty Track Properties, true states the
     * default. Absent sends nothing and the answer carries the track's
     * properties. Selected on a profile without the parameter (draft-16/18)
     * is MOQ_ERR_UNSUPPORTED before anything is sent. */
    bool has_include_properties;
    bool include_properties;
} moq_subscribe_cfg_t;

/*
 * The symbol clears and stamps the layout through new_group_request -- the
 * struct as a caller built before `location` was appended holds it -- so it
 * never writes past such a caller's storage. A caller built against this
 * header reaches the whole struct: the function-like macro below passes its
 * sizeof to moq_subscribe_cfg_init_sized(), which clears and stamps
 * min(cfg_size, sizeof current).
 * A binding that calls the symbol instead of compiling the macro (an FFI, a
 * dlsym lookup) gets only that layout: fields it sets past it are ignored.
 * Such a binding calls moq_subscribe_cfg_init_sized() with the size of its struct.
 */
MOQ_API void moq_subscribe_cfg_init(moq_subscribe_cfg_t *cfg);
MOQ_API void moq_subscribe_cfg_init_sized(moq_subscribe_cfg_t *cfg,
                                          size_t cfg_size);
#define moq_subscribe_cfg_init(cfg) \
    moq_subscribe_cfg_init_sized((cfg), sizeof(*(cfg)))

/* Accepting a subscription. has_track_alias names the alias explicitly;
 * otherwise the session picks one no other subscription or publication of
 * this session uses, which is also what it does for several subscriptions
 * to one track (each then attributes its objects exactly). Where several
 * subscriptions to a track are allowed (draft-22), an explicit alias may be
 * one already given to another accepted subscription, or a publication, of
 * the SAME track; any other alias in use is MOQ_ERR_INVAL. The same holds
 * for an explicit alias on moq_session_publish. */
typedef struct moq_accept_subscribe_cfg {
    uint32_t    struct_size;
    bool        has_track_alias;
    uint64_t    track_alias;
    bool        has_largest;
    uint64_t    largest_group;
    uint64_t    largest_object;
    bool        has_expires;
    uint64_t    expires_ms;
    moq_bytes_t track_properties;  /* borrowed for the call; empty = none */
} moq_accept_subscribe_cfg_t;

MOQ_API void moq_accept_subscribe_cfg_init(moq_accept_subscribe_cfg_t *cfg);

/*
 * The group order in effect for a subscription (MOQT 7.1): the subscriber's,
 * else the publisher's advertised preference, else ascending. Never DEFAULT,
 * except for a stale handle.
 */
MOQ_API moq_group_order_t moq_session_subscription_group_order(
    const moq_session_t *s, moq_subscription_t sub);

/*
 * Build the track properties a publisher passes to an accept (subscription,
 * FETCH, TRACK_STATUS) or a PUBLISH to advertise its preferred group order
 * (ASCENDING or DESCENDING) as DEFAULT_PUBLISHER_GROUP_ORDER: `in` (may be
 * empty) is copied to `out` with the preference added, encoded for this
 * session's draft. An `in` that already carries the same preference is copied
 * unchanged; MOQ_ERR_INVAL if it carries another one or does not parse
 * (other properties are not checked here: an accept or PUBLISH checks the
 * whole set); MOQ_ERR_BUFFER if `out` is too small (the preference adds at
 * most 16 bytes). No session state changes.
 */
MOQ_API moq_result_t moq_session_track_properties_add_group_order(
    const moq_session_t *s, const uint8_t *in, size_t in_len,
    moq_group_order_t order, uint8_t *out, size_t out_cap, size_t *out_len);

/*
 * As moq_session_track_properties_add_group_order(), for the publisher's
 * delivery timeouts: OBJECT_ and SUBGROUP_DELIVERY_TIMEOUT (draft-18 12.1 and
 * 12.2, draft-22 10.1 and 10.2), or draft-16's single delivery timeout
 * (11.1), whose selected values must then be equal and neither 0.
 * At least one is selected. A set already carrying the same value is copied
 * unchanged; another value is MOQ_ERR_INVAL. They add at most 32 bytes.
 */
MOQ_API moq_result_t moq_session_track_properties_add_delivery_timeouts(
    const moq_session_t *s, const uint8_t *in, size_t in_len,
    bool has_object, uint64_t object_ms, bool has_subgroup,
    uint64_t subgroup_ms, uint8_t *out, size_t out_cap, size_t *out_len);

/*
 * REDIRECT (§10.6.1) target, set on a reject cfg only when
 * error_code == MOQ_REQUEST_ERROR_REDIRECT. All fields may be empty (an all-empty
 * Redirect reuses the current session URI and the original namespace/name). A
 * CLIENT must leave connect_uri empty; namespace-scoped families
 * (ANNOUNCEMENT, NS_SUB) must leave track_name empty. Bytes borrow for the call.
 */
typedef struct moq_redirect_target {
    moq_bytes_t     connect_uri;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
} moq_redirect_target_t;

/*
 * Where a request went when it ended by migration: a REDIRECT refusal
 * (redirect: the peer answered with Redirect, §9.4.1 / draft-18 §10.6.1) or
 * a GOAWAY on the request's own stream (goaway: re-issue the request at the
 * new session, draft-22 §9.2). The facades keep it for a track that ended
 * this way (moq_sub_get_track_migration, moq_pub_get_track_migration).
 *
 * target.connect_uri: where to re-issue; empty means the current session's
 * URI. target.track_namespace / track_name (REDIRECT only): the Redirect
 * target exactly as received. On draft-18, when both are empty the
 * redirected request uses the original request's namespace and name; when
 * either is not, both are the literal values to use (draft-18 §10.6.1). On
 * draft-22 they are the literal Redirect target (§9.4.1). can_retry /
 * retry_after_ms (REDIRECT only): the refusal's retry interval. timeout_ms
 * (GOAWAY only): the sender's migration timeout hint, zero when it named
 * none. Spans borrow facade-owned storage valid until the track is released
 * or its facade destroyed, and on the publisher facade also only until the
 * track is published again successfully (moq_pub_publish_track).
 */
typedef struct moq_request_migration {
    uint32_t              struct_size;
    bool                  redirect;
    bool                  goaway;
    bool                  can_retry;
    uint8_t               _reserved;
    uint64_t              retry_after_ms;
    uint64_t              timeout_ms;
    moq_redirect_target_t target;
} moq_request_migration_t;

typedef struct moq_reject_subscribe_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    moq_bytes_t         reason;        /* borrowed for the call */
    bool                can_retry;
    uint64_t            retry_after_ms;
    moq_redirect_target_t redirect;    /* ABI-safe append; REDIRECT only */
} moq_reject_subscribe_cfg_t;

MOQ_API void moq_reject_subscribe_cfg_init(moq_reject_subscribe_cfg_t *cfg);

/* -- Subscribe operations ------------------------------------------ */

/*
 * Initiate a subscription (subscriber side). Advancing call.
 *
 * On success, queues the subscription request and mints a handle.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 *
 * Several subscriptions to one track: draft-22 allows them, each with its
 * own handle and filter (moq_session_supports_concurrent_subscriptions).
 * The publisher may give them one shared alias, also shared with a
 * publication of the same track it started; an object arriving under it is
 * then delivered once to every subscription or publication whose filter
 * (location window and forward state) accepts it, as separate events naming
 * each one, and not at all when none does. Filters that overlap therefore
 * see an object once per copy the publisher sent under that alias: removing
 * such duplicates is the application's. While a subscription to the track
 * still awaits its answer, an object under such an alias that its filter
 * would accept is also held for it, within the session's bounded
 * early-data buffer, and delivered after its subscribe-ok event if that
 * answer names the same alias (discarded otherwise); objects delivered as
 * chunks (streaming_objects) are not held. Drafts 16 and 18 allow one
 * subscription per track: a second returns MOQ_ERR_INVAL before anything
 * is sent.
 */
MOQ_API moq_result_t moq_session_subscribe(moq_session_t *s,
                                            const moq_subscribe_cfg_t *cfg,
                                            uint64_t now_us,
                                            moq_subscription_t *out_handle);

/*
 * Accept an incoming subscription (publisher side). Advancing call.
 * Queues a positive response. Transitions subscription to Established.
 */
MOQ_API moq_result_t moq_session_accept_subscribe(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_accept_subscribe_cfg_t *cfg,
    uint64_t now_us);

/*
 * Reject an incoming subscription (publisher side). Advancing call.
 * Queues an error response. Transitions subscription to Terminated.
 */
MOQ_API moq_result_t moq_session_reject_subscribe(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_reject_subscribe_cfg_t *cfg,
    uint64_t now_us);

/*
 * Unsubscribe from a subscription (subscriber side). Advancing call.
 * Queues a cancellation message. Frees the local subscription entry.
 * Every data stream of the subscription still open is stopped with it
 * (STOP_DATA, CANCELLED) and its receive entry freed; whatever the peer had
 * already sent on it is discarded without events. Those stops need no room
 * in the action queue: one that does not fit now is queued once actions are
 * polled.
 *
 * Valid for subscriber-role subscriptions in PENDING_SUBSCRIBER or
 * ESTABLISHED state. Returns MOQ_ERR_STALE_HANDLE if the handle is
 * invalid, MOQ_ERR_WRONG_STATE if wrong role/state.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full or, where the
 * cancel is a control message (draft-16, draft-18), the send buffer still
 * holds earlier output that polling the actions releases; the handle remains
 * valid for retry. MOQ_ERR_BUFFER only when the message could never fit the
 * send buffer.
 */
MOQ_API moq_result_t moq_session_unsubscribe(moq_session_t *s,
                                              moq_subscription_t sub,
                                              uint64_t now_us);

/*
 * Finish publishing to an accepted subscription (publisher side).
 * Queues a completion message and frees the local subscription entry.
 * All open subgroups for this subscription must be closed or reset
 * before calling.
 *
 * stream_count is the number of data streams the APPLICATION opened for
 * this subscription (every subgroup it opened as a stream, including ones
 * that carried no object). The session adds the fill fetch streams it opened itself
 * (through moq_session_open_fill, and for a fill it answered as a failed
 * stream), so the application never counts those. The sum saturates to
 * the profile's "unknown" sentinel (2^62-1 for draft-16/18 and 2^64-1 for
 * draft-22); passing that sentinel sends it unchanged, meaning the exact
 * count is not known.
 *
 * Valid for publisher-role subscriptions in ESTABLISHED state.
 * Returns MOQ_ERR_WRONG_STATE if subgroups are still open, or while an
 * answer to a subscription update is owed (answer it first: a rejection
 * itself ends the subscription with UPDATE_FAILED).
 * Returns MOQ_ERR_STALE_HANDLE once the session has ended the subscription
 * itself -- e.g. an update it refused on its own, which it ends with
 * UPDATE_FAILED -- whether or not that terminal and the resets before it
 * have gone out yet: the session still sends them. A stale handle is not
 * proof that any byte or FIN reached the peer.
 */
typedef struct moq_done_subscribe_cfg {
    uint32_t    struct_size;
    uint64_t    status_code;
    uint64_t    stream_count;
    moq_bytes_t reason;
} moq_done_subscribe_cfg_t;

MOQ_API void moq_done_subscribe_cfg_init(moq_done_subscribe_cfg_t *cfg);

MOQ_API moq_result_t moq_session_done_subscribe(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_done_subscribe_cfg_t *cfg,
    uint64_t now_us);

/* -- Subscription update ------------------------------------------ */

#define MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT   UINT32_C(1)
#define MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT UINT32_C(2)
typedef struct moq_subscription_update_cfg {
    uint32_t struct_size;
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_forward;
    bool     forward;
    bool     has_delivery_timeout;
    uint64_t delivery_timeout_us;
    /* Appended (ABI-safe): authorization tokens carried on the update,
     * by value, the same shape as subscribe/fetch. Callers compiled
     * against the smaller struct send no tokens. Semantic token validity
     * is the receiver's call (the sender stays permissive). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    /* Appended (ABI-safe): request the original publisher start a new
     * group (largest known Group ID + 1; 0 = "no group info" and is
     * meaningful, hence the has_ flag). Valid only when the track carried
     * dynamic-group support; otherwise MOQ_ERR_INVAL. Callers compiled
     * against the smaller struct send nothing. */
    bool                    has_new_group_request;
    uint64_t                new_group_request;
    /* Appended: update the subscription's filter (both drafts carry it
     * as a parameter on the subscription-update request). Whole-BLOCK
     * gate: the block is read only when struct_size covers THROUGH end_group,
     * so a caller whose struct ends mid-block can never send a torn filter.
     * Location fields are used by the ABSOLUTE_* forms only; values set on
     * the relative forms are IGNORED, exactly like moq_session_subscribe
     * (the wire carries no locations for them). For ABSOLUTE_RANGE,
     * end_group < start_group is MOQ_ERR_INVAL before any mutation. */
    bool                   has_filter;
    moq_subscribe_filter_t filter;
    uint64_t               start_group;
    uint64_t               start_object;
    uint64_t               end_group;
    /* Independent pointer gates, AFTER the complete legacy input extent.
     * Borrowed only for the call, copied before any effects. A non-NULL
     * location overrides inactive legacy filter fields; NONE removes the
     * restriction, whereas NULL falls back to the selected legacy filter.
     * Typed-options profiles only; otherwise selected tails are UNSUPPORTED. */
    const moq_location_filter_cfg_t *location;
    /* Presence requests one independently owned child; a new fill never
     * cancels an earlier one. Omitted nested values inherit the effective
     * post-update parent snapshot (see fill.h). */
    const moq_fill_cfg_t *fill;
    /* Whole block gate through subgroup_delivery_timeout_ms. Unknown flags
     * or nonzero reserved are INVAL when covered; a torn block is ignored.
     * Exact milliseconds, including 0 and UINT64_MAX; an absent counterpart
     * remains unchanged. The generic delivery_timeout_us is UNSUPPORTED on
     * typed-options profiles, even when selected with a zero value, and
     * MOQ_ERR_INVAL together with these. Draft-16 has one DELIVERY_TIMEOUT:
     * both selected values must be equal and neither 0. */
    uint32_t delivery_timeout_flags;
    uint32_t reserved;
    uint64_t object_delivery_timeout_ms;
    uint64_t subgroup_delivery_timeout_ms;
} moq_subscription_update_cfg_t;

/* Pointer-only initializer: zeroes and stamps ONLY the frozen original
 * prefix (struct_size .. delivery_timeout_us -- the layout before the
 * auth-token append). It cannot know the caller's storage size, so it must
 * not write the current sizeof. All appended fields (auth tokens, new-group
 * request, filter, location, fill, independent timeouts) stay disabled; use
 * moq_subscription_update_cfg_init_sized(). */
MOQ_API void moq_subscription_update_cfg_init(
    moq_subscription_update_cfg_t *cfg);

/* Size-aware initializer: zeroes and stamps min(cfg_size, sizeof current
 * struct). Pass sizeof(moq_subscription_update_cfg_t) to initialize the
 * full current struct with all appended fields available. No-op if cfg is
 * NULL or cfg_size cannot hold struct_size. */
MOQ_API void moq_subscription_update_cfg_init_sized(
    moq_subscription_update_cfg_t *cfg, size_t cfg_size);

/*
 * The filter in effect on a subscription as this side knows it: what the
 * request carried, replaced by accepted updates and by the publisher's state
 * notifications. Not an advancing call. MOQ_ERR_STALE_HANDLE for a retired
 * handle; the kind is MOQ_LOCATION_FILTER_NONE when unfiltered.
 *
 * `out` is written like every size-aware libmoq output: the _sized call
 * writes min(out_size, sizeof current) bytes and stamps struct_size with that
 * count, and refuses (MOQ_ERR_INVAL, nothing written) an out_size below
 * MOQ_LOCATION_FILTER_CFG_V0_SIZE. The symbol without the suffix writes the
 * v0 layout only, so it never writes past a caller built against an earlier
 * moq_location_filter_cfg_t; the function-like macro of the same name passes
 * the caller's sizeof to the _sized call. A binding that calls the symbol
 * instead of compiling the macro gets the v0 fields only, and calls the
 * _sized form with the size of its struct to receive more.
 */
MOQ_API moq_result_t moq_session_subscription_filter(moq_session_t *s,
    moq_subscription_t sub, moq_location_filter_cfg_t *out);
MOQ_API moq_result_t moq_session_subscription_filter_sized(moq_session_t *s,
    moq_subscription_t sub, moq_location_filter_cfg_t *out, size_t out_size);
#define moq_session_subscription_filter(s, sub, out) \
    moq_session_subscription_filter_sized((s), (sub), (out), \
                                          sizeof(moq_location_filter_cfg_t))

/*
 * The Location window the filter in effect resolved to: the Locations the
 * subscription admits, with a relative start (Largest Object, Next Group, a
 * relative group, Next Object) turned into a fixed Location against the
 * Largest Object the session resolved it with -- on the publisher side the
 * one its answer (or the acknowledgement of an update) carried, on the
 * subscriber side the one it received. Not an advancing call.
 *
 * resolved: false while no window has been resolved (before the answer);
 * every Location is then inside. empty: resolved, and no Location is inside
 * (a relative start past the largest representable Location). has_end:
 * end_group bounds the window; has_end_object: the end is the inclusive
 * end_object within end_group, otherwise the whole of end_group is inside.
 *
 * The subscriber side applies this window to what it delivers (draft-22
 * §3.3.3, Pass = Forward AND Location Filters). On the publisher side the
 * session does not refuse an object outside it: "A publisher MUST NOT send
 * objects from outside the requested range" (draft-22 §3.3.1, draft-18
 * §5.1.2, draft-16 §9.9) is a duty of the application that chooses what to
 * publish -- the publisher facade applies this window -- and a publisher
 * writing to the raw session checks its objects against it.
 *
 * `out` is written like every size-aware libmoq output: min(out_size,
 * sizeof current) bytes, struct_size stamped with that count; an out_size
 * that cannot hold the fields through end_object is MOQ_ERR_INVAL.
 * MOQ_ERR_STALE_HANDLE for a retired handle.
 */
typedef struct moq_filter_window {
    uint32_t struct_size;
    bool     resolved;
    bool     empty;
    bool     has_end;
    bool     has_end_object;
    uint64_t start_group;
    uint64_t start_object;
    uint64_t end_group;
    uint64_t end_object;
} moq_filter_window_t;

MOQ_API moq_result_t moq_session_subscription_window(moq_session_t *s,
    moq_subscription_t sub, moq_filter_window_t *out, size_t out_size);

/*
 * A publisher's unilateral state notification on a subscription it serves:
 * the Forward value and/or filter currently in effect. The Largest Object this
 * side knows for the track is included automatically. Only profiles with a
 * wire form for it accept the call (MOQ_ERR_UNSUPPORTED otherwise). The
 * subscription must be established with this side as publisher.
 *
 * `filter` is the filter in effect, borrowed for the call; NULL leaves
 * the filter unchanged. At least one of has_forward and filter is required.
 * Each selected value must match the effective subscriber-authorized state;
 * otherwise MOQ_ERR_INVAL is returned without output or state changes. Change
 * subscriber intent through UPDATE and its successful ACK first; a pending
 * UPDATE is not authorization. Equal semantic filters are permitted, ignoring
 * inactive fields. A notification neither answers an UPDATE nor changes the
 * saved filter window (including relative/Next Object resolution).
 */
typedef struct moq_publish_state_cfg {
    uint32_t struct_size;
    bool     has_forward;
    bool     forward;
    uint8_t  _pad[2];
    const moq_location_filter_cfg_t *filter;
} moq_publish_state_cfg_t;

/* Frozen v0 layout: everything through filter. A consuming call refuses a
 * struct_size below it and reads a later field only when struct_size covers
 * the whole field. */
#define MOQ_PUBLISH_STATE_CFG_V0_SIZE \
    (offsetof(moq_publish_state_cfg_t, filter) + \
     sizeof(const moq_location_filter_cfg_t *))

/*
 * The symbol clears and stamps the v0 layout only, so it never writes past a
 * caller built against it. The function-like macro below passes the
 * caller's sizeof to moq_publish_state_cfg_init_sized(), which clears and
 * stamps min(cfg_size, sizeof current); cfg_size below struct_size is a no-op.
 * A binding that calls the symbol instead of compiling the macro (an FFI, a
 * dlsym lookup) gets only that layout: fields it sets past it are ignored.
 * Such a binding calls moq_publish_state_cfg_init_sized() with the size of its struct.
 */
MOQ_API void moq_publish_state_cfg_init(moq_publish_state_cfg_t *cfg);
MOQ_API void moq_publish_state_cfg_init_sized(moq_publish_state_cfg_t *cfg,
                                             size_t cfg_size);
#define moq_publish_state_cfg_init(cfg) \
    moq_publish_state_cfg_init_sized((cfg), sizeof(*(cfg)))

MOQ_API moq_result_t moq_session_notify_subscription_state(moq_session_t *s,
    moq_subscription_t sub, const moq_publish_state_cfg_t *cfg, uint64_t now_us);

/* The same notification from the publisher of a PUBLISH-initiated subscription. */
MOQ_API moq_result_t moq_session_notify_publication_state(moq_session_t *s,
    moq_publication_t pub, const moq_publish_state_cfg_t *cfg, uint64_t now_us);

/*
 * Update an active subscription's parameters. Advancing call.
 * Consumes request capacity.
 *
 * Returns MOQ_ERR_STALE_HANDLE if the subscription handle is invalid.
 * Returns MOQ_ERR_WRONG_STATE if the subscription is not ESTABLISHED
 * with role SUBSCRIBER.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full, or queued output
 * occupies send-buffer space the update would fit after a drain.
 * Returns MOQ_ERR_BUFFER if the encoded update exceeds the session's send
 * capacity. Returns MOQ_ERR_NOMEM if the capacity-probe allocation fails.
 * An empty refresh is valid on typed-options profiles; legacy profiles return
 * MOQ_ERR_INVAL. An ACK without EXPIRES makes no renewal or timer promise.
 */
MOQ_API moq_result_t moq_session_update_subscription(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_subscription_update_cfg_t *cfg,
    uint64_t now_us);

/*
 * Answering a subscriber's subscription update (publisher side), for sessions
 * created with moq_session_cfg_t.defer_subscription_updates. Each answers the
 * update a MOQ_EVENT_SUBSCRIBE_UPDATED with answer_owed named; at most one
 * answer is owed per subscription at a time (see update_count on the event
 * for coalesced updates).
 *
 * update_request_id must be the event's update_request_id. An answer naming
 * an older update -- one a later coalesced event superseded -- returns
 * MOQ_ERR_STALE_HANDLE and changes nothing; answer the newest event.
 */
typedef struct moq_accept_subscription_update_cfg {
    uint32_t struct_size;
    uint64_t update_request_id;
} moq_accept_subscription_update_cfg_t;

typedef struct moq_reject_subscription_update_cfg {
    uint32_t            struct_size;
    uint64_t            update_request_id;
    moq_request_error_t error_code;  /* REDIRECT is refused */
    moq_bytes_t         reason;      /* borrowed for the call; at most 1024 bytes */
} moq_reject_subscription_update_cfg_t;

/* Frozen v0 layouts, each through its last field. An answering call refuses
 * a struct_size below it and reads a later field only when struct_size
 * covers the whole field. */
#define MOQ_ACCEPT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE \
    (offsetof(moq_accept_subscription_update_cfg_t, update_request_id) + \
     sizeof(uint64_t))
#define MOQ_REJECT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE \
    (offsetof(moq_reject_subscription_update_cfg_t, reason) + \
     sizeof(moq_bytes_t))

/*
 * Each symbol clears and stamps its v0 layout only, so it never writes past
 * a caller built against it. The function-like macros below pass the
 * caller's sizeof to the _sized initializer, which clears and stamps
 * min(cfg_size, sizeof current); cfg_size below struct_size is a no-op.
 * A binding that calls a symbol instead of compiling its macro (an FFI, a
 * dlsym lookup) gets only the v0 layout: fields it sets past it are ignored.
 * Such a binding calls the _sized initializer with the size of its struct.
 */
MOQ_API void moq_accept_subscription_update_cfg_init(
    moq_accept_subscription_update_cfg_t *cfg);
MOQ_API void moq_accept_subscription_update_cfg_init_sized(
    moq_accept_subscription_update_cfg_t *cfg, size_t cfg_size);
#define moq_accept_subscription_update_cfg_init(cfg) \
    moq_accept_subscription_update_cfg_init_sized((cfg), sizeof(*(cfg)))

MOQ_API void moq_reject_subscription_update_cfg_init(
    moq_reject_subscription_update_cfg_t *cfg);
MOQ_API void moq_reject_subscription_update_cfg_init_sized(
    moq_reject_subscription_update_cfg_t *cfg, size_t cfg_size);
#define moq_reject_subscription_update_cfg_init(cfg) \
    moq_reject_subscription_update_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Accept the owed update. Advancing call. Applies everything the covered
 * updates ask for -- forward state, subscriber priority, delivery timeouts,
 * the replacement filter and a fill request -- and queues one acknowledgement per
 * covered update, each carrying that Largest Object.
 * D22 retains Largest and the fill's inherited values when the update is
 * processed, across deferred acceptance and retries. D16/18 retain their
 * accept-time Largest behavior.
 *
 * Returns MOQ_ERR_STALE_HANDLE for a dead handle or a superseded
 * update_request_id, MOQ_ERR_WRONG_STATE when the subscription is not a
 * publisher-role ESTABLISHED one or no answer is owed, and
 * MOQ_ERR_WOULD_BLOCK when the action queue or send buffer cannot take the
 * whole answer; nothing is applied or queued then and the call can be
 * retried.
 */
MOQ_API moq_result_t moq_session_accept_subscription_update(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_accept_subscription_update_cfg_t *cfg,
    uint64_t now_us);

/*
 * Reject the owed update. Advancing call. Nothing the covered updates asked
 * for is applied and no fill is opened. Queues the refusal with error_code
 * and reason, then ends the subscription with status UPDATE_FAILED,
 * resetting its open data and fill streams, and frees it: the handle is
 * stale afterwards.
 *
 * Returns as moq_session_accept_subscription_update(), plus MOQ_ERR_INVAL
 * for a code this profile cannot encode, REDIRECT, or a reason longer than
 * 1024 bytes. On MOQ_ERR_WOULD_BLOCK nothing is queued or reset and the call
 * can be retried.
 */
MOQ_API moq_result_t moq_session_reject_subscription_update(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_reject_subscription_update_cfg_t *cfg,
    uint64_t now_us);

/*
 * The same two answers for an update of a subscription this side's PUBLISH
 * began (MOQ_EVENT_PUBLISH_UPDATED with answer_owed), with the same
 * configuration, coalescing, results and atomicity as the subscription
 * calls above. A rejection ends the publication with status UPDATE_FAILED,
 * resetting its open data and fill streams, and frees it: the handle is
 * stale afterwards.
 */
MOQ_API moq_result_t moq_session_accept_publication_update(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_accept_subscription_update_cfg_t *cfg,
    uint64_t now_us);

MOQ_API moq_result_t moq_session_reject_publication_update(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_reject_subscription_update_cfg_t *cfg,
    uint64_t now_us);

/*
 * The update whose answer is owed on a publisher-role subscription, for a
 * component that answers without the event at hand: the Request ID the
 * answer names (the newest covered update) and whether the cumulative update
 * carries a replacement filter, with that filter in the semantic form
 * moq_session_subscription_filter() reports (which keeps reporting the
 * filter in effect until the update is accepted). Each out-pointer may be
 * NULL; out_filter is written only when the update carries a filter. Not an
 * advancing call. MOQ_ERR_STALE_HANDLE for a dead handle, including a
 * subscription the session has ended itself (an update it refused on its
 * own) whose UPDATE_FAILED terminal or preceding resets it has not sent yet
 * -- stale is not proof that they reached the peer; MOQ_ERR_WRONG_STATE when
 * no answer is owed.
 *
 * out_filter is written as moq_session_subscription_filter() writes `out`:
 * the _sized call writes min(out_filter_size, sizeof current) and refuses a
 * non-NULL out_filter with a size below MOQ_LOCATION_FILTER_CFG_V0_SIZE
 * (MOQ_ERR_INVAL, nothing written); the symbol without the suffix writes the
 * v0 layout; the same-name macro passes the caller's sizeof.
 */
MOQ_API moq_result_t moq_session_subscription_owed_update(
    moq_session_t *s, moq_subscription_t sub, uint64_t *out_update_request_id,
    bool *out_has_filter, moq_location_filter_cfg_t *out_filter);
MOQ_API moq_result_t moq_session_subscription_owed_update_sized(
    moq_session_t *s, moq_subscription_t sub, uint64_t *out_update_request_id,
    bool *out_has_filter, moq_location_filter_cfg_t *out_filter,
    size_t out_filter_size);
#define moq_session_subscription_owed_update(s, sub, out_id, out_has, out_filter) \
    moq_session_subscription_owed_update_sized((s), (sub), (out_id), (out_has), \
        (out_filter), sizeof(moq_location_filter_cfg_t))

/*
 * The same observation for a publisher-role publication (a subscription this
 * side began with PUBLISH). Reports the newest cumulative owed update's
 * Request ID and selected replacement filter, including before its
 * MOQ_EVENT_PUBLISH_UPDATED is polled. The selected filter is not the current
 * effective filter: moq_session_publication_filter() keeps reporting that
 * until the update is accepted. This does not report initial PUBLISH inputs.
 *
 * Every out-pointer is optional. out_filter is untouched when no replacement
 * is selected; an explicit NONE is a selected replacement. The query does
 * not allocate, advance time or the borrow epoch, consume events/actions, or
 * clear the obligation. It remains observable after the event is polled.
 *
 * MOQ_ERR_INVAL for NULL s or a non-NULL filter output smaller than
 * MOQ_LOCATION_FILTER_CFG_V0_SIZE; MOQ_ERR_STALE_HANDLE for a stale, foreign
 * or wrong-pool handle, and for a publication the session has ended itself
 * (an update it refused on its own) whose UPDATE_FAILED terminal or
 * preceding resets it has not sent yet -- stale is not proof that they
 * reached the peer; MOQ_ERR_WRONG_STATE for subscriber role or no owed
 * answer. All requested outputs are unchanged on refusal. The _sized form
 * writes/stamps min(out_filter_size, sizeof current) when a filter is selected;
 * the v0 symbol writes only the frozen 48-byte filter and the same-name macro
 * passes the caller's size, as moq_session_subscription_owed_update() does.
 */
MOQ_API moq_result_t moq_session_publication_owed_update(
    moq_session_t *s, moq_publication_t pub, uint64_t *out_update_request_id,
    bool *out_has_filter, moq_location_filter_cfg_t *out_filter);
MOQ_API moq_result_t moq_session_publication_owed_update_sized(
    moq_session_t *s, moq_publication_t pub, uint64_t *out_update_request_id,
    bool *out_has_filter, moq_location_filter_cfg_t *out_filter,
    size_t out_filter_size);
#define moq_session_publication_owed_update(s, pub, out_id, out_has, out_filter) \
    moq_session_publication_owed_update_sized((s), (pub), (out_id), (out_has), \
        (out_filter), sizeof(moq_location_filter_cfg_t))

/* Observe the D22 processing snapshot of a publisher-role owed update, before
 * or after polling its event. update_request_id must name the newest covered
 * update. This does not allocate, advance time/borrow epoch, consume output or
 * clear the obligation. Automatic answers are observed through event options.
 *
 * Validation order: INVAL for NULL s/out or out_size below the frozen v0 floor;
 * STALE_HANDLE for an invalid typed handle; WRONG_STATE for subscriber role;
 * UNSUPPORTED for a legacy profile; WRONG_STATE for no established owed answer;
 * STALE_HANDLE for a superseded expected ID. Refusal leaves output unchanged.
 * Success copies/stamps min(out_size, library sizeof); future bytes are untouched.
 * Existing owed-update queries continue to report raw selected intent. */
MOQ_API moq_result_t moq_session_get_subscription_update_resolution(
    moq_session_t *s, moq_subscription_t sub, uint64_t update_request_id,
    moq_update_resolution_t *out, size_t out_size);
MOQ_API moq_result_t moq_session_get_publication_update_resolution(
    moq_session_t *s, moq_publication_t pub, uint64_t update_request_id,
    moq_update_resolution_t *out, size_t out_size);

/*
 * Switch moq_session_cfg_t.defer_subscription_updates after creation, for a
 * component that answers updates on a session it did not create. Takes
 * effect for updates that arrive afterwards; an answer already owed stays
 * owed whatever the setting and must still be given. Not an advancing call.
 * MOQ_ERR_INVAL for a NULL session.
 */
MOQ_API moq_result_t moq_session_set_defer_subscription_updates(moq_session_t *s,
                                                                 bool defer);

/*
 * Switch moq_session_cfg_t.defer_fetch_ranges after creation, for a component
 * that serves FETCHes on a session it did not create. Takes effect for FETCH
 * requests that arrive afterwards; one already surfaced keeps the form it was
 * surfaced in. Not an advancing call. MOQ_ERR_INVAL for a NULL session.
 */
MOQ_API moq_result_t moq_session_set_defer_fetch_ranges(moq_session_t *s,
                                                        bool defer);

/* -- FETCH --------------------------------------------------------- */

/*
 * FETCH configuration (subscriber/fetcher side).
 *
 * Range: [start_group:start_object, end_group:end_object) — exclusive end.
 * end_object == 0 means the entire end_group is requested.
 */
typedef struct moq_fetch_cfg {
    uint32_t        struct_size;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    uint64_t        start_group;
    uint64_t        start_object;
    uint64_t        end_group;
    uint64_t        end_object;
    moq_group_order_t group_order;
    bool            has_subscriber_priority;
    uint8_t         subscriber_priority;
    bool            is_joining;
    bool            joining_relative;
    moq_subscription_t joining_sub;
    uint64_t        joining_start;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    /* Appended: join a PUBLISH-initiated subscription instead of a
     * SUBSCRIBE-initiated one. A subscription can be initiated either way, so
     * a peer that accepted a PUBLISH holds a subscription a Joining FETCH may
     * name -- but it holds it as a publication handle, not a subscription
     * handle. This field names it directly.
     *
     * For a joining fetch EXACTLY ONE of joining_sub / joining_pub must be
     * named; both or neither is MOQ_ERR_INVAL, checked before anything is
     * mutated. "Named" means non-zero, not well-formed: a stale or malformed
     * handle still selects its discriminator and is answered
     * MOQ_ERR_STALE_HANDLE by the resolver. Reaching this field requires
     * moq_fetch_cfg_init_sized() with the full struct size.
     *
     * BOTH draft profiles are supported, with DIFFERENT eligibility rules --
     * the joined publication must satisfy the rule of the negotiated profile:
     *
     *   draft-18 (5.1, 10.12.2): the publication must be Established with
     *   Forward State 1, and it supplies a saved Joining Location -- the
     *   Largest carried by the publish request that established it, or by
     *   the acknowledgement of a publication update that raised Forward
     *   from 0 to 1. Joining while Forward is 0 gives
     *   MOQ_REQUEST_ERROR_INVALID_RANGE; joining before the publication is
     *   established -- Pending (Publisher) -- gives
     *   MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID and is NOT buffered.
     *
     *   draft-16 (5.1, 9.16.2): the publication must have negotiated
     *   the Largest Object subscription filter when it was accepted, and the
     *   saved location is the establishment-time Largest -- no later update
     *   moves it. A RECEIVED join against a publication with any other filter is a
     *   protocol violation and closes the session, as 9.16.2 requires; it is
     *   not a request error.
     *
     * This is a transport capability in its own right. It does NOT satisfy
     * MSF-01 section 5, which requires SUBSCRIBE with a Joining FETCH
     * (offset = 0) literally. */
    moq_publication_t       joining_pub;
    /* Appended: the range of a standalone fetch in its semantic form
     * (borrowed for the call). When set it replaces the start/end fields
     * above and admits the forms they cannot express: a start relative to
     * the publisher's Largest Object (RELATIVE_GROUP, NEXT_OBJECT), an open
     * end (ABSOLUTE_START, which ends at the Largest Object), and NONE, the
     * whole track through the Largest Object. The publisher resolves those
     * forms when it processes the request. Only a profile whose FETCH
     * carries a Location Filter can send it: elsewhere the call returns
     * MOQ_ERR_UNSUPPORTED before anything is sent. MOQ_ERR_INVAL for a
     * joining fetch, a struct_size below MOQ_LOCATION_FILTER_CFG_V0_SIZE is
     * MOQ_ERR_ABI_MISMATCH, and a bounded form whose end group precedes its
     * start group is MOQ_ERR_INVAL. Requires moq_fetch_cfg_init_sized(). */
    const moq_location_filter_cfg_t *location;
    /* Appended: the fetch's FILL_TIMEOUT, the most a relay should spend
     * waiting for Objects that are not immediately available before it
     * reports them as Timed-Out gaps; zero asks for only what is available
     * at once. Absent (has_fill_timeout false), the relay uses its own wait.
     * The same profile rule as `location` applies (MOQ_ERR_UNSUPPORTED
     * elsewhere, for a standalone fetch; MOQ_ERR_INVAL for a joining one). */
    bool                     has_fill_timeout;
    uint64_t                 fill_timeout_ms;
    /* Appended: INCLUDE_PROPERTIES (draft-22 §9.20.21), read together only
     * when struct_size covers include_properties. Selected, the FETCH carries
     * the parameter: false asks for an accepting answer with empty Track
     * Properties. The same profile rule as `location` applies
     * (MOQ_ERR_UNSUPPORTED on a profile without the parameter, for a
     * standalone fetch; MOQ_ERR_INVAL for a joining one). */
    bool                     has_include_properties;
    bool                     include_properties;
} moq_fetch_cfg_t;

/*
 * Frozen v0 prefix: everything through joining_start, i.e. the layout BEFORE
 * the first appended field. auth_tokens/auth_token_count were themselves
 * appended after that original layout, and joining_pub after them, so the
 * floor sits immediately before auth_tokens -- exactly as the adjacent
 * MOQ_PUBLISH_CFG_V0_SIZE does for PUBLISH.
 *
 * The pointer-only init stamps exactly this, so a caller that allocated the
 * ORIGINAL struct is never written past its own storage. Reaching either
 * auth-token field, or joining_pub, requires moq_fetch_cfg_init_sized().
 */
#define MOQ_FETCH_CFG_V0_SIZE offsetof(moq_fetch_cfg_t, auth_tokens)

/* The frozen prefix must end exactly where the appended field begins; that is
 * asserted at compile time in session_fetch.c (this header is also consumed by
 * pre-C11 translation units, where _Static_assert is not available). */

/* Initialize the frozen v0 prefix only. Use the sized form to reach the
 * appended auth-token fields or joining_pub. */
MOQ_API void moq_fetch_cfg_init(moq_fetch_cfg_t *cfg);

/*
 * Sized init: clears and stamps exactly min(cfg_size, sizeof(*cfg)) bytes and
 * never writes past what the caller supplied. A cfg_size too small to hold
 * struct_size itself touches NOTHING and leaves the storage unchanged -- it is
 * deliberately not inflated to the v0 floor, since that would overwrite a
 * caller allocation smaller than the floor. A stamped size below
 * MOQ_FETCH_CFG_V0_SIZE is rejected by moq_session_fetch(), not here; a stamp
 * AT the floor is accepted and simply carries no appended field.
 */
MOQ_API void moq_fetch_cfg_init_sized(moq_fetch_cfg_t *cfg, size_t cfg_size);

/*
 * Initiate a fetch (subscriber side). Advancing call.
 * Set is_joining=true for a joining fetch; joining_relative selects
 * relative (true) or absolute (false) mode. Track namespace/name are
 * ignored for joining fetches.
 *
 * Name the joined subscription with EXACTLY ONE of:
 *
 *   joining_sub -- a SUBSCRIBE-initiated subscription with the LARGEST_OBJECT
 *     filter that has a *current* stored largest location, i.e. one that has
 *     received SUBSCRIBE_OK (established). A still-pending subscription has no
 *     current largest yet and is rejected with MOQ_ERR_INVAL.
 *   joining_pub -- a PUBLISH-initiated subscription that is ESTABLISHED,
 *     eligible under the NEGOTIATED PROFILE's rule (draft-18: Forward State 1;
 *     draft-16: a Largest Object filter negotiated at acceptance -- see the
 *     field comment on moq_fetch_cfg_t::joining_pub), and holds a saved Joining
 *     Location. Requires moq_fetch_cfg_init_sized(). A publication still
 *     Pending (Publisher), or one ineligible under its profile, is rejected
 *     with MOQ_ERR_WRONG_STATE; one with no saved Joining Location gives
 *     MOQ_ERR_INVAL.
 *
 * Setting both, or neither, is MOQ_ERR_INVAL and mutates nothing.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 */
MOQ_API moq_result_t moq_session_fetch(moq_session_t *s,
                                        const moq_fetch_cfg_t *cfg,
                                        uint64_t now_us,
                                        moq_fetch_t *out_handle);

typedef struct moq_accept_fetch_cfg {
    uint32_t struct_size;
    bool     end_of_track;
    /* The end of the range served, in the FETCH range's own form: end_object
     * is exclusive and zero means the whole end_group. The session converts
     * it to the wire's form (draft-22 sends the inclusive End Location). */
    uint64_t end_group;
    uint64_t end_object;
    bool     empty;  /* true = send FETCH_HEADER + FIN with no objects */
    /* Appended: opaque Track Properties returned to the subscriber when
     * accepting the fetch (empty = none). Borrowed for the call. */
    moq_bytes_t track_properties;
} moq_accept_fetch_cfg_t;

MOQ_API void moq_accept_fetch_cfg_init(moq_accept_fetch_cfg_t *cfg);

/*
 * Accept an incoming fetch (publisher side). Advancing call.
 * Queues a positive response and opens the fetch data stream.
 * If cfg->empty is true, the data stream is opened with FIN set and the fetch
 * is finished: the handle is retired, and on draft-18 the acceptance also
 * ends the local half of the request stream with FIN (see
 * moq_session_end_fetch).
 *
 * On request-stream profiles the fetcher may update its fetch (priority,
 * authorization). The positive response is always the first thing the
 * fetcher receives, so the session answers an update that arrived before
 * this call right after it. An update the session refuses (an unresolvable
 * authorization token) ends the fetch, whenever it arrives: the refusal is
 * the last response to the fetcher, the data stream is reset,
 * MOQ_EVENT_FETCH_CANCELLED is surfaced and the handle is retired (later
 * calls return MOQ_ERR_STALE_HANDLE). Rejecting the fetch instead answers
 * it alone.
 *
 * A fetch surfaced with its range unresolved
 * (moq_fetch_request_options_t.range_unresolved) is accepted with the End
 * Location the application resolved, and that End must be one the request
 * allows: the Largest Object itself for a form without an end, which also
 * fixes the start of a RELATIVE_GROUP or NEXT_OBJECT form; at most the
 * request's own end for a bounded form; and never before the start. An End
 * that contradicts the request returns MOQ_ERR_INVAL with nothing queued and
 * the fetch still pending: a range that cannot be served is answered with
 * moq_session_reject_fetch() and INVALID_RANGE instead.
 */
MOQ_API moq_result_t moq_session_accept_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    const moq_accept_fetch_cfg_t *cfg,
    uint64_t now_us);

typedef struct moq_reject_fetch_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    moq_bytes_t         reason;
    bool                can_retry;
    uint64_t            retry_after_ms;
    moq_redirect_target_t redirect;    /* ABI-safe append; REDIRECT only */
} moq_reject_fetch_cfg_t;

MOQ_API void moq_reject_fetch_cfg_init(moq_reject_fetch_cfg_t *cfg);

/*
 * Reject an incoming fetch (publisher side). Advancing call.
 * Queues an error response. Frees the fetch entry.
 */
MOQ_API moq_result_t moq_session_reject_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    const moq_reject_fetch_cfg_t *cfg,
    uint64_t now_us);

typedef struct moq_fetch_object_cfg {
    uint32_t    struct_size;
    uint64_t    group_id;
    uint64_t    subgroup_id;   /* ignored when datagram is true (object has no subgroup) */
    uint64_t    object_id;
    uint8_t     publisher_priority;
    bool        datagram;      /* emit with forwarding preference Datagram (§11.4.4.1,
                                * 0x40): no subgroup. draft-18 only (draft-16 fetch
                                * objects have no such bit -> MOQ_ERR_INVAL). */
    uint8_t     _pad[6];
    moq_rcbuf_t *payload;
    moq_rcbuf_t *properties;  /* NULL if none */
} moq_fetch_object_cfg_t;

MOQ_API void moq_fetch_object_cfg_init(moq_fetch_object_cfg_t *cfg);

/*
 * Fill children (see fill.h). Advancing, single-owner operations; a refusal
 * commits none of the requested operation.
 *
 * Publisher side, on a child surfaced by MOQ_EVENT_FILL_REQUEST: open_fill
 * opens the fill stream with its header; write_fill_object keeps the caller's
 * rcbuf ownership (queued actions hold their own refs); write_fill_range marks
 * a gap; end_fill finishes the stream with a FIN; abort_fill resets it (an
 * unopened child is opened with its header first, then reset).
 *
 * Subscriber side: cancel_fill stops one fill's stream without touching the
 * subscription. Cancelling or ending the subscription resets every open fill.
 *
 * Capacity for the child's terminal is reserved before a fill is accepted, so
 * teardown is never best-effort; a cancel that cannot queue its RESET or STOP
 * returns MOQ_ERR_WOULD_BLOCK with the obligation retained. MOQ_ERR_STALE_HANDLE
 * after the child's terminal.
 */
MOQ_API moq_result_t moq_session_open_fill(moq_session_t *s, moq_fill_t fill,
                                           uint64_t now_us);
MOQ_API moq_result_t moq_session_write_fill_object(moq_session_t *s, moq_fill_t fill,
                                                   const moq_fetch_object_cfg_t *cfg,
                                                   uint64_t now_us);
MOQ_API moq_result_t moq_session_write_fill_range(moq_session_t *s, moq_fill_t fill,
                                                  moq_fetch_range_kind_t kind,
                                                  uint64_t group_id, uint64_t object_id,
                                                  uint64_t now_us);
MOQ_API moq_result_t moq_session_end_fill(moq_session_t *s, moq_fill_t fill,
                                          uint64_t now_us);
MOQ_API moq_result_t moq_session_abort_fill(moq_session_t *s, moq_fill_t fill,
                                            uint64_t error_code, uint64_t now_us);
MOQ_API moq_result_t moq_session_cancel_fill(moq_session_t *s, moq_fill_t fill,
                                             uint64_t now_us);

/*
 * Write a fetch object on an accepted fetch data stream.
 * Publisher side. Advancing call.
 * Serialization flags are computed internally from prior-object context.
 */
MOQ_API moq_result_t moq_session_write_fetch_object(
    moq_session_t *s,
    moq_fetch_t fetch,
    const moq_fetch_object_cfg_t *cfg,
    uint64_t now_us);

/*
 * Write an end-of-range gap marker on an accepted fetch data stream.
 * Publisher side. Advancing call. MOQ_FETCH_RANGE_TIMED_OUT is written only
 * on a profile with an End of Timed-Out Range (draft-22); elsewhere it
 * returns MOQ_ERR_INVAL with nothing queued and the prior Location
 * unchanged, as for an unknown kind.
 */
MOQ_API moq_result_t moq_session_write_fetch_range(
    moq_session_t *s,
    moq_fetch_t fetch,
    moq_fetch_range_kind_t kind,
    uint64_t group_id,
    uint64_t object_id,
    uint64_t now_us);

/*
 * Write an end-of-range gap marker whose inclusive Location is IMMEDIATELY
 * BEFORE `first_group` -- it covers every Location up to and including the last
 * object of `first_group - 1`. Publisher side. Advancing call. Use this to mark
 * a leading gap before the first object a publisher can serve (e.g. a cache's
 * evicted prefix): the marker lands at group `first_group - 1` with the highest
 * object id the active draft can represent, which the session supplies so the
 * caller never needs a draft-specific limit. Returns MOQ_ERR_INVAL if
 * first_group == 0 (nothing precedes group 0) or kind is invalid (including
 * MOQ_FETCH_RANGE_TIMED_OUT on a profile without an End of Timed-Out Range),
 * in every case without queueing anything or advancing the prior Location.
 * Marker semantics
 * otherwise match moq_session_write_fetch_range (advances the prior Location
 * only).
 */
MOQ_API moq_result_t moq_session_write_fetch_range_before_group(
    moq_session_t *s,
    moq_fetch_t fetch,
    moq_fetch_range_kind_t kind,
    uint64_t first_group,
    uint64_t now_us);

/*
 * True when the negotiated draft's FETCH object header can carry the
 * datagram-preference bit (draft-18 yes, draft-16 no). Pure query: a publisher
 * asks the capability instead of testing a draft version, so version details
 * never leak into caller logic. False for a NULL session.
 */
MOQ_API bool moq_session_supports_fetch_datagram(const moq_session_t *s);

/*
 * True when the negotiated draft has the joining forms of FETCH that
 * reference a subscription (drafts 16 and 18 yes, draft 22 no: it joins a
 * track at the current group with a fill instead, see
 * moq_session_supports_fill). Pure query, same contract as
 * moq_session_supports_fetch_datagram. False for a NULL session.
 */
MOQ_API bool moq_session_supports_joining_fetch(const moq_session_t *s);

/*
 * True when the negotiated draft lets a subscription carry a fill request,
 * on the subscribe or on a later update (draft 22 yes, drafts 16 and 18
 * no). A caller deciding how to join an ongoing track asks this
 * instead of testing a draft version. False for a NULL session.
 */
MOQ_API bool moq_session_supports_fill(const moq_session_t *s);

/*
 * True when the negotiated draft allows several concurrent subscriptions to
 * one track, in either direction, which may share an alias (draft 22 yes,
 * drafts 16 and 18 no: one subscription per track and role). A caller that
 * would subscribe again, or accept another subscription to a track it
 * already serves, asks this instead of testing a draft version. False for a
 * NULL session.
 */
MOQ_API bool moq_session_supports_concurrent_subscriptions(const moq_session_t *s);

/*
 * Close the fetch data stream with FIN. Publisher side. Advancing call.
 * Frees the fetch entry after queuing the final action.
 *
 * On draft-18, where the FETCH travels on its own request stream, the same
 * commit also FINs the local half of that stream: the response is complete,
 * so the stream has nothing left to carry. The fetcher's later FIN or RESET of
 * its own half is absorbed as the ordinary end of the stream. A LibMoQ fetcher
 * FINs its half once it holds both the acceptance and the end of the data
 * stream. Every required action (and, while the fetcher's half is still open,
 * the drain slot that absorbs its terminal) is reserved first:
 * MOQ_ERR_WOULD_BLOCK leaves the fetch and the queued output unchanged, so the
 * call is retried as is.
 */
MOQ_API moq_result_t moq_session_end_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    uint64_t now_us);

/*
 * Abort an ACCEPTED fetch whose data stream has been opened, without
 * completing it: queues a request-local RESET of that fetch's data stream
 * carrying `error_code` and retires the publisher-side fetch handle.
 * Publisher side. Advancing call. Nothing else is touched: the session stays
 * open, no FIN is sent, no subscription is retired, and no peer-facing
 * cancellation event is fabricated. The peer observes a reset data stream
 * instead of a completed one.
 *
 * `error_code` is a QUIC STREAM reset application error code (draft-16
 * section 10.4.3 / draft-18 section 3.3.3, e.g. INTERNAL_ERROR 0x0), not a
 * MoQT request or session error; it must lie within the transport's
 * representable application error range (MOQ_ERR_INVAL otherwise).
 *
 * A fetch that is still pending (not yet accepted) is refused
 * (MOQ_ERR_WRONG_STATE): reject it with moq_session_reject_fetch instead. A
 * fetcher-role handle is likewise MOQ_ERR_WRONG_STATE. Every required action
 * (and, on stream-correlated profiles, the request-stream drain slot that
 * absorbs the peer's late FIN) is reserved before any state changes:
 * MOQ_ERR_WOULD_BLOCK leaves the fetch, previously queued actions and the
 * handle exactly as they were, so the call is retried unchanged. After
 * MOQ_OK the handle is retired: any later use is MOQ_ERR_STALE_HANDLE.
 */
MOQ_API moq_result_t moq_session_abort_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    uint64_t error_code,
    uint64_t now_us);

/*
 * Cancel a pending fetch (subscriber side). Advancing call.
 * Queues cancellation bytes. Frees the fetch entry. MOQ_ERR_WOULD_BLOCK, with
 * the fetch unchanged, when the action queue is full or, where the cancel is
 * a control message (draft-16, draft-18), the send buffer still holds earlier
 * output that polling the actions releases; MOQ_ERR_BUFFER only when the
 * message could never fit the send buffer.
 */
MOQ_API moq_result_t moq_session_fetch_cancel(moq_session_t *s,
                                               moq_fetch_t fetch,
                                               uint64_t now_us);

/*
 * Update a fetch this side requested (draft-22 §9.5: the sender of a request
 * may later update it; a fetch update carries authorization tokens and the
 * subscriber priority). Fetcher side. Advancing call. Consumes request
 * capacity like any request.
 *
 * has_subscriber_priority / subscriber_priority: the new priority of the
 * fetch's data stream at the publisher. auth_tokens: USE_VALUE tokens, each
 * borrowed for the call. At least one of the two is required (MOQ_ERR_INVAL
 * otherwise). struct_size must cover the whole struct.
 *
 * One update may be outstanding per fetch: a second before the first is
 * answered is MOQ_ERR_WRONG_STATE. The publisher answers once it has accepted
 * the fetch; its acknowledgement completes the update without an event, and
 * no query reports it either: the only sign is that a further update is no
 * longer MOQ_ERR_WRONG_STATE. A refusal ends the
 * fetch (§9.5.1: the publisher resets the FETCH data stream): the session
 * stops the data stream and the request stream and surfaces FETCH_ERROR with
 * the refusal's code; objects already polled stay valid and the handle is
 * retired. MOQ_ERR_UNSUPPORTED on a profile without fetcher updates
 * (draft-16 and draft-18 send none); MOQ_ERR_WRONG_STATE for a publisher-role
 * handle or a fetch that has ended or whose request stream this side has
 * closed; MOQ_ERR_REQUEST_BLOCKED without request capacity;
 * MOQ_ERR_WOULD_BLOCK when the action queue is full (nothing changed).
 */
typedef struct moq_fetch_update_cfg {
    uint32_t                struct_size;
    bool                    has_subscriber_priority;
    uint8_t                 subscriber_priority;
    uint8_t                 _reserved[2];
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_fetch_update_cfg_t;

MOQ_API void moq_fetch_update_cfg_init(moq_fetch_update_cfg_t *cfg);

MOQ_API moq_result_t moq_session_update_fetch(moq_session_t *s,
                                              moq_fetch_t fetch,
                                              const moq_fetch_update_cfg_t *cfg,
                                              uint64_t now_us);

/* -- Per-request GOAWAY send (draft-18 §10.4 migration) ------------ *
 * Emit a GOAWAY on an active request bidi to migrate that single request; the
 * peer re-issues it on `new_session_uri` (empty => the current session). Keeps
 * the request bidi and its data streams alive (graceful migration); at most one
 * per request stream (a second ⇒ MOQ_ERR_WRONG_STATE). A client MUST leave the
 * URI empty (else MOQ_ERR_INVAL). Draft-16 has no per-request GOAWAY ⇒
 * MOQ_ERR_INVAL. Eligible states (else MOQ_ERR_WRONG_STATE): SUBSCRIBE/
 * PUBLISH/ANNOUNCEMENT/NS_SUB/SUBSCRIBE_TRACKS established, FETCH accepted,
 * TRACK_STATUS only on the responder (the requester opened with FIN). A
 * subscription or publication owed an update answer is not eligible until it
 * is answered. Once GOAWAY is sent, session-wide application deferral no longer
 * delays in-flight updates. A facade-owned D22 parent retains its pre-ACK
 * admission ownership, including reservation of any requested fill.
 * A subscription this session publishes stays fully usable after its GOAWAY;
 * the subscriber's later RESET_STREAM or STOP_SENDING of the request stream
 * cancels it like any other cancellation (see moq_session_on_bidi_stream_reset)
 * except that no MOQ_EVENT_UNSUBSCRIBED is surfaced for it. */
typedef struct moq_request_goaway_cfg {
    uint32_t    struct_size;
    moq_bytes_t new_session_uri;   /* borrowed; empty (and required so for clients) */
    uint64_t    timeout_ms;        /* graceful-migration hint; 0 = unspecified */
} moq_request_goaway_cfg_t;

MOQ_API void moq_request_goaway_cfg_init(moq_request_goaway_cfg_t *cfg);

MOQ_API moq_result_t moq_session_request_goaway_subscribe(
    moq_session_t *s, moq_subscription_t sub,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us);
MOQ_API moq_result_t moq_session_request_goaway_fetch(
    moq_session_t *s, moq_fetch_t fetch,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us);
MOQ_API moq_result_t moq_session_request_goaway_track_status(
    moq_session_t *s, moq_track_status_handle_t handle,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us);
MOQ_API moq_result_t moq_session_request_goaway_namespace(
    moq_session_t *s, moq_announcement_t ann,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us);
MOQ_API moq_result_t moq_session_request_goaway_ns_sub(
    moq_session_t *s, moq_ns_sub_handle_t handle,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us);
MOQ_API moq_result_t moq_session_request_goaway_publish(
    moq_session_t *s, moq_publication_t pub,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us);
MOQ_API moq_result_t moq_session_request_goaway_subscribe_tracks(
    moq_session_t *s, moq_track_sub_handle_t handle,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us);

/* -- PUBLISH (publisher-initiated subscription) -------------------- */

typedef struct moq_publish_cfg {
    uint32_t               struct_size;
    moq_namespace_t        track_namespace;
    moq_bytes_t            track_name;
    bool                   has_track_alias;
    uint64_t               track_alias;
    bool                   has_forward;
    bool                   forward;
    moq_bytes_t            track_properties;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    /* Draft-22-only tail, beyond the complete old layout including padding.
     * Presence/value pairs are read only when BOTH fields are covered; values
     * with an inactive presence flag are ignored. Expiry zero is not omission. */
    uint64_t expires_ms;
    bool has_expires;
    bool has_subscriber_priority;
    uint8_t subscriber_priority;
    bool has_group_order;
    moq_group_order_t group_order;
    /* Borrowed for this call only. NULL omits LOCATION_FILTER; explicit NONE
     * is distinct. PUBLISH does not support an initial fill. */
    const moq_location_filter_cfg_t *location;
    bool has_object_delivery_timeout;
    uint64_t object_delivery_timeout_ms;
    bool has_subgroup_delivery_timeout;
    uint64_t subgroup_delivery_timeout_ms;
} moq_publish_cfg_t;

/*
 * Frozen v0 prefix: everything through track_properties. Pinned so a later
 * append can never shift the floor an old caller is measured against.
 */
#define MOQ_PUBLISH_CFG_V0_SIZE offsetof(moq_publish_cfg_t, auth_tokens)

/*
 * moq_publish_cfg_init() is POINTER-ONLY: it cannot know the caller's struct
 * size, so it clears and stamps ONLY the frozen v0 prefix and never writes past
 * it (safe on an exact-v0 allocation). Fields beyond the prefix are left
 * untouched and ignored by moq_session_publish() (struct_size == V0).
 *
 * A caller that sets ANY field past the prefix (auth tokens or draft-22 options)
 * MUST use moq_publish_cfg_init_sized(), which records the real struct_size so
 * those fields are read.
 */
MOQ_API void moq_publish_cfg_init(moq_publish_cfg_t *cfg);

/*
 * Clears and stamps min(cfg_size, this library's sizeof) -- never more than the
 * caller allocated, never more than this library knows about.
 */
MOQ_API void moq_publish_cfg_init_sized(moq_publish_cfg_t *cfg, size_t cfg_size);

/*
 * Initiate a publisher-initiated subscription. Advancing call.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 * Draft-22 defaults explicitly emit priority 128 and publisher-preferred group
 * order; caller selections override these library policies. NULL location
 * emits no fabricated filter. Selected draft-22-only fields on draft-16/18
 * return UNSUPPORTED before effects. Timeout values are exact milliseconds,
 * independent of Track Property timeouts; expiry is advisory.
 */
MOQ_API moq_result_t moq_session_publish(moq_session_t *s,
                                          const moq_publish_cfg_t *cfg,
                                          uint64_t now_us,
                                          moq_publication_t *out_handle);

/*
 * moq_session_subscription_group_order() for a publisher-initiated
 * subscription. Until the subscriber's accept is known, the publisher's
 * preference.
 */
MOQ_API moq_group_order_t moq_session_publication_group_order(
    const moq_session_t *s, moq_publication_t pub);

typedef struct moq_accept_publish_cfg {
    uint32_t          struct_size;
    bool              has_subscriber_priority;
    uint8_t           subscriber_priority;
    moq_group_order_t group_order;
    /* Appended (ABI-safe): request the original publisher start a new
     * group (largest known Group ID + 1; 0 = "no group info" and is
     * meaningful, hence the has_ flag). Valid only when the publish
     * carried dynamic-group support (see
     * moq_publish_request_event_t.dynamic_groups); otherwise
     * MOQ_ERR_INVAL. Callers compiled against the smaller struct send
     * nothing. */
    bool              has_new_group_request;
    uint64_t          new_group_request;
    /* Appended (ABI-safe): the FILTER BLOCK -- the subscription filter this
     * subscriber chooses for the publisher-initiated subscription, carried as
     * SUBSCRIPTION_FILTER on the response. Read as a COMPLETE BLOCK: consulted
     * only when struct_size covers through end_group; a prefix stopping partway
     * leaves the subscription unfiltered (a half-present start/object is never
     * read). filter == MOQ_SUBSCRIBE_FILTER_NONE also means unfiltered. */
    moq_subscribe_filter_t filter;
    uint64_t               start_group;
    uint64_t               start_object;
    uint64_t               end_group;
    /* Appended (ABI-safe): the FORWARD BLOCK -- the subscriber's initial
     * forwarding state. Read as a COMPLETE BLOCK: consulted only when
     * struct_size covers through forward. When absent (or has_forward is
     * false) the draft-16/18 EFFECTIVE state resolves to forwarding: wire
     * omission defaults to 1, not the publisher's initial value. Draft-22
     * permits no FORWARD override here and preserves the PUBLISH value. */
    bool              has_forward;
    bool              forward;
    /* Draft-22-only expiry pair. The aligned value preserves all old trailing
     * padding; both fields must be covered before either is consulted. */
    uint64_t          expires_ms;
    bool              has_expires;
    /* Appended: the subscriber's delivery timeouts in its acceptance
     * (draft-16 9.2.2.2, draft-18 10.2.3 and 10.2.4), MOQ_ACCEPT_PUBLISH_-
     * {OBJECT,SUBGROUP}_DELIVERY_TIMEOUT flags. Read as one block through
     * subgroup_delivery_timeout_ms; a torn block is ignored. Unknown flags
     * or nonzero reserved are INVAL. Draft-16 has one DELIVERY_TIMEOUT: both
     * selected values must be equal and neither 0. Draft-22's answer carries
     * none (MOQ_ERR_INVAL): send them with moq_session_update_publication().
     * Aligned past the layout through has_expires. */
#ifdef __cplusplus
    alignas(uint64_t)
#else
    _Alignas(uint64_t)
#endif
    uint32_t          delivery_timeout_flags;
    uint32_t          reserved;
    uint64_t          object_delivery_timeout_ms;
    uint64_t          subgroup_delivery_timeout_ms;
} moq_accept_publish_cfg_t;

#define MOQ_ACCEPT_PUBLISH_OBJECT_DELIVERY_TIMEOUT   0x01u
#define MOQ_ACCEPT_PUBLISH_SUBGROUP_DELIVERY_TIMEOUT 0x02u

/*
 * Frozen v0 prefix: everything through group_order. Pinned so a later append
 * can never shift the floor an old caller is measured against.
 */
#define MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE \
    offsetof(moq_accept_publish_cfg_t, has_new_group_request)

/*
 * POINTER-ONLY: clears and stamps ONLY the frozen v0 prefix, never writing past
 * it (safe on an exact-v0 allocation). A caller that sets ANY field past the
 * prefix (new-group request, filter, forward, or expiry) MUST use
 * moq_accept_publish_cfg_init_sized().
 */
MOQ_API void moq_accept_publish_cfg_init(moq_accept_publish_cfg_t *cfg);

MOQ_API void moq_accept_publish_cfg_init_sized(moq_accept_publish_cfg_t *cfg,
                                                size_t cfg_size);

/*
 * Accept an incoming publish (subscriber side). Advancing call.
 * Queues a positive response.
 * On draft-22 the response carries only optional expiry and empty properties.
 * Explicit legacy priority/order/FORWARD/filter/new-group overrides return
 * INVAL before effects; a default accept preserves the initial PUBLISH state.
 * A selected expiry on draft-16/18 returns UNSUPPORTED before effects.
 */
MOQ_API moq_result_t moq_session_accept_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_accept_publish_cfg_t *cfg,
    uint64_t now_us);

#define MOQ_PUBLISH_INITIAL_HAS_SUBSCRIBER_PRIORITY UINT32_C(1)
#define MOQ_PUBLISH_INITIAL_HAS_GROUP_ORDER UINT32_C(2)
#define MOQ_PUBLISH_INITIAL_HAS_LOCATION UINT32_C(4)
#define MOQ_PUBLISH_INITIAL_HAS_OBJECT_TIMEOUT UINT32_C(8)
#define MOQ_PUBLISH_INITIAL_HAS_SUBGROUP_TIMEOUT UINT32_C(16)

/* Immutable initial peer PUBLISH parameters, never later UPDATE state. Flags
 * describe wire presence; priority/order contain effective initial values.
 * Location is by-value and independently stamped when present. Inactive
 * location/timeout fields and reserved bytes are zero. No owned resources. */
typedef struct moq_publish_initial_parameters {
    uint32_t struct_size;
    uint32_t flags;
    moq_location_filter_cfg_t location;
    uint64_t object_delivery_timeout_ms;
    uint64_t subgroup_delivery_timeout_ms;
    uint32_t group_order;
    uint8_t subscriber_priority;
    uint8_t reserved[3];
} moq_publish_initial_parameters_t;

#define MOQ_PUBLISH_INITIAL_PARAMETERS_V0_SIZE 80u

/* Observing, non-advancing; no allocation, wake, or borrowed backing. Available
 * on draft-22 for a valid peer-publisher publication in pending-subscriber or
 * established state. Bad arguments/size, stale handle, wrong role/state, or an
 * unsupported profile leave out untouched. Requires the frozen v0 floor,
 * copies min(out_size, library size), and stamps bytes written. Core threading
 * rules are unchanged; lifecycle outcomes remain event-driven. */
MOQ_API moq_result_t moq_session_get_publish_initial_parameters(
    const moq_session_t *session,
    moq_publication_t publication,
    moq_publish_initial_parameters_t *out,
    size_t out_size);

typedef struct moq_reject_publish_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    moq_bytes_t         reason;
    bool                can_retry;
    uint64_t            retry_after_ms;
} moq_reject_publish_cfg_t;

MOQ_API void moq_reject_publish_cfg_init(moq_reject_publish_cfg_t *cfg);

/*
 * The filter in effect on a publication as this side knows it: the initial
 * LOCATION_FILTER the PUBLISH carried, replaced by any accepted update. Not an
 * advancing call. MOQ_ERR_STALE_HANDLE for a retired handle; the kind is
 * MOQ_LOCATION_FILTER_NONE when the publication is unfiltered. `out` is
 * written as moq_session_subscription_filter() writes it (the _sized call,
 * the v0-only symbol, and the same-name macro).
 */
/* moq_session_subscription_window() for a subscription a PUBLISH began, on
 * either side. */
MOQ_API moq_result_t moq_session_publication_window(moq_session_t *s,
    moq_publication_t pub, moq_filter_window_t *out, size_t out_size);

MOQ_API moq_result_t moq_session_publication_filter(moq_session_t *s,
    moq_publication_t pub, moq_location_filter_cfg_t *out);
MOQ_API moq_result_t moq_session_publication_filter_sized(moq_session_t *s,
    moq_publication_t pub, moq_location_filter_cfg_t *out, size_t out_size);
#define moq_session_publication_filter(s, pub, out) \
    moq_session_publication_filter_sized((s), (pub), (out), \
                                         sizeof(moq_location_filter_cfg_t))

/*
 * Reject an incoming publish (subscriber side). Advancing call.
 * Queues an error response. Frees the publish entry.
 * A data stream the publication already opened is stopped with it, as for
 * moq_session_unsubscribe.
 */
MOQ_API moq_result_t moq_session_reject_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_reject_publish_cfg_t *cfg,
    uint64_t now_us);

/*
 * Cancel a publication this side accepted (subscriber side). Advancing call.
 * The request stream is reset and its receiving half stopped with
 * `error_code` (draft-22 §6.4.2.3: "Implementations cancel a request by
 * abruptly terminating any directions of the stream that are still open"),
 * every data stream the publication still has open is stopped, and the entry
 * is freed; no event follows, as for moq_session_unsubscribe. A relay that
 * finds a pushed Track malformed cancels it with MALFORMED_TRACK (0x12,
 * §12.1). On draft-16, which has no request streams, the cancel is the
 * control message that ends the subscription by the PUBLISH's Request ID;
 * it carries no code, so `error_code` is not sent. MOQ_ERR_WRONG_STATE
 * unless the publication is established and not already ended by its
 * publisher; MOQ_ERR_WOULD_BLOCK, with nothing changed, when the action
 * queue (or, on draft-16, the late-response tombstones or a send buffer
 * still holding earlier output) cannot take the cancel; MOQ_ERR_BUFFER only
 * when the draft-16 message could never fit the send buffer.
 */
MOQ_API moq_result_t moq_session_cancel_publication(
    moq_session_t *s,
    moq_publication_t pub,
    uint64_t error_code,
    uint64_t now_us);

/* -- Datagram send ------------------------------------------------- */

/*
 * Send an object via datagram (publisher side). Advancing call.
 *
 * Encodes an object datagram and queues a datagram send action.
 * sub must be an established publisher-role subscription.
 *
 * properties/properties_len: optional per-object property bytes.
 * payload must be non-NULL with len > 0; use send_status_datagram
 * for zero-length / status objects.
 */
MOQ_API moq_result_t moq_session_send_object_datagram(
    moq_session_t *s,
    moq_subscription_t sub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    bool end_of_group,
    moq_rcbuf_t *payload,
    const uint8_t *properties, size_t properties_len,
    uint64_t now_us);

/*
 * Send a status-only datagram (publisher side). Advancing call.
 *
 * Encodes a status-only object datagram with no payload.
 */
MOQ_API moq_result_t moq_session_send_status_datagram(
    moq_session_t *s,
    moq_subscription_t sub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    moq_object_status_t status,
    uint64_t now_us);

/*
 * Send an object datagram on a publisher-initiated publication.
 * Same semantics as send_object_datagram but targets a publication.
 */
MOQ_API moq_result_t moq_session_send_pub_object_datagram(
    moq_session_t *s,
    moq_publication_t pub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    bool end_of_group,
    moq_rcbuf_t *payload,
    const uint8_t *properties, size_t properties_len,
    uint64_t now_us);

MOQ_API moq_result_t moq_session_send_pub_status_datagram(
    moq_session_t *s,
    moq_publication_t pub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    moq_object_status_t status,
    uint64_t now_us);

/* -- GOAWAY -------------------------------------------------------- */

/*
 * Send GOAWAY to the peer. Advancing call.
 *
 * uri/uri_len: new session URI (server only; max 8192 bytes).
 * Clients must pass uri_len == 0.
 *
 * Returns MOQ_ERR_WRONG_STATE if GOAWAY was already sent or
 * the session is not ESTABLISHED/DRAINING.
 * Returns MOQ_ERR_INVAL if a client passes a non-empty URI.
 * Transitions session to DRAINING on success.
 */
MOQ_API moq_result_t moq_session_goaway(moq_session_t *s,
                                         const uint8_t *uri,
                                         size_t uri_len,
                                         uint64_t now_us);

/* -- Namespace advertisement --------------------------------------- */

typedef struct moq_publish_namespace_cfg {
    uint32_t        struct_size;
    moq_namespace_t track_namespace;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_publish_namespace_cfg_t;

MOQ_API void moq_publish_namespace_cfg_init(moq_publish_namespace_cfg_t *cfg);

MOQ_API moq_result_t moq_session_publish_namespace(moq_session_t *s,
    const moq_publish_namespace_cfg_t *cfg, uint64_t now_us,
    moq_announcement_t *out_handle);

MOQ_API moq_result_t moq_session_publish_namespace_done(moq_session_t *s,
    moq_announcement_t ann, uint64_t now_us);

typedef struct moq_accept_namespace_cfg {
    uint32_t struct_size;
} moq_accept_namespace_cfg_t;

MOQ_API void moq_accept_namespace_cfg_init(moq_accept_namespace_cfg_t *cfg);

MOQ_API moq_result_t moq_session_accept_namespace(moq_session_t *s,
    moq_announcement_t ann, const moq_accept_namespace_cfg_t *cfg,
    uint64_t now_us);

typedef struct moq_reject_namespace_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    moq_bytes_t         reason;
    bool                can_retry;
    uint64_t            retry_after_ms;
    moq_redirect_target_t redirect;    /* ABI-safe append; REDIRECT only (track_name empty) */
} moq_reject_namespace_cfg_t;

MOQ_API void moq_reject_namespace_cfg_init(moq_reject_namespace_cfg_t *cfg);

/*
 * Reject a pending incoming namespace advertisement. Advancing call.
 * Sends an error response. Only valid when the announcement is PENDING.
 */
MOQ_API moq_result_t moq_session_reject_namespace(moq_session_t *s,
    moq_announcement_t ann, const moq_reject_namespace_cfg_t *cfg,
    uint64_t now_us);

typedef struct moq_cancel_namespace_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    moq_bytes_t         reason;
} moq_cancel_namespace_cfg_t;

MOQ_API void moq_cancel_namespace_cfg_init(moq_cancel_namespace_cfg_t *cfg);

/*
 * Cancel an accepted namespace advertisement. Advancing call.
 * Cancels an accepted namespace advertisement. Only valid when established.
 */
MOQ_API moq_result_t moq_session_cancel_namespace(moq_session_t *s,
    moq_announcement_t ann, const moq_cancel_namespace_cfg_t *cfg,
    uint64_t now_us);

/* -- Namespace subscription (bidi stream) -------------------------- */

/* -- Track status ------------------------------------------------- */

typedef struct moq_track_status_cfg {
    uint32_t        struct_size;
    moq_namespace_t track_namespace;
    moq_bytes_t     track_name;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    /* Appended: INCLUDE_PROPERTIES (draft-22 §9.20.21), read together only
     * when struct_size covers include_properties. Selected, the TRACK_STATUS
     * carries the parameter: false asks for a TRACK_STATUS_OK with empty
     * Track Properties. Selected on a profile without the parameter
     * (draft-16/18) is MOQ_ERR_UNSUPPORTED before anything is sent. */
    bool                    has_include_properties;
    bool                    include_properties;
} moq_track_status_cfg_t;

/* The symbol clears and stamps the layout through auth_token_count, the
 * struct as a caller built before has_include_properties was appended holds
 * it, so it never writes past such a caller's storage. The function-like
 * macro passes the caller's sizeof to moq_track_status_cfg_init_sized(),
 * which clears and stamps min(cfg_size, sizeof current). A binding that calls
 * the symbol instead of compiling the macro calls the _sized form with the
 * size of its struct. */
MOQ_API void moq_track_status_cfg_init(moq_track_status_cfg_t *cfg);
MOQ_API void moq_track_status_cfg_init_sized(moq_track_status_cfg_t *cfg,
                                             size_t cfg_size);
#define moq_track_status_cfg_init(cfg) \
    moq_track_status_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Query track status (subscriber side). Advancing call.
 * Queues a track status request. No subscription state is created.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 */
MOQ_API moq_result_t moq_session_track_status(
    moq_session_t *s,
    const moq_track_status_cfg_t *cfg,
    uint64_t now_us,
    moq_track_status_handle_t *out_handle);

typedef struct moq_accept_track_status_cfg {
    uint32_t struct_size;
    /* Appended (ABI-additive, size-gated): the track's current status reported in
     * TRACK_STATUS_OK. Omitted fields are sent absent. */
    bool     has_largest;
    uint64_t largest_group;
    uint64_t largest_object;
    bool     has_expires;
    uint64_t expires_ms;
    /* Appended: the Track Properties of the TRACK_STATUS_OK (borrowed for
     * the call), the ones a SUBSCRIBE_OK for the track would carry (§9.13;
     * a relay includes all of the track's properties, §7.7). Read only when
     * struct_size covers the field; empty sends none. They are validated as
     * moq_session_accept_subscribe() validates its own (MOQ_ERR_INVAL before
     * anything changes). When the request carried INCLUDE_PROPERTIES = 0 the
     * answer carries none (§9.20.21). Draft-16's answer has no Track
     * Properties, so there they are validated and not sent. */
    moq_bytes_t track_properties;
} moq_accept_track_status_cfg_t;

/* The symbol clears and stamps the layout through expires_ms, the struct as
 * a caller built before track_properties was appended holds it. The
 * function-like macro passes the caller's sizeof to
 * moq_accept_track_status_cfg_init_sized(), which clears and stamps
 * min(cfg_size, sizeof current). A binding that calls the symbol instead of
 * compiling the macro calls the _sized form with the size of its struct. */
MOQ_API void moq_accept_track_status_cfg_init(moq_accept_track_status_cfg_t *cfg);
MOQ_API void moq_accept_track_status_cfg_init_sized(
    moq_accept_track_status_cfg_t *cfg, size_t cfg_size);
#define moq_accept_track_status_cfg_init(cfg) \
    moq_accept_track_status_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Accept a track status request (publisher side). Advancing call.
 * Queues a positive response. No subscription state is created. The Largest
 * Object it reports is the larger of the cfg's and the one this session has
 * observed for the track, as moq_session_accept_subscribe() reports it.
 */
MOQ_API moq_result_t moq_session_accept_track_status(
    moq_session_t *s,
    moq_track_status_handle_t handle,
    const moq_accept_track_status_cfg_t *cfg,
    uint64_t now_us);

typedef struct moq_reject_track_status_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    /* ABI-safe appends — carry full REDIRECT metadata (§10.6.1). */
    moq_bytes_t         reason;
    bool                can_retry;
    uint64_t            retry_after_ms;
    moq_redirect_target_t redirect;    /* REDIRECT only */
} moq_reject_track_status_cfg_t;

MOQ_API void moq_reject_track_status_cfg_init(moq_reject_track_status_cfg_t *cfg);

/*
 * Reject a track status request (publisher side). Advancing call.
 */
MOQ_API moq_result_t moq_session_reject_track_status(
    moq_session_t *s,
    moq_track_status_handle_t handle,
    const moq_reject_track_status_cfg_t *cfg,
    uint64_t now_us);

/* Namespace interest modes. */
#define MOQ_NAMESPACE_INTEREST_PUBLISHER_STATE  0u
#define MOQ_NAMESPACE_INTEREST_NAMESPACE_STATE  1u
#define MOQ_NAMESPACE_INTEREST_BOTH             2u

typedef struct moq_subscribe_namespace_cfg {
    uint32_t                 struct_size;
    moq_namespace_t          track_namespace_prefix;
    moq_namespace_interest_t namespace_interest;
    /* Appended: authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_subscribe_namespace_cfg_t;

MOQ_API void moq_subscribe_namespace_cfg_init(moq_subscribe_namespace_cfg_t *cfg);

/*
 * Subscribe to namespace discovery via bidi stream. Advancing call.
 *
 * Queues a bidi stream open with the namespace subscription request.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full;
 * no state is committed.
 */
MOQ_API moq_result_t moq_session_subscribe_namespace(
    moq_session_t *s,
    const moq_subscribe_namespace_cfg_t *cfg,
    uint64_t now_us,
    moq_ns_sub_handle_t *out_handle);

/*
 * Feed bytes from a bidi stream request half.
 *
 * Parses bidi stream bytes and emits NS_SUB_REQUEST.
 * Returns MOQ_ERR_WOULD_BLOCK if the event queue is full after
 * a complete parse. Retry with buf=NULL, len=0 after draining
 * events. process_pending() is control-stream only.
 *
 * Advancing call: invalidates borrows, takes now_us.
 */
MOQ_API moq_result_t moq_session_on_bidi_stream_bytes(
    moq_session_t *s,
    moq_stream_ref_t stream_ref,
    const uint8_t *buf, size_t len,
    bool fin, uint64_t now_us);

typedef struct moq_accept_ns_sub_cfg {
    uint32_t struct_size;
} moq_accept_ns_sub_cfg_t;

MOQ_API void moq_accept_ns_sub_cfg_init(moq_accept_ns_sub_cfg_t *cfg);

/*
 * Accept a namespace subscription request. Advancing call.
 * Queues a positive response on the bidi stream (fin=false).
 * Removes request-id registry entry (pending-phase only).
 */
MOQ_API moq_result_t moq_session_accept_ns_sub(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    const moq_accept_ns_sub_cfg_t *cfg,
    uint64_t now_us);

typedef struct moq_reject_ns_sub_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    moq_bytes_t         reason;
    /* ABI-safe appends — carry full REDIRECT metadata (§10.6.1, track_name empty). */
    bool                can_retry;
    uint64_t            retry_after_ms;
    moq_redirect_target_t redirect;    /* REDIRECT only */
} moq_reject_ns_sub_cfg_t;

MOQ_API void moq_reject_ns_sub_cfg_init(moq_reject_ns_sub_cfg_t *cfg);

/*
 * Reject a namespace subscription request. Advancing call.
 * Queues an error response on the bidi stream (fin=true).
 * Removes both indexes and frees the pool entry.
 */
MOQ_API moq_result_t moq_session_reject_ns_sub(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    const moq_reject_ns_sub_cfg_t *cfg,
    uint64_t now_us);

/*
 * Send a NAMESPACE message on an established namespace subscription
 * (publisher side). Advancing call.
 * Queues SEND_BIDI_STREAM with encoded NAMESPACE (fin=false).
 * Valid only after accept_ns_sub (state ESTABLISHED, publisher role).
 */
MOQ_API moq_result_t moq_session_send_namespace(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    const moq_namespace_t *suffix,
    uint64_t now_us);

/*
 * Withdraw a namespace advertisement on an established namespace
 * subscription (publisher side). Advancing call.
 * Same constraints as send_namespace.
 */
MOQ_API moq_result_t moq_session_send_namespace_done(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    const moq_namespace_t *suffix,
    uint64_t now_us);

/*
 * Cancel a namespace subscription (subscriber side). Advancing call.
 * Queues CLOSE_BIDI_STREAM. Transitions to CLOSING.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full;
 * no state is committed.
 */
MOQ_API moq_result_t moq_session_cancel_namespace_sub(
    moq_session_t *s,
    moq_ns_sub_handle_t handle,
    uint64_t now_us);

/* -- SUBSCRIBE_TRACKS (draft-18 §10.19 / §10.20, draft-22 §9.18) ---- *
 * Track discovery: request PUBLISH messages for all tracks under a namespace
 * prefix (and future ones). Draft-18 and draft-22 only — these calls return
 * MOQ_ERR_INVAL on a draft-16 session. The overlap space is independent of
 * subscribe_namespace. The publisher answers with the ordinary PUBLISH
 * request (moq_session_publish) on new bidi streams; the SUBSCRIBE_TRACKS
 * stream itself carries the response, PUBLISH_BLOCKED (draft-22
 * PUBLISH_SKIPPED) and, on draft-22, the namespace updates below. */

typedef struct moq_subscribe_tracks_cfg {
    uint32_t                struct_size;
    moq_namespace_t         track_namespace_prefix;  /* 0..32 fields */
    /* FORWARD (§10.2.12): when has_forward and !forward, the resulting PUBLISH
     * messages carry FORWARD=0; otherwise they default to forwarding. */
    bool                    has_forward;
    bool                    forward;
    /* Authorization tokens (borrowed for the call). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    /* Draft-22 only: GROUP_ORDER and INCLUDE_PROPERTIES (§9.18), selected by
     * MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER / _INCLUDE_PROPERTIES in options_flags.
     * Read as one block, only when struct_size covers include_properties; a
     * torn block is absent. A selected group order is ASCENDING or
     * DESCENDING (DEFAULT has no wire value and is MOQ_ERR_INVAL). An unknown
     * flag is MOQ_ERR_INVAL; any flag on an older draft is
     * MOQ_ERR_UNSUPPORTED, before anything is sent. */
    uint32_t                options_flags;
    moq_group_order_t       group_order;
    bool                    include_properties;
} moq_subscribe_tracks_cfg_t;

/* The symbol clears and stamps the layout through auth_token_count, the
 * struct as a caller built before options_flags was appended holds it, so it
 * never writes past such a caller's storage. The function-like macro passes
 * the caller's sizeof to moq_subscribe_tracks_cfg_init_sized(), which clears
 * and stamps min(cfg_size, sizeof current) and defaults include_properties
 * to true when cfg_size covers it. A binding that calls the symbol instead of
 * compiling the macro calls the _sized form with the size of its struct. */
MOQ_API void moq_subscribe_tracks_cfg_init(moq_subscribe_tracks_cfg_t *cfg);
MOQ_API void moq_subscribe_tracks_cfg_init_sized(moq_subscribe_tracks_cfg_t *cfg,
                                               size_t cfg_size);
#define moq_subscribe_tracks_cfg_init(cfg) \
    moq_subscribe_tracks_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Subscribe to track discovery via a bidi stream (subscriber side). Advancing
 * call. Opens a request bidi; the accept/reject arrives as the
 * SUBSCRIBE_TRACKS_OK / SUBSCRIBE_TRACKS_ERROR event. Returns MOQ_ERR_INVAL on a
 * draft-16 session or for an unrepresentable prefix, MOQ_ERR_WOULD_BLOCK if a
 * queue is full (nothing committed).
 */
MOQ_API moq_result_t moq_session_subscribe_tracks(
    moq_session_t *s,
    const moq_subscribe_tracks_cfg_t *cfg,
    uint64_t now_us,
    moq_track_sub_handle_t *out_handle);

typedef struct moq_accept_subscribe_tracks_cfg {
    uint32_t struct_size;
} moq_accept_subscribe_tracks_cfg_t;

MOQ_API void moq_accept_subscribe_tracks_cfg_init(
    moq_accept_subscribe_tracks_cfg_t *cfg);

/*
 * Accept a SUBSCRIBE_TRACKS request (publisher side). Advancing call.
 * Queues a positive response on the bidi (fin=false); the bidi stays established
 * for publish-blocked notifications / future track publications.
 */
MOQ_API moq_result_t moq_session_accept_subscribe_tracks(
    moq_session_t *s,
    moq_track_sub_handle_t handle,
    const moq_accept_subscribe_tracks_cfg_t *cfg,
    uint64_t now_us);

typedef struct moq_reject_subscribe_tracks_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    moq_bytes_t         reason;
} moq_reject_subscribe_tracks_cfg_t;

MOQ_API void moq_reject_subscribe_tracks_cfg_init(
    moq_reject_subscribe_tracks_cfg_t *cfg);

/*
 * Reject a SUBSCRIBE_TRACKS request (publisher side). Advancing call.
 * Queues an error response on the bidi (fin=true) and frees the entry.
 */
MOQ_API moq_result_t moq_session_reject_subscribe_tracks(
    moq_session_t *s,
    moq_track_sub_handle_t handle,
    const moq_reject_subscribe_tracks_cfg_t *cfg,
    uint64_t now_us);

/*
 * Send a PUBLISH_BLOCKED message on an established SUBSCRIBE_TRACKS
 * (publisher side). Advancing call. Signals that a PUBLISH for the named track
 * (suffix relative to the request prefix) cannot be opened. Queues
 * SEND_BIDI_STREAM (fin=false). Valid only in the ESTABLISHED publisher state.
 */
MOQ_API moq_result_t moq_session_send_publish_blocked(
    moq_session_t *s,
    moq_track_sub_handle_t handle,
    const moq_namespace_t *track_namespace_suffix,
    moq_bytes_t track_name,
    uint64_t now_us);

/* -- Namespace request updates (draft-22 §9.5, §9.5.2) -------------- *
 * The requester of a namespace announcement, a namespace subscription or a
 * SUBSCRIBE_TRACKS sends an update on the request's own stream:
 *
 *   namespace announcement   AUTHORIZATION_TOKEN
 *   namespace subscription   AUTHORIZATION_TOKEN, TRACK_NAMESPACE_PREFIX
 *   SUBSCRIBE_TRACKS         AUTHORIZATION_TOKEN, TRACK_NAMESPACE_PREFIX, FORWARD
 *
 * (SUBSCRIBE_TRACKS also permits TRACK_PROPERTY_FILTER, a Range Filter; this
 * side advertises MAX_FILTER_RANGES 0, so it neither sends one nor serves one.)
 * The responder answers each update exactly once, accepting or refusing it; a
 * refusal ends the request, and the responder closes the stream. A new prefix
 * must not share a common prefix with another active request of the same
 * type made in the same direction in the session (PREFIX_OVERLAP); the two
 * discovery types have independent spaces, and a request the peer made of
 * this side never overlaps one this side made of the peer. Draft-22 only:
 * every call below returns MOQ_ERR_UNSUPPORTED on another draft, before any
 * effect.
 */

typedef struct moq_namespace_update_cfg {
    uint32_t                struct_size;
    /* MOQ_NAMESPACE_UPDATE_PREFIX / _FORWARD; the family decides which are
     * legal (above). AUTHORIZATION is not a configuration flag. */
    uint32_t                flags;
    /* PREFIX: the new Track Namespace Prefix, 0..32 fields, borrowed for the
     * call. Zero fields selects the empty prefix. */
    moq_namespace_t         track_namespace_prefix;
    /* FORWARD: 0 pauses, 1 forwards, the subscriptions created from now on. */
    bool                    forward;
    uint8_t                 _reserved[7];
    /* AUTHORIZATION_TOKEN values (USE_VALUE), borrowed for the call. */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
} moq_namespace_update_cfg_t;

/* Frozen v0 layout: everything through auth_token_count. */
#define MOQ_NAMESPACE_UPDATE_CFG_V0_SIZE \
    (offsetof(moq_namespace_update_cfg_t, auth_token_count) + sizeof(size_t))

/* The symbol clears and stamps the v0 layout; the function-like macro passes
 * the caller's sizeof to the _sized form, which clears and stamps
 * min(cfg_size, sizeof current). cfg_size below struct_size is a no-op. */
MOQ_API void moq_namespace_update_cfg_init(moq_namespace_update_cfg_t *cfg);
MOQ_API void moq_namespace_update_cfg_init_sized(moq_namespace_update_cfg_t *cfg,
                                                 size_t cfg_size);
#define moq_namespace_update_cfg_init(cfg) \
    moq_namespace_update_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Send an update on an established request this side made (the announcer of
 * a namespace, the subscriber of a namespace subscription or of
 * SUBSCRIBE_TRACKS). Advancing call. Consumes a Request ID, written to
 * *out_update_request_id (may be NULL); the answer arrives as the family's
 * *_UPDATE_RESULT event naming that ID. A selected prefix replaces the
 * request's prefix only when the peer's acceptance arrives.
 *
 * At most one update is outstanding per request: MOQ_ERR_REQUEST_BLOCKED
 * while one awaits its answer. MOQ_ERR_INVAL for a NULL argument, a
 * struct_size below the v0 layout, an unknown flag or one the family does
 * not permit, an unrepresentable prefix or tokens, or a prefix overlapping
 * another active request of the same type this side made.
 * MOQ_ERR_STALE_HANDLE for a stale or foreign handle; MOQ_ERR_WRONG_STATE
 * when the request is not established with this side as requester, after
 * its terminal or a refused update, or after this side's GOAWAY on it;
 * MOQ_ERR_GOAWAY after the peer's GOAWAY; MOQ_ERR_CLOSED on a closed session;
 * MOQ_ERR_WOULD_BLOCK when the output cannot take the message now. Every
 * refusal leaves the request, its Request ID sequence and the output
 * unchanged.
 */
MOQ_API moq_result_t moq_session_update_namespace(moq_session_t *s,
    moq_announcement_t ann, const moq_namespace_update_cfg_t *cfg,
    uint64_t now_us, uint64_t *out_update_request_id);
MOQ_API moq_result_t moq_session_update_ns_sub(moq_session_t *s,
    moq_ns_sub_handle_t handle, const moq_namespace_update_cfg_t *cfg,
    uint64_t now_us, uint64_t *out_update_request_id);
MOQ_API moq_result_t moq_session_update_subscribe_tracks(moq_session_t *s,
    moq_track_sub_handle_t handle, const moq_namespace_update_cfg_t *cfg,
    uint64_t now_us, uint64_t *out_update_request_id);

/*
 * Answering peer updates. Off by default: without it this side refuses every
 * namespace update with NOT_SUPPORTED and closes the request, never
 * acknowledging one it has not decided. Switched on, a peer update that
 * passes this side's own checks -- its authorization tokens resolve, a new
 * prefix overlaps no other request of its type, it carries no Range Filter --
 * surfaces as the family's *_UPDATE_REQUEST event and waits for the
 * application's answer; a failed check is REFUSED (see the outcome above).
 * The setting governs updates that arrive afterwards; an answer already owed
 * stays owed. Not an advancing call. MOQ_ERR_INVAL for a NULL session; a
 * no-op on drafts without namespace updates.
 */
MOQ_API moq_result_t moq_session_set_defer_namespace_updates(moq_session_t *s,
                                                             bool defer);

/*
 * Answer the owed update of a request this side answers. Advancing calls.
 * update_request_id in the existing answer configurations must name the owed
 * update; another ID is MOQ_ERR_STALE_HANDLE and changes nothing.
 *
 * Accept queues the acceptance and applies the update: a selected prefix
 * becomes the request's prefix -- namespace notifications and
 * PUBLISH_SKIPPED sent after the acceptance name suffixes of it, and the
 * namespaces this side reported that lie outside it leave its set without a
 * withdrawal -- and a selected FORWARD becomes the value for the
 * subscriptions SUBSCRIBE_TRACKS leads to from now on. Existing
 * subscriptions are untouched (§9.5.2). Reject queues the refusal with
 * error_code and reason and closes this side's direction of the request
 * stream; the request ends and the handle is stale afterwards. A rejection
 * never ends a subscription.
 *
 * Both are all-or-nothing: MOQ_ERR_WOULD_BLOCK when the output cannot take
 * the answer now, MOQ_ERR_NOMEM when the new prefix or suffix set cannot be
 * stored; nothing is queued or applied then and the call can be retried.
 * MOQ_ERR_STALE_HANDLE for a stale handle or a non-owed ID,
 * MOQ_ERR_WRONG_STATE when no answer is owed, MOQ_ERR_INVAL as for
 * moq_session_reject_subscription_update (REDIRECT, an unencodable code, a
 * reason over 1024 bytes).
 */
MOQ_API moq_result_t moq_session_accept_namespace_update(moq_session_t *s,
    moq_announcement_t ann, const moq_accept_subscription_update_cfg_t *cfg,
    uint64_t now_us);
MOQ_API moq_result_t moq_session_reject_namespace_update(moq_session_t *s,
    moq_announcement_t ann, const moq_reject_subscription_update_cfg_t *cfg,
    uint64_t now_us);
MOQ_API moq_result_t moq_session_accept_ns_sub_update(moq_session_t *s,
    moq_ns_sub_handle_t handle, const moq_accept_subscription_update_cfg_t *cfg,
    uint64_t now_us);
MOQ_API moq_result_t moq_session_reject_ns_sub_update(moq_session_t *s,
    moq_ns_sub_handle_t handle, const moq_reject_subscription_update_cfg_t *cfg,
    uint64_t now_us);
MOQ_API moq_result_t moq_session_accept_subscribe_tracks_update(moq_session_t *s,
    moq_track_sub_handle_t handle, const moq_accept_subscription_update_cfg_t *cfg,
    uint64_t now_us);
MOQ_API moq_result_t moq_session_reject_subscribe_tracks_update(moq_session_t *s,
    moq_track_sub_handle_t handle, const moq_reject_subscription_update_cfg_t *cfg,
    uint64_t now_us);

/*
 * The update whose answer is owed, before or after its event is polled, for a
 * component answering without the event at hand. Not an advancing call: it
 * allocates nothing, consumes no event or action and does not authorize or
 * answer the update. Writes min(out_size, sizeof current) bytes of the record
 * and stamps struct_size with that count; its pointers borrow session storage
 * until the next advancing call. MOQ_ERR_INVAL for a NULL argument or an
 * out_size below MOQ_NAMESPACE_UPDATE_RECORD_V0_SIZE; MOQ_ERR_UNSUPPORTED on
 * another draft; MOQ_ERR_STALE_HANDLE for a stale handle; MOQ_ERR_WRONG_STATE
 * when nothing is owed. *out is untouched on refusal.
 */
MOQ_API moq_result_t moq_session_namespace_owed_update(moq_session_t *s,
    moq_announcement_t ann, moq_namespace_update_record_t *out, size_t out_size);
MOQ_API moq_result_t moq_session_ns_sub_owed_update(moq_session_t *s,
    moq_ns_sub_handle_t handle, moq_namespace_update_record_t *out,
    size_t out_size);
MOQ_API moq_result_t moq_session_subscribe_tracks_owed_update(moq_session_t *s,
    moq_track_sub_handle_t handle, moq_namespace_update_record_t *out,
    size_t out_size);

/*
 * Notify the session that the peer RESET_STREAM a bidi stream (abruptly
 * terminated its send half). Tears down the correlated request (namespace
 * subscription, or a stream-correlated subscription/fetch) and frees its state.
 * Advancing call: invalidates borrows, takes now_us.
 *
 * MOQ_ERR_WOULD_BLOCK means the teardown needs output capacity (an event, an
 * action or a drain reference) it does not have yet: drain events and actions,
 * then deliver the same signal again; it is not retried on its own. The
 * refusal is not always free of effects. When the request is a subscription
 * this session publishes, the subscription is cancelled from the first
 * delivery -- its subgroups, datagrams and responses are refused with
 * MOQ_ERR_WRONG_STATE from then on -- and a refused delivery may already
 * have queued RESET_DATA (CANCELLED) for some of its subgroups. Delivering it
 * again resumes without repeating them; MOQ_EVENT_UNSUBSCRIBED follows the
 * last of them, once. moq_session_on_bidi_stream_stop() behaves the same.
 */
MOQ_API moq_result_t moq_session_on_bidi_stream_reset(
    moq_session_t *s,
    moq_stream_ref_t stream_ref,
    uint64_t error_code,
    uint64_t now_us);

/*
 * Notify the session that the peer sent STOP_SENDING on a bidi stream (asking
 * the local endpoint to stop sending on its half). This is a distinct signal
 * from RESET_STREAM and MUST NOT be routed through on_bidi_stream_reset. For a
 * stream-correlated request stream it terminates the request and frees its
 * state; for profiles/streams that do not use request bidis it is a no-op.
 * Advancing call: invalidates borrows, takes now_us. MOQ_ERR_WOULD_BLOCK is
 * answered by delivering the same signal again once events and actions are
 * drained, with the partial effects described at
 * moq_session_on_bidi_stream_reset().
 */
MOQ_API moq_result_t moq_session_on_bidi_stream_stop(
    moq_session_t *s,
    moq_stream_ref_t stream_ref,
    uint64_t error_code,
    uint64_t now_us);

/* -- Subgroup lifecycle --------------------------------------------- */

typedef struct moq_subgroup_handle { uint64_t _opaque; } moq_subgroup_handle_t;

#ifdef __cplusplus
#define MOQ_SUBGROUP_INVALID (moq_subgroup_handle_t{0})
#else
#define MOQ_SUBGROUP_INVALID ((moq_subgroup_handle_t){ 0 })
#endif

MOQ_API bool     moq_subgroup_is_valid(moq_subgroup_handle_t h);
MOQ_API bool     moq_subgroup_eq(moq_subgroup_handle_t a, moq_subgroup_handle_t b);
MOQ_API uint64_t moq_subgroup_hash(moq_subgroup_handle_t h);
MOQ_API uint64_t moq_subgroup_id_for_trace(moq_subgroup_handle_t h);

typedef struct moq_subgroup_cfg {
    uint32_t struct_size;
    uint64_t group_id;
    uint64_t subgroup_id;
    uint8_t  publisher_priority;
    uint8_t  _reserved_sg[7];
    bool     object_properties;
    uint8_t  _reserved_sg2[7];
    bool     end_of_group;
    /* Never read. Keeps first_object past the sizeof of the layout that ends
     * at end_of_group, so a caller built against that layout never has its
     * tail padding read as a new field. */
    uint8_t  _reserved_sg3[7];
    /* Appended. Set the subgroup header's FIRST_OBJECT bit: the first object
     * written on this stream is the first object ever published in this
     * subgroup. The Original Publisher MUST set it when it opens a new
     * subgroup, and a relay forwarding a subgroup that begins with that first
     * object MUST set it (draft-18 and draft-22 §2.2); a stream that starts
     * later in the subgroup leaves it clear. Draft-16 has no such bit and
     * ignores it. A caller whose struct_size ends before this field sends the
     * bit clear. */
    bool     first_object;
} moq_subgroup_cfg_t;

/* The symbol clears and stamps the config as a caller built before
 * first_object was appended holds it (its sizeof through end_of_group). The
 * function-like macro passes the caller's sizeof to
 * moq_subgroup_cfg_init_sized(), which clears and stamps min(cfg_size,
 * sizeof(moq_subgroup_cfg_t)); a cfg_size smaller than struct_size is a
 * no-op. Bindings that cannot expand the macro call the sized form. */
MOQ_API void moq_subgroup_cfg_init(moq_subgroup_cfg_t *cfg);
MOQ_API void moq_subgroup_cfg_init_sized(moq_subgroup_cfg_t *cfg, size_t cfg_size);
#define moq_subgroup_cfg_init(cfg) \
    moq_subgroup_cfg_init_sized((cfg), sizeof(*(cfg)))

MOQ_API moq_result_t moq_session_open_subgroup(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_subgroup_cfg_t *cfg,
    uint64_t now_us,
    moq_subgroup_handle_t *out_handle);

MOQ_API moq_result_t moq_session_write_object(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    uint64_t object_id,
    moq_rcbuf_t *payload,
    uint64_t now_us);

/* Write a zero-length terminal status object (END_OF_GROUP / END_OF_TRACK) on a
 * subgroup carrying no object properties -- the reliable, stream-based
 * counterpart to moq_session_send_status_datagram. The caller closes the
 * subgroup afterwards. */
MOQ_API moq_result_t moq_session_write_status_object(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    uint64_t object_id,
    moq_object_status_t status,
    uint64_t now_us);

typedef struct moq_object_cfg {
    uint32_t     struct_size;
    uint64_t     object_id;
    moq_rcbuf_t *payload;
    moq_rcbuf_t *properties;  /* NULL or zero-length = none; non-empty requires object_properties=true */
    /* Appended. Read only when struct_size covers prior_object_gap;
     * _reserved_gap is never read (it keeps the new fields past the original
     * sizeof where that ends in tail padding). The Original Publisher states
     * that the prior_group_gap Groups before this Object's Group, or the
     * prior_object_gap Objects before it in its Group, do not and will never
     * exist: Prior Group ID Gap (0x3C) and Prior Object ID Gap (0x3E),
     * draft-22 §10.8 and §10.9 (draft-18 §12.8, §12.9). Each one set is added
     * to `properties` in type order, so the subgroup must have been opened
     * with object_properties=true. A gap larger than the Group ID or the
     * Object ID, or one that conflicts with the same property already in
     * `properties`, is MOQ_ERR_INVAL; a profile without these properties
     * (draft-16) is MOQ_ERR_UNSUPPORTED. */
    uint32_t     _reserved_gap;
    bool         has_prior_group_gap;
    bool         has_prior_object_gap;
    uint8_t      _pad_gap[2];
    uint64_t     prior_group_gap;
    uint64_t     prior_object_gap;
} moq_object_cfg_t;

/* The symbol clears and stamps the layout through `properties`, the struct as
 * a caller built before the gap fields were appended holds it. The
 * function-like macro passes the caller's sizeof to moq_object_cfg_init_sized(),
 * which clears and stamps min(cfg_size, sizeof current). A binding that calls
 * the symbol instead of compiling the macro calls the _sized form with the
 * size of its struct. */
MOQ_API void moq_object_cfg_init(moq_object_cfg_t *cfg);
MOQ_API void moq_object_cfg_init_sized(moq_object_cfg_t *cfg, size_t cfg_size);
#define moq_object_cfg_init(cfg) moq_object_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Write a complete object with optional properties. Advancing call.
 *
 * If cfg->properties != NULL, the subgroup must have been opened with
 * object_properties=true. Uses two actions when properties are present.
 * Returns MOQ_ERR_WOULD_BLOCK if insufficient action queue slots.
 */
MOQ_API moq_result_t moq_session_write_object_ex(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    const moq_object_cfg_t *cfg,
    uint64_t now_us);

MOQ_API moq_result_t moq_session_close_subgroup(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    uint64_t now_us);

MOQ_API moq_result_t moq_session_reset_subgroup(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    uint64_t error_code,
    uint64_t now_us);

typedef struct moq_publication_update_cfg {
    uint32_t struct_size;
    bool     has_subscriber_priority;
    uint8_t  subscriber_priority;
    bool     has_forward;
    bool     forward;
    bool     has_delivery_timeout;
    uint8_t  _pad[5];
    uint64_t delivery_timeout_us;
    /* Appended (ABI-safe): authorization tokens carried on the update,
     * by value, the same shape as subscribe/fetch. Callers compiled
     * against the smaller struct send no tokens. Semantic token validity
     * is the receiver's call (the sender stays permissive). */
    const moq_auth_token_t *auth_tokens;
    size_t                  auth_token_count;
    /* Appended (ABI-safe): request the original publisher start a new
     * group (largest known Group ID + 1; 0 = "no group info" and is
     * meaningful, hence the has_ flag). Valid only when the track carried
     * dynamic-group support; otherwise MOQ_ERR_INVAL. Callers compiled
     * against the smaller struct send nothing. */
    bool                    has_new_group_request;
    uint64_t                new_group_request;
    /* Draft-22-only tail after the COMPLETE old layout. Nested values are
     * borrowed only for this call and normalized before effects. NULL location
     * leaves the filter unchanged; explicit NONE removes the constraint.
     * A fill is an independent request, never sticky or a cancellation of an
     * earlier fill. A selected location or fill is UNSUPPORTED on
     * draft-16/18. */
    const moq_location_filter_cfg_t *location;
    const moq_fill_cfg_t *fill;
    /* MOQ_SUBSCRIPTION_UPDATE_{OBJECT,SUBGROUP}_DELIVERY_TIMEOUT flags.
     * Read as one complete block through subgroup_delivery_timeout_ms.
     * Omitted carriers retain their previous values; selected zero and
     * UINT64_MAX are exact milliseconds. Unknown flags/reserved are INVAL,
     * and so is selecting them with delivery_timeout_us. Draft-16 has one
     * DELIVERY_TIMEOUT: both selected values must be equal and neither 0. */
    uint32_t delivery_timeout_flags;
    uint32_t reserved;
    uint64_t object_delivery_timeout_ms;
    uint64_t subgroup_delivery_timeout_ms;
} moq_publication_update_cfg_t;

/*
 * The symbol clears and stamps the layout through new_group_request -- the
 * complete pre-draft-22 struct -- so it never
 * writes past such a caller's storage. A caller built against this header
 * reaches the whole struct: the function-like macro below passes its sizeof
 * to moq_publication_update_cfg_init_sized(), which clears and stamps
 * min(cfg_size, sizeof current).
 * A binding that calls the symbol instead of compiling the macro (an FFI, a
 * dlsym lookup) gets only that layout: fields it sets past it are ignored.
 * Such a binding calls moq_publication_update_cfg_init_sized() with the size of its struct.
 */
MOQ_API void moq_publication_update_cfg_init(
    moq_publication_update_cfg_t *cfg);
MOQ_API void moq_publication_update_cfg_init_sized(
    moq_publication_update_cfg_t *cfg, size_t cfg_size);
#define moq_publication_update_cfg_init(cfg) \
    moq_publication_update_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Update an accepted publisher-initiated subscription. Advancing call.
 * Called by the subscriber who accepted PUBLISH. Consumes request
 * capacity.
 *
 * Returns MOQ_ERR_STALE_HANDLE if the publication handle is invalid.
 * Returns MOQ_ERR_WRONG_STATE if the publication is not ESTABLISHED
 * with role SUBSCRIBER, or if an update is already pending.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full.
 * Draft-22 accepts an empty refresh. A selected legacy delivery_timeout_us
 * is UNSUPPORTED on draft-22; use the independent millisecond carriers.
 * Invalid/unsupported typed values are refused before effects. Partial tail
 * pointers or timeout blocks are ignored, never read piecemeal. A successful
 * UPDATE takes effect when acknowledged, not when queued. It never changes
 * the immutable initial PUBLISH parameters.
 * Returns MOQ_ERR_INVAL if no update fields are set on draft-16/18.
 */
MOQ_API moq_result_t moq_session_update_publication(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_publication_update_cfg_t *cfg,
    uint64_t now_us);

/*
 * stream_count follows moq_done_subscribe_cfg_t: the data streams the
 * APPLICATION opened for this publication; the session adds the fill
 * fetch streams it opened itself, saturating to the profile's "unknown"
 * sentinel, which an application may also pass directly.
 */
typedef struct moq_finish_publish_cfg {
    uint32_t    struct_size;
    uint64_t    status_code;
    uint64_t    stream_count;
    moq_bytes_t reason;
} moq_finish_publish_cfg_t;

MOQ_API void moq_finish_publish_cfg_init(moq_finish_publish_cfg_t *cfg);

/*
 * Finish a publisher-initiated subscription. Advancing call.
 * Queues the terminal completion message and frees the entry.
 * Returns MOQ_ERR_WRONG_STATE while an answer to an update is owed (answer
 * it first: a rejection itself ends the subscription with UPDATE_FAILED).
 * Returns MOQ_ERR_STALE_HANDLE once the session has ended the publication
 * itself -- e.g. an update it refused on its own, which it ends with
 * UPDATE_FAILED -- whether or not that terminal and the resets before it
 * have gone out yet: the session still sends them. A stale handle is not
 * proof that any byte or FIN reached the peer.
 */
MOQ_API moq_result_t moq_session_finish_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_finish_publish_cfg_t *cfg,
    uint64_t now_us);

/*
 * Open a subgroup for a publisher-initiated subscription.
 * Mirrors moq_session_open_subgroup but takes a publication handle.
 * The existing write_object/close_subgroup/reset_subgroup APIs
 * work on the returned subgroup handle.
 */
MOQ_API moq_result_t moq_session_open_pub_subgroup(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_subgroup_cfg_t *cfg,
    uint64_t now_us,
    moq_subgroup_handle_t *out_handle);

/* -- Streaming object send ----------------------------------------- */

/*
 * Begin a streamed object on a subgroup. Advancing call.
 *
 * Queues the object header (delta + payload_length). The application
 * must then push exactly payload_length bytes via write_object_data
 * and call end_object to finalize.
 *
 * payload_length must be known upfront (required by wire format).
 * If payload_length == 0, no write_object_data calls are needed.
 *
 * Returns MOQ_ERR_WRONG_STATE if a streamed object is already active.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full; the
 * subgroup remains OPEN for retry.
 */
MOQ_API moq_result_t moq_session_begin_object(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    uint64_t object_id,
    uint64_t payload_length,
    uint64_t now_us);

typedef struct moq_begin_object_cfg {
    uint32_t     struct_size;
    uint64_t     object_id;
    uint64_t     payload_length;
    moq_rcbuf_t *properties;  /* NULL or zero-length = none; non-empty requires object_properties=true */
} moq_begin_object_cfg_t;

MOQ_API void moq_begin_object_cfg_init(moq_begin_object_cfg_t *cfg);

/*
 * Begin a streamed object with optional properties. Advancing call.
 *
 * If cfg->properties != NULL, properties bytes are queued as part of the
 * object header. The application then writes exactly payload_length bytes
 * via write_object_data and calls end_object. payload_length is the
 * declared payload length, not including properties.
 */
MOQ_API moq_result_t moq_session_begin_object_ex(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    const moq_begin_object_cfg_t *cfg,
    uint64_t now_us);

/*
 * Push payload data for the active streamed object. Advancing call.
 *
 * Queues a payload-only SEND_DATA action. data must not be NULL.
 * Zero-length data is a no-op (no action queued, no state change).
 *
 * Returns MOQ_ERR_INVAL if bytes would exceed declared payload_length.
 * Returns MOQ_ERR_WRONG_STATE if no streamed object is active.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full;
 * streaming_bytes_written is NOT advanced on failure.
 */
MOQ_API moq_result_t moq_session_write_object_data(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    moq_rcbuf_t *data,
    uint64_t now_us);

/*
 * Finalize a streamed object. Advancing call.
 *
 * Validates streaming_bytes_written == payload_length and returns the
 * subgroup to OPEN state. Queues no action.
 *
 * Returns MOQ_ERR_INVAL if bytes written != declared payload_length
 * (subgroup stays STREAMING so the app can write remaining bytes).
 * Returns MOQ_ERR_WRONG_STATE if no streamed object is active.
 */
MOQ_API moq_result_t moq_session_end_object(
    moq_session_t *s,
    moq_subgroup_handle_t subgroup,
    uint64_t now_us);

/* -- Timer --------------------------------------------------------- */

/*
 * Returns the absolute time of the next internal deadline.
 * UINT64_MAX means no deadline. Observing (does not invalidate borrows).
 *
 * The adapter should poll this after each advancing call and arm a
 * timer to call moq_session_tick() when time reaches this value.
 */
MOQ_API uint64_t moq_session_next_deadline_us(const moq_session_t *s);

/*
 * Transport-adapter support: returns true if the session owns the
 * transport stream with the given ref -- it holds receive state for a
 * peer data stream (uni), including one it has STOPped and is consuming
 * until the peer's FIN/RESET, or owns a bidi request stream. Adapters
 * call this after MOQ_ERR_WOULD_BLOCK to distinguish post-retention
 * (the bytes were retained; an empty retry is safe) from pre-retention
 * (nothing retained). What an unowned refusal means depends on the input.
 * For a peer uni data stream the session will never parse that stream
 * (its receive entries and stopped-identity records are exhausted); the
 * transport bridge discards the stream's remainder until the peer's
 * FIN/RESET. That is the current measured behaviour, not a lifecycle
 * guarantee: a discarded stream answering a live FETCH leaves the request
 * without a terminal. For a bidi request stream the bridge treats it as
 * fatal. Control bytes are always retained by the session on WOULD_BLOCK.
 * Observing (does not invalidate borrows).
 */
MOQ_API bool moq_session_has_transport_stream(const moq_session_t *s,
                                               moq_stream_ref_t ref);

/*
 * Transport-adapter support: true if the session could admit a NEW peer
 * data stream now (a receive entry is free). Advisory observation, not a
 * reservation: another stream may take the entry before the caller's next
 * input, which is then refused the same way again. Reports true when the
 * session is no longer active so that inputs still reach its terminal
 * path (closed/fatal outcomes are never hidden behind capacity).
 * Observing (does not invalidate borrows).
 */
MOQ_API bool moq_session_can_admit_data_stream(const moq_session_t *s);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_SESSION_H */
