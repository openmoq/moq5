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
} moq_version_t;

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
    uint32_t           max_actions;         /* action queue entries   (default 64) */
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

    uint64_t           goaway_timeout_us;  /* GOAWAY drain timeout; 0 = disabled */

    uint32_t           max_namespace_subscriptions;  /* ns_sub pool (default 16) */

    /*
     * Reserved: the 4 bytes of tail padding the pre-version layout left after
     * max_namespace_subscriptions (it ended the struct at this 8-byte boundary).
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
 * Client sessions start in IDLE; call moq_session_start() to begin
 * the setup handshake. Server sessions are ready to receive the
 * peer's setup immediately after create (no start call needed).
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

/* -- Inputs (advancing calls) -------------------------------------- */

/*
 * Client only: begin setup handshake. Transitions IDLE -> SETUP_SENT.
 * Returns MOQ_ERR_WRONG_STATE if not a client or not in IDLE.
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
 * Returns MOQ_ERR_WOULD_BLOCK if an event or action queue is full.
 * The caller should drain the full queue and then retry with:
 *   moq_session_on_data_bytes(s, stream_ref, NULL, 0, false, now_us)
 * This retries any pending emission for the stream and is a no-op
 * for unknown stream_refs.
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
} moq_send_data_action_t;

typedef struct moq_reset_data_action {
    moq_stream_ref_t stream_ref;
    uint64_t         error_code;
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

typedef uint32_t moq_request_error_t;

#define MOQ_REQUEST_ERROR_INTERNAL_ERROR         0x0u
#define MOQ_REQUEST_ERROR_UNAUTHORIZED           0x1u
#define MOQ_REQUEST_ERROR_TIMEOUT                0x2u
#define MOQ_REQUEST_ERROR_NOT_SUPPORTED          0x3u
#define MOQ_REQUEST_ERROR_GOING_AWAY             0x6u
#define MOQ_REQUEST_ERROR_DOES_NOT_EXIST         0x10u
#define MOQ_REQUEST_ERROR_INVALID_RANGE          0x11u
#define MOQ_REQUEST_ERROR_DUPLICATE_SUBSCRIPTION     0x19u
#define MOQ_REQUEST_ERROR_PREFIX_OVERLAP             0x30u
#define MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID 0x32u
#define MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION      0x33u
#define MOQ_REQUEST_ERROR_REDIRECT                   0x34u

/* -- Subscribe event detail structs -------------------------------- */

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
} moq_subscribe_ok_event_t;

typedef struct moq_subscribe_error_event {
    moq_subscription_t  sub;
    moq_request_error_t error_code;
    bool                can_retry;
    uint8_t             _pad[3];
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
    uint8_t            _pad[4];
    moq_rcbuf_t       *payload;     /* OWNED ref transferred on poll; NULL for status objects */
    moq_rcbuf_t       *properties;  /* OWNED ref transferred on poll; NULL if none */
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
    uint8_t             _pad[3];
    uint64_t            retry_after_ms;
    moq_bytes_t         reason;  /* BORROWED from output scratch */
} moq_namespace_rejected_event_t;

typedef struct moq_namespace_accepted_event {
    moq_announcement_t ann;
} moq_namespace_accepted_event_t;

typedef struct moq_namespace_cancelled_event {
    moq_announcement_t  ann;
    moq_request_error_t error_code;
    uint8_t             _pad[4];
    moq_bytes_t         reason;  /* BORROWED from output scratch */
} moq_namespace_cancelled_event_t;

typedef struct moq_goaway_event {
    moq_bytes_t new_session_uri;  /* BORROWED from output scratch; empty if reuse current URI */
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

/* A relay redirected a request (error code REDIRECT, §10.6.1): retry it on
 * `connect_uri` (empty ⇒ reuse the current session URI) using
 * `track_namespace`/`track_name` (both empty ⇒ reuse the original request's). The
 * request is terminal; `handle` identifies which request (select by `family`). All
 * byte fields are BORROWED from output scratch. */
typedef struct moq_request_redirect_event {
    moq_request_family_t family;
    union {                                  /* select by family */
        moq_subscription_t        subscription;     /* subscribe */
        moq_fetch_t               fetch;            /* fetch */
        moq_track_status_handle_t track_status;     /* track status */
        moq_announcement_t        announcement;     /* namespace announcement */
        moq_ns_sub_handle_t       ns_sub;           /* namespace subscription */
        uint64_t                  raw;
    } handle;
    moq_request_error_t error_code;          /* REDIRECT */
    bool                can_retry;           /* retry interval was non-zero */
    uint8_t             _pad[3];
    uint64_t            retry_after_ms;      /* §10.6.1: relays may cache up to this */
    moq_bytes_t         connect_uri;         /* empty ⇒ reuse current session URI */
    moq_namespace_t     track_namespace;     /* empty ⇒ same as original request */
    moq_bytes_t         track_name;          /* empty ⇒ same as original request */
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
} moq_unsubscribed_event_t;

typedef struct moq_subscribe_done_event {
    moq_subscription_t sub;
    uint64_t           status_code;
    uint64_t           stream_count;
    moq_bytes_t        reason;  /* BORROWED from output scratch */
} moq_subscribe_done_event_t;

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
    moq_object_terminal_t terminal;    /* valid when end=true */
    uint8_t               _pad[3];
    uint64_t              group_id;        /* valid when begin=true */
    uint64_t              subgroup_id;     /* valid when begin=true */
    uint64_t              object_id;       /* valid when begin=true */
    uint8_t               publisher_priority; /* valid when begin=true */
    moq_object_status_t   status;          /* valid when begin=true */
    bool                  end_of_group;    /* valid when begin=true */
    uint8_t               _pad2[4];
    uint64_t              payload_length;  /* declared wire length; valid when begin=true */
    moq_rcbuf_t          *properties;      /* OWNED; valid when begin=true, NULL if none */
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
    uint8_t              _pad[4];
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
} moq_fetch_request_event_t;

typedef struct moq_fetch_error_event {
    moq_fetch_t         fetch;
    moq_request_error_t error_code;
    bool                can_retry;
    uint8_t             _pad[3];
    uint64_t            retry_after_ms;
    moq_bytes_t         reason;  /* BORROWED from output scratch */
} moq_fetch_error_event_t;

typedef struct moq_fetch_cancelled_event {
    moq_fetch_t fetch;
} moq_fetch_cancelled_event_t;

typedef struct moq_fetch_ok_event {
    moq_fetch_t fetch;
    bool        end_of_track;
    uint8_t     _pad[7];
    uint64_t    end_group;
    uint64_t    end_object;
    moq_bytes_t track_properties;  /* BORROWED from output scratch */
} moq_fetch_ok_event_t;

typedef struct moq_fetch_complete_event {
    moq_fetch_t fetch;
} moq_fetch_complete_event_t;

typedef enum moq_fetch_range_kind {
    MOQ_FETCH_RANGE_NON_EXISTENT = 1,
    MOQ_FETCH_RANGE_UNKNOWN      = 2,
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
} moq_fetch_object_event_t;

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
} moq_publish_ok_event_t;

typedef struct moq_publish_error_event {
    moq_publication_t   pub;
    moq_request_error_t error_code;
    bool                can_retry;
    uint8_t             _pad[3];
    uint64_t            retry_after_ms;
    moq_bytes_t         reason;
} moq_publish_error_event_t;

typedef struct moq_publish_finished_event {
    moq_publication_t pub;
    uint64_t          status_code;
    uint64_t          stream_count;
    moq_bytes_t       reason;  /* BORROWED from output scratch */
} moq_publish_finished_event_t;

typedef struct moq_publish_unsubscribed_event {
    moq_publication_t pub;
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
} moq_publish_updated_event_t;

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
    uint8_t                   _pad[3];
    uint64_t                  retry_after_ms;
    moq_bytes_t               reason;
} moq_track_status_error_event_t;

/* -- SUBSCRIBE_TRACKS events (draft-18) ---------------------------- */

/* Publisher side: a peer's SUBSCRIBE_TRACKS arrived. Reply with
 * moq_session_accept_subscribe_tracks / _reject_subscribe_tracks. */
typedef struct moq_subscribe_tracks_request_event {
    moq_track_sub_handle_t handle;
    moq_namespace_t        track_namespace_prefix;  /* BORROWED from output scratch */
    bool                   forward;
    uint8_t                _pad[7];
    const moq_resolved_token_t *tokens;   /* BORROWED from output scratch; NULL if none */
    size_t                 token_count;
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
    uint8_t                _pad[3];
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
} moq_subscribe_tracks_cancelled_event_t;

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
        moq_track_status_request_event_t track_status_request;
        moq_track_status_ok_event_t      track_status_ok;
        moq_track_status_error_event_t   track_status_error;
        moq_subscribe_tracks_request_event_t   subscribe_tracks_request;
        moq_subscribe_tracks_ok_event_t        subscribe_tracks_ok;
        moq_subscribe_tracks_error_event_t     subscribe_tracks_error;
        moq_publish_blocked_event_t            publish_blocked;
        moq_subscribe_tracks_cancelled_event_t subscribe_tracks_cancelled;
        moq_subgroup_finished_event_t          subgroup_finished;
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
 * OBJECT_RECEIVED: decrefs payload and properties, sets both NULL.
 * All other kinds (including SUBGROUP_FINISHED, which owns none): no-op.
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

/*
 * Subscribe configuration.
 *
 * Required: track_namespace (1..32 non-empty parts), track_name,
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
} moq_subscribe_cfg_t;

MOQ_API void moq_subscribe_cfg_init(moq_subscribe_cfg_t *cfg);

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
 *
 * Valid for subscriber-role subscriptions in PENDING_SUBSCRIBER or
 * ESTABLISHED state. Returns MOQ_ERR_STALE_HANDLE if the handle is
 * invalid, MOQ_ERR_WRONG_STATE if wrong role/state.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full; the
 * handle remains valid for retry.
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
 * Valid for publisher-role subscriptions in ESTABLISHED state.
 * Returns MOQ_ERR_WRONG_STATE if subgroups are still open.
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
} moq_subscription_update_cfg_t;

MOQ_API void moq_subscription_update_cfg_init(
    moq_subscription_update_cfg_t *cfg);

/*
 * Update an active subscription's parameters. Advancing call.
 * Consumes request capacity.
 *
 * Returns MOQ_ERR_STALE_HANDLE if the subscription handle is invalid.
 * Returns MOQ_ERR_WRONG_STATE if the subscription is not ESTABLISHED
 * with role SUBSCRIBER.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full.
 * Returns MOQ_ERR_INVAL if no update fields are set.
 */
MOQ_API moq_result_t moq_session_update_subscription(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_subscription_update_cfg_t *cfg,
    uint64_t now_us);

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
} moq_fetch_cfg_t;

MOQ_API void moq_fetch_cfg_init(moq_fetch_cfg_t *cfg);

/*
 * Initiate a fetch (subscriber side). Advancing call.
 * Set is_joining=true for a joining fetch; joining_relative selects
 * relative (true) or absolute (false) mode. joining_sub must be a
 * subscription with the LARGEST_OBJECT filter that has a *current*
 * stored largest location -- i.e. one that has received SUBSCRIBE_OK
 * (established). A still-pending subscription has no current largest
 * yet and is rejected with MOQ_ERR_INVAL. Track namespace/name are
 * ignored for joining fetches.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 */
MOQ_API moq_result_t moq_session_fetch(moq_session_t *s,
                                        const moq_fetch_cfg_t *cfg,
                                        uint64_t now_us,
                                        moq_fetch_t *out_handle);

typedef struct moq_accept_fetch_cfg {
    uint32_t struct_size;
    bool     end_of_track;
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
 * If cfg->empty is true, the data stream is opened with FIN set.
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
 * Publisher side. Advancing call.
 */
MOQ_API moq_result_t moq_session_write_fetch_range(
    moq_session_t *s,
    moq_fetch_t fetch,
    moq_fetch_range_kind_t kind,
    uint64_t group_id,
    uint64_t object_id,
    uint64_t now_us);

/*
 * Close the fetch data stream with FIN. Publisher side. Advancing call.
 * Frees the fetch entry after queuing the final action.
 */
MOQ_API moq_result_t moq_session_end_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    uint64_t now_us);

/*
 * Cancel a pending fetch (subscriber side). Advancing call.
 * Queues cancellation bytes. Frees the fetch entry.
 */
MOQ_API moq_result_t moq_session_fetch_cancel(moq_session_t *s,
                                               moq_fetch_t fetch,
                                               uint64_t now_us);

/* -- Per-request GOAWAY send (draft-18 §10.4 migration) ------------ *
 * Emit a GOAWAY on an active request bidi to migrate that single request; the
 * peer re-issues it on `new_session_uri` (empty => the current session). Keeps
 * the request bidi and its data streams alive (graceful migration); at most one
 * per request stream (a second ⇒ MOQ_ERR_WRONG_STATE). A client MUST leave the
 * URI empty (else MOQ_ERR_INVAL). Draft-16 has no per-request GOAWAY ⇒
 * MOQ_ERR_INVAL. Eligible states (else MOQ_ERR_WRONG_STATE): SUBSCRIBE/
 * PUBLISH/ANNOUNCEMENT/NS_SUB/SUBSCRIBE_TRACKS established, FETCH accepted,
 * TRACK_STATUS only on the responder (the requester opened with FIN). */
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
} moq_publish_cfg_t;

MOQ_API void moq_publish_cfg_init(moq_publish_cfg_t *cfg);

/*
 * Initiate a publisher-initiated subscription. Advancing call.
 * Returns MOQ_ERR_REQUEST_BLOCKED if no request capacity.
 */
MOQ_API moq_result_t moq_session_publish(moq_session_t *s,
                                          const moq_publish_cfg_t *cfg,
                                          uint64_t now_us,
                                          moq_publication_t *out_handle);

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
} moq_accept_publish_cfg_t;

MOQ_API void moq_accept_publish_cfg_init(moq_accept_publish_cfg_t *cfg);

/*
 * Accept an incoming publish (subscriber side). Advancing call.
 * Queues a positive response.
 */
MOQ_API moq_result_t moq_session_accept_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_accept_publish_cfg_t *cfg,
    uint64_t now_us);

typedef struct moq_reject_publish_cfg {
    uint32_t            struct_size;
    moq_request_error_t error_code;
    moq_bytes_t         reason;
    bool                can_retry;
    uint64_t            retry_after_ms;
} moq_reject_publish_cfg_t;

MOQ_API void moq_reject_publish_cfg_init(moq_reject_publish_cfg_t *cfg);

/*
 * Reject an incoming publish (subscriber side). Advancing call.
 * Queues an error response. Frees the publish entry.
 */
MOQ_API moq_result_t moq_session_reject_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_reject_publish_cfg_t *cfg,
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
} moq_track_status_cfg_t;

MOQ_API void moq_track_status_cfg_init(moq_track_status_cfg_t *cfg);

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
} moq_accept_track_status_cfg_t;

MOQ_API void moq_accept_track_status_cfg_init(moq_accept_track_status_cfg_t *cfg);

/*
 * Accept a track status request (publisher side). Advancing call.
 * Queues a positive response. No subscription state is created.
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

/* -- SUBSCRIBE_TRACKS (draft-18 §10.19 / §10.20) ------------------- *
 * Track discovery: request PUBLISH messages for all tracks under a namespace
 * prefix (and future ones). Draft-18 only — these calls return MOQ_ERR_INVAL on
 * a draft-16 session. The overlap space is independent of subscribe_namespace.
 * The resulting PUBLISH messages (separate bidi streams) are not modelled yet;
 * an established subscription carries PUBLISH_BLOCKED in the interim. */

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
} moq_subscribe_tracks_cfg_t;

MOQ_API void moq_subscribe_tracks_cfg_init(moq_subscribe_tracks_cfg_t *cfg);

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

/*
 * Notify the session that the peer RESET_STREAM a bidi stream (abruptly
 * terminated its send half). Tears down the correlated request (namespace
 * subscription, or a stream-correlated subscription/fetch) and frees its state.
 * Advancing call: invalidates borrows, takes now_us.
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
 * Advancing call: invalidates borrows, takes now_us.
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
} moq_subgroup_cfg_t;

MOQ_API void moq_subgroup_cfg_init(moq_subgroup_cfg_t *cfg);

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
} moq_object_cfg_t;

MOQ_API void moq_object_cfg_init(moq_object_cfg_t *cfg);

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
} moq_publication_update_cfg_t;

MOQ_API void moq_publication_update_cfg_init(
    moq_publication_update_cfg_t *cfg);

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
 * Returns MOQ_ERR_INVAL if no update fields are set.
 */
MOQ_API moq_result_t moq_session_update_publication(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_publication_update_cfg_t *cfg,
    uint64_t now_us);

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
 * Transport-adapter support: returns true if the session has retained
 * receive state for a data stream (uni) or bidi stream with the given
 * ref. Adapters call this after MOQ_ERR_WOULD_BLOCK to distinguish
 * post-retention (empty retry safe) from pre-retention (bytes not
 * retained, adapter must go fatal).
 * Observing (does not invalidate borrows).
 */
MOQ_API bool moq_session_has_transport_stream(const moq_session_t *s,
                                               moq_stream_ref_t ref);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_SESSION_H */
