#ifndef MOQ_WTQUIC_MSQUIC_MANAGED_H
#define MOQ_WTQUIC_MSQUIC_MANAGED_H

/*
 * Stability: pre-1.0, transport-specific adapter API. May change
 * before 1.0.
 *
 * moq_wtquic_msquic_managed: owns the wtquic-over-MsQuic lifecycle
 * (environment, listener/client, per-connection MoQ session + attach
 * adapter) behind a small C facade. It is the wtquic sibling of
 * moq_msquic_managed (the raw-MsQuic managed adapter): same lane model
 * and lifecycle surface, but built on wtquic's public WebTransport
 * session API and its managed-domain contract (a caller-owned guard,
 * paired server admission, and a transport-quiescence hook) rather than
 * a raw QuicSocket.
 *
 * Like raw-MsQuic, MsQuic owns its worker threads, so there is no facade
 * network thread. Transport events run on MsQuic's serialized
 * per-connection worker; the facade supplies each connection's lane
 * mutex as wtquic's serialization-domain guard (wtq_guard_t), so a
 * worker callback and an app-driven pump on the same lane exclude each
 * other. A small per-lane doorbell thread delivers wake() and MoQ
 * session deadlines; it never dispatches transport events.
 *
 * Threading model — LANE OWNERSHIP (app-visible contract): the facade
 * exposes `lane_count` lanes, each an independent lock domain with its
 * own doorbell. Every connection is assigned to exactly one lane (server:
 * at accept, cfg.choose_lane or round-robin; client: lane 0) and stays on
 * that lane for its whole life. A lane's `on_lane_pump` is the EXCLUSIVE
 * access window for every session on that lane: moq_session_* /
 * moq_wtquic_conn_* calls on a lane's connections are legal ONLY inside
 * that lane's pump. A pump owns and services MANY connections in one pass
 * (iterate with moq_wtquic_msquic_lane_next_conn). Pumps for DIFFERENT
 * lanes may run concurrently; MsQuic worker callbacks feed+service their
 * connection under that connection's lane guard, so distinct lanes never
 * contend.
 *
 * Applications must not share non-thread-safe session or rcbuf ownership
 * across lanes; moq_rcbuf refcounts are non-atomic (single-lane
 * confinement is a hard requirement).
 *
 * Callback rules:
 *   choose_lane:  server accept placement; returns the lane index that
 *                 owns a new connection. Runs with NO session access;
 *                 must not touch sessions. Optional (NULL = round-robin).
 *                 An out-of-range return REFUSES the connection.
 *   on_lane_pump: may call moq_session_* / moq_wtquic_conn_* on the
 *                 lane's connections, the facade accessors, and
 *                 lane_wake(). Return 0 to continue, nonzero for clean
 *                 termination of the whole facade.
 *   on_activity:  signal-only (wake a blocked thread); must NOT call
 *                 mutation APIs. May be NULL.
 *   From inside on_lane_pump/on_activity, the BLOCKING teardown calls
 *   stop()/join()/wait() are refused with MOQ_ERR_WRONG_STATE (they would
 *   wait on the very lane the callback holds). stop_begin() is NOT blocking
 *   — it only latches teardown and signals the coordinator — so it is legal
 *   from ANY thread INCLUDING a pump/activity callback. The terminal
 *   accessors and lane_wake() detect the callback context and are safe from
 *   EITHER callback. The per-connection accessors, by contrast, are legal
 *   ONLY inside the owning lane's on_lane_pump — never from on_activity — see
 *   HANDLE CONFINEMENT below.
 *
 * Version: DEFERRED multi-version. Unlike raw-MsQuic's single MoQ ALPN,
 * this facade offers a list of WebTransport subprotocols (cfg.wt_protocols,
 * preference order — default draft-18 then draft-16) and the peer selects
 * one during the WT handshake. The per-connection MoQ session is therefore
 * created LAZILY at the negotiated version when establishment completes;
 * moq_wtquic_msquic_managed_negotiated_version reports 0 until then.
 *
 * Client mode: create() opens the environment and starts the connect
 * holding the lane guard, so the returned session handle is published
 * before any callback runs. Server mode: create() starts the listener
 * (port 0 = ephemeral; _port() reports the bound port). Each accepted
 * connection is admitted (facade child reserved on a lane, enforcing
 * cfg.max_connections) BEFORE any wtquic session exists; its MoQ session
 * is created lazily on establishment.
 *
 * Teardown: a teardown COORDINATOR is created at create(), so initiating
 * teardown never fails and never blocks. stop_begin() only latches the
 * facade stop state and signals that coordinator, which runs the ordered
 * shutdown off the caller's thread: refuse new work; stop the listener and
 * JOIN the per-lane doorbell workers (so only the coordinator pumps from here
 * on); quiesce connections; run the transport barrier (env_close, NO lane guard
 * held); one explicit coordinator-owned post-barrier pump per lane so the app
 * sees each connection's final events; then guarded reap. join()/on_stopped
 * observe completion. stop() = stop_begin() + join() (blocking composition
 * for CLI/service hosts); destroy() after completion frees everything.
 */

#include <stddef.h>

#include <moq/wtquic.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_wtquic_msquic_managed moq_wtquic_msquic_managed_t;
/* A lane: an independent lock domain owning a set of connections. */
typedef struct moq_wtquic_msquic_managed_lane moq_wtquic_msquic_managed_lane_t;
/* An accepted connection (server) or the single client connection. */
typedef struct moq_wtquic_msquic_managed_conn moq_wtquic_msquic_managed_conn_t;

/* Information available at server accept, for lane placement. Sparse for
 * now (only struct_size); peer address / negotiated subprotocol land
 * later behind struct_size without breaking choose_lane. */
typedef struct moq_wtquic_msquic_accept_info {
    uint32_t struct_size;
} moq_wtquic_msquic_accept_info_t;

/* Server accept placement: return the lane index (0..lane_count-1) that
 * owns a newly accepted connection. Runs with NO session access; must not
 * touch sessions. Optional — NULL uses round-robin. An out-of-range return
 * REFUSES the connection (no silent clamp). */
typedef uint32_t (*moq_wtquic_msquic_choose_lane_fn)(
    moq_wtquic_msquic_managed_t *m,
    const moq_wtquic_msquic_accept_info_t *info, void *user);

/* A lane's exclusive access window. Iterate the lane's connections with
 * moq_wtquic_msquic_lane_next_conn and drive their sessions here. Return 0
 * to continue, nonzero for clean termination of the whole facade. */
typedef int (*moq_wtquic_msquic_lane_pump_fn)(
    moq_wtquic_msquic_managed_t *m, moq_wtquic_msquic_managed_lane_t *lane,
    uint64_t now_us, void *user);

typedef void (*moq_wtquic_msquic_activity_fn)(
    moq_wtquic_msquic_managed_t *m, void *ctx);

/* The maximum number of entries cfg.wt_protocols may offer. A larger
 * wt_protocol_count is rejected by create() with MOQ_ERR_INVAL. */
#define MOQ_WTQUIC_MSQUIC_MANAGED_MAX_WT_PROTOCOLS 8u

/*
 * OWNERSHIP: every pointer in this struct — host/cert_path/key_path/wt_path,
 * the wt_protocols array AND each protocol string it points at, and alloc —
 * is BORROWED for the duration of the create() call only. create() copies
 * whatever it retains, so the caller may free or reuse all of it as soon as
 * create() returns.
 */
typedef struct moq_wtquic_msquic_managed_cfg {
    uint32_t struct_size;
    const moq_alloc_t *alloc;      /* required; must be thread-safe */
    moq_perspective_t perspective; /* required: CLIENT or SERVER */
    const char *host;              /* client: remote + SNI (required);
                                      server: bind address
                                      (default 0.0.0.0) */
    uint16_t port;                 /* client: remote (required);
                                      server: listen, 0 = ephemeral */
    const char *cert_path;         /* server: required */
    const char *key_path;          /* server: required */
    bool insecure_skip_verify;     /* client: skip cert validation */
    uint32_t idle_timeout_ms;      /* QUIC transport idle; 0 = default */
    const char *wt_path;           /* WebTransport request path
                                      (default "/moq") */
    /* WebTransport subprotocols offered, in preference order. NULL/0 =
     * the facade default (draft-18 then draft-16). The peer selects one;
     * the MoQ session is created at the matching version. At most
     * MOQ_WTQUIC_MSQUIC_MANAGED_MAX_WT_PROTOCOLS entries: a larger count, or a
     * nonzero count with a NULL array, fails create() with MOQ_ERR_INVAL. */
    const char *const *wt_protocols;
    size_t wt_protocol_count;
    moq_wtquic_msquic_lane_pump_fn on_lane_pump; /* required */
    void *on_lane_pump_user;
    moq_wtquic_msquic_activity_fn on_activity;   /* optional, signal-only */
    void *on_activity_ctx;
    /* Actor-host teardown: fires once, on the stopping thread, after the
     * facade has fully torn down (mirrors join() completing). Invoking
     * on_stopped is the coordinator's LAST facade access — NO implementation
     * code reads or writes `m` after the call returns — so destroy() is legal
     * (and expected) directly inside the callback with no use-after-free. When
     * set, no other thread may join()/destroy() concurrently; call destroy()
     * from or after on_stopped. NULL = blocking (join) composition only. */
    void (*on_stopped)(void *ctx);
    void *on_stopped_ctx;
    /* session tuning (forwarded to each per-connection moq_session_cfg_t) */
    bool send_request_capacity;
    uint32_t initial_request_capacity; /* 0 = session default */
    uint32_t max_events;               /* 0 = session default */
    uint32_t max_actions;              /* 0 = session default */
    /* appended (enabled by struct_size): server connection cap
     * (facade-wide, across all lanes). 0 = 1. Ignored for clients. */
    uint32_t max_connections;
    /* appended: lane count. 0 = 1. Each lane is an independent lock domain +
     * doorbell. Clients always use exactly one lane. On a server the count is
     * capped to max_connections (no more lanes than connections it will
     * accept); otherwise the requested count is honored as-is (there is no
     * hidden cap — an amount too large to allocate fails create() cleanly). */
    uint32_t lane_count;
    moq_wtquic_msquic_choose_lane_fn choose_lane; /* NULL = round-robin */
    void *choose_lane_user;
    /* appended: receive-side rendering knob, forwarded to
     * moq_session_cfg_t.streaming_objects. false (default) = whole-object
     * OBJECT_RECEIVED; true = OBJECT_CHUNK slices. */
    bool streaming_objects;
    /* appended: MoQ SESSION idle timeout, forwarded to
     * moq_session_cfg_t.idle_timeout_us (0 = disabled). Distinct from
     * idle_timeout_ms (the QUIC transport idle). */
    uint64_t session_idle_timeout_us;
    /* appended: the WebTransport-over-HTTP/3 wire profile this facade speaks on
     * BOTH paths -- the client extended CONNECT and the server listener. The
     * two profiles are mutually exclusive and never auto-negotiated:
     *   0 = MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT (default; ":protocol =
     *       webtransport-h3", the current WebTransport-H3 draft), and
     *   1 = MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT (":protocol =
     *       webtransport" + the drafts-13/14 max-sessions signal, what
     *       proxygen/moxygen/moqx and the picoquic h3zero family speak).
     * Maps 1:1 onto wtq_webtransport_profile_t; an out-of-range value is
     * rejected by create(). */
    uint32_t webtransport_profile;

    /* appended: optional application service-deadline query — ONE ABI block,
     * read only when struct_size covers THROUGH app_deadline_ctx (the callback
     * and its context are honored together or not at all). Returns the app's
     * earliest pending time-based deadline in this facade's clock domain
     * (CLOCK_MONOTONIC µs), or UINT64_MAX for none. Each lane pump folds it with
     * the min session deadline it publishes, so a purely time-based deadline
     * (e.g. a periodic catalog refresh) wakes an otherwise-idle lane. MUST be
     * pure/non-blocking and MUST NOT re-enter the facade. NULL = none. The fold
     * happens only while a pump recomputes the lane deadline, so when
     * application state changes the value this query would return, the owner
     * MUST issue a facade/lane wake (moq_wtquic_msquic_managed_wake /
     * moq_wtquic_msquic_lane_wake) so the next pump recomputes the deadline; an
     * idle lane will not observe the new deadline on its own. Set via
     * moq_wtquic_msquic_managed_cfg_init_sized. */
    uint64_t (*app_deadline_us)(void *ctx);
    void *app_deadline_ctx;
} moq_wtquic_msquic_managed_cfg_t;

/* The wire profile carried by moq_wtquic_msquic_managed_cfg_t.webtransport_profile
 * (values mirror wtq_webtransport_profile_t). */
typedef enum moq_wtquic_msquic_wt_profile {
    MOQ_WTQUIC_MSQUIC_WT_PROFILE_CURRENT = 0,       /* :protocol = webtransport-h3 */
    MOQ_WTQUIC_MSQUIC_WT_PROFILE_D13_14_COMPAT = 1  /* :protocol = webtransport */
} moq_wtquic_msquic_wt_profile_t;

/* Frozen v0 config floor: the smallest cfg struct_size create() accepts. The
 * required prefix runs through on_activity_ctx; every field past it is optional
 * and gated field-by-field. This floor never moves, so an old caller passing
 * the v0 size keeps working against a newer library. Single source of truth —
 * create() and the ABI tests both use it. */
#define MOQ_WTQUIC_MSQUIC_MANAGED_CFG_V0_SIZE                            \
    (offsetof(moq_wtquic_msquic_managed_cfg_t, on_activity_ctx) +        \
     sizeof(((moq_wtquic_msquic_managed_cfg_t *)0)->on_activity_ctx))

MOQ_API void moq_wtquic_msquic_managed_cfg_init_sized(
    moq_wtquic_msquic_managed_cfg_t *cfg, size_t size);

MOQ_API moq_result_t moq_wtquic_msquic_managed_create(
    const moq_wtquic_msquic_managed_cfg_t *cfg,
    moq_wtquic_msquic_managed_t **out);

/* -- Teardown ------------------------------------------------------- */

/* Latch teardown and signal the coordinator (created at create()), which
 * runs the ordered shutdown off this thread. NONBLOCKING and legal from ANY
 * thread INCLUDING a pump/activity callback — it never touches a lane guard
 * or waits. Initiation cannot fail: returns true if THIS call started the
 * teardown, false if teardown was already in progress (idempotent) — there
 * is no error return. With cfg.on_stopped set, completion is delivered
 * there; otherwise observe it with join(). */
MOQ_API bool moq_wtquic_msquic_managed_stop_begin(
    moq_wtquic_msquic_managed_t *m);

/* Block until a stop_begin()-initiated teardown has fully completed.
 * MOQ_OK on completion, MOQ_ERR_WRONG_STATE from inside a callback or when
 * cfg.on_stopped owns completion. */
MOQ_API moq_result_t moq_wtquic_msquic_managed_join(
    moq_wtquic_msquic_managed_t *m);

/* Blocking composition (stop_begin + join) for CLI/service hosts.
 * Refused with MOQ_ERR_WRONG_STATE from inside a callback. Idempotent. */
MOQ_API moq_result_t moq_wtquic_msquic_managed_stop(
    moq_wtquic_msquic_managed_t *m);

/* After stop()/join()/on_stopped. */
MOQ_API void moq_wtquic_msquic_managed_destroy(
    moq_wtquic_msquic_managed_t *m);

/* CLIENT ONLY: the single client connection's session, valid ONLY inside
 * on_lane_pump. NULL before establishment and ALWAYS NULL on a server. */
MOQ_API moq_session_t *moq_wtquic_msquic_managed_session(
    moq_wtquic_msquic_managed_t *m);
/* CLIENT ONLY: the single client connection's attach adapter (pump-only). */
MOQ_API moq_wtquic_conn_t *moq_wtquic_msquic_managed_adapter(
    moq_wtquic_msquic_managed_t *m);

/* -- Lanes ---------------------------------------------------------- */

MOQ_API uint32_t moq_wtquic_msquic_managed_lane_count(
    const moq_wtquic_msquic_managed_t *m);
MOQ_API moq_wtquic_msquic_managed_lane_t *moq_wtquic_msquic_managed_lane(
    moq_wtquic_msquic_managed_t *m, uint32_t index);
MOQ_API uint32_t moq_wtquic_msquic_lane_index(
    const moq_wtquic_msquic_managed_lane_t *lane);

/* Iterate the connections OWNED BY THIS LANE: prev = NULL starts, returns
 * NULL past the end. ONLY valid inside THIS lane's on_lane_pump. A
 * connection whose transport reached its terminal remains iterable for the
 * pump pass that delivers its final events, then is reaped. */
MOQ_API moq_wtquic_msquic_managed_conn_t *moq_wtquic_msquic_lane_next_conn(
    moq_wtquic_msquic_managed_lane_t *lane,
    moq_wtquic_msquic_managed_conn_t *prev);

/* Schedule on_lane_pump on the lane's doorbell. Safe from any thread,
 * including inside a lane pump (same-lane arms without re-entry — never
 * reentrant). Does not grant session access outside the callback.
 * Stop-race contract:
 *   - MOQ_ERR_CLOSED once facade teardown (stop_begin) has been observed; an
 *     individual connection reaching its terminal does NOT close the lane, so
 *     this is defined solely by facade/lane teardown, not connection state;
 *   - MOQ_OK with no concurrent teardown guarantees a later coalesced pump;
 *   - an MOQ_OK racing stop_begin() may be ABSORBED by teardown (no pump);
 *   - nothing runs after on_stopped/join has completed. */
MOQ_API moq_result_t moq_wtquic_msquic_lane_wake(
    moq_wtquic_msquic_managed_lane_t *lane);

/* Per-lane doorbell statistics: monotonic counters over the lane's
 * wake -> pump -> service path since create. The getter fills at most the
 * out_size prefix the caller passes and zeroes the caller's remainder; the
 * caller sizes the read with the out_size argument, not this field, which the
 * getter treats as output (it stamps it with the bytes written). See
 * moq_msquic_lane_stats for the cycle rules this mirrors. */
typedef struct moq_wtquic_msquic_lane_stats {
    uint32_t struct_size;          /* OUTPUT: stamped by the call with bytes written */
    uint64_t wakes_same_lane;      /* arm from inside THIS lane's own pump */
    uint64_t wakes_cross_lane;     /* arm from a DIFFERENT lane's pump */
    uint64_t wakes_external;       /* arm from an external thread / transport */
    uint64_t wakes_coalesced;      /* arm folded into an unconsumed prior arm */
    uint64_t pump_sweeps;          /* total pump passes */
    uint64_t deadline_sweeps;      /* pump passes triggered by a session deadline */
    uint64_t idle_cap_wakes;       /* ALWAYS 0: this worker has no idle-cap (max
                                    * time between pumps) wake mechanism */
    uint64_t wake_to_pump_max_us;  /* arm -> pump latency (arm-driven pumps only) */
    uint64_t wake_to_pump_total_us;
    uint64_t wake_to_pump_samples;
    uint64_t service_passes;       /* per-connection adapter service invocations */
    uint64_t flush_sends;          /* ALWAYS 0: moq_wtquic_conn_service reports no
                                    * per-flush wire counts; would need bridge
                                    * instrumentation to populate */
    uint64_t flush_bytes;          /* ALWAYS 0: see flush_sends */
} moq_wtquic_msquic_lane_stats_t;

/* Frozen v0 layout floor: the smallest out_size the getter accepts. This floor
 * never moves — a frozen v0 layout mirror plus per-field offset asserts in the
 * implementation pin it, so a counter added or reordered before flush_bytes is a
 * build break, not a silent floor shift. A new counter is appended AFTER
 * flush_bytes; old callers passing the v0 size keep working. */
#define MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE \
    (offsetof(moq_wtquic_msquic_lane_stats_t, flush_bytes) + sizeof(uint64_t))

/* Snapshot one lane's stats into *out (sized-copy by out_size, stamping
 * out->struct_size with the bytes written). The read is taken under the lane's
 * leaf doorbell lock — the same lock every counter write holds — so the
 * snapshot is always internally consistent and cannot invert against the lane
 * guard, whether the caller is this lane's own callback (on_lane_pump / a
 * transport-event guard on this lane) or an external/coordinator thread. Only
 * one caller context is REFUSED: a DIFFERENT lane's callback on this thread
 * gets MOQ_ERR_WRONG_STATE (mirrors moq_wtquic_msquic_lane_wake — read from
 * your own lane or an external thread instead).
 * out_size, not out->struct_size, sizes the copy; struct_size is output only.
 * Returns MOQ_OK, or MOQ_ERR_INVAL for a NULL lane/out or an out_size below
 * MOQ_WTQUIC_MSQUIC_LANE_STATS_V0_SIZE. Counters are monotonic from create;
 * take deltas by subtraction. Valid until destroy(). */
MOQ_API moq_result_t moq_wtquic_msquic_lane_get_stats(
    moq_wtquic_msquic_managed_lane_t *lane,
    moq_wtquic_msquic_lane_stats_t *out, size_t out_size);

/* -- Connections ---------------------------------------------------- */

/*
 * HANDLE CONFINEMENT (normative): the ONLY source of a
 * moq_wtquic_msquic_managed_conn_t handle is moq_wtquic_msquic_lane_next_conn
 * — client code obtains its single connection through it as well (the
 * managed_conn_* functions consume a handle, they do not hand one out). Such
 * a handle is valid ONLY during the owning lane's
 * on_lane_pump in which it was obtained, and EVERY moq_wtquic_msquic_managed_
 * conn_* function below (including managed_conn_close) may be called ONLY from
 * inside that lane's pump.
 *
 * The convenience pointers reached through a conn — its moq_session_t
 * (conn_session / managed_session) and its moq_wtquic_conn_t attach adapter
 * (conn_adapter / managed_adapter) — are likewise BORROWED and pump-confined:
 * they are valid only for that same pump and must not be retained. Note
 * managed_adapter() returns the attach adapter (a moq_wtquic_conn_t), NOT a
 * managed-connection handle.
 *
 * WHEN A TERMINAL CONNECTION IS RECLAIMED depends on whether it ever had a MoQ
 * session:
 *   - a SESSION-BACKED server child stays linked and iterable -- possibly
 *     across many later pumps -- until the application observes its
 *     MOQ_EVENT_SESSION_CLOSED and then acknowledges it from an owning pump
 *     callback (moq_wtquic_msquic_managed_conn_ack_terminal). Reclamation may
 *     happen as soon as that acknowledging callback returns;
 *   - a PRE-SESSION child (negotiation or setup failed before any session was
 *     created) has no terminal event to observe and nothing to acknowledge; it
 *     is reclaimed once its transport is terminal and quiesced.
 * Either way its memory may then be reused: DO NOT retain a conn handle, its
 * session, or its adapter across pumps, and never touch a connection from
 * another lane's pump or an external thread. Acknowledgment does not change
 * that -- the handles stay borrowed and pump-confined in every case. Per-connection
 * state that must outlive the connection belongs elsewhere (keyed by your own
 * id, not the handle address). The facade-level observations that ARE safe
 * from any thread are called out individually below (conn_count, drain, port,
 * wake/wait, and the terminal accessors).
 */

MOQ_API moq_session_t *moq_wtquic_msquic_managed_conn_session(
    moq_wtquic_msquic_managed_conn_t *conn);
MOQ_API moq_wtquic_conn_t *moq_wtquic_msquic_managed_conn_adapter(
    moq_wtquic_msquic_managed_conn_t *conn);
MOQ_API moq_wtquic_msquic_managed_lane_t *moq_wtquic_msquic_managed_conn_lane(
    moq_wtquic_msquic_managed_conn_t *conn);

/* The MoQ version negotiated for THIS connection (0 before its WT handshake
 * selects a subprotocol and the MoQ session is created). Pump-window-only,
 * like every conn accessor. This is the correct per-connection version on a
 * server, where the facade-wide accessor returns 0. */
MOQ_API moq_version_t moq_wtquic_msquic_managed_conn_negotiated_version(
    moq_wtquic_msquic_managed_conn_t *conn);

/* Opaque per-connection application slot (the facade never reads it).
 * Session/connection POINTER VALUES are not stable identities across a
 * connection's life and its successor's — per-connection app state belongs
 * here, not in address-keyed maps. */
MOQ_API void moq_wtquic_msquic_managed_conn_set_user(
    moq_wtquic_msquic_managed_conn_t *conn, void *user);
MOQ_API void *moq_wtquic_msquic_managed_conn_user(
    const moq_wtquic_msquic_managed_conn_t *conn);

/*
 * Acknowledge a terminal SERVER child, so it may be reclaimed.
 *
 * THE ASSERTION. By calling this the application asserts that its terminal
 * processing for this connection is COMPLETE, that the child no longer needs to
 * remain iterable, and that it will not use the borrowed conn/session/adapter
 * handles after the acknowledging callback returns. Independently owned
 * application state and any moq_rcbuf_t the application retained are
 * UNAFFECTED: nothing reference-counted is released by this call, and the
 * handles above are borrowed views owned by the facade, not possessions. The
 * assertion is not mechanically verifiable — the library records that it was
 * made.
 *
 * WHEN IT MAY BE CALLED. Only from the owning lane's on_lane_pump, and only for
 * a connection that callback was presented. The terminal event must ALREADY
 * have been transferred to the application by a poll of this connection's
 * session (MOQ_EVENT_SESSION_CLOSED): a queued-but-unpolled terminal is not
 * enough, and neither is a completed transport shutdown. Because observation is
 * a durable session fact rather than a queue read, the acknowledging callback
 * need NOT be the one that polled — a later pump is fine.
 *
 * RESULTS.
 *   MOQ_OK               accepted; also returned for a duplicate call while the
 *                        handle is still valid in the current callback.
 *   MOQ_ERR_INVAL        conn == NULL.
 *   MOQ_ERR_WRONG_STATE  the single client connection (it is not reaped
 *                        per-child); a different lane's callback; outside any
 *                        callback; or the terminal has not been observed yet.
 *
 * HANDLE LIFETIME. The handle is valid only for the current callback. After a
 * successful acknowledgment the connection may be reclaimed as soon as that
 * callback returns — do not retain the pointer. A stale or released handle is
 * invalid input: no rejection is promised and none is attempted.
 *
 * IF IT IS NEVER CALLED. A SESSION-BACKED terminal server child stays linked,
 * iterable and counted against cfg.max_connections. It is never reclaimed
 * because time passed, so once the reserve is exhausted new accepts are
 * refused. A child whose negotiation or setup failed BEFORE any MoQ session was
 * created is different: it has no terminal event to observe and nothing to
 * acknowledge, and is reclaimed on native terminal plus quiescence alone. Forced
 * facade teardown (stop()/destroy()) may still reclaim it without
 * acknowledgment, behind the documented callback/pump exclusion barrier; that
 * is the forced path, not the normal one.
 */
MOQ_API moq_result_t moq_wtquic_msquic_managed_conn_ack_terminal(
    moq_wtquic_msquic_managed_conn_t *conn);

/* Shut one connection's transport down (async, orderly); code is the
 * application close code. uint32_t — the WebTransport/wtquic close code is
 * 32-bit (wtq_session_close), so exposing the exact width avoids a silent
 * truncation this fire-and-forget call could not report. Its final events
 * arrive on a later pump. */
MOQ_API void moq_wtquic_msquic_managed_conn_close(
    moq_wtquic_msquic_managed_conn_t *conn, uint32_t code);

/* Live connections (safe from any thread). */
MOQ_API size_t moq_wtquic_msquic_managed_conn_count(
    const moq_wtquic_msquic_managed_t *m);

/* Graceful drain: refuse new connections; existing ones run to their own
 * conclusion. Safe from any thread, idempotent. */
MOQ_API void moq_wtquic_msquic_managed_drain(
    moq_wtquic_msquic_managed_t *m);

/* Server: the bound listen port (after create). */
MOQ_API uint16_t moq_wtquic_msquic_managed_port(
    const moq_wtquic_msquic_managed_t *m);

/* Coalesced cross-thread signaling: wake() schedules a pump on EVERY lane.
 * Safe from any thread including a callback; never reentrant. Same stop-race
 * contract as moq_wtquic_msquic_lane_wake: MOQ_ERR_CLOSED once facade teardown
 * (stop_begin) has been observed — never from an individual connection's
 * terminal; MOQ_OK with no concurrent teardown guarantees a later coalesced
 * pump; an MOQ_OK racing stop_begin() may be absorbed; nothing runs after
 * on_stopped/join. wait() blocks until activity (MOQ_OK), timeout
 * (MOQ_DONE; 0 = poll, UINT64_MAX = indefinite), or a terminal state
 * (MOQ_ERR_CLOSED); it is refused from inside a callback (MOQ_ERR_WRONG_STATE). */
MOQ_API moq_result_t moq_wtquic_msquic_managed_wake(
    moq_wtquic_msquic_managed_t *m);
MOQ_API moq_result_t moq_wtquic_msquic_managed_wait(
    moq_wtquic_msquic_managed_t *m, uint64_t timeout_us);

/* Terminal observation — CLIENT convenience (the single connection). On a
 * SERVER these are NOT a per-connection correctness API (poll SESSION_CLOSED
 * in the lane pump). Safe from any thread. Fatal wins over closed. */
MOQ_API bool moq_wtquic_msquic_managed_is_fatal(
    const moq_wtquic_msquic_managed_t *m);
MOQ_API uint64_t moq_wtquic_msquic_managed_fatal_code(
    const moq_wtquic_msquic_managed_t *m);
MOQ_API bool moq_wtquic_msquic_managed_is_closed(
    const moq_wtquic_msquic_managed_t *m);
MOQ_API uint64_t moq_wtquic_msquic_managed_close_code(
    const moq_wtquic_msquic_managed_t *m);

/* CLIENT ONLY: the negotiated MoQ version of the single client connection.
 * DEFERRED — 0 until the WT handshake selects a subprotocol and the MoQ
 * session is created. ALWAYS 0 on a SERVER: a multi-version, concurrent
 * server has no stable facade-wide negotiated version, so query each
 * connection with moq_wtquic_msquic_managed_conn_negotiated_version inside
 * its lane pump instead. Safe from any thread. */
MOQ_API moq_version_t moq_wtquic_msquic_managed_negotiated_version(
    const moq_wtquic_msquic_managed_t *m);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_WTQUIC_MSQUIC_MANAGED_H */
