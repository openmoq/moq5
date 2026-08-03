#ifndef MOQ_MVFST_H
#define MOQ_MVFST_H

/*
 * Stability: pre-1.0, transport-specific adapter API. Mature enough for
 * integration pilots, but may change before 1.0.
 *
 * moq-adapter-mvfst: bridges moq-core sessions to mvfst QUIC transport.
 *
 * C API. Opaque handles only — no C++ types, no mvfst/folly/fizz types.
 *
 * Two modes:
 *
 *   Attach mode (C++ only, via <moq/mvfst.hpp>):
 *     Caller owns the QuicSocket and moq_session_t. The adapter
 *     bridges callbacks and actions.
 *
 *   Managed mode (C or C++, via this header):
 *     Adapter owns the network thread, session, and bridge.
 *     Pure C programs use this mode exclusively.
 *     moq_mvfst_managed_t is the entry point.
 *
 *     In client mode with a host set, create() spawns a network
 *     thread that creates the session, QuicClientTransport, and
 *     attach adapter. The pump loop drives the EventBase and
 *     adapter service. With host NULL/empty, the pump loop runs
 *     with session-only (lifecycle-only mode for tests).
 *
 *     In server mode, create() starts a QuicServer listener.
 *     Each accepted QUIC connection gets its own moq_session_t
 *     and adapter. The app iterates the lane's connections via
 *     moq_mvfst_lane_next_conn() inside on_lane_pump.
 *
 *     MoQ version / ALPN:
 *       The offered MoQ-over-QUIC ALPN is set via cfg.alpn_list /
 *       cfg.alpn_count (default: a single "moqt-16" = draft-16). This is
 *       an EXACT-VERSION adapter: it creates the MoQ session eagerly,
 *       before the TLS/ALPN handshake, so it cannot defer the session
 *       version to a multi-ALPN negotiation the way moq_pq_threaded does.
 *       A multi-entry offer (alpn_count > 1, i.e. AUTO) is rejected with
 *       MOQ_ERR_UNSUPPORTED. See cfg.alpn_list and
 *       moq_mvfst_managed_negotiated_version() below.
 *
 * Thread safety:
 *   wake() and wait() are safe from any thread.
 *   session() returns a pointer valid only inside on_lane_pump callbacks
 *     on the managed network thread. Do not call session APIs from
 *     the application thread.
 *   create/stop/destroy are called from the application thread.
 *   is_fatal/fatal_code are safe from any thread (atomic reads).
 *
 * Packaging: installed as CMake component adapter-mvfst —
 *     find_package(libmoq COMPONENTS adapter-mvfst)
 *     target_link_libraries(app PRIVATE moq::adapter-mvfst)
 *   Requires a C++ compiler and a discoverable mvfst CONFIG package.
 *   CMake-only by design: there is NO pkg-config (.pc) for mvfst, because
 *   its folly/fizz/wangle dependency stack ships CMake config packages,
 *   not .pc files. Meson/autotools consumers should drive the build via
 *   CMake. See docs/transport-integration-guide.md §9.
 */

#include <moq/types.h>
#include <moq/session.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -- Managed lifecycle ----------------------------------------------- */

typedef struct moq_mvfst_managed moq_mvfst_managed_t;

/*
 * A lane is an exclusive access window over a set of the facade's
 * connections. mvfst runs a single EventBase worker per managed facade,
 * so there is exactly ONE lane; lane_count > 1 is rejected at create()
 * with MOQ_ERR_UNSUPPORTED (true multi-lane would need multiple worker
 * EventBases — a separate re-architecture, and mvfst is not the fanout
 * target). The lane exists so the managed API matches the shared lane
 * shape across adapters.
 */
typedef struct moq_mvfst_managed_lane moq_mvfst_managed_lane_t;

/*
 * Lane pump callback: runs on the network thread, inside the lane's
 * exclusive access window, between adapter service calls. It services
 * ALL of the lane's connections in one pass — iterate them with
 * moq_mvfst_lane_next_conn(). The client's single session is available
 * via moq_mvfst_managed_session(); a server reaches its sessions through
 * lane iteration (moq_mvfst_managed_session() is NULL on a server).
 *
 * Return 0 to continue, nonzero to request clean shutdown.
 * Must NOT call moq_mvfst_managed_stop() from inside.
 */
typedef int (*moq_mvfst_lane_pump_fn)(moq_mvfst_managed_t *m,
                                       moq_mvfst_managed_lane_t *lane,
                                       uint64_t now_us,
                                       void *user);

/*
 * Activity callback: signal-only notification after each pump.
 * Runs on the network thread. Must NOT call mutation APIs or stop().
 * Typically used to wake a blocked application thread.
 */
typedef void (*moq_mvfst_activity_fn)(moq_mvfst_managed_t *m,
                                       void *ctx);

typedef struct moq_mvfst_managed_cfg {
    uint32_t              struct_size;
    const moq_alloc_t    *alloc;          /* NULL = default allocator */
    moq_perspective_t     perspective;    /* CLIENT or SERVER */

    const char           *host;           /* client: remote host; server: bind addr (NULL = all) */
    int                   port;           /* client: 1..65535; server: 0..65535 (0 = ephemeral) */

    /* TLS configuration.
     *
     * Client: insecure_skip_verify and cert_path are mutually exclusive.
     *   insecure_skip_verify=true → accept any cert (testing only)
     *   cert_path set            → chain + host/IP identity vs PEM CA
     *   neither                  → system trust store + host/IP identity
     *
     * Server: cert_path and key_path are both required.
     *   insecure_skip_verify must be false.
     *
     * create() returns MOQ_ERR_INVAL for invalid combinations.
     */
    const char           *cert_path;      /* client: PEM CA trust; server: PEM cert (required) */
    const char           *key_path;       /* server: PEM key (required); client: ignored */
    bool                  insecure_skip_verify; /* client only; rejected for server */

    /* Session tuning (0/false = library defaults). */
    bool                  send_request_capacity;
    uint64_t              initial_request_capacity;
    uint32_t              max_actions;
    uint32_t              max_events;
    uint32_t              max_subscriptions;
    uint32_t              max_data_streams;
    uint32_t              send_buffer_size;
    uint32_t              recv_buffer_size;

    /* Callbacks. */
    moq_mvfst_lane_pump_fn on_lane_pump;  /* required */
    moq_mvfst_activity_fn  on_activity;   /* optional, signal-only */
    void                  *user_ctx;      /* passed to both callbacks */

    /* Session deadline tuning (0 = disabled). */
    uint64_t              goaway_timeout_us;  /* GOAWAY drain timeout */

    /* Appended (struct_size append-only ABI) — TLS server name + MoQ version.
     *
     * sni: TLS server name (SNI + the name a cert verifier checks).
     *   NULL/empty = use `host` (the previous behavior). Lets a caller
     *   connect by address while verifying a hostname.
     *
     * alpn_list/alpn_count: the MoQ-over-QUIC ALPN offer ("moqt-16" /
     *   "moqt-18"), read only during create() (the strings need not outlive
     *   the call). EXACT-VERSION ONLY: this adapter creates the MoQ session
     *   eagerly, before the TLS/ALPN handshake, so it cannot defer the
     *   session version to a multi-ALPN negotiation the way moq_pq_threaded
     *   does.
     *     alpn_count == 0 → legacy single "moqt-16" (draft-16), unchanged.
     *     alpn_count == 1 → that ALPN; the session is created at the matching
     *       version and the single ALPN is offered (client) / advertised
     *       (server).
     *     alpn_count  > 1 → MOQ_ERR_UNSUPPORTED from create() (a real offer /
     *       AUTO needs deferred session creation — not yet implemented).
     *   An unrecognized / non-MoQ ALPN string → MOQ_ERR_UNSUPPORTED. */
    const char           *sni;
    const char *const    *alpn_list;
    size_t                alpn_count;

    /* Appended (struct_size append-only ABI) — server-mode connection cap.
     *
     * max_connections: the maximum number of accepted QUIC connections the
     *   managed server retains at once. Once reached, further inbound
     *   connections are rejected at accept time (no session/adapter is
     *   allocated for them), bounding memory and per-tick CPU against idle
     *   peers. 0 = library default (1024). Ignored in client mode. */
    size_t                max_connections;

    /* Appended (struct_size append-only ABI) — QUIC transport tuning.
     * These were originally placed before the callback pointers, which shifted
     * the public on_lane_pump/on_activity/user_ctx offsets; they were moved here
     * so the callback layout (and the frozen v0 prefix) stays stable. Read only
     * behind CFG_HAS, so a shorter caller leaves them at the library defaults.
     *
     * max_num_ptos:   max consecutive PTOs before close (0 = library default).
     * initial_rtt_us: initial RTT estimate in microseconds (0 = default). */
    uint16_t              max_num_ptos;
    uint32_t              initial_rtt_us;

    /* Appended (struct_size append-only ABI) — lane count.
     *
     * mvfst runs a single EventBase worker per managed facade, so it
     * supports exactly ONE lane. 0 or 1 = the single lane (the previous
     * behavior). Any value > 1 fails create() with MOQ_ERR_UNSUPPORTED —
     * the adapter does not fake concurrency behind one serialized domain.
     * This is enforced for client and server facades alike. */
    uint32_t              lane_count;

    /* Appended (struct_size append-only ABI) — optional application service-
     * deadline query, ONE ABI block read only when struct_size covers THROUGH
     * app_deadline_ctx (the callback and its context are honored together or not
     * at all). Returns the app's earliest pending time-based deadline as an
     * ABSOLUTE CLOCK_MONOTONIC microsecond value, or UINT64_MAX for none. The
     * EventBase pump folds it with the session deadlines and drives the one-shot
     * AsyncTimeout, so a purely time-based deadline (e.g. a periodic catalog
     * refresh) wakes an otherwise-idle facade. MUST be pure, non-blocking, and
     * non-reentrant (no re-entry into this facade). NULL = none. The fold happens
     * only inside a live pump, so when application state changes the value this
     * query would return, the owner MUST issue moq_mvfst_managed_wake() /
     * moq_mvfst_lane_wake() so the next pump recomputes the deadline; an idle
     * facade will not observe the new deadline on its own. Enabled through
     * moq_mvfst_managed_cfg_init_sized(). */
    uint64_t            (*app_deadline_us)(void *ctx);
    void                 *app_deadline_ctx;
} moq_mvfst_managed_cfg_t;

/* Pointer-only initializer. Clears and stamps ONLY the frozen v0 prefix (the
 * layout up to and including the callback pointers, before any appended field).
 * It cannot know the caller's storage size, so it must not write the full
 * current sizeof -- that would overflow a caller compiled against the original
 * (smaller) struct. Appended fields (goaway_timeout_us, sni/alpn_*,
 * max_connections, max_num_ptos, initial_rtt_us, lane_count, and the
 * app_deadline_us/app_deadline_ctx block) default to disabled; to set any of
 * them, or to initialize the full current struct, use
 * moq_mvfst_managed_cfg_init_sized(). */
MOQ_API void moq_mvfst_managed_cfg_init(moq_mvfst_managed_cfg_t *cfg);

/* Size-aware initializer. Clears and stamps min(cfg_size, sizeof current
 * struct): an older caller passes its smaller sizeof (prefix init), a newer
 * caller's extra trailing fields are left to its own initializer. Pass
 * sizeof(moq_mvfst_managed_cfg_t) to initialize the full current struct with
 * all appended fields enabled. No-op if cfg is NULL or cfg_size is too small to
 * hold struct_size. */
MOQ_API void moq_mvfst_managed_cfg_init_sized(moq_mvfst_managed_cfg_t *cfg,
                                              size_t cfg_size);

/*
 * Create a managed mvfst adapter.
 *
 * Client mode: spawns a network thread, creates the session and
 * QuicClientTransport, connects to cfg.host:cfg.port, attaches
 * the adapter. With host NULL/empty, runs session-only (no transport).
 *
 * Server mode: spawns a network thread, creates a QuicServer listener
 * bound to cfg.host:cfg.port (0 = ephemeral). Accepted connections
 * get per-connection sessions and adapters, iterable via moq_mvfst_lane_next_conn().
 *
 * create() blocks until init succeeds or fails. On success, the
 * pump loop is running.
 *
 * Returns MOQ_OK on success with *out set.
 * Returns MOQ_ERR_INVAL for missing/invalid config.
 * Returns MOQ_ERR_NOMEM on allocation failure.
 * Returns MOQ_ERR_INTERNAL on thread/session setup failure.
 *
 * On failure: no thread running; stop/destroy not needed.
 */
MOQ_API moq_result_t moq_mvfst_managed_create(
    const moq_mvfst_managed_cfg_t *cfg,
    moq_mvfst_managed_t **out);

/*
 * Request clean shutdown. Idempotent. Joins the network thread.
 * After stop returns, no callbacks will fire and the session is
 * destroyed.
 *
 * Must NOT be called from on_lane_pump or on_activity.
 * Returns MOQ_ERR_INVAL if called from the managed thread.
 */
MOQ_API moq_result_t moq_mvfst_managed_stop(moq_mvfst_managed_t *m);

/*
 * Destroy the managed handle and free all resources.
 * Calls stop() internally if not already stopped.
 * destroy(NULL) is a no-op.
 *
 * Must NOT be called from on_lane_pump or on_activity.
 */
MOQ_API void moq_mvfst_managed_destroy(moq_mvfst_managed_t *m);

/*
 * Access the underlying session (client mode only).
 * Returns non-NULL only when called from the managed network thread
 * (inside on_lane_pump). Returns NULL from any other thread, after stop,
 * before init completes, or in server mode.
 *
 * Server mode: iterate the lane via moq_mvfst_lane_next_conn() +
 * moq_mvfst_conn_session() instead. This is a CLIENT-only convenience,
 * not a server correctness path.
 */
MOQ_API moq_session_t *moq_mvfst_managed_session(moq_mvfst_managed_t *m);

/* -- Lanes ----------------------------------------------------------- */

typedef struct moq_mvfst_conn moq_mvfst_conn_t;

/* Number of lanes (always 1 for mvfst). */
MOQ_API uint32_t moq_mvfst_managed_lane_count(const moq_mvfst_managed_t *m);

/* The lane at `index` (only 0 is valid), or NULL if out of range. The
 * returned pointer is stable for the facade's lifetime. */
MOQ_API moq_mvfst_managed_lane_t *moq_mvfst_managed_lane(
    moq_mvfst_managed_t *m, uint32_t index);

/* The lane's index (0). */
MOQ_API uint32_t moq_mvfst_lane_index(const moq_mvfst_managed_lane_t *lane);

/*
 * Iterate the connections owned by this lane. Returns NULL when done.
 * Pass prev=NULL to start. ONLY valid inside this lane's on_lane_pump
 * (returns NULL otherwise) — the pump sees ALL of the lane's connections
 * in one pass.
 *
 * Handle stability: a moq_mvfst_conn_t* is stable while the connection
 * remains active. Apps may store it as an opaque identity key across
 * on_lane_pump calls. Conn API calls (conn_session, conn_close) are
 * valid only inside on_lane_pump for handles observed in the
 * current/live iteration. Once a handle no longer appears in iteration,
 * it is stale and must not be used.
 */
MOQ_API moq_mvfst_conn_t *moq_mvfst_lane_next_conn(
    moq_mvfst_managed_lane_t *lane,
    moq_mvfst_conn_t *prev);

/* The lane that owns this connection. Valid inside on_lane_pump. */
MOQ_API moq_mvfst_managed_lane_t *moq_mvfst_managed_conn_lane(
    moq_mvfst_conn_t *conn);

/* Ask the adapter to run this lane's pump. Safe from any thread;
 * coalesced. Same delivery as moq_mvfst_managed_wake() (one lane). */
MOQ_API moq_result_t moq_mvfst_lane_wake(moq_mvfst_managed_lane_t *lane);

/*
 * Get the session for a server connection.
 * Valid only inside on_lane_pump for a live connection handle.
 */
MOQ_API moq_session_t *moq_mvfst_conn_session(moq_mvfst_conn_t *conn);

/*
 * Close a single server connection. May be called inside on_lane_pump.
 * Actual cleanup is deferred until after the current on_lane_pump returns,
 * so iteration via moq_mvfst_lane_next_conn() is safe.
 *
 * Close/error lifecycle:
 *   Connections that are closed (by the app or peer) or that hit a
 *   transport/protocol error disappear from moq_mvfst_lane_next_conn() iteration
 *   after the current on_lane_pump returns. There is no separate close
 *   reason API — poll moq_session_poll_events_ex() for
 *   MOQ_EVENT_SESSION_CLOSED inside on_lane_pump to observe close
 *   causes before the connection is removed.
 *
 *   Applications needing detailed transport-level close callbacks
 *   should use the C++ attach API (moq/mvfst.hpp) instead.
 */
MOQ_API moq_result_t moq_mvfst_conn_close(moq_mvfst_conn_t *conn,
                                            uint64_t error_code);

/*
 * Number of active server connections. Safe from any thread.
 */
MOQ_API size_t moq_mvfst_managed_conn_count(const moq_mvfst_managed_t *m);

/* -- Server info ----------------------------------------------------- */

/*
 * Local bound port (useful when bind port is 0 for ephemeral).
 * Returns 0 if not applicable or not yet bound.
 */
MOQ_API uint16_t moq_mvfst_managed_local_port(const moq_mvfst_managed_t *m);

/* -- Cross-thread signaling ------------------------------------------ */

/*
 * Wake the network thread so on_lane_pump runs. Safe from any thread. Coalesced.
 * Returns MOQ_OK or MOQ_ERR_CLOSED if stopped, fatal, or pump exited.
 *
 * The pump is event-driven: transport-driven / reactive work (an inbound
 * SUBSCRIBE, a peer close, a write-ready, a session deadline) schedules on_lane_pump
 * automatically, so you do NOT need to poll or wake for it. But the pump no
 * longer runs on a fixed cadence -- so any APPLICATION-side state change that
 * on_lane_pump must act on (e.g. flagging "publish this now" or "accept this
 * subscribe" from your own thread) MUST call wake() to make on_lane_pump run and
 * observe it. An app that changes state without waking may never be serviced.
 */
MOQ_API moq_result_t moq_mvfst_managed_wake(moq_mvfst_managed_t *m);

/*
 * Wait for activity or timeout.
 * Returns MOQ_OK (activity), MOQ_DONE (timeout), MOQ_ERR_CLOSED.
 * timeout_us == 0: non-blocking check.
 * timeout_us == UINT64_MAX: indefinite wait.
 * Returns MOQ_ERR_CLOSED promptly after stop or natural pump exit.
 */
MOQ_API moq_result_t moq_mvfst_managed_wait(moq_mvfst_managed_t *m,
                                              uint64_t timeout_us);

/* -- Fatal state ----------------------------------------------------- */

/*
 * Check whether the managed adapter hit a fatal transport error.
 * Safe from any thread (atomic reads). Client mode: transport or
 * adapter failure. Server mode: listener-level failure only;
 * per-connection errors remove individual conns without going fatal.
 */
MOQ_API bool     moq_mvfst_managed_is_fatal(const moq_mvfst_managed_t *m);
MOQ_API uint64_t moq_mvfst_managed_fatal_code(const moq_mvfst_managed_t *m);

/* -- Negotiated MoQ version ------------------------------------------ */

/*
 * The MoQ version this adapter is running. EXACT-VERSION adapter: the
 * version configured via cfg.alpn_list (or draft-16 when alpn_count == 0).
 * Returns 0 until the MoQ session reaches ESTABLISHED — client before the
 * handshake/setup completes, or server before a connection is accepted —
 * then the fixed session version. Safe from any thread (atomic read).
 */
MOQ_API moq_version_t moq_mvfst_managed_negotiated_version(
    const moq_mvfst_managed_t *m);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MVFST_H */
