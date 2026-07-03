#ifndef MOQ_PICOQUIC_THREADED_H
#define MOQ_PICOQUIC_THREADED_H

/*
 * Stability: pre-1.0, transport-specific adapter API. Mature enough for
 * integration pilots, but may change before 1.0.
 *
 * moq-adapter-picoquic-threaded: optional helper that wraps picoquic's
 * network-thread API with MoQ session/adapter lifecycle management.
 *
 * Threading model:
 *   The helper spawns one picoquic network thread. All moq_session_*,
 *   moq_pq_*, moq_pub_*, and moq_sub_* calls must happen exclusively
 *   on that thread, inside the on_pump callback. The app thread
 *   communicates via application-level queues and uses wake/wait for
 *   cross-thread signaling.
 *
 * Callback rules:
 *   on_pump:     runs on network thread. May call any session/adapter/
 *                facade API. Return 0 to continue, nonzero to stop.
 *   on_activity: runs on network thread after each pump cycle.
 *                Signal-only — must NOT call mutation APIs.
 *   Neither callback may call moq_pq_threaded_stop.
 *
 * Client mode:
 *   _create builds picoquic context, client connection, MoQ session,
 *   and adapter, then starts the network thread. TLS handshake
 *   completes asynchronously. With no ALPN offer list (legacy single-
 *   version draft-16), _session() and _conn() are non-NULL immediately
 *   after _create returns. With cfg.alpn_list set, session creation is
 *   DEFERRED until the handshake negotiates a version: _session() and
 *   _conn() return NULL until then (a failed negotiation is terminal
 *   fatal and they stay NULL). See the alpn_list field below.
 *
 * Server mode:
 *   _create builds picoquic context and starts listening. A per-connection
 *   session and adapter are created lazily on the network thread as each client
 *   connects, up to cfg.max_connections (default 1024); a connection past the
 *   cap is rejected. Before the first client, on_pump is not called and the
 *   legacy _session()/_conn() return NULL. Iterate connections with
 *   moq_pq_threaded_next_conn() (per-connection API below); the legacy
 *   _session()/_conn() return the FIRST live connection for single-connection
 *   convenience.
 *
 * Build: requires MOQ_BUILD_PQ_THREADED=ON and
 *   MOQ_BUILD_ADAPTER_PICOQUIC=ON.
 *
 * Packaging (cold consumers): installed as CMake component
 *   adapter-picoquic-threaded —
 *     find_package(libmoq COMPONENTS adapter-picoquic-threaded)
 *     target_link_libraries(app PRIVATE moq::adapter-picoquic-threaded)
 *   That component transitively provides moq::adapter-picoquic, which
 *   carries the certificate-verifier helper (<moq/picoquic_verify.h>).
 *   There is no standalone .pc for this adapter; pkg-config consumers
 *   use the single `libmoq` package (it links the threaded + base
 *   picoquic adapters and the verifier helper):
 *     pkg-config --cflags --libs libmoq
 *
 * Certificate policy: see insecure_skip_verify / configure_quic below
 *   and <moq/picoquic_verify.h>. The default is NOT production-safe.
 *
 * See adapters/picoquic/THREADING_DESIGN.md for the full design.
 */

#include <moq/picoquic.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_pq_threaded moq_pq_threaded_t;

/* -- Configuration ------------------------------------------------ */

typedef struct moq_pq_threaded_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;          /* required; must have alloc + realloc + free */

    /* TLS — server requires both; client may omit. */
    const char        *cert_path;
    const char        *key_path;

    /* Network */
    moq_perspective_t  perspective;    /* CLIENT or SERVER; required */
    const char        *host;           /* client: remote host; required.
                                          Also used as the TLS SNI and the
                                          server name a verifier checks the
                                          peer cert against — it must match
                                          the certificate for verification
                                          to pass. */
    int                port;           /* 1..65535; required */

    /* Session tuning (zero/false = default).
     * initial_request_capacity is inert unless send_request_capacity
     * is true. */
    bool               send_request_capacity;
    uint64_t           initial_request_capacity;   /* default 64 */
    uint32_t           max_actions;                /* default 64 */
    uint32_t           max_events;                 /* default 16 */
    uint32_t           max_data_streams;            /* default 64 */
    uint32_t           max_subscriptions;           /* default 64 */
    uint32_t           send_buffer_size;            /* default 4096 */
    uint32_t           recv_buffer_size;            /* default 4096 */

    /* TLS verification.  If true, calls picoquic_set_null_verifier —
     * suitable for demos and tests only.  Default false does NOT install
     * any verifier; picoquic's built-in default has no CA store and
     * accepts the peer cert, so it is NOT production-safe.  For production
     * install a real verifier via configure_quic — the supported helper is
     * moq_picoquic_set_cert_verifier() in <moq/picoquic_verify.h>. */
    bool               insecure_skip_verify;

    /* Optional: called during _create after picoquic_create but
     * before any connections or the network thread starts.  The app
     * may configure TLS settings, certificate verification, token
     * stores, or any other picoquic_quic_t options.  The supported way
     * to enable production cert verification here is
     * moq_picoquic_set_cert_verifier() (<moq/picoquic_verify.h>).
     * Return 0 to continue, nonzero to abort _create. */
    int              (*configure_quic)(picoquic_quic_t *quic, void *ctx);
    void              *configure_quic_ctx;

    /* App pump callback (required).  Called on the network thread
     * between moq_pq_service calls.  The app ticks its facade,
     * publishes objects, polls received objects, and processes
     * queued work here.  All session/adapter/facade calls are safe.
     *
     * For server mode, on_pump is NOT called until the first inbound
     * connection has been accepted. Normally once per loop iteration;
     * when a connection turns terminal in the post-pump service pass,
     * ONE extra sequential invocation runs in the same iteration so the
     * dead connection is observable before it is pruned (the terminal-
     * conn observation window; see moq_pq_threaded_next_conn). Never
     * re-entered.
     *
     * Return 0 to continue.  Nonzero requests clean loop termination
     * (pump_exit state — _wait returns MOQ_ERR_CLOSED, _is_fatal
     * returns false). */
    int              (*on_pump)(moq_pq_threaded_t *t,
                                uint64_t now_us, void *ctx);
    void              *on_pump_ctx;

    /* Optional: called on network thread after each pump cycle.
     * Signal-only — do NOT call session/adapter/facade APIs.
     * May be NULL. */
    void             (*on_activity)(moq_pq_threaded_t *t, void *ctx);
    void              *on_activity_ctx;

    /* Appended (struct_size append-only ABI) — version negotiation.
     *
     * sni: TLS server name (SNI + the name a cert verifier checks).
     * NULL = use `host` (the previous behavior). Lets a caller connect by
     * address while verifying a hostname.
     *
     * alpn_list/alpn_count: the MoQ-over-QUIC ALPN offer list
     * ("moqt-18", "moqt-16", ...), in PREFERENCE ORDER (the client's order
     * decides when the peer supports several). Strings are copied.
     *
     * When alpn_count > 0:
     *   CLIENT — the connection offers the whole list and the MoQ session
     *   is created only once the handshake has negotiated an ALPN, with
     *   moq_session_cfg_t.version set to match (guide §13 readback);
     *   _session() returns NULL until then, and on_pump may run before the
     *   session exists. A negotiated ALPN that is unknown or outside the
     *   offered set fails the connection before any SETUP byte is parsed
     *   (is_fatal()). SERVER — entry 0 is the default ALPN and a selector
     *   picks the first CLIENT-offered entry this list contains; the
     *   accepted session's version follows the negotiated ALPN.
     * When alpn_count == 0: the legacy single draft-16 ALPN and (client)
     * the eager at-create session, unchanged. */
    const char        *sni;
    const char *const *alpn_list;
    size_t             alpn_count;

    /* Appended (struct_size append-only ABI) — session close tuning.
     *
     * goaway_timeout_us: GOAWAY drain timeout in microseconds, forwarded to
     * moq_session_cfg_t.goaway_timeout_us. After moq_session_goaway() the
     * session drains for this long, then force-closes the transport (a clean
     * CONNECTION_CLOSE the peer observes promptly). 0 = disabled (drain
     * forever) — the previous behavior. Mirrors the knob the pico_wt and mvfst
     * managed facades already expose. */
    uint64_t           goaway_timeout_us;

    /* Appended (struct_size append-only ABI) — server connection cap.
     *
     * max_connections: SERVER mode only. The maximum number of simultaneous
     * accepted client connections; a new connection past the cap is rejected
     * at the transport (picoquic closes it). 0 = the library default (1024),
     * mirroring the mvfst managed server. Inert in client mode. */
    uint32_t           max_connections;

    /* Appended (struct_size append-only ABI) — QUIC idle timeout.
     *
     * idle_timeout_ms: QUIC idle timeout in milliseconds. 0 = library
     * default (picoquic's default, currently about 30,000 ms). Bounds how
     * long a peer that vanished WITHOUT a CONNECTION_CLOSE can remain
     * before picoquic reaps the connection and the adapter surfaces
     * MOQ_EVENT_SESSION_CLOSED (see the terminal-conn observation window
     * at moq_pq_threaded_next_conn). Defense-in-depth for crashed/vanished
     * peers only -- graceful clients should still close at the
     * MoQ/session/transport level. Shorter values trade faster dead-peer
     * cleanup against killing legitimately quiet connections; keepalive is
     * a separate concern and not implied. Applied to the QUIC context
     * default before any connection exists; a configure_quic override runs
     * AFTER it and may change it. */
    uint32_t           idle_timeout_ms;
} moq_pq_threaded_cfg_t;

/* Pointer-only initializer. Clears and stamps ONLY the frozen prefix that
 * existed before goaway_timeout_us was appended (it cannot know the caller's
 * storage size, so it must not write the full current sizeof -- that would
 * overflow an old caller's smaller struct). Fields appended after the prefix
 * (currently goaway_timeout_us, max_connections, and idle_timeout_ms)
 * default to disabled/zero (max_connections 0 = the 1024 default,
 * idle_timeout_ms 0 = the picoquic default); sni/alpn_* predate the prefix
 * boundary and stay enabled. To use appended fields, or to get the full
 * current struct, use moq_pq_threaded_cfg_init_sized(). */
void moq_pq_threaded_cfg_init(moq_pq_threaded_cfg_t *cfg);

/* Size-aware initializer. Clears and stamps min(cfg_size, sizeof current
 * struct): an older caller passes its smaller sizeof (prefix init), a newer
 * caller's extra trailing fields are left to its own initializer. Pass
 * sizeof(moq_pq_threaded_cfg_t) to initialize the full current struct with all
 * appended fields enabled. No-op if cfg is NULL or cfg_size is too small to
 * hold struct_size. */
void moq_pq_threaded_cfg_init_sized(moq_pq_threaded_cfg_t *cfg, size_t cfg_size);

/* -- Lifecycle ---------------------------------------------------- */

/*
 * Create picoquic context and start the network thread.
 *
 * CLIENT: creates connection, session, adapter immediately. Returns
 * once the thread is running. TLS handshake completes asynchronously.
 *
 * SERVER: creates picoquic context and starts listening on cfg->port.
 * Session/adapter created lazily on first inbound connection.
 * _session() and _conn() return NULL until then.
 *
 * On failure, no thread is running; _stop / _destroy are not needed.
 * Returns MOQ_ERR_NOMEM on allocation failure, MOQ_ERR_INVAL on bad
 * config, MOQ_ERR_INTERNAL on picoquic/thread setup failure.
 */
moq_result_t moq_pq_threaded_create(const moq_pq_threaded_cfg_t *cfg,
                                     moq_pq_threaded_t **out);

/*
 * Stop the network thread and join it.
 *
 * Idempotent: second call returns MOQ_OK (or MOQ_ERR_CLOSED if fatal).
 * MUST NOT be called from on_pump or on_activity; returns
 * MOQ_ERR_WRONG_STATE if detected.
 *
 * After return, no more callbacks will fire. The caller must then
 * destroy its facade (if any) and call _destroy().
 */
moq_result_t moq_pq_threaded_stop(moq_pq_threaded_t *t);

/*
 * Destroy adapter, session, picoquic context, and wrapper.
 * Must be called after _stop(). The caller must destroy its own
 * facade before calling _destroy().
 */
void moq_pq_threaded_destroy(moq_pq_threaded_t *t);

/* -- Accessors ---------------------------------------------------- */

/*
 * Returns the MoQ session, or NULL if server mode and no connection
 * has been accepted yet.  Thread-safe (reads under mutex).
 * The returned pointer is owned by the helper — do not destroy it.
 *
 * LEGACY / single-connection convenience: in server mode this returns the
 * FIRST live connection's session. A multi-connection server MUST iterate with
 * moq_pq_threaded_next_conn() + moq_pq_threaded_conn_session() instead. In
 * client mode this is the client session (unchanged).
 */
moq_session_t *moq_pq_threaded_session(moq_pq_threaded_t *t);

/*
 * Returns the adapter connection, or NULL if server mode and no
 * connection has been accepted yet.  Thread-safe.
 *
 * LEGACY / single-connection convenience: server mode returns the FIRST live
 * connection's adapter object; multi-connection servers use next_conn().
 */
moq_pq_conn_t *moq_pq_threaded_conn(moq_pq_threaded_t *t);

/* -- Per-connection API (server multi-connection) ------------------- *
 * Mirrors the mvfst managed per-connection model. A moq_pq_threaded_conn_t is
 * an opaque handle to one accepted server connection. All per-connection calls
 * are valid ONLY on the network thread inside on_pump, for a handle observed in
 * the current live next_conn() iteration. */

typedef struct moq_pq_threaded_conn moq_pq_threaded_conn_t;

/*
 * Iterate accepted server connections. Pass prev=NULL to start; returns NULL
 * when done. Call on the network thread inside on_pump.
 *
 * Handle stability: a moq_pq_threaded_conn_t* is stable while the connection
 * remains active (heap-owned; unaffected by the internal list growing). Once a
 * handle no longer appears in iteration it is stale and must not be used.
 *
 * Terminal-connection lifecycle contract: a connection that becomes terminal
 * because the PEER/transport closed or a service/bridge failure occurred
 * REMAINS in iteration until an on_pump has run with it present -- so the
 * app can poll MOQ_EVENT_SESSION_CLOSED (and any other final events) from
 * its session before the adapter prunes and destroys them. When such a
 * death arises in the adapter's post-on_pump service pass, the adapter runs
 * ONE extra sequential on_pump in the same packet-loop iteration (on_pump
 * is never re-entered) and prunes only after it returns; the connection
 * never survives into a later loop iteration. App-requested closes
 * (moq_pq_threaded_conn_close) disappear after the current on_pump, as
 * before -- the app initiated those.
 */
moq_pq_threaded_conn_t *moq_pq_threaded_next_conn(
    moq_pq_threaded_t *t, moq_pq_threaded_conn_t *prev);

/*
 * The MoQ session for a server connection (NULL if the handle is invalid).
 * Valid only inside on_pump for a live handle. Owned by the helper.
 */
moq_session_t *moq_pq_threaded_conn_session(
    moq_pq_threaded_conn_t *conn);

/*
 * Close a single server connection. May be called inside on_pump. The close is
 * deferred until after on_pump returns, so next_conn() iteration stays valid;
 * the connection then disappears from iteration. Returns MOQ_OK, or
 * MOQ_ERR_INVAL for a NULL handle.
 */
moq_result_t moq_pq_threaded_conn_close(
    moq_pq_threaded_conn_t *conn, uint64_t error_code);

/*
 * Number of active server connections. Safe from any thread.
 */
size_t moq_pq_threaded_conn_count(const moq_pq_threaded_t *t);

/* Fatal state.  Thread-safe.  In client mode, a transport disconnect
 * before the MoQ session reaches MOQ_SESS_ESTABLISHED (cert rejection,
 * connection refused, dead peer) latches fatal: is_fatal() becomes true,
 * fatal_code() is 0 for transport/handshake-level failures (otherwise the
 * bridge fatal code), and _wait() returns MOQ_ERR_CLOSED in bounded time.
 * So a failed cert verification is observable via is_fatal(). */
bool     moq_pq_threaded_is_fatal(const moq_pq_threaded_t *t);
uint64_t moq_pq_threaded_fatal_code(const moq_pq_threaded_t *t);

/* Graceful-drain probe (backs moq_endpoint_drain). 1 = local stream flush done
 * (no queued/ready stream data or unsent FIN; NOT a packet-ACK check), 0 = still
 * draining, -2 = called on the network thread. App-thread call. */
int      moq_pq_threaded_drain_state(moq_pq_threaded_t *t);

/* The negotiated MoQ version: 0 until known (deferred-offer client before
 * the handshake completes / server before a connection is accepted), then
 * the version the session was created with. Thread-safe. */
moq_version_t moq_pq_threaded_negotiated_version(const moq_pq_threaded_t *t);

/* -- Cross-thread signaling --------------------------------------- */

/*
 * Wake the network thread so it runs on_pump.
 *
 * Use when the app thread has queued work that on_pump should process.
 * Safe from any thread.  Coalesced: multiple calls before the thread
 * processes them collapse into one callback.
 *
 * Returns MOQ_OK on success (including coalesced).
 * Returns MOQ_ERR_CLOSED if stopped, fatal, or pump_exit.
 * Returns MOQ_ERR_INTERNAL on pipe/event failure.
 */
moq_result_t moq_pq_threaded_wake(moq_pq_threaded_t *t);

/*
 * Block until activity or timeout.
 *
 * Returns MOQ_OK if activity occurred (on_pump ran).
 * Returns MOQ_DONE if timeout elapsed with no activity.
 * Returns MOQ_ERR_CLOSED if stopped, fatal, or pump_exit.
 *
 * Uses a coalesced activity flag: if activity occurred before _wait,
 * returns MOQ_OK immediately.  No wakeups are lost.
 *
 * timeout_us == 0: non-blocking check.
 * timeout_us == UINT64_MAX: indefinite wait.
 */
moq_result_t moq_pq_threaded_wait(moq_pq_threaded_t *t,
                                    uint64_t timeout_us);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICOQUIC_THREADED_H */
