#ifndef MOQ_MSQUIC_H
#define MOQ_MSQUIC_H

/*
 * Stability: pre-1.0, transport-specific adapter API. May change
 * before 1.0.
 *
 * moq-adapter-msquic: bridges moq-core sessions to raw QUIC via
 * MsQuic. Direct MoQ-over-QUIC (ALPN moqt-*): no HTTP/3, no
 * WebTransport, no wtquic dependency.
 *
 * Attach mode: the caller owns the MsQuic API table, Registration,
 * Configuration and the HQUIC connection; the adapter is the
 * connection's callback handler and the connective tissue between
 * MsQuic events and the MoQ session.
 *
 * WIRING
 *   1. Create the moq_session_t (perspective + version) and the
 *      adapter conn around it (moq_msquic_conn_create — the cfg takes
 *      the QUIC_API_TABLE the caller already has from MsQuicOpen2).
 *   2. Client: ConnectionOpen(registration, moq_msquic_conn_callback(),
 *      conn, &h), then moq_msquic_conn_bind(conn, h), then
 *      ConnectionStart. Server: inside the listener's NEW_CONNECTION,
 *      SetCallbackHandler(h, moq_msquic_conn_callback(), conn) and
 *      moq_msquic_conn_bind(conn, h) before returning success.
 *   3. Everything else happens inside MsQuic's serialized connection
 *      callbacks: the adapter feeds the bridge, services it, and then
 *      invokes the hook — the application's slot to poll moq session
 *      events and drive the session, on the transport worker, which is
 *      the only context that may touch the moq session.
 *
 * REQUIRED TRANSPORT SETTINGS (caller's Configuration)
 *   SendBufferingEnabled = FALSE is MANDATORY: the adapter's send
 *   ownership (buffers borrowed until SEND_COMPLETE) and its in-flight
 *   send budget depend on SEND_COMPLETE meaning full acknowledgment.
 *   StreamMultiReceiveEnabled = FALSE is MANDATORY: the inbound
 *   backpressure path arrests a backed-up stream by accepting a receive
 *   only partially, which relies on MsQuic's default single-outstanding-
 *   RECEIVE behavior; multi-receive mode would break receive correctness.
 *   Peer stream counts must be nonzero for the MoQ streams the session
 *   will accept. moq_msquic_settings_init() stamps the required values
 *   (with their IsSet bits) on a QUIC_SETTINGS the caller then loads into
 *   its Configuration; the adapter cannot verify them after the fact, so
 *   a caller that merges its own settings must not re-enable either flag.
 *
 * SCOPE (current)
 *   Datagrams flow both ways (outbound sends are capped in flight and
 *   copied until MsQuic's final send state; peer availability gates
 *   them non-fatally). No idle deadline tick: sessions relying on
 *   timer-driven closes need external service via the managed facade
 *   or app calls.
 *
 * LIFETIME
 *   The moq session must outlive the conn. The conn is the user
 *   context of the connection's callbacks, so destroy it only after
 *   the connection can no longer deliver events: after
 *   SHUTDOWN_COMPLETE has been observed (the last event MsQuic ever
 *   delivers for a connection) and ConnectionClose has returned.
 *   Never call ConnectionClose/StreamClose from inside a callback
 *   while also blocking on that callback's completion elsewhere.
 */

#include <moq/session.h>

#include <msquic.h>

#ifdef __cplusplus
extern "C" {
#endif

/* MoQ-over-QUIC ALPNs (raw QUIC; version selected per session). */
#define MOQ_MSQUIC_ALPN_DRAFT16 "moqt-16"
#define MOQ_MSQUIC_ALPN_DRAFT18 "moqt-18"

typedef struct moq_msquic_conn moq_msquic_conn_t;

/* Fires on the MsQuic transport worker after the adapter serviced the
 * bridge for an event batch; poll moq session events and drive the
 * session from here (the adapter services again afterwards). */
typedef void (*moq_msquic_hook_fn)(moq_msquic_conn_t *conn, void *user);

/* Optional bracket around every transport callback batch (connection
 * and stream events alike): enter fires before any bridge feed, leave
 * after the last service pass. A managed facade points these at its
 * mutex so app-side pumps and MsQuic workers exclude each other; the
 * adapter itself stays lock-free when they are NULL. */
typedef void (*moq_msquic_guard_fn)(void *user);

typedef struct moq_msquic_conn_cfg {
    uint32_t struct_size;
    const moq_alloc_t *alloc;    /* required; copied */
    moq_session_t *session;      /* required; NOT owned; must outlive */
    const QUIC_API_TABLE *api;   /* required; NOT owned; must outlive */
    moq_msquic_hook_fn hook;     /* optional */
    void *hook_user;
    moq_msquic_guard_fn guard_enter; /* optional; both or neither */
    moq_msquic_guard_fn guard_leave;
    void *guard_user;
} moq_msquic_conn_cfg_t;

MOQ_API void moq_msquic_conn_cfg_init_sized(moq_msquic_conn_cfg_t *cfg,
                                            size_t size);

MOQ_API moq_result_t moq_msquic_conn_create(
    const moq_msquic_conn_cfg_t *cfg, moq_msquic_conn_t **out);
MOQ_API void moq_msquic_conn_destroy(moq_msquic_conn_t *conn);

/* The connection callback handler to register with MsQuic; the
 * callback context MUST be the moq_msquic_conn_t. */
MOQ_API QUIC_CONNECTION_CALLBACK_HANDLER moq_msquic_conn_callback(void);

/* Bind the HQUIC connection the callbacks will report for. Must happen
 * before ConnectionStart (client) / returning from NEW_CONNECTION
 * (server) — the endpoint ops need the handle. */
MOQ_API moq_result_t moq_msquic_conn_bind(moq_msquic_conn_t *conn,
                                          HQUIC connection);

MOQ_API moq_session_t *moq_msquic_conn_session(moq_msquic_conn_t *conn);
MOQ_API HQUIC moq_msquic_conn_connection(moq_msquic_conn_t *conn);

/* Run one service cycle (deferred-close feed, bridge service, hook,
 * service again) outside a transport callback — e.g. from a facade's
 * timer/doorbell context. The caller owns the exclusion contract: this
 * must never run concurrently with the connection's callbacks (hold
 * the same guard the cfg installs). */
MOQ_API void moq_msquic_conn_service(moq_msquic_conn_t *conn);

/* Outbound drain probes: accepted stream sends not yet completed and
 * datagram sends not yet finalized. Both drain to zero at the
 * connection's terminal (every accepted send gets exactly one
 * completion/final — canceled ones included — before
 * SHUTDOWN_COMPLETE). Same confinement as the session. */
MOQ_API int moq_msquic_conn_pending_sends(const moq_msquic_conn_t *conn);
MOQ_API int moq_msquic_conn_pending_datagrams(
    const moq_msquic_conn_t *conn);

/* The bridge went fatal (transport failure, protocol error). */
MOQ_API bool moq_msquic_conn_is_fatal(const moq_msquic_conn_t *conn);
MOQ_API uint64_t moq_msquic_conn_fatal_code(const moq_msquic_conn_t *conn);
/* The transport closed cleanly. */
MOQ_API bool moq_msquic_conn_is_closed(const moq_msquic_conn_t *conn);
MOQ_API uint64_t moq_msquic_conn_close_code(const moq_msquic_conn_t *conn);

/* Stamp the transport settings this adapter requires (send buffering
 * off, datagram receive on, MoQ-scale stream counts) onto a caller
 * QUIC_SETTINGS. The caller may adjust further before loading it into
 * its Configuration — but SendBufferingEnabled must stay FALSE. */
MOQ_API void moq_msquic_settings_init(QUIC_SETTINGS *settings);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_MSQUIC_H */
