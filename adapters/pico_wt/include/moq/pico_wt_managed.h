#ifndef MOQ_PICO_WT_MANAGED_H
#define MOQ_PICO_WT_MANAGED_H

/*
 * moq-adapter-pico-wt-managed: optional helper that owns the picoquic
 * WebTransport lifecycle (QUIC context, network thread, WT CONNECT,
 * MoQ session + adapter) behind a small C facade. Parallel in spirit
 * to moq_pq_threaded_t (raw picoquic) and moq_mvfst_managed_t.
 *
 * Threading model (same as moq_pq_threaded):
 *   One picoquic network thread is spawned. ALL moq_session_*,
 *   moq_pico_wt_*, and facade-data calls must happen on that thread,
 *   inside the on_pump callback. The app thread communicates via its
 *   own queues and uses wake()/wait() for cross-thread signaling. This
 *   preserves moq_session_t single-thread confinement.
 *
 * Callback rules:
 *   on_pump:     runs on the network thread once the WT session is up
 *                and the adapter is attached. May call any session /
 *                adapter API. Return 0 to continue, nonzero for clean
 *                loop termination.
 *   on_activity: runs on the network thread after each pump cycle.
 *                Signal-only — must NOT call mutation APIs. May be NULL.
 *   Neither callback may call moq_pico_wt_managed_stop.
 *
 * Client mode:
 *   _create builds the QUIC context, prepares the WT client connection,
 *   and starts the network thread. The MoQ session + adapter are
 *   created on the network thread once WT CONNECT is accepted, and the
 *   client session is started then. _session() returns NULL until that
 *   point; on_pump is not called until then.
 *
 * Server mode:
 *   _create builds the QUIC context (cert + key required), installs the
 *   WT path handler, and starts listening. The MoQ session + adapter
 *   are created on the network thread when the first client's WT
 *   CONNECT is accepted; _session() returns NULL until then and on_pump
 *   is not called before then. One active connection is supported; a
 *   second CONNECT is refused (HTTP 501), which the refused client
 *   observes as a deterministic terminal fatal (see below). A concrete
 *   listen port is required.
 *
 * Terminal states and precedence:
 *   The facade has four terminal conditions, observed via wait()/wake()
 *   returning MOQ_ERR_CLOSED. In precedence order (highest first):
 *     fatal   — is_fatal()==true. Transport/handshake failure (dead
 *               peer, cert rejection, WT CONNECT refused) or a MoQ/
 *               bridge protocol error. fatal_code()==0 for transport/
 *               handshake-level failures; otherwise the bridge fatal
 *               code. Once fatal, the session never comes up (or is
 *               abandoned). fatal wins if both fatal and closed latch.
 *     closed  — is_closed()==true, is_fatal()==false. A *clean* session
 *               close: locally, a GOAWAY drain → CLOSE_SESSION; or a
 *               peer clean close surfaced via on_transport_close —
 *               including a parsed inbound WT CLOSE_WEBTRANSPORT_SESSION
 *               capsule, or connection teardown / deregister. close_code()
 *               is the MoQ close code (propagated from the capsule when
 *               available). wait()/wake() go terminal at once so a
 *               consumer that only loops on wait() observes the *local*
 *               facade terminal state without polling — this is NOT proof
 *               that a locally queued close was observed by the peer.
 *               After a local close the loop makes a bounded best-effort
 *               attempt to push the queued CLOSE_SESSION/FIN out (until
 *               the send backlog drains, capped) before stopping.
 *     pump_exit — on_pump returned nonzero (app-requested clean exit).
 *               is_fatal()==false, is_closed()==false.
 *     stopped — stop() was called.
 *   To classify a terminal wait(): check is_fatal(), then is_closed();
 *   neither means pump_exit or stopped.
 *
 * Certificate policy fails closed by default. A client created with
 * insecure_skip_verify=false (the zero default) installs a real verifier
 * that validates the peer certificate chain against the system trust
 * store AND checks the server name (the `sni` field below, which defaults
 * to the connect host for a DNS host; see that field for the IP-literal
 * rule) — a chain or name mismatch fails the handshake. This
 * uses moq_picoquic_set_cert_verifier() internally; if no verifier can be
 * installed (e.g. no system trust store), _create fails rather than
 * connecting unauthenticated. insecure_skip_verify=true is the explicit
 * opt-out: it installs a null verifier so the client accepts ANY server
 * cert (tests/demos only). The optional configure_quic hook runs AFTER
 * the default verifier, so a caller can swap in a private-CA verifier
 * (moq_picoquic_set_cert_verifier(quic, ca_path) from
 * <moq/picoquic_verify.h>) or other TLS policy. Because the managed
 * client now installs the verifier itself, the base picoquic adapter is a
 * dependency of this component (build, install, and pkg-config wiring pull
 * it in automatically) — consumers do not need to request it separately.
 *
 * Experimental. Installed as component adapter-pico-wt-managed when
 * libmoq is built with MOQ_BUILD_PICO_WT_MANAGED=ON. Consume via CMake
 * (find_package(libmoq COMPONENTS adapter-pico-wt-managed)) or pkg-config
 * (libmoq-pico-wt-managed) — a standalone .pc that is NOT folded into the
 * libmoq.pc package. Requires picoquic with BUILD_HTTP=ON.
 */

#include <moq/pico_wt.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_pico_wt_managed moq_pico_wt_managed_t;

typedef struct moq_pico_wt_managed_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;          /* required: alloc+realloc+free */

    moq_perspective_t  perspective;    /* CLIENT or SERVER; required */

    /* TLS. Server requires both; client may omit. */
    const char        *cert_path;
    const char        *key_path;

    /* Network. */
    const char        *host;           /* client: remote host; required */
    const char        *sni;            /* client TLS SNI / :authority and the
                                          name the default verifier checks.
                                          Default: a DNS host is used as-is. An
                                          IP-literal host has no valid SNI, so it
                                          uses "localhost"; but authenticating a
                                          bare IP with the built-in default
                                          verifier (verified mode and no
                                          configure_quic) REQUIRES an explicit
                                          sni, else _create fails MOQ_ERR_INVAL. */
    const char        *path;           /* WT path; default "/moq" */
    int                port;           /* 1..65535; required */

    /* Session tuning (0 = default). */
    bool               send_request_capacity;
    uint64_t           initial_request_capacity;   /* default 64 */

    /* Cert policy. Default (false) fails closed: the client installs a real
     * verifier (system trust store + server-name check against `sni`). If true,
     * the client accepts ANY server cert (picoquic_set_null_verifier) —
     * tests/demos only. To trust a private CA, install it from configure_quic
     * (which runs after the default verifier). */
    bool               insecure_skip_verify;

    /* Optional: called during _create after picoquic_create and AFTER the
     * default certificate verifier is installed (client mode), before any
     * connection or the network thread. Override/augment TLS verification
     * (e.g. install a private-CA verifier), set token stores, etc. Return 0
     * to continue, nonzero to abort. */
    int              (*configure_quic)(picoquic_quic_t *quic, void *ctx);
    void              *configure_quic_ctx;

    /* App pump callback (required). Runs on the network thread once the
     * WT session is up. Tick facades, subscribe, publish, poll here —
     * all session/adapter calls are safe. Return 0 to continue; nonzero
     * requests clean loop termination (wait() then returns
     * MOQ_ERR_CLOSED, is_fatal() stays false). */
    int              (*on_pump)(moq_pico_wt_managed_t *m,
                                uint64_t now_us, void *ctx);
    void              *on_pump_ctx;

    /* Optional: network-thread post-pump signal. Do NOT call session /
     * adapter APIs here. May be NULL. */
    void             (*on_activity)(moq_pico_wt_managed_t *m, void *ctx);
    void              *on_activity_ctx;

    /* GOAWAY drain timeout (0 = disabled). When nonzero, a session that
     * has sent GOAWAY auto-closes after this many microseconds, emitting
     * CLOSE_SESSION (close_code MOQ_CLOSE_GOAWAY_TIMEOUT) and driving the
     * facade to its terminal closed state. Mirrors the mvfst managed cfg
     * field of the same name. Appended last to preserve the struct_size
     * append-only ABI. */
    uint64_t           goaway_timeout_us;

    /* Appended — WebTransport MoQ version negotiation. A comma-separated
     * token list ("moqt-18, moqt-16"), copied.
     *
     * CLIENT: the WT-Available-Protocols offer, in PREFERENCE ORDER (the
     * server matches the client's order). The selected WT-Protocol token
     * decides moq_session_cfg_t.version before the session is created; a
     * missing, unknown, or un-offered selection is a terminal fatal --
     * never a silent draft-16 default.
     * SERVER: the supported list. A client offer is matched in the
     * client's order; no overlap refuses the CONNECT (explicit non-2xx).
     * A client that sends no offer gets legacy draft-16.
     * NULL: no negotiation either way (legacy draft-16 behavior). */
    const char        *wt_protocols;

} moq_pico_wt_managed_cfg_t;

MOQ_API void moq_pico_wt_managed_cfg_init(moq_pico_wt_managed_cfg_t *cfg);

/* -- Lifecycle ---------------------------------------------------- */

/*
 * Create the QUIC context and start the network thread. On success the
 * thread is running; the WT handshake and session/adapter creation
 * complete asynchronously on the network thread.
 *
 * On failure no thread is running and _stop/_destroy are not needed.
 * Returns MOQ_ERR_INVAL on bad config, MOQ_ERR_NOMEM, or
 * MOQ_ERR_INTERNAL on setup failure.
 */
MOQ_API moq_result_t moq_pico_wt_managed_create(
    const moq_pico_wt_managed_cfg_t *cfg, moq_pico_wt_managed_t **out);

/*
 * Stop and join the network thread. Idempotent. MUST NOT be called
 * from on_pump/on_activity (returns MOQ_ERR_WRONG_STATE). After return,
 * no more callbacks fire; then call _destroy.
 */
MOQ_API moq_result_t moq_pico_wt_managed_stop(moq_pico_wt_managed_t *m);

/* Destroy adapter, session, QUIC context, and wrapper. Call after
 * _stop. */
MOQ_API void moq_pico_wt_managed_destroy(moq_pico_wt_managed_t *m);

/* -- Accessors (thread-safe) -------------------------------------- */

/* The MoQ session, or NULL before the WT session is up. Owned by the
 * helper — do not destroy. */
MOQ_API moq_session_t *moq_pico_wt_managed_session(
    moq_pico_wt_managed_t *m);

/* Fatal state (transport/handshake or bridge error). Thread-safe.
 * fatal_code()==0 for transport/handshake-level fatal; otherwise the
 * bridge fatal code. See the terminal-precedence note above. */
/* The negotiated MoQ version: 0 until known, then the version the session
 * was created with. Thread-safe. */
MOQ_API moq_version_t moq_pico_wt_managed_negotiated_version(
    const moq_pico_wt_managed_t *m);

MOQ_API bool     moq_pico_wt_managed_is_fatal(
    const moq_pico_wt_managed_t *m);
MOQ_API uint64_t moq_pico_wt_managed_fatal_code(
    const moq_pico_wt_managed_t *m);

/* Clean-close state. is_closed() becomes true (and is_fatal() stays
 * false) after a clean session close; close_code() is the MoQ close
 * code (e.g. MOQ_CLOSE_GOAWAY_TIMEOUT). A clean close is terminal:
 * wait()/wake() then return MOQ_ERR_CLOSED and the loop stops.
 * Thread-safe. */
MOQ_API bool     moq_pico_wt_managed_is_closed(
    const moq_pico_wt_managed_t *m);
MOQ_API uint64_t moq_pico_wt_managed_close_code(
    const moq_pico_wt_managed_t *m);

/* The picoquic local error captured when a CLIENT connection failed during the
 * handshake (cert rejection, timeout, refused, ALPN mismatch): a QUIC
 * CRYPTO_ERROR (0x100 | TLS alert) for a TLS failure, else a transport code; 0
 * when there was no such failure. The service endpoint classifies it into a
 * transport-agnostic terminal reason. Thread-safe. */
MOQ_API uint64_t moq_pico_wt_managed_terminal_error(
    const moq_pico_wt_managed_t *m);

/* Graceful-drain probe (backs moq_endpoint_drain). 1 = local stream flush done
 * (no queued/ready reliable stream data or unsent FIN; NOT a packet-ACK check),
 * 0 = still draining, -2 = called on the network thread. App-thread; safe to
 * poll. */
MOQ_API int moq_pico_wt_managed_drain_state(moq_pico_wt_managed_t *m);

/* The configured listen port (server) or target port (client). */
MOQ_API int moq_pico_wt_managed_local_port(
    const moq_pico_wt_managed_t *m);

/* -- Cross-thread signaling --------------------------------------- */

/* Wake the network thread so it runs on_pump. Safe from any thread;
 * coalesced. MOQ_OK on success, MOQ_ERR_CLOSED once terminal
 * (stopped, fatal, clean-closed, or pump-exit). */
MOQ_API moq_result_t moq_pico_wt_managed_wake(moq_pico_wt_managed_t *m);

/*
 * Block until activity or timeout. MOQ_OK if on_pump ran, MOQ_DONE on
 * timeout, MOQ_ERR_CLOSED once terminal (stopped, fatal, clean-closed,
 * or pump-exit). timeout_us==0 is a non-blocking check; UINT64_MAX
 * waits indefinitely.
 */
MOQ_API moq_result_t moq_pico_wt_managed_wait(moq_pico_wt_managed_t *m,
                                              uint64_t timeout_us);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PICO_WT_MANAGED_H */
