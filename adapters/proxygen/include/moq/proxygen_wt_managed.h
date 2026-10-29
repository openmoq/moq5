#ifndef MOQ_PROXYGEN_WT_MANAGED_H
#define MOQ_PROXYGEN_WT_MANAGED_H

/*
 * Managed proxygen WebTransport client facade (C API).
 *
 * Where <moq/proxygen_wt.hpp> (attach mode, C++) bridges a caller-owned
 * proxygen::WebTransport into a MoQ session, this managed facade owns the
 * whole client stack: it spawns a network thread with a folly EventBase,
 * performs the HTTP/3 WebTransport CONNECT (proxygen HTTPCoroConnector +
 * sendWtReq), negotiates the MoQ version via WT-Available-Protocols /
 * WT-Protocol, creates the moq_session_t on success, and drives the libmoq
 * transport bridge through the existing attach adapter underneath.
 *
 * Pure C surface: opaque handle only, no proxygen/folly types. The shape and
 * lifecycle mirror moq_pico_wt_managed_t and moq_mvfst_managed_t.
 *
 * Threading:
 *   wake() and wait() are safe from any thread.
 *   session() returns a pointer valid only inside on_pump callbacks on the
 *     managed network thread. Do not call session APIs from the app thread.
 *   create/stop/destroy are called from the application thread.
 *   is_fatal/fatal_code/is_closed/negotiated_version are safe from any thread.
 *
 * Version negotiation:
 *   The MoQ session is created AFTER the WT CONNECT response, so a real
 *   multi-version offer is negotiated (unlike the exact-version mvfst raw
 *   adapter). cfg.alpn_list/alpn_count is the WT-Available-Protocols offer in
 *   preference order ("moqt-18", "moqt-16"); the selected WT-Protocol decides
 *   the session version. A missing/unknown/un-offered selection is a terminal
 *   fatal -- never a silent draft-16 downgrade.
 *
 * Packaging: CMake-only (proxygen/folly/fizz ship no pkg-config), installed as
 *   CMake component adapter-proxygen-wt-managed. Requires a C++ compiler and a
 *   discoverable proxygen CONFIG package; see docs/transport-integration-guide.md.
 */

#include <moq/types.h>
#include <moq/session.h>
#include <moq/connection_target.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_proxygen_wt_managed moq_proxygen_wt_managed_t;

/*
 * Pump callback: runs on the network thread once the WT session is up. The
 * session pointer from moq_proxygen_wt_managed_session() is valid inside it.
 * Return 0 to continue, nonzero to request clean shutdown. Must NOT call
 * moq_proxygen_wt_managed_stop() from inside.
 */
typedef int (*moq_proxygen_wt_pump_fn)(moq_proxygen_wt_managed_t *m,
                                       uint64_t now_us,
                                       void *ctx);

/*
 * Activity callback: signal-only notification after each pump. Runs on the
 * network thread. Must NOT call mutation APIs or stop(). May be NULL.
 */
typedef void (*moq_proxygen_wt_activity_fn)(moq_proxygen_wt_managed_t *m,
                                            void *ctx);

typedef struct moq_proxygen_wt_managed_cfg {
    uint32_t              struct_size;
    const moq_alloc_t    *alloc;          /* NULL = default allocator */
    moq_perspective_t     perspective;    /* CLIENT only (server unsupported) */

    const char           *host;           /* remote host; required */
    int                   port;           /* 1..65535; required */
    const char           *sni;            /* TLS SNI; NULL = host. Never the
                                             CONNECT :authority (see target) */
    const char           *path;           /* WT CONNECT :path; NULL = "/moq",
                                             or the target's (see target) */

    /* TLS. insecure_skip_verify and ca_file are mutually exclusive.
     *   insecure_skip_verify=true → accept any server cert (testing only)
     *   ca_file set               → verify chain + identity vs this PEM CA
     *   neither                   → system trust store + host/IP identity */
    bool                  insecure_skip_verify;
    const char           *ca_file;        /* PEM CA bundle; NULL = system roots */

    /* Session tuning (0/false = library defaults). */
    bool                  send_request_capacity;
    uint64_t              initial_request_capacity;

    /* WT-Available-Protocols offer, in PREFERENCE ORDER ("moqt-18","moqt-16").
     * Strings are read only during create(). NULL/empty count = legacy single
     * "moqt-16". The selected WT-Protocol from the CONNECT response sets the
     * session version; an unknown/un-offered/missing selection is fatal. */
    const char *const    *alpn_list;
    size_t                alpn_count;

    /* Callbacks. */
    moq_proxygen_wt_pump_fn     on_pump;      /* required */
    moq_proxygen_wt_activity_fn on_activity;  /* optional, signal-only */
    void                       *user_ctx;     /* passed to callbacks */

    /* Session deadline tuning (0 = disabled). */
    uint64_t              goaway_timeout_us;

    /* Connection target: the URI identity the session is bound with when the
     * negotiated wire profile requires a transport binding (see
     * <moq/connection_target.h>; draft-22 does, draft-16/18 do not). The
     * facade supplies the transport facts itself: the session is created once
     * the WebTransport session is up, which over HTTP/3 means QUIC DATAGRAM
     * was negotiated and both sides advertised HTTP/3 datagrams. The target's
     * fields and bytes are copied during create(); NULL derives the authority
     * and path/query from `host`, `port` and `path`. A client target is
     * complete with no policy, and is the one identity the CONNECT sends:
     * :authority as spelled (or "host:port" when derived), :path the
     * path/query as spelled with "/" in front of an empty or query-only one
     * (RFC 9114 Section 4.3.1). A `path` set alongside a target must equal
     * that projection; a conflict, an identity above the binding's bound
     * (authority plus path/query over 65535 bytes) or one embedding a NUL is
     * MOQ_ERR_INVAL at create(). A malformed target is
     * MOQ_ERR_INVAL at create(). Read only when struct_size covers it. */
    const moq_connection_target_t *target;

    /* Reserved: the 4 bytes of tail padding the layout that ended at target
     * leaves on ILP32 targets that align uint64_t to 8. keep_alive_interval_ms
     * is placed AFTER this slot so its struct_size gate lands past that old
     * sizeof. Never read. */
    uint32_t              _reserved_keep_alive;

    /* QUIC keepalive: send a QUIC PING once the connection has been idle this
     * many milliseconds, so a quiet but live session -- e.g. a publisher with
     * no subscriber yet -- is not reaped by the peer's idle timeout. An
     * interval at or above the connection's effective idle timeout never
     * fires. 0 disables keepalive: the default, and what a caller whose
     * struct_size does not cover this field gets. This is transport keepalive
     * only; it implies no MoQ or session deadline. */
    uint32_t              keep_alive_interval_ms;
} moq_proxygen_wt_managed_cfg_t;

/*
 * Both initializers clear the struct, stamp struct_size and default the
 * perspective to CLIENT. A caller built against this header gets the full
 * current struct: the function-like macro below passes its sizeof to
 * moq_proxygen_wt_managed_cfg_init_sized(), which clears and stamps
 * min(cfg_size, sizeof current). The symbol itself, which a caller built
 * before `target` was appended calls, clears and stamps the layout through
 * goaway_timeout_us and never writes past that caller's storage.
 */
MOQ_API void moq_proxygen_wt_managed_cfg_init(moq_proxygen_wt_managed_cfg_t *cfg);
MOQ_API void moq_proxygen_wt_managed_cfg_init_sized(
    moq_proxygen_wt_managed_cfg_t *cfg, size_t cfg_size);
#define moq_proxygen_wt_managed_cfg_init(cfg) \
    moq_proxygen_wt_managed_cfg_init_sized((cfg), sizeof(*(cfg)))

/*
 * Create a managed proxygen WT client: spawn the network thread, connect +
 * WT CONNECT to cfg.host:cfg.port path, negotiate version, create the session
 * and adapter. Blocks until init succeeds or fails; on success the pump loop
 * is running.
 *
 * Returns MOQ_OK with *out set; MOQ_ERR_INVAL for bad config;
 * MOQ_ERR_NOMEM on allocation failure; MOQ_ERR_INTERNAL on connect/handshake/
 * thread setup failure. On failure no thread runs; stop/destroy not needed.
 */
MOQ_API moq_result_t moq_proxygen_wt_managed_create(
    const moq_proxygen_wt_managed_cfg_t *cfg,
    moq_proxygen_wt_managed_t **out);

/* Request clean shutdown; idempotent; joins the network thread. Must NOT be
 * called from on_pump/on_activity. Returns MOQ_ERR_INVAL from the managed
 * thread. */
MOQ_API moq_result_t moq_proxygen_wt_managed_stop(moq_proxygen_wt_managed_t *m);

/* Destroy and free all resources; calls stop() if needed. destroy(NULL) is a
 * no-op. Must NOT be called from on_pump/on_activity. */
MOQ_API void moq_proxygen_wt_managed_destroy(moq_proxygen_wt_managed_t *m);

/* The MoQ session, or NULL outside on_pump / before the WT session is up /
 * after stop. Thread-confined to the network thread. */
MOQ_API moq_session_t *moq_proxygen_wt_managed_session(
    moq_proxygen_wt_managed_t *m);

/* Wake the network thread (coalesced). Safe from any thread. Returns MOQ_OK
 * or MOQ_ERR_CLOSED if stopped/fatal/pump-exited. */
MOQ_API moq_result_t moq_proxygen_wt_managed_wake(moq_proxygen_wt_managed_t *m);

/* Wait for activity or timeout. Returns MOQ_OK (activity), MOQ_DONE (timeout),
 * or MOQ_ERR_CLOSED. timeout_us==0 polls; UINT64_MAX waits indefinitely. */
MOQ_API moq_result_t moq_proxygen_wt_managed_wait(
    moq_proxygen_wt_managed_t *m, uint64_t timeout_us);

/* Fatal transport/handshake/bridge state. Safe from any thread. */
MOQ_API bool     moq_proxygen_wt_managed_is_fatal(
    const moq_proxygen_wt_managed_t *m);
MOQ_API uint64_t moq_proxygen_wt_managed_fatal_code(
    const moq_proxygen_wt_managed_t *m);

/* Clean-close state (peer session end / GOAWAY drain). Safe from any thread. */
MOQ_API bool     moq_proxygen_wt_managed_is_closed(
    const moq_proxygen_wt_managed_t *m);

/* The negotiated MoQ version: 0 until the WT CONNECT completes and the session
 * is created, then the version the selected WT-Protocol mapped to. Thread-safe. */
MOQ_API moq_version_t moq_proxygen_wt_managed_negotiated_version(
    const moq_proxygen_wt_managed_t *m);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_PROXYGEN_WT_MANAGED_H */
