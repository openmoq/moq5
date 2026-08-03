#ifndef MOQ_WTQUIC_NETWORK_MANAGED_H
#define MOQ_WTQUIC_NETWORK_MANAGED_H

/*
 * moq-adapter-wtquic-network-managed: the managed MoQ client over
 * wtquic's Network.framework backend. APPLE ONLY (macOS 13 / iOS 16+).
 *
 * SCOPE — WebTransport over Network.framework, ONE client connection,
 * ONE managed lane. This is not raw QUIC, not a listener/server
 * facade, and not the (separate, future) MsQuic-backed wtquic server
 * facade. The lane interface exists so the managed API matches the
 * shared lane shape across adapters (picoquic-threaded, mvfst,
 * MsQuic-managed): exactly one lane owning exactly one connection.
 *
 * The facade owns the whole vertical:
 *
 *   moq_session -> transport bridge -> wtquic attach adapter
 *              -> wtq_nw_conn (serial-queue domain) -> Network.framework
 *
 * THREADING — THE DOMAIN IS THE ONLY SESSION THREAD
 *   There is NO worker thread here and no dispatch type in this API:
 *   the wtq_nw_conn's backend-owned serial queue is the connection's
 *   SERIALIZATION DOMAIN, and every MoQ/bridge operation runs on it.
 *   on_lane_pump / on_activity fire on the domain; cross-thread work
 *   maps directly onto wtq_nw_conn_post via wake(). moq_session_t
 *   confinement is therefore preserved by construction.
 *
 * TWO ACCESS TIERS — do not conflate them:
 *   - STRICT LANE WINDOW: moq_wtquic_network_lane_next_conn and the
 *     conn-view accessors (conn_session / conn_adapter / conn_lane)
 *     are valid ONLY inside this lane's on_lane_pump. Everywhere else
 *     — including on_activity and posted closures, which are on the
 *     domain but OUTSIDE the window — they return NULL. Returned conn
 *     views are valid for the current pump only.
 *   - ACTOR DOMAIN: moq_wtquic_network_managed_session() /
 *     moq_wtquic_network_managed_adapter() are legal anywhere ON THE
 *     DOMAIN, including posted closures (the actor escape hatch), and
 *     return NULL off-domain. "On the serial domain" is deliberately
 *     broader than "inside the lane pump".
 *
 * NEGOTIATION IS REAL
 *   With a multi-token offer the MoQ session is NOT pre-created: the
 *   facade proxies the wtquic events until on_established, validates
 *   that the selected WT-Protocol token is recognized AND was offered,
 *   creates the MoQ session with the selected version, attaches the
 *   adapter, and forwards establishment. A server that omits the
 *   WT-Protocol token gets the legacy draft-16 fallback ONLY when
 *   draft-16 was offered (or no offer was configured); an exact
 *   draft-18 offer is never silently downgraded — that is a terminal
 *   fatal. Refusal, failure, and pre-establishment close terminate
 *   without ever constructing a MoQ session.
 *
 * TERMINAL PRECEDENCE (mirrors the other managed facades)
 *   fatal   — is_fatal(): handshake/transport failure, refusal,
 *             negotiation failure, or a bridge/MoQ protocol error.
 *             fatal_code()==0 for transport/handshake-level failures.
 *             Fatal wins when both fatal and closed would latch.
 *   closed  — is_closed() && !is_fatal(): a clean session close;
 *             close_code() is the MoQ close code.
 *   pump_exit — on_lane_pump returned nonzero (clean app exit).
 *   stopped — stop was requested.
 *   wait()/wake() return MOQ_ERR_CLOSED once terminal; classify with
 *   is_fatal() then is_closed().
 *
 * LIFECYCLE IS SPLIT (actor-safe, per the wtquic backend)
 *   stop_begin() is nonblocking and legal from ANY thread including
 *   the domain (on_lane_pump, callbacks). join() blocks until the
 *   finished tearing down and is OFF-DOMAIN ONLY. stop() is the
 *   conventional blocking composition (stop_begin + join) for CLI and
 *   service hosts — callback/actor code must never call it. During
 *   final teardown the attach adapter and the owned MoQ session are
 *   destroyed ON-DOMAIN, before join()/wait() observe completion.
 *
 *   ACTORS NEVER BLOCK: configure cfg.on_stopped instead of joining.
 *   It runs on the domain AFTER the adapter and session were
 *   destroyed, as the facade's LAST access to itself — resuming an
 *   actor that immediately calls destroy() (from inside the callback
 *   or from the resumed continuation) is the designed pattern. When
 *   on_stopped is used this way, no other thread may join/destroy
 *   concurrently. destroy() releases the facade after join(), after
 *   wait() observed teardown completion, or from/after on_stopped.
 */

#include <moq/wtquic.h>

#include <wtquic/wtquic_network.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_wtquic_network_managed moq_wtquic_network_managed_t;
/* The single managed lane (a stable handle for the facade's life). */
typedef struct moq_wtquic_network_managed_lane moq_wtquic_network_managed_lane_t;
/* The managed view of the one client connection, yielded by
 * moq_wtquic_network_lane_next_conn inside the pump window. */
typedef struct moq_wtquic_network_managed_conn moq_wtquic_network_managed_conn_t;

/*
 * on_lane_pump: the lane's EXCLUSIVE access window. Runs ON THE DOMAIN
 * after the adapter serviced the bridge for an event batch, and after
 * every wake(); it begins only once the negotiated MoQ session and
 * attach adapter exist (never pre-establishment). Iterate the lane's
 * one connection with moq_wtquic_network_lane_next_conn() and drive its
 * session here. Return 0 to continue; nonzero requests clean
 * termination (pump_exit) — the facade then begins its stop. A
 * terminal connection stays iterable through the pump passes that
 * observe it (the view is torn down only at final teardown).
 * on_activity: NOTIFICATION-ONLY domain callback after each pump; it
 * is NOT a lane access window (the strict accessors return NULL
 * there) and must not call mutation APIs. Either may be NULL.
 */
typedef int (*moq_wtquic_network_lane_pump_fn)(moq_wtquic_network_managed_t *m,
                                          moq_wtquic_network_managed_lane_t *lane,
                                          uint64_t now_us, void *user);
typedef void (*moq_wtquic_network_activity_fn)(moq_wtquic_network_managed_t *m,
                                          void *ctx);

/*
 * Config. struct_size versions the struct (v1 prefix through
 * `insecure_skip_verify` is required; later fields are optional tails
 * read only when they fit).
 */
typedef struct moq_wtquic_network_managed_cfg {
    uint32_t           struct_size;
    const moq_alloc_t *alloc;      /* required (alloc+realloc+free —
                                      the owned session's receive
                                      buffering grows); copied */

    const char        *host;       /* required: remote host / TLS name */
    int                port;       /* required: 1..65535 */
    const char        *path;       /* WT path; default "/moq" */
    const char        *authority;  /* :authority; default = host */

    /*
     * WebTransport MoQ version offer, comma-separated in PREFERENCE
     * ORDER (e.g. "moqt-18, moqt-16"); copied. NULL = legacy draft-16
     * (no WT-Available-Protocols offer). The list is STRICTLY
     * validated before anything dials: single commas (optional spaces
     * around them), every token a recognized MoQ WT-Protocol, no
     * empties or duplicates, at most 8 — anything else is
     * MOQ_ERR_INVAL, never a silently weakened offer. The selected
     * token decides the MoQ session version — see NEGOTIATION IS REAL
     * above.
     */
    const char        *wt_protocols;

    /* Accept ANY server certificate. Loopback/development ONLY;
     * system trust is the default. */
    bool               insecure_skip_verify;

    /* ---- optional tails (versioned) ---- */
    moq_wtquic_network_lane_pump_fn on_lane_pump;      /* may be NULL */
    void                      *on_lane_pump_user;
    moq_wtquic_network_activity_fn  on_activity;       /* may be NULL */
    void                      *on_activity_ctx;

    /* Session tuning (0 = default), applied to the negotiated-version
     * session at creation. */
    bool               send_request_capacity;
    uint64_t           initial_request_capacity;

    /*
     * Optional teardown continuation (the actor alternative to join):
     * runs ON THE DOMAIN after the attach adapter and owned session
     * were destroyed, as the facade's last access to itself. May call
     * moq_wtquic_network_managed_destroy(). May be NULL.
     */
    void             (*on_stopped)(void *ctx);
    void              *on_stopped_ctx;

    /* This facade is a CLIENT: 0 (default) or MOQ_PERSPECTIVE_CLIENT
     * are accepted; MOQ_PERSPECTIVE_SERVER is refused with
     * MOQ_ERR_UNSUPPORTED (a server needs the future MsQuic-backed
     * wtquic facade); any other value is MOQ_ERR_INVAL. */
    moq_perspective_t  perspective;
    /* This facade has ONE lane: 0 (default) or 1 are accepted;
     * anything greater is refused with MOQ_ERR_UNSUPPORTED (matches
     * the mvfst single-lane facade). */
    uint32_t           lane_count;

    /*
     * Optional application service-deadline query — ONE ABI block, read
     * only when struct_size covers THROUGH `app_deadline_ctx` (the
     * callback and its context are read together or not at all).
     *
     * Returns the app's earliest pending time-based deadline in this
     * facade's clock domain (CLOCK_MONOTONIC µs), or UINT64_MAX for none.
     * After each on-domain service cycle the facade folds it with the
     * session's own next deadline and arms wtquic's native delayed
     * doorbell, so a purely time-based deadline (e.g. a periodic catalog
     * refresh) wakes an otherwise-idle domain. MUST be pure/non-blocking
     * and MUST NOT re-enter this facade. NULL = none. The fold happens only
     * during a pump/service cycle, so when application state changes the value
     * this query would return, the owner MUST issue a facade/lane wake
     * (moq_wtquic_network_managed_wake / moq_wtquic_network_lane_wake) so the
     * next pump recomputes the delayed arm; an idle domain will not observe the
     * new deadline on its own.
     *
     * Set it after either initializer — moq_wtquic_network_managed_cfg_init
     * stamps the full current struct, and moq_wtquic_network_managed_cfg_init_sized
     * stamps min(cfg_size, sizeof) — the block is honored once struct_size
     * covers through app_deadline_ctx.
     */
    uint64_t         (*app_deadline_us)(void *ctx);
    void              *app_deadline_ctx;
} moq_wtquic_network_managed_cfg_t;

/* Pointer initializer: clears and stamps the FULL current struct, so a
 * current-header caller can set any field, including the appended
 * app_deadline block, directly afterward. */
MOQ_API void moq_wtquic_network_managed_cfg_init(
    moq_wtquic_network_managed_cfg_t *cfg);

/* Explicit caller-sized initializer: zeroes and stamps min(cfg_size,
 * sizeof(current)). Fields the caller's build lacks stay unset; create()
 * reads each optional tail only when struct_size covers it. */
MOQ_API void moq_wtquic_network_managed_cfg_init_sized(
    moq_wtquic_network_managed_cfg_t *cfg, size_t cfg_size);

/*
 * Dial. Returns as soon as the attempt is underway; establishment,
 * negotiation, session creation, and every event happen on the
 * domain. On failure nothing is running and destroy() is not needed.
 */
MOQ_API moq_result_t moq_wtquic_network_managed_create(
    const moq_wtquic_network_managed_cfg_t *cfg,
    moq_wtquic_network_managed_t **out);

/* Nonblocking stop request; idempotent; legal from any thread
 * including the domain. Returns whether THIS call initiated it. */
MOQ_API bool moq_wtquic_network_managed_stop_begin(moq_wtquic_network_managed_t *m);

/* Block until teardown completed (adapter + session destroyed
 * on-domain). OFF-DOMAIN ONLY: on the domain it returns
 * MOQ_ERR_WRONG_STATE immediately. */
MOQ_API moq_result_t moq_wtquic_network_managed_join(moq_wtquic_network_managed_t *m);

/* The blocking composition (stop_begin + join) for CLI/service hosts.
 * MOQ_ERR_WRONG_STATE on the domain. */
MOQ_API moq_result_t moq_wtquic_network_managed_stop(moq_wtquic_network_managed_t *m);

/* Release the facade. Call only after join()/stop() returned (or
 * wait() went terminal AND stop completed). */
MOQ_API void moq_wtquic_network_managed_destroy(moq_wtquic_network_managed_t *m);

/* -- Actor-domain accessors (the escape hatch) ---------------------- */

/*
 * The MoQ session — legal anywhere ON THE DOMAIN (on_lane_pump,
 * on_activity, POSTED closures, adapter hook contexts). Returns NULL
 * off-domain, NULL before establishment completed negotiation, and
 * NULL once teardown destroyed it. Owned by the facade — never
 * destroy it. Deliberately broader than the strict lane window: a
 * posted actor closure may drive the session without a pump.
 */
MOQ_API moq_session_t *moq_wtquic_network_managed_session(
    moq_wtquic_network_managed_t *m);
/* The attach adapter conn — same domain rules as session(). */
MOQ_API moq_wtquic_conn_t *moq_wtquic_network_managed_adapter(
    moq_wtquic_network_managed_t *m);

/* -- Lanes (the unified managed lane interface) ---------------------- */

/* The number of lanes: always 1 for a valid facade (0 for NULL). */
MOQ_API uint32_t moq_wtquic_network_managed_lane_count(
    const moq_wtquic_network_managed_t *m);

/* The lane at `index`: index 0 only; NULL otherwise. The handle is
 * stable for the facade's lifetime and safe to pass across threads
 * (only the strict accessors below gate on the pump window). */
MOQ_API moq_wtquic_network_managed_lane_t *moq_wtquic_network_managed_lane(
    moq_wtquic_network_managed_t *m, uint32_t index);

/* A lane's index: always 0. */
MOQ_API uint32_t moq_wtquic_network_lane_index(
    const moq_wtquic_network_managed_lane_t *lane);

/*
 * Iterate the lane's connections: prev = NULL yields the one active
 * client connection view (or NULL before establishment); passing that
 * view back yields NULL. ONLY valid inside THIS lane's on_lane_pump —
 * returns NULL everywhere else, including on_activity and posted
 * closures. The returned view is valid for the current pump only.
 */
MOQ_API moq_wtquic_network_managed_conn_t *moq_wtquic_network_lane_next_conn(
    moq_wtquic_network_managed_lane_t *lane, moq_wtquic_network_managed_conn_t *prev);

/* Ask the facade to pump this lane (rings the facade doorbell). Safe
 * from any thread, INCLUDING inside the current pump — delivery is
 * always deferred, never re-entrant. Exactly
 * moq_wtquic_network_managed_wake's contract (one lane): non-allocating,
 * coalesced, MOQ_ERR_CLOSED when stop/terminal was observed first, and
 * an MOQ_OK racing stop_begin() may be absorbed by teardown. */
MOQ_API moq_result_t moq_wtquic_network_lane_wake(
    moq_wtquic_network_managed_lane_t *lane);

/* -- Connection views (strict pump-window accessors) ----------------- */

/* The connection's MoQ session. Pump-window only: NULL elsewhere. */
MOQ_API moq_session_t *moq_wtquic_network_managed_conn_session(
    moq_wtquic_network_managed_conn_t *conn);

/* The connection's wtquic attach adapter (for wtq_session access,
 * datagram/stream plumbing). Pump-window only: NULL elsewhere. */
MOQ_API moq_wtquic_conn_t *moq_wtquic_network_managed_conn_adapter(
    moq_wtquic_network_managed_conn_t *conn);

/* The lane that owns this connection (always lane 0). Pump-window
 * only: NULL elsewhere. */
MOQ_API moq_wtquic_network_managed_lane_t *moq_wtquic_network_managed_conn_lane(
    moq_wtquic_network_managed_conn_t *conn);

/* -- Off-domain-safe snapshots ------------------------------------ */

/* The negotiated MoQ version: 0 until known. */
MOQ_API moq_version_t moq_wtquic_network_managed_negotiated_version(
    const moq_wtquic_network_managed_t *m);

MOQ_API bool     moq_wtquic_network_managed_is_fatal(
    const moq_wtquic_network_managed_t *m);
MOQ_API uint64_t moq_wtquic_network_managed_fatal_code(
    const moq_wtquic_network_managed_t *m);
MOQ_API bool     moq_wtquic_network_managed_is_closed(
    const moq_wtquic_network_managed_t *m);
MOQ_API uint64_t moq_wtquic_network_managed_close_code(
    const moq_wtquic_network_managed_t *m);

/* The wtquic transport-error record snapshotted at the connection
 * terminal (first-causal, sealed by wtquic). Returns true when a
 * record was captured; classification is stable off-domain. */
MOQ_API bool moq_wtquic_network_managed_transport_error(
    const moq_wtquic_network_managed_t *m, wtq_transport_error_t *out);

/* -- Cross-thread signaling ---------------------------------------- */

/*
 * Submit an arbitrary closure to the domain (wtq_nw_conn_post,
 * directly): safe from any thread, including the domain itself.
 *
 *   - MOQ_OK means ACCEPTED: fn(ctx) runs on the domain EXACTLY ONCE,
 *     in submission order relative to every other accepted post, and
 *     always DEFERRED — never inline, even when posted from the
 *     domain.
 *   - Any error means REJECTED: fn never runs. MOQ_ERR_CLOSED once
 *     the stop began.
 *
 * Session/adapter access is legal inside fn (it is on the domain).
 */
MOQ_API moq_result_t moq_wtquic_network_managed_post(moq_wtquic_network_managed_t *m,
                                                void (*fn)(void *ctx),
                                                void *ctx);

/*
 * Schedule on_lane_pump on the domain via the facade's preallocated
 * doorbell: NON-ALLOCATING and infallible to call, COALESCED to
 * deliver. The exact semantics:
 *   - MOQ_ERR_CLOSED: stop/terminal was already observed before the
 *     facade attempted the ring — nothing was scheduled.
 *   - MOQ_OK with no concurrent stop: at least one deferred pump pass
 *     follows (bursts may share a pass; a wake from inside the current
 *     pump re-arms exactly one more).
 *   - MOQ_OK racing stop_begin(): the ring may be ABSORBED by teardown
 *     — MOQ_OK is not a delivery receipt across a stop race; no
 *     delivery is promised once stop wins the doorbell latch.
 *   In every case, no pump callback runs during or after on_stopped.
 * Safe from any thread.
 */
MOQ_API moq_result_t moq_wtquic_network_managed_wake(moq_wtquic_network_managed_t *m);

/* Block until activity or timeout: MOQ_OK if a pump ran (LEVEL-
 * RETAINED and COALESCED: a pump that landed before this call — even
 * long before — satisfies it immediately, any number of pumps since
 * the last consuming wait collapse into one MOQ_OK, and activity
 * between two waits is never lost), MOQ_DONE on
 * timeout, MOQ_ERR_CLOSED once terminal. TERMINAL means a session
 * outcome (fatal/clean close/pump_exit) or COMPLETED teardown — a
 * merely REQUESTED stop does not count: while teardown is in flight
 * wait() keeps waiting (MOQ_DONE on timeout), so a CLOSED result
 * after a stop means the facade is safe to destroy. timeout_us==0 is
 * a nonblocking check; UINT64_MAX waits indefinitely. Off-domain only
 * (MOQ_ERR_WRONG_STATE on the domain). */
MOQ_API moq_result_t moq_wtquic_network_managed_wait(moq_wtquic_network_managed_t *m,
                                                uint64_t timeout_us);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_WTQUIC_NETWORK_MANAGED_H */
