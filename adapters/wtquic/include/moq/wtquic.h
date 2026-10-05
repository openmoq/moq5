#ifndef MOQ_WTQUIC_H
#define MOQ_WTQUIC_H

/*
 * MoQ over wtquic (WebTransport over HTTP/3), backend-neutral.
 *
 * Attach-style prototype: the caller owns both the moq_session_t and
 * the wtquic session; this adapter is the connective tissue — it
 * implements the transport-bridge endpoint ops on wtquic's public API
 * and feeds wtquic's session events into the bridge. It names only core
 * wtquic types, never a transport backend — which backend a session runs
 * on (MsQuic, Network.framework, …) is the caller's choice.
 *
 * WIRING
 *   1. Create the moq_session_t and a bootstrap callback context.
 *   2. Create the wtquic session with bootstrap callbacks. In established,
 *      set cfg.wt_session to that session, create the adapter, then forward
 *      established and subsequent events through moq_wtquic_conn_events()
 *      with the adapter as their user argument. Keep the bootstrap context
 *      alive until provider quiescence. Handle pre-establishment failure
 *      without an adapter; qualification failure must close the WT session.
 *   3. Everything else happens inside wtquic's callbacks: the adapter
 *      feeds the bridge, services it, and then invokes the hook — the
 *      application's slot to poll moq session events and drive the
 *      session, on the transport thread, which is the only thread that
 *      may touch the moq session.
 *
 * SCOPE (prototype)
 *   One WebTransport session per conn (bind a fresh conn per accepted
 *   session). No datagram publishing capability yet (inbound datagrams
 *   are fed through). No idle deadline tick: sessions relying on
 *   timer-driven closes (GOAWAY drain) need external service() calls.
 *
 * LIFETIME
 *   The moq session must outlive the conn. Destroy the conn only after
 *   the wtquic side is fully torn down (after wtq_msquic_env_close
 *   returns, or the session's terminal event when no more events can
 *   fire) — the conn is the user context of live wtquic callbacks.
 */

#include <moq/session.h>

#include <wtquic/wtquic.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct moq_wtquic_conn moq_wtquic_conn_t;

/* Fires on the transport thread after the adapter serviced the bridge
 * for an event batch; poll moq session events and drive the session
 * from here (the adapter services again afterwards). */
typedef void (*moq_wtquic_hook_fn)(moq_wtquic_conn_t *conn, void *user);

typedef struct moq_wtquic_conn_cfg {
    uint32_t struct_size;
    const moq_alloc_t *alloc;  /* required; copied */
    moq_session_t *session;    /* required; NOT owned; must outlive */
    moq_wtquic_hook_fn hook;   /* optional */
    void *hook_user;
    /* Required qualified session, borrowed; create in its established callback
     * before forwarding events. Only FLOW_CONTROLLED receive contracts with
     * a callback quantum <=65535 are supported. The session/callback domain
     * must remain valid until provider quiescence and adapter destruction. */
    wtq_session_t *wt_session;
} moq_wtquic_conn_cfg_t;

MOQ_API void moq_wtquic_conn_cfg_init_sized(moq_wtquic_conn_cfg_t *cfg,
                                            size_t size);

MOQ_API moq_result_t moq_wtquic_conn_create(
    const moq_wtquic_conn_cfg_t *cfg, moq_wtquic_conn_t **out);
MOQ_API void moq_wtquic_conn_destroy(moq_wtquic_conn_t *conn);

/* Forward qualified-session events through this table with the conn as user.
 * Before creation, use the bootstrap callbacks described above. */
MOQ_API const wtq_session_events_t *moq_wtquic_conn_events(void);

MOQ_API moq_session_t *moq_wtquic_conn_session(moq_wtquic_conn_t *conn);

/* Drive one adapter service pass NOW (transport thread only): feed any
 * pending terminals, service the bridge (flushing queued session actions
 * to wtquic), run the hook, and service again. The adapter does this
 * around every transport event; call it after driving the session from
 * OUTSIDE the hook (e.g. a posted closure or a managed facade's
 * wake-initiated pump) so queued control messages reach the wire without
 * waiting for the next transport event. Reentrancy-safe: from inside the
 * hook it coalesces into the running pass instead of recursing. */
MOQ_API void moq_wtquic_conn_service(moq_wtquic_conn_t *conn);
/* The immutable WebTransport session binding supplied at creation. */
MOQ_API wtq_session_t *moq_wtquic_conn_wtq_session(moq_wtquic_conn_t *conn);

/* The bridge went fatal (setup failure, protocol error, refused). */
MOQ_API bool moq_wtquic_conn_is_fatal(const moq_wtquic_conn_t *conn);
/* The transport closed cleanly. */
MOQ_API bool moq_wtquic_conn_is_closed(const moq_wtquic_conn_t *conn);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_WTQUIC_H */
