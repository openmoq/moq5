/*
 * Private test seam for white-box coverage of moq_wtquic_msquic_managed. These
 * symbols exist only in a build with MOQ_WTQ_MM_TESTING defined (the white-box
 * test recompiles the facade source with it); they are NEVER compiled into the
 * production/installed library and never exported.
 */
#ifndef WTQUIC_MSQUIC_MANAGED_TEST_INTERNAL_H
#define WTQUIC_MSQUIC_MANAGED_TEST_INTERNAL_H

#include <moq/wtquic_msquic_managed.h>

#include <wtquic/wtquic_msquic.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Make the nth (1-based) pthread_*_init call inside create() fail; 0 = off. */
void moq_wtquic_msquic_managed_test_fail_pthread(int nth);

/*
 * Replace the client transport primitives so the guard-held publication path
 * runs without a real MsQuic connection. connect is called (holding the lane
 * guard) in place of wtq_msquic_client_connect; release is used at reap in
 * place of wtq_session_release for whatever handle connect returned. Pass
 * (NULL, NULL) to restore the real transport.
 */
void moq_wtquic_msquic_managed_test_set_transport(
    wtq_result_t (*connect)(wtq_msquic_env_t *,
                            const wtq_msquic_client_cfg_t *,
                            wtq_session_t **),
    void (*release)(wtq_session_t *));

/* Pure WT-Protocol negotiation: true + *out for a recognized, offered token
 * (or the draft-16 legacy fallback); false (building no session) otherwise. */
bool moq_wtquic_msquic_managed_test_negotiate(
    const moq_wtquic_msquic_managed_t *m, const char *tok, size_t len,
    moq_version_t *out);

/* Drive the first-cause terminal latches (read back via the public
 * is_fatal / fatal_code / is_closed / close_code accessors). */
void moq_wtquic_msquic_managed_test_latch_fatal(
    moq_wtquic_msquic_managed_t *m, uint64_t code);
void moq_wtquic_msquic_managed_test_latch_closed(
    moq_wtquic_msquic_managed_t *m, uint64_t code);

/* Raise the level-retained activity latch (as establishment does), to drive the
 * wait() state machine without a transport. */
void moq_wtquic_msquic_managed_test_set_activity(
    moq_wtquic_msquic_managed_t *m);

/* Make server create() skip the real listener (env + pool only), so admission
 * can be driven white-box without a certificate or the network. */
void moq_wtquic_msquic_managed_test_no_listener(bool on);

/* The webtransport_profile last forwarded onto a server listener cfg (set in
 * mm_server_start). Proves the SERVER path forwards m->webtransport_profile.
 * 0xFFFFFFFF until a server start has run. */
uint32_t moq_wtquic_msquic_managed_test_last_listener_profile(void);

/* Drive server admission callbacks directly. */
bool moq_wtquic_msquic_managed_test_accept(
    moq_wtquic_msquic_managed_t *m, void **out_conn);
void moq_wtquic_msquic_managed_test_abandon(
    moq_wtquic_msquic_managed_t *m, void *conn);
/* The lane index a child was placed on (UINT32_MAX if unplaced). */
uint32_t moq_wtquic_msquic_managed_test_conn_lane_index(void *conn);

/*
 * Server-side: observe the first established child under its OWNING lane guard,
 * so version/adapter/ws are read serialized against the establishment callback
 * that set them. Returns true (and fills the non-NULL out params) when a child
 * has an established MoQ session; false when none has established yet. Proves
 * the managed server completed establishment — not merely admission.
 */
bool moq_wtquic_msquic_managed_test_server_child(
    const moq_wtquic_msquic_managed_t *m, moq_version_t *out_version,
    bool *out_has_adapter, bool *out_has_ws);

/*
 * Drive an admitted child through the real on_established callback with the
 * given subprotocol token (a fake sentinel session stands in for the transport;
 * the release seam must be installed so no real handle is touched). An
 * unrecognized token exercises the child-local establishment-failure branch; a
 * recognized+offered token would proceed to build a session, so only drive one
 * on a child that has already had quiescence requested (which pre-empts the
 * build). NUL-terminated token; NULL/empty drives the legacy path.
 */
void moq_wtquic_msquic_managed_test_deliver_established(
    moq_wtquic_msquic_managed_t *m, void *conn, const char *tok);

/* Request quiescence of one admitted child (sets the persistent quiesce flag
 * and closes an existing ws), as global teardown's pre-barrier scan does. */
void moq_wtquic_msquic_managed_test_quiesce_conn(
    moq_wtquic_msquic_managed_t *m, void *conn);

/* Run one explicit pump cycle for a lane (opens the window, runs on_lane_pump,
 * services each adapter), as global teardown's final
 * pump does. Returns the pump callback's rc. */
int moq_wtquic_msquic_managed_test_pump_lane(
    moq_wtquic_msquic_managed_t *m, uint32_t lane_index);

/* Read the lane's currently-published next deadline (lane->deadline_us), so a
 * test can assert the app-deadline fold without racing the worker's timed wait. */
uint64_t moq_wtquic_msquic_managed_test_lane_deadline_us(
    moq_wtquic_msquic_managed_t *m, uint32_t lane_index);

/* Mark one child's transport as quiesced, as the on_transport_quiesced callback
 * does (marks-only — releases nothing). */
void moq_wtquic_msquic_managed_test_mark_quiesced(
    moq_wtquic_msquic_managed_t *m, void *conn);

/* Read a child's three reap-gate conditions:
 * logical_terminal && transport_quiesced, plus -- for a session-backed child --
 * terminal observation and acknowledgment. */
void moq_wtquic_msquic_managed_test_conn_gate(
    void *conn, bool *out_logical_terminal, bool *out_transport_quiesced,
    bool *out_session_backed);

/* Whether a child currently has a close latched (set by conn_close inside a
 * pump, cleared when the pump executes it). */
bool moq_wtquic_msquic_managed_test_conn_close_pending(void *conn);

/* Whether a child slot is currently reserved (true between accept and reap). */
bool moq_wtquic_msquic_managed_test_conn_in_use(void *conn);

/* Seed a child's per-generation state + a retained wtquic ref (ws with
 * owns_ws_ref), so a test can prove exactly-once release at reap and a clean
 * reset when the slot is re-admitted. Session stays NULL (no real handle). */
void moq_wtquic_msquic_managed_test_seed_conn(
    void *conn, wtq_session_t *ws, moq_version_t version, void *user,
    bool quiesce_requested, bool close_pending);

/* Read a child's per-generation state directly (not pump-confined), to assert
 * it is cleanly reset after re-admission. */
void moq_wtquic_msquic_managed_test_conn_gen_state(
    void *conn, moq_version_t *out_version, void **out_user,
    bool *out_quiesce_requested, bool *out_close_pending);

/* Replace the deferred-close operation so a test can observe the transport
 * close the pump runs (exactly-once, the code, outside the TLS window). Pass
 * NULL to restore the real wtq_session_close. */
void moq_wtquic_msquic_managed_test_set_close_op(
    void (*op)(wtq_session_t *, uint32_t));

/* True while a pump window is open on THIS thread (for a close-op probe to prove
 * it runs outside the window). */
bool moq_wtquic_msquic_managed_test_in_pump_window(void);

/* Force a child's established handles (ws + MoQ session) to the given sentinels,
 * so the deferred-close-after-establishment path can be driven without a real
 * transport. Pass (NULL, NULL) to un-establish before teardown. */
void moq_wtquic_msquic_managed_test_set_conn_established(
    void *conn, wtq_session_t *ws, void *session);

/* Install a one-shot hook a doorbell worker fires UNDER bell_mu at the idle
 * check-to-wait boundary (right before pthread_cond_wait), so a test can arm the
 * bell exactly there and prove the wake is not lost. Pass NULL to clear. */
void moq_wtquic_msquic_managed_test_set_prewait_hook(void (*hook)(void));

/* Fire the transport-activity hook for a connection, as a MsQuic worker would
 * after a transport event — arms the connection's lane doorbell. */
void moq_wtquic_msquic_managed_test_fire_hook(void *conn);

/* Force a lane's next deadline to duration_us from now and wake its worker, so a
 * deadline sweep can be driven without a real session. */
void moq_wtquic_msquic_managed_test_set_deadline(void *lane, uint64_t duration_us);

/* Replace the pump's adapter service passes (phase 0 = pre-callback, 1 = post),
 * so a test can prove the due path services before on_lane_pump without a real
 * adapter. NULL restores real servicing. */
void moq_wtquic_msquic_managed_test_set_service_op(
    void (*op)(void *conn, int phase));

/* Replace the pump's per-connection next-deadline query, so a test drives the
 * deadline arming/recompute/rearm path deterministically. NULL restores the real
 * moq_session_next_deadline_us. */
void moq_wtquic_msquic_managed_test_set_next_deadline_op(
    uint64_t (*op)(void *conn));

/* Install a hook the live-reap loop fires right after publishing a slot free, so
 * a test can reassign that slot mid-traversal. NULL to clear. */
void moq_wtquic_msquic_managed_test_set_after_reap(void (*op)(void *conn));

/* The client's published wtquic session handle (NULL pre-publish / post-reap). */
void *moq_wtquic_msquic_managed_test_client_ws(
    const moq_wtquic_msquic_managed_t *m);

/* A connection's published handle, read from the connection directly (usable
 * from a connect-seam probe, where cfg->user is the connection). */
void *moq_wtquic_msquic_managed_test_conn_ws(
    const moq_wtquic_msquic_managed_conn_t *conn);

/* Deliver on_draining with a forced non-NULL adapter (exercises the NULL
 * on_draining member of the real attach table). Must not crash. */
void moq_wtquic_msquic_managed_test_deliver_draining(
    moq_wtquic_msquic_managed_t *m);

/* Build the env config the client opens with, to assert allocator bridging and
 * idle-timeout forwarding into the tuning. */
void moq_wtquic_msquic_managed_test_fill_env_cfg(
    moq_wtquic_msquic_managed_t *m, wtq_msquic_env_cfg_t *out);

/* Read back the retained (owned-copy) inputs to prove deep copies. */
const char *moq_wtquic_msquic_managed_test_host(
    const moq_wtquic_msquic_managed_t *m);
size_t moq_wtquic_msquic_managed_test_proto_count(
    const moq_wtquic_msquic_managed_t *m);
const char *moq_wtquic_msquic_managed_test_proto(
    const moq_wtquic_msquic_managed_t *m, size_t i);
uint32_t moq_wtquic_msquic_managed_test_lane_count(
    const moq_wtquic_msquic_managed_t *m);



/* Drive a child's transport-terminal latch directly, so a synthetic child that
 * was given a session can reach a SESSION-BACKED terminal (the establishment
 * seam with a non-MoQ protocol produces a pre-session one). */
void moq_wtquic_msquic_managed_test_mark_logical_terminal(
    moq_wtquic_msquic_managed_t *m, void *conn);

/* Inject the session-terminal OBSERVED fact for a synthetic child (one with no
 * real adapter/session behind it), mirroring test_mark_quiesced for the
 * transport fact. */
void moq_wtquic_msquic_managed_test_mark_terminal_observed(
    moq_wtquic_msquic_managed_t *m, void *conn);

/* Read the per-child terminal facts. */
bool moq_wtquic_msquic_managed_test_conn_terminal_observed(
    const moq_wtquic_msquic_managed_conn_t *conn);
bool moq_wtquic_msquic_managed_test_conn_acked(
    const moq_wtquic_msquic_managed_conn_t *conn);
/* Snapshot the facade's terminal accounting under its mutex. Each name states
 * the exact fact counted: a TRANSPORT terminal on a session-backed child is not
 * "the session terminal was enqueued" and not "it was observed". */
void moq_wtquic_msquic_managed_test_terminal_counters(
    moq_wtquic_msquic_managed_t *m,
    uint64_t *out_transport_terminal_session_backed, uint64_t *out_ack_accepted,
    uint64_t *out_children_reaped, uint64_t *out_accepts_refused_terminal_held,
    uint32_t *out_terminal_held_now);

#ifdef __cplusplus
}
#endif

#endif /* WTQUIC_MSQUIC_MANAGED_TEST_INTERNAL_H */
