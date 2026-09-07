#ifndef MOQR_CLI_CONN_REAP_H
#define MOQR_CLI_CONN_REAP_H

#ifdef MOQR_DUAL_LISTENER
#include <moq/wtquic_msquic_managed.h>
#endif

/*
 * The one production retirement pass for managed connections. Every lane pump
 * calls it, so a change cannot reach one pump and miss another, and tests
 * exercise the same function the relay runs rather than a copy of its logic.
 *
 * A managed child is reclaimed only once the application has polled its
 * SESSION_CLOSED and acknowledged it, so a connection the binding no longer
 * owns still needs someone to transfer that terminal. Nothing else polls it:
 * the binding stops at detach, and a connection refused at admission was never
 * bound at all. This pass is that someone.
 */

#include <stdbool.h>
#include <stdint.h>

#include <moq/msquic_managed.h>

#include "../bind/moqr_bind.h"

/*
 * Per-connection relay state is a TAG in the adapter's conn_user slot — no
 * pointer-keyed maps, because a conn/session pointer value can be reused by a
 * successor connection.
 */
#define MOQR_CONN_OPENED ((void *)(uintptr_t)1)
#define MOQR_CONN_DEAD   ((void *)(uintptr_t)2)

/* What one pass did, for the pump's own reporting. */
typedef struct moqr_reap_stats {
    uint32_t acked;      /* terminal transferred and acknowledged            */
    uint32_t retained;   /* terminal not yet transferred; retried next pump  */
} moqr_reap_stats_t;

/*
 * Retire this lane's dead connections: discover bindings that detached, drain
 * the sessions the binding no longer owns, and acknowledge the terminals that
 * have transferred.
 *
 * Callers run it exactly once, AFTER the bind/shard step, so a binding this
 * callback's own step detached is retired in that same callback and never waits
 * on an unrelated wake. A connection whose terminal is still in flight —
 * refused at admission, or denied at setup — is retained and retried by the
 * next adapter-driven callback, which ends in this same pass.
 *
 * `st` may be NULL. Returns false when there is nothing valid to retire
 * against, or when the acknowledgment reported anything other than success or
 * not-yet-observed: those are lifetime contracts the relay cannot reason about,
 * so the caller fails closed rather than serving on.
 */
bool moqr_relay_reap_pass(moqr_bind_t *bind, moq_msquic_managed_lane_t *lane,
                          moqr_reap_stats_t *st);

#ifdef MOQR_DUAL_LISTENER
/*
 * The same retirement pass for the WebTransport facade.
 *
 * A separate function, and deliberately not a generic one: a lane handle is
 * only meaningful to the facade that produced it, and the two facades' lane
 * and connection types are unrelated. Keeping them apart at the type level is
 * what makes "acknowledge the terminal on the facade that owns it" a
 * compile-time property rather than a convention.
 */
bool moqr_relay_reap_pass_wt(moqr_bind_t *bind,
                             moq_wtquic_msquic_managed_lane_t *lane,
                             moqr_reap_stats_t *st);
#endif

#endif /* MOQR_CLI_CONN_REAP_H */
