/*
 * publisher_conn_guard.h - connection-isolation decision for the singleton
 * picoquic publisher example. Example support ONLY: header-only, example-local,
 * not installed, not part of any public API.
 *
 * Security report finding #9 (MOQ-EXAMPLE-PICOQUIC-CONNECTION-CONFUSION): the
 * simple publisher is singleton state -- one session/adapter/publisher/track,
 * bound to the FIRST connection. Its callback must therefore drive the adapter
 * ONLY from that owning connection; a second connection's callbacks must never
 * be dispatched into the first connection's adapter (cross-connection state
 * confusion), and a second connection must be refused, not silently mixed in.
 *
 * This is a singleton example on purpose: one connection per process lifetime.
 * The owner is adopted once (at the first almost_ready/ready) and never handed
 * off -- a real multi-client publisher would need per-connection sessions,
 * adapters, publishers, tracks, and cleanup, which is out of scope here.
 *
 * The decision is factored into this pure classifier so it is unit-testable
 * without picoquic or a live network. `cnx`/`owner` are compared by identity
 * only; nothing is dereferenced.
 */
#ifndef MOQ_PICOQUIC_PUBLISHER_CONN_GUARD_H
#define MOQ_PICOQUIC_PUBLISHER_CONN_GUARD_H

#include <stdbool.h>

typedef enum {
    /* almost_ready/ready with no owner yet: create the session/adapter/
     * publisher/track and adopt this cnx as the owner. */
    PQ_PUB_CREATE,
    /* almost_ready/ready from the current owner again: benign repeat, return 0
     * (do NOT re-create, do NOT close). */
    PQ_PUB_OWNER_READY,
    /* non-ready callback from the owner: feed moq_pq_callback(). */
    PQ_PUB_DISPATCH,
    /* any callback from a DIFFERENT connection once owned: close it with
     * PICOQUIC_TRANSPORT_SERVER_BUSY and never feed the adapter. */
    PQ_PUB_CLOSE_BUSY,
    /* non-ready callback before any owner exists (a pre-owner handshake
     * event): return 0 -- there is no adapter to feed and no owner to refuse
     * against yet, so it is neither dispatched nor closed. */
    PQ_PUB_IGNORE,
} pq_pub_action_t;

/*
 * Classify one picoquic server callback for the singleton publisher.
 *   has_owner : an owning connection has been adopted (adapter created).
 *   is_ready  : the event is picoquic_callback_almost_ready or _ready.
 *   cnx       : the connection this callback is for.
 *   owner     : the adopted owner (meaningful only when has_owner).
 */
static inline pq_pub_action_t
pq_pub_classify(bool has_owner, bool is_ready,
                const void *cnx, const void *owner)
{
    if (is_ready) {
        if (!has_owner) return PQ_PUB_CREATE;
        return (cnx == owner) ? PQ_PUB_OWNER_READY : PQ_PUB_CLOSE_BUSY;
    }
    if (!has_owner) return PQ_PUB_IGNORE;
    return (cnx == owner) ? PQ_PUB_DISPATCH : PQ_PUB_CLOSE_BUSY;
}

#endif /* MOQ_PICOQUIC_PUBLISHER_CONN_GUARD_H */
