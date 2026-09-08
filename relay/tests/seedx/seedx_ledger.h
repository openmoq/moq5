#ifndef MOQR_SEEDX_LEDGER_H
#define MOQR_SEEDX_LEDGER_H

/*
 * The independent reference model. It is updated ONLY from the operation
 * grammar — never from production snapshots — and its capacities come from
 * the rig's configuration constants. Child lifecycle facets are orthogonal,
 * because transport shutdown, binding ownership, terminal visibility, the
 * application acknowledgment and link reclamation genuinely overlap.
 *
 * Directed-pair credit identities, asserted after every operation:
 *   L1: occupancy + available == capacity
 *   L2: accepted_total - consumed_total == occupancy
 *   L3: unique_offered_total ==
 *         accepted_total + held_current + abandoned_total + superseded_total
 * A retry or re-refusal of a held offer is NOT a new unique offer: it moves
 * an entry between L3's terms, never grows the left side.
 */

#include <stdbool.h>
#include <stdint.h>

#define SEEDX_MAX_CHILDREN 16
#define SEEDX_MAX_PAIRS    4

typedef enum { SX_T_OPEN, SX_T_PEER_CLOSED, SX_T_SHUTDOWN } sx_transport_t;
typedef enum {
    SX_B_UNBOUND, SX_B_BOUND, SX_B_DETACH_PENDING, SX_B_DETACHED, SX_B_REFUSED
} sx_binding_t;
typedef enum { SX_TERM_NONE, SX_TERM_QUEUED, SX_TERM_OBSERVED } sx_terminal_t;

typedef struct sx_child {
    bool           used;
    uint8_t        lane;
    sx_transport_t transport;
    sx_binding_t   binding;
    sx_terminal_t  terminal;
    bool           acked;
    bool           linked;     /* still in the lane list                  */
    bool           established;
} sx_child_t;

typedef struct sx_pair {
    uint64_t capacity;         /* config constant, never a snapshot        */
    uint64_t occupancy;
    uint64_t unique_offered_total;
    uint64_t accepted_total;
    uint64_t consumed_total;
    uint64_t held_current;
    uint64_t abandoned_total;
    uint64_t superseded_total;
} sx_pair_t;

typedef struct sx_ledger {
    sx_child_t child[SEEDX_MAX_CHILDREN];
    sx_pair_t  pair[SEEDX_MAX_PAIRS];
    bool       owed_pump[4];   /* per lane                                 */
    bool       stopped;
} sx_ledger_t;

/* First violated identity, or 0. Identity ids: 1..3 as above; 4 = available
 * underflow (occupancy > capacity). */
static inline int
sx_pair_check(const sx_pair_t *p)
{
    if (p->occupancy > p->capacity) {
        return 4;
    }
    /* L1 is available-by-definition once occupancy <= capacity */
    if (p->accepted_total - p->consumed_total != p->occupancy) {
        return 2;
    }
    if (p->unique_offered_total != p->accepted_total + p->held_current +
                                       p->abandoned_total +
                                       p->superseded_total) {
        return 3;
    }
    return 0;
}

/* One offer (a PUSH): accepted into the channel if capacity remains, else
 * held by the owner. Returns true if accepted. */
static inline bool
sx_pair_offer(sx_pair_t *p)
{
    p->unique_offered_total++;
    if (p->occupancy < p->capacity) {
        p->occupancy++;
        p->accepted_total++;
        return true;
    }
    p->held_current++;
    return false;
}

/* One consume (a POP): returns credit. */
static inline bool
sx_pair_consume(sx_pair_t *p)
{
    if (p->occupancy == 0) {
        return false;
    }
    p->occupancy--;
    p->consumed_total++;
    return true;
}

/* One retry of a held offer: NOT a new unique offer. Accepted only if credit
 * is available; a re-refusal leaves it held. */
static inline bool
sx_pair_retry(sx_pair_t *p)
{
    if (p->held_current == 0) {
        return false;
    }
    if (p->occupancy < p->capacity) {
        p->held_current--;
        p->occupancy++;
        p->accepted_total++;
        return true;
    }
    return false; /* re-refusal: held_current unchanged */
}

static inline void
sx_pair_abandon(sx_pair_t *p)
{
    if (p->held_current > 0) {
        p->held_current--;
        p->abandoned_total++;
    }
}

static inline void
sx_pair_supersede(sx_pair_t *p)
{
    if (p->held_current > 0) {
        p->held_current--;
        p->superseded_total++;
    }
}

#endif /* MOQR_SEEDX_LEDGER_H */
