#ifndef MOQRELAY_PLACEMENT_H
#define MOQRELAY_PLACEMENT_H

/*
 * Placement seam (cluster constitution §21.1).
 *
 * One pluggable function answers "which shard owns this track" today and
 * "which node" later — single-node routing is a cluster of one, not a
 * different product.
 *
 * -- Purity contract (load-bearing; violating it breaks seed-replay) --
 *
 * A placement function is a PURE function of (key, state):
 *
 *   - It never reads live counters, clocks, atomics, globals, or any state
 *     outside its two arguments.
 *   - Everything it may consider — membership, load figures, operator pins,
 *     policy parameters — arrives inside moqr_placement_state_t, which is
 *     built exclusively from epoch-stamped control inputs. Load-aware
 *     placement is expressed by feeding load SNAPSHOTS through the control
 *     plane, never by sampling reality mid-decision.
 *   - Same key + same state => same owner, on every replay, on every node.
 *
 * This is why there is deliberately no `void *ctx` in the signature: a
 * context pointer is where live state sneaks in. Policy parameters travel
 * in the state's params block instead, where they are epoch-stamped and
 * replayable like everything else.
 *
 * The default policy (rendezvous hash over the local shard set) ships with
 * the track table.
 */

#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Placement key: the canonical full track name plus its precomputed hash.
 * Bytes are BORROWED for the duration of the placement call only. */
typedef struct moqr_place_key {
    moq_bytes_t full_track_name;  /* canonical namespace+name encoding */
    uint64_t    hash;             /* precomputed by the caller (one hash,
                                   * many consumers: table, placement) */
} moqr_place_key_t;

/* Epoch-stamped placement inputs. Snapshot semantics: the core swaps in a
 * new state only via a control input that bumps node/shard epochs; a
 * placement decision is attributable to exactly one state generation. */
typedef struct moqr_placement_state {
    uint32_t      struct_size;
    moqr_epochs_t epochs;         /* node/shard epochs of this snapshot     */
    uint16_t      shard_count;    /* local shards eligible for ownership    */
    uint16_t      node_count;     /* cluster nodes (v1: 1)                  */
    const void   *params;         /* policy parameters + membership + load
                                   * snapshot, opaque to the core; owned by
                                   * the control plane, epoch-stamped */
    size_t        params_size;
} moqr_placement_state_t;

/* Owner of a key under a given state. shard is an index into the local
 * shard set when node == the local node; interpretation of node ids is a
 * control-plane concern (v1: always node 0). */
typedef struct moqr_owner {
    uint32_t node;
    uint32_t shard;
} moqr_owner_t;

typedef moqr_owner_t (*moqr_place_fn)(const moqr_place_key_t *key,
                                      const moqr_placement_state_t *state);

#ifdef __cplusplus
}
#endif

#endif /* MOQRELAY_PLACEMENT_H */
