#ifndef MOQ_STREAM_ORDER_H
#define MOQ_STREAM_ORDER_H

#include "../session/action_meta.h"

#include <stdint.h>

/* Private scheduling snapshot: one current class per stream, for one parent
 * subscription/track. The caller decides which heads are schedulable and keeps
 * endpoint-held bytes represented until their class drains. No owner pointer
 * or action ownership is retained here. */
typedef struct moq_stream_order_node {
    moq_action_meta_t meta;
    uint32_t rank;

    /* Overwritten on every call; callers need initialize only meta. The
     * per-node edge_checks counter records relation evaluations, at most
     * 2 * count, without forming a potentially overflowing total n*n count. */
    struct {
        uint64_t edge_checks;
        uint32_t index;
        uint32_t low;
        uint32_t parent;
        uint32_t next;
        uint32_t stack_next;
        uint32_t component;
        uint32_t member_next;
        uint32_t members;
        uint32_t component_next;
    } _scratch;
} moq_stream_order_node_t;

/* Rank the directed preferences in D22 section 5.1.2. Only located SUBGROUP
 * and FILL nodes with the same nonzero owner and equal subscriber/publisher
 * priorities have edges. Parent group_order must be resolved ASCENDING or
 * DESCENDING and agree within that owner; fills also need a resolved
 * fill_group_order. Other modes, standalone FETCH and unlocated nodes are
 * isolated. Two fills have no direct edge; paths through subscription heads
 * can still relate them.
 *
 * meta is unchanged. rank is a zero-based longest-predecessor-path level in
 * the component DAG: every edge between different strongly connected
 * components increases rank, and every member of one component has the same
 * rank. Isolated nodes have rank zero; incomparable components can tie.
 * For count > 0, rank < count. Apply subscriber/publisher priorities before
 * rank, and the caller's fair tie-break after it. This function does not
 * select streams, perform transport admission, or pack/clamp endpoint keys.
 *
 * Iterative Tarjan traversal and a DAG pass each scan at most count
 * candidates per node: at most 2*n*n relation evaluations, O(n*n) total work,
 * and O(n) scratch entirely in nodes. No allocation, recursion, or global
 * state. All scratch is reset, so arrays may be reused after metadata changes.
 * count == 0 accepts NULL and writes nothing; otherwise nodes must reference
 * count writable, initialized metadata records. Private, not exported. */
#ifdef __cplusplus
extern "C" {
#endif

void moq_stream_order_rank(moq_stream_order_node_t *nodes, uint32_t count);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_STREAM_ORDER_H */
