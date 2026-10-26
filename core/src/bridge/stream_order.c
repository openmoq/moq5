#include "stream_order.h"

#include <moq/session.h>

static bool stream_order_resolved(uint8_t order)
{
    return order == MOQ_GROUP_ORDER_ASCENDING ||
           order == MOQ_GROUP_ORDER_DESCENDING;
}

/* A directed edge means x should precede y. Conflicting paths are resolved
 * by component ranking, never by using this relation as a heap comparator. */
static bool stream_order_edge(moq_stream_order_node_t *nodes,
                              uint32_t from, uint32_t to)
{
    const moq_action_meta_t *x = &nodes[from].meta;
    const moq_action_meta_t *y = &nodes[to].meta;
    nodes[from]._scratch.edge_checks++;

    if (from == to || !x->has_location || !y->has_location ||
        x->owner == 0 || x->owner != y->owner ||
        x->subscriber_priority != y->subscriber_priority ||
        x->publisher_priority != y->publisher_priority)
        return false;
    bool xf = x->mode == MOQ_SCHED_MODE_FILL;
    bool yf = y->mode == MOQ_SCHED_MODE_FILL;
    if ((!xf && x->mode != MOQ_SCHED_MODE_SUBGROUP) ||
        (!yf && y->mode != MOQ_SCHED_MODE_SUBGROUP) || (xf && yf))
        return false;
    if (!stream_order_resolved(x->group_order) ||
        x->group_order != y->group_order ||
        (xf && !stream_order_resolved(x->fill_group_order)) ||
        (yf && !stream_order_resolved(y->fill_group_order)))
        return false;

    if (x->group_id != y->group_id) {
        if (xf != yf) {
            const moq_action_meta_t *fill = xf ? x : y;
            if (fill->fill_group_order != fill->group_order)
                return !xf;
        }
        return x->group_order == MOQ_GROUP_ORDER_DESCENDING ?
               x->group_id > y->group_id : x->group_id < y->group_id;
    }
    if (xf != yf) return xf;
    return x->subgroup_id < y->subgroup_id;
}

static void stream_order_discover(moq_stream_order_node_t *nodes,
                                  uint32_t node, uint32_t parent,
                                  uint32_t index, uint32_t *stack)
{
    nodes[node]._scratch.index = index;
    nodes[node]._scratch.low = index;
    nodes[node]._scratch.parent = parent;
    nodes[node]._scratch.stack_next = *stack;
    *stack = node;
}

void moq_stream_order_rank(moq_stream_order_node_t *nodes, uint32_t count)
{
    /* UINT32_MAX is outside the valid index range even when count is
     * UINT32_MAX. Discovery/candidate counters can reach count, never pass
     * it; no sum of edge counts or squared count is formed here. */
    for (uint32_t i = 0; i < count; i++) {
        nodes[i].rank = 0;
        nodes[i]._scratch.edge_checks = 0;
        nodes[i]._scratch.index = UINT32_MAX;
        nodes[i]._scratch.low = UINT32_MAX;
        nodes[i]._scratch.parent = UINT32_MAX;
        nodes[i]._scratch.next = 0;
        nodes[i]._scratch.stack_next = UINT32_MAX;
        nodes[i]._scratch.component = UINT32_MAX;
        nodes[i]._scratch.member_next = UINT32_MAX;
        nodes[i]._scratch.members = UINT32_MAX;
        nodes[i]._scratch.component_next = UINT32_MAX;
    }

    uint32_t index = 0, stack = UINT32_MAX, components = UINT32_MAX;
    for (uint32_t root = 0; root < count; root++) {
        if (nodes[root]._scratch.index != UINT32_MAX) continue;
        stream_order_discover(nodes, root, UINT32_MAX, index++, &stack);
        uint32_t current = root;
        while (current != UINT32_MAX) {
            moq_stream_order_node_t *node = &nodes[current];
            if (node->_scratch.next < count) {
                uint32_t next = node->_scratch.next++;
                if (!stream_order_edge(nodes, current, next)) continue;
                if (nodes[next]._scratch.index == UINT32_MAX) {
                    stream_order_discover(nodes, next, current, index++, &stack);
                    current = next;
                } else if (nodes[next]._scratch.component == UINT32_MAX &&
                           nodes[next]._scratch.index < node->_scratch.low) {
                    /* A visited node without a component is on the SCC
                     * stack, including finished descendants and siblings. */
                    node->_scratch.low = nodes[next]._scratch.index;
                }
                continue;
            }

            uint32_t parent = node->_scratch.parent;
            if (node->_scratch.low == node->_scratch.index) {
                uint32_t member;
                do {
                    member = stack;
                    stack = nodes[member]._scratch.stack_next;
                    nodes[member]._scratch.component = current;
                    nodes[member]._scratch.member_next = node->_scratch.members;
                    node->_scratch.members = member;
                } while (member != current);
                /* Tarjan completes sinks first. Prepending components
                 * produces a topological source-first list, across roots
                 * as well as within a traversal. */
                node->_scratch.component_next = components;
                components = current;
            }
            if (parent != UINT32_MAX &&
                node->_scratch.low < nodes[parent]._scratch.low)
                nodes[parent]._scratch.low = node->_scratch.low;
            current = parent;
        }
    }

    /* Each component has its final longest-predecessor-path rank when
     * visited. Duplicate edges are harmless and need no stored adjacency
     * lists or indegree sums. The two passes each scan n candidates per
     * node, with at most 2*n edge checks in any one uint64_t counter. */
    for (uint32_t component = components; component != UINT32_MAX;
         component = nodes[component]._scratch.component_next) {
        for (uint32_t member = nodes[component]._scratch.members;
             member != UINT32_MAX; member = nodes[member]._scratch.member_next) {
            for (uint32_t next = 0; next < count; next++) {
                uint32_t target = nodes[next]._scratch.component;
                if (target == component || !stream_order_edge(nodes, member, next))
                    continue;
                /* A source with an outgoing DAG edge has rank <= count-2,
                 * so incrementing its rank cannot overflow uint32_t. */
                uint32_t rank = nodes[component].rank + 1;
                if (rank > nodes[target].rank) nodes[target].rank = rank;
            }
        }
    }
    for (uint32_t i = 0; i < count; i++)
        nodes[i].rank = nodes[nodes[i]._scratch.component].rank;
}
