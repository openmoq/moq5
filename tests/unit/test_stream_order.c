/* Pure snapshot ranking: D22 fill/subgroup preferences, SCC collapse, and
 * bounded iterative work. Transport admission and fair service are callers'
 * responsibilities and are deliberately absent from these fixtures. */
#include "../../core/src/bridge/stream_order.h"

#include <moq/session.h>

#include "test_support.h"

#include <string.h>

static int failures;

enum {
    ASC = MOQ_GROUP_ORDER_ASCENDING,
    DESC = MOQ_GROUP_ORDER_DESCENDING,
    TEST_NODE_CAP = 256
};

static void node_init(moq_stream_order_node_t *node, uint8_t mode,
                      uint64_t group, uint64_t subgroup,
                      uint8_t parent_order, uint8_t fill_order)
{
    /* Rank and scratch deliberately start dirty; only meta is input. */
    memset(node, 0xa5, sizeof(*node));
    memset(&node->meta, 0, sizeof(node->meta));
    node->meta.deadline_us = UINT64_MAX;
    node->meta.owner = 7;
    node->meta.group_id = group;
    node->meta.subgroup_id = subgroup;
    node->meta.subscriber_priority = 128;
    node->meta.publisher_priority = 128;
    node->meta.mode = mode;
    node->meta.group_order = parent_order;
    node->meta.fill_group_order = fill_order;
    node->meta.has_location = true;
}

static void check_ranks(const char *name, moq_stream_order_node_t *nodes,
                        const uint32_t *expected, uint32_t count)
{
    unsigned char saved[TEST_NODE_CAP][sizeof(moq_action_meta_t)];
    MOQ_TEST_CHECK(count <= TEST_NODE_CAP);
    if (count > TEST_NODE_CAP) return;
    for (uint32_t i = 0; i < count; i++)
        memcpy(saved[i], &nodes[i].meta, sizeof(nodes[i].meta));

    int before = failures;
    moq_stream_order_rank(nodes, count);
    uint64_t checks = 0;
    for (uint32_t i = 0; i < count; i++) {
        if (nodes[i].rank != expected[i]) {
            fprintf(stderr, "FAIL: %s node %u: rank %u, expected %u\n",
                    name, (unsigned)i, (unsigned)nodes[i].rank,
                    (unsigned)expected[i]);
            failures++;
        }
        MOQ_TEST_CHECK(nodes[i].rank < count);
        MOQ_TEST_CHECK(memcmp(saved[i], &nodes[i].meta, sizeof(nodes[i].meta)) == 0);
        /* Every node scans n candidates during SCC discovery. A second
         * scan may skip relation evaluation for same-component targets. */
        MOQ_TEST_CHECK(nodes[i]._scratch.edge_checks >= count);
        MOQ_TEST_CHECK(nodes[i]._scratch.edge_checks <= 2 * (uint64_t)count);
        checks += nodes[i]._scratch.edge_checks;
    }
    /* count <= 256 here, so neither the sum nor this product can wrap. */
    MOQ_TEST_CHECK(checks <= 2 * (uint64_t)count * count);
    if (failures != before) fprintf(stderr, "  fixture: %s\n", name);
}

static bool next_permutation(uint32_t *order, uint32_t count)
{
    if (count < 2) return false;
    uint32_t pivot = count - 1;
    while (pivot > 0 && order[pivot - 1] >= order[pivot]) pivot--;
    if (pivot == 0) return false;
    uint32_t next = count - 1;
    while (order[next] <= order[pivot - 1]) next--;
    uint32_t tmp = order[pivot - 1];
    order[pivot - 1] = order[next];
    order[next] = tmp;
    for (uint32_t left = pivot, right = count - 1; left < right; left++, right--) {
        tmp = order[left];
        order[left] = order[right];
        order[right] = tmp;
    }
    return true;
}

static void check_permutations(const char *name,
                               const moq_stream_order_node_t *base,
                               const uint32_t *expected, uint32_t count)
{
    moq_stream_order_node_t nodes[5];
    uint32_t order[5], ranks[5];
    MOQ_TEST_CHECK(count <= 5);
    if (count > 5) return;
    for (uint32_t i = 0; i < count; i++) order[i] = i;
    do {
        for (uint32_t i = 0; i < count; i++) {
            memcpy(&nodes[i], &base[order[i]], sizeof(nodes[i]));
            ranks[i] = expected[order[i]];
        }
        check_ranks(name, nodes, ranks, count);
    } while (next_permutation(order, count));
}

static void test_count_boundaries(void)
{
    moq_stream_order_node_t nodes[3];
    unsigned char saved[sizeof(nodes)];
    for (uint32_t i = 0; i < 3; i++)
        node_init(&nodes[i], MOQ_SCHED_MODE_SUBGROUP, i, 0, ASC, ASC);
    memcpy(saved, nodes, sizeof(nodes));
    moq_stream_order_rank(NULL, 0);
    moq_stream_order_rank(nodes, 0);
    MOQ_TEST_CHECK(memcmp(saved, nodes, sizeof(nodes)) == 0);
    const uint32_t one[] = {0};
    check_ranks("one node", nodes, one, 1);
    MOQ_TEST_CHECK(memcmp(saved + sizeof(nodes[0]), &nodes[1],
                          2 * sizeof(nodes[0])) == 0);
    check_ranks("one node repeated", nodes, one, 1);
}

static void test_cycles_and_reuse(void)
{
    const uint8_t orders[] = {ASC, DESC};
    const uint32_t cycle[] = {0, 0, 0};
    const uint32_t acyclic[] = {1, 2, 0};
    const uint32_t no_fill_edge[] = {0, 1, 0};
    for (uint32_t direction = 0; direction < 2; direction++) {
        uint8_t parent = orders[direction], opposite = orders[1 - direction];
        uint64_t first = direction == 0 ? 1 : 2;
        uint64_t second = direction == 0 ? 2 : 1;
        moq_stream_order_node_t nodes[3];
        node_init(&nodes[0], MOQ_SCHED_MODE_SUBGROUP, first, 0, parent, parent);
        node_init(&nodes[1], MOQ_SCHED_MODE_SUBGROUP, second, 0, parent, parent);
        node_init(&nodes[2], MOQ_SCHED_MODE_FILL, first, 0, parent, opposite);
        /* Earlier subscription -> later subscription -> fill -> earlier
         * subscription, in both parent directions. Varying array order
         * exercises every DFS start and neighbor enumeration. */
        check_permutations(direction == 0 ? "ASC cycle" : "DESC cycle",
                           nodes, cycle, 3);
        check_ranks("cycle before reuse", nodes, cycle, 3);

        nodes[2].meta.fill_group_order = parent;
        check_ranks("cycle split by matching order", nodes, acyclic, 3);
        nodes[2].meta.fill_group_order = opposite;
        nodes[2].meta.has_location = false;
        check_ranks("cycle split by location removal", nodes, no_fill_edge, 3);
        nodes[2].meta.has_location = true;
        nodes[2].meta.publisher_priority = 0;
        check_ranks("cycle split by publisher priority", nodes, no_fill_edge, 3);
        nodes[2].meta.publisher_priority = 128;
        nodes[2].meta.subscriber_priority = 255;
        check_ranks("cycle split by subscriber priority", nodes, no_fill_edge, 3);
        nodes[2].meta.subscriber_priority = 128;
        check_ranks("cycle restored without scratch reset", nodes, cycle, 3);
    }
}

static void test_external_edges(void)
{
    const uint8_t orders[] = {ASC, DESC};
    const uint32_t expected[] = {0, 1, 1, 1, 2};
    for (uint32_t direction = 0; direction < 2; direction++) {
        uint8_t parent = orders[direction], opposite = orders[1 - direction];
        moq_stream_order_node_t nodes[5];
        uint64_t groups[] = {0, 1, 2, 1, 3};
        if (direction != 0)
            for (uint32_t i = 0; i < 5; i++) groups[i] = 3 - groups[i];
        for (uint32_t i = 0; i < 3; i++)
            node_init(&nodes[i], MOQ_SCHED_MODE_SUBGROUP, groups[i], 0,
                      parent, parent);
        node_init(&nodes[3], MOQ_SCHED_MODE_FILL, groups[3], 0, parent, opposite);
        node_init(&nodes[4], MOQ_SCHED_MODE_FILL, groups[4], 0, parent, parent);
        /* A -> {B,C,D cycle} -> E. D and E are two fills with no direct
         * edge, yet B/C -> E must survive collapse. Test all 120 layouts
         * to cover edges into already completed components as well. */
        check_permutations(direction == 0 ? "ASC external edges" : "DESC external edges",
                           nodes, expected, 5);
    }
}

static void test_group_and_subgroup_boundaries(void)
{
    moq_stream_order_node_t nodes[4];
    node_init(&nodes[0], MOQ_SCHED_MODE_SUBGROUP, UINT64_MAX, UINT64_MAX, ASC, ASC);
    node_init(&nodes[1], MOQ_SCHED_MODE_SUBGROUP, 0, UINT64_MAX, ASC, ASC);
    node_init(&nodes[2], MOQ_SCHED_MODE_SUBGROUP, 0, 0, ASC, ASC);
    node_init(&nodes[3], MOQ_SCHED_MODE_SUBGROUP, UINT64_MAX, 0, ASC, ASC);
    const uint32_t ascending[] = {3, 1, 0, 2};
    const uint32_t descending[] = {1, 3, 2, 0};
    check_permutations("ASC group/subgroup boundaries", nodes, ascending, 4);
    for (uint32_t i = 0; i < 4; i++) nodes[i].meta.group_order = DESC;
    check_permutations("DESC group/subgroup boundaries", nodes, descending, 4);

    node_init(&nodes[0], MOQ_SCHED_MODE_SUBGROUP, 7, 9, ASC, ASC);
    node_init(&nodes[1], MOQ_SCHED_MODE_SUBGROUP, 7, 9, ASC, ASC);
    nodes[0].meta.object_id = UINT64_MAX;
    nodes[1].meta.deadline_us = 0;
    const uint32_t tied[] = {0, 0};
    check_permutations("object/deadline do not order subgroup ties", nodes, tied, 2);
}

static void test_fill_pairs(void)
{
    const struct {
        uint8_t parent_order;
        uint8_t fill_order;
        uint64_t sub_group;
        uint64_t fill_group;
        bool fill_first;
    } cases[] = {
        {ASC, ASC,  0,          0,          true},
        {ASC, DESC, 0,          0,          true},
        {DESC, ASC, 0,          0,          true},
        {DESC, DESC, 0,         0,          true},
        {ASC, ASC,  0,          UINT64_MAX, false},
        {ASC, ASC,  UINT64_MAX, 0,          true},
        {ASC, DESC, 0,          UINT64_MAX, false},
        {ASC, DESC, UINT64_MAX, 0,          false},
        {DESC, ASC, 0,          UINT64_MAX, false},
        {DESC, ASC, UINT64_MAX, 0,          false},
        {DESC, DESC, 0,         UINT64_MAX, true},
        {DESC, DESC, UINT64_MAX, 0,         false}
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        moq_stream_order_node_t nodes[2];
        node_init(&nodes[0], MOQ_SCHED_MODE_SUBGROUP, cases[i].sub_group, 0,
                  cases[i].parent_order, cases[i].parent_order);
        node_init(&nodes[1], MOQ_SCHED_MODE_FILL, cases[i].fill_group, UINT64_MAX,
                  cases[i].parent_order, cases[i].fill_order);
        uint32_t expected[] = {cases[i].fill_first ? 1u : 0u,
                               cases[i].fill_first ? 0u : 1u};
        check_permutations("fill/subscription pair", nodes, expected, 2);
    }
}

static void test_independent_fills(void)
{
    moq_stream_order_node_t nodes[4];
    const uint32_t expected[] = {0, 0, 0, 0};
    const uint8_t orders[] = {ASC, DESC};
    for (uint32_t direction = 0; direction < 2; direction++) {
        node_init(&nodes[0], MOQ_SCHED_MODE_FILL, 0, 0, orders[direction], ASC);
        node_init(&nodes[1], MOQ_SCHED_MODE_FILL, UINT64_MAX, 0, orders[direction], ASC);
        node_init(&nodes[2], MOQ_SCHED_MODE_FILL, 0, UINT64_MAX, orders[direction], DESC);
        node_init(&nodes[3], MOQ_SCHED_MODE_FILL, UINT64_MAX, 1, orders[direction], DESC);
        check_permutations("no fill/fill edge", nodes, expected, 4);
    }
}

static void test_priority_buckets(void)
{
    moq_stream_order_node_t nodes[6];
    const uint64_t groups[] = {UINT64_MAX, 0, UINT64_MAX, 0, UINT64_MAX, 0};
    const uint8_t subscriber[] = {0, 255, 0, 0, 255, 0};
    const uint8_t publisher[] = {0, 0, 255, 0, 0, 255};
    const uint32_t expected[] = {1, 0, 1, 0, 1, 0};
    for (uint32_t i = 0; i < 6; i++) {
        node_init(&nodes[i], MOQ_SCHED_MODE_SUBGROUP, groups[i], 0, ASC, ASC);
        nodes[i].meta.owner = UINT64_MAX;
        nodes[i].meta.subscriber_priority = subscriber[i];
        nodes[i].meta.publisher_priority = publisher[i];
    }
    /* Each bucket has its own 0 -> 1 path, with no cross-bucket edges or
     * arbitrary global rank offsets. Priority itself stays outside rank. */
    check_ranks("independent priority buckets", nodes, expected, 6);
}

static void test_isolated_metadata(void)
{
    moq_stream_order_node_t nodes[13];
    const uint32_t expected[] = {0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    for (uint32_t i = 0; i < 13; i++)
        node_init(&nodes[i], MOQ_SCHED_MODE_SUBGROUP, 2, 0, ASC, ASC);
    nodes[0].meta.group_id = 1;
    nodes[1].meta.group_id = 3;
    nodes[2].meta.mode = MOQ_SCHED_MODE_FETCH;
    nodes[3].meta.mode = MOQ_SCHED_MODE_NONE;
    nodes[4].meta.mode = MOQ_SCHED_MODE_DATAGRAM;
    nodes[5].meta.mode = UINT8_MAX;
    nodes[6].meta.has_location = false;
    nodes[7].meta.mode = MOQ_SCHED_MODE_FILL;
    nodes[7].meta.group_id = 1;
    nodes[7].meta.fill_group_order = DESC;
    nodes[7].meta.has_location = false;
    nodes[8].meta.owner = 0;
    nodes[9].meta.owner = UINT64_MAX;
    /* Invalid/unresolved order snapshots are defensively isolated too. */
    nodes[10].meta.group_order = MOQ_GROUP_ORDER_DEFAULT;
    nodes[11].meta.group_order = DESC;
    nodes[12].meta.mode = MOQ_SCHED_MODE_FILL;
    nodes[12].meta.group_id = 1;
    nodes[12].meta.fill_group_order = MOQ_GROUP_ORDER_DEFAULT;
    check_ranks("excluded and incomplete metadata", nodes, expected, 13);
}

static void test_work_bound(void)
{
    moq_stream_order_node_t nodes[TEST_NODE_CAP];
    uint32_t expected[TEST_NODE_CAP];
    for (uint32_t i = 0; i < TEST_NODE_CAP; i++) {
        node_init(&nodes[i], MOQ_SCHED_MODE_SUBGROUP, i, 0, ASC, ASC);
        expected[i] = i;
    }
    /* 256-deep DFS and 256 components: ranking cannot rescan the whole
     * graph once per component. The counter bound is checked on every run. */
    check_ranks("256-node chain", nodes, expected, TEST_NODE_CAP);
    check_ranks("256-node chain repeated", nodes, expected, TEST_NODE_CAP);

    nodes[TEST_NODE_CAP - 1].meta.mode = MOQ_SCHED_MODE_FILL;
    nodes[TEST_NODE_CAP - 1].meta.group_id = 0;
    nodes[TEST_NODE_CAP - 1].meta.fill_group_order = DESC;
    for (uint32_t i = 0; i < TEST_NODE_CAP; i++) expected[i] = 0;
    check_ranks("256-node SCC", nodes, expected, TEST_NODE_CAP);

    for (uint32_t i = 0; i < TEST_NODE_CAP; i++) {
        nodes[i].meta.mode = MOQ_SCHED_MODE_FILL;
        nodes[i].meta.fill_group_order = (i % 2) == 0 ? ASC : DESC;
    }
    check_ranks("256 isolated fills", nodes, expected, TEST_NODE_CAP);
}

int main(void)
{
    test_count_boundaries();
    test_cycles_and_reuse();
    test_external_edges();
    test_group_and_subgroup_boundaries();
    test_fill_pairs();
    test_independent_fills();
    test_priority_buckets();
    test_isolated_metadata();
    test_work_bound();
    MOQ_TEST_PASS("stream_order");
    return failures ? 1 : 0;
}
