#include <moq/relay/moqr_shards.h>
#include <moq/relay/moqr_shards_test.h>
#include <moq/rcbuf.h>
#include "../../tests/unit/test_support.h"
#include <stdlib.h>
#include <string.h>

typedef struct memory_count { size_t live, peak; } memory_count_t;
static memory_count_t memory;
static void *ta(size_t n, void *ctx)
{
    memory_count_t *m = ctx;
    void *p = malloc(n);
    if (p != NULL) { m->live += n; if (m->live > m->peak) m->peak = m->live; }
    return p;
}
static void tf(void *p, size_t n, void *ctx)
{
    memory_count_t *m = ctx;
    if (p != NULL) m->live -= n;
    free(p);
}
static void *tr(void *p, size_t old, size_t n, void *ctx)
{
    memory_count_t *m = ctx;
    void *next = realloc(p, n);
    if (next != NULL) {
        m->live = m->live - old + n;
        if (m->live > m->peak) m->peak = m->live;
    }
    return next;
}
static const moq_alloc_t alloc = {&memory, ta, tr, tf};

static int settle(moqr_shards_t *s, bool live, uint64_t now)
{
    int failures = 0;
    if (!live) {
        for (unsigned i = 0; i < 12; ++i)
            MOQ_TEST_CHECK(moqr_shards_step(s, now + i) == MOQR_OK);
    } else {
        uint64_t pending = 1; /* only the original source has an external wake */
        for (unsigned turn = 0; pending != 0 && turn < 200; ++turn) {
            uint16_t shard = 0;
            while (((pending >> shard) & 1u) == 0) ++shard;
            pending &= ~(1ull << shard);
            uint64_t wake = 0;
            MOQ_TEST_CHECK(moqr_shards_step_shard(s, shard, now + turn, &wake) == MOQR_OK);
            pending |= wake;
        }
        MOQ_TEST_CHECK(pending == 0);
    }
    return failures;
}

static int test_cluster_shard_snapshots(bool live)
{
    int failures = 0;
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &alloc);
    cfg.shards = 3;
    cfg.live_visibility = live;
    cfg.cluster_enabled = true;
    cfg.cluster_hop_id = 100;
    cfg.max_cluster_routes = 16;
    cfg.max_cluster_hops = 8;
    cfg.mailbox_entries = 1; /* force alternatives through bounded backpressure */
    moqr_shards_capacity_t capacity;
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&cfg, &capacity) == MOQR_OK);
    memory.peak = memory.live;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    if (s == NULL) return failures + 1;
    moqr_core_t *c0 = moqr_shards_core(s, 0);
    moqr_core_t *c1 = moqr_shards_core(s, 1);
    moqr_binding_t a, b, requester;
    MOQ_TEST_CHECK(moqr_core_binding_open(c0, 10, &a) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c0, 20, &b) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c1, 30, &requester) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_set_cluster(c0, a, true, 21, 3, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_set_cluster(c0, b, true, 22, 3, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_set_cluster(c1, requester, true, 11, 1, 0) == MOQR_OK);
    moq_bytes_t part = {(const uint8_t *)"cluster", 7};
    moqr_ns_t ns = {&part, 1};
    uint64_t ah[] = {11, 21}, bh[] = {12, 22};
    moq_cluster_metadata_t am = {ah, 2, true, 5}, bm = {bh, 2, true, 9};
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, a, ns, 1, &am, false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, b, ns, 2, &bm, false) == MOQR_OK);
    failures += settle(s, live, 0);
    moqr_cluster_route_t route;
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_count, 2);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 11);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[1], 21);
    MOQ_TEST_CHECK_EQ_U64(route.accumulated_cost, 8);
    uint64_t generation = route.route_generation;
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, requester, ns, &route) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 12);
    MOQ_TEST_CHECK_EQ_U64(route.accumulated_cost, 12);
    am.route_cost = 20;
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, a, ns, 1, &am, true) == MOQR_OK);
    failures += settle(s, live, 12);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 12);
    MOQ_TEST_CHECK(moqr_core_unannounce(c0, b, ns) == MOQR_OK);
    failures += settle(s, live, 24);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 11);
    MOQ_TEST_CHECK_EQ_U64(route.accumulated_cost, 23);
    MOQ_TEST_CHECK_EQ_U64(route.route_generation, generation);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, requester, ns, &route) == MOQR_DONE);
    MOQ_TEST_CHECK(moqr_core_unannounce(c0, a, ns) == MOQR_OK);
    failures += settle(s, live, 36);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route) == MOQR_DONE);
    /* Reuse a source pool slot before the receiver sees an intermediate
     * withdrawal. A new namespace must replace its old owned key safely. */
    am.route_cost = 1;
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, a, ns, 3, &am, false) == MOQR_OK);
    failures += settle(s, live, 48);
    MOQ_TEST_CHECK(moqr_core_unannounce(c0, a, ns) == MOQR_OK);
    moq_bytes_t other_part = {(const uint8_t *)"replacement", 11};
    moqr_ns_t other = {&other_part, 1};
    ah[0] = 13;
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, a, other, 4, &am, false) == MOQR_OK);
    failures += settle(s, live, 60);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route) == MOQR_DONE);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, requester, other, &route) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 13);
    MOQ_TEST_CHECK_EQ_U64(route.accumulated_cost, 4);
    MOQ_TEST_CHECK(route.route_generation != generation);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_SIZE(memory.live, 0);
    MOQ_TEST_CHECK(memory.peak <= capacity.relay_alloc_ceiling);
    MOQ_TEST_PASS("cluster_shard_snapshots");
    return failures;
}
static moqr_result_t ingest_byte(moqr_core_t *core, moqr_track_t track,
                                 uint64_t group, uint8_t byte)
{
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_create(&alloc, &byte, 1, &payload) != MOQ_OK)
        return MOQR_ERR_NOMEM;
    moqr_log_append_desc_t desc;
    moqr_log_append_desc_init(&desc);
    desc.group_id = group;
    desc.payload = payload;
    moqr_result_t rc = moqr_core_ingest(core, track, &desc);
    if (rc != MOQR_OK) moq_rcbuf_decref(payload);
    return rc;
}

/* A source transition retains the track handle but changes its source
 * generation. Queue a former owner's object before the transition, then let
 * the replacement become ACTIVE before the requester consumes that FIFO. */
static int test_cluster_stale_queued_object(bool chunks)
{
    int failures = 0;
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &alloc);
    cfg.shards = 2;
    cfg.live_visibility = true;
    cfg.cluster_enabled = true;
    cfg.cluster_hop_id = 100;
    cfg.admit_remote_demand = true;
    cfg.max_cluster_routes = 16;
    cfg.max_cluster_hops = 8;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    if (s == NULL) return failures + 1;
    moqr_core_t *owner = moqr_shards_core(s, 0), *receiver = moqr_shards_core(s, 1);
    moqr_binding_t publisher, subscriber;
    MOQ_TEST_CHECK(moqr_core_binding_open(owner, 10, &publisher) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(receiver, 20, &subscriber) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_set_cluster(owner, publisher, true, 19, 1, 0) == MOQR_OK);
    moq_bytes_t part = {(const uint8_t *)"queued", 6}, name = {(const uint8_t *)"v", 1};
    moqr_ns_t ns = {&part, 1};
    const uint64_t hop = 19;
    moq_cluster_metadata_t metadata = {&hop, 1, true, 0};
    MOQ_TEST_CHECK(moqr_core_announce_cluster(owner, publisher, ns, 1, &metadata, false) == MOQR_OK);
    moqr_track_t source;
    MOQ_TEST_CHECK(moqr_core_publish_open(owner, publisher, ns, name, 1, &source) == MOQR_OK);
    MOQ_TEST_CHECK(ingest_byte(owner, source, 0, 0xA0) == MOQR_OK);
    for (unsigned i = 0; i < 8; ++i) {
        MOQ_TEST_CHECK(moqr_shards_step_shard(s, 0, i, NULL) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_step_shard(s, 1, i, NULL) == MOQR_OK);
    }
    moqr_subscribe_req_t request;
    moqr_subscribe_req_init(&request);
    request.ns = ns; request.name = name;
    moqr_sub_t first, second;
    MOQ_TEST_CHECK(moqr_core_subscribe(receiver, subscriber, &request, &first) == MOQR_OK);
    for (unsigned i = 8; i < 16; ++i) {
        MOQ_TEST_CHECK(moqr_shards_step_shard(s, 1, i, NULL) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_step_shard(s, 0, i, NULL) == MOQR_OK);
    }
    moqr_track_t target, replacement;
    uint64_t old_generation = 0, replacement_generation = 0;
    MOQ_TEST_CHECK(moqr_core_sub_track(receiver, first, &target, &old_generation) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 1);
    if (chunks) {
        moqr_log_append_desc_t open;
        moqr_log_append_desc_init(&open);
        open.group_id = 1;
        open.obj_state = MOQR_OBJ_OPEN;
        open.declared_len = 1;
        MOQ_TEST_CHECK(moqr_core_ingest(owner, source, &open) == MOQR_OK);
        const uint8_t old_byte = 0xA1;
        moq_rcbuf_t *chunk = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&alloc, &old_byte, 1, &chunk) == MOQ_OK);
        MOQ_TEST_CHECK(moqr_core_append_chunk(owner, source, 1, 0, 0, chunk) == MOQR_OK);
        moq_rcbuf_decref(chunk);
    } else {
        MOQ_TEST_CHECK(ingest_byte(owner, source, 1, 0xA1) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_shards_step_shard(s, 0, 20, NULL) == MOQR_OK);
    if (chunks) {
        MOQ_TEST_CHECK(moqr_core_complete_record(owner, source, 1, 0, 0) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_seal_subgroup(owner, source, 1, 0) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_step_shard(s, 0, 20, NULL) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_debug_demand_channel_pending(s, 0, 1) >= 3);
    }
    MOQ_TEST_CHECK(moqr_shards_debug_demand_channel_pending(s, 0, 1) != 0);
    MOQ_TEST_CHECK(moqr_core_upstream_lost(receiver, target, old_generation) == MOQR_OK);
    request.cookie = 2;
    MOQ_TEST_CHECK(moqr_core_subscribe(receiver, subscriber, &request, &second) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_sub_track(receiver, second, &replacement, &replacement_generation) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_track_eq(target, replacement));
    MOQ_TEST_CHECK(replacement_generation != old_generation);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(receiver, replacement, replacement_generation,
                                        999, false, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step_shard(s, 1, 21, NULL) == MOQR_OK);
    moqr_delivery_t delivery;
    MOQ_TEST_CHECK(moqr_core_next_delivery(receiver, subscriber, 21, &delivery) == MOQR_DONE);
    MOQ_TEST_CHECK(ingest_byte(receiver, replacement, 2, 0xB2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(receiver, subscriber, 22, &delivery) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(delivery.rec.group_id, 2);
    MOQ_TEST_CHECK_EQ_U64(moq_rcbuf_data(delivery.rec.payload)[0], 0xB2);
    MOQ_TEST_CHECK(moqr_core_delivery_done(receiver, subscriber, MOQR_DELIVERY_DELIVERED, 22) == MOQR_OK);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_SIZE(memory.live, 0);
    MOQ_TEST_PASS("cluster_stale_queued_object");
    return failures;
}

int main(void)
{
    int failures = test_cluster_shard_snapshots(false);
    failures += test_cluster_shard_snapshots(true);
    failures += test_cluster_stale_queued_object(false);
    failures += test_cluster_stale_queued_object(true);
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
