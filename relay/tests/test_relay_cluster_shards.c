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
    MOQ_TEST_CHECK(moqr_core_binding_set_cluster(c0, a, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 21, .effective_relay_cost = 3, .correlation_id = 0}) == MOQ_OK);
    MOQ_TEST_CHECK(moqr_core_binding_set_cluster(c0, b, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 22, .effective_relay_cost = 3, .correlation_id = 0}) == MOQ_OK);
    MOQ_TEST_CHECK(moqr_core_binding_set_cluster(c1, requester, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 11, .effective_relay_cost = 1, .correlation_id = 0}) == MOQ_OK);
    moq_bytes_t part = {(const uint8_t *)"cluster", 7};
    moqr_ns_t ns = {&part, 1};
    uint64_t ah[] = {11, 21}, bh[] = {12, 22};
    moq_cluster_metadata_t am = {ah, 2, true, 5}, bm = {bh, 2, true, 9};
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, a, ns, 1, &am, false) == MOQ_OK);
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, b, ns, 2, &bm, false) == MOQ_OK);
    failures += settle(s, live, 0);
    moqr_cluster_route_t route;
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_count, 2);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 11);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[1], 21);
    MOQ_TEST_CHECK_EQ_U64(route.accumulated_cost, 8);
    uint64_t generation = route.advertisement.serial;
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, requester, ns, &route, sizeof(route)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 12);
    MOQ_TEST_CHECK_EQ_U64(route.accumulated_cost, 12);
    am.route_cost = 20;
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, a, ns, 1, &am, true) == MOQ_OK);
    failures += settle(s, live, 12);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 12);
    MOQ_TEST_CHECK(moqr_core_unannounce(c0, b, ns) == MOQR_OK);
    failures += settle(s, live, 24);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 11);
    MOQ_TEST_CHECK_EQ_U64(route.accumulated_cost, 23);
    MOQ_TEST_CHECK_EQ_U64(route.advertisement.serial, generation);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, requester, ns, &route, sizeof(route)) == MOQ_DONE);
    MOQ_TEST_CHECK(moqr_core_unannounce(c0, a, ns) == MOQR_OK);
    failures += settle(s, live, 36);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_DONE);
    /* Reuse a source pool slot before the receiver sees an intermediate
     * withdrawal. A new namespace must replace its old owned key safely. */
    am.route_cost = 1;
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, a, ns, 3, &am, false) == MOQ_OK);
    failures += settle(s, live, 48);
    MOQ_TEST_CHECK(moqr_core_unannounce(c0, a, ns) == MOQR_OK);
    moq_bytes_t other_part = {(const uint8_t *)"replacement", 11};
    moqr_ns_t other = {&other_part, 1};
    ah[0] = 13;
    MOQ_TEST_CHECK(moqr_core_announce_cluster(c0, a, other, 4, &am, false) == MOQ_OK);
    failures += settle(s, live, 60);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_DONE);
    MOQ_TEST_CHECK(moqr_core_cluster_route(c1, requester, other, &route, sizeof(route)) == MOQ_OK);
    MOQ_TEST_CHECK_EQ_U64(route.metadata.hop_ids[0], 13);
    MOQ_TEST_CHECK_EQ_U64(route.accumulated_cost, 4);
    MOQ_TEST_CHECK(route.advertisement.serial != generation);
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
    MOQ_TEST_CHECK(moqr_core_binding_set_cluster(owner, publisher, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 19, .effective_relay_cost = 1, .correlation_id = 0}) == MOQ_OK);
    moq_bytes_t part = {(const uint8_t *)"queued", 6}, name = {(const uint8_t *)"v", 1};
    moqr_ns_t ns = {&part, 1};
    const uint64_t hop = 19;
    moq_cluster_metadata_t metadata = {&hop, 1, true, 0};
    MOQ_TEST_CHECK(moqr_core_announce_cluster(owner, publisher, ns, 1, &metadata, false) == MOQ_OK);
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

static bool present(moqr_core_t *core, moqr_ns_t ns)
{
    moqr_cluster_route_t route;
    return moqr_core_cluster_route(core, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_OK;
}

static int step_pair(moqr_shards_t *s, bool live, uint64_t now)
{
    if (!live)
        return moqr_shards_step(s, now) == MOQR_OK;
    return moqr_shards_step_shard(s, 0, now, NULL) == MOQR_OK &&
        moqr_shards_step_shard(s, 1, now, NULL) == MOQR_OK;
}

static int test_withdrawal_progress(bool live, uint32_t routes, uint32_t mailbox)
{
    int failures = 0;
    moqr_shards_t *s = NULL;
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &alloc);
    cfg.shards = 2;
    cfg.live_visibility = live;
    cfg.cluster_enabled = true;
    cfg.cluster_hop_id = 100;
    cfg.max_cluster_routes = routes;
    cfg.max_cluster_hops = 8;
    cfg.mailbox_entries = mailbox;
#define REQUIRE(x) do { \
    if (!(x)) { \
        fprintf(stderr, "PREREQUISITE live=%d routes=%u mailbox=%u line=%d: %s\n", \
                live, routes, mailbox, __LINE__, #x); \
        ++failures; \
        goto cleanup; \
    } \
} while (0)
    REQUIRE(moqr_shards_create(&cfg, &s) == MOQR_OK && s != NULL);
    moqr_core_t *c0 = moqr_shards_core(s, 0);
    moqr_core_t *c1 = moqr_shards_core(s, 1);
    moqr_binding_t p0, p1;
    REQUIRE(moqr_core_binding_open(c0, 1, &p0) == MOQR_OK);
    REQUIRE(moqr_core_binding_open(c1, 2, &p1) == MOQR_OK);
    REQUIRE(moqr_core_binding_set_cluster(c0, p0, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 11, .effective_relay_cost = 1}) == MOQ_OK);
    REQUIRE(moqr_core_binding_set_cluster(c1, p1, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 12, .effective_relay_cost = 1}) == MOQ_OK);
    const uint64_t hop0 = 11, hop1 = 12;
    moq_cluster_metadata_t md0 = {&hop0, 1, true, 0};
    moq_cluster_metadata_t md1 = {&hop1, 1, true, 0};
    const moq_bytes_t a = {(const uint8_t *)"A", 1};
    const moq_bytes_t b = {(const uint8_t *)"B", 1};
    const moq_bytes_t c = {(const uint8_t *)"C", 1};
    const moq_bytes_t y = {(const uint8_t *)"Y", 1};
    const moqr_ns_t na = {&a, 1}, nb = {&b, 1}, nc = {&c, 1}, ny = {&y, 1};
    REQUIRE(moqr_core_announce_cluster(c0, p0, na, 1, &md0, false) == MOQ_OK);
    REQUIRE(moqr_core_announce_cluster(c0, p0, nb, 2, &md0, false) == MOQ_OK);
    for (unsigned i = 0; i < 16; ++i)
        REQUIRE(step_pair(s, live, i));
    REQUIRE(present(c1, na) && present(c1, nb));
    REQUIRE(moqr_core_unannounce(c0, p0, na) == MOQR_OK);
    for (unsigned i = 16; i < 32; ++i)
        REQUIRE(step_pair(s, live, i));
    REQUIRE(!present(c1, na) && present(c1, nb));

    /* No receiver step between these local mutations. */
    REQUIRE(moqr_core_announce_cluster(c1, p1, nc, 3, &md1, false) == MOQ_OK);
    REQUIRE(moqr_core_announce_cluster(c0, p0, ny, 4, &md0, false) == MOQ_OK);
    REQUIRE(moqr_core_unannounce(c0, p0, nb) == MOQR_OK);
    REQUIRE(present(c0, ny) && !present(c0, nb) && present(c1, nc));
    for (unsigned i = 32; i < 96; ++i)
        REQUIRE(step_pair(s, live, i));
    printf("live=%d routes=%u mailbox=%u: B=%d C=%d Y=%d\n",
           live, routes, mailbox, present(c1, nb), present(c1, nc), present(c1, ny));
    if (present(c1, nb)) {
        fprintf(stderr, "FAIL: withdrawn B remains on receiver\n");
        ++failures;
    }
    if (!present(c1, ny)) {
        fprintf(stderr, "FAIL: replacement Y never admitted\n");
        ++failures;
    }
    REQUIRE(present(c1, nc));
cleanup:
    moqr_shards_destroy(s);
    if (memory.live != 0) {
        fprintf(stderr, "PREREQUISITE allocator unbalanced: %zu\n", memory.live);
        ++failures;
    }
#undef REQUIRE
    return failures;
}


/* Consume only the wake set, never an unconditional sweep of every shard. */
static bool wake_drive(moqr_shards_t *s, uint64_t pending, uint64_t now)
{
    for (unsigned turn = 0; pending != 0 && turn < 64; turn++) {
        uint16_t lane = (pending & 1u) != 0 ? 0 : 1;
        pending &= ~(UINT64_C(1) << lane);
        uint64_t wake = 0;
        if (moqr_shards_step_shard(s, lane, now + turn, &wake) != MOQR_OK) return false;
        pending |= wake;
    }
    return pending == 0;
}

static int test_withdrawal_credit_wake(bool reserved, bool pressure, bool withdraw)
{
    int failures = 0;
    moqr_shards_t *s = NULL;
#define CW_REQUIRE(x) do { if (!(x)) { \
    fprintf(stderr, "PREREQUISITE credit-wake reserved=%d pressure=%d withdraw=%d line=%d: %s\n", \
            reserved, pressure, withdraw, __LINE__, #x); failures++; goto cleanup; } } while (0)
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &alloc);
    cfg.shards = 2; cfg.live_visibility = true; cfg.cluster_enabled = true;
    cfg.cluster_hop_id = 100; cfg.max_cluster_routes = 2;
    cfg.max_cluster_hops = 4; cfg.mailbox_entries = 1;
    CW_REQUIRE(moqr_shards_create(&cfg, &s) == MOQR_OK && s != NULL);
    moqr_core_t *source = moqr_shards_core(s, 0), *receiver = moqr_shards_core(s, 1);
    moqr_binding_t upstream, local, watchers[2], reject;
    CW_REQUIRE(moqr_core_binding_open(source, 41, &upstream) == MOQR_OK);
    CW_REQUIRE(moqr_core_binding_open(receiver, 42, &local) == MOQR_OK);
    CW_REQUIRE(moqr_core_binding_open(receiver, 43, &reject) == MOQR_OK);
    const moq_bytes_t ap = MOQ_BYTES_LITERAL("A"), bp = MOQ_BYTES_LITERAL("B");
    const moq_bytes_t cp = MOQ_BYTES_LITERAL("C"), yp = MOQ_BYTES_LITERAL("Y");
    const moq_bytes_t miss = MOQ_BYTES_LITERAL("no-route");
    const moqr_ns_t a = {&ap, 1}, b = {&bp, 1}, c = {&cp, 1}, y = {&yp, 1};
    uint64_t hop = 11;
    moq_cluster_metadata_t md = {&hop, 1, true, 0};
    /* B occupies index 0; its update fills the ordinary slot before A's
     * index-1 withdrawal needs the reserved slot. Both need watcher credit. */
    if (reserved && withdraw) CW_REQUIRE(moqr_core_announce_cluster(source, upstream, b, 1, &md, false) == MOQ_OK);
    CW_REQUIRE(moqr_core_announce_cluster(source, upstream, a, 2, &md, false) == MOQ_OK);
    CW_REQUIRE(wake_drive(s, 1, 1));
    CW_REQUIRE(present(receiver, a));
    if (reserved && withdraw) CW_REQUIRE(present(receiver, b));
    for (unsigned i = 0; i < 2; i++) {
        CW_REQUIRE(moqr_core_binding_open(receiver, 50 + i, &watchers[i]) == MOQR_OK);
        CW_REQUIRE(moqr_core_ns_subscribe(receiver, watchers[i], (moqr_ns_t){NULL, 0}, 60 + i) == MOQR_OK);
    }
    CW_REQUIRE(wake_drive(s, 2, 300));
    if (!withdraw) CW_REQUIRE(moqr_core_announce_cluster(receiver, local, c, 3, &md, false) == MOQ_OK);
    unsigned rejected = 0;
    if (pressure) {
        moqr_subscribe_req_t req; moqr_subscribe_req_init(&req);
        req.ns = (moqr_ns_t){&miss, 1}; req.name = MOQ_BYTES_LITERAL("v");
        moqr_result_t rc = MOQR_OK;
        while (rc == MOQR_OK && rejected < 2048) {
            req.cookie = rejected + 1;
            moqr_sub_t sub;
            rc = moqr_core_subscribe(receiver, reject, &req, &sub);
            if (rc == MOQR_OK) rejected++;
        }
        CW_REQUIRE(rejected > 0 && rejected < 2048 && rc == MOQR_ERR_WOULD_BLOCK);
    }
    if (reserved && withdraw) {
        md.route_cost = 9;
        CW_REQUIRE(moqr_core_announce_cluster(source, upstream, b, 1, &md, true) == MOQ_OK);
    } else if (!withdraw) {
        CW_REQUIRE(moqr_core_announce_cluster(source, upstream, y, 4, &md, false) == MOQ_OK);
    }
    if (withdraw) CW_REQUIRE(moqr_core_unannounce(source, upstream, a) == MOQR_OK);
    uint64_t wake = 0;
    CW_REQUIRE(moqr_shards_step_shard(s, 0, 400, &wake) == MOQR_OK);
    CW_REQUIRE((wake & 2u) != 0 && present(receiver, a));
    /* No receiver progress yet: the submitted withdrawal is not a removal. */
    uint64_t idle_wake = 0;
    CW_REQUIRE(moqr_shards_step_shard(s, 0, 401, &idle_wake) == MOQR_OK);
    CW_REQUIRE(idle_wake == 0 && present(receiver, a));
    uint64_t next = 0;
    CW_REQUIRE(moqr_shards_step_shard(s, 1, 402, &next) == MOQR_OK);
    if (!withdraw) {
        /* Intent credit can return, but unchanged route CAPACITY cannot
         * create a self-wake. No external progress has made Y admissible. */
        MOQ_TEST_CHECK((next & 2u) == 0); /* C can legitimately wake its peer. */
        CW_REQUIRE(wake_drive(s, next, 403));
        MOQ_TEST_CHECK(present(receiver, a) && present(receiver, c) && !present(receiver, y));
    } else {
        CW_REQUIRE(wake_drive(s, next, 403));
        MOQ_TEST_CHECK(!present(receiver, a));
        if (reserved) {
            moqr_cluster_route_t route = {0};
            MOQ_TEST_CHECK(moqr_core_cluster_route(receiver, MOQR_BINDING_INVALID, b,
                                                   &route, sizeof(route)) == MOQ_OK);
            MOQ_TEST_CHECK(route.accumulated_cost == 10);
        }
    }
    printf("credit-wake reserved=%d pressure=%d withdraw=%d rejected=%u first_wake=%llu A=%d\n",
           reserved, pressure, withdraw, rejected, (unsigned long long)next, present(receiver, a));
cleanup:
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_SIZE(memory.live, 0);
#undef CW_REQUIRE
    return failures;
}

static int test_withdrawal_wakes(void)
{
    int failures = 0;
    failures += test_withdrawal_credit_wake(false, false, true);
    failures += test_withdrawal_credit_wake(false, true, true);
    failures += test_withdrawal_credit_wake(true, false, true);
    failures += test_withdrawal_credit_wake(true, true, true);
    failures += test_withdrawal_credit_wake(true, true, false);
    return failures;
}

int main(int argc, char **argv)
{
    if (argc == 2 && strcmp(argv[1], "withdrawal_wakes") == 0) return test_withdrawal_wakes() ? EXIT_FAILURE : EXIT_SUCCESS;
    if (argc != 1) return 2;
    int failures = test_withdrawal_wakes();
    failures += test_cluster_shard_snapshots(false);
    failures += test_cluster_shard_snapshots(true);
    failures += test_cluster_stale_queued_object(false);
    failures += test_cluster_stale_queued_object(true);
    for (unsigned live = 0; live < 2; live++) {
        failures += test_withdrawal_progress(live != 0, 2, 1);
        failures += test_withdrawal_progress(live != 0, 2, 2);
        failures += test_withdrawal_progress(live != 0, 3, 1);
    }
    return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
