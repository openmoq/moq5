#include "../src/core/relay.c"
#include "../src/core/retrieve_internal.h"
#include <moq/moq.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define REQUIRE(x) do { \
    if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); abort(); } \
} while (0)

static void *chunk_alloc(size_t bytes, void *ctx)
{
    (void)ctx;
    return malloc(bytes);
}

static void chunk_free(void *ptr, size_t bytes, void *ctx)
{
    (void)bytes;
    if (ptr != NULL) (*(unsigned *)ctx)++;
    free(ptr);
}

static void test_alternatives(void)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default());
    moqr_cluster_cfg_t cluster;
    moqr_cluster_cfg_init_sized(&cluster, sizeof(cluster));
    cluster.enabled = true;
    cluster.local_hop_id = 50;
    cluster.max_routes = 3;
    cluster.max_hops = 4;
    moqr_core_t *c = NULL;
    REQUIRE(moqr_core_create_cluster(&cfg, &cluster, &c) == MOQ_OK);
    moqr_binding_t a, b, d;
    REQUIRE(moqr_core_binding_open(c, 1, &a) == MOQR_OK);
    REQUIRE(moqr_core_binding_open(c, 2, &b) == MOQR_OK);
    REQUIRE(moqr_core_binding_open(c, 3, &d) == MOQR_OK);
    REQUIRE(moqr_core_binding_set_cluster(c, a, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 10, .effective_relay_cost = 2, .correlation_id = 0}) == MOQ_OK);
    REQUIRE(moqr_core_binding_set_cluster(c, b, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 20, .effective_relay_cost = 1, .correlation_id = 0}) == MOQ_OK);
    REQUIRE(moqr_core_binding_set_cluster(c, d, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = 10, .effective_relay_cost = 1, .correlation_id = 0}) == MOQ_OK);
    moq_bytes_t parts[] = {{(const uint8_t *)"demo", 4}};
    moqr_ns_t ns = {parts, 1};
    uint64_t pa[] = {99, 10}, pb[] = {99, 20};
    moq_cluster_metadata_t ma = {pa, 2, true, 3}, mb = {pb, 2, true, 6};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 101, &ma, false) == MOQ_OK);
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 102, &mb, false) == MOQ_OK);
    moqr_cluster_route_t r;
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &r, sizeof(r)) == MOQ_OK);
    REQUIRE(r.advertisement.source._opaque == a._opaque && r.accumulated_cost == 5);
    char dump[4096];
    size_t written;
    REQUIRE(moqr_core_route_dump_json(c, dump, sizeof(dump), &written) == MOQR_OK);
    REQUIRE(strstr(dump, "\"cluster_routes\"") != NULL && strstr(dump, "\"hop_path\":[99,20]") != NULL);
    REQUIRE(moqr_core_cluster_route(c, d, ns, &r, sizeof(r)) == MOQ_OK);
    REQUIRE(r.advertisement.source._opaque == b._opaque && r.metadata.route_cost == 7);
    mb.route_cost = 0;
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 102, &mb, true) == MOQ_OK);
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &r, sizeof(r)) == MOQ_OK);
    REQUIRE(r.advertisement.source._opaque == b._opaque && r.accumulated_cost == 1);
    REQUIRE(moqr_core_unannounce(c, b, ns) == MOQR_OK);
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &r, sizeof(r)) == MOQ_OK);
    REQUIRE(r.advertisement.source._opaque == a._opaque);
    moqr_core_destroy(c);

}

static moqr_core_t *new_core(uint64_t hop, uint32_t routes, uint32_t hops)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default());
    cfg.max_tracks = 4;
    cfg.max_subs = 8;
    cfg.max_ns_nodes = 8;
    cfg.max_ns_subs = 4;
    moqr_cluster_cfg_t cluster;
    moqr_cluster_cfg_init_sized(&cluster, sizeof(cluster));
    cluster.enabled = true;
    cluster.local_hop_id = hop;
    cluster.max_routes = routes;
    cluster.max_hops = hops;
    moqr_core_t *c = NULL;
    REQUIRE(moqr_core_create_cluster(&cfg, &cluster, &c) == MOQ_OK);
    return c;
}

static moqr_binding_t binding(moqr_core_t *c, uint64_t cookie, uint64_t hop, uint64_t cost)
{
    moqr_binding_t b;
    REQUIRE(moqr_core_binding_open(c, cookie, &b) == MOQR_OK);
    REQUIRE(moqr_core_binding_set_cluster(c, b, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = true, .peer_hop_id = hop, .effective_relay_cost = cost, .correlation_id = 0}) == MOQ_OK);
    return b;
}

static void drain(moqr_core_t *c)
{
    moqr_intent_t items[64];
    while (moqr_core_poll_intents(c, items, 64) != 0) {}
}

static moqr_ns_t demo(moq_bytes_t *parts)
{
    parts[0] = (moq_bytes_t){(const uint8_t *)"demo", 4};
    parts[1] = (moq_bytes_t){(const uint8_t *)"deep", 4};
    return (moqr_ns_t){parts, 1};
}

static void test_selection_and_limits(void)
{
    moqr_core_t *c = new_core(50, 3, 3);
    moqr_binding_t a = binding(c, 1, 10, 1), b = binding(c, 2, 20, 2);
    moqr_binding_t d = binding(c, 3, 99, 0);
    moq_bytes_t parts[2];
    moqr_ns_t ns = demo(parts), deep = {parts, 2};
    uint64_t unknown[] = {0, 10}, known[] = {99, 20}, loop[] = {99, 50};
    moq_cluster_metadata_t m = {unknown, 2, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &m, false) == MOQ_OK);
    m = (moq_cluster_metadata_t){known, 2, true, UINT64_MAX};
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 2, &m, false) == MOQ_OK);
    moqr_cluster_route_t r;
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &r, sizeof(r)) == MOQ_OK);
    REQUIRE(r.advertisement.source._opaque == b._opaque && r.accumulated_cost == UINT64_MAX);
    REQUIRE(moqr_core_cluster_route(c, d, ns, &r, sizeof(r)) == MOQ_OK && r.advertisement.source._opaque == a._opaque);
    m = (moq_cluster_metadata_t){loop, 2, false, 0};
    REQUIRE(moqr_core_announce_cluster(c, d, deep, 3, &m, false) == MOQ_OK);
    moqr_cluster_info_t info;
    moqr_core_cluster_info(c, &info, sizeof(info));
    REQUIRE(info.routes_used == 2); /* local loops are discarded */
    m = (moq_cluster_metadata_t){known, 2, false, 0};
    REQUIRE(moqr_core_announce_cluster(c, b, deep, 3, &m, false) == MOQ_OK);
    REQUIRE(moqr_core_cluster_route(c, d, deep, &r, sizeof(r)) == MOQ_DONE); /* exhausted exact tier */
    uint64_t too_long[] = {1, 2, 3, 4};
    m = (moq_cluster_metadata_t){too_long, 4, false, 0};
    REQUIRE(moqr_core_announce_cluster(c, d, ns, 4, &m, false) == MOQ_ERR_CAPACITY);
    uint64_t duplicate[] = {9, 9};
    m = (moq_cluster_metadata_t){duplicate, 2, false, 0};
    REQUIRE(moqr_core_announce_cluster(c, d, ns, 4, &m, false) == MOQ_ERR_INVAL);
    REQUIRE(moqr_core_binding_close(c, b, 1) == MOQR_OK);
    moqr_core_cluster_info(c, &info, sizeof(info));
    REQUIRE(info.routes_used == 1);
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, deep, &r, sizeof(r)) == MOQ_OK && r.advertisement.source._opaque == a._opaque);
    moqr_core_destroy(c);

    c = new_core(0, 3, 3);
    a = binding(c, 1, 0, 0);
    uint64_t zeros[] = {0, 0};
    m = (moq_cluster_metadata_t){zeros, 2, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &m, false) == MOQ_OK);
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &r, sizeof(r)) == MOQ_OK && r.accumulated_cost == 0);
    uint64_t generation = r.advertisement.serial;
    m = (moq_cluster_metadata_t){NULL, 0, true, 9};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &m, true) == MOQ_OK);
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &r, sizeof(r)) == MOQ_OK && r.advertisement.serial == generation && r.accumulated_cost == 9);
    moqr_core_destroy(c);
}

static moqr_intent_t subscribe_track(moqr_core_t *c, moqr_binding_t d, moqr_ns_t ns, uint64_t cookie, moqr_sub_t *sub)
{
    drain(c);
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = ns;
    req.name = (moq_bytes_t){(const uint8_t *)"video", 5};
    req.cookie = cookie;
    REQUIRE(moqr_core_subscribe(c, d, &req, sub) == MOQR_OK);
    moqr_intent_t it[8];
    size_t n = moqr_core_poll_intents(c, it, 8);
    REQUIRE(n == 1);
    return it[0];
}

static void test_source_pinning_warm_and_origins(void)
{
    moqr_core_t *c = new_core(50, 8, 8);
    moqr_binding_t a = binding(c, 1, 10, 1), b = binding(c, 2, 20, 1);
    moqr_binding_t d = binding(c, 3, 30, 1), e = binding(c, 4, 10, 1);
    moq_bytes_t parts[2];
    moqr_ns_t ns = demo(parts);
    uint64_t pa[] = {99, 10}, pb[] = {99, 20};
    moq_cluster_metadata_t ma = {pa, 2, true, 0}, mb = {pb, 2, true, 8};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, false) == MOQ_OK);
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 2, &mb, false) == MOQ_OK);
    REQUIRE(moqr_core_ns_subscribe(c, d, (moqr_ns_t){NULL, 0}, 999) == MOQR_OK);
    moqr_sub_t sub;
    moqr_intent_t up = subscribe_track(c, d, ns, 1, &sub);
    REQUIRE(up.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE && up.binding_cookie == 1);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 123, false, 0, 0) == MOQR_OK);
    moqr_cluster_route_t route;
    REQUIRE(moqr_core_cluster_route(c, d, ns, &route, sizeof(route)) == MOQ_OK && route.warm && route.metadata.route_cost == 0);
    REQUIRE(moqr_core_cluster_route(c, e, ns, &route, sizeof(route)) == MOQ_OK && !route.warm && route.metadata.route_cost == 9);
    /* An existing active cache must not bypass the requester's exclusions. */
    moqr_sub_t excluded;
    moqr_intent_t denied = subscribe_track(c, e, ns, 2, &excluded);
    REQUIRE(denied.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE && denied.binding_cookie == 2);
    moqr_log_append_desc_t rec;
    moqr_log_append_desc_init(&rec);
    rec.group_id = 1;
    rec.object_id = 0;
    rec.obj_state = MOQR_OBJ_COMPLETE;
    rec.status = MOQ_OBJECT_NORMAL;
    REQUIRE(moqr_core_ingest(c, up.track, &rec) == MOQR_OK);
    moqr_fetch_req_t fetch_req;
    moqr_fetch_req_init(&fetch_req);
    fetch_req.ns = ns;
    fetch_req.name = (moq_bytes_t){(const uint8_t *)"video", 5};
    fetch_req.start_group = 1;
    fetch_req.end_group = 1;
    moqr_fetch_t fetch;
    moqr_fetch_plan_t fetch_plan;
    REQUIRE(moqr_core_fetch_open(c, e, &fetch_req, 1, &fetch, &fetch_plan) == MOQR_OK);
    REQUIRE(fetch_plan.admit == MOQR_FETCH_REJECT); /* cannot use A's cache */
    REQUIRE(moqr_core_fetch_open(c, d, &fetch_req, 1, &fetch, &fetch_plan) == MOQR_OK);
    REQUIRE(fetch_plan.admit == MOQR_FETCH_ACCEPT);
    drain(c);
    REQUIRE(moqr_core_unannounce(c, a, ns) == MOQR_OK);
    moqr_intent_t it[16];
    size_t n = moqr_core_poll_intents(c, it, 16);
    moqr_intent_t next = {0};
    for (size_t i = 0; i < n; i++) if (it[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) next = it[i];
    REQUIRE(next.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE && next.binding_cookie == 2);
    REQUIRE(next.track_gen != up.track_gen);
    REQUIRE(!moqr_core_internal_source_idle(c, next.track, up.track_gen));
    REQUIRE(!moqr_core_internal_source_idle(c, next.track, UINT64_MAX));
    moqr_fetch_item_t item;
    REQUIRE(moqr_core_fetch_peek(c, fetch, 1, &item) == MOQR_ERR_STALE_HANDLE);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 999, false, 0, 0) == MOQR_ERR_STALE_HANDLE);
    REQUIRE(moqr_core_upstream_ok(c, next.track, next.track_gen, 124, false, 0, 0) == MOQR_OK);
    unsigned chunk_frees = 0;
    moq_alloc_t chunk_allocator = {0};
    chunk_allocator.alloc = chunk_alloc;
    chunk_allocator.free = chunk_free;
    chunk_allocator.ctx = &chunk_frees;
    moq_rcbuf_t *chunk;
    const uint8_t chunk_byte = 42;
    REQUIRE(moq_rcbuf_create(&chunk_allocator, &chunk_byte, 1, &chunk) == MOQ_OK);
    REQUIRE(moqr_core_append_chunk(c, next.track, 1, 0, 0, chunk) == MOQR_OK);
    REQUIRE(chunk_frees == 0); /* append_chunk borrows even when dropping a group */
    moq_rcbuf_decref(chunk);
    REQUIRE(chunk_frees == 1);
    rec.object_id = 1; /* same group from replacement must not splice */
    REQUIRE(moqr_core_ingest(c, next.track, &rec) == MOQR_OK);
    moqr_core_stats_t stats;
    moqr_core_get_stats(c, &stats);
    REQUIRE(stats.ingested_total == 1);
    rec.group_id = 2;
    rec.object_id = 0;
    REQUIRE(moqr_core_ingest(c, next.track, &rec) == MOQR_OK);
    REQUIRE(!moqr_core_internal_source_idle(c, next.track, next.track_gen));
    REQUIRE(moqr_core_unsubscribe(c, sub, 4) == MOQR_OK);
    REQUIRE(moqr_core_tick(c, 5) == MOQR_OK);
    REQUIRE(moqr_core_internal_source_idle(c, next.track, next.track_gen));
    REQUIRE(moqr_core_cluster_route(c, d, ns, &route, sizeof(route)) == MOQ_OK && !route.warm && route.metadata.route_cost == 9);
    REQUIRE(cluster_should_reparent(ns, 50, 20) != cluster_should_reparent(ns, 20, 50));
    REQUIRE(!cluster_should_reparent(ns, 0, 0));
    drain(c);
    REQUIRE(moqr_core_unannounce(c, b, ns) == MOQR_OK);
    drain(c);
    uint64_t replacement_path[] = {77, 20};
    mb = (moq_cluster_metadata_t){replacement_path, 2, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 3, &mb, false) == MOQ_OK);
    moqr_track_t replacement;
    REQUIRE(moqr_core_publish_open(c, b, ns,
        (moq_bytes_t){(const uint8_t *)"video", 5}, 4, &replacement) == MOQR_OK);
    REQUIRE(moqr_track_is_valid(replacement));
    REQUIRE(!moqr_core_internal_source_idle(c, next.track, next.track_gen));
    moqr_core_get_stats(c, &stats);
    uint64_t before_replacement = stats.ingested_total;
    rec.group_id = 0;
    REQUIRE(moqr_core_ingest(c, replacement, &rec) == MOQR_OK);
    moqr_core_get_stats(c, &stats);
    REQUIRE(stats.ingested_total == before_replacement + 1); /* fresh origin resets group floor */
    moqr_core_destroy(c);

    /* Anonymous advertisements remain separate even with identical [0] paths. */
    c = new_core(50, 4, 4);
    a = binding(c, 1, 0, 1); b = binding(c, 2, 0, 1); d = binding(c, 3, 0, 1);
    uint64_t anonymous[] = {0};
    ma = (moq_cluster_metadata_t){anonymous, 1, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, false) == MOQ_OK);
    up = subscribe_track(c, d, ns, 1, &sub);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 123, false, 0, 0) == MOQR_OK);
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 2, &ma, false) == MOQ_OK);
    drain(c);
    REQUIRE(moqr_core_binding_close(c, a, 3) == MOQR_OK);
    n = moqr_core_poll_intents(c, it, 16);
    bool done = false;
    for (size_t i = 0; i < n; i++) { REQUIRE(it[i].kind != MOQR_INTENT_UPSTREAM_SUBSCRIBE); if (it[i].kind == MOQR_INTENT_SUB_DONE) done = true; }
    REQUIRE(done);
    up = subscribe_track(c, d, ns, 2, &sub);
    REQUIRE(up.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE && up.binding_cookie == 2);
    moqr_core_destroy(c);
}

static void test_backpressure_and_pool_rollback(void)
{
    moqr_core_t *c = new_core(50, 1, 3);
    moqr_binding_t a = binding(c, 1, 10, 1), b = binding(c, 2, 20, 1);
    moq_bytes_t parts[2];
    moqr_ns_t ns = demo(parts);
    uint64_t pa[] = {99, 10}, pb[] = {99, 20};
    moq_cluster_metadata_t ma = {pa, 2, true, 4}, mb = {pb, 2, true, 1};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, false) == MOQ_OK);
    moqr_cluster_route_t route;
    REQUIRE(moqr_core_cluster_route(c, b, ns, &route, sizeof(route)) == MOQ_OK);
    uint64_t generation = route.advertisement.serial;
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 2, &mb, false) == MOQ_ERR_CAPACITY);
    REQUIRE(moqr_core_ns_subscribe(c, b, (moqr_ns_t){NULL, 0}, 1) == MOQR_OK);
    for (unsigned i = 0; i < 1000; i++) {
        moqr_result_t rc = moqr_core_track_status(c, b, ns,
            (moq_bytes_t){(const uint8_t *)"missing", 7}, i);
        if (rc == MOQR_ERR_WOULD_BLOCK) break;
        REQUIRE(rc == MOQR_OK);
        REQUIRE(i != 999);
    }
    ma.route_cost = 1;
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, true) == MOQ_ERR_WOULD_BLOCK);
    REQUIRE(moqr_core_cluster_route(c, b, ns, &route, sizeof(route)) == MOQ_OK);
    REQUIRE(route.advertisement.serial == generation && route.accumulated_cost == 5);
    REQUIRE(moqr_core_unannounce(c, a, ns) == MOQR_ERR_WOULD_BLOCK);
    REQUIRE(moqr_core_cluster_route(c, b, ns, &route, sizeof(route)) == MOQ_OK);
    drain(c);
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, true) == MOQ_OK);
    REQUIRE(moqr_core_cluster_route(c, b, ns, &route, sizeof(route)) == MOQ_OK && route.accumulated_cost == 2);
    drain(c);
    REQUIRE(moqr_core_unannounce(c, a, ns) == MOQR_OK);
    REQUIRE(moqr_core_cluster_route(c, b, ns, &route, sizeof(route)) == MOQ_DONE);
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 2, &mb, false) == MOQ_OK);
    REQUIRE(moqr_core_cluster_route(c, a, ns, &route, sizeof(route)) == MOQ_OK && route.advertisement.serial != generation);
    moqr_core_destroy(c);
}

static void test_anonymous_push_isolation(void)
{
    moqr_core_t *c = new_core(50, 4, 4);
    moqr_binding_t a = binding(c, 1, 0, 1), b = binding(c, 2, 0, 1);
    moqr_binding_t d = binding(c, 3, 0, 1);
    REQUIRE(moqr_core_binding_set_cluster(c, a, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = false, .peer_hop_id = 0, .effective_relay_cost = 1, .correlation_id = 11}) == MOQ_OK);
    REQUIRE(moqr_core_binding_set_cluster(c, b, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = false, .peer_hop_id = 0, .effective_relay_cost = 1, .correlation_id = 22}) == MOQ_OK);
    REQUIRE(moqr_core_binding_set_cluster(c, d, &(moqr_cluster_peer_cfg_t){.struct_size = sizeof(moqr_cluster_peer_cfg_t), .negotiated = false, .peer_hop_id = 0, .effective_relay_cost = 1, .correlation_id = 22}) == MOQ_OK);
    moq_bytes_t parts[2];
    moqr_ns_t ns = demo(parts);
    REQUIRE(moqr_core_announce(c, a, ns) == MOQR_OK);
    REQUIRE(moqr_core_announce(c, b, ns) == MOQR_OK);
    moq_bytes_t name = {(const uint8_t *)"video", 5};
    moqr_track_t ta, tb;
    REQUIRE(moqr_core_publish_open(c, a, ns, name, 1, &ta) == MOQR_OK);
    REQUIRE(moqr_core_publish_open(c, b, ns, name, 2, &tb) == MOQR_OK);
    REQUIRE(moqr_track_is_valid(ta) && moqr_track_is_valid(tb) && !moqr_track_eq(ta, tb));
    drain(c);
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = ns;
    req.name = name;
    req.cookie = 3;
    moqr_sub_t sub;
    REQUIRE(moqr_core_subscribe(c, d, &req, &sub) == MOQR_OK);
    moqr_track_t selected;
    REQUIRE(moqr_core_sub_track(c, sub, &selected, NULL) == MOQR_OK);
    REQUIRE(moqr_track_eq(selected, ta));
    moqr_core_destroy(c);
}

static moqr_cluster_export_t imported_route(moqr_ns_t ns, uint32_t index,
    const uint64_t *path, uint64_t source, uint64_t lifetime, uint64_t cost)
{
    moqr_cluster_export_t r = {0};
    r.source_index = index;
    r.ns = ns;
    r.route.advertisement.source = (moqr_binding_t){moq_handle_pack(MOQR_HANDLE_POOL_BINDING, 2, 1, (uint16_t)(source - 101u))};
    r.route.advertisement.serial = lifetime;
    r.route.accumulated_cost = cost;
    r.route.metadata = (moq_cluster_metadata_t){path, 2, true, cost};
    return r;
}

static void test_import_selected_source_pin(void)
{
    moqr_core_t *c = new_core(50, 8, 4);
    moqr_binding_t mirror = binding(c, MOQR_SHARD_COOKIE_BASE + 1, 0, 0);
    moqr_binding_t downstream = binding(c, 3, 30, 1);
    moq_bytes_t parts[2];
    moqr_ns_t ns = demo(parts);
    uint64_t p1[] = {91, 11}, p2[] = {92, 12};
    moqr_cluster_export_t r1 = imported_route(ns, 0, p1, 101, 201, 50);
    moqr_cluster_export_t r2 = imported_route(ns, 1, p2, 102, 202, 1);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &r1, false) == MOQR_OK);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &r2, false) == MOQR_OK);
    moqr_cluster_route_t selected;
    moqr_cluster_advertisement_id_t pinned;
    REQUIRE(moqr_core_cluster_route(c, downstream, ns, &selected, sizeof(selected)) == MOQ_OK);
    REQUIRE(selected.origin_id == 92 && moqr_binding_eq(selected.advertisement.source, r2.route.advertisement.source));
    moqr_sub_t sub;
    moqr_intent_t up = subscribe_track(c, downstream, ns, 1, &sub);
    REQUIRE(up.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    uint64_t hop, correlation;
    REQUIRE(moqr_core_internal_cluster_upstream_route(c, up.track, up.track_gen,
        &pinned, &hop, &correlation) == MOQR_OK);
    REQUIRE(moqr_cluster_advertisement_id_eq(pinned, selected.advertisement));
    moqr_core_destroy(c);
}

static void test_import_reparent_exact_route(void)
{
    moqr_core_t *c = new_core(50, 8, 4);
    moqr_binding_t a = binding(c, 1, 10, 1), d = binding(c, 3, 30, 1);
    moqr_binding_t mirror = binding(c, MOQR_SHARD_COOKIE_BASE + 1, 0, 0);
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts);
    uint64_t pa[] = {99, 10}, p1[] = {99, 20}, p2[] = {99, 21};
    moq_cluster_metadata_t ma = {pa, 2, true, 9};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, false) == MOQ_OK);
    moqr_sub_t sub; moqr_intent_t up = subscribe_track(c, d, ns, 1, &sub);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 1, false, 0, 0) == MOQR_OK);
    drain(c);
    moqr_cluster_export_t r1 = imported_route(ns, 0, p1, 101, 201, 1);
    moqr_cluster_export_t r2 = imported_route(ns, 1, p2, 102, 202, 100);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &r1, false) == MOQR_OK);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &r2, false) == MOQR_OK);
    REQUIRE(moqr_core_tick(c, 1) == MOQR_OK);
    moqr_intent_t intents[16], next = {0};
    size_t n = moqr_core_poll_intents(c, intents, 16);
    bool cancelled = false;
    for (size_t i = 0; i < n; i++) {
        if (intents[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) next = intents[i];
        if (intents[i].kind == MOQR_INTENT_UPSTREAM_UNSUBSCRIBE) {
            REQUIRE(intents[i].cookie == 1 && intents[i].track_gen == up.track_gen);
            cancelled = true;
        }
    }
    REQUIRE(cancelled && next.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_cluster_advertisement_id_t pinned; uint64_t hop, correlation;
    REQUIRE(moqr_core_internal_cluster_upstream_route(c, next.track, next.track_gen, &pinned, &hop, &correlation) == MOQR_OK);
    REQUIRE(moqr_cluster_advertisement_id_eq(pinned, r1.route.advertisement));
    moqr_core_destroy(c);
}

static void test_import_same_shard_failover(void)
{
    moqr_core_t *c = new_core(50, 8, 4);
    moqr_binding_t d = binding(c, 3, 30, 1);
    moqr_binding_t mirror = binding(c, MOQR_SHARD_COOKIE_BASE + 1, 0, 0);
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts);
    uint64_t p1[] = {99, 20}, p2[] = {99, 21};
    moqr_cluster_export_t r1 = imported_route(ns, 0, p1, 101, 201, 1);
    moqr_cluster_export_t r2 = imported_route(ns, 1, p2, 102, 202, 5);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &r1, false) == MOQR_OK);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &r2, false) == MOQR_OK);
    moqr_sub_t sub; moqr_intent_t up = subscribe_track(c, d, ns, 1, &sub);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 1, false, 0, 0) == MOQR_OK);
    drain(c);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &r1, true) == MOQR_OK);
    moqr_intent_t intents[16], next = {0};
    size_t n = moqr_core_poll_intents(c, intents, 16);
    bool cancelled = false;
    for (size_t i = 0; i < n; i++) {
        if (intents[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) next = intents[i];
        if (intents[i].kind == MOQR_INTENT_UPSTREAM_UNSUBSCRIBE) {
            REQUIRE(intents[i].cookie == 1 && intents[i].track_gen == up.track_gen);
            cancelled = true;
        }
    }
    REQUIRE(cancelled && next.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_cluster_advertisement_id_t pinned; uint64_t hop, correlation;
    REQUIRE(moqr_core_internal_cluster_upstream_route(c, next.track, next.track_gen, &pinned, &hop, &correlation) == MOQR_OK);
    REQUIRE(moqr_cluster_advertisement_id_eq(pinned, r2.route.advertisement));
    moqr_core_destroy(c);
}

static void test_withdraw_draining_active_subscriber(void)
{
    moqr_core_t *c = new_core(50, 4, 4);
    moqr_binding_t a = binding(c, 1, 10, 1), d = binding(c, 3, 30, 1);
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts);
    uint64_t path[] = {99, 10}; moq_cluster_metadata_t ma = {path, 2, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, false) == MOQ_OK);
    moqr_sub_t sub; moqr_intent_t up = subscribe_track(c, d, ns, 1, &sub);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 1, false, 0, 0) == MOQR_OK);
    moqr_pd_desc_t done;
    REQUIRE(moqr_pd_desc_local(MOQR_PD_TRACK_ENDED, &done) == MOQR_OK);
    REQUIRE(moqr_core_source_finish(c, up.track, up.track_gen, done, 1) == MOQR_OK);
    drain(c);
    REQUIRE(moqr_core_unannounce(c, a, ns) == MOQR_OK);
    moqr_intent_t intents[16]; size_t n = moqr_core_poll_intents(c, intents, 16);
    bool terminated = false;
    for (size_t i = 0; i < n; i++) {
        REQUIRE(intents[i].kind != MOQR_INTENT_REJECT_SUB);
        if (intents[i].kind == MOQR_INTENT_SUB_DONE) {
            REQUIRE(moqr_pd_desc_valid(intents[i].pd));
            terminated = true;
        }
    }
    REQUIRE(terminated);
    moqr_core_destroy(c);
}

static void test_update_invalidates_live_loop(void)
{
    moqr_core_t *c = new_core(50, 4, 4);
    moqr_binding_t a = binding(c, 1, 10, 1), d = binding(c, 3, 30, 1);
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts);
    uint64_t path[] = {99, 10}, loop[] = {99, 30, 10};
    moq_cluster_metadata_t ma = {path, 2, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, false) == MOQ_OK);
    moqr_sub_t sub; moqr_intent_t up = subscribe_track(c, d, ns, 1, &sub);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 1, false, 0, 0) == MOQR_OK);
    moqr_log_append_desc_t object; moqr_log_append_desc_init(&object);
    REQUIRE(moqr_core_ingest(c, up.track, &object) == MOQR_OK);
    moqr_fetch_req_t req; moqr_fetch_req_init(&req);
    req.ns = ns; req.name = (moq_bytes_t){(const uint8_t *)"video", 5};
    moqr_fetch_t fetch; moqr_fetch_plan_t plan;
    REQUIRE(moqr_core_fetch_open(c, d, &req, 1, &fetch, &plan) == MOQR_OK && plan.admit == MOQR_FETCH_ACCEPT);
    drain(c);
    ma = (moq_cluster_metadata_t){NULL, 0, true, 2};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, true) == MOQ_OK);
    moqr_track_t same_track;
    REQUIRE(moqr_core_sub_track(c, sub, &same_track, NULL) == MOQR_OK);
    REQUIRE(moqr_track_eq(same_track, up.track));
    moqr_fetch_item_t retained;
    REQUIRE(moqr_core_fetch_peek(c, fetch, 1, &retained) == MOQR_OK);
    drain(c);
    for (unsigned i = 0; i < 1000; i++) {
        moqr_result_t rc = moqr_core_track_status(c, d, ns, req.name, i);
        if (rc == MOQR_ERR_WOULD_BLOCK) break;
        REQUIRE(rc == MOQR_OK && i != 999);
    }
    ma = (moq_cluster_metadata_t){loop, 3, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, true) == MOQ_ERR_WOULD_BLOCK);
    moqr_cluster_route_t route;
    REQUIRE(moqr_core_cluster_route(c, d, ns, &route, sizeof(route)) == MOQ_OK);
    drain(c);
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, true) == MOQ_OK);
    REQUIRE(moqr_core_cluster_route(c, d, ns, &route, sizeof(route)) == MOQ_DONE);
    moqr_intent_t intents[16]; size_t n = moqr_core_poll_intents(c, intents, 16);
    bool terminated = false;
    for (size_t i = 0; i < n; i++) if (intents[i].kind == MOQR_INTENT_SUB_DONE) terminated = true;
    REQUIRE(terminated);
    moqr_sub_state_t state;
    REQUIRE(moqr_core_sub_state(c, sub, &state) == MOQR_ERR_STALE_HANDLE);
    moqr_fetch_item_t item;
    REQUIRE(moqr_core_fetch_peek(c, fetch, 1, &item) == MOQR_ERR_STALE_HANDLE);
    moqr_core_destroy(c);
}

static void test_failover_uses_surviving_requester(void)
{
    moqr_core_t *c = new_core(50, 8, 4);
    moqr_binding_t a = binding(c, 1, 10, 1), b = binding(c, 2, 20, 1);
    moqr_binding_t first = binding(c, 3, 30, 1), survivor = binding(c, 4, 40, 1);
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts);
    uint64_t pa[] = {99, 10}, pb[] = {99, 30, 20};
    moq_cluster_metadata_t ma = {pa, 2, true, 0}, mb = {pb, 3, true, 8};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, false) == MOQ_OK);
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 2, &mb, false) == MOQ_OK);
    moqr_sub_t original;
    moqr_intent_t up = subscribe_track(c, first, ns, 1, &original);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 123, false, 0, 0) == MOQR_OK);
    drain(c);
    moqr_subscribe_req_t req; moqr_subscribe_req_init(&req);
    req.ns = ns; req.name = (moq_bytes_t){(const uint8_t *)"video", 5}; req.cookie = 2;
    moqr_sub_t remaining;
    REQUIRE(moqr_core_subscribe(c, survivor, &req, &remaining) == MOQR_OK);
    moqr_track_t shared;
    REQUIRE(moqr_core_sub_track(c, remaining, &shared, NULL) == MOQR_OK && moqr_track_eq(shared, up.track));
    REQUIRE(moqr_core_unsubscribe(c, original, 1) == MOQR_OK);
    drain(c);
    REQUIRE(moqr_core_binding_close(c, a, 2) == MOQR_OK);
    moqr_intent_t intents[16], next = {0};
    size_t n = moqr_core_poll_intents(c, intents, 16);
    for (size_t i = 0; i < n; i++) if (intents[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) next = intents[i];
    REQUIRE(next.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE && next.binding_cookie == 2);
    moqr_cluster_advertisement_id_t pinned; uint64_t hop, correlation;
    REQUIRE(moqr_core_internal_cluster_upstream_route(c, next.track, next.track_gen, &pinned, &hop, &correlation) == MOQR_OK);
    REQUIRE(hop == 40 && correlation == 0); /* cross-shard dispatch uses live requester */
    REQUIRE(moqr_core_sub_track(c, remaining, &shared, NULL) == MOQR_OK);
    moqr_core_destroy(c);
}

static void test_failover_closes_all_old_groups(void)
{
    moqr_core_t *c = new_core(50, 4, 4);
    moqr_binding_t a = binding(c, 1, 10, 1), b = binding(c, 2, 20, 1), d = binding(c, 3, 30, 1);
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts);
    uint64_t pa[] = {99, 10}, pb[] = {99, 20};
    moq_cluster_metadata_t ma = {pa, 2, true, 0}, mb = {pb, 2, true, 8};
    REQUIRE(moqr_core_announce_cluster(c, a, ns, 1, &ma, false) == MOQ_OK);
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 2, &mb, false) == MOQ_OK);
    moqr_sub_t sub; moqr_intent_t up = subscribe_track(c, d, ns, 1, &sub);
    REQUIRE(moqr_core_upstream_ok(c, up.track, up.track_gen, 123, false, 0, 0) == MOQR_OK);
    moqr_log_append_desc_t object; moqr_log_append_desc_init(&object);
    REQUIRE(moqr_core_ingest(c, up.track, &object) == MOQR_OK);
    moqr_delivery_t delivery;
    REQUIRE(moqr_core_next_delivery(c, d, 1, &delivery) == MOQR_OK && delivery.rec.group_id == 0);
    REQUIRE(moqr_core_delivery_done(c, d, MOQR_DELIVERY_DELIVERED, 1) == MOQR_OK);
    for (uint64_t group = 1; group <= 2; group++) {
        moqr_log_append_desc_init(&object);
        object.group_id = group; object.obj_state = MOQR_OBJ_OPEN; object.declared_len = 2;
        REQUIRE(moqr_core_ingest(c, up.track, &object) == MOQR_OK);
        moq_rcbuf_t *chunk; const uint8_t byte = 42;
        REQUIRE(moq_rcbuf_create(moq_alloc_default(), &byte, 1, &chunk) == MOQ_OK);
        REQUIRE(moqr_core_append_chunk(c, up.track, group, 0, 0, chunk) == MOQR_OK);
        moq_rcbuf_decref(chunk);
    }
    moqr_log_append_desc_init(&object); object.group_id = 3;
    REQUIRE(moqr_core_ingest(c, up.track, &object) == MOQR_OK);
    REQUIRE(moqr_core_seal_subgroup(c, up.track, 3, 0) == MOQR_OK);
    drain(c);
    REQUIRE(moqr_core_unannounce(c, a, ns) == MOQR_OK);
    moqr_core_stats_t stats; moqr_core_get_stats(c, &stats);
    REQUIRE(stats.retained_bytes == 0); /* every old partial payload released */
    moqr_intent_t intents[16], next = {0}; size_t n = moqr_core_poll_intents(c, intents, 16);
    for (size_t i = 0; i < n; i++) if (intents[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) next = intents[i];
    REQUIRE(next.kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    REQUIRE(moqr_core_upstream_ok(c, next.track, next.track_gen, 124, false, 0, 0) == MOQR_OK);
    bool old_complete_reset = false, sealed_complete_fin = false;
    for (unsigned i = 0; i < 16; i++) {
        moqr_result_t rc = moqr_core_next_delivery(c, d, 2, &delivery);
        if (rc == MOQR_DONE) break;
        REQUIRE(rc == MOQR_OK);
        if (delivery.notice == MOQR_DELIVERY_NOTICE_SEAL && delivery.rec.group_id == 0) {
            REQUIRE(delivery.seal_reset); old_complete_reset = true;
        }
        if (delivery.notice == MOQR_DELIVERY_NOTICE_SEAL && delivery.rec.group_id == 3) {
            REQUIRE(!delivery.seal_reset); sealed_complete_fin = true;
        }
        REQUIRE(moqr_core_delivery_done(c, d, MOQR_DELIVERY_DELIVERED, 2) == MOQR_OK);
    }
    REQUIRE(old_complete_reset && sealed_complete_fin);
    moqr_core_destroy(c);
}


/* Established PUBLISH needs no namespace advertisement (D22 §7.6). The
 * exact track is eligible; this does not grant access to sibling names. */
static void test_direct_publish(bool cluster_enabled)
{
    moqr_core_relay_cfg_t cfg; moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default());
    moqr_cluster_cfg_t cl; moqr_cluster_cfg_init(&cl);
    cl.enabled = cluster_enabled; cl.local_hop_id = 50; cl.max_routes = 4;
    moqr_core_t *c = NULL;
    REQUIRE(moqr_core_create_cluster(&cfg, &cl, &c) == MOQ_OK);
    moqr_binding_t pub, down;
    REQUIRE(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    REQUIRE(moqr_core_binding_open(c, 2, &down) == MOQR_OK);
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts);
    moq_bytes_t name = MOQ_BYTES_LITERAL("video");
    moqr_track_t pushed;
    REQUIRE(moqr_core_publish_open(c, pub, ns, name, 101, &pushed) == MOQR_OK);
    moqr_intent_t it[8];
    REQUIRE(moqr_core_poll_intents(c, it, 8) == 1 && it[0].kind == MOQR_INTENT_ACCEPT_PUBLISH);
    REQUIRE(it[0].cookie == 101 && it[0].binding_cookie == 1 && moqr_track_eq(it[0].track, pushed));
    moq_rcbuf_t *payload = NULL;
    REQUIRE(moq_rcbuf_create(moq_alloc_default(), (const uint8_t *)"exact", 5, &payload) == MOQ_OK);
    moqr_log_append_desc_t object; moqr_log_append_desc_init(&object);
    object.payload = payload;
    REQUIRE(moqr_core_ingest(c, pushed, &object) == MOQR_OK); /* transfers payload */
    moqr_tracks_req_t tracks; moqr_tracks_req_init_sized(&tracks, sizeof(tracks));
    tracks.prefix = ns; tracks.cookie = 301;
    REQUIRE(moqr_core_tracks_subscribe(c, down, &tracks) == MOQR_OK);
    moqr_discovery_t found;
    REQUIRE(moqr_core_next_discovery(c, down, &found, sizeof(found)) == MOQR_OK);
    REQUIRE(moqr_track_eq(found.track, pushed) && found.name.len == 5 &&
           memcmp(found.name.data, "video", 5) == 0);
    tracks.cookie = 302;
    REQUIRE(moqr_core_tracks_subscribe(c, pub, &tracks) == MOQR_OK);
    REQUIRE(moqr_core_next_discovery(c, pub, &found, sizeof(found)) == MOQR_DONE);
    drain(c);
    moqr_subscribe_req_t req; moqr_subscribe_req_init(&req);
    req.ns = ns; req.name = name; req.cookie = 201;
    req.filter.type = MOQR_FILTER_ABSOLUTE_START;
    moqr_sub_t sub;
    REQUIRE(moqr_core_subscribe(c, down, &req, &sub) == MOQR_OK);
    REQUIRE(moqr_core_poll_intents(c, it, 8) == 1 && it[0].kind == MOQR_INTENT_ACCEPT_SUB);
    moqr_track_t selected;
    REQUIRE(moqr_core_sub_track(c, sub, &selected, NULL) == MOQR_OK && moqr_track_eq(selected, pushed));
    moqr_delivery_t delivery;
    REQUIRE(moqr_core_next_delivery(c, down, 1, &delivery) == MOQR_OK);
    REQUIRE(delivery.rec.payload != NULL && moq_rcbuf_len(delivery.rec.payload) == 5 &&
           memcmp(moq_rcbuf_data(delivery.rec.payload), "exact", 5) == 0);
    REQUIRE(moqr_core_delivery_done(c, down, MOQR_DELIVERY_DELIVERED, 1) == MOQR_OK);
    moqr_fetch_req_t fetch_req; moqr_fetch_req_init(&fetch_req);
    fetch_req.ns = ns; fetch_req.name = name;
    moqr_fetch_t fetch; moqr_fetch_plan_t plan;
    REQUIRE(moqr_core_fetch_open(c, down, &fetch_req, 1, &fetch, &plan) == MOQR_OK);
    REQUIRE(plan.admit == MOQR_FETCH_ACCEPT);
    moqr_fetch_item_t item;
    REQUIRE(moqr_core_fetch_peek(c, fetch, 1, &item) == MOQR_OK);
    REQUIRE(moqr_core_fetch_close(c, fetch) == MOQR_OK);
    req.name = MOQ_BYTES_LITERAL("other");
    REQUIRE(moqr_core_subscribe(c, down, &req, &sub) == MOQR_OK);
    REQUIRE(moqr_core_poll_intents(c, it, 8) == 1 && it[0].kind == MOQR_INTENT_REJECT_SUB);
    moqr_core_destroy(c);
}

static moqr_core_t *owner_core(uint16_t shard)
{
    moqr_core_relay_cfg_t cfg; moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default());
    cfg.shard_index = shard; cfg.shard_count = 2;
    moqr_cluster_cfg_t cl; moqr_cluster_cfg_init(&cl);
    cl.enabled = true; cl.local_hop_id = 50; cl.max_routes = 8; cl.max_hops = 4;
    moqr_core_t *c = NULL;
    REQUIRE(moqr_core_create_cluster(&cfg, &cl, &c) == MOQ_OK);
    return c;
}

static void test_original_owner_identity(void)
{
    moqr_core_t *c = owner_core(0), *other = owner_core(1);
    /* Deliberately equal local cookie, pool slot, generation and serial. */
    moqr_binding_t local = binding(c, 1, 11, 1), remote = binding(other, 1, 22, 1);
    REQUIRE(moq_handle_slot(local._opaque) == moq_handle_slot(remote._opaque));
    REQUIRE(moq_handle_generation(local._opaque) == moq_handle_generation(remote._opaque));
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts);
    uint64_t lp[] = {11}, rp[] = {22};
    moq_cluster_metadata_t lm = {lp, 1, true, 9}, rm = {rp, 1, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, local, ns, 101, &lm, false) == MOQ_OK);
    REQUIRE(moqr_core_announce_cluster(other, remote, ns, 102, &rm, false) == MOQ_OK);
    moqr_cluster_route_t exact, best;
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &exact, sizeof(exact)) == MOQ_OK);
    moqr_cluster_export_t imported;
    REQUIRE(moqr_core_internal_cluster_export(other, 0, &imported) == MOQR_OK);
    REQUIRE(exact.advertisement.serial == imported.route.advertisement.serial);
    REQUIRE(!moqr_cluster_advertisement_id_eq(exact.advertisement, imported.route.advertisement));
    moqr_binding_t mirror = binding(c, MOQR_SHARD_COOKIE_BASE + 1, 0, 0);
    moqr_binding_t requester = binding(c, 3, 30, 1);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &imported, false) == MOQR_OK);
    REQUIRE(moqr_core_cluster_route(c, requester, ns, &best, sizeof(best)) == MOQ_OK);
    REQUIRE(moqr_cluster_advertisement_id_eq(best.advertisement, imported.route.advertisement));
    moqr_subscribe_req_t req; moqr_subscribe_req_init(&req);
    req.ns = ns; req.name = MOQ_BYTES_LITERAL("video"); req.cookie = 201;
    moqr_sub_t sub;
    REQUIRE(moqr_core_internal_subscribe_cluster(c, requester, &req,
        exact.advertisement, 30, 0, &sub) == MOQR_OK);
    moqr_intent_t it[8]; size_t n = moqr_core_poll_intents(c, it, 8);
    REQUIRE(n == 1 && it[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE && it[0].binding_cookie == 1);
    moqr_cluster_advertisement_id_t pinned; uint64_t hop, correlation;
    REQUIRE(moqr_core_internal_cluster_upstream_route(c, it[0].track, it[0].track_gen,
        &pinned, &hop, &correlation) == MOQR_OK);
    REQUIRE(moqr_cluster_advertisement_id_eq(pinned, exact.advertisement) && hop == 30 && correlation == 0);
    REQUIRE(moqr_core_upstream_ok(c, it[0].track, it[0].track_gen, 301, false, 0, 0) == MOQR_OK);
    moqr_log_append_desc_t object; moqr_log_append_desc_init(&object);
    REQUIRE(moqr_core_ingest(c, it[0].track, &object) == MOQR_OK);
    drain(c);
    moqr_fetch_req_t fq; moqr_fetch_req_init(&fq); fq.ns = ns; fq.name = req.name;
    moqr_fetch_t fetch = MOQR_FETCH_INVALID; moqr_core_retrieve_plan_t plan;
    REQUIRE(moqr_core_internal_retrieve_open_cluster(c, requester, &fq,
        exact.advertisement, 30, 0, 1, &fetch, &plan) == MOQR_OK);
    REQUIRE(plan.wire.admit == MOQR_FETCH_ACCEPT && moqr_track_eq(plan.track, it[0].track));
    REQUIRE(moqr_core_fetch_close(c, fetch) == MOQR_OK);
    REQUIRE(moqr_core_internal_retrieve_open_cluster(c, requester, &fq,
        imported.route.advertisement, 30, 0, 1, &fetch, &plan) == MOQR_ERR_STALE_HANDLE);
    /* A live owner identity does not waive recipient exclusions. */
    REQUIRE(moqr_core_internal_subscribe_cluster(c, requester, &req,
        exact.advertisement, 11, 0, &sub) == MOQR_OK);
    REQUIRE(moqr_core_poll_intents(c, it, 8) == 1 && it[0].kind == MOQR_INTENT_REJECT_SUB);
    /* Foreign owner cannot be resolved through the locally cheaper mirror. */
    REQUIRE(moqr_core_internal_subscribe_cluster(c, requester, &req,
        imported.route.advertisement, 30, 0, &sub) == MOQR_ERR_STALE_HANDLE);
    REQUIRE(moqr_core_poll_intents(c, it, 8) == 0);
    REQUIRE(moqr_core_unannounce(c, local, ns) == MOQR_OK); drain(c);
    REQUIRE(moqr_core_announce_cluster(c, local, ns, 103, &lm, false) == MOQ_OK);
    REQUIRE(moqr_core_internal_subscribe_cluster(c, requester, &req,
        exact.advertisement, 30, 0, &sub) == MOQR_ERR_STALE_HANDLE);
    REQUIRE(moqr_core_binding_close(c, local, 2) == MOQR_OK); drain(c);
    moqr_binding_t replacement = binding(c, 1, 11, 1);
    REQUIRE(!moqr_binding_eq(local, replacement));
    REQUIRE(moqr_core_announce_cluster(c, replacement, ns, 104, &lm, false) == MOQ_OK);
    REQUIRE(moqr_core_internal_subscribe_cluster(c, requester, &req,
        exact.advertisement, 30, 0, &sub) == MOQR_ERR_STALE_HANDLE);
    REQUIRE(moqr_core_poll_intents(c, it, 8) == 0);
    moqr_core_destroy(c); moqr_core_destroy(other);
}

static void test_cluster_sizes_and_exhaustion(void)
{
    moqr_core_t *c = new_core(0, 4, 4);
    moqr_binding_t b; REQUIRE(moqr_core_binding_open(c, 7, &b) == MOQR_OK);
    moqr_cluster_peer_cfg_t peer; moqr_cluster_peer_cfg_init(&peer);
    REQUIRE(peer.struct_size == MOQR_CLUSTER_PEER_CFG_V0_SIZE && peer.effective_relay_cost == 1);
    REQUIRE(moqr_core_binding_set_cluster(c, b, NULL) == MOQ_OK);
    peer.peer_hop_id = 1;
    REQUIRE(moqr_core_binding_set_cluster(c, b, &peer) == MOQ_ERR_INVAL);
    peer.peer_hop_id = 0; peer.effective_relay_cost = 0;
    peer.struct_size = MOQR_CLUSTER_PEER_CFG_V0_SIZE - 1;
    REQUIRE(moqr_core_binding_set_cluster(c, b, &peer) == MOQ_ERR_INVAL);
    peer.struct_size = sizeof(peer);
    REQUIRE(moqr_core_binding_set_cluster(c, b, &peer) == MOQ_OK);
    moq_bytes_t parts[2]; moqr_ns_t ns = demo(parts), deep = {parts, 2};
    uint64_t path = 0; moq_cluster_metadata_t md = {&path, 1, true, 0};
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 1, &md, false) == MOQ_OK);
    moqr_cluster_route_t route;
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_OK);
    REQUIRE(route.accumulated_cost == 0 && route.origin_id == 0);
    moqr_cluster_advertisement_id_t id = route.advertisement;
    struct { moqr_cluster_info_t value; uint64_t canary; } info;
    memset(&info, 0xa5, sizeof(info));
    REQUIRE(moqr_core_cluster_info(c, &info.value, MOQR_CLUSTER_INFO_V0_SIZE - 1) == MOQ_ERR_INVAL);
    const uint8_t *bytes = (const uint8_t *)&info;
    for (size_t i = 0; i < sizeof(info); i++) REQUIRE(bytes[i] == 0xa5);
    REQUIRE(moqr_core_cluster_info(c, &info.value, MOQR_CLUSTER_INFO_V0_SIZE) == MOQ_OK);
    REQUIRE(info.value.struct_size == MOQR_CLUSTER_INFO_V0_SIZE && info.value.routes_used == 1 &&
           info.canary == UINT64_C(0xa5a5a5a5a5a5a5a5));
    moqr_cluster_route_t before = route;
    REQUIRE(moqr_core_cluster_route(c, b, ns, &route, sizeof(route)) == MOQ_DONE);
    REQUIRE(memcmp(&before, &route, sizeof(route)) == 0);
    REQUIRE(moqr_core_binding_set_cluster(c, b, NULL) == MOQ_ERR_WRONG_STATE);
    c->cluster_sequence = UINT64_MAX - 1; /* private white-box boundary setup */
    uint64_t epoch = c->route_epoch;
    REQUIRE(moqr_core_announce_cluster(c, b, deep, 2, &md, false) == MOQ_ERR_LIMIT);
    REQUIRE(c->route_epoch == epoch && c->cluster_sequence == UINT64_MAX - 1);
    md.route_cost = 3;
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 1, &md, true) == MOQ_OK);
    REQUIRE(c->cluster_sequence == UINT64_MAX);
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_OK);
    REQUIRE(moqr_cluster_advertisement_id_eq(route.advertisement, id) && route.accumulated_cost == 3);
    md.route_cost = 9; before = route; epoch = c->route_epoch;
    REQUIRE(moqr_core_announce_cluster(c, b, ns, 1, &md, true) == MOQ_ERR_LIMIT);
    REQUIRE(c->route_epoch == epoch);
    REQUIRE(moqr_core_cluster_route(c, MOQR_BINDING_INVALID, ns, &route, sizeof(route)) == MOQ_OK);
    REQUIRE(route.accumulated_cost == before.accumulated_cost &&
           moqr_cluster_advertisement_id_eq(route.advertisement, id));
    REQUIRE(moqr_core_unannounce(c, b, ns) == MOQR_OK);
    moqr_binding_t mirror = binding(c, MOQR_SHARD_COOKIE_BASE + 1, 0, 0);
    uint64_t remote_path[] = {91, 11};
    moqr_cluster_export_t imported = imported_route(ns, 0, remote_path, 101, 201, 1);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &imported, false) == MOQR_INTERNAL_LIMIT);
    REQUIRE(moqr_core_cluster_info(c, &info.value, sizeof(info.value)) == MOQ_OK && info.value.routes_used == 0);
    REQUIRE(moqr_core_internal_cluster_import(c, mirror, &imported, true) == MOQR_OK);
    REQUIRE(moqr_core_binding_close(c, b, 1) == MOQR_OK);
    /* Existing full-handle generation retirement, not wrap/reuse. */
    uint32_t slot = moq_handle_slot(b._opaque);
    c->bindings[slot].gen = R_GEN_LAST_LIVE - 1u;
    moqr_binding_t last, next;
    REQUIRE(moqr_core_binding_open(c, 7, &last) == MOQR_OK);
    REQUIRE(moq_handle_slot(last._opaque) == slot && moq_handle_generation(last._opaque) == R_GEN_LAST_LIVE);
    REQUIRE(moqr_core_binding_close(c, last, 2) == MOQR_OK);
    REQUIRE(c->bindings[slot].gen == R_GEN_RETIRED);
    REQUIRE(moqr_core_binding_open(c, 7, &next) == MOQR_OK);
    REQUIRE(moq_handle_slot(next._opaque) != slot);
    REQUIRE(moqr_core_binding_set_cluster(c, last, NULL) == MOQ_ERR_STALE_HANDLE);
    moqr_core_destroy(c);
}

int main(int argc, char **argv)
{
    if (argc > 1) {
        if (strcmp(argv[1], "direct_publish") == 0) { test_direct_publish(false); test_direct_publish(true); }
        else if (strcmp(argv[1], "owner_identity") == 0) test_original_owner_identity();
        else if (strcmp(argv[1], "sizes") == 0) test_cluster_sizes_and_exhaustion();
        else if (strcmp(argv[1], "cleanup") == 0) test_failover_closes_all_old_groups();
        else if (strcmp(argv[1], "survivor") == 0) test_failover_uses_surviving_requester();
        else if (strcmp(argv[1], "chunk") == 0) test_source_pinning_warm_and_origins();
        else if (strcmp(argv[1], "reparent") == 0) test_import_reparent_exact_route();
        else if (strcmp(argv[1], "failover") == 0) test_import_same_shard_failover();
        else if (strcmp(argv[1], "draining") == 0) test_withdraw_draining_active_subscriber();
        else if (strcmp(argv[1], "update_loop") == 0) test_update_invalidates_live_loop();
        else return 2;
        return 0;
    }
    test_direct_publish(false);
    test_direct_publish(true);
    test_original_owner_identity();
    test_cluster_sizes_and_exhaustion();
    test_failover_closes_all_old_groups();
    test_failover_uses_surviving_requester();
    test_import_reparent_exact_route();
    test_import_same_shard_failover();
    test_withdraw_draining_active_subscriber();
    test_update_invalidates_live_loop();
    test_import_selected_source_pin();
    test_anonymous_push_isolation();
    test_backpressure_and_pool_rollback();
    test_alternatives();
    test_selection_and_limits();
    test_source_pinning_warm_and_origins();
    puts("relay cluster: ok");
    return 0;
}
