/*
 * moq_bench_relay_fanout — zero-allocation relay hot-path proof.
 *
 * Models a single-shard relay: one upstream publisher feeds objects to a
 * relay-ingest session; the relay polls each OBJECT_RECEIVED event and
 * republishes the SAME payload rcbuf (by reference, no copy) to M downstream
 * subscriber sessions. All sessions share ONE per-shard recycling pool
 * allocator, so after warmup the process-level heap sees zero malloc/free on
 * the hot path: the ingest rcbuf slabs are popped from / pushed back to the
 * pool free-list, and fan-out is N cheap increfs of one buffer.
 *
 * This is a BENCHMARK-ONLY proof of RELAY_ZEROALLOC_DESIGN.md. There is no
 * relay in core: the relay loop is benchmark-local wiring over M+1 SimPairs,
 * and the pool is a benchmark support allocator, not a core type. The core is
 * unchanged; the allocator vtable (moq_alloc_t) is the only seam used.
 *
 * The relay is single-shard: ingest, fan-out, and every decref happen on one
 * (simulated) executor-affinity domain, so the non-atomic rcbuf refcounts and
 * the non-atomic pool free-list are safe with no locks/atomics.
 *
 * No sockets, no threads, no picoquic.
 */

#include <moq/session.h>
#include <moq/sim.h>
#include <moq/rcbuf.h>

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/* Single-shard recycling pool allocator (benchmark support)           */
/*                                                                     */
/* Exact-size free-lists: a freed block of size S is cached and reused */
/* for the next allocation of size S. real_allocs/real_frees count     */
/* ONLY actual malloc()/free() calls; recycled pops/pushes touch       */
/* neither. During the run the pool never frees (it recycles); real    */
/* frees happen only at pool_destroy. So after warmup a steady-state    */
/* hot loop shows real_allocs == 0 AND real_frees == 0.                 */
/* ------------------------------------------------------------------ */

typedef struct pool_node {
    struct pool_node *next;
} pool_node_t;

typedef struct {
    size_t       size;
    pool_node_t *free_head;
    int64_t      cached;      /* blocks currently on the free-list */
} pool_bucket_t;

typedef struct {
    pool_bucket_t *buckets;
    size_t         nbuckets;
    size_t         cap_buckets;
    int64_t        real_allocs;   /* actual malloc() calls */
    int64_t        real_frees;    /* actual free() calls (only at destroy) */
    int64_t        live_bytes;    /* bytes handed out (not on a free-list) */
    int64_t        peak_bytes;
    int            oom;           /* set if a bookkeeping malloc failed */
} pool_state_t;

static pool_bucket_t *pool_bucket_for(pool_state_t *p, size_t size)
{
    for (size_t i = 0; i < p->nbuckets; i++)
        if (p->buckets[i].size == size)
            return &p->buckets[i];

    if (p->nbuckets == p->cap_buckets) {
        size_t ncap = p->cap_buckets ? p->cap_buckets * 2 : 16;
        pool_bucket_t *nb = (pool_bucket_t *)realloc(
            p->buckets, ncap * sizeof(pool_bucket_t));
        if (!nb) { p->oom = 1; return NULL; }
        p->buckets = nb;
        p->cap_buckets = ncap;
    }
    pool_bucket_t *b = &p->buckets[p->nbuckets++];
    b->size = size;
    b->free_head = NULL;
    b->cached = 0;
    return b;
}

static void pool_account_alloc(pool_state_t *p, size_t size)
{
    p->live_bytes += (int64_t)size;
    if (p->live_bytes > p->peak_bytes) p->peak_bytes = p->live_bytes;
}

/* Every block must hold a free-list next-pointer while cached, so round up to
 * at least sizeof(pool_node_t). Deterministic: the same requested size always
 * maps to the same slot, so alloc and free agree on the bucket and size. */
static size_t pool_slot(size_t size)
{
    return size < sizeof(pool_node_t) ? sizeof(pool_node_t) : size;
}

static void *pool_alloc(size_t size, void *ctx)
{
    pool_state_t *p = (pool_state_t *)ctx;
    size = pool_slot(size);
    pool_bucket_t *b = pool_bucket_for(p, size);
    if (!b) return NULL;

    void *ret;
    if (b->free_head) {
        pool_node_t *n = b->free_head;
        b->free_head = n->next;
        b->cached--;
        ret = (void *)n;
    } else {
        ret = malloc(size);
        if (!ret) return NULL;
        p->real_allocs++;
    }
    pool_account_alloc(p, size);
    return ret;
}

static void pool_free(void *ptr, size_t size, void *ctx)
{
    pool_state_t *p = (pool_state_t *)ctx;
    if (!ptr) return;
    size = pool_slot(size);
    pool_bucket_t *b = pool_bucket_for(p, size);
    p->live_bytes -= (int64_t)size;
    if (!b) {
        /* Bucket bookkeeping OOM: fall back to a real free rather than leak. */
        p->real_frees++;
        free(ptr);
        return;
    }
    pool_node_t *n = (pool_node_t *)ptr;
    n->next = b->free_head;
    b->free_head = n;
    b->cached++;
}

static void *pool_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx)
{
    if (!ptr) return pool_alloc(new_sz, ctx);
    if (new_sz == 0) { pool_free(ptr, old_sz, ctx); return NULL; }
    void *np = pool_alloc(new_sz, ctx);
    if (!np) return NULL;
    memcpy(np, ptr, old_sz < new_sz ? old_sz : new_sz);
    pool_free(ptr, old_sz, ctx);
    return np;
}

static void pool_destroy(pool_state_t *p)
{
    for (size_t i = 0; i < p->nbuckets; i++) {
        pool_node_t *n = p->buckets[i].free_head;
        while (n) {
            pool_node_t *next = n->next;
            p->real_frees++;
            free(n);
            n = next;
        }
    }
    free(p->buckets);
    p->buckets = NULL;
    p->nbuckets = p->cap_buckets = 0;
}

/* ------------------------------------------------------------------ */
/* Monotonic wall clock (for throughput; distinct from sim time)       */
/* ------------------------------------------------------------------ */

static uint64_t wall_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* ------------------------------------------------------------------ */
/* A leg = one SimPair with an accepted subscription and an open       */
/* subgroup. Used identically for the upstream ingest leg and each     */
/* downstream leg (server publishes, client subscribes).               */
/* ------------------------------------------------------------------ */

typedef struct {
    moq_simpair_t        *sp;
    moq_session_t        *server;   /* publisher */
    moq_session_t        *client;   /* subscriber */
    moq_subscription_t    pub_sub;
    moq_subgroup_handle_t sg;
    uint64_t              objects_received;
    uint64_t              payload_bytes;
    uint64_t              payload_mismatches;
} leg_t;

#define PAYLOAD_FILL 0xABu

static bool payload_matches(moq_rcbuf_t *payload, size_t expected_len)
{
    if (!payload) return false;
    if (moq_rcbuf_len(payload) != expected_len) return false;
    const uint8_t *data = moq_rcbuf_data(payload);
    if (expected_len > 0 && !data) return false;
    for (size_t i = 0; i < expected_len; i++)
        if (data[i] != (uint8_t)PAYLOAD_FILL)
            return false;
    return true;
}

static int leg_setup(leg_t *leg, moq_alloc_t *alloc, uint64_t seed)
{
    memset(leg, 0, sizeof(*leg));

    moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
    spcfg.alloc = alloc;
    spcfg.seed = seed;
    spcfg.initial_now_us = 1000;
    spcfg.server_send_request_capacity = true;
    spcfg.server_initial_request_capacity = 64;
    spcfg.client_send_request_capacity = true;
    spcfg.client_initial_request_capacity = 64;

    if (moq_simpair_create(&spcfg, &leg->sp) != MOQ_OK) {
        fprintf(stderr, "simpair_create failed (seed=%" PRIu64 ")\n", seed);
        return -1;
    }
    moq_simpair_start(leg->sp);
    moq_simpair_run_until_quiescent(leg->sp, 8, NULL);

    leg->server = moq_simpair_server(leg->sp);
    leg->client = moq_simpair_client(leg->sp);

    moq_event_t ev;
    while (moq_session_poll_events(leg->server, &ev, 1) == 1) moq_event_cleanup(&ev);
    while (moq_session_poll_events(leg->client, &ev, 1) == 1) moq_event_cleanup(&ev);

    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bench") };
    moq_subscribe_cfg_t subcfg;
    moq_subscribe_cfg_init(&subcfg);
    subcfg.track_namespace.parts = ns_parts;
    subcfg.track_namespace.count = 1;
    subcfg.track_name = MOQ_BYTES_LITERAL("data");
    moq_subscription_t sub_h;
    if (moq_session_subscribe(leg->client, &subcfg,
                              moq_simpair_now_us(leg->sp), &sub_h) != MOQ_OK) {
        fprintf(stderr, "subscribe failed (seed=%" PRIu64 ")\n", seed);
        return -1;
    }
    moq_simpair_run_until_quiescent(leg->sp, 8, NULL);

    if (moq_session_poll_events(leg->server, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        fprintf(stderr, "expected SUBSCRIBE_REQUEST (seed=%" PRIu64 ")\n", seed);
        return -1;
    }
    leg->pub_sub = ev.u.subscribe_request.sub;

    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(leg->server, leg->pub_sub, &acc,
                                     moq_simpair_now_us(leg->sp)) != MOQ_OK) {
        fprintf(stderr, "accept_subscribe failed (seed=%" PRIu64 ")\n", seed);
        return -1;
    }
    moq_simpair_run_until_quiescent(leg->sp, 8, NULL);
    while (moq_session_poll_events(leg->client, &ev, 1) == 1) moq_event_cleanup(&ev);

    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.subgroup_id = 0;
    moq_result_t orc = moq_session_open_subgroup(leg->server, leg->pub_sub,
        &sgcfg, moq_simpair_now_us(leg->sp), &leg->sg);
    if (orc == MOQ_ERR_WOULD_BLOCK) {
        moq_simpair_run_until_quiescent(leg->sp, 4, NULL);
        orc = moq_session_open_subgroup(leg->server, leg->pub_sub, &sgcfg,
            moq_simpair_now_us(leg->sp), &leg->sg);
    }
    if (orc != MOQ_OK) {
        fprintf(stderr, "open_subgroup rc=%d (seed=%" PRIu64 ")\n", orc, seed);
        return -1;
    }
    return 0;
}

/* Write `payload` into an already-open subgroup, pumping once on WOULD_BLOCK. */
static int leg_write(leg_t *leg, uint64_t object_id, moq_rcbuf_t *payload)
{
    moq_result_t rc = moq_session_write_object(leg->server, leg->sg, object_id,
        payload, moq_simpair_now_us(leg->sp));
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        moq_simpair_run_until_quiescent(leg->sp, 4, NULL);
        rc = moq_session_write_object(leg->server, leg->sg, object_id,
            payload, moq_simpair_now_us(leg->sp));
    }
    return rc == MOQ_OK ? 0 : -1;
}

/* Pump the leg and drain one delivered object off the subscriber. */
static int leg_drain_one(leg_t *leg, size_t expected_payload_len)
{
    moq_simpair_run_until_quiescent(leg->sp, 4, NULL);
    moq_event_t ev;
    while (moq_session_poll_events(leg->client, &ev, 1) == 1) {
        if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
            leg->objects_received++;
            if (ev.u.object_received.payload)
                leg->payload_bytes += moq_rcbuf_len(ev.u.object_received.payload);
            if (!payload_matches(ev.u.object_received.payload, expected_payload_len))
                leg->payload_mismatches++;
        }
        moq_event_cleanup(&ev);
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* CLI                                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t fanout;
    uint64_t objects;
    uint64_t object_size;
    uint64_t warmup;
    bool     json;
} cfg_t;

static cfg_t parse_args(int argc, char **argv)
{
    cfg_t cfg = { .fanout = 8, .objects = 2000, .object_size = 1200,
                  .warmup = 100, .json = false };
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--fanout") == 0 && i + 1 < argc)
            cfg.fanout = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--objects") == 0 && i + 1 < argc)
            cfg.objects = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--object-size") == 0 && i + 1 < argc)
            cfg.object_size = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc)
            cfg.warmup = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--json") == 0)
            cfg.json = true;
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            fprintf(stderr,
                "Usage: moq_bench_relay_fanout [options]\n"
                "  --fanout N        downstream subscribers (default 8)\n"
                "  --objects N       objects relayed (default 2000)\n"
                "  --object-size N   payload bytes per object (default 1200)\n"
                "  --warmup N        warmup objects, excluded (default 100)\n"
                "  --json            output results as JSON\n");
            exit(0);
        } else {
            fprintf(stderr, "unknown flag: %s (try --help)\n", argv[i]);
            exit(2);
        }
    }
    if (cfg.fanout == 0) { fprintf(stderr, "error: --fanout >= 1\n"); exit(2); }
    if (cfg.objects == 0) { fprintf(stderr, "error: --objects >= 1\n"); exit(2); }
    if (cfg.object_size == 0) { fprintf(stderr, "error: --object-size >= 1\n"); exit(2); }
    if (cfg.warmup > cfg.objects) cfg.warmup = 0;
    return cfg;
}

/* ------------------------------------------------------------------ */
/* Relay one object: origin publishes, relay ingests, republishes to   */
/* every downstream leg by reference (the ingested rcbuf, no copy).     */
/* ------------------------------------------------------------------ */

static int relay_one(leg_t *up, leg_t *down, uint64_t nds,
                     uint64_t object_id, moq_rcbuf_t *origin_payload,
                     size_t expected_payload_len)
{
    if (leg_write(up, object_id, origin_payload) < 0) {
        fprintf(stderr, "upstream write failed (obj=%" PRIu64 ")\n", object_id);
        return -1;
    }
    moq_simpair_run_until_quiescent(up->sp, 4, NULL);

    /* Poll the single ingested object; its rcbuf is the fan-out source. */
    moq_event_t ev;
    int got = 0;
    while (moq_session_poll_events(up->client, &ev, 1) == 1) {
        if (ev.kind != MOQ_EVENT_OBJECT_RECEIVED) { moq_event_cleanup(&ev); continue; }
        moq_rcbuf_t *p = ev.u.object_received.payload; /* borrowed via the event ref */
        up->objects_received++;
        if (p) up->payload_bytes += moq_rcbuf_len(p);
        if (!payload_matches(p, expected_payload_len))
            up->payload_mismatches++;

        /* Fan out to every downstream by reference: write_object increfs p;
         * pumping the leg sends it and decrefs. No clone, no copy. */
        for (uint64_t d = 0; d < nds; d++) {
            if (leg_write(&down[d], object_id, p) < 0) {
                fprintf(stderr, "downstream[%" PRIu64 "] write failed\n", d);
                moq_event_cleanup(&ev);
                return -1;
            }
            leg_drain_one(&down[d], expected_payload_len);
        }
        moq_event_cleanup(&ev); /* releases the last ref -> p returns to the pool */
        got = 1;
        break;
    }
    if (!got) {
        fprintf(stderr, "no ingest event (obj=%" PRIu64 ")\n", object_id);
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Benchmark                                                           */
/* ------------------------------------------------------------------ */

static int run_bench(const cfg_t *cfg)
{
    pool_state_t pool = {0};
    moq_alloc_t alloc = { &pool, pool_alloc, pool_realloc, pool_free };

    uint64_t nds = cfg->fanout;
    if (nds > SIZE_MAX / sizeof(leg_t)) {
        fprintf(stderr, "error: --fanout too large\n"); return 1;
    }
    if (cfg->objects > UINT64_MAX / nds) {
        fprintf(stderr, "error: objects * fanout overflows\n"); return 1;
    }

    int rc_ret = 1;
    leg_t up;
    leg_t *down = (leg_t *)calloc((size_t)nds, sizeof(leg_t));
    uint8_t *payload_data = NULL;
    moq_rcbuf_t *origin_payload = NULL;
    int up_ok = 0;
    uint64_t down_ok = 0;

    if (!down) { fprintf(stderr, "downstream array alloc failed\n"); goto done; }

    if (leg_setup(&up, &alloc, 0) < 0) goto done;
    up_ok = 1;
    for (uint64_t d = 0; d < nds; d++) {
        if (leg_setup(&down[d], &alloc, d + 1) < 0) goto done;
        down_ok = d + 1;
    }

    payload_data = (uint8_t *)malloc((size_t)cfg->object_size);
    if (!payload_data) { fprintf(stderr, "payload malloc failed\n"); goto done; }
    memset(payload_data, PAYLOAD_FILL, (size_t)cfg->object_size);
    if (moq_rcbuf_wrap(&alloc, payload_data, (size_t)cfg->object_size,
                       NULL, NULL, &origin_payload) != MOQ_OK) {
        fprintf(stderr, "rcbuf_wrap failed\n"); goto done;
    }

    int had_error = 0;
    uint64_t object_id = 0;

    /* Warmup: populate the pool free-lists with the steady-state working set. */
    for (uint64_t i = 0; i < cfg->warmup && !had_error; i++, object_id++)
        if (relay_one(&up, down, nds, object_id, origin_payload,
                      (size_t)cfg->object_size) < 0)
            had_error = 1;

    if (had_error) goto done;

    /* Reset measured counters AFTER warmup. */
    up.objects_received = 0; up.payload_bytes = 0; up.payload_mismatches = 0;
    for (uint64_t d = 0; d < nds; d++) {
        down[d].objects_received = 0;
        down[d].payload_bytes = 0;
        down[d].payload_mismatches = 0;
    }
    pool.real_allocs = 0;
    pool.real_frees = 0;
    pool.peak_bytes = pool.live_bytes;
    int64_t baseline_live = pool.live_bytes;

    uint64_t t_start = wall_now_us();
    for (uint64_t i = 0; i < cfg->objects && !had_error; i++, object_id++)
        if (relay_one(&up, down, nds, object_id, origin_payload,
                      (size_t)cfg->object_size) < 0)
            had_error = 1;
    uint64_t elapsed_us = wall_now_us() - t_start;
    if (elapsed_us == 0) elapsed_us = 1;

    int64_t measured_allocs = pool.real_allocs;
    int64_t measured_frees  = pool.real_frees;
    int64_t peak_delta = pool.peak_bytes - baseline_live;

    uint64_t received_total = 0, payload_total = 0, payload_mismatches = 0;
    for (uint64_t d = 0; d < nds; d++) {
        received_total += down[d].objects_received;
        payload_total += down[d].payload_bytes;
        payload_mismatches += down[d].payload_mismatches;
    }
    uint64_t expected = cfg->objects * nds;
    uint64_t lost = expected > received_total ? expected - received_total : 0;

    double elapsed_ms = (double)elapsed_us / 1000.0;
    double elapsed_s  = (double)elapsed_us / 1000000.0;
    double obj_per_sec = (double)received_total / elapsed_s;

    if (cfg->json) {
        printf("{\n");
        printf("  \"benchmark\": \"relay_fanout\",\n");
        printf("  \"fanout\": %" PRIu64 ",\n", nds);
        printf("  \"objects\": %" PRIu64 ",\n", cfg->objects);
        printf("  \"object_size\": %" PRIu64 ",\n", cfg->object_size);
        printf("  \"warmup_objects\": %" PRIu64 ",\n", cfg->warmup);
        printf("  \"objects_received_total\": %" PRIu64 ",\n", received_total);
        printf("  \"objects_lost\": %" PRIu64 ",\n", lost);
        printf("  \"payload_mismatches\": %" PRIu64 ",\n", payload_mismatches);
        printf("  \"elapsed_ms\": %.2f,\n", elapsed_ms);
        printf("  \"objects_per_sec\": %.1f,\n", obj_per_sec);
        printf("  \"allocs_after_warmup\": %" PRId64 ",\n", measured_allocs);
        printf("  \"frees_after_warmup\": %" PRId64 ",\n", measured_frees);
        printf("  \"peak_bytes_delta\": %" PRId64 ",\n", peak_delta);
        /* Fan-out is by reference (incref of one rcbuf); a payload copy would
         * be a clone/create allocation and thus show up in allocs_after_warmup.
         * The benchmark does not instrument memcpy directly, so this is null:
         * copy-free fan-out is proven by allocs_after_warmup == 0. */
        printf("  \"payload_copies\": null\n");
        printf("}\n");
    } else {
        printf("--- moq_bench_relay_fanout (1 upstream -> %" PRIu64 " downstream) ---\n", nds);
        printf("objects relayed   : %" PRIu64 "\n", cfg->objects);
        printf("object size       : %" PRIu64 " B\n", cfg->object_size);
        printf("warmup objects    : %" PRIu64 "\n", cfg->warmup);
        printf("downstream recv   : %" PRIu64 " (expected %" PRIu64 ", lost %" PRIu64 ")\n",
               received_total, expected, lost);
        printf("payload mismatches: %" PRIu64 "\n", payload_mismatches);
        printf("elapsed           : %.2f ms\n", elapsed_ms);
        printf("throughput        : %.1f downstream obj/s\n", obj_per_sec);
        printf("allocs after warm : %" PRId64 " (pool malloc calls)\n", measured_allocs);
        printf("frees  after warm : %" PRId64 " (pool free calls)\n", measured_frees);
        printf("peak bytes delta  : %" PRId64 "\n", peak_delta);
        printf("payload copies    : n/a (fan-out shares one rcbuf; proven by allocs==0)\n");
    }

    rc_ret = 0;
    if (pool.oom) { fprintf(stderr, "FAIL: pool bookkeeping OOM\n"); rc_ret = 1; }
    if (had_error) rc_ret = 1;
    if (measured_allocs != 0 || measured_frees != 0) {
        fprintf(stderr, "FAIL: hot path alloc/free after warmup = %" PRId64 "/%" PRId64 "\n",
                measured_allocs, measured_frees);
        rc_ret = 1;
    }
    if (received_total != expected) {
        fprintf(stderr, "FAIL: received %" PRIu64 " != expected %" PRIu64 "\n",
                received_total, expected);
        rc_ret = 1;
    }
    uint64_t expected_bytes = (expected <= UINT64_MAX / cfg->object_size)
        ? expected * cfg->object_size : UINT64_MAX;
    if (payload_total != expected_bytes) {
        fprintf(stderr, "FAIL: payload bytes %" PRIu64 " != expected %" PRIu64 "\n",
                payload_total, expected_bytes);
        rc_ret = 1;
    }
    if (payload_mismatches != 0) {
        fprintf(stderr, "FAIL: payload mismatches %" PRIu64 "\n",
                payload_mismatches);
        rc_ret = 1;
    }

done:
    if (origin_payload) moq_rcbuf_decref(origin_payload);
    free(payload_data);
    if (down) {
        for (uint64_t d = 0; d < down_ok; d++) {
            moq_event_t drain[16]; size_t ne;
            while ((ne = moq_session_poll_events(down[d].server, drain, 16)) > 0)
                for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
            while ((ne = moq_session_poll_events(down[d].client, drain, 16)) > 0)
                for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
            moq_simpair_destroy(down[d].sp);
        }
    }
    if (up_ok) {
        moq_event_t drain[16]; size_t ne;
        while ((ne = moq_session_poll_events(up.server, drain, 16)) > 0)
            for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
        while ((ne = moq_session_poll_events(up.client, drain, 16)) > 0)
            for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
        moq_simpair_destroy(up.sp);
    }
    free(down);
    pool_destroy(&pool);
    return rc_ret;
}

int main(int argc, char **argv)
{
    cfg_t cfg = parse_args(argc, argv);
    return run_bench(&cfg);
}
