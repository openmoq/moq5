/*
 * moq_bench_session_scale — per-subscriber session scaling benchmark.
 *
 * Creates N independent publisher/subscriber session pairs via SimPair.
 * Writes the same payload to each server session's subscriber, pumps
 * all pairs, and drains all client events per object.  Measures how
 * per-session encode/pump/decode cost scales with subscriber count.
 *
 * Each subscriber has its own independent server session — this does
 * NOT measure shared-publisher fanout or relay forwarding cost.
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
/* Counting allocator                                                  */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t  alloc_count;
    int64_t  free_count;
    int64_t  live_bytes;
    int64_t  peak_bytes;
} bench_alloc_state_t;

static void *bench_alloc(size_t size, void *ctx)
{
    bench_alloc_state_t *s = (bench_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) {
        s->alloc_count++;
        s->live_bytes += (int64_t)size;
        if (s->live_bytes > s->peak_bytes)
            s->peak_bytes = s->live_bytes;
    }
    return p;
}

static void *bench_realloc(void *ptr, size_t old_sz, size_t new_sz,
                            void *ctx)
{
    bench_alloc_state_t *s = (bench_alloc_state_t *)ctx;
    void *p = realloc(ptr, new_sz);
    if (p) {
        s->live_bytes += (int64_t)new_sz - (int64_t)old_sz;
        if (s->live_bytes > s->peak_bytes)
            s->peak_bytes = s->live_bytes;
    }
    return p;
}

static void bench_free(void *ptr, size_t size, void *ctx)
{
    bench_alloc_state_t *s = (bench_alloc_state_t *)ctx;
    if (ptr) {
        s->free_count++;
        s->live_bytes -= (int64_t)size;
    }
    free(ptr);
}

/* ------------------------------------------------------------------ */
/* Monotonic clock                                                     */
/* ------------------------------------------------------------------ */

static uint64_t now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* ------------------------------------------------------------------ */
/* Per-subscriber state                                                */
/* ------------------------------------------------------------------ */

typedef struct {
    moq_simpair_t     *sp;
    moq_subscription_t pub_sub;
    uint64_t           objects_received;
    uint64_t           payload_bytes;
} sub_slot_t;

/* ------------------------------------------------------------------ */
/* CLI                                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t subscribers;
    uint64_t objects;
    uint64_t object_size;
    uint64_t objects_per_group;
    uint64_t warmup;
    bool     json;
} fanout_cfg_t;

static fanout_cfg_t parse_args(int argc, char **argv)
{
    fanout_cfg_t cfg = {
        .subscribers      = 4,
        .objects           = 1000,
        .object_size       = 1200,
        .objects_per_group = 30,
        .warmup            = 50,
        .json              = false,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--subscribers") == 0 && i + 1 < argc)
            cfg.subscribers = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--objects") == 0 && i + 1 < argc)
            cfg.objects = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--object-size") == 0 && i + 1 < argc)
            cfg.object_size = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--objects-per-group") == 0 && i + 1 < argc)
            cfg.objects_per_group = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc)
            cfg.warmup = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--json") == 0)
            cfg.json = true;
        else if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            fprintf(stderr,
                "Usage: moq_bench_session_scale [options]\n"
                "  --subscribers N        number of subscribers (default 4)\n"
                "  --objects N            objects to publish (default 1000)\n"
                "  --object-size N        payload bytes per object (default 1200)\n"
                "  --objects-per-group N  objects per group (default 30)\n"
                "  --warmup N             warmup objects, excluded from timing (default 50)\n"
                "  --json                 output results as JSON\n");
            exit(0);
        } else {
            fprintf(stderr, "unknown flag: %s (try --help)\n", argv[i]);
            exit(1);
        }
    }

    if (cfg.object_size == 0 || cfg.object_size > SIZE_MAX) {
        fprintf(stderr, "error: --object-size must be 1..%zu\n", (size_t)SIZE_MAX);
        exit(1);
    }
    if (cfg.objects == 0) {
        fprintf(stderr, "error: --objects must be >= 1\n");
        exit(1);
    }
    if (cfg.subscribers == 0) {
        fprintf(stderr, "error: --subscribers must be >= 1\n");
        exit(1);
    }
    if (cfg.objects_per_group == 0) cfg.objects_per_group = 1;
    if (cfg.warmup > cfg.objects) cfg.warmup = 0;

    return cfg;
}

/* ------------------------------------------------------------------ */
/* Publish one object to all subscribers                               */
/* ------------------------------------------------------------------ */

static int publish_one(sub_slot_t *subs, uint64_t nsubs,
                        uint64_t group, uint64_t obj_in_group,
                        bool end_of_group, moq_rcbuf_t *payload)
{
    for (uint64_t s = 0; s < nsubs; s++) {
        moq_session_t *pub = moq_simpair_server(subs[s].sp);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = group;
        sgcfg.subgroup_id = 0;
        if (end_of_group) sgcfg.end_of_group = true;

        moq_subgroup_handle_t sg;
        moq_result_t rc = moq_session_open_subgroup(pub, subs[s].pub_sub,
            &sgcfg, moq_simpair_now_us(subs[s].sp), &sg);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            moq_simpair_run_until_quiescent(subs[s].sp, 4, NULL);
            rc = moq_session_open_subgroup(pub, subs[s].pub_sub,
                &sgcfg, moq_simpair_now_us(subs[s].sp), &sg);
        }
        if (rc != MOQ_OK) {
            fprintf(stderr, "open_subgroup failed: sub=%" PRIu64 " rc=%d\n",
                s, rc);
            return -1;
        }

        rc = moq_session_write_object(pub, sg, obj_in_group,
            payload, moq_simpair_now_us(subs[s].sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "write_object failed: sub=%" PRIu64 " rc=%d\n",
                s, rc);
            return -1;
        }

        rc = moq_session_close_subgroup(pub, sg,
            moq_simpair_now_us(subs[s].sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "close_subgroup failed: sub=%" PRIu64 " rc=%d\n",
                s, rc);
            return -1;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Pump all simpairs and drain subscriber events                       */
/* ------------------------------------------------------------------ */

static void pump_and_drain(sub_slot_t *subs, uint64_t nsubs)
{
    for (uint64_t s = 0; s < nsubs; s++) {
        moq_simpair_run_until_quiescent(subs[s].sp, 4, NULL);

        moq_session_t *client = moq_simpair_client(subs[s].sp);
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                subs[s].objects_received++;
                if (ev.u.object_received.payload)
                    subs[s].payload_bytes +=
                        moq_rcbuf_len(ev.u.object_received.payload);
            }
            moq_event_cleanup(&ev);
        }
    }
}

/* ------------------------------------------------------------------ */
/* Benchmark                                                           */
/* ------------------------------------------------------------------ */

static int run_bench(const fanout_cfg_t *cfg)
{
    bench_alloc_state_t alloc_state = {0};
    moq_alloc_t alloc = {
        &alloc_state, bench_alloc, bench_realloc, bench_free
    };

    uint64_t nsubs = cfg->subscribers;

    if (nsubs > SIZE_MAX / sizeof(sub_slot_t)) {
        fprintf(stderr, "error: --subscribers too large\n");
        return 1;
    }
    if (cfg->objects > UINT64_MAX / nsubs) {
        fprintf(stderr, "error: objects * subscribers overflows\n");
        return 1;
    }

    sub_slot_t *subs = (sub_slot_t *)calloc((size_t)nsubs, sizeof(sub_slot_t));
    if (!subs) {
        fprintf(stderr, "subscriber array alloc failed\n");
        return 1;
    }

    /* Create N simpairs and establish subscriptions. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bench") };
    int setup_ok = 1;

    for (uint64_t s = 0; s < nsubs; s++) {
        moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
        spcfg.alloc = &alloc;
        spcfg.seed = s;
        spcfg.initial_now_us = 1000;
        spcfg.server_send_request_capacity = true;
        spcfg.server_initial_request_capacity = 64;
        spcfg.client_send_request_capacity = true;
        spcfg.client_initial_request_capacity = 64;

        if (moq_simpair_create(&spcfg, &subs[s].sp) != MOQ_OK) {
            fprintf(stderr, "simpair_create failed: sub=%" PRIu64 "\n", s);
            setup_ok = 0; break;
        }
        moq_simpair_start(subs[s].sp);
        moq_simpair_run_until_quiescent(subs[s].sp, 8, NULL);

        /* Drain setup events. */
        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_server(subs[s].sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_client(subs[s].sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        /* Subscribe. */
        moq_subscribe_cfg_t subcfg;
        moq_subscribe_cfg_init(&subcfg);
        subcfg.track_namespace.parts = ns_parts;
        subcfg.track_namespace.count = 1;
        subcfg.track_name = MOQ_BYTES_LITERAL("data");
        moq_subscription_t sub_h;
        moq_result_t rc = moq_session_subscribe(
            moq_simpair_client(subs[s].sp), &subcfg,
            moq_simpair_now_us(subs[s].sp), &sub_h);
        if (rc != MOQ_OK) {
            fprintf(stderr, "subscribe failed: sub=%" PRIu64 " rc=%d\n", s, rc);
            setup_ok = 0; break;
        }
        moq_simpair_run_until_quiescent(subs[s].sp, 8, NULL);

        /* Accept. */
        if (moq_session_poll_events(moq_simpair_server(subs[s].sp), &ev, 1) != 1 ||
            ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
            fprintf(stderr, "expected SUBSCRIBE_REQUEST: sub=%" PRIu64 "\n", s);
            setup_ok = 0; break;
        }
        subs[s].pub_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        rc = moq_session_accept_subscribe(
            moq_simpair_server(subs[s].sp), subs[s].pub_sub, &acc,
            moq_simpair_now_us(subs[s].sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "accept_subscribe failed: sub=%" PRIu64 " rc=%d\n",
                s, rc);
            setup_ok = 0; break;
        }
        moq_simpair_run_until_quiescent(subs[s].sp, 8, NULL);

        while (moq_session_poll_events(moq_simpair_client(subs[s].sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    if (!setup_ok) {
        for (uint64_t s = 0; s < nsubs; s++)
            if (subs[s].sp) moq_simpair_destroy(subs[s].sp);
        free(subs);
        return 1;
    }

    /* Pre-allocate payload (zero-copy, shared across all writes). */
    uint8_t *payload_data = (uint8_t *)malloc(cfg->object_size);
    if (!payload_data) {
        fprintf(stderr, "payload malloc failed\n");
        for (uint64_t s = 0; s < nsubs; s++)
            moq_simpair_destroy(subs[s].sp);
        free(subs);
        return 1;
    }
    memset(payload_data, 0xAB, cfg->object_size);

    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_wrap(&alloc, payload_data, cfg->object_size,
                        NULL, NULL, &payload) != MOQ_OK) {
        fprintf(stderr, "rcbuf_wrap failed\n");
        free(payload_data);
        for (uint64_t s = 0; s < nsubs; s++)
            moq_simpair_destroy(subs[s].sp);
        free(subs);
        return 1;
    }

    int had_error = 0;

    /* Reset peak after setup. */
    alloc_state.peak_bytes = alloc_state.live_bytes;

    /* Warmup. */
    for (uint64_t i = 0; i < cfg->warmup && !had_error; i++) {
        uint64_t group = i / cfg->objects_per_group;
        uint64_t obj_in_group = i % cfg->objects_per_group;
        bool eog = (obj_in_group == cfg->objects_per_group - 1);
        if (publish_one(subs, nsubs, group, obj_in_group, eog, payload) < 0) {
            had_error = 1; break;
        }
        pump_and_drain(subs, nsubs);
    }

    /* Reset counters. */
    for (uint64_t s = 0; s < nsubs; s++) {
        subs[s].objects_received = 0;
        subs[s].payload_bytes = 0;
    }
    alloc_state.alloc_count = 0;
    alloc_state.free_count = 0;
    alloc_state.peak_bytes = alloc_state.live_bytes;
    int64_t baseline_live = alloc_state.live_bytes;

    /* Latency tracking. */
    uint64_t lat_min = UINT64_MAX;
    uint64_t lat_max = 0;
    uint64_t lat_sum = 0;
    uint64_t lat_count = 0;

    /* ---- Measured phase ---- */
    uint64_t t_start = now_us();

    for (uint64_t i = 0; i < cfg->objects && !had_error; i++) {
        uint64_t abs_i = cfg->warmup + i;
        uint64_t group = abs_i / cfg->objects_per_group;
        uint64_t obj_in_group = abs_i % cfg->objects_per_group;
        bool eog = (obj_in_group == cfg->objects_per_group - 1);

        uint64_t t_pub = now_us();

        if (publish_one(subs, nsubs, group, obj_in_group, eog, payload) < 0) {
            had_error = 1; break;
        }
        pump_and_drain(subs, nsubs);

        uint64_t t_recv = now_us();
        uint64_t lat = t_recv - t_pub;
        if (lat < lat_min) lat_min = lat;
        if (lat > lat_max) lat_max = lat;
        if (lat_sum > UINT64_MAX - lat) {
            fprintf(stderr, "error: latency sum overflow\n");
            had_error = 1; break;
        }
        lat_sum += lat;
        lat_count++;
    }

    uint64_t t_end = now_us();
    uint64_t elapsed_us = t_end - t_start;
    if (elapsed_us == 0) elapsed_us = 1;

    /* Aggregate results. */
    uint64_t total_received = 0;
    uint64_t total_payload_bytes = 0;
    for (uint64_t s = 0; s < nsubs; s++) {
        if (total_received > UINT64_MAX - subs[s].objects_received ||
            total_payload_bytes > UINT64_MAX - subs[s].payload_bytes) {
            fprintf(stderr, "error: aggregate counter overflow\n");
            had_error = 1; break;
        }
        total_received += subs[s].objects_received;
        total_payload_bytes += subs[s].payload_bytes;
    }

    uint64_t expected = cfg->objects * nsubs;
    uint64_t lost = expected > total_received ? expected - total_received : 0;

    double elapsed_ms = (double)elapsed_us / 1000.0;
    double elapsed_s  = (double)elapsed_us / 1000000.0;
    double obj_per_sec = (double)total_received / elapsed_s;
    double mbps = ((double)total_payload_bytes * 8.0) / (1048576.0 * elapsed_s);

    double lat_avg_us = lat_count > 0 ? (double)lat_sum / (double)lat_count : 0;
    if (lat_min == UINT64_MAX) lat_min = 0;

    int64_t measured_allocs = alloc_state.alloc_count;
    int64_t measured_frees  = alloc_state.free_count;
    int64_t peak_delta = alloc_state.peak_bytes - baseline_live;

    /* Cleanup. */
    moq_rcbuf_decref(payload);
    free(payload_data);
    for (uint64_t s = 0; s < nsubs; s++) {
        moq_event_t drain[16]; size_t ne;
        while ((ne = moq_session_poll_events(
                    moq_simpair_server(subs[s].sp), drain, 16)) > 0)
            for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
        while ((ne = moq_session_poll_events(
                    moq_simpair_client(subs[s].sp), drain, 16)) > 0)
            for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
        moq_simpair_destroy(subs[s].sp);
    }
    free(subs);

    /* Report. */
    if (cfg->json) {
        printf("{\n");
        printf("  \"subscribers\": %" PRIu64 ",\n", nsubs);
        printf("  \"objects_published\": %" PRIu64 ",\n", cfg->objects);
        printf("  \"objects_received\": %" PRIu64 ",\n", total_received);
        printf("  \"objects_lost\": %" PRIu64 ",\n", lost);
        printf("  \"object_size\": %" PRIu64 ",\n", cfg->object_size);
        printf("  \"objects_per_group\": %" PRIu64 ",\n", cfg->objects_per_group);
        printf("  \"warmup\": %" PRIu64 ",\n", cfg->warmup);
        printf("  \"total_payload_bytes\": %" PRIu64 ",\n", total_payload_bytes);
        printf("  \"elapsed_ms\": %.2f,\n", elapsed_ms);
        printf("  \"objects_per_sec\": %.1f,\n", obj_per_sec);
        printf("  \"mbps\": %.2f,\n", mbps);
        printf("  \"cycle_latency_us_min\": %" PRIu64 ",\n", lat_min);
        printf("  \"cycle_latency_us_avg\": %.1f,\n", lat_avg_us);
        printf("  \"cycle_latency_us_max\": %" PRIu64 ",\n", lat_max);
        printf("  \"allocs\": %" PRId64 ",\n", measured_allocs);
        printf("  \"frees\": %" PRId64 ",\n", measured_frees);
        printf("  \"peak_bytes_delta\": %" PRId64 "\n", peak_delta);
        printf("}\n");
    } else {
        printf("--- moq_bench_session_scale ---\n");
        printf("subscribers       : %" PRIu64 "\n", nsubs);
        printf("objects published : %" PRIu64 "\n", cfg->objects);
        printf("objects received  : %" PRIu64 " (expected %" PRIu64 ")\n",
            total_received, expected);
        printf("objects lost      : %" PRIu64 "\n", lost);
        printf("object size       : %" PRIu64 " B\n", cfg->object_size);
        printf("objects/group     : %" PRIu64 "\n", cfg->objects_per_group);
        printf("warmup            : %" PRIu64 "\n", cfg->warmup);
        printf("total payload     : %" PRIu64 " B\n", total_payload_bytes);
        printf("elapsed           : %.2f ms\n", elapsed_ms);
        printf("throughput        : %.1f obj/s\n", obj_per_sec);
        printf("throughput        : %.2f Mbps\n", mbps);
        printf("cycle latency     : min=%" PRIu64 " avg=%.1f max=%" PRIu64 " us\n",
            lat_min, lat_avg_us, lat_max);
        printf("allocs (measured) : %" PRId64 "\n", measured_allocs);
        printf("frees  (measured) : %" PRId64 "\n", measured_frees);
        printf("peak delta        : %" PRId64 " B\n", peak_delta);
    }

    if (total_received != expected) {
        fprintf(stderr, "FAIL: objects received %" PRIu64 " != expected %" PRIu64 "\n",
            total_received, expected);
        return 1;
    }
    uint64_t expected_bytes = (expected <= UINT64_MAX / cfg->object_size)
        ? expected * cfg->object_size : UINT64_MAX;
    if (total_payload_bytes != expected_bytes) {
        fprintf(stderr, "FAIL: payload bytes %" PRIu64 " != expected %" PRIu64 "\n",
            total_payload_bytes, expected_bytes);
        return 1;
    }
    if (had_error)
        return 1;

    return 0;
}

int main(int argc, char **argv)
{
    fanout_cfg_t cfg = parse_args(argc, argv);
    return run_bench(&cfg);
}
