/*
 * moq_bench_process_fanout — process-level fanout benchmark.
 *
 * Models a relay/process that receives one input object and forwards
 * it to N downstream clients.  Each downstream client is connected
 * via an independent SimPair (one server session per client, as in a
 * real relay).  The payload rcbuf is shared across all N downstream
 * writes — no per-subscriber copy.
 *
 * This measures the application/process-level cost of fan-out with
 * today's session APIs, before a dedicated relay library exists.
 * A future libmoq-relay benchmark will replace the manual forwarding
 * harness with the relay's own API.
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
/* Per-downstream-client state                                         */
/* ------------------------------------------------------------------ */

typedef struct {
    moq_simpair_t     *sp;
    moq_subscription_t pub_sub;
    uint64_t           objects_received;
    uint64_t           payload_bytes;
} downstream_t;

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
                "Usage: moq_bench_process_fanout [options]\n"
                "  --subscribers N        downstream clients (default 4)\n"
                "  --objects N            input objects (default 1000)\n"
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
/* Forward one input object to all downstream sessions                 */
/* ------------------------------------------------------------------ */

static int forward_one(downstream_t *ds, uint64_t nds,
                        uint64_t group, uint64_t obj_in_group,
                        bool end_of_group, moq_rcbuf_t *payload)
{
    for (uint64_t d = 0; d < nds; d++) {
        moq_session_t *srv = moq_simpair_server(ds[d].sp);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = group;
        sgcfg.subgroup_id = 0;
        if (end_of_group) sgcfg.end_of_group = true;

        moq_subgroup_handle_t sg;
        moq_result_t rc = moq_session_open_subgroup(srv, ds[d].pub_sub,
            &sgcfg, moq_simpair_now_us(ds[d].sp), &sg);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            moq_simpair_run_until_quiescent(ds[d].sp, 4, NULL);
            rc = moq_session_open_subgroup(srv, ds[d].pub_sub,
                &sgcfg, moq_simpair_now_us(ds[d].sp), &sg);
        }
        if (rc != MOQ_OK) {
            fprintf(stderr, "open_subgroup failed: ds=%" PRIu64 " rc=%d\n",
                d, rc);
            return -1;
        }

        rc = moq_session_write_object(srv, sg, obj_in_group,
            payload, moq_simpair_now_us(ds[d].sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "write_object failed: ds=%" PRIu64 " rc=%d\n",
                d, rc);
            return -1;
        }

        rc = moq_session_close_subgroup(srv, sg,
            moq_simpair_now_us(ds[d].sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "close_subgroup failed: ds=%" PRIu64 " rc=%d\n",
                d, rc);
            return -1;
        }
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* Pump all downstream simpairs and drain events                       */
/* ------------------------------------------------------------------ */

static void pump_and_drain(downstream_t *ds, uint64_t nds)
{
    for (uint64_t d = 0; d < nds; d++) {
        moq_simpair_run_until_quiescent(ds[d].sp, 4, NULL);

        moq_session_t *client = moq_simpair_client(ds[d].sp);
        moq_event_t ev;
        while (moq_session_poll_events(client, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                ds[d].objects_received++;
                if (ev.u.object_received.payload)
                    ds[d].payload_bytes +=
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

    uint64_t nds = cfg->subscribers;

    if (nds > SIZE_MAX / sizeof(downstream_t)) {
        fprintf(stderr, "error: --subscribers too large\n");
        return 1;
    }
    if (cfg->objects > UINT64_MAX / nds) {
        fprintf(stderr, "error: objects * subscribers overflows\n");
        return 1;
    }

    downstream_t *ds = (downstream_t *)calloc((size_t)nds, sizeof(downstream_t));
    if (!ds) {
        fprintf(stderr, "downstream array alloc failed\n");
        return 1;
    }

    /* Set up N downstream SimPairs with accepted subscriptions. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bench") };
    int setup_ok = 1;

    for (uint64_t d = 0; d < nds; d++) {
        moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
        spcfg.alloc = &alloc;
        spcfg.seed = d;
        spcfg.initial_now_us = 1000;
        spcfg.server_send_request_capacity = true;
        spcfg.server_initial_request_capacity = 64;
        spcfg.client_send_request_capacity = true;
        spcfg.client_initial_request_capacity = 64;

        if (moq_simpair_create(&spcfg, &ds[d].sp) != MOQ_OK) {
            fprintf(stderr, "simpair_create failed: ds=%" PRIu64 "\n", d);
            setup_ok = 0; break;
        }
        moq_simpair_start(ds[d].sp);
        moq_simpair_run_until_quiescent(ds[d].sp, 8, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(moq_simpair_server(ds[d].sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
        if (moq_session_poll_events(moq_simpair_client(ds[d].sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);

        moq_subscribe_cfg_t subcfg;
        moq_subscribe_cfg_init(&subcfg);
        subcfg.track_namespace.parts = ns_parts;
        subcfg.track_namespace.count = 1;
        subcfg.track_name = MOQ_BYTES_LITERAL("data");
        moq_subscription_t sub_h;
        moq_result_t rc = moq_session_subscribe(
            moq_simpair_client(ds[d].sp), &subcfg,
            moq_simpair_now_us(ds[d].sp), &sub_h);
        if (rc != MOQ_OK) {
            fprintf(stderr, "subscribe failed: ds=%" PRIu64 " rc=%d\n", d, rc);
            setup_ok = 0; break;
        }
        moq_simpair_run_until_quiescent(ds[d].sp, 8, NULL);

        if (moq_session_poll_events(moq_simpair_server(ds[d].sp), &ev, 1) != 1 ||
            ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
            fprintf(stderr, "expected SUBSCRIBE_REQUEST: ds=%" PRIu64 "\n", d);
            setup_ok = 0; break;
        }
        ds[d].pub_sub = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        rc = moq_session_accept_subscribe(
            moq_simpair_server(ds[d].sp), ds[d].pub_sub, &acc,
            moq_simpair_now_us(ds[d].sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "accept failed: ds=%" PRIu64 " rc=%d\n", d, rc);
            setup_ok = 0; break;
        }
        moq_simpair_run_until_quiescent(ds[d].sp, 8, NULL);

        while (moq_session_poll_events(moq_simpair_client(ds[d].sp), &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    if (!setup_ok) {
        for (uint64_t d = 0; d < nds; d++)
            if (ds[d].sp) moq_simpair_destroy(ds[d].sp);
        free(ds);
        return 1;
    }

    /* Single shared payload rcbuf — zero-copy across all downstream writes. */
    uint8_t *payload_data = (uint8_t *)malloc(cfg->object_size);
    if (!payload_data) {
        fprintf(stderr, "payload malloc failed\n");
        for (uint64_t d = 0; d < nds; d++)
            moq_simpair_destroy(ds[d].sp);
        free(ds);
        return 1;
    }
    memset(payload_data, 0xAB, cfg->object_size);

    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_wrap(&alloc, payload_data, cfg->object_size,
                        NULL, NULL, &payload) != MOQ_OK) {
        fprintf(stderr, "rcbuf_wrap failed\n");
        free(payload_data);
        for (uint64_t d = 0; d < nds; d++)
            moq_simpair_destroy(ds[d].sp);
        free(ds);
        return 1;
    }

    int had_error = 0;
    alloc_state.peak_bytes = alloc_state.live_bytes;

    /* Warmup. */
    for (uint64_t i = 0; i < cfg->warmup && !had_error; i++) {
        uint64_t group = i / cfg->objects_per_group;
        uint64_t obj_in_group = i % cfg->objects_per_group;
        bool eog = (obj_in_group == cfg->objects_per_group - 1);
        if (forward_one(ds, nds, group, obj_in_group, eog, payload) < 0) {
            had_error = 1; break;
        }
        pump_and_drain(ds, nds);
    }

    /* Reset counters. */
    for (uint64_t d = 0; d < nds; d++) {
        ds[d].objects_received = 0;
        ds[d].payload_bytes = 0;
    }
    alloc_state.alloc_count = 0;
    alloc_state.free_count = 0;
    alloc_state.peak_bytes = alloc_state.live_bytes;
    int64_t baseline_live = alloc_state.live_bytes;

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

        if (forward_one(ds, nds, group, obj_in_group, eog, payload) < 0) {
            had_error = 1; break;
        }
        pump_and_drain(ds, nds);

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

    /* Aggregate. */
    uint64_t total_received = 0;
    uint64_t total_payload_bytes = 0;
    for (uint64_t d = 0; d < nds; d++) {
        if (total_received > UINT64_MAX - ds[d].objects_received ||
            total_payload_bytes > UINT64_MAX - ds[d].payload_bytes) {
            fprintf(stderr, "error: aggregate counter overflow\n");
            had_error = 1; break;
        }
        total_received += ds[d].objects_received;
        total_payload_bytes += ds[d].payload_bytes;
    }

    uint64_t expected = cfg->objects * nds;
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
    for (uint64_t d = 0; d < nds; d++) {
        moq_event_t drain[16]; size_t ne;
        while ((ne = moq_session_poll_events(
                    moq_simpair_server(ds[d].sp), drain, 16)) > 0)
            for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
        while ((ne = moq_session_poll_events(
                    moq_simpair_client(ds[d].sp), drain, 16)) > 0)
            for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
        moq_simpair_destroy(ds[d].sp);
    }
    free(ds);

    /* Report. */
    if (cfg->json) {
        printf("{\n");
        printf("  \"subscribers\": %" PRIu64 ",\n", nds);
        printf("  \"objects_in\": %" PRIu64 ",\n", cfg->objects);
        printf("  \"downstream_objects_expected\": %" PRIu64 ",\n", expected);
        printf("  \"downstream_objects_received\": %" PRIu64 ",\n", total_received);
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
        printf("--- moq_bench_process_fanout ---\n");
        printf("subscribers       : %" PRIu64 "\n", nds);
        printf("objects in        : %" PRIu64 "\n", cfg->objects);
        printf("downstream recv   : %" PRIu64 " (expected %" PRIu64 ")\n",
            total_received, expected);
        printf("objects lost      : %" PRIu64 "\n", lost);
        printf("object size       : %" PRIu64 " B\n", cfg->object_size);
        printf("objects/group     : %" PRIu64 "\n", cfg->objects_per_group);
        printf("warmup            : %" PRIu64 "\n", cfg->warmup);
        printf("total payload     : %" PRIu64 " B\n", total_payload_bytes);
        printf("elapsed           : %.2f ms\n", elapsed_ms);
        printf("throughput        : %.1f downstream obj/s\n", obj_per_sec);
        printf("throughput        : %.2f Mbps\n", mbps);
        printf("cycle latency     : min=%" PRIu64 " avg=%.1f max=%" PRIu64 " us\n",
            lat_min, lat_avg_us, lat_max);
        printf("allocs (measured) : %" PRId64 "\n", measured_allocs);
        printf("frees  (measured) : %" PRId64 "\n", measured_frees);
        printf("peak delta        : %" PRId64 " B\n", peak_delta);
    }

    if (total_received != expected) {
        fprintf(stderr, "FAIL: downstream objects %" PRIu64 " != expected %" PRIu64 "\n",
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
