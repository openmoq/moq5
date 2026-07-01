/*
 * moq_bench_core — sans-I/O core throughput benchmark.
 *
 * One publisher session and one subscriber session connected in-process
 * via explicit action pumping.  Publishes N objects of configurable size
 * on a single track with configurable group layout, measures wall-clock
 * time, and reports throughput.
 *
 * No sockets, no threads, no picoquic — pure session-layer performance.
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
/* CLI parsing                                                         */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t objects;
    uint64_t object_size;
    uint64_t objects_per_group;
    uint64_t warmup;
    bool     json;
} bench_cfg_t;

static bench_cfg_t parse_args(int argc, char **argv)
{
    bench_cfg_t cfg = {
        .objects          = 10000,
        .object_size      = 1200,
        .objects_per_group = 30,
        .warmup           = 100,
        .json             = false,
    };

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--objects") == 0 && i + 1 < argc)
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
                "Usage: moq_bench_core [options]\n"
                "  --objects N            total objects to publish (default 10000)\n"
                "  --object-size N        payload bytes per object (default 1200)\n"
                "  --objects-per-group N  objects per group (default 30)\n"
                "  --warmup N             warmup objects, excluded from timing (default 100)\n"
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
    if (cfg.objects_per_group == 0) cfg.objects_per_group = 1;
    if (cfg.warmup > cfg.objects) cfg.warmup = 0;

    return cfg;
}

/* ------------------------------------------------------------------ */
/* Benchmark core                                                      */
/* ------------------------------------------------------------------ */

static int run_bench(const bench_cfg_t *cfg)
{
    bench_alloc_state_t alloc_state = {0};
    moq_alloc_t alloc = {
        &alloc_state, bench_alloc, bench_realloc, bench_free
    };

    /* Create paired sessions via simpair. */
    moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
    spcfg.alloc = &alloc;
    spcfg.seed = 0;
    spcfg.initial_now_us = 1000;
    spcfg.server_send_request_capacity = true;
    spcfg.server_initial_request_capacity = 64;
    spcfg.client_send_request_capacity = true;
    spcfg.client_initial_request_capacity = 64;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&spcfg, &sp) != MOQ_OK) {
        fprintf(stderr, "simpair_create failed\n");
        return 1;
    }
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *pub_session = moq_simpair_server(sp);
    moq_session_t *sub_session = moq_simpair_client(sp);

    /* Drain setup events. */
    { moq_event_t ev;
      if (moq_session_poll_events(pub_session, &ev, 1) == 1)
          moq_event_cleanup(&ev);
      if (moq_session_poll_events(sub_session, &ev, 1) == 1)
          moq_event_cleanup(&ev);
    }

    /* Subscribe from client. */
    moq_subscribe_cfg_t subcfg;
    moq_subscribe_cfg_init(&subcfg);
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bench") };
    subcfg.track_namespace.parts = ns_parts;
    subcfg.track_namespace.count = 1;
    subcfg.track_name = MOQ_BYTES_LITERAL("data");
    moq_subscription_t sub_h;
    moq_session_subscribe(sub_session, &subcfg,
        moq_simpair_now_us(sp), &sub_h);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Accept subscription on server. */
    moq_event_t ev;
    if (moq_session_poll_events(pub_session, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        fprintf(stderr, "expected SUBSCRIBE_REQUEST\n");
        moq_simpair_destroy(sp);
        return 1;
    }
    moq_subscription_t pub_sub = ev.u.subscribe_request.sub;
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    moq_session_accept_subscribe(pub_session, pub_sub, &acc,
        moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    /* Drain subscribe_ok from client. */
    while (moq_session_poll_events(sub_session, &ev, 1) == 1)
        moq_event_cleanup(&ev);

    /* Pre-allocate payload buffer (reused for every object, zero-copy). */
    uint8_t *payload_data = (uint8_t *)malloc(cfg->object_size);
    if (!payload_data) {
        fprintf(stderr, "payload malloc failed\n");
        moq_simpair_destroy(sp);
        return 1;
    }
    memset(payload_data, 0xAB, cfg->object_size);

    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_wrap(&alloc, payload_data, cfg->object_size,
                        NULL, NULL, &payload) != MOQ_OK) {
        fprintf(stderr, "rcbuf_wrap failed\n");
        free(payload_data);
        moq_simpair_destroy(sp);
        return 1;
    }

    uint64_t objects_received = 0;
    uint64_t total_payload_bytes = 0;
    int      had_error = 0;

    /* Reset peak after setup. */
    alloc_state.peak_bytes = alloc_state.live_bytes;

    /* Warmup phase. */
    for (uint64_t i = 0; i < cfg->warmup; i++) {
        uint64_t group = i / cfg->objects_per_group;
        uint64_t obj_in_group = i % cfg->objects_per_group;
        bool end_of_group = (obj_in_group == cfg->objects_per_group - 1);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = group;
        sgcfg.subgroup_id = 0;
        if (end_of_group)
            sgcfg.end_of_group = true;

        moq_subgroup_handle_t sg;
        moq_result_t rc = moq_session_open_subgroup(pub_session, pub_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            rc = moq_session_open_subgroup(pub_session, pub_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
        }
        if (rc != MOQ_OK) {
            fprintf(stderr, "warmup open_subgroup failed: %d (obj %" PRIu64 ")\n",
                rc, i);
            had_error = 1; break;
        }

        rc = moq_session_write_object(pub_session, sg, obj_in_group,
            payload, moq_simpair_now_us(sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "warmup write_object failed: %d\n", rc);
            had_error = 1; break;
        }
        rc = moq_session_close_subgroup(pub_session, sg,
            moq_simpair_now_us(sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "warmup close_subgroup failed: %d\n", rc);
            had_error = 1; break;
        }

        moq_simpair_run_until_quiescent(sp, 4, NULL);

        while (moq_session_poll_events(sub_session, &ev, 1) == 1)
            moq_event_cleanup(&ev);
    }

    /* Reset counters after warmup. */
    alloc_state.alloc_count = 0;
    alloc_state.free_count = 0;
    alloc_state.peak_bytes = alloc_state.live_bytes;
    int64_t baseline_live = alloc_state.live_bytes;

    /* ---- Measured phase ---- */
    uint64_t t_start = now_us();

    for (uint64_t i = 0; i < cfg->objects; i++) {
        uint64_t abs_i = cfg->warmup + i;
        uint64_t group = abs_i / cfg->objects_per_group;
        uint64_t obj_in_group = abs_i % cfg->objects_per_group;
        bool end_of_group = (obj_in_group == cfg->objects_per_group - 1);

        moq_subgroup_cfg_t sgcfg;
        moq_subgroup_cfg_init(&sgcfg);
        sgcfg.group_id = group;
        sgcfg.subgroup_id = 0;
        if (end_of_group)
            sgcfg.end_of_group = true;

        moq_subgroup_handle_t sg;
        moq_result_t rc = moq_session_open_subgroup(pub_session, pub_sub,
            &sgcfg, moq_simpair_now_us(sp), &sg);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            moq_simpair_run_until_quiescent(sp, 4, NULL);
            rc = moq_session_open_subgroup(pub_session, pub_sub,
                &sgcfg, moq_simpair_now_us(sp), &sg);
        }
        if (rc != MOQ_OK) {
            fprintf(stderr, "open_subgroup failed: %d (obj %" PRIu64 ")\n",
                rc, i);
            had_error = 1; break;
        }

        rc = moq_session_write_object(pub_session, sg, obj_in_group,
            payload, moq_simpair_now_us(sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "write_object failed: %d\n", rc);
            had_error = 1; break;
        }
        rc = moq_session_close_subgroup(pub_session, sg,
            moq_simpair_now_us(sp));
        if (rc != MOQ_OK) {
            fprintf(stderr, "close_subgroup failed: %d\n", rc);
            had_error = 1; break;
        }

        moq_simpair_run_until_quiescent(sp, 4, NULL);

        while (moq_session_poll_events(sub_session, &ev, 1) == 1) {
            if (ev.kind == MOQ_EVENT_OBJECT_RECEIVED) {
                objects_received++;
                if (ev.u.object_received.payload)
                    total_payload_bytes +=
                        moq_rcbuf_len(ev.u.object_received.payload);
            }
            moq_event_cleanup(&ev);
        }
    }

    uint64_t t_end = now_us();
    uint64_t elapsed_us = t_end - t_start;
    if (elapsed_us == 0) elapsed_us = 1;

    double elapsed_ms = (double)elapsed_us / 1000.0;
    double elapsed_s  = (double)elapsed_us / 1000000.0;
    double obj_per_sec = (double)objects_received / elapsed_s;
    double mbps = ((double)total_payload_bytes * 8.0) / (1048576.0 * elapsed_s);

    int64_t measured_allocs = alloc_state.alloc_count;
    int64_t measured_frees  = alloc_state.free_count;
    int64_t peak_delta = alloc_state.peak_bytes - baseline_live;

    /* Cleanup. */
    if (payload) moq_rcbuf_decref(payload);
    free(payload_data);

    { moq_event_t drain[16]; size_t ne;
      while ((ne = moq_session_poll_events(pub_session, drain, 16)) > 0)
          for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
      while ((ne = moq_session_poll_events(sub_session, drain, 16)) > 0)
          for (size_t j = 0; j < ne; j++) moq_event_cleanup(&drain[j]);
    }
    moq_simpair_destroy(sp);

    /* Report. */
    if (cfg->json) {
        printf("{\n");
        printf("  \"objects_published\": %" PRIu64 ",\n", cfg->objects);
        printf("  \"objects_received\": %" PRIu64 ",\n", objects_received);
        printf("  \"object_size\": %" PRIu64 ",\n", cfg->object_size);
        printf("  \"objects_per_group\": %" PRIu64 ",\n", cfg->objects_per_group);
        printf("  \"warmup\": %" PRIu64 ",\n", cfg->warmup);
        printf("  \"total_payload_bytes\": %" PRIu64 ",\n", total_payload_bytes);
        printf("  \"elapsed_ms\": %.2f,\n", elapsed_ms);
        printf("  \"objects_per_sec\": %.1f,\n", obj_per_sec);
        printf("  \"mbps\": %.2f,\n", mbps);
        printf("  \"allocs\": %" PRId64 ",\n", measured_allocs);
        printf("  \"frees\": %" PRId64 ",\n", measured_frees);
        printf("  \"peak_bytes_delta\": %" PRId64 "\n", peak_delta);
        printf("}\n");
    } else {
        printf("--- moq_bench_core ---\n");
        printf("objects published : %" PRIu64 "\n", cfg->objects);
        printf("objects received  : %" PRIu64 "\n", objects_received);
        printf("object size       : %" PRIu64 " B\n", cfg->object_size);
        printf("objects/group     : %" PRIu64 "\n", cfg->objects_per_group);
        printf("warmup            : %" PRIu64 "\n", cfg->warmup);
        printf("total payload     : %" PRIu64 " B\n", total_payload_bytes);
        printf("elapsed           : %.2f ms\n", elapsed_ms);
        printf("throughput        : %.1f obj/s\n", obj_per_sec);
        printf("throughput        : %.2f Mbps\n", mbps);
        printf("allocs (measured) : %" PRId64 "\n", measured_allocs);
        printf("frees  (measured) : %" PRId64 "\n", measured_frees);
        printf("peak delta        : %" PRId64 " B\n", peak_delta);
    }

    if (objects_received != cfg->objects) {
        fprintf(stderr, "FAIL: objects received %" PRIu64 " != expected %" PRIu64 "\n",
            objects_received, cfg->objects);
        return 1;
    }
    uint64_t expected_bytes = (cfg->objects <= UINT64_MAX / cfg->object_size)
        ? cfg->objects * cfg->object_size : UINT64_MAX;
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
    bench_cfg_t cfg = parse_args(argc, argv);
    return run_bench(&cfg);
}
