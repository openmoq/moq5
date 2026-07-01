/*
 * moq_bench_session_subgroup — steady-state subgroup receive allocation cost.
 *
 * One SimPair, one subscription, ONE subgroup opened once. N objects are
 * written into that single subgroup and closed once at the end, so the uni
 * stream is established once and reused -- the receiver's rx stream is set up
 * once, isolating the true per-object receive cost. This is deliberately
 * unlike bench_session_scale, which opens AND closes a subgroup per object (so
 * ~3 of its 5 allocs/object are per-stream setup/teardown, not per-object).
 *
 * The per-object allocs/frees reported here are the steady-state subgroup
 * receive cost. All are receive-side (send is zero-copy; SimPair no-fault
 * delivery is zero-alloc borrowed on_data_bytes). Currently one per object:
 * the delivered payload rcbuf itself. The object is assembled directly into
 * that rcbuf's inline storage (moq_rcbuf_alloc_uninit), so there is no
 * separate assembly buffer and no copy at emit; input staging (rx->input_buf)
 * is retained and reused across drains. Reaching zero would require pooling
 * the app-visible rcbuf, which is deferred pending a cross-thread ownership
 * model (the service tier frees payloads on a different thread).
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
    int64_t alloc_count;
    int64_t free_count;
    int64_t live_bytes;
    int64_t peak_bytes;
} bench_alloc_state_t;

static void *bench_alloc(size_t size, void *ctx)
{
    bench_alloc_state_t *s = (bench_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) {
        s->alloc_count++;
        s->live_bytes += (int64_t)size;
        if (s->live_bytes > s->peak_bytes) s->peak_bytes = s->live_bytes;
    }
    return p;
}

static void *bench_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx)
{
    bench_alloc_state_t *s = (bench_alloc_state_t *)ctx;
    void *p = realloc(ptr, new_sz);
    if (p) {
        s->live_bytes += (int64_t)new_sz - (int64_t)old_sz;
        if (s->live_bytes > s->peak_bytes) s->peak_bytes = s->live_bytes;
    }
    return p;
}

static void bench_free(void *ptr, size_t size, void *ctx)
{
    bench_alloc_state_t *s = (bench_alloc_state_t *)ctx;
    if (ptr) { s->free_count++; s->live_bytes -= (int64_t)size; }
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
/* CLI                                                                 */
/* ------------------------------------------------------------------ */

typedef struct {
    uint64_t objects;
    uint64_t object_size;
    uint64_t warmup;
    bool     json;
} cfg_t;

static cfg_t parse_args(int argc, char **argv)
{
    cfg_t cfg = { .objects = 5000, .object_size = 1200, .warmup = 100,
                  .json = false };
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--objects") == 0 && i + 1 < argc)
            cfg.objects = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--object-size") == 0 && i + 1 < argc)
            cfg.object_size = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc)
            cfg.warmup = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--json") == 0)
            cfg.json = true;
        else if (strcmp(argv[i], "--help") == 0) {
            fprintf(stderr,
                "Usage: moq_bench_session_subgroup [options]\n"
                "  --objects N       objects into one subgroup (default 5000)\n"
                "  --object-size N   payload bytes per object (default 1200)\n"
                "  --warmup N        warmup objects, excluded (default 100)\n"
                "  --json            output results as JSON\n");
            exit(0);
        } else {
            fprintf(stderr, "unknown flag: %s (try --help)\n", argv[i]);
            exit(2);
        }
    }
    if (cfg.objects == 0) { fprintf(stderr, "error: --objects >= 1\n"); exit(2); }
    if (cfg.object_size == 0) {
        fprintf(stderr, "error: --object-size >= 1\n"); exit(2);
    }
    return cfg;
}

/* Drive one object write + delivery, counting received objects. */
static int write_one(moq_simpair_t *sp, moq_subgroup_handle_t sg,
                     uint64_t object_id, moq_rcbuf_t *payload,
                     uint64_t *received, uint64_t *payload_bytes)
{
    moq_session_t *srv = moq_simpair_server(sp);
    moq_session_t *cli = moq_simpair_client(sp);

    moq_result_t rc = moq_session_write_object(srv, sg, object_id, payload,
                                               moq_simpair_now_us(sp));
    if (rc == MOQ_ERR_WOULD_BLOCK) {
        moq_simpair_run_until_quiescent(sp, 4, NULL);
        rc = moq_session_write_object(srv, sg, object_id, payload,
                                      moq_simpair_now_us(sp));
    }
    if (rc != MOQ_OK) {
        fprintf(stderr, "write_object rc=%d (obj=%" PRIu64 ")\n", rc, object_id);
        return -1;
    }
    moq_simpair_run_until_quiescent(sp, 4, NULL);

    moq_event_t ev[16];
    size_t n;
    while ((n = moq_session_poll_events(cli, ev, 16)) > 0) {
        for (size_t j = 0; j < n; j++) {
            if (ev[j].kind == MOQ_EVENT_OBJECT_RECEIVED) {
                (*received)++;
                if (ev[j].u.object_received.payload)
                    *payload_bytes +=
                        moq_rcbuf_len(ev[j].u.object_received.payload);
            }
            moq_event_cleanup(&ev[j]);
        }
    }
    return 0;
}

static int run_bench(const cfg_t *cfg)
{
    bench_alloc_state_t alloc_state = {0};
    moq_alloc_t alloc = { &alloc_state, bench_alloc, bench_realloc, bench_free };

    moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
    spcfg.alloc = &alloc;
    spcfg.seed = 1;
    spcfg.initial_now_us = 1000;
    spcfg.server_send_request_capacity = true;
    spcfg.server_initial_request_capacity = 64;
    spcfg.client_send_request_capacity = true;
    spcfg.client_initial_request_capacity = 64;

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&spcfg, &sp) != MOQ_OK) {
        fprintf(stderr, "simpair_create failed\n"); return 1;
    }
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *srv = moq_simpair_server(sp);
    moq_session_t *cli = moq_simpair_client(sp);
    {
        moq_event_t ev;
        while (moq_session_poll_events(srv, &ev, 1) == 1) moq_event_cleanup(&ev);
        while (moq_session_poll_events(cli, &ev, 1) == 1) moq_event_cleanup(&ev);
    }

    /* One subscription. */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bench") };
    moq_subscribe_cfg_t subcfg;
    moq_subscribe_cfg_init(&subcfg);
    subcfg.track_namespace.parts = ns_parts;
    subcfg.track_namespace.count = 1;
    subcfg.track_name = MOQ_BYTES_LITERAL("data");
    moq_subscription_t sub_h;
    if (moq_session_subscribe(cli, &subcfg, moq_simpair_now_us(sp), &sub_h)
            != MOQ_OK) {
        fprintf(stderr, "subscribe failed\n"); moq_simpair_destroy(sp); return 1;
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    moq_event_t ev;
    if (moq_session_poll_events(srv, &ev, 1) != 1 ||
        ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
        fprintf(stderr, "expected SUBSCRIBE_REQUEST\n");
        moq_simpair_destroy(sp); return 1;
    }
    moq_subscription_t pub_sub = ev.u.subscribe_request.sub;
    moq_accept_subscribe_cfg_t acc;
    moq_accept_subscribe_cfg_init(&acc);
    if (moq_session_accept_subscribe(srv, pub_sub, &acc, moq_simpair_now_us(sp))
            != MOQ_OK) {
        fprintf(stderr, "accept_subscribe failed\n");
        moq_simpair_destroy(sp); return 1;
    }
    moq_simpair_run_until_quiescent(sp, 8, NULL);
    while (moq_session_poll_events(cli, &ev, 1) == 1) moq_event_cleanup(&ev);

    /* Shared zero-copy payload. */
    uint8_t *payload_data = (uint8_t *)malloc((size_t)cfg->object_size);
    if (!payload_data) { moq_simpair_destroy(sp); return 1; }
    memset(payload_data, 0xAB, (size_t)cfg->object_size);
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_wrap(&alloc, payload_data, (size_t)cfg->object_size,
                        NULL, NULL, &payload) != MOQ_OK) {
        free(payload_data); moq_simpair_destroy(sp); return 1;
    }

    /* Open ONE subgroup and keep it open across all objects. */
    moq_subgroup_cfg_t sgcfg;
    moq_subgroup_cfg_init(&sgcfg);
    sgcfg.group_id = 0;
    sgcfg.subgroup_id = 0;
    moq_subgroup_handle_t sg;
    moq_result_t orc = moq_session_open_subgroup(srv, pub_sub, &sgcfg,
                                                 moq_simpair_now_us(sp), &sg);
    if (orc == MOQ_ERR_WOULD_BLOCK) {
        moq_simpair_run_until_quiescent(sp, 4, NULL);
        orc = moq_session_open_subgroup(srv, pub_sub, &sgcfg,
                                        moq_simpair_now_us(sp), &sg);
    }
    if (orc != MOQ_OK) {
        fprintf(stderr, "open_subgroup rc=%d\n", orc);
        moq_rcbuf_decref(payload); free(payload_data);
        moq_simpair_destroy(sp); return 1;
    }

    uint64_t received = 0, payload_bytes = 0;
    int had_error = 0;
    uint64_t object_id = 0;

    for (uint64_t i = 0; i < cfg->warmup && !had_error; i++, object_id++)
        if (write_one(sp, sg, object_id, payload, &received, &payload_bytes) < 0)
            had_error = 1;

    received = 0; payload_bytes = 0;
    alloc_state.alloc_count = 0;
    alloc_state.free_count = 0;
    int64_t baseline_live = alloc_state.live_bytes;
    alloc_state.peak_bytes = alloc_state.live_bytes;

    uint64_t lat_min = UINT64_MAX, lat_max = 0, lat_sum = 0, lat_count = 0;
    uint64_t t_start = now_us();
    for (uint64_t i = 0; i < cfg->objects && !had_error; i++, object_id++) {
        uint64_t t_pub = now_us();
        if (write_one(sp, sg, object_id, payload, &received, &payload_bytes) < 0) {
            had_error = 1; break;
        }
        uint64_t lat = now_us() - t_pub;
        if (lat < lat_min) lat_min = lat;
        if (lat > lat_max) lat_max = lat;
        lat_sum += lat;
        lat_count++;
    }
    uint64_t elapsed_us = now_us() - t_start;
    if (elapsed_us == 0) elapsed_us = 1;

    (void)moq_session_close_subgroup(srv, sg, moq_simpair_now_us(sp));
    moq_simpair_run_until_quiescent(sp, 4, NULL);
    while (moq_session_poll_events(cli, &ev, 1) == 1) moq_event_cleanup(&ev);

    uint64_t lost = cfg->objects > received ? cfg->objects - received : 0;
    double elapsed_ms = (double)elapsed_us / 1000.0;
    double elapsed_s  = (double)elapsed_us / 1000000.0;
    double obj_per_sec = (double)received / elapsed_s;
    double lat_avg_us = lat_count > 0 ? (double)lat_sum / (double)lat_count : 0;
    if (lat_min == UINT64_MAX) lat_min = 0;
    int64_t measured_allocs = alloc_state.alloc_count;
    int64_t measured_frees  = alloc_state.free_count;
    int64_t peak_delta = alloc_state.peak_bytes - baseline_live;

    moq_rcbuf_decref(payload);
    free(payload_data);
    moq_simpair_destroy(sp);

    if (cfg->json) {
        printf("{\n");
        printf("  \"benchmark\": \"session_subgroup\",\n");
        printf("  \"objects\": %" PRIu64 ",\n", cfg->objects);
        printf("  \"objects_received\": %" PRIu64 ",\n", received);
        printf("  \"objects_lost\": %" PRIu64 ",\n", lost);
        printf("  \"object_size\": %" PRIu64 ",\n", cfg->object_size);
        printf("  \"objects_per_group\": %" PRIu64 ",\n", cfg->objects + cfg->warmup);
        printf("  \"warmup\": %" PRIu64 ",\n", cfg->warmup);
        printf("  \"total_payload_bytes\": %" PRIu64 ",\n", payload_bytes);
        printf("  \"elapsed_ms\": %.2f,\n", elapsed_ms);
        printf("  \"objects_per_sec\": %.1f,\n", obj_per_sec);
        printf("  \"cycle_latency_us_min\": %" PRIu64 ",\n", lat_min);
        printf("  \"cycle_latency_us_avg\": %.1f,\n", lat_avg_us);
        printf("  \"cycle_latency_us_max\": %" PRIu64 ",\n", lat_max);
        printf("  \"allocs\": %" PRId64 ",\n", measured_allocs);
        printf("  \"frees\": %" PRId64 ",\n", measured_frees);
        printf("  \"peak_bytes_delta\": %" PRId64 "\n", peak_delta);
        printf("}\n");
    } else {
        printf("--- moq_bench_session_subgroup (one subgroup, N objects) ---\n");
        printf("objects           : %" PRIu64 " (received %" PRIu64
               ", lost %" PRIu64 ")\n", cfg->objects, received, lost);
        printf("object size       : %" PRIu64 " B\n", cfg->object_size);
        printf("elapsed           : %.2f ms\n", elapsed_ms);
        printf("throughput        : %.1f obj/s\n", obj_per_sec);
        printf("cycle latency us  : min %" PRIu64 " avg %.1f max %" PRIu64 "\n",
               lat_min, lat_avg_us, lat_max);
        printf("allocs/frees      : %" PRId64 " / %" PRId64
               " (%.2f allocs/object)\n", measured_allocs, measured_frees,
               received ? (double)measured_allocs / (double)received : 0.0);
        printf("peak bytes delta  : %" PRId64 "\n", peak_delta);
    }

    if (had_error) return 1;
    if (received != cfg->objects) {
        fprintf(stderr, "error: received %" PRIu64 " != objects %" PRIu64 "\n",
            received, cfg->objects);
        return 1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    cfg_t cfg = parse_args(argc, argv);
    return run_bench(&cfg);
}
