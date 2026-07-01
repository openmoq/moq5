/*
 * moq_bench_session_lookup — single-session subscription-alias lookup scaling.
 *
 * Establishes M subscriptions (M distinct track aliases) on ONE client/server
 * SimPair, then delivers objects as datagrams round-robin across those aliases.
 * Every received datagram forces the subscriber's alias lookup exactly once:
 * sub_find_by_alias_subscriber (core/src/session/session_subscribe.c) is a
 * linear scan of s->subs, hit on the datagram receive path. Per-object cost
 * therefore scales with the subscription count -- the O(n) receive-side scan
 * a Phase 3 alias index would replace. Vary --subscriptions to get the curve.
 *
 * NOTE: this benchmark deliberately does NOT cover subgroup stream-ref lookup.
 * sg_find_by_stream_ref is a scan, but its only caller is
 * moq_session_on_data_stop (the peer STOP/reset handler) -- it is not on the
 * per-object path. The object write path resolves subgroups by O(1) packed
 * handle, and the per-chunk receive path uses an O(1) stream-ref index
 * (rx_find_by_ref -> moq_index_find), so there is no per-object stream-ref
 * scan to measure honestly.
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
    uint64_t subscriptions;
    uint64_t objects;
    uint64_t object_size;
    uint64_t objects_per_group;
    uint64_t warmup;
    bool     json;
} cfg_t;

static cfg_t parse_args(int argc, char **argv)
{
    cfg_t cfg = {
        .subscriptions    = 64,
        .objects          = 1000,
        .object_size      = 1200,
        .objects_per_group = 30,
        .warmup           = 50,
        .json             = false,
    };
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--subscriptions") == 0 && i + 1 < argc)
            cfg.subscriptions = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--objects") == 0 && i + 1 < argc)
            cfg.objects = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--object-size") == 0 && i + 1 < argc)
            cfg.object_size = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--objects-per-group") == 0 && i + 1 < argc)
            cfg.objects_per_group = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc)
            cfg.warmup = (uint64_t)strtoull(argv[++i], NULL, 10);
        else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            /* Only alias mode exists; accept it explicitly, reject others so a
             * mistaken --mode subgroup fails loudly rather than silently. */
            const char *m = argv[++i];
            if (strcmp(m, "alias") != 0) {
                fprintf(stderr, "error: only --mode alias is supported "
                    "(subgroup stream-ref lookup is not a per-object path)\n");
                exit(2);
            }
        } else if (strcmp(argv[i], "--json") == 0)
            cfg.json = true;
        else if (strcmp(argv[i], "--help") == 0) {
            fprintf(stderr,
                "Usage: moq_bench_session_lookup [options]\n"
                "  --mode alias           lookup path to stress (only: alias)\n"
                "  --subscriptions N      subscriptions on one session (default 64)\n"
                "  --objects N            objects delivered (default 1000)\n"
                "  --object-size N        payload bytes per object (default 1200)\n"
                "  --objects-per-group N  objects per group (default 30)\n"
                "  --warmup N             warmup objects, excluded (default 50)\n"
                "  --json                 output results as JSON\n");
            exit(0);
        } else {
            fprintf(stderr, "unknown flag: %s (try --help)\n", argv[i]);
            exit(2);
        }
    }
    if (cfg.subscriptions == 0) {
        fprintf(stderr, "error: --subscriptions must be >= 1\n");
        exit(2);
    }
    if (cfg.objects == 0) {
        fprintf(stderr, "error: --objects must be >= 1\n");
        exit(2);
    }
    if (cfg.object_size == 0) {
        fprintf(stderr, "error: --object-size must be >= 1\n");
        exit(2);
    }
    if (cfg.objects_per_group == 0) {
        fprintf(stderr, "error: --objects-per-group must be >= 1\n");
        exit(2);
    }
    return cfg;
}

/* ------------------------------------------------------------------ */
/* Drain client events, counting received datagram objects             */
/* ------------------------------------------------------------------ */

static void drain_events(moq_session_t *sess, uint64_t *received,
                          uint64_t *payload_bytes)
{
    moq_event_t ev[16];
    size_t n;
    while ((n = moq_session_poll_events(sess, ev, 16)) > 0) {
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
}

/* ------------------------------------------------------------------ */
/* Benchmark                                                           */
/* ------------------------------------------------------------------ */

static int run_bench(const cfg_t *cfg)
{
    bench_alloc_state_t alloc_state = {0};
    moq_alloc_t alloc = { &alloc_state, bench_alloc, bench_realloc, bench_free };

    uint64_t nsub = cfg->subscriptions;
    if (nsub > SIZE_MAX / sizeof(moq_subscription_t) ||
        nsub + 16 > UINT32_MAX) {
        fprintf(stderr, "error: --subscriptions too large\n");
        return 1;
    }

    /* One session pair. Grant enough request capacity to establish every
     * subscription (each subscribe consumes one request id). */
    moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
    spcfg.alloc = &alloc;
    spcfg.seed = 1;
    spcfg.initial_now_us = 1000;
    spcfg.server_send_request_capacity = true;
    spcfg.server_initial_request_capacity = (nsub + 16) * 4;
    spcfg.client_send_request_capacity = true;
    spcfg.client_initial_request_capacity = (nsub + 16) * 4;
    /* The client holds the M subscriber-role subscriptions (the alias scan);
     * the server holds the M publisher-role entries from accept. Both pools
     * must exceed M. */
    spcfg.client_max_subscriptions = (uint32_t)(nsub + 16);
    spcfg.server_max_subscriptions = (uint32_t)(nsub + 16);

    moq_simpair_t *sp = NULL;
    if (moq_simpair_create(&spcfg, &sp) != MOQ_OK) {
        fprintf(stderr, "simpair_create failed\n");
        return 1;
    }
    moq_simpair_start(sp);
    moq_simpair_run_until_quiescent(sp, 8, NULL);

    moq_session_t *srv = moq_simpair_server(sp);
    moq_session_t *cli = moq_simpair_client(sp);

    /* Drain setup events. */
    {
        moq_event_t ev;
        while (moq_session_poll_events(srv, &ev, 1) == 1) moq_event_cleanup(&ev);
        while (moq_session_poll_events(cli, &ev, 1) == 1) moq_event_cleanup(&ev);
    }

    moq_subscription_t *pub_sub =
        (moq_subscription_t *)calloc((size_t)nsub, sizeof(moq_subscription_t));
    if (!pub_sub) {
        fprintf(stderr, "pub_sub array alloc failed\n");
        moq_simpair_destroy(sp);
        return 1;
    }

    /* Establish nsub subscriptions on the single session, each a distinct
     * track name -> distinct server-assigned track alias. Complete each
     * subscribe/accept before the next so only one request is ever in flight
     * (request-id budget is cumulative, capacity above covers the total). */
    moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("bench") };
    int setup_ok = 1;
    for (uint64_t i = 0; i < nsub && setup_ok; i++) {
        char name[24];
        int nlen = snprintf(name, sizeof(name), "t%" PRIu64, i);

        moq_subscribe_cfg_t subcfg;
        moq_subscribe_cfg_init(&subcfg);
        subcfg.track_namespace.parts = ns_parts;
        subcfg.track_namespace.count = 1;
        subcfg.track_name = (moq_bytes_t){ (const uint8_t *)name, (size_t)nlen };
        moq_subscription_t sub_h;
        if (moq_session_subscribe(cli, &subcfg, moq_simpair_now_us(sp), &sub_h)
                != MOQ_OK) {
            fprintf(stderr, "subscribe failed at i=%" PRIu64 "\n", i);
            setup_ok = 0; break;
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        moq_event_t ev;
        if (moq_session_poll_events(srv, &ev, 1) != 1 ||
            ev.kind != MOQ_EVENT_SUBSCRIBE_REQUEST) {
            fprintf(stderr, "expected SUBSCRIBE_REQUEST at i=%" PRIu64 "\n", i);
            setup_ok = 0; break;
        }
        pub_sub[i] = ev.u.subscribe_request.sub;
        moq_accept_subscribe_cfg_t acc;
        moq_accept_subscribe_cfg_init(&acc);
        if (moq_session_accept_subscribe(srv, pub_sub[i], &acc,
                moq_simpair_now_us(sp)) != MOQ_OK) {
            fprintf(stderr, "accept_subscribe failed at i=%" PRIu64 "\n", i);
            setup_ok = 0; break;
        }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        while (moq_session_poll_events(cli, &ev, 1) == 1) moq_event_cleanup(&ev);
    }
    if (!setup_ok) {
        free(pub_sub);
        moq_simpair_destroy(sp);
        return 1;
    }

    /* Shared zero-copy payload. */
    uint8_t *payload_data = (uint8_t *)malloc((size_t)cfg->object_size);
    if (!payload_data) {
        fprintf(stderr, "payload malloc failed\n");
        free(pub_sub); moq_simpair_destroy(sp);
        return 1;
    }
    memset(payload_data, 0xAB, (size_t)cfg->object_size);
    moq_rcbuf_t *payload = NULL;
    if (moq_rcbuf_wrap(&alloc, payload_data, (size_t)cfg->object_size,
                        NULL, NULL, &payload) != MOQ_OK) {
        fprintf(stderr, "rcbuf_wrap failed\n");
        free(payload_data); free(pub_sub); moq_simpair_destroy(sp);
        return 1;
    }

    uint64_t received = 0, payload_bytes = 0;
    int had_error = 0;

    /* Warmup: deliver across aliases, round-robin. */
    for (uint64_t i = 0; i < cfg->warmup && !had_error; i++) {
        uint64_t sel = i % nsub;
        uint64_t group = i / cfg->objects_per_group;
        uint64_t obj = i % cfg->objects_per_group;
        bool eog = (obj == cfg->objects_per_group - 1);
        moq_result_t rc = moq_session_send_object_datagram(
            srv, pub_sub[sel], group, obj, 0, eog, payload, NULL, 0,
            moq_simpair_now_us(sp));
        if (rc != MOQ_OK) { fprintf(stderr, "warmup datagram rc=%d\n", rc);
            had_error = 1; break; }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        drain_events(cli, &received, &payload_bytes);
    }

    /* Reset counters for the measured phase. */
    received = 0; payload_bytes = 0;
    alloc_state.alloc_count = 0;
    alloc_state.free_count = 0;
    int64_t baseline_live = alloc_state.live_bytes;
    alloc_state.peak_bytes = alloc_state.live_bytes;

    uint64_t lat_min = UINT64_MAX, lat_max = 0, lat_sum = 0, lat_count = 0;

    uint64_t t_start = now_us();
    for (uint64_t i = 0; i < cfg->objects && !had_error; i++) {
        uint64_t abs_i = cfg->warmup + i;
        uint64_t sel = abs_i % nsub;
        uint64_t group = abs_i / cfg->objects_per_group;
        uint64_t obj = abs_i % cfg->objects_per_group;
        bool eog = (obj == cfg->objects_per_group - 1);

        uint64_t t_pub = now_us();
        moq_result_t rc = moq_session_send_object_datagram(
            srv, pub_sub[sel], group, obj, 0, eog, payload, NULL, 0,
            moq_simpair_now_us(sp));
        if (rc != MOQ_OK) { fprintf(stderr, "datagram rc=%d\n", rc);
            had_error = 1; break; }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        drain_events(cli, &received, &payload_bytes);
        uint64_t lat = now_us() - t_pub;
        if (lat < lat_min) lat_min = lat;
        if (lat > lat_max) lat_max = lat;
        lat_sum += lat;
        lat_count++;
    }
    uint64_t elapsed_us = now_us() - t_start;
    if (elapsed_us == 0) elapsed_us = 1;

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
    free(pub_sub);
    moq_simpair_destroy(sp);

    if (cfg->json) {
        printf("{\n");
        printf("  \"benchmark\": \"session_lookup\",\n");
        printf("  \"mode\": \"alias\",\n");
        printf("  \"subscriptions\": %" PRIu64 ",\n", nsub);
        printf("  \"objects\": %" PRIu64 ",\n", cfg->objects);
        printf("  \"objects_received\": %" PRIu64 ",\n", received);
        printf("  \"objects_lost\": %" PRIu64 ",\n", lost);
        printf("  \"object_size\": %" PRIu64 ",\n", cfg->object_size);
        printf("  \"objects_per_group\": %" PRIu64 ",\n", cfg->objects_per_group);
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
        printf("--- moq_bench_session_lookup (mode=alias) ---\n");
        printf("subscriptions     : %" PRIu64 "\n", nsub);
        printf("objects           : %" PRIu64 " (received %" PRIu64
               ", lost %" PRIu64 ")\n", cfg->objects, received, lost);
        printf("object size       : %" PRIu64 " B\n", cfg->object_size);
        printf("elapsed           : %.2f ms\n", elapsed_ms);
        printf("throughput        : %.1f obj/s\n", obj_per_sec);
        printf("cycle latency us  : min %" PRIu64 " avg %.1f max %" PRIu64 "\n",
               lat_min, lat_avg_us, lat_max);
        printf("allocs/frees      : %" PRId64 " / %" PRId64 "\n",
               measured_allocs, measured_frees);
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
