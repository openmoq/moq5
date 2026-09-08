/*
 * bench_relay_core — the relay primitive's hot path, no sessions, no
 * transport: rcbuf ingest -> log append -> N cursor reads (each simulating
 * a delivery ref with incref/decref) -> group-granular eviction.
 *
 * The acceptance target: with the recycling pool allocator,
 * 0 real allocations and 0 real frees after warmup. Payload bytes never
 * copy; fan-out is reference traffic only.
 *
 * Conventions follow benchmarks/: counting/pool allocator, warmup with
 * post-warmup counter reset, --json output, smoke + rejects CTest guards.
 * Wall-clock throughput is scheduler-noisy; the durable signals are the
 * allocation counters and the per-(object x cursor) average.
 */

#include <moq/relay/capacity.h>
#include <moq/relay/log.h>
#include <moq/relay/trace.h>

#include <moq/rcbuf.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* -- exact-size recycling pool allocator (bench-local) ------------------- */

#define POOL_CLASSES 64

typedef struct pool_node {
    struct pool_node *next;
} pool_node_t;

typedef struct pool_class {
    size_t       size;
    pool_node_t *free;
} pool_class_t;

typedef struct pool_alloc {
    moq_alloc_t  vt;
    pool_class_t classes[POOL_CLASSES];
    long         real_allocs;
    long         real_frees;
    long         recycled;
} pool_alloc_t;

static pool_class_t *
pool_class_for(pool_alloc_t *p, size_t size)
{
    for (int i = 0; i < POOL_CLASSES; i++) {
        if (p->classes[i].size == size) {
            return &p->classes[i];
        }
        if (p->classes[i].size == 0) {
            p->classes[i].size = size;
            return &p->classes[i];
        }
    }
    return NULL;
}

static void *
pool_alloc_fn(size_t size, void *ctx)
{
    pool_alloc_t *p = ctx;
    if (size < sizeof(pool_node_t)) {
        size = sizeof(pool_node_t);
    }
    pool_class_t *c = pool_class_for(p, size);
    if (c != NULL && c->free != NULL) {
        pool_node_t *n = c->free;
        c->free = n->next;
        p->recycled++;
        return n;
    }
    p->real_allocs++;
    return malloc(size);
}

static void
pool_free_fn(void *ptr, size_t size, void *ctx)
{
    pool_alloc_t *p = ctx;
    if (ptr == NULL) {
        return;
    }
    if (size < sizeof(pool_node_t)) {
        size = sizeof(pool_node_t);
    }
    pool_class_t *c = pool_class_for(p, size);
    if (c != NULL) {
        pool_node_t *n = ptr;
        n->next = c->free;
        c->free = n;
        return;
    }
    p->real_frees++;
    free(ptr);
}

static void *
pool_realloc_fn(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    void *n = pool_alloc_fn(new_size, ctx);
    if (n == NULL) {
        return NULL;
    }
    if (ptr != NULL) {
        memcpy(n, ptr, old_size < new_size ? old_size : new_size);
        pool_free_fn(ptr, old_size, ctx);
    }
    return n;
}

static void
pool_drain(pool_alloc_t *p)
{
    for (int i = 0; i < POOL_CLASSES; i++) {
        pool_node_t *n = p->classes[i].free;
        while (n != NULL) {
            pool_node_t *next = n->next;
            free(n);
            p->real_frees++;
            n = next;
        }
        p->classes[i].free = NULL;
    }
}

static void
pool_init(pool_alloc_t *p)
{
    memset(p, 0, sizeof(*p));
    p->vt.ctx = p;
    p->vt.alloc = pool_alloc_fn;
    p->vt.realloc = pool_realloc_fn;
    p->vt.free = pool_free_fn;
}

/* -- bench --------------------------------------------------------------- */

static uint64_t
now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static void
usage(const char *argv0)
{
    fprintf(stderr,
            "usage: %s [--objects N] [--cursors N] [--object-size N]\n"
            "          [--warmup N] [--group-size N] [--trace] [--json]\n",
            argv0);
}

int
main(int argc, char **argv)
{
    uint64_t objects = 20000;
    uint32_t cursors = 8;
    size_t object_size = 1200;
    uint64_t warmup = 2000;
    uint64_t group_size = 30;
    int use_trace = 0;
    int json = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--objects") == 0 && i + 1 < argc) {
            objects = strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--cursors") == 0 && i + 1 < argc) {
            cursors = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--object-size") == 0 && i + 1 < argc) {
            object_size = strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc) {
            warmup = strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--group-size") == 0 && i + 1 < argc) {
            group_size = strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--trace") == 0) {
            use_trace = 1;
        } else if (strcmp(argv[i], "--json") == 0) {
            json = 1;
        } else {
            usage(argv[0]);
            return 2;
        }
    }
    if (objects == 0 || cursors == 0 || group_size == 0 ||
        warmup >= objects) {
        usage(argv[0]);
        return 2;
    }

    pool_alloc_t pool;
    pool_init(&pool);

    moqr_trace_t *trace = NULL;
    if (use_trace &&
        moqr_trace_create(&pool.vt, 4096, &trace) != MOQR_OK) {
        fprintf(stderr, "trace create failed\n");
        return 1;
    }

    moqr_log_cfg_t cfg;
    moqr_log_cfg_init_sized(&cfg, sizeof(cfg), &pool.vt);
    cfg.budget.max_groups = 8;
    cfg.budget.max_bytes = (uint64_t)object_size * group_size * 8 + 4096;
    cfg.max_cursors = cursors;
    cfg.trace = trace;

    moqr_log_t *log = NULL;
    if (moqr_log_create(&cfg, &log) != MOQR_OK) {
        fprintf(stderr, "log create failed\n");
        return 1;
    }

    moqr_cursor_t *cur = calloc(cursors, sizeof(*cur));
    if (cur == NULL) {
        return 1;
    }
    for (uint32_t c = 0; c < cursors; c++) {
        if (moqr_log_cursor_open(log, MOQR_CURSOR_FROM_OLDEST, &cur[c]) !=
            MOQR_OK) {
            fprintf(stderr, "cursor open failed\n");
            return 1;
        }
    }

    uint8_t *payload_src = malloc(object_size);
    if (payload_src == NULL) {
        return 1;
    }
    memset(payload_src, 0xAB, object_size);

    uint64_t delivered = 0, skips = 0, append_errors = 0;
    uint64_t t_start = 0;
    long warm_allocs = 0, warm_frees = 0;

    for (uint64_t o = 0; o < objects; o++) {
        if (o == warmup) {
            warm_allocs = pool.real_allocs;
            warm_frees = pool.real_frees;
            t_start = now_ns();
        }

        uint64_t group = o / group_size;
        uint64_t object = o % group_size;
        bool eog = object == group_size - 1;

        moq_rcbuf_t *payload = NULL;
        if (moq_rcbuf_create(&pool.vt, payload_src, object_size,
                             &payload) != 0) {
            fprintf(stderr, "rcbuf create failed\n");
            return 1;
        }

        moqr_log_append_desc_t d;
        moqr_log_append_desc_init(&d);
        d.group_id = group + 1;
        d.subgroup_id = 0;
        d.object_id = object;
        d.publisher_priority = 128;
        d.end_of_group = eog;
        d.payload = payload;
        d.now_us = o;
        if (moqr_log_append(log, &d) != MOQR_OK) {
            append_errors++;
            moq_rcbuf_decref(payload);
            continue;
        }

        for (uint32_t c = 0; c < cursors; c++) {
            moqr_record_view_t v;
            bool skipped = false;
            while (moqr_log_cursor_next(log, cur[c], &v, &skipped) ==
                   MOQR_OK) {
                if (skipped) {
                    skips++;
                }
                if (v.payload != NULL) {
                    /* Simulate handing the payload to a session write
                     * (write_object_ex increfs internally). */
                    moq_rcbuf_t *ref =
                        moq_rcbuf_incref((moq_rcbuf_t *)v.payload);
                    moq_rcbuf_decref(ref);
                }
                delivered++;
            }
        }
    }

    uint64_t elapsed_ns = now_ns() - t_start;
    long allocs_after = pool.real_allocs - warm_allocs;
    long frees_after = pool.real_frees - warm_frees;
    uint64_t measured = objects - warmup;
    double obj_per_sec = elapsed_ns > 0
                             ? (double)measured * 1e9 / (double)elapsed_ns
                             : 0.0;
    double ns_per_delivery =
        delivered > 0 ? (double)elapsed_ns / (double)(measured * cursors)
                      : 0.0;

    const char *check_reason = NULL;
    int check_ok = moqr_log_capacity_check(log, &check_reason) == MOQR_OK;

    if (json) {
        printf("{\"benchmark\":\"relay_core\",\"objects\":%llu,"
               "\"cursors\":%u,\"object_size\":%zu,\"warmup\":%llu,"
               "\"group_size\":%llu,\"delivered\":%llu,\"skips\":%llu,"
               "\"append_errors\":%llu,\"allocs_after_warmup\":%ld,"
               "\"frees_after_warmup\":%ld,\"recycled\":%ld,"
               "\"elapsed_ms\":%.3f,\"objects_per_sec\":%.0f,"
               "\"ns_per_object_cursor\":%.1f,\"trace_hash\":\"%llx\","
               "\"capacity_check\":%s}\n",
               (unsigned long long)objects, cursors, object_size,
               (unsigned long long)warmup, (unsigned long long)group_size,
               (unsigned long long)delivered, (unsigned long long)skips,
               (unsigned long long)append_errors, allocs_after, frees_after,
               pool.recycled, (double)elapsed_ns / 1e6, obj_per_sec,
               ns_per_delivery,
               (unsigned long long)moqr_trace_hash(trace),
               check_ok ? "true" : "false");
    } else {
        printf("relay_core: %llu objects x %u cursors (%zu B, groups of "
               "%llu)\n",
               (unsigned long long)objects, cursors, object_size,
               (unsigned long long)group_size);
        printf("  delivered            %llu (skips %llu, append errors "
               "%llu)\n",
               (unsigned long long)delivered, (unsigned long long)skips,
               (unsigned long long)append_errors);
        printf("  allocs after warmup  %ld\n", allocs_after);
        printf("  frees after warmup   %ld\n", frees_after);
        printf("  objects/sec          %.0f\n", obj_per_sec);
        printf("  ns/(object x cursor) %.1f\n", ns_per_delivery);
        printf("  capacity check       %s\n",
               check_ok ? "ok" : check_reason);
    }

    int rc = 0;
    if (!check_ok) {
        fprintf(stderr, "capacity check failed: %s\n", check_reason);
        rc = 1;
    }
    if (allocs_after != 0 || frees_after != 0) {
        fprintf(stderr,
                "zero-alloc target missed: %ld allocs, %ld frees after "
                "warmup\n",
                allocs_after, frees_after);
        rc = 1;
    }
    for (uint32_t c = 0; c < cursors; c++) {
        (void)moqr_log_cursor_close(log, cur[c]);
    }
    moqr_log_destroy(log);
    moqr_trace_destroy(trace);
    pool_drain(&pool);
    free(cur);
    free(payload_src);
    return rc;
}
