/*
 * bench_relay_sched — the PRODUCTION subscribe-delivery scheduler hot path.
 *
 * bench_relay_core measures the log's arrival-seq cursor (moqr_log_cursor_next),
 * which the relay does NOT use for delivery. This benchmark drives the REAL
 * scheduler flagged as the hot path:
 * moqr_core_next_delivery / moqr_core_delivery_done over the per-subscription
 * gpos cursor + sub_best_candidate. No moqr_log_cursor_next is used.
 *
 * Model: one hot retained track; N downstream bindings, ONE subscription each
 * (the production hot-track fan-out shape). max_subs is sized to the case's
 * subscriber count, so the per-call O(max_subs) sub scan is proportional — the
 * ns/delivery curve then reveals it. Retained content is ingested BEFORE timing;
 * the timed drain only calls next_delivery/delivery_done, which incref/decref
 * already-retained rcbufs and never malloc — verified per run (ops-during-drain
 * must be 0), so the numbers reflect scheduler traversal, not payload copying.
 *
 * Output: CSV on stdout —
 *   case,subs,groups,lists,objects,deliveries,ns_total,ns_per_delivery
 * Deterministic (fixed inputs; the scheduler has no clock/randomness). deliveries
 * and the alloc check are exact; ns is wall-clock (min over --repeats runs) and
 * advisory. --quick runs a reduced matrix with one repeat (the smoke shape).
 *
 * Conventions follow benchmarks/ and bench_relay_core: counting allocator, CSV
 * output, unknown-flag rejection, a bounded smoke + WILL_FAIL guard in CTest.
 */

#include <moq/relay/relay.h>

#include <moq/rcbuf.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* -- counting allocator: proves the timed drain does no malloc/realloc/free --- */

typedef struct ca {
    moq_alloc_t vt;
    long        ops;   /* every alloc/realloc/free call (success or not) */
} ca_t;

static void *
ca_a(size_t n, void *c)
{
    ((ca_t *)c)->ops++;
    return malloc(n);
}
static void *
ca_r(void *p, size_t o, size_t n, void *c)
{
    (void)o;
    ((ca_t *)c)->ops++;
    return realloc(p, n);
}
static void
ca_f(void *p, size_t n, void *c)
{
    (void)n;
    ((ca_t *)c)->ops++;
    free(p);
}
static void
ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
}

static uint64_t
now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

static moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

#define BENCH_NOW UINT64_C(1000000)   /* fixed virtual instant (> 0) */

/* Drain the intent ring so per-op ACCEPT_* intents never overflow it. */
static void
drain_intents(moqr_core_t *c)
{
    moqr_intent_t its[32];
    while (moqr_core_poll_intents(c, its, 32) > 0) {
    }
}

/* Ingest one whole-object NORMAL record (small payload; delivery is zero-copy so
 * size is irrelevant to the measurement). */
static int
ing(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t sg, uint64_t o)
{
    uint8_t buf[16];
    memset(buf, (uint8_t)(g * 31u + o), sizeof(buf));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&a->vt, buf, sizeof(buf), &pl) != 0) {
        return -1;
    }
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 128;
    d.payload = pl;
    d.now_us = 1;
    if (moqr_core_ingest(c, t, &d) != MOQR_OK) {
        moq_rcbuf_decref(pl);
        return -1;
    }
    return 0;
}

typedef struct {
    const char *name;
    uint32_t    subs;
    uint32_t    groups;
    uint32_t    lists;   /* subgroups per group */
    uint32_t    objs;    /* objects per subgroup */
} bench_case_t;

/* Build the core + hot track + N single-sub bindings, ingest the matrix, then
 * TIME the fan-out drain (next_delivery/delivery_done over every binding).
 * Prints one CSV row (min ns over `repeats`). Returns failures (setup error,
 * delivery-count mismatch, or a non-alloc-free drain). */
static int
run_case(const bench_case_t *cs, int repeats)
{
    uint64_t objects = (uint64_t)cs->groups * cs->lists * cs->objs;
    uint64_t expect = (uint64_t)cs->subs * objects;
    uint64_t best_ns = UINT64_MAX;
    uint64_t best_deliv = 0;
    int      failures = 0;

    moqr_binding_t *dbs = calloc(cs->subs, sizeof(*dbs));
    if (dbs == NULL) {
        fprintf(stderr, "# %s: OOM binding array\n", cs->name);
        return 1;
    }

    for (int rep = 0; rep < repeats; rep++) {
        ca_t a;
        ca_init(&a);
        moqr_core_relay_cfg_t cfg;
        moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
        cfg.max_bindings = cs->subs + 2;   /* N downstream + publisher */
        cfg.max_subs = cs->subs + 2;       /* sized to fan-out -> O(subs) scan */
        cfg.max_tracks = 2;
        cfg.max_ns_nodes = 16;
        cfg.log_budget.max_groups = cs->groups;
        cfg.log_budget.max_bytes = objects * 64 + (1u << 16);
        cfg.log_max_subgroups = cs->lists;
        cfg.log_max_objects_per_group = cs->lists * cs->objs + 4;
        cfg.linger_us = 1000000;

        moqr_core_t *c = NULL;
        if (moqr_core_create(&cfg, &c) != MOQR_OK) {
            fprintf(stderr, "# %s: core create failed\n", cs->name);
            failures++;
            break;
        }

        moq_bytes_t nsb[1] = { B("bench") };
        moqr_ns_t ns = { nsb, 1 };
        moqr_binding_t pub;
        moqr_track_t track;
        if (moqr_core_binding_open(c, 1, &pub) != MOQR_OK ||
            moqr_core_announce(c, pub, ns) != MOQR_OK ||
            moqr_core_publish_open(c, pub, ns, B("v"), 5, &track) != MOQR_OK) {
            fprintf(stderr, "# %s: publisher setup failed\n", cs->name);
            failures++;
            moqr_core_destroy(c);
            break;
        }
        drain_intents(c);

        int ing_fail = 0;
        for (uint32_t g = 0; g < cs->groups && !ing_fail; g++) {
            for (uint32_t sg = 0; sg < cs->lists && !ing_fail; sg++) {
                for (uint32_t o = 0; o < cs->objs; o++) {
                    if (ing(c, &a, track, g, sg,
                            (uint64_t)sg * cs->objs + o) != 0) {
                        ing_fail = 1;
                        break;
                    }
                }
            }
        }
        if (ing_fail) {
            fprintf(stderr, "# %s: ingest failed\n", cs->name);
            failures++;
            moqr_core_destroy(c);
            break;
        }

        int sub_fail = 0;
        for (uint32_t i = 0; i < cs->subs; i++) {
            if (moqr_core_binding_open(c, 100 + i, &dbs[i]) != MOQR_OK) {
                sub_fail = 1;
                break;
            }
            moqr_subscribe_req_t rq;
            moqr_subscribe_req_init(&rq);
            rq.ns = ns;
            rq.name = B("v");
            rq.filter.type = MOQR_FILTER_ABSOLUTE_START;   /* from {0,0} */
            rq.cookie = 100 + i;
            moqr_sub_t sub;
            if (moqr_core_subscribe(c, dbs[i], &rq, &sub) != MOQR_OK) {
                sub_fail = 1;
                break;
            }
            drain_intents(c);   /* consume the ACCEPT_SUB intent */
        }
        if (sub_fail) {
            fprintf(stderr, "# %s: subscribe setup failed\n", cs->name);
            failures++;
            moqr_core_destroy(c);
            break;
        }

        /* TIMED: fan the retained matrix out to every subscription. */
        long     ops0 = a.ops;
        uint64_t deliv = 0;
        int      rep_bad = 0;
        uint64_t t0 = now_ns();
        for (uint32_t i = 0; i < cs->subs && !rep_bad; i++) {
            for (;;) {
                moqr_delivery_t d;
                moqr_result_t rc =
                    moqr_core_next_delivery(c, dbs[i], BENCH_NOW, &d);
                if (rc == MOQR_DONE) {
                    break;
                }
                if (rc != MOQR_OK) {
                    fprintf(stderr, "# %s: next_delivery rc=%d\n", cs->name,
                            (int)rc);
                    rep_bad = 1;
                    break;
                }
                if (moqr_core_delivery_done(c, dbs[i], MOQR_DELIVERY_DELIVERED,
                                            BENCH_NOW) != MOQR_OK) {
                    fprintf(stderr, "# %s: delivery_done failed\n", cs->name);
                    rep_bad = 1;
                    break;
                }
                deliv++;
            }
        }
        uint64_t elapsed = now_ns() - t0;
        long ops = a.ops - ops0;
        moqr_core_destroy(c);

        /* Validate EVERY repeat, not just the fastest: a delivery error, a wrong
         * delivery count, or ANY allocator op during the timed drain fails the
         * case even if another repeat was clean. Only valid repeats feed the
         * timing output. */
        if (rep_bad) {
            failures++;
            continue;
        }
        if (deliv != expect) {
            fprintf(stderr, "# %s: rep %d deliveries %llu != expected %llu\n",
                    cs->name, rep, (unsigned long long)deliv,
                    (unsigned long long)expect);
            failures++;
            continue;
        }
        if (ops != 0) {
            fprintf(stderr,
                    "# %s: rep %d timed drain did %ld allocator ops (not "
                    "malloc-free) — result would reflect allocation, not "
                    "scheduling\n",
                    cs->name, rep, ops);
            failures++;
            continue;
        }
        if (elapsed < best_ns) {
            best_ns = elapsed;
            best_deliv = deliv;
        }
    }

    free(dbs);

    if (failures == 0) {
        double ns_per =
            best_deliv > 0 ? (double)best_ns / (double)best_deliv : 0.0;
        printf("%s,%u,%u,%u,%u,%llu,%llu,%.1f\n", cs->name, cs->subs, cs->groups,
               cs->lists, (unsigned)objects, (unsigned long long)best_deliv,
               (unsigned long long)best_ns, ns_per);
    }
    return failures;
}

static const bench_case_t FULL_MATRIX[] = {
    /* fan-out scaling: minimal groups/lists so the per-call O(max_subs) sub scan
     * (which is NOT indexed by binding) dominates; vary subscriber count */
    { "fanout", 1, 1, 1, 64 },
    { "fanout", 8, 1, 1, 64 },
    { "fanout", 32, 1, 1, 64 },
    { "fanout", 128, 1, 1, 64 },
    { "fanout", 256, 1, 1, 64 },
    /* group scaling: fixed subs, vary retained groups (quadratic gpos_entry) */
    { "groups", 32, 1, 4, 2 },
    { "groups", 32, 8, 4, 2 },
    { "groups", 32, 32, 4, 2 },
    { "groups", 32, 64, 4, 2 },
    /* list scaling: fixed subs+groups, vary subgroups per group */
    { "lists", 32, 8, 1, 2 },
    { "lists", 32, 8, 4, 2 },
    { "lists", 32, 8, 16, 2 },
    /* default stress shape: many subs over several groups/lists (the bad case) */
    { "stress", 128, 32, 8, 2 },
};

static const bench_case_t QUICK_MATRIX[] = {
    { "fanout", 1, 1, 1, 64 },
    { "fanout", 128, 1, 1, 64 },
    { "groups", 16, 8, 4, 2 },
    { "groups", 16, 32, 4, 2 },
    { "lists", 16, 8, 4, 2 },
    { "lists", 16, 8, 16, 2 },
    { "stress", 32, 8, 4, 2 },
};

static void
usage(const char *argv0)
{
    fprintf(stderr, "usage: %s [--quick] [--repeats N]\n", argv0);
}

int
main(int argc, char **argv)
{
    int quick = 0;
    int repeats = 3;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--quick") == 0) {
            quick = 1;
            repeats = 1;
        } else if (strcmp(argv[i], "--repeats") == 0 && i + 1 < argc) {
            repeats = atoi(argv[++i]);
            if (repeats < 1) {
                usage(argv[0]);
                return 2;
            }
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    const bench_case_t *m = quick ? QUICK_MATRIX : FULL_MATRIX;
    size_t n = quick ? sizeof(QUICK_MATRIX) / sizeof(QUICK_MATRIX[0])
                     : sizeof(FULL_MATRIX) / sizeof(FULL_MATRIX[0]);

    printf("case,subs,groups,lists,objects,deliveries,ns_total,ns_per_delivery\n");
    int failures = 0;
    for (size_t i = 0; i < n; i++) {
        failures += run_case(&m[i], repeats);
    }
    return failures == 0 ? 0 : 1;
}
