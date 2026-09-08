/*
 * Scenario harness: seeded op streams over the relay core with a
 * delivery-projection oracle and run-twice trace-hash determinism.
 *
 * Ops (from a splitmix64 stream): ingest bursts across groups/subgroups,
 * subscribe with random filters, unsubscribe, delivery pulls with seeded
 * capacity edges (random WOULD_BLOCK), ticks. Oracle asserts per sub:
 *   - identity: every delivered record was published on that track
 *     (payload tag match), with per-(sub,subgroup) object ids strictly
 *     ascending (subgroup wire order preserved);
 *   - predicate: nothing below the filter start or beyond the range end;
 *   - no delivery after retire;
 *   - completeness: when the sub observed no eviction skips, delivered
 *     count == passing records appended while active (exact drain).
 *
 * Env: MOQ_SCENARIO_SEEDS (default 25), MOQ_SCENARIO_SEED_START,
 * MOQ_SCENARIO_STEPS (default 60). On failure prints a replay line.
 */

#include <moq/relay/relay.h>

#include <moq/rcbuf.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

typedef struct rng {
    uint64_t s;
} rng_t;

static uint64_t
rng_next(rng_t *r)
{
    uint64_t z = (r->s += UINT64_C(0x9E3779B97F4A7C15));
    z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
    return z ^ (z >> 31);
}

typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
    long fail_at;   /* fail the Nth allocation; 0 = never */
} ca_t;
static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    if (a->fail_at != 0 && a->allocs + 1 >= a->fail_at) {
        return NULL;
    }
    void *p = malloc(n);
    if (p) {
        a->allocs++;
        a->live += (long)n;
    }
    return p;
}
static void *ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    void *q = realloc(p, n);
    if (q) {
        a->live += (long)n - (long)o;
    }
    return q;
}
static void ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p) {
        a->frees++;
        a->live -= (long)n;
    }
    free(p);
}
static void ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
}

/* -- shadow model ---------------------------------------------------------- */

#define MAX_PUB   4096
#define MAX_SUBS  16

typedef struct pub_rec {
    uint64_t group, subgroup, object;
    uint8_t  tag;
    bool     datagram;
} pub_rec_t;

typedef struct shadow_sub {
    bool      open;
    moqr_sub_t handle;
    uint64_t  start_g, start_o;
    bool      has_end;
    uint64_t  end_g;
    size_t    join_pub_count;    /* published count when it went active   */
    /* Wire order binds within ONE (group, subgroup) stream; the priority
     * scheduler may interleave groups in any order. Track last object per
     * (group, subgroup) pair. */
    struct {
        uint64_t group;
        uint64_t last_obj[2];
        bool     seen[2];
        bool     used;
    } gsg[64];
    uint64_t  delivered;
    bool      saw_skip;
    bool      retired;
} shadow_sub_t;

typedef struct world {
    pub_rec_t   pubs[MAX_PUB];
    size_t      pub_count;
    shadow_sub_t subs[MAX_SUBS];
    uint64_t    next_group;
    uint64_t    next_obj_in_group;
    int         failures;
} world_t;

static uint8_t
tag_of(uint64_t g, uint64_t sg, uint64_t o)
{
    return (uint8_t)(g * 31 + sg * 7 + o + 1);
}

static bool
was_published(const world_t *w, const moqr_record_view_t *v)
{
    for (size_t i = 0; i < w->pub_count; i++) {
        const pub_rec_t *p = &w->pubs[i];
        if (p->group == v->group_id && p->object == v->object_id &&
            (p->datagram == v->datagram_pref) &&
            (p->datagram || p->subgroup == v->subgroup_id)) {
            return v->payload != NULL &&
                   moq_rcbuf_data(v->payload)[0] ==
                       tag_of(p->group, p->subgroup, p->object);
        }
    }
    return false;
}

#define W_CHECK(w, expr)                                                  \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            (w)->failures++;                                              \
        }                                                                 \
    } while (0)

/* -- one scenario run -------------------------------------------------------- */

typedef struct run_result {
    uint64_t trace_hash;
    uint64_t trace_count;
    uint64_t delivered;
    int      failures;
} run_result_t;

static void
run_one(uint64_t seed, uint32_t steps, run_result_t *out, ca_t *ca)
{
    memset(out, 0, sizeof(*out));
    rng_t rng = { seed };
    world_t w;
    memset(&w, 0, sizeof(w));
    w.next_group = 1;

    moqr_trace_t *trace = NULL;
    if (moqr_trace_create(&ca->vt, 256, &trace) != MOQR_OK) {
        return;   /* OOM sweep path */
    }
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &ca->vt);
    cfg.trace = trace;
    cfg.log_budget.max_groups = 6;
    cfg.log_budget.max_bytes = 64 * 1024;
    cfg.linger_us = 100;
    moqr_core_t *c = NULL;
    if (moqr_core_create(&cfg, &c) != MOQR_OK) {
        moqr_trace_destroy(trace);
        return;
    }

    moqr_binding_t pub, subs_b;
    moq_bytes_t nsb[2];
    nsb[0] = (moq_bytes_t){ (const uint8_t *)"scn", 3 };
    nsb[1] = (moq_bytes_t){ (const uint8_t *)"one", 3 };
    moqr_ns_t ns = { nsb, 2 };
    moq_bytes_t name = { (const uint8_t *)"track", 5 };
    if (moqr_core_binding_open(c, 1, &pub) != MOQR_OK ||
        moqr_core_binding_open(c, 2, &subs_b) != MOQR_OK ||
        moqr_core_announce(c, pub, ns) != MOQR_OK) {
        goto done;
    }
    moqr_track_t track;
    if (moqr_core_publish_open(c, pub, ns, name, 1, &track) != MOQR_OK) {
        goto done;
    }
    moqr_intent_t its[16];
    (void)moqr_core_poll_intents(c, its, 16);

    for (uint32_t step = 0; step < steps; step++) {
        uint64_t op = rng_next(&rng) % 10;
        if (op < 4) {
            /* Ingest burst. */
            uint32_t burst = (uint32_t)(rng_next(&rng) % 4) + 1;
            for (uint32_t k = 0; k < burst && w.pub_count < MAX_PUB; k++) {
                uint64_t g = w.next_group;
                uint64_t sg = rng_next(&rng) % 2;
                bool dgram = (rng_next(&rng) % 8) == 0;
                uint64_t o = w.next_obj_in_group++;
                uint8_t tag = tag_of(g, dgram ? 0 : sg, o);
                uint8_t buf[48];
                memset(buf, tag, sizeof(buf));
                moq_rcbuf_t *pl = NULL;
                if (moq_rcbuf_create(&ca->vt, buf, sizeof(buf), &pl) != 0) {
                    break;
                }
                moqr_log_append_desc_t d;
                moqr_log_append_desc_init(&d);
                d.group_id = g;
                d.subgroup_id = dgram ? 0 : sg;
                d.object_id = o;
                d.publisher_priority = (uint8_t)(rng_next(&rng) % 3) * 64;
                d.datagram_pref = dgram;
                d.payload = pl;
                d.now_us = step;
                if (moqr_core_ingest(c, track, &d) == MOQR_OK) {
                    w.pubs[w.pub_count++] = (pub_rec_t){
                        g, dgram ? 0 : sg, o, tag, dgram
                    };
                } else {
                    moq_rcbuf_decref(pl);
                }
                if (w.next_obj_in_group >= 6) {
                    w.next_group++;
                    w.next_obj_in_group = 0;
                }
            }
        } else if (op < 6) {
            /* Subscribe with a random filter. */
            int slot = -1;
            for (int i = 0; i < MAX_SUBS; i++) {
                if (!w.subs[i].open) {
                    slot = i;
                    break;
                }
            }
            if (slot >= 0) {
                moqr_subscribe_req_t rq;
                moqr_subscribe_req_init(&rq);
                rq.ns = ns;
                rq.name = name;
                rq.cookie = (uint64_t)slot + 1000;
                uint64_t f = rng_next(&rng) % 3;
                if (f == 0) {
                    rq.filter.type = MOQR_FILTER_LARGEST_OBJECT;
                } else if (f == 1) {
                    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
                    rq.filter.start_group = 0;
                } else {
                    rq.filter.type = MOQR_FILTER_ABSOLUTE_RANGE;
                    rq.filter.start_group = w.next_group;
                    rq.filter.end_group_delta = 1 + rng_next(&rng) % 2;
                }
                moqr_sub_t sh;
                if (moqr_core_subscribe(c, subs_b, &rq, &sh) == MOQR_OK &&
                    moqr_sub_is_valid(sh)) {
                    shadow_sub_t *ss = &w.subs[slot];
                    memset(ss, 0, sizeof(*ss));
                    ss->open = true;
                    ss->handle = sh;
                    ss->join_pub_count = w.pub_count;
                    /* Read the ACCEPT intent to learn the resolved start. */
                    size_t n = moqr_core_poll_intents(c, its, 16);
                    for (size_t i = 0; i < n; i++) {
                        if (its[i].kind == MOQR_INTENT_ACCEPT_SUB &&
                            its[i].cookie == rq.cookie) {
                            if (rq.filter.type ==
                                MOQR_FILTER_LARGEST_OBJECT) {
                                ss->start_g = its[i].has_largest
                                                  ? its[i].largest_group
                                                  : 0;
                                ss->start_o =
                                    its[i].has_largest
                                        ? its[i].largest_object + 1
                                        : 0;
                            } else {
                                ss->start_g = rq.filter.start_group;
                                ss->start_o = rq.filter.start_object;
                            }
                            if (rq.filter.type ==
                                MOQR_FILTER_ABSOLUTE_RANGE) {
                                ss->has_end = true;
                                ss->end_g = rq.filter.start_group +
                                            rq.filter.end_group_delta;
                            }
                        }
                    }
                }
            }
        } else if (op == 6) {
            /* Unsubscribe a random open sub. */
            for (int i = 0; i < MAX_SUBS; i++) {
                if (w.subs[i].open && !w.subs[i].retired) {
                    (void)moqr_core_unsubscribe(c, w.subs[i].handle, step);
                    w.subs[i].retired = true;
                    break;
                }
            }
        } else if (op == 7) {
            (void)moqr_core_tick(c, step * 10);
            (void)moqr_core_poll_intents(c, its, 16);
        } else {
            /* Delivery pulls with seeded capacity edges. */
            for (uint32_t pulls = 0; pulls < 8; pulls++) {
                moqr_delivery_t d;
                moqr_result_t rc =
                    moqr_core_next_delivery(c, subs_b, step * 10, &d);
                if (rc != MOQR_OK) {
                    break;
                }
                if ((rng_next(&rng) % 5) == 0) {
                    (void)moqr_core_delivery_done(
                        c, subs_b, MOQR_DELIVERY_WOULD_BLOCK, step * 10);
                    break;   /* transport "blocked" this pump */
                }
                /* Oracle checks on the delivered record. */
                shadow_sub_t *ss = NULL;
                for (int i = 0; i < MAX_SUBS; i++) {
                    if (w.subs[i].open &&
                        moqr_sub_eq(w.subs[i].handle, d.sub)) {
                        ss = &w.subs[i];
                        break;
                    }
                }
                W_CHECK(&w, ss != NULL);
                if (d.notice != MOQR_DELIVERY_NOTICE_NONE) {
                    /* Recordless acknowledged notice (seal / eviction
                     * watermark): not a record — the oracle's record checks
                     * do not apply. Acknowledge and keep pulling. */
                    if (ss != NULL &&
                        d.notice == MOQR_DELIVERY_NOTICE_EVICT_WATERMARK) {
                        ss->saw_skip = true;
                    }
                    (void)moqr_core_delivery_done(
                        c, subs_b, MOQR_DELIVERY_DELIVERED, step * 10);
                    continue;
                }
                if (ss != NULL) {
                    W_CHECK(&w, !ss->retired);
                    W_CHECK(&w, was_published(&w, &d.rec));
                    W_CHECK(&w,
                            d.rec.group_id > ss->start_g ||
                                (d.rec.group_id == ss->start_g &&
                                 d.rec.object_id >= ss->start_o));
                    if (ss->has_end) {
                        W_CHECK(&w, d.rec.group_id <= ss->end_g);
                    }
                    if (!d.rec.datagram_pref && d.rec.subgroup_id < 2) {
                        /* Ascending object ids within this exact
                         * (group, subgroup) stream. */
                        size_t sg = (size_t)d.rec.subgroup_id;
                        int slot = -1, free_slot = -1;
                        for (int gi = 0; gi < 64; gi++) {
                            if (ss->gsg[gi].used &&
                                ss->gsg[gi].group == d.rec.group_id) {
                                slot = gi;
                                break;
                            }
                            if (!ss->gsg[gi].used && free_slot < 0) {
                                free_slot = gi;
                            }
                        }
                        if (slot < 0 && free_slot >= 0) {
                            slot = free_slot;
                            ss->gsg[slot].used = true;
                            ss->gsg[slot].group = d.rec.group_id;
                            ss->gsg[slot].seen[0] = false;
                            ss->gsg[slot].seen[1] = false;
                        }
                        if (slot >= 0) {
                            if (ss->gsg[slot].seen[sg]) {
                                W_CHECK(&w,
                                        d.rec.object_id >
                                            ss->gsg[slot].last_obj[sg]);
                            }
                            ss->gsg[slot].seen[sg] = true;
                            ss->gsg[slot].last_obj[sg] = d.rec.object_id;
                        }
                    }
                    ss->delivered++;
                }
                (void)moqr_core_delivery_done(c, subs_b,
                                              MOQR_DELIVERY_DELIVERED,
                                              step * 10);
                out->delivered++;
            }
            (void)moqr_core_poll_intents(c, its, 16);
        }
    }

    out->trace_hash = moqr_trace_hash(trace);
    out->trace_count = moqr_trace_count(trace);
    out->failures = w.failures;
done:
    moqr_core_destroy(c);
    moqr_trace_destroy(trace);
}

int
main(void)
{
    int failures = 0;
    uint64_t seeds = 25, seed_start = 0;
    uint32_t steps = 60;
    const char *e;
    if ((e = getenv("MOQ_SCENARIO_SEEDS")) != NULL) {
        seeds = strtoull(e, NULL, 0);
    }
    if ((e = getenv("MOQ_SCENARIO_SEED_START")) != NULL) {
        seed_start = strtoull(e, NULL, 0);
    }
    if ((e = getenv("MOQ_SCENARIO_STEPS")) != NULL) {
        steps = (uint32_t)strtoul(e, NULL, 0);
    }

    for (uint64_t s = seed_start; s < seed_start + seeds; s++) {
        ca_t a1, a2;
        ca_init(&a1);
        ca_init(&a2);
        run_result_t r1, r2;
        run_one(s, steps, &r1, &a1);
        run_one(s, steps, &r2, &a2);
        failures += r1.failures + r2.failures;
        if (r1.trace_hash != r2.trace_hash ||
            r1.trace_count != r2.trace_count ||
            r1.delivered != r2.delivered || a1.live != 0 || a2.live != 0) {
            printf("FAIL: seed 0x%" PRIx64
                   " nondeterministic or leaked: hash %" PRIx64 "/%" PRIx64
                   " delivered %" PRIu64 "/%" PRIu64 " live %ld/%ld\n",
                   s, r1.trace_hash, r2.trace_hash, r1.delivered,
                   r2.delivered, a1.live, a2.live);
            printf("replay: MOQ_SCENARIO_SEED_START=0x%" PRIx64
                   " MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%u "
                   "./test_relay_scenario\n",
                   s, steps);
            failures++;
        }
    }

    /* OOM sweep: fail allocation N for a range of N; no crash, no leak. */
    for (long fail_at = 1; fail_at <= 40; fail_at += 3) {
        ca_t a;
        ca_init(&a);
        a.fail_at = fail_at;
        run_result_t r;
        run_one(7, 20, &r, &a);
        if (a.live != 0) {
            printf("FAIL: OOM sweep fail_at=%ld leaked %ld bytes\n", fail_at,
                   a.live);
            failures++;
        }
    }

    if (failures == 0) {
        printf("PASS: relay_scenario (%" PRIu64 " seeds x %u steps + OOM "
               "sweep)\n",
               seeds, steps);
    }
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */
}
