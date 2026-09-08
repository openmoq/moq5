/*
 * bench_relay_saturation — the in-process OWNER-BOUNDARY throughput harness
 * (threaded per-shard steppers + machine-readable schema). This binary
 * MEASURES and DIAGNOSES; it optimizes nothing.
 *
 * One pthread owns each shard and free-runs the per-shard threaded seam
 * (moqr_shards_step_shard, live_visibility = true). Shard 0 owns one publisher
 * and one source track; K-1 remote destination shards each carry ONE coalesced
 * cross-shard demand (all subscribers on a shard share it), fanned out to local
 * subscribers by REFERENCE (never re-cloned). The owner lane clones each
 * produced record once per remote destination (O(K-1) per record) and pushes it
 * over the bounded ordered FIFO demand channels.
 *
 * This harness implements the CLOSED-LOOP ceiling / verification path only
 * (relay-r8g-saturation-plan.md, campaign 1). Open-loop-only schema fields are
 * emitted null/not-applicable, never fabricated.
 *
 * Collection respects the ownership contract: no coordinator reads a live
 * shard/core; each owning lane self-snapshots its own stats at two coordinated
 * barriers (B0 after warmup, B1 at a stop epoch), and the main thread reads the
 * published rows and post-join state. Timed B1-B0 checks are ALGEBRAIC ONLY
 * (partial in-flight objects may straddle either endpoint); the exact per-kind /
 * byte / clone / terminal totals are asserted on the LIFETIME vector after
 * drain.
 */

/* glibc hides cpu_set_t's macros and pthread_setaffinity_np unless the GNU
 * profile is selected, and a feature-test macro only counts if it precedes
 * every system header. Declared here rather than by the build so a direct or
 * standalone compile of this file cannot quietly lose the profile and fall
 * back to implicit declarations. */
#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE 1
#endif

#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/rcbuf.h>
#include <moq/relay/capacity.h>
#include <moq/relay/log.h>

#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>

#if defined(__linux__)
#  include <unistd.h>
#endif

/* ------------------------------------------------------------------ limits */

#define SAT_MAX_SHARDS 64u   /* == MOQR_SHARDS_MAX                            */

/* The owner source track uses ONE group with this byte budget and record cap
 * (no eviction during a valid run). The whole produced set must fit all three
 * resolved log ceilings: byte budget, per-group record count, and — for the
 * chunked shape — the OPEN-record chunk-node pool. */
#define SAT_LOG_BUDGET_BYTES (64ull << 20)
#define SAT_LOG_MAX_GROUPS 4u
#define SAT_LOG_OBJECTS_PER_GROUP 4096u   /* per-group record cap (one group)  */

static int g_failures;

#define SAT_CHECK(expr)                                                   \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

/* ------------------------------------------------- thread-safe allocator */

/* Malloc-backed, counters under a mutex (the runtime's mailbox / demand
 * memory and rcbuf clones cross shard threads with the message: producer
 * allocates, consumer frees). One watched size bucket gives a NON-VACUOUS
 * clone oracle: every rcbuf payload of the measured workload is allocated at
 * exactly moq_rcbuf_allocation_size(payload_len), so cumulative allocs at that
 * size = source-creates + boundary clones. A SECOND watched bucket at the
 * property allocation size gives an independent property-clone oracle (payload
 * and property clones are counted separately). The two sizes MUST be distinct
 * from each other and from every structural allocation. */
typedef struct sat_alloc {
    moq_alloc_t     vt;
    pthread_mutex_t mu;
    long            allocs, frees, live, peak;
    size_t          watch_size;    /* rcbuf alloc size for the measured payload */
    long            watch_allocs;   /* cumulative allocs at watch_size          */
    size_t          watch_size2;    /* rcbuf alloc size for the measured props  */
    long            watch_allocs2;  /* cumulative allocs at watch_size2         */
} sat_alloc_t;

static void *
sat_a(size_t n, void *c)
{
    sat_alloc_t *a = c;
    void        *p = malloc(n);
    if (p != NULL) {
        pthread_mutex_lock(&a->mu);
        a->allocs++;
        a->live += (long)n;
        if (a->live > a->peak) {
            a->peak = a->live;
        }
        if (a->watch_size != 0 && n == a->watch_size) {
            a->watch_allocs++;
        }
        if (a->watch_size2 != 0 && n == a->watch_size2) {
            a->watch_allocs2++;
        }
        pthread_mutex_unlock(&a->mu);
    }
    return p;
}

static void *
sat_realloc(void *p, size_t old_n, size_t new_n, void *c)
{
    sat_alloc_t *a = c;
    void        *q = realloc(p, new_n);
    if (q != NULL) {
        pthread_mutex_lock(&a->mu);
        a->live += (long)new_n - (long)old_n;
        if (a->live > a->peak) {
            a->peak = a->live;
        }
        pthread_mutex_unlock(&a->mu);
    }
    return q;
}

static void
sat_f(void *p, size_t n, void *c)
{
    sat_alloc_t *a = c;
    if (p != NULL) {
        pthread_mutex_lock(&a->mu);
        a->frees++;
        a->live -= (long)n;
        pthread_mutex_unlock(&a->mu);
        free(p);
    }
}

static void
sat_alloc_init(sat_alloc_t *a, size_t watch, size_t watch2)
{
    memset(a, 0, sizeof(*a));
    pthread_mutex_init(&a->mu, NULL);
    a->vt.ctx = a;
    a->vt.alloc = sat_a;
    a->vt.realloc = sat_realloc;
    a->vt.free = sat_f;
    a->watch_size = watch;
    /* A property size that collides with the payload size (or is zero) is not a
     * usable second oracle — disable the bucket so it never double-counts. */
    a->watch_size2 = (watch2 != 0 && watch2 != watch) ? watch2 : 0;
}

/* --------------------------------------------------------------- config */

typedef enum { SHAPE_WHOLE, SHAPE_CHUNKED } sat_shape_t;
typedef enum { DIST_REMOTE_UNIFORM, DIST_GAUNTLET } sat_dist_t;
typedef enum { FMT_CSV, FMT_JSON } sat_fmt_t;
/* CEILING = closed-loop bounded-window (measures B*(K)/mu_cap(K)); KNEE =
 * fixed-rate open-loop lambda-sweep (measures the rate-dependent knee). */
typedef enum { CAMPAIGN_CEILING, CAMPAIGN_KNEE } sat_campaign_t;

typedef struct sat_cfg {
    bool        verify;
    uint16_t    K;
    uint32_t    fanout;        /* remote-uniform: subs per remote shard;
                                  gauntlet: TOTAL subscriber count S           */
    uint32_t    objects;       /* N measured all-destination completions       */
    uint32_t    cap;           /* bounded in-flight window (owner backlog)      */
    uint32_t    payload;       /* payload bytes (whole) / chunk bytes (chunked) */
    uint32_t    property;      /* property bytes per record (0 == none)         */
    uint32_t    chunks;        /* chunks per object for the chunked shape       */
    sat_shape_t shape;
    sat_dist_t  dist;
    sat_fmt_t   fmt;
    sat_campaign_t campaign;    /* ceiling (closed-loop) | knee (open-loop)     */
    double      lambda;         /* knee: requested offered rate (objects/sec)   */
    uint32_t    duration_ms;    /* knee: fixed timed-window duration             */
    double      rate_tolerance; /* knee: max |actual-requested|/requested        */
    double      skew_fraction;
    double      lateness_fraction;
    bool        require_affinity;
} sat_cfg_t;

/* ------------------------------------------------------------ affinity */

/* Pin the calling thread to CPU `cpu`. Returns true only where real affinity
 * exists (Linux). macOS lacks a strict portable pthread affinity, so a
 * canonical pinned curve is not available there. */
static bool
sat_pin_self(int cpu)
{
#if defined(__linux__)
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    return pthread_setaffinity_np(pthread_self(), sizeof(set), &set) == 0;
#else
    (void)cpu;
    return false;
#endif
}

static long
sat_ncpu(void)
{
#if defined(__linux__)
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? n : 1;
#else
    return 1;
#endif
}

/* --------------------------------------------------------------- timing */

static uint64_t
sat_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static moq_bytes_t
SB(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

/* ------------------------------------------------------- topology model */

/* Per-shard subscriber counts for the chosen distribution. Shard 0 always
 * owns the publisher. */
typedef struct sat_topo {
    uint32_t nsub[SAT_MAX_SHARDS];   /* subscribers on each shard             */
    uint32_t local_subs;             /* nsub[0] (served from the owner log)   */
    uint32_t remote_subs;            /* sum over d>0                          */
    uint16_t remote_demands;         /* #{d>0 : nsub[d] > 0}                  */
} sat_topo_t;

static void
sat_topology(const sat_cfg_t *c, sat_topo_t *t)
{
    memset(t, 0, sizeof(*t));
    if (c->dist == DIST_REMOTE_UNIFORM) {
        /* F subscribers on every remote shard 1..K-1; none local. */
        for (uint16_t d = 1; d < c->K; d++) {
            t->nsub[d] = c->fanout;
        }
    } else {
        /* gauntlet: the relay's connection->lane rule is round-robin by
         * arrival index. The publisher connects first (fresh in-process
         * counter -> lane 0); subscriber i (1..S) lands on lane i % K. So
         * floor(S/K) subscribers co-locate on the owner shard (local) and the
         * rest spread one-per-lane across the remote shards. */
        for (uint32_t i = 1; i <= c->fanout; i++) {
            t->nsub[i % c->K]++;
        }
    }
    t->local_subs = t->nsub[0];
    for (uint16_t d = 1; d < c->K; d++) {
        t->remote_subs += t->nsub[d];
        if (t->nsub[d] > 0) {
            t->remote_demands++;
        }
    }
}

/* ------------------------------------------------------------- the rig */

typedef struct sat_sub {
    moqr_binding_t bind;
    uint32_t       count;   /* subs on this shard's binding                   */
} sat_shard_binding_t;

typedef struct sat_rig {
    sat_alloc_t        *alloc;
    moqr_shards_t      *s;
    sat_cfg_t           cfg;
    sat_topo_t          topo;
    moqr_binding_t      pub;
    moqr_track_t        track;
    sat_shard_binding_t sb[SAT_MAX_SHARDS];   /* one binding per shard w/ subs */
    uint16_t            payload_alloc;         /* moq_rcbuf_allocation_size(P)  */
    moqr_shards_cfg_t   shcfg;                 /* kept for capacity_describe    */
} sat_rig_t;

/* The wake bits a K-shard step may legally return: every shard index, the
 * stepped shard's own included — peers arrive via push/credit, the self bit
 * via the one coalesced local continuation. Anything out of range is a bug. */
static uint64_t
sat_legal_wake(uint16_t K)
{
    return (K >= 64) ? ~0ull : ((1ull << K) - 1u);
}

/* Round-robin one step across every shard (single-threaded setup / drain under
 * live visibility; a serialized schedule of the same per-shard seam the lanes
 * run). Returns true if every step was MOQR_OK and no wake mask carried an
 * out-of-range bit; the stepped shard's own bit is legal (the local
 * continuation) and the loop's next pass performs it. */
static bool
sat_step_all(sat_rig_t *r, uint64_t now)
{
    bool ok = true;
    for (uint16_t d = 0; d < r->cfg.K; d++) {
        uint64_t mask = 0;
        if (moqr_shards_step_shard(r->s, d, now, &mask) != MOQR_OK) {
            ok = false;
        }
        if ((mask & ~sat_legal_wake(r->cfg.K)) != 0) {
            ok = false;
        }
    }
    return ok;
}

/* In-flight DATA slots: channels + progress + open objects (0 == data drained;
 * an active pump-sub / recorded demand is NOT counted, so this reaches 0 while
 * the demand is still admitted). */
static uint32_t
sat_inflight(sat_rig_t *r)
{
    uint32_t n = 0;
    for (uint16_t d = 0; d < r->cfg.K; d++) {
        n += moqr_shards_debug_owner_progress_slots(r->s, d);
        n += moqr_shards_debug_requester_open_objects(r->s, d);
        for (uint16_t e = 0; e < r->cfg.K; e++) {
            n += moqr_shards_debug_demand_channel_pending(r->s, d, e);
            n += moqr_shards_debug_mailbox_pending(r->s, d, e);
        }
    }
    return n;
}

/* Full quiescence: data slots PLUS the admitted pump-subs and recorded demands
 * (0 only after teardown, when every demand is retired). */
static uint32_t
sat_quiesced(sat_rig_t *r)
{
    uint32_t n = sat_inflight(r);
    for (uint16_t d = 0; d < r->cfg.K; d++) {
        n += moqr_shards_debug_owner_pump_subs(r->s, d);
        n += moqr_shards_debug_pending_demand(r->s, d);
    }
    return n;
}

/* Make one property rcbuf of the configured size (distinct byte fill so it can
 * never be mistaken for a payload). Returns NULL when properties are disabled
 * (cfg.property == 0) OR on allocation failure — the caller distinguishes the
 * two via `*failed`. The source creates exactly one per object; the owner clones
 * it once per remote destination (the property-copy oracle). */
static moq_rcbuf_t *
sat_make_props(sat_rig_t *r, uint64_t o, bool *failed)
{
    *failed = false;
    if (r->cfg.property == 0) {
        return NULL;
    }
    uint8_t *pb = malloc(r->cfg.property);
    if (pb == NULL) {
        *failed = true;
        return NULL;
    }
    memset(pb, (uint8_t)(0x30 + (o & 0xF)), r->cfg.property);
    moq_rcbuf_t *props = NULL;
    moq_result_t pr =
        moq_rcbuf_create(&r->alloc->vt, pb, r->cfg.property, &props);
    free(pb);
    if (pr != MOQ_OK) {
        *failed = true;
        return NULL;
    }
    return props;
}

/* Owner ingest of one whole object into the source track (direct core ingest,
 * exactly the deterministic bench's path). */
static bool
sat_pub_whole(sat_rig_t *r, uint64_t g, uint64_t o, uint64_t now)
{
    uint8_t *body = malloc(r->cfg.payload);
    if (body == NULL) {
        return false;
    }
    memset(body, (uint8_t)(0xB0 + (o & 0xF)), r->cfg.payload);
    moq_rcbuf_t *pl = NULL;
    moq_result_t rr = moq_rcbuf_create(&r->alloc->vt, body, r->cfg.payload, &pl);
    free(body);
    if (rr != MOQ_OK) {
        return false;
    }
    bool         pfail = false;
    moq_rcbuf_t *props = sat_make_props(r, o, &pfail);
    if (pfail) {
        moq_rcbuf_decref(pl);
        return false;
    }
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = 0;
    d.object_id = o;
    d.publisher_priority = 100;
    d.payload = pl;
    d.properties = props;   /* ownership transfers on ingest OK */
    d.now_us = now;
    moqr_result_t rc = moqr_core_ingest(moqr_shards_core(r->s, 0), r->track, &d);
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(pl);
        moq_rcbuf_decref(props);
    }
    return rc == MOQR_OK;
}

/* Owner ingest of one chunked object (OPEN + chunks + implicit END on the last
 * chunk via append semantics). Each chunk carries `payload` bytes. */
static bool
sat_pub_chunked(sat_rig_t *r, uint64_t g, uint64_t o, uint64_t now)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = 0;
    d.object_id = o;
    d.publisher_priority = 100;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = (uint64_t)r->cfg.payload * r->cfg.chunks;
    bool         pfail = false;
    moq_rcbuf_t *props = sat_make_props(r, o, &pfail);
    if (pfail) {
        return false;
    }
    d.properties = props;   /* ownership transfers on ingest OK */
    d.now_us = now;
    if (moqr_core_ingest(moqr_shards_core(r->s, 0), r->track, &d) != MOQR_OK) {
        moq_rcbuf_decref(props);
        return false;
    }
    for (uint32_t ci = 0; ci < r->cfg.chunks; ci++) {
        uint8_t *body = malloc(r->cfg.payload);
        if (body == NULL) {
            return false;
        }
        memset(body, (uint8_t)(0xC0 + ci), r->cfg.payload);
        moq_rcbuf_t *cb = NULL;
        moq_result_t rr =
            moq_rcbuf_create(&r->alloc->vt, body, r->cfg.payload, &cb);
        free(body);
        if (rr != MOQ_OK) {
            return false;
        }
        moqr_result_t rc = moqr_core_append_chunk(moqr_shards_core(r->s, 0),
                                                  r->track, g, 0, o, cb);
        moq_rcbuf_decref(cb);
        if (rc != MOQR_OK) {
            return false;
        }
    }
    /* Explicitly complete the record (the chunked OPEN does not auto-END). */
    return moqr_core_complete_record(moqr_shards_core(r->s, 0), r->track, g, 0,
                                     o) == MOQR_OK;
}

static bool
sat_pub_one(sat_rig_t *r, uint64_t o, uint64_t now)
{
    /* One group holds the whole run so nothing evicts during the measured
     * window (retention coupling: an evicted undelivered record would be loss
     * and invalidate the point). */
    return r->cfg.shape == SHAPE_WHOLE ? sat_pub_whole(r, 0, o, now)
                                       : sat_pub_chunked(r, 0, o, now);
}

/* Confirm one delivery outcome, checking the result. Every confirmation in the
 * bench routes through here so no delivery_done value is discarded. */
static void
sat_confirm(moqr_core_t *c, moqr_binding_t b, moqr_delivery_outcome_t outcome,
            uint64_t now, int *err)
{
    if (moqr_core_delivery_done(c, b, outcome, now) != MOQR_OK && err != NULL) {
        (*err)++;
    }
}

/* Drain deliverable records/notices on shard `d`'s subscriber binding. Counts
 * only COMPLETE objects (with their full byte total); a live-edge OPEN record is
 * confirmed MOQR_DELIVERY_STALLED — NOT DELIVERED — so the cursor does not
 * advance past a partial object before its remaining chunks arrive. Every
 * delivery_done result is checked (via sat_confirm); a non-OK outcome accrues
 * to `*err` (nullable). */
static void
sat_pull(sat_rig_t *r, uint16_t d, uint64_t now, uint64_t *objs, uint64_t *bytes,
         int *err)
{
    if (r->sb[d].count == 0) {
        return;
    }
    moqr_core_t   *c = moqr_shards_core(r->s, d);
    moqr_binding_t b = r->sb[d].bind;
    for (;;) {
        moqr_delivery_t dv;
        moqr_result_t   drc = moqr_core_next_delivery(c, b, now, &dv);
        if (drc == MOQR_DONE) {
            break;   /* ordinary: nothing deliverable right now */
        }
        if (drc != MOQR_OK) {
            /* CAPACITY / NOMEM / INVAL / stale / wrong-state are real errors,
             * not an empty queue — invalidate the point. */
            if (err != NULL) {
                (*err)++;
            }
            break;
        }
        if (dv.notice != MOQR_DELIVERY_NOTICE_NONE) {
            sat_confirm(c, b, MOQR_DELIVERY_DELIVERED, now, err);
            continue;
        }
        if (dv.rec.obj_state == MOQR_OBJ_ABANDONED) {
            sat_confirm(c, b, MOQR_DELIVERY_ABANDONED, now, err);
            continue;   /* terminal, never counted (no-loss workload) */
        }
        /* Byte-count the chunks not yet consumed for this record. Chunk indices
         * are ABSOLUTE append order; a chunk already consumed on a prior
         * (STALLED) exposure returns MOQR_DONE and is skipped, so summing every
         * exposure counts each chunk exactly once. */
        if (bytes != NULL && dv.rec.chunk_count > 0) {
            for (uint32_t i = 0; i < dv.rec.chunk_count; i++) {
                const moq_rcbuf_t *cb = NULL;
                uint64_t           cl = 0;
                if (moqr_core_delivery_chunk(c, b, i, &cb, &cl) == MOQR_OK) {
                    *bytes += cl;
                }
            }
        }
        if (dv.rec.obj_state == MOQR_OBJ_OPEN) {
            sat_confirm(c, b, MOQR_DELIVERY_STALLED, now, err);
            break;   /* not complete yet — the next step grows it */
        }
        /* COMPLETE: count one object; whole-object bytes are the payload. */
        if (objs != NULL) {
            (*objs)++;
        }
        if (bytes != NULL && dv.rec.chunk_count == 0 && dv.rec.payload != NULL) {
            *bytes += moq_rcbuf_len(dv.rec.payload);
        }
        sat_confirm(c, b, MOQR_DELIVERY_DELIVERED, now, err);
    }
}

/* Build the runtime + topology and step to the admitted fixed point (every
 * remote destination's coalesced demand ACKed, its subs active; local subs
 * active on the owner). */
static bool
sat_build(sat_rig_t *r, sat_alloc_t *a, const sat_cfg_t *c)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->cfg = *c;
    sat_topology(c, &r->topo);

    size_t asz = 0;
    (void)moq_rcbuf_allocation_size(c->payload, &asz);
    r->payload_alloc = (uint16_t)asz;

    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.shards = c->K;
    cfg.live_visibility = true;   /* threaded per-shard stepping, no round     */
    cfg.admit_remote_demand = true;   /* inert at K == 1                        */
    /* Budgets from max(K-1, F) + headroom (shared template covers the busiest
     * shard: owner holds ~K-1 pump-subs, each destination holds its subs). */
    uint32_t busiest = c->fanout;
    if ((uint32_t)(c->K - 1) > busiest) {
        busiest = (uint32_t)(c->K - 1);
    }
    cfg.core_cfg.max_bindings = 2u * c->K + 16u;
    cfg.core_cfg.max_subs = busiest + 32u;
    cfg.core_cfg.max_tracks = 64u;
    /* One group holds the whole run; size the log so nothing evicts. */
    cfg.core_cfg.log_budget.max_groups = SAT_LOG_MAX_GROUPS;
    cfg.core_cfg.log_budget.max_bytes = SAT_LOG_BUDGET_BYTES;
    cfg.core_cfg.log_max_objects_per_group = SAT_LOG_OBJECTS_PER_GROUP;
    cfg.core_cfg.linger_us = 5000;
    /* Give the demand channels and per-turn budget generous room — this
     * harness measures the boundary, it does not tune it. */
    cfg.demand_channel_entries = 256u;
    cfg.demand_channel_bytes = 64u << 20;
    cfg.pump_turn_messages = 64u;
    cfg.pump_turn_bytes = 8u << 20;
    r->shcfg = cfg;   /* kept for the capacity ceiling (pure over cfg) */
    if (moqr_shards_create(&cfg, &r->s) != MOQR_OK) {
        return false;
    }

    /* Publisher + announced ACTIVE source track on shard 0. */
    if (moqr_core_binding_open(moqr_shards_core(r->s, 0), 1, &r->pub) !=
        MOQR_OK) {
        return false;
    }
    moq_bytes_t nsp[1] = { SB("sat") };
    moqr_ns_t   ns = { nsp, 1 };
    if (moqr_core_announce(moqr_shards_core(r->s, 0), r->pub, ns) != MOQR_OK) {
        return false;
    }
    if (moqr_core_publish_open(moqr_shards_core(r->s, 0), r->pub, ns, SB("v"),
                               900, &r->track) != MOQR_OK) {
        return false;
    }
    uint64_t now = 1000;
    for (int i = 0; i < 16; i++) {   /* announce -> mirror on every shard     */
        if (!sat_step_all(r, now)) {
            return false;
        }
        now += 1000;
    }

    /* Subscribers per shard. cookie space is per binding. */
    uint32_t bcookie = 2;
    for (uint16_t d = 0; d < c->K; d++) {
        if (r->topo.nsub[d] == 0) {
            continue;
        }
        moqr_binding_t b;
        if (moqr_core_binding_open(moqr_shards_core(r->s, d), bcookie++, &b) !=
            MOQR_OK) {
            return false;
        }
        r->sb[d].bind = b;
        r->sb[d].count = r->topo.nsub[d];
        for (uint32_t f = 0; f < r->topo.nsub[d]; f++) {
            moqr_subscribe_req_t rq;
            moqr_subscribe_req_init(&rq);
            rq.ns = ns;
            rq.name = SB("v");
            rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
            rq.cookie = f;
            moqr_sub_t sub;
            if (moqr_core_subscribe(moqr_shards_core(r->s, d), b, &rq, &sub) !=
                MOQR_OK) {
                return false;
            }
        }
    }
    /* Step to the admitted fixed point: every remote demand crosses, the owner
     * admits + ACKs, parked subs activate. */
    for (int i = 0; i < 24; i++) {
        if (!sat_step_all(r, now)) {
            return false;
        }
        now += 1000;
    }
    /* Verify the fixed point (only meaningful at K > 1 with remote demands). */
    if (r->cfg.K > 1) {
        if (moqr_shards_debug_owner_pump_subs(r->s, 0) != r->topo.remote_demands) {
            return false;
        }
    }
    return true;
}

static void
sat_destroy(sat_rig_t *r)
{
    moqr_shards_destroy(r->s);
}

/* ------------------------------------------------- shared threaded state */

typedef enum { PH_WARMUP = 0, PH_WINDOW, PH_STOP } sat_phase_t;

typedef struct sat_shared {
    sat_rig_t *r;
    uint16_t   K;
    uint32_t   warmup;   /* completions to discard before B0                 */
    uint32_t   window;   /* N: additional all-destination completions        */
    uint32_t   cap;      /* bounded in-flight window                          */

    _Atomic uint32_t phase;
    _Atomic int      at_b0;      /* lanes that reached the B0 rendezvous       */
    _Atomic int      b0_ready;   /* destinations that snapshotted B0 + parked  */
    _Atomic int      b0_release; /* owner releases lanes into the window        */
    _Atomic int      b0_violation; /* a destination stepped before B0 release  */

    /* Per-destination completion = that shard's core ingested_total, written
     * ONLY by its owning lane, read by the owner for CAP / stop. Shard 0 slot
     * carries the owner's local delivered-object count. */
    _Atomic uint64_t completed[SAT_MAX_SHARDS];

    /* Per-lane self-sampled snapshots (written by the owning lane, read post
     * join). */
    moqr_shards_stats_t b0[SAT_MAX_SHARDS];
    moqr_shards_stats_t b1[SAT_MAX_SHARDS];
    uint64_t            core_ingest_b0[SAT_MAX_SHARDS];
    uint64_t            core_ingest_b1[SAT_MAX_SHARDS];
    uint64_t            endpoint_ns[SAT_MAX_SHARDS];
    uint64_t            dlv_objs[SAT_MAX_SHARDS];    /* lifetime, cumulative   */
    uint64_t            dlv_bytes[SAT_MAX_SHARDS];   /* lifetime, cumulative   */
    uint64_t            dlv_objs_b0[SAT_MAX_SHARDS]; /* delivery at B0         */
    uint64_t            dlv_objs_b1[SAT_MAX_SHARDS]; /* delivery at B1         */
    uint64_t            dlv_bytes_b0[SAT_MAX_SHARDS];
    uint64_t            dlv_bytes_b1[SAT_MAX_SHARDS];
    uint64_t            prog_b0;    /* owner all-dest progress at B0           */
    uint64_t            prog_b1;    /* owner all-dest progress at B1           */
    int                 lane_err[SAT_MAX_SHARDS];

    uint64_t owner_t0;
    uint64_t owner_t1;
    uint64_t produced;   /* owner: total source objects ingested             */

    /* Open-loop (knee) campaign inputs + generator-validity outputs (owner
     * lane only). inject_period_ns == 0 when campaign == ceiling. */
    sat_campaign_t campaign;
    uint64_t       inject_period_ns;   /* 1e9 / lambda                         */
    uint64_t       duration_ns;        /* fixed timed-window length T          */
    uint64_t       gen_scheduled;      /* deadlines that fell inside [t0,t0+T) */
    uint64_t       gen_attempted;      /* injections we tried                  */
    uint64_t       gen_accepted;       /* injections the source log accepted   */
    uint64_t       gen_missed;         /* injections >1 period late            */
    uint64_t       gen_max_lateness;   /* worst injection lateness (ns)        */

    int         base_cpu;       /* first CPU for pinning; -1 = do not pin     */
    _Atomic int pin_fail;
    _Atomic int abort_run;       /* a spawn failure -> everyone bails, no hang */
    _Atomic int backlog_emptied; /* window ran the owner dry (starved != sat.) */
    uint16_t    lanes_started;   /* how many lane threads were created         */
} sat_shared_t;

typedef struct sat_lane {
    sat_shared_t *sh;
    uint16_t      shard;
} sat_lane_t;

/* Objects completed to EVERY remote destination in a window, from each
 * destination's OWN lane-sampled core ingested_total delta (b1 - b0) — the
 * signed protocol's `min_d(core_ingest_b1[d] - core_ingest_b0[d])`. Pure over
 * the published snapshot arrays so it is unit-testable: a destination that
 * finishes one more object in its final step (after the owner already read its
 * live progress) records it in its own b1 snapshot, and this min captures the
 * coherent per-lane population — not the owner's earlier live read. */
static uint64_t
sat_min_core_delta(const uint64_t *b0, const uint64_t *b1,
                   const uint32_t *sub_count, uint16_t K)
{
    uint64_t m = UINT64_MAX;
    for (uint16_t d = 1; d < K; d++) {
        if (sub_count[d] > 0) {
            uint64_t delta = b1[d] - b0[d];
            if (delta < m) {
                m = delta;
            }
        }
    }
    return (m == UINT64_MAX) ? 0u : m;
}

/* Distinct source objects completed to EVERY remote destination (owner-thread
 * only). When there is no remote destination (K==1 or all-local gauntlet), the
 * degenerate progress is distinct objects delivered to every local subscriber
 * on the owner shard (delivered_total counts one per (sub, object)). */
static uint64_t
sat_owner_progress(sat_shared_t *sh)
{
    if (sh->r->topo.remote_demands > 0) {
        uint64_t m = UINT64_MAX;
        for (uint16_t d = 1; d < sh->K; d++) {
            if (sh->r->sb[d].count > 0) {
                uint64_t v = atomic_load(&sh->completed[d]);
                if (v < m) {
                    m = v;
                }
            }
        }
        return m;
    }
    moqr_core_stats_t cs;
    moqr_core_get_stats(moqr_shards_core(sh->r->s, 0), &cs);
    uint32_t ls = sh->r->topo.local_subs;
    return ls ? cs.delivered_total / ls : cs.ingested_total;
}

/* Lane self-snapshot at a barrier (own shard + own core: single-writer safe). */
static void
sat_snap(sat_shared_t *sh, uint16_t d, moqr_shards_stats_t *st, uint64_t *ing)
{
    memset(st, 0, sizeof(*st));
    if (moqr_shards_get_stats(sh->r->s, d, st) != MOQR_OK) {
        sh->lane_err[d]++;
    }
    moqr_core_stats_t cs;
    moqr_core_get_stats(moqr_shards_core(sh->r->s, d), &cs);
    *ing = cs.ingested_total;
}

/* A destination lane: step its shard, drain its subscribers, publish its
 * completion, and snapshot at B0/B1. */
static void *
sat_dest_run(void *arg)
{
    sat_lane_t   *ln = arg;
    sat_shared_t *sh = ln->sh;
    uint16_t      d = ln->shard;
    uint64_t      now = 100000;
    bool          did_b0 = false;
    bool          did_b1 = false;

    if (sh->base_cpu >= 0 && !sat_pin_self(sh->base_cpu + d)) {
        atomic_fetch_add(&sh->pin_fail, 1);
    }
    for (;;) {
        if (atomic_load(&sh->abort_run)) {
            break;   /* a spawn failed: bail without waiting */
        }
        uint32_t ph = atomic_load(&sh->phase);
        if (ph >= PH_STOP && !did_b1) {
            /* Stop epoch: snapshot BEFORE the next step (stats + delivery). */
            sh->endpoint_ns[d] = sat_now_ns();
            sat_snap(sh, d, &sh->b1[d], &sh->core_ingest_b1[d]);
            sh->dlv_objs_b1[d] = sh->dlv_objs[d];
            sh->dlv_bytes_b1[d] = sh->dlv_bytes[d];
            did_b1 = true;
            break;
        }
        if (ph >= PH_WINDOW && !did_b0) {
            /* Two-phase B0 barrier so timing starts from a COHERENT baseline:
             * (1) all lanes rendezvous; (2) each destination snapshots B0 and
             * PARKS (stops stepping) until the owner has captured its own B0 +
             * prog_b0 + owner_t0 and released everyone. A destination must not
             * complete objects between prog_b0 and owner_t0. */
            atomic_fetch_add(&sh->at_b0, 1);
            while (atomic_load(&sh->at_b0) < (int)sh->lanes_started &&
                   !atomic_load(&sh->abort_run)) {
                sched_yield();
            }
            sat_snap(sh, d, &sh->b0[d], &sh->core_ingest_b0[d]);
            sh->dlv_objs_b0[d] = sh->dlv_objs[d];
            sh->dlv_bytes_b0[d] = sh->dlv_bytes[d];
            atomic_fetch_add(&sh->b0_ready, 1);   /* parked; owner may sample */
            while (!atomic_load(&sh->b0_release) &&
                   !atomic_load(&sh->abort_run)) {
                sched_yield();
            }
            did_b0 = true;
        }
        /* Load-bearing barrier proof: a destination must NEVER step between its
         * B0 snapshot and the owner's release (else it could complete an object
         * inside the [prog_b0, owner_t0] gap). Reaching a step post-B0 with
         * b0_release still false is a coherence violation. */
        if (did_b0 && !atomic_load(&sh->b0_release) &&
            !atomic_load(&sh->abort_run)) {
            atomic_store(&sh->b0_violation, 1);
        }
        uint64_t mask = 0;
        if (moqr_shards_step_shard(sh->r->s, d, now, &mask) != MOQR_OK) {
            sh->lane_err[d]++;
        }
        if ((mask & ~sat_legal_wake(sh->K)) != 0) {
            sh->lane_err[d]++;
        }
        uint64_t objs = 0, bytes = 0;
        sat_pull(sh->r, d, now, &objs, &bytes, &sh->lane_err[d]);
        sh->dlv_objs[d] += objs;
        sh->dlv_bytes[d] += bytes;
        moqr_core_stats_t cs;
        moqr_core_get_stats(moqr_shards_core(sh->r->s, d), &cs);
        atomic_store(&sh->completed[d], cs.ingested_total);
        now += 100;
    }
    return NULL;
}

/* Deadlines that fall inside a duration-T open-loop window at the given period,
 * derived ONLY from (T, period) — never from what the injector managed to
 * attempt. Offsets 0, p, 2p, ... < T, so the count is ceil(T/p). Pure and
 * unit-tested so a loop that exits across the window edge cannot silently drop
 * its trailing scheduled deadlines. */
static uint64_t
sat_scheduled_deadlines(uint64_t duration_ns, uint64_t period)
{
    if (period == 0) {
        return 0;
    }
    /* ceil(duration/period) via quotient/remainder — the naive
     * (duration + period - 1) form overflows for a huge period. */
    return duration_ns / period + ((duration_ns % period) ? 1u : 0u);
}

/* The owner lane: maintain the in-flight window, drive the phase machine, and
 * snapshot at B0/B1. */
static void *
sat_owner_run(void *arg)
{
    sat_lane_t   *ln = arg;
    sat_shared_t *sh = ln->sh;
    sat_rig_t    *r = sh->r;
    uint64_t      now = 100000;
    uint64_t      produced = 0;
    uint64_t      base_completed = 0;

    if (sh->base_cpu >= 0 && !sat_pin_self(sh->base_cpu)) {
        atomic_fetch_add(&sh->pin_fail, 1);
    }

    /* Stall guard: a run that stops making progress is turned into an
     * invalidated point (never a hang). Budget is generous relative to any
     * healthy schedule (steps-per-object is O(1)). */
    const uint64_t step_budget = 20000ull + (uint64_t)sh->window * 4000ull;
    uint64_t       steps = 0;
    uint64_t       last_progress = 0;
    uint64_t       stuck = 0;

    /* -- warmup: maintain the window until `warmup` completions everywhere -- */
    while (sat_owner_progress(sh) < sh->warmup) {
        if (++steps > step_budget) {
            sh->lane_err[0]++;   /* stalled */
            break;
        }
        uint64_t minc = sat_owner_progress(sh);
        while (produced - minc < sh->cap &&
               produced < (uint64_t)sh->warmup + sh->cap) {
            if (!sat_pub_one(r, produced, now)) {
                sh->lane_err[0]++;
                break;
            }
            produced++;
        }
        uint64_t mask = 0;
        if (moqr_shards_step_shard(r->s, 0, now, &mask) != MOQR_OK) {
            sh->lane_err[0]++;
        }
        /* Any in-range bit is legal: peers via push/credit, and shard 0
         * itself via the one coalesced local continuation — which needs no
         * dispatch here, since this loop steps shard 0 again next pass. */
        if ((mask & ~sat_legal_wake(sh->K)) != 0) {
            sh->lane_err[0]++;
        }
        uint64_t lobjs = 0, lbytes = 0;
        sat_pull(r, 0, now, &lobjs, &lbytes, &sh->lane_err[0]);
        sh->dlv_objs[0] += lobjs;
        sh->dlv_bytes[0] += lbytes;
        now += 100;
    }

    /* -- B0: declare the window, rendezvous, then take a COHERENT baseline
     * while every destination is parked (two-phase barrier) -- */
    atomic_store(&sh->phase, PH_WINDOW);
    atomic_fetch_add(&sh->at_b0, 1);
    while (atomic_load(&sh->at_b0) < (int)sh->lanes_started &&
           !atomic_load(&sh->abort_run)) {
        sched_yield();
    }
    /* Wait until every destination has snapshotted B0 and parked, so completed[]
     * is frozen while the owner captures prog_b0 + owner_t0. */
    while (atomic_load(&sh->b0_ready) < (int)sh->lanes_started - 1 &&
           !atomic_load(&sh->abort_run)) {
        sched_yield();
    }
    sat_snap(sh, 0, &sh->b0[0], &sh->core_ingest_b0[0]);
    sh->dlv_objs_b0[0] = sh->dlv_objs[0];
    sh->dlv_bytes_b0[0] = sh->dlv_bytes[0];
    base_completed = sat_owner_progress(sh);
    sh->prog_b0 = base_completed;
    sh->owner_t0 = sat_now_ns();
    atomic_store(&sh->b0_release, 1);   /* release the destinations */

    if (sh->campaign == CAMPAIGN_KNEE) {
        /* -- open-loop: inject against ABSOLUTE CLOCK_MONOTONIC deadlines at the
         * requested rate for a fixed duration T, regardless of completion. B1 is
         * the fixed deadline t0+T (NOT a completion count) — waiting for
         * completion would relax mu to lambda and erase the knee. The owner both
         * injects and steps, so it records generator-validity: scheduled /
         * attempted / accepted / deadlines-missed / max-lateness; the actual
         * offered rate is computed from ACCEPTED, and any refused ingest or
         * over-tolerance / over-lateness point is invalidated downstream. */
        uint64_t t0 = sh->owner_t0;
        uint64_t period = sh->inject_period_ns ? sh->inject_period_ns : 1u;
        uint64_t win_end = t0 + sh->duration_ns;
        uint64_t next_deadline = t0;   /* first object due at window start */
        uint64_t nowns;
        /* The scheduled deadline count is fixed by (T, period), computed UP
         * FRONT — never derived from what the loop managed to attempt. Deadlines
         * fall at offsets 0, p, 2p, ... < T, so count = ceil(T/p). */
        uint64_t scheduled = sat_scheduled_deadlines(sh->duration_ns, period);
        uint64_t attempted = 0, accepted = 0, late_cnt = 0, maxlate = 0;
        /* The wall-clock deadline (win_end) bounds this loop; CLOCK_MONOTONIC
         * always advances, so no step-count stall guard is needed (and a
         * step-count cap would falsely trip on a long, fast T). */
        while ((nowns = sat_now_ns()) < win_end) {
            while (next_deadline < win_end && next_deadline <= nowns) {
                attempted++;
                uint64_t late = nowns - next_deadline;
                if (late > maxlate) {
                    maxlate = late;
                }
                if (late > period) {
                    late_cnt++;   /* fell more than one slot behind */
                }
                if (sat_pub_one(r, produced, now)) {
                    produced++;
                    accepted++;
                } else {
                    sh->lane_err[0]++;   /* refused ingest invalidates */
                }
                next_deadline += period;
            }
            uint64_t mask = 0;
            if (moqr_shards_step_shard(r->s, 0, now, &mask) != MOQR_OK) {
                sh->lane_err[0]++;
            }
            /* In-range only; the self continuation is performed by this
             * loop's next pass. */
            if ((mask & ~sat_legal_wake(sh->K)) != 0) {
                sh->lane_err[0]++;
            }
            uint64_t lobjs = 0, lbytes = 0;
            sat_pull(r, 0, now, &lobjs, &lbytes, &sh->lane_err[0]);
            sh->dlv_objs[0] += lobjs;
            sh->dlv_bytes[0] += lbytes;
            now += 100;
        }
        /* Trailing deadlines the loop never attempted (a step crossed win_end
         * before the next injection) are STILL scheduled misses — count them and
         * charge their window-end lateness, so a late-window deficit cannot pass
         * the rate tolerance as a "valid" irregular-load point. */
        uint64_t unattempted =
            (scheduled > attempted) ? (scheduled - attempted) : 0u;
        if (unattempted > 0 && win_end > next_deadline) {
            uint64_t tail_late = win_end - next_deadline;
            if (tail_late > maxlate) {
                maxlate = tail_late;
            }
        }
        sh->gen_scheduled = scheduled;
        sh->gen_attempted = attempted;
        sh->gen_accepted = accepted;
        sh->gen_missed = late_cnt + unattempted;
        sh->gen_max_lateness = maxlate;
    } else {
        /* -- closed-loop ceiling: maintain the CAP window until N more
         * completions; if the backlog ever empties the owner was starved. -- */
        last_progress = base_completed;
        while (sat_owner_progress(sh) - base_completed < sh->window) {
            uint64_t prog = sat_owner_progress(sh);
            if (prog > last_progress) {
                last_progress = prog;
                stuck = 0;
            } else if (++stuck > step_budget) {
                sh->lane_err[0]++;   /* stalled mid-window */
                break;
            }
            uint64_t minc = prog;
            if (produced <= minc) {
                atomic_store(&sh->backlog_emptied, 1);   /* owner ran dry */
            }
            while (produced - minc < sh->cap) {
                if (!sat_pub_one(r, produced, now)) {
                    sh->lane_err[0]++;
                    break;
                }
                produced++;
            }
            uint64_t mask = 0;
            if (moqr_shards_step_shard(r->s, 0, now, &mask) != MOQR_OK) {
                sh->lane_err[0]++;
            }
            /* In-range only; the self continuation is performed by this
             * loop's next pass. */
            if ((mask & ~sat_legal_wake(sh->K)) != 0) {
                sh->lane_err[0]++;
            }
            uint64_t lobjs = 0, lbytes = 0;
            sat_pull(r, 0, now, &lobjs, &lbytes, &sh->lane_err[0]);
            sh->dlv_objs[0] += lobjs;
            sh->dlv_bytes[0] += lbytes;
            now += 100;
        }
    }

    /* -- B1 stop epoch: stop feeding, snapshot BEFORE next step -- */
    sh->owner_t1 = sat_now_ns();
    sh->endpoint_ns[0] = sh->owner_t1;
    sat_snap(sh, 0, &sh->b1[0], &sh->core_ingest_b1[0]);
    sh->dlv_objs_b1[0] = sh->dlv_objs[0];
    sh->dlv_bytes_b1[0] = sh->dlv_bytes[0];
    sh->prog_b1 = sat_owner_progress(sh);
    sh->produced = produced;
    atomic_store(&sh->phase, PH_STOP);
    return NULL;
}

/* ------------------------------------------------ measured-run result */

typedef struct sat_result {
    bool     valid;
    char     invalid_reason[96];
    bool     pinned;

    uint16_t shards;
    uint32_t local_subs, remote_subs;
    uint16_t remote_demands;
    uint32_t fanout, payload;
    uint32_t cap;   /* the resolved in-flight window (a workload dimension —
                     * rows at different CAPs must never pool into one median) */
    const char *shape;
    const char *dist;

    const char *campaign;   /* "ceiling" (closed-loop) | "knee" (open-loop)     */
    bool     is_knee;

    uint64_t objects_completed, bytes_completed, objects_produced;
    uint64_t window_deliveries;   /* sub-deliveries counted IN [B0,B1]         */
    uint64_t elapsed_ns;
    double   objs_per_sec, bytes_per_sec, ns_per_object;

    /* Open-loop (knee) generator-validity outputs — populated only on the knee
     * campaign; null on ceiling rows. */
    double   requested_lambda, actual_offered_rate;
    uint64_t objects_scheduled, objects_attempted, objects_accepted;
    uint64_t injection_deadlines_missed, max_injection_lateness_ns;

    uint64_t b_star_msgs_per_sec;
    double   mu_cap;
    int64_t  steady_state_residual;

    uint64_t pump_turns, pump_messages, pump_bytes;
    double   messages_per_turn, bytes_per_turn;
    uint64_t wake_push, wake_credit, wake_local;
    uint32_t chan_entries_hwm;
    uint64_t chan_bytes_hwm;

    uint64_t cross_shard_messages;
    uint64_t payload_bearing_messages;
    int64_t  cross_shard_copies;   /* -1 == null / not provably attributed    */
    int64_t  property_copies;      /* -1 == null (no properties measured here)      */
    double   clones_per_object;

    uint64_t allocs, frees;
    uint64_t peak_bytes, capacity_ceiling;
    bool     capacity_described;   /* the closed-form ceiling model resolved   */
    bool     backlog_emptied;      /* window starved (owner not saturated)     */
    bool     ceiling_valid;        /* a saturated ceiling point (not starved)  */

    uint64_t endpoint_skew_ns;
    double   skew_fraction;

    /* Open-loop (knee) fields — NOT measured on the closed-loop path; emitted
     * as null / not-applicable so the schema is stable across campaigns. */
    double   lateness_fraction;    /* configured threshold echo               */

    uint64_t remote_data_rejected, term_capacity, term_overrun;
    char     affinity[160];   /* lane->CPU map, or "unpinned"                 */
} sat_result_t;

/* --------------------------------------------- pure lifetime validator */

/* Every raw measurement the validity decision consumes, gathered at the
 * quiesced post-join/post-teardown fixed point. Decoupling the DECISION from
 * how the numbers were gathered lets each gate be exercised directly (feed a
 * short destination, a missing terminal, a leftover slot) without depending on
 * the setup fixed-point check or the live stall guard. */
typedef struct sat_life {
    uint16_t    K;
    uint16_t    remote_demands;
    uint64_t    produced;
    sat_shape_t shape;
    uint32_t    payload, property, chunks, cap;

    uint32_t    sub_count[SAT_MAX_SHARDS];   /* subscribers on each shard      */
    uint64_t    ingested[SAT_MAX_SHARDS];    /* per-shard core ingested_total  */
    uint64_t    dlv_bytes[SAT_MAX_SHARDS];   /* per-shard delivered bytes      */
    uint64_t    core_b0[SAT_MAX_SHARDS];     /* lane-sampled core ingest at B0 */
    uint64_t    core_b1[SAT_MAX_SHARDS];     /* lane-sampled core ingest at B1 */
    uint64_t    window_completed;            /* production's objects_completed */

    uint64_t    kind[MOQR_SHARDS_MSG__COUNT];   /* lifetime per-kind totals    */
    uint64_t    owner_pump_bytes, owner_pump_messages;

    int64_t     cross_shard_copies, property_copies;   /* -1 == null           */

    uint64_t    timed_data, timed_control, timed_msgs, timed_bytes;
    uint64_t    timed_obj, timed_open, timed_chunk;
    int64_t     steady_state_residual;

    uint32_t    open_slots, open_objs, chan_pending, mbox, pump_subs, pend;
    uint64_t    rejected, term_capacity, term_overrun;

    long        final_live;
    uint64_t    peak_bytes, capacity_ceiling;
    bool        capacity_described;   /* the ceiling model resolved OK          */

    bool        lane_error, b0_violation, pin_failed;
    uint64_t    endpoint_skew_ns, elapsed_ns;
    double      skew_fraction;

    /* Open-loop (knee) generator-validity inputs (ignored on ceiling). */
    bool        is_knee;
    double      requested_lambda, actual_offered_rate, rate_tolerance;
    uint64_t    max_injection_lateness_ns, inject_period_ns;
    double      lateness_fraction;
    uint64_t    duration_ns;                        /* the fixed timed window */
    uint64_t    objects_scheduled, objects_attempted, deadlines_missed;
} sat_life_t;

/* Decide validity from the snapshot alone. Returns NULL when every exact
 * lifetime total / terminal / quiescence / algebraic gate holds, else the FIRST
 * failing reason (checked in a fixed order — a run is reported by its
 * highest-priority defect). All expectations are derived here from the raw
 * config + measurements, so the caller cannot pre-bake a passing verdict. */
static const char *
sat_decide(const sat_life_t *v)
{
    uint64_t rd = v->remote_demands;
    uint64_t prd = v->produced;
    uint64_t ppr = (v->shape == SHAPE_WHOLE) ? v->payload
                                             : (uint64_t)v->chunks * v->payload;
    uint64_t mbar = (v->shape == SHAPE_WHOLE) ? 1u : (v->chunks + 2u);

    uint64_t exp_obj, exp_open, exp_chunk, exp_end, exp_pump_bytes;
    if (v->shape == SHAPE_WHOLE) {
        exp_obj = rd * prd;
        exp_open = exp_chunk = exp_end = 0;
        exp_pump_bytes = rd * prd * ((uint64_t)v->payload + v->property);
    } else {
        exp_obj = 0;
        exp_open = rd * prd;
        exp_end = rd * prd;
        exp_chunk = rd * prd * v->chunks;
        exp_pump_bytes =
            rd * prd * ((uint64_t)v->chunks * v->payload + v->property);
    }
    uint64_t exp_copies =
        (v->shape == SHAPE_WHOLE) ? rd * prd : rd * prd * v->chunks;
    uint64_t exp_prop_copies = rd * prd;

    uint32_t tsubs = 0;
    uint64_t rmin = UINT64_MAX, rmax = 0;
    for (uint16_t d = 0; d < v->K; d++) {
        tsubs += v->sub_count[d];
        if (d > 0 && v->sub_count[d] > 0) {
            if (v->ingested[d] < rmin) {
                rmin = v->ingested[d];
            }
            if (v->ingested[d] > rmax) {
                rmax = v->ingested[d];
            }
        }
    }
    uint64_t life_delivered = 0;
    for (uint16_t d = 0; d < v->K; d++) {
        life_delivered += v->dlv_bytes[d];
    }

    const uint64_t *k = v->kind;
    bool ex_kinds = (k[MOQR_SHARDS_MSG_OBJ] == exp_obj &&
                     k[MOQR_SHARDS_MSG_OBJ_OPEN] == exp_open &&
                     k[MOQR_SHARDS_MSG_OBJ_CHUNK] == exp_chunk &&
                     k[MOQR_SHARDS_MSG_OBJ_END] == exp_end);
    bool ex_no_unexpected = (k[MOQR_SHARDS_MSG_OBJ_RESET] == 0 &&
                             k[MOQR_SHARDS_MSG_GRP_RESET] == 0 &&
                             k[MOQR_SHARDS_MSG_GRP_EVICT] == 0 &&
                             k[MOQR_SHARDS_MSG_SG_SEAL] == 0);
    uint64_t exp_timed_bytes =
        (v->shape == SHAPE_WHOLE)
            ? v->timed_obj * ((uint64_t)v->payload + v->property)
            : v->timed_open * (uint64_t)v->property +
                  v->timed_chunk * (uint64_t)v->payload;
    bool ex_timed_bytes = (v->timed_bytes == exp_timed_bytes);
    bool ex_pumpbytes = (v->owner_pump_bytes == exp_pump_bytes);
    bool ex_pumpmsgs =
        (v->owner_pump_messages == exp_obj + exp_open + exp_chunk + exp_end);
    bool ex_copies = (v->shape != SHAPE_WHOLE) ||
                     (v->cross_shard_copies >= 0 &&
                      (uint64_t)v->cross_shard_copies == exp_copies);
    bool ex_prop = (v->property == 0 || v->shape != SHAPE_WHOLE) ||
                   (v->property_copies >= 0 &&
                    (uint64_t)v->property_copies == exp_prop_copies);
    bool ex_delivered = (life_delivered == (uint64_t)tsubs * prd * ppr);
    bool ex_perdest = true;
    for (uint16_t d = 0; d < v->K; d++) {
        if (v->dlv_bytes[d] != (uint64_t)v->sub_count[d] * prd * ppr) {
            ex_perdest = false;
        }
    }
    bool ex_timed_ctrl0 = (v->timed_control == 0);
    bool ex_timed_msgs = (v->timed_data == v->timed_msgs);
    uint64_t resid_bound = rd * (uint64_t)v->cap * mbar;

    if (v->lane_error) {
        return "lane-error";
    }
    if (v->rejected != 0) {
        return "loss-remote-data-rejected";
    }
    if (v->term_capacity != 0 || v->term_overrun != 0) {
        return "fail-stop-terminal";
    }
    if (v->open_slots || v->open_objs || v->chan_pending || v->mbox ||
        v->pump_subs || v->pend) {
        return "not-quiesced";
    }
    if (k[MOQR_SHARDS_MSG_DEMAND] != rd) {
        return "demand-count";
    }
    if (k[MOQR_SHARDS_MSG_ACK] != rd) {
        return "ack-count";
    }
    if (k[MOQR_SHARDS_MSG_UNDEMAND] != rd) {
        return "undemand-count";
    }
    if (k[MOQR_SHARDS_MSG_DONE] != 0) {
        return "done-nonzero";
    }
    if (rd > 0 && (rmin != prd || rmax != prd)) {
        return "completion-mismatch";
    }
    if (!ex_kinds) {
        return "data-kind-total";
    }
    if (!ex_no_unexpected) {
        return "unexpected-kind";
    }
    if (!ex_pumpbytes) {
        return "pump-bytes-total";
    }
    if (!ex_pumpmsgs) {
        return "pump-msgs-sum";
    }
    if (!ex_copies) {
        return "clone-count";
    }
    if (!ex_prop) {
        return "property-copy-count";
    }
    if (!ex_delivered) {
        return "delivered-bytes-total";
    }
    if (!ex_perdest) {
        return "per-dest-delivered-bytes";
    }
    if (!ex_timed_ctrl0) {
        return "timed-control-kind";
    }
    if (!ex_timed_msgs) {
        return "timed-msgs-mismatch";
    }
    if (!ex_timed_bytes) {
        return "timed-bytes-mismatch";
    }
    /* The steady-state residual bound is a CLOSED-LOOP consistency check (the
     * CAP window bounds the in-flight tail). Open-loop deliberately lets the
     * backlog build past CAP, so the residual is not so bounded — skip it. */
    if (!v->is_knee && (v->steady_state_residual > (int64_t)resid_bound ||
                        v->steady_state_residual < -(int64_t)resid_bound)) {
        return "residual-out-of-bound";
    }
    /* Window-population cross-check (both campaigns, remote topologies): the
     * reported window completion count MUST equal the minimum per-destination
     * core delta recomputed here from the lane-owned B0/B1 snapshots. A
     * producer that derived it from any other population (e.g. the owner's
     * earlier live progress read) fails this whenever the two diverge. */
    if (rd > 0 &&
        v->window_completed !=
            sat_min_core_delta(v->core_b0, v->core_b1, v->sub_count, v->K)) {
        return "window-population-mismatch";
    }
    /* Open-loop generator-validity: the offered rate the owner actually
     * SUSTAINED must track the requested rate (else generator saturation is
     * masquerading as relay saturation), and no injection may be late by more
     * than a stated fraction of the injection period. */
    if (v->is_knee) {
        /* The scheduled-deadline count is fixed by (T, period): a generator
         * that derived it from its attempts (erasing trailing misses) fails. */
        if (v->objects_scheduled !=
            sat_scheduled_deadlines(v->duration_ns, v->inject_period_ns)) {
            return "scheduled-count-mismatch";
        }
        /* Every unattempted deadline is a miss: missed can never undercount
         * the schedule/attempt deficit. */
        if (v->objects_scheduled >= v->objects_attempted &&
            v->deadlines_missed <
                v->objects_scheduled - v->objects_attempted) {
            return "missed-undercount";
        }
        double req = v->requested_lambda;
        double lo = req * (1.0 - v->rate_tolerance);
        double hi = req * (1.0 + v->rate_tolerance);
        if (req > 0.0 &&
            (v->actual_offered_rate < lo || v->actual_offered_rate > hi)) {
            return "offered-rate-out-of-tolerance";
        }
        if (v->inject_period_ns > 0 &&
            (double)v->max_injection_lateness_ns >
                v->lateness_fraction * (double)v->inject_period_ns) {
            return "injection-lateness";
        }
    }
    if (v->final_live != 0) {
        return "allocator-leak";
    }
    /* The closed-form capacity model MUST have resolved (else there is no
     * ceiling to check peak_live against — D4 requires the bound). */
    if (!v->capacity_described) {
        return "capacity-model-refused";
    }
    if (v->peak_bytes > v->capacity_ceiling) {
        return "over-capacity-ceiling";
    }
    if (v->elapsed_ns > 0 &&
        (double)v->endpoint_skew_ns > v->skew_fraction * (double)v->elapsed_ns) {
        return "endpoint-skew";
    }
    if (v->pin_failed) {
        return "affinity-failed";
    }
    if (rd > 0 && v->b0_violation) {
        return "b0-incoherent";
    }
    return NULL;
}

/* Run one closed-loop point. Fills `out`; returns true if the run COMPLETED
 * (validity is a field, not the return). */
static bool
sat_run_one(const sat_cfg_t *c, int base_cpu, sat_result_t *out)
{
    memset(out, 0, sizeof(*out));
    out->shape = (c->shape == SHAPE_WHOLE) ? "whole" : "chunked";
    out->dist = (c->dist == DIST_REMOTE_UNIFORM) ? "remote-uniform" : "gauntlet";
    out->fanout = c->fanout;
    out->payload = c->payload;
    out->shards = c->K;
    out->skew_fraction = c->skew_fraction;
    out->lateness_fraction = c->lateness_fraction;
    out->cross_shard_copies = -1;
    out->property_copies = -1;   /* properties not measured here */
    out->campaign = (c->campaign == CAMPAIGN_KNEE) ? "knee" : "ceiling";

    size_t watch = 0;
    (void)moq_rcbuf_allocation_size(c->payload, &watch);
    size_t watch2 = 0;
    if (c->property != 0) {
        (void)moq_rcbuf_allocation_size(c->property, &watch2);
    }
    sat_alloc_t a;
    sat_alloc_init(&a, watch, watch2);

    sat_rig_t *r = calloc(1, sizeof(*r));
    if (r == NULL) {
        snprintf(out->invalid_reason, sizeof(out->invalid_reason), "oom-rig");
        return false;
    }
    if (!sat_build(r, &a, c)) {
        snprintf(out->invalid_reason, sizeof(out->invalid_reason),
                 "build-failed");
        sat_destroy(r);
        free(r);
        return false;
    }
    out->local_subs = r->topo.local_subs;
    out->remote_subs = r->topo.remote_subs;
    out->remote_demands = r->topo.remote_demands;

    /* One-time source-create baseline for the clone oracle: allocations at the
     * watched size BEFORE any object is produced are structural (must be 0 for
     * a clean attribution — any nonzero means the size aliases structure). */
    long watch_pre = a.watch_allocs;
    long watch2_pre = a.watch_allocs2;

    sat_shared_t *sh = calloc(1, sizeof(*sh));
    if (sh == NULL) {
        snprintf(out->invalid_reason, sizeof(out->invalid_reason), "oom-sh");
        sat_destroy(r);
        free(r);
        return false;
    }
    sh->r = r;
    sh->K = c->K;
    sh->window = c->objects;
    sh->warmup = c->objects / 4 + 8;
    sh->cap = c->cap ? c->cap : 32u;   /* in-flight window (calibrated: --cap) */
    out->cap = sh->cap;
    sh->campaign = c->campaign;
    if (c->campaign == CAMPAIGN_KNEE) {
        sh->inject_period_ns =
            (c->lambda > 0.0) ? (uint64_t)(1e9 / c->lambda) : 0u;
        sh->duration_ns = (uint64_t)c->duration_ms * 1000000ull;
    }
    sh->base_cpu = base_cpu;
    atomic_store(&sh->phase, PH_WARMUP);

    /* Spawn one lane per shard, checking EVERY creation. A failed create sets
     * abort_run so no lane blocks forever at the B0 rendezvous; only started
     * lanes are joined, and the run is flagged invalid (never a hang). */
    pthread_t  tids[SAT_MAX_SHARDS];
    bool       started[SAT_MAX_SHARDS] = { false };
    sat_lane_t lanes[SAT_MAX_SHARDS];
    for (uint16_t d = 0; d < c->K; d++) {
        lanes[d].sh = sh;
        lanes[d].shard = d;
    }
    /* lanes_started is the rendezvous target; it MUST be fully written before
     * any thread starts (thread creation is the happens-before edge), so it is
     * set to the expected count K up front — never incremented concurrently. A
     * spawn failure instead raises abort_run, which releases every B0 spinner. */
    sh->lanes_started = c->K;
    bool spawn_failed = false;
    /* Destinations first, owner last, so the window machine has consumers. */
    for (uint16_t d = 1; d < c->K; d++) {
        if (pthread_create(&tids[d], NULL, sat_dest_run, &lanes[d]) == 0) {
            started[d] = true;
        } else {
            spawn_failed = true;
            atomic_store(&sh->abort_run, 1);
        }
    }
    if (!spawn_failed &&
        pthread_create(&tids[0], NULL, sat_owner_run, &lanes[0]) == 0) {
        started[0] = true;
    } else {
        spawn_failed = true;
        atomic_store(&sh->abort_run, 1);
    }
    if (spawn_failed) {
        sh->lane_err[0]++;   /* -> invalid, never a hang */
    }
    for (uint16_t d = 0; d < c->K; d++) {
        if (started[d]) {
            pthread_join(tids[d], NULL);
        }
    }

    /* -- post-join, single-threaded: drain to quiescence AND full delivery
     * (bounded), tallying the drain-tail deliveries into the LIFETIME
     * accumulators so the delivered-byte total covers the whole run -- */
    uint64_t       now = 5000000;
    uint32_t       total_subs = r->topo.local_subs + r->topo.remote_subs;
    uint64_t       want_deliveries = (uint64_t)total_subs * sh->produced;
    const uint64_t drain_budget =
        20000ull + (uint64_t)sh->produced * (uint64_t)c->K * 32ull;
    for (uint64_t i = 0; i < drain_budget; i++) {
        (void)sat_step_all(r, now);
        uint64_t pulled = 0;
        for (uint16_t d = 0; d < c->K; d++) {
            uint64_t objs = 0, bytes = 0;
            sat_pull(r, d, now, &objs, &bytes, &sh->lane_err[0]);
            sh->dlv_objs[d] += objs;
            sh->dlv_bytes[d] += bytes;
            pulled += sh->dlv_objs[d];   /* records this bench actually pulled */
        }
        now += 1000;
        /* Drain until the cross-shard plane is empty AND every subscriber has
         * pulled every produced object (the byte tally is exactly this count
         * times the per-object bytes). */
        if (sat_inflight(r) == 0 && pulled >= want_deliveries) {
            break;
        }
    }

    /* -- ALL timed fields share ONE window [B0, B1] -- */
    out->elapsed_ns = sh->owner_t1 - sh->owner_t0;
    out->objects_produced = sh->produced;
    /* Open-loop generator-validity outputs (x-axis is the ACTUAL sustained
     * offered rate from ACCEPTED objects, never the requested lambda). */
    out->is_knee = (c->campaign == CAMPAIGN_KNEE);
    if (out->is_knee) {
        out->requested_lambda = c->lambda;
        out->objects_scheduled = sh->gen_scheduled;
        out->objects_attempted = sh->gen_attempted;
        out->objects_accepted = sh->gen_accepted;
        out->injection_deadlines_missed = sh->gen_missed;
        out->max_injection_lateness_ns = sh->gen_max_lateness;
        double sec = (double)out->elapsed_ns / 1e9;
        out->actual_offered_rate =
            (sec > 0.0) ? (double)sh->gen_accepted / sec : 0.0;
    }
    /* Objects completed to every remote destination IN the window: the minimum
     * per-destination core ingested_total delta across the lane-owned B0/B1
     * snapshots (protocol population — coherent past the owner's earlier live
     * read). All-local (no remote demand) falls back to the owner progress. */
    if (r->topo.remote_demands > 0) {
        uint32_t subc[SAT_MAX_SHARDS];
        for (uint16_t d = 0; d < c->K; d++) {
            subc[d] = r->sb[d].count;
        }
        out->objects_completed = sat_min_core_delta(
            sh->core_ingest_b0, sh->core_ingest_b1, subc, c->K);
    } else {
        out->objects_completed = sh->prog_b1 - sh->prog_b0;
    }
    /* Delivered bytes + deliveries IN the window: the B1-B0 delta across all
     * subs (owner-local + every destination). All from the SAME [B0,B1]. */
    for (uint16_t d = 0; d < c->K; d++) {
        out->bytes_completed += sh->dlv_bytes_b1[d] - sh->dlv_bytes_b0[d];
        out->window_deliveries += sh->dlv_objs_b1[d] - sh->dlv_objs_b0[d];
    }

    /* Owner pump cadence is owner-only (shard 0 produces all cross-shard
     * work). */
    uint64_t d_msgs = sh->b1[0].pump_messages - sh->b0[0].pump_messages;
    uint64_t d_turns = sh->b1[0].pump_turns - sh->b0[0].pump_turns;
    uint64_t d_bytes = sh->b1[0].pump_bytes - sh->b0[0].pump_bytes;
    out->pump_messages = d_msgs;
    out->pump_turns = d_turns;
    out->pump_bytes = d_bytes;
    out->messages_per_turn = d_turns ? (double)d_msgs / (double)d_turns : 0.0;
    out->bytes_per_turn = d_turns ? (double)d_bytes / (double)d_turns : 0.0;
    out->wake_push = sh->b1[0].wake_requests_push - sh->b0[0].wake_requests_push;
    /* Local-continuation wakes are raised by whichever lane applied inbound
     * data (or budget-bounded a delivery pass): aggregate over EVERY lane. */
    out->wake_local = 0;
    for (uint16_t d = 0; d < c->K; d++) {
        out->wake_local +=
            sh->b1[d].wake_requests_local - sh->b0[d].wake_requests_local;
    }
    /* The owner->d DATA channels have their inbound HWM in the DESTINATION
     * snapshots, and the producer-credit wakes toward the owner are raised BY
     * the destinations (their pop frees a 0->d slot). Aggregate over d>0. */
    out->wake_credit = 0;
    out->chan_entries_hwm = 0;
    out->chan_bytes_hwm = 0;
    for (uint16_t d = 1; d < c->K; d++) {
        out->wake_credit +=
            sh->b1[d].wake_requests_credit - sh->b0[d].wake_requests_credit;
        if (sh->b1[d].channel_entries_hwm > out->chan_entries_hwm) {
            out->chan_entries_hwm = sh->b1[d].channel_entries_hwm;
        }
        if (sh->b1[d].channel_bytes_hwm > out->chan_bytes_hwm) {
            out->chan_bytes_hwm = sh->b1[d].channel_bytes_hwm;
        }
    }

    if (out->elapsed_ns > 0 && out->objects_completed > 0) {
        double sec = (double)out->elapsed_ns / 1e9;
        out->mu_cap = (double)out->objects_completed / sec;
        out->b_star_msgs_per_sec = (uint64_t)((double)d_msgs / sec);
        out->objs_per_sec = out->mu_cap;
        out->bytes_per_sec = (double)out->bytes_completed / sec;
        out->ns_per_object =
            (double)out->elapsed_ns / (double)out->objects_completed;
    }
    /* Steady-state consistency residual for the timed window: |Δpump_messages
     * - N·(K-1)·m̄|. NOT an identity — up to CAP·(K-1)·m̄ of in-flight tail
     * straddles the window; the gate bounds it by exactly that. */
    uint64_t mbar = (c->shape == SHAPE_WHOLE) ? 1u : (c->chunks + 2u);
    int64_t model = (int64_t)out->objects_completed *
                    (int64_t)r->topo.remote_demands * (int64_t)mbar;
    out->steady_state_residual = (int64_t)d_msgs - model;

    /* endpoint skew across all lanes. */
    uint64_t mn = UINT64_MAX, mx = 0;
    for (uint16_t d = 0; d < c->K; d++) {
        uint64_t e = sh->endpoint_ns[d];
        if (e < mn) {
            mn = e;
        }
        if (e > mx) {
            mx = e;
        }
    }
    out->endpoint_skew_ns = mx - mn;

    /* -- lifetime totals after drain (exact; order-independent) -- */
    /* Re-read every shard's final cross-shard stats and cores. Every produced
     * object must have crossed to EVERY remote destination (no loss), so each
     * remote destination's ingested_total equals produced. */
    uint64_t rmin = UINT64_MAX, rmax = 0;
    uint64_t rejected = 0, tcap = 0, tover = 0;
    uint32_t open_slots = 0, open_objs = 0, chan_pending = 0, mbox = 0,
             pump_subs = 0, pend = 0;
    uint64_t ingested[SAT_MAX_SHARDS] = { 0 };
    for (uint16_t d = 0; d < c->K; d++) {
        moqr_core_stats_t cs;
        moqr_core_get_stats(moqr_shards_core(r->s, d), &cs);
        ingested[d] = cs.ingested_total;
        if (d > 0 && r->sb[d].count > 0) {
            if (cs.ingested_total < rmin) {
                rmin = cs.ingested_total;
            }
            if (cs.ingested_total > rmax) {
                rmax = cs.ingested_total;
            }
        }
        rejected += moqr_shards_debug_remote_data_rejected(r->s, d);
        tcap += moqr_shards_debug_remote_demand_term_capacity(r->s, d);
        tover += moqr_shards_debug_remote_demand_term_overrun(r->s, d);
        open_slots += moqr_shards_debug_owner_progress_slots(r->s, d);
        open_objs += moqr_shards_debug_requester_open_objects(r->s, d);
        pump_subs += moqr_shards_debug_owner_pump_subs(r->s, d);
        pend += moqr_shards_debug_pending_demand(r->s, d);
        for (uint16_t e = 0; e < c->K; e++) {
            chan_pending += moqr_shards_debug_demand_channel_pending(r->s, d, e);
            mbox += moqr_shards_debug_mailbox_pending(r->s, d, e);
        }
    }
    out->remote_data_rejected = rejected;
    out->term_capacity = tcap;
    out->term_overrun = tover;

    moqr_shards_capacity_t cap;
    out->capacity_described =
        (moqr_shards_capacity_describe(&r->shcfg, &cap) == MOQR_OK);
    if (out->capacity_described) {
        out->capacity_ceiling = cap.relay_alloc_ceiling;
    }

    /* -- teardown: unsubscribe all, drive to full quiescence -- */
    /* (Subs are retired by binding close; step to process UNDEMAND -> owner
     * retires the pump-sub, no DONE for a clean unsubscribe.) */
    for (uint16_t d = 0; d < c->K; d++) {
        if (r->sb[d].count > 0) {
            (void)moqr_core_binding_close(moqr_shards_core(r->s, d),
                                          r->sb[d].bind, now);
        }
    }
    for (uint64_t i = 0; i < drain_budget; i++) {
        (void)sat_step_all(r, now);
        now += 1000;
        if (sat_quiesced(r) == 0) {
            break;
        }
    }

    /* Re-read quiescence after teardown. */
    open_slots = open_objs = chan_pending = mbox = pump_subs = pend = 0;
    uint64_t undemand_final = 0, demand_final = 0, ack_final = 0, done_final = 0;
    for (uint16_t d = 0; d < c->K; d++) {
        open_slots += moqr_shards_debug_owner_progress_slots(r->s, d);
        open_objs += moqr_shards_debug_requester_open_objects(r->s, d);
        pump_subs += moqr_shards_debug_owner_pump_subs(r->s, d);
        pend += moqr_shards_debug_pending_demand(r->s, d);
        for (uint16_t e = 0; e < c->K; e++) {
            chan_pending += moqr_shards_debug_demand_channel_pending(r->s, d, e);
            mbox += moqr_shards_debug_mailbox_pending(r->s, d, e);
        }
        moqr_shards_stats_t st;
        if (moqr_shards_get_stats(r->s, d, &st) == MOQR_OK) {
            demand_final += st.enqueued[MOQR_SHARDS_MSG_DEMAND];
            ack_final += st.enqueued[MOQR_SHARDS_MSG_ACK];
            undemand_final += st.enqueued[MOQR_SHARDS_MSG_UNDEMAND];
            done_final += st.enqueued[MOQR_SHARDS_MSG_DONE];
        }
    }

    /* -- lifetime per-kind message totals (final, post-quiescence; monotonic
     * cumulative counters, so this is the whole-run total, order-independent) -- */
    uint64_t k_obj = 0, k_open = 0, k_chunk = 0, k_end = 0;
    uint64_t k_reset = 0, k_grpreset = 0, k_grpevict = 0, k_seal = 0;
    for (uint16_t d = 0; d < c->K; d++) {
        moqr_shards_stats_t st;
        if (moqr_shards_get_stats(r->s, d, &st) == MOQR_OK) {
            k_obj += st.enqueued[MOQR_SHARDS_MSG_OBJ];
            k_open += st.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN];
            k_chunk += st.enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK];
            k_end += st.enqueued[MOQR_SHARDS_MSG_OBJ_END];
            k_reset += st.enqueued[MOQR_SHARDS_MSG_OBJ_RESET];
            k_grpreset += st.enqueued[MOQR_SHARDS_MSG_GRP_RESET];
            k_grpevict += st.enqueued[MOQR_SHARDS_MSG_GRP_EVICT];
            k_seal += st.enqueued[MOQR_SHARDS_MSG_SG_SEAL];
        }
    }
    out->payload_bearing_messages = k_obj + k_chunk;
    /* Diagnostic total across ALL kinds (a row with an unexpected terminal is
     * invalid, but its message total must not understate what crossed). */
    out->cross_shard_messages = k_obj + k_open + k_chunk + k_end + k_reset +
                                k_grpreset + k_grpevict + k_seal + demand_final +
                                ack_final + undemand_final + done_final;

    /* Clone attribution (post-quiescence, WHOLE only): cumulative allocs at the
     * watched payload size, minus the exactly-known source creates (one rcbuf
     * per produced object). Provably valid only if no structural allocation
     * aliased the size (watch_pre == 0). The chunked shape allocates one rcbuf
     * per chunk and its live-edge staging touches the same size class under a
     * schedule-dependent pattern, so per-run chunk-clone attribution is NOT
     * exact — chunked emits cross_shard_copies=null and is proven at the
     * boundary instead (the exact OBJ_CHUNK count and pump_bytes below). */
    long     watch_total = a.watch_allocs - watch_pre;
    uint64_t payload_records = sh->produced;   /* whole: one payload/object */
    int64_t  clones = (int64_t)watch_total - (int64_t)payload_records;
    if (c->shape == SHAPE_WHOLE && watch_pre == 0 && clones >= 0 &&
        payload_records > 0) {
        out->cross_shard_copies = clones;
        out->clones_per_object = (double)clones / (double)payload_records;
    } else {
        out->cross_shard_copies = -1;   /* null (chunked, or aliased) */
    }

    /* Property-clone attribution (a SEPARATE bucket at the property allocation
     * size). The source creates one property rcbuf per object; the owner clones
     * it once per remote destination — on the OBJ message (whole) or the
     * OBJ_OPEN message (chunked). For WHOLE the property clone rides the same
     * push as the payload clone, so its attribution is exactly as schedule-clean
     * as the payload oracle. For CHUNKED the OBJ_OPEN clone is subject to the
     * same WOULD_BLOCK re-derivation as the chunk payloads, so it emits null and
     * is proven at the boundary instead (the exact OBJ_OPEN count + pump_bytes).
     * Valid only when the property size is a live, non-aliasing bucket
     * (watch_size2 != 0 after the collision guard, and no structural allocation
     * hit it before production: watch2_pre == 0). */
    long    watch2_total = a.watch_allocs2 - watch2_pre;
    int64_t prop_clones = (int64_t)watch2_total - (int64_t)payload_records;
    if (c->shape == SHAPE_WHOLE && c->property != 0 && a.watch_size2 != 0 &&
        watch2_pre == 0 && prop_clones >= 0 && payload_records > 0) {
        out->property_copies = prop_clones;
    } else {
        out->property_copies = -1;   /* null (no properties, chunked, or aliased) */
    }

    /* Owner lifetime pump totals (only shard 0 produces cross-shard data). The
     * exact per-kind / byte / clone / delivered expectations are derived inside
     * the pure validator (sat_decide) from the raw counts assembled below. */
    moqr_shards_stats_t ost;
    memset(&ost, 0, sizeof(ost));
    (void)moqr_shards_get_stats(r->s, 0, &ost);
    /* Timed-window deltas (all shards): the algebraic-only checks. */
    uint64_t timed_data = 0, timed_control = 0;
    for (uint16_t d = 0; d < c->K; d++) {
        timed_data += (sh->b1[d].enqueued[MOQR_SHARDS_MSG_OBJ] -
                       sh->b0[d].enqueued[MOQR_SHARDS_MSG_OBJ]) +
                      (sh->b1[d].enqueued[MOQR_SHARDS_MSG_OBJ_OPEN] -
                       sh->b0[d].enqueued[MOQR_SHARDS_MSG_OBJ_OPEN]) +
                      (sh->b1[d].enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK] -
                       sh->b0[d].enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK]) +
                      (sh->b1[d].enqueued[MOQR_SHARDS_MSG_OBJ_END] -
                       sh->b0[d].enqueued[MOQR_SHARDS_MSG_OBJ_END]);
        timed_control += (sh->b1[d].enqueued[MOQR_SHARDS_MSG_DEMAND] -
                          sh->b0[d].enqueued[MOQR_SHARDS_MSG_DEMAND]) +
                         (sh->b1[d].enqueued[MOQR_SHARDS_MSG_ACK] -
                          sh->b0[d].enqueued[MOQR_SHARDS_MSG_ACK]) +
                         (sh->b1[d].enqueued[MOQR_SHARDS_MSG_UNDEMAND] -
                          sh->b0[d].enqueued[MOQR_SHARDS_MSG_UNDEMAND]) +
                         (sh->b1[d].enqueued[MOQR_SHARDS_MSG_DONE] -
                          sh->b0[d].enqueued[MOQR_SHARDS_MSG_DONE]);
    }
    /* Owner-shard timed per-kind counts for the byte algebra: only shard 0
     * produces cross-shard data, so pump_bytes and these counts are owner-only.
     * The timed window's byte total is NOT arbitrary even though its per-kind
     * composition is schedule-dependent — each message carries a fixed byte
     * cost, so Δpump_bytes must equal the actual timed message mix (a whole OBJ
     * = payload+property; a chunked OBJ_OPEN = property; a chunk = payload). */
    uint64_t t_obj = sh->b1[0].enqueued[MOQR_SHARDS_MSG_OBJ] -
                     sh->b0[0].enqueued[MOQR_SHARDS_MSG_OBJ];
    uint64_t t_open = sh->b1[0].enqueued[MOQR_SHARDS_MSG_OBJ_OPEN] -
                      sh->b0[0].enqueued[MOQR_SHARDS_MSG_OBJ_OPEN];
    uint64_t t_chunk = sh->b1[0].enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK] -
                       sh->b0[0].enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK];

    out->backlog_emptied = atomic_load(&sh->backlog_emptied) != 0;
    /* A closed-loop CEILING point is meaningful only when the owner stayed
     * saturated. If the backlog emptied, the owner was starved (the signed plan:
     * "invalid, not a lower ceiling"), so the ceiling-rate fields are emitted as
     * null — B*(K) is not measured here. The correctness gates still apply. */
    out->ceiling_valid = !out->backlog_emptied;

    /* Destroy the runtime BEFORE the allocator-balance check: while the runtime
     * lives its allocations are legitimately outstanding, not a leak. */
    sat_destroy(r);
    long final_live = a.live;
    out->allocs = (uint64_t)a.allocs;
    out->frees = (uint64_t)a.frees;
    out->peak_bytes = (uint64_t)a.peak;

    /* -- assemble the lifetime snapshot and decide (pure validator) -- */
    sat_life_t life;
    memset(&life, 0, sizeof(life));
    life.K = c->K;
    life.remote_demands = r->topo.remote_demands;
    life.produced = sh->produced;
    life.shape = c->shape;
    life.payload = c->payload;
    life.property = c->property;
    life.chunks = c->chunks;
    life.cap = sh->cap;
    for (uint16_t d = 0; d < c->K; d++) {
        life.sub_count[d] = r->sb[d].count;
        life.ingested[d] = ingested[d];
        life.dlv_bytes[d] = sh->dlv_bytes[d];
        life.core_b0[d] = sh->core_ingest_b0[d];
        life.core_b1[d] = sh->core_ingest_b1[d];
        if (sh->lane_err[d] != 0) {
            life.lane_error = true;
        }
    }
    life.window_completed = out->objects_completed;
    life.kind[MOQR_SHARDS_MSG_OBJ] = k_obj;
    life.kind[MOQR_SHARDS_MSG_OBJ_OPEN] = k_open;
    life.kind[MOQR_SHARDS_MSG_OBJ_CHUNK] = k_chunk;
    life.kind[MOQR_SHARDS_MSG_OBJ_END] = k_end;
    life.kind[MOQR_SHARDS_MSG_OBJ_RESET] = k_reset;
    life.kind[MOQR_SHARDS_MSG_GRP_RESET] = k_grpreset;
    life.kind[MOQR_SHARDS_MSG_GRP_EVICT] = k_grpevict;
    life.kind[MOQR_SHARDS_MSG_SG_SEAL] = k_seal;
    life.kind[MOQR_SHARDS_MSG_DEMAND] = demand_final;
    life.kind[MOQR_SHARDS_MSG_ACK] = ack_final;
    life.kind[MOQR_SHARDS_MSG_UNDEMAND] = undemand_final;
    life.kind[MOQR_SHARDS_MSG_DONE] = done_final;
    life.owner_pump_bytes = ost.pump_bytes;
    life.owner_pump_messages = ost.pump_messages;
    life.cross_shard_copies = out->cross_shard_copies;
    life.property_copies = out->property_copies;
    life.timed_data = timed_data;
    life.timed_control = timed_control;
    life.timed_msgs = d_msgs;
    life.timed_bytes = d_bytes;
    life.timed_obj = t_obj;
    life.timed_open = t_open;
    life.timed_chunk = t_chunk;
    life.steady_state_residual = out->steady_state_residual;
    life.open_slots = open_slots;
    life.open_objs = open_objs;
    life.chan_pending = chan_pending;
    life.mbox = mbox;
    life.pump_subs = pump_subs;
    life.pend = pend;
    life.rejected = rejected;
    life.term_capacity = tcap;
    life.term_overrun = tover;
    life.final_live = final_live;
    life.peak_bytes = out->peak_bytes;
    life.capacity_ceiling = out->capacity_ceiling;
    life.capacity_described = out->capacity_described;
    life.b0_violation = atomic_load(&sh->b0_violation) != 0;
    life.pin_failed = (base_cpu >= 0 && atomic_load(&sh->pin_fail) != 0);
    life.endpoint_skew_ns = out->endpoint_skew_ns;
    life.elapsed_ns = out->elapsed_ns;
    life.skew_fraction = c->skew_fraction;
    life.is_knee = out->is_knee;
    life.requested_lambda = out->requested_lambda;
    life.actual_offered_rate = out->actual_offered_rate;
    life.rate_tolerance = c->rate_tolerance;
    life.max_injection_lateness_ns = out->max_injection_lateness_ns;
    life.inject_period_ns = sh->inject_period_ns;
    life.lateness_fraction = c->lateness_fraction;
    life.duration_ns = sh->duration_ns;
    life.objects_scheduled = out->objects_scheduled;
    life.objects_attempted = out->objects_attempted;
    life.deadlines_missed = out->injection_deadlines_missed;

    const char *why = sat_decide(&life);

    out->pinned = (base_cpu >= 0 && atomic_load(&sh->pin_fail) == 0);
    /* Lane->CPU map: lane d is pinned to (owner_cpu + d). */
    if (out->pinned) {
        snprintf(out->affinity, sizeof(out->affinity),
                 "owner_cpu=%d;lane_stride=1;lanes=%u", base_cpu, c->K);
    } else {
        snprintf(out->affinity, sizeof(out->affinity), "unpinned");
    }
    out->valid = (why == NULL);
    if (why != NULL) {
        snprintf(out->invalid_reason, sizeof(out->invalid_reason), "%s", why);
    } else {
        snprintf(out->invalid_reason, sizeof(out->invalid_reason), "none");
    }

    free(r);
    free(sh);
    return true;
}

/* ------------------------------------------------------------- output */

/* Nullable integer -> "null" or the value (open-loop / not-yet-attributed
 * fields stay explicit in the closed-loop schema). */
static void
sat_nulli(char *buf, size_t n, int64_t v)
{
    if (v < 0) {
        snprintf(buf, n, "null");
    } else {
        snprintf(buf, n, "%lld", (long long)v);
    }
}

/* `present ? value : "null"` for the campaign-specific fields (open-loop
 * generator metrics are null on ceiling rows and numeric on knee rows). */
static void
sat_nullu(char *buf, size_t n, bool present, uint64_t v)
{
    if (present) {
        snprintf(buf, n, "%llu", (unsigned long long)v);
    } else {
        snprintf(buf, n, "null");
    }
}

static void
sat_nulld(char *buf, size_t n, bool present, double v)
{
    if (present) {
        snprintf(buf, n, "%.3f", v);
    } else {
        snprintf(buf, n, "null");
    }
}

/* The full stable schema. Open-loop (knee) fields are emitted as `null` in this
 * closed-loop (ceiling) path; they exist so the schema is campaign-stable. */
#define SAT_COLS                                                              \
    "measurement_basis,campaign,valid,invalidation_reason,pinned,"            \
    "cpu_affinity_map,shards,local_subs,remote_subs,remote_demands,fanout,"   \
    "object_shape,distribution,object_size,cap,objects_produced,objects_completed," \
    "bytes_completed,objects_scheduled,objects_attempted,objects_accepted,"   \
    "requested_lambda,actual_offered_rate,injection_deadlines_missed,"        \
    "max_injection_lateness_ns,elapsed_ns,wall_objects_per_sec,"              \
    "wall_bytes_per_sec,wall_ns_per_object,B_star_msgs_per_sec,mu_cap,"       \
    "steady_state_residual,backlog_emptied,pump_turns,pump_messages,"        \
    "pump_bytes,messages_per_turn,bytes_per_turn,wake_requests_push,"         \
    "wake_requests_credit,wake_requests_local,"                               \
    "channel_entries_hwm,channel_bytes_hwm,"                                  \
    "cross_shard_messages,payload_bearing_messages,cross_shard_copies,"       \
    "property_copies,clones_per_object,allocs,frees,peak_live_bytes,"         \
    "capacity_ceiling_bytes,endpoint_skew_ns,skew_fraction,lateness_fraction," \
    "remote_data_rejected,term_capacity,term_overrun"

static void
sat_emit_csv_header(void)
{
    printf(SAT_COLS "\n");
}

/* Serialize one CSV row into `buf` (no trailing newline) so the exact field
 * layout can be inspected by a test, not just written to stdout. Returns the
 * snprintf result: the number of characters the row WOULD occupy (excluding the
 * NUL), so the caller can detect truncation (>= n). */
static int
sat_format_csv(const sat_result_t *o, char *buf, size_t n)
{
    char copies[24], props[24], bstar[24], mucap[24];
    sat_nulli(copies, sizeof(copies), o->cross_shard_copies);
    sat_nulli(props, sizeof(props), o->property_copies);
    /* Ceiling-rate fields are null when the owner was not saturated. */
    if (o->ceiling_valid) {
        snprintf(bstar, sizeof(bstar), "%llu",
                 (unsigned long long)o->b_star_msgs_per_sec);
        snprintf(mucap, sizeof(mucap), "%.1f", o->mu_cap);
    } else {
        snprintf(bstar, sizeof(bstar), "null");
        snprintf(mucap, sizeof(mucap), "null");
    }
    /* Open-loop generator fields: numeric on knee rows, null on ceiling rows.
     * objects_attempted/accepted are always numeric (ceiling: == produced). */
    char sched[24], att[24], acc[24], reqlam[32], actrate[32], missed[24],
        maxlate[24];
    bool k = o->is_knee;
    sat_nullu(sched, sizeof(sched), k, o->objects_scheduled);
    sat_nullu(att, sizeof(att), true,
              k ? o->objects_attempted : o->objects_produced);
    sat_nullu(acc, sizeof(acc), true,
              k ? o->objects_accepted : o->objects_produced);
    sat_nulld(reqlam, sizeof(reqlam), k, o->requested_lambda);
    sat_nulld(actrate, sizeof(actrate), k, o->actual_offered_rate);
    sat_nullu(missed, sizeof(missed), k, o->injection_deadlines_missed);
    sat_nullu(maxlate, sizeof(maxlate), k, o->max_injection_lateness_ns);
    const char *aff = o->affinity;
    return snprintf(buf, n,
           "in_process_wall,%s,%d,%s,%d,%s,%u,%u,%u,%u,%u,%s,%s,%u,%u,"
           "%llu,%llu,%llu,%s,%s,%s,%s,%s,%s,%s,"
           "%llu,%.1f,%.1f,%.1f,%s,%s,%lld,%d,"
           "%llu,%llu,%llu,%.3f,%.3f,%llu,%llu,%llu,%u,%llu,"
           "%llu,%llu,%s,%s,%.3f,%llu,%llu,%llu,%llu,"
           "%llu,%.4f,%.4f,%llu,%llu,%llu",
           o->campaign, o->valid ? 1 : 0, o->invalid_reason, o->pinned ? 1 : 0,
           aff, o->shards, o->local_subs, o->remote_subs, o->remote_demands,
           o->fanout, o->shape, o->dist, o->payload, o->cap,
           (unsigned long long)o->objects_produced,
           (unsigned long long)o->objects_completed,
           (unsigned long long)o->bytes_completed,
           sched, att, acc, reqlam, actrate, missed, maxlate,
           (unsigned long long)o->elapsed_ns, o->objs_per_sec, o->bytes_per_sec,
           o->ns_per_object, bstar, mucap, (long long)o->steady_state_residual,
           o->backlog_emptied ? 1 : 0, (unsigned long long)o->pump_turns,
           (unsigned long long)o->pump_messages,
           (unsigned long long)o->pump_bytes, o->messages_per_turn,
           o->bytes_per_turn, (unsigned long long)o->wake_push,
           (unsigned long long)o->wake_credit,
           (unsigned long long)o->wake_local, o->chan_entries_hwm,
           (unsigned long long)o->chan_bytes_hwm,
           (unsigned long long)o->cross_shard_messages,
           (unsigned long long)o->payload_bearing_messages, copies, props,
           o->clones_per_object, (unsigned long long)o->allocs,
           (unsigned long long)o->frees, (unsigned long long)o->peak_bytes,
           (unsigned long long)o->capacity_ceiling,
           (unsigned long long)o->endpoint_skew_ns, o->skew_fraction,
           o->lateness_fraction, (unsigned long long)o->remote_data_rejected,
           (unsigned long long)o->term_capacity,
           (unsigned long long)o->term_overrun);
}

/* Serialize one JSON object into `buf` (no trailing newline). Returns the
 * snprintf result (would-be length) so the caller can detect truncation. */
static int
sat_format_json(const sat_result_t *o, char *buf, size_t n)
{
    char copies[24], props[24], bstar[24], mucap[24];
    sat_nulli(copies, sizeof(copies), o->cross_shard_copies);
    sat_nulli(props, sizeof(props), o->property_copies);
    if (o->ceiling_valid) {
        snprintf(bstar, sizeof(bstar), "%llu",
                 (unsigned long long)o->b_star_msgs_per_sec);
        snprintf(mucap, sizeof(mucap), "%.1f", o->mu_cap);
    } else {
        snprintf(bstar, sizeof(bstar), "null");
        snprintf(mucap, sizeof(mucap), "null");
    }
    char sched[24], att[24], acc[24], reqlam[32], actrate[32], missed[24],
        maxlate[24];
    bool k = o->is_knee;
    sat_nullu(sched, sizeof(sched), k, o->objects_scheduled);
    sat_nullu(att, sizeof(att), true,
              k ? o->objects_attempted : o->objects_produced);
    sat_nullu(acc, sizeof(acc), true,
              k ? o->objects_accepted : o->objects_produced);
    sat_nulld(reqlam, sizeof(reqlam), k, o->requested_lambda);
    sat_nulld(actrate, sizeof(actrate), k, o->actual_offered_rate);
    sat_nullu(missed, sizeof(missed), k, o->injection_deadlines_missed);
    sat_nullu(maxlate, sizeof(maxlate), k, o->max_injection_lateness_ns);
    const char *aff = o->affinity;
    return snprintf(buf, n,
           "{\"measurement_basis\":\"in_process_wall\",\"campaign\":\"%s\","
           "\"valid\":%s,\"invalidation_reason\":\"%s\",\"pinned\":%s,"
           "\"cpu_affinity_map\":\"%s\",\"shards\":%u,\"local_subs\":%u,"
           "\"remote_subs\":%u,\"remote_demands\":%u,\"fanout\":%u,"
           "\"object_shape\":\"%s\",\"distribution\":\"%s\",\"object_size\":%u,"
           "\"cap\":%u,"
           "\"objects_produced\":%llu,\"objects_completed\":%llu,"
           "\"bytes_completed\":%llu,\"objects_scheduled\":%s,"
           "\"objects_attempted\":%s,\"objects_accepted\":%s,"
           "\"requested_lambda\":%s,\"actual_offered_rate\":%s,"
           "\"injection_deadlines_missed\":%s,"
           "\"max_injection_lateness_ns\":%s,\"elapsed_ns\":%llu,"
           "\"wall_objects_per_sec\":%.1f,\"wall_bytes_per_sec\":%.1f,"
           "\"wall_ns_per_object\":%.1f,\"B_star_msgs_per_sec\":%s,"
           "\"mu_cap\":%s,"
           "\"steady_state_residual\":%lld,\"backlog_emptied\":%s,"
           "\"pump_turns\":%llu,\"pump_messages\":%llu,\"pump_bytes\":%llu,"
           "\"messages_per_turn\":%.3f,\"bytes_per_turn\":%.3f,"
           "\"wake_requests_push\":%llu,\"wake_requests_credit\":%llu,"
           "\"wake_requests_local\":%llu,"
           "\"channel_entries_hwm\":%u,"
           "\"channel_bytes_hwm\":%llu,\"cross_shard_messages\":%llu,"
           "\"payload_bearing_messages\":%llu,\"cross_shard_copies\":%s,"
           "\"property_copies\":%s,\"clones_per_object\":%.3f,\"allocs\":%llu,"
           "\"frees\":%llu,\"peak_live_bytes\":%llu,"
           "\"capacity_ceiling_bytes\":%llu,"
           "\"endpoint_skew_ns\":%llu,\"skew_fraction\":%.4f,"
           "\"lateness_fraction\":%.4f,\"remote_data_rejected\":%llu,"
           "\"term_capacity\":%llu,\"term_overrun\":%llu}",
           o->campaign, o->valid ? "true" : "false", o->invalid_reason,
           o->pinned ? "true" : "false", aff, o->shards, o->local_subs,
           o->remote_subs, o->remote_demands, o->fanout, o->shape, o->dist,
           o->payload, o->cap, (unsigned long long)o->objects_produced,
           (unsigned long long)o->objects_completed,
           (unsigned long long)o->bytes_completed,
           sched, att, acc, reqlam, actrate, missed, maxlate,
           (unsigned long long)o->elapsed_ns, o->objs_per_sec, o->bytes_per_sec,
           o->ns_per_object, bstar, mucap, (long long)o->steady_state_residual,
           o->backlog_emptied ? "true" : "false",
           (unsigned long long)o->pump_turns,
           (unsigned long long)o->pump_messages,
           (unsigned long long)o->pump_bytes, o->messages_per_turn,
           o->bytes_per_turn, (unsigned long long)o->wake_push,
           (unsigned long long)o->wake_credit,
           (unsigned long long)o->wake_local, o->chan_entries_hwm,
           (unsigned long long)o->chan_bytes_hwm,
           (unsigned long long)o->cross_shard_messages,
           (unsigned long long)o->payload_bearing_messages, copies, props,
           o->clones_per_object, (unsigned long long)o->allocs,
           (unsigned long long)o->frees, (unsigned long long)o->peak_bytes,
           (unsigned long long)o->capacity_ceiling,
           (unsigned long long)o->endpoint_skew_ns, o->skew_fraction,
           o->lateness_fraction, (unsigned long long)o->remote_data_rejected,
           (unsigned long long)o->term_capacity,
           (unsigned long long)o->term_overrun);
}

/* Returns false (without printing the row) if the row did not fit the buffer —
 * a truncated CSV/JSON line must never be published as if it were complete. */
static bool
sat_emit(const sat_cfg_t *c, const sat_result_t *o, bool header)
{
    char row[2560];
    int  len;
    if (c->fmt == FMT_CSV) {
        if (header) {
            sat_emit_csv_header();
        }
        len = sat_format_csv(o, row, sizeof(row));
    } else {
        len = sat_format_json(o, row, sizeof(row));
    }
    if (len < 0 || (size_t)len >= sizeof(row)) {
        fprintf(stderr, "row serialization truncated (need %d, have %zu)\n", len,
                sizeof(row));
        return false;
    }
    printf("%s\n", row);
    return true;
}

/* --------------------------------------------------- retention preflight */

/* Pure over cfg: returns NULL if the whole retained set (warmup + window +
 * in-flight cap, all in one group during a valid run) fits EVERY resolved
 * source-log ceiling, else a short reason. Checks the configured one-group
 * record cap AND the resolver's journal record ceiling AND the byte budget AND
 * (chunked) the chunk-node pool — all in checked uint64, so a huge --cap
 * refuses here instead of wrapping into a long stall. */
static const char *
sat_retention_reason(const sat_cfg_t *c)
{
    /* A property size equal to the payload size collapses the two clone oracles
     * into one bucket (the allocator can no longer separate payload from
     * property clones), so the property-copy oracle would be unmeasurable and a
     * threaded run would predictably invalidate. Refuse before launching. */
    if (c->property != 0 && c->property == c->payload) {
        return "property-aliases-payload";
    }
    uint64_t warmup = (uint64_t)c->objects / 4u + 8u;
    uint64_t cap = c->cap ? c->cap : 32u;   /* mirrors sat_run_one's default */
    /* The source log retains payload AND properties per record. */
    uint64_t per_obj = ((c->shape == SHAPE_WHOLE)
                            ? c->payload
                            : (uint64_t)c->chunks * c->payload) +
                       c->property;
    /* Upper bound on the retained (undelivered) set during the timed window.
     * Ceiling: warmup + measured window + one CAP of in-flight. Knee (open-loop):
     * warmup + every object the fixed-rate schedule can inject over the window
     * (lambda * T), since a saturating rate leaves them undelivered. */
    uint64_t retained;
    if (c->campaign == CAMPAIGN_KNEE) {
        /* The injection period must be representable on BOTH sides: a lambda
         * past 1e9 has a sub-nanosecond period (truncates to 0), and a tiny
         * lambda has a period past the whole window (1e9/lambda can even
         * overflow to inf, and its uint64 conversion plus the deadline
         * arithmetic would be undefined). Require at least one full period to
         * fit the window, which also bounds period_ns <= duration_ns so every
         * downstream sum/ceil stays far from uint64 overflow. */
        double period_d = (c->lambda > 0.0) ? 1e9 / c->lambda : 0.0;
        double dur_ns_d = (double)c->duration_ms * 1e6;
        if (!isfinite(period_d) || period_d < 1.0 || period_d > dur_ns_d) {
            return "lambda-period-unrepresentable";
        }
        /* Bound the injected count IN DOUBLE before any integer conversion: a
         * finite-but-huge lambda*T (e.g. 1e300 * 1ms) would be undefined (or
         * saturate then WRAP in the uint64 sum below), silently passing the
         * record-cap compare. Anything at or past the record cap is refused
         * here, so the cast below is always in range. */
        double dur_s = (double)c->duration_ms / 1000.0;
        double inj_d = (c->lambda > 0.0 && dur_s > 0.0) ? c->lambda * dur_s
                                                        : 0.0;
        if (!isfinite(inj_d) ||
            inj_d >= (double)SAT_LOG_OBJECTS_PER_GROUP) {
            return "per-group-record-count";
        }
        retained = warmup + ((uint64_t)inj_d + 1u);
    } else {
        retained = warmup + c->objects + cap;
    }

    moqr_log_cfg_t lc;
    moqr_log_cfg_init_sized(&lc, sizeof(lc), moq_alloc_default());
    lc.budget.max_bytes = SAT_LOG_BUDGET_BYTES;
    lc.budget.max_groups = SAT_LOG_MAX_GROUPS;
    lc.max_objects_per_group = SAT_LOG_OBJECTS_PER_GROUP;
    moqr_log_capacity_t lcap;
    if (moqr_log_capacity_describe(&lc, &lcap) != MOQR_OK) {
        return "log-model-refused";
    }
    if (retained > SAT_LOG_OBJECTS_PER_GROUP) {
        return "per-group-record-count";   /* configured one-group cap */
    }
    if (retained > lcap.max_records) {
        return "journal-record-ceiling";    /* resolved journal window */
    }
    if (per_obj != 0 && retained > SAT_LOG_BUDGET_BYTES / per_obj) {
        return "byte-budget";
    }
    if (c->shape == SHAPE_CHUNKED &&
        (lcap.max_chunk_nodes == 0 ||
         retained > (uint64_t)lcap.max_chunk_nodes / c->chunks)) {
        return "chunk-node-pool";
    }
    return NULL;
}

/* ------------------------------------------------------------- verify */

/* A self-consistent VALID lifetime snapshot (K=3, two remote destinations, one
 * subscriber each) that sat_decide accepts. Unit tests mutate one field to
 * drive a specific gate directly — independent of the setup fixed-point check
 * and the live stall guard, which pre-empt some defects in a full run. */
static void
sat_life_good(sat_life_t *v, sat_shape_t shape)
{
    memset(v, 0, sizeof(*v));
    v->K = 3;
    v->remote_demands = 2;
    v->produced = 10;
    v->shape = shape;
    v->payload = (shape == SHAPE_WHOLE) ? 257u : 61u;
    v->property = 0;
    v->chunks = 4;
    v->cap = 32;
    v->sub_count[1] = 1;   /* remote-uniform F=1: one sub on each of shard 1,2 */
    v->sub_count[2] = 1;
    uint64_t prd = v->produced, rd = v->remote_demands;
    uint64_t ppr = (shape == SHAPE_WHOLE) ? v->payload
                                          : (uint64_t)v->chunks * v->payload;
    for (uint16_t d = 1; d < v->K; d++) {
        v->ingested[d] = prd;
        v->dlv_bytes[d] = (uint64_t)v->sub_count[d] * prd * ppr;
        v->core_b0[d] = 2;   /* lane-sampled window population: delta 6 */
        v->core_b1[d] = 8;
    }
    v->window_completed = 6;   /* == min per-dest core delta */
    if (shape == SHAPE_WHOLE) {
        v->kind[MOQR_SHARDS_MSG_OBJ] = rd * prd;
        v->owner_pump_bytes = rd * prd * (uint64_t)v->payload;
        v->owner_pump_messages = rd * prd;
        v->cross_shard_copies = (int64_t)(rd * prd);
        v->timed_obj = 6;
        v->timed_data = 6;
        v->timed_msgs = 6;
        v->timed_bytes = 6u * (uint64_t)v->payload;
    } else {
        v->kind[MOQR_SHARDS_MSG_OBJ_OPEN] = rd * prd;
        v->kind[MOQR_SHARDS_MSG_OBJ_END] = rd * prd;
        v->kind[MOQR_SHARDS_MSG_OBJ_CHUNK] = rd * prd * v->chunks;
        v->owner_pump_bytes = rd * prd * (uint64_t)v->chunks * v->payload;
        v->owner_pump_messages = rd * prd + rd * prd + rd * prd * v->chunks;
        v->cross_shard_copies = -1;   /* chunked: null (ex_copies passes) */
        v->timed_open = 3;
        v->timed_chunk = 12;
        v->timed_data = 3 + 12 + 3;   /* open + chunk + end */
        v->timed_msgs = 3 + 12 + 3;
        v->timed_bytes = v->timed_chunk * (uint64_t)v->payload;   /* property 0 */
    }
    v->property_copies = -1;
    v->kind[MOQR_SHARDS_MSG_DEMAND] = rd;
    v->kind[MOQR_SHARDS_MSG_ACK] = rd;
    v->kind[MOQR_SHARDS_MSG_UNDEMAND] = rd;
    v->peak_bytes = 1000;
    v->capacity_described = true;
    v->capacity_ceiling = 1u << 30;   /* well above peak_bytes */
    v->elapsed_ns = 1000000;
    v->skew_fraction = 0.10;
}

/* A short structural run for CI: no wall-clock threshold. Exercises the exact
 * lifetime gates, K=1 inertness, and the F-vs-2F clone invariance. */
static int
sat_verify(void)
{
    int fail = 0;

    /* Pure lifetime-validator gates, driven directly (no setup / stall guard in
     * the path): a good snapshot is accepted, and each targeted mutation flips
     * exactly its gate. This is where the D9 items whose full-run neuters are
     * pre-empted by the setup fixed-point check (missing destination) or the
     * live stall guard (dropped terminal) are proven. */
    {
        sat_life_t v;
        int        ok = 1;
        sat_life_good(&v, SHAPE_WHOLE);
        const char *r0 = sat_decide(&v);
        if (r0 != NULL) {
            printf("FAIL: good whole snapshot rejected: %s\n", r0);
            ok = 0;
        }
        sat_life_good(&v, SHAPE_CHUNKED);
        const char *r0c = sat_decide(&v);
        if (r0c != NULL) {
            printf("FAIL: good chunked snapshot rejected: %s\n", r0c);
            ok = 0;
        }
        /* Short destination: one remote ingested fewer than produced. Reaches
         * completion-mismatch directly (a full run's setup check pre-empts it). */
        sat_life_good(&v, SHAPE_WHOLE);
        v.ingested[1] = v.produced - 1;
        const char *rc = sat_decide(&v);
        if (rc == NULL || strcmp(rc, "completion-mismatch") != 0) {
            printf("FAIL: short-destination -> '%s' (want completion-mismatch)\n",
                   rc ? rc : "null");
            ok = 0;
        }
        /* Missing OBJ_END (dropped terminal): the data-kind vector is short.
         * Reaches data-kind-total directly (a full run's stall guard pre-empts
         * it as lane-error). */
        sat_life_good(&v, SHAPE_CHUNKED);
        v.kind[MOQR_SHARDS_MSG_OBJ_END] -= 1;
        const char *re = sat_decide(&v);
        if (re == NULL || strcmp(re, "data-kind-total") != 0) {
            printf("FAIL: missing-OBJ_END -> '%s' (want data-kind-total)\n",
                   re ? re : "null");
            ok = 0;
        }
        /* A leftover requester-open / owner-progress slot: quiescence gate. */
        sat_life_good(&v, SHAPE_CHUNKED);
        v.open_objs = 1;
        const char *rq = sat_decide(&v);
        if (rq == NULL || strcmp(rq, "not-quiesced") != 0) {
            printf("FAIL: leftover-open-slot -> '%s' (want not-quiesced)\n",
                   rq ? rq : "null");
            ok = 0;
        }
        /* The capacity model failing to resolve invalidates the point (there is
         * no ceiling to bound peak_live against) — a zero ceiling must NOT read
         * as "check disabled". */
        sat_life_good(&v, SHAPE_WHOLE);
        v.capacity_described = false;
        v.capacity_ceiling = 0;
        const char *rm = sat_decide(&v);
        if (rm == NULL || strcmp(rm, "capacity-model-refused") != 0) {
            printf("FAIL: no-capacity-model -> '%s' (want capacity-model-refused)\n",
                   rm ? rm : "null");
            ok = 0;
        }
        SAT_CHECK(ok);
        if (ok) {
            printf("PASS: verify pure validator (good ok; short-dest, "
                   "missing-END, leftover-slot each flip their gate)\n");
        } else {
            fail = 1;
        }
    }

    /* Window completions come from the minimum per-destination core delta
     * (each destination's OWN B0/B1 snapshot), not the owner's earlier live
     * read. A destination that completes one extra object in its final step
     * records it in b1[d]; the min over sub-bearing remotes is the coherent
     * population, and shards with no subscribers are ignored. */
    {
        int      ok = 1;
        uint64_t b0[SAT_MAX_SHARDS] = { 0, 10, 10, 10 };
        /* dest1 did 100, dest2 did 101 (finished one more at stop), dest3 140 */
        uint64_t b1[SAT_MAX_SHARDS] = { 0, 110, 111, 150 };
        uint32_t sc[SAT_MAX_SHARDS] = { 0, 1, 1, 1 };
        uint64_t m = sat_min_core_delta(b0, b1, sc, 4);
        if (m != 100) {
            printf("FAIL: min-core-delta got %llu (want 100)\n",
                   (unsigned long long)m);
            ok = 0;
        }
        /* a subscriber-less shard is excluded even with a smaller delta */
        uint64_t b1b[SAT_MAX_SHARDS] = { 0, 110, 20, 150 };
        uint32_t scb[SAT_MAX_SHARDS] = { 0, 1, 0, 1 };
        uint64_t mb = sat_min_core_delta(b0, b1b, scb, 4);
        if (mb != 100) {
            printf("FAIL: min-core-delta excl. subless got %llu (want 100)\n",
                   (unsigned long long)mb);
            ok = 0;
        }
        SAT_CHECK(ok);
        if (ok) {
            printf("PASS: verify window completions = min per-dest core delta\n");
        } else {
            fail = 1;
        }
    }

    /* Open-loop scheduled-deadline count is (T,period)-derived, independent of
     * what the injector attempted, so trailing deadlines the loop skipped at the
     * window edge are still counted (deficit -> missed). No wall clock. */
    {
        int ok = 1;
        struct { uint64_t t, p, want; } cs[] = {
            { 100, 50, 2 }, { 100, 30, 4 }, { 150, 50, 3 }, { 1, 1, 1 },
            { 0, 50, 0 },
            /* a huge period must not overflow the ceil (naive t+p-1 wraps) */
            { 1000000, UINT64_MAX, 1 }, { 1000000, UINT64_MAX - 5, 1 },
        };
        for (size_t i = 0; i < sizeof(cs) / sizeof(cs[0]); i++) {
            uint64_t got = sat_scheduled_deadlines(cs[i].t, cs[i].p);
            if (got != cs[i].want) {
                printf("FAIL: scheduled(%llu,%llu)=%llu want %llu\n",
                       (unsigned long long)cs[i].t, (unsigned long long)cs[i].p,
                       (unsigned long long)got, (unsigned long long)cs[i].want);
                ok = 0;
            }
        }
        /* A window that attempts fewer than scheduled leaves a counted deficit. */
        uint64_t scheduled = sat_scheduled_deadlines(1000000, 10000);   /* 100 */
        uint64_t attempted = 97;
        uint64_t unattempted =
            (scheduled > attempted) ? scheduled - attempted : 0u;
        if (scheduled != 100 || unattempted != 3) {
            printf("FAIL: deficit scheduled=%llu unattempted=%llu (want 100/3)\n",
                   (unsigned long long)scheduled,
                   (unsigned long long)unattempted);
            ok = 0;
        }
        SAT_CHECK(ok);
        if (ok) {
            printf("PASS: verify open-loop scheduled count independent of "
                   "attempts (deficit counted)\n");
        } else {
            fail = 1;
        }
    }

    /* Open-loop (knee) generator-validity gates, driven directly: a good knee
     * snapshot is accepted, and the offered-rate tolerance, injection-lateness,
     * and endpoint-skew boundaries each flip only on their far side; a refused
     * ingest (lane error) invalidates. The residual gate is skipped for knee. */
    {
        int ok = 1;
        /* A valid knee snapshot = a valid whole lifetime + a sustained rate that
         * tracks the request within tolerance and on-time injection. */
        sat_life_t g;
        sat_life_good(&g, SHAPE_WHOLE);
        g.is_knee = true;
        g.requested_lambda = 1000.0;
        g.actual_offered_rate = 1000.0;
        g.rate_tolerance = 0.15;
        g.inject_period_ns = 1000000;    /* 1 ms period */
        g.lateness_fraction = 0.25;      /* 250 us budget */
        g.max_injection_lateness_ns = 200000;   /* 0.2 ms: on time */
        g.duration_ns = 150u * 1000000u;        /* T=150ms -> 150 deadlines */
        g.objects_scheduled = 150;
        g.objects_attempted = 150;
        g.deadlines_missed = 0;
        /* A large in-flight backlog would trip the CLOSED-LOOP residual bound;
         * prove it is skipped for knee by setting an out-of-ceiling residual. */
        g.steady_state_residual = (int64_t)g.remote_demands * (int64_t)g.cap *
                                      1 * 1000;
        const char *rg = sat_decide(&g);
        if (rg != NULL) {
            printf("FAIL: good knee snapshot rejected: %s\n", rg);
            ok = 0;
        }
        struct {
            const char *name;
            double      actual;
            uint64_t    maxlate;
            uint64_t    skew;
            bool        lane_err;
            const char *want;   /* NULL == valid */
        } cases[] = {
            { "rate just inside (low)", 860.0, 200000, 0, false, NULL },
            { "rate just outside (low)", 840.0, 200000, 0, false,
              "offered-rate-out-of-tolerance" },
            { "rate just inside (high)", 1140.0, 200000, 0, false, NULL },
            { "rate just outside (high)", 1160.0, 200000, 0, false,
              "offered-rate-out-of-tolerance" },
            { "lateness just inside", 1000.0, 249000, 0, false, NULL },
            { "lateness just outside", 1000.0, 251000, 0, false,
              "injection-lateness" },
            { "skew just inside", 1000.0, 200000, 99999, false, NULL },
            { "skew just outside", 1000.0, 200000, 100001, false,
              "endpoint-skew" },
            { "refused ingest", 1000.0, 200000, 0, true, "lane-error" },
        };
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            sat_life_t v = g;
            v.actual_offered_rate = cases[i].actual;
            v.max_injection_lateness_ns = cases[i].maxlate;
            v.endpoint_skew_ns = cases[i].skew;
            /* skew is gated as a fraction of the window; use skew_fraction 0.10
             * with a 1 ms window so 100000 ns is the boundary. */
            v.elapsed_ns = 1000000;
            v.skew_fraction = 0.10;
            v.lane_error = cases[i].lane_err;
            const char *r = sat_decide(&v);
            bool good_case = (cases[i].want == NULL) ? (r == NULL)
                             : (r != NULL && strcmp(r, cases[i].want) == 0);
            if (!good_case) {
                printf("FAIL: knee case '%s' -> '%s' (want '%s')\n",
                       cases[i].name, r ? r : "null",
                       cases[i].want ? cases[i].want : "valid");
                ok = 0;
            }
        }
        /* Call-site cross-checks: the validator RECOMPUTES the window
         * population and the scheduled-deadline count, so production deriving
         * either from the wrong source (owner's live progress read; attempt
         * counting) is caught on the reported values themselves. */
        sat_life_t m = g;
        m.window_completed = 5;   /* owner-live-read style value; snapshots say 6 */
        const char *rw = sat_decide(&m);
        if (rw == NULL || strcmp(rw, "window-population-mismatch") != 0) {
            printf("FAIL: wrong window population -> '%s' (want "
                   "window-population-mismatch)\n", rw ? rw : "null");
            ok = 0;
        }
        m = g;
        m.objects_scheduled = 147;   /* attempt-derived; ceil(T/p) says 150 */
        m.objects_attempted = 147;
        const char *rs = sat_decide(&m);
        if (rs == NULL || strcmp(rs, "scheduled-count-mismatch") != 0) {
            printf("FAIL: attempt-derived scheduled -> '%s' (want "
                   "scheduled-count-mismatch)\n", rs ? rs : "null");
            ok = 0;
        }
        m = g;
        m.objects_attempted = 147;   /* 3 unattempted deadlines... */
        m.deadlines_missed = 0;      /* ...not counted as missed */
        const char *ru = sat_decide(&m);
        if (ru == NULL || strcmp(ru, "missed-undercount") != 0) {
            printf("FAIL: uncounted deficit -> '%s' (want missed-undercount)\n",
                   ru ? ru : "null");
            ok = 0;
        }
        SAT_CHECK(ok);
        if (ok) {
            printf("PASS: verify knee validator (good ok; rate-tolerance, "
                   "lateness, skew boundaries both sides; refused-ingest; "
                   "window-population, scheduled-count, missed-undercount "
                   "cross-checks)\n");
        } else {
            fail = 1;
        }
    }

    /* Serializer field layout: format a known row and pin the CSV field count,
     * selected name->value positions, and the null conventions (open-loop
     * fields, starved ceiling fields, null copy fields); and the JSON canonical
     * keys + null values. A reordered format arg or a `0`-instead-of-`null`
     * would be caught here. */
    {
        sat_result_t o;
        memset(&o, 0, sizeof(o));
        o.campaign = "ceiling";
        o.shape = "whole";
        o.dist = "remote-uniform";
        o.valid = true;
        snprintf(o.invalid_reason, sizeof(o.invalid_reason), "none");
        snprintf(o.affinity, sizeof(o.affinity), "unpinned");
        o.shards = 3;
        o.remote_subs = 2;
        o.remote_demands = 2;
        o.fanout = 1;
        o.payload = 257;
        o.cap = 77;   /* a workload dimension: pinned at its own CSV position */
        o.objects_produced = 10;
        o.objects_completed = 10;
        o.bytes_completed = 5140;
        o.elapsed_ns = 1000000;
        o.cross_shard_copies = -1;   /* -> "null" */
        o.property_copies = -1;      /* -> "null" */
        o.ceiling_valid = false;     /* -> B_star / mu_cap "null" */

        char   csv[2560];
        sat_format_csv(&o, csv, sizeof(csv));
        char  *fields[80];
        size_t nf = 0;
        char   tmp[2560];
        snprintf(tmp, sizeof(tmp), "%s", csv);
        for (char *p = tmp;; ) {
            fields[nf < 80 ? nf : 79] = p;
            nf++;
            char *comma = strchr(p, ',');
            if (comma == NULL) {
                break;
            }
            *comma = '\0';
            p = comma + 1;
        }
        int ok = 1;
        if (nf != 58) {
            printf("FAIL: CSV field count %zu != 58\n", nf);
            ok = 0;
        }
#define SAT_FIELD_IS(idx, want)                                               \
    do {                                                                      \
        if (nf > (idx) && strcmp(fields[idx], want) != 0) {                   \
            printf("FAIL: CSV field %d = '%s' (want '%s')\n", (idx),          \
                   fields[idx], want);                                        \
            ok = 0;                                                           \
        }                                                                     \
    } while (0)
        SAT_FIELD_IS(0, "in_process_wall");   /* measurement_basis */
        SAT_FIELD_IS(1, "ceiling");           /* campaign          */
        SAT_FIELD_IS(11, "whole");            /* object_shape      */
        SAT_FIELD_IS(12, "remote-uniform");   /* distribution      */
        SAT_FIELD_IS(14, "77");               /* cap               */
        SAT_FIELD_IS(18, "null");             /* objects_scheduled */
        SAT_FIELD_IS(21, "null");             /* requested_lambda  */
        SAT_FIELD_IS(29, "null");             /* B_star (starved)  */
        SAT_FIELD_IS(30, "null");             /* mu_cap (starved)  */
        SAT_FIELD_IS(45, "null");             /* cross_shard_copies*/
        SAT_FIELD_IS(46, "null");             /* property_copies   */
#undef SAT_FIELD_IS

        char json[2560];
        sat_format_json(&o, json, sizeof(json));
        static const char *const jkeys[] = {
            "\"measurement_basis\":\"in_process_wall\"",
            "\"distribution\":\"remote-uniform\"",
            "\"requested_lambda\":null",
            "\"B_star_msgs_per_sec\":null",
            "\"mu_cap\":null",
            "\"cross_shard_copies\":null",
            "\"property_copies\":null",
        };
        for (size_t i = 0; i < sizeof(jkeys) / sizeof(jkeys[0]); i++) {
            if (strstr(json, jkeys[i]) == NULL) {
                printf("FAIL: JSON missing '%s'\n", jkeys[i]);
                ok = 0;
            }
        }
        /* Truncation is detectable: an undersized buffer reports a would-be
         * length past its end (so sat_emit refuses to publish it), while a
         * full-size buffer reports a length that fits. */
        char   tiny[16];
        int    cneed = sat_format_csv(&o, tiny, sizeof(tiny));
        int    jneed = sat_format_json(&o, tiny, sizeof(tiny));
        int    cfull = sat_format_csv(&o, csv, sizeof(csv));
        int    jfull = sat_format_json(&o, json, sizeof(json));
        if (cneed < (int)sizeof(tiny) || jneed < (int)sizeof(tiny)) {
            printf("FAIL: truncation not reported (csv=%d json=%d, buf=%zu)\n",
                   cneed, jneed, sizeof(tiny));
            ok = 0;
        }
        if (cfull <= 0 || cfull >= (int)sizeof(csv) || jfull <= 0 ||
            jfull >= (int)sizeof(json)) {
            printf("FAIL: full-size format not clean (csv=%d json=%d)\n", cfull,
                   jfull);
            ok = 0;
        }
        /* A KNEE row populates the open-loop fields with NUMBERS (not null), and
         * requested_lambda / actual_offered_rate are DISTINCT fields (actual is
         * computed from accepted, not echoed from requested). */
        sat_result_t kn;
        memset(&kn, 0, sizeof(kn));
        kn.campaign = "knee";
        kn.is_knee = true;
        kn.shape = "whole";
        kn.dist = "remote-uniform";
        kn.valid = true;
        snprintf(kn.invalid_reason, sizeof(kn.invalid_reason), "none");
        snprintf(kn.affinity, sizeof(kn.affinity), "unpinned");
        kn.shards = 2;
        kn.remote_demands = 1;
        kn.payload = 257;
        kn.requested_lambda = 1000.0;
        kn.actual_offered_rate = 900.0;   /* DISTINCT from requested */
        kn.objects_scheduled = 2000;
        kn.objects_attempted = 2000;
        kn.objects_accepted = 1800;
        kn.injection_deadlines_missed = 5;
        kn.max_injection_lateness_ns = 12345;
        kn.ceiling_valid = true;
        char kjson[2560];
        sat_format_json(&kn, kjson, sizeof(kjson));
        static const char *const kkeys[] = {
            "\"campaign\":\"knee\"",
            "\"requested_lambda\":1000.000",
            "\"actual_offered_rate\":900.000",
            "\"objects_scheduled\":2000",
            "\"objects_accepted\":1800",
            "\"injection_deadlines_missed\":5",
            "\"max_injection_lateness_ns\":12345",
        };
        for (size_t i = 0; i < sizeof(kkeys) / sizeof(kkeys[0]); i++) {
            if (strstr(kjson, kkeys[i]) == NULL) {
                printf("FAIL: knee JSON missing '%s'\n", kkeys[i]);
                ok = 0;
            }
        }
        if (strstr(kjson, "\"requested_lambda\":null") != NULL ||
            strstr(kjson, "\"actual_offered_rate\":null") != NULL) {
            printf("FAIL: knee row emitted null for a populated open-loop field\n");
            ok = 0;
        }
        SAT_CHECK(ok);
        if (ok) {
            printf("PASS: verify serializers (CSV 58 fields + positions/nulls; "
                   "JSON canonical keys + nulls; knee fields numeric + distinct; "
                   "truncation reported)\n");
        } else {
            fail = 1;
        }
    }

    /* Schema stability: the canonical column names, in EXACT ORDER, with an
     * exact COUNT. A rename, reorder, insertion, or deletion all fail here —
     * substring presence (the old check) would let a renamed column that still
     * contains an old name as a substring slip through, and could not catch a
     * reorder or a duplicate. */
    {
        static const char *const expect[] = {
            "measurement_basis", "campaign", "valid", "invalidation_reason",
            "pinned", "cpu_affinity_map", "shards", "local_subs", "remote_subs",
            "remote_demands", "fanout", "object_shape", "distribution",
            "object_size", "cap", "objects_produced", "objects_completed",
            "bytes_completed", "objects_scheduled", "objects_attempted",
            "objects_accepted", "requested_lambda", "actual_offered_rate",
            "injection_deadlines_missed", "max_injection_lateness_ns",
            "elapsed_ns", "wall_objects_per_sec", "wall_bytes_per_sec",
            "wall_ns_per_object", "B_star_msgs_per_sec", "mu_cap",
            "steady_state_residual", "backlog_emptied", "pump_turns",
            "pump_messages", "pump_bytes", "messages_per_turn", "bytes_per_turn",
            "wake_requests_push", "wake_requests_credit",
            "wake_requests_local", "channel_entries_hwm",
            "channel_bytes_hwm", "cross_shard_messages",
            "payload_bearing_messages", "cross_shard_copies", "property_copies",
            "clones_per_object", "allocs", "frees", "peak_live_bytes",
            "capacity_ceiling_bytes", "endpoint_skew_ns", "skew_fraction",
            "lateness_fraction", "remote_data_rejected", "term_capacity",
            "term_overrun",
        };
        size_t      nexp = sizeof(expect) / sizeof(expect[0]);
        const char *p = SAT_COLS;
        size_t      n = 0;
        int         ok = 1;
        for (;;) {
            const char *comma = strchr(p, ',');
            size_t      len = comma ? (size_t)(comma - p) : strlen(p);
            if (n >= nexp || strlen(expect[n]) != len ||
                strncmp(p, expect[n], len) != 0) {
                printf("FAIL: schema column %zu mismatch (got '%.*s')\n", n,
                       (int)len, p);
                ok = 0;
            }
            n++;
            if (comma == NULL) {
                break;
            }
            p = comma + 1;
        }
        if (n != nexp) {
            printf("FAIL: schema column count %zu != %zu\n", n, nexp);
            ok = 0;
        }
        /* Nullable formatting must not drift: the null helper maps a negative to
         * "null" and a non-negative to its decimal (so cross_shard_copies /
         * property_copies / ceiling fields render null consistently). */
        char nb[24];
        sat_nulli(nb, sizeof(nb), -1);
        if (strcmp(nb, "null") != 0) {
            printf("FAIL: sat_nulli(-1) = '%s' (want null)\n", nb);
            ok = 0;
        }
        sat_nulli(nb, sizeof(nb), 7);
        if (strcmp(nb, "7") != 0) {
            printf("FAIL: sat_nulli(7) = '%s' (want 7)\n", nb);
            ok = 0;
        }
        SAT_CHECK(ok);
        if (ok) {
            printf("PASS: verify schema (exact ordered names + count = %zu)\n",
                   nexp);
        } else {
            fail = 1;
        }
    }

    /* (1) K=1: the cross-shard plane is structurally inert. gauntlet puts all
     * subs local on shard 0. */
    {
        sat_cfg_t c = { .verify = true,
                        .K = 1,
                        .fanout = 3,
                        .objects = 64,
                        .payload = 257,
                        .chunks = 4,
                        .shape = SHAPE_WHOLE,
                        .dist = DIST_GAUNTLET,
                        .fmt = FMT_CSV,
                        .skew_fraction = 1.0,
                        .lateness_fraction = 1.0,
                        .require_affinity = false };
        sat_result_t o;
        SAT_CHECK(sat_run_one(&c, -1, &o));
        SAT_CHECK(o.valid);
        SAT_CHECK(o.remote_demands == 0);
        SAT_CHECK(o.cross_shard_messages == 0);
        SAT_CHECK(o.pump_messages == 0);
        SAT_CHECK(o.local_subs == 3);
        if (o.valid && o.remote_demands == 0 && o.cross_shard_messages == 0) {
            printf("PASS: verify K=1 inert (local=%u)\n", o.local_subs);
        } else {
            fail = 1;
        }
    }

    /* (2) K=2 remote-uniform, whole objects: exact lifetime gates + a real
     * boundary. */
    {
        sat_cfg_t c = { .verify = true,
                        .K = 2,
                        .fanout = 4,
                        .objects = 64,
                        .payload = 257,
                        .chunks = 4,
                        .shape = SHAPE_WHOLE,
                        .dist = DIST_REMOTE_UNIFORM,
                        .fmt = FMT_CSV,
                        .skew_fraction = 1.0,
                        .lateness_fraction = 1.0,
                        .require_affinity = false };
        sat_result_t o;
        SAT_CHECK(sat_run_one(&c, -1, &o));
        SAT_CHECK(o.valid);
        SAT_CHECK(o.remote_demands == 1);
        SAT_CHECK(o.cross_shard_copies >= 0);   /* attribution valid */
        /* Finding 1 — every timed field shares ONE window [B0,B1]: the byte
         * rate is assigned (was 0), and the window byte total is exactly the
         * window delivery count times the payload (same population, not a
         * lifetime sum). */
        SAT_CHECK(o.bytes_per_sec > 0.0);
        SAT_CHECK(o.objects_completed > 0);
        SAT_CHECK(o.bytes_completed == o.window_deliveries * o.payload);
        /* Finding 3 — the data-channel HWM is read from the DESTINATION rows
         * (owner->d channels), so with a 32-deep in-flight window it exceeds
         * the owner's own inbound max of <=1. Reading shard 0 would report <=1. */
        SAT_CHECK(o.chan_entries_hwm > 1);
        /* Core-only shape guard: every subscriber here is a DIRECT core binding
         * with no bind connection, so no bind-pump delivery (and no genuine
         * residual) exists — wake_requests_local MUST be exactly zero. Their
         * core-ready cookies alias unused bind-conn slots; a raw dl_ready scan
         * would treat those sticky bits as residual and manufacture one false
         * local continuation per inbound pop. RED: drop the live-connection
         * guard in moqr_bind_pump_continuation_pending and this jumps to match
         * the credit count. */
        SAT_CHECK(o.wake_local == 0);
        if (o.valid && o.bytes_completed == o.window_deliveries * o.payload &&
            o.chan_entries_hwm > 1) {
            printf("PASS: verify K=2 remote-uniform (copies/obj=%.2f, "
                   "chanHWM=%u, bytes/sec=%.0f)\n",
                   o.clones_per_object, o.chan_entries_hwm, o.bytes_per_sec);
        } else {
            fail = 1;
            printf("FAIL: verify K=2 invalid: %s (chanHWM=%u bytes/sec=%.0f)\n",
                   o.invalid_reason, o.chan_entries_hwm, o.bytes_per_sec);
        }
    }

    /* (3) F-vs-2F clone invariance: clones per object == remote_demands and is
     * UNCHANGED across fanout; delivered bytes scale with fanout. */
    {
        sat_cfg_t base = { .verify = true,
                           .K = 4,
                           .fanout = 2,
                           .objects = 48,
                           .payload = 257,
                           .chunks = 4,
                           .shape = SHAPE_WHOLE,
                           .dist = DIST_REMOTE_UNIFORM,
                           .fmt = FMT_CSV,
                           .skew_fraction = 1.0,
                           .lateness_fraction = 1.0,
                           .require_affinity = false };
        sat_result_t o1, o2;
        sat_cfg_t     c2 = base;
        c2.fanout = 4;
        SAT_CHECK(sat_run_one(&base, -1, &o1));
        SAT_CHECK(sat_run_one(&c2, -1, &o2));
        /* Both valid ⇒ both passed the EXACT per-run gates, incl. the exact
         * clone count (cross_shard_copies == remote_demands·produced) and the
         * exact delivered-bytes total (== total_subs·produced·payload). So the
         * fanout relationship is proven exactly, not by a >1.5x heuristic. */
        SAT_CHECK(o1.valid && o2.valid);
        SAT_CHECK(o1.cross_shard_copies >= 0 && o2.cross_shard_copies >= 0);
        SAT_CHECK((uint64_t)(o1.clones_per_object + 0.5) == o1.remote_demands);
        SAT_CHECK((uint64_t)(o2.clones_per_object + 0.5) == o2.remote_demands);
        SAT_CHECK(o1.remote_demands == 3 && o2.remote_demands == 3);   /* K-1 */
        /* Fanout doubled (remote subscriber count 2x) but the clone count per
         * object is UNCHANGED (== remote_demands) — reference-only local fan. */
        SAT_CHECK(o2.remote_subs == 2u * o1.remote_subs);
        SAT_CHECK(o1.clones_per_object == o2.clones_per_object);
        if (o1.valid && o2.valid && o2.remote_subs == 2u * o1.remote_subs) {
            printf("PASS: verify F-vs-2F clone invariance "
                   "(clones/obj=%.2f both; remote_subs %u -> %u)\n",
                   o1.clones_per_object, o1.remote_subs, o2.remote_subs);
        } else {
            fail = 1;
        }
    }

    /* (4) chunked shape completes cleanly at K=2. */
    {
        sat_cfg_t c = { .verify = true,
                        .K = 2,
                        .fanout = 2,
                        .objects = 32,
                        .payload = 61,
                        .chunks = 4,
                        .shape = SHAPE_CHUNKED,
                        .dist = DIST_REMOTE_UNIFORM,
                        .fmt = FMT_CSV,
                        .skew_fraction = 1.0,
                        .lateness_fraction = 1.0,
                        .require_affinity = false };
        sat_result_t o;
        SAT_CHECK(sat_run_one(&c, -1, &o));
        SAT_CHECK(o.valid);
        if (o.valid) {
            printf("PASS: verify K=2 chunked\n");
        } else {
            fail = 1;
            printf("FAIL: verify chunked invalid: %s\n", o.invalid_reason);
        }
    }

    /* (5) Property-copy oracle: whole objects carrying properties, at F and 2F.
     * The property clone count is an INDEPENDENT bucket (a distinct, non-aliasing
     * allocation size) equal to remote_demands·produced — one property clone per
     * object per remote destination — and its per-object value is UNCHANGED
     * across fanout, exactly like the payload clone count. That proves properties
     * are cloned once per remote destination and fanned out locally by reference
     * (a per-subscriber property clone would scale property_copies by F while
     * leaving cross_shard_copies fixed — the two oracles are separable). */
    {
        sat_cfg_t base = { .verify = true,
                           .K = 4,
                           .fanout = 2,
                           .objects = 48,
                           .payload = 257,
                           .property = 193,   /* distinct from payload size */
                           .chunks = 4,
                           .shape = SHAPE_WHOLE,
                           .dist = DIST_REMOTE_UNIFORM,
                           .fmt = FMT_CSV,
                           .skew_fraction = 1.0,
                           .lateness_fraction = 1.0,
                           .require_affinity = false };
        sat_cfg_t c2 = base;
        c2.fanout = 4;
        sat_result_t o1, o2;
        SAT_CHECK(sat_run_one(&base, -1, &o1));
        SAT_CHECK(sat_run_one(&c2, -1, &o2));
        SAT_CHECK(o1.valid && o2.valid);
        SAT_CHECK(o1.cross_shard_copies >= 0 && o1.property_copies >= 0);
        SAT_CHECK(o2.cross_shard_copies >= 0 && o2.property_copies >= 0);
        /* Both valid ⇒ both passed ex_prop (property_copies == rd·produced) and
         * ex_copies (cross_shard_copies == rd·produced), so per-object each is
         * exactly remote_demands. Check the per-object ratios are F-invariant. */
        double pay1 = (double)o1.cross_shard_copies / (double)o1.objects_produced;
        double pay2 = (double)o2.cross_shard_copies / (double)o2.objects_produced;
        double prp1 = (double)o1.property_copies / (double)o1.objects_produced;
        double prp2 = (double)o2.property_copies / (double)o2.objects_produced;
        SAT_CHECK((uint64_t)(prp1 + 0.5) == o1.remote_demands);
        SAT_CHECK((uint64_t)(prp2 + 0.5) == o2.remote_demands);
        SAT_CHECK(pay1 == pay2 && prp1 == prp2);
        SAT_CHECK(o2.remote_subs == 2u * o1.remote_subs);
        if (o1.valid && o2.valid && prp1 == prp2 &&
            (uint64_t)(prp1 + 0.5) == o1.remote_demands &&
            o2.remote_subs == 2u * o1.remote_subs) {
            printf("PASS: verify property-copy oracle (payload/obj=%.2f "
                   "property/obj=%.2f both F; remote_subs %u -> %u)\n",
                   pay1, prp1, o1.remote_subs, o2.remote_subs);
        } else {
            fail = 1;
            printf("FAIL: verify property oracle (o1 valid=%d prop=%lld; o2 "
                   "valid=%d prop=%lld)\n",
                   o1.valid, (long long)o1.property_copies, o2.valid,
                   (long long)o2.property_copies);
        }
    }

    /* (6) Starvation: an in-flight window of 1 against fast in-process
     * destinations empties the backlog, so the owner is NOT saturated — the
     * point is not a valid ceiling and the ceiling-rate fields must be null. */
    {
        sat_cfg_t c = { .verify = true,
                        .K = 2,
                        .fanout = 2,
                        .objects = 32,
                        .cap = 1,
                        .payload = 257,
                        .chunks = 4,
                        .shape = SHAPE_WHOLE,
                        .dist = DIST_REMOTE_UNIFORM,
                        .fmt = FMT_CSV,
                        .skew_fraction = 1.0,
                        .lateness_fraction = 1.0,
                        .require_affinity = false };
        sat_result_t o;
        SAT_CHECK(sat_run_one(&c, -1, &o));
        SAT_CHECK(o.backlog_emptied);        /* cap=1 starves the owner */
        SAT_CHECK(!o.ceiling_valid);         /* -> not a ceiling point   */
        if (o.backlog_emptied && !o.ceiling_valid) {
            printf("PASS: verify starvation (cap=1 -> backlog_emptied, ceiling "
                   "fields null)\n");
        } else {
            fail = 1;
            printf("FAIL: verify starvation not flagged (backlog=%d ceiling=%d)\n",
                   o.backlog_emptied, o.ceiling_valid);
        }
    }

    /* (7) Retention preflight — standing regression for every structural
     * refusal arm (a byte-fitting config that overflows records or chunk nodes,
     * and a huge --cap), plus a good config accepted. */
    {
        sat_cfg_t good = { .K = 2,
                           .fanout = 2,
                           .objects = 512,
                           .cap = 128,
                           .payload = 256,
                           .chunks = 4,
                           .shape = SHAPE_WHOLE,
                           .dist = DIST_REMOTE_UNIFORM };
        sat_cfg_t rec = good;   /* 5000 x 1B: bytes fit, records don't */
        rec.objects = 5000;
        rec.payload = 1;
        rec.cap = 32;
        sat_cfg_t chn = good;   /* chunked: records+bytes fit, chunk nodes don't */
        chn.objects = 2000;
        chn.payload = 61;
        chn.shape = SHAPE_CHUNKED;
        chn.cap = 32;
        sat_cfg_t big = good;   /* huge --cap overflows every count */
        big.cap = 4294967295u;
        sat_cfg_t alias = good;   /* property size == payload: unmeasurable oracle */
        alias.property = alias.payload;
        /* Open-loop overflow: lambda*T injected objects exceed the one-group
         * record cap and must be refused before workers start. */
        sat_cfg_t kover = good;
        kover.campaign = CAMPAIGN_KNEE;
        kover.lambda = 20000.0;
        kover.duration_ms = 1000;   /* ~20000 injected >> 4096 */
        sat_cfg_t kok = good;       /* a knee config that DOES fit */
        kok.campaign = CAMPAIGN_KNEE;
        kok.lambda = 2000.0;
        kok.duration_ms = 200;      /* ~400 injected, fits */
        /* Finite but astronomically large: refused by the period gate (any
         * lambda past 1e9 is sub-nanosecond); the double bound below the gate
         * additionally protects the cast for the in-range products. */
        sat_cfg_t kbig = good;
        kbig.campaign = CAMPAIGN_KNEE;
        kbig.lambda = 1e300;
        kbig.duration_ms = 1;
        /* In-range lambda whose product still overflows retention: exercises
         * the DOUBLE-space bound (compared before any integer conversion). */
        sat_cfg_t kmid = good;
        kmid.campaign = CAMPAIGN_KNEE;
        kmid.lambda = 5e8;
        kmid.duration_ms = 60000;   /* 3e10 objects >> 4096 */
        /* Finite but past 1e9: the injection period truncates below 1 ns. */
        sat_cfg_t kfast = good;
        kfast.campaign = CAMPAIGN_KNEE;
        kfast.lambda = 2e9;
        kfast.duration_ms = 1;
        /* Finite but tiny: 1e9/lambda overflows to inf, and no full period
         * fits the window — must refuse, never enter a long-running state. */
        sat_cfg_t ktiny = good;
        ktiny.campaign = CAMPAIGN_KNEE;
        ktiny.lambda = 1e-300;
        ktiny.duration_ms = 1;

        const char *rg = sat_retention_reason(&good);
        const char *rr = sat_retention_reason(&rec);
        const char *rc = sat_retention_reason(&chn);
        const char *rb = sat_retention_reason(&big);
        const char *ra = sat_retention_reason(&alias);
        const char *rk = sat_retention_reason(&kover);
        const char *rko = sat_retention_reason(&kok);
        const char *rkb = sat_retention_reason(&kbig);
        const char *rkm = sat_retention_reason(&kmid);
        const char *rkf = sat_retention_reason(&kfast);
        const char *rkt = sat_retention_reason(&ktiny);
        SAT_CHECK(rg == NULL);
        SAT_CHECK(rr != NULL && strcmp(rr, "per-group-record-count") == 0);
        SAT_CHECK(rc != NULL && strcmp(rc, "chunk-node-pool") == 0);
        SAT_CHECK(rb != NULL);
        SAT_CHECK(ra != NULL && strcmp(ra, "property-aliases-payload") == 0);
        SAT_CHECK(rk != NULL && strcmp(rk, "per-group-record-count") == 0);
        SAT_CHECK(rko == NULL);
        SAT_CHECK(rkb != NULL &&
                  strcmp(rkb, "lambda-period-unrepresentable") == 0);
        SAT_CHECK(rkm != NULL && strcmp(rkm, "per-group-record-count") == 0);
        SAT_CHECK(rkf != NULL &&
                  strcmp(rkf, "lambda-period-unrepresentable") == 0);
        SAT_CHECK(rkt != NULL &&
                  strcmp(rkt, "lambda-period-unrepresentable") == 0);
        if (rg == NULL && rr != NULL && rc != NULL && rb != NULL && ra != NULL &&
            rk != NULL && rko == NULL && rkb != NULL && rkm != NULL &&
            rkf != NULL && rkt != NULL) {
            printf("PASS: verify retention preflight (good ok; record=%s "
                   "chunk-node=%s huge-cap=%s alias=%s knee-overflow=%s "
                   "knee-ok=null huge-lambda=%s mid-lambda=%s fast-lambda=%s "
                   "tiny-lambda=%s)\n",
                   rr, rc, rb, ra, rk, rkb, rkm, rkf, rkt);
        } else {
            fail = 1;
            printf("FAIL: verify retention preflight (good=%s rec=%s chn=%s "
                   "big=%s alias=%s kover=%s kok=%s kbig=%s kmid=%s kfast=%s "
                   "ktiny=%s)\n",
                   rg ? rg : "null", rr ? rr : "null", rc ? rc : "null",
                   rb ? rb : "null", ra ? ra : "null", rk ? rk : "null",
                   rko ? rko : "null", rkb ? rkb : "null", rkm ? rkm : "null",
                   rkf ? rkf : "null", rkt ? rkt : "null");
        }
    }

    /* Open-loop B1 fires at the fixed injection DEADLINE, not on completion: a
     * short knee run's timed window is ~duration_ms (a completion-driven B1
     * would run much longer). Loose skew/lateness fractions keep the structural
     * check robust on an unpinned, noisy dev host (this is not a perf gate). */
    {
        sat_cfg_t c = { .verify = true,
                        .K = 2,
                        .fanout = 1,
                        .objects = 64,
                        .payload = 257,
                        .chunks = 4,
                        .shape = SHAPE_WHOLE,
                        .dist = DIST_REMOTE_UNIFORM,
                        .fmt = FMT_CSV,
                        .campaign = CAMPAIGN_KNEE,
                        .lambda = 2000.0,
                        .duration_ms = 150,
                        .rate_tolerance = 1.0,
                        .skew_fraction = 1.0,
                        .lateness_fraction = 1000.0,
                        .require_affinity = false };
        sat_result_t o;
        SAT_CHECK(sat_run_one(&c, -1, &o));
        uint64_t dur_ns = 150ull * 1000000ull;
        /* B1 at the deadline: elapsed >= ~duration and not a long completion
         * wait (a completion-driven B1 on any backlog would blow past this). */
        int ok = o.is_knee && o.elapsed_ns >= dur_ns * 8 / 10 &&
                 o.elapsed_ns <= dur_ns + 1000000000ull &&
                 o.objects_accepted > 0;
        SAT_CHECK(ok);
        if (ok) {
            printf("PASS: verify knee B1-at-deadline (elapsed=%.0fms ~ T=150ms, "
                   "accepted=%llu)\n",
                   (double)o.elapsed_ns / 1e6,
                   (unsigned long long)o.objects_accepted);
        } else {
            fail = 1;
            printf("FAIL: knee B1 timing (knee=%d elapsed=%lluns accepted=%llu "
                   "reason=%s)\n",
                   o.is_knee, (unsigned long long)o.elapsed_ns,
                   (unsigned long long)o.objects_accepted, o.invalid_reason);
        }
    }

    if (fail == 0 && g_failures == 0) {
        printf("# ALL PASS\n");
    }
    return (fail == 0 && g_failures == 0) ? 0 : 1;
}

/* ------------------------------------------------------------- args */

static bool
parse_u32(const char *s, uint32_t *out)
{
    if (s == NULL || *s == '\0') {
        return false;
    }
    char         *end = NULL;
    unsigned long v = strtoul(s, &end, 10);
    if (*end != '\0' || v > 0xFFFFFFFFul) {
        return false;
    }
    *out = (uint32_t)v;
    return true;
}

static bool
parse_frac(const char *s, double *out)
{
    if (s == NULL || *s == '\0') {
        return false;
    }
    char  *end = NULL;
    double v = strtod(s, &end);
    /* Reject NaN/Inf: a non-finite fraction would make every downstream
     * comparison (tolerance / skew / lateness) silently false and let an
     * invalid point read as valid. */
    if (*end != '\0' || !isfinite(v) || v < 0.0) {
        return false;
    }
    *out = v;
    return true;
}

static void
usage(const char *p)
{
    fprintf(stderr,
            "usage: %s [--verify] [--shards K] [--fanout F] [--objects N]\n"
            "          [--cap N] [--payload-bytes N] [--property-bytes N]\n"
            "          [--shape whole|chunked]\n"
            "          [--distribution remote-uniform|gauntlet]\n"
            "          [--format csv|json] [--endpoint-skew-fraction X]\n"
            "          [--lateness-fraction X] [--require-affinity]\n"
            "          [--campaign ceiling|knee] [--lambda objs/sec]\n"
            "          [--duration-ms T] [--rate-tolerance X]\n",
            p);
}

int
main(int argc, char **argv)
{
    sat_cfg_t c = { .verify = false,
                    .K = 2,
                    .fanout = 1,
                    .objects = 256,
                    .cap = 32,
                    .payload = 257,
                    .property = 0,
                    .chunks = 4,
                    .shape = SHAPE_WHOLE,
                    .dist = DIST_REMOTE_UNIFORM,
                    .fmt = FMT_CSV,
                    .campaign = CAMPAIGN_CEILING,
                    .lambda = 0.0,
                    .duration_ms = 500,
                    .rate_tolerance = 0.15,
                    .skew_fraction = 0.10,
                    .lateness_fraction = 0.25,
                    .require_affinity = false };

    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
#define NEXT()                                                               \
    (++i < argc ? argv[i] : (fprintf(stderr, "missing value for %s\n", a),   \
                             usage(argv[0]), exit(2), (char *)NULL))
        if (strcmp(a, "--verify") == 0) {
            c.verify = true;
        } else if (strcmp(a, "--require-affinity") == 0) {
            c.require_affinity = true;
        } else if (strcmp(a, "--shards") == 0) {
            uint32_t v;
            if (!parse_u32(NEXT(), &v) || v < 1 || v > SAT_MAX_SHARDS) {
                fprintf(stderr, "bad --shards\n");
                return 2;
            }
            c.K = (uint16_t)v;
        } else if (strcmp(a, "--fanout") == 0) {
            if (!parse_u32(NEXT(), &c.fanout) || c.fanout < 1) {
                fprintf(stderr, "bad --fanout\n");
                return 2;
            }
        } else if (strcmp(a, "--objects") == 0) {
            if (!parse_u32(NEXT(), &c.objects) || c.objects < 1) {
                fprintf(stderr, "bad --objects\n");
                return 2;
            }
        } else if (strcmp(a, "--cap") == 0) {
            if (!parse_u32(NEXT(), &c.cap) || c.cap < 1) {
                fprintf(stderr, "bad --cap\n");
                return 2;
            }
        } else if (strcmp(a, "--payload-bytes") == 0) {
            if (!parse_u32(NEXT(), &c.payload) || c.payload < 1 ||
                c.payload > 4096) {
                fprintf(stderr, "bad --payload-bytes\n");
                return 2;
            }
        } else if (strcmp(a, "--property-bytes") == 0) {
            /* 0 disables properties; otherwise a distinct, bounded size (the
             * clone oracle needs it not to alias the payload size). */
            if (!parse_u32(NEXT(), &c.property) || c.property > 4096) {
                fprintf(stderr, "bad --property-bytes\n");
                return 2;
            }
        } else if (strcmp(a, "--shape") == 0) {
            const char *v = NEXT();
            if (strcmp(v, "whole") == 0) {
                c.shape = SHAPE_WHOLE;
            } else if (strcmp(v, "chunked") == 0) {
                c.shape = SHAPE_CHUNKED;
            } else {
                fprintf(stderr, "bad --shape\n");
                return 2;
            }
        } else if (strcmp(a, "--distribution") == 0) {
            const char *v = NEXT();
            if (strcmp(v, "remote-uniform") == 0) {
                c.dist = DIST_REMOTE_UNIFORM;
            } else if (strcmp(v, "gauntlet") == 0) {
                c.dist = DIST_GAUNTLET;
            } else {
                fprintf(stderr, "bad --distribution\n");
                return 2;
            }
        } else if (strcmp(a, "--format") == 0) {
            const char *v = NEXT();
            if (strcmp(v, "csv") == 0) {
                c.fmt = FMT_CSV;
            } else if (strcmp(v, "json") == 0) {
                c.fmt = FMT_JSON;
            } else {
                fprintf(stderr, "bad --format\n");
                return 2;
            }
        } else if (strcmp(a, "--endpoint-skew-fraction") == 0) {
            if (!parse_frac(NEXT(), &c.skew_fraction)) {
                fprintf(stderr, "bad --endpoint-skew-fraction\n");
                return 2;
            }
        } else if (strcmp(a, "--lateness-fraction") == 0) {
            if (!parse_frac(NEXT(), &c.lateness_fraction)) {
                fprintf(stderr, "bad --lateness-fraction\n");
                return 2;
            }
        } else if (strcmp(a, "--campaign") == 0) {
            const char *v = NEXT();
            if (strcmp(v, "ceiling") == 0) {
                c.campaign = CAMPAIGN_CEILING;
            } else if (strcmp(v, "knee") == 0) {
                c.campaign = CAMPAIGN_KNEE;
            } else {
                fprintf(stderr, "bad --campaign\n");
                return 2;
            }
        } else if (strcmp(a, "--lambda") == 0) {
            const char *v = NEXT();
            char       *end = NULL;
            c.lambda = strtod(v, &end);
            if (*end != '\0' || !isfinite(c.lambda) || !(c.lambda > 0.0)) {
                fprintf(stderr, "bad --lambda\n");
                return 2;
            }
        } else if (strcmp(a, "--duration-ms") == 0) {
            if (!parse_u32(NEXT(), &c.duration_ms) || c.duration_ms < 1) {
                fprintf(stderr, "bad --duration-ms\n");
                return 2;
            }
        } else if (strcmp(a, "--rate-tolerance") == 0) {
            if (!parse_frac(NEXT(), &c.rate_tolerance)) {
                fprintf(stderr, "bad --rate-tolerance\n");
                return 2;
            }
        } else {
            fprintf(stderr, "unknown argument: %s\n", a);
            usage(argv[0]);
            return 2;
        }
#undef NEXT
    }

    /* Range coherence. */
    if (c.dist == DIST_REMOTE_UNIFORM && c.K < 2) {
        fprintf(stderr, "remote-uniform requires --shards >= 2\n");
        return 2;
    }
    /* The knee campaign needs a positive offered rate (--lambda) and a fixed
     * timed window (--duration-ms); those are the open-loop schedule. */
    if (c.campaign == CAMPAIGN_KNEE && !(c.lambda > 0.0)) {
        fprintf(stderr, "--campaign knee requires --lambda > 0\n");
        return 2;
    }

    /* Retention fit: the whole produced set (warmup + window + in-flight cap,
     * all retained in one group during a valid run) must fit ALL THREE resolved
     * source-log ceilings — byte budget, per-group record count, and (chunked)
     * the OPEN-record chunk-node pool — or the run would evict undelivered
     * records (loss). Resolve the chunk-node pool via the shared
     * moqr_log_capacity_describe; compute in checked uint64 so a huge --cap
     * refuses cleanly here instead of wrapping into a long stall. */
    {
        const char *rr = sat_retention_reason(&c);
        if (rr != NULL) {
            fprintf(stderr,
                    "config exceeds retention (%s): warmup+window+cap objects "
                    "must fit the fixed one-group source log (%u records, "
                    "%llu bytes, resolver-bounded chunk nodes)\n",
                    rr, SAT_LOG_OBJECTS_PER_GROUP,
                    (unsigned long long)SAT_LOG_BUDGET_BYTES);
            return 2;
        }
    }

    if (c.verify) {
        return sat_verify();
    }

    /* Affinity: canonical only where every lane can pin to a distinct CPU. */
    int base_cpu = -1;
    if (sat_ncpu() >= (long)c.K) {
        /* Probe: try to pin the main thread to CPU 0. */
        if (sat_pin_self(0)) {
            base_cpu = 0;
        }
    }
    if (c.require_affinity && base_cpu < 0) {
        fprintf(stderr,
                "affinity required but unavailable (need >= %u distinct CPUs "
                "with strict pthread affinity; this platform/host does not "
                "provide it)\n",
                c.K);
        return 3;
    }

    sat_result_t o;
    if (!sat_run_one(&c, base_cpu, &o)) {
        fprintf(stderr, "run failed: %s\n", o.invalid_reason);
        return 1;
    }
    if (!sat_emit(&c, &o, true)) {
        return 1;
    }
    return 0;
}
