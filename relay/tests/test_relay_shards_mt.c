/*
 * Concurrent per-shard stepping over the cross-shard channels: threads each
 * own one shard and free-run churn through the per-shard step seam
 * (moqr_shards_debug_step_shard) under LIVE inbound visibility — no shared
 * round, no coordination beyond the mailboxes' and demand channels' own leaf
 * mutexes. The control case exercises the coalescing mailboxes (latest-wins
 * slots, canon ownership handoff: producer allocates, consumer frees); the
 * data case streams whole-object messages whose rcbuf CLONES cross with the
 * message (sole-reference handoff, released by the consumer or at destroy),
 * racing a sustained control churn over the same 1-entry channels — the
 * sticky arbiter's adversarial shape. All under real contention.
 *
 * Assertions are fixed-point equivalence, not schedules: after the threads
 * join and the runtime settles, every surviving namespace must hold exactly
 * the same journal candidates / winner / mirror on both shards as a fresh
 * DETERMINISTIC runtime given the same final announces — and no pending
 * self-echo, no pending mailbox state, and a balanced allocator may remain.
 * Trace hashes and route epochs are deliberately NOT compared: they count
 * churn, and churn depends on the thread schedule.
 *
 * Run under fatal ASan/UBSan and TSan: with the mailbox mutex removed, the
 * producer's in-place coalesce races the consumer's drain and TSan reports it.
 * Everything that crosses shard threads — canonical control keys, demand
 * messages, and rcbuf clones — travels through the shared THREAD-SAFE
 * allocator (producer allocates, consumer frees), which the counting
 * allocator here models with its own mutex.
 */

#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/rcbuf.h>

#include <pthread.h>
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../../tests/unit/test_support.h"

/* Thread-safe counting allocator: malloc-backed, counters under a mutex (the
 * runtime's mailbox memory crosses shard threads with the message). */
typedef struct ca {
    moq_alloc_t     vt;
    pthread_mutex_t mu;
    long            allocs, frees, live;
} ca_t;

static moqr_reset_desc_t
rd_wire(uint64_t code)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_wire(MOQ_VERSION_DRAFT_16, code, &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}

static void *
ca_a(size_t n, void *c)
{
    ca_t *a = c;
    void *p = malloc(n);
    if (p != NULL) {
        pthread_mutex_lock(&a->mu);
        a->allocs++;
        a->live += (long)n;
        pthread_mutex_unlock(&a->mu);
    }
    return p;
}

static void *
ca_r(void *p, size_t old_n, size_t new_n, void *c)
{
    ca_t *a = c;
    void *q = realloc(p, new_n);
    if (q != NULL) {
        pthread_mutex_lock(&a->mu);
        a->live += (long)new_n - (long)old_n;
        pthread_mutex_unlock(&a->mu);
    }
    return q;
}

static void
ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p != NULL) {
        pthread_mutex_lock(&a->mu);
        a->frees++;
        a->live -= (long)n;
        pthread_mutex_unlock(&a->mu);
        free(p);
    }
}

static void
ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    pthread_mutex_init(&a->mu, NULL);
    a->vt.ctx = a;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
}

enum { MT_SHARDS = 2, MT_NS = 4, MT_ITERS = 300 };

/* Per-thread world: written only by its own thread; read after join. */
typedef struct mt_thread {
    moqr_shards_t *s;
    uint16_t       shard;
    moqr_binding_t pub;
    int            errs;       /* op failures / impossible masks           */
} mt_thread_t;

static void
mt_ns_name(char *buf, size_t n, uint16_t shard, int j)
{
    snprintf(buf, n, "mt%u_%d", (unsigned)shard, j);
}

static void *
mt_run(void *arg)
{
    mt_thread_t *t = arg;
    /* Legal wake destinations: the peer (push/credit) and — when a step
     * applied inbound data into its own core — the shard itself (the ONE
     * coalesced local continuation). Anything else is impossible. */
    uint64_t legal_mask = (1ull << (1u - t->shard)) | (1ull << t->shard);
    for (int it = 0; it < MT_ITERS; it++) {
        for (int j = 0; j < MT_NS; j++) {
            char nm[24];
            mt_ns_name(nm, sizeof(nm), t->shard, j);
            moq_bytes_t part = { (const uint8_t *)nm, (uint32_t)strlen(nm) };
            moqr_ns_t ns = { &part, 1 };
            if (moqr_core_announce(moqr_shards_core(t->s, t->shard), t->pub,
                                   ns) != MOQR_OK) {
                t->errs++;
            }
        }
        for (int r = 0; r < 3; r++) {
            uint64_t mask = 0;
            if (moqr_shards_debug_step_shard(t->s, t->shard, 1000, &mask) !=
                MOQR_OK) {
                t->errs++;   /* the seam reported a bind-pump error */
            }
            if ((mask & ~legal_mask) != 0) {
                t->errs++;   /* pushed to an impossible destination */
            }
        }
        /* Cross-shard demand churn: subscribe a PEER namespace (a forwarded
         * demand when its mirror exists; a local reject when it does not —
         * both are valid schedules), then leave. The refusal round-trip may
         * retire the subscription first, so STALE on unsubscribe is a legal
         * race outcome, not an error. */
        {
            char pnm[24];
            mt_ns_name(pnm, sizeof(pnm), (uint16_t)(1u - t->shard), 0);
            moq_bytes_t ppart = { (const uint8_t *)pnm,
                                  (uint32_t)strlen(pnm) };
            moqr_subscribe_req_t rq;
            moqr_subscribe_req_init(&rq);
            rq.ns = (moqr_ns_t){ &ppart, 1 };
            rq.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
            rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
            rq.cookie = 77;
            moqr_sub_t xsub;
            if (moqr_core_subscribe(moqr_shards_core(t->s, t->shard), t->pub,
                                    &rq, &xsub) != MOQR_OK) {
                t->errs++;
            } else {
                uint64_t mask = 0;
                if (moqr_shards_debug_step_shard(t->s, t->shard, 1000,
                                                 &mask) != MOQR_OK) {
                    t->errs++;
                }
                moqr_result_t ur = moqr_core_unsubscribe(
                    moqr_shards_core(t->s, t->shard), xsub, 1000);
                if (ur != MOQR_OK && ur != MOQR_ERR_STALE_HANDLE) {
                    t->errs++;
                }
            }
        }
        /* Withdraw everything except on the FINAL iteration, so the settled
         * end state is all-announced (the fixed point the main thread pins). */
        if (it == MT_ITERS - 1) {
            break;
        }
        for (int j = 0; j < MT_NS; j++) {
            char nm[24];
            mt_ns_name(nm, sizeof(nm), t->shard, j);
            moq_bytes_t part = { (const uint8_t *)nm, (uint32_t)strlen(nm) };
            moqr_ns_t ns = { &part, 1 };
            if (moqr_core_unannounce(moqr_shards_core(t->s, t->shard), t->pub,
                                     ns) != MOQR_OK) {
                t->errs++;
            }
        }
        for (int r = 0; r < 3; r++) {
            uint64_t mask = 0;
            if (moqr_shards_debug_step_shard(t->s, t->shard, 1000, &mask) !=
                MOQR_OK) {
                t->errs++;   /* the seam reported a bind-pump error */
            }
            if ((mask & ~legal_mask) != 0) {
                t->errs++;
            }
        }
    }
    return NULL;
}

/* Build a runtime with every "mt<i>_<j>" namespace announced, in barrier mode,
 * stepped to convergence: the deterministic reference fixed point. */
static moqr_shards_t *
mt_reference(ca_t *a, moqr_binding_t *pubs)
{
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.shards = MT_SHARDS;
    moqr_shards_t *s = NULL;
    if (moqr_shards_create(&cfg, &s) != MOQR_OK) {
        return NULL;
    }
    for (uint16_t i = 0; i < MT_SHARDS; i++) {
        (void)moqr_core_binding_open(moqr_shards_core(s, i), 1, &pubs[i]);
        for (int j = 0; j < MT_NS; j++) {
            char nm[24];
            mt_ns_name(nm, sizeof(nm), i, j);
            moq_bytes_t part = { (const uint8_t *)nm, (uint32_t)strlen(nm) };
            moqr_ns_t ns = { &part, 1 };
            (void)moqr_core_announce(moqr_shards_core(s, i), pubs[i], ns);
        }
    }
    for (int r = 0; r < 10; r++) {
        (void)moqr_shards_step(s, 1000);
    }
    return s;
}

static void
mt_jinfo(moqr_shards_t *s, uint16_t shard, uint16_t owner, int j,
         moqr_shards_jinfo_t *out)
{
    char nm[24];
    mt_ns_name(nm, sizeof(nm), owner, j);
    moq_bytes_t part = { (const uint8_t *)nm, (uint32_t)strlen(nm) };
    moqr_shards_debug_journal(s, shard, &part, 1, out);
}

/* Threaded data-pump churn: shard 0 (owner) free-runs producing objects for
 * an acknowledged remote demand while shard 1 (requester) free-runs draining
 * them AND churning fresh control demands against the same 1-entry channels
 * — the adversarial shape for the sticky arbiter (each thread's drain timing
 * is uncoordinated, so a schedule-position arbiter would let fresh control
 * permanently outrace data). No timing claims: the assertions are eventual
 * progress of BOTH classes and an exact, leak-free fixed point after join. */
#define MTD_OBJS 48

typedef struct mtd_thread {
    moqr_shards_t *s;
    moqr_track_t   track;      /* owner: the published ACTIVE track       */
    moqr_binding_t bind;
    ca_t          *alloc;
    uint16_t       shard;
    int            produced;   /* owner: objects accepted by ingest        */
    int            errs;
} mtd_thread_t;

static void *
mtd_owner_run(void *arg)
{
    mtd_thread_t *t = arg;
    for (int o = 0; o < MTD_OBJS; o++) {
        uint8_t buf[16];
        memset(buf, (uint8_t)o, sizeof(buf));
        moq_rcbuf_t *pl = NULL;
        if (moq_rcbuf_create(&t->alloc->vt, buf, sizeof(buf), &pl) != 0) {
            t->errs++;
            break;
        }
        moqr_log_append_desc_t d;
        moqr_log_append_desc_init(&d);
        d.group_id = 0;
        d.subgroup_id = 0;
        d.object_id = (uint64_t)o;
        d.publisher_priority = 128;
        d.payload = pl;
        d.now_us = 1;
        if (moqr_core_ingest(moqr_shards_core(t->s, 0), t->track, &d) ==
            MOQR_OK) {
            t->produced++;
        } else {
            moq_rcbuf_decref(pl);
            t->errs++;
        }
        for (int r = 0; r < 2; r++) {
            if (moqr_shards_debug_step_shard(t->s, 0, 1000, NULL) !=
                MOQR_OK) {
                t->errs++;
            }
        }
    }
    /* Keep stepping so the tail of the backlog drains under contention. */
    for (int r = 0; r < 4 * MTD_OBJS; r++) {
        if (moqr_shards_debug_step_shard(t->s, 0, 1000, NULL) != MOQR_OK) {
            t->errs++;
        }
    }
    return NULL;
}

static void *
mtd_requester_run(void *arg)
{
    mtd_thread_t *t = arg;
    for (int it = 0; it < 2 * MTD_OBJS; it++) {
        /* Sustained fresh control: a demand on the second ACTIVE namespace,
         * acked then cancelled (STALE is a legal race outcome). */
        moq_bytes_t part = { (const uint8_t *)"mtdB", 4 };
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = (moqr_ns_t){ &part, 1 };
        rq.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 91;
        moqr_sub_t xsub;
        if (moqr_core_subscribe(moqr_shards_core(t->s, 1), t->bind, &rq,
                                &xsub) == MOQR_OK) {
            if (moqr_shards_debug_step_shard(t->s, 1, 1000, NULL) !=
                MOQR_OK) {
                t->errs++;
            }
            moqr_result_t ur = moqr_core_unsubscribe(
                moqr_shards_core(t->s, 1), xsub, 1000);
            if (ur != MOQR_OK && ur != MOQR_ERR_STALE_HANDLE) {
                t->errs++;
            }
        }
        for (int r = 0; r < 2; r++) {
            if (moqr_shards_debug_step_shard(t->s, 1, 5000, NULL) !=
                MOQR_OK) {
                t->errs++;
            }
        }
    }
    return NULL;
}

static int
mtd_data_case(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.shards = 2;
    cfg.admit_remote_demand = true;
    cfg.demand_channel_entries = 1;   /* the contended shape */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);

    /* Single-threaded setup to a settled ACKED demand (barrier mode). */
    moqr_binding_t p0, d1;
    MOQ_TEST_CHECK(moqr_core_binding_open(moqr_shards_core(s, 0), 1, &p0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(moqr_shards_core(s, 1), 2, &d1) ==
                   MOQR_OK);
    moq_bytes_t pa = { (const uint8_t *)"mtdA", 4 };
    moq_bytes_t pb = { (const uint8_t *)"mtdB", 4 };
    moqr_ns_t nsa = { &pa, 1 };
    moqr_ns_t nsb = { &pb, 1 };
    MOQ_TEST_CHECK(moqr_core_announce(moqr_shards_core(s, 0), p0, nsa) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(moqr_shards_core(s, 0), p0, nsb) ==
                   MOQR_OK);
    moq_bytes_t name = { (const uint8_t *)"v", 1 };
    moqr_track_t ta, tb;
    MOQ_TEST_CHECK(moqr_core_publish_open(moqr_shards_core(s, 0), p0, nsa,
                                          name, 900, &ta) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_publish_open(moqr_shards_core(s, 0), p0, nsb,
                                          name, 901, &tb) == MOQR_OK);
    (void)tb;
    for (int r = 0; r < 10; r++) {
        (void)moqr_shards_step(s, 1000);
    }
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = nsa;
    rq.name = name;
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 90;
    moqr_sub_t data_sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(moqr_shards_core(s, 1), d1, &rq,
                                       &data_sub) == MOQR_OK);
    for (int r = 0; r < 8; r++) {
        (void)moqr_shards_step(s, 1000);
    }
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);   /* ACKED before the threads */

    moqr_shards_debug_set_live_visibility(s, true);
    mtd_thread_t ow = { s, ta, p0, &a, 0, 0, 0 };
    mtd_thread_t rq2 = { s, ta, d1, &a, 1, 0, 0 };
    pthread_t t0, t1;
    MOQ_TEST_CHECK(pthread_create(&t0, NULL, mtd_owner_run, &ow) == 0);
    MOQ_TEST_CHECK(pthread_create(&t1, NULL, mtd_requester_run, &rq2) == 0);
    MOQ_TEST_CHECK(pthread_join(t0, NULL) == 0);
    MOQ_TEST_CHECK(pthread_join(t1, NULL) == 0);
    MOQ_TEST_CHECK_EQ_INT(ow.errs, 0);
    MOQ_TEST_CHECK_EQ_INT(rq2.errs, 0);
    MOQ_TEST_CHECK_EQ_INT(ow.produced, MTD_OBJS);

    /* Quiesced settle, then the fixed point: EVERY produced object crossed
     * (data progressed under sustained control), the churned demands all
     * resolved (control progressed under data), and nothing leaks. */
    moqr_shards_debug_set_live_visibility(s, false);
    for (int r = 0; r < 4 * MTD_OBJS; r++) {
        (void)moqr_shards_step(s, 5000);
    }
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, MTD_OBJS);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);
    for (uint16_t i = 0; i < 2; i++) {
        for (uint16_t d = 0; d < 2; d++) {
            MOQ_TEST_CHECK_EQ_U64(
                moqr_shards_debug_demand_channel_pending(s, i, d), 0);
        }
    }
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        printf("PASS: mt data pump churn\n");
    }
    return failures;
}

/* Threaded terminal-plane churn: the owner free-runs the FULL data
 * vocabulary — live-edge OBJ_OPEN/CHUNK/END, one mid-stream reset with a
 * 62-bit code, group eviction (watermark + group reset), and a final SEAL —
 * while the requester free-runs draining it, churning fresh control demands
 * over the same cap-1 channels, and cancelling a second demand while its
 * object is still open. Schedule-independent fixed points only, after join:
 * exact accepted objects and retained bytes, the sealed/unsealed pull
 * outcomes, zero open progress on both sides, quiet cancellation (no loss
 * metric), drained channels/mailboxes, and a balanced allocator. */
#define MTT_GROUPS 6
#define MTT_ABANDON_GROUP 2
#define MTT_WIDE_CODE 0x3456789ABCDEF0ull

/* Bounded fail-safe against a genuine hang.
 *
 * The workers pace each other by WORK, never by spin count: on an
 * oversubscribed host a thread can execute an unbounded number of empty turns
 * before its peer is scheduled at all, so "I looped N times and you had not
 * finished" describes the OS scheduler, not the relay. Only elapsed monotonic
 * time ends a run early, so a real stall is reported here with counts instead
 * of being killed from outside. Whichever thread trips it publishes `aborted`,
 * so neither can spin on a peer that has already stopped and both always join.
 *
 * The budget is EXECUTABLE-level, not per case. CTest bounds the whole binary
 * (TIMEOUT 60), so N stalled cases must not be able to spend N budgets: the
 * deadline is measured from process start, which keeps the worst case at one
 * budget no matter how many cases are added later. 25s against a 60s contract
 * leaves >2x headroom, against an observed green runtime of well under 2s even
 * at 12-way process concurrency. */
#define MTT_EXEC_FAILSAFE_MS 25000u
static uint64_t g_mtt_exec_start_ms;

typedef struct mtt_gate {
    int aborted;   /* __atomic release/acquire between the two workers */
} mtt_gate_t;

static uint64_t
mtt_now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

/* Release both workers now: the run cannot reach its end state, so waiting out
 * the deadline would only delay the report. */
static void
mtt_gate_abort(mtt_gate_t *g)
{
    __atomic_store_n(&g->aborted, 1, __ATOMIC_RELEASE);
}

/* true => keep going; false => the fail-safe fired here or in the peer. */
static bool
mtt_gate_ok(mtt_gate_t *g)
{
    if (__atomic_load_n(&g->aborted, __ATOMIC_ACQUIRE) != 0) {
        return false;
    }
    if (mtt_now_ms() - g_mtt_exec_start_ms >= MTT_EXEC_FAILSAFE_MS) {
        __atomic_store_n(&g->aborted, 1, __ATOMIC_RELEASE);
        return false;
    }
    return true;
}

/* Fixture controls (off by default), each making one race deterministic
 * instead of waiting for ambient host load to expose it. */
#define MTT_RETIRED_CEILING 20000
static int g_mtt_skew;           /* hold the requester past a spin ceiling  */
static int g_mtt_cancel_skew;    /* hold the owner past the cancel point    */
static int g_mtt_stale_cancel;   /* retire nsC before the armed cancel runs  */
static int g_mtt_cancel_fail;    /* same, but through the ORDINARY policy    */
static int g_mtt_owner_rounds;   /* __atomic: owner empty-extraction rounds */

typedef struct mtt_thread {
    moqr_shards_t *s;
    moqr_track_t   ta;         /* owner: the streamed nsA track            */
    moqr_track_t   tc;         /* owner: the cancel-target nsC track       */
    moqr_binding_t bind;
    ca_t          *alloc;
    moqr_sub_t     csub;       /* requester: the nsC sub to cancel         */
    int           *owner_done; /* shared: the requester must keep popping
                                * the cap-1 channels until the owner's
                                * paced extraction finishes               */
    int           *nsc_ready;  /* shared: the owner has durably enqueued the
                                * nsC OBJ_OPEN and its first chunk (its own
                                * pump_messages floor, published from the
                                * owning lane)                            */
    int           *req_at_cancel; /* shared: the requester reached the churn
                                   * point where cancellation is armed     */
    mtt_gate_t    *gate;
    int            stale_guard_held; /* a stale result did NOT satisfy the
                                      * once-only cancellation guard        */
    int            fail_primed;      /* fail-fast arm: nsC retired out of band */
    int            errs;
} mtt_thread_t;

/* -- test-only dump/stats ownership instrumentation -------------------------
 * The signed discipline: route/journal dumps and stats snapshots run only
 * inside the OWNING LANE; the coordinator touches nothing but published
 * rows. Production cannot pin a lane to one OS thread (a lane is a
 * serialized lock domain that may migrate threads), but THIS HARNESS gives
 * each logical role exactly one thread — so thread identity identifies the
 * role here, and every dump/stats call in the MT cases goes through these
 * guards. A call under the wrong role is refused BY THE HARNESS (counted,
 * never forwarded into the runtime), which keeps coordinator traversal a
 * deterministic test failure; TSan over the churn cases remains the
 * systemic net for unguarded access. */
static pthread_t g_lane_thread[MT_SHARDS];
static int       g_lanes_registered;   /* __atomic; guards activate at 2 */

static void
tg_register_lane(uint16_t shard)
{
    g_lane_thread[shard] = pthread_self();
    __atomic_add_fetch(&g_lanes_registered, 1, __ATOMIC_ACQ_REL);
    while (__atomic_load_n(&g_lanes_registered, __ATOMIC_ACQUIRE) <
           MT_SHARDS) {
        sched_yield();   /* both roles bound before any guarded call */
    }
}

static void
tg_roles_off(void)
{
    __atomic_store_n(&g_lanes_registered, 0, __ATOMIC_RELEASE);
}

static bool
tg_role_ok(uint16_t shard)
{
    if (__atomic_load_n(&g_lanes_registered, __ATOMIC_ACQUIRE) < MT_SHARDS) {
        return true;   /* roles inactive: single-threaded / post-join mode */
    }
    return pthread_equal(pthread_self(), g_lane_thread[shard]) != 0;
}

static moqr_result_t
tg_get_stats(moqr_shards_t *s, uint16_t shard, moqr_shards_stats_t *out,
             int *viol)
{
    if (!tg_role_ok(shard)) {
        (*viol)++;   /* refused by the harness: the runtime is never entered */
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_shards_get_stats(s, shard, out);
}

static moqr_result_t
tg_journal_dump(moqr_shards_t *s, uint16_t shard, char *buf, size_t cap,
                size_t *w, int *viol)
{
    if (!tg_role_ok(shard)) {
        (*viol)++;
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_shards_journal_dump_text(s, shard, buf, cap, w);
}

static moqr_result_t
tg_route_dump(moqr_shards_t *s, uint16_t shard, char *buf, size_t cap,
              size_t *w, int *viol)
{
    if (!tg_role_ok(shard)) {
        (*viol)++;
        return MOQR_ERR_WRONG_STATE;
    }
    return moqr_core_route_dump_text(moqr_shards_core(s, shard), buf, cap,
                                     w);
}

static bool
mtt_step(moqr_shards_t *s, uint16_t shard, uint64_t now)
{
    uint64_t mask = 0;
    if (moqr_shards_debug_step_shard(s, shard, now, &mask) != MOQR_OK) {
        return false;
    }
    /* K=2: pushes and producer credits name the peer; a step that applied
     * inbound data may also name ITSELF (the one coalesced local
     * continuation). Nothing else is possible. */
    return (mask & ~((1ull << (1u - shard)) | (1ull << shard))) == 0;
}

static bool
mtt_chunk(mtt_thread_t *t, moqr_track_t th, uint64_t g, uint64_t sg,
          uint64_t o, uint8_t fill)
{
    uint8_t buf[16];
    memset(buf, fill, sizeof(buf));
    moq_rcbuf_t *cb = NULL;
    if (moq_rcbuf_create(&t->alloc->vt, buf, sizeof(buf), &cb) != 0) {
        return false;
    }
    moqr_result_t rc =
        moqr_core_append_chunk(moqr_shards_core(t->s, 0), th, g, sg, o, cb);
    moq_rcbuf_decref(cb);
    return rc == MOQR_OK;
}

/* Pace the producer on its OWN pump counter (written only by this thread's
 * stepper, so the read is single-writer safe): step until at least `min`
 * messages have been extracted. Keeps production from outrunning extraction
 * into the eviction window — losses would be loss-VISIBLE but would make the
 * post-join counts schedule-dependent. Terminal messages only add, so a
 * floor comparison stays schedule-independent. */
static bool
mtt_wait_msgs(mtt_thread_t *t, uint64_t min)
{
    uint64_t last_msgs = UINT64_MAX;
    for (;;) {
        uint64_t turns = 0, msgs = 0, bytes = 0;
        moqr_shards_debug_pump_counters(t->s, 0, &turns, &msgs, &bytes);
        /* The production stats snapshot, read MID-RUN from the owning lane
         * (the allowed pattern): its pump counters agree with the debug
         * accessors — both are this thread's own single-writer state — and
         * the inbound occupancy reads ride the channel leaf mutexes while
         * the peer lane pushes concurrently. */
        moqr_shards_stats_t st;
        int viol = 0;
        if (tg_get_stats(t->s, 0, &st, &viol) != MOQR_OK || viol != 0 ||
            st.pump_messages != msgs || st.pump_turns != turns ||
            st.pump_bytes != bytes) {
            return false;
        }
        if (msgs >= min) {
            return true;
        }
        if (!mtt_gate_ok(t->gate)) {
            return false;   /* a real stall, measured in time, not in turns */
        }
        if (!mtt_step(t->s, 0, 1000)) {
            return false;
        }
        if (msgs == last_msgs) {
            /* An empty turn: the peer lane owns the drain of these cap-1
             * channels, so give it the CPU rather than burn a budget. */
            __atomic_fetch_add(&g_mtt_owner_rounds, 1, __ATOMIC_RELAXED);
            sched_yield();
        }
        last_msgs = msgs;
    }
}

static void *
mtt_owner_run(void *arg)
{
    mtt_thread_t *t = arg;
    tg_register_lane(0);
    if (g_mtt_cancel_skew) {
        /* Let the requester reach the cancellation point before nsC exists at
         * all: the ordering the handshake has to survive. */
        while (__atomic_load_n(t->req_at_cancel, __ATOMIC_ACQUIRE) == 0 &&
               mtt_gate_ok(t->gate)) {
            sched_yield();
        }
    }
    moqr_core_t *c0 = moqr_shards_core(t->s, 0);
    uint64_t floor_msgs = 0;
    /* nsC: one long-lived open object — the cancellation target. */
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 0;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 128;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = 4096;
    d.now_us = 1;
    if (moqr_core_ingest(c0, t->tc, &d) != MOQR_OK ||
        !mtt_chunk(t, t->tc, 0, 0, 0, 0xC0)) {
        t->errs++;
    }
    floor_msgs += 2;   /* nsC OBJ_OPEN + its first chunk */
    if (!mtt_wait_msgs(t, floor_msgs)) {
        t->errs++;   /* floor NOT met: the handshake must stay unarmed */
    } else {
        /* Published from the owning lane, and only on success:
         * pump_messages has passed the nsC floor, so the object the requester
         * is about to cancel really is on the wire. */
        __atomic_store_n(t->nsc_ready, 1, __ATOMIC_RELEASE);
    }
    for (uint64_t g = 0; g < MTT_GROUPS; g++) {
        moqr_log_append_desc_init(&d);
        d.group_id = g;
        d.subgroup_id = 0;
        d.object_id = 0;
        d.publisher_priority = 128;
        d.obj_state = MOQR_OBJ_OPEN;
        d.declared_len = (g == MTT_ABANDON_GROUP) ? 4096 : 32;
        d.now_us = 1;
        if (moqr_core_ingest(c0, t->ta, &d) != MOQR_OK) {
            t->errs++;
            continue;
        }
        if (!mtt_chunk(t, t->ta, g, 0, 0, (uint8_t)g)) {
            t->errs++;
        }
        for (int r = 0; r < 2; r++) {
            if (!mtt_step(t->s, 0, 1000)) {
                t->errs++;
            }
        }
        if (g == MTT_ABANDON_GROUP) {
            /* The mid-stream reset, with a 62-bit code: OPEN + one chunk
             * crossed (waited above), OBJ_RESET follows. */
            floor_msgs += 2;
            if (!mtt_wait_msgs(t, floor_msgs)) {
                t->errs++;
            }
            if (moqr_core_abandon_record(c0, t->ta, g, 0, 0,
                                         rd_wire(MTT_WIDE_CODE)) != MOQR_OK) {
                t->errs++;
            }
            floor_msgs += 1;   /* the OBJ_RESET */
        } else {
            if (!mtt_chunk(t, t->ta, g, 0, 0, (uint8_t)g) ||
                moqr_core_complete_record(c0, t->ta, g, 0, 0) != MOQR_OK) {
                t->errs++;
            }
            floor_msgs += 4;   /* OBJ_OPEN + 2 chunks + OBJ_END */
        }
        /* Fully extracted before the next group can evict this one: the
         * clones own the bytes from here, so later owner eviction cannot
         * un-deliver anything. */
        if (!mtt_wait_msgs(t, floor_msgs)) {
            t->errs++;
        }
    }
    /* The late FIN on the final (retained) group. */
    if (moqr_core_seal_subgroup(c0, t->ta, MTT_GROUPS - 1, 0) != MOQR_OK) {
        t->errs++;
    }
    floor_msgs += 1;   /* the SG_SEAL */
    if (!mtt_wait_msgs(t, floor_msgs)) {
        t->errs++;
    }
    __atomic_store_n(t->owner_done, 1, __ATOMIC_RELEASE);
    return NULL;
}

/* Fire the armed nsC cancellation, but only once the open object it is meant
 * to interrupt demonstrably exists on BOTH sides:
 *   - the owner published its nsC pump floor (OBJ_OPEN + first chunk enqueued);
 *   - shard 1 holds requester open-object state for it;
 *   - the directed 0->1 demand channel has drained after a requester step,
 *     so nothing is still in flight toward this lane.
 * Cancelling earlier makes the owner's second message moot and leaves its
 * extraction floor permanently unreachable — an ordering race in the fixture,
 * not a stall in the relay. The cancellation still lands mid-object, which is
 * the case this fixture exists to cover. */
/* The armed cancellation may fire only once the open object it interrupts
 * demonstrably exists on both sides. */
static bool
mtt_cancel_ready(mtt_thread_t *t)
{
    return __atomic_load_n(t->nsc_ready, __ATOMIC_ACQUIRE) != 0 &&
           moqr_shards_debug_requester_open_objects(t->s, 1) != 0 &&
           moqr_shards_debug_demand_channel_pending(t->s, 0, 1) == 0;
}

/* Execute the cancellation. ONLY an exact MOQR_OK means it ran, and only that
 * may satisfy the once-only workload guard. Anything else — STALE_HANDLE
 * included — is a no-op that did not perform the cancellation this fixture
 * claims to exercise.
 *
 * `abort_on_fail` is the difference between a real failure and a deliberate
 * observation. The ordinary arms record ONE error and abort, releasing both
 * workers at once: the call is once-only, so retrying a result that already
 * failed cannot succeed and would merely burn the deadline. The stale-cancel
 * discriminator passes false — it EXPECTS one stale result and must not poison
 * a fixture that has already done its real work. */
static bool
mtt_cancel_exec(mtt_thread_t *t, int it, bool *cancel_done, bool abort_on_fail)
{
    moqr_result_t ur = moqr_core_unsubscribe(moqr_shards_core(t->s, 1),
                                             t->csub, 5000 + (uint64_t)it);
    if (ur == MOQR_OK) {
        *cancel_done = true;
        return true;
    }
    t->errs++;
    if (abort_on_fail) {
        mtt_gate_abort(t->gate);
    }
    return false;
}

typedef enum {
    MTT_CANCEL_WAIT = 0,   /* preconditions not met yet: keep pumping        */
    MTT_CANCEL_DONE,       /* executed exactly once, exact OK                */
    MTT_CANCEL_FAILED      /* recorded and aborted; must NOT be retried      */
} mtt_cancel_t;

static mtt_cancel_t
mtt_try_cancel(mtt_thread_t *t, int it, bool *cancel_done)
{
    if (!mtt_step(t->s, 1, 5000 + (uint64_t)it)) {
        t->errs++;
    }
    if (!mtt_cancel_ready(t)) {
        return MTT_CANCEL_WAIT;
    }
    if (g_mtt_cancel_fail && !t->fail_primed) {
        /* Retire nsC out of band HERE, immediately before the first execution
         * attempt, so the ordinary policy deterministically meets a non-OK
         * result no matter which pumping path first satisfies readiness. */
        if (moqr_core_unsubscribe(moqr_shards_core(t->s, 1), t->csub,
                                  5000 + (uint64_t)it) != MOQR_OK) {
            t->errs++;
        }
        t->fail_primed = 1;
    }
    if (mtt_cancel_exec(t, it, cancel_done, true)) {
        return MTT_CANCEL_DONE;
    }
    return MTT_CANCEL_FAILED;
}

/* Prove a stale result cannot stand in for the cancellation.
 *
 * Once the handshake is satisfied, retire the nsC demand out of band through
 * the SAME production call, so the armed cancellation that follows meets a
 * genuinely stale handle — the real MOQR_ERR_STALE_HANDLE path, with no
 * injection seam and no production edit. The armed call must refuse to mark
 * the workload complete and must count an error. The workload is unchanged:
 * nsC is still cancelled exactly once, mid-object. */
static void
mtt_stale_probe(mtt_thread_t *t, int it, bool *cancel_done)
{
    if (!mtt_step(t->s, 1, 5000 + (uint64_t)it)) {
        t->errs++;
    }
    if (!mtt_cancel_ready(t)) {
        return;
    }
    /* The real, once-only cancellation. */
    if (!mtt_cancel_exec(t, it, cancel_done, true)) {
        return;
    }
    /* The handle is now genuinely retired, so the SAME execution path meets a
     * real MOQR_ERR_STALE_HANDLE. It must neither report success nor satisfy
     * the completion predicate, and it must record the error. */
    int  before = t->errs;
    bool done = false;
    bool fired = mtt_cancel_exec(t, it, &done, false);
    t->stale_guard_held = (!fired && !done && t->errs == before + 1) ? 1 : 0;
}

static void *
mtt_requester_run(void *arg)
{
    mtt_thread_t *t = arg;
    tg_register_lane(1);
    bool cancel_armed = false;
    bool cancel_done = false;
    bool cancel_failed = false;
    if (g_mtt_skew) {
        /* Starve the owner past the retired spin ceiling before doing any
         * work: fatal to a spin budget, merely slow under a time fail-safe. */
        while (__atomic_load_n(&g_mtt_owner_rounds, __ATOMIC_RELAXED) <
                   MTT_RETIRED_CEILING + 5000 &&
               mtt_gate_ok(t->gate)) {
            sched_yield();
        }
    }
    /* Run until the owner's paced extraction completes AND this thread's own
     * churn quota is done — the cap-1 channels drain only through these
     * steps, so exiting early would starve the producer's floor waits. The
     * armed cancellation must also have fired, or the workload was not the one
     * this case claims to run. */
    for (int it = 0;; it++) {
        if (it >= 4 * MTT_GROUPS && cancel_done &&
            __atomic_load_n(t->owner_done, __ATOMIC_ACQUIRE)) {
            break;
        }
        if (!mtt_gate_ok(t->gate)) {
            t->errs++;   /* the owner really stalled: report, don't hang */
            break;
        }
        if (it >= 4 * MTT_GROUPS) {
            /* Quota done: just keep draining for the owner. */
            for (int r = 0; r < 3; r++) {
                if (!mtt_step(t->s, 1, 5000 + (uint64_t)it)) {
                    t->errs++;
                }
            }
            if (cancel_armed && !cancel_done) {
                if (g_mtt_stale_cancel) {
                    mtt_stale_probe(t, it, &cancel_done);
                } else if (mtt_try_cancel(t, it, &cancel_done) ==
                           MTT_CANCEL_FAILED) {
                    cancel_failed = true;
                    break;   /* once-only: recorded and aborted, never retried */
                }
            }
            sched_yield();   /* the owner owns the progress being waited on */
            continue;
        }
        /* Sustained fresh control on the same cap-1 channels. */
        moq_bytes_t part = { (const uint8_t *)"mttB", 4 };
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = (moqr_ns_t){ &part, 1 };
        rq.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 92;
        moqr_sub_t xsub;
        if (moqr_core_subscribe(moqr_shards_core(t->s, 1), t->bind, &rq,
                                &xsub) == MOQR_OK) {
            if (!mtt_step(t->s, 1, 5000 + (uint64_t)it)) {
                t->errs++;
            }
            moqr_result_t ur = moqr_core_unsubscribe(
                moqr_shards_core(t->s, 1), xsub, 5000 + (uint64_t)it);
            if (ur != MOQR_OK && ur != MOQR_ERR_STALE_HANDLE) {
                t->errs++;
            }
        }
        if (it == 2 * MTT_GROUPS) {
            /* ARM the cancellation at the original churn point, and tell the
             * owner this point was reached. */
            cancel_armed = true;
            __atomic_store_n(t->req_at_cancel, 1, __ATOMIC_RELEASE);
        }
        if (cancel_armed && !cancel_done) {
            if (g_mtt_stale_cancel) {
                mtt_stale_probe(t, it, &cancel_done);
            } else if (mtt_try_cancel(t, it, &cancel_done) ==
                       MTT_CANCEL_FAILED) {
                cancel_failed = true;
                break;   /* once-only: recorded and aborted, never retried */
            }
        }
        /* Own-lane observability under churn: the stats snapshot and the
         * journal dump both run HERE, on this shard's owning thread, while
         * the peer lane pushes into the shared channels — the dump-
         * ownership discipline the production CLI keeps (the coordinator
         * never traverses a live lane's journal). */
        if ((it & 7) == 0) {
            moqr_shards_stats_t st;
            if (tg_get_stats(t->s, 1, &st, &t->errs) != MOQR_OK) {
                t->errs++;
            }
            char jd[4096];
            size_t w = 0;
            moqr_result_t jr =
                tg_journal_dump(t->s, 1, jd, sizeof(jd), &w, &t->errs);
            if (jr != MOQR_OK && jr != MOQR_ERR_CAPACITY) {
                t->errs++;
            }
        }
        for (int r = 0; r < 3; r++) {
            if (!mtt_step(t->s, 1, 5000 + (uint64_t)it)) {
                t->errs++;
            }
        }
    }
    if (!cancel_done && !cancel_failed) {
        t->errs++;   /* the partial-open cancellation never ran */
    }
    return NULL;
}

static int
mtt_terminal_case(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.shards = 2;
    cfg.admit_remote_demand = true;
    cfg.demand_channel_entries = 1;
    cfg.core_cfg.log_budget.max_groups = 2;   /* the eviction pressure */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);

    /* Single-threaded setup: nsA (data), nsB (control churn), nsC (cancel
     * target), all ACTIVE on shard 0; both demands ACKED from shard 1. */
    moqr_binding_t p0, d1;
    MOQ_TEST_CHECK(moqr_core_binding_open(moqr_shards_core(s, 0), 1, &p0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(moqr_shards_core(s, 1), 2, &d1) ==
                   MOQR_OK);
    moq_bytes_t pa = { (const uint8_t *)"mttA", 4 };
    moq_bytes_t pb = { (const uint8_t *)"mttB", 4 };
    moq_bytes_t pc = { (const uint8_t *)"mttC", 4 };
    moqr_ns_t nsa = { &pa, 1 };
    moqr_ns_t nsb = { &pb, 1 };
    moqr_ns_t nsc = { &pc, 1 };
    MOQ_TEST_CHECK(moqr_core_announce(moqr_shards_core(s, 0), p0, nsa) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(moqr_shards_core(s, 0), p0, nsb) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(moqr_shards_core(s, 0), p0, nsc) ==
                   MOQR_OK);
    moq_bytes_t name = { (const uint8_t *)"v", 1 };
    moqr_track_t ta, tb, tc;
    MOQ_TEST_CHECK(moqr_core_publish_open(moqr_shards_core(s, 0), p0, nsa,
                                          name, 900, &ta) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_publish_open(moqr_shards_core(s, 0), p0, nsb,
                                          name, 901, &tb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_publish_open(moqr_shards_core(s, 0), p0, nsc,
                                          name, 902, &tc) == MOQR_OK);
    (void)tb;
    for (int r = 0; r < 10; r++) {
        (void)moqr_shards_step(s, 1000);
    }
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = nsa;
    rq.name = name;
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 93;
    moqr_sub_t data_sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(moqr_shards_core(s, 1), d1, &rq,
                                       &data_sub) == MOQR_OK);
    moqr_subscribe_req_init(&rq);
    rq.ns = nsc;
    rq.name = name;
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 94;
    moqr_sub_t c_sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(moqr_shards_core(s, 1), d1, &rq,
                                       &c_sub) == MOQR_OK);
    for (int r = 0; r < 8; r++) {
        (void)moqr_shards_step(s, 1000);
    }
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 2);   /* both ACKED pre-thread */

    moqr_shards_debug_set_live_visibility(s, true);
    int owner_done = 0;
    int nsc_ready = 0;
    int req_at_cancel = 0;
    mtt_gate_t gate = { 0 };
    __atomic_store_n(&g_mtt_owner_rounds, 0, __ATOMIC_RELAXED);
    mtt_thread_t ow = { s,  ta, tc, p0, &a, { 0 },  &owner_done,
                        &nsc_ready, &req_at_cancel, &gate, 0, 0, 0 };
    mtt_thread_t rr = { s,  ta, tc, d1, &a, c_sub, &owner_done,
                        &nsc_ready, &req_at_cancel, &gate, 0, 0, 0 };
    pthread_t t0, t1;
    uint64_t  run_start_ms = mtt_now_ms();
    MOQ_TEST_CHECK(pthread_create(&t0, NULL, mtt_owner_run, &ow) == 0);
    MOQ_TEST_CHECK(pthread_create(&t1, NULL, mtt_requester_run, &rr) == 0);
    MOQ_TEST_CHECK(pthread_join(t0, NULL) == 0);
    MOQ_TEST_CHECK(pthread_join(t1, NULL) == 0);
    uint64_t run_ms = mtt_now_ms() - run_start_ms;
    if (g_mtt_cancel_fail) {
        /* A once-only cancellation that comes back non-OK is recorded ONCE,
         * publishes the shared abort, and is never retried — so both workers
         * join promptly instead of spinning out the executable deadline. */
        printf("  cancel_fail: rr.errs=%d aborted=%d run_ms=%llu\n",
               rr.errs, __atomic_load_n(&gate.aborted, __ATOMIC_ACQUIRE),
               (unsigned long long)run_ms);
        MOQ_TEST_CHECK_EQ_INT(rr.errs, 1);          /* exactly one, no retry */
        MOQ_TEST_CHECK_EQ_INT(__atomic_load_n(&gate.aborted,
                                              __ATOMIC_ACQUIRE), 1);
        /* Both workers joined promptly. The owner's own error count is NOT
         * asserted: whether it was mid-wait when the abort landed, or had
         * already finished, is a legitimate schedule difference. Release is a
         * property of the join completing far inside the deadline. */
        MOQ_TEST_CHECK(run_ms < 5000);              /* vs a 25s deadline */
        tg_roles_off();
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
        if (failures == 0) {
            printf("PASS: mt terminal plane churn (fail-fast cancel)\n");
        }
        return failures;
    }
    MOQ_TEST_CHECK_EQ_INT(ow.errs, 0);
    if (g_mtt_stale_cancel) {
        /* Exactly the stale call's own fail-closed increment, and the guard
         * refused to treat that no-op as the cancellation. */
        MOQ_TEST_CHECK_EQ_INT(rr.errs, 1);
        MOQ_TEST_CHECK_EQ_INT(rr.stale_guard_held, 1);
    } else {
        MOQ_TEST_CHECK_EQ_INT(rr.errs, 0);
        MOQ_TEST_CHECK_EQ_INT(rr.stale_guard_held, 0);
    }
    tg_roles_off();   /* lanes joined: direct post-join reads are legal */

    /* Quiesced settle past the linger, then the fixed point. */
    moqr_shards_debug_set_live_visibility(s, false);
    for (int r = 0; r < 12 * MTT_GROUPS; r++) {
        (void)moqr_shards_step(s, 50000 + (uint64_t)r);
    }
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    /* Every group except the abandoned one completed and crossed exactly
     * once; nsC never completed anything. */
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, MTT_GROUPS - 1);
    /* The requester retains exactly the last two groups (budget 2), 32
     * payload bytes each — the cancelled demand's partial OPEN record was
     * scrubbed, never stranded WARM. */
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 64);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);   /* nsA rides; nsC is gone */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_remote_data_rejected(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_shards_debug_remote_demand_term_capacity(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_shards_debug_remote_demand_term_overrun(s, 1), 0);
    for (uint16_t i = 0; i < 2; i++) {
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, i), 0);
        for (uint16_t d2 = 0; d2 < 2; d2++) {
            MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_mailbox_pending(s, i, d2),
                                  0);
            MOQ_TEST_CHECK_EQ_U64(
                moqr_shards_debug_demand_channel_pending(s, i, d2), 0);
        }
    }
    /* Post-join direct snapshot (both lanes joined; no epoch protocol
     * needed): the owner's per-kind enqueue counters land on the workload's
     * EXACT data-message counts — nsC's open + first chunk, six group
     * opens, twelve chunks, five completions, the one wide-code reset, and
     * the one late seal — while the cap-1 channels pin the entry high-water
     * mark at exactly 1 and both wake causes fired at least once. */
    {
        moqr_shards_stats_t ss0, ss1;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &ss0) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &ss1) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(ss0.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN], 7);
        MOQ_TEST_CHECK_EQ_U64(ss0.enqueued[MOQR_SHARDS_MSG_OBJ_CHUNK], 12);
        MOQ_TEST_CHECK_EQ_U64(ss0.enqueued[MOQR_SHARDS_MSG_OBJ_END], 5);
        MOQ_TEST_CHECK_EQ_U64(ss0.enqueued[MOQR_SHARDS_MSG_OBJ_RESET], 1);
        MOQ_TEST_CHECK_EQ_U64(ss0.enqueued[MOQR_SHARDS_MSG_SG_SEAL], 1);
        MOQ_TEST_CHECK(ss0.enqueued[MOQR_SHARDS_MSG_GRP_EVICT] >= 1);
        MOQ_TEST_CHECK_EQ_U64(ss0.channel_entries_hwm, 1);
        MOQ_TEST_CHECK_EQ_U64(ss1.channel_entries_hwm, 1);
        MOQ_TEST_CHECK(ss0.wake_requests_push >= 1);
        MOQ_TEST_CHECK(ss0.wake_requests_credit >= 1);
        MOQ_TEST_CHECK(ss1.wake_requests_push >= 1);
        MOQ_TEST_CHECK(ss1.wake_requests_credit >= 1);
        MOQ_TEST_CHECK_EQ_U64(ss1.inbound_channel_entries, 0);
        MOQ_TEST_CHECK_EQ_U64(ss1.inbound_channel_bytes, 0);
    }
    /* Terminal outcomes at the requester: the sealed final group reports
     * subgroup_end; the unsealed one before it does not (notices skipped). */
    bool saw_last = false, saw_prev = false;
    for (int pulls = 0; pulls < 8; pulls++) {
        moqr_delivery_t dl;
        moqr_result_t rc = moqr_core_next_delivery(moqr_shards_core(s, 1),
                                                   d1, 90000, &dl);
        if (rc != MOQR_OK) {
            break;
        }
        moqr_delivery_outcome_t oc = MOQR_DELIVERY_DELIVERED;
        if (dl.notice == MOQR_DELIVERY_NOTICE_NONE &&
            dl.rec.obj_state == MOQR_OBJ_COMPLETE) {
            if (dl.rec.group_id == MTT_GROUPS - 1) {
                saw_last = true;
                MOQ_TEST_CHECK(dl.subgroup_end);   /* the crossed SEAL */
            } else if (dl.rec.group_id == MTT_GROUPS - 2) {
                saw_prev = true;
                MOQ_TEST_CHECK(!dl.subgroup_end);  /* unsealed neighbor */
            }
        }
        MOQ_TEST_CHECK(moqr_core_delivery_done(moqr_shards_core(s, 1), d1,
                                               oc, 90000) == MOQR_OK);
    }
    MOQ_TEST_CHECK(saw_last);
    MOQ_TEST_CHECK(saw_prev);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        printf("PASS: mt terminal plane churn%s\n",
               g_mtt_skew         ? " (scheduling skew)"
               : g_mtt_cancel_skew ? " (cancel-ordering skew)"
               : g_mtt_stale_cancel ? " (stale-cancel guard)"
                                    : "");
    }
    return failures;
}

/* -- the dump/stats ownership discipline (the permanent regression) --------- */

typedef struct own_lane {
    moqr_shards_t *s;
    uint16_t       shard;
    int           *go;        /* main sets after its refusal probes */
    int            errs;
} own_lane_t;

static void *
own_lane_run(void *arg)
{
    own_lane_t *t = arg;
    tg_register_lane(t->shard);
    /* Own-shard access works under the lane role. */
    moqr_shards_stats_t st;
    if (tg_get_stats(t->s, t->shard, &st, &t->errs) != MOQR_OK) {
        t->errs++;
    }
    char b[512];
    size_t w = 0;
    moqr_result_t rc =
        tg_journal_dump(t->s, t->shard, b, sizeof(b), &w, &t->errs);
    if (rc != MOQR_OK && rc != MOQR_ERR_CAPACITY) {
        t->errs++;
    }
    rc = tg_route_dump(t->s, t->shard, b, sizeof(b), &w, &t->errs);
    if (rc != MOQR_OK && rc != MOQR_ERR_CAPACITY) {
        t->errs++;
    }
    while (!__atomic_load_n(t->go, __ATOMIC_ACQUIRE)) {
        sched_yield();
    }
    /* The PEER's shard is refused even from another lane role. */
    int viol = 0;
    if (tg_get_stats(t->s, (uint16_t)(1u - t->shard), &st, &viol) !=
            MOQR_ERR_WRONG_STATE ||
        viol != 1) {
        t->errs++;
    }
    return NULL;
}

/* The standing gate for the dump-ownership discipline, as TEST-owned
 * instrumentation over logical roles (production lanes are serialized lock
 * domains, not pinned threads, so the runtime carries no thread affinity):
 * every dump/stats call in this suite rides the tg_* guards, lane roles
 * pass on their own shard, and the coordinator role is refused on all
 * three entry points — deterministically, before the runtime is entered.
 * Rerouting any of this suite's dumps onto the coordinator fails here
 * without needing TSan (which still nets unguarded access in the churn
 * cases). */
static int
mtt_ownership_case(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.shards = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);

    /* Roles inactive: the single-threaded caller may read directly. */
    moqr_shards_stats_t st;
    int viol = 0;
    MOQ_TEST_CHECK(tg_get_stats(s, 0, &st, &viol) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(viol, 0);

    int go = 0;
    own_lane_t l0 = { s, 0, &go, 0 };
    own_lane_t l1 = { s, 1, &go, 0 };
    pthread_t t0, t1;
    MOQ_TEST_CHECK(pthread_create(&t0, NULL, own_lane_run, &l0) == 0);
    MOQ_TEST_CHECK(pthread_create(&t1, NULL, own_lane_run, &l1) == 0);
    while (__atomic_load_n(&g_lanes_registered, __ATOMIC_ACQUIRE) <
           MT_SHARDS) {
        sched_yield();
    }

    /* The coordinator role is refused on EVERY guarded entry point — the
     * guard, not the runtime, answers, so nothing is traversed. */
    char b[512];
    size_t w = 0;
    for (uint16_t i = 0; i < 2; i++) {
        viol = 0;
        MOQ_TEST_CHECK(tg_get_stats(s, i, &st, &viol) ==
                       MOQR_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(tg_journal_dump(s, i, b, sizeof(b), &w, &viol) ==
                       MOQR_ERR_WRONG_STATE);
        MOQ_TEST_CHECK(tg_route_dump(s, i, b, sizeof(b), &w, &viol) ==
                       MOQR_ERR_WRONG_STATE);
        MOQ_TEST_CHECK_EQ_INT(viol, 3);
    }
    __atomic_store_n(&go, 1, __ATOMIC_RELEASE);
    MOQ_TEST_CHECK(pthread_join(t0, NULL) == 0);
    MOQ_TEST_CHECK(pthread_join(t1, NULL) == 0);
    MOQ_TEST_CHECK_EQ_INT(l0.errs, 0);
    MOQ_TEST_CHECK_EQ_INT(l1.errs, 0);

    /* Post-join the roles come off and direct access is legal again. */
    tg_roles_off();
    viol = 0;
    MOQ_TEST_CHECK(tg_get_stats(s, 0, &st, &viol) == MOQR_OK);
    MOQ_TEST_CHECK(tg_journal_dump(s, 0, b, sizeof(b), &w, &viol) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(tg_route_dump(s, 0, b, sizeof(b), &w, &viol) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(viol, 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        printf("PASS: mt ownership gate\n");
    }
    return failures;
}

int
main(void)
{
    g_mtt_exec_start_ms = mtt_now_ms();   /* one budget for the binary */
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.shards = MT_SHARDS;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_shards_debug_set_live_visibility(s, true);

    mt_thread_t th[MT_SHARDS];
    pthread_t tid[MT_SHARDS];
    for (uint16_t i = 0; i < MT_SHARDS; i++) {
        th[i].s = s;
        th[i].shard = i;
        th[i].errs = 0;
        MOQ_TEST_CHECK(moqr_core_binding_open(moqr_shards_core(s, i), 1,
                                              &th[i].pub) == MOQR_OK);
    }
    for (uint16_t i = 0; i < MT_SHARDS; i++) {
        MOQ_TEST_CHECK(pthread_create(&tid[i], NULL, mt_run, &th[i]) == 0);
    }
    for (uint16_t i = 0; i < MT_SHARDS; i++) {
        MOQ_TEST_CHECK(pthread_join(tid[i], NULL) == 0);
    }
    for (uint16_t i = 0; i < MT_SHARDS; i++) {
        MOQ_TEST_CHECK_EQ_INT(th[i].errs, 0);
    }

    /* Quiesced settle (single-threaded now; live visibility just means the
     * remaining mailbox state applies without waiting out a barrier). */
    for (int r = 0; r < 10; r++) {
        (void)moqr_shards_step(s, 1000);
    }

    /* Fixed point vs the deterministic reference: identical candidates /
     * winner / mirror for every namespace on every shard, no pending echoes,
     * no pending mailbox state. */
    ca_t ra;
    ca_init(&ra);
    moqr_binding_t ref_pubs[MT_SHARDS];
    moqr_shards_t *ref = mt_reference(&ra, ref_pubs);
    MOQ_TEST_CHECK(ref != NULL);
    for (uint16_t owner = 0; owner < MT_SHARDS; owner++) {
        for (int j = 0; j < MT_NS; j++) {
            for (uint16_t shard = 0; shard < MT_SHARDS; shard++) {
                moqr_shards_jinfo_t got, want;
                mt_jinfo(s, shard, owner, j, &got);
                mt_jinfo(ref, shard, owner, j, &want);
                MOQ_TEST_CHECK(got.present == want.present);
                MOQ_TEST_CHECK_EQ_U64(got.candidates, want.candidates);
                MOQ_TEST_CHECK_EQ_INT(got.winner, want.winner);
                MOQ_TEST_CHECK_EQ_INT(got.mirror, want.mirror);
            }
        }
    }
    for (uint16_t i = 0; i < MT_SHARDS; i++) {
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, i), 0);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, i), 0);
        for (uint16_t d = 0; d < MT_SHARDS; d++) {
            MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_mailbox_pending(s, i, d),
                                  0);
            MOQ_TEST_CHECK_EQ_U64(
                moqr_shards_debug_demand_channel_pending(s, i, d), 0);
        }
        moqr_core_stats_t cs;
        moqr_core_get_stats(moqr_shards_core(s, i), &cs);
        MOQ_TEST_CHECK_EQ_U64(cs.tracks, 0);   /* no PENDING/track leaks */
    }
    moqr_shards_destroy(ref);
    MOQ_TEST_CHECK_EQ_INT((int)ra.live, 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);   /* canon ownership balanced */

    failures += mtd_data_case();
    failures += mtt_terminal_case();
    /* The same fixture and the same final assertions, with each race forced
     * rather than waited for. */
    g_mtt_skew = 1;
    failures += mtt_terminal_case();
    g_mtt_skew = 0;
    g_mtt_cancel_skew = 1;
    failures += mtt_terminal_case();
    g_mtt_cancel_skew = 0;
    g_mtt_stale_cancel = 1;
    failures += mtt_terminal_case();
    g_mtt_stale_cancel = 0;
    g_mtt_cancel_fail = 1;
    failures += mtt_terminal_case();
    g_mtt_cancel_fail = 0;
    failures += mtt_ownership_case();

    if (failures == 0) {
        printf("ALL PASS\n");
    }
    return failures == 0 ? 0 : 1;
}
