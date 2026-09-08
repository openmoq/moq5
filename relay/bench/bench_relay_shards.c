/*
 * bench_relay_shards — deterministic cross-shard CADENCE, not throughput.
 *
 * The durable result is exact message counts and pump-turn bounds under the
 * deterministic runner (moqr_shards_step): no threads, no SimPair, no
 * transport, no wall clock. K=2 with a test-owned admit_remote_demand=true
 * config (production turns admission on for every multi-lane serve); shard 0
 * owns the publisher and source track, shard 1 owns ONE remote demand fanned
 * out to F=8 local subscribers. Every metric is read through
 * moqr_shards_get_stats; every assertion stays active in normal runs.
 *
 * -- The cadence model (the bound formulas) --------------------------------
 *
 * Explicit config: channel ENTRY capacity C = 4, per-turn message budget
 * M = 4 (bytes non-binding), so P = min(C, M) = 4 messages cross per push
 * round. Under the deterministic runner (ascending visit order, owner
 * first; round-barrier visibility) the pipeline alternates strictly:
 *
 *   round r   : the owner pushes P messages (the channel fills);
 *   round r+1 : the owner's data phase runs ZERO-PROGRESS (channel still
 *               holds round-r messages — the consumer, visited second,
 *               drains them later in this same round);
 *   round r+2 : the owner pushes the next P; ...
 *
 * So N data messages need R = ceil(N/P) push rounds and at most R
 * full-channel zero-progress rounds, and EVERY step with a live admitted
 * demand counts one owner pump turn (progress or not), so:
 *
 *   owner pump turns <= 2*ceil(N/P) + S
 *
 * with S = 2: the final drain round plus the driver's one convergence-check
 * step (both run with the demand still live). Workloads that pace in B
 * bursts (evict, reset) apply the same formula per burst:
 *
 *   turns <= sum_b (2*ceil(N_b/P) + S)
 *
 * Control occupancy: the ACK shares the FIFO but crosses BEFORE extraction
 * begins (data is gated on ack_sent riding the same channel), so it
 * occupies its own earlier round — counted in the setup baseline, never
 * concurrent with data occupancy. Wake requests are mask-level: each push
 * round raises exactly ONE producer->consumer push wake, each drain round
 * exactly ONE consumer->producer credit wake, so for a saturated workload
 * push wakes == credit wakes == ceil(N/P) exactly (per burst when paced).
 * Channel-entry HWM == min(P, largest burst) and the byte HWM is the
 * largest P-message window's logical bytes — both asserted exactly per
 * workload.
 */

#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/rcbuf.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* -- explicit cadence config (every bound derives from these) -------------- */

#define BR_DCH_ENTRIES 4u   /* C: directed channel entry capacity          */
#define BR_TURN_MSGS 4u     /* M: data-pump per-turn message budget        */
#define BR_P 4u             /* P = min(C, M): messages per push round      */
#define BR_SLACK 2u         /* S: final drain + convergence-check turns    */
#define BR_FANOUT 8u        /* F: requester-local subscribers              */
#define BR_WHOLE_LEN 64u    /* whole-object payload bytes                  */
#define BR_CHUNK_LEN 16u    /* chunk payload bytes                         */

/* The 62-bit abandon code (count-pinned here; the bit-exact end-to-end
 * code propagation is pinned by the deterministic shards suite). */
#define BR_WIDE_RESET 0x23456789ABCDEFull

static moqr_reset_desc_t
rd_wire(uint64_t code)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_wire(MOQ_VERSION_DRAFT_16, code, &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}

static int g_failures;

#define B_CHECK(expr)                                                     \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

/* counting allocator (single-threaded: the deterministic runner) */
typedef struct ca {
    moq_alloc_t vt;
    long        live;
} ca_t;
static void *
ca_a(size_t n, void *c)
{
    ca_t *a = c;
    void *p = malloc(n);
    if (p) {
        a->live += (long)n;
    }
    return p;
}
static void *
ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    void *q = realloc(p, n);
    if (q) {
        a->live += (long)n - (long)o;
    }
    return q;
}
static void
ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p) {
        a->live -= (long)n;
    }
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

/* -- the rig ---------------------------------------------------------------- */

typedef struct br_rig {
    ca_t          *alloc;
    moqr_shards_t *s;
    uint16_t       k;
    uint16_t       req_shard;   /* subscriber shard: 1 at K=2, 0 at K=1 */
    moqr_binding_t pub;
    moqr_binding_t subb;        /* one binding carrying the F subs      */
    moqr_track_t   track;
    moqr_sub_t     subs[BR_FANOUT];
    uint64_t       now;
} br_rig_t;

static moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

static void
br_step(br_rig_t *r)
{
    r->now += 1000;
    (void)moqr_shards_step(r->s, r->now);
}

static void
br_step_n(br_rig_t *r, int n)
{
    for (int i = 0; i < n; i++) {
        br_step(r);
    }
}

static uint32_t
br_dch_total(br_rig_t *r)
{
    uint32_t n = 0;
    for (uint16_t i = 0; i < r->k; i++) {
        for (uint16_t d = 0; d < r->k; d++) {
            n += moqr_shards_debug_demand_channel_pending(r->s, i, d);
        }
    }
    return n;
}

/* Build the rig THROUGH the announce/mirror fixed point only — publisher +
 * announced ACTIVE track on shard 0, its mirror on the requester shard — but
 * NOT the subscribe/admit round-trip. Split from br_admit so a workload can
 * baseline the COMPLETE per-kind vector (DEMAND/ACK included) across the
 * admission, then baseline again for the data-phase turn deltas. */
static bool
br_build(br_rig_t *r, ca_t *a, uint16_t k, uint32_t log_max_groups)
{
    memset(r, 0, sizeof(*r));
    r->alloc = a;
    r->k = k;
    r->req_shard = (uint16_t)(k > 1 ? 1 : 0);
    r->now = 1;
    moqr_shards_cfg_t cfg;
    moqr_shards_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.shards = k;
    cfg.admit_remote_demand = true;   /* test-owned; inert at K == 1 */
    cfg.demand_channel_entries = BR_DCH_ENTRIES;
    cfg.pump_turn_messages = BR_TURN_MSGS;
    cfg.pump_turn_bytes = 1u << 20;   /* bytes never bind these workloads */
    cfg.core_cfg.log_budget.max_groups = log_max_groups;
    cfg.core_cfg.log_budget.max_bytes = 1 << 20;
    cfg.core_cfg.linger_us = 500;
    if (moqr_shards_create(&cfg, &r->s) != MOQR_OK) {
        return false;
    }
    if (moqr_core_binding_open(moqr_shards_core(r->s, 0), 1, &r->pub) !=
        MOQR_OK) {
        return false;
    }
    moq_bytes_t nsp[1] = { B("bw") };
    moqr_ns_t ns = { nsp, 1 };
    if (moqr_core_announce(moqr_shards_core(r->s, 0), r->pub, ns) !=
        MOQR_OK) {
        return false;
    }
    if (moqr_core_publish_open(moqr_shards_core(r->s, 0), r->pub, ns,
                               B("v"), 900, &r->track) != MOQR_OK) {
        return false;
    }
    br_step_n(r, 8);   /* announce -> mirror (K=2) */
    return true;
}

/* Subscribe F local subscribers on the requester shard and step to the
 * admitted fixed point: exactly one DEMAND crosses, the owner admits and
 * ACKs, the parked subs activate. Call between the pre-admission and
 * post-admission baselines. */
static bool
br_admit(br_rig_t *r)
{
    moq_bytes_t nsp[1] = { B("bw") };
    moqr_ns_t ns = { nsp, 1 };
    if (moqr_core_binding_open(moqr_shards_core(r->s, r->req_shard), 2,
                               &r->subb) != MOQR_OK) {
        return false;
    }
    for (uint32_t f = 0; f < BR_FANOUT; f++) {
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = ns;
        rq.name = B("v");
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = f;
        if (moqr_core_subscribe(moqr_shards_core(r->s, r->req_shard),
                                r->subb, &rq, &r->subs[f]) != MOQR_OK) {
            return false;
        }
    }
    br_step_n(r, 8);   /* DEMAND -> admit -> ACK -> activation (K=2) */
    if (r->k > 1) {
        B_CHECK(moqr_shards_debug_owner_pump_subs(r->s, 0) == 1);
        B_CHECK(moqr_shards_debug_pending_demand(r->s, 1) == 1);
        B_CHECK(br_dch_total(r) == 0);
    }
    return true;
}

static void
br_destroy(br_rig_t *r)
{
    moqr_shards_destroy(r->s);
}

/* -- publishing helpers (direct owner-core ingest) --------------------------- */

static bool
br_pub_whole(br_rig_t *r, uint64_t g, uint64_t sg, uint64_t o, size_t len)
{
    uint8_t body[256];
    memset(body, (uint8_t)(0xB0 + (o & 0xF)), len);
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&r->alloc->vt, body, len, &pl) != MOQ_OK) {
        return false;
    }
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 100;
    d.payload = pl;
    d.now_us = r->now;
    moqr_result_t rc = moqr_core_ingest(moqr_shards_core(r->s, 0), r->track,
                                        &d);
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(pl);
    }
    return rc == MOQR_OK;
}

static bool
br_pub_open(br_rig_t *r, uint64_t g, uint64_t sg, uint64_t o,
            uint64_t declared)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 100;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = declared;
    d.now_us = r->now;
    return moqr_core_ingest(moqr_shards_core(r->s, 0), r->track, &d) ==
           MOQR_OK;
}

static bool
br_pub_chunk(br_rig_t *r, uint64_t g, uint64_t sg, uint64_t o, size_t len,
             uint8_t fill)
{
    uint8_t body[64];
    memset(body, fill, len);
    moq_rcbuf_t *cb = NULL;
    if (moq_rcbuf_create(&r->alloc->vt, body, len, &cb) != MOQ_OK) {
        return false;
    }
    moqr_result_t rc = moqr_core_append_chunk(moqr_shards_core(r->s, 0),
                                              r->track, g, sg, o, cb);
    moq_rcbuf_decref(cb);
    return rc == MOQR_OK;
}

/* -- delivery accounting ------------------------------------------------------ */

typedef struct br_dlv {
    uint64_t objects[BR_FANOUT];
    uint64_t bytes;      /* payload bytes across all subs          */
    uint64_t notices;    /* SEAL/EVICT notices acknowledged        */
} br_dlv_t;

/* Pull every deliverable record/notice on the subscriber binding: records
 * tally per-sub objects + payload bytes (chunked records sum their pinned
 * chunks); notices are acknowledged and counted separately. */
static void
br_pull(br_rig_t *r, br_dlv_t *dl)
{
    moqr_core_t *c = moqr_shards_core(r->s, r->req_shard);
    for (;;) {
        moqr_delivery_t d;
        moqr_result_t rc = moqr_core_next_delivery(c, r->subb, r->now, &d);
        if (rc != MOQR_OK) {
            break;
        }
        if (d.notice != MOQR_DELIVERY_NOTICE_NONE) {
            dl->notices++;
        } else {
            if (d.sub_cookie < BR_FANOUT) {
                dl->objects[d.sub_cookie]++;
            }
            if (d.rec.chunk_count > 0) {
                for (uint32_t i = 0; i < d.rec.chunk_count; i++) {
                    const moq_rcbuf_t *cb = NULL;
                    uint64_t cl = 0;
                    if (moqr_core_delivery_chunk(c, r->subb, i, &cb, &cl) ==
                        MOQR_OK) {
                        dl->bytes += cl;
                    }
                }
            } else if (d.rec.payload != NULL) {
                dl->bytes += moq_rcbuf_len(d.rec.payload);
            }
        }
        (void)moqr_core_delivery_done(c, r->subb, MOQR_DELIVERY_DELIVERED,
                                      r->now);
    }
}

/* Prove the retained horizon by IDENTITY: a FRESH retained-state
 * subscriber on the requester shard is served exactly the surviving groups
 * {NG-2, NG-1} (one object each), not merely "some two groups". The track
 * is already ACTIVE on the requester (the admitted demand sourced it, or —
 * at K=1 — the publisher is local), so the new subscribe fast-paths onto
 * the local log and replays its retained content; an ABSOLUTE_START from
 * {0,0} sits below the eviction horizon and is served from the oldest
 * retained group up. */
static void
br_assert_horizon(br_rig_t *r, uint64_t ng)
{
    moqr_core_t *c = moqr_shards_core(r->s, r->req_shard);
    moqr_binding_t hb;
    if (moqr_core_binding_open(c, 3, &hb) != MOQR_OK) {
        B_CHECK(false);
        return;
    }
    moq_bytes_t nsp[1] = { B("bw") };
    moqr_ns_t ns = { nsp, 1 };
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = ns;
    rq.name = B("v");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;   /* from {0,0} */
    rq.cookie = 99;
    moqr_sub_t hs;
    if (moqr_core_subscribe(c, hb, &rq, &hs) != MOQR_OK) {
        B_CHECK(false);
        return;
    }
    br_step_n(r, 6);   /* activate + serve retained (fast path) */

    bool saw[64];
    memset(saw, 0, sizeof(saw));
    uint64_t objs = 0;
    for (;;) {
        moqr_delivery_t d;
        if (moqr_core_next_delivery(c, hb, r->now, &d) != MOQR_OK) {
            break;
        }
        if (d.notice == MOQR_DELIVERY_NOTICE_NONE) {
            if (d.rec.group_id < 64) {
                saw[d.rec.group_id] = true;
            }
            objs++;
        }
        (void)moqr_core_delivery_done(c, hb, MOQR_DELIVERY_DELIVERED,
                                      r->now);
    }
    /* EXACTLY the two surviving groups, and no other. */
    for (uint64_t g = 0; g < ng; g++) {
        bool expect = (g == ng - 2 || g == ng - 1);
        if (saw[g] != expect) {
            printf("FAIL: horizon group %llu: saw=%d expect=%d\n",
                   (unsigned long long)g, (int)saw[g], (int)expect);
            g_failures++;
        }
    }
    B_CHECK(objs == 2);
    (void)moqr_core_unsubscribe(c, hs, r->now);
    (void)moqr_core_binding_close(c, hb, r->now);
    br_step_n(r, 2);
}

/* -- stats deltas -------------------------------------------------------------- */

typedef struct br_snap {
    moqr_shards_stats_t s0;   /* owner shard  */
    moqr_shards_stats_t s1;   /* requester shard */
} br_snap_t;

static void
br_snap(br_rig_t *r, br_snap_t *sn)
{
    memset(sn, 0, sizeof(*sn));
    B_CHECK(moqr_shards_get_stats(r->s, 0, &sn->s0) == MOQR_OK);
    if (r->k > 1) {
        B_CHECK(moqr_shards_get_stats(r->s, 1, &sn->s1) == MOQR_OK);
    }
}

/* Step until the owner's data enqueue total grows by `expect` and the
 * channels drain, bounded by `max_steps`. Returns the steps taken. */
static uint32_t
br_drain(br_rig_t *r, const br_snap_t *base, uint64_t expect,
         uint32_t max_steps)
{
    uint32_t steps = 0;
    for (;;) {
        br_step(r);
        steps++;
        if (r->k == 1) {
            if (steps >= 2) {
                return steps;   /* nothing crosses at K == 1 */
            }
            continue;
        }
        moqr_shards_stats_t st;
        B_CHECK(moqr_shards_get_stats(r->s, 0, &st) == MOQR_OK);
        uint64_t moved = 0;
        for (uint32_t kk = MOQR_SHARDS_MSG_OBJ; kk < MOQR_SHARDS_MSG__COUNT;
             kk++) {
            moved += st.enqueued[kk] - base->s0.enqueued[kk];
        }
        if (moved >= expect && br_dch_total(r) == 0) {
            return steps;
        }
        if (steps > max_steps) {
            printf("FAIL: drain did not converge (%u steps, moved %llu of "
                   "%llu)\n",
                   steps, (unsigned long long)moved,
                   (unsigned long long)expect);
            g_failures++;
            return steps;
        }
    }
}

/* The total of every enqueue kind (both shards) between two snapshots. */
static uint64_t
br_kind(const br_snap_t *lo, const br_snap_t *hi, uint32_t kind)
{
    return (hi->s0.enqueued[kind] - lo->s0.enqueued[kind]) +
           (hi->s1.enqueued[kind] - lo->s1.enqueued[kind]);
}

/*
 * One cadence row: the COMPLETE per-kind vector (control kinds — DEMAND,
 * ACK, ... — measured from `pre`, the pre-admission baseline, so the row
 * always shows the admission's exactly-one DEMAND and one ACK), the
 * data-phase deltas (turns/messages/bytes/wakes measured from `post`, the
 * post-admission baseline, so the bound is data-only), and the absolute
 * channel HWM. No wall clock.
 */
static void
br_row(const char *workload, uint16_t k, uint64_t objects,
       const br_snap_t *pre, const br_snap_t *post, const br_snap_t *end,
       const br_dlv_t *dl, uint64_t turn_bound)
{
    const moqr_shards_stats_t *p0 = &post->s0, *e0 = &end->s0;
    const moqr_shards_stats_t *p1 = &post->s1, *e1 = &end->s1;
    uint64_t dlv = 0;
    for (uint32_t f = 0; f < BR_FANOUT; f++) {
        dlv += dl->objects[f];
    }
    printf("CADENCE,bench=relay_shards,workload=%s,k=%u,fanout=%u,"
           "objects=%" PRIu64 ",demand=%" PRIu64 ",undemand=%" PRIu64
           ",done=%" PRIu64 ",ack=%" PRIu64 ",obj=%" PRIu64 ",obj_open=%"
           PRIu64 ",obj_chunk=%" PRIu64 ",obj_end=%" PRIu64 ",obj_reset=%"
           PRIu64 ",grp_reset=%" PRIu64 ",grp_evict=%" PRIu64 ",sg_seal=%"
           PRIu64 ",pump_turns=%" PRIu64 ",pump_msgs=%" PRIu64
           ",pump_bytes=%" PRIu64 ",hwm_entries=%u,hwm_bytes=%" PRIu64
           ",push_wakes=%" PRIu64 ",credit_wakes=%" PRIu64 ",turn_bound=%"
           PRIu64 ",deliveries=%" PRIu64 ",delivered_bytes=%" PRIu64 "\n",
           workload, (unsigned)k, (unsigned)BR_FANOUT, objects,
           br_kind(pre, end, MOQR_SHARDS_MSG_DEMAND),
           br_kind(pre, end, MOQR_SHARDS_MSG_UNDEMAND),
           br_kind(pre, end, MOQR_SHARDS_MSG_DONE),
           br_kind(pre, end, MOQR_SHARDS_MSG_ACK),
           br_kind(pre, end, MOQR_SHARDS_MSG_OBJ),
           br_kind(pre, end, MOQR_SHARDS_MSG_OBJ_OPEN),
           br_kind(pre, end, MOQR_SHARDS_MSG_OBJ_CHUNK),
           br_kind(pre, end, MOQR_SHARDS_MSG_OBJ_END),
           br_kind(pre, end, MOQR_SHARDS_MSG_OBJ_RESET),
           br_kind(pre, end, MOQR_SHARDS_MSG_GRP_RESET),
           br_kind(pre, end, MOQR_SHARDS_MSG_GRP_EVICT),
           br_kind(pre, end, MOQR_SHARDS_MSG_SG_SEAL),
           e0->pump_turns - p0->pump_turns,
           e0->pump_messages - p0->pump_messages,
           e0->pump_bytes - p0->pump_bytes, e1->channel_entries_hwm,
           e1->channel_bytes_hwm,
           e0->wake_requests_push - p0->wake_requests_push,
           e1->wake_requests_credit - p1->wake_requests_credit, turn_bound,
           dlv, dl->bytes);
}

/* Every cross-shard counter of a K == 1 runtime is structurally zero. */
static void
br_assert_k1_inert(br_rig_t *r)
{
    moqr_shards_stats_t st, zero;
    memset(&zero, 0, sizeof(zero));
    B_CHECK(moqr_shards_get_stats(r->s, 0, &st) == MOQR_OK);
    B_CHECK(memcmp(&st, &zero, sizeof(st)) == 0);
}

/*
 * The COMPLETE per-kind vector assertion from the pre-admission baseline:
 * a K == 2 workload's `want` carries the full contract — exactly one
 * DEMAND and one ACK from the admission plus the workload's data kinds —
 * so no kind is left unpinned. Every kind absent from `want` is asserted
 * zero.
 */
static void
br_assert_kinds(const br_snap_t *pre, const br_snap_t *end,
                const uint64_t want[MOQR_SHARDS_MSG__COUNT])
{
    for (uint32_t kk = 0; kk < MOQR_SHARDS_MSG__COUNT; kk++) {
        uint64_t got = br_kind(pre, end, kk);
        if (got != want[kk]) {
            printf("FAIL: kind %s: got %llu want %llu\n",
                   moqr_shards_msg_kind_name(kk), (unsigned long long)got,
                   (unsigned long long)want[kk]);
            g_failures++;
        }
    }
}

/* ============================== workloads ================================= */

/* W-whole: 64 whole objects, one group, one subgroup. Exactly one DEMAND,
 * one ACK (setup), 64 OBJ, nothing else. Turn bound: 2*ceil(64/4)+2 = 34;
 * push/credit wakes exactly ceil(64/4) = 16 each; entry HWM exactly C = 4;
 * byte HWM exactly 4*64 = 256 (a full window of whole objects). */
static void
w_whole(uint16_t k)
{
    ca_t a;
    ca_init(&a);
    br_rig_t r;
    if (!br_build(&r, &a, k, 8)) {
        printf("FAIL: W-whole rig (k=%u)\n", (unsigned)k);
        g_failures++;
        return;
    }
    const uint64_t N = 64;
    const uint64_t bound = 2u * ((N + BR_P - 1) / BR_P) + BR_SLACK;
    br_snap_t pre;
    br_snap(&r, &pre);            /* pre-admission: cross-shard zero */
    B_CHECK(br_admit(&r));
    br_snap_t base;
    br_snap(&r, &base);           /* post-admission: DEMAND/ACK done */
    for (uint64_t o = 0; o < N; o++) {
        B_CHECK(br_pub_whole(&r, 0, 0, o, BR_WHOLE_LEN));
    }
    (void)br_drain(&r, &base, N, (uint32_t)(10 * bound));
    br_snap_t end;
    br_snap(&r, &end);
    br_dlv_t dl;
    memset(&dl, 0, sizeof(dl));
    br_pull(&r, &dl);

    for (uint32_t f = 0; f < BR_FANOUT; f++) {
        B_CHECK(dl.objects[f] == N);
    }
    B_CHECK(dl.bytes == BR_FANOUT * N * BR_WHOLE_LEN);
    if (k > 1) {
        /* The COMPLETE contract from the pre-admission baseline: one DEMAND
         * and one ACK plus exactly N whole objects, nothing else. */
        uint64_t want[MOQR_SHARDS_MSG__COUNT] = { 0 };
        want[MOQR_SHARDS_MSG_DEMAND] = 1;
        want[MOQR_SHARDS_MSG_ACK] = 1;
        want[MOQR_SHARDS_MSG_OBJ] = N;
        br_assert_kinds(&pre, &end, want);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_DEMAND) == 1);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_ACK) == 1);
        B_CHECK(end.s0.pump_messages - base.s0.pump_messages == N);
        B_CHECK(end.s0.pump_bytes - base.s0.pump_bytes == N * BR_WHOLE_LEN);
        B_CHECK(end.s0.pump_turns - base.s0.pump_turns <= bound);
        B_CHECK(end.s1.channel_entries_hwm == BR_P);
        B_CHECK(end.s1.channel_bytes_hwm == BR_P * BR_WHOLE_LEN);
        B_CHECK(end.s0.wake_requests_push - base.s0.wake_requests_push ==
                (N + BR_P - 1) / BR_P);
        B_CHECK(end.s1.wake_requests_credit -
                    base.s1.wake_requests_credit ==
                (N + BR_P - 1) / BR_P);
        br_row("W-whole", k, N, &pre, &base, &end, &dl, bound);
    } else {
        br_assert_k1_inert(&r);
        br_row("W-whole", k, N, &pre, &base, &end, &dl, 0);
    }
    br_destroy(&r);
    B_CHECK(a.live == 0);
}

/* W-chunk: 16 completed objects x 8 chunks. Exactly 16 OBJ_OPEN + 128
 * OBJ_CHUNK + 16 OBJ_END = 160 data enqueues. Turn bound: 2*ceil(160/4)+2
 * = 82; wakes exactly 40 each; entry HWM 4; byte HWM = the heaviest
 * 4-message window = 4 chunks = 64 bytes (OPEN/END carry no payload). */
static void
w_chunk(uint16_t k)
{
    ca_t a;
    ca_init(&a);
    br_rig_t r;
    if (!br_build(&r, &a, k, 8)) {
        printf("FAIL: W-chunk rig (k=%u)\n", (unsigned)k);
        g_failures++;
        return;
    }
    const uint64_t NOBJ = 16, NCH = 8;
    const uint64_t MSGS = NOBJ * (NCH + 2);
    const uint64_t bound = 2u * ((MSGS + BR_P - 1) / BR_P) + BR_SLACK;
    br_snap_t pre;
    br_snap(&r, &pre);
    B_CHECK(br_admit(&r));
    br_snap_t base;
    br_snap(&r, &base);
    for (uint64_t o = 0; o < NOBJ; o++) {
        B_CHECK(br_pub_open(&r, 0, 0, o, NCH * BR_CHUNK_LEN));
        for (uint64_t c = 0; c < NCH; c++) {
            B_CHECK(br_pub_chunk(&r, 0, 0, o, BR_CHUNK_LEN,
                                 (uint8_t)(0xC0 + c)));
        }
        B_CHECK(moqr_core_complete_record(moqr_shards_core(r.s, 0), r.track,
                                          0, 0, o) == MOQR_OK);
    }
    (void)br_drain(&r, &base, MSGS, (uint32_t)(10 * bound));
    br_snap_t end;
    br_snap(&r, &end);
    br_dlv_t dl;
    memset(&dl, 0, sizeof(dl));
    br_pull(&r, &dl);

    for (uint32_t f = 0; f < BR_FANOUT; f++) {
        B_CHECK(dl.objects[f] == NOBJ);
    }
    B_CHECK(dl.bytes == BR_FANOUT * NOBJ * NCH * BR_CHUNK_LEN);
    if (k > 1) {
        uint64_t want[MOQR_SHARDS_MSG__COUNT] = { 0 };
        want[MOQR_SHARDS_MSG_DEMAND] = 1;
        want[MOQR_SHARDS_MSG_ACK] = 1;
        want[MOQR_SHARDS_MSG_OBJ_OPEN] = NOBJ;
        want[MOQR_SHARDS_MSG_OBJ_CHUNK] = NOBJ * NCH;
        want[MOQR_SHARDS_MSG_OBJ_END] = NOBJ;
        br_assert_kinds(&pre, &end, want);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_DEMAND) == 1);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_ACK) == 1);
        B_CHECK(end.s0.pump_messages - base.s0.pump_messages == MSGS);
        B_CHECK(end.s0.pump_bytes - base.s0.pump_bytes ==
                NOBJ * NCH * BR_CHUNK_LEN);
        B_CHECK(end.s0.pump_turns - base.s0.pump_turns <= bound);
        B_CHECK(end.s1.channel_entries_hwm == BR_P);
        B_CHECK(end.s1.channel_bytes_hwm == BR_P * BR_CHUNK_LEN);
        B_CHECK(end.s0.wake_requests_push - base.s0.wake_requests_push ==
                (MSGS + BR_P - 1) / BR_P);
        B_CHECK(end.s1.wake_requests_credit -
                    base.s1.wake_requests_credit ==
                (MSGS + BR_P - 1) / BR_P);
        br_row("W-chunk", k, NOBJ, &pre, &base, &end, &dl, bound);
    } else {
        br_assert_k1_inert(&r);
        br_row("W-chunk", k, NOBJ, &pre, &base, &end, &dl, 0);
    }
    br_destroy(&r);
    B_CHECK(a.live == 0);
}

/* W-seal: 64 whole objects across four subgroups (16 each), then all four
 * subgroups FIN. Exactly 64 OBJ + 4 SG_SEAL; no duplicate seal, no reset
 * kinds. Turn bound: data 2*ceil(64/4) plus the one seal push round and
 * its drain lag, plus slack per burst: 2*16 + 2 + 2*1 + 2 = 38. */
static void
w_seal(uint16_t k)
{
    ca_t a;
    ca_init(&a);
    br_rig_t r;
    if (!br_build(&r, &a, k, 8)) {
        printf("FAIL: W-seal rig (k=%u)\n", (unsigned)k);
        g_failures++;
        return;
    }
    const uint64_t N = 64, NSG = 4;
    const uint64_t bound = (2u * ((N + BR_P - 1) / BR_P) + BR_SLACK) +
                           (2u * ((NSG + BR_P - 1) / BR_P) + BR_SLACK);
    br_snap_t pre;
    br_snap(&r, &pre);
    B_CHECK(br_admit(&r));
    br_snap_t base;
    br_snap(&r, &base);
    /* One group, NSG subgroups, object ids strictly ascending (per-group
     * monotonic ingest) cycling across the subgroups. */
    for (uint64_t o = 0; o < N; o++) {
        B_CHECK(br_pub_whole(&r, 0, o % NSG, o, BR_WHOLE_LEN));
    }
    (void)br_drain(&r, &base, N, (uint32_t)(10 * bound));
    for (uint64_t sg = 0; sg < NSG; sg++) {
        moqr_result_t src = moqr_core_seal_subgroup(
            moqr_shards_core(r.s, 0), r.track, 0, sg);
        B_CHECK(src == MOQR_OK || src == MOQR_DONE);
    }
    (void)br_drain(&r, &base, N + NSG, (uint32_t)(10 * bound));
    br_snap_t end;
    br_snap(&r, &end);
    br_dlv_t dl;
    memset(&dl, 0, sizeof(dl));
    br_pull(&r, &dl);

    for (uint32_t f = 0; f < BR_FANOUT; f++) {
        B_CHECK(dl.objects[f] == N);
    }
    B_CHECK(dl.bytes == BR_FANOUT * N * BR_WHOLE_LEN);   /* exact bytes */
    if (k > 1) {
        uint64_t want[MOQR_SHARDS_MSG__COUNT] = { 0 };
        want[MOQR_SHARDS_MSG_DEMAND] = 1;
        want[MOQR_SHARDS_MSG_ACK] = 1;
        want[MOQR_SHARDS_MSG_OBJ] = N;
        want[MOQR_SHARDS_MSG_SG_SEAL] = NSG;
        br_assert_kinds(&pre, &end, want);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_DEMAND) == 1);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_ACK) == 1);
        /* The data pump carried exactly the 64 whole objects (the seals
         * are zero-byte control), so bytes and message count are exact. */
        B_CHECK(end.s0.pump_messages - base.s0.pump_messages == N + NSG);
        B_CHECK(end.s0.pump_bytes - base.s0.pump_bytes == N * BR_WHOLE_LEN);
        B_CHECK(end.s0.pump_turns - base.s0.pump_turns <= bound);
        B_CHECK(end.s1.channel_entries_hwm == BR_P);
        B_CHECK(end.s1.channel_bytes_hwm == BR_P * BR_WHOLE_LEN);
        /* Wakes: the 64 objects pace at ceil(64/P) push rounds and the 4
         * seals at ceil(4/P) = 1 more (their own burst). */
        B_CHECK(end.s0.wake_requests_push - base.s0.wake_requests_push ==
                (N + BR_P - 1) / BR_P + (NSG + BR_P - 1) / BR_P);
        B_CHECK(end.s1.wake_requests_credit -
                    base.s1.wake_requests_credit ==
                (N + BR_P - 1) / BR_P + (NSG + BR_P - 1) / BR_P);
        br_row("W-seal", k, N, &pre, &base, &end, &dl, bound);
    } else {
        br_assert_k1_inert(&r);
        br_row("W-seal", k, N, &pre, &base, &end, &dl, 0);
    }
    br_destroy(&r);
    B_CHECK(a.live == 0);
}

/* W-evict: group budget 2, six single-object groups, DRAINED TO QUIESCENCE
 * after every group so eviction watermarks cannot collapse. Exactly 6 OBJ
 * + 4 GRP_EVICT (groups 0..3 evict as 2..5 arrive), 0 GRP_RESET. Each
 * group is one burst of at most 2 messages (its OBJ plus, from group 2 on,
 * the watermark notice), so entry HWM is exactly 2, byte HWM exactly one
 * whole object (notices carry no payload), and the bound is six bursts of
 * 2*ceil(2/4)+2 = 4 turns. The subscribers pull between groups, so each
 * receives all six objects before the requester's own budget evicts them,
 * and the final retained horizon is groups {4,5}. */
static void
w_evict(uint16_t k)
{
    ca_t a;
    ca_init(&a);
    br_rig_t r;
    if (!br_build(&r, &a, k, 2)) {   /* the eviction pressure */
        printf("FAIL: W-evict rig (k=%u)\n", (unsigned)k);
        g_failures++;
        return;
    }
    const uint64_t NG = 6;
    const uint64_t bound = NG * (2u * 1u + BR_SLACK);
    br_snap_t pre;
    br_snap(&r, &pre);
    B_CHECK(br_admit(&r));
    br_snap_t base;
    br_snap(&r, &base);
    br_dlv_t dl;
    memset(&dl, 0, sizeof(dl));
    uint64_t moved = 0;
    for (uint64_t g = 0; g < NG; g++) {
        B_CHECK(br_pub_whole(&r, g, 0, 0, BR_WHOLE_LEN));
        moved += 1 + (g >= 2 ? 1 : 0);   /* the OBJ + its eviction notice */
        (void)br_drain(&r, &base, moved, (uint32_t)(10 * bound));
        br_pull(&r, &dl);   /* consume before the requester evicts it */
    }
    br_snap_t end;
    br_snap(&r, &end);

    for (uint32_t f = 0; f < BR_FANOUT; f++) {
        B_CHECK(dl.objects[f] == NG);
    }
    B_CHECK(dl.bytes == BR_FANOUT * NG * BR_WHOLE_LEN);
    if (k > 1) {
        uint64_t want[MOQR_SHARDS_MSG__COUNT] = { 0 };
        want[MOQR_SHARDS_MSG_DEMAND] = 1;
        want[MOQR_SHARDS_MSG_ACK] = 1;
        want[MOQR_SHARDS_MSG_OBJ] = NG;
        want[MOQR_SHARDS_MSG_GRP_EVICT] = NG - 2;
        br_assert_kinds(&pre, &end, want);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_DEMAND) == 1);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_ACK) == 1);
        /* The owner pump carried the NG objects (the GRP_EVICT notices are
         * zero-byte control): exact message and byte totals. */
        B_CHECK(end.s0.pump_messages - base.s0.pump_messages ==
                NG + (NG - 2));
        B_CHECK(end.s0.pump_bytes - base.s0.pump_bytes == NG * BR_WHOLE_LEN);
        B_CHECK(end.s0.pump_turns - base.s0.pump_turns <= bound);
        B_CHECK(end.s1.channel_entries_hwm == 2);
        B_CHECK(end.s1.channel_bytes_hwm == BR_WHOLE_LEN);
        B_CHECK(end.s0.wake_requests_push - base.s0.wake_requests_push ==
                NG);
        B_CHECK(end.s1.wake_requests_credit -
                    base.s1.wake_requests_credit == NG);
    }
    /* The final retained horizon, by IDENTITY (both K): a fresh
     * retained-state subscriber on the requester shard is served exactly
     * the surviving groups — groups {NG-2, NG-1}, not merely "two groups".
     * At K=1 the publisher/subscriber share shard 0; at K=2 the requester
     * shard's mirror already sources the track, so the new sub joins the
     * WARM/ACTIVE local track and replays its retained log. */
    br_assert_horizon(&r, NG);
    br_row("W-evict", k, NG, &pre, &base, &end, &dl, k > 1 ? bound : 0);
    if (k == 1) {
        br_assert_k1_inert(&r);
    }
    br_destroy(&r);
    B_CHECK(a.live == 0);
}

/* W-reset: eight begun objects abandoned mid-stream (sequentially in one
 * subgroup), each paced so the OPEN+first-chunk burst crosses before the
 * abandon. Exactly 8 OBJ_OPEN + 8 OBJ_CHUNK + 8 OBJ_RESET; NO clean END
 * for any of them. The abandon carries the 62-bit code (its bit-exact
 * propagation is pinned by the deterministic shards suite). Two bursts per
 * object (2 then 1 messages) bound the turns at 8 * 2 * (2*1+2) = 64;
 * entry HWM exactly 2 (OPEN+chunk), byte HWM exactly one chunk. */
static void
w_reset(uint16_t k)
{
    ca_t a;
    ca_init(&a);
    br_rig_t r;
    if (!br_build(&r, &a, k, 8)) {
        printf("FAIL: W-reset rig (k=%u)\n", (unsigned)k);
        g_failures++;
        return;
    }
    const uint64_t N = 8;
    const uint64_t bound = N * 2u * (2u * 1u + BR_SLACK);
    br_snap_t pre;
    br_snap(&r, &pre);
    B_CHECK(br_admit(&r));
    br_snap_t base;
    br_snap(&r, &base);
    uint64_t moved = 0;
    for (uint64_t o = 0; o < N; o++) {
        B_CHECK(br_pub_open(&r, 0, 0, o, 4u * BR_CHUNK_LEN));
        B_CHECK(br_pub_chunk(&r, 0, 0, o, BR_CHUNK_LEN, (uint8_t)o));
        moved += 2;   /* OBJ_OPEN + OBJ_CHUNK */
        (void)br_drain(&r, &base, moved, (uint32_t)(10 * bound));
        B_CHECK(moqr_core_abandon_record(moqr_shards_core(r.s, 0), r.track,
                                         0, 0, o,
                                         rd_wire(BR_WIDE_RESET)) == MOQR_OK);
        moved += 1;   /* OBJ_RESET */
        (void)br_drain(&r, &base, moved, (uint32_t)(10 * bound));
    }
    br_snap_t end;
    br_snap(&r, &end);
    br_dlv_t dl;
    memset(&dl, 0, sizeof(dl));
    br_pull(&r, &dl);

    for (uint32_t f = 0; f < BR_FANOUT; f++) {
        B_CHECK(dl.objects[f] == 0);   /* nothing completed cleanly */
    }
    B_CHECK(dl.bytes == 0);   /* no clean delivery => zero delivered bytes */
    if (k > 1) {
        uint64_t want[MOQR_SHARDS_MSG__COUNT] = { 0 };
        want[MOQR_SHARDS_MSG_DEMAND] = 1;
        want[MOQR_SHARDS_MSG_ACK] = 1;
        want[MOQR_SHARDS_MSG_OBJ_OPEN] = N;
        want[MOQR_SHARDS_MSG_OBJ_CHUNK] = N;
        want[MOQR_SHARDS_MSG_OBJ_RESET] = N;
        br_assert_kinds(&pre, &end, want);   /* END == 0 by omission */
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_DEMAND) == 1);
        B_CHECK(br_kind(&pre, &end, MOQR_SHARDS_MSG_ACK) == 1);
        /* Owner pump: N OPEN + N CHUNK + N RESET = 3N messages, exactly N
         * chunks' payload (OPEN/RESET carry none). */
        B_CHECK(end.s0.pump_messages - base.s0.pump_messages == 3u * N);
        B_CHECK(end.s0.pump_bytes - base.s0.pump_bytes == N * BR_CHUNK_LEN);
        B_CHECK(end.s0.pump_turns - base.s0.pump_turns <= bound);
        B_CHECK(end.s1.channel_entries_hwm == 2);
        B_CHECK(end.s1.channel_bytes_hwm == BR_CHUNK_LEN);
        B_CHECK(end.s0.wake_requests_push - base.s0.wake_requests_push ==
                2 * N);
        B_CHECK(end.s1.wake_requests_credit -
                    base.s1.wake_requests_credit == 2 * N);
        B_CHECK(moqr_shards_debug_requester_open_objects(r.s, 1) == 0);
        br_row("W-reset", k, N, &pre, &base, &end, &dl, bound);
    } else {
        br_assert_k1_inert(&r);
        br_row("W-reset", k, N, &pre, &base, &end, &dl, 0);
    }
    br_destroy(&r);
    B_CHECK(a.live == 0);
}

static void
usage(const char *argv0)
{
    fprintf(stderr, "usage: %s (no flags; deterministic cadence rows on "
                    "stdout)\n",
            argv0);
}

int
main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++) {
        usage(argv[0]);
        return 2;   /* unknown flag: fail loudly */
    }
    printf("# bench_relay_shards: deterministic cadence (C=%u M=%u P=%u "
           "S=%u fanout=%u)\n",
           (unsigned)BR_DCH_ENTRIES, (unsigned)BR_TURN_MSGS,
           (unsigned)BR_P, (unsigned)BR_SLACK, (unsigned)BR_FANOUT);
    w_whole(2);
    w_chunk(2);
    w_seal(2);
    w_evict(2);
    w_reset(2);
    /* The same logical workloads at K == 1: every cross-shard counter and
     * wake stays structurally zero while local delivery is identical. */
    w_whole(1);
    w_chunk(1);
    w_seal(1);
    w_evict(1);
    w_reset(1);
    if (g_failures == 0) {
        printf("# ALL PASS\n");
    }
    return g_failures == 0 ? 0 : 1;
}
