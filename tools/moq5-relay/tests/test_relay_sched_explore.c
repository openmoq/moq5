/*
 * The seeded scheduler explorer. One binary, three modes:
 *
 *   --seeds            the committed 16-seed set against the committed
 *                      manifest: generate, execute against production with
 *                      the invariants checked after every operation, then
 *                      replay the canonical bytes in a second fresh rig
 *                      without the PRNG and require identical dispositions,
 *                      coverage counters, model final state and
 *                      production-observation digest;
 *   --seed 0x<hex>     one seed, exploration;
 *   --trace <file>     exact replay of canonical trace bytes (nonzero exit
 *                      on the first invariant violation, with seed,
 *                      versions, trace hash, op index, invariant ID, causal
 *                      key and the ledger/snapshot delta);
 *   --cli-units        the argument/manifest robustness unit cases.
 *
 * The model is the oracle; the execution is production code — the real
 * lane pump over a real K=2 moqr_shards runtime, driven through the
 * sans-I/O seams and the fake-wire shuttle. On a violation in --seeds or
 * --seed, ddmin produces a strictly smaller precondition-valid canonical
 * trace that must replay to the same invariant with the same causal key.
 */

#include <inttypes.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <moq/relay/moqr_bind.h>
#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/msquic_managed.h>
#include <moq/rcbuf.h>
#include <moq/session.h>

#include "support/fake_msq_managed.h"
#include "support/msq_test_seams.h"

#include "seedx_shuttle.h"
#include "sched/sched_gen.h"

#include <stdarg.h>

extern moq_msquic_lane_pump_fn moqr_test_lanes_pump(void);
extern void *moqr_test_mk_lanes_ctx(moqr_shards_t *shards, uint32_t lanes);
extern uint64_t moqr_test_lanes_ctx_pump_turns(void *ctx, uint32_t lane);
extern uint64_t moqr_test_lanes_ctx_wake_pushes(void *ctx, uint32_t lane);

/* -- invariant identifiers ------------------------------------------------ */

typedef enum {
    INV_NONE = 0,
    INV_I1_OWNERSHIP = 1,
    INV_I2_OWED_WAKE = 2,
    INV_I3_BOUNDED_REAP = 3,
    INV_I4_EXACTLY_ONCE = 4,
    INV_I5_CREDIT = 5,
    INV_I6_CANCEL_PRECEDENCE = 6,
    INV_I7_STOP_ORDER = 7,
    INV_EXEC = 8,   /* an executed exchange failed to complete its script  */
    INV_DRAIN = 9,  /* quiescence not reached within the derived bound     */
} sched_inv_t;

static const char *inv_name(sched_inv_t v)
{
    static const char *n[] = { "NONE", "I1_OWNERSHIP", "I2_OWED_WAKE",
                               "I3_BOUNDED_REAP", "I4_EXACTLY_ONCE",
                               "I5_CREDIT", "I6_CANCEL_PRECEDENCE",
                               "I7_STOP_ORDER", "EXEC", "DRAIN" };
    return n[v <= INV_DRAIN ? v : 0];
}

/* -- coverage counters (compared between generation and replay) ---------- */

typedef struct sched_cov {
    uint64_t op_kind[SCHED_OP_MAX + 1];
    uint64_t lane_steps[SCHED_LANES];
    uint64_t pair_offers[SCHED_LANES][SCHED_LANES];
    uint64_t held_offers;
    uint64_t retries;
    uint64_t cancels;
    uint64_t terminals;
    uint64_t stops;
    uint64_t wake_coalesced;
    uint64_t drain_steps;
    uint64_t revokes;
    uint64_t credit_wakes;
} sched_cov_t;

/* -- the execution rig ------------------------------------------------------ */

typedef struct pubst {
    moq_subscription_t    up_sub[SCHED_MAX_CHILDREN]; /* per subscriber    */
    moq_subgroup_handle_t sgh;
    bool                  sg_open;
} pubst_t;

typedef struct exec {
    fake_mgd_t                 fake;
    moqr_shards_t             *shards;
    void                      *ctx;
    moq_msquic_managed_t      *m;
    moq_msquic_managed_lane_t *lane[SCHED_LANES];

    shx_driver_t   drv[SCHED_MAX_CHILDREN];
    bool           open[SCHED_MAX_CHILDREN];
    const void    *row_id[SCHED_MAX_CHILDREN]; /* identity while linked    */
    const void    *pre_ids[SCHED_LANES][SCHED_MAX_LIVE];
    size_t         pre_n[SCHED_LANES];
    bool           reaped[SCHED_MAX_CHILDREN];
    int            lane_steps_since_term[SCHED_MAX_CHILDREN];
    moq_announcement_t ann[SCHED_MAX_CHILDREN];
    moq_subscription_t subh[SCHED_MAX_CHILDREN];
    pubst_t        ps[SCHED_MAX_CHILDREN];
    uint64_t       oid[SCHED_MAX_CHILDREN];
    int            unsub_seen[SCHED_MAX_CHILDREN];
    int            frozen_objs[SCHED_MAX_CHILDREN]; /* -1 = sub active     */

    sched_model_t  model;
    sched_cov_t    cov;
    uint64_t       digest;
    uint8_t       *disp;        /* per-op dispositions                     */
    uint32_t       disp_len;

    /* failure context */
    sched_inv_t    violated;
    uint32_t       fail_index;
    int            fail_child;  /* -1 none */
    int            fail_pair_src, fail_pair_dst;
    char           fail_delta[256];
} exec_t;

static void dg(exec_t *e, uint64_t v)
{
    e->digest ^= v + 0x9E3779B97F4A7C15ull;
    e->digest *= 0x100000001B3ull;
}

static void lane_bits(exec_t *e, uint32_t lane, bool *bits,
                      moq_msq_test_lane_row_t *row_out,
                      moq_msq_test_child_row_t *rows, size_t cap,
                      size_t *nrows)
{
    moq_msq_test_lane_row_t row;
    size_t n = moq_msq_test_lane_snapshot(e->lane[lane], &row, rows,
                                          rows != NULL ? cap : 0);

    if (bits != NULL) {
        *bits = row.ext_wake || row.wake_pending || row.pump_pending;
    }
    if (row_out != NULL) {
        *row_out = row;
    }
    if (nrows != NULL) {
        *nrows = n;
    }
}

static void violate(exec_t *e, sched_inv_t inv, uint32_t index, int child,
                    int src, int dst, const char *fmt, ...)
{
    if (e->violated != INV_NONE) {
        return; /* first violation wins */
    }
    e->violated = inv;
    e->fail_index = index;
    e->fail_child = child;
    e->fail_pair_src = src;
    e->fail_pair_dst = dst;
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(e->fail_delta, sizeof(e->fail_delta), fmt, ap);
    va_end(ap);
}

/* one production step on a lane, with the model's drain semantics and the
 * I2/I3/I4 bookkeeping every executed step carries */
static moq_msq_test_step_t
exec_step(exec_t *e, uint32_t lane, uint32_t index, bool count_drain)
{
    bool owed_bits = false;
    uint64_t turns_before = moqr_test_lanes_ctx_pump_turns(e->ctx, lane);

    lane_bits(e, lane, &owed_bits, NULL, NULL, 0, NULL);
    moq_msq_test_step_t r = moq_msq_test_lane_step(e->lane[lane]);

    e->cov.lane_steps[lane]++;
    if (count_drain) {
        e->cov.drain_steps++;
    }
    if (owed_bits) {
        /* I2: visible owed work must be consumed by a PUMPED sweep — a
         * lost wake surfaces as an IDLE step here. The managed suppressor
         * may legally skip the application callback when the sweep has
         * nothing to hand over, so the app-entry count is digest material
         * (replay identity), not an invariant. */
        uint64_t turns_after = moqr_test_lanes_ctx_pump_turns(e->ctx, lane);

        if (r != MOQ_MSQ_TEST_STEP_PUMPED &&
            r != MOQ_MSQ_TEST_STEP_STOP) {
            violate(e, INV_I2_OWED_WAKE, index, -1, (int)lane, -1,
                    "owed bits visible but step=%d turns %" PRIu64
                    "->%" PRIu64, (int)r, turns_before, turns_after);
        }
    }
    /* model: an executed step on lane L drains every pair directed at L
     * (the push wake guarantees the pump sees the channel content) */
    for (uint32_t s = 0; s < SCHED_LANES; s++) {
        if (s == lane) {
            continue;
        }
        while (sx_pair_consume(&e->model.pair[s][lane])) {
        }
    }
    /* I3: only an UNARMED step can reach the doorbell's idle loop-top,
     * where reclamation runs — armed steps pump instead. Age terminal
     * children by reap-eligible steps, which is what the derived bound
     * counts. */
    if (!owed_bits) {
        for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
            if (e->model.child[c].used && e->model.child[c].terminal &&
                !e->reaped[c] && e->drv[c].child != NULL &&
                e->model.child[c].lane == lane) {
                e->lane_steps_since_term[c]++;
            }
        }
    }
    dg(e, (uint64_t)r);
    return r;
}

/* post-op observation: identity checks + digest fold, shared by every op */
static void observe(exec_t *e, uint32_t index)
{
    moq_msq_test_child_row_t rows[SCHED_MAX_LIVE + 1];

    if (getenv("SCHED_DEBUG") != NULL) { /* transient diagnosis */
        for (uint32_t l2 = 0; l2 < SCHED_LANES; l2++) {
            moq_msq_test_lane_row_t lr2;
            size_t n2 = 0;

            lane_bits(e, l2, NULL, &lr2, rows, SCHED_MAX_LIVE + 1,
                      &n2);
            for (size_t i2 = 0; i2 < n2 && n2 <= SCHED_MAX_LIVE;
                 i2++) {
                fprintf(stderr,
                        "  dbg idx=%u lane=%u row id=%p sc=%d reap=%d "
                        "ack=%d obs=%d ev=%d\n", index, l2, rows[i2].id,
                        rows[i2].shutdown_complete, rows[i2].reapable,
                        rows[i2].app_terminal_acked,
                        rows[i2].terminal_observed, rows[i2].has_events);
            }
        }
    }

    for (uint32_t l = 0; l < SCHED_LANES; l++) {
        moq_msq_test_lane_row_t row;
        size_t n = 0;
        bool bits = false;

        lane_bits(e, l, &bits, &row, rows, SCHED_MAX_LIVE + 1, &n);
        if (n > SCHED_MAX_LIVE) {
            violate(e, INV_I1_OWNERSHIP, index, -1, (int)l, -1,
                    "snapshot overflow n=%zu", n);
            return;
        }
        /* reap detection: a terminal child whose row vanished */
        for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
            if (!e->model.child[c].used || e->reaped[c] ||
                e->model.child[c].lane != l || !e->model.child[c].terminal) {
                continue;
            }
            bool present = false;

            for (size_t i = 0; i < n; i++) {
                if (rows[i].id == e->row_id[c]) {
                    present = true;
                }
            }
            if (!present) {
                e->reaped[c] = true;
            } else if (e->lane_steps_since_term[c] > 4) {
                /* I3: 2 steps (owed pump, idle reap) + 1 re-drive + 1
                 * event-guard slack — the derived bound for this rig */
                const moq_msq_test_child_row_t *cr = NULL;

                for (size_t i = 0; i < n; i++) {
                    if (rows[i].id == e->row_id[c]) {
                        cr = &rows[i];
                    }
                }
                violate(e, INV_I3_BOUNDED_REAP, index, (int)c, (int)l, -1,
                        "terminal child %u still linked after %d lane steps"
                        " (sc=%d reap=%d ack=%d obs=%d ev=%d)",
                        c, e->lane_steps_since_term[c],
                        cr ? cr->shutdown_complete : -1,
                        cr ? cr->reapable : -1,
                        cr ? cr->app_terminal_acked : -1,
                        cr ? cr->terminal_observed : -1,
                        cr ? cr->has_events : -1);
            }
        }
        /* I1: every linked row must be one of the model's children on this
         * lane; count must match the model's linked population */
        uint32_t model_linked = 0;

        for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
            if (e->model.child[c].used && e->model.child[c].lane == l &&
                !e->reaped[c]) {
                model_linked++;
            }
        }
        if ((uint32_t)n != model_linked) {
            violate(e, INV_I1_OWNERSHIP, index, -1, (int)l, -1,
                    "lane %u linked rows %zu, ledger %u", l, n,
                    model_linked);
            if (getenv("SCHED_DEBUG") != NULL) { /* transient diagnosis */
                for (uint32_t c2 = 0; c2 < SCHED_MAX_CHILDREN; c2++) {
                    if (!e->open[c2]) continue;
                    const fake_mgd_conn_t *mc2 = e->drv[c2].child;
                    fprintf(stderr,
                            "  dbg child %u: mgd closed=%d shutdowns=%d "
                            "inner_shutdowns=%d code=%llu drv_evs=%d [",
                            c2, mc2->closed, mc2->shutdowns,
                            mc2->fake.conn_shutdowns,
                            (unsigned long long)
                                mc2->fake.last_conn_shutdown_code,
                            e->drv[c2].ev_count);
                    for (int k2 = 0; k2 < e->drv[c2].ev_count && k2 < 24;
                         k2++) {
                        fprintf(stderr, " %u",
                                e->drv[c2].ev_kind[k2]);
                    }
                    fprintf(stderr, " ]\n");
                    for (int k4 = 0; k4 < SHX_MAX_STREAMS; k4++) {
                        const shx_tap_t *tp = &e->drv[c2].tap[k4];

                        if (!tp->used) continue;
                        fprintf(stderr,
                                "  dbg child %u tap stream=%llu len=%zu "
                                "fin=%d head=", c2,
                                (unsigned long long)tp->id, tp->len,
                                tp->fin_seen);
                        for (size_t k5 = 0; k5 < tp->len && k5 < 10; k5++) {
                            fprintf(stderr, "%02x", tp->bytes[k5]);
                        }
                        fprintf(stderr, "\n");
                    }
                    for (size_t k3 = 0; k3 < e->drv[c2].ep.count; k3++) {
                        if (e->drv[c2].ep.ops[k3].kind == FAKE_OP_CLOSE) {
                            fprintf(stderr,
                                    "  dbg child %u CLOSE code=%llu "
                                    "reason='%.*s'\n", c2,
                                    (unsigned long long)
                                        e->drv[c2].ep.ops[k3].error_code,
                                    (int)e->drv[c2].ep.ops[k3].data_len,
                                    (const char *)
                                        e->drv[c2].ep.ops[k3].data);
                        }
                    }
                }
            }
        }
        for (size_t i = 0; i < n; i++) {
            for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
                if (!e->reaped[c] && e->row_id[c] == rows[i].id &&
                    e->model.child[c].used &&
                    e->model.child[c].lane != l) {
                    violate(e, INV_I1_OWNERSHIP, index, (int)c, (int)l, -1,
                            "child %u on lane %u, ledger lane %u", c, l,
                            e->model.child[c].lane);
                }
            }
        }
        dg(e, ((uint64_t)bits << 32) | row.conn_count);
        dg(e, moqr_test_lanes_ctx_pump_turns(e->ctx, l));
    }
    /* I5: channel occupancy <-> channel bytes, plus the pair identities */
    for (uint32_t s = 0; s < SCHED_LANES; s++) {
        for (uint32_t d = 0; d < SCHED_LANES; d++) {
            if (s == d) {
                continue;
            }
            int idfail = sx_pair_check(&e->model.pair[s][d]);

            if (idfail != 0) {
                violate(e, INV_I5_CREDIT, index, -1, (int)s, (int)d,
                        "ledger identity L%d failed", idfail);
            }
            uint64_t bytes =
                moqr_shards_debug_demand_channel_bytes(e->shards,
                                                       (uint16_t)s,
                                                       (uint16_t)d);
            bool model_occ = e->model.pair[s][d].occupancy > 0;

            /* one sound direction: content the model believes in-flight
             * must exist in production. Production may retain MORE under
             * downstream delivery backpressure (the model's drain is the
             * unblocked contract); the quiescent drain still requires the
             * channels to empty completely. */
            if (model_occ && bytes == 0) {
                violate(e, INV_I5_CREDIT, index, -1, (int)s, (int)d,
                        "occupancy %" PRIu64 " but channel bytes 0",
                        e->model.pair[s][d].occupancy);
                if (getenv("SCHED_DEBUG") != NULL) { /* transient */
                    moqr_bind_stats_t b0, b1;

                    moqr_bind_get_stats(moqr_shards_bind(e->shards, 0),
                                        &b0);
                    moqr_bind_get_stats(moqr_shards_bind(e->shards, 1),
                                        &b1);
                    fprintf(stderr,
                            "  dbg-i5: pair %u->%u ownerpumpsubs=%u/%u "
                            "b0(ev=%llu del=%llu ref=%llu err=%llu) "
                            "b1(ev=%llu del=%llu ref=%llu err=%llu)\n",
                            s, d,
                            moqr_shards_debug_owner_pump_subs(e->shards, 0),
                            moqr_shards_debug_owner_pump_subs(e->shards, 1),
                            (unsigned long long)b0.events_translated,
                            (unsigned long long)b0.deliveries_written,
                            (unsigned long long)b0.ingest_refusals,
                            (unsigned long long)b0.session_errors,
                            (unsigned long long)b1.events_translated,
                            (unsigned long long)b1.deliveries_written,
                            (unsigned long long)b1.ingest_refusals,
                            (unsigned long long)b1.session_errors);
                    for (uint32_t c2 = 0; c2 < SCHED_MAX_CHILDREN; c2++) {
                        if (!e->open[c2] || e->reaped[c2]) continue;
                        fprintf(stderr,
                                "  dbg-i5: child %u lane %u objs=%d "
                                "closed=%d\n", c2, e->model.child[c2].lane,
                                shx_ev_count(&e->drv[c2],
                                             MOQ_EVENT_OBJECT_RECEIVED),
                                shx_ev_count(&e->drv[c2],
                                             MOQ_EVENT_SESSION_CLOSED));
                    }
                }
            }
            dg(e, bytes > 0 ? 1u : 0u);
        }
    }
    /* the fake transport must never have silently lost fidelity */
    for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
        if (e->open[c] &&
            (e->drv[c].ep.overflowed || e->drv[c].ep.truncated)) {
            violate(e, INV_EXEC, index, (int)c, -1, -1,
                    "fake endpoint %s", e->drv[c].ep.overflowed
                                            ? "op overflow" : "truncation");
        }
    }
    /* I6: a child with no active subscription must not accumulate objects */
    for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
        if (!e->open[c] || e->frozen_objs[c] < 0) {
            continue;
        }
        int now = shx_ev_count(&e->drv[c], MOQ_EVENT_OBJECT_RECEIVED);

        if (now > e->frozen_objs[c]) {
            violate(e, INV_I6_CANCEL_PRECEDENCE, index, (int)c, -1, -1,
                    "objects %d after acknowledged cancel froze %d", now,
                    e->frozen_objs[c]);
        }
    }
}

/* bounded settle: shuttle every open driver + step both lanes, driving the
 * deterministic publisher policy (auto-accept + subgroup open) */
static void settle(exec_t *e, uint32_t index, int rounds)
{
    for (int r2 = 0; r2 < rounds; r2++) {
        for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
            if (e->open[c] && !e->reaped[c]) {
                (void)shx_round(&e->drv[c]);
            }
        }
        for (uint32_t l = 0; l < SCHED_LANES; l++) {
            (void)moq_msquic_lane_wake(e->lane[l]);
            (void)exec_step(e, l, index, false);
        }
        for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
            if (e->open[c] && !e->reaped[c]) {
                (void)shx_round(&e->drv[c]);
            }
            /* publisher policy: an upstream teardown retires the open
             * subgroup — the next accepted subscribe starts a fresh one */
            if (e->open[c]) {
                int un = shx_ev_count(&e->drv[c], MOQ_EVENT_UNSUBSCRIBED);

                if (un > e->unsub_seen[c]) {
                    e->unsub_seen[c] = un;
                    e->ps[c].sg_open = false;
                }
            }
            /* publisher policy: accept a subscribe, open one subgroup */
            if (e->open[c] && e->drv[c].got_subscribe) {
                e->drv[c].got_subscribe = false;
                pubst_t *ps = &e->ps[c];
                moq_accept_subscribe_cfg_t acfg;

                moq_accept_subscribe_cfg_init(&acfg);
                e->drv[c].now += 1000;
                (void)moq_session_accept_subscribe(
                    e->drv[c].sess, e->drv[c].subscribe_handle, &acfg,
                    e->drv[c].now);
                if (!ps->sg_open) {
                    moq_subgroup_cfg_t sgc;

                    moq_subgroup_cfg_init(&sgc);
                    /* a successor source publishes a FRESH group: the log
                     * retains the previous generation's records, and a
                     * re-publication into a retained (group, subgroup,
                     * object) identity is correctly refused as stale */
                    sgc.group_id = 1u + c;
                    sgc.publisher_priority = 100;
                    e->drv[c].now += 1000;
                    if (moq_session_open_subgroup(
                            e->drv[c].sess, e->drv[c].subscribe_handle,
                            &sgc, e->drv[c].now, &ps->sgh) == MOQ_OK) {
                        ps->sg_open = true;
                    }
                }
            }
        }
    }
}

/* -- op execution ------------------------------------------------------------ */

#define D_OK    0u
#define D_NOOP  1u
#define D_HELD  2u
#define D_PUMP  3u
#define D_IDLE  4u
#define D_TICK  5u
#define D_STOPD 6u

static const uint8_t frag_class[5] = { 0, 1, 2, 7, 255 }; /* 255 = len-1 */

static uint8_t
exec_op(exec_t *e, const seedx_rec_t *r, uint32_t index)
{
    switch (r->op) {
    case SCHED_OP_ACCEPT: {
        uint32_t c = r->a;
        moq_msq_test_child_row_t pre[SCHED_MAX_LIVE + 1];

        /* the recipe reclaims reapable terminals first: a terminal child
         * holds its facade slot until an UNARMED step reaches the
         * doorbell's idle loop-top. Bounded: one settle to flush owed
         * work, then bare steps per lane. */
        for (int rl = 0; rl < 16 &&
                         moq_msquic_managed_conn_count(e->m) >=
                             SCHED_MAX_LIVE;
             rl++) {
            settle(e, index, 1);
            for (uint32_t l = 0; l < SCHED_LANES; l++) {
                (void)exec_step(e, l, index, false);
                (void)exec_step(e, l, index, false);
            }
        }

        for (uint32_t l = 0; l < SCHED_LANES; l++) {
            size_t pn = 0;

            lane_bits(e, l, NULL, NULL, pre, SCHED_MAX_LIVE + 1, &pn);
            e->pre_n[l] = pn <= SCHED_MAX_LIVE ? pn : 0;
            for (size_t k = 0; k < e->pre_n[l]; k++) {
                e->pre_ids[l][k] = pre[k].id;
            }
        }
        if (!shx_driver_open(&e->drv[c], &e->fake, MOQ_VERSION_DRAFT_18,
                             "moqt-18")) {
            moq_msq_test_child_row_t dbg_rows[SCHED_MAX_LIVE + 1];
            char why[160];
            size_t off = 0;

            for (uint32_t l = 0; l < SCHED_LANES; l++) {
                size_t dn = 0;

                lane_bits(e, l, NULL, NULL, dbg_rows, SCHED_MAX_LIVE + 1,
                          &dn);
                for (size_t i2 = 0; i2 < dn && dn <= SCHED_MAX_LIVE &&
                                    off < sizeof(why) - 32; i2++) {
                    int who = -1;

                    for (uint32_t k2 = 0; k2 < SCHED_MAX_CHILDREN; k2++) {
                        if (!e->reaped[k2] &&
                            e->row_id[k2] == dbg_rows[i2].id) {
                            who = (int)k2;
                        }
                    }
                    off += (size_t)snprintf(why + off, sizeof(why) - off,
                                            " L%u#%d(sc%d,r%d,a%d,o%d,e%d)",
                                            l, who,
                                            dbg_rows[i2].shutdown_complete,
                                            dbg_rows[i2].reapable,
                                            dbg_rows[i2].app_terminal_acked,
                                            dbg_rows[i2].terminal_observed,
                                            dbg_rows[i2].has_events);
                }
            }
            violate(e, INV_EXEC, index, (int)c, -1, -1,
                    "driver open failed; linked:%s", why);
            return D_NOOP;
        }
        e->open[c] = true;
        e->drv[c].compact_ops = true; /* long runs: bounded op history */
        e->frozen_objs[c] = -1;
        /* identify the new row by diffing against the pre-accept row sets
         * (pointer identities are only comparable within one instant —
         * a reaped child's address can be recycled) */
        moq_msq_test_child_row_t rows[SCHED_MAX_LIVE + 1];
        int found_lane = -1;
        const void *new_id = NULL;

        for (uint32_t l = 0; l < SCHED_LANES; l++) {
            size_t n = 0;

            lane_bits(e, l, NULL, NULL, rows, SCHED_MAX_LIVE + 1, &n);
            for (size_t i = 0; i < n && n <= SCHED_MAX_LIVE; i++) {
                bool known = false;

                for (size_t k = 0; k < e->pre_n[l]; k++) {
                    if (e->pre_ids[l][k] == rows[i].id) {
                        known = true;
                    }
                }
                if (!known) {
                    found_lane = (int)l;
                    new_id = rows[i].id;
                }
            }
        }
        if (found_lane < 0) {
            violate(e, INV_I1_OWNERSHIP, index, (int)c, r->lane, -1,
                    "accepted child never appeared on any lane");
        } else {
            /* the freed identity of a reaped child can be recycled for
             * this accept: the new binding shadows any older claim */
            for (uint32_t k = 0; k < SCHED_MAX_CHILDREN; k++) {
                if (k != c && e->row_id[k] == new_id) {
                    if (e->model.child[k].used &&
                        !e->model.child[k].terminal) {
                        violate(e, INV_I4_EXACTLY_ONCE, index, (int)k, -1,
                                -1, "live child %u identity recycled", k);
                    }
                    e->reaped[k] = true;
                    e->row_id[k] = NULL;
                }
            }
            e->row_id[c] = new_id;
            if (found_lane != (int)r->lane) {
                violate(e, INV_I1_OWNERSHIP, index, (int)c, r->lane,
                        found_lane,
                        "child %u accepted on lane %d, ledger lane %u", c,
                        found_lane, r->lane);
            }
            bool bits = false;

            lane_bits(e, (uint32_t)found_lane, &bits, NULL, NULL, 0, NULL);
            if (!bits) {
                violate(e, INV_I2_OWED_WAKE, index, (int)c, found_lane, -1,
                        "accept armed no pump");
            }
        }
        return D_OK;
    }
    case SCHED_OP_STEP: {
        moq_msq_test_step_t s = exec_step(e, r->lane, index, false);

        return s == MOQ_MSQ_TEST_STEP_PUMPED ? D_PUMP :
               s == MOQ_MSQ_TEST_STEP_TICKED ? D_TICK :
               s == MOQ_MSQ_TEST_STEP_STOP ? D_STOPD : D_IDLE;
    }
    case SCHED_OP_WAKE: {
        bool before = false;

        lane_bits(e, r->lane, &before, NULL, NULL, 0, NULL);
        (void)moq_msquic_lane_wake(e->lane[r->lane]);
        if (before) {
            e->cov.wake_coalesced++;
        }
        bool after = false;

        lane_bits(e, r->lane, &after, NULL, NULL, 0, NULL);
        if (!after) {
            violate(e, INV_I2_OWED_WAKE, index, -1, r->lane, -1,
                    "wake left no pending bit");
        }
        return before ? D_NOOP : D_OK;
    }
    case SCHED_OP_SHUTTLE: {
        shx_driver_t *d = &e->drv[r->a];

        if (!e->open[r->a]) {
            return D_NOOP;
        }
        d->fragment = frag_class[r->b];
        bool moved = shx_round(d);

        d->fragment = 0;
        return moved ? D_OK : D_NOOP;
    }
    case SCHED_OP_ESTABLISH: {
        shx_driver_t *d = &e->drv[r->a];

        for (int i = 0; i < 24 &&
                        shx_ev_count(d, MOQ_EVENT_SETUP_COMPLETE) < 1;
             i++) {
            settle(e, index, 1);
        }
        if (shx_ev_count(d, MOQ_EVENT_SETUP_COMPLETE) != 1) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "establish incomplete");
            return D_NOOP;
        }
        settle(e, index, 4); /* fixed tail: the recipe's drain guarantee */
        return D_OK;
    }
    case SCHED_OP_ANNOUNCE: {
        shx_driver_t *d = &e->drv[r->a];
        static const char *ns_str[SCHED_NS_COUNT] = { SCHED_NS0_PART,
                                                      SCHED_NS1_PART };
        moq_bytes_t part = { (const uint8_t *)ns_str[r->b],
                             strlen(ns_str[r->b]) };
        moq_publish_namespace_cfg_t pcfg;

        moq_publish_namespace_cfg_init(&pcfg);
        pcfg.track_namespace = (moq_namespace_t){ .parts = &part,
                                                  .count = 1 };
        d->now += 1000;
        if (moq_session_publish_namespace(d->sess, &pcfg, d->now,
                                          &e->ann[r->a]) != MOQ_OK) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "publish_namespace refused");
            return D_NOOP;
        }
        settle(e, index, 4);
        return D_OK;
    }
    case SCHED_OP_WITHDRAW: {
        shx_driver_t *d = &e->drv[r->a];

        d->now += 1000;
        if (moq_session_publish_namespace_done(d->sess, e->ann[r->a],
                                               d->now) != MOQ_OK) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "withdraw refused");
            return D_NOOP;
        }
        settle(e, index, 4);
        return D_OK;
    }
    case SCHED_OP_REVOKE: {
        static const char *ns_str[SCHED_NS_COUNT] = { SCHED_NS0_PART,
                                                      SCHED_NS1_PART };
        moq_bytes_t part = { (const uint8_t *)ns_str[r->b],
                             strlen(ns_str[r->b]) };
        moqr_ns_t ns = { .parts = &part, .count = 1 };
        uint16_t owner = (uint16_t)sched_ns_owner((uint8_t)r->b);
        moqr_result_t rc = moqr_core_force_withdraw(
            moqr_shards_core(e->shards, owner), ns, 0x10u,
            e->drv[r->a].now);

        if (rc != MOQR_OK) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "force_withdraw rc=%d", (int)rc);
            return D_NOOP;
        }
        e->cov.revokes++;
        settle(e, index, 4);
        return D_OK;
    }
    case SCHED_OP_SUBSCRIBE: {
        shx_driver_t *d = &e->drv[r->a];
        static const char *ns_str[SCHED_NS_COUNT] = { SCHED_NS0_PART,
                                                      SCHED_NS1_PART };
        moq_bytes_t part = { (const uint8_t *)ns_str[r->b],
                             strlen(ns_str[r->b]) };
        moq_subscribe_cfg_t sc;

        moq_subscribe_cfg_init(&sc);
        sc.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
        sc.track_name = (moq_bytes_t){ (const uint8_t *)"t", 1 };
        sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
        /* a prior subscription to a withdrawn namespace may still be
         * draining at the driver: retry across settles, bounded */
        moq_result_t src = MOQ_ERR_WOULD_BLOCK;

        for (int i = 0; i < 8 && src != MOQ_OK; i++) {
            d->now += 1000;
            src = moq_session_subscribe(d->sess, &sc, d->now,
                                        &e->subh[r->a]);
            if (src != MOQ_OK) {
                settle(e, index, 2);
            }
        }
        if (src != MOQ_OK) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "subscribe refused rc=%d", (int)src);
            return D_NOOP;
        }
        int base = shx_ev_count(d, MOQ_EVENT_SUBSCRIBE_OK);

        for (int i = 0; i < 24 &&
                        shx_ev_count(d, MOQ_EVENT_SUBSCRIBE_OK) <= base;
             i++) {
            settle(e, index, 1);
        }
        if (shx_ev_count(d, MOQ_EVENT_SUBSCRIBE_OK) != base + 1) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "subscribe never accepted");
            return D_NOOP;
        }
        settle(e, index, 4); /* fixed tail: the recipe's drain guarantee */
        e->frozen_objs[r->a] = -1;
        return D_OK;
    }
    case SCHED_OP_CANCEL: {
        shx_driver_t *d = &e->drv[r->a];

        d->now += 1000;
        if (moq_session_unsubscribe(d->sess, e->subh[r->a], d->now) !=
            MOQ_OK) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "unsubscribe refused");
            return D_NOOP;
        }
        settle(e, index, 6);
        e->frozen_objs[r->a] =
            shx_ev_count(d, MOQ_EVENT_OBJECT_RECEIVED);
        e->cov.cancels++;
        return D_OK;
    }
    case SCHED_OP_PUSH: {
        shx_driver_t *d = &e->drv[r->a];
        pubst_t *ps = &e->ps[r->a];

        /* after a withdraw + republication the relay REVIVES parked
         * demand by re-subscribing upstream; wait for the scripted
         * revival (accept + fresh subgroup), bounded */
        for (int i = 0; i < 12 && !ps->sg_open; i++) {
            settle(e, index, 1);
        }
        if (!ps->sg_open) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "push with no open subgroup");
            return D_NOOP;
        }
        uint8_t body[32];
        moq_rcbuf_t *pl = NULL;

        memset(body, (int)(0xA0u + (e->oid[r->a] & 0xF)), sizeof(body));
        if (moq_rcbuf_create(moq_alloc_default(), body, sizeof(body),
                             &pl) < 0) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1, "rcbuf");
            return D_NOOP;
        }
        d->now += 1000;
        moq_result_t rc = moq_session_write_object(d->sess, ps->sgh,
                                                   e->oid[r->a]++, pl,
                                                   d->now);

        moq_rcbuf_decref(pl);
        if (rc != MOQ_OK) {
            violate(e, INV_EXEC, index, (int)r->a, -1, -1,
                    "write_object rc=%d", (int)rc);
            return D_NOOP;
        }
        /* move the object off the publisher and through the owner lane:
         * shuttle the pub, one owner-lane step, shuttle again */
        uint32_t owner = e->model.child[r->a].lane;

        (void)shx_round(d);
        (void)moq_msquic_lane_wake(e->lane[owner]);
        (void)exec_step(e, owner, index, false);
        (void)shx_round(d);
        /* disposition from the MODEL's admission arithmetic */
        bool held = false;

        for (uint32_t s2 = 0; s2 < SCHED_MAX_CHILDREN; s2++) {
            const sched_child_t *sub = &e->model.child[s2];

            if (sub->used && !sub->terminal && sub->sub_accepted &&
                sub->sub_ns == e->model.child[r->a].announced_ns &&
                sub->lane != owner &&
                e->model.pair[owner][sub->lane].occupancy >=
                    e->model.pair[owner][sub->lane].capacity) {
                held = true;
            }
        }
        if (held) {
            e->cov.held_offers++;
        }
        return held ? D_HELD : D_OK;
    }
    case SCHED_OP_CONSUME: {
        /* one requester-lane consuming step; the freed credit must wake
         * every producer lane still holding entries toward this lane */
        bool held_before[SCHED_LANES] = { false };

        for (uint32_t s2 = 0; s2 < SCHED_LANES; s2++) {
            if (s2 != r->lane &&
                e->model.pair[s2][r->lane].held_current > 0) {
                held_before[s2] = true;
            }
        }
        (void)moq_msquic_lane_wake(e->lane[r->lane]);
        (void)exec_step(e, r->lane, index, false);
        for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
            if (e->open[c] && !e->reaped[c]) {
                (void)shx_round(&e->drv[c]);
            }
        }
        for (uint32_t s2 = 0; s2 < SCHED_LANES; s2++) {
            if (!held_before[s2]) {
                continue;
            }
            bool bits = false;

            lane_bits(e, s2, &bits, NULL, NULL, 0, NULL);
            if (!bits) {
                violate(e, INV_I5_CREDIT, index, -1, (int)s2,
                        (int)r->lane,
                        "freed credit produced no producer-lane wake");
            } else {
                e->cov.credit_wakes++;
            }
        }
        return D_OK;
    }
    case SCHED_OP_RETRY: {
        (void)moq_msquic_lane_wake(e->lane[r->a]);
        (void)exec_step(e, r->a, index, false);
        for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
            if (e->open[c] && !e->reaped[c]) {
                (void)shx_round(&e->drv[c]);
            }
        }
        e->cov.retries++;
        return D_OK;
    }
    case SCHED_OP_TERMINAL: {
        shx_driver_t *d = &e->drv[r->a];

        fake_mgd_deliver_peer_close(d->child, 0);
        fake_mgd_deliver_shutdown_complete(d->child);
        e->lane_steps_since_term[r->a] = 0;
        e->cov.terminals++;
        bool bits = false;

        lane_bits(e, e->model.child[r->a].lane, &bits, NULL, NULL, 0, NULL);
        if (!bits) {
            violate(e, INV_I2_OWED_WAKE, index, (int)r->a,
                    (int)e->model.child[r->a].lane, -1,
                    "terminal batch armed no pump");
        }
        /* the failover recipe: the binding close retargets or terminates
         * standing demand during these settles (both lanes step) */
        settle(e, index, 6);
        return D_OK;
    }
    case SCHED_OP_CLOSE_FEED_FAULT: {
        uint32_t c = r->a;

        for (int rl = 0; rl < 16 &&
                         moq_msquic_managed_conn_count(e->m) >=
                             SCHED_MAX_LIVE;
             rl++) {
            settle(e, index, 1);
            for (uint32_t l = 0; l < SCHED_LANES; l++) {
                (void)exec_step(e, l, index, false);
                (void)exec_step(e, l, index, false);
            }
        }
        moq_msq_test_child_row_t pre[SCHED_MAX_LIVE + 1];

        for (uint32_t l = 0; l < SCHED_LANES; l++) {
            size_t pn = 0;

            lane_bits(e, l, NULL, NULL, pre, SCHED_MAX_LIVE + 1, &pn);
            e->pre_n[l] = pn <= SCHED_MAX_LIVE ? pn : 0;
            for (size_t k = 0; k < e->pre_n[l]; k++) {
                e->pre_ids[l][k] = pre[k].id;
            }
        }
        shx_arm_stream_start_fail = true;
        if (!shx_driver_open(&e->drv[c], &e->fake, MOQ_VERSION_DRAFT_18,
                             "moqt-18")) {
            shx_arm_stream_start_fail = false;
            violate(e, INV_EXEC, index, (int)c, -1, -1,
                    "faulted driver open failed");
            return D_NOOP;
        }
        e->open[c] = true;
        e->drv[c].compact_ops = true;
        e->frozen_objs[c] = -1;
        /* identify + facet-check the faulted child on its expected lane */
        moq_msq_test_child_row_t rows[SCHED_MAX_LIVE + 1];
        const moq_msq_test_child_row_t *fr = NULL;
        int found_lane = -1;

        for (uint32_t l = 0; l < SCHED_LANES; l++) {
            size_t n = 0;

            lane_bits(e, l, NULL, NULL, rows, SCHED_MAX_LIVE + 1, &n);
            for (size_t i = 0; i < n && n <= SCHED_MAX_LIVE; i++) {
                bool known = false;

                for (size_t k = 0; k < e->pre_n[l]; k++) {
                    if (e->pre_ids[l][k] == rows[i].id) {
                        known = true;
                    }
                }
                if (!known) {
                    found_lane = (int)l;
                    fr = &rows[i];
                    for (uint32_t k2 = 0; k2 < SCHED_MAX_CHILDREN; k2++) {
                        if (k2 != c && e->row_id[k2] == rows[i].id) {
                            e->reaped[k2] = true;
                            e->row_id[k2] = NULL;
                        }
                    }
                    e->row_id[c] = rows[i].id;
                }
            }
            if (fr != NULL) {
                break;
            }
        }
        if (fr == NULL || found_lane != (int)r->lane) {
            violate(e, INV_I1_OWNERSHIP, index, (int)c, r->lane,
                    found_lane, "faulted child lane mismatch");
            return D_NOOP;
        }
        /* facet 1: the bridge latched its FIRST fatal (0x1) and nothing
         * else happened — no transport terminal, no feed, session open */
        if (!fr->bridge_fatal || fr->bridge_fatal_code != 0x1 ||
            fr->shutdown_complete || fr->close_feed_commits != 0 ||
            fr->terminal_observed) {
            violate(e, INV_EXEC, index, (int)c, -1, -1,
                    "fault facets wrong: fatal=%d code=%llx sc=%d feeds=%u",
                    fr->bridge_fatal,
                    (unsigned long long)fr->bridge_fatal_code,
                    fr->shutdown_complete, fr->close_feed_commits);
            return D_NOOP;
        }
        /* facet 2: the transport terminal arrives (orderly, code 0) and
         * the close feed commits EXACTLY once; a duplicate completion is
         * idempotent */
        fake_mgd_deliver_peer_close(e->drv[c].child, 0);
        fake_mgd_deliver_shutdown_complete(e->drv[c].child);
        fake_mgd_deliver_shutdown_complete(e->drv[c].child);
        {
            size_t n = 0;
            const moq_msq_test_child_row_t *cr2 = NULL;

            lane_bits(e, (uint32_t)found_lane, NULL, NULL, rows,
                      SCHED_MAX_LIVE + 1, &n);
            for (size_t i = 0; i < n && n <= SCHED_MAX_LIVE; i++) {
                if (rows[i].id == e->row_id[c]) {
                    cr2 = &rows[i];
                }
            }
            if (cr2 == NULL || !cr2->shutdown_complete ||
                cr2->close_feed_commits != 1 ||
                cr2->bridge_fatal_code != 0x1) {
                violate(e, INV_EXEC, index, (int)c, -1, -1,
                        "terminal facets wrong: sc=%d feeds=%u code=%llx",
                        cr2 ? cr2->shutdown_complete : -1,
                        cr2 ? cr2->close_feed_commits : 0,
                        cr2 ? (unsigned long long)cr2->bridge_fatal_code
                            : 0ull);
                return D_NOOP;
            }
        }
        e->lane_steps_since_term[c] = 0;
        e->cov.terminals++;
        return D_OK;
    }
    case SCHED_OP_STOP: {
        uint64_t turns_before[SCHED_LANES];

        for (uint32_t l = 0; l < SCHED_LANES; l++) {
            turns_before[l] = moqr_test_lanes_ctx_pump_turns(e->ctx, l);
        }
        (void)moq_msquic_managed_stop(e->m);
        e->cov.stops++;
        /* I7: stop returns with every child reclaimed, and no application
         * entry occurs after the publication boundary (0168: the stop
         * atomic is acquire-checked at every doorbell loop top) */
        if (moq_msquic_managed_conn_count(e->m) != 0) {
            violate(e, INV_I7_STOP_ORDER, index, -1, -1, -1,
                    "stop returned with %u children",
                    (unsigned)moq_msquic_managed_conn_count(e->m));
        }
        for (uint32_t l = 0; l < SCHED_LANES; l++) {
            /* the 0168 property: after the stop publication boundary, a
             * wake plus a step may classify however the doorbell likes,
             * but NO application callback entry may occur */
            (void)moq_msquic_lane_wake(e->lane[l]);
            moq_msq_test_step_t s2 = moq_msq_test_lane_step(e->lane[l]);

            dg(e, (uint64_t)s2);
            if (moqr_test_lanes_ctx_pump_turns(e->ctx, l) !=
                turns_before[l]) {
                violate(e, INV_I7_STOP_ORDER, index, -1, (int)l, -1,
                        "app entry after stop (step=%d)", (int)s2);
            }
        }
        return D_STOPD;
    }
    default:
        violate(e, INV_EXEC, index, -1, -1, -1, "unknown op %u", r->op);
        return D_NOOP;
    }
}

/* -- run one trace against a fresh rig -------------------------------------- */

static bool exec_up(exec_t *e)
{
    memset(e, 0, sizeof(*e));
    for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
        e->frozen_objs[c] = -1;
    }
    e->fail_child = -1;
    e->fail_pair_src = e->fail_pair_dst = -1;
    e->digest = 0xcbf29ce484222325ull;
    sched_model_init(&e->model);
    fake_mgd_init(&e->fake);
    moq_msq_test_api_override = fake_mgd_table(&e->fake);
    moq_msq_test_no_doorbell = true;

    moqr_shards_cfg_t shcfg;

    moqr_shards_cfg_init_sized(&shcfg, sizeof(shcfg), moq_alloc_default());
    shcfg.shards = SCHED_LANES;
    shcfg.admit_remote_demand = true;
    shcfg.live_visibility = true;
    shcfg.demand_channel_entries = SCHED_CREDIT_CAP;
    if (moqr_shards_create(&shcfg, &e->shards) != MOQR_OK) {
        return false;
    }
    /* the ns->shard constants are configuration: assert them against the
     * production placement before anything runs */
    static const char *ns_str[SCHED_NS_COUNT] = { SCHED_NS0_PART,
                                                  SCHED_NS1_PART };

    for (uint32_t i = 0; i < SCHED_NS_COUNT; i++) {
        moq_bytes_t part = { (const uint8_t *)ns_str[i], strlen(ns_str[i]) };

        if (moqr_shards_debug_hrw_winner(0x3u, &part, 1) !=
            (int32_t)sched_ns_owner((uint8_t)i)) {
            fprintf(stderr, "ns placement drift: %s\n", ns_str[i]);
            return false;
        }
    }
    e->ctx = moqr_test_mk_lanes_ctx(e->shards, SCHED_LANES);

    moq_msquic_managed_cfg_t scfg;

    moq_msquic_managed_cfg_init_sized(&scfg, sizeof(scfg));
    scfg.alloc = moq_alloc_default();
    scfg.perspective = MOQ_PERSPECTIVE_SERVER;
    scfg.host = NULL;
    scfg.port = 0;
    scfg.cert_path = "unused-by-the-fake-cert.pem";
    scfg.key_path = "unused-by-the-fake-key.pem";
    scfg.send_request_capacity = true;
    scfg.initial_request_capacity = 1024;
    scfg.version = MOQ_VERSION_DRAFT_18;
    scfg.lane_count = SCHED_LANES;
    scfg.max_connections = SCHED_MAX_LIVE;
    scfg.on_lane_pump = moqr_test_lanes_pump();
    scfg.on_lane_pump_user = e->ctx;
    if (moq_msquic_managed_create(&scfg, &e->m) != MOQ_OK) {
        return false;
    }
    for (uint32_t l = 0; l < SCHED_LANES; l++) {
        e->lane[l] = moq_msquic_managed_lane(e->m, l);
        if (e->lane[l] == NULL) {
            return false;
        }
    }
    return true;
}

static void exec_down(exec_t *e)
{
    for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
        if (e->open[c]) {
            shx_driver_close(&e->drv[c]);
            e->open[c] = false;
        }
    }
    if (e->m != NULL) {
        (void)moq_msquic_managed_stop(e->m);
        moq_msquic_managed_destroy(e->m);
    }
    free(e->ctx);
    if (e->shards != NULL) {
        moqr_shards_destroy(e->shards);
    }
    moq_msq_test_api_override = NULL;
    moq_msq_test_no_doorbell = false;
}

/* Runs the records; on success also runs the quiescent drain. Returns the
 * first violated invariant (INV_NONE if clean). `disp` must hold count. */
static sched_inv_t
run_records(exec_t *e, const seedx_rec_t *recs, uint32_t count,
            uint8_t *disp)
{
    if (!exec_up(e)) {
        exec_down(e);
        return INV_EXEC;
    }
    e->disp = disp;
    e->disp_len = 0;
    for (uint32_t i = 0; i < count && e->violated == INV_NONE; i++) {
        sched_op_t op = { recs[i].op, recs[i].lane, recs[i].a, recs[i].b,
                          recs[i].c };

        if (!sched_model_apply(&e->model, &op)) {
            violate(e, INV_EXEC, i, -1, -1, -1,
                    "record %u not precondition-valid", i);
            break;
        }
        e->cov.op_kind[recs[i].op]++;
        uint8_t d = exec_op(e, &recs[i], i);

        disp[e->disp_len++] = d;
        dg(e, ((uint64_t)recs[i].op << 8) | d);
        if (recs[i].op != SCHED_OP_STOP) {
            observe(e, i);
        }
        /* record pair offers for coverage after the model applied them */
        if (recs[i].op == SCHED_OP_PUSH) {
            for (uint32_t s2 = 0; s2 < SCHED_LANES; s2++) {
                for (uint32_t d2 = 0; d2 < SCHED_LANES; d2++) {
                    e->cov.pair_offers[s2][d2] =
                        e->model.pair[s2][d2].unique_offered_total;
                }
            }
        }
    }
    /* quiescent drain (skipped after STOP): bounded by the derived
     * per-child reap bound plus one settle round per live child */
    if (e->violated == INV_NONE && !e->model.stopped) {
        uint32_t bound = 8u + 6u * SCHED_MAX_CHILDREN;
        bool quiescent = false;

        for (uint32_t i = 0; i < bound && !quiescent; i++) {
            settle(e, e->disp_len, 1);
            for (uint32_t l = 0; l < SCHED_LANES; l++) {
                (void)exec_step(e, l, e->disp_len, true); /* unarmed: reap */
            }
            e->cov.drain_steps++;
            quiescent = true;
            for (uint32_t s2 = 0; s2 < SCHED_LANES; s2++) {
                for (uint32_t d2 = 0; d2 < SCHED_LANES; d2++) {
                    if (s2 != d2 &&
                        moqr_shards_debug_demand_channel_bytes(
                            e->shards, (uint16_t)s2, (uint16_t)d2) > 0) {
                        quiescent = false;
                    }
                }
            }
            for (uint32_t c = 0; c < SCHED_MAX_CHILDREN; c++) {
                if (e->model.child[c].used && e->model.child[c].terminal &&
                    !e->reaped[c]) {
                    quiescent = false;
                }
            }
            observe(e, e->disp_len);
            if (e->violated != INV_NONE) {
                break;
            }
        }
        if (e->violated == INV_NONE && !quiescent) {
            violate(e, INV_DRAIN, e->disp_len, -1, -1, -1,
                    "not quiescent within %u drain rounds", bound);
        }
    }
    sched_inv_t v = e->violated;

    exec_down(e);
    return v;
}

/* -- failure reporting, ddmin ----------------------------------------------- */

static void
report_failure(const exec_t *e, uint64_t seed, const uint8_t *trace,
               size_t trace_len)
{
    uint8_t dsum[32];
    char hex[65];

    sched_sha256_of(trace, trace_len, dsum);
    sched_sha256_hex(dsum, hex);
    printf("VIOLATION seed=0x%016" PRIx64
           " prng=splitmix64-v1 trace=v1 grammar=%u config=%016" PRIx64 "\n"
           "  trace_sha256=%s\n"
           "  op_index=%u invariant=%s causal_child=%d causal_pair=%d->%d\n"
           "  delta: %s\n",
           seed, SCHED_GRAMMAR_VERSION, sched_config_hash(), hex,
           e->fail_index, inv_name(e->violated), e->fail_child,
           e->fail_pair_src, e->fail_pair_dst, e->fail_delta);
}

/* validity: every record must be enabled from reset — no repair, no skip */
static bool
candidate_valid(const seedx_rec_t *recs, uint32_t count)
{
    sched_model_t m;

    sched_model_init(&m);
    for (uint32_t i = 0; i < count; i++) {
        sched_op_t op = { recs[i].op, recs[i].lane, recs[i].a, recs[i].b,
                          recs[i].c };

        if (!sched_model_apply(&m, &op)) {
            return false;
        }
    }
    return true;
}

static uint32_t
ddmin(seedx_rec_t *recs, uint32_t count, sched_inv_t want_inv,
      int want_child, int want_src, int want_dst)
{
    uint32_t n = 2;

    while (count >= 2) {
        bool reduced = false;
        uint32_t chunk = count / n;

        if (chunk == 0) {
            break;
        }
        for (uint32_t i = 0; i < n && !reduced; i++) {
            /* complement of chunk i */
            seedx_rec_t cand[SCHED_OPS_PER_SEED];
            uint32_t cn = 0;
            uint32_t lo = i * chunk;
            uint32_t hi = (i == n - 1) ? count : lo + chunk;

            for (uint32_t k = 0; k < count; k++) {
                if (k < lo || k >= hi) {
                    cand[cn++] = recs[k];
                }
            }
            if (cn == count || cn == 0 || !candidate_valid(cand, cn)) {
                continue;
            }
            exec_t *ex = malloc(sizeof(*ex));
            uint8_t disp[SCHED_OPS_PER_SEED];
            sched_inv_t v = run_records(ex, cand, cn, disp);
            bool same = v == want_inv && ex->fail_child == want_child &&
                        ex->fail_pair_src == want_src &&
                        ex->fail_pair_dst == want_dst;

            free(ex);
            if (same) {
                memcpy(recs, cand, cn * sizeof(*cand));
                count = cn;
                n = n > 2 ? n - 1 : 2;
                reduced = true;
            }
        }
        if (!reduced) {
            if (n >= count) {
                break;
            }
            n = n * 2 > count ? count : n * 2;
        }
    }
    return count;
}

/* -- modes -------------------------------------------------------------------- */

typedef struct seed_verdict {
    bool        failed;
    sched_inv_t inv;
    uint32_t    fail_index;
} seed_verdict_t;

static int run_one_seed_v(uint64_t seed, bool with_replay, bool shrink,
                          seed_verdict_t *out);

static int run_one_seed(uint64_t seed, bool with_replay, bool shrink)
{
    return run_one_seed_v(seed, with_replay, shrink, NULL);
}

static int run_one_seed_v(uint64_t seed, bool with_replay, bool shrink,
                          seed_verdict_t *out)
{
    static seedx_rec_t recs[SCHED_OPS_PER_SEED];
    static uint8_t trace[SEEDX_TRACE_HDR +
                         SCHED_OPS_PER_SEED * SEEDX_TRACE_REC];
    uint32_t count = sched_generate(seed, recs, NULL);
    size_t tlen = seedx_trace_encode(seed, sched_config_hash(), recs, count,
                                     trace, sizeof(trace));
    static uint8_t disp_a[SCHED_OPS_PER_SEED], disp_b[SCHED_OPS_PER_SEED];
    exec_t *ea = malloc(sizeof(*ea));
    sched_inv_t va = run_records(ea, recs, count, disp_a);

    if (out != NULL) {
        out->failed = va != INV_NONE;
        out->inv = va;
        out->fail_index = ea->fail_index;
    }
    if (va != INV_NONE) {
        report_failure(ea, seed, trace, tlen);
        if (shrink) {
            sched_inv_t wi = va;
            int wc = ea->fail_child, ws = ea->fail_pair_src,
                wd = ea->fail_pair_dst;
            uint32_t mn = ddmin(recs, count, wi, wc, ws, wd);

            if (mn < count) {
                exec_t *em = malloc(sizeof(*em));
                sched_inv_t vm = run_records(em, recs, mn, disp_b);

                printf("MINIMIZED ops=%u (from %u) — replays %s "
                       "child=%d pair=%d->%d\n", mn, count, inv_name(vm),
                       em->fail_child, em->fail_pair_src,
                       em->fail_pair_dst);
                size_t ml = seedx_trace_encode(seed, sched_config_hash(),
                                               recs, mn, trace,
                                               sizeof(trace));
                uint8_t dsum[32];
                char hex[65];

                sched_sha256_of(trace, ml, dsum);
                sched_sha256_hex(dsum, hex);
                printf("MINIMIZED trace_sha256=%s\n", hex);
                const char *out = getenv("SCHED_MIN_TRACE_OUT");

                if (out != NULL) {
                    FILE *f = fopen(out, "wb");

                    if (f != NULL) {
                        fwrite(trace, 1, ml, f);
                        fclose(f);
                    }
                }
                free(em);
            } else {
                printf("MINIMIZED: no smaller valid trace found\n");
            }
        }
        free(ea);
        return 1;
    }
    if (with_replay) {
        /* decode the canonical bytes and replay in a second fresh rig
         * without the PRNG */
        static seedx_rec_t arena[SCHED_OPS_PER_SEED];
        seedx_trace_t t;
        seedx_trace_err_t de =
            seedx_trace_decode(trace, tlen, sched_config_hash(),
                               SCHED_OP_MAX, sched_field_ok, arena,
                               SCHED_OPS_PER_SEED, &t);

        if (de != SEEDX_TRACE_OK) {
            printf("FAIL: self-decode: %s\n", seedx_trace_err_name(de));
            free(ea);
            return 1;
        }
        exec_t *eb = malloc(sizeof(*eb));
        sched_inv_t vb = run_records(eb, t.recs, t.op_count, disp_b);
        int rc = 0;

        if (vb != INV_NONE) {
            printf("FAIL: replay violated %s at %u\n", inv_name(vb),
                   eb->fail_index);
            rc = 1;
        } else if (t.op_count != count ||
                   memcmp(disp_a, disp_b, count) != 0) {
            printf("FAIL: replay disposition drift\n");
            rc = 1;
        } else if (eb->digest != ea->digest) {
            printf("FAIL: replay observation-digest drift %016" PRIx64
                   " vs %016" PRIx64 "\n", ea->digest, eb->digest);
            rc = 1;
        } else if (memcmp(&ea->cov, &eb->cov, sizeof(ea->cov)) != 0) {
            printf("FAIL: replay coverage drift\n");
            rc = 1;
        } else if (memcmp(&ea->model, &eb->model, sizeof(ea->model)) != 0) {
            printf("FAIL: replay model final-state drift\n");
            rc = 1;
        }
        free(eb);
        if (rc != 0) {
            free(ea);
            return rc;
        }
    }
    free(ea);
    return 0;
}

static int run_seeds(void)
{
    /* The committed acceptance surface: every seed must be fully green
     * with PRNG-free replay identity. The EXPECT_RED table is the gate's
     * standing mechanism for a known deterministic defect (each listed
     * seed must fail the SAME invariant at the SAME op index on a replay
     * of its canonical bytes, and an expected-RED seed that starts passing
     * is a loud gate failure — the expectation is retired deliberately,
     * never silently). It is EMPTY: no known defect stands, and the
     * mandatory exact scripts carry the coverage the seeds may not draw. */
    static const struct { uint32_t index; const char *reason; } EXPECT_RED[] = {
        { UINT32_MAX, "" },
    };
    sched_cov_t total;
    uint64_t effective = 0;
    int gate_failures = 0;

    memset(&total, 0, sizeof(total));
    for (uint32_t i2 = 0; i2 < SCHED_SEED_COUNT; i2++) {
        uint64_t seed = sched_seed(i2);
        const char *expect_red = NULL;

        for (size_t k = 0; k < sizeof(EXPECT_RED) / sizeof(EXPECT_RED[0]);
             k++) {
            if (EXPECT_RED[k].index == i2) {
                expect_red = EXPECT_RED[k].reason;
            }
        }
        static seedx_rec_t recs[SCHED_OPS_PER_SEED];
        uint32_t count = sched_generate(seed, recs, NULL);

        effective += count;
        seed_verdict_t v;
        struct timespec t0, t1;

        clock_gettime(CLOCK_MONOTONIC, &t0); /* report-only measurement */
        int rc = run_one_seed_v(seed, expect_red == NULL, false, &v);

        clock_gettime(CLOCK_MONOTONIC, &t1);
        double ms = (double)(t1.tv_sec - t0.tv_sec) * 1e3 +
                    (double)(t1.tv_nsec - t0.tv_nsec) / 1e6;

        printf("TIMING seed %2u: %3u ops %7.2f ms %s\n", i2, count, ms,
               expect_red != NULL ? "expected-red"
                                  : (rc == 0 ? "green" : "RED"));

        if (expect_red == NULL) {
            if (rc != 0) {
                printf("FAIL: seed %u unexpectedly RED\n", i2);
                gate_failures++;
                continue;
            }
        } else {
            if (!v.failed) {
                printf("FAIL: seed %u expected RED (%s) but PASSED — "
                       "retire the expectation deliberately\n", i2,
                       expect_red);
                gate_failures++;
                continue;
            }
            /* determinism: the canonical bytes must fail identically on a
             * PRNG-free replay */
            static uint8_t trace[SEEDX_TRACE_HDR +
                                 SCHED_OPS_PER_SEED * SEEDX_TRACE_REC];
            size_t tlen = seedx_trace_encode(seed, sched_config_hash(),
                                             recs, count, trace,
                                             sizeof(trace));
            static seedx_rec_t arena[SCHED_OPS_PER_SEED];
            seedx_trace_t t;

            if (seedx_trace_decode(trace, tlen, sched_config_hash(),
                                   SCHED_OP_MAX, sched_field_ok, arena,
                                   SCHED_OPS_PER_SEED, &t) !=
                SEEDX_TRACE_OK) {
                printf("FAIL: seed %u self-decode\n", i2);
                gate_failures++;
                continue;
            }
            static uint8_t disp[SCHED_OPS_PER_SEED];
            exec_t *er = malloc(sizeof(*er));
            sched_inv_t vr = run_records(er, t.recs, t.op_count, disp);

            if (vr != v.inv || er->fail_index != v.fail_index) {
                printf("FAIL: seed %u expected-RED not deterministic "
                       "(%s@%u vs %s@%u)\n", i2, inv_name(v.inv),
                       v.fail_index, inv_name(vr), er->fail_index);
                gate_failures++;
            } else {
                printf("EXPECTED-RED seed %u: %s@%u (%s)\n", i2,
                       inv_name(v.inv), v.fail_index, expect_red);
            }
            free(er);
            continue; /* no coverage accumulation from RED seeds */
        }
        /* coverage from the green seeds */
        exec_t *ex = malloc(sizeof(*ex));
        static uint8_t disp2[SCHED_OPS_PER_SEED];

        (void)run_records(ex, recs, count, disp2);
        for (uint32_t k = 0; k <= SCHED_OP_MAX; k++) {
            total.op_kind[k] += ex->cov.op_kind[k];
        }
        for (uint32_t l = 0; l < SCHED_LANES; l++) {
            total.lane_steps[l] += ex->cov.lane_steps[l];
        }
        for (uint32_t s2 = 0; s2 < SCHED_LANES; s2++) {
            for (uint32_t d2 = 0; d2 < SCHED_LANES; d2++) {
                total.pair_offers[s2][d2] += ex->cov.pair_offers[s2][d2];
            }
        }
        total.held_offers += ex->cov.held_offers;
        total.retries += ex->cov.retries;
        total.cancels += ex->cov.cancels;
        total.terminals += ex->cov.terminals;
        total.stops += ex->cov.stops;
        total.wake_coalesced += ex->cov.wake_coalesced;
        total.drain_steps += ex->cov.drain_steps;
        total.revokes += ex->cov.revokes;
        total.credit_wakes += ex->cov.credit_wakes;
        free(ex);
    }
    /* the coverage gate over the green seeds: every operation kind
     * (reserved excluded), both lanes, both directed pairs,
     * refusal/hold/retry, cancellation, terminal, stop, wake coalescing,
     * quiescent drain, and the two mandatory arms */
    int missing = gate_failures;

    for (uint32_t k = 1; k <= SCHED_OP_MAX; k++) {
        if (k == SCHED_OP_CLOSE_FEED_FAULT) {
            continue;
        }
        if (total.op_kind[k] == 0) {
            printf("FAIL: coverage: op 0x%02x never visited\n", k);
            missing++;
        }
    }
    if (total.lane_steps[0] == 0 || total.lane_steps[1] == 0 ||
        total.pair_offers[0][1] == 0 || total.pair_offers[1][0] == 0 ||
        total.held_offers == 0 || total.retries == 0 ||
        total.cancels == 0 || total.terminals == 0 || total.stops == 0 ||
        total.wake_coalesced == 0 || total.drain_steps == 0 ||
        total.revokes == 0 || total.credit_wakes == 0) {
        printf("FAIL: coverage gate: lanes %" PRIu64 "/%" PRIu64
               " pairs %" PRIu64 "/%" PRIu64 " held %" PRIu64 " retry %"
               PRIu64 " cancel %" PRIu64 " terminal %" PRIu64 " stop %"
               PRIu64 " coalesced %" PRIu64 " drain %" PRIu64 " revoke %"
               PRIu64 " credit-wake %" PRIu64 "\n",
               total.lane_steps[0], total.lane_steps[1],
               total.pair_offers[0][1], total.pair_offers[1][0],
               total.held_offers, total.retries, total.cancels,
               total.terminals, total.stops, total.wake_coalesced,
               total.drain_steps, total.revokes, total.credit_wakes);
        missing++;
    }
    if (effective < 3000) {
        printf("FAIL: aggregate effective ops %" PRIu64 " < 3000\n",
               effective);
        missing++;
    }
    if (missing != 0) {
        return 1;
    }
    printf("PASS: sched_explore --seeds (%u seeds green with replay "
           "identity; %" PRIu64
           " effective ops, revokes %" PRIu64 ", credit-wakes %" PRIu64
       ", held %" PRIu64 ", coalesced %" PRIu64 ")\n",
           SCHED_SEED_COUNT, effective, total.revokes, total.credit_wakes,
           total.held_offers, total.wake_coalesced);
    return 0;
}

static int run_trace_file(const char *path)
{
    static uint8_t buf[SEEDX_TRACE_HDR +
                       SCHED_OPS_PER_SEED * SEEDX_TRACE_REC + 64];
    FILE *f = fopen(path, "rb");

    if (f == NULL) {
        printf("ERROR: trace file unreadable: %s\n", path);
        return 2;
    }
    size_t len = fread(buf, 1, sizeof(buf), f);

    fclose(f);
    static seedx_rec_t arena[SCHED_OPS_PER_SEED];
    seedx_trace_t t;
    seedx_trace_err_t de =
        seedx_trace_decode(buf, len, sched_config_hash(), SCHED_OP_MAX,
                           sched_field_ok, arena, SCHED_OPS_PER_SEED, &t);

    if (de != SEEDX_TRACE_OK) {
        printf("ERROR: trace decode: %s\n", seedx_trace_err_name(de));
        return 2;
    }
    if (!candidate_valid(t.recs, t.op_count)) {
        printf("ERROR: trace is not precondition-valid from reset\n");
        return 2;
    }
    static uint8_t disp[SCHED_OPS_PER_SEED];
    exec_t *ex = malloc(sizeof(*ex));
    sched_inv_t v = run_records(ex, t.recs, t.op_count, disp);
    int rc = 0;

    if (v != INV_NONE) {
        report_failure(ex, t.seed, buf, len);
        rc = 1;
    } else {
        printf("PASS: trace replay (%u ops)\n", t.op_count);
        if (getenv("SCHED_DEBUG") != NULL) {
            for (uint32_t c2 = 0; c2 < SCHED_MAX_CHILDREN; c2++) {
                if (!ex->open[c2]) continue;
                for (int k4 = 0; k4 < SHX_MAX_STREAMS; k4++) {
                    const shx_tap_t *tp = &ex->drv[c2].tap[k4];

                    if (!tp->used) continue;
                    fprintf(stderr, "  dbg-pass child %u tap stream=%llu "
                            "len=%zu fin=%d head=", c2,
                            (unsigned long long)tp->id, tp->len,
                            tp->fin_seen);
                    for (size_t k5 = 0; k5 < tp->len && k5 < 12; k5++) {
                        fprintf(stderr, "%02x", tp->bytes[k5]);
                    }
                    fprintf(stderr, "\n");
                }
            }
        }
    }
    free(ex);
    return rc;
}

/* -- CLI robustness units ----------------------------------------------------- */

static int g_failures;

#define T_CHECK(expr)                                                      \
    do {                                                                   \
        if (!(expr)) {                                                     \
            g_failures++;                                                  \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);        \
        }                                                                  \
    } while (0)

static bool parse_seed_arg(const char *s, uint64_t *out)
{
    if (s == NULL || s[0] == '\0') {
        return false;
    }
    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 0);

    if (end == s || *end != '\0') {
        return false;
    }
    *out = (uint64_t)v;
    return true;
}

static int run_cli_units(void)
{
    int before = g_failures;
    uint64_t v = 0;

    /* malformed --seed arguments */
    T_CHECK(!parse_seed_arg(NULL, &v));
    T_CHECK(!parse_seed_arg("", &v));
    T_CHECK(!parse_seed_arg("0xZZ", &v));
    T_CHECK(!parse_seed_arg("12 34", &v));
    T_CHECK(parse_seed_arg("0x2a", &v) && v == 42);

    /* malformed --trace bytes: every named decode error is reachable and
     * fails closed */
    static seedx_rec_t recs[4];
    static uint8_t buf[SEEDX_TRACE_HDR + 4 * SEEDX_TRACE_REC];
    recs[0].op = SCHED_OP_STEP;
    size_t len = seedx_trace_encode(1, sched_config_hash(), recs, 1, buf,
                                    sizeof(buf));
    static seedx_rec_t arena[4];
    seedx_trace_t t;

    T_CHECK(seedx_trace_decode(buf, len, sched_config_hash(), SCHED_OP_MAX,
                               sched_field_ok, arena, 4, &t) ==
            SEEDX_TRACE_OK);
    buf[0] ^= 0xFF; /* magic */
    T_CHECK(seedx_trace_decode(buf, len, sched_config_hash(), SCHED_OP_MAX,
                               sched_field_ok, arena, 4, &t) ==
            SEEDX_TRACE_E_MAGIC);
    buf[0] ^= 0xFF;
    T_CHECK(seedx_trace_decode(buf, len - 1, sched_config_hash(),
                               SCHED_OP_MAX, sched_field_ok, arena, 4,
                               &t) == SEEDX_TRACE_E_TRUNCATED);
    /* manifest/config mismatch is a named error, never a silent replay */
    T_CHECK(seedx_trace_decode(buf, len, sched_config_hash() ^ 1u,
                               SCHED_OP_MAX, sched_field_ok, arena, 4,
                               &t) == SEEDX_TRACE_E_CONFIG);
    /* the close-feed op is ACTIVE grammar — its record decodes, and
     * validity is the model's precondition, not the codec's */
    seedx_rec_t rr[1];

    memset(rr, 0, sizeof(rr));
    rr[0].op = SCHED_OP_CLOSE_FEED_FAULT;
    len = seedx_trace_encode(1, sched_config_hash(), rr, 1, buf,
                             sizeof(buf));
    T_CHECK(seedx_trace_decode(buf, len, sched_config_hash(), SCHED_OP_MAX,
                               sched_field_ok, arena, 4, &t) ==
            SEEDX_TRACE_OK);
    if (g_failures == before) {
        printf("PASS: sched_cli_units\n");
    }
    return g_failures == before ? 0 : 1;
}

/* the exact close-feed scenario: one faulted admission, transport
 * terminal + duplicate, one owed pump, bounded reclamation — every facet
 * asserted, no wall time */
static int run_close_feed_scenario(void)
{
    static seedx_rec_t recs[8];
    static uint8_t disp[8];
    uint32_t n = 0;

    memset(recs, 0, sizeof(recs));
    recs[n].op = SCHED_OP_CLOSE_FEED_FAULT;
    recs[n].lane = 0;
    recs[n].a = 0;
    n++;
    recs[n].op = SCHED_OP_STEP;   /* the owed pump: observe + ack + reap */
    recs[n].lane = 0;
    n++;
    recs[n].op = SCHED_OP_STEP;   /* idle: nothing left */
    recs[n].lane = 0;
    n++;
    exec_t *ex = malloc(sizeof(*ex));
    sched_inv_t v = run_records(ex, recs, n, disp);
    int rc = 0;

    if (v != INV_NONE) {
        printf("FAIL: close-feed scenario violated %s at %u (%s)\n",
               inv_name(v), ex->fail_index, ex->fail_delta);
        rc = 1;
    } else {
        printf("PASS: sched_close_feed (fatal 0x1 preserved, one feed "
               "commit, duplicate idempotent, reclaimed)\n");
    }
    free(ex);
    return rc;
}

int main(int argc, char **argv)
{
    shx_touch_shared_helpers();
    (void)shx_scan;
    if (argc >= 2 && strcmp(argv[1], "--close-feed") == 0) {
        return run_close_feed_scenario();
    }
    if (argc >= 2 && strcmp(argv[1], "--cli-units") == 0) {
        return run_cli_units();
    }
    if (argc >= 3 && strcmp(argv[1], "--seed") == 0) {
        uint64_t seed = 0;

        if (!parse_seed_arg(argv[2], &seed)) {
            printf("ERROR: malformed --seed argument: %s\n", argv[2]);
            return 2;
        }
        return run_one_seed(seed, true, true);
    }
    if (argc >= 3 && strcmp(argv[1], "--trace") == 0) {
        return run_trace_file(argv[2]);
    }
    if (argc >= 2 && strcmp(argv[1], "--seeds") != 0) {
        printf("ERROR: unknown mode %s\n", argv[1]);
        return 2;
    }
    return run_seeds();
}
