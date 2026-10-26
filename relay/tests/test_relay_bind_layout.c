/*
 * Bind delivery-scheduler capacity oracle. White-box (includes moqr_bind.c):
 * pins the 64-bit delivery-park structural delta with a baseline b_conn_t
 * replica (the park fields cost one size_t in that ABI's padding), the
 * widened bitset word arithmetic at the uint32_t
 * maximum, the descriptor slope across the 63/64/65 and 128/129 word
 * boundaries (ready + parked add exactly TWO 8-byte words per crossing),
 * the bind-owned pump scratch term, and the create-time counting-allocator
 * slope with leak-free destruction.
 */

#include "moqr_bind.c"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"
#include "../../tests/unit/test_raw_pair_support.h"

/* counting allocator (same shape as test_relay_log) */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live, attempts;
    long fail_at;   /* fail exactly this allocation attempt; 0 = never */
} ca_t;
static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
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
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;
    }
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

/* -- 64-bit footprint pin -------------------------------------------------- */

typedef struct b_conn_baseline {
    bool           used;
    bool           closed;
    bool           detach_pending;
    moq_session_t *session;
    moqr_binding_t binding;
    b_dsub_t      *dsubs;
    b_sg_t        *sgs;
    b_usub_t      *usubs;
    b_pub_t       *pubs;
    b_ann_t       *anns;
    b_cadv_t      *cluster_adverts;
    uint64_t      *cluster_paths;
    bool           cluster_peer_ready;
    bool           cluster_negotiated;
    bool           cluster_cost_override;
    bool           cluster_discovery_closed;
    bool           cluster_discovery_retiring;
    bool           cluster_discovery_cancel_owed;
    uint64_t       cluster_cost;
    uint64_t       cluster_correlation;
    uint64_t       cluster_peer_hop;
    moq_ns_sub_handle_t cluster_discovery;
    b_fetch_t     *fetches;
    b_refusal_t   *refusals;
    uint32_t       refusals_reserved;
    uint64_t       requests_seen;
} b_conn_baseline_t;

/* Smaller ABIs have different pointer/uint64_t padding. Their allocation
 * sizes are checked by the descriptor and counting-allocator equations below. */
_Static_assert(sizeof(void *) != 8 || sizeof(size_t) != 8 ||
                   sizeof(b_conn_t) == sizeof(b_conn_baseline_t) + sizeof(size_t),
               "64-bit park state and retry hints must cost one size_t");

/* -- Word arithmetic: widened round-up ---------------------------------------- */

static int
test_bind_word_count(void)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(1), 1);
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(64), 1);
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(65), 2);
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(UINT32_MAX - 63), 67108863);
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(UINT32_MAX), 67108864);
    MOQ_TEST_PASS("bind_word_count");
    return failures;
}

/* -- Descriptor + allocator slopes across word boundaries --------------------- */

static uint64_t
bind_pump_scratch_bytes_test(void)
{
    return (uint64_t)BIND_PUMP_EVENT_BATCH * sizeof(moq_event_t) +
           (uint64_t)BIND_PUMP_REVOKED_BATCH *
               sizeof(moqr_revoked_grant_t) +
           (uint64_t)BIND_PUMP_INTENT_BATCH * sizeof(moqr_intent_t);
}

static uint64_t
describe_bind(ca_t *a, moqr_core_t *core, uint32_t max_conns)
{
    (void)core;
    moqr_bind_cfg_t cfg;
    moqr_bind_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.max_conns = max_conns;
    /* Fixed small tables so the per-conn slope is exact and cheap. */
    cfg.max_downstream_subs = 4;
    cfg.max_open_subgroups = 4;
    cfg.max_upstream_subs = 4;
    cfg.max_publishes = 4;
    cfg.max_announces = 4;
    moqr_core_limits_t lim;
    moqr_core_relay_cfg_t ccfg;
    moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), &a->vt);
    if (moqr_core_limits_resolve(&ccfg, &lim) != MOQR_OK) {
        return UINT64_MAX;
    }
    moqr_bind_capacity_t cap;
    if (moqr_bind_capacity_describe(&cfg, &lim, &cap) != MOQR_OK) {
        return UINT64_MAX;
    }
    return cap.structure_bytes;
}

static long
create_bind_live(ca_t *a, moqr_core_t *core, uint32_t max_conns)
{
    moqr_bind_cfg_t cfg;
    moqr_bind_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.core = core;
    cfg.max_conns = max_conns;
    cfg.max_downstream_subs = 4;
    cfg.max_open_subgroups = 4;
    cfg.max_upstream_subs = 4;
    cfg.max_publishes = 4;
    cfg.max_announces = 4;
    long before = a->live;
    moqr_bind_t *b = NULL;
    if (moqr_bind_create(&cfg, &b) != MOQR_OK) {
        return -1;
    }
    long created = a->live - before;
    moqr_bind_destroy(b);
    return a->live == before ? created : -1;   /* -1: leaked */
}

static int
test_bind_capacity_slopes(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    /* One shared core for the create() runs (bind does not own it). */
    moqr_core_relay_cfg_t ccfg;
    moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), &a.vt);
    ccfg.log_budget.max_groups = 4;
    ccfg.log_budget.max_bytes = 1 << 20;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&ccfg, &core) == MOQR_OK);

    moqr_core_limits_t lim;
    MOQ_TEST_CHECK(moqr_core_limits_resolve(&ccfg, &lim) == MOQR_OK);
    uint64_t per_conn = sizeof(b_conn_t) + 4 * sizeof(b_dsub_t) +
                        4 * sizeof(b_sg_t) + 4 * sizeof(b_usub_t) +
                        4 * sizeof(b_pub_t) + 4 * sizeof(b_ann_t) +
                        lim.max_fetches * sizeof(b_fetch_t) +
                        (BIND_PUMP_EVENT_POLL + lim.max_parked) *
                            sizeof(b_refusal_t);
    uint64_t scratch = bind_pump_scratch_bytes_test();

    uint64_t d1 = describe_bind(&a, core, 1);
    uint64_t d63 = describe_bind(&a, core, 63);
    uint64_t d64 = describe_bind(&a, core, 64);
    uint64_t d65 = describe_bind(&a, core, 65);
    uint64_t d128 = describe_bind(&a, core, 128);
    uint64_t d129 = describe_bind(&a, core, 129);
    MOQ_TEST_CHECK(d1 != UINT64_MAX && d129 != UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(d63 - d1, 62 * per_conn);       /* 1 word pair  */
    MOQ_TEST_CHECK_EQ_U64(d64 - d63, per_conn);           /* 1 word pair  */
    MOQ_TEST_CHECK_EQ_U64(d65 - d64, per_conn + 16);      /* 2 word pairs */
    MOQ_TEST_CHECK_EQ_U64(d128 - d65, 63 * per_conn);     /* 2 word pairs */
    MOQ_TEST_CHECK_EQ_U64(d129 - d128, per_conn + 16);    /* 3 word pairs */
    MOQ_TEST_CHECK(scratch > 2048u);   /* too large to live on the pump's stack */

    /* Create-time slope mirrors the descriptor exactly (all bind tables are
     * eager), and every size destroys leak-free. */
    long c1 = create_bind_live(&a, core, 1);
    long c63 = create_bind_live(&a, core, 63);
    long c64 = create_bind_live(&a, core, 64);
    long c65 = create_bind_live(&a, core, 65);
    MOQ_TEST_CHECK(c1 > 0 && c63 > 0 && c64 > 0 && c65 > 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)c1, d1);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(c64 - c63), per_conn);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(c65 - c64), per_conn + 16);

    moqr_core_destroy(core);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("bind_capacity_slopes");
    return failures;
}

static int
test_bind_pump_scratch_owned(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    moqr_core_relay_cfg_t ccfg;
    moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), &a.vt);
    ccfg.log_budget.max_groups = 4;
    ccfg.log_budget.max_bytes = 1 << 20;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&ccfg, &core) == MOQR_OK);

    moqr_bind_cfg_t cfg;
    moqr_bind_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.core = core;
    cfg.max_conns = 1;
    cfg.max_downstream_subs = 1;
    cfg.max_open_subgroups = 1;
    cfg.max_upstream_subs = 1;
    cfg.max_publishes = 1;
    cfg.max_announces = 1;

    moqr_bind_t *b = NULL;
    MOQ_TEST_CHECK(moqr_bind_create(&cfg, &b) == MOQR_OK);
    MOQ_TEST_CHECK(b != NULL);
    MOQ_TEST_CHECK(b->pump_events != NULL);
    MOQ_TEST_CHECK(b->pump_revoked != NULL);
    MOQ_TEST_CHECK(b->pump_intents != NULL);
    MOQ_TEST_CHECK(b->pump_events != (moq_event_t *)(void *)b->pump_revoked);
    MOQ_TEST_CHECK(b->pump_intents !=
                   (moqr_intent_t *)(void *)b->pump_events);
    MOQ_TEST_CHECK(b->pump_intents !=
                   (moqr_intent_t *)(void *)b->pump_revoked);

    long attempts_before = a.attempts;
    long live_before = a.live;
    MOQ_TEST_CHECK(moqr_bind_pump(b, 1000) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT((int)(a.attempts - attempts_before), 0);
    MOQ_TEST_CHECK_EQ_INT((int)(a.live - live_before), 0);

    b->in_pump = true;
    MOQ_TEST_CHECK(moqr_bind_pump(b, 2000) == MOQR_ERR_INVAL);
    b->in_pump = false;

    moqr_bind_destroy(b);
    moqr_core_destroy(core);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);

    for (long fail = 7; fail <= 9; fail++) {
        ca_t fa;
        ca_init(&fa);
        moqr_core_t *fc = NULL;
        moqr_core_relay_cfg_t fccfg;
        moqr_core_relay_cfg_init_sized(&fccfg, sizeof(fccfg), &fa.vt);
        fccfg.log_budget.max_groups = 4;
        fccfg.log_budget.max_bytes = 1 << 20;
        MOQ_TEST_CHECK(moqr_core_create(&fccfg, &fc) == MOQR_OK);
        moqr_bind_cfg_t fcfg;
        moqr_bind_cfg_init_sized(&fcfg, sizeof(fcfg), &fa.vt);
        fcfg.core = fc;
        fcfg.max_conns = 1;
        fcfg.max_downstream_subs = 1;
        fcfg.max_open_subgroups = 1;
        fcfg.max_upstream_subs = 1;
        fcfg.max_publishes = 1;
        fcfg.max_announces = 1;
        fa.fail_at = fa.attempts + fail;
        moqr_bind_t *fb = NULL;
        MOQ_TEST_CHECK(moqr_bind_create(&fcfg, &fb) == MOQR_ERR_NOMEM);
        MOQ_TEST_CHECK(fb == NULL);
        moqr_core_destroy(fc);
        MOQ_TEST_CHECK_EQ_INT((int)fa.live, 0);
    }

    MOQ_TEST_PASS("bind_pump_scratch_owned");
    return failures;
}

static int
test_terminal_event_identity(void)
{
    int failures = 0;
    for (unsigned publication = 0; publication < 2; publication++) {
        b_sg_t sg = { .used = true, .group = 10, .subgroup = 7,
                      .obj_begun = true, .next_chunk = 3 };
        sg.sub_raw = moq_handle_pack(publication ? MOQ_HANDLE_POOL_PUBLICATION
                                                 : MOQ_HANDLE_POOL_SUBSCRIPTION,
                                    42, 1, 0);
        sg.h._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP, 42, 3, 0);
        b_conn_t cn = { .used = true, .sgs = &sg,
                        .park_reason = B_PARK_ACTION_CAP, .park_cap = 2 };
        uint64_t ready = 0, parked = 1;
        moqr_bind_t b = { .max_conns = 1, .n_sgs = 1, .conns = &cn,
                         .dl_ready = &ready, .dl_parked = &parked };
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED;
        ev.detail_size = MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE;
        moq_outgoing_subgroup_terminated_event_t *te = &ev.u.outgoing_subgroup_terminated;
        te->subgroup = sg.h;
        if (publication) te->pub._opaque = sg.sub_raw;
        else te->sub._opaque = sg.sub_raw;
        te->group_id = sg.group;
        te->subgroup_id = sg.subgroup;
        te->stream_ref._v = 77;
        te->cause = MOQ_OUTGOING_SUBGROUP_TERMINAL_PEER_STOP_SENDING;
        te->error_code = 0;   /* a real code, not an absence sentinel */
        moq_event_t bad = ev;
        bad.detail_size--;
        bind_on_event(&b, &cn, &bad, 1);
        bad = ev;
        bad.u.outgoing_subgroup_terminated.subgroup._opaque =
            moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP, 42, 1, 0);
        bind_on_event(&b, &cn, &bad, 1);   /* old generation, same slot */
        bad = ev;
        bad.u.outgoing_subgroup_terminated.subgroup._opaque =
            moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP, 43, 3, 0);
        bind_on_event(&b, &cn, &bad, 1);   /* foreign session */
        bad = ev;
        bad.u.outgoing_subgroup_terminated.group_id++;
        bind_on_event(&b, &cn, &bad, 1);
        bad = ev;
        bad.u.outgoing_subgroup_terminated.subgroup_id++;
        bind_on_event(&b, &cn, &bad, 1);
        bad = ev;
        if (publication) bad.u.outgoing_subgroup_terminated.pub._opaque++;
        else bad.u.outgoing_subgroup_terminated.sub._opaque++;
        bind_on_event(&b, &cn, &bad, 1);
        bad = ev;
        bad.u.outgoing_subgroup_terminated.sub._opaque = 1;
        bad.u.outgoing_subgroup_terminated.pub._opaque = 1;
        bind_on_event(&b, &cn, &bad, 1);
        bad = ev;
        bad.u.outgoing_subgroup_terminated.cause = 99;
        bind_on_event(&b, &cn, &bad, 1);
        MOQ_TEST_CHECK(!sg.stopped && sg.obj_begun && sg.next_chunk == 3);
        MOQ_TEST_CHECK(parked == 1 && ready == 0 && cn.park_cap == 2);
        bind_on_event(&b, &cn, &ev, 2);
        MOQ_TEST_CHECK(sg.used && sg.stopped && !sg.obj_begun && sg.next_chunk == 0);
        MOQ_TEST_CHECK(sg.stopped_ref._v == 77);
        MOQ_TEST_CHECK(parked == 0 && ready == 1 && cn.park_reason == B_PARK_NONE);
        MOQ_TEST_CHECK(cn.park_cap == 0);
        te->stream_ref._v = 78;
        te->cause = MOQ_OUTGOING_SUBGROUP_TERMINAL_DELIVERY_TIMEOUT;
        bind_on_event(&b, &cn, &ev, 3);
        MOQ_TEST_CHECK(sg.stopped_ref._v == 77);   /* duplicate cannot replace it */
        /* Reuse the session pool slot; the old event cannot suppress it. */
        sg.stopped = false;
        sg.h._opaque = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP, 42, 5, 0);
        bind_on_event(&b, &cn, &ev, 4);
        MOQ_TEST_CHECK(!sg.stopped);
    }
    MOQ_TEST_PASS("terminal_event_identity");
    return failures;
}

static moqr_track_t
stopped_track(moqr_core_t *core, moqr_binding_t bh, const char *name,
               uint64_t cookie, uint8_t priority, moqr_sub_t *sub)
{
    const moq_bytes_t ns = { .data = (const uint8_t *)"stop", .len = 4 };
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = (moqr_ns_t){ .parts = &ns, .count = 1 };
    req.name = (moq_bytes_t){ .data = (const uint8_t *)name, .len = strlen(name) };
    req.cookie = cookie;
    req.subscriber_priority = priority;
    req.filter.type = MOQR_FILTER_ABSOLUTE_START;
    if (moqr_core_subscribe(core, bh, &req, sub) != MOQR_OK) return MOQR_TRACK_INVALID;
    moqr_intent_t it[16];
    size_t n = moqr_core_poll_intents(core, it, 16);
    moqr_track_t track = MOQR_TRACK_INVALID;
    for (size_t i = 0; i < n; i++) {
        if (it[i].kind != MOQR_INTENT_UPSTREAM_SUBSCRIBE) continue;
        track = it[i].track;
        if (moqr_core_upstream_ok(core, track, it[i].track_gen,
                                  cookie + 1000, false, 0, 0) != MOQR_OK) {
            return MOQR_TRACK_INVALID;
        }
    }
    (void)moqr_core_poll_intents(core, it, 16);
    return track;
}

static int
test_deferred_event_readiness(void)
{
    int failures = 0;
    b_refusal_t refusal = { .used = true };
    b_meta_publish_t metadata;
    memset(&metadata, 0, sizeof(metadata));
    b_conn_t cn = { .used = true, .events_pending = true,
                    .refusals = &refusal, .retry_pending = B_RETRY_REFUSAL,
                    .park_reason = B_PARK_ACTION_CAP };
    uint64_t ready = 1, parked = 1;
    moqr_bind_t b = { .max_conns = 1, .n_refusals = 1, .conns = &cn,
                     .metadata_publish_cap = 1, .metadata_publishes = &metadata,
                     .dl_ready = &ready, .dl_parked = &parked };
    MOQ_TEST_CHECK(!moqr_bind_pump_continuation_pending(&b));
    MOQ_TEST_CHECK(ready == 1 && parked == 1 && cn.events_pending);
    refusal.used = false;   /* recovering refusal room is not an event drain */
    MOQ_TEST_CHECK(moqr_bind_pump_continuation_pending(&b));
    MOQ_TEST_CHECK(ready == 1 && parked == 1 && cn.events_pending);
    cn.events_pending = false;
    MOQ_TEST_CHECK(moqr_bind_pump_continuation_pending(&b));
    ready = 0;
    MOQ_TEST_CHECK(!moqr_bind_pump_continuation_pending(&b));
    MOQ_TEST_PASS("deferred_event_readiness");
    return failures;
}

static moqr_result_t
stopped_ingest(moqr_core_t *core, moqr_track_t track, uint64_t group,
                uint64_t object)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = group;
    d.subgroup_id = 0;
    d.object_id = object;
    d.status = MOQR_OBJ_NORMAL;
    return moqr_core_ingest(core, track, &d);
}

/* No session operations are possible in this full-table fixture: every
 * stream is already stopped. The fresh record must refuse before open,
 * while lower-priority buffered records and their seals still get serviced. */
static int
test_stopped_full_table(void)
{
    enum { LOW_RECORDS = 600 };
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &core) == MOQR_OK);
    if (core == NULL) return failures;
    moqr_binding_t pub, bh;
    MOQ_TEST_CHECK(moqr_core_binding_open(core, 2, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(core, 1, &bh) == MOQR_OK);
    const moq_bytes_t ns = { .data = (const uint8_t *)"stop", .len = 4 };
    MOQ_TEST_CHECK(moqr_core_announce(core, pub,
        (moqr_ns_t){ .parts = &ns, .count = 1 }) == MOQR_OK);
    moqr_sub_t slow, other, fresh;
    moqr_track_t low = stopped_track(core, bh, "low", 100, 200, &slow);
    moqr_track_t mid = stopped_track(core, bh, "mid", 101, 100, &other);
    moqr_track_t high = stopped_track(core, bh, "high", 102, 1, &fresh);
    MOQ_TEST_CHECK(moqr_track_is_valid(low) && moqr_track_is_valid(mid) &&
                   moqr_track_is_valid(high));
    for (uint64_t i = 0; i < LOW_RECORDS; i++) {
        MOQ_TEST_CHECK(stopped_ingest(core, low, 1, i) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(core, low, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK(stopped_ingest(core, mid, 2, 0) == MOQR_OK);
    MOQ_TEST_CHECK(stopped_ingest(core, high, 3, 0) == MOQR_OK);
    b_sg_t sgs[2] = {
        { .used = true, .stopped = true, .sub_raw = 100, .rsub = slow,
          .group = 1, .subgroup = 0, .core_list = 0 },
        { .used = true, .stopped = true, .sub_raw = 101, .rsub = other,
          .group = 2, .subgroup = 0, .core_list = 0 },
    };
    b_conn_t cn = { .used = true, .binding = bh, .sgs = sgs };
    uint64_t ready = 0, parked = 0;
    moqr_bind_t b = { .core = core, .max_conns = 1, .n_sgs = 2,
                     .conns = &cn, .dl_ready = &ready, .dl_parked = &parked };
    MOQ_TEST_CHECK(bind_discard_stopped(&b, &cn, 1));
    MOQ_TEST_CHECK(sgs[0].used && sgs[1].used);
    MOQ_TEST_CHECK(b.budget_pending && ready == 1);
    bool yielded = false;
    b_deliver_outcome_t outcome = B_DELIVER_BUDGET;
    for (unsigned pass = 0; pass < 4 && outcome == B_DELIVER_BUDGET; pass++) {
        outcome = bind_deliver(&b, &cn, 2);
        yielded = yielded || outcome == B_DELIVER_BUDGET;
    }
    MOQ_TEST_CHECK(yielded && outcome == B_DELIVER_BLOCKED_BIND_SG);
    MOQ_TEST_CHECK(cn.bind_sg_waiting);
    moqr_delivery_t d;
    /* Its seal was acknowledged despite the inadmissible high-priority head. */
    b_delivery_capacity_t cap = { .bind = &b, .conn = &cn };
    MOQ_TEST_CHECK(moqr_core_internal_next_eligible(core, bh, 3, &d,
        bind_delivery_eligible, &cap) == MOQR_DONE);
    MOQ_TEST_CHECK(moqr_core_next_delivery(core, bh, 3, &d) == MOQR_OK);
    MOQ_TEST_CHECK(d.notice == MOQR_DELIVERY_NOTICE_NONE && moqr_sub_eq(d.sub, fresh));
    MOQ_TEST_CHECK(moqr_core_delivery_done(core, bh, MOQR_DELIVERY_REFUSED_UNBEGUN, 3) == MOQR_OK);
    MOQ_TEST_CHECK(stopped_ingest(core, low, 1, LOW_RECORDS) == MOQR_OK);
    moqr_reset_desc_t reset = moqr_reset_desc_none();
    MOQ_TEST_CHECK(moqr_reset_desc_local(MOQR_RESET_CANCELLED, &reset) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_reset_subgroup(core, low, 1, 0, reset) == MOQR_OK);
    MOQ_TEST_CHECK(bind_discard_stopped(&b, &cn, 4));
    MOQ_TEST_CHECK(bind_deliver(&b, &cn, 4) == B_DELIVER_BLOCKED_BIND_SG);
    MOQ_TEST_CHECK(sgs[0].used && sgs[0].stopped);   /* seal recurrence suppressed */
    MOQ_TEST_CHECK(stopped_ingest(core, low, 1, LOW_RECORDS + 1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(core, low, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK(bind_discard_stopped(&b, &cn, 4));
    MOQ_TEST_CHECK(bind_deliver(&b, &cn, 4) == B_DELIVER_BLOCKED_BIND_SG);
    MOQ_TEST_CHECK(sgs[0].used && sgs[0].stopped);   /* reset recurrence suppressed */
    moqr_core_stats_t stats;
    moqr_core_get_stats(core, &stats);
    MOQ_TEST_CHECK(stats.subs_active == 3 && stats.delivered_total == 0);
    MOQ_TEST_CHECK(b.deliveries_written == 0 && b.session_errors == 0);
    ready = 0;
    MOQ_TEST_CHECK(moqr_core_note_track_evicted_below(core, low, 2) == MOQR_OK);
    MOQ_TEST_CHECK(bind_discard_stopped(&b, &cn, 5));
    MOQ_TEST_CHECK(sgs[0].used && sgs[0].stopped && sgs[1].used);
    /* The watermark seals retained data, but does not retire its numeric key.
     * A retained-key recurrence must still be suppressed before cache eviction. */
    MOQ_TEST_CHECK(stopped_ingest(core, low, 1, LOW_RECORDS + 2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(core, low, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK(bind_discard_stopped(&b, &cn, 5));
    MOQ_TEST_CHECK(bind_deliver(&b, &cn, 5) == B_DELIVER_BLOCKED_BIND_SG);
    MOQ_TEST_CHECK(sgs[0].used && sgs[0].stopped && sgs[1].used);
    /* Four newer groups exceed the configured four-group cache and physically
     * evict group 1. No session operation is needed to release its marker. */
    for (uint64_t group = 2; group <= 5; group++) {
        MOQ_TEST_CHECK(stopped_ingest(core, low, group, 0) == MOQR_OK);
    }
    ready = 0;
    MOQ_TEST_CHECK(bind_discard_stopped(&b, &cn, 5));
    MOQ_TEST_CHECK(!sgs[0].used && sgs[1].used && ready == 1);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(core, other, 6) == MOQR_OK);
    MOQ_TEST_CHECK(bind_discard_stopped(&b, &cn, 6));
    MOQ_TEST_CHECK(!sgs[1].used);
    moqr_core_destroy(core);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("stopped_full_table");
    return failures;
}

typedef struct answer_fixture {
    ca_t alloc;
    moqr_core_t *core;
    moqr_bind_t *bind;
    moq_session_t *peer, *server;
    moq_stream_ref_t opened, data_ref;
    moq_stream_ref_t reset_ref;
    uint64_t reset_code;
    unsigned resets;
} answer_fixture_t;

static bool
answer_fixture_open(answer_fixture_t *f)
{
    memset(f, 0, sizeof(*f));
    ca_init(&f->alloc);
    moq_session_cfg_t sc;
    moq_session_cfg_init_sized(&sc, sizeof(sc), &f->alloc.vt,
                                MOQ_PERSPECTIVE_CLIENT);
    sc.version = MOQ_VERSION_DRAFT_22;
    sc.max_actions = 8;
    sc.max_events = 32;
    sc.max_track_subscriptions = 16;
    sc.send_request_capacity = true;
    sc.initial_request_capacity = 32;
    if (moq_session_create(&sc, 0, &f->peer) != MOQ_OK) return false;
    sc.perspective = MOQ_PERSPECTIVE_SERVER;
    sc.max_events = 4;
    if (moq_session_create(&sc, 0, &f->server) != MOQ_OK ||
        test_bind_if_required(f->peer) != MOQ_OK ||
        test_bind_if_required(f->server) != MOQ_OK ||
        moq_session_start(f->peer, 0) != MOQ_OK ||
        moq_session_start(f->server, 0) != MOQ_OK) return false;
    run(f->peer, f->server, 1);
    drain(f->peer);
    drain(f->server);
    if (moq_session_state(f->peer) != MOQ_SESS_ESTABLISHED ||
        moq_session_state(f->server) != MOQ_SESS_ESTABLISHED) return false;
    moqr_core_relay_cfg_t cc;
    moqr_core_relay_cfg_init_sized(&cc, sizeof(cc), &f->alloc.vt);
    cc.max_bindings = 4;
    cc.max_tracks = 4;
    cc.max_subs = 8;
    cc.max_parked = 1;
    cc.log_budget.max_groups = 4;
    cc.log_budget.max_bytes = 1 << 20;
    if (moqr_core_create(&cc, &f->core) != MOQR_OK) return false;
    moqr_bind_cfg_t bc;
    moqr_bind_cfg_init_sized(&bc, sizeof(bc), &f->alloc.vt);
    bc.core = f->core;
    bc.max_conns = 1;
    bc.max_downstream_subs = 4;
    bc.max_open_subgroups = 4;
    bc.max_upstream_subs = 2;
    bc.max_publishes = 2;
    bc.max_announces = 2;
    return moqr_bind_create(&bc, &f->bind) == MOQR_OK &&
           moqr_bind_conn_open(f->bind, f->server, MOQ_VERSION_DRAFT_22) == MOQR_OK;
}

static void
answer_fixture_close(answer_fixture_t *f)
{
    moqr_bind_destroy(f->bind);
    moqr_core_destroy(f->core);
    moq_session_destroy(f->peer);
    moq_session_destroy(f->server);
}

/* The two scan hints share storage, but settling one cannot clear the other. */
static int
test_retry_hint_independence(void)
{
    int failures = 0;
    answer_fixture_t f;
    bool opened = answer_fixture_open(&f);
    MOQ_TEST_CHECK(opened);
    if (!opened) goto cleanup;
    b_conn_t *cn = &f.bind->conns[0];
    MOQ_TEST_CHECK(cn->retry_pending == 0);

    cn->retry_pending = B_RETRY_REFUSAL | B_RETRY_CANCEL;
    bind_refusal_pump(f.bind, 2);
    MOQ_TEST_CHECK(cn->retry_pending == B_RETRY_CANCEL);

    b_refusal_t held = { .used = true, .kind = MOQR_AUTH_FETCH };
    bind_terminal_hold(f.bind, cn, &held);
    MOQ_TEST_CHECK(cn->retry_pending == (B_RETRY_REFUSAL | B_RETRY_CANCEL));
    bind_retry_owed_cancels(f.bind, cn, 2);
    MOQ_TEST_CHECK(cn->retry_pending == B_RETRY_REFUSAL);
    MOQ_TEST_CHECK(cn->refusals[0].used);
    MOQ_TEST_CHECK(bind_refusal_free(f.bind, cn) == f.bind->n_refusals - 1u);

    /* Releasing a held entry leaves a conservative hint until the next walk. */
    memset(&cn->refusals[0], 0, sizeof(cn->refusals[0]));
    bind_refusal_pump(f.bind, 3);
    MOQ_TEST_CHECK(cn->retry_pending == 0);
    MOQ_TEST_CHECK(f.bind->session_errors == 0);
cleanup:
    answer_fixture_close(&f);
    MOQ_TEST_CHECK_EQ_INT((int)f.alloc.live, 0);
    MOQ_TEST_PASS("retry_hint_independence");
    return failures;
}

/* Use the existing raw-pair transport, recording the actual sender refs. */
static void
answer_transfer(answer_fixture_t *f, bool from_server, uint64_t now)
{
    moq_session_t *from = from_server ? f->server : f->peer;
    moq_session_t *to = from_server ? f->peer : f->server;
    moq_action_t a;
    while (moq_session_poll_actions(from, &a, 1) > 0) {
        if (!from_server && a.kind == MOQ_ACTION_OPEN_BIDI_STREAM) {
            f->opened = a.u.open_bidi_stream.stream_ref;
        }
        if (from_server && a.kind == MOQ_ACTION_SEND_DATA) {
            f->data_ref = a.u.send_data.stream_ref;
        }
        if (from_server && a.kind == MOQ_ACTION_RESET_DATA) {
            f->resets++;
            f->reset_ref = a.u.reset_data.stream_ref;
            f->reset_code = a.u.reset_data.error_code;
        }
        deliver(to, &a, from_server, now);
        moq_action_cleanup(&a);
    }
}

typedef struct answer_tracks {
    moq_track_sub_handle_t peer, server;
    moq_stream_ref_t ref;
} answer_tracks_t;

static bool
answer_tracks_open(answer_fixture_t *f, const char *prefix, answer_tracks_t *out)
{
    memset(out, 0, sizeof(*out));
    moq_bytes_t part = { .data = (const uint8_t *)prefix, .len = strlen(prefix) };
    moq_subscribe_tracks_cfg_t cfg;
    moq_subscribe_tracks_cfg_init(&cfg);
    cfg.track_namespace_prefix = (moq_namespace_t){ .parts = &part, .count = 1 };
    if (moq_session_subscribe_tracks(f->peer, &cfg, 2, &out->peer) != MOQ_OK) return false;
    answer_transfer(f, false, 2);
    out->ref = f->opened;
    moq_event_t ev;
    if (!take(f->server, MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST, &ev)) return false;
    out->server = ev.u.subscribe_tracks_request.handle;
    moq_event_cleanup(&ev);
    return true;
}

/* A temporary drain gate preserves the exact first/update answer, including
 * refusal code and update ID. Recovery sends that real request's answer once. */
static int
test_namespace_answer_event_gate(bool initial, bool reject)
{
    int failures = 0;
    answer_fixture_t f;
    bool opened = answer_fixture_open(&f);
    MOQ_TEST_CHECK(opened);
    if (!opened) goto cleanup;
    answer_tracks_t tracks;
    bool requested = answer_tracks_open(&f, "held", &tracks);
    MOQ_TEST_CHECK(requested);
    if (!requested) goto cleanup;
    b_conn_t *cn = &f.bind->conns[0];
    moq_bytes_t part = MOQ_BYTES_LITERAL("held");
    moqr_ns_t prefix = { .parts = &part, .count = 1 };
    uint64_t id = 0;
    b_nsans_t *held;
    if (initial) {
        if (!reject) {
            moqr_tracks_req_t req;
            moqr_tracks_req_init_sized(&req, sizeof(req));
            req.prefix = prefix;
            req.cookie = tracks.server._opaque;
            req.held = true;
            MOQ_TEST_CHECK(moqr_core_tracks_subscribe(f.core, cn->binding, &req) == MOQR_OK);
        }
        bind_tracks_hold_answer(f.bind, cn, tracks.server._opaque, reject,
                                 MOQ_REQUEST_ERROR_UNAUTHORIZED, 2);
        held = bind_nsans_lookup(f.bind, cn, tracks.server._opaque, true);
    } else {
        bind_tracks_admit(f.bind, cn, tracks.server._opaque, prefix, true, true,
                          MOQ_GROUP_ORDER_DEFAULT, NULL, 0, 2);
        answer_transfer(&f, true, 2);
        moq_event_t ev;
        bool accepted = take(f.peer, MOQ_EVENT_SUBSCRIBE_TRACKS_OK, &ev);
        MOQ_TEST_CHECK(accepted);
        if (!accepted) goto cleanup;
        MOQ_TEST_CHECK(ev.u.subscribe_tracks_ok.handle._opaque == tracks.peer._opaque);
        moq_event_cleanup(&ev);
        moq_namespace_update_cfg_t update;
        moq_namespace_update_cfg_init(&update);
        update.flags = MOQ_NAMESPACE_UPDATE_FORWARD;
        update.forward = false;
        MOQ_TEST_CHECK(moq_session_update_subscribe_tracks(f.peer, tracks.peer,
                                                            &update, 3, &id) == MOQ_OK);
        answer_transfer(&f, false, 3);
        bool updated = take(f.server, MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_REQUEST, &ev);
        MOQ_TEST_CHECK(updated);
        if (!updated) goto cleanup;
        MOQ_TEST_CHECK(ev.u.subscribe_tracks_update_request.handle._opaque == tracks.server._opaque);
        MOQ_TEST_CHECK(ev.u.subscribe_tracks_update_request.update_request_id == id);
        moq_event_cleanup(&ev);
        if (reject) bind_nsu_retire(f.bind, cn, B_NSU_TRACKS, tracks.server._opaque, 3);
        held = bind_nsans_hold(f.bind, cn, B_NSU_TRACKS, tracks.server._opaque, id, 3);
        if (held != NULL) {
            held->reject = reject;
            held->error_code = MOQ_REQUEST_ERROR_UNAUTHORIZED;
        }
    }
    MOQ_TEST_CHECK(held != NULL && f.bind->nsans_live == 1);
    if (held == NULL) goto cleanup;
    b_nsans_t saved;
    memcpy(&saved, held, sizeof(saved));
    size_t capacity = moq_session_action_capacity(f.server);
    MOQ_TEST_CHECK(capacity > 0);
    cn->events_pending = true;
    for (unsigned pass = 0; pass < 2; pass++) {
        bind_nsu_pump(f.bind, 4);
        MOQ_TEST_CHECK(f.bind->nsans_live == 1 && memcmp(held, &saved, sizeof(saved)) == 0);
        MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
    }
    answer_transfer(&f, true, 4);
    moq_event_t ev;
    MOQ_TEST_CHECK(moq_session_poll_events(f.peer, &ev, 1) == 0);
    cn->events_pending = false;
    bind_nsu_pump(f.bind, 5);
    MOQ_TEST_CHECK(f.bind->nsans_live == 0);
    answer_transfer(&f, true, 5);
    unsigned answers = 0;
    while (moq_session_poll_events(f.peer, &ev, 1) > 0) {
        if (initial && ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_OK) {
            MOQ_TEST_CHECK(!reject && ev.u.subscribe_tracks_ok.handle._opaque == tracks.peer._opaque);
            answers++;
        } else if (initial && ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR) {
            MOQ_TEST_CHECK(reject && ev.u.subscribe_tracks_error.handle._opaque == tracks.peer._opaque);
            MOQ_TEST_CHECK(ev.u.subscribe_tracks_error.error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED);
            answers++;
        } else if (!initial && ev.kind == MOQ_EVENT_SUBSCRIBE_TRACKS_UPDATE_RESULT) {
            MOQ_TEST_CHECK(ev.u.subscribe_tracks_update_result.handle._opaque == tracks.peer._opaque);
            MOQ_TEST_CHECK(ev.u.subscribe_tracks_update_result.update_request_id == id);
            MOQ_TEST_CHECK(ev.u.subscribe_tracks_update_result.outcome ==
                (reject ? MOQ_NAMESPACE_UPDATE_OUTCOME_ERROR : MOQ_NAMESPACE_UPDATE_OUTCOME_OK));
            if (reject) MOQ_TEST_CHECK(ev.u.subscribe_tracks_update_result.error_code ==
                                       MOQ_REQUEST_ERROR_UNAUTHORIZED);
            answers++;
        }
        MOQ_TEST_CHECK(ev.kind != MOQ_EVENT_SESSION_CLOSED);
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(answers == 1 && f.bind->session_errors == 0);
    bind_nsu_pump(f.bind, 6);
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
    MOQ_TEST_CHECK(moq_session_state(f.peer) == MOQ_SESS_ESTABLISHED &&
                   moq_session_state(f.server) == MOQ_SESS_ESTABLISHED);
cleanup:
    answer_fixture_close(&f);
    MOQ_TEST_CHECK_EQ_INT((int)f.alloc.live, 0);
    MOQ_TEST_PASS(initial ? "initial_answer_event_gate" : "namespace_update_answer_event_gate");
    return failures;
}

typedef struct answer_target {
    moq_subscription_t peer, server;
    moqr_sub_t sub;
    moqr_track_t track;
    b_sg_t *sg;
    moq_stream_ref_t ref;
} answer_target_t;

/* Admit a real peer SUBSCRIBE through core intents, then let the production
 * pump stream one OPEN chunk to STALLED. The source is an in-memory binding. */
static bool
answer_target_open(answer_fixture_t *f, uint64_t declared, answer_target_t *t)
{
    memset(t, 0, sizeof(*t));
    moq_bytes_t part = MOQ_BYTES_LITERAL("stop");
    moq_subscribe_cfg_t sc;
    moq_subscribe_cfg_init(&sc);
    sc.track_namespace = (moq_namespace_t){ .parts = &part, .count = 1 };
    sc.track_name = MOQ_BYTES_LITERAL("v");
    sc.filter = MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START;
    if (moq_session_subscribe(f->peer, &sc, 2, &t->peer) != MOQ_OK) return false;
    answer_transfer(f, false, 2);
    moq_event_t ev;
    if (!take(f->server, MOQ_EVENT_SUBSCRIBE_REQUEST, &ev)) return false;
    t->server = ev.u.subscribe_request.sub;
    moq_event_cleanup(&ev);
    moqr_binding_t source;
    if (moqr_core_binding_open(f->core, 2, &source) != MOQR_OK ||
        moqr_core_announce(f->core, source,
            (moqr_ns_t){ .parts = &part, .count = 1 }) != MOQR_OK) return false;
    moqr_subscribe_req_t req;
    moqr_subscribe_req_init(&req);
    req.ns = (moqr_ns_t){ .parts = &part, .count = 1 };
    req.name = MOQ_BYTES_LITERAL("v");
    req.cookie = t->server._opaque;
    req.filter.type = MOQR_FILTER_ABSOLUTE_START;
    b_conn_t *cn = &f->bind->conns[0];
    if (moqr_core_subscribe(f->core, cn->binding, &req, &t->sub) != MOQR_OK) return false;
    moqr_intent_t intent[8];
    size_t n = moqr_core_poll_intents(f->core, intent, 8);
    uint64_t gen = 0;
    for (size_t i = 0; i < n; i++) {
        if (intent[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            t->track = intent[i].track;
            gen = intent[i].track_gen;
        }
    }
    if (!moqr_track_is_valid(t->track) ||
        moqr_core_upstream_ok(f->core, t->track, gen, 700, false, 0, 0) != MOQR_OK ||
        moqr_bind_pump(f->bind, 2) != MOQR_OK) return false;
    answer_transfer(f, true, 2);
    if (!take(f->peer, MOQ_EVENT_SUBSCRIBE_OK, &ev)) return false;
    bool accepted = moq_subscription_eq(ev.u.subscribe_ok.sub, t->peer);
    moq_event_cleanup(&ev);
    if (!accepted) return false;
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 10;
    d.subgroup_id = 7;
    d.object_id = 0;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = declared;
    d.now_us = 3;
    if (moqr_core_ingest(f->core, t->track, &d) != MOQR_OK) return false;
    moq_rcbuf_t *chunk = NULL;
    if (moq_rcbuf_create(&f->alloc.vt, (const uint8_t *)"data", 4, &chunk) != MOQ_OK)
        return false;
    moqr_result_t rc = moqr_core_append_chunk(f->core, t->track, 10, 7, 0, chunk);
    moq_rcbuf_decref(chunk);
    if (rc != MOQR_OK || moqr_bind_pump(f->bind, 3) != MOQR_OK) return false;
    answer_transfer(f, true, 3);
    drain(f->peer);
    t->ref = f->data_ref;
    for (uint32_t i = 0; i < f->bind->n_sgs; i++) {
        b_sg_t *sg = &cn->sgs[i];
        if (sg->used && sg->sub_raw == t->server._opaque &&
            sg->group == 10 && sg->subgroup == 7) {
            t->sg = sg;
            break;
        }
    }
    moqr_delivery_t idle;
    return t->sg != NULL && t->sg->obj_begun && t->sg->next_chunk == 1 &&
           t->ref._v != 0 &&
           moqr_core_next_delivery(f->core, cn->binding, 3, &idle) == MOQR_DONE;
}

static int
test_subscription_answer_event_gate(void)
{
    int failures = 0;
    answer_fixture_t f;
    bool opened = answer_fixture_open(&f);
    MOQ_TEST_CHECK(opened);
    if (!opened) goto cleanup;
    answer_target_t target;
    bool ready = answer_target_open(&f, 8, &target);
    MOQ_TEST_CHECK(ready);
    if (!ready) goto cleanup;
    moq_subscription_update_cfg_t update;
    moq_subscription_update_cfg_init_sized(&update, sizeof(update));
    update.has_subscriber_priority = true;
    update.subscriber_priority = 10;
    MOQ_TEST_CHECK(moq_session_update_subscription(f.peer, target.peer, &update, 4) == MOQ_OK);
    answer_transfer(&f, false, 4);
    moq_event_t ev;
    bool updated = take(f.server, MOQ_EVENT_SUBSCRIBE_UPDATED, &ev);
    MOQ_TEST_CHECK(updated);
    if (!updated) goto cleanup;
    uint64_t id = ev.u.subscribe_updated.update_request_id;
    MOQ_TEST_CHECK(moq_subscription_eq(ev.u.subscribe_updated.sub, target.server));
    moq_event_cleanup(&ev);
    b_conn_t *cn = &f.bind->conns[0];
    b_dsub_t *ds = dsub_find(f.bind, cn, target.server._opaque);
    MOQ_TEST_CHECK(ds != NULL);
    if (ds == NULL) goto cleanup;
    ds->upd_answer = B_UPD_ACCEPT;
    ds->upd_id = id;
    f.bind->upd_answers_held = 1;
    b_dsub_t saved;
    memcpy(&saved, ds, sizeof(saved));
    size_t capacity = moq_session_action_capacity(f.server);
    cn->events_pending = true;
    for (unsigned pass = 0; pass < 2; pass++) {
        bind_update_answer_pump(f.bind, 4);
        MOQ_TEST_CHECK(f.bind->upd_answers_held == 1 && memcmp(ds, &saved, sizeof(saved)) == 0);
        MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
        uint64_t owed = 0;
        MOQ_TEST_CHECK(moq_session_subscription_owed_update(f.server, target.server,
                                                            &owed, NULL, NULL) == MOQ_OK);
        MOQ_TEST_CHECK(owed == id);
    }
    cn->events_pending = false;
    bind_update_answer_pump(f.bind, 5);
    MOQ_TEST_CHECK(f.bind->upd_answers_held == 0 && ds->upd_answer == B_UPD_NONE);
    answer_transfer(&f, true, 5);
    bool answered = take(f.peer, MOQ_EVENT_SUBSCRIPTION_UPDATE_OK, &ev);
    MOQ_TEST_CHECK(answered);
    if (answered) {
        MOQ_TEST_CHECK(moq_subscription_eq(ev.u.subscription_update_ok.sub, target.peer));
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(moq_session_subscription_owed_update(f.server, target.server,
                                                        NULL, NULL, NULL) == MOQ_ERR_WRONG_STATE);
    bind_update_answer_pump(f.bind, 6);
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
    MOQ_TEST_CHECK(f.bind->session_errors == 0);
cleanup:
    answer_fixture_close(&f);
    MOQ_TEST_CHECK_EQ_INT((int)f.alloc.live, 0);
    MOQ_TEST_PASS("subscription_answer_event_gate");
    return failures;
}

/* Full-table refusals become moot when their real request streams reset.
 * Their cancellation events precede the target terminal in the session FIFO. */
static int
test_terminal_refusal_fifo(void)
{
    int failures = 0;
    answer_fixture_t f;
    bool opened = answer_fixture_open(&f);
    MOQ_TEST_CHECK(opened);
    if (!opened) goto cleanup;
    answer_target_t target;
    bool ready = answer_target_open(&f, 8, &target);
    MOQ_TEST_CHECK(ready);
    if (!ready) goto cleanup;
    b_conn_t *cn = &f.bind->conns[0];
    answer_tracks_t filler, pending[2];
    bool requested = answer_tracks_open(&f, "filler", &filler);
    MOQ_TEST_CHECK(requested);
    if (!requested) goto cleanup;
    moq_accept_subscribe_tracks_cfg_t accept;
    moq_accept_subscribe_tracks_cfg_init(&accept);
    MOQ_TEST_CHECK(moq_session_accept_subscribe_tracks(f.server, filler.server, &accept, 4) == MOQ_OK);
    answer_transfer(&f, true, 4);
    drain(f.peer);
    moq_namespace_t suffix = { .parts = NULL, .count = 0 };
    for (unsigned i = 0; i < 8; i++) {
        MOQ_TEST_CHECK(moq_session_send_publish_blocked(f.server, filler.server,
            &suffix, MOQ_BYTES_LITERAL("filler"), 4) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == 0 && f.bind->n_refusals == 2);
    for (unsigned i = 0; i < 2; i++) {
        requested = answer_tracks_open(&f, i == 0 ? "pending-a" : "pending-b", &pending[i]);
        MOQ_TEST_CHECK(requested);
        if (!requested) goto cleanup;
        bind_refuse(f.bind, cn, B_REFUSE_SUBSCRIBE_TRACKS, pending[i].server._opaque,
                     MOQ_REQUEST_ERROR_UNAUTHORIZED, false, 0, 4);
    }
    MOQ_TEST_CHECK(bind_refusal_free(f.bind, cn) == 0 && f.bind->nsans_live == 0);
    answer_transfer(&f, true, 4);   /* capacity recovers; no binding pump yet */
    drain(f.peer);
    for (unsigned i = 0; i < 2; i++) {
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(f.server, pending[i].ref, 1, 5) == MOQ_OK);
    }
    MOQ_TEST_CHECK(moq_session_on_data_stop(f.server, target.ref, 0, 5) == MOQ_OK);
    size_t capacity = moq_session_action_capacity(f.server);
    MOQ_TEST_CHECK(capacity > 0);
    cn->events_pending = true;
    cn->park_reason = B_PARK_ACTION_CAP;
    cn->park_cap = capacity;
    bind_dl_set(f.bind->dl_ready, 0);
    bind_dl_set(f.bind->dl_parked, 0);
    MOQ_TEST_CHECK(!moqr_bind_pump_continuation_pending(f.bind));
    uint64_t translated = f.bind->events_translated;
    MOQ_TEST_CHECK(moqr_bind_pump(f.bind, 6) == MOQR_OK);
    MOQ_TEST_CHECK(f.bind->events_translated == translated && cn->events_pending);
    MOQ_TEST_CHECK(bind_refusal_free(f.bind, cn) == 2);
    MOQ_TEST_CHECK(bind_dl_test(f.bind->dl_ready, 0) && bind_dl_test(f.bind->dl_parked, 0));
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
    MOQ_TEST_CHECK(target.sg->used && !target.sg->stopped && target.sg->obj_begun);
    MOQ_TEST_CHECK(moqr_bind_pump_budget_pending(f.bind) &&
                   moqr_bind_pump_continuation_pending(f.bind));
    /* No session input or output drain between recovery and this pump. */
    MOQ_TEST_CHECK(moqr_bind_pump(f.bind, 7) == MOQR_OK);
    MOQ_TEST_CHECK(f.bind->events_translated == translated + 3 && !cn->events_pending);
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
    MOQ_TEST_CHECK(target.sg->used && target.sg->stopped && !target.sg->obj_begun);
    MOQ_TEST_CHECK(target.sg->stopped_ref._v == target.ref._v);
    MOQ_TEST_CHECK(cn->park_reason == B_PARK_NONE && !bind_dl_test(f.bind->dl_parked, 0));
    MOQ_TEST_CHECK(!moqr_bind_pump_budget_pending(f.bind) &&
                   !moqr_bind_pump_continuation_pending(f.bind));
    MOQ_TEST_CHECK(moqr_bind_pump(f.bind, 8) == MOQR_OK);
    MOQ_TEST_CHECK(f.bind->events_translated == translated + 3 &&
                   !moqr_bind_pump_continuation_pending(f.bind));
    moqr_core_stats_t stats;
    moqr_core_get_stats(f.core, &stats);
    MOQ_TEST_CHECK(stats.subs_active == 1 && stats.delivered_total == 0 && stats.retained_bytes == 4);
    MOQ_TEST_CHECK(f.bind->deliveries_written == 0 && f.bind->forward_latency.count == 0);
    /* Source abandonment and its watermark leave the cached key retained.
     * Suppression must survive without another terminal; this is not a
     * physical-eviction test. */
    moqr_reset_desc_t reset = moqr_reset_desc_none();
    MOQ_TEST_CHECK(moqr_reset_desc_local(MOQR_RESET_CANCELLED, &reset) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_abandon_record(f.core, target.track, 10, 7, 0,
                                           reset) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_note_track_evicted_below(f.core, target.track, 11) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_bind_pump(f.bind, 9) == MOQR_OK);
    moqr_core_get_stats(f.core, &stats);
    MOQ_TEST_CHECK(stats.subs_active == 1 && stats.delivered_total == 0);
    MOQ_TEST_CHECK(f.bind->deliveries_written == 0 && f.bind->forward_latency.count == 0);
    MOQ_TEST_CHECK(target.sg->used && target.sg->stopped && !target.sg->obj_begun);
    MOQ_TEST_CHECK(target.sg->stopped_ref._v == target.ref._v);
    MOQ_TEST_CHECK(f.bind->events_translated == translated + 3);
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
    MOQ_TEST_CHECK(!moqr_bind_pump_budget_pending(f.bind) &&
                   !moqr_bind_pump_continuation_pending(f.bind));
    answer_transfer(&f, true, 9);
    drain(f.peer);
    MOQ_TEST_CHECK(f.resets == 1 && f.reset_ref._v == target.ref._v && f.reset_code == 0);
    MOQ_TEST_CHECK(f.bind->session_errors == 0);
cleanup:
    answer_fixture_close(&f);
    MOQ_TEST_CHECK_EQ_INT((int)f.alloc.live, 0);
    MOQ_TEST_PASS("terminal_refusal_fifo");
    return failures;
}

/* All payload has shipped but end_object still owes its successful commit.
 * STOP recognizes terminal debt while four real cancellation events fill the
 * FIFO. The real binder writer must retain the delivery on end WOULD_BLOCK. */
static int
test_end_object_terminal_event_pressure(void)
{
    int failures = 0;
    answer_fixture_t f;
    bool opened = answer_fixture_open(&f);
    MOQ_TEST_CHECK(opened);
    if (!opened) goto cleanup;
    answer_target_t target;
    bool ready = answer_target_open(&f, 4, &target);
    MOQ_TEST_CHECK(ready);
    if (!ready) goto cleanup;
    answer_tracks_t cancelled[4];
    const char *prefix[4] = { "cancel-a", "cancel-b", "cancel-c", "cancel-d" };
    for (unsigned i = 0; i < 4; i++) {
        bool requested = answer_tracks_open(&f, prefix[i], &cancelled[i]);
        MOQ_TEST_CHECK(requested);
        if (!requested) goto cleanup;
    }
    for (unsigned i = 0; i < 4; i++) {
        MOQ_TEST_CHECK(moq_session_on_bidi_stream_reset(f.server, cancelled[i].ref, 1, 4) == MOQ_OK);
    }
    b_conn_t *cn = &f.bind->conns[0];
    MOQ_TEST_CHECK(moqr_core_complete_record(f.core, target.track, 10, 7, 0) == MOQR_OK);
    moqr_delivery_t held;
    moqr_result_t drc = moqr_core_next_delivery(f.core, cn->binding, 4, &held);
    MOQ_TEST_CHECK(drc == MOQR_OK);
    if (drc != MOQR_OK) goto cleanup;
    MOQ_TEST_CHECK(moqr_sub_eq(held.sub, target.sub) && held.rec.obj_state == MOQR_OBJ_COMPLETE);
    MOQ_TEST_CHECK(target.sg->obj_begun && target.sg->next_chunk == held.rec.chunk_count);
    size_t capacity = moq_session_action_capacity(f.server);
    MOQ_TEST_CHECK(capacity > 0);
    MOQ_TEST_CHECK(moq_session_on_data_stop(f.server, target.ref, 0, 5) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
    moqr_delivery_outcome_t outcome = bind_write_chunked(f.bind, cn, &held, target.sg, 5);
    MOQ_TEST_CHECK(outcome == MOQR_DELIVERY_WOULD_BLOCK && target.sg->obj_begun);
    if (outcome != MOQR_DELIVERY_WOULD_BLOCK) goto cleanup;
    bool confirmed = bind_confirm_delivery(f.bind, cn, outcome, 5);
    MOQ_TEST_CHECK(confirmed);
    if (!confirmed) goto cleanup;
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) == capacity);
    moqr_delivery_t replay;
    MOQ_TEST_CHECK(moqr_core_next_delivery(f.core, cn->binding, 5, &replay) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_sub_eq(replay.sub, target.sub) && replay.rec.object_id == held.rec.object_id);
    moqr_core_stats_t stats;
    moqr_core_get_stats(f.core, &stats);
    MOQ_TEST_CHECK(stats.subs_active == 1 && stats.delivered_total == 0 && stats.retained_bytes == 4);
    MOQ_TEST_CHECK(f.bind->deliveries_written == 0 && f.bind->forward_latency.count == 0);
    cn->park_reason = B_PARK_ACTION_CAP;
    cn->park_cap = capacity;
    bind_dl_set(f.bind->dl_ready, 0);
    bind_dl_set(f.bind->dl_parked, 0);
    uint64_t translated = f.bind->events_translated;
    /* Polling frees event room and commits the exact terminal; its handler
     * removes the now-unreachable capacity floor before keyed discard. */
    MOQ_TEST_CHECK(moqr_bind_pump(f.bind, 6) == MOQR_OK);
    MOQ_TEST_CHECK(f.bind->events_translated == translated + 5);
    MOQ_TEST_CHECK(target.sg->used && target.sg->stopped && !target.sg->obj_begun);
    MOQ_TEST_CHECK(target.sg->stopped_ref._v == target.ref._v);
    MOQ_TEST_CHECK(moq_session_action_capacity(f.server) <= capacity);
    MOQ_TEST_CHECK(cn->park_reason == B_PARK_NONE && !bind_dl_test(f.bind->dl_parked, 0));
    MOQ_TEST_CHECK(moqr_core_next_delivery(f.core, cn->binding, 6, &replay) == MOQR_DONE);
    const moq_rcbuf_t *chunk = NULL;
    uint64_t len = 0;
    MOQ_TEST_CHECK(moqr_core_delivery_chunk(f.core, cn->binding, 0, &chunk, &len) ==
                   MOQR_ERR_WRONG_STATE);
    moqr_core_get_stats(f.core, &stats);
    MOQ_TEST_CHECK(stats.subs_active == 1 && stats.delivered_total == 0 && stats.retained_bytes == 4);
    MOQ_TEST_CHECK(f.bind->deliveries_written == 0 && f.bind->forward_latency.count == 0);
    MOQ_TEST_CHECK(!moqr_bind_pump_budget_pending(f.bind) &&
                   !moqr_bind_pump_continuation_pending(f.bind));
    MOQ_TEST_CHECK(moqr_bind_pump(f.bind, 7) == MOQR_OK);
    MOQ_TEST_CHECK(f.bind->events_translated == translated + 5 &&
                   !moqr_bind_pump_continuation_pending(f.bind));
    answer_transfer(&f, true, 7);
    drain(f.peer);
    MOQ_TEST_CHECK(f.resets == 1 && f.reset_ref._v == target.ref._v && f.reset_code == 0);
    MOQ_TEST_CHECK(f.bind->session_errors == 0);
cleanup:
    answer_fixture_close(&f);
    MOQ_TEST_CHECK_EQ_INT((int)f.alloc.live, 0);
    MOQ_TEST_PASS("end_object_terminal_event_pressure");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_bind_word_count();
    failures += test_bind_capacity_slopes();
    failures += test_bind_pump_scratch_owned();
    failures += test_terminal_event_identity();
    failures += test_deferred_event_readiness();
    failures += test_stopped_full_table();
    failures += test_retry_hint_independence();
    for (unsigned reject = 0; reject < 2; reject++) {
        failures += test_namespace_answer_event_gate(true, reject != 0);
        failures += test_namespace_answer_event_gate(false, reject != 0);
    }
    failures += test_subscription_answer_event_gate();
    failures += test_terminal_refusal_fifo();
    failures += test_end_object_terminal_event_pressure();
    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("ALL PASS\n");
    return 0;
}
