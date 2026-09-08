/*
 * moqr_core_upstream_cancel: retiring a PENDING track's outstanding upstream
 * attempt once nobody subscribes to it. Pure core — announce/subscribe ops in,
 * polled intents out. The contract under test: PENDING + zero subscribers of
 * ANY state is the only valid target; exactly one UPSTREAM_UNSUBSCRIBE is
 * emitted (preflighted — WOULD_BLOCK means zero mutation); release mirrors the
 * source-release rules (free when nothing is retained, WARM preserving the
 * retained log otherwise); the identity bump makes late upstream answers
 * STALE. A WARM rejoin ACCEPTs its subscriber and only then re-enters PENDING,
 * so the live-subscriber guard is exercised through that path specifically.
 */

#include <moq/relay/relay.h>
#include <moq/relay/trace.h>

#include <moq/rcbuf.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
} ca_t;

static void *
ca_a(size_t n, void *c)
{
    ca_t *a = c;
    void *p = malloc(n);
    if (p) {
        a->allocs++;
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
        a->frees++;
        a->live -= (long)n;
        free(p);
    }
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

static moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

static moqr_ns_t
NS1(moq_bytes_t *storage, const char *a)
{
    storage[0] = B(a);
    return (moqr_ns_t){ .parts = storage, .count = 1 };
}

static moqr_core_t *
mkcore(ca_t *a)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 1000;
    moqr_core_t *c = NULL;
    if (moqr_core_create(&cfg, &c) != MOQR_OK) {
        return NULL;
    }
    return c;
}

static size_t
drain(moqr_core_t *c, moqr_intent_t *out, size_t cap)
{
    return moqr_core_poll_intents(c, out, cap);
}

static moqr_result_t
ing(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t sg,
    uint64_t o, uint8_t prio)
{
    uint8_t buf[64];
    memset(buf, (uint8_t)(g * 16 + o), sizeof(buf));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&a->vt, buf, sizeof(buf), &pl) != 0) {
        return MOQR_ERR_NOMEM;
    }
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = prio;
    d.payload = pl;
    d.now_us = 1;
    moqr_result_t rc = moqr_core_ingest(c, t, &d);
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(pl);
    }
    return rc;
}

/* Subscribe and harvest {track, gen, accepted?} from the drained intents. */
static moqr_sub_t
sub_track(moqr_core_t *c, moqr_binding_t sb, moqr_ns_t ns, const char *name,
          uint64_t cookie, moqr_track_t *track, uint64_t *tgen, bool *accepted)
{
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = ns;
    rq.name = B(name);
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = cookie;
    moqr_sub_t sub = (moqr_sub_t){ 0 };
    (void)moqr_core_subscribe(c, sb, &rq, &sub);
    if (accepted != NULL) {
        *accepted = false;
    }
    moqr_intent_t its[16];
    size_t n = drain(c, its, 16);
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE && track != NULL) {
            *track = its[i].track;
            if (tgen != NULL) {
                *tgen = its[i].track_gen;
            }
        }
        if (its[i].kind == MOQR_INTENT_ACCEPT_SUB && accepted != NULL) {
            *accepted = true;
        }
    }
    return sub;
}

static uint32_t
track_count(moqr_core_t *c)
{
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    return st.tracks;
}

/* -- PENDING + zero subs + nothing retained: unsubscribe emitted, slot freed - */
static int
test_uc_pending_empty_frees(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moq_bytes_t nsp[1];
    moqr_ns_t ns = NS1(nsp, "uc");
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, ns) == MOQR_OK);

    moqr_track_t track = (moqr_track_t){ 0 };
    uint64_t tgen = 0;
    moqr_sub_t sub = sub_track(c, sb, ns, "v", 7, &track, &tgen, NULL);
    MOQ_TEST_CHECK(tgen != 0);
    MOQ_TEST_CHECK_EQ_U64(track_count(c), 1);

    /* The last (parked) subscriber leaves: nothing fires for PENDING — the
     * cancel below is the only thing that can retire the upstream attempt. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 10) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, track, tgen, 20) == MOQR_OK);
    moqr_intent_t its[8];
    size_t n = drain(c, its, 8);
    bool saw = false;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_UNSUBSCRIBE &&
            its[i].binding_cookie == 1 && its[i].track_gen == tgen) {
            saw = true;
        }
    }
    MOQ_TEST_CHECK(saw);                      /* exactly the upstream told */
    MOQ_TEST_CHECK_EQ_U64(track_count(c), 0); /* nothing retained: freed  */
    /* A fresh subscribe builds a NEW pending track (no stale fast-path). */
    moqr_track_t t2 = (moqr_track_t){ 0 };
    uint64_t g2 = 0;
    bool accepted = false;
    (void)sub_track(c, sb, ns, "v", 8, &t2, &g2, &accepted);
    MOQ_TEST_CHECK(g2 != 0);        /* re-created: a new upstream attempt */
    MOQ_TEST_CHECK(!accepted);      /* parked, not served from a stale log */
    /* A repeat cancel on the OLD identity is stale — never double-fires. */
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, track, tgen, 30) ==
                   MOQR_ERR_STALE_HANDLE);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("uc_pending_empty_frees");
    return failures;
}

/* -- WARM rejoin cancelled: retained log survives and still fast-paths ------- */
static int
test_uc_warm_rejoin_preserved(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moq_bytes_t nsp[1];
    moqr_ns_t ns = NS1(nsp, "wr");
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, ns) == MOQR_OK);

    /* Build a WARM retained track: subscribe -> resolve -> ingest -> leave. */
    moqr_track_t track = (moqr_track_t){ 0 };
    uint64_t tgen = 0;
    moqr_sub_t s1 = sub_track(c, sb, ns, "v", 1, &track, &tgen, NULL);
    MOQ_TEST_CHECK(tgen != 0);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen, 99, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 100) == MOQR_OK);
    (void)drain(c, (moqr_intent_t[16]){ 0 }, 16);   /* consume accept/etc. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, s1, 100) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_tick(c, 5000) == MOQR_OK);   /* linger fires  */
    (void)drain(c, (moqr_intent_t[16]){ 0 }, 16);   /* its unsubscribe    */
    MOQ_TEST_CHECK_EQ_U64(track_count(c), 1);       /* WARM, retained     */

    /* WARM rejoin: the subscriber is ACCEPTED and the track re-enters
     * PENDING (relay.c:2679) — a live subscriber on a PENDING track. */
    moqr_track_t rt = (moqr_track_t){ 0 };
    uint64_t rgen = 0;
    bool accepted = false;
    moqr_sub_t s2 = sub_track(c, sb, ns, "v", 2, &rt, &rgen, &accepted);
    MOQ_TEST_CHECK(accepted);
    MOQ_TEST_CHECK(rgen != 0);

    /* Live subscriber (accepted, not parked) => WRONG_STATE, no mutation. */
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, rt, rgen, 200) ==
                   MOQR_ERR_WRONG_STATE);

    /* The rejoin subscriber leaves; cancel the rejoin: the retained log must
     * SURVIVE (WARM), not be freed with the upstream attempt. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, s2, 300) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, rt, rgen, 300) == MOQR_OK);
    moqr_intent_t its[8];
    size_t n = drain(c, its, 8);
    bool saw = false;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_UNSUBSCRIBE) {
            saw = true;
        }
    }
    MOQ_TEST_CHECK(saw);
    MOQ_TEST_CHECK_EQ_U64(track_count(c), 1);   /* WARM survives, not freed */

    /* Retained content still fast-path serves: a third subscribe is ACCEPTED
     * immediately from the WARM log (a freed slot would park instead). */
    bool fast = false;
    (void)sub_track(c, sb, ns, "v", 3, NULL, NULL, &fast);
    MOQ_TEST_CHECK(fast);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("uc_warm_rejoin_preserved");
    return failures;
}

/* -- guards: parked subscriber / ACTIVE / stale generation ------------------- */
static int
test_uc_guards(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moq_bytes_t nsp[1];
    moqr_ns_t ns = NS1(nsp, "gd");
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, ns) == MOQR_OK);

    moqr_track_t track = (moqr_track_t){ 0 };
    uint64_t tgen = 0;
    moqr_sub_t sub = sub_track(c, sb, ns, "v", 1, &track, &tgen, NULL);
    MOQ_TEST_CHECK(tgen != 0);
    /* Parked subscriber still wants it. */
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, track, tgen, 10) ==
                   MOQR_ERR_WRONG_STATE);
    /* Stale generation. */
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, track, tgen + 1, 10) ==
                   MOQR_ERR_STALE_HANDLE);
    /* ACTIVE (resolved) track. */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen, 9, false, 0, 0) ==
                   MOQR_OK);
    (void)drain(c, (moqr_intent_t[16]){ 0 }, 16);
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, track, tgen, 20) ==
                   MOQR_ERR_WRONG_STATE);
    (void)sub;
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("uc_guards");
    return failures;
}

/* -- intent ring full: WOULD_BLOCK with ZERO mutation, then retry succeeds --- */
static int
test_uc_would_block(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    /* Squeeze the intent ring to its floor of 2 (max(2, max_subs + 1,
     * ns_subs, nodes) — max_subs must be 1 so the withdraw-unit term
     * stays at the floor). */
    cfg.max_intents = 2;
    cfg.max_ns_nodes = 2;
    cfg.max_ns_subs = 2;
    cfg.max_subs = 1;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moq_bytes_t nsp[1];
    moqr_ns_t ns = NS1(nsp, "wb");
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, ns) == MOQR_OK);

    /* Target track (its UPSTREAM_SUBSCRIBE is drained away immediately). */
    moqr_track_t track = (moqr_track_t){ 0 };
    uint64_t tgen = 0;
    moqr_sub_t sub = sub_track(c, sb, ns, "v", 1, &track, &tgen, NULL);
    MOQ_TEST_CHECK(tgen != 0);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 5) == MOQR_OK);

    /* Fill the 2-slot ring exactly: two namespace watchers, each replaying
     * one NS_FOUND for the already-announced "wb". */
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, sb, ns, 91) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, pb, ns, 92) == MOQR_OK);

    /* Ring 2/2 full: the cancel must refuse with ZERO mutation. */
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, track, tgen, 10) ==
                   MOQR_ERR_WOULD_BLOCK);
    /* Zero mutation proof: after draining, the SAME identity still cancels
     * (a premature gen bump would make this STALE). */
    (void)drain(c, (moqr_intent_t[8]){ 0 }, 8);
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, track, tgen, 20) == MOQR_OK);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("uc_would_block");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_uc_pending_empty_frees();
    failures += test_uc_warm_rejoin_preserved();
    failures += test_uc_guards();
    failures += test_uc_would_block();
    if (failures == 0) {
        printf("ALL PASS\n");
    }
    return failures == 0 ? 0 : 1;
}
