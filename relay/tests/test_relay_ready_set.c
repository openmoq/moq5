/*
 * Binding-ready set oracle. White-box (includes relay.c): the core must mark
 * exactly the bindings whose deliverable state can have changed — activation
 * (parked resolve + immediate accept), the whole ingest family (whole/OPEN/
 * chunk/completion/abandon), seal, eviction pending-skip, and subscription
 * retirement — deduplicated by slot, filtered of pseudo-bindings at
 * production, drained in ascending slot order with exact remainder
 * retention, and cleared at binding close before slot reuse. Capacity: the
 * bitset's descriptor and create-time allocation terms are pinned across
 * word boundaries (1/63/64/65 bindings). Nothing consumes the set in
 * production yet, so every fixture drives the public API and observes only
 * moqr_core_drain_ready plus white-box set internals.
 */

#include "relay.c"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* counting allocator (same shape as test_relay_log) */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
} ca_t;
/* An upstream terminal arrives in some connection's draft; these rigs speak
 * draft-16 unless a case says otherwise. */
static moqr_pd_desc_t
pd_local(moqr_pd_status_t st)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(st, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}

static moqr_reset_desc_t
rd_wire(uint64_t code)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_wire(MOQ_VERSION_DRAFT_16, code, &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}

static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
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

static moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

static moqr_ns_t
NS2(moq_bytes_t *storage, const char *a, const char *b)
{
    storage[0] = B(a);
    storage[1] = B(b);
    return (moqr_ns_t){ .parts = storage, .count = 2 };
}

static moqr_core_t *
mkcore(ca_t *a, uint32_t max_bindings, uint32_t max_subs)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.max_bindings = max_bindings;
    cfg.max_subs = max_subs;
    cfg.max_tracks = 4;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 1000;
    moqr_core_t *c = NULL;
    if (moqr_core_create(&cfg, &c) != MOQR_OK) {
        return NULL;
    }
    return c;
}

static moqr_result_t
ing(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t o)
{
    uint8_t buf[32];
    memset(buf, (uint8_t)(g * 16 + o), sizeof(buf));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&a->vt, buf, sizeof(buf), &pl) != 0) {
        return MOQR_ERR_NOMEM;
    }
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = 0;
    d.object_id = o;
    d.publisher_priority = 128;
    d.payload = pl;
    d.now_us = 1;
    moqr_result_t rc = moqr_core_ingest(c, t, &d);
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(pl);
    }
    return rc;
}

/* Open a chunk-through record (OPEN head). */
static moqr_result_t
ing_open(moqr_core_t *c, moqr_track_t t, uint64_t g, uint64_t o,
         uint64_t declared)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = 0;
    d.object_id = o;
    d.publisher_priority = 128;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = declared;
    d.now_us = 1;
    return moqr_core_ingest(c, t, &d);
}

static moqr_result_t
ing_chunk(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t o,
          uint8_t fill)
{
    uint8_t buf[16];
    memset(buf, fill, sizeof(buf));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&a->vt, buf, sizeof(buf), &pl) != 0) {
        return MOQR_ERR_NOMEM;
    }
    moqr_result_t rc = moqr_core_append_chunk(c, t, g, 0, o, pl);
    moq_rcbuf_decref(pl);   /* append borrows/retains; caller's ref drops */
    return rc;
}

/* Drain everything and compare against an expected cookie sequence (which
 * is ascending-slot by construction of the fixtures). Returns 0 on match. */
static int
expect_drain(moqr_core_t *c, const uint64_t *want, uint32_t want_n)
{
    uint64_t got[128];
    uint32_t n = moqr_core_drain_ready(c, got, 128);
    if (n != want_n) {
        fprintf(stderr, "  drain: got %u cookies, want %u\n", n, want_n);
        return 1;
    }
    for (uint32_t i = 0; i < n; i++) {
        if (got[i] != want[i]) {
            fprintf(stderr, "  drain[%u]: got %llu want %llu\n", i,
                    (unsigned long long)got[i],
                    (unsigned long long)want[i]);
            return 1;
        }
    }
    if (c->ready_count != 0) {
        fprintf(stderr, "  drain: count %u after full drain\n",
                c->ready_count);
        return 1;
    }
    return 0;
}

/* Create an ACTIVE track named `name` with one subscription per binding in
 * subs[]. Returns the track handle; sub handles land in out_subs. */
static moqr_track_t
mk_active_track(moqr_core_t *c, moq_bytes_t *nsb, const char *name,
                const moqr_binding_t *subs, const moqr_sub_t **unused,
                moqr_sub_t *out_subs, uint32_t nsubs, uint64_t *cookie)
{
    (void)unused;
    moqr_track_t track = { 0 };
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B(name);
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    for (uint32_t i = 0; i < nsubs; i++) {
        rq.cookie = (*cookie)++;
        if (moqr_core_subscribe(c, subs[i], &rq, &out_subs[i]) != MOQR_OK) {
            return track;
        }
    }
    moqr_intent_t its[16];
    size_t n = moqr_core_poll_intents(c, its, 16);
    bool pending = false;
    uint64_t tgen = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            track = its[i].track;
            tgen = its[i].track_gen;
            pending = true;
        }
    }
    if (!pending) {
        /* Already ACTIVE (or a lingering warm slot): immediate accepts,
         * nothing to resolve. */
        uint64_t g = 0;
        if (moqr_core_sub_track(c, out_subs[0], &track, &g) != MOQR_OK) {
            moqr_track_t none = { 0 };
            return none;
        }
        return track;
    }
    if (moqr_core_upstream_ok(c, track, tgen, 700, false, 0, 0) != MOQR_OK) {
        moqr_track_t none = { 0 };
        return none;
    }
    (void)moqr_core_poll_intents(c, its, 16);
    return track;
}

/* -- 1. Set mechanics: dedup, ascending drain, partial drain, cap 0 --------- */

static int
test_ready_mechanics(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    /* 70 bindings spans two bitset words: slots 64+ prove cross-word
     * ascending order. */
    moqr_core_t *c = mkcore(&a, 70, 16);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t bh[70];
    for (uint32_t i = 0; i < 70; i++) {
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 1000 + i, &bh[i]) ==
                       MOQR_OK);
    }
    /* Duplicate marks: one bit, one count. */
    ready_mark(c, 3);
    ready_mark(c, 3);
    ready_mark(c, 3);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 1);
    /* Mark out of order, across words. */
    ready_mark(c, 65);
    ready_mark(c, 0);
    ready_mark(c, 64);
    ready_mark(c, 69);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 5);

    /* cap == 0 mutates nothing. */
    uint64_t out[8];
    MOQ_TEST_CHECK_EQ_U64(moqr_core_drain_ready(c, out, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 5);

    /* Partial drain: lowest two slots; the suffix stays marked. */
    MOQ_TEST_CHECK_EQ_U64(moqr_core_drain_ready(c, out, 2), 2);
    MOQ_TEST_CHECK_EQ_U64(out[0], 1000);        /* slot 0  */
    MOQ_TEST_CHECK_EQ_U64(out[1], 1003);        /* slot 3  */
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 3);

    /* Remainder drains ascending across the word boundary. */
    uint64_t want[3] = { 1064, 1065, 1069 };
    MOQ_TEST_CHECK(expect_drain(c, want, 3) == 0);

    /* Re-mark after drain works (drained != permanently consumed). */
    ready_mark(c, 64);
    uint64_t want2[1] = { 1064 };
    MOQ_TEST_CHECK(expect_drain(c, want2, 1) == 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ready_mechanics");
    return failures;
}

/* -- 2. Activation: parked never marked; resolve + immediate accept mark ---- */

static int
test_ready_activation(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 8, 16);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsb[2];
    moqr_binding_t pub, s1, s2, s3;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 10, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 11, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 12, &s2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 13, &s3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 0);

    /* PARKED subscriptions do not mark. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    moqr_sub_t k1, k2, k3;
    rq.cookie = 1;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &k1) == MOQR_OK);
    rq.cookie = 2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s2, &rq, &k2) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 0);

    moqr_intent_t its[16];
    size_t n = moqr_core_poll_intents(c, its, 16);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);

    /* Activation marks exactly the parked subs' bindings. */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, its[0].track, its[0].track_gen,
                                         700, false, 0, 0) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 16);
    uint64_t want[2] = { 11, 12 };
    MOQ_TEST_CHECK(expect_drain(c, want, 2) == 0);

    /* Immediate ACTIVE accept marks the new subscriber only. */
    rq.cookie = 3;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s3, &rq, &k3) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 16);
    uint64_t want2[1] = { 13 };
    MOQ_TEST_CHECK(expect_drain(c, want2, 1) == 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ready_activation");
    return failures;
}

/* -- 3. Ingest family: exact per-source sets; unrelated tracks excluded ----- */

static int
test_ready_ingest_family(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 8, 16);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsb[2];
    moqr_binding_t pub, s1, s2, s3;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 10, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 11, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 12, &s2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 13, &s3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);

    uint64_t cookie = 100;
    moqr_binding_t subsA[2] = { s1, s2 };
    moqr_binding_t subsB[1] = { s3 };
    moqr_sub_t ksA[2], ksB[1];
    moqr_track_t ta =
        mk_active_track(c, nsb, "ta", subsA, NULL, ksA, 2, &cookie);
    moqr_track_t tb =
        mk_active_track(c, nsb, "tb", subsB, NULL, ksB, 1, &cookie);
    MOQ_TEST_CHECK(moqr_track_is_valid(ta) && moqr_track_is_valid(tb));
    (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);   /* flush */

    uint64_t wantA[2] = { 11, 12 };
    uint64_t wantB[1] = { 13 };

    /* Whole-object ingest marks exactly track A's subscribers. */
    MOQ_TEST_CHECK(ing(c, &a, ta, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK(expect_drain(c, wantA, 2) == 0);

    /* ...and track B's ingest marks only its own. */
    MOQ_TEST_CHECK(ing(c, &a, tb, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK(expect_drain(c, wantB, 1) == 0);

    /* OPEN head. */
    MOQ_TEST_CHECK(ing_open(c, ta, 1, 1, 16) == MOQR_OK);
    MOQ_TEST_CHECK(expect_drain(c, wantA, 2) == 0);

    /* Chunk growth. */
    MOQ_TEST_CHECK(ing_chunk(c, &a, ta, 1, 1, 0x41) == MOQR_OK);
    MOQ_TEST_CHECK(expect_drain(c, wantA, 2) == 0);

    /* Completion. */
    MOQ_TEST_CHECK(moqr_core_complete_record(c, ta, 1, 0, 1) == MOQR_OK);
    MOQ_TEST_CHECK(expect_drain(c, wantA, 2) == 0);

    /* Abandon (a fresh OPEN record). */
    MOQ_TEST_CHECK(ing_open(c, ta, 1, 2, 16) == MOQR_OK);
    (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, ta, 1, 0, 2, rd_wire(7)) == MOQR_OK);
    MOQ_TEST_CHECK(expect_drain(c, wantA, 2) == 0);

    /* Seal. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, ta, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK(expect_drain(c, wantA, 2) == 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ready_ingest_family");
    return failures;
}

/* -- 4. Eviction pending-skip marks at the reclaim site --------------------- */

static int
test_ready_eviction_skip(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 8, 16);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsb[2];
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 10, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 11, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    uint64_t cookie = 100;
    moqr_binding_t subs[1] = { s1 };
    moqr_sub_t ks[1];
    moqr_track_t t = mk_active_track(c, nsb, "t", subs, NULL, ks, 1, &cookie);
    MOQ_TEST_CHECK(moqr_track_is_valid(t));

    /* Retained groups 1..4; deliver one object so the sub owns position
     * entries for every scanned group. */
    for (uint64_t g = 1; g <= 4; g++) {
        MOQ_TEST_CHECK(ing(c, &a, t, g, 0) == MOQR_OK);
    }
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, 100, &d) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           100) == MOQR_OK);

    /* Evict 1..4 by ingesting 5..8, then flush every ingest-driven mark. */
    for (uint64_t g = 5; g <= 8; g++) {
        MOQ_TEST_CHECK(ing(c, &a, t, g, 0) == MOQR_OK);
    }
    (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 0);

    /* The next delivery pass reclaims the evicted, unbegun positions
     * (pending_skip) — the reclaim site itself must mark. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, 200, &d) == MOQR_OK);
    MOQ_TEST_CHECK(c->subs[0].pending_skip);   /* the reclaim latched */
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 1);
    uint64_t want[1] = { 11 };
    MOQ_TEST_CHECK(expect_drain(c, want, 1) == 0);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           200) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ready_eviction_skip");
    return failures;
}

/* -- 5. Retirement marks the binding (sibling work can surface) ------------- */

static int
test_ready_retire(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 8, 16);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsb[2];
    moqr_binding_t pub, s1, s2;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 10, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 11, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 12, &s2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    uint64_t cookie = 100;
    moqr_binding_t subs[2] = { s1, s2 };
    moqr_sub_t ks[2];
    moqr_track_t t = mk_active_track(c, nsb, "t", subs, NULL, ks, 2, &cookie);
    MOQ_TEST_CHECK(moqr_track_is_valid(t));
    (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);
    moqr_intent_t its[16];

    /* Explicit unsubscribe (silent local retire) marks its binding. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, ks[0], 50) == MOQR_OK);
    uint64_t want1[1] = { 11 };
    MOQ_TEST_CHECK(expect_drain(c, want1, 1) == 0);

    /* Revoke (auth transition) marks its binding. */
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, ks[1], pd_local(MOQR_PD_UNAUTHORIZED), 60) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 16);
    uint64_t want2[1] = { 12 };
    MOQ_TEST_CHECK(expect_drain(c, want2, 1) == 0);

    /* Sibling surfacing — the STATED reason for the retire mark. Two
     * subscriptions on ONE binding, different tracks; only the sibling's
     * track holds an undelivered object and its ingest mark is flushed.
     * Retiring the other subscription must re-mark the binding, and the
     * mark must point at REAL work: next_delivery selects the sibling. */
    {
        moqr_sub_t ka[1], kb[1];
        moqr_binding_t one[1] = { s1 };
        moqr_track_t tx =
            mk_active_track(c, nsb, "tx", one, NULL, ka, 1, &cookie);
        moqr_track_t ty =
            mk_active_track(c, nsb, "ty", one, NULL, kb, 1, &cookie);
        MOQ_TEST_CHECK(moqr_track_is_valid(tx) && moqr_track_is_valid(ty));
        /* Only the sibling (kb, track ty) becomes deliverable. */
        MOQ_TEST_CHECK(ing(c, &a, ty, 3, 0) == MOQR_OK);
        (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);
        MOQ_TEST_CHECK_EQ_U64(c->ready_count, 0);
        /* Retire ka: the binding must re-mark although ka itself had
         * nothing deliverable. */
        MOQ_TEST_CHECK(moqr_core_unsubscribe(c, ka[0], 65) == MOQR_OK);
        uint64_t wants[1] = { 11 };
        MOQ_TEST_CHECK(expect_drain(c, wants, 1) == 0);
        /* The mark points at real sibling work. */
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, 66, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 3);
        MOQ_TEST_CHECK_EQ_U64(d.sub._opaque, kb[0]._opaque);
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1,
                                               MOQR_DELIVERY_DELIVERED,
                                               66) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_unsubscribe(c, kb[0], 67) == MOQR_OK);
        (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);
    }

    /* Teardown: force_withdraw retires ACTIVE subs (SUB_DONE) — both
     * subscriber bindings mark through the same retire path. */
    moqr_sub_t ks2[2];
    moqr_track_t t2 =
        mk_active_track(c, nsb, "t", subs, NULL, ks2, 2, &cookie);
    MOQ_TEST_CHECK(moqr_track_is_valid(t2));
    MOQ_TEST_CHECK(ing(c, &a, t2, 1, 0) == MOQR_OK);
    (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS2(nsb, "n", "s"), 0x10,
                                            70) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 16);
    {
        moqr_revoked_grant_t rg[8];
        size_t g = moqr_core_peek_revoked_grants(c, rg, 8);
        for (size_t i = 0; i < g; i++) {
            moqr_core_ack_revoked_grant(c, rg[i].binding_cookie,
                                        rg[i].session_cookie);
        }
    }
    uint64_t want3[2] = { 11, 12 };
    MOQ_TEST_CHECK(expect_drain(c, want3, 2) == 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ready_retire");
    return failures;
}

/* -- 6. Pseudo-bindings are filtered at production --------------------------- */

static int
test_ready_pseudo_filtered(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 8, 16);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsb[2];
    moqr_binding_t pub, s1, ps;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 10, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 11, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, MOQR_SHARD_COOKIE_BASE | 5u,
                                          &ps) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    uint64_t cookie = 100;
    moqr_binding_t subs[2] = { s1, ps };   /* real + pseudo on ONE track */
    moqr_sub_t ks[2];
    moqr_track_t t = mk_active_track(c, nsb, "t", subs, NULL, ks, 2, &cookie);
    MOQ_TEST_CHECK(moqr_track_is_valid(t));
    (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);

    /* Track-wide ingest walks BOTH subs; only the real binding enters. */
    MOQ_TEST_CHECK(ing(c, &a, t, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 1);
    uint64_t want[1] = { 11 };
    MOQ_TEST_CHECK(expect_drain(c, want, 1) == 0);

    /* Even a direct mark of the pseudo slot is a no-op. */
    {
        uint32_t pslot = 0;
        MOQ_TEST_CHECK(binding_resolve(c, ps, &pslot) != NULL);
        ready_mark(c, pslot);
        MOQ_TEST_CHECK_EQ_U64(c->ready_count, 0);
    }

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ready_pseudo_filtered");
    return failures;
}

/* -- 7. Close clears before slot reuse --------------------------------------- */

static int
test_ready_close_reuse(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 4, 16);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsb[2];
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 10, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 11, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    uint64_t cookie = 100;
    moqr_binding_t subs[1] = { s1 };
    moqr_sub_t ks[1];
    moqr_track_t t = mk_active_track(c, nsb, "t", subs, NULL, ks, 1, &cookie);
    MOQ_TEST_CHECK(moqr_track_is_valid(t));
    (void)moqr_core_drain_ready(c, (uint64_t[16]){ 0 }, 16);

    uint32_t s1slot = 0;
    MOQ_TEST_CHECK(binding_resolve(c, s1, &s1slot) != NULL);

    /* Mark via ingest, then close WITHOUT draining. */
    MOQ_TEST_CHECK(ing(c, &a, t, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 1);
    moqr_intent_t its[16];
    moqr_result_t rc = moqr_core_binding_close(c, s1, 200);
    while (rc == MOQR_ERR_WOULD_BLOCK) {
        (void)moqr_core_poll_intents(c, its, 16);
        rc = moqr_core_binding_close(c, s1, 200);
    }
    MOQ_TEST_CHECK(rc == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 16);
    MOQ_TEST_CHECK_EQ_U64(c->ready_count, 0);   /* cleared at close */

    /* Reuse the slot: the replacement must not inherit a mark. */
    moqr_binding_t s1b;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 99, &s1b) == MOQR_OK);
    uint32_t reslot = 0;
    MOQ_TEST_CHECK(binding_resolve(c, s1b, &reslot) != NULL);
    MOQ_TEST_CHECK_EQ_U64(reslot, s1slot);   /* same pool slot */
    uint64_t out[4];
    MOQ_TEST_CHECK_EQ_U64(moqr_core_drain_ready(c, out, 4), 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ready_close_reuse");
    return failures;
}

/* -- 8. Capacity: bitset word terms across boundary sizes -------------------- */

static uint64_t
describe_structure_b(ca_t *a, uint32_t max_bindings)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.max_bindings = max_bindings;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    moqr_core_capacity_t cap;
    if (moqr_core_capacity_describe(&cfg, &cap) != MOQR_OK) {
        return UINT64_MAX;
    }
    return cap.structure_bytes;
}

static long
create_live_bytes_b(ca_t *a, uint32_t max_bindings)
{
    long before = a->live;
    moqr_core_t *c = mkcore(a, max_bindings, 16);
    if (c == NULL) {
        return -1;
    }
    long created = a->live - before;
    moqr_core_destroy(c);
    return a->live == before ? created : -1;   /* -1: leaked */
}

static int
test_ready_capacity(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    /* Per-binding descriptor slope: the pool struct + the worst-case
     * chunk-pin array the model has always charged. The bitset adds ONE
     * 8-byte word exactly when the binding count crosses a multiple of 64. */
    moqr_log_cfg_t lc;
    moqr_log_cfg_init_sized(&lc, sizeof(lc), &a.vt);
    lc.budget.max_groups = 4;
    lc.budget.max_bytes = 1 << 20;
    moqr_log_capacity_t logc;
    MOQ_TEST_CHECK(moqr_log_capacity_describe(&lc, &logc) == MOQR_OK);
    /* max_cancels resolves to max_bindings by default, so the per-binding
     * descriptor slope carries one pending-cancel record too. */
    uint64_t per_binding = sizeof(r_binding_t) + sizeof(r_pending_cancel) +
                           logc.max_chunk_nodes * sizeof(r_pin_chunk_t);

    uint64_t d1 = describe_structure_b(&a, 1);
    uint64_t d63 = describe_structure_b(&a, 63);
    uint64_t d64 = describe_structure_b(&a, 64);
    uint64_t d65 = describe_structure_b(&a, 65);
    uint64_t d128 = describe_structure_b(&a, 128);
    uint64_t d129 = describe_structure_b(&a, 129);
    MOQ_TEST_CHECK(d1 != UINT64_MAX && d129 != UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(d63 - d1, 62 * per_binding);          /* 1 word  */
    MOQ_TEST_CHECK_EQ_U64(d64 - d63, per_binding);              /* 1 word  */
    MOQ_TEST_CHECK_EQ_U64(d65 - d64, per_binding + 8);          /* 2 words */
    MOQ_TEST_CHECK_EQ_U64(d128 - d65, 63 * per_binding);        /* 2 words */
    MOQ_TEST_CHECK_EQ_U64(d129 - d128, per_binding + 8);        /* 3 words */

    /* Create-time allocator slope: pools only (pin arrays/logs are lazy),
     * so the measured deltas isolate the pool struct + the bitset word. */
    long c63 = create_live_bytes_b(&a, 63);
    long c64 = create_live_bytes_b(&a, 64);
    long c65 = create_live_bytes_b(&a, 65);
    MOQ_TEST_CHECK(c63 > 0 && c64 > 0 && c65 > 0);
    uint64_t create_pb = sizeof(r_binding_t) + sizeof(r_pending_cancel);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(c64 - c63), create_pb);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(c65 - c64), create_pb + 8);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);

    /* The shared word-count helper must be immune to the 32-bit +63
     * wrap: at UINT32_MAX a narrow round-up computes ZERO words. */
    MOQ_TEST_CHECK_EQ_U64(ready_word_count(1), 1);
    MOQ_TEST_CHECK_EQ_U64(ready_word_count(64), 1);
    MOQ_TEST_CHECK_EQ_U64(ready_word_count(65), 2);
    MOQ_TEST_CHECK_EQ_U64(ready_word_count(UINT32_MAX - 63), 67108863);
    MOQ_TEST_CHECK_EQ_U64(ready_word_count(UINT32_MAX), 67108864);

    /* The word formula holds at the top of the configurable range too. */
    uint64_t dmaxm64 = describe_structure_b(&a, UINT32_MAX - 64);
    uint64_t dmax = describe_structure_b(&a, UINT32_MAX);
    MOQ_TEST_CHECK(dmax != UINT64_MAX && dmaxm64 != UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(dmax - dmaxm64, 64 * per_binding + 8);

    MOQ_TEST_PASS("ready_capacity");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_ready_mechanics();
    failures += test_ready_activation();
    failures += test_ready_ingest_family();
    failures += test_ready_eviction_skip();
    failures += test_ready_retire();
    failures += test_ready_pseudo_filtered();
    failures += test_ready_close_reuse();
    failures += test_ready_capacity();
    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("ALL PASS\n");
    return 0;
}
