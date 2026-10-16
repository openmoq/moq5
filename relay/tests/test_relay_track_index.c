/*
 * Per-track subscriber index oracle. White-box (includes relay.c): the
 * intrusive track_next/track_prev list on each track must hold EXACTLY the
 * subscriptions track_sub_count's historical exhaustive-scan predicate
 * accepts — live sub, bound to this track slot, generation-matched — with
 * bidirectional link integrity, no cycles, no duplicates, no stale or
 * cross-track members, at every point of the subscription lifecycle:
 * parked creation, activation, range completion, unsubscribe, revoke,
 * binding close, upstream error, track teardown (force_withdraw), warm
 * rejoin, and slot reuse. A randomized churn arm validates the whole pool
 * against the scan oracle after every operation. Capacity: the descriptor
 * and create-time allocation deltas per sub/track slot are pinned to the
 * exact sizeof terms the model advertises; every core destroys leak-free.
 */

#include "relay.c"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* counting allocator (same shape as test_relay_log) */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
} ca_t;
static moqr_pd_desc_t
pd_local(moqr_pd_status_t st)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_local(st, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
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

/* -- Layout pins ------------------------------------------------------------
 * Field-for-field replicas of r_track_t / r_sub_t WITHOUT the index fields,
 * so the structural delta the capacity report claims (+8 B per sub slot,
 * +0 B per track slot — subs_head absorbed by padding) is pinned against an
 * independent baseline instead of the very sizeof values production uses.
 * A layout change on either side breaks these at compile time. */
typedef struct r_track_baseline {
    uint32_t        gen;
    r_key_t         key;
    uint64_t        hash;
    moqr_log_t     *log;
    r_track_state_t state;
    uint64_t        track_gen;
    r_track_source_t source_kind;
    bool            has_upstream_binding;
    uint32_t        up_binding;
    uint32_t        up_binding_gen;
    uint32_t        src_ann_node;
    uint32_t        src_ann_binding;
    uint32_t        src_ann_binding_gen;
    uint64_t        upstream_cookie;
    bool            has_largest;
    uint64_t        largest_group;
    uint64_t        largest_object;
    uint64_t        linger_deadline_us;
} r_track_baseline_t;

typedef struct r_sub_baseline {
    uint32_t      gen;
    uint32_t      binding;
    uint32_t      binding_gen;
    uint32_t      sub_next;
    uint32_t      sub_prev;
    uint32_t      track;
    uint32_t      track_gen_slot;
    uint8_t      *gpos;
    uint32_t      gpos_groups;
    uint32_t      gpos_lists;
    uint32_t     *gpos_hash;
    uint32_t      gpos_hash_cap;
    uint64_t      gpos_gc_evicted;
    uint32_t      begun_count;
    bool          pending_skip;
    r_sub_state_t state;
    uint64_t      start_group;
    uint64_t      start_object;
    bool          has_end;
    uint64_t      end_group;
    uint8_t       subscriber_priority;
    moqr_group_order_t group_order;
    moqr_filter_type_t filter_type;
    uint64_t      cookie;
} r_sub_baseline_t;

_Static_assert(sizeof(r_sub_t) == sizeof(r_sub_baseline_t) +
                                      2 * sizeof(uint32_t),
               "track_next/track_prev must cost exactly 8 bytes per sub");
/* subs_head fits former padding (0 bytes); the reclaim bookkeeping
 * (interest_seq + idle_since_us) costs exactly 16 bytes per track. */
_Static_assert(sizeof(r_track_t) == sizeof(r_track_baseline_t) +
                                        2 * sizeof(uint64_t),
               "r_track_t grows only by the two reclaim words");

/* -- The oracle ------------------------------------------------------------ */

/* The HISTORICAL membership predicate (the exhaustive scan the index
 * replaced), kept verbatim as the independent truth. */
static bool
oracle_member(const moqr_core_t *c, uint32_t sslot, uint32_t tslot)
{
    return (c->subs[sslot].gen & 1u) != 0 && c->subs[sslot].track == tslot &&
           c->subs[sslot].track_gen_slot == c->tracks[tslot].gen;
}

static uint32_t
oracle_scan_count(const moqr_core_t *c, uint32_t tslot)
{
    uint32_t n = 0;
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if (oracle_member(c, i, tslot)) {
            n++;
        }
    }
    return n;
}

/* Validate one track's index against the scan oracle. Returns NULL when
 * consistent, else a static string naming the SPECIFIC violation (the failure
 * signatures key off these). */
static const char *
index_check(const moqr_core_t *c, uint32_t tslot, bool *seen /* max_subs */)
{
    memset(seen, 0, c->max_subs * sizeof(*seen));
    if ((c->tracks[tslot].gen & 1u) == 0) {
        /* A slot that HAS lived (gen > 0) must have had its list cleared by
         * track_free_slot; a never-created slot (gen 0) is zeroed pool
         * memory and carries no meaning. */
        if (c->tracks[tslot].gen != 0 &&
            c->tracks[tslot].subs_head != R_SUB_NIL) {
            return "dead-track-head-not-cleared";
        }
        return NULL;
    }
    uint32_t steps = 0;
    uint32_t prev = R_SUB_NIL;
    for (uint32_t i = c->tracks[tslot].subs_head; i != R_SUB_NIL;
         i = c->subs[i].track_next) {
        if (i >= c->max_subs) {
            return "index-out-of-range";
        }
        if (++steps > c->max_subs) {
            return "index-cycle";
        }
        if (seen[i]) {
            return "index-duplicate-member";
        }
        seen[i] = true;
        if ((c->subs[i].gen & 1u) == 0) {
            return "index-holds-dead-sub";
        }
        if (c->subs[i].track != tslot) {
            return "index-cross-track-member";
        }
        if (c->subs[i].track_gen_slot != c->tracks[tslot].gen) {
            return "index-stale-generation-member";
        }
        if (c->subs[i].track_prev != prev) {
            return "index-back-link-broken";
        }
        prev = i;
    }
    /* Exactly-once coverage: every sub the scan predicate accepts must have
     * been walked; nothing else may have been (non-members already rejected
     * above, so only omissions remain). */
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if (oracle_member(c, i, tslot) && !seen[i]) {
            return "index-missing-live-sub";
        }
    }
    if (steps != oracle_scan_count(c, tslot) ||
        steps != track_sub_count(c, tslot)) {
        return "index-count-mismatch";
    }
    return NULL;
}

/* Validate EVERY track slot; returns the first violation ("track:reason")
 * into errbuf, or NULL. */
static const char *
index_check_all(const moqr_core_t *c, bool *seen, char *errbuf, size_t cap)
{
    for (uint32_t t = 0; t < c->max_tracks; t++) {
        const char *why = index_check(c, t, seen);
        if (why != NULL) {
            snprintf(errbuf, cap, "track %" PRIu32 ": %s", t, why);
            return errbuf;
        }
    }
    /* Orphan sweep: every live sub is linked at creation, so a live sub
     * that is NOT a member of its track (the track died or its slot was
     * recycled) must have had both links cleared by track_free_slot. */
    for (uint32_t i = 0; i < c->max_subs; i++) {
        if ((c->subs[i].gen & 1u) == 0 ||
            oracle_member(c, i, c->subs[i].track)) {
            continue;
        }
        if (c->subs[i].track_next != R_SUB_NIL ||
            c->subs[i].track_prev != R_SUB_NIL) {
            snprintf(errbuf, cap, "sub %" PRIu32 ": orphan-links-not-cleared", i);
            return errbuf;
        }
    }
    return NULL;
}

/* -- Fixtures --------------------------------------------------------------- */

static moqr_core_t *
mkcore(ca_t *a, uint32_t max_subs, uint32_t max_tracks)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.max_subs = max_subs;
    cfg.max_tracks = max_tracks;
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

#define CHECK_INDEX(c, seen)                                                \
    do {                                                                    \
        char eb[128];                                                       \
        const char *why_ = index_check_all((c), (seen), eb, sizeof(eb));    \
        if (why_ != NULL) {                                                 \
            fprintf(stderr, "  index oracle: %s\n", why_);                  \
        }                                                                   \
        MOQ_TEST_CHECK(why_ == NULL);                                       \
    } while (0)

/* -- 1. Deterministic lifecycle coverage ------------------------------------ */

static int
test_index_lifecycle(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 16, 4);
    MOQ_TEST_CHECK(c != NULL);
    bool seen[16];
    moq_bytes_t nsb[2];

    moqr_binding_t pub, s1, s2, s3;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 3, &s2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 4, &s3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    CHECK_INDEX(c, seen);

    /* Parked creation: three subs on a new PENDING track. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    moqr_sub_t k1, k2, k3;
    rq.cookie = 1;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &k1) == MOQR_OK);
    CHECK_INDEX(c, seen);
    rq.cookie = 2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s2, &rq, &k2) == MOQR_OK);
    rq.cookie = 3;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s3, &rq, &k3) == MOQR_OK);
    CHECK_INDEX(c, seen);

    moqr_intent_t its[16];
    size_t n = moqr_core_poll_intents(c, its, 16);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_track_t track = its[0].track;

    /* Parked activation: membership must be unchanged across resolve. */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, its[0].track_gen, 77, true,
                                         1, 0) == MOQR_OK);
    CHECK_INDEX(c, seen);
    (void)moqr_core_poll_intents(c, its, 16);   /* 3 ACCEPTs */

    /* Unsubscribe (k1), revoke (k2): the two explicit retire entries. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, k1, 10) == MOQR_OK);
    CHECK_INDEX(c, seen);
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, k2, pd_local(MOQR_PD_UNAUTHORIZED), 11) == MOQR_OK);
    CHECK_INDEX(c, seen);
    (void)moqr_core_poll_intents(c, its, 16);

    /* Binding close retires k3 (the linger arms: last subscriber out). */
    MOQ_TEST_CHECK(moqr_core_binding_close(c, s3, 12) == MOQR_OK);
    CHECK_INDEX(c, seen);
    (void)moqr_core_poll_intents(c, its, 16);

    /* Warm rejoin reuses the track slot and a retired SUB SLOT: creation-
     * time relink on reused slots is exercised here. */
    rq.cookie = 4;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &k1) == MOQR_OK);
    CHECK_INDEX(c, seen);
    (void)moqr_core_poll_intents(c, its, 16);

    /* Range completion (normal delivery-path retirement): group 1 only,
     * SUB_DONE once group 2 arrives, retire via delivery_done. */
    moqr_subscribe_req_t rr;
    moqr_subscribe_req_init(&rr);
    rr.ns = NS2(nsb, "n", "s");
    rr.name = B("t");
    rr.filter.type = MOQR_FILTER_ABSOLUTE_RANGE;
    rr.filter.start_group = 1;
    rr.filter.start_object = 0;
    rr.filter.end_group_delta = 0;
    rr.cookie = 5;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s2, &rr, &k2) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 16);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 2, 0) == MOQR_OK);
    for (;;) {
        moqr_delivery_t d;
        moqr_result_t rc = moqr_core_next_delivery(c, s2, 20, &d);
        if (rc != MOQR_OK) {
            break;
        }
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s2,
                                               MOQR_DELIVERY_DELIVERED,
                                               20) == MOQR_OK);
        CHECK_INDEX(c, seen);
    }
    CHECK_INDEX(c, seen);   /* k2 range-retired; k1 still indexed */
    (void)moqr_core_poll_intents(c, its, 16);

    /* Straggler setup: retire k1 (arms the linger), tick the track WARM,
     * then warm-rejoin k4 — an ACTIVE subscriber on a PENDING track (the
     * rejoin re-enters PENDING; no ingest follows, so it stays there).
     * force_withdraw terminals only PARKED subs on PENDING tracks, so k4
     * is the straggler the pool-gen bump orphans — track_free_slot's
     * clearing walk is what must sever its links. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, k1, 25) == MOQR_OK);
    CHECK_INDEX(c, seen);
    MOQ_TEST_CHECK(moqr_core_tick(c, 2025) == MOQR_OK);   /* past linger */
    (void)moqr_core_poll_intents(c, its, 16);
    moqr_sub_t k4;
    rq.cookie = 7;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &k4) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 16);
    CHECK_INDEX(c, seen);
    uint32_t orphan_slot = 0, dead_tslot = 0;
    MOQ_TEST_CHECK(sub_resolve(c, k4, &orphan_slot) != NULL);
    MOQ_TEST_CHECK(track_resolve(c, track, &dead_tslot) != NULL);
    MOQ_TEST_CHECK(c->tracks[dead_tslot].state == R_TRACK_PENDING);
    MOQ_TEST_CHECK(c->subs[orphan_slot].state == R_SUB_ACTIVE);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS2(nsb, "n", "s"), 0x10,
                                            2030) == MOQR_OK);
    CHECK_INDEX(c, seen);
    MOQ_TEST_CHECK((c->tracks[dead_tslot].gen & 1u) == 0);
    MOQ_TEST_CHECK(c->tracks[dead_tslot].subs_head == R_SUB_NIL);
    MOQ_TEST_CHECK((c->subs[orphan_slot].gen & 1u) != 0);   /* orphaned, live */
    MOQ_TEST_CHECK(c->subs[orphan_slot].track_next == R_SUB_NIL &&
                   c->subs[orphan_slot].track_prev == R_SUB_NIL);
    (void)moqr_core_poll_intents(c, its, 16);
    {
        moqr_revoked_grant_t rg[8];
        size_t g = moqr_core_peek_revoked_grants(c, rg, 8);
        for (size_t i = 0; i < g; i++) {
            moqr_core_ack_revoked_grant(c, rg[i].binding_cookie,
                                        rg[i].session_cookie);
        }
    }

    /* Upstream error retires PARKED subs (track freed underneath them). */
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    rq.cookie = 6;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &k3) == MOQR_OK);
    n = moqr_core_poll_intents(c, its, 16);
    MOQ_TEST_CHECK(n >= 1);
    moqr_track_t t2 = { 0 };
    uint64_t t2gen = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            t2 = its[i].track;
            t2gen = its[i].track_gen;
        }
    }
    MOQ_TEST_CHECK(moqr_sub_is_valid(k3));
    CHECK_INDEX(c, seen);

    /* The replacement track reuses the dead slot; retiring the orphan now
     * must not touch the replacement's list (its generation differs). */
    uint32_t t2slot = 0;
    MOQ_TEST_CHECK(track_resolve(c, t2, &t2slot) != NULL);
    MOQ_TEST_CHECK_EQ_U64(t2slot, dead_tslot);
    uint32_t t2_members = track_sub_count(c, t2slot);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, k4, 2040) == MOQR_OK);
    CHECK_INDEX(c, seen);
    MOQ_TEST_CHECK_EQ_U64(track_sub_count(c, t2slot), t2_members);

    MOQ_TEST_CHECK(moqr_core_upstream_error(c, t2, t2gen, 404, 40) ==
                   MOQR_OK);
    CHECK_INDEX(c, seen);
    (void)moqr_core_poll_intents(c, its, 16);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("index_lifecycle");
    return failures;
}

/* -- 2. Randomized churn vs the scan oracle --------------------------------- */

static uint64_t rng_state;
static uint32_t
rnd(uint32_t bound)
{
    rng_state = rng_state * 6364136223846793005ull + 1442695040888963407ull;
    return (uint32_t)((rng_state >> 33) % bound);
}

static int
test_index_random_churn(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    enum { SUBS = 24, TRACKS = 6, NAMES = 4, SBINDS = 3 };
    moqr_core_t *c = mkcore(&a, SUBS, TRACKS);
    MOQ_TEST_CHECK(c != NULL);
    bool seen[SUBS];
    moq_bytes_t nsb[2];

    moqr_binding_t pub;
    moqr_binding_t sb[SBINDS];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    for (uint32_t i = 0; i < SBINDS; i++) {
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 200 + i, &sb[i]) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);

    static const char *names[NAMES] = { "t0", "t1", "t2", "t3" };
    moqr_sub_t live_subs[SUBS];
    uint32_t live_n = 0;
    /* Unresolved PENDING tracks seen via UPSTREAM_SUBSCRIBE intents. */
    moqr_track_t pend_t[TRACKS];
    uint64_t pend_gen[TRACKS];
    uint32_t pend_n = 0;
    uint64_t cookie = 1000;
    uint64_t now = 1;
    moqr_intent_t its[32];

    rng_state = 0x1447c0ffee15600dull;
    for (int iter = 0; iter < 4000; iter++) {
        now += 7;
        uint32_t op = rnd(100);
        if (op < 45) {   /* subscribe */
            moqr_subscribe_req_t rq;
            moqr_subscribe_req_init(&rq);
            rq.ns = NS2(nsb, "n", "s");
            rq.name = B(names[rnd(NAMES)]);
            rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
            rq.cookie = cookie++;
            moqr_sub_t h;
            moqr_result_t rc = moqr_core_subscribe(c, sb[rnd(SBINDS)], &rq,
                                                   &h);
            MOQ_TEST_CHECK(rc == MOQR_OK || rc == MOQR_ERR_CAPACITY ||
                           rc == MOQR_ERR_WOULD_BLOCK);
            if (rc == MOQR_OK && moqr_sub_is_valid(h) &&
                live_n < SUBS) {
                live_subs[live_n++] = h;
            }
        } else if (op < 65 && live_n > 0) {   /* unsubscribe or revoke */
            uint32_t pick = rnd(live_n);
            moqr_sub_t h = live_subs[pick];
            moqr_result_t rc;
            if (rnd(2) == 0) {
                rc = moqr_core_unsubscribe(c, h, now);
            } else {
                rc = moqr_core_revoke_sub(c, h, pd_local(MOQR_PD_UNAUTHORIZED), now);
            }
            MOQ_TEST_CHECK(rc == MOQR_OK || rc == MOQR_ERR_STALE_HANDLE ||
                           rc == MOQR_ERR_WOULD_BLOCK);
            if (rc != MOQR_ERR_WOULD_BLOCK) {
                live_subs[pick] = live_subs[--live_n];
            }
        } else if (op < 75 && pend_n > 0) {   /* resolve a pending track */
            uint32_t pick = rnd(pend_n);
            if (rnd(3) != 0) {
                (void)moqr_core_upstream_ok(c, pend_t[pick], pend_gen[pick],
                                            cookie++, true, 0, 0);
            } else {
                (void)moqr_core_upstream_error(c, pend_t[pick],
                                               pend_gen[pick], 404, now);
            }
            pend_t[pick] = pend_t[--pend_n];
            pend_gen[pick] = pend_gen[pend_n];
        } else if (op < 82) {   /* close + reopen a subscriber binding */
            uint32_t pick = rnd(SBINDS);
            moqr_result_t rc = moqr_core_binding_close(c, sb[pick], now);
            MOQ_TEST_CHECK(rc == MOQR_OK || rc == MOQR_ERR_WOULD_BLOCK);
            (void)moqr_core_poll_intents(c, its, 32);
            for (int tries = 0; rc == MOQR_ERR_WOULD_BLOCK; tries++) {
                MOQ_TEST_CHECK(tries < 1000);
                if (tries >= 1000) {
                    break;
                }
                rc = moqr_core_binding_close(c, sb[pick], now);
                (void)moqr_core_poll_intents(c, its, 32);
            }
            MOQ_TEST_CHECK(moqr_core_binding_open(c, 200 + SBINDS + cookie++,
                                                  &sb[pick]) == MOQR_OK);
        } else if (op < 90) {   /* withdraw everything, re-announce */
            moqr_result_t rc = moqr_core_force_withdraw(
                c, NS2(nsb, "n", "s"), 0x10, now);
            MOQ_TEST_CHECK(rc == MOQR_OK || rc == MOQR_ERR_WOULD_BLOCK);
            (void)moqr_core_poll_intents(c, its, 32);
            for (int tries = 0; rc == MOQR_ERR_WOULD_BLOCK; tries++) {
                MOQ_TEST_CHECK(tries < 1000);
                if (tries >= 1000) {
                    break;
                }
                rc = moqr_core_force_withdraw(c, NS2(nsb, "n", "s"), 0x10,
                                              now);
                (void)moqr_core_poll_intents(c, its, 32);
                moqr_revoked_grant_t rg[8];
                size_t g = moqr_core_peek_revoked_grants(c, rg, 8);
                for (size_t i = 0; i < g; i++) {
                    moqr_core_ack_revoked_grant(c, rg[i].binding_cookie,
                                                rg[i].session_cookie);
                }
            }
            {
                moqr_revoked_grant_t rg[8];
                size_t g = moqr_core_peek_revoked_grants(c, rg, 8);
                for (size_t i = 0; i < g; i++) {
                    moqr_core_ack_revoked_grant(c, rg[i].binding_cookie,
                                                rg[i].session_cookie);
                }
            }
            pend_n = 0;   /* every pending attempt died with its track */
            MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) ==
                           MOQR_OK);
        } else {   /* time passes: linger expiry frees warm tracks */
            MOQ_TEST_CHECK(moqr_core_tick(c, now + 2000) == MOQR_OK);
            now += 2000;
        }
        /* Harvest intents; remember unresolved upstream attempts. */
        size_t got = moqr_core_poll_intents(c, its, 32);
        for (size_t i = 0; i < got; i++) {
            if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE &&
                pend_n < TRACKS) {
                pend_t[pend_n] = its[i].track;
                pend_gen[pend_n] = its[i].track_gen;
                pend_n++;
            }
        }

        /* THE ORACLE: every track's index vs the exhaustive scan. */
        char eb[128];
        const char *why = index_check_all(c, seen, eb, sizeof(eb));
        if (why != NULL) {
            fprintf(stderr, "  iter %d: %s\n", iter, why);
            MOQ_TEST_CHECK(why == NULL);
            break;
        }
    }

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("index_random_churn");
    return failures;
}

/* -- 3. Capacity: exact per-slot structural deltas --------------------------- */

static uint64_t
describe_structure(ca_t *a, uint32_t max_subs, uint32_t max_tracks)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.max_subs = max_subs;
    cfg.max_tracks = max_tracks;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    moqr_core_capacity_t cap;
    if (moqr_core_capacity_describe(&cfg, &cap) != MOQR_OK) {
        return UINT64_MAX;
    }
    return cap.structure_bytes;
}

static long
create_live_bytes(ca_t *a, uint32_t max_subs, uint32_t max_tracks)
{
    long before = a->live;
    moqr_core_t *c = mkcore(a, max_subs, max_tracks);
    if (c == NULL) {
        return -1;
    }
    long created = a->live - before;
    moqr_core_destroy(c);
    return a->live == before ? created : -1;   /* -1: leaked */
}

static int
test_index_capacity_delta(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    /* Descriptor delta per added sub slot: the pool term (sizeof(r_sub_t),
     * which now carries track_next/track_prev) plus the per-sub gpos
     * position + hash terms the model has always charged. */
    uint64_t groups = 4;
    uint64_t lists = 16 + 1;   /* default max_subgroups + datagram list */
    uint64_t per_sub = sizeof(r_sub_t) + groups * gpos_stride_for(lists) +
                       gpos_hash_cap_for(groups) * sizeof(uint32_t);
    uint64_t d8 = describe_structure(&a, 16, 4);
    uint64_t d24 = describe_structure(&a, 24, 4);
    MOQ_TEST_CHECK(d8 != UINT64_MAX && d24 != UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(d24 - d8, 8 * per_sub);

    /* Descriptor delta per added track slot: pool term + the per-track log
     * terms (structure + retained-header ceilings) — subs_head must add
     * NOTHING beyond sizeof(r_track_t), which this closed form pins. */
    moqr_log_cfg_t lc;
    moqr_log_cfg_init_sized(&lc, sizeof(lc), &a.vt);
    lc.budget.max_groups = 4;
    lc.budget.max_bytes = 1 << 20;
    moqr_log_capacity_t logc;
    MOQ_TEST_CHECK(moqr_log_capacity_describe(&lc, &logc) == MOQR_OK);
    uint64_t per_track =
        sizeof(r_track_t) + logc.structure_bytes + logc.header_bytes;
    uint64_t t4 = describe_structure(&a, 16, 4);
    uint64_t t6 = describe_structure(&a, 16, 6);
    MOQ_TEST_CHECK_EQ_U64(t6 - t4, 2 * per_track);

    /* Create-time counting-allocator deltas: pools are the ONLY per-slot
     * allocation at create (gpos/logs are lazy), so the measured delta must
     * equal the sizeof terms EXACTLY — described and allocated can't drift. */
    long c16 = create_live_bytes(&a, 16, 4);
    long c24 = create_live_bytes(&a, 24, 4);
    long ct6 = create_live_bytes(&a, 16, 6);
    MOQ_TEST_CHECK(c16 > 0 && c24 > 0 && ct6 > 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(c24 - c16), 8 * sizeof(r_sub_t));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(ct6 - c16), 2 * sizeof(r_track_t));
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);

    MOQ_TEST_PASS("index_capacity_delta");
    return failures;
}

/* -- Generation exhaustion ---------------------------------------------------- */

/* Slot generations never wrap. In every relay pool, the last live generation
 * (R_GEN_LAST_LIVE) still mints a valid, nonzero handle; releasing it retires
 * the slot, so the next admission falls back to another slot and the old
 * handle stays stale; with no openable slot left the admission is refused,
 * never answered with a zero or aliased handle. */
static int
test_generation_exhaustion_pools(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 4, 2);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsp[1] = { { (const uint8_t *)"g", 1 } };
    moqr_ns_t ns = { nsp, 1 };
    moq_bytes_t name = { (const uint8_t *)"t", 1 };
    moqr_intent_t its[8];

    /* Bindings. */
    c->bindings[0].gen = R_GEN_LAST_LIVE - 1u;
    moqr_binding_t b0;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &b0) == MOQR_OK);
    MOQ_TEST_CHECK(b0._opaque != 0 && moqr_binding_is_valid(b0));
    MOQ_TEST_CHECK_EQ_U64(moq_handle_generation(b0._opaque), R_GEN_LAST_LIVE);
    MOQ_TEST_CHECK(moqr_core_binding_close(c, b0, 1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(c->bindings[0].gen, R_GEN_RETIRED);
    moqr_binding_t pb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(pb._opaque != 0 && pb._opaque != b0._opaque);
    MOQ_TEST_CHECK_EQ_U64(moq_handle_slot(pb._opaque), 1);
    MOQ_TEST_CHECK(moqr_core_binding_close(c, b0, 1) ==
                   MOQR_ERR_STALE_HANDLE);
    for (uint32_t i = 2; i < c->max_bindings; i++) {
        c->bindings[i].gen = R_GEN_RETIRED;
    }
    moqr_binding_t none = MOQR_BINDING_INVALID;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 3, &none) == MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(none._opaque == 0);

    /* Subscriptions, on a live track. */
    moqr_track_t tr;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, ns, name, 7, &tr) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    c->subs[0].gen = R_GEN_LAST_LIVE - 1u;
    for (uint32_t i = 1; i < c->max_subs; i++) {
        c->subs[i].gen = R_GEN_RETIRED;
    }
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = ns;
    rq.name = name;
    rq.cookie = 3;
    moqr_sub_t s0;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, pb, &rq, &s0) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK(s0._opaque != 0 && moqr_sub_is_valid(s0));
    MOQ_TEST_CHECK_EQ_U64(moq_handle_generation(s0._opaque), R_GEN_LAST_LIVE);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, s0, 3) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK_EQ_U64(c->subs[0].gen, R_GEN_RETIRED);
    moqr_sub_t s1 = MOQR_SUB_INVALID;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, pb, &rq, &s1) == MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(s1._opaque == 0);
    c->subs[2].gen = 6u;   /* another slot becomes openable */
    MOQ_TEST_CHECK(moqr_core_subscribe(c, pb, &rq, &s1) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK_EQ_U64(moq_handle_slot(s1._opaque), 2);
    MOQ_TEST_CHECK(s1._opaque != s0._opaque);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, s0, 4) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, s1, 5) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);

    /* Fetch cursors: the finder skips a retired slot. */
    c->fetches[0].gen = R_GEN_RETIRED;
    MOQ_TEST_CHECK_EQ_U64(fetch_slot_find(c), 1);
    c->fetches[0].gen = R_GEN_LAST_LIVE - 1u;
    MOQ_TEST_CHECK_EQ_U64(fetch_slot_find(c), 0);
    c->fetches[0].gen = 0;

    /* Parked requests. */
    moqr_park_req_t pr;
    memset(&pr, 0, sizeof(pr));
    pr.action = MOQR_AUTH_SUBSCRIBE;
    pr.binding_cookie = 2;
    pr.session_cookie = 9;
    pr.ns = ns;
    pr.name = name;
    c->parked[0].gen = R_GEN_LAST_LIVE - 1u;
    MOQ_TEST_CHECK(moqr_core_park(c, &pr, 41, 1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(c->parked[0].gen, R_GEN_LAST_LIVE);
    parked_free_slot(c, 0);
    MOQ_TEST_CHECK_EQ_U64(c->parked[0].gen, R_GEN_RETIRED);
    MOQ_TEST_CHECK(moqr_core_park(c, &pr, 42, 1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(c->parked[1].gen & 1u, 1);
    MOQ_TEST_CHECK_EQ_U64(c->parked[0].gen, R_GEN_RETIRED);
    parked_free_slot(c, 1);

    /* Revalidation grants: the reservation generation is caller-visible. */
    moqr_grant_res_t res;
    c->grants[0].gen = R_GEN_LAST_LIVE - 1u;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &pr, 1000, 1, &res) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(res.slot, 0);
    MOQ_TEST_CHECK_EQ_U64(res.gen, R_GEN_LAST_LIVE);
    grant_free_slot(c, 0);
    moqr_grant_res_t res2;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &pr, 1000, 1, &res2) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(res2.slot, 1);
    MOQ_TEST_CHECK_EQ_U64(c->grants[0].gen, R_GEN_RETIRED);
    grant_free_slot(c, 1);
    for (uint32_t i = 1; i < c->max_grants; i++) {
        c->grants[i].gen = R_GEN_RETIRED;
    }
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &pr, 1000, 1, &res2) ==
                   MOQR_ERR_CAPACITY);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("generation_exhaustion_pools");
    return failures;
}

static uint32_t
count_kind(const moqr_intent_t *its, size_t n, moqr_intent_kind_t kind)
{
    uint32_t k = 0;
    for (size_t i = 0; i < n; i++) {
        k += its[i].kind == kind ? 1u : 0u;
    }
    return k;
}

static uint64_t
records_of(const moqr_core_t *c, uint32_t slot)
{
    moqr_log_stats_t ls;
    moqr_log_get_stats(c->tracks[slot].log, &ls);
    return ls.record_count;
}

/* Tracks: creation at the last live generation; a re-PUBLISH of a retained
 * track whose slot cannot renew moves the new source to another slot, or is
 * refused with the retained track (cache, subscriber, handle) untouched; and
 * demand-driven reclaim never counts a track on its last generation as a
 * free slot, since releasing it retires the slot. */
static int
test_generation_exhaustion_tracks(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 4, 2);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsp[1] = { { (const uint8_t *)"g", 1 } };
    moqr_ns_t ns = { nsp, 1 };
    moq_bytes_t name = { (const uint8_t *)"t", 1 };
    moq_bytes_t name2 = { (const uint8_t *)"u", 1 };
    moqr_intent_t its[8];
    moqr_pd_desc_t done;
    MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_TRACK_ENDED, &done) == MOQR_OK);
    moqr_binding_t pb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);

    /* Last valid generation: created, valid, nonzero. */
    c->tracks[0].gen = R_GEN_LAST_LIVE - 1u;
    c->tracks[1].gen = R_GEN_RETIRED;
    moqr_track_t t1;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, ns, name, 7, &t1) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK(t1._opaque != 0);
    MOQ_TEST_CHECK_EQ_U64(moq_handle_generation(t1._opaque), R_GEN_LAST_LIVE);
    MOQ_TEST_CHECK(ing(c, &a, t1, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_source_done(c, t1, 1, done, 2) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK(c->tracks[0].state == R_TRACK_WARM);

    /* A subscriber on the retained track. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = ns;
    rq.name = name;
    rq.cookie = 3;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, pb, &rq, &sub) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK(moqr_sub_is_valid(sub));

    /* Re-PUBLISH with no other slot: refused, nothing changes. */
    uint64_t intern_before = c->intern_used;
    uint64_t source_gen = c->tracks[0].track_gen;
    moqr_track_t t2 = MOQR_TRACK_INVALID;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, ns, name, 8, &t2) ==
                   MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(t2._opaque == 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_poll_intents(c, its, 8), 0);
    MOQ_TEST_CHECK_EQ_U64(c->tracks[0].gen, R_GEN_LAST_LIVE);
    MOQ_TEST_CHECK(c->tracks[0].state == R_TRACK_WARM);
    MOQ_TEST_CHECK_EQ_U64(records_of(c, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(track_sub_count(c, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(c->intern_used, intern_before);
    MOQ_TEST_CHECK_EQ_U64(c->refusals[MOQR_REFUSE_TRACKS], 1);

    /* Another slot opens: the new source takes it; the old slot retires. */
    c->tracks[1].gen = 4u;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, ns, name, 9, &t2) == MOQR_OK);
    size_t n = moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK_EQ_U64(count_kind(its, n, MOQR_INTENT_SUB_DONE), 1);
    MOQ_TEST_CHECK_EQ_U64(count_kind(its, n, MOQR_INTENT_ACCEPT_PUBLISH), 1);
    MOQ_TEST_CHECK(t2._opaque != 0 && t2._opaque != t1._opaque);
    MOQ_TEST_CHECK_EQ_U64(moq_handle_slot(t2._opaque), 1);
    MOQ_TEST_CHECK_EQ_U64(moq_handle_generation(t2._opaque), 5u);
    MOQ_TEST_CHECK_EQ_U64(c->tracks[0].gen, R_GEN_RETIRED);
    MOQ_TEST_CHECK(c->tracks[1].state == R_TRACK_ACTIVE);
    MOQ_TEST_CHECK_EQ_U64(c->tracks[1].track_gen, source_gen + 1u);
    MOQ_TEST_CHECK_EQ_U64(c->intern_used, intern_before);
    uint64_t h = 0;
    MOQ_TEST_CHECK_EQ_U64(track_lookup(c, ns, name, &h), 1);
    MOQ_TEST_CHECK(ing(c, &a, t1, 1, 0) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 4) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(ing(c, &a, t2, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(records_of(c, 1), 1);
    bool seen[4];
    CHECK_INDEX(c, seen);
    MOQ_TEST_CHECK(moqr_core_source_done(c, t2, c->tracks[1].track_gen, done, 3) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);

    /* Reclaim: the least recently used reclaimable track is on its last
     * generation, so the slot comes from the next one; when every
     * reclaimable track is on its last generation the admission is refused
     * and both retained tracks survive. */
    ca_init(&a);
    c = mkcore(&a, 4, 2);
    MOQ_TEST_CHECK(c != NULL);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    c->tracks[0].gen = R_GEN_LAST_LIVE - 1u;
    moqr_track_t ta, tb;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, ns, name, 7, &ta) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, ns, name2, 8, &tb) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK_EQ_U64(moq_handle_slot(ta._opaque), 0);
    MOQ_TEST_CHECK_EQ_U64(moq_handle_slot(tb._opaque), 1);
    MOQ_TEST_CHECK(ing(c, &a, ta, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, tb, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_source_done(c, ta, 1, done, 2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_source_done(c, tb, 1, done, 3) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK(track_reclaimable(c, 0) && track_reclaimable(c, 1));
    MOQ_TEST_CHECK(c->tracks[0].interest_seq < c->tracks[1].interest_seq);

    uint32_t saved = c->tracks[1].gen;
    c->tracks[1].gen = R_GEN_LAST_LIVE;   /* both on their last generation */
    moq_bytes_t name3 = { (const uint8_t *)"v", 1 };
    moqr_track_t tc = MOQR_TRACK_INVALID;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, ns, name3, 9, &tc) ==
                   MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(tc._opaque == 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_poll_intents(c, its, 8), 0);
    MOQ_TEST_CHECK(track_reclaimable(c, 0) && track_reclaimable(c, 1));
    MOQ_TEST_CHECK_EQ_U64(c->tracks_evicted_capacity, 0);
    c->tracks[1].gen = saved;

    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, ns, name3, 9, &tc) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);
    MOQ_TEST_CHECK_EQ_U64(moq_handle_slot(tc._opaque), 1);
    MOQ_TEST_CHECK(tc._opaque != tb._opaque);
    MOQ_TEST_CHECK_EQ_U64(c->tracks_evicted_capacity, 1);
    MOQ_TEST_CHECK_EQ_U64(c->tracks[0].gen, R_GEN_LAST_LIVE);
    MOQ_TEST_CHECK_EQ_U64(records_of(c, 0), 1);
    MOQ_TEST_CHECK(ing(c, &a, tb, 1, 0) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_source_done(c, tc, 1, done, 4) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 8);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("generation_exhaustion_tracks");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_index_lifecycle();
    failures += test_index_random_churn();
    failures += test_index_capacity_delta();
    failures += test_generation_exhaustion_pools();
    failures += test_generation_exhaustion_tracks();
    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("ALL PASS\n");
    return 0;
}
