/*
 * Pure-core tests for moqr_core_force_withdraw: split-brain loser enforcement.
 * Purge by stored source-announce identity (per-state terminals, fetch
 * invalidation, slot free), announce clear + NS_GONE, publisher cancel + live
 * grant retirement, and an atomic WOULD_BLOCK preflight. Allocator balance zero
 * ends every test.
 */

#include <moq/relay/relay.h>
#include <moq/relay/trace.h>

#include <moq/rcbuf.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

#define FW_NOW  1000u
#define FW_CODE 0x20u  /* REQUEST_ERROR UNINTERESTED: parked rejects + cancel */
#define FW_DONE 0x2u   /* PUBLISH_DONE TRACK_ENDED: active-sub terminals */

/* counting allocator (same shape as the other relay tests) */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
} ca_t;
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
NS1(moq_bytes_t *storage, const char *a)
{
    storage[0] = B(a);
    return (moqr_ns_t){ .parts = storage, .count = 1 };
}

static moqr_ns_t
NS2(moq_bytes_t *storage, const char *a, const char *b)
{
    storage[0] = B(a);
    storage[1] = B(b);
    return (moqr_ns_t){ .parts = storage, .count = 2 };
}

static moqr_core_t *
mkcore(ca_t *a, moqr_trace_t *trace)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.trace = trace;
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
ing(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t sg, uint64_t o,
    uint8_t prio)
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

/* Subscribe on `sb` for (ns, name); return the emitted UPSTREAM_SUBSCRIBE's track
 * + gen (0 gen if the subscribe fast-path-accepted from an existing track). */
static void
sub_upstream(moqr_core_t *c, moqr_binding_t sb, moqr_ns_t ns, const char *name,
             uint64_t cookie, moqr_track_t *track, uint64_t *tgen)
{
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = ns;
    rq.name = B(name);
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = cookie;
    moqr_sub_t sub;
    (void)moqr_core_subscribe(c, sb, &rq, &sub);
    *track = (moqr_track_t){ 0 };
    *tgen = 0;
    moqr_intent_t its[16];
    size_t n = drain(c, its, 16);
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            *track = its[i].track;
            *tgen = its[i].track_gen;
        }
    }
}

static bool
drain_has(moqr_core_t *c, uint32_t kind, uint64_t error_code)
{
    moqr_intent_t its[32];
    size_t n = drain(c, its, 32);
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == kind && its[i].error_code == error_code) {
            return true;
        }
    }
    return false;
}

/* -- ACTIVE loser purged: SUB_DONE, slot freed + reusable, publisher cancel. -- */
static int
test_fw_active_purged(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "loser")) == MOQR_OK);

    moqr_track_t track;
    uint64_t tgen;
    sub_upstream(c, sub, NS1(nsb, "loser"), "v", 11, &track, &tgen);
    MOQ_TEST_CHECK(tgen != 0);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen, 777, true, 4, 9) ==
                   MOQR_OK);
    moqr_intent_t its[16];
    (void)drain(c, its, 16);   /* consume ACCEPT_SUB */

    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    /* The ACTIVE sub is terminated with PUBLISH_DONE TRACK_ENDED — never the
     * REQUEST_ERROR-domain withdrawal code. */
    MOQ_TEST_CHECK(drain_has(c, MOQR_INTENT_SUB_DONE, FW_DONE));
    /* The publisher is queued for a wire cancel. */
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(rv[0].binding_cookie, 100);
    MOQ_TEST_CHECK_EQ_U64(rv[0].error_code, FW_CODE);

    /* Slot freed: re-announce + re-subscribe yields a FRESH upstream attempt
     * (a kept ACTIVE track would fast-path-accept with no new upstream). */
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "loser")) == MOQR_OK);
    moqr_track_t track2;
    uint64_t tgen2;
    sub_upstream(c, sub, NS1(nsb, "loser"), "v", 12, &track2, &tgen2);
    MOQ_TEST_CHECK(tgen2 != 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_active_purged");
    return failures;
}

/* -- PENDING loser purged with REJECT_SUB (not SUB_DONE). -- */
static int
test_fw_pending_purged(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "loser")) == MOQR_OK);

    moqr_track_t track;
    uint64_t tgen;
    sub_upstream(c, sub, NS1(nsb, "loser"), "v", 11, &track, &tgen);
    MOQ_TEST_CHECK(tgen != 0);   /* PENDING, upstream not resolved */

    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    /* A parked sub is refused with REJECT_SUB, NEVER SUB_DONE (WRONG_STATE). */
    moqr_intent_t its[16];
    size_t n = drain(c, its, 16);
    bool saw_reject = false, saw_done = false;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_REJECT_SUB && its[i].error_code == FW_CODE) {
            saw_reject = true;
        }
        if (its[i].kind == MOQR_INTENT_SUB_DONE) {
            saw_done = true;
        }
    }
    MOQ_TEST_CHECK(saw_reject);
    MOQ_TEST_CHECK(!saw_done);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_pending_purged");
    return failures;
}

/* -- Cancel carries the real announce handle; ack clears it. -- */
static int
test_fw_cancel_real_handle(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    moq_bytes_t nsb[1];
    /* Announce via _ex with a non-zero wire handle so the cancel can target it. */
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, pub, NS1(nsb, "loser"),
                                         0xABCDEFu) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(rv[0].binding_cookie, 100);
    MOQ_TEST_CHECK_EQ_U64(rv[0].session_cookie, 0xABCDEFu);   /* real handle */
    MOQ_TEST_CHECK_EQ_U64(rv[0].error_code, FW_CODE);
    /* Non-draining until ack. */
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);
    moqr_core_ack_revoked_grant(c, 100, 0xABCDEFu);
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_cancel_real_handle");
    return failures;
}

/* -- No-op on missing / mirror-owned / already-cleared announce. -- */
static int
test_fw_noop(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moq_bytes_t nsb[1];

    /* (a) never announced → OK, no cancel. */
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "ghost"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)0);

    /* (b) mirror-owned (owner cookie >= MOQR_SHARD_COOKIE_BASE) → no-op, the
     * announce survives. */
    moqr_binding_t mirror;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, MOQR_SHARD_COOKIE_BASE + 3u,
                                          &mirror) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, mirror, NS1(nsb, "mir")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "mir"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)0);
    /* announce intact: unannounce by the owner still succeeds. */
    MOQ_TEST_CHECK(moqr_core_unannounce(c, mirror, NS1(nsb, "mir")) == MOQR_OK);

    /* (c) already-cleared: a second force-withdraw is a clean no-op. */
    moqr_binding_t pub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "real")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "real"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "real"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    /* exactly one cancel from the first withdraw (second is a no-op). */
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_noop");
    return failures;
}

static moqr_core_t *
mkcore_small(ca_t *a, uint32_t max_cancels, uint32_t max_grants)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 1000;
    if (max_cancels != 0) {
        cfg.max_cancels = max_cancels;
    }
    if (max_grants != 0) {
        cfg.max_grants = max_grants;
    }
    moqr_core_t *c = NULL;
    if (moqr_core_create(&cfg, &c) != MOQR_OK) {
        return NULL;
    }
    return c;
}

/* -- WARM retained-log loser purged: no retained fast-path after withdraw. -- */
static int
test_fw_warm_purged(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "loser")) == MOQR_OK);

    /* ACTIVE with a retained record, then drop the sub + linger → WARM. */
    moqr_track_t track;
    uint64_t tgen;
    sub_upstream(c, sub, NS1(nsb, "loser"), "v", 11, &track, &tgen);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen, 777, true, 1, 0) ==
                   MOQR_OK);
    moqr_intent_t its[16];
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 128) == MOQR_OK);
    /* Drop the only subscriber and linger past the deadline: the track keeps its
     * retained log and transitions to WARM. */
    MOQ_TEST_CHECK(moqr_core_binding_close(c, sub, FW_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_tick(c, FW_NOW + 2000u) == MOQR_OK);
    (void)drain(c, its, 16);   /* drain UPSTREAM_UNSUBSCRIBE; track now WARM */

    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    /* Retained log gone: re-announce + re-subscribe gets NO retained ACCEPT_SUB
     * (a surviving WARM track would fast-path-accept from its log). */
    moqr_binding_t sub2;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 300, &sub2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "loser")) == MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS1(nsb, "loser");
    rq.name = B("v");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 22;
    moqr_sub_t s2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sub2, &rq, &s2) == MOQR_OK);
    size_t n = drain(c, its, 16);
    bool saw_accept = false, saw_upstream = false;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_ACCEPT_SUB) {
            saw_accept = true;
        }
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            saw_upstream = true;
        }
    }
    MOQ_TEST_CHECK(!saw_accept);    /* no retained log to serve */
    MOQ_TEST_CHECK(saw_upstream);   /* a fresh PENDING track */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_warm_purged");
    return failures;
}

/* -- Source-identity churn: a parent-sourced child track is purged when the
 *    PARENT is withdrawn, even after a more-specific child announce appears; a
 *    sibling announce is not over-purged. -- */
static int
test_fw_source_churn(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t p1[1];
    moq_bytes_t p2[2];

    /* Parent "app" announced; a subscribe under "app"/"cam" is sourced from the
     * PARENT node (longest announce at subscribe time). */
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(p1, "app")) == MOQR_OK);
    moqr_track_t child;
    uint64_t cgen;
    sub_upstream(c, sub, NS2(p2, "app", "cam"), "v", 11, &child, &cgen);
    MOQ_TEST_CHECK(cgen != 0);

    /* A more-specific child announce appears AFTER the track was sourced. */
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(p2, "app", "cam")) == MOQR_OK);

    /* Force-withdraw the PARENT: the parent-sourced child track is still purged
     * (stored source == parent node), even though longest-prefix now resolves to
     * the child node. */
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(p1, "app"), FW_CODE, FW_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(drain_has(c, MOQR_INTENT_REJECT_SUB, FW_CODE));

    /* The sibling child announce survives (only the parent was withdrawn): a
     * subscribe under it still resolves to an upstream. */
    moqr_track_t c2;
    uint64_t c2gen;
    sub_upstream(c, sub, NS2(p2, "app", "cam"), "w", 12, &c2, &c2gen);
    MOQ_TEST_CHECK(c2gen != 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_source_churn");
    return failures;
}

/* -- Cancel dedupe: a second withdraw for a still-pending {owner, handle} does
 *    not queue a duplicate cancel. -- */
static int
test_fw_cancel_dedupe(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    moq_bytes_t nsb[1];

    MOQ_TEST_CHECK(moqr_core_announce_ex(c, pub, NS1(nsb, "loser"), 0x55u) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    /* Re-announce with the SAME wire handle, then withdraw again: the cancel for
     * {100, 0x55} is already pending → no duplicate. */
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, pub, NS1(nsb, "loser"), 0x55u) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    moqr_revoked_grant_t rv[8];
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 8), (size_t)1);
    moqr_core_ack_revoked_grant(c, 100, 0x55u);
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 8), (size_t)0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_cancel_dedupe");
    return failures;
}

/* -- Cancel queue full → WOULD_BLOCK on the FINAL unit; the tracks are already
 *    purged, the announce still stands, and the retry after the ack completes
 *    the namespace effects. -- */
static int
test_fw_cancel_queue_full(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_small(&a, /*max_cancels=*/1, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t p1, p2, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &p1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 101, &p2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t n1[1], n2[1];

    MOQ_TEST_CHECK(moqr_core_announce_ex(c, p1, NS1(n1, "one"), 0x1u) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, p2, NS1(n2, "two"), 0x2u) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(n1, "one"), FW_CODE, FW_NOW) ==
                   MOQR_OK);
    /* Queue is full (1 slot). The second withdraw WOULD_BLOCKs with zero
     * mutation: its announce is untouched. */
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(n2, "two"), FW_CODE, FW_NOW) ==
                   MOQR_ERR_WOULD_BLOCK);
    moqr_track_t tk;
    uint64_t tg;
    sub_upstream(c, sub, NS1(n2, "two"), "v", 11, &tk, &tg);
    MOQ_TEST_CHECK(tg != 0);   /* "two" still announced → upstream fired */

    /* Ack the first cancel; the slot frees and the retry completes. */
    moqr_core_ack_revoked_grant(c, 100, 0x1u);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(n2, "two"), FW_CODE, FW_NOW) ==
                   MOQR_OK);
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(rv[0].session_cookie, 0x2u);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_cancel_queue_full");
    return failures;
}

/* -- Live announce grant retired: force-withdraw frees the matching grant so it
 *    stops revalidating and the slot is reusable. -- */
static int
test_fw_grant_retired(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_small(&a, 0, /*max_grants=*/1);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    moq_bytes_t nsb[1];

    /* A committed PUBLISH_NAMESPACE grant + the announce it authorizes. */
    moqr_park_req_t gr;
    memset(&gr, 0, sizeof(gr));
    gr.action = MOQR_AUTH_PUBLISH_NAMESPACE;
    gr.binding_cookie = 100;
    gr.session_cookie = 0x99u;
    gr.ns = NS1(nsb, "loser");
    moqr_grant_res_t res;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr, 1000, 1, &res) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, pub, NS1(nsb, "loser"), 0x99u) ==
                   MOQR_OK);

    /* max_grants == 1: a fresh reserve fails until the grant is retired. */
    moqr_park_req_t gr2 = gr;
    gr2.session_cookie = 0xAAu;
    moqr_grant_res_t res2;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr2, 1000, 1, &res2) !=
                   MOQR_OK);

    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    /* One cancel queued; the grant is retired so the slot is reusable. */
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr2, 1000, 1, &res2) == MOQR_OK);
    moqr_core_grant_abort(c, res2);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_grant_retired");
    return failures;
}

/* -- Held fetch invalidated: next peek is STALE_HANDLE, no replayed object. -- */
static int
test_fw_held_fetch(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "demo")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS1(nsb, "demo"), B("v"), 5,
                                          &track) == MOQR_OK);
    moqr_intent_t its[8];
    (void)drain(c, its, 8);   /* ACCEPT_PUBLISH */
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 0, 0, 128) == MOQR_OK);   /* retain (5,0) */

    moqr_fetch_req_t r;
    moqr_fetch_req_init(&r);
    r.ns = NS1(nsb, "demo");
    r.name = B("v");
    r.start_group = 5;
    r.start_object = 0;
    r.end_group = 5;
    r.end_object = 0;   /* whole group 5 */
    r.cookie = 7;
    moqr_fetch_t f;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FW_NOW, &f, &plan) == MOQR_OK);
    MOQ_TEST_CHECK(plan.admit == MOQR_FETCH_ACCEPT);
    moqr_fetch_item_t it;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, f, FW_NOW, &it) == MOQR_OK);  /* pins */
    MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);

    /* Purge the track: the held fetch is unpinned + its handle invalidated. */
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "demo"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    /* Next peek resolves the dead handle → STALE_HANDLE (never the held loser
     * object, never TOO_OLD). */
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, f, FW_NOW, &it) ==
                   MOQR_ERR_STALE_HANDLE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_held_fetch");
    return failures;
}

/* -- The withdrawal's request-error code is stored, not narrowed. --
 *
 * A REQUEST_ERROR is a varint. The code a force-withdrawal carries reaches two
 * durable places — every parked subscriber's REJECT_SUB and the publisher-side
 * revoked grant — and both must hold all 64 bits. FW_WIDE's low half is 0x7,
 * unassigned in both registries, so a narrowed carrier yields a legal-looking
 * code instead of an obvious zero. -- */
#define FW_WIDE UINT64_C(0x100000007)

static int
test_fw_request_error_full_width(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 1000;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, pub, NS1(nsb, "wide"), 888) ==
                   MOQR_OK);
    moqr_track_t tv;
    uint64_t tvg;
    sub_upstream(c, sub, NS1(nsb, "wide"), "v", 1, &tv, &tvg);
    MOQ_TEST_CHECK(tvg != 0);

    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "wide"), FW_WIDE,
                                            FW_NOW) == MOQR_OK);

    /* the parked subscriber's REJECT_SUB */
    moqr_intent_t its[16];
    size_t n = drain(c, its, 16);
    int rejects = 0;
    uint64_t seen = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_REJECT_SUB) {
            rejects++;
            seen = its[i].error_code;
        }
    }
    printf("ORACLE fw_width reject=0x%llx\n", (unsigned long long)seen);
    MOQ_TEST_CHECK_EQ_INT(rejects, 1);
    MOQ_TEST_CHECK_EQ_U64(seen, FW_WIDE);

    /* the publisher-side revoked grant */
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);
    printf("ORACLE fw_width grant=0x%llx\n",
           (unsigned long long)rv[0].error_code);
    MOQ_TEST_CHECK_EQ_U64(rv[0].error_code, FW_WIDE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_request_error_full_width");
    return failures;
}

/* -- Partial progress is real, observable, and durable. --
 *
 * The purge runs one track per reserved unit. With room for exactly one unit,
 * the first call returns WOULD_BLOCK having ALREADY freed one track: those
 * terminals are on the ring and will not be produced again. What must NOT have
 * happened is any FINAL namespace effect — no NS_GONE, no publisher cancel, no
 * cleared announce — because the namespace may not stop existing while a track
 * it sourced still stands. The caller drains and retries to completion. -- */
static int
test_fw_partial_progress_is_durable(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 1000;
    cfg.max_subs = 2;
    cfg.max_tracks = 2;
    cfg.max_ns_nodes = 2;
    cfg.max_ns_subs = 1;
    cfg.max_intents = 2;   /* clamped up to max_subs + 1 == 3 */
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "loser")) == MOQR_OK);

    /* Two sourced tracks, each one parked subscriber plus one upstream: two
     * intents per unit. Both upstream subscribes are drained, so the ring is
     * empty when the withdraw starts. */
    moqr_track_t tv, tw;
    uint64_t tvg, twg;
    sub_upstream(c, sub, NS1(nsb, "loser"), "v", 1, &tv, &tvg);
    sub_upstream(c, sub, NS1(nsb, "loser"), "w", 2, &tw, &twg);
    MOQ_TEST_CHECK(tvg != 0);
    MOQ_TEST_CHECK(twg != 0);

    /* A watcher, so the FINAL unit has an NS_GONE to fan and its absence
     * after the partial return is meaningful. */
    moq_bytes_t wpfx[1];
    wpfx[0] = B("loser");
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(
                       c, sub, (moqr_ns_t){ .parts = wpfx, .count = 1 },
                       900) == MOQR_OK);
    moqr_intent_t its[8];
    (void)drain(c, its, 8);   /* the watcher's NS_FOUND; ring empty again */

    /* Room for one unit (2) but not two: partial return. */
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_ERR_WOULD_BLOCK);

    /* EXACTLY one track's worth of work was done, and it is durable. */
    size_t n = drain(c, its, 8);
    int rejects = 0, releases = 0, gones = 0, dones = 0;
    for (size_t i = 0; i < n; i++) {
        switch (its[i].kind) {
        case MOQR_INTENT_REJECT_SUB:            rejects++;  break;
        case MOQR_INTENT_UPSTREAM_UNSUBSCRIBE:  releases++; break;
        case MOQR_INTENT_NS_GONE:               gones++;    break;
        case MOQR_INTENT_SUB_DONE:              dones++;    break;
        default: break;
        }
    }
    printf("ORACLE fw_partial first{rejects=%d releases=%d gones=%d dones=%d}\n",
           rejects, releases, gones, dones);
    MOQ_TEST_CHECK_EQ_INT(rejects, 1);
    MOQ_TEST_CHECK_EQ_INT(releases, 1);
    /* No FINAL namespace effect on a partial return. */
    MOQ_TEST_CHECK_EQ_INT(gones, 0);
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)0);
    /* The announce is still installed: a second withdraw still has work, and
     * would be the no-op MOQR_OK of a missing node otherwise. */
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);

    /* The retry finished the remainder — the SECOND track only, never the
     * first again — and performed the final effects exactly once. */
    n = drain(c, its, 8);
    rejects = releases = gones = dones = 0;
    for (size_t i = 0; i < n; i++) {
        switch (its[i].kind) {
        case MOQR_INTENT_REJECT_SUB:            rejects++;  break;
        case MOQR_INTENT_UPSTREAM_UNSUBSCRIBE:  releases++; break;
        case MOQR_INTENT_NS_GONE:               gones++;    break;
        case MOQR_INTENT_SUB_DONE:              dones++;    break;
        default: break;
        }
    }
    printf("ORACLE fw_partial retry{rejects=%d releases=%d gones=%d dones=%d}\n",
           rejects, releases, gones, dones);
    MOQ_TEST_CHECK_EQ_INT(rejects, 1);      /* not 2: no restart */
    MOQ_TEST_CHECK_EQ_INT(releases, 1);
    MOQ_TEST_CHECK_EQ_INT(gones, 1);
    MOQ_TEST_CHECK_EQ_INT(dones, 0);
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(rv[0].error_code, FW_CODE);

    /* Idempotent once complete: the node is gone, so nothing more is owed. */
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)0);
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)1);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_partial_progress_is_durable");
    return failures;
}

/* -- First-unit preflight: with too little room for even the FIRST track unit,
 *    the call returns WOULD_BLOCK before mutating anything. This is the one
 *    narrow atomicity guarantee force-withdraw makes — see
 *    test_fw_partial_progress_is_durable for what happens once a unit fits. -- */
static int
test_fw_first_unit_preflight(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 1000;
    cfg.max_subs = 2;
    cfg.max_tracks = 2;
    cfg.max_ns_nodes = 2;
    cfg.max_ns_subs = 1;
    cfg.max_intents = 2;   /* the atomicity clamp raises this to 3
                            * (max_subs + 1: one track's terminals + its
                            * upstream release) */
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "loser")) == MOQR_OK);

    /* Two parked subs, one each on tracks "v" and "w" under the loser (=> 2
     * REJECT_SUB at withdraw). "v"'s UPSTREAM is drained; "w"'s is left UNDRAINED,
     * so the ring already holds one intent when force-withdraw runs. */
    moqr_track_t tv;
    uint64_t tvg;
    sub_upstream(c, sub, NS1(nsb, "loser"), "v", 1, &tv, &tvg);   /* drains */
    MOQ_TEST_CHECK(tvg != 0);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS1(nsb, "loser");
    rq.name = B("w");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 2;
    moqr_sub_t sw;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sub, &rq, &sw) == MOQR_OK);
    /* leave "w"'s UPSTREAM_SUBSCRIBE in the ring */

    /* A namespace watcher on the loser prefix: its NS_FOUND occupies a
     * second ring slot (beside the undrained UPSTREAM). Ring is clamped
     * to 3; the first per-track unit (1 terminal + 1 release = 2) needs
     * more than the 1 free slot, so nothing is mutated at all. */
    moq_bytes_t wpfx[1];
    wpfx[0] = B("loser");
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(
                       c, sub, (moqr_ns_t){ .parts = wpfx, .count = 1 },
                       900) == MOQR_OK);
    moqr_intent_t its[8];
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_ERR_WOULD_BLOCK);
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)2);   /* the UPSTREAM + the NS_FOUND */

    /* Chunked completion under drain-retry: every parked sub refused,
     * every upstream released, the one watcher told — exactly once. */
    int rejects = 0, releases = 0, gones = 0;
    moqr_result_t frc;
    int spins = 0;
    do {
        frc = moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                       FW_NOW);
        n = drain(c, its, 8);
        for (size_t i = 0; i < n; i++) {
            if (its[i].kind == MOQR_INTENT_REJECT_SUB &&
                its[i].error_code == FW_CODE) {
                rejects++;
            }
            if (its[i].kind == MOQR_INTENT_UPSTREAM_UNSUBSCRIBE) {
                releases++;
            }
            if (its[i].kind == MOQR_INTENT_NS_GONE) {
                gones++;
            }
        }
    } while (frc == MOQR_ERR_WOULD_BLOCK && ++spins < 8);
    MOQ_TEST_CHECK(frc == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT(rejects, 2);
    MOQ_TEST_CHECK_EQ_INT(releases, 2);
    MOQ_TEST_CHECK_EQ_INT(gones, 1);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_first_unit_preflight");
    return failures;
}

/* -- Held delivery: force-withdraw retires the sub through sub_retire, clearing
 *    the binding's outstanding-delivery pin before the slot frees (no dangling
 *    pin, no leaked payload). -- */
static int
test_fw_held_delivery(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sub) == MOQR_OK);
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS1(nsb, "loser")) == MOQR_OK);
    moqr_track_t track;
    uint64_t tgen;
    sub_upstream(c, sub, NS1(nsb, "loser"), "v", 11, &track, &tgen);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen, 777, true, 5, 0) ==
                   MOQR_OK);
    moqr_intent_t its[8];
    (void)drain(c, its, 8);   /* ACCEPT_SUB */
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 0, 0, 128) == MOQR_OK);

    /* Peek a delivery and HOLD it (no delivery_done): the binding pins the sub. */
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, FW_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(d.rec.payload != NULL);

    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "loser"), FW_CODE,
                                            FW_NOW) == MOQR_OK);
    /* The pinned sub is retired: nothing left to deliver, no dangling pin. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, FW_NOW, &d) == MOQR_DONE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("fw_held_delivery");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_fw_active_purged();
    failures += test_fw_pending_purged();
    failures += test_fw_cancel_real_handle();
    failures += test_fw_noop();
    failures += test_fw_warm_purged();
    failures += test_fw_source_churn();
    failures += test_fw_cancel_dedupe();
    failures += test_fw_cancel_queue_full();
    failures += test_fw_grant_retired();
    failures += test_fw_held_fetch();
    failures += test_fw_held_delivery();
    failures += test_fw_request_error_full_width();
    failures += test_fw_partial_progress_is_durable();
    failures += test_fw_first_unit_preflight();
    if (failures == 0) {
        printf("ALL FORCEWITHDRAW TESTS PASSED\n");
    }
    return failures == 0 ? 0 : 1;
}
