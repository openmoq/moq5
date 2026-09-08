/*
 * Core retained-hit FETCH tests (relay.h fetch API), driven directly against
 * moqr_core_fetch_open/peek/commit/close — no binding, no session. Covers:
 * exact retained replay, multi-subgroup k-way merge, NORMAL-only status skip,
 * before-tail vs tail end_of_track, the fetch terminals (unknown track,
 * INVALID_RANGE, whole-group End), max_fetches exhaustion, the fetch_pin_bytes
 * budget (multi-fetch backpressure + the no-infinite-wait floor), the idempotent
 * re-peek (WOULD_BLOCK hold invariant), and close/binding-close pin release.
 * Allocator balance zero ends every test.
 */

#include <moq/relay/relay.h>

#include <moq/rcbuf.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

#define FNOW 1000u

/* counting allocator (same shape as the other relay tests) */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
    long attempts;   /* every alloc/realloc attempt (success or injected fail) */
    long fail_at;    /* fail EXACTLY the fail_at-th attempt; 0 = never. Test-
                      * local OOM injection; a single failure lets later cleanup
                      * allocations still succeed.                              */
} ca_t;
/* An upstream terminal arrives in some connection's draft; these rigs speak
 * draft-16 unless a case says otherwise. */
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
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;   /* injected OOM at this attempt index */
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
        return NULL;   /* realloc failure: caller keeps the old buffer p */
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

/* Core with tunable per-track byte budget + fetch pin knobs (0 = default). */
static moqr_core_t *
mkcore(ca_t *a, uint32_t max_bytes, uint32_t fetch_pin_bytes, uint32_t max_fetches)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.log_budget.max_groups = 8;
    cfg.log_budget.max_bytes = max_bytes != 0 ? max_bytes : (1u << 20);
    cfg.linger_us = 1000;
    cfg.fetch_pin_bytes = fetch_pin_bytes;
    cfg.max_fetches = max_fetches;
    moqr_core_t *c = NULL;
    if (moqr_core_create(&cfg, &c) != MOQR_OK) {
        return NULL;
    }
    return c;
}

/* Announce + publish-open a track "demo/<name>" as ACTIVE; returns the track. */
static moqr_result_t
mktrack(moqr_core_t *c, moqr_binding_t pub, const char *name, moqr_track_t *out)
{
    moq_bytes_t nsb[1];
    moqr_result_t rc = moqr_core_announce(c, pub, NS1(nsb, "demo"));
    if (rc != MOQR_OK) {
        return rc;
    }
    rc = moqr_core_publish_open(c, pub, NS1(nsb, "demo"), B(name), 5, out);
    if (rc != MOQR_OK) {
        return rc;
    }
    moqr_intent_t its[4];
    (void)moqr_core_poll_intents(c, its, 4);   /* ACCEPT_PUBLISH */
    return MOQR_OK;
}

/* Ingest a NORMAL object; payload = `len` bytes of fingerprint fp. */
static moqr_result_t
ingp(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t sg,
     uint64_t o, uint8_t fp, size_t len)
{
    uint8_t *buf = malloc(len);
    memset(buf, fp, len);
    moq_rcbuf_t *pl = NULL;
    int rc = moq_rcbuf_create(&a->vt, buf, len, &pl);
    free(buf);
    if (rc != 0) {
        return MOQR_ERR_NOMEM;
    }
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 128;
    d.payload = pl;
    d.now_us = 1;
    moqr_result_t r = moqr_core_ingest(c, t, &d);
    if (r != MOQR_OK) {
        moq_rcbuf_decref(pl);
    }
    return r;
}

/* Ingest a NORMAL object with a default 8-byte payload, fp == g*16+o. */
static moqr_result_t
ing(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t sg, uint64_t o)
{
    return ingp(c, a, t, g, sg, o, (uint8_t)(g * 16 + o), 8);
}

/* Ingest a status object (no payload) — e.g. END_OF_GROUP / END_OF_TRACK. */
static moqr_result_t
ing_status(moqr_core_t *c, moqr_track_t t, uint64_t g, uint64_t sg, uint64_t o,
           moqr_obj_status_t status)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 128;
    d.status = status;
    d.payload = NULL;
    d.now_us = 1;
    return moqr_core_ingest(c, t, &d);
}

static void
fetch_req(moqr_fetch_req_t *r, const char *name, moq_bytes_t *nsb, uint64_t sg,
          uint64_t so, uint64_t eg, uint64_t eo)
{
    moqr_fetch_req_init(r);
    r->ns = NS1(nsb, "demo");
    r->name = B(name);
    r->start_group = sg;
    r->start_object = so;
    r->end_group = eg;
    r->end_object = eo;
    r->cookie = 7;
}

/* Drain an accepted fetch: record (group, object, fp) per OBJECT item and
 * return the count; asserts each item is a NORMAL object with a payload. */
typedef struct { uint64_t g, o; uint8_t fp; } fitem_t;
static int
collect(moqr_core_t *c, moqr_fetch_t fh, fitem_t *out, int cap, int *pf)
{
    int failures = 0;
    int n = 0;
    for (;;) {
        moqr_fetch_item_t it;
        moqr_result_t rc = moqr_core_fetch_peek(c, fh, FNOW, &it);
        MOQ_TEST_CHECK(rc == MOQR_OK);
        if (rc != MOQR_OK) {
            break;
        }
        if (it.kind == MOQR_FETCH_ITEM_END) {
            break;
        }
        MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);
        MOQ_TEST_CHECK(it.rec.status == MOQR_OBJ_NORMAL);
        MOQ_TEST_CHECK(it.rec.payload != NULL);
        if (n < cap) {
            out[n].g = it.rec.group_id;
            out[n].o = it.rec.object_id;
            out[n].fp = it.rec.payload != NULL
                            ? ((const uint8_t *)moq_rcbuf_data(it.rec.payload))[0]
                            : 0;
        }
        n++;
        MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);
    }
    *pf += failures;
    return n;
}

/* -- T1: exact retained replay over one subgroup -------------------------- */
static int
test_exact_replay(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    for (uint64_t o = 0; o < 3; o++) {
        MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, o) == MOQR_OK);
    }

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 3);   /* [0:0 .. 0:3) */
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_ACCEPT);
    MOQ_TEST_CHECK_EQ_U64(plan.end_group, 0);
    MOQ_TEST_CHECK_EQ_U64(plan.end_object, 3);
    MOQ_TEST_CHECK(!plan.end_of_track);
    MOQ_TEST_CHECK(moqr_fetch_is_valid(fh));

    fitem_t got[8];
    int n = collect(c, fh, got, 8, &failures);
    MOQ_TEST_CHECK_EQ_INT(n, 3);
    for (int i = 0; i < 3 && i < n; i++) {
        MOQ_TEST_CHECK_EQ_U64(got[i].g, 0);
        MOQ_TEST_CHECK_EQ_U64(got[i].o, (uint64_t)i);
        MOQ_TEST_CHECK_EQ_INT(got[i].fp, (uint8_t)(0 * 16 + i));
    }
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("exact_replay");
    return failures;
}

/* -- T3: multi-subgroup merge by object_id -------------------------------- */
static int
test_merge_order(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    /* group 5: sg0 = {0,2,4}, sg1 = {1,3,5} — interleaved object ids. */
    MOQ_TEST_CHECK(ing(c, &a, t, 5, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 5, 0, 2) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 5, 0, 4) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 5, 1, 1) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 5, 1, 3) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 5, 1, 5) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 5, 0, 5, 0);   /* whole group 5 (end_object 0) */
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_ACCEPT);

    fitem_t got[8];
    int n = collect(c, fh, got, 8, &failures);
    MOQ_TEST_CHECK_EQ_INT(n, 6);
    for (int i = 0; i < 6 && i < n; i++) {
        MOQ_TEST_CHECK_EQ_U64(got[i].g, 5);
        MOQ_TEST_CHECK_EQ_U64(got[i].o, (uint64_t)i);   /* strictly ascending */
    }
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("merge_order");
    return failures;
}

/* -- T12a: status objects are skipped (never surfaced as fetch items) ----- */
static int
test_status_skip(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    /* group 0: NORMAL 0,1 then an END_OF_GROUP status object at object 2. */
    MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, 1) == MOQR_OK);
    MOQ_TEST_CHECK(ing_status(c, t, 0, 0, 2, MOQR_OBJ_END_OF_GROUP) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 0);   /* whole group 0 */
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    fitem_t got[8];
    int n = collect(c, fh, got, 8, &failures);
    MOQ_TEST_CHECK_EQ_INT(n, 2);   /* only the two NORMAL objects, no status */
    MOQ_TEST_CHECK(n < 2 || (got[0].o == 0 && got[1].o == 1));
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("status_skip");
    return failures;
}

/* -- T12b/c: before-tail vs tail end_of_track ----------------------------- */
static int
test_end_of_track(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    /* NORMAL 0..4, then END_OF_TRACK status at the final location 0:5. */
    for (uint64_t o = 0; o < 5; o++) {
        MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, o) == MOQR_OK);
    }
    MOQ_TEST_CHECK(ing_status(c, t, 0, 0, 5, MOQR_OBJ_END_OF_TRACK) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;

    /* Tail fetch: whole group 0 reaches the final object -> end_of_track = 1. */
    fetch_req(&r, "v", nsb, 0, 0, 0, 0);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_ACCEPT);
    MOQ_TEST_CHECK(plan.end_of_track);   /* reaches largest (the EOT location) */
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    /* Before-tail fetch: [0:0 .. 0:3) stops short -> end_of_track = 0. */
    fetch_req(&r, "v", nsb, 0, 0, 0, 3);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_ACCEPT);
    MOQ_TEST_CHECK(!plan.end_of_track);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("end_of_track");
    return failures;
}

/* -- T6/T7: terminals — unknown track, INVALID_RANGE, whole-group End ----- */
static int
test_terminals(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;

    /* Unknown track (no publisher) -> DOES_NOT_EXIST, no slot allocated. */
    fetch_req(&r, "ghost", nsb, 0, 0, 0, 1);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_REJECT);
    MOQ_TEST_CHECK_EQ_U64(plan.error_code, 0x10);   /* DOES_NOT_EXIST */

    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);

    /* Empty track (no ingest) -> INVALID_RANGE. */
    fetch_req(&r, "v", nsb, 0, 0, 0, 1);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_REJECT);
    MOQ_TEST_CHECK_EQ_U64(plan.error_code, 0x11);   /* INVALID_RANGE */

    for (uint64_t o = 0; o < 3; o++) {
        MOQ_TEST_CHECK(ing(c, &a, t, 2, 0, o) == MOQR_OK);
    }
    MOQ_TEST_CHECK(ing(c, &a, t, 4, 0, 0) == MOQR_OK);   /* largest = 4:0 */
    /* Start past largest -> INVALID_RANGE. */
    fetch_req(&r, "v", nsb, 9, 0, 9, 1);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_REJECT);
    MOQ_TEST_CHECK_EQ_U64(plan.error_code, 0x11);

    /* Whole-group End (end_object 0) of a NON-largest group: serves all of
     * group 2; the "whole group" encoding is preserved -> End = (2,0). */
    fetch_req(&r, "v", nsb, 2, 0, 2, 0);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_ACCEPT);
    MOQ_TEST_CHECK_EQ_U64(plan.end_group, 2);
    MOQ_TEST_CHECK_EQ_U64(plan.end_object, 0);
    fitem_t got[8];
    int n = collect(c, fh, got, 8, &failures);
    MOQ_TEST_CHECK_EQ_INT(n, 3);   /* only group 2's objects, not group 4 */
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("terminals");
    return failures;
}

/* -- max_fetches exhaustion refuses cleanly ------------------------------- */
static int
test_max_fetches(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 2);   /* max_fetches = 2 */
    MOQ_TEST_CHECK(c != NULL);
    moqr_core_limits_t lim;
    moqr_core_get_limits(c, &lim);
    MOQ_TEST_CHECK_EQ_U64(lim.max_fetches, 2);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, 0) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 1);
    moqr_fetch_t f1, f2, f3;
    moqr_fetch_plan_t p;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &f1, &p) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(p.admit, MOQR_FETCH_ACCEPT);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &f2, &p) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(p.admit, MOQR_FETCH_ACCEPT);
    /* Pool exhausted -> CAPACITY (counted MOQR_REFUSE_FETCHES). */
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &f3, &p) ==
                   MOQR_ERR_CAPACITY);
    /* Close one; a new open succeeds again (slot recycled). */
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, f1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &f3, &p) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(p.admit, MOQR_FETCH_ACCEPT);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, f2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, f3) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("max_fetches");
    return failures;
}

/* -- T13: fetch_pin_bytes budget — multi-fetch backpressure --------------- */
static int
test_pin_budget(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    /* max_bytes = 64 so a 64-byte object is the largest admissible record;
     * fetch_pin_bytes = 64 -> budget holds exactly one pinned object. */
    moqr_core_t *c = mkcore(&a, 64, 64, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    MOQ_TEST_CHECK(ingp(c, &a, t, 0, 0, 0, 0xAB, 64) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 1);
    moqr_fetch_t f1, f2;
    moqr_fetch_plan_t p;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &f1, &p) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &f2, &p) == MOQR_OK);

    moqr_fetch_item_t it;
    /* f1 peek pins the object (uses the whole 64-byte budget). */
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, f1, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);
    /* f2 peek can't pin now: budget full -> WOULD_BLOCK (transient, not error). */
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, f2, FNOW, &it) == MOQR_ERR_WOULD_BLOCK);
    /* commit f1 -> releases the budget; f2 now makes progress. */
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, f1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, f2, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, f2) == MOQR_OK);

    MOQ_TEST_CHECK(moqr_core_fetch_close(c, f1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, f2) == MOQR_OK);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("pin_budget");
    return failures;
}

/* -- T14: no-infinite-wait — a tiny configured budget is FLOORED so a lone
 *         object still fits and the fetch completes (never hangs). --------- */
static int
test_pin_floor(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    /* Configured budget 1 byte, floored up to the per-track max_bytes (64), so
     * the 64-byte object fits an idle budget and the fetch completes. */
    moqr_core_t *c = mkcore(&a, 64, 1, 0);
    MOQ_TEST_CHECK(c != NULL);
    /* The capacity ceiling reflects the floor, not the tiny configured value. */
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_bytes = 64;
    cfg.fetch_pin_bytes = 1;
    moqr_core_capacity_t cap;
    moqr_core_capacity_describe(&cfg, &cap);
    MOQ_TEST_CHECK(cap.payload_bytes >= 64);   /* floored, not 1 */

    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    MOQ_TEST_CHECK(ingp(c, &a, t, 0, 0, 0, 0xCD, 64) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 1);
    moqr_fetch_t fh;
    moqr_fetch_plan_t p;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &p) == MOQR_OK);
    /* A bounded pump: the object is delivered within one peek (no infinite
     * WOULD_BLOCK), because the budget was floored to fit it. */
    moqr_fetch_item_t it;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_END);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("pin_floor");
    return failures;
}

/* -- T9: idempotent re-peek (the WOULD_BLOCK hold invariant) -------------- */
static int
test_repeek_idempotent(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, 1) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 2);
    moqr_fetch_t fh;
    moqr_fetch_plan_t p;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &p) == MOQR_OK);

    moqr_fetch_item_t a1, a2;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &a1) == MOQR_OK);
    /* Re-peek WITHOUT commit returns the identical object (a blocked write is
     * retried by peeking again). Also ingest between peeks — the pinned view is
     * stable regardless. */
    MOQ_TEST_CHECK(ing(c, &a, t, 3, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &a2) == MOQR_OK);
    MOQ_TEST_CHECK(a1.kind == MOQR_FETCH_ITEM_OBJECT &&
                   a2.kind == MOQR_FETCH_ITEM_OBJECT);
    MOQ_TEST_CHECK_EQ_U64(a1.rec.object_id, a2.rec.object_id);
    MOQ_TEST_CHECK_EQ_U64(a1.rec.object_id, 0);
    MOQ_TEST_CHECK(a2.rec.payload != NULL);
    /* Now commit and advance to object 1. */
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);
    moqr_fetch_item_t b1;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &b1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(b1.rec.object_id, 1);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("repeek_idempotent");
    return failures;
}

/* -- T10: close + binding-close release pins and invalidate handles ------- */
static int
test_close_cleanup(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, 0) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 1);

    /* (a) fetch_close after a peek (pin held) releases the pin; handle dead. */
    moqr_fetch_t fh;
    moqr_fetch_plan_t p;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &p) == MOQR_OK);
    moqr_fetch_item_t it;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);   /* pinned */
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_ERR_STALE_HANDLE);

    /* (b) binding_close releases a mid-fetch pin (no leak). */
    moqr_fetch_t fh2;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh2, &p) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh2, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_close(c, sub, FNOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh2, FNOW, &it) == MOQR_ERR_STALE_HANDLE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);   /* pins were released */
    MOQ_TEST_PASS("close_cleanup");
    return failures;
}

/* -- Lifetime: an open fetch pins its range against retention aging --------
 * A live fetch is a track reader: ticking past max_age must NOT age out (or
 * free) the accepted retained range under it — the fetch drains fully instead
 * of clean-ENDing a truncated range. */
static int
test_age_retention(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 8;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.log_budget.max_age_us = 100;   /* records older than 100us age out */
    cfg.linger_us = 10;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    for (uint64_t o = 0; o < 3; o++) {
        MOQ_TEST_CHECK(ing(c, &a, t, 0, 0, o) == MOQR_OK);   /* arrival_us = 1 */
    }

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 3);
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, 10, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_ACCEPT);

    /* Tick far past retention BEFORE draining. The open fetch pins the range:
     * records must survive, so the fetch delivers all 3 (never a truncated
     * clean-END). Without the reader guard, tick would age them out. */
    MOQ_TEST_CHECK(moqr_core_tick(c, 100000) == MOQR_OK);

    fitem_t got[8];
    int n = collect(c, fh, got, 8, &failures);
    MOQ_TEST_CHECK_EQ_INT(n, 3);   /* full range, not truncated */
    for (int i = 0; i < 3 && i < n; i++) {
        MOQ_TEST_CHECK_EQ_U64(got[i].o, (uint64_t)i);
    }
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    /* Cleanup resumes after close: aging now removes the stale records. */
    MOQ_TEST_CHECK(moqr_core_tick(c, 200000) == MOQR_OK);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 0);   /* aged out post-close */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("age_retention");
    return failures;
}

/* -- evicted-prefix lead marker ------------------------------------------- */
/* A fetch whose Start fell below the retention horizon is now served (not
 * fail-closed): the retained suffix, led by one UNKNOWN marker before the first
 * retained group. */
static int
test_evicted_prefix(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);   /* max_groups = 8 */
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);
    /* Groups 0..10, one object each. max_groups=8 evicts oldest-first, so groups
     * 0,1,2 age out and oldest_group_id == 3. */
    for (uint64_t g = 0; g <= 10; g++) {
        MOQ_TEST_CHECK(ing(c, &a, t, g, 0, 0) == MOQR_OK);
    }

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 10, 1);   /* [0:0 .. 10:0], Start in evicted prefix */
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    /* Now accepts (this was an INVALID_RANGE fail-close before). End Location is
     * unchanged: capped to the retained/largest end (10,1). */
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_ACCEPT);
    MOQ_TEST_CHECK_EQ_U64(plan.end_group, 10);
    MOQ_TEST_CHECK_EQ_U64(plan.end_object, 1);
    MOQ_TEST_CHECK(moqr_fetch_is_valid(fh));

    /* First peek: the UNKNOWN marker before the first retained group (3). */
    moqr_fetch_item_t it;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(it.kind, MOQR_FETCH_ITEM_MARKER);
    MOQ_TEST_CHECK_EQ_U64(it.marker_group, 3);
    /* Re-peek BEFORE commit returns the identical marker (no pin, idempotent). */
    moqr_fetch_item_t it2;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it2) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(it2.kind, MOQR_FETCH_ITEM_MARKER);
    MOQ_TEST_CHECK_EQ_U64(it2.marker_group, 3);
    /* Commit the marker (its only state advance); objects follow from group 3. */
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);

    fitem_t got[16];
    int n = collect(c, fh, got, 16, &failures);
    MOQ_TEST_CHECK_EQ_INT(n, 8);   /* retained groups 3..10, one object each */
    for (int i = 0; i < 8 && i < n; i++) {
        MOQ_TEST_CHECK_EQ_U64(got[i].g, (uint64_t)(3 + i));
        MOQ_TEST_CHECK_EQ_U64(got[i].o, 0);
    }
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    /* A range entirely below the horizon (never reaches retained data) still
     * fail-closes with INVALID_RANGE — nothing to serve, no bare marker. */
    fetch_req(&r, "v", nsb, 0, 0, 2, 0);   /* [0..2 whole], all evicted */
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_REJECT);
    MOQ_TEST_CHECK_EQ_U64(plan.error_code, 0x11);   /* INVALID_RANGE */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("evicted_prefix");
    return failures;
}

/* Relay-core chunk ingest wrappers (moqr_core_ingest OPEN + append_chunk +
 * complete/abandon). open -> append -> complete retains and counts the object;
 * open -> append -> abandon releases and does not count. */
static int
test_chunk_ingest_core(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);

    /* Object A: open (group 0, object 0, declared 20) -> two 10-byte chunks. */
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 0;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 128;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = 20;
    d.now_us = 1;
    MOQ_TEST_CHECK(moqr_core_ingest(c, t, &d) == MOQR_OK);
    for (int k = 0; k < 2; k++) {
        uint8_t buf[10];
        memset(buf, (uint8_t)(0x40 + k), sizeof(buf));
        moq_rcbuf_t *ch = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&a.vt, buf, sizeof(buf), &ch) == 0);
        MOQ_TEST_CHECK(moqr_core_append_chunk(c, t, 0, 0, 0, ch) == MOQR_OK);
        moq_rcbuf_decref(ch);
    }
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 20);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 0);   /* not counted until complete */
    MOQ_TEST_CHECK(moqr_core_complete_record(c, t, 0, 0, 0) == MOQR_OK);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 20);   /* COMPLETE retains */
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 1);

    /* Append against the now-COMPLETE identity -> WRONG_STATE (no OPEN record). */
    {
        uint8_t buf[1] = { 0 };
        moq_rcbuf_t *ch = NULL;
        moq_rcbuf_create(&a.vt, buf, 1, &ch);
        MOQ_TEST_CHECK(moqr_core_append_chunk(c, t, 0, 0, 0, ch) ==
                       MOQR_ERR_WRONG_STATE);
        moq_rcbuf_decref(ch);
    }

    /* Object B: open (group 1) -> append 15 -> abandon (releases, not counted). */
    moqr_log_append_desc_init(&d);
    d.group_id = 1;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 128;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = 30;
    d.now_us = 2;
    MOQ_TEST_CHECK(moqr_core_ingest(c, t, &d) == MOQR_OK);
    {
        uint8_t buf[15];
        memset(buf, 0x55, sizeof(buf));
        moq_rcbuf_t *ch = NULL;
        moq_rcbuf_create(&a.vt, buf, sizeof(buf), &ch);
        MOQ_TEST_CHECK(moqr_core_append_chunk(c, t, 1, 0, 0, ch) == MOQR_OK);
        moq_rcbuf_decref(ch);
    }
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 35);   /* 20 (A) + 15 (B) */
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, t, 1, 0, 0, rd_wire(0)) == MOQR_OK);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 20);   /* B's chunks released */
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 1);     /* B never counted */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("chunk_ingest_core");
    return failures;
}

/* Retained standalone FETCH over a chunked COMPLETE record.
 * Production streaming ingest stores NORMAL objects as chunked COMPLETE
 * records (payload in the log chunk list). fetch_scan_next must find them and
 * fetch_peek must coalesce the slices into ONE payload, in order.
 * Proves: the item is served as whole-object (chunk_count presented 0) with the
 * exact coalesced bytes, re-peek is idempotent (same pin, no re-coalesce), commit
 * advances to END, and the allocator balances (the coalesced buffer is released
 * exactly once). RED: restore the `chunk_count > 0` skip in fetch_scan_next and
 * the peek returns FETCH_ITEM_END with zero objects. */
static int
test_chunked_complete_fetch(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);

    /* Chunked COMPLETE record at (0,0,0): 3 DISTINCT 4-byte chunks -> 12 bytes,
     * so a dropped or reordered chunk changes the coalesced bytes. */
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 0;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 1;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = 12;
    d.now_us = 1;
    MOQ_TEST_CHECK(moqr_core_ingest(c, t, &d) == MOQR_OK);
    static const uint8_t CB[3] = { 0x11u, 0x22u, 0x33u };
    for (int i = 0; i < 3; i++) {
        uint8_t body[4];
        memset(body, CB[i], sizeof(body));
        moq_rcbuf_t *ch = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&a.vt, body, sizeof(body), &ch) == 0);
        MOQ_TEST_CHECK(moqr_core_append_chunk(c, t, 0, 0, 0, ch) == MOQR_OK);
        moq_rcbuf_decref(ch);
    }
    MOQ_TEST_CHECK(moqr_core_complete_record(c, t, 0, 0, 0) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 0);   /* whole group 0 */
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(plan.admit, MOQR_FETCH_ACCEPT);

    static const uint8_t WANT[12] = { 0x11, 0x11, 0x11, 0x11, 0x22, 0x22,
                                      0x22, 0x22, 0x33, 0x33, 0x33, 0x33 };
    moqr_fetch_item_t it;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);
    MOQ_TEST_CHECK(it.rec.status == MOQR_OBJ_NORMAL);
    MOQ_TEST_CHECK_EQ_U64(it.rec.object_id, 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)it.rec.chunk_count, 0);  /* whole-object view */
    MOQ_TEST_CHECK(it.rec.payload != NULL);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_rcbuf_len(it.rec.payload), 12);
    if (it.rec.payload != NULL && moq_rcbuf_len(it.rec.payload) == 12) {
        MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(it.rec.payload), WANT, 12) == 0);
    }

    /* Re-peek is idempotent: the coalesced buffer is held (same pointer), not
     * rebuilt, so a downstream WOULD_BLOCK replays it verbatim. */
    moqr_fetch_item_t it2;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it2) == MOQR_OK);
    MOQ_TEST_CHECK(it2.kind == MOQR_FETCH_ITEM_OBJECT);
    MOQ_TEST_CHECK(it2.rec.payload == it.rec.payload);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_rcbuf_len(it2.rec.payload), 12);

    /* Commit advances past the object; the range then ends. */
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);
    moqr_fetch_item_t it3;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it3) == MOQR_OK);
    MOQ_TEST_CHECK(it3.kind == MOQR_FETCH_ITEM_END);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);   /* coalesced buffer released once */
    MOQ_TEST_PASS("chunked_complete_fetch");
    return failures;
}

/* Coalesce fail-closed under OOM. The coalesce path inside an accepted
 * FETCH allocates two blocks — the temp buffer (r_alloc) and the owning rcbuf
 * (moq_rcbuf_create). Injecting a failure at EITHER must make fetch_peek return a
 * TERMINAL (not END, not an object), leak nothing (temp + rcbuf), leave no stale
 * pin/accounting (a retry serves the same object), and let close succeed. RED:
 * drop the `r_free(tmp)` after moq_rcbuf_create in fetch_coalesce_chunks and the
 * final a.live check catches the leaked temp. */
static int
test_chunked_fetch_oom(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);

    /* One chunked COMPLETE record (0,0,0): 3 x 4-byte chunks -> 12 bytes. */
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 0;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 1;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = 12;
    d.now_us = 1;
    MOQ_TEST_CHECK(moqr_core_ingest(c, t, &d) == MOQR_OK);
    static const uint8_t CB[3] = { 0x11u, 0x22u, 0x33u };
    for (int i = 0; i < 3; i++) {
        uint8_t body[4];
        memset(body, CB[i], sizeof(body));
        moq_rcbuf_t *ch = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&a.vt, body, sizeof(body), &ch) == 0);
        MOQ_TEST_CHECK(moqr_core_append_chunk(c, t, 0, 0, 0, ch) == MOQR_OK);
        moq_rcbuf_decref(ch);
    }
    MOQ_TEST_CHECK(moqr_core_complete_record(c, t, 0, 0, 0) == MOQR_OK);

    moq_bytes_t nsb[1];

    /* Baseline — a clean coalesce peek consumes EXACTLY two allocs (the temp
     * buffer + the owning rcbuf block), pinning the injection offsets below. */
    {
        moqr_fetch_req_t r;
        fetch_req(&r, "v", nsb, 0, 0, 0, 0);
        moqr_fetch_t fh;
        moqr_fetch_plan_t plan;
        MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) ==
                       MOQR_OK);
        long a0 = a.attempts;
        moqr_fetch_item_t it;
        MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it) == MOQR_OK);
        MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);
        MOQ_TEST_CHECK_EQ_INT((int)(a.attempts - a0), 2);
        MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);   /* unpin */
    }

    /* which == 0 -> fail the temp buffer alloc (offset +1); == 1 -> fail the
     * owning-rcbuf (moq_rcbuf_create) alloc (offset +2; the temp succeeds then
     * must be freed on the failure). */
    for (int which = 0; which < 2; which++) {
        moqr_fetch_req_t r;
        fetch_req(&r, "v", nsb, 0, 0, 0, 0);
        moqr_fetch_t fh;
        moqr_fetch_plan_t plan;
        MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) ==
                       MOQR_OK);
        a.fail_at = a.attempts + (which == 0 ? 1 : 2);
        moqr_fetch_item_t it;
        moqr_result_t prc = moqr_core_fetch_peek(c, fh, FNOW, &it);
        MOQ_TEST_CHECK(prc == MOQR_ERR_NOMEM);           /* terminal, fail-closed */
        MOQ_TEST_CHECK(it.kind != MOQR_FETCH_ITEM_OBJECT);
        MOQ_TEST_CHECK(it.kind != MOQR_FETCH_ITEM_END);
        a.fail_at = 0;                                   /* disable injection */
        /* No stale pin/accounting: a retry serves the SAME object. */
        moqr_fetch_item_t it2;
        MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it2) == MOQR_OK);
        MOQ_TEST_CHECK(it2.kind == MOQR_FETCH_ITEM_OBJECT);
        MOQ_TEST_CHECK(it2.rec.payload != NULL);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_rcbuf_len(it2.rec.payload), 12);
        MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);
    }

    /* A fresh, uninjected fetch still fully serves the object + FINs. */
    {
        moqr_fetch_req_t r;
        fetch_req(&r, "v", nsb, 0, 0, 0, 0);
        moqr_fetch_t fh;
        moqr_fetch_plan_t plan;
        MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) ==
                       MOQR_OK);
        fitem_t got[4];
        int n = collect(c, fh, got, 4, &failures);
        MOQ_TEST_CHECK_EQ_INT(n, 1);
        MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);
    }

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);   /* no leaked temp / owning rcbuf */
    MOQ_TEST_PASS("chunked_fetch_oom");
    return failures;
}

/* Payload lifetime is core-INDEPENDENT. A coalesced FETCH payload is handed to
 * the session layer, which increfs it into a send action that may
 * hold the final reference PAST core teardown (after fetch_commit unpins the
 * core's ref). So the payload rcbuf must free via the allocator, not the core.
 * This takes a ref, tears the core DOWN, then does the final decref — which must
 * be clean. RED: revert fetch_coalesce_chunks to moq_rcbuf_wrap with a
 * core-pointer release callback and this decref-after-destroy is a use-after-free
 * of the core (ASan abort). */
static int
test_chunked_fetch_payload_outlives_core(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 0, 0, 0);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    moqr_track_t t;
    MOQ_TEST_CHECK(mktrack(c, pub, "v", &t) == MOQR_OK);

    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 0;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 1;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = 12;
    d.now_us = 1;
    MOQ_TEST_CHECK(moqr_core_ingest(c, t, &d) == MOQR_OK);
    static const uint8_t CB[3] = { 0x11u, 0x22u, 0x33u };
    for (int i = 0; i < 3; i++) {
        uint8_t body[4];
        memset(body, CB[i], sizeof(body));
        moq_rcbuf_t *ch = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_create(&a.vt, body, sizeof(body), &ch) == 0);
        MOQ_TEST_CHECK(moqr_core_append_chunk(c, t, 0, 0, 0, ch) == MOQR_OK);
        moq_rcbuf_decref(ch);
    }
    MOQ_TEST_CHECK(moqr_core_complete_record(c, t, 0, 0, 0) == MOQR_OK);

    moq_bytes_t nsb[1];
    moqr_fetch_req_t r;
    fetch_req(&r, "v", nsb, 0, 0, 0, 0);
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sub, &r, FNOW, &fh, &plan) == MOQR_OK);

    /* Take a ref on the coalesced payload that we deliberately hold past core
     * teardown — standing in for a session send action that increfs it. */
    moqr_fetch_item_t it;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, FNOW, &it) == MOQR_OK);
    MOQ_TEST_CHECK(it.kind == MOQR_FETCH_ITEM_OBJECT);
    MOQ_TEST_CHECK(it.rec.payload != NULL);
    moq_rcbuf_t *held = moq_rcbuf_incref((moq_rcbuf_t *)(void *)it.rec.payload);

    /* Commit (unpins the core's ref; `held` is now the last ref) + close +
     * DESTROY THE CORE. The payload must survive. */
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);
    moqr_core_destroy(c);
    c = NULL;

    /* Bytes still valid; the final decref frees via the allocator, not the core. */
    static const uint8_t WANT[12] = { 0x11, 0x11, 0x11, 0x11, 0x22, 0x22,
                                      0x22, 0x22, 0x33, 0x33, 0x33, 0x33 };
    MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_rcbuf_len(held), 12);
    MOQ_TEST_CHECK(memcmp(moq_rcbuf_data(held), WANT, 12) == 0);
    moq_rcbuf_decref(held);   /* final decref AFTER core destroy: must be clean */

    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("chunked_fetch_payload_outlives_core");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_chunk_ingest_core();
    failures += test_chunked_complete_fetch();   /* FETCH over a chunked COMPLETE record */
    failures += test_chunked_fetch_oom();        /* coalesce fail-closed under OOM */
    failures += test_chunked_fetch_payload_outlives_core();  /* coalesced payload outlives the core */
    failures += test_exact_replay();
    failures += test_merge_order();
    failures += test_status_skip();
    failures += test_end_of_track();
    failures += test_terminals();
    failures += test_max_fetches();
    failures += test_pin_budget();
    failures += test_pin_floor();
    failures += test_repeek_idempotent();
    failures += test_close_cleanup();
    failures += test_age_retention();
    failures += test_evicted_prefix();
    if (failures == 0) {
        printf("ALL FETCH CORE TESTS PASSED\n");
    }
    return failures == 0 ? 0 : 1;
}
