/*
 * Control-plane tests: announce/subscribe/coalescing/resolution, warm
 * rejoin + linger, filters, ns discovery, publish push, track status,
 * capacity/reserve refusals, binding close. Allocator balance zero ends
 * every test.
 */

#include <moq/relay/relay.h>
#include <moq/relay/capacity.h>
#include <moq/relay/log.h>
#include <moq/relay/trace.h>

#include <moq/rcbuf.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* These control tests assert delivery ordering at a single fixed instant;
 * they do not advance a clock. A retire triggered here (range completion /
 * stream error) arms the warm linger with this value — nonzero so it is
 * never a stale-clock (past) deadline. */
#define CTRL_NOW 1000u

/* counting allocator (same shape as test_relay_log) */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
} ca_t;
/* An upstream terminal arrives in some connection's draft; these rigs speak
 * draft-16 unless a case says otherwise. */
/* SUB_DONE carries its terminal as a tagged descriptor; read it the way an
 * emitting connection would rather than through the REQUEST_ERROR-domain
 * error_code field. */
static uint64_t
pd_wire_for(moqr_pd_desc_t d, moq_version_t v)
{
    uint64_t w = UINT64_MAX;

    (void)moqr_pd_desc_emit(d, v, &w);
    return w;
}

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

/* Ingest a chunked COMPLETE record: open the object, append `nchunks` chunks of
 * `clen` bytes each (chunk k filled with fill0+k), then complete. Mirrors the
 * streaming-ingest path that the relay drives from OBJECT_CHUNK events. */
static moqr_result_t
ing_chunked(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t sg,
            uint64_t o, uint8_t prio, uint32_t nchunks, uint64_t clen,
            uint8_t fill0)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = prio;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = (uint64_t)nchunks * clen;
    d.now_us = 1;
    moqr_result_t rc = moqr_core_ingest(c, t, &d);   /* open */
    if (rc != MOQR_OK) {
        return rc;
    }
    for (uint32_t k = 0; k < nchunks; k++) {
        uint8_t buf[64];
        memset(buf, (uint8_t)(fill0 + k), (size_t)clen);
        moq_rcbuf_t *ch = NULL;
        if (moq_rcbuf_create(&a->vt, buf, (size_t)clen, &ch) != 0) {
            return MOQR_ERR_NOMEM;
        }
        rc = moqr_core_append_chunk(c, t, g, sg, o, ch);
        moq_rcbuf_decref(ch);
        if (rc != MOQR_OK) {
            return rc;
        }
    }
    return moqr_core_complete_record(c, t, g, sg, o);   /* complete */
}

/* Open an OPEN record (no chunks yet, declared length `declared`). */
static moqr_result_t
open_rec(moqr_core_t *c, moqr_track_t t, uint64_t g, uint64_t sg, uint64_t o,
         uint64_t declared)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 128;
    d.status = MOQR_OBJ_NORMAL;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = declared;
    d.now_us = 1;
    return moqr_core_ingest(c, t, &d);
}

/* Append one `len`-byte chunk (filled with `fill`) to an OPEN record. */
static moqr_result_t
append1(moqr_core_t *c, ca_t *a, moqr_track_t t, uint64_t g, uint64_t sg,
        uint64_t o, uint64_t len, uint8_t fill)
{
    uint8_t buf[64];
    memset(buf, fill, (size_t)len);
    moq_rcbuf_t *ch = NULL;
    if (moq_rcbuf_create(&a->vt, buf, (size_t)len, &ch) != 0) {
        return MOQR_ERR_NOMEM;
    }
    moqr_result_t rc = moqr_core_append_chunk(c, t, g, sg, o, ch);
    moq_rcbuf_decref(ch);
    return rc;
}

/* Drain intents into a caller array. */
static size_t
drain(moqr_core_t *c, moqr_intent_t *out, size_t cap)
{
    return moqr_core_poll_intents(c, out, cap);
}

static int
test_announce_subscribe_fanout(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    moqr_binding_t pub, s1, s2;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 300, &s2) == MOQR_OK);

    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);

    /* Two subscribers to the same new track: ONE upstream subscribe. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.name = B("video");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;   /* from {0,0} */
    rq.cookie = 11;
    moqr_sub_t sub1, sub2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub1) == MOQR_OK);
    rq.cookie = 22;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s2, &rq, &sub2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_sub_is_valid(sub1) && moqr_sub_is_valid(sub2));

    moqr_intent_t its[16];
    size_t n = drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);   /* coalesced */
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    MOQ_TEST_CHECK_EQ_U64(its[0].binding_cookie, 100);
    MOQ_TEST_CHECK_EQ_U64(its[0].ns_count, 2);
    MOQ_TEST_CHECK(its[0].name.len == 5 &&
                   memcmp(its[0].name.data, "video", 5) == 0);
    moqr_track_t track = its[0].track;
    uint64_t tgen = its[0].track_gen;

    /* Resolve: both parked subs accept. */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen, 777, true, 4, 9) ==
                   MOQR_OK);
    n = drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)2);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_ACCEPT_SUB);
    MOQ_TEST_CHECK_EQ_U64(its[1].kind, MOQR_INTENT_ACCEPT_SUB);
    MOQ_TEST_CHECK(its[0].has_largest && its[0].largest_group == 4 &&
                   its[0].largest_object == 9);

    /* Stale resolution is refused. */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen, 777, false, 0, 0) ==
                   MOQR_ERR_WRONG_STATE);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen + 1, 7, false, 0,
                                         0) == MOQR_ERR_STALE_HANDLE);

    /* Ingest and fan out to both bindings; payload identity preserved. */
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 0, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 0, 1, 128) == MOQR_OK);
    for (int side = 0; side < 2; side++) {
        moqr_binding_t bb = side == 0 ? s1 : s2;
        for (uint64_t want = 0; want < 2; want++) {
            moqr_delivery_t d;
            MOQ_TEST_CHECK(moqr_core_next_delivery(c, bb, CTRL_NOW, &d) == MOQR_OK);
            MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
            MOQ_TEST_CHECK_EQ_U64(d.rec.object_id, want);
            MOQ_TEST_CHECK(d.rec.payload != NULL);
            MOQ_TEST_CHECK(moqr_core_delivery_done(
                               c, bb, MOQR_DELIVERY_DELIVERED, CTRL_NOW) == MOQR_OK);
        }
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, bb, CTRL_NOW, &d) == MOQR_DONE);
    }

    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 4);
    MOQ_TEST_CHECK_EQ_U64(st.tracks, 1);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("announce_subscribe_fanout");
    return failures;
}

static int
test_filters_and_range(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);

    /* Publish-push a track and pre-fill retained content. */
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"),
                                          B("t"), 5, &track) == MOQR_OK);
    moqr_intent_t its[8];
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_ACCEPT_PUBLISH);
    for (uint64_t g = 1; g <= 2; g++) {
        for (uint64_t o = 0; o < 3; o++) {
            MOQ_TEST_CHECK(ing(c, &a, track, g, 0, o, 128) == MOQR_OK);
        }
    }

    /* LARGEST_OBJECT: starts past {2,2}; retained must not pass. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_LARGEST_OBJECT;
    rq.cookie = 1;
    moqr_sub_t live_sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &live_sub) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);
    MOQ_TEST_CHECK(its[0].kind == MOQR_INTENT_ACCEPT_SUB &&
                   its[0].has_largest && its[0].largest_group == 2 &&
                   its[0].largest_object == 2);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);
    MOQ_TEST_CHECK(ing(c, &a, track, 2, 0, 3, 128) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.object_id, 3);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, live_sub, 10) == MOQR_OK);

    /* ABSOLUTE_RANGE over retained: groups [1, 1+0] = group 1 only, then
     * SUB_DONE once group 2 is reached. */
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_RANGE;
    rq.filter.start_group = 1;
    rq.filter.start_object = 1;
    rq.filter.end_group_delta = 0;
    rq.cookie = 2;
    moqr_sub_t range_sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &range_sub) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);
    uint64_t got[8];
    size_t got_n = 0;
    for (;;) {
        moqr_result_t rc = moqr_core_next_delivery(c, s1, CTRL_NOW, &d);
        if (rc != MOQR_OK) {
            break;
        }
        got[got_n++] = d.rec.object_id;
        MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 1);
        MOQ_TEST_CHECK(moqr_core_delivery_done(
                           c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_SIZE(got_n, (size_t)2);   /* {1,1} and {1,2} */
    MOQ_TEST_CHECK(got[0] == 1 && got[1] == 2);
    /* Range completion produced SUB_DONE and retired the sub. */
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_SUB_DONE);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, range_sub, 11) ==
                   MOQR_ERR_STALE_HANDLE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("filters_and_range");
    return failures;
}

static int
test_warm_linger_and_status(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);

    /* Linger is a pull-source policy.  Establish this track through the
     * announce -> upstream SUBSCRIBE path, not PUBLISH: a live push source
     * remains authoritative until PUBLISH_FINISHED or its binding closes. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 9;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    moqr_intent_t its[8];
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_track_t track = its[0].track;
    uint64_t track_gen = its[0].track_gen;
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, track_gen, 5, false, 0, 0) ==
                   MOQR_OK);
    n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_ACCEPT_SUB);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 128) == MOQR_OK);

    /* Subscribe, deliver, unsubscribe -> linger -> WARM after tick. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 100) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_tick(c, 500) == MOQR_OK);   /* not yet */
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)0);
    MOQ_TEST_CHECK(moqr_core_tick(c, 1200) == MOQR_OK);  /* past linger */
    n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);

    /* Ingest to a WARM track is refused: the handle is still live (slot
     * generation), but the state no longer accepts ingest. */
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 9, 128) == MOQR_ERR_WRONG_STATE);

    /* Track status from WARM retained state. */
    MOQ_TEST_CHECK(moqr_core_track_status(c, s1, NS2(nsb, "n", "s"), B("t"),
                                          77) == MOQR_OK);
    n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK(its[0].kind == MOQR_INTENT_TRACK_STATUS_OK &&
                   its[0].has_largest && its[0].largest_group == 1);
    MOQ_TEST_CHECK(moqr_core_track_status(c, s1, NS2(nsb, "n", "s"),
                                          B("nope"), 78) == MOQR_OK);
    n = drain(c, its, 8);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_TRACK_STATUS_ERROR);

    /* Warm rejoin: immediate accept with largest + fresh upstream intent,
     * retained content served. */
    rq.cookie = 10;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)2);
    MOQ_TEST_CHECK(its[0].kind == MOQR_INTENT_ACCEPT_SUB &&
                   its[0].has_largest);
    MOQ_TEST_CHECK_EQ_U64(its[1].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 1);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                   MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("warm_linger_and_status");
    return failures;
}

/* A terminal stream error on an outstanding delivery retires the
 * subscriber INSIDE moqr_core_delivery_done. When it is the last subscriber of
 * an ACTIVE track, that retire arms the warm linger — and it must use the
 * now_us handed to delivery_done, not a stale 0. We retire at a now_us well
 * past linger_us so a stale-clock deadline (0 + linger_us) would already be in
 * the past: a tick between the two deadlines then warms the buggy track early
 * but leaves the correctly clocked one lingering. */
static int
test_stream_error_retire_linger(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);   /* linger_us = 1000 */
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);

    /* This clock oracle also needs a pull source: only a pull subscription has
     * a demand-owned linger deadline to release. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 9;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    moqr_intent_t its[8];
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_track_t track = its[0].track;
    uint64_t track_gen = its[0].track_gen;
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, track_gen, 5, false, 0, 0) ==
                   MOQR_OK);
    n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_ACCEPT_SUB);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 128) == MOQR_OK);

    /* One outstanding delivery, failed terminally at now_us = 10000. The last
     * subscriber of the ACTIVE track retires; linger arms at 10000 + 1000. */
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, 10000, &d) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STREAM_ERROR,
                                           10000) == MOQR_OK);

    /* Between the stale (1000) and correct (11000) deadlines: the correctly
     * clocked track is still lingering, so no upstream release yet. A stale-0
     * retire warmed at 1000 and emits UPSTREAM_UNSUBSCRIBE here. */
    MOQ_TEST_CHECK(moqr_core_tick(c, 10500) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)0);

    /* Past 11000 the track warms and releases the upstream exactly once. */
    MOQ_TEST_CHECK(moqr_core_tick(c, 11500) == MOQR_OK);
    n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("stream_error_retire_linger");
    return failures;
}

static int
test_ns_discovery_and_close(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, watcher;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &watcher) == MOQR_OK);
    moq_bytes_t nsb[2], pfx[1];

    pfx[0] = B("n");
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(
                       c, watcher,
                       (moqr_ns_t){ .parts = pfx, .count = 1 },
                       500) == MOQR_OK);
    moqr_intent_t its[8];
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)0);   /* nothing yet */

    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "live")) ==
                   MOQR_OK);
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK(its[0].kind == MOQR_INTENT_NS_FOUND &&
                   its[0].binding_cookie == 2 && its[0].cookie == 500 &&
                   its[0].ns_count == 2 && its[0].value == 1);
    MOQ_TEST_CHECK(its[0].ns_parts[1].len == 4 &&
                   memcmp(its[0].ns_parts[1].data, "live", 4) == 0);

    /* Non-matching prefix stays silent. */
    pfx[0] = B("other");
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(
                       c, watcher,
                       (moqr_ns_t){ .parts = pfx, .count = 1 },
                       501) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)0);

    /* Publisher close: NS_GONE to the matching watcher. */
    MOQ_TEST_CHECK(moqr_core_binding_close(c, pub, 100) == MOQR_OK);
    n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK(its[0].kind == MOQR_INTENT_NS_GONE &&
                   its[0].cookie == 500);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ns_discovery_and_close");
    return failures;
}

static int
test_reserve_and_capacity_refusals(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    /* Intent ring of 1: a subscribe that may need 2 intents refuses with
     * pool exhaustion (a real bound) yields CAPACITY. The intent ring is
     * clamped so a single fan-out always fits (see intent_ring_invariant);
     * transient ring-full WOULD_BLOCK is exercised at the binding layer. */
    moqr_core_relay_cfg_t cfg;
    moqr_core_t *c = NULL;
    moqr_binding_t pub, s1;
    moq_bytes_t nsb[2];
    moqr_subscribe_req_t rq;
    moqr_sub_t sub;

    /* Sub pool of 1: the second subscriber is refused with CAPACITY. */
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_subs = 1;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    moqr_sub_t sub2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub2) ==
                   MOQR_ERR_CAPACITY);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);

    MOQ_TEST_PASS("reserve_and_capacity_refusals");
    return failures;
}

static int
test_scheduling_order(void)
{
    /* Spec §7.1.1 (-18 :2378-2400): among schedulable objects — the next
     * object of EACH subgroup stream — publisher priority (lower wins)
     * outranks arrival order, and a DESCENDING subscription prefers the
     * newest group. */
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"),
                                          B("t"), 5, &track) == MOQR_OK);
    moqr_intent_t its[8];
    (void)drain(c, its, 8);

    /* Subgroup 0 (priority 200, low) arrives BEFORE subgroup 1
     * (priority 10, high). The high-priority subgroup head must be
     * scheduled first despite later arrival. */
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 200) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 2, 200) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 1, 1, 10) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 1, 3, 10) == MOQR_OK);

    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 1;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    (void)drain(c, its, 8);

    uint64_t seen_obj[4];
    for (size_t i = 0; i < 4; i++) {
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
        seen_obj[i] = d.rec.object_id;
        MOQ_TEST_CHECK(moqr_core_delivery_done(
                           c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) == MOQR_OK);
    }
    /* Priority order: subgroup 1 first (objects 1, 3), then subgroup 0. */
    MOQ_TEST_CHECK_EQ_U64(seen_obj[0], 1);
    MOQ_TEST_CHECK_EQ_U64(seen_obj[1], 3);
    MOQ_TEST_CHECK_EQ_U64(seen_obj[2], 0);
    MOQ_TEST_CHECK_EQ_U64(seen_obj[3], 2);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 1) == MOQR_OK);

    /* DESCENDING group order: newest retained group first. */
    MOQ_TEST_CHECK(ing(c, &a, track, 2, 0, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 3, 0, 0, 128) == MOQR_OK);
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.filter.start_group = 2;   /* isolate rule 3: equal priorities only */
    rq.group_order = MOQR_GROUP_ORDER_DESCENDING;
    rq.cookie = 2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    (void)drain(c, its, 8);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 3);   /* newest first */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                   MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("scheduling_order");
    return failures;
}

static int
test_delivery_pin_and_status_pending(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 2;   /* tiny: eviction is easy to force */
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"),
                                          B("t"), 5, &track) == MOQR_OK);
    moqr_intent_t its[8];
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 128) == MOQR_OK);

    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 1;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    (void)drain(c, its, 8);

    /* Outstanding delivery for group 1, then ingest evicts group 1
     * underneath it. The pinned payload must stay readable and the
     * completion must not corrupt state. */
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 1);
    MOQ_TEST_CHECK(ing(c, &a, track, 2, 0, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 3, 0, 0, 128) == MOQR_OK); /* evicts 1 */
    MOQ_TEST_CHECK(d.rec.payload != NULL &&
                   moq_rcbuf_data(d.rec.payload)[0] == (uint8_t)(1 * 16));
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                   MOQR_OK);

    /* TRACK_STATUS on a PENDING track must not report OK. */
    moq_bytes_t nsb2[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb2, "p", "q")) ==
                   MOQR_OK);
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb2, "p", "q");
    rq.name = B("pend");
    rq.cookie = 7;
    moqr_sub_t psub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &psub) == MOQR_OK);
    (void)drain(c, its, 8);   /* UPSTREAM_SUBSCRIBE; track stays PENDING */
    MOQ_TEST_CHECK(moqr_core_track_status(c, s1, NS2(nsb2, "p", "q"),
                                          B("pend"), 99) == MOQR_OK);
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_TRACK_STATUS_ERROR);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("delivery_pin_and_status_pending");
    return failures;
}

static int
test_handle_and_view_hygiene(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t b;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &b) == MOQR_OK);

    /* Cross-shard repack: same pool/gen/slot, different shard tag, must
     * not resolve. */
    uint64_t gen = moq_handle_generation(b._opaque);
    uint32_t slot = moq_handle_slot(b._opaque);
    moqr_binding_t foreign = {
        moq_handle_pack(MOQR_HANDLE_POOL_BINDING, /*shard_tag=*/7,
                        (uint32_t)gen, slot)
    };
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, foreign, NS2(nsb, "n", "s")) ==
                   MOQR_ERR_STALE_HANDLE);

    /* Malformed views: non-null length with NULL data, NULL parts. */
    moq_bytes_t bad_parts[2] = {
        { NULL, 3 },                       /* len without data */
        { (const uint8_t *)"x", 1 },
    };
    moqr_ns_t bad_ns = { bad_parts, 2 };
    MOQ_TEST_CHECK(moqr_core_announce(c, b, bad_ns) == MOQR_ERR_INVAL);
    moqr_ns_t null_parts = { NULL, 2 };
    MOQ_TEST_CHECK(moqr_core_announce(c, b, null_parts) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, b, null_parts) ==
                   MOQR_ERR_INVAL);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = null_parts;
    rq.name = B("t");
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, b, &rq, &sub) == MOQR_ERR_INVAL);
    moq_bytes_t nsok[1] = { { (const uint8_t *)"n", 1 } };
    rq.ns = (moqr_ns_t){ nsok, 1 };
    rq.name = (moq_bytes_t){ NULL, 4 };   /* name len without data */
    MOQ_TEST_CHECK(moqr_core_subscribe(c, b, &rq, &sub) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_track_status(c, b, (moqr_ns_t){ nsok, 1 },
                                          (moq_bytes_t){ NULL, 4 },
                                          1) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_publish_open(c, b, null_parts, B("t"), 1,
                                          &(moqr_track_t){ 0 }) ==
                   MOQR_ERR_INVAL);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("handle_and_view_hygiene");
    return failures;
}

static int
test_wide_group_scheduling(void)
{
    /* Scheduling must consider EVERY retained group: with a group budget
     * above 64, a descending subscription's first delivery is the newest
     * retained group, not the newest inside some internal window. */
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 70;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"),
                                          B("t"), 5, &track) == MOQR_OK);
    moqr_intent_t its[8];
    (void)drain(c, its, 8);
    for (uint64_t g = 1; g <= 70; g++) {
        MOQ_TEST_CHECK(ing(c, &a, track, g, 0, 0, 128) == MOQR_OK);
    }
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.filter.start_group = 1;
    rq.group_order = MOQR_GROUP_ORDER_DESCENDING;
    rq.cookie = 1;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    (void)drain(c, its, 8);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 70);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                   MOQR_OK);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("wide_group_scheduling");
    return failures;
}

static int
test_namespace_length_bounds(void)
{
    /* Every namespace/name view is bounded before any copy: per-part and
     * cumulative <= MOQ_FULL_TRACK_NAME_MAX, on every entry point
     * including the prefix-only ns_subscribe path. */
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t b;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &b) == MOQR_OK);

    static uint8_t big[8];   /* pointer is fine; length must be refused
                              * before anything reads through it */
    moq_bytes_t huge_part[1] = { { big, (size_t)MOQ_FULL_TRACK_NAME_MAX + 1 } };
    moqr_ns_t huge_ns = { huge_part, 1 };
    MOQ_TEST_CHECK(moqr_core_announce(c, b, huge_ns) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, b, huge_ns, 1) ==
                   MOQR_ERR_INVAL);

    /* Narrowing-overflow shape: a length that would wrap 32-bit sizing. */
    moq_bytes_t wrap_part[1] = { { big, (size_t)UINT32_MAX + 16 } };
    moqr_ns_t wrap_ns = { wrap_part, 1 };
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, b, wrap_ns, 2) ==
                   MOQR_ERR_INVAL);

    /* Cumulative bound: two parts of 3000 exceed the full-name cap. */
    static uint8_t p3k[3000];
    moq_bytes_t two_parts[2] = { { p3k, 3000 }, { p3k, 3000 } };
    moqr_ns_t cum_ns = { two_parts, 2 };
    MOQ_TEST_CHECK(moqr_core_announce(c, b, cum_ns) == MOQR_ERR_INVAL);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = cum_ns;
    rq.name = B("t");
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, b, &rq, &sub) == MOQR_ERR_INVAL);

    /* Combined namespace + name over the full-track-name cap. */
    static uint8_t p2k5[2500];
    moq_bytes_t near_parts[1] = { { p2k5, 2500 } };
    rq.ns = (moqr_ns_t){ near_parts, 1 };
    rq.name = (moq_bytes_t){ p2k5, 2500 };   /* 5000 > 4096 combined */
    MOQ_TEST_CHECK(moqr_core_subscribe(c, b, &rq, &sub) == MOQR_ERR_INVAL);

    /* Oversized name. */
    moq_bytes_t nsok[1] = { { (const uint8_t *)"n", 1 } };
    rq.ns = (moqr_ns_t){ nsok, 1 };
    rq.name = (moq_bytes_t){ big, (size_t)MOQ_FULL_TRACK_NAME_MAX + 1 };
    MOQ_TEST_CHECK(moqr_core_subscribe(c, b, &rq, &sub) == MOQR_ERR_INVAL);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("namespace_length_bounds");
    return failures;
}

static int
test_intent_ring_invariant(void)
{
    /* The intent ring is clamped so any single fan-out fits an empty ring:
     * many parked subscribers on one track ALL resolve, no WOULD_BLOCK,
     * none stranded — even when the configured ring is far smaller than
     * the subscriber pool. */
    int failures = 0;
    ca_t a;
    ca_init(&a);
    const uint32_t N = 100;
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_bindings = N + 2;
    cfg.max_subs = N;
    cfg.max_intents = 8;         /* deliberately tiny; must clamp up */
    cfg.log_budget.max_groups = 2;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);

    /* The ring was clamped to at least the subscriber pool. */
    moqr_core_limits_t lim;
    moqr_core_get_limits(c, &lim);
    MOQ_TEST_CHECK(lim.max_intents >= N);

    moqr_binding_t pubb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pubb) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pubb, NS2(nsb, "n", "s")) ==
                   MOQR_OK);
    moqr_intent_t its[256];

    /* Park N subscribers (distinct bindings) on one track. The first
     * subscribe creates the track and its single UPSTREAM_SUBSCRIBE; the
     * rest park with no new intents. */
    for (uint32_t i = 0; i < N; i++) {
        moqr_binding_t sb;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, i + 2, &sb) == MOQR_OK);
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = NS2(nsb, "n", "s");
        rq.name = B("t");
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 1000 + i;
        moqr_sub_t sh;
        MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq, &sh) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_sub_is_valid(sh));
    }
    /* Exactly one upstream subscribe for the whole parked set. */
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 256), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_track_t track = its[0].track;
    uint64_t tgen = its[0].track_gen;

    /* Upstream resolves: fan-out to ALL N parked subs in one atomic call
     * (no WOULD_BLOCK), each an ACCEPT. */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, tgen, 7, true, 3, 4) ==
                   MOQR_OK);
    size_t got = drain(c, its, 256);
    MOQ_TEST_CHECK_EQ_SIZE(got, (size_t)N);
    uint32_t accepts = 0;
    for (size_t i = 0; i < got; i++) {
        if (its[i].kind == MOQR_INTENT_ACCEPT_SUB) {
            accepts++;
        }
    }
    MOQ_TEST_CHECK_EQ_U64(accepts, N);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs, N);   /* none stranded */
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);

    /* Same for the error path: N parked subs all rejected atomically. */
    ca_init(&a);
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_bindings = N + 2;
    cfg.max_subs = N;
    cfg.max_intents = 8;
    cfg.log_budget.max_groups = 2;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pubb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pubb, NS2(nsb, "n", "s")) ==
                   MOQR_OK);
    for (uint32_t i = 0; i < N; i++) {
        moqr_binding_t sb;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, i + 2, &sb) == MOQR_OK);
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = NS2(nsb, "n", "s");
        rq.name = B("t");
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 1000 + i;
        moqr_sub_t sh;
        MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq, &sh) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 256), (size_t)1);
    track = its[0].track;
    tgen = its[0].track_gen;
    MOQ_TEST_CHECK(moqr_core_upstream_error(c, track, tgen, 0x10, CTRL_NOW) ==
                   MOQR_OK);
    got = drain(c, its, 256);
    uint32_t rejects = 0;
    for (size_t i = 0; i < got; i++) {
        if (its[i].kind == MOQR_INTENT_REJECT_SUB) {
            rejects++;
        }
    }
    MOQ_TEST_CHECK_EQ_U64(rejects, N);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);

    MOQ_TEST_PASS("intent_ring_invariant");
    return failures;
}

static int
test_multi_namespace_close(void)
{
    /* A single binding announces M namespaces under a common prefix, with
     * K namespace subscribers matching all of them. Closing that binding
     * must deliver ALL M*K NS_GONE withdrawals — the total fan-out (M*K)
     * exceeds the intent ring (clamped to the largest single announce's
     * fan-out, not the product), so the close must be resumable: deliver
     * what fits, return WOULD_BLOCK, retry after draining. It must never be
     * a permanent WOULD_BLOCK. */
    int failures = 0;
    ca_t a;
    ca_init(&a);
    const uint32_t M = 6;   /* namespaces the publisher announces */
    const uint32_t K = 2;   /* watchers matching them all         */
    /* Caps chosen so the ring clamps to 8 (max_ns_nodes = root + "p" + M
     * leaves) while the atomic close fan-out is M*K = 12 > 8. Under an
     * all-or-nothing close this is a permanent WOULD_BLOCK. */
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_subs = 2;
    cfg.max_ns_subs = K;
    cfg.max_ns_nodes = 2 + M;   /* root + "p" + M leaves */
    cfg.max_intents = 1;        /* clamps up to 8, still < M*K */
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);

    moqr_binding_t pubb, wb[2];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pubb) == MOQR_OK);
    /* K watchers subscribe the shared prefix {"p"}. */
    moq_bytes_t pfx[1];
    pfx[0] = B("p");
    for (uint32_t k = 0; k < K; k++) {
        MOQ_TEST_CHECK(moqr_core_binding_open(c, k + 2, &wb[k]) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_ns_subscribe(
                           c, wb[k], (moqr_ns_t){ pfx, 1 }, 500 + k) ==
                       MOQR_OK);
    }
    moqr_intent_t its[64];
    (void)drain(c, its, 64);

    /* Publisher announces M distinct namespaces {"p","aK"} — each matches
     * both watchers, so each announce fans out K NS_FOUND. */
    char names[16][8];
    for (uint32_t m = 0; m < M; m++) {
        snprintf(names[m], sizeof(names[m]), "a%u", m);
        moq_bytes_t parts[2];
        parts[0] = B("p");
        parts[1] = B(names[m]);
        MOQ_TEST_CHECK(moqr_core_announce(c, pubb,
                                          (moqr_ns_t){ parts, 2 }) ==
                       MOQR_OK);
        (void)drain(c, its, 64);   /* K NS_FOUND each; drop them */
    }

    /* Close the publisher. A single call may not fit all M*K NS_GONE, so
     * drive close-then-drain until it completes, collecting the GONEs. */
    uint32_t gones = 0;
    int guard = 0;
    for (;;) {
        moqr_result_t rc = moqr_core_binding_close(c, pubb, 100);
        size_t n = drain(c, its, 64);
        for (size_t i = 0; i < n; i++) {
            if (its[i].kind == MOQR_INTENT_NS_GONE) {
                gones++;
            }
        }
        if (rc == MOQR_OK) {
            break;
        }
        MOQ_TEST_CHECK(rc == MOQR_ERR_WOULD_BLOCK);
        if (++guard > 100) {
            MOQ_TEST_CHECK(0);   /* never completed: stuck */
            break;
        }
    }
    /* Every withdrawal was delivered — none suppressed. */
    MOQ_TEST_CHECK_EQ_U64(gones, M * K);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("multi_namespace_close");
    return failures;
}

/* The observability counters reflect real core activity: admission refusals
 * are tagged by resource, retention evictions accumulate monotonically, and
 * the intent-ring high-water tracks fan-out pressure. These are what the
 * metrics exporter renders, so prove they move with the core, not just that
 * they format. */
static int
test_observability_counters(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_ns_subs = 1;              /* one slot: the 2nd ns-sub refuses  */
    cfg.log_budget.max_groups = 2;    /* a 3rd group evicts the oldest     */
    cfg.log_budget.max_bytes = 1 << 20;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);

    moqr_intent_t its[16];
    moq_bytes_t nsb[2];

    /* Publisher announces and opens a track. */
    moqr_binding_t pub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"),
                                          B("t"), 5, &track) == MOQR_OK);
    (void)drain(c, its, 16);

    /* Eviction: three groups into a two-group log leaves one evicted. */
    for (uint64_t g = 0; g < 3; g++) {
        MOQ_TEST_CHECK(ing(c, &a, track, g, 0, 0, 1) == MOQR_OK);
    }

    /* Watcher subscribes a prefix; a later matching announce fans out one
     * NS_FOUND, lifting the intent high-water. */
    moqr_binding_t watch;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &watch) == MOQR_OK);
    moq_bytes_t pfx[1];
    pfx[0] = B("n");
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, watch, (moqr_ns_t){ pfx, 1 },
                                          40) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "x")) == MOQR_OK);

    /* Refusal: the single ns-sub slot is taken, so a second refuses and is
     * tagged NS_SUBS. */
    moq_bytes_t pfx2[1];
    pfx2[0] = B("p");
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, watch, (moqr_ns_t){ pfx2, 1 },
                                          41) == MOQR_ERR_CAPACITY);
    (void)drain(c, its, 16);

    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 3);
    MOQ_TEST_CHECK(st.evicted_total >= 1);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_NS_SUBS], 1);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_TRACKS], 0);
    MOQ_TEST_CHECK(st.intent_highwater >= 1);
    /* The name maps to a stable snake token for the metric label. */
    MOQ_TEST_CHECK(strcmp(moqr_refuse_reason_name(MOQR_REFUSE_NS_SUBS),
                          "ns_subs") == 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("observability_counters");
    return failures;
}

/* A misbehaving hook that writes out-of-range values must never leak them:
 * moqr_core_authorize canonicalizes to a fail-closed DENY/POLICY, and the
 * allow-all (no hook) path is a clean ALLOW. */
static void
bad_verdict_hook(void *ctx, const moqr_auth_request_t *req,
                 moqr_auth_verdict_t *out)
{
    (void)ctx;
    (void)req;
    out->decision = 99u;   /* invalid */
    out->reason = 77u;     /* invalid */
    out->revalidate_after_us = 0;
    out->error_code = 0;
    out->ticket = 0;
}

static int
test_auth_canonical_verdict(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.authorize = bad_verdict_hook;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);

    moqr_auth_request_t req;
    memset(&req, 0, sizeof(req));
    req.struct_size = sizeof(req);
    req.action = MOQR_AUTH_SUBSCRIBE;
    req.binding_cookie = 7;
    moqr_auth_verdict_t v;
    memset(&v, 0, sizeof(v));
    moqr_core_authorize(c, &req, &v);
    MOQ_TEST_CHECK_EQ_U64(v.decision, MOQR_AUTH_DENY);
    MOQ_TEST_CHECK_EQ_U64(v.reason, MOQR_AUTH_REASON_POLICY);

    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(
        st.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_DENY], 1);
    MOQ_TEST_CHECK_EQ_U64(st.auth_denials[MOQR_AUTH_REASON_POLICY], 1);
    moqr_core_destroy(c);

    /* Allow-all default (no hook) is a clean, counted ALLOW. */
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    memset(&v, 0, sizeof(v));
    moqr_core_authorize(c, &req, &v);
    MOQ_TEST_CHECK_EQ_U64(v.decision, MOQR_AUTH_ALLOW);
    MOQ_TEST_CHECK_EQ_U64(v.reason, MOQR_AUTH_REASON_OK);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(
        st.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_ALLOW], 1);
    MOQ_TEST_CHECK(strcmp(moqr_auth_action_name(MOQR_AUTH_TRACK_STATUS),
                          "track_status") == 0);
    moqr_core_destroy(c);

    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("auth_canonical_verdict");
    return failures;
}

/* Core-level parked (DEFER) storage: deep-copy, ticket rules, capacity,
 * retire, and stale-after-finish. The binding-driven end-to-end resume/reject
 * is covered in the session-binding suite. */
static int
test_parked_storage(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.max_parked = 2;      /* small to exercise slot exhaustion       */
    cfg.parked_bytes = 4096;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);

    moq_bytes_t       nsb[2];
    uint8_t           tokval[4] = { 0xAA, 0xBB, 0xCC, 0xDD };
    moqr_auth_token_t toks[1];
    toks[0].token_type = 7;
    toks[0].token_value.data = tokval;
    toks[0].token_value.len = 4;

    moqr_park_req_t req;
    memset(&req, 0, sizeof(req));
    req.action = MOQR_AUTH_SUBSCRIBE;
    req.binding_cookie = 100;
    req.session_cookie = 555;
    req.ns = NS2(nsb, "live", "cam");
    req.name = B("video");
    req.tokens = toks;
    req.token_count = 1;
    req.sub_filter_type = 2;
    req.sub_start_group = 9;

    /* Zero ticket fails closed; a real park then succeeds. */
    MOQ_TEST_CHECK(moqr_core_park(c, &req, 0, 1) == MOQR_ERR_INVAL);

    /* Malformed borrowed views fail closed (INVAL), no slot consumed. */
    {
        moqr_park_req_t   bad;
        moq_bytes_t       nullpart[1] = { { NULL, 4 } };  /* len>0, data NULL */
        moqr_auth_token_t nulltok[1];
        moq_bytes_t       huge[2];
        nulltok[0].token_type = 1;
        nulltok[0].token_value.data = NULL;
        nulltok[0].token_value.len = 4;
        huge[0].data = (const uint8_t *)"x";
        huge[0].len = SIZE_MAX - 1;
        huge[1].data = (const uint8_t *)"y";
        huge[1].len = 8;

        bad = req;
        bad.ns.parts = NULL;
        bad.ns.count = 1;
        MOQ_TEST_CHECK(moqr_core_park(c, &bad, 77, 1) == MOQR_ERR_INVAL);
        bad = req;
        bad.ns.parts = nullpart;
        bad.ns.count = 1;
        MOQ_TEST_CHECK(moqr_core_park(c, &bad, 77, 1) == MOQR_ERR_INVAL);
        bad = req;
        bad.name.data = NULL;
        bad.name.len = 3;
        MOQ_TEST_CHECK(moqr_core_park(c, &bad, 77, 1) == MOQR_ERR_INVAL);
        bad = req;
        bad.tokens = NULL;
        bad.token_count = 1;
        MOQ_TEST_CHECK(moqr_core_park(c, &bad, 77, 1) == MOQR_ERR_INVAL);
        bad = req;
        bad.tokens = nulltok;
        bad.token_count = 1;
        MOQ_TEST_CHECK(moqr_core_park(c, &bad, 77, 1) == MOQR_ERR_INVAL);
        bad = req;
        bad.ns.parts = huge;
        bad.ns.count = 2;   /* summed length overflows */
        MOQ_TEST_CHECK(moqr_core_park(c, &bad, 77, 1) == MOQR_ERR_INVAL);
        bad = req;
        bad.ns.count = 33;  /* > moqr_ns_t's 0..32 (uint32_t store) */
        MOQ_TEST_CHECK(moqr_core_park(c, &bad, 77, 1) == MOQR_ERR_INVAL);
        bad = req;
        bad.tokens = toks;                           /* a VALID 1-element array */
        bad.token_count = MOQR_AUTH_MAX_TOKENS + 1;  /* > cap: reject before the
                                                      * walk would read OOB     */
        MOQ_TEST_CHECK(moqr_core_park(c, &bad, 77, 1) == MOQR_ERR_INVAL);
    }

    MOQ_TEST_CHECK(moqr_core_park(c, &req, 42, 1) == MOQR_OK);
    /* Duplicate live ticket fails closed (never overwrite/merge). */
    MOQ_TEST_CHECK(moqr_core_park(c, &req, 42, 1) == MOQR_ERR_STALE_HANDLE);

    /* Corrupt the SOURCE bytes: a correct deep copy is unaffected. */
    nsb[0] = B("XXXX");
    tokval[0] = 0x00;

    moqr_park_req_t     view;
    moqr_auth_verdict_t vd;
    MOQ_TEST_CHECK(moqr_core_auth_begin_resolve(
                       c, 42,
                       &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_ALLOW,
                                               .reason = MOQR_AUTH_REASON_OK },
                       1, &vd, &view) == MOQR_OK);
    MOQ_TEST_CHECK(vd.decision == MOQR_AUTH_ALLOW &&
                   vd.reason == MOQR_AUTH_REASON_OK);   /* canonical verdict */
    MOQ_TEST_CHECK(view.action == MOQR_AUTH_SUBSCRIBE);
    MOQ_TEST_CHECK(view.binding_cookie == 100 && view.session_cookie == 555);
    MOQ_TEST_CHECK(view.ns.count == 2);
    MOQ_TEST_CHECK(view.ns.parts[0].len == 4 &&
                   memcmp(view.ns.parts[0].data, "live", 4) == 0);
    MOQ_TEST_CHECK(view.name.len == 5 &&
                   memcmp(view.name.data, "video", 5) == 0);
    MOQ_TEST_CHECK(view.token_count == 1 && view.tokens[0].token_type == 7);
    MOQ_TEST_CHECK(view.tokens[0].token_value.len == 4 &&
                   view.tokens[0].token_value.data[0] == 0xAA);
    MOQ_TEST_CHECK(view.sub_filter_type == 2 && view.sub_start_group == 9);

    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK(st.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_ALLOW] == 1);

    /* Retire while pinned must NOT free the entry — the begin/finish view stays
     * stable until finish (retire-pending). */
    moqr_core_retire_parked(c, 100);   /* binding 100 owns ticket 42 */
    MOQ_TEST_CHECK(view.ns.parts[0].len == 4 &&
                   memcmp(view.ns.parts[0].data, "live", 4) == 0);
    MOQ_TEST_CHECK(view.tokens[0].token_value.data[0] == 0xAA);

    /* Finish frees the (retired) slot; the ticket is now stale (no mutation on
     * retry), and the canonical verdict fails closed to DENY. */
    moqr_core_auth_finish_resolve(c, 42);
    MOQ_TEST_CHECK(moqr_core_auth_begin_resolve(
                       c, 42,
                       &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_ALLOW },
                       1, &vd, &view) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(vd.decision == MOQR_AUTH_DENY);

    /* Slot cap: fill both, third park fails closed. */
    MOQ_TEST_CHECK(moqr_core_park(c, &req, 1, 1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_park(c, &req, 2, 1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_park(c, &req, 3, 1) == MOQR_ERR_CAPACITY);

    /* Retire binding 100 clears both; the tickets go stale, slots reclaim. */
    moqr_core_retire_parked(c, 100);
    MOQ_TEST_CHECK(moqr_core_auth_begin_resolve(
                       c, 1, &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_DENY,
                                                     .reason =
                                                         MOQR_AUTH_REASON_POLICY },
                       1, &vd, &view) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_park(c, &req, 4, 1) == MOQR_OK);

    /* A DENY resolution: canonical verdict is DENY + reason + the verifier's
     * wire error_code (so the binding rejects with that wire code), and it
     * counts a denial by reason. */
    MOQ_TEST_CHECK(moqr_core_auth_begin_resolve(
                       c, 4,
                       &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_DENY,
                                               .reason = MOQR_AUTH_REASON_UNSCOPED,
                                               .error_code = 0x5 },
                       1, &vd, &view) == MOQR_OK);
    MOQ_TEST_CHECK(vd.decision == MOQR_AUTH_DENY &&
                   vd.reason == MOQR_AUTH_REASON_UNSCOPED);
    MOQ_TEST_CHECK(vd.error_code == 0x5);   /* wire code carried through */
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK(st.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_DENY] >= 1);
    MOQ_TEST_CHECK(st.auth_denials[MOQR_AUTH_REASON_UNSCOPED] >= 1);
    moqr_core_auth_finish_resolve(c, 4);

    /* A DEFER or garbage completion is clamped to a canonical DENY (no
     * recursive parking): the binding, which branches on the returned verdict,
     * therefore rejects. */
    MOQ_TEST_CHECK(moqr_core_park(c, &req, 5, 1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_auth_begin_resolve(
                       c, 5,
                       &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_DEFER },
                       1, &vd, &view) == MOQR_OK);
    MOQ_TEST_CHECK(vd.decision == MOQR_AUTH_DENY);
    moqr_core_auth_finish_resolve(c, 5);
    MOQ_TEST_CHECK(moqr_core_park(c, &req, 6, 1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_auth_begin_resolve(
                       c, 6,
                       &(moqr_auth_verdict_t){ .decision =
                                                   (moqr_auth_decision_t)99u },
                       1, &vd, &view) == MOQR_OK);
    MOQ_TEST_CHECK(vd.decision == MOQR_AUTH_DENY);
    moqr_core_auth_finish_resolve(c, 6);

    /* Zero-length namespace part / token value must not do NULL + off pointer
     * arithmetic (UBSan flags it without the fix); the view returns {NULL, 0}. */
    {
        moq_bytes_t       ep[1] = { { NULL, 0 } };
        moqr_auth_token_t et[1];
        et[0].token_type = 3;
        et[0].token_value.data = NULL;
        et[0].token_value.len = 0;
        moqr_park_req_t z = req;
        z.ns.parts = ep;
        z.ns.count = 1;
        z.name.data = NULL;
        z.name.len = 0;
        z.tokens = et;
        z.token_count = 1;
        MOQ_TEST_CHECK(moqr_core_park(c, &z, 8, 1) == MOQR_OK);
        moqr_park_req_t zv;
        MOQ_TEST_CHECK(moqr_core_auth_begin_resolve(
                           c, 8,
                           &(moqr_auth_verdict_t){ .decision = MOQR_AUTH_ALLOW },
                           1, &vd, &zv) == MOQR_OK);
        MOQ_TEST_CHECK(zv.ns.count == 1 && zv.ns.parts[0].len == 0 &&
                       zv.ns.parts[0].data == NULL);
        MOQ_TEST_CHECK(zv.token_count == 1 &&
                       zv.tokens[0].token_value.len == 0 &&
                       zv.tokens[0].token_value.data == NULL);
        moqr_core_auth_finish_resolve(c, 8);
    }

    moqr_core_destroy(c);

    /* parked_bytes cap: a single oversized park fails closed. */
    moqr_core_relay_cfg_t tiny;
    moqr_core_relay_cfg_init_sized(&tiny, sizeof(tiny), &a.vt);
    tiny.log_budget.max_groups = 4;
    tiny.log_budget.max_bytes = 1 << 20;
    tiny.max_parked = 8;
    tiny.parked_bytes = 16;   /* smaller than one copied request         */
    moqr_core_t *tc = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&tiny, &tc) == MOQR_OK);
    nsb[0] = B("live");       /* restore for the copy sizing              */
    MOQ_TEST_CHECK(moqr_core_park(tc, &req, 9, 1) == MOQR_ERR_CAPACITY);
    moqr_core_destroy(tc);

    MOQ_TEST_CHECK(a.live == 0);   /* every deep copy freed */
    MOQ_TEST_PASS("parked_storage");
    return failures;
}

/* Revalidation grant storage: add for the two grantable actions, lease/action
 * guards, slot + byte capacity fail-closed, and retire. Deep-copy correctness
 * is inherited from the shared r_copy_material helper (exercised under ASan by
 * test_parked_storage) and re-proven end-to-end at the revalidation tick. */
static int
test_grant_storage(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.max_grants = 2;
    cfg.grant_bytes = 4096;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);

    moq_bytes_t     nsb[2];
    moqr_park_req_t req;
    memset(&req, 0, sizeof(req));
    req.action = MOQR_AUTH_SUBSCRIBE;
    req.binding_cookie = 100;
    req.session_cookie = 555;
    req.ns = NS2(nsb, "live", "cam");
    req.name = B("video");

    moqr_grant_res_t r1, r2, r3;
    /* Lease 0 -> INVAL. */
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &req, 0, 1, &r1) ==
                   MOQR_ERR_INVAL);
    /* Non-grantable action -> UNSUPPORTED. */
    req.action = MOQR_AUTH_TRACK_STATUS;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &req, 1000, 1, &r1) ==
                   MOQR_ERR_UNSUPPORTED);

    /* Reserve subscribe + announce; the two slots fill. */
    req.action = MOQR_AUTH_SUBSCRIBE;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &req, 1000, 1, &r1) == MOQR_OK);
    req.action = MOQR_AUTH_PUBLISH_NAMESPACE;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &req, 1000, 1, &r2) == MOQR_OK);
    /* Slots full -> CAPACITY (reservation happens BEFORE the op). */
    req.action = MOQR_AUTH_SUBSCRIBE;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &req, 1000, 1, &r3) ==
                   MOQR_ERR_CAPACITY);

    /* A subscribe grant must commit with a real sub handle, not 0. */
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, r1, 0) == MOQR_ERR_INVAL);
    /* Commit both (announce uses 0); a double-commit is a stale misuse. */
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, r1, 42) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, r2, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, r1, 42) == MOQR_ERR_STALE_HANDLE);

    /* Retire frees both; reserve + abort round-trips a slot (op-failed path). */
    moqr_core_retire_grants(c, 100);
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &req, 1000, 1, &r3) == MOQR_OK);
    moqr_core_grant_abort(c, r3);
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &req, 1000, 1, &r3) == MOQR_OK);

    /* A stale reservation: commit fails, abort is a no-op. */
    moqr_grant_res_t bad = { 0, 0 };
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, bad, 0) == MOQR_ERR_STALE_HANDLE);
    moqr_core_grant_abort(c, bad);
    moqr_core_destroy(c);

    /* Byte cap: reserve of an oversized grant fails closed with no state. */
    moqr_core_relay_cfg_t tiny;
    moqr_core_relay_cfg_init_sized(&tiny, sizeof(tiny), &a.vt);
    tiny.log_budget.max_groups = 4;
    tiny.log_budget.max_bytes = 1 << 20;
    tiny.max_grants = 8;
    tiny.grant_bytes = 16;
    moqr_core_t *tc = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&tiny, &tc) == MOQR_OK);
    req.action = MOQR_AUTH_SUBSCRIBE;
    moqr_grant_res_t rt;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(tc, &req, 1000, 1, &rt) ==
                   MOQR_ERR_CAPACITY);
    moqr_core_destroy(tc);

    MOQ_TEST_CHECK(a.live == 0); /* every grant copy freed */
    MOQ_TEST_PASS("grant_storage");
    return failures;
}

/* Relay-initiated subscription revoke tells the subscriber on the wire: it
 * queues MOQR_INTENT_SUB_DONE (which the binding maps to done_subscribe),
 * carrying the downstream cookie + status. Plain unsubscribe does NOT — that
 * is exactly why auth revocation must use revoke, not unsubscribe. */
static int
test_revoke_sub(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sb) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"), B("t"), 5,
                                          &track) == MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 777;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq, &sub) == MOQR_OK);
    moqr_intent_t its[16];
    (void)moqr_core_poll_intents(c, its, 16); /* drain accept/upstream */

    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, sub, pd_local(MOQR_PD_UNAUTHORIZED), 1) == MOQR_OK);
    size_t n = moqr_core_poll_intents(c, its, 16);
    bool done = false;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_SUB_DONE && its[i].cookie == 777 &&
            pd_wire_for(its[i].pd, MOQ_VERSION_DRAFT_16) == 0x1u) {
            done = true;
        }
    }
    MOQ_TEST_CHECK(done); /* wire SUBSCRIBE_DONE (unsubscribe would deliver none) */
    /* The sub is now retired: a second revoke is stale. */
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, sub, pd_local(MOQR_PD_UNAUTHORIZED), 1) == MOQR_ERR_STALE_HANDLE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK(a.live == 0);
    MOQ_TEST_PASS("revoke_sub");
    return failures;
}

/* A controllable revalidation hook: returns g_reval_decision with g_reval_lease,
 * counting calls and recording the first token byte it saw (borrow-lifetime). */
static moqr_auth_decision_t g_reval_decision = MOQR_AUTH_ALLOW;
static uint64_t             g_reval_lease = 0;
static int                  g_reval_calls = 0;
static uint8_t              g_reval_token0 = 0;

static void
reval_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out)
{
    (void)ctx;
    g_reval_calls++;
    if (req->token_count > 0 && req->tokens[0].token_value.len > 0) {
        g_reval_token0 = req->tokens[0].token_value.data[0];
    }
    out->decision = g_reval_decision;
    out->revalidate_after_us = g_reval_lease;
    if (g_reval_decision == MOQR_AUTH_DENY) {
        out->error_code = 0x9u;
    }
}

/* Subscribe grant: scheduled recheck (not-due no-op), ALLOW reschedules while
 * seeing the COPIED token (not a corrupted source), and DENY revokes with a
 * wire SUBSCRIBE_DONE. */
static int
test_grant_reval(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.authorize = reval_hook;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);

    moqr_binding_t pub, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sb) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"), B("t"), 5,
                                          &track) == MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 777;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq, &sub) == MOQR_OK);
    moqr_intent_t its[16];
    (void)moqr_core_poll_intents(c, its, 16);

    /* Grant with a lease, committed at now=10, carrying one token. */
    uint8_t          tv[1] = { 0xAB };
    moqr_auth_token_t tk[1];
    tk[0].token_type = 5;
    tk[0].token_value.data = tv;
    tk[0].token_value.len = 1;
    moqr_park_req_t gr;
    memset(&gr, 0, sizeof(gr));
    gr.action = MOQR_AUTH_SUBSCRIBE;
    gr.binding_cookie = 200;
    gr.session_cookie = 777;
    gr.ns = NS2(nsb, "n", "s");
    gr.name = B("t");
    gr.tokens = tk;
    gr.token_count = 1;
    moqr_grant_res_t res;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr, 1000, 10, &res) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, sub._opaque) == MOQR_OK);
    tv[0] = 0x00; /* corrupt the SOURCE after commit (borrow-lifetime) */

    g_reval_decision = MOQR_AUTH_ALLOW;
    g_reval_lease = 1000;
    g_reval_calls = 0;
    /* Not due (now 500 < 10+1000). */
    MOQ_TEST_CHECK(moqr_core_tick(c, 500) == MOQR_OK);
    MOQ_TEST_CHECK(g_reval_calls == 0);
    /* Due: recheck ALLOW -> reschedule, and it saw the COPIED token. */
    MOQ_TEST_CHECK(moqr_core_tick(c, 2000) == MOQR_OK);
    MOQ_TEST_CHECK(g_reval_calls == 1);
    MOQ_TEST_CHECK(g_reval_token0 == 0xAB); /* copy, not the corrupted source */
    /* Rescheduled to 2000+1000: not due at 2500. */
    g_reval_calls = 0;
    MOQ_TEST_CHECK(moqr_core_tick(c, 2500) == MOQR_OK);
    MOQ_TEST_CHECK(g_reval_calls == 0);

    /* DENY at the next due: revoke -> wire SUBSCRIBE_DONE carrying the hook's
     * CUSTOM denial code, grant gone. 0x9 is draft-18's EXCESSIVE_LOAD, so it
     * is NOT a usable extension number — a custom code may only be one both
     * drafts leave unassigned. The denial therefore degrades to the truthful
     * UNAUTHORIZED meaning rather than asserting EXCESSIVE_LOAD to a d18
     * subscriber. */
    g_reval_decision = MOQR_AUTH_DENY;
    MOQ_TEST_CHECK(moqr_core_tick(c, 3500) == MOQR_OK);
    size_t n = moqr_core_poll_intents(c, its, 16);
    bool done = false;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_SUB_DONE && its[i].cookie == 777) {
            done = true;
            /* Not 0x9: a registered code in either draft is refused as an
             * extension and falls back to UNAUTHORIZED. */
            uint64_t w16 = 0, w18 = 0;
            MOQ_TEST_CHECK(moqr_pd_desc_emit(its[i].pd, MOQ_VERSION_DRAFT_16,
                                             &w16) == MOQR_OK);
            MOQ_TEST_CHECK(moqr_pd_desc_emit(its[i].pd, MOQ_VERSION_DRAFT_18,
                                             &w18) == MOQR_OK);
            MOQ_TEST_CHECK_EQ_U64(w16, 0x1u);
            MOQ_TEST_CHECK_EQ_U64(w18, 0x1u);
        }
    }
    MOQ_TEST_CHECK(done);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK(st.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_ALLOW] >= 1);
    MOQ_TEST_CHECK(st.auth_decisions[MOQR_AUTH_SUBSCRIBE][MOQR_AUTH_DENY] >= 1);
    /* Grant gone: a further tick does not recheck. */
    g_reval_calls = 0;
    MOQ_TEST_CHECK(moqr_core_tick(c, 9000) == MOQR_OK);
    MOQ_TEST_CHECK(g_reval_calls == 0);

    /* A DEFER revalidation carries no error_code (the hook leaves it 0). The
     * core must default it to UNAUTHORIZED (0x1) so the wire SUBSCRIBE_DONE
     * never reads as a normal "range complete" (status 0). Fresh sub + grant. */
    moqr_subscribe_req_t rq2 = rq;
    rq2.cookie = 778;
    moqr_sub_t sub2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq2, &sub2) == MOQR_OK);
    (void)moqr_core_poll_intents(c, its, 16);
    moqr_park_req_t gr2 = gr; /* same ns / name / tokens */
    gr2.session_cookie = 778;
    moqr_grant_res_t res2;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr2, 1000, 9000, &res2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res2, sub2._opaque) == MOQR_OK);
    g_reval_decision = MOQR_AUTH_DEFER;
    MOQ_TEST_CHECK(moqr_core_tick(c, 11000) == MOQR_OK);
    n = moqr_core_poll_intents(c, its, 16);
    bool done2 = false;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_SUB_DONE && its[i].cookie == 778) {
            done2 = true;
            /* UNAUTHORIZED default, in both drafts' numbering. */
            MOQ_TEST_CHECK_EQ_U64(pd_wire_for(its[i].pd,
                                              MOQ_VERSION_DRAFT_16), 0x1u);
            MOQ_TEST_CHECK_EQ_U64(pd_wire_for(its[i].pd,
                                              MOQ_VERSION_DRAFT_18), 0x1u);
        }
    }
    MOQ_TEST_CHECK(done2);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK(a.live == 0);
    MOQ_TEST_PASS("grant_reval");
    return failures;
}

/* An announce grant whose revocation cannot finish in one tick. The teardown
 * runs force_withdraw, which is resumable: a short intent ring stops it with
 * some of the namespace's tracks already purged. The grant must therefore
 * SURVIVE that tick and resume — dropping it as though nothing had happened
 * would strand a half-purged namespace with its announce still installed and
 * no publisher cancel ever sent. */
static int
test_grant_teardown_resumes_after_partial(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.authorize = reval_hook;
    cfg.max_subs = 2;
    cfg.max_tracks = 2;
    cfg.max_ns_nodes = 4;
    cfg.max_ns_subs = 1;
    cfg.max_intents = 2;   /* clamped up to max_subs + 1 == 3 */
    moqr_core_t *c = NULL;

    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    if (c == NULL) {
        return failures;
    }
    moqr_binding_t pb, sb;

    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sb) == MOQR_OK);
    moq_bytes_t nsb[2];

    MOQ_TEST_CHECK(moqr_core_announce_ex(c, pb, NS2(nsb, "n", "s"), 888) ==
                   MOQR_OK);

    /* Two sourced tracks, one parked subscriber each: two intents per unit,
     * so the ring of 3 fits exactly one unit. */
    moqr_intent_t its[16];

    for (int k = 0; k < 2; k++) {
        moqr_subscribe_req_t rq;

        moqr_subscribe_req_init(&rq);
        rq.ns = NS2(nsb, "n", "s");
        rq.name = k == 0 ? B("v") : B("w");
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = (uint64_t)(k + 1);

        moqr_sub_t sh;

        MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq, &sh) == MOQR_OK);
        if (k == 0) {
            (void)moqr_core_poll_intents(c, its, 16);
        }
        /* The SECOND track's UPSTREAM_SUBSCRIBE is deliberately left on the
         * ring, so the withdrawal has room for exactly one track unit. */
    }

    moqr_park_req_t gr;

    memset(&gr, 0, sizeof(gr));
    gr.action = MOQR_AUTH_PUBLISH_NAMESPACE;
    gr.binding_cookie = 100;
    gr.session_cookie = 888;
    gr.ns = NS2(nsb, "n", "s");

    moqr_grant_res_t res;

    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr, 1000, 10, &res) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, 0) == MOQR_OK);

    /* The recheck denies: the teardown starts and cannot finish this tick. */
    g_reval_decision = MOQR_AUTH_DENY;
    g_reval_lease = 0;
    MOQ_TEST_CHECK(moqr_core_tick(c, 2000) == MOQR_OK);

    moqr_revoked_grant_t rv[4];
    size_t peeked = moqr_core_peek_revoked_grants(c, rv, 4);
    size_t n1 = moqr_core_poll_intents(c, its, 16);
    int rej1 = 0;

    for (size_t i = 0; i < n1; i++) {
        if (its[i].kind == MOQR_INTENT_REJECT_SUB) {
            rej1++;
        }
    }
    printf("ORACLE grant_partial tick1{rejects=%d peeked=%zu}\n", rej1,
           peeked);
    /* Partial: one track purged, no publisher cancel yet. */
    MOQ_TEST_CHECK_EQ_INT(rej1, 1);
    MOQ_TEST_CHECK_EQ_SIZE(peeked, (size_t)0);

    /* The grant SURVIVED: a later tick resumes and completes it. */
    MOQ_TEST_CHECK(moqr_core_tick(c, 3000) == MOQR_OK);
    size_t n2 = moqr_core_poll_intents(c, its, 16);
    int rej2 = 0;

    for (size_t i = 0; i < n2; i++) {
        if (its[i].kind == MOQR_INTENT_REJECT_SUB) {
            rej2++;
        }
    }
    peeked = moqr_core_peek_revoked_grants(c, rv, 4);
    printf("ORACLE grant_partial tick2{rejects=%d peeked=%zu}\n", rej2,
           peeked);
    MOQ_TEST_CHECK_EQ_INT(rej2, 1);            /* the remaining track only */
    MOQ_TEST_CHECK_EQ_SIZE(peeked, (size_t)1); /* the cancel, exactly once */
    MOQ_TEST_CHECK_EQ_U64(rv[0].session_cookie, 888u);

    moqr_core_ack_revoked_grant(c, 100, 888);
    MOQ_TEST_CHECK_EQ_SIZE(moqr_core_peek_revoked_grants(c, rv, 4), (size_t)0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK(a.live == 0);
    MOQ_TEST_PASS("grant_teardown_resumes_after_partial");
    return failures;
}

/* Announce grant: an ALLOW+lease0 recheck clears the grant; a DEFER recheck
 * fails closed -> the namespace is withdrawn and a cancel is drained. */
static int
test_grant_reval_announce(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.authorize = reval_hook;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t b;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &b) == MOQR_OK);
    moq_bytes_t nsb[2];

    moqr_park_req_t gr;
    memset(&gr, 0, sizeof(gr));
    gr.action = MOQR_AUTH_PUBLISH_NAMESPACE;
    gr.binding_cookie = 100;
    gr.session_cookie = 888;
    gr.ns = NS2(nsb, "n", "s");

    /* (1) ALLOW + lease 0 clears the grant with no revoke. announce_ex
     * records the wire announce handle (888) on the node — what the
     * production binding stores — so a revocation's withdrawal dedupes its
     * publisher cancel against this grant instead of minting a second. */
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, b, NS2(nsb, "n", "s"), 888) ==
                   MOQR_OK);
    moqr_grant_res_t res;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr, 1000, 10, &res) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, 0) == MOQR_OK);
    g_reval_decision = MOQR_AUTH_ALLOW;
    g_reval_lease = 0;
    MOQ_TEST_CHECK(moqr_core_tick(c, 2000) == MOQR_OK);
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 0); /* not revoked */
    moqr_core_stats_t st0;
    moqr_core_get_stats(c, &st0);
    MOQ_TEST_CHECK(st0.ns_nodes >= 1); /* still announced */

    /* (2) DEFER fails closed -> withdraw + a pending cancel. The DEFER verdict
     * carries no error_code, so the cancel code must default to UNAUTHORIZED
     * (0x1) rather than a zero "clean cancel". Peek is NON-draining: it keeps
     * returning the grant until ack frees it (this is what makes a WOULD_BLOCK
     * cancel retry-safe). */
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr, 1000, 3000, &res) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, 0) == MOQR_OK);
    g_reval_decision = MOQR_AUTH_DEFER;
    MOQ_TEST_CHECK(moqr_core_tick(c, 5000) == MOQR_OK);
    size_t n = moqr_core_peek_revoked_grants(c, rv, 4);
    MOQ_TEST_CHECK(n == 1 && rv[0].binding_cookie == 100 &&
                   rv[0].session_cookie == 888 &&
                   rv[0].error_code == 0x1u); /* UNAUTHORIZED default */
    /* Peek again (no ack): the grant is still there (retry survivability). */
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 1);
    moqr_core_ack_revoked_grant(c, 100, 888);
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 0); /* acked */

    /* (3) DENY with a custom code -> the custom code is peeked (preserved, not
     * overwritten by the UNAUTHORIZED default). Re-announce first. */
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, b, NS2(nsb, "n", "s"), 888) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr, 1000, 6000, &res) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, 0) == MOQR_OK);
    g_reval_decision = MOQR_AUTH_DENY; /* reval_hook sets error_code 0x9 */
    MOQ_TEST_CHECK(moqr_core_tick(c, 8000) == MOQR_OK);
    n = moqr_core_peek_revoked_grants(c, rv, 4);
    MOQ_TEST_CHECK(n == 1 && rv[0].binding_cookie == 100 &&
                   rv[0].session_cookie == 888 &&
                   rv[0].error_code == 0x9u); /* custom DENY code preserved */
    moqr_core_ack_revoked_grant(c, 100, 888);
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK(a.live == 0);
    MOQ_TEST_PASS("grant_reval_announce");
    return failures;
}

/* A STALE PUBLISH_NAMESPACE grant must never withdraw a REPLACEMENT
 * publisher's namespace. Two independent guards: (1) a voluntary
 * unannounce retires the announce's own grant (it never revalidates again
 * at all), and (2) a revoked grant's teardown verifies the announced node
 * still matches the grant's exact {binding_cookie, session_cookie} before
 * withdrawing anything — a mismatch retires the stale grant untouched. */
static int
test_grant_reval_stale_owner(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.authorize = reval_hook;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t ba, bb, bs;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &ba) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &bb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 300, &bs) == MOQR_OK);
    moq_bytes_t nsb[2];
    g_reval_decision = MOQR_AUTH_ALLOW;
    g_reval_lease = 1000;

    moqr_park_req_t gra;
    memset(&gra, 0, sizeof(gra));
    gra.action = MOQR_AUTH_PUBLISH_NAMESPACE;
    gra.binding_cookie = 100;
    gra.session_cookie = 888;
    gra.ns = NS2(nsb, "n", "s");

    /* (1) A announces with a lease, then VOLUNTARILY unannounces: the
     * grant retires with the announce — it never revalidates again (the
     * DENY below is never even consulted, so the decision counter for
     * PUBLISH_NAMESPACE DENY stays zero). */
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, ba, NS2(nsb, "n", "s"), 888) ==
                   MOQR_OK);
    moqr_grant_res_t res;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gra, 1000, 10, &res) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, ba, NS2(nsb, "n", "s")) ==
                   MOQR_OK);
    g_reval_decision = MOQR_AUTH_DENY;
    MOQ_TEST_CHECK(moqr_core_tick(c, 5000) == MOQR_OK);
    moqr_revoked_grant_t rv[4];
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 0);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK(
        st.auth_decisions[MOQR_AUTH_PUBLISH_NAMESPACE][MOQR_AUTH_DENY] == 0);

    /* B re-announces the SAME key (distinct handle) and sources a track,
     * with its own long-lease grant. */
    g_reval_decision = MOQR_AUTH_ALLOW;
    MOQ_TEST_CHECK(moqr_core_announce_ex(c, bb, NS2(nsb, "n", "s"), 999) ==
                   MOQR_OK);
    moqr_park_req_t grb = gra;
    grb.binding_cookie = 200;
    grb.session_cookie = 999;
    grb.ns = NS2(nsb, "n", "s");
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &grb, 1000000, 5500, &res) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, 0) == MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("v");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 7;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, bs, &rq, &sub) == MOQR_OK);
    moqr_intent_t its[8];
    MOQ_TEST_CHECK(drain(c, its, 8) == 1);   /* B's upstream subscribe */
    MOQ_TEST_CHECK(its[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    MOQ_TEST_CHECK(its[0].binding_cookie == 200);
    moqr_core_stats_t before;
    moqr_core_get_stats(c, &before);

    /* (2) The defensive guard: a manually committed STALE grant carrying
     * A's old identity reaches DENY against B's live announce. It must
     * retire itself and touch NOTHING — B's route, track, grant, and
     * cancellation state stay exactly as they were. */
    g_reval_decision = MOQR_AUTH_ALLOW;
    g_reval_lease = 1000;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gra, 1000, 6000, &res) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, 0) == MOQR_OK);
    g_reval_decision = MOQR_AUTH_DENY;
    MOQ_TEST_CHECK(moqr_core_tick(c, 8000) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 0);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK(st.tracks == before.tracks);       /* B's track lives   */
    MOQ_TEST_CHECK(st.subs == before.subs);           /* the sub lives     */
    MOQ_TEST_CHECK(st.ns_nodes == before.ns_nodes);
    MOQ_TEST_CHECK(drain(c, its, 8) == 0);            /* no purge intents  */
    /* The stale grant is gone, not looping: another due tick consults
     * nothing and still cancels nothing. */
    MOQ_TEST_CHECK(moqr_core_tick(c, 9000) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 0);
    /* B's announce still routes: a second subscribe reaches B upstream. */
    rq.name = B("w");
    rq.cookie = 8;
    moqr_sub_t sub2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, bs, &rq, &sub2) == MOQR_OK);
    MOQ_TEST_CHECK(drain(c, its, 8) == 1);
    MOQ_TEST_CHECK(its[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    MOQ_TEST_CHECK(its[0].binding_cookie == 200);

    moqr_core_destroy(c);
    g_reval_decision = MOQR_AUTH_ALLOW;
    g_reval_lease = 0;
    MOQ_TEST_CHECK(a.live == 0);
    MOQ_TEST_PASS("grant_reval_stale_owner");
    return failures;
}

/* A grant retired via moqr_core_retire_grants (what the binding's conn_detach
 * calls on connection close) is not revalidated on a later tick: the hook is
 * never re-invoked and no revoke fires, even when the hook would now DENY. */
static int
test_grant_retire_no_reval(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.authorize = reval_hook;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);

    moqr_binding_t pub, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &sb) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"), B("t"), 5,
                                          &track) == MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 555;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq, &sub) == MOQR_OK);
    moqr_intent_t its[16];
    (void)moqr_core_poll_intents(c, its, 16);

    /* Commit a subscribe grant on the subscriber's binding (cookie 200). */
    moqr_park_req_t gr;
    memset(&gr, 0, sizeof(gr));
    gr.action = MOQR_AUTH_SUBSCRIBE;
    gr.binding_cookie = 200;
    gr.session_cookie = 555;
    gr.ns = NS2(nsb, "n", "s");
    gr.name = B("t");
    moqr_grant_res_t res;
    MOQ_TEST_CHECK(moqr_core_grant_reserve(c, &gr, 1000, 10, &res) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_grant_commit(c, res, sub._opaque) == MOQR_OK);

    /* Retire the subscriber's grants (as binding close does), THEN flip the hook
     * to DENY and tick well past the lease: nothing may revalidate or revoke. */
    moqr_core_retire_grants(c, 200);
    g_reval_decision = MOQR_AUTH_DENY;
    g_reval_calls = 0;
    MOQ_TEST_CHECK(moqr_core_tick(c, 5000) == MOQR_OK);
    MOQ_TEST_CHECK(g_reval_calls == 0); /* retired: hook never re-invoked */
    size_t n = moqr_core_poll_intents(c, its, 16);
    bool done = false;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_SUB_DONE && its[i].cookie == 555) {
            done = true;
        }
    }
    MOQ_TEST_CHECK(!done); /* no wire teardown for the retired grant */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK(a.live == 0);
    MOQ_TEST_PASS("grant_retire_no_reval");
    return failures;
}

/* Announce demo/cam, subscribe from {0,0}, resolve upstream at largest (4,9);
 * returns the track and fills *out_sub. Deliveries land on sub_b. */
static moqr_track_t
chunked_sub_setup(moqr_core_t *c, moqr_binding_t pub, moqr_binding_t sub_b,
                  moqr_sub_t *out_sub)
{
    moq_bytes_t nsb[2];
    (void)moqr_core_announce(c, pub, NS2(nsb, "demo", "cam"));
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.name = B("video");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 11;
    (void)moqr_core_subscribe(c, sub_b, &rq, out_sub);
    moqr_intent_t its[16];
    size_t n = moqr_core_poll_intents(c, its, 16);
    moqr_track_t track;
    memset(&track, 0, sizeof(track));
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            track = its[i].track;
            (void)moqr_core_upstream_ok(c, track, its[i].track_gen, 777, true, 4,
                                        9);
        }
    }
    (void)moqr_core_poll_intents(c, its, 16);   /* ACCEPT_SUB */
    return track;
}

/* Subscribe `sub_b` to demo/cam `name` (namespace already announced) and resolve
 * its upstream at largest {4,9}; returns the track and fills *out_sub. Used to
 * put a SECOND subscription (a sibling to a different track) on the SAME binding
 * so binding-wide outstanding-delivery HOL can be exercised. */
static moqr_track_t
sub_on_binding(moqr_core_t *c, moqr_binding_t sub_b, moq_bytes_t name,
               uint64_t cookie, moqr_sub_t *out_sub)
{
    moq_bytes_t nsb[2];
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.name = name;
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = cookie;
    (void)moqr_core_subscribe(c, sub_b, &rq, out_sub);
    moqr_intent_t its[16];
    size_t n = moqr_core_poll_intents(c, its, 16);
    moqr_track_t track;
    memset(&track, 0, sizeof(track));
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            track = its[i].track;
            (void)moqr_core_upstream_ok(c, track, its[i].track_gen, 778, true, 4,
                                        9);
        }
    }
    (void)moqr_core_poll_intents(c, its, 16);   /* ACCEPT_SUB */
    return track;
}

/* Chunked COMPLETE delivery core mechanics: a chunked COMPLETE record is selected for delivery, its
 * chunks read in order via the accessor, a re-peek while held is idempotent, a
 * WOULD_BLOCK holds the record (keeps the pins, does NOT advance), and the
 * record advances only on DELIVERED. */
static int
test_chunked_delivery_core(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* Chunked COMPLETE record: group 5, object 0, three 20-byte chunks. */
    MOQ_TEST_CHECK(ing_chunked(c, &a, track, 5, 0, 0, 128, 3, 20, 0xA0) ==
                   MOQR_OK);

    /* Selected as chunked: chunk_count 3, declared 60, no single payload. */
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK_EQ_U64(d.rec.object_id, 0);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 3);
    MOQ_TEST_CHECK_EQ_U64(d.rec.declared_len, 60);
    MOQ_TEST_CHECK(d.rec.payload == NULL);

    /* Re-peek while held is idempotent: the same record, no advance. */
    moqr_delivery_t d2;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d2) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d2.rec.object_id, 0);
    MOQ_TEST_CHECK_EQ_U64(d2.rec.chunk_count, 3);

    /* Chunks read in order, exact length and bytes; one past the end is DONE. */
    for (uint32_t k = 0; k < 3; k++) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, k, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(cl, 20);
        MOQ_TEST_CHECK(cb != NULL &&
                       moq_rcbuf_data(cb)[0] == (uint8_t)(0xA0 + k));
    }
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 3, &cb, &cl) ==
                       MOQR_DONE);
    }

    /* WOULD_BLOCK holds: re-peek returns the SAME record (not advanced) and the
     * chunks stay pinned/readable. */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.object_id, 0);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 3);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 1, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK(cb != NULL && moq_rcbuf_data(cb)[0] == 0xA1);
    }

    /* DELIVERED advances: nothing more to deliver; counted once. */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 1);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("chunked_delivery_core");
    return failures;
}

/* The REFUSED_UNBEGUN confirm contract, pinned at the CORE boundary (the
 * integrated scenarios exercise the behavior; this pins the API): a
 * pre-begin refusal RELEASES a chunked outstanding delivery (re-derived,
 * never duplicated) and is whole-record-equivalent to WOULD_BLOCK — while
 * every provably-false pre-begin claim fails closed: a batch resuming at a
 * nonzero chunk index (the object began in an earlier batch), a recordless
 * notice, and an abandoned-reset are all INVAL and leave the outstanding
 * delivery held exactly as before. */
static int
test_refused_unbegun_contract(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);
    moqr_delivery_t d;
    moqr_core_stats_t st;

    /* 1. VALID initial refusal: a chunked COMPLETE record peeked with its
     * batch at chunk 0 releases on REFUSED_UNBEGUN — the SAME record
     * re-derives on the next selection (nothing advanced, nothing counted)
     * and then delivers exactly once. */
    MOQ_TEST_CHECK(ing_chunked(c, &a, track, 5, 0, 0, 128, 2, 20, 0xA0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);
    MOQ_TEST_CHECK(moqr_core_delivery_done(
                       c, s1, MOQR_DELIVERY_REFUSED_UNBEGUN, CTRL_NOW) ==
                   MOQR_OK);
    /* released, not held: the pinned-batch accessor has no outstanding
     * delivery to read from */
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) !=
                       MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);   /* re-derived, base 0 */
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(cl == 20 && moq_rcbuf_data(cb)[0] == 0xA0);
    }
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 1);

    /* 2. WHOLE-record equivalence: REFUSED_UNBEGUN behaves exactly like
     * WOULD_BLOCK (release + re-derive + deliver once). */
    MOQ_TEST_CHECK(ing(c, &a, track, 6, 0, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 6);
    MOQ_TEST_CHECK(moqr_core_delivery_done(
                       c, s1, MOQR_DELIVERY_REFUSED_UNBEGUN, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 6);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* 3. PARTIALLY-EMITTED rejection: a batch that resumes at a nonzero
     * chunk index proves the object began earlier — REFUSED_UNBEGUN is
     * INVAL and the delivery stays held (WOULD_BLOCK semantics intact). */
    MOQ_TEST_CHECK(open_rec(c, track, 7, 0, 0, 40) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 7, 0, 0, 20, 0xC0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_OPEN);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 7, 0, 0, 20, 0xC1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_complete_record(c, track, 7, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);   /* batch resumes at 1 */
    MOQ_TEST_CHECK(moqr_core_delivery_done(
                       c, s1, MOQR_DELIVERY_REFUSED_UNBEGUN, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);   /* still held verbatim */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* 4. NOTICE rejection: a late SEAL notice (recordless) refuses
     * REFUSED_UNBEGUN, stays outstanding, and acknowledges normally. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 5, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK(moqr_core_delivery_done(
                       c, s1, MOQR_DELIVERY_REFUSED_UNBEGUN, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* 5. ABANDONED rejection: a begun-then-abandoned record's reset
     * delivery refuses REFUSED_UNBEGUN and still resets normally. */
    MOQ_TEST_CHECK(open_rec(c, track, 8, 0, 0, 60) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 8, 0, 0, 20, 0xD0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 8, 0, 0, rd_wire(0x77u)) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_ABANDONED);
    MOQ_TEST_CHECK(moqr_core_delivery_done(
                       c, s1, MOQR_DELIVERY_REFUSED_UNBEGUN, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                           CTRL_NOW) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("refused_unbegun_contract");
    return failures;
}

/* Forward pause releases a held ORDINARY-record delivery and clears
 * binding-wide HOL (corrections #2 + #3, deterministic at the core boundary).
 * Two subscriptions to different tracks share ONE binding. sub_v holds a
 * chunked delivery across WOULD_BLOCK, then pauses; the held delivery MUST be
 * released (cursor unadvanced) so the binding is not monopolized, sub_v MUST
 * NOT be retired, and its SIBLING sub_a (a different track) MUST still deliver
 * through the same binding. On resume sub_v re-derives the SAME record and
 * completes exactly once. Also asserts resume re-arms the owning binding
 * (production liveness). */
static int
test_forward_pause_release(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub_v, sub_a;
    moqr_track_t tv = chunked_sub_setup(c, pub, s1, &sub_v);   /* video */
    moqr_track_t ta = sub_on_binding(c, s1, B("audio"), 12, &sub_a);

    /* sub_v: chunked COMPLETE record; peek and HOLD across a WOULD_BLOCK. */
    MOQ_TEST_CHECK(ing_chunked(c, &a, tv, 5, 0, 0, 128, 3, 20, 0xA0) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 3);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 3);   /* held: re-peek idempotent */

    /* PAUSE sub_v: releases the held delivery (accessor now empty), sub_v
     * survives, and sub_v itself is no longer selectable. */
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub_v, false, CTRL_NOW) ==
                   MOQR_OK);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) != MOQR_OK);
    }
    moqr_sub_state_t st;
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub_v, &st) == MOQR_OK);   /* alive */

    /* Binding-wide HOL cleared: an object on the SIBLING track delivers through
     * the same binding even though sub_v's held delivery was dropped. */
    MOQ_TEST_CHECK(ing(c, &a, ta, 0, 0, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(d.sub._opaque == sub_a._opaque);   /* the sibling, not sub_v */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    /* nothing else selectable while sub_v stays paused */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);
    moqr_core_stats_t sbefore;
    moqr_core_get_stats(c, &sbefore);
    MOQ_TEST_CHECK_EQ_U64(sbefore.delivered_total, 1);   /* only the sibling */

    /* Drain stale ready marks so the resume re-arm is observed alone. */
    {
        uint64_t rk[8];
        while (moqr_core_drain_ready(c, rk, 8) > 0) {
        }
    }
    /* RESUME sub_v: re-arms the owning binding (a wake-driven pump only serves
     * marked bindings), then the released delivery re-derives and completes. */
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub_v, true, CTRL_NOW) ==
                   MOQR_OK);
    {
        uint64_t rk[8];
        uint32_t nr = moqr_core_drain_ready(c, rk, 8);
        bool armed = false;
        for (uint32_t i = 0; i < nr; i++) {
            if (rk[i] == 200) {
                armed = true;   /* s1's binding cookie */
            }
        }
        MOQ_TEST_CHECK(armed);
    }
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);   /* the SAME held record */
    MOQ_TEST_CHECK(d.sub._opaque == sub_v._opaque);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);
    moqr_core_stats_t safter;
    moqr_core_get_stats(c, &safter);
    MOQ_TEST_CHECK_EQ_U64(safter.delivered_total, 2);   /* each exactly once */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("forward_pause_release");
    return failures;
}

/* Pause with a PARTIAL held chunked delivery — normal completion path
 * (correction #1). The bind has emitted >=1 chunk (checkpointed via
 * moqr_core_delivery_note_emitted) before the downstream WOULD_BLOCK; the sub
 * then pauses. The partial's begun-downstream watermark MUST persist across the
 * release (so the object is known begun), and on resume the record re-derives
 * from the unadvanced cursor and completes exactly once. */
static int
test_forward_pause_partial_resume(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    MOQ_TEST_CHECK(ing_chunked(c, &a, track, 5, 0, 0, 128, 3, 20, 0xA0) ==
                   MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 3);
    /* Bind wrote chunk 0, then the write of chunk 1 blocked: it checkpoints the
     * exact emitted count (1) and holds. */
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, 1, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);

    /* PAUSE: releases the held partial; the sub survives and is unselectable. */
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub1, false, CTRL_NOW) ==
                   MOQR_OK);
    moqr_sub_state_t stt;
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub1, &stt) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    /* RESUME: the begun partial re-derives and completes exactly once. */
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub1, true, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 1);   /* exactly once */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("forward_pause_partial_resume");
    return failures;
}

/* Pause with a PARTIAL held chunked delivery — abandon-while-paused path
 * (corrections #1 + #2). The bind emitted >=1 chunk (checkpointed) before the
 * hold, the sub pauses, THEN the source abandons the group. Because the begun
 * watermark persisted, the required downstream RESET MUST still surface (an
 * ABANDONED delivery is stream maintenance, eligible while paused) rather than
 * being skipped as never-begun — the exact truncation hazard correction #1
 * closes. The subscription survives and the begun accounting is balanced.
 * RED: neuter moqr_core_delivery_note_emitted and the abandon is skipped
 * (watermark 0), so next_delivery returns DONE instead of the reset. */
static int
test_forward_pause_partial_abandon(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* An OPEN (not-yet-complete) chunked record with two chunks: the source can
     * still abandon it mid-object, which is the truncation hazard. */
    MOQ_TEST_CHECK(open_rec(c, track, 5, 0, 0, 40) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA0) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA1) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, 1, CTRL_NOW) ==
                   MOQR_OK);   /* bind emitted 1 of 2 chunks downstream */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);

    /* PAUSE (drops the ordinary-record hold, keeps the begun watermark). */
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub1, false, CTRL_NOW) ==
                   MOQR_OK);
    /* Source ABANDONS the begun group WHILE PAUSED. */
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 5, 0, 0, rd_wire(0x77u)) ==
                   MOQR_OK);
    /* The reset surfaces despite the pause: an ABANDONED (begun) delivery is
     * maintenance, not ordinary object transmission. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_ABANDONED);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                           CTRL_NOW) == MOQR_OK);
    /* Subscription survived; begun accounting balanced (a later clean delivery
     * on resume still works and is counted once). */
    moqr_sub_state_t stt;
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub1, &stt) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub1, true, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 6, 0, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 6);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 1);   /* only group 6 counted */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("forward_pause_partial_abandon");
    return failures;
}

/* Begin-with-ZERO-chunks still needs its RESET (round-3 correction #1): the bind
 * can enqueue begin_object (the object header is committed downstream, obj_begun
 * = true) and then hit WOULD_BLOCK on the FIRST chunk with next_chunk == 0. The
 * checkpoint records begun with emitted == 0. If Forward then pauses and the
 * source abandons the object, the begun-but-headerless object MUST still receive
 * its downstream RESET — it must NOT be treated as never-begun (which would leave
 * the peer with an open object header and no close). "Begun" is tracked distinctly
 * from the chunk count, so this works without faking a count of 1 (which would
 * skip chunk 0 on re-derivation). RED: with note_emitted's emitted==0 treated as
 * a no-op, the abandon is skipped and next_delivery returns DONE. */
static int
test_forward_pause_begun_zero_abandon(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* OPEN chunked record with chunks available. */
    MOQ_TEST_CHECK(open_rec(c, track, 5, 0, 0, 40) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA0) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA1) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);
    /* begin_object shipped the header; the FIRST chunk write blocked =>
     * next_chunk == 0. Checkpoint the begun-with-zero state. */
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, 0, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);

    /* PAUSE, then the source ABANDONS the (header-only) begun object. */
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub1, false, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 5, 0, 0, rd_wire(0x77u)) ==
                   MOQR_OK);
    /* The RESET surfaces despite zero chunks emitted. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_ABANDONED);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_sub_state_t stt;
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub1, &stt) == MOQR_OK);   /* alive */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("forward_pause_begun_zero_abandon");
    return failures;
}

/* moqr_core_delivery_note_emitted range contract (round-3 correction #2):
 * emitted must be 0..chunk_count of the outstanding record. An out-of-range
 * count is rejected MOQR_ERR_INVAL (it would otherwise become a bogus pin base /
 * watermark that pins zero chunks and silently advances); the boundary and zero
 * are accepted. RED: drop the upper-bound check and note_emitted(3) returns OK. */
static int
test_note_emitted_range_contract(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    MOQ_TEST_CHECK(open_rec(c, track, 5, 0, 0, 40) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA0) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA1) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);

    /* out of range (> chunk_count) rejected; state unchanged. */
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, 3, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    /* boundary (== chunk_count) and zero accepted. */
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, 2, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, 0, CTRL_NOW) ==
                   MOQR_OK);

    /* Release the outstanding delivery cleanly (STALLED unpins, no advance). */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                           CTRL_NOW) == MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("note_emitted_range_contract");
    return failures;
}

/* Forward pause keeps stream MAINTENANCE eligible (correction #2): a SEAL close
 * is discovered AND, once held across a WOULD_BLOCK, survives the pause rather
 * than being dropped — so a downstream FIN is never deferred to resume (which
 * would retain the subgroup slot against siblings). RED: revert the selection
 * gate to a blanket paused-sub skip and the notice is never discovered; revert
 * the binding_drop_held maintenance guard and the held notice is dropped. */
static int
test_forward_pause_seal_maintenance(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* Deliver groups 5 and 6 (whole) to establish the sub's subgroup positions. */
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 0, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 6, 0, 0, 128) == MOQR_OK);
    moqr_delivery_t d;
    for (int k = 0; k < 2; k++) {
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }

    /* Part A — held notice survives the pause. Seal group 5 while ACTIVE, hold
     * the notice across a WOULD_BLOCK, THEN pause: the eager release in
     * moqr_core_sub_set_forward must NOT drop a held maintenance delivery, and
     * neither must next_delivery's re-peek. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 5, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub1, false, CTRL_NOW) ==
                   MOQR_OK);   /* pause with a held notice */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);   /* not dropped */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* Part B — notice DISCOVERED while paused. Seal group 6 with the sub
     * already paused: the selection gate must still surface the SEAL. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 6, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_sub_state_t stt;
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub1, &stt) == MOQR_OK);   /* alive */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("forward_pause_seal_maintenance");
    return failures;
}

/* Forward pause keeps the EVICT-RESET sweep eligible (correction #2): a group a
 * paused sub had begun downstream (emitted >=1 chunk, checkpointed) is evicted;
 * the required reset MUST still surface while paused so the peer's begun-but-
 * unfinishable subgroup is closed rather than left hanging until resume. RED:
 * re-add the paused-sub skip in the evict-reset sweep (or neuter
 * note_emitted, dropping begun_count to 0) and no reset surfaces. */
static int
test_forward_pause_evict_maintenance(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 2;   /* tiny: eviction is easy to force */
    cfg.linger_us = 1000;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* Begin group 5 downstream: OPEN chunked record, emit chunk 0, hold. */
    MOQ_TEST_CHECK(open_rec(c, track, 5, 0, 0, 40) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA0) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA1) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, 1, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);

    /* PAUSE, then evict group 5 (two more groups into a 2-group log). */
    MOQ_TEST_CHECK(moqr_core_sub_set_forward(c, sub1, false, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 6, 0, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 7, 0, 0, 128) == MOQR_OK);   /* evicts 5 */

    /* The begun-evicted reset surfaces despite the pause. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_ABANDONED);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_sub_state_t stt;
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub1, &stt) == MOQR_OK);   /* alive */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("forward_pause_evict_maintenance");
    return failures;
}

/* Prefix-size (ABI) contract for moqr_subscribe_req_t (correction #4): a caller
 * that predates the trailing `forward` field (struct_size ending before it) is
 * still valid and defaults to forward=true — the field it never set must be
 * ignored, not read. A struct_size below that minimum is rejected. RED: revert
 * the gate to `< sizeof(moqr_subscribe_req_t)` and the prefix-sized subscribe
 * returns MOQR_ERR_INVAL. */
static int
test_subscribe_prefix_compat(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);

    /* Old caller: struct_size ends before `forward`, and `forward` is (stale
     * garbage) false — it must be IGNORED and the sub default to delivering. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.name = B("video");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 11;
    rq.forward = false;   /* NOT covered by struct_size below => ignored */
    rq.struct_size = (uint32_t)offsetof(moqr_subscribe_req_t, forward);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);
    moqr_intent_t its[16];
    size_t n = moqr_core_poll_intents(c, its, 16);
    moqr_track_t track;
    memset(&track, 0, sizeof(track));
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            track = its[i].track;
            (void)moqr_core_upstream_ok(c, track, its[i].track_gen, 777, true, 4,
                                        9);
        }
    }
    (void)moqr_core_poll_intents(c, its, 16);
    /* forward defaulted true (delivers) despite rq.forward==false. */
    MOQ_TEST_CHECK(ing(c, &a, track, 0, 0, 0, 128) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* Below the minimum prefix: rejected. */
    moqr_subscribe_req_t bad = rq;
    bad.struct_size = (uint32_t)(offsetof(moqr_subscribe_req_t, forward) - 1u);
    moqr_sub_t sub2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &bad, &sub2) == MOQR_ERR_INVAL);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("subscribe_prefix_compat");
    return failures;
}

/* Pin survives eviction: a chunked delivery held across a WOULD_BLOCK keeps
 * its chunks pinned even when the source group is evicted mid-delivery, so the
 * object still finishes intact (never truncated) and the cursor then resumes. */
static int
test_chunked_delivery_evict_hold(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);   /* mkcore: max_groups == 4 */
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* Chunked record in group 5, then peek+hold it across a WOULD_BLOCK. */
    MOQ_TEST_CHECK(ing_chunked(c, &a, track, 5, 0, 0, 128, 3, 20, 0xA0) ==
                   MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 3);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);

    /* Evict group 5: ingest four more groups (max_groups == 4 ⇒ oldest drops). */
    for (uint64_t g = 6; g <= 9; g++) {
        MOQ_TEST_CHECK(ing(c, &a, track, g, 0, 0, 128) == MOQR_OK);
    }

    /* The hold + pin survived eviction: re-peek still returns group 5's chunked
     * record (NOT the now-oldest group 6), and its chunks are intact. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 3);
    for (uint32_t k = 0; k < 3; k++) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, k, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(cl, 20);
        MOQ_TEST_CHECK(cb != NULL &&
                       moq_rcbuf_data(cb)[0] == (uint8_t)(0xA0 + k));
    }
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* The eviction jump surfaces as an acknowledged EVICT_WATERMARK notice
     * BEFORE any post-jump record; a WOULD_BLOCK holds it for an idempotent
     * re-peek — the watermark can no longer be lost to a blocked consumer
     * or observed twice across a re-peek. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_EVICT_WATERMARK);
    MOQ_TEST_CHECK_EQ_U64(d.oldest_group, 6);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_EVICT_WATERMARK);
    MOQ_TEST_CHECK_EQ_U64(d.oldest_group, 6);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* Acknowledged exactly once: delivery resumes past the evicted group with
     * a plain record and no second watermark. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_NONE);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 6);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("chunked_delivery_evict_hold");
    return failures;
}

/* Live edge: an OPEN record is selected and delivered chunk-by-chunk before
 * it completes; STALLED records the emitted watermark, releases without advancing
 * and suppresses re-selection until a new chunk arrives; a WOULD_BLOCK holds the
 * batch; completion delivers the remainder and advances the cursor. */
static int
test_open_liveedge_delivery(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* OPEN record (group 5, object 0, declared 60 = 3x20) with one chunk so far. */
    MOQ_TEST_CHECK(open_rec(c, track, 5, 0, 0, 60) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA0) == MOQR_OK);

    /* Selected while still OPEN (before complete). */
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_OPEN);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 1);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK(cl == 20 && moq_rcbuf_data(cb)[0] == 0xA0);
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 1, &cb, &cl) == MOQR_DONE);
    }

    /* STALLED: no delivery counted, and the drained head is not re-selected. */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 0);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    /* A second chunk makes it selectable again; the batch starts at index 1. */
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_OPEN);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 1, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK(cl == 20 && moq_rcbuf_data(cb)[0] == 0xA1);
        /* below the batch base: not in the pinned batch */
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) == MOQR_DONE);
    }
    /* WOULD_BLOCK holds the batch (unlike STALLED): re-peek replays it. */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 2);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 1, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK(moq_rcbuf_data(cb)[0] == 0xA1);
    }
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                           CTRL_NOW) == MOQR_OK);

    /* Third chunk + complete → COMPLETE selected, remaining chunk delivered,
     * DELIVERED advances the cursor and counts the object. */
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_complete_record(c, track, 5, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_COMPLETE);
    MOQ_TEST_CHECK_EQ_U64(d.rec.chunk_count, 3);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 2, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK(moq_rcbuf_data(cb)[0] == 0xA2);
    }
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 1);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("open_liveedge_delivery");
    return failures;
}

/* Abandon: an OPEN record abandoned AFTER this sub began it downstream is
 * surfaced (so the bind can reset) and advances without counting; an OPEN record
 * abandoned BEFORE any downstream begin is skipped silently. */
static int
test_open_abandon(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* Group 5: OPEN, one chunk delivered (emitted>0), THEN abandoned. */
    MOQ_TEST_CHECK(open_rec(c, track, 5, 0, 0, 60) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA0) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_OPEN);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 5, 0, 0, rd_wire(0x77u)) ==
                   MOQR_OK);

    /* Surfaced as an ABANDONED delivery (begun downstream) carrying the terminal
     * reset code; ABANDONED advances past it without counting. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_ABANDONED);
    MOQ_TEST_CHECK_EQ_U64(d.rec.object_id, 0);
    /* The upstream code is retained with the registry it arrived in, so a
     * later reader can still translate it. */
    MOQ_TEST_CHECK(d.rec.reset.tag == MOQR_CODE_WIRE_D16);
    MOQ_TEST_CHECK_EQ_U64(d.rec.reset.value, 0x77u);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 0);

    /* Group 6: OPEN + abandoned with NOTHING delivered (emitted==0) → skipped.
     * Group 7: a whole COMPLETE object that must still be delivered after it. */
    MOQ_TEST_CHECK(open_rec(c, track, 6, 0, 0, 40) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 6, 0, 0, 20, 0xB0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 6, 0, 0, rd_wire(0)) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 7, 0, 0, 128) == MOQR_OK);

    /* The abandoned-before-begin group 6 never surfaces; group 7 is delivered. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 7);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_COMPLETE);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 1);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("open_abandon");
    return failures;
}

/* No head-of-line block: two subscriptions on ONE binding — a stalled OPEN
 * live edge on one must not prevent a COMPLETE object on the other from being
 * delivered (STALLED releases the single outstanding-delivery slot). */
static int
test_liveedge_no_hol(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.name = B("video1");
    rq.cookie = 11;
    moqr_sub_t sub1;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub1) == MOQR_OK);
    rq.name = B("video2");
    rq.cookie = 22;
    moqr_sub_t sub2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub2) == MOQR_OK);
    moqr_intent_t its[16];
    size_t n = drain(c, its, 16);
    moqr_track_t track1, track2;
    memset(&track1, 0, sizeof(track1));
    memset(&track2, 0, sizeof(track2));
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind != MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            continue;
        }
        moqr_track_t tk = its[i].track;
        (void)moqr_core_upstream_ok(c, tk, its[i].track_gen, 777, true, 4, 9);
        if (its[i].name.len == 6 &&
            memcmp(its[i].name.data, "video1", 6) == 0) {
            track1 = tk;
        } else {
            track2 = tk;
        }
    }
    (void)drain(c, its, 16);   /* ACCEPT_SUB x2 */

    /* track1: an OPEN record (one chunk, will stall). track2: a whole COMPLETE. */
    MOQ_TEST_CHECK(open_rec(c, track1, 5, 0, 0, 40) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track1, 5, 0, 0, 20, 0xA0) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track2, 5, 0, 0, 128) == MOQR_OK);

    /* Drain the binding: the COMPLETE object is delivered regardless of the
     * order the two heads are visited — the stalled OPEN never blocks it. */
    bool delivered_complete = false;
    for (int i = 0; i < 8; i++) {
        moqr_delivery_t d;
        moqr_result_t rc = moqr_core_next_delivery(c, s1, CTRL_NOW, &d);
        if (rc == MOQR_DONE) {
            break;
        }
        MOQ_TEST_CHECK(rc == MOQR_OK);
        if (d.rec.chunk_count == 0 && d.rec.obj_state == MOQR_OBJ_COMPLETE) {
            MOQ_TEST_CHECK(moqr_core_delivery_done(
                               c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                           MOQR_OK);
            delivered_complete = true;
        } else {
            const moq_rcbuf_t *cb = NULL;
            uint64_t cl = 0;
            (void)moqr_core_delivery_chunk(c, s1, 0, &cb, &cl);
            MOQ_TEST_CHECK(moqr_core_delivery_done(
                               c, s1, MOQR_DELIVERY_STALLED, CTRL_NOW) ==
                           MOQR_OK);
        }
    }
    MOQ_TEST_CHECK(delivered_complete);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 1);   /* only the COMPLETE object */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("liveedge_no_hol");
    return failures;
}

/* Eviction after live-edge: an OPEN object begun downstream whose group is
 * capacity-evicted before it completes must surface an evict-reset (so the bind
 * resets the peer's begun subgroup) rather than silently disappearing. */
static int
test_liveedge_evict_reset(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);   /* mkcore: max_groups == 4 */
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* Begin an OPEN object at group 5, live-deliver one chunk, then STALL. */
    MOQ_TEST_CHECK(open_rec(c, track, 5, 0, 0, 60) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA0) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_OPEN);
    {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                           CTRL_NOW) == MOQR_OK);

    /* Evict group 5: four more groups (max_groups == 4 drops the oldest). */
    for (uint64_t g = 6; g <= 9; g++) {
        MOQ_TEST_CHECK(ing(c, &a, track, g, 0, 0, 128) == MOQR_OK);
    }

    /* The begun object's group is gone → an evict-reset is surfaced (obj_state
     * ABANDONED + evicted_reset, group 5) so the bind resets the peer's begun
     * subgroup; NOT counted as a delivery. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_ABANDONED);
    MOQ_TEST_CHECK(d.evicted_reset);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                           CTRL_NOW) == MOQR_OK);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.delivered_total, 0);   /* reset, not delivered */

    /* No second evict-reset (the slot was reclaimed); the reclaim's watermark
     * notice is acknowledged, then normal delivery resumes on the retained
     * groups. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_EVICT_WATERMARK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(!d.evicted_reset);
    MOQ_TEST_CHECK(d.rec.group_id >= 6 && d.rec.obj_state == MOQR_OBJ_COMPLETE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("liveedge_evict_reset");
    return failures;
}

/* A subgroup sealed AFTER its last record was already delivered surfaces as
 * an acknowledged SEAL notice — no record delivery will ever carry the
 * final-record flag for it, and before the notice existed the downstream
 * stream simply never closed. The notice names the WIRE subgroup id, holds
 * across a WOULD_BLOCK, acknowledges exactly once, and never re-fires (the
 * acknowledgement is stored per position list, so an idempotent re-seal has
 * nothing new to say). */
static int
test_seal_notice_late_fin(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* Two records on (group 5, wire subgroup 3); consume both UNSEALED. */
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 3, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 3, 1, 128) == MOQR_OK);
    moqr_delivery_t d;
    for (int i = 0; i < 2; i++) {
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_NONE);
        MOQ_TEST_CHECK(!d.subgroup_end);   /* unsealed: no flag possible */
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    /* The late FIN: sealed with nothing left to deliver. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 5, 3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK_EQ_U64(d.rec.subgroup_id, 3);   /* the WIRE id */
    MOQ_TEST_CHECK(d.subgroup_end);
    /* Held verbatim across a WOULD_BLOCK. */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.rec.subgroup_id, 3);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* Acknowledged exactly once; a re-seal (idempotent) never re-fires. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 5, 3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("seal_notice_late_fin");
    return failures;
}

/* One durable close path: a seal that lands BEFORE the subgroup's last
 * record is delivered flags that record (advisory), and the SEAL notice —
 * the actual close signal, retryable under WOULD_BLOCK — follows the
 * record's acknowledgement exactly once. */
static int
test_seal_flag_single_notice(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    MOQ_TEST_CHECK(ing(c, &a, track, 5, 3, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 3, 1, 128) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(!d.subgroup_end);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* Seal while the last record is still undelivered: the record carries
     * the ADVISORY flag; the close still rides the notice. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 5, 3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_NONE);
    MOQ_TEST_CHECK_EQ_U64(d.rec.object_id, 1);
    MOQ_TEST_CHECK(d.subgroup_end);   /* advisory final-record flag */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* The durable close: exactly one SEAL notice after the record's ack —
     * a WOULD_BLOCKed close is retried here instead of being swallowed by
     * the record delivery's acknowledgement. */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 5);
    MOQ_TEST_CHECK_EQ_U64(d.rec.subgroup_id, 3);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("seal_flag_single_notice");
    return failures;
}

static moqr_core_t *mkcore_groups(ca_t *a, uint32_t max_groups);

/* SEAL notices name the lowest WIRE subgroup id first — log list slots are
 * insertion-ordered, so inserting wire ids 9 then 2 puts 9 at slot 0; the
 * notice order must still be 2, then 9. */
static int
test_seal_order_lowest_subgroup(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    MOQ_TEST_CHECK(ing(c, &a, track, 5, 9, 0, 128) == MOQR_OK);   /* slot 0 */
    MOQ_TEST_CHECK(ing(c, &a, track, 5, 2, 1, 128) == MOQR_OK);   /* slot 1 */
    moqr_delivery_t d;
    for (int i = 0; i < 2; i++) {
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 5, 9) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 5, 2) == MOQR_OK);

    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.rec.subgroup_id, 2);   /* lowest wire id first */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.rec.subgroup_id, 9);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("seal_order_lowest_subgroup");
    return failures;
}

/* A notice never preempts a higher-priority sibling's record: sub A (default
 * priority) holds a pending SEAL notice while sub B (priority 1) still has
 * record work — B's record delivers first, then B's own notice (it drained
 * the same sealed subgroup, and its priority beats A's), then A's. */
static int
test_seal_notice_priority(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t subA;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &subA);

    MOQ_TEST_CHECK(ing(c, &a, track, 5, 3, 0, 128) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 5, 3) == MOQR_OK);

    /* Sub B joins at higher priority with the retained record still owed. */
    moq_bytes_t nsb[2];
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.name = B("video");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.subscriber_priority = 1;
    rq.cookie = 22;
    moqr_sub_t subB;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &subB) == MOQR_OK);
    moqr_intent_t its[8];
    (void)moqr_core_poll_intents(c, its, 8);

    /* B's record outranks A's pending notice (priority 1 beats default). */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_NONE);
    MOQ_TEST_CHECK_EQ_U64(d.sub_cookie, 22);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    /* Then the notices, priority order: B's (1) before A's (default). */
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.sub_cookie, 22);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.sub_cookie, 11);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("seal_notice_priority");
    return failures;
}

/* The advertised capacity ceiling must track the runtime position-entry
 * stride across a seal-ack bit-word boundary. The 32->33-list boundary is
 * alignment-degenerate (the extra word disappears into the 8-byte pad, so
 * old and new formulas share the same DELTA there); the runtime allocation
 * is asserted ABSOLUTELY against the documented stride — which pins the
 * 32/33 sizes too — and the model's delta is checked across 64->65 lists
 * (two acked words -> three), where a stale formula visibly diverges. */
static int
test_capacity_gpos_oracle(void)
{
    int failures = 0;
    /* The documented stride: group_id + idx[] + emitted[] + seal-ack words,
     * 8-aligned (mirrors gpos_stride_for; the oracle breaks if either side
     * drifts). */
#define STRIDE_DOC(L) \
    ((8u + 8u * (L) + 4u * (((L) + 31u) / 32u) + 7u) & ~7u)
    uint64_t sub_delta[2] = { 0, 0 };
    uint64_t cap_struct[2] = { 0, 0 };
    for (int k = 0; k < 2; k++) {
        uint32_t max_sg = 63u + (uint32_t)k;   /* lists = 64, then 65 */
        ca_t a;
        ca_init(&a);
        moqr_core_relay_cfg_t cfg;
        moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
        cfg.log_budget.max_groups = 4;
        cfg.log_budget.max_bytes = 1u << 20;
        cfg.log_max_subgroups = max_sg;
        moqr_core_capacity_t cap;
        moqr_core_capacity_describe(&cfg, &cap);
        cap_struct[k] = cap.structure_bytes;
        moqr_core_t *c = NULL;
        MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
        moqr_binding_t pub, s1;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
        moqr_sub_t subA;
        (void)chunked_sub_setup(c, pub, s1, &subA);
        /* A SECOND subscriber on the existing track allocates exactly its
         * positions block + hash index. */
        long before = a.live;
        moq_bytes_t nsb[2];
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = NS2(nsb, "demo", "cam");
        rq.name = B("video");
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 22;
        moqr_sub_t subB;
        MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &subB) == MOQR_OK);
        sub_delta[k] = (uint64_t)(a.live - before);
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    /* Runtime, ABSOLUTE: the positions block is groups x stride plus the
     * 8-entry hash index (32 bytes) — any drift from the documented stride
     * (including a missing acked term) shifts these. */
    MOQ_TEST_CHECK_EQ_U64(sub_delta[0], 4u * STRIDE_DOC(64) + 32u);
    MOQ_TEST_CHECK_EQ_U64(sub_delta[1], 4u * STRIDE_DOC(65) + 32u);
    /* The model: its per-sub gpos term must move by the same stride delta.
     * Everything else in structure_bytes that scales with the subgroup
     * count belongs to the log model — isolate the gpos term by
     * differencing against moqr_log_capacity_describe's own delta. */
    moqr_log_cfg_t lc31, lc32;
    ca_t la;
    ca_init(&la);
    moqr_log_cfg_init_sized(&lc31, sizeof(lc31), &la.vt);
    moqr_log_cfg_init_sized(&lc32, sizeof(lc32), &la.vt);
    lc31.budget.max_groups = 4;
    lc31.budget.max_bytes = 1u << 20;
    lc31.max_subgroups_per_group = 63;
    lc32 = lc31;
    lc32.max_subgroups_per_group = 64;
    moqr_log_capacity_t lg31, lg32;
    moqr_log_capacity_describe(&lc31, &lg31);
    moqr_log_capacity_describe(&lc32, &lg32);
    uint64_t resolved_subs = 0, resolved_tracks = 0;
    {
        ca_t a;
        ca_init(&a);
        moqr_core_relay_cfg_t cfg;
        moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
        moqr_core_t *c = NULL;
        MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
        moqr_core_limits_t lim;
        moqr_core_get_limits(c, &lim);
        resolved_subs = lim.max_subs;
        resolved_tracks = lim.max_tracks;
        moqr_core_destroy(c);
    }
    MOQ_TEST_CHECK_EQ_U64(
        cap_struct[1] - cap_struct[0],
        resolved_tracks * (lg32.structure_bytes - lg31.structure_bytes) +
            resolved_subs * 4u * (STRIDE_DOC(65) - STRIDE_DOC(64)));
#undef STRIDE_DOC
    MOQ_TEST_PASS("capacity_gpos_oracle");
    return failures;
}

/* A position slot reused for a new group must not inherit the old group's
 * seal acknowledgement: acknowledge group 0's late seal, evict it (its slot
 * reclaims and a fresh group reuses it), then late-seal the NEW group — the
 * notice must fire again. Pins the acked-state reset on slot init/reuse. */
static int
test_seal_ack_slot_reuse(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_groups(&a, 2);   /* 2-group cap forces reuse */
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_sub_t sub1;
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    /* Group 0: deliver, then late-seal and acknowledge the notice. */
    MOQ_TEST_CHECK(ing(c, &a, track, 0, 3, 0, 128) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 0, 3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 0);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* Evict group 0 (cap 2: groups 1 and 2 push it out); its position slot
     * reclaims via the watermark notice, then group 2 reuses it. */
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 3, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 2, 3, 0, 128) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_EVICT_WATERMARK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    for (int i = 0; i < 2; i++) {
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_NONE);
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }

    /* Group 1 reuses group 0's reclaimed position slot (free slots claim in
     * ascending group order). Its late seal must notify — a stale inherited
     * acknowledgement on the reused slot would silently suppress it and the
     * downstream stream would never FIN. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 1, 3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_SEAL);
    MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 1);
    MOQ_TEST_CHECK_EQ_U64(d.rec.subgroup_id, 3);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_DONE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("seal_ack_slot_reuse");
    return failures;
}

/* Drain every deliverable object for one binding; returns the count. The guard
 * bounds the pull loop (a corrupted per-binding sub list could otherwise spin). */
static int
deliver_count(moqr_core_t *c, moqr_binding_t b)
{
    int n = 0;
    for (int guard = 0; guard < 100000; guard++) {
        moqr_delivery_t d;
        moqr_result_t rc = moqr_core_next_delivery(c, b, CTRL_NOW, &d);
        if (rc != MOQR_OK) {
            break;   /* DONE or a terminal */
        }
        (void)moqr_core_delivery_done(c, b, MOQR_DELIVERY_DELIVERED, CTRL_NOW);
        n++;
    }
    return n;
}

/* The per-binding subscription index. Several bindings subscribe to
 * ONE track; each binding's next_delivery must serve only its own sub. Closing
 * one binding must not disturb another's indexed subs, and a retired sub's slot,
 * reused on the SAME binding, must not corrupt the list. RED: neuter
 * sub_list_push and the isolation delivery drops to 0; neuter sub_list_remove and
 * the same-binding slot-reuse spins (a self-cycle in the list). */
static int
test_per_binding_sub_index(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    moqr_binding_t pub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "n", "s")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "n", "s"), B("t"), 5,
                                          &track) == MOQR_OK);
    moqr_intent_t its[8];
    (void)drain(c, its, 8);   /* ACCEPT_PUBLISH */
    for (uint64_t o = 0; o < 3; o++) {
        MOQ_TEST_CHECK(ing(c, &a, track, 0, 0, o, 128) == MOQR_OK);
    }

    /* Three downstream bindings, one subscription each, all on the same track. */
    moqr_binding_t bnd[3];
    moqr_sub_t sub[3];
    for (int k = 0; k < 3; k++) {
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 100 + (uint64_t)k, &bnd[k]) ==
                       MOQR_OK);
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = NS2(nsb, "n", "s");
        rq.name = B("t");
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 100 + (uint64_t)k;
        MOQ_TEST_CHECK(moqr_core_subscribe(c, bnd[k], &rq, &sub[k]) == MOQR_OK);
        (void)drain(c, its, 8);   /* ACCEPT_SUB */
    }
    /* Each binding serves exactly its own sub's 3 objects (index isolation). */
    for (int k = 0; k < 3; k++) {
        MOQ_TEST_CHECK_EQ_INT(deliver_count(c, bnd[k]), 3);
    }

    /* Close the middle binding; the others must be undisturbed. */
    MOQ_TEST_CHECK(moqr_core_binding_close(c, bnd[1], CTRL_NOW) == MOQR_OK);
    (void)drain(c, its, 8);
    for (uint64_t o = 0; o < 2; o++) {
        MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, o, 128) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(deliver_count(c, bnd[0]), 2);   /* new group */
    MOQ_TEST_CHECK_EQ_INT(deliver_count(c, bnd[2]), 2);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, bnd[1], CTRL_NOW, &d) ==
                   MOQR_ERR_STALE_HANDLE);   /* closed binding */

    /* Slot reuse on the SAME binding: retire binding 0's sub, then re-subscribe
     * (sub_slot_find reclaims the freed slot). A missed list-remove would leave a
     * self-cycle here; a correct remove yields a clean re-subscribe serving all
     * 5 retained objects (g0:3 + g1:2). */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub[0], CTRL_NOW) == MOQR_OK);
    moqr_subscribe_req_t rq0;
    moqr_subscribe_req_init(&rq0);
    rq0.ns = NS2(nsb, "n", "s");
    rq0.name = B("t");
    rq0.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq0.cookie = 200;
    moqr_sub_t sub0b;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, bnd[0], &rq0, &sub0b) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_INT(deliver_count(c, bnd[0]), 5);
    MOQ_TEST_CHECK_EQ_INT(deliver_count(c, bnd[2]), 0);   /* still isolated */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("per_binding_sub_index");
    return failures;
}

/* Create a core with a chosen per-track group budget (mkcore hardcodes 4). */
static moqr_core_t *
mkcore_groups(ca_t *a, uint32_t max_groups)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.log_budget.max_groups = max_groups;
    cfg.log_budget.max_bytes = 1u << 20;
    cfg.linger_us = 1000;
    moqr_core_t *c = NULL;
    if (moqr_core_create(&cfg, &c) != MOQR_OK) {
        return NULL;
    }
    return c;
}

/* Publisher + subscriber over one track; drains the ACCEPT_* intents. */
static void
gpos_setup(moqr_core_t *c, moqr_binding_t *pub, moqr_binding_t *sub,
           moqr_track_t *track, moqr_sub_t *s, int *pf)
{
    int failures = 0;
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, *pub, NS2(nsb, "n", "s")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_publish_open(c, *pub, NS2(nsb, "n", "s"), B("t"), 5,
                                          track) == MOQR_OK);
    moqr_intent_t its[8];
    (void)drain(c, its, 8);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "n", "s");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 1;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, *sub, &rq, s) == MOQR_OK);
    (void)drain(c, its, 8);
    *pf += failures;
}

/* The group_id-keyed O(1) gpos index.
 * (a) Many retained groups all deliver in ascending order (hash find/insert
 *     across groups). RED: neuter gpos_hash_insert -> lookups miss, entries
 *     duplicate until the array fills, and delivery stalls short.
 * (b) An evicted UNBEGUN group reclaims + skips, and its slot — reused by a new
 *     group — resolves to the NEW group with no stale group_id (rebuild-on-
 *     reclaim / self-validating find).
 * The begun-evicted retention path (kept until reset) is covered by
 * test_liveedge_evict_reset, which relies on gpos_gc NOT reclaiming a begun
 * entry. */
static int
test_gpos_index(void)
{
    int failures = 0;

    /* (a) many groups deliver in order. */
    {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c = mkcore_groups(&a, 8);
        MOQ_TEST_CHECK(c != NULL);
        moqr_binding_t pub, sub;
        moqr_track_t track;
        moqr_sub_t s1;
        gpos_setup(c, &pub, &sub, &track, &s1, &failures);
        for (uint64_t g = 0; g < 8; g++) {
            MOQ_TEST_CHECK(ing(c, &a, track, g, 0, 0, 128) == MOQR_OK);
        }
        for (uint64_t g = 0; g < 8; g++) {
            moqr_delivery_t d;
            MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, CTRL_NOW, &d) ==
                           MOQR_OK);
            MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, g);   /* ascending, all found */
            MOQ_TEST_CHECK(moqr_core_delivery_done(c, sub, MOQR_DELIVERY_DELIVERED,
                                                   CTRL_NOW) == MOQR_OK);
        }
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, CTRL_NOW, &d) == MOQR_DONE);
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }

    /* (b) evict an unbegun group; its slot is reused by a new group. */
    {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c = mkcore_groups(&a, 2);   /* 2-group cap */
        MOQ_TEST_CHECK(c != NULL);
        moqr_binding_t pub, sub;
        moqr_track_t track;
        moqr_sub_t s1;
        gpos_setup(c, &pub, &sub, &track, &s1, &failures);
        MOQ_TEST_CHECK(ing(c, &a, track, 0, 0, 0, 128) == MOQR_OK);
        MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 128) == MOQR_OK);
        moqr_delivery_t d;
        /* Deliver group 0 (creates + advances its gpos entry). */
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 0);
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, sub, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
        /* Ingest group 2 -> evicts group 0 (unbegun -> reclaim + skip); group 2
         * reuses group 0's freed slot. The jump surfaces as an acknowledged
         * watermark notice before any post-jump record. */
        MOQ_TEST_CHECK(ing(c, &a, track, 2, 0, 0, 128) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.notice, MOQR_DELIVERY_NOTICE_EVICT_WATERMARK);
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, sub, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 1);   /* group 0 gone, not re-served */
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, sub, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 2);   /* reused slot -> correct id */
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, sub, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, sub, CTRL_NOW, &d) == MOQR_DONE);
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }

    MOQ_TEST_PASS("gpos_index");
    return failures;
}

/* Subscribe `sub_b` to demo/cam/<name> (namespace already announced) and settle
 * the upstream, returning the track. Like chunked_sub_setup but track-named, so
 * one binding can hold subs to several tracks. */
static moqr_track_t
sub_named(moqr_core_t *c, moqr_binding_t sub_b, const char *name, uint64_t cookie,
          moqr_sub_t *out_sub)
{
    moq_bytes_t nsb[2];
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.name = B(name);
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = cookie;
    (void)moqr_core_subscribe(c, sub_b, &rq, out_sub);
    moqr_intent_t its[16];
    size_t n = moqr_core_poll_intents(c, its, 16);
    moqr_track_t track;
    memset(&track, 0, sizeof(track));
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
            track = its[i].track;
            (void)moqr_core_upstream_ok(c, track, its[i].track_gen, 777, true, 4,
                                        9);
        }
    }
    (void)moqr_core_poll_intents(c, its, 16);
    return track;
}

/* The evict-reset sweep is gated on begun live-edge state, so it never scans
 * under whole-object delivery (even with eviction), arms only once a sub has
 * begun an OPEN object, and stands down once the reset is confirmed. evict_sweeps
 * (stats) counts sweep-body runs, making the gate directly observable.
 * RED: neuter sub_begun_add (the STALLED arm) -> begun_subs stays 0 -> the sweep
 * is gated off even when a begun group evicts -> part (b) loses its reset. */
static int
test_evict_sweep_gate(void)
{
    int failures = 0;

    /* (a) whole-object delivery WITH eviction never runs the sweep. */
    {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c = mkcore(&a, NULL);   /* max_groups == 4 */
        MOQ_TEST_CHECK(c != NULL);
        moqr_binding_t pub, s1;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
        moqr_sub_t sub1;
        moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);
        for (uint64_t g = 0; g < 6; g++) {   /* evicts the oldest two */
            MOQ_TEST_CHECK(ing(c, &a, track, g, 0, 0, 128) == MOQR_OK);
        }
        moqr_delivery_t d;
        while (moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK) {
            MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                                   CTRL_NOW) == MOQR_OK);
        }
        moqr_core_stats_t st;
        moqr_core_get_stats(c, &st);
        MOQ_TEST_CHECK_EQ_U64(st.evict_sweeps, 0);   /* eviction, nothing begun */
        MOQ_TEST_CHECK(st.delivered_total >= 4);
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }

    /* (b) a STALLED live-edge object arms the sweep; the evict-reset fires; then
     * the gate clears and the sweep goes idle again. */
    {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c = mkcore(&a, NULL);
        MOQ_TEST_CHECK(c != NULL);
        moqr_binding_t pub, s1;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
        moqr_sub_t sub1;
        moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);
        MOQ_TEST_CHECK(open_rec(c, track, 5, 0, 0, 60) == MOQR_OK);
        MOQ_TEST_CHECK(append1(c, &a, track, 5, 0, 0, 20, 0xA0) == MOQR_OK);
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.rec.obj_state, MOQR_OBJ_OPEN);
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        MOQ_TEST_CHECK(moqr_core_delivery_chunk(c, s1, 0, &cb, &cl) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                               CTRL_NOW) == MOQR_OK);
        moqr_core_stats_t st;
        moqr_core_get_stats(c, &st);
        MOQ_TEST_CHECK_EQ_U64(st.evict_sweeps, 0);   /* the STALL runs no sweep */

        for (uint64_t g = 6; g <= 9; g++) {   /* evict group 5 */
            MOQ_TEST_CHECK(ing(c, &a, track, g, 0, 0, 128) == MOQR_OK);
        }
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK(d.evicted_reset && d.rec.group_id == 5);
        moqr_core_get_stats(c, &st);
        MOQ_TEST_CHECK(st.evict_sweeps >= 1);        /* armed: the sweep ran */
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                               CTRL_NOW) == MOQR_OK);
        moqr_core_get_stats(c, &st);
        uint64_t swept = st.evict_sweeps;

        while (moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK) {
            MOQ_TEST_CHECK(!d.evicted_reset);        /* reset not repeated */
            MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                                   CTRL_NOW) == MOQR_OK);
        }
        moqr_core_get_stats(c, &st);
        MOQ_TEST_CHECK_EQ_U64(st.evict_sweeps, swept);   /* gate cleared */
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }

    /* (c) two subs on one binding: a begun+evicted sub still gets its reset even
     * though the other sub's track never evicts. The gate is per-binding-begun,
     * not per-track, so one track cannot hide another sub's needed reset. */
    {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c = mkcore(&a, NULL);
        MOQ_TEST_CHECK(c != NULL);
        moqr_binding_t pub, s1;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
        moq_bytes_t nsb[2];
        MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "demo", "cam")) ==
                       MOQR_OK);
        moqr_sub_t subV, subA;
        moqr_track_t tV = sub_named(c, s1, "video", 11, &subV);
        moqr_track_t tA = sub_named(c, s1, "audio", 12, &subA);
        MOQ_TEST_CHECK(ing(c, &a, tA, 0, 0, 0, 64) == MOQR_OK);   /* audio, retained */
        MOQ_TEST_CHECK(open_rec(c, tV, 5, 0, 0, 60) == MOQR_OK);
        MOQ_TEST_CHECK(append1(c, &a, tV, 5, 0, 0, 20, 0xA0) == MOQR_OK);
        /* Drain: stall video's open head, deliver audio's whole object. */
        moqr_delivery_t d;
        for (int guard = 0; guard < 8; guard++) {
            moqr_result_t rc = moqr_core_next_delivery(c, s1, CTRL_NOW, &d);
            if (rc == MOQR_DONE) {
                break;
            }
            MOQ_TEST_CHECK(rc == MOQR_OK);
            if (d.rec.obj_state == MOQR_OBJ_OPEN) {
                const moq_rcbuf_t *cb = NULL;
                uint64_t cl = 0;
                (void)moqr_core_delivery_chunk(c, s1, 0, &cb, &cl);
                MOQ_TEST_CHECK(moqr_core_delivery_done(
                                   c, s1, MOQR_DELIVERY_STALLED, CTRL_NOW) ==
                               MOQR_OK);
            } else {
                MOQ_TEST_CHECK(moqr_core_delivery_done(
                                   c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                               MOQR_OK);
            }
        }
        for (uint64_t g = 6; g <= 9; g++) {   /* evict video group 5; audio untouched */
            MOQ_TEST_CHECK(ing(c, &a, tV, g, 0, 0, 128) == MOQR_OK);
        }
        /* The begun video sub still surfaces its reset — audio does not hide it. */
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
        MOQ_TEST_CHECK(d.evicted_reset && d.rec.group_id == 5);
        MOQ_TEST_CHECK_EQ_U64(d.sub_cookie, 11);   /* the video sub, not audio */
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                               CTRL_NOW) == MOQR_OK);
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }

    MOQ_TEST_PASS("evict_sweep_gate");
    return failures;
}

/* Create a core carrying a chosen shard identity (mkcore is single-shard). */
static moqr_core_t *
mkcore_shard(ca_t *a, moqr_trace_t *trace, uint16_t idx, uint16_t count)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.trace = trace;
    cfg.shard_index = idx;
    cfg.shard_count = count;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1u << 20;
    cfg.linger_us = 1000;
    moqr_core_t *c = NULL;
    if (moqr_core_create(&cfg, &c) != MOQR_OK) {
        return NULL;
    }
    return c;
}

/* Multi-shard identity seams: (a) a handle minted by one shard's core is
 * structurally refused by another shard's core (per-core shard tag); (b) the
 * control-plane epochs set via moqr_core_set_epochs stamp trace records and
 * route dumps and survive a route mutation.
 * RED: revert r_pack to a fixed shard tag -> (a) foreign handles are accepted;
 * revert r_publish_epochs to MOQR_EPOCHS_INIT -> (b) the post-set route mutation
 * zeroes the stamped node/shard epochs. */
static int
test_shard_seams(void)
{
    int failures = 0;

    /* (a) cross-shard handle rejection. */
    {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c0 = mkcore_shard(&a, NULL, 0, 2);   /* shard tag 1 */
        moqr_core_t *c1 = mkcore_shard(&a, NULL, 1, 2);   /* shard tag 2 */
        MOQ_TEST_CHECK(c0 != NULL && c1 != NULL);
        moqr_binding_t b0, b1;
        MOQ_TEST_CHECK(moqr_core_binding_open(c0, 1, &b0) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_binding_open(c1, 1, &b1) == MOQR_OK);
        moq_bytes_t nsb[2];
        /* own binding accepted; the other shard's binding is refused. */
        MOQ_TEST_CHECK(moqr_core_announce(c0, b0, NS2(nsb, "n", "s")) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_announce(c1, b1, NS2(nsb, "n", "s")) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_announce(c0, b1, NS2(nsb, "x", "y")) ==
                       MOQR_ERR_STALE_HANDLE);
        MOQ_TEST_CHECK(moqr_core_announce(c1, b0, NS2(nsb, "x", "y")) ==
                       MOQR_ERR_STALE_HANDLE);
        moqr_core_destroy(c0);
        moqr_core_destroy(c1);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }

    /* (b) epoch set + preservation across a route mutation. */
    {
        ca_t a;
        ca_init(&a);
        moqr_trace_t *tr = NULL;
        MOQ_TEST_CHECK(moqr_trace_create(&a.vt, 256, &tr) == MOQR_OK);
        moqr_core_t *c = mkcore_shard(&a, tr, 1, 4);
        MOQ_TEST_CHECK(c != NULL);
        moqr_core_set_epochs(c, 3, 7);
        moqr_binding_t b;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &b) == MOQR_OK);
        moq_bytes_t nsb[2];
        MOQ_TEST_CHECK(moqr_core_announce(c, b, NS2(nsb, "n", "s")) == MOQR_OK);
        char buf[2048];
        size_t w = 0;
        MOQ_TEST_CHECK(moqr_core_route_dump_text(c, buf, sizeof(buf), &w) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(strstr(buf, "epochs: node=3 shard=7 route=") != NULL);
        MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(strstr(buf, "\"node\":3,\"shard\":7,\"route\":") != NULL);
        /* the ROUTE_ADD record (emitted after the announce bumped route_epoch)
         * carries the preserved node/shard epochs, not zeroed. */
        MOQ_TEST_CHECK(moqr_trace_write_jsonl(tr, buf, sizeof(buf), &w) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(strstr(buf, "\"node_epoch\":3,\"shard_epoch\":7,") !=
                       NULL);
        moqr_core_destroy(c);
        moqr_trace_destroy(tr);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }

    MOQ_TEST_PASS("shard_seams");
    return failures;
}

/* The narrow observability seam: moqr_core_sub_state reads a subscription's
 * exact state (PARKED before upstream resolution, ACTIVE after) without
 * touching it, and every retired/foreign/never-issued handle is
 * STALE_HANDLE — the generation guard, never a stale value. */
static int
test_sub_state_seam(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "st", "a")) ==
                   MOQR_OK);

    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "st", "a");
    rq.name = B("v");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 5;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub) == MOQR_OK);

    /* Parked while the track awaits its upstream resolution. */
    moqr_sub_state_t st = MOQR_SUB_ACTIVE;
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub, &st) == MOQR_OK);
    MOQ_TEST_CHECK(st == MOQR_SUB_PARKED);

    moqr_intent_t its[4];
    size_t n = drain(c, its, 4);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, its[0].track, its[0].track_gen,
                                         77, false, 0, 0) == MOQR_OK);

    /* Active once resolved. Pure: repeated reads agree. */
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub, &st) == MOQR_OK);
    MOQ_TEST_CHECK(st == MOQR_SUB_ACTIVE);
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub, &st) == MOQR_OK);
    MOQ_TEST_CHECK(st == MOQR_SUB_ACTIVE);

    /* Bad arguments are INVAL; the state out-param is required. */
    MOQ_TEST_CHECK(moqr_core_sub_state(NULL, sub, &st) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub, NULL) == MOQR_ERR_INVAL);

    /* A never-issued handle and the retired handle are both STALE. */
    MOQ_TEST_CHECK(moqr_core_sub_state(c, MOQR_SUB_INVALID, &st) ==
                   MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub, &st) ==
                   MOQR_ERR_STALE_HANDLE);

    /* A recycled slot never resurrects the old handle: subscribe again
     * (same track, same slot pool) and the OLD handle stays STALE while
     * the new one reads cleanly. */
    (void)drain(c, its, 4);
    rq.cookie = 6;
    moqr_sub_t sub2;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub, &st) ==
                   MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_sub_state(c, sub2, &st) == MOQR_OK);
    MOQ_TEST_CHECK(st == MOQR_SUB_ACTIVE);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("sub_state_seam");
    return failures;
}

/* draft-ietf-moq-transport-18 Section 2.4.1: "Track Namespace is an ordered
 * set of between 0 and 32 Track Namespace Fields" -- the ROOT namespace (zero
 * elements) is valid. Draft 16 Section 2.4.1 says "between 1 and 32", so the
 * root is a draft-18-only shape; the relay core is protocol-neutral and must
 * be able to REPRESENT it, leaving the draft distinction to the profiles.
 *
 * The trie already has root node 0 and ns-subscriptions already accept an
 * empty prefix, so this pins the announce/unannounce/withdraw side against the
 * same single representation. */
/* Root FULL TRACK NAMES, direct on the protocol-neutral core.
 *
 * draft-18 Section 2.4.1: a Full Track Name is a Track Namespace (0..32 fields)
 * plus a Track Name, and the Track Name may itself be empty. So a ROOT FTN --
 * zero namespace fields plus a name -- is valid, and all four core entry paths
 * that take an FTN must admit it. draft-16 Section 2.4.1 keeps the 1..32 floor,
 * which its session profile enforces ahead of this core.
 *
 * This drives the real entry points, not a copied validator. */
static int
test_root_full_track_name_core(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);

    moqr_ns_t root = { NULL, 0 };
    moq_bytes_t name = B("v");
    moqr_intent_t its[16];
    size_t n;

    /* The root namespace must be announced for a root-FTN subscribe to find a
     * publisher, exactly as any other namespace would be. */
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, root) == MOQR_OK);
    (void)drain(c, its, 16);

    /* 1. publish_open on a root FTN: accepted, and it must queue the PUBLISH
     *    acceptance for THIS request, not merely return OK. */
    moqr_track_t th;
    memset(&th, 0, sizeof(th));
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, root, name, 41, &th) ==
                   MOQR_OK);

    /* 2. track_status on a root FTN: accepted, with the status response. */
    MOQ_TEST_CHECK(moqr_core_track_status(c, sub, root, name, 42) == MOQR_OK);

    /* Consume and CLASSIFY what those two produced. Anything other than the
     * expected acceptance -- a reject, an error kind, the wrong cookie -- fails
     * here, so a hard error can no longer masquerade as acceptance. */
    n = drain(c, its, 16);
    int accept_pub = 0, status_ok = 0, other = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_ACCEPT_PUBLISH && its[i].cookie == 41) {
            accept_pub++;
        } else if (its[i].kind == MOQR_INTENT_TRACK_STATUS_OK &&
                   its[i].cookie == 42) {
            status_ok++;
        } else {
            other++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(accept_pub, 1);
    MOQ_TEST_CHECK_EQ_INT(status_ok, 1);
    MOQ_TEST_CHECK_EQ_INT(other, 0);
    /* The returned track handle is not merely non-zero: it is USABLE on the
     * production ingest path. */
    {
        moqr_log_append_desc_t d;
        moqr_log_append_desc_init(&d);
        d.group_id = 0;
        d.subgroup_id = 0;
        d.object_id = 0;
        d.publisher_priority = 128;
        d.obj_state = MOQR_OBJ_OPEN;
        d.declared_len = 16;
        d.now_us = 1;
        MOQ_TEST_CHECK(moqr_core_ingest(c, th, &d) == MOQR_OK);
    }

    /* 3. subscribe on a root FTN, and it must route to the root publisher --
     *    the key/routing behaviour, not merely the validator. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = root;
    rq.name = name;
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 43;
    moqr_sub_t sh;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sub, &rq, &sh) == MOQR_OK);
    n = drain(c, its, 16);
    int accept_sub = 0, sub_other = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_ACCEPT_SUB && its[i].cookie == 43) {
            accept_sub++;
        } else {
            sub_other++;   /* a REJECT_SUB, or anything unexpected */
        }
    }
    /* The root publisher's track is ACTIVE, so this resolves to a direct
     * acceptance for exactly this request and nothing else. */
    MOQ_TEST_CHECK_EQ_INT(accept_sub, 1);
    MOQ_TEST_CHECK_EQ_INT(sub_other, 0);
    MOQ_TEST_CHECK(moqr_sub_is_valid(sh));   /* a usable subscription handle */

    /* 4. fetch_open on a root FTN. */
    moqr_fetch_req_t fq;
    moqr_fetch_req_init(&fq);
    fq.ns = root;
    fq.name = name;
    fq.cookie = 44;
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    memset(&plan, 0, sizeof(plan));
    moqr_result_t frc = moqr_core_fetch_open(c, sub, &fq, 1000, &fh, &plan);
    /* Exact expected outcome for THIS state: the root track exists but holds no
     * object yet, so admission is a decided REJECT carried on an OK return --
     * not a hard error, and not the validator refusing the shape. Pinning both
     * halves means CAPACITY/NOMEM/WRONG_STATE can no longer pass as acceptance. */
    MOQ_TEST_CHECK(frc == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)plan.admit, (uint64_t)MOQR_FETCH_REJECT);
    /* The whole contract for this decision, not just the verdict: the empty
     * track rejects with INVALID_RANGE and hands back no usable handle. */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)plan.error_code, 0x11u);
    MOQ_TEST_CHECK(!moqr_fetch_is_valid(fh));

    /* An empty TRACK NAME is permitted by both drafts. Paired here with the
     * root namespace, whose complete Full Track Name is draft-18 only -- the
     * point is that the empty NAME contributes no refusal, not that this whole
     * FTN would be legal on a draft-16 session. */
    MOQ_TEST_CHECK(moqr_core_track_status(c, sub, root,
                                          (moq_bytes_t){ NULL, 0 }, 45) ==
                   MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        printf("PASS: root full track name accepted by all four core paths\n");
    }
    return failures;
}

/* A PRESENT Track Namespace Field MUST contain at least one byte -- draft-18
 * and draft-16 Section 2.4.1 both say so. That is distinct from the FIELD COUNT
 * range and from Track Names, which may be empty. These drive the core APIs
 * directly, since the session parser rejects such a field before the relay sees
 * it on a real wire. */
static int
test_empty_namespace_field_rejected_core(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    moqr_binding_t b0;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &b0) == MOQR_OK);

    /* Three malformed shapes:
     *  bad1 - a lone present field that is NULL/zero-length;
     *  bad2 - a valid field FOLLOWED by an empty one, so every field is
     *         inspected rather than only field zero;
     *  bad3 - a NON-NULL, zero-length field, which a check that only rejects
     *         NULL data would wrongly admit. */
    static const uint8_t backing[4] = { 'l', 'i', 'v', 'e' };
    moq_bytes_t empty1[1] = { { NULL, 0 } };
    moq_bytes_t mixed[2]  = { { backing, 4 }, { NULL, 0 } };
    moq_bytes_t nonnull0[1] = { { backing, 0 } };
    moqr_ns_t bad1 = { empty1, 1 };
    moqr_ns_t bad2 = { mixed, 2 };
    moqr_ns_t bad3 = { nonnull0, 1 };
    moq_bytes_t name = B("v");

    /* Announce / unannounce / force-withdraw. */
    MOQ_TEST_CHECK(moqr_core_announce(c, b0, bad1) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_announce(c, b0, bad2) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_announce(c, b0, bad3) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, b0, bad3, 66) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_track_status(c, b0, bad3, name, 67) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, b0, bad1) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, bad1, 0x1, 1000) ==
                   MOQR_ERR_INVAL);

    /* Namespace subscription (prefix). */
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, b0, bad1, 60) == MOQR_ERR_INVAL);

    /* All four FTN entry paths. */
    moqr_track_t th;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, b0, bad1, name, 61, &th) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_track_status(c, b0, bad1, name, 62) ==
                   MOQR_ERR_INVAL);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = bad1;
    rq.name = name;
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 63;
    moqr_sub_t sh;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, b0, &rq, &sh) == MOQR_ERR_INVAL);
    moqr_fetch_req_t fq;
    moqr_fetch_req_init(&fq);
    fq.ns = bad1;
    fq.name = name;
    fq.cookie = 64;
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, b0, &fq, 1000, &fh, &plan) ==
                   MOQR_ERR_INVAL);

    /* Controls: the ROOT namespace and an EMPTY TRACK NAME stay valid, so the
     * rejection above is about empty FIELDS, not about zero counts or names. */
    moqr_ns_t root = { NULL, 0 };
    MOQ_TEST_CHECK(moqr_core_announce(c, b0, root) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_track_status(c, b0, root,
                                          (moq_bytes_t){ NULL, 0 }, 65) ==
                   MOQR_OK);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        printf("PASS: present-but-empty namespace field rejected core-wide\n");
    }
    return failures;
}

static int
test_root_namespace_roundtrip(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    moqr_binding_t pub, watch_root, watch_leaf;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &watch_root) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 3, &watch_leaf) == MOQR_OK);

    moqr_ns_t root = { NULL, 0 };
    moq_bytes_t pfx[1];
    pfx[0] = B("other");
    /* An empty prefix matches everything; a non-root prefix must NOT match
     * the root announce (root-vs-nonroot isolation). */
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, watch_root, root, 10) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, watch_leaf,
                                          (moqr_ns_t){ pfx, 1 }, 11) ==
                   MOQR_OK);
    moqr_intent_t its[16];
    (void)drain(c, its, 16);

    /* Announce the ROOT namespace. */
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, root) == MOQR_OK);
    size_t n = drain(c, its, 16);
    int found_root = 0, found_leaf = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind != MOQR_INTENT_NS_FOUND) {
            continue;
        }
        if (its[i].cookie == 10) {
            found_root++;
            /* The suffix beyond an empty prefix is itself empty. */
            MOQ_TEST_CHECK(its[i].ns_count == 0);
            MOQ_TEST_CHECK(its[i].value == 0);
        } else if (its[i].cookie == 11) {
            found_leaf++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(found_root, 1);   /* exactly once */
    MOQ_TEST_CHECK_EQ_INT(found_leaf, 0);   /* isolation */

    /* Withdrawing the root must fan exactly one NS_GONE to the root watcher. */
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pub, root) == MOQR_OK);
    n = drain(c, its, 16);
    int gone_root = 0, gone_leaf = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind != MOQR_INTENT_NS_GONE) {
            continue;
        }
        if (its[i].cookie == 10) {
            gone_root++;
        } else if (its[i].cookie == 11) {
            gone_leaf++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(gone_root, 1);
    MOQ_TEST_CHECK_EQ_INT(gone_leaf, 0);

    /* Re-announce, then force-withdraw the root through the operator path.
     * The withdraw's intents are ASSERTED, not discarded. */
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, root) == MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, root, 0x5u, CTRL_NOW) ==
                   MOQR_OK);
    n = drain(c, its, 16);
    int fw_gone_root = 0, fw_gone_leaf = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind != MOQR_INTENT_NS_GONE) {
            continue;
        }
        if (its[i].cookie == 10) {
            fw_gone_root++;
        } else if (its[i].cookie == 11) {
            fw_gone_leaf++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(fw_gone_root, 1);   /* empty prefix told exactly once */
    MOQ_TEST_CHECK_EQ_INT(fw_gone_leaf, 0);   /* non-root watcher untouched     */
    /* The operator path also owes the publisher a cancel, exactly once and
     * carrying the operator's wire code. */
    moqr_revoked_grant_t rv[4];
    size_t rvn = moqr_core_peek_revoked_grants(c, rv, 4);
    printf("  root force_withdraw: gone_root=%d gone_leaf=%d grants=%zu%s\n",
           fw_gone_root, fw_gone_leaf, rvn,
           rvn > 0 ? "" : " (no grant recorded)");
    MOQ_TEST_CHECK_EQ_INT((int)rvn, 1);
    MOQ_TEST_CHECK(rv[0].error_code == 0x5u);
    moqr_core_ack_revoked_grant(c, rv[0].binding_cookie, rv[0].session_cookie);
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 0);

    /* The root is no longer active: a repeated withdraw is the documented
     * no-op MOQR_OK and fans NOTHING. */
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, root, 0x5u, CTRL_NOW) ==
                   MOQR_OK);
    n = drain(c, its, 16);
    int repeat_gone = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind == MOQR_INTENT_NS_GONE) {
            repeat_gone++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(repeat_gone, 0);
    MOQ_TEST_CHECK(moqr_core_peek_revoked_grants(c, rv, 4) == 0);

    /* A non-root announce still behaves exactly as before. */
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "other", "x")) ==
                   MOQR_OK);
    n = drain(c, its, 16);
    int leaf_hit = 0, root_hit = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind != MOQR_INTENT_NS_FOUND) {
            continue;
        }
        if (its[i].cookie == 11) {
            leaf_hit++;
        } else if (its[i].cookie == 10) {
            root_hit++;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(leaf_hit, 1);
    MOQ_TEST_CHECK_EQ_INT(root_hit, 1);   /* empty prefix matches everything */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("root_namespace_roundtrip");
    return failures;
}


/* The SOURCE of an ACTIVE track dies by binding close. With a live
 * alternate announce the standing demand retargets — exactly one fresh
 * upstream SUBSCRIBE to the alternate, no downstream terminal, no duplicate
 * acceptance, retained delivery preserved, stale resolutions from the dead
 * generation refused. Without one, every live subscriber gets exactly one
 * explicit terminal. Capacity exhaustion mid-close is zero-mutation until
 * the caller's retry — no idle nudge involved. */
static int
test_source_failover(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    moqr_binding_t pubA, pubB, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pubA) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &pubB) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 300, &s1) == MOQR_OK);

    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pubA, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.name = B("video");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 11;
    moqr_sub_t sub1;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub1) == MOQR_OK);

    moqr_intent_t its[16];
    size_t n = drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    MOQ_TEST_CHECK_EQ_U64(its[0].binding_cookie, 100);
    moqr_track_t track = its[0].track;
    uint64_t gen_a = its[0].track_gen;
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, gen_a, 777, true, 1, 0) ==
                   MOQR_OK);
    n = drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_ACCEPT_SUB);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 128) == MOQR_OK);

    /* A withdraws the advertisement (nonterminal: the track stays usable),
     * B takes the namespace over. */
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pubA, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 1, 128) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pubB, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);
    (void)drain(c, its, 16); /* ns-sub fanout noise, none expected here */

    /* A's binding dies: exactly one fresh upstream SUBSCRIBE to B, no
     * downstream terminal, no duplicate acceptance. */
    MOQ_TEST_CHECK(moqr_core_binding_close(c, pubA, CTRL_NOW) == MOQR_OK);
    n = drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    MOQ_TEST_CHECK_EQ_U64(its[0].binding_cookie, 200);
    uint64_t gen_b = its[0].track_gen;
    MOQ_TEST_CHECK(gen_b != gen_a);

    /* stale resolutions from the dead generation are refused */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, gen_a, 5, false, 0, 0) ==
                   MOQR_ERR_STALE_HANDLE);
    /* the fresh one resolves silently: no duplicate downstream acceptance */
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, gen_b, 888, true, 1, 1) ==
                   MOQR_OK);
    n = drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)0);

    /* retained delivery state survived the failover: pre-failover records
     * still deliver, and new-source ingest continues behind them */
    MOQ_TEST_CHECK(ing(c, &a, track, 2, 0, 0, 128) == MOQR_OK);
    for (uint64_t want = 0; want < 3; want++) {
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_delivery_done(
                           c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                       MOQR_OK);
    }

    /* B dies with NO alternate: exactly one explicit downstream terminal. */
    MOQ_TEST_CHECK(moqr_core_binding_close(c, pubB, CTRL_NOW) == MOQR_OK);
    n = drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_SUB_DONE);
    MOQ_TEST_CHECK_EQ_U64(its[0].binding_cookie, 300);
    /* PUBLISH_DONE TRACK_ENDED: the track is no longer being published. */
    MOQ_TEST_CHECK_EQ_U64(its[0].error_code, 0x2);
    /* exactly once: nothing further surfaces */
    n = drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("source_failover");
    return failures;
}

/* Capacity mid-failover: a full intent ring leaves the track UNTOUCHED and
 * the close resumable — the retry completes with the identical outcome. */
static int
test_source_failover_backpressure(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 1000;
    cfg.max_intents = 4; /* post atomicity clamp floor: keep tiny */
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    if (c == NULL) {
        return failures + 1;
    }

    moqr_binding_t pubA, pubB, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pubA) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &pubB) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 300, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_announce(c, pubA, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "demo", "cam");
    rq.name = B("video");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = 11;
    moqr_sub_t sub1;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub1) == MOQR_OK);
    moqr_intent_t its[8];
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
    moqr_track_t track = its[0].track;
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, its[0].track_gen, 7,
                                         false, 0, 0) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pubA, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pubB, NS2(nsb, "demo", "cam")) ==
                   MOQR_OK);

    /* FILL the intent ring so the failover's reservation must fail */
    moq_bytes_t nsb2[2];
    for (int i = 0; i < 4; i++) {
        moq_bytes_t part = { (const uint8_t *)"x", 1 };
        moqr_ns_t xns = { .parts = &part, .count = 1 };
        (void)xns; (void)nsb2;
        /* one announce+unannounce pair produces no intents without
         * watchers; fill with subscribes instead: each new-track subscribe
         * pushes one UPSTREAM_SUBSCRIBE */
        char nm[8];
        snprintf(nm, sizeof(nm), "f%d", i);
        moqr_subscribe_req_t rq2;
        moqr_subscribe_req_init(&rq2);
        rq2.ns = NS2(nsb2, "demo", "cam");
        rq2.name = (moq_bytes_t){ (const uint8_t *)nm, strlen(nm) };
        rq2.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq2.cookie = 100 + (uint64_t)i;
        moqr_sub_t sx;
        if (moqr_core_subscribe(c, s1, &rq2, &sx) != MOQR_OK) {
            break;
        }
    }
    /* the close must make NO track mutation and report WOULD_BLOCK */
    moqr_result_t rc = moqr_core_binding_close(c, pubA, CTRL_NOW);
    if (rc == MOQR_ERR_WOULD_BLOCK) {
        /* zero-mutation until retry: drain, then the retry completes with
         * the identical retarget outcome — no idle nudge involved */
        (void)drain(c, its, 8);
        rc = moqr_core_binding_close(c, pubA, CTRL_NOW);
        while (rc == MOQR_ERR_WOULD_BLOCK) {
            (void)drain(c, its, 8);
            rc = moqr_core_binding_close(c, pubA, CTRL_NOW);
        }
    }
    MOQ_TEST_CHECK(rc == MOQR_OK);
    bool retarget_seen = false;
    while ((n = drain(c, its, 8)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE &&
                its[i].binding_cookie == 200 &&
                its[i].name.len == 5) {
                retarget_seen = true;
            }
        }
    }
    MOQ_TEST_CHECK(retarget_seen);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("source_failover_backpressure");
    return failures;
}

/* Post-revocation re-admission at the CORE: a force-withdraw terminates the
 * generation, and a later accepted re-announcement plus a fresh downstream
 * subscription must create a fresh upstream demand and carry data — three
 * generations, same publisher binding. */
static int
test_readmission_core(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pub, s1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 300, &s1) == MOQR_OK);
    moq_bytes_t nsb[2];
    moqr_intent_t its[16];

    for (int gen = 0; gen < 3; gen++) {
        MOQ_TEST_CHECK(moqr_core_announce(c, pub,
                                          NS2(nsb, "demo", "cam")) ==
                       MOQR_OK);
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = NS2(nsb, "demo", "cam");
        rq.name = B("video");
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 11 + (uint64_t)gen;
        moqr_sub_t sub1;
        MOQ_TEST_CHECK(moqr_core_subscribe(c, s1, &rq, &sub1) == MOQR_OK);
        size_t n = drain(c, its, 16);
        MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
        MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);
        MOQ_TEST_CHECK_EQ_U64(its[0].binding_cookie, 100);
        moqr_track_t track = its[0].track;
        MOQ_TEST_CHECK(moqr_core_upstream_ok(c, track, its[0].track_gen,
                                             7 + (uint64_t)gen, false, 0,
                                             0) == MOQR_OK);
        n = drain(c, its, 16);
        MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)1);
        MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_ACCEPT_SUB);
        MOQ_TEST_CHECK(ing(c, &a, track, (uint64_t)gen + 1, 0, 0, 128) ==
                       MOQR_OK);
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_delivery_done(
                           c, s1, MOQR_DELIVERY_DELIVERED, CTRL_NOW) ==
                       MOQR_OK);
        /* revoke this generation: the subscriber terminal AND the
         * upstream release — without the release the binding's upstream
         * slot outlives the track and the next generation dies dark */
        MOQ_TEST_CHECK(moqr_core_force_withdraw(
                           c, NS2(nsb, "demo", "cam"), 0x10,
                           CTRL_NOW) == MOQR_OK);
        n = drain(c, its, 16);
        MOQ_TEST_CHECK_EQ_SIZE(n, (size_t)2);
        MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_SUB_DONE);
        MOQ_TEST_CHECK_EQ_U64(its[1].kind,
                              MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
        MOQ_TEST_CHECK_EQ_U64(its[1].binding_cookie, 100);
        /* the wire-cancel to the publisher must be acknowledged, exactly
         * as the binding does after sending cancel_namespace */
        moqr_revoked_grant_t rg[4];
        size_t rn = moqr_core_peek_revoked_grants(c, rg, 4);
        for (size_t k = 0; k < rn; k++) {
            moqr_core_ack_revoked_grant(c, rg[k].binding_cookie,
                                        rg[k].session_cookie);
        }
    }

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("readmission_core");
    return failures;
}

/* A corrupt descriptor reaching a PUBLIC core entry must be refused before any
 * durable state moves. Aggregate counters cannot show this: a corrupt reset can
 * flip a record or subgroup terminal without changing any of them. Each entry
 * is therefore proven by its OWN transition — the record still completes, the
 * subgroup still takes its clean terminal, the subscription is still
 * deliverable — on an isolated fixture, so no earlier refusal can mask a later
 * one. The structs are copyable, so these shapes are reachable without a
 * constructor. */

/* Every descriptor shape a public entry must refuse. */
#define BAD_RESET_NONE    (moqr_reset_desc_none())
#define BAD_RESET_TAG     ((moqr_reset_desc_t){ (moqr_code_tag_t)99, 0x1u })
#define BAD_RESET_LOCAL   ((moqr_reset_desc_t){ MOQR_CODE_LOCAL, 4242u })
#define BAD_RESET_PDEXT   ((moqr_reset_desc_t){ MOQR_CODE_LOCAL_PD_EXTENSION, \
                                                0x7u })
#define BAD_RESET_WIDE    ((moqr_reset_desc_t){ MOQR_CODE_WIRE_D16, \
                                                UINT64_C(0x4000000000000000) })
#define BAD_PD_TAG        ((moqr_pd_desc_t){ (moqr_code_tag_t)99, 0x2u })
#define BAD_PD_LOCAL      ((moqr_pd_desc_t){ MOQR_CODE_LOCAL, 4242u })
#define BAD_PD_EXT        ((moqr_pd_desc_t){ MOQR_CODE_LOCAL_PD_EXTENSION, \
                                             0x2u })
#define BAD_PD_WIDE       ((moqr_pd_desc_t){ MOQR_CODE_WIRE_D18, \
                                             UINT64_C(0x4000000000000000) })

/* An invalid abandon must leave the record OPEN: it still completes and is
 * delivered COMPLETE, never ABANDONED. */
static int
test_invalid_abandon_record_no_mutation(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    moqr_binding_t pub, s1;
    moqr_sub_t sub1;

    MOQ_TEST_CHECK(c != NULL);
    if (c == NULL) {
        return failures;
    }
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);
    MOQ_TEST_CHECK(open_rec(c, track, 1, 0, 0, 20) == MOQR_OK);

    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 1, 0, 0,
                                            BAD_RESET_NONE) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 1, 0, 0,
                                            BAD_RESET_TAG) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 1, 0, 0,
                                            BAD_RESET_LOCAL) == MOQR_ERR_INVAL);
    /* The PD-only extension is not a reset cause. */
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 1, 0, 0,
                                            BAD_RESET_PDEXT) == MOQR_ERR_INVAL);
    /* Foreign wire is extensible, but only within the varint ceiling. */
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 1, 0, 0,
                                            BAD_RESET_WIDE) == MOQR_ERR_INVAL);

    /* The transition itself: the record was untouched, so it still completes
     * and arrives COMPLETE rather than ABANDONED. */
    MOQ_TEST_CHECK(append1(c, &a, track, 1, 0, 0, 20, 0xA0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_complete_record(c, track, 1, 0, 0) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(d.rec.obj_state == MOQR_OBJ_COMPLETE);
    MOQ_TEST_CHECK(d.rec.reset.tag == MOQR_CODE_NONE);

    moqr_core_destroy(c);
    return failures;
}

/* An invalid subgroup reset must leave the subgroup able to take its normal
 * terminal: downstream sees the clean seal, not a retained RESET. */
static int
test_invalid_reset_subgroup_no_mutation(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    moqr_binding_t pub, s1;
    moqr_sub_t sub1;

    MOQ_TEST_CHECK(c != NULL);
    if (c == NULL) {
        return failures;
    }
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);
    MOQ_TEST_CHECK(ing(c, &a, track, 1, 0, 0, 16) == MOQR_OK);

    MOQ_TEST_CHECK(moqr_core_reset_subgroup(c, track, 1, 0, BAD_RESET_NONE) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_reset_subgroup(c, track, 1, 0, BAD_RESET_PDEXT) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_reset_subgroup(c, track, 1, 0, BAD_RESET_TAG) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_reset_subgroup(c, track, 1, 0, BAD_RESET_LOCAL) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_reset_subgroup(c, track, 1, 0, BAD_RESET_WIDE) ==
                   MOQR_ERR_INVAL);

    /* The subgroup still takes its clean terminal: nothing downstream ever
     * sees a retained RESET. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, track, 1, 0) == MOQR_OK);
    int seals = 0;
    for (int k = 0; k < 8; k++) {
        moqr_delivery_t d;
        memset(&d, 0, sizeof(d));
        if (moqr_core_next_delivery(c, s1, CTRL_NOW, &d) != MOQR_OK) {
            break;
        }
        if (d.notice == MOQR_DELIVERY_NOTICE_SEAL) {
            seals++;
            MOQ_TEST_CHECK(!d.seal_reset);   /* clean seal, not a RESET */
        }
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }
    MOQ_TEST_CHECK(seals >= 1);

    moqr_core_destroy(c);
    return failures;
}

/* An invalid PD terminal must leave the subscription live: no intent appears,
 * and a later valid revoke still succeeds exactly once. */
static int
test_invalid_pd_entry_no_mutation(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    moqr_binding_t pub, s1;
    moqr_sub_t sub1;

    MOQ_TEST_CHECK(c != NULL);
    if (c == NULL) {
        return failures;
    }
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);
    moqr_intent_t its[8];
    (void)moqr_core_poll_intents(c, its, 8);

    MOQ_TEST_CHECK(moqr_core_source_done(c, track, 1, BAD_PD_TAG, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_source_done(c, track, 1, BAD_PD_LOCAL, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    /* A registered number smuggled under the extension tag. */
    MOQ_TEST_CHECK(moqr_core_source_done(c, track, 1, BAD_PD_EXT, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_source_done(c, track, 1, BAD_PD_WIDE, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, sub1, BAD_PD_TAG, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, sub1, BAD_PD_LOCAL, CTRL_NOW) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, sub1, BAD_PD_EXT, CTRL_NOW) ==
                   MOQR_ERR_INVAL);

    /* No terminal was queued for anyone. */
    size_t n = moqr_core_poll_intents(c, its, 8);
    for (size_t i = 0; i < n; i++) {
        MOQ_TEST_CHECK(its[i].kind != MOQR_INTENT_SUB_DONE);
    }
    /* The subscription is still deliverable. */
    MOQ_TEST_CHECK(ing(c, &a, track, 2, 0, 0, 16) == MOQR_OK);
    moqr_delivery_t d;
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &d) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                           CTRL_NOW) == MOQR_OK);

    /* And a valid revoke still works, once. */
    moqr_pd_desc_t good;
    MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_UNAUTHORIZED, &good) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, sub1, good, CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, sub1, good, CTRL_NOW) ==
                   MOQR_ERR_STALE_HANDLE);

    moqr_core_destroy(c);
    return failures;
}

/* An invalid group abandon must leave every OPEN record in the group
 * completable: each still delivers COMPLETE and no reset notice appears. A
 * separate fixture proves the valid call does abandon, so the refusals above
 * are the descriptor and not the group. */
static int
test_invalid_abandon_group_no_mutation(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    moqr_binding_t pub, s1;
    moqr_sub_t sub1;

    MOQ_TEST_CHECK(c != NULL);
    if (c == NULL) {
        return failures;
    }
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);
    /* Two OPEN records in the same group, one per subgroup (a subgroup holds
     * at most one open object at a time). */
    MOQ_TEST_CHECK(open_rec(c, track, 3, 0, 0, 20) == MOQR_OK);
    MOQ_TEST_CHECK(open_rec(c, track, 3, 1, 1, 20) == MOQR_OK);

    MOQ_TEST_CHECK(moqr_core_abandon_group_open(c, track, 3, BAD_RESET_NONE) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_abandon_group_open(c, track, 3, BAD_RESET_TAG) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_abandon_group_open(c, track, 3, BAD_RESET_LOCAL) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_abandon_group_open(c, track, 3, BAD_RESET_PDEXT) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_core_abandon_group_open(c, track, 3, BAD_RESET_WIDE) ==
                   MOQR_ERR_INVAL);

    /* Both records are untouched: they complete and arrive COMPLETE. */
    /* (group 3, subgroup 0, object 0) and (group 3, subgroup 1, object 1):
     * object ids ascend across the group. */
    const uint64_t sgs[2] = { 0, 1 };
    const uint64_t objs[2] = { 0, 1 };
    for (int k = 0; k < 2; k++) {
        MOQ_TEST_CHECK(append1(c, &a, track, 3, sgs[k], objs[k], 20,
                               (uint8_t)(0xB0 + k)) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_complete_record(c, track, 3, sgs[k],
                                                 objs[k]) == MOQR_OK);
    }
    int complete_seen = 0;
    for (int k = 0; k < 8; k++) {
        moqr_delivery_t d;
        memset(&d, 0, sizeof(d));
        if (moqr_core_next_delivery(c, s1, CTRL_NOW, &d) != MOQR_OK) {
            break;
        }
        if (d.notice == 0) {
            MOQ_TEST_CHECK(d.rec.obj_state == MOQR_OBJ_COMPLETE);
            MOQ_TEST_CHECK(d.rec.reset.tag == MOQR_CODE_NONE);
            complete_seen++;
        } else if (d.notice == MOQR_DELIVERY_NOTICE_SEAL) {
            MOQ_TEST_CHECK(!d.seal_reset);
        }
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(complete_seen, 2);
    moqr_core_destroy(c);
    return failures;
}

/* The valid control, in its own fixture: the same entry that refuses every
 * corrupt descriptor must actually act when given a good one, or the refusal
 * fixture proves nothing.
 *
 * The observable is the record state itself: after the sweep, an OPEN record
 * can no longer be completed. That is production behaviour, not a model — the
 * refusal fixture above proves the same records DO still complete when the
 * descriptor is rejected, so the two fixtures differ only by the descriptor.
 *
 * MOQR_OK is required exactly: this group is retained, so MOQR_DONE would mean
 * the sweep found nothing and the control was vacuous. */
static int
test_valid_abandon_group_control(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    moqr_binding_t pub, s1;
    moqr_sub_t sub1;

    MOQ_TEST_CHECK(c != NULL);
    if (c == NULL) {
        return failures;
    }
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    const uint64_t sgs[2] = { 0, 1 };
    const uint64_t objs[2] = { 0, 1 };
    for (int k = 0; k < 2; k++) {
        MOQ_TEST_CHECK(open_rec(c, track, 4, sgs[k], objs[k], 40) == MOQR_OK);
        MOQ_TEST_CHECK(append1(c, &a, track, 4, sgs[k], objs[k], 10,
                               (uint8_t)(0xC0 + k)) == MOQR_OK);
    }

    moqr_reset_desc_t good;
    MOQ_TEST_CHECK(moqr_reset_desc_local(MOQR_RESET_CANCELLED, &good) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_abandon_group_open(c, track, 4, good) == MOQR_OK);

    /* Both records left OPEN: neither can be completed any more. In the
     * refusal fixture the identical calls still complete. */
    for (int k = 0; k < 2; k++) {
        MOQ_TEST_CHECK(moqr_core_complete_record(c, track, 4, sgs[k],
                                                 objs[k]) ==
                       MOQR_ERR_WRONG_STATE);
    }

    /* And a corrupt descriptor at the same entry is still refused outright,
     * never MOQR_DONE — the difference is the descriptor. */
    MOQ_TEST_CHECK(moqr_core_abandon_group_open(c, track, 4, BAD_RESET_TAG) ==
                   MOQR_ERR_INVAL);

    moqr_core_destroy(c);
    return failures;
}

/* Test-only core seam: both halves of the begun ledger for one subscription.
 * Declared here because it is deliberately absent from every installed
 * header. */
extern bool moqr_core_debug_begun_ledger(const moqr_core_t *c, moqr_sub_t sh,
                                         uint32_t *out_sub_begun,
                                         uint32_t *out_binding_begun_subs);

/* Assert both halves at once; the two are records of the same fact. */
#define CHECK_LEDGER(c, sub, want_sub, want_binding)                      \
    do {                                                                  \
        uint32_t sb_ = 0xFFFFFFFFu, bb_ = 0xFFFFFFFFu;                    \
        MOQ_TEST_CHECK(moqr_core_debug_begun_ledger((c), (sub), &sb_,     \
                                                    &bb_));               \
        MOQ_TEST_CHECK_EQ_U64(sb_, (uint32_t)(want_sub));                 \
        MOQ_TEST_CHECK_EQ_U64(bb_, (uint32_t)(want_binding));             \
    } while (0)

/* A held OPEN delivery owns a frozen snapshot: MOQR_DELIVERY_WOULD_BLOCK pins
 * the record and its chunk batch so the binding can resume its sub-object write
 * cursor without skipping or duplicating a chunk. An abandon landing underneath
 * therefore must NOT rewrite that snapshot — but it must not be lost either.
 * This pins the whole sequence: the snapshot survives verbatim, and the terminal
 * arrives exactly once as soon as the hold is released.
 *
 * `emitted_zero` runs the same sequence having checkpointed zero chunks, so the
 * begun-but-nothing-written arm is covered too. */
static int
held_abandon_sequence(bool emitted_zero)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    moqr_binding_t pub, s1;
    moqr_sub_t sub1;

    MOQ_TEST_CHECK(c != NULL);
    if (c == NULL) {
        return failures;
    }
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    moqr_track_t track = chunked_sub_setup(c, pub, s1, &sub1);

    CHECK_LEDGER(c, sub1, 0, 0);   /* (1) nothing begun yet */

    /* (1) an OPEN chunked record at the live edge */
    MOQ_TEST_CHECK(open_rec(c, track, 9, 0, 0, 60) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 9, 0, 0, 10, 0xE0) == MOQR_OK);
    MOQ_TEST_CHECK(append1(c, &a, track, 9, 0, 0, 10, 0xE1) == MOQR_OK);

    moqr_delivery_t first;
    memset(&first, 0, sizeof(first));
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &first) == MOQR_OK);
    MOQ_TEST_CHECK(first.rec.obj_state == MOQR_OBJ_OPEN);
    uint32_t pinned = first.rec.chunk_count;
    MOQ_TEST_CHECK(pinned >= 2);

    /* (2) begin downstream and checkpoint the exact emitted count */
    uint32_t emitted = emitted_zero ? 0u : 1u;
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, emitted, CTRL_NOW) ==
                   MOQR_OK);
    /* (2) BOTH arms are begun here, including emitted == 0: the documented
     * contract is that note_emitted marks the object begun downstream
     * regardless of the count, because the header may already be on the wire
     * with zero payload bytes behind it. Asserting the real transition rather
     * than the one I first assumed. */
    CHECK_LEDGER(c, sub1, 1, 1);

    /* (3) the write blocks: the snapshot is now held */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_WOULD_BLOCK,
                                           CTRL_NOW) == MOQR_OK);

    /* (4) the source dies underneath the hold, with a distinctive cause */
    moqr_reset_desc_t cause;
    MOQ_TEST_CHECK(moqr_reset_desc_local(MOQR_RESET_DELIVERY_TIMEOUT, &cause) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_abandon_record(c, track, 9, 0, 0, cause) ==
                   MOQR_OK);

    /* (3) the abandon underneath the frozen hold does not release or rewrite
     * the accounting early. */
    CHECK_LEDGER(c, sub1, 1, 1);

    /* (5) the re-peek replays the held snapshot verbatim: still OPEN, same
     * chunk pinning, and no terminal laundered into it. */
    moqr_delivery_t again;
    memset(&again, 0, sizeof(again));
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &again) == MOQR_OK);
    MOQ_TEST_CHECK(again.rec.obj_state == MOQR_OBJ_OPEN);
    MOQ_TEST_CHECK(again.rec.reset.tag == MOQR_CODE_NONE);
    MOQ_TEST_CHECK_EQ_U64(again.rec.group_id, first.rec.group_id);
    MOQ_TEST_CHECK_EQ_U64(again.rec.subgroup_id, first.rec.subgroup_id);
    MOQ_TEST_CHECK_EQ_U64(again.rec.object_id, first.rec.object_id);
    MOQ_TEST_CHECK_EQ_U64(again.rec.chunk_count, pinned);

    /* (6) finish the held batch as the binding does: the snapshot is still an
     * OPEN object that has not grown, so it reports STALLED. */
    MOQ_TEST_CHECK(moqr_core_delivery_note_emitted(c, s1, pinned, CTRL_NOW) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_STALLED,
                                           CTRL_NOW) == MOQR_OK);

    /* (7) the very next selection is the terminal, carrying the exact cause. */
    moqr_delivery_t term;
    memset(&term, 0, sizeof(term));
    MOQ_TEST_CHECK(moqr_core_next_delivery(c, s1, CTRL_NOW, &term) == MOQR_OK);
    MOQ_TEST_CHECK(term.rec.obj_state == MOQR_OBJ_ABANDONED);
    MOQ_TEST_CHECK(term.rec.reset.tag == MOQR_CODE_LOCAL);
    MOQ_TEST_CHECK_EQ_U64(term.rec.reset.value, MOQR_RESET_DELIVERY_TIMEOUT);
    MOQ_TEST_CHECK_EQ_U64(term.rec.object_id, first.rec.object_id);

    /* (8) reported once, never duplicated, subscription still live */
    MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_ABANDONED,
                                           CTRL_NOW) == MOQR_OK);
    /* (4) the begun position is released exactly when the terminal is
     * confirmed — both halves back to zero. */
    CHECK_LEDGER(c, sub1, 0, 0);

    int extra_terminals = 0;
    for (int k = 0; k < 6; k++) {
        moqr_delivery_t d;
        memset(&d, 0, sizeof(d));
        if (moqr_core_next_delivery(c, s1, CTRL_NOW, &d) != MOQR_OK) {
            break;
        }
        if (d.notice == 0 && d.rec.obj_state == MOQR_OBJ_ABANDONED &&
            d.rec.object_id == first.rec.object_id) {
            extra_terminals++;
        }
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(extra_terminals, 0);

    /* The begun ledger for the abandoned object is genuinely cleared, not just
     * quiet: push the eviction watermark past its group and drain the
     * legitimate maintenance. A stale begun bit would surface a second,
     * synthetic terminal for that object here. */
    for (uint64_t g = 20; g < 26; g++) {
        MOQ_TEST_CHECK(ing(c, &a, track, g, 0, g, 16) == MOQR_OK);
    }
    int stale_terminals = 0;
    int drained = 0;
    for (int k = 0; k < 24; k++) {
        moqr_delivery_t d;
        memset(&d, 0, sizeof(d));
        if (moqr_core_next_delivery(c, s1, CTRL_NOW, &d) != MOQR_OK) {
            break;
        }
        if (d.rec.group_id == 9 &&
            (d.rec.obj_state == MOQR_OBJ_ABANDONED ||
             d.rec.reset.tag != MOQR_CODE_NONE)) {
            stale_terminals++;
        }
        drained++;
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_INT(stale_terminals, 0);
    CHECK_LEDGER(c, sub1, 0, 0);
    (void)drained;

    /* the subscription is still usable afterwards */
    MOQ_TEST_CHECK(ing(c, &a, track, 30, 0, 30, 16) == MOQR_OK);
    bool served_after = false;
    for (int k = 0; k < 8 && !served_after; k++) {
        moqr_delivery_t after;
        memset(&after, 0, sizeof(after));
        if (moqr_core_next_delivery(c, s1, CTRL_NOW, &after) != MOQR_OK) {
            break;
        }
        if (after.notice == MOQR_DELIVERY_NOTICE_NONE &&
            after.rec.group_id == 30) {
            served_after = true;
        }
        MOQ_TEST_CHECK(moqr_core_delivery_done(c, s1, MOQR_DELIVERY_DELIVERED,
                                               CTRL_NOW) == MOQR_OK);
    }
    MOQ_TEST_CHECK(served_after);
    /* (5) and stays clear through eviction and the later delivery. */
    CHECK_LEDGER(c, sub1, 0, 0);

    moqr_core_destroy(c);
    return failures;
}

static int
test_held_abandon_release_terminal(void)
{
    return held_abandon_sequence(false) + held_abandon_sequence(true);
}

/* The seam itself fails closed: an unknown core, a stale subscription, or a
 * null output is refused with both outputs left exactly as the caller had
 * them. A debug reader that scribbles on refusal is worse than none. */
static int
test_begun_ledger_seam_fail_closed(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, NULL);
    moqr_binding_t pub, s1;
    moqr_sub_t sub1;

    MOQ_TEST_CHECK(c != NULL);
    if (c == NULL) {
        return failures;
    }
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 100, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 200, &s1) == MOQR_OK);
    (void)chunked_sub_setup(c, pub, s1, &sub1);

    uint32_t sb = 0xA5A5A5A5u, bb = 0x5A5A5A5Au;

    /* a live subscription reads */
    MOQ_TEST_CHECK(moqr_core_debug_begun_ledger(c, sub1, &sb, &bb));

    /* null core / null outputs are refused without writing */
    sb = 0xA5A5A5A5u; bb = 0x5A5A5A5Au;
    MOQ_TEST_CHECK(!moqr_core_debug_begun_ledger(NULL, sub1, &sb, &bb));
    MOQ_TEST_CHECK(!moqr_core_debug_begun_ledger(c, sub1, NULL, &bb));
    MOQ_TEST_CHECK(!moqr_core_debug_begun_ledger(c, sub1, &sb, NULL));
    MOQ_TEST_CHECK_EQ_U64(sb, 0xA5A5A5A5u);
    MOQ_TEST_CHECK_EQ_U64(bb, 0x5A5A5A5Au);

    /* a stale handle is refused, outputs untouched */
    moqr_sub_t bogus = { sub1._opaque ^ 0xDEAD0000ull };
    MOQ_TEST_CHECK(!moqr_core_debug_begun_ledger(c, bogus, &sb, &bb));
    MOQ_TEST_CHECK_EQ_U64(sb, 0xA5A5A5A5u);
    MOQ_TEST_CHECK_EQ_U64(bb, 0x5A5A5A5Au);

    /* and after the subscription is retired, its handle no longer reads */
    moqr_pd_desc_t good;
    MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_UNAUTHORIZED, &good) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_revoke_sub(c, sub1, good, CTRL_NOW) == MOQR_OK);
    MOQ_TEST_CHECK(!moqr_core_debug_begun_ledger(c, sub1, &sb, &bb));
    MOQ_TEST_CHECK_EQ_U64(sb, 0xA5A5A5A5u);
    MOQ_TEST_CHECK_EQ_U64(bb, 0x5A5A5A5Au);

    moqr_core_destroy(c);
    return failures;
}

/* The verdict's revocation terminal must be cleared by the core BEFORE the hook
 * runs, so a hook that says nothing about it cannot inherit whatever the
 * caller's stack object happened to hold. Proven with one output object reused
 * across two calls: the hook states a terminal on the first and deliberately
 * leaves the field alone on the second. */
static int g_a4_calls;

static void
a4_hook(void *ctx, const moqr_auth_request_t *req, moqr_auth_verdict_t *out)
{
    (void)ctx;
    (void)req;
    g_a4_calls++;
    out->decision = MOQR_AUTH_DENY;
    out->error_code = 0x11u;
    out->reason = MOQR_AUTH_REASON_POLICY;
    if (g_a4_calls == 1) {
        moqr_pd_desc_t t;

        if (moqr_pd_desc_extension(0x7u, &t) == MOQR_OK) {
            out->revoke_terminal = t;
        }
    }
    /* call 2 says nothing about revoke_terminal, on purpose */
}

static int
test_verdict_revoke_terminal_defaults_none(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.authorize = a4_hook;
    moqr_core_t *c = NULL;

    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    if (c == NULL) {
        return failures;
    }
    g_a4_calls = 0;

    moqr_auth_request_t req;

    memset(&req, 0, sizeof(req));
    req.struct_size = (uint32_t)sizeof(req);
    req.action = MOQR_AUTH_SUBSCRIBE;
    req.now_us = 1000;

    /* ONE output object, reused: the second call must not inherit the first. */
    moqr_auth_verdict_t v;

    memset(&v, 0, sizeof(v));

    moqr_core_authorize(c, &req, &v);
    MOQ_TEST_CHECK(v.decision == MOQR_AUTH_DENY);
    MOQ_TEST_CHECK_EQ_U64(v.error_code, 0x11u);
    MOQ_TEST_CHECK(v.revoke_terminal.tag == MOQR_CODE_LOCAL_PD_EXTENSION);
    MOQ_TEST_CHECK_EQ_U64(v.revoke_terminal.value, 0x7u);

    moqr_core_authorize(c, &req, &v);
    /* the ordinary fields are still what the hook said... */
    MOQ_TEST_CHECK(v.decision == MOQR_AUTH_DENY);
    MOQ_TEST_CHECK_EQ_U64(v.error_code, 0x11u);
    MOQ_TEST_CHECK(v.reason == MOQR_AUTH_REASON_POLICY);
    /* ...and the terminal the hook did NOT state is exactly NONE. */
    MOQ_TEST_CHECK(v.revoke_terminal.tag == MOQR_CODE_NONE);
    MOQ_TEST_CHECK_EQ_INT(g_a4_calls, 2);

    moqr_core_destroy(c);
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_announce_subscribe_fanout();
    failures += test_per_binding_sub_index();
    failures += test_gpos_index();
    failures += test_evict_sweep_gate();
    failures += test_shard_seams();
    failures += test_sub_state_seam();
    failures += test_filters_and_range();
    failures += test_warm_linger_and_status();
    failures += test_stream_error_retire_linger();
    failures += test_ns_discovery_and_close();
    failures += test_reserve_and_capacity_refusals();
    failures += test_scheduling_order();
    failures += test_delivery_pin_and_status_pending();
    failures += test_handle_and_view_hygiene();
    failures += test_wide_group_scheduling();
    failures += test_namespace_length_bounds();
    failures += test_intent_ring_invariant();
    failures += test_multi_namespace_close();
    failures += test_observability_counters();
    failures += test_auth_canonical_verdict();
    failures += test_parked_storage();
    failures += test_grant_storage();
    failures += test_revoke_sub();
    failures += test_grant_reval();
    failures += test_grant_reval_announce();
    failures += test_grant_teardown_resumes_after_partial();
    failures += test_grant_reval_stale_owner();
    failures += test_grant_retire_no_reval();
    failures += test_chunked_delivery_core();
    failures += test_refused_unbegun_contract();
    failures += test_forward_pause_release();
    failures += test_forward_pause_partial_resume();
    failures += test_forward_pause_partial_abandon();
    failures += test_forward_pause_begun_zero_abandon();
    failures += test_note_emitted_range_contract();
    failures += test_forward_pause_seal_maintenance();
    failures += test_forward_pause_evict_maintenance();
    failures += test_subscribe_prefix_compat();
    failures += test_chunked_delivery_evict_hold();
    failures += test_open_liveedge_delivery();
    failures += test_open_abandon();
    failures += test_liveedge_no_hol();
    failures += test_liveedge_evict_reset();
    failures += test_seal_notice_late_fin();
    failures += test_seal_flag_single_notice();
    failures += test_seal_ack_slot_reuse();
    failures += test_seal_order_lowest_subgroup();
    failures += test_seal_notice_priority();
    failures += test_capacity_gpos_oracle();
    failures += test_root_namespace_roundtrip();
    failures += test_source_failover();
    failures += test_readmission_core();
    failures += test_source_failover_backpressure();
    failures += test_root_full_track_name_core();
    failures += test_empty_namespace_field_rejected_core();
    failures += test_invalid_abandon_record_no_mutation();
    failures += test_invalid_reset_subgroup_no_mutation();
    failures += test_invalid_pd_entry_no_mutation();
    failures += test_invalid_abandon_group_no_mutation();
    failures += test_valid_abandon_group_control();
    failures += test_held_abandon_release_terminal();
    failures += test_begun_ledger_seam_fail_closed();
    failures += test_verdict_revoke_terminal_defaults_none();
    return failures == 0 ? 0 : 1; /* exit status truncates to 8 bits */






}
