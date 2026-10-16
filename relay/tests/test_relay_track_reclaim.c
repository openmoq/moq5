/*
 * Track-table reclaim. A WARM track (source released, no subscription, no
 * fetch, no pending upstream) holds only cache, so it never blocks admission:
 * a full table evicts the least recently used one, and an idle bound frees it
 * on tick. Live and pinned tracks are never evicted; a table of them refuses
 * with MOQR_ERR_CAPACITY. Allocator balance zero ends every test.
 */

#include <moq/relay/relay.h>
#include <moq/relay/log.h>
#include <moq/relay/trace.h>

#include <moq/rcbuf.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

typedef struct ca {
    moq_alloc_t vt;
    long live;
    long fail_after;   /* < 0 = never; else allocations left before NULL */
} ca_t;

static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    if (a->fail_after == 0) {
        return NULL;
    }
    if (a->fail_after > 0) {
        a->fail_after--;
    }
    void *p = malloc(n);
    if (p) {
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
    a->fail_after = -1;
}

static moq_bytes_t
B(const char *s)
{
    return (moq_bytes_t){ .data = (const uint8_t *)s, .len = strlen(s) };
}

/* One-part namespace view over caller storage. */
static moqr_ns_t
NS1(moq_bytes_t *storage, const char *a)
{
    storage[0] = B(a);
    return (moqr_ns_t){ .parts = storage, .count = 1 };
}

/* A small table, no upstream linger, no age limit on the log,
 * and a byte budget no session ever reaches. */
static moqr_core_t *
mkcore(ca_t *a, uint32_t max_tracks, uint64_t warm_idle_us,
       moqr_trace_t *trace)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.trace = trace;
    cfg.max_tracks = max_tracks;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.log_budget.max_age_us = 0;
    cfg.linger_us = 0;
    cfg.warm_idle_us = warm_idle_us;
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

static size_t
drain(moqr_core_t *c, moqr_intent_t *out, size_t cap)
{
    return moqr_core_poll_intents(c, out, cap);
}

static moqr_result_t
subscribe(moqr_core_t *c, moqr_binding_t b, const char *ns, const char *name,
          uint64_t cookie, moqr_sub_t *out)
{
    moq_bytes_t nsb[1];
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS1(nsb, ns);
    rq.name = B(name);
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = cookie;
    return moqr_core_subscribe(c, b, &rq, out);
}

/* A pull session: subscribe to an announced track, the upstream accepts, one
 * object arrives, the subscriber leaves, and the upstream is released on the
 * next tick (linger_us = 0). The track ends WARM with a retained record. */
static int
pull_cycle(moqr_core_t *c, ca_t *a, moqr_binding_t sb, const char *ns,
           const char *name, uint64_t now_us, moqr_track_t *out_track)
{
    int failures = 0;
    moqr_intent_t its[8];
    moqr_sub_t sub;
    MOQ_TEST_CHECK(subscribe(c, sb, ns, name, 1, &sub) == MOQR_OK);
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    if (n != 1) {
        return failures + 1;
    }
    moqr_track_t th = its[0].track;
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, th, its[0].track_gen, 5, false, 0,
                                         0) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK(ing(c, a, th, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, now_us) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_tick(c, now_us) == MOQR_OK);
    (void)drain(c, its, 8);   /* UPSTREAM_UNSUBSCRIBE */
    if (out_track != NULL) {
        *out_track = th;
    }
    return failures;
}

/* A push session: PUBLISH, one object, PUBLISH_DONE. WARM with a record. */
static int
push_cycle(moqr_core_t *c, ca_t *a, moqr_binding_t pb, const char *ns,
           const char *name, uint64_t now_us, moqr_track_t *out_track)
{
    int failures = 0;
    moqr_intent_t its[8];
    moq_bytes_t nsb[1];
    moqr_track_t th;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, NS1(nsb, ns), B(name), 9,
                                          &th) == MOQR_OK);
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_ACCEPT_PUBLISH);
    if (n != 1 || its[0].kind != MOQR_INTENT_ACCEPT_PUBLISH) {
        return failures + 1;
    }
    MOQ_TEST_CHECK(ing(c, a, th, 0, 0) == MOQR_OK);
    moqr_pd_desc_t done;
    MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_TRACK_ENDED, &done) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_source_done(c, th, its[0].track_gen, done,
                                         now_us) == MOQR_OK);
    (void)drain(c, its, 8);
    if (out_track != NULL) {
        *out_track = th;
    }
    return failures;
}

static moqr_core_stats_t
reclaim_stats(const moqr_core_t *c)
{
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    return st;
}

static uint32_t
live_tracks(const moqr_core_t *c)
{
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    return st.tracks;
}

/* Table exhaustion by finished publications: a small table, linger 0 and no
 * log age limit. Every finished publication (pull or push, its publisher
 * session then gone) leaves a WARM track with retained records. After many
 * more finished publications than slots, a fresh SUBSCRIBE for a new track
 * must still be admitted, and no refusal may be counted. */
static int
test_publish_then_leave_cycles(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 4, UINT64_MAX, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moq_bytes_t nsb[1];
    uint64_t now = 1000;
    char ns[16], name[16];
    for (int cycle = 0; cycle < 12; cycle++) {
        moqr_binding_t pb;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 100 + (uint64_t)cycle,
                                              &pb) == MOQR_OK);
        snprintf(ns, sizeof(ns), "ns%d", cycle);
        snprintf(name, sizeof(name), "t%d", cycle);
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, ns)) == MOQR_OK);
        if ((cycle & 1) == 0) {
            failures += pull_cycle(c, &a, sb, ns, name, now, NULL);
        } else {
            failures += push_cycle(c, &a, pb, ns, name, now, NULL);
        }
        /* The publisher's session leaves. */
        moqr_result_t crc;
        while ((crc = moqr_core_binding_close(c, pb, now)) ==
               MOQR_ERR_WOULD_BLOCK) {
            moqr_intent_t its[16];
            (void)drain(c, its, 16);
        }
        MOQ_TEST_CHECK(crc == MOQR_OK);
        moqr_intent_t its[16];
        (void)drain(c, its, 16);
        now += 1000;
        MOQ_TEST_CHECK(moqr_core_tick(c, now) == MOQR_OK);
        MOQ_TEST_CHECK(live_tracks(c) <= 4);
    }
    /* A fresh publisher and a fresh SUBSCRIBE needing a new track. */
    moqr_binding_t pb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 999, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "fresh")) == MOQR_OK);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(subscribe(c, sb, "fresh", "v", 77, &sub) == MOQR_OK);
    moqr_intent_t its[8];
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_TRACKS], 0);
    MOQ_TEST_CHECK_EQ_U64(st.tracks, 4);
    moqr_core_stats_t rs = reclaim_stats(c);
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_evicted_capacity, 9);   /* 13 tracks, 4 slots */
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_evicted_idle, 0);
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_reclaimable, 3);
    /* The discarded cache records count as record evictions. */
    MOQ_TEST_CHECK(st.evicted_total >= 9);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("publish_then_leave_cycles");
    return failures;
}

/* A table full of LIVE tracks (an ACTIVE subscribed track and an ACTIVE push
 * source) refuses a new track with MOQR_ERR_CAPACITY, counted once under
 * refusals[tracks], and evicts nothing. */
static int
test_live_tracks_refuse(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 2, UINT64_MAX, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moq_bytes_t nsb[1];
    moqr_intent_t its[8];
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "n")) == MOQR_OK);
    moqr_sub_t live;
    MOQ_TEST_CHECK(subscribe(c, sb, "n", "a", 1, &live) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, its[0].track, its[0].track_gen, 3,
                                         false, 0, 0) == MOQR_OK);
    (void)drain(c, its, 8);
    moqr_track_t pushed;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, NS1(nsb, "n"), B("b"), 2,
                                          &pushed) == MOQR_OK);
    (void)drain(c, its, 8);

    moqr_sub_t refused;
    MOQ_TEST_CHECK(subscribe(c, sb, "n", "c", 3, &refused) ==
                   MOQR_ERR_CAPACITY);
    moqr_track_t refused_push;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, NS1(nsb, "n"), B("d"), 4,
                                          &refused_push) == MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)0);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_TRACKS], 2);
    MOQ_TEST_CHECK_EQ_U64(st.tracks, 2);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);
    MOQ_TEST_CHECK(ing(c, &a, pushed, 0, 0) == MOQR_OK);   /* still live */
    moqr_core_stats_t rs = reclaim_stats(c);
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_evicted_capacity, 0);
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_reclaimable, 0);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("live_tracks_refuse");
    return failures;
}

/* Pending work pins a track: a WARM track read by an open FETCH, and a track
 * whose upstream SUBSCRIBE is still unanswered (the state a rendezvous or a
 * cross-shard demand also holds), are never evicted. Closing the fetch makes
 * its track reclaimable again. */
static int
test_pinned_tracks_safe(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 2, UINT64_MAX, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moq_bytes_t nsb[1];
    moqr_intent_t its[8];
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "p")) == MOQR_OK);

    /* Track A: WARM with a record, then pinned by a FETCH. */
    moqr_track_t ta;
    failures += push_cycle(c, &a, pb, "p", "a", 10, &ta);
    moqr_fetch_req_t fr;
    moqr_fetch_req_init(&fr);
    fr.ns = NS1(nsb, "p");
    fr.name = B("a");
    fr.start_group = 0;
    fr.end_group = 0;
    fr.cookie = 50;
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sb, &fr, 11, &fh, &plan) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(plan.admit == MOQR_FETCH_ACCEPT);

    /* Track B: PENDING, its upstream request unanswered. */
    moqr_sub_t pending;
    MOQ_TEST_CHECK(subscribe(c, sb, "p", "b", 2, &pending) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_UPSTREAM_SUBSCRIBE);

    /* Both pinned: a third track is refused and nothing is evicted. */
    moqr_sub_t third;
    MOQ_TEST_CHECK(subscribe(c, sb, "p", "c", 3, &third) == MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(moqr_core_tick(c, 1000000000ull) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(reclaim_stats(c).tracks_reclaimable, 0);
    MOQ_TEST_CHECK_EQ_U64(live_tracks(c), 2);

    /* The fetch still serves the pinned cache. */
    moqr_fetch_item_t fo;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, 12, &fo) == MOQR_OK);
    MOQ_TEST_CHECK(fo.kind == MOQR_FETCH_ITEM_OBJECT &&
                   fo.rec.group_id == 0 && fo.rec.object_id == 0);
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    /* Unpinned: the next new track takes A's slot; B is still pending. */
    MOQ_TEST_CHECK_EQ_U64(reclaim_stats(c).tracks_reclaimable, 1);
    MOQ_TEST_CHECK(subscribe(c, sb, "p", "c", 3, &third) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(reclaim_stats(c).tracks_evicted_capacity, 1);
    MOQ_TEST_CHECK(moqr_core_ingest(c, ta, NULL) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(ing(c, &a, ta, 1, 0) == MOQR_ERR_STALE_HANDLE);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_parked, 2);   /* B and C both pending */
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_TRACKS], 1);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("pinned_tracks_safe");
    return failures;
}

/* Track-status answer for (ns, name): true = the track still exists. */
static bool
track_known_ns(moqr_core_t *c, moqr_binding_t b, const char *ns,
               const char *name)
{
    moq_bytes_t nsb[1];
    moqr_intent_t its[4];
    if (moqr_core_track_status(c, b, NS1(nsb, ns), B(name), 1) != MOQR_OK ||
        drain(c, its, 4) != 1) {
        return false;
    }
    return its[0].kind == MOQR_INTENT_TRACK_STATUS_OK;
}

static bool
track_known(moqr_core_t *c, moqr_binding_t b, const char *name)
{
    return track_known_ns(c, b, "l", name);
}

/* The least recently used reclaimable track is evicted first. Recency is the
 * last interest change: creation, a subscription joining or leaving, a fetch
 * opening or closing, the source being released. Touching the oldest track
 * (a subscriber rejoins its cache and leaves) makes it the newest. */
static int
test_lru_order(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 3, UINT64_MAX, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moqr_intent_t its[8];
    failures += push_cycle(c, &a, pb, "l", "x", 10, NULL);
    failures += push_cycle(c, &a, pb, "l", "y", 20, NULL);
    failures += push_cycle(c, &a, pb, "l", "z", 30, NULL);

    /* x is the oldest; a subscriber rejoins x's cache and leaves. */
    moqr_sub_t s;
    MOQ_TEST_CHECK(subscribe(c, sb, "l", "x", 5, &s) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);   /* ACCEPT only */
    MOQ_TEST_CHECK_EQ_U64(its[0].kind, MOQR_INTENT_ACCEPT_SUB);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, s, 40) == MOQR_OK);

    /* Order is now y, z, x: two new tracks evict y then z. */
    failures += push_cycle(c, &a, pb, "l", "n1", 50, NULL);
    MOQ_TEST_CHECK(!track_known(c, sb, "y"));
    MOQ_TEST_CHECK(track_known(c, sb, "z"));
    MOQ_TEST_CHECK(track_known(c, sb, "x"));
    failures += push_cycle(c, &a, pb, "l", "n2", 60, NULL);
    MOQ_TEST_CHECK(!track_known(c, sb, "z"));
    MOQ_TEST_CHECK(track_known(c, sb, "x"));
    MOQ_TEST_CHECK(track_known(c, sb, "n1"));
    MOQ_TEST_CHECK(track_known(c, sb, "n2"));
    MOQ_TEST_CHECK_EQ_U64(reclaim_stats(c).tracks_evicted_capacity, 2);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("lru_order");
    return failures;
}

/* The idle bound frees a reclaimable track on a tick at least warm_idle_us
 * after the first tick that saw it idle, even with free slots; interest in
 * between restarts the clock; the default is 60 s and UINT64_MAX disables
 * it. */
static int
test_idle_linger(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 8, 5000, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moqr_intent_t its[8];
    moqr_track_t th;
    failures += push_cycle(c, &a, pb, "l", "w", 100, &th);
    MOQ_TEST_CHECK(moqr_core_tick(c, 1000) == MOQR_OK);     /* armed @1000 */
    MOQ_TEST_CHECK(moqr_core_tick(c, 5999) == MOQR_OK);     /* not yet */
    MOQ_TEST_CHECK(track_known(c, sb, "w"));

    /* A subscriber rejoins and leaves: the clock restarts. */
    moqr_sub_t s;
    MOQ_TEST_CHECK(subscribe(c, sb, "l", "w", 5, &s) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK(moqr_core_tick(c, 6000) == MOQR_OK);     /* not idle */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, s, 6100) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_tick(c, 7000) == MOQR_OK);     /* armed @7000 */
    MOQ_TEST_CHECK(moqr_core_tick(c, 11999) == MOQR_OK);
    MOQ_TEST_CHECK(track_known(c, sb, "w"));
    MOQ_TEST_CHECK_EQ_U64(reclaim_stats(c).tracks_reclaimable, 1);
    MOQ_TEST_CHECK(moqr_core_tick(c, 12000) == MOQR_OK);    /* freed */
    MOQ_TEST_CHECK(!track_known(c, sb, "w"));
    MOQ_TEST_CHECK(ing(c, &a, th, 2, 0) == MOQR_ERR_STALE_HANDLE);
    moqr_core_stats_t rs = reclaim_stats(c);
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_evicted_idle, 1);
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_evicted_capacity, 0);
    MOQ_TEST_CHECK_EQ_U64(rs.evicted_total, 1);   /* its cached record */
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_reclaimable, 0);
    MOQ_TEST_CHECK_EQ_U64(live_tracks(c), 0);
    moqr_core_destroy(c);

    /* Default (0) resolves to 60 s; UINT64_MAX keeps the cache forever. */
    c = mkcore(&a, 8, 0, NULL);
    MOQ_TEST_CHECK(c != NULL);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    failures += push_cycle(c, &a, pb, "l", "w", 100, NULL);
    MOQ_TEST_CHECK(moqr_core_tick(c, 1000) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_tick(c, 60000999ull) == MOQR_OK);
    MOQ_TEST_CHECK(track_known(c, sb, "w"));
    MOQ_TEST_CHECK(moqr_core_tick(c, 60001000ull) == MOQR_OK);
    MOQ_TEST_CHECK(!track_known(c, sb, "w"));
    moqr_core_destroy(c);

    c = mkcore(&a, 8, UINT64_MAX, NULL);
    MOQ_TEST_CHECK(c != NULL);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    failures += push_cycle(c, &a, pb, "l", "w", 100, NULL);
    MOQ_TEST_CHECK(moqr_core_tick(c, 1000) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_tick(c, UINT64_MAX / 2) == MOQR_OK);
    MOQ_TEST_CHECK(track_known(c, sb, "w"));
    moqr_core_destroy(c);

    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("idle_linger");
    return failures;
}

/* Namespace withdrawal (PUBLISH_NAMESPACE_DONE, or the publisher session
 * closing) does not invalidate cached objects — a SUBSCRIBE or FETCH after
 * withdrawal is legal, and objects are immutable — so the WARM track keeps
 * serving FETCH until reclaimed. It holds no pin: under pressure it is
 * evicted like any other cache entry. */
static int
test_withdrawal_reclaimable(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 2, UINT64_MAX, NULL);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t p1, p2, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &p1) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &p2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 3, &sb) == MOQR_OK);
    moq_bytes_t nsb[1];
    moqr_intent_t its[16];

    /* w1: pulled, then its namespace is withdrawn with PUBLISH_NAMESPACE_DONE. */
    MOQ_TEST_CHECK(moqr_core_announce(c, p1, NS1(nsb, "w1")) == MOQR_OK);
    failures += pull_cycle(c, &a, sb, "w1", "v", 10, NULL);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, p1, NS1(nsb, "w1")) == MOQR_OK);
    /* w2: pushed by a publisher whose session then closes, still ACTIVE. */
    MOQ_TEST_CHECK(moqr_core_announce(c, p2, NS1(nsb, "w2")) == MOQR_OK);
    moqr_track_t t2;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, p2, NS1(nsb, "w2"), B("v"), 4,
                                          &t2) == MOQR_OK);
    MOQ_TEST_CHECK(ing(c, &a, t2, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_close(c, p2, 20) == MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK_EQ_U64(reclaim_stats(c).tracks_reclaimable, 2);

    /* Cached objects are still served after withdrawal. */
    moqr_fetch_req_t fr;
    moqr_fetch_req_init(&fr);
    fr.ns = NS1(nsb, "w2");
    fr.name = B("v");
    fr.cookie = 9;
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sb, &fr, 30, &fh, &plan) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(plan.admit == MOQR_FETCH_ACCEPT);
    moqr_fetch_item_t fo;
    MOQ_TEST_CHECK(moqr_core_fetch_peek(c, fh, 31, &fo) == MOQR_OK);
    MOQ_TEST_CHECK(fo.kind == MOQR_FETCH_ITEM_OBJECT);
    MOQ_TEST_CHECK(moqr_core_fetch_commit(c, fh) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);

    /* Pressure: w1 (least recently used) goes first, then w2. */
    moqr_binding_t p3;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 4, &p3) == MOQR_OK);
    failures += push_cycle(c, &a, p3, "n", "a", 40, NULL);
    MOQ_TEST_CHECK_EQ_U64(reclaim_stats(c).tracks_evicted_capacity, 1);
    moqr_core_stats_t rs = reclaim_stats(c);
    MOQ_TEST_CHECK_EQ_U64(rs.tracks_reclaimable, 2);
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sb, &fr, 50, &fh, &plan) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(plan.admit == MOQR_FETCH_ACCEPT);   /* w2 survived */
    MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK);
    fr.ns = NS1(nsb, "w1");
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sb, &fr, 51, &fh, &plan) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(plan.admit == MOQR_FETCH_REJECT);   /* w1 reclaimed */

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("withdrawal_reclaimable");
    return failures;
}

/* A handle to an evicted track fails by generation on every entry point that
 * takes one, including the upstream resolutions; the slot's new occupant is a
 * different handle. The ROUTE_REMOVE trace names the reclaim reason. */
static int
test_stale_handles(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_trace_t *tr = NULL;
    MOQ_TEST_CHECK(moqr_trace_create(&a.vt, 256, &tr) == MOQR_OK);
    moqr_core_t *c = mkcore(&a, 1, UINT64_MAX, tr);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moq_bytes_t nsb[1];
    moqr_intent_t its[8];
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "s")) == MOQR_OK);
    moqr_track_t old;
    failures += pull_cycle(c, &a, sb, "s", "old", 10, &old);
    moqr_track_t fresh;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, NS1(nsb, "s"), B("new"), 2,
                                          &fresh) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);
    MOQ_TEST_CHECK(old._opaque != fresh._opaque);

    MOQ_TEST_CHECK(ing(c, &a, old, 1, 0) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, old, 1, 0, false, 0, 0) ==
                   MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_upstream_error(c, old, 1, 0x10, 11) ==
                   MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_upstream_lost(c, old, 1) ==
                   MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(c, old, 0, 0) ==
                   MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_note_track_evicted_below(c, old, 1) ==
                   MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(ing(c, &a, fresh, 0, 0) == MOQR_OK);

    /* Exactly one track ROUTE_REMOVE, tagged with the capacity reason. */
    moqr_trace_rec_t recs[256];
    size_t n = moqr_trace_read(tr, recs, 256);
    int removes = 0;
    for (size_t i = 0; i < n; i++) {
        if (recs[i].kind == MOQR_TRACE_ROUTE_REMOVE && recs[i].detail == 1) {
            removes++;
            MOQ_TEST_CHECK_EQ_U64(recs[i].e1, 1);
        }
    }
    MOQ_TEST_CHECK_EQ_INT(removes, 1);

    moqr_core_destroy(c);
    moqr_trace_destroy(tr);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("stale_handles");
    return failures;
}

/* Allocation failure on the reclaim path, each allocation of the admitting
 * SUBSCRIBE failed in turn. Room is made before the new track allocates
 * anything, so the relay never holds more than max_tracks logs (the memory
 * ceiling stays exact); the price is that an allocation failure after the
 * eviction has lost the victim's cache. Every outcome is consistent: the
 * victim reclaimed and counted with no half-built track left behind, and
 * every core destroys leak-free. */
static int
test_reclaim_oom_sweep(void)
{
    int failures = 0;
    for (long k = 0; k < 64; k++) {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c = mkcore(&a, 1, UINT64_MAX, NULL);
        MOQ_TEST_CHECK(c != NULL);
        moqr_binding_t pb, sb;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
        moq_bytes_t nsb[1];
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "o")) == MOQR_OK);
        failures += push_cycle(c, &a, pb, "o", "victim", 10, NULL);
        a.fail_after = k;
        moqr_sub_t s;
        moqr_result_t rc = subscribe(c, sb, "o", "new", 3, &s);
        a.fail_after = -1;
        moqr_intent_t its[8];
        (void)drain(c, its, 8);
        if (rc == MOQR_OK) {
            MOQ_TEST_CHECK_EQ_U64(reclaim_stats(c).tracks_evicted_capacity, 1);
            MOQ_TEST_CHECK(!track_known_ns(c, sb, "o", "victim"));
            moqr_core_destroy(c);
            MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
            break;   /* past the last failable allocation */
        }
        /* Any failure leaves either the victim untouched (nothing was
         * evicted yet) or the victim reclaimed and counted, and never a
         * half-built track. */
        MOQ_TEST_CHECK(rc == MOQR_ERR_NOMEM);
        moqr_core_stats_t rs = reclaim_stats(c);
        MOQ_TEST_CHECK((rs.tracks_evicted_capacity == 0 && rs.tracks_reclaimable == 1 &&
                        live_tracks(c) == 1 && track_known_ns(c, sb, "o", "victim")) ||
                       (rs.tracks_evicted_capacity == 1 && live_tracks(c) == 0));
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("reclaim_oom_sweep");
    return failures;
}

/* Freeing a track never leaves an unpolled UPSTREAM_SUBSCRIBE behind: that
 * intent borrows the track's key bytes for its namespace / name views, and
 * with the track gone the upstream request has nothing to serve. Every free
 * path drops it (and an UPSTREAM_UNSUBSCRIBE for the same, never-sent
 * attempt) from the ring before the key is released, so a consumer polling
 * afterwards never reads freed bytes. Each schedule queues the intent, frees
 * the track before any poll, then polls; under ASan a surviving intent is a
 * heap-use-after-free on its name view. */
enum {
    FREE_BY_BINDING_CLOSE,   /* PENDING track, its publisher session closes  */
    FREE_BY_AGED_TICK,       /* WARM rejoin aged empty by the log, then tick */
    FREE_BY_FORCE_WITHDRAW,  /* PENDING track, publisher still connected     */
    FREE_BY_CAPACITY,        /* WARM rejoin evicted for a new track          */
    FREE_BY_IDLE,            /* WARM rejoin past warm_idle_us                */
    FREE_BY__COUNT
};

static int
free_path_case(int path)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_tracks = path == FREE_BY_CAPACITY ? 1 : 4;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.log_budget.max_age_us = path == FREE_BY_AGED_TICK ? 50 : 0;
    cfg.linger_us = 0;
    cfg.warm_idle_us = path == FREE_BY_IDLE ? 100 : UINT64_MAX;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pb, pb2, sb;
    moq_bytes_t nsb[1];
    moqr_intent_t its[16];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 3, &pb2) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "v")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb2, NS1(nsb, "o")) == MOQR_OK);
    bool warm = path == FREE_BY_AGED_TICK || path == FREE_BY_CAPACITY ||
                path == FREE_BY_IDLE;
    if (warm) {
        failures += pull_cycle(c, &a, sb, "v", "w", 10, NULL);
    }
    (void)drain(c, its, 16);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(subscribe(c, sb, "v", "w", 9, &sub) == MOQR_OK);
    /* UPSTREAM_SUBSCRIBE(v/w) is now queued, unpolled. */
    switch (path) {
    case FREE_BY_BINDING_CLOSE:
        MOQ_TEST_CHECK(moqr_core_binding_close(c, pb, 20) == MOQR_OK);
        break;
    case FREE_BY_AGED_TICK:
        MOQ_TEST_CHECK(moqr_core_binding_close(c, pb, 20) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 21) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_tick(c, 100) == MOQR_OK);
        break;
    case FREE_BY_FORCE_WITHDRAW:
        MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "v"), 0x10, 20) ==
                       MOQR_OK);
        break;
    case FREE_BY_CAPACITY:
        MOQ_TEST_CHECK(moqr_core_binding_close(c, pb, 20) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 21) == MOQR_OK);
        {
            moqr_sub_t s2;
            MOQ_TEST_CHECK(subscribe(c, sb, "o", "x", 10, &s2) == MOQR_OK);
        }
        break;
    case FREE_BY_IDLE:
        MOQ_TEST_CHECK(moqr_core_binding_close(c, pb, 20) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 21) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_tick(c, 1000) == MOQR_OK);    /* arms */
        MOQ_TEST_CHECK(moqr_core_tick(c, 2000) == MOQR_OK);    /* frees */
        break;
    default:
        break;
    }
    size_t n = drain(c, its, 16);
    int upstream_w = 0;
    for (size_t i = 0; i < n; i++) {
        if ((its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE ||
             its[i].kind == MOQR_INTENT_UPSTREAM_UNSUBSCRIBE) &&
            its[i].binding_cookie == 1) {
            upstream_w++;
            /* reading the view is the use-after-free a survivor would be */
            if (its[i].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE) {
                MOQ_TEST_CHECK(its[i].name.len == 1 &&
                               its[i].name.data[0] == 'w');
            }
        }
    }
    MOQ_TEST_CHECK_EQ_INT(upstream_w, 0);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    return failures;
}

/* upstream_cancel: the only subscriber leaves a PENDING track and its
 * upstream attempt is cancelled before anything was polled. Neither the
 * queued subscribe nor the cancel's unsubscribe for that never-sent attempt
 * may surface, and the track is gone. */
static int
free_path_upstream_cancel(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 4, UINT64_MAX, NULL);
    moqr_binding_t pb, sb;
    moq_bytes_t nsb[1];
    moqr_intent_t its[16];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "v")) == MOQR_OK);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(subscribe(c, sb, "v", "w", 9, &sub) == MOQR_OK);
    moqr_track_t th;
    uint64_t tgen = 0;
    MOQ_TEST_CHECK(moqr_core_sub_track(c, sub, &th, &tgen) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 20) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_upstream_cancel(c, th, tgen, 21) == MOQR_OK);
    size_t n = drain(c, its, 16);
    for (size_t i = 0; i < n; i++) {
        MOQ_TEST_CHECK(its[i].kind != MOQR_INTENT_UPSTREAM_SUBSCRIBE &&
                       its[i].kind != MOQR_INTENT_UPSTREAM_UNSUBSCRIBE);
    }
    MOQ_TEST_CHECK_EQ_U64(live_tracks(c), 0);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    return failures;
}

static int
test_free_paths_drop_borrowed_intents(void)
{
    int failures = 0;
    for (int p = 0; p < FREE_BY__COUNT; p++) {
        failures += free_path_case(p);
    }
    failures += free_path_upstream_cancel();
    MOQ_TEST_PASS("free_paths_drop_borrowed_intents");
    return failures;
}

/* -- namespace trie reclaim ------------------------------------------------- */

static moqr_core_t *
mkcore_ns(ca_t *a, uint32_t max_ns_nodes)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.max_ns_nodes = max_ns_nodes;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    moqr_core_t *c = NULL;
    if (moqr_core_create(&cfg, &c) != MOQR_OK) {
        return NULL;
    }
    return c;
}

/* Two-part namespace view over caller storage. */
static moqr_ns_t
NS2(moq_bytes_t *storage, const char *a, const char *b)
{
    storage[0] = B(a);
    storage[1] = B(b);
    return (moqr_ns_t){ .parts = storage, .count = 2 };
}

/* Announce/withdraw cycles of distinct namespaces under a trie far smaller
 * than the number of namespaces ever seen: every announce is admitted, no
 * ns_nodes refusal is counted, and the gauges report what was reclaimed.
 * (PUBLISH_NAMESPACE is withdrawn by cancelling it, d18 §6.2 / d16
 * PUBLISH_NAMESPACE_DONE; nothing in either draft asks a relay to remember a
 * withdrawn namespace, so the node is pure index.) */
static int
test_ns_announce_withdraw_cycles(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_ns(&a, 5);   /* root + 4 */
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    moq_bytes_t nsb[2];
    char p0[16];
    for (int i = 0; i < 20; i++) {
        snprintf(p0, sizeof(p0), "org%d", i);
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, p0, "live")) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS2(nsb, p0, "live")) ==
                       MOQR_OK);
    }
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_NS_NODES], 0);
    MOQ_TEST_CHECK(st.ns_nodes <= 5);
    MOQ_TEST_CHECK_EQ_U64(st.ns_nodes_reclaimable, st.ns_nodes - 1u);
    MOQ_TEST_CHECK(st.ns_nodes_reclaimed >= 32);   /* 2 per cycle after fill */
    /* A fresh announce on the reclaimed trie routes a subscribe normally. */
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "fresh", "live")) ==
                   MOQR_OK);
    moqr_binding_t sb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "fresh", "live");
    rq.name = B("v");
    rq.cookie = 3;
    moqr_sub_t sub;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq, &sub) == MOQR_OK);
    moqr_intent_t its[8];
    size_t n = drain(c, its, 8);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE &&
                   its[0].binding_cookie == 1);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ns_announce_withdraw_cycles");
    return failures;
}

/* What pins a trie node: an announce (and so every ancestor on its path,
 * which has a child). What does not: a live track that was sourced by a
 * since-withdrawn announce (force-withdraw reads that identity only on a node
 * that still carries an announce; the freed node clears it), and a namespace
 * subscription (it keeps its own prefix). */
static int
test_ns_pinned_nodes(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_ns(&a, 5);   /* root + 4 */
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, wb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &wb) == MOQR_OK);
    moq_bytes_t nsb[2];
    moqr_intent_t its[16];

    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "a", "x")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "b", "y")) == MOQR_OK);
    /* Full trie of announced paths: refused, untouched. */
    moqr_core_stats_t before, st;
    moqr_core_get_stats(c, &before);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "c", "z")) ==
                   MOQR_ERR_CAPACITY);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_NS_NODES], 1);
    MOQ_TEST_CHECK_EQ_U64(st.ns_nodes, before.ns_nodes);
    MOQ_TEST_CHECK_EQ_U64(st.route_epoch, before.route_epoch);

    /* A live push track sourced by b/y; then b/y is withdrawn. */
    moqr_track_t tb;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, NS2(nsb, "b", "y"), B("v"),
                                          7, &tb) == MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS2(nsb, "b", "y")) ==
                   MOQR_OK);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.ns_nodes_reclaimable, 2);   /* b, y */
    /* A watcher on "b" pins nothing. */
    moqr_ns_t bpre = { .parts = nsb, .count = 1 };
    nsb[0] = B("b");
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, wb, bpre, 55) == MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "c", "z")) == MOQR_OK);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.ns_nodes_reclaimed, 2);
    /* The track lives on and no longer names a source announce: withdrawing
     * the new c/z (which took over its node slots) must not purge it. */
    MOQ_TEST_CHECK(ing(c, &a, tb, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS2(nsb, "c", "z"), 0x10, 30) ==
                   MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(ing(c, &a, tb, 0, 1) == MOQR_OK);
    /* The watcher still sees a new namespace under its prefix. */
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "b", "w")) == MOQR_OK);
    size_t n = drain(c, its, 16);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_NS_FOUND &&
                   its[0].binding_cookie == 2 && its[0].cookie == 55);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ns_pinned_nodes");
    return failures;
}

/* Announce under trie pressure decides before it mutates. Room that only
 * unpolled intent views hold gives WOULD_BLOCK with nothing changed (nodes,
 * reclaim count, route epoch), and the retry after a drain succeeds. When
 * even draining could not make enough room, the answer is CAPACITY at once,
 * also untouched, never a WOULD_BLOCK that cannot help. */
static int
test_ns_preflight_no_mutation(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_ns(&a, 5);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, wb;
    moq_bytes_t nsb[2];
    moqr_intent_t its[16];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &wb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "a")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS1(nsb, "a")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "c")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, wb, NS1(nsb, "b"), 9) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "b", "y")) == MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS2(nsb, "b", "y")) ==
                   MOQR_OK);   /* NS_GONE(b/y) queued: b, y view-pinned */
    /* Free now: a (1). Pinned: b, y (2). z/q needs 2. */
    moqr_core_stats_t before, after;
    moqr_core_get_stats(c, &before);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "z", "q")) ==
                   MOQR_ERR_WOULD_BLOCK);
    moqr_core_get_stats(c, &after);
    MOQ_TEST_CHECK_EQ_U64(after.ns_nodes, before.ns_nodes);
    MOQ_TEST_CHECK_EQ_U64(after.ns_nodes_reclaimed, before.ns_nodes_reclaimed);
    MOQ_TEST_CHECK_EQ_U64(after.route_epoch, before.route_epoch);
    MOQ_TEST_CHECK_EQ_U64(after.refusals[MOQR_REFUSE_NS_NODES], 0);
    size_t n = drain(c, its, 16);
    MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_NS_GONE &&
                   its[0].ns_count == 2 && its[0].ns_parts[1].len == 1 &&
                   memcmp(its[0].ns_parts[1].data, "y", 1) == 0);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "z", "q")) == MOQR_OK);
    moqr_core_destroy(c);

    /* Root, two live leaves and one view-pinned leaf in a 4-node trie: a
     * 2-node announce cannot fit even after a drain. */
    c = mkcore_ns(&a, 4);
    MOQ_TEST_CHECK(c != NULL);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &wb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "l1")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "l2")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, wb, NS1(nsb, "p"), 9) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "p")) == MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS1(nsb, "p")) == MOQR_OK);
    moqr_core_get_stats(c, &before);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "m", "n")) ==
                   MOQR_ERR_CAPACITY);
    moqr_core_get_stats(c, &after);
    MOQ_TEST_CHECK_EQ_U64(after.ns_nodes, before.ns_nodes);
    MOQ_TEST_CHECK_EQ_U64(after.route_epoch, before.route_epoch);
    MOQ_TEST_CHECK_EQ_U64(after.refusals[MOQR_REFUSE_NS_NODES], 1);
    (void)drain(c, its, 16);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ns_preflight_no_mutation");
    return failures;
}

/* Intent refusal must preserve the cache even when the new prefix needs
 * name bytes that can only be supplied by evicting a WARM track. */
static int
test_ns_subscribe_reserves_before_reclaim(void)
{
    int failures = 0;
    for (size_t prefix_count = 0; prefix_count <= 1; prefix_count++) {
        ca_t a;
        ca_init(&a);
        moqr_core_relay_cfg_t cfg;
        moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
        cfg.name_intern_bytes = 16;   /* namespace (1) + track key (14) */
        cfg.max_tracks = cfg.max_subs = cfg.max_ns_subs = 2;
        cfg.max_intents = 4;
        cfg.warm_idle_us = UINT64_MAX;
        cfg.log_budget.max_age_us = 0;
        moqr_core_t *c = NULL;
        MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
        if (c == NULL) {
            return failures + 1;
        }
        moqr_binding_t pb, wb;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &wb) == MOQR_OK);
        moq_bytes_t parts[1];
        moqr_ns_t ns = NS1(parts, "a");
        moqr_ns_t prefix = { parts, prefix_count };
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, ns) == MOQR_OK);
        failures += push_cycle(c, &a, pb, "a", "v", 1, NULL);
        moqr_core_limits_t lim;
        moqr_core_get_limits(c, &lim);
        for (uint32_t i = 0; i < lim.max_intents; i++) {
            MOQ_TEST_CHECK(moqr_core_track_status(c, wb, ns, B("v"),
                                                  100u + i) == MOQR_OK);
        }
        moqr_core_stats_t before = reclaim_stats(c);
        uint64_t room_before = moqr_core_room_epoch(c);
        long live_before = a.live;
        MOQ_TEST_CHECK_EQ_U64(before.tracks, 1);
        MOQ_TEST_CHECK(before.retained_bytes != 0);
        for (int i = 0; i < 2; i++) {
            MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, wb, prefix, 4) ==
                           MOQR_ERR_WOULD_BLOCK);
            moqr_core_stats_t after = reclaim_stats(c);
            MOQ_TEST_CHECK_EQ_U64(after.tracks, before.tracks);
            MOQ_TEST_CHECK_EQ_U64(after.retained_bytes, before.retained_bytes);
            MOQ_TEST_CHECK_EQ_U64(after.tracks_evicted_capacity, 0);
            MOQ_TEST_CHECK_EQ_U64(after.route_epoch, before.route_epoch);
            MOQ_TEST_CHECK_EQ_U64(after.ns_subs, 0);
            MOQ_TEST_CHECK_EQ_U64(moqr_core_room_epoch(c), room_before);
            MOQ_TEST_CHECK(a.live == live_before);
        }
        moqr_intent_t it;
        for (uint32_t i = 0; i < lim.max_intents; i++) {
            MOQ_TEST_CHECK(drain(c, &it, 1) == 1);
            MOQ_TEST_CHECK(it.kind == MOQR_INTENT_TRACK_STATUS_OK);
            MOQ_TEST_CHECK_EQ_U64(it.cookie, 100u + i);
        }
        MOQ_TEST_CHECK(drain(c, &it, 1) == 0);
        MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, wb, prefix, 4) == MOQR_OK);
        moqr_core_stats_t after = reclaim_stats(c);
        MOQ_TEST_CHECK_EQ_U64(after.tracks, 0);
        MOQ_TEST_CHECK_EQ_U64(after.tracks_evicted_capacity, 1);
        MOQ_TEST_CHECK_EQ_U64(after.ns_subs, 1);
        MOQ_TEST_CHECK(drain(c, &it, 1) == 1);
        MOQ_TEST_CHECK(it.kind == MOQR_INTENT_NS_FOUND && it.cookie == 4);
        MOQ_TEST_CHECK(it.ns_count == 1 && it.ns_parts[0].len == 1 &&
                       memcmp(it.ns_parts[0].data, "a", 1) == 0);
        MOQ_TEST_CHECK(drain(c, &it, 1) == 0);
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("ns_subscribe_reserves_before_reclaim");
    return failures;
}

/* Unique namespaces through a small trie while each one's finished
 * publication stays WARM: reclaimable tracks pin no trie node, so every
 * announce is admitted. */
static int
test_ns_reclaimable_tracks_do_not_pin(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_ns(&a, 5);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, sb;
    moq_bytes_t nsb[2];
    moqr_intent_t its[16];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    char ns[16];
    for (int i = 0; i < 12; i++) {
        snprintf(ns, sizeof(ns), "s%d", i);
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, ns)) == MOQR_OK);
        (void)drain(c, its, 16);
        failures += pull_cycle(c, &a, sb, ns, "t", 10 + (uint64_t)i, NULL);
        MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS1(nsb, ns)) == MOQR_OK);
        (void)drain(c, its, 16);
    }
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_NS_NODES], 0);
    MOQ_TEST_CHECK_EQ_U64(st.tracks, 12);   /* all cache, all kept */
    MOQ_TEST_CHECK_EQ_U64(st.tracks_reclaimable, 12);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ns_reclaimable_tracks_do_not_pin");
    return failures;
}

/* Name-byte pressure reclaims too. Unique track names through a byte budget
 * far smaller than the slot table: every publication is admitted by evicting
 * the least recently used cache, and nothing is refused while anything is
 * reclaimable. A SUBSCRIBE that hits existing cache under the same pressure
 * is served from it and evicts nothing. Byte pressure from an announce frees
 * withdrawn trie nodes' bytes. */
static int
test_name_bytes_pressure(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_tracks = 64;
    cfg.name_intern_bytes = 256;
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 0;
    cfg.warm_idle_us = UINT64_MAX;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pb, sb;
    moq_bytes_t nsb[2];
    moqr_intent_t its[16];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "ns")) == MOQR_OK);
    (void)drain(c, its, 16);
    char name[32];
    for (int i = 0; i < 40; i++) {
        snprintf(name, sizeof(name), "track-name-%04d", i);
        failures += pull_cycle(c, &a, sb, "ns", name, 10 + (uint64_t)i, NULL);
    }
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_NAME_BYTES], 0);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_TRACKS], 0);
    MOQ_TEST_CHECK(st.tracks_evicted_capacity > 0);
    MOQ_TEST_CHECK(st.tracks < 40);
    /* The newest track is still cached: subscribing serves it and evicts
     * nothing, even though the byte budget is full. */
    uint64_t evicted = st.tracks_evicted_capacity;
    moqr_sub_t s;
    MOQ_TEST_CHECK(subscribe(c, sb, "ns", "track-name-0039", 5, &s) ==
                   MOQR_OK);
    size_t n = drain(c, its, 16);
    MOQ_TEST_CHECK(n >= 1 && its[0].kind == MOQR_INTENT_ACCEPT_SUB);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.tracks_evicted_capacity, evicted);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, s, 100) == MOQR_OK);
    moqr_core_destroy(c);

    /* Byte-only trie pressure: plenty of nodes, few bytes. Withdrawn
     * namespaces with long parts are reclaimed for a new long one. */
    cfg.max_tracks = 4;
    cfg.max_ns_nodes = 64;
    cfg.name_intern_bytes = 120;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    char p0[48];
    for (int i = 0; i < 6; i++) {
        snprintf(p0, sizeof(p0), "a-long-namespace-part-%02d-xxxxxxxx", i);
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, p0)) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS1(nsb, p0)) == MOQR_OK);
    }
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.refusals[MOQR_REFUSE_NAME_BYTES], 0);
    MOQ_TEST_CHECK(st.ns_nodes_reclaimed >= 3);
    MOQ_TEST_CHECK(st.ns_nodes < 6);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("name_bytes_pressure");
    return failures;
}

/* The trie reclaim's ROUTE_REMOVE records carry the route epoch of the state
 * they produced (the epoch moves before the first removal). */
static int
test_ns_reclaim_trace_epoch(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_trace_t *trace = NULL;
    MOQ_TEST_CHECK(moqr_trace_create(&a.vt, 64, &trace) == MOQR_OK);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_ns_nodes = 3;
    cfg.trace = trace;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pb;
    moq_bytes_t nsb[1];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "a")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS1(nsb, "a")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "b")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "c")) == MOQR_OK);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    moqr_trace_rec_t recs[64];
    size_t n = moqr_trace_read(trace, recs, 64);
    int removes = 0;
    uint64_t reclaim_epoch = 0;
    for (size_t i = 0; i < n; i++) {
        if (recs[i].kind == MOQR_TRACE_ROUTE_REMOVE && recs[i].detail == 3) {
            removes++;
            reclaim_epoch = recs[i].epochs.route_epoch;
        }
    }
    MOQ_TEST_CHECK_EQ_INT(removes, 1);
    /* The reclaim bumped the epoch once, then the announce of c bumped it
     * again: the record names the state between the two. */
    MOQ_TEST_CHECK_EQ_U64(reclaim_epoch + 1u, st.route_epoch);
    moqr_core_destroy(c);
    moqr_trace_destroy(trace);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ns_reclaim_trace_epoch");
    return failures;
}

/* Trie memory stays inside the advertised ceiling: wide fan-out churn
 * (many parents, many children each, announced and withdrawn) allocates
 * nothing per child beyond its node and part bytes. */
static int
test_ns_memory_ceiling(void)
{
    int failures = 0;
    const unsigned parents = 255, children = 256;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.max_bindings = cfg.max_tracks = cfg.max_subs = 1;
    cfg.max_ns_subs = cfg.max_intents = cfg.max_parked = 1;
    cfg.max_grants = cfg.max_fetches = cfg.max_cancels = 1;
    cfg.parked_bytes = cfg.grant_bytes = cfg.fetch_pin_bytes = 1;
    cfg.name_intern_bytes = 4096;
    cfg.max_ns_nodes = 1 + parents + children;
    cfg.log_budget.max_groups = 1;
    cfg.log_budget.max_bytes = 1;
    cfg.log_max_subgroups = cfg.log_max_objects_per_group = 1;
    cfg.log_max_cursors = cfg.log_max_chunk_nodes = 1;
    moqr_core_capacity_t cap;
    MOQ_TEST_CHECK(moqr_core_capacity_describe(&cfg, &cap) == MOQR_OK);
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_binding_t pb;
    moq_bytes_t nsb[2];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    char parent[16], child[16];
    for (unsigned p = 0; p < parents; p++) {
        snprintf(parent, sizeof(parent), "p%u", p);
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, parent)) == MOQR_OK);
    }
    long peak = a.live;
    for (unsigned p = 0; p < parents; p++) {
        snprintf(parent, sizeof(parent), "p%u", p);
        for (unsigned k = 0; k < children; k++) {
            snprintf(child, sizeof(child), "c%u", k);
            MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, parent, child)) ==
                           MOQR_OK);
            if (a.live > peak) {
                peak = a.live;
            }
        }
        for (unsigned k = 0; k < children; k++) {
            snprintf(child, sizeof(child), "c%u", k);
            MOQ_TEST_CHECK(moqr_core_unannounce(c, pb,
                                                NS2(nsb, parent, child)) ==
                           MOQR_OK);
        }
    }
    MOQ_TEST_CHECK((uint64_t)peak <= cap.total_bytes);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ns_memory_ceiling");
    return failures;
}

/* Allocation failure while an announce makes room: each allocation of the
 * announce fails in turn after reclaim compacted the trie. The trie stays
 * consistent (a later announce routes), and every core is leak-free. */
static int
test_ns_announce_oom_sweep(void)
{
    int failures = 0;
    for (long k = 0; k < 16; k++) {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c = mkcore_ns(&a, 5)   /* root, k, 1, w + one free */;
        MOQ_TEST_CHECK(c != NULL);
        moqr_binding_t pb;
        moq_bytes_t nsb[2];
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "k", "1")) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "w")) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS1(nsb, "w")) == MOQR_OK);
        a.fail_after = k;
        moqr_result_t rc = moqr_core_announce(c, pb, NS2(nsb, "n", "2"));
        a.fail_after = -1;
        MOQ_TEST_CHECK(rc == MOQR_OK || rc == MOQR_ERR_NOMEM);
        moqr_core_stats_t st;
        moqr_core_get_stats(c, &st);
        MOQ_TEST_CHECK(st.ns_nodes <= 5);
        if (rc != MOQR_OK) {
            MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "n", "2")) ==
                           MOQR_OK);
        }
        moqr_binding_t sb;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = NS2(nsb, "n", "2");
        rq.name = B("v");
        moqr_sub_t sub;
        MOQ_TEST_CHECK(moqr_core_subscribe(c, sb, &rq, &sub) == MOQR_OK);
        moqr_intent_t its[8];
        size_t n = drain(c, its, 8);
        MOQ_TEST_CHECK(n == 1 && its[0].kind == MOQR_INTENT_UPSTREAM_SUBSCRIBE);
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
        if (rc == MOQR_OK) {
            break;
        }
    }
    MOQ_TEST_PASS("ns_announce_oom_sweep");
    return failures;
}

/* Compaction keeps every stored node index right: a live track sourced by an
 * announce whose node is moved by a reclaim is still purged by force-withdraw
 * of that announce, and a track sourced by another announce is not. */
static int
test_ns_compaction_keeps_sources(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_ns(&a, 5);
    MOQ_TEST_CHECK(c != NULL);
    moqr_binding_t pb, qb;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &qb) == MOQR_OK);
    moq_bytes_t nsb[2];
    moqr_intent_t its[16];
    /* Low indices: d/1 withdrawn (reclaimable). High indices: e/2, sourcing a
     * live push track, so the reclaim of d/1 moves e/2's nodes. */
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "d", "1")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, qb, NS2(nsb, "e", "2")) == MOQR_OK);
    moqr_track_t te;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, qb, NS2(nsb, "e", "2"), B("v"),
                                          8, &te) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS2(nsb, "d", "1")) ==
                   MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS2(nsb, "f", "3")) == MOQR_OK);
    moqr_core_stats_t st;
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.ns_nodes_reclaimed, 2);
    /* A second live track, sourced by the new f/3 announce. */
    moqr_track_t tf;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, NS2(nsb, "f", "3"), B("v"),
                                          9, &tf) == MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(ing(c, &a, te, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS2(nsb, "e", "2"), 0x10, 20) ==
                   MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(ing(c, &a, te, 0, 1) == MOQR_ERR_STALE_HANDLE);   /* purged */
    MOQ_TEST_CHECK(ing(c, &a, tf, 0, 0) == MOQR_OK);   /* and only it */
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK_EQ_U64(st.tracks, 1);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("ns_compaction_keeps_sources");
    return failures;
}

/* The core reports when its tick next has time-driven work, so a runtime can
 * sleep until then: nothing to do is UINT64_MAX; an unarmed reclaimable track
 * is due now (0, the tick that arms it); an armed one is due at its idle
 * deadline; a lingering upstream at its linger deadline; interest clears it. */
static int
test_next_deadline(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 4;
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 300;
    cfg.warm_idle_us = 5000;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_next_deadline_us(NULL), UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_next_deadline_us(c), UINT64_MAX);
    moqr_binding_t pb, sb;
    moq_bytes_t nsb[1];
    moqr_intent_t its[8];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "d")) == MOQR_OK);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(subscribe(c, sb, "d", "t", 1, &sub) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(drain(c, its, 8), (size_t)1);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(c, its[0].track, its[0].track_gen, 5,
                                         false, 0, 0) == MOQR_OK);
    moqr_track_t th = its[0].track;
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK(ing(c, &a, th, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_next_deadline_us(c), UINT64_MAX);
    /* Last subscriber leaves at 1000: the upstream lingers until 1300. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 1000) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_next_deadline_us(c), 1300);
    /* At 1300 the upstream is released and the same tick arms the idle
     * clock of the now-reclaimable track: due at 1300 + 5000. */
    MOQ_TEST_CHECK(moqr_core_tick(c, 1300) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_next_deadline_us(c), 6300);
    /* Interest clears it. */
    MOQ_TEST_CHECK(subscribe(c, sb, "d", "t", 2, &sub) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_next_deadline_us(c), UINT64_MAX);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 1500) == MOQR_OK);
    /* A track that turns reclaimable outside a tick (a push source that
     * finishes) is due at once: the next tick must arm it. */
    moqr_track_t tp;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, NS1(nsb, "d"), B("p"), 3,
                                          &tp) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK(ing(c, &a, tp, 0, 0) == MOQR_OK);
    moqr_pd_desc_t done;
    MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_TRACK_ENDED, &done) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_source_done(c, tp, 1, done, 1600) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_next_deadline_us(c), 0);
    MOQ_TEST_CHECK(moqr_core_tick(c, 2000) == MOQR_OK);       /* arms both */
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_next_deadline_us(c), 7000);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("next_deadline");
    return failures;
}

/* Announce a namespace given as "x" or "p/x". */
static moqr_result_t
ann_path(moqr_core_t *c, moqr_binding_t b, const char *p, bool withdraw)
{
    moq_bytes_t parts[2];
    moqr_ns_t ns;
    const char *slash = strchr(p, '/');
    if (slash != NULL) {
        parts[0] = (moq_bytes_t){ (const uint8_t *)p, (size_t)(slash - p) };
        parts[1] = B(slash + 1);
        ns = (moqr_ns_t){ parts, 2 };
    } else {
        parts[0] = B(p);
        ns = (moqr_ns_t){ parts, 1 };
    }
    return withdraw ? moqr_core_unannounce(c, b, ns)
                    : moqr_core_announce(c, b, ns);
}

/* What a fresh root watcher is told: the announced namespaces it learns, as
 * "a" or "p/x" strings in fan-out order, joined by ','. */
static void
root_watch(moqr_core_t *c, moqr_binding_t wb, uint64_t cookie, char *out,
           size_t cap)
{
    moqr_intent_t its[32];
    (void)drain(c, its, 32);
    out[0] = '\0';
    moq_bytes_t none[1];
    if (moqr_core_ns_subscribe(c, wb, (moqr_ns_t){ none, 0 }, cookie) !=
        MOQR_OK) {
        return;
    }
    size_t n = drain(c, its, 32);
    size_t o = 0;
    for (size_t i = 0; i < n; i++) {
        if (its[i].kind != MOQR_INTENT_NS_FOUND) {
            continue;
        }
        for (uint32_t k = 0; k < its[i].ns_count && k < 2; k++) {
            int w = snprintf(out + o, cap - o, "%s%.*s",
                             k == 0 ? (o == 0 ? "" : ",") : "/",
                             (int)its[i].ns_parts[k].len,
                             (const char *)its[i].ns_parts[k].data);
            o += (size_t)w;
        }
    }
    (void)moqr_core_ns_unsubscribe(c, wb, cookie);
}

/* Trie compaction moves the newest node into each freed slot and rewrites
 * every link that named it. Three placements of the moved node among its
 * siblings, each followed by an append under the same parent, and each
 * checked by what a root watcher learns (every live namespace, once, in
 * creation order):
 *   last child   - the parent's last_child must follow the move, or the next
 *                  append links after a vacated slot and loses siblings;
 *   first child  - the parent's first_child and the next sibling's prev;
 *   middle child - the previous sibling's next and the next sibling's prev.
 * A stale prev link only shows when that sibling is later unlinked, so the
 * first- and middle-child variants then withdraw it and force its reclaim. */
static int
test_ns_compaction_sibling_links(void)
{
    int failures = 0;
    char seen[256];
    for (int variant = 0; variant < 3; variant++) {
        ca_t a;
        ca_init(&a);
        moqr_core_t *c = NULL;
        moqr_binding_t pb, wb;
        const char *want = NULL;
        if (variant == 0) {
            /* root: a(1) b(2). Freeing a moves b (root's last child). */
            c = mkcore_ns(&a, 3);
            MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
            MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &wb) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "a", false) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "b", false) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "a", true) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "c", false) == MOQR_OK);
            want = "b,c";
        } else {
            /* h1(1) h2(2) p(3), then p's children. Freeing h2 moves the
             * newest node; freeing h1 then moves the next newest, which is
             * p's first child (variant 1: p/x, p/y) or its middle child
             * (variant 2: p/w, p/x, p/y). */
            c = mkcore_ns(&a, variant == 1 ? 6 : 7);
            MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
            MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &wb) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "h1", false) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "h2", false) == MOQR_OK);
            if (variant == 2) {
                MOQ_TEST_CHECK(ann_path(c, pb, "p/w", false) == MOQR_OK);
            }
            MOQ_TEST_CHECK(ann_path(c, pb, "p/x", false) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "p/y", false) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "h1", true) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "h2", true) == MOQR_OK);
            /* Needs two nodes: frees h2 then h1, moving y then x. */
            MOQ_TEST_CHECK(ann_path(c, pb, "p/z", false) == MOQR_OK);
            want = variant == 1 ? "p/x,p/y,p/z" : "p/w,p/x,p/y,p/z";
        }
        moqr_core_stats_t st;
        moqr_core_get_stats(c, &st);
        MOQ_TEST_CHECK(st.ns_nodes_reclaimed >= 1);
        root_watch(c, wb, 7, seen, sizeof(seen));
        if (strcmp(seen, want) != 0) {
            printf("  variant %d: watcher learned [%s], want [%s]\n", variant,
                   seen, want);
        }
        MOQ_TEST_CHECK(strcmp(seen, want) == 0);
        if (variant != 0) {
            /* Unlinking a sibling of the moved node reads that sibling's
             * prev link, which the move had to rewrite: fill the last free
             * node, withdraw p/y, and make room for p/q, which frees y. */
            MOQ_TEST_CHECK(ann_path(c, pb, "k", false) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "p/y", true) == MOQR_OK);
            MOQ_TEST_CHECK(ann_path(c, pb, "p/q", false) == MOQR_OK);
            const char *want2 = variant == 1 ? "p/x,p/z,p/q,k"
                                             : "p/w,p/x,p/z,p/q,k";
            root_watch(c, wb, 8, seen, sizeof(seen));
            if (strcmp(seen, want2) != 0) {
                printf("  variant %d after unlink: watcher learned [%s], "
                       "want [%s]\n", variant, seen, want2);
            }
            MOQ_TEST_CHECK(strcmp(seen, want2) == 0);
        }
        moqr_core_destroy(c);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("ns_compaction_sibling_links");
    return failures;
}

/* moqr_core_announce_room answers whether an announce would fit, from the
 * same preflight, without announcing, reclaiming or counting: free room or
 * reclaimable room is OK, room held only by queued intent views is
 * WOULD_BLOCK, no room is CAPACITY, and the refusal metric does not move. */
static int
test_announce_room_probe(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore_ns(&a, 3);   /* root + 2 */
    moqr_binding_t pb, wb;
    moq_bytes_t nsb[1];
    moqr_intent_t its[16];
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &wb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce_room(c, NS1(nsb, "a")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "a")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "b")) == MOQR_OK);
    moqr_core_stats_t before, after;
    moqr_core_get_stats(c, &before);
    uint64_t room0 = moqr_core_room_epoch(c);
    MOQ_TEST_CHECK(moqr_core_announce_room(c, NS1(nsb, "c")) ==
                   MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK_EQ_U64(moqr_core_room_epoch(c), room0);   /* pure */
    MOQ_TEST_CHECK(moqr_core_announce_room(c, NS1(nsb, "a")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, wb, NS1(nsb, "b"), 4) == MOQR_OK);
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS1(nsb, "b")) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce_room(c, NS1(nsb, "c")) ==
                   MOQR_ERR_WOULD_BLOCK);   /* NS_GONE(b) unpolled */
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(moqr_core_announce_room(c, NS1(nsb, "c")) == MOQR_OK);
    moqr_core_get_stats(c, &after);
    MOQ_TEST_CHECK_EQ_U64(after.refusals[MOQR_REFUSE_NS_NODES],
                          before.refusals[MOQR_REFUSE_NS_NODES]);
    MOQ_TEST_CHECK_EQ_U64(after.ns_nodes_reclaimed, before.ns_nodes_reclaimed);
    MOQ_TEST_CHECK(moqr_core_announce_room(NULL, NS1(nsb, "c")) ==
                   MOQR_ERR_INVAL);
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("announce_room_probe");
    return failures;
}

/* The room epoch moves exactly when admission room may have appeared, so a
 * caller parked on a capacity refusal retries only when it could succeed.
 * Changes that only consume room do not move it: an announce, a new track,
 * a subscription, a namespace subscription (each also a route change, which
 * moves route_epoch). Every way room appears does: a withdrawal (its trie path
 * becomes reclaimable), a subscriber leaving (its track may become
 * reclaimable), a source finishing, a track freed, a namespace subscription
 * released, a fetch ending, trie nodes reclaimed. */
static int
test_room_epoch(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a, 4, UINT64_MAX, NULL);
    moqr_binding_t pb, sb;
    moq_bytes_t nsb[1];
    moqr_intent_t its[16];
    MOQ_TEST_CHECK_EQ_U64(moqr_core_room_epoch(NULL), 0);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sb) == MOQR_OK);
    moqr_core_stats_t st;
#define ROOM_SAME(op)                                                      \
    do {                                                                   \
        uint64_t r0 = moqr_core_room_epoch(c);                             \
        op;                                                                \
        MOQ_TEST_CHECK_EQ_U64(moqr_core_room_epoch(c), r0);                \
    } while (0)
#define ROOM_MOVES(op)                                                     \
    do {                                                                   \
        uint64_t r0 = moqr_core_room_epoch(c);                             \
        op;                                                                \
        MOQ_TEST_CHECK(moqr_core_room_epoch(c) > r0);                      \
    } while (0)
    /* Consuming room: route changes, no room change. */
    moqr_core_get_stats(c, &st);
    uint64_t route0 = st.route_epoch;
    ROOM_SAME(MOQ_TEST_CHECK(moqr_core_announce(c, pb, NS1(nsb, "r")) ==
                             MOQR_OK));
    moqr_track_t th;
    ROOM_SAME(MOQ_TEST_CHECK(moqr_core_publish_open(c, pb, NS1(nsb, "r"),
                                                    B("t"), 3, &th) ==
                             MOQR_OK));
    (void)drain(c, its, 16);
    MOQ_TEST_CHECK(ing(c, &a, th, 0, 0) == MOQR_OK);
    moqr_sub_t sub;
    ROOM_SAME(MOQ_TEST_CHECK(subscribe(c, sb, "r", "t", 4, &sub) == MOQR_OK));
    (void)drain(c, its, 16);
    ROOM_SAME(MOQ_TEST_CHECK(moqr_core_ns_subscribe(c, sb, NS1(nsb, "q"), 5) ==
                             MOQR_OK));
    (void)drain(c, its, 16);
    moqr_core_get_stats(c, &st);
    MOQ_TEST_CHECK(st.route_epoch > route0);   /* those were route changes */
    /* Releasing room. */
    ROOM_MOVES(MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sub, 10) == MOQR_OK));
    moqr_pd_desc_t done;
    MOQ_TEST_CHECK(moqr_pd_desc_local(MOQR_PD_TRACK_ENDED, &done) == MOQR_OK);
    ROOM_MOVES(MOQ_TEST_CHECK(moqr_core_source_done(c, th, 1, done, 11) ==
                              MOQR_OK));
    (void)drain(c, its, 16);
    ROOM_MOVES(MOQ_TEST_CHECK(moqr_core_unannounce(c, pb, NS1(nsb, "r")) ==
                              MOQR_OK));
    (void)drain(c, its, 16);
    ROOM_MOVES(MOQ_TEST_CHECK(moqr_core_ns_unsubscribe(c, sb, 5) == MOQR_OK));
    /* A fetch ending, and a track freed by the idle bound. */
    moqr_fetch_req_t fr;
    moqr_fetch_req_init(&fr);
    fr.ns = NS1(nsb, "r");
    fr.name = B("t");
    fr.cookie = 6;
    moqr_fetch_t fh;
    moqr_fetch_plan_t plan;
    MOQ_TEST_CHECK(moqr_core_fetch_open(c, sb, &fr, 12, &fh, &plan) ==
                   MOQR_OK);
    ROOM_MOVES(MOQ_TEST_CHECK(moqr_core_fetch_close(c, fh) == MOQR_OK));
    MOQ_TEST_CHECK(moqr_core_force_withdraw(c, NS1(nsb, "r"), 0x10, 13) ==
                   MOQR_OK);   /* no announce left: a no-op */
    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
#undef ROOM_SAME
#undef ROOM_MOVES
    MOQ_TEST_PASS("room_epoch");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_publish_then_leave_cycles();
    failures += test_live_tracks_refuse();
    failures += test_pinned_tracks_safe();
    failures += test_lru_order();
    failures += test_idle_linger();
    failures += test_withdrawal_reclaimable();
    failures += test_stale_handles();
    failures += test_reclaim_oom_sweep();
    failures += test_free_paths_drop_borrowed_intents();
    failures += test_ns_announce_withdraw_cycles();
    failures += test_ns_pinned_nodes();
    failures += test_ns_preflight_no_mutation();
    failures += test_ns_subscribe_reserves_before_reclaim();
    failures += test_ns_reclaimable_tracks_do_not_pin();
    failures += test_name_bytes_pressure();
    failures += test_ns_reclaim_trace_epoch();
    failures += test_ns_memory_ceiling();
    failures += test_ns_announce_oom_sweep();
    failures += test_next_deadline();
    failures += test_ns_compaction_sibling_links();
    failures += test_announce_room_probe();
    failures += test_room_epoch();
    failures += test_ns_compaction_keeps_sources();
    if (failures != 0) {
        printf("%d failure(s)\n", failures);
        return 1;
    }
    return 0;
}
