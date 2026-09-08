/* Route dump: deterministic entity-detailed JSON/text snapshots (read-only). */

#include <moq/relay/relay.h>

#include <moq/rcbuf.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

#define HAS(hay, needle) (strstr((hay), (needle)) != NULL)

/* -- counting allocator + tiny harness (mirrors the control test) -------- */
typedef struct ca {
    long        live;
    moq_alloc_t vt;
} ca_t;

static void *
ca_a(size_t n, void *c)
{
    ca_t *a = c;
    a->live++;
    return malloc(n);
}
static void *
ca_r(void *p, size_t o, size_t n, void *c)
{
    (void)o;
    (void)c;
    return realloc(p, n);
}
static void
ca_f(void *p, size_t n, void *c)
{
    (void)n;
    ca_t *a = c;
    if (p != NULL) {
        a->live--;
    }
    free(p);
}
static void
ca_init(ca_t *a)
{
    a->live = 0;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
    a->vt.ctx = a;
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
mkcore(ca_t *a)
{
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.log_budget.max_groups = 8;
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
    d.publisher_priority = 1;
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

/* -- tests --------------------------------------------------------------- */

static int
test_route_dump_empty(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a);
    MOQ_TEST_CHECK(c != NULL);

    char buf[1024];
    size_t w = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(strlen(buf), w);
    MOQ_TEST_CHECK(HAS(buf, "\"epochs\":{\"node\":0,\"shard\":0,\"route\":0}"));
    MOQ_TEST_CHECK(HAS(buf, "\"announces\":[]"));
    MOQ_TEST_CHECK(HAS(buf, "\"namespace_subscriptions\":[]"));
    MOQ_TEST_CHECK(HAS(buf, "\"tracks\":[]"));

    MOQ_TEST_CHECK(moqr_core_route_dump_text(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf, "announces:\n  (none)"));
    MOQ_TEST_CHECK(HAS(buf, "tracks:\n  (none)"));

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("route_dump_empty");
    return failures;
}

/* Build: pub announces two namespaces and publishes one track (ACTIVE with
 * 3 retained groups); a watcher subscribes the "v" prefix; one subscriber is
 * ACTIVE on the active track but has delivered nothing (lag=3); another is
 * PARKED on an announced-but-unpublished track (PENDING). */
static moqr_core_t *
build_populated(ca_t *a, moqr_track_t *track_out)
{
    moqr_core_t *c = mkcore(a);
    if (c == NULL) {
        return NULL;
    }
    moqr_intent_t its[16];
    moq_bytes_t nsb[2];

    moqr_binding_t pub, watch, subA, subB;
    (void)(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    (void)(moqr_core_binding_open(c, 2, &watch) == MOQR_OK);
    (void)(moqr_core_binding_open(c, 3, &subA) == MOQR_OK);
    (void)(moqr_core_binding_open(c, 4, &subB) == MOQR_OK);

    moqr_core_announce(c, pub, NS2(nsb, "v", "cam"));
    moqr_core_announce(c, pub, NS2(nsb, "v", "cam2"));

    moqr_track_t track;
    moqr_core_publish_open(c, pub, NS2(nsb, "v", "cam"), B("hd"), 5, &track);
    (void)drain(c, its, 16);
    for (uint64_t g = 1; g <= 3; g++) {
        (void)ing(c, a, track, g, 0);
    }

    moq_bytes_t pfx[1];
    pfx[0] = B("v");
    moqr_core_ns_subscribe(c, watch, (moqr_ns_t){ pfx, 1 }, 20);
    (void)drain(c, its, 16);

    /* Active sub on the live track, delivering nothing => full backlog. */
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "v", "cam");
    rq.name = B("hd");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.filter.start_group = 1;
    rq.filter.start_object = 0;
    rq.cookie = 100;
    moqr_sub_t sa;
    moqr_core_subscribe(c, subA, &rq, &sa);
    (void)drain(c, its, 16);

    /* Parked sub on an announced-but-unpublished track (stays PENDING). */
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "v", "cam2");
    rq.name = B("x");
    rq.filter.type = MOQR_FILTER_LARGEST_OBJECT;
    rq.cookie = 200;
    moqr_sub_t sb;
    moqr_core_subscribe(c, subB, &rq, &sb);
    (void)drain(c, its, 16);

    if (track_out != NULL) {
        *track_out = track;
    }
    return c;
}

static int
test_route_dump_json(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = build_populated(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    char buf[4096];
    size_t w = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(strlen(buf), w);

    /* Announces (both namespaces, name excluded) tagged with the publisher's
     * connection cookie (1), not an internal pool slot. */
    MOQ_TEST_CHECK(HAS(buf, "\"namespace\":[\"v\",\"cam\"],\"binding\":1"));
    MOQ_TEST_CHECK(HAS(buf, "\"namespace\":[\"v\",\"cam2\"],\"binding\":1"));
    /* Namespace watcher on the "v" prefix (watcher cookie 2). */
    MOQ_TEST_CHECK(HAS(buf,
        "\"namespace_subscriptions\":[{\"prefix\":[\"v\"],\"binding\":2}"));
    /* Active track: name, state, and upstream tagged with the publisher's
     * cookie (1), not an internal slot. */
    MOQ_TEST_CHECK(HAS(buf,
        "\"name\":\"hd\",\"state\":\"active\",\"upstream_binding\":1"));
    MOQ_TEST_CHECK(HAS(buf,
        "\"oldest_group\":1,\"newest_group\":3,\"groups\":3"));
    /* Active subscriber (cookie 3) with a full 3-group backlog from group 1;
     * start not below retention (oldest is still 1), so no loss flag. */
    MOQ_TEST_CHECK(HAS(buf,
        "\"binding\":3,\"state\":\"active\",\"start_group\":1,"
        "\"start_object\":0,\"end_group\":null,\"lag_groups\":3,"
        "\"frontier_group\":1,\"skip_pending\":false,"
        "\"start_before_retention\":false"));
    /* The pending track: upstream is the same publisher (cookie 1), and it
     * carries the parked subscriber (cookie 4). */
    MOQ_TEST_CHECK(HAS(buf,
        "\"name\":\"x\",\"state\":\"pending\",\"upstream_binding\":1"));
    MOQ_TEST_CHECK(HAS(buf, "\"binding\":4,\"state\":\"parked\""));

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("route_dump_json");
    return failures;
}

static int
test_route_dump_text(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = build_populated(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    char buf[4096];
    size_t w = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_text(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf, "epochs: node=0 shard=0 route="));
    MOQ_TEST_CHECK(HAS(buf, "v/cam -> binding 1"));      /* publisher cookie */
    MOQ_TEST_CHECK(HAS(buf, "v -> binding 2"));          /* watcher cookie   */
    MOQ_TEST_CHECK(HAS(buf, "v/cam \"hd\" state=active upstream_binding=1"));
    MOQ_TEST_CHECK(HAS(buf, "groups=3 records=3"));
    MOQ_TEST_CHECK(HAS(buf, "oldest=1 newest=3"));
    MOQ_TEST_CHECK(HAS(buf, "binding=3 state=active start=1/0 end=- "
                           "lag_groups=3 frontier=1 skip_pending=0 "
                           "start_before_retention=0"));
    MOQ_TEST_CHECK(HAS(buf, "v/cam2 \"x\" state=pending"));
    MOQ_TEST_CHECK(HAS(buf, "binding=4 state=parked"));

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("route_dump_text");
    return failures;
}

/* A warm track: publish + retain, then lose the upstream. It keeps serving
 * content but reports no upstream binding. */
static int
test_route_dump_warm(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a);
    MOQ_TEST_CHECK(c != NULL);
    moqr_intent_t its[8];
    moq_bytes_t nsb[2];

    moqr_binding_t pub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "a", "b")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "a", "b"), B("t"),
                                          5, &track) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK(ing(c, &a, track, 7, 0) == MOQR_OK);

    /* Closing the publishing binding leaves the retained track WARM. */
    MOQ_TEST_CHECK(moqr_core_binding_close(c, pub, 100) == MOQR_OK);
    (void)drain(c, its, 8);

    char buf[2048];
    size_t w = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf,
        "\"name\":\"t\",\"state\":\"warm\",\"upstream_binding\":null"));
    MOQ_TEST_CHECK(HAS(buf, "\"oldest_group\":7,\"newest_group\":7"));
    /* The announce and its binding are gone with the closed publisher. */
    MOQ_TEST_CHECK(HAS(buf, "\"announces\":[]"));

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("route_dump_warm");
    return failures;
}

static int
test_route_dump_truncation(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = build_populated(&a, NULL);
    MOQ_TEST_CHECK(c != NULL);

    char big[4096];
    size_t need = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, big, sizeof(big), &need) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(need > 0);

    char small[48];
    size_t w = 0;
    moqr_result_t rc =
        moqr_core_route_dump_json(c, small, sizeof(small), &w);
    MOQ_TEST_CHECK(rc == MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK_EQ_SIZE(w, need);               /* required size */
    MOQ_TEST_CHECK(small[sizeof(small) - 1] == '\0');   /* NUL-terminated */

    /* Growing to need + 1 succeeds. */
    char exact[4096];
    MOQ_TEST_CHECK(need + 1 <= sizeof(exact));
    w = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, exact, need + 1, &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(w, need);

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("route_dump_truncation");
    return failures;
}

/* Retired subscriptions and freed tracks must vanish from the dump — no
 * stale name or slot garbage. */
static int
test_route_dump_stale(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a);
    MOQ_TEST_CHECK(c != NULL);
    moqr_intent_t its[8];
    moq_bytes_t nsb[2];

    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "s", "t")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "s", "t"),
                                          B("zz"), 5, &track) == MOQR_OK);
    (void)drain(c, its, 8);

    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "s", "t");
    rq.name = B("zz");
    rq.filter.type = MOQR_FILTER_LARGEST_OBJECT;
    rq.cookie = 1;
    moqr_sub_t sh;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sub, &rq, &sh) == MOQR_OK);
    (void)drain(c, its, 8);

    char buf[2048];
    size_t w = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf, "\"name\":\"zz\""));
    MOQ_TEST_CHECK(HAS(buf, "\"state\":\"active\""));   /* the subscriber */

    /* Retire the subscription: it must disappear, leaving an empty sub list
     * on a still-live (WARM/linger) track, with no leftover parked/active. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(c, sh, 50) == MOQR_OK);
    (void)drain(c, its, 8);
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf, "\"subscriptions\":[]"));   /* sub retired */
    MOQ_TEST_CHECK(!HAS(buf, "\"skip_pending\""));      /* no sub objects */
    MOQ_TEST_CHECK(HAS(buf, "\"name\":\"zz\""));         /* track still live */

    /* Tear the whole thing down: the freed track's name must not survive. */
    MOQ_TEST_CHECK(moqr_core_binding_close(c, pub, 60) == MOQR_OK);
    (void)drain(c, its, 8);
    (void)moqr_core_tick(c, 100000);   /* expire linger */
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(!HAS(buf, "zz"));            /* no stale name */
    MOQ_TEST_CHECK(HAS(buf, "\"tracks\":[]"));

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("route_dump_stale");
    return failures;
}

/* Binary namespace/name bytes must escape safely: JSON stays valid ASCII
 * (\u00XX for control/non-ASCII, \" and \\ for the specials, "/" literal),
 * and text renders the specials and non-printables as \xXX. */
static int
test_route_dump_escaping(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_t *c = mkcore(&a);
    MOQ_TEST_CHECK(c != NULL);
    moqr_intent_t its[8];

    moqr_binding_t pub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);

    /* A namespace part carrying slash, backslash, NUL, a control byte, and a
     * non-ASCII byte; a track name carrying a quote, DEL, and 0xFF. */
    static const uint8_t part_bin[] = { '/', '\\', 0x00, 0x1f, 0xe2 };
    static const uint8_t name_bin[] = { '"', 0x7f, 0xff };
    moq_bytes_t parts[2];
    parts[0] = B("ns");
    parts[1] = (moq_bytes_t){ part_bin, sizeof(part_bin) };
    moqr_ns_t ns = { parts, 2 };
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, ns) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(
                       c, pub, ns,
                       (moq_bytes_t){ name_bin, sizeof(name_bin) }, 5,
                       &track) == MOQR_OK);
    (void)drain(c, its, 8);

    char buf[2048];
    size_t w = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(strlen(buf), w);       /* no embedded NUL leaked */
    MOQ_TEST_CHECK(HAS(buf, "\\u0000"));          /* NUL           */
    MOQ_TEST_CHECK(HAS(buf, "\\u001f"));          /* control       */
    MOQ_TEST_CHECK(HAS(buf, "\\u00e2"));          /* non-ASCII     */
    MOQ_TEST_CHECK(HAS(buf, "\\u007f"));          /* DEL           */
    MOQ_TEST_CHECK(HAS(buf, "\\u00ff"));          /* high non-ASCII */
    MOQ_TEST_CHECK(HAS(buf, "\\\""));             /* escaped quote */
    MOQ_TEST_CHECK(!HAS(buf, "\\u002f"));         /* "/" not \u-escaped */
    MOQ_TEST_CHECK(!HAS(buf, "\\/"));             /* "/" not \-escaped  */

    MOQ_TEST_CHECK(moqr_core_route_dump_text(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf, "\\x2f"));            /* slash     */
    MOQ_TEST_CHECK(HAS(buf, "\\x5c"));            /* backslash */
    MOQ_TEST_CHECK(HAS(buf, "\\x22"));            /* quote     */
    MOQ_TEST_CHECK(HAS(buf, "\\x00"));            /* NUL       */
    MOQ_TEST_CHECK(HAS(buf, "\\x1f"));            /* control   */
    MOQ_TEST_CHECK(HAS(buf, "\\xe2"));
    MOQ_TEST_CHECK(HAS(buf, "\\x7f"));
    MOQ_TEST_CHECK(HAS(buf, "\\xff"));

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("route_dump_escaping");
    return failures;
}

/* The read-only loss signal skip_pending cannot see: an ACTIVE sub asks to
 * start at group 1, never pulls, and retention evicts group 1. skip_pending
 * stays false (no delivery pass fired the latch), but start_before_retention
 * flags that the requested start fell out of the retention window. */
static int
test_route_dump_before_retention(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_core_relay_cfg_t cfg;
    moqr_core_relay_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.log_budget.max_groups = 2;          /* keep 2 groups; a 3rd evicts */
    cfg.log_budget.max_bytes = 1 << 20;
    cfg.linger_us = 1000;
    moqr_core_t *c = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&cfg, &c) == MOQR_OK);
    moqr_intent_t its[8];
    moq_bytes_t nsb[2];

    moqr_binding_t pub, sub;
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &pub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c, 2, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c, pub, NS2(nsb, "a", "b")) == MOQR_OK);
    moqr_track_t track;
    MOQ_TEST_CHECK(moqr_core_publish_open(c, pub, NS2(nsb, "a", "b"), B("t"),
                                          5, &track) == MOQR_OK);
    (void)drain(c, its, 8);
    /* Groups 1..3 into a 2-group log: group 1 is evicted (oldest becomes 2). */
    for (uint64_t g = 1; g <= 3; g++) {
        MOQ_TEST_CHECK(ing(c, &a, track, g, 0) == MOQR_OK);
    }

    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = NS2(nsb, "a", "b");
    rq.name = B("t");
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.filter.start_group = 1;
    rq.filter.start_object = 0;
    rq.cookie = 42;
    moqr_sub_t sh;
    MOQ_TEST_CHECK(moqr_core_subscribe(c, sub, &rq, &sh) == MOQR_OK);
    (void)drain(c, its, 8);   /* accepted; never pulled */

    char buf[2048];
    size_t w = 0;
    MOQ_TEST_CHECK(moqr_core_route_dump_json(c, buf, sizeof(buf), &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(HAS(buf, "\"oldest_group\":2"));    /* group 1 evicted */
    MOQ_TEST_CHECK(HAS(buf, "\"start_group\":1"));
    /* The honest signal: latch quiet, retention-window flag set. */
    MOQ_TEST_CHECK(HAS(buf,
        "\"skip_pending\":false,\"start_before_retention\":true"));

    moqr_core_destroy(c);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("route_dump_before_retention");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_route_dump_empty();
    failures += test_route_dump_escaping();
    failures += test_route_dump_before_retention();
    failures += test_route_dump_json();
    failures += test_route_dump_text();
    failures += test_route_dump_warm();
    failures += test_route_dump_truncation();
    failures += test_route_dump_stale();
    return failures != 0;
}
