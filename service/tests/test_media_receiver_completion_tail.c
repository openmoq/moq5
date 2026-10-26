/*
 * A terminating catalog (MSF-01 §5.1.3 / §11.3: isComplete with an empty
 * tracks array) ends the broadcast; it does not withdraw media the receiver
 * has already accepted.
 *
 * A publisher that finishes writes its last objects, ends the track and
 * publishes the terminating catalog. Nothing orders that catalog behind the
 * media on the subscriber, so it can be reconciled while objects the receiver
 * already parsed and queued are waiting for the application. Adopting it
 * reports every track REMOVED on its stable handle and latches
 * catalog_complete; the accepted objects stay pollable, byte for byte, on the
 * same handle and in order, and are never counted as dropped.
 *
 * The controls pin what completion does NOT change: an ordinary
 * (non-terminating) catalog that drops the track still purges its queued
 * objects, as does the application's own unsubscribe; and a repeated
 * terminating catalog neither removes the track twice nor loses the tail.
 *
 * Every row runs once per transport draft (16 and 18). Each object's
 * location is carried in its payload (the public media object has no
 * group/object field) together with end_of_group, and ownership is judged on
 * a counting allocator: a polled object's cleanup releases exactly what its
 * intake retained, a purge releases the purged objects, and nothing is live
 * after the receiver is freed. Event sequences are declared in full.
 *
 * Scope: this is the ACCEPTED queue only. Objects of the track still in flight
 * when the terminating catalog is adopted are not covered here.
 */

#include <moq/media_receiver.h>
#include "test_support.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Test seams (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING). */
moq_media_receiver_t *moq_media_receiver_test_new_cfg_alloc(
    const moq_media_receiver_cfg_t *cfg, const moq_alloc_t *alloc);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_ingest(moq_media_receiver_t *r, uint64_t group,
                                    uint64_t object, const char *json, size_t len);
bool moq_media_receiver_test_poll(moq_media_receiver_t *r,
                                  moq_media_track_event_kind_t *kind,
                                  moq_media_track_t **track);
void moq_media_receiver_test_set_transport_version(moq_media_receiver_t *r, moq_version_t v);
void moq_media_receiver_test_push_object(moq_media_receiver_t *r, moq_media_track_t *track,
                                         uint64_t group, uint64_t object, moq_object_status_t status,
                                         bool end_of_group, bool datagram,
                                         const uint8_t *payload, size_t len,
                                         const uint8_t *properties, size_t plen);

/* -- counting allocator --------------------------------------------------- */
typedef struct { long live; long total; } astats_t;
static void *c_alloc(size_t n, void *ctx)
{ astats_t *s = ctx; void *p = malloc(n ? n : 1); if (p) { s->live++; s->total++; } return p; }
static void *c_realloc(void *p, size_t o, size_t n, void *ctx)
{ astats_t *s = ctx; (void)o; void *q = realloc(p, n ? n : 1); if (q && !p) { s->live++; s->total++; } return q; }
static void c_free(void *p, size_t n, void *ctx)
{ astats_t *s = ctx; (void)n; if (p) { free(p); s->live--; } }

/* -- fixture --------------------------------------------------------------- */
#define TRK(name) "{\"name\":\"" name "\",\"packaging\":\"loc\",\"isLive\":true,\"role\":\"video\"}"
static const char k_live[]     = "{\"version\":\"1\",\"tracks\":[" TRK("video") "]}";
static const char k_withdraw[] = "{\"version\":\"1\",\"tracks\":[" TRK("other") "]}";
static const char k_complete[] = "{\"version\":\"1\",\"isComplete\":true,\"tracks\":[]}";

/* Five accepted objects across two groups; each payload spells its location. */
#define OBJECTS 5
static const struct { uint64_t group, object; bool end_of_group; } k_loc[OBJECTS] = {
    { 0, 0, false }, { 0, 1, true }, { 1, 0, false }, { 1, 1, false }, { 1, 2, true },
};

static size_t payload_of(int i, uint8_t out[16])
{
    size_t len = 6 + (size_t)i;                      /* distinct lengths */
    out[0] = 'M';
    out[1] = (uint8_t)k_loc[i].group;
    out[2] = (uint8_t)k_loc[i].object;
    for (size_t k = 3; k < len; k++) out[k] = (uint8_t)(0xA0 + i * 7 + (int)k);
    return len;
}

static uint64_t bytes_of(int from, int to)
{
    uint8_t p[16];
    uint64_t n = 0;
    for (int i = from; i < to; i++) n += payload_of(i, p);
    return n;
}

enum mode { TERMINAL, TERMINAL_REPEATED, ORDINARY_REMOVAL, UNSUBSCRIBE };
static const char *const k_mode_name[] = {
    "terminal_catalog_keeps_the_accepted_tail",
    "repeated_terminal_catalog_neither_removes_twice_nor_loses",
    "ordinary_removal_still_purges",
    "unsubscribe_still_purges",
};

typedef struct {
    astats_t st;
    moq_alloc_t alloc;
    moq_media_receiver_t *r;
    moq_media_track_t *video;
    long per_object;            /* allocations one accepted object retains */
} rig_t;

/* -- declared event sequences --------------------------------------------- */
typedef struct {
    moq_media_track_event_kind_t kind;
    moq_media_track_t *track;
    bool any_track;             /* a handle this row cannot name in advance */
} ev_t;

static void expect_events(rig_t *g, const ev_t *want, int n, const char *phase)
{
    ev_t got[16];
    int m = 0;
    moq_media_track_event_kind_t k;
    moq_media_track_t *t;
    while (moq_media_receiver_test_poll(g->r, &k, &t)) {
        if (m < 16) got[m] = (ev_t){ .kind = k, .track = t };
        m++;
    }
    bool ok = m == n;
    for (int i = 0; ok && i < n; i++)
        ok = got[i].kind == want[i].kind && (want[i].any_track || got[i].track == want[i].track);
    if (!ok) {
        fprintf(stderr, "  %s: event sequence differs; got %d:", phase, m);
        for (int i = 0; i < m && i < 16; i++) fprintf(stderr, " %d", (int)got[i].kind);
        fprintf(stderr, "; declared %d:", n);
        for (int i = 0; i < n; i++) fprintf(stderr, " %d", (int)want[i].kind);
        fprintf(stderr, "\n");
    }
    MOQ_TEST_CHECK(ok);
}

static moq_media_receiver_stats_t stats_of(rig_t *g)
{
    moq_media_receiver_stats_t s;
    memset(&s, 0, sizeof s);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_get_stats(g->r, &s, sizeof s), (int)MOQ_OK);
    return s;
}

/* Objects [queued_from, OBJECTS) are queued, `dropped` were dropped, and no
 * other policy acted. */
static void expect_queue(rig_t *g, int queued_from, uint64_t dropped)
{
    moq_media_receiver_stats_t s = stats_of(g);
    MOQ_TEST_CHECK_EQ_U64(s.objects_received, OBJECTS);
    MOQ_TEST_CHECK_EQ_U64(s.objects_queued, (uint64_t)(OBJECTS - queued_from));
    MOQ_TEST_CHECK_EQ_U64(s.bytes_queued, bytes_of(queued_from, OBJECTS));
    MOQ_TEST_CHECK_EQ_U64(s.objects_dropped, dropped);
    MOQ_TEST_CHECK_EQ_U64(s.keyframes_dropped, 0);
    MOQ_TEST_CHECK_EQ_U64(s.groups_dropped, 0);
    MOQ_TEST_CHECK_EQ_U64(s.overflow_events, 0);
    MOQ_TEST_CHECK_EQ_U64(s.parse_drops, 0);
}

/* Poll object `i`: the same handle, its exact bytes and location, then a
 * cleanup that releases exactly what its intake retained. */
static void expect_object(rig_t *g, int i)
{
    moq_media_object_t o;
    moq_result_t rc = moq_media_receiver_poll_object(g->r, &o, sizeof o);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) return;
    uint8_t want[16];
    size_t len = payload_of(i, want);
    MOQ_TEST_CHECK(o.track == g->video);
    MOQ_TEST_CHECK_EQ_INT((int)o.packaging, (int)MOQ_MEDIA_PACKAGING_RAW);
    MOQ_TEST_CHECK_EQ_INT((int)o.status, (int)MOQ_OBJECT_NORMAL);
    MOQ_TEST_CHECK(o.end_of_group == k_loc[i].end_of_group);
    MOQ_TEST_CHECK_EQ_SIZE(o.payload.len, len);
    MOQ_TEST_CHECK(o.payload.len == len && memcmp(o.payload.data, want, len) == 0);
    MOQ_TEST_CHECK_EQ_SIZE(o.fragment.len, 0);
    long before = g->st.live;
    moq_media_object_cleanup(&o);
    MOQ_TEST_CHECK_EQ_INT((int)(before - g->st.live), (int)g->per_object);
}

/* The queue is empty. A receiver without an endpoint reads as closed once
 * empty, so the answer is CLOSED rather than DONE; either way nothing moves. */
static void expect_empty(rig_t *g)
{
    moq_media_object_t o;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_poll_object(g->r, &o, sizeof o), (int)MOQ_ERR_CLOSED);
}

static void ingest(rig_t *g, uint64_t group, const char *json)
{
    moq_media_receiver_test_ingest(g->r, group, 0, json, strlen(json));
}

static void run(moq_version_t version, enum mode mode)
{
    int before_failures = failures;
    rig_t g;
    memset(&g, 0, sizeof g);
    g.alloc = (moq_alloc_t){ .ctx = &g.st, .alloc = c_alloc, .realloc = c_realloc, .free = c_free };
    static moq_bytes_t ns[2] = { MOQ_BYTES_LITERAL("live"), MOQ_BYTES_LITERAL("cam1") };
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live(&cfg);
    cfg.namespace_ = (moq_namespace_t){ ns, 2 };
    cfg.auto_subscribe = false;
    g.r = moq_media_receiver_test_new_cfg_alloc(&cfg, &g.alloc);
    MOQ_TEST_CHECK(g.r != NULL);
    if (!g.r) return;
    /* The transport draft the pump would read from the live session. */
    moq_media_receiver_test_set_transport_version(g.r, version);

    ingest(&g, 0, k_live);
    moq_media_track_event_kind_t k = MOQ_MEDIA_CATALOG_READY;
    MOQ_TEST_CHECK(moq_media_receiver_test_poll(g.r, &k, &g.video));
    MOQ_TEST_CHECK_EQ_INT((int)k, (int)MOQ_MEDIA_TRACK_ADDED);
    const ev_t discovered[] = { { .kind = MOQ_MEDIA_CATALOG_READY, .track = NULL } };
    expect_events(&g, discovered, 1, "discovery");
    if (!g.video) { moq_media_receiver_test_free(g.r); return; }

    /* The application subscribes through the public call; the peer delivers. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_subscribe_track(g.r, g.video, NULL), (int)MOQ_OK);
    expect_events(&g, NULL, 0, "subscribe");
    for (int i = 0; i < OBJECTS; i++) {
        uint8_t p[16];
        size_t len = payload_of(i, p);
        long live = g.st.live;
        moq_media_receiver_test_push_object(g.r, g.video, k_loc[i].group, k_loc[i].object, MOQ_OBJECT_NORMAL,
                                            k_loc[i].end_of_group, false, p, len, NULL, 0);
        long retained = g.st.live - live;
        if (i == 0) g.per_object = retained;
        MOQ_TEST_CHECK_EQ_INT((int)retained, (int)g.per_object);
    }
    MOQ_TEST_CHECK(g.per_object > 0);
    expect_queue(&g, 0, 0);

    /* The application has read the first object when the next step lands. */
    expect_object(&g, 0);
    expect_queue(&g, 1, 0);

    long live = g.st.live;
    switch (mode) {
    case TERMINAL:
    case TERMINAL_REPEATED: {
        ingest(&g, 1, k_complete);
        const ev_t removed[] = { { .kind = MOQ_MEDIA_TRACK_REMOVED, .track = g.video } };
        expect_events(&g, removed, 1, "terminal catalog");
        MOQ_TEST_CHECK(stats_of(&g).catalog_complete);
        expect_queue(&g, 1, 0);
        if (mode == TERMINAL_REPEATED) {
            ingest(&g, 1, k_complete);                         /* the same object again */
            expect_events(&g, NULL, 0, "duplicate terminal catalog");
            ingest(&g, 2, k_complete);                         /* a later terminating generation */
            expect_events(&g, NULL, 0, "later terminal catalog");
            MOQ_TEST_CHECK(stats_of(&g).catalog_complete);
            expect_queue(&g, 1, 0);
        }
        for (int i = 1; i < OBJECTS; i++) expect_object(&g, i);
        expect_empty(&g);
        moq_media_receiver_stats_t s = stats_of(&g);
        MOQ_TEST_CHECK_EQ_U64(s.objects_received, OBJECTS);
        MOQ_TEST_CHECK_EQ_U64(s.objects_queued, 0);
        MOQ_TEST_CHECK_EQ_U64(s.bytes_queued, 0);
        MOQ_TEST_CHECK_EQ_U64(s.objects_dropped, 0);
        break;
    }
    case ORDINARY_REMOVAL: {
        ingest(&g, 1, k_withdraw);
        const ev_t replaced[] = {
            { .kind = MOQ_MEDIA_TRACK_ADDED, .any_track = true },
            { .kind = MOQ_MEDIA_TRACK_REMOVED, .track = g.video },
        };
        expect_events(&g, replaced, 2, "ordinary removal");
        MOQ_TEST_CHECK(!stats_of(&g).catalog_complete);
        expect_queue(&g, OBJECTS, OBJECTS - 1);
        expect_empty(&g);
        break;
    }
    case UNSUBSCRIBE: {
        MOQ_TEST_CHECK_EQ_INT((int)moq_media_receiver_unsubscribe_track(g.r, g.video), (int)MOQ_OK);
        expect_events(&g, NULL, 0, "unsubscribe");
        expect_queue(&g, OBJECTS, OBJECTS - 1);
        MOQ_TEST_CHECK_EQ_INT((int)(live - g.st.live), (int)(g.per_object * (OBJECTS - 1)));
        expect_empty(&g);
        break;
    }
    }

    moq_media_receiver_test_free(g.r);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)g.st.live, 0);
    printf("%s: media_receiver.completion_tail.%s.d%d (%ld allocations)\n",
           failures == before_failures ? "PASS" : "FAIL", k_mode_name[mode], (int)version, g.st.total);
}

int main(void)
{
    static const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18 };
    for (size_t v = 0; v < sizeof versions / sizeof versions[0]; v++)
        for (int m = TERMINAL; m <= UNSUBSCRIBE; m++)
            run(versions[v], (enum mode)m);
    return failures ? 1 : 0;
}
