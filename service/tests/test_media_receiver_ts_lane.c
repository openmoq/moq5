/*
 * MPEG2TS reorder windows on the stream-terminal lane. White-box: the
 * production window, release, intake and lane paths, fed directly, with
 * the endpoint wake observed.
 *
 * Pinned:
 * - a held Object keeps the lane slot its intake reserved: a record of its
 *   track that waits for it never takes the room its release needs, so a
 *   full lane whose head waits for a window still progresses, and the pass
 *   that releases the Object marks the record ready and wakes;
 * - a release that fits the queue destroys nothing and needs no further
 *   room; one that must evict waits, with nothing dropped, while the lane
 *   lacks a record per delivery domain: the window reports no deadline,
 *   intake stops, a record of its track stays behind the waiting Object,
 *   and the application's poll wakes the network thread;
 * - every reserved slot returns: a rejected payload's, and a window's when
 *   it is freed; an Object refused at admission puts its drop in its own
 *   reserved slot;
 * - a drop made by a timer release precedes the released Object and keeps
 *   its sequence: an Object the window still holds, taken in later, does
 *   not hold the drop back;
 * - a release whose payload transform fails during a newer intake leaves
 *   that intake its own sequence;
 * - a status Object goes out after everything that was due when it
 *   arrived, even when that release had to wait for room; while it waits,
 *   intake waits too, even once the application's polls have returned the
 *   room, so a later status cannot replace it; resuming the window first
 *   lets the later one in after it;
 * - an END_OF_TRACK that ends the track at once waits for the window's
 *   stopped release, and follows it; intake waits with it.
 */
#include <moq/media_receiver.h>
#include <moq/subscriber.h>
static unsigned g_wakes;
static void observed_wake(moq_endpoint_t *ep) { (void)ep; g_wakes++; }
#define moq_endpoint_wake observed_wake
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"
#undef moq_endpoint_wake
#include "test_support.h"
#include "test_alloc_support.h"
#include "ts_stream.h"

#include <stdio.h>
#include <string.h>

static int failures;

static const char ts_catalog[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
    "\"mimeType\":\"video/mp2t\",\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":188,\"mpeg2tsProgramNumber\":1},"
    "{\"name\":\"loc\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";
static const char m2ts_catalog[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
    "\"mimeType\":\"video/mp2t\",\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":192,\"mpeg2tsProgramNumber\":1},"
    "{\"name\":\"loc\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";

static char gen_ts, gen_loc, gen_catalog;
#define SUB_TS      ((moq_sub_track_t *)(void *)&gen_ts)
#define SUB_LOC     ((moq_sub_track_t *)(void *)&gen_loc)
#define SUB_CATALOG ((moq_sub_track_t *)(void *)&gen_catalog)
#define WINDOW_US   1000u
#define LATE        (10 + 2 * WINDOW_US)

/* The counting allocator, refusing the next request once armed. */
typedef struct {
    test_alloc_state_t state;
    bool fail_next;
} fail_alloc_t;

static void *fail_alloc(size_t size, void *ctx)
{
    fail_alloc_t *a = (fail_alloc_t *)ctx;
    if (a->fail_next) { a->fail_next = false; return NULL; }
    return test_alloc(size, &a->state);
}
static void *fail_realloc(void *p, size_t o, size_t n, void *ctx)
{
    return test_realloc(p, o, n, &((fail_alloc_t *)ctx)->state);
}
static void fail_free(void *p, size_t size, void *ctx)
{
    test_free(p, size, &((fail_alloc_t *)ctx)->state);
}

typedef struct fx {
    fail_alloc_t a;
    moq_alloc_t alloc;
    moq_media_receiver_t *r;
    moq_media_track_t *ts, *loc;
    ts_gen_t gen;
} fx_t;

/* A DROP_GROUP queue of `max_objects` and a lane of `lane` records. With
 * `m2ts`, 192-octet packets whose prefixes the receiver strips. */
static bool fx_open(fx_t *f, uint32_t max_objects, uint32_t lane, bool m2ts)
{
    memset(f, 0, sizeof(*f));
    f->alloc = (moq_alloc_t){ &f->a, fail_alloc, fail_realloc, fail_free };
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    static moq_bytes_t ns[] = { { (const uint8_t *)"tslane", 6 } };
    cfg.namespace_ = (moq_namespace_t){ ns, 1 };
    cfg.auto_subscribe = true;
    cfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    cfg.overflow.max_objects = max_objects;
    cfg.mpeg2ts_reorder_window_us = WINDOW_US;
    cfg.mpeg2ts_strip_prefix = m2ts;
    cfg.companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    cfg.surface_stream_terminals = true;
    cfg.max_stream_terminals = lane;
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    MOQ_TEST_CHECK(f->r != NULL);
    if (!f->r) return false;
    f->r->transport_version = MOQ_VERSION_DRAFT_18;
    const char *cat = m2ts ? m2ts_catalog : ts_catalog;
    moq_media_receiver_test_ingest(f->r, 0, 0, cat, strlen(cat));
    moq_media_track_event_t ev;
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {
        if (ev.kind != MOQ_MEDIA_TRACK_ADDED) continue;
        if (ev.desc->name.len == 2) f->ts = ev.track;
        if (ev.desc->name.len == 3) f->loc = ev.track;
    }
    MOQ_TEST_CHECK(f->ts != NULL && f->loc != NULL);
    if (!f->ts || !f->loc) return false;
    f->ts->sub_track = SUB_TS;
    f->loc->sub_track = SUB_LOC;
    f->r->catalog_sub = SUB_CATALOG;
    MOQ_TEST_CHECK(receiver_ts_start(f->r, f->ts, 0));
    ts_gen_init(&f->gen, m2ts ? 192 : 188, 1, false);
    return true;
}

static void fx_close(fx_t *f)
{
    moq_media_track_event_t ev;
    moq_media_object_t o;
    if (!f->r) return;
    receiver_ts_free(f->r, f->ts);
    MOQ_TEST_CHECK(f->r->st_reserved == 0);
    while (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) == MOQ_OK)
        moq_media_object_cleanup(&o);
    while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {}
    moq_media_receiver_test_free(f->r);
    f->r = NULL;
    MOQ_TEST_CHECK(f->a.state.balance == 0);
}

static moq_sub_object_t ts_object(fx_t *f, uint64_t g, uint64_t o)
{
    uint8_t buf[8 * 192];
    size_t n = ts_gen_gop(&f->gen, buf, 1);
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_TS;
    so.group_id = g;
    so.object_id = o;
    so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f->r->alloc, buf, n, &so.payload) == MOQ_OK);
    return so;
}

/* An Object of the TS track, taken in at `now`. */
static void take_ts(fx_t *f, uint64_t g, uint64_t o, uint64_t now)
{
    MOQ_TEST_CHECK(receiver_intake_begin(f->r));
    moq_sub_object_t so = ts_object(f, g, o);
    receiver_on_ts_object(f->r, f->ts, &so, now);
    receiver_intake_end(f->r);
}

/* A status Object of the TS track, taken in at `now`. */
static void take_ts_status(fx_t *f, uint64_t g, uint64_t o,
                           moq_object_status_t status, uint64_t now)
{
    MOQ_TEST_CHECK(receiver_intake_begin(f->r));
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_TS;
    so.group_id = g;
    so.object_id = o;
    so.status = status;
    receiver_on_ts_object(f->r, f->ts, &so, now);
    receiver_intake_end(f->r);
}

/* An Object of the LOC track, taken in. */
static void take_loc(fx_t *f, uint64_t g, uint64_t o)
{
    MOQ_TEST_CHECK(receiver_intake_begin(f->r));
    static const uint8_t d[4] = { 1, 2, 3, 4 };
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_LOC;
    so.group_id = g;
    so.object_id = o;
    so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f->r->alloc, d, sizeof(d), &so.payload) == MOQ_OK);
    receiver_on_media_object(f->r, f->loc, &so, 0);
    receiver_intake_end(f->r);
}

/* A FIN of group g on track t, as the facade surfaces it. */
static void take_fin(fx_t *f, moq_media_track_t *t, uint64_t g)
{
    pthread_mutex_lock(&f->r->mu);
    moq_media_stream_terminal_t rec;
    memset(&rec, 0, sizeof(rec));
    rec.kind = MOQ_MEDIA_STREAM_FIN;
    rec.track = t;
    rec.group_id = g;
    f->r->st_held = rec;
    f->r->st_has_held = true;
    st_place_held_locked(f->r);
    MOQ_TEST_CHECK(!f->r->st_has_held);
    pthread_mutex_unlock(&f->r->mu);
}

static uint64_t dropped(fx_t *f)
{
    moq_media_receiver_stats_t sd;
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(f->r, &sd, sizeof(sd)) == MOQ_OK);
    return sd.objects_dropped;
}

static bool poll_is(fx_t *f, moq_media_track_t *t, uint64_t g, uint64_t o)
{
    moq_media_object_t obj;
    if (moq_media_receiver_poll_object(f->r, &obj, sizeof(obj)) != MOQ_OK) return false;
    bool ok = obj.track == t && obj.group_id == g && obj.object_id == o;
    if (!ok)
        fprintf(stderr, "  polled %s %llu/%llu, wanted %llu/%llu\n",
                obj.track == f->ts ? "ts" : "loc", (unsigned long long)obj.group_id,
                (unsigned long long)obj.object_id, (unsigned long long)g,
                (unsigned long long)o);
    moq_media_object_cleanup(&obj);
    return ok;
}

static moq_result_t poll_st(fx_t *f, moq_media_stream_terminal_t *st)
{
    return moq_media_receiver_poll_stream_terminal(f->r, st, sizeof(*st));
}

/* TS Objects 0/1 and, later, 0/3 are held (0/0 and 0/2 never come). The
 * LOC track's Object fills the one-Object queue, and the TS group's FIN
 * takes the lane record the held Objects' reserved slots leave. The timer
 * would release 0/1 by evicting the LOC Object, with no room for that
 * drop: the window waits and reports no deadline, although 0/3 is due
 * later; intake stops, and the FIN stays behind the waiting Object.
 * Polling the LOC Object wakes the network thread, and the release goes
 * ahead into the empty queue, with nothing evicted; 0/3 follows on its own
 * timer, then the FIN. */
static void test_release_waits_for_room(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 1, 3, false)) { fx_close(&f); return; }
    take_ts(&f, 0, 1, 10);
    take_ts(&f, 0, 3, 500);
    take_loc(&f, 5, 0);
    take_fin(&f, f.ts, 0);
    receiver_ts_timers(f.r, 10 + WINDOW_US + 90);
    receiver_st_mark_ready(f.r);
    MOQ_TEST_CHECK(receiver_next_deadline_us(f.r) == UINT64_MAX);
    MOQ_TEST_CHECK(f.r->ts_stalled == 1);
    MOQ_TEST_CHECK(dropped(&f) == 0);
    MOQ_TEST_CHECK(f.r->obj_tail - f.r->obj_head == 1);
    MOQ_TEST_CHECK(!receiver_intake_begin(f.r));
    moq_media_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);

    unsigned wakes = g_wakes;
    MOQ_TEST_CHECK(poll_is(&f, f.loc, 5, 0));
    MOQ_TEST_CHECK(g_wakes == wakes + 1);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    receiver_ts_timers(f.r, 10 + WINDOW_US + 90);
    receiver_st_mark_ready(f.r);
    MOQ_TEST_CHECK(f.r->ts_stalled == 0);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 1));
    MOQ_TEST_CHECK(receiver_next_deadline_us(f.r) == 500 + WINDOW_US);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    receiver_ts_timers(f.r, 500 + WINDOW_US);
    receiver_st_mark_ready(f.r);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 3));
    MOQ_TEST_CHECK(dropped(&f) == 0);
    MOQ_TEST_CHECK(receiver_intake_begin(f.r));
    receiver_intake_end(f.r);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.track == f.ts && st.kind == MOQ_MEDIA_STREAM_FIN);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_release_waits_for_room\n");
}

/* A release that waits for room has taken its Object out of the window
 * and parked it: the TS group's FIN still waits behind it, and follows it
 * once the release goes ahead. */
static void test_fin_waits_for_parked(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 1, 2, false)) { fx_close(&f); return; }
    take_ts(&f, 0, 1, 10);
    take_loc(&f, 5, 0);
    take_fin(&f, f.ts, 0);
    receiver_ts_timers(f.r, LATE);
    receiver_st_mark_ready(f.r);
    MOQ_TEST_CHECK(f.r->ts_stalled == 1);
    moq_media_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_is(&f, f.loc, 5, 0));
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    receiver_ts_timers(f.r, LATE);
    receiver_st_mark_ready(f.r);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 1));
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.track == f.ts && st.kind == MOQ_MEDIA_STREAM_FIN);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_fin_waits_for_parked\n");
}

/* With room in a two-Object queue, the held TS Object's timer release
 * destroys nothing, so it needs no lane room: it goes ahead although the
 * LOC FIN took the last unreserved record. */
static void test_fit_needs_no_room(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 2, 2, false)) { fx_close(&f); return; }
    take_ts(&f, 0, 1, 10);
    take_loc(&f, 5, 0);
    take_fin(&f, f.loc, 5);
    MOQ_TEST_CHECK(st_unreserved_locked(f.r) == 0);
    receiver_ts_timers(f.r, LATE);
    MOQ_TEST_CHECK(f.r->ts_stalled == 0);
    MOQ_TEST_CHECK(f.r->obj_tail - f.r->obj_head == 2);
    MOQ_TEST_CHECK(poll_is(&f, f.loc, 5, 0));
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 1));
    MOQ_TEST_CHECK(dropped(&f) == 0);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_fit_needs_no_room\n");
}

/* A held Object whose payload the window rejects at release returns its
 * reserved slot. */
static void test_rejected_returns_slot(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 4, 2, false)) { fx_close(&f); return; }
    MOQ_TEST_CHECK(receiver_intake_begin(f.r));
    static const uint8_t junk[188] = { 0x00 };
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_TS;
    so.group_id = 0;
    so.object_id = 1;
    so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f.r->alloc, junk, sizeof(junk), &so.payload) == MOQ_OK);
    receiver_on_ts_object(f.r, f.ts, &so, 10);
    receiver_intake_end(f.r);
    MOQ_TEST_CHECK(f.r->st_reserved == 1);
    receiver_ts_timers(f.r, LATE);
    MOQ_TEST_CHECK(f.r->obj_tail == f.r->obj_head);
    MOQ_TEST_CHECK(f.r->st_reserved == 0);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_rejected_returns_slot\n");
}

/* On a one-record lane, an Object larger than the whole byte budget is
 * refused at admission, and its RECEIVER_DROP takes the one slot its
 * intake reserved. */
static void test_own_drop_takes_slot(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 4, 1, false)) { fx_close(&f); return; }
    f.r->byte_cap = 2;
    take_loc(&f, 5, 0);
    MOQ_TEST_CHECK(dropped(&f) == 1);
    moq_media_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_MEDIA_STREAM_RECEIVER_DROP && st.track == f.loc);
    MOQ_TEST_CHECK(st.group_id == 5 && st.last_group_id == 5);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_own_drop_takes_slot\n");
}

/* The TS track's FIN waits at the head of a full three-record lane for its
 * held Object, with the LOC track's FIN behind it. The held Object kept its
 * own slot, so its timer release needs nothing from the lane: the pass
 * that releases it marks the FIN ready and wakes, and every record and
 * Object arrives, in order. */
static void test_held_keeps_slot(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 1, 3, false)) { fx_close(&f); return; }
    take_ts(&f, 0, 1, 10);
    take_fin(&f, f.ts, 0);
    take_loc(&f, 5, 0);
    take_fin(&f, f.loc, 5);
    MOQ_TEST_CHECK(st_free_locked(f.r) == f.r->st_reserved);
    MOQ_TEST_CHECK(poll_is(&f, f.loc, 5, 0));
    moq_media_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);

    unsigned wakes = g_wakes;
    receiver_ts_timers(f.r, LATE);
    receiver_st_mark_ready(f.r);
    MOQ_TEST_CHECK(g_wakes > wakes);
    MOQ_TEST_CHECK(f.r->ts_stalled == 0);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 1));
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.track == f.ts && st.kind == MOQ_MEDIA_STREAM_FIN);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.track == f.loc && st.kind == MOQ_MEDIA_STREAM_FIN);
    MOQ_TEST_CHECK(dropped(&f) == 0);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_held_keeps_slot\n");
}

/* TS Object 0/0 is queued; 1/1 and, later, 1/3 are held. 1/1's timer
 * release evicts group 0: the RECEIVER_DROP takes 1/1's sequence, so it is
 * pollable before 1/1 and while 1/3, taken in after it, is still held. */
static void test_drop_keeps_sequence(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 1, 3, false)) { fx_close(&f); return; }
    take_ts(&f, 0, 0, 10);
    receiver_ts_timers(f.r, 10 + WINDOW_US + 1);
    MOQ_TEST_CHECK(f.r->obj_tail - f.r->obj_head == 1);
    take_ts(&f, 1, 1, 2000);
    take_ts(&f, 1, 3, 2500);
    receiver_ts_timers(f.r, 2000 + WINDOW_US + 1);
    receiver_st_mark_ready(f.r);
    MOQ_TEST_CHECK(dropped(&f) == 1);
    moq_media_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_MEDIA_STREAM_RECEIVER_DROP && st.track == f.ts);
    MOQ_TEST_CHECK(st.group_id == 0 && st.last_group_id == 0);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 1, 1));
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_drop_keeps_sequence\n");
}

/* Group 2's intake releases the held 1/1 first, and the 192-octet prefix
 * strip of that release fails: 1/1 is dropped, and the Object being taken
 * in keeps the ingress sequence its own intake gave it. */
static void test_payload_failure_keeps_intake(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 64, 8, true)) { fx_close(&f); return; }
    take_ts(&f, 1, 1, 10);
    MOQ_TEST_CHECK(receiver_intake_begin(f.r));
    uint64_t seq = f.r->intake_seq;
    moq_sub_object_t so = ts_object(&f, 2, 0);
    f.a.fail_next = true;
    receiver_on_ts_object(f.r, f.ts, &so, 10 + WINDOW_US + 1);
    MOQ_TEST_CHECK(!f.a.fail_next);
    MOQ_TEST_CHECK(dropped(&f) == 1);
    MOQ_TEST_CHECK(f.r->intake_seq == seq);
    receiver_intake_end(f.r);
    receiver_ts_timers(f.r, 10 + 3 * WINDOW_US);
    bool found = false;
    for (uint32_t i = f.r->obj_head; i != f.r->obj_tail; i++) {
        const receiver_obj_entry_t *e = &f.r->objs[i % f.r->ring_cap];
        if (e->obj.group_id != 2) continue;
        found = true;
        MOQ_TEST_CHECK(e->seq == seq);
    }
    MOQ_TEST_CHECK(found);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_payload_failure_keeps_intake\n");
}

/* TS 0/1 and 0/2 are held; LOC groups 5 and 6 fill the two-Object queue
 * of a four-record lane. Group 0's END_OF_GROUP status Object makes both
 * due: 0/1 goes out by evicting group 5, but 0/2 would have to evict too
 * and the lane lacks the room, so it waits, and the status Object waits
 * behind it. Once the application polls, 0/2 and then the status Object
 * go out, in that order. */
static void test_status_after_due(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 2, 4, false)) { fx_close(&f); return; }
    take_ts(&f, 0, 1, 10);
    take_ts(&f, 0, 2, 10);
    take_loc(&f, 5, 0);
    take_loc(&f, 6, 0);
    take_ts_status(&f, 0, 3, MOQ_OBJECT_END_OF_GROUP, LATE);
    MOQ_TEST_CHECK(f.r->ts_stalled == 1);
    MOQ_TEST_CHECK(dropped(&f) == 1);
    MOQ_TEST_CHECK(poll_is(&f, f.loc, 6, 0));
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 1));
    receiver_ts_timers(f.r, LATE);
    MOQ_TEST_CHECK(f.r->ts_stalled == 0);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 2));
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 3));
    MOQ_TEST_CHECK(dropped(&f) == 1);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_status_after_due\n");
}

/* As above, then the application's polls return the room before the
 * window has resumed: intake still waits, so group 1's status cannot
 * replace the parked one. Resuming the window first, as the hook does,
 * releases 0/2 and the parked status; group 1's status is then taken in
 * after them: each arrives once, in order, and every slot returns. */
static void test_parked_status_kept(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 2, 4, false)) { fx_close(&f); return; }
    take_ts(&f, 0, 1, 10);
    take_ts(&f, 0, 2, 10);
    take_loc(&f, 5, 0);
    take_loc(&f, 6, 0);
    take_ts_status(&f, 0, 3, MOQ_OBJECT_END_OF_GROUP, LATE);
    MOQ_TEST_CHECK(f.r->ts_stalled == 1);
    MOQ_TEST_CHECK(poll_is(&f, f.loc, 6, 0));
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 1));
    pthread_mutex_lock(&f.r->mu);
    MOQ_TEST_CHECK(st_room_locked(f.r));
    pthread_mutex_unlock(&f.r->mu);
    MOQ_TEST_CHECK(!receiver_intake_begin(f.r));
    receiver_ts_resume(f.r, LATE);
    MOQ_TEST_CHECK(f.r->ts_stalled == 0);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 2));
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 3));
    take_ts_status(&f, 1, 0, MOQ_OBJECT_END_OF_GROUP, LATE + 1);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 1, 0));
    MOQ_TEST_CHECK(f.r->obj_tail == f.r->obj_head);
    MOQ_TEST_CHECK(f.r->st_reserved == 0);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_parked_status_kept\n");
}

/* As above, but the status Object is an END_OF_TRACK at 0/2's Location,
 * which ends the track at once after everything the window holds. Its
 * release of 0/2 waits for room, so the END_OF_TRACK record and
 * TRACK_ENDED wait for it, and follow it once the application polls. */
static void test_end_waits_for_release(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, 2, 4, false)) { fx_close(&f); return; }
    take_ts(&f, 0, 1, 10);
    take_ts(&f, 0, 2, 10);
    take_loc(&f, 5, 0);
    take_loc(&f, 6, 0);
    take_ts_status(&f, 0, 2, MOQ_OBJECT_END_OF_TRACK, LATE);
    MOQ_TEST_CHECK(f.r->ts_stalled == 1);
    MOQ_TEST_CHECK(!f.ts->ended);
    MOQ_TEST_CHECK(poll_is(&f, f.loc, 6, 0));
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 1));
    MOQ_TEST_CHECK(!receiver_intake_begin(f.r));
    moq_media_stream_terminal_t st;
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_MEDIA_STREAM_RECEIVER_DROP && st.track == f.loc);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    receiver_ts_timers(f.r, LATE);
    receiver_st_mark_ready(f.r);
    MOQ_TEST_CHECK(f.ts->ended);
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_DONE);
    MOQ_TEST_CHECK(poll_is(&f, f.ts, 0, 2));
    MOQ_TEST_CHECK(poll_st(&f, &st) == MOQ_OK);
    MOQ_TEST_CHECK(st.kind == MOQ_MEDIA_STREAM_END_OF_TRACK && st.track == f.ts);
    MOQ_TEST_CHECK(st.group_id == 0 && st.object_id == 2);
    fx_close(&f);
    if (failures == before) printf("PASS: ts_lane_end_waits_for_release\n");
}

int main(void)
{
    test_release_waits_for_room();
    test_fin_waits_for_parked();
    test_fit_needs_no_room();
    test_rejected_returns_slot();
    test_own_drop_takes_slot();
    test_held_keeps_slot();
    test_drop_keeps_sequence();
    test_payload_failure_keeps_intake();
    test_status_after_due();
    test_parked_status_kept();
    test_end_waits_for_release();
    if (failures) {
        printf("FAIL: %d TS lane failures\n", failures);
        return 1;
    }
    printf("PASS: all TS lane tests\n");
    return 0;
}
