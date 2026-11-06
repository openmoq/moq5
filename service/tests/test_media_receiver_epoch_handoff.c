/*
 * A queued object reports the same times whether its buffers crossed to the
 * application as they were or as the receiver's copies. Under SHARED_EPOCH
 * every time is reported against one receiver-wide anchor (the first parsed
 * object's decode time) and saturates at zero before it; RAW reports them as
 * parsed. A shared payload or shared properties are copied inside the
 * object's reservation and the object reparsed from the copies: the rebase
 * applies once, to that final record.
 *
 * A payload that is exclusive but from an allocator the receiver does not
 * trust is copied too, with the same times.
 *
 * And a parked MPEG-TS status Object whose properties are shared keeps the
 * receiver's own copy: destroying the receiver on another thread, while the
 * properties' owner releases its reference, never touches the owner's buffer.
 *
 * White-box: the real intake and window (media_receiver.c is included), fed
 * directly with Objects whose payload and properties the test owns.
 */
#define MOQ_MEDIA_RECEIVER_TESTING 1
#include "../src/media_receiver.c"
#include <moq/loc.h>
#include "test_support.h"
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
#include "ts_stream.h"
#endif

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int failures;

#define CHECKED(call) do { int ck_e_ = (call); if (ck_e_ != 0) { \
    fprintf(stderr, "FATAL: %s:%d: %s = %d\n", __FILE__, __LINE__, #call, ck_e_); \
    abort(); } } while (0)

/* -- Allocators: the receiver's (thread-safe, counting), and an owner's
 *    that records which thread frees its one watched block ------------- */

typedef struct {
    pthread_mutex_t mu;
    int64_t         balance;
    void           *watch;
    int             freed;
    pthread_t       freed_by;
} rec_alloc_t;

static void *rec_alloc_fn(size_t n, void *ctx)
{
    rec_alloc_t *a = ctx;
    void *p = malloc(n);
    CHECKED(pthread_mutex_lock(&a->mu));
    if (p) a->balance++;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return p;
}
static void *rec_realloc_fn(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    return realloc(p, n);
}
static void rec_free_fn(void *p, size_t n, void *ctx)
{
    rec_alloc_t *a = ctx;
    (void)n;
    if (!p) return;
    CHECKED(pthread_mutex_lock(&a->mu));
    if (a->watch == p) {
        a->freed++;
        a->freed_by = pthread_self();
        a->watch = NULL;
    }
    a->balance--;
    CHECKED(pthread_mutex_unlock(&a->mu));
    free(p);
}
static void rec_init(rec_alloc_t *a)
{
    memset(a, 0, sizeof(*a));
    CHECKED(pthread_mutex_init(&a->mu, NULL));
}
static moq_alloc_t rec_allocator(rec_alloc_t *a)
{
    return (moq_alloc_t){ a, rec_alloc_fn, rec_realloc_fn, rec_free_fn };
}
static int64_t rec_balance(rec_alloc_t *a)
{
    CHECKED(pthread_mutex_lock(&a->mu));
    int64_t b = a->balance;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return b;
}

/* -- The fixture: a receiver with a LOC track (and a TS track), driven
 *    through its own intake ------------------------------------------- */

static const char catalog_json[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
    "\"mimeType\":\"video/mp2t\",\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":188,\"mpeg2tsProgramNumber\":1},"
    "{\"name\":\"loc\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";

static char gen_ts, gen_loc, gen_catalog;
#define SUB_TS      ((moq_sub_track_t *)(void *)&gen_ts)
#define SUB_LOC     ((moq_sub_track_t *)(void *)&gen_loc)
#define SUB_CATALOG ((moq_sub_track_t *)(void *)&gen_catalog)
#define WINDOW_US   1000u
#define LATE        (10 + 2 * WINDOW_US)

typedef struct {
    rec_alloc_t           ra;
    moq_alloc_t           alloc;
    moq_media_receiver_t *r;
    moq_media_track_t    *ts, *loc;
} fx_t;

/* `lane` 0: no stream-terminal lane, a 64-Object queue. Otherwise a lane of
 * `lane` records and a 2-Object queue (the TS window can then wait). */
static bool fx_open(fx_t *f, moq_media_time_mode_t mode, uint32_t lane)
{
    memset(f, 0, sizeof(*f));
    rec_init(&f->ra);
    f->alloc = rec_allocator(&f->ra);
    moq_media_receiver_cfg_t cfg;
    moq_media_receiver_cfg_init_live_sized(&cfg, sizeof(cfg));
    static moq_bytes_t ns[] = { { (const uint8_t *)"epoch", 5 } };
    cfg.namespace_ = (moq_namespace_t){ ns, 1 };
    cfg.auto_subscribe = true;
    cfg.time_mode = mode;
    cfg.overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    cfg.overflow.max_objects = lane ? 2 : 64;
    cfg.mpeg2ts_reorder_window_us = WINDOW_US;
    cfg.companion_profile = MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01;
    if (lane) {
        cfg.surface_stream_terminals = true;
        cfg.max_stream_terminals = lane;
    }
    moq_media_receiver_test_set_alloc(&f->alloc);
    f->r = moq_media_receiver_test_new_cfg(&cfg);
    moq_media_receiver_test_set_alloc(NULL);
    MOQ_TEST_CHECK(f->r != NULL);
    if (!f->r) return false;
    f->r->transport_version = MOQ_VERSION_DRAFT_18;
    moq_media_receiver_test_ingest(f->r, 0, 0, catalog_json, strlen(catalog_json));
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
    return true;
}

static void fx_close(fx_t *f)
{
    moq_media_object_t o;
    moq_media_track_event_t ev;
    if (f->r) {
        while (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) == MOQ_OK)
            moq_media_object_cleanup(&o);
        while (moq_media_receiver_poll_track(f->r, &ev, sizeof(ev)) == MOQ_OK) {}
        moq_media_receiver_test_free(f->r);
        f->r = NULL;
    }
    MOQ_TEST_CHECK(rec_balance(&f->ra) == 0);
    CHECKED(pthread_mutex_destroy(&f->ra.mu));
}

/* A LOC Object of group g whose LOC timestamp is ts_us, its payload and
 * properties from the receiver's allocator; *keep_payload / *keep_props, when
 * given, receive a second reference the caller then holds. */
static void take_loc(fx_t *f, uint64_t g, uint64_t ts_us,
                     moq_rcbuf_t **keep_payload, moq_rcbuf_t **keep_props)
{
    static const uint8_t body[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_LOC;
    so.group_id = g;
    so.object_id = 0;
    so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f->r->alloc, body, sizeof(body), &so.payload) == MOQ_OK);
    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    h.timestamp = ts_us;
    MOQ_TEST_CHECK(moq_loc_encode(&f->r->alloc, MOQ_VERSION_DRAFT_18, MOQ_LOC_PROFILE_01,
                                  &h, &so.properties) == MOQ_OK);
    if (keep_payload) *keep_payload = moq_rcbuf_incref(so.payload);
    if (keep_props) *keep_props = moq_rcbuf_incref(so.properties);
    MOQ_TEST_CHECK(receiver_intake_begin(f->r));
    receiver_on_media_object(f->r, f->loc, &so, 0);
    receiver_intake_end(f->r);
}

typedef struct { uint64_t decode, presentation, capture; bool has_capture, body_ok; } times_t;

/* The same semantic record: every field compared by value (the struct has
 * padding, so it is never compared as bytes). */
static bool times_equal(const times_t *a, const times_t *b)
{
    return a->decode == b->decode && a->presentation == b->presentation &&
           a->capture == b->capture && a->has_capture == b->has_capture &&
           a->body_ok == b->body_ok;
}

static bool poll_times(fx_t *f, uint64_t g, times_t *t)
{
    moq_media_object_t o;
    if (moq_media_receiver_poll_object(f->r, &o, sizeof(o)) != MOQ_OK) return false;
    bool ok = o.track == f->loc && o.group_id == g;
    t->decode = o.decode_time_us;
    t->presentation = o.presentation_time_us;
    t->capture = o.capture_time_us;
    t->has_capture = o.has_capture_time;
    t->body_ok = o.payload.len == 8 && o.payload.data[0] == 1 && o.payload.data[7] == 8;
    moq_media_object_cleanup(&o);
    return ok;
}

enum { BR_UNIQUE, BR_PAYLOAD, BR_PROPS, BR_BOTH, BR_COUNT };
static const char *const br_name[] = { "unique", "payload_copy", "properties_copy", "both_copied" };

/* One Object at ts_us through branch b; returns its reported times. */
static times_t through_branch(fx_t *f, uint64_t g, uint64_t ts_us, int b)
{
    moq_rcbuf_t *kp = NULL, *kq = NULL;
    uint64_t copies = g_handoff_copies;
    take_loc(f, g, ts_us, (b == BR_PAYLOAD || b == BR_BOTH) ? &kp : NULL,
             (b == BR_PROPS || b == BR_BOTH) ? &kq : NULL);
    uint64_t want = (uint64_t)((b == BR_PAYLOAD || b == BR_BOTH) + (b == BR_PROPS || b == BR_BOTH));
    MOQ_TEST_CHECK(g_handoff_copies - copies == want);
    /* The receiver kept a copy of what was shared, nothing of the caller's. */
    if (kp) { MOQ_TEST_CHECK(moq_rcbuf_refcount(kp) == 1); moq_rcbuf_decref(kp); }
    if (kq) { MOQ_TEST_CHECK(moq_rcbuf_refcount(kq) == 1); moq_rcbuf_decref(kq); }
    times_t t;
    memset(&t, 0, sizeof(t));
    MOQ_TEST_CHECK(poll_times(f, g, &t));
    MOQ_TEST_CHECK(t.body_ok && t.has_capture);
    return t;
}

static void expect_times(const times_t *t, uint64_t want, const char *what, int b)
{
    bool ok = t->decode == want && t->presentation == want && t->capture == want;
    if (!ok)
        fprintf(stderr, "  %s/%s: decode %llu presentation %llu capture %llu, wanted %llu\n",
                what, br_name[b], (unsigned long long)t->decode,
                (unsigned long long)t->presentation, (unsigned long long)t->capture,
                (unsigned long long)want);
    MOQ_TEST_CHECK(ok);
}

#define EPOCH 1000000u

/* SHARED_EPOCH: the anchor is the first Object's time (1,000,000 us). A later
 * Object at 1,010,000 reports 10,000 and one at 999,000 reports 0, through
 * every branch, and each branch reports exactly what the unique branch does. */
static void test_shared_epoch_branches(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, MOQ_MEDIA_TIME_SHARED_EPOCH, 0)) { fx_close(&f); return; }
    times_t first = through_branch(&f, 1, EPOCH, BR_UNIQUE);
    expect_times(&first, 0, "anchor", BR_UNIQUE);
    times_t later[BR_COUNT], early[BR_COUNT];
    uint64_t g = 2;
    for (int b = 0; b < BR_COUNT; b++) {
        later[b] = through_branch(&f, g++, EPOCH + 10000, b);
        expect_times(&later[b], 10000, "later", b);
        early[b] = through_branch(&f, g++, EPOCH - 1000, b);
        expect_times(&early[b], 0, "before_anchor", b);
    }
    for (int b = 1; b < BR_COUNT; b++) {
        MOQ_TEST_CHECK(times_equal(&later[b], &later[BR_UNIQUE]));
        MOQ_TEST_CHECK(times_equal(&early[b], &early[BR_UNIQUE]));
    }
    fx_close(&f);
    printf("ROW shared_epoch_branches: %s\n", failures == before ? "ok" : "FAILED");
}

/* RAW: the same Objects report their times as parsed, through every branch. */
static void test_raw_branches(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, MOQ_MEDIA_TIME_RAW, 0)) { fx_close(&f); return; }
    times_t first = through_branch(&f, 1, EPOCH, BR_UNIQUE);
    expect_times(&first, EPOCH, "anchor", BR_UNIQUE);
    uint64_t g = 2;
    for (int b = 0; b < BR_COUNT; b++) {
        times_t t = through_branch(&f, g++, EPOCH + 10000, b);
        expect_times(&t, EPOCH + 10000, "later", b);
        t = through_branch(&f, g++, EPOCH - 1000, b);
        expect_times(&t, EPOCH - 1000, "before_anchor", b);
    }
    fx_close(&f);
    printf("ROW raw_branches: %s\n", failures == before ? "ok" : "FAILED");
}

/* An Object whose payload is exclusive but from an allocator the receiver
 * does not trust (as any source other than its session could hand it): the
 * receiver queues its own copy, made in the reservation window, releases the
 * owner's buffer there, on the calling (network) thread, and reports the
 * same rebased times. */
static void test_untrusted_allocator_queued(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, MOQ_MEDIA_TIME_SHARED_EPOCH, 0)) { fx_close(&f); return; }
    times_t first = through_branch(&f, 1, EPOCH, BR_UNIQUE);
    expect_times(&first, 0, "anchor", BR_UNIQUE);

    rec_alloc_t oa;
    rec_init(&oa);
    moq_alloc_t oal = rec_allocator(&oa);
    static const uint8_t body[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_LOC;
    so.group_id = 2;
    so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&oal, body, sizeof(body), &so.payload) == MOQ_OK);
    CHECKED(pthread_mutex_lock(&oa.mu));
    oa.watch = so.payload;
    CHECKED(pthread_mutex_unlock(&oa.mu));
    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    h.timestamp = EPOCH + 10000;
    MOQ_TEST_CHECK(moq_loc_encode(&f.r->alloc, MOQ_VERSION_DRAFT_18, MOQ_LOC_PROFILE_01,
                                  &h, &so.properties) == MOQ_OK);
    uint64_t copies = g_handoff_copies;
    MOQ_TEST_CHECK(receiver_intake_begin(f.r));
    receiver_on_media_object(f.r, f.loc, &so, 0);
    receiver_intake_end(f.r);
    MOQ_TEST_CHECK(g_handoff_copies - copies == 1);
    CHECKED(pthread_mutex_lock(&oa.mu));
    bool released_here = oa.freed == 1 && pthread_equal(oa.freed_by, pthread_self());
    CHECKED(pthread_mutex_unlock(&oa.mu));
    MOQ_TEST_CHECK(released_here);
    times_t t2;
    memset(&t2, 0, sizeof(t2));
    MOQ_TEST_CHECK(poll_times(&f, 2, &t2));
    MOQ_TEST_CHECK(t2.body_ok && t2.has_capture);
    expect_times(&t2, 10000, "untrusted", BR_PAYLOAD);
    MOQ_TEST_CHECK(rec_balance(&oa) == 0);
    CHECKED(pthread_mutex_destroy(&oa.mu));
    fx_close(&f);
    printf("ROW untrusted_allocator_queued: %s\n", failures == before ? "ok" : "FAILED");
}

#ifdef MOQ_SERVICE_HAVE_MPEG2TS
/* -- A parked TS status Object with shared properties -------------------- */

static ts_gen_t g_gen;

static void take_ts(fx_t *f, uint64_t g, uint64_t o, uint64_t now)
{
    uint8_t buf[8 * 192];
    size_t n = ts_gen_gop(&g_gen, buf, 1);
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_TS;
    so.group_id = g;
    so.object_id = o;
    so.status = MOQ_OBJECT_NORMAL;
    MOQ_TEST_CHECK(moq_rcbuf_create(&f->r->alloc, buf, n, &so.payload) == MOQ_OK);
    MOQ_TEST_CHECK(receiver_intake_begin(f->r));
    receiver_on_ts_object(f->r, f->ts, &so, now);
    receiver_intake_end(f->r);
}

/* A two-party barrier with a bounded wait. */
typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    unsigned        arrived, generation;
} barrier_t;

static void barrier_init(barrier_t *b)
{
    memset(b, 0, sizeof(*b));
    CHECKED(pthread_mutex_init(&b->mu, NULL));
    CHECKED(pthread_cond_init(&b->cv, NULL));
}
static void barrier_fini(barrier_t *b)
{
    CHECKED(pthread_cond_destroy(&b->cv));
    CHECKED(pthread_mutex_destroy(&b->mu));
}
static bool barrier_wait(barrier_t *b)
{
    CHECKED(pthread_mutex_lock(&b->mu));
    unsigned gen = b->generation;
    if (++b->arrived == 2) {
        b->arrived = 0;
        b->generation++;
        CHECKED(pthread_cond_broadcast(&b->cv));
        CHECKED(pthread_mutex_unlock(&b->mu));
        return true;
    }
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    bool ok = true;
    while (b->generation == gen) {
        int e = pthread_cond_timedwait(&b->cv, &b->mu, &dl);
        if (e == ETIMEDOUT) {
            if (b->generation == gen) { b->arrived--; ok = false; }
            break;
        }
        CHECKED(e);
    }
    CHECKED(pthread_mutex_unlock(&b->mu));
    return ok;
}

typedef struct { fx_t *f; barrier_t *b; pthread_t self; bool ok; } destroyer_t;

static void *destroy_receiver(void *arg)
{
    destroyer_t *d = arg;
    d->self = pthread_self();
    d->ok = barrier_wait(d->b);
    moq_media_receiver_test_free(d->f->r);
    return NULL;
}

/* TS 0/1 and 0/2 are held and LOC groups 5 and 6 fill the two-Object queue
 * of a four-record lane, so group 0's END_OF_GROUP status Object -- carrying
 * properties its owner still references -- parks. It parks the receiver's
 * copy. The receiver is then destroyed on another thread while the owner
 * releases its reference: the owner's buffer is released once, by the owner. */
static void test_parked_status_shared_properties(void)
{
    int before = failures;
    fx_t f;
    if (!fx_open(&f, MOQ_MEDIA_TIME_RAW, 4)) { fx_close(&f); return; }
    MOQ_TEST_CHECK(receiver_ts_start(f.r, f.ts, 0));
    ts_gen_init(&g_gen, 188, 1, false);
    take_ts(&f, 0, 1, 10);
    take_ts(&f, 0, 2, 10);
    take_loc(&f, 5, EPOCH, NULL, NULL);
    take_loc(&f, 6, EPOCH, NULL, NULL);

    rec_alloc_t oa;
    rec_init(&oa);
    moq_alloc_t oal = rec_allocator(&oa);
    moq_loc_headers_t h;
    moq_loc_headers_init(&h);
    h.has_timestamp = true;
    h.timestamp = EPOCH;
    moq_rcbuf_t *props = NULL;
    MOQ_TEST_CHECK(moq_loc_encode(&oal, MOQ_VERSION_DRAFT_18, MOQ_LOC_PROFILE_01, &h, &props) == MOQ_OK);
    CHECKED(pthread_mutex_lock(&oa.mu));
    oa.watch = props;
    CHECKED(pthread_mutex_unlock(&oa.mu));

    uint64_t copies = g_handoff_copies;
    moq_sub_object_t so;
    memset(&so, 0, sizeof(so));
    so.track = SUB_TS;
    so.group_id = 0;
    so.object_id = 3;
    so.status = MOQ_OBJECT_END_OF_GROUP;
    so.properties = moq_rcbuf_incref(props);
    MOQ_TEST_CHECK(receiver_intake_begin(f.r));
    receiver_on_ts_object(f.r, f.ts, &so, LATE);
    receiver_intake_end(f.r);
    receiver_ts_t *ts = (receiver_ts_t *)f.ts->ts;
    MOQ_TEST_CHECK(f.r->ts_stalled == 1 && ts && ts->has_status);
    MOQ_TEST_CHECK(g_handoff_copies - copies == 1);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(props) == 1);       /* only the owner's */
    if (ts && ts->has_status)
        MOQ_TEST_CHECK(ts->status.properties != props);

    barrier_t bar;
    barrier_init(&bar);
    destroyer_t d = { &f, &bar, pthread_self(), false };
    pthread_t th;
    CHECKED(pthread_create(&th, NULL, destroy_receiver, &d));
    bool ok = barrier_wait(&bar);
    moq_rcbuf_decref(props);                 /* concurrently with the destroy */
    CHECKED(pthread_join(th, NULL));
    f.r = NULL;
    MOQ_TEST_CHECK(ok && d.ok);
    barrier_fini(&bar);

    CHECKED(pthread_mutex_lock(&oa.mu));
    int freed = oa.freed;
    bool by_owner = freed == 1 && pthread_equal(oa.freed_by, pthread_self());
    CHECKED(pthread_mutex_unlock(&oa.mu));
    MOQ_TEST_CHECK(freed == 1 && by_owner);
    MOQ_TEST_CHECK(!pthread_equal(d.self, pthread_self()));
    MOQ_TEST_CHECK(rec_balance(&oa) == 0);
    CHECKED(pthread_mutex_destroy(&oa.mu));
    fx_close(&f);
    printf("ROW parked_status_shared_properties: %s\n", failures == before ? "ok" : "FAILED");
}
#endif

int main(void)
{
    test_shared_epoch_branches();
    test_raw_branches();
    test_untrusted_allocator_queued();
#ifdef MOQ_SERVICE_HAVE_MPEG2TS
    test_parked_status_shared_properties();
    printf("mpeg2ts rows: built and run\n");
#else
    printf("mpeg2ts rows: not built (no moq::mpeg2ts)\n");
#endif
    if (failures) {
        fprintf(stderr, "test_media_receiver_epoch_handoff: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_media_receiver_epoch_handoff: all passed\n");
    return 0;
}
