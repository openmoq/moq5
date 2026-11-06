/*
 * Every buffer the media receiver keeps for the application may be released
 * on the application's thread, or on whichever thread destroys the receiver.
 * It is kept as is only when the network side proves the move exclusive: the
 * buffer is the only reference to everything it keeps alive, keeps no data
 * beyond its own bytes, and is rcbuf-owned storage released through an
 * allocator valid on the destination thread. Otherwise the receiver keeps an
 * independent copy, made only once the object is admitted, and releases the
 * shared buffer on the network thread.
 *
 * A REAL receiver is paired with a scripted draft-16 peer (no transport). Each
 * object's header is fed as bytes and its payload as an rcbuf the test
 * creates, under the ownership condition the row names; the allocator records
 * when, and on which thread, each input root is freed.
 *
 * Rows: exclusive transfer; caller-retained input (copy); input from an
 * allocator other than the session's (the session copies, that copy
 * crosses); wrapped input (the session copies, that copy crosses); an application object held while production
 * continues; the handoff copy refused (the object is dropped); a slice of a
 * root, larger or not (the session copies, so the charge is the retained
 * data); no copy for a track disabled before the object arrives or while it
 * is in flight, or for an object over the byte ceiling; an unsubscribe in
 * the copy window (the reservation is rolled back, nothing is queued or left
 * charged); a duplicate catalog Object is never copied, so a
 * refused copy cannot fail the bootstrap. With MPEG-TS built: an Object held
 * in the reorder window while the receiver is destroyed on another thread,
 * for a caller-retained input released concurrently, an input from another
 * allocator, and an exclusive input.
 *
 * --concurrent: an application thread polls and releases 48 objects while the
 * network side produces them, alternating exclusive and caller-retained
 * inputs; for each retained one both threads reach their release only after
 * a common barrier, so a shared buffer would be released concurrently. Under
 * ThreadSanitizer it must report nothing.
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include <moq/loc.h>
#include <moq/sim.h>
#include <moq/session.h>
#include <moq/control.h>
#include <moq/codec.h>
#include "test_support.h"
#ifdef MOQ_TEST_HAVE_MPEG2TS
#include "ts_stream.h"
#endif

#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static int failures = 0;

/* Every row ends with its own line, ok or FAILED, so a transcript shows each
 * row ran to its end. */
static void row_done(const char *name, const char *suffix, int before)
{
    printf("ROW %s%s: %s\n", name, suffix, failures == before ? "ok" : "FAILED");
}

/* -- Test seams (media_receiver.c, MOQ_MEDIA_RECEIVER_TESTING) ----------- */
moq_media_receiver_t *moq_media_receiver_test_new_cfg(const moq_media_receiver_cfg_t *cfg);
void moq_media_receiver_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_destroy_with_session(moq_media_receiver_t *r,
                                                  moq_session_t *session, uint64_t now_us);
void moq_media_receiver_test_pump(moq_media_receiver_t *r, moq_session_t *session,
                                  uint64_t now_us);
uint64_t moq_media_receiver_test_handoff_copies(void);
void moq_media_receiver_test_set_handoff_hook(void (*fn)(void *, int), void *ctx);
uint64_t moq_media_receiver_test_stage_catalog(moq_media_receiver_t *r, uint64_t group,
                                               uint64_t object, moq_rcbuf_t *payload);
bool moq_media_receiver_test_is_fatal(const moq_media_receiver_t *r);

/* A failed pthread call ends the run: nothing after it is proved. */
#define CHECKED(call) do { int ck_e_ = (call); if (ck_e_ != 0) { \
    fprintf(stderr, "FATAL: %s:%d: %s = %d\n", __FILE__, __LINE__, #call, ck_e_); \
    abort(); } } while (0)

/* -- A thread-safe allocator that records the frees of watched blocks ----- */

#define HO_WATCH 64
typedef struct {
    pthread_mutex_t mu;
    int64_t         balance;
    void           *watch[HO_WATCH];
    int             freed[HO_WATCH];
    pthread_t       freed_by[HO_WATCH];
    size_t          refuse_size;
    int             refused;
} ho_alloc_t;

static void *ho_alloc_fn(size_t n, void *ctx)
{
    ho_alloc_t *a = ctx;
    CHECKED(pthread_mutex_lock(&a->mu));
    if (a->refuse_size && n == a->refuse_size) {
        a->refuse_size = 0;
        a->refused++;
        CHECKED(pthread_mutex_unlock(&a->mu));
        return NULL;
    }
    void *p = malloc(n);
    if (p) a->balance++;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return p;
}
static void *ho_realloc_fn(void *p, size_t o, size_t n, void *ctx)
{
    (void)o; (void)ctx;
    return realloc(p, n);
}
static void ho_free_fn(void *p, size_t n, void *ctx)
{
    ho_alloc_t *a = ctx;
    (void)n;
    if (!p) return;
    CHECKED(pthread_mutex_lock(&a->mu));
    for (int i = 0; i < HO_WATCH; i++)
        if (a->watch[i] == p) {
            a->freed[i]++;
            a->freed_by[i] = pthread_self();
            a->watch[i] = NULL;
        }
    a->balance--;
    CHECKED(pthread_mutex_unlock(&a->mu));
    free(p);
}
static void ho_init(ho_alloc_t *a)
{
    memset(a, 0, sizeof(*a));
    CHECKED(pthread_mutex_init(&a->mu, NULL));
}
static void ho_fini(ho_alloc_t *a) { CHECKED(pthread_mutex_destroy(&a->mu)); }
static moq_alloc_t ho_allocator(ho_alloc_t *a)
{
    return (moq_alloc_t){ a, ho_alloc_fn, ho_realloc_fn, ho_free_fn };
}
static int ho_freed(ho_alloc_t *a, int w, pthread_t *by)
{
    CHECKED(pthread_mutex_lock(&a->mu));
    int f = a->freed[w];
    if (by) *by = a->freed_by[w];
    CHECKED(pthread_mutex_unlock(&a->mu));
    return f;
}
static int64_t ho_balance(ho_alloc_t *a)
{
    CHECKED(pthread_mutex_lock(&a->mu));
    int64_t b = a->balance;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return b;
}
static void ho_refuse(ho_alloc_t *a, size_t n)
{
    CHECKED(pthread_mutex_lock(&a->mu));
    a->refuse_size = n;
    CHECKED(pthread_mutex_unlock(&a->mu));
}
static int ho_refused(ho_alloc_t *a)
{
    CHECKED(pthread_mutex_lock(&a->mu));
    int n = a->refused;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return n;
}

/* A created input root over `n` bytes, watched in slot w. */
static moq_rcbuf_t *ho_root(ho_alloc_t *a, const moq_alloc_t *al, const uint8_t *src,
                            size_t n, int w)
{
    moq_rcbuf_t *b = NULL;
    if (moq_rcbuf_create(al, src, n, &b) != MOQ_OK) abort();
    CHECKED(pthread_mutex_lock(&a->mu));
    a->watch[w] = b;
    a->freed[w] = 0;
    CHECKED(pthread_mutex_unlock(&a->mu));
    return b;
}

/* -- A two-party barrier with a bounded wait. Each side's work before it
 *    happens before the other side's work after it; nothing orders the two
 *    sides' work after it. ------------------------------------------------ */

typedef struct {
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    unsigned        arrived, generation;
} ho_barrier_t;

static void barrier_init(ho_barrier_t *b)
{
    memset(b, 0, sizeof(*b));
    CHECKED(pthread_mutex_init(&b->mu, NULL));
    CHECKED(pthread_cond_init(&b->cv, NULL));
}
static void barrier_fini(ho_barrier_t *b)
{
    CHECKED(pthread_cond_destroy(&b->cv));
    CHECKED(pthread_mutex_destroy(&b->mu));
}
/* False when the other side did not arrive within 10 s. */
static bool barrier_wait(ho_barrier_t *b)
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

/* -- Scripted draft-16 peer (as test_media_receiver_largeobj.c) ----------- */

static size_t put_varint(uint8_t *buf, uint64_t v)
{
    if (v < 0x40) { buf[0] = (uint8_t)v; return 1; }
    if (v < 0x4000) { buf[0] = (uint8_t)(0x40 | (v >> 8)); buf[1] = (uint8_t)v; return 2; }
    buf[0] = (uint8_t)(0x80 | (v >> 24)); buf[1] = (uint8_t)(v >> 16);
    buf[2] = (uint8_t)(v >> 8); buf[3] = (uint8_t)v;
    return 4;
}

static size_t build_subscribe_ok(uint8_t *out, size_t cap, uint64_t rid, uint64_t alias)
{
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, out, cap);
    if (moq_d16_encode_subscribe_ok(&w, rid, alias, NULL, 0, NULL, 0) < 0) return 0;
    return moq_buf_writer_offset(&w);
}

typedef struct { bool have_cat, have_vid; uint64_t cat_rid, vid_rid; } learned_t;

static void drain_and_learn(moq_session_t *client, learned_t *l)
{
    moq_action_t acts[16];
    size_t n;
    while ((n = moq_session_poll_actions(client, acts, 16)) > 0) {
        for (size_t i = 0; i < n; i++) {
            if (acts[i].kind == MOQ_ACTION_SEND_CONTROL) {
                moq_control_envelope_t env;
                moq_buf_reader_t r;
                moq_buf_reader_init(&r, acts[i].u.send_control.data,
                                    acts[i].u.send_control.len);
                if (moq_control_decode_envelope(&r, &env) >= 0 &&
                    env.msg_type == MOQ_D16_SUBSCRIBE) {
                    moq_bytes_t parts[8];
                    moq_kvp_entry_t params[16];
                    moq_d16_subscribe_t s;
                    memset(&s, 0, sizeof(s));
                    s.params = params;
                    s.params_cap = 16;
                    if (moq_d16_decode_subscribe(env.payload, env.payload_len, parts, 8, &s) >= 0) {
                        if (!l->have_cat) { l->have_cat = true; l->cat_rid = s.request_id; }
                        else if (!l->have_vid) { l->have_vid = true; l->vid_rid = s.request_id; }
                    }
                }
            }
            moq_action_cleanup(&acts[i]);
        }
    }
}

static const char CATALOG_LOC[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"video\",\"packaging\":\"loc\",\"isLive\":true,"
    "\"role\":\"video\",\"codec\":\"avc1.42e01e\"}]}";
#ifdef MOQ_TEST_HAVE_MPEG2TS
static const char CATALOG_TS[] =
    "{\"version\":1,\"tracks\":["
    "{\"name\":\"ts\",\"packaging\":\"mpeg2ts\",\"isLive\":true,"
    "\"bitrate\":4000000,\"mpeg2tsMode\":\"unmodified-program\","
    "\"mpeg2tsPacketSize\":188}]}";
#endif

static const moq_bytes_t g_ns_parts[] = {
    { (const uint8_t *)"media", 5 }, { (const uint8_t *)"cat", 3 },
};

static void receiver_cfg(moq_media_receiver_cfg_t *rcfg, uint64_t max_bytes, bool ts)
{
    moq_media_receiver_cfg_init_live_sized(rcfg, sizeof(*rcfg));
    rcfg->namespace_.parts = g_ns_parts;
    rcfg->namespace_.count = 2;
    rcfg->auto_subscribe = true;
    rcfg->time_mode = MOQ_MEDIA_TIME_RAW;
    rcfg->overflow.policy = MOQ_MEDIA_OVERFLOW_DROP_GROUP;
    rcfg->overflow.max_objects = 64;
    rcfg->overflow.max_bytes = max_bytes;
    if (ts) {
        rcfg->mpeg2ts_reorder_window_us = 5u * 1000u * 1000u;   /* never by deadline here */
        rcfg->mpeg2ts_reorder_slots = 8;
    }
}

typedef struct {
    ho_alloc_t            ha;
    moq_alloc_t           alloc;
    moq_simpair_t        *sp;
    moq_session_t        *client;
    moq_media_receiver_t *r;
    moq_media_track_t    *track;
    uint64_t              now;
    learned_t             l;
} ho_rig_t;

/* The network side's turn: the receiver's pump and the session's output.
 * Called only from the thread that owns the session. */
static void rig_pump_net(ho_rig_t *g, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        g->now += 1000;
        moq_media_receiver_test_pump(g->r, g->client, g->now);
        drain_and_learn(g->client, &g->l);
        (void)moq_session_process_pending(g->client, g->now);
    }
}

/* Both sides on one thread: the network's turn, then the track events. */
static void rig_pump(ho_rig_t *g, int cycles)
{
    for (int i = 0; i < cycles; i++) {
        rig_pump_net(g, 1);
        moq_media_track_event_t te;
        while (moq_media_receiver_poll_track(g->r, &te, sizeof(te)) == MOQ_OK)
            if (te.kind == MOQ_MEDIA_TRACK_ADDED && !g->track) g->track = te.track;
    }
}

static bool rig_open(ho_rig_t *g, const char *catalog, uint64_t max_bytes, bool ts)
{
    memset(g, 0, sizeof(*g));
    ho_init(&g->ha);
    g->alloc = ho_allocator(&g->ha);
    moq_simpair_cfg_t cfg = MOQ_SIMPAIR_CFG_INIT;
    cfg.alloc = &g->alloc;
    cfg.seed = 42;
    cfg.initial_now_us = 1000;
    cfg.server_send_request_capacity = true;
    cfg.server_initial_request_capacity = 16;
    cfg.client_send_request_capacity = true;
    cfg.client_initial_request_capacity = 16;
    if (moq_simpair_create(&cfg, &g->sp) != MOQ_OK) return false;
    moq_simpair_start(g->sp);
    moq_simpair_run_until_quiescent(g->sp, 16, NULL);
    g->client = moq_simpair_client(g->sp);
    moq_event_t ev;
    while (moq_session_poll_events(g->client, &ev, 1) == 1) moq_event_cleanup(&ev);
    moq_session_t *server = moq_simpair_server(g->sp);
    while (moq_session_poll_events(server, &ev, 1) == 1) moq_event_cleanup(&ev);

    moq_media_receiver_cfg_t rcfg;
    receiver_cfg(&rcfg, max_bytes, ts);
    moq_media_receiver_test_set_alloc(&g->alloc);
    g->r = moq_media_receiver_test_new_cfg(&rcfg);
    moq_media_receiver_test_set_alloc(NULL);
    if (!g->r) return false;

    uint8_t ctrl[256];
    bool cat_ok = false, cat_data = false, vid_ok = false;
    g->now = 1000;
    for (int cycle = 0; cycle < 64 && !vid_ok; cycle++) {
        rig_pump(g, 1);
        if (g->l.have_cat && !cat_ok) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), g->l.cat_rid, 0);
            if (!n || moq_session_on_control_bytes(g->client, ctrl, n, g->now) < 0) return false;
            cat_ok = true;
        }
        if (cat_ok && !cat_data) {
            uint8_t buf[512];
            size_t o = 0;
            buf[o++] = 0x30;
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            o += put_varint(buf + o, 0);
            size_t jlen = strlen(catalog);
            o += put_varint(buf + o, jlen);
            memcpy(buf + o, catalog, jlen);
            o += jlen;
            if (moq_session_on_data_bytes(g->client, moq_stream_ref_from_u64(101), buf, o,
                                          true, g->now) < 0) return false;
            cat_data = true;
        }
        if (g->track && g->l.have_vid && !vid_ok) {
            size_t n = build_subscribe_ok(ctrl, sizeof(ctrl), g->l.vid_rid, 2);
            if (!n || moq_session_on_control_bytes(g->client, ctrl, n, g->now) < 0) return false;
            vid_ok = true;
        }
    }
    rig_pump(g, 4);
    return vid_ok && g->track;
}

static void rig_close(ho_rig_t *g)
{
    if (g->r) moq_media_receiver_test_destroy_with_session(g->r, g->client, g->now);
    g->r = NULL;
    moq_simpair_destroy(g->sp);
    MOQ_TEST_CHECK(ho_balance(&g->ha) == 0);
    ho_fini(&g->ha);
}

static uint8_t pat(size_t obj, size_t i) { return (uint8_t)((i * 131u + 7u + obj * 17u) & 0xffu); }

/* Object 0 of `group` on its own subgroup stream: the header as bytes, then
 * `payload` as an rcbuf, then the FIN. */
static void feed_group(ho_rig_t *g, uint64_t group, moq_rcbuf_t *payload)
{
    uint8_t header[32];
    size_t hdr = 0;
    header[hdr++] = 0x30;
    hdr += put_varint(header + hdr, 2);            /* track alias */
    hdr += put_varint(header + hdr, group);
    hdr += put_varint(header + hdr, 0);            /* object id delta */
    hdr += put_varint(header + hdr, moq_rcbuf_len(payload));
    moq_stream_ref_t ref = moq_stream_ref_from_u64(1000 + group);
    MOQ_TEST_CHECK(moq_session_on_data_bytes(g->client, ref, header, hdr, false, g->now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_data_rcbuf(g->client, ref, payload, false, g->now) == MOQ_OK);
    MOQ_TEST_CHECK(moq_session_on_data_bytes(g->client, ref, NULL, 0, true, g->now) == MOQ_OK);
}
/* Video object `obj`: group obj + 2, its payload pat(obj, .). */
static void feed_object(ho_rig_t *g, size_t obj, moq_rcbuf_t *payload)
{
    feed_group(g, obj + 2, payload);
}

static bool payload_ok(const moq_media_object_t *o, size_t obj, size_t n)
{
    if (o->payload.len != n) return false;
    for (size_t i = 0; i < n; i++)
        if (o->payload.data[i] != pat(obj, i)) return false;
    return true;
}

static moq_rcbuf_t *make_input(ho_rig_t *g, const moq_alloc_t *al, size_t obj, size_t n, int w)
{
    uint8_t *b = malloc(n);
    if (!b) abort();
    for (size_t i = 0; i < n; i++) b[i] = pat(obj, i);
    moq_rcbuf_t *in = ho_root(&g->ha, al, b, n, w);
    free(b);
    return in;
}

static moq_media_receiver_stats_t stats(ho_rig_t *g)
{
    moq_media_receiver_stats_t st;
    memset(&st, 0, sizeof(st));
    MOQ_TEST_CHECK(moq_media_receiver_get_stats(g->r, &st, sizeof(st)) == MOQ_OK);
    return st;
}

/* Nothing is queued for the application. */
static bool queue_empty(ho_rig_t *g)
{
    moq_media_object_t o;
    if (moq_media_receiver_poll_object(g->r, &o, sizeof(o)) != MOQ_OK) return true;
    moq_media_object_cleanup(&o);
    return false;
}

#define N 20000
#define BIG (8u << 20)

/* -- Deterministic rows ------------------------------------------------- */

static void test_exclusive_transfer(void)
{
    int row_before = failures;
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, BIG, false));
    uint64_t copies = moq_media_receiver_test_handoff_copies();
    moq_rcbuf_t *in = make_input(&g, &g.alloc, 0, N, 0);
    const uint8_t *data = moq_rcbuf_data(in);
    feed_object(&g, 0, in);
    moq_rcbuf_decref(in);                    /* the caller lets go first */
    rig_pump(&g, 4);
    uint64_t charge = 0;
    moq_media_object_t o;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object_within(g.r, N, &charge, &o, sizeof(o)) == MOQ_OK);
    MOQ_TEST_CHECK(payload_ok(&o, 0, N));
    MOQ_TEST_CHECK(o.payload.data == data);  /* the input itself crossed */
    MOQ_TEST_CHECK(charge == N && moq_media_object_charge(&o) == N);
    MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 0);
    moq_media_object_cleanup(&o);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 1);
    rig_close(&g);
    row_done("exclusive_transfer", "", row_before);
}

static void test_caller_retained_input_is_copied(void)
{
    int row_before = failures;
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, BIG, false));
    uint64_t copies = moq_media_receiver_test_handoff_copies();
    moq_rcbuf_t *in = make_input(&g, &g.alloc, 1, N, 0);
    const uint8_t *data = moq_rcbuf_data(in);
    feed_object(&g, 1, in);
    rig_pump(&g, 4);                         /* the caller still holds `in` */
    moq_media_object_t o;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(g.r, &o, sizeof(o)) == MOQ_OK);
    MOQ_TEST_CHECK(payload_ok(&o, 1, N));
    MOQ_TEST_CHECK(o.payload.data != data);
    MOQ_TEST_CHECK(moq_media_object_charge(&o) == N);
    MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies + 1);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);   /* the receiver kept nothing */
    moq_media_object_cleanup(&o);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 0);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 1);
    rig_close(&g);
    row_done("caller_retained_input_is_copied", "", row_before);
}

/* An exact created input from an allocator other than the session's is
 * copied by the session, which keeps nothing of it: it is released at the
 * caller's decref, and the session's own copy crosses without another. */
static void test_other_allocator_session_copies(void)
{
    int row_before = failures;
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, BIG, false));
    ho_alloc_t other;
    ho_init(&other);
    moq_alloc_t oal = ho_allocator(&other);
    uint64_t copies = moq_media_receiver_test_handoff_copies();
    moq_rcbuf_t *in = make_input(&g, &oal, 2, N, 0);
    other.watch[0] = in;
    const uint8_t *data = moq_rcbuf_data(in);
    feed_object(&g, 2, in);
    moq_rcbuf_decref(in);
    pthread_t by;
    MOQ_TEST_CHECK(ho_freed(&other, 0, &by) == 1);  /* at the caller's decref */
    MOQ_TEST_CHECK(pthread_equal(by, pthread_self()));
    rig_pump(&g, 4);
    moq_media_object_t o;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(g.r, &o, sizeof(o)) == MOQ_OK);
    MOQ_TEST_CHECK(payload_ok(&o, 2, N));
    MOQ_TEST_CHECK(o.payload.data != data);
    MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies);
    moq_media_object_cleanup(&o);
    MOQ_TEST_CHECK(ho_balance(&other) == 0);
    ho_fini(&other);
    rig_close(&g);
    row_done("other_allocator_session_copies", "", row_before);
}

static int g_wrap_released;
static void wrap_release(void *ctx, const uint8_t *d, size_t n)
{
    (void)d; (void)n;
    free(ctx);
    g_wrap_released++;
}

static void test_wrapped_input_session_copies(void)
{
    int row_before = failures;
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, BIG, false));
    uint64_t copies = moq_media_receiver_test_handoff_copies();
    uint8_t *backing = malloc(4 * N);
    if (!backing) abort();
    for (size_t i = 0; i < N; i++) backing[N + i] = pat(3, i);
    moq_rcbuf_t *in = NULL;
    g_wrap_released = 0;
    MOQ_TEST_CHECK(moq_rcbuf_wrap(&g.alloc, backing + N, N, wrap_release, backing, &in) == MOQ_OK);
    feed_object(&g, 3, in);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(g_wrap_released == 1);    /* the session kept nothing of it */
    rig_pump(&g, 4);
    moq_media_object_t o;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(g.r, &o, sizeof(o)) == MOQ_OK);
    MOQ_TEST_CHECK(payload_ok(&o, 3, N));
    /* The session's own copy is exclusive and trusted: it crosses as is. */
    MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies);
    moq_media_object_cleanup(&o);
    rig_close(&g);
    row_done("wrapped_input_session_copies", "", row_before);
}

static void test_held_object_while_producer_continues(void)
{
    int row_before = failures;
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, BIG, false));
    moq_rcbuf_t *a = make_input(&g, &g.alloc, 4, N, 0);
    feed_object(&g, 4, a);
    moq_rcbuf_decref(a);
    rig_pump(&g, 4);
    moq_media_object_t held;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(g.r, &held, sizeof(held)) == MOQ_OK);
    MOQ_TEST_CHECK(payload_ok(&held, 4, N));
    for (size_t k = 5; k < 9; k++) {
        moq_rcbuf_t *b = make_input(&g, &g.alloc, k, N, (int)k);
        feed_object(&g, k, b);
        moq_rcbuf_decref(b);
        rig_pump(&g, 4);
        moq_media_object_t o;
        MOQ_TEST_CHECK(moq_media_receiver_poll_object(g.r, &o, sizeof(o)) == MOQ_OK);
        MOQ_TEST_CHECK(payload_ok(&o, k, N));
        moq_media_object_cleanup(&o);
        MOQ_TEST_CHECK(ho_freed(&g.ha, (int)k, NULL) == 1);
        MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 0);
    }
    MOQ_TEST_CHECK(payload_ok(&held, 4, N));   /* still intact */
    moq_media_object_cleanup(&held);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 1);
    rig_close(&g);
    row_done("held_object_while_producer_continues", "", row_before);
}

/* The handoff copy is refused: the object is dropped as a parse drop, its
 * reservation is returned (with a ceiling of one and a half objects, the
 * next object is still admitted), and the input is released once. */
static void test_handoff_copy_refused(void)
{
    int row_before = failures;
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, N + N / 2, false));
    moq_media_receiver_stats_t before = stats(&g);
    moq_rcbuf_t *in = make_input(&g, &g.alloc, 9, N, 0);
    feed_object(&g, 9, in);
    size_t clone_size = 0;
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(N, &clone_size) == MOQ_OK);
    ho_refuse(&g.ha, clone_size);            /* the handoff copy cannot be made */
    rig_pump(&g, 4);
    MOQ_TEST_CHECK(ho_refused(&g.ha) == 1);
    ho_refuse(&g.ha, 0);
    MOQ_TEST_CHECK(queue_empty(&g));
    moq_media_receiver_stats_t after = stats(&g);
    MOQ_TEST_CHECK(after.parse_drops == before.parse_drops + 1);
    MOQ_TEST_CHECK(after.objects_dropped == before.objects_dropped + 1);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 1);
    moq_rcbuf_t *next = make_input(&g, &g.alloc, 10, N, 1);
    feed_object(&g, 10, next);
    moq_rcbuf_decref(next);
    rig_pump(&g, 4);
    moq_media_object_t o2;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(g.r, &o2, sizeof(o2)) == MOQ_OK);
    MOQ_TEST_CHECK(payload_ok(&o2, 10, N));
    moq_media_object_cleanup(&o2);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 1, NULL) == 1);
    rig_close(&g);
    row_done("handoff_copy_refused", "", row_before);
}

/* A polled object retains exactly what its charge says. A payload that
 * arrives as a slice -- of a larger root (20000 of 30000 bytes), or covering
 * its whole root -- is copied by the session, so the root is released as
 * soon as the caller lets go, poll_object_within admits the object at its
 * payload and refuses it one byte below, and the charge the player relies on
 * is the data the object keeps. */
static void test_slice_input_charge_is_retained_data(void)
{
    int row_before = failures;
    for (int whole = 0; whole < 2; whole++) {
        ho_rig_t g;
        MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, BIG, false));
        size_t obj = 30 + (size_t)whole;
        size_t root_n = whole ? N : N + N / 2;
        uint8_t *b = malloc(root_n);
        if (!b) abort();
        for (size_t i = 0; i < N; i++) b[i] = pat(obj, i);
        memset(b + N, 0, root_n - N);
        moq_rcbuf_t *root = ho_root(&g.ha, &g.alloc, b, root_n, 0);
        free(b);
        moq_rcbuf_t *in = NULL;
        MOQ_TEST_CHECK(moq_rcbuf_slice(&g.alloc, root, 0, N, &in) == MOQ_OK);
        const uint8_t *data = moq_rcbuf_data(root);
        feed_object(&g, obj, in);
        moq_rcbuf_decref(in);
        moq_rcbuf_decref(root);
        MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 1);   /* nothing retains the root */
        rig_pump(&g, 4);
        uint64_t charge = 0;
        moq_media_object_t o;
        MOQ_TEST_CHECK(moq_media_receiver_poll_object_within(g.r, N - 1, &charge, &o, sizeof(o))
                       == MOQ_ERR_BUFFER);
        MOQ_TEST_CHECK(charge == N);
        MOQ_TEST_CHECK(moq_media_receiver_poll_object_within(g.r, N, &charge, &o, sizeof(o)) == MOQ_OK);
        MOQ_TEST_CHECK(charge == N && moq_media_object_charge(&o) == N);
        MOQ_TEST_CHECK(payload_ok(&o, obj, N));
        MOQ_TEST_CHECK(o.payload.data != data);
        moq_media_object_cleanup(&o);
        rig_close(&g);
    }
    row_done("slice_input_charge_is_retained_data", "", row_before);
}

/* The application disables the track at the named hook site (0: an object
 * about to be admitted; 1: a reserved object's copy window), once. */
typedef struct { ho_rig_t *g; int site; int fired; } race_t;
static void unsubscribe_at(void *ctx, int site)
{
    race_t *x = ctx;
    if (site != x->site || x->fired++) return;
    MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(x->g->r, x->g->track) == MOQ_OK);
}

/* No copy is made for an object the receiver will not queue: a track
 * disabled before the object arrives (dropped at intake), a track disabled
 * while the object is in flight (dropped at admission), or an object over the
 * byte ceiling. The input is caller-retained, so a queued object would have
 * needed a copy. */
static void test_no_copy_before_admission(void)
{
    int row_before = failures;
    for (int why = 0; why < 3; why++) {
        ho_rig_t g;
        MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, why == 2 ? N / 2 : BIG, false));
        if (why == 0) {
            MOQ_TEST_CHECK(moq_media_receiver_unsubscribe_track(g.r, g.track) == MOQ_OK);
            rig_pump(&g, 2);
        }
        race_t x = { &g, 0, 0 };
        if (why == 1) moq_media_receiver_test_set_handoff_hook(unsubscribe_at, &x);
        uint64_t copies = moq_media_receiver_test_handoff_copies();
        moq_media_receiver_stats_t before = stats(&g);
        moq_rcbuf_t *in = make_input(&g, &g.alloc, 40 + (size_t)why, N, 0);
        feed_object(&g, 40 + (size_t)why, in);
        rig_pump(&g, 4);
        moq_media_receiver_test_set_handoff_hook(NULL, NULL);
        if (why == 1) MOQ_TEST_CHECK(x.fired == 1);
        moq_media_receiver_stats_t after = stats(&g);
        MOQ_TEST_CHECK(after.objects_received == before.objects_received + 1);
        MOQ_TEST_CHECK(after.objects_dropped == before.objects_dropped + 1);
        MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies);
        MOQ_TEST_CHECK(queue_empty(&g));
        MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
        moq_rcbuf_decref(in);
        MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 1);
        rig_close(&g);
    }
    row_done("no_copy_before_admission", "", row_before);
}

/* An unsubscribe lands while the reserved object's copy is being made (no
 * lock held): the reservation is rolled back, nothing is queued, and the
 * copy and the input are each released once. Nothing of the reservation
 * stays charged: with a ceiling of one and a half objects, the next object
 * of the re-enabled track is still admitted. */
static void test_unsubscribe_in_copy_window(void)
{
    int row_before = failures;
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, N + N / 2, false));
    race_t x = { &g, 1, 0 };
    uint64_t copies = moq_media_receiver_test_handoff_copies();
    moq_media_receiver_stats_t before = stats(&g);
    moq_rcbuf_t *in = make_input(&g, &g.alloc, 50, N, 0);
    feed_object(&g, 50, in);
    moq_media_receiver_test_set_handoff_hook(unsubscribe_at, &x);
    rig_pump(&g, 4);
    moq_media_receiver_test_set_handoff_hook(NULL, NULL);
    MOQ_TEST_CHECK(x.fired == 1);
    MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies + 1);
    moq_media_receiver_stats_t after = stats(&g);
    MOQ_TEST_CHECK(after.objects_dropped == before.objects_dropped + 1);
    MOQ_TEST_CHECK(after.parse_drops == before.parse_drops);
    MOQ_TEST_CHECK(queue_empty(&g));
    MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
    moq_rcbuf_decref(in);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 1);

    MOQ_TEST_CHECK(moq_media_receiver_subscribe_track(g.r, g.track, NULL) == MOQ_OK);
    rig_pump(&g, 2);
    moq_rcbuf_t *next = make_input(&g, &g.alloc, 51, N, 1);
    feed_object(&g, 51, next);
    moq_rcbuf_decref(next);
    rig_pump(&g, 4);
    moq_media_object_t o;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(g.r, &o, sizeof(o)) == MOQ_OK);
    MOQ_TEST_CHECK(payload_ok(&o, 51, N));
    moq_media_object_cleanup(&o);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 1, NULL) == 1);
    rig_close(&g);
    row_done("unsubscribe_in_copy_window", "", row_before);
}

/* A catalog Object is checked for a duplicate Location before any copy is
 * made: a duplicate of a shared input is ignored without one, so a copy
 * refusal armed for it never fires and the bootstrap goes on. The same
 * refusal on a new Location does fire, and fails the bootstrap. */
static void test_catalog_duplicate_is_never_copied(void)
{
    int row_before = failures;
    ho_alloc_t ha;
    ho_init(&ha);
    moq_alloc_t al = ho_allocator(&ha);
    moq_media_receiver_cfg_t rcfg;
    receiver_cfg(&rcfg, BIG, false);
    moq_media_receiver_test_set_alloc(&al);
    moq_media_receiver_t *r = moq_media_receiver_test_new_cfg(&rcfg);
    moq_media_receiver_test_set_alloc(NULL);
    MOQ_TEST_CHECK(r != NULL);
    if (!r) { ho_fini(&ha); return; }

    const uint8_t *json = (const uint8_t *)CATALOG_LOC;
    size_t n = strlen(CATALOG_LOC);
    size_t clone_size = 0;
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(n, &clone_size) == MOQ_OK);
    uint64_t copies = moq_media_receiver_test_handoff_copies();

    /* The first Object at (7, 0): shared (the caller keeps it), so copied. */
    moq_rcbuf_t *first = ho_root(&ha, &al, json, n, 0);
    MOQ_TEST_CHECK(moq_media_receiver_test_stage_catalog(r, 7, 0, first) == 1);
    MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies + 1);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(first) == 1);
    moq_rcbuf_decref(first);
    MOQ_TEST_CHECK(ho_freed(&ha, 0, NULL) == 1);

    /* Its duplicate, with the copy refused: never attempted. */
    moq_rcbuf_t *dup = ho_root(&ha, &al, json, n, 1);
    ho_refuse(&ha, clone_size);
    MOQ_TEST_CHECK(moq_media_receiver_test_stage_catalog(r, 7, 0, dup) == 1);
    MOQ_TEST_CHECK(ho_refused(&ha) == 0);
    ho_refuse(&ha, 0);
    MOQ_TEST_CHECK(!moq_media_receiver_test_is_fatal(r));
    MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies + 1);
    MOQ_TEST_CHECK(moq_rcbuf_refcount(dup) == 1);
    moq_rcbuf_decref(dup);
    MOQ_TEST_CHECK(ho_freed(&ha, 1, NULL) == 1);

    /* A new Location with the copy refused: attempted, and fatal. */
    moq_rcbuf_t *other = ho_root(&ha, &al, json, n, 2);
    ho_refuse(&ha, clone_size);
    (void)moq_media_receiver_test_stage_catalog(r, 8, 0, other);
    MOQ_TEST_CHECK(ho_refused(&ha) == 1);
    ho_refuse(&ha, 0);
    MOQ_TEST_CHECK(moq_media_receiver_test_is_fatal(r));
    MOQ_TEST_CHECK(moq_rcbuf_refcount(other) == 1);
    moq_rcbuf_decref(other);
    MOQ_TEST_CHECK(ho_freed(&ha, 2, NULL) == 1);

    moq_media_receiver_test_free(r);
    MOQ_TEST_CHECK(ho_balance(&ha) == 0);
    ho_fini(&ha);
    row_done("catalog_duplicate_is_never_copied", "", row_before);
}

#ifdef MOQ_TEST_HAVE_MPEG2TS
/* -- MPEG-TS: an Object held in the reorder window while the receiver is
 *    destroyed on another thread ---------------------------------------- */

static ts_gen_t g_ts_gen;

static moq_rcbuf_t *ts_input(ho_alloc_t *a, const moq_alloc_t *al, int w)
{
    uint8_t buf[16 * 188];
    size_t n = ts_gen_gop(&g_ts_gen, buf, 1);
    return ho_root(a, al, buf, n, w);
}

typedef struct {
    ho_rig_t     *g;
    ho_barrier_t *b;
    pthread_t     self;
    bool          barrier_ok;
} destroyer_t;

static void *destroy_thread(void *arg)
{
    destroyer_t *d = arg;
    d->self = pthread_self();
    d->barrier_ok = barrier_wait(d->b);
    moq_media_receiver_test_destroy_with_session(d->g->r, d->g->client, d->g->now);
    return NULL;
}

/* how: 0 a caller-retained input, released by the network side concurrently
 * with the destroy (copied at the window); 1 an input from an allocator other
 * than the session's (copied by the session, released at the caller's decref);
 * 2 an exclusive input from the session's allocator (kept, released by the
 * destroying thread). */
static void test_ts_held_object_destroyed_elsewhere(int how)
{
    int row_before = failures;
    static const char *names[3] = { "retained", "other_allocator", "exclusive" };
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_TS, BIG, true));
    ho_alloc_t other;
    ho_init(&other);
    moq_alloc_t oal = ho_allocator(&other);

    /* (2, 0) starts the window and is delivered. */
    moq_rcbuf_t *start = ts_input(&g.ha, &g.alloc, 1);
    feed_group(&g, 2, start);
    moq_rcbuf_decref(start);
    rig_pump(&g, 4);
    moq_media_object_t o;
    MOQ_TEST_CHECK(moq_media_receiver_poll_object(g.r, &o, sizeof(o)) == MOQ_OK);
    MOQ_TEST_CHECK(o.packaging == MOQ_MEDIA_PACKAGING_MPEG2TS && o.group_id == 2);
    moq_media_object_cleanup(&o);
    MOQ_TEST_CHECK(ho_freed(&g.ha, 1, NULL) == 1);

    /* (4, 0) is held: Group 3 may still come. */
    uint64_t copies = moq_media_receiver_test_handoff_copies();
    moq_rcbuf_t *in = ts_input(how == 1 ? &other : &g.ha, how == 1 ? &oal : &g.alloc, 0);
    feed_group(&g, 4, in);
    if (how != 0) moq_rcbuf_decref(in);
    if (how == 1) {
        pthread_t by;
        MOQ_TEST_CHECK(ho_freed(&other, 0, &by) == 1);  /* the session kept nothing */
        MOQ_TEST_CHECK(pthread_equal(by, pthread_self()));
    }
    rig_pump(&g, 4);
    MOQ_TEST_CHECK(queue_empty(&g));
    MOQ_TEST_CHECK(moq_media_receiver_test_handoff_copies() == copies + (how == 0 ? 1 : 0));
    if (how == 0) MOQ_TEST_CHECK(moq_rcbuf_refcount(in) == 1);
    if (how == 2) MOQ_TEST_CHECK(ho_freed(&g.ha, 0, NULL) == 0);   /* the window holds it */

    ho_barrier_t bar;
    barrier_init(&bar);
    destroyer_t d = { &g, &bar, pthread_self(), false };
    pthread_t th;
    CHECKED(pthread_create(&th, NULL, destroy_thread, &d));
    bool ok = barrier_wait(&bar);
    if (how == 0) moq_rcbuf_decref(in);      /* concurrently with the destroy */
    CHECKED(pthread_join(th, NULL));
    MOQ_TEST_CHECK(ok && d.barrier_ok);
    MOQ_TEST_CHECK(!pthread_equal(d.self, pthread_self()));
    g.r = NULL;                              /* destroyed by the other thread */
    barrier_fini(&bar);

    pthread_t by;
    if (how == 0) {
        MOQ_TEST_CHECK(ho_freed(&g.ha, 0, &by) == 1);
        MOQ_TEST_CHECK(pthread_equal(by, pthread_self()));   /* only by its owner */
    } else if (how == 2) {
        MOQ_TEST_CHECK(ho_freed(&g.ha, 0, &by) == 1);
        MOQ_TEST_CHECK(pthread_equal(by, d.self));           /* moved whole */
    }
    MOQ_TEST_CHECK(ho_balance(&other) == 0);
    ho_fini(&other);
    rig_close(&g);
    row_done("ts_held_object_destroyed_elsewhere_", names[how], row_before);
}
#endif

/* -- Concurrency --------------------------------------------------------- */

#define OBJECTS 48

typedef struct {
    ho_rig_t        *g;
    ho_barrier_t     bar;
    pthread_mutex_t  mu;
    pthread_cond_t   cv;
    const uint8_t   *input_data[OBJECTS];
    int              polled;          /* under mu */
    int              bad_content, bad_identity, timeouts;
    pthread_t        self;
} app_t;

/* Poll the next object, waiting at most 10 s. */
static bool app_poll(app_t *app, moq_media_object_t *o)
{
    for (int i = 0; i < 10000; i++) {
        if (moq_media_receiver_poll_object(app->g->r, o, sizeof(*o)) == MOQ_OK) return true;
        struct timespec ts = { 0, 1000000 };
        nanosleep(&ts, NULL);
    }
    return false;
}

static void *app_thread(void *arg)
{
    app_t *app = arg;
    for (int k = 0; k < OBJECTS; k++) {
        moq_media_object_t o;
        if (!app_poll(app, &o)) { app->timeouts++; break; }
        size_t obj = (size_t)(o.group_id - 2);
        bool retained = (k % 2) == 1;
        if (obj != (size_t)(10 + k) || !payload_ok(&o, obj, N)) app->bad_content++;
        /* An exclusive input crosses itself; a retained one is copied. */
        bool same = o.payload.data == app->input_data[k];
        if (same == retained) app->bad_identity++;
        if (retained && !barrier_wait(&app->bar)) app->timeouts++;
        moq_media_object_cleanup(&o);
        CHECKED(pthread_mutex_lock(&app->mu));
        app->polled++;
        CHECKED(pthread_cond_signal(&app->cv));
        CHECKED(pthread_mutex_unlock(&app->mu));
    }
    return NULL;
}

static int test_concurrent(void)
{
    ho_rig_t g;
    MOQ_TEST_CHECK(rig_open(&g, CATALOG_LOC, BIG, false));
    app_t app;
    memset(&app, 0, sizeof(app));
    app.g = &g;
    barrier_init(&app.bar);
    CHECKED(pthread_mutex_init(&app.mu, NULL));
    CHECKED(pthread_cond_init(&app.cv, NULL));
    moq_rcbuf_t *inputs[OBJECTS];
    for (int k = 0; k < OBJECTS; k++) {
        inputs[k] = make_input(&g, &g.alloc, (size_t)(10 + k), N, k);
        app.input_data[k] = moq_rcbuf_data(inputs[k]);
    }
    pthread_t th;
    CHECKED(pthread_create(&th, NULL, app_thread, &app));
    app.self = th;
    int net_timeouts = 0;
    for (int k = 0; k < OBJECTS; k++) {
        bool retained = (k % 2) == 1;
        feed_object(&g, (size_t)(10 + k), inputs[k]);
        if (!retained) moq_rcbuf_decref(inputs[k]);
        rig_pump_net(&g, 2);
        if (retained) {
            /* Both releases become eligible together: the application's of
             * what it polled, and this side's of the input. */
            if (!barrier_wait(&app.bar)) net_timeouts++;
            moq_rcbuf_decref(inputs[k]);
        }
    }
    /* Completion: every object polled and released, waited for boundedly. */
    struct timespec dl;
    clock_gettime(CLOCK_REALTIME, &dl);
    dl.tv_sec += 10;
    CHECKED(pthread_mutex_lock(&app.mu));
    while (app.polled < OBJECTS) {
        int e = pthread_cond_timedwait(&app.cv, &app.mu, &dl);
        if (e == ETIMEDOUT) break;
        CHECKED(e);
    }
    int polled = app.polled;
    CHECKED(pthread_mutex_unlock(&app.mu));
    CHECKED(pthread_join(th, NULL));

    int app_freed = 0, net_freed = 0, misplaced = 0, not_once = 0;
    for (int k = 0; k < OBJECTS; k++) {
        pthread_t by;
        if (ho_freed(&g.ha, k, &by) != 1) { not_once++; continue; }
        bool on_app = pthread_equal(by, app.self);
        if (on_app) app_freed++; else net_freed++;
        if (on_app != ((k % 2) == 0)) misplaced++;
    }
    printf("concurrent: polled=%d of %d, input roots freed once=%d (on the app thread=%d, "
           "on the network thread=%d), misplaced=%d, bad content=%d, bad identity=%d, "
           "timeouts app=%d net=%d\n", polled, OBJECTS, OBJECTS - not_once, app_freed,
           net_freed, misplaced, app.bad_content, app.bad_identity, app.timeouts,
           net_timeouts);
    MOQ_TEST_CHECK(polled == OBJECTS);
    MOQ_TEST_CHECK(app.timeouts == 0 && net_timeouts == 0);
    MOQ_TEST_CHECK(app.bad_content == 0 && app.bad_identity == 0);
    MOQ_TEST_CHECK(not_once == 0);
    MOQ_TEST_CHECK(app_freed == OBJECTS / 2 && net_freed == OBJECTS / 2 && misplaced == 0);
    MOQ_TEST_CHECK(queue_empty(&g));
    CHECKED(pthread_cond_destroy(&app.cv));
    CHECKED(pthread_mutex_destroy(&app.mu));
    barrier_fini(&app.bar);
    rig_close(&g);
    return failures;
}

int main(int argc, char **argv)
{
    if (argc > 1 && strcmp(argv[1], "--concurrent") == 0) {
        int f = test_concurrent();
        printf("test_media_receiver_payload_handoff --concurrent: %s\n", f ? "FAILED" : "passed");
        return f ? 1 : 0;
    }
    test_exclusive_transfer();
    test_caller_retained_input_is_copied();
    test_other_allocator_session_copies();
    test_wrapped_input_session_copies();
    test_held_object_while_producer_continues();
    test_handoff_copy_refused();
    test_slice_input_charge_is_retained_data();
    test_no_copy_before_admission();
    test_unsubscribe_in_copy_window();
    test_catalog_duplicate_is_never_copied();
#ifdef MOQ_TEST_HAVE_MPEG2TS
    ts_gen_init(&g_ts_gen, 188, 1, false);
    for (int how = 0; how < 3; how++) test_ts_held_object_destroyed_elsewhere(how);
    printf("mpeg2ts rows: built and run\n");
#else
    printf("mpeg2ts rows: not built (no moq::mpeg2ts)\n");
#endif
    if (failures) {
        fprintf(stderr, "test_media_receiver_payload_handoff: %d failure(s)\n", failures);
        return 1;
    }
    printf("test_media_receiver_payload_handoff: all passed\n");
    return 0;
}
