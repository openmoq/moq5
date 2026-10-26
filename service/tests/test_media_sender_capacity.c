/*
 * Sender capacity arithmetic (media_sender.c sender_new / write accounting).
 *
 * The public cfg carries two u32 object bounds, queue_max_objects and
 * pre_ready_max_objects (0 = default). One physical ring serves both phases
 * and is sized max(both) + 1 so a full ring is distinguishable from empty,
 * then allocated as ring_cap * sizeof(entry) and indexed by `% ring_cap`.
 * That arithmetic is judged here, per field, at the boundaries 0, 1, 255,
 * 256, UINT32_MAX - 1 and UINT32_MAX, through an allocator the test owns and
 * through the SAME pure geometry helper construction consumes:
 *
 *   REPRESENTABLE   max + 1 fits u32 and the byte product fits the target:
 *                   the ring geometry is exactly max + 1, construction
 *                   succeeds, and the pre-ready boundary is exactly the
 *                   configured bound (the bound-th write accepted, the next
 *                   refused as WOULD_BLOCK);
 *   GEOMETRY WRAP   max == UINT32_MAX: max + 1 is 0 in u32, an impossible
 *                   configuration -> MOQ_ERR_INVAL, before any allocation;
 *   BYTE WRAP       ring_cap fits u32 but ring_cap * sizeof(entry) does not
 *                   fit the target's size_t: an impossible configuration ->
 *                   MOQ_ERR_INVAL from checked multiplication, before any
 *                   allocator request. The host row supplies a 32-bit
 *                   representable limit through the test-only wrapper, so a
 *                   64-bit host exercises the 32-bit arithmetic; the Android
 *                   armeabi-v7a lane compiles the same rows with a real
 *                   32-bit size_t (its execution there is owed, not claimed);
 *   REFUSAL         the byte count IS representable but the allocator
 *                   refuses it: MOQ_ERR_NOMEM, with the exact requested size
 *                   observed -- arithmetic corruption and allocator refusal
 *                   are distinguished, never conflated.
 *
 * Every construction failure must leave `*out == NULL` (the public contract:
 * both constructors null it before validation), make no zero-byte request,
 * transfer no ownership and end balanced.
 *
 * Two write-time byte-budget wraps are frozen too, with wrapped buffers whose
 * logical length is not backed by memory (moq_rcbuf_wrap allocates only the
 * header; the data pointer is never dereferenced by the sender before
 * admission): a payload + properties sum that wraps size_t must be
 * MOQ_ERR_INVAL with neither reference transferred; a representable length
 * above the byte cap, written behind one small queued object so that
 * `preq_bytes + need` wraps, must be MOQ_ERR_WOULD_BLOCK with no transfer and
 * no mutation.
 *
 * The allocator refuses any single request above a fixed limit, so no row
 * ever touches memory of the configured size. No sleep, no network: create()
 * rows intercept the connection boundary with a test-only hook that starts
 * nothing and counts attempts.
 */

#include <moq/media_sender.h>
#include <moq/endpoint.h>
#include "test_support.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* -- test seams (media_sender.c, MOQ_MEDIA_SENDER_TESTING) -------------- */
moq_result_t moq_media_sender_test_new_cfg_alloc(
    const moq_media_sender_cfg_t *cfg, const moq_alloc_t *alloc,
    moq_media_sender_t **out);
moq_result_t moq_media_sender_test_new_cfg_alloc_limit(
    const moq_media_sender_cfg_t *cfg, const moq_alloc_t *alloc,
    size_t size_limit, moq_media_sender_t **out);
void moq_media_sender_test_ring_geometry(uint32_t queue_cap, uint32_t preq_cap,
                                         size_t size_limit,
                                         uint32_t *ring_cap, size_t *bytes,
                                         bool *cap_representable,
                                         bool *bytes_representable);
uint32_t moq_media_sender_test_ring_cap(const moq_media_sender_t *s);
size_t moq_media_sender_test_ring_entry_size(void);
moq_result_t moq_media_sender_test_validate_cfg(const moq_media_sender_cfg_t *cfg);
void moq_media_sender_test_set_connect_hook(
    moq_result_t (*fn)(const moq_endpoint_cfg_t *, moq_endpoint_t **, void *), void *ctx);
uint32_t moq_media_sender_test_queue_entry(moq_media_sender_t *s, uint32_t i,
                                           const moq_media_track_t **track,
                                           uint64_t *group_seq, bool *is_sync,
                                           bool *starts_group, size_t *bytes,
                                           const uint8_t **payload_data,
                                           size_t *payload_len);

/* -- the refusing, counting allocator ------------------------------------ */

#define ALLOC_LIMIT (64u * 1024u * 1024u)

typedef struct {
    long   live_blocks;
    long   total_allocs;
    long   refused;
    size_t largest_request;
    size_t largest_refused;
    long   zero_requests;
} astats_t;

static void *c_alloc(size_t size, void *ctx)
{
    astats_t *st = (astats_t *)ctx;
    st->total_allocs++;
    if (size > st->largest_request) st->largest_request = size;
    if (size == 0) st->zero_requests++;
    if (size > ALLOC_LIMIT) {
        st->refused++;
        if (size > st->largest_refused) st->largest_refused = size;
        return NULL;
    }
    void *p = malloc(size ? size : 1);
    if (p) st->live_blocks++;
    return p;
}
static void *c_realloc(void *ptr, size_t old_size, size_t new_size, void *ctx)
{
    astats_t *st = (astats_t *)ctx;
    (void)old_size;
    if (new_size > st->largest_request) st->largest_request = new_size;
    if (new_size > ALLOC_LIMIT) { st->refused++; return NULL; }
    void *p = realloc(ptr, new_size ? new_size : 1);
    if (p && !ptr) st->live_blocks++;
    return p;
}
static void c_free(void *ptr, size_t size, void *ctx)
{
    astats_t *st = (astats_t *)ctx;
    (void)size;
    if (ptr) { free(ptr); st->live_blocks--; }
}
static moq_alloc_t make_alloc(astats_t *st)
{
    memset(st, 0, sizeof(*st));
    moq_alloc_t a = { st, c_alloc, c_realloc, c_free };
    return a;
}

/* -- fixture ------------------------------------------------------------ */

#define SENDER_DEFAULT_OBJECTS 256u   /* SENDER_DEFAULT_{QUEUE,PRE_READY}_OBJECTS */
#define STALE ((moq_media_sender_t *)(uintptr_t)0x1)

static void fill_cfg(moq_media_sender_cfg_t *cfg, moq_bytes_t *parts)
{
    moq_media_sender_cfg_init_live_sized(cfg, sizeof(*cfg));
    parts[0] = MOQ_BYTES_LITERAL("svc");
    parts[1] = MOQ_BYTES_LITERAL("demo");
    cfg->namespace_ = (moq_namespace_t){ parts, 2 };
    cfg->backpressure = MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK;
}

typedef enum { FIELD_QUEUE, FIELD_PRE_READY } field_t;
static const char *field_name(field_t f)
{ return f == FIELD_QUEUE ? "queue_max_objects" : "pre_ready_max_objects"; }
static uint32_t effective(uint32_t v) { return v ? v : SENDER_DEFAULT_OBJECTS; }
static void set_field(moq_media_sender_cfg_t *cfg, field_t f, uint32_t v)
{ if (f == FIELD_QUEUE) cfg->queue_max_objects = v; else cfg->pre_ready_max_objects = v; }

/* The test's OWN expectation of the geometry, independent of the product. */
static bool expected_ring_cap(uint32_t queue, uint32_t pre, uint32_t *cap)
{
    uint32_t m = effective(queue) > effective(pre) ? effective(queue) : effective(pre);
    if (m == UINT32_MAX) return false;
    *cap = m + 1;
    return true;
}

static moq_media_track_t *add_trk(moq_media_sender_t *s, const char *name,
                                  moq_media_packaging_t packaging)
{
    moq_media_track_cfg_t tc;
    moq_media_track_cfg_init(&tc);
    tc.name = (moq_bytes_t){ (const uint8_t *)name, strlen(name) };
    tc.media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc.packaging = packaging;
    tc.codec = packaging == MOQ_MEDIA_PACKAGING_CMAF
        ? MOQ_BYTES_LITERAL("avc1.640028") : MOQ_BYTES_LITERAL("av01");
    tc.bitrate = 1500000;
    if (packaging == MOQ_MEDIA_PACKAGING_CMAF) tc.timescale = 90000;
    tc.is_live = true;
    moq_media_track_t *t = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_add_track(s, &tc, &t), (int)MOQ_OK);
    return t;
}

/* One write of a tiny sync object. Ownership-on-write: an ACCEPTED object's
 * reference belongs to the sender; a refused one is released here. */
static moq_result_t write_one(moq_media_sender_t *s, moq_media_track_t *t,
                              const moq_alloc_t *alloc, bool first)
{
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)"x", 1, &pl) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_media_send_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o);
    o.payload = pl;
    o.is_sync = first;
    o.starts_group = first;
    moq_result_t rc = moq_media_sender_write(s, t, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(pl);
    return rc;
}

static void get_stats(const moq_media_sender_t *s, moq_media_sender_stats_t *st)
{
    memset(st, 0, sizeof(*st));
    st->struct_size = sizeof(*st);
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_get_stats(s, st, sizeof(*st)), (int)MOQ_OK);
}

/* The complete public stats contract on a write refusal: every accounting
 * member exactly unchanged and last_error exactly the refusal. Named member
 * by member, so an unexamined counter cannot move silently. */
#define STATS_SAME(m) do { \
    if (a->m != b->m) { \
        fprintf(stderr, "FAIL: %s:%d: stats." #m " moved: %llu -> %llu\n", __FILE__, __LINE__, \
                (unsigned long long)b->m, (unsigned long long)a->m); \
        failures++; \
    } } while (0)
static void expect_refused_stats(const moq_media_sender_stats_t *b,
                                 const moq_media_sender_stats_t *a,
                                 moq_result_t want_last_error)
{
    STATS_SAME(struct_size);
    STATS_SAME(objects_written);
    STATS_SAME(objects_sent);
    STATS_SAME(objects_queued);
    STATS_SAME(bytes_queued);
    STATS_SAME(objects_dropped);
    STATS_SAME(groups_dropped);
    STATS_SAME(keyframes_dropped);
    STATS_SAME(groups_abandoned);
    STATS_SAME(backpressure_stalls);
    STATS_SAME(sap_records_evicted);
    MOQ_TEST_CHECK_EQ_INT((int)a->last_error, (int)want_last_error);
}

/* -- construction rows ----------------------------------------------------- */

static void row_representable(field_t f, uint32_t value)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    set_field(&cfg, f, value);
    uint32_t want_cap = 0;
    MOQ_TEST_CHECK(expected_ring_cap(cfg.queue_max_objects, cfg.pre_ready_max_objects, &want_cap));

    /* The product's helper must agree with the test's expectation. */
    uint32_t hcap = 0; size_t hbytes = 0; bool cap_ok = false, bytes_ok = false;
    moq_media_sender_test_ring_geometry(effective(cfg.queue_max_objects),
                                        effective(cfg.pre_ready_max_objects),
                                        SIZE_MAX, &hcap, &hbytes, &cap_ok, &bytes_ok);
    MOQ_TEST_CHECK(cap_ok && bytes_ok);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)hcap, (uint64_t)want_cap);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)hbytes,
                          (uint64_t)want_cap * (uint64_t)moq_media_sender_test_ring_entry_size());

    moq_media_sender_t *s = STALE;
    moq_result_t rc = moq_media_sender_test_new_cfg_alloc(&cfg, &alloc, &s);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    if (rc != MOQ_OK) { MOQ_TEST_CHECK(s == NULL); return; }
    MOQ_TEST_CHECK(s != NULL && s != STALE);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)moq_media_sender_test_ring_cap(s), (uint64_t)want_cap);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.zero_requests, 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.refused, 0);

    if (f == FIELD_PRE_READY) {
        uint32_t bound = effective(value);
        moq_media_track_t *t = add_trk(s, "v", MOQ_MEDIA_PACKAGING_RAW);
        uint32_t accepted = 0;
        for (uint32_t i = 0; i < bound; i++) {
            moq_result_t w = write_one(s, t, &alloc, i == 0);
            if (w != MOQ_OK) {
                fprintf(stderr, "  [%s=%" PRIu32 "] write %" PRIu32 " of %" PRIu32 " refused: %d\n",
                        field_name(f), value, i + 1, bound, (int)w);
                break;
            }
            accepted++;
        }
        MOQ_TEST_CHECK_EQ_U64((uint64_t)accepted, (uint64_t)bound);
        MOQ_TEST_CHECK_EQ_INT((int)write_one(s, t, &alloc, false), (int)MOQ_ERR_WOULD_BLOCK);
    }
    moq_media_sender_destroy(s);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.live_blocks, 0);
    printf("ok: %s=%" PRIu32 " ring_cap=%" PRIu32 " allocs=%ld balanced\n",
           field_name(f), value, want_cap, st.total_allocs);
}

/* Shared tail for every construction failure: exact code, NULL out, no
 * zero-byte request, exact balance; a stale sentinel can never survive. */
static void expect_refusal(const char *label, moq_result_t rc, moq_result_t want,
                           moq_media_sender_t *s, astats_t *st)
{
    printf("%s: rc=%d out=%s zero_requests=%ld refused=%ld largest=%zu live=%ld\n",
           label, (int)rc, s == STALE ? "STALE" : (s == NULL ? "NULL" : "HANDLE"),
           st->zero_requests, st->refused, st->largest_request, st->live_blocks);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)want);
    MOQ_TEST_CHECK(s == NULL);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st->zero_requests, 0);
    if (rc == MOQ_OK && s != NULL && s != STALE) moq_media_sender_destroy(s);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st->live_blocks, 0);
}

/* max + 1 wraps to 0: an impossible configuration. */
static void row_geometry_wrap(field_t f)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    set_field(&cfg, f, UINT32_MAX);
    uint32_t hcap = 1; size_t hbytes = 1; bool cap_ok = true, bytes_ok = true;
    moq_media_sender_test_ring_geometry(effective(cfg.queue_max_objects),
                                        effective(cfg.pre_ready_max_objects),
                                        SIZE_MAX, &hcap, &hbytes, &cap_ok, &bytes_ok);
    MOQ_TEST_CHECK(!cap_ok);
    MOQ_TEST_CHECK(!bytes_ok);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)hcap, 0);   /* the u32 wrap, as computed */
    moq_media_sender_t *s = STALE;
    moq_result_t rc = moq_media_sender_test_new_cfg_alloc(&cfg, &alloc, &s);
    char label[64]; snprintf(label, sizeof(label), "geometry-wrap %s=UINT32_MAX", field_name(f));
    expect_refusal(label, rc, MOQ_ERR_INVAL, s, &st);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.total_allocs, 0);   /* refused before any allocation */
}

/* ring_cap fits u32 but the byte product does not fit a 32-bit size_t: the
 * host supplies UINT32_MAX as the representable bound, so the product's
 * checked multiplication is exercised on a 64-bit host. */
static void row_bytes_wrap_32(field_t f)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    set_field(&cfg, f, UINT32_MAX - 1);
    uint32_t hcap = 0; size_t hbytes = 0; bool cap_ok = false, bytes_ok = true;
    moq_media_sender_test_ring_geometry(effective(cfg.queue_max_objects),
                                        effective(cfg.pre_ready_max_objects),
                                        (size_t)UINT32_MAX, &hcap, &hbytes, &cap_ok, &bytes_ok);
    MOQ_TEST_CHECK(cap_ok);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)hcap, (uint64_t)UINT32_MAX);
    MOQ_TEST_CHECK(!bytes_ok);   /* UINT32_MAX * 88 does not fit 32 bits */
    moq_media_sender_t *s = STALE;
    moq_result_t rc = moq_media_sender_test_new_cfg_alloc_limit(&cfg, &alloc, (size_t)UINT32_MAX, &s);
    char label[64]; snprintf(label, sizeof(label), "bytes-wrap-32 %s=UINT32_MAX-1", field_name(f));
    expect_refusal(label, rc, MOQ_ERR_INVAL, s, &st);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.total_allocs, 0);   /* checked multiplication, before any allocation */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.refused, 0);
}

/* The same geometry on THIS host's real size_t: representable, so the
 * allocator is asked for the exact unwrapped product and refuses it. */
static void row_bytes_refused(field_t f)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    set_field(&cfg, f, UINT32_MAX - 1);
    uint32_t hcap = 0; size_t hbytes = 0; bool cap_ok = false, bytes_ok = false;
    moq_media_sender_test_ring_geometry(effective(cfg.queue_max_objects),
                                        effective(cfg.pre_ready_max_objects),
                                        SIZE_MAX, &hcap, &hbytes, &cap_ok, &bytes_ok);
    uint64_t product = (uint64_t)UINT32_MAX * (uint64_t)moq_media_sender_test_ring_entry_size();
    moq_media_sender_t *s = STALE;
    moq_result_t rc = moq_media_sender_test_new_cfg_alloc(&cfg, &alloc, &s);
    char label[64]; snprintf(label, sizeof(label), "bytes-refused %s=UINT32_MAX-1", field_name(f));
    if (sizeof(size_t) >= 8) {
        MOQ_TEST_CHECK(cap_ok && bytes_ok);
        expect_refusal(label, rc, MOQ_ERR_NOMEM, s, &st);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)st.refused, 1);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)st.largest_refused, product);
    } else {
        /* A real 32-bit target: this IS the byte-wrap case. */
        MOQ_TEST_CHECK(cap_ok && !bytes_ok);
        expect_refusal(label, rc, MOQ_ERR_INVAL, s, &st);
        MOQ_TEST_CHECK_EQ_U64((uint64_t)st.total_allocs, 0);
    }
}

/* NULL-out contract on a plain validation failure (no namespace). */
static void row_validation_null_out(void)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    cfg.namespace_.count = 0;
    moq_media_sender_t *s = STALE;
    moq_result_t rc = moq_media_sender_test_new_cfg_alloc(&cfg, &alloc, &s);
    expect_refusal("validation (no namespace)", rc, MOQ_ERR_INVAL, s, &st);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.total_allocs, 0);
}

/* -- write-time byte-budget rows ----------------------------------------- */

typedef struct { int released; const uint8_t *data; size_t len; } release_log_t;
static void on_release(void *ctx, const uint8_t *data, size_t len)
{ release_log_t *l = ctx; l->released++; l->data = data; l->len = len; }

/* A buffer whose LOGICAL length is `len` but whose storage is one byte that
 * is never read: the sender only reads lengths before admission. */
static moq_rcbuf_t *wrapped(const moq_alloc_t *alloc, size_t len, release_log_t *log)
{
    static uint8_t dummy[1] = { 0 };
    moq_rcbuf_t *b = NULL;
    memset(log, 0, sizeof(*log));
    MOQ_TEST_CHECK_EQ_INT((int)moq_rcbuf_wrap(alloc, dummy, len, on_release, log, &b), (int)MOQ_OK);
    return b;
}

static void row_sum_wrap(void)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    cfg.validate_cmaf = false;   /* explicit passthrough: the payload is never parsed */
    moq_media_sender_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_new_cfg_alloc(&cfg, &alloc, &s), (int)MOQ_OK);
    if (!s) return;
    moq_media_track_t *t = add_trk(s, "c", MOQ_MEDIA_PACKAGING_CMAF);
    moq_media_sender_stats_t before; get_stats(s, &before);
    long live_before = st.live_blocks;

    release_log_t lp, lq;
    moq_rcbuf_t *payload = wrapped(&alloc, SIZE_MAX - 16, &lp);
    moq_rcbuf_t *props   = wrapped(&alloc, 64, &lq);   /* 64 + (SIZE_MAX-16) wraps to 47 */
    moq_media_send_object_t o; memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o); o.payload = payload; o.properties = props;
    o.is_sync = true; o.starts_group = true;
    moq_result_t rc = moq_media_sender_write(s, t, &o);
    printf("sum-wrap: payload=%zu + properties=%zu -> rc=%d\n", (size_t)(SIZE_MAX - 16), (size_t)64, (int)rc);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);

    /* Neither reference transferred: the caller's decref is the LAST one and
     * fires each release exactly once, BEFORE destroy. Ownership-on-write is
     * honoured in the fixture too: were the object accepted (today's RED),
     * the references now belong to the sender and are released by destroy --
     * still exactly once each, which is asserted after destroy in both
     * cases, so the fixture is sanitizer-clean in either state. */
    if (rc != MOQ_OK) { moq_rcbuf_decref(payload); moq_rcbuf_decref(props); }
    MOQ_TEST_CHECK_EQ_INT(lp.released, rc != MOQ_OK ? 1 : 0);
    MOQ_TEST_CHECK_EQ_INT(lq.released, rc != MOQ_OK ? 1 : 0);
    /* Accounting and queue unchanged; last_error updated to the refusal. */
    moq_media_sender_stats_t after; get_stats(s, &after);
    expect_refused_stats(&before, &after, MOQ_ERR_INVAL);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.live_blocks, (uint64_t)live_before);
    /* A small write still succeeds afterwards: nothing was corrupted. */
    MOQ_TEST_CHECK_EQ_INT((int)write_one(s, t, &alloc, true), (int)MOQ_OK);
    moq_media_sender_destroy(s);
    /* Each release exactly once, with the exact logical length it wrapped. */
    MOQ_TEST_CHECK_EQ_INT(lp.released, 1);
    MOQ_TEST_CHECK_EQ_INT(lq.released, 1);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)lp.len, (uint64_t)(SIZE_MAX - 16));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)lq.len, 64);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.live_blocks, 0);
}

static void row_budget_wrap(void)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    cfg.pre_ready_max_bytes = 1024;   /* the u32 byte cap under test */
    moq_media_sender_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_new_cfg_alloc(&cfg, &alloc, &s), (int)MOQ_OK);
    if (!s) return;
    moq_media_track_t *t = add_trk(s, "v", MOQ_MEDIA_PACKAGING_RAW);
    /* One small object queued: preq_bytes == 1. */
    MOQ_TEST_CHECK_EQ_INT((int)write_one(s, t, &alloc, true), (int)MOQ_OK);
    moq_media_sender_stats_t before; get_stats(s, &before);
    MOQ_TEST_CHECK_EQ_U64(before.bytes_queued, 1);
    long live_before = st.live_blocks;

    /* A representable single length above the cap; preq_bytes + need would
     * wrap size_t today (1 + (SIZE_MAX - 1) == SIZE_MAX, then +1 more == 0
     * for SIZE_MAX itself): use SIZE_MAX so 1 + need wraps to 0 <= cap. */
    release_log_t lp;
    moq_rcbuf_t *payload = wrapped(&alloc, SIZE_MAX, &lp);
    moq_media_send_object_t o; memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o); o.payload = payload;
    o.is_sync = false; o.starts_group = false;   /* same group, after the anchor */
    moq_result_t rc = moq_media_sender_write(s, t, &o);
    printf("budget-wrap: queued=1 need=%zu cap=1024 -> rc=%d\n", (size_t)SIZE_MAX, (int)rc);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    if (rc != MOQ_OK) moq_rcbuf_decref(payload);      /* refused: still ours */
    MOQ_TEST_CHECK_EQ_INT(lp.released, rc != MOQ_OK ? 1 : 0);   /* no transfer */
    /* Accounting and queue unchanged; last_error updated to the refusal. */
    moq_media_sender_stats_t after; get_stats(s, &after);
    expect_refused_stats(&before, &after, MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64(after.bytes_queued, 1);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.live_blocks, (uint64_t)live_before);
    moq_media_sender_destroy(s);
    MOQ_TEST_CHECK_EQ_INT(lp.released, 1);            /* exactly once, either way */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)lp.len, (uint64_t)SIZE_MAX);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.live_blocks, 0);
}

/* -- construction preflight rows ------------------------------------------ */

/* The independent expectation for a bound: representable iff max + 1 fits
 * u32 AND (max + 1) * entry fits the TARGET's size_t. Computed in 64-bit from
 * a supplied width so the host exercises the 32-bit arithmetic too. */
static bool geometry_ok_for(uint32_t queue, uint32_t pre, size_t entry, unsigned size_t_bits)
{
    uint32_t m = effective(queue) > effective(pre) ? effective(queue) : effective(pre);
    if (m == UINT32_MAX) return false;
    uint64_t limit = size_t_bits >= 64 ? UINT64_MAX : ((uint64_t)1 << size_t_bits) - 1;
    return (uint64_t)(m + 1u) <= limit / (uint64_t)entry;
}
/* The largest bound whose ring still fits a 32-bit size_t: a positive
 * neighbour representable on BOTH widths. */
static uint32_t largest_cap_fitting_32(size_t entry)
{
    uint64_t cap_plus_one = (uint64_t)UINT32_MAX / (uint64_t)entry;
    return (uint32_t)(cap_plus_one - 1);
}

/* The SHARED config validation must refuse impossible geometry itself, so a
 * real create() never connects an endpoint for a configuration no sender can
 * be built from. Both fields, through the existing validate seam; the
 * expected result comes from the independent geometry above evaluated at
 * THIS target's size_t width, and its 32-bit evaluation is asserted on the
 * host as well. Validation has no side effects: nothing is allocated. */
static void row_validate_rejects_geometry(field_t f)
{
    size_t entry = moq_media_sender_test_ring_entry_size();
    unsigned bits = (unsigned)(sizeof(size_t) * 8u);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);

    /* UINT32_MAX: impossible on every width (max + 1 wraps). */
    set_field(&cfg, f, UINT32_MAX);
    MOQ_TEST_CHECK(!geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, 32));
    MOQ_TEST_CHECK(!geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, 64));
    moq_result_t rc = moq_media_sender_test_validate_cfg(&cfg);
    printf("validate-geometry %s=UINT32_MAX (size_t %u-bit) -> rc=%d\n", field_name(f), bits, (int)rc);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);

    /* The exact maximum neighbour, UINT32_MAX - 1: ring of UINT32_MAX entries.
     * Its byte product fits a 64-bit size_t and NOT a 32-bit one, so the
     * expectation depends on the target. The 32-bit answer is computed here
     * on every host; only a 32-bit target executes it against the product. */
    set_field(&cfg, f, UINT32_MAX - 1);
    bool ok32 = geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, 32);
    bool ok64 = geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, 64);
    MOQ_TEST_CHECK(!ok32);
    MOQ_TEST_CHECK(ok64);
    bool ok_here = geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, bits);
    rc = moq_media_sender_test_validate_cfg(&cfg);
    printf("validate-geometry %s=UINT32_MAX-1 (size_t %u-bit; 32-bit model %s, 64-bit model %s) -> rc=%d\n",
           field_name(f), bits, ok32 ? "OK" : "INVAL", ok64 ? "OK" : "INVAL", (int)rc);
    MOQ_TEST_CHECK_EQ_INT((int)rc, ok_here ? (int)MOQ_OK : (int)MOQ_ERR_INVAL);

    /* A positive neighbour representable on both widths. */
    uint32_t both = largest_cap_fitting_32(entry);
    set_field(&cfg, f, both);
    MOQ_TEST_CHECK(geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, 32));
    MOQ_TEST_CHECK(geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, 64));
    rc = moq_media_sender_test_validate_cfg(&cfg);
    printf("validate-geometry %s=%" PRIu32 " (fits 32-bit) -> rc=%d\n", field_name(f), both, (int)rc);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    /* And one above it does not fit 32-bit: INVAL there, OK on 64-bit. */
    set_field(&cfg, f, both + 1u);
    MOQ_TEST_CHECK(!geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, 32));
    rc = moq_media_sender_test_validate_cfg(&cfg);
    MOQ_TEST_CHECK_EQ_INT((int)rc, geometry_ok_for(cfg.queue_max_objects, cfg.pre_ready_max_objects, entry, bits) ? (int)MOQ_OK : (int)MOQ_ERR_INVAL);
}

/* The PUBLIC create(): validation precedes the endpoint connection. The
 * connection boundary is intercepted by a test-only hook that starts no
 * socket or worker, counts every entry, and answers with a declared result.
 * An impossible geometry must be INVAL with NULL output and ZERO entries; a
 * representable control must enter exactly once -- which is what makes the
 * counter a live observer -- and, because the declared answer is itself
 * INVAL, a validation placed after the connection could not hide behind the
 * return code. The hook is cleared on every exit path. */
typedef struct { int attempts; moq_result_t answer; } connect_probe_t;
static moq_result_t probe_connect(const moq_endpoint_cfg_t *cfg, moq_endpoint_t **out, void *ctx)
{
    connect_probe_t *p = ctx; (void)cfg;
    p->attempts++;
    *out = NULL;
    return p->answer;
}
static void row_create_rejects_before_connect(field_t f)
{
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    static char url[] = "moqt://127.0.0.1:4433";
    moq_endpoint_cfg_t ec; moq_endpoint_cfg_init(&ec);
    ec.url = (moq_bytes_t){ (const uint8_t *)url, sizeof(url) - 1 };
    cfg.endpoint = &ec;
    connect_probe_t probe = { 0, MOQ_ERR_INVAL };
    moq_media_sender_test_set_connect_hook(probe_connect, &probe);

    set_field(&cfg, f, UINT32_MAX);
    moq_media_sender_t *s = STALE;
    moq_result_t rc = moq_media_sender_create(&cfg, &s);
    printf("create-before-connect %s=UINT32_MAX -> rc=%d out=%s connect_attempts=%d\n", field_name(f), (int)rc,
           s == STALE ? "STALE" : (s == NULL ? "NULL" : "HANDLE"), probe.attempts);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(s == NULL);
    MOQ_TEST_CHECK_EQ_INT(probe.attempts, 0);
    if (rc == MOQ_OK && s && s != STALE) moq_media_sender_destroy(s);

    /* The representable control reaches the boundary exactly once. */
    set_field(&cfg, f, 256u);
    s = STALE;
    rc = moq_media_sender_create(&cfg, &s);
    printf("create-control %s=256 -> rc=%d connect_attempts=%d\n", field_name(f), (int)rc, probe.attempts);
    MOQ_TEST_CHECK_EQ_INT(probe.attempts, 1);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_INVAL);   /* the declared answer, not a geometry refusal */
    MOQ_TEST_CHECK(s == NULL);
    if (rc == MOQ_OK && s && s != STALE) moq_media_sender_destroy(s);
    moq_media_sender_test_set_connect_hook(NULL, NULL);
}

/* -- the second unchecked addition in preq_make_room ---------------------- */

/* Both drop policies. Track A queues one sync object that stays this row's
 * retained anchor (A's latest anchor is never evictable); track B queues one
 * sync object that IS evictable once B writes a fresh anchor. B then writes
 * an oversized fresh-anchor object: `preq_bytes - free_bytes + need` (the
 * retained remainder plus the object) wraps today, and the object can never
 * fit. The contract: WOULD_BLOCK, no transfer, the queue's exact identities
 * and bytes untouched (A's and B's originals still queued), every accounting
 * stat unchanged -- in particular NO drop was performed to reach the refusal
 * -- and last_error WOULD_BLOCK. */
typedef struct {
    const moq_media_track_t *track; uint64_t group_seq; bool is_sync, starts_group;
    size_t bytes; const uint8_t *data; size_t len;
} qentry_t;
/* A bounded inventory of the queued ring: every entry's identity and lengths,
 * without dereferencing any payload. `complete` is true only when the whole
 * queue fitted the observer's storage; an inventory that did not fit is a
 * NAMED refusal and is never compared or adopted as a shorter valid one. */
typedef struct { qentry_t e[4]; uint32_t depth; uint32_t cap; bool complete; } qinv_t;
/* A refusal is a failure unless a control has declared it expected, in which
 * case it is counted here instead, by name, without failing the test. */
static bool g_refusal_expected;
static int  g_refusals;
static void refuse(const char *what)
{
    if (g_refusal_expected) {
        g_refusals++;
        printf("refused (expected by the control): %s\n", what);
    } else {
        fprintf(stderr, "FAIL: queue inventory refused: %s\n", what);
        failures++;
    }
}
static void observe_queue_into(moq_media_sender_t *s, qinv_t *inv, uint32_t cap)
{
    memset(inv, 0, sizeof(*inv));
    inv->cap = cap <= 4u ? cap : 4u;
    inv->depth = moq_media_sender_test_queue_entry(s, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
    inv->complete = inv->depth <= inv->cap;
    if (!inv->complete) {
        char why[96];
        snprintf(why, sizeof(why), "depth %u exceeds observer capacity %u", inv->depth, inv->cap);
        refuse(why);
        return;
    }
    for (uint32_t i = 0; i < inv->depth; i++)
        moq_media_sender_test_queue_entry(s, i, &inv->e[i].track, &inv->e[i].group_seq, &inv->e[i].is_sync,
                                          &inv->e[i].starts_group, &inv->e[i].bytes, &inv->e[i].data, &inv->e[i].len);
}
static bool qentry_same(const qentry_t *a, const qentry_t *b)
{
    return a->track == b->track && a->group_seq == b->group_seq && a->is_sync == b->is_sync &&
           a->starts_group == b->starts_group && a->bytes == b->bytes && a->data == b->data && a->len == b->len;
}
/* Compare two COMPLETE inventories entry by entry. Incomplete inventories are
 * refused by name here too, so a refused observation can never be compared. */
static void expect_same_inventory(const qinv_t *before, const qinv_t *after, const char *what)
{
    if (!before->complete || !after->complete) {
        char why[128];
        snprintf(why, sizeof(why), "%s: an incomplete inventory is not comparable", what);
        refuse(why);
        return;
    }
    MOQ_TEST_CHECK_EQ_U64((uint64_t)after->depth, (uint64_t)before->depth);
    uint32_t n = before->depth < after->depth ? before->depth : after->depth;
    for (uint32_t i = 0; i < n && i < before->cap && i < after->cap; i++) {
        if (!qentry_same(&before->e[i], &after->e[i])) {
            fprintf(stderr, "FAIL: %s:%d: %s: queue entry %u changed\n", __FILE__, __LINE__, what, i);
            failures++;
        }
    }
}
/* Guarded small-payload check: the pointer and the exact length are checked
 * before any byte is read, so a synthetic logical extent is never touched. */
static bool entry_reads(const qentry_t *e, const char *lit)
{
    size_t n = strlen(lit);
    return e->data != NULL && e->len == n && n <= 8 && memcmp(e->data, lit, n) == 0;
}

/* Write one small object with a DISTINCT payload, kept by the sender. */
static moq_result_t write_named(moq_media_sender_t *s, moq_media_track_t *t,
                                const moq_alloc_t *alloc, const char *payload, bool first)
{
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(alloc, (const uint8_t *)payload, strlen(payload), &pl) != MOQ_OK)
        return MOQ_ERR_NOMEM;
    moq_media_send_object_t o; memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o); o.payload = pl; o.is_sync = first; o.starts_group = first;
    moq_result_t rc = moq_media_sender_write(s, t, &o);
    if (rc != MOQ_OK) moq_rcbuf_decref(pl);
    return rc;
}

static void row_make_room_wrap(moq_media_send_backpressure_t policy, const char *label)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    cfg.backpressure = policy;
    cfg.pre_ready_max_bytes = 4096;
    moq_media_sender_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_new_cfg_alloc(&cfg, &alloc, &s), (int)MOQ_OK);
    if (!s) return;
    moq_media_track_t *ta = add_trk(s, "A", MOQ_MEDIA_PACKAGING_RAW);
    moq_media_track_t *tb = add_trk(s, "B", MOQ_MEDIA_PACKAGING_RAW);
    MOQ_TEST_CHECK_EQ_INT((int)write_named(s, ta, &alloc, "A1", true), (int)MOQ_OK);   /* A anchor, retained */
    MOQ_TEST_CHECK_EQ_INT((int)write_named(s, tb, &alloc, "B1", true), (int)MOQ_OK);   /* B anchor, evictable */
    moq_media_sender_stats_t before; get_stats(s, &before);
    MOQ_TEST_CHECK_EQ_U64(before.objects_queued, 2);
    MOQ_TEST_CHECK_EQ_U64(before.bytes_queued, 4);
    long live_before = st.live_blocks;
    /* The complete pre-existing queue, by identity, order and real bytes. */
    qinv_t qb; observe_queue_into(s, &qb, 4);
    MOQ_TEST_CHECK(qb.complete);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)qb.depth, 2);
    bool two = qb.complete && qb.depth == 2;
    MOQ_TEST_CHECK(two && qb.e[0].track == ta && qb.e[1].track == tb);
    MOQ_TEST_CHECK(two && qb.e[0].is_sync && qb.e[0].starts_group && qb.e[1].is_sync && qb.e[1].starts_group);
    MOQ_TEST_CHECK(two && entry_reads(&qb.e[0], "A1"));
    MOQ_TEST_CHECK(two && entry_reads(&qb.e[1], "B1"));

    release_log_t lp;
    /* retained (4 - 2 = 2) + need must wrap the expression's own width. On this
     * host size_t is 64-bit, so SIZE_MAX - 1 makes 2 + need == 2^64 -> 0. On a
     * 32-bit target `need` widens to uint64_t and this expression cannot wrap
     * at all; the host is the overflow witness, the subtraction form is
     * correct on both. */
    moq_rcbuf_t *payload = wrapped(&alloc, SIZE_MAX - 1, &lp);
    moq_media_send_object_t o; memset(&o, 0, sizeof(o));
    o.struct_size = sizeof(o); o.payload = payload;
    o.is_sync = true; o.starts_group = true;                       /* a fresh B anchor */
    moq_result_t rc = moq_media_sender_write(s, tb, &o);
    moq_media_sender_stats_t after; get_stats(s, &after);
    printf("make-room-wrap %s: queued=2 free=2 need=%zu cap=4096 -> rc=%d queued=%llu bytes=%llu dropped=%llu\n",
           label, (size_t)(SIZE_MAX - 1), (int)rc, (unsigned long long)after.objects_queued,
           (unsigned long long)after.bytes_queued, (unsigned long long)after.objects_dropped);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_ERR_WOULD_BLOCK);
    if (rc != MOQ_OK) moq_rcbuf_decref(payload);
    MOQ_TEST_CHECK_EQ_INT(lp.released, rc != MOQ_OK ? 1 : 0);
    /* Accounting unchanged; last_error updated; nothing evicted. */
    expect_refused_stats(&before, &after, MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.live_blocks, (uint64_t)live_before);
    /* The complete pre-existing queue is EXACTLY what it was: same depth, same
     * entries in the same order, same identities, flags, bytes and buffers.
     * An admitted synthetic buffer shows up as a depth mismatch here, by name,
     * without its logical extent ever being read. */
    qinv_t qa; observe_queue_into(s, &qa, 4);
    expect_same_inventory(&qb, &qa, "across the refusal");
    MOQ_TEST_CHECK(qa.complete && qa.depth >= 2 && entry_reads(&qa.e[0], "A1"));
    MOQ_TEST_CHECK(qa.complete && qa.depth >= 2 && entry_reads(&qa.e[1], "B1"));
    /* A small B delta still lands behind B's original anchor. */
    MOQ_TEST_CHECK_EQ_INT((int)write_named(s, tb, &alloc, "B2", false), (int)MOQ_OK);
    qinv_t qc; observe_queue_into(s, &qc, 4);
    MOQ_TEST_CHECK(qc.complete);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)qc.depth, 3);
    bool three = qc.complete && qc.depth == 3 && qb.complete && qb.depth == 2;
    MOQ_TEST_CHECK(three && qentry_same(&qb.e[0], &qc.e[0]) && qentry_same(&qb.e[1], &qc.e[1]));
    MOQ_TEST_CHECK(three && qc.e[2].track == tb && !qc.e[2].starts_group && entry_reads(&qc.e[2], "B2"));
    moq_media_sender_destroy(s);
    MOQ_TEST_CHECK_EQ_INT(lp.released, 1);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)lp.len, (uint64_t)(SIZE_MAX - 1));
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.live_blocks, 0);
}

/* -- control: an inventory beyond the observer's storage is refused ---------- */
/* Three ordinary accepted entries observed with a deliberately smaller
 * observer capacity (2): the observation must be a NAMED refusal, the
 * comparison must not proceed, and the row must still clean up to balance.
 * Its two expected refusals are counted by name through the shared refusal
 * path, so the row is green exactly when both fired and nothing else failed. */
static void row_inventory_over_capacity(void)
{
    astats_t st; moq_alloc_t alloc = make_alloc(&st);
    moq_bytes_t parts[2]; moq_media_sender_cfg_t cfg; fill_cfg(&cfg, parts);
    moq_media_sender_t *s = NULL;
    MOQ_TEST_CHECK_EQ_INT((int)moq_media_sender_test_new_cfg_alloc(&cfg, &alloc, &s), (int)MOQ_OK);
    if (!s) return;
    moq_media_track_t *ta = add_trk(s, "A", MOQ_MEDIA_PACKAGING_RAW);
    MOQ_TEST_CHECK_EQ_INT((int)write_named(s, ta, &alloc, "A1", true), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_named(s, ta, &alloc, "A2", false), (int)MOQ_OK);
    MOQ_TEST_CHECK_EQ_INT((int)write_named(s, ta, &alloc, "A3", false), (int)MOQ_OK);
    int before = failures;
    g_refusal_expected = true; g_refusals = 0;
    qinv_t small; observe_queue_into(s, &small, 2);          /* refused by name */
    MOQ_TEST_CHECK(!small.complete);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)small.depth, 3);
    qinv_t full; observe_queue_into(s, &full, 4);
    MOQ_TEST_CHECK(full.complete && full.depth == 3);
    expect_same_inventory(&small, &full, "over-capacity control");   /* refused by name; compares nothing */
    g_refusal_expected = false;
    printf("over-capacity control: depth 3 vs observer capacity 2 -> %d named refusal(s), comparison not performed\n", g_refusals);
    MOQ_TEST_CHECK_EQ_INT(g_refusals, 2);
    MOQ_TEST_CHECK_EQ_INT(failures, before);                        /* nothing else failed */
    moq_media_sender_destroy(s);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)st.live_blocks, 0);
}

int main(void)
{
    static const uint32_t representable[] = { 0u, 1u, 255u, 256u };
    for (int f = 0; f < 2; f++) {
        for (size_t i = 0; i < sizeof(representable) / sizeof(representable[0]); i++)
            row_representable((field_t)f, representable[i]);
        row_bytes_refused((field_t)f);
        row_bytes_wrap_32((field_t)f);
        row_geometry_wrap((field_t)f);
    }
    row_validation_null_out();
    row_validate_rejects_geometry(FIELD_QUEUE);
    row_validate_rejects_geometry(FIELD_PRE_READY);
    row_create_rejects_before_connect(FIELD_QUEUE);
    row_create_rejects_before_connect(FIELD_PRE_READY);
    row_sum_wrap();
    row_budget_wrap();
    row_make_room_wrap(MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME, "drop-to-keyframe");
    row_make_room_wrap(MOQ_MEDIA_SEND_BP_DROP_GROUP, "drop-group");
    row_inventory_over_capacity();
    MOQ_TEST_PASS("media_sender_capacity");
    if (failures)
        fprintf(stderr, "test_media_sender_capacity: %d failure(s)\n", failures);
    return failures ? 1 : 0;
}
