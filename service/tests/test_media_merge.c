/* moq_media_merge: one MPEG-TS track received over two paths. Each Location
 * comes back once, in Location order; a later copy is counted as a duplicate
 * or, after its Location was given up, as late; a hole on one path is filled
 * by the other path's copy. */
#include <moq/media_merge.h>
#include <moq/media_object.h>
#include <moq/control.h>
#include <moq/rcbuf.h>
#include "test_support.h"
#include "test_alloc_support.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* No field has been appended yet: the v0 floors are the whole records. */
_Static_assert(MOQ_MEDIA_MERGE_CFG_V0_SIZE == sizeof(moq_media_merge_cfg_t),
               "the cfg floor is its first release");
_Static_assert(MOQ_MEDIA_MERGE_PATH_STATS_V0_SIZE == sizeof(moq_media_merge_path_stats_t),
               "the stats floor is its first release");

/* One 188-octet TS packet on PID 0x100, payload only, its continuity counter
 * taken from the Location so every Object differs. */
static moq_media_object_t make_object(const moq_alloc_t *alloc, uint64_t group, uint64_t object,
                                      bool valid)
{
    uint8_t pkt[188];
    memset(pkt, 0xff, sizeof(pkt));
    pkt[0] = valid ? 0x47 : 0x00;  /* a wrong sync byte makes it invalid */
    pkt[1] = 0x01;
    pkt[2] = 0x00;
    pkt[3] = (uint8_t)(0x10 | ((group * 16 + object) & 0x0f));
    moq_media_object_t o;
    memset(&o, 0, sizeof(o));
    o.struct_size = (uint32_t)sizeof(o);
    o.packaging = MOQ_MEDIA_PACKAGING_MPEG2TS;
    o.status = MOQ_OBJECT_STATUS_NORMAL;
    o.group_id = group;
    o.object_id = object;
    o.has_subgroup = true;
    moq_rcbuf_t *buf = NULL;
    MOQ_TEST_CHECK(moq_rcbuf_create(alloc, pkt, sizeof(pkt), &buf) == MOQ_OK);
    o.payload_ref = buf;
    o.payload = (moq_bytes_t){ moq_rcbuf_data(buf), sizeof(pkt) };
    o.track = (moq_media_track_t *)(uintptr_t)0x1;  /* a receiver's handle */
    return o;
}

static void offer(moq_media_merge_t *m, const moq_alloc_t *alloc, uint32_t path, uint64_t now,
                  uint64_t group, uint64_t object, bool valid)
{
    moq_media_object_t o = make_object(alloc, group, object, valid);
    MOQ_TEST_CHECK(moq_media_merge_offer(m, path, now, &o, sizeof(o)) == MOQ_OK);
    MOQ_TEST_CHECK(o.payload_ref == NULL);  /* the merge took it */
}

typedef struct {
    uint64_t group, object;
    uint32_t path;
    bool gap;
} out_t;

/* Drain what is due at `now`; returns how many came back. */
static size_t drain(moq_media_merge_t *m, uint64_t now, out_t *out, size_t cap)
{
    size_t n = 0;
    moq_media_object_t o;
    uint32_t path = 99;
    while (moq_media_merge_poll(m, now, &o, sizeof(o), &path) == MOQ_OK) {
        MOQ_TEST_CHECK(o.track == NULL);
        MOQ_TEST_CHECK(o.has_loss_report);
        if (n < cap) out[n] = (out_t){ o.group_id, o.object_id, path, o.gap };
        n++;
        moq_media_object_cleanup(&o);
    }
    return n;
}

static moq_media_merge_t *make_merge(const moq_alloc_t *alloc, uint32_t slots)
{
    moq_media_merge_cfg_t cfg;
    moq_media_merge_cfg_init(&cfg);
    cfg.window_us = 1000;
    cfg.slot_count = slots;
    moq_media_merge_t *m = NULL;
    MOQ_TEST_CHECK(moq_media_merge_create(&cfg, alloc, &m) == MOQ_OK);
    return m;
}

static moq_media_merge_path_stats_t stats(moq_media_merge_t *m, uint32_t path)
{
    moq_media_merge_path_stats_t s;
    MOQ_TEST_CHECK(moq_media_merge_get_path_stats(m, path, &s, sizeof(s)) == MOQ_OK);
    return s;
}

/* Path 0 loses (0,2); path 1 lags but has it. Every Location comes back once,
 * in order, and the hole is filled from path 1 without a gap. */
static void test_other_path_fills_a_hole(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_media_merge_t *m = make_merge(&alloc, 16);
    offer(m, &alloc, 0, 10, 0, 0, true);
    offer(m, &alloc, 0, 11, 0, 1, true);
    offer(m, &alloc, 0, 12, 0, 3, true);   /* (0,2) lost on path 0 */
    out_t out[8];
    size_t n = drain(m, 13, out, 8);
    MOQ_TEST_CHECK(n == 2);               /* (0,3) waits for (0,2) */
    offer(m, &alloc, 1, 20, 0, 0, true);  /* path 1's copies arrive later */
    offer(m, &alloc, 1, 21, 0, 1, true);
    offer(m, &alloc, 1, 22, 0, 2, true);
    offer(m, &alloc, 1, 23, 0, 3, true);
    n += drain(m, 24, out + n, 8 - n);
    MOQ_TEST_CHECK(n == 4);
    for (size_t i = 0; i < n && i < 4; i++) {
        MOQ_TEST_CHECK(out[i].group == 0 && out[i].object == i);
        MOQ_TEST_CHECK(!out[i].gap);
    }
    MOQ_TEST_CHECK(out[2].path == 1 && out[3].path == 0);
    moq_media_merge_path_stats_t s0 = stats(m, 0), s1 = stats(m, 1);
    MOQ_TEST_CHECK_EQ_U64(s0.offered, 3);
    MOQ_TEST_CHECK_EQ_U64(s0.delivered, 3);
    MOQ_TEST_CHECK_EQ_U64(s1.offered, 4);
    MOQ_TEST_CHECK_EQ_U64(s1.delivered, 1);
    MOQ_TEST_CHECK_EQ_U64(s1.duplicates, 3);  /* (0,0) and (0,1) delivered, (0,3) held */
    moq_media_merge_destroy(m);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("other_path_fills_a_hole");
}

/* A copy that comes after its Location was given up is late, not delivered,
 * and the merged stream reports the gap once. */
static void test_late_copy_after_the_window(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_media_merge_t *m = make_merge(&alloc, 16);
    offer(m, &alloc, 0, 100, 0, 0, true);
    offer(m, &alloc, 0, 101, 0, 2, true);   /* (0,1) missing on every path */
    out_t out[8];
    size_t n = drain(m, 102, out, 8);
    MOQ_TEST_CHECK(n == 1);
    n += drain(m, 101 + 1000, out + n, 8 - n);  /* the window expires */
    MOQ_TEST_CHECK(n == 2);
    MOQ_TEST_CHECK(out[1].object == 2 && out[1].gap);
    offer(m, &alloc, 1, 2000, 0, 1, true);  /* too late */
    MOQ_TEST_CHECK(drain(m, 2001, out, 8) == 0);
    MOQ_TEST_CHECK_EQ_U64(stats(m, 1).late, 1);
    MOQ_TEST_CHECK_EQ_U64(stats(m, 1).delivered, 0);
    moq_media_merge_destroy(m);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("late_copy_after_the_window");
}

/* A Group's end from either path releases the next Group at once. */
static void test_end_group_releases_the_next_group(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_media_merge_t *m = make_merge(&alloc, 16);
    offer(m, &alloc, 0, 10, 0, 0, true);
    offer(m, &alloc, 0, 11, 0, 1, true);
    offer(m, &alloc, 0, 12, 1, 0, true);
    out_t out[8];
    size_t n = drain(m, 13, out, 8);
    MOQ_TEST_CHECK(n == 2);                 /* (1,0) waits: end of Group 0 unknown */
    MOQ_TEST_CHECK(moq_media_merge_end_group(m, 1, 0) == MOQ_OK);
    n += drain(m, 14, out + n, 8 - n);
    MOQ_TEST_CHECK(n == 3);
    MOQ_TEST_CHECK(out[2].group == 1 && out[2].object == 0 && !out[2].gap);
    moq_media_merge_destroy(m);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("end_group_releases_the_next_group");
}

/* An invalid first copy is dropped and counted; the slots bound what is held;
 * held Objects are released on destroy; bad arguments are refused. */
static void test_invalid_full_and_arguments(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_media_merge_t *m = make_merge(&alloc, 2);
    offer(m, &alloc, 0, 10, 0, 0, false);
    out_t out[4];
    MOQ_TEST_CHECK(drain(m, 11, out, 4) == 0);
    MOQ_TEST_CHECK_EQ_U64(stats(m, 0).invalid, 1);

    offer(m, &alloc, 0, 20, 0, 2, true);
    offer(m, &alloc, 0, 21, 0, 3, true);    /* both slots held, waiting for (0,1) */
    moq_media_object_t o = make_object(&alloc, 0, 4, true);
    MOQ_TEST_CHECK(moq_media_merge_offer(m, 0, 22, &o, sizeof(o)) == MOQ_ERR_WOULD_BLOCK);
    MOQ_TEST_CHECK(o.payload_ref != NULL);  /* still the caller's */
    MOQ_TEST_CHECK(moq_media_merge_offer(m, 2, 22, &o, sizeof(o)) == MOQ_ERR_INVAL);
    o.packaging = 0;
    MOQ_TEST_CHECK(moq_media_merge_offer(m, 0, 22, &o, sizeof(o)) == MOQ_ERR_INVAL);
    moq_media_object_cleanup(&o);
    moq_media_merge_destroy(m);             /* releases the two held Objects */
    MOQ_TEST_CHECK(as.balance == 0);

    moq_media_merge_cfg_t cfg;
    moq_media_merge_cfg_init(&cfg);
    cfg.path_count = 1;
    moq_media_merge_t *bad = NULL;
    MOQ_TEST_CHECK(moq_media_merge_create(&cfg, &alloc, &bad) == MOQ_ERR_INVAL && !bad);
    moq_media_merge_cfg_init(&cfg);
    MOQ_TEST_CHECK(cfg.mpeg2ts_packet_size == 188 && cfg.mpeg2ts_program_number == 0);
    cfg.mpeg2ts_packet_size = 100;
    MOQ_TEST_CHECK(moq_media_merge_create(&cfg, &alloc, &bad) == MOQ_ERR_INVAL && !bad);
    moq_media_merge_cfg_init(&cfg);
    cfg.struct_size = (uint32_t)MOQ_MEDIA_MERGE_CFG_V0_SIZE - 1;
    MOQ_TEST_CHECK(moq_media_merge_create(&cfg, &alloc, &bad) == MOQ_ERR_INVAL && !bad);
    moq_media_merge_cfg_init(&cfg);
    cfg.mpeg2ts_packet_size = 192;
    MOQ_TEST_CHECK(moq_media_merge_create(&cfg, &alloc, &bad) == MOQ_OK && bad);
    moq_media_merge_destroy(bad);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("invalid_full_and_arguments");
}

/* An Object stamped larger than this library's moq_media_object_t did not come
 * from this library's receiver: it is refused and stays the caller's, and
 * nothing past the library's own layout is read or written. */
static void test_larger_object_refused(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_media_merge_t *m = make_merge(&alloc, 2);
    struct { moq_media_object_t o; uint8_t tail[16]; } big;
    memset(&big, 0, sizeof(big));
    big.o = make_object(&alloc, 0, 0, true);
    big.o.struct_size = (uint32_t)sizeof(big);
    MOQ_TEST_CHECK(moq_media_merge_offer(m, 0, 10, &big.o, sizeof(big)) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(big.o.payload_ref != NULL);  /* still the caller's */
    MOQ_TEST_CHECK_EQ_U64(stats(m, 0).offered, 0);
    moq_media_object_cleanup(&big.o);
    moq_media_merge_destroy(m);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("larger_object_refused");
}

/* A stats record smaller than its first release's layout is refused, as the
 * other stats getters refuse one. */
static void test_stats_size_floor(void)
{
    test_alloc_state_t as = {0};
    moq_alloc_t alloc = test_allocator(&as);
    moq_media_merge_t *m = make_merge(&alloc, 2);
    moq_media_merge_path_stats_t s;
    memset(&s, 0xa5, sizeof(s));
    MOQ_TEST_CHECK(moq_media_merge_get_path_stats(m, 0, &s, sizeof(uint32_t)) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_media_merge_get_path_stats(m, 0, &s, sizeof(s) - 1) == MOQ_ERR_INVAL);
    MOQ_TEST_CHECK(moq_media_merge_get_path_stats(m, 0, &s, sizeof(s)) == MOQ_OK);
    MOQ_TEST_CHECK(s.struct_size == sizeof(s) && s.offered == 0);
    moq_media_merge_destroy(m);
    MOQ_TEST_CHECK(as.balance == 0);
    MOQ_TEST_PASS("stats_size_floor");
}

/* An allocator that refuses its Nth call and fills every block it hands out
 * with a non-zero pattern, so nothing may rely on fresh memory being zero. */
typedef struct {
    unsigned calls, fail_at;
    size_t live;
} dirty_alloc_t;

static void *dirty_alloc(size_t size, void *ctx)
{
    dirty_alloc_t *d = (dirty_alloc_t *)ctx;
    if (++d->calls == d->fail_at) return NULL;
    void *p = malloc(size);
    if (p) {
        memset(p, 0xa5, size);
        d->live++;
    }
    return p;
}

static void dirty_free(void *ptr, size_t size, void *ctx)
{
    (void)size;
    dirty_alloc_t *d = (dirty_alloc_t *)ctx;
    if (ptr) {
        d->live--;
        free(ptr);
    }
}

/* create fails cleanly at every allocation it makes: MOQ_ERR_NOMEM, no merge,
 * nothing left allocated, and no partly built state read. */
static void test_create_allocation_failures(void)
{
    moq_media_merge_cfg_t cfg;
    moq_media_merge_cfg_init(&cfg);
    cfg.slot_count = 2;
    dirty_alloc_t probe = {0};
    moq_alloc_t alloc = { .ctx = &probe, .alloc = dirty_alloc, .free = dirty_free };
    moq_media_merge_t *m = NULL;
    MOQ_TEST_CHECK(moq_media_merge_create(&cfg, &alloc, &m) == MOQ_OK && m);
    moq_media_merge_destroy(m);
    MOQ_TEST_CHECK(probe.live == 0 && probe.calls >= 2);
    for (unsigned n = 1; n <= probe.calls; n++) {
        dirty_alloc_t d = { .fail_at = n };
        alloc.ctx = &d;
        m = (moq_media_merge_t *)(uintptr_t)1;
        MOQ_TEST_CHECK(moq_media_merge_create(&cfg, &alloc, &m) == MOQ_ERR_NOMEM);
        MOQ_TEST_CHECK(m == NULL);
        MOQ_TEST_CHECK(d.live == 0);
    }
    MOQ_TEST_PASS("create_allocation_failures");
}

int main(void)
{
    test_other_path_fills_a_hole();
    test_late_copy_after_the_window();
    test_end_group_releases_the_next_group();
    test_invalid_full_and_arguments();
    test_larger_object_refused();
    test_stats_size_floor();
    test_create_allocation_failures();
    fprintf(stderr, "media_merge: %d failure(s)\n", failures);
    return failures ? 1 : 0;
}
