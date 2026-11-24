/*
 * The receiver's in-group ordering stage: fake items that carry their own
 * (Group, Object), a counting allocator that must end at zero, and exact
 * release sequences, gap reports, totals and deadlines.
 */
#include "media_receiver_order.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

#define CHECK_U64(actual, expected) do { \
    uint64_t a_ = (uint64_t)(actual), e_ = (uint64_t)(expected); \
    if (a_ != e_) { \
        fprintf(stderr, "FAIL %s:%d: %s == %llu, expected %llu\n", \
                __FILE__, __LINE__, #actual, (unsigned long long)a_, \
                (unsigned long long)e_); \
        failures++; \
    } \
} while (0)

#define CHECK_RC(actual, expected) do { \
    int a_ = (int)(actual), e_ = (int)(expected); \
    if (a_ != e_) { \
        fprintf(stderr, "FAIL %s:%d: %s == %d, expected %d\n", \
                __FILE__, __LINE__, #actual, a_, e_); \
        failures++; \
    } \
} while (0)

#define W  300000u      /* window, us */
#define T0 1000000u     /* the first now_us of every test */
#define M  UINT64_MAX   /* the largest Object ID (vi64) */

/* -- Counting allocator ------------------------------------------------ */

#define BLOCKS_MAX 4

typedef struct counting_alloc {
    void  *ptr[BLOCKS_MAX];
    size_t size[BLOCKS_MAX];
    size_t live;        /* blocks outstanding */
    size_t calls;       /* alloc calls */
    size_t fail_call;   /* fail this call (1-based); 0: never */
    size_t max_size;    /* refuse larger requests; 0: no cap */
} counting_alloc_t;

static void *ca_alloc(size_t size, void *ctx)
{
    counting_alloc_t *ca = ctx;
    ca->calls++;
    if (ca->fail_call == ca->calls) return NULL;
    if (ca->max_size && size > ca->max_size) return NULL;
    if (ca->live == BLOCKS_MAX) {
        fprintf(stderr, "FAIL: more than %d live blocks\n", BLOCKS_MAX);
        failures++;
        return NULL;
    }
    void *p = malloc(size);
    if (!p) return NULL;
    ca->ptr[ca->live] = p;
    ca->size[ca->live] = size;
    ca->live++;
    return p;
}

static void ca_free(void *p, size_t size, void *ctx)
{
    counting_alloc_t *ca = ctx;
    size_t i = 0;
    while (i < ca->live && ca->ptr[i] != p) i++;
    CHECK(i < ca->live);
    if (i == ca->live) return;
    CHECK_U64(ca->size[i], size);
    free(p);
    ca->live--;
    ca->ptr[i] = ca->ptr[ca->live];
    ca->size[i] = ca->size[ca->live];
}

/* -- Fake items -------------------------------------------------------- */

typedef struct fake_item {
    uint64_t group;
    uint64_t object;
    unsigned delivered;   /* times polled out */
    unsigned released;    /* times handed to a release callback */
} fake_item_t;

#define POOL_MAX 1024
static fake_item_t pool[POOL_MAX];
static size_t pool_used;

static fake_item_t *item(uint64_t g, uint64_t k)
{
    if (pool_used == POOL_MAX) {
        fprintf(stderr, "FAIL: item pool exhausted\n");
        exit(2);
    }
    fake_item_t *it = &pool[pool_used++];
    memset(it, 0, sizeof(*it));
    it->group = g;
    it->object = k;
    return it;
}

typedef struct release_log {
    size_t calls;
} release_log_t;

static void on_release(void *ctx, void *p)
{
    release_log_t *log = ctx;
    fake_item_t *it = p;
    log->calls++;
    it->released++;
}

static void count_item(void *ctx, void *p)
{
    (void)p;
    (*(size_t *)ctx)++;
}

static size_t held_count(const receiver_order_t *o)
{
    size_t n = 0;
    receiver_order_for_each(o, count_item, &n);
    return n;
}

/* -- Fixture ----------------------------------------------------------- */

typedef struct fixture {
    counting_alloc_t ca;
    moq_alloc_t      alloc;
    receiver_order_t *o;
    release_log_t    rel;
} fixture_t;

static receiver_order_t *fx_open(fixture_t *f, uint32_t groups,
                                 uint32_t slots, uint64_t window)
{
    memset(f, 0, sizeof(*f));
    pool_used = 0;
    f->alloc = (moq_alloc_t){ .ctx = &f->ca, .alloc = ca_alloc,
                              .free = ca_free };
    receiver_order_cfg_t cfg = { .max_groups = groups, .max_slots = slots,
                                 .window_us = window };
    CHECK_RC(receiver_order_create(&f->alloc, &cfg, &f->o), MOQ_OK);
    CHECK_U64(f->ca.calls, 1);
    if (!f->o) {
        fprintf(stderr, "FAIL: no stage\n");
        exit(2);
    }
    return f->o;
}

/* Every item is polled out or released at most once, in total; the
 * allocator ends at zero. */
static void check_items(void)
{
    for (size_t i = 0; i < pool_used; i++) {
        if (pool[i].delivered + pool[i].released > 1) {
            fprintf(stderr, "FAIL: item (%llu, %llu) delivered %u, "
                    "released %u\n", (unsigned long long)pool[i].group,
                    (unsigned long long)pool[i].object, pool[i].delivered,
                    pool[i].released);
            failures++;
        }
    }
}

static void fx_close(fixture_t *f)
{
    receiver_order_destroy(f->o, on_release, &f->rel);
    f->o = NULL;
    CHECK_U64(f->ca.live, 0);
    check_items();
}

/* -- Release expectations ---------------------------------------------- */

#define F_STATUS 1u
#define F_GAP    2u
#define F_TAIL   4u   /* a gap that is only an unconfirmed tail */

static void expect_rel(int line, receiver_order_t *o, uint64_t now,
                       uint64_t g, uint64_t k, unsigned flags,
                       uint64_t gg, uint64_t go)
{
    receiver_order_delivery_t d;
    memset(&d, 0x5a, sizeof(d));
    moq_result_t rc = receiver_order_poll(o, now, &d);
    if (rc != MOQ_OK) {
        fprintf(stderr, "FAIL line %d: poll at %llu returned %d, expected "
                "(%llu, %llu)\n", line, (unsigned long long)now, (int)rc,
                (unsigned long long)g, (unsigned long long)k);
        failures++;
        return;
    }
    bool gap = (flags & (F_GAP | F_TAIL)) != 0;
    bool tail = (flags & F_TAIL) != 0;
    bool status = (flags & F_STATUS) != 0;
    fake_item_t *it = d.ctx;
    bool ok = it && it->group == g && it->object == k &&
              d.group == g && d.object == k && d.is_status == status &&
              d.gap == gap && d.gap_tail_unknown == tail &&
              d.gap_group == (gap ? gg : 0) &&
              d.gap_object == (gap ? go : 0);
    if (it) it->delivered++;
    if (!ok) {
        fprintf(stderr, "FAIL line %d: released (%llu, %llu) status %d gap %d "
                "at (%llu, %llu) tail %d; expected (%llu, %llu) status %d "
                "gap %d at (%llu, %llu) tail %d\n", line,
                (unsigned long long)d.group, (unsigned long long)d.object,
                (int)d.is_status, (int)d.gap,
                (unsigned long long)d.gap_group,
                (unsigned long long)d.gap_object, (int)d.gap_tail_unknown,
                (unsigned long long)g, (unsigned long long)k, (int)status,
                (int)gap, (unsigned long long)(gap ? gg : 0),
                (unsigned long long)(gap ? go : 0), (int)tail);
        failures++;
    }
}

static void expect_none(int line, receiver_order_t *o, uint64_t now)
{
    receiver_order_delivery_t d;
    memset(&d, 0x5a, sizeof(d));
    moq_result_t rc = receiver_order_poll(o, now, &d);
    if (rc == MOQ_DONE) {
        unsigned char probe;
        memcpy(&probe, &d, 1);
        if (probe != 0x5a) {
            fprintf(stderr, "FAIL line %d: MOQ_DONE wrote *out\n", line);
            failures++;
        }
        return;
    }
    fprintf(stderr, "FAIL line %d: poll at %llu returned %d", line,
            (unsigned long long)now, (int)rc);
    if (rc == MOQ_OK) {
        fake_item_t *it = d.ctx;
        if (it) it->delivered++;
        fprintf(stderr, " with (%llu, %llu)", (unsigned long long)d.group,
                (unsigned long long)d.object);
    }
    fprintf(stderr, ", expected nothing due\n");
    failures++;
}

#define REL(o, now, g, k) \
    expect_rel(__LINE__, (o), (now), (g), (k), 0u, 0u, 0u)
#define REL_GAP(o, now, g, k, gg, go) \
    expect_rel(__LINE__, (o), (now), (g), (k), F_GAP, (gg), (go))
#define REL_TAIL(o, now, g, k, gg, go) \
    expect_rel(__LINE__, (o), (now), (g), (k), F_TAIL, (gg), (go))
#define REL_ST(o, now, g, k) \
    expect_rel(__LINE__, (o), (now), (g), (k), F_STATUS, 0u, 0u)
#define REL_ST_GAP(o, now, g, k, gg, go) \
    expect_rel(__LINE__, (o), (now), (g), (k), F_STATUS | F_GAP, (gg), (go))
#define NONE(o, now) expect_none(__LINE__, (o), (now))

#define OFFER(o, now, g, k) \
    receiver_order_offer((o), (now), (g), (k), item((g), (k)))
#define STATUS(o, now, g, e) \
    receiver_order_offer_status((o), (now), (g), (e), item((g), (e)))
#define FIN(o, now, g, l)  receiver_order_fin((o), (now), (g), (l))
#define LOSS(o, now, g, c) receiver_order_loss((o), (now), (g), (c))
#define DEADLINE(o)        receiver_order_next_deadline_us(o)
#define GAPS(o)            receiver_order_gaps_total(o)
#define FORCED(o)          receiver_order_forced_total(o)

/* -- Construction and arguments ---------------------------------------- */

static void test_create_and_null_arguments(void)
{
    counting_alloc_t ca;
    memset(&ca, 0, sizeof(ca));
    moq_alloc_t a = { .ctx = &ca, .alloc = ca_alloc, .free = ca_free };
    receiver_order_cfg_t cfg = { .max_groups = 4, .max_slots = 64,
                                 .window_us = W };
    receiver_order_t *sentinel = (receiver_order_t *)(void *)&ca;
    receiver_order_t *o = sentinel;

    CHECK_RC(receiver_order_create(NULL, &cfg, &o), MOQ_ERR_INVAL);
    CHECK(o == NULL);
    o = sentinel;
    CHECK_RC(receiver_order_create(&a, NULL, &o), MOQ_ERR_INVAL);
    CHECK(o == NULL);
    CHECK_RC(receiver_order_create(&a, &cfg, NULL), MOQ_ERR_INVAL);
    cfg.max_groups = 0;
    CHECK_RC(receiver_order_create(&a, &cfg, &o), MOQ_ERR_INVAL);
    cfg.max_groups = 4;
    cfg.max_slots = 0;
    CHECK_RC(receiver_order_create(&a, &cfg, &o), MOQ_ERR_INVAL);
    cfg.max_slots = 64;
    moq_alloc_t no_alloc = a;
    no_alloc.alloc = NULL;
    CHECK_RC(receiver_order_create(&no_alloc, &cfg, &o), MOQ_ERR_INVAL);
    moq_alloc_t no_free = a;
    no_free.free = NULL;
    CHECK_RC(receiver_order_create(&no_free, &cfg, &o), MOQ_ERR_INVAL);
    CHECK_U64(ca.calls, 0);

    /* Allocation failure: nothing left behind. */
    ca.fail_call = 1;
    o = sentinel;
    CHECK_RC(receiver_order_create(&a, &cfg, &o), MOQ_ERR_NOMEM);
    CHECK(o == NULL);
    CHECK_U64(ca.calls, 1);
    CHECK_U64(ca.live, 0);
    ca.fail_call = 0;

    /* The largest tables: on a 64-bit size_t they fit and the allocation
     * is refused; on a 32-bit size_t they do not fit. */
    ca.max_size = (size_t)1 << 20;
    receiver_order_cfg_t big = { .max_groups = UINT32_MAX,
                                 .max_slots = UINT32_MAX, .window_us = W };
    o = sentinel;
#if SIZE_MAX <= UINT32_MAX
    CHECK_RC(receiver_order_create(&a, &big, &o), MOQ_ERR_INVAL);
#else
    CHECK_RC(receiver_order_create(&a, &big, &o), MOQ_ERR_NOMEM);
#endif
    CHECK(o == NULL);
    CHECK_U64(ca.live, 0);
    ca.max_size = 0;

    /* One allocation, released by destroy. */
    size_t calls = ca.calls;
    CHECK_RC(receiver_order_create(&a, &cfg, &o), MOQ_OK);
    CHECK(o != NULL && o != sentinel);
    CHECK_U64(ca.calls, calls + 1);
    CHECK_U64(ca.live, 1);
    if (!o || o == sentinel) return;

    fake_item_t dummy;
    memset(&dummy, 0, sizeof(dummy));
    receiver_order_delivery_t d;
    CHECK_RC(receiver_order_offer(NULL, T0, 0, 0, &dummy), MOQ_ERR_INVAL);
    CHECK_RC(receiver_order_offer(o, T0, 0, 0, NULL), MOQ_ERR_INVAL);
    CHECK_RC(receiver_order_offer_status(NULL, T0, 0, 0, &dummy),
             MOQ_ERR_INVAL);
    CHECK_RC(receiver_order_offer_status(o, T0, 0, 0, NULL), MOQ_ERR_INVAL);
    CHECK_RC(receiver_order_fin(NULL, T0, 0, 0), MOQ_ERR_INVAL);
    CHECK_RC(receiver_order_loss(NULL, T0, 0, 0), MOQ_ERR_INVAL);
    CHECK_RC(receiver_order_force(NULL, T0), MOQ_ERR_INVAL);
    CHECK_RC(receiver_order_poll(NULL, T0, &d), MOQ_ERR_INVAL);
    CHECK_RC(receiver_order_poll(o, T0, NULL), MOQ_ERR_INVAL);
    CHECK_U64(receiver_order_next_deadline_us(NULL), UINT64_MAX);
    CHECK(!receiver_order_holds(NULL));
    CHECK_U64(receiver_order_gaps_total(NULL), 0);
    CHECK_U64(receiver_order_forced_total(NULL), 0);
    size_t n = 0;
    receiver_order_for_each(NULL, count_item, &n);
    CHECK_U64(n, 0);
    receiver_order_for_each(o, NULL, NULL);
    receiver_order_finish(NULL);
    receiver_order_reset(NULL, NULL, NULL);
    receiver_order_destroy(NULL, NULL, NULL);

    /* A fresh stage holds nothing, waits for nothing, cannot be forced. */
    CHECK(!receiver_order_holds(o));
    CHECK_U64(held_count(o), 0);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    NONE(o, T0);
    CHECK_RC(receiver_order_force(o, T0), MOQ_DONE);
    CHECK_U64(GAPS(o), 0);
    CHECK_U64(FORCED(o), 0);

    receiver_order_destroy(o, NULL, NULL);
    CHECK_U64(ca.live, 0);
}

/* -- Basic order ------------------------------------------------------- */

static void test_in_order_release(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    for (uint64_t k = 0; k < 4; k++) {
        CHECK_RC(OFFER(o, T0 + k, 10, k), MOQ_OK);
        CHECK(receiver_order_holds(o));          /* due, not yet polled */
        REL(o, T0 + k, 10, k);
        NONE(o, T0 + k);
        CHECK_U64(DEADLINE(o), UINT64_MAX);      /* idle */
    }
    CHECK(!receiver_order_holds(o));
    CHECK_U64(GAPS(o), 0);
    CHECK_U64(FORCED(o), 0);
    fx_close(&f);
}

static void test_reorder_within_group(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    NONE(o, T0);
    CHECK_U64(DEADLINE(o), T0 + W);
    CHECK_RC(OFFER(o, T0 + 1, 10, 0), MOQ_OK);
    REL(o, T0 + 1, 10, 0);
    NONE(o, T0 + 1);
    CHECK_U64(DEADLINE(o), T0 + 1 + W);          /* a new stall at Object 1 */
    CHECK_RC(OFFER(o, T0 + 2, 10, 3), MOQ_OK);
    NONE(o, T0 + 2);
    CHECK_U64(DEADLINE(o), T0 + 1 + W);          /* armed once per stall */
    CHECK_RC(OFFER(o, T0 + 3, 10, 1), MOQ_OK);
    CHECK_U64(DEADLINE(o), UINT64_MAX);          /* due now: nothing armed */
    REL(o, T0 + 3, 10, 1);
    REL(o, T0 + 3, 10, 2);
    REL(o, T0 + 3, 10, 3);
    NONE(o, T0 + 3);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_U64(GAPS(o), 0);
    fx_close(&f);

    /* A zero window gives the run up at once. */
    o = fx_open(&f, 4, 64, 0);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    CHECK_U64(DEADLINE(o), T0);
    REL_GAP(o, T0, 10, 1, 10, 0);
    NONE(o, T0);
    CHECK_U64(GAPS(o), 1);
    fx_close(&f);
}

static void test_duplicate_and_late_refusals(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_ERR_PROTO);   /* already held */
    CHECK_U64(held_count(o), 1);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    REL(o, T0, 10, 1);
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_ERR_PROTO);   /* below the frontier */
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_ERR_PROTO);
    CHECK_RC(STATUS(o, T0, 10, 1), MOQ_ERR_PROTO);
    /* Group 9 is below a current Group that has started releasing. */
    CHECK_RC(OFFER(o, T0, 9, 5), MOQ_ERR_PROTO);
    CHECK_RC(FIN(o, T0, 9, 5), MOQ_DONE);
    CHECK_RC(LOSS(o, T0, 9, 5), MOQ_DONE);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    REL(o, T0, 10, 2);
    NONE(o, T0);
    CHECK_U64(held_count(o), 0);
    CHECK_U64(GAPS(o), 0);
    fx_close(&f);
}

static void test_above_final_refusals(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(FIN(o, T0, 10, 2), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 10, 3), MOQ_ERR_PROTO);   /* above fin_last */
    for (uint64_t k = 0; k <= 2; k++) {
        CHECK_RC(OFFER(o, T0, 10, k), MOQ_OK);
        REL(o, T0, 10, k);
    }
    NONE(o, T0);

    CHECK_RC(STATUS(o, T0, 11, 2), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 11, 2), MOQ_ERR_PROTO);   /* the status holds it */
    CHECK_RC(OFFER(o, T0, 11, 3), MOQ_ERR_PROTO);   /* above eog_marker */
    CHECK_RC(OFFER(o, T0, 11, 0), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 11, 1), MOQ_OK);
    REL(o, T0, 11, 0);
    REL(o, T0, 11, 1);
    REL_ST(o, T0, 11, 2);
    NONE(o, T0);

    CHECK_RC(FIN(o, T0, 12, 4), MOQ_OK);
    CHECK_RC(STATUS(o, T0, 12, 4), MOQ_ERR_PROTO);  /* at fin_last */
    CHECK_RC(STATUS(o, T0, 12, 3), MOQ_ERR_PROTO);  /* below it */
    CHECK_RC(STATUS(o, T0, 12, 5), MOQ_OK);
    CHECK_U64(held_count(o), 1);
    fx_close(&f);
    CHECK_U64(f.rel.calls, 1);                       /* the status item */
}

/* -- A later Group's terminal while the current Group is blocked --------- */

static void feed_blocked_g_and_complete_g1(receiver_order_t *o)
{
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    REL(o, T0, 10, 1);
    CHECK_RC(OFFER(o, T0 + 10, 10, 3), MOQ_OK);    /* blocked on 2 */
    NONE(o, T0 + 10);
    CHECK_U64(DEADLINE(o), T0 + 10 + W);
    CHECK_RC(OFFER(o, T0 + 20, 10, 4), MOQ_OK);
    CHECK_RC(FIN(o, T0 + 20, 10, 4), MOQ_OK);
    NONE(o, T0 + 20);
    for (uint64_t k = 0; k <= 3; k++) {
        CHECK_RC(OFFER(o, T0 + 30 + k, 11, k), MOQ_OK);
        NONE(o, T0 + 30 + k);
    }
    CHECK_RC(FIN(o, T0 + 40, 11, 3), MOQ_OK);
    NONE(o, T0 + 40);
    CHECK_U64(DEADLINE(o), T0 + 10 + W);            /* the same stall */
}

static void test_later_group_terminal_while_blocked_then_silence(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    feed_blocked_g_and_complete_g1(o);
    NONE(o, T0 + 10 + W - 1);
    REL_GAP(o, T0 + 10 + W, 10, 3, 10, 2);
    REL(o, T0 + 10 + W, 10, 4);
    for (uint64_t k = 0; k <= 3; k++) REL(o, T0 + 10 + W, 11, k);
    NONE(o, T0 + 10 + W);
    CHECK_U64(GAPS(o), 1);
    CHECK_U64(FORCED(o), 0);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK(!receiver_order_holds(o));
    NONE(o, T0 + 10 * W);                           /* silence: no new gap */
    CHECK_U64(GAPS(o), 1);
    CHECK_RC(OFFER(o, T0 + 10 * W, 10, 2), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0 + 10 * W, 11, 4), MOQ_ERR_PROTO);
    fx_close(&f);
}

static void test_later_group_terminal_while_blocked_recovered(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    feed_blocked_g_and_complete_g1(o);
    uint64_t t = T0 + 10 + W / 2;
    CHECK_RC(OFFER(o, t, 10, 2), MOQ_OK);
    REL(o, t, 10, 2);
    REL(o, t, 10, 3);
    REL(o, t, 10, 4);
    for (uint64_t k = 0; k <= 3; k++) REL(o, t, 11, k);
    NONE(o, t);
    NONE(o, T0 + 10 * W);
    CHECK_U64(GAPS(o), 0);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    fx_close(&f);
}

static void test_completing_g_keeps_g1_facts(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    CHECK_RC(FIN(o, T0, 11, 2), MOQ_OK);
    CHECK_RC(LOSS(o, T0, 11, 1), MOQ_OK);
    NONE(o, T0);               /* Group 10's Object 1 is not the lost one */
    CHECK_RC(FIN(o, T0, 10, 1), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    REL(o, T0, 10, 1);         /* no gap: the loss is Group 11's */
    NONE(o, T0);               /* 10 retires; 11 waits for its Object 0 */
    CHECK_U64(DEADLINE(o), T0 + W);
    CHECK_RC(OFFER(o, T0 + 1, 11, 0), MOQ_OK);
    REL(o, T0 + 1, 11, 0);
    NONE(o, T0 + 1);           /* skips the lost Object 1, waits for 2 */
    CHECK_RC(OFFER(o, T0 + 2, 11, 2), MOQ_OK);
    REL_GAP(o, T0 + 2, 11, 2, 11, 1);
    NONE(o, T0 + 2);
    CHECK_U64(GAPS(o), 0);     /* a lost Object is reported, not counted */
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    fx_close(&f);
}

/* -- Terminal facts ---------------------------------------------------- */

static void test_terminal_before_first_object(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    /* The record opens on the terminal, frontier 0, nothing held: a known
     * missing position, so the deadline is armed. */
    CHECK_RC(FIN(o, T0, 10, 2), MOQ_OK);
    CHECK(!receiver_order_holds(o));
    CHECK_U64(DEADLINE(o), T0 + W);
    CHECK_RC(OFFER(o, T0 + 1, 10, 1), MOQ_OK);
    NONE(o, T0 + 1);
    CHECK_U64(DEADLINE(o), T0 + W);
    CHECK_RC(OFFER(o, T0 + 1, 10, 0), MOQ_OK);
    REL(o, T0 + 1, 10, 0);
    REL(o, T0 + 1, 10, 1);
    NONE(o, T0 + 1);
    CHECK_U64(DEADLINE(o), T0 + 1 + W);
    CHECK_RC(OFFER(o, T0 + 2, 10, 2), MOQ_OK);
    REL(o, T0 + 2, 10, 2);
    NONE(o, T0 + 2);
    CHECK_U64(DEADLINE(o), UINT64_MAX);

    /* Group 21's terminal arrives while Group 20 is current. */
    CHECK_RC(OFFER(o, T0 + 3, 20, 0), MOQ_OK);
    REL(o, T0 + 3, 20, 0);
    CHECK_RC(FIN(o, T0 + 3, 21, 2), MOQ_OK);
    NONE(o, T0 + 3);
    CHECK_RC(FIN(o, T0 + 4, 20, 0), MOQ_OK);
    NONE(o, T0 + 4);           /* 20 retires; 21 current, nothing held */
    CHECK_U64(DEADLINE(o), T0 + 4 + W);
    CHECK_RC(OFFER(o, T0 + 5, 21, 2), MOQ_OK);
    NONE(o, T0 + 5);
    CHECK_RC(OFFER(o, T0 + 5, 21, 0), MOQ_OK);
    REL(o, T0 + 5, 21, 0);
    NONE(o, T0 + 5);
    CHECK_RC(OFFER(o, T0 + 6, 21, 1), MOQ_OK);
    REL(o, T0 + 6, 21, 1);
    REL(o, T0 + 6, 21, 2);
    NONE(o, T0 + 6);
    CHECK_U64(GAPS(o), 0);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    fx_close(&f);
}

static void test_eog_status_first(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    CHECK_RC(STATUS(o, T0, 11, 3), MOQ_OK);        /* Group 11's first */
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0, 11, 1), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 11, 0), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 11, 2), MOQ_OK);
    NONE(o, T0);
    CHECK_RC(FIN(o, T0, 10, 1), MOQ_OK);
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    REL(o, T0, 10, 1);
    REL(o, T0, 11, 0);
    REL(o, T0, 11, 1);
    REL(o, T0, 11, 2);
    REL_ST(o, T0, 11, 3);
    NONE(o, T0);
    /* An empty Group: its status at 0 is all of it. */
    CHECK_RC(STATUS(o, T0, 12, 0), MOQ_OK);
    REL_ST(o, T0, 12, 0);
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0, 12, 0), MOQ_ERR_PROTO);
    CHECK_U64(GAPS(o), 0);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK(!receiver_order_holds(o));
    fx_close(&f);
}

static void test_fin_and_status_both(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    REL(o, T0, 10, 1);
    CHECK_RC(FIN(o, T0, 10, 2), MOQ_OK);           /* t = 2 */
    CHECK_RC(STATUS(o, T0, 10, 5), MOQ_OK);        /* e = t + 3 */
    CHECK_RC(OFFER(o, T0, 10, 3), MOQ_ERR_PROTO);  /* known absent */
    CHECK_RC(OFFER(o, T0, 10, 4), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    REL(o, T0, 10, 2);
    REL_ST(o, T0, 10, 5);      /* 3 and 4 skipped as absent: no gap */
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0, 11, 0), MOQ_OK);
    REL(o, T0, 11, 0);         /* nothing carried forward either */
    CHECK_U64(GAPS(o), 0);

    /* Completed by its FIN alone, a Group takes no later marker, whether
     * or not a poll has retired it yet. */
    CHECK_RC(FIN(o, T0, 11, 0), MOQ_OK);
    CHECK_RC(STATUS(o, T0, 11, 3), MOQ_ERR_PROTO);
    NONE(o, T0);
    CHECK_RC(STATUS(o, T0, 11, 3), MOQ_ERR_PROTO);
    CHECK(!receiver_order_holds(o));
    fx_close(&f);
}

static void test_conflicting_terminals(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(FIN(o, T0, 10, 5), MOQ_OK);
    CHECK_RC(FIN(o, T0, 10, 7), MOQ_DONE);         /* a different fin_last */
    CHECK_RC(FIN(o, T0, 10, 5), MOQ_OK);           /* the same one */
    CHECK_RC(OFFER(o, T0, 10, 6), MOQ_ERR_PROTO);  /* above final */
    CHECK_RC(STATUS(o, T0, 10, 4), MOQ_ERR_PROTO); /* below fin_last */
    CHECK_RC(STATUS(o, T0, 10, 5), MOQ_ERR_PROTO); /* at fin_last */
    CHECK_RC(STATUS(o, T0, 10, 8), MOQ_OK);        /* absent: 6, 7 */
    CHECK_RC(STATUS(o, T0, 10, 9), MOQ_ERR_PROTO); /* another marker */
    CHECK_RC(STATUS(o, T0, 10, 8), MOQ_ERR_PROTO); /* the same, held */
    CHECK_RC(OFFER(o, T0, 10, 7), MOQ_ERR_PROTO);  /* known absent */
    CHECK_RC(OFFER(o, T0, 10, 8), MOQ_ERR_PROTO);  /* the status's place */

    CHECK_RC(STATUS(o, T0, 11, 3), MOQ_OK);
    CHECK_RC(FIN(o, T0, 11, 3), MOQ_DONE);         /* last >= eog_marker */
    CHECK_RC(FIN(o, T0, 11, 4), MOQ_DONE);
    CHECK_RC(FIN(o, T0, 11, 2), MOQ_OK);           /* no absent position */
    CHECK_RC(OFFER(o, T0, 11, 3), MOQ_ERR_PROTO);

    CHECK_RC(OFFER(o, T0, 12, 4), MOQ_OK);
    CHECK_RC(FIN(o, T0, 12, 3), MOQ_DONE);         /* Object 4 held above */
    CHECK_RC(LOSS(o, T0, 12, 6), MOQ_OK);
    CHECK_RC(FIN(o, T0, 12, 5), MOQ_DONE);         /* the loss above */
    CHECK_RC(FIN(o, T0, 12, 6), MOQ_DONE);         /* the loss at last */
    CHECK_RC(STATUS(o, T0, 12, 5), MOQ_ERR_PROTO); /* below the loss */
    CHECK_RC(STATUS(o, T0, 12, 7), MOQ_OK);
    CHECK_RC(FIN(o, T0, 12, 7), MOQ_DONE);
    CHECK_RC(OFFER(o, T0, 12, 7), MOQ_ERR_PROTO);

    /* The facts that stood release exactly. */
    for (uint64_t k = 0; k <= 5; k++) {
        CHECK_RC(OFFER(o, T0, 10, k), MOQ_OK);
        REL(o, T0, 10, k);
    }
    REL_ST(o, T0, 10, 8);
    for (uint64_t k = 0; k <= 2; k++) {
        CHECK_RC(OFFER(o, T0, 11, k), MOQ_OK);
        REL(o, T0, 11, k);
    }
    REL_ST(o, T0, 11, 3);
    NONE(o, T0);
    CHECK_U64(GAPS(o), 0);
    CHECK_U64(held_count(o), 2);                   /* Group 12: 4 and 7 */
    fx_close(&f);
    CHECK_U64(f.rel.calls, 2);
}

/* -- The largest Object ID --------------------------------------------- */

static void test_max_object_id(void)
{
    fixture_t f;

    /* fin_last UINT64_MAX: releasing it exhausts the frontier. */
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    CHECK_RC(FIN(o, T0, 10, M), MOQ_OK);
    NONE(o, T0);
    CHECK_U64(DEADLINE(o), T0 + W);
    CHECK_RC(OFFER(o, T0, 10, M), MOQ_OK);
    NONE(o, T0 + W - 1);
    REL_GAP(o, T0 + W, 10, M, 10, 1);
    CHECK_U64(GAPS(o), M - 1);                      /* Objects 1 .. M - 1 */
    /* Exhausted and awaiting retirement: never delivered again. */
    CHECK_RC(OFFER(o, T0 + W, 10, M), MOQ_ERR_PROTO);
    CHECK_RC(STATUS(o, T0 + W, 10, M), MOQ_ERR_PROTO);
    CHECK_RC(FIN(o, T0 + W, 10, M), MOQ_DONE);
    CHECK_RC(LOSS(o, T0 + W, 10, M), MOQ_DONE);
    NONE(o, T0 + W);
    CHECK_RC(OFFER(o, T0 + W, 10, M), MOQ_ERR_PROTO);
    CHECK(!receiver_order_holds(o));
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    fx_close(&f);

    /* A status marker at UINT64_MAX, behind a FIN three below it. */
    o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 11, 0), MOQ_OK);
    REL(o, T0, 11, 0);
    CHECK_RC(FIN(o, T0, 11, M - 3), MOQ_OK);
    CHECK_RC(STATUS(o, T0, 11, M), MOQ_OK);        /* absent: M-2, M-1 */
    CHECK_RC(OFFER(o, T0, 11, M - 1), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0, 11, M - 3), MOQ_OK);
    NONE(o, T0 + W - 1);
    REL_GAP(o, T0 + W, 11, M - 3, 11, 1);
    REL_ST(o, T0 + W, 11, M);
    CHECK_U64(GAPS(o), M - 4);                      /* Objects 1 .. M - 4 */
    CHECK_RC(STATUS(o, T0 + W, 11, M), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0 + W, 11, M), MOQ_ERR_PROTO);
    NONE(o, T0 + W);
    CHECK_RC(STATUS(o, T0 + W, 11, M), MOQ_ERR_PROTO);
    /* The total saturates. */
    CHECK_RC(FIN(o, T0 + W, 12, M), MOQ_OK);
    NONE(o, T0 + 2 * W);
    CHECK_U64(GAPS(o), UINT64_MAX);
    fx_close(&f);

    /* A run from 0 through UINT64_MAX is 2^64 positions: it does not wrap
     * to zero, and the frontier does not wrap back to 0. */
    o = fx_open(&f, 4, 64, W);
    CHECK_RC(FIN(o, T0, 5, M), MOQ_OK);
    CHECK_U64(DEADLINE(o), T0 + W);
    NONE(o, T0 + W);
    CHECK_U64(GAPS(o), UINT64_MAX);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_RC(OFFER(o, T0 + W, 5, 0), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0 + W, 6, 0), MOQ_OK);
    REL_GAP(o, T0 + W, 6, 0, 5, 0);
    fx_close(&f);
}

/* -- Interleaved Groups ------------------------------------------------ */

static void test_interleaved_groups_no_drop(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    uint64_t t = T0;
    for (uint64_t k = 0; k <= 3; k++, t += 1000) {
        CHECK_RC(OFFER(o, t, 10, k), MOQ_OK);
        REL(o, t, 10, k);
        NONE(o, t);
    }
    for (uint64_t k = 0; k <= 3; k++, t += 1000) {
        CHECK_RC(OFFER(o, t, 11, k), MOQ_OK);
        NONE(o, t);                 /* held behind Group 10 */
    }
    for (uint64_t k = 4; k <= 5; k++, t += 1000) {
        CHECK_RC(OFFER(o, t, 10, k), MOQ_OK);
        REL(o, t, 10, k);
        NONE(o, t);
    }
    CHECK_RC(FIN(o, t, 10, 5), MOQ_OK);
    for (uint64_t k = 0; k <= 3; k++) REL(o, t, 11, k);
    NONE(o, t);
    CHECK_RC(FIN(o, t, 11, 3), MOQ_OK);
    NONE(o, t);
    CHECK_U64(GAPS(o), 0);
    CHECK_U64(FORCED(o), 0);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK(!receiver_order_holds(o));
    fx_close(&f);
}

/* -- Boundary wait ----------------------------------------------------- */

static void feed_contiguous_g(receiver_order_t *o)
{
    for (uint64_t k = 0; k <= 4; k++) {
        CHECK_RC(OFFER(o, T0, 10, k), MOQ_OK);
        REL(o, T0, 10, k);
    }
    NONE(o, T0);
    CHECK_U64(DEADLINE(o), UINT64_MAX);            /* idle, no terminal */
}

static void test_boundary_wait_tail_arrives(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    feed_contiguous_g(o);
    uint64_t t1 = T0 + 1000;
    CHECK_RC(OFFER(o, t1, 11, 0), MOQ_OK);
    NONE(o, t1);
    CHECK_U64(DEADLINE(o), t1 + W);
    uint64_t t2 = t1 + 100000;                       /* 100 ms later */
    CHECK_RC(OFFER(o, t2, 10, 5), MOQ_OK);
    REL(o, t2, 10, 5);                               /* before Group 11 */
    NONE(o, t2);
    CHECK_U64(DEADLINE(o), t2 + W);                  /* a new stall at 6 */
    NONE(o, t2 + W - 1);
    REL_TAIL(o, t2 + W, 11, 0, 10, 6);
    NONE(o, t2 + W);
    CHECK_U64(GAPS(o), 0);
    fx_close(&f);
}

static void test_boundary_wait_no_tail(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    feed_contiguous_g(o);
    uint64_t t1 = T0 + 1000;
    CHECK_RC(OFFER(o, t1, 11, 0), MOQ_OK);
    NONE(o, t1);
    CHECK_U64(DEADLINE(o), t1 + W);
    NONE(o, t1 + W - 1);
    REL_TAIL(o, t1 + W, 11, 0, 10, 5);
    NONE(o, t1 + W);
    CHECK_U64(GAPS(o), 0);                           /* not a known loss */
    CHECK_U64(FORCED(o), 0);
    CHECK_RC(OFFER(o, t1 + W, 10, 5), MOQ_ERR_PROTO);
    fx_close(&f);
}

static void test_boundary_wait_avoided_by_status(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    feed_contiguous_g(o);
    uint64_t t1 = T0 + 1000;
    CHECK_RC(OFFER(o, t1, 11, 0), MOQ_OK);
    CHECK_RC(STATUS(o, t1, 10, 5), MOQ_OK);
    REL_ST(o, t1, 10, 5);
    REL(o, t1, 11, 0);
    NONE(o, t1);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_U64(GAPS(o), 0);
    fx_close(&f);
}

static void test_gap_wait_excludes_boundary_wait(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0 + 1, 11, 0), MOQ_OK);
    NONE(o, T0 + 1);
    CHECK_U64(DEADLINE(o), T0 + W);   /* still the gap wait: 10 knows 2 */
    NONE(o, T0 + W - 1);
    REL_GAP(o, T0 + W, 10, 2, 10, 1); /* the run is given up; 10 stays */
    NONE(o, T0 + W);
    CHECK_U64(DEADLINE(o), T0 + 2 * W); /* now the boundary wait at 3 */
    CHECK_U64(held_count(o), 1);
    NONE(o, T0 + 2 * W - 1);
    REL_TAIL(o, T0 + 2 * W, 11, 0, 10, 3);
    NONE(o, T0 + 2 * W);
    CHECK_U64(GAPS(o), 1);
    fx_close(&f);
}

/* -- Lower Groups and the floor ---------------------------------------- */

static void test_lower_group_before_current_starts(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 12, 1), MOQ_OK);
    NONE(o, T0);
    CHECK_U64(DEADLINE(o), T0 + W);
    /* Nothing of 12 is released yet, so 11 may still become current. */
    CHECK_RC(OFFER(o, T0 + 5, 11, 0), MOQ_OK);
    REL(o, T0 + 5, 11, 0);
    NONE(o, T0 + 5);
    CHECK_U64(DEADLINE(o), T0 + 5 + W);   /* 11's boundary wait */
    CHECK_RC(FIN(o, T0 + 6, 11, 0), MOQ_OK);
    NONE(o, T0 + 6);                       /* 11 retires; 12 current */
    CHECK_U64(DEADLINE(o), T0 + 6 + W);
    CHECK_RC(OFFER(o, T0 + 6, 10, 0), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0 + 7, 12, 0), MOQ_OK);
    REL(o, T0 + 7, 12, 0);
    REL(o, T0 + 7, 12, 1);
    NONE(o, T0 + 7);
    CHECK_RC(OFFER(o, T0 + 7, 11, 1), MOQ_ERR_PROTO);
    CHECK_U64(GAPS(o), 0);
    fx_close(&f);
}

static void test_floor_after_sole_group_retires(void)
{
    fixture_t f;

    /* Group 0 completes and retires: the table is empty, Group 0 stays
     * closed. */
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 0, 0), MOQ_OK);
    CHECK_RC(FIN(o, T0, 0, 0), MOQ_OK);
    REL(o, T0, 0, 0);
    NONE(o, T0);
    CHECK(!receiver_order_holds(o));
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_RC(OFFER(o, T0, 0, 1), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0, 0, 0), MOQ_ERR_PROTO);
    CHECK_RC(STATUS(o, T0, 0, 1), MOQ_ERR_PROTO);
    CHECK_RC(FIN(o, T0, 0, 0), MOQ_DONE);
    CHECK_RC(FIN(o, T0, 0, 4), MOQ_DONE);
    CHECK_RC(LOSS(o, T0, 0, 1), MOQ_DONE);
    NONE(o, T0 + 10 * W);
    CHECK_U64(held_count(o), 0);
    CHECK_RC(OFFER(o, T0, 1, 0), MOQ_OK);
    REL(o, T0, 1, 0);
    /* A new generation opens Group 0 again. */
    receiver_order_reset(o, on_release, &f.rel);
    CHECK_U64(f.rel.calls, 0);
    CHECK_RC(OFFER(o, T0, 0, 0), MOQ_OK);
    REL(o, T0, 0, 0);
    fx_close(&f);

    /* Group 7 is given up by the window as the sole Group. */
    o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 7, 0), MOQ_OK);
    REL(o, T0, 7, 0);
    CHECK_RC(FIN(o, T0, 7, 1), MOQ_OK);
    NONE(o, T0);
    CHECK_U64(DEADLINE(o), T0 + W);
    NONE(o, T0 + W);
    CHECK_U64(GAPS(o), 1);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_RC(OFFER(o, T0 + W, 7, 1), MOQ_ERR_PROTO);
    CHECK_RC(FIN(o, T0 + W, 7, 1), MOQ_DONE);
    CHECK_RC(LOSS(o, T0 + W, 7, 1), MOQ_DONE);
    CHECK_RC(STATUS(o, T0 + W, 7, 2), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0 + W, 6, 0), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0 + W, 8, 0), MOQ_OK);
    REL_GAP(o, T0 + W, 8, 0, 7, 1);
    fx_close(&f);

    /* The floor follows the current Group's first release. */
    o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 21, 0), MOQ_OK);
    REL(o, T0, 21, 0);
    CHECK_RC(OFFER(o, T0, 20, 0), MOQ_ERR_PROTO);
    CHECK_RC(FIN(o, T0, 20, 0), MOQ_DONE);
    CHECK_RC(LOSS(o, T0, 20, 0), MOQ_DONE);
    fx_close(&f);
}

static void test_known_terminal_missing_prefix_then_silence(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(FIN(o, T0, 10, 3), MOQ_OK);
    CHECK(!receiver_order_holds(o));
    CHECK_U64(DEADLINE(o), T0 + W);
    NONE(o, T0 + W - 1);
    CHECK_U64(DEADLINE(o), T0 + W);
    NONE(o, T0 + W);           /* 0..3 given up; 10 retires */
    CHECK_U64(GAPS(o), 4);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK(!receiver_order_holds(o));
    CHECK_RC(OFFER(o, T0 + W, 10, 0), MOQ_ERR_PROTO);
    CHECK_RC(FIN(o, T0 + W, 10, 3), MOQ_DONE);
    CHECK_RC(LOSS(o, T0 + W, 10, 1), MOQ_DONE);
    CHECK_RC(OFFER(o, T0 + W, 11, 0), MOQ_OK);
    REL_GAP(o, T0 + W, 11, 0, 10, 0);
    NONE(o, T0 + W);
    fx_close(&f);
}

/* -- Losses ------------------------------------------------------------ */

static void test_loss_facts(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 4, W);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    CHECK_RC(LOSS(o, T0, 10, 1), MOQ_OK);
    NONE(o, T0);               /* skipped at once: no wait */
    CHECK(!receiver_order_holds(o));
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    REL_GAP(o, T0, 10, 2, 10, 1);
    CHECK_RC(LOSS(o, T0, 10, 1), MOQ_DONE);        /* below the frontier */
    CHECK_RC(LOSS(o, T0, 10, 2), MOQ_DONE);
    CHECK_RC(OFFER(o, T0, 10, 4), MOQ_OK);
    CHECK_RC(LOSS(o, T0, 10, 4), MOQ_DONE);        /* held */
    CHECK_RC(LOSS(o, T0, 10, 5), MOQ_OK);
    CHECK_RC(LOSS(o, T0, 10, 5), MOQ_DONE);        /* recorded */
    CHECK_RC(OFFER(o, T0, 10, 6), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 10, 7), MOQ_OK);         /* every slot in use */
    CHECK_RC(OFFER(o, T0, 10, 8), MOQ_ERR_BUFFER);
    CHECK_RC(LOSS(o, T0, 10, 8), MOQ_ERR_BUFFER);
    /* The lost Object arrives after all: held in the loss's slot. */
    CHECK_RC(OFFER(o, T0, 10, 5), MOQ_OK);
    CHECK_U64(held_count(o), 4);
    NONE(o, T0 + W - 1);
    REL_GAP(o, T0 + W, 10, 4, 10, 3);
    REL(o, T0 + W, 10, 5);
    REL(o, T0 + W, 10, 6);
    REL(o, T0 + W, 10, 7);
    NONE(o, T0 + W);
    CHECK_U64(GAPS(o), 1);     /* Object 3, given up by the window */

    CHECK_RC(FIN(o, T0 + W, 10, 10), MOQ_OK);
    CHECK_RC(LOSS(o, T0 + W, 10, 10), MOQ_DONE);   /* at fin_last */
    CHECK_RC(LOSS(o, T0 + W, 10, 11), MOQ_DONE);   /* above it */
    CHECK_RC(LOSS(o, T0 + W, 10, 8), MOQ_OK);
    CHECK_RC(LOSS(o, T0 + W, 10, 9), MOQ_OK);
    NONE(o, T0 + W);           /* 8 and 9 skipped; waits for 10 */
    CHECK_RC(OFFER(o, T0 + W, 10, 10), MOQ_OK);
    REL_GAP(o, T0 + W, 10, 10, 10, 8);
    NONE(o, T0 + W);

    /* A loss can open a record. */
    CHECK_RC(LOSS(o, T0 + W, 11, 0), MOQ_OK);
    NONE(o, T0 + W);
    CHECK_RC(OFFER(o, T0 + W, 11, 1), MOQ_OK);
    REL_GAP(o, T0 + W, 11, 1, 11, 0);
    CHECK_U64(GAPS(o), 1);     /* lost Objects are reported, not counted */
    fx_close(&f);
}

/* -- Capacity ---------------------------------------------------------- */

static void test_capacity_forced_accounting(void)
{
    fixture_t f;

    /* A fifth open Group: the step gives up Group 10's Object 0, which
     * completes 10, and nothing of 11..13 is lost. */
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    CHECK_RC(FIN(o, T0, 10, 1), MOQ_OK);
    for (uint64_t g = 11; g <= 13; g++) {
        CHECK_RC(OFFER(o, T0, g, 0), MOQ_OK);
        CHECK_RC(FIN(o, T0, g, 0), MOQ_OK);
    }
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0 + 5, 14, 0), MOQ_ERR_BUFFER);
    CHECK_RC(FIN(o, T0 + 5, 14, 0), MOQ_ERR_BUFFER);
    CHECK_RC(LOSS(o, T0 + 5, 14, 0), MOQ_ERR_BUFFER);
    CHECK_RC(receiver_order_force(o, T0 + 5), MOQ_OK);
    CHECK_U64(FORCED(o), 1);
    CHECK_U64(GAPS(o), 1);
    REL_GAP(o, T0 + 5, 10, 1, 10, 0);
    for (uint64_t g = 11; g <= 13; g++) REL(o, T0 + 5, g, 0);
    NONE(o, T0 + 5);
    CHECK_RC(OFFER(o, T0 + 5, 14, 0), MOQ_OK);
    REL(o, T0 + 5, 14, 0);
    NONE(o, T0 + 5);
    CHECK_U64(FORCED(o), 1);
    CHECK_U64(GAPS(o), 1);
    fx_close(&f);

    /* The same with no terminal for Group 10: a gap step, then a boundary
     * step. */
    o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    for (uint64_t g = 11; g <= 13; g++) {
        CHECK_RC(OFFER(o, T0, g, 0), MOQ_OK);
        CHECK_RC(FIN(o, T0, g, 0), MOQ_OK);
    }
    CHECK_RC(OFFER(o, T0 + 5, 14, 0), MOQ_ERR_BUFFER);
    CHECK_RC(receiver_order_force(o, T0 + 5), MOQ_OK);
    CHECK_U64(FORCED(o), 1);
    CHECK_U64(GAPS(o), 1);
    REL_GAP(o, T0 + 5, 10, 1, 10, 0);
    NONE(o, T0 + 5);           /* 10 boundary-waits at 2 */
    CHECK_U64(DEADLINE(o), T0 + 5 + W);
    CHECK_RC(OFFER(o, T0 + 5, 14, 0), MOQ_ERR_BUFFER);
    CHECK_RC(receiver_order_force(o, T0 + 5), MOQ_OK);
    CHECK_U64(FORCED(o), 2);
    CHECK_U64(GAPS(o), 1);
    REL_TAIL(o, T0 + 5, 11, 0, 10, 2);
    REL(o, T0 + 5, 12, 0);
    REL(o, T0 + 5, 13, 0);
    NONE(o, T0 + 5);
    CHECK_RC(OFFER(o, T0 + 5, 14, 0), MOQ_OK);
    REL(o, T0 + 5, 14, 0);
    NONE(o, T0 + 5);
    CHECK_U64(FORCED(o), 2);
    CHECK_U64(GAPS(o), 1);
    fx_close(&f);

    /* The 65th held item. */
    o = fx_open(&f, 4, 64, W);
    for (uint64_t k = 1; k <= 32; k++) CHECK_RC(OFFER(o, T0, 10, k), MOQ_OK);
    for (uint64_t k = 0; k <= 31; k++) CHECK_RC(OFFER(o, T0, 11, k), MOQ_OK);
    NONE(o, T0);
    CHECK_U64(held_count(o), 64);
    CHECK_RC(OFFER(o, T0 + 1, 11, 32), MOQ_ERR_BUFFER);
    CHECK_RC(receiver_order_force(o, T0 + 1), MOQ_OK);
    CHECK_U64(FORCED(o), 1);
    CHECK_U64(GAPS(o), 1);
    REL_GAP(o, T0 + 1, 10, 1, 10, 0);
    for (uint64_t k = 2; k <= 32; k++) REL(o, T0 + 1, 10, k);
    NONE(o, T0 + 1);
    CHECK_RC(OFFER(o, T0 + 1, 11, 32), MOQ_OK);
    NONE(o, T0 + 1);
    CHECK_RC(FIN(o, T0 + 2, 10, 32), MOQ_OK);
    for (uint64_t k = 0; k <= 32; k++) REL(o, T0 + 2, 11, k);
    NONE(o, T0 + 2);
    CHECK_U64(FORCED(o), 1);
    CHECK_U64(GAPS(o), 1);
    CHECK(!receiver_order_holds(o));
    fx_close(&f);

    /* A loss needs a slot too. */
    o = fx_open(&f, 4, 2, W);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    CHECK_RC(LOSS(o, T0, 10, 3), MOQ_ERR_BUFFER);
    CHECK_RC(receiver_order_force(o, T0), MOQ_OK);
    REL_GAP(o, T0, 10, 1, 10, 0);
    REL(o, T0, 10, 2);
    NONE(o, T0);
    CHECK_RC(LOSS(o, T0, 10, 3), MOQ_OK);
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0, 10, 4), MOQ_OK);
    REL_GAP(o, T0, 10, 4, 10, 3);
    CHECK_U64(FORCED(o), 1);
    CHECK_U64(GAPS(o), 1);
    fx_close(&f);
}

static void test_terminal_only_records_plus_a_fifth(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    for (uint64_t g = 10; g <= 13; g++) CHECK_RC(FIN(o, T0, g, 2), MOQ_OK);
    CHECK(!receiver_order_holds(o));
    CHECK_U64(DEADLINE(o), T0 + W);   /* facts are work, not idle */
    NONE(o, T0);
    CHECK_RC(FIN(o, T0 + 7, 14, 2), MOQ_ERR_BUFFER);
    CHECK_RC(receiver_order_force(o, T0 + 7), MOQ_OK);   /* retires 10 */
    CHECK_U64(FORCED(o), 1);
    CHECK_U64(GAPS(o), 3);
    CHECK_U64(DEADLINE(o), T0 + 7 + W);                  /* 11's stall */
    CHECK_RC(FIN(o, T0 + 7, 14, 2), MOQ_OK);
    NONE(o, T0 + 7);
    NONE(o, T0 + 7 + W - 1);
    NONE(o, T0 + 7 + W);              /* 11 given up by the window */
    CHECK_U64(GAPS(o), 6);
    CHECK_U64(DEADLINE(o), T0 + 7 + 2 * W);
    uint64_t t = T0 + 7 + W;
    CHECK_RC(OFFER(o, t, 12, 0), MOQ_OK);
    REL_GAP(o, t, 12, 0, 10, 0);      /* the earliest unreported run */
    CHECK_RC(OFFER(o, t, 12, 1), MOQ_OK);
    REL(o, t, 12, 1);
    CHECK_RC(OFFER(o, t, 12, 2), MOQ_OK);
    REL(o, t, 12, 2);
    NONE(o, t);
    CHECK_U64(DEADLINE(o), t + W);    /* 13 current, nothing held */
    for (uint64_t g = 13; g <= 14; g++) {
        for (uint64_t k = 0; k <= 2; k++) {
            CHECK_RC(OFFER(o, t, g, k), MOQ_OK);
            REL(o, t, g, k);
        }
    }
    NONE(o, t);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_U64(GAPS(o), 6);
    CHECK_U64(FORCED(o), 1);
    fx_close(&f);
}

static void test_force_done_when_nothing_can_be_freed(void)
{
    fixture_t f;

    /* Both slots hold items that are due but not yet polled: a force has
     * nothing to free, so the incoming Object is refused. */
    receiver_order_t *o = fx_open(&f, 4, 2, W);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    unsigned forces = 0;
    moq_result_t rc = MOQ_OK;
    for (unsigned i = 0; i < 16; i++) {
        rc = OFFER(o, T0, 10, 2);
        if (rc != MOQ_ERR_BUFFER) break;
        forces++;
        if (receiver_order_force(o, T0) != MOQ_OK) break;   /* refuse */
    }
    CHECK_RC(rc, MOQ_ERR_BUFFER);
    CHECK_U64(forces, 1);
    CHECK_U64(FORCED(o), 0);
    CHECK_U64(held_count(o), 2);
    REL(o, T0, 10, 0);
    REL(o, T0, 10, 1);
    NONE(o, T0);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    REL(o, T0, 10, 2);
    fx_close(&f);

    /* A caller that cannot poll: one real step, then MOQ_DONE. */
    o = fx_open(&f, 4, 2, W);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 11, 1), MOQ_OK);
    forces = 0;
    for (unsigned i = 0; i < 16; i++) {
        rc = OFFER(o, T0 + 1, 12, 0);
        if (rc != MOQ_ERR_BUFFER) break;
        forces++;
        if (receiver_order_force(o, T0 + 1) != MOQ_OK) break;
    }
    CHECK_RC(rc, MOQ_ERR_BUFFER);
    CHECK_U64(forces, 2);
    CHECK_U64(FORCED(o), 1);
    CHECK_U64(GAPS(o), 1);
    REL_GAP(o, T0 + 1, 10, 1, 10, 0);
    NONE(o, T0 + 1);
    CHECK_RC(OFFER(o, T0 + 1, 12, 0), MOQ_OK);
    fx_close(&f);
    CHECK_U64(f.rel.calls, 2);

    /* After finish everything is due already. */
    o = fx_open(&f, 4, 2, W);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    receiver_order_finish(o);
    CHECK_RC(receiver_order_force(o, T0), MOQ_DONE);
    CHECK_U64(FORCED(o), 0);
    fx_close(&f);
}

/* -- Finish, reset, destroy -------------------------------------------- */

static void test_finish_releases_everything_in_order(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 11, 1), MOQ_OK);
    CHECK_RC(FIN(o, T0, 11, 2), MOQ_OK);
    CHECK_RC(FIN(o, T0, 12, 1), MOQ_OK);
    NONE(o, T0);
    receiver_order_finish(o);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_RC(OFFER(o, T0, 13, 0), MOQ_ERR_PROTO);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_ERR_PROTO);
    CHECK_RC(STATUS(o, T0, 13, 0), MOQ_ERR_PROTO);
    CHECK_RC(FIN(o, T0, 13, 0), MOQ_DONE);
    CHECK_RC(LOSS(o, T0, 13, 0), MOQ_DONE);
    REL_GAP(o, T0, 10, 2, 10, 1);
    /* 10's tail is unconfirmed; 11's missing Object 0 is a known loss
     * and outranks it. */
    REL_GAP(o, T0, 11, 1, 11, 0);
    NONE(o, T0);
    CHECK_U64(GAPS(o), 5);     /* 10/1, 11/0, 11/2, 12/0, 12/1 */
    CHECK_U64(FORCED(o), 0);
    CHECK(!receiver_order_holds(o));
    NONE(o, T0);
    receiver_order_reset(o, on_release, &f.rel);
    CHECK_U64(f.rel.calls, 0);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);         /* nothing carried across the reset */
    CHECK_U64(GAPS(o), 5);
    fx_close(&f);
}

static void test_reset_clears_and_hands_back(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 9, 0), MOQ_OK);
    CHECK_RC(FIN(o, T0, 9, 0), MOQ_OK);
    REL(o, T0, 9, 0);
    CHECK_RC(OFFER(o, T0, 10, 0), MOQ_OK);
    REL(o, T0, 10, 0);
    fake_item_t *a = item(10, 2), *b = item(11, 0), *c = item(11, 3);
    CHECK_RC(receiver_order_offer(o, T0, 10, 2, a), MOQ_OK);
    CHECK_RC(receiver_order_offer(o, T0, 11, 0, b), MOQ_OK);
    CHECK_RC(receiver_order_offer_status(o, T0, 11, 3, c), MOQ_OK);
    CHECK_RC(LOSS(o, T0, 11, 1), MOQ_OK);
    NONE(o, T0);
    CHECK_U64(held_count(o), 3);
    CHECK_U64(DEADLINE(o), T0 + W);
    receiver_order_reset(o, on_release, &f.rel);
    CHECK_U64(f.rel.calls, 3);
    CHECK(a->released == 1 && b->released == 1 && c->released == 1);
    CHECK(!receiver_order_holds(o));
    CHECK_U64(held_count(o), 0);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    CHECK_U64(GAPS(o), 0);
    NONE(o, T0 + 10 * W);
    /* The floor is gone: Group 9 opens again. */
    CHECK_RC(OFFER(o, T0, 9, 0), MOQ_OK);
    REL(o, T0, 9, 0);
    CHECK_RC(OFFER(o, T0, 11, 1), MOQ_OK);
    /* Without a release callback the items are left to the caller. */
    fake_item_t *d = item(11, 2);
    CHECK_RC(receiver_order_offer(o, T0, 11, 2, d), MOQ_OK);
    receiver_order_reset(o, NULL, NULL);
    CHECK(!receiver_order_holds(o));
    CHECK_U64(d->released, 0);
    fx_close(&f);
    CHECK_U64(f.rel.calls, 3);
}

static void test_destroy_releases_held(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 10, 2), MOQ_OK);
    CHECK_RC(OFFER(o, T0, 11, 0), MOQ_OK);
    CHECK_RC(STATUS(o, T0, 12, 0), MOQ_OK);
    CHECK_RC(LOSS(o, T0, 10, 3), MOQ_OK);
    CHECK_U64(held_count(o), 4);
    fx_close(&f);
    CHECK_U64(f.rel.calls, 4);
    for (size_t i = 0; i < pool_used; i++) CHECK_U64(pool[i].released, 1);

    /* Without a release callback: freed, items untouched. */
    o = fx_open(&f, 4, 64, W);
    CHECK_RC(OFFER(o, T0, 10, 1), MOQ_OK);
    receiver_order_destroy(o, NULL, NULL);
    CHECK_U64(f.ca.live, 0);
    CHECK_U64(pool[0].released, 0);
}

static void test_deadline_saturates(void)
{
    fixture_t f;
    receiver_order_t *o = fx_open(&f, 4, 64, W);
    uint64_t late = UINT64_MAX - 5;
    CHECK_RC(OFFER(o, late, 10, 1), MOQ_OK);
    NONE(o, late);
    CHECK_U64(DEADLINE(o), UINT64_MAX);
    NONE(o, UINT64_MAX - 1);
    REL_GAP(o, UINT64_MAX, 10, 1, 10, 0);
    fx_close(&f);
}

int main(void)
{
    static void (*const tests[])(void) = {
        test_create_and_null_arguments,
        test_in_order_release,
        test_reorder_within_group,
        test_duplicate_and_late_refusals,
        test_above_final_refusals,
        test_later_group_terminal_while_blocked_then_silence,
        test_later_group_terminal_while_blocked_recovered,
        test_completing_g_keeps_g1_facts,
        test_terminal_before_first_object,
        test_eog_status_first,
        test_fin_and_status_both,
        test_conflicting_terminals,
        test_max_object_id,
        test_interleaved_groups_no_drop,
        test_boundary_wait_tail_arrives,
        test_boundary_wait_no_tail,
        test_boundary_wait_avoided_by_status,
        test_gap_wait_excludes_boundary_wait,
        test_lower_group_before_current_starts,
        test_floor_after_sole_group_retires,
        test_known_terminal_missing_prefix_then_silence,
        test_loss_facts,
        test_capacity_forced_accounting,
        test_terminal_only_records_plus_a_fifth,
        test_force_done_when_nothing_can_be_freed,
        test_finish_releases_everything_in_order,
        test_reset_clears_and_hands_back,
        test_destroy_releases_held,
        test_deadline_saturates,
    };
    const size_t n = sizeof(tests) / sizeof(tests[0]);
    for (size_t i = 0; i < n; i++) tests[i]();
    if (failures) {
        fprintf(stderr, "media_receiver_order: %d failure(s)\n", failures);
        return 1;
    }
    printf("media_receiver_order: %zu tests PASS\n", n);
    return 0;
}
