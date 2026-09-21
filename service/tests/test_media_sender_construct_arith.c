/*
 * Contract test for checked media-sender construction arithmetic.
 *
 * sender_new() derives ring_cap (max(queue_max_objects, pre_ready_max_objects)
 * + 1 in uint32), the namespace-storage sum plus the catalog-name addition
 * (size_t), the parts-index product and the ring allocation product, and must
 * check every one BEFORE the first allocation or endpoint-attachment effect.
 * An unrepresentable configuration is refused with MOQ_ERR_INVAL and makes
 * ZERO allocation requests; a representable configuration reaches the
 * allocator with exactly the derived sizes, and an allocator refusal is
 * MOQ_ERR_NOMEM with exact cleanup.
 *
 * The oracle observes the REAL construction result through the test seam
 * (moq_media_sender_test_construct: validation result or allocator result)
 * and through the PUBLIC pre-attachment boundary (moq_media_sender_attach on
 * a bare test endpoint, whose attachment count is read back). Every allocator
 * request goes through one LEDGER that lives from construction through
 * destruction: it records each request size in order, validates every free
 * against the block it supplied (pointer AND size), refuses and counts any
 * realloc, any request beyond its array, and any request above a finite
 * per-request ceiling, and refuses the NOMINATED request so a giant size is
 * measured, never performed.
 *
 * Platform gate: on LP64 the uint32 ring capacity times entry-size product
 * cannot wrap and the ring boundary is a valid configuration; on ILP32 the
 * same boundary wraps size_t and belongs to the refusal class (compiled only
 * there). Neither build claims the other's rows.
 *
 * White-box: links moq-service-sender-test-internals (MOQ_MEDIA_SENDER_TESTING).
 */
#include <moq/media_sender.h>
#include <moq/msf.h>
#include "test_support.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Test seams (media_sender.c / endpoint.c, MOQ_MEDIA_SENDER_TESTING). */
moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_sender_test_entry_sizes(size_t *ring_entry, size_t *track);
void moq_media_sender_test_set_parts_walk_hook(bool (*fn)(size_t, void *), void *ctx);
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
int moq_endpoint_test_attachments(moq_endpoint_t *ep);

/* -- pre-traversal observation --------------------------------------------- */
typedef struct { int calls; size_t count; } walk_probe_t;
static walk_probe_t g_walk;
static bool probe_walk(size_t count, void *ctx)
{
    walk_probe_t *w = (walk_probe_t *)ctx;
    w->calls++; w->count = count;
    return false;
}

/* -- allocation ledger ---------------------------------------------------- */
#define MAX_REQ 32
#define REQUEST_CEILING ((size_t)64u << 20)   /* finite per-request ceiling: 64 MiB */

typedef struct { void *ptr; size_t size; bool live; int request; } block_t;
typedef struct { int request; size_t supplied; size_t freed; } bad_free_t;
typedef struct {
    size_t  requested[MAX_REQ];
    int     requests;
    block_t blocks[MAX_REQ];
    int     supplied;
    int     freed;
    int     refuse_at;            /* nominated 0-based request index; -1 never */
    bool    refuse_once;          /* true: refuse ONLY the nominated request;
                                     false: refuse it and every later one (the
                                     safe mode for forged huge-input rows) */
    int     refused;
    size_t  refused_size;         /* size of the FIRST nominated refusal */
    int     overflow;
    int     oversize;
    int     bad_free;
    bad_free_t bad[MAX_REQ];      /* identity of every wrong-sized free */
    int     unexpected_realloc;
} ledger_t;

static ledger_t g_ledger;
static moq_alloc_t g_alloc;

static void *ledger_alloc(size_t size, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    int i = l->requests++;
    if (i >= MAX_REQ) { l->overflow++; return NULL; }
    l->requested[i] = size;
    if (l->refuse_at >= 0 && (l->refuse_once ? i == l->refuse_at : i >= l->refuse_at)) {
        if (l->refused++ == 0) l->refused_size = size;
        return NULL;
    }
    if (size > REQUEST_CEILING) { l->oversize++; return NULL; }
    void *p = malloc(size ? size : 1);
    if (!p) return NULL;
    l->blocks[l->supplied++] = (block_t){ p, size, true, i };
    return p;
}
static void ledger_free(void *p, size_t size, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    if (!p) return;
    for (int i = 0; i < l->supplied; i++) {
        block_t *b = &l->blocks[i];
        if (b->live && b->ptr == p) {
            if (b->size != size) {
                if (l->bad_free < MAX_REQ)
                    l->bad[l->bad_free] = (bad_free_t){ b->request, b->size, size };
                l->bad_free++;
            }
            b->live = false;
            l->freed++;
            free(p);
            return;
        }
    }
    l->bad_free++;
}
static void *ledger_realloc(void *p, size_t old, size_t nw, void *ctx)
{
    (void)p; (void)old; (void)nw;
    ((ledger_t *)ctx)->unexpected_realloc++;
    return NULL;
}

static void arm_mode(int refuse_at, bool once)
{
    memset(&g_ledger, 0, sizeof(g_ledger));
    g_ledger.refuse_at = refuse_at;
    g_ledger.refuse_once = once;
    g_alloc.alloc = ledger_alloc; g_alloc.free = ledger_free;
    g_alloc.realloc = ledger_realloc; g_alloc.ctx = &g_ledger;
    moq_media_sender_test_set_alloc(&g_alloc);
}
/* Fail-from-here: the safe mode for forged huge-input rows (nothing after the
 * nominated request is ever supplied). */
static void arm(int refuse_at) { arm_mode(refuse_at, false); }
/* Fail-once: exactly the nominated request is refused; every other request is
 * supplied, so the constructor's unwind of a PARTIAL success is observed. */
static void arm_once(int refuse_at) { arm_mode(refuse_at, true); }
static void disarm(void) { moq_media_sender_test_set_alloc(NULL); }

static int outstanding(void) { return g_ledger.supplied - g_ledger.freed; }
static bool ledger_clean(void)
{
    return g_ledger.overflow == 0 && g_ledger.oversize == 0 &&
           g_ledger.bad_free == 0 && g_ledger.unexpected_realloc == 0;
}
static void dump_ledger(const char *what)
{
    fprintf(stderr, "  ledger[%s]: requests=%d supplied=%d freed=%d refused=%d"
            " overflow=%d oversize=%d bad_free=%d realloc=%d sizes=",
            what, g_ledger.requests, g_ledger.supplied, g_ledger.freed,
            g_ledger.refused, g_ledger.overflow, g_ledger.oversize,
            g_ledger.bad_free, g_ledger.unexpected_realloc);
    int n = g_ledger.requests < MAX_REQ ? g_ledger.requests : MAX_REQ;
    for (int i = 0; i < n; i++) fprintf(stderr, "%s%zu", i ? "," : "", g_ledger.requested[i]);
    int nb = g_ledger.bad_free < MAX_REQ ? g_ledger.bad_free : MAX_REQ;
    for (int i = 0; i < nb; i++)
        fprintf(stderr, " bad_free[request %d: supplied %zu, freed %zu]",
                g_ledger.bad[i].request, g_ledger.bad[i].supplied, g_ledger.bad[i].freed);
    fprintf(stderr, "\n");
}

static void row(const char *what, const char *name, bool ok, int line)
{
    if (ok) return;
    fprintf(stderr, "FAIL[%s.%s]: %s:%d\n", what, name, __FILE__, line);
    dump_ledger(what);
    failures++;
}
#define ROW(what, name, cond) row((what), (name), (cond), __LINE__)

static const uint8_t NS0[] = "live";
static const uint8_t NS1[] = "cam1";

static void base_cfg(moq_media_sender_cfg_t *cfg, const moq_bytes_t *parts, size_t count)
{
    moq_media_sender_cfg_init_live_sized(cfg, sizeof(*cfg));
    cfg->namespace_.parts = parts;
    cfg->namespace_.count = count;
}

/* -- refusal rows ---------------------------------------------------------- */
/* arm(1): request 0 (the sender struct) would be SUPPLIED and every later
 * request refused, so a constructor that allocated before validating is caught
 * by the ledger (requests > 0, NOMEM instead of INVAL). The contract: refusal
 * (INVAL) precedes the first request. */
static void expect_refused(const moq_media_sender_cfg_t *cfg, const char *what)
{
    arm(1);
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_test_construct(cfg, &s);
    disarm();
    if (rc != MOQ_ERR_INVAL)
        fprintf(stderr, "  %s: measured result %d (INVAL is %d, NOMEM is %d)\n",
                what, (int)rc, (int)MOQ_ERR_INVAL, (int)MOQ_ERR_NOMEM);
    ROW(what, "result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "no_sender", s == NULL);
    ROW(what, "zero_requests", g_ledger.requests == 0);
    if (s) moq_media_sender_test_free(s);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());
}

/* The same input at the PUBLIC pre-attachment boundary: attach on a bare
 * endpoint must refuse with INVAL, request nothing and attach nothing. */
static void expect_refused_public(const moq_media_sender_cfg_t *cfg, const char *what)
{
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    ROW(what, "public.fixture_endpoint", ep != NULL);
    if (!ep) return;
    arm(1);
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_attach(ep, cfg, &s);
    disarm();
    ROW(what, "public.result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "public.no_sender", s == NULL);
    ROW(what, "public.zero_requests", g_ledger.requests == 0);
    ROW(what, "public.zero_attachments", moq_endpoint_test_attachments(ep) == 0);
    if (s) moq_media_sender_destroy(s);
    ROW(what, "public.zero_outstanding", outstanding() == 0);
    ROW(what, "public.ledger_clean", ledger_clean());
    moq_endpoint_test_free_bare(ep);
}

/* MEASURED ring arithmetic: with every request supplied, a wrapped ring
 * capacity shows up as a zero-byte ring request (request 3). The sender is
 * never written to (a zero ring would make the first enqueue index `% 0`); it
 * is freed through the same ledger. This row is diagnostic evidence only:
 * the CONTRACT (refusal before any request) is the expect_refused rows. */
static void measure_ring_request(const moq_media_sender_cfg_t *cfg, const char *what)
{
    arm(-1);
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_test_construct(cfg, &s);
    disarm();
    if (rc == MOQ_OK && g_ledger.requests >= 4)
        fprintf(stderr, "  %s: construction accepted; measured ring request = %zu bytes\n",
                what, g_ledger.requested[3]);
    ROW(what, "measured.not_accepted_with_zero_ring",
        !(rc == MOQ_OK && g_ledger.requests >= 4 && g_ledger.requested[3] == 0));
    if (s) moq_media_sender_test_free(s);
    ROW(what, "measured.zero_outstanding", outstanding() == 0);
}

static void test_b1_queue_objects_ring_wrap(void)
{
    /* queue_max_objects: max(queue, pre_ready) + 1 wraps uint32 to 0 for
     * UINT32_MAX. */
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
    cfg.queue_max_objects = UINT32_MAX;
    measure_ring_request(&cfg, "b1.queue_max_objects.ring_cap_uint32_wrap");
    expect_refused(&cfg, "b1.queue_max_objects.ring_cap_uint32_wrap");
    expect_refused_public(&cfg, "b1.queue_max_objects.ring_cap_uint32_wrap");
}

static void test_b2_pre_ready_objects_ring_wrap(void)
{
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
    cfg.pre_ready_max_objects = UINT32_MAX;
    measure_ring_request(&cfg, "b2.pre_ready_max_objects.ring_cap_uint32_wrap");
    expect_refused(&cfg, "b2.pre_ready_max_objects.ring_cap_uint32_wrap");
    expect_refused_public(&cfg, "b2.pre_ready_max_objects.ring_cap_uint32_wrap");
}

static void test_b3_namespace_sum_wrap(void)
{
    /* Two lengths whose sum wraps size_t (SIZE_MAX-2 + 8): the ns_data request
     * would be the WRAPPED tiny size (5 + catalog 7 = 12). No data pointer is
     * dereferenced before that allocation, and the ledger refuses it, so the
     * row is safe to run today. */
    moq_bytes_t parts[2] = { { NS0, SIZE_MAX - 2 }, { NS1, 8 } };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
    expect_refused(&cfg, "b3.namespace_.parts_len_sum_size_t_wrap");
    expect_refused_public(&cfg, "b3.namespace_.parts_len_sum_size_t_wrap");
}

static void test_b4_catalog_name_addition_wrap(void)
{
    /* One part SIZE_MAX-3 does not wrap alone; adding the default catalog track
     * name (7 bytes) wraps the ns_data request to 3. */
    moq_bytes_t parts[1] = { { NS0, SIZE_MAX - 3 } };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 1);
    expect_refused(&cfg, "b4.catalog_track.name_addition_size_t_wrap");
    expect_refused_public(&cfg, "b4.catalog_track.name_addition_size_t_wrap");
}

/* An intrinsically unrepresentable part count (count * sizeof(moq_bytes_t)
 * cannot fit size_t) must be refused BEFORE any part is read. One real element
 * is allocated; the pre-traversal probe both records and vetoes the walk, so
 * the fixture is finite in either order. */
static void test_b5_namespace_count_unrepresentable(void)
{
    const char *what = "b5.namespace_.count_unrepresentable";
    moq_bytes_t *part = (moq_bytes_t *)malloc(sizeof(*part));
    ROW(what, "fixture.part", part != NULL);
    if (!part) return;
    *part = (moq_bytes_t){ NS0, 4 };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, part, SIZE_MAX / sizeof(moq_bytes_t) + 1);
    moq_media_sender_test_set_parts_walk_hook(probe_walk, &g_walk);

    memset(&g_walk, 0, sizeof(g_walk)); arm(1);
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_test_construct(&cfg, &s);
    disarm();
    ROW(what, "result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "no_sender", s == NULL);
    ROW(what, "walk_not_entered", g_walk.calls == 0);
    ROW(what, "zero_requests", g_ledger.requests == 0);

    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    ROW(what, "public.fixture_endpoint", ep != NULL);
    if (ep) {
        memset(&g_walk, 0, sizeof(g_walk)); arm(1);
        s = NULL;
        rc = moq_media_sender_attach(ep, &cfg, &s);
        disarm();
        ROW(what, "public.result_is_inval", rc == MOQ_ERR_INVAL);
        ROW(what, "public.no_sender", s == NULL);
        ROW(what, "public.walk_not_entered", g_walk.calls == 0);
        ROW(what, "public.zero_requests", g_ledger.requests == 0);
        ROW(what, "public.zero_attachments", moq_endpoint_test_attachments(ep) == 0);
        moq_endpoint_test_free_bare(ep);
    }
    moq_media_sender_test_set_parts_walk_hook(NULL, NULL);
    free(part);
}

/* -- acceptance rows: valid configs reach the allocator with EXACT sizes ----- */
/* Order of requests today (sender_new): struct, ns_parts, ns_data, ring,
 * catalog track. Sizes must be exactly the derived products. */
#define EXPECTED_REQUESTS 5

static void test_b6_defaults_request_exact_sizes(void)
{
    const char *what = "b6.defaults";
    size_t ring_entry = 0, track = 0;
    moq_media_sender_test_entry_sizes(&ring_entry, &track);
    ROW(what, "entry_sizes", ring_entry > 0 && track > 0);
    fprintf(stderr, "  %s: sizeof(ring entry)=%zu sizeof(track)=%zu\n", what, ring_entry, track);
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
    arm(-1);
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_test_construct(&cfg, &s);
    disarm();
    ROW(what, "result_ok", rc == MOQ_OK && s != NULL);
    ROW(what, "five_requests", g_ledger.requests == EXPECTED_REQUESTS);
    if (g_ledger.requests == EXPECTED_REQUESTS) {
        ROW(what, "size.ns_parts", g_ledger.requested[1] == 2 * sizeof(moq_bytes_t));
        ROW(what, "size.ns_data",  g_ledger.requested[2] == (size_t)8 + MOQ_MSF_CATALOG_TRACK_NAME_LEN);
        ROW(what, "size.ring",     g_ledger.requested[3] == (size_t)257 * ring_entry);
        ROW(what, "size.track",    g_ledger.requested[4] == track);
    }
    ROW(what, "all_supplied", g_ledger.supplied == g_ledger.requests);
    if (s) moq_media_sender_test_free(s);
    ROW(what, "freed_every_block", g_ledger.freed == g_ledger.supplied);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());
}

/* Positive public control: a valid configuration attaches exactly once and
 * detaches exactly once, with the same five requests and a balanced ledger. */
static void test_b7_public_attach_counts(void)
{
    const char *what = "b7.public.defaults";
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    ROW(what, "fixture_endpoint", ep != NULL);
    if (!ep) return;
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
    arm(-1);
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_attach(ep, &cfg, &s);
    disarm();
    ROW(what, "result_ok", rc == MOQ_OK && s != NULL);
    ROW(what, "five_requests", g_ledger.requests == EXPECTED_REQUESTS);
    ROW(what, "attached_once", moq_endpoint_test_attachments(ep) == 1);
    if (s) moq_media_sender_destroy(s);
    ROW(what, "detached", moq_endpoint_test_attachments(ep) == 0);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());
    moq_endpoint_test_free_bare(ep);
}

/* Allocator refusal at exactly ONE nominated request of a valid
 * configuration (every other request supplied): NOMEM, no sender, and every
 * block that WAS supplied -- before or after the refusal -- is freed with the
 * exact size it was supplied with. The constructor may stop at the refusal or
 * request later blocks and unwind them; both are legal, a wrong-sized free is
 * not. Each wrong-sized free is identified by request index. */
static void test_b8_nomem_once_unwinds_exactly(void)
{
    for (int at = 0; at < EXPECTED_REQUESTS; at++) {
        char what[48];
        snprintf(what, sizeof(what), "b8.nomem_once_at_request_%d", at);
        moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
        moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
        arm_once(at);
        moq_media_sender_t *s = NULL;
        moq_result_t rc = moq_media_sender_test_construct(&cfg, &s);
        disarm();
        ROW(what, "result_is_nomem", rc == MOQ_ERR_NOMEM);
        ROW(what, "no_sender", s == NULL);
        ROW(what, "one_refusal", g_ledger.refused == 1);
        ROW(what, "refused_size_is_nominated", g_ledger.refused_size == g_ledger.requested[at]);
        if (s) moq_media_sender_test_free(s);
        ROW(what, "freed_every_supplied_block", g_ledger.freed == g_ledger.supplied);
        ROW(what, "zero_outstanding", outstanding() == 0);
        ROW(what, "every_free_exact_size", g_ledger.bad_free == 0);
        ROW(what, "ledger_clean", ledger_clean());
    }
}

/* The same with fail-from-here (nothing after the refusal is supplied): the
 * constructor must still free what it got with exact sizes. */
static void test_b8_nomem_from_here_unwinds_exactly(void)
{
    for (int at = 0; at < EXPECTED_REQUESTS; at++) {
        char what[48];
        snprintf(what, sizeof(what), "b8.nomem_from_request_%d", at);
        moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
        moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
        arm(at);
        moq_media_sender_t *s = NULL;
        moq_result_t rc = moq_media_sender_test_construct(&cfg, &s);
        disarm();
        ROW(what, "result_is_nomem", rc == MOQ_ERR_NOMEM);
        ROW(what, "no_sender", s == NULL);
        ROW(what, "supplied_before_refusal", g_ledger.supplied == at);
        if (s) moq_media_sender_test_free(s);
        ROW(what, "freed_every_supplied_block", g_ledger.freed == g_ledger.supplied);
        ROW(what, "zero_outstanding", outstanding() == 0);
        ROW(what, "every_free_exact_size", g_ledger.bad_free == 0);
        ROW(what, "ledger_clean", ledger_clean());
    }
}

#if SIZE_MAX > 0xFFFFFFFFu
/* LP64 only: queue_max_objects = UINT32_MAX-1 gives ring_cap = UINT32_MAX (no
 * wrap) and a representable ring product, so the input is a VALID
 * configuration whose one giant request is refused by the ledger at its
 * nominated index (3), measured and never performed. A valid configuration
 * refused by its allocator reports the allocator's NOMEM. */
static void test_b9_boundary_ring_reaches_exact_nominated_request(void)
{
    const char *what = "b9.queue_max_objects.boundary_4294967294";
    size_t ring_entry = 0, track = 0;
    moq_media_sender_test_entry_sizes(&ring_entry, &track);
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
    cfg.queue_max_objects = UINT32_MAX - 1;
    arm(3);
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_test_construct(&cfg, &s);
    disarm();
    ROW(what, "allocator_refusal_is_nomem", rc == MOQ_ERR_NOMEM && s == NULL);
    ROW(what, "four_requests", g_ledger.requests == 4);
    ROW(what, "one_nominated_refusal", g_ledger.refused == 1);
    ROW(what, "size.ring_nominated", g_ledger.refused_size == (size_t)4294967295u * ring_entry);
    ROW(what, "unwound_exactly", g_ledger.supplied == 3 && g_ledger.freed == 3);
    if (s) moq_media_sender_test_free(s);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());
}
#else
/* ILP32: ring_cap * sizeof(sender_preq_entry_t) wraps size_t for this input,
 * so it must be refused before any request. Compiled only for a 32-bit
 * size_t; not executed or claimed on an LP64 host. */
static void test_b9_ilp32_boundary_ring_product_wrap(void)
{
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_sender_cfg_t cfg; base_cfg(&cfg, parts, 2);
    cfg.queue_max_objects = UINT32_MAX - 1;
    expect_refused(&cfg, "b9.queue_max_objects.boundary_4294967294.ilp32_ring_product_size_t_wrap");
    expect_refused_public(&cfg, "b9.queue_max_objects.boundary_4294967294.ilp32_ring_product_size_t_wrap");
}
#endif

int main(void)
{
    test_b1_queue_objects_ring_wrap();
    test_b2_pre_ready_objects_ring_wrap();
    test_b3_namespace_sum_wrap();
    test_b4_catalog_name_addition_wrap();
    test_b5_namespace_count_unrepresentable();
    test_b6_defaults_request_exact_sizes();
    test_b7_public_attach_counts();
    test_b8_nomem_once_unwinds_exactly();
    test_b8_nomem_from_here_unwinds_exactly();
#if SIZE_MAX > 0xFFFFFFFFu
    test_b9_boundary_ring_reaches_exact_nominated_request();
    fprintf(stderr, "sender_construct_arith: LP64 build; ILP32 size_t product-wrap rows NOT executed (qualification gap)\n");
#else
    test_b9_ilp32_boundary_ring_product_wrap();
    fprintf(stderr, "sender_construct_arith: ILP32 build; LP64 boundary allocation rows NOT executed\n");
#endif
    if (failures) {
        fprintf(stderr, "media_sender_construct_arith: %d failure(s)\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("media_sender_construct_arith");
    return 0;
}
