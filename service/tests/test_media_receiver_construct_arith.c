/*
 * Contract test for checked media-receiver construction arithmetic.
 *
 * receiver_new() derives ev_cap, obj_cap, ring_cap (FLOW_CONTROL: obj_cap +
 * obj_cap/2 + 1 in uint32), byte_cap and the FLOW_CONTROL byte ceiling
 * (uint64), the namespace-storage sum plus the catalog-name addition (size_t),
 * and the event/object/SAP/timeline allocation products, and checks every one
 * BEFORE the first allocation or endpoint-attachment effect. An unrepresentable
 * configuration is refused with MOQ_ERR_INVAL and makes ZERO allocation
 * requests; a representable configuration reaches the allocator with exactly
 * the derived sizes, and an allocator refusal is MOQ_ERR_NOMEM with exact
 * cleanup.
 *
 * The oracle observes the REAL construction result through the test seam
 * (moq_media_receiver_test_construct: validation result or allocator result)
 * and through the PUBLIC pre-attachment boundary (moq_media_receiver_attach on
 * a bare test endpoint, whose attachment count is read back). Every allocator
 * request goes through one LEDGER that lives from construction through
 * destruction: it records each request size in order, validates every free
 * against the block it supplied (pointer AND size), refuses and counts any
 * realloc, any request beyond its array, and any request above a finite
 * per-request ceiling, and refuses the NOMINATED request so a giant size is
 * measured, never performed.
 *
 * Platform gate: on LP64 the uint32 capacity times entry-size products cannot
 * wrap and the ring boundary is a valid configuration (b6); on ILP32 the same
 * boundary wraps size_t and belongs to the refusal class (compiled only
 * there). Neither build claims the other's rows.
 *
 * White-box: links moq-service-receiver-test-internals (MOQ_MEDIA_RECEIVER_TESTING).
 */
#include <moq/media_receiver.h>
#include <moq/msf.h>
#include "test_support.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

/* Test seams (media_receiver.c / endpoint.c, MOQ_MEDIA_RECEIVER_TESTING). */
moq_result_t moq_media_receiver_test_construct(const moq_media_receiver_cfg_t *cfg,
                                               moq_media_receiver_t **out);
void moq_media_receiver_test_free(moq_media_receiver_t *r);
void moq_media_receiver_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_receiver_test_entry_sizes(size_t *event_entry, size_t *obj_entry);
moq_endpoint_t *moq_endpoint_test_make_bare(void);
void moq_endpoint_test_free_bare(moq_endpoint_t *ep);
int moq_endpoint_test_attachments(moq_endpoint_t *ep);
void moq_media_receiver_test_set_connect(
    moq_result_t (*fn)(const moq_endpoint_cfg_t *, moq_endpoint_t **, void *), void *ctx);
void moq_media_receiver_test_set_parts_walk_hook(bool (*fn)(size_t, void *), void *ctx);

/* -- owning-create endpoint boundary --------------------------------------- */
/* Counting stand-in for moq_endpoint_connect on the owning path: refuses with
 * the adapter-free build's real code (UNSUPPORTED) and never creates an
 * endpoint, so the constructor's ordering is observed without a socket. */
typedef struct { int calls; moq_result_t code; } connect_probe_t;
static connect_probe_t g_connect;
static moq_result_t probe_connect(const moq_endpoint_cfg_t *cfg, moq_endpoint_t **out, void *ctx)
{
    (void)cfg;
    connect_probe_t *c = (connect_probe_t *)ctx;
    c->calls++;
    *out = NULL;
    return c->code;
}
static void arm_connect(moq_result_t code)
{
    g_connect.calls = 0; g_connect.code = code;
    moq_media_receiver_test_set_connect(probe_connect, &g_connect);
}
static void disarm_connect(void) { moq_media_receiver_test_set_connect(NULL, NULL); }

static const uint8_t EP_URL[] = "moqt://127.0.0.1:4433";
static void owning_cfg(moq_media_receiver_cfg_t *cfg, moq_endpoint_cfg_t *ep_cfg)
{
    moq_endpoint_cfg_init_sized(ep_cfg, sizeof(*ep_cfg));
    ep_cfg->url = (moq_bytes_t){ EP_URL, sizeof(EP_URL) - 1 };
    cfg->endpoint = ep_cfg;
}

/* -- pre-traversal observation --------------------------------------------- */
/* Records that configuration validation was about to walk the namespace
 * parts, and vetoes the walk so a fixture with an unrepresentable count reads
 * no element. The contract: an impossible count is refused BEFORE this point. */
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

typedef struct { void *ptr; size_t size; bool live; } block_t;
typedef struct {
    size_t  requested[MAX_REQ];   /* every request size, in order */
    int     requests;
    block_t blocks[MAX_REQ];      /* blocks actually supplied */
    int     supplied;
    int     freed;                /* frees that matched a supplied block */
    int     refuse_at;            /* nominated 0-based request index; -1 never */
    int     refused;              /* nominated refusals */
    size_t  refused_size;         /* size of the FIRST nominated refusal */
    int     overflow;             /* requests beyond MAX_REQ (refused) */
    int     oversize;             /* requests above the ceiling (refused) */
    int     bad_free;             /* unknown pointer or wrong size */
    int     unexpected_realloc;   /* receiver construction never reallocs */
} ledger_t;

static ledger_t g_ledger;
static moq_alloc_t g_alloc;

static void *ledger_alloc(size_t size, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    int i = l->requests++;
    if (i >= MAX_REQ) { l->overflow++; return NULL; }
    l->requested[i] = size;
    if (l->refuse_at >= 0 && i >= l->refuse_at) {
        if (l->refused++ == 0) l->refused_size = size;
        return NULL;
    }
    if (size > REQUEST_CEILING) { l->oversize++; return NULL; }
    void *p = malloc(size ? size : 1);
    if (!p) return NULL;
    l->blocks[l->supplied++] = (block_t){ p, size, true };
    return p;
}
static void ledger_free(void *p, size_t size, void *ctx)
{
    ledger_t *l = (ledger_t *)ctx;
    if (!p) return;
    for (int i = 0; i < l->supplied; i++) {
        block_t *b = &l->blocks[i];
        if (b->live && b->ptr == p) {
            if (b->size != size) l->bad_free++;
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

/* One ledger per scenario: armed once BEFORE construction and read after
 * destruction. disarm() only stops routing NEW constructions through it; a
 * receiver built under it keeps freeing through it (r->alloc is copied). */
static void arm(int refuse_at)
{
    memset(&g_ledger, 0, sizeof(g_ledger));
    g_ledger.refuse_at = refuse_at;
    g_alloc.alloc = ledger_alloc; g_alloc.free = ledger_free;
    g_alloc.realloc = ledger_realloc; g_alloc.ctx = &g_ledger;
    moq_media_receiver_test_set_alloc(&g_alloc);
}
static void disarm(void) { moq_media_receiver_test_set_alloc(NULL); }

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
    fprintf(stderr, "\n");
}

/* Named rows: the diagnostic carries the scenario AND the row, so each
 * refusal input/field fails under its own name. */
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

static void base_cfg(moq_media_receiver_cfg_t *cfg, moq_media_overflow_policy_t policy,
                     const moq_bytes_t *parts, size_t count)
{
    moq_media_receiver_cfg_init(cfg);
    cfg->overflow.policy = policy;
    cfg->namespace_.parts = parts;
    cfg->namespace_.count = count;
}

/* -- refusal rows ---------------------------------------------------------- */
/* arm(1): request 0 (the receiver struct) would be SUPPLIED and every later
 * request refused -- so a constructor that allocated before validating would
 * be caught by the ledger (requests > 0, NOMEM instead of INVAL). The
 * contract: refusal (INVAL) precedes the first request. */
static void expect_refused(const moq_media_receiver_cfg_t *cfg, const char *what)
{
    arm(1);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_test_construct(cfg, &r);
    disarm();
    if (rc != MOQ_ERR_INVAL)
        fprintf(stderr, "  %s: measured result %d (INVAL is %d, NOMEM is %d)\n",
                what, (int)rc, (int)MOQ_ERR_INVAL, (int)MOQ_ERR_NOMEM);
    ROW(what, "result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "no_receiver", r == NULL);
    ROW(what, "zero_requests", g_ledger.requests == 0);
    if (r) moq_media_receiver_test_free(r);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());
}

/* The same input at the PUBLIC pre-attachment boundary: attach on a bare
 * endpoint must refuse with INVAL, request nothing and attach nothing. */
static void expect_refused_public(const moq_media_receiver_cfg_t *cfg, const char *what)
{
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    ROW(what, "public.fixture_endpoint", ep != NULL);
    if (!ep) return;
    arm(1);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_attach(ep, cfg, &r);
    disarm();
    ROW(what, "public.result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "public.no_receiver", r == NULL);
    ROW(what, "public.zero_requests", g_ledger.requests == 0);
    ROW(what, "public.zero_attachments", moq_endpoint_test_attachments(ep) == 0);
    if (r) moq_media_receiver_destroy(r);
    ROW(what, "public.zero_outstanding", outstanding() == 0);
    ROW(what, "public.ledger_clean", ledger_clean());
    moq_endpoint_test_free_bare(ep);
}

/* The same input at the PUBLIC owning boundary: create must refuse with INVAL
 * before connecting -- zero connect calls, zero requests, no receiver. */
static void expect_refused_create(const moq_media_receiver_cfg_t *cfg_in, const char *what)
{
    moq_media_receiver_cfg_t cfg = *cfg_in;
    moq_endpoint_cfg_t ep_cfg;
    owning_cfg(&cfg, &ep_cfg);
    arm(1);
    arm_connect(MOQ_ERR_UNSUPPORTED);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_create(&cfg, &r);
    disarm_connect();
    disarm();
    if (rc != MOQ_ERR_INVAL)
        fprintf(stderr, "  %s: create measured result %d, connect calls %d\n", what, (int)rc, g_connect.calls);
    ROW(what, "create.result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "create.no_receiver", r == NULL);
    ROW(what, "create.zero_connects", g_connect.calls == 0);
    ROW(what, "create.zero_requests", g_ledger.requests == 0);
    if (r) moq_media_receiver_destroy(r);
    ROW(what, "create.zero_outstanding", outstanding() == 0);
    ROW(what, "create.ledger_clean", ledger_clean());
}

static void test_b1_flow_control_ring_wrap(void)
{
    /* overflow.max_objects: obj_cap + obj_cap/2 + 1 wraps uint32 to 0
     * (measured: 2863311530 -> 0). */
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, parts, 2);
    cfg.overflow.max_objects = 2863311530u;
    expect_refused(&cfg, "b1.max_objects.ring_cap_uint32_wrap");
    expect_refused_public(&cfg, "b1.max_objects.ring_cap_uint32_wrap");
    expect_refused_create(&cfg, "b1.max_objects.ring_cap_uint32_wrap");
}

static void test_b2_flow_control_byte_ceiling_wrap(void)
{
    /* overflow.max_bytes: byte_cap + byte_cap/2 wraps uint64
     * (measured: UINT64_MAX -> 9223372036854775806). */
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, parts, 2);
    cfg.overflow.max_bytes = UINT64_MAX;
    expect_refused(&cfg, "b2.max_bytes.byte_ceiling_uint64_wrap");
    expect_refused_public(&cfg, "b2.max_bytes.byte_ceiling_uint64_wrap");
    expect_refused_create(&cfg, "b2.max_bytes.byte_ceiling_uint64_wrap");
}

static void test_b3_catalog_name_addition_wrap(void)
{
    /* namespace_ sum alone does not wrap (one part, SIZE_MAX-3); adding the
     * default catalog track name (7 bytes) wraps the ns_data request to 3.
     * The part's data pointer is never dereferenced before that allocation. */
    moq_bytes_t parts[1] = { { NS0, SIZE_MAX - 3 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, parts, 1);
    expect_refused(&cfg, "b3.catalog_track.name_addition_size_t_wrap");
    expect_refused_public(&cfg, "b3.catalog_track.name_addition_size_t_wrap");
    expect_refused_create(&cfg, "b3.catalog_track.name_addition_size_t_wrap");
}

static void test_b4_namespace_sum_wrap(void)
{
    /* namespace_.parts: two lengths whose sum wraps size_t (SIZE_MAX-2 + 8),
     * so the ns_data request is the WRAPPED tiny size (5 + catalog 7 = 12).
     * No data pointer is dereferenced before that allocation, and the ledger
     * refuses it, so the row is safe to run today. */
    moq_bytes_t parts[2] = { { NS0, SIZE_MAX - 2 }, { NS1, 8 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, parts, 2);
    expect_refused(&cfg, "b4.namespace_.parts_len_sum_size_t_wrap");
    expect_refused_public(&cfg, "b4.namespace_.parts_len_sum_size_t_wrap");
    expect_refused_create(&cfg, "b4.namespace_.parts_len_sum_size_t_wrap");
}

/* -- acceptance rows: valid configs reach the allocator with EXACT sizes ----- */
static void test_b5_defaults_request_exact_sizes(void)
{
    const char *what = "b5.defaults";
    size_t ev_entry = 0, obj_entry = 0;
    moq_media_receiver_test_entry_sizes(&ev_entry, &obj_entry);
    ROW(what, "entry_sizes", ev_entry > 0 && obj_entry > 0);
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, parts, 2);
    arm(-1);                                  /* ONE ledger: construction through destruction */
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_test_construct(&cfg, &r);
    disarm();
    ROW(what, "result_ok", rc == MOQ_OK && r != NULL);
    /* Order of requests today (receiver_new): struct, ns_parts, ns_data,
     * events, objs, saps, mts. Sizes must be exactly the derived products. */
    ROW(what, "seven_requests", g_ledger.requests == 7);
    if (g_ledger.requests == 7) {
        ROW(what, "size.ns_parts", g_ledger.requested[1] == 2 * sizeof(moq_bytes_t));
        ROW(what, "size.ns_data",  g_ledger.requested[2] == (size_t)8 + MOQ_MSF_CATALOG_TRACK_NAME_LEN);
        ROW(what, "size.events",   g_ledger.requested[3] == (size_t)64 * ev_entry);
        ROW(what, "size.objs",     g_ledger.requested[4] == (size_t)256 * obj_entry);
        ROW(what, "size.saps",     g_ledger.requested[5] == (size_t)256 * sizeof(moq_media_sap_record_t));
        ROW(what, "size.mts",      g_ledger.requested[6] == (size_t)256 * sizeof(moq_media_timeline_record_t));
    }
    ROW(what, "all_supplied", g_ledger.supplied == g_ledger.requests);
    if (r) moq_media_receiver_test_free(r);   /* frees through the SAME ledger */
    ROW(what, "freed_every_block", g_ledger.freed == g_ledger.supplied);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());   /* exact pointer+size on every free */
}

#if SIZE_MAX > 0xFFFFFFFFu
/* LP64 only: the ring product 4294967294 * sizeof(entry) is representable, so
 * the boundary input is a VALID configuration whose one giant request is
 * refused by the ledger. On ILP32 the same product wraps size_t, which places
 * this input in the overflow-refusal class (test_b6_ilp32 below), never in the
 * valid-allocation class. */
static void test_b6_boundary_ring_reaches_exact_nominated_request(void)
{
    /* The largest FLOW_CONTROL max_objects that does NOT wrap: 2863311529 ->
     * ring_cap = 2863311529 + 1431655764 + 1 = 4294967294 (one more wraps to 0,
     * as measured). The ledger supplies struct, ns_parts, ns_data and events
     * (requests 0..3) and refuses the NOMINATED ring request (4), so the
     * hundreds-of-GB size is measured, never performed (the ceiling would
     * refuse it too). The constructor's own unwind frees the four blocks. A
     * valid configuration refused by its allocator reports the allocator's
     * NOMEM; that is the honest result at this boundary, not a validation. */
    const char *what = "b6.max_objects.boundary_2863311529";
    size_t ev_entry = 0, obj_entry = 0;
    moq_media_receiver_test_entry_sizes(&ev_entry, &obj_entry);
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, parts, 2);
    cfg.overflow.max_objects = 2863311529u;
    arm(4);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_test_construct(&cfg, &r);
    disarm();
    ROW(what, "allocator_refusal_is_nomem", rc == MOQ_ERR_NOMEM && r == NULL);
    ROW(what, "five_requests", g_ledger.requests == 5);
    ROW(what, "one_nominated_refusal", g_ledger.refused == 1);
    ROW(what, "size.events", g_ledger.requests >= 4 && g_ledger.requested[3] == (size_t)64 * ev_entry);
    ROW(what, "size.ring_nominated", g_ledger.refused_size == (size_t)4294967294u * obj_entry);
    ROW(what, "unwound_exactly", g_ledger.supplied == 4 && g_ledger.freed == 4);
    if (r) moq_media_receiver_test_free(r);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());
}
#else
/* ILP32: ring_cap * sizeof(receiver_obj_entry_t) wraps size_t for this
 * input, so it must be refused before any request. Compiled only for a
 * 32-bit size_t; not executed or claimed on this LP64 host. */
static void test_b6_ilp32_boundary_ring_product_wrap(void)
{
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_FLOW_CONTROL, parts, 2);
    cfg.overflow.max_objects = 2863311529u;
    expect_refused(&cfg, "b6.max_objects.boundary_2863311529.ilp32_ring_product_size_t_wrap");
    expect_refused_public(&cfg, "b6.max_objects.boundary_2863311529.ilp32_ring_product_size_t_wrap");
}
#endif

/* Positive public control: a valid configuration attaches exactly once and
 * detaches exactly once, with the same seven requests and a balanced ledger. */
static void test_b7_public_attach_counts(void)
{
    const char *what = "b7.public.defaults";
    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    ROW(what, "fixture_endpoint", ep != NULL);
    if (!ep) return;
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, parts, 2);
    arm(-1);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_attach(ep, &cfg, &r);
    disarm();
    ROW(what, "result_ok", rc == MOQ_OK && r != NULL);
    ROW(what, "seven_requests", g_ledger.requests == 7);
    ROW(what, "attached_once", moq_endpoint_test_attachments(ep) == 1);
    if (r) moq_media_receiver_destroy(r);     /* public destroy: detach + free, no subscriber yet */
    ROW(what, "detached", moq_endpoint_test_attachments(ep) == 0);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());
    moq_endpoint_test_free_bare(ep);
}

/* An intrinsically unrepresentable part count (count * sizeof(moq_bytes_t)
 * cannot fit size_t) must be refused BEFORE any part is read, at every
 * boundary. One real element is allocated; the pre-traversal probe both
 * records and vetoes the walk, so the fixture is finite in either order. */
static void test_b8_namespace_count_unrepresentable(void)
{
    const char *what = "b8.namespace_.count_unrepresentable";
    moq_bytes_t *part = (moq_bytes_t *)malloc(sizeof(*part));
    ROW(what, "fixture.part", part != NULL);
    if (!part) return;
    *part = (moq_bytes_t){ NS0, 4 };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, part,
                                            SIZE_MAX / sizeof(moq_bytes_t) + 1);
    moq_media_receiver_test_set_parts_walk_hook(probe_walk, &g_walk);

    memset(&g_walk, 0, sizeof(g_walk)); arm(1);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_test_construct(&cfg, &r);
    disarm();
    ROW(what, "result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "no_receiver", r == NULL);
    ROW(what, "walk_not_entered", g_walk.calls == 0);
    ROW(what, "zero_requests", g_ledger.requests == 0);

    moq_endpoint_t *ep = moq_endpoint_test_make_bare();
    ROW(what, "public.fixture_endpoint", ep != NULL);
    if (ep) {
        memset(&g_walk, 0, sizeof(g_walk)); arm(1);
        r = NULL;
        rc = moq_media_receiver_attach(ep, &cfg, &r);
        disarm();
        ROW(what, "public.result_is_inval", rc == MOQ_ERR_INVAL);
        ROW(what, "public.no_receiver", r == NULL);
        ROW(what, "public.walk_not_entered", g_walk.calls == 0);
        ROW(what, "public.zero_requests", g_ledger.requests == 0);
        ROW(what, "public.zero_attachments", moq_endpoint_test_attachments(ep) == 0);
        moq_endpoint_test_free_bare(ep);
    }

    moq_endpoint_cfg_t ep_cfg; owning_cfg(&cfg, &ep_cfg);
    memset(&g_walk, 0, sizeof(g_walk)); arm(1); arm_connect(MOQ_ERR_UNSUPPORTED);
    r = NULL;
    rc = moq_media_receiver_create(&cfg, &r);
    disarm_connect(); disarm();
    ROW(what, "create.result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "create.no_receiver", r == NULL);
    ROW(what, "create.walk_not_entered", g_walk.calls == 0);
    ROW(what, "create.zero_connects", g_connect.calls == 0);
    ROW(what, "create.zero_requests", g_ledger.requests == 0);

    moq_media_receiver_test_set_parts_walk_hook(NULL, NULL);
    free(part);
}

/* Valid owning control: preflight passes, the endpoint boundary is called
 * exactly once, its real failure code is returned unchanged, nothing was
 * allocated. */
static void test_b9_create_refused_connect_control(void)
{
    const char *what = "b9.create.refused_connect";
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, parts, 2);
    moq_endpoint_cfg_t ep_cfg; owning_cfg(&cfg, &ep_cfg);
    arm(-1); arm_connect(MOQ_ERR_UNSUPPORTED);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_create(&cfg, &r);
    disarm_connect(); disarm();
    ROW(what, "result_is_boundary_code", rc == MOQ_ERR_UNSUPPORTED);
    ROW(what, "no_receiver", r == NULL);
    ROW(what, "one_connect", g_connect.calls == 1);
    ROW(what, "zero_requests", g_ledger.requests == 0);
    if (r) moq_media_receiver_destroy(r);
    ROW(what, "ledger_clean", ledger_clean() && outstanding() == 0);
}

#if SIZE_MAX > 0xFFFFFFFFu
/* LP64 only: max_track_events at its uint32 maximum is a VALID configuration
 * whose event allocation (UINT32_MAX * sizeof(event)) is the nominated giant
 * request; the ledger refuses it and the constructor unwinds. Distinct from
 * the object ring boundary (b6): the events request is index 3, before any
 * ring request. */
static void test_b10_boundary_events_reaches_exact_nominated_request(void)
{
    const char *what = "b10.max_track_events.boundary_uint32_max";
    size_t ev_entry = 0, obj_entry = 0;
    moq_media_receiver_test_entry_sizes(&ev_entry, &obj_entry);
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, parts, 2);
    cfg.max_track_events = UINT32_MAX;
    arm(3);
    moq_media_receiver_t *r = NULL;
    moq_result_t rc = moq_media_receiver_test_construct(&cfg, &r);
    disarm();
    ROW(what, "allocator_refusal_is_nomem", rc == MOQ_ERR_NOMEM && r == NULL);
    ROW(what, "four_requests", g_ledger.requests == 4);
    ROW(what, "one_nominated_refusal", g_ledger.refused == 1);
    ROW(what, "size.events_nominated", g_ledger.refused_size == (size_t)UINT32_MAX * ev_entry);
    ROW(what, "unwound_exactly", g_ledger.supplied == 3 && g_ledger.freed == 3);
    if (r) moq_media_receiver_test_free(r);
    ROW(what, "zero_outstanding", outstanding() == 0);
    ROW(what, "ledger_clean", ledger_clean());
}
#else
/* ILP32: UINT32_MAX * sizeof(receiver_event_t) wraps size_t, so the same
 * input belongs to the refusal class, independently of the object ring
 * (default geometry otherwise). Compiled only for a 32-bit size_t; not
 * executed or claimed on this LP64 host. */
static void test_b10_ilp32_events_product_wrap(void)
{
    moq_bytes_t parts[2] = { { NS0, 4 }, { NS1, 4 } };
    moq_media_receiver_cfg_t cfg; base_cfg(&cfg, MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME, parts, 2);
    cfg.max_track_events = UINT32_MAX;
    expect_refused(&cfg, "b10.max_track_events.ilp32_event_product_size_t_wrap");
    expect_refused_public(&cfg, "b10.max_track_events.ilp32_event_product_size_t_wrap");
    expect_refused_create(&cfg, "b10.max_track_events.ilp32_event_product_size_t_wrap");
}
#endif

int main(void)
{
    test_b1_flow_control_ring_wrap();
    test_b2_flow_control_byte_ceiling_wrap();
    test_b3_catalog_name_addition_wrap();
    test_b4_namespace_sum_wrap();
    test_b5_defaults_request_exact_sizes();
#if SIZE_MAX > 0xFFFFFFFFu
    test_b6_boundary_ring_reaches_exact_nominated_request();
    test_b10_boundary_events_reaches_exact_nominated_request();
    fprintf(stderr, "construct_arith: LP64 build; ILP32 size_t product-wrap rows NOT executed (qualification gap)\n");
#else
    test_b6_ilp32_boundary_ring_product_wrap();
    test_b10_ilp32_events_product_wrap();
    fprintf(stderr, "construct_arith: ILP32 build; LP64 boundary allocation rows NOT executed\n");
#endif
    test_b7_public_attach_counts();
    test_b8_namespace_count_unrepresentable();
    test_b9_create_refused_connect_control();
    if (failures) {
        fprintf(stderr, "media_receiver_construct_arith: %d failure(s)\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("media_receiver_construct_arith");
    return 0;
}
