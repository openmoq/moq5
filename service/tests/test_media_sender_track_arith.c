/*
 * Contract test for checked media-sender add_track arithmetic.
 *
 * add_track derives (1) the span aggregate name + codec + init_data + role +
 * lang + channel_config + ref-id bytes (size_t) for one strings buffer,
 * (2) the content-protection ref-id index product count * sizeof(moq_bytes_t)
 * and the ref-id traversal that precedes it, and (3) the generated timeline
 * name addition media name + ".sap" / ".timeline" (sender_make_timeline).
 * Each must be checked BEFORE the first allocation request; an
 * unrepresentable configuration is refused with MOQ_ERR_INVAL, registers
 * nothing, dirties no catalog, and makes ZERO allocation requests. A
 * representable configuration reaches the allocator with exactly the derived
 * sizes, and an allocator refusal is MOQ_ERR_NOMEM with exact cleanup and no
 * registry/catalog effect.
 *
 * Oracle: the sender is constructed through the test allocator seam so one
 * LEDGER lives from construction through destruction (every alloc/realloc
 * request in order, exact-size frees, nominated refusal in fail-ONCE or
 * fail-from-here mode, a finite per-request ceiling so a giant size is
 * measured, never performed). Forged lengths are paired with small VALID data
 * pointers: pointer validity and size representability are distinct
 * preconditions, and no forged length is ever copied or compared -- the
 * ledger refuses the buffer before any copy, and the ref-id pre-traversal
 * witness vetoes the walk before any element is read.
 *
 * Resolved ref-id BYTES are part of the same aggregate: a ref that RESOLVES
 * to a real root content-protection still adds its length to the six media
 * spans, so one small valid ref can wrap an otherwise representable sum.
 * That is measured here, not excluded. The SAP/media-timeline record
 * histories grow at emit time, not in add_track: a separate mechanism, not
 * covered here.
 *
 * Platform gate: the ref-count threshold is DERIVED from
 * SIZE_MAX / sizeof(moq_bytes_t), so one row expresses the first overflowing
 * count on either target; compile-time assertions pin the fitting and the
 * overflowing side, and the concrete ILP32 value. Span boundaries are
 * representable inputs whose giant request is refused by the ledger. Neither
 * build claims the other's execution.
 *
 * White-box: links moq-service-sender-test-internals (MOQ_MEDIA_SENDER_TESTING).
 */
#include <moq/media_sender.h>
#include "test_support.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

moq_result_t moq_media_sender_test_construct(const moq_media_sender_cfg_t *cfg,
                                             moq_media_sender_t **out);
void moq_media_sender_test_free(moq_media_sender_t *s);
void moq_media_sender_test_set_alloc(const moq_alloc_t *alloc);
void moq_media_sender_test_entry_sizes(size_t *ring_entry, size_t *track);
void moq_media_sender_test_set_refs_walk_hook(bool (*fn)(size_t, void *), void *ctx);
void moq_media_sender_test_set_ready(moq_media_sender_t *s);
bool moq_media_sender_test_catalog_dirty(const moq_media_sender_t *s);
size_t moq_media_sender_test_track_count(moq_media_sender_t *s);

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
#define MAX_REQ 64
#define REQUEST_CEILING ((size_t)64u << 20)
typedef struct { void *ptr; size_t size; bool live; int request; } block_t;
typedef struct { int request; size_t supplied; size_t freed; } bad_free_t;
typedef struct {
    size_t  requested[MAX_REQ];
    int     requests;
    block_t blocks[MAX_REQ];
    int     supplied;
    int     freed;
    int     refuse_at;            /* nominated 0-based request index; -1 never */
    bool    refuse_once;
    int     refused;
    size_t  refused_size;
    int     overflow;
    int     oversize;
    int     bad_free;
    bad_free_t bad[MAX_REQ];
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
static bool ledger_release(ledger_t *l, void *p, size_t size)
{
    for (int i = 0; i < l->supplied; i++) {
        block_t *b = &l->blocks[i];
        if (b->live && b->ptr == p) {
            if (b->size != size) {
                if (l->bad_free < MAX_REQ)
                    l->bad[l->bad_free] = (bad_free_t){ b->request, b->size, size };
                l->bad_free++;
            }
            b->live = false; l->freed++; free(p);
            return true;
        }
    }
    l->bad_free++;
    return false;
}
static void ledger_free(void *p, size_t size, void *ctx)
{
    if (!p) return;
    (void)ledger_release((ledger_t *)ctx, p, size);
}
static void *ledger_realloc(void *p, size_t old, size_t nw, void *ctx)
{
    void *np = ledger_alloc(nw, ctx);
    if (!np) return NULL;
    if (p) { memcpy(np, p, old < nw ? old : nw); (void)ledger_release((ledger_t *)ctx, p, old); }
    return np;
}
static void arm_ledger(void)
{
    memset(&g_ledger, 0, sizeof(g_ledger));
    g_ledger.refuse_at = -1;
    g_alloc.alloc = ledger_alloc; g_alloc.free = ledger_free;
    g_alloc.realloc = ledger_realloc; g_alloc.ctx = &g_ledger;
    moq_media_sender_test_set_alloc(&g_alloc);
}
/* Nominate a refusal relative to the CURRENT request count (the sender keeps
 * allocating through the ledger after construction). */
static void refuse_from_next(void) { g_ledger.refuse_at = g_ledger.requests; g_ledger.refuse_once = false; g_ledger.refused = 0; }
static void refuse_once_at_offset(int off) { g_ledger.refuse_at = g_ledger.requests + off; g_ledger.refuse_once = true; g_ledger.refused = 0; }
static void refuse_never(void) { g_ledger.refuse_at = -1; }
static int outstanding(void) { return g_ledger.supplied - g_ledger.freed; }
static bool ledger_clean(void) { return g_ledger.overflow == 0 && g_ledger.oversize == 0 && g_ledger.bad_free == 0; }
static void dump(const char *what)
{
    fprintf(stderr, "  ledger[%s]: requests=%d supplied=%d freed=%d refused=%d oversize=%d bad_free=%d sizes=",
            what, g_ledger.requests, g_ledger.supplied, g_ledger.freed, g_ledger.refused,
            g_ledger.oversize, g_ledger.bad_free);
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
    dump(what);
    failures++;
}
#define ROW(what, name, cond) row((what), (name), (cond), __LINE__)

/* count * sizeof(moq_bytes_t) fits size_t exactly while count <= this. The
 * first OVERFLOWING count is REF_COUNT_MAX + 1 on either target; both sides
 * are pinned at compile time using the division predicate (a widened
 * multiplication would itself overflow on LP64). */
#define REF_ELEM (sizeof(moq_bytes_t))
#define REF_COUNT_MAX (SIZE_MAX / REF_ELEM)
_Static_assert(REF_COUNT_MAX <= SIZE_MAX / REF_ELEM,
               "fitting side: REF_COUNT_MAX * sizeof(moq_bytes_t) fits size_t");
_Static_assert(REF_COUNT_MAX + 1u > SIZE_MAX / REF_ELEM,
               "overflowing side: (REF_COUNT_MAX + 1) * sizeof(moq_bytes_t) cannot fit size_t");
#if SIZE_MAX <= 0xFFFFFFFFu
/* Arm GNU target layout: sizeof(size_t)=4, sizeof(moq_bytes_t)=8. */
_Static_assert(REF_ELEM == 8u, "ILP32 moq_bytes_t layout");
_Static_assert(REF_COUNT_MAX + 1u == 0x20000000u, "ILP32 first overflowing ref count");
#else
_Static_assert(REF_ELEM == 16u, "LP64 moq_bytes_t layout");
#endif

static const uint8_t NS0[] = "live";
static const uint8_t NAME[] = "video";
static const uint8_t CODEC[] = "avc1.64001f";
#define NAME_LEN 5
#define CODEC_LEN 11
static const size_t SAP_SUFFIX = 4;   /* ".sap" */
static const size_t MT_SUFFIX = 9;    /* ".timeline" */

typedef struct { int requests, supplied, freed, bad_free; } snap_t;
static snap_t snap(void) { return (snap_t){ g_ledger.requests, g_ledger.supplied, g_ledger.freed, g_ledger.bad_free }; }

static moq_media_sender_t *ledger_sender(const char *what)
{
    static const moq_bytes_t parts[1] = { { NS0, 4 } };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_.parts = parts; cfg.namespace_.count = 1;
    arm_ledger();
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_test_construct(&cfg, &s);
    ROW(what, "fixture.sender", rc == MOQ_OK && s != NULL);
    return s;
}

/* A sender carrying one REAL root content-protection, so a track ref id can
 * actually resolve (the service deep-copies the entry at construction). */
static const uint8_t CP_REF[] = "cp1";
#define CP_REF_LEN 3
static const uint8_t CP_SCHEME[] = "cenc";
static const uint8_t CP_KID[] = "12345678-1234-1234-1234-123456789abc";
static const uint8_t CP_SYSID[] = "87654321-4321-4321-4321-cba987654321";

static moq_media_sender_t *ledger_sender_cp(const char *what)
{
    static const moq_bytes_t parts[1] = { { NS0, 4 } };
    static moq_bytes_t kids[1];
    static moq_cmsf_content_protection_t cp;
    kids[0] = (moq_bytes_t){ CP_KID, 36 };
    memset(&cp, 0, sizeof(cp));
    cp.ref_id = (moq_bytes_t){ CP_REF, CP_REF_LEN };
    cp.default_kids = kids; cp.default_kid_count = 1;
    cp.scheme = (moq_bytes_t){ CP_SCHEME, 4 };
    cp.drm_system.system_id = (moq_bytes_t){ CP_SYSID, 36 };
    moq_media_sender_cfg_t cfg;
    moq_media_sender_cfg_init_live_sized(&cfg, sizeof(cfg));
    cfg.namespace_.parts = parts; cfg.namespace_.count = 1;
    cfg.content_protections = &cp;
    cfg.content_protection_count = 1;
    arm_ledger();
    moq_media_sender_t *s = NULL;
    moq_result_t rc = moq_media_sender_test_construct(&cfg, &s);
    ROW(what, "fixture.sender_with_root_cp", rc == MOQ_OK && s != NULL);
    return s;
}
static void finish(const char *what, moq_media_sender_t *s)
{
    refuse_never();
    if (s) moq_media_sender_test_free(s);
    moq_media_sender_test_set_alloc(NULL);
    ROW(what, "final.zero_outstanding", outstanding() == 0);
    ROW(what, "final.ledger_clean", ledger_clean());
}
static void track_cfg(moq_media_track_cfg_t *tc)
{
    moq_media_track_cfg_init(tc);
    tc->name = (moq_bytes_t){ NAME, NAME_LEN };
    tc->media_type = MOQ_MEDIA_TYPE_VIDEO;
    tc->packaging = MOQ_MEDIA_PACKAGING_RAW;
    tc->codec = (moq_bytes_t){ CODEC, CODEC_LEN };
    tc->bitrate = 1000000;
}

/* -- refusal rows: INVAL before any request, nothing registered ------------ */
static void expect_refused(moq_media_sender_t *s, const moq_media_track_cfg_t *tc, const char *what)
{
    size_t before = moq_media_sender_test_track_count(s);
    bool dirty_before = moq_media_sender_test_catalog_dirty(s);
    snap_t a = snap();
    refuse_from_next();                 /* nothing may be supplied from here */
    moq_media_track_t *t = NULL;
    moq_result_t rc = moq_media_sender_add_track(s, tc, &t);
    refuse_never();
    snap_t b = snap();
    if (rc != MOQ_ERR_INVAL || b.requests != a.requests)
        fprintf(stderr, "  %s: measured result %d (INVAL is %d, NOMEM is %d), requests +%d\n",
                what, (int)rc, (int)MOQ_ERR_INVAL, (int)MOQ_ERR_NOMEM, b.requests - a.requests);
    ROW(what, "result_is_inval", rc == MOQ_ERR_INVAL);
    ROW(what, "no_handle", t == NULL);
    ROW(what, "zero_requests", b.requests == a.requests);
    ROW(what, "registry_unchanged", moq_media_sender_test_track_count(s) == before);
    ROW(what, "catalog_not_dirtied", moq_media_sender_test_catalog_dirty(s) == dirty_before);
    ROW(what, "outstanding_unchanged", (b.supplied - b.freed) == (a.supplied - a.freed));
    ROW(what, "exact_frees", b.bad_free == a.bad_free);
}

static void test_a1_span_aggregate_wrap(void)
{
    /* name SIZE_MAX-8 + codec 11 wraps the strings request to 2. The name
     * data pointer is a valid 5-byte literal; the length is forged and is
     * never copied (the ledger refuses the buffer, and today's product would
     * only reach the copy after an allocation this row forbids). */
    const char *what = "a1.spans.name_plus_codec_size_t_wrap";
    moq_media_sender_t *s = ledger_sender(what); if (!s) return;
    moq_media_track_cfg_t tc; track_cfg(&tc);
    tc.name = (moq_bytes_t){ NAME, SIZE_MAX - 8 };
    expect_refused(s, &tc, what);
    /* the same wrap through init_data: name 5 + codec 11 + init SIZE_MAX-10 */
    moq_media_track_cfg_t tc2; track_cfg(&tc2);
    tc2.init_data = (moq_bytes_t){ CODEC, SIZE_MAX - 10 };
    expect_refused(s, &tc2, "a1.spans.init_data_size_t_wrap");
    finish(what, s);
}

static void test_a2_ref_count_unrepresentable(void)
{
    /* REF_COUNT_MAX + 1 is the FIRST count whose index product cannot fit
     * size_t on this target (both sides pinned by the assertions above), so it
     * must be refused BEFORE the ref ids are walked. One real element exists
     * and the array is never enlarged: the pre-traversal probe records the
     * walk and vetoes it, keeping the fixture finite whatever the product
     * does. NOTE ON CAUSALITY: while the probe is installed the INVAL comes
     * from the probe's own veto, not from a size check and not from ref
     * resolution -- `walk_not_entered` is the load-bearing observation. */
    const char *what = "a2.refs.count_unrepresentable";
    moq_media_sender_t *s = ledger_sender(what); if (!s) return;
    moq_bytes_t *ref = (moq_bytes_t *)malloc(sizeof(*ref));
    ROW(what, "fixture.ref", ref != NULL);
    if (ref) {
        *ref = (moq_bytes_t){ NS0, 4 };
        moq_media_track_cfg_t tc; track_cfg(&tc);
        tc.content_protection_ref_ids = ref;
        tc.content_protection_ref_id_count = REF_COUNT_MAX + 1u;
        memset(&g_walk, 0, sizeof(g_walk));
        moq_media_sender_test_set_refs_walk_hook(probe_walk, &g_walk);
        expect_refused(s, &tc, what);
        moq_media_sender_test_set_refs_walk_hook(NULL, NULL);
        ROW(what, "walk_not_entered", g_walk.calls == 0);
        ROW(what, "walk_count_if_entered", g_walk.calls == 0 || g_walk.count == REF_COUNT_MAX + 1u);
        free(ref);
    }
    finish(what, s);
}

/* Each generated sibling derives its own name = media name + suffix, in
 * sender_make_timeline -- a mechanism distinct from the media span aggregate.
 * Each arm keeps the MEDIA aggregate representable and overflows only the
 * sibling's addition, so the refusal can only come from the sibling check.
 * The suffix lengths are declared here, independently of the product. */
static void timeline_arm(const char *what, size_t name_len, bool sap, bool mt)
{
    moq_media_sender_t *s = ledger_sender(what); if (!s) return;
    moq_media_track_cfg_t tc; track_cfg(&tc);
    tc.name = (moq_bytes_t){ NAME, name_len };
    tc.codec = (moq_bytes_t){ CODEC, 1 };            /* media aggregate = name + 1 */
    tc.emit_sap_timeline = sap;
    tc.emit_media_timeline = mt;
    expect_refused(s, &tc, what);
    finish(what, s);
}

static void test_a3a_sap_name_addition_wrap(void)
{
    /* name SIZE_MAX-3: media aggregate SIZE_MAX-2 is representable, the SAP
     * name SIZE_MAX-3 + 4 wraps to 0. */
    timeline_arm("a3a.sap_timeline.name_plus_suffix_size_t_wrap", SIZE_MAX - 3, true, false);
}

static void test_a3b_media_timeline_name_addition_wrap(void)
{
    /* name SIZE_MAX-8: media aggregate SIZE_MAX-7 is representable, the
     * media-timeline name SIZE_MAX-8 + 9 wraps to 0. */
    timeline_arm("a3b.media_timeline.name_plus_suffix_size_t_wrap", SIZE_MAX - 8, false, true);
}

/* Disabled-sibling positive controls: the SAME names with no sibling enabled
 * are representable configurations, so they must reach the allocator (NOMEM at
 * the nominated strings request), never INVAL. These keep a blanket refusal
 * from passing the two arms above. */
static void timeline_disabled_control(const char *what, size_t name_len, size_t expect_strings)
{
    moq_media_sender_t *s = ledger_sender(what); if (!s) return;
    size_t track_size = 0; moq_media_sender_test_entry_sizes(NULL, &track_size);
    moq_media_track_cfg_t tc; track_cfg(&tc);
    tc.name = (moq_bytes_t){ NAME, name_len };
    tc.codec = (moq_bytes_t){ CODEC, 1 };
    size_t before = moq_media_sender_test_track_count(s);
    snap_t a = snap();
    refuse_once_at_offset(1);
    moq_media_track_t *t = NULL;
    moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
    refuse_never();
    snap_t b = snap();
    ROW(what, "allocator_refusal_is_nomem", rc == MOQ_ERR_NOMEM && t == NULL);
    ROW(what, "first_is_track_struct", b.requests - a.requests >= 1 && g_ledger.requested[a.requests] == track_size);
    ROW(what, "nominated_is_strings", g_ledger.refused == 1 && g_ledger.refused_size == expect_strings);
    ROW(what, "registry_unchanged", moq_media_sender_test_track_count(s) == before);
    ROW(what, "unwound_exactly", (b.supplied - b.freed) == (a.supplied - a.freed) && b.bad_free == a.bad_free);
    finish(what, s);
}

static void test_r3_timeline_names_disabled_are_representable(void)
{
    timeline_disabled_control("r3a.sap_name_len_no_sibling", SIZE_MAX - 3, SIZE_MAX - 2);
    timeline_disabled_control("r3b.mt_name_len_no_sibling", SIZE_MAX - 8, SIZE_MAX - 7);
}

/* -- resolved ref-id bytes join the same aggregate ------------------------- */
static void test_a4_resolved_ref_bytes_overflow_aggregate(void)
{
    /* name SIZE_MAX-12 + codec 11 = SIZE_MAX-1 is representable (the without-ref
     * control below measures exactly that request). Adding ONE real, RESOLVING
     * 3-byte ref id wraps the aggregate to 1. The ref array holds one genuine
     * element; nothing is forged inside it. */
    const char *what = "a4.refs.resolved_bytes_overflow_aggregate";
    moq_media_sender_t *s = ledger_sender_cp(what); if (!s) return;
    size_t track_size = 0; moq_media_sender_test_entry_sizes(NULL, &track_size);

    /* control: the same media spans WITHOUT the ref are representable. */
    {
        const char *cw = "r2.spans.boundary_without_ref";
        moq_media_track_cfg_t tc; track_cfg(&tc);
        tc.name = (moq_bytes_t){ NAME, SIZE_MAX - 12 };
        snap_t a = snap();
        refuse_once_at_offset(1);
        moq_media_track_t *t = NULL;
        moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
        refuse_never();
        snap_t b = snap();
        ROW(cw, "allocator_refusal_is_nomem", rc == MOQ_ERR_NOMEM && t == NULL);
        ROW(cw, "first_is_track_struct", b.requests - a.requests >= 1 && g_ledger.requested[a.requests] == track_size);
        ROW(cw, "nominated_is_strings", g_ledger.refused == 1 && g_ledger.refused_size == SIZE_MAX - 1);
        ROW(cw, "registry_unchanged", moq_media_sender_test_track_count(s) == 0);
        ROW(cw, "unwound_exactly", (b.supplied - b.freed) == (a.supplied - a.freed) && b.bad_free == a.bad_free);
    }

    /* the same spans PLUS one resolving ref: the sum wraps. */
    moq_bytes_t ref = { CP_REF, CP_REF_LEN };
    moq_media_track_cfg_t tc; track_cfg(&tc);
    tc.name = (moq_bytes_t){ NAME, SIZE_MAX - 12 };
    tc.content_protection_ref_ids = &ref;
    tc.content_protection_ref_id_count = 1;
    expect_refused(s, &tc, what);
    finish(what, s);
}

static void test_c4_valid_resolved_ref_exact_requests(void)
{
    /* A small valid resolving ref: strings carry the media spans AND the ref
     * bytes, the index array is one element, the track registers. */
    const char *what = "c4.valid_resolved_ref";
    moq_media_sender_t *s = ledger_sender_cp(what); if (!s) return;
    size_t track_size = 0; moq_media_sender_test_entry_sizes(NULL, &track_size);
    moq_bytes_t ref = { CP_REF, CP_REF_LEN };
    moq_media_track_cfg_t tc; track_cfg(&tc);
    tc.content_protection_ref_ids = &ref;
    tc.content_protection_ref_id_count = 1;
    snap_t a = snap();
    moq_media_track_t *t = NULL;
    moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
    snap_t b = snap();
    ROW(what, "result_ok", rc == MOQ_OK && t != NULL);
    ROW(what, "four_requests", b.requests - a.requests == 4 && b.supplied - a.supplied == 4);
    if (b.requests - a.requests == 4) {
        const size_t *r = &g_ledger.requested[a.requests];
        ROW(what, "size.track", r[0] == track_size);
        ROW(what, "size.strings_with_ref", r[1] == NAME_LEN + CODEC_LEN + CP_REF_LEN);
        ROW(what, "size.ref_index", r[2] == REF_ELEM);
        ROW(what, "size.registry", r[3] == 4 * sizeof(void *));
    }
    ROW(what, "registered_one", moq_media_sender_test_track_count(s) == 1);
    finish(what, s);
}

/* -- representable side: the giant but valid request is measured, never performed */
static void test_r1_boundary_span_reaches_exact_nominated_request(void)
{
    /* name SIZE_MAX-12 + codec 11 = SIZE_MAX-1: representable, so this is a
     * VALID configuration whose strings request is the nominated refusal
     * (fail-once at offset 1, after the track struct). NOMEM, exact unwind,
     * nothing registered. The forged length is never copied. */
    const char *what = "r1.spans.boundary_size_max_minus_1";
    moq_media_sender_t *s = ledger_sender(what); if (!s) return;
    size_t track_size = 0; moq_media_sender_test_entry_sizes(NULL, &track_size);
    moq_media_track_cfg_t tc; track_cfg(&tc);
    tc.name = (moq_bytes_t){ NAME, SIZE_MAX - 12 };
    size_t before = moq_media_sender_test_track_count(s);
    snap_t a = snap();
    refuse_once_at_offset(1);
    moq_media_track_t *t = NULL;
    moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
    refuse_never();
    snap_t b = snap();
    ROW(what, "allocator_refusal_is_nomem", rc == MOQ_ERR_NOMEM && t == NULL);
    ROW(what, "two_requests", b.requests - a.requests == 2);
    ROW(what, "first_is_track_struct", b.requests - a.requests >= 1 && g_ledger.requested[a.requests] == track_size);
    ROW(what, "nominated_is_strings", g_ledger.refused == 1 && g_ledger.refused_size == SIZE_MAX - 1);
    ROW(what, "unwound_exactly", (b.supplied - b.freed) == (a.supplied - a.freed) && b.bad_free == a.bad_free);
    ROW(what, "registry_unchanged", moq_media_sender_test_track_count(s) == before);
    finish(what, s);
}

/* -- valid controls: exact requests, registry and catalog effects ---------- */
static void test_c1_defaults_exact_requests(void)
{
    const char *what = "c1.defaults";
    moq_media_sender_t *s = ledger_sender(what); if (!s) return;
    size_t track_size = 0; moq_media_sender_test_entry_sizes(NULL, &track_size);
    moq_media_track_cfg_t tc; track_cfg(&tc);
    snap_t a = snap();
    moq_media_track_t *t = NULL;
    moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
    snap_t b = snap();
    ROW(what, "result_ok", rc == MOQ_OK && t != NULL);
    ROW(what, "three_requests", b.requests - a.requests == 3 && b.supplied - a.supplied == 3);
    if (b.requests - a.requests == 3) {
        ROW(what, "size.track", g_ledger.requested[a.requests] == track_size);
        ROW(what, "size.strings", g_ledger.requested[a.requests + 1] == NAME_LEN + CODEC_LEN);
        ROW(what, "size.registry", g_ledger.requested[a.requests + 2] == 4 * sizeof(void *));
    }
    ROW(what, "registered_one", moq_media_sender_test_track_count(s) == 1);
    ROW(what, "pre_ready_not_dirty", !moq_media_sender_test_catalog_dirty(s));
    /* post-ready: a second add dirties the catalog (no endpoint: no wake). */
    moq_media_sender_test_set_ready(s);
    static const uint8_t NAME2[] = "audio2";
    moq_media_track_cfg_t tc2; track_cfg(&tc2);
    tc2.name = (moq_bytes_t){ NAME2, 6 };
    moq_media_track_t *t2 = NULL;
    ROW(what, "post_ready.add_ok", moq_media_sender_add_track(s, &tc2, &t2) == MOQ_OK && t2 != NULL);
    ROW(what, "post_ready.dirty", moq_media_sender_test_catalog_dirty(s));
    ROW(what, "post_ready.registered_two", moq_media_sender_test_track_count(s) == 2);
    finish(what, s);
}

static void test_c2_generated_timelines_exact_requests(void)
{
    /* Both generated siblings: media track (struct, strings), SAP timeline
     * (struct, name 5+4, depends index), media timeline (struct, name 5+9,
     * depends index), then the registry vector: 9 requests, 3 registered. */
    const char *what = "c2.timelines";
    moq_media_sender_t *s = ledger_sender(what); if (!s) return;
    size_t track_size = 0; moq_media_sender_test_entry_sizes(NULL, &track_size);
    moq_media_track_cfg_t tc; track_cfg(&tc);
    tc.emit_sap_timeline = true;
    tc.emit_media_timeline = true;
    snap_t a = snap();
    moq_media_track_t *t = NULL;
    moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
    snap_t b = snap();
    ROW(what, "result_ok", rc == MOQ_OK && t != NULL);
    ROW(what, "nine_requests", b.requests - a.requests == 9 && b.supplied - a.supplied == 9);
    if (b.requests - a.requests == 9) {
        const size_t *r = &g_ledger.requested[a.requests];
        ROW(what, "size.track", r[0] == track_size);
        ROW(what, "size.strings", r[1] == NAME_LEN + CODEC_LEN);
        ROW(what, "size.sap_track", r[2] == track_size);
        ROW(what, "size.sap_name", r[3] == NAME_LEN + SAP_SUFFIX);
        ROW(what, "size.sap_depends", r[4] == sizeof(moq_bytes_t));
        ROW(what, "size.mt_track", r[5] == track_size);
        ROW(what, "size.mt_name", r[6] == NAME_LEN + MT_SUFFIX);
        ROW(what, "size.mt_depends", r[7] == sizeof(moq_bytes_t));
        ROW(what, "size.registry", r[8] == 4 * sizeof(void *));
    }
    ROW(what, "registered_three", moq_media_sender_test_track_count(s) == 3);
    finish(what, s);
}

static void test_c3_nomem_once_unwinds_exactly(void)
{
    /* Refuse exactly one of the nine requests of the timeline configuration:
     * NOMEM, no handle, registry unchanged, catalog not dirtied, every
     * supplied block freed with its exact size. */
    for (int at = 0; at < 9; at++) {
        char what[48];
        snprintf(what, sizeof(what), "c3.nomem_once_at_offset_%d", at);
        moq_media_sender_t *s = ledger_sender(what); if (!s) continue;
        moq_media_track_cfg_t tc; track_cfg(&tc);
        tc.emit_sap_timeline = true;
        tc.emit_media_timeline = true;
        snap_t a = snap();
        refuse_once_at_offset(at);
        moq_media_track_t *t = NULL;
        moq_result_t rc = moq_media_sender_add_track(s, &tc, &t);
        refuse_never();
        snap_t b = snap();
        ROW(what, "result_is_nomem", rc == MOQ_ERR_NOMEM);
        ROW(what, "no_handle", t == NULL);
        ROW(what, "one_refusal", g_ledger.refused == 1);
        ROW(what, "registry_unchanged", moq_media_sender_test_track_count(s) == 0);
        ROW(what, "catalog_not_dirtied", !moq_media_sender_test_catalog_dirty(s));
        ROW(what, "unwound_exactly", (b.supplied - b.freed) == (a.supplied - a.freed));
        ROW(what, "exact_frees", b.bad_free == a.bad_free);
        finish(what, s);
    }
}

int main(void)
{
    test_a1_span_aggregate_wrap();
    test_a2_ref_count_unrepresentable();
    test_a3a_sap_name_addition_wrap();
    test_a3b_media_timeline_name_addition_wrap();
    test_a4_resolved_ref_bytes_overflow_aggregate();
    test_r1_boundary_span_reaches_exact_nominated_request();
    test_r3_timeline_names_disabled_are_representable();
    test_c1_defaults_exact_requests();
    test_c2_generated_timelines_exact_requests();
    test_c3_nomem_once_unwinds_exactly();
    test_c4_valid_resolved_ref_exact_requests();
    fprintf(stderr, "track_arith: sizeof(size_t)=%zu sizeof(moq_bytes_t)=%zu REF_COUNT_MAX+1=%zu;"
            " compile-time boundary assertions only -- no ILP32 execution\n",
            sizeof(size_t), REF_ELEM, (size_t)(REF_COUNT_MAX + 1u));
    if (failures) {
        fprintf(stderr, "media_sender_track_arith: %d failure(s)\n", failures);
        return 1;
    }
    MOQ_TEST_PASS("media_sender_track_arith");
    return 0;
}
