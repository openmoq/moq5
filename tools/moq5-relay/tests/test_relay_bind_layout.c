/*
 * Bind delivery-scheduler capacity oracle. White-box (includes moqr_bind.c):
 * pins the R8H-1c structural delta with a baseline b_conn_t replica (the
 * park fields must cost exactly one size_t — park_reason rides existing
 * bool padding), the widened bitset word arithmetic at the uint32_t
 * maximum, the descriptor slope across the 63/64/65 and 128/129 word
 * boundaries (ready + parked add exactly TWO 8-byte words per crossing),
 * the bind-owned pump scratch term, and the create-time counting-allocator
 * slope with leak-free destruction.
 */

#include "moqr_bind.c"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../../tests/unit/test_support.h"

/* counting allocator (same shape as test_relay_log) */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live, attempts;
    long fail_at;   /* fail exactly this allocation attempt; 0 = never */
} ca_t;
static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;
    }
    void *p = malloc(n);
    if (p) {
        a->allocs++;
        a->live += (long)n;
    }
    return p;
}
static void *ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    a->attempts++;
    if (a->fail_at != 0 && a->attempts == a->fail_at) {
        return NULL;
    }
    void *q = realloc(p, n);
    if (q) {
        a->live += (long)n - (long)o;
    }
    return q;
}
static void ca_f(void *p, size_t n, void *c)
{
    ca_t *a = c;
    if (p) {
        a->frees++;
        a->live -= (long)n;
    }
    free(p);
}
static void ca_init(ca_t *a)
{
    memset(a, 0, sizeof(*a));
    a->vt.ctx = a;
    a->vt.alloc = ca_a;
    a->vt.realloc = ca_r;
    a->vt.free = ca_f;
}

/* -- Layout pin: the park state costs exactly one size_t -------------------- */

typedef struct b_conn_baseline {
    bool           used;
    bool           closed;
    bool           detach_pending;
    moq_session_t *session;
    moqr_binding_t binding;
    b_dsub_t      *dsubs;
    b_sg_t        *sgs;
    b_usub_t      *usubs;
    b_pub_t       *pubs;
    b_ann_t       *anns;
    b_fetch_t     *fetches;
    uint64_t       requests_seen;
} b_conn_baseline_t;

_Static_assert(sizeof(b_conn_t) == sizeof(b_conn_baseline_t) + sizeof(size_t),
               "park_reason must ride bool padding; park_cap costs one size_t");

/* -- Word arithmetic: widened round-up ---------------------------------------- */

static int
test_bind_word_count(void)
{
    int failures = 0;
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(1), 1);
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(64), 1);
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(65), 2);
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(UINT32_MAX - 63), 67108863);
    MOQ_TEST_CHECK_EQ_U64(bind_dl_word_count(UINT32_MAX), 67108864);
    MOQ_TEST_PASS("bind_word_count");
    return failures;
}

/* -- Descriptor + allocator slopes across word boundaries --------------------- */

static uint64_t
bind_pump_scratch_bytes_test(void)
{
    return (uint64_t)BIND_PUMP_EVENT_BATCH * sizeof(moq_event_t) +
           (uint64_t)BIND_PUMP_REVOKED_BATCH *
               sizeof(moqr_revoked_grant_t) +
           (uint64_t)BIND_PUMP_INTENT_BATCH * sizeof(moqr_intent_t);
}

static uint64_t
describe_bind(ca_t *a, moqr_core_t *core, uint32_t max_conns)
{
    (void)core;
    moqr_bind_cfg_t cfg;
    moqr_bind_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.max_conns = max_conns;
    /* Fixed small tables so the per-conn slope is exact and cheap. */
    cfg.max_downstream_subs = 4;
    cfg.max_open_subgroups = 4;
    cfg.max_upstream_subs = 4;
    cfg.max_publishes = 4;
    cfg.max_announces = 4;
    moqr_core_limits_t lim;
    moqr_core_relay_cfg_t ccfg;
    moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), &a->vt);
    if (moqr_core_limits_resolve(&ccfg, &lim) != MOQR_OK) {
        return UINT64_MAX;
    }
    moqr_bind_capacity_t cap;
    if (moqr_bind_capacity_describe(&cfg, &lim, &cap) != MOQR_OK) {
        return UINT64_MAX;
    }
    return cap.structure_bytes;
}

static long
create_bind_live(ca_t *a, moqr_core_t *core, uint32_t max_conns)
{
    moqr_bind_cfg_t cfg;
    moqr_bind_cfg_init_sized(&cfg, sizeof(cfg), &a->vt);
    cfg.core = core;
    cfg.max_conns = max_conns;
    cfg.max_downstream_subs = 4;
    cfg.max_open_subgroups = 4;
    cfg.max_upstream_subs = 4;
    cfg.max_publishes = 4;
    cfg.max_announces = 4;
    long before = a->live;
    moqr_bind_t *b = NULL;
    if (moqr_bind_create(&cfg, &b) != MOQR_OK) {
        return -1;
    }
    long created = a->live - before;
    moqr_bind_destroy(b);
    return a->live == before ? created : -1;   /* -1: leaked */
}

static int
test_bind_capacity_slopes(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    /* One shared core for the create() runs (bind does not own it). */
    moqr_core_relay_cfg_t ccfg;
    moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), &a.vt);
    ccfg.log_budget.max_groups = 4;
    ccfg.log_budget.max_bytes = 1 << 20;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&ccfg, &core) == MOQR_OK);

    moqr_core_limits_t lim;
    MOQ_TEST_CHECK(moqr_core_limits_resolve(&ccfg, &lim) == MOQR_OK);
    uint64_t per_conn = sizeof(b_conn_t) + 4 * sizeof(b_dsub_t) +
                        4 * sizeof(b_sg_t) + 4 * sizeof(b_usub_t) +
                        4 * sizeof(b_pub_t) + 4 * sizeof(b_ann_t) +
                        lim.max_fetches * sizeof(b_fetch_t);
    uint64_t scratch = bind_pump_scratch_bytes_test();

    uint64_t d1 = describe_bind(&a, core, 1);
    uint64_t d63 = describe_bind(&a, core, 63);
    uint64_t d64 = describe_bind(&a, core, 64);
    uint64_t d65 = describe_bind(&a, core, 65);
    uint64_t d128 = describe_bind(&a, core, 128);
    uint64_t d129 = describe_bind(&a, core, 129);
    MOQ_TEST_CHECK(d1 != UINT64_MAX && d129 != UINT64_MAX);
    MOQ_TEST_CHECK_EQ_U64(d63 - d1, 62 * per_conn);       /* 1 word pair  */
    MOQ_TEST_CHECK_EQ_U64(d64 - d63, per_conn);           /* 1 word pair  */
    MOQ_TEST_CHECK_EQ_U64(d65 - d64, per_conn + 16);      /* 2 word pairs */
    MOQ_TEST_CHECK_EQ_U64(d128 - d65, 63 * per_conn);     /* 2 word pairs */
    MOQ_TEST_CHECK_EQ_U64(d129 - d128, per_conn + 16);    /* 3 word pairs */
    MOQ_TEST_CHECK(scratch > 2048u);   /* the embedded stack RED is real */

    /* Create-time slope mirrors the descriptor exactly (all bind tables are
     * eager), and every size destroys leak-free. */
    long c1 = create_bind_live(&a, core, 1);
    long c63 = create_bind_live(&a, core, 63);
    long c64 = create_bind_live(&a, core, 64);
    long c65 = create_bind_live(&a, core, 65);
    MOQ_TEST_CHECK(c1 > 0 && c63 > 0 && c64 > 0 && c65 > 0);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)c1, d1);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(c64 - c63), per_conn);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)(c65 - c64), per_conn + 16);

    moqr_core_destroy(core);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("bind_capacity_slopes");
    return failures;
}

static int
test_bind_pump_scratch_owned(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);

    moqr_core_relay_cfg_t ccfg;
    moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), &a.vt);
    ccfg.log_budget.max_groups = 4;
    ccfg.log_budget.max_bytes = 1 << 20;
    moqr_core_t *core = NULL;
    MOQ_TEST_CHECK(moqr_core_create(&ccfg, &core) == MOQR_OK);

    moqr_bind_cfg_t cfg;
    moqr_bind_cfg_init_sized(&cfg, sizeof(cfg), &a.vt);
    cfg.core = core;
    cfg.max_conns = 1;
    cfg.max_downstream_subs = 1;
    cfg.max_open_subgroups = 1;
    cfg.max_upstream_subs = 1;
    cfg.max_publishes = 1;
    cfg.max_announces = 1;

    moqr_bind_t *b = NULL;
    MOQ_TEST_CHECK(moqr_bind_create(&cfg, &b) == MOQR_OK);
    MOQ_TEST_CHECK(b != NULL);
    MOQ_TEST_CHECK(b->pump_events != NULL);
    MOQ_TEST_CHECK(b->pump_revoked != NULL);
    MOQ_TEST_CHECK(b->pump_intents != NULL);
    MOQ_TEST_CHECK(b->pump_events != (moq_event_t *)(void *)b->pump_revoked);
    MOQ_TEST_CHECK(b->pump_intents !=
                   (moqr_intent_t *)(void *)b->pump_events);
    MOQ_TEST_CHECK(b->pump_intents !=
                   (moqr_intent_t *)(void *)b->pump_revoked);

    long attempts_before = a.attempts;
    long live_before = a.live;
    MOQ_TEST_CHECK(moqr_bind_pump(b, 1000) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_INT((int)(a.attempts - attempts_before), 0);
    MOQ_TEST_CHECK_EQ_INT((int)(a.live - live_before), 0);

    b->in_pump = true;
    MOQ_TEST_CHECK(moqr_bind_pump(b, 2000) == MOQR_ERR_INVAL);
    b->in_pump = false;

    moqr_bind_destroy(b);
    moqr_core_destroy(core);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);

    for (long fail = 7; fail <= 9; fail++) {
        ca_t fa;
        ca_init(&fa);
        moqr_core_t *fc = NULL;
        moqr_core_relay_cfg_t fccfg;
        moqr_core_relay_cfg_init_sized(&fccfg, sizeof(fccfg), &fa.vt);
        fccfg.log_budget.max_groups = 4;
        fccfg.log_budget.max_bytes = 1 << 20;
        MOQ_TEST_CHECK(moqr_core_create(&fccfg, &fc) == MOQR_OK);
        moqr_bind_cfg_t fcfg;
        moqr_bind_cfg_init_sized(&fcfg, sizeof(fcfg), &fa.vt);
        fcfg.core = fc;
        fcfg.max_conns = 1;
        fcfg.max_downstream_subs = 1;
        fcfg.max_open_subgroups = 1;
        fcfg.max_upstream_subs = 1;
        fcfg.max_publishes = 1;
        fcfg.max_announces = 1;
        fa.fail_at = fa.attempts + fail;
        moqr_bind_t *fb = NULL;
        MOQ_TEST_CHECK(moqr_bind_create(&fcfg, &fb) == MOQR_ERR_NOMEM);
        MOQ_TEST_CHECK(fb == NULL);
        moqr_core_destroy(fc);
        MOQ_TEST_CHECK_EQ_INT((int)fa.live, 0);
    }

    MOQ_TEST_PASS("bind_pump_scratch_owned");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_bind_word_count();
    failures += test_bind_capacity_slopes();
    failures += test_bind_pump_scratch_owned();
    if (failures != 0) {
        fprintf(stderr, "%d failure(s)\n", failures);
        return 1;
    }
    printf("ALL PASS\n");
    return 0;
}
