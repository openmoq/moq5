/*
 * The multi-shard runtime skeleton: K {core, bind, trace} shards, the six-phase
 * deterministic stepper, rendezvous placement, and the K == 1 pass-through.
 * Pure bind+core — no SimPair, no transport; shards are driven with core control
 * ops + moqr_shards_step, exactly as the eventual runtime drives them.
 */

#include <moq/relay/moqr_shards.h>
#if defined(MOQR_RELAY_INSPECT) || defined(MOQR_BIND_TESTING)
#include <moq/relay/moqr_shards_test.h>
#endif

#include <moq/rcbuf.h>

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../tests/unit/test_support.h"

/* Counting allocator (same shape as the other relay tests), with an
 * injection hook: fail_at > 0 fails the Nth allocation from now (counting
 * down); fail_all fails every allocation. */
typedef struct ca {
    moq_alloc_t vt;
    long allocs, frees, live;
    long peak;   /* continuously tracked high-water of live */
    long fail_at;
    bool fail_all;
} ca_t;
/* An upstream terminal arrives in some connection's draft; these rigs speak
 * draft-16 unless a case says otherwise. */
static moqr_pd_desc_t
pd_wire(uint64_t code)
{
    moqr_pd_desc_t d;

    if (moqr_pd_desc_wire(MOQ_VERSION_DRAFT_16, code, &d) != MOQR_OK) {
        return moqr_pd_desc_none();
    }
    return d;
}

static moqr_reset_desc_t
rd_wire(uint64_t code)
{
    moqr_reset_desc_t d;

    if (moqr_reset_desc_wire(MOQ_VERSION_DRAFT_16, code, &d) != MOQR_OK) {
        return moqr_reset_desc_none();
    }
    return d;
}

static void *ca_a(size_t n, void *c)
{
    ca_t *a = c;
    if (a->fail_all) {
        return NULL;
    }
    if (a->fail_at > 0 && --a->fail_at == 0) {
        return NULL;
    }
    void *p = malloc(n);
    if (p) {
        a->allocs++;
        a->live += (long)n;
        if (a->live > a->peak) {
            a->peak = a->live;
        }
    }
    return p;
}
/* Size-guarded variant for impossible-configuration tests: refuses any
 * single allocation over 1 GiB so a missing config validation surfaces as a
 * clean NOMEM instead of an attempted terabyte reservation. */
static void *ca_a_guard(size_t n, void *c)
{
    if (n > ((size_t)1u << 30)) {
        return NULL;
    }
    return ca_a(n, c);
}
static void *ca_r(void *p, size_t o, size_t n, void *c)
{
    ca_t *a = c;
    void *q = realloc(p, n);
    if (q) {
        a->live += (long)n - (long)o;
        if (a->live > a->peak) {
            a->peak = a->live;
        }
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

/* Build a shards config with K shards and a small per-shard log budget. */
static void
shards_cfg(moqr_shards_cfg_t *cfg, ca_t *a, uint16_t k, uint64_t permute_seed)
{
    moqr_shards_cfg_init_sized(cfg, sizeof(*cfg), &a->vt);
    cfg->shards = k;
    cfg->permute_seed = permute_seed;
    cfg->core_cfg.log_budget.max_groups = 4;
    cfg->core_cfg.log_budget.max_bytes = 1u << 20;
    cfg->core_cfg.linger_us = 1000;
}

/* K == 1 is a pure pass-through: one core/bind/trace, stepping is one bind pump,
 * and everything balances on destroy. */
static int
test_shards_lifecycle_k1(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 1, 0);
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_count(s), 1);
    MOQ_TEST_CHECK(moqr_shards_core(s, 0) != NULL);
    MOQ_TEST_CHECK(moqr_shards_bind(s, 0) != NULL);
    MOQ_TEST_CHECK(moqr_shards_trace(s, 0) != NULL);
    MOQ_TEST_CHECK(moqr_shards_core(s, 1) == NULL);   /* out of range */
    for (int r = 0; r < 4; r++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    }
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_lifecycle_k1");
    return failures;
}

/* K == 3: three independent cores with distinct shard tags — a handle minted by
 * one shard's core is structurally refused by another. */
static int
test_shards_k3_isolation(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 3, 0);
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_count(s), 3);
    MOQ_TEST_CHECK(moqr_shards_core(s, 2) != NULL);
    MOQ_TEST_CHECK(moqr_shards_core(s, 3) == NULL);

    moqr_core_t *c0 = moqr_shards_core(s, 0);
    moqr_core_t *c1 = moqr_shards_core(s, 1);
    /* Open a binding at the SAME slot+generation on both cores (cookie 1 → slot 0,
     * gen 1 on each), so ONLY the per-core shard tag distinguishes the handles —
     * the gen check can't mask a missing tag guard. */
    moqr_binding_t b0, b1;
    MOQ_TEST_CHECK(moqr_core_binding_open(c0, 1, &b0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_binding_open(c1, 1, &b1) == MOQR_OK);
    moq_bytes_t nsp[1] = { { (const uint8_t *)"ns", 2 } };
    moqr_ns_t ns = { nsp, 1 };
    /* own core accepts the handle; the sibling shard's core refuses it (tag). */
    MOQ_TEST_CHECK(moqr_core_announce(c0, b0, ns) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_announce(c1, b0, ns) == MOQR_ERR_STALE_HANDLE);
    MOQ_TEST_CHECK(moqr_core_announce(c0, b1, ns) == MOQR_ERR_STALE_HANDLE);

    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_k3_isolation");
    return failures;
}

/* Drive a fixed per-shard local activity, step, and fold each shard's trace hash
 * / count / route_epoch into the caller's vectors. */
static void
shards_capture(uint64_t permute_seed, uint64_t *hash, uint64_t *count,
               uint64_t *repoch, int *pf)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 3, permute_seed);
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    /* Shard i announces i+1 distinct namespaces, so per-shard route_epoch/count
     * differ (proving the rings/cores are independent) and are reproducible. */
    for (uint16_t i = 0; i < 3; i++) {
        moqr_core_t *c = moqr_shards_core(s, i);
        moqr_binding_t b;
        MOQ_TEST_CHECK(moqr_core_binding_open(c, 1, &b) == MOQR_OK);
        for (uint16_t j = 0; j <= i; j++) {
            char part[16];
            snprintf(part, sizeof(part), "s%u_%u", (unsigned)i, (unsigned)j);
            moq_bytes_t nsp[1] = { { (const uint8_t *)part,
                                    (uint32_t)strlen(part) } };
            moqr_ns_t ns = { nsp, 1 };
            MOQ_TEST_CHECK(moqr_core_announce(c, b, ns) == MOQR_OK);
        }
    }
    for (int r = 0; r < 12; r++) {   /* run to a converged fixed point */
        MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    }
    for (uint16_t i = 0; i < 3; i++) {
        hash[i] = moqr_trace_hash(moqr_shards_trace(s, i));
        count[i] = moqr_trace_count(moqr_shards_trace(s, i));
        moqr_core_stats_t st;
        moqr_core_get_stats(moqr_shards_core(s, i), &st);
        repoch[i] = st.route_epoch;
    }
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    *pf += failures;
}

/* The runtime is deterministic under the cross-shard control plane: the same
 * seed reproduces every shard's trace vector run-twice, AND a seeded shard-
 * visitation permutation produces the identical per-shard vector — the round
 * barrier makes a round's result invariant under intra-round shard order. */
static int
test_shards_determinism(void)
{
    int failures = 0;
    uint64_t h1[3], c1[3], e1[3], h2[3], c2[3], e2[3], hp[3], cp[3], ep[3];
    shards_capture(0, h1, c1, e1, &failures);          /* ascending order */
    shards_capture(0, h2, c2, e2, &failures);          /* run-twice */
    shards_capture(0xA5A5A5A5u, hp, cp, ep, &failures); /* permuted order */
    for (int i = 0; i < 3; i++) {
        MOQ_TEST_CHECK_EQ_U64(h1[i], h2[i]);   /* run-twice */
        MOQ_TEST_CHECK_EQ_U64(c1[i], c2[i]);
        MOQ_TEST_CHECK_EQ_U64(e1[i], e2[i]);
        MOQ_TEST_CHECK_EQ_U64(h1[i], hp[i]);   /* permutation invariance */
        MOQ_TEST_CHECK_EQ_U64(c1[i], cp[i]);
        MOQ_TEST_CHECK_EQ_U64(e1[i], ep[i]);
        MOQ_TEST_CHECK(c1[i] > 0);             /* every shard has its own ring */
    }
    /* Convergence: with 3 publishers (one per shard, distinct namespaces) every
     * shard resolves all three — its own announces plus mirrors of the others. */
    MOQ_TEST_CHECK_EQ_U64(e1[0], e1[1]);
    MOQ_TEST_CHECK_EQ_U64(e1[1], e1[2]);
    MOQ_TEST_PASS("shards_determinism");
    return failures;
}

/* Rendezvous placement is a pure function of the key + shard set: the same key
 * always returns the same owner, every shard is reachable, and no key lands out
 * of range. */
static int
test_shards_placement(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 4, 0);
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    int seen[4] = { 0, 0, 0, 0 };
    for (uint64_t n = 0; n < 256; n++) {
        moqr_place_key_t key;
        memset(&key, 0, sizeof(key));
        key.hash = n * 0x9E3779B97F4A7C15ull + 0x1234567u;   /* spread of keys */
        moqr_owner_t o1 = moqr_shards_place(s, &key);
        moqr_owner_t o2 = moqr_shards_place(s, &key);
        MOQ_TEST_CHECK_EQ_U64(o1.shard, o2.shard);   /* pure */
        MOQ_TEST_CHECK_EQ_U64(o1.node, 0);
        MOQ_TEST_CHECK(o1.shard < 4);
        seen[o1.shard]++;
    }
    for (int i = 0; i < 4; i++) {
        MOQ_TEST_CHECK(seen[i] > 0);   /* every shard owns some key */
    }
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_placement");
    return failures;
}

/* -- control-plane test helpers -------------------------------------------- */

static moqr_shards_t *
mk_shards(ca_t *a, uint16_t k, uint32_t mbox, uint64_t seed)
{
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, a, k, seed);
    if (mbox != 0) {
        cfg.mailbox_entries = mbox;
    }
    moqr_shards_t *s = NULL;
    (void)moqr_shards_create(&cfg, &s);
    return s;
}

static void
step_n(moqr_shards_t *s, int n)
{
    for (int i = 0; i < n; i++) {
        (void)moqr_shards_step(s, 1000);
    }
}

static moqr_binding_t
pub_open(moqr_shards_t *s, uint16_t i)
{
    moqr_binding_t b;
    (void)moqr_core_binding_open(moqr_shards_core(s, i), 1, &b);
    return b;
}

/* A publisher binding at an explicit cookie (distinct publisher identities
 * keep force-withdraw cancels from deduping onto one pending entry). */
static moqr_binding_t
pub_open_at(moqr_shards_t *s, uint16_t i, uint64_t cookie)
{
    moqr_binding_t b;
    (void)moqr_core_binding_open(moqr_shards_core(s, i), cookie, &b);
    return b;
}

static moqr_result_t
ann(moqr_shards_t *s, uint16_t i, moqr_binding_t b, const char *p)
{
    moq_bytes_t nsp[1] = { { (const uint8_t *)p, (uint32_t)strlen(p) } };
    moqr_ns_t ns = { nsp, 1 };
    return moqr_core_announce(moqr_shards_core(s, i), b, ns);
}

static moqr_result_t
unann(moqr_shards_t *s, uint16_t i, moqr_binding_t b, const char *p)
{
    moq_bytes_t nsp[1] = { { (const uint8_t *)p, (uint32_t)strlen(p) } };
    moqr_ns_t ns = { nsp, 1 };
    return moqr_core_unannounce(moqr_shards_core(s, i), b, ns);
}

static void
jinfo(moqr_shards_t *s, uint16_t i, const char *p, moqr_shards_jinfo_t *out)
{
    moq_bytes_t nsp[1] = { { (const uint8_t *)p, (uint32_t)strlen(p) } };
    moqr_shards_debug_journal(s, i, nsp, 1, out);
}

static uint64_t
repoch(moqr_shards_t *s, uint16_t i)
{
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, i), &st);
    return st.route_epoch;
}

/* Find a 1-part namespace "<pfx><n>" whose HRW winner over `cand` is `want`. */
static void
find_ns(char *buf, size_t bufn, const char *pfx, uint64_t cand, int32_t want)
{
    for (int n = 0; n < 100000; n++) {
        snprintf(buf, bufn, "%s%d", pfx, n);
        moq_bytes_t nsp[1] = { { (const uint8_t *)buf, (uint32_t)strlen(buf) } };
        if (moqr_shards_debug_hrw_winner(cand, nsp, 1) == want) {
            return;
        }
    }
    buf[0] = '\0';
}

#define BIT(i) (1ull << (i))

/* Pending publisher-cancels on shard i's core (peek is non-draining). */
static size_t
peek_cancels(moqr_shards_t *s, uint16_t i, moqr_revoked_grant_t *out,
             size_t max)
{
    return moqr_core_peek_revoked_grants(moqr_shards_core(s, i), out, max);
}

/* Canonical-order comparator for 1-part namespace names: the journal's canon
 * encoding is [count][len][bytes], so for equal part-counts a SHORTER name
 * sorts first, then byte order. Reconcile walks in this order — tests that
 * depend on which namespace a round visits first sort their hunted names. */
static int
ns1_canon_cmp(const void *va, const void *vb)
{
    const char *a = *(const char *const *)va;
    const char *b = *(const char *const *)vb;
    size_t la = strlen(a), lb = strlen(b);
    if (la != lb) {
        return la < lb ? -1 : 1;
    }
    return memcmp(a, b, la);
}

/* Open a downstream-subscriber binding (real cookie, distinct from publishers). */
static moqr_binding_t
sub_open(moqr_shards_t *s, uint16_t i, uint64_t cookie)
{
    moqr_binding_t b;
    (void)moqr_core_binding_open(moqr_shards_core(s, i), cookie, &b);
    return b;
}

/* Subscribe on shard i's core for a 1-part namespace. */
static moqr_result_t
do_subscribe(moqr_shards_t *s, uint16_t i, moqr_binding_t b, const char *p,
             uint64_t cookie, moqr_sub_t *out)
{
    moq_bytes_t nsp[1] = { { (const uint8_t *)p, (uint32_t)strlen(p) } };
    moqr_subscribe_req_t rq;
    moqr_subscribe_req_init(&rq);
    rq.ns = (moqr_ns_t){ nsp, 1 };
    rq.name = (moq_bytes_t){ (const uint8_t *)"v", 1 };
    rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
    rq.cookie = cookie;
    return moqr_core_subscribe(moqr_shards_core(s, i), b, &rq, out);
}

static uint64_t
rdr(moqr_shards_t *s, uint16_t i)
{
    return moqr_shards_debug_remote_demand_refused(s, i);
}

/* The per-shard step seam: a hand-rolled ascending pass over
 * debug_step_shard + one debug_round_advance must be indistinguishable from
 * moqr_shards_step — identical per-shard trace vectors, route epochs, and
 * journal state — and the outbound push mask must report exactly the
 * destinations whose mailboxes accepted state, exactly when they did. */
static int
test_shards_step_seam(void)
{
    int failures = 0;

    /* Equivalence: two identical runtimes, one stepped by the runner, one by
     * the composed seam, fold identical determinism vectors. */
    for (int manual = 0; manual < 2; manual++) {
        ca_t a;
        ca_init(&a);
        moqr_shards_t *s = mk_shards(&a, 3, 0, 0);
        MOQ_TEST_CHECK(s != NULL);
        for (uint16_t i = 0; i < 3; i++) {
            char part[8];
            snprintf(part, sizeof(part), "sm%u", (unsigned)i);
            moqr_binding_t b = pub_open(s, i);
            MOQ_TEST_CHECK(ann(s, i, b, part) == MOQR_OK);
        }
        for (int r = 0; r < 8; r++) {
            if (manual) {
                for (uint16_t i = 0; i < 3; i++) {
                    (void)moqr_shards_debug_step_shard(s, i, 1000, NULL);
                }
                moqr_shards_debug_round_advance(s);
            } else {
                MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
            }
        }
        static uint64_t hash[2][3], count[2][3], epoch[2][3];
        for (uint16_t i = 0; i < 3; i++) {
            hash[manual][i] = moqr_trace_hash(moqr_shards_trace(s, i));
            count[manual][i] = moqr_trace_count(moqr_shards_trace(s, i));
            epoch[manual][i] = repoch(s, i);
        }
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
        if (manual) {
            for (uint16_t i = 0; i < 3; i++) {
                MOQ_TEST_CHECK_EQ_U64(hash[1][i], hash[0][i]);
                MOQ_TEST_CHECK_EQ_U64(count[1][i], count[0][i]);
                MOQ_TEST_CHECK_EQ_U64(epoch[1][i], epoch[0][i]);
            }
        }
    }

    /* The wake mask: bit d exactly when destination d's mailbox accepted an
     * outbound push in THIS shard's outbound phase — on the initial export, on
     * a withdrawal export, and never in a quiet round — plus the stepped
     * shard's OWN bit on the round whose reconcile mutated its core, since
     * that fan-out lands after the pass that would have drained it. One such
     * continuation, then quiet. */
    {
        ca_t a;
        ca_init(&a);
        moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
        MOQ_TEST_CHECK(s != NULL);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "mk") == MOQR_OK);
        uint64_t mask = ~0ull;
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(mask, BIT(1));   /* the announce exported */
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(mask, 0);        /* barrier: not yet visible */
        moqr_shards_debug_round_advance(s);
        for (int r = 0; r < 3; r++) {   /* converge: one continuation, then
                                         * quiet */
            MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                           MOQR_OK);
            MOQ_TEST_CHECK_EQ_U64(mask, 0);
            MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                           MOQR_OK);
            /* Round 0 installs the mirror on shard 1; its namespace fan-out
             * is owed a drain, so exactly that shard's own bit is set, and
             * only once. */
            MOQ_TEST_CHECK_EQ_U64(mask, r == 0 ? BIT(1) : 0);
            moqr_shards_debug_round_advance(s);
        }
        moqr_shards_jinfo_t j1;
        jinfo(s, 1, "mk", &j1);
        MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);   /* the seam really converged */
        MOQ_TEST_CHECK(unann(s, 0, p0, "mk") == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(mask, BIT(1));   /* the withdrawal exported */
        /* The mirror teardown on shard 1 is owed the same continuation. */
        moqr_shards_debug_round_advance(s);
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(mask, BIT(1));
        moqr_shards_debug_round_advance(s);
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                       MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(mask, 0);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("shards_step_seam");
    return failures;
}

/* One publisher on shard 0; a remote shard resolves the winner mirror. */
static int
test_shards_broadcast(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "cam") == MOQR_OK);
    step_n(s, 8);
    moqr_shards_jinfo_t j0, j1;
    jinfo(s, 0, "cam", &j0);
    jinfo(s, 1, "cam", &j1);
    /* owner shard: local publisher, winner 0, no mirror. */
    MOQ_TEST_CHECK(j0.present && j0.candidates == BIT(0));
    MOQ_TEST_CHECK_EQ_INT(j0.winner, 0);
    MOQ_TEST_CHECK_EQ_INT(j0.mirror, -1);
    /* remote shard: candidate 0 broadcast in, winner 0, mirror PB(1,0). */
    MOQ_TEST_CHECK(j1.present && j1.candidates == BIT(0));
    MOQ_TEST_CHECK_EQ_INT(j1.winner, 0);
    MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_broadcast");
    return failures;
}

/* Mirror replacement + self-echo classification, in the enforcement-lag
 * window — the one place a REPLACE (unannounce old PB + announce new PB in a
 * single reconcile visit) still occurs now that split-brain losers self-
 * withdraw. Three namespaces, all winner-1 over {0,1}, announced on both
 * shards from DISTINCT shard-0 bindings (distinct publisher identities keep
 * their cancels from deduping), with max_cancels = 1 so enforcement lands one
 * loser per round in canonical order:
 *   round 2: s1 withdrawn (slot); s2 + main defer WOULD_BLOCK, zero mutation.
 *   round 3: the pump acks s1's cancel (its publisher has no live conn), s2
 *            takes the slot; main defers again — its candidate is still live
 *            everywhere. The winner (shard 1) withdrew main between rounds 2
 *            and 3, exporting its absence in round 3.
 *   round 4: shard 2 applies absent(1) while main's candidate 0 is STILL live
 *            (shard 0 never exported an absence — the deferrals never
 *            mutated): winner flips 1 -> 0 with mirror 1 installed → REPLACE
 *            1 -> 0 in one visit. Shard 0 itself now recomputes winner = self,
 *            stops being a holdout, and keeps its never-touched announce: the
 *            blocked loser became the winner precisely because WOULD_BLOCK
 *            was zero-mutation.
 * Neither the old mirror's NS_GONE nor the new mirror's NS_FOUND is mistaken
 * for a local publisher — shard 2's own bit is never set, and no pending
 * self-echo is left dangling. */
static int
test_shards_remote_vs_remote(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 3, 0);
    cfg.core_cfg.max_cancels = 1;   /* one enforcement lands per round */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    char h0[24], h1[24], h2[24];
    find_ns(h0, sizeof(h0), "rvA", BIT(0) | BIT(1), 1);
    find_ns(h1, sizeof(h1), "rvB", BIT(0) | BIT(1), 1);
    find_ns(h2, sizeof(h2), "rvC", BIT(0) | BIT(1), 1);
    MOQ_TEST_CHECK(h0[0] != '\0' && h1[0] != '\0' && h2[0] != '\0');
    const char *sorted[3] = { h0, h1, h2 };
    qsort(sorted, 3, sizeof(sorted[0]), ns1_canon_cmp);
    const char *s1 = sorted[0], *s2 = sorted[1], *main_ns = sorted[2];

    moqr_binding_t pa = pub_open_at(s, 0, 1);        /* s1   */
    moqr_binding_t pb = pub_open_at(s, 0, 2);        /* s2   */
    moqr_binding_t pc = pub_open_at(s, 0, 3);        /* main */
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 0, pa, s1) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, pb, s2) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, pc, main_ns) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 1, p1, s1) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 1, p1, s2) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 1, p1, main_ns) == MOQR_OK);
    step_n(s, 2);
    moqr_shards_jinfo_t j0, jc;
    jinfo(s, 0, main_ns, &j0);
    jinfo(s, 2, main_ns, &jc);
    MOQ_TEST_CHECK(j0.holdout);                        /* deferred loser */
    MOQ_TEST_CHECK(j0.candidates == (BIT(0) | BIT(1)));  /* zero mutation */
    MOQ_TEST_CHECK_EQ_INT(j0.mirror, -1);
    MOQ_TEST_CHECK(jc.candidates == (BIT(0) | BIT(1)));
    MOQ_TEST_CHECK_EQ_INT(jc.mirror, 1);               /* winner mirrored */
    MOQ_TEST_CHECK((jc.candidates & BIT(2)) == 0);     /* no phantom local */

    /* The winner leaves main while shard 0's withdrawal is still deferred. */
    MOQ_TEST_CHECK(unann(s, 1, p1, main_ns) == MOQR_OK);
    step_n(s, 1);                                      /* round 3 */
    step_n(s, 1);                                      /* round 4: REPLACE */
    jinfo(s, 2, main_ns, &jc);
    MOQ_TEST_CHECK(jc.candidates == BIT(0));           /* winner-1 bit removed */
    MOQ_TEST_CHECK((jc.candidates & BIT(2)) == 0);     /* self-echo suppressed */
    MOQ_TEST_CHECK_EQ_INT(jc.winner, 0);
    MOQ_TEST_CHECK_EQ_INT(jc.mirror, 0);               /* replaced 1 -> 0 */

    /* Settle: the loser-became-winner keeps its live announce; the other two
     * namespaces converged to the enforced steady state; every self-echo was
     * consumed and only quiesced state remains. */
    step_n(s, 6);
    jinfo(s, 0, main_ns, &j0);
    MOQ_TEST_CHECK(!j0.holdout);
    MOQ_TEST_CHECK(j0.candidates == BIT(0));
    MOQ_TEST_CHECK_EQ_INT(j0.winner, 0);
    jinfo(s, 2, main_ns, &jc);
    MOQ_TEST_CHECK(jc.candidates == BIT(0) && jc.mirror == 0);   /* stable */
    jinfo(s, 0, s1, &j0);
    MOQ_TEST_CHECK(j0.candidates == BIT(1) && j0.mirror == 1);   /* enforced */
    jinfo(s, 0, s2, &j0);
    MOQ_TEST_CHECK(j0.candidates == BIT(1) && j0.mirror == 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_remote_vs_remote");
    return failures;
}

/* Late local announce behind a remote mirror is rejected (documented K>1
 * divergence); the mirror and candidate set are unchanged. */
static int
test_shards_late_local(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ll") == MOQR_OK);
    step_n(s, 8);
    moqr_shards_jinfo_t j1;
    jinfo(s, 1, "ll", &j1);
    MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);   /* shard 1 holds the mirror */
    /* A real local publisher on shard 1 now announces the mirrored namespace:
     * the core refuses it (the mirror occupies the trie node). */
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 1, p1, "ll") == MOQR_ERR_WRONG_STATE);
    step_n(s, 4);
    jinfo(s, 1, "ll", &j1);
    MOQ_TEST_CHECK(j1.candidates == BIT(0));   /* no local bit — not corrupted */
    MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);       /* mirror unchanged */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_late_local");
    return failures;
}

/* Mailbox-full durability: with a 1-entry mailbox, two announces from shard 0
 * cannot both be in flight at once, but the journal holds them and re-exports as
 * space frees — shard 1 eventually mirrors both, exactly once each. */
static int
test_shards_mailbox_full(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 1, 0);   /* mailbox_entries = 1 */
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "aa") == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "bb") == MOQR_OK);
    step_n(s, 12);
    moqr_shards_jinfo_t ja, jb;
    jinfo(s, 1, "aa", &ja);
    jinfo(s, 1, "bb", &jb);
    MOQ_TEST_CHECK(ja.present && ja.candidates == BIT(0) && ja.mirror == 0);
    MOQ_TEST_CHECK(jb.present && jb.candidates == BIT(0) && jb.mirror == 0);
    /* exactly one mirror install each: shard 1 = wildcard sub + 2 mirrors. */
    MOQ_TEST_CHECK_EQ_U64(repoch(s, 1), 3);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_mailbox_full");
    return failures;
}

/* State-convergence collapse: shard 0 announces then unannounces the same
 * namespace before it is exported; the intermediate announce collapses, so shard
 * 1 never installs a mirror (no transient install/withdraw). */
static int
test_shards_collapse(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    uint64_t base = repoch(s, 1);   /* shard 1's wildcard-sub baseline */
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "col") == MOQR_OK);
    MOQ_TEST_CHECK(unann(s, 0, p0, "col") == MOQR_OK);
    step_n(s, 8);
    moqr_shards_jinfo_t j1;
    jinfo(s, 1, "col", &j1);
    MOQ_TEST_CHECK(!j1.present);            /* shard 1 never saw it */
    MOQ_TEST_CHECK_EQ_U64(repoch(s, 1), base);   /* no transient mirror churn */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_collapse");
    return failures;
}

/* Same-round batch invariance: shard 2 receives A's and B's announces for one
 * namespace in the same round (B wins HRW); it reconciles once and installs only
 * B's mirror — one mirror op, not install-A-then-replace. */
static int
test_shards_same_round_batch(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 3, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    char ns[24];
    find_ns(ns, sizeof(ns), "srb", BIT(0) | BIT(1), 1);
    MOQ_TEST_CHECK(ns[0] != '\0');
    moqr_binding_t p0 = pub_open(s, 0);
    moqr_binding_t p1 = pub_open(s, 1);
    /* Both announce before any step, so both broadcasts reach shard 2 in the
     * same round and batch before its single reconcile. */
    MOQ_TEST_CHECK(ann(s, 0, p0, ns) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 1, p1, ns) == MOQR_OK);
    step_n(s, 8);
    moqr_shards_jinfo_t jc;
    jinfo(s, 2, ns, &jc);
    /* By now shard 0's losing publisher has been force-withdrawn, so the steady
     * state is the winner alone; the batching pin is repoch: exactly ONE mirror
     * op ever ran on shard 2 (wildcard sub + 1 install) — the batched candidates
     * picked the winner directly, never install-loser-then-replace. */
    MOQ_TEST_CHECK(jc.candidates == BIT(1));
    MOQ_TEST_CHECK_EQ_INT(jc.mirror, 1);
    MOQ_TEST_CHECK_EQ_U64(repoch(s, 2), 2);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_same_round_batch");
    return failures;
}

/* Split-brain loser ENFORCEMENT: two publishers announce the same namespace on
 * two shards; all shards agree on the HRW winner, and the losing shard force-
 * withdraws its own publisher — announce purged, publisher cancel queued with
 * GOING_AWAY — then installs the winner's mirror in the SAME reconcile pass.
 * The forced NS_GONE echoes back a round later as a real (non-mirror) event and
 * must neither leak a pending self-echo nor resurrect the local candidate. A
 * post-enforcement subscribe on the loser shard routes to the mirror and is
 * refused as remote-owner demand — the local source is truly gone at the core,
 * not just in the journal. And when the WINNER later leaves, the withdrawn
 * loser must not reappear as a candidate anywhere. */
static int
test_shards_split_brain(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 3, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    /* Pick a namespace shard 0 wins over {0,1}, so shard 1 is the local loser. */
    char ns[24];
    find_ns(ns, sizeof(ns), "sb", BIT(0) | BIT(1), 0);
    MOQ_TEST_CHECK(ns[0] != '\0');
    moqr_binding_t p0 = pub_open(s, 0);
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 0, p0, ns) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 1, p1, ns) == MOQR_OK);

    /* Step until the loser's enforcement lands (its publisher cancel becomes
     * peekable), then assert the SAME post-step state: the withdrawal and the
     * winner-mirror install happen in ONE reconcile pass, not a round apart. */
    moqr_revoked_grant_t rg[4];
    int enforced_round = -1;
    for (int r = 0; r < 12 && enforced_round < 0; r++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
        if (peek_cancels(s, 1, rg, 4) == 1) {
            enforced_round = r;
        }
    }
    MOQ_TEST_CHECK(enforced_round >= 0);
    moqr_shards_jinfo_t j0, j1, j2;
    jinfo(s, 1, ns, &j1);
    MOQ_TEST_CHECK(j1.candidates == BIT(0));   /* own losing bit cleared */
    MOQ_TEST_CHECK_EQ_INT(j1.winner, 0);
    MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);       /* installed in the same pass */
    MOQ_TEST_CHECK(!j1.holdout);
    /* The queued cancel targets the loser's real publisher with the
     * REQUEST_ERROR UNINTERESTED code (the relay no longer wants this
     * publisher's namespace — the remote winner owns it). */
    MOQ_TEST_CHECK_EQ_U64(rg[0].binding_cookie, 1);
    MOQ_TEST_CHECK_EQ_U64(rg[0].session_cookie, 0);
    MOQ_TEST_CHECK_EQ_U64(rg[0].error_code, 0x20u);

    /* Settle: the forced NS_GONE echo re-clears an already-clear bit (idempotent)
     * and leaves no dangling self-echo; the winner and third shard converge on
     * the winner alone. */
    step_n(s, 6);
    jinfo(s, 0, ns, &j0);
    jinfo(s, 1, ns, &j1);
    jinfo(s, 2, ns, &j2);
    MOQ_TEST_CHECK(!j0.holdout && j0.candidates == BIT(0) && j0.mirror == -1);
    MOQ_TEST_CHECK(j1.candidates == BIT(0) && j1.mirror == 0);   /* stable */
    MOQ_TEST_CHECK(j2.candidates == BIT(0) && j2.mirror == 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, 1), 0);
    /* The queued cancel was DRAINED by a later pump: the loser publisher has
     * no live session in this pure-core rig, so the binding acks it as moot
     * ("connection gone") rather than leaving it pending forever. */
    MOQ_TEST_CHECK_EQ_SIZE(peek_cancels(s, 1, rg, 4), (size_t)0);

    /* The purge reaches the CORE, not just the journal: a fresh subscribe on the
     * loser shard now routes to the winner's mirror and is refused as remote-
     * owner demand (were the loser's announce or a stale sourced track alive, it
     * would have been served locally and never metered). */
    moqr_binding_t d1 = sub_open(s, 1, 5);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, ns, 7, &sub) == MOQR_OK);
    step_n(s, 3);   /* round-trip: send, owner answers, requester resolves */
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 1);

    /* The WINNER leaves: every journal drains to no candidates and the mirrors
     * withdraw. The force-withdrawn loser must NOT resurrect (its bit-clear was
     * durable; only a real re-announce could bring it back). */
    MOQ_TEST_CHECK(unann(s, 0, p0, ns) == MOQR_OK);
    step_n(s, 10);
    jinfo(s, 0, ns, &j0);
    jinfo(s, 1, ns, &j1);
    jinfo(s, 2, ns, &j2);
    MOQ_TEST_CHECK(!j0.present);   /* fully quiesced entries reclaim */
    MOQ_TEST_CHECK(!j1.present);
    MOQ_TEST_CHECK(!j2.present);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_split_brain");
    return failures;
}

/* Loser enforcement defers atomically on WOULD_BLOCK and retries. Two losing
 * namespaces from DISTINCT shard-0 publisher bindings, max_cancels = 1: the
 * round that enforces the canonically-first loser fills the cancel queue, so
 * the second loser's force-withdrawal preflight fails with ZERO mutation —
 * its announce, candidate bit, and holdout stay exactly as they were, and the
 * winner's mirror is NOT installed over the still-live announce. The next
 * round's pump drains the first cancel (its publisher has no live conn — the
 * moot-ack path), freeing the slot; the retry then withdraws the second loser
 * and converges. */
static int
test_shards_loser_would_block_retry(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.core_cfg.max_cancels = 1;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    char h0[24], h1[24];
    find_ns(h0, sizeof(h0), "lwA", BIT(0) | BIT(1), 1);   /* shard 0 loses */
    find_ns(h1, sizeof(h1), "lwB", BIT(0) | BIT(1), 1);
    MOQ_TEST_CHECK(h0[0] != '\0' && h1[0] != '\0');
    const char *sorted[2] = { h0, h1 };
    qsort(sorted, 2, sizeof(sorted[0]), ns1_canon_cmp);
    const char *first = sorted[0], *second = sorted[1];

    moqr_binding_t pa = pub_open_at(s, 0, 1);
    moqr_binding_t pb = pub_open_at(s, 0, 2);
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 0, pa, first) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, pb, second) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 1, p1, first) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 1, p1, second) == MOQR_OK);

    /* Step to the enforcement round (the first loser's cancel is peekable). */
    moqr_revoked_grant_t rg[4];
    int enforced_round = -1;
    for (int r = 0; r < 12 && enforced_round < 0; r++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
        if (peek_cancels(s, 0, rg, 4) == 1) {
            enforced_round = r;
        }
    }
    MOQ_TEST_CHECK(enforced_round >= 0);
    MOQ_TEST_CHECK_EQ_U64(rg[0].binding_cookie, 1);   /* the first loser's */
    moqr_shards_jinfo_t jf, js;
    jinfo(s, 0, first, &jf);
    jinfo(s, 0, second, &js);
    MOQ_TEST_CHECK(jf.candidates == BIT(1));          /* first: withdrawn */
    MOQ_TEST_CHECK_EQ_INT(jf.mirror, 1);
    MOQ_TEST_CHECK(!jf.holdout);
    MOQ_TEST_CHECK(js.holdout);                       /* second: deferred */
    MOQ_TEST_CHECK(js.candidates == (BIT(0) | BIT(1)));  /* zero mutation */
    MOQ_TEST_CHECK_EQ_INT(js.mirror, -1);             /* never installed over
                                                       * a still-live loser */

    /* One more round: the pump's moot-ack frees the slot and the retry lands. */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    jinfo(s, 0, second, &js);
    MOQ_TEST_CHECK(!js.holdout);
    MOQ_TEST_CHECK(js.candidates == BIT(1));
    MOQ_TEST_CHECK_EQ_INT(js.mirror, 1);
    MOQ_TEST_CHECK_EQ_SIZE(peek_cancels(s, 0, rg, 4), (size_t)1);
    MOQ_TEST_CHECK_EQ_U64(rg[0].binding_cookie, 2);   /* the second loser's */
    MOQ_TEST_CHECK_EQ_U64(rg[0].error_code, 0x20u);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, 0), 0);
    MOQ_TEST_CHECK_EQ_SIZE(peek_cancels(s, 0, rg, 4), (size_t)0);  /* drained */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_loser_would_block_retry");
    return failures;
}

/* Force-withdraw is a no-op on a MIRROR-owned announce: called directly against
 * the mirroring shard's core it returns OK without purging the mirror, queuing
 * a cancel, or perturbing the journal — and the mirror still routes demand. */
static int
test_shards_forcewithdraw_mirror_noop(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "mn") == MOQR_OK);
    step_n(s, 6);
    moqr_shards_jinfo_t j1;
    jinfo(s, 1, "mn", &j1);
    MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);
    moq_bytes_t nsp[1] = { { (const uint8_t *)"mn", 2 } };
    moqr_ns_t ns = { nsp, 1 };
    MOQ_TEST_CHECK(moqr_core_force_withdraw(moqr_shards_core(s, 1), ns, 0x6u,
                                            1000) == MOQR_OK);
    jinfo(s, 1, "mn", &j1);
    MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);               /* mirror untouched */
    MOQ_TEST_CHECK(j1.candidates == BIT(0));
    moqr_revoked_grant_t rg[2];
    MOQ_TEST_CHECK_EQ_SIZE(peek_cancels(s, 1, rg, 2), (size_t)0);  /* no cancel */
    step_n(s, 2);                                      /* stability */
    jinfo(s, 1, "mn", &j1);
    MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);
    /* The mirror still routes: a subscribe on the mirroring shard is refused as
     * remote-owner demand, exactly as before the no-op call. */
    moqr_binding_t d1 = sub_open(s, 1, 5);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "mn", 9, &sub) == MOQR_OK);
    step_n(s, 3);   /* round-trip: send, owner answers, requester resolves */
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 1);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_forcewithdraw_mirror_noop");
    return failures;
}

/* Cross-namespace ordering: the outbound cursor and dirty-reconcile walk the
 * journal in canonical (lexicographic) key order, not announce/arrival order. A
 * source announces cnoZ then cnoA through a 1-entry mailbox, so only one mirror is
 * in flight at a time; because export is lexicographic the alphabetically-first
 * namespace (cnoA) crosses FIRST — even though cnoZ was announced first. Then the
 * rest drains and both mirror, exactly once each. */
static int
test_shards_cross_ns_order(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 1, 0);   /* mailbox_entries = 1 */
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "cnoZ") == MOQR_OK);   /* announced first */
    MOQ_TEST_CHECK(ann(s, 0, p0, "cnoA") == MOQR_OK);   /* announced second */
    step_n(s, 2);
    moqr_shards_jinfo_t ja, jz;
    jinfo(s, 1, "cnoA", &ja);
    jinfo(s, 1, "cnoZ", &jz);
    /* Lexicographic export: cnoA mirrors before cnoZ despite the announce order. */
    MOQ_TEST_CHECK(ja.present && ja.mirror == 0);
    MOQ_TEST_CHECK(!jz.present);
    /* Draining the rest delivers cnoZ too — both mirrored. */
    step_n(s, 6);
    jinfo(s, 1, "cnoA", &ja);
    jinfo(s, 1, "cnoZ", &jz);
    MOQ_TEST_CHECK(ja.present && ja.mirror == 0);
    MOQ_TEST_CHECK(jz.present && jz.mirror == 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_cross_ns_order");
    return failures;
}

/* Reconcile retries a transient mirror op instead of dropping it — pinned on
 * the two-intent REPLACE, whose intermediate (old mirror unannounced, new
 * install WOULD_BLOCKed) must stay dirty and finish a round later, never
 * strand a winner with no mirror. Post-enforcement, a replace only occurs in
 * the enforcement-lag window, so the rig staggers it: five namespaces, all
 * winner-1 over {0,1}, from shard-0 bindings s1(cookie 1), m1(cookie 2), and
 * m2/m3/m4 (cookie 3), with max_cancels = 1:
 *   round 2: s1 enforced (fills the cancel slot); m1..m4 defer, zero mutation.
 *   round 3: the pump moot-acks s1's cancel; m1 enforced (exports absent(0));
 *            m2..m4 defer. The owner (shard 1) withdrew ALL FOUR mains between
 *            rounds 2 and 3, exporting absent(1) for each in round 3.
 *   round 4: shard 0 applies absent(1) first, so m2..m4 recompute winner =
 *            self and are never enforced (their announces survive). Shard 2
 *            applies the round-3 absences and reconciles four dirty entries
 *            on its 6-slot ring (max_ns_nodes = 6: root + five namespaces):
 *            m1 (both candidates gone) withdraws its mirror — 1 intent; m2/m3
 *            REPLACE 1->0 — 2 intents each; m4's replace unannounces (6/6)
 *            and its install WOULD_BLOCKs. m4 must stay dirty, mirror NONE.
 *   round 5: the drained ring lets m4's install complete — mirror 0. */
static int
test_shards_reconcile_would_block(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 3, 0);
    cfg.core_cfg.max_intents = 6;
    cfg.core_cfg.max_ns_nodes = 6;
    cfg.core_cfg.max_ns_subs = 3;
    cfg.core_cfg.max_subs = 3;
    cfg.core_cfg.max_cancels = 1;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    char h[5][24];
    const char *sorted[5];
    for (int i = 0; i < 5; i++) {
        char pfx[8];
        snprintf(pfx, sizeof(pfx), "rw%d", i);
        find_ns(h[i], sizeof(h[i]), pfx, BIT(0) | BIT(1), 1);
        MOQ_TEST_CHECK(h[i][0] != '\0');
        sorted[i] = h[i];
    }
    qsort(sorted, 5, sizeof(sorted[0]), ns1_canon_cmp);
    const char *s1 = sorted[0], *m1 = sorted[1];
    const char *m2 = sorted[2], *m3 = sorted[3], *m4 = sorted[4];

    moqr_binding_t pa = pub_open_at(s, 0, 1);
    moqr_binding_t pb = pub_open_at(s, 0, 2);
    moqr_binding_t pc = pub_open_at(s, 0, 3);
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 0, pa, s1) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, pb, m1) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, pc, m2) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, pc, m3) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, pc, m4) == MOQR_OK);
    for (int i = 0; i < 5; i++) {
        MOQ_TEST_CHECK(ann(s, 1, p1, sorted[i]) == MOQR_OK);
    }
    step_n(s, 2);                    /* rounds 1-2: converge + s1 enforced */
    MOQ_TEST_CHECK(unann(s, 1, p1, m1) == MOQR_OK);
    MOQ_TEST_CHECK(unann(s, 1, p1, m2) == MOQR_OK);
    MOQ_TEST_CHECK(unann(s, 1, p1, m3) == MOQR_OK);
    MOQ_TEST_CHECK(unann(s, 1, p1, m4) == MOQR_OK);
    step_n(s, 1);                    /* round 3 */
    step_n(s, 1);                    /* round 4: the collision */
    moqr_shards_jinfo_t j2, j3, j4;
    jinfo(s, 2, m2, &j2);
    jinfo(s, 2, m3, &j3);
    jinfo(s, 2, m4, &j4);
    MOQ_TEST_CHECK_EQ_INT(j2.mirror, 0);   /* replaced within the ring */
    MOQ_TEST_CHECK_EQ_INT(j3.mirror, 0);
    MOQ_TEST_CHECK_EQ_INT(j4.mirror, -1);  /* old gone, install WOULD_BLOCKed */
    MOQ_TEST_CHECK_EQ_INT(j4.winner, 0);
    step_n(s, 1);                    /* round 5: the retried install lands */
    jinfo(s, 2, m4, &j4);
    MOQ_TEST_CHECK_EQ_INT(j4.mirror, 0);   /* finished a round later */
    step_n(s, 5);                    /* settle */
    jinfo(s, 2, m2, &j2);
    jinfo(s, 2, m4, &j4);
    MOQ_TEST_CHECK(j2.candidates == BIT(0) && j2.mirror == 0);   /* stable */
    MOQ_TEST_CHECK(j4.candidates == BIT(0) && j4.mirror == 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_tokens(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_reconcile_would_block");
    return failures;
}

/* Inbound durability under a full receiver journal: an announce that arrives when
 * the receiver's candidate journal is full is NOT dropped — it stalls in the
 * mailbox (cursor lag) and is recorded once a slot frees. With a 1-entry journal,
 * each shard's own local namespace fills its journal, so the peer's broadcast
 * arrives at a full journal via the INBOUND path (no local fail-stop). Freeing the
 * local slot then lets the stalled peer announce through. */
static int
test_shards_inbound_saturation_durable(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.journal_entries = 1;   /* one namespace fills a shard's journal */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 0, p0, "A") == MOQR_OK);   /* shard 0's own slot */
    MOQ_TEST_CHECK(ann(s, 1, p1, "B") == MOQR_OK);   /* shard 1's own slot */
    /* Each shard's journal is now full with its local namespace; the peer's
     * broadcast stalls on the full journal without a local fail-stop. */
    for (int i = 0; i < 6; i++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* stall, not drop */
    }
    moqr_shards_jinfo_t j;
    jinfo(s, 0, "B", &j);
    MOQ_TEST_CHECK(!j.present);            /* B stalled in shard 0's mailbox */
    jinfo(s, 0, "A", &j);
    MOQ_TEST_CHECK(j.present);             /* shard 0's own A still held */

    /* Free shard 0's slot → the stalled B is recorded, exactly once. */
    MOQ_TEST_CHECK(unann(s, 0, p0, "A") == MOQR_OK);
    step_n(s, 8);
    jinfo(s, 0, "A", &j); MOQ_TEST_CHECK(!j.present);   /* A withdrawn */
    jinfo(s, 0, "B", &j);
    MOQ_TEST_CHECK(j.present && j.candidates == BIT(1));   /* B survived the stall */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_inbound_saturation_durable");
    return failures;
}

/* Local observation drop fail-stops the step. A borrowed NS_FOUND the router
 * cannot record (here: the candidate journal is full) is unrecoverable — it cannot
 * be parked or replayed — so moqr_shards_step surfaces a non-OK result rather than
 * letting the runtime report convergence over a candidate set that lost an
 * observation. */
static int
test_shards_local_drop_failstops(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.journal_entries = 2;   /* the local shard saturates at 2 namespaces */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "L0") == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "L1") == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* journal 2/2 */
    /* A third local announce the journal cannot hold: the core admits it, but the
     * router cannot record the observation → the step fail-stops. */
    MOQ_TEST_CHECK(ann(s, 0, p0, "L2") == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_ERR_NOMEM);
    moqr_shards_jinfo_t j;
    jinfo(s, 0, "L2", &j);
    MOQ_TEST_CHECK(!j.present);   /* the dropped observation is not in the set */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_local_drop_failstops");
    return failures;
}

/* Mailbox coalescing: an announce that stalls on a full receiver journal, then is
 * withdrawn, must COLLAPSE — the withdraw coalesces onto the stalled announce so
 * the pair nets to nothing and the mailbox drains. A per-namespace, per-slot
 * mailbox never head-of-line-blocks: the no-op withdraw applies even while the
 * journal stays full, so nothing gets wedged behind a blocked announce. */
static int
test_shards_mailbox_coalesce(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.journal_entries = 1;   /* shard 0's own slot stays full */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 0, p0, "A") == MOQR_OK);   /* fills shard 0's journal */
    MOQ_TEST_CHECK(ann(s, 1, p1, "X") == MOQR_OK);   /* stalls at shard 0 */
    step_n(s, 4);
    moqr_shards_jinfo_t j;
    jinfo(s, 0, "X", &j);
    MOQ_TEST_CHECK(!j.present);                                  /* X stalled */
    MOQ_TEST_CHECK(moqr_shards_debug_mailbox_pending(s, 1, 0) >= 1);

    /* Withdraw the stalled X: it must collapse in the mailbox, not wedge it. */
    MOQ_TEST_CHECK(unann(s, 1, p1, "X") == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_mailbox_pending(s, 1, 0), 0);  /* drained */
    jinfo(s, 0, "X", &j);
    MOQ_TEST_CHECK(!j.present);            /* X never recorded (collapsed to nothing) */
    jinfo(s, 0, "A", &j);
    MOQ_TEST_CHECK(j.present);             /* shard 0 still holds its own A */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_mailbox_coalesce");
    return failures;
}

/* Prefix-safe config: a caller that fills only the leading {struct_size, alloc,
 * shards, ...} prefix (stopping before the nested core/bind templates) creates a
 * default runtime. The whole config is poisoned first, so if create read the
 * uninitialized nested templates it would fault on garbage struct_sizes. */
static int
test_shards_prefix_safe(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    memset(&cfg, 0xAA, sizeof(cfg));   /* poison the whole config */
    size_t prefix = offsetof(moqr_shards_cfg_t, core_cfg);
    moqr_shards_cfg_init_sized(&cfg, prefix, &a.vt);   /* init only the prefix */
    cfg.shards = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_count(s), 2);
    MOQ_TEST_CHECK(moqr_shards_core(s, 0) != NULL);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_prefix_safe");
    return failures;
}

/* -- remote-owner subscribe demand refusal --------------------------------- */

/* A subscribe for a namespace this shard OWNS locally targets the local publisher
 * (a real cookie), so it never routes to the manager and is never counted. */
static int
test_shards_demand_local_owner(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "loc") == MOQR_OK);
    step_n(s, 4);
    moqr_binding_t d0 = sub_open(s, 0, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 0, d0, "loc", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 0), 0);   /* local demand never hits the manager */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_local_owner");
    return failures;
}

/* A subscribe for a namespace this shard only MIRRORS is refused with
 * NOT_SUPPORTED — but the refusal ROUND-TRIPS: the demand is forwarded to
 * the owner over the demand channel, the owner (admission left at its
 * default OFF) answers DONE, and only the requester's resolution counts the refusal
 * and rejects the parked sub. Every hop is asserted through the channel
 * gauges; the wire-visible 0x3 is pinned end-to-end by the loopback (the
 * resolving step's own bind pump consumes the REJECT_SUB intent here). */
static int
test_shards_demand_remote_refused(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "cam") == MOQR_OK);
    step_n(s, 8);
    moqr_shards_jinfo_t j1;
    jinfo(s, 1, "cam", &j1);
    MOQ_TEST_CHECK_EQ_INT(j1.mirror, 0);   /* shard 1 mirrors the owner */

    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "cam", 42, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* record + send */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 1, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);            /* nothing resolved yet */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* owner answers */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 1, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1), 1);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);            /* answer not seen yet  */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* resolve      */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 1);            /* the round-trip count */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_remote_refused");
    return failures;
}

/* Metric is per upstream attempt: two subscribers to one mirrored namespace in a
 * round share ONE pending upstream (one bump); a later re-subscribe (after the
 * first refusal retired the track) is a fresh attempt. */
static int
test_shards_demand_per_attempt(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "cam") == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sa, sb;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "cam", 1, &sa) == MOQR_OK);
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "cam", 2, &sb) == MOQR_OK);
    step_n(s, 3);                          /* one round-tripped attempt */
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 1);   /* two subs, one upstream attempt */
    moqr_sub_t sc;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "cam", 3, &sc) == MOQR_OK);
    step_n(s, 3);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 2);   /* fresh attempt bumps again */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_per_attempt");
    return failures;
}

/* Pending-demand array full: a borrowed UPSTREAM_SUBSCRIBE that can't be recorded
 * fail-stops the step with MOQR_ERR_NOMEM — never a silent drop. */
static int
test_shards_demand_failstop(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.pending_demand_entries = 1;   /* one pending demand fills the array */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "A") == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "B") == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sa, sb;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "A", 1, &sa) == MOQR_OK);
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "B", 2, &sb) == MOQR_OK);
    /* Two remote-owner demands in one round, capacity 1 → the second cannot be
     * recorded. Fail-stop means the step does NOT report convergence (returns
     * MOQR_ERR_NOMEM), not that it rolls back: the first demand may still be
     * refused this round; the point is the lost second demand is surfaced, never
     * silently dropped. */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_ERR_NOMEM);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_failstop");
    return failures;
}

/* Drive a K=3 runtime to the round where shard 2 holds the owner's DONE for
 * its forwarded wbB demand (three parked subs) but the resolving
 * upstream_error WOULD_BLOCKs: the prior round's phase-3 withdrawal of TWO
 * mirrors (the owner unannounced wbX and wbY together) left two echo intents
 * on the 4-slot ring (max_ns_nodes = 4: the root + three namespaces), so the
 * three rejects can't fit and the DONE stays durable at the reply-channel
 * head. All three namespaces have a single owner (shard 1) — no split-brain,
 * so loser enforcement never runs here. On return pending_demand(2) == 1 and
 * remote_demand_refused(2) == 0, with sa/sb/sc the three parked subs. Caller
 * owns *s and passed-in `a`. */
static moqr_shards_t *
wb_setup(ca_t *a, moqr_sub_t *sa, moqr_sub_t *sb, moqr_sub_t *sc)
{
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, a, 3, 0);
    cfg.core_cfg.max_intents = 3;
    cfg.core_cfg.max_ns_nodes = 4;
    cfg.core_cfg.max_ns_subs = 3;
    cfg.core_cfg.max_subs = 3;
    moqr_shards_t *s = NULL;
    if (moqr_shards_create(&cfg, &s) != MOQR_OK) {
        return NULL;
    }
    moqr_binding_t p1 = pub_open(s, 1);
    (void)ann(s, 1, p1, "wbX");
    (void)ann(s, 1, p1, "wbY");
    (void)ann(s, 1, p1, "wbB");
    step_n(s, 10);   /* converge: shards 0 and 2 mirror PB(·,1) for all three */
    /* The owner withdraws wbX and wbY while three subscribers demand wbB.
     * Round-trip timing: step T sends the DEMAND and exports the absences;
     * step T+1 has the owner answer DONE while shard 2's reconcile withdraws
     * BOTH mirrors (two echo intents on its 4-slot ring); step T+2's phase-1
     * resolution then needs three rejects against two free slots and
     * WOULD_BLOCKs — the answer stays durable at the reply-channel head. */
    (void)unann(s, 1, p1, "wbX");
    (void)unann(s, 1, p1, "wbY");
    moqr_binding_t d2 = sub_open(s, 2, 5);
    (void)do_subscribe(s, 2, d2, "wbB", 1, sa);
    (void)do_subscribe(s, 2, d2, "wbB", 2, sb);
    (void)do_subscribe(s, 2, d2, "wbB", 3, sc);
    step_n(s, 3);
    return s;
}

/* upstream_error WOULD_BLOCK: the recorded demand is durable — the pending entry
 * survives and nothing is counted until a later round drains the ring and the
 * retry completes. */
static int
test_shards_demand_would_block_retry(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_sub_t sa, sb, sc;
    moqr_shards_t *s = wb_setup(&a, &sa, &sb, &sc);
    MOQ_TEST_CHECK(s != NULL);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 2), 1);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 2), 0);   /* WOULD_BLOCK: kept, uncounted */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 2), 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 2), 1);   /* retry drained the ring, one attempt */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_would_block_retry");
    return failures;
}

/* Moot result: after the demand is recorded and WOULD_BLOCKed, its parked subs are
 * freed and the recorded track is resolved out of PENDING before the retry. The
 * retry's upstream_error then returns WRONG_STATE (or STALE_HANDLE) → the pending
 * entry is dropped as moot, uncounted, and without a fail-stop. */
static int
test_shards_demand_stale_moot(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_sub_t sa, sb, sc;
    moqr_shards_t *s = wb_setup(&a, &sa, &sb, &sc);
    MOQ_TEST_CHECK(s != NULL);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 2), 1);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 2), 0);
    /* Free both parked subs (intent-free), then resolve the recorded track from
     * outside the manager — with no parked sub left, upstream_ok needs no ring
     * space, so it succeeds on the WOULD_BLOCK-full ring and moves the track to
     * ACTIVE. The manager's recorded {track, track_gen} is now moot. */
    uint64_t tgen = 0;
    moqr_track_t th = moqr_shards_debug_pending_demand_track(s, 2, 0, &tgen);
    (void)moqr_core_unsubscribe(moqr_shards_core(s, 2), sa, 1000);
    (void)moqr_core_unsubscribe(moqr_shards_core(s, 2), sb, 1000);
    (void)moqr_core_unsubscribe(moqr_shards_core(s, 2), sc, 1000);
    MOQ_TEST_CHECK(moqr_core_upstream_ok(moqr_shards_core(s, 2), th, tgen, 0,
                                         false, 0, 0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* not a fail-stop */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 2), 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 2), 0);   /* moot: dropped, uncounted */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_stale_moot");
    return failures;
}

/* Cancel before SENT: the subscriber leaves while the demand is only
 * RECORDED. A PENDING track fires no intent for that, so the phase-5 probe
 * (moqr_core_upstream_cancel) is the only detector: the entry drops without a
 * wire message and the requester's track table drains. */
static int
test_shards_demand_cancel_before_sent(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "cbs") == MOQR_OK);
    step_n(s, 8);

    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "cbs", 5, &sub) == MOQR_OK);
    /* One step records the demand (phase 2) — and its own phase 5 already
     * probes-then-sends, so cancel before THAT step to hit RECORDED. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub, 100) ==
                   MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 1, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);            /* nothing round-tripped */
    moqr_core_stats_t cs;
    moqr_core_get_stats(moqr_shards_core(s, 1), &cs);
    MOQ_TEST_CHECK_EQ_U64(cs.tracks, 0);            /* no PENDING leak */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_cancel_before_sent");
    return failures;
}

/* Cancel after SENT (before the answer): the probe retires the local track,
 * an UNDEMAND flows to the owner (moot with admission at its default OFF),
 * and the owner's late DONE resolves moot by the gen guard: refused count
 * stays zero and nothing leaks on either side. */
static int
test_shards_demand_cancel_after_sent(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "cas") == MOQR_OK);
    step_n(s, 8);

    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "cas", 5, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* SENT */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 1, 0), 1);
    /* The subscriber leaves while the demand is in flight. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub, 100) ==
                   MOQR_OK);
    step_n(s, 5);   /* probe cancels, UNDEMAND flows, late DONE moots */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);            /* moot, never counted */
    for (uint16_t i = 0; i < 2; i++) {
        for (uint16_t d = 0; d < 2; d++) {
            MOQ_TEST_CHECK_EQ_U64(
                moqr_shards_debug_demand_channel_pending(s, i, d), 0);
        }
    }
    moqr_core_stats_t cs;
    moqr_core_get_stats(moqr_shards_core(s, 1), &cs);
    MOQ_TEST_CHECK_EQ_U64(cs.tracks, 0);            /* no PENDING leak */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_cancel_after_sent");
    return failures;
}

/* A 1-slot demand channel with two concurrent demands pins BOTH channel
 * disciplines at once. Lag-not-loss: the second DEMAND stays RECORDED until
 * the ring frees, and both round-trips complete exactly once.
 * Pop-only-when-durable: with ascending visit order the owner's second
 * answer finds the 1-slot reply channel still holding the first (the
 * requester resolves later the same round), so the second DEMAND stays
 * durable at the request-channel head until the reply fits. */
static int
test_shards_demand_channel_full(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.demand_channel_entries = 1;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dfA") == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dfB") == MOQR_OK);
    step_n(s, 8);

    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sa, sb;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dfA", 1, &sa) == MOQR_OK);
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dfB", 2, &sb) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    /* Only one DEMAND fit; the other is lag, not loss. */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 1, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 2);
    step_n(s, 2);
    /* First answered; the owner's SECOND answer could not fit the occupied
     * reply channel, so the second DEMAND is still held durably. */
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 1, 0), 1);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 2);            /* both, exactly once */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    for (uint16_t i = 0; i < 2; i++) {
        for (uint16_t d = 0; d < 2; d++) {
            MOQ_TEST_CHECK_EQ_U64(
                moqr_shards_debug_demand_channel_pending(s, i, d), 0);
        }
    }
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_channel_full");
    return failures;
}

/* The demand channel and the announce mailbox are separate organs: with the
 * demand channel saturated, announce replication still converges. */
static int
test_shards_demand_mailbox_isolated(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.demand_channel_entries = 1;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "isoA") == MOQR_OK);
    step_n(s, 8);
    /* Saturate the (1 -> 0) demand channel with an un-drainable demand: fill
     * the reply direction first so the owner cannot answer... simpler: two
     * demands with a 1-slot request channel keep it occupied across steps. */
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sa;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "isoA", 1, &sa) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* DEMAND in ring */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 1, 0), 1);
    /* A brand-new announce still replicates over the mailbox. */
    MOQ_TEST_CHECK(ann(s, 0, p0, "isoB") == MOQR_OK);
    step_n(s, 6);
    moqr_shards_jinfo_t j;
    jinfo(s, 1, "isoB", &j);
    MOQ_TEST_CHECK(j.present && j.mirror == 0);   /* replication unaffected */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_mailbox_isolated");
    return failures;
}

/* -- owner-side admission (admit_remote_demand = true) ---------------------- */

/* An admission-enabled runtime (deterministic tests own the flag directly;
 * the production CLI turns it on for every multi-lane serve). */
static moqr_shards_t *
mk_shards_admit(ca_t *a, uint16_t k, uint32_t dch)
{
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, a, k, 0);
    cfg.admit_remote_demand = true;
    if (dch != 0) {
        cfg.demand_channel_entries = dch;
    }
    moqr_shards_t *s = NULL;
    (void)moqr_shards_create(&cfg, &s);
    return s;
}

/* Total occupancy across every directed demand channel (0 = quiesced). */
static uint32_t
dch_total(moqr_shards_t *s, uint16_t k)
{
    uint32_t n = 0;
    for (uint16_t i = 0; i < k; i++) {
        for (uint16_t d = 0; d < k; d++) {
            n += moqr_shards_debug_demand_channel_pending(s, i, d);
        }
    }
    return n;
}

static uint32_t
core_tracks(moqr_shards_t *s, uint16_t i)
{
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, i), &st);
    return st.tracks;
}

static uint64_t
resolved(moqr_shards_t *s, uint16_t i, uint64_t *code, bool *pre)
{
    return moqr_shards_debug_remote_demand_resolved(s, i, code, pre);
}

/* Ingest one object (optionally with properties, a status shape, or the
 * datagram preference) into an owned track on shard i. Payload bytes are a
 * fill pattern so identity survives the boundary crossing verifiably. */
static moqr_result_t
pub_obj(moqr_shards_t *s, ca_t *a, uint16_t i, moqr_track_t th, uint64_t g,
        uint64_t sg, uint64_t o, uint8_t fill, size_t len, uint8_t prop_fill,
        size_t prop_len, uint8_t status, bool datagram, bool eog)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 77;
    d.status = (moqr_obj_status_t)status;
    d.datagram_pref = datagram;
    d.end_of_group = eog;
    d.now_us = 1;
    if (len != 0) {
        uint8_t buf[256];
        memset(buf, fill, len);
        if (moq_rcbuf_create(&a->vt, buf, len, &d.payload) != 0) {
            return MOQR_ERR_NOMEM;
        }
    }
    if (prop_len != 0) {
        uint8_t buf[64];
        memset(buf, prop_fill, prop_len);
        if (moq_rcbuf_create(&a->vt, buf, prop_len, &d.properties) != 0) {
            moq_rcbuf_decref(d.payload);
            return MOQR_ERR_NOMEM;
        }
    }
    moqr_result_t rc = moqr_core_ingest(moqr_shards_core(s, i), th, &d);
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(d.payload);
        moq_rcbuf_decref(d.properties);
    }
    return rc;
}

/* Open an owned ACTIVE (ns, "v") track on shard i via publish, returning its
 * handle so tests can keep ingesting into it. */
static moqr_result_t
pub_track_h(moqr_shards_t *s, uint16_t i, moqr_binding_t b, const char *p,
            moqr_track_t *out)
{
    moq_bytes_t nsp[1] = { { (const uint8_t *)p, (uint32_t)strlen(p) } };
    moqr_ns_t ns = { nsp, 1 };
    moq_bytes_t name = { (const uint8_t *)"v", 1 };
    return moqr_core_publish_open(moqr_shards_core(s, i), b, ns, name, 900,
                                  out);
}

/* Pull one delivery from shard i's TEST binding (pull-model: the bind never
 * pulls for a conn-less binding, so retained records wait for the test),
 * copying the identity + payload/property fingerprints out of the borrowed
 * view before confirming DELIVERED. Returns next_delivery's result. */
typedef struct dl_probe {
    uint64_t group, subgroup, object;
    uint8_t  prio;
    uint8_t  status;
    bool     datagram;
    bool     eog;
    size_t   len, prop_len;
    uint8_t  first, prop_first;
    uint8_t  notice;
    bool     sg_end;
    moqr_reset_desc_t reset;
} dl_probe_t;

static moqr_result_t
pull_one(moqr_shards_t *s, uint16_t i, moqr_binding_t b, dl_probe_t *out)
{
    moqr_delivery_t d;
    moqr_result_t rc =
        moqr_core_next_delivery(moqr_shards_core(s, i), b, 1000, &d);
    if (rc != MOQR_OK) {
        return rc;
    }
    memset(out, 0, sizeof(*out));
    out->group = d.rec.group_id;
    out->subgroup = d.rec.subgroup_id;
    out->object = d.rec.object_id;
    out->prio = d.rec.publisher_priority;
    out->status = (uint8_t)d.rec.status;
    out->datagram = d.rec.datagram_pref;
    out->eog = d.rec.end_of_group;
    out->notice = d.notice;
    out->sg_end = d.subgroup_end;
    out->reset = d.rec.reset;
    if (d.rec.payload != NULL) {
        out->len = moq_rcbuf_len(d.rec.payload);
        out->first = moq_rcbuf_data(d.rec.payload)[0];
    }
    for (uint32_t ci = 0; ci < d.rec.chunk_count; ci++) {
        const moq_rcbuf_t *cb = NULL;
        uint64_t cl = 0;
        if (moqr_core_delivery_chunk(moqr_shards_core(s, i), b, ci, &cb,
                                     &cl) != MOQR_OK) {
            break;
        }
        if (ci == 0) {
            out->first = moq_rcbuf_data(cb)[0];
        }
        out->len += (size_t)cl;
    }
    if (d.rec.properties != NULL) {
        out->prop_len = moq_rcbuf_len(d.rec.properties);
        out->prop_first = moq_rcbuf_data(d.rec.properties)[0];
    }
    (void)moqr_core_delivery_done(moqr_shards_core(s, i), b,
                                  MOQR_DELIVERY_DELIVERED, 1000);
    return MOQR_OK;
}

/* Publish one object into shard i so the (ns, "v") track is ACTIVE — the
 * state an admitted pump-sub joins with an immediate accept. */
static moqr_result_t
pub_track(moqr_shards_t *s, ca_t *a, uint16_t i, moqr_binding_t b,
          const char *p)
{
    moq_bytes_t nsp[1] = { { (const uint8_t *)p, (uint32_t)strlen(p) } };
    moqr_ns_t ns = { nsp, 1 };
    moq_bytes_t name = { (const uint8_t *)"v", 1 };
    moqr_track_t th;
    moqr_result_t rc = moqr_core_publish_open(moqr_shards_core(s, i), b, ns,
                                              name, 900, &th);
    if (rc != MOQR_OK) {
        return rc;
    }
    uint8_t buf[16];
    memset(buf, 0x5A, sizeof(buf));
    moq_rcbuf_t *pl = NULL;
    if (moq_rcbuf_create(&a->vt, buf, sizeof(buf), &pl) != 0) {
        return MOQR_ERR_NOMEM;
    }
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = 0;
    d.subgroup_id = 0;
    d.object_id = 0;
    d.publisher_priority = 128;
    d.payload = pl;
    d.now_us = 1;
    rc = moqr_core_ingest(moqr_shards_core(s, i), th, &d);
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(pl);
    }
    return rc;
}

/* The full acknowledge lifecycle: a remote demand for an ACTIVE owner track
 * round-trips DEMAND -> pump-sub accept -> ACK, and the requester's parked
 * subscriber goes ACTIVE with upstream_cookie = demand_id. The acknowledged
 * entry stays counted (it is the cancel notice's durable home), and a second
 * subscriber then fast-paths on the ACTIVE local track without a new demand. */
static int
test_shards_admit_ack_lifecycle(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "adA") == MOQR_OK);
    MOQ_TEST_CHECK(pub_track(s, &a, 0, p0, "adA") == MOQR_OK);
    step_n(s, 8);
    moqr_shards_jinfo_t j;
    jinfo(s, 1, "adA", &j);
    MOQ_TEST_CHECK_EQ_INT(j.mirror, 0);

    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "adA", 7, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* DEMAND sent */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 1, 0), 1);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* admit + ACK */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1), 1);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* resolve ACK */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 1);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);   /* the parked sub accepted */
    MOQ_TEST_CHECK_EQ_U64(st.subs_parked, 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, NULL, NULL), 0);

    /* Fast path: the local track is ACTIVE now — no second round-trip. */
    moqr_sub_t sub2;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "adA", 8, &sub2) == MOQR_OK);
    step_n(s, 3);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 1);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 2);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_ack_lifecycle");
    return failures;
}

/* The owner's REJECT code arrives verbatim: a demand recorded in the one-round
 * window after the owner unannounced (the requester's mirror is still up) is
 * admitted against an empty trie, rejected DOES_NOT_EXIST (0x10), and the
 * requester resolves exactly that code pre-ACK. The withdrawn mirror is NOT a
 * re-target — the in-flight demand waits for the owner's answer. */
static int
test_shards_admit_reject_code(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "adR") == MOQR_OK);
    step_n(s, 8);

    MOQ_TEST_CHECK(unann(s, 0, p0, "adR") == MOQR_OK);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "adR", 7, &sub) == MOQR_OK);
    step_n(s, 6);
    uint64_t code = 0;
    bool pre = false;
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, &code, &pre), 1);
    MOQ_TEST_CHECK_EQ_U64(code, 0x10);   /* DOES_NOT_EXIST, verbatim */
    MOQ_TEST_CHECK(pre);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_reject_code");
    return failures;
}

/* A post-ACK terminal carries the owner's code verbatim: after the lifecycle
 * settles ACTIVE, the owner force-withdraws its source with an arbitrary
 * wire code; the pump-sub's SUB_DONE becomes DONE(post-ACK, code) and the
 * requester resolves it through source_done — refused stays zero. */
static int
test_shards_admit_sub_done_code(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "adS") == MOQR_OK);
    MOQ_TEST_CHECK(pub_track(s, &a, 0, p0, "adS") == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "adS", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 1);

    moq_bytes_t nsp[1] = { { (const uint8_t *)"adS", 3 } };
    moqr_ns_t ns = { nsp, 1 };
    MOQ_TEST_CHECK(moqr_core_force_withdraw(moqr_shards_core(s, 0), ns, 0x42,
                                            1000) == MOQR_OK);
    step_n(s, 6);
    uint64_t code = 0;
    bool pre = true;
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, &code, &pre), 1);
    /* The owner's active-sub terminal is PUBLISH_DONE TRACK_ENDED; the
     * withdrawal's REQUEST_ERROR-domain argument never crosses into it. */
    MOQ_TEST_CHECK_EQ_U64(code, 0x2);
    MOQ_TEST_CHECK(!pre);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);   /* served then done — not refused */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_sub_done_code");
    return failures;
}

/* Cancel before ACK: the requester leaves while the owner's pump-sub is
 * parked on a PENDING owner track. The probe turns the entry UNDEMANDING,
 * the UNDEMAND runs the staged teardown, and stage 2's upstream cancel is
 * what keeps the owner track from sitting in PENDING limbo — both cores
 * drain to zero tracks. */
static int
test_shards_admit_cancel_before_ack(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "adC") == MOQR_OK);   /* announce only */
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "adC", 7, &sub) == MOQR_OK);
    step_n(s, 2);   /* admitted; the owner track awaits its own publisher */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 0), 1);

    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub, 1000) ==
                   MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 0), 0);   /* no PENDING limbo */
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, NULL, NULL), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_cancel_before_ack");
    return failures;
}

/* Cancel after ACK: with the demand ACTIVE, the last subscriber leaves, the
 * linger elapses, and the core's UPSTREAM_UNSUBSCRIBE — carrying
 * upstream_cookie = demand_id toward the mirror pseudo-binding — is what the
 * router turns into the UNDEMAND. The owner track survives as the owner's
 * own retained source; everything demand-shaped drains. */
static int
test_shards_admit_cancel_after_ack(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "adD") == MOQR_OK);
    MOQ_TEST_CHECK(pub_track(s, &a, 0, p0, "adD") == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "adD", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 1);

    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub, 1000) ==
                   MOQR_OK);
    /* Advance past the linger deadline so the tick emits the unsubscribe. */
    for (int i = 0; i < 6; i++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 5000) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 0), 1);   /* the owner's own source */
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, NULL, NULL), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_cancel_after_ack");
    return failures;
}

/* The staged teardown is retry-safe across a WOULD_BLOCK: the owner's
 * UNDEMAND lands in the same round its reconcile installs a fresh mirror
 * whose NS_FOUND fan (four watchers) fills the 4-slot intent ring — stage 1
 * retires the pump-sub, stage 2's cancel preflight WOULD_BLOCKs, and the
 * entry is HELD (never dropped, never re-running stage 1). Next round the
 * drained ring lets stage 2 land and the owner track is released — the leak
 * the hold exists to prevent. */
static int
test_shards_admit_undemand_staged_retry(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    /* The intent ring's floor is max(max_subs + 1, max_ns_subs,
     * max_ns_nodes): pin the terms so the ring is exactly 4 — one mirror
     * install with four matching watchers (the manager's plus three test
     * wildcards) fills it completely. */
    cfg.core_cfg.max_intents = 4;
    cfg.core_cfg.max_ns_nodes = 4;
    cfg.core_cfg.max_ns_subs = 4;
    cfg.core_cfg.max_subs = 3;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "usX") == MOQR_OK);   /* announce only */
    moqr_binding_t w0 = sub_open(s, 0, 5);
    moqr_ns_t wild = { NULL, 0 };
    for (uint64_t w = 0; w < 3; w++) {
        MOQ_TEST_CHECK(moqr_core_ns_subscribe(moqr_shards_core(s, 0), w0, wild,
                                              100 + w) == MOQR_OK);
    }
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "usX", 7, &sub) == MOQR_OK);
    step_n(s, 2);   /* admitted; owner track PENDING under the pump-sub */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 0), 1);

    /* A fresh requester announce rides the same round as the UNDEMAND: the
     * owner's reconcile installs its mirror first, fanning NS_FOUND to all
     * four watchers (ring 4/4), so the teardown's stage 2 finds no space. */
    moqr_binding_t p1 = pub_open_at(s, 1, 3);
    MOQ_TEST_CHECK(ann(s, 1, p1, "usA") == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub, 1000) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* UNDEMAND out */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* stage 2 held */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 0), 1);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* stage 2 lands */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_undemand_staged_retry");
    return failures;
}

/* Mirror re-target fails terminally, never migrates silently: while the
 * demand is in flight to shard 1, the namespace's home moves to shard 2 —
 * the old owner withdraws (a merely-GONE mirror leaves the demand waiting,
 * pinned by the reject-code test above), and once the withdrawal has
 * propagated the new owner announces. The requester's re-installed mirror no
 * longer matches the demand's origin: the local track terminates GOING_AWAY,
 * the OLD owner gets the UNDEMAND, and its staged teardown releases the
 * orphaned pump-sub and owner track. The subscriber's retry would ride the
 * new mirror. */
static int
test_shards_admit_retarget(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 3, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 1, p1, "rt") == MOQR_OK);   /* announce only */
    step_n(s, 10);
    moqr_shards_jinfo_t j;
    jinfo(s, 0, "rt", &j);
    MOQ_TEST_CHECK_EQ_INT(j.mirror, 1);

    moqr_binding_t d0 = sub_open(s, 0, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 0, d0, "rt", 7, &sub) == MOQR_OK);
    step_n(s, 2);   /* SENT; shard 1 admitted (owner track PENDING) */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 1), 1);

    MOQ_TEST_CHECK(unann(s, 1, p1, "rt") == MOQR_OK);
    step_n(s, 4);   /* the withdrawal propagates; mirrors come down; the
                     * demand stays SENT (a gone mirror is not a re-target) */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 0), 1);
    moqr_binding_t p2 = pub_open(s, 2);
    MOQ_TEST_CHECK(ann(s, 2, p2, "rt") == MOQR_OK);
    step_n(s, 8);
    jinfo(s, 0, "rt", &j);
    MOQ_TEST_CHECK_EQ_INT(j.mirror, 2);   /* re-targeted */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 0), 0);   /* terminal, no migration */
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 0, NULL, NULL), 0);   /* local fallback */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 3), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_retarget");
    return failures;
}

/* Demand ids are only monotonic per REQUESTER: two requester shards' FIRST
 * demands both carry id 1 to the same owner, and every owner-side
 * correlation (the pump-sub lifecycle intents and the UNDEMAND) must key on
 * {src, demand_id}, never the id alone. Both demands ACK independently, and
 * cancelling one requester tears down exactly ITS pump-sub — the other stays
 * ACTIVE end-to-end. */
static int
test_shards_admit_demand_id_collision(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 3, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "adK") == MOQR_OK);
    MOQ_TEST_CHECK(pub_track(s, &a, 0, p0, "adK") == MOQR_OK);
    step_n(s, 10);

    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_binding_t d2 = sub_open(s, 2, 2);
    moqr_sub_t s1, s2;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "adK", 7, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(do_subscribe(s, 2, d2, "adK", 8, &s2) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 2);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);   /* both ACKed independently */
    moqr_core_get_stats(moqr_shards_core(s, 2), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 2), 1);

    /* Cancel shard 2's demand: its UNDEMAND carries the SAME id as shard 1's
     * still-live demand, and must not touch shard 1's pump-sub. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 2), s2, 1000) ==
                   MOQR_OK);
    for (int i = 0; i < 6; i++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 5000) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 2), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 1);
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);   /* shard 1 untouched */

    /* Then shard 1's, draining to full quiescence. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), s1, 5000) ==
                   MOQR_OK);
    for (int i = 0; i < 6; i++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 9000) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 2), 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 2), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 3), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_demand_id_collision");
    return failures;
}

/* The tagged terminal survives the cross-shard crossing whole. The owner
 * copies it out of its core intent, into the demand message, and the
 * requester reads it back out — three copies of a 64-bit value whose ORIGIN
 * draft is the only thing that makes it translatable later. A 1-slot reply
 * channel forces the owner to be refused and hold the DONE for a round, so
 * the descriptor also has to survive a re-drive: what the requester finally
 * peeks must be byte-for-byte what the owner staged.
 */
static int
run_shards_done_pd_crossing(const char *label, moq_version_t origin,
                            uint64_t wire, const char *ns)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 1);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, ns) == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, ns, &ot) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x41, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, ns, 7, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* DEMAND sent */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* ACK in ring */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1), 1);

    /* NEGATIVE CONTROL: nothing has resolved yet, so the reader reports
     * "none" and leaves the caller's descriptor alone — it must never hand
     * back a zeroed struct that reads as a real NONE terminal. */
    moqr_pd_desc_t untouched;
    untouched.tag = MOQR_CODE_LOCAL;
    untouched.value = 0xA5A5A5A5u;
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, NULL, NULL), 0);
    MOQ_TEST_CHECK(!moqr_shards_debug_remote_demand_last_pd(s, 1, &untouched));
    MOQ_TEST_CHECK(untouched.tag == MOQR_CODE_LOCAL);
    MOQ_TEST_CHECK_EQ_U64(untouched.value, 0xA5A5A5A5u);
    /* invalid inputs stay fail-closed and equally non-mutating */
    MOQ_TEST_CHECK(!moqr_shards_debug_remote_demand_last_pd(NULL, 1,
                                                            &untouched));
    MOQ_TEST_CHECK(!moqr_shards_debug_remote_demand_last_pd(s, 99,
                                                            &untouched));
    MOQ_TEST_CHECK(!moqr_shards_debug_remote_demand_last_pd(s, 1, NULL));
    MOQ_TEST_CHECK(untouched.tag == MOQR_CODE_LOCAL);
    MOQ_TEST_CHECK_EQ_U64(untouched.value, 0xA5A5A5A5u);

    moqr_pd_desc_t up;
    MOQ_TEST_CHECK(moqr_pd_desc_wire(origin, wire, &up) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_source_done(moqr_shards_core(s, 0), ot, 1, up,
                                         1000) == MOQR_OK);

    /* REFUSED: the single reply slot still holds the ACK, so the owner's DONE
     * cannot be pushed and is held on its entry. */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, NULL, NULL), 0);
    moqr_pd_desc_t head;
    /* the head is still the ACK, not a terminal */
    MOQ_TEST_CHECK(!moqr_shards_debug_demand_channel_head_pd(s, 0, 1, &head));

    /* RE-DRIVE: the slot frees and the held DONE is pushed, descriptor whole. */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK(moqr_shards_debug_demand_channel_head_pd(s, 0, 1, &head));
    MOQ_TEST_CHECK(head.tag == (origin == MOQ_VERSION_DRAFT_16
                                    ? MOQR_CODE_WIRE_D16
                                    : MOQR_CODE_WIRE_D18));
    MOQ_TEST_CHECK_EQ_U64(head.value, wire);

    /* RE-PEEK: reading the head does not consume or alter it. */
    moqr_pd_desc_t head2;
    MOQ_TEST_CHECK(moqr_shards_debug_demand_channel_head_pd(s, 0, 1, &head2));
    MOQ_TEST_CHECK(head2.tag == head.tag);
    MOQ_TEST_CHECK_EQ_U64(head2.value, head.value);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1), 1);

    /* The terminal follows the release, and what the requester resolved is
     * the descriptor the owner staged — tag, origin and all 64 bits. */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    uint64_t code = 0;
    bool pre = true;
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, &code, &pre), 1);
    MOQ_TEST_CHECK(!pre);
    moqr_pd_desc_t got;
    MOQ_TEST_CHECK(moqr_shards_debug_remote_demand_last_pd(s, 1, &got));
    printf("ORACLE %s staged{tag=%d val=0x%llx} resolved{tag=%d val=0x%llx}\n",
           label, (int)up.tag, (unsigned long long)up.value, (int)got.tag,
           (unsigned long long)got.value);
    MOQ_TEST_CHECK(got.tag == up.tag);
    MOQ_TEST_CHECK_EQ_U64(got.value, up.value);

    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    if (failures == 0) {
        MOQ_TEST_PASS(label);
    }
    return failures;
}

static int
test_shards_done_pd_crossing_d18_64bit(void)
{
    return run_shards_done_pd_crossing("shards_done_pd_crossing_d18_64bit",
                                       MOQ_VERSION_DRAFT_18,
                                       UINT64_C(0x100000000), "pdA");
}

/* Same crossing, the OTHER origin: the tag is carried, not assumed. */
static int
test_shards_done_pd_crossing_d16_64bit(void)
{
    return run_shards_done_pd_crossing("shards_done_pd_crossing_d16_64bit",
                                       MOQ_VERSION_DRAFT_16,
                                       UINT64_C(0x1FFFFFFFF), "pdB");
}

/* A pre-ACK refusal is a REQUEST_ERROR answer, not a terminal: it must resolve
 * with NO tagged descriptor at all, so nothing downstream can mistake the
 * request code for a PUBLISH_DONE status. */
static int
test_shards_done_pd_pre_ack_is_none(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "pdC") == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "pdC", 7, &sub) == MOQR_OK);
    /* The owner withdraws the namespace while the demand is in flight, so the
     * answer is a refusal that never reached an ACCEPT. */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    MOQ_TEST_CHECK(unann(s, 0, p0, "pdC") == MOQR_OK);
    step_n(s, 12);
    uint64_t code = 0;
    bool pre = false;
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, &code, &pre), 1);
    MOQ_TEST_CHECK(pre);
    moqr_pd_desc_t got;
    MOQ_TEST_CHECK(moqr_shards_debug_remote_demand_last_pd(s, 1, &got));
    MOQ_TEST_CHECK(got.tag == MOQR_CODE_NONE);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_done_pd_pre_ack_is_none");
    return failures;
}

/* Reply-channel backpressure is lag, never loss, and never reorders: with a
 * 1-slot channel the owner's DONE finds the ring still holding the ACK and
 * is HELD on the entry until the slot frees — the requester sees ACK then
 * DONE(code) in order, exactly once. */
static int
test_shards_admit_reply_channel_full(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 1);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "adF") == MOQR_OK);
    MOQ_TEST_CHECK(pub_track(s, &a, 0, p0, "adF") == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "adF", 7, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* DEMAND sent */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* ACK in ring */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1), 1);

    /* The source terminates while the ACK still occupies the reply slot. */
    moq_bytes_t nsp[1] = { { (const uint8_t *)"adF", 3 } };
    moqr_ns_t ns = { nsp, 1 };
    MOQ_TEST_CHECK(moqr_core_force_withdraw(moqr_shards_core(s, 0), ns, 0x42,
                                            1000) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    /* The owner (stepping first) found the ring full: DONE held on the
     * entry; the requester (stepping second) resolved the ACK. */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 1);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* DONE pushed */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* DONE resolved */
    uint64_t code = 0;
    bool pre = true;
    MOQ_TEST_CHECK_EQ_U64(resolved(s, 1, &code, &pre), 1);
    MOQ_TEST_CHECK_EQ_U64(code, 0x2);   /* PUBLISH_DONE TRACK_ENDED */
    MOQ_TEST_CHECK(!pre);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_admit_reply_channel_full");
    return failures;
}

/* -- the whole-object data pump (admit_remote_demand = true) ---------------- */

/* One admitted whole-object round trip, identity exact: payload bytes,
 * properties, priority, ids, and the header-EOG bit all survive the clone
 * crossing; the requester's local subscriber then pulls the record from its
 * OWN log. Status and datagram shapes cross too. */
static int
test_shards_data_obj_delivery(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtA") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtA", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtA", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED: the pump-sub rides the live edge */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 3, 0, 0xA7, 24, 0xB1, 8, 0, false,
                           true) == MOQR_OK);
    step_n(s, 4);   /* extract + cross + apply */

    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 1);
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.group, 0);
    MOQ_TEST_CHECK_EQ_U64(pr.subgroup, 3);
    MOQ_TEST_CHECK_EQ_U64(pr.object, 0);
    MOQ_TEST_CHECK_EQ_U64(pr.prio, 77);
    MOQ_TEST_CHECK(pr.eog);
    MOQ_TEST_CHECK_EQ_U64(pr.len, 24);
    MOQ_TEST_CHECK_EQ_U64(pr.first, 0xA7);
    MOQ_TEST_CHECK_EQ_U64(pr.prop_len, 8);
    MOQ_TEST_CHECK_EQ_U64(pr.prop_first, 0xB1);

    /* Status and datagram shapes cross the same path. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 1, 0, 0, 0xC2, 16, 0, 0, 0, true,
                           false) == MOQR_OK);   /* datagram */
    step_n(s, 4);
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK(pr.datagram);
    MOQ_TEST_CHECK_EQ_U64(pr.first, 0xC2);
    uint64_t turns = 0, msgs = 0, bytes = 0;
    moqr_shards_debug_pump_counters(s, 0, &turns, &msgs, &bytes);
    MOQ_TEST_CHECK_EQ_U64(msgs, 2);
    MOQ_TEST_CHECK_EQ_U64(bytes, 24 + 8 + 16);
    MOQ_TEST_CHECK(turns >= 2);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_bytes(s, 0, 1), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_obj_delivery");
    return failures;
}

/* Local-vs-remote parity: the same publication feeds an owner-local
 * subscriber and a remote requester's subscriber; both must observe the
 * identical delivery sequence (identity, shape flags, payload bytes). */
static int
test_shards_data_parity(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtP") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtP", &ot) == MOQR_OK);
    moqr_binding_t l0 = sub_open(s, 0, 3);   /* owner-local subscriber */
    moqr_sub_t lsub;
    MOQ_TEST_CHECK(do_subscribe(s, 0, l0, "dtP", 9, &lsub) == MOQR_OK);
    moqr_binding_t d1 = sub_open(s, 1, 2);   /* remote subscriber */
    moqr_sub_t sub;
    step_n(s, 8);
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtP", 7, &sub) == MOQR_OK);
    step_n(s, 5);

    /* A mixed shape sequence: normal + props, status object, datagram. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x11, 20, 0x22, 4, 0,
                           false, false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 1, 0, 0, 0, 0, 3 /* status */,
                           false, false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 1, 0, 0, 0x33, 12, 0, 0, 0, true,
                           false) == MOQR_OK);
    step_n(s, 8);

    for (int n = 0; n < 3; n++) {
        dl_probe_t lp, rp;
        MOQ_TEST_CHECK(pull_one(s, 0, l0, &lp) == MOQR_OK);
        MOQ_TEST_CHECK(pull_one(s, 1, d1, &rp) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(rp.group, lp.group);
        MOQ_TEST_CHECK_EQ_U64(rp.subgroup, lp.subgroup);
        MOQ_TEST_CHECK_EQ_U64(rp.object, lp.object);
        MOQ_TEST_CHECK_EQ_U64(rp.status, lp.status);
        MOQ_TEST_CHECK_EQ_U64(rp.datagram, lp.datagram);
        MOQ_TEST_CHECK_EQ_U64(rp.len, lp.len);
        MOQ_TEST_CHECK_EQ_U64(rp.first, lp.first);
    }
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_parity");
    return failures;
}

/* ACK-before-object: the object enqueues in the SAME step as the ACK,
 * strictly behind it in the one FIFO, and the requester applies both in
 * order in one drain — never data into a not-yet-acked demand. */
static int
test_shards_data_ack_order(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtO") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtO", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtO", 7, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* DEMAND */
    /* Owner-only steps: the ACK enqueues, then an object, WITHOUT the
     * requester draining between — both sit in the ONE FIFO in order. */
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) == MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x44, 16, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) == MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1),
                          2);   /* [ACK, OBJ] in one FIFO */
    /* One requester drain applies both, strictly in order. */
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, NULL) == MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1),
                          0);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 1);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_ack_order");
    return failures;
}

/* A 1-entry channel: ACK then objects cross one at a time — lag, never loss,
 * never duplication (exact count and last-payload identity). */
static int
test_shards_data_channel_lag(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 1);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtL") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtL", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtL", 7, &sub) == MOQR_OK);
    step_n(s, 8);   /* ACKED over the 1-slot ring */
    for (uint64_t o = 0; o < 4; o++) {
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, o, (uint8_t)(0x50 + o), 16,
                               0, 0, 0, false, false) == MOQR_OK);
    }
    step_n(s, 24);   /* one slot: each hop takes its own round */
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 4);   /* all, exactly once */
    dl_probe_t pr;
    for (uint64_t o = 0; o < 4; o++) {
        MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(pr.object, o);
        MOQ_TEST_CHECK_EQ_U64(pr.first, 0x50 + o);
    }
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_channel_lag");
    return failures;
}

/* Byte-cap config: the default resolves to the per-track log byte budget;
 * an explicit smaller value is a create-time INVAL; an explicit value at or
 * above the resolved bound is accepted. */
static int
test_shards_data_byte_cap_cfg(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);   /* log_budget.max_bytes = 1 << 20 */
    cfg.admit_remote_demand = true;
    cfg.demand_channel_bytes = 1024;   /* below the resolved log budget */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(s == NULL);
    cfg.demand_channel_bytes = 1u << 20;   /* exactly the resolved bound */
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_shards_destroy(s);
    cfg.demand_channel_bytes = 0;   /* default = resolved bound */
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_byte_cap_cfg");
    return failures;
}

/* The queued message owns an independent clone: evicting the owner's record
 * after the enqueue (before the requester drains) must not corrupt the
 * crossing bytes. */
static int
test_shards_data_clone_survives_evict(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_budget.max_groups = 1;   /* next group evicts the last */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtE") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtE", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtE", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x5E, 32, 0, 0, 0, false,
                           false) == MOQR_OK);
    /* Owner steps alone: the OBJ enqueues without the requester draining. */
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) == MOQR_OK);
    moqr_shards_debug_round_advance(s);
    /* Evict the source record while its clone sits queued, then drain ONLY
     * the requester: eviction now propagates (GRP_EVICT + the next group's
     * record follow in the FIFO), so the clone's bytes are checked before
     * the requester's own budget evicts group 0 locally. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 9, 0, 0, 0x00, 32, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, NULL) == MOQR_OK);
    moqr_shards_debug_round_advance(s);
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.group, 0);
    MOQ_TEST_CHECK_EQ_U64(pr.len, 32);
    MOQ_TEST_CHECK_EQ_U64(pr.first, 0x5E);   /* intact clone bytes */
    step_n(s, 8);   /* the watermark and group 9 flow through */
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    if (pr.notice != 0) {   /* d1's own local watermark precedes the record */
        MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_U64(pr.group, 9);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_clone_survives_evict");
    return failures;
}

/* Destroy with data still queued releases every clone and canon exactly
 * once (the counting allocator balances to zero). */
static int
test_shards_data_destroy_queued(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtD") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtD", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtD", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x66, 40, 0x77, 8, 0,
                           false, false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_debug_demand_channel_bytes(s, 0, 1) > 0);
    moqr_shards_destroy(s);   /* clones + canons queued: all released once */
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_destroy_queued");
    return failures;
}

/* Producer clone OOM: the owner releases the outstanding delivery by
 * retiring the pump-sub (nothing stays pinned), then fail-stops — swept
 * across the payload and properties clone allocations. */
static int
test_shards_data_producer_oom(void)
{
    int failures = 0;
    for (long fail = 1; fail <= 2; fail++) {
        ca_t a;
        ca_init(&a);
        moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
        MOQ_TEST_CHECK(s != NULL);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "dtM") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtM", &ot) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtM", 7, &sub) == MOQR_OK);
        step_n(s, 4);   /* ACKED */
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x69, 32, 0x6A, 8, 0,
                               false, false) == MOQR_OK);
        /* The next owner step extracts and clones: fail the Nth clone
         * allocation (payload first, properties second). */
        a.fail_at = fail;
        moqr_result_t rc = moqr_shards_step(s, 1000);
        a.fail_at = 0;
        MOQ_TEST_CHECK(rc == MOQR_ERR_NOMEM);   /* fail-stop, promptly */
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("shards_data_producer_oom");
    return failures;
}

/* Requester-side allocation failure during apply: the step reports NOMEM in
 * bounded steps, the durable head is NOT popped, and nothing leaks. */
static int
test_shards_data_requester_oom(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtN") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtN", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtN", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED, no data yet */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x70, 32, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* OBJ queued */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1),
                          1);
    a.fail_all = true;
    moqr_result_t rc = moqr_shards_step(s, 1000);
    a.fail_all = false;
    MOQ_TEST_CHECK(rc == MOQR_ERR_NOMEM);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1),
                          1);   /* head still durable */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_requester_oom");
    return failures;
}

/* Turn budgets: with pump_turn_messages = 2, five ready objects cross two
 * per data phase, and the counters account every turn, message, and byte
 * exactly. */
static int
test_shards_data_turn_budget(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.pump_turn_messages = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtT") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtT", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtT", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    for (uint64_t o = 0; o < 5; o++) {
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, o, (uint8_t)o, 10, 0, 0,
                               0, false, false) == MOQR_OK);
    }
    uint64_t t0, m0, b0;
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    uint64_t t1, m1, b1;
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 2);   /* budget: two per turn */
    MOQ_TEST_CHECK_EQ_U64(b1 - b0, 20);
    MOQ_TEST_CHECK_EQ_U64(t1 - t0, 1);
    step_n(s, 4);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 5);   /* all cross over turns */
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 5);
    MOQ_TEST_CHECK_EQ_U64(b1 - b0, 50);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_turn_budget");
    return failures;
}

static moqr_result_t pub_obj_big(moqr_shards_t *s, ca_t *a, uint16_t i,
                                 moqr_track_t th, uint64_t g, uint64_t o,
                                 uint8_t fill, size_t len);

/* Turn-outcome classification: the four turns_* counters partition
 * pump_turns exactly, with the message-budget class taking precedence and
 * the drained class covering the final partial turn. Same workload as
 * shards_data_turn_budget (budget 2, five ready objects -> turns of
 * 2/2/1): two message-budget turns, one drained turn, three turns with
 * messages, zero byte-budget/blocked. */
static int
test_shards_turn_outcomes(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.pump_turn_messages = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "toA") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "toA", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "toA", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    moqr_shards_stats_t st0;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    for (uint64_t o = 0; o < 5; o++) {
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, o, (uint8_t)o, 10, 0, 0,
                               0, false, false) == MOQR_OK);
    }
    step_n(s, 6);   /* drain: turns of 2, 2, 1, then three idle turns */
    moqr_shards_stats_t st;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st.turns_msg_budget - st0.turns_msg_budget, 2);
    MOQ_TEST_CHECK_EQ_U64(st.turns_byte_budget - st0.turns_byte_budget, 0);
    MOQ_TEST_CHECK_EQ_U64(st.turns_blocked - st0.turns_blocked, 0);
    /* the 1-message final turn AND the three idle attempted turns after it
     * all classify DRAINED (budget remained, nothing pending) */
    MOQ_TEST_CHECK_EQ_U64(st.turns_drained - st0.turns_drained, 4);
    MOQ_TEST_CHECK_EQ_U64(st.turns_with_messages - st0.turns_with_messages,
                          3);
    /* the LIFETIME partition identity, not just the window delta */
    MOQ_TEST_CHECK_EQ_U64(st.turns_msg_budget + st.turns_byte_budget +
                              st.turns_blocked + st.turns_drained,
                          st.pump_turns);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_turn_outcomes");
    return failures;
}

/* Precedence tie: a ONE-message budget spent on an oversize-first delivery
 * exhausts the message and byte budgets in the same spend — the
 * pre-registered precedence classifies it MSG_BUDGET, never data-dependent.
 * The counter-arm with the message budget OPEN (default) and the same
 * oversize objects classifies BYTE_BUDGET for each soft-first turn. */
static int
test_shards_turn_outcome_precedence(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    {
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 2, 0);
        cfg.admit_remote_demand = true;
        cfg.pump_turn_messages = 1;
        cfg.pump_turn_bytes = 8;
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "toB") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "toB", &ot) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "toB", 7, &sub) == MOQR_OK);
        step_n(s, 4);
        moqr_shards_stats_t st0;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x7B, 64, 0, 0, 0,
                               false, false) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
        moqr_shards_stats_t st;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(st.turns_msg_budget - st0.turns_msg_budget, 1);
        MOQ_TEST_CHECK_EQ_U64(st.turns_byte_budget - st0.turns_byte_budget,
                              0);
        step_n(s, 4);
        moqr_shards_destroy(s);
    }
    {
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 2, 0);
        cfg.admit_remote_demand = true;
        cfg.pump_turn_bytes = 8;
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "toC") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "toC", &ot) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "toC", 7, &sub) == MOQR_OK);
        step_n(s, 4);
        moqr_shards_stats_t st0;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x7C, 64, 0, 0, 0,
                               false, false) == MOQR_OK);
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 1, 0x7D, 64, 0, 0, 0,
                               false, false) == MOQR_OK);
        step_n(s, 4);   /* two soft-first turns */
        moqr_shards_stats_t st;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(st.turns_byte_budget - st0.turns_byte_budget,
                              2);
        MOQ_TEST_CHECK_EQ_U64(st.turns_msg_budget - st0.turns_msg_budget, 0);
        moqr_shards_destroy(s);
    }
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_turn_outcome_precedence");
    return failures;
}

/* Blocked classification + the per-pair refusal SPLIT: a 1-entry channel
 * refuses on ENTRIES (byte gate open) and classifies the turn BLOCKED;
 * the byte-gate workload refuses on BYTES (entry cap open). One increment
 * per refusal event, predicate order = admission order (entries first). */
static int
test_shards_pair_refusal_split(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    {
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 2, 0);
        cfg.admit_remote_demand = true;
        cfg.demand_channel_entries = 1;
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "prA") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "prA", &ot) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "prA", 7, &sub) == MOQR_OK);
        step_n(s, 4);
        moqr_shards_stats_t st0;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
        for (uint64_t o = 0; o < 3; o++) {
            MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, o, (uint8_t)o, 10, 0,
                                   0, 0, false, false) == MOQR_OK);
        }
        moqr_shards_pair_stats_t pre;
        MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, 0, 1, &pre,
                                                  sizeof(pre)) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) ==
                       MOQR_OK);
        moqr_shards_pair_stats_t pr;
        MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, 0, 1, &pr, sizeof(pr)) == MOQR_OK);
        /* EXACTLY one refusal event for the one refusing step: the inner
         * loop breaks on the first refused push, so a double-count here is
         * a counting bug, not a workload artifact */
        MOQ_TEST_CHECK_EQ_U64(pr.refused_entries - pre.refused_entries, 1);
        MOQ_TEST_CHECK_EQ_U64(pr.refused_bytes, 0);
        moqr_shards_stats_t st;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
        MOQ_TEST_CHECK(st.turns_blocked - st0.turns_blocked >= 1);
        moqr_shards_debug_round_advance(s);
        step_n(s, 12);   /* drain the rest through the 1-slot channel */
        moqr_shards_destroy(s);
    }
    {
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 2, 0);
        cfg.admit_remote_demand = true;
        cfg.pump_turn_bytes = 8u << 20;
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "prB") == MOQR_OK);
        MOQ_TEST_CHECK(ann(s, 0, p0, "prC") == MOQR_OK);
        moqr_track_t ot, oh;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "prB", &ot) == MOQR_OK);
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "prC", &oh) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub, sub2;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "prB", 7, &sub) == MOQR_OK);
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "prC", 8, &sub2) == MOQR_OK);
        step_n(s, 4);
        MOQ_TEST_CHECK(pub_obj_big(s, &a, 0, ot, 0, 0, 0x93,
                                   600u * 1024u) == MOQR_OK);
        MOQ_TEST_CHECK(pub_obj_big(s, &a, 0, oh, 0, 0, 0x94,
                                   600u * 1024u) == MOQR_OK);
        moqr_shards_pair_stats_t pre2;
        MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, 0, 1, &pre2,
                                                  sizeof(pre2)) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) ==
                       MOQR_OK);
        moqr_shards_pair_stats_t pr;
        MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, 0, 1, &pr, sizeof(pr)) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(pr.refused_bytes - pre2.refused_bytes, 1);
        MOQ_TEST_CHECK_EQ_U64(pr.refused_entries, 0);
        moqr_shards_debug_round_advance(s);
        step_n(s, 8);
        moqr_shards_destroy(s);
    }
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_pair_refusal_split");
    return failures;
}

/* Per-pair identities close against the per-shard aggregates in the SAME
 * currency: summed over destinations, pair data messages/bytes equal the
 * producer's pump_messages/pump_bytes, and pair control messages equal the
 * control kinds of its enqueued[] vector — for every shard, owner and
 * requesters alike (K = 3 fan-out). Plus the accessor's refusal arms. */
static int
test_shards_pair_identities(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 3, 0);
    cfg.admit_remote_demand = true;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "piA") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "piA", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_binding_t d2 = sub_open(s, 2, 3);
    moqr_sub_t sub1, sub2;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "piA", 7, &sub1) == MOQR_OK);
    MOQ_TEST_CHECK(do_subscribe(s, 2, d2, "piA", 8, &sub2) == MOQR_OK);
    step_n(s, 6);   /* both ACKED */
    for (uint64_t o = 0; o < 4; o++) {
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, o, (uint8_t)o, 16, 0, 0,
                               0, false, false) == MOQR_OK);
    }
    step_n(s, 10);   /* full drain */
    /* Make EVERY control kind nonzero before checking the identities:
     * requester 1's clean unsubscribe enqueues an UNDEMAND; the owner's
     * force-withdraw sends requester 2 a DONE. Without these arms an
     * implementation that dropped terminal control kinds from the pair
     * counter would still satisfy the identity. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub1,
                                         1000) == MOQR_OK);
    for (int r = 0; r < 10; r++) {
        (void)moqr_shards_step(s, 100000);   /* past the linger deadline */
    }
    {
        moq_bytes_t nsp[1] = { { (const uint8_t *)"piA", 3 } };
        moqr_ns_t ns = { nsp, 1 };
        MOQ_TEST_CHECK(moqr_core_force_withdraw(moqr_shards_core(s, 0), ns,
                                                0x42, 1000) == MOQR_OK);
    }
    step_n(s, 8);
    {
        moqr_shards_stats_t s0, s1;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &s0) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &s1) == MOQR_OK);
        MOQ_TEST_CHECK(s0.enqueued[MOQR_SHARDS_MSG_ACK] >= 1);
        MOQ_TEST_CHECK(s0.enqueued[MOQR_SHARDS_MSG_DONE] >= 1);
        MOQ_TEST_CHECK(s1.enqueued[MOQR_SHARDS_MSG_DEMAND] >= 1);
        MOQ_TEST_CHECK(s1.enqueued[MOQR_SHARDS_MSG_UNDEMAND] >= 1);
    }
    for (uint16_t src = 0; src < 3; src++) {
        moqr_shards_stats_t st;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, src, &st) == MOQR_OK);
        uint64_t dm = 0, db = 0, cm = 0;
        for (uint16_t dst = 0; dst < 3; dst++) {
            if (dst == src) {
                continue;
            }
            moqr_shards_pair_stats_t pr;
            MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, src, dst, &pr,
                                                      sizeof(pr)) == MOQR_OK);
            dm += pr.data_messages;
            db += pr.data_bytes;
            cm += pr.control_messages;
        }
        MOQ_TEST_CHECK_EQ_U64(dm, st.pump_messages);
        MOQ_TEST_CHECK_EQ_U64(db, st.pump_bytes);
        MOQ_TEST_CHECK_EQ_U64(cm, st.enqueued[MOQR_SHARDS_MSG_DEMAND] +
                                      st.enqueued[MOQR_SHARDS_MSG_UNDEMAND] +
                                      st.enqueued[MOQR_SHARDS_MSG_DONE] +
                                      st.enqueued[MOQR_SHARDS_MSG_ACK]);
    }
    /* accessor refusal arms: NULL out, self pair, out-of-range index */
    moqr_shards_pair_stats_t pr;
    MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, 0, 1, NULL, sizeof(pr)) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, 1, 1, &pr, sizeof(pr)) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, 3, 0, &pr, sizeof(pr)) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_shards_get_pair_stats(s, 0, 3, &pr, sizeof(pr)) ==
                   MOQR_ERR_INVAL);
    /* sized-output discipline: below the v0 floor refused; the exact floor
     * accepted (prefix copy) */
    MOQ_TEST_CHECK(moqr_shards_get_pair_stats(
                       s, 0, 1, &pr, MOQR_SHARDS_PAIR_STATS_V0_SIZE - 1) ==
                   MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_shards_get_pair_stats(
                       s, 0, 1, &pr, MOQR_SHARDS_PAIR_STATS_V0_SIZE) ==
                   MOQR_OK);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_pair_identities");
    return failures;
}

/* Soft first message: one legal object larger than pump_turn_bytes crosses
 * as the turn's first message (a legal record is never permanently
 * unsendable), and spending that exception ends the turn OUTRIGHT — even a
 * zero-byte status object, which no bytes-remaining arithmetic would ever
 * stop, must wait for the next turn. */
static int
test_shards_data_oversize_first(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.pump_turn_bytes = 8;   /* smaller than any test payload */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtF") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtF", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtF", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x7A, 64, 0, 0, 0, false,
                           false) == MOQR_OK);
    /* A ZERO-BYTE status object: only an explicit end-of-turn can hold it. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 1, 0, 0, 0, 0, 1 /* status */,
                           false, false) == MOQR_OK);
    uint64_t t0, m0, b0;
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    uint64_t t1, m1, b1;
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 1);   /* the oversize FIRST crossed... */
    MOQ_TEST_CHECK_EQ_U64(b1 - b0, 64);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 2);   /* ...and the second the next turn */
    step_n(s, 3);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 2);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_oversize_first");
    return failures;
}

/* Sticky arbitration on a 1-entry channel: sustained fresh control (a new
 * demand ACKing every window) cannot starve an older demand's data — the
 * token transfers only on a successful preferred enqueue — and a DATA
 * token with no eligible data never blocks control. */
static int
test_shards_data_arbiter(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 1);   /* 1-entry channels */
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "arA") == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "arB") == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "arC") == MOQR_OK);
    moqr_track_t ot, otb, otc;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "arA", &ot) == MOQR_OK);
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "arB", &otb) == MOQR_OK);
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "arC", &otc) == MOQR_OK);
    (void)otb;
    (void)otc;
    step_n(s, 10);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "arA", 7, &sub) == MOQR_OK);
    step_n(s, 8);   /* ACKED over the 1-slot ring */
    /* Backlog of data plus a stream of fresh control demands. */
    for (uint64_t o = 0; o < 3; o++) {
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, o, (uint8_t)(0x80 + o),
                               16, 0, 0, 0, false, false) == MOQR_OK);
    }
    moqr_sub_t sb, sc;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "arB", 8, &sb) == MOQR_OK);
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "arC", 9, &sc) == MOQR_OK);
    step_n(s, 40);
    /* Both classes completed: every fresh demand ACKed (control progressed)
     * AND every object crossed (data progressed). */
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 3);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 3);
    /* Token-idle: with all data drained (DATA may hold the token), fresh
     * control still flows. */
    moqr_sub_t s2;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "arA", 10, &s2) == MOQR_OK);
    step_n(s, 6);
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 4);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_arbiter");
    return failures;
}

/* Ingest one LARGE object (heap-staged payload) into an owned track. */
static moqr_result_t
pub_obj_big(moqr_shards_t *s, ca_t *a, uint16_t i, moqr_track_t th,
            uint64_t g, uint64_t o, uint8_t fill, size_t len)
{
    uint8_t *buf = malloc(len);
    if (buf == NULL) {
        return MOQR_ERR_NOMEM;
    }
    memset(buf, fill, len);
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.object_id = o;
    d.publisher_priority = 77;
    d.now_us = 1;
    moqr_result_t rc = moq_rcbuf_create(&a->vt, buf, len, &d.payload) == 0
                           ? moqr_core_ingest(moqr_shards_core(s, i), th, &d)
                           : MOQR_ERR_NOMEM;
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(d.payload);
    }
    free(buf);
    return rc;
}

/* Open a chunk-through record on an owned track. */
static moqr_result_t
pub_open_rec(moqr_shards_t *s, ca_t *a, uint16_t i, moqr_track_t th,
             uint64_t g, uint64_t sg, uint64_t o, uint64_t declared, bool eog,
             uint8_t prop_fill, size_t prop_len)
{
    moqr_log_append_desc_t d;
    moqr_log_append_desc_init(&d);
    d.group_id = g;
    d.subgroup_id = sg;
    d.object_id = o;
    d.publisher_priority = 77;
    d.end_of_group = eog;
    d.obj_state = MOQR_OBJ_OPEN;
    d.declared_len = declared;
    d.now_us = 1;
    if (prop_len != 0) {
        uint8_t buf[64];
        memset(buf, prop_fill, prop_len);
        if (moq_rcbuf_create(&a->vt, buf, prop_len, &d.properties) != 0) {
            return MOQR_ERR_NOMEM;
        }
    }
    moqr_result_t rc = moqr_core_ingest(moqr_shards_core(s, i), th, &d);
    if (rc != MOQR_OK) {
        moq_rcbuf_decref(d.properties);
    }
    return rc;
}

static moqr_result_t
pub_chunk(moqr_shards_t *s, ca_t *a, uint16_t i, moqr_track_t th, uint64_t g,
          uint64_t sg, uint64_t o, uint8_t fill, size_t len)
{
    uint8_t buf[64];
    memset(buf, fill, len);
    moq_rcbuf_t *cb = NULL;
    if (moq_rcbuf_create(&a->vt, buf, len, &cb) != 0) {
        return MOQR_ERR_NOMEM;
    }
    moqr_result_t rc =
        moqr_core_append_chunk(moqr_shards_core(s, i), th, g, sg, o, cb);
    moq_rcbuf_decref(cb);   /* the log increfs on success */
    return rc;
}

static moqr_result_t
pub_complete(moqr_shards_t *s, uint16_t i, moqr_track_t th, uint64_t g,
             uint64_t sg, uint64_t o)
{
    return moqr_core_complete_record(moqr_shards_core(s, i), th, g, sg, o);
}

/* The channel BYTE gate: two objects that fit the entry cap but not the
 * byte cap together cross one at a time — lag, never loss — and the byte
 * gauge tracks queued logical bytes exactly. */
static int
test_shards_data_byte_gate(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);   /* log max_bytes = 1 << 20 */
    cfg.admit_remote_demand = true;
    /* A turn budget far above the channel cap, so the CHANNEL byte gate —
     * not the per-turn budget — is what holds the second object. */
    cfg.pump_turn_bytes = 8u << 20;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtG") == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtH") == MOQR_OK);
    moqr_track_t ot, oh;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtG", &ot) == MOQR_OK);
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtH", &oh) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub, sub2;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtG", 7, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtH", 8, &sub2) == MOQR_OK);
    step_n(s, 4);   /* both ACKED */
    /* 600 KiB on each TRACK (each within its own log budget): the entry cap
     * would admit both, the 1 MiB channel byte cap only one at a time. */
    MOQ_TEST_CHECK(pub_obj_big(s, &a, 0, ot, 0, 0, 0x91,
                               600u * 1024u) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj_big(s, &a, 0, oh, 0, 0, 0x92,
                               600u * 1024u) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1),
                          1);   /* byte-gated: only one queued */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_bytes(s, 0, 1),
                          600u * 1024u);
    moqr_shards_debug_round_advance(s);
    step_n(s, 8);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 2);   /* both, exactly once */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_bytes(s, 0, 1), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_byte_gate");
    return failures;
}

/* A requester-only capacity edge terminates the demand loss-visibly through
 * the staged path: the requester's group is pre-filled to its per-group
 * record cap (directly, so the OWNER's mirrored budget never refuses), the
 * next remote object hits CAPACITY, and the demand resolves with INTERNAL
 * 0x0 — subscribers terminated, the owner torn down by the cancel notice,
 * the head consumed as moot, and the capacity metric counting it. */
static int
test_shards_data_capacity_terminal(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_max_objects_per_group = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtC") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtC", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtC", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    /* Pre-fill the REQUESTER's group 0 to the record cap through the demand
     * track itself (a purely local imbalance the owner cannot see). */
    uint64_t tgen = 0;
    moqr_track_t rt = moqr_shards_debug_pending_demand_track(s, 1, 0, &tgen);
    MOQ_TEST_CHECK(pub_obj(s, &a, 1, rt, 0, 9, 5, 0x21, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 1, rt, 0, 9, 6, 0x22, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    /* The remote object for group 0 now has no room at the requester. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x23, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    step_n(s, 10);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_shards_debug_remote_demand_term_capacity(s, 1), 1);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 0);   /* terminated, loss-visible */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_capacity_terminal");
    return failures;
}

/* A late publisher FIN crosses as SG_SEAL: the requester's subgroup seals
 * (its last record now reports subgroup_end downstream) and the data queued
 * behind the notice keeps flowing — the pump no longer lags on it. */
static int
test_shards_data_unsupported_hold(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dtU") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dtU", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dtU", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    /* One object crosses; the pump-sub then drains its subgroup. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x31, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    step_n(s, 4);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 1);
    /* A late seal mints a recordless SEAL notice for the drained pump-sub;
     * it crosses as SG_SEAL and the next group's data follows behind it. */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(moqr_shards_core(s, 0), ot, 0,
                                           0) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 1, 0, 0, 0x32, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    step_n(s, 6);
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 2);   /* nothing lags */
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.group, 0);
    MOQ_TEST_CHECK(pr.sg_end);   /* the crossed seal closed the subgroup */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_seal_crosses");
    return failures;
}

/* K == 1 and admission-off runtimes never touch the data machinery: the
 * pump counters and channel gauges stay zero. */
static int
test_shards_data_inert(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);   /* admission OFF */
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "diA") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "diA", &ot) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x1F, 16, 0, 0, 0, false,
                           false) == MOQR_OK);
    step_n(s, 6);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "diA", 7, &sub) == MOQR_OK);
    step_n(s, 6);
    uint64_t t = 0, m = 0, b = 0;
    moqr_shards_debug_pump_counters(s, 0, &t, &m, &b);
    MOQ_TEST_CHECK_EQ_U64(t + m + b, 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_bytes(s, 0, 1), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_data_inert");
    return failures;
}

/* -- capacity model: resolver parity, guards, ceiling oracle ---------------- */

/* Fill a cfg tail with poison beyond a prefix-sized struct_size: resolve and
 * create must read only the prefix (R8C poisoned-tail discipline). */
static int
test_shards_cap_poisoned_prefix(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    /* A prefix that reaches only {struct_size, alloc, shards}: everything
     * after is poison and must not be read. */
    moqr_shards_cfg_t cfg;
    memset(&cfg, 0xA5, sizeof(cfg));
    cfg.struct_size = offsetof(moqr_shards_cfg_t, shards) +
                      sizeof(cfg.shards);
    cfg.alloc = &a.vt;
    cfg.shards = 2;
    moqr_shards_limits_t lim;
    MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&cfg, &lim) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(lim.shards, 2);
    MOQ_TEST_CHECK(!lim.admit);   /* poison beyond the prefix ignored */
    moqr_shards_capacity_t cap;
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&cfg, &cap) == MOQR_OK);
    MOQ_TEST_CHECK(cap.relay_alloc_ceiling > 0);
    /* Create/describe parity on the same poisoned prefix. */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    MOQ_TEST_CHECK(s != NULL);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    /* BIND resolver prefix parity: poisoned tail beyond a prefix reaching
     * max_conns resolves identically to the clean prefix. */
    {
        moqr_core_relay_cfg_t ccfg;
        moqr_core_relay_cfg_init_sized(&ccfg, sizeof(ccfg), &a.vt);
        moqr_core_limits_t clim;
        MOQ_TEST_CHECK(moqr_core_limits_resolve(&ccfg, &clim) == MOQR_OK);
        moqr_bind_cfg_t pb, cb2;
        memset(&pb, 0xA5, sizeof(pb));
        memset(&cb2, 0, sizeof(cb2));
        uint32_t prefix = (uint32_t)(offsetof(moqr_bind_cfg_t, max_conns) +
                                     sizeof(pb.max_conns));
        pb.struct_size = prefix;
        pb.alloc = &a.vt;
        pb.core = NULL;
        pb.max_conns = 5;
        cb2.struct_size = prefix;
        cb2.alloc = &a.vt;
        cb2.max_conns = 5;
        moqr_bind_limits_t bp, bc;
        MOQ_TEST_CHECK(moqr_bind_cfg_resolve(&pb, &clim, &bp) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_bind_cfg_resolve(&cb2, &clim, &bc) == MOQR_OK);
        MOQ_TEST_CHECK(memcmp(&bp, &bc, sizeof(bp)) == 0);
        MOQ_TEST_CHECK_EQ_U64(bp.max_conns, 5);
        /* Isolated derived-count rejection: a deferral ring the 32-bit
         * field cannot hold refuses INVAL (no byte term involved). */
        moqr_core_limits_t huge = clim;
        huge.max_subs = UINT32_MAX;
        huge.max_tracks = UINT32_MAX;
        huge.max_intents = UINT32_MAX;
        moqr_bind_limits_t bo;
        MOQ_TEST_CHECK(moqr_bind_cfg_resolve(&cb2, &huge, &bo) ==
                       MOQR_ERR_INVAL);
    }
    MOQ_TEST_PASS("shards_cap_poisoned_prefix");
    return failures;
}

/* The K>1 binding-budget guard and the usable-bindings rule, at every ruled
 * boundary. */
static int
test_shards_cap_binding_guard(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    /* K>1 with max_bindings == K: refused before any allocation. */
    {
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 3, 0);
        cfg.core_cfg.max_bindings = 3;
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(s == NULL);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
        moqr_shards_limits_t lim;
        MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&cfg, &lim) ==
                       MOQR_ERR_INVAL);
    }
    /* K>1 with max_bindings == K+1: creates; exactly one external slot. */
    {
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 3, 0);
        cfg.core_cfg.max_bindings = 4;
        moqr_shards_limits_t lim;
        MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&cfg, &lim) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(lim.usable_bindings, 1);
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        /* One external binding fits; a second is refused by the core. */
        moqr_binding_t b1, b2;
        MOQ_TEST_CHECK(moqr_core_binding_open(moqr_shards_core(s, 0), 1,
                                              &b1) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_binding_open(moqr_shards_core(s, 0), 2,
                                              &b2) != MOQR_OK);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    /* K=1 reserves nothing: max_bindings == 1 stays valid, usable = 1. */
    {
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 1, 0);
        cfg.core_cfg.max_bindings = 1;
        moqr_shards_limits_t lim;
        MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&cfg, &lim) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(lim.usable_bindings, 1);
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    /* K=1 with an explicitly SMALL bind max_conns: the min wins. */
    {
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 1, 0);
        cfg.core_cfg.max_bindings = 8;
        cfg.bind_cfg.max_conns = 3;
        moqr_shards_limits_t lim;
        MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&cfg, &lim) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(lim.usable_bindings, 3);
    }
    MOQ_TEST_PASS("shards_cap_binding_guard");
    return failures;
}

/* K=1 admission is structurally inert in the MODEL exactly as in the
 * runtime: describe shows zero cross-shard terms and identical totals with
 * admission on or off, and create allocates identical bytes. */
static int
test_shards_cap_k1_admission_inert(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t on, off;
    shards_cfg(&on, &a, 1, 0);
    on.admit_remote_demand = true;
    shards_cfg(&off, &a, 1, 0);
    moqr_shards_capacity_t con, coff;
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&on, &con) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&off, &coff) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(con.channel_byte_ceiling, 0);
    MOQ_TEST_CHECK_EQ_U64(con.canon_byte_ceiling, 0);
    MOQ_TEST_CHECK_EQ_U64(con.staging_byte_ceiling, 0);
    MOQ_TEST_CHECK_EQ_U64(con.relay_alloc_ceiling, coff.relay_alloc_ceiling);
    long live_on, live_off;
    {
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&on, &s) == MOQR_OK);
        live_on = a.live;
        moqr_shards_destroy(s);
    }
    {
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&off, &s) == MOQR_OK);
        live_off = a.live;
        moqr_shards_destroy(s);
    }
    MOQ_TEST_CHECK_EQ_INT((int)live_on, (int)live_off);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_cap_k1_admission_inert");
    return failures;
}

/* The admission delta is EXACTLY the progress tables + row bitmap at K>1 —
 * the structural coupling proof, immune to slack in other terms. */
static int
test_shards_cap_admission_delta(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t on, off;
    shards_cfg(&on, &a, 2, 0);
    on.admit_remote_demand = true;
    shards_cfg(&off, &a, 2, 0);
    moqr_shards_capacity_t con, coff;
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&on, &con) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&off, &coff) == MOQR_OK);
    /* The delta lives ONLY in the structure term (channels/canon/staging
     * are admission-independent), and it scales exactly with K — pinning
     * it as K x (progress tables + row bitmap), with no other term able to
     * absorb it. */
    MOQ_TEST_CHECK_EQ_U64(con.channel_byte_ceiling, coff.channel_byte_ceiling);
    MOQ_TEST_CHECK_EQ_U64(con.canon_byte_ceiling, coff.canon_byte_ceiling);
    MOQ_TEST_CHECK_EQ_U64(con.staging_byte_ceiling,
                          coff.staging_byte_ceiling);
    uint64_t d2 = con.shards_structure_bytes - coff.shards_structure_bytes;
    MOQ_TEST_CHECK(d2 > 0);
    MOQ_TEST_CHECK_EQ_U64(con.relay_alloc_ceiling - coff.relay_alloc_ceiling,
                          d2);
    moqr_shards_cfg_t on3, off3;
    shards_cfg(&on3, &a, 3, 0);
    on3.admit_remote_demand = true;
    shards_cfg(&off3, &a, 3, 0);
    moqr_shards_capacity_t con3, coff3;
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&on3, &con3) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&off3, &coff3) == MOQR_OK);
    uint64_t d3 = con3.shards_structure_bytes - coff3.shards_structure_bytes;
    MOQ_TEST_CHECK_EQ_U64(d2 % 2, 0);
    MOQ_TEST_CHECK_EQ_U64(d3 % 3, 0);
    MOQ_TEST_CHECK_EQ_U64(d2 / 2, d3 / 3);   /* exactly per-shard tables */
    MOQ_TEST_PASS("shards_cap_admission_delta");
    return failures;
}

/* The peak oracle: a K=2 admitted runtime driven through control churn and
 * full data channels never requests more than the described ceiling; a K=1
 * runtime holds under its (cross-shard-free) ceiling. peak is tracked
 * continuously by the allocator, so transient staging is covered. */
static int
test_shards_cap_peak_oracle(void)
{
    int failures = 0;
    {
        ca_t a;
        ca_init(&a);
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 2, 0);
        cfg.admit_remote_demand = true;
        cfg.demand_channel_entries = 2;
        moqr_shards_capacity_t cap;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&cfg, &cap) == MOQR_OK);
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "cpO") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "cpO", &ot) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "cpO", 7, &sub) == MOQR_OK);
        step_n(s, 6);
        /* Whole + chunked data, owner-only steps so the channels FILL. */
        for (uint64_t g = 0; g < 4; g++) {
            MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, g, 0, 0, (uint8_t)g, 64, 0,
                                   0, 0, false, false) == MOQR_OK);
        }
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 4, 0, 0, 32, false, 0x11,
                                    8) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 4, 0, 0, 0x12, 16) ==
                       MOQR_OK);
        for (int r = 0; r < 3; r++) {
            MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) ==
                           MOQR_OK);
            moqr_shards_debug_round_advance(s);
        }
        step_n(s, 12);   /* drain + settle */
        MOQ_TEST_CHECK((uint64_t)a.peak <= cap.relay_alloc_ceiling);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    {
        /* Saturation arm: a full-track key at EXACTLY the 4096-byte shared
         * cap (32 parts: 31x128 + 127, + a 1-byte name) through mailbox,
         * journal, and demand channel; both demand-table entries claimed;
         * and the owner->requester channel holding DATA in one slot with a
         * second demand's control ACK co-resident in the other — with the
         * intermediate gauges asserted, not assumed. */
        ca_t a;
        ca_init(&a);
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 2, 0);
        cfg.admit_remote_demand = true;
        cfg.mailbox_entries = 2;
        cfg.journal_entries = 3;
        cfg.pending_demand_entries = 2;
        cfg.demand_channel_entries = 2;
        cfg.pump_subgroup_slots = 2;
        moqr_shards_capacity_t cap;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&cfg, &cap) == MOQR_OK);
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        moqr_binding_t p0 = pub_open(s, 0);
        static uint8_t big[32][128];
        moq_bytes_t parts[32];
        for (int i = 0; i < 32; i++) {
            memset(big[i], 'a' + (i % 26), sizeof(big[i]));
            big[i][0] = (uint8_t)i;
            parts[i] = (moq_bytes_t){ big[i], 128 };
        }
        parts[31].len = 127;   /* 31*128 + 127 = 4095; +1-byte name = 4096 */
        moqr_ns_t bigns = { parts, 32 };
        /* A second, announce-only namespace at EXACTLY the 4096-byte
         * NAMESPACE ceiling (32 x 128 with no name headroom needed). */
        static uint8_t big2[32][128];
        moq_bytes_t parts2[32];
        for (int i = 0; i < 32; i++) {
            memset(big2[i], 'A' + (i % 26), sizeof(big2[i]));
            big2[i][0] = (uint8_t)(64 + i);
            parts2[i] = (moq_bytes_t){ big2[i], 128 };
        }
        moqr_ns_t maxns = { parts2, 32 };
        /* Two distinct announces queued before any drain: the 2-entry
         * mailbox is FULL, holding one 4095-byte and one 4096-byte key. */
        MOQ_TEST_CHECK(moqr_core_announce(moqr_shards_core(s, 0), p0,
                                          bigns) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_core_announce(moqr_shards_core(s, 0), p0,
                                          maxns) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) ==
                       MOQR_OK);
        moqr_shards_debug_round_advance(s);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_mailbox_pending(s, 0, 1), 2);
        moq_bytes_t name = { (const uint8_t *)"v", 1 };
        moqr_track_t ot;
        MOQ_TEST_CHECK(moqr_core_publish_open(moqr_shards_core(s, 0), p0,
                                              bigns, name, 900,
                                              &ot) == MOQR_OK);
        MOQ_TEST_CHECK(ann(s, 0, p0, "satB") == MOQR_OK);
        moqr_track_t bt;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "satB", &bt) == MOQR_OK);
        step_n(s, 10);
        /* Journal occupancy: all THREE namespaces present — the 3-entry
         * journal is full, with keys at 4095 and 4096 bytes. */
        moqr_shards_jinfo_t j;
        moq_bytes_t sb = { (const uint8_t *)"satB", 4 };
        moqr_shards_debug_journal(s, 1, &sb, 1, &j);
        MOQ_TEST_CHECK(j.present);
        moqr_shards_debug_journal(s, 1, parts, 32, &j);
        MOQ_TEST_CHECK(j.present);
        moqr_shards_debug_journal(s, 1, parts2, 32, &j);
        MOQ_TEST_CHECK(j.present);
        /* Demand A (the max-size key) to ACKED. */
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_subscribe_req_t rq;
        moqr_subscribe_req_init(&rq);
        rq.ns = bigns;
        rq.name = name;
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 71;
        moqr_sub_t asub;
        MOQ_TEST_CHECK(moqr_core_subscribe(moqr_shards_core(s, 1), d1, &rq,
                                           &asub) == MOQR_OK);
        step_n(s, 8);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
        /* Demand B's DEMAND lands BEFORE the data extraction, so ONE owner
         * step pushes its control ACK (phase 5) and then the chunked data
         * (phase 6) into the SAME owner->requester channel — control and
         * data co-resident, the control entry riding exempt of the byte
         * gauge. */
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 16, false, 0x21,
                                    8) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x22, 16) ==
                       MOQR_OK);
        moqr_subscribe_req_init(&rq);
        moq_bytes_t pb2 = { (const uint8_t *)"satB", 4 };
        rq.ns = (moqr_ns_t){ &pb2, 1 };
        rq.name = name;
        rq.filter.type = MOQR_FILTER_ABSOLUTE_START;
        rq.cookie = 72;
        moqr_sub_t bsub;
        MOQ_TEST_CHECK(moqr_core_subscribe(moqr_shards_core(s, 1), d1, &rq,
                                           &bsub) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, NULL) ==
                       MOQR_OK);
        moqr_shards_debug_round_advance(s);
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) ==
                       MOQR_OK);
        moqr_shards_debug_round_advance(s);
        /* Co-resident: [ACK, OBJ_OPEN] in one 2-slot channel; both demand
         * table entries and the owner progress slot occupied; channel bytes
         * carry the data while the control entry rode past the gauge. */
        MOQ_TEST_CHECK_EQ_U64(
            moqr_shards_debug_demand_channel_pending(s, 0, 1), 2);
        MOQ_TEST_CHECK(moqr_shards_debug_demand_channel_bytes(s, 0, 1) > 0);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 2);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 2);
        MOQ_TEST_CHECK(moqr_shards_debug_owner_progress_slots(s, 0) >= 1);
        MOQ_TEST_CHECK((uint64_t)a.peak <= cap.relay_alloc_ceiling);
        MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
        step_n(s, 16);   /* drain, apply, settle */
        /* Fill the progress tables COMPLETELY: two demands x two subgroup
         * slots — chunked objects in a second subgroup on both tracks. */
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 1, 1, 16, false, 0,
                                    0) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 1, 1, 0x31, 16) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 1, 1) == MOQR_OK);
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, bt, 0, 0, 0, 16, false, 0,
                                    0) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, bt, 0, 0, 0, 0x32, 16) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(pub_complete(s, 0, bt, 0, 0, 0) == MOQR_OK);
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, bt, 0, 1, 1, 16, false, 0,
                                    0) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, bt, 0, 1, 1, 0x33, 16) ==
                       MOQR_OK);
        MOQ_TEST_CHECK(pub_complete(s, 0, bt, 0, 1, 1) == MOQR_OK);
        step_n(s, 24);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0),
                              4);
        MOQ_TEST_CHECK((uint64_t)a.peak <= cap.relay_alloc_ceiling);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    {
        ca_t a;
        ca_init(&a);
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 1, 0);
        moqr_shards_capacity_t cap;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&cfg, &cap) == MOQR_OK);
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "cpK") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "cpK", &ot) == MOQR_OK);
        for (uint64_t g = 0; g < 4; g++) {
            MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, g, 0, 0, (uint8_t)g, 64, 0,
                                   0, 0, false, false) == MOQR_OK);
        }
        step_n(s, 6);
        MOQ_TEST_CHECK((uint64_t)a.peak <= cap.relay_alloc_ceiling);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("shards_cap_peak_oracle");
    return failures;
}

/* Per-knob term proofs: each independently variable knob moves the model by
 * its exact closed form (public constants asserted numerically; private
 * struct sizes pinned via linearity, independence, and K-power scaling —
 * a dropped or mis-scoped term cannot hide behind another). 4228/4232 are
 * the contract canon ceilings, static-asserted inside the library. */
static int
test_shards_cap_term_deltas(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    size_t hdr = 0;
    MOQ_TEST_CHECK(moq_rcbuf_allocation_size(0, &hdr) == MOQ_OK);
    moqr_shards_cfg_t base;
    shards_cfg(&base, &a, 2, 0);
    base.admit_remote_demand = true;
    moqr_shards_capacity_t cb;
    MOQ_TEST_CHECK(moqr_shards_capacity_describe(&base, &cb) == MOQR_OK);
    MOQ_TEST_CHECK(cb.channel_byte_ceiling > 0);
    MOQ_TEST_CHECK(cb.canon_byte_ceiling > 0);
    MOQ_TEST_CHECK(cb.staging_byte_ceiling > 0);
    moqr_shards_limits_t lb;
    MOQ_TEST_CHECK(moqr_shards_cfg_resolve(&base, &lb) == MOQR_OK);

    /* mailbox_entries: canon ceiling moves by exactly K^2 x 4228 per entry;
     * the channel/staging terms do not move; structure moves linearly. */
    {
        moqr_shards_cfg_t c1 = base, c2 = base;
        c1.mailbox_entries = lb.mbox_cap + 1;
        c2.mailbox_entries = lb.mbox_cap + 2;
        moqr_shards_capacity_t d1, d2;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c2, &d2) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d1.canon_byte_ceiling - cb.canon_byte_ceiling,
                              4ull * 4228ull);
        MOQ_TEST_CHECK_EQ_U64(d1.channel_byte_ceiling,
                              cb.channel_byte_ceiling);
        uint64_t s1 = d1.shards_structure_bytes - cb.shards_structure_bytes;
        uint64_t s2 = d2.shards_structure_bytes - d1.shards_structure_bytes;
        MOQ_TEST_CHECK(s1 > 0);
        MOQ_TEST_CHECK_EQ_U64(s1, s2);   /* linear */
    }
    /* journal_entries: canon moves by K x 4228 (per-shard, not K^2). */
    {
        moqr_shards_cfg_t c1 = base;
        c1.journal_entries = lb.jrn_cap + 1;
        moqr_shards_capacity_t d1;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d1.canon_byte_ceiling - cb.canon_byte_ceiling,
                              2ull * 4228ull);
    }
    /* pending_demands: canon moves by K x 4232; structure grows (entry +
     * psub + progress row + bitmap byte), linearly. */
    {
        moqr_shards_cfg_t c1 = base, c2 = base;
        c1.pending_demand_entries = lb.pend_cap + 1;
        c2.pending_demand_entries = lb.pend_cap + 2;
        moqr_shards_capacity_t d1, d2;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c2, &d2) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d1.canon_byte_ceiling - cb.canon_byte_ceiling,
                              2ull * 4232ull);
        uint64_t s1 = d1.shards_structure_bytes - cb.shards_structure_bytes;
        uint64_t s2 = d2.shards_structure_bytes - d1.shards_structure_bytes;
        MOQ_TEST_CHECK(s1 > 0);
        MOQ_TEST_CHECK_EQ_U64(s1, s2);
    }
    /* demand_channel_entries: the channel ceiling moves by exactly
     * K^2 x (4232 + 2 x HDR) per slot; canon does not move. */
    {
        moqr_shards_cfg_t c1 = base;
        c1.demand_channel_entries = lb.dch_cap + 1;
        moqr_shards_capacity_t d1;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(
            d1.channel_byte_ceiling - cb.channel_byte_ceiling,
            4ull * (4232ull + 2ull * (uint64_t)hdr));
        MOQ_TEST_CHECK_EQ_U64(d1.canon_byte_ceiling, cb.canon_byte_ceiling);
    }
    /* demand_channel_bytes: +X moves the channel ceiling by exactly K^2 x X
     * and the staging term follows the RECORD budget, not the cap. */
    {
        moqr_shards_cfg_t c1 = base;
        c1.demand_channel_bytes = lb.dch_byte_cap + 4096;
        moqr_shards_capacity_t d1;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(
            d1.channel_byte_ceiling - cb.channel_byte_ceiling,
            4ull * 4096ull);
        MOQ_TEST_CHECK_EQ_U64(d1.staging_byte_ceiling,
                              cb.staging_byte_ceiling);
    }
    /* subgroup slots: structure-only, linear (admission tables). */
    {
        moqr_shards_cfg_t c1 = base, c2 = base;
        c1.pump_subgroup_slots = lb.sg_slots + 1;
        c2.pump_subgroup_slots = lb.sg_slots + 2;
        moqr_shards_capacity_t d1, d2;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c2, &d2) == MOQR_OK);
        uint64_t s1 = d1.shards_structure_bytes - cb.shards_structure_bytes;
        uint64_t s2 = d2.shards_structure_bytes - d1.shards_structure_bytes;
        MOQ_TEST_CHECK(s1 > 0);
        MOQ_TEST_CHECK_EQ_U64(s1, s2);
        MOQ_TEST_CHECK_EQ_U64(d1.canon_byte_ceiling, cb.canon_byte_ceiling);
    }
    /* trace ring: exactly the trace descriptor's own delta, per shard. */
    {
        moqr_shards_cfg_t c1 = base;
        c1.trace_ring_records = lb.trace_ring * 2;
        moqr_shards_capacity_t d1;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d1.trace_bytes,
                              moqr_trace_bytes(lb.trace_ring * 2));
    }
    /* Independence: bumping two knobs together equals the sum of their
     * individual deltas — no cross-term absorbs another. */
    {
        moqr_shards_cfg_t cm = base, cj = base, cmj = base;
        cm.mailbox_entries = lb.mbox_cap + 1;
        cj.journal_entries = lb.jrn_cap + 1;
        cmj.mailbox_entries = lb.mbox_cap + 1;
        cmj.journal_entries = lb.jrn_cap + 1;
        moqr_shards_capacity_t dm, dj, dmj;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&cm, &dm) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&cj, &dj) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&cmj, &dmj) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(
            dmj.relay_alloc_ceiling - cb.relay_alloc_ceiling,
            (dm.relay_alloc_ceiling - cb.relay_alloc_ceiling) +
                (dj.relay_alloc_ceiling - cb.relay_alloc_ceiling));
    }
    /* Overflow: a poisoned product refuses INVAL, never wraps — at the
     * shards layer, through a wrapped CORE component (the reviewer repro,
     * K=1 generic shard runtime), and through a wrapped BIND component. */
    {
        moqr_shards_cfg_t c1 = base;
        c1.demand_channel_bytes = UINT64_MAX;
        moqr_shards_capacity_t d1;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) ==
                       MOQR_ERR_INVAL);
    }
    {
        moqr_shards_cfg_t c1;
        shards_cfg(&c1, &a, 1, 0);
        c1.core_cfg.max_tracks = 1048576;
        c1.core_cfg.log_budget.max_groups = 1048576;
        c1.core_cfg.log_budget.max_bytes = (uint64_t)1 << 40;
        c1.core_cfg.log_max_subgroups = 1048576;
        c1.core_cfg.log_max_objects_per_group = 1048576;
        c1.core_cfg.log_max_cursors = 1048576;
        moqr_shards_capacity_t d1;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) ==
                       MOQR_ERR_INVAL);
        MOQ_TEST_CHECK_EQ_U64(d1.relay_alloc_ceiling, 0);
    }
    {
        moqr_shards_cfg_t c1 = base;
        c1.bind_cfg.max_conns = UINT32_MAX;
        c1.bind_cfg.max_announces = UINT32_MAX;
        c1.bind_cfg.max_downstream_subs = UINT32_MAX;
        moqr_shards_capacity_t d1;
        MOQ_TEST_CHECK(moqr_shards_capacity_describe(&c1, &d1) ==
                       MOQR_ERR_INVAL);
    }
    MOQ_TEST_PASS("shards_cap_term_deltas");
    return failures;
}

/* -- chunked + live-edge forwarding ----------------------------------------- */

/* A chunk-through object crosses as OBJ_OPEN / OBJ_CHUNK... / OBJ_END and
 * reassembles at the requester byte-for-byte, properties included. The
 * owner's subgroup progress slot survives the completed object. */
static int
test_shards_chunk_delivery(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckA") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckA", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckA", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 3, 0, 48, false, 0xB1, 8) ==
                   MOQR_OK);
    for (uint8_t c = 0; c < 3; c++) {
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 3, 0, 0xA7, 16) == MOQR_OK);
    }
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 3, 0) == MOQR_OK);
    step_n(s, 6);
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.group, 0);
    MOQ_TEST_CHECK_EQ_U64(pr.subgroup, 3);
    MOQ_TEST_CHECK_EQ_U64(pr.object, 0);
    MOQ_TEST_CHECK_EQ_U64(pr.prio, 77);
    MOQ_TEST_CHECK_EQ_U64(pr.len, 48);
    MOQ_TEST_CHECK_EQ_U64(pr.first, 0xA7);
    MOQ_TEST_CHECK_EQ_U64(pr.prop_len, 8);
    MOQ_TEST_CHECK_EQ_U64(pr.prop_first, 0xB1);
    uint64_t turns = 0, msgs = 0, bytes = 0;
    moqr_shards_debug_pump_counters(s, 0, &turns, &msgs, &bytes);
    MOQ_TEST_CHECK_EQ_U64(msgs, 5);   /* OPEN + 3 chunks + END, no repeats */
    /* The completed object keeps its (group, subgroup) slot alive; nothing
     * is open on the requester once END lands. */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_delivery");
    return failures;
}

/* Whole-object and chunk-through paths deliver identical bytes for the same
 * content on the same track. */
static int
test_shards_chunk_parity(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckP") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckP", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckP", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x5C, 48, 0, 0, 0, false,
                           false) == MOQR_OK);   /* whole */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 1, 48, false, 0, 0) ==
                   MOQR_OK);                     /* chunked, same bytes */
    for (uint8_t c = 0; c < 3; c++) {
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 1, 0x5C, 16) == MOQR_OK);
    }
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 1) == MOQR_OK);
    step_n(s, 8);
    dl_probe_t w, c;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &w) == MOQR_OK);
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &c) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(w.object, 0);
    MOQ_TEST_CHECK_EQ_U64(c.object, 1);
    MOQ_TEST_CHECK_EQ_U64(w.len, c.len);
    MOQ_TEST_CHECK_EQ_U64(w.first, c.first);
    MOQ_TEST_CHECK_EQ_U64(w.prio, c.prio);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_parity");
    return failures;
}

/* The live edge: chunks published while the object is still OPEN cross as
 * they appear, resume without duplication, and exactly one OBJ_END follows
 * the completion. */
static int
test_shards_chunk_live_edge(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckL") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckL", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckL", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 48, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x11, 16) == MOQR_OK);
    step_n(s, 6);   /* the exposed edge crosses; delivery goes STALLED */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 1);
    uint64_t t0, m0, b0;
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK_EQ_U64(m0, 2);   /* OPEN + first chunk, nothing repeated */
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x11, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x11, 16) == MOQR_OK);
    step_n(s, 6);   /* resume from next_chunk, still open */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 1);
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK_EQ_U64(m0, 4);   /* + exactly the two new chunks */
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    step_n(s, 6);
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK_EQ_U64(m0, 5);   /* + one OBJ_END */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.len, 48);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_live_edge");
    return failures;
}

/* A capacity-1 channel forwards a many-chunk object one message per round,
 * in order, preserving next_chunk across every channel-full retry. */
static int
test_shards_chunk_cap1(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 1);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckQ") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckQ", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckQ", 7, &sub) == MOQR_OK);
    step_n(s, 6);   /* ACKED (the ACK itself needs the 1-entry channel) */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 64, false, 0, 0) ==
                   MOQR_OK);
    for (uint8_t c = 0; c < 4; c++) {
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, (uint8_t)(0x20 + c),
                                 16) == MOQR_OK);
    }
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    step_n(s, 20);   /* six messages, one channel slot */
    uint64_t t0, m0, b0;
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK_EQ_U64(m0, 6);   /* OPEN + 4 chunks + END, exactly once */
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.len, 64);
    MOQ_TEST_CHECK_EQ_U64(pr.first, 0x20);   /* chunk order preserved */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_cap1");
    return failures;
}

/* The per-turn message budget bounds chunk extraction mid-object and the
 * next turn resumes without duplication. */
static int
test_shards_chunk_turn_budget(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.pump_turn_messages = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckT") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckT", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckT", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 60, false, 0, 0) ==
                   MOQR_OK);
    for (uint8_t c = 0; c < 5; c++) {
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x33, 12) == MOQR_OK);
    }
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    uint64_t t0, m0, b0;
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    uint64_t t1, m1, b1;
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 2);   /* two messages, then the turn ends */
    step_n(s, 8);
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 7);   /* OPEN + 5 chunks + END, no repeats */
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.len, 60);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_turn_budget");
    return failures;
}

/* A header end_of_group on OBJ_OPEN is subgroup metadata: it survives to the
 * requester record and does not seal the subgroup — a later object in the
 * same subgroup still crosses. */
static int
test_shards_chunk_eog_header(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckE") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckE", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckE", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 16, true, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x44, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    step_n(s, 6);
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK(pr.eog);   /* the header bit crossed on OBJ_OPEN */
    /* No premature seal: object 1 in the same subgroup follows through. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 1, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 1, 0x45, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 1) == MOQR_OK);
    step_n(s, 6);
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.object, 1);
    MOQ_TEST_CHECK_EQ_U64(pr.first, 0x45);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_eog_header");
    return failures;
}

/* One subgroup slot: sequential objects in the same subgroup reuse it, and a
 * second live subgroup exhausts the owner table into the staged terminal —
 * STREAM_ERROR release, captured-track cancel, post-ACK DONE(INTERNAL), and
 * the capacity metric. */
static int
test_shards_chunk_slot_exhaust(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.pump_subgroup_slots = 1;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckX") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckX", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckX", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    /* Two sequential objects in (0, 0) share the single slot. */
    for (uint64_t o = 0; o < 2; o++) {
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, o, 16, false, 0,
                                    0) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, o, (uint8_t)(0x50 + o),
                                 16) == MOQR_OK);
        MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, o) == MOQR_OK);
        step_n(s, 6);
        dl_probe_t pr;
        MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(pr.object, o);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0),
                              1);
    }
    /* A second live subgroup has no slot: the delivery terminates the demand
     * loss-visibly instead of stalling the whole pump. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 1, 2, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 1, 2, 0x52, 16) == MOQR_OK);
    step_n(s, 10);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_shards_debug_remote_demand_term_capacity(s, 0), 1);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 0);   /* DONE(INTERNAL) landed */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_slot_exhaust");
    return failures;
}

/* Requester-side CAPACITY with an object still open: the staged terminal
 * abandons the open object before source_done, and every progress row and
 * clone unwinds. */
static int
test_shards_chunk_requester_abandon(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_max_objects_per_group = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckR") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckR", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckR", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    /* One local record + the remote OPEN fill group 0 (cap 2); the object
     * stays open at the requester. */
    uint64_t tgen = 0;
    moqr_track_t rt = moqr_shards_debug_pending_demand_track(s, 1, 0, &tgen);
    MOQ_TEST_CHECK(pub_obj(s, &a, 1, rt, 0, 9, 5, 0x21, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x22, 16) == MOQR_OK);
    step_n(s, 6);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 1);
    /* A second remote OPEN in the full group hits CAPACITY: the terminal
     * must abandon the open object first, then source_done. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 1, 1, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 1, 1, 0x23, 16) == MOQR_OK);
    step_n(s, 10);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_shards_debug_remote_demand_term_capacity(s, 1), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_requester_abandon");
    return failures;
}

/* WOULD_BLOCK between requester terminal stages: the abandon stage commits
 * while a squeezed intent ring holds source_done, and the retry finishes the
 * terminal without repeating the abandon or double-counting the metric. */
static int
test_shards_chunk_terminal_staged_wb(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_max_objects_per_group = 2;
    /* Ring floor pinned at 4 (max_subs + 1 is one of the floor terms):
     * one mirror install fans NS_FOUND to the manager's watcher plus three
     * test wildcards and fills it. */
    cfg.core_cfg.max_intents = 4;
    cfg.core_cfg.max_ns_nodes = 4;
    cfg.core_cfg.max_ns_subs = 4;
    cfg.core_cfg.max_subs = 3;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "wbR") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "wbR", &ot) == MOQR_OK);
    moqr_binding_t w1 = sub_open(s, 1, 5);
    moqr_ns_t wild = { NULL, 0 };
    for (uint64_t w = 0; w < 3; w++) {
        MOQ_TEST_CHECK(moqr_core_ns_subscribe(moqr_shards_core(s, 1), w1,
                                              wild, 100 + w) == MOQR_OK);
    }
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "wbR", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    /* Fill the requester's group 0 (cap 2): one local record plus the
     * remote open object. */
    uint64_t tgen = 0;
    moqr_track_t rt = moqr_shards_debug_pending_demand_track(s, 1, 0, &tgen);
    MOQ_TEST_CHECK(pub_obj(s, &a, 1, rt, 0, 9, 5, 0x31, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x32, 16) == MOQR_OK);
    step_n(s, 6);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 1);
    /* The CAPACITY OPEN and a fresh announce ride adjacent rounds: the
     * mirror install fans NS_FOUND to all four watchers in the same round
     * the terminal runs, so the abandon stage commits and source_done
     * holds. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 1, 1, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 1, 1, 0x33, 16) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "wbQ") == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    /* Held: the open object is abandoned, the demand is not yet done. */
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_shards_debug_remote_demand_term_capacity(s, 1), 0);
    step_n(s, 8);   /* the retry commits source_done, then UNDEMANDING */
    MOQ_TEST_CHECK_EQ_U64(
        moqr_shards_debug_remote_demand_term_capacity(s, 1), 1);
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_terminal_staged_wb");
    return failures;
}

/* An owner DONE queued behind the data message that triggered a requester
 * terminal must stay ordered behind it: while the staged terminal holds, the
 * trigger is not popped, so the DONE cannot jump the queue and erase the
 * terminal before it commits. */
static int
test_shards_chunk_done_behind_trigger(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_max_objects_per_group = 2;
    cfg.core_cfg.max_intents = 4;
    cfg.core_cfg.max_ns_nodes = 4;
    cfg.core_cfg.max_ns_subs = 4;
    cfg.core_cfg.max_subs = 4;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dbT") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "dbT", &ot) == MOQR_OK);
    moqr_binding_t w1 = sub_open(s, 1, 5);
    moqr_ns_t wild = { NULL, 0 };
    for (uint64_t w = 0; w < 3; w++) {
        MOQ_TEST_CHECK(moqr_core_ns_subscribe(moqr_shards_core(s, 1), w1,
                                              wild, 100 + w) == MOQR_OK);
    }
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "dbT", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    /* Fill the requester's group 0 so the remote object terminates it. */
    uint64_t tgen = 0;
    moqr_track_t rt = moqr_shards_debug_pending_demand_track(s, 1, 0, &tgen);
    MOQ_TEST_CHECK(pub_obj(s, &a, 1, rt, 0, 9, 5, 0x41, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 1, rt, 0, 9, 6, 0x42, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x43, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 0, p0, "dbQ") == MOQR_OK);   /* the ring squeeze */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* OBJ crosses */
    /* The owner track ends: its DONE queues BEHIND the object. */
    MOQ_TEST_CHECK(moqr_core_source_done(moqr_shards_core(s, 0), ot, 1, pd_wire(0),
                                         1000) == MOQR_OK);
    step_n(s, 12);
    /* The local terminal committed exactly once, despite the queued DONE. */
    MOQ_TEST_CHECK_EQ_U64(
        moqr_shards_debug_remote_demand_term_capacity(s, 1), 1);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_done_behind_trigger");
    return failures;
}

/* A post-ACK owner DONE arriving while an object is still open must abandon
 * it before source_done: the partial chunks drop out of the retained log
 * instead of surviving as an orphan OPEN record. */
static int
test_shards_chunk_done_abandons_open(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "daO") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "daO", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "daO", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 32, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x44, 16) == MOQR_OK);
    step_n(s, 6);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 1);
    MOQ_TEST_CHECK(moqr_core_source_done(moqr_shards_core(s, 0), ot, 1, pd_wire(0),
                                         1000) == MOQR_OK);
    step_n(s, 8);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 0);
    /* The abandoned object's partial chunk bytes are gone; an orphan OPEN
     * record would still retain them. */
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_done_abandons_open");
    return failures;
}

/* A remote OPEN below the requester's eviction horizon is rejected TOO_OLD:
 * its whole chunk/END sequence is consumed loss-visibly, the demand stays
 * live, and later in-range objects cross untouched. */
static int
test_shards_chunk_too_old_discard(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_budget.max_groups = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "toD") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "toD", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "toD", 7, &sub) == MOQR_OK);
    step_n(s, 4);   /* ACKED */
    /* Advance the requester's horizon past group 1: three local groups on a
     * two-group log evict the oldest. */
    uint64_t tgen = 0;
    moqr_track_t rt = moqr_shards_debug_pending_demand_track(s, 1, 0, &tgen);
    for (uint64_t g = 1; g <= 3; g++) {
        MOQ_TEST_CHECK(pub_obj(s, &a, 1, rt, g, 9, 0, (uint8_t)g, 8, 0, 0,
                               0, false, false) == MOQR_OK);
    }
    /* A chunked object in group 0 is below the horizon at the requester. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x45, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    for (int r = 0; r < 6; r++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_remote_data_rejected(s, 1), 3);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);   /* the demand survives */
    /* An in-range object crosses as if the discard never happened. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 5, 0, 0, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 5, 0, 0, 0x46, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 5, 0, 0) == MOQR_OK);
    for (int r = 0; r < 6; r++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    }
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 4);   /* 3 local + group 5 */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_too_old_discard");
    return failures;
}

/* -- cross-shard terminals and notices -------------------------------------- */

/* A late FIN crosses a capacity-1 channel: the SG_SEAL retries behind the
 * data until a slot frees and applies exactly once. */
static int
test_shards_term_seal_cap1(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 1);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tsC") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tsC", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tsC", 7, &sub) == MOQR_OK);
    step_n(s, 6);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x71, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(moqr_shards_core(s, 0), ot, 0,
                                           0) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 1, 0, 0, 0x72, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    step_n(s, 14);   /* three messages through a one-slot channel */
    uint64_t t0, m0, b0;
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK_EQ_U64(m0, 3);   /* OBJ + SG_SEAL + OBJ, exactly once */
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 2);
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.group, 0);
    MOQ_TEST_CHECK(pr.sg_end);   /* sealed at the requester */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_seal_cap1");
    return failures;
}

/* Header EOG stays metadata across the boundary: completing an EOG-headed
 * chunked object does NOT close the subgroup — only the crossed SG_SEAL
 * does. */
static int
test_shards_term_eog_vs_seal(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tsE") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tsE", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tsE", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 16, true, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x73, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    step_n(s, 6);
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK(pr.eog);
    MOQ_TEST_CHECK(!pr.sg_end);   /* EOG alone never seals */
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(moqr_shards_core(s, 0), ot, 0,
                                           0) == MOQR_OK);
    step_n(s, 6);
    /* The crossed seal is the durable FIN: d1 now owes a SEAL notice. */
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.notice, 1);   /* MOQR_DELIVERY_NOTICE_SEAL */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_eog_vs_seal");
    return failures;
}

/* The eviction watermark crosses ahead of newer records: tracked subgroups
 * below it close (sealed) at the requester before its own budget catches
 * up, and the notice applies exactly once across cap-1 retries. */
static int
test_shards_term_evict_watermark(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_budget.max_groups = 1;
    cfg.demand_channel_entries = 1;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tsW") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tsW", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tsW", 7, &sub) == MOQR_OK);
    step_n(s, 6);
    /* A completed-but-unsealed chunked subgroup in group 0 keeps a tracked
     * slot on both sides. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x74, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    step_n(s, 10);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0), 1);
    /* Group 5 pushes group 0 out at the owner: the watermark rides the FIFO
     * ahead of group 5's record (cap-1 keeps them a round apart), so the
     * requester's completed-but-unsealed group-0 subgroup closes while its
     * own copy of group 0 is still retained. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 5, 0, 0, 0x75, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* GRP_EVICT out */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* ...applied */
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.group, 0);
    MOQ_TEST_CHECK(pr.sg_end);   /* closed by the crossed watermark */
    step_n(s, 10);   /* group 5 follows through */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0), 0);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 2);   /* group 0 + group 5 */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_evict_watermark");
    return failures;
}

/* Cross-shard destination ingest reaches the core's ready-set marking with
 * zero extra code: the requester applies remote data through the SAME core
 * ingest functions. Since the bind pump (shard step phase 2) consumes the
 * set each round, the observable here is consumption-shaped: the OWNER
 * core's set stays empty at every stage (its only subscriber is the pseudo
 * pump-sub, filtered at production), the DESTINATION set is empty after the
 * step round (produced, then drained by its own bind pump — never
 * accumulating), and the delivery itself is untouched. The mark-production
 * sites are white-box-proven per source in test_relay_ready_set; the
 * end-to-end K=2 consumption proof with real sessions is the msquic
 * loopback lanes=2 deterministic vector. */
static int
test_shards_ready_set_k2(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tsQ") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tsQ", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tsQ", 7, &sub) == MOQR_OK);
    step_n(s, 8);
    /* Flush activation-time marks on the destination; the owner core's set
     * must already be empty (its only subscriber is the pseudo pump-sub). */
    uint64_t cookies[8];
    (void)moqr_core_drain_ready(moqr_shards_core(s, 1), cookies, 8);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_core_drain_ready(moqr_shards_core(s, 0), cookies, 8), 0);

    /* One published object crosses shards; the destination core ingest
     * marks the subscriber's binding and the same round's bind pump drains
     * it — the set never accumulates, and the owner set never fills. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x51, 16, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_core_drain_ready(moqr_shards_core(s, 0), cookies, 8), 0);
    step_n(s, 8);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_core_drain_ready(moqr_shards_core(s, 1), cookies, 8), 0);
    MOQ_TEST_CHECK_EQ_U64(
        moqr_core_drain_ready(moqr_shards_core(s, 0), cookies, 8), 0);

    /* The marked binding really has the delivery. */
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK(pr.group == 0 && pr.object == 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_ready_set_k2");
    return failures;
}

/* A publisher reset code above UINT32_MAX crosses bit-exact: the requester
 * object is abandoned with the same 62-bit value, visible to its begun
 * downstream subscriber. */
static int
test_shards_term_reset_code_wide(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tsR") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tsR", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tsR", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 32, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x77, 16) == MOQR_OK);
    step_n(s, 6);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 1);
    /* d1 begins the object downstream (live edge, STALLED). */
    {
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(moqr_shards_core(s, 1), d1,
                                               1000, &d) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(d.rec.group_id, 0);
        MOQ_TEST_CHECK(moqr_core_delivery_done(moqr_shards_core(s, 1), d1,
                                               MOQR_DELIVERY_STALLED,
                                               1000) == MOQR_OK);
    }
    /* The publisher resets the object with a 62-bit code. */
    const uint64_t wide = 0x23456789ABCDEFull;
    MOQ_TEST_CHECK(moqr_core_abandon_record(moqr_shards_core(s, 0), ot, 0, 0,
                                            0, rd_wire(wide)) == MOQR_OK);
    step_n(s, 8);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0), 0);
    /* The begun downstream subscriber sees the abandoned object with the
     * exact code. */
    {
        moqr_delivery_t d;
        MOQ_TEST_CHECK(moqr_core_next_delivery(moqr_shards_core(s, 1), d1,
                                               2000, &d) == MOQR_OK);
        MOQ_TEST_CHECK(d.rec.obj_state == MOQR_OBJ_ABANDONED);
        MOQ_TEST_CHECK_EQ_U64(d.rec.reset.value, wide);
        MOQ_TEST_CHECK(moqr_core_delivery_done(moqr_shards_core(s, 1), d1,
                                               MOQR_DELIVERY_ABANDONED,
                                               2000) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_reset_code_wide");
    return failures;
}

/* Evicting a group with begun live-edge objects produces exactly one
 * GRP_RESET that abandons every affected open subgroup at the requester. */
static int
test_shards_term_group_evict_reset(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_budget.max_groups = 2;
    cfg.demand_channel_entries = 1;   /* one message per round: the reset's
                                       * own effect is observable before the
                                       * watermark could mask a partial one */
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tsG") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tsG", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tsG", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    /* Two live-edge subgroups in group 0, both begun across the boundary. */
    for (uint64_t sg = 0; sg < 2; sg++) {
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, sg, sg, 32, false, 0,
                                    0) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, sg, sg,
                                 (uint8_t)(0x78 + sg), 16) == MOQR_OK);
    }
    step_n(s, 8);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 2);
    uint64_t t0, m0, b0;
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    /* Groups 5 and 9 evict begun group 0. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 5, 0, 0, 0x7A, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 9, 0, 0, 0x7B, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* GRP_RESET out */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* ...applied */
    /* The reset ALONE cleans both open subgroups — nothing rides on the
     * watermark behind it. */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    step_n(s, 16);
    uint64_t t1, m1, b1;
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    /* One GRP_RESET + one GRP_EVICT + the two records: exactly four. */
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 4);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0), 0);
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);   /* the demand survives */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_group_evict_reset");
    return failures;
}

/* Mixed-group eviction is two-stage, like the local bind: GRP_RESET resets
 * only the BEGUN subgroup and must retain the completed-but-unsealed one's
 * progress slot, so the GRP_EVICT behind it can still find and seal that
 * stream — exactly once, before the requester's own budget evicts the
 * group. */
static int
test_shards_term_group_reset_mixed(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_budget.max_groups = 2;
    cfg.demand_channel_entries = 1;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tmX") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tmX", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tmX", 7, &sub) == MOQR_OK);
    step_n(s, 6);
    /* Group 0, subgroup 0: completed but unsealed (a tracked slot on both
     * sides with no open object). */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 16, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x7D, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    step_n(s, 10);
    /* Group 0, subgroup 1: begun and still open. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 1, 1, 32, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 1, 1, 0x7E, 16) == MOQR_OK);
    step_n(s, 10);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 1);
    /* Group 5 settles, then group 9 evicts begun group 0. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 5, 0, 0, 0x7F, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    step_n(s, 8);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 9, 0, 0, 0x80, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* GRP_RESET out */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* ...applied */
    /* Intermediate pin: the reset closed ONLY the begun subgroup. */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* GRP_EVICT out */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);   /* ...applied */
    /* The watermark found the retained slot and sealed the completed
     * subgroup while its record is still locally retained. */
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.group, 0);
    MOQ_TEST_CHECK_EQ_U64(pr.subgroup, 0);
    MOQ_TEST_CHECK(pr.sg_end);   /* sealed by GRP_EVICT, exactly once */
    step_n(s, 12);   /* groups 5 and 9 follow through */
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_group_reset_mixed");
    return failures;
}

/* Zero-byte terminal messages still spend the turn-message budget. */
static int
test_shards_term_zero_byte_budget(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.pump_turn_messages = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tsZ") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tsZ", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tsZ", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    /* Three drained subgroups, all sealed late: three zero-byte notices. */
    for (uint64_t sg = 0; sg < 3; sg++) {
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, sg, sg, (uint8_t)sg, 8, 0,
                               0, 0, false, false) == MOQR_OK);
    }
    step_n(s, 6);
    for (uint64_t sg = 0; sg < 3; sg++) {
        MOQ_TEST_CHECK(moqr_core_seal_subgroup(moqr_shards_core(s, 0), ot,
                                               0, sg) == MOQR_OK);
    }
    uint64_t t0, m0, b0;
    moqr_shards_debug_pump_counters(s, 0, &t0, &m0, &b0);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    uint64_t t1, m1, b1;
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 2);   /* zero bytes, still two messages */
    step_n(s, 6);
    moqr_shards_debug_pump_counters(s, 0, &t1, &m1, &b1);
    MOQ_TEST_CHECK_EQ_U64(m1 - m0, 3);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_zero_byte_budget");
    return failures;
}

/* Destroy with reset/seal/eviction messages still queued releases
 * everything (the counting allocator balances). */
static int
test_shards_term_destroy_queued(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tsD") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tsD", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tsD", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x7C, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_seal_subgroup(moqr_shards_core(s, 0), ot, 0,
                                           0) == MOQR_OK);
    for (int i = 0; i < 4; i++) {   /* owner-only: notices sit queued */
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) ==
                       MOQR_OK);
        moqr_shards_debug_round_advance(s);
    }
    MOQ_TEST_CHECK(moqr_shards_debug_demand_channel_pending(s, 0, 1) > 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_destroy_queued");
    return failures;
}

/* Cancellation matrix: before ACK, after ACK (drained), and mid-chunk.
 * Every arm quiesces both cores, the progress tables, and the channels; the
 * mid-chunk arm is the WARM race — the core goes ACTIVE->WARM before the
 * unsubscribe intent surfaces, and the partially ingested OPEN record must
 * still be abandoned (STOPPED), never stranded behind a cleared row. */
static int
test_shards_term_cancel_matrix(void)
{
    int failures = 0;
    for (int arm = 0; arm < 3; arm++) {
        ca_t a;
        ca_init(&a);
        moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
        MOQ_TEST_CHECK(s != NULL);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "tcM") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tcM", &ot) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tcM", 7, &sub) == MOQR_OK);
        if (arm == 0) {
            /* Before the ACK returns. */
            MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
            MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub,
                                                 1000) == MOQR_OK);
        } else {
            step_n(s, 4);   /* ACKED */
            if (arm == 1) {
                MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x51, 8, 0, 0,
                                       0, false, false) == MOQR_OK);
                step_n(s, 4);
            } else {
                MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 32, false,
                                            0, 0) == MOQR_OK);
                MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x52, 16) ==
                               MOQR_OK);
                step_n(s, 4);
                MOQ_TEST_CHECK_EQ_U64(
                    moqr_shards_debug_requester_open_objects(s, 1), 1);
            }
            MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub,
                                                 1000) == MOQR_OK);
        }
        /* Advance past the ACTIVE linger so the idle teardown fires. */
        for (int r = 0; r < 12; r++) {
            MOQ_TEST_CHECK(moqr_shards_step(s, 5000 + (uint64_t)r) ==
                           MOQR_OK);
        }
        moqr_core_stats_t st;
        moqr_core_get_stats(moqr_shards_core(s, 1), &st);
        MOQ_TEST_CHECK_EQ_U64(st.subs_active, 0);
        MOQ_TEST_CHECK_EQ_U64(
            moqr_shards_debug_requester_open_objects(s, 1), 0);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 1), 0);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_progress_slots(s, 0),
                              0);
        if (arm == 2) {
            /* The WARM log kept the record but not the partial payload:
             * the teardown abandoned it (chunk bytes dropped). An ordinary
             * stop is not a loss terminal — no metric. */
            MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 0);
        }
        MOQ_TEST_CHECK_EQ_U64(
            moqr_shards_debug_remote_demand_term_capacity(s, 1), 0);
        MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("shards_term_cancel_matrix");
    return failures;
}

/* K=3, same demand_id from two requesters: cancelling one mid-object must
 * not disturb the other's identical id at the shared owner. */
static int
test_shards_term_collision_k3(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 3, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tcK") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tcK", &ot) == MOQR_OK);
    step_n(s, 10);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_binding_t d2 = sub_open(s, 2, 2);
    moqr_sub_t s1, s2;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tcK", 7, &s1) == MOQR_OK);
    MOQ_TEST_CHECK(do_subscribe(s, 2, d2, "tcK", 7, &s2) == MOQR_OK);
    step_n(s, 6);   /* both ACKED with demand_id 1 at the same owner */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 2);
    /* A live-edge object reaches both requesters. */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 32, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x53, 16) == MOQR_OK);
    step_n(s, 6);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 2), 1);
    /* Requester 1 cancels mid-object; requester 2 rides to completion. */
    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), s1, 1000) ==
                   MOQR_OK);
    for (int r = 0; r < 12; r++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 5000 + (uint64_t)r) == MOQR_OK);
    }
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x53, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
    for (int r = 0; r < 10; r++) {
        MOQ_TEST_CHECK(moqr_shards_step(s, 6000 + (uint64_t)r) == MOQR_OK);
    }
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 1);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 2), 0);
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 2, d2, &pr) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(pr.len, 32);   /* completed despite the sibling */
    MOQ_TEST_CHECK_EQ_U64(pr.first, 0x53);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 3), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_collision_k3");
    return failures;
}

/* Mirror re-target while an object is open: the old demand terminates
 * GOING_AWAY (the partial record abandoned), the old owner's queued data
 * pops as moot, and nothing from it enters the new target. */
static int
test_shards_term_retarget_open(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 3, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p1 = pub_open(s, 1);
    MOQ_TEST_CHECK(ann(s, 1, p1, "trO") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 1, p1, "trO", &ot) == MOQR_OK);
    step_n(s, 10);
    moqr_binding_t d0 = sub_open(s, 0, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 0, d0, "trO", 7, &sub) == MOQR_OK);
    step_n(s, 6);   /* ACKED at shard 1 */
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 1, ot, 0, 0, 0, 32, false, 0, 0) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 1, ot, 0, 0, 0, 0x54, 16) == MOQR_OK);
    step_n(s, 6);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 0), 1);
    /* More data enqueues owner-only, then the mirror flips to shard 2. */
    MOQ_TEST_CHECK(pub_chunk(s, &a, 1, ot, 0, 0, 0, 0x54, 16) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, NULL) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK(unann(s, 1, p1, "trO") == MOQR_OK);
    step_n(s, 6);   /* the withdrawal propagates; mirrors come down */
    moqr_binding_t p2 = pub_open(s, 2);
    MOQ_TEST_CHECK(ann(s, 2, p2, "trO") == MOQR_OK);
    step_n(s, 12);
    /* The old demand is gone terminally; the partial record was abandoned
     * (its chunk bytes dropped), and the queued chunk popped as moot. */
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 0), &st);
    MOQ_TEST_CHECK_EQ_U64(st.subs_active, 0);   /* GOING_AWAY, no migration */
    MOQ_TEST_CHECK_EQ_U64(st.retained_bytes, 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_requester_open_objects(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_pending_demand(s, 0), 0);
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 1), 0);
    MOQ_TEST_CHECK_EQ_U64(core_tracks(s, 2), 0);   /* nothing leaked over */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 3), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_retarget_open");
    return failures;
}

/* Local-vs-remote parity: the same reordered-group sequence — including a
 * below-horizon straggler and an eviction — leaves a control core (direct
 * local ingest) and the requester core (crossed over the demand channel)
 * with the same accepted count, the same rejected count, and the same
 * retained bytes under equal budgets. */
static int
test_shards_term_parity_reorder(void)
{
    int failures = 0;
    /* op list: {group, kind} — kind 0 = whole 8-byte object, 1 = chunked
     * 16-byte object, all subgroup 0 with per-group object ids 0. */
    static const struct { uint64_t g; int chunked; } seq[] = {
        { 1, 0 }, { 2, 0 }, { 3, 1 }, { 1, 0 }, { 2, 1 }, { 9, 0 },
    };
    enum { SEQ_N = sizeof(seq) / sizeof(seq[0]) };

    /* Control: direct local ingest with the shared budget. */
    ca_t ca;
    ca_init(&ca);
    moqr_shards_cfg_t ccfg;
    shards_cfg(&ccfg, &ca, 1, 0);
    ccfg.core_cfg.log_budget.max_groups = 2;
    moqr_shards_t *cs = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&ccfg, &cs) == MOQR_OK);
    moqr_binding_t cp = pub_open(cs, 0);
    MOQ_TEST_CHECK(ann(cs, 0, cp, "tpP") == MOQR_OK);
    moqr_track_t ct;
    MOQ_TEST_CHECK(pub_track_h(cs, 0, cp, "tpP", &ct) == MOQR_OK);
    moqr_binding_t cd = sub_open(cs, 0, 2);
    moqr_sub_t csub;
    MOQ_TEST_CHECK(do_subscribe(cs, 0, cd, "tpP", 7, &csub) == MOQR_OK);
    step_n(cs, 4);
    uint64_t c_rej = 0;
    for (int i = 0; i < SEQ_N; i++) {
        moqr_result_t rc;
        if (seq[i].chunked) {
            rc = pub_open_rec(cs, &ca, 0, ct, seq[i].g, 0, (uint64_t)i, 16,
                              false, 0, 0);
            if (rc == MOQR_OK) {
                MOQ_TEST_CHECK(pub_chunk(cs, &ca, 0, ct, seq[i].g, 0,
                                         (uint64_t)i, 0x60, 16) == MOQR_OK);
                MOQ_TEST_CHECK(pub_complete(cs, 0, ct, seq[i].g, 0,
                                            (uint64_t)i) == MOQR_OK);
            }
        } else {
            rc = pub_obj(cs, &ca, 0, ct, seq[i].g, 0, (uint64_t)i, 0x60, 8,
                         0, 0, 0, false, false);
        }
        if (rc == MOQR_ERR_TOO_OLD || rc == MOQR_ERR_CAPACITY) {
            c_rej++;
        } else {
            MOQ_TEST_CHECK(rc == MOQR_OK);
        }
        step_n(cs, 2);
    }
    moqr_core_stats_t cst;
    moqr_core_get_stats(moqr_shards_core(cs, 0), &cst);

    /* Remote: the same sequence crosses the demand channel. */
    ca_t a;
    ca_init(&a);
    moqr_shards_cfg_t cfg;
    shards_cfg(&cfg, &a, 2, 0);
    cfg.admit_remote_demand = true;
    cfg.core_cfg.log_budget.max_groups = 2;
    moqr_shards_t *s = NULL;
    MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "tpP") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "tpP", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "tpP", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    for (int i = 0; i < SEQ_N; i++) {
        moqr_result_t rc;
        if (seq[i].chunked) {
            rc = pub_open_rec(s, &a, 0, ot, seq[i].g, 0, (uint64_t)i, 16,
                              false, 0, 0);
            if (rc == MOQR_OK) {
                MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, seq[i].g, 0,
                                         (uint64_t)i, 0x60, 16) == MOQR_OK);
                MOQ_TEST_CHECK(pub_complete(s, 0, ot, seq[i].g, 0,
                                            (uint64_t)i) == MOQR_OK);
            }
        } else {
            rc = pub_obj(s, &a, 0, ot, seq[i].g, 0, (uint64_t)i, 0x60, 8, 0,
                         0, 0, false, false);
        }
        MOQ_TEST_CHECK(rc == MOQR_OK || rc == MOQR_ERR_TOO_OLD ||
                       rc == MOQR_ERR_CAPACITY);
        step_n(s, 4);
    }
    step_n(s, 10);
    moqr_core_stats_t rst;
    moqr_core_get_stats(moqr_shards_core(s, 1), &rst);
    /* Owner-side rejects never even cross; requester-side rejects count in
     * remote_data_rejected. The union must match the control's rejects, and
     * accepted records + retained bytes must be identical. */
    MOQ_TEST_CHECK_EQ_U64(rst.ingested_total, cst.ingested_total);
    MOQ_TEST_CHECK_EQ_U64(rst.retained_bytes, cst.retained_bytes);
    MOQ_TEST_CHECK(c_rej > 0);   /* the straggler really was rejected */
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    moqr_shards_destroy(cs);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_CHECK_EQ_INT((int)ca.live, 0);
    MOQ_TEST_PASS("shards_term_parity_reorder");
    return failures;
}

/* Producer-credit wake: a durable pop that frees a cap-1 channel slot must
 * report the PRODUCER's bit in the consumer's step mask — a lane held on
 * channel-full is woken by credit, never by an idle sweep. */
static int
test_shards_term_credit_wake(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 1);   /* cap-1 channels */
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "cwK") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "cwK", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "cwK", 7, &sub) == MOQR_OK);
    step_n(s, 6);   /* ACKED, channels quiesced */
    /* Two objects: the first fills the one slot, the second holds at the
     * producer. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x81, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 1, 0x82, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    uint64_t mask = 0;
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK_EQ_U64(mask, 1ull << 1);   /* the push woke the consumer */
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1),
                          1);
    /* The consumer applies the head: its step mask is JUST the producer credit
     * toward shard 0 (one 0->1 slot freed, nothing pushed). It requests NO local
     * continuation (its own bit 1) — the single applied object drains in this one
     * bind-pump pass, so its live subscription has no residual ready work (the
     * connless cross-shard demand binding's stale ready bit is NOT residual and
     * is excluded by the live-connection scan). */
    mask = 0;
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK_EQ_U64(mask, 1ull << 0);
    /* The woken producer immediately fills the freed slot. */
    mask = 0;
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK(mask & (1ull << 1));
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1),
                          1);
    step_n(s, 8);   /* quiesce */
    moqr_core_stats_t st;
    moqr_core_get_stats(moqr_shards_core(s, 1), &st);
    MOQ_TEST_CHECK_EQ_U64(st.ingested_total, 2);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_term_credit_wake");
    return failures;
}

/* Impossible progress-table products are rejected at create with INVAL
 * before any dependent allocation — never an undersized table — but ONLY
 * where the tables can exist: K>1 with admission on. Admission-off and K=1
 * runtimes neither reserve nor validate the disabled machinery, so the same
 * impossible knob is inert there. The guarded allocator turns a missing
 * validation into a clean refused allocation. */
static int
test_shards_cfg_progress_overflow(void)
{
    int failures = 0;
    /* Both rejected via the row-count bound on this LP64 host; the byte-
     * product term of the same check is the 32-bit size_t guard, not an
     * independently reachable branch here. */
    static const uint32_t cases[][2] = {
        { 64u, UINT32_MAX },
        { 1u << 27, UINT32_MAX },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        ca_t a;
        ca_init(&a);
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, 2, 0);
        moq_alloc_t gvt = a.vt;
        gvt.alloc = ca_a_guard;
        cfg.alloc = &gvt;
        cfg.admit_remote_demand = true;
        cfg.pending_demand_entries = cases[i][0];
        cfg.pump_subgroup_slots = cases[i][1];
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_ERR_INVAL);
        MOQ_TEST_CHECK(s == NULL);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    /* Admission off (K=2) and K=1: the impossible product is dead config —
     * create succeeds through the guarded allocator (nothing table-sized is
     * ever requested), the gauges stay zero, and teardown balances. */
    for (uint16_t k = 1; k <= 2; k++) {
        ca_t a;
        ca_init(&a);
        moqr_shards_cfg_t cfg;
        shards_cfg(&cfg, &a, k, 0);
        moq_alloc_t gvt = a.vt;
        gvt.alloc = ca_a_guard;
        cfg.alloc = &gvt;
        cfg.pump_subgroup_slots = UINT32_MAX;
        moqr_shards_t *s = NULL;
        MOQ_TEST_CHECK(moqr_shards_create(&cfg, &s) == MOQR_OK);
        MOQ_TEST_CHECK(s != NULL);
        for (int r = 0; r < 4; r++) {
            MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
        }
        for (uint16_t i = 0; i < k; i++) {
            MOQ_TEST_CHECK_EQ_U64(
                moqr_shards_debug_owner_progress_slots(s, i), 0);
            MOQ_TEST_CHECK_EQ_U64(
                moqr_shards_debug_requester_open_objects(s, i), 0);
        }
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("shards_cfg_progress_overflow");
    return failures;
}

/* A clone failure mid chunked extraction releases the delivery with
 * STREAM_ERROR and fail-stops; nothing pins and everything unwinds. A
 * warm-up object grows the core's pin array first so the fail sweep lands
 * on the clone sites (properties, then each chunk). The final arm fails the
 * pin-array growth itself — that surfaces from next_delivery before any
 * delivery is outstanding, so it fail-stops with nothing to release. */
static int
test_shards_chunk_producer_oom(void)
{
    int failures = 0;
    for (long fail = 1; fail <= 3; fail++) {
        ca_t a;
        ca_init(&a);
        moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
        MOQ_TEST_CHECK(s != NULL);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "ckO") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckO", &ot) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckO", 7, &sub) == MOQR_OK);
        step_n(s, 4);
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 32, false, 0, 0)
                       == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x60, 16) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x61, 16) == MOQR_OK);
        MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
        step_n(s, 6);   /* warm-up crosses; the pin array is now sized */
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 1, 32, false, 0x6A,
                                    8) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 1, 0x6B, 16) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 1, 0x6C, 16) == MOQR_OK);
        MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 1) == MOQR_OK);
        a.fail_at = fail;
        moqr_result_t rc = moqr_shards_step(s, 1000);
        a.fail_at = 0;
        MOQ_TEST_CHECK(rc == MOQR_ERR_NOMEM);
        MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_owner_pump_subs(s, 0), 0);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    {
        /* Pin-array growth failure: next_delivery reports it before the
         * delivery exists, so the pump fail-stops with the pump-sub intact
         * and destroy unwinds everything. */
        ca_t a;
        ca_init(&a);
        moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
        MOQ_TEST_CHECK(s != NULL);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "ckO") == MOQR_OK);
        moqr_track_t ot;
        MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckO", &ot) == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckO", 7, &sub) == MOQR_OK);
        step_n(s, 4);
        MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 32, false, 0, 0)
                       == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x62, 16) == MOQR_OK);
        MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x63, 16) == MOQR_OK);
        MOQ_TEST_CHECK(pub_complete(s, 0, ot, 0, 0, 0) == MOQR_OK);
        a.fail_at = 1;
        moqr_result_t rc = moqr_shards_step(s, 1000);
        a.fail_at = 0;
        MOQ_TEST_CHECK(rc == MOQR_ERR_NOMEM);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("shards_chunk_producer_oom");
    return failures;
}

/* Destroy with OBJ_OPEN / OBJ_CHUNK messages still queued in the channel:
 * every clone releases and the allocator balances. */
static int
test_shards_chunk_destroy_queued(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "ckD") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "ckD", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "ckD", 7, &sub) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(pub_open_rec(s, &a, 0, ot, 0, 0, 0, 32, false, 0x7A, 8) ==
                   MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x7B, 16) == MOQR_OK);
    MOQ_TEST_CHECK(pub_chunk(s, &a, 0, ot, 0, 0, 0, 0x7C, 16) == MOQR_OK);
    /* Owner-only steps: the messages sit in the (0 -> 1) channel and the
     * requester never drains them. */
    for (int i = 0; i < 3; i++) {
        MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, NULL) ==
                       MOQR_OK);
        moqr_shards_debug_round_advance(s);
    }
    MOQ_TEST_CHECK(moqr_shards_debug_demand_channel_pending(s, 0, 1) > 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_chunk_destroy_queued");
    return failures;
}

/* K == 1 stays inert: no manager, so the demand path never runs and a subscribe
 * behaves exactly as a single-shard relay. */
static int
test_shards_demand_k1_inert(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 1, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "x") == MOQR_OK);
    moqr_binding_t d0 = sub_open(s, 0, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 0, d0, "x", 5, &sub) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(rdr(s, 0), 0);   /* no manager, no refusal */
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_demand_k1_inert");
    return failures;
}

/* -- production stats: per-kind enqueues, wake causes, HWM, journal epoch --- */

/* The healthy admit round-trip's EXACT durable enqueues, by kind: one
 * DEMAND from the requester, one ACK then one OBJ from the owner, one
 * UNDEMAND on the idle teardown — and nothing else. The stats mirror the
 * debug pump counters exactly. */
static int
test_shards_stats_enqueue_counts(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "obE") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "obE", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "obE", 7, &sub) == MOQR_OK);
    step_n(s, 5);   /* DEMAND -> admit -> ACK -> resolve */

    moqr_shards_stats_t st0, st1;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st1.enqueued[MOQR_SHARDS_MSG_DEMAND], 1);
    MOQ_TEST_CHECK_EQ_U64(st0.enqueued[MOQR_SHARDS_MSG_ACK], 1);
    MOQ_TEST_CHECK_EQ_U64(st0.enqueued[MOQR_SHARDS_MSG_DEMAND], 0);
    MOQ_TEST_CHECK_EQ_U64(st0.enqueued[MOQR_SHARDS_MSG_OBJ], 0);
    MOQ_TEST_CHECK_EQ_U64(st0.enqueued[MOQR_SHARDS_MSG_DONE], 0);
    MOQ_TEST_CHECK_EQ_U64(st1.enqueued[MOQR_SHARDS_MSG_ACK], 0);

    /* One whole object: exactly one owner OBJ enqueue. */
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x5C, 24, 0, 0, 0, false,
                           false) == MOQR_OK);
    step_n(s, 4);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st0.enqueued[MOQR_SHARDS_MSG_OBJ], 1);
    MOQ_TEST_CHECK_EQ_U64(st0.enqueued[MOQR_SHARDS_MSG_OBJ_OPEN], 0);
    uint64_t turns = 0, msgs = 0, bytes = 0;
    moqr_shards_debug_pump_counters(s, 0, &turns, &msgs, &bytes);
    MOQ_TEST_CHECK_EQ_U64(st0.pump_turns, turns);
    MOQ_TEST_CHECK_EQ_U64(st0.pump_messages, msgs);
    MOQ_TEST_CHECK_EQ_U64(st0.pump_bytes, bytes);

    /* Idle teardown: exactly one requester UNDEMAND, then quiescence. */
    dl_probe_t pr;
    MOQ_TEST_CHECK(pull_one(s, 1, d1, &pr) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_core_unsubscribe(moqr_shards_core(s, 1), sub, 1000) ==
                   MOQR_OK);
    for (int r = 0; r < 10; r++) {
        (void)moqr_shards_step(s, 100000);   /* past the linger deadline */
    }
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st1.enqueued[MOQR_SHARDS_MSG_UNDEMAND], 1);
    MOQ_TEST_CHECK_EQ_U64(st1.pending_demands, 0);
    MOQ_TEST_CHECK_EQ_U64(st1.inbound_channel_entries, 0);
    MOQ_TEST_CHECK_EQ_U64(st1.inbound_channel_bytes, 0);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_stats_enqueue_counts");
    return failures;
}

/* Only DURABLE enqueues count: a push refused by a full cap-1 channel is an
 * attempt, not an enqueue — the counter moves only when the retry lands. */
static int
test_shards_stats_attempt_not_counted(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 1);   /* cap-1 channels */
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "obG") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "obG", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "obG", 7, &sub) == MOQR_OK);
    step_n(s, 6);   /* ACKED, channels quiesced */

    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0x91, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 1, 0x92, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    /* One owner step: the first OBJ fills the one slot; the second push is
     * REFUSED. Exactly one durable enqueue counts. */
    uint64_t mask = 0;
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    moqr_shards_stats_t st0;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st0.enqueued[MOQR_SHARDS_MSG_OBJ], 1);
    /* The consumer frees the slot; the retried push then counts. */
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st0.enqueued[MOQR_SHARDS_MSG_OBJ], 2);
    step_n(s, 8);
    MOQ_TEST_CHECK_EQ_U64(dch_total(s, 2), 0);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_stats_attempt_not_counted");
    return failures;
}

/* Wake requests count at the MASK level: two messages toward ONE
 * destination in one step are ONE push wake request; two pops on one
 * channel are ONE credit wake request; an idle step raises none. */
static int
test_shards_stats_wake_masks(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "obW") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "obW", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "obW", 7, &sub) == MOQR_OK);
    step_n(s, 6);   /* ACKED; baselines absorb the setup's own wakes */

    moqr_shards_stats_t st0, st1;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    uint64_t bp0 = st0.wake_requests_push, bc0 = st0.wake_requests_credit;
    uint64_t bp1 = st1.wake_requests_push, bc1 = st1.wake_requests_credit;
    uint64_t bl0 = st0.wake_requests_local;
    uint64_t bl1 = st1.wake_requests_local;

    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0xA1, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 1, 0xA2, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    uint64_t mask = 0;
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK_EQ_U64(mask, BIT(1));
    MOQ_TEST_CHECK_EQ_U64(moqr_shards_debug_demand_channel_pending(s, 0, 1),
                          2);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st0.wake_requests_push, bp0 + 1);   /* ONE, not two */
    MOQ_TEST_CHECK_EQ_U64(st0.wake_requests_credit, bc0);
    MOQ_TEST_CHECK_EQ_U64(st0.wake_requests_local, bl0);   /* owner applied
                                                            * nothing inbound */

    /* The consumer's step pops both messages: one 0->1 channel regained
     * capacity — ONE credit wake request toward shard 0, nothing pushed — and
     * its mask is JUST that credit bit. It requests NO local continuation (its
     * own bit 1): the two-object batch drains in this one bind-pump pass, so its
     * live subscription has no residual (the connless cross-shard demand
     * binding's stale ready bit is excluded by the live-connection scan). */
    mask = 0;
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK_EQ_U64(mask, BIT(0));
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st1.wake_requests_credit, bc1 + 1);
    MOQ_TEST_CHECK_EQ_U64(st1.wake_requests_push, bp1);
    MOQ_TEST_CHECK_EQ_U64(st1.wake_requests_local, bl1);   /* no false wake */

    /* A further idle step raises no wake requests of any cause. */
    mask = 0;
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK_EQ_U64(mask, 0);
    moqr_shards_stats_t st1b;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1b) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st1b.wake_requests_local, st1.wake_requests_local);
    MOQ_TEST_CHECK_EQ_U64(st1b.wake_requests_push, st1.wake_requests_push);
    MOQ_TEST_CHECK_EQ_U64(st1b.wake_requests_credit,
                          st1.wake_requests_credit);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_stats_wake_masks");
    return failures;
}

/* Per-directed-channel high-water marks: maintained inside the push's own
 * critical section, surviving the drain — occupancy returns to zero, the
 * peak stays. */
static int
test_shards_stats_channel_hwm(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "obH") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "obH", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "obH", 7, &sub) == MOQR_OK);
    step_n(s, 6);

    /* Baseline: shard 1's one inbound channel carried exactly the ACK
     * (one entry, zero logical bytes). */
    moqr_shards_stats_t st1;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st1.channel_entries_hwm, 1);
    MOQ_TEST_CHECK_EQ_U64(st1.channel_bytes_hwm, 0);

    /* Three 32-byte objects queue concurrently in one owner step. */
    for (uint64_t o = 0; o < 3; o++) {
        MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, o, (uint8_t)(0xB0 + o),
                               32, 0, 0, 0, false, false) == MOQR_OK);
    }
    uint64_t mask = 0;
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 0, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st1.inbound_channel_entries, 3);
    MOQ_TEST_CHECK_EQ_U64(st1.inbound_channel_bytes, 96);
    MOQ_TEST_CHECK_EQ_U64(st1.channel_entries_hwm, 3);
    MOQ_TEST_CHECK_EQ_U64(st1.channel_bytes_hwm, 96);

    /* Drain: occupancy zero, peaks retained. */
    MOQ_TEST_CHECK(moqr_shards_debug_step_shard(s, 1, 1000, &mask) ==
                   MOQR_OK);
    moqr_shards_debug_round_advance(s);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st1.inbound_channel_entries, 0);
    MOQ_TEST_CHECK_EQ_U64(st1.inbound_channel_bytes, 0);
    MOQ_TEST_CHECK_EQ_U64(st1.channel_entries_hwm, 3);
    MOQ_TEST_CHECK_EQ_U64(st1.channel_bytes_hwm, 96);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_stats_channel_hwm");
    return failures;
}

/* Pump-sub gauges classify through the core sub-state seam (PARKED before
 * the owner track resolves, ACTIVE after), and the manager's own entities
 * are reported for the exporter's exclusion: K binding slots + one
 * watcher. */
static int
test_shards_stats_gauges(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    {
        /* ACTIVE: the pump-sub joined a publishing track. */
        moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
        MOQ_TEST_CHECK(s != NULL);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "gaA") == MOQR_OK);
        MOQ_TEST_CHECK(pub_track(s, &a, 0, p0, "gaA") == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "gaA", 7, &sub) == MOQR_OK);
        step_n(s, 5);
        moqr_shards_stats_t st0, st1;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(st0.pump_subs_active, 1);
        MOQ_TEST_CHECK_EQ_U64(st0.pump_subs_parked, 0);
        MOQ_TEST_CHECK_EQ_U64(st0.internal_bindings, 2);
        MOQ_TEST_CHECK_EQ_U64(st0.internal_ns_subs, 1);
        MOQ_TEST_CHECK_EQ_U64(st1.pending_demands, 1);
        MOQ_TEST_CHECK_EQ_U64(st1.pump_subs_active, 0);
        /* The exclusion invariant the exporter subtracts under: the core's
         * own gauges dominate the internal counts. */
        moqr_core_stats_t cs;
        moqr_core_get_stats(moqr_shards_core(s, 0), &cs);
        MOQ_TEST_CHECK(cs.subs_active >= st0.pump_subs_active);
        MOQ_TEST_CHECK(cs.bindings >= st0.internal_bindings);
        MOQ_TEST_CHECK(cs.ns_subs >= st0.internal_ns_subs);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    {
        /* PARKED: the owner namespace is announced but never published, so
         * the pump-sub waits on a PENDING track. */
        moqr_shards_t *s = mk_shards_admit(&a, 2, 0);
        MOQ_TEST_CHECK(s != NULL);
        moqr_binding_t p0 = pub_open(s, 0);
        MOQ_TEST_CHECK(ann(s, 0, p0, "gaP") == MOQR_OK);
        step_n(s, 8);
        moqr_binding_t d1 = sub_open(s, 1, 2);
        moqr_sub_t sub;
        MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "gaP", 7, &sub) == MOQR_OK);
        step_n(s, 4);
        moqr_shards_stats_t st0;
        MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
        MOQ_TEST_CHECK_EQ_U64(st0.pump_subs_parked, 1);
        MOQ_TEST_CHECK_EQ_U64(st0.pump_subs_active, 0);
        moqr_shards_destroy(s);
        MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    }
    MOQ_TEST_PASS("shards_stats_gauges");
    return failures;
}

/* K == 1 is structurally inert for the whole cross-shard stats plane: every
 * field zero, no matter what the single shard does locally. */
static int
test_shards_stats_k1_inert(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 1, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "k1S") == MOQR_OK);
    MOQ_TEST_CHECK(pub_track(s, &a, 0, p0, "k1S") == MOQR_OK);
    moqr_binding_t d0 = sub_open(s, 0, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 0, d0, "k1S", 5, &sub) == MOQR_OK);
    step_n(s, 4);
    moqr_shards_stats_t st, zero;
    memset(&zero, 0, sizeof(zero));
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
    MOQ_TEST_CHECK(memcmp(&st, &zero, sizeof(st)) == 0);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_shards_get_stats(NULL, 0, &st) == MOQR_ERR_INVAL);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, NULL) == MOQR_ERR_INVAL);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_stats_k1_inert");
    return failures;
}

/* The journal projection generation: candidate arrivals, the winner's
 * mirror install, a CANDIDATE-ONLY removal (winner and mirror unchanged),
 * teardown, and entry reclaim each advance it exactly once — and the whole
 * sequence replays to the identical epoch. */
static int
journal_epoch_scenario(ca_t *a, uint64_t *final_epoch)
{
    int failures = 0;
    moqr_shards_t *s = mk_shards(a, 3, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    /* A namespace whose HRW winner over {1,2} is shard 1, announced on
     * BOTH 1 and 2 in the same round: the observer (shard 0) records two
     * candidates and installs the winner's mirror; shard 2 then loses,
     * force-withdraws, and its retraction reaches shard 0 as a
     * candidate-only removal — the winner and mirror do not move. */
    char nsn[32];
    find_ns(nsn, sizeof(nsn), "je", BIT(1) | BIT(2), 1);
    MOQ_TEST_CHECK(nsn[0] != '\0');
    moqr_binding_t p1 = pub_open(s, 1);
    moqr_binding_t p2 = pub_open(s, 2);
    MOQ_TEST_CHECK(ann(s, 1, p1, nsn) == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 2, p2, nsn) == MOQR_OK);

    moqr_shards_stats_t st;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st.journal_epoch, 0);

    step_n(s, 8);   /* converge: dual announce, loser withdrawn */
    moqr_shards_jinfo_t j;
    jinfo(s, 0, nsn, &j);
    MOQ_TEST_CHECK(j.present);
    MOQ_TEST_CHECK_EQ_U64(j.candidates, BIT(1));
    MOQ_TEST_CHECK_EQ_INT(j.winner, 1);
    MOQ_TEST_CHECK_EQ_INT(j.mirror, 1);
    /* Shard 0's converged total: two candidate arrivals (+2), the mirror
     * install (+1), and the loser's candidate-only removal (+1) — no
     * mirror churn, no winner churn, exactly 4. */
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st.journal_epoch, 4);

    /* Withdraw the winner: candidate removal (+1), mirror teardown (+1),
     * and the fully-quiesced entry's reclaim (+1). */
    MOQ_TEST_CHECK(unann(s, 1, p1, nsn) == MOQR_OK);
    step_n(s, 8);
    jinfo(s, 0, nsn, &j);
    MOQ_TEST_CHECK(!j.present);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st.journal_epoch, 7);
    if (final_epoch != NULL) {
        *final_epoch = st.journal_epoch;
    }
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a->live, 0);
    return failures;
}

static int
test_shards_stats_journal_epoch(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    uint64_t e1 = 0, e2 = 0;
    failures += journal_epoch_scenario(&a, &e1);
    failures += journal_epoch_scenario(&a, &e2);
    MOQ_TEST_CHECK_EQ_U64(e1, e2);   /* deterministic replay: identical */
    MOQ_TEST_PASS("shards_stats_journal_epoch");
    return failures;
}

/* The DETERMINISTIC runner records the same mask-level wake causes as the
 * per-shard seam: driven exclusively through moqr_shards_step, two data
 * messages to one destination in the owner's round are ONE push request,
 * and the consumer's next-round double pop is ONE credit request — the
 * counters a deterministic benchmark harness asserts under this runner. */
static int
test_shards_stats_wake_deterministic(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards_admit(&a, 2, 0);   /* seed 0: ascending */
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p0 = pub_open(s, 0);
    MOQ_TEST_CHECK(ann(s, 0, p0, "obD") == MOQR_OK);
    moqr_track_t ot;
    MOQ_TEST_CHECK(pub_track_h(s, 0, p0, "obD", &ot) == MOQR_OK);
    step_n(s, 8);
    moqr_binding_t d1 = sub_open(s, 1, 2);
    moqr_sub_t sub;
    MOQ_TEST_CHECK(do_subscribe(s, 1, d1, "obD", 7, &sub) == MOQR_OK);
    step_n(s, 6);   /* ACKED, quiesced */

    moqr_shards_stats_t st0, st1;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    uint64_t bp0 = st0.wake_requests_push, bc0 = st0.wake_requests_credit;
    uint64_t bp1 = st1.wake_requests_push, bc1 = st1.wake_requests_credit;

    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 0, 0xD1, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    MOQ_TEST_CHECK(pub_obj(s, &a, 0, ot, 0, 0, 1, 0xD2, 8, 0, 0, 0, false,
                           false) == MOQR_OK);
    /* Round r: the owner (stepped first, ascending order) pushes BOTH
     * objects toward shard 1 — one destination, ONE push request; the
     * barrier hides them from shard 1 until next round, so no credits. */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st0.wake_requests_push, bp0 + 1);
    MOQ_TEST_CHECK_EQ_U64(st0.wake_requests_credit, bc0);
    MOQ_TEST_CHECK_EQ_U64(st1.wake_requests_push, bp1);
    MOQ_TEST_CHECK_EQ_U64(st1.wake_requests_credit, bc1);
    /* Round r+1: shard 1 drains both messages — one channel regained
     * capacity, ONE credit request; nothing new pushes anywhere. */
    MOQ_TEST_CHECK(moqr_shards_step(s, 1000) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st0) == MOQR_OK);
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 1, &st1) == MOQR_OK);
    MOQ_TEST_CHECK_EQ_U64(st0.wake_requests_push, bp0 + 1);
    MOQ_TEST_CHECK_EQ_U64(st1.wake_requests_credit, bc1 + 1);
    MOQ_TEST_CHECK_EQ_U64(st1.wake_requests_push, bp1);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_stats_wake_deterministic");
    return failures;
}

/* Rendering oracle for the journal dump's part escaping — the test's own
 * independent implementation of the promised format. */
static size_t
jdump_expect_part(const uint8_t *p, size_t n, char *out, size_t cap)
{
    size_t o = 0;
    o += (size_t)snprintf(out + o, cap - o, "[%u]\"", (unsigned)n);
    for (size_t i = 0; i < n; i++) {
        uint8_t b = p[i];
        if (b == (uint8_t)'\\' || b == (uint8_t)'"') {
            o += (size_t)snprintf(out + o, cap - o, "\\%c", (char)b);
        } else if (b >= 0x20u && b <= 0x7Eu) {
            o += (size_t)snprintf(out + o, cap - o, "%c", (char)b);
        } else {
            o += (size_t)snprintf(out + o, cap - o, "\\x%02x", (unsigned)b);
        }
    }
    o += (size_t)snprintf(out + o, cap - o, "\"");
    return o;
}

/* The owning-lane journal dump: header + shard-local epoch, entries in
 * ascending CANONICAL order, and a maximum-size binary namespace rendered
 * length-delimited and lossless — never a C string, never a prefix. */
static int
test_shards_stats_journal_dump(void)
{
    int failures = 0;
    ca_t a;
    ca_init(&a);
    moqr_shards_t *s = mk_shards(&a, 2, 0, 0);
    MOQ_TEST_CHECK(s != NULL);
    moqr_binding_t p1 = pub_open(s, 1);
    /* Canonical order is over the ENCODED key ([count][len][bytes]), so
     * for 1-part namespaces the SHORTER name sorts first: "zz" precedes
     * "aaa" despite byte order. */
    MOQ_TEST_CHECK(ann(s, 1, p1, "zz") == MOQR_OK);
    MOQ_TEST_CHECK(ann(s, 1, p1, "aaa") == MOQR_OK);
    /* A maximum-size BINARY part: every byte value cycles through, with
     * quote/backslash/NUL/newline all present, at the full shared 4096-byte
     * namespace cap. */
    static uint8_t big[4096];
    for (size_t i = 0; i < sizeof(big); i++) {
        big[i] = (uint8_t)(i * 7u + 3u);
    }
    big[0] = 0x00;
    big[1] = (uint8_t)'"';
    big[2] = (uint8_t)'\\';
    big[3] = (uint8_t)'\n';
    big[4] = 0xFF;
    {
        moq_bytes_t part = { big, sizeof(big) };
        moqr_ns_t ns = { &part, 1 };
        MOQ_TEST_CHECK(moqr_core_announce(moqr_shards_core(s, 1), p1, ns) ==
                       MOQR_OK);
    }
    step_n(s, 8);   /* shard 0 mirrors all three */

    size_t need = 0;
    char tiny[16];
    MOQ_TEST_CHECK(moqr_shards_journal_dump_text(s, 0, tiny, sizeof(tiny),
                                                 &need) ==
                   MOQR_ERR_CAPACITY);
    MOQ_TEST_CHECK(need > 0);
    MOQ_TEST_CHECK(tiny[sizeof(tiny) - 1] == '\0');
    char *buf = malloc(need + 1);
    MOQ_TEST_CHECK(buf != NULL);
    size_t w = 0;
    MOQ_TEST_CHECK(moqr_shards_journal_dump_text(s, 0, buf, need + 1, &w) ==
                   MOQR_OK);
    MOQ_TEST_CHECK_EQ_SIZE(w, need);
    MOQ_TEST_CHECK_EQ_SIZE(strlen(buf), w);

    MOQ_TEST_CHECK(strstr(buf, "shard 0\n") == buf);
    moqr_shards_stats_t st;
    MOQ_TEST_CHECK(moqr_shards_get_stats(s, 0, &st) == MOQR_OK);
    char epoch_line[64];
    snprintf(epoch_line, sizeof(epoch_line), "journal_epoch=%llu\n",
             (unsigned long long)st.journal_epoch);
    MOQ_TEST_CHECK(strstr(buf, epoch_line) != NULL);

    /* Ascending canonical order: "zz" (len 2) before "aaa" (len 3). */
    const char *zz = strstr(buf, "ns 1:[2]\"zz\" candidates=0x2 winner=1 "
                                 "mirror=1");
    const char *aaa = strstr(buf, "ns 1:[3]\"aaa\" candidates=0x2 winner=1 "
                                  "mirror=1");
    MOQ_TEST_CHECK(zz != NULL && aaa != NULL);
    MOQ_TEST_CHECK(zz < aaa);

    /* The max-size binary key renders WHOLE and lossless: the full
     * expected escape (built independently here) appears verbatim, with
     * its raw length delimiter. */
    size_t expcap = sizeof(big) * 4u + 32u;
    char *expect = malloc(expcap);
    MOQ_TEST_CHECK(expect != NULL);
    (void)jdump_expect_part(big, sizeof(big), expect, expcap);
    MOQ_TEST_CHECK(strstr(expect, "[4096]\"") == expect);
    MOQ_TEST_CHECK(strstr(buf, expect) != NULL);
    free(expect);
    free(buf);

    /* K == 1: header + zero epoch + no entries (structurally inert). */
    moqr_shards_t *s1 = mk_shards(&a, 1, 0, 0);
    MOQ_TEST_CHECK(s1 != NULL);
    char small[128];
    MOQ_TEST_CHECK(moqr_shards_journal_dump_text(s1, 0, small, sizeof(small),
                                                 &w) == MOQR_OK);
    MOQ_TEST_CHECK(strcmp(small,
                          "shard 0\njournal_epoch=0\n  (none)\n") == 0);
    moqr_shards_destroy(s1);
    moqr_shards_destroy(s);
    MOQ_TEST_CHECK_EQ_INT((int)a.live, 0);
    MOQ_TEST_PASS("shards_stats_journal_dump");
    return failures;
}

int
main(void)
{
    int failures = 0;
    failures += test_shards_lifecycle_k1();
    failures += test_shards_prefix_safe();
    failures += test_shards_k3_isolation();
    failures += test_shards_determinism();
    failures += test_shards_placement();
    failures += test_shards_step_seam();
    failures += test_shards_broadcast();
    failures += test_shards_remote_vs_remote();
    failures += test_shards_late_local();
    failures += test_shards_mailbox_full();
    failures += test_shards_collapse();
    failures += test_shards_same_round_batch();
    failures += test_shards_split_brain();
    failures += test_shards_loser_would_block_retry();
    failures += test_shards_forcewithdraw_mirror_noop();
    failures += test_shards_cross_ns_order();
    failures += test_shards_reconcile_would_block();
    failures += test_shards_inbound_saturation_durable();
    failures += test_shards_local_drop_failstops();
    failures += test_shards_mailbox_coalesce();
    failures += test_shards_demand_local_owner();
    failures += test_shards_demand_remote_refused();
    failures += test_shards_demand_per_attempt();
    failures += test_shards_demand_failstop();
    failures += test_shards_demand_would_block_retry();
    failures += test_shards_demand_stale_moot();
    failures += test_shards_demand_cancel_before_sent();
    failures += test_shards_demand_cancel_after_sent();
    failures += test_shards_demand_channel_full();
    failures += test_shards_demand_mailbox_isolated();
    failures += test_shards_admit_ack_lifecycle();
    failures += test_shards_admit_reject_code();
    failures += test_shards_admit_sub_done_code();
    failures += test_shards_admit_cancel_before_ack();
    failures += test_shards_admit_cancel_after_ack();
    failures += test_shards_admit_undemand_staged_retry();
    failures += test_shards_admit_retarget();
    failures += test_shards_admit_demand_id_collision();
    failures += test_shards_admit_reply_channel_full();
    failures += test_shards_done_pd_crossing_d18_64bit();
    failures += test_shards_done_pd_crossing_d16_64bit();
    failures += test_shards_done_pd_pre_ack_is_none();
    failures += test_shards_data_obj_delivery();
    failures += test_shards_data_parity();
    failures += test_shards_data_ack_order();
    failures += test_shards_data_channel_lag();
    failures += test_shards_data_byte_cap_cfg();
    failures += test_shards_data_byte_gate();
    failures += test_shards_data_capacity_terminal();
    failures += test_shards_data_unsupported_hold();
    failures += test_shards_data_clone_survives_evict();
    failures += test_shards_data_destroy_queued();
    failures += test_shards_data_producer_oom();
    failures += test_shards_data_requester_oom();
    failures += test_shards_data_turn_budget();
    failures += test_shards_turn_outcomes();
    failures += test_shards_turn_outcome_precedence();
    failures += test_shards_pair_refusal_split();
    failures += test_shards_pair_identities();
    failures += test_shards_data_oversize_first();
    failures += test_shards_data_arbiter();
    failures += test_shards_data_inert();
    failures += test_shards_chunk_delivery();
    failures += test_shards_chunk_parity();
    failures += test_shards_chunk_live_edge();
    failures += test_shards_chunk_cap1();
    failures += test_shards_chunk_turn_budget();
    failures += test_shards_chunk_eog_header();
    failures += test_shards_chunk_slot_exhaust();
    failures += test_shards_chunk_requester_abandon();
    failures += test_shards_chunk_terminal_staged_wb();
    failures += test_shards_chunk_done_behind_trigger();
    failures += test_shards_chunk_done_abandons_open();
    failures += test_shards_chunk_too_old_discard();
    failures += test_shards_term_seal_cap1();
    failures += test_shards_term_eog_vs_seal();
    failures += test_shards_term_evict_watermark();
    failures += test_shards_ready_set_k2();
    failures += test_shards_term_reset_code_wide();
    failures += test_shards_term_group_evict_reset();
    failures += test_shards_term_group_reset_mixed();
    failures += test_shards_term_zero_byte_budget();
    failures += test_shards_term_destroy_queued();
    failures += test_shards_term_cancel_matrix();
    failures += test_shards_term_collision_k3();
    failures += test_shards_term_retarget_open();
    failures += test_shards_term_parity_reorder();
    failures += test_shards_term_credit_wake();
    failures += test_shards_cap_poisoned_prefix();
    failures += test_shards_cap_binding_guard();
    failures += test_shards_cap_k1_admission_inert();
    failures += test_shards_cap_admission_delta();
    failures += test_shards_cap_peak_oracle();
    failures += test_shards_cap_term_deltas();
    failures += test_shards_cfg_progress_overflow();
    failures += test_shards_chunk_producer_oom();
    failures += test_shards_chunk_destroy_queued();
    failures += test_shards_demand_k1_inert();
    failures += test_shards_stats_enqueue_counts();
    failures += test_shards_stats_attempt_not_counted();
    failures += test_shards_stats_wake_masks();
    failures += test_shards_stats_wake_deterministic();
    failures += test_shards_stats_channel_hwm();
    failures += test_shards_stats_gauges();
    failures += test_shards_stats_k1_inert();
    failures += test_shards_stats_journal_epoch();
    failures += test_shards_stats_journal_dump();
    if (failures == 0) {
        printf("ALL PASS\n");
    }
    return failures == 0 ? 0 : 1;
}
