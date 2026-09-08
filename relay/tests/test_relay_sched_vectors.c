/*
 * The trace-vector unit: proves the committed 16-seed set is exactly what
 * the pure generator produces, independent of compiler and platform.
 *
 *   (default)          derive the 16 seeds, generate every trace TWICE and
 *                      require byte-identical output, hash each trace, and
 *                      compare seed/op-count/sha256 against the committed
 *                      manifest — any drift is a loud failure, never a new
 *                      manifest;
 *   --emit-manifest    print the manifest to stdout (the only way the
 *                      committed file is produced);
 *   --print-trace <i>  render seed i's trace (human, never parsed back).
 *
 * PURE: model + generator + codec only. No adapter, no rig — this is the
 * unit built under both clang and gcc to pin compiler-independence of the
 * committed trace bytes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sched/sched_gen.h"

static int g_failures;

#define T_CHECK(expr)                                                      \
    do {                                                                   \
        if (!(expr)) {                                                     \
            g_failures++;                                                  \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);        \
        }                                                                  \
    } while (0)

#define TRACE_CAP (SEEDX_TRACE_HDR + SCHED_OPS_PER_SEED * SEEDX_TRACE_REC)


/* The established-track-source model unit: a namespace withdrawal removes
 * the advertisement but neither parks nor terminates an ESTABLISHED
 * downstream track, and a fresh announcement by another publisher never
 * enables PUSH by itself — the source identity survives until a real
 * track-source transition (last cancel, or revocation of the source). */
static bool enabled_has(const sched_model_t *m, uint32_t idx, uint8_t op,
                        uint16_t a)
{
    sched_op_t en[SCHED_ENABLED_MAX];
    int n = sched_enabled(m, idx, en, SCHED_ENABLED_MAX);

    for (int i = 0; i < n && i < SCHED_ENABLED_MAX; i++) {
        if (en[i].op == op && en[i].a == a) {
            return true;
        }
    }
    return false;
}

static int model_source_unit(void)
{
    int before = g_failures;
    sched_model_t m;
    sched_op_t op;

    sched_model_init(&m);
#define APPLY(o, l, aa, bb)                                                \
    do {                                                                   \
        op = (sched_op_t){ (o), (l), (aa), (bb), 0 };                      \
        T_CHECK(sched_model_apply(&m, &op));                               \
    } while (0)
    /* A (child 0, lane 0) announces ns0; S (child 1, lane 1) establishes */
    APPLY(SCHED_OP_ACCEPT, 0, 0, 0);
    APPLY(SCHED_OP_ACCEPT, 1, 1, 0);
    APPLY(SCHED_OP_ESTABLISH, 0, 0, 0);
    APPLY(SCHED_OP_ESTABLISH, 1, 1, 0);
    APPLY(SCHED_OP_ANNOUNCE, 0, 0, 0);
    APPLY(SCHED_OP_SUBSCRIBE, 1, 1, 0);
    T_CHECK(m.src_child[0] == 0);
    T_CHECK(enabled_has(&m, 10, SCHED_OP_PUSH, 0));

    /* A withdraws: the advertisement goes, the established track stays */
    APPLY(SCHED_OP_WITHDRAW, 0, 0, 0);
    T_CHECK(m.src_child[0] == 0);
    T_CHECK(m.child[1].sub_ns == 0 && m.child[1].sub_accepted);

    /* B (child 2, lane 0) announces the same namespace: winner changes,
     * source does not — and PUSH by B must NOT be enabled */
    APPLY(SCHED_OP_ACCEPT, 0, 2, 0);
    APPLY(SCHED_OP_ESTABLISH, 0, 2, 0);
    APPLY(SCHED_OP_ANNOUNCE, 0, 2, 0);
    T_CHECK(m.ns_pub[0] == 2);
    T_CHECK(m.src_child[0] == 0);
    T_CHECK(!enabled_has(&m, 12, SCHED_OP_PUSH, 2));
    op = (sched_op_t){ SCHED_OP_PUSH, 0, 2, 0, 0 };
    T_CHECK(!sched_model_apply(&m, &op));
    /* the standing source no longer holds the advertisement: not pushable
     * from the seeded grammar either (the conservative AND) */
    T_CHECK(!enabled_has(&m, 12, SCHED_OP_PUSH, 0));
    /* grammar v3: a standing source IS terminal-eligible — the failover
     * contract retargets its demand to the current winner (B here) */
    T_CHECK(enabled_has(&m, 12, SCHED_OP_TERMINAL, 0));
    {
        sched_op_t tm = { SCHED_OP_TERMINAL, 0, 0, 0, 0 };
        sched_model_t saved = m;

        T_CHECK(sched_model_apply(&m, &tm));
        T_CHECK(m.src_child[0] == 2);          /* retargeted to B */
        T_CHECK(m.child[1].sub_ns == 0);       /* subscriber survives */
        m = saved;                             /* continue the walk */
    }

    /* the real track-source transition: the last subscriber cancels; only
     * then does a fresh establishment select B */
    APPLY(SCHED_OP_CANCEL, 1, 1, 0);
    T_CHECK(m.src_child[0] == -1);
    APPLY(SCHED_OP_SUBSCRIBE, 1, 1, 0);
    T_CHECK(m.src_child[0] == 2);
    T_CHECK(enabled_has(&m, 16, SCHED_OP_PUSH, 2));
#undef APPLY
    if (g_failures == before) {
        printf("PASS: sched_model_source_unit\n");
    }
    return g_failures - before;
}

static const char *manifest_path(void)
{
    const char *p = getenv("SCHED_MANIFEST");

    return p != NULL ? p : "relay/tests/vectors/sched_seeds_v1.txt";
}

static void emit_manifest(void)
{
    printf("# sched_seeds_v1 — committed seed/trace manifest\n");
    printf("# derivation: seed(i) = first 8 bytes of SHA-256(\"%s/<i>\"),\n",
           SCHED_SEED_NAMESPACE);
    printf("#   big-endian u64; i is the bare decimal index 0..%u\n",
           SCHED_SEED_COUNT - 1u);
    printf("# prng=splitmix64-v1 trace=v1 grammar=%u config_hash=%016llx\n",
           SCHED_GRAMMAR_VERSION,
           (unsigned long long)sched_config_hash());
    printf("# columns: index seed_hex effective_ops trace_sha256\n");
    for (uint32_t i = 0; i < SCHED_SEED_COUNT; i++) {
        uint8_t buf[TRACE_CAP];
        uint32_t ops = 0;
        size_t len = sched_generate_trace(sched_seed(i), buf, sizeof(buf),
                                          &ops, NULL);
        uint8_t d[32];
        char hex[65];

        sched_sha256_of(buf, len, d);
        sched_sha256_hex(d, hex);
        printf("%u %016llx %u %s\n", i,
               (unsigned long long)sched_seed(i), ops, hex);
    }
}

static int verify_against_manifest(void)
{
    int before = g_failures;
    FILE *f = fopen(manifest_path(), "r");

    if (f == NULL) {
        printf("FAIL: manifest not found: %s\n", manifest_path());
        g_failures++;
        return g_failures - before;
    }
    char line[256];
    uint64_t want_cfg = 0;
    bool cfg_seen = false;
    uint32_t rows = 0;
    uint64_t total_ops = 0;

    while (fgets(line, sizeof(line), f) != NULL) {
        if (line[0] == '#') {
            const char *p = strstr(line, "config_hash=");

            if (p != NULL) {
                want_cfg = strtoull(p + 12, NULL, 16);
                cfg_seen = true;
            }
            continue;
        }
        unsigned idx = 0, ops = 0;
        unsigned long long seed = 0;
        char hex[80];

        if (sscanf(line, "%u %llx %u %79s", &idx, &seed, &ops, hex) != 4) {
            printf("FAIL: malformed manifest row: %s", line);
            g_failures++;
            continue;
        }
        T_CHECK(idx < SCHED_SEED_COUNT);
        if (idx >= SCHED_SEED_COUNT) {
            continue;
        }
        T_CHECK(sched_seed(idx) == (uint64_t)seed);

        /* generate twice; byte-identical is required, not observed */
        uint8_t b1[TRACE_CAP], b2[TRACE_CAP];
        uint32_t n1 = 0, n2 = 0;
        size_t l1 = sched_generate_trace((uint64_t)seed, b1, sizeof(b1),
                                         &n1, NULL);
        size_t l2 = sched_generate_trace((uint64_t)seed, b2, sizeof(b2),
                                         &n2, NULL);

        T_CHECK(l1 > 0 && l1 == l2 && n1 == n2);
        T_CHECK(memcmp(b1, b2, l1) == 0);
        T_CHECK(n1 == ops);
        total_ops += n1;

        uint8_t d[32];
        char got[65];

        sched_sha256_of(b1, l1, d);
        sched_sha256_hex(d, got);
        if (strcmp(got, hex) != 0) {
            printf("FAIL: seed %u trace sha256 drift\n  manifest %s\n"
                   "  built    %s\n", idx, hex, got);
            g_failures++;
        }

        /* the strict decoder must accept its own canonical bytes */
        seedx_rec_t arena[SCHED_OPS_PER_SEED];
        seedx_trace_t t;

        T_CHECK(seedx_trace_decode(b1, l1, sched_config_hash(),
                                   SCHED_OP_MAX, sched_field_ok, arena,
                                   SCHED_OPS_PER_SEED, &t) ==
                SEEDX_TRACE_OK);
        rows++;
    }
    fclose(f);
    T_CHECK(cfg_seen && want_cfg == sched_config_hash());
    T_CHECK(rows == SCHED_SEED_COUNT);
    /* the declared minimum aggregate effective-operation count: early
     * STOPs cannot make the committed set vacuous */
    T_CHECK(total_ops >= 3000);
    if (g_failures == before) {
        printf("PASS: sched_vectors (%u seeds, %llu effective ops)\n",
               rows, (unsigned long long)total_ops);
    }
    return g_failures - before;
}

static void print_trace(uint32_t idx)
{
    uint8_t buf[TRACE_CAP];
    uint32_t ops = 0;
    size_t len = sched_generate_trace(sched_seed(idx), buf, sizeof(buf),
                                      &ops, NULL);
    seedx_rec_t arena[SCHED_OPS_PER_SEED];
    seedx_trace_t t;

    if (seedx_trace_decode(buf, len, sched_config_hash(), SCHED_OP_MAX,
                           sched_field_ok, arena, SCHED_OPS_PER_SEED,
                           &t) != SEEDX_TRACE_OK) {
        printf("decode failed\n");
        return;
    }
    char out[SCHED_OPS_PER_SEED * 48u];
    size_t n = seedx_trace_render(&t, out, sizeof(out));

    fwrite(out, 1, n, stdout);
}

int main(int argc, char **argv)
{
    if (argc >= 2 && strcmp(argv[1], "--emit-manifest") == 0) {
        emit_manifest();
        return 0;
    }
    if (argc >= 3 && strcmp(argv[1], "--print-trace") == 0) {
        print_trace((uint32_t)strtoul(argv[2], NULL, 0));
        return 0;
    }
    (void)model_source_unit();
    (void)verify_against_manifest();
    return g_failures == 0 ? 0 : 1;
}
