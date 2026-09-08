#ifndef MOQR_SCHED_GEN_H
#define MOQR_SCHED_GEN_H

/*
 * Seed derivation, configuration hash, and the seeded trace generator.
 * PURE — a trace's bytes are a function of (seed, grammar version,
 * configuration constants) alone. The generator draws only from the model's
 * currently-enabled set, so a generated trace contains no
 * precondition-failing record; STOP terminates the trace.
 */

#include "../seedx/seedx_prng.h"
#include "sched_model.h"
#include "sched_sha256.h"

#include <stdio.h>

/* -- committed seed provenance ------------------------------------------- *
 * seed(i) = the FIRST 8 BYTES of SHA-256("moq-relay-sched-v1/<i>") read as a
 * BIG-ENDIAN u64 — i is the bare decimal index, no padding. Mechanical, no
 * hand-picking; documented in the committed manifest header.
 */

#define SCHED_SEED_NAMESPACE "moq-relay-sched-v1"
#define SCHED_SEED_COUNT     16u

static inline uint64_t sched_seed(uint32_t index)
{
    char msg[64];
    uint8_t d[32];
    int n = snprintf(msg, sizeof(msg), "%s/%u", SCHED_SEED_NAMESPACE, index);

    sched_sha256_of(msg, (size_t)n, d);
    uint64_t v = 0;

    for (int i = 0; i < 8; i++) {
        v = (v << 8) | d[i];
    }
    return v;
}

/* -- rig namespace strings (configuration; ownership asserted in the rig) */

#define SCHED_NS0_PART "sched-ns-3" /* placed on shard 0 under K=2 HRW */
#define SCHED_NS1_PART "sched-ns-0" /* placed on shard 1 under K=2 HRW */

/* FNV-1a/64 over every rig constant the traces depend on. */
static inline uint64_t sched_config_hash(void)
{
    static const char cfg[] =
        "sched-v1"
        " grammar=3 cff=1 srcterm=1"
        " lanes=2 children=16 live=4 ns=2 credit=2 ops=256"
        " ns0=" SCHED_NS0_PART " ns1=" SCHED_NS1_PART
        " prng=splitmix64-v1 trace=1 draft=18";
    uint64_t h = 0xcbf29ce484222325ull;

    for (size_t i = 0; i + 1 < sizeof(cfg); i++) {
        h ^= (uint8_t)cfg[i];
        h *= 0x100000001b3ull;
    }
    return h;
}

/* -- generation ------------------------------------------------------------ *
 * Fills recs (cap >= SCHED_OPS_PER_SEED) and the final model state; returns
 * the effective operation count (STOP included when drawn).
 */

static inline uint32_t
sched_generate(uint64_t seed, seedx_rec_t *recs, sched_model_t *final_model)
{
    seedx_prng_t rng;
    sched_model_t m;
    sched_op_t enabled[SCHED_ENABLED_MAX];
    uint32_t count = 0;

    seedx_prng_init(&rng, seed);
    sched_model_init(&m);
    for (uint32_t i = 0; i < SCHED_OPS_PER_SEED; i++) {
        int n = sched_enabled(&m, i, enabled, SCHED_ENABLED_MAX);

        if (n <= 0 || n > SCHED_ENABLED_MAX) {
            break; /* stopped (or overflow, which generation must not hit) */
        }
        uint64_t pick = seedx_draw(&rng, (uint64_t)n);
        const sched_op_t *op = &enabled[pick];

        if (!sched_model_apply(&m, op)) {
            break; /* unreachable by construction; fail closed */
        }
        recs[count].op = op->op;
        recs[count].lane = op->lane;
        recs[count].a = op->a;
        recs[count].b = op->b;
        recs[count].c = op->c;
        count++;
        if (op->op == SCHED_OP_STOP) {
            break;
        }
    }
    if (final_model != NULL) {
        *final_model = m;
    }
    return count;
}

/* Convenience: generate + encode into canonical bytes. Returns byte size. */
static inline size_t
sched_generate_trace(uint64_t seed, uint8_t *out, size_t cap,
                     uint32_t *op_count, sched_model_t *final_model)
{
    seedx_rec_t recs[SCHED_OPS_PER_SEED];
    uint32_t n = sched_generate(seed, recs, final_model);

    if (op_count != NULL) {
        *op_count = n;
    }
    return seedx_trace_encode(seed, sched_config_hash(), recs, n, out, cap);
}

#endif /* MOQR_SCHED_GEN_H */
