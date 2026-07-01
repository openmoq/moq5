#ifndef MOQ_TEST_MEDIA_FUZZ_H
#define MOQ_TEST_MEDIA_FUZZ_H

/*
 * Seeded property-test driver for the media formats (test-only).
 *
 * Reuses the repo's scenario conventions: the MOQ_SCENARIO_* env knobs, an
 * inline splitmix64 PRNG, and the canonical "Replay:" line. This is SEEDED
 * GENERATION of valid models plus (in later slices) structured mutation of the
 * encoded bytes -- a property/seeded-mutation layer, NOT coverage-guided
 * fuzzing (that lives in fuzz/ under libFuzzer).
 *
 * Layout: this core TU (media_fuzz.c) owns the PRNG, env config, replay
 * printer, and the per-case counting allocator; each format's generator +
 * oracle lives in its own TU (media_fuzz_loc.c, media_fuzz_msf.c, ...) so a
 * runner links only the format libraries it exercises. A thin scenario runner
 * parses the env config and loops seeds, calling one moq_fuzz_<fmt>_run_seed().
 */

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <moq/types.h>   /* moq_alloc_t */

#ifdef __cplusplus
extern "C" {
#endif

/* splitmix64 PRNG (scenario convention): the seed fully determines the
 * sequence, so a failing seed replays bit-for-bit. */
typedef struct { uint64_t s; } moq_fuzz_rng_t;
uint64_t moq_fuzz_rng_next(moq_fuzz_rng_t *r);
uint64_t moq_fuzz_rng_below(moq_fuzz_rng_t *r, uint64_t n); /* [0,n); 0 if n==0 */
bool     moq_fuzz_rng_bool(moq_fuzz_rng_t *r);

/* Seeded-run config from MOQ_SCENARIO_SEED_START / _SEEDS / _STEPS. The
 * defaults apply only when an env var is absent (the light ctest workload). */
typedef struct {
    uint64_t seed_start;
    uint64_t seeds;
    int      steps;            /* cases per seed */
    uint32_t mutate_permille;  /* 0 = generate-only (ctest default); >0 = each
                                  case also runs one byte-mutation with this
                                  per-mille probability */
} moq_fuzz_cfg_t;
void moq_fuzz_cfg_from_env(moq_fuzz_cfg_t *cfg,
                           uint64_t default_seeds, int default_steps);

/* Print the canonical replay line to stderr for a failing seed; includes
 * MOQ_SCENARIO_MUTATE_PERMILLE only when nonzero. */
void moq_fuzz_print_replay(const char *argv0, uint64_t seed, int steps,
                           uint32_t mutate_permille);

/* Deterministic per-mille gate. IMPORTANT: when permille == 0 this draws NO
 * randomness and returns false, so the generate-only sequence is byte-identical
 * to the pre-mutation runners (existing seeds reproduce). */
bool moq_fuzz_hit_permille(moq_fuzz_rng_t *rng, uint32_t permille);

/* Flip one deterministically-chosen bit of one byte in buf[0,len). */
void moq_fuzz_flip_byte(uint8_t *buf, size_t len, moq_fuzz_rng_t *rng);

/* Per-case counting allocator (leak balance). Build a moq_alloc_t backed by a
 * caller-owned state; after a case, st->balance must be 0. */
typedef struct { int64_t balance; } moq_fuzz_alloc_state_t;
void moq_fuzz_alloc_init(moq_alloc_t *out, moq_fuzz_alloc_state_t *st);

/* Per-format runs: execute one seed's `steps` cases against a per-case counting
 * allocator. Return the failure count; print FAIL details + one replay line
 * (using argv0) when nonzero. */
int moq_fuzz_loc_run_seed(uint64_t seed, int steps, uint32_t mutate_permille,
                          const char *argv0);
int moq_fuzz_msf_run_seed(uint64_t seed, int steps, uint32_t mutate_permille,
                          const char *argv0);
/* CMAF validation is stack-only (no allocator), so this run has no leak
 * balance to check -- unlike loc/msf. */
int moq_fuzz_cmaf_run_seed(uint64_t seed, int steps, uint32_t mutate_permille,
                           const char *argv0);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_TEST_MEDIA_FUZZ_H */
