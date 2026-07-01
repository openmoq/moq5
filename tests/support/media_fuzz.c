/*
 * Seeded media-format property tests -- shared core. See media_fuzz.h.
 *
 * Owns the PRNG, env config, the canonical replay printer, and the per-case
 * counting allocator. The per-format generators + oracles live in their own
 * TUs (media_fuzz_loc.c, media_fuzz_msf.c, ...) so each runner links only the
 * format libraries it exercises.
 */

#include "media_fuzz.h"

#include <stdio.h>
#include <stdlib.h>

/* -- splitmix64 PRNG (scenario convention) -------------------------------- */

uint64_t moq_fuzz_rng_next(moq_fuzz_rng_t *r)
{
    uint64_t z = (r->s += 0x9E3779B97F4A7C15ULL);
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
}

uint64_t moq_fuzz_rng_below(moq_fuzz_rng_t *r, uint64_t n)
{
    return n ? moq_fuzz_rng_next(r) % n : 0;
}

bool moq_fuzz_rng_bool(moq_fuzz_rng_t *r)
{
    return (moq_fuzz_rng_next(r) & 1u) != 0;
}

/* -- Env config ----------------------------------------------------------- */

void moq_fuzz_cfg_from_env(moq_fuzz_cfg_t *cfg,
                           uint64_t default_seeds, int default_steps)
{
    const char *s = getenv("MOQ_SCENARIO_SEEDS");
    const char *a = getenv("MOQ_SCENARIO_SEED_START");
    const char *p = getenv("MOQ_SCENARIO_STEPS");
    const char *m = getenv("MOQ_SCENARIO_MUTATE_PERMILLE");
    cfg->seeds = s ? (uint64_t)strtoull(s, NULL, 0) : default_seeds;
    cfg->seed_start = a ? (uint64_t)strtoull(a, NULL, 0) : 0;
    cfg->steps = p ? atoi(p) : default_steps;
    if (cfg->steps < 1) cfg->steps = 1;
    cfg->mutate_permille = m ? (uint32_t)strtoul(m, NULL, 0) : 0;
    if (cfg->mutate_permille > 1000) cfg->mutate_permille = 1000;
}

void moq_fuzz_print_replay(const char *argv0, uint64_t seed, int steps,
                           uint32_t mutate_permille)
{
    const char *bin = (argv0 && argv0[0]) ? argv0 : "<binary>";
    if (mutate_permille)
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d "
                "MOQ_SCENARIO_MUTATE_PERMILLE=%u %s\n",
                (unsigned long long)seed, steps, mutate_permille, bin);
    else
        fprintf(stderr, "  Replay: MOQ_SCENARIO_SEED_START=0x%llx "
                "MOQ_SCENARIO_SEEDS=1 MOQ_SCENARIO_STEPS=%d %s\n",
                (unsigned long long)seed, steps, bin);
}

bool moq_fuzz_hit_permille(moq_fuzz_rng_t *rng, uint32_t permille)
{
    if (permille == 0) return false;          /* no rng draw: preserves the
                                                 generate-only sequence */
    if (permille >= 1000) return true;
    return moq_fuzz_rng_below(rng, 1000) < permille;
}

void moq_fuzz_flip_byte(uint8_t *buf, size_t len, moq_fuzz_rng_t *rng)
{
    if (len == 0) return;
    size_t i = (size_t)moq_fuzz_rng_below(rng, len);
    buf[i] ^= (uint8_t)(1u << moq_fuzz_rng_below(rng, 8));
}

/* -- Counting allocator (per-case leak balance) --------------------------- */

static void *fz_alloc(size_t size, void *ctx)
{
    moq_fuzz_alloc_state_t *s = (moq_fuzz_alloc_state_t *)ctx;
    void *p = malloc(size);
    if (p) s->balance++;
    return p;
}
static void *fz_realloc(void *ptr, size_t old_sz, size_t new_sz, void *ctx)
{
    moq_fuzz_alloc_state_t *s = (moq_fuzz_alloc_state_t *)ctx;
    (void)old_sz;
    if (new_sz == 0) {
        /* realloc(ptr, 0) frees ptr -- count it as a free so the leak balance
         * stays honest if a parser/helper shrinks an allocation to 0. */
        if (ptr) s->balance--;
        free(ptr);
        return NULL;
    }
    if (!ptr) { void *r = realloc(NULL, new_sz); if (r) s->balance++; return r; }
    return realloc(ptr, new_sz);
}
static void fz_free(void *ptr, size_t size, void *ctx)
{
    moq_fuzz_alloc_state_t *s = (moq_fuzz_alloc_state_t *)ctx;
    (void)size;
    if (ptr) s->balance--;
    free(ptr);
}

void moq_fuzz_alloc_init(moq_alloc_t *out, moq_fuzz_alloc_state_t *st)
{
    out->ctx = st;
    out->alloc = fz_alloc;
    out->realloc = fz_realloc;
    out->free = fz_free;
}
