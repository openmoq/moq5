#ifndef MOQR_SEEDX_PRNG_H
#define MOQR_SEEDX_PRNG_H

/*
 * splitmix64-v1: the explorer's one randomness source. Pure 64-bit constants
 * (Steele/Lea/Vigna), platform-stable, never libc rand(). Bounded draws are
 * lemire-v1 — the complete rejection form, so results are unbiased AND the
 * number of raw draws a bounded draw consumes is part of the replayable
 * contract (a rejection consumed on one platform is consumed on every
 * platform). Golden vectors: relay/tests/vectors/prng_splitmix64_v1.txt,
 * generated
 * from the reference algorithm before this header existed.
 */

#include <stdint.h>

typedef struct seedx_prng {
    uint64_t state;
    uint64_t raw_draws; /* lifetime raw outputs, part of the replay contract */
} seedx_prng_t;

static inline void
seedx_prng_init(seedx_prng_t *p, uint64_t seed)
{
    p->state = seed;
    p->raw_draws = 0;
}

static inline uint64_t
seedx_next(seedx_prng_t *p)
{
    p->state += 0x9E3779B97F4A7C15ull;
    p->raw_draws++;
    uint64_t z = p->state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
    return z ^ (z >> 31);
}

/* 64x64 -> 128 multiply. __uint128_t where available; otherwise the four
 * 32x32 partial products, which is bit-identical by construction. */
static inline void
seedx_mul128(uint64_t a, uint64_t b, uint64_t *hi, uint64_t *lo)
{
#if defined(__SIZEOF_INT128__)
    __uint128_t m = (__uint128_t)a * b;
    *hi = (uint64_t)(m >> 64);
    *lo = (uint64_t)m;
#else
    uint64_t a_lo = (uint32_t)a, a_hi = a >> 32;
    uint64_t b_lo = (uint32_t)b, b_hi = b >> 32;
    uint64_t p0 = a_lo * b_lo;
    uint64_t p1 = a_lo * b_hi;
    uint64_t p2 = a_hi * b_lo;
    uint64_t p3 = a_hi * b_hi;
    uint64_t mid = (p0 >> 32) + (uint32_t)p1 + (uint32_t)p2;
    *lo = (mid << 32) | (uint32_t)p0;
    *hi = p3 + (p1 >> 32) + (p2 >> 32) + (mid >> 32);
#endif
}

/*
 * lemire-v1 bounded draw in [0, n). n == 0 is a CALLER error: the explorer's
 * trace layer rejects it before ever reaching here (callers assert).
 *
 *   m = x * n (128-bit)
 *   if lo64(m) < n:            -- possible bias region
 *       t = (2^64 - n) mod n   -- computed as (0 - n) % n in uint64
 *       while lo64(m) < t: x = next(); m = x * n   -- rejection loop
 *   return hi64(m)
 */
static inline uint64_t
seedx_draw(seedx_prng_t *p, uint64_t n)
{
    uint64_t hi, lo;
    uint64_t x = seedx_next(p);

    seedx_mul128(x, n, &hi, &lo);
    if (lo < n) {
        uint64_t t = (0 - n) % n;

        while (lo < t) {
            x = seedx_next(p);
            seedx_mul128(x, n, &hi, &lo);
        }
    }
    return hi;
}

#endif /* MOQR_SEEDX_PRNG_H */
