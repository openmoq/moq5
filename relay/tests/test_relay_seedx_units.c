/*
 * Phase-A foundations, unit-proven before any exploration exists:
 *
 *  - splitmix64-v1 + lemire-v1 against committed golden vectors generated
 *    from the reference algorithm BEFORE the C implementation — including
 *    vectors whose bounded draw takes the rejection loop, asserted by the
 *    exact raw-draw count they consume;
 *  - the canonical trace codec: round-trip, deterministic rendering, and a
 *    malformed-input matrix where every corruption is a NAMED error — magic,
 *    version, prng id, truncation, trailing bytes, count overflow, CRC (in
 *    header and in records), config hash, unknown op, field range;
 *  - the reference ledger's credit identities across the exact
 *    full/hold/consume/retry/abandon sequence, every term exercised.
 *
 * Specs cited by the scripts: /Users/jekyll/Projects/MoQ/Spec/
 * draft-ietf-moq-transport-16.txt
 *   sha256 2174e50090f20801df4d21e16b9ec21abe593e6ba2a84e43142aabdeb47b2c18
 * draft-ietf-moq-transport-18.txt
 *   sha256 9e6b32cb7797c151e9e127374c1291af3ed546b2d453cd5bbb15946977eeeeb6
 */

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "seedx/seedx_ledger.h"
#include "seedx/seedx_prng.h"
#include "seedx/seedx_trace.h"

static int g_failures;

#define T_CHECK(expr)                                                     \
    do {                                                                  \
        if (!(expr)) {                                                    \
            printf("FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr);       \
            g_failures++;                                                 \
        }                                                                 \
    } while (0)

/* -- golden vectors -------------------------------------------------------- */

static void
t_prng_vectors(const char *path)
{
    FILE *f = fopen(path, "r");
    char line[512];
    int raw_rows = 0, draw_rows = 0, reject_rows = 0;

    T_CHECK(f != NULL);
    if (f == NULL) {
        return;
    }
    while (fgets(line, sizeof(line), f) != NULL) {
        if (line[0] == '#') {
            continue;
        }
        unsigned long long seed, o0, o1, o2, o3, n, v, c;

        if (sscanf(line, "raw 0x%llx %llx %llx %llx %llx", &seed, &o0, &o1,
                   &o2, &o3) == 5) {
            seedx_prng_t p;

            seedx_prng_init(&p, seed);
            T_CHECK(seedx_next(&p) == o0);
            T_CHECK(seedx_next(&p) == o1);
            T_CHECK(seedx_next(&p) == o2);
            T_CHECK(seedx_next(&p) == o3);
            raw_rows++;
        } else if (sscanf(line, "draw 0x%llx %llu %llu %llu", &seed, &n, &v,
                          &c) == 4) {
            seedx_prng_t p;

            seedx_prng_init(&p, seed);
            uint64_t got = seedx_draw(&p, n);

            T_CHECK(got == v);
            T_CHECK(p.raw_draws == c); /* the rejection loop is part of the
                                        * replay contract, not an accident */
            draw_rows++;
            if (c > 1) {
                reject_rows++;
            }
        }
    }
    fclose(f);
    T_CHECK(raw_rows == 3);
    T_CHECK(draw_rows == 11);
    T_CHECK(reject_rows >= 2); /* vectors that actually take the loop */
    if (g_failures == 0) {
        printf("PASS: prng_golden_vectors (%d raw, %d draw, %d rejecting)\n",
               raw_rows, draw_rows, reject_rows);
    }
}

/* -- trace codec ------------------------------------------------------------ */

#define UT_MAX_OP 6

static int
ut_field_ok(const seedx_rec_t *r)
{
    if (r->lane > 1) {
        return 0;
    }
    if (r->op == 3 && r->a > 100) { /* an op with a declared field range */
        return 0;
    }
    return 1;
}

static void
t_trace_codec(void)
{
    int before = g_failures;
    seedx_rec_t recs[4] = {
        { 1, 0, 7, 8, 9 },
        { 3, 1, 99, 0, 0 },
        { 6, 0, 0, 0, 0 },
        { 2, 1, 65535, 1, 2 },
    };
    uint8_t buf[SEEDX_TRACE_HDR + 4 * SEEDX_TRACE_REC];
    uint8_t tmp[sizeof(buf) + 8];
    seedx_rec_t arena[8];
    seedx_trace_t t;

    size_t len = seedx_trace_encode(0xABCDEF0011223344ull, 0x42ull, recs, 4,
                                    buf, sizeof(buf));

    T_CHECK(len == sizeof(buf));

    /* round-trip */
    T_CHECK(seedx_trace_decode(buf, len, 0x42, UT_MAX_OP, ut_field_ok, arena,
                               8, &t) == SEEDX_TRACE_OK);
    T_CHECK(t.seed == 0xABCDEF0011223344ull && t.op_count == 4);
    T_CHECK(memcmp(arena, recs, sizeof(recs)) == 0);

    /* deterministic renderer: identical twice, never re-parsed */
    char r1[512], r2[512];

    T_CHECK(seedx_trace_render(&t, r1, sizeof(r1)) ==
            seedx_trace_render(&t, r2, sizeof(r2)));
    T_CHECK(strcmp(r1, r2) == 0);

    /* malformed matrix: every corruption is its own NAMED error */
    struct {
        const char       *what;
        seedx_trace_err_t want;
        void (*mutate)(uint8_t *b, size_t *l);
    } cases[10];
    (void)cases;

#define CASE(desc, expect, ...)                                           \
    do {                                                                  \
        memcpy(tmp, buf, len);                                            \
        size_t tl = len;                                                  \
        { __VA_ARGS__; }                                                  \
        T_CHECK(seedx_trace_decode(tmp, tl, 0x42, UT_MAX_OP, ut_field_ok, \
                                   arena, 8, &t) == (expect));            \
    } while (0)

    CASE("magic", SEEDX_TRACE_E_MAGIC, tmp[0] ^= 0xFF);
    CASE("version", SEEDX_TRACE_E_VERSION, tmp[4] = 9);
    CASE("prng", SEEDX_TRACE_E_PRNG, tmp[6] = 9);
    CASE("truncated-header", SEEDX_TRACE_E_TRUNCATED, tl = 12);
    CASE("truncated-records", SEEDX_TRACE_E_TRUNCATED, tl -= 3);
    CASE("trailing", SEEDX_TRACE_E_TRAILING, tmp[tl] = 0; tl += 1);
    CASE("count-overflow", SEEDX_TRACE_E_COUNT,
         tmp[16] = 0xFF; tmp[17] = 0xFF; tmp[18] = 0xFF; tmp[19] = 0x7F);
    CASE("crc-header", SEEDX_TRACE_E_CRC, tmp[8] ^= 0x01); /* seed byte */
    CASE("crc-record", SEEDX_TRACE_E_CRC,
         tmp[SEEDX_TRACE_HDR + 2] ^= 0x01);
    /* CRC recomputed so the DEEPER validators are reached and named */
    CASE("config-hash", SEEDX_TRACE_E_CONFIG,
         { size_t n2 = seedx_trace_encode(t.seed, 0x43, recs, 4, tmp,
                                          sizeof(tmp)); tl = n2; });
    CASE("unknown-op", SEEDX_TRACE_E_OP, {
        seedx_rec_t bad[4]; memcpy(bad, recs, sizeof(recs));
        bad[2].op = UT_MAX_OP + 1;
        tl = seedx_trace_encode(t.seed, 0x42, bad, 4, tmp, sizeof(tmp));
    });
    CASE("field-range", SEEDX_TRACE_E_FIELD, {
        seedx_rec_t bad[4]; memcpy(bad, recs, sizeof(recs));
        bad[1].a = 101; /* op 3 declares a <= 100 */
        tl = seedx_trace_encode(t.seed, 0x42, bad, 4, tmp, sizeof(tmp));
    });
    CASE("lane-range", SEEDX_TRACE_E_FIELD, {
        seedx_rec_t bad[4]; memcpy(bad, recs, sizeof(recs));
        bad[0].lane = 2;
        tl = seedx_trace_encode(t.seed, 0x42, bad, 4, tmp, sizeof(tmp));
    });
#undef CASE

    if (g_failures == before) {
        printf("PASS: trace_codec (round-trip, renderer, 13 named "
               "malformed cases)\n");
    }
}

/* -- ledger identities ------------------------------------------------------ */

static int
check_pair(const sx_pair_t *p)
{
    return sx_pair_check(p);
}

static void
t_ledger_identities(void)
{
    int before = g_failures;
    sx_pair_t p;

    memset(&p, 0, sizeof(p));
    p.capacity = 2;

    /* fill to capacity: L1/L2 terms move */
    T_CHECK(sx_pair_offer(&p));            /* accepted, occ 1 */
    T_CHECK(check_pair(&p) == 0);
    T_CHECK(sx_pair_offer(&p));            /* accepted, occ 2 == capacity */
    T_CHECK(check_pair(&p) == 0 && p.occupancy == 2);

    /* the offer past capacity is HELD, and the sum identities still hold —
     * this is exactly the state where the old single identity exceeded
     * capacity and broke */
    T_CHECK(!sx_pair_offer(&p));
    T_CHECK(check_pair(&p) == 0 && p.held_current == 1);
    T_CHECK(p.unique_offered_total == 3);

    /* a retry while still full is a RE-REFUSAL: nothing moves, above all not
     * unique_offered_total */
    T_CHECK(!sx_pair_retry(&p));
    T_CHECK(check_pair(&p) == 0 && p.held_current == 1 &&
            p.unique_offered_total == 3);

    /* consume returns credit; retry then accepts the held offer */
    T_CHECK(sx_pair_consume(&p));
    T_CHECK(check_pair(&p) == 0 && p.occupancy == 1 &&
            p.consumed_total == 1);
    T_CHECK(sx_pair_retry(&p));
    T_CHECK(check_pair(&p) == 0 && p.held_current == 0 &&
            p.accepted_total == 3 && p.unique_offered_total == 3);

    /* abandon + supersede exercise L3's remaining terms */
    T_CHECK(!sx_pair_offer(&p));           /* held again (occ back at cap) */
    sx_pair_abandon(&p);
    T_CHECK(check_pair(&p) == 0 && p.abandoned_total == 1);
    T_CHECK(!sx_pair_offer(&p));
    sx_pair_supersede(&p);
    T_CHECK(check_pair(&p) == 0 && p.superseded_total == 1);
    T_CHECK(p.unique_offered_total == 5);

    /* drain to empty: every identity holds at every step */
    T_CHECK(sx_pair_consume(&p) && check_pair(&p) == 0);
    T_CHECK(sx_pair_consume(&p) && check_pair(&p) == 0);
    T_CHECK(!sx_pair_consume(&p));
    T_CHECK(p.occupancy == 0 && check_pair(&p) == 0);

    if (g_failures == before) {
        printf("PASS: ledger_identities (full/hold/re-refusal/consume/retry/"
               "abandon/supersede)\n");
    }
}

int
main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "usage: %s <prng_vectors.txt>\n", argv[0]);
        return 2;
    }
    t_prng_vectors(argv[1]);
    t_trace_codec();
    t_ledger_identities();
    return g_failures;
}
