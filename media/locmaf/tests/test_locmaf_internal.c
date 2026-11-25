/*
 * The functions kept out of the public API: zigzag and the checked
 * arithmetic, the size limits of sections 15.7 and 8.3 at lengths no test
 * can allocate, the storage carve, the encode bound at the limits, and the
 * reference rule.
 */
#include "locmaf_internal.h"

#include <stdio.h>

static int failures = 0;

#define CHECK(expr) do { \
    if (!(expr)) { \
        fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        failures++; \
    } \
} while (0)

static void test_zigzag(void)
{
    /* section 7.4 */
    CHECK(locmaf_zz_enc(0) == 0 && locmaf_zz_dec(0) == 0);
    CHECK(locmaf_zz_enc(-1) == 1 && locmaf_zz_dec(1) == -1);
    CHECK(locmaf_zz_enc(1) == 2 && locmaf_zz_dec(2) == 1);
    CHECK(locmaf_zz_enc(-2) == 3 && locmaf_zz_dec(3) == -2);
    CHECK(locmaf_zz_enc(2) == 4 && locmaf_zz_enc(-3) == 5 && locmaf_zz_enc(3) == 6);
    CHECK(locmaf_zz_enc(INT64_MIN) == UINT64_MAX && locmaf_zz_dec(UINT64_MAX) == INT64_MIN);
    CHECK(locmaf_zz_enc(INT64_MAX) == UINT64_MAX - 1 &&
          locmaf_zz_dec(UINT64_MAX - 1) == INT64_MAX);

    /* arith_zz_roundtrip: identity both ways over the edges and seeded
     * values */
    static const int64_t edges[] = { INT64_MIN, INT64_MIN + 1, -1, 0, 1,
                                     INT64_MAX - 1, INT64_MAX };
    for (size_t i = 0; i < sizeof(edges) / sizeof(edges[0]); i++)
        CHECK(locmaf_zz_dec(locmaf_zz_enc(edges[i])) == edges[i]);
    uint64_t s = 0x243F6A8885A308D3ull;
    for (int i = 0; i < 100000; i++) {
        s ^= s << 13; s ^= s >> 7; s ^= s << 17;
        int64_t v = (int64_t)s;
        if (locmaf_zz_dec(locmaf_zz_enc(v)) != v) { CHECK(!"zz round trip"); break; }
        if (locmaf_zz_enc(locmaf_zz_dec(s)) != s) { CHECK(!"zz inverse"); break; }
    }
}

static void test_checked_arithmetic(void)
{
    int64_t out = 77;
    const int64_t u32 = (int64_t)UINT32_MAX;
    CHECK(locmaf_add_in_range(5, -5, 0, u32, &out) && out == 0);
    out = 77;
    CHECK(!locmaf_add_in_range(5, -6, 0, u32, &out) && out == 77);
    CHECK(locmaf_add_in_range(u32 - 1, 1, 0, u32, &out) && out == u32);
    CHECK(!locmaf_add_in_range(u32 - 1, 2, 0, u32, &out));
    CHECK(!locmaf_add_in_range(0, INT64_MIN, 0, u32, &out));
    CHECK(!locmaf_add_in_range(u32, INT64_MAX, 0, u32, &out));
    CHECK(!locmaf_add_in_range(INT32_MIN, -1, INT32_MIN, u32, &out));
    CHECK(locmaf_add_in_range(INT32_MIN, u32 - INT32_MIN, INT32_MIN, u32, &out) && out == u32);
    uint64_t acc = 0;
    CHECK(locmaf_u64_add(UINT64_MAX - 1, 1, &acc) && acc == UINT64_MAX);
    CHECK(!locmaf_u64_add(UINT64_MAX, 1, &acc));
    CHECK(!locmaf_u64_add(1, UINT64_MAX, &acc));
    size_t sz = 0;
    CHECK(lm_size_add(SIZE_MAX - 1, 1, &sz) && sz == SIZE_MAX);
    CHECK(!lm_size_add(SIZE_MAX, 1, &sz));
}

static void test_limits(void)
{
    moq_locmaf_reason_t why = MOQ_LOCMAF_REASON_NONE;
    /* section 15.7: the mdat payload must fit a 32-bit mdat size */
    CHECK(locmaf_check_mdat_len(0xFFFFFFF7u, &why) && why == MOQ_LOCMAF_REASON_NONE);
    CHECK(!locmaf_check_mdat_len(0xFFFFFFF8u, &why) && why == MOQ_LOCMAF_REASON_MDAT_TOO_LARGE);
    CHECK(!locmaf_check_mdat_len(UINT64_MAX, NULL));
    /* data_offset = moof_size + 8 is a signed 32-bit trun field */
    CHECK(locmaf_check_moof_size((uint64_t)INT32_MAX - 8));
    CHECK(!locmaf_check_moof_size((uint64_t)INT32_MAX - 7));
    CHECK(!locmaf_check_moof_size(UINT64_MAX));
}

static void test_carve(void)
{
    locmaf_carve_t c = { NULL, 0, true };
    CHECK(locmaf_carve_take(&c, 3, 1) == NULL && c.ok && c.off == 3);
    (void)locmaf_carve_take(&c, 1, 8);
    CHECK(c.ok && c.off == 16);                     /* rounded to 8 first */
    (void)locmaf_carve_take(&c, SIZE_MAX / 2, 4);
    CHECK(!c.ok);
    locmaf_carve_t d = { NULL, SIZE_MAX - 3, true };
    (void)locmaf_carve_take(&d, 0, 1);
    CHECK(!d.ok);                                   /* the alignment itself overflows */
    /* the regions of a measured layout land at 8-byte offsets */
    static uint64_t store[64];
    locmaf_carve_t e = { (uint8_t *)store, 0, true };
    uint8_t *a = locmaf_carve_take(&e, 5, 1);
    uint32_t *b = locmaf_carve_take(&e, 3, sizeof(uint32_t));
    CHECK(a == (uint8_t *)store && (uint8_t *)b == (uint8_t *)store + 8);
}

static void test_bound(void)
{
    size_t out = 0;
    /* check_bound_holds at the limits: n = 65536, U = 2^20, 256 genBoxes,
     * the largest payload */
    CHECK(locmaf_encode_bound_counts(256u * (4u + 100u), 256, 65536, 1u << 20,
                                     0xFFFFFFF7u, &out));
    uint64_t want = 256u * 104u + 2560u + (10 + 20 + 42 + 70 + 19) +
                    65536ull * 44u + (1ull << 20) * 8u + 0xFFFFFFF7ull;
    if (sizeof(size_t) == 8) CHECK(out == want);
    /* overflow is refused, never wrapped */
    CHECK(!locmaf_encode_bound_counts(UINT64_MAX, 0, 0, 0, 0, &out));
    CHECK(!locmaf_encode_bound_counts(0, 0, UINT64_MAX / 8, 0, 0, &out));
    CHECK(!locmaf_encode_bound_counts(0, 0, 0, UINT64_MAX / 4, 0, &out));
    CHECK(!locmaf_encode_bound_counts(0, 0, 0, 0, UINT64_MAX - 100, &out));
}

static void test_reference_rule(void)
{
    moq_locmaf_ref_state_t prev, cur;
    memset(&prev, 0, sizeof(prev));
    memset(&cur, 0, sizeof(cur));
    /* Deltas chain to the previous chunk whatever its header kind. */
    CHECK(locmaf_ref_next(&prev, &cur, MOQ_LOCMAF_OBJECT_FULL) == &cur);
    CHECK(locmaf_ref_next(&prev, &cur, MOQ_LOCMAF_OBJECT_DELTA) == &cur);
}

static void test_decide(void)
{
    moq_locmaf_track_t t;
    memset(&t, 0, sizeof(t));
    t.trex_default_sample_description_index = 1;
    t.trex_default_sample_duration = 1024;
    t.trex_default_sample_flags = 0x01010000u;
    moq_locmaf_sample_t s[3];
    memset(s, 0, sizeof(s));
    locmaf_canon_t k;
    /* n = 0: nothing beyond fields 10 and 14 */
    locmaf_decide(&t, 7, s, 0, 0, &k);
    CHECK(!k.sdi && !k.dur_default && !k.size_box && !k.flags_default && !k.cto);
    /* first-sample flags: f[1..] equal, f[0] differs; v = f[1] = trex */
    for (int i = 0; i < 3; i++) { s[i].duration = 1024; s[i].size = 10; s[i].flags = 0x01010000u; }
    s[0].flags = 0x02000000u;
    locmaf_decide(&t, 1, s, 3, 30, &k);
    CHECK(k.first_flags && !k.flags_list && !k.flags_default && k.first == 0x02000000u);
    CHECK(!k.dur_list && !k.dur_default && k.size_wire && k.size_box && k.size == 10);
    /* varying flags past the first: per-sample */
    s[2].flags = 0x02000000u;
    locmaf_decide(&t, 1, s, 3, 30, &k);
    CHECK(k.flags_list && !k.first_flags && !k.flags_default);
    /* one sample: its size is the payload, never on the wire */
    locmaf_decide(&t, 1, s, 1, 30, &k);
    CHECK(!k.size_wire && !k.size_list && k.size_box && k.size == 30);
    /* a negative offset selects trun version 1 */
    s[1].composition_offset = -1;
    locmaf_decide(&t, 1, s, 3, 30, &k);
    CHECK(k.cto && k.cto_signed);
}

int main(void)
{
    test_zigzag();
    test_checked_arithmetic();
    test_limits();
    test_carve();
    test_bound();
    test_reference_rule();
    test_decide();
    printf("%s: locmaf_internal: %d failures\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
