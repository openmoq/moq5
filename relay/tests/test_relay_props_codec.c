/* moqr_core_internal_props_convert: lossless re-encoding of a Key-Value-Pair
 * block between draft-16 (i) and draft-18/22 (vi64) integers, and the cases
 * it must refuse. Expected bytes are written out by hand from the two
 * integer encodings (RFC 9000 16; draft-18 1.4.1), never produced by the
 * converter under test. */
#include "props_codec_internal.h"

#include <stdio.h>
#include <string.h>

static int failures;
#define CHECK(e) do { if (!(e)) { \
    printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #e); failures++; } } while (0)

enum { Q = MOQR_PROPS_CODEC_QUIC_VARINT, V = MOQR_PROPS_CODEC_VI64,
       U = MOQR_PROPS_CODEC_UNKNOWN };

/* Convert `in` from src to dst, expecting `want` bytes (CONVERTED). */
static void expect_conv(uint8_t src, uint8_t dst, const uint8_t *in, size_t len,
                        const uint8_t *want, size_t want_len, const char *what)
{
    size_t need = 0;
    moqr_props_conv_t r = moqr_core_internal_props_convert(src, dst, in, len, NULL, 0,
                                                           &need);
    uint8_t out[256];
    size_t got = 0;
    moqr_props_conv_t w = moqr_core_internal_props_convert(src, dst, in, len, out,
                                                           sizeof(out), &got);
    bool ok = r == MOQR_PROPS_CONV_CONVERTED && w == MOQR_PROPS_CONV_CONVERTED &&
              need == want_len && got == want_len && memcmp(out, want, want_len) == 0;
    if (!ok) {
        printf("FAIL %s: measure=%d need=%zu write=%d got=%zu want=%zu\n", what, (int)r,
               need, (int)w, got, want_len);
        failures++;
    }
}

static void expect_outcome(uint8_t src, uint8_t dst, const uint8_t *in, size_t len,
                           moqr_props_conv_t want, const char *what)
{
    uint8_t out[256];
    size_t got = 0;
    moqr_props_conv_t r = moqr_core_internal_props_convert(src, dst, in, len, out,
                                                           sizeof(out), &got);
    if (r != want) {
        printf("FAIL %s: outcome %d, want %d\n", what, (int)r, (int)want);
        failures++;
    }
}

int main(void)
{
    /* Even Type 0x10 with values at the one-byte boundary and above.
     * QUIC: 63 = 3F, 64 = 40 40, 1000 = 43 E8, 16383 = 7F FF,
     * 16384 = 80 00 40 00. vi64: 63 = 3F, 64 = 40, 127 = 7F, 128 = 80 80,
     * 1000 = 83 E8, 16383 = BF FF, 16384 = C0 40 00. */
    {
        static const uint8_t q[] = { 0x10, 0x3F };
        expect_conv(Q, V, q, sizeof(q), q, sizeof(q), "63 alike");
    }
    {
        static const uint8_t q[] = { 0x10, 0x40, 0x40 }, v[] = { 0x10, 0x40 };
        expect_conv(Q, V, q, sizeof(q), v, sizeof(v), "64 q->v");
        expect_conv(V, Q, v, sizeof(v), q, sizeof(q), "64 v->q");
    }
    {
        static const uint8_t q[] = { 0x10, 0x43, 0xE8 }, v[] = { 0x10, 0x83, 0xE8 };
        expect_conv(Q, V, q, sizeof(q), v, sizeof(v), "1000 q->v");
        expect_conv(V, Q, v, sizeof(v), q, sizeof(q), "1000 v->q");
    }
    {
        static const uint8_t q[] = { 0x10, 0x7F, 0xFF }, v[] = { 0x10, 0xBF, 0xFF };
        expect_conv(Q, V, q, sizeof(q), v, sizeof(v), "16383 q->v");
    }
    {
        static const uint8_t q[] = { 0x10, 0x80, 0x00, 0x40, 0x00 },
                             v[] = { 0x10, 0xC0, 0x40, 0x00 };
        expect_conv(Q, V, q, sizeof(q), v, sizeof(v), "16384 q->v");
        expect_conv(V, Q, v, sizeof(v), q, sizeof(q), "16384 v->q");
    }
    /* A Delta Type that is two bytes in QUIC and one in vi64 (Type 80: QUIC
     * 40 50, vi64 50), and an odd Type with a 70-byte value (Length QUIC
     * 40 46, vi64 46). */
    {
        uint8_t q[2 + 1 + 1 + 2 + 70], v[1 + 1 + 1 + 1 + 70];
        q[0] = 0x40; q[1] = 0x50; q[2] = 0x00;        /* Type 80, value 0 */
        q[3] = 0x01; q[4] = 0x40; q[5] = 0x46;        /* Type 81, Length 70 */
        v[0] = 0x50; v[1] = 0x00;
        v[2] = 0x01; v[3] = 0x46;
        for (int i = 0; i < 70; i++) { q[6 + i] = (uint8_t)(0xC0 | i); v[4 + i] = q[6 + i]; }
        expect_conv(Q, V, q, sizeof(q), v, sizeof(v), "delta 80 + odd 70 bytes q->v");
        expect_conv(V, Q, v, sizeof(v), q, sizeof(q), "delta 80 + odd 70 bytes v->q");
    }
    /* Same codec and empty blocks: SAME, nothing written. */
    {
        static const uint8_t q[] = { 0x10, 0x43, 0xE8 };
        size_t got = 99;
        CHECK(moqr_core_internal_props_convert(Q, Q, q, sizeof(q), NULL, 0, &got) ==
              MOQR_PROPS_CONV_SAME && got == sizeof(q));
        CHECK(moqr_core_internal_props_convert(V, Q, NULL, 0, NULL, 0, &got) ==
              MOQR_PROPS_CONV_SAME && got == 0);
        CHECK(moqr_core_internal_props_convert(U, Q, NULL, 0, NULL, 0, &got) ==
              MOQR_PROPS_CONV_SAME);
    }
    /* Nonempty UNKNOWN is never converted; bad tags are INVAL. */
    {
        static const uint8_t b[] = { 0x10, 0x01 };
        expect_outcome(U, Q, b, sizeof(b), MOQR_PROPS_CONV_UNKNOWN, "unknown source");
        expect_outcome(Q, U, b, sizeof(b), MOQR_PROPS_CONV_INVAL, "unknown destination");
        expect_outcome(3, Q, b, sizeof(b), MOQR_PROPS_CONV_INVAL, "bad source tag");
        expect_outcome(Q, 9, b, sizeof(b), MOQR_PROPS_CONV_INVAL, "bad destination tag");
    }
    /* A valid vi64 value d16 cannot carry: 2^62 (vi64's nine-byte form FF +
     * 40 00 00 00 00 00 00 00). 2^62-1 still fits: vi64 FF 3F FF FF FF FF
     * FF FF FF, QUIC's eight-byte form FF FF FF FF FF FF FF FF. */
    {
        static const uint8_t v[] = { 0x10, 0xFF, 0x40, 0, 0, 0, 0, 0, 0, 0 };
        expect_outcome(V, Q, v, sizeof(v), MOQR_PROPS_CONV_UNSUPPORTED, "2^62 to d16");
        static const uint8_t vmax[] = { 0x10, 0xFF, 0x3F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                        0xFF },
                             qmax[] = { 0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };
        expect_conv(V, Q, vmax, sizeof(vmax), qmax, sizeof(qmax), "2^62-1 to d16");
    }
    /* Malformed sources: truncated integer, odd Length past the block, Type
     * accumulation past 2^64-1, Length over 2^16-1. */
    {
        static const uint8_t t1[] = { 0x10, 0x43 };            /* QUIC 2-byte cut */
        expect_outcome(Q, V, t1, sizeof(t1), MOQR_PROPS_CONV_MALFORMED, "truncated value");
        static const uint8_t t2[] = { 0x11, 0x05, 0xAA };       /* Length 5, 1 byte */
        expect_outcome(Q, V, t2, sizeof(t2), MOQR_PROPS_CONV_MALFORMED, "short odd value");
        static const uint8_t t3[] = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                                      0x00, 0x02, 0x00 };       /* 2^64-1, then +2 */
        expect_outcome(V, Q, t3, sizeof(t3), MOQR_PROPS_CONV_MALFORMED, "type overflow");
        static const uint8_t t4[] = { 0x11, 0xC1, 0x00, 0x00 }; /* vi64 Length 65536 */
        expect_outcome(V, Q, t4, sizeof(t4), MOQR_PROPS_CONV_MALFORMED, "length > 2^16-1");
    }
    /* A Type above d16's single-integer limit is carried as long as each
     * encoded Delta fits: Types 2^62-2 then 2^62 (delta 2). */
    {
        static const uint8_t v[] = { 0xFF, 0x3F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE,
                                     0x00, 0x02, 0x00 };
        static const uint8_t q[] = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0x00,
                                     0x02, 0x00 };
        expect_conv(V, Q, v, sizeof(v), q, sizeof(q), "absolute type past 2^62 by deltas");
    }
    /* Immutable Properties (0xB). Compatible: header and the pairs it
     * protects are all one-byte integers, so the bytes read the same in both
     * codecs and are kept exactly. */
    {
        static const uint8_t b[] = { 0x0B, 0x04, 0x02, 0x05, 0x02, 0x07 };
        expect_conv(Q, V, b, sizeof(b), b, sizeof(b), "immutable compatible q->v");
        expect_conv(V, Q, b, sizeof(b), b, sizeof(b), "immutable compatible v->q");
    }
    /* Incompatible: the protected value is a two-byte integer (QUIC 43 E8
     * = 1000) that vi64 cannot read the same way. */
    {
        static const uint8_t b[] = { 0x0B, 0x03, 0x02, 0x43, 0xE8 };
        expect_outcome(Q, V, b, sizeof(b), MOQR_PROPS_CONV_UNSUPPORTED,
                       "immutable incompatible");
    }
    /* Parses in both codecs, to different pairs. Protected 02 40 05 01 00:
     * QUIC reads Type 2 = 5 (40 05), then Type 3 with an empty value; vi64
     * reads Type 2 = 64 (40), then Type 7 with the one byte 00. */
    {
        static const uint8_t b[] = { 0x0B, 0x05, 0x02, 0x40, 0x05, 0x01, 0x00 };
        expect_outcome(Q, V, b, sizeof(b), MOQR_PROPS_CONV_UNSUPPORTED,
                       "immutable parses both ways, differently");
    }
    /* Malformed protected contents are MALFORMED, never UNSUPPORTED: nested
     * Immutable Properties, an even Type without its value, an odd Length
     * past the block -- also when an earlier pair was unrepresentable. */
    {
        static const uint8_t nest[] = { 0x0B, 0x04, 0x0B, 0x02, 0x02, 0x01 };
        expect_outcome(Q, V, nest, sizeof(nest), MOQR_PROPS_CONV_MALFORMED, "nested immutable");
        expect_outcome(V, Q, nest, sizeof(nest), MOQR_PROPS_CONV_MALFORMED,
                       "nested immutable v->q");
        static const uint8_t novalue[] = { 0x0B, 0x01, 0x02 };
        expect_outcome(Q, V, novalue, sizeof(novalue), MOQR_PROPS_CONV_MALFORMED,
                       "immutable even type without value");
        static const uint8_t longlen[] = { 0x0B, 0x03, 0x01, 0x05, 0xAA };
        expect_outcome(Q, V, longlen, sizeof(longlen), MOQR_PROPS_CONV_MALFORMED,
                       "immutable odd length past its block");
        /* Type 2 = 2^62 (unrepresentable in d16), then delta 9 to Type 0xB. */
        static const uint8_t after_unrep[] = { 0x02, 0xFF, 0x40, 0, 0, 0, 0, 0, 0, 0,
                                               0x09, 0x01, 0x02 };
        expect_outcome(V, Q, after_unrep, sizeof(after_unrep), MOQR_PROPS_CONV_MALFORMED,
                       "malformed immutable after an unrepresentable pair");
    }
    /* Mutable pairs around a compatible immutable entry: re-encoded, the
     * immutable entry byte-exact. Types 0x02 (64), 0x0B, 0x10 (1000). */
    {
        static const uint8_t q[] = { 0x02, 0x40, 0x40, 0x09, 0x02, 0x02, 0x05,
                                     0x05, 0x43, 0xE8 };
        static const uint8_t v[] = { 0x02, 0x40, 0x09, 0x02, 0x02, 0x05,
                                     0x05, 0x83, 0xE8 };
        expect_conv(Q, V, q, sizeof(q), v, sizeof(v), "mutable around immutable q->v");
        expect_conv(V, Q, v, sizeof(v), q, sizeof(q), "mutable around immutable v->q");
    }
    /* A too-small buffer reports the need. */
    {
        static const uint8_t q[] = { 0x10, 0x43, 0xE8 };
        uint8_t out[2];
        size_t got = 0;
        CHECK(moqr_core_internal_props_convert(Q, V, q, sizeof(q), out, sizeof(out), &got) ==
              MOQR_PROPS_CONV_BUFFER && got == 3);
    }
    printf("%s: relay_props_codec failures=%d\n", failures ? "FAIL" : "PASS", failures);
    return failures ? 1 : 0;
}
