/*
 * Tests for the vi64 codec (MoQT variable-length integer, draft-18 §1.4.1):
 * length selection, round-trip across all length boundaries, the spec worked
 * examples, non-minimal decode acceptance, the full-range 9-byte form, error
 * handling, and the buf cursor helpers.
 */
#include <moq/vi64.h>
#include <moq/buf.h>
#include "test_support.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* == moq_vi64_len: each length's min and max boundary ============= */
    {
        struct { uint64_t v; size_t len; } cases[] = {
            { 0, 1 }, { 127, 1 },
            { 128, 2 }, { 16383, 2 },
            { 16384, 3 }, { 2097151, 3 },
            { 2097152, 4 }, { 268435455, 4 },
            { 268435456ULL, 5 }, { 34359738367ULL, 5 },
            { 34359738368ULL, 6 }, { 4398046511103ULL, 6 },
            { 4398046511104ULL, 7 }, { 562949953421311ULL, 7 },
            { 562949953421312ULL, 8 }, { 72057594037927935ULL, 8 },
            { 72057594037927936ULL, 9 }, { UINT64_MAX, 9 },
        };
        for (size_t i = 0; i < sizeof(cases)/sizeof(cases[0]); i++)
            MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_len(cases[i].v), cases[i].len);
    }

    /* == moq_vi64_encoded_len from a first byte ======================= */
    {
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0x00), 1);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0x7F), 1);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0x80), 2);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0xC0), 3);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0xE0), 4);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0xF0), 5);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0xF8), 6);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0xFC), 7);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0xFE), 8);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encoded_len(0xFF), 9);
    }

    /* == Round-trip (minimum encode) across boundaries ================ */
    {
        uint64_t vs[] = {
            0, 1, 37, 127, 128, 255, 256, 16383, 16384, 65535, 65536,
            2097151, 2097152, 268435455, 268435456ULL,
            34359738367ULL, 34359738368ULL,
            4398046511103ULL, 4398046511104ULL,
            562949953421311ULL, 562949953421312ULL,
            72057594037927935ULL, 72057594037927936ULL,
            UINT64_MAX - 1, UINT64_MAX,
        };
        for (size_t i = 0; i < sizeof(vs)/sizeof(vs[0]); i++) {
            uint8_t buf[MOQ_VI64_MAX_LEN];
            size_t n = moq_vi64_encode(vs[i], buf, sizeof(buf));
            MOQ_TEST_CHECK_EQ_SIZE(n, moq_vi64_len(vs[i]));
            uint64_t got = 0;
            size_t m = moq_vi64_decode(buf, n, &got);
            MOQ_TEST_CHECK_EQ_SIZE(m, n);
            MOQ_TEST_CHECK_EQ_U64(got, vs[i]);
        }
    }

    /* == Full-range 9-byte form: 0xFF followed by 8x 0xFF ============= */
    {
        uint8_t buf[MOQ_VI64_MAX_LEN];
        size_t n = moq_vi64_encode(UINT64_MAX, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_SIZE(n, 9);
        for (size_t i = 0; i < 9; i++)
            MOQ_TEST_CHECK_EQ_INT(buf[i], 0xFF);
        uint64_t got = 0;
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_decode(buf, 9, &got), 9);
        MOQ_TEST_CHECK_EQ_U64(got, UINT64_MAX);
    }

    /* == Spec worked examples (decode) ================================ */
    {
        struct { uint8_t bytes[9]; size_t len; uint64_t val; } ex[] = {
            { {0x25}, 1, 37 },
            { {0x80,0x25}, 2, 37 },                       /* non-minimal */
            { {0xbb,0xbd}, 2, 15293 },
            { {0xed,0x7f,0x3e,0x7d}, 4, 226442877ULL },
            { {0xfa,0xa1,0xa0,0xe4,0x03,0xd8}, 6, 2893212287960ULL },
            { {0xfc,0x89,0x98,0xab,0xc6,0x6b,0xc0}, 7, 151288809941952ULL },
            { {0xfe,0xfa,0x31,0x8f,0xa8,0xe3,0xca,0x11}, 8,
              70423237261249041ULL },
            { {0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff}, 9, UINT64_MAX },
        };
        for (size_t i = 0; i < sizeof(ex)/sizeof(ex[0]); i++) {
            uint64_t got = 0;
            size_t n = moq_vi64_decode(ex[i].bytes, ex[i].len, &got);
            MOQ_TEST_CHECK_EQ_SIZE(n, ex[i].len);
            MOQ_TEST_CHECK_EQ_U64(got, ex[i].val);
        }
    }

    /* == Non-minimal: 37 encoded at every length 1..9 decodes to 37 == */
    {
        static const uint8_t pfx[10] =
            { 0, 0, 0x80, 0xC0, 0xE0, 0xF0, 0xF8, 0xFC, 0xFE, 0xFF };
        for (size_t L = 1; L <= 9; L++) {
            uint8_t buf[9];
            memset(buf, 0, sizeof(buf));
            buf[0] = (L == 1) ? 0x25 : pfx[L];
            if (L > 1) buf[L - 1] = 0x25;  /* low byte holds 37 */
            uint64_t got = 0;
            size_t n = moq_vi64_decode(buf, L, &got);
            MOQ_TEST_CHECK_EQ_SIZE(n, L);
            MOQ_TEST_CHECK_EQ_U64(got, 37);
        }
    }

    /* == Error handling =============================================== */
    {
        uint64_t got = 0;
        uint8_t buf[9];

        /* Empty / NULL inputs. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_decode(NULL, 0, &got), 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_decode(buf, 0, &got), 0);
        buf[0] = 0x25;
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_decode(buf, 1, NULL), 0);

        /* Truncated: first byte indicates length 3 but only 2 available. */
        buf[0] = 0xC0; buf[1] = 0x00;
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_decode(buf, 2, &got), 0);
        /* 9-byte form needs 9 bytes; 8 is truncated. */
        memset(buf, 0xFF, sizeof(buf));
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_decode(buf, 8, &got), 0);

        /* Encode into too-small / NULL buffer. */
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encode(128, buf, 1), 0);  /* needs 2 */
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encode(UINT64_MAX, buf, 8), 0);
        MOQ_TEST_CHECK_EQ_SIZE(moq_vi64_encode(1, NULL, 4), 0);
    }

    /* == buf cursor helpers: write/read round-trip + advance ========== */
    {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&w, 37), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&w, 226442877ULL),
                              (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&w, UINT64_MAX),
                              (int)MOQ_OK);
        size_t total = moq_buf_writer_offset(&w);
        MOQ_TEST_CHECK_EQ_SIZE(total, 1 + 4 + 9);

        moq_buf_reader_t r;
        moq_buf_reader_init(&r, buf, total);
        uint64_t a = 0, b = 0, c = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_read_vi64(&r, &a), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_read_vi64(&r, &b), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_read_vi64(&r, &c), (int)MOQ_OK);
        MOQ_TEST_CHECK_EQ_U64(a, 37);
        MOQ_TEST_CHECK_EQ_U64(b, 226442877ULL);
        MOQ_TEST_CHECK_EQ_U64(c, UINT64_MAX);
        MOQ_TEST_CHECK_EQ_SIZE(moq_buf_reader_remaining(&r), 0);

        /* Reading past the end is a buffer error, not a crash. */
        uint64_t d = 0;
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_read_vi64(&r, &d),
                              (int)MOQ_ERR_BUFFER);

        /* Writing the 9-byte form into a 4-byte buffer fails cleanly. */
        uint8_t small[4];
        moq_buf_writer_t w2;
        moq_buf_writer_init(&w2, small, sizeof(small));
        MOQ_TEST_CHECK_EQ_INT((int)moq_buf_write_vi64(&w2, UINT64_MAX),
                              (int)MOQ_ERR_BUFFER);
    }

    MOQ_TEST_PASS("vi64");
    return failures != 0;
}
