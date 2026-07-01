#include <moq/kvp.h>
#include <moq/wire.h>
#include "test_support.h"
#include <string.h>

int main(void)
{
    int failures = 0;

    /* -- Roundtrip: two entries (even + odd type) ------------------ */
    {
        /* Entry 1: type=2 (even, varint value 42) */
        uint8_t val_buf[8];
        size_t val_len = moq_quic_varint_encode(42, val_buf, sizeof(val_buf));

        moq_kvp_entry_t e1 = {
            .type      = 2,
            .value     = val_buf,
            .value_len = val_len,
            .is_varint = true,
        };

        /* Entry 2: type=3 (odd, byte field "hello") */
        moq_kvp_entry_t e2 = {
            .type      = 3,
            .value     = (const uint8_t *)"hello",
            .value_len = 5,
            .is_varint = false,
        };

        /* Encode both entries. */
        uint8_t wire[64];
        size_t pos = 0;
        size_t n;

        n = moq_kvp_encode_entry(0, &e1, wire + pos, sizeof(wire) - pos);
        MOQ_TEST_CHECK(n > 0);
        pos += n;

        n = moq_kvp_encode_entry(e1.type, &e2, wire + pos, sizeof(wire) - pos);
        MOQ_TEST_CHECK(n > 0);
        pos += n;

        /* Decode and verify. */
        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, wire, pos);

        moq_kvp_entry_t out;
        moq_result_t rc;

        rc = moq_kvp_decode_next(&dec, &out);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(out.type == 2);
        MOQ_TEST_CHECK(out.is_varint == true);
        {
            uint64_t v = 0;
            moq_quic_varint_decode(out.value, out.value_len, &v);
            MOQ_TEST_CHECK(v == 42);
        }

        rc = moq_kvp_decode_next(&dec, &out);
        MOQ_TEST_CHECK(rc == MOQ_OK);
        MOQ_TEST_CHECK(out.type == 3);
        MOQ_TEST_CHECK(out.is_varint == false);
        MOQ_TEST_CHECK(out.value_len == 5);
        MOQ_TEST_CHECK(memcmp(out.value, "hello", 5) == 0);

        rc = moq_kvp_decode_next(&dec, &out);
        MOQ_TEST_CHECK(rc == MOQ_DONE);
    }

    /* -- Delta encoding correctness -------------------------------- */
    {
        /* Types 10, 20, 30 — deltas are 10, 10, 10. */
        uint8_t v10[2], v20[2], v30[2];
        size_t v10_len = moq_quic_varint_encode(100, v10, sizeof(v10));
        size_t v20_len = moq_quic_varint_encode(200, v20, sizeof(v20));
        size_t v30_len = moq_quic_varint_encode(255, v30, sizeof(v30));
        MOQ_TEST_CHECK(v10_len == 2);
        MOQ_TEST_CHECK(v20_len == 2);
        MOQ_TEST_CHECK(v30_len == 2);

        moq_kvp_entry_t entries[] = {
            { .type = 10, .value = v10, .value_len = v10_len, .is_varint = true },
            { .type = 20, .value = v20, .value_len = v20_len, .is_varint = true },
            { .type = 30, .value = v30, .value_len = v30_len, .is_varint = true },
        };

        uint8_t wire[64];
        size_t pos = 0;
        uint64_t prev = 0;
        for (int i = 0; i < 3; i++) {
            size_t n = moq_kvp_encode_entry(prev, &entries[i],
                                            wire + pos, sizeof(wire) - pos);
            MOQ_TEST_CHECK(n > 0);
            pos += n;
            prev = entries[i].type;
        }

        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, wire, pos);
        moq_kvp_entry_t out;

        for (int i = 0; i < 3; i++) {
            MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_OK);
            MOQ_TEST_CHECK(out.type == entries[i].type);
        }
        MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_DONE);
    }

    /* -- Non-ascending types: encode fails ------------------------- */
    {
        moq_kvp_entry_t e = { .type = 5, .value = NULL, .value_len = 0, .is_varint = true };
        uint8_t buf[16];
        /* prev_type=10 > entry type=5 — should fail. */
        MOQ_TEST_CHECK(moq_kvp_encode_entry(10, &e, buf, sizeof(buf)) == 0);
    }

    /* -- Odd-type value length > 2^16-1: encode fails -------------- */
    {
        moq_kvp_entry_t e = {
            .type = 1, .value = NULL, .value_len = 0x10000, .is_varint = false,
            .raw = NULL, .raw_len = 0
        };
        MOQ_TEST_CHECK(moq_kvp_entry_encoded_len(0, &e) == 0);
    }

    /* -- Type parity mismatch: encode rejects ---------------------- */
    {
        uint8_t vbuf[1];
        moq_quic_varint_encode(7, vbuf, 1);
        /* Even type but is_varint = false. */
        moq_kvp_entry_t e = {
            .type = 2, .value = vbuf, .value_len = 1, .is_varint = false,
            .raw = NULL, .raw_len = 0
        };
        uint8_t buf[16];
        MOQ_TEST_CHECK(moq_kvp_encode_entry(0, &e, buf, sizeof(buf)) == 0);
    }
    {
        /* Odd type but is_varint = true. */
        uint8_t vbuf[1];
        moq_quic_varint_encode(7, vbuf, 1);
        moq_kvp_entry_t e = {
            .type = 3, .value = vbuf, .value_len = 1, .is_varint = true,
            .raw = NULL, .raw_len = 0
        };
        uint8_t buf[16];
        MOQ_TEST_CHECK(moq_kvp_encode_entry(0, &e, buf, sizeof(buf)) == 0);
    }

    /* -- Even-type value that is not a valid varint: encode rejects - */
    {
        uint8_t bad[] = { 0xC0 }; /* 8-byte varint header, only 1 byte */
        moq_kvp_entry_t e = {
            .type = 2, .value = bad, .value_len = 1, .is_varint = true,
            .raw = NULL, .raw_len = 0
        };
        uint8_t buf[16];
        MOQ_TEST_CHECK(moq_kvp_encode_entry(0, &e, buf, sizeof(buf)) == 0);
    }

    /* -- Odd-type value_len > 0 with NULL value: encode rejects ---- */
    {
        moq_kvp_entry_t e = {
            .type = 1, .value = NULL, .value_len = 5, .is_varint = false,
            .raw = NULL, .raw_len = 0
        };
        uint8_t buf[16];
        MOQ_TEST_CHECK(moq_kvp_encode_entry(0, &e, buf, sizeof(buf)) == 0);
    }

    /* -- Raw span set by decoder ----------------------------------- */
    {
        uint8_t val_buf[8];
        size_t val_len = moq_quic_varint_encode(99, val_buf, sizeof(val_buf));
        moq_kvp_entry_t e1 = {
            .type = 4, .value = val_buf, .value_len = val_len,
            .is_varint = true, .raw = NULL, .raw_len = 0
        };

        uint8_t wire[32];
        size_t wlen = moq_kvp_encode_entry(0, &e1, wire, sizeof(wire));
        MOQ_TEST_CHECK(wlen > 0);

        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, wire, wlen);
        moq_kvp_entry_t out;
        MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.raw != NULL);
        MOQ_TEST_CHECK(out.raw_len == wlen);
        MOQ_TEST_CHECK(out.raw == wire);
    }

    /* -- Truncated decode ------------------------------------------ */
    {
        /* Just a delta type varint with no value. */
        uint8_t wire[] = { 0x02 }; /* delta=2, type=2 (even), needs varint value */
        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, wire, 1);
        moq_kvp_entry_t out;
        MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_ERR_PROTO);
    }

    /* -- Odd-type value length exceeds remaining ------------------- */
    {
        /* delta=1 (type=1, odd), length=10, but only 2 bytes of value */
        uint8_t wire[] = { 0x01, 0x0A, 0xAA, 0xBB };
        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, wire, sizeof(wire));
        moq_kvp_entry_t out;
        MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_ERR_PROTO);
    }

    /* -- Empty buffer ---------------------------------------------- */
    {
        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, NULL, 0);
        moq_kvp_entry_t out;
        MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_DONE);
    }

    /* -- NULL args ------------------------------------------------- */
    {
        MOQ_TEST_CHECK(moq_kvp_decode_next(NULL, NULL) == MOQ_ERR_INVAL);
        MOQ_TEST_CHECK(moq_kvp_encode_entry(0, NULL, NULL, 0) == 0);
        MOQ_TEST_CHECK(moq_kvp_entry_encoded_len(0, NULL) == 0);
    }

    /* -- Even type exposes the encoded varint in value/value_len ----- */
    {
        uint8_t val_buf[8];
        size_t val_len = moq_quic_varint_encode(12345, val_buf, sizeof(val_buf));
        moq_kvp_entry_t e = {
            .type = 2, .value = val_buf, .value_len = val_len,
            .is_varint = true,
        };
        uint8_t wire[16];
        size_t wlen = moq_kvp_encode_entry(0, &e, wire, sizeof(wire));
        MOQ_TEST_CHECK(wlen > 0);

        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, wire, wlen);
        moq_kvp_entry_t out;
        MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.is_varint == true);
        /* Canonical: the caller decodes the varint from value/value_len. */
        uint64_t v = 0;
        size_t n = moq_quic_varint_decode(out.value, out.value_len, &v);
        MOQ_TEST_CHECK(n == out.value_len);
        MOQ_TEST_CHECK_EQ_U64(v, 12345);
    }

    /* -- Odd type exposes the raw byte field in value/value_len ------ */
    {
        moq_kvp_entry_t e = {
            .type = 3, .value = (const uint8_t *)"abc", .value_len = 3,
            .is_varint = false,
        };
        uint8_t wire[16];
        size_t wlen = moq_kvp_encode_entry(0, &e, wire, sizeof(wire));
        MOQ_TEST_CHECK(wlen > 0);

        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, wire, wlen);
        moq_kvp_entry_t out;
        MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_OK);
        MOQ_TEST_CHECK(out.is_varint == false);
        MOQ_TEST_CHECK(out.value_len == 3);
        MOQ_TEST_CHECK(memcmp(out.value, "abc", 3) == 0);
    }

    /* -- Malformed even value still returns PROTO --------------------- */
    {
        /* delta=2 (type=2, even), then truncated 2-byte varint: 0x40 */
        uint8_t wire[] = { 0x02, 0x40 };
        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, wire, sizeof(wire));
        moq_kvp_entry_t out;
        MOQ_TEST_CHECK(moq_kvp_decode_next(&dec, &out) == MOQ_ERR_PROTO);
    }

    /* -- encode_varint_entry matches manual encoding ------------------ */
    {
        /* Manual: build entry, encode with moq_kvp_encode_entry. */
        uint8_t val_buf[8];
        size_t val_len = moq_quic_varint_encode(42, val_buf, sizeof(val_buf));
        moq_kvp_entry_t e = {
            .type = 4, .value = val_buf, .value_len = val_len,
            .is_varint = true,
        };
        uint8_t manual[16];
        size_t mlen = moq_kvp_encode_entry(0, &e, manual, sizeof(manual));
        MOQ_TEST_CHECK(mlen > 0);

        /* Convenience: moq_kvp_encode_varint_entry. */
        uint8_t conv[16];
        size_t clen = moq_kvp_encode_varint_entry(0, 4, 42,
            conv, sizeof(conv));
        MOQ_TEST_CHECK(clen > 0);
        MOQ_TEST_CHECK_EQ_SIZE(clen, mlen);
        MOQ_TEST_CHECK(memcmp(manual, conv, mlen) == 0);
    }

    /* -- encode_varint_entry with delta matches manual ----------------- */
    {
        uint8_t val_buf[8];
        size_t val_len = moq_quic_varint_encode(99, val_buf, sizeof(val_buf));
        moq_kvp_entry_t e = {
            .type = 6, .value = val_buf, .value_len = val_len,
            .is_varint = true,
        };
        uint8_t manual[16];
        size_t mlen = moq_kvp_encode_entry(2, &e, manual, sizeof(manual));
        MOQ_TEST_CHECK(mlen > 0);

        uint8_t conv[16];
        size_t clen = moq_kvp_encode_varint_entry(2, 6, 99,
            conv, sizeof(conv));
        MOQ_TEST_CHECK_EQ_SIZE(clen, mlen);
        MOQ_TEST_CHECK(memcmp(manual, conv, mlen) == 0);
    }

    /* -- encode_varint_entry rejects odd type ------------------------- */
    {
        uint8_t buf[16];
        MOQ_TEST_CHECK(moq_kvp_encode_varint_entry(0, 3, 42,
            buf, sizeof(buf)) == 0);
    }

    /* -- encode_varint_entry rejects non-ascending type --------------- */
    {
        uint8_t buf[16];
        MOQ_TEST_CHECK(moq_kvp_encode_varint_entry(10, 4, 42,
            buf, sizeof(buf)) == 0);
    }

    /* -- varint_entry_encoded_len matches actual output --------------- */
    {
        size_t expected = moq_kvp_varint_entry_encoded_len(0, 2, 1000000);
        MOQ_TEST_CHECK(expected > 0);
        uint8_t buf[16];
        size_t actual = moq_kvp_encode_varint_entry(0, 2, 1000000,
            buf, sizeof(buf));
        MOQ_TEST_CHECK_EQ_SIZE(actual, expected);
    }

    /* -- encode_varint_entry NULL buf returns 0 ---------------------- */
    {
        MOQ_TEST_CHECK(moq_kvp_encode_varint_entry(0, 2, 42, NULL, 16) == 0);
    }

    MOQ_TEST_PASS("test_kvp");
    return failures;
}
