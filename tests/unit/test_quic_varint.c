#include <moq/wire.h>
#include "test_support.h"
#include <string.h>

static int test_encode_decode_roundtrip(uint64_t value, size_t expected_len)
{
    int failures = 0;
    uint8_t buf[8];
    memset(buf, 0xFF, sizeof(buf));

    size_t enc_len = moq_quic_varint_encode(value, buf, sizeof(buf));
    MOQ_TEST_CHECK(enc_len == expected_len);

    MOQ_TEST_CHECK(moq_quic_varint_len(value) == expected_len);

    uint64_t decoded = 0;
    size_t dec_len = moq_quic_varint_decode(buf, enc_len, &decoded);
    MOQ_TEST_CHECK(dec_len == expected_len);
    MOQ_TEST_CHECK(decoded == value);

    return failures;
}

int main(void)
{
    int failures = 0;

    /* 1-byte: 0..63 */
    failures += test_encode_decode_roundtrip(0, 1);
    failures += test_encode_decode_roundtrip(1, 1);
    failures += test_encode_decode_roundtrip(63, 1);

    /* 2-byte: 64..16383 */
    failures += test_encode_decode_roundtrip(64, 2);
    failures += test_encode_decode_roundtrip(16383, 2);

    /* 4-byte: 16384..1073741823 */
    failures += test_encode_decode_roundtrip(16384, 4);
    failures += test_encode_decode_roundtrip(1073741823ULL, 4);

    /* 8-byte: 1073741824..2^62-1 */
    failures += test_encode_decode_roundtrip(1073741824ULL, 8);
    failures += test_encode_decode_roundtrip(MOQ_QUIC_VARINT_MAX, 8);

    /* Value too large. */
    MOQ_TEST_CHECK(moq_quic_varint_len(MOQ_QUIC_VARINT_MAX + 1) == 0);
    {
        uint8_t buf[8];
        MOQ_TEST_CHECK(moq_quic_varint_encode(MOQ_QUIC_VARINT_MAX + 1, buf, 8) == 0);
    }

    /* Buffer too small for encode. */
    {
        uint8_t buf[1];
        MOQ_TEST_CHECK(moq_quic_varint_encode(64, buf, 1) == 0);  /* needs 2 bytes */
    }

    /* Decode with insufficient data. */
    {
        uint8_t buf[] = { 0x40 };  /* 2-byte encoding, but only 1 byte given */
        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_quic_varint_decode(buf, 1, &v) == 0);
    }

    /* Decode empty buffer. */
    {
        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_quic_varint_decode(NULL, 0, &v) == 0);
    }

    /* encoded_len from first byte. */
    MOQ_TEST_CHECK(moq_quic_varint_encoded_len(0x00) == 1);
    MOQ_TEST_CHECK(moq_quic_varint_encoded_len(0x3F) == 1);
    MOQ_TEST_CHECK(moq_quic_varint_encoded_len(0x40) == 2);
    MOQ_TEST_CHECK(moq_quic_varint_encoded_len(0x7F) == 2);
    MOQ_TEST_CHECK(moq_quic_varint_encoded_len(0x80) == 4);
    MOQ_TEST_CHECK(moq_quic_varint_encoded_len(0xBF) == 4);
    MOQ_TEST_CHECK(moq_quic_varint_encoded_len(0xC0) == 8);
    MOQ_TEST_CHECK(moq_quic_varint_encoded_len(0xFF) == 8);

    /* NULL pointer guards. */
    MOQ_TEST_CHECK(moq_quic_varint_encode(0, NULL, 8) == 0);
    {
        uint8_t buf[] = { 0x25 };
        MOQ_TEST_CHECK(moq_quic_varint_decode(buf, 1, NULL) == 0);
    }
    {
        uint64_t v = 0;
        MOQ_TEST_CHECK(moq_quic_varint_decode(NULL, 5, &v) == 0);
    }

    /* Non-minimum encoding decode (receivers MUST accept any valid encoding). */
    {
        /* Encode 1 in 2-byte form: 0x40, 0x01 */
        uint8_t buf[] = { 0x40, 0x01 };
        uint64_t v = 0;
        size_t n = moq_quic_varint_decode(buf, 2, &v);
        MOQ_TEST_CHECK(n == 2);
        MOQ_TEST_CHECK(v == 1);
    }

    /* RFC 9000 Appendix A test vectors. */
    {
        /* 151288809941952652 → C2197C5EFF14E88C */
        uint8_t expected[] = { 0xC2, 0x19, 0x7C, 0x5E, 0xFF, 0x14, 0xE8, 0x8C };
        uint64_t val = 151288809941952652ULL;

        uint8_t buf[8];
        size_t n = moq_quic_varint_encode(val, buf, 8);
        MOQ_TEST_CHECK(n == 8);
        MOQ_TEST_CHECK(memcmp(buf, expected, 8) == 0);

        uint64_t decoded = 0;
        n = moq_quic_varint_decode(expected, 8, &decoded);
        MOQ_TEST_CHECK(n == 8);
        MOQ_TEST_CHECK(decoded == val);
    }
    {
        /* 494878333 → 9D7F3E7D */
        uint8_t expected[] = { 0x9D, 0x7F, 0x3E, 0x7D };
        uint64_t val = 494878333ULL;

        uint8_t buf[4];
        size_t n = moq_quic_varint_encode(val, buf, 4);
        MOQ_TEST_CHECK(n == 4);
        MOQ_TEST_CHECK(memcmp(buf, expected, 4) == 0);

        uint64_t decoded = 0;
        n = moq_quic_varint_decode(expected, 4, &decoded);
        MOQ_TEST_CHECK(n == 4);
        MOQ_TEST_CHECK(decoded == val);
    }
    {
        /* 15293 → 7BBD */
        uint8_t expected[] = { 0x7B, 0xBD };
        uint64_t val = 15293ULL;

        uint8_t buf[2];
        size_t n = moq_quic_varint_encode(val, buf, 2);
        MOQ_TEST_CHECK(n == 2);
        MOQ_TEST_CHECK(memcmp(buf, expected, 2) == 0);

        uint64_t decoded = 0;
        n = moq_quic_varint_decode(expected, 2, &decoded);
        MOQ_TEST_CHECK(n == 2);
        MOQ_TEST_CHECK(decoded == val);
    }
    {
        /* 37 → 25 */
        uint8_t expected[] = { 0x25 };
        uint64_t val = 37ULL;

        uint8_t buf[1];
        size_t n = moq_quic_varint_encode(val, buf, 1);
        MOQ_TEST_CHECK(n == 1);
        MOQ_TEST_CHECK(memcmp(buf, expected, 1) == 0);

        uint64_t decoded = 0;
        n = moq_quic_varint_decode(expected, 1, &decoded);
        MOQ_TEST_CHECK(n == 1);
        MOQ_TEST_CHECK(decoded == val);
    }

    MOQ_TEST_PASS("test_quic_varint");
    return failures;
}
