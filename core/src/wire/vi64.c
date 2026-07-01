#include "moq/vi64.h"

size_t moq_vi64_len(uint64_t value)
{
    if (value <= 0x7FULL)             return 1;
    if (value <= 0x3FFFULL)           return 2;
    if (value <= 0x1FFFFFULL)         return 3;
    if (value <= 0xFFFFFFFULL)        return 4;
    if (value <= 0x7FFFFFFFFULL)      return 5;
    if (value <= 0x3FFFFFFFFFFULL)    return 6;
    if (value <= 0x1FFFFFFFFFFFFULL)  return 7;
    if (value <= 0xFFFFFFFFFFFFFFULL) return 8;
    return 9;
}

size_t moq_vi64_encode(uint64_t value, uint8_t *buf, size_t buf_len)
{
    if (!buf)
        return 0;

    size_t len = moq_vi64_len(value);
    if (buf_len < len)
        return 0;

    if (len == 9) {
        /* First byte is all prefix (0xFF); value occupies 8 bytes. */
        buf[0] = 0xFFu;
        for (size_t i = 0; i < 8; i++)
            buf[1 + i] = (uint8_t)(value >> (8 * (7 - i)));
        return 9;
    }

    /* Prefix: (len-1) leading 1-bits then a 0-bit. The value's high (8-len)
     * bits share the first byte; the rest follow big-endian. */
    uint8_t prefix = (len == 1)
        ? 0u
        : (uint8_t)(((1u << (len - 1)) - 1u) << (9 - len));
    buf[0] = (uint8_t)(prefix | (uint8_t)(value >> (8 * (len - 1))));
    for (size_t i = 1; i < len; i++)
        buf[i] = (uint8_t)(value >> (8 * (len - 1 - i)));
    return len;
}

size_t moq_vi64_decode(const uint8_t *buf, size_t buf_len, uint64_t *out_value)
{
    if (!buf || buf_len == 0 || !out_value)
        return 0;

    size_t len = moq_vi64_encoded_len(buf[0]);
    if (buf_len < len)
        return 0;

    uint64_t v;
    if (len == 9) {
        v = 0;
        for (size_t i = 0; i < 8; i++)
            v = (v << 8) | (uint64_t)buf[1 + i];
    } else {
        /* Low (8-len) bits of the first byte are the value's high bits. */
        uint8_t mask = (uint8_t)(0xFFu >> len);
        v = (uint64_t)(buf[0] & mask);
        for (size_t i = 1; i < len; i++)
            v = (v << 8) | (uint64_t)buf[i];
    }

    *out_value = v;
    return len;
}
