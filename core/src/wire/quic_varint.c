#include "moq/wire.h"

size_t moq_quic_varint_len(uint64_t value)
{
    if (value <= 0x3F)             return 1;
    if (value <= 0x3FFF)           return 2;
    if (value <= 0x3FFFFFFF)       return 4;
    if (value <= MOQ_QUIC_VARINT_MAX) return 8;
    return 0;
}

size_t moq_quic_varint_encode(uint64_t value,
                               uint8_t *buf, size_t buf_len)
{
    if (!buf)
        return 0;

    size_t len = moq_quic_varint_len(value);
    if (len == 0 || buf_len < len)
        return 0;

    switch (len) {
    case 1:
        buf[0] = (uint8_t)value;
        break;
    case 2:
        buf[0] = (uint8_t)(0x40 | (value >> 8));
        buf[1] = (uint8_t)(value);
        break;
    case 4:
        buf[0] = (uint8_t)(0x80 | (value >> 24));
        buf[1] = (uint8_t)(value >> 16);
        buf[2] = (uint8_t)(value >> 8);
        buf[3] = (uint8_t)(value);
        break;
    case 8:
        buf[0] = (uint8_t)(0xC0 | (value >> 56));
        buf[1] = (uint8_t)(value >> 48);
        buf[2] = (uint8_t)(value >> 40);
        buf[3] = (uint8_t)(value >> 32);
        buf[4] = (uint8_t)(value >> 24);
        buf[5] = (uint8_t)(value >> 16);
        buf[6] = (uint8_t)(value >> 8);
        buf[7] = (uint8_t)(value);
        break;
    }

    return len;
}

size_t moq_quic_varint_decode(const uint8_t *buf, size_t buf_len,
                               uint64_t *out_value)
{
    if (!buf || buf_len == 0 || !out_value)
        return 0;

    size_t len = moq_quic_varint_encoded_len(buf[0]);
    if (buf_len < len)
        return 0;

    uint64_t v;
    switch (len) {
    case 1:
        v = buf[0] & 0x3Fu;
        break;
    case 2:
        v = ((uint64_t)(buf[0] & 0x3Fu) << 8)
          |  (uint64_t) buf[1];
        break;
    case 4:
        v = ((uint64_t)(buf[0] & 0x3Fu) << 24)
          | ((uint64_t) buf[1]          << 16)
          | ((uint64_t) buf[2]          << 8)
          |  (uint64_t) buf[3];
        break;
    case 8:
        v = ((uint64_t)(buf[0] & 0x3Fu) << 56)
          | ((uint64_t) buf[1]          << 48)
          | ((uint64_t) buf[2]          << 40)
          | ((uint64_t) buf[3]          << 32)
          | ((uint64_t) buf[4]          << 24)
          | ((uint64_t) buf[5]          << 16)
          | ((uint64_t) buf[6]          << 8)
          |  (uint64_t) buf[7];
        break;
    default:
        return 0;
    }

    *out_value = v;
    return len;
}
