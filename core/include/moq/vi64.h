#ifndef MOQ_VI64_H
#define MOQ_VI64_H

/*
 * Stability: low-level wire/tooling primitive used by the session engine,
 * adapters, fuzz targets, and tools - NOT the application-level session API.
 * Applications should normally include <moq/moq.h> or <moq/session.h>.
 *
 * vi64: MoQT variable-length integer (draft-ietf-moq-transport-18 §1.4.1).
 *
 * The number of leading 1-bits in the first byte gives the total encoded
 * length; the first 0-bit terminates the length prefix. The remaining bits of
 * the first byte plus the following bytes hold the value in network byte order
 * (big-endian).
 *
 *   first byte   length   value bits   max value
 *   0xxxxxxx       1          7         127
 *   10xxxxxx       2         14         16383
 *   110xxxxx       3         21         2097151
 *   1110xxxx       4         28         268435455
 *   11110xxx       5         35         34359738367
 *   111110xx       6         42         4398046511103
 *   1111110x       7         49         562949953421311
 *   11111110       8         56         72057594037927935
 *   11111111       9         64         18446744073709551615
 *
 * Range is the full 64-bit unsigned space. Unlike the QUIC varint, non-minimal
 * encodings are valid: any length able to represent the value is legal, and a
 * decoder MUST accept any of them. Encoders emit the minimum length.
 */

#include "export.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MOQ_VI64_MAX     UINT64_MAX
#define MOQ_VI64_MAX_LEN 9

/* Minimum number of bytes needed to encode `value` (1..9). */
MOQ_API size_t moq_vi64_len(uint64_t value);

/*
 * Encode `value` using the minimum length into `buf` (capacity `buf_len`).
 * Returns the number of bytes written (1..9), or 0 if buf is NULL or too small.
 */
MOQ_API size_t moq_vi64_encode(uint64_t value, uint8_t *buf, size_t buf_len);

/*
 * Decode a vi64 from `buf` (length `buf_len`) into *out_value. Accepts any
 * valid encoding, including non-minimal ones. Returns the number of bytes
 * consumed (1..9), or 0 if the buffer is empty, an argument is NULL, or the
 * buffer is shorter than the length the first byte indicates.
 */
MOQ_API size_t moq_vi64_decode(const uint8_t *buf, size_t buf_len,
                                uint64_t *out_value);

/*
 * Encoded length implied by a first byte, without a full decode: the leading
 * 1-bit count plus 1 (9 for 0xFF). Does not validate buffer length.
 */
static inline size_t moq_vi64_encoded_len(uint8_t first_byte)
{
    size_t n = 0;
    uint8_t b = first_byte;
    while (n < 8 && (b & 0x80u)) { n++; b = (uint8_t)(b << 1); }
    return n + 1;
}

#ifdef __cplusplus
}
#endif

#endif /* MOQ_VI64_H */
