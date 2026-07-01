#ifndef MOQ_WIRE_H
#define MOQ_WIRE_H

/*
 * Stability: low-level wire/tooling primitive used by the session engine,
 * adapters, fuzz targets, and tools - NOT the application-level session
 * API. Applications should normally include <moq/moq.h> or
 * <moq/session.h>. (Generic across profiles, not strictly draft-16.)
 *
 * QUIC varint encode/decode (RFC 9000 §16).
 *
 * Named moq_quic_varint_* per docs/PROTOCOL_EVOLUTION.md §7.1. Retained for
 * draft-16 and other QUIC-varint tooling; the MoQT vi64 variable-length
 * integer (draft-18) lives in <moq/vi64.h>.
 *
 * Max value: 2^62 - 1 (4611686018427387903).
 * Max encoded length: 8 bytes.
 */

#include "export.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MOQ_QUIC_VARINT_MAX     ((uint64_t)0x3FFFFFFFFFFFFFFFULL)
#define MOQ_QUIC_VARINT_MAX_LEN 8

/*
 * Return the number of bytes needed to encode `value` using minimum encoding.
 * Returns 0 if value > MOQ_QUIC_VARINT_MAX.
 */
MOQ_API size_t moq_quic_varint_len(uint64_t value);

/*
 * Encode `value` into `buf` of size `buf_len` using minimum encoding.
 *
 * Returns the number of bytes written (1, 2, 4, or 8) on success.
 * Returns 0 and writes nothing if value > MOQ_QUIC_VARINT_MAX or
 * buf_len is too small.
 */
MOQ_API size_t moq_quic_varint_encode(uint64_t value,
                                       uint8_t *buf, size_t buf_len);

/*
 * Decode a varint from `buf` of size `buf_len`.
 * On success, writes the decoded value to *out_value and returns the
 * number of bytes consumed (1, 2, 4, or 8).
 *
 * Returns 0 if buf_len is 0 or less than the length indicated by the
 * first byte (need more data).
 */
MOQ_API size_t moq_quic_varint_decode(const uint8_t *buf, size_t buf_len,
                                       uint64_t *out_value);

/*
 * Read the encoded length from the first byte without full decode.
 * Returns 1, 2, 4, or 8. Does not validate buf_len.
 */
static inline size_t moq_quic_varint_encoded_len(uint8_t first_byte)
{
    return (size_t)1 << (first_byte >> 6);
}

#ifdef __cplusplus
}
#endif

#endif /* MOQ_WIRE_H */
