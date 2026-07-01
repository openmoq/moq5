#ifndef MOQ_KVP_H
#define MOQ_KVP_H

/*
 * Stability: draft-specific wire/tooling API. This is NOT the
 * application-level session API; applications should normally include
 * <moq/moq.h> or <moq/session.h>. Wire/profile details may change across
 * drafts.
 *
 * Key-Value-Pair encode/decode for draft-16 (delta-encoded types).
 *
 * KVP is the wire primitive for parameters, properties, and extension
 * headers. Draft-16 uses delta-encoded type values; draft-14 uses
 * absolute. This module implements the delta variant.
 *
 * See draft-ietf-moq-transport-16 section 1.4.2.
 */

#include "export.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* -- KVP entry ----------------------------------------------------- */

typedef struct moq_kvp_entry {
    uint64_t       type;      /* resolved absolute type (not the delta) */
    const uint8_t *value;     /* borrowed; points into decode/encode buffer */
    size_t         value_len;
    bool           is_varint; /* true if type is even (value is a varint) */

    /* Raw span of the entire encoded entry (delta + value on wire).
     * Set by the decoder for byte-exact relay forwarding of unknown
     * entries. NULL/0 when constructed for encoding. */
    const uint8_t *raw;
    size_t         raw_len;
} moq_kvp_entry_t;

/* -- Decode -------------------------------------------------------- */

/*
 * Incremental KVP decoder state. Initialize with moq_kvp_decoder_init.
 * Call moq_kvp_decode_next repeatedly to consume entries.
 */
typedef struct moq_kvp_decoder {
    const uint8_t *buf;
    size_t         remaining;
    uint64_t       prev_type; /* accumulated type for delta decode */
    bool           first;     /* true before the first entry */
} moq_kvp_decoder_t;

/*
 * Initialize a decoder over a raw KVP byte span.
 *
 * The caller is responsible for reading any count/length prefix from
 * the surrounding message structure before calling this. The decoder
 * consumes bytes until remaining == 0 or an error occurs.
 */
MOQ_API void moq_kvp_decoder_init(moq_kvp_decoder_t *dec,
                                   const uint8_t *buf, size_t len);

/*
 * Decode the next KVP entry.
 *
 * Returns MOQ_OK and fills *entry on success.
 * Returns MOQ_DONE when all bytes are consumed (remaining == 0).
 * Returns MOQ_ERR_PROTO on malformed data:
 *   - delta overflow (accumulated type > 2^64 - 1)
 *   - odd-type value length exceeds remaining bytes
 *   - odd-type value length exceeds 2^16 - 1
 *   - truncated varint
 *
 * entry->value is borrowed from the decode buffer.
 * entry->type is the resolved absolute type.
 * entry->is_varint is true when type is even.
 * entry->raw / raw_len span the entire encoded entry for relay forwarding.
 *
 * For even types, entry->value points to the raw varint bytes and
 * entry->value_len is the varint's encoded length. The caller can
 * decode it with moq_quic_varint_decode(entry->value, entry->value_len, &v).
 *
 * After any error return, the decoder state is invalid and must not
 * be reused. Errors are terminal for the current KVP sequence.
 */
MOQ_API moq_result_t moq_kvp_decode_next(moq_kvp_decoder_t *dec,
                                          moq_kvp_entry_t *entry);

/* -- Encode -------------------------------------------------------- */

/*
 * Encode a single KVP entry in delta form, appending to buf.
 * `prev_type` is the absolute type of the previous entry (0 if first).
 *
 * For even types: value/value_len must be exactly one valid QUIC varint.
 * For odd types: value/value_len is the raw byte field (value_len <= 2^16-1).
 *
 * Returns the number of bytes written on success (always > 0).
 * Returns 0 on any error; all failure kinds collapse to a single
 * sentinel. Callers needing granular errors should validate inputs
 * before calling. Returns 0 if:
 *   - buf or entry is NULL
 *   - entry->type < prev_type (non-ascending)
 *   - entry->is_varint does not match type parity
 *   - even-type value is not a valid single QUIC varint
 *   - odd-type value_len > 0 with value == NULL
 *   - odd-type value_len > 2^16 - 1
 *   - buf_len is too small
 */
MOQ_API size_t moq_kvp_encode_entry(uint64_t prev_type,
                                     const moq_kvp_entry_t *entry,
                                     uint8_t *buf, size_t buf_len);

/*
 * Compute the encoded size of a single KVP entry in delta form.
 * Returns 0 on any validation failure (same conditions as encode).
 */
MOQ_API size_t moq_kvp_entry_encoded_len(uint64_t prev_type,
                                          const moq_kvp_entry_t *entry);

/*
 * Encode a single even-type KVP entry from a type + uint64 value.
 *
 * Convenience wrapper: encodes the varint value internally so the
 * caller does not need a temporary buffer. type must be even.
 *
 * Returns the number of bytes written on success (always > 0).
 * Returns 0 on error (odd type, value > QUIC varint max,
 * type < prev_type, buf too small).
 */
MOQ_API size_t moq_kvp_encode_varint_entry(uint64_t prev_type,
                                            uint64_t type,
                                            uint64_t value,
                                            uint8_t *buf,
                                            size_t buf_len);

/*
 * Compute the encoded size of a single even-type KVP entry.
 * Returns 0 on any validation failure.
 */
MOQ_API size_t moq_kvp_varint_entry_encoded_len(uint64_t prev_type,
                                                 uint64_t type,
                                                 uint64_t value);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_KVP_H */
