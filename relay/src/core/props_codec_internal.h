#ifndef MOQR_PROPS_CODEC_INTERNAL_H
#define MOQR_PROPS_CODEC_INTERNAL_H

#include <moq/relay/log.h>

#include <stddef.h>
#include <stdint.h>

/*
 * Same-build internal interface between relay-core and the relay runtime.
 * Never installed and not an SDK API.
 *
 * An Object or Track Property block is a sequence of Key-Value-Pairs whose
 * Delta Type, odd-type Length and even-type value are integers in the wire
 * draft's own encoding: MOQR_PROPS_CODEC_QUIC_VARINT on draft-16 (its 1.4.2,
 * "(i)", at most 2^62-1), MOQR_PROPS_CODEC_VI64 on draft-18 and draft-22
 * (draft-18 1.4.1/1.4.3, "(vi64)"). The grammar is otherwise the same: an
 * even Type's Value is one integer, an odd Type's Value is Length bytes
 * (Length at most 2^16-1), and Types accumulate from 0 (at most 2^64-1).
 *
 * moqr_core_internal_props_convert re-encodes a block for another codec,
 * losslessly: the same Types in the same order, the same even values, the
 * same odd bytes. Immutable Properties (Type 0xB, draft-18 12.7; draft-16
 * 11.2 Immutable Extensions) are never re-encoded, stripped or moved: the
 * whole entry, its Delta Type and Length included, is kept byte-exact, and
 * only when those bytes and every Key-Value-Pair they protect read the same
 * -- the same Types, values and bytes -- in both codecs. A block that cannot
 * be carried that way is UNSUPPORTED for that destination; a block that does
 * not parse in its own codec is MALFORMED.
 */

typedef enum moqr_props_conv {
    MOQR_PROPS_CONV_SAME = 0,      /* forward the input bytes unchanged      */
    MOQR_PROPS_CONV_CONVERTED,     /* the destination encoding (out_len)     */
    MOQR_PROPS_CONV_UNSUPPORTED,   /* valid, not representable at the
                                    * destination                            */
    MOQR_PROPS_CONV_MALFORMED,     /* does not parse in its own codec        */
    MOQR_PROPS_CONV_UNKNOWN,       /* nonempty bytes whose codec is unknown  */
    MOQR_PROPS_CONV_INVAL,         /* a codec tag out of range, or a NULL
                                    * buffer with nonzero length             */
    MOQR_PROPS_CONV_BUFFER,        /* out too small: *out_len is the need   */
} moqr_props_conv_t;

/*
 * Convert `len` bytes `in` from `src_codec` to `dst_codec`.
 *
 * SAME, with *out_len = len and nothing written, for an empty block or equal
 * known codecs: the caller forwards its input, which the ingesting session
 * already validated (SAME does not validate arbitrary input). A nonempty
 * block of UNKNOWN codec is UNKNOWN, never SAME. Otherwise the whole source
 * is read before the destination is judged, so a source malformed anywhere
 * -- including inside Immutable Properties -- is MALFORMED even where an
 * earlier pair was unrepresentable. With `out` NULL the block is measured:
 * CONVERTED with *out_len the exact output size and nothing written. With a
 * buffer, BUFFER (and *out_len the exact need) when `cap` is short, before
 * any byte is written; otherwise CONVERTED with exactly *out_len bytes. A
 * dst_codec of MOQR_PROPS_CODEC_UNKNOWN, a bad tag, or an output size past
 * SIZE_MAX is INVAL. Pure: no allocation, no state.
 */
MOQR_CORE_API moqr_props_conv_t moqr_core_internal_props_convert(
    uint8_t src_codec, uint8_t dst_codec, const uint8_t *in, size_t len,
    uint8_t *out, size_t cap, size_t *out_len);

#endif
