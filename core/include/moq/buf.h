#ifndef MOQ_BUF_H
#define MOQ_BUF_H

/*
 * Stability: low-level wire/tooling primitive used by the session engine,
 * adapters, fuzz targets, and tools - NOT the application-level session
 * API. Applications should normally include <moq/moq.h> or
 * <moq/session.h>. (Generic across profiles, not strictly draft-16.)
 *
 * Zero-allocation buffer cursor helpers for reading and writing wire
 * formats. Public so adapters, tools, and fuzz targets can use them.
 *
 * Reader: borrows a const byte span; advances a cursor on success.
 * Writer: borrows a mutable byte span with a capacity; advances on success.
 *
 * All read/write functions return MOQ_OK on success or MOQ_ERR_BUFFER
 * on truncation. On failure the cursor is NOT advanced (no partial reads
 * or writes) unless explicitly documented otherwise.
 */

#include "export.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* -- Reader -------------------------------------------------------- */

typedef struct moq_buf_reader {
    const uint8_t *data;
    size_t         len;
    size_t         pos;
} moq_buf_reader_t;

static inline void moq_buf_reader_init(moq_buf_reader_t *r,
                                        const uint8_t *data, size_t len)
{
    r->data = data;
    r->len  = len;
    r->pos  = 0;
}

static inline size_t moq_buf_reader_remaining(const moq_buf_reader_t *r)
{
    if (!r || r->pos > r->len) return 0;
    return r->len - r->pos;
}

static inline size_t moq_buf_reader_offset(const moq_buf_reader_t *r)
{
    return r ? r->pos : 0;
}

static inline const uint8_t *moq_buf_reader_ptr(const moq_buf_reader_t *r)
{
    if (!r || !r->data || r->pos > r->len) return NULL;
    return r->data + r->pos;
}

/* Read a QUIC varint. */
MOQ_API moq_result_t moq_buf_read_varint(moq_buf_reader_t *r,
                                          uint64_t *out);

/* Read a MoQT vi64 variable-length integer. */
MOQ_API moq_result_t moq_buf_read_vi64(moq_buf_reader_t *r,
                                        uint64_t *out);

/* Read a big-endian uint16 (for control message Length fields). */
MOQ_API moq_result_t moq_buf_read_uint16(moq_buf_reader_t *r,
                                          uint16_t *out);

/* Read a varint-length-prefixed byte span (borrowed from the buffer). */
MOQ_API moq_result_t moq_buf_read_span(moq_buf_reader_t *r,
                                        moq_bytes_t *out);

/* Read exactly `len` raw bytes (borrowed from the buffer). */
MOQ_API moq_result_t moq_buf_read_raw(moq_buf_reader_t *r,
                                       size_t len, moq_bytes_t *out);

/* Skip `len` bytes. */
MOQ_API moq_result_t moq_buf_skip(moq_buf_reader_t *r, size_t len);

/* -- Writer -------------------------------------------------------- */

typedef struct moq_buf_writer {
    uint8_t *data;
    size_t   cap;
    size_t   pos;
} moq_buf_writer_t;

static inline void moq_buf_writer_init(moq_buf_writer_t *w,
                                        uint8_t *data, size_t cap)
{
    w->data = data;
    w->cap  = cap;
    w->pos  = 0;
}

static inline size_t moq_buf_writer_remaining(const moq_buf_writer_t *w)
{
    if (!w || w->pos > w->cap) return 0;
    return w->cap - w->pos;
}

static inline size_t moq_buf_writer_offset(const moq_buf_writer_t *w)
{
    return w ? w->pos : 0;
}

static inline uint8_t *moq_buf_writer_ptr(moq_buf_writer_t *w)
{
    if (!w || !w->data || w->pos > w->cap) return NULL;
    return w->data + w->pos;
}

/* Write a QUIC varint. */
MOQ_API moq_result_t moq_buf_write_varint(moq_buf_writer_t *w,
                                           uint64_t value);

/* Write a MoQT vi64 variable-length integer (minimum length). */
MOQ_API moq_result_t moq_buf_write_vi64(moq_buf_writer_t *w,
                                         uint64_t value);

/* Write a big-endian uint16. */
MOQ_API moq_result_t moq_buf_write_uint16(moq_buf_writer_t *w,
                                           uint16_t value);

/* Write a varint-length-prefixed byte span. */
MOQ_API moq_result_t moq_buf_write_span(moq_buf_writer_t *w,
                                         moq_bytes_t span);

/* Write raw bytes. */
MOQ_API moq_result_t moq_buf_write_raw(moq_buf_writer_t *w,
                                        const uint8_t *data, size_t len);

/*
 * Reserve a uint16 slot at the current position (writes 0x0000) and
 * return the offset. After writing the payload, call moq_buf_patch_uint16
 * to fill in the actual length.
 *
 * Useful for control message framing: reserve the Length field, write
 * the payload, then patch with (writer.pos - payload_start).
 */
MOQ_API moq_result_t moq_buf_reserve_uint16(moq_buf_writer_t *w,
                                             size_t *out_offset);

/* Patch a previously reserved uint16 at the given offset. */
MOQ_API moq_result_t moq_buf_patch_uint16(moq_buf_writer_t *w,
                                           size_t offset, uint16_t value);

/* -- Namespace encode/decode --------------------------------------- */

/*
 * Decode a Track Namespace tuple (count + parts) from the reader.
 * Writes parts into the caller-provided array (up to max_parts).
 * Enforces draft-16 general namespace constraints:
 *   - 1..32 fields
 *   - each field >= 1 byte
 *   - total field value bytes <= 4096
 *
 * Returns MOQ_ERR_PROTO if constraints are violated.
 * Returns MOQ_ERR_BUFFER if the caller's parts array is too small.
 */
MOQ_API moq_result_t moq_buf_read_namespace(moq_buf_reader_t *r,
                                             moq_bytes_t *parts,
                                             size_t max_parts,
                                             moq_namespace_t *out);

/*
 * Encode a Track Namespace tuple (count + parts) into the writer.
 * Enforces the same constraints as the reader.
 */
MOQ_API moq_result_t moq_buf_write_namespace(moq_buf_writer_t *w,
                                              const moq_namespace_t *ns);

/*
 * Decode a Track Namespace Prefix (0..32 fields).
 * Used by SUBSCRIBE_NAMESPACE where a zero-field prefix means "match all."
 * Constraints:
 *   - 0..32 fields
 *   - each field >= 1 byte (if present)
 *   - total field value bytes <= 4096
 *
 * When count is 0, parts may be NULL. out->parts is set to the caller's
 * array (possibly NULL) and out->count is 0. No fields are read.
 */
MOQ_API moq_result_t moq_buf_read_namespace_prefix(moq_buf_reader_t *r,
                                                    moq_bytes_t *parts,
                                                    size_t max_parts,
                                                    moq_namespace_t *out);

/*
 * Encode a Track Namespace Prefix (0..32 fields).
 * Allows count == 0 (writes just the count varint).
 */
MOQ_API moq_result_t moq_buf_write_namespace_prefix(moq_buf_writer_t *w,
                                                     const moq_namespace_t *ns);

#ifdef __cplusplus
}
#endif

#endif /* MOQ_BUF_H */
