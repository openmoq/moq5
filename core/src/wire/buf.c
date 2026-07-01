#include "moq/buf.h"
#include "moq/wire.h"
#include "moq/vi64.h"
#include <string.h>

/* -- Reader -------------------------------------------------------- */

static bool reader_valid(const moq_buf_reader_t *r)
{
    if (!r) return false;
    if (r->pos > r->len) return false;
    if (r->len > 0 && !r->data) return false;
    return true;
}

static bool writer_valid(const moq_buf_writer_t *w)
{
    if (!w) return false;
    if (w->pos > w->cap) return false;
    if (w->cap > 0 && !w->data) return false;
    return true;
}

moq_result_t moq_buf_read_varint(moq_buf_reader_t *r, uint64_t *out)
{
    if (!reader_valid(r) || !out)
        return MOQ_ERR_INVAL;
    size_t remaining = r->len - r->pos;
    if (remaining == 0)
        return MOQ_ERR_BUFFER;
    size_t n = moq_quic_varint_decode(r->data + r->pos, remaining, out);
    if (n == 0)
        return MOQ_ERR_BUFFER;
    r->pos += n;
    return MOQ_OK;
}

moq_result_t moq_buf_read_vi64(moq_buf_reader_t *r, uint64_t *out)
{
    if (!reader_valid(r) || !out)
        return MOQ_ERR_INVAL;
    size_t remaining = r->len - r->pos;
    if (remaining == 0)
        return MOQ_ERR_BUFFER;
    size_t n = moq_vi64_decode(r->data + r->pos, remaining, out);
    if (n == 0)
        return MOQ_ERR_BUFFER;
    r->pos += n;
    return MOQ_OK;
}

moq_result_t moq_buf_read_uint16(moq_buf_reader_t *r, uint16_t *out)
{
    if (!reader_valid(r) || !out)
        return MOQ_ERR_INVAL;
    if (r->len - r->pos < 2)
        return MOQ_ERR_BUFFER;
    *out = (uint16_t)((r->data[r->pos] << 8) | r->data[r->pos + 1]);
    r->pos += 2;
    return MOQ_OK;
}

moq_result_t moq_buf_read_span(moq_buf_reader_t *r, moq_bytes_t *out)
{
    if (!reader_valid(r) || !out)
        return MOQ_ERR_INVAL;
    uint64_t len = 0;
    size_t saved = r->pos;
    moq_result_t rc = moq_buf_read_varint(r, &len);
    if (rc < 0) return rc;

    if (len > r->len - r->pos) {
        r->pos = saved;
        return MOQ_ERR_BUFFER;
    }
    out->data = (len == 0) ? moq_buf_reader_ptr(r) : r->data + r->pos;
    out->len  = (size_t)len;
    r->pos += (size_t)len;
    return MOQ_OK;
}

moq_result_t moq_buf_read_raw(moq_buf_reader_t *r, size_t len,
                               moq_bytes_t *out)
{
    if (!reader_valid(r) || !out)
        return MOQ_ERR_INVAL;
    if (len > r->len - r->pos)
        return MOQ_ERR_BUFFER;
    out->data = (len == 0) ? moq_buf_reader_ptr(r) : r->data + r->pos;
    out->len  = len;
    r->pos += len;
    return MOQ_OK;
}

moq_result_t moq_buf_skip(moq_buf_reader_t *r, size_t len)
{
    if (!reader_valid(r))
        return MOQ_ERR_INVAL;
    if (len > r->len - r->pos)
        return MOQ_ERR_BUFFER;
    r->pos += len;
    return MOQ_OK;
}

/* -- Writer -------------------------------------------------------- */

moq_result_t moq_buf_write_varint(moq_buf_writer_t *w, uint64_t value)
{
    if (!writer_valid(w))
        return MOQ_ERR_INVAL;
    if (moq_quic_varint_len(value) == 0)
        return MOQ_ERR_INVAL;
    size_t remaining = w->cap - w->pos;
    if (remaining == 0)
        return MOQ_ERR_BUFFER;
    size_t n = moq_quic_varint_encode(value, w->data + w->pos, remaining);
    if (n == 0)
        return MOQ_ERR_BUFFER;
    w->pos += n;
    return MOQ_OK;
}

moq_result_t moq_buf_write_vi64(moq_buf_writer_t *w, uint64_t value)
{
    if (!writer_valid(w))
        return MOQ_ERR_INVAL;
    size_t remaining = w->cap - w->pos;
    if (remaining == 0)
        return MOQ_ERR_BUFFER;
    size_t n = moq_vi64_encode(value, w->data + w->pos, remaining);
    if (n == 0)
        return MOQ_ERR_BUFFER;
    w->pos += n;
    return MOQ_OK;
}

moq_result_t moq_buf_write_uint16(moq_buf_writer_t *w, uint16_t value)
{
    if (!writer_valid(w))
        return MOQ_ERR_INVAL;
    if (w->cap - w->pos < 2)
        return MOQ_ERR_BUFFER;
    w->data[w->pos]     = (uint8_t)(value >> 8);
    w->data[w->pos + 1] = (uint8_t)(value);
    w->pos += 2;
    return MOQ_OK;
}

moq_result_t moq_buf_write_span(moq_buf_writer_t *w, moq_bytes_t span)
{
    if (!writer_valid(w))
        return MOQ_ERR_INVAL;
    if (span.len > 0 && !span.data)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_varint(w, span.len);
    if (rc < 0) return rc;

    if (span.len > 0) {
        if (span.len > w->cap - w->pos) {
            w->pos = saved;
            return MOQ_ERR_BUFFER;
        }
        memcpy(w->data + w->pos, span.data, span.len);
        w->pos += span.len;
    }
    return MOQ_OK;
}

moq_result_t moq_buf_write_raw(moq_buf_writer_t *w,
                                const uint8_t *data, size_t len)
{
    if (!writer_valid(w))
        return MOQ_ERR_INVAL;
    if (len > 0 && !data)
        return MOQ_ERR_INVAL;
    if (len > w->cap - w->pos)
        return MOQ_ERR_BUFFER;
    if (len > 0) {
        memcpy(w->data + w->pos, data, len);
        w->pos += len;
    }
    return MOQ_OK;
}

moq_result_t moq_buf_reserve_uint16(moq_buf_writer_t *w,
                                     size_t *out_offset)
{
    if (!writer_valid(w) || !out_offset)
        return MOQ_ERR_INVAL;
    if (w->cap - w->pos < 2)
        return MOQ_ERR_BUFFER;
    *out_offset = w->pos;
    w->data[w->pos]     = 0;
    w->data[w->pos + 1] = 0;
    w->pos += 2;
    return MOQ_OK;
}

moq_result_t moq_buf_patch_uint16(moq_buf_writer_t *w,
                                   size_t offset, uint16_t value)
{
    if (!writer_valid(w))
        return MOQ_ERR_INVAL;
    if (offset > w->pos || w->pos - offset < 2)
        return MOQ_ERR_INVAL;
    w->data[offset]     = (uint8_t)(value >> 8);
    w->data[offset + 1] = (uint8_t)(value);
    return MOQ_OK;
}

/* -- Namespace (shared) -------------------------------------------- */

/*
 * Internal: read `count` namespace fields, enforcing per-field and
 * total-size constraints. Caller has already decoded the count varint.
 */
static moq_result_t ns_read_fields(moq_buf_reader_t *r,
                                    moq_bytes_t *parts, uint64_t count,
                                    size_t saved)
{
    size_t total_bytes = 0;
    for (uint64_t i = 0; i < count; i++) {
        moq_result_t rc = moq_buf_read_span(r, &parts[i]);
        if (rc < 0) {
            r->pos = saved;
            return rc;
        }
        if (parts[i].len == 0) {
            r->pos = saved;
            return MOQ_ERR_PROTO;
        }
        if (parts[i].len > 4096 - total_bytes) {
            r->pos = saved;
            return MOQ_ERR_PROTO;
        }
        total_bytes += parts[i].len;
    }
    return MOQ_OK;
}

/*
 * Internal: validate namespace fields for writing.
 * Returns MOQ_ERR_INVAL on constraint violation.
 */
static moq_result_t ns_validate_fields(const moq_namespace_t *ns)
{
    if (ns->count > 0 && !ns->parts)
        return MOQ_ERR_INVAL;
    size_t total_bytes = 0;
    for (size_t i = 0; i < ns->count; i++) {
        if (ns->parts[i].len == 0)
            return MOQ_ERR_INVAL;
        if (ns->parts[i].len > 4096 - total_bytes)
            return MOQ_ERR_INVAL;
        total_bytes += ns->parts[i].len;
    }
    return MOQ_OK;
}

static moq_result_t ns_write_fields(moq_buf_writer_t *w,
                                     const moq_namespace_t *ns,
                                     size_t saved)
{
    moq_result_t rc = moq_buf_write_varint(w, ns->count);
    if (rc < 0) return rc;

    for (size_t i = 0; i < ns->count; i++) {
        rc = moq_buf_write_span(w, ns->parts[i]);
        if (rc < 0) {
            w->pos = saved;
            return rc;
        }
    }
    return MOQ_OK;
}

/* -- Namespace (general: 1..32 fields) ----------------------------- */

moq_result_t moq_buf_read_namespace(moq_buf_reader_t *r,
                                     moq_bytes_t *parts,
                                     size_t max_parts,
                                     moq_namespace_t *out)
{
    if (!r || !parts || !out)
        return MOQ_ERR_INVAL;

    size_t saved = r->pos;
    uint64_t count = 0;
    moq_result_t rc = moq_buf_read_varint(r, &count);
    if (rc < 0) return rc;

    if (count == 0 || count > 32) {
        r->pos = saved;
        return MOQ_ERR_PROTO;
    }
    if (count > max_parts) {
        r->pos = saved;
        return MOQ_ERR_BUFFER;
    }

    rc = ns_read_fields(r, parts, count, saved);
    if (rc < 0) return rc;

    out->parts = parts;
    out->count = (size_t)count;
    return MOQ_OK;
}

moq_result_t moq_buf_write_namespace(moq_buf_writer_t *w,
                                      const moq_namespace_t *ns)
{
    if (!w || !ns)
        return MOQ_ERR_INVAL;
    if (ns->count == 0 || ns->count > 32)
        return MOQ_ERR_INVAL;

    moq_result_t rc = ns_validate_fields(ns);
    if (rc < 0) return rc;

    size_t saved = w->pos;
    return ns_write_fields(w, ns, saved);
}

/* -- Namespace prefix (0..32 fields) ------------------------------- */

moq_result_t moq_buf_read_namespace_prefix(moq_buf_reader_t *r,
                                            moq_bytes_t *parts,
                                            size_t max_parts,
                                            moq_namespace_t *out)
{
    if (!r || !out)
        return MOQ_ERR_INVAL;

    size_t saved = r->pos;
    uint64_t count = 0;
    moq_result_t rc = moq_buf_read_varint(r, &count);
    if (rc < 0) return rc;

    if (count > 32) {
        r->pos = saved;
        return MOQ_ERR_PROTO;
    }
    if (count > max_parts) {
        r->pos = saved;
        return MOQ_ERR_BUFFER;
    }
    if (count > 0 && !parts) {
        r->pos = saved;
        return MOQ_ERR_INVAL;
    }

    if (count > 0) {
        rc = ns_read_fields(r, parts, count, saved);
        if (rc < 0) return rc;
    }

    out->parts = parts;
    out->count = (size_t)count;
    return MOQ_OK;
}

moq_result_t moq_buf_write_namespace_prefix(moq_buf_writer_t *w,
                                             const moq_namespace_t *ns)
{
    if (!w || !ns)
        return MOQ_ERR_INVAL;
    if (ns->count > 32)
        return MOQ_ERR_INVAL;

    if (ns->count > 0) {
        moq_result_t rc = ns_validate_fields(ns);
        if (rc < 0) return rc;
    }

    size_t saved = w->pos;
    return ns_write_fields(w, ns, saved);
}
