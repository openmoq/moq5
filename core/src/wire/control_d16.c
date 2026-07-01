#include "moq/control.h"
#include "moq/wire.h"
#include <string.h>

/* -- Control envelope ---------------------------------------------- */

moq_result_t moq_control_decode_envelope(moq_buf_reader_t *r,
                                          moq_control_envelope_t *out)
{
    if (!r || !out)
        return MOQ_ERR_INVAL;

    size_t saved = r->pos;

    moq_result_t rc = moq_buf_read_varint(r, &out->msg_type);
    if (rc < 0) return rc;

    rc = moq_buf_read_uint16(r, &out->payload_len);
    if (rc < 0) {
        r->pos = saved;
        return rc;
    }

    if (out->payload_len > moq_buf_reader_remaining(r)) {
        r->pos = saved;
        return MOQ_ERR_BUFFER;
    }

    out->payload = moq_buf_reader_ptr(r);
    r->pos += out->payload_len;
    return MOQ_OK;
}

moq_result_t moq_control_encode_envelope(moq_buf_writer_t *w,
                                          uint64_t msg_type,
                                          const uint8_t *payload,
                                          uint16_t payload_len)
{
    if (!w)
        return MOQ_ERR_INVAL;
    if (payload_len > 0 && !payload)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    moq_result_t rc = moq_buf_write_varint(w, msg_type);
    if (rc < 0) return rc;

    rc = moq_buf_write_uint16(w, payload_len);
    if (rc < 0) { w->pos = saved; return rc; }

    if (payload_len > 0) {
        rc = moq_buf_write_raw(w, payload, payload_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    return MOQ_OK;
}

moq_result_t moq_control_write_header(moq_buf_writer_t *w,
                                       uint64_t msg_type,
                                       size_t *len_offset)
{
    if (!w || !len_offset)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    moq_result_t rc = moq_buf_write_varint(w, msg_type);
    if (rc < 0) return rc;

    rc = moq_buf_reserve_uint16(w, len_offset);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- SETUP decode -------------------------------------------------- */

/* -- Internal helpers ---------------------------------------------- */

static size_t namespace_byte_len(const moq_namespace_t *ns)
{
    size_t total = 0;
    for (size_t i = 0; i < ns->count; i++)
        total += ns->parts[i].len;
    return total;
}

/*
 * Decode count + terminal KVP params from the current reader position.
 * Rejects trailing bytes after the declared count.
 * Fills out->params/params_count. Caller must set params/params_cap.
 */
static moq_result_t decode_counted_params(moq_buf_reader_t *r,
                                           moq_kvp_entry_t *params,
                                           size_t params_cap,
                                           size_t *params_count_out)
{
    uint64_t count = 0;
    moq_result_t rc = moq_buf_read_varint(r, &count);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (count > params_cap)
        return MOQ_ERR_BUFFER;
    if (count > 0 && !params)
        return MOQ_ERR_INVAL;

    moq_kvp_decoder_t dec;
    moq_kvp_decoder_init(&dec, moq_buf_reader_ptr(r),
                          moq_buf_reader_remaining(r));

    size_t decoded = 0;
    for (uint64_t i = 0; i < count; i++) {
        rc = moq_kvp_decode_next(&dec, &params[decoded]);
        if (rc == MOQ_DONE) return MOQ_ERR_PROTO;
        if (rc < 0) return MOQ_ERR_PROTO;
        decoded++;
    }

    moq_kvp_entry_t extra;
    rc = moq_kvp_decode_next(&dec, &extra);
    if (rc != MOQ_DONE) return MOQ_ERR_PROTO;

    r->pos = r->len;
    *params_count_out = decoded;
    return MOQ_OK;
}

/*
 * Encode count + KVP params at the current writer position.
 */
static moq_result_t encode_counted_params(moq_buf_writer_t *w,
                                           const moq_kvp_entry_t *params,
                                           size_t params_count,
                                           size_t saved)
{
    moq_result_t rc = moq_buf_write_varint(w, params_count);
    if (rc < 0) { w->pos = saved; return rc; }

    uint64_t prev_type = 0;
    for (size_t i = 0; i < params_count; i++) {
        size_t n = moq_kvp_encode_entry(prev_type, &params[i],
                                        moq_buf_writer_ptr(w),
                                        moq_buf_writer_remaining(w));
        if (n == 0) { w->pos = saved; return MOQ_ERR_BUFFER; }
        w->pos += n;
        prev_type = params[i].type;
    }
    return MOQ_OK;
}

/*
 * Decode a Reason Phrase: Length (i) + Value (..)
 * Max 1024 bytes. Sets *reason and *reason_len.
 */
static moq_result_t decode_reason(moq_buf_reader_t *r,
                                   const uint8_t **reason,
                                   size_t *reason_len)
{
    moq_bytes_t span;
    moq_result_t rc = moq_buf_read_span(r, &span);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (span.len > 1024)
        return MOQ_ERR_PROTO;

    *reason     = span.len > 0 ? span.data : NULL;
    *reason_len = span.len;
    return MOQ_OK;
}

/*
 * Encode a Reason Phrase: Length (i) + Value (..)
 */
static moq_result_t encode_reason(moq_buf_writer_t *w,
                                   const uint8_t *reason,
                                   size_t reason_len,
                                   size_t saved)
{
    moq_bytes_t span = { .data = reason, .len = reason_len };
    moq_result_t rc = moq_buf_write_span(w, span);
    if (rc < 0) { w->pos = saved; return rc; }
    return MOQ_OK;
}

/* -- SETUP decode -------------------------------------------------- */

/*
 * Shared decode logic for CLIENT_SETUP and SERVER_SETUP.
 * Draft-16: both have identical wire format (no version fields).
 */
static moq_result_t decode_setup(const uint8_t *payload, size_t payload_len,
                                  moq_d16_setup_t *out)
{
    if (!payload && payload_len > 0)
        return MOQ_ERR_INVAL;
    if (!out)
        return MOQ_ERR_INVAL;
    out->params_count = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    return decode_counted_params(&r, out->params, out->params_cap,
                                 &out->params_count);
}

moq_result_t moq_d16_decode_client_setup(const uint8_t *payload,
                                           size_t payload_len,
                                           moq_d16_setup_t *out)
{
    return decode_setup(payload, payload_len, out);
}

moq_result_t moq_d16_decode_server_setup(const uint8_t *payload,
                                           size_t payload_len,
                                           moq_d16_setup_t *out)
{
    return decode_setup(payload, payload_len, out);
}

/* -- SETUP encode -------------------------------------------------- */

static moq_result_t encode_setup(moq_buf_writer_t *w, uint64_t msg_type,
                                  const moq_kvp_entry_t *params,
                                  size_t params_count)
{
    if (!w)
        return MOQ_ERR_INVAL;
    if (params_count > 0 && !params)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;
    moq_result_t rc = moq_control_write_header(w, msg_type, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    size_t payload_len = w->pos - payload_start;
    if (payload_len > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)payload_len);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_client_setup(moq_buf_writer_t *w,
                                           const moq_kvp_entry_t *params,
                                           size_t params_count)
{
    return encode_setup(w, MOQ_D16_CLIENT_SETUP, params, params_count);
}

moq_result_t moq_d16_encode_server_setup(moq_buf_writer_t *w,
                                           const moq_kvp_entry_t *params,
                                           size_t params_count)
{
    return encode_setup(w, MOQ_D16_SERVER_SETUP, params, params_count);
}

/* -- Simple varint messages ---------------------------------------- */

static bool is_varint_msg_type(uint64_t msg_type)
{
    return msg_type == MOQ_D16_UNSUBSCRIBE ||
           msg_type == MOQ_D16_FETCH_CANCEL ||
           msg_type == MOQ_D16_PUBLISH_NAMESPACE_DONE ||
           msg_type == MOQ_D16_MAX_REQUEST_ID ||
           msg_type == MOQ_D16_REQUESTS_BLOCKED;
}

moq_result_t moq_d16_decode_varint_msg(const uint8_t *payload,
                                        size_t payload_len,
                                        uint64_t *out_value)
{
    if (!out_value)
        return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0)
        return MOQ_ERR_INVAL;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, out_value);
    if (rc < 0)
        return MOQ_ERR_PROTO;

    if (moq_buf_reader_remaining(&r) != 0)
        return MOQ_ERR_PROTO;

    return MOQ_OK;
}

moq_result_t moq_d16_encode_varint_msg(moq_buf_writer_t *w,
                                        uint64_t msg_type,
                                        uint64_t value)
{
    if (!w)
        return MOQ_ERR_INVAL;
    if (!is_varint_msg_type(msg_type))
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, msg_type, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, value);
    if (rc < 0) { w->pos = saved; return rc; }

    size_t plen = w->pos - payload_start;
    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- GOAWAY -------------------------------------------------------- */

moq_result_t moq_d16_decode_goaway(const uint8_t *payload,
                                    size_t payload_len,
                                    moq_d16_goaway_t *out)
{
    if (!out)
        return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0)
        return MOQ_ERR_INVAL;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_bytes_t uri;
    moq_result_t rc = moq_buf_read_span(&r, &uri);
    if (rc < 0)
        return MOQ_ERR_PROTO;

    if (uri.len > 8192)
        return MOQ_ERR_PROTO;

    if (moq_buf_reader_remaining(&r) != 0)
        return MOQ_ERR_PROTO;

    out->uri     = uri.len > 0 ? uri.data : NULL;
    out->uri_len = uri.len;
    return MOQ_OK;
}

moq_result_t moq_d16_encode_goaway(moq_buf_writer_t *w,
                                    const uint8_t *uri,
                                    size_t uri_len)
{
    if (!w)
        return MOQ_ERR_INVAL;
    if (uri_len > 0 && !uri)
        return MOQ_ERR_INVAL;
    if (uri_len > 8192)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_GOAWAY, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    moq_bytes_t span = { .data = uri, .len = uri_len };
    rc = moq_buf_write_span(w, span);
    if (rc < 0) { w->pos = saved; return rc; }

    size_t plen = w->pos - payload_start;
    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- PUBLISH_NAMESPACE_CANCEL -------------------------------------- */

moq_result_t moq_d16_decode_publish_namespace_cancel(
    const uint8_t *payload, size_t payload_len,
    moq_d16_publish_namespace_cancel_t *out)
{
    if (!out)
        return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0)
        return MOQ_ERR_INVAL;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->error_code);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = decode_reason(&r, &out->reason, &out->reason_len);
    if (rc < 0) return rc;

    if (moq_buf_reader_remaining(&r) != 0)
        return MOQ_ERR_PROTO;

    return MOQ_OK;
}

moq_result_t moq_d16_encode_publish_namespace_cancel(
    moq_buf_writer_t *w, uint64_t request_id,
    uint64_t error_code, const uint8_t *reason, size_t reason_len)
{
    if (!w)
        return MOQ_ERR_INVAL;
    if (reason_len > 0 && !reason)
        return MOQ_ERR_INVAL;
    if (reason_len > 1024)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_PUBLISH_NAMESPACE_CANCEL,
                                                &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, error_code);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_reason(w, reason, reason_len, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- REQUEST_OK ---------------------------------------------------- */

moq_result_t moq_d16_decode_request_ok(const uint8_t *payload,
                                        size_t payload_len,
                                        moq_d16_request_ok_t *out)
{
    if (!out)
        return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0)
        return MOQ_ERR_INVAL;
    out->params_count = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    return decode_counted_params(&r, out->params, out->params_cap,
                                 &out->params_count);
}

moq_result_t moq_d16_encode_request_ok(moq_buf_writer_t *w,
                                        uint64_t request_id,
                                        const moq_kvp_entry_t *params,
                                        size_t params_count)
{
    if (!w) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_REQUEST_OK, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- REQUEST_ERROR ------------------------------------------------- */

moq_result_t moq_d16_decode_request_error(const uint8_t *payload,
                                           size_t payload_len,
                                           moq_d16_request_error_t *out)
{
    if (!out)
        return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0)
        return MOQ_ERR_INVAL;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->error_code);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->retry_interval);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = decode_reason(&r, &out->reason, &out->reason_len);
    if (rc < 0) return rc;

    if (moq_buf_reader_remaining(&r) != 0)
        return MOQ_ERR_PROTO;

    return MOQ_OK;
}

moq_result_t moq_d16_encode_request_error(moq_buf_writer_t *w,
                                           uint64_t request_id,
                                           uint64_t error_code,
                                           uint64_t retry_interval,
                                           const uint8_t *reason,
                                           size_t reason_len)
{
    if (!w) return MOQ_ERR_INVAL;
    if (reason_len > 0 && !reason) return MOQ_ERR_INVAL;
    if (reason_len > 1024) return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_REQUEST_ERROR, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, error_code);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, retry_interval);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_reason(w, reason, reason_len, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- REQUEST_UPDATE ------------------------------------------------- */

moq_result_t moq_d16_decode_request_update(
    const uint8_t *payload, size_t payload_len,
    moq_d16_request_update_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = moq_buf_read_varint(&r, &out->existing_request_id);
    if (rc < 0) return rc;

    out->params_count = 0;
    rc = decode_counted_params(&r, out->params, out->params_cap,
                                &out->params_count);
    if (rc < 0) return rc;
    return MOQ_OK;
}

moq_result_t moq_d16_encode_request_update(
    moq_buf_writer_t *w,
    uint64_t request_id,
    uint64_t existing_request_id,
    const moq_kvp_entry_t *params,
    size_t params_count)
{
    if (!w) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset;

    moq_result_t rc = moq_control_write_header(w,
        MOQ_D16_REQUEST_UPDATE, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }
    rc = moq_buf_write_varint(w, existing_request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) { w->pos = saved; return rc; }

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }
    return MOQ_OK;
}

/* -- PUBLISH_NAMESPACE --------------------------------------------- */

moq_result_t moq_d16_decode_publish_namespace(const uint8_t *payload,
                                               size_t payload_len,
                                               moq_bytes_t *ns_parts,
                                               size_t ns_parts_cap,
                                               moq_d16_publish_namespace_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    out->params_count = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_namespace(&r, ns_parts, ns_parts_cap,
                                 &out->track_namespace);
    if (rc < 0) return MOQ_ERR_PROTO;

    /* Namespace-only Full Track Name limit: namespace bytes <= 4096 */
    size_t ns_bytes = namespace_byte_len(&out->track_namespace);
    if (ns_bytes > MOQ_FULL_TRACK_NAME_MAX)
        return MOQ_ERR_PROTO;

    return decode_counted_params(&r, out->params, out->params_cap,
                                  &out->params_count);
}

moq_result_t moq_d16_encode_publish_namespace(moq_buf_writer_t *w,
                                               uint64_t request_id,
                                               const moq_namespace_t *ns,
                                               const moq_kvp_entry_t *params,
                                               size_t params_count)
{
    if (!w || !ns) return MOQ_ERR_INVAL;
    if (ns->count == 0 || ns->count > 32) return MOQ_ERR_INVAL;
    if (!ns->parts) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;

    size_t ns_bytes = namespace_byte_len(ns);
    if (ns_bytes > MOQ_FULL_TRACK_NAME_MAX)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_PUBLISH_NAMESPACE,
                                                &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_namespace(w, ns);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- SUBSCRIBE ----------------------------------------------------- */

moq_result_t moq_d16_decode_subscribe(const uint8_t *payload,
                                       size_t payload_len,
                                       moq_bytes_t *ns_parts,
                                       size_t ns_parts_cap,
                                       moq_d16_subscribe_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    out->params_count = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_namespace(&r, ns_parts, ns_parts_cap,
                                 &out->track_namespace);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_span(&r, &out->track_name);
    if (rc < 0) return MOQ_ERR_PROTO;

    /* Full Track Name limit: namespace bytes + track name bytes <= 4096 */
    size_t ns_bytes = namespace_byte_len(&out->track_namespace);
    if (ns_bytes + out->track_name.len > MOQ_FULL_TRACK_NAME_MAX)
        return MOQ_ERR_PROTO;

    return decode_counted_params(&r, out->params, out->params_cap,
                                  &out->params_count);
}

moq_result_t moq_d16_encode_subscribe(moq_buf_writer_t *w,
                                       uint64_t request_id,
                                       const moq_namespace_t *ns,
                                       moq_bytes_t track_name,
                                       const moq_kvp_entry_t *params,
                                       size_t params_count)
{
    if (!w || !ns) return MOQ_ERR_INVAL;
    if (ns->count == 0 || ns->count > 32) return MOQ_ERR_INVAL;
    if (!ns->parts) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;

    size_t ns_bytes = namespace_byte_len(ns);
    if (ns_bytes > MOQ_FULL_TRACK_NAME_MAX ||
        track_name.len > MOQ_FULL_TRACK_NAME_MAX - ns_bytes)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_SUBSCRIBE, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_namespace(w, ns);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_span(w, track_name);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- SUBSCRIBE_OK -------------------------------------------------- */

moq_result_t moq_d16_decode_subscribe_ok(const uint8_t *payload,
                                          size_t payload_len,
                                          moq_d16_subscribe_ok_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    out->params_count = 0;
    out->track_extensions = NULL;
    out->track_extensions_len = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->track_alias);
    if (rc < 0) return MOQ_ERR_PROTO;

    /* Decode params without rejecting trailing bytes.
     * Read param count, decode that many KVP entries, advance reader
     * past only the consumed bytes. */
    uint64_t count = 0;
    rc = moq_buf_read_varint(&r, &count);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (count > out->params_cap)
        return MOQ_ERR_BUFFER;
    if (count > 0 && !out->params)
        return MOQ_ERR_INVAL;

    if (count > 0) {
        const uint8_t *kvp_start = moq_buf_reader_ptr(&r);
        size_t kvp_avail = moq_buf_reader_remaining(&r);

        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, kvp_start, kvp_avail);

        for (uint64_t i = 0; i < count; i++) {
            rc = moq_kvp_decode_next(&dec, &out->params[out->params_count]);
            if (rc == MOQ_DONE) return MOQ_ERR_PROTO;
            if (rc < 0) return MOQ_ERR_PROTO;
            out->params_count++;
        }

        size_t kvp_consumed = kvp_avail - dec.remaining;
        r.pos += kvp_consumed;
    }

    /* Remaining bytes are track extensions (raw pass-through). */
    size_t ext_remaining = moq_buf_reader_remaining(&r);
    if (ext_remaining > 0) {
        out->track_extensions = moq_buf_reader_ptr(&r);
        out->track_extensions_len = ext_remaining;
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_subscribe_ok(moq_buf_writer_t *w,
                                          uint64_t request_id,
                                          uint64_t track_alias,
                                          const moq_kvp_entry_t *params,
                                          size_t params_count,
                                          const uint8_t *track_extensions,
                                          size_t track_extensions_len)
{
    if (!w) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;
    if (track_extensions_len > 0 && !track_extensions) return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_SUBSCRIBE_OK, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, track_alias);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    /* Track extensions: raw bytes appended after params. */
    if (track_extensions_len > 0) {
        rc = moq_buf_write_raw(w, track_extensions, track_extensions_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- Message parameter helpers ------------------------------------- */

static moq_result_t decode_single_varint(const uint8_t *value, size_t len,
                                          uint64_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!value && len > 0) return MOQ_ERR_INVAL;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, value, len);

    moq_result_t rc = moq_buf_read_varint(&r, out);
    if (rc < 0) return MOQ_ERR_PROTO;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

moq_result_t moq_d16_decode_subscription_filter(
    const uint8_t *value, size_t value_len,
    moq_d16_subscription_filter_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!value && value_len > 0) return MOQ_ERR_INVAL;

    memset(out, 0, sizeof(*out));

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, value, value_len);

    uint64_t ft = 0;
    moq_result_t rc = moq_buf_read_varint(&r, &ft);
    if (rc < 0) return MOQ_ERR_PROTO;

    out->filter_type = (uint32_t)ft;

    switch (ft) {
    case 0x1:
    case 0x2:
        break;
    case 0x3:
        rc = moq_buf_read_varint(&r, &out->start_group);
        if (rc < 0) return MOQ_ERR_PROTO;
        rc = moq_buf_read_varint(&r, &out->start_object);
        if (rc < 0) return MOQ_ERR_PROTO;
        break;
    case 0x4:
        rc = moq_buf_read_varint(&r, &out->start_group);
        if (rc < 0) return MOQ_ERR_PROTO;
        rc = moq_buf_read_varint(&r, &out->start_object);
        if (rc < 0) return MOQ_ERR_PROTO;
        rc = moq_buf_read_varint(&r, &out->end_group);
        if (rc < 0) return MOQ_ERR_PROTO;
        if (out->end_group < out->start_group) return MOQ_ERR_PROTO;
        break;
    default:
        return MOQ_ERR_PROTO;
    }

    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

moq_result_t moq_d16_encode_subscription_filter(
    uint8_t *buf, size_t cap, size_t *out_len,
    const moq_d16_subscription_filter_t *filter)
{
    if (!buf || !out_len || !filter) return MOQ_ERR_INVAL;

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, cap);

    moq_result_t rc = moq_buf_write_varint(&w, filter->filter_type);
    if (rc < 0) return rc;

    switch (filter->filter_type) {
    case 0x1:
    case 0x2:
        break;
    case 0x3:
        rc = moq_buf_write_varint(&w, filter->start_group);
        if (rc < 0) return rc;
        rc = moq_buf_write_varint(&w, filter->start_object);
        if (rc < 0) return rc;
        break;
    case 0x4:
        if (filter->end_group < filter->start_group)
            return MOQ_ERR_INVAL;
        rc = moq_buf_write_varint(&w, filter->start_group);
        if (rc < 0) return rc;
        rc = moq_buf_write_varint(&w, filter->start_object);
        if (rc < 0) return rc;
        rc = moq_buf_write_varint(&w, filter->end_group);
        if (rc < 0) return rc;
        break;
    default:
        return MOQ_ERR_INVAL;
    }

    *out_len = moq_buf_writer_offset(&w);
    return MOQ_OK;
}

moq_result_t moq_d16_decode_location(
    const uint8_t *value, size_t value_len,
    moq_d16_location_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!value && value_len > 0) return MOQ_ERR_INVAL;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, value, value_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->group);
    if (rc < 0) return MOQ_ERR_PROTO;
    rc = moq_buf_read_varint(&r, &out->object);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

moq_result_t moq_d16_decode_param_forward(
    const uint8_t *value, size_t value_len, bool *out)
{
    if (!out) return MOQ_ERR_INVAL;
    uint64_t v = 0;
    moq_result_t rc = decode_single_varint(value, value_len, &v);
    if (rc < 0) return rc;
    if (v > 1) return MOQ_ERR_PROTO;
    *out = (v == 1);
    return MOQ_OK;
}

moq_result_t moq_d16_decode_param_subscriber_priority(
    const uint8_t *value, size_t value_len, uint8_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    uint64_t v = 0;
    moq_result_t rc = decode_single_varint(value, value_len, &v);
    if (rc < 0) return rc;
    if (v > 255) return MOQ_ERR_PROTO;
    *out = (uint8_t)v;
    return MOQ_OK;
}

moq_result_t moq_d16_decode_param_group_order(
    const uint8_t *value, size_t value_len, uint8_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    uint64_t v = 0;
    moq_result_t rc = decode_single_varint(value, value_len, &v);
    if (rc < 0) return rc;
    if (v < 1 || v > 2) return MOQ_ERR_PROTO;
    *out = (uint8_t)v;
    return MOQ_OK;
}

moq_result_t moq_d16_decode_param_expires(
    const uint8_t *value, size_t value_len, uint64_t *out)
{
    return decode_single_varint(value, value_len, out);
}

/* -- Subgroup header ----------------------------------------------- */

bool moq_d16_subgroup_type_valid(uint8_t type)
{
    if ((type & 0x10) == 0) return false;
    if ((type & 0xC0) != 0) return false;
    if ((type & 0x40) != 0) return false;

    uint8_t base = type & 0x0F;
    uint8_t id_mode = (base >> 1) & 0x03;
    if (id_mode == 3) return false;

    return true;
}

moq_result_t moq_d16_decode_subgroup_header(
    moq_buf_reader_t *r,
    moq_d16_subgroup_header_t *out)
{
    if (!r || !out) return MOQ_ERR_INVAL;

    size_t saved = r->pos;

    uint64_t type_v = 0;
    moq_result_t rc = moq_buf_read_varint(r, &type_v);
    if (rc < 0) return rc;

    if (type_v > 0xFF || !moq_d16_subgroup_type_valid((uint8_t)type_v)) {
        r->pos = saved;
        return MOQ_ERR_PROTO;
    }

    uint8_t type = (uint8_t)type_v;
    memset(out, 0, sizeof(*out));
    out->type = type;
    out->has_extensions   = (type & MOQ_SUBGROUP_BIT_EXTENSIONS) != 0;
    out->subgroup_id_mode = (type & MOQ_SUBGROUP_MASK_ID_MODE) >> 1;
    out->end_of_group     = (type & MOQ_SUBGROUP_BIT_END_OF_GROUP) != 0;
    out->default_priority = (type & MOQ_SUBGROUP_BIT_DEFAULT_PRIORITY) != 0;

    rc = moq_buf_read_varint(r, &out->track_alias);
    if (rc < 0) { r->pos = saved; return rc; }
    if (out->track_alias > MOQ_QUIC_VARINT_MAX) {
        r->pos = saved; return MOQ_ERR_PROTO;
    }

    rc = moq_buf_read_varint(r, &out->group_id);
    if (rc < 0) { r->pos = saved; return rc; }
    if (out->group_id > MOQ_QUIC_VARINT_MAX) {
        r->pos = saved; return MOQ_ERR_PROTO;
    }

    if (out->subgroup_id_mode == MOQ_SUBGROUP_ID_MODE_PRESENT) {
        rc = moq_buf_read_varint(r, &out->subgroup_id);
        if (rc < 0) { r->pos = saved; return rc; }
        if (out->subgroup_id > MOQ_QUIC_VARINT_MAX) {
            r->pos = saved; return MOQ_ERR_PROTO;
        }
    }

    if (!out->default_priority) {
        if (moq_buf_reader_remaining(r) < 1) { r->pos = saved; return MOQ_ERR_BUFFER; }
        out->publisher_priority = *moq_buf_reader_ptr(r);
        r->pos += 1;
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_subgroup_header(
    moq_buf_writer_t *w,
    const moq_d16_subgroup_header_t *hdr)
{
    if (!w || !hdr) return MOQ_ERR_INVAL;
    if (!moq_d16_subgroup_type_valid(hdr->type)) return MOQ_ERR_INVAL;
    if (hdr->track_alias > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    if (hdr->group_id > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    /* Validate struct fields match type bits. */
    uint8_t type_id_mode = (hdr->type & MOQ_SUBGROUP_MASK_ID_MODE) >> 1;
    if (hdr->subgroup_id_mode != type_id_mode) return MOQ_ERR_INVAL;
    if (hdr->has_extensions != ((hdr->type & MOQ_SUBGROUP_BIT_EXTENSIONS) != 0))
        return MOQ_ERR_INVAL;
    if (hdr->end_of_group != ((hdr->type & MOQ_SUBGROUP_BIT_END_OF_GROUP) != 0))
        return MOQ_ERR_INVAL;
    if (hdr->default_priority != ((hdr->type & MOQ_SUBGROUP_BIT_DEFAULT_PRIORITY) != 0))
        return MOQ_ERR_INVAL;

    if (hdr->subgroup_id_mode == MOQ_SUBGROUP_ID_MODE_PRESENT &&
        hdr->subgroup_id > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    moq_result_t rc = moq_buf_write_varint(w, hdr->type);
    if (rc < 0) return rc;

    rc = moq_buf_write_varint(w, hdr->track_alias);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, hdr->group_id);
    if (rc < 0) { w->pos = saved; return rc; }

    if (hdr->subgroup_id_mode == MOQ_SUBGROUP_ID_MODE_PRESENT) {
        rc = moq_buf_write_varint(w, hdr->subgroup_id);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    if (!hdr->default_priority) {
        rc = moq_buf_write_raw(w, &hdr->publisher_priority, 1);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    return MOQ_OK;
}

/* -- Subgroup object fields ---------------------------------------- */

moq_result_t moq_d16_decode_object_fields(
    moq_buf_reader_t *r,
    bool has_extensions,
    moq_d16_object_fields_t *out)
{
    if (!r || !out) return MOQ_ERR_INVAL;

    size_t saved = r->pos;
    memset(out, 0, sizeof(*out));

    moq_result_t rc = moq_buf_read_varint(r, &out->object_id_delta);
    if (rc < 0) { r->pos = saved; return rc; }

    if (has_extensions) {
        uint64_t ext_len = 0;
        rc = moq_buf_read_varint(r, &ext_len);
        if (rc < 0) { r->pos = saved; return rc; }
        if (ext_len > moq_buf_reader_remaining(r)) {
            r->pos = saved; return MOQ_ERR_BUFFER;
        }
        out->extensions = moq_buf_reader_ptr(r);
        out->extensions_len = (size_t)ext_len;
        r->pos += (size_t)ext_len;
    }

    rc = moq_buf_read_varint(r, &out->payload_len);
    if (rc < 0) { r->pos = saved; return rc; }

    if (out->payload_len == 0) {
        out->has_status = true;
        uint64_t status = 0;
        rc = moq_buf_read_varint(r, &status);
        if (rc < 0) { r->pos = saved; return rc; }
        if (status != MOQ_OBJECT_STATUS_NORMAL &&
            status != MOQ_OBJECT_STATUS_END_OF_GROUP &&
            status != MOQ_OBJECT_STATUS_END_OF_TRACK) {
            r->pos = saved;
            return MOQ_ERR_PROTO;
        }
        out->status = (uint8_t)status;
        /* §10.2.1.2: extension headers on non-Normal objects are
         * a PROTOCOL_VIOLATION. */
        if (out->extensions_len > 0 &&
            status != MOQ_OBJECT_STATUS_NORMAL) {
            r->pos = saved;
            return MOQ_ERR_PROTO;
        }
        out->payload = NULL;
    } else {
        out->has_status = false;
        if (out->payload_len > moq_buf_reader_remaining(r)) {
            r->pos = saved;
            return MOQ_ERR_BUFFER;
        }
        out->payload = moq_buf_reader_ptr(r);
        r->pos += (size_t)out->payload_len;
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_object_header(
    moq_buf_writer_t *w,
    uint64_t object_id_delta,
    uint64_t payload_len)
{
    if (!w) return MOQ_ERR_INVAL;
    if (object_id_delta > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    if (payload_len > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    moq_result_t rc = moq_buf_write_varint(w, object_id_delta);
    if (rc < 0) return rc;

    rc = moq_buf_write_varint(w, payload_len);
    if (rc < 0) { w->pos = saved; return rc; }

    if (payload_len == 0) {
        rc = moq_buf_write_varint(w, MOQ_OBJECT_STATUS_NORMAL);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_object_fields(
    moq_buf_writer_t *w,
    uint64_t object_id_delta,
    uint64_t payload_len,
    const uint8_t *payload)
{
    if (!w) return MOQ_ERR_INVAL;
    if (payload_len > 0 && !payload) return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    moq_result_t rc = moq_d16_encode_object_header(w, object_id_delta,
                                                     payload_len);
    if (rc < 0) return rc;

    if (payload_len > 0) {
        rc = moq_buf_write_raw(w, payload, (size_t)payload_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    return MOQ_OK;
}

/* -- Auth token structure ------------------------------------------ */

moq_result_t moq_d16_auth_token_decode(const uint8_t *value,
                                        size_t value_len,
                                        moq_d16_auth_token_t *out)
{
    if (!value || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, value, value_len);

    uint64_t alias_type;
    if (moq_buf_read_varint(&r, &alias_type) < 0) return MOQ_ERR_PROTO;
    if (alias_type > MOQ_AUTH_TOKEN_USE_VALUE) return MOQ_ERR_PROTO;
    out->alias_type = (uint32_t)alias_type;

    switch (alias_type) {
    case MOQ_AUTH_TOKEN_DELETE:
        if (moq_buf_read_varint(&r, &out->alias) < 0) return MOQ_ERR_PROTO;
        break;
    case MOQ_AUTH_TOKEN_REGISTER:
        if (moq_buf_read_varint(&r, &out->alias) < 0) return MOQ_ERR_PROTO;
        if (moq_buf_read_varint(&r, &out->token_type) < 0) return MOQ_ERR_PROTO;
        out->token_value = moq_buf_reader_ptr(&r);
        out->token_value_len = moq_buf_reader_remaining(&r);
        r.pos = r.len;
        break;
    case MOQ_AUTH_TOKEN_USE_ALIAS:
        if (moq_buf_read_varint(&r, &out->alias) < 0) return MOQ_ERR_PROTO;
        break;
    case MOQ_AUTH_TOKEN_USE_VALUE:
        if (moq_buf_read_varint(&r, &out->token_type) < 0) return MOQ_ERR_PROTO;
        out->token_value = moq_buf_reader_ptr(&r);
        out->token_value_len = moq_buf_reader_remaining(&r);
        r.pos = r.len;
        break;
    }

    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

moq_result_t moq_d16_auth_token_encode(moq_buf_writer_t *w,
                                        const moq_d16_auth_token_t *tok)
{
    if (!w || !tok) return MOQ_ERR_INVAL;
    if (tok->alias_type > MOQ_AUTH_TOKEN_USE_VALUE) return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    if (moq_buf_write_varint(w, tok->alias_type) < 0) return MOQ_ERR_BUFFER;

    switch (tok->alias_type) {
    case MOQ_AUTH_TOKEN_DELETE:
        if (moq_buf_write_varint(w, tok->alias) < 0) { w->pos = saved; return MOQ_ERR_BUFFER; }
        break;
    case MOQ_AUTH_TOKEN_REGISTER:
        if (moq_buf_write_varint(w, tok->alias) < 0) { w->pos = saved; return MOQ_ERR_BUFFER; }
        if (moq_buf_write_varint(w, tok->token_type) < 0) { w->pos = saved; return MOQ_ERR_BUFFER; }
        if (tok->token_value_len > 0) {
            if (!tok->token_value) { w->pos = saved; return MOQ_ERR_INVAL; }
            if (moq_buf_write_raw(w, tok->token_value, tok->token_value_len) < 0)
                { w->pos = saved; return MOQ_ERR_BUFFER; }
        }
        break;
    case MOQ_AUTH_TOKEN_USE_ALIAS:
        if (moq_buf_write_varint(w, tok->alias) < 0) { w->pos = saved; return MOQ_ERR_BUFFER; }
        break;
    case MOQ_AUTH_TOKEN_USE_VALUE:
        if (moq_buf_write_varint(w, tok->token_type) < 0) { w->pos = saved; return MOQ_ERR_BUFFER; }
        if (tok->token_value_len > 0) {
            if (!tok->token_value) { w->pos = saved; return MOQ_ERR_INVAL; }
            if (moq_buf_write_raw(w, tok->token_value, tok->token_value_len) < 0)
                { w->pos = saved; return MOQ_ERR_BUFFER; }
        }
        break;
    }

    return MOQ_OK;
}

/* -- FETCH (0x16) ------------------------------------------------- */

moq_result_t moq_d16_decode_fetch(const uint8_t *payload,
                                    size_t payload_len,
                                    moq_bytes_t *ns_parts,
                                    size_t ns_parts_cap,
                                    moq_d16_fetch_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    out->params_count = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    uint64_t ft = 0;
    rc = moq_buf_read_varint(&r, &ft);
    if (rc < 0) return MOQ_ERR_PROTO;
    if (ft < 1 || ft > 3) return MOQ_ERR_PROTO;
    out->fetch_type = (uint32_t)ft;

    if (ft == MOQ_D16_FETCH_TYPE_STANDALONE) {
        rc = moq_buf_read_namespace(&r, ns_parts, ns_parts_cap,
                                     &out->track_namespace);
        if (rc < 0) return MOQ_ERR_PROTO;

        rc = moq_buf_read_span(&r, &out->track_name);
        if (rc < 0) return MOQ_ERR_PROTO;

        size_t ns_bytes = namespace_byte_len(&out->track_namespace);
        if (ns_bytes + out->track_name.len > MOQ_FULL_TRACK_NAME_MAX)
            return MOQ_ERR_PROTO;

        /* Start Location { Group (i), Object (i) } */
        rc = moq_buf_read_varint(&r, &out->start_group);
        if (rc < 0) return MOQ_ERR_PROTO;
        rc = moq_buf_read_varint(&r, &out->start_object);
        if (rc < 0) return MOQ_ERR_PROTO;

        /* End Location { Group (i), Object (i) } */
        rc = moq_buf_read_varint(&r, &out->end_group);
        if (rc < 0) return MOQ_ERR_PROTO;
        rc = moq_buf_read_varint(&r, &out->end_object);
        if (rc < 0) return MOQ_ERR_PROTO;
    } else {
        /* Joining (type 2 or 3) */
        rc = moq_buf_read_varint(&r, &out->joining_request_id);
        if (rc < 0) return MOQ_ERR_PROTO;
        rc = moq_buf_read_varint(&r, &out->joining_start);
        if (rc < 0) return MOQ_ERR_PROTO;
    }

    return decode_counted_params(&r, out->params, out->params_cap,
                                  &out->params_count);
}

moq_result_t moq_d16_encode_fetch(moq_buf_writer_t *w,
                                    const moq_d16_fetch_t *fetch,
                                    const moq_kvp_entry_t *params,
                                    size_t params_count)
{
    if (!w || !fetch) return MOQ_ERR_INVAL;
    if (fetch->fetch_type < 1 || fetch->fetch_type > 3) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;

    if (fetch->fetch_type == MOQ_D16_FETCH_TYPE_STANDALONE) {
        const moq_namespace_t *ns = &fetch->track_namespace;
        if (ns->count == 0 || ns->count > 32) return MOQ_ERR_INVAL;
        if (!ns->parts) return MOQ_ERR_INVAL;
        size_t ns_bytes = namespace_byte_len(ns);
        if (fetch->track_name.len > MOQ_FULL_TRACK_NAME_MAX - ns_bytes)
            return MOQ_ERR_INVAL;
    }

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_FETCH, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, fetch->request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, fetch->fetch_type);
    if (rc < 0) { w->pos = saved; return rc; }

    if (fetch->fetch_type == MOQ_D16_FETCH_TYPE_STANDALONE) {
        rc = moq_buf_write_namespace(w, &fetch->track_namespace);
        if (rc < 0) { w->pos = saved; return rc; }

        rc = moq_buf_write_span(w, fetch->track_name);
        if (rc < 0) { w->pos = saved; return rc; }

        rc = moq_buf_write_varint(w, fetch->start_group);
        if (rc < 0) { w->pos = saved; return rc; }
        rc = moq_buf_write_varint(w, fetch->start_object);
        if (rc < 0) { w->pos = saved; return rc; }
        rc = moq_buf_write_varint(w, fetch->end_group);
        if (rc < 0) { w->pos = saved; return rc; }
        rc = moq_buf_write_varint(w, fetch->end_object);
        if (rc < 0) { w->pos = saved; return rc; }
    } else {
        rc = moq_buf_write_varint(w, fetch->joining_request_id);
        if (rc < 0) { w->pos = saved; return rc; }
        rc = moq_buf_write_varint(w, fetch->joining_start);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- FETCH_OK (0x18) ---------------------------------------------- */

moq_result_t moq_d16_decode_fetch_ok(const uint8_t *payload,
                                       size_t payload_len,
                                       moq_d16_fetch_ok_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;

    out->params_count = 0;
    out->track_extensions = NULL;
    out->track_extensions_len = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    /* End Of Track: uint8 */
    if (moq_buf_reader_remaining(&r) < 1) return MOQ_ERR_PROTO;
    out->end_of_track = *moq_buf_reader_ptr(&r);
    r.pos += 1;
    if (out->end_of_track > 1) return MOQ_ERR_PROTO;

    /* End Location { Group (i), Object (i) } */
    rc = moq_buf_read_varint(&r, &out->end_group);
    if (rc < 0) return MOQ_ERR_PROTO;
    rc = moq_buf_read_varint(&r, &out->end_object);
    if (rc < 0) return MOQ_ERR_PROTO;

    /* Params without rejecting trailing bytes (track extensions follow). */
    uint64_t count = 0;
    rc = moq_buf_read_varint(&r, &count);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (count > out->params_cap)
        return MOQ_ERR_BUFFER;
    if (count > 0 && !out->params)
        return MOQ_ERR_INVAL;

    if (count > 0) {
        const uint8_t *kvp_start = moq_buf_reader_ptr(&r);
        size_t kvp_avail = moq_buf_reader_remaining(&r);

        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, kvp_start, kvp_avail);

        for (uint64_t i = 0; i < count; i++) {
            rc = moq_kvp_decode_next(&dec, &out->params[out->params_count]);
            if (rc == MOQ_DONE) return MOQ_ERR_PROTO;
            if (rc < 0) return MOQ_ERR_PROTO;
            out->params_count++;
        }

        size_t kvp_consumed = kvp_avail - dec.remaining;
        r.pos += kvp_consumed;
    }

    /* Remaining bytes are track extensions (raw pass-through). */
    size_t ext_remaining = moq_buf_reader_remaining(&r);
    if (ext_remaining > 0) {
        out->track_extensions = moq_buf_reader_ptr(&r);
        out->track_extensions_len = ext_remaining;
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_fetch_ok(moq_buf_writer_t *w,
                                       const moq_d16_fetch_ok_t *ok)
{
    if (!w || !ok) return MOQ_ERR_INVAL;
    if (ok->params_count > 0 && !ok->params) return MOQ_ERR_INVAL;
    if (ok->track_extensions_len > 0 && !ok->track_extensions) return MOQ_ERR_INVAL;
    if (ok->end_of_track > 1) return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_FETCH_OK, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, ok->request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    /* End Of Track: uint8 */
    rc = moq_buf_write_raw(w, &ok->end_of_track, 1);
    if (rc < 0) { w->pos = saved; return rc; }

    /* End Location */
    rc = moq_buf_write_varint(w, ok->end_group);
    if (rc < 0) { w->pos = saved; return rc; }
    rc = moq_buf_write_varint(w, ok->end_object);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, ok->params, ok->params_count, saved);
    if (rc < 0) return rc;

    /* Track extensions: raw bytes appended after params. */
    if (ok->track_extensions_len > 0) {
        rc = moq_buf_write_raw(w, ok->track_extensions, ok->track_extensions_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- FETCH_HEADER (raw varints on uni stream) --------------------- */

moq_result_t moq_d16_decode_fetch_header(moq_buf_reader_t *r,
                                           moq_d16_fetch_header_t *out)
{
    if (!r || !out) return MOQ_ERR_INVAL;

    size_t saved = r->pos;

    uint64_t stream_type = 0;
    moq_result_t rc = moq_buf_read_varint(r, &stream_type);
    if (rc < 0) { r->pos = saved; return rc; }

    if (stream_type != MOQ_D16_FETCH_HEADER_TYPE) {
        r->pos = saved;
        return MOQ_ERR_PROTO;
    }

    rc = moq_buf_read_varint(r, &out->request_id);
    if (rc < 0) { r->pos = saved; return rc; }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_fetch_header(moq_buf_writer_t *w,
                                           uint64_t request_id)
{
    if (!w) return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    moq_result_t rc = moq_buf_write_varint(w, MOQ_D16_FETCH_HEADER_TYPE);
    if (rc < 0) return rc;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- Fetch Object Fields ------------------------------------------ */

moq_result_t moq_d16_decode_fetch_object(moq_buf_reader_t *r,
                                           const moq_d16_fetch_object_t *prev,
                                           moq_d16_fetch_object_t *out)
{
    if (!r || !out) return MOQ_ERR_INVAL;

    size_t saved = r->pos;
    memset(out, 0, sizeof(*out));

    uint64_t flags = 0;
    moq_result_t rc = moq_buf_read_varint(r, &flags);
    if (rc < 0) { r->pos = saved; return rc; }

    /* End-of-range markers: group/object from wire, subgroup/priority from prev */
    if (flags == MOQ_D16_FETCH_END_NON_EXISTENT || flags == MOQ_D16_FETCH_END_UNKNOWN) {
        out->is_end_of_range = true;
        out->range_kind = (uint32_t)flags;

        rc = moq_buf_read_varint(r, &out->group_id);
        if (rc < 0) { r->pos = saved; return rc; }
        rc = moq_buf_read_varint(r, &out->object_id);
        if (rc < 0) { r->pos = saved; return rc; }

        if (prev) {
            out->subgroup_id = prev->subgroup_id;
            out->publisher_priority = prev->publisher_priority;
        }

        return MOQ_OK;
    }

    /* Any other value >= 128 is PROTOCOL_VIOLATION */
    if (flags >= 128) {
        r->pos = saved;
        return MOQ_ERR_PROTO;
    }

    /* Normal object: parse flag bits */
    bool has_group_id  = (flags & MOQ_D16_FETCH_FLAG_GROUP_ID)  != 0;
    bool has_object_id = (flags & MOQ_D16_FETCH_FLAG_OBJECT_ID) != 0;
    bool has_priority  = (flags & MOQ_D16_FETCH_FLAG_PRIORITY)  != 0;
    bool has_ext       = (flags & MOQ_D16_FETCH_FLAG_EXTENSIONS) != 0;
    uint8_t subgroup_mode = (uint8_t)(flags & MOQ_D16_FETCH_FLAG_SUBGROUP_MASK);

    /* First object in stream must have group_id, object_id, priority */
    if (!prev) {
        if (!has_group_id || !has_object_id || !has_priority) {
            r->pos = saved;
            return MOQ_ERR_PROTO;
        }
    }

    /* Group ID */
    if (has_group_id) {
        rc = moq_buf_read_varint(r, &out->group_id);
        if (rc < 0) { r->pos = saved; return rc; }
    } else {
        if (!prev) { r->pos = saved; return MOQ_ERR_PROTO; }
        out->group_id = prev->group_id;
    }

    /* Subgroup ID */
    if ((flags & MOQ_D16_FETCH_FLAG_DATAGRAM) != 0) {
        /* datagram: ignore subgroup bits, subgroup_id = 0 */
        out->subgroup_id = 0;
    } else {
        switch (subgroup_mode) {
        case 0: /* zero */
            out->subgroup_id = 0;
            break;
        case 1: /* prior */
            if (!prev) { r->pos = saved; return MOQ_ERR_PROTO; }
            out->subgroup_id = prev->subgroup_id;
            break;
        case 2: /* prior + 1 */
            if (!prev) { r->pos = saved; return MOQ_ERR_PROTO; }
            out->subgroup_id = prev->subgroup_id + 1;
            break;
        case 3: /* present */
            rc = moq_buf_read_varint(r, &out->subgroup_id);
            if (rc < 0) { r->pos = saved; return rc; }
            break;
        }
    }

    /* Object ID */
    if (has_object_id) {
        rc = moq_buf_read_varint(r, &out->object_id);
        if (rc < 0) { r->pos = saved; return rc; }
    } else {
        if (!prev) { r->pos = saved; return MOQ_ERR_PROTO; }
        out->object_id = prev->object_id + 1;
    }

    /* Publisher Priority */
    if (has_priority) {
        if (moq_buf_reader_remaining(r) < 1) { r->pos = saved; return MOQ_ERR_BUFFER; }
        out->publisher_priority = *moq_buf_reader_ptr(r);
        r->pos += 1;
    } else {
        if (!prev) { r->pos = saved; return MOQ_ERR_PROTO; }
        out->publisher_priority = prev->publisher_priority;
    }

    /* Extensions */
    if (has_ext) {
        uint64_t ext_len = 0;
        rc = moq_buf_read_varint(r, &ext_len);
        if (rc < 0) { r->pos = saved; return rc; }
        if (ext_len > moq_buf_reader_remaining(r)) {
            r->pos = saved;
            return MOQ_ERR_BUFFER;
        }
        out->extensions = moq_buf_reader_ptr(r);
        out->extensions_len = (size_t)ext_len;
        r->pos += (size_t)ext_len;
    }

    /* Payload Length + Payload */
    rc = moq_buf_read_varint(r, &out->payload_len);
    if (rc < 0) { r->pos = saved; return rc; }

    if (out->payload_len > 0) {
        if (out->payload_len > moq_buf_reader_remaining(r)) {
            r->pos = saved;
            return MOQ_ERR_BUFFER;
        }
        out->payload = moq_buf_reader_ptr(r);
        r->pos += (size_t)out->payload_len;
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_fetch_object(moq_buf_writer_t *w,
                                           const moq_d16_fetch_object_t *obj,
                                           const moq_d16_fetch_object_t *prev)
{
    if (!w || !obj) return MOQ_ERR_INVAL;

    /* End-of-range markers are encoded via moq_d16_encode_fetch_end_of_range */
    if (obj->is_end_of_range)
        return moq_d16_encode_fetch_end_of_range(w, obj->range_kind,
                                                   obj->group_id, obj->object_id);

    if (obj->payload_len > 0 && !obj->payload) return MOQ_ERR_INVAL;
    if (obj->extensions_len > 0 && !obj->extensions) return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    /* Compute optimal flags */
    uint64_t flags = 0;

    if (!prev) {
        /* First object: must encode group_id, object_id, priority */
        flags |= MOQ_D16_FETCH_FLAG_GROUP_ID;
        flags |= MOQ_D16_FETCH_FLAG_OBJECT_ID;
        flags |= MOQ_D16_FETCH_FLAG_PRIORITY;
        /* Subgroup: encode explicitly if non-zero, else use mode 0 (zero) */
        if (obj->subgroup_id != 0) {
            flags |= 0x03; /* present */
        }
    } else {
        /* Group ID: set if different from prior */
        if (obj->group_id != prev->group_id) {
            flags |= MOQ_D16_FETCH_FLAG_GROUP_ID;
        }

        /* Object ID: set if not prev+1 */
        if (obj->object_id != prev->object_id + 1) {
            flags |= MOQ_D16_FETCH_FLAG_OBJECT_ID;
        }

        /* Priority: set if different from prior */
        if (obj->publisher_priority != prev->publisher_priority) {
            flags |= MOQ_D16_FETCH_FLAG_PRIORITY;
        }

        /* Subgroup: compute minimal encoding */
        if (obj->subgroup_id == 0) {
            /* mode 0: zero — no bits set */
        } else if (obj->subgroup_id == prev->subgroup_id) {
            flags |= 0x01; /* prior */
        } else if (obj->subgroup_id == prev->subgroup_id + 1) {
            flags |= 0x02; /* prior + 1 */
        } else {
            flags |= 0x03; /* present */
        }
    }

    if (obj->extensions_len > 0) {
        flags |= MOQ_D16_FETCH_FLAG_EXTENSIONS;
    }

    moq_result_t rc = moq_buf_write_varint(w, flags);
    if (rc < 0) return rc;

    /* Group ID */
    if (flags & MOQ_D16_FETCH_FLAG_GROUP_ID) {
        rc = moq_buf_write_varint(w, obj->group_id);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    /* Subgroup ID (only if mode == present == 0x03) */
    if ((flags & MOQ_D16_FETCH_FLAG_SUBGROUP_MASK) == 0x03) {
        rc = moq_buf_write_varint(w, obj->subgroup_id);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    /* Object ID */
    if (flags & MOQ_D16_FETCH_FLAG_OBJECT_ID) {
        rc = moq_buf_write_varint(w, obj->object_id);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    /* Priority */
    if (flags & MOQ_D16_FETCH_FLAG_PRIORITY) {
        rc = moq_buf_write_raw(w, &obj->publisher_priority, 1);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    /* Extensions */
    if (flags & MOQ_D16_FETCH_FLAG_EXTENSIONS) {
        rc = moq_buf_write_varint(w, (uint64_t)obj->extensions_len);
        if (rc < 0) { w->pos = saved; return rc; }
        if (obj->extensions_len > 0) {
            rc = moq_buf_write_raw(w, obj->extensions, obj->extensions_len);
            if (rc < 0) { w->pos = saved; return rc; }
        }
    }

    /* Payload Length + Payload */
    rc = moq_buf_write_varint(w, obj->payload_len);
    if (rc < 0) { w->pos = saved; return rc; }

    if (obj->payload_len > 0) {
        rc = moq_buf_write_raw(w, obj->payload, (size_t)obj->payload_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_fetch_end_of_range(moq_buf_writer_t *w,
                                                 uint32_t range_kind,
                                                 uint64_t group_id,
                                                 uint64_t object_id)
{
    if (!w) return MOQ_ERR_INVAL;
    if (range_kind != MOQ_D16_FETCH_END_NON_EXISTENT &&
        range_kind != MOQ_D16_FETCH_END_UNKNOWN)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    moq_result_t rc = moq_buf_write_varint(w, range_kind);
    if (rc < 0) return rc;

    rc = moq_buf_write_varint(w, group_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, object_id);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- PUBLISH (0x1D) ----------------------------------------------- */

moq_result_t moq_d16_decode_publish(const uint8_t *payload, size_t payload_len,
                                     moq_bytes_t *ns_parts, size_t ns_parts_cap,
                                     moq_d16_publish_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    out->params_count = 0;
    out->track_extensions = NULL;
    out->track_extensions_len = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_namespace(&r, ns_parts, ns_parts_cap,
                                 &out->track_namespace);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_span(&r, &out->track_name);
    if (rc < 0) return MOQ_ERR_PROTO;

    /* Full Track Name limit: namespace bytes + track name bytes <= 4096 */
    size_t ns_bytes = namespace_byte_len(&out->track_namespace);
    if (ns_bytes + out->track_name.len > MOQ_FULL_TRACK_NAME_MAX)
        return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->track_alias);
    if (rc < 0) return MOQ_ERR_PROTO;

    /* Decode params without rejecting trailing bytes (track extensions follow). */
    uint64_t count = 0;
    rc = moq_buf_read_varint(&r, &count);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (count > out->params_cap)
        return MOQ_ERR_BUFFER;
    if (count > 0 && !out->params)
        return MOQ_ERR_INVAL;

    if (count > 0) {
        const uint8_t *kvp_start = moq_buf_reader_ptr(&r);
        size_t kvp_avail = moq_buf_reader_remaining(&r);

        moq_kvp_decoder_t dec;
        moq_kvp_decoder_init(&dec, kvp_start, kvp_avail);

        for (uint64_t i = 0; i < count; i++) {
            rc = moq_kvp_decode_next(&dec, &out->params[out->params_count]);
            if (rc == MOQ_DONE) return MOQ_ERR_PROTO;
            if (rc < 0) return MOQ_ERR_PROTO;
            out->params_count++;
        }

        size_t kvp_consumed = kvp_avail - dec.remaining;
        r.pos += kvp_consumed;
    }

    /* Remaining bytes are track extensions (raw pass-through). */
    size_t ext_remaining = moq_buf_reader_remaining(&r);
    if (ext_remaining > 0) {
        out->track_extensions = moq_buf_reader_ptr(&r);
        out->track_extensions_len = ext_remaining;
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_publish(moq_buf_writer_t *w,
                                     const moq_d16_publish_t *pub)
{
    if (!w || !pub) return MOQ_ERR_INVAL;
    if (!pub->track_namespace.parts) return MOQ_ERR_INVAL;
    if (pub->track_namespace.count == 0 || pub->track_namespace.count > 32)
        return MOQ_ERR_INVAL;
    if (pub->params_count > 0 && !pub->params) return MOQ_ERR_INVAL;
    if (pub->track_extensions_len > 0 && !pub->track_extensions) return MOQ_ERR_INVAL;

    size_t ns_bytes = namespace_byte_len(&pub->track_namespace);
    if (ns_bytes > MOQ_FULL_TRACK_NAME_MAX ||
        pub->track_name.len > MOQ_FULL_TRACK_NAME_MAX - ns_bytes)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_PUBLISH, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, pub->request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_namespace(w, &pub->track_namespace);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_span(w, pub->track_name);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, pub->track_alias);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, pub->params, pub->params_count, saved);
    if (rc < 0) return rc;

    /* Track extensions: raw bytes appended after params. */
    if (pub->track_extensions_len > 0) {
        rc = moq_buf_write_raw(w, pub->track_extensions, pub->track_extensions_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- PUBLISH_OK (0x1E) -------------------------------------------- */

moq_result_t moq_d16_decode_publish_ok(const uint8_t *payload, size_t payload_len,
                                        moq_d16_publish_ok_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    out->params_count = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    return decode_counted_params(&r, out->params, out->params_cap,
                                  &out->params_count);
}

moq_result_t moq_d16_encode_publish_ok(moq_buf_writer_t *w,
                                        uint64_t request_id,
                                        const moq_kvp_entry_t *params,
                                        size_t params_count)
{
    if (!w) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_PUBLISH_OK, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- PUBLISH_DONE (0x0B) ------------------------------------------ */

moq_result_t moq_d16_decode_publish_done(const uint8_t *payload, size_t payload_len,
                                          moq_d16_publish_done_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->status_code);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->stream_count);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = decode_reason(&r, &out->reason, &out->reason_len);
    if (rc < 0) return rc;

    if (moq_buf_reader_remaining(&r) != 0)
        return MOQ_ERR_PROTO;

    return MOQ_OK;
}

moq_result_t moq_d16_encode_publish_done(moq_buf_writer_t *w,
                                          uint64_t request_id,
                                          uint64_t status_code,
                                          uint64_t stream_count,
                                          const uint8_t *reason,
                                          size_t reason_len)
{
    if (!w) return MOQ_ERR_INVAL;
    if (reason_len > 0 && !reason) return MOQ_ERR_INVAL;
    if (reason_len > 1024) return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_PUBLISH_DONE, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, status_code);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, stream_count);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_reason(w, reason, reason_len, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- SUBSCRIBE_NAMESPACE (0x11) ----------------------------------- */

moq_result_t moq_d16_decode_subscribe_namespace(
    const uint8_t *payload, size_t payload_len,
    moq_bytes_t *ns_parts, size_t ns_parts_cap,
    moq_d16_subscribe_namespace_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    out->params_count = 0;

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_varint(&r, &out->request_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_namespace_prefix(&r, ns_parts, ns_parts_cap,
                                       &out->track_namespace_prefix);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (out->track_namespace_prefix.count > 32)
        return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->subscribe_options);
    if (rc < 0) return MOQ_ERR_PROTO;
    if (out->subscribe_options > 2)
        return MOQ_ERR_PROTO;

    return decode_counted_params(&r, out->params, out->params_cap,
                                  &out->params_count);
}

moq_result_t moq_d16_encode_subscribe_namespace(
    moq_buf_writer_t *w,
    uint64_t request_id,
    const moq_namespace_t *prefix,
    uint64_t subscribe_options,
    const moq_kvp_entry_t *params,
    size_t params_count)
{
    if (!w || !prefix) return MOQ_ERR_INVAL;
    if (prefix->count > 32) return MOQ_ERR_INVAL;
    if (!prefix->parts && prefix->count > 0) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;
    if (subscribe_options > 2) return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_SUBSCRIBE_NAMESPACE,
                                                &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_namespace_prefix(w, prefix);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, subscribe_options);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- TRACK_STATUS (0x0D) ------------------------------------------- */

moq_result_t moq_d16_decode_track_status(const uint8_t *payload,
                                          size_t payload_len,
                                          moq_bytes_t *ns_parts,
                                          size_t ns_parts_cap,
                                          moq_d16_track_status_t *out)
{
    return moq_d16_decode_subscribe(payload, payload_len,
                                    ns_parts, ns_parts_cap, out);
}

moq_result_t moq_d16_encode_track_status(moq_buf_writer_t *w,
                                          uint64_t request_id,
                                          const moq_namespace_t *ns,
                                          moq_bytes_t track_name,
                                          const moq_kvp_entry_t *params,
                                          size_t params_count)
{
    if (!w || !ns) return MOQ_ERR_INVAL;
    if (ns->count == 0 || ns->count > 32) return MOQ_ERR_INVAL;
    if (!ns->parts) return MOQ_ERR_INVAL;
    if (params_count > 0 && !params) return MOQ_ERR_INVAL;

    size_t ns_bytes = namespace_byte_len(ns);
    if (ns_bytes > MOQ_FULL_TRACK_NAME_MAX ||
        track_name.len > MOQ_FULL_TRACK_NAME_MAX - ns_bytes)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos;
    size_t len_offset = 0;

    moq_result_t rc = moq_control_write_header(w, MOQ_D16_TRACK_STATUS, &len_offset);
    if (rc < 0) return rc;

    size_t payload_start = w->pos;

    rc = moq_buf_write_varint(w, request_id);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_namespace(w, ns);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_span(w, track_name);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = encode_counted_params(w, params, params_count, saved);
    if (rc < 0) return rc;

    size_t plen = w->pos - payload_start;
    if (plen > 0xFFFF) { w->pos = saved; return MOQ_ERR_BUFFER; }

    rc = moq_buf_patch_uint16(w, len_offset, (uint16_t)plen);
    if (rc < 0) { w->pos = saved; return rc; }

    return MOQ_OK;
}

/* -- OBJECT_DATAGRAM (§10.3.1) ------------------------------------- */

static bool datagram_type_valid(uint64_t type)
{
    if (type > 0x3F) return false;
    if (type & 0x10u) return false;
    if ((type & MOQ_D16_DGRAM_FLAG_STATUS) &&
        (type & MOQ_D16_DGRAM_FLAG_END_OF_GROUP))
        return false;
    return true;
}

moq_result_t moq_d16_decode_object_datagram(
    const uint8_t *data, size_t len,
    moq_d16_object_datagram_t *out)
{
    if (!data || !out) return MOQ_ERR_INVAL;

    memset(out, 0, sizeof(*out));

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);

    uint64_t type = 0;
    moq_result_t rc = moq_buf_read_varint(&r, &type);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (!datagram_type_valid(type))
        return MOQ_ERR_PROTO;

    out->has_extensions   = (type & MOQ_D16_DGRAM_FLAG_EXTENSIONS) != 0;
    out->end_of_group     = (type & MOQ_D16_DGRAM_FLAG_END_OF_GROUP) != 0;
    bool zero_object_id   = (type & MOQ_D16_DGRAM_FLAG_ZERO_OBJECT_ID) != 0;
    out->default_priority = (type & MOQ_D16_DGRAM_FLAG_DEFAULT_PRIO) != 0;
    out->is_status        = (type & MOQ_D16_DGRAM_FLAG_STATUS) != 0;

    rc = moq_buf_read_varint(&r, &out->track_alias);
    if (rc < 0) return MOQ_ERR_PROTO;

    rc = moq_buf_read_varint(&r, &out->group_id);
    if (rc < 0) return MOQ_ERR_PROTO;

    if (zero_object_id) {
        out->object_id = 0;
    } else {
        rc = moq_buf_read_varint(&r, &out->object_id);
        if (rc < 0) return MOQ_ERR_PROTO;
    }

    if (out->default_priority) {
        out->publisher_priority = 128;
    } else {
        if (moq_buf_reader_remaining(&r) < 1)
            return MOQ_ERR_PROTO;
        out->publisher_priority = r.data[r.pos];
        r.pos++;
    }

    if (out->has_extensions) {
        uint64_t ext_len = 0;
        rc = moq_buf_read_varint(&r, &ext_len);
        if (rc < 0) return MOQ_ERR_PROTO;
        if (ext_len == 0) return MOQ_ERR_PROTO;
        if (ext_len > moq_buf_reader_remaining(&r))
            return MOQ_ERR_PROTO;
        out->extensions = moq_buf_reader_ptr(&r);
        out->extensions_len = (size_t)ext_len;
        r.pos += (size_t)ext_len;
    }

    if (out->is_status) {
        if (out->has_extensions) return MOQ_ERR_PROTO;
        rc = moq_buf_read_varint(&r, &out->object_status);
        if (rc < 0) return MOQ_ERR_PROTO;
        if (out->object_status != MOQ_OBJECT_STATUS_NORMAL &&
            out->object_status != MOQ_OBJECT_STATUS_END_OF_GROUP &&
            out->object_status != MOQ_OBJECT_STATUS_END_OF_TRACK)
            return MOQ_ERR_PROTO;
        if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
        out->payload = NULL;
        out->payload_len = 0;
    } else {
        if (moq_buf_reader_remaining(&r) == 0) return MOQ_ERR_PROTO;
        out->payload = moq_buf_reader_ptr(&r);
        out->payload_len = moq_buf_reader_remaining(&r);
    }

    return MOQ_OK;
}

moq_result_t moq_d16_encode_object_datagram(
    moq_buf_writer_t *w,
    const moq_d16_object_datagram_t *dg)
{
    if (!w || !dg) return MOQ_ERR_INVAL;

    if (dg->is_status && dg->end_of_group) return MOQ_ERR_INVAL;
    if (dg->is_status && dg->has_extensions) return MOQ_ERR_INVAL;
    if (dg->is_status &&
        dg->object_status != MOQ_OBJECT_STATUS_NORMAL &&
        dg->object_status != MOQ_OBJECT_STATUS_END_OF_GROUP &&
        dg->object_status != MOQ_OBJECT_STATUS_END_OF_TRACK)
        return MOQ_ERR_INVAL;
    if (!dg->is_status && dg->payload_len == 0) return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    uint64_t type = 0;
    if (dg->has_extensions)   type |= MOQ_D16_DGRAM_FLAG_EXTENSIONS;
    if (dg->end_of_group)     type |= MOQ_D16_DGRAM_FLAG_END_OF_GROUP;
    bool zero_oid = (dg->object_id == 0);
    if (zero_oid)             type |= MOQ_D16_DGRAM_FLAG_ZERO_OBJECT_ID;
    if (dg->default_priority) type |= MOQ_D16_DGRAM_FLAG_DEFAULT_PRIO;
    if (dg->is_status)        type |= MOQ_D16_DGRAM_FLAG_STATUS;

    moq_result_t rc = moq_buf_write_varint(w, type);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, dg->track_alias);
    if (rc < 0) { w->pos = saved; return rc; }

    rc = moq_buf_write_varint(w, dg->group_id);
    if (rc < 0) { w->pos = saved; return rc; }

    if (!zero_oid) {
        rc = moq_buf_write_varint(w, dg->object_id);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    if (!dg->default_priority) {
        rc = moq_buf_write_raw(w, &dg->publisher_priority, 1);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    if (dg->has_extensions) {
        if (dg->extensions_len == 0) { w->pos = saved; return MOQ_ERR_INVAL; }
        rc = moq_buf_write_varint(w, (uint64_t)dg->extensions_len);
        if (rc < 0) { w->pos = saved; return rc; }
        rc = moq_buf_write_raw(w, dg->extensions, dg->extensions_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }

    if (dg->is_status) {
        rc = moq_buf_write_varint(w, dg->object_status);
        if (rc < 0) { w->pos = saved; return rc; }
    } else {
        if (dg->payload_len > 0) {
            if (!dg->payload) { w->pos = saved; return MOQ_ERR_INVAL; }
            rc = moq_buf_write_raw(w, dg->payload, dg->payload_len);
            if (rc < 0) { w->pos = saved; return rc; }
        }
    }

    return MOQ_OK;
}
