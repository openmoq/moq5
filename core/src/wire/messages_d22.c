#include "messages_d22_internal.h"
#include "control_d18_internal.h"

#define TRY(expr) do { moq_result_t rc_ = (expr); \
    if (rc_ != MOQ_OK) return rc_; } while (0)

enum { HAS_ID=1, HAS_NS=2, HAS_NAME=4, HAS_ALIAS=8,
       HAS_PARAMS=16, HAS_PROPERTIES=32 };

static bool namespace_request(uint64_t type)
{
    return type == MOQ_D22_PUBLISH_NAMESPACE ||
           type == MOQ_D22_SUBSCRIBE_NAMESPACE || type == MOQ_D22_SUBSCRIBE_TRACKS;
}

static moq_result_t layout(uint64_t type, const moq_d22_message_context_t *ctx,
                           unsigned *fields, moq_d22_param_scope_t *scope)
{
    *fields = 0;
    *scope = MOQ_D22_SCOPE_FETCH_OK;
    switch (type) {
    case MOQ_D22_SUBSCRIBE:
        *scope = MOQ_D22_SCOPE_SUBSCRIBE;
        *fields = HAS_ID|HAS_NS|HAS_NAME|HAS_PARAMS; break;
    case MOQ_D22_FETCH:
        *scope = MOQ_D22_SCOPE_FETCH;
        *fields = HAS_ID|HAS_NS|HAS_NAME|HAS_PARAMS; break;
    case MOQ_D22_TRACK_STATUS:
        *scope = MOQ_D22_SCOPE_TRACK_STATUS;
        *fields = HAS_ID|HAS_NS|HAS_NAME|HAS_PARAMS; break;
    case MOQ_D22_PUBLISH:
        *scope = MOQ_D22_SCOPE_PUBLISH;
        *fields = HAS_ID|HAS_NS|HAS_NAME|HAS_ALIAS|HAS_PARAMS|HAS_PROPERTIES; break;
    case MOQ_D22_SUBSCRIBE_OK:
        *scope = MOQ_D22_SCOPE_SUBSCRIBE_OK;
        *fields = HAS_ALIAS|HAS_PARAMS|HAS_PROPERTIES; break;
    case MOQ_D22_FETCH_OK:
        *fields = HAS_PARAMS|HAS_PROPERTIES; break;
    case MOQ_D22_PUBLISH_NAMESPACE:
        *scope = MOQ_D22_SCOPE_PUBLISH_NAMESPACE;
        *fields = HAS_ID|HAS_NS|HAS_PARAMS; break;
    case MOQ_D22_SUBSCRIBE_NAMESPACE:
        *scope = MOQ_D22_SCOPE_SUBSCRIBE_NAMESPACE;
        *fields = HAS_ID|HAS_NS|HAS_PARAMS; break;
    case MOQ_D22_SUBSCRIBE_TRACKS:
        *scope = MOQ_D22_SCOPE_SUBSCRIBE_TRACKS;
        *fields = HAS_ID|HAS_NS|HAS_PARAMS; break;
    case MOQ_D22_PUBLISH_STATE_NOTIFY:
        *scope = MOQ_D22_SCOPE_NOTIFY;
        *fields = HAS_PARAMS; break;
    case MOQ_D22_NAMESPACE:
        *fields = HAS_NS;
        if (ctx->cluster) {
            *fields |= HAS_PARAMS;
            *scope = MOQ_D22_SCOPE_NAMESPACE;
        }
        break;
    case MOQ_D22_NAMESPACE_DONE:
        *fields = HAS_NS; break;
    case MOQ_D22_PUBLISH_SKIPPED:
        *fields = HAS_NS|HAS_NAME; break;
    case MOQ_D22_PUBLISH_DONE: case MOQ_D22_REQUEST_ERROR: break;
    case MOQ_D22_REQUEST_OK:
        *fields = HAS_PARAMS;
        switch (ctx->request_type) {
        case MOQ_D22_PUBLISH: *scope = MOQ_D22_SCOPE_PUBLISH_OK; break;
        case MOQ_D22_REQUEST_UPDATE: *scope = MOQ_D22_SCOPE_UPDATE_OK; break;
        case MOQ_D22_TRACK_STATUS:
            *scope = MOQ_D22_SCOPE_TRACK_STATUS_OK;
            *fields |= HAS_PROPERTIES; break;
        case MOQ_D22_PUBLISH_NAMESPACE: case MOQ_D22_SUBSCRIBE_NAMESPACE:
            *scope = MOQ_D22_SCOPE_NAMESPACE_OK; break;
        /* §9.3 requires the close for Track Properties only in PUBLISH_OK,
         * REQUEST_UPDATE_OK, SUBSCRIBE_NAMESPACE_OK and PUBLISH_NAMESPACE_OK;
         * SUBSCRIBE_TRACKS_OK may carry them (the receiver ignores them). */
        case MOQ_D22_SUBSCRIBE_TRACKS:
            *scope = MOQ_D22_SCOPE_NAMESPACE_OK;
            *fields |= HAS_PROPERTIES; break;
        default: return MOQ_ERR_INVAL;
        }
        break;
    case MOQ_D22_REQUEST_UPDATE:
        *fields = HAS_ID|HAS_PARAMS;
        switch (ctx->request_type) {
        case MOQ_D22_SUBSCRIBE: case MOQ_D22_PUBLISH:
            *scope = ctx->update_from_subscriber ? MOQ_D22_SCOPE_UPDATE_SUBSCRIBER :
                MOQ_D22_SCOPE_UPDATE_PUBLISHER; break;
        case MOQ_D22_FETCH: *scope = MOQ_D22_SCOPE_UPDATE_FETCH; break;
        case MOQ_D22_PUBLISH_NAMESPACE: *scope = MOQ_D22_SCOPE_UPDATE_PUBLISH_NAMESPACE; break;
        case MOQ_D22_SUBSCRIBE_NAMESPACE: *scope = MOQ_D22_SCOPE_UPDATE_SUBSCRIBE_NAMESPACE; break;
        case MOQ_D22_SUBSCRIBE_TRACKS: *scope = MOQ_D22_SCOPE_UPDATE_SUBSCRIBE_TRACKS; break;
        default: return MOQ_ERR_INVAL;
        }
        break;
    default: return MOQ_ERR_PROTO;
    }
    return MOQ_OK;
}

static moq_result_t read_span(moq_buf_reader_t *r, size_t limit, moq_bytes_t *out)
{
    uint64_t len;
    TRY(moq_buf_read_vi64(r, &len));
    if (len > limit) return MOQ_ERR_PROTO;
    if (len > moq_buf_reader_remaining(r)) return MOQ_ERR_BUFFER;
    return moq_buf_read_raw(r, (size_t)len, out);
}

/* §3.7: types 0x4000-0x7FFF are Mandatory Track Properties. None is
 * understood here, so any one present is recorded in *mandatory (when
 * non-NULL); it is not itself malformed. */
static moq_result_t validate_property_ranges(moq_bytes_t bytes, bool nested,
                                             bool *mandatory)
{
    moq_buf_reader_t r = {bytes.data, bytes.len, 0};
    uint64_t previous = 0;
    while (moq_buf_reader_remaining(&r)) {
        moq_d22_kvp_t kvp;
        moq_result_t rc = moq_d22_decode_kvp(&r, &previous, &kvp);
        if (rc != MOQ_OK) return rc == MOQ_ERR_BUFFER ? MOQ_ERR_PROTO : rc;
        if (mandatory && kvp.type >= UINT64_C(0x4000) && kvp.type <= UINT64_C(0x7fff))
            *mandatory = true;
        /* 10.5-.7 apply these ranges equally to mutable and immutable members. */
        if (kvp.type == UINT64_C(0x22) &&
            (kvp.integer < 1 || kvp.integer > 2))
            return MOQ_ERR_PROTO;
        if (kvp.type == UINT64_C(0x30) && kvp.integer > 1)
            return MOQ_ERR_PROTO;
        if (kvp.type == UINT64_C(0x0b)) {
            if (nested) return MOQ_ERR_PROTO;
            TRY(validate_property_ranges(kvp.bytes, true, mandatory));
        }
    }
    return MOQ_OK;
}

static moq_result_t scan_track_properties(const uint8_t *data, size_t len,
                                          bool *mandatory)
{
    if ((!data && len) || len > UINT16_MAX) return MOQ_ERR_INVAL;
    TRY(validate_property_ranges((moq_bytes_t){data, len}, false, mandatory));
    uint8_t priority;
    return moq_d18_scan_default_publisher_priority(data, len, &priority);
}

moq_result_t moq_d22_validate_track_properties(const uint8_t *data, size_t len)
{
    return scan_track_properties(data, len, NULL);
}

static moq_result_t decode_body(moq_buf_reader_t *r, unsigned fields,
                                moq_d22_param_scope_t scope,
                                const moq_d22_message_context_t *ctx,
                                moq_d22_message_t *v)
{
    bool invalid_filter = false;
    if (fields & HAS_ID) TRY(moq_buf_read_vi64(r, &v->request_id));
    if (fields & HAS_NS) TRY(moq_d22_decode_namespace(r, &v->ns));
    if (fields & HAS_NAME) TRY(read_span(r, 4096 - v->ns.value_bytes, &v->track_name));
    if (fields & HAS_ALIAS) TRY(moq_buf_read_vi64(r, &v->track_alias));
    if (v->type == MOQ_D22_FETCH_OK) {
        moq_bytes_t byte;
        TRY(moq_buf_read_raw(r, 1, &byte));
        v->end_of_track = byte.data[0];
        if (v->end_of_track > 1) return MOQ_ERR_PROTO;
        TRY(moq_buf_read_vi64(r, &v->end_location.group));
        TRY(moq_buf_read_vi64(r, &v->end_location.object));
    }
    if (v->type == MOQ_D22_PUBLISH_DONE || v->type == MOQ_D22_REQUEST_ERROR) {
        TRY(moq_buf_read_vi64(r, &v->code));
        if (v->type == MOQ_D22_PUBLISH_DONE)
            TRY(moq_buf_read_vi64(r, &v->stream_count));
        else TRY(moq_buf_read_vi64(r, &v->retry_interval));
        TRY(read_span(r, MOQ_D22_MAX_REASON, &v->reason));
        if (v->type == MOQ_D22_REQUEST_ERROR && v->code == MOQ_D22_ERROR_REDIRECT) {
            /* 9.4.1 has no GOAWAY-style 8192-byte URI cap. The control body
             * length bounds it; URI/endpoint role rules belong to the session. */
            TRY(read_span(r, UINT16_MAX, &v->redirect_uri));
            TRY(moq_d22_decode_namespace(r, &v->ns));
            TRY(read_span(r, 4096 - v->ns.value_bytes, &v->track_name));
            uint64_t target = ctx->request_type == MOQ_D22_REQUEST_UPDATE ?
                ctx->updated_request_type : ctx->request_type;
            if (namespace_request(target) && v->track_name.len)
                return MOQ_ERR_PROTO;
        }
    }
    if (fields & HAS_PARAMS) {
        uint64_t count;
        TRY(moq_buf_read_vi64(r, &count));
        TRY(moq_d22_scan_params(r, count, scope, ctx->max_filter_ranges,
                               &v->params, &invalid_filter));
    }
    if (fields & HAS_PROPERTIES) {
        v->properties = (moq_bytes_t){moq_buf_reader_ptr(r), moq_buf_reader_remaining(r)};
        TRY(scan_track_properties(v->properties.data, v->properties.len,
                                  &v->mandatory_track_property));
        r->pos = r->len;
    }
    if (r->pos != r->len) return MOQ_ERR_PROTO;
    return invalid_filter ? MOQ_D22_ERR_INVALID_FILTER : MOQ_OK;
}

moq_result_t moq_d22_scan_message(uint64_t type, const uint8_t *body, size_t len,
    const moq_d22_message_context_t *ctx, moq_d22_message_t *out)
{
    if ((!body && len) || !ctx || !out) return MOQ_ERR_INVAL;
    if (len > UINT16_MAX) return MOQ_ERR_PROTO;
    unsigned fields;
    moq_d22_param_scope_t scope;
    TRY(layout(type, ctx, &fields, &scope));
    moq_buf_reader_t r = {body, len, 0};
    moq_d22_message_t value = {.type = type};
    moq_result_t rc = decode_body(&r, fields, scope, ctx, &value);
    if (rc != MOQ_OK && rc != MOQ_D22_ERR_INVALID_FILTER)
        return rc == MOQ_ERR_BUFFER ? MOQ_ERR_PROTO : rc;
    *out = value;
    return rc;
}

moq_result_t moq_d22_decode_message(uint64_t type, const uint8_t *body, size_t len,
    const moq_d22_message_context_t *ctx, moq_d22_message_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    moq_d22_message_t value;
    moq_result_t rc = moq_d22_scan_message(type, body, len, ctx, &value);
    if (rc == MOQ_OK) *out = value;
    return rc;
}

static moq_result_t write_span(moq_buf_writer_t *w, moq_bytes_t bytes, size_t limit)
{
    if ((!bytes.data && bytes.len) || bytes.len > limit) return MOQ_ERR_INVAL;
    TRY(moq_buf_write_vi64(w, bytes.len));
    return moq_buf_write_raw(w, bytes.data, bytes.len);
}

static moq_result_t write_namespace(moq_buf_writer_t *w, const moq_d22_namespace_t *ns)
{
    moq_bytes_t b = ns->encoded;
    if ((!b.data && b.len) || b.len > UINT16_MAX) return MOQ_ERR_INVAL;
    moq_buf_reader_t r = {b.data, b.len, 0};
    moq_d22_namespace_t checked;
    if (moq_d22_decode_namespace(&r, &checked) != MOQ_OK || r.pos != r.len)
        return MOQ_ERR_INVAL;
    return moq_buf_write_raw(w, b.data, b.len);
}

static moq_result_t encode_body(moq_buf_writer_t *w, const moq_d22_message_t *m,
    const moq_d22_param_t *p, size_t count, unsigned fields,
    moq_d22_param_scope_t scope, const moq_d22_message_context_t *ctx)
{
    if (fields & HAS_ID) TRY(moq_buf_write_vi64(w, m->request_id));
    if (fields & HAS_NS) TRY(write_namespace(w, &m->ns));
    if (fields & HAS_NAME) TRY(write_span(w, m->track_name, 4096));
    if (fields & HAS_ALIAS) TRY(moq_buf_write_vi64(w, m->track_alias));
    if (m->type == MOQ_D22_FETCH_OK) {
        if (m->end_of_track > 1) return MOQ_ERR_INVAL;
        TRY(moq_buf_write_raw(w, &m->end_of_track, 1));
        TRY(moq_buf_write_vi64(w, m->end_location.group));
        TRY(moq_buf_write_vi64(w, m->end_location.object));
    }
    if (m->type == MOQ_D22_PUBLISH_DONE || m->type == MOQ_D22_REQUEST_ERROR) {
        TRY(moq_buf_write_vi64(w, m->code));
        TRY(moq_buf_write_vi64(w, m->type == MOQ_D22_PUBLISH_DONE ?
                               m->stream_count : m->retry_interval));
        TRY(write_span(w, m->reason, MOQ_D22_MAX_REASON));
        if (m->type == MOQ_D22_REQUEST_ERROR && m->code == MOQ_D22_ERROR_REDIRECT) {
            TRY(write_span(w, m->redirect_uri, UINT16_MAX));
            TRY(write_namespace(w, &m->ns));
            TRY(write_span(w, m->track_name, 4096));
        }
    }
    if (fields & HAS_PARAMS) {
        TRY(moq_buf_write_vi64(w, count));
        TRY(moq_d22_encode_params(w, p, count, scope, ctx->max_filter_ranges));
    } else if (count) return MOQ_ERR_INVAL;
    if (fields & HAS_PROPERTIES) {
        if (moq_d22_validate_track_properties(m->properties.data,
                                            m->properties.len) != MOQ_OK)
            return MOQ_ERR_INVAL;
        TRY(moq_buf_write_raw(w, m->properties.data, m->properties.len));
    } else if (m->properties.len) return MOQ_ERR_INVAL;
    return MOQ_OK;
}

moq_result_t moq_d22_encode_message(moq_buf_writer_t *w,
    const moq_d22_message_t *m, const moq_d22_param_t *p, size_t count,
    const moq_d22_message_context_t *ctx)
{
    if (!w || w->pos > w->cap || (!w->data && w->cap) || !m || !ctx ||
        (!p && count) || count > UINT16_MAX / 2) return MOQ_ERR_INVAL;
    unsigned fields;
    moq_d22_param_scope_t scope;
    if (layout(m->type, ctx, &fields, &scope) != MOQ_OK) return MOQ_ERR_INVAL;
    moq_buf_writer_t c = *w;
    TRY(moq_buf_write_vi64(&c, m->type));
    size_t length_at;
    TRY(moq_buf_reserve_uint16(&c, &length_at));
    size_t body_at = c.pos;
    TRY(encode_body(&c, m, p, count, fields, scope, ctx));
    size_t len = c.pos - body_at;
    if (len > UINT16_MAX) return MOQ_ERR_INVAL;
    moq_d22_message_t checked;
    if (moq_d22_decode_message(m->type, c.data + body_at, len, ctx, &checked) != MOQ_OK)
        return MOQ_ERR_INVAL;
    TRY(moq_buf_patch_uint16(&c, length_at, (uint16_t)len));
    *w = c;
    return MOQ_OK;
}
