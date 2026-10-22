#include "control_d22_internal.h"
#include "moq/vi64.h"

/* All composite operations use local cursors and publish them only on success. */
#define D22_TRY(expr) do { moq_result_t rc_ = (expr); \
    if (rc_ != MOQ_OK) return rc_; } while (0)

static bool reader_valid(const moq_buf_reader_t *r)
{
    return r && r->pos <= r->len && (r->data || r->len == 0);
}

static bool writer_valid(const moq_buf_writer_t *w)
{
    return w && w->pos <= w->cap && (w->data || w->cap == 0);
}

moq_result_t moq_d22_decode_envelope(moq_buf_reader_t *r,
                                    moq_control_envelope_t *out)
{
    if (!reader_valid(r) || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t cursor = *r;
    moq_control_envelope_t value = {0};
    D22_TRY(moq_buf_read_vi64(&cursor, &value.msg_type));
    D22_TRY(moq_buf_read_uint16(&cursor, &value.payload_len));
    if (value.payload_len > moq_buf_reader_remaining(&cursor))
        return MOQ_ERR_BUFFER;
    value.payload = moq_buf_reader_ptr(&cursor);
    cursor.pos += value.payload_len;
    *r = cursor;
    *out = value;
    return MOQ_OK;
}

moq_result_t moq_d22_decode_location_filter(moq_buf_reader_t *r,
                                    moq_d22_location_filter_t *out)
{
    if (!reader_valid(r) || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t cursor = *r;
    moq_d22_location_filter_t value = {0};
    uint64_t type;
    D22_TRY(moq_buf_read_vi64(&cursor, &type));
    if (type > MOQ_D22_FILTER_NEXT_OBJECT) return MOQ_ERR_PROTO;
    value.type = (moq_d22_filter_type_t)type;
    if (type >= MOQ_D22_FILTER_RELATIVE && type <= MOQ_D22_FILTER_RANGE) {
        D22_TRY(moq_buf_read_vi64(&cursor, &value.start_group));
        if (type >= MOQ_D22_FILTER_ABSOLUTE)
            D22_TRY(moq_buf_read_vi64(&cursor, &value.start_object));
        if (type >= MOQ_D22_FILTER_GROUP_END) {
            D22_TRY(moq_buf_read_vi64(&cursor, &value.end_group_delta));
            if (value.end_group_delta > UINT64_MAX - value.start_group)
                return MOQ_ERR_PROTO;
        }
        if (type == MOQ_D22_FILTER_RANGE)
            D22_TRY(moq_buf_read_vi64(&cursor, &value.end_object));
    }
    *r = cursor;
    *out = value;
    return MOQ_OK;
}

moq_result_t moq_d22_encode_location_filter(moq_buf_writer_t *w,
                                    const moq_d22_location_filter_t *filter)
{
    if (!writer_valid(w) || !filter) return MOQ_ERR_INVAL;
    if ((unsigned)filter->type > MOQ_D22_FILTER_NEXT_OBJECT) return MOQ_ERR_INVAL;
    if ((filter->type == MOQ_D22_FILTER_GROUP_END ||
         filter->type == MOQ_D22_FILTER_RANGE) &&
        filter->end_group_delta > UINT64_MAX - filter->start_group)
        return MOQ_ERR_INVAL;
    moq_buf_writer_t cursor = *w;
    D22_TRY(moq_buf_write_vi64(&cursor, (uint64_t)filter->type));
    if (filter->type >= MOQ_D22_FILTER_RELATIVE &&
        filter->type <= MOQ_D22_FILTER_RANGE) {
        D22_TRY(moq_buf_write_vi64(&cursor, filter->start_group));
        if (filter->type >= MOQ_D22_FILTER_ABSOLUTE)
            D22_TRY(moq_buf_write_vi64(&cursor, filter->start_object));
        if (filter->type >= MOQ_D22_FILTER_GROUP_END)
            D22_TRY(moq_buf_write_vi64(&cursor, filter->end_group_delta));
        if (filter->type == MOQ_D22_FILTER_RANGE)
            D22_TRY(moq_buf_write_vi64(&cursor, filter->end_object));
    }
    *w = cursor;
    return MOQ_OK;
}

uint64_t moq_d22_relative_start_group(uint64_t group, uint64_t offset)
{
    /* Compute clamp(group + 1 - offset) without overflowing at group=MAX. */
    if (offset == 0) return group == UINT64_MAX ? UINT64_MAX : group + 1;
    return offset - 1 > group ? 0 : group - (offset - 1);
}

moq_result_t moq_d22_decode_kvp(moq_buf_reader_t *r, uint64_t *previous_type,
                               moq_d22_kvp_t *out)
{
    if (!reader_valid(r) || !previous_type || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t cursor = *r;
    moq_d22_kvp_t value = {0};
    uint64_t delta;
    D22_TRY(moq_buf_read_vi64(&cursor, &delta));
    if (delta > UINT64_MAX - *previous_type) return MOQ_ERR_PROTO;
    value.type = *previous_type + delta;
    if ((value.type & 1u) != 0) {
        uint64_t len;
        D22_TRY(moq_buf_read_vi64(&cursor, &len));
        if (len > MOQ_D22_MAX_KVP) return MOQ_ERR_PROTO;
        if (len > moq_buf_reader_remaining(&cursor)) return MOQ_ERR_BUFFER;
        D22_TRY(moq_buf_read_raw(&cursor, (size_t)len, &value.bytes));
    } else {
        D22_TRY(moq_buf_read_vi64(&cursor, &value.integer));
    }
    *r = cursor;
    *previous_type = value.type;
    *out = value;
    return MOQ_OK;
}

static moq_result_t validate_token(moq_bytes_t bytes)
{
    moq_buf_reader_t r = {bytes.data, bytes.len, 0};
    uint64_t type, unused;
    if (moq_buf_read_vi64(&r, &type) != MOQ_OK || type > 3)
        return MOQ_D22_ERR_KVP_FORMAT;
    if (type != 3 && moq_buf_read_vi64(&r, &unused) != MOQ_OK)
        return MOQ_D22_ERR_KVP_FORMAT;
    if (type == 1 || type == 3) {
        if (moq_buf_read_vi64(&r, &unused) != MOQ_OK)
            return MOQ_D22_ERR_KVP_FORMAT;
    } else if (moq_buf_reader_remaining(&r) != 0) {
        return MOQ_D22_ERR_KVP_FORMAT;
    }
    return MOQ_OK;
}

/* Shared semantic validation for encode and decode of understood SETUP KVPs. */
static moq_result_t accept_setup_option(moq_d22_setup_options_t *s,
                                        const moq_d22_kvp_t *kvp)
{
    bool *present = NULL;
    switch (kvp->type) {
    case 1: present = &s->has_path; s->path = kvp->bytes; break;
    case 3:
        D22_TRY(validate_token(kvp->bytes));
        ++s->auth_token_count;
        return MOQ_OK;
    case 4:
        present = &s->has_max_auth_token_cache_size;
        s->max_auth_token_cache_size = kvp->integer;
        break;
    case 5: present = &s->has_authority; s->authority = kvp->bytes; break;
    case 6:
        present = &s->has_max_filter_ranges;
        s->max_filter_ranges = kvp->integer;
        break;
    case 7:
        present = &s->has_implementation; s->implementation = kvp->bytes;
        break;
    case UINT64_C(0x40B54):
        present = &s->has_hop_id;
        s->hop_id = kvp->integer;
        break;
    case UINT64_C(0x40B56):
        present = &s->has_relay_cost;
        s->relay_cost = kvp->integer;
        break;
    case 8:
        present = &s->has_max_request_updates;
        s->max_request_updates = kvp->integer;
        break;
    default: return MOQ_OK;
    }
    if (*present) return MOQ_ERR_PROTO;
    *present = true;
    return MOQ_OK;
}

moq_result_t moq_d22_decode_setup_options(const uint8_t *body, size_t len,
                                         moq_d22_setup_options_t *out)
{
    if ((!body && len != 0) || !out) return MOQ_ERR_INVAL;
    if (len > UINT16_MAX) return MOQ_ERR_PROTO;
    moq_buf_reader_t r = {body, len, 0};
    moq_d22_setup_options_t value = {0};
    uint64_t previous = 0;
    while (moq_buf_reader_remaining(&r) > 0) {
        moq_d22_kvp_t kvp;
        moq_result_t rc = moq_d22_decode_kvp(&r, &previous, &kvp);
        if (rc != MOQ_OK) return rc == MOQ_ERR_BUFFER ? MOQ_ERR_PROTO : rc;
        D22_TRY(accept_setup_option(&value, &kvp));
    }
    value.encoded = (moq_bytes_t){body, len};
    *out = value;
    return MOQ_OK;
}

moq_result_t moq_d22_encode_setup(moq_buf_writer_t *w,
                                 const moq_d22_kvp_t *options, size_t count)
{
    if (!writer_valid(w) || (!options && count != 0)) return MOQ_ERR_INVAL;
    if (count > UINT16_MAX / 2) return MOQ_ERR_INVAL;
    size_t size = 0;
    uint64_t previous = 0;
    moq_d22_setup_options_t checked = {0};
    for (size_t i = 0; i < count; ++i) {
        const moq_d22_kvp_t *kvp = &options[i];
        if (kvp->type < previous) return MOQ_ERR_INVAL;
        size_t n = moq_vi64_len(kvp->type - previous);
        if ((kvp->type & 1u) != 0) {
            if (kvp->bytes.len > MOQ_D22_MAX_KVP ||
                (!kvp->bytes.data && kvp->bytes.len != 0)) return MOQ_ERR_INVAL;
            n += moq_vi64_len(kvp->bytes.len) + kvp->bytes.len;
        } else {
            n += moq_vi64_len(kvp->integer);
        }
        if (n > UINT16_MAX - size) return MOQ_ERR_INVAL;
        if (accept_setup_option(&checked, kvp) != MOQ_OK) return MOQ_ERR_INVAL;
        size += n;
        previous = kvp->type;
    }
    if (moq_vi64_len(MOQ_D22_SETUP) + 2 + size > moq_buf_writer_remaining(w))
        return MOQ_ERR_BUFFER;
    moq_buf_writer_t cursor = *w;
    D22_TRY(moq_buf_write_vi64(&cursor, MOQ_D22_SETUP));
    D22_TRY(moq_buf_write_uint16(&cursor, (uint16_t)size));
    previous = 0;
    for (size_t i = 0; i < count; ++i) {
        const moq_d22_kvp_t *kvp = &options[i];
        D22_TRY(moq_buf_write_vi64(&cursor, kvp->type - previous));
        if ((kvp->type & 1u) != 0) {
            D22_TRY(moq_buf_write_vi64(&cursor, kvp->bytes.len));
            D22_TRY(moq_buf_write_raw(&cursor, kvp->bytes.data, kvp->bytes.len));
        } else {
            D22_TRY(moq_buf_write_vi64(&cursor, kvp->integer));
        }
        previous = kvp->type;
    }
    *w = cursor;
    return MOQ_OK;
}

moq_result_t moq_d22_decode_goaway(const uint8_t *body, size_t len,
                                 moq_d22_goaway_t *out)
{
    if ((!body && len != 0) || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r = {body, len, 0};
    moq_d22_goaway_t value = {0};
    uint64_t uri_len;
    if (moq_buf_read_vi64(&r, &uri_len) != MOQ_OK || uri_len > MOQ_D22_MAX_URI ||
        uri_len > moq_buf_reader_remaining(&r)) return MOQ_ERR_PROTO;
    D22_TRY(moq_buf_read_raw(&r, (size_t)uri_len, &value.uri));
    if (moq_buf_read_vi64(&r, &value.timeout_ms) != MOQ_OK ||
        moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    *out = value;
    return MOQ_OK;
}

moq_result_t moq_d22_encode_goaway(moq_buf_writer_t *w,
                                 const moq_d22_goaway_t *goaway)
{
    if (!writer_valid(w) || !goaway || goaway->uri.len > MOQ_D22_MAX_URI ||
        (!goaway->uri.data && goaway->uri.len != 0)) return MOQ_ERR_INVAL;
    size_t size = moq_vi64_len(goaway->uri.len) + goaway->uri.len +
        moq_vi64_len(goaway->timeout_ms);
    if (moq_vi64_len(MOQ_D22_GOAWAY) + 2 + size > moq_buf_writer_remaining(w))
        return MOQ_ERR_BUFFER;
    moq_buf_writer_t cursor = *w;
    D22_TRY(moq_buf_write_vi64(&cursor, MOQ_D22_GOAWAY));
    D22_TRY(moq_buf_write_uint16(&cursor, (uint16_t)size));
    D22_TRY(moq_buf_write_vi64(&cursor, goaway->uri.len));
    D22_TRY(moq_buf_write_raw(&cursor, goaway->uri.data, goaway->uri.len));
    D22_TRY(moq_buf_write_vi64(&cursor, goaway->timeout_ms));
    *w = cursor;
    return MOQ_OK;
}
