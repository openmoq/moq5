#include "params_d22_internal.h"
#include "moq/vi64.h"

#define TRY(expr) do { moq_result_t rc_ = (expr); \
    if (rc_ != MOQ_OK) return rc_; } while (0)
#define B(type) MOQ_D22_PARAM_PRESENT(type)
#define RANGES (B(0x25) | B(0x26) | B(0x27) | B(0x28))

/* Sections 9.3-9.20. In particular, 9.18's explicit allowed set wins over
 * the contradictory SUBSCRIBE_TRACKS fill/location prose in 3.6.2.
 * FILL_PARAMETERS is subscriber-originated: 3.4/3.4.1 have the publisher
 * open a fill when it processes the SUBSCRIBE or REQUEST_UPDATE carrying
 * it, and the subscriber learn its range from the response, so a fill
 * request only flows from subscriber to publisher (SUBSCRIBE,
 * SUBSCRIBE_TRACKS, the subscriber's REQUEST_UPDATE, including one after
 * PUBLISH_OK). The publisher's own REQUEST_UPDATE of its PUBLISH, like its
 * PUBLISH and PUBLISH_STATE_NOTIFY, cannot carry one. */
static const uint64_t allowed[] = {
    [MOQ_D22_SCOPE_SUBSCRIBE] =
        B(2) | B(3) | B(4) | B(6) | B(16) | B(32) | B(33) | B(34) | B(35) | RANGES | B(50) | B(53),
    [MOQ_D22_SCOPE_PUBLISH] = B(2) | B(3) | B(6) | B(8) | B(9) | B(16) | B(32) | B(33) | B(34),
    [MOQ_D22_SCOPE_FETCH] = B(3) | B(10) | B(32) | B(33) | B(34) | RANGES | B(53),
    [MOQ_D22_SCOPE_TRACK_STATUS] = B(3) | B(53),
    [MOQ_D22_SCOPE_PUBLISH_NAMESPACE] = B(3) | B(63),
    [MOQ_D22_SCOPE_SUBSCRIBE_NAMESPACE] = B(3),
    [MOQ_D22_SCOPE_SUBSCRIBE_TRACKS] = B(3) | B(16) | B(34) | RANGES | B(41) | B(53),
    [MOQ_D22_SCOPE_SUBSCRIBE_OK] = B(8) | B(9),
    [MOQ_D22_SCOPE_PUBLISH_OK] = B(8),
    [MOQ_D22_SCOPE_UPDATE_OK] = B(8) | B(9),
    [MOQ_D22_SCOPE_TRACK_STATUS_OK] = B(9),
    [MOQ_D22_SCOPE_NAMESPACE_OK] = B(8),
    [MOQ_D22_SCOPE_FETCH_OK] = 0,
    [MOQ_D22_SCOPE_NOTIFY] = B(9) | B(16) | B(33),
    [MOQ_D22_SCOPE_UPDATE_SUBSCRIBER] =
        B(2) | B(3) | B(6) | B(16) | B(32) | B(33) | B(35) | RANGES | B(50),
    [MOQ_D22_SCOPE_UPDATE_PUBLISHER] = B(2) | B(3) | B(6) | B(16) | B(32) | B(33) | B(50),
    [MOQ_D22_SCOPE_UPDATE_FETCH] = B(3) | B(32),
    [MOQ_D22_SCOPE_UPDATE_PUBLISH_NAMESPACE] = B(3) | B(63),
    [MOQ_D22_SCOPE_UPDATE_SUBSCRIBE_NAMESPACE] = B(3) | B(52),
    [MOQ_D22_SCOPE_UPDATE_SUBSCRIBE_TRACKS] = B(3) | B(16) | B(41) | B(52),
    [MOQ_D22_SCOPE_FILL] = B(10) | B(32) | B(33) | B(34) | RANGES,
    [MOQ_D22_SCOPE_NAMESPACE] = B(63)};

static bool reader_valid(const moq_buf_reader_t *r)
{ return r && r->pos <= r->len && (r->data || r->len == 0); }

static bool writer_valid(const moq_buf_writer_t *w)
{ return w && w->pos <= w->cap && (w->data || w->cap == 0); }

static bool scope_valid(moq_d22_param_scope_t scope)
{
    return (unsigned)scope <= (unsigned)MOQ_D22_SCOPE_NAMESPACE;
}

static bool is_range(uint64_t type)
{ return type >= 0x25 && type <= 0x29; }

static moq_result_t read_byte(moq_buf_reader_t *r, uint8_t *out)
{
    if (moq_buf_reader_remaining(r) == 0) return MOQ_ERR_BUFFER;
    *out = r->data[r->pos++];
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

static moq_result_t write_span(moq_buf_writer_t *w, moq_bytes_t bytes)
{
    if ((!bytes.data && bytes.len) || bytes.len > UINT16_MAX) return MOQ_ERR_INVAL;
    TRY(moq_buf_write_vi64(w, bytes.len));
    return moq_buf_write_raw(w, bytes.data, bytes.len);
}

moq_result_t moq_d22_decode_namespace(moq_buf_reader_t *r, moq_d22_namespace_t *out)
{
    if (!reader_valid(r) || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t c = *r;
    moq_d22_namespace_t v = {0};
    uint64_t count;
    TRY(moq_buf_read_vi64(&c, &count));
    if (count > 32) return MOQ_ERR_PROTO;
    v.count = (size_t)count;
    v.fields.data = moq_buf_reader_ptr(&c);
    size_t fields_start = c.pos;
    for (size_t i = 0; i < v.count; ++i) {
        moq_bytes_t field;
        TRY(read_span(&c, 4096 - v.value_bytes, &field));
        if (field.len == 0) return MOQ_ERR_PROTO;
        v.value_bytes += field.len;
    }
    v.fields.len = c.pos - fields_start;
    v.encoded = (moq_bytes_t){moq_buf_reader_ptr(r), c.pos - r->pos};
    *r = c;
    *out = v;
    return MOQ_OK;
}

moq_result_t moq_d22_encode_namespace(moq_buf_writer_t *w, const moq_namespace_t *ns)
{
    if (!writer_valid(w) || !ns || ns->count > 32 ||
        (!ns->parts && ns->count)) return MOQ_ERR_INVAL;
    size_t total = 0;
    for (size_t i = 0; i < ns->count; ++i) {
        if (!ns->parts[i].data || ns->parts[i].len == 0 ||
            ns->parts[i].len > 4096 - total) return MOQ_ERR_INVAL;
        total += ns->parts[i].len;
    }
    moq_buf_writer_t c = *w;
    TRY(moq_buf_write_vi64(&c, ns->count));
    for (size_t i = 0; i < ns->count; ++i) TRY(write_span(&c, ns->parts[i]));
    *w = c;
    return MOQ_OK;
}

static moq_result_t scan_range(moq_buf_reader_t *r, uint64_t *previous_end,
                              moq_d22_range_t *out, bool *invalid_filter)
{
    moq_buf_reader_t c = *r;
    moq_d22_range_t v = {0};
    uint64_t delta;
    bool invalid = false;
    TRY(moq_buf_read_vi64(&c, &delta));
    if (delta > UINT64_MAX - *previous_end) invalid = true;
    else v.start = *previous_end + delta;
    v.open_end = moq_buf_reader_remaining(&c) == 0;
    v.end = UINT64_MAX;
    if (!v.open_end) {
        TRY(moq_buf_read_vi64(&c, &delta));
        if (delta > UINT64_MAX - v.start) invalid = true;
        else v.end = v.start + delta;
    }
    /* Invalid numeric fields are not published by strict decoders. Still
     * consume the integers so a later malformed field cannot be hidden. */
    *r = c;
    *previous_end = v.end;
    *out = v;
    *invalid_filter |= invalid;
    return MOQ_OK;
}

moq_result_t moq_d22_range_next(moq_buf_reader_t *r, uint64_t *previous_end,
                              moq_d22_range_t *out)
{
    if (!reader_valid(r) || !previous_end || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t c = *r;
    uint64_t previous = *previous_end;
    moq_d22_range_t v;
    bool invalid = false;
    TRY(scan_range(&c, &previous, &v, &invalid));
    if (invalid) return MOQ_D22_ERR_INVALID_FILTER;
    *r = c;
    *previous_end = previous;
    *out = v;
    return MOQ_OK;
}

static moq_result_t decode_token(moq_bytes_t bytes, moq_d22_token_t *out)
{
    moq_buf_reader_t r = {bytes.data, bytes.len, 0};
    moq_d22_token_t v = {0};
    if (moq_buf_read_vi64(&r, &v.alias_type) != MOQ_OK || v.alias_type > 3)
        return MOQ_D22_ERR_KVP_FORMAT;
    if (v.alias_type != 3 && moq_buf_read_vi64(&r, &v.alias) != MOQ_OK)
        return MOQ_D22_ERR_KVP_FORMAT;
    if (v.alias_type == 1 || v.alias_type == 3) {
        if (moq_buf_read_vi64(&r, &v.type) != MOQ_OK) return MOQ_D22_ERR_KVP_FORMAT;
        v.value = (moq_bytes_t){moq_buf_reader_ptr(&r), moq_buf_reader_remaining(&r)};
    } else if (moq_buf_reader_remaining(&r)) {
        return MOQ_D22_ERR_KVP_FORMAT;
    }
    *out = v;
    return MOQ_OK;
}

static moq_result_t decode_ranges(uint64_t type, moq_bytes_t bytes,
                                  moq_d22_range_filter_t *out, bool *invalid_filter)
{
    moq_d22_range_filter_t v = {0};
    v.clear = bytes.len == 0;
    moq_buf_reader_t r = {bytes.data, bytes.len, 0};
    if (!v.clear) {
        TRY(read_byte(&r, &v.set_id));
        if (type == 0x28 || type == 0x29) {
            if (moq_buf_read_vi64(&r, &v.property_type) != MOQ_OK) return MOQ_ERR_PROTO;
            if (v.property_type & 1u) *invalid_filter = true;
        }
        v.ranges = (moq_bytes_t){moq_buf_reader_ptr(&r), moq_buf_reader_remaining(&r)};
        uint64_t previous = 0;
        while (moq_buf_reader_remaining(&r)) {
            moq_d22_range_t range;
            moq_result_t rc = scan_range(&r, &previous, &range, invalid_filter);
            if (rc != MOQ_OK) return rc == MOQ_ERR_BUFFER ? MOQ_ERR_PROTO : rc;
            if (type == 0x27 && (range.start > 255 || (!range.open_end && range.end > 255)))
                *invalid_filter = true;
            ++v.count;
        }
    }
    *out = v;
    return MOQ_OK;
}

moq_result_t moq_d22_validate_hop_path(moq_bytes_t path)
{
    if (!path.data || !path.len)
        return MOQ_ERR_PROTO;
    /* Fixed scratch, independent of peer path length. A full table divides
     * its ID space by one more bit and re-scans those partitions. At most
     * 64 partitions can be pending; each partition accepts 256 distinct IDs.
     * This avoids quadratic decoder work and needs no allocator in the codec.
     * Zero is the table's empty slot and is deliberately never inserted. */
    struct {
        uint64_t prefix;
        unsigned bits;
    } pending[65];
    size_t count = 1;
    pending[0].prefix = 0;
    pending[0].bits = 0;
    while (count) {
        --count;
        uint64_t prefix = pending[count].prefix;
        unsigned bits = pending[count].bits;
        uint64_t mask = bits ? UINT64_MAX << (64 - bits) : 0;
        uint64_t seen[512] = {0};
        size_t used = 0;
        bool split = false;
        moq_buf_reader_t r = {path.data, path.len, 0};
        while (r.pos < r.len) {
            uint64_t id;
            if (moq_buf_read_vi64(&r, &id) != MOQ_OK)
                return MOQ_ERR_PROTO;
            if (!id || (id & mask) != prefix)
                continue;
            uint64_t hash = id ^ (id >> 33);
            hash *= UINT64_C(0xff51afd7ed558ccd);
            hash ^= hash >> 33;
            size_t slot = (size_t)hash & 511;
            while (seen[slot] && seen[slot] != id)
                slot = (slot + 1) & 511;
            if (seen[slot] == id)
                return MOQ_ERR_PROTO;
            seen[slot] = id;
            if (++used > 256) {
                split = true;
                break;
            }
        }
        if (split) {
            /* More than one distinct ID cannot share all 64 bits. */
            if (bits == 64 || count + 2 > 65)
                return MOQ_ERR_PROTO;
            uint64_t bit = UINT64_C(1) << (63 - bits);
            pending[count].prefix = prefix;
            pending[count++].bits = bits + 1;
            pending[count].prefix = prefix | bit;
            pending[count++].bits = bits + 1;
        }
    }
    return MOQ_OK;
}

static bool cluster_type(uint64_t type)
{
    return type == MOQ_D22_HOP_PATH || type == MOQ_D22_ROUTE_COST;
}

static moq_result_t scan_param(moq_buf_reader_t *r, uint64_t *previous_type,
                              uint64_t legal, moq_d22_param_t *out,
                              bool *invalid_filter)
{
    moq_buf_reader_t c = *r;
    moq_d22_param_t v = {0};
    uint64_t delta;
    uint8_t byte;
    moq_bytes_t span;
    TRY(moq_buf_read_vi64(&c, &delta));
    if (delta > UINT64_MAX - *previous_type) return MOQ_ERR_PROTO;
    v.type = *previous_type + delta;
    if (cluster_type(v.type)) {
        if (!(legal & B(63)))
            return MOQ_ERR_PROTO;
    } else if (v.type >= 63 || !(legal & B(v.type)))
        return MOQ_ERR_PROTO;
    switch (v.type) {
    case MOQ_D22_ROUTE_COST:
    case 2: case 4: case 6: case 8: case 10: case 50:
        TRY(moq_buf_read_vi64(&c, &v.value.integer)); break;
    case MOQ_D22_HOP_PATH:
        TRY(read_span(&c, UINT16_MAX, &v.value.hop_path));
        TRY(moq_d22_validate_hop_path(v.value.hop_path));
        break;
    case 3:
        TRY(read_span(&c, UINT16_MAX, &span));
        TRY(decode_token(span, &v.value.token)); break;
    case 9:
        TRY(moq_buf_read_vi64(&c, &v.value.location.group));
        TRY(moq_buf_read_vi64(&c, &v.value.location.object)); break;
    case 16: case 32: case 34: case 53:
        TRY(read_byte(&c, &byte));
        if (((v.type == 16 || v.type == 53) && byte > 1) ||
            (v.type == 34 && (byte < 1 || byte > 2))) return MOQ_ERR_PROTO;
        v.value.integer = byte; break;
    case 33:
        TRY(moq_d22_decode_location_filter(&c, &v.value.filter)); break;
    case 35:
        TRY(read_span(&c, UINT16_MAX, &v.value.fill)); break;
    case 37: case 38: case 39: case 40: case 41:
        TRY(read_span(&c, UINT16_MAX, &span));
        TRY(decode_ranges(v.type, span, &v.value.ranges, invalid_filter)); break;
    case 52:
        TRY(moq_d22_decode_namespace(&c, &v.value.ns)); break;
    default: return MOQ_ERR_PROTO;
    }
    *r = c;
    *previous_type = v.type;
    *out = v;
    return MOQ_OK;
}

moq_result_t moq_d22_param_next(moq_buf_reader_t *r, uint64_t *previous_type,
                              moq_d22_param_t *out)
{
    if (!reader_valid(r) || !previous_type || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t c = *r;
    uint64_t previous = *previous_type;
    moq_d22_param_t v;
    bool invalid = false;
    TRY(scan_param(&c, &previous, UINT64_MAX, &v, &invalid));
    if (invalid) return MOQ_D22_ERR_INVALID_FILTER;
    *r = c;
    *previous_type = previous;
    *out = v;
    return MOQ_OK;
}

moq_result_t moq_d22_param_next_scanned(moq_buf_reader_t *r, uint64_t *previous_type,
                                      moq_d22_param_t *out)
{
    if (!reader_valid(r) || !previous_type || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t c = *r;
    uint64_t previous = *previous_type;
    moq_d22_param_t v;
    bool invalid = false;
    TRY(scan_param(&c, &previous, UINT64_MAX, &v, &invalid));
    *r = c;
    *previous_type = previous;
    *out = v;
    return MOQ_OK;
}

/* Revisit earlier borrowed ranges instead of allocating a token/filter table.
 * Only repeated range types take this path; scalar duplicates use a bitset. */
static moq_result_t check_range_key(moq_bytes_t prior, const moq_d22_param_t *p)
{
    moq_buf_reader_t r = {prior.data, prior.len, 0};
    uint64_t previous = 0;
    while (moq_buf_reader_remaining(&r)) {
        moq_d22_param_t earlier;
        bool invalid = false;
        TRY(scan_param(&r, &previous, UINT64_MAX, &earlier, &invalid));
        if (earlier.type != p->type) continue;
        const moq_d22_range_filter_t *a = &earlier.value.ranges;
        const moq_d22_range_filter_t *b = &p->value.ranges;
        /* A zero-length removal has no SetID/property key on the wire. */
        if (!a->clear && !b->clear && a->set_id == b->set_id &&
            a->property_type == b->property_type)
            return MOQ_D22_ERR_INVALID_FILTER;
    }
    return MOQ_OK;
}

static moq_result_t decode_fill_value(moq_bytes_t value, uint64_t max,
                                     moq_d22_params_t *out, bool *invalid_filter);

static moq_result_t decode_sequence(moq_buf_reader_t *r, uint64_t count,
                                   moq_d22_param_scope_t scope,
                                   uint64_t max, moq_d22_params_t *out,
                                   bool *invalid_filter)
{
    moq_buf_reader_t c = *r;
    moq_d22_params_t v = {0};
    uint64_t previous = 0;
    bool invalid = false;
    v.encoded.data = moq_buf_reader_ptr(r);
    while (v.count < count) {
        moq_d22_param_t p;
        TRY(scan_param(&c, &previous, allowed[scope], &p, &invalid));
        bool repeat = p.type == MOQ_D22_HOP_PATH     ? v.has_hop_path
                      : p.type == MOQ_D22_ROUTE_COST ? v.has_route_cost
                                                     : (v.present & B(p.type)) != 0;
        if (repeat && p.type != 3 && !is_range(p.type)) return MOQ_ERR_PROTO;
        if (is_range(p.type)) {
            if (max == 0 || v.range_count > max ||
                p.value.ranges.count > max - v.range_count) invalid = true;
            /* Another duplicate cannot change an existing request refusal;
             * the main scan still checks every later field's syntax/scope. */
            if (!invalid && repeat && !p.value.ranges.clear) {
                moq_result_t rc = check_range_key(v.encoded, &p);
                if (rc == MOQ_D22_ERR_INVALID_FILTER) invalid = true;
                else if (rc != MOQ_OK) return rc;
            }
            v.range_count += p.value.ranges.count;
        } else if (p.type == 35) {
            moq_d22_params_t fill;
            bool fill_invalid;
            TRY(decode_fill_value(p.value.fill, max, &fill, &fill_invalid));
            invalid |= fill_invalid;
        }
        if (p.type == MOQ_D22_HOP_PATH)
            v.has_hop_path = true;
        else if (p.type == MOQ_D22_ROUTE_COST)
            v.has_route_cost = true;
        else
            v.present |= B(p.type);
        v.encoded.len = c.pos - r->pos;
        if (v.encoded.len > UINT16_MAX) return MOQ_ERR_PROTO;
        ++v.count;
    }
    *r = c;
    *out = v;
    *invalid_filter = invalid;
    return MOQ_OK;
}

moq_result_t moq_d22_scan_params(moq_buf_reader_t *r, uint64_t count,
    moq_d22_param_scope_t scope, uint64_t max, moq_d22_params_t *out,
    bool *invalid_filter)
{
    if (!reader_valid(r) || !out || !invalid_filter || !scope_valid(scope))
        return MOQ_ERR_INVAL;
    return decode_sequence(r, count, scope, max, out, invalid_filter);
}

moq_result_t moq_d22_decode_params(moq_buf_reader_t *r, uint64_t count,
    moq_d22_param_scope_t scope, uint64_t max, moq_d22_params_t *out)
{
    if (!reader_valid(r) || !out || !scope_valid(scope)) return MOQ_ERR_INVAL;
    moq_buf_reader_t c = *r;
    moq_d22_params_t v;
    bool invalid;
    TRY(moq_d22_scan_params(&c, count, scope, max, &v, &invalid));
    if (invalid) return MOQ_D22_ERR_INVALID_FILTER;
    *r = c;
    *out = v;
    return MOQ_OK;
}

/* Section 9.20.15: the length-prefixed value holds Parameters "encoded as
 * if they were Parameters for a separate message", and a message's
 * Parameters are a Number of Parameters followed by that many (9.20: "the
 * block is bounded by a parameter count rather than a length"). The nested
 * Type Delta chain restarts at 0. The count's parameters must end exactly
 * at the value's end; a short or overlong count is malformed. */
static moq_result_t decode_fill_value(moq_bytes_t value, uint64_t max,
                                     moq_d22_params_t *out, bool *invalid_filter)
{
    moq_buf_reader_t r = {value.data, value.len, 0};
    uint64_t count;
    if (moq_buf_read_vi64(&r, &count) != MOQ_OK) return MOQ_ERR_PROTO;
    moq_d22_params_t v;
    bool invalid;
    moq_result_t rc = decode_sequence(&r, count, MOQ_D22_SCOPE_FILL, max, &v, &invalid);
    if (rc != MOQ_OK) return rc == MOQ_ERR_BUFFER ? MOQ_ERR_PROTO : rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    *out = v;
    *invalid_filter = invalid;
    return MOQ_OK;
}

moq_result_t moq_d22_decode_fill(moq_bytes_t body, uint64_t max, moq_d22_params_t *out)
{
    if ((!body.data && body.len) || !out) return MOQ_ERR_INVAL;
    if (body.len > UINT16_MAX) return MOQ_ERR_PROTO;
    moq_d22_params_t v;
    bool invalid;
    TRY(decode_fill_value(body, max, &v, &invalid));
    if (invalid) return MOQ_D22_ERR_INVALID_FILTER;
    *out = v;
    return MOQ_OK;
}

static moq_result_t encode_value(moq_buf_writer_t *w, const moq_d22_param_t *p)
{
    switch (p->type) {
    case MOQ_D22_ROUTE_COST:
    case 2: case 4: case 6: case 8: case 10: case 50:
        return moq_buf_write_vi64(w, p->value.integer);
    case 16: case 32: case 34: case 53: {
        uint64_t n = p->value.integer;
        if (n > 255 || ((p->type == 16 || p->type == 53) && n > 1) ||
            (p->type == 34 && (n < 1 || n > 2))) return MOQ_ERR_INVAL;
        uint8_t byte = (uint8_t)n;
        return moq_buf_write_raw(w, &byte, 1);
    }
    case 9:
        TRY(moq_buf_write_vi64(w, p->value.location.group));
        return moq_buf_write_vi64(w, p->value.location.object);
    case 33: return moq_d22_encode_location_filter(w, &p->value.filter);
    case MOQ_D22_HOP_PATH:
        if (p->hop_ids) {
            if(p->hop_count>UINT16_MAX)return MOQ_ERR_INVAL;
            size_t bytes = 0;
            for (size_t i = 0; i < p->hop_count; i++)
                bytes += moq_vi64_len(p->hop_ids[i]);
            if (bytes > UINT16_MAX)
                return MOQ_ERR_INVAL;
            TRY(moq_buf_write_vi64(w, bytes));
            for (size_t i = 0; i < p->hop_count; i++)
                TRY(moq_buf_write_vi64(w, p->hop_ids[i]));
            return MOQ_OK;
        }
        return write_span(w, p->value.hop_path);
    case 35: return write_span(w, p->value.fill);
    case 52: {
        const moq_bytes_t bytes = p->value.ns.encoded;
        if ((!bytes.data && bytes.len) || bytes.len > UINT16_MAX) return MOQ_ERR_INVAL;
        moq_buf_reader_t r = {bytes.data, bytes.len, 0};
        moq_d22_namespace_t ns;
        if (moq_d22_decode_namespace(&r, &ns) != MOQ_OK || r.pos != r.len)
            return MOQ_ERR_INVAL;
        return moq_buf_write_raw(w, bytes.data, bytes.len);
    }
    case 3: {
        const moq_d22_token_t *t = &p->value.token;
        if (t->alias_type > 3) return MOQ_ERR_INVAL;
        bool has_value = t->alias_type == 1 || t->alias_type == 3;
        size_t size = moq_vi64_len(t->alias_type);
        if (t->alias_type != 3) size += moq_vi64_len(t->alias);
        if (has_value) {
            size += moq_vi64_len(t->type);
            if ((!t->value.data && t->value.len) || t->value.len > UINT16_MAX - size)
                return MOQ_ERR_INVAL;
            size += t->value.len;
        }
        TRY(moq_buf_write_vi64(w, size));
        TRY(moq_buf_write_vi64(w, t->alias_type));
        if (t->alias_type != 3) TRY(moq_buf_write_vi64(w, t->alias));
        if (has_value) {
            TRY(moq_buf_write_vi64(w, t->type));
            TRY(moq_buf_write_raw(w, t->value.data, t->value.len));
        }
        return MOQ_OK;
    }
    case 37: case 38: case 39: case 40: case 41: {
        const moq_d22_range_filter_t *f = &p->value.ranges;
        if (f->clear) return moq_buf_write_vi64(w, 0);
        bool prop = p->type == 40 || p->type == 41;
        size_t head = 1 + (prop ? moq_vi64_len(f->property_type) : 0);
        if ((!f->ranges.data && f->ranges.len) || f->ranges.len > UINT16_MAX - head)
            return MOQ_ERR_INVAL;
        TRY(moq_buf_write_vi64(w, head + f->ranges.len));
        TRY(moq_buf_write_raw(w, &f->set_id, 1));
        if (prop) TRY(moq_buf_write_vi64(w, f->property_type));
        return moq_buf_write_raw(w, f->ranges.data, f->ranges.len);
    }
    default: return MOQ_ERR_INVAL;
    }
}

moq_result_t moq_d22_encode_params(moq_buf_writer_t *w, const moq_d22_param_t *p,
    size_t count, moq_d22_param_scope_t scope, uint64_t max)
{
    if (!writer_valid(w) || (!p && count) || !scope_valid(scope) ||
        count > UINT16_MAX / 2) return MOQ_ERR_INVAL;
    moq_buf_writer_t c = *w;
    uint64_t previous = 0;
    for (size_t i = 0; i < count; ++i) {
        if (p[i].type < previous)
            return MOQ_ERR_INVAL;
        if (cluster_type(p[i].type)) {
            if (!(allowed[scope] & B(63)))
                return MOQ_ERR_INVAL;
        } else if (p[i].type >= 63 || !(allowed[scope] & B(p[i].type)))
            return MOQ_ERR_INVAL;
        TRY(moq_buf_write_vi64(&c, p[i].type - previous));
        TRY(encode_value(&c, &p[i]));
        if (c.pos - w->pos > UINT16_MAX) return MOQ_ERR_INVAL;
        previous = p[i].type;
    }
    moq_buf_reader_t check = {w->data, c.pos, w->pos};
    moq_d22_params_t checked;
    if (moq_d22_decode_params(&check, count, scope, max, &checked) != MOQ_OK)
        return MOQ_ERR_INVAL;
    *w = c;
    return MOQ_OK;
}
