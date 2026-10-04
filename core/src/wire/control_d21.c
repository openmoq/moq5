/* TRANSITIONAL: see the banner in moq/control_d21.h. */
#include "moq/control_d21.h"
#include "control_d21_internal.h"
#include "moq/vi64.h"
#include <string.h>

/* -- Control envelope (Type vi64 + Length 16 + payload) ------------ */

moq_result_t moq_d21_decode_envelope(moq_buf_reader_t *r,
                                     moq_control_envelope_t *out)
{
    if (!r || !out)
        return MOQ_ERR_INVAL;

    size_t saved = r->pos;

    moq_result_t rc = moq_buf_read_vi64(r, &out->msg_type);
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

moq_result_t moq_d21_encode_setup(moq_buf_writer_t *w)
{
    return moq_d21_encode_setup_opts(w, NULL);
}

/* -- vi64 field helpers (draft-18 uses vi64 for these lengths/counts) - */

/* Write Type (vi64) + a reserved 16-bit Length; *len_off receives the patch
 * offset for moq_buf_patch_uint16 once the payload is written. */
static moq_result_t d21_write_header(moq_buf_writer_t *w, uint64_t type,
                                     size_t *len_off)
{
    moq_result_t rc = moq_buf_write_vi64(w, type);
    if (rc < 0) return rc;
    return moq_buf_reserve_uint16(w, len_off);
}

/* Patch the reserved Length with the payload bytes written since len_off. */
static moq_result_t d21_patch_len(moq_buf_writer_t *w, size_t len_off)
{
    size_t payload = moq_buf_writer_offset(w) - (len_off + 2);
    if (payload > 0xFFFFu) return MOQ_ERR_INVAL;
    return moq_buf_patch_uint16(w, len_off, (uint16_t)payload);
}

static moq_result_t d21_write_span(moq_buf_writer_t *w, moq_bytes_t b)
{
    moq_result_t rc = moq_buf_write_vi64(w, b.len);
    if (rc < 0) return rc;
    if (b.len > 0)
        rc = moq_buf_write_raw(w, b.data, b.len);
    return rc;
}

static moq_result_t d21_read_span(moq_buf_reader_t *r, moq_bytes_t *out)
{
    uint64_t len = 0;
    moq_result_t rc = moq_buf_read_vi64(r, &len);
    if (rc < 0) return rc;
    return moq_buf_read_raw(r, (size_t)len, out);
}

static moq_result_t d21_write_namespace(moq_buf_writer_t *w,
                                        const moq_namespace_t *ns)
{
    if (ns->count > 32) return MOQ_ERR_INVAL;   /* draft-18: 0..32 fields */
    moq_result_t rc = moq_buf_write_vi64(w, ns->count);
    if (rc < 0) return rc;
    for (size_t i = 0; i < ns->count; i++) {
        if (ns->parts[i].len == 0)              /* fields must be non-empty */
            return MOQ_ERR_INVAL;
        rc = d21_write_span(w, ns->parts[i]);
        if (rc < 0) return rc;
    }
    return MOQ_OK;
}

static moq_result_t d21_read_namespace(moq_buf_reader_t *r, moq_bytes_t *parts,
                                       size_t max_parts, moq_namespace_t *out)
{
    uint64_t count = 0;
    moq_result_t rc = moq_buf_read_vi64(r, &count);
    if (rc < 0) return rc;
    if (count > 32) return MOQ_ERR_PROTO;        /* draft-18: 0..32 fields */
    if (count > max_parts) return MOQ_ERR_BUFFER;
    for (uint64_t i = 0; i < count; i++) {
        rc = d21_read_span(r, &parts[i]);
        if (rc < 0) return rc;
        if (parts[i].len == 0) return MOQ_ERR_PROTO;  /* fields non-empty */
    }
    out->parts = parts;
    out->count = (size_t)count;
    return MOQ_OK;
}

/* Full Track Name length = sum of namespace field lengths + track name. */
static uint64_t d21_full_track_len(const moq_namespace_t *ns,
                                   moq_bytes_t track_name)
{
    uint64_t total = track_name.len;
    for (size_t i = 0; i < ns->count; i++)
        total += ns->parts[i].len;
    return total;
}

/* Message parameters each request message may carry (§10.2). The encoder
 * validates the supplied params against the message's set so it cannot emit a
 * parameter the corresponding decoder would reject. */

/* Whether `p` uses only parameters in `mask`: the encoder-side legality check,
 * so an encoder cannot emit a parameter the matching decoder would reject. */
static bool d21_params_within_mask(const moq_d21_msg_params_t *p, uint32_t mask)
{
#define D21_NEEDS(flag, bit) if ((flag) && !(mask & (bit))) return false
    D21_NEEDS(p->has_object_delivery_timeout, MOQ_D21_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT);
    D21_NEEDS(p->auth_token_count > 0, MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN);
    D21_NEEDS(p->has_rendezvous_timeout, MOQ_D21_PARAM_BIT_RENDEZVOUS_TIMEOUT);
    D21_NEEDS(p->has_subgroup_delivery_timeout, MOQ_D21_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT);
    D21_NEEDS(p->has_expires, MOQ_D21_PARAM_BIT_EXPIRES);
    D21_NEEDS(p->has_largest, MOQ_D21_PARAM_BIT_LARGEST_OBJECT);
    D21_NEEDS(p->has_fill_timeout, MOQ_D21_PARAM_BIT_FILL_TIMEOUT);
    D21_NEEDS(p->has_forward, MOQ_D21_PARAM_BIT_FORWARD);
    D21_NEEDS(p->has_subscriber_priority, MOQ_D21_PARAM_BIT_SUBSCRIBER_PRIORITY);
    D21_NEEDS(p->has_location_filter, MOQ_D21_PARAM_BIT_LOCATION_FILTER);
    D21_NEEDS(p->has_group_order, MOQ_D21_PARAM_BIT_GROUP_ORDER);
    D21_NEEDS(p->has_fill, MOQ_D21_PARAM_BIT_FILL_PARAMETERS);
    D21_NEEDS(p->has_new_group_request, MOQ_D21_PARAM_BIT_NEW_GROUP_REQUEST);
    D21_NEEDS(p->has_track_namespace_prefix, MOQ_D21_PARAM_BIT_TRACK_NAMESPACE_PREFIX);
    D21_NEEDS(p->has_include_properties, MOQ_D21_PARAM_BIT_INCLUDE_PROPERTIES);
#undef D21_NEEDS
    return true;
}

/* -- SUBSCRIBE ----------------------------------------------------- */

moq_result_t moq_d21_encode_subscribe(moq_buf_writer_t *w, uint64_t request_id,
                                      const moq_namespace_t *ns,
                                      moq_bytes_t track_name,
                                      const moq_d21_msg_params_t *params)
{
    if (!w || !ns || !params) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(params, MOQ_D21_MASK_SUBSCRIBE))
        return MOQ_ERR_INVAL;
    if (d21_full_track_len(ns, track_name) > MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_SUBSCRIBE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d21_write_namespace(w, ns)) < 0) goto fail;
    if ((rc = d21_write_span(w, track_name)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_subscribe(const uint8_t *payload,
                                      size_t payload_len, moq_bytes_t *parts,
                                      size_t max_parts, moq_d21_subscribe_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d21_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    rc = d21_read_span(&r, &out->track_name);
    if (rc < 0) return rc;
    if (d21_full_track_len(&out->track_namespace, out->track_name) >
        MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d21_decode_msg_params(&r, count, MOQ_D21_MASK_SUBSCRIBE,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- PUBLISH_NAMESPACE (draft-18 §10.15) --------------------------- */

/* PUBLISH_NAMESPACE permits only the AUTHORIZATION_TOKEN message parameter. */

moq_result_t moq_d21_encode_publish_namespace(moq_buf_writer_t *w,
                                              uint64_t request_id,
                                              const moq_namespace_t *ns,
                                              const moq_d21_msg_params_t *params)
{
    if (!w || !ns || !params) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(params, MOQ_D21_MASK_NAMESPACE_REQUEST))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_PUBLISH_NAMESPACE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d21_write_namespace(w, ns)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_publish_namespace(const uint8_t *payload,
                                              size_t payload_len,
                                              moq_bytes_t *parts,
                                              size_t max_parts,
                                              moq_d21_publish_namespace_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d21_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d21_decode_msg_params(&r, count, MOQ_D21_MASK_NAMESPACE_REQUEST,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- SUBSCRIBE_NAMESPACE (draft-18 §10.18) ------------------------- */

/* SUBSCRIBE_NAMESPACE permits only the AUTHORIZATION_TOKEN message parameter
 * (it is namespace-only in draft-18; FORWARD and the interest field moved to
 * SUBSCRIBE_TRACKS). */

moq_result_t moq_d21_encode_subscribe_namespace(moq_buf_writer_t *w,
                                                uint64_t request_id,
                                                const moq_namespace_t *prefix,
                                                const moq_d21_msg_params_t *params)
{
    if (!w || !prefix || !params) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(params, MOQ_D21_MASK_NAMESPACE_REQUEST))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_SUBSCRIBE_NAMESPACE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d21_write_namespace(w, prefix)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_subscribe_namespace(const uint8_t *payload,
                                                size_t payload_len,
                                                moq_bytes_t *parts,
                                                size_t max_parts,
                                                moq_d21_subscribe_namespace_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d21_read_namespace(&r, parts, max_parts, &out->track_namespace_prefix);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d21_decode_msg_params(&r, count, MOQ_D21_MASK_NAMESPACE_REQUEST,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- SUBSCRIBE_TRACKS (draft-18 §10.19) ---------------------------- */

/* SUBSCRIBE_TRACKS permits the FORWARD and AUTHORIZATION_TOKEN message
 * parameters (it governs the FORWARD value of the resulting PUBLISH messages;
 * §10.19). Any other parameter is a protocol violation. */

moq_result_t moq_d21_encode_subscribe_tracks(moq_buf_writer_t *w,
                                             uint64_t request_id,
                                             const moq_namespace_t *prefix,
                                             const moq_d21_msg_params_t *params)
{
    if (!w || !prefix || !params) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(params, MOQ_D21_MASK_SUBSCRIBE_TRACKS))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_SUBSCRIBE_TRACKS, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d21_write_namespace(w, prefix)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_subscribe_tracks(const uint8_t *payload,
                                             size_t payload_len,
                                             moq_bytes_t *parts,
                                             size_t max_parts,
                                             moq_d21_subscribe_tracks_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d21_read_namespace(&r, parts, max_parts, &out->track_namespace_prefix);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d21_decode_msg_params(&r, count, MOQ_D21_MASK_SUBSCRIBE_TRACKS,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- PUBLISH_SKIPPED (draft-21 9.19) ------------------------------- */

moq_result_t moq_d21_encode_publish_skipped(moq_buf_writer_t *w,
                                            const moq_namespace_t *suffix,
                                            moq_bytes_t track_name)
{
    if (!w || !suffix) return MOQ_ERR_INVAL;
    if (d21_full_track_len(suffix, track_name) > MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_PUBLISH_SKIPPED, &len_off);
    if (rc < 0) return rc;
    if ((rc = d21_write_namespace(w, suffix)) < 0) goto fail;
    if ((rc = d21_write_span(w, track_name)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_publish_skipped(const uint8_t *payload,
                                            size_t payload_len,
                                            moq_bytes_t *parts,
                                            size_t max_parts,
                                            moq_d21_publish_skipped_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = d21_read_namespace(&r, parts, max_parts,
                                         &out->track_namespace_suffix);
    if (rc < 0) return rc;
    rc = d21_read_span(&r, &out->track_name);
    if (rc < 0) return rc;
    if (d21_full_track_len(&out->track_namespace_suffix, out->track_name) >
        MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- NAMESPACE / NAMESPACE_DONE (draft-18 §10.16 / §10.17) --------- */

moq_result_t moq_d21_encode_namespace_msg(moq_buf_writer_t *w,
                                          const moq_namespace_t *suffix,
                                          bool is_done)
{
    if (!w || !suffix) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    uint64_t type = is_done ? MOQ_D21_NAMESPACE_DONE : MOQ_D21_NAMESPACE;
    moq_result_t rc = d21_write_header(w, type, &len_off);
    if (rc < 0) return rc;
    if ((rc = d21_write_namespace(w, suffix)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_namespace_msg(const uint8_t *payload,
                                          size_t payload_len,
                                          moq_bytes_t *parts,
                                          size_t max_parts,
                                          moq_namespace_t *out_suffix)
{
    if (!payload || !parts || !out_suffix) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = d21_read_namespace(&r, parts, max_parts, out_suffix);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- TRACK_STATUS (draft-18 §10.14) -------------------------------- */

/* TRACK_STATUS is the SUBSCRIBE layout minus Track-delivery params; only the
 * AUTHORIZATION_TOKEN message parameter applies. */

moq_result_t moq_d21_encode_track_status(moq_buf_writer_t *w, uint64_t request_id,
                                         const moq_namespace_t *ns,
                                         moq_bytes_t track_name,
                                         const moq_d21_msg_params_t *params)
{
    if (!w || !ns || !params) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(params, MOQ_D21_MASK_TRACK_STATUS))
        return MOQ_ERR_INVAL;
    if (d21_full_track_len(ns, track_name) > MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_TRACK_STATUS, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d21_write_namespace(w, ns)) < 0) goto fail;
    if ((rc = d21_write_span(w, track_name)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_track_status(const uint8_t *payload,
                                         size_t payload_len, moq_bytes_t *parts,
                                         size_t max_parts,
                                         moq_d21_track_status_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d21_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    rc = d21_read_span(&r, &out->track_name);
    if (rc < 0) return rc;
    if (d21_full_track_len(&out->track_namespace, out->track_name) >
        MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d21_decode_msg_params(&r, count, MOQ_D21_MASK_TRACK_STATUS,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- Track Properties (KVP tail, §1.4.3 / §2.5) -------------------- *
 * The tail is preserved opaquely; this validates its structure so malformed or
 * mandatory-but-unknown properties are rejected rather than passed through. Each
 * Key-Value-Pair is a Type-Delta then, for an even type, a single varint value;
 * for an odd type, a vi64 length (<= 2^16-1) and that many bytes. Mandatory
 * properties (0x4000-0x7FFF) are not understood here and so are rejected.
 * IMMUTABLE_PROPERTIES (0x0B, §12.7) wraps a nested Key-Value-Pair sequence that
 * is itself Track Properties, so its contents are validated recursively (a
 * mandatory unknown may not hide inside it); a nested IMMUTABLE_PROPERTIES is
 * malformed. */
#define D21_PROP_IMMUTABLE      0x0Bu
#define D21_PROP_DYNAMIC_GROUPS 0x30u   /* §12.6: 0/1; >1 is a violation */

/* Validate a Property KVP block structurally. A Mandatory Track Property
 * (0x4000-0x7FFF, §2.5.1 — including one hidden inside IMMUTABLE_PROPERTIES,
 * §12.7) is handled per `out_mandatory`: when non-NULL it is recorded
 * (*out_mandatory = true) and is NOT itself an error (the caller decides — object
 * properties are malformed and close, track properties respond
 * UNSUPPORTED_EXTENSION); when NULL it is rejected (MOQ_ERR_PROTO). Malformed
 * structure is always MOQ_ERR_PROTO. */
static moq_result_t d21_validate_props_inner(const uint8_t *data, size_t len,
                                             bool nested, bool *out_mandatory,
                                             bool *out_dynamic_groups)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    uint64_t prev = 0;
    while (moq_buf_reader_remaining(&r) > 0) {
        uint64_t delta;
        if (moq_buf_read_vi64(&r, &delta) < 0) return MOQ_ERR_PROTO;
        if (delta > UINT64_MAX - prev) return MOQ_ERR_PROTO;   /* §1.4.3 */
        uint64_t type = prev + delta;
        prev = type;
        if (type >= 0x4000 && type <= 0x7FFF) {
            if (out_mandatory) *out_mandatory = true;
            else return MOQ_ERR_PROTO;       /* unknown mandatory property */
        }
        if (type & 1) {                      /* odd: length-prefixed value */
            uint64_t vlen;
            if (moq_buf_read_vi64(&r, &vlen) < 0) return MOQ_ERR_PROTO;
            if (vlen > 0xFFFFu) return MOQ_ERR_PROTO;
            if (vlen > moq_buf_reader_remaining(&r)) return MOQ_ERR_PROTO;
            if (type == D21_PROP_IMMUTABLE) {
                if (nested) return MOQ_ERR_PROTO;   /* §12.7: no nesting */
                moq_result_t rc = d21_validate_props_inner(
                    moq_buf_reader_ptr(&r), (size_t)vlen, true, out_mandatory,
                    out_dynamic_groups);
                if (rc < 0) return rc;
            }
            r.pos += (size_t)vlen;
        } else {                             /* even: single varint value */
            uint64_t v;
            if (moq_buf_read_vi64(&r, &v) < 0) return MOQ_ERR_PROTO;
            if (type == D21_PROP_DYNAMIC_GROUPS) {
                /* §12.6: allowed values 0/1; anything larger MUST close the
                 * session with PROTOCOL_VIOLATION (the profile maps this
                 * MOQ_ERR_PROTO to the 0x3 close). Immutable Properties are
                 * themselves Track Properties, so the rule applies inside
                 * them too. */
                if (v > 1) return MOQ_ERR_PROTO;
                if (out_dynamic_groups && v == 1) *out_dynamic_groups = true;
            }
        }
    }
    return MOQ_OK;
}

/* Encode-side strict validation: reject malformed structure and mandatory. */
static moq_result_t d21_validate_track_properties(const uint8_t *data,
                                                  size_t len)
{
    return d21_validate_props_inner(data, len, false, NULL, NULL);
}

/* §9.8: pure per-profile timeout scanner. Extracts OBJECT_DELIVERY_TIMEOUT
 * (0x02) and SUBGROUP_DELIVERY_TIMEOUT (0x06) Track Properties (§12.1/§12.2)
 * from a property block, searching BOTH the mutable list and the contents of
 * IMMUTABLE_PROPERTIES (§12.7 processors MUST search both). The timeouts are
 * single-value properties, so the same type appearing twice anywhere across
 * (mutable UNION immutable) is malformed (MOQ_ERR_PROTO), as is a nested
 * IMMUTABLE_PROPERTIES or any structural failure. Unknown properties
 * (including mandatory ones) pass through untouched -- this is an extractor,
 * not a validator; callers layer their own strictness. */
static moq_result_t d21_scan_timeouts_inner(const uint8_t *data, size_t len,
                                            bool nested,
                                            bool *has_obj, uint64_t *obj_ms,
                                            bool *has_sub, uint64_t *sub_ms)
{
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);
    uint64_t prev = 0;
    while (moq_buf_reader_remaining(&r) > 0) {
        uint64_t delta;
        if (moq_buf_read_vi64(&r, &delta) < 0) return MOQ_ERR_PROTO;
        if (delta > UINT64_MAX - prev) return MOQ_ERR_PROTO;
        uint64_t type = prev + delta;
        prev = type;
        if (type & 1) {
            uint64_t vlen;
            if (moq_buf_read_vi64(&r, &vlen) < 0) return MOQ_ERR_PROTO;
            if (vlen > 0xFFFFu) return MOQ_ERR_PROTO;
            if (vlen > moq_buf_reader_remaining(&r)) return MOQ_ERR_PROTO;
            if (type == D21_PROP_IMMUTABLE) {
                if (nested) return MOQ_ERR_PROTO;
                moq_result_t rc = d21_scan_timeouts_inner(
                    moq_buf_reader_ptr(&r), (size_t)vlen, true,
                    has_obj, obj_ms, has_sub, sub_ms);
                if (rc < 0) return rc;
            }
            r.pos += (size_t)vlen;
        } else {
            uint64_t v;
            if (moq_buf_read_vi64(&r, &v) < 0) return MOQ_ERR_PROTO;
            if (type == 0x02u) {               /* OBJECT_DELIVERY_TIMEOUT */
                if (*has_obj) return MOQ_ERR_PROTO;   /* duplicate */
                *has_obj = true; *obj_ms = v;
            } else if (type == 0x06u) {        /* SUBGROUP_DELIVERY_TIMEOUT */
                if (*has_sub) return MOQ_ERR_PROTO;
                *has_sub = true; *sub_ms = v;
            }
        }
    }
    return MOQ_OK;
}

moq_result_t moq_d21_scan_delivery_timeouts(const uint8_t *props, size_t len,
                                            bool *out_has_object,
                                            uint64_t *out_object_ms,
                                            bool *out_has_subgroup,
                                            uint64_t *out_subgroup_ms)
{
    *out_has_object = false;   *out_object_ms = 0;
    *out_has_subgroup = false; *out_subgroup_ms = 0;
    if (!props || len == 0) return MOQ_OK;
    return d21_scan_timeouts_inner(props, len, false,
                                   out_has_object, out_object_ms,
                                   out_has_subgroup, out_subgroup_ms);
}

/* Decode-side scan: structurally valid; *out_mandatory reports whether any
 * Mandatory Track Property is present so the caller can respond with a
 * request-level UNSUPPORTED_EXTENSION rather than closing the session. */
static moq_result_t d21_scan_track_properties(const uint8_t *data, size_t len,
                                              bool *out_mandatory,
                                              bool *out_dynamic_groups)
{
    *out_mandatory = false;
    if (out_dynamic_groups) *out_dynamic_groups = false;
    return d21_validate_props_inner(data, len, false, out_mandatory,
                                    out_dynamic_groups);
}

moq_result_t moq_d21_validate_properties(const uint8_t *data, size_t len)
{
    return d21_validate_props_inner(data, len, false, NULL, NULL);
}

/* Lenient DYNAMIC_GROUPS extraction for the profile's outbound latch: walks
 * tolerantly (mandatory properties permitted) and reports whether 0x30 == 1.
 * Returns MOQ_ERR_PROTO only for a structurally unreadable blob or a 0x30
 * value above 1 (the caller treats both as "no support" on the outbound
 * side; inbound enforcement happens in the decode scans). */
moq_result_t moq_d21_scan_dynamic_groups(const uint8_t *data, size_t len,
                                         bool *out_dynamic_groups)
{
    if (!out_dynamic_groups) return MOQ_ERR_INVAL;
    if (len > 0 && !data) return MOQ_ERR_INVAL;
    bool mandatory_ignored = false;
    *out_dynamic_groups = false;
    return d21_validate_props_inner(data, len, false, &mandatory_ignored,
                                    out_dynamic_groups);
}

/* -- OBJECT_DATAGRAM (§11.3.1) + Padding Datagram (§11.5.2) -------- *
 * The D18 data plane is vi64 throughout (type, track alias, group/object id,
 * property length, object status), matching the subgroup/object codecs above;
 * publisher priority is a raw byte. */
moq_result_t moq_d21_decode_object_datagram(
    const uint8_t *data, size_t len,
    moq_d21_object_datagram_t *out)
{
    if (!data || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);

    uint64_t type = 0;
    if (moq_buf_read_vi64(&r, &type) < 0) return MOQ_ERR_PROTO;
    if (type == MOQ_D21_PADDING_DATAGRAM) {
        /* §11.5.2: the padding bytes MUST all be zero; any non-zero byte is
         * malformed (caller closes), otherwise discard. */
        size_t rem = moq_buf_reader_remaining(&r);
        const uint8_t *p = moq_buf_reader_ptr(&r);
        for (size_t i = 0; i < rem; i++)
            if (p[i] != 0) return MOQ_ERR_PROTO;
        return MOQ_DONE;
    }
    /* Object datagram Type form 0b00X0XXXX: 0x00-0x0F / 0x20-0x2F. */
    if (type > 0x2Fu || (type & 0x10u)) return MOQ_ERR_PROTO;

    bool status = (type & MOQ_D21_DGRAM_BIT_STATUS) != 0;
    bool eog    = (type & MOQ_D21_DGRAM_BIT_END_OF_GROUP) != 0;
    if (status && eog) return MOQ_ERR_PROTO;   /* status cannot signal end of group */
    out->has_properties   = (type & MOQ_D21_DGRAM_BIT_PROPERTIES) != 0;
    out->end_of_group     = eog;
    bool zero_object_id   = (type & MOQ_D21_DGRAM_BIT_ZERO_OBJECT_ID) != 0;
    out->default_priority = (type & MOQ_D21_DGRAM_BIT_DEFAULT_PRIO) != 0;
    out->is_status        = status;

    if (moq_buf_read_vi64(&r, &out->track_alias) < 0) return MOQ_ERR_PROTO;
    if (moq_buf_read_vi64(&r, &out->group_id) < 0) return MOQ_ERR_PROTO;
    if (zero_object_id) {
        out->object_id = 0;
    } else if (moq_buf_read_vi64(&r, &out->object_id) < 0) {
        return MOQ_ERR_PROTO;
    }

    if (out->default_priority) {
        out->publisher_priority = 128;
    } else {
        if (moq_buf_reader_remaining(&r) < 1) return MOQ_ERR_PROTO;
        out->publisher_priority = *moq_buf_reader_ptr(&r);
        r.pos++;
    }

    if (out->has_properties) {
        uint64_t props_len = 0;
        if (moq_buf_read_vi64(&r, &props_len) < 0) return MOQ_ERR_PROTO;
        if (props_len == 0) return MOQ_ERR_PROTO;   /* PROPERTIES bit set, length 0 */
        if (props_len > moq_buf_reader_remaining(&r)) return MOQ_ERR_PROTO;
        const uint8_t *pp = moq_buf_reader_ptr(&r);
        moq_result_t prc = moq_d21_validate_properties(pp, (size_t)props_len);
        if (prc < 0) return prc;
        out->properties = pp;
        out->properties_len = (size_t)props_len;
        r.pos += (size_t)props_len;
    }

    if (out->is_status) {
        if (moq_buf_read_vi64(&r, &out->object_status) < 0) return MOQ_ERR_PROTO;
        if (out->object_status != MOQ_OBJECT_STATUS_NORMAL &&
            out->object_status != MOQ_OBJECT_STATUS_END_OF_GROUP &&
            out->object_status != MOQ_OBJECT_STATUS_END_OF_TRACK)
            return MOQ_ERR_PROTO;
        /* STATUS + PROPERTIES is permitted only on a Normal object (§11.3.1). */
        if (out->has_properties && out->object_status != MOQ_OBJECT_STATUS_NORMAL)
            return MOQ_ERR_PROTO;
        if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
        out->payload = NULL;
        out->payload_len = 0;
    } else {
        /* No STATUS bit ⇒ payload present; the rest of the datagram is the payload.
         * Mirror D16: a non-status object carries a non-empty payload. */
        if (moq_buf_reader_remaining(&r) == 0) return MOQ_ERR_PROTO;
        out->payload = moq_buf_reader_ptr(&r);
        out->payload_len = moq_buf_reader_remaining(&r);
    }
    return MOQ_OK;
}

moq_result_t moq_d21_encode_object_datagram(
    moq_buf_writer_t *w,
    const moq_d21_object_datagram_t *dg)
{
    if (!w || !dg) return MOQ_ERR_INVAL;

    if (dg->is_status && dg->end_of_group) return MOQ_ERR_INVAL;
    if (dg->is_status && dg->has_properties &&
        dg->object_status != MOQ_OBJECT_STATUS_NORMAL)
        return MOQ_ERR_INVAL;
    if (dg->is_status &&
        dg->object_status != MOQ_OBJECT_STATUS_NORMAL &&
        dg->object_status != MOQ_OBJECT_STATUS_END_OF_GROUP &&
        dg->object_status != MOQ_OBJECT_STATUS_END_OF_TRACK)
        return MOQ_ERR_INVAL;
    if (!dg->is_status && dg->payload_len == 0) return MOQ_ERR_INVAL;
    if (dg->has_properties && dg->properties_len == 0) return MOQ_ERR_INVAL;

    size_t saved = w->pos;

    uint64_t type = 0;
    if (dg->has_properties)   type |= MOQ_D21_DGRAM_BIT_PROPERTIES;
    if (dg->end_of_group)     type |= MOQ_D21_DGRAM_BIT_END_OF_GROUP;
    bool zero_oid = (dg->object_id == 0);
    if (zero_oid)             type |= MOQ_D21_DGRAM_BIT_ZERO_OBJECT_ID;
    if (dg->default_priority) type |= MOQ_D21_DGRAM_BIT_DEFAULT_PRIO;
    if (dg->is_status)        type |= MOQ_D21_DGRAM_BIT_STATUS;

    moq_result_t rc = moq_buf_write_vi64(w, type);
    if (rc < 0) { w->pos = saved; return rc; }
    rc = moq_buf_write_vi64(w, dg->track_alias);
    if (rc < 0) { w->pos = saved; return rc; }
    rc = moq_buf_write_vi64(w, dg->group_id);
    if (rc < 0) { w->pos = saved; return rc; }
    if (!zero_oid) {
        rc = moq_buf_write_vi64(w, dg->object_id);
        if (rc < 0) { w->pos = saved; return rc; }
    }
    if (!dg->default_priority) {
        rc = moq_buf_write_raw(w, &dg->publisher_priority, 1);
        if (rc < 0) { w->pos = saved; return rc; }
    }
    if (dg->has_properties) {
        if (!dg->properties) { w->pos = saved; return MOQ_ERR_INVAL; }
        /* Outbound is strict, symmetric with inbound (§2.5.1): never emit a
         * malformed property block or a mandatory track property as an object
         * property. Validate before writing, rolling back on failure. */
        rc = moq_d21_validate_properties(dg->properties, dg->properties_len);
        if (rc < 0) { w->pos = saved; return rc; }
        rc = moq_buf_write_vi64(w, (uint64_t)dg->properties_len);
        if (rc < 0) { w->pos = saved; return rc; }
        rc = moq_buf_write_raw(w, dg->properties, dg->properties_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }
    if (dg->is_status) {
        rc = moq_buf_write_vi64(w, dg->object_status);
        if (rc < 0) { w->pos = saved; return rc; }
    } else {
        if (!dg->payload) { w->pos = saved; return MOQ_ERR_INVAL; }
        rc = moq_buf_write_raw(w, dg->payload, dg->payload_len);
        if (rc < 0) { w->pos = saved; return rc; }
    }
    return MOQ_OK;
}

/* -- SUBSCRIBE_OK -------------------------------------------------- */

/* SUBSCRIBE_OK carries LARGEST_OBJECT / EXPIRES message parameters. */

moq_result_t moq_d21_encode_subscribe_ok(moq_buf_writer_t *w,
                                         uint64_t track_alias,
                                         const moq_d21_msg_params_t *params,
                                         moq_bytes_t track_properties)
{
    if (!w || !params) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(params, MOQ_D21_MASK_SUBSCRIBE_OK))
        return MOQ_ERR_INVAL;
    if (track_properties.len > 0 &&
        d21_validate_track_properties(track_properties.data,
                                      track_properties.len) < 0)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_SUBSCRIBE_OK, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, track_alias)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, params)) < 0) goto fail;
    if (track_properties.len > 0 &&
        (rc = moq_buf_write_raw(w, track_properties.data,
                                track_properties.len)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_subscribe_ok(const uint8_t *payload,
                                         size_t payload_len,
                                         moq_d21_subscribe_ok_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->track_alias);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d21_decode_msg_params(&r, count, MOQ_D21_MASK_SUBSCRIBE_OK,
                                   &out->params);
    if (rc < 0) return rc;
    /* The remainder is the opaque Track Properties tail. */
    out->track_properties.data = moq_buf_reader_ptr(&r);
    out->track_properties.len = moq_buf_reader_remaining(&r);
    return d21_scan_track_properties(out->track_properties.data,
                                     out->track_properties.len,
                                     &out->track_properties_unsupported,
                                     &out->dynamic_groups);
}

/* -- PUBLISH (draft-18 §10.10) ------------------------------------- */

/* PUBLISH permits FORWARD (publisher's initial forward intent),
 * AUTHORIZATION_TOKEN, LARGEST_OBJECT (§10.2.11 -- MUST be included once
 * Objects have been published), and EXPIRES (§10.2.10); other parameters are a
 * violation. */

moq_result_t moq_d21_encode_publish(moq_buf_writer_t *w,
                                    const moq_d21_publish_t *p)
{
    if (!w || !p) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(&p->params, MOQ_D21_MASK_PUBLISH))
        return MOQ_ERR_INVAL;
    if (d21_full_track_len(&p->track_namespace, p->track_name) >
        MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;
    if (p->track_properties.len > 0 &&
        d21_validate_track_properties(p->track_properties.data,
                                      p->track_properties.len) < 0)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_PUBLISH, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, p->request_id)) < 0) goto fail;
    if ((rc = d21_write_namespace(w, &p->track_namespace)) < 0) goto fail;
    if ((rc = d21_write_span(w, p->track_name)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, p->track_alias)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, &p->params)) < 0) goto fail;
    if (p->track_properties.len > 0 &&
        (rc = moq_buf_write_raw(w, p->track_properties.data,
                                p->track_properties.len)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_publish(const uint8_t *payload, size_t payload_len,
                                    moq_bytes_t *parts, size_t max_parts,
                                    moq_d21_publish_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d21_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    rc = d21_read_span(&r, &out->track_name);
    if (rc < 0) return rc;
    if (d21_full_track_len(&out->track_namespace, out->track_name) >
        MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;
    if ((rc = moq_buf_read_vi64(&r, &out->track_alias)) < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d21_decode_msg_params(&r, count, MOQ_D21_MASK_PUBLISH,
                                   &out->params);
    if (rc < 0) return rc;
    /* The remainder is the opaque Track Properties tail. */
    out->track_properties.data = moq_buf_reader_ptr(&r);
    out->track_properties.len = moq_buf_reader_remaining(&r);
    return d21_scan_track_properties(out->track_properties.data,
                                     out->track_properties.len,
                                     &out->track_properties_unsupported,
                                     &out->dynamic_groups);
}

/* -- PUBLISH_OK (draft-18 §10.10 / §10.5) -------------------------- */

/* -- GOAWAY (draft-21 9.2) ----------------------------------------- */

moq_result_t moq_d21_encode_goaway(moq_buf_writer_t *w,
                                   const uint8_t *uri, size_t uri_len,
                                   uint64_t timeout_ms)
{
    if (!w) return MOQ_ERR_INVAL;
    if (uri_len > 0 && !uri) return MOQ_ERR_INVAL;
    if (uri_len > 8192) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_GOAWAY, &len_off);
    if (rc < 0) return rc;
    moq_bytes_t span = { uri, uri_len };
    if ((rc = d21_write_span(w, span)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, timeout_ms)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_goaway(const uint8_t *payload, size_t payload_len,
                                   moq_d21_goaway_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_bytes_t uri;
    moq_result_t rc = d21_read_span(&r, &uri);
    if (rc < 0) return rc;
    if (uri.len > 8192) return MOQ_ERR_PROTO;
    if ((rc = moq_buf_read_vi64(&r, &out->timeout_ms)) < 0) return rc;
    /* Strict: draft 21 has no Request ID, so anything after the Timeout (such as
     * a draft-18 Request ID) is not part of a GOAWAY. */
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    out->uri.data = uri.len > 0 ? uri.data : NULL;
    out->uri.len = uri.len;
    return MOQ_OK;
}

/* -- REQUEST_ERROR ------------------------------------------------- */

moq_result_t moq_d21_encode_request_error(moq_buf_writer_t *w,
                                          uint64_t error_code,
                                          uint64_t retry_interval,
                                          moq_bytes_t reason)
{
    if (!w) return MOQ_ERR_INVAL;
    if (reason.len > MOQ_D21_MAX_REASON) return MOQ_ERR_INVAL;
    /* REDIRECT without its Redirect is malformed (9.4.2): use the *_redirect
     * encoder, which writes it. */
    if (error_code == MOQ_D21_ERROR_REDIRECT) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_REQUEST_ERROR, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, error_code)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, retry_interval)) < 0) goto fail;
    if ((rc = d21_write_span(w, reason)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_request_error(const uint8_t *payload,
                                          size_t payload_len,
                                          moq_d21_request_error_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->error_code);
    if (rc < 0) return rc;
    rc = moq_buf_read_vi64(&r, &out->retry_interval);
    if (rc < 0) return rc;
    rc = d21_read_span(&r, &out->reason);
    if (rc < 0) return rc;
    if (out->reason.len > MOQ_D21_MAX_REASON) return MOQ_ERR_PROTO;
    /* The base decoder does not parse a Redirect tail; reject leftover bytes
     * rather than silently accepting them. A REDIRECT code always has one (9.4.2),
     * so it must go through the redirect-aware decoder. */
    if (out->error_code == MOQ_D21_ERROR_REDIRECT) return MOQ_ERR_PROTO;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- Redirect structure (draft-18 §10.6.1) ------------------------- */

/* Read a Redirect from an in-progress reader (no trailing-byte check). */
static moq_result_t d21_read_redirect(moq_buf_reader_t *r,
                                      moq_bytes_t *parts, size_t max_parts,
                                      moq_d21_redirect_t *out)
{
    memset(out, 0, sizeof(*out));
    moq_result_t rc = d21_read_span(r, &out->connect_uri);
    if (rc < 0) return rc;
    rc = d21_read_namespace(r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    return d21_read_span(r, &out->track_name);
}

static moq_result_t d21_write_redirect(moq_buf_writer_t *w,
                                       const moq_d21_redirect_t *redirect)
{
    moq_result_t rc = d21_write_span(w, redirect->connect_uri);
    if (rc < 0) return rc;
    rc = d21_write_namespace(w, &redirect->track_namespace);
    if (rc < 0) return rc;
    return d21_write_span(w, redirect->track_name);
}

moq_result_t moq_d21_encode_redirect(moq_buf_writer_t *w,
                                     const moq_d21_redirect_t *redirect)
{
    if (!w || !redirect) return MOQ_ERR_INVAL;
    size_t saved = w->pos;
    moq_result_t rc = d21_write_redirect(w, redirect);
    if (rc < 0) w->pos = saved;   /* transactional: no partial output */
    return rc;
}

moq_result_t moq_d21_decode_redirect(const uint8_t *payload, size_t payload_len,
                                     moq_bytes_t *parts, size_t max_parts,
                                     moq_d21_redirect_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = d21_read_redirect(&r, parts, max_parts, out);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

moq_result_t moq_d21_encode_request_error_redirect(
    moq_buf_writer_t *w, uint64_t error_code, uint64_t retry_interval,
    moq_bytes_t reason, const moq_d21_redirect_t *redirect)
{
    if (!w || !redirect) return MOQ_ERR_INVAL;
    /* The Redirect tail is present only for the REDIRECT error code (§10.6.1). */
    if (error_code != MOQ_D21_ERROR_REDIRECT) return MOQ_ERR_INVAL;
    if (reason.len > MOQ_D21_MAX_REASON) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_REQUEST_ERROR, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, error_code)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, retry_interval)) < 0) goto fail;
    if ((rc = d21_write_span(w, reason)) < 0) goto fail;
    if ((rc = d21_write_redirect(w, redirect)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_request_error_redirect(
    const uint8_t *payload, size_t payload_len,
    moq_bytes_t *parts, size_t max_parts,
    moq_d21_request_error_t *out_err, moq_d21_redirect_t *out_redirect)
{
    if (!payload || !parts || !out_err || !out_redirect) return MOQ_ERR_INVAL;
    memset(out_redirect, 0, sizeof(*out_redirect));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out_err->error_code);
    if (rc < 0) return rc;
    rc = moq_buf_read_vi64(&r, &out_err->retry_interval);
    if (rc < 0) return rc;
    rc = d21_read_span(&r, &out_err->reason);
    if (rc < 0) return rc;
    if (out_err->reason.len > MOQ_D21_MAX_REASON) return MOQ_ERR_PROTO;
    if (out_err->error_code == MOQ_D21_ERROR_REDIRECT) {
        rc = d21_read_redirect(&r, parts, max_parts, out_redirect);
        if (rc < 0) return rc;
    }
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- Message Parameters (draft-18 §10.2) --------------------------- */

static moq_result_t d21_write_u8(moq_buf_writer_t *w, uint8_t v)
{
    return moq_buf_write_raw(w, &v, 1);
}

static moq_result_t d21_read_u8(moq_buf_reader_t *r, uint8_t *out)
{
    if (moq_buf_reader_remaining(r) < 1) return MOQ_ERR_BUFFER;
    *out = *moq_buf_reader_ptr(r);
    r->pos += 1;
    return MOQ_OK;
}

/* Location Filter (draft-21 9.20.10): a vi64 byte Length, then 0 to 4 optional
 * vi64 fields in the order StartGroup, StartObject, EndGroupDelta, EndObject. */
static moq_result_t d21_write_location_filter(moq_buf_writer_t *w,
                                              const moq_d21_location_filter_t *f)
{
    if (f->field_count > 4) return MOQ_ERR_INVAL;
    if (f->field_count >= 3 && f->end_group_delta > UINT64_MAX - f->start_group)
        return MOQ_ERR_INVAL;   /* StartGroup + EndGroupDelta must fit 2^64-1 */
    const uint64_t fields[4] = { f->start_group, f->start_object,
                                 f->end_group_delta, f->end_object };
    uint64_t blen = 0;
    for (uint8_t i = 0; i < f->field_count; i++) blen += moq_vi64_len(fields[i]);
    moq_result_t rc = moq_buf_write_vi64(w, blen);
    for (uint8_t i = 0; rc >= 0 && i < f->field_count; i++)
        rc = moq_buf_write_vi64(w, fields[i]);
    return rc;
}

/* `trunc_rc` is what a value that runs past its buffer reports: MOQ_ERR_BUFFER
 * at the message level, MOQ_ERR_PROTO inside a bounded nested block. A length
 * that does not divide into at most four whole vi64 fields is malformed. */
static moq_result_t d21_read_location_filter(moq_buf_reader_t *r,
                                             moq_d21_location_filter_t *out,
                                             moq_result_t trunc_rc)
{
    moq_bytes_t span;
    if (d21_read_span(r, &span) < 0) return trunc_rc;
    moq_buf_reader_t fr;
    moq_buf_reader_init(&fr, span.data, span.len);
    uint64_t v[4];
    uint8_t n = 0;
    while (moq_buf_reader_remaining(&fr) > 0) {
        if (n == 4) return MOQ_ERR_PROTO;                 /* a fifth field */
        if (moq_buf_read_vi64(&fr, &v[n]) < 0) return MOQ_ERR_PROTO;
        n++;
    }
    /* "StartGroup + EndGroupDelta" above 2^64-1 closes the session (9.20.10). */
    if (n >= 3 && v[2] > UINT64_MAX - v[0]) return MOQ_ERR_PROTO;
    memset(out, 0, sizeof(*out));
    out->field_count = n;
    if (n >= 1) out->start_group = v[0];
    if (n >= 2) out->start_object = v[1];
    if (n >= 3) out->end_group_delta = v[2];
    if (n >= 4) out->end_object = v[3];
    return MOQ_OK;
}

/* A Range Filter parameter (9.20.11 to 9.20.15, 8.6): vi64 Length, then
 * SetID (8), a Property Type (vi64, Object and Track Property filters only), and
 * a run of vi64 Range values (Start, End, Start, End, ... with the last End
 * optional). Parsed only to find the end, count the Ranges, and flag the values
 * the draft answers with INVALID_FILTER; nothing is retained. */
static moq_result_t d21_read_range_filter(moq_buf_reader_t *r, uint64_t type,
                                          moq_result_t trunc_rc,
                                          uint32_t *params, uint64_t *ranges,
                                          bool *invalid)
{
    moq_bytes_t span;
    if (d21_read_span(r, &span) < 0) return trunc_rc;
    (*params)++;
    if (span.len == 0) return MOQ_OK;                     /* "no filter" */
    moq_buf_reader_t fr;
    moq_buf_reader_init(&fr, span.data, span.len);
    uint8_t set_id;
    if (d21_read_u8(&fr, &set_id) < 0) return MOQ_ERR_PROTO;
    (void)set_id;
    if (type == MOQ_D21_PARAM_OBJECT_PROPERTY_FILTER ||
        type == MOQ_D21_PARAM_TRACK_PROPERTY_FILTER) {
        uint64_t property_type;
        if (moq_buf_read_vi64(&fr, &property_type) < 0) return MOQ_ERR_PROTO;
        if (property_type & 1u) *invalid = true;          /* must be even (9.20.14) */
    }
    uint64_t position = 0, values = 0;
    while (moq_buf_reader_remaining(&fr) > 0) {
        uint64_t v;
        if (moq_buf_read_vi64(&fr, &v) < 0) return MOQ_ERR_PROTO;
        if (v > UINT64_MAX - position) {                  /* delta overflow (8.6) */
            *invalid = true;
            position = UINT64_MAX;
        } else {
            position += v;
        }
        if (type == MOQ_D21_PARAM_PRIORITY_FILTER && position > 255u)
            *invalid = true;                              /* Publisher Priority is 8 bits */
        values++;
    }
    *ranges += (values + 1) / 2;
    return MOQ_OK;
}

/* AUTHORIZATION_TOKEN (§10.2.2) Token structure, written as the length-prefixed
 * value of the parameter. Integers are vi64; the Token Value (REGISTER /
 * USE_VALUE) runs to the end of the structure. The value can be arbitrarily
 * large, so the span length is computed up front rather than staged. */
static moq_result_t d21_write_auth_token(moq_buf_writer_t *w,
                                         const moq_d21_auth_token_t *t)
{
    if (t->alias_type > MOQ_AUTH_TOKEN_USE_VALUE) return MOQ_ERR_INVAL;
    uint64_t blen = moq_vi64_len(t->alias_type);
    switch (t->alias_type) {
    case MOQ_AUTH_TOKEN_DELETE:
    case MOQ_AUTH_TOKEN_USE_ALIAS:
        blen += moq_vi64_len(t->alias);
        break;
    case MOQ_AUTH_TOKEN_REGISTER:
        blen += moq_vi64_len(t->alias) + moq_vi64_len(t->token_type)
                + t->token_value.len;
        break;
    case MOQ_AUTH_TOKEN_USE_VALUE:
        blen += moq_vi64_len(t->token_type) + t->token_value.len;
        break;
    }
    moq_result_t rc = moq_buf_write_vi64(w, blen);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, t->alias_type)) < 0) return rc;
    switch (t->alias_type) {
    case MOQ_AUTH_TOKEN_DELETE:
    case MOQ_AUTH_TOKEN_USE_ALIAS:
        rc = moq_buf_write_vi64(w, t->alias);
        break;
    case MOQ_AUTH_TOKEN_REGISTER:
        if ((rc = moq_buf_write_vi64(w, t->alias)) < 0) break;
        if ((rc = moq_buf_write_vi64(w, t->token_type)) < 0) break;
        if (t->token_value.len > 0)
            rc = moq_buf_write_raw(w, t->token_value.data, t->token_value.len);
        break;
    case MOQ_AUTH_TOKEN_USE_VALUE:
        if ((rc = moq_buf_write_vi64(w, t->token_type)) < 0) break;
        if (t->token_value.len > 0)
            rc = moq_buf_write_raw(w, t->token_value.data, t->token_value.len);
        break;
    }
    return rc;
}

/* Read one AUTHORIZATION_TOKEN Token structure from the parameter's
 * length-prefixed value. A structure that cannot be decoded (bad alias type,
 * truncated field, declared length wrong, or trailing bytes) returns
 * MOQ_D21_ERR_KVP_FORMAT so the profile closes with KEY_VALUE_FORMATTING_ERROR
 * (§10.2.2) rather than the generic PROTOCOL_VIOLATION. token_value borrows from
 * the span. */
static moq_result_t d21_read_auth_token(moq_buf_reader_t *r,
                                        moq_d21_auth_token_t *out)
{
    moq_bytes_t span;
    if (d21_read_span(r, &span) < 0) return MOQ_D21_ERR_KVP_FORMAT;
    moq_buf_reader_t tr;
    moq_buf_reader_init(&tr, span.data, span.len);
    uint64_t at;
    if (moq_buf_read_vi64(&tr, &at) < 0) return MOQ_D21_ERR_KVP_FORMAT;
    if (at > MOQ_AUTH_TOKEN_USE_VALUE) return MOQ_D21_ERR_KVP_FORMAT;
    memset(out, 0, sizeof(*out));
    out->alias_type = (uint32_t)at;
    switch (at) {
    case MOQ_AUTH_TOKEN_DELETE:
    case MOQ_AUTH_TOKEN_USE_ALIAS:
        if (moq_buf_read_vi64(&tr, &out->alias) < 0)
            return MOQ_D21_ERR_KVP_FORMAT;
        break;
    case MOQ_AUTH_TOKEN_REGISTER:
        if (moq_buf_read_vi64(&tr, &out->alias) < 0)
            return MOQ_D21_ERR_KVP_FORMAT;
        if (moq_buf_read_vi64(&tr, &out->token_type) < 0)
            return MOQ_D21_ERR_KVP_FORMAT;
        out->token_value.data = moq_buf_reader_ptr(&tr);
        out->token_value.len = moq_buf_reader_remaining(&tr);
        tr.pos = tr.len;
        break;
    case MOQ_AUTH_TOKEN_USE_VALUE:
        if (moq_buf_read_vi64(&tr, &out->token_type) < 0)
            return MOQ_D21_ERR_KVP_FORMAT;
        out->token_value.data = moq_buf_reader_ptr(&tr);
        out->token_value.len = moq_buf_reader_remaining(&tr);
        tr.pos = tr.len;
        break;
    }
    if (moq_buf_reader_remaining(&tr) != 0) return MOQ_D21_ERR_KVP_FORMAT;
    return MOQ_OK;
}

/* -- SETUP Options (§10.3.1) ---------------------------------------- *
 * vi64 Key-Value-Pairs (§1.4.3) spanning the SETUP payload — NOT the draft-16
 * QUIC-varint KVP form (the two encodings diverge at values 64..127, the same
 * trap as the data-plane property work). Unknown options are self-describing and
 * skipped per §10.3 (duplicates of unknown options allowed); a duplicate known
 * non-repeatable option closes; AUTHORIZATION_TOKEN may repeat. */

moq_result_t moq_d21_decode_setup_opts(const uint8_t *payload, size_t len,
                                       moq_d21_setup_opts_t *out)
{
    if (!out || (len > 0 && !payload)) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, len);
    uint64_t prev_type = 0;
    bool first = true;

    while (moq_buf_reader_remaining(&r) > 0) {
        uint64_t delta;
        if (moq_buf_read_vi64(&r, &delta) < 0) return MOQ_ERR_PROTO;
        uint64_t type;
        if (first) {
            type = delta;
            first = false;
        } else {
            if (delta > UINT64_MAX - prev_type) return MOQ_ERR_PROTO;
            type = prev_type + delta;
        }
        prev_type = type;

        if (type == MOQ_D21_SETUP_OPT_AUTHORIZATION_TOKEN) {
            /* Odd type: d21_read_auth_token consumes the vi64 Length + token
             * structure directly. May repeat (§10.3.1.4). */
            if (out->auth_token_count >= MOQ_D21_MAX_AUTH_TOKENS)
                return MOQ_ERR_PROTO;
            moq_result_t arc = d21_read_auth_token(
                &r, &out->auth_tokens[out->auth_token_count]);
            if (arc < 0) return arc;       /* may be MOQ_D21_ERR_KVP_FORMAT */
            out->auth_token_count++;
            continue;
        }

        if ((type & 1u) == 0) {
            /* Even type: single vi64 value. */
            uint64_t v;
            if (moq_buf_read_vi64(&r, &v) < 0) return MOQ_ERR_PROTO;
            if (type == MOQ_D21_SETUP_OPT_MAX_AUTH_TOKEN_CACHE_SIZE) {
                if (out->has_max_auth_token_cache_size)
                    return MOQ_ERR_PROTO;  /* duplicate non-repeatable option */
                out->has_max_auth_token_cache_size = true;
                out->max_auth_token_cache_size = v;
            } else if (type == MOQ_D21_SETUP_OPT_MAX_FILTER_RANGES) {
                if (out->has_max_filter_ranges) return MOQ_ERR_PROTO;
                out->has_max_filter_ranges = true;
                out->max_filter_ranges = v;
            } else if (type == MOQ_D21_SETUP_OPT_MAX_REQUEST_UPDATES) {
                if (out->has_max_request_updates) return MOQ_ERR_PROTO;
                out->has_max_request_updates = true;
                out->max_request_updates = v;
            }
            /* Unknown even option: value already consumed; ignore. */
            continue;
        }

        /* Odd type: vi64 Length + bytes (§1.4.3: length above 2^16-1 closes). */
        uint64_t vlen;
        if (moq_buf_read_vi64(&r, &vlen) < 0) return MOQ_ERR_PROTO;
        if (vlen > 0xFFFFu) return MOQ_ERR_PROTO;
        if (vlen > moq_buf_reader_remaining(&r)) return MOQ_ERR_PROTO;
        if (type == MOQ_D21_SETUP_OPT_PATH) {
            if (out->has_path) return MOQ_ERR_PROTO;   /* non-repeatable */
            out->has_path = true;
        } else if (type == MOQ_D21_SETUP_OPT_AUTHORITY) {
            if (out->has_authority) return MOQ_ERR_PROTO;
            out->has_authority = true;
        } else if (type == MOQ_D21_SETUP_OPT_MOQT_IMPLEMENTATION) {
            if (out->has_implementation) return MOQ_ERR_PROTO;
            out->has_implementation = true;
            out->implementation.data = r.data + r.pos;
            out->implementation.len = (size_t)vlen;
        }
        /* Unknown odd option: skip its value; duplicates allowed. */
        r.pos += (size_t)vlen;
    }
    return MOQ_OK;
}

moq_result_t moq_d21_encode_setup_opts(moq_buf_writer_t *w,
                                       const moq_d21_setup_opts_t *opts)
{
    if (!w) return MOQ_ERR_INVAL;
    /* PATH, AUTHORITY and tokens are not sourced yet; refuse rather than
     * silently drop a requested option. */
    if (opts && (opts->has_path || opts->has_authority ||
                 opts->auth_token_count > 0))
        return MOQ_ERR_INVAL;
    if (opts && opts->has_implementation &&
        (opts->implementation.len > 0xFFFFu ||
         (opts->implementation.len > 0 && !opts->implementation.data)))
        return MOQ_ERR_INVAL;

    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_STREAM_SETUP, &len_off);
    if (rc < 0) { w->pos = saved; return rc; }
    /* Ascending type order; each Delta Type is relative to the previous option
     * emitted (the first is the absolute type). Even types carry a vi64 value,
     * odd types a vi64 length plus bytes. */
    uint64_t prev = 0;
#define D21_SETUP_EMIT_TYPE(t) do { \
        if ((rc = moq_buf_write_vi64(w, (t) - prev)) < 0) goto fail; \
        prev = (t); } while (0)
    if (opts && opts->has_max_auth_token_cache_size) {
        D21_SETUP_EMIT_TYPE(MOQ_D21_SETUP_OPT_MAX_AUTH_TOKEN_CACHE_SIZE);
        if ((rc = moq_buf_write_vi64(w, opts->max_auth_token_cache_size)) < 0)
            goto fail;
    }
    if (opts && opts->has_max_filter_ranges) {
        D21_SETUP_EMIT_TYPE(MOQ_D21_SETUP_OPT_MAX_FILTER_RANGES);
        if ((rc = moq_buf_write_vi64(w, opts->max_filter_ranges)) < 0)
            goto fail;
    }
    if (opts && opts->has_implementation) {
        D21_SETUP_EMIT_TYPE(MOQ_D21_SETUP_OPT_MOQT_IMPLEMENTATION);
        if ((rc = moq_buf_write_vi64(w, opts->implementation.len)) < 0)
            goto fail;
        if (opts->implementation.len > 0 &&
            (rc = moq_buf_write_raw(w, opts->implementation.data,
                                    opts->implementation.len)) < 0)
            goto fail;
    }
    if (opts && opts->has_max_request_updates) {
        D21_SETUP_EMIT_TYPE(MOQ_D21_SETUP_OPT_MAX_REQUEST_UPDATES);
        if ((rc = moq_buf_write_vi64(w, opts->max_request_updates)) < 0)
            goto fail;
    }
#undef D21_SETUP_EMIT_TYPE
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

static uint64_t d21_param_count(const moq_d21_msg_params_t *p)
{
    return (p->has_object_delivery_timeout ? 1u : 0u) +
           (uint64_t)p->auth_token_count +
           (p->has_rendezvous_timeout ? 1u : 0u) +
           (p->has_subgroup_delivery_timeout ? 1u : 0u) +
           (p->has_expires ? 1u : 0u) +
           (p->has_largest ? 1u : 0u) +
           (p->has_fill_timeout ? 1u : 0u) +
           (p->has_forward ? 1u : 0u) +
           (p->has_subscriber_priority ? 1u : 0u) +
           (p->has_location_filter ? 1u : 0u) +
           (p->has_group_order ? 1u : 0u) +
           (p->has_fill ? 1u : 0u) +
           (p->has_new_group_request ? 1u : 0u) +
           (p->has_include_properties ? 1u : 0u);
}

/* The value ranges the wire can carry for the enumerated parameters. */
static bool d21_params_values_ok(const moq_d21_msg_params_t *p)
{
    if (p->has_forward && p->forward > 1) return false;
    if (p->has_group_order && (p->group_order < 1 || p->group_order > 2))
        return false;
    if (p->has_include_properties && p->include_properties > 1) return false;
    if (p->has_location_filter && p->location_filter.field_count > 4)
        return false;
    if (p->auth_token_count > MOQ_D21_MAX_AUTH_TOKENS) return false;
    return true;
}

/* Write the parameters (no count) in ascending Type-Delta order. Each delta is
 * the type minus the previous type (the type itself for the first parameter, and
 * zero for a repeated AUTHORIZATION_TOKEN). `fill_scope` is true for the body of
 * FILL_PARAMETERS, where a FILL_PARAMETERS may not nest. */
static moq_result_t d21_write_params_body(moq_buf_writer_t *w,
                                          const moq_d21_msg_params_t *p)
{
    uint64_t prev = 0;
    moq_result_t rc = MOQ_OK;
#define D21_PARAM_TYPE(t) do { \
        if ((rc = moq_buf_write_vi64(w, (t) - prev)) < 0) return rc; \
        prev = (t); } while (0)
    if (p->has_object_delivery_timeout) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_OBJECT_DELIVERY_TIMEOUT);
        if ((rc = moq_buf_write_vi64(w, p->object_delivery_timeout_ms)) < 0)
            return rc;
    }
    for (size_t i = 0; i < p->auth_token_count; i++) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_AUTHORIZATION_TOKEN);
        if ((rc = d21_write_auth_token(w, &p->auth_tokens[i])) < 0) return rc;
    }
    if (p->has_rendezvous_timeout) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_RENDEZVOUS_TIMEOUT);
        if ((rc = moq_buf_write_vi64(w, p->rendezvous_timeout_ms)) < 0) return rc;
    }
    if (p->has_subgroup_delivery_timeout) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_SUBGROUP_DELIVERY_TIMEOUT);
        if ((rc = moq_buf_write_vi64(w, p->subgroup_delivery_timeout_ms)) < 0)
            return rc;
    }
    if (p->has_expires) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_EXPIRES);
        if ((rc = moq_buf_write_vi64(w, p->expires_ms)) < 0) return rc;
    }
    if (p->has_largest) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_LARGEST_OBJECT);
        if ((rc = moq_buf_write_vi64(w, p->largest_group)) < 0) return rc;
        if ((rc = moq_buf_write_vi64(w, p->largest_object)) < 0) return rc;
    }
    if (p->has_fill_timeout) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_FILL_TIMEOUT);
        if ((rc = moq_buf_write_vi64(w, p->fill_timeout_ms)) < 0) return rc;
    }
    if (p->has_forward) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_FORWARD);
        if ((rc = d21_write_u8(w, p->forward)) < 0) return rc;
    }
    if (p->has_subscriber_priority) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_SUBSCRIBER_PRIORITY);
        if ((rc = d21_write_u8(w, p->subscriber_priority)) < 0) return rc;
    }
    if (p->has_location_filter) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_LOCATION_FILTER);
        if ((rc = d21_write_location_filter(w, &p->location_filter)) < 0)
            return rc;
    }
    if (p->has_group_order) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_GROUP_ORDER);
        if ((rc = d21_write_u8(w, p->group_order)) < 0) return rc;
    }
    if (p->has_fill) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_FILL_PARAMETERS);
        /* The nested block is a plain parameter sequence with no count (open
         * ambiguity A1): stage it to learn its byte length, then length-prefix
         * it. A fill block is a handful of small parameters, so it fits. */
        const moq_d21_fill_params_t *f = &p->fill;
        if (f->range_filter_params != 0) return MOQ_ERR_INVAL;   /* never emitted */
        moq_d21_msg_params_t inner;
        memset(&inner, 0, sizeof(inner));
        inner.has_fill_timeout = f->has_fill_timeout;
        inner.fill_timeout_ms = f->fill_timeout_ms;
        inner.has_subscriber_priority = f->has_subscriber_priority;
        inner.subscriber_priority = f->subscriber_priority;
        inner.has_group_order = f->has_group_order;
        inner.group_order = f->group_order;
        inner.has_location_filter = f->has_location_filter;
        inner.location_filter = f->location_filter;
        if (!d21_params_values_ok(&inner)) return MOQ_ERR_INVAL;
        uint8_t staged[96];
        moq_buf_writer_t sw;
        moq_buf_writer_init(&sw, staged, sizeof(staged));
        if ((rc = d21_write_params_body(&sw, &inner)) < 0) return rc;
        moq_bytes_t span = { staged, moq_buf_writer_offset(&sw) };
        if ((rc = d21_write_span(w, span)) < 0) return rc;
    }
    if (p->has_new_group_request) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_NEW_GROUP_REQUEST);
        if ((rc = moq_buf_write_vi64(w, p->new_group_request)) < 0) return rc;
    }
    if (p->has_include_properties) {
        D21_PARAM_TYPE(MOQ_D21_PARAM_INCLUDE_PROPERTIES);
        if ((rc = d21_write_u8(w, p->include_properties)) < 0) return rc;
    }
#undef D21_PARAM_TYPE
    return MOQ_OK;
}

moq_result_t moq_d21_encode_msg_params(moq_buf_writer_t *w,
                                       const moq_d21_msg_params_t *p)
{
    if (!w || !p) return MOQ_ERR_INVAL;
    if (!d21_params_values_ok(p)) return MOQ_ERR_INVAL;
    /* This codec never emits Range Filters (the implementation does not use
     * them) or a Track Namespace Prefix update; refuse rather than drop them. */
    if (p->range_filter_params != 0 || p->has_track_namespace_prefix)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, d21_param_count(p));
    if (rc < 0) return rc;
    if ((rc = d21_write_params_body(w, p)) < 0) {
        w->pos = saved;
        return rc;
    }
    return MOQ_OK;
}

/* A Parameter Type that may repeat in one message: AUTHORIZATION_TOKEN (8.9) and
 * the Range Filters, which the draft lets a message carry more than once with
 * distinct keys (3.3.2). Any other repeat is a protocol violation (9.20). */
static bool d21_type_may_repeat(uint64_t type, uint32_t mask)
{
    switch (type) {
    case MOQ_D21_PARAM_AUTHORIZATION_TOKEN:
        return (mask & MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN) != 0;
    case MOQ_D21_PARAM_SUBGROUP_FILTER:
        return (mask & MOQ_D21_PARAM_BIT_SUBGROUP_FILTER) != 0;
    case MOQ_D21_PARAM_OBJECTID_FILTER:
        return (mask & MOQ_D21_PARAM_BIT_OBJECTID_FILTER) != 0;
    case MOQ_D21_PARAM_PRIORITY_FILTER:
        return (mask & MOQ_D21_PARAM_BIT_PRIORITY_FILTER) != 0;
    case MOQ_D21_PARAM_OBJECT_PROPERTY_FILTER:
        return (mask & MOQ_D21_PARAM_BIT_OBJECT_PROPERTY_FILTER) != 0;
    case MOQ_D21_PARAM_TRACK_PROPERTY_FILTER:
        return (mask & MOQ_D21_PARAM_BIT_TRACK_PROPERTY_FILTER) != 0;
    default:
        return false;
    }
}

/* Decode a parameter sequence. With `until_end` the sequence runs to the end of
 * the reader (a nested FILL_PARAMETERS block, which has no count); otherwise
 * `count` parameters are read. `out` must already be zeroed. */
static moq_result_t d21_decode_params_inner(moq_buf_reader_t *r, uint64_t count,
                                            bool until_end, uint32_t allowed_mask,
                                            moq_result_t trunc_rc,
                                            moq_d21_msg_params_t *out)
{
    uint64_t prev = 0;
    for (uint64_t i = 0; until_end ? moq_buf_reader_remaining(r) > 0 : i < count;
         i++) {
        uint64_t delta;
        if (moq_buf_read_vi64(r, &delta) < 0) return trunc_rc;
        uint64_t type;
        if (delta == 0) {
            if (!d21_type_may_repeat(prev, allowed_mask)) return MOQ_ERR_PROTO;
            type = prev;
        } else {
            if (delta > UINT64_MAX - prev) return MOQ_ERR_PROTO;
            type = prev + delta;
        }
        prev = type;
#define D21_REQUIRE(bit) do { if (!(allowed_mask & (bit))) return MOQ_ERR_PROTO; } while (0)
#define D21_READ_VI64(dst) do { if (moq_buf_read_vi64(r, &(dst)) < 0) return trunc_rc; } while (0)
        switch (type) {
        case MOQ_D21_PARAM_OBJECT_DELIVERY_TIMEOUT:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT);
            D21_READ_VI64(out->object_delivery_timeout_ms);
            out->has_object_delivery_timeout = true;
            break;
        case MOQ_D21_PARAM_AUTHORIZATION_TOKEN: {
            D21_REQUIRE(MOQ_D21_PARAM_BIT_AUTHORIZATION_TOKEN);
            if (out->auth_token_count >= MOQ_D21_MAX_AUTH_TOKENS)
                return MOQ_ERR_PROTO;       /* too many auth tokens */
            moq_result_t arc = d21_read_auth_token(
                r, &out->auth_tokens[out->auth_token_count]);
            if (arc < 0) return arc;        /* may be MOQ_D21_ERR_KVP_FORMAT */
            out->auth_token_count++;
            break;
        }
        case MOQ_D21_PARAM_RENDEZVOUS_TIMEOUT:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_RENDEZVOUS_TIMEOUT);
            D21_READ_VI64(out->rendezvous_timeout_ms);
            out->has_rendezvous_timeout = true;
            break;
        case MOQ_D21_PARAM_SUBGROUP_DELIVERY_TIMEOUT:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT);
            D21_READ_VI64(out->subgroup_delivery_timeout_ms);
            out->has_subgroup_delivery_timeout = true;
            break;
        case MOQ_D21_PARAM_EXPIRES:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_EXPIRES);
            D21_READ_VI64(out->expires_ms);
            out->has_expires = true;
            break;
        case MOQ_D21_PARAM_LARGEST_OBJECT:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_LARGEST_OBJECT);
            D21_READ_VI64(out->largest_group);
            D21_READ_VI64(out->largest_object);
            out->has_largest = true;
            break;
        case MOQ_D21_PARAM_FILL_TIMEOUT:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_FILL_TIMEOUT);
            D21_READ_VI64(out->fill_timeout_ms);
            out->has_fill_timeout = true;
            break;
        case MOQ_D21_PARAM_FORWARD: {
            D21_REQUIRE(MOQ_D21_PARAM_BIT_FORWARD);
            uint8_t v;
            if (d21_read_u8(r, &v) < 0) return trunc_rc;
            if (v > 1) return MOQ_ERR_PROTO;       /* 9.20.19: only 0 or 1 */
            out->has_forward = true;
            out->forward = v;
            break;
        }
        case MOQ_D21_PARAM_SUBSCRIBER_PRIORITY: {
            D21_REQUIRE(MOQ_D21_PARAM_BIT_SUBSCRIBER_PRIORITY);
            uint8_t v;
            if (d21_read_u8(r, &v) < 0) return trunc_rc;
            out->has_subscriber_priority = true;
            out->subscriber_priority = v;
            break;
        }
        case MOQ_D21_PARAM_LOCATION_FILTER: {
            D21_REQUIRE(MOQ_D21_PARAM_BIT_LOCATION_FILTER);
            moq_result_t frc = d21_read_location_filter(r, &out->location_filter,
                                                        trunc_rc);
            if (frc < 0) return frc;
            out->has_location_filter = true;
            break;
        }
        case MOQ_D21_PARAM_GROUP_ORDER: {
            D21_REQUIRE(MOQ_D21_PARAM_BIT_GROUP_ORDER);
            uint8_t v;
            if (d21_read_u8(r, &v) < 0) return trunc_rc;
            if (v < 1 || v > 2) return MOQ_ERR_PROTO;   /* 9.20.9: 1 or 2 */
            out->has_group_order = true;
            out->group_order = v;
            break;
        }
        case MOQ_D21_PARAM_FILL_PARAMETERS: {
            D21_REQUIRE(MOQ_D21_PARAM_BIT_FILL_PARAMETERS);
            moq_bytes_t span;
            if (d21_read_span(r, &span) < 0) return trunc_rc;
            moq_buf_reader_t fr;
            moq_buf_reader_init(&fr, span.data, span.len);
            moq_d21_msg_params_t inner;
            memset(&inner, 0, sizeof(inner));
            moq_result_t frc = d21_decode_params_inner(
                &fr, 0, true, MOQ_D21_MASK_FILL_NESTED, MOQ_ERR_PROTO, &inner);
            if (frc < 0) return frc;
            moq_d21_fill_params_t *f = &out->fill;
            memset(f, 0, sizeof(*f));
            f->has_fill_timeout = inner.has_fill_timeout;
            f->fill_timeout_ms = inner.fill_timeout_ms;
            f->has_subscriber_priority = inner.has_subscriber_priority;
            f->subscriber_priority = inner.subscriber_priority;
            f->has_group_order = inner.has_group_order;
            f->group_order = inner.group_order;
            f->has_location_filter = inner.has_location_filter;
            f->location_filter = inner.location_filter;
            f->range_filter_params = inner.range_filter_params;
            f->range_filter_ranges = inner.range_filter_ranges;
            f->range_filter_invalid = inner.range_filter_invalid;
            out->has_fill = true;
            break;
        }
        case MOQ_D21_PARAM_SUBGROUP_FILTER:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_SUBGROUP_FILTER); goto range_filter;
        case MOQ_D21_PARAM_OBJECTID_FILTER:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_OBJECTID_FILTER); goto range_filter;
        case MOQ_D21_PARAM_PRIORITY_FILTER:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_PRIORITY_FILTER); goto range_filter;
        case MOQ_D21_PARAM_OBJECT_PROPERTY_FILTER:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_OBJECT_PROPERTY_FILTER); goto range_filter;
        case MOQ_D21_PARAM_TRACK_PROPERTY_FILTER:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_TRACK_PROPERTY_FILTER);
        range_filter: {
            moq_result_t frc = d21_read_range_filter(
                r, type, trunc_rc, &out->range_filter_params,
                &out->range_filter_ranges, &out->range_filter_invalid);
            if (frc < 0) return frc;
            break;
        }
        case MOQ_D21_PARAM_NEW_GROUP_REQUEST:
            D21_REQUIRE(MOQ_D21_PARAM_BIT_NEW_GROUP_REQUEST);
            D21_READ_VI64(out->new_group_request);
            out->has_new_group_request = true;
            break;
        case MOQ_D21_PARAM_TRACK_NAMESPACE_PREFIX: {
            D21_REQUIRE(MOQ_D21_PARAM_BIT_TRACK_NAMESPACE_PREFIX);
            /* A Track Namespace with 0..32 fields (9.20.21); a zero-field root
             * prefix is legal. Validated and skipped, not surfaced. */
            uint64_t nparts;
            D21_READ_VI64(nparts);
            if (nparts > 32) return MOQ_ERR_PROTO;
            for (uint64_t pi = 0; pi < nparts; pi++) {
                uint64_t plen;
                D21_READ_VI64(plen);
                if (plen == 0) return MOQ_ERR_PROTO;
                if (plen > moq_buf_reader_remaining(r)) return trunc_rc;
                r->pos += (size_t)plen;
            }
            out->has_track_namespace_prefix = true;
            break;
        }
        case MOQ_D21_PARAM_INCLUDE_PROPERTIES: {
            D21_REQUIRE(MOQ_D21_PARAM_BIT_INCLUDE_PROPERTIES);
            uint8_t v;
            if (d21_read_u8(r, &v) < 0) return trunc_rc;
            if (v > 1) return MOQ_ERR_PROTO;       /* 9.20.22: only 0 or 1 */
            out->has_include_properties = true;
            out->include_properties = v;
            break;
        }
        default:
            /* Unknown parameters cannot be skipped (9.20). */
            return MOQ_ERR_PROTO;
        }
#undef D21_REQUIRE
#undef D21_READ_VI64
    }
    return MOQ_OK;
}

moq_result_t moq_d21_decode_msg_params(moq_buf_reader_t *r, uint64_t count,
                                       uint32_t allowed_mask,
                                       moq_d21_msg_params_t *out)
{
    if (!r || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    return d21_decode_params_inner(r, count, false, allowed_mask, MOQ_ERR_BUFFER,
                                   out);
}

/* -- REQUEST_OK (draft-21 9.3) ------------------------------------ */

static bool d21_request_ok_kind(moq_d21_request_ok_kind_t kind, uint32_t *mask,
                                bool *properties_allowed)
{
    switch (kind) {
    case MOQ_D21_REQUEST_OK_PUBLISH:
        *mask = MOQ_D21_MASK_PUBLISH_OK; *properties_allowed = false; return true;
    case MOQ_D21_REQUEST_OK_REQUEST_UPDATE:
        *mask = MOQ_D21_MASK_REQUEST_UPDATE_OK; *properties_allowed = false; return true;
    case MOQ_D21_REQUEST_OK_TRACK_STATUS:
        *mask = MOQ_D21_MASK_TRACK_STATUS_OK; *properties_allowed = true; return true;
    case MOQ_D21_REQUEST_OK_SUBSCRIBE_NAMESPACE:
    case MOQ_D21_REQUEST_OK_SUBSCRIBE_TRACKS:
    case MOQ_D21_REQUEST_OK_PUBLISH_NAMESPACE:
        *mask = MOQ_D21_MASK_NAMESPACE_OK; *properties_allowed = false; return true;
    }
    return false;
}

moq_result_t moq_d21_encode_request_ok(moq_buf_writer_t *w,
                                       moq_d21_request_ok_kind_t kind,
                                       const moq_d21_msg_params_t *params,
                                       moq_bytes_t track_properties)
{
    uint32_t mask;
    bool props_ok;
    if (!w || !d21_request_ok_kind(kind, &mask, &props_ok)) return MOQ_ERR_INVAL;
    moq_d21_msg_params_t none;
    if (!params) { memset(&none, 0, sizeof(none)); params = &none; }
    if (!d21_params_within_mask(params, mask)) return MOQ_ERR_INVAL;
    if (track_properties.len > 0) {
        /* Track Properties are populated only for TRACK_STATUS_OK (9.3). */
        if (!props_ok || !track_properties.data) return MOQ_ERR_INVAL;
        if (d21_validate_track_properties(track_properties.data,
                                          track_properties.len) < 0)
            return MOQ_ERR_INVAL;
    }
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_REQUEST_OK, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_d21_encode_msg_params(w, params)) < 0) goto fail;
    if (track_properties.len > 0 &&
        (rc = moq_buf_write_raw(w, track_properties.data,
                                track_properties.len)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_request_ok(const uint8_t *payload, size_t payload_len,
                                       moq_d21_request_ok_kind_t kind,
                                       moq_d21_request_ok_t *out)
{
    uint32_t mask;
    bool props_ok;
    if (!payload || !out || !d21_request_ok_kind(kind, &mask, &props_ok))
        return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    uint64_t count;
    moq_result_t rc = moq_buf_read_vi64(&r, &count);
    if (rc < 0) return rc;
    if ((rc = moq_d21_decode_msg_params(&r, count, mask, &out->params)) < 0)
        return rc;
    out->track_properties.data = moq_buf_reader_ptr(&r);
    out->track_properties.len = moq_buf_reader_remaining(&r);
    if (out->track_properties.len == 0) return MOQ_OK;
    /* Track Properties in any form but TRACK_STATUS_OK close the session (9.3). */
    if (!props_ok) return MOQ_ERR_PROTO;
    /* TRACK_STATUS_OK is not among the messages for which an unknown Mandatory
     * Track Property is fatal (3.6 lists PUBLISH, SUBSCRIBE_OK and FETCH_OK), so
     * the structure is checked and the property is surfaced rather than rejected. */
    bool mandatory_ignored = false;
    return d21_scan_track_properties(out->track_properties.data,
                                     out->track_properties.len,
                                     &mandatory_ignored, NULL);
}

/* -- Code registries (draft-21 16.11) ------------------------------ */

bool moq_d21_request_error_registered(uint64_t code)
{
    switch (code) {
    case MOQ_D21_ERROR_INTERNAL_ERROR: case MOQ_D21_ERROR_UNAUTHORIZED:
    case MOQ_D21_ERROR_TIMEOUT: case MOQ_D21_ERROR_NOT_SUPPORTED:
    case MOQ_D21_ERROR_MALFORMED_AUTH_TOKEN: case MOQ_D21_ERROR_EXPIRED_AUTH_TOKEN:
    case MOQ_D21_ERROR_GOING_AWAY: case MOQ_D21_ERROR_EXCESSIVE_LOAD:
    case MOQ_D21_ERROR_DOES_NOT_EXIST: case MOQ_D21_ERROR_INVALID_RANGE:
    case MOQ_D21_ERROR_MALFORMED_TRACK: case MOQ_D21_ERROR_UNINTERESTED:
    case MOQ_D21_ERROR_PREFIX_OVERLAP: case MOQ_D21_ERROR_NAMESPACE_TOO_LARGE:
    case MOQ_D21_ERROR_UNSUPPORTED_EXTENSION: case MOQ_D21_ERROR_REDIRECT:
    case MOQ_D21_ERROR_CONFLICTING_FILTERS: case MOQ_D21_ERROR_INVALID_FILTER:
        return true;
    default:
        return false;
    }
}

bool moq_d21_publish_done_registered(uint64_t code)
{
    switch (code) {
    case MOQ_D21_PUBLISH_DONE_INTERNAL_ERROR: case MOQ_D21_PUBLISH_DONE_UNAUTHORIZED:
    case MOQ_D21_PUBLISH_DONE_TRACK_ENDED: case MOQ_D21_PUBLISH_DONE_GOING_AWAY:
    case MOQ_D21_PUBLISH_DONE_TOO_FAR_BEHIND: case MOQ_D21_PUBLISH_DONE_EXPIRED:
    case MOQ_D21_PUBLISH_DONE_UPDATE_FAILED: case MOQ_D21_PUBLISH_DONE_EXCESSIVE_LOAD:
    case MOQ_D21_PUBLISH_DONE_MALFORMED_TRACK:
        return true;
    default:
        return false;
    }
}

bool moq_d21_session_error_registered(uint64_t code)
{
    switch (code) {
    case MOQ_D21_SESSION_ERR_NO_ERROR: case MOQ_D21_SESSION_ERR_INTERNAL_ERROR:
    case MOQ_D21_SESSION_ERR_UNAUTHORIZED: case MOQ_D21_SESSION_ERR_PROTOCOL_VIOLATION:
    case MOQ_D21_SESSION_ERR_INVALID_REQUEST_ID: case MOQ_D21_SESSION_ERR_DUPLICATE_TRACK_ALIAS:
    case MOQ_D21_SESSION_ERR_KEY_VALUE_FORMATTING: case MOQ_D21_SESSION_ERR_INVALID_PATH:
    case MOQ_D21_SESSION_ERR_MALFORMED_PATH: case MOQ_D21_SESSION_ERR_GOAWAY_TIMEOUT:
    case MOQ_D21_SESSION_ERR_CONTROL_MESSAGE_TIMEOUT: case MOQ_D21_SESSION_ERR_DATA_STREAM_TIMEOUT:
    case MOQ_D21_SESSION_ERR_AUTH_TOKEN_CACHE_OVERFLOW:
    case MOQ_D21_SESSION_ERR_DUPLICATE_AUTH_TOKEN_ALIAS:
    case MOQ_D21_SESSION_ERR_MALFORMED_AUTH_TOKEN:
    case MOQ_D21_SESSION_ERR_UNKNOWN_AUTH_TOKEN_ALIAS:
    case MOQ_D21_SESSION_ERR_EXPIRED_AUTH_TOKEN: case MOQ_D21_SESSION_ERR_INVALID_AUTHORITY:
    case MOQ_D21_SESSION_ERR_MALFORMED_AUTHORITY:
    case MOQ_D21_SESSION_ERR_TOO_MANY_REQUEST_UPDATES:
        return true;
    default:
        return false;
    }
}

/* -- REQUEST_UPDATE (draft-18 §10.9) ------------------------------- */

moq_result_t moq_d21_encode_request_update(moq_buf_writer_t *w,
                                           uint64_t request_id,
                                           const moq_d21_msg_params_t *p)
{
    if (!w || !p) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(p, MOQ_D21_MASK_REQUEST_UPDATE))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_REQUEST_UPDATE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, p)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_request_update(const uint8_t *payload,
                                           size_t payload_len,
                                           moq_d21_request_update_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    /* A subscription REQUEST_UPDATE carries FORWARD / SUBSCRIBER_PRIORITY /
     * SUBSCRIPTION_FILTER and the delivery-timeout parameters. */
    if ((rc = moq_d21_decode_msg_params(&r, count,
            MOQ_D21_MASK_REQUEST_UPDATE, &out->params)) < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- REQUEST_OK (draft-18 §10.5) ----------------------------------- */

/* -- PUBLISH_DONE (draft-21 9.9) ----------------------------------- */

moq_result_t moq_d21_encode_publish_done(moq_buf_writer_t *w,
                                         uint64_t status_code,
                                         uint64_t stream_count,
                                         moq_bytes_t reason)
{
    if (!w) return MOQ_ERR_INVAL;
    if (reason.len > MOQ_D21_MAX_REASON) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_PUBLISH_DONE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, status_code)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, stream_count)) < 0) goto fail;
    if ((rc = d21_write_span(w, reason)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_publish_done(const uint8_t *payload,
                                         size_t payload_len,
                                         moq_d21_publish_done_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->status_code);
    if (rc < 0) return rc;
    rc = moq_buf_read_vi64(&r, &out->stream_count);
    if (rc < 0) return rc;
    rc = d21_read_span(&r, &out->reason);
    if (rc < 0) return rc;
    if (out->reason.len > MOQ_D21_MAX_REASON) return MOQ_ERR_PROTO;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- PUBLISH_STATE_NOTIFY (draft-21 9.10) ------------------------- */

moq_result_t moq_d21_encode_publish_state_notify(moq_buf_writer_t *w,
                                                 const moq_d21_msg_params_t *params)
{
    if (!w || !params) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(params, MOQ_D21_MASK_PUBLISH_STATE_NOTIFY))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_PUBLISH_STATE_NOTIFY, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_d21_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_publish_state_notify(
    const uint8_t *payload, size_t payload_len,
    moq_d21_publish_state_notify_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    uint64_t count;
    moq_result_t rc = moq_buf_read_vi64(&r, &count);
    if (rc < 0) return rc;
    if ((rc = moq_d21_decode_msg_params(&r, count, MOQ_D21_MASK_PUBLISH_STATE_NOTIFY,
                                        &out->params)) < 0)
        return rc;
    /* The body is the parameters and nothing more. */
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- SUBGROUP_HEADER (draft-18 §11.4.2) ---------------------------- */

bool moq_d21_subgroup_type_valid(uint8_t type)
{
    /* Form 0b0XX1XXXX: bit 7 clear, bit 4 set. */
    if ((type & 0x90u) != 0x10u) return false;
    /* SUBGROUP_ID_MODE 0b11 is reserved for future use. */
    if (((type & MOQ_D21_SUBGROUP_MASK_ID_MODE) >> 1) == 0x03u) return false;
    return true;
}

moq_result_t moq_d21_encode_subgroup_header(moq_buf_writer_t *w,
                                            const moq_d21_subgroup_header_t *hdr)
{
    if (!w || !hdr) return MOQ_ERR_INVAL;
    if (hdr->subgroup_id_mode > MOQ_SUBGROUP_ID_MODE_PRESENT)
        return MOQ_ERR_INVAL;

    uint8_t type = 0x10u;   /* bit 4 set */
    if (hdr->has_properties)  type |= MOQ_D21_SUBGROUP_BIT_PROPERTIES;
    type |= (uint8_t)((hdr->subgroup_id_mode & 0x03u) << 1);
    if (hdr->end_of_group)    type |= MOQ_D21_SUBGROUP_BIT_END_OF_GROUP;
    if (hdr->default_priority) type |= MOQ_D21_SUBGROUP_BIT_DEFAULT_PRIORITY;
    if (hdr->first_object)    type |= MOQ_D21_SUBGROUP_BIT_FIRST_OBJECT;

    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, type);   /* type < 0x80 -> 1 byte */
    if (rc < 0) return rc;
    rc = moq_buf_write_vi64(w, hdr->track_alias);
    if (rc < 0) { w->pos = saved; return rc; }
    rc = moq_buf_write_vi64(w, hdr->group_id);
    if (rc < 0) { w->pos = saved; return rc; }
    if (hdr->subgroup_id_mode == MOQ_SUBGROUP_ID_MODE_PRESENT) {
        rc = moq_buf_write_vi64(w, hdr->subgroup_id);
        if (rc < 0) { w->pos = saved; return rc; }
    }
    if (!hdr->default_priority) {
        rc = moq_buf_write_raw(w, &hdr->publisher_priority, 1);
        if (rc < 0) { w->pos = saved; return rc; }
    }
    return MOQ_OK;
}

moq_result_t moq_d21_decode_subgroup_header(moq_buf_reader_t *r,
                                            moq_d21_subgroup_header_t *out)
{
    if (!r || !out) return MOQ_ERR_INVAL;

    size_t saved = r->pos;
    uint64_t type_v = 0;
    moq_result_t rc = moq_buf_read_vi64(r, &type_v);
    if (rc < 0) return rc;
    if (type_v > 0xFF || !moq_d21_subgroup_type_valid((uint8_t)type_v)) {
        r->pos = saved;
        return MOQ_ERR_PROTO;
    }

    uint8_t type = (uint8_t)type_v;
    memset(out, 0, sizeof(*out));
    out->type             = type;
    out->has_properties   = (type & MOQ_D21_SUBGROUP_BIT_PROPERTIES) != 0;
    out->subgroup_id_mode = (uint8_t)((type & MOQ_D21_SUBGROUP_MASK_ID_MODE) >> 1);
    out->end_of_group     = (type & MOQ_D21_SUBGROUP_BIT_END_OF_GROUP) != 0;
    out->default_priority = (type & MOQ_D21_SUBGROUP_BIT_DEFAULT_PRIORITY) != 0;
    out->first_object     = (type & MOQ_D21_SUBGROUP_BIT_FIRST_OBJECT) != 0;

    rc = moq_buf_read_vi64(r, &out->track_alias);
    if (rc < 0) { r->pos = saved; return rc; }
    rc = moq_buf_read_vi64(r, &out->group_id);
    if (rc < 0) { r->pos = saved; return rc; }
    if (out->subgroup_id_mode == MOQ_SUBGROUP_ID_MODE_PRESENT) {
        rc = moq_buf_read_vi64(r, &out->subgroup_id);
        if (rc < 0) { r->pos = saved; return rc; }
    }
    if (!out->default_priority) {
        if (moq_buf_reader_remaining(r) < 1) { r->pos = saved; return MOQ_ERR_BUFFER; }
        out->publisher_priority = *moq_buf_reader_ptr(r);
        r->pos += 1;
    }
    return MOQ_OK;
}

/* -- FETCH family (draft-18 §10.12 / §10.13 / §11.4.4) ------------- */

static moq_result_t d21_write_location(moq_buf_writer_t *w,
                                       moq_d21_location_t loc)
{
    moq_result_t rc = moq_buf_write_vi64(w, loc.group);
    if (rc < 0) return rc;
    return moq_buf_write_vi64(w, loc.object);
}

static moq_result_t d21_read_location(moq_buf_reader_t *r,
                                      moq_d21_location_t *out)
{
    moq_result_t rc = moq_buf_read_vi64(r, &out->group);
    if (rc < 0) return rc;
    return moq_buf_read_vi64(r, &out->object);
}

moq_result_t moq_d21_encode_fetch(moq_buf_writer_t *w, const moq_d21_fetch_t *f)
{
    if (!w || !f) return MOQ_ERR_INVAL;
    if (!d21_params_within_mask(&f->params, MOQ_D21_MASK_FETCH))
        return MOQ_ERR_INVAL;
    if (d21_full_track_len(&f->track_namespace, f->track_name) >
        MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;

    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_FETCH, &len_off);
    if (rc < 0) { w->pos = saved; return rc; }
    if ((rc = moq_buf_write_vi64(w, f->request_id)) < 0) goto fail;
    if ((rc = d21_write_namespace(w, &f->track_namespace)) < 0) goto fail;
    if ((rc = d21_write_span(w, f->track_name)) < 0) goto fail;
    if ((rc = moq_d21_encode_msg_params(w, &f->params)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_fetch(const uint8_t *payload, size_t payload_len,
                                  moq_bytes_t *parts, size_t max_parts,
                                  moq_d21_fetch_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d21_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    if ((rc = d21_read_span(&r, &out->track_name)) < 0) return rc;
    if (d21_full_track_len(&out->track_namespace, out->track_name) >
        MOQ_D21_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;

    uint64_t param_count = 0;
    if ((rc = moq_buf_read_vi64(&r, &param_count)) < 0) return rc;
    rc = moq_d21_decode_msg_params(&r, param_count, MOQ_D21_MASK_FETCH,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

moq_result_t moq_d21_encode_fetch_ok(moq_buf_writer_t *w, bool end_of_track,
                                     moq_d21_location_t end,
                                     moq_bytes_t track_properties)
{
    if (!w) return MOQ_ERR_INVAL;
    if (track_properties.len > 0 &&
        d21_validate_track_properties(track_properties.data,
                                      track_properties.len) < 0)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d21_write_header(w, MOQ_D21_FETCH_OK, &len_off);
    if (rc < 0) { w->pos = saved; return rc; }
    uint8_t eot = end_of_track ? 1u : 0u;
    if ((rc = moq_buf_write_raw(w, &eot, 1)) < 0) goto fail;
    if ((rc = d21_write_location(w, end)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, 0)) < 0) goto fail;   /* zero parameters */
    if (track_properties.len > 0 &&
        (rc = moq_buf_write_raw(w, track_properties.data,
                                track_properties.len)) < 0) goto fail;
    if ((rc = d21_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_fetch_ok(const uint8_t *payload, size_t payload_len,
                                     moq_d21_fetch_ok_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_bytes_t eot;
    moq_result_t rc = moq_buf_read_raw(&r, 1, &eot);
    if (rc < 0) return rc;
    if (eot.data[0] > 1) return MOQ_ERR_PROTO;
    out->end_of_track = (eot.data[0] != 0);
    if ((rc = d21_read_location(&r, &out->end)) < 0) return rc;
    uint64_t param_count = 0;
    if ((rc = moq_buf_read_vi64(&r, &param_count)) < 0) return rc;
    /* No parameter is defined for FETCH_OK (9.20): any is a violation. */
    if (param_count != 0) return MOQ_ERR_PROTO;
    /* The remainder is the opaque Track Properties tail. */
    out->track_properties.data = moq_buf_reader_ptr(&r);
    out->track_properties.len = moq_buf_reader_remaining(&r);
    return d21_scan_track_properties(out->track_properties.data,
                                     out->track_properties.len,
                                     &out->track_properties_unsupported,
                                     NULL);
}

moq_result_t moq_d21_encode_fetch_header(moq_buf_writer_t *w, uint64_t request_id)
{
    if (!w) return MOQ_ERR_INVAL;
    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, MOQ_D21_STREAM_FETCH_HEADER);
    if (rc == MOQ_OK)
        rc = moq_buf_write_vi64(w, request_id);
    if (rc < 0) w->pos = saved;
    return rc;
}

moq_result_t moq_d21_decode_fetch_header(moq_buf_reader_t *r,
                                         uint64_t *out_request_id)
{
    if (!r || !out_request_id) return MOQ_ERR_INVAL;
    size_t saved = r->pos;
    uint64_t type = 0;
    moq_result_t rc = moq_buf_read_vi64(r, &type);
    if (rc < 0) { r->pos = saved; return rc; }
    if (type != MOQ_D21_STREAM_FETCH_HEADER) { r->pos = saved; return MOQ_ERR_PROTO; }
    if ((rc = moq_buf_read_vi64(r, out_request_id)) < 0) { r->pos = saved; return rc; }
    return MOQ_OK;
}
