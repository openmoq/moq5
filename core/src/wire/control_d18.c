#include "moq/control_d18.h"
#include "moq/vi64.h"
#include <string.h>

/* -- Control envelope (Type vi64 + Length 16 + payload) ------------ */

moq_result_t moq_d18_decode_envelope(moq_buf_reader_t *r,
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

moq_result_t moq_d18_encode_setup(moq_buf_writer_t *w)
{
    return moq_d18_encode_setup_opts(w, NULL);
}

/* -- vi64 field helpers (draft-18 uses vi64 for these lengths/counts) - */

/* Write Type (vi64) + a reserved 16-bit Length; *len_off receives the patch
 * offset for moq_buf_patch_uint16 once the payload is written. */
static moq_result_t d18_write_header(moq_buf_writer_t *w, uint64_t type,
                                     size_t *len_off)
{
    moq_result_t rc = moq_buf_write_vi64(w, type);
    if (rc < 0) return rc;
    return moq_buf_reserve_uint16(w, len_off);
}

/* Patch the reserved Length with the payload bytes written since len_off. */
static moq_result_t d18_patch_len(moq_buf_writer_t *w, size_t len_off)
{
    size_t payload = moq_buf_writer_offset(w) - (len_off + 2);
    if (payload > 0xFFFFu) return MOQ_ERR_INVAL;
    return moq_buf_patch_uint16(w, len_off, (uint16_t)payload);
}

static moq_result_t d18_write_span(moq_buf_writer_t *w, moq_bytes_t b)
{
    moq_result_t rc = moq_buf_write_vi64(w, b.len);
    if (rc < 0) return rc;
    if (b.len > 0)
        rc = moq_buf_write_raw(w, b.data, b.len);
    return rc;
}

static moq_result_t d18_read_span(moq_buf_reader_t *r, moq_bytes_t *out)
{
    uint64_t len = 0;
    moq_result_t rc = moq_buf_read_vi64(r, &len);
    if (rc < 0) return rc;
    return moq_buf_read_raw(r, (size_t)len, out);
}

static moq_result_t d18_write_namespace(moq_buf_writer_t *w,
                                        const moq_namespace_t *ns)
{
    if (ns->count > 32) return MOQ_ERR_INVAL;   /* draft-18: 0..32 fields */
    moq_result_t rc = moq_buf_write_vi64(w, ns->count);
    if (rc < 0) return rc;
    for (size_t i = 0; i < ns->count; i++) {
        if (ns->parts[i].len == 0)              /* fields must be non-empty */
            return MOQ_ERR_INVAL;
        rc = d18_write_span(w, ns->parts[i]);
        if (rc < 0) return rc;
    }
    return MOQ_OK;
}

static moq_result_t d18_read_namespace(moq_buf_reader_t *r, moq_bytes_t *parts,
                                       size_t max_parts, moq_namespace_t *out)
{
    uint64_t count = 0;
    moq_result_t rc = moq_buf_read_vi64(r, &count);
    if (rc < 0) return rc;
    if (count > 32) return MOQ_ERR_PROTO;        /* draft-18: 0..32 fields */
    if (count > max_parts) return MOQ_ERR_BUFFER;
    for (uint64_t i = 0; i < count; i++) {
        rc = d18_read_span(r, &parts[i]);
        if (rc < 0) return rc;
        if (parts[i].len == 0) return MOQ_ERR_PROTO;  /* fields non-empty */
    }
    out->parts = parts;
    out->count = (size_t)count;
    return MOQ_OK;
}

/* Full Track Name length = sum of namespace field lengths + track name. */
static uint64_t d18_full_track_len(const moq_namespace_t *ns,
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
#define D18_SUBSCRIBE_PARAM_MASK \
    (MOQ_D18_PARAM_BIT_FORWARD | MOQ_D18_PARAM_BIT_SUBSCRIBER_PRIORITY | \
     MOQ_D18_PARAM_BIT_SUBSCRIPTION_FILTER | MOQ_D18_PARAM_BIT_GROUP_ORDER | \
     MOQ_D18_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT | \
     MOQ_D18_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT | \
     MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN | \
     MOQ_D18_PARAM_BIT_RENDEZVOUS_TIMEOUT | \
     MOQ_D18_PARAM_BIT_NEW_GROUP_REQUEST)
#define D18_FETCH_PARAM_MASK \
    (MOQ_D18_PARAM_BIT_SUBSCRIBER_PRIORITY | MOQ_D18_PARAM_BIT_GROUP_ORDER | \
     MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN | MOQ_D18_PARAM_BIT_FILL_TIMEOUT)
#define D18_REQUEST_UPDATE_PARAM_MASK \
    (MOQ_D18_PARAM_BIT_FORWARD | MOQ_D18_PARAM_BIT_SUBSCRIBER_PRIORITY | \
     MOQ_D18_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT | \
     MOQ_D18_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT | \
     MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN | \
     MOQ_D18_PARAM_BIT_NEW_GROUP_REQUEST | \
     MOQ_D18_PARAM_BIT_TRACK_NAMESPACE_PREFIX)

static bool d18_params_within_mask(const moq_d18_msg_params_t *p, uint32_t mask)
{
    if (p->has_forward && !(mask & MOQ_D18_PARAM_BIT_FORWARD)) return false;
    if (p->has_subscriber_priority &&
        !(mask & MOQ_D18_PARAM_BIT_SUBSCRIBER_PRIORITY)) return false;
    if (p->has_filter && !(mask & MOQ_D18_PARAM_BIT_SUBSCRIPTION_FILTER))
        return false;
    if (p->has_group_order && !(mask & MOQ_D18_PARAM_BIT_GROUP_ORDER))
        return false;
    if (p->has_expires && !(mask & MOQ_D18_PARAM_BIT_EXPIRES)) return false;
    if (p->has_largest && !(mask & MOQ_D18_PARAM_BIT_LARGEST_OBJECT))
        return false;
    if (p->has_object_delivery_timeout &&
        !(mask & MOQ_D18_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT)) return false;
    if (p->has_subgroup_delivery_timeout &&
        !(mask & MOQ_D18_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT)) return false;
    if (p->auth_token_count > 0 &&
        !(mask & MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN)) return false;
    return true;
}

/* -- SUBSCRIBE ----------------------------------------------------- */

moq_result_t moq_d18_encode_subscribe(moq_buf_writer_t *w, uint64_t request_id,
                                      const moq_namespace_t *ns,
                                      moq_bytes_t track_name,
                                      const moq_d18_msg_params_t *params)
{
    if (!w || !ns || !params) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(params, D18_SUBSCRIBE_PARAM_MASK))
        return MOQ_ERR_INVAL;
    if (d18_full_track_len(ns, track_name) > MOQ_D18_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_SUBSCRIBE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d18_write_namespace(w, ns)) < 0) goto fail;
    if ((rc = d18_write_span(w, track_name)) < 0) goto fail;
    if ((rc = moq_d18_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_subscribe(const uint8_t *payload,
                                      size_t payload_len, moq_bytes_t *parts,
                                      size_t max_parts, moq_d18_subscribe_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d18_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    rc = d18_read_span(&r, &out->track_name);
    if (rc < 0) return rc;
    if (d18_full_track_len(&out->track_namespace, out->track_name) >
        MOQ_D18_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_SUBSCRIBE_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- PUBLISH_NAMESPACE (draft-18 §10.15) --------------------------- */

/* PUBLISH_NAMESPACE permits only the AUTHORIZATION_TOKEN message parameter. */
#define D18_PUBLISH_NAMESPACE_PARAM_MASK  MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN

moq_result_t moq_d18_encode_publish_namespace(moq_buf_writer_t *w,
                                              uint64_t request_id,
                                              const moq_namespace_t *ns,
                                              const moq_d18_msg_params_t *params)
{
    if (!w || !ns || !params) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(params, D18_PUBLISH_NAMESPACE_PARAM_MASK))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_PUBLISH_NAMESPACE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d18_write_namespace(w, ns)) < 0) goto fail;
    if ((rc = moq_d18_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_publish_namespace(const uint8_t *payload,
                                              size_t payload_len,
                                              moq_bytes_t *parts,
                                              size_t max_parts,
                                              moq_d18_publish_namespace_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d18_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_PUBLISH_NAMESPACE_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- SUBSCRIBE_NAMESPACE (draft-18 §10.18) ------------------------- */

/* SUBSCRIBE_NAMESPACE permits only the AUTHORIZATION_TOKEN message parameter
 * (it is namespace-only in draft-18; FORWARD and the interest field moved to
 * SUBSCRIBE_TRACKS). */
#define D18_SUBSCRIBE_NAMESPACE_PARAM_MASK  MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN

moq_result_t moq_d18_encode_subscribe_namespace(moq_buf_writer_t *w,
                                                uint64_t request_id,
                                                const moq_namespace_t *prefix,
                                                const moq_d18_msg_params_t *params)
{
    if (!w || !prefix || !params) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(params, D18_SUBSCRIBE_NAMESPACE_PARAM_MASK))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_SUBSCRIBE_NAMESPACE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d18_write_namespace(w, prefix)) < 0) goto fail;
    if ((rc = moq_d18_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_subscribe_namespace(const uint8_t *payload,
                                                size_t payload_len,
                                                moq_bytes_t *parts,
                                                size_t max_parts,
                                                moq_d18_subscribe_namespace_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d18_read_namespace(&r, parts, max_parts, &out->track_namespace_prefix);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_SUBSCRIBE_NAMESPACE_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- SUBSCRIBE_TRACKS (draft-18 §10.19) ---------------------------- */

/* SUBSCRIBE_TRACKS permits the FORWARD and AUTHORIZATION_TOKEN message
 * parameters (it governs the FORWARD value of the resulting PUBLISH messages;
 * §10.19). Any other parameter is a protocol violation. */
#define D18_SUBSCRIBE_TRACKS_PARAM_MASK \
    (MOQ_D18_PARAM_BIT_FORWARD | MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN)

moq_result_t moq_d18_encode_subscribe_tracks(moq_buf_writer_t *w,
                                             uint64_t request_id,
                                             const moq_namespace_t *prefix,
                                             const moq_d18_msg_params_t *params)
{
    if (!w || !prefix || !params) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(params, D18_SUBSCRIBE_TRACKS_PARAM_MASK))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_SUBSCRIBE_TRACKS, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d18_write_namespace(w, prefix)) < 0) goto fail;
    if ((rc = moq_d18_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_subscribe_tracks(const uint8_t *payload,
                                             size_t payload_len,
                                             moq_bytes_t *parts,
                                             size_t max_parts,
                                             moq_d18_subscribe_tracks_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d18_read_namespace(&r, parts, max_parts, &out->track_namespace_prefix);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_SUBSCRIBE_TRACKS_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- PUBLISH_BLOCKED (draft-18 §10.20) ----------------------------- */

moq_result_t moq_d18_encode_publish_blocked(moq_buf_writer_t *w,
                                            const moq_namespace_t *suffix,
                                            moq_bytes_t track_name)
{
    if (!w || !suffix) return MOQ_ERR_INVAL;
    if (d18_full_track_len(suffix, track_name) > MOQ_D18_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_PUBLISH_BLOCKED, &len_off);
    if (rc < 0) return rc;
    if ((rc = d18_write_namespace(w, suffix)) < 0) goto fail;
    if ((rc = d18_write_span(w, track_name)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_publish_blocked(const uint8_t *payload,
                                            size_t payload_len,
                                            moq_bytes_t *parts,
                                            size_t max_parts,
                                            moq_d18_publish_blocked_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = d18_read_namespace(&r, parts, max_parts,
                                         &out->track_namespace_suffix);
    if (rc < 0) return rc;
    rc = d18_read_span(&r, &out->track_name);
    if (rc < 0) return rc;
    if (d18_full_track_len(&out->track_namespace_suffix, out->track_name) >
        MOQ_D18_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- NAMESPACE / NAMESPACE_DONE (draft-18 §10.16 / §10.17) --------- */

moq_result_t moq_d18_encode_namespace_msg(moq_buf_writer_t *w,
                                          const moq_namespace_t *suffix,
                                          bool is_done)
{
    if (!w || !suffix) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    uint64_t type = is_done ? MOQ_D18_NAMESPACE_DONE : MOQ_D18_NAMESPACE;
    moq_result_t rc = d18_write_header(w, type, &len_off);
    if (rc < 0) return rc;
    if ((rc = d18_write_namespace(w, suffix)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_namespace_msg(const uint8_t *payload,
                                          size_t payload_len,
                                          moq_bytes_t *parts,
                                          size_t max_parts,
                                          moq_namespace_t *out_suffix)
{
    if (!payload || !parts || !out_suffix) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = d18_read_namespace(&r, parts, max_parts, out_suffix);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- TRACK_STATUS (draft-18 §10.14) -------------------------------- */

/* TRACK_STATUS is the SUBSCRIBE layout minus Track-delivery params; only the
 * AUTHORIZATION_TOKEN message parameter applies. */
#define D18_TRACK_STATUS_PARAM_MASK  MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN

moq_result_t moq_d18_encode_track_status(moq_buf_writer_t *w, uint64_t request_id,
                                         const moq_namespace_t *ns,
                                         moq_bytes_t track_name,
                                         const moq_d18_msg_params_t *params)
{
    if (!w || !ns || !params) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(params, D18_TRACK_STATUS_PARAM_MASK))
        return MOQ_ERR_INVAL;
    if (d18_full_track_len(ns, track_name) > MOQ_D18_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_TRACK_STATUS, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d18_write_namespace(w, ns)) < 0) goto fail;
    if ((rc = d18_write_span(w, track_name)) < 0) goto fail;
    if ((rc = moq_d18_encode_msg_params(w, params)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_track_status(const uint8_t *payload,
                                         size_t payload_len, moq_bytes_t *parts,
                                         size_t max_parts,
                                         moq_d18_track_status_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d18_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    rc = d18_read_span(&r, &out->track_name);
    if (rc < 0) return rc;
    if (d18_full_track_len(&out->track_namespace, out->track_name) >
        MOQ_D18_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_TRACK_STATUS_PARAM_MASK,
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
#define D18_PROP_IMMUTABLE      0x0Bu
#define D18_PROP_DYNAMIC_GROUPS 0x30u   /* §12.6: 0/1; >1 is a violation */

/* Validate a Property KVP block structurally. A Mandatory Track Property
 * (0x4000-0x7FFF, §2.5.1 — including one hidden inside IMMUTABLE_PROPERTIES,
 * §12.7) is handled per `out_mandatory`: when non-NULL it is recorded
 * (*out_mandatory = true) and is NOT itself an error (the caller decides — object
 * properties are malformed and close, track properties respond
 * UNSUPPORTED_EXTENSION); when NULL it is rejected (MOQ_ERR_PROTO). Malformed
 * structure is always MOQ_ERR_PROTO. */
static moq_result_t d18_validate_props_inner(const uint8_t *data, size_t len,
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
            if (type == D18_PROP_IMMUTABLE) {
                if (nested) return MOQ_ERR_PROTO;   /* §12.7: no nesting */
                moq_result_t rc = d18_validate_props_inner(
                    moq_buf_reader_ptr(&r), (size_t)vlen, true, out_mandatory,
                    out_dynamic_groups);
                if (rc < 0) return rc;
            }
            r.pos += (size_t)vlen;
        } else {                             /* even: single varint value */
            uint64_t v;
            if (moq_buf_read_vi64(&r, &v) < 0) return MOQ_ERR_PROTO;
            if (type == D18_PROP_DYNAMIC_GROUPS) {
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
static moq_result_t d18_validate_track_properties(const uint8_t *data,
                                                  size_t len)
{
    return d18_validate_props_inner(data, len, false, NULL, NULL);
}

/* Decode-side scan: structurally valid; *out_mandatory reports whether any
 * Mandatory Track Property is present so the caller can respond with a
 * request-level UNSUPPORTED_EXTENSION rather than closing the session. */
static moq_result_t d18_scan_track_properties(const uint8_t *data, size_t len,
                                              bool *out_mandatory,
                                              bool *out_dynamic_groups)
{
    *out_mandatory = false;
    if (out_dynamic_groups) *out_dynamic_groups = false;
    return d18_validate_props_inner(data, len, false, out_mandatory,
                                    out_dynamic_groups);
}

moq_result_t moq_d18_validate_properties(const uint8_t *data, size_t len)
{
    return d18_validate_props_inner(data, len, false, NULL, NULL);
}

/* Lenient DYNAMIC_GROUPS extraction for the profile's outbound latch: walks
 * tolerantly (mandatory properties permitted) and reports whether 0x30 == 1.
 * Returns MOQ_ERR_PROTO only for a structurally unreadable blob or a 0x30
 * value above 1 (the caller treats both as "no support" on the outbound
 * side; inbound enforcement happens in the decode scans). */
moq_result_t moq_d18_scan_dynamic_groups(const uint8_t *data, size_t len,
                                         bool *out_dynamic_groups)
{
    if (!out_dynamic_groups) return MOQ_ERR_INVAL;
    if (len > 0 && !data) return MOQ_ERR_INVAL;
    bool mandatory_ignored = false;
    *out_dynamic_groups = false;
    return d18_validate_props_inner(data, len, false, &mandatory_ignored,
                                    out_dynamic_groups);
}

/* -- OBJECT_DATAGRAM (§11.3.1) + Padding Datagram (§11.5.2) -------- *
 * The D18 data plane is vi64 throughout (type, track alias, group/object id,
 * property length, object status), matching the subgroup/object codecs above;
 * publisher priority is a raw byte. */
moq_result_t moq_d18_decode_object_datagram(
    const uint8_t *data, size_t len,
    moq_d18_object_datagram_t *out)
{
    if (!data || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));

    moq_buf_reader_t r;
    moq_buf_reader_init(&r, data, len);

    uint64_t type = 0;
    if (moq_buf_read_vi64(&r, &type) < 0) return MOQ_ERR_PROTO;
    if (type == MOQ_D18_PADDING_DATAGRAM) {
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

    bool status = (type & MOQ_D18_DGRAM_BIT_STATUS) != 0;
    bool eog    = (type & MOQ_D18_DGRAM_BIT_END_OF_GROUP) != 0;
    if (status && eog) return MOQ_ERR_PROTO;   /* status cannot signal end of group */
    out->has_properties   = (type & MOQ_D18_DGRAM_BIT_PROPERTIES) != 0;
    out->end_of_group     = eog;
    bool zero_object_id   = (type & MOQ_D18_DGRAM_BIT_ZERO_OBJECT_ID) != 0;
    out->default_priority = (type & MOQ_D18_DGRAM_BIT_DEFAULT_PRIO) != 0;
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
        moq_result_t prc = moq_d18_validate_properties(pp, (size_t)props_len);
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

moq_result_t moq_d18_encode_object_datagram(
    moq_buf_writer_t *w,
    const moq_d18_object_datagram_t *dg)
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
    if (dg->has_properties)   type |= MOQ_D18_DGRAM_BIT_PROPERTIES;
    if (dg->end_of_group)     type |= MOQ_D18_DGRAM_BIT_END_OF_GROUP;
    bool zero_oid = (dg->object_id == 0);
    if (zero_oid)             type |= MOQ_D18_DGRAM_BIT_ZERO_OBJECT_ID;
    if (dg->default_priority) type |= MOQ_D18_DGRAM_BIT_DEFAULT_PRIO;
    if (dg->is_status)        type |= MOQ_D18_DGRAM_BIT_STATUS;

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
        rc = moq_d18_validate_properties(dg->properties, dg->properties_len);
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
#define D18_SUBSCRIBE_OK_PARAM_MASK \
    (MOQ_D18_PARAM_BIT_EXPIRES | MOQ_D18_PARAM_BIT_LARGEST_OBJECT)

moq_result_t moq_d18_encode_subscribe_ok(moq_buf_writer_t *w,
                                         uint64_t track_alias,
                                         const moq_d18_msg_params_t *params,
                                         moq_bytes_t track_properties)
{
    if (!w || !params) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(params, D18_SUBSCRIBE_OK_PARAM_MASK))
        return MOQ_ERR_INVAL;
    if (track_properties.len > 0 &&
        d18_validate_track_properties(track_properties.data,
                                      track_properties.len) < 0)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_SUBSCRIBE_OK, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, track_alias)) < 0) goto fail;
    if ((rc = moq_d18_encode_msg_params(w, params)) < 0) goto fail;
    if (track_properties.len > 0 &&
        (rc = moq_buf_write_raw(w, track_properties.data,
                                track_properties.len)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_subscribe_ok(const uint8_t *payload,
                                         size_t payload_len,
                                         moq_d18_subscribe_ok_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->track_alias);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_SUBSCRIBE_OK_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    /* The remainder is the opaque Track Properties tail. */
    out->track_properties.data = moq_buf_reader_ptr(&r);
    out->track_properties.len = moq_buf_reader_remaining(&r);
    return d18_scan_track_properties(out->track_properties.data,
                                     out->track_properties.len,
                                     &out->track_properties_unsupported,
                                     &out->dynamic_groups);
}

/* -- PUBLISH (draft-18 §10.10) ------------------------------------- */

/* PUBLISH permits the FORWARD (publisher's initial forward intent) and
 * AUTHORIZATION_TOKEN message parameters; other parameters are a violation. */
#define D18_PUBLISH_PARAM_MASK \
    (MOQ_D18_PARAM_BIT_FORWARD | MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN)

moq_result_t moq_d18_encode_publish(moq_buf_writer_t *w,
                                    const moq_d18_publish_t *p)
{
    if (!w || !p) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(&p->params, D18_PUBLISH_PARAM_MASK))
        return MOQ_ERR_INVAL;
    if (d18_full_track_len(&p->track_namespace, p->track_name) >
        MOQ_D18_MAX_FULL_TRACK)
        return MOQ_ERR_INVAL;
    if (p->track_properties.len > 0 &&
        d18_validate_track_properties(p->track_properties.data,
                                      p->track_properties.len) < 0)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_PUBLISH, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, p->request_id)) < 0) goto fail;
    if ((rc = d18_write_namespace(w, &p->track_namespace)) < 0) goto fail;
    if ((rc = d18_write_span(w, p->track_name)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, p->track_alias)) < 0) goto fail;
    if ((rc = moq_d18_encode_msg_params(w, &p->params)) < 0) goto fail;
    if (p->track_properties.len > 0 &&
        (rc = moq_buf_write_raw(w, p->track_properties.data,
                                p->track_properties.len)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_publish(const uint8_t *payload, size_t payload_len,
                                    moq_bytes_t *parts, size_t max_parts,
                                    moq_d18_publish_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    rc = d18_read_namespace(&r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    rc = d18_read_span(&r, &out->track_name);
    if (rc < 0) return rc;
    if (d18_full_track_len(&out->track_namespace, out->track_name) >
        MOQ_D18_MAX_FULL_TRACK)
        return MOQ_ERR_PROTO;
    if ((rc = moq_buf_read_vi64(&r, &out->track_alias)) < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_PUBLISH_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    /* The remainder is the opaque Track Properties tail. */
    out->track_properties.data = moq_buf_reader_ptr(&r);
    out->track_properties.len = moq_buf_reader_remaining(&r);
    return d18_scan_track_properties(out->track_properties.data,
                                     out->track_properties.len,
                                     &out->track_properties_unsupported,
                                     &out->dynamic_groups);
}

/* -- PUBLISH_OK (draft-18 §10.10 / §10.5) -------------------------- */

/* PUBLISH_OK is a REQUEST_OK carrying the subscriber's delivery parameters and
 * an empty Track Properties tail (a non-empty tail is a PROTOCOL_VIOLATION). */
#define D18_PUBLISH_OK_PARAM_MASK \
    (MOQ_D18_PARAM_BIT_SUBSCRIBER_PRIORITY | MOQ_D18_PARAM_BIT_FORWARD | \
     MOQ_D18_PARAM_BIT_GROUP_ORDER | MOQ_D18_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT | \
     MOQ_D18_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT | \
     MOQ_D18_PARAM_BIT_NEW_GROUP_REQUEST)

moq_result_t moq_d18_encode_publish_ok(moq_buf_writer_t *w,
                                       const moq_d18_msg_params_t *params)
{
    if (!w || !params) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(params, D18_PUBLISH_OK_PARAM_MASK))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_REQUEST_OK, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_d18_encode_msg_params(w, params)) < 0) goto fail;
    /* Track Properties are empty in PUBLISH_OK (§10.5). */
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_publish_ok(const uint8_t *payload,
                                       size_t payload_len,
                                       moq_d18_publish_ok_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    uint64_t count;
    moq_result_t rc = moq_buf_read_vi64(&r, &count);
    if (rc < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_PUBLISH_OK_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    /* PUBLISH_OK carries an empty Track Properties tail; any remainder is a
     * protocol violation (§10.5). */
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- GOAWAY (draft-18 §10.4) --------------------------------------- */

/* Control-stream form: New Session URI + Timeout + Request ID. */
moq_result_t moq_d18_encode_goaway(moq_buf_writer_t *w,
                                   const uint8_t *uri, size_t uri_len,
                                   uint64_t timeout_ms, uint64_t request_id)
{
    if (!w) return MOQ_ERR_INVAL;
    if (uri_len > 0 && !uri) return MOQ_ERR_INVAL;
    if (uri_len > 8192) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_GOAWAY, &len_off);
    if (rc < 0) return rc;
    moq_bytes_t span = { uri, uri_len };
    if ((rc = d18_write_span(w, span)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, timeout_ms)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_goaway(const uint8_t *payload, size_t payload_len,
                                   moq_d18_goaway_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_bytes_t uri;
    moq_result_t rc = d18_read_span(&r, &uri);
    if (rc < 0) return rc;
    if (uri.len > 8192) return MOQ_ERR_PROTO;
    if ((rc = moq_buf_read_vi64(&r, &out->timeout_ms)) < 0) return rc;
    if ((rc = moq_buf_read_vi64(&r, &out->request_id)) < 0) return rc;
    /* Strict: nothing follows the Request ID on a control-stream GOAWAY. */
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    out->uri.data = uri.len > 0 ? uri.data : NULL;
    out->uri.len = uri.len;
    return MOQ_OK;
}

/* Request-stream form (§10.4): New Session URI + Timeout, no Request ID (the
 * stream identifies the request being migrated). */
moq_result_t moq_d18_encode_goaway_request(moq_buf_writer_t *w,
                                           const uint8_t *uri, size_t uri_len,
                                           uint64_t timeout_ms)
{
    if (!w) return MOQ_ERR_INVAL;
    if (uri_len > 0 && !uri) return MOQ_ERR_INVAL;
    if (uri_len > 8192) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_GOAWAY, &len_off);
    if (rc < 0) return rc;
    moq_bytes_t span = { uri, uri_len };
    if ((rc = d18_write_span(w, span)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, timeout_ms)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_goaway_request(const uint8_t *payload,
                                           size_t payload_len,
                                           moq_d18_goaway_t *out)
{
    if (!out) return MOQ_ERR_INVAL;
    if (!payload && payload_len > 0) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_bytes_t uri;
    moq_result_t rc = d18_read_span(&r, &uri);
    if (rc < 0) return rc;
    if (uri.len > 8192) return MOQ_ERR_PROTO;
    if ((rc = moq_buf_read_vi64(&r, &out->timeout_ms)) < 0) return rc;
    /* Strict: no Request ID and nothing else on a request-stream GOAWAY. */
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    out->uri.data = uri.len > 0 ? uri.data : NULL;
    out->uri.len = uri.len;
    return MOQ_OK;
}

/* -- TRACK_STATUS_OK (draft-18 §10.14 / §10.5) --------------------- */

/* TRACK_STATUS_OK is a REQUEST_OK carrying LARGEST_OBJECT / EXPIRES parameters
 * and a Track Properties tail, with no Track Alias (§10.14). */
#define D18_TRACK_STATUS_OK_PARAM_MASK \
    (MOQ_D18_PARAM_BIT_EXPIRES | MOQ_D18_PARAM_BIT_LARGEST_OBJECT)

moq_result_t moq_d18_encode_track_status_ok(moq_buf_writer_t *w,
                                            const moq_d18_msg_params_t *params,
                                            moq_bytes_t track_properties)
{
    if (!w || !params) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(params, D18_TRACK_STATUS_OK_PARAM_MASK))
        return MOQ_ERR_INVAL;
    if (track_properties.len > 0 &&
        d18_validate_track_properties(track_properties.data,
                                      track_properties.len) < 0)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_REQUEST_OK, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_d18_encode_msg_params(w, params)) < 0) goto fail;
    if (track_properties.len > 0 &&
        (rc = moq_buf_write_raw(w, track_properties.data,
                                track_properties.len)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_track_status_ok(const uint8_t *payload,
                                            size_t payload_len,
                                            moq_d18_track_status_ok_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    uint64_t count;
    moq_result_t rc = moq_buf_read_vi64(&r, &count);
    if (rc < 0) return rc;
    rc = moq_d18_decode_msg_params(&r, count, D18_TRACK_STATUS_OK_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    /* The remainder is the opaque Track Properties tail. TRACK_STATUS_OK is not in
     * the UNSUPPORTED_EXTENSION list (§10.6), so an unknown mandatory property is
     * accepted and surfaced (validated structurally only) rather than rejected. */
    out->track_properties.data = moq_buf_reader_ptr(&r);
    out->track_properties.len = moq_buf_reader_remaining(&r);
    bool ts_mandatory_ignored = false;
    return d18_scan_track_properties(out->track_properties.data,
                                     out->track_properties.len,
                                     &ts_mandatory_ignored, NULL);
}

/* -- REQUEST_ERROR ------------------------------------------------- */

moq_result_t moq_d18_encode_request_error(moq_buf_writer_t *w,
                                          uint64_t error_code,
                                          uint64_t retry_interval,
                                          moq_bytes_t reason)
{
    if (!w) return MOQ_ERR_INVAL;
    if (reason.len > MOQ_D18_MAX_REASON) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_REQUEST_ERROR, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, error_code)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, retry_interval)) < 0) goto fail;
    if ((rc = d18_write_span(w, reason)) < 0) goto fail;
    /* No Redirect (only present for the REDIRECT error code). */
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_request_error(const uint8_t *payload,
                                          size_t payload_len,
                                          moq_d18_request_error_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->error_code);
    if (rc < 0) return rc;
    rc = moq_buf_read_vi64(&r, &out->retry_interval);
    if (rc < 0) return rc;
    rc = d18_read_span(&r, &out->reason);
    if (rc < 0) return rc;
    if (out->reason.len > MOQ_D18_MAX_REASON) return MOQ_ERR_PROTO;
    /* The base decoder does not parse a Redirect tail; reject leftover bytes
     * rather than silently accepting them. Use the redirect-aware decoder for the
     * REDIRECT error code. */
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- Redirect structure (draft-18 §10.6.1) ------------------------- */

/* Read a Redirect from an in-progress reader (no trailing-byte check). */
static moq_result_t d18_read_redirect(moq_buf_reader_t *r,
                                      moq_bytes_t *parts, size_t max_parts,
                                      moq_d18_redirect_t *out)
{
    memset(out, 0, sizeof(*out));
    moq_result_t rc = d18_read_span(r, &out->connect_uri);
    if (rc < 0) return rc;
    rc = d18_read_namespace(r, parts, max_parts, &out->track_namespace);
    if (rc < 0) return rc;
    return d18_read_span(r, &out->track_name);
}

static moq_result_t d18_write_redirect(moq_buf_writer_t *w,
                                       const moq_d18_redirect_t *redirect)
{
    moq_result_t rc = d18_write_span(w, redirect->connect_uri);
    if (rc < 0) return rc;
    rc = d18_write_namespace(w, &redirect->track_namespace);
    if (rc < 0) return rc;
    return d18_write_span(w, redirect->track_name);
}

moq_result_t moq_d18_encode_redirect(moq_buf_writer_t *w,
                                     const moq_d18_redirect_t *redirect)
{
    if (!w || !redirect) return MOQ_ERR_INVAL;
    size_t saved = w->pos;
    moq_result_t rc = d18_write_redirect(w, redirect);
    if (rc < 0) w->pos = saved;   /* transactional: no partial output */
    return rc;
}

moq_result_t moq_d18_decode_redirect(const uint8_t *payload, size_t payload_len,
                                     moq_bytes_t *parts, size_t max_parts,
                                     moq_d18_redirect_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = d18_read_redirect(&r, parts, max_parts, out);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

moq_result_t moq_d18_encode_request_error_redirect(
    moq_buf_writer_t *w, uint64_t error_code, uint64_t retry_interval,
    moq_bytes_t reason, const moq_d18_redirect_t *redirect)
{
    if (!w || !redirect) return MOQ_ERR_INVAL;
    /* The Redirect tail is present only for the REDIRECT error code (§10.6.1). */
    if (error_code != MOQ_D18_ERROR_REDIRECT) return MOQ_ERR_INVAL;
    if (reason.len > MOQ_D18_MAX_REASON) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_REQUEST_ERROR, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, error_code)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, retry_interval)) < 0) goto fail;
    if ((rc = d18_write_span(w, reason)) < 0) goto fail;
    if ((rc = d18_write_redirect(w, redirect)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_request_error_redirect(
    const uint8_t *payload, size_t payload_len,
    moq_bytes_t *parts, size_t max_parts,
    moq_d18_request_error_t *out_err, moq_d18_redirect_t *out_redirect)
{
    if (!payload || !parts || !out_err || !out_redirect) return MOQ_ERR_INVAL;
    memset(out_redirect, 0, sizeof(*out_redirect));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out_err->error_code);
    if (rc < 0) return rc;
    rc = moq_buf_read_vi64(&r, &out_err->retry_interval);
    if (rc < 0) return rc;
    rc = d18_read_span(&r, &out_err->reason);
    if (rc < 0) return rc;
    if (out_err->reason.len > MOQ_D18_MAX_REASON) return MOQ_ERR_PROTO;
    if (out_err->error_code == MOQ_D18_ERROR_REDIRECT) {
        rc = d18_read_redirect(&r, parts, max_parts, out_redirect);
        if (rc < 0) return rc;
    }
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- Message Parameters (draft-18 §10.2) --------------------------- */

static moq_result_t d18_write_u8(moq_buf_writer_t *w, uint8_t v)
{
    return moq_buf_write_raw(w, &v, 1);
}

static moq_result_t d18_read_u8(moq_buf_reader_t *r, uint8_t *out)
{
    if (moq_buf_reader_remaining(r) < 1) return MOQ_ERR_BUFFER;
    *out = *moq_buf_reader_ptr(r);
    r->pos += 1;
    return MOQ_OK;
}

/* Subscription Filter (§5.1.2) body, written as the length-prefixed value of the
 * SUBSCRIPTION_FILTER parameter: Filter Type, then Start Location (types 3/4),
 * then End Group Delta (type 4). */
static moq_result_t d18_write_filter(moq_buf_writer_t *w,
                                     const moq_d18_msg_params_t *p)
{
    uint8_t body[48];
    moq_buf_writer_t bw;
    moq_buf_writer_init(&bw, body, sizeof(body));
    moq_result_t rc = moq_buf_write_vi64(&bw, p->filter_type);
    if (rc < 0) return rc;
    if (p->filter_type == 3 || p->filter_type == 4) {
        if ((rc = moq_buf_write_vi64(&bw, p->filter_start_group)) < 0) return rc;
        if ((rc = moq_buf_write_vi64(&bw, p->filter_start_object)) < 0) return rc;
    }
    if (p->filter_type == 4) {
        if (p->filter_end_group < p->filter_start_group) return MOQ_ERR_INVAL;
        if ((rc = moq_buf_write_vi64(&bw,
                p->filter_end_group - p->filter_start_group)) < 0) return rc;
    }
    moq_bytes_t span = { body, moq_buf_writer_offset(&bw) };
    return d18_write_span(w, span);
}

static moq_result_t d18_read_filter(moq_buf_reader_t *r,
                                    moq_d18_msg_params_t *out)
{
    moq_bytes_t fb;
    if (d18_read_span(r, &fb) < 0) return MOQ_ERR_BUFFER;
    moq_buf_reader_t fr;
    moq_buf_reader_init(&fr, fb.data, fb.len);
    uint64_t ft;
    if (moq_buf_read_vi64(&fr, &ft) < 0) return MOQ_ERR_PROTO;
    if (ft < 1 || ft > 4) return MOQ_ERR_PROTO;        /* §5.1.2 */
    out->filter_type = (uint32_t)ft;
    if (ft == 3 || ft == 4) {
        if (moq_buf_read_vi64(&fr, &out->filter_start_group) < 0)
            return MOQ_ERR_PROTO;
        if (moq_buf_read_vi64(&fr, &out->filter_start_object) < 0)
            return MOQ_ERR_PROTO;
    }
    if (ft == 4) {
        uint64_t delta;
        if (moq_buf_read_vi64(&fr, &delta) < 0) return MOQ_ERR_PROTO;
        if (delta > UINT64_MAX - out->filter_start_group) return MOQ_ERR_PROTO;
        out->filter_end_group = out->filter_start_group + delta;
    }
    if (moq_buf_reader_remaining(&fr) != 0) return MOQ_ERR_PROTO;
    out->has_filter = true;
    return MOQ_OK;
}

/* AUTHORIZATION_TOKEN (§10.2.2) Token structure, written as the length-prefixed
 * value of the parameter. Integers are vi64; the Token Value (REGISTER /
 * USE_VALUE) runs to the end of the structure. The value can be arbitrarily
 * large, so the span length is computed up front rather than staged. */
static moq_result_t d18_write_auth_token(moq_buf_writer_t *w,
                                         const moq_d18_auth_token_t *t)
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
 * MOQ_D18_ERR_KVP_FORMAT so the profile closes with KEY_VALUE_FORMATTING_ERROR
 * (§10.2.2) rather than the generic PROTOCOL_VIOLATION. token_value borrows from
 * the span. */
static moq_result_t d18_read_auth_token(moq_buf_reader_t *r,
                                        moq_d18_auth_token_t *out)
{
    moq_bytes_t span;
    if (d18_read_span(r, &span) < 0) return MOQ_D18_ERR_KVP_FORMAT;
    moq_buf_reader_t tr;
    moq_buf_reader_init(&tr, span.data, span.len);
    uint64_t at;
    if (moq_buf_read_vi64(&tr, &at) < 0) return MOQ_D18_ERR_KVP_FORMAT;
    if (at > MOQ_AUTH_TOKEN_USE_VALUE) return MOQ_D18_ERR_KVP_FORMAT;
    memset(out, 0, sizeof(*out));
    out->alias_type = (uint32_t)at;
    switch (at) {
    case MOQ_AUTH_TOKEN_DELETE:
    case MOQ_AUTH_TOKEN_USE_ALIAS:
        if (moq_buf_read_vi64(&tr, &out->alias) < 0)
            return MOQ_D18_ERR_KVP_FORMAT;
        break;
    case MOQ_AUTH_TOKEN_REGISTER:
        if (moq_buf_read_vi64(&tr, &out->alias) < 0)
            return MOQ_D18_ERR_KVP_FORMAT;
        if (moq_buf_read_vi64(&tr, &out->token_type) < 0)
            return MOQ_D18_ERR_KVP_FORMAT;
        out->token_value.data = moq_buf_reader_ptr(&tr);
        out->token_value.len = moq_buf_reader_remaining(&tr);
        tr.pos = tr.len;
        break;
    case MOQ_AUTH_TOKEN_USE_VALUE:
        if (moq_buf_read_vi64(&tr, &out->token_type) < 0)
            return MOQ_D18_ERR_KVP_FORMAT;
        out->token_value.data = moq_buf_reader_ptr(&tr);
        out->token_value.len = moq_buf_reader_remaining(&tr);
        tr.pos = tr.len;
        break;
    }
    if (moq_buf_reader_remaining(&tr) != 0) return MOQ_D18_ERR_KVP_FORMAT;
    return MOQ_OK;
}

/* -- SETUP Options (§10.3.1) ---------------------------------------- *
 * vi64 Key-Value-Pairs (§1.4.3) spanning the SETUP payload — NOT the draft-16
 * QUIC-varint KVP form (the two encodings diverge at values 64..127, the same
 * trap as the data-plane property work). Unknown options are self-describing and
 * skipped per §10.3 (duplicates of unknown options allowed); a duplicate known
 * non-repeatable option closes; AUTHORIZATION_TOKEN may repeat. */

moq_result_t moq_d18_decode_setup_opts(const uint8_t *payload, size_t len,
                                       moq_d18_setup_opts_t *out)
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

        if (type == MOQ_D18_SETUP_OPT_AUTHORIZATION_TOKEN) {
            /* Odd type: d18_read_auth_token consumes the vi64 Length + token
             * structure directly. May repeat (§10.3.1.4). */
            if (out->auth_token_count >= MOQ_D18_MAX_AUTH_TOKENS)
                return MOQ_ERR_PROTO;
            moq_result_t arc = d18_read_auth_token(
                &r, &out->auth_tokens[out->auth_token_count]);
            if (arc < 0) return arc;       /* may be MOQ_D18_ERR_KVP_FORMAT */
            out->auth_token_count++;
            continue;
        }

        if ((type & 1u) == 0) {
            /* Even type: single vi64 value. */
            uint64_t v;
            if (moq_buf_read_vi64(&r, &v) < 0) return MOQ_ERR_PROTO;
            if (type == MOQ_D18_SETUP_OPT_MAX_AUTH_TOKEN_CACHE_SIZE) {
                if (out->has_max_auth_token_cache_size)
                    return MOQ_ERR_PROTO;  /* duplicate non-repeatable option */
                out->has_max_auth_token_cache_size = true;
                out->max_auth_token_cache_size = v;
            }
            /* Unknown even option: value already consumed; ignore. */
            continue;
        }

        /* Odd type: vi64 Length + bytes (§1.4.3: length above 2^16-1 closes). */
        uint64_t vlen;
        if (moq_buf_read_vi64(&r, &vlen) < 0) return MOQ_ERR_PROTO;
        if (vlen > 0xFFFFu) return MOQ_ERR_PROTO;
        if (vlen > moq_buf_reader_remaining(&r)) return MOQ_ERR_PROTO;
        if (type == MOQ_D18_SETUP_OPT_PATH) {
            if (out->has_path) return MOQ_ERR_PROTO;   /* non-repeatable */
            out->has_path = true;
        } else if (type == MOQ_D18_SETUP_OPT_AUTHORITY) {
            if (out->has_authority) return MOQ_ERR_PROTO;
            out->has_authority = true;
        }
        /* Unknown odd option: skip its value; duplicates allowed. */
        r.pos += (size_t)vlen;
    }
    return MOQ_OK;
}

moq_result_t moq_d18_encode_setup_opts(moq_buf_writer_t *w,
                                       const moq_d18_setup_opts_t *opts)
{
    if (!w) return MOQ_ERR_INVAL;
    /* Only cache-size emission is sourced today; refuse silently dropping any
     * other requested option. */
    if (opts && (opts->has_path || opts->has_authority ||
                 opts->auth_token_count > 0))
        return MOQ_ERR_INVAL;

    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_STREAM_SETUP, &len_off);
    if (rc < 0) { w->pos = saved; return rc; }
    if (opts && opts->has_max_auth_token_cache_size) {
        /* First option: Delta Type == absolute type; even => vi64 value. */
        if ((rc = moq_buf_write_vi64(
                w, MOQ_D18_SETUP_OPT_MAX_AUTH_TOKEN_CACHE_SIZE)) < 0)
            goto fail;
        if ((rc = moq_buf_write_vi64(w, opts->max_auth_token_cache_size)) < 0)
            goto fail;
    }
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_encode_msg_params(moq_buf_writer_t *w,
                                       const moq_d18_msg_params_t *p)
{
    if (!w || !p) return MOQ_ERR_INVAL;
    if (p->has_forward && p->forward > 1) return MOQ_ERR_INVAL;
    if (p->has_group_order && (p->group_order < 1 || p->group_order > 2))
        return MOQ_ERR_INVAL;
    if (p->has_filter && (p->filter_type < 1 || p->filter_type > 4))
        return MOQ_ERR_INVAL;
    if (p->auth_token_count > MOQ_D18_MAX_AUTH_TOKENS) return MOQ_ERR_INVAL;
    uint64_t count = (p->has_object_delivery_timeout ? 1u : 0u) +
                     (uint64_t)p->auth_token_count +
                     (p->has_subgroup_delivery_timeout ? 1u : 0u) +
                     (p->has_expires ? 1u : 0u) +
                     (p->has_largest ? 1u : 0u) +
                     (p->has_forward ? 1u : 0u) +
                     (p->has_subscriber_priority ? 1u : 0u) +
                     (p->has_filter ? 1u : 0u) +
                     (p->has_group_order ? 1u : 0u) +
                     (p->has_new_group_request ? 1u : 0u);
    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, count);
    if (rc < 0) return rc;
    /* Ascending Type-Delta order: OBJECT_DELIVERY_TIMEOUT (0x02),
     * AUTHORIZATION_TOKEN (0x03, repeatable), SUBGROUP_DELIVERY_TIMEOUT (0x06),
     * EXPIRES (0x08), LARGEST_OBJECT (0x09), FORWARD (0x10), SUBSCRIBER_PRIORITY
     * (0x20), SUBSCRIPTION_FILTER (0x21), GROUP_ORDER (0x22),
     * NEW_GROUP_REQUEST (0x32). The delta is the type minus the previous type
     * (the type itself for the first parameter; zero for a repeated
     * AUTHORIZATION_TOKEN). */
    uint64_t prev = 0;
    if (p->has_object_delivery_timeout) {
        if ((rc = moq_buf_write_vi64(w,
                MOQ_D18_PARAM_OBJECT_DELIVERY_TIMEOUT - prev)) < 0) goto fail;
        if ((rc = moq_buf_write_vi64(w, p->object_delivery_timeout_ms)) < 0)
            goto fail;
        prev = MOQ_D18_PARAM_OBJECT_DELIVERY_TIMEOUT;
    }
    /* AUTHORIZATION_TOKEN (0x03) is repeatable: emit each token, the first as a
     * delta from the previous type and the rest as a zero delta. */
    for (size_t i = 0; i < p->auth_token_count; i++) {
        if ((rc = moq_buf_write_vi64(w,
                MOQ_D18_PARAM_AUTHORIZATION_TOKEN - prev)) < 0) goto fail;
        if ((rc = d18_write_auth_token(w, &p->auth_tokens[i])) < 0) goto fail;
        prev = MOQ_D18_PARAM_AUTHORIZATION_TOKEN;
    }
    if (p->has_subgroup_delivery_timeout) {
        if ((rc = moq_buf_write_vi64(w,
                MOQ_D18_PARAM_SUBGROUP_DELIVERY_TIMEOUT - prev)) < 0) goto fail;
        if ((rc = moq_buf_write_vi64(w, p->subgroup_delivery_timeout_ms)) < 0)
            goto fail;
        prev = MOQ_D18_PARAM_SUBGROUP_DELIVERY_TIMEOUT;
    }
    if (p->has_expires) {
        if ((rc = moq_buf_write_vi64(w, MOQ_D18_PARAM_EXPIRES - prev)) < 0)
            goto fail;
        if ((rc = moq_buf_write_vi64(w, p->expires_ms)) < 0) goto fail;
        prev = MOQ_D18_PARAM_EXPIRES;
    }
    if (p->has_largest) {
        if ((rc = moq_buf_write_vi64(w, MOQ_D18_PARAM_LARGEST_OBJECT - prev)) < 0)
            goto fail;
        if ((rc = moq_buf_write_vi64(w, p->largest_group)) < 0) goto fail;
        if ((rc = moq_buf_write_vi64(w, p->largest_object)) < 0) goto fail;
        prev = MOQ_D18_PARAM_LARGEST_OBJECT;
    }
    if (p->has_forward) {
        if ((rc = moq_buf_write_vi64(w, MOQ_D18_PARAM_FORWARD - prev)) < 0)
            goto fail;
        if ((rc = d18_write_u8(w, p->forward)) < 0) goto fail;
        prev = MOQ_D18_PARAM_FORWARD;
    }
    if (p->has_subscriber_priority) {
        if ((rc = moq_buf_write_vi64(w,
                MOQ_D18_PARAM_SUBSCRIBER_PRIORITY - prev)) < 0) goto fail;
        if ((rc = d18_write_u8(w, p->subscriber_priority)) < 0) goto fail;
        prev = MOQ_D18_PARAM_SUBSCRIBER_PRIORITY;
    }
    if (p->has_filter) {
        if ((rc = moq_buf_write_vi64(w,
                MOQ_D18_PARAM_SUBSCRIPTION_FILTER - prev)) < 0) goto fail;
        if ((rc = d18_write_filter(w, p)) < 0) goto fail;
        prev = MOQ_D18_PARAM_SUBSCRIPTION_FILTER;
    }
    if (p->has_group_order) {
        if ((rc = moq_buf_write_vi64(w,
                MOQ_D18_PARAM_GROUP_ORDER - prev)) < 0) goto fail;
        if ((rc = d18_write_u8(w, p->group_order)) < 0) goto fail;
        prev = MOQ_D18_PARAM_GROUP_ORDER;
    }
    if (p->has_new_group_request) {
        if ((rc = moq_buf_write_vi64(w,
                MOQ_D18_PARAM_NEW_GROUP_REQUEST - prev)) < 0) goto fail;
        if ((rc = moq_buf_write_vi64(w, p->new_group_request)) < 0) goto fail;
        prev = MOQ_D18_PARAM_NEW_GROUP_REQUEST;
    }
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_msg_params(moq_buf_reader_t *r, uint64_t count,
                                       uint32_t allowed_mask,
                                       moq_d18_msg_params_t *out)
{
    if (!r || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    uint64_t prev = 0;
    for (uint64_t i = 0; i < count; i++) {
        uint64_t delta;
        if (moq_buf_read_vi64(r, &delta) < 0) return MOQ_ERR_BUFFER;
        uint64_t type;
        if (delta == 0) {
            /* A zero delta repeats the previous Parameter Type. Only
             * AUTHORIZATION_TOKEN may repeat (§10.2.2); any other zero delta is
             * a duplicate or out-of-order parameter. */
            if (prev != MOQ_D18_PARAM_AUTHORIZATION_TOKEN ||
                !(allowed_mask & MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN))
                return MOQ_ERR_PROTO;
            type = prev;
        } else {
            if (delta > UINT64_MAX - prev) return MOQ_ERR_PROTO;
            type = prev + delta;
        }
        prev = type;
        switch (type) {
        case MOQ_D18_PARAM_OBJECT_DELIVERY_TIMEOUT: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_OBJECT_DELIVERY_TIMEOUT))
                return MOQ_ERR_PROTO;
            if (moq_buf_read_vi64(r, &out->object_delivery_timeout_ms) < 0)
                return MOQ_ERR_BUFFER;
            out->has_object_delivery_timeout = true;
            break;
        }
        case MOQ_D18_PARAM_AUTHORIZATION_TOKEN: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_AUTHORIZATION_TOKEN))
                return MOQ_ERR_PROTO;
            if (out->auth_token_count >= MOQ_D18_MAX_AUTH_TOKENS)
                return MOQ_ERR_PROTO;       /* too many auth tokens */
            moq_result_t arc = d18_read_auth_token(
                r, &out->auth_tokens[out->auth_token_count]);
            if (arc < 0) return arc;        /* may be MOQ_D18_ERR_KVP_FORMAT */
            out->auth_token_count++;
            break;
        }
        case MOQ_D18_PARAM_SUBGROUP_DELIVERY_TIMEOUT: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_SUBGROUP_DELIVERY_TIMEOUT))
                return MOQ_ERR_PROTO;
            if (moq_buf_read_vi64(r, &out->subgroup_delivery_timeout_ms) < 0)
                return MOQ_ERR_BUFFER;
            out->has_subgroup_delivery_timeout = true;
            break;
        }
        case MOQ_D18_PARAM_EXPIRES: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_EXPIRES))
                return MOQ_ERR_PROTO;
            if (moq_buf_read_vi64(r, &out->expires_ms) < 0) return MOQ_ERR_BUFFER;
            out->has_expires = true;
            break;
        }
        case MOQ_D18_PARAM_LARGEST_OBJECT: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_LARGEST_OBJECT))
                return MOQ_ERR_PROTO;
            if (moq_buf_read_vi64(r, &out->largest_group) < 0)
                return MOQ_ERR_BUFFER;
            if (moq_buf_read_vi64(r, &out->largest_object) < 0)
                return MOQ_ERR_BUFFER;
            out->has_largest = true;
            break;
        }
        case MOQ_D18_PARAM_FORWARD: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_FORWARD))
                return MOQ_ERR_PROTO;
            uint8_t v;
            if (d18_read_u8(r, &v) < 0) return MOQ_ERR_BUFFER;
            if (v > 1) return MOQ_ERR_PROTO;     /* §10.2.12: only 0 or 1 */
            out->has_forward = true;
            out->forward = v;
            break;
        }
        case MOQ_D18_PARAM_SUBSCRIBER_PRIORITY: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_SUBSCRIBER_PRIORITY))
                return MOQ_ERR_PROTO;
            uint8_t v;
            if (d18_read_u8(r, &v) < 0) return MOQ_ERR_BUFFER;
            out->has_subscriber_priority = true;
            out->subscriber_priority = v;
            break;
        }
        case MOQ_D18_PARAM_SUBSCRIPTION_FILTER: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_SUBSCRIPTION_FILTER))
                return MOQ_ERR_PROTO;
            moq_result_t frc = d18_read_filter(r, out);
            if (frc < 0) return frc;
            break;
        }
        case MOQ_D18_PARAM_GROUP_ORDER: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_GROUP_ORDER))
                return MOQ_ERR_PROTO;
            uint8_t v;
            if (d18_read_u8(r, &v) < 0) return MOQ_ERR_BUFFER;
            if (v < 1 || v > 2) return MOQ_ERR_PROTO;  /* §10.2.8: 1 or 2 */
            out->has_group_order = true;
            out->group_order = v;
            break;
        }
        /* Defined-but-unmodeled parameters (§10.2.5/6/14): a defined parameter
         * must not be treated as unknown (that closes the session), so its value
         * is decoded and form-validated, then dropped. Scope masks still apply
         * (§10.2.1: a defined parameter in the wrong message closes).
         * NEW_GROUP_REQUEST (§10.2.13) is modeled and stored below. */
        case MOQ_D18_PARAM_RENDEZVOUS_TIMEOUT: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_RENDEZVOUS_TIMEOUT))
                return MOQ_ERR_PROTO;
            uint64_t v;
            if (moq_buf_read_vi64(r, &v) < 0) return MOQ_ERR_BUFFER;
            break;
        }
        case MOQ_D18_PARAM_FILL_TIMEOUT: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_FILL_TIMEOUT))
                return MOQ_ERR_PROTO;
            uint64_t v;
            if (moq_buf_read_vi64(r, &v) < 0) return MOQ_ERR_BUFFER;
            break;
        }
        case MOQ_D18_PARAM_NEW_GROUP_REQUEST: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_NEW_GROUP_REQUEST))
                return MOQ_ERR_PROTO;
            if (out->has_new_group_request) return MOQ_ERR_PROTO;  /* dup */
            uint64_t v;
            if (moq_buf_read_vi64(r, &v) < 0) return MOQ_ERR_BUFFER;
            out->has_new_group_request = true;
            out->new_group_request = v;
            break;
        }
        case MOQ_D18_PARAM_TRACK_NAMESPACE_PREFIX: {
            if (!(allowed_mask & MOQ_D18_PARAM_BIT_TRACK_NAMESPACE_PREFIX))
                return MOQ_ERR_PROTO;
            /* Track Namespace Prefix (§10.2.14): a Track Namespace with 0..32
             * fields -- a zero-field (root) prefix is legal. Validate + skip. */
            uint64_t nparts;
            if (moq_buf_read_vi64(r, &nparts) < 0) return MOQ_ERR_BUFFER;
            if (nparts > 32) return MOQ_ERR_PROTO;
            for (uint64_t pi = 0; pi < nparts; pi++) {
                uint64_t plen;
                if (moq_buf_read_vi64(r, &plen) < 0) return MOQ_ERR_BUFFER;
                if (plen == 0) return MOQ_ERR_PROTO;
                if (plen > moq_buf_reader_remaining(r)) return MOQ_ERR_BUFFER;
                r->pos += (size_t)plen;
            }
            break;
        }
        default:
            /* Unknown parameters cannot be skipped (§10.2). */
            return MOQ_ERR_PROTO;
        }
    }
    return MOQ_OK;
}

/* -- REQUEST_UPDATE (draft-18 §10.9) ------------------------------- */

moq_result_t moq_d18_encode_request_update(moq_buf_writer_t *w,
                                           uint64_t request_id,
                                           const moq_d18_msg_params_t *p)
{
    if (!w || !p) return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(p, D18_REQUEST_UPDATE_PARAM_MASK))
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_REQUEST_UPDATE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, request_id)) < 0) goto fail;
    if ((rc = moq_d18_encode_msg_params(w, p)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_request_update(const uint8_t *payload,
                                           size_t payload_len,
                                           moq_d18_request_update_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    uint64_t count;
    if ((rc = moq_buf_read_vi64(&r, &count)) < 0) return rc;
    /* A subscription REQUEST_UPDATE carries FORWARD / SUBSCRIBER_PRIORITY and
     * the delivery-timeout parameters. */
    if ((rc = moq_d18_decode_msg_params(&r, count,
            D18_REQUEST_UPDATE_PARAM_MASK, &out->params)) < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- REQUEST_OK (draft-18 §10.5) ----------------------------------- */

moq_result_t moq_d18_encode_request_ok(moq_buf_writer_t *w)
{
    if (!w) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_REQUEST_OK, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, 0)) < 0) goto fail;  /* 0 parameters */
    /* REQUEST_UPDATE_OK carries empty Track Properties (payload ends here). */
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_request_ok(const uint8_t *payload,
                                       size_t payload_len)
{
    if (!payload) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    uint64_t count;
    moq_result_t rc = moq_buf_read_vi64(&r, &count);
    if (rc < 0) return rc;
    /* Parameters and Track Properties are not modelled for REQUEST_UPDATE_OK
     * yet: accept only the zero-parameter, empty-properties form. */
    if (count != 0) return MOQ_ERR_PROTO;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- PUBLISH_DONE (draft-18 §10.11) -------------------------------- */

moq_result_t moq_d18_encode_publish_done(moq_buf_writer_t *w,
                                         uint64_t status_code,
                                         uint64_t stream_count,
                                         moq_bytes_t reason)
{
    if (!w) return MOQ_ERR_INVAL;
    if (reason.len > MOQ_D18_MAX_REASON) return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_PUBLISH_DONE, &len_off);
    if (rc < 0) return rc;
    if ((rc = moq_buf_write_vi64(w, status_code)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, stream_count)) < 0) goto fail;
    if ((rc = d18_write_span(w, reason)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_publish_done(const uint8_t *payload,
                                         size_t payload_len,
                                         moq_d18_publish_done_t *out)
{
    if (!payload || !out) return MOQ_ERR_INVAL;
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);
    moq_result_t rc = moq_buf_read_vi64(&r, &out->status_code);
    if (rc < 0) return rc;
    rc = moq_buf_read_vi64(&r, &out->stream_count);
    if (rc < 0) return rc;
    rc = d18_read_span(&r, &out->reason);
    if (rc < 0) return rc;
    if (out->reason.len > MOQ_D18_MAX_REASON) return MOQ_ERR_PROTO;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

/* -- SUBGROUP_HEADER (draft-18 §11.4.2) ---------------------------- */

bool moq_d18_subgroup_type_valid(uint8_t type)
{
    /* Form 0b0XX1XXXX: bit 7 clear, bit 4 set. */
    if ((type & 0x90u) != 0x10u) return false;
    /* SUBGROUP_ID_MODE 0b11 is reserved for future use. */
    if (((type & MOQ_D18_SUBGROUP_MASK_ID_MODE) >> 1) == 0x03u) return false;
    return true;
}

moq_result_t moq_d18_encode_subgroup_header(moq_buf_writer_t *w,
                                            const moq_d18_subgroup_header_t *hdr)
{
    if (!w || !hdr) return MOQ_ERR_INVAL;
    if (hdr->subgroup_id_mode > MOQ_SUBGROUP_ID_MODE_PRESENT)
        return MOQ_ERR_INVAL;

    uint8_t type = 0x10u;   /* bit 4 set */
    if (hdr->has_properties)  type |= MOQ_D18_SUBGROUP_BIT_PROPERTIES;
    type |= (uint8_t)((hdr->subgroup_id_mode & 0x03u) << 1);
    if (hdr->end_of_group)    type |= MOQ_D18_SUBGROUP_BIT_END_OF_GROUP;
    if (hdr->default_priority) type |= MOQ_D18_SUBGROUP_BIT_DEFAULT_PRIORITY;
    if (hdr->first_object)    type |= MOQ_D18_SUBGROUP_BIT_FIRST_OBJECT;

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

moq_result_t moq_d18_decode_subgroup_header(moq_buf_reader_t *r,
                                            moq_d18_subgroup_header_t *out)
{
    if (!r || !out) return MOQ_ERR_INVAL;

    size_t saved = r->pos;
    uint64_t type_v = 0;
    moq_result_t rc = moq_buf_read_vi64(r, &type_v);
    if (rc < 0) return rc;
    if (type_v > 0xFF || !moq_d18_subgroup_type_valid((uint8_t)type_v)) {
        r->pos = saved;
        return MOQ_ERR_PROTO;
    }

    uint8_t type = (uint8_t)type_v;
    memset(out, 0, sizeof(*out));
    out->type             = type;
    out->has_properties   = (type & MOQ_D18_SUBGROUP_BIT_PROPERTIES) != 0;
    out->subgroup_id_mode = (uint8_t)((type & MOQ_D18_SUBGROUP_MASK_ID_MODE) >> 1);
    out->end_of_group     = (type & MOQ_D18_SUBGROUP_BIT_END_OF_GROUP) != 0;
    out->default_priority = (type & MOQ_D18_SUBGROUP_BIT_DEFAULT_PRIORITY) != 0;
    out->first_object     = (type & MOQ_D18_SUBGROUP_BIT_FIRST_OBJECT) != 0;

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

static moq_result_t d18_write_location(moq_buf_writer_t *w,
                                       moq_d18_location_t loc)
{
    moq_result_t rc = moq_buf_write_vi64(w, loc.group);
    if (rc < 0) return rc;
    return moq_buf_write_vi64(w, loc.object);
}

static moq_result_t d18_read_location(moq_buf_reader_t *r,
                                      moq_d18_location_t *out)
{
    moq_result_t rc = moq_buf_read_vi64(r, &out->group);
    if (rc < 0) return rc;
    return moq_buf_read_vi64(r, &out->object);
}

moq_result_t moq_d18_encode_fetch(moq_buf_writer_t *w, const moq_d18_fetch_t *f)
{
    if (!w || !f) return MOQ_ERR_INVAL;
    if (f->fetch_type < MOQ_D18_FETCH_TYPE_STANDALONE ||
        f->fetch_type > MOQ_D18_FETCH_TYPE_ABSOLUTE)
        return MOQ_ERR_INVAL;
    if (!d18_params_within_mask(&f->params, D18_FETCH_PARAM_MASK))
        return MOQ_ERR_INVAL;

    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_FETCH, &len_off);
    if (rc < 0) { w->pos = saved; return rc; }
    if ((rc = moq_buf_write_vi64(w, f->request_id)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, f->fetch_type)) < 0) goto fail;
    if (f->fetch_type == MOQ_D18_FETCH_TYPE_STANDALONE) {
        if (d18_full_track_len(&f->track_namespace, f->track_name) >
            MOQ_D18_MAX_FULL_TRACK) {
            rc = MOQ_ERR_INVAL;
            goto fail;
        }
        if ((rc = d18_write_namespace(w, &f->track_namespace)) < 0) goto fail;
        if ((rc = d18_write_span(w, f->track_name)) < 0) goto fail;
        if ((rc = d18_write_location(w, f->start)) < 0) goto fail;
        if ((rc = d18_write_location(w, f->end)) < 0) goto fail;
    } else {
        if ((rc = moq_buf_write_vi64(w, f->joining_request_id)) < 0) goto fail;
        if ((rc = moq_buf_write_vi64(w, f->joining_start)) < 0) goto fail;
    }
    if ((rc = moq_d18_encode_msg_params(w, &f->params)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_fetch(const uint8_t *payload, size_t payload_len,
                                  moq_bytes_t *parts, size_t max_parts,
                                  moq_d18_fetch_t *out)
{
    if (!payload || !parts || !out) return MOQ_ERR_INVAL;
    memset(out, 0, sizeof(*out));
    moq_buf_reader_t r;
    moq_buf_reader_init(&r, payload, payload_len);

    moq_result_t rc = moq_buf_read_vi64(&r, &out->request_id);
    if (rc < 0) return rc;
    if ((rc = moq_buf_read_vi64(&r, &out->fetch_type)) < 0) return rc;
    if (out->fetch_type < MOQ_D18_FETCH_TYPE_STANDALONE ||
        out->fetch_type > MOQ_D18_FETCH_TYPE_ABSOLUTE)
        return MOQ_ERR_PROTO;

    if (out->fetch_type == MOQ_D18_FETCH_TYPE_STANDALONE) {
        rc = d18_read_namespace(&r, parts, max_parts, &out->track_namespace);
        if (rc < 0) return rc;
        if ((rc = d18_read_span(&r, &out->track_name)) < 0) return rc;
        if (d18_full_track_len(&out->track_namespace, out->track_name) >
            MOQ_D18_MAX_FULL_TRACK)
            return MOQ_ERR_PROTO;
        if ((rc = d18_read_location(&r, &out->start)) < 0) return rc;
        if ((rc = d18_read_location(&r, &out->end)) < 0) return rc;
    } else {
        if ((rc = moq_buf_read_vi64(&r, &out->joining_request_id)) < 0) return rc;
        if ((rc = moq_buf_read_vi64(&r, &out->joining_start)) < 0) return rc;
    }

    uint64_t param_count = 0;
    if ((rc = moq_buf_read_vi64(&r, &param_count)) < 0) return rc;
    /* FETCH carries SUBSCRIBER_PRIORITY / GROUP_ORDER and AUTHORIZATION_TOKEN
     * (no FORWARD or filter). */
    rc = moq_d18_decode_msg_params(&r, param_count, D18_FETCH_PARAM_MASK,
                                   &out->params);
    if (rc < 0) return rc;
    if (moq_buf_reader_remaining(&r) != 0) return MOQ_ERR_PROTO;
    return MOQ_OK;
}

moq_result_t moq_d18_encode_fetch_ok(moq_buf_writer_t *w, bool end_of_track,
                                     moq_d18_location_t end,
                                     moq_bytes_t track_properties)
{
    if (!w) return MOQ_ERR_INVAL;
    if (track_properties.len > 0 &&
        d18_validate_track_properties(track_properties.data,
                                      track_properties.len) < 0)
        return MOQ_ERR_INVAL;
    size_t saved = w->pos, len_off;
    moq_result_t rc = d18_write_header(w, MOQ_D18_FETCH_OK, &len_off);
    if (rc < 0) { w->pos = saved; return rc; }
    uint8_t eot = end_of_track ? 1u : 0u;
    if ((rc = moq_buf_write_raw(w, &eot, 1)) < 0) goto fail;
    if ((rc = d18_write_location(w, end)) < 0) goto fail;
    if ((rc = moq_buf_write_vi64(w, 0)) < 0) goto fail;   /* zero parameters */
    if (track_properties.len > 0 &&
        (rc = moq_buf_write_raw(w, track_properties.data,
                                track_properties.len)) < 0) goto fail;
    if ((rc = d18_patch_len(w, len_off)) < 0) goto fail;
    return MOQ_OK;
fail:
    w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_fetch_ok(const uint8_t *payload, size_t payload_len,
                                     moq_d18_fetch_ok_t *out)
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
    if ((rc = d18_read_location(&r, &out->end)) < 0) return rc;
    uint64_t param_count = 0;
    if ((rc = moq_buf_read_vi64(&r, &param_count)) < 0) return rc;
    if (param_count != 0) return MOQ_ERR_PROTO;   /* no FETCH_OK params in scope */
    /* The remainder is the opaque Track Properties tail. */
    out->track_properties.data = moq_buf_reader_ptr(&r);
    out->track_properties.len = moq_buf_reader_remaining(&r);
    return d18_scan_track_properties(out->track_properties.data,
                                     out->track_properties.len,
                                     &out->track_properties_unsupported,
                                     NULL);
}

moq_result_t moq_d18_encode_fetch_header(moq_buf_writer_t *w, uint64_t request_id)
{
    if (!w) return MOQ_ERR_INVAL;
    size_t saved = w->pos;
    moq_result_t rc = moq_buf_write_vi64(w, MOQ_D18_STREAM_FETCH_HEADER);
    if (rc == MOQ_OK)
        rc = moq_buf_write_vi64(w, request_id);
    if (rc < 0) w->pos = saved;
    return rc;
}

moq_result_t moq_d18_decode_fetch_header(moq_buf_reader_t *r,
                                         uint64_t *out_request_id)
{
    if (!r || !out_request_id) return MOQ_ERR_INVAL;
    size_t saved = r->pos;
    uint64_t type = 0;
    moq_result_t rc = moq_buf_read_vi64(r, &type);
    if (rc < 0) { r->pos = saved; return rc; }
    if (type != MOQ_D18_STREAM_FETCH_HEADER) { r->pos = saved; return MOQ_ERR_PROTO; }
    if ((rc = moq_buf_read_vi64(r, out_request_id)) < 0) { r->pos = saved; return rc; }
    return MOQ_OK;
}
