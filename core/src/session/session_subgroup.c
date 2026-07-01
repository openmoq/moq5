#include "session_internal.h"

static bool sg_send_allowed(moq_session_t *s, const moq_sg_entry_t *sg)
{
    if (moq_subscription_is_valid(sg->sub)) {
        int slot = sub_resolve_handle(s, sg->sub);
        if (slot < 0) return false;
        return s->subs[slot].forward;
    }
    if (moq_publication_is_valid(sg->pub)) {
        int slot = pub_resolve_handle(s, sg->pub);
        if (slot < 0) return false;
        return s->publishes[slot].send_allowed;
    }
    return false;
}

/* -- Subgroup pool ------------------------------------------------- */

int sg_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->sg_cap; i++)
        if (s->subgroups[i].state == MOQ_SG_FREE) return (int)i;
    return -1;
}

static bool sg_is_duplicate(moq_session_t *s, moq_subscription_t sub,
                             uint64_t group_id, uint64_t subgroup_id)
{
    for (size_t i = 0; i < s->sg_cap; i++) {
        moq_sg_entry_t *e = &s->subgroups[i];
        if (e->state == MOQ_SG_FREE) continue;
        if (moq_subscription_eq(e->sub, sub) &&
            e->group_id == group_id && e->subgroup_id == subgroup_id)
            return true;
    }
    return false;
}

moq_subgroup_handle_t sg_make_handle(moq_session_t *s, size_t slot)
{
    moq_sg_entry_t *e = &s->subgroups[slot];
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_SUBGROUP,
                                       s->session_tag,
                                       e->generation, (uint32_t)slot);
    moq_subgroup_handle_t h = { packed };
    return h;
}

/* Returns slot if handle is valid and state matches, -1 if stale, -2 if
 * handle is valid but in a different state (for WRONG_STATE errors). */
static int sg_resolve_handle_ex(moq_session_t *s, moq_subgroup_handle_t h,
                                 moq_sg_state_t required)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_SUBGROUP) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->sg_cap) return -1;
    if (s->subgroups[slot].generation != gen) return -1;
    if (s->subgroups[slot].state != required) return -2;
    return (int)slot;
}

static int sg_resolve_handle(moq_session_t *s, moq_subgroup_handle_t h)
{
    int r = sg_resolve_handle_ex(s, h, MOQ_SG_OPEN);
    return r == -2 ? -1 : r;
}

static int sg_find_by_stream_ref(moq_session_t *s, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->sg_cap; i++) {
        moq_sg_entry_t *e = &s->subgroups[i];
        if (e->state != MOQ_SG_FREE && e->stream_ref._v == ref._v)
            return (int)i;
    }
    return -1;
}

void sg_free_entry(size_t slot, moq_sg_entry_t *entries)
{
    entries[slot].delivery_deadline_us = UINT64_MAX;
    entries[slot].state = MOQ_SG_FREE;
    entries[slot].generation++;
    /* Clear ownership handles so a reused slot can never be matched by an
     * owner scan (e.g. session_core_on_publish_unsubscribed) via a stale
     * pub/sub left over from the slot's previous occupant. */
    entries[slot].sub = MOQ_SUBSCRIPTION_INVALID;
    entries[slot].pub = MOQ_PUBLICATION_INVALID;
}

void sg_recompute_deadline(moq_session_t *s)
{
    uint64_t d = UINT64_MAX;
    for (size_t i = 0; i < s->sg_cap; i++)
        if (s->subgroups[i].state != MOQ_SG_FREE &&
            s->subgroups[i].delivery_deadline_us < d)
            d = s->subgroups[i].delivery_deadline_us;
    s->subgroup_deadline_us = d;
}

void sg_reap_terminal(moq_session_t *s)
{
    bool reaped = false;
    for (size_t i = 0; i < s->sg_cap; i++) {
        if (s->subgroups[i].state == MOQ_SG_CLOSING ||
            s->subgroups[i].state == MOQ_SG_RESETTING) {
            sg_free_entry(i, s->subgroups);
            reaped = true;
        }
    }
    if (reaped) sg_recompute_deadline(s);
}

/* -- Subgroup handle helpers --------------------------------------- */

static bool sg_handle_valid(uint64_t h)
{
    if (h == 0) return false;
    uint32_t pool = moq_handle_pool_tag(h);
    if (pool != MOQ_HANDLE_POOL_SUBGROUP) return false;
    uint16_t tag = moq_handle_session_tag(h);
    if (tag == 0) return false;
    uint32_t gen = moq_handle_generation(h);
    return (gen & 1u) != 0;
}

bool moq_subgroup_is_valid(moq_subgroup_handle_t h)
{
    return sg_handle_valid(h._opaque);
}

bool moq_subgroup_eq(moq_subgroup_handle_t a, moq_subgroup_handle_t b)
{
    return a._opaque == b._opaque;
}

uint64_t moq_subgroup_hash(moq_subgroup_handle_t h)
{
    uint64_t x = h._opaque;
    x ^= x >> 33;
    x *= 0xFF51AFD7ED558CCDULL;
    x ^= x >> 33;
    return x;
}

uint64_t moq_subgroup_id_for_trace(moq_subgroup_handle_t h)
{
    return h._opaque;
}

void moq_subgroup_cfg_init(moq_subgroup_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_subgroup_cfg_t);
}

/* -- Subgroup data path -------------------------------------------- */

moq_result_t moq_session_open_subgroup(
    moq_session_t *s, moq_subscription_t sub,
    const moq_subgroup_cfg_t *cfg, uint64_t now_us,
    moq_subgroup_handle_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_subgroup_cfg_t, publisher_priority) +
        sizeof(cfg->publisher_priority)) return MOQ_ERR_INVAL;
    if (cfg->group_id > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    if (cfg->subgroup_id > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    *out_handle = MOQ_SUBGROUP_INVALID;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int sub_slot = sub_resolve_handle(s, sub);
    if (sub_slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[sub_slot].state != MOQ_SUB_ESTABLISHED) return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].role != MOQ_SUB_ROLE_PUBLISHER) return MOQ_ERR_WRONG_STATE;
    if (!s->subs[sub_slot].forward) return MOQ_ERR_WRONG_STATE;

    if (sg_is_duplicate(s, sub, cfg->group_id, cfg->subgroup_id))
        return MOQ_ERR_INVAL;

    int slot = sg_find_free(s);
    if (slot < 0) return MOQ_ERR_WOULD_BLOCK;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    bool has_ext = false;
    if (cfg->struct_size >= offsetof(moq_subgroup_cfg_t, object_properties) +
        sizeof(cfg->object_properties))
        has_ext = cfg->object_properties;

    bool eog = false;
    if (cfg->struct_size >= offsetof(moq_subgroup_cfg_t, end_of_group) +
        sizeof(cfg->end_of_group))
        eog = cfg->end_of_group;

    moq_subgroup_header_encode_args_t hdr_args = {
        .track_alias = s->subs[sub_slot].track_alias,
        .group_id = cfg->group_id,
        .subgroup_id = cfg->subgroup_id,
        .publisher_priority = cfg->publisher_priority,
        .has_extensions = has_ext,
        .end_of_group = eog,
    };

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_subgroup_header(s, &hw, &hdr_args);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);

    a.u.send_data.stream_ref = moq_stream_ref_from_u64(s->next_stream_ref);
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = false;

    rc = push_action(s, &a);
    if (rc < 0) return rc;

    /* Commit. */
    moq_sg_entry_t *entry = &s->subgroups[slot];
    entry->generation |= 1;
    entry->state = MOQ_SG_OPEN;
    entry->sub = sub;
    entry->pub = MOQ_PUBLICATION_INVALID;   /* subscription-backed: no owning
                                             * publication (mirror of the
                                             * publication path setting sub
                                             * INVALID) so an owner scan never
                                             * matches a stale reused handle. */
    entry->stream_ref = a.u.send_data.stream_ref;
    entry->group_id = cfg->group_id;
    entry->subgroup_id = cfg->subgroup_id;
    entry->has_prev_object = false;
    entry->has_extensions = has_ext;
    if (s->subs[sub_slot].delivery_timeout_us > 0) {
        entry->delivery_deadline_us = deadline_add(now_us,
            s->subs[sub_slot].delivery_timeout_us);
        if (entry->delivery_deadline_us < s->subgroup_deadline_us)
            s->subgroup_deadline_us = entry->delivery_deadline_us;
    } else {
        entry->delivery_deadline_us = UINT64_MAX;
    }
    s->next_stream_ref++;

    *out_handle = sg_make_handle(s, (size_t)slot);
    return MOQ_OK;
}

moq_result_t moq_session_write_object(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t object_id, moq_rcbuf_t *payload, uint64_t now_us)
{
    if (!s || !payload) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    /* Monotonically increasing object IDs; gaps legal. */
    if (entry->has_prev_object && object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;

    size_t payload_len = moq_rcbuf_len(payload);
    if (payload_len > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_object_header_encode_args_t obj_args = {
        .object_id = object_id,
        .prev_object_id = entry->prev_object_id,
        .has_prev_object = entry->has_prev_object,
        .payload_len = payload_len,
        /* On an extension-enabled subgroup, an object with no properties still
         * encodes a zero-length extensions field (matches *_ex with no
         * properties); omitting it would emit the non-extension header on a
         * stream whose subgroup header promised extensions. */
        .has_extensions = entry->has_extensions,
    };

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);

    a.u.send_data.stream_ref = entry->stream_ref;
    a.u.send_data.payload = payload;
    a.u.send_data.fin = false;

    /* Incref AFTER all validation succeeds. */
    moq_rcbuf_incref(payload);

    rc = push_action(s, &a);
    if (rc < 0) {
        moq_rcbuf_decref(payload);
        return rc;
    }

    /* Commit. */
    entry->prev_object_id = object_id;
    entry->has_prev_object = true;
    return MOQ_OK;
}

/* Write a zero-length terminal status object on a subgroup stream (the reliable,
 * non-datagram counterpart to send_status_datagram). END_OF_GROUP / END_OF_TRACK
 * only -- NORMAL has no zero-length stream meaning. The terminal object carries
 * no properties, but on an extension-enabled subgroup it still encodes the
 * zero-length extensions field (a zero-length extension block on a non-Normal
 * stream object is legal). The caller closes the subgroup afterwards
 * (END_OF_TRACK ends the track; the stream FIN is the close). */
moq_result_t moq_session_write_status_object(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t object_id, moq_object_status_t status, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    uint64_t wire;
    if (status == MOQ_OBJECT_END_OF_GROUP) wire = 0x3;
    else if (status == MOQ_OBJECT_END_OF_TRACK) wire = 0x4;
    else return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;
    if (entry->has_prev_object && object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_object_header_encode_args_t obj_args = {
        .object_id = object_id,
        .prev_object_id = entry->prev_object_id,
        .has_prev_object = entry->has_prev_object,
        .payload_len = 0,
        .object_status = wire,
        /* Encode the zero-length extensions field on an extension-enabled
         * subgroup so the terminal status object matches the stream's promised
         * header shape. */
        .has_extensions = entry->has_extensions,
    };

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
    a.u.send_data.stream_ref = entry->stream_ref;
    a.u.send_data.payload = NULL;   /* zero-length status object */
    a.u.send_data.fin = false;

    rc = push_action(s, &a);
    if (rc < 0) return rc;

    entry->prev_object_id = object_id;
    entry->has_prev_object = true;
    return MOQ_OK;
}

void moq_object_cfg_init(moq_object_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_session_write_object_ex(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    const moq_object_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_object_cfg_t)) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    if (entry->has_prev_object && cfg->object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;

    size_t payload_len = cfg->payload ? moq_rcbuf_len(cfg->payload) : 0;
    if (payload_len > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    size_t props_len = cfg->properties ? moq_rcbuf_len(cfg->properties) : 0;

    /* Properties require extensions-enabled subgroup. */
    if (props_len > 0 && !entry->has_extensions) return MOQ_ERR_INVAL;

    /* Refuse to emit object properties the draft forbids (draft-18: a Mandatory
     * Track Property carried as an object property is malformed) -- symmetric
     * with the inbound check, so we never put unreadable wire on the network. */
    if (props_len > 0 && s->profile->validate_object_properties &&
        s->profile->validate_object_properties(
            s, moq_rcbuf_data(cfg->properties), props_len) < 0)
        return MOQ_ERR_INVAL;

    bool has_props = (props_len > 0);
    size_t slots_needed = has_props ? 2 : 1;
    if (action_queue_avail(s) < slots_needed) return MOQ_ERR_WOULD_BLOCK;

    if (has_props) {
        /* Two-action pattern: header+properties, then payload_len+payload. */
        moq_object_header_encode_args_t hdr_args = {
            .object_id = cfg->object_id,
            .prev_object_id = entry->prev_object_id,
            .has_prev_object = entry->has_prev_object,
            .has_extensions = true,
            .properties_len = props_len,
            .header_only = true,
        };

        moq_action_t a1;
        memset(&a1, 0, sizeof(a1));
        a1.kind = MOQ_ACTION_SEND_DATA;
        a1.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a1.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw1;
        moq_buf_writer_init(&hw1, a1.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_object_header(s, &hw1, &hdr_args);
        if (rc < 0) return rc;
        a1.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw1);
        a1.u.send_data.stream_ref = entry->stream_ref;
        a1.u.send_data.payload = cfg->properties;
        a1.u.send_data.fin = false;

        moq_action_t a2;
        memset(&a2, 0, sizeof(a2));
        a2.kind = MOQ_ACTION_SEND_DATA;
        a2.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a2.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw2;
        moq_buf_writer_init(&hw2, a2.u.send_data.header, 32);
        rc = s->profile->encode_object_payload_prefix(s, &hw2, payload_len, true);
        if (rc < 0) return rc;
        a2.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw2);
        a2.u.send_data.stream_ref = entry->stream_ref;
        a2.u.send_data.payload = cfg->payload;
        a2.u.send_data.fin = false;

        moq_rcbuf_incref(cfg->properties);
        if (cfg->payload) moq_rcbuf_incref(cfg->payload);

        rc = push_action(s, &a1);
        if (rc < 0) {
            moq_rcbuf_decref(cfg->properties);
            if (cfg->payload) moq_rcbuf_decref(cfg->payload);
            return rc;
        }
        rc = push_action(s, &a2);
        if (rc < 0) {
            if (cfg->payload) moq_rcbuf_decref(cfg->payload);
            return rc;
        }
    } else {
        /* Single-action: encode full object header + payload. */
        moq_object_header_encode_args_t obj_args = {
            .object_id = cfg->object_id,
            .prev_object_id = entry->prev_object_id,
            .has_prev_object = entry->has_prev_object,
            .payload_len = payload_len,
            .has_extensions = entry->has_extensions,
        };

        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_SEND_DATA;
        a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw;
        moq_buf_writer_init(&hw, a.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
        if (rc < 0) return rc;
        a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
        a.u.send_data.stream_ref = entry->stream_ref;
        a.u.send_data.payload = cfg->payload;
        a.u.send_data.fin = false;

        if (cfg->payload) moq_rcbuf_incref(cfg->payload);

        rc = push_action(s, &a);
        if (rc < 0) {
            if (cfg->payload) moq_rcbuf_decref(cfg->payload);
            return rc;
        }
    }

    entry->prev_object_id = cfg->object_id;
    entry->has_prev_object = true;
    return MOQ_OK;
}

moq_result_t moq_session_close_subgroup(
    moq_session_t *s, moq_subgroup_handle_t subgroup, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_data.stream_ref = s->subgroups[slot].stream_ref;
    a.u.send_data.header_len = 0;
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = true;

    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->subgroups[slot].state = MOQ_SG_CLOSING;
    if (s->subgroups[slot].delivery_deadline_us != UINT64_MAX) {
        s->subgroups[slot].delivery_deadline_us = UINT64_MAX;
        sg_recompute_deadline(s);
    }
    return MOQ_OK;
}

moq_result_t moq_session_reset_subgroup(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t error_code, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (error_code > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot < 0) {
        slot = sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING);
        if (slot == -2) return MOQ_ERR_STALE_HANDLE;
        if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    }

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_data.stream_ref = s->subgroups[slot].stream_ref;
    a.u.reset_data.error_code = error_code;

    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->subgroups[slot].state = MOQ_SG_RESETTING;
    s->subgroups[slot].streaming_payload_len = 0;
    s->subgroups[slot].streaming_bytes_written = 0;
    if (s->subgroups[slot].delivery_deadline_us != UINT64_MAX) {
        s->subgroups[slot].delivery_deadline_us = UINT64_MAX;
        sg_recompute_deadline(s);
    }
    return MOQ_OK;
}

/* -- Streaming object send ----------------------------------------- */

moq_result_t moq_session_begin_object(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t object_id, uint64_t payload_length, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    if (entry->has_prev_object && object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;

    if (payload_length > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_object_header_encode_args_t obj_args = {
        .object_id = object_id,
        .prev_object_id = entry->prev_object_id,
        .has_prev_object = entry->has_prev_object,
        .payload_len = payload_length,
        /* Match *_ex: an extension-enabled subgroup encodes a zero-length
         * extensions field even when the streamed object carries no properties. */
        .has_extensions = entry->has_extensions,
    };

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);

    a.u.send_data.stream_ref = entry->stream_ref;
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = false;

    rc = push_action(s, &a);
    if (rc < 0) return rc;

    entry->prev_object_id = object_id;
    entry->has_prev_object = true;
    entry->state = MOQ_SG_STREAMING;
    entry->streaming_payload_len = payload_length;
    entry->streaming_bytes_written = 0;
    return MOQ_OK;
}

void moq_begin_object_cfg_init(moq_begin_object_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_session_begin_object_ex(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    const moq_begin_object_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_begin_object_cfg_t)) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle(s, subgroup);
    if (slot < 0) {
        if (sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING) >= 0)
            return MOQ_ERR_WRONG_STATE;
        return MOQ_ERR_STALE_HANDLE;
    }

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    if (entry->has_prev_object && cfg->object_id <= entry->prev_object_id)
        return MOQ_ERR_INVAL;

    if (cfg->payload_length > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    size_t props_len = cfg->properties ? moq_rcbuf_len(cfg->properties) : 0;
    if (props_len > 0 && !entry->has_extensions) return MOQ_ERR_INVAL;
    if (props_len > 0 && s->profile->validate_object_properties &&
        s->profile->validate_object_properties(
            s, moq_rcbuf_data(cfg->properties), props_len) < 0)
        return MOQ_ERR_INVAL;

    bool has_props = (props_len > 0);
    size_t slots_needed = has_props ? 2 : 1;
    if (action_queue_avail(s) < slots_needed) return MOQ_ERR_WOULD_BLOCK;

    if (has_props) {
        /* Two-action: header(delta+extensions_len)+properties, then payload_len. */
        moq_object_header_encode_args_t hdr_args = {
            .object_id = cfg->object_id,
            .prev_object_id = entry->prev_object_id,
            .has_prev_object = entry->has_prev_object,
            .has_extensions = true,
            .properties_len = props_len,
            .header_only = true,
        };

        moq_action_t a1;
        memset(&a1, 0, sizeof(a1));
        a1.kind = MOQ_ACTION_SEND_DATA;
        a1.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a1.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw1;
        moq_buf_writer_init(&hw1, a1.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_object_header(s, &hw1, &hdr_args);
        if (rc < 0) return rc;
        a1.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw1);
        a1.u.send_data.stream_ref = entry->stream_ref;
        a1.u.send_data.payload = cfg->properties;
        a1.u.send_data.fin = false;

        moq_action_t a2;
        memset(&a2, 0, sizeof(a2));
        a2.kind = MOQ_ACTION_SEND_DATA;
        a2.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a2.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw2;
        moq_buf_writer_init(&hw2, a2.u.send_data.header, 32);
        rc = s->profile->encode_object_payload_prefix(s, &hw2,
                                                      cfg->payload_length, true);
        if (rc < 0) return rc;
        a2.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw2);
        a2.u.send_data.stream_ref = entry->stream_ref;
        a2.u.send_data.payload = NULL;
        a2.u.send_data.fin = false;

        moq_rcbuf_incref(cfg->properties);

        rc = push_action(s, &a1);
        if (rc < 0) {
            moq_rcbuf_decref(cfg->properties);
            return rc;
        }
        rc = push_action(s, &a2);
        if (rc < 0) return rc;
    } else {
        moq_object_header_encode_args_t obj_args = {
            .object_id = cfg->object_id,
            .prev_object_id = entry->prev_object_id,
            .has_prev_object = entry->has_prev_object,
            .payload_len = cfg->payload_length,
            .has_extensions = entry->has_extensions,
        };

        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_SEND_DATA;
        a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw;
        moq_buf_writer_init(&hw, a.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_object_header(s, &hw, &obj_args);
        if (rc < 0) return rc;
        a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
        a.u.send_data.stream_ref = entry->stream_ref;
        a.u.send_data.payload = NULL;
        a.u.send_data.fin = false;

        rc = push_action(s, &a);
        if (rc < 0) return rc;
    }

    entry->prev_object_id = cfg->object_id;
    entry->has_prev_object = true;
    entry->state = MOQ_SG_STREAMING;
    entry->streaming_payload_len = cfg->payload_length;
    entry->streaming_bytes_written = 0;
    return MOQ_OK;
}

moq_result_t moq_session_write_object_data(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    moq_rcbuf_t *data, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING);
    if (slot == -2) return MOQ_ERR_WRONG_STATE;
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    if (!data) return MOQ_ERR_INVAL;

    size_t data_len = moq_rcbuf_len(data);
    if (data_len == 0) return MOQ_OK;

    moq_sg_entry_t *entry = &s->subgroups[slot];
    if (!sg_send_allowed(s, entry)) return MOQ_ERR_WRONG_STATE;

    if (data_len > entry->streaming_payload_len -
        entry->streaming_bytes_written)
        return MOQ_ERR_INVAL;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_data.stream_ref = entry->stream_ref;
    a.u.send_data.header_len = 0;
    a.u.send_data.payload = data;
    a.u.send_data.fin = false;

    moq_rcbuf_incref(data);

    moq_result_t rc = push_action(s, &a);
    if (rc < 0) {
        moq_rcbuf_decref(data);
        return rc;
    }

    entry->streaming_bytes_written += data_len;
    return MOQ_OK;
}

moq_result_t moq_session_end_object(
    moq_session_t *s, moq_subgroup_handle_t subgroup,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_resolve_handle_ex(s, subgroup, MOQ_SG_STREAMING);
    if (slot == -2) return MOQ_ERR_WRONG_STATE;
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_sg_entry_t *entry = &s->subgroups[slot];

    if (entry->streaming_bytes_written != entry->streaming_payload_len)
        return MOQ_ERR_INVAL;

    entry->state = MOQ_SG_OPEN;
    entry->streaming_payload_len = 0;
    entry->streaming_bytes_written = 0;
    return MOQ_OK;
}

/* -- Datagram inbound handler -------------------------------------- */

moq_result_t session_core_on_object_datagram(moq_session_t *s,
    const moq_decoded_object_datagram_t *d,
    const uint8_t *payload_data, size_t payload_len,
    const uint8_t *props_data, size_t props_len)
{
    /* Apply the same receive limits as the stream/fetch object paths. A datagram
     * is a self-contained, unreliable unit, so an over-limit one is dropped
     * (no event, session stays established) rather than closing the session.
     * All arithmetic is overflow-safe against attacker-controlled lengths. */
    if (payload_len > s->max_obj_payload)
        return MOQ_ERR_WOULD_BLOCK;
    if (props_len > SIZE_MAX - payload_len)
        return MOQ_ERR_WOULD_BLOCK;
    size_t obj_budget = payload_len + props_len;
    if (s->recv_payload_bytes > s->max_recv_buf ||
        obj_budget > s->max_recv_buf - s->recv_payload_bytes)
        return MOQ_ERR_WOULD_BLOCK;

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_rcbuf_t *pay_rc = NULL;
    moq_rcbuf_t *prop_rc = NULL;

    if (payload_len > 0) {
        moq_result_t rc = moq_rcbuf_create(&s->alloc,
            payload_data, payload_len, &pay_rc);
        if (rc < 0) return rc;
    }
    if (props_len > 0) {
        moq_result_t rc = moq_rcbuf_create(&s->alloc,
            props_data, props_len, &prop_rc);
        if (rc < 0) {
            if (pay_rc) moq_rcbuf_decref(pay_rc);
            return rc;
        }
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_OBJECT_RECEIVED;
    e.detail_size = (uint32_t)sizeof(moq_object_received_event_t);
    e.borrow_epoch = s->borrow_epoch;

    moq_object_received_event_t *obj = &e.u.object_received;

    if (d->sub_slot >= 0)
        obj->sub = s->subs[d->sub_slot].handle;
    if (d->pub_slot >= 0)
        obj->pub = s->publishes[d->pub_slot].handle;

    obj->group_id           = d->group_id;
    obj->subgroup_id        = 0;
    obj->object_id          = d->object_id;
    obj->publisher_priority = d->publisher_priority;
    obj->status             = d->status;
    obj->end_of_group       = d->end_of_group;
    obj->datagram           = true;
    obj->payload            = pay_rc;
    obj->properties         = prop_rc;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        if (pay_rc) moq_rcbuf_decref(pay_rc);
        if (prop_rc) moq_rcbuf_decref(prop_rc);
        return rc;
    }

    s->recv_payload_bytes += obj_budget;
    return MOQ_OK;
}

/* -- Datagram outbound send ---------------------------------------- */

static moq_result_t send_datagram_encoded(moq_session_t *s,
    const moq_datagram_encode_args_t *args)
{
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);

    moq_result_t rc = s->profile->encode_object_datagram(s, &w, args);
    if (rc < 0) return rc;

    size_t elen = moq_buf_writer_offset(&w);
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATAGRAM;
    a.detail_size = (uint32_t)sizeof(moq_send_datagram_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_datagram.data = s->send_buf + s->send_len;
    a.u.send_datagram.len  = elen;

    rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->send_len += elen;
    return MOQ_OK;
}

moq_result_t moq_session_send_object_datagram(
    moq_session_t *s,
    moq_subscription_t sub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    bool end_of_group,
    moq_rcbuf_t *payload,
    const uint8_t *properties, size_t properties_len,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int sub_slot = sub_resolve_handle(s, sub);
    if (sub_slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[sub_slot].state != MOQ_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].role != MOQ_SUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (!s->subs[sub_slot].forward) return MOQ_ERR_WRONG_STATE;

    size_t plen = payload ? moq_rcbuf_len(payload) : 0;
    const uint8_t *pdata = payload ? moq_rcbuf_data(payload) : NULL;

    moq_datagram_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.track_alias        = s->subs[sub_slot].track_alias;
    args.group_id           = group_id;
    args.object_id          = object_id;
    args.publisher_priority = publisher_priority;
    args.end_of_group       = end_of_group;
    args.properties         = properties;
    args.properties_len     = properties_len;
    args.payload            = pdata;
    args.payload_len        = plen;

    return send_datagram_encoded(s, &args);
}

moq_result_t moq_session_send_status_datagram(
    moq_session_t *s,
    moq_subscription_t sub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    moq_object_status_t status,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int sub_slot = sub_resolve_handle(s, sub);
    if (sub_slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[sub_slot].state != MOQ_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->subs[sub_slot].role != MOQ_SUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (!s->subs[sub_slot].forward) return MOQ_ERR_WRONG_STATE;

    uint64_t wire_status;
    if (status == MOQ_OBJECT_NORMAL) wire_status = 0x0;
    else if (status == MOQ_OBJECT_END_OF_GROUP) wire_status = 0x3;
    else if (status == MOQ_OBJECT_END_OF_TRACK) wire_status = 0x4;
    else return MOQ_ERR_INVAL;

    moq_datagram_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.track_alias        = s->subs[sub_slot].track_alias;
    args.group_id           = group_id;
    args.object_id          = object_id;
    args.publisher_priority = publisher_priority;
    args.is_status          = true;
    args.object_status      = wire_status;

    return send_datagram_encoded(s, &args);
}

moq_result_t moq_session_send_pub_object_datagram(
    moq_session_t *s,
    moq_publication_t pub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    bool end_of_group,
    moq_rcbuf_t *payload,
    const uint8_t *properties, size_t properties_len,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->publishes[slot].role != MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (!s->publishes[slot].send_allowed)
        return MOQ_ERR_WRONG_STATE;

    size_t plen = payload ? moq_rcbuf_len(payload) : 0;
    const uint8_t *pdata = payload ? moq_rcbuf_data(payload) : NULL;

    moq_datagram_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.track_alias        = s->publishes[slot].track_alias;
    args.group_id           = group_id;
    args.object_id          = object_id;
    args.publisher_priority = publisher_priority;
    args.end_of_group       = end_of_group;
    args.properties         = properties;
    args.properties_len     = properties_len;
    args.payload            = pdata;
    args.payload_len        = plen;

    return send_datagram_encoded(s, &args);
}

moq_result_t moq_session_send_pub_status_datagram(
    moq_session_t *s,
    moq_publication_t pub,
    uint64_t group_id, uint64_t object_id,
    uint8_t publisher_priority,
    moq_object_status_t status,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->publishes[slot].role != MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (!s->publishes[slot].send_allowed)
        return MOQ_ERR_WRONG_STATE;

    uint64_t wire_status;
    if (status == MOQ_OBJECT_NORMAL) wire_status = 0x0;
    else if (status == MOQ_OBJECT_END_OF_GROUP) wire_status = 0x3;
    else if (status == MOQ_OBJECT_END_OF_TRACK) wire_status = 0x4;
    else return MOQ_ERR_INVAL;

    moq_datagram_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.track_alias        = s->publishes[slot].track_alias;
    args.group_id           = group_id;
    args.object_id          = object_id;
    args.publisher_priority = publisher_priority;
    args.is_status          = true;
    args.object_status      = wire_status;

    return send_datagram_encoded(s, &args);
}

moq_result_t moq_session_on_data_stop(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       uint64_t error_code,
                                       uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (error_code > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sg_find_by_stream_ref(s, stream_ref);
    if (slot < 0) return MOQ_OK;
    if (s->subgroups[slot].state != MOQ_SG_OPEN &&
        s->subgroups[slot].state != MOQ_SG_STREAMING) return MOQ_OK;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_data.stream_ref = stream_ref;
    a.u.reset_data.error_code = error_code;

    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->subgroups[slot].state = MOQ_SG_RESETTING;
    s->subgroups[slot].streaming_payload_len = 0;
    s->subgroups[slot].streaming_bytes_written = 0;
    if (s->subgroups[slot].delivery_deadline_us != UINT64_MAX) {
        s->subgroups[slot].delivery_deadline_us = UINT64_MAX;
        sg_recompute_deadline(s);
    }
    return MOQ_OK;
}
