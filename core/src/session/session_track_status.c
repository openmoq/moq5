#include "session_internal.h"
#include "../internal/validate.h"

/* -- Track-status pool --------------------------------------------- */

int ts_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->ts_cap; i++)
        if (s->track_statuses[i].state == MOQ_TS_FREE) return (int)i;
    return -1;
}

int ts_resolve_handle(moq_session_t *s, moq_track_status_handle_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_TRACK_STATUS) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->ts_cap) return -1;
    if (s->track_statuses[slot].generation != gen) return -1;
    if (s->track_statuses[slot].state == MOQ_TS_FREE) return -1;
    return (int)slot;
}

void ts_free_entry(moq_session_t *s, int slot)
{
    moq_ts_entry_t *e = &s->track_statuses[slot];
    request_registry_remove_by_id(s, e->request_id);
    /* Stream-correlated profiles also key the entry by its request bidi. */
    if (e->request_stream_ref._v != 0)
        request_registry_remove_by_streamref(s, e->request_stream_ref);
    uint32_t next_gen = e->generation + 1;
    /* Preserve the co-allocated request-bidi receive buffer across reuse. */
    uint8_t *recv_buf = e->req_recv_buf;
    size_t   recv_cap = e->req_recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_TS_FREE;
    e->generation = next_gen;
    e->req_recv_buf = recv_buf;
    e->req_recv_cap = recv_cap;
}

/* -- Copy namespace into output scratch ---------------------------- */

static bool event_scratch_copy_namespace(moq_session_t *s,
                                    const moq_namespace_t *src,
                                    moq_namespace_t *dst)
{
    moq_bytes_t *parts = (moq_bytes_t *)event_scratch_alloc_aligned(
        s, src->count * sizeof(moq_bytes_t), _Alignof(moq_bytes_t));
    if (!parts) return false;

    for (size_t i = 0; i < src->count; i++) {
        uint8_t *data = event_scratch_copy(s, src->parts[i].data, src->parts[i].len);
        if (!data && src->parts[i].len > 0) return false;
        parts[i].data = data;
        parts[i].len  = src->parts[i].len;
    }

    dst->parts = parts;
    dst->count = src->count;
    return true;
}

/* -- Inbound TRACK_STATUS request ---------------------------------- */

moq_result_t session_core_on_track_status_request(moq_session_t *s,
    moq_decoded_track_status_request_t *d, bool request_fin)
{
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    size_t scratch_saved = s->event_scratch_len;
    /* A pre-commit terminal reject (auth / pool-full) on a stream-correlated
     * profile sends REQUEST_ERROR + FIN and frees the staging slot without
     * committing an entry. If the requester has not already closed its half, a
     * trailing FIN / late bytes would otherwise hit a vanished stream-ref, so
     * retire the bidi via the drain ring. */
    bool reject_drain = d->endpoint.has_stream_ref && !request_fin;

    if (event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    /* A message-level authorization-token reject fails the request with
     * REQUEST_ERROR and surfaces no event; a REGISTER carried alongside still
     * commits its alias (§10.2.2). The error rides the request bidi for
     * stream-correlated profiles, the control channel otherwise. */
    if (d->auth_reject_code) {
        if (reject_drain && s->drain_ref_count >= s->drain_ref_cap) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->endpoint.request_id,
                .error_code = d->auth_reject_code });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = d->endpoint.has_stream_ref
            ? queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        if (reject_drain)
            (void)drain_ref_add(s, d->endpoint.stream_ref);  /* slot reserved */
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    int slot = ts_find_free(s);
    if (slot < 0) {
        if (reject_drain && s->drain_ref_count >= s->drain_ref_cap) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->endpoint.request_id, .error_code = 0x0,
                .reason = (const uint8_t *)"track status pool full",
                .reason_len = 22 });
        if (rc < 0) { result = rc; goto cleanup_all; }
        /* The error rides the request bidi for stream-correlated profiles, the
         * control channel otherwise. */
        rc = d->endpoint.has_stream_ref
            ? queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        if (reject_drain)
            (void)drain_ref_add(s, d->endpoint.stream_ref);  /* slot reserved */
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    moq_namespace_t ev_ns;
    if (!event_scratch_copy_namespace(s, &d->track_namespace, &ev_ns)) {
        if (scratch_saved == 0) {
            result = close_with_error(s, 0x1,
                "event scratch permanently too small");
            goto cleanup_all;
        }
        result = MOQ_ERR_BUFFER;
        goto cleanup_all;
    }
    moq_bytes_t ev_name;
    ev_name.data = event_scratch_copy(s, d->track_name.data, d->track_name.len);
    ev_name.len  = d->track_name.len;
    if (d->track_name.len > 0 && !ev_name.data) {
        s->event_scratch_len = scratch_saved;
        if (scratch_saved == 0) {
            result = close_with_error(s, 0x1,
                "event scratch permanently too small");
            goto cleanup_all;
        }
        result = MOQ_ERR_BUFFER;
        goto cleanup_all;
    }

    for (size_t i = 0; i < d->token_count; i++) {
        if (d->tokens[i].token_value.len > 0) {
            const uint8_t *src = d->tokens[i].token_value.data;
            size_t src_len = d->tokens[i].token_value.len;
            uint8_t *copy = event_scratch_copy(s, src, src_len);
            if (d->token_staged[i])
                s->alloc.free((void *)(uintptr_t)src, src_len, s->alloc.ctx);
            d->token_staged[i] = false;
            if (!copy) {
                s->event_scratch_len = scratch_saved;
                if (scratch_saved == 0) {
                    result = close_with_error(s, 0x1,
                        "event scratch permanently too small");
                    goto cleanup_all;
                }
                result = MOQ_ERR_BUFFER;
                goto cleanup_all;
            }
            d->tokens[i].token_value.data = copy;
        } else {
            d->tokens[i].token_value.data = NULL;
        }
    }
    moq_resolved_token_t *ev_tokens = NULL;
    if (d->token_count > 0) {
        ev_tokens = (moq_resolved_token_t *)event_scratch_alloc_aligned(
            s, d->token_count * sizeof(moq_resolved_token_t),
            _Alignof(moq_resolved_token_t));
        if (!ev_tokens) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0) {
                result = close_with_error(s, 0x1,
                    "event scratch permanently too small");
                goto cleanup_all;
            }
            result = MOQ_ERR_BUFFER;
            goto cleanup_all;
        }
        memcpy(ev_tokens, d->tokens,
               d->token_count * sizeof(moq_resolved_token_t));
    }

    moq_ts_entry_t *entry = &s->track_statuses[slot];
    uint32_t live_gen = entry->generation | 1;
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_TRACK_STATUS,
                                       s->session_tag, live_gen,
                                       (uint32_t)slot);
    moq_track_status_handle_t handle = { packed };

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_TRACK_STATUS_REQUEST;
    e.detail_size = (uint32_t)sizeof(moq_track_status_request_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.track_status_request.handle = handle;
    e.u.track_status_request.track_namespace = ev_ns;
    e.u.track_status_request.track_name = ev_name;
    e.u.track_status_request.tokens = ev_tokens;
    e.u.track_status_request.token_count = d->token_count;

    rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        result = rc;
        goto cleanup_all;
    }

    entry->generation = live_gen;
    entry->state = MOQ_TS_PENDING_PUBLISHER;
    entry->role = MOQ_TS_ROLE_PUBLISHER;
    entry->handle = handle;
    entry->request_id = d->endpoint.request_id;
    d->endpoint.kind = MOQ_REQ_TRACK_STATUS;
    d->endpoint.slot = slot;
    if (d->endpoint.has_stream_ref) {
        /* Hand the request bidi off from the staging sub slot to this track-status
         * slot: re-key the stream-ref to (TRACK_STATUS, slot). The request-stream
         * handler frees the staging slot afterwards. */
        request_registry_remove_by_streamref(s, d->endpoint.stream_ref);
        entry->request_stream_ref = d->endpoint.stream_ref;
        request_registry_insert_by_streamref(s, d->endpoint.stream_ref,
                                             d->endpoint);
    } else {
        request_registry_insert_by_id(s, d->endpoint.request_id, d->endpoint);
    }
    s->profile->commit_inbound_request(s, &d->endpoint);
    auth_committed = true;
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    return MOQ_OK;

cleanup_all:
    process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
        d->token_count);
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

/* -- Inbound TRACK_STATUS OK (REQUEST_OK for track-status) ---------- */

moq_result_t session_core_on_track_status_ok(moq_session_t *s,
    const moq_decoded_track_status_ok_t *d)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_ts_entry_t *e = &s->track_statuses[d->target_slot];

    /* Surface the opaque Track Properties tail (borrowed from output scratch) so
     * a requester/relay can preserve received status metadata. */
    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t ev_props = {0};
    if (d->track_properties_len > 0) {
        ev_props.data = event_scratch_copy(s, d->track_properties,
                                     d->track_properties_len);
        ev_props.len = d->track_properties_len;
        if (!ev_props.data) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
    }

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_TRACK_STATUS_OK;
    ev.detail_size = (uint32_t)sizeof(moq_track_status_ok_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.track_status_ok.handle = e->handle;
    ev.u.track_status_ok.has_largest = d->has_largest;
    ev.u.track_status_ok.largest_group = d->largest_group;
    ev.u.track_status_ok.largest_object = d->largest_object;
    ev.u.track_status_ok.has_expires = d->has_expires;
    ev.u.track_status_ok.expires_ms = d->expires_ms;
    ev.u.track_status_ok.track_properties = ev_props;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    /* Stream-correlated profiles keep the request bidi drainable until its FIN
     * (the request-stream handler frees it); draft-16 frees now. */
    if (e->request_stream_ref._v != 0)
        e->state = MOQ_TS_DRAINING_RESPONSE;
    else
        ts_free_entry(s, d->target_slot);
    return MOQ_OK;
}

/* -- Inbound TRACK_STATUS ERROR (REQUEST_ERROR for track-status) ---- */

moq_result_t session_core_on_track_status_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len,
    const moq_decoded_redirect_t *redirect)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_ts_entry_t *e = &s->track_statuses[slot];
    size_t scratch_saved = s->event_scratch_len;
    moq_result_t rc;
    if (redirect) {
        rc = session_core_emit_request_redirect(s,
            MOQ_REQUEST_FAMILY_TRACK_STATUS, e->handle._opaque, redirect,
            error_code, can_retry, retry_after_ms, reason, reason_len);
        if (rc < 0) return rc;
    } else {
        moq_bytes_t ev_reason = {0};
        if (reason_len > 0) {
            ev_reason.data = event_scratch_copy(s, reason, reason_len);
            ev_reason.len = reason_len;
            if (!ev_reason.data) {
                s->event_scratch_len = scratch_saved;
                if (scratch_saved == 0)
                    return close_with_error(s, 0x1,
                        "event scratch permanently too small");
                return MOQ_ERR_BUFFER;
            }
        }

        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_TRACK_STATUS_ERROR;
        ev.detail_size = (uint32_t)sizeof(moq_track_status_error_event_t);
        ev.borrow_epoch = s->borrow_epoch;
        ev.u.track_status_error.handle = e->handle;
        ev.u.track_status_error.error_code = (moq_request_error_t)error_code;
        ev.u.track_status_error.can_retry = can_retry;
        ev.u.track_status_error.retry_after_ms = retry_after_ms;
        ev.u.track_status_error.reason = ev_reason;

        rc = push_event(s, &ev);
        if (rc < 0) {
            s->event_scratch_len = scratch_saved;
            return rc;
        }
    }

    /* Stream-correlated profiles keep the request bidi drainable until its FIN;
     * draft-16 frees now. */
    if (e->request_stream_ref._v != 0)
        e->state = MOQ_TS_DRAINING_RESPONSE;
    else
        ts_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Outbound track_status request --------------------------------- */

void moq_track_status_cfg_init(moq_track_status_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_track_status_cfg_t);
}

moq_result_t moq_session_track_status(moq_session_t *s,
    const moq_track_status_cfg_t *cfg, uint64_t now_us,
    moq_track_status_handle_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
#define TS_CFG_MIN offsetof(moq_track_status_cfg_t, auth_tokens)
    if (cfg->struct_size < TS_CFG_MIN) return MOQ_ERR_INVAL;
#define TS_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_track_status_cfg_t, f) + sizeof(cfg->f))

    *out_handle = (moq_track_status_handle_t){ 0 };

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (TS_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;
#undef TS_CFG_HAS
#undef TS_CFG_MIN

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    /* §10.4: after a GOAWAY an endpoint SHOULD NOT initiate new requests
     * (TRACK_STATUS among them). Mirror the other request entry points. */
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    moq_request_endpoint_t req_ep;
    memset(&req_ep, 0, sizeof(req_ep));
    moq_result_t rc = s->profile->prepare_request(s, &req_ep);
    if (rc < 0) return rc;

    int slot = ts_find_free(s);
    if (slot < 0) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (action_queue_full(s)) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_track_status_encode_args_t args = {
        .request_id = req_ep.request_id,
        .track_namespace = cfg->track_namespace,
        .track_name = cfg->track_name,
        .auth_tokens = auth_tokens,
        .auth_token_count = auth_token_count,
    };

    bool req_stream = moq_session_uses_request_streams(s);
    moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0);

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);
    rc = s->profile->encode_track_status(s, &w, &args);
    if (rc < 0) {
        s->profile->abort_request(s, &req_ep);
        return rc;
    }

    size_t encoded_len = moq_buf_writer_offset(&w);
    moq_action_t act;
    memset(&act, 0, sizeof(act));
    act.borrow_epoch = s->borrow_epoch;
    if (req_stream) {
        /* Stream-correlated profiles open a request bidi; the response
         * (TRACK_STATUS_OK / REQUEST_ERROR) returns on it, then FIN. */
        req_ref = moq_stream_ref_from_u64(s->next_stream_ref);
        act.kind = MOQ_ACTION_OPEN_BIDI_STREAM;
        act.detail_size = (uint32_t)sizeof(moq_open_bidi_stream_action_t);
        act.u.open_bidi_stream.stream_ref = req_ref;
        act.u.open_bidi_stream.data = s->send_buf + s->send_len;
        act.u.open_bidi_stream.len = encoded_len;
        /* TRACK_STATUS is the first and only message: close our send half now so
         * the peer observes the FIN and need not hold a drain ref. */
        act.u.open_bidi_stream.fin = true;
    } else {
        act.kind = MOQ_ACTION_SEND_CONTROL;
        act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
        act.u.send_control.data = s->send_buf + s->send_len;
        act.u.send_control.len = encoded_len;
    }
    rc = push_action(s, &act);
    if (rc < 0) {
        s->profile->abort_request(s, &req_ep);
        return rc;
    }
    s->send_len += encoded_len;

    moq_ts_entry_t *e = &s->track_statuses[slot];
    uint32_t live_gen = e->generation | 1;
    e->generation = live_gen;
    e->state = MOQ_TS_PENDING_REQUESTER;
    e->role = MOQ_TS_ROLE_REQUESTER;
    e->request_id = req_ep.request_id;

    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_TRACK_STATUS,
                                       s->session_tag, live_gen,
                                       (uint32_t)slot);
    moq_track_status_handle_t h = { packed };
    e->handle = h;

    req_ep.kind = MOQ_REQ_TRACK_STATUS;
    req_ep.slot = slot;
    if (req_stream) {
        e->request_stream_ref = req_ref;
        req_ep.has_stream_ref = true;
        req_ep.stream_ref = req_ref;
        request_registry_insert_by_streamref(s, req_ref, req_ep);
        s->next_stream_ref++;
    } else {
        request_registry_insert_by_id(s, req_ep.request_id, req_ep);
    }
    s->profile->commit_request(s, &req_ep);

    *out_handle = h;
    return MOQ_OK;
}

/* -- Accept track-status request ----------------------------------- */

void moq_accept_track_status_cfg_init(moq_accept_track_status_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_accept_track_status_cfg_t);
}

moq_result_t moq_session_accept_track_status(moq_session_t *s,
    moq_track_status_handle_t handle,
    const moq_accept_track_status_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
#define ATS_CFG_MIN offsetof(moq_accept_track_status_cfg_t, has_largest)
    if (cfg->struct_size < ATS_CFG_MIN) return MOQ_ERR_INVAL;
#define ATS_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_accept_track_status_cfg_t, f) + sizeof(cfg->f))

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ts_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_ts_entry_t *e = &s->track_statuses[slot];
    if (e->state != MOQ_TS_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    /* Migrated by an outbound GOAWAY: no further request-bidi response. Check before
     * capacity preflight so the contract is WRONG_STATE, not WOULD_BLOCK. */
    if (e->goaway_sent) return MOQ_ERR_WRONG_STATE;

    moq_track_status_ok_encode_args_t ok_args;
    memset(&ok_args, 0, sizeof(ok_args));
    ok_args.request_id = e->request_id;
    if (ATS_CFG_HAS(largest_object) && cfg->has_largest) {
        ok_args.has_largest = true;
        ok_args.largest_group = cfg->largest_group;
        ok_args.largest_object = cfg->largest_object;
    }
    if (ATS_CFG_HAS(expires_ms) && cfg->has_expires) {
        ok_args.has_expires = true;
        ok_args.expires_ms = cfg->expires_ms;
    }
#undef ATS_CFG_HAS
#undef ATS_CFG_MIN

    /* Stream-correlated profiles free the request bidi after the terminal
     * response, so reserve a drain slot up front (unless the requester already
     * FIN'd, in which case the bidi is fully closed and nothing late arrives). */
    bool req_stream = (e->request_stream_ref._v != 0);
    bool need_drain = req_stream && !e->req_recv_fin;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_track_status_ok(s, &w, &ok_args);
    if (rc < 0) return rc;
    rc = req_stream
        ? queue_send_bidi(s, e->request_stream_ref, buf,
                          moq_buf_writer_offset(&w), true)
        : queue_send_control(s, buf, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;

    if (need_drain)
        (void)drain_ref_add(s, e->request_stream_ref);   /* slot reserved above */
    ts_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Reject track-status request ----------------------------------- */

void moq_reject_track_status_cfg_init(moq_reject_track_status_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_reject_track_status_cfg_t);
}

moq_result_t moq_session_reject_track_status(moq_session_t *s,
    moq_track_status_handle_t handle,
    const moq_reject_track_status_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    /* Pre-redirect minimum: the original cfg was {struct_size, error_code}. */
    if (cfg->struct_size < offsetof(moq_reject_track_status_cfg_t, reason))
        return MOQ_ERR_INVAL;
#define TS_REJ_HAS(f) \
    (cfg->struct_size >= offsetof(moq_reject_track_status_cfg_t, f) + sizeof(cfg->f))

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ts_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_ts_entry_t *e = &s->track_statuses[slot];
    if (e->state != MOQ_TS_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    /* Migrated by an outbound GOAWAY: no further request-bidi response. Check before
     * capacity preflight so the contract is WRONG_STATE, not WOULD_BLOCK. */
    if (e->goaway_sent) return MOQ_ERR_WRONG_STATE;

    moq_request_error_encode_args_t err_args = { .request_id = e->request_id,
                                                 .error_code = cfg->error_code };
    if (TS_REJ_HAS(reason)) {
        if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;
        err_args.reason = cfg->reason.data;
        err_args.reason_len = cfg->reason.len;
    }
    if (TS_REJ_HAS(retry_after_ms)) {
        if (cfg->can_retry && cfg->retry_after_ms >= MOQ_QUIC_VARINT_MAX)
            return MOQ_ERR_INVAL;
        err_args.can_retry = cfg->can_retry;
        err_args.retry_after_ms = cfg->retry_after_ms;
    }
    moq_result_t vrc = reject_apply_redirect(
        s, &err_args, TS_REJ_HAS(redirect) ? &cfg->redirect : NULL,
        false /* track-scoped */);
    if (vrc < 0) return vrc;
#undef TS_REJ_HAS

    bool req_stream = (e->request_stream_ref._v != 0);
    bool need_drain = req_stream && !e->req_recv_fin;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    moq_result_t rc;
    if (err_args.has_redirect) {
        rc = queue_request_error_bidi(s, e->request_stream_ref, &err_args);
    } else {
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew, &err_args);
        if (rc < 0) return rc;
        rc = req_stream
            ? queue_send_bidi(s, e->request_stream_ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
    }
    if (rc < 0) return rc;

    if (need_drain)
        (void)drain_ref_add(s, e->request_stream_ref);   /* slot reserved above */
    ts_free_entry(s, slot);
    return MOQ_OK;
}

moq_result_t moq_session_request_goaway_track_status(
    moq_session_t *s, moq_track_status_handle_t handle,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_request_goaway_cfg_t)) return MOQ_ERR_INVAL;
    if (cfg->new_session_uri.len > 0 && !cfg->new_session_uri.data)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = ts_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_TRACK_STATUS, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}
