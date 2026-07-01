#include "session_internal.h"
#include "../internal/validate.h"

/*
 * SUBSCRIBE_TRACKS (draft-18 §10.19) + PUBLISH_BLOCKED (§10.20).
 *
 * Track discovery: a subscriber asks for PUBLISH messages for all tracks under a
 * namespace prefix. Draft-18 only — there is no draft-16 wire form, so this
 * rides the generic request-bidi staging/handoff (like FETCH / TRACK_STATUS) and
 * is keyed in the shared request registry by stream-ref (MOQ_REQ_SUBSCRIBE_TRACKS),
 * with no dedicated index. The overlap space is independent of the namespace-sub
 * pool. After REQUEST_OK the bidi stays established and carries PUBLISH_BLOCKED;
 * the resulting PUBLISH messages (separate bidis) are not modelled yet.
 */

/* -- Pool helpers -------------------------------------------------- */

int track_sub_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->track_sub_cap; i++)
        if (s->track_subs[i].state == MOQ_TRACK_SUB_FREE) return (int)i;
    return -1;
}

int track_sub_resolve_handle(moq_session_t *s, moq_track_sub_handle_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->track_sub_cap) return -1;
    if (s->track_subs[slot].generation != gen) return -1;
    if (s->track_subs[slot].state == MOQ_TRACK_SUB_FREE) return -1;
    return (int)slot;
}

static moq_track_sub_handle_t track_sub_make_handle(moq_session_t *s, size_t slot,
                                                    uint32_t gen)
{
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIBE_TRACKS,
                                       s->session_tag, gen, (uint32_t)slot);
    moq_track_sub_handle_t h = { packed };
    return h;
}

void track_sub_free_entry(moq_session_t *s, int slot)
{
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (e->request_stream_ref._v != 0)
        request_registry_remove_by_streamref(s, e->request_stream_ref);
    moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                    &e->prefix_parts, &e->prefix_count);
    process_auth_tokens_free_staging(s, e->resolved_tokens,
        e->token_staged, e->token_count);
    if (!e->auth_committed)
        process_auth_tokens_abort_txn(s, &e->auth_txn);
    uint32_t next_gen = e->generation + 1;
    /* Preserve the co-allocated request-bidi receive buffer across reuse. */
    uint8_t *recv_buf = e->req_recv_buf;
    size_t   recv_cap = e->req_recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_TRACK_SUB_FREE;
    e->generation = next_gen;
    e->req_recv_buf = recv_buf;
    e->req_recv_cap = recv_cap;
}

void track_sub_destroy_all(moq_session_t *s)
{
    for (size_t i = 0; i < s->track_sub_cap; i++) {
        moq_track_sub_entry_t *e = &s->track_subs[i];
        moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                        &e->prefix_parts, &e->prefix_count);
    }
}

/* Independent overlap space (§10.19): scan only the track-sub pool. */
bool track_sub_prefix_conflicts(moq_session_t *s, const moq_bytes_t *parts,
                                size_t count, int exclude_slot)
{
    for (size_t i = 0; i < s->track_sub_cap; i++) {
        if (s->track_subs[i].state == MOQ_TRACK_SUB_FREE) continue;
        if ((int)i == exclude_slot) continue;
        if (moq_prefix_overlaps(s->track_subs[i].prefix_parts,
                s->track_subs[i].prefix_count, parts, count))
            return true;
    }
    return false;
}

/* -- Copy namespace into output scratch ---------------------------- */

static bool event_scratch_copy_namespace(moq_session_t *s,
                                   const moq_namespace_t *src,
                                   moq_namespace_t *dst)
{
    moq_bytes_t *parts = (moq_bytes_t *)event_scratch_alloc_aligned(
        s, src->count * sizeof(moq_bytes_t), _Alignof(moq_bytes_t));
    if (!parts && src->count > 0) return false;
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

/* Emit a terminal REQUEST_ERROR + FIN for a pre-commit reject on the request
 * bidi, retiring the bidi via the drain ring when the requester has not yet
 * closed its half. Mirrors the track-status reject path. */
static moq_result_t track_sub_reject_request(moq_session_t *s,
    const moq_request_endpoint_t *ep, uint64_t error_code, bool reject_drain,
    const char *reason, size_t reason_len)
{
    if (reject_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    uint8_t err_buf[256];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_result_t rc = s->profile->encode_request_error(s, &ew,
        &(moq_request_error_encode_args_t){
            .request_id = ep->request_id,
            .error_code = error_code,
            .reason = (const uint8_t *)reason,
            .reason_len = reason_len });
    if (rc < 0) return rc;
    rc = queue_send_bidi(s, ep->stream_ref, err_buf,
                         moq_buf_writer_offset(&ew), true);
    if (rc < 0) return rc;
    if (reject_drain)
        (void)drain_ref_add(s, ep->stream_ref);   /* slot reserved above */
    return MOQ_OK;
}

/* -- Inbound SUBSCRIBE_TRACKS request ------------------------------ */

moq_result_t session_core_on_subscribe_tracks(moq_session_t *s,
    moq_decoded_subscribe_tracks_request_t *d, bool request_fin)
{
    bool auth_committed = false;
    bool prefix_stored = false;
    int slot = -1;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    size_t scratch_saved = s->event_scratch_len;
    /* A pre-commit terminal reject (auth / overlap / pool-full) sends
     * REQUEST_ERROR + FIN without committing an entry. The requester keeps its
     * send half open (the bidi normally stays up for PUBLISH_BLOCKED), so retire
     * the bidi via the drain ring unless a FIN already arrived. */
    bool reject_drain = d->endpoint.has_stream_ref && !request_fin;

    if (event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    /* Message-level authorization-token reject: REQUEST_ERROR, no event; a
     * REGISTER carried alongside still commits its alias (§10.2.2). */
    if (d->auth_reject_code) {
        rc = track_sub_reject_request(s, &d->endpoint, d->auth_reject_code,
                                      reject_drain, NULL, 0);
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    /* Independent overlap space (§10.19): a prefix sharing a common prefix with
     * an established SUBSCRIBE_TRACKS is rejected with PREFIX_OVERLAP. */
    if (track_sub_prefix_conflicts(s, d->track_namespace_prefix.parts,
                                   d->track_namespace_prefix.count, -1)) {
        rc = track_sub_reject_request(s, &d->endpoint,
                                      MOQ_REQUEST_ERROR_PREFIX_OVERLAP,
                                      reject_drain, NULL, 0);
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    slot = track_sub_find_free(s);
    if (slot < 0) {
        rc = track_sub_reject_request(s, &d->endpoint, 0x0, reject_drain,
                                      "subscribe-tracks pool full", 26);
        if (rc < 0) { result = rc; goto cleanup_all; }
        slot = -1;
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    /* Scratch-copy the prefix + tokens for the borrowed event fields. */
    moq_namespace_t ev_prefix;
    if (!event_scratch_copy_namespace(s, &d->track_namespace_prefix, &ev_prefix)) {
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

    /* Store the prefix on the entry (for the overlap scan against later
     * requests). Freed on any non-commit exit below. */
    moq_track_sub_entry_t *entry = &s->track_subs[slot];
    if (!moq_prefix_store(s, &d->track_namespace_prefix,
                          &entry->prefix_buf, &entry->prefix_buf_len,
                          &entry->prefix_parts, &entry->prefix_count)) {
        result = close_with_error(s, 0x1,
            "allocation failure storing SUBSCRIBE_TRACKS prefix");
        goto cleanup_all;
    }
    prefix_stored = true;

    uint32_t live_gen = entry->generation | 1;
    moq_track_sub_handle_t handle = track_sub_make_handle(s, (size_t)slot,
                                                          live_gen);

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SUBSCRIBE_TRACKS_REQUEST;
    e.detail_size = (uint32_t)sizeof(moq_subscribe_tracks_request_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.subscribe_tracks_request.handle = handle;
    e.u.subscribe_tracks_request.track_namespace_prefix = ev_prefix;
    e.u.subscribe_tracks_request.forward = d->forward;
    e.u.subscribe_tracks_request.tokens = ev_tokens;
    e.u.subscribe_tracks_request.token_count = d->token_count;

    rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        result = rc;
        goto cleanup_all;
    }

    entry->generation = live_gen;
    entry->state = MOQ_TRACK_SUB_PENDING_PUBLISHER;
    entry->role = MOQ_TRACK_SUB_ROLE_PUBLISHER;
    entry->handle = handle;
    entry->request_id = d->endpoint.request_id;
    entry->forward = d->forward;
    d->endpoint.kind = MOQ_REQ_SUBSCRIBE_TRACKS;
    d->endpoint.slot = slot;
    /* Hand the request bidi off from the staging sub slot to this track-sub
     * slot: re-key the stream-ref to (SUBSCRIBE_TRACKS, slot). The request-stream
     * handler frees the staging slot afterwards. */
    request_registry_remove_by_streamref(s, d->endpoint.stream_ref);
    entry->request_stream_ref = d->endpoint.stream_ref;
    request_registry_insert_by_streamref(s, d->endpoint.stream_ref, d->endpoint);
    s->profile->commit_inbound_request(s, &d->endpoint);
    auth_committed = true;
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    return MOQ_OK;

cleanup_all:
    if (prefix_stored && slot >= 0) {
        moq_track_sub_entry_t *en = &s->track_subs[slot];
        moq_prefix_free(s, &en->prefix_buf, &en->prefix_buf_len,
                        &en->prefix_parts, &en->prefix_count);
    }
    process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
        d->token_count);
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

/* -- Inbound REQUEST_OK (subscriber side) -------------------------- */

moq_result_t session_core_on_subscribe_tracks_ok(moq_session_t *s, int slot)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_track_sub_entry_t *e = &s->track_subs[slot];

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_SUBSCRIBE_TRACKS_OK;
    ev.detail_size = (uint32_t)sizeof(moq_subscribe_tracks_ok_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.subscribe_tracks_ok.handle = e->handle;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;

    /* The bidi stays established for PUBLISH_BLOCKED (and future PUBLISH). The
     * stream-ref registry entry is retained to route those messages. */
    e->state = MOQ_TRACK_SUB_ESTABLISHED;
    return MOQ_OK;
}

/* -- Inbound REQUEST_ERROR (subscriber side) ----------------------- */

moq_result_t session_core_on_subscribe_tracks_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    /* The subscription failed; close our send half so the publisher retires its
     * request bidi promptly (otherwise its drain ref lingers until the session
     * ends). Reserve the close action up front for retryability. */
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
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

    moq_track_sub_entry_t *e = &s->track_subs[slot];

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR;
    ev.detail_size = (uint32_t)sizeof(moq_subscribe_tracks_error_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.subscribe_tracks_error.handle = e->handle;
    ev.u.subscribe_tracks_error.error_code = (moq_request_error_t)error_code;
    ev.u.subscribe_tracks_error.can_retry = can_retry;
    ev.u.subscribe_tracks_error.retry_after_ms = retry_after_ms;
    ev.u.subscribe_tracks_error.reason = ev_reason;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    /* Close our send half (action slot reserved above): nothing more to send on a
     * failed subscription, and this lets the publisher retire its request bidi. */
    if (e->request_stream_ref._v != 0)
        (void)queue_close_bidi(s, e->request_stream_ref);

    /* Terminal: keep the bidi drainable until its FIN (the request-stream handler
     * frees the entry on FIN, or immediately if the FIN already arrived). */
    e->state = MOQ_TRACK_SUB_DRAINING_RESPONSE;
    return MOQ_OK;
}

/* -- Inbound PUBLISH_BLOCKED (subscriber side) --------------------- */

moq_result_t session_core_on_publish_blocked(moq_session_t *s, int slot,
    const moq_decoded_publish_blocked_t *d)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_track_sub_entry_t *e = &s->track_subs[slot];

    size_t scratch_saved = s->event_scratch_len;
    moq_namespace_t ev_suffix;
    if (!event_scratch_copy_namespace(s, &d->track_namespace_suffix, &ev_suffix)) {
        s->event_scratch_len = scratch_saved;
        if (scratch_saved == 0)
            return close_with_error(s, 0x1,
                "event scratch permanently too small");
        return MOQ_ERR_BUFFER;
    }
    moq_bytes_t ev_name = {0};
    ev_name.data = event_scratch_copy(s, d->track_name.data, d->track_name.len);
    ev_name.len = d->track_name.len;
    if (d->track_name.len > 0 && !ev_name.data) {
        s->event_scratch_len = scratch_saved;
        if (scratch_saved == 0)
            return close_with_error(s, 0x1,
                "event scratch permanently too small");
        return MOQ_ERR_BUFFER;
    }

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_BLOCKED;
    ev.detail_size = (uint32_t)sizeof(moq_publish_blocked_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.publish_blocked.handle = e->handle;
    ev.u.publish_blocked.track_namespace_suffix = ev_suffix;
    ev.u.publish_blocked.track_name = ev_name;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }
    return MOQ_OK;   /* stays ESTABLISHED */
}

/* -- Peer teardown (RESET/STOP or FIN on the request bidi) --------- */

moq_result_t session_core_on_subscribe_tracks_torn_down(moq_session_t *s,
                                                        int slot)
{
    moq_track_sub_entry_t *e = &s->track_subs[slot];

    /* A terminal REQUEST_ERROR was already surfaced; the bidi was only kept to
     * absorb the FIN. Free silently. */
    if (e->state == MOQ_TRACK_SUB_DRAINING_RESPONSE) {
        track_sub_free_entry(s, slot);
        return MOQ_OK;
    }

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_SUBSCRIBE_TRACKS_CANCELLED;
    ev.detail_size = (uint32_t)sizeof(moq_subscribe_tracks_cancelled_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.subscribe_tracks_cancelled.handle = e->handle;
    (void)push_event(s, &ev);
    track_sub_free_entry(s, slot);
    return MOQ_OK;
}

/* §10.9.1: a REQUEST_UPDATE on a SUBSCRIBE_TRACKS bidi (re-prefix) is not
 * supported yet; reject and close the bidi (not the session). REQUEST_ERROR +
 * FIN on the bidi, commit the update's request id, terminate. Retryable on
 * WOULD_BLOCK (nothing mutated until the response is queued). */
moq_result_t session_core_on_track_sub_update_rejected(moq_session_t *s,
    int slot, const moq_request_endpoint_t *uep)
{
    if (s->drain_ref_count >= s->drain_ref_cap) return MOQ_ERR_WOULD_BLOCK;
    uint8_t err_buf[128];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_result_t rc = s->profile->encode_request_error(s, &ew,
        &(moq_request_error_encode_args_t){
            .error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED });
    if (rc < 0) return rc;
    moq_stream_ref_t ref = s->track_subs[slot].request_stream_ref;
    rc = queue_send_bidi(s, ref, err_buf, moq_buf_writer_offset(&ew), true);
    if (rc < 0) return rc;        /* WOULD_BLOCK: retryable, nothing mutated */
    s->profile->commit_inbound_request(s, uep);
    if (ref._v != 0) (void)drain_ref_add(s, ref);   /* slot reserved above */
    track_sub_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Outbound subscribe_tracks (subscriber side) ------------------- */

void moq_subscribe_tracks_cfg_init(moq_subscribe_tracks_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_session_subscribe_tracks(moq_session_t *s,
    const moq_subscribe_tracks_cfg_t *cfg, uint64_t now_us,
    moq_track_sub_handle_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
#define ST_CFG_MIN offsetof(moq_subscribe_tracks_cfg_t, has_forward)
    if (cfg->struct_size < ST_CFG_MIN) return MOQ_ERR_INVAL;
#define ST_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_subscribe_tracks_cfg_t, f) + sizeof(cfg->f))
    *out_handle = MOQ_TRACK_SUB_HANDLE_INVALID;

    bool has_forward = false, forward = true;
    if (ST_CFG_HAS(forward)) {
        has_forward = cfg->has_forward;
        forward = cfg->forward;
    }
    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (ST_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;
#undef ST_CFG_HAS
#undef ST_CFG_MIN

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    /* SUBSCRIBE_TRACKS has no draft-16 wire form. */
    if (!moq_session_uses_request_streams(s)) return MOQ_ERR_INVAL;

    {
        const moq_namespace_t *pfx = &cfg->track_namespace_prefix;
        if (pfx->count > 32) return MOQ_ERR_INVAL;
        if (pfx->count > 0 && !pfx->parts) return MOQ_ERR_INVAL;
        size_t pfx_total = 0;
        for (size_t i = 0; i < pfx->count; i++) {
            if (pfx->parts[i].len == 0) return MOQ_ERR_INVAL;
            if (!pfx->parts[i].data) return MOQ_ERR_INVAL;
            if (pfx->parts[i].len > MOQ_FULL_TRACK_NAME_MAX - pfx_total)
                return MOQ_ERR_INVAL;
            pfx_total += pfx->parts[i].len;
        }
    }
    if (track_sub_prefix_conflicts(s, cfg->track_namespace_prefix.parts,
                                   cfg->track_namespace_prefix.count, -1))
        return MOQ_ERR_INVAL;

    moq_request_endpoint_t req_ep;
    moq_result_t prc = s->profile->prepare_request(s, &req_ep);
    if (prc < 0) return prc;

    int slot = track_sub_find_free(s);
    if (slot < 0) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_stream_ref_t ref = moq_stream_ref_from_u64(s->next_stream_ref);

    if (action_queue_full(s)) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (!moq_prefix_store(s, &cfg->track_namespace_prefix,
                          &e->prefix_buf, &e->prefix_buf_len,
                          &e->prefix_parts, &e->prefix_count)) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_NOMEM;
    }

    size_t avail = s->send_cap - s->send_len;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len, avail);
    moq_subscribe_tracks_encode_args_t enc_args = {
        .request_id = req_ep.request_id,
        .prefix = cfg->track_namespace_prefix,
        .has_forward = has_forward,
        .forward = forward,
        .auth_tokens = auth_tokens,
        .auth_token_count = auth_token_count,
    };
    moq_result_t rc = s->profile->encode_subscribe_tracks(s, &w, &enc_args);
    if (rc < 0) {
        moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                        &e->prefix_parts, &e->prefix_count);
        s->profile->abort_request(s, &req_ep);
        return rc;
    }
    size_t enc_len = moq_buf_writer_offset(&w);

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_OPEN_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_open_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.open_bidi_stream.stream_ref = ref;
    a.u.open_bidi_stream.data = s->send_buf + s->send_len;
    a.u.open_bidi_stream.len  = enc_len;
    /* The bidi stays open: the response (REQUEST_OK / REQUEST_ERROR) and
     * subsequent PUBLISH_BLOCKED return on it. */
    a.u.open_bidi_stream.fin = false;
    rc = push_action(s, &a);
    if (rc < 0) {
        moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                        &e->prefix_parts, &e->prefix_count);
        s->profile->abort_request(s, &req_ep);
        return rc;
    }
    s->send_len += enc_len;

    uint32_t live_gen = e->generation | 1;
    e->generation = live_gen;
    e->state = MOQ_TRACK_SUB_PENDING_SUBSCRIBER;
    e->role = MOQ_TRACK_SUB_ROLE_SUBSCRIBER;
    e->request_id = req_ep.request_id;
    e->forward = forward;
    e->request_stream_ref = ref;
    e->handle = track_sub_make_handle(s, (size_t)slot, live_gen);

    req_ep.kind = MOQ_REQ_SUBSCRIBE_TRACKS;
    req_ep.slot = slot;
    req_ep.has_stream_ref = true;
    req_ep.stream_ref = ref;
    request_registry_insert_by_streamref(s, ref, req_ep);
    s->profile->commit_request(s, &req_ep);
    s->next_stream_ref++;

    *out_handle = e->handle;
    return MOQ_OK;
}

/* -- Accept (publisher side) --------------------------------------- */

void moq_accept_subscribe_tracks_cfg_init(moq_accept_subscribe_tracks_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_session_accept_subscribe_tracks(moq_session_t *s,
    moq_track_sub_handle_t handle,
    const moq_accept_subscribe_tracks_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(*cfg)) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = track_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (e->state != MOQ_TRACK_SUB_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_request_ok(s, &w, e->request_id);
    if (rc < 0) return rc;
    /* No FIN: the bidi stays open for PUBLISH_BLOCKED (and future PUBLISH). */
    rc = queue_send_bidi(s, e->request_stream_ref, buf,
                         moq_buf_writer_offset(&w), false);
    if (rc < 0) return rc;

    e->state = MOQ_TRACK_SUB_ESTABLISHED;
    return MOQ_OK;
}

/* -- Reject (publisher side) --------------------------------------- */

void moq_reject_subscribe_tracks_cfg_init(moq_reject_subscribe_tracks_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(*cfg);
}

moq_result_t moq_session_reject_subscribe_tracks(moq_session_t *s,
    moq_track_sub_handle_t handle,
    const moq_reject_subscribe_tracks_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(*cfg)) return MOQ_ERR_INVAL;
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;
    /* SUBSCRIBE_TRACKS is not REDIRECT-eligible (§10.6); refuse a REDIRECT code. */
    if (cfg->error_code == MOQ_REQUEST_ERROR_REDIRECT) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = track_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (e->state != MOQ_TRACK_SUB_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    /* Free the request bidi after REQUEST_ERROR + FIN; reserve a drain slot up
     * front unless the requester already closed its half. */
    bool need_drain = !e->req_recv_fin;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    uint8_t err_buf[1152];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_result_t rc = s->profile->encode_request_error(s, &ew,
        &(moq_request_error_encode_args_t){
            .request_id = e->request_id,
            .error_code = cfg->error_code,
            .reason = cfg->reason.data,
            .reason_len = cfg->reason.len });
    if (rc < 0) return rc;
    rc = queue_send_bidi(s, e->request_stream_ref, err_buf,
                         moq_buf_writer_offset(&ew), true);
    if (rc < 0) return rc;

    if (need_drain && e->request_stream_ref._v != 0)
        (void)drain_ref_add(s, e->request_stream_ref);   /* slot reserved above */
    track_sub_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Send PUBLISH_BLOCKED (publisher side) ------------------------- */

moq_result_t moq_session_send_publish_blocked(moq_session_t *s,
    moq_track_sub_handle_t handle, const moq_namespace_t *track_namespace_suffix,
    moq_bytes_t track_name, uint64_t now_us)
{
    if (!s || !track_namespace_suffix) return MOQ_ERR_INVAL;
    {
        const moq_namespace_t *sfx = track_namespace_suffix;
        if (sfx->count > 32) return MOQ_ERR_INVAL;
        if (sfx->count > 0 && !sfx->parts) return MOQ_ERR_INVAL;
        size_t total = track_name.len;
        for (size_t i = 0; i < sfx->count; i++) {
            if (sfx->parts[i].len == 0) return MOQ_ERR_INVAL;
            if (!sfx->parts[i].data) return MOQ_ERR_INVAL;
            if (sfx->parts[i].len > MOQ_FULL_TRACK_NAME_MAX - total)
                return MOQ_ERR_INVAL;
            total += sfx->parts[i].len;
        }
        if (track_name.len > 0 && !track_name.data) return MOQ_ERR_INVAL;
    }
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = track_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (e->state != MOQ_TRACK_SUB_ESTABLISHED ||
        e->role != MOQ_TRACK_SUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    uint8_t pbuf[MOQ_FULL_TRACK_NAME_MAX + 128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, pbuf, sizeof(pbuf));
    moq_result_t rc = s->profile->encode_publish_blocked(s, &w,
        track_namespace_suffix, track_name);
    if (rc < 0) return rc;
    return queue_send_bidi(s, e->request_stream_ref, pbuf,
                           moq_buf_writer_offset(&w), false);
}

moq_result_t moq_session_request_goaway_subscribe_tracks(
    moq_session_t *s, moq_track_sub_handle_t handle,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_request_goaway_cfg_t)) return MOQ_ERR_INVAL;
    if (cfg->new_session_uri.len > 0 && !cfg->new_session_uri.data)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = track_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}
