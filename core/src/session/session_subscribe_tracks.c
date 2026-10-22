#include "session_internal.h"
#include "request_stream_internal.h"
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
    if (e->rs.ref._v != 0)
        request_registry_remove_by_streamref(s, e->rs.ref);
    moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                    &e->prefix_parts, &e->prefix_count);
    process_auth_tokens_free_staging(s, e->resolved_tokens,
        e->token_staged, e->token_count);
    if (!e->auth_committed)
        process_auth_tokens_abort_txn(s, &e->auth_txn);
    ns_update_release(s, &e->update);
    uint32_t next_gen = e->generation + 1;
    /* Preserve the co-allocated request-bidi receive buffer across reuse. */
    uint8_t *recv_buf = e->rs.recv_buf;
    size_t   recv_cap = e->rs.recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_TRACK_SUB_FREE;
    e->generation = next_gen;
    e->rs.recv_buf = recv_buf;
    e->rs.recv_cap = recv_cap;
}

void track_sub_destroy_all(moq_session_t *s)
{
    for (size_t i = 0; i < s->track_sub_cap; i++) {
        moq_track_sub_entry_t *e = &s->track_subs[i];
        ns_update_release(s, &e->update);
        moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                        &e->prefix_parts, &e->prefix_count);
    }
}

/* Independent overlap space (§10.19, draft-22 §9.5.2): scan only the
 * track-sub pool, and only the requests of `role` -- the ones this side made
 * (SUBSCRIBER) or the ones the peer made of it (PUBLISHER). PREFIX_OVERLAP
 * compares one subscriber's SUBSCRIBE_TRACKS at one publisher (draft-22 §3.6:
 * "if a publisher receives a SUBSCRIBE_TRACKS with a Track Namespace Prefix
 * that shares a common prefix with an established SUBSCRIBE_TRACKS"); a
 * request in the other direction has the other subscriber. A prefix an owed
 * or outstanding update would install is claimed as well, so neither a later
 * request nor a second update can take an overlapping one before that update
 * concludes. */
bool track_sub_prefix_conflicts(moq_session_t *s, const moq_bytes_t *parts,
                                size_t count, moq_track_sub_role_t role,
                                int exclude_slot)
{
    for (size_t i = 0; i < s->track_sub_cap; i++) {
        const moq_track_sub_entry_t *e = &s->track_subs[i];
        if (e->state == MOQ_TRACK_SUB_FREE) continue;
        if ((int)i == exclude_slot) continue;
        if (e->role != role) continue;
        if (moq_prefix_overlaps(e->prefix_parts, e->prefix_count, parts, count))
            return true;
        if (e->update.owed && (e->update.owed_flags & MOQ_NAMESPACE_UPDATE_PREFIX) &&
            moq_prefix_overlaps(e->update.owed_prefix_parts,
                                e->update.owed_prefix_count, parts, count))
            return true;
        if (e->update.outstanding &&
            (e->update.out_flags & MOQ_NAMESPACE_UPDATE_PREFIX) &&
            moq_prefix_overlaps(e->update.out_prefix_parts,
                                e->update.out_prefix_count, parts, count))
            return true;
    }
    return false;
}

/* -- Copy namespace into output scratch ---------------------------- */

/* Emit a terminal REQUEST_ERROR + FIN for a pre-commit reject on the request
 * bidi, retiring the bidi via the drain ring when the requester has not yet
 * closed its half. Mirrors the track-status reject path. */
static moq_result_t track_sub_reject_request_args(moq_session_t *s,
    const moq_request_endpoint_t *ep, bool reject_drain,
    const moq_request_error_encode_args_t *args)
{
    if (reject_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    uint8_t err_buf[256];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_result_t rc = s->profile->encode_request_error(s, &ew, args);
    if (rc < 0) return rc;
    rc = queue_send_bidi(s, ep->stream_ref, err_buf,
                         moq_buf_writer_offset(&ew), true);
    if (rc < 0) return rc;
    if (reject_drain)
        (void)drain_ref_add(s, ep->stream_ref);   /* slot reserved above */
    return MOQ_OK;
}

static moq_result_t track_sub_reject_request(moq_session_t *s,
    const moq_request_endpoint_t *ep, uint64_t error_code, bool reject_drain,
    const char *reason, size_t reason_len)
{
    return track_sub_reject_request_args(s, ep, reject_drain,
        &(moq_request_error_encode_args_t){
            .request_id = ep->request_id,
            .error_code = error_code,
            .reason = (const uint8_t *)reason,
            .reason_len = reason_len });
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
                                   d->track_namespace_prefix.count,
                                   MOQ_TRACK_SUB_ROLE_PUBLISHER, -1)) {
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
        moq_request_error_encode_args_t full = session_capacity_request_error(
            s, d->endpoint.request_id, "subscribe-tracks pool full", 26);
        rc = track_sub_reject_request_args(s, &d->endpoint, reject_drain, &full);
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
    if (s->profile->subscribe_tracks_options) {
        moq_subscribe_tracks_options_t *options = event_scratch_alloc_aligned(s,
            sizeof(*options), _Alignof(moq_subscribe_tracks_options_t));
        if (!options) {
            s->event_scratch_len = scratch_saved;
            result = scratch_saved == 0 ? close_with_error(s, 0x1,
                "event scratch permanently too small") : MOQ_ERR_BUFFER;
            goto cleanup_all;
        }
        *options = d->options;
        e.u.subscribe_tracks_request.options = options;
    }

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
    entry->options = d->options;
    d->endpoint.kind = MOQ_REQ_SUBSCRIBE_TRACKS;
    d->endpoint.slot = slot;
    /* Hand the request bidi off from the staging sub slot to this track-sub
     * slot: re-key the stream-ref to (SUBSCRIBE_TRACKS, slot). The request-stream
     * handler frees the staging slot afterwards. */
    request_registry_remove_by_streamref(s, d->endpoint.stream_ref);
    entry->rs.ref = d->endpoint.stream_ref;
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
    const uint8_t *reason, size_t reason_len,
    const moq_decoded_redirect_t *redirect)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    /* The subscription failed; close our send half so the publisher retires its
     * request bidi promptly (otherwise its drain ref lingers until the session
     * ends). Reserve the close action up front for retryability. */
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (redirect) {
        /* The redirect event stands in for the family's error event; the
         * emitter reports a terminal close as MOQ_OK, so the teardown below
         * runs only while the session is still open. */
        moq_result_t rrc = session_core_emit_request_redirect(s,
            MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, e->handle._opaque, redirect,
            error_code, can_retry, retry_after_ms, reason, reason_len);
        if (rrc < 0 || s->state == MOQ_SESS_CLOSED) return rrc;
        if (e->rs.ref._v != 0)
            (void)queue_close_bidi(s, e->rs.ref);
        e->state = MOQ_TRACK_SUB_DRAINING_RESPONSE;
        return MOQ_OK;
    }

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

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_SUBSCRIBE_TRACKS_ERROR;
    ev.detail_size = (uint32_t)sizeof(moq_subscribe_tracks_error_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.subscribe_tracks_error.handle = e->handle;
    ev.u.subscribe_tracks_error.error_code =
        s->profile->semantic_request_error(error_code);
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
    if (e->rs.ref._v != 0)
        (void)queue_close_bidi(s, e->rs.ref);

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
                                                        int slot,
                                                        moq_request_terminal_t term)
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
    ev.u.subscribe_tracks_cancelled.cause = term.cause;
    ev.u.subscribe_tracks_cancelled.stream_error_code = term.error_code;
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
    bool need_drain = !track_sub_peer_fin_observed(&s->track_subs[slot]);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    uint8_t err_buf[128];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_result_t rc = s->profile->encode_request_error(s, &ew,
        &(moq_request_error_encode_args_t){
            .error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED });
    if (rc < 0) return rc;
    moq_stream_ref_t ref = s->track_subs[slot].rs.ref;
    rc = queue_send_bidi(s, ref, err_buf, moq_buf_writer_offset(&ew), true);
    if (rc < 0) return rc;        /* WOULD_BLOCK: retryable, nothing mutated */
    s->profile->commit_inbound_request(s, uep);
    if (need_drain && ref._v != 0)
        (void)drain_ref_add(s, ref);   /* slot reserved above */
    track_sub_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Outbound subscribe_tracks (subscriber side) ------------------- */

void (moq_subscribe_tracks_cfg_init)(moq_subscribe_tracks_cfg_t *cfg)
{
    moq_subscribe_tracks_cfg_init_sized(cfg,
        offsetof(moq_subscribe_tracks_cfg_t, options_flags));
}

void moq_subscribe_tracks_cfg_init_sized(moq_subscribe_tracks_cfg_t *cfg,
                                         size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_subscribe_tracks_cfg_t, include_properties) + sizeof(cfg->include_properties))
        cfg->include_properties = true;
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
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count,
                                 s->profile->auth_token_type_max) < 0)
        return MOQ_ERR_INVAL;
    uint32_t options_flags = 0;
    moq_group_order_t group_order = MOQ_GROUP_ORDER_DEFAULT;
    bool include_properties = true;
    if (ST_CFG_HAS(include_properties)) {
        options_flags = cfg->options_flags;
        if (options_flags & ~(MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER | MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES))
            return MOQ_ERR_INVAL;
        if (options_flags && !s->profile->subscribe_tracks_options)
            return MOQ_ERR_UNSUPPORTED;
        if (options_flags & MOQ_SUBSCRIBE_TRACKS_GROUP_ORDER) {
            /* A selected GROUP_ORDER travels as 1 or 2 (§9.20.8); the
             * publisher's default is the omitted parameter. */
            group_order = cfg->group_order;
            if (group_order != MOQ_GROUP_ORDER_ASCENDING &&
                group_order != MOQ_GROUP_ORDER_DESCENDING)
                return MOQ_ERR_INVAL;
        }
        if (options_flags & MOQ_SUBSCRIBE_TRACKS_INCLUDE_PROPERTIES)
            include_properties = cfg->include_properties;
    }
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
                                   cfg->track_namespace_prefix.count,
                                   MOQ_TRACK_SUB_ROLE_SUBSCRIBER, -1))
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
        .options_flags = options_flags,
        .group_order = group_order,
        .include_properties = include_properties,
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
    e->options = (moq_subscribe_tracks_options_t){
        .struct_size = sizeof(moq_subscribe_tracks_options_t),
        .flags = options_flags,
        .group_order = group_order,
        .include_properties = include_properties,
    };
    e->rs.ref = ref;
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
    rc = queue_send_bidi(s, e->rs.ref, buf,
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
    /* REDIRECT answers SUBSCRIBE_TRACKS only where the profile allows it. */
    if (cfg->error_code == MOQ_REQUEST_ERROR_REDIRECT &&
        !s->profile->redirect_on_any_request)
        return MOQ_ERR_INVAL;
    /* The code must be representable in THIS profile's wire encoding; refuse
     * before any mutation rather than truncate (draft-16 encodes a QUIC
     * varint, draft-18 a vi64 spanning the full 64-bit range). */
    if (cfg->error_code > s->profile->request_error_wire_max)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = track_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (e->state != MOQ_TRACK_SUB_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    /* Free the request bidi after REQUEST_ERROR + FIN; reserve a drain slot up
     * front unless the requester already closed its half. */
    bool need_drain = !track_sub_peer_fin_observed(e);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    /* A complete update the requester pipelined behind the request is
     * cancelled with it, but it consumed a Request ID and may carry token
     * registrations: admit both with the rejection. */
    moq_decoded_request_update_t pending;
    memset(&pending, 0, sizeof(pending));
    if (e->rs.recv_len > 0 && s->profile->request_codec &&
        s->profile->request_codec->decode_namespace_update) {
        moq_result_t prc = moq_request_stream_prepare_pending_reject(s, &e->rs,
            MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, track_sub_peer_fin_observed(e),
            &pending);
        if (prc < 0 || s->state == MOQ_SESS_CLOSED) {
            process_auth_tokens_free_staging(s, pending.tokens,
                pending.token_staged, pending.token_count);
            process_auth_tokens_abort_txn(s, &pending.auth_txn);
            return prc < 0 ? prc : MOQ_OK;
        }
    }

    uint8_t err_buf[1152];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_request_error_encode_args_t err_args = {
        .request_id = e->request_id,
        .error_code = cfg->error_code,
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len };
    /* A REDIRECT without a target: the all-empty tail means "same session,
     * same request" (the config carries no redirect target). */
    moq_result_t rc = reject_apply_redirect(s, &err_args, NULL, true);
    if (rc >= 0) rc = s->profile->encode_request_error(s, &ew, &err_args);
    if (rc >= 0)
        rc = queue_send_bidi(s, e->rs.ref, err_buf,
                             moq_buf_writer_offset(&ew), true);
    if (rc < 0) {
        process_auth_tokens_free_staging(s, pending.tokens,
            pending.token_staged, pending.token_count);
        process_auth_tokens_abort_txn(s, &pending.auth_txn);
        return rc;
    }
    if (pending.endpoint.has_request_id) {
        s->profile->commit_inbound_request(s, &pending.endpoint);
        process_auth_tokens_commit_txn(s, &pending.auth_txn);
    }
    process_auth_tokens_free_staging(s, pending.tokens, pending.token_staged,
                                     pending.token_count);

    if (need_drain && e->rs.ref._v != 0)
        (void)drain_ref_add(s, e->rs.ref);   /* slot reserved above */
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
    return queue_send_bidi(s, e->rs.ref, pbuf,
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

/* -- Draft-22 namespace updates (§9.5, §9.5.2) ----------------------- *
 * SUBSCRIBE_TRACKS's update carries AUTHORIZATION_TOKEN, FORWARD and
 * TRACK_NAMESPACE_PREFIX (TRACK_PROPERTY_FILTER is a Range Filter, refused at
 * the advertised zero limit). A prefix or FORWARD change governs only the
 * subscriptions discovery creates from now on (§9.5.2, §9.20.18). */

static bool track_sub_updates_supported(const moq_session_t *s)
{
    return s->profile->request_codec &&
           s->profile->request_codec->decode_namespace_update &&
           s->profile->encode_namespace_update;
}

/* REQUEST_ERROR + FIN closes the request on this side: the publisher-side
 * entry is retired and the requester's direction is drained. Event, action
 * and drain room are reserved before anything is committed. */
static moq_result_t track_sub_update_terminal(moq_session_t *s, int slot,
    uint64_t request_id, uint64_t code, const uint8_t *reason, size_t reason_len,
    bool report_refused)
{
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    bool need_drain = !track_sub_peer_fin_observed(e) && e->rs.ref._v != 0;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    if (report_refused && event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    uint8_t buf[1152];
    size_t len = 0;
    moq_result_t rc = ns_update_encode_error(s, request_id, code, reason,
                                             reason_len, buf, sizeof(buf), &len);
    if (rc < 0) return rc;
    rc = queue_send_bidi(s, e->rs.ref, buf, len, true);
    if (rc < 0) return rc;
    if (report_refused)
        (void)ns_update_push_result_event(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, e->handle._opaque,
                                          request_id, MOQ_NAMESPACE_UPDATE_OUTCOME_REFUSED,
                                          MOQ_REQUEST_ROLE_RESPONDER, code, NULL, 0, false, 0,
                                          NULL);
    if (need_drain) (void)drain_ref_add(s, e->rs.ref);   /* slot reserved above */
    return MOQ_OK;
}

moq_result_t track_sub_on_namespace_update(moq_session_t *s, int slot,
                                           const moq_request_envelope_t *env)
{
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (e->update.owed)
        return close_with_error(s, s->profile->request_updates_exceeded_error,
                                "too many outstanding REQUEST_UPDATEs");
    ns_update_admission_t a;
    moq_result_t rc = ns_update_admit(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS,
                                      e->rs.ref, env, &a);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    uint64_t refuse = a.refuse_code;
    if (!refuse && a.d.has_prefix &&
        track_sub_prefix_conflicts(s, a.d.prefix.parts, a.d.prefix.count,
                                   MOQ_TRACK_SUB_ROLE_PUBLISHER, slot))
        refuse = MOQ_REQUEST_ERROR_PREFIX_OVERLAP;
    if (refuse) {
        rc = track_sub_update_terminal(s, slot, a.d.request_id, refuse, NULL, 0,
                                       true);
        if (rc < 0 || s->state == MOQ_SESS_CLOSED) {
            ns_update_admission_abort(s, &a);
            return rc;
        }
        ns_update_admission_commit(s, &a);
        track_sub_free_entry(s, slot);
        return MOQ_OK;
    }
    rc = ns_update_defer(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS,
                         e->handle._opaque, &e->update, &a);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) ns_update_admission_abort(s, &a);
    return rc;
}

/* Updates not left to the application: admitted exactly as a deferred one
 * (Request ID, token-cache transactions, parameter limits such as
 * MAX_FILTER_RANGES) and refused with the admission's code, else
 * NOT_SUPPORTED. No event, as before updates could be deferred. */
moq_result_t track_sub_refuse_namespace_update(moq_session_t *s, int slot,
                                               const moq_request_envelope_t *env)
{
    ns_update_admission_t a;
    moq_result_t rc = ns_update_admit(s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS,
                                      s->track_subs[slot].rs.ref, env, &a);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    uint64_t code = a.refuse_code ? a.refuse_code : MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    rc = track_sub_update_terminal(s, slot, a.d.request_id, code, NULL, 0, false);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) {
        ns_update_admission_abort(s, &a);
        return rc;
    }
    ns_update_admission_commit(s, &a);
    track_sub_free_entry(s, slot);
    return MOQ_OK;
}

moq_result_t track_sub_on_update_response(moq_session_t *s, int slot, bool ok,
    uint64_t error_code, const uint8_t *reason, size_t reason_len,
    bool has_expires, uint64_t expires_ms)
{
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    moq_ns_update_t *u = &e->update;
    moq_result_t rc = ns_update_push_result_event(
        s, MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, e->handle._opaque, u->out_request_id,
        ok ? MOQ_NAMESPACE_UPDATE_OUTCOME_OK : MOQ_NAMESPACE_UPDATE_OUTCOME_ERROR,
        MOQ_REQUEST_ROLE_REQUESTER, ok ? 0 : s->profile->semantic_request_error(error_code),
        ok ? NULL : reason, ok ? 0 : reason_len, has_expires, expires_ms, NULL);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    if (ok) {
        /* The barrier: the new prefix names later PUBLISH_SKIPPED suffixes. */
        if (u->out_flags & MOQ_NAMESPACE_UPDATE_PREFIX) {
            moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                            &e->prefix_parts, &e->prefix_count);
            ns_update_take_out_prefix(u, &e->prefix_buf, &e->prefix_buf_len,
                                      &e->prefix_parts, &e->prefix_count);
        }
        if (u->out_flags & MOQ_NAMESPACE_UPDATE_FORWARD)
            e->forward = u->out_forward;
    } else {
        u->failed = true;
    }
    ns_update_clear_outstanding(s, u);
    return MOQ_OK;
}

moq_result_t moq_session_update_subscribe_tracks(moq_session_t *s,
    moq_track_sub_handle_t handle, const moq_namespace_update_cfg_t *cfg,
    uint64_t now_us, uint64_t *out_update_request_id)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (!track_sub_updates_supported(s)) return MOQ_ERR_UNSUPPORTED;
    moq_namespace_update_encode_args_t args;
    moq_result_t rc = ns_update_cfg_read(s, cfg,
        MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS, &args);
    if (rc < 0) return rc;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = track_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    if (e->role != MOQ_TRACK_SUB_ROLE_SUBSCRIBER ||
        e->state != MOQ_TRACK_SUB_ESTABLISHED || e->update.failed ||
        e->goaway_sent)
        return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;
    if (e->update.outstanding) return MOQ_ERR_REQUEST_BLOCKED;
    if (args.has_prefix &&
        track_sub_prefix_conflicts(s, args.prefix.parts, args.prefix.count,
                                   MOQ_TRACK_SUB_ROLE_SUBSCRIBER, slot))
        return MOQ_ERR_INVAL;
    return ns_update_send(s, e->rs.ref, &e->update, &args, out_update_request_id);
}

static moq_result_t track_sub_owed_slot(moq_session_t *s,
    moq_track_sub_handle_t handle, int *out_slot)
{
    int slot = track_sub_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->track_subs[slot].role != MOQ_TRACK_SUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    *out_slot = slot;
    return MOQ_OK;
}

moq_result_t moq_session_accept_subscribe_tracks_update(moq_session_t *s,
    moq_track_sub_handle_t handle, const moq_accept_subscription_update_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (!track_sub_updates_supported(s)) return MOQ_ERR_UNSUPPORTED;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = -1;
    moq_result_t rc = track_sub_owed_slot(s, handle, &slot);
    if (rc < 0) return rc;
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    moq_ns_update_t *u = &e->update;
    rc = ns_update_check_accept(s, u, cfg);
    if (rc < 0) return rc;
    /* A request retired after this answer also finishes this side's
     * direction and, after the peer's GOAWAY without its FIN, drains. */
    bool retire = u->retire_after_answer;
    bool retire_drain = retire && u->retire_goaway_received &&
                        !track_sub_peer_fin_observed(e);
    if (retire && (action_queue_avail(s) < 2 ||
                   (retire_drain && s->drain_ref_count >= s->drain_ref_cap)))
        return MOQ_ERR_WOULD_BLOCK;
    uint8_t buf[64];
    size_t len = 0;
    rc = ns_update_encode_ok(s, u->owed_request_id, buf, sizeof(buf), &len);
    if (rc < 0) return rc;
    rc = queue_send_bidi(s, e->rs.ref, buf, len, false);
    if (rc < 0) return rc;
    /* The prefix storage was reserved when the update was admitted. */
    if (u->owed_flags & MOQ_NAMESPACE_UPDATE_PREFIX) {
        moq_prefix_free(s, &e->prefix_buf, &e->prefix_buf_len,
                        &e->prefix_parts, &e->prefix_count);
        ns_update_take_owed_prefix(u, &e->prefix_buf, &e->prefix_buf_len,
                                   &e->prefix_parts, &e->prefix_count);
    }
    if (u->owed_flags & MOQ_NAMESPACE_UPDATE_FORWARD)
        e->forward = u->owed_forward;
    ns_update_clear_owed(s, u);
    if (retire) {
        if (u->retire_close_half) (void)queue_close_bidi(s, e->rs.ref);
        if (retire_drain) (void)drain_ref_add_goaway_received(s, e->rs.ref);
        track_sub_free_entry(s, slot);
    }
    return MOQ_OK;
}

moq_result_t moq_session_reject_subscribe_tracks_update(moq_session_t *s,
    moq_track_sub_handle_t handle, const moq_reject_subscription_update_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (!track_sub_updates_supported(s)) return MOQ_ERR_UNSUPPORTED;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = -1;
    moq_result_t rc = track_sub_owed_slot(s, handle, &slot);
    if (rc < 0) return rc;
    moq_track_sub_entry_t *e = &s->track_subs[slot];
    rc = ns_update_check_reject(s, &e->update, cfg);
    if (rc < 0) return rc;
    rc = track_sub_update_terminal(s, slot, e->update.owed_request_id,
        cfg->error_code, cfg->reason.data, cfg->reason.len, false);
    if (rc < 0) return rc;
    track_sub_free_entry(s, slot);
    return MOQ_OK;
}

moq_result_t moq_session_subscribe_tracks_owed_update(moq_session_t *s,
    moq_track_sub_handle_t handle, moq_namespace_update_record_t *out,
    size_t out_size)
{
    if (!s || !out || out_size < MOQ_NAMESPACE_UPDATE_RECORD_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (!track_sub_updates_supported(s)) return MOQ_ERR_UNSUPPORTED;
    int slot = -1;
    moq_result_t rc = track_sub_owed_slot(s, handle, &slot);
    if (rc < 0) return rc;
    return ns_update_owed_query(&s->track_subs[slot].update, out, out_size);
}
