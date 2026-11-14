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
    if (s->track_statuses[slot].terminal_notified) return -1;
    if (s->track_statuses[slot].migration_committed) return -1;
    if (s->track_statuses[slot].goaway_sent &&
        s->track_statuses[slot].state == MOQ_TS_DRAINING_RESPONSE) return -1;
    return (int)slot;
}

void ts_free_entry(moq_session_t *s, int slot)
{
    moq_ts_entry_t *e = &s->track_statuses[slot];
    request_registry_remove_by_id(s, e->request_id);
    /* Stream-correlated profiles also key the entry by its request bidi. */
    if (e->rs.ref._v != 0)
        request_registry_remove_by_streamref(s, e->rs.ref);
    /* Release the reserved registry record and free the owned track-name key
     * exactly once BEFORE the memset. */
    if (e->hist) {
        track_hist_release(s, e->hist);
        e->hist = NULL;
    }
    if (e->track_id_buf) {
        s->alloc.free(e->track_id_buf, e->track_id_len, s->alloc.ctx);
        e->track_id_buf = NULL;
        e->track_id_len = 0;
    }
    uint32_t next_gen = e->generation + 1;
    /* Preserve the co-allocated request-bidi receive buffer across reuse. */
    uint8_t *recv_buf = e->rs.recv_buf;
    size_t   recv_cap = e->rs.recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_TS_FREE;
    e->generation = next_gen;
    e->rs.recv_buf = recv_buf;
    e->rs.recv_cap = recv_cap;
}

void ts_apply_deferred_signals(moq_ts_entry_t *e)
{
    if (e->deferred_reset)
        (void)moq_request_lifetime_peer_reset(&e->rs.lifetime);
    if (e->deferred_stop)
        (void)moq_request_lifetime_peer_stop(&e->rs.lifetime);
    e->deferred_reset = false;
    e->deferred_stop = false;
}

/* Each direction commits independently; a one-action queue can make progress.
 * A queued STOP is not evidence that the peer's sending direction has ended. */
moq_result_t ts_queue_cancellation(moq_session_t *s, int slot)
{
    moq_ts_entry_t *e = &s->track_statuses[slot];
    unsigned owed = moq_request_lifetime_cancel_actions(&e->rs.lifetime);
    if (owed & MOQ_REQUEST_CANCEL_RESET_LOCAL) {
        if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        request_bidi_reset_local(s, e->rs.ref);
        (void)moq_request_lifetime_reset_committed(&e->rs.lifetime);
    }
    if (owed & MOQ_REQUEST_CANCEL_STOP_PEER) {
        if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_STOP_BIDI_STREAM;
        a.detail_size = (uint32_t)sizeof(moq_stop_bidi_stream_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.stop_bidi_stream.stream_ref = e->rs.ref;
        a.u.stop_bidi_stream.error_code = MOQ_STREAM_ERROR_CANCELLED;
        (void)push_action(s, &a);
        (void)moq_request_lifetime_stop_committed(&e->rs.lifetime);
    }
    return MOQ_OK;
}

moq_result_t ts_flush_terminal(moq_session_t *s, int slot)
{
    moq_ts_entry_t *e = &s->track_statuses[slot];
    if (e->state == MOQ_TS_FREE) return MOQ_OK;
    if (e->response_validated) return MOQ_ERR_WOULD_BLOCK;
    const bool sent_migration = e->goaway_sent && e->state == MOQ_TS_DRAINING_RESPONSE &&
                                !e->terminal_cause && !e->migration_committed;
    bool blocked = false;
    if (e->terminal_cause && !e->terminal_notified) {
        if (event_queue_full(s)) {
            blocked = true;
        } else {
            moq_request_terminal_t term = {e->terminal_cause, e->terminal_code};
            push_request_terminated(s, MOQ_REQUEST_FAMILY_TRACK_STATUS,
                e->role == MOQ_TS_ROLE_REQUESTER ? MOQ_REQUEST_ROLE_REQUESTER
                                                 : MOQ_REQUEST_ROLE_RESPONDER,
                term, e->handle._opaque);
            e->terminal_notified = true;
        }
    }
    if (e->terminal_cause || e->migration_committed || e->rs.lifetime.cancelled ||
        sent_migration) {
        if (ts_queue_cancellation(s, slot) != MOQ_OK) blocked = true;
    }
    const moq_request_lifetime_t *life = &e->rs.lifetime;
    if (!blocked && (e->terminal_notified || e->state == MOQ_TS_DRAINING_RESPONSE) &&
        (life->local_fin || life->local_reset) && (life->peer_fin || life->peer_reset))
        ts_free_entry(s, slot);
    /* Sent migration has no application outcome to retry. The peer signal is
     * consumed; the retained owner retries any blocked local output on polls. */
    return blocked && !sent_migration ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
}

moq_result_t ts_receive_terminal(moq_session_t *s, int slot,
                                moq_request_terminal_t term)
{
    moq_ts_entry_t *e = &s->track_statuses[slot];
    if (e->response_validated) {
        /* The original validated bytes stay authoritative. Retain each later
         * direction's first code without failing the response lifetime. */
        if (term.cause == MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM && !e->deferred_reset) {
            e->deferred_reset = true;
            e->deferred_reset_code = term.error_code;
        }
        if (term.cause == MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING && !e->deferred_stop) {
            e->deferred_stop = true;
            e->deferred_stop_code = term.error_code;
        }
        return MOQ_ERR_WOULD_BLOCK;
    }
    if (term.cause == MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM)
        (void)moq_request_lifetime_peer_reset(&e->rs.lifetime);
    else if (term.cause == MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING)
        (void)moq_request_lifetime_peer_stop(&e->rs.lifetime);
    else
        (void)moq_request_lifetime_peer_fin(&e->rs.lifetime);
    if (e->goaway_sent && !e->terminal_cause)
        e->state = MOQ_TS_DRAINING_RESPONSE;
    if (!e->terminal_cause && e->state != MOQ_TS_DRAINING_RESPONSE) {
        e->terminal_cause = term.cause;
        e->terminal_code = term.error_code;
    }
    return ts_flush_terminal(s, slot);
}

/* -- Copy namespace into output scratch ---------------------------- */

/* -- Inbound TRACK_STATUS request ---------------------------------- */

moq_result_t session_core_on_track_status_request(moq_session_t *s,
    moq_decoded_track_status_request_t *d, bool request_fin)
{
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    /* Reserved registry record + retained key, released/freed on any pre-commit
     * failure via cleanup_all, or stored on the entry at commit. */
    moq_track_hist_t *tshist = NULL;
    uint8_t *tskey = NULL;
    size_t   tskey_len = 0;
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
        moq_request_error_encode_args_t full = session_capacity_request_error(
            s, d->endpoint.request_id, "track status pool full", 22);
        rc = s->profile->encode_request_error(s, &ew, &full);
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

    /* Reserve this track's registry record and retain its canonical key,
     * selecting allocation-free first so a full registry and an allocation
     * failure stay distinct. A full registry is NOT a session-fatal parser
     * error: reject with REQUEST_ERROR(INTERNAL_ERROR 0x0) via the same
     * pre-commit pattern as the pool-full branch above -- nothing is
     * committed until the error is queued, WOULD_BLOCK leaves it
     * replayable, and the session stays open. */
    bool tshist_full = moq_track_hist_select(s, &d->track_namespace,
                                             d->track_name) ==
                       MOQ_TH_SEL_FULL;
    if (!tshist_full) {
        /* This path RETAINS the canonical key on the committed entry, so
         * it is built here rather than inside the reserve helper. Its
         * allocation failure is MOQ_ERR_NOMEM with no response queued.
         * Registry-full was already excluded by the selection above, so a
         * NULL reservation here can only be the record's key copy failing
         * -- also MOQ_ERR_NOMEM, never the history-full rejection. */
        tskey = moq_build_track_key(s, &d->track_namespace, d->track_name,
                                    &tskey_len);
        if (!tskey && tskey_len > 0) {
            result = MOQ_ERR_NOMEM;
            goto cleanup_all;
        }
        tshist = track_hist_reserve(s, tskey, tskey_len);
        if (!tshist) { result = MOQ_ERR_NOMEM; goto cleanup_all; }
    }
    if (tshist_full) {
        if (reject_drain && s->drain_ref_count >= s->drain_ref_cap) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t herr_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, herr_buf, sizeof(herr_buf));
        moq_request_error_encode_args_t full = session_capacity_request_error(
            s, d->endpoint.request_id, "track history full", 18);
        rc = s->profile->encode_request_error(s, &ew, &full);
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = d->endpoint.has_stream_ref
            ? queue_send_bidi(s, d->endpoint.stream_ref, herr_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, herr_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        if (reject_drain)
            (void)drain_ref_add(s, d->endpoint.stream_ref);
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
    entry->track_id_buf = tskey;
    entry->track_id_len = tskey_len;
    entry->hist = tshist;
    entry->omit_track_properties = d->omit_track_properties;
    if (s->profile->premature_fin_fails_request) {
        (void)moq_request_lifetime_init(&entry->rs.lifetime,
            MOQ_REQUEST_FAMILY_TRACK_STATUS, false);
        if (request_fin) (void)moq_request_lifetime_peer_fin(&entry->rs.lifetime);
    }
    d->endpoint.kind = MOQ_REQ_TRACK_STATUS;
    d->endpoint.slot = slot;
    if (d->endpoint.has_stream_ref) {
        /* Hand the request bidi off from the staging sub slot to this track-status
         * slot: re-key the stream-ref to (TRACK_STATUS, slot). The request-stream
         * handler frees the staging slot afterwards. */
        request_registry_remove_by_streamref(s, d->endpoint.stream_ref);
        entry->rs.ref = d->endpoint.stream_ref;
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
    /* Release the reserved record + free the key on every failure path (both are
     * only stored on the entry at the committed return above). */
    if (tshist) track_hist_release(s, tshist);
    if (tskey) s->alloc.free(tskey, tskey_len, s->alloc.ctx);
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
    /* Weigh the tail before copying: one larger than the whole arena can never
     * be satisfied and ends the session, while one only the live tail cannot
     * hold is retryable once the queue drains and the arena recycles. */
    switch (event_scratch_classify_bytes(s, d->track_properties_len)) {
    case MOQ_EVENT_SCRATCH_PERMANENT:
        return close_with_error(s, 0x1, "event scratch permanently too small");
    case MOQ_EVENT_SCRATCH_BLOCKED:
        return MOQ_ERR_WOULD_BLOCK;
    case MOQ_EVENT_SCRATCH_FITS:
        break;
    }
    if (d->track_properties_len > 0) {
        ev_props.data = event_scratch_copy(s, d->track_properties,
                                     d->track_properties_len);
        ev_props.len = d->track_properties_len;
        if (!ev_props.data) {
            /* The preflight said it fits: a failure here is an internal
             * inconsistency with no unblock edge to wait on. */
            s->event_scratch_len = scratch_saved;
            return close_with_error(s, 0x1,
                "internal: event scratch preflight mismatch");
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

    /* Merge the peer-reported Largest Object into the registry. Observing a largest pins the record, so it persists
     * past the ts_free_entry below (which only reclaims empty reservations). */
    if (d->has_largest && e->hist)
        track_hist_merge(e->hist, d->largest_group, d->largest_object);

    if (s->profile->premature_fin_fails_request) {
        (void)moq_request_lifetime_initial_response_committed(&e->rs.lifetime, false, true);
        e->response_validated = false;
        ts_apply_deferred_signals(e);
    }

    /* Stream-correlated profiles keep the request bidi drainable until its FIN
     * (the request-stream handler frees it); draft-16 frees now. */
    if (e->rs.ref._v != 0)
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
        /* The emitter reports a terminal close as MOQ_OK, so the teardown below
         * runs only while the session is still open. */
        if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    } else {
        moq_bytes_t ev_reason = {0};
        switch (event_scratch_classify_bytes(s, reason_len)) {
        case MOQ_EVENT_SCRATCH_PERMANENT:
            return close_with_error(s, 0x1,
                "event scratch permanently too small");
        case MOQ_EVENT_SCRATCH_BLOCKED:
            return MOQ_ERR_WOULD_BLOCK;
        case MOQ_EVENT_SCRATCH_FITS:
            break;
        }
        if (reason_len > 0) {
            ev_reason.data = event_scratch_copy(s, reason, reason_len);
            ev_reason.len = reason_len;
            if (!ev_reason.data) {
                s->event_scratch_len = scratch_saved;
                return close_with_error(s, 0x1,
                    "internal: event scratch preflight mismatch");
            }
        }

        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_TRACK_STATUS_ERROR;
        ev.detail_size = (uint32_t)sizeof(moq_track_status_error_event_t);
        ev.borrow_epoch = s->borrow_epoch;
        ev.u.track_status_error.handle = e->handle;
        ev.u.track_status_error.error_code =
            s->profile->semantic_request_error(error_code);
        ev.u.track_status_error.can_retry = can_retry;
        ev.u.track_status_error.retry_after_ms = retry_after_ms;
        ev.u.track_status_error.reason = ev_reason;

        rc = push_event(s, &ev);
        if (rc < 0) {
            s->event_scratch_len = scratch_saved;
            return rc;
        }
    }

    if (s->profile->premature_fin_fails_request) {
        (void)moq_request_lifetime_initial_response_committed(&e->rs.lifetime, false, false);
        e->response_validated = false;
        ts_apply_deferred_signals(e);
    }

    /* Stream-correlated profiles keep the request bidi drainable until its FIN;
     * draft-16 frees now. */
    if (e->rs.ref._v != 0)
        e->state = MOQ_TS_DRAINING_RESPONSE;
    else
        ts_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Outbound track_status request --------------------------------- */

/* The layout through auth_token_count, rounded to the struct's alignment:
 * the sizeof of the struct before has_include_properties was appended. */
#define MOQ_TRACK_STATUS_CFG_SIZE_THROUGH_TOKENS \
    ((offsetof(moq_track_status_cfg_t, auth_token_count) + \
      sizeof(((moq_track_status_cfg_t *)0)->auth_token_count) + \
      (_Alignof(moq_track_status_cfg_t) - 1)) & \
     ~(_Alignof(moq_track_status_cfg_t) - 1))

/* Parenthesized: defines the symbol, not the header's sized macro. */
void (moq_track_status_cfg_init)(moq_track_status_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_TRACK_STATUS_CFG_SIZE_THROUGH_TOKENS);
    cfg->struct_size = (uint32_t)MOQ_TRACK_STATUS_CFG_SIZE_THROUGH_TOKENS;
}

void moq_track_status_cfg_init_sized(moq_track_status_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
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
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count,
                                 s->profile->auth_token_type_max) < 0)
        return MOQ_ERR_INVAL;
    if (moq_validate_full_track_name_min_fields(
            &cfg->track_namespace, cfg->track_name,
            s->profile->min_track_namespace_fields) < 0)
        return MOQ_ERR_INVAL;
    const bool has_include_properties =
        TS_CFG_HAS(include_properties) && cfg->has_include_properties;
    const bool include_properties = has_include_properties && cfg->include_properties;
    if (has_include_properties && !s->profile->include_properties_request)
        return MOQ_ERR_UNSUPPORTED;
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

    /* Reserve this track's registry record and retain its canonical key
     *: the inbound TRACK_STATUS response merges its Largest Object
     * into this record, so the response must be attributable to a track. A full
     * registry fails the local request with NOMEM (released on the pre-commit
     * failure paths below and in ts_free_entry). */
    size_t tskey_len = 0;
    uint8_t *tskey = moq_build_track_key(s, &cfg->track_namespace,
                                         cfg->track_name, &tskey_len);
    if (!tskey && tskey_len > 0) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_NOMEM;
    }
    moq_track_hist_t *tshist = track_hist_reserve(s, tskey, tskey_len);
    if (!tshist) {
        if (tskey) s->alloc.free(tskey, tskey_len, s->alloc.ctx);
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_NOMEM;
    }

    moq_track_status_encode_args_t args = {
        .request_id = req_ep.request_id,
        .track_namespace = cfg->track_namespace,
        .track_name = cfg->track_name,
        .auth_tokens = auth_tokens,
        .auth_token_count = auth_token_count,
        .has_include_properties = has_include_properties,
        .include_properties = include_properties,
    };

    bool req_stream = moq_session_uses_request_streams(s);
    moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0);

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);
    rc = s->profile->encode_track_status(s, &w, &args);
    if (rc < 0) {
        track_hist_release(s, tshist);
        if (tskey) s->alloc.free(tskey, tskey_len, s->alloc.ctx);
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
        track_hist_release(s, tshist);
        if (tskey) s->alloc.free(tskey, tskey_len, s->alloc.ctx);
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
    e->track_id_buf = tskey;
    e->track_id_len = tskey_len;
    e->hist = tshist;
    if (s->profile->premature_fin_fails_request) {
        (void)moq_request_lifetime_init(&e->rs.lifetime, MOQ_REQUEST_FAMILY_TRACK_STATUS, true);
        (void)moq_request_lifetime_fin_committed(&e->rs.lifetime);
    }

    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_TRACK_STATUS,
                                       s->session_tag, live_gen,
                                       (uint32_t)slot);
    moq_track_status_handle_t h = { packed };
    e->handle = h;

    req_ep.kind = MOQ_REQ_TRACK_STATUS;
    req_ep.slot = slot;
    if (req_stream) {
        e->rs.ref = req_ref;
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

/* The layout through expires_ms, rounded to the struct's alignment: the
 * sizeof of the struct before track_properties was appended. */
#define MOQ_ACCEPT_TRACK_STATUS_CFG_SIZE_THROUGH_EXPIRES \
    ((offsetof(moq_accept_track_status_cfg_t, expires_ms) + \
      sizeof(((moq_accept_track_status_cfg_t *)0)->expires_ms) + \
      (_Alignof(moq_accept_track_status_cfg_t) - 1)) & \
     ~(_Alignof(moq_accept_track_status_cfg_t) - 1))

/* Parenthesized: defines the symbol, not the header's sized macro. */
void (moq_accept_track_status_cfg_init)(moq_accept_track_status_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_ACCEPT_TRACK_STATUS_CFG_SIZE_THROUGH_EXPIRES);
    cfg->struct_size = (uint32_t)MOQ_ACCEPT_TRACK_STATUS_CFG_SIZE_THROUGH_EXPIRES;
}

void moq_accept_track_status_cfg_init_sized(moq_accept_track_status_cfg_t *cfg,
                                            size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
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

    /* The answer's Track Properties are checked as the SUBSCRIBE_OK's would
     * be, before anything advances. */
    moq_bytes_t props = { NULL, 0 };
    if (ATS_CFG_HAS(track_properties)) props = cfg->track_properties;
    if (props.len > 0 && !props.data) return MOQ_ERR_INVAL;
    if (s->profile->validate_track_properties &&
        s->profile->validate_track_properties(props.data, props.len) < 0)
        return MOQ_ERR_INVAL;

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

    if (s->profile->premature_fin_fails_request) {
        if (e->terminal_cause) return MOQ_ERR_WRONG_STATE;
        if (e->response_validated) return MOQ_ERR_WOULD_BLOCK;
        moq_result_t lrc = moq_request_lifetime_check_initial_response(&e->rs.lifetime, true);
        if (lrc != MOQ_OK) return lrc;
    }

    moq_track_status_ok_encode_args_t ok_args;
    memset(&ok_args, 0, sizeof(ok_args));
    ok_args.request_id = e->request_id;
    if (ATS_CFG_HAS(largest_object) && cfg->has_largest) {
        ok_args.has_largest = true;
        ok_args.largest_group = cfg->largest_group;
        ok_args.largest_object = cfg->largest_object;
    }
    /* §9.13: the parameters a SUBSCRIBE_OK would carry, so the same Largest
     * Object -- the larger of the application's and this track's registry
     * record, as moq_session_accept_subscribe() reports it. */
    const moq_track_hist_t *th = e->hist;
    if (th && th->has_largest &&
        (!ok_args.has_largest || th->largest_group > ok_args.largest_group ||
         (th->largest_group == ok_args.largest_group &&
          th->largest_object > ok_args.largest_object))) {
        ok_args.has_largest = true;
        ok_args.largest_group = th->largest_group;
        ok_args.largest_object = th->largest_object;
    }
    if (ATS_CFG_HAS(expires_ms) && cfg->has_expires) {
        ok_args.has_expires = true;
        ok_args.expires_ms = cfg->expires_ms;
    }
    /* §9.20.21: INCLUDE_PROPERTIES = 0 asks for empty Track Properties. */
    if (!e->omit_track_properties) {
        ok_args.track_properties = props.data;
        ok_args.track_properties_len = props.len;
    }
#undef ATS_CFG_HAS
#undef ATS_CFG_MIN

    /* Stream-correlated profiles free the request bidi after the terminal
     * response, so reserve a drain slot up front (unless the requester already
     * FIN'd, in which case the bidi is fully closed and nothing late arrives). */
    bool req_stream = (e->rs.ref._v != 0);
    bool need_drain = req_stream && !ts_peer_fin_observed(e);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    /* Encoded in place at the send buffer's tail and committed only once the
     * action is queued. A shortfall the buffer can recover from (it holds
     * other output) is retryable; one it never can is not. */
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len, s->send_cap - s->send_len);
    moq_result_t rc = s->profile->encode_track_status_ok(s, &w, &ok_args);
    if (rc == MOQ_ERR_BUFFER && s->send_len > 0) return MOQ_ERR_WOULD_BLOCK;
    if (rc < 0) return rc;
    size_t ok_len = moq_buf_writer_offset(&w);

    moq_action_t act;
    memset(&act, 0, sizeof(act));
    act.borrow_epoch = s->borrow_epoch;
    if (req_stream) {
        act.kind = MOQ_ACTION_SEND_BIDI_STREAM;
        act.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
        act.u.send_bidi_stream.stream_ref = e->rs.ref;
        act.u.send_bidi_stream.data = s->send_buf + s->send_len;
        act.u.send_bidi_stream.len = ok_len;
        act.u.send_bidi_stream.fin = true;
    } else {
        act.kind = MOQ_ACTION_SEND_CONTROL;
        act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
        act.u.send_control.data = s->send_buf + s->send_len;
        act.u.send_control.len = ok_len;
    }
    rc = push_action(s, &act);
    if (rc < 0) return rc;
    s->send_len += ok_len;

    if (s->profile->premature_fin_fails_request) {
        (void)moq_request_lifetime_initial_response_committed(&e->rs.lifetime, true, true);
        (void)moq_request_lifetime_fin_committed(&e->rs.lifetime);
    }
    if (need_drain)
        (void)drain_ref_add(s, e->rs.ref);   /* slot reserved above */
    ts_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Reject track-status request ----------------------------------- */

/* Parenthesized: defines the symbol, not the header's sized macro. */
void (moq_reject_track_status_cfg_init)(moq_reject_track_status_cfg_t *cfg)
{
    moq_reject_track_status_cfg_init_sized(cfg, MOQ_REJECT_TRACK_STATUS_CFG_SIZE_THROUGH_REDIRECT);
}

void moq_reject_track_status_cfg_init_sized(moq_reject_track_status_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

moq_result_t moq_session_reject_track_status(moq_session_t *s,
    moq_track_status_handle_t handle,
    const moq_reject_track_status_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    /* Pre-redirect minimum: the original cfg was {struct_size, error_code}. */
    if (cfg->struct_size < MOQ_REJECT_TRACK_STATUS_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    /* The code must be representable in THIS profile's wire encoding; refuse
     * before any mutation rather than truncate (draft-16 encodes a QUIC
     * varint, draft-18 a vi64 spanning the full 64-bit range). */
    if (cfg->error_code > s->profile->request_error_wire_max)
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

    if (s->profile->premature_fin_fails_request) {
        if (e->terminal_cause) return MOQ_ERR_WRONG_STATE;
        if (e->response_validated) return MOQ_ERR_WOULD_BLOCK;
        moq_result_t lrc = moq_request_lifetime_check_initial_response(&e->rs.lifetime, true);
        if (lrc != MOQ_OK) return lrc;
    }

    bool req_stream = (e->rs.ref._v != 0);
    bool need_drain = req_stream && !ts_peer_fin_observed(e);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    moq_result_t rc;
    if (err_args.has_redirect) {
        rc = queue_request_error_bidi(s, e->rs.ref, &err_args);
    } else {
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew, &err_args);
        if (rc < 0) return rc;
        rc = req_stream
            ? queue_send_bidi(s, e->rs.ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
    }
    if (rc < 0) return rc;

    if (s->profile->premature_fin_fails_request) {
        (void)moq_request_lifetime_initial_response_committed(&e->rs.lifetime, true, false);
        (void)moq_request_lifetime_fin_committed(&e->rs.lifetime);
    }
    if (need_drain)
        (void)drain_ref_add(s, e->rs.ref);   /* slot reserved above */
    ts_free_entry(s, slot);
    return MOQ_OK;
}

moq_result_t moq_session_request_goaway_track_status(
    moq_session_t *s, moq_track_status_handle_t handle,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MOQ_REQUEST_GOAWAY_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    if (cfg->new_session_uri.len > 0 && !cfg->new_session_uri.data)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = ts_resolve_handle(s, handle);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->track_statuses[slot].terminal_cause) return MOQ_ERR_WRONG_STATE;
    if (s->track_statuses[slot].response_validated) return MOQ_ERR_WOULD_BLOCK;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_TRACK_STATUS, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}
