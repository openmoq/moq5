#include "session_internal.h"
#include "../internal/validate.h"

/* -- Publish pool -------------------------------------------------- */

int pub_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->pub_cap; i++)
        if (s->publishes[i].state == MOQ_PUB_FREE) return (int)i;
    return -1;
}

static moq_publication_t pub_make_handle(moq_session_t *s, size_t slot)
{
    moq_pub_entry_t *e = &s->publishes[slot];
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                       s->session_tag,
                                       e->generation, (uint32_t)slot);
    moq_publication_t h = { packed };
    return h;
}

int pub_resolve_handle(moq_session_t *s, moq_publication_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_PUBLICATION) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->pub_cap) return -1;
    if (s->publishes[slot].generation != gen) return -1;
    if (s->publishes[slot].state == MOQ_PUB_FREE) return -1;
    return (int)slot;
}

void pub_free_entry(moq_session_t *s, int slot)
{
    moq_pub_entry_t *e = &s->publishes[slot];
    request_registry_remove_by_id(s, e->request_id);
    if (e->update_pending)
        request_registry_remove_by_id(s, e->update_request_id);
    /* Stream-correlated profiles also key the entry by its PUBLISH request bidi. */
    if (e->request_stream_ref._v != 0)
        request_registry_remove_by_streamref(s, e->request_stream_ref);
    if (e->done_reason_buf) {
        s->alloc.free(e->done_reason_buf, e->done_reason_len, s->alloc.ctx);
        e->done_reason_buf = NULL;
        e->done_reason_len = 0;
    }
    uint32_t next_gen = e->generation + 1;
    /* Preserve the co-allocated request-bidi receive buffer across reuse. */
    uint8_t *recv_buf = e->req_recv_buf;
    size_t   recv_cap = e->req_recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_PUB_FREE;
    e->generation = next_gen;
    e->req_recv_buf = recv_buf;
    e->req_recv_cap = recv_cap;
}

/* Emit the deferred PUBLISH_FINISHED for a subscriber-role publication whose
 * Stream Count has now been satisfied, then free the entry. Returns
 * MOQ_ERR_WOULD_BLOCK if the event queue (or scratch) cannot take it yet; the
 * caller leaves done_pending set and retries via pub_reap_deferred_dones. */
static moq_result_t pub_finalize_done(moq_session_t *s, int slot)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    /* Stream-correlated profiles: if the request bidi has not FIN'd yet, freeing
     * the entry drops its by-streamref key, so a later FIN/bytes would be
     * misclassified. Reserve a drain-ref to absorb that late FIN. */
    bool need_drain = pe->request_stream_ref._v != 0 && !pe->req_recv_fin;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t ev_reason = {0};
    if (pe->done_reason_len > 0) {
        ev_reason.data = event_scratch_copy(s, pe->done_reason_buf,
                                            pe->done_reason_len);
        ev_reason.len = pe->done_reason_len;
        if (!ev_reason.data) {
            s->event_scratch_len = scratch_saved;
            /* Empty scratch that still cannot hold the reason is permanently too
             * small: fail terminally (mirrors the immediate PUBLISH_DONE path)
             * instead of retrying the deferred finish forever. */
            if (scratch_saved == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_PUBLISH_FINISHED;
    e.detail_size = (uint32_t)sizeof(moq_publish_finished_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.publish_finished.pub = pe->handle;
    e.u.publish_finished.status_code = pe->done_status_code;
    e.u.publish_finished.stream_count = pe->done_stream_count;
    e.u.publish_finished.reason = ev_reason;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }
    if (need_drain)
        (void)drain_ref_add(s, pe->request_stream_ref);   /* slot reserved above */
    pub_free_entry(s, slot);
    return MOQ_OK;
}

/* Count a completed (FIN'd) data stream toward a subscriber-role publication's
 * PUBLISH_DONE Stream Count. If a deferred PUBLISH_DONE is now satisfied,
 * finalize promptly (a WOULD_BLOCK is retried by pub_reap_deferred_dones). */
void pub_note_stream_processed(moq_session_t *s, moq_publication_t pub)
{
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return;
    moq_pub_entry_t *pe = &s->publishes[slot];
    if (pe->role != MOQ_PUB_ROLE_SUBSCRIBER) return;
    pe->processed_stream_count++;
    if (pe->done_pending &&
        pe->processed_stream_count >= pe->done_stream_count)
        (void)pub_finalize_done(s, slot);
}

/* Retry-finalize any deferred PUBLISH_DONE whose Stream Count is satisfied but
 * whose PUBLISH_FINISHED could not be queued earlier (event queue was full). */
void pub_reap_deferred_dones(moq_session_t *s)
{
    for (size_t i = 0; i < s->pub_cap; i++) {
        if (s->state == MOQ_SESS_CLOSED) return;  /* finalize may close (scratch) */
        moq_pub_entry_t *pe = &s->publishes[i];
        if (pe->done_pending &&
            pe->processed_stream_count >= pe->done_stream_count) {
            if (pub_finalize_done(s, (int)i) < 0)
                return;   /* event queue full: try again next advance */
        }
    }
}

bool pub_track_alias_in_use(moq_session_t *s, uint64_t alias)
{
    for (size_t i = 0; i < s->pub_cap; i++) {
        moq_pub_entry_t *e = &s->publishes[i];
        if (e->track_alias != alias) continue;
        if (e->state == MOQ_PUB_ESTABLISHED) return true;
        /* A subscriber-role publication reserves its alias from creation, not just
         * once established: a pending publication already routes early objects
         * (§9.4), so a second inbound PUBLISH reusing the alias is ambiguous and
         * must be rejected (the inbound-PUBLISH path closes with 0x5). */
        if (e->role == MOQ_PUB_ROLE_SUBSCRIBER &&
            e->state == MOQ_PUB_PENDING_SUBSCRIBER)
            return true;
    }
    return false;
}

bool pub_outbound_alias_in_use(moq_session_t *s, uint64_t alias)
{
    for (size_t i = 0; i < s->pub_cap; i++) {
        moq_pub_entry_t *e = &s->publishes[i];
        if (e->role != MOQ_PUB_ROLE_PUBLISHER) continue;
        if (e->state == MOQ_PUB_FREE) continue;
        if (e->track_alias == alias) return true;
    }
    return false;
}

/* Queue an already-encoded publish response. Stream-correlated profiles emit it
 * on the publication's request bidi (with `fin` finishing that stream for
 * terminal responses); others emit SEND_CONTROL on the shared control channel.
 * Both copy `data` into the send buffer and distinguish a permanent overflow
 * (MOQ_ERR_BUFFER) from a retryable shortfall (MOQ_ERR_WOULD_BLOCK on the bidi
 * path), so callers encode into a local buffer and route here. */
static moq_result_t queue_publish_response(moq_session_t *s, size_t slot,
                                           const uint8_t *data, size_t len,
                                           bool fin)
{
    if (moq_session_uses_request_streams(s))
        return queue_send_bidi(s, s->publishes[slot].request_stream_ref,
                               data, len, fin);
    return queue_send_control(s, data, len);
}

/* -- Copy namespace parts into output scratch ---------------------- */

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

/* -- Inbound PUBLISH handler --------------------------------------- */

moq_result_t session_core_on_publish(moq_session_t *s,
                                      moq_decoded_publish_t *d)
{
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    size_t scratch_saved = s->event_scratch_len;

    if (event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    /* Stream-correlated profiles route a pre-commit reject (auth / pool-full) as
     * REQUEST_ERROR + FIN on the request bidi and retire it via the drain ring:
     * the publisher's send half stays open (PUBLISH is not first-and-only), so a
     * late message before it sees our FIN must be discarded, not reclassified as a
     * fresh request. Draft-16 carries the reject on the control channel and never
     * drains (it also handled auth-reject in its profile decode). */
    bool reject_drain = d->endpoint.has_stream_ref;

    /* Message-level authorization-token reject (stream-correlated profiles): a
     * REGISTER carried alongside still commits its alias (§10.2.2). */
    if (d->auth_reject_code) {
        if (action_queue_full(s) ||
            (reject_drain && s->drain_ref_count >= s->drain_ref_cap)) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id,
                .error_code = d->auth_reject_code });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = d->endpoint.has_stream_ref
            ? queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        if (reject_drain)
            (void)drain_ref_add(s, d->endpoint.stream_ref);
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    /* §2.5.1: a PUBLISH whose Track Properties carry an unknown Mandatory Track
     * Property -> the subscriber MUST respond REQUEST_ERROR(UNSUPPORTED_EXTENSION).
     * Auto-reject on the request bidi (no PUBLISH_REQUEST event surfaces). */
    if (d->track_properties_unsupported) {
        if (action_queue_full(s) ||
            (reject_drain && s->drain_ref_count >= s->drain_ref_cap)) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id,
                .error_code = MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION,
                .reason = (const uint8_t *)"unsupported mandatory track property",
                .reason_len = 36 });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = d->endpoint.has_stream_ref
            ? queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        if (reject_drain)
            (void)drain_ref_add(s, d->endpoint.stream_ref);
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    int slot = pub_find_free(s);
    if (slot < 0) {
        if (action_queue_full(s) ||
            (reject_drain && s->drain_ref_count >= s->drain_ref_cap)) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = 0x0,
                .reason = (const uint8_t *)"publish pool full",
                .reason_len = 17 });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = d->endpoint.has_stream_ref
            ? queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
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

    moq_bytes_t ev_props = {0};
    if (d->track_properties_len > 0) {
        ev_props.data = event_scratch_copy(s, d->track_properties, d->track_properties_len);
        ev_props.len  = d->track_properties_len;
        if (!ev_props.data) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0) {
                result = close_with_error(s, 0x1,
                    "event scratch permanently too small");
                goto cleanup_all;
            }
            result = MOQ_ERR_BUFFER;
            goto cleanup_all;
        }
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

    if (sub_track_alias_in_use(s, d->track_alias) ||
        pub_track_alias_in_use(s, d->track_alias)) {
        result = close_with_error(s, 0x5, "duplicate track alias");
        goto cleanup_all;
    }

    moq_pub_entry_t *entry = &s->publishes[slot];
    uint32_t live_gen = entry->generation | 1;
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                       s->session_tag, live_gen,
                                       (uint32_t)slot);
    moq_publication_t handle = { packed };

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_PUBLISH_REQUEST;
    e.detail_size = (uint32_t)sizeof(moq_publish_request_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.publish_request.pub = handle;
    e.u.publish_request.track_namespace = ev_ns;
    e.u.publish_request.track_name = ev_name;
    e.u.publish_request.track_alias = d->track_alias;
    e.u.publish_request.forward = d->forward;
    e.u.publish_request.tokens = ev_tokens;
    e.u.publish_request.token_count = d->token_count;
    e.u.publish_request.track_properties = ev_props;
    e.u.publish_request.dynamic_groups = d->dynamic_groups;

    rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        result = rc;
        goto cleanup_all;
    }

    entry->generation = live_gen;
    entry->state = MOQ_PUB_PENDING_SUBSCRIBER;
    entry->role = MOQ_PUB_ROLE_SUBSCRIBER;
    entry->handle = handle;
    entry->request_id = d->request_id;
    entry->track_alias = d->track_alias;
    /* Gates outbound new-group requests on the accept and later updates. */
    entry->dynamic_groups = d->dynamic_groups;
    /* Initial Forward State (§9.4): with FORWARD omitted/1 the publisher may begin
     * sending objects immediately, possibly before our PUBLISH_OK. Record it so the
     * inbound data path accepts those early objects on this still-pending
     * publication (send_allowed is only consumed by the publisher-role send path, so
     * setting it on this subscriber-role entry is inert there). */
    entry->send_allowed = d->forward;
    d->endpoint.kind = MOQ_REQ_PUBLISH;
    d->endpoint.slot = slot;
    if (d->endpoint.has_stream_ref) {
        /* Hand the request bidi off from the staging sub slot to this publish
         * slot: re-key the stream-ref to (PUBLISH, slot). The request-stream
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

/* -- Inbound PUBLISH_OK handler ------------------------------------ */

moq_result_t session_core_on_publish_ok(moq_session_t *s,
                                         const moq_decoded_publish_ok_t *d)
{
    /* §10.2.13: NEW_GROUP_REQUEST in PUBLISH_OK requires that OUR track
     * properties advertised DYNAMIC_GROUPS == 1; otherwise the peer violated
     * a MUST NOT -- protocol violation, never delivered. */
    if (d->has_new_group_request &&
        !s->publishes[d->target_slot].dynamic_groups)
        return close_with_error(s, 0x3,
            "NEW_GROUP_REQUEST without dynamic-group support");

    moq_pub_entry_t *e = &s->publishes[d->target_slot];

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    if (d->has_deferred_param_error)
        return close_with_error(s, 0x3, d->deferred_param_reason);

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_OK;
    ev.detail_size = (uint32_t)sizeof(moq_publish_ok_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.publish_ok.pub = e->handle;
    ev.u.publish_ok.send_allowed = d->forward;
    ev.u.publish_ok.subscriber_priority = d->subscriber_priority;
    ev.u.publish_ok.group_order = d->group_order;
    ev.u.publish_ok.has_delivery_timeout = d->has_delivery_timeout;
    ev.u.publish_ok.delivery_timeout_ms = d->delivery_timeout_ms;
    ev.u.publish_ok.has_expires = d->has_expires;
    ev.u.publish_ok.expires_ms = d->expires_ms;
    ev.u.publish_ok.has_new_group_request = d->has_new_group_request;
    ev.u.publish_ok.new_group_request = d->new_group_request;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;

    e->state = MOQ_PUB_ESTABLISHED;
    e->send_allowed = d->forward;
    e->subscriber_priority = d->subscriber_priority;
    e->group_order = d->group_order;
    e->has_delivery_timeout = d->has_delivery_timeout;
    e->delivery_timeout_ms = d->delivery_timeout_ms;
    e->has_expires = d->has_expires;
    e->expires_ms = d->expires_ms;
    return MOQ_OK;
}

/* -- Inbound PUBLISH error (REQUEST_ERROR for publish) ------------- */

moq_result_t session_core_on_publish_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len, bool free_now)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    /* Stream-correlated profiles keep the entry to drain the request bidi's FIN
     * and close our (the publisher's) still-open send half reciprocally. Reserve
     * the close action up front (reserve-before-mutate). */
    bool close_half = !free_now && pe->request_stream_ref._v != 0;
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

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

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_PUBLISH_ERROR;
    e.detail_size = (uint32_t)sizeof(moq_publish_error_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.publish_error.pub = s->publishes[slot].handle;
    e.u.publish_error.error_code = (moq_request_error_t)error_code;
    e.u.publish_error.can_retry = can_retry;
    e.u.publish_error.retry_after_ms = retry_after_ms;
    e.u.publish_error.reason = ev_reason;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    if (free_now) {
        pub_free_entry(s, slot);
    } else {
        if (close_half)
            (void)queue_close_bidi(s, pe->request_stream_ref);
        pe->state = MOQ_PUB_DRAINING_RESPONSE;
    }
    return MOQ_OK;
}

/* -- Public API ---------------------------------------------------- */

void moq_publish_cfg_init(moq_publish_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_publish_cfg_t);
}

void moq_accept_publish_cfg_init(moq_accept_publish_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_accept_publish_cfg_t);
}

void moq_reject_publish_cfg_init(moq_reject_publish_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_reject_publish_cfg_t);
}

moq_result_t moq_session_publish(moq_session_t *s,
                                  const moq_publish_cfg_t *cfg,
                                  uint64_t now_us,
                                  moq_publication_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
#define PUB_CFG_MIN offsetof(moq_publish_cfg_t, auth_tokens)
    if (cfg->struct_size < PUB_CFG_MIN) return MOQ_ERR_INVAL;
#define PUB_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_publish_cfg_t, f) + sizeof(cfg->f))
    *out_handle = MOQ_PUBLICATION_INVALID;

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (PUB_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }

    session_begin_advance(s, now_us);

    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    if (moq_validate_full_track_name(&cfg->track_namespace,
                                      cfg->track_name) < 0)
        return MOQ_ERR_INVAL;
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;
#undef PUB_CFG_HAS
#undef PUB_CFG_MIN

    moq_request_endpoint_t req_ep;
    {
        moq_result_t prc = s->profile->prepare_request(s, &req_ep);
        if (prc < 0) return prc;
    }

    int slot = pub_find_free(s);
    if (slot < 0) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (action_queue_full(s)) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }
    {
        bool forward = cfg->has_forward ? cfg->forward : true;

        uint64_t alias;
        uint64_t next_alias_after;
        if (cfg->has_track_alias) {
            alias = cfg->track_alias;
            if (pub_outbound_alias_in_use(s, alias) ||
                sub_track_alias_in_use(s, alias)) {
                s->profile->abort_request(s, &req_ep);
                return MOQ_ERR_INVAL;
            }
            next_alias_after = s->profile->next_track_alias(s);
        } else {
            alias = s->profile->next_track_alias(s);
            bool found = false;
            for (size_t attempts = 0; attempts < s->pub_cap + s->sub_cap + 1;
                 attempts++) {
                if (!pub_outbound_alias_in_use(s, alias) &&
                    !sub_track_alias_in_use(s, alias)) {
                    found = true; break;
                }
                alias++;
                if (alias == 0) alias = 1;
            }
            if (!found) {
                s->profile->abort_request(s, &req_ep);
                return MOQ_ERR_INTERNAL;
            }
            next_alias_after = alias + 1;
            if (next_alias_after == 0) next_alias_after = 1;
        }

        moq_publish_encode_args_t args = {
            .request_id = req_ep.request_id,
            .track_namespace = cfg->track_namespace,
            .track_name = cfg->track_name,
            .track_alias = alias,
            .has_forward = true,
            .forward = forward,
            .track_properties = cfg->track_properties.data,
            .track_properties_len = cfg->track_properties.len,
            .auth_tokens = auth_tokens,
            .auth_token_count = auth_token_count,
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);

        moq_result_t rc2 = s->profile->encode_publish(s, &w, &args);
        if (rc2 < 0) {
            s->profile->abort_request(s, &req_ep);
            return rc2;
        }

        size_t encoded_len = moq_buf_writer_offset(&w);
        /* Stream-correlated profiles open a dedicated bidi for the PUBLISH; the
         * subscription lives on it (no FIN) and the response correlates by the
         * stream. Others send the request on the shared control channel. */
        bool req_stream = moq_session_uses_request_streams(s);
        moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0);
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.borrow_epoch = s->borrow_epoch;
        if (req_stream) {
            req_ref = moq_stream_ref_from_u64(s->next_stream_ref);
            act.kind = MOQ_ACTION_OPEN_BIDI_STREAM;
            act.detail_size = (uint32_t)sizeof(moq_open_bidi_stream_action_t);
            act.u.open_bidi_stream.stream_ref = req_ref;
            act.u.open_bidi_stream.data = s->send_buf + s->send_len;
            act.u.open_bidi_stream.len = encoded_len;
        } else {
            act.kind = MOQ_ACTION_SEND_CONTROL;
            act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
            act.u.send_control.data = s->send_buf + s->send_len;
            act.u.send_control.len = encoded_len;
        }
        moq_result_t arc = push_action(s, &act);
        if (arc < 0) {
            s->profile->abort_request(s, &req_ep);
            return arc;
        }
        s->send_len += encoded_len;

        moq_pub_entry_t *entry = &s->publishes[slot];
        entry->generation |= 1;
        entry->state = MOQ_PUB_PENDING_PUBLISHER;
        entry->role = MOQ_PUB_ROLE_PUBLISHER;
        entry->request_id = req_ep.request_id;
        entry->track_alias = alias;
        /* Latch whether OUR track properties advertised dynamic groups:
         * inbound new-group requests on PUBLISH_OK / publication updates are
         * gated on it (the peer MUST NOT send one otherwise, §10.2.13). */
        entry->dynamic_groups = s->profile->track_properties_dynamic_groups(
            cfg->track_properties.data, cfg->track_properties.len);
        entry->handle = pub_make_handle(s, (size_t)slot);
        req_ep.kind = MOQ_REQ_PUBLISH;
        req_ep.slot = slot;
        if (req_stream) {
            /* Correlate the response by the request's bidi stream identity. */
            entry->request_stream_ref = req_ref;
            req_ep.has_stream_ref = true;
            req_ep.stream_ref = req_ref;
            request_registry_insert_by_streamref(s, req_ref, req_ep);
            s->next_stream_ref++;
        } else {
            request_registry_insert_by_id(s, req_ep.request_id, req_ep);
        }

        *out_handle = entry->handle;
        s->profile->advance_track_alias(s, next_alias_after);
        s->profile->commit_request(s, &req_ep);
    }
    return MOQ_OK;
}

moq_result_t moq_session_accept_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_accept_publish_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    /* ABI-additive cfg: the new-group-request fields are appended; callers
     * compiled against the smaller struct remain valid and send nothing. */
#define ACC_CFG_MIN offsetof(moq_accept_publish_cfg_t, has_new_group_request)
#define ACC_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_accept_publish_cfg_t, f) + \
     sizeof(cfg->f))
    if (cfg->struct_size < ACC_CFG_MIN) return MOQ_ERR_INVAL;
    bool has_new_group_request = false;
    uint64_t new_group_request = 0;
    if (ACC_CFG_HAS(new_group_request) && cfg->has_new_group_request) {
        has_new_group_request = true;
        new_group_request = cfg->new_group_request;
    }
#undef ACC_CFG_HAS
#undef ACC_CFG_MIN
    if (cfg->group_order != MOQ_GROUP_ORDER_DEFAULT &&
        cfg->group_order != MOQ_GROUP_ORDER_ASCENDING &&
        cfg->group_order != MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_PENDING_SUBSCRIBER)
        return MOQ_ERR_WRONG_STATE;
    /* §10.2.13: a new-group request may ride PUBLISH_OK only when the track
     * carried DYNAMIC_GROUPS == 1. Refused before any mutation. */
    if (has_new_group_request && !s->publishes[slot].dynamic_groups)
        return MOQ_ERR_INVAL;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t priority = cfg->has_subscriber_priority ?
                           cfg->subscriber_priority : 128;
        moq_result_t rc2 = s->profile->encode_publish_ok(s, &w,
            s->publishes[slot].request_id, priority,
            (uint8_t)cfg->group_order,
            has_new_group_request, new_group_request);
        if (rc2 < 0) return rc2;

        /* PUBLISH_OK carries no FIN -- the subscription lives on the bidi. A
         * send-buffer shortfall is retryable (WOULD_BLOCK), not BUFFER. */
        moq_result_t arc = queue_publish_response(s, (size_t)slot, buf,
                                                  moq_buf_writer_offset(&w),
                                                  false);
        if (arc < 0) return arc;
    }

    s->publishes[slot].state = MOQ_PUB_ESTABLISHED;
    return MOQ_OK;
}

moq_result_t moq_session_reject_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_reject_publish_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_reject_publish_cfg_t))
        return MOQ_ERR_INVAL;
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;
    /* PUBLISH is not REDIRECT-eligible (§10.6 lists 5 families; PUBLISH not among
     * them); refuse a REDIRECT code rather than emit an unacceptable response. */
    if (cfg->error_code == MOQ_REQUEST_ERROR_REDIRECT) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_PENDING_SUBSCRIBER)
        return MOQ_ERR_WRONG_STATE;

    /* Stream-correlated profiles finish the request bidi with the terminal
     * REQUEST_ERROR and free the entry; reserve a drain slot up front for the
     * peer's late FIN (unless it already arrived). */
    moq_pub_entry_t *pe = &s->publishes[slot];
    bool req_stream = (pe->request_stream_ref._v != 0);
    bool need_drain = req_stream && !pe->req_recv_fin;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_request_error_encode_args_t err_args = {
            .request_id = pe->request_id,
            .error_code = (uint64_t)cfg->error_code,
            .can_retry = cfg->can_retry,
            .retry_after_ms = cfg->retry_after_ms,
            .reason = cfg->reason.data,
            .reason_len = cfg->reason.len,
        };
        moq_result_t rc2 = s->profile->encode_request_error(s, &w, &err_args);
        if (rc2 < 0) return rc2;

        /* Terminal: finish the request bidi (FIN) for stream-correlated profiles.
         * A send-buffer shortfall is retryable (WOULD_BLOCK), not BUFFER. */
        moq_result_t arc = queue_publish_response(s, (size_t)slot, buf,
                                                  moq_buf_writer_offset(&w),
                                                  req_stream);
        if (arc < 0) return arc;
    }

    if (need_drain)
        (void)drain_ref_add(s, pe->request_stream_ref);   /* slot reserved above */
    pub_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Inbound PUBLISH_DONE handler ---------------------------------- */

moq_result_t session_core_on_publish_done(moq_session_t *s,
                                           const moq_decoded_publish_done_t *d,
                                           bool free_now)
{
    moq_pub_entry_t *pe = &s->publishes[d->target_slot];
    bool need_tomb = pe->update_pending;
    /* A PUBLISH_DONE that finishes a publication with a pending REQUEST_UPDATE
     * must leave a tombstone so the peer's late REQUEST_OK/ERROR for that update
     * is absorbed rather than treated as an unknown request. If the shared
     * tombstone array is full this is NOT recoverable backpressure: PUBLISH_DONE
     * arrives on the ordered control stream, and the only messages that free
     * tombstones (REQUEST_OK/ERROR) are queued behind it -- a WOULD_BLOCK here
     * would stall the control stream permanently. A full array
     * (sub_cap + pub_cap outstanding responses) is a peer-induced resource
     * exhaustion, so fail closed. */
    if (need_tomb && s->unsub_tomb_count + 1 > s->unsub_tomb_cap)
        return close_with_error(s, 0x3, "publish done tombstone budget exceeded");

    /* Stream-correlated profiles keep the entry to drain the request bidi's FIN
     * and close our (the subscriber's) still-open send half reciprocally. Reserve
     * the close action up front (reserve-before-mutate). */
    bool close_half = !free_now && pe->request_stream_ref._v != 0;

    /* Stream-Count gating (draft-16 §9.15): PUBLISH_DONE arrives on the control
     * channel and is likely to precede late-arriving / late-opening data streams.
     * Keep the publication ESTABLISHED (so those streams still bind its alias and
     * deliver objects) until the advertised number of data streams have been
     * processed; only then surface PUBLISH_FINISHED and free the entry. A Stream
     * Count of 0 (datagram-only) or the 2^62-1 "unknown" sentinel finalizes
     * immediately -- the sans-I/O core has no timer to bound the unknown case
     * (the embedding's session/idle timeout does). */
    if (pe->role == MOQ_PUB_ROLE_SUBSCRIBER &&
        d->stream_count != 0 && d->stream_count != MOQ_QUIC_VARINT_MAX &&
        pe->processed_stream_count < d->stream_count) {
        if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        uint8_t *rbuf = NULL;
        if (d->reason_len > 0) {
            rbuf = (uint8_t *)s->alloc.alloc(d->reason_len, s->alloc.ctx);
            if (!rbuf) return MOQ_ERR_NOMEM;
            memcpy(rbuf, d->reason, d->reason_len);
        }
        if (need_tomb) unsub_tomb_add(s, pe->update_request_id);
        if (pe->done_reason_buf)   /* a re-DONE shouldn't happen; be safe */
            s->alloc.free(pe->done_reason_buf, pe->done_reason_len, s->alloc.ctx);
        pe->done_reason_buf = rbuf;
        pe->done_reason_len = d->reason_len;
        pe->done_status_code = d->status_code;
        pe->done_stream_count = d->stream_count;
        pe->done_pending = true;
        if (close_half)
            (void)queue_close_bidi(s, pe->request_stream_ref);
        return MOQ_OK;
    }

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t ev_reason = {0};
    if (d->reason_len > 0) {
        ev_reason.data = event_scratch_copy(s, d->reason, d->reason_len);
        ev_reason.len = d->reason_len;
        if (!ev_reason.data) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_PUBLISH_FINISHED;
    e.detail_size = (uint32_t)sizeof(moq_publish_finished_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.publish_finished.pub = s->publishes[d->target_slot].handle;
    e.u.publish_finished.status_code = d->status_code;
    e.u.publish_finished.stream_count = d->stream_count;
    e.u.publish_finished.reason = ev_reason;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    if (need_tomb)
        unsub_tomb_add(s, pe->update_request_id);
    if (free_now) {
        pub_free_entry(s, d->target_slot);
    } else {
        if (close_half)
            (void)queue_close_bidi(s, pe->request_stream_ref);
        pe->state = MOQ_PUB_DRAINING_RESPONSE;
    }
    return MOQ_OK;
}

/* -- Inbound UNSUBSCRIBE for publisher-role publication ------------ */

moq_result_t session_core_on_publish_unsubscribed(moq_session_t *s, int slot)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    moq_publication_t pub_handle = pe->handle;

    bool need_recompute = false;
    if (pe->state == MOQ_PUB_ESTABLISHED) {
        for (size_t i = 0; i < s->sg_cap; i++) {
            if (s->subgroups[i].state != MOQ_SG_OPEN &&
                s->subgroups[i].state != MOQ_SG_STREAMING)
                continue;
            /* Only reset publication-backed subgroups: a subscription-backed
             * subgroup carries an invalid pub handle, so it is never confused
             * with a real publication (defense in depth alongside clearing
             * stale handles on free/open). */
            if (!moq_publication_is_valid(s->subgroups[i].pub))
                continue;
            if (!moq_publication_eq(s->subgroups[i].pub, pub_handle))
                continue;
            if (action_queue_full(s)) {
                if (need_recompute) sg_recompute_deadline(s);
                return MOQ_ERR_WOULD_BLOCK;
            }
            moq_action_t a;
            memset(&a, 0, sizeof(a));
            a.kind = MOQ_ACTION_RESET_DATA;
            a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
            a.borrow_epoch = s->borrow_epoch;
            a.u.reset_data.stream_ref = s->subgroups[i].stream_ref;
            a.u.reset_data.error_code = 0x1;
            moq_result_t rc = push_action(s, &a);
            if (rc < 0) {
                if (need_recompute) sg_recompute_deadline(s);
                return rc;
            }
            s->subgroups[i].state = MOQ_SG_RESETTING;
            s->subgroups[i].streaming_payload_len = 0;
            s->subgroups[i].streaming_bytes_written = 0;
            if (s->subgroups[i].delivery_deadline_us != UINT64_MAX) {
                s->subgroups[i].delivery_deadline_us = UINT64_MAX;
                need_recompute = true;
            }
        }
    }
    if (need_recompute) sg_recompute_deadline(s);

    if (event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_UNSUBSCRIBED;
    ev.detail_size = (uint32_t)sizeof(moq_publish_unsubscribed_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.publish_unsubscribed.pub = pub_handle;
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;

    pub_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Inbound REQUEST_UPDATE for publisher-role publication ----------- */

moq_result_t session_core_on_publish_request_update(
    moq_session_t *s, moq_decoded_request_update_t *d)
{
    /* Stream-correlated profiles carry the REQUEST_UPDATE acknowledgement on the
     * PUBLISH request bidi (no FIN -- the subscription continues); draft-16 uses
     * the shared control channel. */
    bool req_stream = d->endpoint.has_stream_ref;
    moq_stream_ref_t resp_ref = d->endpoint.stream_ref;

    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    size_t scratch_saved = s->event_scratch_len;

    /* §10.2.13: as with PUBLISH_OK, an inbound new-group request on a
     * publication update is a protocol violation unless OUR track
     * properties advertised DYNAMIC_GROUPS == 1. */
    if (d->has_new_group_request &&
        !s->publishes[d->target_slot].dynamic_groups) {
        process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
                                         d->token_count);
        process_auth_tokens_abort_txn(s, &d->auth_txn);
        return close_with_error(s, 0x3,
            "NEW_GROUP_REQUEST without dynamic-group support");
    }

    /* An unsupported param or a message-level authorization-token reject
     * answers REQUEST_ERROR; the publication continues (unlike subscription
     * updates there is no terminal UPDATE_FAILED sequence here). A REGISTER
     * carried alongside still commits its alias (the spec's
     * register-even-on-reject rule), so the auth txn commits on this path. */
    if (d->has_unsupported || d->auth_reject_code) {
        uint64_t err_code = d->auth_reject_code
            ? d->auth_reject_code : MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        if (action_queue_full(s)) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id,
                .error_code = err_code });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = req_stream
            ? queue_send_bidi(s, resp_ref, err_buf,
                              moq_buf_writer_offset(&ew), false)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    if (event_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }
    if (action_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }

    /* Copy resolved auth tokens into scratch for borrow-epoch-safe delivery. */
    moq_resolved_token_t *ev_tokens = NULL;
    rc = session_stage_tokens_for_event(s, d->tokens, d->token_staged,
                                        d->token_count, scratch_saved,
                                        &ev_tokens);
    if (rc < 0) { result = rc; goto cleanup_all; }

    {
        uint8_t ok_buf[128];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));
        rc = s->profile->encode_request_ok(s, &ow, d->request_id);
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = req_stream
            ? queue_send_bidi(s, resp_ref, ok_buf,
                              moq_buf_writer_offset(&ow), false)
            : queue_send_control(s, ok_buf, moq_buf_writer_offset(&ow));
        if (rc < 0) { result = rc; goto cleanup_all; }
    }

    moq_pub_entry_t *e = &s->publishes[d->target_slot];

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_UPDATED;
    ev.detail_size = (uint32_t)sizeof(moq_publish_updated_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.publish_updated.pub = e->handle;
    ev.u.publish_updated.has_subscriber_priority = d->has_subscriber_priority;
    ev.u.publish_updated.subscriber_priority = d->subscriber_priority;
    ev.u.publish_updated.has_forward = d->has_forward;
    ev.u.publish_updated.forward = d->forward;
    ev.u.publish_updated.has_delivery_timeout = d->has_delivery_timeout;
    ev.u.publish_updated.delivery_timeout_us = d->delivery_timeout_us;
    ev.u.publish_updated.tokens = ev_tokens;
    ev.u.publish_updated.token_count = d->token_count;
    ev.u.publish_updated.has_new_group_request = d->has_new_group_request;
    ev.u.publish_updated.new_group_request = d->new_group_request;

    rc = push_event(s, &ev);
    if (rc < 0) { result = rc; goto cleanup_all; }

    if (d->has_subscriber_priority)
        e->subscriber_priority = d->subscriber_priority;
    if (d->has_forward)
        e->send_allowed = d->forward;
    if (d->has_delivery_timeout)
        e->delivery_timeout_ms = d->delivery_timeout_us / 1000;
    s->profile->commit_inbound_request(s, &d->endpoint);
    auth_committed = true;
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    return MOQ_OK;

cleanup_all:
    /* No event was surfaced on a failure path: roll back any token values
     * staged into output scratch so a retryable WOULD_BLOCK does not leak
     * scratch (mirrors the subscription update handler). */
    s->event_scratch_len = scratch_saved;
    process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
                                     d->token_count);
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

/* -- Update publication (subscriber side) -------------------------- */

void moq_publication_update_cfg_init(moq_publication_update_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_publication_update_cfg_t);
}

moq_result_t moq_session_update_publication(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_publication_update_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    /* ABI-additive cfg: auth_tokens/auth_token_count are appended fields;
     * callers compiled against the smaller struct remain valid and send
     * no tokens. */
#define UPD_CFG_MIN offsetof(moq_publication_update_cfg_t, auth_tokens)
#define UPD_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_publication_update_cfg_t, f) + \
     sizeof(cfg->f))
    if (cfg->struct_size < UPD_CFG_MIN) return MOQ_ERR_INVAL;

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (UPD_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }
    bool has_new_group_request = false;
    uint64_t new_group_request = 0;
    if (UPD_CFG_HAS(new_group_request) && cfg->has_new_group_request) {
        has_new_group_request = true;
        new_group_request = cfg->new_group_request;
    }
#undef UPD_CFG_HAS
#undef UPD_CFG_MIN
    if (!cfg->has_subscriber_priority && !cfg->has_forward &&
        !cfg->has_delivery_timeout && auth_token_count == 0 &&
        !has_new_group_request)
        return MOQ_ERR_INVAL;
    if (cfg->has_delivery_timeout && cfg->delivery_timeout_us < 1000)
        return MOQ_ERR_INVAL;
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_pub_entry_t *e = &s->publishes[slot];
    if (e->role != MOQ_PUB_ROLE_SUBSCRIBER)
        return MOQ_ERR_WRONG_STATE;
    if (e->state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (e->goaway_sent) return MOQ_ERR_WRONG_STATE;   /* migrated: no REQUEST_UPDATE */
    if (e->update_pending)
        return MOQ_ERR_WRONG_STATE;
    /* A prior update failed; the publication is awaiting its terminal
     * PUBLISH_DONE(UPDATE_FAILED) and may not be updated again. */
    if (e->update_failed)
        return MOQ_ERR_WRONG_STATE;
    /* §10.2.13: a new-group request may ride a publication update only when
     * the PUBLISH carried DYNAMIC_GROUPS == 1. Refused before any mutation. */
    if (has_new_group_request && !e->dynamic_groups)
        return MOQ_ERR_INVAL;

    moq_request_endpoint_t req_ep;
    moq_result_t prc = s->profile->prepare_request(s, &req_ep);
    if (prc < 0) return prc;

    if (action_queue_full(s)) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    moq_request_update_encode_args_t args = {
        .request_id = req_ep.request_id,
        .existing_request_id = e->request_id,
        .has_subscriber_priority = cfg->has_subscriber_priority,
        .subscriber_priority = cfg->subscriber_priority,
        .has_forward = cfg->has_forward,
        .forward = cfg->forward,
        .has_delivery_timeout = cfg->has_delivery_timeout,
        .delivery_timeout_us = cfg->delivery_timeout_us,
        .auth_tokens = auth_tokens,
        .auth_token_count = auth_token_count,
        .has_new_group_request = has_new_group_request,
        .new_group_request = new_group_request,
    };

    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));

    moq_result_t rc = s->profile->encode_request_update(s, &w, &args);
    if (rc < 0) {
        s->profile->abort_request(s, &req_ep);
        return rc;
    }

    /* The update travels on the publication's existing request bidi (no FIN);
     * the REQUEST_OK correlates by that stream, so no by-id registration. A
     * send-buffer shortfall is retryable (WOULD_BLOCK), not BUFFER. */
    moq_result_t arc = queue_publish_response(s, (size_t)slot, buf,
                                              moq_buf_writer_offset(&w), false);
    if (arc < 0) {
        s->profile->abort_request(s, &req_ep);
        return arc;
    }
    if (!moq_session_uses_request_streams(s)) {
        req_ep.kind = MOQ_REQ_PUBLICATION_UPDATE;
        req_ep.slot = slot;
        request_registry_insert_by_id(s, req_ep.request_id, req_ep);
    }

    e->update_pending = true;
    e->update_request_id = req_ep.request_id;
    s->profile->commit_request(s, &req_ep);
    return MOQ_OK;
}

/* -- Finish publish (publisher side) ------------------------------- */

void moq_finish_publish_cfg_init(moq_finish_publish_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_finish_publish_cfg_t);
}

moq_result_t moq_session_finish_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_finish_publish_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_finish_publish_cfg_t))
        return MOQ_ERR_INVAL;
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->publishes[slot].role != MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    /* Migrated by an outbound GOAWAY: no PUBLISH_DONE on the request bidi. Check
     * before capacity preflight so the contract is WRONG_STATE, not WOULD_BLOCK. */
    if (s->publishes[slot].goaway_sent) return MOQ_ERR_WRONG_STATE;

    for (size_t i = 0; i < s->sg_cap; i++) {
        moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->state == MOQ_SG_FREE || sg->state == MOQ_SG_CLOSING ||
            sg->state == MOQ_SG_RESETTING)
            continue;
        if (moq_publication_eq(sg->pub, pub))
            return MOQ_ERR_WRONG_STATE;
    }

    /* Stream-correlated profiles finish the request bidi with PUBLISH_DONE + FIN
     * and free the entry; reserve a drain slot up front for the subscriber's late
     * reciprocal FIN (unless it already arrived). */
    moq_pub_entry_t *pe = &s->publishes[slot];
    bool req_stream = (pe->request_stream_ref._v != 0);
    bool need_drain = req_stream && !pe->req_recv_fin;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    moq_finish_publish_encode_args_t args = {
        .request_id = pe->request_id,
        .status_code = cfg->status_code,
        .stream_count = cfg->stream_count,
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len,
    };

    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_publish_done(s, &w, &args);
    if (rc < 0) return rc;

    /* Terminal: finish the request bidi (FIN) for stream-correlated profiles. A
     * send-buffer shortfall is retryable (WOULD_BLOCK), not BUFFER. */
    moq_result_t arc = queue_publish_response(s, (size_t)slot, buf,
                                              moq_buf_writer_offset(&w),
                                              req_stream);
    if (arc < 0) return arc;

    if (need_drain)
        (void)drain_ref_add(s, pe->request_stream_ref);   /* slot reserved above */
    pub_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Publication alias lookup (receive side) ----------------------- */

int pub_find_by_alias_subscriber(moq_session_t *s, uint64_t alias)
{
    for (size_t i = 0; i < s->pub_cap; i++) {
        moq_pub_entry_t *e = &s->publishes[i];
        if (e->role != MOQ_PUB_ROLE_SUBSCRIBER || e->track_alias != alias)
            continue;
        /* Established always receives objects. A publication still pending our
         * PUBLISH_OK accepts objects too, but only when its initial Forward State
         * is 1 (§9.4: the publisher may send before PUBLISH_OK). A pending,
         * forward-0 publication delivers nothing yet -- early data falls through to
         * the unknown-alias path (STOP_SENDING). */
        if (e->state == MOQ_PUB_ESTABLISHED ||
            (e->state == MOQ_PUB_PENDING_SUBSCRIBER && e->send_allowed))
            return (int)i;
    }
    return -1;
}

/* -- Open subgroup for publisher-initiated subscription ------------ */

moq_result_t moq_session_open_pub_subgroup(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_subgroup_cfg_t *cfg,
    uint64_t now_us,
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

    int pub_slot = pub_resolve_handle(s, pub);
    if (pub_slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[pub_slot].state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->publishes[pub_slot].role != MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (!s->publishes[pub_slot].send_allowed)
        return MOQ_ERR_WRONG_STATE;

    for (size_t i = 0; i < s->sg_cap; i++) {
        moq_sg_entry_t *e = &s->subgroups[i];
        if (e->state == MOQ_SG_FREE) continue;
        if (moq_publication_eq(e->pub, pub) &&
            e->group_id == cfg->group_id && e->subgroup_id == cfg->subgroup_id)
            return MOQ_ERR_INVAL;
    }

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
        .track_alias = s->publishes[pub_slot].track_alias,
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

    moq_sg_entry_t *entry = &s->subgroups[slot];
    entry->generation |= 1;
    entry->state = MOQ_SG_OPEN;
    entry->sub = MOQ_SUBSCRIPTION_INVALID;
    entry->pub = pub;
    entry->stream_ref = a.u.send_data.stream_ref;
    entry->group_id = cfg->group_id;
    entry->subgroup_id = cfg->subgroup_id;
    entry->has_prev_object = false;
    entry->has_extensions = has_ext;
    entry->delivery_deadline_us = UINT64_MAX;
    s->next_stream_ref++;

    *out_handle = sg_make_handle(s, (size_t)slot);
    return MOQ_OK;
}

moq_result_t moq_session_request_goaway_publish(
    moq_session_t *s, moq_publication_t pub,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_request_goaway_cfg_t)) return MOQ_ERR_INVAL;
    if (cfg->new_session_uri.len > 0 && !cfg->new_session_uri.data)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_PUBLISH, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}
