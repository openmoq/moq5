#include "session_internal.h"
#include "../internal/validate.h"

/* unsub_tomb_add declared in session_internal.h */

/* -- Subscription pool --------------------------------------------- */

static int sub_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->sub_cap; i++)
        if (s->subs[i].state == MOQ_SUB_FREE) return (int)i;
    return -1;
}

int sub_find_by_request_id(moq_session_t *s, uint64_t request_id)
{
    moq_request_endpoint_t ep = request_registry_find_by_id(s, request_id);
    if (ep.kind != MOQ_REQ_SUBSCRIPTION) return -1;
    return ep.slot;
}

static bool sub_is_duplicate_track(moq_session_t *s,
                                    const uint8_t *track_id, size_t track_id_len,
                                    moq_sub_role_t role)
{
    for (size_t i = 0; i < s->sub_cap; i++) {
        moq_sub_entry_t *e = &s->subs[i];
        if (e->state == MOQ_SUB_FREE || e->state == MOQ_SUB_TERMINATED)
            continue;
        if (e->role != role) continue;
        if (e->track_id_len == track_id_len &&
            memcmp(e->track_id_buf, track_id, track_id_len) == 0)
            return true;
    }
    return false;
}

/* Maintain idx_sub_by_alias (alias -> slot for ESTABLISHED subscriber-role
 * subscriptions). Insert once a subscription is established with its alias;
 * clear before it leaves ESTABLISHED. The clear is guarded on the current
 * (role, state) so it is a precise no-op for entries that were never indexed
 * (pending, publisher-role, or already terminated). */
static void sub_alias_index_insert(moq_session_t *s, size_t slot)
{
    moq_index_insert(s->idx_sub_by_alias, s->idx_sub_alias_mask,
                     s->subs[slot].track_alias, (int)slot);
}

static void sub_alias_index_clear(moq_session_t *s, size_t slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    if (e->state == MOQ_SUB_ESTABLISHED && e->role == MOQ_SUB_ROLE_SUBSCRIBER)
        moq_index_remove(s->idx_sub_by_alias, s->idx_sub_alias_mask,
                         e->track_alias);
}

int sub_find_by_alias_subscriber(moq_session_t *s, uint64_t alias)
{
    int slot = moq_index_find(s->idx_sub_by_alias, s->idx_sub_alias_mask, alias);
    /* Re-check the predicate: a stale index entry fails closed to -1 rather than
     * returning the wrong subscription (defence in depth; preserves the exact
     * invariant of the former linear scan). */
    if (slot >= 0) {
        moq_sub_entry_t *e = &s->subs[slot];
        if (e->state == MOQ_SUB_ESTABLISHED &&
            e->role == MOQ_SUB_ROLE_SUBSCRIBER &&
            e->track_alias == alias)
            return slot;
    }
    return -1;
}

bool session_has_forwarding_pending_subscriber(moq_session_t *s)
{
    for (size_t i = 0; i < s->sub_cap; i++) {
        moq_sub_entry_t *e = &s->subs[i];
        if (e->state == MOQ_SUB_PENDING_SUBSCRIBER &&
            e->role == MOQ_SUB_ROLE_SUBSCRIBER &&
            e->forward)
            return true;
    }
    return false;
}

bool sub_track_alias_in_use(moq_session_t *s, uint64_t alias)
{
    for (size_t i = 0; i < s->sub_cap; i++) {
        moq_sub_entry_t *e = &s->subs[i];
        if (e->state == MOQ_SUB_ESTABLISHED && e->track_alias == alias)
            return true;
    }
    return false;
}

static moq_subscription_t sub_make_handle(moq_session_t *s, size_t slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                       s->session_tag,
                                       e->generation, (uint32_t)slot);
    moq_subscription_t h = { packed };
    return h;
}

int sub_resolve_handle(moq_session_t *s, moq_subscription_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_SUBSCRIPTION) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->sub_cap) return -1;
    if (s->subs[slot].generation != gen) return -1;
    if (s->subs[slot].state == MOQ_SUB_FREE) return -1;
    return (int)slot;
}

static void sub_free_entry(moq_session_t *s, size_t slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    /* Safety net for any Joining FETCHes (§10.12.2) still buffered against a
     * pending subscription: free them (dropping their entry-owned token storage)
     * with no control message. The alive teardown paths -- public reject and
     * inbound bidi teardown -- reserve capacity and reject pending joins explicitly
     * before reaching here, so this only fires on a closing session, where the
     * stream-byte handler short-circuits on CLOSED and no late FIN is delivered. */
    if (e->state == MOQ_SUB_PENDING_PUBLISHER)
        session_core_discard_pending_joins(s, e->request_id);
    request_registry_remove_by_id(s, e->request_id);
    /* Stream-correlated profiles register by request stream_ref; remove that
     * key and reset the field so a recycled slot never carries a stale ref. */
    if (e->request_stream_ref._v != 0) {
        request_registry_remove_by_streamref(s, e->request_stream_ref);
        e->request_stream_ref = moq_stream_ref_from_u64(0);
    }
    /* Reset the request-stream receive buffer so a recycled slot never reuses
     * stale buffered bytes (stream-correlated profiles). */
    e->req_recv_len = 0;
    e->req_recv_fin = false;
    if (e->update_pending) {
        request_registry_remove_by_id(s, e->update_request_id);
        e->update_pending = false;
        e->update_request_id = 0;
    }
    e->update_failed = false;
    if (e->track_id_buf) {
        s->alloc.free(e->track_id_buf, e->track_id_len, s->alloc.ctx);
        e->track_id_buf = NULL;
        e->track_id_len = 0;
    }
    e->goaway_sent = false;   /* selective free: clear the migration marker so a
                               * reused slot is never seen as already migrated */
    /* Clear the cached largest-object location so a reused slot can never carry
     * a prior subscription's largest state into a joining FETCH (only
     * SUBSCRIBE_OK / accept stamps current largest, and only at ESTABLISHED). */
    e->has_largest = false;
    e->largest_group = 0;
    e->largest_object = 0;
    sub_alias_index_clear(s, slot);   /* remove alias while still ESTABLISHED */
    e->state = MOQ_SUB_FREE;
    e->generation++;

    /* Freeing a pending subscription may leave no forwarding subscriber
     * pending, in which case any data held for an as-yet-unestablished alias
     * can never be matched -- discard it (no-op while one remains). */
    session_discard_staged_if_no_pending(s);
}

static uint8_t *build_track_id(moq_session_t *s,
                                const moq_namespace_t *ns,
                                moq_bytes_t name,
                                size_t *out_len)
{
    size_t total = 1;
    for (size_t i = 0; i < ns->count; i++)
        total += 2 + ns->parts[i].len;
    total += 2 + name.len;
    *out_len = total;

    uint8_t *buf = (uint8_t *)s->alloc.alloc(total, s->alloc.ctx);
    if (!buf) return NULL;

    size_t off = 0;
    buf[off++] = (uint8_t)ns->count;
    for (size_t i = 0; i < ns->count; i++) {
        uint16_t flen = (uint16_t)ns->parts[i].len;
        buf[off++] = (uint8_t)(flen >> 8);
        buf[off++] = (uint8_t)(flen & 0xFF);
        if (flen > 0)
            memcpy(buf + off, ns->parts[i].data, flen);
        off += flen;
    }
    uint16_t nlen = (uint16_t)name.len;
    buf[off++] = (uint8_t)(nlen >> 8);
    buf[off++] = (uint8_t)(nlen & 0xFF);
    if (name.len > 0 && name.data)
        memcpy(buf + off, name.data, name.len);

    return buf;
}

/* -- Message parameter validation ---------------------------------- */

/* Copy namespace parts into output scratch. */
static bool event_scratch_copy_namespace(moq_session_t *s,
                                    const moq_namespace_t *src,
                                    moq_namespace_t *dst)
{
    moq_bytes_t *parts = (moq_bytes_t *)event_scratch_alloc_aligned(s, src->count * sizeof(moq_bytes_t), _Alignof(moq_bytes_t));
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

/* -- Subscribe handlers -------------------------------------------- */

moq_result_t session_core_on_subscribe(moq_session_t *s,
                                        moq_decoded_subscribe_t *d,
                                        int reserved_slot)
{
    /*
     * All exits route through cleanup_all to free staging
     * and commit/abort the auth transaction.
     */
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    uint8_t *tid = NULL;
    size_t tid_len = 0;
    size_t scratch_saved = s->event_scratch_len;

    if (event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    /* A message-level authorization-token reject (e.g. unknown alias) fails the
     * request with REQUEST_ERROR and surfaces no event; a REGISTER carried in the
     * same message still commits its alias (§10.2.2), so commit the auth txn. The
     * error rides the request bidi for stream-correlated profiles (never the
     * control channel) and the shared control channel otherwise. */
    if (d->auth_reject_code) {
        if (d->endpoint.has_stream_ref) {
            /* Encode to a stack buffer first so a temporarily full send buffer
             * surfaces as WOULD_BLOCK (retryable), not a hard error that would
             * drop the request and the required REQUEST_ERROR. */
            uint8_t err_buf[256];
            moq_buf_writer_t ew;
            moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
            rc = s->profile->encode_request_error(s, &ew,
                &(moq_request_error_encode_args_t){
                    .error_code = d->auth_reject_code });
            if (rc < 0) { result = rc; goto cleanup_all; }
            rc = queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                                 moq_buf_writer_offset(&ew), true);
            if (rc < 0) { result = rc; goto cleanup_all; }
            if (reserved_slot >= 0) sub_free_entry(s, (size_t)reserved_slot);
            s->profile->commit_inbound_request(s, &d->endpoint);
            auth_committed = true;
            process_auth_tokens_commit_txn(s, &d->auth_txn);
            result = MOQ_OK;
            goto cleanup_all;
        }
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = d->auth_reject_code });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    tid = build_track_id(s, &d->track_namespace, d->track_name, &tid_len);
    if (tid_len > 0 && !tid) {
        result = MOQ_ERR_NOMEM;
        goto cleanup_all;
    }

    if (sub_is_duplicate_track(s, tid, tid_len, MOQ_SUB_ROLE_PUBLISHER)) {
        if (tid) { s->alloc.free(tid, tid_len, s->alloc.ctx); tid = NULL; }
        if (d->endpoint.has_stream_ref) {
            /* Stream-correlated profiles deliver the error on the request bidi
             * and terminate that stream; the reserved slot is freed here and the
             * peer's request sequence advances (the request was well-formed, it
             * is the track that is a duplicate). */
            /* Encode to a stack buffer first so a temporarily full send buffer
             * surfaces as WOULD_BLOCK (retryable), not a hard error that would
             * drop the request and the required REQUEST_ERROR. */
            uint8_t derr_buf[256];
            moq_buf_writer_t ew;
            moq_buf_writer_init(&ew, derr_buf, sizeof(derr_buf));
            rc = s->profile->encode_request_error(s, &ew,
                &(moq_request_error_encode_args_t){
                    .error_code = 0x19,
                    .reason = (const uint8_t *)"duplicate subscription",
                    .reason_len = 22 });
            if (rc < 0) { result = rc; goto cleanup_all; }
            rc = queue_send_bidi(s, d->endpoint.stream_ref, derr_buf,
                                 moq_buf_writer_offset(&ew), true);
            if (rc < 0) { result = rc; goto cleanup_all; }
            if (reserved_slot >= 0) sub_free_entry(s, (size_t)reserved_slot);
            s->profile->commit_inbound_request(s, &d->endpoint);
            auth_committed = true;
            process_auth_tokens_commit_txn(s, &d->auth_txn);
            result = MOQ_OK;
            goto cleanup_all;
        }
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = 0x19,
                .reason = (const uint8_t *)"duplicate subscription",
                .reason_len = 22 });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    int slot = reserved_slot >= 0 ? reserved_slot : sub_find_free(s);
    if (slot < 0) {
        if (tid) { s->alloc.free(tid, tid_len, s->alloc.ctx); tid = NULL; }
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = 0x0,
                .reason = (const uint8_t *)"subscription pool full",
                .reason_len = 22 });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
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

    /* Copy resolved token values into scratch for borrow epoch safety.
     * Staged (USE_ALIAS) values are allocator-owned copies; free each
     * one immediately after copying to scratch. */
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

    /* Copy resolved token array into scratch. */
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

    /* Compute would-be live generation and handle WITHOUT mutating entry. */
    moq_sub_entry_t *entry = &s->subs[slot];
    uint32_t live_gen = entry->generation | 1;
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_SUBSCRIPTION,
                                       s->session_tag, live_gen,
                                       (uint32_t)slot);
    moq_subscription_t handle = { packed };

    moq_subscribe_filter_t filter = d->has_filter ? d->filter_type
                                                  : MOQ_SUBSCRIBE_FILTER_NONE;

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SUBSCRIBE_REQUEST;
    e.detail_size = (uint32_t)sizeof(moq_subscribe_request_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.subscribe_request.sub = handle;
    e.u.subscribe_request.track_namespace = ev_ns;
    e.u.subscribe_request.track_name = ev_name;
    e.u.subscribe_request.filter = filter;
    e.u.subscribe_request.subscriber_priority = d->subscriber_priority;
    e.u.subscribe_request.group_order = d->group_order;
    e.u.subscribe_request.forward = d->forward;
    e.u.subscribe_request.start_group = d->start_group;
    e.u.subscribe_request.start_object = d->start_object;
    e.u.subscribe_request.end_group = d->end_group;
    e.u.subscribe_request.delivery_timeout_us = d->delivery_timeout_us;
    e.u.subscribe_request.tokens = ev_tokens;
    e.u.subscribe_request.token_count = d->token_count;
    e.u.subscribe_request.has_new_group_request = d->has_new_group_request;
    e.u.subscribe_request.new_group_request = d->new_group_request;

    rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        result = rc;
        goto cleanup_all;
    }

    /* Commit: now safe to mutate entry. */
    entry->generation = live_gen;
    entry->state = MOQ_SUB_PENDING_PUBLISHER;
    entry->role = MOQ_SUB_ROLE_PUBLISHER;
    entry->handle = handle;
    entry->request_id = d->request_id;
    entry->track_alias = 0;
    entry->track_id_buf = tid;
    entry->track_id_len = tid_len;
    entry->delivery_timeout_us = d->delivery_timeout_us;
    entry->filter_type = d->has_filter ? d->filter_type : 0;
    entry->forward = d->forward;
    d->endpoint.kind = MOQ_REQ_SUBSCRIPTION;
    d->endpoint.slot = slot;
    if (d->endpoint.has_stream_ref) {
        /* Stream-correlated profiles register the request bidi by stream_ref
         * when it is first seen; just bind it to the entry here (no by-id
         * key -- responses are correlated by stream, not request id). */
        entry->request_stream_ref = d->endpoint.stream_ref;
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
    if (tid) s->alloc.free(tid, tid_len, s->alloc.ctx);
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

/* Buffer and dispatch a fetcher's request-bidi RESPONSE (FETCH_OK /
 * REQUEST_ERROR). The fetch entry owns the response buffer; backpressure keeps
 * it for a re-feed, a terminal REQUEST_ERROR marks the entry draining and it is
 * freed once the trailing FIN arrives. Objects arrive on the data uni, not here. */
static moq_result_t handle_fetch_response_bytes(moq_session_t *s, int fslot,
                                                moq_stream_ref_t stream_ref,
                                                const uint8_t *buf, size_t len,
                                                bool fin)
{
    moq_fetch_entry_t *fe = &s->fetches[fslot];

    /* Draining after a terminal REQUEST_ERROR: absorb the FIN, reject bytes. */
    if (fe->state == MOQ_FETCH_DRAINING_RESPONSE) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes after terminal fetch response");
        if (fin) fetch_free_entry(s, fslot);
        return MOQ_OK;
    }

    /* Past the first response (FETCH_OK surfaced): the response phase is done.
     * A trailing FIN is benign; extra bytes fail closed. */
    if (fe->state != MOQ_FETCH_PENDING_FETCHER || fe->control_response_seen) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "unexpected bytes after fetch response");
        return MOQ_OK;
    }

    if (len > 0) {
        if (len > fe->req_recv_cap - fe->req_recv_len)
            return close_with_error(s, 0x3, "fetch response too large");
        memcpy(fe->req_recv_buf + fe->req_recv_len, buf, len);
        fe->req_recv_len += len;
    }
    if (fin) fe->req_recv_fin = true;

    size_t consumed = 0;
    moq_result_t rc = s->profile->process_response_stream(
        s, stream_ref, fslot, (uint32_t)MOQ_REQ_FETCH,
        fe->req_recv_buf, fe->req_recv_len, fe->req_recv_fin, &consumed);
    if (s->state == MOQ_SESS_CLOSED)
        return MOQ_OK;                /* profile failed closed; torn down */
    if (rc == MOQ_ERR_WOULD_BLOCK)
        return rc;                    /* keep buffer; a re-feed retries */
    if (rc < 0)
        return rc;                    /* committed entry left intact for retry */

    if (consumed == 0) {
        if (fe->req_recv_fin)
            return close_with_error(s, 0x3, "truncated fetch response");
        return MOQ_OK;                /* incomplete; wait for more */
    }
    if (consumed < fe->req_recv_len)
        return close_with_error(s, 0x3, "extra bytes after fetch response");
    fe->req_recv_len = 0;

    /* A terminal REQUEST_ERROR left the entry draining; free now if the FIN
     * already arrived in this chunk. */
    if (fe->state == MOQ_FETCH_DRAINING_RESPONSE && fe->req_recv_fin)
        fetch_free_entry(s, fslot);
    return MOQ_OK;
}

/* Bytes on a TRACK_STATUS request bidi (stream-correlated profiles). The
 * requester buffers the terminal response (TRACK_STATUS_OK / REQUEST_ERROR) and
 * frees on FIN (kept drainable in between); the publisher side, awaiting the
 * app's accept/reject, only records the requester's FIN (TRACK_STATUS is the
 * first and only message) and rejects any further bytes. */
static moq_result_t handle_track_status_stream_bytes(moq_session_t *s, int tslot,
                                                     moq_stream_ref_t stream_ref,
                                                     const uint8_t *buf, size_t len,
                                                     bool fin)
{
    moq_ts_entry_t *te = &s->track_statuses[tslot];

    /* Publisher side: only the requester's FIN may follow its request. */
    if (te->role == MOQ_TS_ROLE_PUBLISHER) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes on track-status request bidi");
        if (fin) te->req_recv_fin = true;
        return MOQ_OK;
    }

    /* Requester side, draining after the terminal response: absorb the FIN. */
    if (te->state == MOQ_TS_DRAINING_RESPONSE) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes after terminal track-status response");
        if (fin) ts_free_entry(s, tslot);
        return MOQ_OK;
    }

    if (len > 0) {
        if (len > te->req_recv_cap - te->req_recv_len)
            return close_with_error(s, 0x3, "track-status response too large");
        memcpy(te->req_recv_buf + te->req_recv_len, buf, len);
        te->req_recv_len += len;
    }
    if (fin) te->req_recv_fin = true;

    size_t consumed = 0;
    moq_result_t rc = s->profile->process_response_stream(
        s, stream_ref, tslot, (uint32_t)MOQ_REQ_TRACK_STATUS,
        te->req_recv_buf, te->req_recv_len, te->req_recv_fin, &consumed);
    if (s->state == MOQ_SESS_CLOSED)
        return MOQ_OK;
    if (rc == MOQ_ERR_WOULD_BLOCK)
        return rc;                    /* keep buffer; a re-feed retries */
    if (rc < 0)
        return rc;

    if (consumed == 0) {
        if (te->req_recv_fin)
            return close_with_error(s, 0x3, "truncated track-status response");
        return MOQ_OK;                /* incomplete; wait for more */
    }
    if (consumed < te->req_recv_len)
        return close_with_error(s, 0x3,
            "extra bytes after track-status response");
    te->req_recv_len = 0;

    /* The terminal response left the entry draining; free now if the FIN already
     * arrived in this chunk. */
    if (te->state == MOQ_TS_DRAINING_RESPONSE && te->req_recv_fin)
        ts_free_entry(s, tslot);
    return MOQ_OK;
}

/* Bytes on a SUBSCRIBE_TRACKS request bidi (draft-18 only). The subscriber side
 * buffers the response stream (REQUEST_OK / REQUEST_ERROR, then PUBLISH_BLOCKED
 * messages while established); the publisher side carries only a deferred
 * REQUEST_UPDATE (rejected + bidi closed). On either side a clean FIN cancels the
 * subscription. A terminal REQUEST_ERROR keeps the entry drainable until the FIN
 * (split-FIN handling), like the fetch / track-status response paths. */
static moq_result_t handle_subscribe_tracks_stream_bytes(moq_session_t *s,
                                                         int slot,
                                                         moq_stream_ref_t stream_ref,
                                                         const uint8_t *buf, size_t len,
                                                         bool fin)
{
    moq_track_sub_entry_t *e = &s->track_subs[slot];

    /* Draining after a terminal REQUEST_ERROR: absorb trailing bytes/FIN. */
    if (e->state == MOQ_TRACK_SUB_DRAINING_RESPONSE) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes after terminal subscribe-tracks response");
        if (fin) track_sub_free_entry(s, slot);
        return MOQ_OK;
    }

    if (len > 0) {
        if (len > e->req_recv_cap - e->req_recv_len)
            return close_with_error(s, 0x3,
                "subscribe-tracks stream message too large");
        memcpy(e->req_recv_buf + e->req_recv_len, buf, len);
        e->req_recv_len += len;
    }
    if (fin) e->req_recv_fin = true;

    bool as_response = (e->role == MOQ_TRACK_SUB_ROLE_SUBSCRIBER);

    for (;;) {
        e = &s->track_subs[slot];
        size_t consumed = 0;
        moq_result_t rc = as_response
            ? s->profile->process_response_stream(s, stream_ref, slot,
                  (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS,
                  e->req_recv_buf, e->req_recv_len, e->req_recv_fin, &consumed)
            : s->profile->process_request_stream(s, stream_ref, slot,
                  e->req_recv_buf, e->req_recv_len, e->req_recv_fin, &consumed);
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;  /* keep buffer; re-feed retries */
        if (rc < 0) return rc;

        if (consumed == 0) {
            if (e->req_recv_len > 0) {
                if (e->req_recv_fin)
                    return close_with_error(s, 0x3,
                        "truncated message on subscribe-tracks stream");
                return MOQ_OK;            /* incomplete; wait for more bytes */
            }
            /* No buffered bytes: a clean FIN cancels the subscription. */
            if (e->req_recv_fin)
                return session_core_on_subscribe_tracks_torn_down(s, slot);
            return MOQ_OK;
        }

        /* The dispatch may have freed the entry (terminal error / rejected
         * update). */
        if (s->track_subs[slot].state == MOQ_TRACK_SUB_FREE) return MOQ_OK;

        e = &s->track_subs[slot];
        size_t remaining = e->req_recv_len - consumed;
        if (remaining > 0)
            memmove(e->req_recv_buf, e->req_recv_buf + consumed, remaining);
        e->req_recv_len = remaining;

        if (remaining == 0) {
            /* A terminal REQUEST_ERROR left the entry draining; free now if the
             * FIN already arrived in this chunk. */
            if (e->state == MOQ_TRACK_SUB_DRAINING_RESPONSE && e->req_recv_fin)
                track_sub_free_entry(s, slot);
            return MOQ_OK;
        }
        /* More buffered messages remain (e.g. several PUBLISH_BLOCKED). */
    }
}

/* The peer tore down a PUBLISH request bidi (a clean FIN with no terminal
 * message, or a RESET/STOP). A draining entry already surfaced its terminal
 * event, so it is reclaimed silently; otherwise the cancellation is surfaced by
 * role -- the publisher learns the subscriber went away (PUBLISH_UNSUBSCRIBED,
 * which also resets its outbound subgroups), the subscriber learns the
 * publication's source vanished (PUBLISH_FINISHED). Event capacity is reserved
 * before any mutation (a full queue yields WOULD_BLOCK, retried later). */
static moq_result_t publish_torn_down(moq_session_t *s, int slot)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    if (pe->state == MOQ_PUB_DRAINING_RESPONSE) {
        pub_free_entry(s, slot);
        return MOQ_OK;
    }
    if (pe->role == MOQ_PUB_ROLE_PUBLISHER)
        return session_core_on_publish_unsubscribed(s, slot);

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_PUBLISH_FINISHED;
    e.detail_size = (uint32_t)sizeof(moq_publish_finished_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.publish_finished.pub = pe->handle;
    (void)push_event(s, &e);
    pub_free_entry(s, slot);
    return MOQ_OK;
}

/* Bytes on a PUBLISH request bidi (draft-18 only). PUBLISH is the inverse of
 * SUBSCRIBE: the publisher (the bidi opener) reads the response stream
 * (PUBLISH_OK / REQUEST_ERROR, then the subscriber's REQUEST_UPDATEs); the
 * subscriber (the responder) reads the request stream (the publisher's
 * REQUEST_OK update acks, then the terminal PUBLISH_DONE). A terminal message
 * leaves the entry draining until the FIN (split-FIN handling); a clean FIN with
 * no message tears the publication down. */
static moq_result_t handle_publish_stream_bytes(moq_session_t *s, int slot,
                                                moq_stream_ref_t stream_ref,
                                                const uint8_t *buf, size_t len,
                                                bool fin)
{
    moq_pub_entry_t *e = &s->publishes[slot];

    /* Draining after a terminal (PUBLISH_DONE / REQUEST_ERROR): absorb FIN. */
    if (e->state == MOQ_PUB_DRAINING_RESPONSE) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes after terminal publish message");
        if (fin) pub_free_entry(s, slot);
        return MOQ_OK;
    }

    /* A deferred PUBLISH_DONE (Stream-Count gating) keeps the entry ESTABLISHED
     * so late data streams still bind; the terminal message was already consumed.
     * The request bidi is now draining: record its FIN but do NOT tear the
     * publication down here (the gated PUBLISH_FINISHED + free happens once the
     * data streams drain). Extra bytes after the terminal are a protocol error. */
    if (e->done_pending) {
        if (len > 0)
            return close_with_error(s, 0x3, "extra bytes after PUBLISH_DONE");
        if (fin) e->req_recv_fin = true;
        return MOQ_OK;
    }

    if (len > 0) {
        if (len > e->req_recv_cap - e->req_recv_len)
            return close_with_error(s, 0x3, "publish stream message too large");
        memcpy(e->req_recv_buf + e->req_recv_len, buf, len);
        e->req_recv_len += len;
    }
    if (fin) e->req_recv_fin = true;

    /* The publisher (opener) reads the response stream; the subscriber
     * (responder) reads the request stream. */
    bool as_response = (e->role == MOQ_PUB_ROLE_PUBLISHER);

    for (;;) {
        e = &s->publishes[slot];
        size_t consumed = 0;
        moq_result_t rc = as_response
            ? s->profile->process_response_stream(s, stream_ref, slot,
                  (uint32_t)MOQ_REQ_PUBLISH,
                  e->req_recv_buf, e->req_recv_len, e->req_recv_fin, &consumed)
            : s->profile->process_request_stream(s, stream_ref, slot,
                  e->req_recv_buf, e->req_recv_len, e->req_recv_fin, &consumed);
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;  /* keep buffer; re-feed retries */
        if (rc < 0) return rc;

        if (consumed == 0) {
            if (e->req_recv_len > 0) {
                if (e->req_recv_fin)
                    return close_with_error(s, 0x3,
                        "truncated message on publish stream");
                return MOQ_OK;            /* incomplete; wait for more bytes */
            }
            /* No buffered bytes: a clean FIN tears the publication down --
             * unless a PUBLISH_DONE was just deferred for Stream-Count gating
             * (e.g. PUBLISH_DONE and the FIN arrived together), in which case the
             * FIN is absorbed and the gated finish waits for the data streams. */
            if (e->req_recv_fin) {
                if (e->done_pending) return MOQ_OK;
                return publish_torn_down(s, slot);
            }
            return MOQ_OK;
        }

        /* The dispatch may have freed the entry (terminal handling). */
        if (s->publishes[slot].state == MOQ_PUB_FREE) return MOQ_OK;

        e = &s->publishes[slot];
        size_t remaining = e->req_recv_len - consumed;
        if (remaining > 0)
            memmove(e->req_recv_buf, e->req_recv_buf + consumed, remaining);
        e->req_recv_len = remaining;

        if (remaining == 0) {
            /* A terminal left the entry draining; free now if the FIN already
             * arrived in this chunk. */
            if (e->state == MOQ_PUB_DRAINING_RESPONSE && e->req_recv_fin)
                pub_free_entry(s, slot);
            return MOQ_OK;
        }
        /* More buffered messages remain (established lifecycle). */
    }
}

/* -- Request bidi bytes (stream-correlated profiles) --------------- *
 * Buffers bytes arriving on a request bidi and dispatches them via the profile.
 * Two phases share this path, keyed by the entry state behind the stream_ref:
 *   - MOQ_SUB_RECVING_REQUEST: an inbound peer request being received into a
 *     core-reserved slot, dispatched via process_request_stream.
 *   - MOQ_SUB_PENDING_SUBSCRIBER: the response to our own outbound request,
 *     dispatched via process_response_stream.
 * Backpressure keeps the buffered bytes for a later re-feed; the reserved slot
 * of an inbound request is freed on any terminal non-commit exit so it is never
 * orphaned. Only RECVING reserved slots are core-owned and freed here; committed
 * subscriptions are freed by the response handlers (e.g. on REQUEST_ERROR). */
moq_result_t handle_request_stream_bytes(moq_session_t *s,
                                          moq_stream_ref_t stream_ref,
                                          const uint8_t *buf, size_t len,
                                          bool fin)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    /* Early arrival (§3.3): QUIC gives no cross-stream ordering, so a request
     * bidi can be delivered before the peer's SETUP (on its own control
     * stream) has been processed. The spec says such streams SHOULD be
     * buffered until setup completes -- so before the session is active the
     * bytes are accepted into the reserved slot below but dispatch is
     * deferred; request_streams_refeed_deferred() re-feeds every deferred
     * slot at establishment. Rejecting here instead tears down the whole
     * connection on a benign packet-arrival race. */
    const bool defer_dispatch = !session_is_active(s);

    /* A request bidi we locally cancelled: discard any late in-flight response
     * (SUBSCRIBE_OK / FETCH_OK / REQUEST_ERROR the peer sent before seeing our
     * STOP_SENDING) rather than mistake it for a new inbound request. Retire the
     * ref once the bidi FINs; a peer reset retires it via bidi_stream_teardown.
     * A GOAWAY-strict ref (a request migrated by a request-stream GOAWAY, §10.4)
     * additionally treats any further non-empty bytes as a PROTOCOL_VIOLATION
     * (e.g. a second GOAWAY) -- only an empty FIN may follow. */
    if (drain_ref_contains(s, stream_ref)) {
        if (len > 0 &&
            drain_ref_reason(s, stream_ref) == MOQ_DRAIN_GOAWAY_STRICT)
            return close_with_error(s, 0x3,
                "bytes on a request stream already migrated by GOAWAY");
        if (fin) drain_ref_remove(s, stream_ref);
        return MOQ_OK;
    }

    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(s, stream_ref);

    /* A request we migrated with an outbound GOAWAY stays live so the app can keep
     * producing on the old session (§10.4); the peer's empty-FIN old-stream close
     * silently retires it. Non-empty bytes fall through to the normal per-family
     * path -- a duplicate GOAWAY there closes 0x3, a legitimate terminal message
     * (e.g. PUBLISH_DONE) is processed as usual. */
    if (len == 0 && fin && request_goaway_free_on_teardown(s, stream_ref))
        return MOQ_OK;

    /* A known FETCH request bidi is keyed into the fetch pool (not the sub pool)
     * and carries the fetcher's response (FETCH_OK / REQUEST_ERROR). Buffer it in
     * the fetch entry's response buffer and dispatch by kind. */
    if (ep.kind == MOQ_REQ_FETCH)
        return handle_fetch_response_bytes(s, ep.slot, stream_ref,
                                           buf, len, fin);

    /* An established PUBLISH_NAMESPACE bidi: post-first-message control bytes
     * (the announcer's response, or an inbound REQUEST_UPDATE) are buffered and
     * dispatched role-keyed in the announcement pool, not treated as a new
     * request. */
    if (ep.kind == MOQ_REQ_ANNOUNCEMENT)
        return handle_announcement_stream_bytes(s, ep.slot, stream_ref,
                                                buf, len, fin);

    /* A known TRACK_STATUS request bidi: the requester's terminal response or the
     * requester's FIN on the publisher side. */
    if (ep.kind == MOQ_REQ_TRACK_STATUS)
        return handle_track_status_stream_bytes(s, ep.slot, stream_ref,
                                                buf, len, fin);

    /* A known SUBSCRIBE_TRACKS bidi: the subscriber's response stream
     * (REQUEST_OK / REQUEST_ERROR / PUBLISH_BLOCKED) or the publisher side's
     * deferred REQUEST_UPDATE / cancel FIN. */
    if (ep.kind == MOQ_REQ_SUBSCRIBE_TRACKS)
        return handle_subscribe_tracks_stream_bytes(s, ep.slot, stream_ref,
                                                    buf, len, fin);

    /* A known PUBLISH request bidi (after the staging handoff): the publisher's
     * response stream (PUBLISH_OK / REQUEST_ERROR / REQUEST_UPDATE) or the
     * subscriber's request stream (REQUEST_OK update ack / PUBLISH_DONE). */
    if (ep.kind == MOQ_REQ_PUBLISH)
        return handle_publish_stream_bytes(s, ep.slot, stream_ref,
                                           buf, len, fin);

    int slot = (ep.kind == MOQ_REQ_SUBSCRIPTION) ? ep.slot : -1;

    if (slot < 0) {
        if (len == 0 && !fin) return MOQ_OK;
        if (len == 0 && fin)
            return close_with_error(s, 0x3,
                "empty FIN on request stream without request");
        /* Reserve a slot to buffer the inbound request. */
        slot = sub_find_free(s);
        if (slot < 0)
            return close_with_error(s, 0x3, "subscription pool full");
        moq_sub_entry_t *re = &s->subs[slot];
        re->generation |= 1;
        re->state = MOQ_SUB_RECVING_REQUEST;
        re->role = MOQ_SUB_ROLE_PUBLISHER;
        re->request_id = 0;   /* stream-correlated; no by-id key while receiving */
        re->request_stream_ref = stream_ref;
        re->req_recv_len = 0;
        re->req_recv_fin = false;
        moq_request_endpoint_t rep;
        memset(&rep, 0, sizeof(rep));
        rep.kind = MOQ_REQ_SUBSCRIPTION;
        rep.slot = slot;
        rep.has_stream_ref = true;
        rep.stream_ref = stream_ref;
        request_registry_insert_by_streamref(s, stream_ref, rep);
    }

    moq_sub_entry_t *e = &s->subs[slot];

    /* Append the new bytes to the per-entry buffer. */
    if (len > 0) {
        if (len > e->req_recv_cap - e->req_recv_len) {
            if (e->state == MOQ_SUB_RECVING_REQUEST)
                sub_free_entry(s, (size_t)slot);
            return close_with_error(s, 0x3, "request stream message too large");
        }
        memcpy(e->req_recv_buf + e->req_recv_len, buf, len);
        e->req_recv_len += len;
    }
    if (fin) e->req_recv_fin = true;

    /* Pre-setup: buffered only (bounded by req_recv_cap); dispatched by the
     * establishment re-feed. */
    if (defer_dispatch) return MOQ_OK;

    /* Dispatch buffered messages one at a time. The dispatch mode follows the
     * entry's state and role: a reserved slot receives its first request; a
     * pending subscriber receives its first response; an established
     * subscription carries the ongoing lifecycle -- REQUEST_UPDATE inbound on
     * the publisher side, REQUEST_OK / terminal responses inbound on the
     * subscriber side. Bytes are consumed only after a message dispatches
     * successfully; WOULD_BLOCK or an incomplete message keeps the remaining
     * buffer for a re-feed. */
    for (;;) {
        e = &s->subs[slot];

        /* A terminal response was surfaced; the entry drains its request bidi.
         * Absorb a trailing FIN (freeing the slot); reject further bytes. */
        if (e->state == MOQ_SUB_TERMINATED) {
            if (e->req_recv_len > 0)
                return close_with_error(s, 0x3,
                    "extra bytes after terminal response on request stream");
            if (e->req_recv_fin) sub_free_entry(s, (size_t)slot);
            return MOQ_OK;
        }

        bool receiving = (e->state == MOQ_SUB_RECVING_REQUEST);
        bool as_request = receiving ||
            (e->state == MOQ_SUB_ESTABLISHED &&
             e->role == MOQ_SUB_ROLE_PUBLISHER);
        bool as_response = (e->state == MOQ_SUB_PENDING_SUBSCRIBER) ||
            (e->state == MOQ_SUB_ESTABLISHED &&
             e->role == MOQ_SUB_ROLE_SUBSCRIBER);

        /* A committed-but-not-yet-established slot (e.g. a publisher awaiting the
         * local accept) has no defined inbound lifecycle message. An empty
         * re-feed is a harmless no-op; real bytes fail closed. */
        if (!as_request && !as_response) {
            if (e->req_recv_len > 0 || e->req_recv_fin)
                return close_with_error(s, 0x3,
                    "unexpected bytes on request stream");
            return MOQ_OK;
        }

        size_t consumed = 0;
        moq_result_t rc = as_request
            ? s->profile->process_request_stream(s, stream_ref, slot,
                  e->req_recv_buf, e->req_recv_len, e->req_recv_fin, &consumed)
            : s->profile->process_response_stream(s, stream_ref, slot,
                  (uint32_t)MOQ_REQ_SUBSCRIPTION,
                  e->req_recv_buf, e->req_recv_len, e->req_recv_fin, &consumed);

        if (s->state == MOQ_SESS_CLOSED) {
            /* Free a still-reserved receiving slot so its stream-ref key and
             * buffer are not orphaned (committed entries close with the
             * session). */
            if (receiving && s->subs[slot].state == MOQ_SUB_RECVING_REQUEST)
                sub_free_entry(s, (size_t)slot);
            return MOQ_OK;
        }

        /* Namespace-sub handoff: the request committed into the ns_sub pool, keyed
         * in idx_ns_by_ref, which now owns the bidi -- including any WOULD_BLOCK
         * retry, which the index-first router sends to the ns_sub path, never back
         * here. The staging slot's buffered bytes are now redundant; free it (the
         * stream-ref left set so sub_free_entry reclaims the stale registry key)
         * so it cannot be orphaned by the WOULD_BLOCK return below. A late-freed
         * ns_sub entry (auth/overlap reject) leaves idx_ns_by_ref empty and is
         * handled by the generic handoff cleanup further down. */
        if (receiving && s->subs[slot].state == MOQ_SUB_RECVING_REQUEST &&
            moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, stream_ref._v) >= 0) {
            /* The handoff reports the request bytes consumed even when processing
             * blocks, so trailing bytes the same chunk carried after the request
             * are a violation on the WOULD_BLOCK path too (not silently dropped
             * with the freed staging slot). */
            bool extra = (consumed < e->req_recv_len);
            sub_free_entry(s, (size_t)slot);
            if (extra)
                return close_with_error(s, 0x3,
                    "extra bytes after request on stream");
            return rc;                /* MOQ_OK (committed) or WOULD_BLOCK (retry) */
        }
        if (rc == MOQ_ERR_WOULD_BLOCK)
            return rc;                /* keep buffer as-is; a re-feed retries */
        if (rc < 0) {
            /* A reserved receiving slot has no retry trigger on hard failure:
             * free it. A committed entry is left intact for a re-feed retry. */
            if (receiving) sub_free_entry(s, (size_t)slot);
            return rc;
        }

        if (consumed == 0) {
            if (e->req_recv_fin) {
                if (receiving) sub_free_entry(s, (size_t)slot);
                return close_with_error(s, 0x3,
                    "truncated message on request stream");
            }
            return MOQ_OK;            /* incomplete; wait for more bytes */
        }

        /* Handoff: the request committed into another pool, leaving this staging
         * sub slot in RECVING. Release it. Two re-key shapes exist:
         *  - Same registry index re-keyed to the new owner (FETCH re-keys the
         *    stream-ref to MOQ_REQ_FETCH): clear our ref so freeing this slot does
         *    not remove the handed-off key.
         *  - Separate index owns the bidi (namespace-sub via idx_ns_by_ref): the
         *    request registry's streamref key is now this stale staging key, so
         *    leave the ref set and let sub_free_entry reclaim it.
         * Lifecycle bytes following a handoff in the same chunk are not handled
         * here. */
        if (receiving && s->subs[slot].state == MOQ_SUB_RECVING_REQUEST) {
            if (consumed < e->req_recv_len)
                return close_with_error(s, 0x3,
                    "extra bytes after request on stream");
            moq_request_endpoint_t cur =
                request_registry_find_by_streamref(s, stream_ref);
            bool staging_key_is_stale =
                (cur.kind == MOQ_REQ_SUBSCRIPTION && cur.slot == slot);
            if (!staging_key_is_stale)
                s->subs[slot].request_stream_ref = moq_stream_ref_from_u64(0);
            sub_free_entry(s, (size_t)slot);
            return MOQ_OK;
        }

        /* The dispatch may have freed the entry (terminal handling). */
        if (s->subs[slot].state == MOQ_SUB_FREE)
            return MOQ_OK;

        /* Consume the message; shift any trailing buffered bytes down. */
        e = &s->subs[slot];
        size_t remaining = e->req_recv_len - consumed;
        if (remaining > 0)
            memmove(e->req_recv_buf, e->req_recv_buf + consumed, remaining);
        e->req_recv_len = remaining;

        if (remaining == 0) {
            /* A terminal response left the entry draining: free now if the FIN
             * already arrived, otherwise keep the slot to absorb a later FIN. */
            if (e->state == MOQ_SUB_TERMINATED && e->req_recv_fin)
                sub_free_entry(s, (size_t)slot);
            return MOQ_OK;
        }
        /* More buffered messages remain: continue (established lifecycle). */
    }
}

/* Dispatch request bidis that buffered bytes before the session established
 * (§3.3 early arrival, see handle_request_stream_bytes): every reserved slot
 * still holding deferred bytes (or a deferred FIN) gets an empty re-feed, which
 * runs the normal dispatch loop now that the session is active. Called by the
 * profile at the moment the session transitions to ESTABLISHED, and again from
 * the event-drain path while request_refeed_pending is set.
 *
 * A WOULD_BLOCK from one slot (e.g. SETUP_COMPLETE filled a tiny event queue
 * at establishment) keeps that slot's buffer intact and latches
 * request_refeed_pending: there is no bridge pending_retry for these bytes
 * (they were ACCEPTED pre-establishment) and the peer may never send more on
 * that stream, so moq_session_poll_events retries the refeed after the app
 * drains events -- mirroring session_replay_staged. A hard error closed the
 * session inside the dispatch; stop and report it. */
moq_result_t request_streams_refeed_deferred(moq_session_t *s)
{
    s->request_refeed_pending = false;
    for (size_t i = 0; i < s->sub_cap; i++) {
        moq_sub_entry_t *e = &s->subs[i];
        if (e->state != MOQ_SUB_RECVING_REQUEST) continue;
        if (e->req_recv_len == 0 && !e->req_recv_fin) continue;
        moq_result_t rc = handle_request_stream_bytes(
            s, e->request_stream_ref, NULL, 0, false);
        if (rc == MOQ_ERR_WOULD_BLOCK) {
            s->request_refeed_pending = true;   /* retried on event drain */
            continue;
        }
        if (rc < 0) return rc;
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
    }
    return MOQ_OK;
}

/* Terminate a stream-correlated request whose request bidi the peer tore down
 * (RESET_STREAM or STOP_SENDING). Frees the entry, removes its registry keys, and
 * surfaces the cancellation event; for a fetch, the response data uni is reset if
 * it is still open. Capacity (event slot, and the fetch data-reset action) is
 * reserved before any mutation: a full queue yields MOQ_ERR_WOULD_BLOCK with the
 * entry untouched so the bridge re-drives the teardown once room frees. Returns
 * MOQ_OK for a ref that is not a stream-correlated request (the caller handles
 * other stream kinds). */
moq_result_t request_stream_teardown(moq_session_t *s,
                                     moq_stream_ref_t stream_ref)
{
    moq_request_endpoint_t ep =
        request_registry_find_by_streamref(s, stream_ref);

    if (ep.kind == MOQ_REQ_SUBSCRIPTION) {
        if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        /* A still-pending responder subscription may hold buffered Joining FETCHes
         * (§10.12.2): the subscription is gone, so each gets
         * INVALID_JOINING_REQUEST_ID on its own bidi. Reserve their capacity
         * alongside the UNSUBSCRIBED event (which uses an event slot, not contended
         * by the reject actions) so this teardown stays retryable. */
        bool has_joins = s->subs[ep.slot].state == MOQ_SUB_PENDING_PUBLISHER;
        if (has_joins) {
            moq_result_t pjrc = session_core_pending_joins_can_resolve(
                s, s->subs[ep.slot].request_id, false /* reject-all */, 0, 0, 0, 0);
            if (pjrc < 0) return pjrc;
        }
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_UNSUBSCRIBED;
        e.detail_size = (uint32_t)sizeof(moq_unsubscribed_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.unsubscribed.sub = s->subs[ep.slot].handle;
        (void)push_event(s, &e);
        if (has_joins) {
            moq_result_t prc = session_core_reject_pending_joins(
                s, s->subs[ep.slot].request_id);
            if (prc < 0) return prc;   /* defensive: preflight guarantees MOQ_OK */
        }
        sub_free_entry(s, (size_t)ep.slot);
        return MOQ_OK;
    }

    if (ep.kind == MOQ_REQ_ANNOUNCEMENT)
        return session_core_on_announce_torn_down(s, ep.slot);

    if (ep.kind == MOQ_REQ_FETCH) {
        moq_fetch_entry_t *fe = &s->fetches[ep.slot];
        bool need_reset = fe->data_stream_started && !fe->data_stream_fin;
        /* Reserve everything before mutating: the data-reset MUST be queued, so
         * a full action queue defers the whole teardown for a later retry. */
        if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        if (need_reset && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        if (need_reset) {
            moq_action_t a;
            memset(&a, 0, sizeof(a));
            a.kind = MOQ_ACTION_RESET_DATA;
            a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
            a.borrow_epoch = s->borrow_epoch;
            a.u.reset_data.stream_ref = fe->data_stream_ref;
            a.u.reset_data.error_code = 0x1;   /* CANCELLED */
            (void)push_action(s, &a);
        }
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_FETCH_CANCELLED;
        e.detail_size = (uint32_t)sizeof(moq_fetch_cancelled_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.fetch_cancelled.fetch = fe->handle;
        (void)push_event(s, &e);
        fetch_free_entry(s, ep.slot);
        return MOQ_OK;
    }

    if (ep.kind == MOQ_REQ_TRACK_STATUS) {
        /* A peer RESET/STOP on a TRACK_STATUS bidi (before or after the terminal
         * response): drop the query state. There is no track-status cancellation
         * event in the public surface, so this frees silently. */
        ts_free_entry(s, ep.slot);
        return MOQ_OK;
    }

    if (ep.kind == MOQ_REQ_SUBSCRIBE_TRACKS)
        return session_core_on_subscribe_tracks_torn_down(s, ep.slot);

    if (ep.kind == MOQ_REQ_PUBLISH)
        return publish_torn_down(s, ep.slot);

    return MOQ_OK;
}

moq_result_t session_core_on_request_goaway(
    moq_session_t *s, moq_request_family_t family, int slot,
    moq_stream_ref_t ref, const uint8_t *uri, size_t uri_len,
    uint64_t timeout_ms)
{
    /* We already migrated this request with our own GOAWAY (entry kept live): a
     * GOAWAY received now is a second GOAWAY on the stream -> PROTOCOL_VIOLATION
     * (§10.4), even though our entry is not yet in the strict drain ring. */
    if (request_goaway_already_sent(s, ref))
        return close_with_error(s, 0x3, "second GOAWAY on a request stream");

    /* Resolve the request handle by family. TRACK_STATUS is the only request that
     * opens its bidi *with* FIN (first-and-only message), so the only side that
     * can receive a GOAWAY there -- the requester -- already closed its send half;
     * skip the close for it. Every other family (FETCH included) keeps its send
     * half open until terminal, so we close it. */
    uint64_t handle_opaque;
    bool close_half = true;
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE:
        handle_opaque = s->subs[slot].handle._opaque; break;
    case MOQ_REQUEST_FAMILY_FETCH:
        handle_opaque = s->fetches[slot].handle._opaque; break;
    case MOQ_REQUEST_FAMILY_TRACK_STATUS:
        handle_opaque = s->track_statuses[slot].handle._opaque;
        close_half = false; break;
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:
        handle_opaque = s->announcements[slot].handle._opaque; break;
    case MOQ_REQUEST_FAMILY_NS_SUB:
        handle_opaque = s->ns_subs[slot].handle._opaque; break;
    case MOQ_REQUEST_FAMILY_PUBLISH:
        handle_opaque = s->publishes[slot].handle._opaque; break;
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS:
        handle_opaque = s->track_subs[slot].handle._opaque; break;
    default:
        return close_with_error(s, 0x3, "GOAWAY for unknown request family");
    }

    /* Fixed-count, reserve-before-mutate: event slot, the conditional close
     * action, and a strict drain slot. No data-stream resets (graceful
     * migration leaves media on the old session). */
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (s->drain_ref_count >= s->drain_ref_cap) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t ev_uri = {0};
    if (uri_len > 0) {
        ev_uri.data = event_scratch_copy(s, uri, uri_len);
        ev_uri.len = uri_len;
        if (!ev_uri.data) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small for GOAWAY URI");
            return MOQ_ERR_BUFFER;
        }
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_REQUEST_GOAWAY;
    e.detail_size = (uint32_t)sizeof(moq_request_goaway_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.request_goaway.family = family;
    e.u.request_goaway.handle.raw = handle_opaque;
    e.u.request_goaway.new_session_uri = ev_uri;
    e.u.request_goaway.timeout_ms = timeout_ms;
    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    /* Close our send half (reserved above) unless it is already FIN'd, then retire
     * the bidi (strict drain + free the entry). */
    if (close_half)
        (void)queue_close_bidi(s, ref);
    request_goaway_retire(s, family, slot, ref);
    return MOQ_OK;
}

/* Free the entry for a request migrated by a per-request GOAWAY -- the per-family
 * free switch, no events. Data streams are intentionally left intact (no
 * RESET/STOP): SUBSCRIBE/PUBLISH late objects correlate by track alias, which is
 * now unknown, so the receive path stops+drops them (non-fatal); FETCH late data
 * correlates by request id, where an unknown id IS session-fatal, so a FETCH whose
 * data uni has not started keeps a migration tombstone to absorb a late
 * FETCH_HEADER instead. Never a session close from late data. */
static void request_goaway_free_entry(moq_session_t *s,
                                      moq_request_family_t family, int slot)
{
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE:        sub_free_entry(s, (size_t)slot); break;
    case MOQ_REQUEST_FAMILY_FETCH:            fetch_on_request_goaway_release(s, slot); break;
    case MOQ_REQUEST_FAMILY_TRACK_STATUS:     ts_free_entry(s, slot); break;
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:     ann_free_entry(s, (size_t)slot); break;
    case MOQ_REQUEST_FAMILY_NS_SUB:           ns_sub_free_entry(s, (size_t)slot); break;
    case MOQ_REQUEST_FAMILY_PUBLISH:          pub_free_entry(s, slot); break;
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS: track_sub_free_entry(s, slot); break;
    default: break;
    }
}

/* Receive-side retire (a GOAWAY we *received*): strict-drain the request bidi so
 * the peer's later FIN/RESET/STOP retires the ref while a duplicate GOAWAY or stray
 * non-empty bytes close 0x3, then free the entry. The caller reserved the drain
 * slot. */
void request_goaway_retire(moq_session_t *s, moq_request_family_t family,
                           int slot, moq_stream_ref_t ref)
{
    (void)drain_ref_add_strict(s, ref);
    request_goaway_free_entry(s, family, slot);
}

/* Resolve a request bidi's family + per-entry goaway_sent marker. Returns false
 * for an unknown ref or a kind with no request-bidi GOAWAY family. */
static bool request_goaway_lookup(moq_session_t *s, moq_stream_ref_t ref,
                                  moq_request_family_t *out_family, int *out_slot,
                                  bool *out_sent)
{
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    moq_request_family_t fam;
    bool sent;
    switch (ep.kind) {
    case MOQ_REQ_SUBSCRIPTION:
        fam = MOQ_REQUEST_FAMILY_SUBSCRIBE; sent = s->subs[ep.slot].goaway_sent; break;
    case MOQ_REQ_FETCH:
        fam = MOQ_REQUEST_FAMILY_FETCH; sent = s->fetches[ep.slot].goaway_sent; break;
    case MOQ_REQ_PUBLISH:
        fam = MOQ_REQUEST_FAMILY_PUBLISH; sent = s->publishes[ep.slot].goaway_sent; break;
    case MOQ_REQ_ANNOUNCEMENT:
        fam = MOQ_REQUEST_FAMILY_ANNOUNCEMENT; sent = s->announcements[ep.slot].goaway_sent; break;
    case MOQ_REQ_NAMESPACE_SUB:
        fam = MOQ_REQUEST_FAMILY_NS_SUB; sent = s->ns_subs[ep.slot].goaway_sent; break;
    case MOQ_REQ_SUBSCRIBE_TRACKS:
        fam = MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS; sent = s->track_subs[ep.slot].goaway_sent; break;
    case MOQ_REQ_TRACK_STATUS:
        fam = MOQ_REQUEST_FAMILY_TRACK_STATUS; sent = s->track_statuses[ep.slot].goaway_sent; break;
    default: {
        /* Namespace-sub bidis are NOT in the stream-ref registry; they are keyed
         * on idx_ns_by_ref. Resolve there before giving up. */
        int32_t ns = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v);
        if (ns < 0) return false;
        *out_family = MOQ_REQUEST_FAMILY_NS_SUB; *out_slot = ns;
        *out_sent = s->ns_subs[ns].goaway_sent;
        return true;
    }
    }
    *out_family = fam; *out_slot = ep.slot; *out_sent = sent;
    return true;
}

/* The peer's spec-mandated old-stream teardown after a per-request GOAWAY we sent
 * (§10.4: "close the old request stream using ... FIN, stream reset, or
 * PUBLISH_DONE"). If this request carries our GOAWAY marker, silently retire it --
 * free the entry, no cancellation/error event -- and report handled. Used for the
 * peer's empty FIN and RESET/STOP; a legitimate terminal message (PUBLISH_DONE) is
 * left to the normal per-family path, and a duplicate GOAWAY closes 0x3 there. */
bool request_goaway_free_on_teardown(moq_session_t *s, moq_stream_ref_t ref)
{
    moq_request_family_t fam; int slot; bool sent;
    if (!request_goaway_lookup(s, ref, &fam, &slot, &sent) || !sent)
        return false;
    request_goaway_free_entry(s, fam, slot);
    return true;
}

/* True iff this request bidi carries a per-request GOAWAY we sent (entry live). A
 * GOAWAY *received* on it is then a second GOAWAY on the stream -> PROTOCOL_VIOLATION. */
bool request_goaway_already_sent(moq_session_t *s, moq_stream_ref_t ref)
{
    moq_request_family_t fam; int slot; bool sent;
    return request_goaway_lookup(s, ref, &fam, &slot, &sent) && sent;
}

/* -- Unsubscribe tombstone ring ------------------------------------ */

bool unsub_tomb_add(moq_session_t *s, uint64_t request_id)
{
    if (s->unsub_tomb_count >= s->unsub_tomb_cap) return false;
    s->unsub_tombstones[s->unsub_tomb_count++] = request_id;
    return true;
}

bool unsub_tomb_consume(moq_session_t *s, uint64_t request_id)
{
    for (size_t i = 0; i < s->unsub_tomb_count; i++) {
        if (s->unsub_tombstones[i] == request_id) {
            s->unsub_tombstones[i] =
                s->unsub_tombstones[--s->unsub_tomb_count];
            return true;
        }
    }
    return false;
}

static bool drain_ref_add_reason(moq_session_t *s, moq_stream_ref_t ref,
                                 moq_drain_reason_t reason)
{
    if (s->drain_ref_count >= s->drain_ref_cap) return false;
    s->drain_refs[s->drain_ref_count] = ref._v;
    s->drain_ref_reasons[s->drain_ref_count] = (uint8_t)reason;
    s->drain_ref_count++;
    return true;
}

bool drain_ref_add(moq_session_t *s, moq_stream_ref_t ref)
{
    return drain_ref_add_reason(s, ref, MOQ_DRAIN_NORMAL);
}

bool drain_ref_add_strict(moq_session_t *s, moq_stream_ref_t ref)
{
    return drain_ref_add_reason(s, ref, MOQ_DRAIN_GOAWAY_STRICT);
}

bool drain_ref_contains(const moq_session_t *s, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->drain_ref_count; i++)
        if (s->drain_refs[i] == ref._v) return true;
    return false;
}

moq_drain_reason_t drain_ref_reason(const moq_session_t *s, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->drain_ref_count; i++)
        if (s->drain_refs[i] == ref._v)
            return (moq_drain_reason_t)s->drain_ref_reasons[i];
    return MOQ_DRAIN_NORMAL;
}

bool drain_ref_remove(moq_session_t *s, moq_stream_ref_t ref)
{
    for (size_t i = 0; i < s->drain_ref_count; i++) {
        if (s->drain_refs[i] == ref._v) {
            /* Swap-remove both parallel arrays in lockstep. */
            size_t last = --s->drain_ref_count;
            s->drain_refs[i] = s->drain_refs[last];
            s->drain_ref_reasons[i] = s->drain_ref_reasons[last];
            return true;
        }
    }
    return false;
}

static moq_result_t subscribe_request_bidi_cancel(moq_session_t *s, int slot);

moq_result_t session_core_on_subscribe_ok(moq_session_t *s,
                                           const moq_decoded_subscribe_ok_t *d,
                                           int resolved_slot)
{
    int slot = resolved_slot >= 0 ? resolved_slot
                                  : sub_find_by_request_id(s, d->request_id);
    if (slot < 0 || s->subs[slot].state != MOQ_SUB_PENDING_SUBSCRIBER)
        return close_with_error(s, 0x3, "SUBSCRIBE_OK for unknown request");

    /* The peer-assigned alias shares the inbound data-alias namespace with our
     * subscriber-role publications, so reject a collision with either (mirrors
     * the inbound-PUBLISH alias check). */
    if (sub_track_alias_in_use(s, d->track_alias) ||
        pub_track_alias_in_use(s, d->track_alias))
        return close_with_error(s, 0x5, "duplicate track alias");

    /* §2.5.1: an unknown Mandatory Track Property in SUBSCRIBE_OK -> the
     * subscriber MUST cancel the subscription. Surface a terminal
     * SUBSCRIBE_ERROR(UNSUPPORTED_EXTENSION) and tear down the request bidi via
     * the internal cancel sequence (never the public unsubscribe entrypoint).
     * Reserve event + cancel actions before mutating. */
    if (d->track_properties_unsupported) {
        if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        size_t scratch_saved0 = s->event_scratch_len;
        static const char k_reason[] = "unsupported mandatory track property";
        moq_bytes_t reason = {0};
        reason.data = event_scratch_copy(s, (const uint8_t *)k_reason,
                                   sizeof(k_reason) - 1);
        reason.len = sizeof(k_reason) - 1;
        if (!reason.data) {
            s->event_scratch_len = scratch_saved0;
            if (scratch_saved0 == 0)
                return close_with_error(s, 0x1, "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
        moq_subscription_t handle = s->subs[slot].handle;
        moq_result_t crc = subscribe_request_bidi_cancel(s, slot);
        if (crc < 0) { s->event_scratch_len = scratch_saved0; return crc; }
        moq_event_t ee;
        memset(&ee, 0, sizeof(ee));
        ee.kind = MOQ_EVENT_SUBSCRIBE_ERROR;
        ee.detail_size = (uint32_t)sizeof(moq_subscribe_error_event_t);
        ee.borrow_epoch = s->borrow_epoch;
        ee.u.subscribe_error.sub = handle;
        ee.u.subscribe_error.error_code = MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION;
        ee.u.subscribe_error.reason = reason;
        return push_event(s, &ee);   /* slot reserved above */
    }

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    if (d->has_deferred_param_error)
        return close_with_error(s, 0x3, d->deferred_param_reason);

    /* Copy track properties into scratch. */
    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t props = {0};
    if (d->track_properties_len > 0) {
        props.data = event_scratch_copy(s, d->track_properties, d->track_properties_len);
        props.len = d->track_properties_len;
        if (!props.data) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0)
                return close_with_error(s, 0x1, "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SUBSCRIBE_OK;
    e.detail_size = (uint32_t)sizeof(moq_subscribe_ok_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.subscribe_ok.sub = s->subs[slot].handle;
    e.u.subscribe_ok.track_alias = d->track_alias;
    e.u.subscribe_ok.has_largest = d->has_largest;
    e.u.subscribe_ok.largest_group = d->largest_group;
    e.u.subscribe_ok.largest_object = d->largest_object;
    e.u.subscribe_ok.has_expires = d->has_expires;
    e.u.subscribe_ok.expires_ms = d->expires_ms;
    e.u.subscribe_ok.track_properties = props;
    e.u.subscribe_ok.dynamic_groups = d->dynamic_groups;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    /* Commit. */
    s->subs[slot].state = MOQ_SUB_ESTABLISHED;
    s->subs[slot].track_alias = d->track_alias;
    s->subs[slot].has_largest = d->has_largest;
    s->subs[slot].largest_group = d->has_largest ? d->largest_group : 0;
    s->subs[slot].largest_object = d->has_largest ? d->largest_object : 0;
    /* Gates outbound new-group requests on this subscription's updates. */
    s->subs[slot].dynamic_groups = d->dynamic_groups;
    /* Index this established subscriber-role alias BEFORE replaying deferred
     * data below, which looks it up via sub_find_by_alias_subscriber. */
    sub_alias_index_insert(s, (size_t)slot);

    /* The alias is now established. Release any data that arrived for it
     * before this OK (the OK event is already queued, so released objects
     * order after it). Backpressure-safe: anything that cannot be delivered
     * now stays held and is retried after event-queue capacity frees up. */
    session_release_staged_for_alias(s, d->track_alias);
    /* If this was the last forwarding subscription pending, data held for any
     * other (now unreachable) alias can never match -- discard it rather than
     * letting it sit until session destroy. No-op while one remains pending. */
    session_discard_staged_if_no_pending(s);
    return MOQ_OK;
}

moq_result_t session_core_on_subscribe_error(moq_session_t *s,
                                              const moq_decoded_subscribe_error_t *d,
                                              bool free_now,
                                              const moq_decoded_redirect_t *redirect)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    /* Stream-correlated profiles close the requester half after the terminal
     * error so the peer can retire the request bidi; reserve that action up
     * front (retryable on re-feed). */
    bool close_half = !free_now &&
        s->subs[d->target_slot].request_stream_ref._v != 0;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_result_t rc;
    if (redirect) {
        /* REDIRECT: surface the dedicated migration event instead of the plain
         * error; the free/drain tail below is identical. */
        rc = session_core_emit_request_redirect(s, MOQ_REQUEST_FAMILY_SUBSCRIBE,
            s->subs[d->target_slot].handle._opaque, redirect, d->error_code,
            d->can_retry, d->retry_after_ms, d->reason, d->reason_len);
        if (rc < 0) return rc;
    } else {
        moq_bytes_t reason = {0};
        if (d->reason_len > 0) {
            reason.data = event_scratch_copy(s, d->reason, d->reason_len);
            reason.len = d->reason_len;
            if (!reason.data) {
                s->event_scratch_len = scratch_saved;
                if (scratch_saved == 0)
                    return close_with_error(s, 0x1,
                        "event scratch permanently too small");
                return MOQ_ERR_BUFFER;
            }
        }

        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_SUBSCRIBE_ERROR;
        e.detail_size = (uint32_t)sizeof(moq_subscribe_error_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.subscribe_error.sub = s->subs[d->target_slot].handle;
        e.u.subscribe_error.error_code = (moq_request_error_t)d->error_code;
        e.u.subscribe_error.can_retry = d->can_retry;
        e.u.subscribe_error.retry_after_ms = d->retry_after_ms;
        e.u.subscribe_error.reason = reason;

        rc = push_event(s, &e);
        if (rc < 0) {
            s->event_scratch_len = scratch_saved;
            return rc;
        }
    }

    if (free_now) {
        sub_free_entry(s, (size_t)d->target_slot);
    } else {
        /* Stream-correlated profiles keep the slot so the request bidi can drain
         * its trailing FIN; the request-stream handler frees it on FIN. Close our
         * send half (reserved above) so the peer can retire the bidi. */
        if (close_half)
            (void)queue_close_bidi(s, s->subs[d->target_slot].request_stream_ref);
        sub_alias_index_clear(s, (size_t)d->target_slot);
        s->subs[d->target_slot].state = MOQ_SUB_TERMINATED;
    }
    return MOQ_OK;
}

/* -- UNSUBSCRIBE handler (semantic) -------------------------------- */

/* Reset every open/streaming subgroup belonging to subscription `slot`'s handle:
 * queue RESET_DATA (0x1 CANCELLED), mark MOQ_SG_RESETTING, clear the streaming
 * counters, and clear/recompute the delivery deadline. Shared by the UNSUBSCRIBE
 * and the failed-REQUEST_UPDATE termination paths so both tear down active data
 * streams identically -- otherwise a terminated subscription leaves its data
 * streams un-reset on the wire and its subgroup slots pinned. Idempotent across
 * WOULD_BLOCK retries: an already-RESETTING subgroup is skipped, so a mid-loop
 * action-queue shortfall resumes on the next call. Returns MOQ_OK once all are
 * reset, or
 * MOQ_ERR_WOULD_BLOCK / a push error if the action queue cannot take a reset; the
 * subscription itself is not freed here. */
static moq_result_t sub_reset_subgroups(moq_session_t *s, size_t slot)
{
    bool need_recompute = false;
    moq_result_t rc = MOQ_OK;
    for (size_t i = 0; i < s->sg_cap; i++) {
        if (s->subgroups[i].state != MOQ_SG_OPEN &&
            s->subgroups[i].state != MOQ_SG_STREAMING)
            continue;
        if (!moq_subscription_eq(s->subs[slot].handle, s->subgroups[i].sub))
            continue;
        if (action_queue_full(s)) { rc = MOQ_ERR_WOULD_BLOCK; break; }
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_RESET_DATA;
        a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.reset_data.stream_ref = s->subgroups[i].stream_ref;
        a.u.reset_data.error_code = 0x1; /* CANCELLED */
        rc = push_action(s, &a);
        if (rc < 0) break;
        s->subgroups[i].state = MOQ_SG_RESETTING;
        s->subgroups[i].streaming_payload_len = 0;
        s->subgroups[i].streaming_bytes_written = 0;
        if (s->subgroups[i].delivery_deadline_us != UINT64_MAX) {
            s->subgroups[i].delivery_deadline_us = UINT64_MAX;
            need_recompute = true;
        }
    }
    if (need_recompute) sg_recompute_deadline(s);
    return rc;
}

moq_result_t session_core_on_unsubscribe(moq_session_t *s,
                                          const moq_decoded_unsubscribe_t *d)
{
    int slot = d->target_slot;
    moq_sub_entry_t *e = &s->subs[slot];

    moq_result_t rc;
    if (e->state == MOQ_SUB_ESTABLISHED) {
        rc = sub_reset_subgroups(s, (size_t)slot);
        if (rc < 0) return rc;
    }

    if (event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_UNSUBSCRIBED;
    ev.detail_size = (uint32_t)sizeof(moq_unsubscribed_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.unsubscribed.sub = e->handle;
    rc = push_event(s, &ev);
    if (rc < 0) return rc;

    sub_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* -- REQUEST_UPDATE handler ---------------------------------------- */

/* Defined later: queue a response already encoded into send_buf, on the request
 * bidi (stream-correlated profiles) or the shared control channel. */
static moq_result_t queue_subscribe_response(moq_session_t *s, size_t slot,
                                             size_t enc_len, bool fin);

/* A REQUEST_OK arrived for our pending subscription update (stream-correlated
 * profiles correlate it by the request bidi). Clear the pending state; an OK for
 * an entry with no pending update is a protocol violation. */
moq_result_t session_core_on_subscribe_update_ok(moq_session_t *s, int slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    if (e->state != MOQ_SUB_ESTABLISHED || !e->update_pending)
        return close_with_error(s, 0x3, "REQUEST_OK without a pending update");
    e->update_pending = false;
    e->update_request_id = 0;
    return MOQ_OK;
}

/* A REQUEST_ERROR arrived for our pending subscription update: the update
 * failed. Clear the pending state; the subscription stays established here and
 * is terminated by the PUBLISH_DONE(UPDATE_FAILED) the publisher must follow
 * with (no separate app event -- the SUBSCRIBE_DONE conveys the outcome). An
 * error for an entry with no pending update is a protocol violation. */
moq_result_t session_core_on_subscribe_update_error(moq_session_t *s, int slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    if (e->state != MOQ_SUB_ESTABLISHED || !e->update_pending)
        return close_with_error(s, 0x3, "REQUEST_ERROR without a pending update");
    e->update_pending = false;
    e->update_request_id = 0;
    /* The subscription must now be terminated by PUBLISH_DONE(UPDATE_FAILED);
     * block further updates and require that terminal message. */
    e->update_failed = true;
    return MOQ_OK;
}

/* Copy resolved token values and the token array into output scratch so they
 * stay valid for the event's borrow epoch, freeing any staged (USE_ALIAS) copies
 * as it goes. On scratch exhaustion restores the high-water mark and returns
 * WOULD_BLOCK (or closes the session if the scratch is permanently too small).
 * *out is the scratch array (NULL when count is 0). Shared with the publish
 * update handler (session_publish.c). */
moq_result_t session_stage_tokens_for_event(moq_session_t *s,
    moq_resolved_token_t *tokens, bool *staged, size_t count,
    size_t scratch_saved, moq_resolved_token_t **out)
{
    *out = NULL;
    for (size_t i = 0; i < count; i++) {
        if (tokens[i].token_value.len > 0) {
            const uint8_t *src = tokens[i].token_value.data;
            size_t src_len = tokens[i].token_value.len;
            uint8_t *copy = event_scratch_copy(s, src, src_len);
            if (staged[i])
                s->alloc.free((void *)(uintptr_t)src, src_len, s->alloc.ctx);
            staged[i] = false;
            if (!copy) {
                s->event_scratch_len = scratch_saved;
                if (scratch_saved == 0)
                    return close_with_error(s, 0x1,
                        "event scratch permanently too small");
                return MOQ_ERR_BUFFER;
            }
            tokens[i].token_value.data = copy;
        } else {
            tokens[i].token_value.data = NULL;
        }
    }
    if (count > 0) {
        moq_resolved_token_t *arr = (moq_resolved_token_t *)event_scratch_alloc_aligned(
            s, count * sizeof(moq_resolved_token_t),
            _Alignof(moq_resolved_token_t));
        if (!arr) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
        memcpy(arr, tokens, count * sizeof(moq_resolved_token_t));
        *out = arr;
    }
    return MOQ_OK;
}

moq_result_t session_core_on_request_update(moq_session_t *s,
                                             moq_decoded_request_update_t *d)
{
    if (d->target_kind == MOQ_REQ_PUBLISH)
        return session_core_on_publish_request_update(s, d);

    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    size_t scratch_saved = s->event_scratch_len;

    /* §10.2.13: the peer MUST NOT send NEW_GROUP_REQUEST on an update unless
     * this track advertised DYNAMIC_GROUPS == 1 -- receiving one anyway is a
     * protocol violation, never delivered to the application. */
    if (d->has_new_group_request &&
        d->target_kind == MOQ_REQ_SUBSCRIPTION &&
        !s->subs[d->target_slot].dynamic_groups) {
        process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
                                         d->token_count);
        process_auth_tokens_abort_txn(s, &d->auth_txn);
        return close_with_error(s, 0x3,
            "NEW_GROUP_REQUEST without dynamic-group support");
    }

    /* A failed update -- an unsupported target/param or a message-level
     * authorization-token reject -- requires REQUEST_ERROR plus
     * PUBLISH_DONE(UPDATE_FAILED) and subscription termination, and surfaces no
     * SUBSCRIBE_UPDATED event. A REGISTER carried alongside still commits its
     * alias (§10.2.2), so the auth txn is committed on this path. */
    if (d->has_unsupported || d->auth_reject_code) {
        uint64_t err_code = d->auth_reject_code
            ? d->auth_reject_code : MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        if (d->target_kind == MOQ_REQ_SUBSCRIPTION) {
            /* Terminating the subscription: tear down its active subgroups
             * (same as UNSUBSCRIBE) before announcing PUBLISH_DONE and freeing
             * it, so its data streams are reset on the wire and its subgroup
             * slots released rather than left pinned after termination.
             * WOULD_BLOCK here is retryable and resumes via the idempotent reset
             * loop; nothing is committed/freed yet. */
            rc = sub_reset_subgroups(s, (size_t)d->target_slot);
            if (rc < 0) { result = rc; goto cleanup_all; }

            if (moq_session_uses_request_streams(s)) {
                /* The REQUEST_ERROR and the terminal PUBLISH_DONE ride the
                 * subscription's request bidi. Encode both to stack buffers, then
                 * reserve the action slots and the combined send space before
                 * queuing either, so a temporary shortfall is a retryable
                 * WOULD_BLOCK rather than partial output. */
                uint8_t err_buf[128];
                moq_buf_writer_t ew;
                moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
                rc = s->profile->encode_request_error(s, &ew,
                    &(moq_request_error_encode_args_t){
                        .request_id = d->request_id, .error_code = err_code });
                if (rc < 0) { result = rc; goto cleanup_all; }
                size_t err_len = moq_buf_writer_offset(&ew);

                uint8_t done_buf[128];
                moq_buf_writer_t dw;
                moq_buf_writer_init(&dw, done_buf, sizeof(done_buf));
                moq_finish_publish_encode_args_t done_args = {
                    .request_id = s->subs[d->target_slot].request_id,
                    .status_code = 0x8,
                };
                rc = s->profile->encode_publish_done(s, &dw, &done_args);
                if (rc < 0) { result = rc; goto cleanup_all; }
                size_t done_len = moq_buf_writer_offset(&dw);

                if (action_queue_avail(s) < 2) {
                    result = MOQ_ERR_WOULD_BLOCK;
                    goto cleanup_all;
                }
                if (err_len + done_len > s->send_cap) {
                    result = MOQ_ERR_BUFFER;        /* can never fit */
                    goto cleanup_all;
                }
                if (err_len + done_len > s->send_cap - s->send_len) {
                    result = MOQ_ERR_WOULD_BLOCK;    /* not now; retryable */
                    goto cleanup_all;
                }
                moq_stream_ref_t up_ref = s->subs[d->target_slot].request_stream_ref;
                rc = queue_send_bidi(s, up_ref, err_buf, err_len, false);
                if (rc < 0) { result = rc; goto cleanup_all; }
                rc = queue_send_bidi(s, up_ref, done_buf, done_len, true);
                if (rc < 0) { result = rc; goto cleanup_all; }
            } else {
                /* Control-channel profiles (draft-16): encode both before queuing
                 * to avoid partial output. */
                uint8_t err_buf[128];
                moq_buf_writer_t ew;
                moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
                rc = s->profile->encode_request_error(s, &ew,
                    &(moq_request_error_encode_args_t){
                        .request_id = d->request_id, .error_code = err_code });
                if (rc < 0) { result = rc; goto cleanup_all; }
                size_t err_len = moq_buf_writer_offset(&ew);

                uint8_t done_buf[128];
                moq_buf_writer_t dw;
                moq_buf_writer_init(&dw, done_buf, sizeof(done_buf));
                moq_finish_publish_encode_args_t done_args = {
                    .request_id = s->subs[d->target_slot].request_id,
                    .status_code = 0x8,
                };
                rc = s->profile->encode_publish_done(s, &dw, &done_args);
                if (rc < 0) { result = rc; goto cleanup_all; }
                size_t done_len = moq_buf_writer_offset(&dw);

                if (action_queue_avail(s) < 2) {
                    result = MOQ_ERR_WOULD_BLOCK;
                    goto cleanup_all;
                }
                if (err_len + done_len > s->send_cap - s->send_len) {
                    result = MOQ_ERR_WOULD_BLOCK;
                    goto cleanup_all;
                }
                rc = queue_send_control(s, err_buf, err_len);
                if (rc < 0) { result = rc; goto cleanup_all; }
                rc = queue_send_control(s, done_buf, done_len);
                if (rc < 0) { result = rc; goto cleanup_all; }
            }
            s->profile->commit_inbound_request(s, &d->endpoint);
            sub_free_entry(s, (size_t)d->target_slot);
            auth_committed = true;
            process_auth_tokens_commit_txn(s, &d->auth_txn);
            result = MOQ_OK;
            goto cleanup_all;
        }

        /* Non-subscription target: simple NOT_SUPPORTED without termination.
         * Only control-channel profiles reach this branch (stream-correlated
         * REQUEST_UPDATE always targets an established subscription), so the
         * error goes on the control channel. */
        if (action_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = err_code });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    moq_sub_entry_t *e = &s->subs[d->target_slot];

    /* Pre-check capacity: need 1 event + 1 action for REQUEST_OK. */
    if (event_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }
    if (action_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }

    /* Copy resolved auth tokens into scratch for borrow-epoch-safe delivery. */
    moq_resolved_token_t *ev_tokens = NULL;
    rc = session_stage_tokens_for_event(s, d->tokens, d->token_staged,
                                        d->token_count, scratch_saved,
                                        &ev_tokens);
    if (rc < 0) { result = rc; goto cleanup_all; }

    /* Queue REQUEST_OK on the request's transport: the request bidi for
     * stream-correlated profiles, the shared control channel otherwise. Encode to
     * a stack buffer first so a temporary send-buffer shortfall on the bidi path
     * is a retryable WOULD_BLOCK rather than a hard error the bridge treats as
     * fatal. */
    {
        uint8_t ok_buf[64];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));
        rc = s->profile->encode_request_ok(s, &ow, d->request_id);
        if (rc < 0) { result = rc; goto cleanup_all; }
        size_t ok_len = moq_buf_writer_offset(&ow);
        if (moq_session_uses_request_streams(s))
            rc = queue_send_bidi(s, s->subs[d->target_slot].request_stream_ref,
                                 ok_buf, ok_len, false);
        else
            rc = queue_send_control(s, ok_buf, ok_len);
        if (rc < 0) { result = rc; goto cleanup_all; }
    }

    /* Build and push event. */
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_SUBSCRIBE_UPDATED;
    ev.detail_size = (uint32_t)sizeof(moq_subscribe_updated_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.subscribe_updated.sub = e->handle;
    ev.u.subscribe_updated.has_subscriber_priority = d->has_subscriber_priority;
    ev.u.subscribe_updated.subscriber_priority = d->subscriber_priority;
    ev.u.subscribe_updated.has_forward = d->has_forward;
    ev.u.subscribe_updated.forward = d->forward;
    ev.u.subscribe_updated.has_delivery_timeout = d->has_delivery_timeout;
    ev.u.subscribe_updated.delivery_timeout_us = d->delivery_timeout_us;
    ev.u.subscribe_updated.tokens = ev_tokens;
    ev.u.subscribe_updated.token_count = d->token_count;
    ev.u.subscribe_updated.has_new_group_request = d->has_new_group_request;
    ev.u.subscribe_updated.new_group_request = d->new_group_request;

    rc = push_event(s, &ev);
    if (rc < 0) { result = rc; goto cleanup_all; }

    /* Commit. */
    if (d->has_forward)
        e->forward = d->forward;
    if (d->has_delivery_timeout)
        e->delivery_timeout_us = d->delivery_timeout_us;
    s->profile->commit_inbound_request(s, &d->endpoint);
    auth_committed = true;
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    return MOQ_OK;

cleanup_all:
    /* No event was surfaced on this path: roll back any token values staged into
     * output scratch so a retryable WOULD_BLOCK does not leak scratch. (The
     * success path returns above without reaching here.) */
    s->event_scratch_len = scratch_saved;
    process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
                                     d->token_count);
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

/* -- Subscribe public API ------------------------------------------ */

void moq_subscribe_cfg_init(moq_subscribe_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_subscribe_cfg_t);
}

void moq_accept_subscribe_cfg_init(moq_accept_subscribe_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_accept_subscribe_cfg_t);
}

void moq_reject_subscribe_cfg_init(moq_reject_subscribe_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_reject_subscribe_cfg_t);
}

moq_result_t moq_session_subscribe(moq_session_t *s,
                                    const moq_subscribe_cfg_t *cfg,
                                    uint64_t now_us,
                                    moq_subscription_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
#define SUB_CFG_MIN offsetof(moq_subscribe_cfg_t, auth_tokens)
    if (cfg->struct_size < SUB_CFG_MIN) return MOQ_ERR_INVAL;
#define SUB_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_subscribe_cfg_t, f) + sizeof(cfg->f))
    *out_handle = MOQ_SUBSCRIPTION_INVALID;

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (SUB_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }
    /* §10.2.13: SUBSCRIBE may carry a new-group request WITHOUT foreknowledge
     * of dynamic-group support (a non-supporting publisher ignores it). */
    bool has_new_group_request = false;
    uint64_t new_group_request = 0;
    if (SUB_CFG_HAS(new_group_request) && cfg->has_new_group_request) {
        has_new_group_request = true;
        new_group_request = cfg->new_group_request;
    }

    session_begin_advance(s, now_us);

    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    /* Validate cfg BEFORE credit checks / REQUESTS_BLOCKED side effects. */
    if (moq_validate_full_track_name(&cfg->track_namespace,
                                      cfg->track_name) < 0)
        return MOQ_ERR_INVAL;
    if (cfg->group_order != MOQ_GROUP_ORDER_DEFAULT &&
        cfg->group_order != MOQ_GROUP_ORDER_ASCENDING &&
        cfg->group_order != MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;
#undef SUB_CFG_HAS
#undef SUB_CFG_MIN

    /* Check request ID credit via profile. */
    moq_request_endpoint_t req_ep;
    {
        moq_result_t prc = s->profile->prepare_request(s, &req_ep);
        if (prc < 0) return prc;
    }

    /* Find free slot. */
    int slot = sub_find_free(s);
    if (slot < 0) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    /* Build track identity for duplicate detection. */
    size_t tid_len = 0;
    uint8_t *tid = build_track_id(s, &cfg->track_namespace, cfg->track_name,
                                   &tid_len);
    if (tid_len > 0 && !tid) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_NOMEM;
    }

    if (sub_is_duplicate_track(s, tid, tid_len, MOQ_SUB_ROLE_SUBSCRIBER)) {
        if (tid) s->alloc.free(tid, tid_len, s->alloc.ctx);
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_INVAL;
    }

    /* Stream-correlated profiles open a dedicated bidi stream per request;
     * others send the request on the shared control channel. */
    bool req_stream = moq_session_uses_request_streams(s);
    moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0);

    /* Encode directly into send_buf via profile op. */
    if (action_queue_full(s)) {
        if (tid) s->alloc.free(tid, tid_len, s->alloc.ctx);
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }
    {
        uint8_t priority = cfg->has_subscriber_priority ?
                           cfg->subscriber_priority : 128;
        bool forward = cfg->has_forward ? cfg->forward : true;

        moq_subscribe_encode_args_t args = {
            .request_id = req_ep.request_id,
            .track_namespace = cfg->track_namespace,
            .track_name = cfg->track_name,
            .subscriber_priority = priority,
            .group_order = cfg->group_order,
            .has_forward = true,
            .forward = forward,
            .filter = cfg->filter,
            .start_group = cfg->start_group,
            .start_object = cfg->start_object,
            .end_group = cfg->end_group,
            .auth_tokens = auth_tokens,
            .auth_token_count = auth_token_count,
            .has_new_group_request = has_new_group_request,
            .new_group_request = new_group_request,
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);

        moq_result_t rc2 = s->profile->encode_subscribe(s, &w, &args);
        if (rc2 < 0) {
            if (tid) s->alloc.free(tid, tid_len, s->alloc.ctx);
            s->profile->abort_request(s, &req_ep);
            return rc2;
        }

        size_t encoded_len = moq_buf_writer_offset(&w);
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.borrow_epoch = s->borrow_epoch;
        if (req_stream) {
            /* Stream-correlated profiles: the request opens its own bidi
             * stream and the response returns on it. */
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
            if (tid) s->alloc.free(tid, tid_len, s->alloc.ctx);
            s->profile->abort_request(s, &req_ep);
            return arc;
        }
        s->send_len += encoded_len;
    }

    /* Commit. */
    moq_sub_entry_t *entry = &s->subs[slot];
    entry->generation |= 1;
    entry->state = MOQ_SUB_PENDING_SUBSCRIBER;
    entry->role = MOQ_SUB_ROLE_SUBSCRIBER;
    entry->request_id = req_ep.request_id;
    entry->track_alias = 0;
    entry->track_id_buf = tid;
    entry->track_id_len = tid_len;
    /* A new pending subscription has no current largest location yet; it is
     * stamped only when SUBSCRIBE_OK arrives (at ESTABLISHED). Initialize so a
     * reused slot never exposes a prior occupant's largest to a joining FETCH. */
    entry->has_largest = false;
    entry->largest_group = 0;
    entry->largest_object = 0;
    entry->filter_type = cfg->filter;
    /* Commit the effective Forward State (default true) so the data-plane
     * reordering buffer only holds early data for a forwarding subscription. */
    entry->forward = cfg->has_forward ? cfg->forward : true;
    entry->handle = sub_make_handle(s, (size_t)slot);
    req_ep.kind = MOQ_REQ_SUBSCRIPTION;
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
    s->profile->commit_request(s, &req_ep);
    return MOQ_OK;
}

/* Queue a response that has already been encoded into send_buf at send_len.
 * Profiles that carry responses on the shared control channel emit SEND_CONTROL;
 * stream-correlated profiles emit it on the subscription's request bidi, with
 * `fin` finishing that stream for terminal responses (e.g. REQUEST_ERROR).
 * Callers must have reserved action-queue space (action_queue_full) first. */
static moq_result_t queue_subscribe_response(moq_session_t *s, size_t slot,
                                             size_t enc_len, bool fin)
{
    moq_action_t act;
    memset(&act, 0, sizeof(act));
    act.borrow_epoch = s->borrow_epoch;
    if (moq_session_uses_request_streams(s)) {
        /* Post-GOAWAY gate (§10.4): a request migrated by an outbound GOAWAY accepts
         * no further request-bidi control bytes. Bail before committing (send_len is
         * not yet advanced), mirroring the queue_send_bidi guard. */
        if (request_goaway_already_sent(s, s->subs[slot].request_stream_ref))
            return MOQ_ERR_WRONG_STATE;
        act.kind = MOQ_ACTION_SEND_BIDI_STREAM;
        act.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
        act.u.send_bidi_stream.stream_ref = s->subs[slot].request_stream_ref;
        act.u.send_bidi_stream.data = s->send_buf + s->send_len;
        act.u.send_bidi_stream.len = enc_len;
        act.u.send_bidi_stream.fin = fin;
    } else {
        act.kind = MOQ_ACTION_SEND_CONTROL;
        act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
        act.u.send_control.data = s->send_buf + s->send_len;
        act.u.send_control.len = enc_len;
    }
    moq_result_t arc = push_action(s, &act);
    if (arc < 0) return arc;
    s->send_len += enc_len;
    return MOQ_OK;
}

moq_result_t moq_session_accept_subscribe(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_accept_subscribe_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_accept_subscribe_cfg_t))
        return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[slot].state != MOQ_SUB_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    /* Validate all inputs before any state mutation. */
    if (cfg->has_largest) {
        if (cfg->largest_group > MOQ_QUIC_VARINT_MAX ||
            cfg->largest_object > MOQ_QUIC_VARINT_MAX)
            return MOQ_ERR_INVAL;
    }
    if (cfg->has_expires && cfg->expires_ms > MOQ_QUIC_VARINT_MAX)
        return MOQ_ERR_INVAL;
    if (cfg->track_properties.len > 0 && !cfg->track_properties.data)
        return MOQ_ERR_INVAL;

    /* Compute alias locally without mutating profile state. */
    uint64_t alias;
    uint64_t next_alias_after;
    if (cfg->has_track_alias) {
        alias = cfg->track_alias;
        /* The accepted subscription's data shares the outbound data-alias
         * namespace with our publisher-role publications, so reject a collision
         * with either (mirrors the outbound-publish alias check). */
        if (sub_track_alias_in_use(s, alias) ||
            pub_outbound_alias_in_use(s, alias)) return MOQ_ERR_INVAL;
        next_alias_after = s->profile->next_track_alias(s);
    } else {
        alias = s->profile->next_track_alias(s);
        bool found = false;
        /* The scan now skips both subscription and publisher-role publication
         * aliases, so it must allow for both pools' worth of occupied aliases
         * ahead of a free one (matches the outbound-publish scan bound). */
        for (size_t attempts = 0;
             attempts < s->pub_cap + s->sub_cap + 1; attempts++) {
            if (!sub_track_alias_in_use(s, alias) &&
                !pub_outbound_alias_in_use(s, alias)) { found = true; break; }
            alias++;
            if (alias == 0) alias = 1;
        }
        if (!found) return MOQ_ERR_INTERNAL;
        next_alias_after = alias + 1;
        if (next_alias_after == 0) next_alias_after = 1;
    }

    /* Encode SUBSCRIBE_OK into send_buf without committing it (send_len unchanged)
     * so its exact size feeds the combined preflight below. */
    moq_subscribe_ok_encode_args_t args = {
        .request_id = s->subs[slot].request_id,
        .track_alias = alias,
        .has_largest = cfg->has_largest,
        .largest_group = cfg->largest_group,
        .largest_object = cfg->largest_object,
        .has_expires = cfg->has_expires,
        .expires_ms = cfg->expires_ms,
        .track_properties = cfg->track_properties.data,
        .track_properties_len = cfg->track_properties.len,
    };
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len, s->send_cap - s->send_len);
    moq_result_t rc2 = s->profile->encode_subscribe_ok(s, &w, &args);
    if (rc2 < 0) return rc2;
    size_t ok_len = moq_buf_writer_offset(&w);

    /* Reserve-before-mutate: SUBSCRIBE_OK (1 action + ok_len send bytes) PLUS every
     * Joining FETCH (§10.12.2) released/rejected after the accept commits, decided
     * by the accept outcome (cfg largest). Nothing is mutated yet, so a shortfall
     * leaves the accept fully retryable (BUFFER if it can never fit). */
    moq_result_t pjrc = session_core_pending_joins_can_resolve(s, s->subs[slot].request_id,
            cfg->has_largest, cfg->largest_group, 1 /* OK action */,
            0 /* OK takes no drain */, ok_len);
    if (pjrc < 0) return pjrc;

    /* SUBSCRIBE_OK keeps the request bidi open for the subscription. */
    moq_result_t arc = queue_subscribe_response(s, (size_t)slot, ok_len,
                                                false /* fin */);
    if (arc < 0) return arc;

    /* Commit: all outputs reserved. */
    s->subs[slot].state = MOQ_SUB_ESTABLISHED;
    s->subs[slot].track_alias = alias;
    s->subs[slot].has_largest = cfg->has_largest;
    s->subs[slot].largest_group = cfg->has_largest ? cfg->largest_group : 0;
    s->subs[slot].largest_object = cfg->has_largest ? cfg->largest_object : 0;
    /* Latch whether OUR track properties advertised dynamic groups: inbound
     * new-group requests on this subscription's updates are gated on it (the
     * peer MUST NOT send one otherwise, §10.2.13). */
    s->subs[slot].dynamic_groups = s->profile->track_properties_dynamic_groups(
        cfg->track_properties.data, cfg->track_properties.len);
    s->profile->advance_track_alias(s, next_alias_after);
    /* Release/reject the Joining FETCHes buffered against this subscription
     * (capacity reserved above): FETCH_REQUEST per release, INVALID_RANGE per
     * reject. Guaranteed to succeed given the preflight; the check is defensive. */
    moq_result_t prc = session_core_release_pending_joins(s, slot);
    if (prc < 0) return prc;
    return MOQ_OK;
}

moq_result_t moq_session_reject_subscribe(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_reject_subscribe_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_reject_subscribe_cfg_t, redirect))
        return MOQ_ERR_INVAL;   /* pre-redirect minimum; older callers still work */
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;
    if (cfg->can_retry && cfg->retry_after_ms >= MOQ_QUIC_VARINT_MAX)
        return MOQ_ERR_INVAL;
#define SUB_REJ_HAS(f) \
    (cfg->struct_size >= offsetof(moq_reject_subscribe_cfg_t, f) + sizeof(cfg->f))

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[slot].state != MOQ_SUB_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    moq_request_error_encode_args_t err_args = {
        .request_id = s->subs[slot].request_id,
        .error_code = (uint64_t)cfg->error_code,
        .can_retry = cfg->can_retry,
        .retry_after_ms = cfg->retry_after_ms,
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len,
    };
    moq_result_t vrc = reject_apply_redirect(
        s, &err_args, SUB_REJ_HAS(redirect) ? &cfg->redirect : NULL,
        false /* track-scoped */);
    if (vrc < 0) return vrc;
#undef SUB_REJ_HAS

    /* REQUEST_ERROR + FIN closes only our send half; the requester opened the bidi
     * without FIN, so a later empty FIN / in-flight bytes on the ref must be
     * absorbed by the drain ring (not mistaken for — or, on the empty-FIN path,
     * fatally rejected as — a fresh request) once the entry is freed. Reserve the
     * drain slot before mutating (D16 responds on the control channel: no drain). */
    moq_stream_ref_t req_ref = s->subs[slot].request_stream_ref;
    bool need_drain = req_ref._v != 0 && !s->subs[slot].req_recv_fin;

    /* Reserve-before-mutate for the whole terminal batch: this REQUEST_ERROR (1
     * action + maybe 1 drain + its encoded bytes) PLUS rejecting every Joining
     * FETCH buffered against this subscription (§10.12.2) -- the subscription is
     * gone, so each pending join gets INVALID_JOINING_REQUEST_ID. Computing both up
     * front keeps the reject retryable and never best-effort (nothing is queued or
     * freed unless the entire batch fits). Measure the REQUEST_ERROR's real size
     * (it may carry a reason/redirect) into scratch so the preflight is exact and
     * preserves BUFFER (never fits) vs WOULD_BLOCK (transient) for the error. */
    size_t err_scratch_saved = s->output_scratch_len;
    size_t own_bound = 48 + err_args.reason_len;
    if (err_args.has_redirect) {
        own_bound += err_args.connect_uri_len + err_args.redirect_track_name_len + 16;
        for (size_t i = 0; i < err_args.redirect_namespace.count; i++)
            own_bound += err_args.redirect_namespace.parts[i].len + 9;
    }
    uint8_t *probe = (uint8_t *)scratch_alloc_aligned(s, own_bound, 1);
    if (!probe) return MOQ_ERR_BUFFER;   /* message larger than the scratch arena */
    moq_buf_writer_t pw;
    moq_buf_writer_init(&pw, probe, own_bound);
    moq_result_t erc = s->profile->encode_request_error(s, &pw, &err_args);
    size_t own_len = moq_buf_writer_offset(&pw);
    s->output_scratch_len = err_scratch_saved;   /* only the size was needed */
    if (erc < 0) return erc;
    if (own_len > s->send_cap) return MOQ_ERR_BUFFER;   /* never fits (cf. queue_send_bidi) */
    moq_result_t pjrc = session_core_pending_joins_can_resolve(s, s->subs[slot].request_id,
            false /* reject-all outcome */, 0,
            1 /* our REQUEST_ERROR action */, need_drain ? 1 : 0, own_len);
    if (pjrc < 0) return pjrc;

    if (err_args.has_redirect) {
        /* Hardened path: sized scratch encode + queue_send_bidi (BUFFER vs
         * WOULD_BLOCK per send_cap), terminal FIN on the request bidi. */
        moq_result_t rc = queue_request_error_bidi(s, req_ref, &err_args);
        if (rc < 0) return rc;
    } else {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);
        moq_result_t rc2 = s->profile->encode_request_error(s, &w, &err_args);
        if (rc2 < 0) return rc2;
        /* REQUEST_ERROR is terminal: finish the request bidi. */
        moq_result_t arc = queue_subscribe_response(s, (size_t)slot,
                                                    moq_buf_writer_offset(&w),
                                                    true /* fin */);
        if (arc < 0) return arc;
    }

    if (need_drain)
        (void)drain_ref_add(s, req_ref);   /* slot reserved above */
    /* Reject every Joining FETCH buffered against this now-rejected subscription
     * (capacity reserved above, so this cannot block); sub_free_entry then finds
     * none left to discard. */
    uint64_t req_id = s->subs[slot].request_id;
    moq_result_t prc = session_core_reject_pending_joins(s, req_id);
    if (prc < 0) return prc;   /* defensive: preflight guarantees MOQ_OK */
    /* Commit: terminate. */
    sub_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

moq_result_t moq_session_request_goaway_subscribe(
    moq_session_t *s, moq_subscription_t sub,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_request_goaway_cfg_t)) return MOQ_ERR_INVAL;
    if (cfg->new_session_uri.len > 0 && !cfg->new_session_uri.data)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_SUBSCRIBE, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}

/* Internal draft-18 cancel of a subscriber-role subscription: STOP_SENDING +
 * RESET the request bidi, reserve a drain slot so a late SUBSCRIBE_OK /
 * REQUEST_ERROR is discarded rather than mistaken for a new inbound request, then
 * free the entry. Reserve-before-mutate (WOULD_BLOCK leaves state intact). Shared
 * by moq_session_unsubscribe and the UNSUPPORTED_EXTENSION (0x33) path; the caller
 * has already validated role/state and that request streams are in use. Does NOT
 * call session_begin_advance, so it is safe inside an inbound advance. */
static moq_result_t subscribe_request_bidi_cancel(moq_session_t *s, int slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    if (action_queue_avail(s) < 2) return MOQ_ERR_WOULD_BLOCK;
    if (s->drain_ref_count >= s->drain_ref_cap) return MOQ_ERR_WOULD_BLOCK;
    moq_stream_ref_t ref = e->request_stream_ref;
    moq_action_t stop;
    memset(&stop, 0, sizeof(stop));
    stop.kind = MOQ_ACTION_STOP_BIDI_STREAM;
    stop.detail_size = (uint32_t)sizeof(moq_stop_bidi_stream_action_t);
    stop.borrow_epoch = s->borrow_epoch;
    stop.u.stop_bidi_stream.stream_ref = ref;
    stop.u.stop_bidi_stream.error_code = 0x1;   /* CANCELLED */
    moq_result_t src = push_action(s, &stop);
    if (src < 0) return src;
    moq_action_t reset;
    memset(&reset, 0, sizeof(reset));
    reset.kind = MOQ_ACTION_RESET_BIDI_STREAM;
    reset.detail_size = (uint32_t)sizeof(moq_reset_bidi_stream_action_t);
    reset.borrow_epoch = s->borrow_epoch;
    reset.u.reset_bidi_stream.stream_ref = ref;
    reset.u.reset_bidi_stream.error_code = 0x1;
    src = push_action(s, &reset);
    if (src < 0) return src;
    (void)drain_ref_add(s, ref);   /* slot reserved above */
    sub_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

moq_result_t moq_session_unsubscribe(moq_session_t *s,
                                      moq_subscription_t sub,
                                      uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_sub_entry_t *e = &s->subs[slot];
    if (e->role != MOQ_SUB_ROLE_SUBSCRIBER)
        return MOQ_ERR_WRONG_STATE;
    if (e->state != MOQ_SUB_PENDING_SUBSCRIBER &&
        e->state != MOQ_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;

    /* Stream-correlated profiles have no UNSUBSCRIBE message: cancel by tearing
     * down the request bidi (the shared internal sequence). No control message is
     * emitted, and stray objects on a data uni are dropped by track-alias
     * mismatch. */
    if (moq_session_uses_request_streams(s))
        return subscribe_request_bidi_cancel(s, slot);

    /* Reserve tombstone capacity before queueing UNSUBSCRIBE so late
     * responses are safely consumed.  Need one tombstone per pending
     * request that will outlive the freed entry: the original subscribe
     * (if still PENDING) and/or a pending update. */
    size_t tomb_needed = 0;
    if (e->state == MOQ_SUB_PENDING_SUBSCRIBER) tomb_needed++;
    if (e->update_pending) tomb_needed++;
    if (s->unsub_tomb_count + tomb_needed > s->unsub_tomb_cap)
        return MOQ_ERR_WOULD_BLOCK;

    uint8_t buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_unsubscribe(s, &w, e->request_id);
    if (rc < 0) return rc;

    rc = queue_send_control(s, buf, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;

    if (e->state == MOQ_SUB_PENDING_SUBSCRIBER)
        unsub_tomb_add(s, e->request_id);
    if (e->update_pending)
        unsub_tomb_add(s, e->update_request_id);
    sub_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* -- Inbound PUBLISH_DONE for subscriber-role subscription --------- */

moq_result_t session_core_on_subscribe_done(moq_session_t *s,
                                             int slot,
                                             const moq_decoded_publish_done_t *d,
                                             bool free_now)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    /* On the terminal PUBLISH_DONE the subscriber closes its send half so the
     * publisher can retire the request bidi; reserve that action up front. */
    bool close_half = !free_now && s->subs[slot].request_stream_ref._v != 0;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t reason = {0};
    if (d->reason_len > 0) {
        reason.data = event_scratch_copy(s, d->reason, d->reason_len);
        reason.len = d->reason_len;
        if (!reason.data) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0)
                return close_with_error(s, 0x1,
                    "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
    }

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_SUBSCRIBE_DONE;
    ev.detail_size = (uint32_t)sizeof(moq_subscribe_done_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.subscribe_done.sub = s->subs[slot].handle;
    ev.u.subscribe_done.status_code = d->status_code;
    ev.u.subscribe_done.stream_count = d->stream_count;
    ev.u.subscribe_done.reason = reason;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    if (free_now) {
        sub_free_entry(s, (size_t)slot);
    } else {
        /* Stream-correlated profiles keep the slot so the request bidi can drain
         * its trailing FIN; the request-stream handler frees it on FIN. Close our
         * send half (reserved above) so the peer can retire the bidi. */
        if (close_half)
            (void)queue_close_bidi(s, s->subs[slot].request_stream_ref);
        sub_alias_index_clear(s, (size_t)slot);
        s->subs[slot].state = MOQ_SUB_TERMINATED;
    }
    return MOQ_OK;
}

/* -- Done subscribe (publisher terminates accepted SUBSCRIBE) ------ */

void moq_done_subscribe_cfg_init(moq_done_subscribe_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_done_subscribe_cfg_t);
}

moq_result_t moq_session_done_subscribe(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_done_subscribe_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_done_subscribe_cfg_t))
        return MOQ_ERR_INVAL;
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_sub_entry_t *e = &s->subs[slot];
    if (e->role != MOQ_SUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (e->state != MOQ_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    /* Migrated by an outbound GOAWAY: no further request-bidi control. Check before
     * capacity preflight so the contract is WRONG_STATE, not WOULD_BLOCK. */
    if (e->goaway_sent) return MOQ_ERR_WRONG_STATE;
    for (size_t i = 0; i < s->sg_cap; i++) {
        if (s->subgroups[i].state != MOQ_SG_OPEN &&
            s->subgroups[i].state != MOQ_SG_STREAMING)
            continue;
        if (moq_subscription_eq(s->subgroups[i].sub, sub))
            return MOQ_ERR_WRONG_STATE;
    }

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    bool req_stream = moq_session_uses_request_streams(s);
    /* Reserve a drain slot so a late in-flight REQUEST_UPDATE arriving on the
     * request bidi after we FIN + free is discarded, not mistaken for a new
     * inbound request. */
    if (req_stream && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    moq_stream_ref_t req_ref = e->request_stream_ref;

    moq_finish_publish_encode_args_t args = {
        .request_id = e->request_id,
        .status_code = cfg->status_code,
        .stream_count = cfg->stream_count,
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len,
    };

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);
    moq_result_t rc = s->profile->encode_publish_done(s, &w, &args);
    if (rc < 0) return rc;

    size_t elen = moq_buf_writer_offset(&w);
    if (req_stream) {
        /* PUBLISH_DONE is the final message on the subscription's request bidi;
         * FIN it. The publisher may destroy subscription state immediately. */
        rc = queue_subscribe_response(s, (size_t)slot, elen, true /* fin */);
        if (rc < 0) return rc;
        if (req_ref._v != 0)
            (void)drain_ref_add(s, req_ref);   /* slot reserved above */
    } else {
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.kind = MOQ_ACTION_SEND_CONTROL;
        act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
        act.borrow_epoch = s->borrow_epoch;
        act.u.send_control.data = s->send_buf + s->send_len;
        act.u.send_control.len = elen;
        rc = push_action(s, &act);
        if (rc < 0) return rc;
        s->send_len += elen;
    }

    sub_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* -- REQUEST_UPDATE (outbound) ------------------------------------- */

void moq_subscription_update_cfg_init(moq_subscription_update_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_subscription_update_cfg_t);
}

moq_result_t moq_session_update_subscription(
    moq_session_t *s,
    moq_subscription_t sub,
    const moq_subscription_update_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    /* ABI-additive cfg: auth_tokens/auth_token_count are appended fields;
     * callers compiled against the smaller struct remain valid and send
     * no tokens. */
#define UPD_CFG_MIN offsetof(moq_subscription_update_cfg_t, auth_tokens)
#define UPD_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_subscription_update_cfg_t, f) + \
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

    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_sub_entry_t *e = &s->subs[slot];
    if (e->role != MOQ_SUB_ROLE_SUBSCRIBER)
        return MOQ_ERR_WRONG_STATE;
    if (e->state != MOQ_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (e->goaway_sent) return MOQ_ERR_WRONG_STATE;   /* migrated: no REQUEST_UPDATE */
    if (e->update_pending)
        return MOQ_ERR_WRONG_STATE;
    /* A prior update failed; the subscription is awaiting its terminal
     * PUBLISH_DONE(UPDATE_FAILED) and may not be updated again. */
    if (e->update_failed)
        return MOQ_ERR_WRONG_STATE;
    /* §10.2.13: a new-group request may ride a subscription update only when
     * SUBSCRIBE_OK carried DYNAMIC_GROUPS == 1. Refused before any mutation. */
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

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);

    moq_result_t rc = s->profile->encode_request_update(s, &w, &args);
    if (rc < 0) {
        s->profile->abort_request(s, &req_ep);
        return rc;
    }

    size_t encoded_len = moq_buf_writer_offset(&w);
    if (moq_session_uses_request_streams(s)) {
        /* The update travels on the subscription's existing request bidi; the
         * REQUEST_OK correlates by that stream, so no by-id registration. */
        moq_result_t arc = queue_subscribe_response(s, (size_t)slot,
                                                    encoded_len, false);
        if (arc < 0) {
            s->profile->abort_request(s, &req_ep);
            return arc;
        }
    } else {
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.kind = MOQ_ACTION_SEND_CONTROL;
        act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
        act.borrow_epoch = s->borrow_epoch;
        act.u.send_control.data = s->send_buf + s->send_len;
        act.u.send_control.len = encoded_len;
        moq_result_t arc = push_action(s, &act);
        if (arc < 0) {
            s->profile->abort_request(s, &req_ep);
            return arc;
        }
        s->send_len += encoded_len;
        req_ep.kind = MOQ_REQ_SUBSCRIPTION_UPDATE;
        req_ep.slot = slot;
        request_registry_insert_by_id(s, req_ep.request_id, req_ep);
    }

    e->update_pending = true;
    e->update_request_id = req_ep.request_id;
    s->profile->commit_request(s, &req_ep);
    return MOQ_OK;
}
