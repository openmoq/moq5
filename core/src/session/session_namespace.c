#include "session_internal.h"
#include "../internal/validate.h"

/* -- Announcement pool helpers ------------------------------------- */

static int ann_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->ann_cap; i++)
        if (s->announcements[i].state == MOQ_ANN_FREE) return (int)i;
    return -1;
}

int ann_find_by_request_id(moq_session_t *s, uint64_t request_id)
{
    moq_request_endpoint_t ep = request_registry_find_by_id(s, request_id);
    if (ep.kind != MOQ_REQ_ANNOUNCEMENT) return -1;
    return ep.slot;
}

static moq_announcement_t ann_make_handle(moq_session_t *s, size_t slot)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                       s->session_tag,
                                       e->generation, (uint32_t)slot);
    moq_announcement_t h = { packed };
    return h;
}

static int ann_resolve_handle(moq_session_t *s, moq_announcement_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_ANNOUNCEMENT) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->ann_cap) return -1;
    if (s->announcements[slot].generation != gen) return -1;
    if (s->announcements[slot].state == MOQ_ANN_FREE) return -1;
    return (int)slot;
}

void ann_free_entry(moq_session_t *s, size_t slot)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    request_registry_remove_by_id(s, e->request_id);
    /* Stream-correlated profiles register by request stream_ref; remove that key
     * and reset the field so a recycled slot never carries a stale ref. */
    if (e->request_stream_ref._v != 0) {
        request_registry_remove_by_streamref(s, e->request_stream_ref);
        e->request_stream_ref = moq_stream_ref_from_u64(0);
    }
    /* Reset (but preserve) the co-allocated request-stream receive buffer so a
     * recycled slot never reuses stale buffered bytes. */
    e->req_recv_len = 0;
    e->req_recv_fin = false;
    if (e->ns_id_buf) {
        s->alloc.free(e->ns_id_buf, e->ns_id_len, s->alloc.ctx);
        e->ns_id_buf = NULL;
        e->ns_id_len = 0;
    }
    e->goaway_sent = false;   /* selective free: clear the migration marker so a
                               * reused slot is never seen as already migrated */
    e->state = MOQ_ANN_FREE;
    e->generation++;
}

/* Queue an already-encoded announce response on the announce transport: the
 * request bidi for stream-correlated profiles (retryable WOULD_BLOCK contract),
 * the shared control channel otherwise. */
static moq_result_t ann_queue_resp(moq_session_t *s, size_t slot,
                                   const uint8_t *data, size_t len, bool fin)
{
    if (moq_session_uses_request_streams(s))
        return queue_send_bidi(s, s->announcements[slot].request_stream_ref,
                               data, len, fin);
    return queue_send_control(s, data, len);
}

/* Stream-correlated local teardown of an announce request bidi (§3.3.2): cancel
 * both still-open directions with STOP + RESET, retire the ref via the drain ring
 * (so a late in-flight response is discarded rather than mistaken for a fresh
 * request), and free the entry. Reserves all capacity before mutating. */
static moq_result_t ann_local_teardown(moq_session_t *s, size_t slot)
{
    if (action_queue_avail(s) < 2) return MOQ_ERR_WOULD_BLOCK;
    if (s->drain_ref_count >= s->drain_ref_cap) return MOQ_ERR_WOULD_BLOCK;
    moq_stream_ref_t ref = s->announcements[slot].request_stream_ref;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_STOP_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_stop_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.stop_bidi_stream.stream_ref = ref;
    a.u.stop_bidi_stream.error_code = 0x1;   /* CANCELLED (§3.3.3) */
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_reset_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_bidi_stream.stream_ref = ref;
    a.u.reset_bidi_stream.error_code = 0x1;
    rc = push_action(s, &a);
    if (rc < 0) return rc;
    if (ref._v != 0) (void)drain_ref_add(s, ref);   /* slot reserved above */
    ann_free_entry(s, slot);
    return MOQ_OK;
}

/* Dispatch bytes on an established PUBLISH_NAMESPACE request bidi, role-keyed:
 * announcer parses the response (REQUEST_OK/REQUEST_ERROR), receiver parses an
 * inbound REQUEST_UPDATE. Buffers fragmented control messages in the entry; a
 * WOULD_BLOCK keeps the buffer for a re-feed. */
moq_result_t handle_announcement_stream_bytes(moq_session_t *s, int slot,
                                              moq_stream_ref_t stream_ref,
                                              const uint8_t *buf, size_t len,
                                              bool fin)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    if (len > 0) {
        if (len > e->req_recv_cap - e->req_recv_len)
            return close_with_error(s, 0x3,
                "announce request stream message too large");
        memcpy(e->req_recv_buf + e->req_recv_len, buf, len);
        e->req_recv_len += len;
    }
    if (fin) e->req_recv_fin = true;

    for (;;) {
        e = &s->announcements[slot];
        bool as_response = (e->role == MOQ_ANN_ROLE_ANNOUNCER);
        size_t consumed = 0;
        moq_result_t rc = as_response
            ? s->profile->process_response_stream(s, stream_ref, slot,
                  (uint32_t)MOQ_REQ_ANNOUNCEMENT,
                  e->req_recv_buf, e->req_recv_len, e->req_recv_fin, &consumed)
            : s->profile->process_request_stream(s, stream_ref, slot,
                  e->req_recv_buf, e->req_recv_len, e->req_recv_fin, &consumed);
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;  /* keep buffer; re-feed retries */
        if (rc < 0) return rc;
        if (consumed == 0) {
            if (e->req_recv_fin && e->req_recv_len > 0)
                return close_with_error(s, 0x3,
                    "truncated message on announce request stream");
            return MOQ_OK;        /* incomplete or clean FIN; wait for teardown */
        }
        /* The dispatch may have terminated the announcement (REQUEST_ERROR / a
         * rejected update). */
        if (s->announcements[slot].state == MOQ_ANN_FREE) return MOQ_OK;
        e = &s->announcements[slot];
        size_t remaining = e->req_recv_len - consumed;
        if (remaining > 0)
            memmove(e->req_recv_buf, e->req_recv_buf + consumed, remaining);
        e->req_recv_len = remaining;
        if (remaining == 0) return MOQ_OK;
    }
}

/* §10.9.1: a REQUEST_UPDATE on PUBLISH_NAMESPACE is unsupported here; reject and
 * close the bidi. REQUEST_ERROR + FIN on the request bidi, commit the update's
 * request id, terminate the announcement. Retryable on WOULD_BLOCK (nothing
 * mutated until the response is queued). */
moq_result_t session_core_on_announce_update_rejected(
    moq_session_t *s, int slot, const moq_request_endpoint_t *uep)
{
    /* Reserve a drain slot up front: after REQUEST_ERROR + FIN frees the entry
     * (and its stream-ref key), late bytes the peer sent before seeing our FIN
     * must be discarded rather than mistaken for a fresh request. */
    if (s->drain_ref_count >= s->drain_ref_cap) return MOQ_ERR_WOULD_BLOCK;
    uint8_t err_buf[128];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_result_t rc = s->profile->encode_request_error(s, &ew,
        &(moq_request_error_encode_args_t){
            .error_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED });
    if (rc < 0) return rc;
    rc = ann_queue_resp(s, (size_t)slot, err_buf, moq_buf_writer_offset(&ew),
                        true);
    if (rc < 0) return rc;        /* WOULD_BLOCK: retryable, nothing mutated */
    s->profile->commit_inbound_request(s, uep);
    moq_stream_ref_t ref = s->announcements[slot].request_stream_ref;
    if (ref._v != 0) (void)drain_ref_add(s, ref);   /* slot reserved above */
    ann_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* A peer teardown (RESET_STREAM or STOP_SENDING) on an established announce bidi.
 * Role-keyed and idempotent: RECEIVER side -> the announcer withdrew
 * (NAMESPACE_DONE); ANNOUNCER side -> the receiver revoked (NAMESPACE_CANCELLED).
 * The first teardown signal frees the entry + keys; a second one misses the
 * stream-ref lookup and never reaches here. */
moq_result_t session_core_on_announce_torn_down(moq_session_t *s, int slot)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.borrow_epoch = s->borrow_epoch;
    if (e->role == MOQ_ANN_ROLE_RECEIVER) {
        ev.kind = MOQ_EVENT_NAMESPACE_DONE;
        ev.detail_size = (uint32_t)sizeof(moq_namespace_done_event_t);
        ev.u.namespace_done.ann = e->handle;
    } else {
        ev.kind = MOQ_EVENT_NAMESPACE_CANCELLED;
        ev.detail_size = (uint32_t)sizeof(moq_namespace_cancelled_event_t);
        ev.u.namespace_cancelled.ann = e->handle;
        /* The peer's §3.3.3 stream-reset code is not a request-error code and is
         * not surfaced; the reason phrase cannot ride a RESET/STOP. */
    }
    (void)push_event(s, &ev);
    ann_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

static bool ann_is_duplicate_ns(moq_session_t *s,
                                 const uint8_t *ns_id, size_t ns_id_len,
                                 moq_ann_role_t role)
{
    for (size_t i = 0; i < s->ann_cap; i++) {
        moq_ann_entry_t *e = &s->announcements[i];
        if (e->state == MOQ_ANN_FREE) continue;
        if (e->role != role) continue;
        if (e->ns_id_len == ns_id_len &&
            memcmp(e->ns_id_buf, ns_id, ns_id_len) == 0)
            return true;
    }
    return false;
}

/*
 * Build a canonical namespace blob for dup detection.
 * Same format as build_track_id in session_subscribe.c but namespace-only.
 */
static uint8_t *build_ns_id(moq_session_t *s,
                             const moq_namespace_t *ns,
                             size_t *out_len)
{
    size_t total = 1;
    for (size_t i = 0; i < ns->count; i++)
        total += 2 + ns->parts[i].len;
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

    return buf;
}

/* Copy namespace parts into output scratch. */
static bool event_scratch_copy_ns(moq_session_t *s,
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

/* -- Incoming PUBLISH_NAMESPACE handler (semantic) ------------------ */

moq_result_t session_core_on_publish_namespace(moq_session_t *s,
                                                moq_decoded_publish_namespace_t *d)
{
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    uint8_t *nid = NULL;
    size_t nid_len = 0;
    size_t scratch_saved = s->event_scratch_len;

    if (event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    bool req_stream = d->endpoint.has_stream_ref;

    /* A message-level authorization-token reject (e.g. unknown alias): fail the
     * request with REQUEST_ERROR (no event); a REGISTER carried alongside still
     * commits its alias (§10.2.2). Stack-encode so a temporarily full send buffer
     * is a retryable WOULD_BLOCK. */
    if (d->auth_reject_code) {
        /* Stream-correlated reject before commit: REQUEST_ERROR + FIN, then the
         * staging stream-ref key is removed and the bidi retained in the drain
         * ring so a trailing FIN / late bytes are absorbed (the staging sub slot
         * is freed by the request-stream handoff). Reserve drain before mutating. */
        if (req_stream && s->drain_ref_count >= s->drain_ref_cap) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = d->auth_reject_code });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = req_stream
            ? queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        if (req_stream && d->endpoint.stream_ref._v != 0) {
            request_registry_remove_by_streamref(s, d->endpoint.stream_ref);
            (void)drain_ref_add(s, d->endpoint.stream_ref); /* reserved above */
        }
        result = MOQ_OK;
        goto cleanup_all;
    }

    int slot = ann_find_free(s);
    if (slot < 0) {
        if (req_stream && s->drain_ref_count >= s->drain_ref_cap) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = 0x0,
                .reason = (const uint8_t *)"announcement pool full",
                .reason_len = 22 });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = req_stream
            ? queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                              moq_buf_writer_offset(&ew), true)
            : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        if (req_stream && d->endpoint.stream_ref._v != 0) {
            request_registry_remove_by_streamref(s, d->endpoint.stream_ref);
            (void)drain_ref_add(s, d->endpoint.stream_ref); /* reserved above */
        }
        result = MOQ_OK;
        goto cleanup_all;
    }

    nid = build_ns_id(s, &d->track_namespace, &nid_len);
    if (nid_len > 0 && !nid) {
        result = MOQ_ERR_NOMEM;
        goto cleanup_all;
    }

    moq_namespace_t ev_ns;
    if (!event_scratch_copy_ns(s, &d->track_namespace, &ev_ns)) {
        if (scratch_saved == 0) {
            result = close_with_error(s, 0x1,
                "event scratch permanently too small");
            goto cleanup_all;
        }
        result = MOQ_ERR_BUFFER;
        goto cleanup_all;
    }

    /* Copy resolved token values into scratch. */
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

    moq_ann_entry_t *entry = &s->announcements[slot];
    uint32_t live_gen = entry->generation | 1;
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_ANNOUNCEMENT,
                                       s->session_tag, live_gen,
                                       (uint32_t)slot);
    moq_announcement_t handle = { packed };

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_NAMESPACE_PUBLISHED;
    e.detail_size = (uint32_t)sizeof(moq_namespace_published_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.namespace_published.ann = handle;
    e.u.namespace_published.track_namespace = ev_ns;
    e.u.namespace_published.tokens = ev_tokens;
    e.u.namespace_published.token_count = d->token_count;

    rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        result = rc;
        goto cleanup_all;
    }

    /* Commit. */
    entry->generation = live_gen;
    entry->state = MOQ_ANN_PENDING_RECEIVER;
    entry->role = MOQ_ANN_ROLE_RECEIVER;
    entry->handle = handle;
    entry->request_id = d->request_id;
    entry->ns_id_buf = nid;
    entry->ns_id_len = nid_len;
    d->endpoint.kind = MOQ_REQ_ANNOUNCEMENT;
    d->endpoint.slot = slot;
    if (req_stream) {
        /* Hand the request bidi off from the staging sub slot to this announce
         * slot: remove the staging stream-ref mapping, then re-key it to
         * (ANNOUNCEMENT, slot). The request-stream handler frees the staging slot
         * afterwards (after clearing its request_stream_ref, so this mapping is
         * not removed). */
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
    if (nid) s->alloc.free(nid, nid_len, s->alloc.ctx);
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

/* -- Incoming PUBLISH_NAMESPACE_DONE handler (semantic) ------------- */

moq_result_t session_core_on_publish_namespace_done(
    moq_session_t *s, const moq_decoded_publish_namespace_done_t *d)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_NAMESPACE_DONE;
    e.detail_size = (uint32_t)sizeof(moq_namespace_done_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.namespace_done.ann = s->announcements[d->target_slot].handle;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) return rc;

    ann_free_entry(s, (size_t)d->target_slot);
    return MOQ_OK;
}

/* -- Incoming PUBLISH_NAMESPACE_CANCEL handler (semantic) ----------- */

moq_result_t session_core_on_publish_namespace_cancel(
    moq_session_t *s, const moq_decoded_publish_namespace_cancel_t *d)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

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

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_NAMESPACE_CANCELLED;
    e.detail_size = (uint32_t)sizeof(moq_namespace_cancelled_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.namespace_cancelled.ann = s->announcements[d->target_slot].handle;
    e.u.namespace_cancelled.error_code = (moq_request_error_t)d->error_code;
    e.u.namespace_cancelled.reason = reason;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    ann_free_entry(s, (size_t)d->target_slot);
    return MOQ_OK;
}

/* -- Announcement accepted (semantic) ------------------------------ */

moq_result_t session_core_on_announcement_ok(moq_session_t *s,
                                              const moq_decoded_announcement_ok_t *d)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_ann_entry_t *entry = &s->announcements[d->target_slot];

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_NAMESPACE_ACCEPTED;
    e.detail_size = (uint32_t)sizeof(moq_namespace_accepted_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.namespace_accepted.ann = entry->handle;

    moq_result_t erc = push_event(s, &e);
    if (erc < 0) return erc;

    entry->state = MOQ_ANN_ESTABLISHED;
    return MOQ_OK;
}

/* -- Announcement rejected (semantic) ------------------------------ */

moq_result_t session_core_on_announcement_error(moq_session_t *s,
                                                 const moq_decoded_announcement_error_t *d,
                                                 const moq_decoded_redirect_t *redirect)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_ann_entry_t *entry = &s->announcements[d->target_slot];

    /* Stream-correlated profiles: REQUEST_ERROR is terminal, but the peer may
     * deliver its FIN split from the message bytes. Retain the request bidi in
     * the drain ring after freeing so a trailing empty FIN is absorbed rather
     * than mistaken for a fresh request. Reserve the drain slot before mutating
     * (D16 carries the response on the control channel: no stream-ref, no drain). */
    bool req_stream = (entry->request_stream_ref._v != 0);
    moq_stream_ref_t req_ref = entry->request_stream_ref;
    if (req_stream && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    /* Close the announcer's send half after the terminal error so the peer can
     * retire the request bidi; reserve that action up front (alongside the drain
     * slot) for retryability. */
    if (req_stream && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_result_t rc;
    if (redirect) {
        rc = session_core_emit_request_redirect(s,
            MOQ_REQUEST_FAMILY_ANNOUNCEMENT, entry->handle._opaque, redirect,
            d->error_code, d->can_retry, d->retry_after_ms,
            d->reason, d->reason_len);
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
        e.kind = MOQ_EVENT_NAMESPACE_REJECTED;
        e.detail_size = (uint32_t)sizeof(moq_namespace_rejected_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.namespace_rejected.ann = entry->handle;
        e.u.namespace_rejected.error_code = (moq_request_error_t)d->error_code;
        e.u.namespace_rejected.can_retry = d->can_retry;
        e.u.namespace_rejected.retry_after_ms = d->retry_after_ms;
        e.u.namespace_rejected.reason = reason;

        rc = push_event(s, &e);
        if (rc < 0) {
            s->event_scratch_len = scratch_saved;
            return rc;
        }
    }

    if (req_stream && req_ref._v != 0) {
        (void)queue_close_bidi(s, req_ref);   /* action reserved above */
        (void)drain_ref_add(s, req_ref);      /* slot reserved above */
    }
    ann_free_entry(s, (size_t)d->target_slot);
    return MOQ_OK;
}

/* -- Public API: publish_namespace ---------------------------------- */

void moq_publish_namespace_cfg_init(moq_publish_namespace_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_publish_namespace_cfg_t);
}

moq_result_t moq_session_publish_namespace(moq_session_t *s,
    const moq_publish_namespace_cfg_t *cfg, uint64_t now_us,
    moq_announcement_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
#define PNS_CFG_MIN offsetof(moq_publish_namespace_cfg_t, auth_tokens)
#define PNS_CFG_HAS(f) (cfg->struct_size >= \
    offsetof(moq_publish_namespace_cfg_t, f) + sizeof(cfg->f))
    if (cfg->struct_size < PNS_CFG_MIN)
        return MOQ_ERR_INVAL;
    *out_handle = MOQ_ANNOUNCEMENT_INVALID;

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (PNS_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }

    session_begin_advance(s, now_us);

    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;

    /* Validate namespace. */
    if (moq_validate_namespace(&cfg->track_namespace) < 0)
        return MOQ_ERR_INVAL;

    /* Check request ID credit via profile. */
    moq_request_endpoint_t req_ep;
    {
        moq_result_t prc = s->profile->prepare_request(s, &req_ep);
        if (prc < 0) return prc;
    }

    bool req_stream = moq_session_uses_request_streams(s);
    moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0);

    /* Find free slot. */
    int slot = ann_find_free(s);
    if (slot < 0) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    /* Build namespace identity for dup detection. */
    size_t nid_len = 0;
    uint8_t *nid = build_ns_id(s, &cfg->track_namespace, &nid_len);
    if (nid_len > 0 && !nid) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_NOMEM;
    }

    if (ann_is_duplicate_ns(s, nid, nid_len, MOQ_ANN_ROLE_ANNOUNCER)) {
        if (nid) s->alloc.free(nid, nid_len, s->alloc.ctx);
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_INVAL;
    }

    /* Encode PUBLISH_NAMESPACE into send_buf. */
    if (action_queue_full(s)) {
        if (nid) s->alloc.free(nid, nid_len, s->alloc.ctx);
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);

        moq_publish_namespace_encode_args_t pn_args = {
            .request_id = req_ep.request_id,
            .track_namespace = cfg->track_namespace,
            .auth_tokens = auth_tokens,
            .auth_token_count = auth_token_count,
        };
        moq_result_t rc2 = s->profile->encode_publish_namespace(s, &w, &pn_args);
        if (rc2 < 0) {
            if (nid) s->alloc.free(nid, nid_len, s->alloc.ctx);
            s->profile->abort_request(s, &req_ep);
            return rc2;
        }

        size_t encoded_len = moq_buf_writer_offset(&w);
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.borrow_epoch = s->borrow_epoch;
        if (req_stream) {
            /* Stream-correlated profiles: the request opens its own bidi stream
             * and the response (REQUEST_OK/REQUEST_ERROR) returns on it. */
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
            if (nid) s->alloc.free(nid, nid_len, s->alloc.ctx);
            s->profile->abort_request(s, &req_ep);
            return arc;
        }
        s->send_len += encoded_len;
    }

    /* Commit. */
    moq_ann_entry_t *entry = &s->announcements[slot];
    entry->generation |= 1;
    entry->state = MOQ_ANN_PENDING_ANNOUNCER;
    entry->role = MOQ_ANN_ROLE_ANNOUNCER;
    entry->request_id = req_ep.request_id;
    entry->ns_id_buf = nid;
    entry->ns_id_len = nid_len;
    entry->handle = ann_make_handle(s, (size_t)slot);
    req_ep.kind = MOQ_REQ_ANNOUNCEMENT;
    req_ep.slot = slot;
    if (req_stream) {
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
#undef PNS_CFG_MIN
#undef PNS_CFG_HAS
}

/* -- Public API: publish_namespace_done ----------------------------- */

moq_result_t moq_session_publish_namespace_done(moq_session_t *s,
    moq_announcement_t ann, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ann_resolve_handle(s, ann);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->announcements[slot].state != MOQ_ANN_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->announcements[slot].role != MOQ_ANN_ROLE_ANNOUNCER)
        return MOQ_ERR_WRONG_STATE;

    /* Stream-correlated profiles have no PUBLISH_NAMESPACE_DONE message: withdraw
     * by cancelling the request bidi (§3.3.2 = RESET + STOP). */
    if (moq_session_uses_request_streams(s))
        return ann_local_teardown(s, (size_t)slot);

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);

        moq_result_t rc2 = s->profile->encode_publish_namespace_done(s, &w,
            s->announcements[slot].request_id);
        if (rc2 < 0) return rc2;

        size_t elen = moq_buf_writer_offset(&w);
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.kind = MOQ_ACTION_SEND_CONTROL;
        act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
        act.borrow_epoch = s->borrow_epoch;
        act.u.send_control.data = s->send_buf + s->send_len;
        act.u.send_control.len = elen;
        moq_result_t arc = push_action(s, &act);
        if (arc < 0) return arc;
        s->send_len += elen;
    }

    /* Commit: free entry. */
    ann_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* -- Public API: accept_namespace ----------------------------------- */

void moq_accept_namespace_cfg_init(moq_accept_namespace_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_accept_namespace_cfg_t);
}

moq_result_t moq_session_accept_namespace(moq_session_t *s,
    moq_announcement_t ann, const moq_accept_namespace_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_accept_namespace_cfg_t))
        return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ann_resolve_handle(s, ann);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->announcements[slot].state != MOQ_ANN_PENDING_RECEIVER)
        return MOQ_ERR_WRONG_STATE;

    /* Encode REQUEST_OK (zero params, empty Track Properties) to a stack buffer,
     * then queue on the announce transport (request bidi for stream-correlated
     * profiles, fin=false: the announce bidi stays open while established). */
    uint8_t ok_buf[64];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, ok_buf, sizeof(ok_buf));
    moq_result_t rc2 = s->profile->encode_request_ok(s, &w,
        s->announcements[slot].request_id);
    if (rc2 < 0) return rc2;
    moq_result_t arc = ann_queue_resp(s, (size_t)slot, ok_buf,
                                      moq_buf_writer_offset(&w), false);
    if (arc < 0) return arc;

    /* Commit. */
    s->announcements[slot].state = MOQ_ANN_ESTABLISHED;
    return MOQ_OK;
}

/* -- Public API: reject_namespace ----------------------------------- */

void moq_reject_namespace_cfg_init(moq_reject_namespace_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_reject_namespace_cfg_t);
}

moq_result_t moq_session_reject_namespace(moq_session_t *s,
    moq_announcement_t ann, const moq_reject_namespace_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_reject_namespace_cfg_t, redirect))
        return MOQ_ERR_INVAL;   /* pre-redirect minimum; older callers still work */
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;
#define ANN_REJ_HAS(f) \
    (cfg->struct_size >= offsetof(moq_reject_namespace_cfg_t, f) + sizeof(cfg->f))

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ann_resolve_handle(s, ann);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_ann_entry_t *entry = &s->announcements[slot];

    if (entry->role != MOQ_ANN_ROLE_RECEIVER)
        return MOQ_ERR_WRONG_STATE;
    if (entry->state != MOQ_ANN_PENDING_RECEIVER)
        return MOQ_ERR_WRONG_STATE;

    moq_request_error_encode_args_t err_args = {
        .request_id = entry->request_id,
        .error_code = (uint64_t)cfg->error_code,
        .can_retry = cfg->can_retry,
        .retry_after_ms = cfg->retry_after_ms,
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len,
    };
    moq_result_t vrc = reject_apply_redirect(
        s, &err_args, ANN_REJ_HAS(redirect) ? &cfg->redirect : NULL,
        true /* namespace-scoped: track name must be empty */);
    if (vrc < 0) return vrc;
#undef ANN_REJ_HAS

    /* Stream-correlated profiles: REQUEST_ERROR + FIN closes only our send half;
     * the peer's request half can still deliver a trailing FIN (or in-flight
     * bytes). Retain the request bidi in the drain ring after freeing so that is
     * absorbed, not mistaken for a fresh request. Reserve the slot before
     * mutating (D16 carries the response on the control channel: no drain). */
    bool req_stream = (entry->request_stream_ref._v != 0);
    moq_stream_ref_t req_ref = entry->request_stream_ref;
    if (req_stream && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    moq_result_t arc;
    if (err_args.has_redirect) {
        /* Redirect may exceed a fixed reason buffer: sized scratch + queue. */
        arc = queue_request_error_bidi(s, req_ref, &err_args);
    } else {
        /* Reject without processing: REQUEST_ERROR + FIN (§3.3.2). The buffer
         * holds the protocol-max reason (1024) plus envelope overhead; the encoder
         * bounds the reason and the bounded writer prevents overflow regardless. */
        uint8_t err_buf[1152];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, err_buf, sizeof(err_buf));
        moq_result_t rc2 = s->profile->encode_request_error(s, &w, &err_args);
        if (rc2 < 0) return rc2;
        arc = ann_queue_resp(s, (size_t)slot, err_buf,
                             moq_buf_writer_offset(&w), true);
    }
    if (arc < 0) return arc;

    if (req_stream && req_ref._v != 0)
        (void)drain_ref_add(s, req_ref);   /* slot reserved above */
    ann_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* -- Public API: cancel_namespace ----------------------------------- */

void moq_cancel_namespace_cfg_init(moq_cancel_namespace_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_cancel_namespace_cfg_t);
}

moq_result_t moq_session_cancel_namespace(moq_session_t *s,
    moq_announcement_t ann, const moq_cancel_namespace_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_cancel_namespace_cfg_t))
        return MOQ_ERR_INVAL;
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ann_resolve_handle(s, ann);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_ann_entry_t *entry = &s->announcements[slot];

    if (entry->role != MOQ_ANN_ROLE_RECEIVER)
        return MOQ_ERR_WRONG_STATE;
    if (entry->state != MOQ_ANN_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;

    /* Stream-correlated profiles revoke by cancelling the request bidi (§3.3.2 =
     * RESET + STOP). A RESET/STOP carries only a §3.3.3 numeric code, not a
     * REQUEST_ERROR code or a reason phrase, so a non-empty reason is not
     * representable: reject it rather than silently drop it. */
    if (moq_session_uses_request_streams(s)) {
        if (cfg->reason.len > 0) return MOQ_ERR_INVAL;
        return ann_local_teardown(s, (size_t)slot);
    }

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);

    moq_publish_namespace_cancel_encode_args_t cancel_args = {
        .request_id = entry->request_id,
        .error_code = (uint64_t)cfg->error_code,
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len,
    };
    moq_result_t rc2 = s->profile->encode_publish_namespace_cancel(
        s, &w, &cancel_args);
    if (rc2 < 0) return rc2;

    size_t elen = moq_buf_writer_offset(&w);
    moq_action_t act;
    memset(&act, 0, sizeof(act));
    act.kind = MOQ_ACTION_SEND_CONTROL;
    act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
    act.borrow_epoch = s->borrow_epoch;
    act.u.send_control.data = s->send_buf + s->send_len;
    act.u.send_control.len = elen;
    moq_result_t arc = push_action(s, &act);
    if (arc < 0) return arc;
    s->send_len += elen;

    ann_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

moq_result_t moq_session_request_goaway_namespace(
    moq_session_t *s, moq_announcement_t ann,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_request_goaway_cfg_t)) return MOQ_ERR_INVAL;
    if (cfg->new_session_uri.len > 0 && !cfg->new_session_uri.data)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = ann_resolve_handle(s, ann);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}
