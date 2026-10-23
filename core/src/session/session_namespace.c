#include "session_internal.h"
#include "request_stream_internal.h"
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
    if (s->announcements[slot].state == MOQ_ANN_FREE ||
        s->announcements[slot].state == MOQ_ANN_DRAINING) return -1;
    return (int)slot;
}

void ann_free_entry(moq_session_t *s, size_t slot)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    request_registry_remove_by_id(s, e->request_id);
    /* Stream-correlated profiles register by request stream_ref; remove that key
     * and reset the field so a recycled slot never carries a stale ref. */
    if (e->rs.ref._v != 0) {
        request_registry_remove_by_streamref(s, e->rs.ref);
        e->rs.ref = moq_stream_ref_from_u64(0);
    }
    /* Reset (but preserve) the co-allocated request-stream receive buffer so a
     * recycled slot never reuses stale buffered bytes. */
    e->rs.recv_len = 0;
    e->rs.recv_fin = false;
    if (e->ns_id_buf) {
        s->alloc.free(e->ns_id_buf, e->ns_id_len, s->alloc.ctx);
        e->ns_id_buf = NULL;
        e->ns_id_len = 0;
    }
    ns_update_release(s, &e->update);
    uint32_t next_gen = e->generation + 1;
    uint8_t *recv_buf = e->rs.recv_buf;
    size_t recv_cap = e->rs.recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_ANN_FREE;
    e->generation = next_gen;
    e->rs.recv_buf = recv_buf;
    e->rs.recv_cap = recv_cap;
}

/* D22 owns the application outcome separately from each transport direction.
 * All pending work stays in this pool entry; no terminal needs an allocation
 * or a second receive buffer. */
static moq_result_t ann_flush_terminal(moq_session_t *s, int slot)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    if (e->state == MOQ_ANN_FREE) return MOQ_OK;
    if (e->response_validated) return MOQ_ERR_WOULD_BLOCK;
    bool blocked = false;
    if (e->terminal_cause && !e->terminal_notified) {
        if (event_queue_full(s)) {
            blocked = true;
        } else {
            moq_event_t ev;
            memset(&ev, 0, sizeof(ev));
            ev.kind = MOQ_EVENT_NAMESPACE_TERMINATED;
            ev.detail_size = (uint32_t)sizeof(moq_namespace_terminated_event_t);
            ev.borrow_epoch = s->borrow_epoch;
            ev.u.namespace_terminated.ann = e->handle;
            ev.u.namespace_terminated.cause = e->terminal_cause;
            ev.u.namespace_terminated.local_role = e->role == MOQ_ANN_ROLE_ANNOUNCER
                ? MOQ_NAMESPACE_ROLE_REQUESTER : MOQ_NAMESPACE_ROLE_RESPONDER;
            ev.u.namespace_terminated.stream_error_code = e->terminal_code;
            (void)push_event(s, &ev);
            e->terminal_notified = true;
            e->state = MOQ_ANN_DRAINING;
        }
    }
    moq_request_lifetime_t *life = &e->rs.lifetime;
    if (e->finish_fin && !life->local_fin && !life->local_reset) {
        /* A later STOP/RESET can make a previously owed graceful FIN invalid,
         * without changing the first application outcome. */
        if (moq_request_lifetime_check_fin(life) != MOQ_OK) {
            e->finish_fin = false;
            e->cancel_pending = true;
        } else if (action_queue_full(s)) {
            blocked = true;
        } else {
            (void)queue_close_bidi(s, e->rs.ref);
            (void)moq_request_lifetime_fin_committed(life);
            e->finish_fin = false;
        }
    }
    if (e->cancel_pending) {
        unsigned owed = moq_request_lifetime_cancel_actions(life);
        if (owed & MOQ_REQUEST_CANCEL_RESET_LOCAL) {
            if (action_queue_full(s)) {
                blocked = true;
            } else {
                request_bidi_reset_local(s, e->rs.ref);
                (void)moq_request_lifetime_reset_committed(life);
            }
        }
        if (owed & MOQ_REQUEST_CANCEL_STOP_PEER) {
            if (action_queue_full(s)) {
                blocked = true;
            } else {
                moq_action_t a;
                memset(&a, 0, sizeof(a));
                a.kind = MOQ_ACTION_STOP_BIDI_STREAM;
                a.detail_size = (uint32_t)sizeof(moq_stop_bidi_stream_action_t);
                a.borrow_epoch = s->borrow_epoch;
                a.u.stop_bidi_stream.stream_ref = e->rs.ref;
                a.u.stop_bidi_stream.error_code = MOQ_STREAM_ERROR_CANCELLED;
                (void)push_action(s, &a);
                (void)moq_request_lifetime_stop_committed(life);
            }
        }
        if (moq_request_lifetime_cancel_actions(life) == 0)
            e->cancel_pending = false;
    }
    if (!blocked && e->state == MOQ_ANN_DRAINING &&
        (life->local_fin || life->local_reset) &&
        (life->peer_fin || life->peer_reset))
        ann_free_entry(s, (size_t)slot);
    return blocked ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
}

static void ann_set_terminal(moq_ann_entry_t *e,
    moq_namespace_terminal_cause_t cause, uint64_t code)
{
    if (!e->terminal_cause && e->state != MOQ_ANN_DRAINING) {
        if (e->goaway_sent && cause != MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY) {
            e->state = MOQ_ANN_DRAINING;
        } else {
            e->terminal_cause = cause;
            e->terminal_code = code;
        }
    }
    if (cause == MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE)
        e->finish_fin = true;
    else
        e->cancel_pending = true;
}

/* Apply physical facts after selecting the first outcome, before issuing any
 * output. Crossed STOP then RESET must not queue a now-unnecessary STOP. */
static void ann_apply_deferred(moq_ann_entry_t *e)
{
    /* FIN's place in causal order is known on arrival, but its meaning is
     * known only after the original response and any suffix are parsed. */
    if (e->deferred_cause == MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE &&
        e->rs.recv_len > 0)
        e->deferred_cause = MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE;
    if (e->deferred_cause)
        ann_set_terminal(e, e->deferred_cause, e->deferred_code);
    if (e->deferred_reset)
        (void)moq_request_lifetime_peer_reset(&e->rs.lifetime);
    if (e->deferred_stop)
        (void)moq_request_lifetime_peer_stop(&e->rs.lifetime);
    if (e->deferred_fin)
        (void)moq_request_lifetime_peer_fin(&e->rs.lifetime);
    e->deferred_cause = 0;
    e->deferred_code = 0;
    e->deferred_reset = false;
    e->deferred_stop = false;
    e->deferred_fin = false;
}

void ann_validate_response(moq_session_t *s, int slot)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    if (!e->response_validated) {
        e->response_validated = true;
        e->response_fin = e->rs.recv_fin && !e->deferred_fin;
    }
}

static void ann_receive_fin(moq_ann_entry_t *e, bool truncated)
{
    if (e->rs.lifetime.peer_fin) return;
    moq_result_t rc = moq_request_lifetime_peer_fin(&e->rs.lifetime);
    if (e->state == MOQ_ANN_DRAINING || e->terminal_cause) return;
    if (truncated)
        ann_set_terminal(e, MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE, 0);
    else if (rc == MOQ_REQUEST_LIFETIME_FAILED)
        ann_set_terminal(e, MOQ_NAMESPACE_TERMINAL_MISSING_RESPONSE, 0);
    else if (e->role == MOQ_ANN_ROLE_ANNOUNCER || e->goaway_sent)
        ann_set_terminal(e, MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE, 0);
    /* A requester's FIN is legal even before the receiver accepts/rejects. */
}

static moq_result_t ann_handle_d22_bytes(moq_session_t *s, int slot,
    const uint8_t *buf, size_t len, bool fin)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    if (!e->response_validated &&
        (e->state == MOQ_ANN_DRAINING || e->terminal_cause)) {
        if (e->migration_committed && len > 0) {
            moq_result_t rc = drain_goaway_scan(s, &e->migration_scan, buf, len);
            if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
        }
        if (fin) ann_receive_fin(e, false);
        return ann_flush_terminal(s, slot);
    }
    if (len > e->rs.recv_cap - e->rs.recv_len)
        return close_with_error(s, 0x3, "announce request stream message too large");
    if (len > 0) {
        memcpy(e->rs.recv_buf + e->rs.recv_len, buf, len);
        e->rs.recv_len += len;
    }
    if (fin || e->handoff_fin_pending) {
        if (e->response_validated && !e->response_fin) {
            e->deferred_fin = true;
            if (!e->deferred_cause && e->role == MOQ_ANN_ROLE_ANNOUNCER)
                e->deferred_cause = MOQ_NAMESPACE_TERMINAL_PEER_FIN_COMPLETE;
        }
        e->rs.recv_fin = true;
        e->handoff_fin_pending = false;
    }
    for (;;) {
        size_t consumed = 0;
        moq_result_t rc = e->role == MOQ_ANN_ROLE_ANNOUNCER
            ? s->profile->process_response_stream(s, e->rs.ref, slot,
                (uint32_t)MOQ_REQ_ANNOUNCEMENT, e->rs.recv_buf, e->rs.recv_len,
                e->rs.recv_fin, &consumed)
            : s->profile->process_request_stream(s, e->rs.ref, slot,
                e->rs.recv_buf, e->rs.recv_len, e->rs.recv_fin, &consumed);
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
        if (rc < 0) return rc;
        if (consumed == 0) {
            if (e->rs.recv_fin && !e->deferred_fin)
                ann_receive_fin(e, e->rs.recv_len > 0);
            ann_apply_deferred(e);
            if (e->rs.recv_fin) ann_receive_fin(e, e->rs.recv_len > 0);
            return ann_flush_terminal(s, slot);
        }
        if (e->state == MOQ_ANN_FREE) return MOQ_OK;
        size_t remaining = e->rs.recv_len - consumed;
        if (remaining > 0)
            memmove(e->rs.recv_buf, e->rs.recv_buf + consumed, remaining);
        e->rs.recv_len = remaining;
        e->response_validated = false;
        /* Finish all original complete messages before later signals. FIN is
         * at the end of the byte sequence, not immediately after the OK. */
        if (remaining > 0 && e->state != MOQ_ANN_DRAINING && !e->terminal_cause)
            continue;
        if (e->response_fin) ann_receive_fin(e, remaining > 0);
        e->response_fin = false;
        ann_apply_deferred(e);
        if (e->rs.recv_fin) ann_receive_fin(e, remaining > 0);
        if (remaining == 0 || e->state == MOQ_ANN_DRAINING || e->terminal_cause)
            return ann_flush_terminal(s, slot);
    }
}

void ann_retry_pending(moq_session_t *s)
{
    if (!s->profile->premature_fin_fails_request) return;
    for (size_t i = 0; i < s->ann_cap && session_is_active(s); ++i) {
        moq_ann_entry_t *e = &s->announcements[i];
        if (e->state == MOQ_ANN_FREE) continue;
        if (e->response_validated)
            (void)ann_handle_d22_bytes(s, (int)i, NULL, 0, false);
        else if (e->terminal_cause || e->state == MOQ_ANN_DRAINING)
            (void)ann_flush_terminal(s, (int)i);
    }
}

/* Queue an already-encoded announce response on the announce transport: the
 * request bidi for stream-correlated profiles (retryable WOULD_BLOCK contract),
 * the shared control channel otherwise. */
static moq_result_t ann_queue_resp(moq_session_t *s, size_t slot,
                                   const uint8_t *data, size_t len, bool fin)
{
    if (moq_session_uses_request_streams(s))
        return queue_send_bidi(s, s->announcements[slot].rs.ref,
                               data, len, fin);
    return queue_send_control(s, data, len);
}

/* D22 accepts a retained, direction-by-direction cancellation. Legacy stream
 * profiles keep the atomic whole-stream abort and drain-ring contract. */
static moq_result_t ann_local_teardown(moq_session_t *s, size_t slot)
{
    if (action_queue_avail(s) < 1) return MOQ_ERR_WOULD_BLOCK;
    if (s->profile->premature_fin_fails_request) {
        moq_ann_entry_t *e = &s->announcements[slot];
        e->state = MOQ_ANN_DRAINING;
        e->cancel_pending = true;
        /* Acceptance owns both remaining directions. A short action queue
         * defers the second output, not the accepted operation's result. */
        (void)ann_flush_terminal(s, (int)slot);
        return MOQ_OK;
    }
    /* A peer FIN already observed on this bidi -- latched, or handed over with
     * the request -- leaves nothing to absorb, so the drain is not owed and a
     * full ring must not refuse the teardown. */
    bool need_drain = !ann_peer_fin_observed(&s->announcements[slot]);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    moq_stream_ref_t ref = s->announcements[slot].rs.ref;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_ABORT_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_abort_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.abort_bidi_stream.stream_ref = ref;
    a.u.abort_bidi_stream.error_code = 0x1;   /* CANCELLED (§3.3.3) */
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;
    if (need_drain && ref._v != 0)
        (void)drain_ref_add(s, ref);   /* slot reserved above */
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
    if (s->profile->premature_fin_fails_request)
        return ann_handle_d22_bytes(s, slot, buf, len, fin);
    moq_ann_entry_t *e = &s->announcements[slot];
    if (len > 0) {
        if (len > e->rs.recv_cap - e->rs.recv_len)
            return close_with_error(s, 0x3,
                "announce request stream message too large");
        memcpy(e->rs.recv_buf + e->rs.recv_len, buf, len);
        e->rs.recv_len += len;
    }
    if (fin) e->rs.recv_fin = true;
    /* A FIN that rode the creating PUBLISH_NAMESPACE was handed to this entry
     * by the commit that re-keyed the bidi; from here it is the same fact as a
     * wire FIN. The durable latch takes it over and the owner is RETAINED --
     * the application still has to accept or reject -- so this cannot block. */
    if (e->handoff_fin_pending) {
        e->rs.recv_fin = true;
        e->handoff_fin_pending = false;
    }

    for (;;) {
        e = &s->announcements[slot];
        bool as_response = (e->role == MOQ_ANN_ROLE_ANNOUNCER);
        size_t consumed = 0;
        moq_result_t rc = as_response
            ? s->profile->process_response_stream(s, stream_ref, slot,
                  (uint32_t)MOQ_REQ_ANNOUNCEMENT,
                  e->rs.recv_buf, e->rs.recv_len, e->rs.recv_fin, &consumed)
            : s->profile->process_request_stream(s, stream_ref, slot,
                  e->rs.recv_buf, e->rs.recv_len, e->rs.recv_fin, &consumed);
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;  /* keep buffer; re-feed retries */
        if (rc < 0) return rc;
        if (consumed == 0) {
            /* The receiver closed its direction before answering: on a
             * profile where a FIN is no cancellation the announcement failed. */
            if (e->rs.recv_fin && as_response &&
                e->state == MOQ_ANN_PENDING_ANNOUNCER &&
                s->profile->premature_fin_fails_request)
                return request_stream_failed_by_fin(s, stream_ref, e->rs.recv_len > 0);
            if (e->rs.recv_fin && e->rs.recv_len > 0)
                return close_with_error(s, 0x3,
                    "truncated message on announce request stream");
            return MOQ_OK;        /* incomplete or clean FIN; wait for teardown */
        }
        /* The dispatch may have terminated the announcement (REQUEST_ERROR / a
         * rejected update). */
        if (s->announcements[slot].state == MOQ_ANN_FREE) return MOQ_OK;
        e = &s->announcements[slot];
        size_t remaining = e->rs.recv_len - consumed;
        if (remaining > 0)
            memmove(e->rs.recv_buf, e->rs.recv_buf + consumed, remaining);
        e->rs.recv_len = remaining;
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
     * must be discarded rather than mistaken for a fresh request. A peer FIN
     * already observed on this bidi -- latched by the announcement handler
     * before dispatch, or handed over with the creating request -- leaves
     * nothing to absorb, so the reference is not owed and a full ring must not
     * refuse the terminal. */
    bool need_drain = !s->profile->premature_fin_fails_request &&
                      !ann_peer_fin_observed(&s->announcements[slot]);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
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
    if (s->profile->premature_fin_fails_request) {
        moq_ann_entry_t *e = &s->announcements[slot];
        (void)moq_request_lifetime_fin_committed(&e->rs.lifetime);
        e->state = MOQ_ANN_DRAINING;
        return MOQ_OK; /* parser still owns the buffered update until consumed */
    }
    moq_stream_ref_t ref = s->announcements[slot].rs.ref;
    if (need_drain && ref._v != 0)
        (void)drain_ref_add(s, ref);   /* slot reserved above */
    ann_free_entry(s, (size_t)slot);
    return MOQ_OK;
}

/* A peer teardown (RESET_STREAM or STOP_SENDING) on an established announce bidi.
 * Role-keyed and idempotent: RECEIVER side -> the announcer withdrew
 * (NAMESPACE_DONE); ANNOUNCER side -> the receiver revoked (NAMESPACE_CANCELLED).
 * The first teardown signal frees the entry + keys; a second one misses the
 * stream-ref lookup and never reaches here. */
moq_result_t session_core_on_announce_torn_down(moq_session_t *s, int slot,
                                                moq_request_terminal_t term)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    if (s->profile->premature_fin_fails_request) {
        moq_namespace_terminal_cause_t cause;
        switch (term.cause) {
        case MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM:
            cause = MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM; break;
        case MOQ_REQUEST_TERMINAL_PEER_STOP_SENDING:
            cause = MOQ_NAMESPACE_TERMINAL_PEER_STOP_SENDING; break;
        case MOQ_REQUEST_TERMINAL_MISSING_RESPONSE:
            cause = MOQ_NAMESPACE_TERMINAL_MISSING_RESPONSE; break;
        default:
            cause = MOQ_NAMESPACE_TERMINAL_TRUNCATED_MESSAGE; break;
        }
        if (!e->deferred_cause) {
            e->deferred_cause = cause;
            e->deferred_code = term.error_code;
        }
        if (cause == MOQ_NAMESPACE_TERMINAL_PEER_RESET_STREAM)
            e->deferred_reset = true;
        else if (cause == MOQ_NAMESPACE_TERMINAL_PEER_STOP_SENDING)
            e->deferred_stop = true;
        else
            e->deferred_fin = true;
        if (e->response_validated) return MOQ_ERR_WOULD_BLOCK;
        ann_apply_deferred(e);
        bool silent = e->state == MOQ_ANN_DRAINING && !e->terminal_cause;
        moq_result_t rc = ann_flush_terminal(s, slot);
        return silent ? MOQ_OK : rc;
    }
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
        if (e->state == MOQ_ANN_FREE || e->state == MOQ_ANN_DRAINING) continue;
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

static bool ann_scratch_region(size_t cap, size_t *cursor, size_t bytes,
                               size_t align)
{
    if (*cursor > cap) return false;
    size_t pad = (align - (*cursor & (align - 1))) & (align - 1);
    if (pad > cap - *cursor) return false;
    *cursor += pad;
    if (bytes > cap - *cursor) return false;
    *cursor += bytes;
    return true;
}

/* Matches the actual projection order: namespace array and parts, token bytes,
 * then the aligned resolved-token array. Empty arrays consume no padding. */
static bool ann_projection_fits(const moq_session_t *s,
    const moq_decoded_publish_namespace_t *d, size_t cursor)
{
    size_t count = d->track_namespace.count;
    if (count > SIZE_MAX / sizeof(moq_bytes_t) ||
        d->token_count > SIZE_MAX / sizeof(moq_resolved_token_t)) return false;
    if (count && !ann_scratch_region(s->event_scratch_cap, &cursor,
            count * sizeof(moq_bytes_t), _Alignof(moq_bytes_t))) return false;
    for (size_t i = 0; i < count; ++i)
        if (!ann_scratch_region(s->event_scratch_cap, &cursor,
                d->track_namespace.parts[i].len, 1)) return false;
    for (size_t i = 0; i < d->token_count; ++i)
        if (!ann_scratch_region(s->event_scratch_cap, &cursor,
                d->tokens[i].token_value.len, 1)) return false;
    return !d->token_count || ann_scratch_region(s->event_scratch_cap, &cursor,
        d->token_count * sizeof(moq_resolved_token_t), _Alignof(moq_resolved_token_t));
}

/* -- Incoming PUBLISH_NAMESPACE handler (semantic) ------------------ */

moq_result_t session_core_on_publish_namespace(moq_session_t *s,
                                                moq_decoded_publish_namespace_t *d,
                                                bool request_fin_observed)
{
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    uint8_t *nid = NULL;
    size_t nid_len = 0;
    size_t scratch_saved = s->event_scratch_len;

    bool retained = s->profile->premature_fin_fails_request;
    if (!retained && event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    bool req_stream = d->endpoint.has_stream_ref;
    /* The pre-commit rejection's single drain decision, shared by the auth and
     * pool-full branches. An observed FIN closes the peer's send half -- with
     * this message, or on an earlier call whose refusal staging retained -- so
     * no late bytes can arrive and the reference is unnecessary; the rejection
     * still routes REQUEST_ERROR + FIN on the bidi and still removes its staging
     * stream-ref key, which `req_stream` alone governs. */
    bool reject_drain = req_stream && !request_fin_observed;

    /* A message-level authorization-token reject (e.g. unknown alias): fail the
     * request with REQUEST_ERROR (no event); a REGISTER carried alongside still
     * commits its alias (§10.2.2). Stack-encode so a temporarily full send buffer
     * is a retryable WOULD_BLOCK. */
    if (d->auth_reject_code) {
        /* Stream-correlated reject before commit: REQUEST_ERROR + FIN, then the
         * staging stream-ref key is removed and the bidi retained in the drain
         * ring so a trailing FIN / late bytes are absorbed (the staging sub slot
         * is freed by the request-stream handoff). Reserve drain before mutating. */
        if (reject_drain && s->drain_ref_count >= s->drain_ref_cap) {
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
            if (reject_drain)
                (void)drain_ref_add(s, d->endpoint.stream_ref); /* reserved above */
        }
        result = MOQ_OK;
        goto cleanup_all;
    }

    bool impossible = retained && !ann_projection_fits(s, d, 0);
    int slot = ann_find_free(s);
    if (slot < 0 || impossible) {
        if (reject_drain && s->drain_ref_count >= s->drain_ref_cap) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[256];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        /* A full pool is this side's capacity; an announcement whose event
         * can never fit is not load, and stays INTERNAL_ERROR. */
        moq_request_error_encode_args_t err = impossible
            ? (moq_request_error_encode_args_t){
                  .request_id = d->request_id, .error_code = 0x0,
                  .reason = (const uint8_t *)"announcement event too large",
                  .reason_len = 28 }
            : session_capacity_request_error(s, d->request_id,
                                             "announcement pool full", 22);
        rc = s->profile->encode_request_error(s, &ew, &err);
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
            if (reject_drain)
                (void)drain_ref_add(s, d->endpoint.stream_ref); /* reserved above */
        }
        result = MOQ_OK;
        goto cleanup_all;
    }

    if (retained && (event_queue_full(s) ||
                    !ann_projection_fits(s, d, scratch_saved))) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    nid = build_ns_id(s, &d->track_namespace, &nid_len);
    if (nid_len > 0 && !nid) {
        result = MOQ_ERR_NOMEM;
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
    if (retained) {
        (void)moq_request_lifetime_init(&entry->rs.lifetime,
            MOQ_REQUEST_FAMILY_ANNOUNCEMENT, false);
        /* The staging handoff drives FIN after transferring any trailing
         * bytes; latching it here would hide a truncated trailing message. */
    }
    d->endpoint.kind = MOQ_REQ_ANNOUNCEMENT;
    d->endpoint.slot = slot;
    if (req_stream) {
        /* Hand the request bidi off from the staging sub slot to this announce
         * slot: remove the staging stream-ref mapping, then re-key it to
         * (ANNOUNCEMENT, slot). The request-stream handler frees the staging slot
         * afterwards (after clearing its rs.ref, so this mapping is
         * not removed). */
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
    e.u.namespace_cancelled.error_code =
        s->profile->semantic_request_error(d->error_code);
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
    if (s->profile->premature_fin_fails_request)
        (void)moq_request_lifetime_initial_response_committed(
            &entry->rs.lifetime, false, true);
    return MOQ_OK;
}

static moq_result_t ann_error_request_local(moq_session_t *s,
    const moq_decoded_announcement_error_t *d,
    const moq_decoded_redirect_t *redirect)
{
    moq_ann_entry_t *entry = &s->announcements[d->target_slot];
    moq_event_scratch_verdict_t verdict = redirect
        ? redirect_event_scratch_classify(s, redirect, d->reason_len)
        : event_scratch_classify_bytes(s, d->reason_len);
    if (verdict == MOQ_EVENT_SCRATCH_PERMANENT) {
        (void)moq_request_lifetime_initial_response_committed(
            &entry->rs.lifetime, false, false);
        ann_set_terminal(entry, MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY, 0);
        return MOQ_OK; /* consumed; scalar outcome/output retry independently */
    }
    if (verdict == MOQ_EVENT_SCRATCH_BLOCKED || event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;
    moq_result_t rc;
    if (redirect) {
        rc = session_core_emit_request_redirect(s,
            MOQ_REQUEST_FAMILY_ANNOUNCEMENT, entry->handle._opaque, redirect,
            d->error_code, d->can_retry, d->retry_after_ms,
            d->reason, d->reason_len);
    } else {
        size_t saved = s->event_scratch_len;
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_NAMESPACE_REJECTED;
        ev.detail_size = (uint32_t)sizeof(moq_namespace_rejected_event_t);
        ev.borrow_epoch = s->borrow_epoch;
        ev.u.namespace_rejected.ann = entry->handle;
        ev.u.namespace_rejected.error_code =
            s->profile->semantic_request_error(d->error_code);
        ev.u.namespace_rejected.can_retry = d->can_retry;
        ev.u.namespace_rejected.retry_after_ms = d->retry_after_ms;
        if (d->reason_len) {
            ev.u.namespace_rejected.reason.data =
                event_scratch_copy(s, d->reason, d->reason_len);
            ev.u.namespace_rejected.reason.len = d->reason_len;
        }
        rc = push_event(s, &ev);
        if (rc < 0) s->event_scratch_len = saved;
    }
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    (void)moq_request_lifetime_initial_response_committed(
        &entry->rs.lifetime, false, false);
    entry->state = MOQ_ANN_DRAINING;
    entry->finish_fin = true;
    return MOQ_OK;
}

moq_result_t ann_receive_goaway(moq_session_t *s, int slot,
    const uint8_t *uri, size_t uri_len, uint64_t timeout_ms)
{
    moq_ann_entry_t *entry = &s->announcements[slot];
    moq_event_scratch_verdict_t verdict = event_scratch_classify_bytes(s, uri_len);
    if (verdict == MOQ_EVENT_SCRATCH_PERMANENT) {
        ann_set_terminal(entry, MOQ_NAMESPACE_TERMINAL_LOCAL_CAPACITY, 0);
        entry->migration_committed = true;
        return MOQ_OK;
    }
    if (verdict == MOQ_EVENT_SCRATCH_BLOCKED || event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;
    size_t saved = s->event_scratch_len;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_REQUEST_GOAWAY;
    ev.detail_size = (uint32_t)sizeof(moq_request_goaway_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.request_goaway.family = MOQ_REQUEST_FAMILY_ANNOUNCEMENT;
    ev.u.request_goaway.handle.raw = entry->handle._opaque;
    ev.u.request_goaway.timeout_ms = timeout_ms;
    if (uri_len) {
        ev.u.request_goaway.new_session_uri.data = event_scratch_copy(s, uri, uri_len);
        ev.u.request_goaway.new_session_uri.len = uri_len;
    }
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) {
        s->event_scratch_len = saved;
        return rc;
    }
    entry->migration_committed = true;
    entry->state = MOQ_ANN_DRAINING;
    entry->cancel_pending = true;
    return MOQ_OK;
}

/* -- Announcement rejected (semantic) ------------------------------ */

moq_result_t session_core_on_announcement_error(moq_session_t *s,
                                                 const moq_decoded_announcement_error_t *d,
                                                 const moq_decoded_redirect_t *redirect)
{
    if (s->profile->premature_fin_fails_request)
        return ann_error_request_local(s, d, redirect);
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_ann_entry_t *entry = &s->announcements[d->target_slot];

    /* Stream-correlated profiles: REQUEST_ERROR is terminal, but the peer may
     * deliver its FIN split from the message bytes. Retain the request bidi in
     * the drain ring after freeing so a trailing empty FIN is absorbed rather
     * than mistaken for a fresh request. Reserve the drain slot before mutating
     * (D16 carries the response on the control channel: no stream-ref, no drain). */
    bool req_stream = (entry->rs.ref._v != 0);
    moq_stream_ref_t req_ref = entry->rs.ref;
    bool need_drain = req_stream && !ann_peer_fin_observed(entry);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
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
        /* The emitter reports a terminal close as MOQ_OK, so the teardown below
         * runs only while the session is still open. */
        if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
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
        e.u.namespace_rejected.error_code =
            s->profile->semantic_request_error(d->error_code);
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
        if (need_drain)
            (void)drain_ref_add(s, req_ref);  /* slot reserved above */
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
    /* The application may not publish under a reserved session-level
     * namespace. */
    if (s->profile->reserved_session_namespaces &&
        session_namespace_reserved(&cfg->track_namespace))
        return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);

    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    if (moq_validate_auth_tokens(auth_tokens, auth_token_count,
                                 s->profile->auth_token_type_max) < 0)
        return MOQ_ERR_INVAL;

    /* Validate namespace. */
    if (moq_validate_namespace_min_fields(
            &cfg->track_namespace,
            s->profile->min_track_namespace_fields) < 0)
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
        if (rc2 == MOQ_ERR_BUFFER && s->send_len > 0) {
            moq_buf_writer_t probe;
            rc2 = send_probe_begin(s, &probe);
            if (rc2 == MOQ_OK) {
                rc2 = s->profile->encode_publish_namespace(s, &probe, &pn_args);
                rc2 = send_probe_end(s, &probe, rc2);
            }
        }
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
    if (s->profile->premature_fin_fails_request)
        (void)moq_request_lifetime_init(&entry->rs.lifetime,
            MOQ_REQUEST_FAMILY_ANNOUNCEMENT, true);
    req_ep.kind = MOQ_REQ_ANNOUNCEMENT;
    req_ep.slot = slot;
    if (req_stream) {
        entry->rs.ref = req_ref;
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
    moq_ann_entry_t *entry = &s->announcements[slot];
    if (entry->terminal_cause || entry->response_validated)
        return MOQ_ERR_WRONG_STATE;
    if (entry->state != MOQ_ANN_ESTABLISHED &&
        !(s->profile->premature_fin_fails_request &&
          entry->state == MOQ_ANN_PENDING_ANNOUNCER))
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
    moq_ann_entry_t *entry = &s->announcements[slot];
    if (s->profile->premature_fin_fails_request &&
        (entry->terminal_cause || entry->response_validated ||
         moq_request_lifetime_check_initial_response(&entry->rs.lifetime, true) != MOQ_OK))
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
    if (s->profile->premature_fin_fails_request)
        (void)moq_request_lifetime_initial_response_committed(
            &entry->rs.lifetime, true, true);
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
    /* A retry is sent as milliseconds + 1 (d16 Section 9.8, d18 Section
     * 10.6): refuse an interval whose wire form is unrepresentable rather
     * than let it wrap to 0, which means "do not retry". */
    if (cfg->can_retry && cfg->retry_after_ms >= MOQ_QUIC_VARINT_MAX)
        return MOQ_ERR_INVAL;
    /* The code must be representable in THIS profile's wire encoding; refuse
     * before any mutation rather than truncate (draft-16 encodes a QUIC
     * varint, draft-18 a vi64 spanning the full 64-bit range). */
    if (cfg->error_code > s->profile->request_error_wire_max)
        return MOQ_ERR_INVAL;
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
    bool retained = s->profile->premature_fin_fails_request;
    if (retained && (entry->terminal_cause || entry->response_validated ||
        moq_request_lifetime_check_initial_response(&entry->rs.lifetime, true) != MOQ_OK))
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
    bool req_stream = (entry->rs.ref._v != 0);
    moq_stream_ref_t req_ref = entry->rs.ref;
    bool need_drain = req_stream && !retained && !ann_peer_fin_observed(entry);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
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

    if (retained) {
        (void)moq_request_lifetime_initial_response_committed(
            &entry->rs.lifetime, true, false);
        (void)moq_request_lifetime_fin_committed(&entry->rs.lifetime);
        entry->state = MOQ_ANN_DRAINING;
        (void)ann_flush_terminal(s, slot);
        return MOQ_OK;
    }
    if (need_drain && req_ref._v != 0)
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
    /* The code must be representable in THIS profile's wire encoding; refuse
     * before any mutation rather than truncate (draft-16 encodes a QUIC
     * varint, draft-18 a vi64 spanning the full 64-bit range). */
    if (cfg->error_code > s->profile->request_error_wire_max)
        return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = ann_resolve_handle(s, ann);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_ann_entry_t *entry = &s->announcements[slot];

    if (entry->role != MOQ_ANN_ROLE_RECEIVER)
        return MOQ_ERR_WRONG_STATE;
    if (entry->state != MOQ_ANN_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (entry->terminal_cause || entry->response_validated)
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
    if (s->announcements[slot].terminal_cause ||
        s->announcements[slot].response_validated) return MOQ_ERR_WRONG_STATE;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}

/* -- Draft-22 namespace updates (§9.5) ------------------------------- *
 * PUBLISH_NAMESPACE's update carries AUTHORIZATION_TOKEN only. The receiver
 * of the announcement answers it; the announcer sends it. */

static bool ann_updates_supported(const moq_session_t *s)
{
    return s->profile->request_codec &&
           s->profile->request_codec->decode_namespace_update &&
           s->profile->encode_namespace_update;
}

/* Answer a refused update with REQUEST_ERROR + FIN, report REFUSED and end
 * the announcement on this side. Event and output room are reserved before
 * anything is committed. */
static moq_result_t ann_refuse_update(moq_session_t *s, int slot,
                                      ns_update_admission_t *a, bool report)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    if (report && event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    uint8_t buf[128];
    size_t len = 0;
    moq_result_t rc = ns_update_encode_error(s, a->d.request_id, a->refuse_code,
                                             NULL, 0, buf, sizeof(buf), &len);
    if (rc < 0) return rc;
    rc = queue_send_bidi(s, e->rs.ref, buf, len, true);
    if (rc < 0) return rc;
    if (report)
        (void)ns_update_push_result_event(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT,
            e->handle._opaque, a->d.request_id, MOQ_NAMESPACE_UPDATE_OUTCOME_REFUSED,
            MOQ_REQUEST_ROLE_RESPONDER, a->refuse_code, NULL, 0, false, 0);
    ns_update_admission_commit(s, a);
    (void)moq_request_lifetime_fin_committed(&e->rs.lifetime);
    e->state = MOQ_ANN_DRAINING;
    return MOQ_OK;
}

moq_result_t ann_on_namespace_update(moq_session_t *s, int slot,
                                     const moq_request_envelope_t *env)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    if (e->update.owed)
        return close_with_error(s, s->profile->request_updates_exceeded_error,
                                "too many outstanding REQUEST_UPDATEs");
    ns_update_admission_t a;
    moq_result_t rc = ns_update_admit(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT,
                                      e->rs.ref, env, &a);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    rc = a.refuse_code
        ? ann_refuse_update(s, slot, &a, true)
        : ns_update_defer(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT, e->handle._opaque,
                          &e->update, &a);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) ns_update_admission_abort(s, &a);
    return rc;
}

/* Updates not left to the application: the update is admitted exactly as a
 * deferred one is -- its Request ID, its token-cache transactions (a
 * REGISTER commits even though the update is refused, draft-22 §8.9), its
 * parameter limits -- and refused with its admission code, else
 * NOT_SUPPORTED. No event, as before updates could be deferred. */
moq_result_t ann_refuse_namespace_update(moq_session_t *s, int slot,
                                         const moq_request_envelope_t *env)
{
    ns_update_admission_t a;
    moq_result_t rc = ns_update_admit(s, MOQ_REQUEST_FAMILY_ANNOUNCEMENT,
                                      s->announcements[slot].rs.ref, env, &a);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    if (!a.refuse_code) a.refuse_code = MOQ_REQUEST_ERROR_NOT_SUPPORTED;
    rc = ann_refuse_update(s, slot, &a, false);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) ns_update_admission_abort(s, &a);
    return rc;
}

moq_result_t ann_on_update_response(moq_session_t *s, int slot, bool ok,
    uint64_t error_code, const uint8_t *reason, size_t reason_len,
    bool has_expires, uint64_t expires_ms)
{
    moq_ann_entry_t *e = &s->announcements[slot];
    moq_ns_update_t *u = &e->update;
    moq_result_t rc = ns_update_push_result_event(s,
        MOQ_REQUEST_FAMILY_ANNOUNCEMENT, e->handle._opaque, u->out_request_id,
        ok ? MOQ_NAMESPACE_UPDATE_OUTCOME_OK : MOQ_NAMESPACE_UPDATE_OUTCOME_ERROR,
        MOQ_REQUEST_ROLE_REQUESTER,
        ok ? 0 : s->profile->semantic_request_error(error_code),
        ok ? NULL : reason, ok ? 0 : reason_len, has_expires, expires_ms);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
    if (!ok) u->failed = true;
    ns_update_clear_outstanding(s, u);
    return MOQ_OK;
}

moq_result_t moq_session_update_namespace(moq_session_t *s,
    moq_announcement_t ann, const moq_namespace_update_cfg_t *cfg,
    uint64_t now_us, uint64_t *out_update_request_id)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (!ann_updates_supported(s)) return MOQ_ERR_UNSUPPORTED;
    moq_namespace_update_encode_args_t args;
    moq_result_t rc = ns_update_cfg_read(s, cfg, MOQ_REQUEST_FAMILY_ANNOUNCEMENT,
                                         &args);
    if (rc < 0) return rc;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = ann_resolve_handle(s, ann);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_ann_entry_t *e = &s->announcements[slot];
    if (e->role != MOQ_ANN_ROLE_ANNOUNCER || e->state != MOQ_ANN_ESTABLISHED ||
        e->terminal_cause || e->update.failed || e->goaway_sent ||
        e->rs.lifetime.local_fin || e->rs.lifetime.local_reset)
        return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;
    return ns_update_send(s, e->rs.ref, &e->update, &args, out_update_request_id);
}

static moq_result_t ann_owed_slot(moq_session_t *s, moq_announcement_t ann,
                                  int *out_slot)
{
    int slot = ann_resolve_handle(s, ann);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->announcements[slot].role != MOQ_ANN_ROLE_RECEIVER ||
        s->announcements[slot].terminal_cause)
        return MOQ_ERR_WRONG_STATE;
    *out_slot = slot;
    return MOQ_OK;
}

moq_result_t moq_session_accept_namespace_update(moq_session_t *s,
    moq_announcement_t ann, const moq_accept_subscription_update_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (!ann_updates_supported(s)) return MOQ_ERR_UNSUPPORTED;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = -1;
    moq_result_t rc = ann_owed_slot(s, ann, &slot);
    if (rc < 0) return rc;
    moq_ann_entry_t *e = &s->announcements[slot];
    rc = ns_update_check_accept(s, &e->update, cfg);
    if (rc < 0) return rc;
    uint8_t buf[64];
    size_t len = 0;
    rc = ns_update_encode_ok(s, e->update.owed_request_id, buf, sizeof(buf), &len);
    if (rc < 0) return rc;
    rc = queue_send_bidi(s, e->rs.ref, buf, len, false);
    if (rc < 0) return rc;
    ns_update_clear_owed(s, &e->update);
    return MOQ_OK;
}

moq_result_t moq_session_reject_namespace_update(moq_session_t *s,
    moq_announcement_t ann, const moq_reject_subscription_update_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (!ann_updates_supported(s)) return MOQ_ERR_UNSUPPORTED;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = -1;
    moq_result_t rc = ann_owed_slot(s, ann, &slot);
    if (rc < 0) return rc;
    moq_ann_entry_t *e = &s->announcements[slot];
    rc = ns_update_check_reject(s, &e->update, cfg);
    if (rc < 0) return rc;
    uint8_t buf[1152];
    size_t len = 0;
    rc = ns_update_encode_error(s, e->update.owed_request_id, cfg->error_code,
        cfg->reason.data, cfg->reason.len, buf, sizeof(buf), &len);
    if (rc < 0) return rc;
    rc = queue_send_bidi(s, e->rs.ref, buf, len, true);
    if (rc < 0) return rc;
    ns_update_clear_owed(s, &e->update);
    (void)moq_request_lifetime_fin_committed(&e->rs.lifetime);
    e->state = MOQ_ANN_DRAINING;
    (void)ann_flush_terminal(s, slot);
    return MOQ_OK;
}

moq_result_t moq_session_namespace_owed_update(moq_session_t *s,
    moq_announcement_t ann, moq_namespace_update_record_t *out, size_t out_size)
{
    if (!s || !out || out_size < MOQ_NAMESPACE_UPDATE_RECORD_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (!ann_updates_supported(s)) return MOQ_ERR_UNSUPPORTED;
    int slot = -1;
    moq_result_t rc = ann_owed_slot(s, ann, &slot);
    if (rc < 0) return rc;
    return ns_update_owed_query(&s->announcements[slot].update, out, out_size);
}
