#include "session_internal.h"
#include "../internal/validate.h"

/* -- Fetch pool ---------------------------------------------------- */

int fetch_find_free(moq_session_t *s)
{
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_FREE) return (int)i;
    return -1;
}

static moq_fetch_t fetch_make_handle(moq_session_t *s, size_t slot)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
                                       s->session_tag,
                                       e->generation, (uint32_t)slot);
    moq_fetch_t h = { packed };
    return h;
}

int fetch_resolve_handle(moq_session_t *s, moq_fetch_t h)
{
    uint32_t pool = moq_handle_pool_tag(h._opaque);
    uint16_t tag  = moq_handle_session_tag(h._opaque);
    uint32_t slot = moq_handle_slot(h._opaque);
    uint32_t gen  = moq_handle_generation(h._opaque);
    if (pool != MOQ_HANDLE_POOL_FETCH) return -1;
    if (tag != s->session_tag) return -1;
    if (slot >= s->fetch_cap) return -1;
    if (s->fetches[slot].generation != gen) return -1;
    if (s->fetches[slot].state == MOQ_FETCH_FREE) return -1;
    return (int)slot;
}

void fetch_free_entry(moq_session_t *s, int slot)
{
    fetch_occ_unlink(s, (size_t)slot);
    moq_fetch_entry_t *e = &s->fetches[slot];
    if (e->mt_failing) {
        e->mt_failing = false;
        s->mt_failing_count--;
    }
    if (e->track_key) {
        s->alloc.free(e->track_key, e->track_key_len, s->alloc.ctx);
        e->track_key = NULL;
        e->track_key_len = 0;
    }
    /* Free any buffered Joining-FETCH staged token values the entry still owns
     * (a PENDING_JOIN freed without release; the memset below only drops the
     * descriptors, not the heap values). */
    if (e->join_token_count > 0)
        process_auth_tokens_free_staging(s, e->join_tokens,
            e->join_token_staged, e->join_token_count);
    request_registry_remove_by_id(s, e->request_id);
    /* Stream-correlated profiles key the request bidi by stream_ref; remove that
     * mapping so a recycled slot never carries a stale key. */
    if (e->rs.ref._v != 0)
        request_registry_remove_by_streamref(s, e->rs.ref);
    uint32_t next_gen = e->generation + 1;
    /* The request-bidi receive buffer is co-allocated in the session block;
     * preserve the pointer/cap across the reset (memset would null it). */
    uint8_t *recv_buf = e->rs.recv_buf;
    size_t   recv_cap = e->rs.recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_FETCH_FREE;
    e->generation = next_gen;
    e->occ_next = -1;      /* the memset zeroed these; -1 is "unlinked" */
    e->occ_prev = -1;
    e->rs.recv_buf = recv_buf;
    e->rs.recv_cap = recv_cap;
}

/* Called only after buffered response messages have been dispatched. Data FIN
 * and request FIN are independent; neither can discard a coalesced outcome. */
moq_result_t fetch_finish_response(moq_session_t *s, int slot)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    if (!s->profile->request_fin_is_not_cancel ||
        e->role != MOQ_FETCH_ROLE_FETCHER ||
        e->state != MOQ_FETCH_PENDING_FETCHER || !e->control_ok ||
        e->rs.recv_len != 0)
        return MOQ_OK;

    bool complete = e->data_stream_fin;
    if (e->rs.recv_fin) (void)moq_request_lifetime_peer_fin(&e->rs.lifetime);
    bool close_half = !e->rs.lifetime.local_fin && (complete || e->rs.recv_fin);
    bool need_drain = complete && !fetch_peer_fin_observed(e);
    if (close_half) e->rs.retained_output = MOQ_RETAINED_OUTPUT_FIN;
    if ((complete && event_queue_full(s)) ||
        (close_half && action_queue_full(s)) ||
        (need_drain && s->drain_ref_count >= s->drain_ref_cap))
        return MOQ_ERR_WOULD_BLOCK;

    if (close_half) {
        (void)queue_close_bidi(s, e->rs.ref); /* reserved above */
        (void)moq_request_lifetime_fin_committed(&e->rs.lifetime);
        e->rs.retained_output = MOQ_RETAINED_OUTPUT_NONE;
    }
    if (complete) {
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_FETCH_COMPLETE;
        ev.detail_size = (uint32_t)sizeof(moq_fetch_complete_event_t);
        ev.borrow_epoch = s->borrow_epoch;
        ev.u.fetch_complete.fetch = e->handle;
        (void)push_event(s, &ev); /* reserved above */
        if (need_drain) (void)drain_ref_add(s, e->rs.ref);
        fetch_free_entry(s, slot);
    }
    return MOQ_OK;
}

/* -- Fetcher-role completion --------------------------------------- *
 * Draft-18 §5.2: the fetcher keeps FETCH state until the data stream ends. Once
 * it also holds FETCH_OK the request stream has nothing left to carry in either
 * direction, so the fetcher FINs its half (the request was opened without FIN)
 * rather than leaving the stream, and its stream credit, open forever. The
 * publisher's half may still be open -- its FIN can trail the data stream on an
 * independent QUIC stream -- so a drain reference absorbs that FIN or RESET as
 * the ordinary end of the stream rather than as a new request. */

static bool fetch_completion_closes_request(const moq_fetch_entry_t *e)
{
    return e->rs.ref._v != 0;
}

bool fetch_fetcher_completion_fits(const moq_session_t *s, int slot,
                                   size_t extra_events)
{
    const moq_fetch_entry_t *e = &s->fetches[slot];
    bool close_half = fetch_completion_closes_request(e);
    bool need_drain = close_half && !fetch_peer_fin_observed(e);
    if (event_queue_avail(s) < 1 + extra_events) return false;
    if (close_half && action_queue_avail(s) < 1) return false;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap) return false;
    return true;
}

moq_result_t fetch_fetcher_completion_commit(moq_session_t *s, int slot)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    moq_stream_ref_t req_ref = e->rs.ref;
    bool close_half = fetch_completion_closes_request(e);
    bool need_drain = close_half && !fetch_peer_fin_observed(e);

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FETCH_COMPLETE;
    ev.detail_size = (uint32_t)sizeof(moq_fetch_complete_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.fetch_complete.fetch = e->handle;
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;
    if (close_half)
        (void)queue_close_bidi(s, req_ref);   /* reserved by _fits */
    if (need_drain)
        (void)drain_ref_add(s, req_ref);      /* reserved by _fits */
    fetch_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Locally-cancelled FETCH request-id grace cache ---------------- */

void fetch_cancel_tomb_add(moq_session_t *s, uint64_t request_id)
{
    if (s->fetch_cancel_tomb_cap == 0) return;
    /* De-dupe: a re-cancelled / repeated id stays a single entry. */
    for (size_t i = 0; i < s->fetch_cancel_tomb_count; i++)
        if (s->fetch_cancel_tombs[i] == request_id) return;
    if (s->fetch_cancel_tomb_count >= s->fetch_cancel_tomb_cap) {
        /* Full: drop the oldest entry. The cache is a grace window, so an
         * evicted id whose late traffic later arrives fails closed (the unknown-
         * request path closes the session) rather than reoccupying a slot. */
        memmove(&s->fetch_cancel_tombs[0], &s->fetch_cancel_tombs[1],
                (s->fetch_cancel_tomb_cap - 1) * sizeof(uint64_t));
        s->fetch_cancel_tomb_count = s->fetch_cancel_tomb_cap - 1;
    }
    s->fetch_cancel_tombs[s->fetch_cancel_tomb_count++] = request_id;
}

bool fetch_cancel_tomb_contains(const moq_session_t *s, uint64_t request_id)
{
    for (size_t i = 0; i < s->fetch_cancel_tomb_count; i++)
        if (s->fetch_cancel_tombs[i] == request_id) return true;
    return false;
}

bool fetch_cancel_tomb_consume(moq_session_t *s, uint64_t request_id)
{
    for (size_t i = 0; i < s->fetch_cancel_tomb_count; i++) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_tomb_probes++;
#endif
        if (s->fetch_cancel_tombs[i] == request_id) {
            /* Preserve FIFO order of the remaining entries so drop-oldest stays
             * meaningful. */
            memmove(&s->fetch_cancel_tombs[i], &s->fetch_cancel_tombs[i + 1],
                    (s->fetch_cancel_tomb_count - i - 1) * sizeof(uint64_t));
            s->fetch_cancel_tomb_count--;
            return true;
        }
    }
    return false;
}

void fetch_on_request_goaway_release(moq_session_t *s, int slot)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    /* Data uni already bound to an rx context: its remaining objects + FIN are
     * absorbed via the stale-handle path (session_receive.c), and no further
     * FETCH_HEADER lookup occurs, so the by-id key is no longer needed. */
    if (e->data_stream_started) {
        fetch_free_entry(s, slot);
        return;
    }
    /* The data uni has not presented its FETCH_HEADER yet. Keep the request-id
     * registry mapping (so a late header resolves to this slot and is absorbed),
     * drop only the request-bidi stream-ref mapping (that bidi is drained by the
     * caller), and invalidate the public handle. */
    if (e->rs.ref._v != 0) {
        request_registry_remove_by_streamref(s, e->rs.ref);
        e->rs.ref = moq_stream_ref_from_u64(0);
    }
    e->goaway_sent = false;   /* selective free: clear the migration marker (the
                               * slot is tombstoned, not reusable, until the full
                               * memset free, but keep the invariant uniform) */
    e->state = MOQ_FETCH_GOAWAY_LOCAL;
    e->generation++;
}

/* -- Inbound FETCH handler ----------------------------------------- */

/* -- Inbound REQUEST_UPDATE on a FETCH this side serves ----------------- */

/* A refused REQUEST_UPDATE ends a fetch whose FETCH_OK has gone out: "When a
 * REQUEST_UPDATE fails for a FETCH, the publisher MUST reset the FETCH data
 * stream" (draft-18 §10.9.1, draft-22 §9.5.1). The caller has queued the
 * REQUEST_ERROR with FIN as the last message on the request stream and
 * reserved one action (the reset, when the data stream is still open), one
 * event and, when `need_drain`, a drain slot for the fetcher's later FIN.
 * The serving application learns of the end from FETCH_CANCELLED; the handle
 * is retired, so no write can open a data stream without its FETCH_HEADER. */
static void fetch_end_on_refused_update(moq_session_t *s, int slot,
                                        bool reset_data, bool need_drain)
{
    moq_fetch_entry_t *fe = &s->fetches[slot];
    if (reset_data) {
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_RESET_DATA;
        a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.reset_data.stream_ref = fe->data_stream_ref;
        a.u.reset_data.error_code = 0x1;   /* CANCELLED */
        a.u.reset_data.reliable_size = fe->data_header_len;
        (void)push_action(s, &a);           /* slot reserved by the caller */
    }
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FETCH_CANCELLED;
    ev.detail_size = (uint32_t)sizeof(moq_fetch_cancelled_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.fetch_cancelled.fetch = fe->handle;
    (void)push_event(s, &ev);               /* slot reserved by the caller */
    if (need_drain)
        (void)drain_ref_add(s, fe->rs.ref); /* slot reserved by the caller */
    fetch_free_entry(s, slot);
}

moq_result_t session_core_on_fetch_request_update(
    moq_session_t *s, moq_decoded_request_update_t *d)
{
    moq_stream_ref_t resp_ref = d->endpoint.stream_ref;
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    moq_fetch_entry_t *fe = &s->fetches[d->target_slot];
    const bool refused = d->has_unsupported || d->auth_reject_code;
    const uint64_t err_code = d->auth_reject_code
        ? d->auth_reject_code : MOQ_REQUEST_ERROR_NOT_SUPPORTED;

    /* Not yet answered. FETCH_OK is the first message on the request stream
     * (draft-18 §10.13, draft-22 §9.12), so the answer waits for it and the
     * update is recorded: accept_fetch sends the answers right after
     * FETCH_OK, in arrival order; a refusal is answered there too and ends
     * the fetch. A refusal already owed covers every later update (one
     * REQUEST_ERROR may answer coalesced updates). Recorded answers count
     * against the advertised MAX_REQUEST_UPDATES. */
    if (fe->state != MOQ_FETCH_ACCEPTED) {
        if (s->profile->request_updates_max != 0 &&
            fe->upd_owed >= s->profile->request_updates_max) {
            process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
                                             d->token_count);
            process_auth_tokens_abort_txn(s, &d->auth_txn);
            return close_with_error(s, s->profile->request_updates_exceeded_error,
                "too many outstanding REQUEST_UPDATEs");
        }
        fe->upd_owed++;
        if (!fe->upd_failed) {
            if (refused) {
                fe->upd_failed = true;
                fe->upd_fail_code = err_code;
            } else {
                fe->upd_ok_owed++;
                /* The priority steers the data stream once it opens. */
                if (d->has_subscriber_priority)
                    fe->subscriber_priority = d->subscriber_priority;
            }
        }
        goto committed;
    }

    /* A refused update answers REQUEST_ERROR, the last message on the
     * request stream, and ends the fetch. */
    if (refused) {
        bool reset_data = fe->data_stream_started && !fe->data_stream_fin;
        bool need_drain = !fetch_peer_fin_observed(fe);
        if (event_queue_full(s) ||
            action_queue_avail(s) < (reset_data ? 2u : 1u) ||
            (need_drain && s->drain_ref_count >= s->drain_ref_cap)) {
            result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all;
        }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = err_code });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = queue_send_bidi(s, resp_ref, err_buf, moq_buf_writer_offset(&ew), true);
        if (rc < 0) { result = rc; goto cleanup_all; }
        fetch_end_on_refused_update(s, d->target_slot, reset_data, need_drain);
        goto committed;
    }

    /* REQUEST_OK, and the data stream's new subscriber priority. */
    const size_t prio_slots = session_owner_priority_slots(s,
        d->has_subscriber_priority, d->subscriber_priority,
        fe->subscriber_priority);
    if (action_queue_avail(s) < 1 + prio_slots) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }
    {
        uint8_t ok_buf[64];
        moq_buf_writer_t ow;
        moq_buf_writer_init(&ow, ok_buf, sizeof(ok_buf));
        moq_request_update_ok_encode_args_t oka = { .request_id = d->request_id };
        rc = s->profile->encode_request_update_ok(s, &ow, &oka);
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = queue_send_bidi(s, resp_ref, ok_buf, moq_buf_writer_offset(&ow), false);
        if (rc < 0) { result = rc; goto cleanup_all; }
    }
    if (prio_slots)   /* reserved above */
        (void)session_update_owner_priority(s, fe->handle._opaque,
                                            d->subscriber_priority);
    /* The priority steers this fetch's data-stream scheduling from here on. */
    if (d->has_subscriber_priority)
        fe->subscriber_priority = d->subscriber_priority;

committed:
    s->profile->commit_inbound_request(s, &d->endpoint);
    auth_committed = true;
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    result = MOQ_OK;

cleanup_all:
    process_auth_tokens_free_staging(s, d->tokens, d->token_staged, d->token_count);
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

/* Emit a core-generated FETCH REQUEST_ERROR, routed like the auth-reject path: on
 * the request bidi for stream-correlated profiles (draft-18, REQUEST_ERROR + FIN),
 * on the shared control channel otherwise (draft-16). The caller commits the
 * inbound request + auth transaction after this returns MOQ_OK. */
static moq_result_t fetch_auto_reject_ex(moq_session_t *s,
    const moq_decoded_fetch_t *d, const moq_request_error_encode_args_t *args)
{
    uint8_t err_buf[256];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_request_error_encode_args_t a = *args;
    if (d->endpoint.has_stream_ref) {
        /* REQUEST_ERROR + FIN is terminal and the request is never registered as a
         * fetch, so the fetcher's *later* FIN/RESET must be absorbed by the drain
         * ring rather than mistaken for a stray FIN on an unknown request (which
         * would close the session). When the request bidi already FIN'd in this
         * same chunk there is nothing left to absorb, so no drain ref is taken
         * (otherwise it would leak a slot forever). Reserve up front. */
        bool need_drain = !d->request_fin;
        if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
            return MOQ_ERR_WOULD_BLOCK;
        a.request_id = 0;
        moq_result_t rc = s->profile->encode_request_error(s, &ew, &a);
        if (rc < 0) return rc;
        rc = queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                             moq_buf_writer_offset(&ew), true);
        if (rc < 0) return rc;
        if (need_drain)
            (void)drain_ref_add(s, d->endpoint.stream_ref);   /* slot reserved above */
        return MOQ_OK;
    }
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    a.request_id = d->request_id;
    moq_result_t rc = s->profile->encode_request_error(s, &ew, &a);
    if (rc < 0) return rc;
    return queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
}

static moq_result_t fetch_auto_reject(moq_session_t *s,
                                      const moq_decoded_fetch_t *d,
                                      uint64_t error_code)
{
    return fetch_auto_reject_ex(s, d,
        &(moq_request_error_encode_args_t){ .error_code = error_code });
}

/* Upper bound on a bare REQUEST_ERROR (no reason / no redirect) queued on a
 * request bidi -- matches queue_request_error_bidi's own scratch/encode bound. */
#define PENDING_JOIN_REJECT_BOUND 48

/* Buffer a Joining FETCH whose associated subscription is still pending
 * (§10.12.2): create a PENDING_JOIN fetch entry holding the join parameters and
 * the resolved auth tokens, register the request bidi, and commit the inbound
 * request + auth now. No FETCH_REQUEST is surfaced until the subscription is
 * accepted (release) or rejected (reject). Sets *auth_committed so the caller's
 * cleanup neither double-commits nor frees the original staged token values.
 *
 * Token ownership: resolved token VALUES borrow either the auth staging heap
 * (token_staged) or the request receive buffer (USE_VALUE inline). The receive
 * buffer is reused by later inbound requests, so a borrowed value cannot survive
 * the wait for acceptance -- it is deep-copied into entry-owned heap storage here.
 * A staged value is already heap and is taken over (its staging flag cleared so the
 * caller's free_staging leaves it). Either way the entry owns every value (marked
 * join_token_staged) until release/reject/teardown frees it. */
static moq_result_t fetch_buffer_pending_join(moq_session_t *s,
                                              moq_decoded_fetch_t *d,
                                              bool *auth_committed)
{
    int slot = fetch_find_free(s);
    if (slot < 0) {
        /* A full fetch pool is this side's capacity, not a peer error: the
         * request is refused on its own stream, as a standalone FETCH is. */
        moq_request_error_encode_args_t full =
            session_capacity_request_error(s, 0, "fetch pool full", 15);
        moq_result_t rrc = fetch_auto_reject_ex(s, d, &full);
        if (rrc < 0) return rrc;
        s->profile->commit_inbound_request(s, &d->endpoint);
        *auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        return MOQ_OK;
    }

    /* Build entry-owned token storage in temporaries first, so a copy failure
     * leaves the fetch slot untouched (still FREE) and commits nothing: no
     * entry, no registry re-key, no request id, no event.
     *
     * That failure is MOQ_ERR_NOMEM -- a hard error, since no drain makes memory
     * appear -- and the request-stream handler frees the still-receiving staging
     * slot on any hard failure, so the request's buffered bytes go with it. An
     * empty re-feed therefore has nothing to resume; recovery is re-delivery of
     * the whole request, which admits cleanly because no request id was
     * committed. A caller driving the session through the transport bridge gets
     * a connection fatal instead, deliberately: only MOQ_ERR_WOULD_BLOCK is a
     * wait there. */
    size_t n = d->token_count;
    moq_resolved_token_t tmp[MOQ_DECODED_MAX_TOKENS];
    bool tmp_staged[MOQ_DECODED_MAX_TOKENS];
    bool took_from_d[MOQ_DECODED_MAX_TOKENS];
    for (size_t i = 0; i < n; i++) {
        tmp[i] = d->tokens[i];
        tmp_staged[i] = false;
        took_from_d[i] = false;
        if (d->tokens[i].token_value.len == 0) {
            tmp[i].token_value.data = NULL;
            continue;
        }
        if (d->token_staged[i]) {
            /* Already heap (auth staging owns it): take it over below. */
            tmp_staged[i] = true;
            took_from_d[i] = true;
        } else {
            /* Borrowed from the request receive buffer: copy it before the buffer
             * is reused by a later inbound request. */
            size_t len = d->tokens[i].token_value.len;
            uint8_t *copy = (uint8_t *)s->alloc.alloc(len, s->alloc.ctx);
            if (!copy) {
                for (size_t j = 0; j < i; j++)
                    if (tmp_staged[j] && !took_from_d[j])
                        s->alloc.free((void *)(uintptr_t)tmp[j].token_value.data,
                                      tmp[j].token_value.len, s->alloc.ctx);
                /* Every copy taken so far is released and no entry, registry key
                 * or event exists yet -- but no drain makes memory appear, so
                 * this is an error rather than a wait. */
                return MOQ_ERR_NOMEM;
            }
            memcpy(copy, d->tokens[i].token_value.data, len);
            tmp[i].token_value.data = copy;
            tmp_staged[i] = true;
        }
    }

    moq_fetch_entry_t *e = &s->fetches[slot];
    uint32_t live_gen = e->generation | 1;
    e->generation = live_gen;
    /* Allocated: join the occupancy list the preamble scans walk. */
    fetch_occ_link(s, (size_t)slot);
    e->state = MOQ_FETCH_PENDING_JOIN;
    e->role = MOQ_FETCH_ROLE_PUBLISHER;
    e->handle = (moq_fetch_t){ moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
        s->session_tag, live_gen, (uint32_t)slot) };
    e->request_id = d->request_id;
    e->rs.ref = d->endpoint.stream_ref;
    /* The peer's FIN is NOT copied here: the generic destination handoff
     * installs it as transitional ownership on this entry and drives the
     * family's own FIN handling, exactly as it does for a standalone FETCH. */
    e->join_fetch_type = (uint8_t)d->fetch_type;
    e->join_request_id = d->joining_request_id;
    e->join_start = d->joining_start;
    e->join_subscriber_priority = d->subscriber_priority;
    e->subscriber_priority = d->subscriber_priority;
    e->join_group_order = (moq_group_order_t)d->group_order;
    e->effective_group_order = d->group_order == MOQ_GROUP_ORDER_DEFAULT
        ? MOQ_GROUP_ORDER_ASCENDING : (moq_group_order_t)d->group_order;
    for (size_t i = 0; i < n; i++) {
        e->join_tokens[i] = tmp[i];
        e->join_token_staged[i] = tmp_staged[i];
        if (took_from_d[i])
            d->token_staged[i] = false;   /* ownership moved to the entry */
    }
    e->join_token_count = n;
    /* Hand the request bidi off from staging to this fetch slot. */
    d->endpoint.kind = MOQ_REQ_FETCH;
    d->endpoint.slot = slot;
    request_registry_remove_by_streamref(s, d->endpoint.stream_ref);
    request_registry_insert_by_streamref(s, d->endpoint.stream_ref, d->endpoint);
    s->profile->commit_inbound_request(s, &d->endpoint);
    *auth_committed = true;
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    return MOQ_OK;
}

/* Terminally reject a buffered Joining FETCH: REQUEST_ERROR(error_code) + FIN on
 * its request bidi, drain it (absorb the fetcher's later FIN unless already seen),
 * then free the entry (which frees the held token values). Reserve-before-mutate:
 * on MOQ_ERR_WOULD_BLOCK / MOQ_ERR_BUFFER nothing is sent or freed and the entry
 * stays PENDING_JOIN for a retry. */
static moq_result_t fetch_reject_pending_join(moq_session_t *s, int slot,
                                              uint64_t error_code)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    moq_stream_ref_t ref = e->rs.ref;
    bool need_drain = ref._v != 0 && !fetch_peer_fin_observed(e);
    /* Reserve the drain slot before the send: the fetcher's later FIN/RESET on the
     * (soon-freed) join bidi must be absorbed, never read as a stray FIN. */
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    if (ref._v != 0) {
        moq_request_error_encode_args_t err_args = { .error_code = error_code };
        moq_result_t rc = queue_request_error_bidi(s, ref, &err_args);
        if (rc < 0) return rc;   /* nothing mutated (reserve-before-mutate) */
    }
    if (need_drain)
        (void)drain_ref_add(s, ref);   /* slot reserved above */
    fetch_free_entry(s, slot);
    return MOQ_OK;
}

/* Scratch bytes a single releasing join consumes (its FETCH_REQUEST token array +
 * value copies). The array alloc is alignment-padded; values are byte-packed.
 * Releases accumulate (each event keeps its scratch until polled). */

static size_t pending_join_release_scratch(const moq_fetch_entry_t *e)
{
    if (e->join_token_count == 0) return 0;
    size_t n = _Alignof(moq_resolved_token_t)
             + e->join_token_count * sizeof(moq_resolved_token_t);
    for (size_t t = 0; t < e->join_token_count; t++)
        n += e->join_tokens[t].token_value.len;
    return n;
}

/*
 * The End Location of a Joining Fetch, from a Joining Location object.
 *
 * draft-16 9.16.1 / draft-18 10.12.1 define a FETCH End Location as "the end
 * Location, plus 1", and state that an End Location.Object of 0 means the
 * ENTIRE group is requested. So when the Joining Location object sits at the
 * negotiated profile's Location ceiling, the exact representable End is
 * {same group, 0} -- which covers the whole end group and therefore still
 * includes the Joining Location itself, exactly as 9.16.2.1 / 10.12.2.1
 * require ("the last Object included ... is the Object at the Joining
 * Location").
 *
 * Deliberately NOT moq_loc_successor(): that helper carries to
 * {group + 1, 0} and exists for subscription filter START windows. A FETCH End
 * uses the same-group zero sentinel, so the end GROUP must not move.
 *
 * The arithmetic is checked in BOTH profiles rather than relying on unsigned
 * wrap. On draft-18 the ceiling is UINT64_MAX and wrap happens to land on the
 * same value; on draft-16 the ceiling is 2^62-1, where +1 is outside the
 * profile's encoding altogether. One rule, no accidental correctness.
 *
 * All three Joining-Fetch end computations route through here so they cannot
 * drift apart.
 */
static uint64_t fetch_joining_end_object(const moq_session_t *s, uint64_t object)
{
    return object == s->profile->location_varint_max ? 0 : object + 1;
}

moq_result_t session_core_pending_joins_can_resolve(moq_session_t *s, uint64_t sub_req_id,
    bool has_largest, uint64_t largest_group,
    size_t extra_actions, size_t extra_drains, size_t extra_send)
{
    size_t n_event = 0, n_action = 0, n_drain = 0, send = 0, rel_scratch = 0;
    bool any_reject = false;
    /* Only an allocated fetch can be a buffered join, and only allocated slots
     * are linked, so the preflight walks the live fetches, not `fetch_cap`. */
    for (int32_t i = s->fetch_occ_head; i >= 0; i = s->fetches[i].occ_next) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_join_resolve_probes++;
#endif
        moq_fetch_entry_t *e = &s->fetches[i];
        if (e->state != MOQ_FETCH_PENDING_JOIN || e->join_request_id != sub_req_id)
            continue;
        /* Same outcome split as session_core_release_pending_joins. */
        bool reject = !has_largest ||
            (e->join_fetch_type == 3 && e->join_start > largest_group);
        if (reject) {
            any_reject = true;
            n_action++;
            send += PENDING_JOIN_REJECT_BOUND;
            if (e->rs.ref._v != 0 &&
                !fetch_peer_fin_observed(e)) n_drain++;
        } else {
            n_event++;
            rel_scratch += pending_join_release_scratch(e);   /* scratch-copied tokens */
        }
    }
    size_t need_actions = n_action + extra_actions;
    size_t need_drains  = n_drain + extra_drains;
    size_t need_send    = send + extra_send;
    /* A reject encodes its REQUEST_ERROR through scratch transiently (restored each
     * time), so it needs one bound on top of the accumulated release copies. */
    size_t need_scratch = rel_scratch + (any_reject ? PENDING_JOIN_REJECT_BOUND : 0);

    /* Permanent shortfalls (retry can never make progress) => BUFFER. */
    if (n_event > s->event_cap) return MOQ_ERR_BUFFER;
    if (need_actions > s->action_cap) return MOQ_ERR_BUFFER;
    if (need_drains > s->drain_ref_cap) return MOQ_ERR_BUFFER;
    if (need_send > s->send_cap) return MOQ_ERR_BUFFER;
    if (need_scratch > s->event_scratch_cap) return MOQ_ERR_BUFFER;

    /* Transient shortfalls (retry after the app drains) => WOULD_BLOCK. */
    size_t drain_avail = s->drain_ref_cap > s->drain_ref_count
        ? s->drain_ref_cap - s->drain_ref_count : 0;
    if (event_queue_avail(s) < n_event) return MOQ_ERR_WOULD_BLOCK;
    if (action_queue_avail(s) < need_actions) return MOQ_ERR_WOULD_BLOCK;
    if (drain_avail < need_drains) return MOQ_ERR_WOULD_BLOCK;
    if (s->send_cap - s->send_len < need_send) return MOQ_ERR_WOULD_BLOCK;
    if (s->event_scratch_cap - s->event_scratch_len < need_scratch)
        return MOQ_ERR_WOULD_BLOCK;
    return MOQ_OK;
}

moq_result_t session_core_release_pending_joins(moq_session_t *s, int sub_slot)
{
    moq_sub_entry_t *sub = &s->subs[sub_slot];
    /* Same occupancy list as the preflight and the reject; the successor is
     * captured before a release or reject can free the entry. */
    for (int32_t i = s->fetch_occ_head, nxt; i >= 0; i = nxt) {
        nxt = s->fetches[i].occ_next;
        moq_fetch_entry_t *e = &s->fetches[i];
        if (e->state != MOQ_FETCH_PENDING_JOIN ||
            e->join_request_id != sub->request_id)
            continue;

        if (!sub->has_largest) {
            moq_result_t rc = fetch_reject_pending_join(s, (int)i,
                MOQ_REQUEST_ERROR_INVALID_RANGE);
            if (rc < 0) return rc;   /* defensive: preflight reserved capacity */
            continue;
        }
        uint64_t end_group = sub->largest_group;
        uint64_t end_object = fetch_joining_end_object(s, sub->largest_object);
        uint64_t start_group, start_object = 0;
        if (e->join_fetch_type == 2) {
            start_group = e->join_start > sub->largest_group
                ? 0 : sub->largest_group - e->join_start;
        } else {
            if (e->join_start > sub->largest_group) {
                moq_result_t rc = fetch_reject_pending_join(s, (int)i,
                    MOQ_REQUEST_ERROR_INVALID_RANGE);
                if (rc < 0) return rc;
                continue;
            }
            start_group = e->join_start;
        }
        /* Copy the held resolved tokens into output scratch for the event: per the
         * public contract (moq/session.h) token values borrow output scratch, valid
         * only until the next poll -- NOT entry storage, which a peer RESET could
         * free (fetch_free_entry) before the app polls this FETCH_REQUEST. Capacity
         * was reserved by the preflight. The entry-owned heap values are freed only
         * after push_event succeeds, so a failure leaves the join retryable. */
        size_t scratch_saved = s->event_scratch_len;
        moq_resolved_token_t *ev_tokens = NULL;
        if (e->join_token_count > 0) {
            ev_tokens = (moq_resolved_token_t *)event_scratch_alloc_aligned(s,
                e->join_token_count * sizeof(moq_resolved_token_t),
                _Alignof(moq_resolved_token_t));
            if (!ev_tokens) { s->event_scratch_len = scratch_saved; return MOQ_ERR_BUFFER; }
            for (size_t t = 0; t < e->join_token_count; t++) {
                ev_tokens[t].token_type = e->join_tokens[t].token_type;
                size_t vlen = e->join_tokens[t].token_value.len;
                if (vlen > 0) {
                    uint8_t *copy = event_scratch_copy(s, e->join_tokens[t].token_value.data, vlen);
                    if (!copy) { s->event_scratch_len = scratch_saved; return MOQ_ERR_BUFFER; }
                    ev_tokens[t].token_value.data = copy;
                    ev_tokens[t].token_value.len = vlen;
                } else {
                    ev_tokens[t].token_value.data = NULL;
                    ev_tokens[t].token_value.len = 0;
                }
            }
        }
        moq_event_t ev;
        memset(&ev, 0, sizeof(ev));
        ev.kind = MOQ_EVENT_FETCH_REQUEST;
        ev.detail_size = (uint32_t)sizeof(moq_fetch_request_event_t);
        ev.borrow_epoch = s->borrow_epoch;
        ev.u.fetch_request.fetch = e->handle;
        ev.u.fetch_request.joining_sub = sub->handle;
        ev.u.fetch_request.start_group = start_group;
        ev.u.fetch_request.start_object = start_object;
        ev.u.fetch_request.end_group = end_group;
        ev.u.fetch_request.end_object = end_object;
        ev.u.fetch_request.subscriber_priority = e->join_subscriber_priority;
        ev.u.fetch_request.group_order = e->join_group_order;
        ev.u.fetch_request.tokens = ev_tokens;
        ev.u.fetch_request.token_count = ev_tokens ? e->join_token_count : 0;
        moq_result_t rc = push_event(s, &ev);   /* slot reserved by preflight */
        if (rc < 0) { s->event_scratch_len = scratch_saved; return rc; }
        /* Event staged with scratch copies: the entry-owned heap values are now
         * redundant; free them (so fetch_free_entry doesn't, avoiding a stale-event
         * dangling pointer) and stop counting them. Scratch is NOT restored -- the
         * event borrows it until polled. */
        if (e->join_token_count > 0)
            process_auth_tokens_free_staging(s, e->join_tokens,
                e->join_token_staged, e->join_token_count);
        e->join_token_count = 0;
        e->state = MOQ_FETCH_PENDING_PUBLISHER;
    }
    return MOQ_OK;
}

moq_result_t session_core_reject_pending_joins(moq_session_t *s, uint64_t sub_req_id)
{
    /* Same list; the successor is captured before the reject frees the entry. */
    for (int32_t i = s->fetch_occ_head, nxt; i >= 0; i = nxt) {
        nxt = s->fetches[i].occ_next;
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_join_reject_probes++;
#endif
        if (s->fetches[i].state == MOQ_FETCH_PENDING_JOIN &&
            s->fetches[i].join_request_id == sub_req_id) {
            moq_result_t rc = fetch_reject_pending_join(s, (int)i,
                MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
            if (rc < 0) return rc;   /* defensive: preflight reserved capacity */
        }
    }
    return MOQ_OK;
}

void session_core_discard_pending_joins(moq_session_t *s, uint64_t sub_req_id)
{
    /* Same list; the successor is captured before the free that unlinks. */
    for (int32_t i = s->fetch_occ_head, nxt; i >= 0; i = nxt) {
        nxt = s->fetches[i].occ_next;
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_join_probes++;
#endif
        if (s->fetches[i].state == MOQ_FETCH_PENDING_JOIN &&
            s->fetches[i].join_request_id == sub_req_id)
            fetch_free_entry(s, (int)i);   /* frees the entry-owned token storage */
    }
}

moq_result_t session_core_on_fetch(moq_session_t *s,
                                    moq_decoded_fetch_t *d)
{
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    size_t scratch_saved = s->event_scratch_len;

    /* A message-level authorization-token reject fails the fetch with
     * REQUEST_ERROR and surfaces no event; a REGISTER in the same message still
     * commits its alias (§10.2.2). The error rides the request bidi for
     * stream-correlated profiles (never the control channel). */
    if (d->auth_reject_code) {
        /* Route through the shared helper so the stream-correlated path drains the
         * request bidi (absorbing the fetcher's later FIN/RESET) exactly like the
         * joining-reject paths. */
        rc = fetch_auto_reject(s, d, d->auth_reject_code);
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    /* Under moq_session_cfg_t.defer_fetch_ranges a standalone range stated
     * as a Location Filter is the application's to resolve: it is surfaced
     * as sent and checked against the accept's End instead. */
    const bool range_deferred = s->defer_fetch_ranges && d->has_options &&
                                d->fetch_type == 1;

    /* A range stated relative to the Largest Object resolves against this
     * side's track history. No objects, or a start past the Largest Object,
     * cannot be served: INVALID_RANGE on the request, not a session error. */
    if (!range_deferred &&
        (d->start_relative || d->start_next_object || d->end_at_largest)) {
        const moq_track_hist_t *h =
            track_hist_find_id(s, &d->track_namespace, d->track_name);
        /* Next Object is past Largest even when Object + 1 cannot fit. */
        bool servable = !d->start_next_object && h && h->has_largest;
        if (servable) {
            const uint64_t lg = h->largest_group, lo = h->largest_object;
            if (d->start_relative) {
                /* `relative_offset` groups before the next group, clamped at
                 * the origin and at the ceiling. */
                if (d->relative_offset == 0)
                    d->start_group = lg == UINT64_MAX ? UINT64_MAX : lg + 1;
                else
                    d->start_group = d->relative_offset - 1 > lg
                                   ? 0 : lg - (d->relative_offset - 1);
                d->start_object = 0;
            }
            if (d->end_at_largest) {
                /* The core's end object is exclusive, zero meaning the whole
                 * end group. */
                d->end_group = lg;
                d->end_object = lo == UINT64_MAX ? 0 : lo + 1;
            }
            servable = d->start_group < lg ||
                       (d->start_group == lg && d->start_object <= lo);
        }
        if (!servable) {
            rc = fetch_auto_reject(s, d, MOQ_REQUEST_ERROR_INVALID_RANGE);
            if (rc < 0) { result = rc; goto cleanup_all; }
            s->profile->commit_inbound_request(s, &d->endpoint);
            auth_committed = true;
            process_auth_tokens_commit_txn(s, &d->auth_txn);
            result = MOQ_OK;
            goto cleanup_all;
        }
    }

    /* Joining fetch: resolve referenced subscription and calculate range. */
    if (d->fetch_type == 2 || d->fetch_type == 3) {
        int jsub = sub_find_by_request_id(s, d->joining_request_id);
        if (jsub < 0) {
            /* Stream-correlated profiles (draft-18) index responder subscriptions
             * by stream ref, not request id (responses correlate by stream). A
             * Joining Fetch references the subscription by its Request ID, so
             * resolve it with a pool scan over the publisher-role entries. */
            for (size_t i = 0; i < s->sub_cap; i++) {
                /* Only an eligible (established / pending-subscriber) entry counts:
                 * a staging RECVING slot is also publisher-role and starts with
                 * request_id 0, so it must not shadow the real subscription. */
                if (s->subs[i].role == MOQ_SUB_ROLE_PUBLISHER &&
                    s->subs[i].request_id == d->joining_request_id &&
                    (s->subs[i].state == MOQ_SUB_ESTABLISHED ||
                     s->subs[i].state == MOQ_SUB_PENDING_PUBLISHER)) {
                    jsub = (int)i;
                    break;
                }
            }
        }
        /*
         * No subscription matched. A Joining Request ID may equally name a
         * PUBLISH-INITIATED subscription (draft-18 5.1: "A publisher initiates
         * a subscription to a track by sending the PUBLISH message"), which
         * lives in the publication pool. Resolve it there before rejecting.
         *
         * The scan is unambiguous by construction: only PUBLISHER-role entries
         * are considered (this endpoint sent the PUBLISH, so it is the one that
         * would serve the fetch), and a request id is unique per pool, so at
         * most one entry can match.
         */
        int jpub = -1;
        if (jsub < 0) {
            for (size_t i = 0; i < s->pub_cap; i++) {
                const moq_pub_entry_t *pe = &s->publishes[i];
                if (pe->state == MOQ_PUB_FREE) continue;
                if (pe->role != MOQ_PUB_ROLE_PUBLISHER) continue;
                if (pe->request_id != d->joining_request_id) continue;
                jpub = (int)i;
                break;
            }
        }

        if (jpub >= 0) {
            const moq_pub_entry_t *pe = &s->publishes[jpub];
            /* 10.12.2's state list is "Established or Pending (subscriber)":
             * Pending (Publisher) is deliberately absent, so a join before
             * PUBLISH_OK is INVALID_JOINING_REQUEST_ID and is NOT buffered. */
            if (pe->state != MOQ_PUB_ESTABLISHED || pe->done_pending) {
                rc = fetch_auto_reject(s, d,
                    MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
                if (rc < 0) { result = rc; goto cleanup_all; }
                s->profile->commit_inbound_request(s, &d->endpoint);
                auth_committed = true;
                process_auth_tokens_commit_txn(s, &d->auth_txn);
                result = MOQ_OK;
                goto cleanup_all;
            }
            if (!pub_joining_eligible(s, pe)) {
                /* draft-18 10.12.2: Forward State 0 -> INVALID_RANGE.
                 * draft-16 9.16.2: a non-Largest-Object filter is a protocol
                 * violation, not a request error. */
                if (!moq_session_uses_request_streams(s)) {
                    result = close_with_error(s, 0x3,
                        "joining fetch requires LARGEST_OBJECT filter");
                    goto cleanup_all;
                }
                rc = fetch_auto_reject(s, d, MOQ_REQUEST_ERROR_INVALID_RANGE);
                if (rc < 0) { result = rc; goto cleanup_all; }
                s->profile->commit_inbound_request(s, &d->endpoint);
                auth_committed = true;
                process_auth_tokens_commit_txn(s, &d->auth_txn);
                result = MOQ_OK;
                goto cleanup_all;
            }
            /* The LATCHED Joining Location -- never a live history read. */
            if (!pe->has_joining_loc) {
                rc = fetch_auto_reject(s, d, MOQ_REQUEST_ERROR_INVALID_RANGE);
                if (rc < 0) { result = rc; goto cleanup_all; }
                s->profile->commit_inbound_request(s, &d->endpoint);
                auth_committed = true;
                process_auth_tokens_commit_txn(s, &d->auth_txn);
                result = MOQ_OK;
                goto cleanup_all;
            }
            /* 10.12.2.1: End is {Joining Location.Group,
             * Joining Location.Object + 1}; a Relative start is
             * {Joining Location.Group - Joining Start, 0}. */
            d->start_object = 0;
            d->end_group = pe->joining_group;
            d->end_object = fetch_joining_end_object(s, pe->joining_object);
            if (d->fetch_type == 2) {
                d->start_group = (d->joining_start > pe->joining_group)
                                     ? 0
                                     : pe->joining_group - d->joining_start;
            } else {
                /* Absolute: the Start comes from the REQUEST while the End
                 * comes from the Joining Location, so the two can disagree --
                 * unlike Relative, which derives both from the location and
                 * clamps at 0. 10.13 requires End >= Start, and a Start past
                 * the Largest is INVALID_RANGE. Same rule the SUBSCRIBE-origin
                 * branch below applies. */
                d->start_group = d->joining_start;
                if (d->start_group > pe->joining_group) {
                    rc = fetch_auto_reject(s, d,
                        MOQ_REQUEST_ERROR_INVALID_RANGE);
                    if (rc < 0) { result = rc; goto cleanup_all; }
                    s->profile->commit_inbound_request(s, &d->endpoint);
                    auth_committed = true;
                    process_auth_tokens_commit_txn(s, &d->auth_txn);
                    result = MOQ_OK;
                    goto cleanup_all;
                }
            }
            d->joining_pub_slot = jpub;
            d->joining_sub_slot = -1;
            goto joining_resolved;
        }

        /* A subscription the peer has already cancelled is gone for joining
         * purposes even while its teardown is still owed. */
        if (jsub < 0 ||
            (s->subs[jsub].state != MOQ_SUB_ESTABLISHED &&
             s->subs[jsub].state != MOQ_SUB_PENDING_PUBLISHER) ||
            s->subs[jsub].cancel_pending) {
            rc = fetch_auto_reject(s, d,
                MOQ_REQUEST_ERROR_INVALID_JOINING_REQUEST_ID);
            if (rc < 0) { result = rc; goto cleanup_all; }
            s->profile->commit_inbound_request(s, &d->endpoint);
            auth_committed = true;
            process_auth_tokens_commit_txn(s, &d->auth_txn);
            result = MOQ_OK;
            goto cleanup_all;
        }
        moq_sub_entry_t *jsub_e = &s->subs[jsub];
        if (jsub_e->filter_type != MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT) {
            result = close_with_error(s, 0x3,
                "joining fetch requires LARGEST_OBJECT filter");
            goto cleanup_all;
        }
        /* §10.12.2: a Joining Fetch is only permitted when the associated
         * subscription has Forward State 1, regardless of its state; otherwise
         * REQUEST_ERROR INVALID_RANGE. */
        if (!jsub_e->forward) {
            rc = fetch_auto_reject(s, d, MOQ_REQUEST_ERROR_INVALID_RANGE);
            if (rc < 0) { result = rc; goto cleanup_all; }
            s->profile->commit_inbound_request(s, &d->endpoint);
            auth_committed = true;
            process_auth_tokens_commit_txn(s, &d->auth_txn);
            result = MOQ_OK;
            goto cleanup_all;
        }
        /* §10.12.2 (stream-correlated profiles only): if the associated subscription
         * is not yet established, buffer the Joining FETCH on its request bidi until
         * the subscription is accepted (release) or rejected/torn down. No
         * FETCH_REQUEST is surfaced yet. Buffering is a request-stream mechanism
         * (the held bidi receives the eventual response), so draft-16 -- which has
         * no request bidi to defer on -- keeps the eager INVALID_RANGE reject below
         * for a pending subscription, leaving its joining behaviour unchanged. */
        if (d->endpoint.has_stream_ref &&
            jsub_e->state == MOQ_SUB_PENDING_PUBLISHER) {
            result = fetch_buffer_pending_join(s, d, &auth_committed);
            goto cleanup_all;
        }
        /* Established: a stored Largest is required (objects have been published),
         * else INVALID_RANGE. */
        if (!jsub_e->has_largest) {
            rc = fetch_auto_reject(s, d, MOQ_REQUEST_ERROR_INVALID_RANGE);
            if (rc < 0) { result = rc; goto cleanup_all; }
            s->profile->commit_inbound_request(s, &d->endpoint);
            auth_committed = true;
            process_auth_tokens_commit_txn(s, &d->auth_txn);
            result = MOQ_OK;
            goto cleanup_all;
        }
        d->end_group = jsub_e->largest_group;
        d->end_object = fetch_joining_end_object(s, jsub_e->largest_object);
        if (d->fetch_type == 2) {
            if (d->joining_start > jsub_e->largest_group)
                d->start_group = 0;
            else
                d->start_group = jsub_e->largest_group - d->joining_start;
            d->start_object = 0;
        } else {
            d->start_group = d->joining_start;
            d->start_object = 0;
            if (d->start_group > jsub_e->largest_group) {
                rc = fetch_auto_reject(s, d, MOQ_REQUEST_ERROR_INVALID_RANGE);
                if (rc < 0) { result = rc; goto cleanup_all; }
                s->profile->commit_inbound_request(s, &d->endpoint);
                auth_committed = true;
                process_auth_tokens_commit_txn(s, &d->auth_txn);
                result = MOQ_OK;
                goto cleanup_all;
            }
        }
        d->joining_sub_slot = jsub;
        d->joining_pub_slot = -1;
joining_resolved: ;
    }

    if (event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    int slot = fetch_find_free(s);
    if (slot < 0) {
        if (d->endpoint.has_stream_ref) {
            /* No free fetch slot is this side's load, not a peer error: on a
             * request stream the FETCH is refused before any application
             * processing with REQUEST_ERROR and a FIN (draft-18 §3.3.2,
             * draft-22 §6.4.2.3), carrying the profile's capacity code. */
            moq_request_error_encode_args_t full =
                session_capacity_request_error(s, 0, "fetch pool full", 15);
            rc = fetch_auto_reject_ex(s, d, &full);
            if (rc < 0) { result = rc; goto cleanup_all; }
            s->profile->commit_inbound_request(s, &d->endpoint);
            auth_committed = true;
            process_auth_tokens_commit_txn(s, &d->auth_txn);
            result = MOQ_OK;
            goto cleanup_all;
        }
        if (action_queue_full(s)) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        moq_request_error_encode_args_t full = session_capacity_request_error(
            s, d->request_id, "fetch pool full", 15);
        rc = s->profile->encode_request_error(s, &ew, &full);
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    /* Copy namespace + track name into scratch. */
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

    /* Copy resolved tokens into scratch. */
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

    /* The request's options, borrowed from scratch with the event. */
    moq_fetch_request_options_t *ev_opts = NULL;
    if (d->has_options) {
        ev_opts = (moq_fetch_request_options_t *)event_scratch_alloc_aligned(
            s, sizeof(*ev_opts), _Alignof(moq_fetch_request_options_t));
        if (!ev_opts) {
            s->event_scratch_len = scratch_saved;
            if (scratch_saved == 0) {
                result = close_with_error(s, 0x1,
                    "event scratch permanently too small");
                goto cleanup_all;
            }
            result = MOQ_ERR_BUFFER;
            goto cleanup_all;
        }
        memset(ev_opts, 0, sizeof(*ev_opts));
        ev_opts->struct_size = (uint32_t)sizeof(*ev_opts);
        ev_opts->range_unresolved = range_deferred;
        ev_opts->flags = (d->has_fill_timeout ? MOQ_FETCH_REQUEST_HAS_FILL_TIMEOUT : 0u) |
                        (d->omit_track_properties ? 0u : MOQ_FETCH_REQUEST_INCLUDE_PROPERTIES);
        ev_opts->fill_timeout_ms = d->has_fill_timeout ? d->fill_timeout_ms : 0;
        moq_location_filter_cfg_init(&ev_opts->location);
        if (d->has_location) {
            moq_location_filter_from_core(&(moq_core_filter_t){
                .type = d->location_type,
                .start_group = d->location_start_group, .start_object = d->location_start_object,
                .end_group = d->location_end_group, .end_object = d->location_end_object },
                &ev_opts->location, sizeof(ev_opts->location));
        }
    }

    /* Build handle WITHOUT mutating entry. */
    moq_fetch_entry_t *entry = &s->fetches[slot];
    uint32_t live_gen = entry->generation | 1;
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
                                       s->session_tag, live_gen,
                                       (uint32_t)slot);
    moq_fetch_t handle = { packed };

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_FETCH_REQUEST;
    e.detail_size = (uint32_t)sizeof(moq_fetch_request_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.fetch_request.fetch = handle;
    e.u.fetch_request.track_namespace = ev_ns;
    e.u.fetch_request.track_name = ev_name;
    if (d->joining_sub_slot >= 0)
        e.u.fetch_request.joining_sub = s->subs[d->joining_sub_slot].handle;
    if (d->joining_pub_slot >= 0)
        e.u.fetch_request.joining_pub = s->publishes[d->joining_pub_slot].handle;
    e.u.fetch_request.start_group = d->start_group;
    e.u.fetch_request.start_object = d->start_object;
    e.u.fetch_request.end_group = d->end_group;
    e.u.fetch_request.end_object = d->end_object;
    e.u.fetch_request.subscriber_priority = d->subscriber_priority;
    e.u.fetch_request.group_order = d->group_order;
    e.u.fetch_request.tokens = ev_tokens;
    e.u.fetch_request.token_count = d->token_count;
    e.u.fetch_request.options = ev_opts;

    rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        result = rc;
        goto cleanup_all;
    }

    /* Commit: now safe to mutate entry. */
    entry->generation = live_gen;
    /* Allocated: join the occupancy list the preamble scans walk. */
    fetch_occ_link(s, (size_t)slot);
    entry->state = MOQ_FETCH_PENDING_PUBLISHER;
    entry->subscriber_priority = d->subscriber_priority;
    entry->omit_track_properties = d->omit_track_properties;
    entry->role = MOQ_FETCH_ROLE_PUBLISHER;
    entry->effective_group_order = d->group_order == MOQ_GROUP_ORDER_DEFAULT
        ? MOQ_GROUP_ORDER_ASCENDING : (moq_group_order_t)d->group_order;
    if (range_deferred) {
        /* What the accept's End is checked against. */
        entry->range_deferred = true;
        entry->req_start_relative = d->start_relative;
        entry->req_start_next_object = d->start_next_object;
        entry->req_end_at_largest = d->end_at_largest;
        entry->req_relative_offset = d->relative_offset;
        entry->start_group = d->start_group;
        entry->start_object = d->start_object;
        entry->req_end_group = d->end_group;
        entry->req_end_object = d->end_object;
    }
    entry->handle = handle;
    entry->request_id = d->request_id;
    d->endpoint.kind = MOQ_REQ_FETCH;
    d->endpoint.slot = slot;
    if (d->endpoint.has_stream_ref) {
        /* Hand the request bidi off from the staging slot to this fetch slot:
         * remove the staging stream-ref mapping, then bind it to (FETCH, slot).
         * The request-stream handler frees the staging slot afterwards (after
         * clearing its rs.ref, so this mapping is not removed). */
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
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

moq_result_t moq_session_set_defer_fetch_ranges(moq_session_t *s, bool defer)
{
    if (!s) return MOQ_ERR_INVAL;
    s->defer_fetch_ranges = defer;
    return MOQ_OK;
}

/* -- Inbound FETCH error (REQUEST_ERROR for fetch) ----------------- */

moq_result_t session_core_on_fetch_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len, bool free_now,
    const moq_decoded_redirect_t *redirect)
{
    if (s->fetches[slot].control_response_seen)
        return close_with_error(s, 0x3, "duplicate control response for fetch");
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    /* Stream-correlated profiles close the fetcher's send half after the terminal
     * error so the peer can retire the request bidi; reserve the action up front. */
    bool close_half = !free_now && s->fetches[slot].rs.ref._v != 0;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_result_t rc;
    if (redirect) {
        rc = session_core_emit_request_redirect(s, MOQ_REQUEST_FAMILY_FETCH,
            s->fetches[slot].handle._opaque, redirect, error_code,
            can_retry, retry_after_ms, reason, reason_len);
        /* The emitter reports a terminal close as MOQ_OK, so the teardown below
         * runs only while the session is still open. */
        if (rc < 0 || s->state == MOQ_SESS_CLOSED) return rc;
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

        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_FETCH_ERROR;
        e.detail_size = (uint32_t)sizeof(moq_fetch_error_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.fetch_error.fetch = s->fetches[slot].handle;
        e.u.fetch_error.error_code =
            s->profile->semantic_request_error(error_code);
        e.u.fetch_error.can_retry = can_retry;
        e.u.fetch_error.retry_after_ms = retry_after_ms;
        e.u.fetch_error.reason = ev_reason;

        rc = push_event(s, &e);
        if (rc < 0) {
            s->event_scratch_len = scratch_saved;
            return rc;
        }
    }

    if (free_now) {
        fetch_free_entry(s, slot);
    } else {
        /* Stream-correlated profiles keep the slot so the request bidi can drain
         * its trailing FIN; the request-stream handler frees it on FIN. Close our
         * send half (reserved above) so the peer can retire the bidi. */
        if (close_half)
            (void)queue_close_bidi(s, s->fetches[slot].rs.ref);
        s->fetches[slot].state = MOQ_FETCH_DRAINING_RESPONSE;
        s->fetches[slot].control_response_seen = true;
    }
    return MOQ_OK;
}

/* -- Inbound FETCH_CANCEL handler ---------------------------------- */

moq_result_t session_core_on_fetch_cancel(moq_session_t *s,
                                           const moq_decoded_fetch_cancel_t *d)
{
    int slot = d->target_slot;
    moq_fetch_entry_t *e = &s->fetches[slot];

    bool need_reset = (e->state == MOQ_FETCH_ACCEPTED &&
                       e->data_stream_started);

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (need_reset && action_queue_avail(s) < 1) return MOQ_ERR_WOULD_BLOCK;

    if (need_reset) {
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_RESET_DATA;
        a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.reset_data.stream_ref = e->data_stream_ref;
        a.u.reset_data.error_code = 0x1;
        a.u.reset_data.reliable_size = e->data_header_len;
        moq_result_t arc = push_action(s, &a);
        if (arc < 0) return arc;
    }

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FETCH_CANCELLED;
    ev.detail_size = (uint32_t)sizeof(moq_fetch_cancelled_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.fetch_cancelled.fetch = e->handle;
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;

    fetch_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Inbound FETCH_OK handler -------------------------------------- */

int fetch_find_by_request_id(moq_session_t *s, uint64_t request_id)
{
    moq_request_endpoint_t ep = request_registry_find_by_id(s, request_id);
    if (ep.kind != MOQ_REQ_FETCH) return -1;
    return ep.slot;
}


static moq_result_t fetch_request_bidi_cancel(moq_session_t *s, int slot);
static moq_result_t fetch_request_bidi_cancel_ex(moq_session_t *s, int slot,
                                                 bool stop_data_stream,
                                                 uint64_t error_code);

moq_result_t session_core_on_fetch_ok(moq_session_t *s,
                                       const moq_decoded_fetch_ok_t *d)
{
    moq_fetch_entry_t *e = &s->fetches[d->target_slot];

    if (e->control_response_seen)
        return close_with_error(s, 0x3, "duplicate control response for fetch");

    /* If data FIN already arrived, FETCH_OK also completes the fetch: reserve
     * the completion (its event, request-half FIN and drain slot) alongside
     * FETCH_OK's own event. A draft-22 completion instead waits until the
     * enclosing dispatcher has consumed every buffered response, including a
     * following GOAWAY. */
    bool complete_here = e->data_stream_fin &&
                         !s->profile->request_fin_is_not_cancel;
    if (complete_here) {
        if (!fetch_fetcher_completion_fits(s, d->target_slot, 1))
            return MOQ_ERR_WOULD_BLOCK;
    } else if (event_queue_avail(s) < 1) {
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (d->has_deferred_param_error)
        return close_with_error(s, 0x3, d->deferred_param_reason);

    if (d->end_group < e->start_group ||
        (d->end_group == e->start_group && d->end_object > 0 &&
         d->end_object <= e->start_object))
        return close_with_error(s, 0x3, "FETCH_OK end before requested start");

    /* §2.5.1: an unknown Mandatory Track Property in FETCH_OK -> the subscriber
     * MUST cancel the fetch. Surface FETCH_ERROR(UNSUPPORTED_EXTENSION) and tear
     * down the request bidi via the internal cancel sequence (not the public
     * fetch-cancel entrypoint). Reserve event + cancel actions before mutating. */
    if (d->track_properties_unsupported) {
        if (event_queue_avail(s) < 1) return MOQ_ERR_WOULD_BLOCK;
        size_t scr0 = s->event_scratch_len;
        static const char k_reason[] = "unsupported mandatory track property";
        moq_bytes_t reason = {0};
        reason.data = event_scratch_copy(s, (const uint8_t *)k_reason,
                                   sizeof(k_reason) - 1);
        reason.len = sizeof(k_reason) - 1;
        if (!reason.data) {
            s->event_scratch_len = scr0;
            if (scr0 == 0)
                return close_with_error(s, 0x1, "event scratch permanently too small");
            return MOQ_ERR_BUFFER;
        }
        moq_fetch_t handle = e->handle;
        moq_result_t crc = fetch_request_bidi_cancel(s, d->target_slot);
        if (crc < 0) { s->event_scratch_len = scr0; return crc; }
        moq_event_t ee;
        memset(&ee, 0, sizeof(ee));
        ee.kind = MOQ_EVENT_FETCH_ERROR;
        ee.detail_size = (uint32_t)sizeof(moq_fetch_error_event_t);
        ee.borrow_epoch = s->borrow_epoch;
        ee.u.fetch_error.fetch = handle;
        ee.u.fetch_error.error_code = MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION;
        ee.u.fetch_error.reason = reason;
        return push_event(s, &ee);   /* slot reserved above */
    }

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

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FETCH_OK;
    ev.detail_size = (uint32_t)sizeof(moq_fetch_ok_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.fetch_ok.fetch = e->handle;
    ev.u.fetch_ok.end_of_track = d->end_of_track;
    ev.u.fetch_ok.end_group = d->end_group;
    ev.u.fetch_ok.end_object = d->end_object;
    ev.u.fetch_ok.track_properties = props;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    e->control_response_seen = true;
    e->control_ok = true;
    if (s->profile->request_fin_is_not_cancel)
        (void)moq_request_lifetime_initial_response_committed(&e->rs.lifetime,
                                                             false, true);

    /* The completion precheck above reserved everything this commits. */
    if (complete_here)
        return fetch_fetcher_completion_commit(s, d->target_slot);

    return MOQ_OK;
}

/* -- Public API ---------------------------------------------------- */

/*
 * Pointer-only init: touches ONLY the frozen v0 prefix. A caller compiled
 * against the older header allocated exactly that much, so a newer library
 * must not clear or stamp past it -- that is the whole point of freezing the
 * floor rather than memset(sizeof(*cfg)).
 */
/* Pin the PUBLIC macro to the first appended field.
 *
 * offsetof(auth_tokens) is by definition that field's starting offset -- any
 * preceding padding is already inside it -- so this is not guarding against
 * alignment. What it guards is the macro's DEFINITION: someone redefining
 * MOQ_FETCH_CFG_V0_SIZE to a larger boundary (say offsetof(joining_pub)) would
 * make the pointer-only initializer write past an allocation sized from the
 * original layout, and that enlargement fails the build here instead. */
_Static_assert(MOQ_FETCH_CFG_V0_SIZE ==
                   offsetof(moq_fetch_cfg_t, auth_tokens),
               "MOQ_FETCH_CFG_V0_SIZE must end exactly at auth_tokens");
/* The appended fields are ordered auth_tokens, auth_token_count, joining_pub;
 * a reordering would silently change which sized offers reach what. */
_Static_assert(offsetof(moq_fetch_cfg_t, auth_tokens) <
                   offsetof(moq_fetch_cfg_t, auth_token_count) &&
               offsetof(moq_fetch_cfg_t, auth_token_count) <
                   offsetof(moq_fetch_cfg_t, joining_pub),
               "appended FETCH cfg fields must stay in append order");

void moq_fetch_cfg_init(moq_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_FETCH_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_FETCH_CFG_V0_SIZE;
}

void moq_fetch_cfg_init_sized(moq_fetch_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about -- the same contract as moq_publish_cfg_init_sized().
     * A size too small even to hold the stamp touches NOTHING: inflating it to
     * the v0 floor would write past a caller's allocation, which is the one
     * thing a sized initializer exists to prevent. Rejecting a stamp below the
     * frozen floor is the CONSUMING call's job (moq_session_fetch), not this
     * one's. */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

void moq_accept_fetch_cfg_init(moq_accept_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_accept_fetch_cfg_t);
}

/* Order of two Locations. */
static int fetch_loc_cmp(uint64_t ag, uint64_t ao, uint64_t bg, uint64_t bo)
{
    if (ag != bg) return ag < bg ? -1 : 1;
    if (ao != bo) return ao < bo ? -1 : 1;
    return 0;
}

/* The inclusive Location of a fetch end in the core's form: an exclusive end
 * object, zero meaning the whole end group. */
static void fetch_end_inclusive(uint64_t end_group, uint64_t end_object,
                                uint64_t *g, uint64_t *o)
{
    *g = end_group;
    *o = end_object == 0 ? UINT64_MAX : end_object - 1;
}

/* Whether the End Location an application resolved for a fetch surfaced with
 * its range unresolved is one the request allows. A form without an end ends
 * at the Largest Object, so the End is the Largest Object, and a start stated
 * relative to it follows from the End: {G + 1 - N, 0} for a relative offset
 * N, clamped to [0, UINT64_MAX], and {G, O + 1} for the Next Object. The End may not
 * precede the start (the fetcher closes the session on such a FETCH_OK) and
 * may not pass a bounded request's own end. */
static bool fetch_deferred_end_allowed(const moq_fetch_entry_t *e,
                                       uint64_t end_group, uint64_t end_object)
{
    uint64_t eg, eo;
    fetch_end_inclusive(end_group, end_object, &eg, &eo);
    uint64_t sg = e->start_group, so = e->start_object;
    if (e->req_start_relative) {
        so = 0;
        if (e->req_relative_offset == 0) {
            sg = eg == UINT64_MAX ? UINT64_MAX : eg + 1;
        } else {
            sg = e->req_relative_offset - 1 > eg
               ? 0 : eg - (e->req_relative_offset - 1);
        }
    } else if (e->req_start_next_object) {
        if (eo == UINT64_MAX) return false;
        sg = eg;
        so = eo + 1;
    }
    if (fetch_loc_cmp(eg, eo, sg, so) < 0) return false;
    if (!e->req_end_at_largest) {
        uint64_t rg, ro;
        fetch_end_inclusive(e->req_end_group, e->req_end_object, &rg, &ro);
        if (fetch_loc_cmp(eg, eo, rg, ro) > 0) return false;
    }
    return true;
}

/* Classify only the first bytes of a fresh stream, through its profile. */
bool bridge_session_is_fetch_header(
    const moq_session_t *s, const moq_send_data_action_t *data)
{
    return s->profile->classify_data_stream(data->header, data->header_len) ==
        MOQ_STREAM_KIND_FETCH;
}

/* A FETCH response stream's key: its single stream needs no group order.
 * The publisher priority is a placeholder; each object's travels in its
 * SEND_DATA scheduling_priority. */
static moq_result_t fetch_push_data_key(moq_session_t *s,
                                        const moq_fetch_entry_t *e,
                                        moq_stream_ref_t ref)
{
    moq_data_priority_t key;
    memset(&key, 0, sizeof(key));
    key.subscriber_priority = e->subscriber_priority;
    key.publisher_priority = 128;
    key.owner = e->handle._opaque;
    return session_push_data_key(s, ref, &key, NULL);
}

moq_result_t moq_session_accept_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    const moq_accept_fetch_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
#define ACCEPT_FETCH_CFG_MIN offsetof(moq_accept_fetch_cfg_t, track_properties)
    if (cfg->struct_size < ACCEPT_FETCH_CFG_MIN) return MOQ_ERR_INVAL;
#define ACCEPT_FETCH_HAS(f) \
    (cfg->struct_size >= offsetof(moq_accept_fetch_cfg_t, f) + sizeof(cfg->f))

    /* Every draft's FETCH_OK ends with the track's properties (draft-16
     * Track Extensions, draft-18/22 Track Properties). */
    const uint8_t *fetch_props = NULL;
    size_t fetch_props_len = 0;
    if (ACCEPT_FETCH_HAS(track_properties) && cfg->track_properties.len > 0) {
        if (!cfg->track_properties.data) return MOQ_ERR_INVAL;
        fetch_props = cfg->track_properties.data;
        fetch_props_len = cfg->track_properties.len;
    }
    if (s->profile->validate_track_properties &&
        s->profile->validate_track_properties(fetch_props, fetch_props_len) < 0)
        return MOQ_ERR_INVAL;
    /* The request asked for none (INCLUDE_PROPERTIES = 0): "the Track
     * Properties are still present in the message, but they SHOULD be
     * empty". Applied once the handle resolves, below. */
#undef ACCEPT_FETCH_HAS
#undef ACCEPT_FETCH_CFG_MIN

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->fetches[slot].state != MOQ_FETCH_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (s->fetches[slot].range_deferred &&
        !fetch_deferred_end_allowed(&s->fetches[slot], cfg->end_group,
                                    cfg->end_object))
        return MOQ_ERR_INVAL;
    if (s->fetches[slot].omit_track_properties) {
        fetch_props = NULL;
        fetch_props_len = 0;
    }
    if (s->profile->request_fin_is_not_cancel && cfg->empty) {
        moq_result_t rc = fetch_process_buffered_request(s, slot);
        if (rc < 0) return rc;
        if (!session_is_active(s)) return MOQ_ERR_CLOSED;
        slot = fetch_resolve_handle(s, fetch);
        if (slot < 0) return MOQ_ERR_STALE_HANDLE;
        if (s->fetches[slot].rs.recv_len != 0) return MOQ_ERR_WOULD_BLOCK;
    }

    /* Stream-correlated profiles deliver FETCH_OK on the request bidi, then open
     * the response data uni with a FETCH_HEADER (carrying the Request ID). */
    if (moq_session_uses_request_streams(s)) {
        /* REQUEST_UPDATEs that arrived before this answer are answered right
         * after FETCH_OK, in the same send; a refused one ends the fetch
         * here (see fetch_end_on_refused_update). An empty fetch is finished
         * by this call (§10.12.3: header and FIN on the data stream), so
         * FETCH_OK also FINs the request stream's local half. Either way,
         * while the fetcher's half is still open a drain slot absorbs its
         * later FIN or RESET once the entry is gone. */
        moq_fetch_entry_t *fe = &s->fetches[slot];
        const bool upd_refused = fe->upd_failed;
        const bool empty_fin = cfg->empty;
        const bool need_drain = (upd_refused || empty_fin) &&
                                !fetch_peer_fin_observed(fe);
        /* FETCH_OK on the request bidi, FETCH_HEADER on the uni and the
         * uni's key; for a refusal also the data stream's reset and an
         * event. */
        if (action_queue_avail(s) < (upd_refused ? 3u : 2u) +
                                    (s->data_priority_updates ? 1u : 0u))
            return MOQ_ERR_WOULD_BLOCK;
        if (upd_refused && event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
            return MOQ_ERR_WOULD_BLOCK;
        moq_accept_fetch_encode_args_t ok_args = {
            .request_id = s->fetches[slot].request_id,
            .end_of_track = cfg->end_of_track,
            .end_group = cfg->end_group,
            .end_object = cfg->end_object,
            .track_properties = fetch_props,
            .track_properties_len = fetch_props_len,
        };
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                            s->send_cap - s->send_len);
        moq_result_t rc = s->profile->encode_fetch_ok(s, &w, &ok_args);
        if (rc < 0) return rc;
        for (uint32_t i = 0; i < fe->upd_ok_owed; i++) {
            moq_request_update_ok_encode_args_t oka = { .request_id = 0 };
            rc = s->profile->encode_request_update_ok(s, &w, &oka);
            if (rc < 0) return rc;
        }
        if (upd_refused) {
            rc = s->profile->encode_request_error(s, &w,
                &(moq_request_error_encode_args_t){ .error_code = fe->upd_fail_code });
            if (rc < 0) return rc;
        }
        size_t ok_len = moq_buf_writer_offset(&w);

        uint8_t hdr_inline[32];
        moq_buf_writer_t hw;
        moq_buf_writer_init(&hw, hdr_inline, sizeof(hdr_inline));
        rc = s->profile->encode_fetch_header(s, &hw,
                                             s->fetches[slot].request_id);
        if (rc < 0) return rc;
        uint8_t hdr_len = (uint8_t)moq_buf_writer_offset(&hw);

        /* FETCH_OK on the request bidi, which stays open while the response is
         * still to be written and ends with it for an empty fetch. Post-GOAWAY
         * gate (§10.4): a migrated request accepts no further request-bidi bytes
         * (defensive — accept on an already-ACCEPTED+migrated fetch is unreachable). */
        if (request_goaway_already_sent(s, s->fetches[slot].rs.ref))
            return MOQ_ERR_WRONG_STATE;
        moq_action_t ok_act;
        memset(&ok_act, 0, sizeof(ok_act));
        ok_act.kind = MOQ_ACTION_SEND_BIDI_STREAM;
        ok_act.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
        ok_act.borrow_epoch = s->borrow_epoch;
        ok_act.u.send_bidi_stream.stream_ref =
            s->fetches[slot].rs.ref;
        ok_act.u.send_bidi_stream.data = s->send_buf + s->send_len;
        ok_act.u.send_bidi_stream.len = ok_len;
        ok_act.u.send_bidi_stream.fin = upd_refused || empty_fin;
        rc = push_action(s, &ok_act);
        if (rc < 0) return rc;
        s->send_len += ok_len;

        /* FETCH_HEADER on a fresh data uni (FIN now for an empty fetch). */
        moq_action_t data_act;
        memset(&data_act, 0, sizeof(data_act));
        data_act.kind = MOQ_ACTION_SEND_DATA;
        data_act.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        data_act.borrow_epoch = s->borrow_epoch;
        memcpy(data_act.u.send_data.header, hdr_inline, hdr_len);
        data_act.u.send_data.header_len = hdr_len;
        data_act.u.send_data.stream_ref =
            moq_stream_ref_from_u64(s->next_stream_ref);
        data_act.u.send_data.payload = NULL;
        data_act.u.send_data.fin = cfg->empty && !upd_refused;
        data_act.u.send_data.scheduling_priority = UINT64_C(0x10000) |
            ((uint64_t)s->fetches[slot].subscriber_priority << 8) | 128;
        rc = fetch_push_data_key(s, &s->fetches[slot],
                                 data_act.u.send_data.stream_ref);
        if (rc < 0) return rc;
        rc = push_action(s, &data_act);
        if (rc < 0) return rc;

        s->fetches[slot].state = MOQ_FETCH_ACCEPTED;
        s->fetches[slot].data_stream_ref = data_act.u.send_data.stream_ref;
        s->fetches[slot].data_stream_started = true;
        s->fetches[slot].data_header_len = hdr_len;
        s->next_stream_ref++;
        if (upd_refused)
            fetch_end_on_refused_update(s, slot, true, need_drain);
        else if (cfg->empty) {
            if (need_drain)   /* slot reserved above */
                (void)drain_ref_add(s, fe->rs.ref);
            fetch_free_entry(s, slot);
        }
        return MOQ_OK;
    }

    /* Pre-check: 2 action slots (3 with the uni's key) before any
     * encoding/mutation. */
    if (action_queue_avail(s) < (s->data_priority_updates ? 3u : 2u))
        return MOQ_ERR_WOULD_BLOCK;

    /* Encode both outputs before pushing either action. */
    moq_accept_fetch_encode_args_t ok_args = {
        .request_id = s->fetches[slot].request_id,
        .end_of_track = cfg->end_of_track,
        .end_group = cfg->end_group,
        .end_object = cfg->end_object,
        .track_properties = fetch_props,
        .track_properties_len = fetch_props_len,
    };
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);
    moq_result_t rc = s->profile->encode_fetch_ok(s, &w, &ok_args);
    if (rc < 0) return rc;
    size_t ok_len = moq_buf_writer_offset(&w);

    uint8_t hdr_inline[32];
    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, hdr_inline, sizeof(hdr_inline));
    rc = s->profile->encode_fetch_header(s, &hw, s->fetches[slot].request_id);
    if (rc < 0) return rc;
    uint8_t hdr_len = (uint8_t)moq_buf_writer_offset(&hw);

    /* Push FETCH_OK action. */
    moq_action_t ctl_act;
    memset(&ctl_act, 0, sizeof(ctl_act));
    ctl_act.kind = MOQ_ACTION_SEND_CONTROL;
    ctl_act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
    ctl_act.borrow_epoch = s->borrow_epoch;
    ctl_act.u.send_control.data = s->send_buf + s->send_len;
    ctl_act.u.send_control.len = ok_len;
    rc = push_action(s, &ctl_act);
    if (rc < 0) return rc;
    s->send_len += ok_len;

    /* Push SEND_DATA action with pre-encoded header. */
    moq_action_t data_act;
    memset(&data_act, 0, sizeof(data_act));
    data_act.kind = MOQ_ACTION_SEND_DATA;
    data_act.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    data_act.borrow_epoch = s->borrow_epoch;
    memcpy(data_act.u.send_data.header, hdr_inline, hdr_len);
    data_act.u.send_data.header_len = hdr_len;
    data_act.u.send_data.stream_ref = moq_stream_ref_from_u64(s->next_stream_ref);
    data_act.u.send_data.payload = NULL;
    data_act.u.send_data.fin = cfg->empty;
    data_act.u.send_data.scheduling_priority = UINT64_C(0x10000) |
        ((uint64_t)s->fetches[slot].subscriber_priority << 8) | 128;
    rc = fetch_push_data_key(s, &s->fetches[slot],
                             data_act.u.send_data.stream_ref);
    if (rc < 0) return rc;
    rc = push_action(s, &data_act);
    if (rc < 0) return rc;

    /* Commit. */
    moq_fetch_entry_t *entry = &s->fetches[slot];
    entry->state = MOQ_FETCH_ACCEPTED;
    entry->data_stream_ref = data_act.u.send_data.stream_ref;
    entry->data_stream_started = true;
    entry->data_header_len = hdr_len;
    s->next_stream_ref++;

    if (cfg->empty) {
        fetch_free_entry(s, slot);
    }

    return MOQ_OK;
}

void moq_reject_fetch_cfg_init(moq_reject_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_reject_fetch_cfg_t);
}

moq_result_t moq_session_fetch(moq_session_t *s,
                                const moq_fetch_cfg_t *cfg,
                                uint64_t now_us,
                                moq_fetch_t *out_handle)
{
    if (!s || !cfg || !out_handle) return MOQ_ERR_INVAL;
    /* The public frozen floor, not a duplicate local offset: a stamp below it
     * cannot describe even the original layout. */
    if (cfg->struct_size < MOQ_FETCH_CFG_V0_SIZE) return MOQ_ERR_INVAL;
#define FETCH_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_fetch_cfg_t, f) + sizeof(cfg->f))
    *out_handle = MOQ_FETCH_INVALID;

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (FETCH_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count,
                                 s->profile->auth_token_type_max) < 0)
        return MOQ_ERR_INVAL;

    /* Exactly one joining discriminator. Checked here, before
     * session_begin_advance and before any state is touched, so a malformed
     * pair can never half-apply. */
    /* "Named", not "well-formed": a caller that set a STALE or malformed
     * handle has still named that discriminator, and must hear
     * MOQ_ERR_STALE_HANDLE from the resolver rather than have the field read
     * as absent. Only a zero handle means "not set". */
    const bool has_join_pub =
        FETCH_CFG_HAS(joining_pub) && cfg->joining_pub._opaque != 0;
    const bool has_join_sub = cfg->joining_sub._opaque != 0;
    if (cfg->is_joining && !s->profile->supports_joining_fetch)
        return MOQ_ERR_UNSUPPORTED;  /* no joining form on this profile */
    if (cfg->is_joining && (has_join_pub == has_join_sub))
        return MOQ_ERR_INVAL;      /* both, or neither */
    if (!cfg->is_joining && (has_join_pub || has_join_sub))
        return MOQ_ERR_INVAL;      /* a standalone fetch names no owner */

    /* A standalone range in its semantic form, and a FILL_TIMEOUT: only on a
     * profile whose FETCH carries them. */
    const moq_location_filter_cfg_t *location =
        FETCH_CFG_HAS(location) ? cfg->location : NULL;
    const bool has_fill_timeout =
        FETCH_CFG_HAS(fill_timeout_ms) && cfg->has_fill_timeout;
    uint64_t loc_start_group = 0, loc_start_object = 0;
    if (location || has_fill_timeout) {
        if (cfg->is_joining) return MOQ_ERR_INVAL;
        if (!s->profile->fetch_request_options) return MOQ_ERR_UNSUPPORTED;
    }
    const bool has_include_properties =
        FETCH_CFG_HAS(include_properties) && cfg->has_include_properties;
    const bool include_properties = has_include_properties && cfg->include_properties;
    if (has_include_properties) {
        if (cfg->is_joining) return MOQ_ERR_INVAL;
        if (!s->profile->include_properties_request) return MOQ_ERR_UNSUPPORTED;
    }
    if (location) {
        uint32_t ft;
        uint64_t eg, eo;
        moq_result_t lrc = moq_location_filter_to_core(location, &ft,
            &loc_start_group, &loc_start_object, &eg, &eo);
        if (lrc < 0) return lrc;
        /* A start relative to the publisher's Largest Object is not known
         * here: FETCH_OK's End is checked against the origin, which every
         * start follows. */
        if (location->kind == MOQ_LOCATION_FILTER_RELATIVE_GROUP ||
            location->kind == MOQ_LOCATION_FILTER_NEXT_OBJECT ||
            location->kind == MOQ_LOCATION_FILTER_NONE) {
            loc_start_group = 0;
            loc_start_object = 0;
        }
    }
#undef FETCH_CFG_HAS

    session_begin_advance(s, now_us);

    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    uint64_t joining_wire_request_id = 0;
    uint64_t joining_computed_start_group = 0;
    if (cfg->is_joining && has_join_pub) {
        /* PUBLISH-initiated subscription (draft-18 5.1). Established only:
         * 10.12.2's state list is "Established or Pending (subscriber)", which
         * deliberately excludes Pending (Publisher). */
        int jpub = pub_resolve_handle(s, cfg->joining_pub);
        if (jpub < 0) return MOQ_ERR_STALE_HANDLE;
        const moq_pub_entry_t *pe = &s->publishes[jpub];
        if (pe->state != MOQ_PUB_ESTABLISHED) return MOQ_ERR_WRONG_STATE;
        if (pe->done_pending) return MOQ_ERR_WRONG_STATE;
        if (!pub_joining_eligible(s, pe)) return MOQ_ERR_WRONG_STATE;
        /* The LATCHED Joining Location, not a live history read. */
        if (!pe->has_joining_loc) return MOQ_ERR_INVAL;
        joining_wire_request_id = pe->request_id;
        if (cfg->joining_relative) {
            joining_computed_start_group =
                (cfg->joining_start > pe->joining_group)
                    ? 0
                    : pe->joining_group - cfg->joining_start;
        } else {
            joining_computed_start_group = cfg->joining_start;
        }
    } else if (cfg->is_joining) {
        int jsub = sub_resolve_handle(s, cfg->joining_sub);
        if (jsub < 0) return MOQ_ERR_STALE_HANDLE;
        if (s->subs[jsub].state != MOQ_SUB_ESTABLISHED &&
            s->subs[jsub].state != MOQ_SUB_PENDING_SUBSCRIBER)
            return MOQ_ERR_WRONG_STATE;
        /* A deferred terminal (done_pending) is logically Terminated; the
         * Joining Location is usable only while Established (§5.1). */
        if (s->subs[jsub].done_pending)
            return MOQ_ERR_WRONG_STATE;
        if (s->subs[jsub].filter_type != MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT)
            return MOQ_ERR_INVAL;
        /* Use the saved Joining Location: establishment stamps it, and D18
         * replaces it on a Forward 0 -> 1 ACK. Slot allocation/free clears it,
         * so a reused subscription cannot inherit its predecessor's value. */
        if (!s->subs[jsub].has_largest)
            return MOQ_ERR_INVAL;
        joining_wire_request_id = s->subs[jsub].request_id;
        {
            if (cfg->joining_relative) {
                if (cfg->joining_start > s->subs[jsub].largest_group)
                    joining_computed_start_group = 0;
                else
                    joining_computed_start_group =
                        s->subs[jsub].largest_group - cfg->joining_start;
            } else {
                joining_computed_start_group = cfg->joining_start;
            }
        }
    } else {
        if (moq_validate_full_track_name_min_fields(
                &cfg->track_namespace, cfg->track_name,
                s->profile->min_track_namespace_fields) < 0)
            return MOQ_ERR_INVAL;
        if (!location && cfg->end_group < cfg->start_group)
            return MOQ_ERR_INVAL;
        if (!location && cfg->end_group == cfg->start_group &&
            cfg->end_object > 0 && cfg->end_object <= cfg->start_object)
            return MOQ_ERR_INVAL;
    }
    if (cfg->group_order != MOQ_GROUP_ORDER_DEFAULT &&
        cfg->group_order != MOQ_GROUP_ORDER_ASCENDING &&
        cfg->group_order != MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
    /* A profile whose fetch data plane cannot reconstruct a descending
     * response must not request one; refused before anything is sent. */
    if (cfg->group_order == MOQ_GROUP_ORDER_DESCENDING &&
        !s->profile->fetch_descending_supported)
        return MOQ_ERR_INVAL;

    /* The Track this FETCH is for, kept with the request: a malformed Track
     * found on any request for it cancels this one too (draft-22 §12.1). A
     * joining FETCH is for its subscription's Track. */
    uint8_t *track_key = NULL;
    size_t track_key_len = 0;
    {
        const uint8_t *src = NULL;
        size_t src_len = 0;
        if (cfg->is_joining && has_join_pub) {
            const moq_track_hist_t *h = s->publishes[pub_resolve_handle(s, cfg->joining_pub)].hist;
            if (h) { src = h->key; src_len = h->key_len; }
        } else if (cfg->is_joining) {
            const moq_sub_entry_t *je = &s->subs[sub_resolve_handle(s, cfg->joining_sub)];
            if (je->track_id_buf) { src = je->track_id_buf; src_len = je->track_id_len; }
            else if (je->hist) { src = je->hist->key; src_len = je->hist->key_len; }
        }
        if (cfg->is_joining) {
            if (src && src_len > 0) {
                track_key = (uint8_t *)s->alloc.alloc(src_len, s->alloc.ctx);
                if (!track_key) return MOQ_ERR_NOMEM;
                memcpy(track_key, src, src_len);
                track_key_len = src_len;
            }
        } else {
            track_key = moq_build_track_key(s, &cfg->track_namespace, cfg->track_name,
                                            &track_key_len);
            if (!track_key) return MOQ_ERR_NOMEM;
        }
    }
#define FETCH_DROP_KEY() \
    do { if (track_key) s->alloc.free(track_key, track_key_len, s->alloc.ctx); } while (0)

    bool req_stream = moq_session_uses_request_streams(s);
    moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0);

    moq_request_endpoint_t req_ep;
    {
        moq_result_t prc = s->profile->prepare_request(s, &req_ep);
        if (prc < 0) { FETCH_DROP_KEY(); return prc; }
    }

    int slot = fetch_find_free(s);
    if (slot < 0) {
        s->profile->abort_request(s, &req_ep);
        FETCH_DROP_KEY();
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (action_queue_full(s)) {
        s->profile->abort_request(s, &req_ep);
        FETCH_DROP_KEY();
        return MOQ_ERR_WOULD_BLOCK;
    }
    {
        uint8_t priority = cfg->has_subscriber_priority ?
                           cfg->subscriber_priority : 128;

        uint32_t ft = 1;
        if (cfg->is_joining)
            ft = cfg->joining_relative ? 2 : 3;

        moq_fetch_encode_args_t args = {
            .request_id = req_ep.request_id,
            .fetch_type = ft,
            .track_namespace = cfg->track_namespace,
            .track_name = cfg->track_name,
            .start_group = cfg->start_group,
            .start_object = cfg->start_object,
            .end_group = cfg->end_group,
            .end_object = cfg->end_object,
            .joining_request_id = joining_wire_request_id,
            .joining_start = cfg->joining_start,
            .subscriber_priority = priority,
            .group_order = (uint8_t)cfg->group_order,
            .auth_tokens = auth_tokens,
            .auth_token_count = auth_token_count,
            .location = location,
            .has_fill_timeout = has_fill_timeout,
            .fill_timeout_ms = has_fill_timeout ? cfg->fill_timeout_ms : 0,
            .has_include_properties = has_include_properties,
            .include_properties = include_properties,
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);

        moq_result_t rc2 = s->profile->encode_fetch(s, &w, &args);
        if (rc2 == MOQ_ERR_BUFFER && s->send_len > 0) {
            moq_buf_writer_t probe;
            rc2 = send_probe_begin(s, &probe);
            if (rc2 == MOQ_OK) {
                rc2 = s->profile->encode_fetch(s, &probe, &args);
                rc2 = send_probe_end(s, &probe, rc2);
            }
        }
        if (rc2 < 0) {
            s->profile->abort_request(s, &req_ep);
            FETCH_DROP_KEY();
            return rc2;
        }

        size_t encoded_len = moq_buf_writer_offset(&w);
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.borrow_epoch = s->borrow_epoch;
        if (req_stream) {
            /* Stream-correlated profiles: the request opens its own bidi stream
             * and the FETCH_OK/REQUEST_ERROR response returns on it. */
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
            FETCH_DROP_KEY();
            return arc;
        }
        s->send_len += encoded_len;
    }
#undef FETCH_DROP_KEY

    /* Commit. */
    moq_fetch_entry_t *entry = &s->fetches[slot];
    entry->generation |= 1;
    entry->track_key = track_key;
    entry->track_key_len = track_key_len;
    /* Allocated: join the occupancy list the preamble scans walk. */
    fetch_occ_link(s, (size_t)slot);
    entry->state = MOQ_FETCH_PENDING_FETCHER;
    entry->role = MOQ_FETCH_ROLE_FETCHER;
    entry->effective_group_order = cfg->group_order == MOQ_GROUP_ORDER_DEFAULT
        ? MOQ_GROUP_ORDER_ASCENDING : (moq_group_order_t)cfg->group_order;
    entry->request_id = req_ep.request_id;
    entry->handle = fetch_make_handle(s, (size_t)slot);
    entry->start_group = cfg->is_joining ? joining_computed_start_group
                       : location ? loc_start_group : cfg->start_group;
    entry->start_object = cfg->is_joining ? 0
                        : location ? loc_start_object : cfg->start_object;
    req_ep.kind = MOQ_REQ_FETCH;
    req_ep.slot = slot;
    if (req_stream) {
        /* The FETCH_OK/REQUEST_ERROR response correlates by the request bidi
         * stream identity; the response data uni (FETCH_HEADER) carries the
         * Request ID, so register BOTH keys. fetch_free_entry removes both. */
        entry->rs.ref = req_ref;
        if (s->profile->request_fin_is_not_cancel)
            (void)moq_request_lifetime_init(&entry->rs.lifetime,
                                            MOQ_REQUEST_FAMILY_FETCH, true);
        req_ep.has_stream_ref = true;
        req_ep.stream_ref = req_ref;
        request_registry_insert_by_streamref(s, req_ref, req_ep);
        request_registry_insert_by_id(s, req_ep.request_id, req_ep);
        s->next_stream_ref++;
    } else {
        request_registry_insert_by_id(s, req_ep.request_id, req_ep);
    }

    *out_handle = entry->handle;
    s->profile->commit_request(s, &req_ep);
    return MOQ_OK;
}

moq_result_t moq_session_reject_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    const moq_reject_fetch_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_reject_fetch_cfg_t, redirect))
        return MOQ_ERR_INVAL;   /* pre-redirect minimum; older callers still work */
    if (cfg->reason.len > 0 && !cfg->reason.data) return MOQ_ERR_INVAL;
    if (cfg->can_retry && cfg->retry_after_ms >= MOQ_QUIC_VARINT_MAX)
        return MOQ_ERR_INVAL;
    /* The code must be representable in THIS profile's wire encoding; refuse
     * before any mutation rather than truncate (draft-16 encodes a QUIC
     * varint, draft-18 a vi64 spanning the full 64-bit range). */
    if (cfg->error_code > s->profile->request_error_wire_max)
        return MOQ_ERR_INVAL;
#define FETCH_REJ_HAS(f) \
    (cfg->struct_size >= offsetof(moq_reject_fetch_cfg_t, f) + sizeof(cfg->f))

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->fetches[slot].state != MOQ_FETCH_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    moq_request_error_encode_args_t err_args = {
        .request_id = s->fetches[slot].request_id,
        .error_code = (uint64_t)cfg->error_code,
        .can_retry = cfg->can_retry,
        .retry_after_ms = cfg->retry_after_ms,
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len,
    };
    moq_result_t vrc = reject_apply_redirect(
        s, &err_args, FETCH_REJ_HAS(redirect) ? &cfg->redirect : NULL,
        false /* track-scoped */);
    if (vrc < 0) return vrc;
#undef FETCH_REJ_HAS

    /* REQUEST_ERROR + FIN closes only our send half; the fetcher opened the bidi
     * without FIN, so absorb its later empty FIN / in-flight bytes via the drain
     * ring once the entry is freed (else the empty-FIN path is fatal). Reserve the
     * drain slot before mutating (D16 responds on the control channel: no drain). */
    moq_stream_ref_t req_ref = s->fetches[slot].rs.ref;
    bool need_drain = req_ref._v != 0 &&
                      !fetch_peer_fin_observed(&s->fetches[slot]);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (err_args.has_redirect) {
        moq_result_t rc = queue_request_error_bidi(s, req_ref, &err_args);
        if (rc < 0) return rc;
    } else {
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);
        moq_result_t rc2 = s->profile->encode_request_error(s, &w, &err_args);
        if (rc2 < 0) return rc2;
        size_t elen = moq_buf_writer_offset(&w);
        moq_action_t act;
        memset(&act, 0, sizeof(act));
        act.borrow_epoch = s->borrow_epoch;
        if (moq_session_uses_request_streams(s)) {
            /* REQUEST_ERROR is terminal: deliver it on the request bidi and
             * finish that stream. Post-GOAWAY gate (§10.4): a migrated request
             * accepts no further request-bidi bytes (defensive — a reject targets a
             * pending fetch, which cannot have been migrated). */
            if (request_goaway_already_sent(s, s->fetches[slot].rs.ref))
                return MOQ_ERR_WRONG_STATE;
            act.kind = MOQ_ACTION_SEND_BIDI_STREAM;
            act.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
            act.u.send_bidi_stream.stream_ref =
                s->fetches[slot].rs.ref;
            act.u.send_bidi_stream.data = s->send_buf + s->send_len;
            act.u.send_bidi_stream.len = elen;
            act.u.send_bidi_stream.fin = true;
        } else {
            act.kind = MOQ_ACTION_SEND_CONTROL;
            act.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
            act.u.send_control.data = s->send_buf + s->send_len;
            act.u.send_control.len = elen;
        }
        moq_result_t arc = push_action(s, &act);
        if (arc < 0) return arc;
        s->send_len += elen;
    }

    if (need_drain)
        (void)drain_ref_add(s, req_ref);   /* slot reserved above */
    fetch_free_entry(s, slot);
    return MOQ_OK;
}

/* Internal draft-18 cancel of a fetcher-role fetch: STOP_SENDING + RESET the
 * request bidi, STOP the response data uni if it is open, enter the request bidi
 * in the drain ring when the peer's FIN has not already been observed, then free
 * the fetch slot and record the request id in the cancel-tombstone cache so an
 * in-flight data uni is absorbed rather than treated as an unknown request
 * (which would close the session). The data-stream STOP and the tombstone are
 * unconditional; only the drain reference follows the observed FIN. Reserve-before-mutate
 * (WOULD_BLOCK leaves state intact). Shared by moq_session_fetch_cancel and the
 * UNSUPPORTED_EXTENSION (0x33) path; the caller validated role/state and
 * request-stream use, and does not depend on session_begin_advance (safe inside
 * an inbound advance). */
static moq_result_t fetch_request_bidi_cancel(moq_session_t *s, int slot)
{
    return fetch_request_bidi_cancel_ex(s, slot, true, 0x1 /* CANCELLED */);
}

/* stop_data_stream false leaves the response data stream to a caller that
 * stops it itself (the receive path, which owns that stream's state). */
static moq_result_t fetch_request_bidi_cancel_ex(moq_session_t *s, int slot,
                                                 bool stop_data_stream,
                                                 uint64_t error_code)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    bool stop_data = stop_data_stream &&
                     e->data_stream_started && !e->data_stream_fin;
    /* An early responder FIN may already have closed both request directions,
     * while this owner still receives data. Cancel only that remaining data. */
    bool abort_request = !(s->profile->request_fin_is_not_cancel &&
        e->rs.lifetime.local_fin && fetch_peer_fin_observed(e));
    /* One drain decision for both the preflight and the insertion below: the
     * reference exists to absorb what the peer may still send on the request
     * bidi, so an already-observed FIN makes it unnecessary. The data-stream
     * STOP, registry removal, cancel tombstone and retirement still apply. */
    bool need_drain = e->rs.ref._v != 0 &&
                      !fetch_peer_fin_observed(e);
    if (action_queue_avail(s) < (size_t)((abort_request ? 1 : 0) + (stop_data ? 1 : 0)))
        return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    moq_stream_ref_t ref = e->rs.ref;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_ABORT_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_abort_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.abort_bidi_stream.stream_ref = ref;
    a.u.abort_bidi_stream.error_code = error_code;
    moq_result_t arc;
    if (abort_request) {
        arc = push_action(s, &a);
        if (arc < 0) return arc;
    }
    if (stop_data) {
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_STOP_DATA;
        a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.stop_data.stream_ref = e->data_stream_ref;
        a.u.stop_data.error_code = error_code;
        arc = push_action(s, &a);
        if (arc < 0) return arc;
    }
    if (e->rs.ref._v != 0) {
        request_registry_remove_by_streamref(s, e->rs.ref);
        if (need_drain)
            (void)drain_ref_add(s, ref);   /* slot reserved above */
        e->rs.ref = moq_stream_ref_from_u64(0);
    }
    /* The request bidi is entered in the drain ring only while still peer-open;
     * either way the fetch slot is freed now and the request id tombstoned, so a
     * late data uni (FETCH_HEADER) is stopped and absorbed without reoccupying
     * the pool. */
    fetch_cancel_tomb_add(s, e->request_id);
    fetch_free_entry(s, slot);
    return MOQ_OK;
}

void moq_fetch_update_cfg_init(moq_fetch_update_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = (uint32_t)sizeof(*cfg);
}

moq_result_t moq_session_update_fetch(moq_session_t *s, moq_fetch_t fetch,
                                      const moq_fetch_update_cfg_t *cfg,
                                      uint64_t now_us)
{
    if (!s || !cfg || cfg->struct_size < sizeof(moq_fetch_update_cfg_t))
        return MOQ_ERR_INVAL;
    if (!cfg->has_subscriber_priority && cfg->auth_token_count == 0)
        return MOQ_ERR_INVAL;
    if (moq_validate_auth_tokens(cfg->auth_tokens, cfg->auth_token_count,
                                 s->profile->auth_token_type_max) < 0)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    if (!s->profile->requester_updates) return MOQ_ERR_UNSUPPORTED;
    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_fetch_entry_t *e = &s->fetches[slot];
    if (e->role != MOQ_FETCH_ROLE_FETCHER ||
        e->state != MOQ_FETCH_PENDING_FETCHER ||
        e->rs.ref._v == 0 || e->rs.lifetime.local_fin ||
        e->goaway_sent || e->own_update_pending)
        return MOQ_ERR_WRONG_STATE;

    moq_request_update_encode_args_t args = {
        .existing_request_id = e->request_id,
        .has_subscriber_priority = cfg->has_subscriber_priority,
        .subscriber_priority = cfg->subscriber_priority,
        .auth_tokens = cfg->auth_token_count ? cfg->auth_tokens : NULL,
        .auth_token_count = cfg->auth_token_count,
        .sender = MOQ_UPDATE_SENDER_FETCHER,
    };
    moq_request_endpoint_t ep;
    moq_result_t rc = s->profile->prepare_request(s, &ep);
    if (rc < 0) return rc;
    args.request_id = ep.request_id;
    uint8_t buf[1024];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    rc = s->profile->encode_request_update(s, &w, &args);
    if (rc == MOQ_OK && action_queue_full(s)) rc = MOQ_ERR_WOULD_BLOCK;
    if (rc == MOQ_OK)
        rc = queue_send_bidi(s, e->rs.ref, buf, moq_buf_writer_offset(&w), false);
    if (rc < 0) {
        s->profile->abort_request(s, &ep);
        return rc;
    }
    s->profile->commit_request(s, &ep);
    e->own_update_pending = true;
    return MOQ_OK;
}

/* The publisher acknowledged this side's update of its FETCH. */
moq_result_t session_core_on_fetch_update_ok(moq_session_t *s, int slot)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    if (!e->own_update_pending)
        return close_with_error(s, 0x3, "REQUEST_OK without a pending update");
    e->own_update_pending = false;
    return MOQ_OK;
}

/* The publisher refused this side's update of its FETCH (§9.5.1: it resets
 * the FETCH data stream): the fetch ends. Both of its streams are stopped,
 * the handle retired, and FETCH_ERROR carries the refusal's code. The event
 * slot and the reason's arena space are reserved before anything changes; a
 * reason no arena of this session's size can hold is dropped, never the
 * session: the refusal is this request's outcome. */
moq_result_t session_core_on_fetch_update_error(moq_session_t *s, int slot,
    uint64_t error_code, const uint8_t *reason, size_t reason_len)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_fetch_entry_t *e = &s->fetches[slot];
    moq_fetch_t handle = e->handle;
    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t ev_reason = {0};
    switch (event_scratch_classify_bytes(s, reason_len)) {
    case MOQ_EVENT_SCRATCH_PERMANENT:
        reason_len = 0;   /* the outcome is the code; see moq_session_update_fetch */
        break;
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
    moq_result_t rc = fetch_request_bidi_cancel_ex(s, slot, true, 0x1 /* CANCELLED */);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FETCH_ERROR;
    ev.detail_size = (uint32_t)sizeof(moq_fetch_error_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.fetch_error.fetch = handle;
    ev.u.fetch_error.error_code = s->profile->semantic_request_error(error_code);
    ev.u.fetch_error.reason = ev_reason;
    (void)push_event(s, &ev);               /* slot reserved above */
    return MOQ_OK;
}

moq_result_t moq_session_fetch_cancel(moq_session_t *s,
                                       moq_fetch_t fetch,
                                       uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_fetch_entry_t *e = &s->fetches[slot];
    if (e->role != MOQ_FETCH_ROLE_FETCHER)
        return MOQ_ERR_WRONG_STATE;
    if (e->state != MOQ_FETCH_PENDING_FETCHER)
        return MOQ_ERR_WRONG_STATE;

    /* Stream-correlated profiles have no FETCH_CANCEL message: tear down the
     * request bidi via the shared internal sequence (STOP_SENDING + RESET, STOP
     * the data uni if open), which frees the entry and records its request id in
     * the cancel-tombstone cache. */
    if (moq_session_uses_request_streams(s))
        return fetch_request_bidi_cancel(s, slot);

    uint8_t buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_fetch_cancel(s, &w, e->request_id);
    if (rc < 0) return rc;

    rc = queue_send_cancel(s, buf, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;

    /* Free the fetch slot immediately so max_fetches is reusable; record the
     * request id in the bounded cancel-tombstone cache so a late FETCH_OK /
     * REQUEST_ERROR / data stream for it is absorbed without reoccupying the
     * pool or closing the session. */
    fetch_cancel_tomb_add(s, e->request_id);
    fetch_free_entry(s, slot);
    return MOQ_OK;
}

/* This side cannot take an object the response delivered (it exceeds the
 * local object-size limit): the fetch is cancelled, not the session.
 * §3.2.4: a subscriber cancelling a FETCH "MUST send STOP_SENDING for the bidi
 * request stream" (and MAY stop the data stream, which the caller does); on
 * a profile without request streams the cancel is the FETCH_CANCEL message.
 * Streams are terminated with CANCELLED (§12.5: "The stream was cancelled by
 * either endpoint"). The application sees FETCH_RESET with cause
 * MOQ_REQUEST_TERMINAL_LOCAL_CANCEL. The caller has reserved one event and
 * the actions; a refusal (WOULD_BLOCK) leaves everything unchanged. A fetch
 * that owes no terminal any more is left alone. */
static moq_result_t fetch_cancel_on_receive_code(moq_session_t *s, int slot,
                                                 uint64_t error_code,
                                                 moq_request_terminal_cause_t cause);

moq_result_t fetch_cancel_on_receive(moq_session_t *s, int slot)
{
    return fetch_cancel_on_receive_code(s, slot, 0x1 /* CANCELLED */,
                                        MOQ_REQUEST_TERMINAL_LOCAL_CANCEL);
}

/* The response carries a malformed Track (draft-22 §12.1): the fetch is
 * cancelled as above, its request stream reset and stopped with
 * MALFORMED_TRACK, and FETCH_RESET carries the MALFORMED_TRACK cause. */
moq_result_t session_fetch_fail_malformed(moq_session_t *s, int slot)
{
    return fetch_cancel_on_receive_code(s, slot, MOQ_STREAM_ERROR_MALFORMED_TRACK,
                                        MOQ_REQUEST_TERMINAL_MALFORMED_TRACK);
}

static moq_result_t fetch_cancel_on_receive_code(moq_session_t *s, int slot,
                                                 uint64_t error_code,
                                                 moq_request_terminal_cause_t cause)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    if (e->role != MOQ_FETCH_ROLE_FETCHER) return MOQ_OK;
    if (e->state != MOQ_FETCH_PENDING_FETCHER) return MOQ_OK;
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_fetch_t handle = e->handle;
    moq_result_t rc;
    if (moq_session_uses_request_streams(s)) {
        rc = fetch_request_bidi_cancel_ex(s, slot, false, error_code);
        if (rc < 0) return rc;
    } else {
        uint8_t buf[32];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));
        rc = s->profile->encode_fetch_cancel(s, &w, e->request_id);
        if (rc < 0) return rc;
        rc = queue_send_control(s, buf, moq_buf_writer_offset(&w));
        if (rc < 0) return rc;
        fetch_cancel_tomb_add(s, e->request_id);
        fetch_free_entry(s, slot);
    }
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FETCH_RESET;
    ev.detail_size = (uint32_t)sizeof(moq_fetch_reset_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.fetch_reset.fetch = handle;
    /* As sent: FETCH_CANCEL carries no code. */
    ev.u.fetch_reset.error_code =
        moq_session_uses_request_streams(s) ? error_code : 0;
    ev.u.fetch_reset.data_stream = true;
    ev.u.fetch_reset.stop_sending = false;
    ev.u.fetch_reset.cause = cause;
    (void)push_event(s, &ev);               /* slot reserved above */
    return MOQ_OK;
}

/* -- Fetcher-side terminal for a reset response data stream -------- */

moq_result_t fetch_on_data_stream_reset(moq_session_t *s, int slot,
                                        uint64_t error_code)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    /* Only an active fetcher-role request owes a terminal. DRAINING_RESPONSE
     * already surfaced FETCH_ERROR (the slot merely waits for the request
     * stream's FIN); GOAWAY_LOCAL was migrated; a publisher-role entry never
     * receives on its own response stream. */
    if (e->role != MOQ_FETCH_ROLE_FETCHER) return MOQ_OK;
    if (e->state != MOQ_FETCH_PENDING_FETCHER) return MOQ_OK;

    /* Stream-correlated profiles: the request bidi is still open in both
     * directions, so retire it the way a local cancel does (one ABORT action,
     * drain reference while the peer's FIN is unobserved) -- a late FETCH_OK
     * or REQUEST_ERROR on it is then absorbed, never a fresh request. On the
     * control-channel profile the cancel tombstone absorbs those instead. */
    bool uses_streams = e->rs.ref._v != 0;
    bool need_drain = uses_streams && !fetch_peer_fin_observed(e);
    bool abort_request = uses_streams && !(s->profile->request_fin_is_not_cancel &&
        e->rs.lifetime.local_fin && fetch_peer_fin_observed(e));
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (abort_request && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_FETCH_RESET;
    ev.detail_size = (uint32_t)sizeof(moq_fetch_reset_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.fetch_reset.fetch = e->handle;
    ev.u.fetch_reset.error_code = error_code;
    ev.u.fetch_reset.data_stream = true;
    ev.u.fetch_reset.stop_sending = false;
    ev.u.fetch_reset.cause = MOQ_REQUEST_TERMINAL_PEER_RESET_STREAM;
    (void)push_event(s, &ev);               /* slot reserved above */

    if (uses_streams) {
        moq_stream_ref_t ref = e->rs.ref;
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_ABORT_BIDI_STREAM;
        a.detail_size = (uint32_t)sizeof(moq_abort_bidi_stream_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.abort_bidi_stream.stream_ref = ref;
        a.u.abort_bidi_stream.error_code = 0x1;   /* CANCELLED */
        if (abort_request) (void)push_action(s, &a); /* slot reserved above */
        request_registry_remove_by_streamref(s, ref);
        if (need_drain)
            (void)drain_ref_add(s, ref);    /* slot reserved above */
        e->rs.ref = moq_stream_ref_from_u64(0);
    }
    fetch_cancel_tomb_add(s, e->request_id);
    fetch_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Locally-aborted publisher FETCH request-id grace cache -------- */

void fetch_abort_tomb_add(moq_session_t *s, uint64_t request_id)
{
    if (s->fetch_abort_tomb_cap == 0) return;
    for (size_t i = 0; i < s->fetch_abort_tomb_count; i++)
        if (s->fetch_abort_tombs[i] == request_id) return;
    if (s->fetch_abort_tomb_count >= s->fetch_abort_tomb_cap) {
        /* Full: drop the oldest. A grace window, not unbounded state: an
         * evicted id's late cancel is an unknown request and fails closed. */
        memmove(&s->fetch_abort_tombs[0], &s->fetch_abort_tombs[1],
                (s->fetch_abort_tomb_cap - 1) * sizeof(uint64_t));
        s->fetch_abort_tomb_count = s->fetch_abort_tomb_cap - 1;
    }
    s->fetch_abort_tombs[s->fetch_abort_tomb_count++] = request_id;
}

bool fetch_abort_tomb_consume(moq_session_t *s, uint64_t request_id)
{
    for (size_t i = 0; i < s->fetch_abort_tomb_count; i++) {
        if (s->fetch_abort_tombs[i] == request_id) {
            memmove(&s->fetch_abort_tombs[i], &s->fetch_abort_tombs[i + 1],
                    (s->fetch_abort_tomb_count - i - 1) * sizeof(uint64_t));
            s->fetch_abort_tomb_count--;
            return true;
        }
    }
    return false;
}

/* -- Publisher write APIs ------------------------------------------ */

void moq_fetch_object_cfg_init(moq_fetch_object_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_fetch_object_cfg_t);
}

moq_result_t moq_session_write_fetch_object(
    moq_session_t *s,
    moq_fetch_t fetch,
    const moq_fetch_object_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_fetch_object_cfg_t)) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->fetches[slot].state != MOQ_FETCH_ACCEPTED)
        return MOQ_ERR_WRONG_STATE;

    size_t payload_len = cfg->payload ? moq_rcbuf_len(cfg->payload) : 0;
    if (!session_payload_size_fits_profile(s, payload_len))
        return MOQ_ERR_INVAL;
    size_t props_len = cfg->properties ? moq_rcbuf_len(cfg->properties) : 0;

    /* Refuse object properties the draft forbids (draft-18: a Mandatory Track
     * Property as an object property is malformed) -- symmetric with inbound. */
    if (props_len > 0 && s->profile->validate_object_properties &&
        s->profile->validate_object_properties(
            s, moq_rcbuf_data(cfg->properties), props_len) < 0)
        return MOQ_ERR_INVAL;

    bool has_props = (props_len > 0);
    size_t slots_needed = has_props ? 2 : 1;
    if (action_queue_avail(s) < slots_needed) return MOQ_ERR_WOULD_BLOCK;

    moq_fetch_entry_t *entry = &s->fetches[slot];

    if (has_props) {
        /* Two-action encoding:
         * Action 1: header = flags+fields+properties_len; payload = properties rcbuf
         * Action 2: header = payload_len varint; payload = object payload rcbuf */
        moq_fetch_object_encode_args_t hdr_args = {
            .group_id = cfg->group_id,
            .subgroup_id = cfg->subgroup_id,
            .object_id = cfg->object_id,
            .publisher_priority = cfg->publisher_priority,
            .datagram = cfg->datagram,
            .properties_len = props_len,
            .header_only = true,
        };

        /* Encode action 1 header (flags+fields+properties_len). */
        moq_action_t a1;
        memset(&a1, 0, sizeof(a1));
        a1.kind = MOQ_ACTION_SEND_DATA;
        a1.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a1.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw1;
        moq_buf_writer_init(&hw1, a1.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_fetch_object(s, &hw1,
                                                           entry->effective_group_order, &hdr_args,
                                                           &entry->prior);
        if (rc < 0) return rc;
        a1.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw1);
        a1.u.send_data.stream_ref = entry->data_stream_ref;
        a1.u.send_data.scheduling_priority = UINT64_C(0x10000) |
            ((uint64_t)entry->subscriber_priority << 8) | cfg->publisher_priority;
        a1.u.send_data.payload = cfg->properties;
        a1.u.send_data.fin = false;

        /* Encode action 2 header (payload_len varint). */
        moq_action_t a2;
        memset(&a2, 0, sizeof(a2));
        a2.kind = MOQ_ACTION_SEND_DATA;
        a2.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a2.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw2;
        moq_buf_writer_init(&hw2, a2.u.send_data.header, 32);
        rc = s->profile->encode_object_payload_prefix(s, &hw2, payload_len, false);
        if (rc < 0) return rc;
        a2.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw2);
        a2.u.send_data.stream_ref = entry->data_stream_ref;
        a2.u.send_data.scheduling_priority = UINT64_C(0x10000) |
            ((uint64_t)entry->subscriber_priority << 8) | cfg->publisher_priority;
        a2.u.send_data.payload = cfg->payload;
        a2.u.send_data.fin = false;

        /* Incref both rcbufs after all encoding succeeds. */
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
        /* Single-action encoding: header = flags+fields+payload_len. */
        moq_fetch_object_encode_args_t obj_args = {
            .group_id = cfg->group_id,
            .subgroup_id = cfg->subgroup_id,
            .object_id = cfg->object_id,
            .publisher_priority = cfg->publisher_priority,
            .datagram = cfg->datagram,
            .payload_len = payload_len,
        };

        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_SEND_DATA;
        a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
        a.borrow_epoch = s->borrow_epoch;

        moq_buf_writer_t hw;
        moq_buf_writer_init(&hw, a.u.send_data.header, 32);
        moq_result_t rc = s->profile->encode_fetch_object(s, &hw,
                                                           entry->effective_group_order, &obj_args,
                                                           &entry->prior);
        if (rc < 0) return rc;
        a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
        a.u.send_data.stream_ref = entry->data_stream_ref;
        a.u.send_data.scheduling_priority = UINT64_C(0x10000) |
            ((uint64_t)entry->subscriber_priority << 8) | cfg->publisher_priority;
        a.u.send_data.payload = cfg->payload;
        a.u.send_data.fin = false;

        if (cfg->payload) moq_rcbuf_incref(cfg->payload);

        rc = push_action(s, &a);
        if (rc < 0) {
            if (cfg->payload) moq_rcbuf_decref(cfg->payload);
            return rc;
        }
    }

    entry->prior.has_prev = true;
    entry->prior.has_actual = true;
    entry->prior.group_id = cfg->group_id;
    /* A datagram object has no subgroup and is decoded by the peer as subgroup 0;
     * record 0 (not cfg->subgroup_id, which is ignored) so a later object's
     * prior-subgroup delta resolves to the same value on both ends. */
    entry->prior.subgroup_id = cfg->datagram ? 0 : cfg->subgroup_id;
    entry->prior.object_id = cfg->object_id;
    entry->prior.publisher_priority = cfg->publisher_priority;
    return MOQ_OK;
}

/* Shared end-of-range marker emit: validated by the public entry points, which
 * own their own argument contracts. Rejects before any queueing or cursor
 * mutation, so a refused call leaves the fetch entry byte-identical. */
static moq_result_t fetch_range_marker(
    moq_session_t *s,
    moq_fetch_t fetch,
    moq_fetch_range_kind_t kind,
    uint64_t group_id,
    uint64_t object_id,
    uint64_t now_us)
{

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->fetches[slot].state != MOQ_FETCH_ACCEPTED)
        return MOQ_ERR_WRONG_STATE;

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_fetch_entry_t *entry = &s->fetches[slot];

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;

    moq_buf_writer_t hw;
    moq_buf_writer_init(&hw, a.u.send_data.header, 32);
    moq_result_t rc = s->profile->encode_fetch_range(s, &hw,
                                                       kind, group_id, object_id);
    if (rc < 0) return rc;
    a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);

    a.u.send_data.stream_ref = entry->data_stream_ref;
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = false;

    rc = push_action(s, &a);
    if (rc < 0) return rc;

    /* A range marker advances the prior Location only; prior actual-object
     * metadata (subgroup/priority) is unchanged. */
    entry->prior.has_prev = true;
    entry->prior.group_id = group_id;
    entry->prior.object_id = object_id;
    return MOQ_OK;
}

/* A marker kind the session's profile can write on a fetch stream: Timed-Out
 * only where the profile has an End of Timed-Out Range. */
static bool fetch_range_kind_writable(const moq_session_t *s,
                                      moq_fetch_range_kind_t kind)
{
    return kind == MOQ_FETCH_RANGE_NON_EXISTENT ||
           kind == MOQ_FETCH_RANGE_UNKNOWN ||
           (kind == MOQ_FETCH_RANGE_TIMED_OUT &&
            s->profile->fetch_range_timed_out);
}

moq_result_t moq_session_write_fetch_range(
    moq_session_t *s,
    moq_fetch_t fetch,
    moq_fetch_range_kind_t kind,
    uint64_t group_id,
    uint64_t object_id,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (!fetch_range_kind_writable(s, kind)) return MOQ_ERR_INVAL;
    return fetch_range_marker(s, fetch, kind, group_id, object_id, now_us);
}

moq_result_t moq_session_write_fetch_range_before_group(
    moq_session_t *s,
    moq_fetch_t fetch,
    moq_fetch_range_kind_t kind,
    uint64_t first_group,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (!fetch_range_kind_writable(s, kind)) return MOQ_ERR_INVAL;
    if (first_group == 0) return MOQ_ERR_INVAL;   /* nothing precedes group 0 */
    /* The profile owns the draft's maximum encodable object id; the marker's
     * inclusive Location is the LAST object of the group before first_group. */
    uint64_t max_obj = s->profile->location_varint_max;
    return fetch_range_marker(s, fetch, kind, first_group - 1u, max_obj, now_us);
}

bool moq_session_supports_fetch_datagram(const moq_session_t *s)
{
    if (!s) return false;
    return s->profile->fetch_datagram_supported;
}

bool moq_session_supports_joining_fetch(const moq_session_t *s)
{
    if (!s) return false;
    return s->profile->supports_joining_fetch;
}

bool moq_session_supports_fill(const moq_session_t *s)
{
    if (!s) return false;
    return s->profile->supports_fill;
}

bool moq_session_supports_concurrent_subscriptions(const moq_session_t *s)
{
    if (!s) return false;
    return s->profile->allows_concurrent_subscriptions;
}

moq_result_t moq_session_end_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->fetches[slot].state != MOQ_FETCH_ACCEPTED)
        return MOQ_ERR_WRONG_STATE;

    moq_fetch_entry_t *entry = &s->fetches[slot];

    const bool finish_request = s->profile->request_fin_is_not_cancel;
    if (finish_request) {
        moq_result_t rc = fetch_process_buffered_request(s, slot);
        if (rc < 0) return rc;
        if (!session_is_active(s)) return MOQ_ERR_CLOSED;
        slot = fetch_resolve_handle(s, fetch);
        if (slot < 0) return MOQ_ERR_STALE_HANDLE; /* refused UPDATE won */
        entry = &s->fetches[slot];
        if (entry->rs.recv_len != 0) return MOQ_ERR_WOULD_BLOCK;
    }

    /* The response ends here (§5.2: the publisher can destroy FETCH state once
     * the data stream is closed with FIN). On stream-correlated profiles the
     * request stream has nothing more to carry either, so its local half is
     * FINed in the same commit; while the fetcher's half is still open a drain
     * slot absorbs its later FIN or RESET once the entry is gone. Everything
     * is reserved first: WOULD_BLOCK leaves the fetch untouched. */
    moq_stream_ref_t req_ref = entry->rs.ref;
    bool close_req = req_ref._v != 0;
    bool need_drain = close_req && !fetch_peer_fin_observed(entry);
    if (action_queue_avail(s) < (close_req ? 2u : 1u)) return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_DATA;
    a.detail_size = (uint32_t)sizeof(moq_send_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_data.stream_ref = entry->data_stream_ref;
    a.u.send_data.header_len = 0;
    a.u.send_data.payload = NULL;
    a.u.send_data.fin = true;

    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    if (close_req)
        (void)queue_close_bidi(s, req_ref);   /* reserved above */
    if (need_drain)
        (void)drain_ref_add(s, req_ref);      /* reserved above */
    fetch_free_entry(s, slot);
    return MOQ_OK;
}

moq_result_t moq_session_abort_fetch(
    moq_session_t *s,
    moq_fetch_t fetch,
    uint64_t error_code,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    /* A QUIC application error code travels as a varint on the wire. */
    if (error_code > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_fetch_entry_t *entry = &s->fetches[slot];
    /* Publisher side only, and only once the response data stream exists: a
     * pending fetch has nothing to reset and is answered by reject_fetch. */
    if (entry->role != MOQ_FETCH_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (entry->state != MOQ_FETCH_ACCEPTED || !entry->data_stream_started)
        return MOQ_ERR_WRONG_STATE;

    /* Reserve everything before mutating. The reset is one action. On
     * stream-correlated profiles the request bidi outlives the entry: unless
     * the peer's FIN was already observed, its later empty FIN / RESET must be
     * absorbed by the drain ring (an unknown request stream's empty FIN is
     * otherwise fatal), exactly as reject_fetch arranges. */
    moq_stream_ref_t req_ref = entry->rs.ref;
    bool uses_streams = req_ref._v != 0;
    bool need_drain = uses_streams && !fetch_peer_fin_observed(entry);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    /* Stream-correlated profiles terminate the request stream too (one ABORT
     * of both directions with the same code, §5.3: the request stream's
     * termination is what ends the fetch even when the data stream's reset
     * is lost before its header): both actions are reserved up front so a
     * refusal never leaves a half-aborted fetch. */
    if (action_queue_avail(s) < (size_t)(uses_streams ? 2 : 1))
        return MOQ_ERR_WOULD_BLOCK;

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_DATA;
    a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_data.stream_ref = entry->data_stream_ref;
    a.u.reset_data.error_code = error_code;
    a.u.reset_data.reliable_size = entry->data_header_len;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;
    if (uses_streams) {
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_ABORT_BIDI_STREAM;
        a.detail_size = (uint32_t)sizeof(moq_abort_bidi_stream_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.abort_bidi_stream.stream_ref = req_ref;
        a.u.abort_bidi_stream.error_code = error_code;
        (void)push_action(s, &a);           /* slot reserved above */
    }

    if (need_drain)
        (void)drain_ref_add(s, req_ref);   /* slot reserved above */
    /* Committed: remember the peer's request id so a FETCH_CANCEL that crosses
     * the reset is absorbed (control-message cancellation profiles only; a
     * stream-correlated cancel is a request-bidi reset the session already
     * absorbs). Captured before the free, recorded only on success. */
    if (!moq_session_uses_request_streams(s))
        fetch_abort_tomb_add(s, entry->request_id);
    fetch_free_entry(s, slot);
    return MOQ_OK;
}

moq_result_t moq_session_request_goaway_fetch(
    moq_session_t *s, moq_fetch_t fetch,
    const moq_request_goaway_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < sizeof(moq_request_goaway_cfg_t)) return MOQ_ERR_INVAL;
    if (cfg->new_session_uri.len > 0 && !cfg->new_session_uri.data)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    return session_core_send_request_goaway(s, MOQ_REQUEST_FAMILY_FETCH, slot,
        cfg->new_session_uri.data, cfg->new_session_uri.len, cfg->timeout_ms);
}
