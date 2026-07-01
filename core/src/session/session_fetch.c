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
    moq_fetch_entry_t *e = &s->fetches[slot];
    /* Free any buffered Joining-FETCH staged token values the entry still owns
     * (a PENDING_JOIN freed without release; the memset below only drops the
     * descriptors, not the heap values). */
    if (e->join_token_count > 0)
        process_auth_tokens_free_staging(s, e->join_tokens,
            e->join_token_staged, e->join_token_count);
    request_registry_remove_by_id(s, e->request_id);
    /* Stream-correlated profiles key the request bidi by stream_ref; remove that
     * mapping so a recycled slot never carries a stale key. */
    if (e->request_stream_ref._v != 0)
        request_registry_remove_by_streamref(s, e->request_stream_ref);
    uint32_t next_gen = e->generation + 1;
    /* The request-bidi receive buffer is co-allocated in the session block;
     * preserve the pointer/cap across the reset (memset would null it). */
    uint8_t *recv_buf = e->req_recv_buf;
    size_t   recv_cap = e->req_recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_FETCH_FREE;
    e->generation = next_gen;
    e->req_recv_buf = recv_buf;
    e->req_recv_cap = recv_cap;
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
     * drop only the request-bidi stream-ref mapping (that bidi is GOAWAY-strict
     * drained by the caller), and invalidate the public handle. */
    if (e->request_stream_ref._v != 0) {
        request_registry_remove_by_streamref(s, e->request_stream_ref);
        e->request_stream_ref = moq_stream_ref_from_u64(0);
    }
    e->goaway_sent = false;   /* selective free: clear the migration marker (the
                               * slot is tombstoned, not reusable, until the full
                               * memset free, but keep the invariant uniform) */
    e->state = MOQ_FETCH_GOAWAY_LOCAL;
    e->generation++;
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

/* -- Inbound FETCH handler ----------------------------------------- */

/* Emit a core-generated FETCH REQUEST_ERROR, routed like the auth-reject path: on
 * the request bidi for stream-correlated profiles (draft-18, REQUEST_ERROR + FIN),
 * on the shared control channel otherwise (draft-16). The caller commits the
 * inbound request + auth transaction after this returns MOQ_OK. */
static moq_result_t fetch_auto_reject(moq_session_t *s,
                                      const moq_decoded_fetch_t *d,
                                      uint64_t error_code)
{
    uint8_t err_buf[256];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
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
        moq_result_t rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){ .error_code = error_code });
        if (rc < 0) return rc;
        rc = queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                             moq_buf_writer_offset(&ew), true);
        if (rc < 0) return rc;
        if (need_drain)
            (void)drain_ref_add(s, d->endpoint.stream_ref);   /* slot reserved above */
        return MOQ_OK;
    }
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_result_t rc = s->profile->encode_request_error(s, &ew,
        &(moq_request_error_encode_args_t){
            .request_id = d->request_id, .error_code = error_code });
    if (rc < 0) return rc;
    return queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
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
    if (slot < 0)
        /* Stream-correlated: a fetch-pool shortfall is fatal (mirrors the
         * standalone staging path), never a per-request error. */
        return close_with_error(s, 0x3, "fetch pool full");

    /* Build entry-owned token storage in temporaries first, so a copy failure
     * leaves the fetch slot untouched (still FREE) and the request fully retryable. */
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
                return MOQ_ERR_WOULD_BLOCK;   /* nothing mutated: retryable */
            }
            memcpy(copy, d->tokens[i].token_value.data, len);
            tmp[i].token_value.data = copy;
            tmp_staged[i] = true;
        }
    }

    moq_fetch_entry_t *e = &s->fetches[slot];
    uint32_t live_gen = e->generation | 1;
    e->generation = live_gen;
    e->state = MOQ_FETCH_PENDING_JOIN;
    e->role = MOQ_FETCH_ROLE_PUBLISHER;
    e->handle = (moq_fetch_t){ moq_handle_pack(MOQ_HANDLE_POOL_FETCH,
        s->session_tag, live_gen, (uint32_t)slot) };
    e->request_id = d->request_id;
    e->request_stream_ref = d->endpoint.stream_ref;
    e->req_recv_fin = d->request_fin;
    e->join_fetch_type = (uint8_t)d->fetch_type;
    e->join_request_id = d->joining_request_id;
    e->join_start = d->joining_start;
    e->join_subscriber_priority = d->subscriber_priority;
    e->join_group_order = (moq_group_order_t)d->group_order;
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
    moq_stream_ref_t ref = e->request_stream_ref;
    bool need_drain = ref._v != 0 && !e->req_recv_fin;
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

moq_result_t session_core_pending_joins_can_resolve(moq_session_t *s, uint64_t sub_req_id,
    bool has_largest, uint64_t largest_group,
    size_t extra_actions, size_t extra_drains, size_t extra_send)
{
    size_t n_event = 0, n_action = 0, n_drain = 0, send = 0, rel_scratch = 0;
    bool any_reject = false;
    for (size_t i = 0; i < s->fetch_cap; i++) {
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
            if (e->request_stream_ref._v != 0 && !e->req_recv_fin) n_drain++;
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
    for (size_t i = 0; i < s->fetch_cap; i++) {
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
        uint64_t end_object = sub->largest_object + 1;
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
    for (size_t i = 0; i < s->fetch_cap; i++) {
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
    for (size_t i = 0; i < s->fetch_cap; i++)
        if (s->fetches[i].state == MOQ_FETCH_PENDING_JOIN &&
            s->fetches[i].join_request_id == sub_req_id)
            fetch_free_entry(s, (int)i);   /* frees the entry-owned token storage */
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
        if (jsub < 0 ||
            (s->subs[jsub].state != MOQ_SUB_ESTABLISHED &&
             s->subs[jsub].state != MOQ_SUB_PENDING_PUBLISHER)) {
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
        d->end_object = jsub_e->largest_object + 1;
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
    }

    if (event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    int slot = fetch_find_free(s);
    if (slot < 0) {
        if (d->endpoint.has_stream_ref) {
            /* Stream-correlated profiles: a local resource shortfall (no free
             * fetch slot) is treated as fatal rather than a per-request
             * REQUEST_ERROR. Routes through cleanup_all to release any auth
             * staging/transaction. */
            result = close_with_error(s, 0x3, "fetch pool full");
            goto cleanup_all;
        }
        if (action_queue_full(s)) {
            result = MOQ_ERR_WOULD_BLOCK;
            goto cleanup_all;
        }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = 0x0,
                .reason = (const uint8_t *)"fetch pool full",
                .reason_len = 15 });
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
    e.u.fetch_request.start_group = d->start_group;
    e.u.fetch_request.start_object = d->start_object;
    e.u.fetch_request.end_group = d->end_group;
    e.u.fetch_request.end_object = d->end_object;
    e.u.fetch_request.subscriber_priority = d->subscriber_priority;
    e.u.fetch_request.group_order = d->group_order;
    e.u.fetch_request.tokens = ev_tokens;
    e.u.fetch_request.token_count = d->token_count;

    rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        result = rc;
        goto cleanup_all;
    }

    /* Commit: now safe to mutate entry. */
    entry->generation = live_gen;
    entry->state = MOQ_FETCH_PENDING_PUBLISHER;
    entry->role = MOQ_FETCH_ROLE_PUBLISHER;
    entry->handle = handle;
    entry->request_id = d->request_id;
    d->endpoint.kind = MOQ_REQ_FETCH;
    d->endpoint.slot = slot;
    if (d->endpoint.has_stream_ref) {
        /* Hand the request bidi off from the staging slot to this fetch slot:
         * remove the staging stream-ref mapping, then bind it to (FETCH, slot).
         * The request-stream handler frees the staging slot afterwards (after
         * clearing its request_stream_ref, so this mapping is not removed). */
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
    bool close_half = !free_now && s->fetches[slot].request_stream_ref._v != 0;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_result_t rc;
    if (redirect) {
        rc = session_core_emit_request_redirect(s, MOQ_REQUEST_FAMILY_FETCH,
            s->fetches[slot].handle._opaque, redirect, error_code,
            can_retry, retry_after_ms, reason, reason_len);
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

        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_FETCH_ERROR;
        e.detail_size = (uint32_t)sizeof(moq_fetch_error_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.fetch_error.fetch = s->fetches[slot].handle;
        e.u.fetch_error.error_code = (moq_request_error_t)error_code;
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
            (void)queue_close_bidi(s, s->fetches[slot].request_stream_ref);
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

moq_result_t session_core_on_fetch_ok(moq_session_t *s,
                                       const moq_decoded_fetch_ok_t *d)
{
    moq_fetch_entry_t *e = &s->fetches[d->target_slot];

    if (e->control_response_seen)
        return close_with_error(s, 0x3, "duplicate control response for fetch");

    /* If data FIN already arrived, we need 2 event slots: OK + COMPLETE. */
    size_t need = e->data_stream_fin ? 2 : 1;
    if (event_queue_avail(s) < need) return MOQ_ERR_WOULD_BLOCK;

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

    /* event_queue_avail >= 2 precheck above guarantees the second slot. */
    if (e->data_stream_fin) {
        moq_event_t cev;
        memset(&cev, 0, sizeof(cev));
        cev.kind = MOQ_EVENT_FETCH_COMPLETE;
        cev.detail_size = (uint32_t)sizeof(moq_fetch_complete_event_t);
        cev.borrow_epoch = s->borrow_epoch;
        cev.u.fetch_complete.fetch = e->handle;
        moq_result_t crc = push_event(s, &cev);
        if (crc < 0) return crc;
        fetch_free_entry(s, d->target_slot);
    }

    return MOQ_OK;
}

/* -- Public API ---------------------------------------------------- */

void moq_fetch_cfg_init(moq_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_fetch_cfg_t);
}

void moq_accept_fetch_cfg_init(moq_accept_fetch_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_accept_fetch_cfg_t);
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

    /* Track Properties on FETCH_OK are carried only by stream-correlated
     * profiles (draft-18); reject rather than silently drop them elsewhere. */
    const uint8_t *fetch_props = NULL;
    size_t fetch_props_len = 0;
    if (ACCEPT_FETCH_HAS(track_properties) && cfg->track_properties.len > 0) {
        if (!cfg->track_properties.data) return MOQ_ERR_INVAL;
        if (!moq_session_uses_request_streams(s)) return MOQ_ERR_INVAL;
        fetch_props = cfg->track_properties.data;
        fetch_props_len = cfg->track_properties.len;
    }
#undef ACCEPT_FETCH_HAS
#undef ACCEPT_FETCH_CFG_MIN

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = fetch_resolve_handle(s, fetch);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->fetches[slot].state != MOQ_FETCH_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    /* Stream-correlated profiles deliver FETCH_OK on the request bidi, then open
     * the response data uni with a FETCH_HEADER (carrying the Request ID). */
    if (moq_session_uses_request_streams(s)) {
        /* Two actions: FETCH_OK on the request bidi + FETCH_HEADER on the uni. */
        if (action_queue_avail(s) < 2) return MOQ_ERR_WOULD_BLOCK;
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
        rc = s->profile->encode_fetch_header(s, &hw,
                                             s->fetches[slot].request_id);
        if (rc < 0) return rc;
        uint8_t hdr_len = (uint8_t)moq_buf_writer_offset(&hw);

        /* FETCH_OK on the request bidi (stays open for the subscription). Post-GOAWAY
         * gate (§10.4): a migrated request accepts no further request-bidi bytes
         * (defensive — accept on an already-ACCEPTED+migrated fetch is unreachable). */
        if (request_goaway_already_sent(s, s->fetches[slot].request_stream_ref))
            return MOQ_ERR_WRONG_STATE;
        moq_action_t ok_act;
        memset(&ok_act, 0, sizeof(ok_act));
        ok_act.kind = MOQ_ACTION_SEND_BIDI_STREAM;
        ok_act.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
        ok_act.borrow_epoch = s->borrow_epoch;
        ok_act.u.send_bidi_stream.stream_ref =
            s->fetches[slot].request_stream_ref;
        ok_act.u.send_bidi_stream.data = s->send_buf + s->send_len;
        ok_act.u.send_bidi_stream.len = ok_len;
        ok_act.u.send_bidi_stream.fin = false;
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
        data_act.u.send_data.fin = cfg->empty;
        rc = push_action(s, &data_act);
        if (rc < 0) return rc;

        s->fetches[slot].state = MOQ_FETCH_ACCEPTED;
        s->fetches[slot].data_stream_ref = data_act.u.send_data.stream_ref;
        s->fetches[slot].data_stream_started = true;
        s->next_stream_ref++;
        if (cfg->empty)
            fetch_free_entry(s, slot);
        return MOQ_OK;
    }

    /* Pre-check: need 2 action slots before any encoding/mutation. */
    if (action_queue_avail(s) < 2) return MOQ_ERR_WOULD_BLOCK;

    /* Encode both outputs before pushing either action. */
    moq_accept_fetch_encode_args_t ok_args = {
        .request_id = s->fetches[slot].request_id,
        .end_of_track = cfg->end_of_track,
        .end_group = cfg->end_group,
        .end_object = cfg->end_object,
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
    rc = push_action(s, &data_act);
    if (rc < 0) return rc;

    /* Commit. */
    moq_fetch_entry_t *entry = &s->fetches[slot];
    entry->state = MOQ_FETCH_ACCEPTED;
    entry->data_stream_ref = data_act.u.send_data.stream_ref;
    entry->data_stream_started = true;
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
#define FETCH_CFG_MIN offsetof(moq_fetch_cfg_t, auth_tokens)
    if (cfg->struct_size < FETCH_CFG_MIN) return MOQ_ERR_INVAL;
#define FETCH_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_fetch_cfg_t, f) + sizeof(cfg->f))
    *out_handle = MOQ_FETCH_INVALID;

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (FETCH_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;
#undef FETCH_CFG_HAS
#undef FETCH_CFG_MIN

    session_begin_advance(s, now_us);

    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    uint64_t joining_wire_request_id = 0;
    uint64_t joining_computed_start_group = 0;
    if (cfg->is_joining) {
        int jsub = sub_resolve_handle(s, cfg->joining_sub);
        if (jsub < 0) return MOQ_ERR_STALE_HANDLE;
        if (s->subs[jsub].state != MOQ_SUB_ESTABLISHED &&
            s->subs[jsub].state != MOQ_SUB_PENDING_SUBSCRIBER)
            return MOQ_ERR_WRONG_STATE;
        if (s->subs[jsub].filter_type != MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT)
            return MOQ_ERR_INVAL;
        /* The joining start is derived from the subscription's *current* largest
         * location. has_largest is stamped only by SUBSCRIBE_OK / accept (at
         * ESTABLISHED) and cleared on slot alloc/free, so a pending or reused
         * subscription has no current largest and is rejected here -- it can
         * never carry a prior occupant's stale largest into this computation. */
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
        if (moq_validate_full_track_name(&cfg->track_namespace,
                                          cfg->track_name) < 0)
            return MOQ_ERR_INVAL;
        if (cfg->end_group < cfg->start_group)
            return MOQ_ERR_INVAL;
        if (cfg->end_group == cfg->start_group && cfg->end_object > 0 &&
            cfg->end_object <= cfg->start_object)
            return MOQ_ERR_INVAL;
    }
    if (cfg->group_order != MOQ_GROUP_ORDER_DEFAULT &&
        cfg->group_order != MOQ_GROUP_ORDER_ASCENDING &&
        cfg->group_order != MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
    /* A profile whose fetch data plane cannot reconstruct a descending response
     * (draft-18: ascending-only group deltas) must not request one it would
     * mis-decode; refuse up front rather than desync on the reply. */
    if (cfg->group_order == MOQ_GROUP_ORDER_DESCENDING &&
        !s->profile->fetch_descending_supported)
        return MOQ_ERR_INVAL;

    bool req_stream = moq_session_uses_request_streams(s);
    moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0);

    moq_request_endpoint_t req_ep;
    {
        moq_result_t prc = s->profile->prepare_request(s, &req_ep);
        if (prc < 0) return prc;
    }

    int slot = fetch_find_free(s);
    if (slot < 0) {
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_WOULD_BLOCK;
    }

    if (action_queue_full(s)) {
        s->profile->abort_request(s, &req_ep);
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
        };

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);

        moq_result_t rc2 = s->profile->encode_fetch(s, &w, &args);
        if (rc2 < 0) {
            s->profile->abort_request(s, &req_ep);
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
            return arc;
        }
        s->send_len += encoded_len;
    }

    /* Commit. */
    moq_fetch_entry_t *entry = &s->fetches[slot];
    entry->generation |= 1;
    entry->state = MOQ_FETCH_PENDING_FETCHER;
    entry->role = MOQ_FETCH_ROLE_FETCHER;
    entry->request_id = req_ep.request_id;
    entry->handle = fetch_make_handle(s, (size_t)slot);
    entry->start_group = cfg->is_joining ? joining_computed_start_group : cfg->start_group;
    entry->start_object = cfg->is_joining ? 0 : cfg->start_object;
    req_ep.kind = MOQ_REQ_FETCH;
    req_ep.slot = slot;
    if (req_stream) {
        /* The FETCH_OK/REQUEST_ERROR response correlates by the request bidi
         * stream identity; the response data uni (FETCH_HEADER) carries the
         * Request ID, so register BOTH keys. fetch_free_entry removes both. */
        entry->request_stream_ref = req_ref;
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
    moq_stream_ref_t req_ref = s->fetches[slot].request_stream_ref;
    bool need_drain = req_ref._v != 0 && !s->fetches[slot].req_recv_fin;
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
            if (request_goaway_already_sent(s, s->fetches[slot].request_stream_ref))
                return MOQ_ERR_WRONG_STATE;
            act.kind = MOQ_ACTION_SEND_BIDI_STREAM;
            act.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
            act.u.send_bidi_stream.stream_ref =
                s->fetches[slot].request_stream_ref;
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
 * request bidi, STOP the response data uni if it is open, drain the request bidi
 * (drain_refs), then free the fetch slot and record the request id in the
 * cancel-tombstone cache so an in-flight data uni is absorbed rather than treated
 * as an unknown request (which would close the session). Reserve-before-mutate
 * (WOULD_BLOCK leaves state intact). Shared by moq_session_fetch_cancel and the
 * UNSUPPORTED_EXTENSION (0x33) path; the caller validated role/state and
 * request-stream use, and does not depend on session_begin_advance (safe inside
 * an inbound advance). */
static moq_result_t fetch_request_bidi_cancel(moq_session_t *s, int slot)
{
    moq_fetch_entry_t *e = &s->fetches[slot];
    bool stop_data = e->data_stream_started && !e->data_stream_fin;
    if (action_queue_avail(s) < (size_t)(2 + (stop_data ? 1 : 0)))
        return MOQ_ERR_WOULD_BLOCK;
    if (e->request_stream_ref._v != 0 &&
        s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    moq_stream_ref_t ref = e->request_stream_ref;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_STOP_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_stop_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.stop_bidi_stream.stream_ref = ref;
    a.u.stop_bidi_stream.error_code = 0x1;   /* CANCELLED */
    moq_result_t arc = push_action(s, &a);
    if (arc < 0) return arc;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_RESET_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_reset_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.reset_bidi_stream.stream_ref = ref;
    a.u.reset_bidi_stream.error_code = 0x1;
    arc = push_action(s, &a);
    if (arc < 0) return arc;
    if (stop_data) {
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_STOP_DATA;
        a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.stop_data.stream_ref = e->data_stream_ref;
        a.u.stop_data.error_code = 0x1;
        arc = push_action(s, &a);
        if (arc < 0) return arc;
    }
    if (e->request_stream_ref._v != 0) {
        request_registry_remove_by_streamref(s, e->request_stream_ref);
        (void)drain_ref_add(s, ref);   /* slot reserved above */
        e->request_stream_ref = moq_stream_ref_from_u64(0);
    }
    /* The request bidi is drained via drain_refs; free the fetch slot now and
     * tombstone the request id so a late data uni (FETCH_HEADER) is stopped and
     * absorbed without reoccupying the pool. */
    fetch_cancel_tomb_add(s, e->request_id);
    fetch_free_entry(s, slot);
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
     * the data uni if open), keeping the entry as a request-id tombstone. */
    if (moq_session_uses_request_streams(s))
        return fetch_request_bidi_cancel(s, slot);

    uint8_t buf[32];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_fetch_cancel(s, &w, e->request_id);
    if (rc < 0) return rc;

    rc = queue_send_control(s, buf, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;

    /* Free the fetch slot immediately so max_fetches is reusable; record the
     * request id in the bounded cancel-tombstone cache so a late FETCH_OK /
     * REQUEST_ERROR / data stream for it is absorbed without reoccupying the
     * pool or closing the session. */
    fetch_cancel_tomb_add(s, e->request_id);
    fetch_free_entry(s, slot);
    return MOQ_OK;
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
    if (payload_len > MOQ_QUIC_VARINT_MAX) return MOQ_ERR_INVAL;
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
        moq_result_t rc = s->profile->encode_fetch_object(s, &hw1, &hdr_args,
                                                           &entry->prior);
        if (rc < 0) return rc;
        a1.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw1);
        a1.u.send_data.stream_ref = entry->data_stream_ref;
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
        moq_result_t rc = s->profile->encode_fetch_object(s, &hw, &obj_args,
                                                           &entry->prior);
        if (rc < 0) return rc;
        a.u.send_data.header_len = (uint8_t)moq_buf_writer_offset(&hw);
        a.u.send_data.stream_ref = entry->data_stream_ref;
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

moq_result_t moq_session_write_fetch_range(
    moq_session_t *s,
    moq_fetch_t fetch,
    moq_fetch_range_kind_t kind,
    uint64_t group_id,
    uint64_t object_id,
    uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (kind != MOQ_FETCH_RANGE_NON_EXISTENT && kind != MOQ_FETCH_RANGE_UNKNOWN)
        return MOQ_ERR_INVAL;

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

    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    moq_fetch_entry_t *entry = &s->fetches[slot];

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
