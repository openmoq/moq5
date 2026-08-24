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

/*
 * Allocation-free duplicate detection: compare the DECODED identity
 * against each live entry's stored canonical key in place, so selection
 * never allocates. The key-blob form below serves the outbound paths,
 * which already hold a built key.
 */
static bool sub_is_duplicate_track_id(moq_session_t *s,
                                       const moq_namespace_t *ns,
                                       moq_bytes_t name,
                                       moq_sub_role_t role)
{
    for (size_t i = 0; i < s->sub_cap; i++) {
        moq_sub_entry_t *e = &s->subs[i];
        if (e->state == MOQ_SUB_FREE || e->state == MOQ_SUB_TERMINATED)
            continue;
        if (e->role != role) continue;
        if (moq_track_key_matches(e->track_id_buf, e->track_id_len, ns, name))
            return true;
    }
    return false;
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
    /* Only allocated subscriptions can be forwarding-pending, and only those
     * are linked, so the early-exit search no longer tracks the matching
     * owner's physical slot. */
    for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next) {
        moq_sub_entry_t *e = &s->subs[i];
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_fwd_pending_probes++;
#endif
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

const moq_resolved_window_t *moq_session_sub_resolved_window(
    moq_session_t *s, moq_subscription_t sub)
{
    if (!s) return NULL;
    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return NULL;
    moq_sub_entry_t *e = &s->subs[slot];
    if (e->role != MOQ_SUB_ROLE_PUBLISHER || !e->window.has_window) return NULL;
    return &e->window;
}

static void sub_free_entry(moq_session_t *s, size_t slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    sub_occ_unlink(s, slot);
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
    e->update_has_forward = false;
    e->update_forward = false;
    e->update_has_filter = false;
    e->update_filter_type = 0;
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
    /* Release the reserved registry record exactly once (empty reservations are
     * reclaimed here; observed records stay pinned). Clear the resolved window
     * and captured raw filter so a recycled slot carries no stale state. */
    if (e->hist) {
        track_hist_release(s, e->hist);
        e->hist = NULL;
    }
    memset(&e->window, 0, sizeof(e->window));
    e->req_start_group = 0;
    e->req_start_object = 0;
    e->req_end_group = 0;
    /* Release the owned deferred-done reason exactly once and clear the
     * Stream-Count gating state so a recycled slot starts ungated. */
    if (e->done_reason_buf) {
        s->alloc.free(e->done_reason_buf, e->done_reason_len, s->alloc.ctx);
        e->done_reason_buf = NULL;
    }
    e->done_reason_len = 0;
    e->done_pending = false;
    e->done_status_code = 0;
    e->done_stream_count = 0;
    e->processed_stream_count = 0;
    e->done_deadline_us = 0;
    e->done_expired = false;
    e->dt_pub_has_object = e->dt_pub_has_subgroup = false;
    e->dt_pub_object_ms = e->dt_pub_subgroup_ms = 0;
    e->dt_sub_has_object = e->dt_sub_has_subgroup = false;
    e->dt_sub_object_ms = e->dt_sub_subgroup_ms = 0;
    e->dt_upd_has_object = e->dt_upd_has_subgroup = false;
    e->dt_upd_object_ms = e->dt_upd_subgroup_ms = 0;
    sub_alias_index_clear(s, slot);   /* remove alias while still ESTABLISHED */
    e->state = MOQ_SUB_FREE;
    e->generation++;

    /* Freeing a pending subscription may leave no forwarding subscriber
     * pending, in which case any data held for an as-yet-unestablished alias
     * can never be matched -- discard it (no-op while one remains). */
    session_discard_staged_if_no_pending(s);
}

/* Finalize a deferred subscriber-role done: the advertised Stream Count is
 * satisfied -- or the terminal deadline EXPIRED and the reaper's
 * scan/stop/rescan loop found no live bound streams left (§9.8) -- so
 * surface the retained SUBSCRIBE_DONE and dispose of the entry.
 * Retryable: nothing is mutated until the event is queued (queue/scratch
 * pressure leaves the deferral intact for sub_reap_deferred_dones or the next
 * processed stream). Control-correlated profiles (no request bidi) free the
 * entry immediately; stream-correlated profiles free it if the bidi's FIN
 * already arrived, otherwise the entry drains as TERMINATED and the
 * request-stream handler frees it on FIN (the local send half was already
 * closed when the terminal was accepted). Mirrors pub_finalize_done -- minus
 * the drain-ref, because the entry (and its by-streamref key) is retained
 * until the FIN rather than freed early. */
static moq_result_t sub_finalize_done(moq_session_t *s, int slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t reason = {0};
    if (e->done_reason_len > 0) {
        reason.data = event_scratch_copy(s, e->done_reason_buf,
                                         e->done_reason_len);
        reason.len = e->done_reason_len;
        if (!reason.data) {
            s->event_scratch_len = scratch_saved;
            /* Empty scratch that still cannot hold the reason is permanently
             * too small: fail terminally (mirrors the immediate path) instead
             * of retrying the deferred done forever. */
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
    ev.u.subscribe_done.sub = e->handle;
    ev.u.subscribe_done.status_code = e->done_status_code;
    ev.u.subscribe_done.stream_count = e->done_stream_count;
    ev.u.subscribe_done.reason = reason;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }

    if (e->request_stream_ref._v == 0 || e->req_recv_fin) {
        sub_free_entry(s, (size_t)slot);   /* frees the owned reason */
    } else {
        e->done_pending = false;   /* reason stays owned; freed on the FIN */
        sub_alias_index_clear(s, (size_t)slot);
        e->state = MOQ_SUB_TERMINATED;
    }
    return MOQ_OK;
}

void sub_note_stream_processed(moq_session_t *s, moq_subscription_t sub)
{
    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return;
    moq_sub_entry_t *e = &s->subs[slot];
    if (e->role != MOQ_SUB_ROLE_SUBSCRIBER) return;
    e->processed_stream_count++;
    /* §9.8: direct count-based finalization is legal only in GATED. Once
     * EXPIRED, the count is merely recorded -- finalization belongs to the
     * reaper's scan-before-finalize loop (a FIN consumed while expiry
     * stops sit action-blocked must not finalize past unstopped streams).
     * The sentinel count is never satisfiable by counting. */
    if (e->done_pending && !e->done_expired &&
        e->done_stream_count != MOQ_QUIC_VARINT_MAX &&
        e->processed_stream_count >= e->done_stream_count)
        (void)sub_finalize_done(s, slot);
}

/* Subscription-pool half of the deferred-completion sweep. Same resumption
 * contract as pub_reap_deferred_dones_resumable(): resumes at s->sweep_slot,
 * evaluates against s->sweep_now_us, skips non-runnable owners UNCHARGED, charges
 * the due-mark / stop / finalize transitions separately, and
 * returns false when it suspended with sweep_slot left on the pending owner. */
/* See PUB_SWEEP_ADVANCE: advance unless a free already moved the cursor. */
#define SUB_SWEEP_ADVANCE()                                                   \
    do { if (s->sweep_slot == i)                                              \
             s->sweep_slot = (nxt >= 0) ? (size_t)nxt : s->sub_cap; } while (0)

bool sub_reap_deferred_dones_resumable(moq_session_t *s, uint32_t *budget)
{
    /*
     * Walks the subscription OCCUPANCY list, not the pool -- the mirror of
     * pub_reap_deferred_dones_resumable(); see the note there.
     */
    while (s->sweep_slot < s->sub_cap) {
        if (s->state == MOQ_SESS_CLOSED) return true;  /* finalize may close */

        size_t i = s->sweep_slot;
        int32_t nxt = s->subs[i].occ_next;
        moq_sub_entry_t *e = &s->subs[i];
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_sub_reap_probes++;
#endif
        if (!e->done_pending) {            /* costs nothing; never suspends */
            SUB_SWEEP_ADVANCE();
            continue;
        }
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_sub_reap_pending++;
#endif

        /* A suspension inside STOP_STREAMS or FINALIZE resumes in that phase:
         * SELECT already qualified this owner, and re-running it could reach a
         * different verdict now that earlier transitions have committed. */
        if (s->sweep_phase == MOQ_SWEEP_PHASE_SELECT) {
            /* SELECT: an owner with an unsatisfied count and a future deadline has
             * no runnable work and skips UNCHARGED, so a pool of future owners
             * cannot exhaust the budget and suspend an idle pass. */
            bool count_satisfied =
                (e->done_stream_count != MOQ_QUIC_VARINT_MAX &&
                 e->processed_stream_count >= e->done_stream_count);
            bool due = (s->sweep_now_us >= e->done_deadline_us);
            if (!e->done_expired && !count_satisfied && !due) {
                SUB_SWEEP_ADVANCE();
                continue;
            }

            if (count_satisfied && !e->done_expired) {
                if (budget) {
                    if (*budget == 0) return false;
                    (*budget)--;
                }
                (void)sub_finalize_done(s, (int)i);
                SUB_SWEEP_ADVANCE();
                continue;
            }

            if (!e->done_expired) {
                /* Durable due-mark: its own charged transition, fired once. */
                if (budget) {
                    if (*budget == 0) return false;
                    (*budget)--;
                }
                e->done_expired = true;
            }
        }

        /* EXPIRED: scan, stop, rescan-to-zero, then finalize. Action
         * backpressure leaves the entry EXPIRED (blocked work -> the post-sweep
         * retry deadline); an event-blocked finalize likewise -- the next pass
         * re-enters at the scan. Each STOP attempt is one unit, tracked by
         * s->sweep_rx_pos so a suspended scan resumes mid-pool. */

        /* STOP_STREAMS: branch on the PERSISTED phase. Assigning it here
         * unconditionally would make a suspension at the finalize check rescan
         * the rx pool on re-entry instead of finalizing. */
        if (s->sweep_phase == MOQ_SWEEP_PHASE_SELECT ||
            s->sweep_phase == MOQ_SWEEP_PHASE_STOP_STREAMS) {
            if (s->sweep_phase == MOQ_SWEEP_PHASE_SELECT)
                /* A FRESH rx scan for this owner starts at the rx occupancy
                 * head; a RESUMED one keeps the position it suspended at. */
                s->sweep_rx_pos = moq_occ_first(s->rx_occ_head, s->rx_cap);
            s->sweep_phase = MOQ_SWEEP_PHASE_STOP_STREAMS;
            moq_result_t src = session_stop_bound_streams_resumable(s, e->handle,
                                                     MOQ_PUBLICATION_INVALID, budget);
            if (src == MOQ_SESSION_SUSPENDED)
                return false;              /* rx cursor stays: we resume here */
            if (src < 0) {
                /* Action capacity: this owner is abandoned for now, so its rx
                 * cursor must NOT leak into the next owner's scan. */
                session_sweep_owner_reset(s);
                SUB_SWEEP_ADVANCE();
                continue;
            }
            /* Owner's rx scan is complete; the next phase owns a clean cursor. */
            s->sweep_rx_pos = 0;
            s->sweep_rx_found = false;
            s->sweep_phase = MOQ_SWEEP_PHASE_FINALIZE;
        }

        if (budget) {
            if (*budget == 0) return false;   /* resume AT finalize */
            (*budget)--;
        }
        (void)sub_finalize_done(s, (int)i);
        session_sweep_owner_reset(s);
        SUB_SWEEP_ADVANCE();
    }
    s->sweep_phase = MOQ_SWEEP_PHASE_SELECT;
    return true;
}

#undef SUB_SWEEP_ADVANCE


/* The canonical full-track-name key builder lives in session_track_hist.c
 * (moq_build_track_key) and is shared so subscription request identities and
 * history-registry keys cannot drift. This thin alias keeps the local call
 * sites readable. */
static uint8_t *build_track_id(moq_session_t *s,
                                const moq_namespace_t *ns,
                                moq_bytes_t name,
                                size_t *out_len)
{
    return moq_build_track_key(s, ns, name, out_len);
}

/* -- Message parameter validation ---------------------------------- */

/* -- Subscribe handlers -------------------------------------------- */

/*
 * Queue a terminal REQUEST_ERROR for an inbound SUBSCRIBE that cannot be
 * accepted -- uniformly across ALL reject reasons (auth-token, duplicate-track,
 * subscription-pool-full, history-cap). Pre-commit: encode the error, queue it
 * on the request bidi (stream-correlated) or the control channel, keep the bidi
 * DRAINABLE (`reject_drain`) so a still-open subscriber's trailing FIN is
 * absorbed by the drain ring -- unless the FIN already arrived with the request
 * (`reject_drain` false then, so no drain slot is consumed/leaked), free any
 * reserved staging slot, then commit the inbound request + auth transaction.
 * NOTHING else is committed (no event, entry, or history). On MOQ_OK the caller
 * marks auth committed; on WOULD_BLOCK (capacity shortfall) nothing was
 * committed, so cleanup_all aborts the txn and the whole reject replays.
 * `reason` may be NULL.
 */
static moq_result_t sub_reject_terminal(moq_session_t *s,
    moq_decoded_subscribe_t *d, int reserved_slot, bool reject_drain,
    uint64_t error_code, const char *reason, size_t reason_len)
{
    if (reject_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    uint8_t err_buf[256];
    moq_buf_writer_t ew;
    moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
    moq_result_t rc = s->profile->encode_request_error(s, &ew,
        &(moq_request_error_encode_args_t){
            /* request_id is used only by the control-channel (non-stream) form. */
            .request_id = d->endpoint.has_stream_ref ? 0 : d->request_id,
            .error_code = error_code,
            .reason = (const uint8_t *)reason, .reason_len = reason_len });
    if (rc < 0) return rc;
    rc = d->endpoint.has_stream_ref
        ? queue_send_bidi(s, d->endpoint.stream_ref, err_buf,
                          moq_buf_writer_offset(&ew), true)
        : queue_send_control(s, err_buf, moq_buf_writer_offset(&ew));
    if (rc < 0) return rc;
    if (reject_drain)
        (void)drain_ref_add(s, d->endpoint.stream_ref);
    if (reserved_slot >= 0) sub_free_entry(s, (size_t)reserved_slot);
    s->profile->commit_inbound_request(s, &d->endpoint);
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    return MOQ_OK;
}

moq_result_t session_core_on_subscribe(moq_session_t *s,
                                        moq_decoded_subscribe_t *d,
                                        int reserved_slot,
                                        bool request_fin)
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
    /* Reserved registry record; released on any pre-commit failure via
     * cleanup_all, or stored on the entry at commit. */
    moq_track_hist_t *hist = NULL;
    size_t scratch_saved = s->event_scratch_len;
    /* Stream-correlated rejects keep the request bidi drainable so the rejected
     * subscriber's trailing FIN is absorbed -- but only when the FIN has not
     * already arrived with the request (then it is consumed in this call and a
     * drain slot would leak). Mirrors the TRACK_STATUS reject. */
    bool reject_drain = d->endpoint.has_stream_ref && !request_fin;

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
        result = sub_reject_terminal(s, d, reserved_slot, reject_drain,
                                     d->auth_reject_code, NULL, 0);
        if (result == MOQ_OK) auth_committed = true;
        goto cleanup_all;
    }

    /* Selection is allocation-free: duplicate detection and the history
     * decision both compare the decoded identity against stored canonical
     * keys in place. The request's own identity key is built only in
     * funded execution below, once every selected rejection is behind us. */
    if (sub_is_duplicate_track_id(s, &d->track_namespace, d->track_name,
                                  MOQ_SUB_ROLE_PUBLISHER)) {
        result = sub_reject_terminal(s, d, reserved_slot, reject_drain,
                                     0x19, "duplicate subscription", 22);
        if (result == MOQ_OK) auth_committed = true;
        goto cleanup_all;
    }

    int slot = reserved_slot >= 0 ? reserved_slot : sub_find_free(s);
    if (slot < 0) {
        result = sub_reject_terminal(s, d, reserved_slot, reject_drain,
                                     0x0, "subscription pool full", 22);
        if (result == MOQ_OK) auth_committed = true;
        goto cleanup_all;
    }

    /* Reserve this track's registry record. Select first (allocation-free,
     * comparing the decoded identity against stored canonical keys), so a
     * genuinely full registry and an allocation failure stay distinct: a
     * full registry is NOT a session-fatal parser error and rejects the
     * well-formed request with REQUEST_ERROR(INTERNAL_ERROR 0x0) via the
     * shared terminal-reject path -- nothing (no event, entry, or history)
     * is committed until the error is queued, the bidi stays drainable so
     * the session survives the trailing FIN, and WOULD_BLOCK leaves the
     * whole reject replayable -- while a failed record-key copy reports
     * MOQ_ERR_NOMEM and queues no response at all. */
    if (moq_track_hist_select(s, &d->track_namespace, d->track_name) ==
            MOQ_TH_SEL_FULL) {
        result = sub_reject_terminal(s, d, reserved_slot, reject_drain,
                                     0x0, "track history full", 18);
        if (result == MOQ_OK) auth_committed = true;
        goto cleanup_all;
    }
    /* Funded execution: the entry's retained identity key, then the
     * registry reservation. Both allocation failures are MOQ_ERR_NOMEM
     * with no response queued. */
    tid = build_track_id(s, &d->track_namespace, d->track_name, &tid_len);
    if (tid_len > 0 && !tid) {
        result = MOQ_ERR_NOMEM;
        goto cleanup_all;
    }
    {
        moq_result_t hrc = track_hist_reserve_selected(
            s, &d->track_namespace, d->track_name, &hist);
        if (hrc < 0) { result = hrc; goto cleanup_all; }
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
    /* Allocated: join the occupancy list the preamble scans walk. */
    sub_occ_link(s, (size_t)slot);
    entry->state = MOQ_SUB_PENDING_PUBLISHER;
    entry->role = MOQ_SUB_ROLE_PUBLISHER;
    entry->handle = handle;
    entry->request_id = d->request_id;
    entry->track_alias = 0;
    entry->track_id_buf = tid;
    entry->track_id_len = tid_len;
    entry->hist = hist;
    entry->delivery_timeout_us = d->delivery_timeout_us;
    entry->dt_sub_has_object   = d->dt_has_object;
    entry->dt_sub_object_ms    = d->dt_object_ms;
    entry->dt_sub_has_subgroup = d->dt_has_subgroup;
    entry->dt_sub_subgroup_ms  = d->dt_subgroup_ms;
    entry->filter_type = d->has_filter ? d->filter_type : 0;
    /* Capture the raw SUBSCRIBE filter window so moq_session_accept_subscribe can
     * resolve it against the accept snapshot; only meaningful for the
     * ABSOLUTE_* filters, harmless zeros otherwise. */
    entry->req_start_group = d->start_group;
    entry->req_start_object = d->start_object;
    entry->req_end_group = d->end_group;
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
    /* Release the reserved record on every failure path (it is only stored on
     * the entry at the committed return above; reaching here means it was not). */
    if (hist) track_hist_release(s, hist);
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

    /* Publisher side: this bidi carried the peer's FETCH and nothing else. Only
     * its FIN may follow, whether it arrives on the wire or was handed over by
     * the commit that re-keyed the bidi onto this entry. The durable latch takes
     * it and the owner is RETAINED -- the response, and for an accepted fetch the
     * data stream, are still to come -- so this cannot block. The FETCHER
     * response decoder below is never reached from here. */
    if (fe->role == MOQ_FETCH_ROLE_PUBLISHER) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes on fetch request bidi");
        if (fin || fe->handoff_fin_pending) {
            fe->req_recv_fin = true;
            fe->handoff_fin_pending = false;
        }
        return MOQ_OK;
    }

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
    /* A FIN that rode the creating TRACK_STATUS was handed to this entry by the
     * commit that re-keyed the bidi; it is the same fact as a wire FIN. */
    bool fin_now = fin || te->handoff_fin_pending;

    /* Publisher side: only the requester's FIN may follow its request. */
    if (te->role == MOQ_TS_ROLE_PUBLISHER) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes on track-status request bidi");
        /* The durable latch takes the FIN over; the owner is retained so the
         * application can still accept or reject. This cannot block. */
        if (fin_now) {
            te->req_recv_fin = true;
            te->handoff_fin_pending = false;
        }
        return MOQ_OK;
    }

    /* Requester side, draining after the terminal response: absorb the FIN. */
    if (te->state == MOQ_TS_DRAINING_RESPONSE) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes after terminal track-status response");
        if (fin_now) ts_free_entry(s, tslot);
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
    /* A FIN that rode the creating SUBSCRIBE_TRACKS was handed to this entry by
     * the commit that re-keyed the bidi; from here it is the same observed
     * close as a wire FIN. It is NOT copied into req_recv_fin on the way in:
     * the cancellation below can refuse on event capacity, and the fact must
     * survive that refusal so an empty re-feed completes it exactly once. */
    bool fin_now = fin || e->handoff_fin_pending;

    /* Draining after a terminal REQUEST_ERROR: absorb trailing bytes/FIN. */
    if (e->state == MOQ_TRACK_SUB_DRAINING_RESPONSE) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes after terminal subscribe-tracks response");
        if (fin_now) track_sub_free_entry(s, slot);
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
        bool peer_fin = track_sub_peer_fin_observed(e);
        size_t consumed = 0;
        moq_result_t rc = as_response
            ? s->profile->process_response_stream(s, stream_ref, slot,
                  (uint32_t)MOQ_REQ_SUBSCRIBE_TRACKS,
                  e->req_recv_buf, e->req_recv_len, peer_fin, &consumed)
            : s->profile->process_request_stream(s, stream_ref, slot,
                  e->req_recv_buf, e->req_recv_len, peer_fin, &consumed);
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;  /* keep buffer; re-feed retries */
        if (rc < 0) return rc;

        if (consumed == 0) {
            if (e->req_recv_len > 0) {
                if (peer_fin)
                    return close_with_error(s, 0x3,
                        "truncated message on subscribe-tracks stream");
                return MOQ_OK;            /* incomplete; wait for more bytes */
            }
            /* No buffered bytes: a clean FIN cancels the subscription. A
             * refusal on event capacity leaves the marker set for the re-feed;
             * a completion frees the entry, whose reset clears both facts. */
            if (peer_fin)
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
            if (e->state == MOQ_TRACK_SUB_DRAINING_RESPONSE &&
                track_sub_peer_fin_observed(e))
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
    /* A FIN that rode the creating PUBLISH was handed to this entry by the
     * commit that re-keyed the bidi; from here it is the same fact as a FIN
     * arriving on the wire. It stays in the marker -- not in req_recv_fin --
     * until this handler durably absorbs it, so a teardown that blocks on
     * event capacity is resumed by an empty re-feed and completes once. */
    bool fin_now = fin || e->handoff_fin_pending;

    /* Draining after a terminal (PUBLISH_DONE / REQUEST_ERROR): absorb FIN. */
    if (e->state == MOQ_PUB_DRAINING_RESPONSE) {
        if (len > 0)
            return close_with_error(s, 0x3,
                "extra bytes after terminal publish message");
        if (fin_now) pub_free_entry(s, slot);
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
        /* The durable latch takes the FIN over here; the transitional marker
         * has done its job. */
        if (fin_now) {
            e->req_recv_fin = true;
            e->handoff_fin_pending = false;
        }
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
        bool peer_fin = e->req_recv_fin || e->handoff_fin_pending;
        size_t consumed = 0;
        moq_result_t rc = as_response
            ? s->profile->process_response_stream(s, stream_ref, slot,
                  (uint32_t)MOQ_REQ_PUBLISH,
                  e->req_recv_buf, e->req_recv_len, peer_fin, &consumed)
            : s->profile->process_request_stream(s, stream_ref, slot,
                  e->req_recv_buf, e->req_recv_len, peer_fin, &consumed);
        if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;
        if (rc == MOQ_ERR_WOULD_BLOCK) return rc;  /* keep buffer; re-feed retries */
        if (rc < 0) return rc;

        if (consumed == 0) {
            if (e->req_recv_len > 0) {
                if (peer_fin)
                    return close_with_error(s, 0x3,
                        "truncated message on publish stream");
                return MOQ_OK;            /* incomplete; wait for more bytes */
            }
            /* No buffered bytes: a clean FIN tears the publication down --
             * unless a PUBLISH_DONE was just deferred for Stream-Count gating
             * (e.g. PUBLISH_DONE and the FIN arrived together), in which case the
             * FIN is absorbed and the gated finish waits for the data streams.
             * A blocked teardown leaves the marker set for the next re-feed;
             * a completed one frees the entry, which clears it. */
            if (peer_fin) {
                if (e->done_pending) {
                    e->req_recv_fin = true;
                    e->handoff_fin_pending = false;
                    return MOQ_OK;
                }
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
            if (e->state == MOQ_PUB_DRAINING_RESPONSE &&
                (e->req_recv_fin || e->handoff_fin_pending))
                pub_free_entry(s, slot);
            return MOQ_OK;
        }
        /* More buffered messages remain (established lifecycle). */
    }
}

/* A peer request that carried the FIN in the same chunk commits into another
 * pool and re-keys the bidi to that owner; the staging slot that recorded the
 * FIN is then released. The FIN belongs to the destination owner, so record it
 * there as transitional ownership and drive that family's own FIN handling --
 * the same handler a later wire FIN would reach. Nothing here decides the
 * outcome: a family whose teardown blocks on capacity keeps the marker and is
 * resumed by an empty re-feed, which routes back to the same handler. Families
 * outside this slice keep their existing behavior. */
static moq_result_t request_stream_handoff_fin(moq_session_t *s,
                                                moq_stream_ref_t stream_ref,
                                                moq_request_endpoint_t dest)
{
    switch (dest.kind) {
    case MOQ_REQ_PUBLISH:
        return handle_publish_stream_bytes(s, (int)dest.slot, stream_ref,
                                            NULL, 0, false);
    case MOQ_REQ_TRACK_STATUS:
        return handle_track_status_stream_bytes(s, (int)dest.slot, stream_ref,
                                                 NULL, 0, false);
    case MOQ_REQ_FETCH:
        return handle_fetch_response_bytes(s, (int)dest.slot, stream_ref,
                                            NULL, 0, false);
    case MOQ_REQ_ANNOUNCEMENT:
        return handle_announcement_stream_bytes(s, (int)dest.slot, stream_ref,
                                                 NULL, 0, false);
    case MOQ_REQ_SUBSCRIBE_TRACKS:
        return handle_subscribe_tracks_stream_bytes(s, (int)dest.slot,
                                                     stream_ref, NULL, 0, false);
    default:
        return MOQ_OK;
    }
}

/* Install the transitional FIN ownership on the destination owner. Separate
 * from the drive above so it happens while the staging slot is still being
 * retired -- the fact is never absent between the commit and its handling. */
static bool request_stream_handoff_fin_install(moq_session_t *s,
                                                moq_request_endpoint_t dest)
{
    switch (dest.kind) {
    case MOQ_REQ_PUBLISH:
        s->publishes[dest.slot].handoff_fin_pending = true;
        return true;
    case MOQ_REQ_TRACK_STATUS:
        s->track_statuses[dest.slot].handoff_fin_pending = true;
        return true;
    case MOQ_REQ_FETCH:
        s->fetches[dest.slot].handoff_fin_pending = true;
        return true;
    case MOQ_REQ_ANNOUNCEMENT:
        s->announcements[dest.slot].handoff_fin_pending = true;
        return true;
    case MOQ_REQ_SUBSCRIBE_TRACKS:
        s->track_subs[dest.slot].handoff_fin_pending = true;
        return true;
    default:
        return false;
    }
}

/* -- No-slot admission carriers (#245b) ----------------------------- *
 * A bounded, session-owned record of a no-slot request-bidi terminal that is
 * owed but not yet emittable. Storage is arena-backed in the session slab at
 * create time (never lazily allocated), so installing one during ingress cannot
 * fail on the allocator; the cap is bounded by the total request-owner/drain
 * lifecycle capacity. The carrier holds only the stream ref and FIN fact --
 * never peer bytes -- because the terminal is content-independent. */
int noslot_carrier_find(const moq_session_t *s, moq_stream_ref_t ref)
{
    if (!s->noslot_carriers || ref._v == 0) return -1;
    for (size_t i = 0; i < s->noslot_carrier_cap; i++)
        if (s->noslot_carriers[i].stream_ref == ref._v) return (int)i;
    return -1;
}

bool noslot_carrier_install(moq_session_t *s, moq_stream_ref_t ref, bool fin)
{
    if (ref._v == 0) return false;
    int existing = noslot_carrier_find(s, ref);
    if (existing >= 0) {
        /* An already-owed terminal: update its FIN fact monotonically (a later
         * FIN observation must not be lost) and keep the same carrier. */
        if (fin) s->noslot_carriers[existing].fin = true;
        return true;
    }
    /* Arena-backed at session create: no allocation happens here, so a fresh
     * install can only fail on genuine pool exhaustion, never on an allocator
     * failure that would strand a retainable WOULD_BLOCK. */
    if (s->noslot_carrier_cap == 0 || !s->noslot_carriers) return false;
    for (size_t i = 0; i < s->noslot_carrier_cap; i++) {
        if (s->noslot_carriers[i].stream_ref == 0) {
            s->noslot_carriers[i].stream_ref = ref._v;
            s->noslot_carriers[i].fin = fin;
            s->noslot_carrier_count++;
            return true;
        }
    }
    return false;   /* pool exhausted -- genuine concurrent-request pressure */
}

void noslot_carrier_remove(moq_session_t *s, moq_stream_ref_t ref)
{
    int i = noslot_carrier_find(s, ref);
    if (i < 0) return;
    s->noslot_carriers[i].stream_ref = 0;
    s->noslot_carriers[i].fin = false;
    if (s->noslot_carrier_count) s->noslot_carrier_count--;
}

/* True when the WHOLE per-stream STOP+RESET terminal transaction can be admitted
 * right now without mutating anything: two action slots (STOP_SENDING +
 * RESET_STREAM) and, for a FIN-unobserved stream, one NORMAL drain slot to
 * absorb the peer's terminal. Partial admission would lose the peer-terminal
 * identity, so callers must gate the whole transaction on this. */
static bool noslot_stream_reset_admissible(const moq_session_t *s, bool eff_fin)
{
    if (action_queue_avail(s) < 2) return false;
    if (!eff_fin && s->drain_ref_count >= s->drain_ref_cap) return false;
    return true;
}

/* Drop a no-owner request bidi by resetting only this stream (draft-18 §3.3
 * permits resetting an early/unbufferable request bidi). Reserves two action
 * slots (STOP_SENDING + RESET_STREAM); a no-FIN request additionally installs a
 * NORMAL drain reference to absorb the peer's terminal. The caller MUST have
 * confirmed the whole transaction is admissible (noslot_stream_reset_admissible)
 * so no partial mutation is possible. Any retry carrier for this stream is
 * retired -- the operation is now resolved, not retained. */
static moq_result_t noslot_stream_reset(moq_session_t *s,
                                        moq_stream_ref_t stream_ref,
                                        bool eff_fin)
{
    moq_action_t stop;
    memset(&stop, 0, sizeof(stop));
    stop.kind = MOQ_ACTION_STOP_BIDI_STREAM;
    stop.detail_size = (uint32_t)sizeof(moq_stop_bidi_stream_action_t);
    stop.borrow_epoch = s->borrow_epoch;
    stop.u.stop_bidi_stream.stream_ref = stream_ref;
    stop.u.stop_bidi_stream.error_code = 0x1;   /* CANCELLED */
    moq_result_t rrc = push_action(s, &stop);
    if (rrc < 0) return rrc;
    moq_action_t reset;
    memset(&reset, 0, sizeof(reset));
    reset.kind = MOQ_ACTION_RESET_BIDI_STREAM;
    reset.detail_size = (uint32_t)sizeof(moq_reset_bidi_stream_action_t);
    reset.borrow_epoch = s->borrow_epoch;
    reset.u.reset_bidi_stream.stream_ref = stream_ref;
    reset.u.reset_bidi_stream.error_code = 0x1;
    rrc = push_action(s, &reset);
    if (rrc < 0) return rrc;
    if (!eff_fin) {
        /* The caller pre-admitted this slot; a failure here would strand the
         * peer terminal, so it is a hard internal error, never a silent drop. */
        if (!drain_ref_add(s, stream_ref))
            return MOQ_ERR_INTERNAL;
    }
    noslot_carrier_remove(s, stream_ref);
    return MOQ_OK;
}

/* The no-owner carrier pool is exhausted, so this refused request has no durable
 * retry identity -- returning WOULD_BLOCK now would leave the bridge unable to
 * find an owner and fatalize the connection. Resolve it without that: drop just
 * this stream with a STOP+RESET, but ONLY when the ENTIRE transaction fits (two
 * action slots plus, for a FIN-unobserved stream, one NORMAL drain slot). When
 * neither durable carrier storage NOR the whole per-stream terminal transaction
 * is admissible (e.g. a full carrier pool AND a full drain ring, even with
 * action slots free), close the session gracefully -- a partial reset would lose
 * the peer-terminal identity, and a bridge fatal bypasses close semantics. The
 * close carries the draft-18 SESSION termination code INTERNAL_ERROR (0x1) --
 * NOT the request-error INTERNAL_ERROR (0x0, a different code space) and NOT
 * UNAUTHORIZED (0x2). Never returns WOULD_BLOCK. */
static moq_result_t noslot_carrier_exhausted(moq_session_t *s,
                                             moq_stream_ref_t stream_ref,
                                             bool eff_fin)
{
    if (noslot_stream_reset_admissible(s, eff_fin))
        return noslot_stream_reset(s, stream_ref, eff_fin);
    return close_with_error(s, 0x1 /* SESSION INTERNAL_ERROR */,
                            "no-owner admission carrier storage and per-stream"
                            " terminal both inadmissible");
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
        /* A no-slot terminal already owed on this bidi (#245b): re-drive it
         * from the durable carrier, folding in whatever FIN was recorded, and
         * without ever re-staging or redelivering peer bytes. Checked BEFORE
         * the empty-input early return so the documented empty re-feed reaches
         * the terminal. */
        int car = noslot_carrier_find(s, stream_ref);
        if (car < 0) {
            if (len == 0 && !fin) return MOQ_OK;
            if (len == 0 && fin)
                return close_with_error(s, 0x3,
                    "empty FIN on request stream without request");
            /* Reserve a slot to buffer the inbound request. */
            slot = sub_find_free(s);
        }
        if (slot < 0) {
            /* Staging/subscription pool exhausted, or a re-fed carrier. Local
             * resource pressure is NOT peer protocol misbehavior, so the
             * SESSION always survives -- never a PROTOCOL_VIOLATION close. The
             * terminal is fixed and content-independent; when it cannot be
             * emitted yet, the OPERATION is retained in a carrier (holding only
             * the stream ref and FIN fact) so an empty re-feed completes it and
             * moq_session_has_transport_stream() keeps the bridge from
             * fatalizing. Nothing was staged, so there is nothing to free. */
            bool eff_fin = fin || (car >= 0 && s->noslot_carriers[car].fin);
            if (!eff_fin && s->drain_ref_count >= s->drain_ref_cap) {
                /* Wait for drain capacity to free. The carrier is the retry
                 * identity moq_session_has_transport_stream() reports; if the
                 * carrier pool is exhausted the operation cannot be retained, so
                 * resolve it now instead of a fatalizing bare WOULD_BLOCK. */
                if (!noslot_carrier_install(s, stream_ref, eff_fin))
                    return noslot_carrier_exhausted(s, stream_ref, eff_fin);
                return MOQ_ERR_WOULD_BLOCK;   /* retry when drain capacity frees */
            }
            if (defer_dispatch) {
                /* Pre-setup: no control stream yet, so an application-level
                 * REQUEST_ERROR cannot be sent. draft-18 §3.3 permits resetting an
                 * early request bidi that we do not want to (or cannot) buffer, so
                 * RESET only this stream and keep the session. Needs two action
                 * slots (STOP_SENDING + RESET_STREAM) plus the drain ref checked
                 * above -- all no-mutation until reserved. */
                if (action_queue_avail(s) < 2) {
                    if (!noslot_carrier_install(s, stream_ref, eff_fin))
                        return noslot_carrier_exhausted(s, stream_ref, eff_fin);
                    return MOQ_ERR_WOULD_BLOCK;
                }
                return noslot_stream_reset(s, stream_ref, eff_fin);
            }
            /* Post-setup: graceful stream-correlated rejection -- REQUEST_ERROR
             * (INTERNAL_ERROR) + FIN on the request bidi. */
            uint8_t perr[64];
            moq_buf_writer_t pw;
            moq_buf_writer_init(&pw, perr, sizeof(perr));
            moq_result_t erc = s->profile->encode_request_error(s, &pw,
                &(moq_request_error_encode_args_t){
                    .error_code = 0x0,
                    .reason = (const uint8_t *)"request pool full",
                    .reason_len = 17 });
            if (erc < 0) return erc;
            erc = queue_send_bidi(s, stream_ref, perr,
                                  moq_buf_writer_offset(&pw), true);
            if (erc == MOQ_ERR_WOULD_BLOCK) {
                if (!noslot_carrier_install(s, stream_ref, eff_fin))
                    return noslot_carrier_exhausted(s, stream_ref, eff_fin);
                return erc;
            }
            if (erc < 0) return erc;
            /* Absorb the peer's trailing FIN unless it already arrived here. */
            if (!eff_fin) (void)drain_ref_add(s, stream_ref);
            noslot_carrier_remove(s, stream_ref);
            return MOQ_OK;
        }
        moq_sub_entry_t *re = &s->subs[slot];
        re->generation |= 1;
    /* Allocated: join the occupancy list the preamble scans walk. */
        sub_occ_link(s, (size_t)slot);
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

        /* A terminal done was consumed but deferred for Stream-Count gating:
         * the entry stays ESTABLISHED so late data streams still bind, while
         * the request bidi drains. Only empty input / a FIN is acceptable --
         * the FIN was recorded on append and is absorbed at finalize (freeing
         * the entry then, or via TERMINATED above if it arrives later). Extra
         * bytes after the terminal are a protocol error. Mirrors the
         * done_pending branch in handle_publish_stream_bytes. */
        if (e->done_pending) {
            if (e->req_recv_len > 0)
                return close_with_error(s, 0x3,
                    "extra bytes after terminal response on request stream");
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
            /* The destination lives in idx_ns_by_ref, NOT the request registry --
             * the stream-ref key still names this staging slot until the free
             * below. Resolve the committed namespace-sub owner through its own
             * index, and drive its transitional FIN ownership only after the
             * staging slot is released, through the family's own handler by an
             * empty internal re-feed. No post-FIN peer bytes are invented. */
            int32_t ns_dest = moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask,
                                              stream_ref._v);
            bool handed_fin = rc == MOQ_OK && ns_dest >= 0 &&
                              s->ns_subs[ns_dest].handoff_fin_pending;
            sub_free_entry(s, (size_t)slot);
            if (extra)
                return close_with_error(s, 0x3,
                    "extra bytes after request on stream");
            if (handed_fin)
                return handle_bidi_stream_bytes(s, stream_ref, NULL, 0, false);
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
            /* The FIN rode the request into the new owner; hand it over before
             * the staging slot that recorded it is released. */
            bool handed_fin = !staging_key_is_stale && e->req_recv_fin &&
                request_stream_handoff_fin_install(s, cur);
            if (!staging_key_is_stale)
                s->subs[slot].request_stream_ref = moq_stream_ref_from_u64(0);
            sub_free_entry(s, (size_t)slot);
            if (handed_fin)
                return request_stream_handoff_fin(s, stream_ref, cur);
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
        bool need_data_abort = fe->data_stream_started && !fe->data_stream_fin;
        /* The abort direction follows stream ownership, not the request.
         *
         * A fetch data stream is unidirectional and always flows publisher ->
         * fetcher, so only the publisher holds a sending half on it. The
         * fetcher, which merely receives, must abort with STOP_DATA; issuing
         * RESET_DATA there asks the transport to reset a direction this
         * endpoint does not own, which the transport rejects and the bridge
         * escalates to a connection fatal. This mirrors
         * fetch_request_bidi_cancel(), which already stops the incoming
         * response stream for the fetcher role. */
        bool fetcher = (fe->role == MOQ_FETCH_ROLE_FETCHER);
        /* Reserve everything before mutating: the data abort MUST be queued, so
         * a full action queue defers the whole teardown for a later retry. */
        if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        if (need_data_abort && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        if (need_data_abort) {
            moq_action_t a;
            memset(&a, 0, sizeof(a));
            if (fetcher) {
                a.kind = MOQ_ACTION_STOP_DATA;
                a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
                a.borrow_epoch = s->borrow_epoch;
                a.u.stop_data.stream_ref = fe->data_stream_ref;
                a.u.stop_data.error_code = 0x1;   /* CANCELLED */
            } else {
                a.kind = MOQ_ACTION_RESET_DATA;
                a.detail_size = (uint32_t)sizeof(moq_reset_data_action_t);
                a.borrow_epoch = s->borrow_epoch;
                a.u.reset_data.stream_ref = fe->data_stream_ref;
                a.u.reset_data.error_code = 0x1;   /* CANCELLED */
            }
            (void)push_action(s, &a);
        }
        moq_event_t e;
        memset(&e, 0, sizeof(e));
        e.kind = MOQ_EVENT_FETCH_CANCELLED;
        e.detail_size = (uint32_t)sizeof(moq_fetch_cancelled_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.fetch_cancelled.fetch = fe->handle;
        (void)push_event(s, &e);
        /* A fetcher's data uni may not have presented its FETCH_HEADER yet.
         * Tombstone the request id so late data is absorbed and stopped instead
         * of resolving against a freed request -- the same protection
         * fetch_request_bidi_cancel() applies on its cancel path. */
        if (fetcher)
            fetch_cancel_tomb_add(s, fe->request_id);
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
    uint64_t timeout_ms, bool peer_fin_observed)
{
    /* The one authoritative drain decision for this terminal, used by the
     * preflight below and by the retirement that follows it. */
    bool need_drain = !peer_fin_observed;
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
     * action, and -- only when one is owed -- a strict drain slot. No
     * data-stream resets (graceful migration leaves media on the old session).
     * A peer that already closed its send half can send nothing late, so an
     * exhausted ring must not refuse a terminal that needs no reference. */
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

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
     * the bidi (free the entry, plus the strict drain when one is owed). */
    if (close_half)
        (void)queue_close_bidi(s, ref);
    request_goaway_retire(s, family, slot, ref, need_drain);
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

/* Receive-side retire (a GOAWAY we *received*): when `need_drain`, strict-drain
 * the request bidi so the peer's later FIN/RESET/STOP retires the ref while a
 * duplicate GOAWAY or stray non-empty bytes close 0x3; then free the entry
 * either way. The caller decided `need_drain` and reserved the slot when true;
 * a peer FIN already observed leaves nothing for a reference to absorb. */
void request_goaway_retire(moq_session_t *s, moq_request_family_t family,
                           int slot, moq_stream_ref_t ref, bool need_drain)
{
    if (need_drain)
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

    /* §9.8 inbound extraction, scanned ONCE before any success/error event
     * or entry transition (a malformed timeout must close with 0x3 and can
     * be neither masked by scratch exhaustion nor downgraded to the
     * unsupported-property reject below). The extracted values are retained
     * here and committed only after push_event succeeds. */
    moq_dt_scan_t insc;
    if (session_scan_dt_props(s, d->track_properties,
                              d->track_properties_len, false, &insc) < 0)
        return close_with_error(s, 0x3, "malformed track properties");

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

    /* Commit (incl. the §9.8 timeout values scanned above). */
    s->subs[slot].dt_pub_has_object   = insc.has_object;
    s->subs[slot].dt_pub_object_ms    = insc.object_ms;
    s->subs[slot].dt_pub_has_subgroup = insc.has_subgroup;
    s->subs[slot].dt_pub_subgroup_ms  = insc.subgroup_ms;
    s->subs[slot].state = MOQ_SUB_ESTABLISHED;
    s->subs[slot].track_alias = d->track_alias;
    s->subs[slot].has_largest = d->has_largest;
    s->subs[slot].largest_group = d->has_largest ? d->largest_group : 0;
    s->subs[slot].largest_object = d->has_largest ? d->largest_object : 0;
    /* Merge the peer-advertised Largest Object into the registry. Monotonic; idempotent under retransmission. */
    if (d->has_largest && s->subs[slot].hist)
        track_hist_merge(s->subs[slot].hist, d->largest_group, d->largest_object);
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
        e.kind = MOQ_EVENT_SUBSCRIBE_ERROR;
        e.detail_size = (uint32_t)sizeof(moq_subscribe_error_event_t);
        e.borrow_epoch = s->borrow_epoch;
        e.u.subscribe_error.sub = s->subs[d->target_slot].handle;
        e.u.subscribe_error.error_code =
            s->profile->semantic_request_error(d->error_code);
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
moq_result_t session_core_on_subscribe_update_ok(moq_session_t *s, int slot,
    bool has_largest, uint64_t largest_group, uint64_t largest_object,
    bool has_expires, uint64_t expires_ms)
{
    moq_sub_entry_t *e = &s->subs[slot];
    if (e->state != MOQ_SUB_ESTABLISHED || !e->update_pending)
        return close_with_error(s, 0x3, "REQUEST_OK without a pending update");
    /* Emit MOQ_EVENT_SUBSCRIPTION_UPDATE_OK exactly once. A full event queue is
     * a retryable WOULD_BLOCK checked BEFORE clearing update_pending, so the
     * acknowledgment is never dropped and never double-emitted (a duplicate
     * REQUEST_OK after the flag clears is a protocol violation above). */
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_SUBSCRIPTION_UPDATE_OK;
    ev.detail_size = (uint32_t)sizeof(moq_subscription_update_ok_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.subscription_update_ok.sub = e->handle;
    ev.u.subscription_update_ok.has_largest = has_largest;
    ev.u.subscription_update_ok.largest_group = has_largest ? largest_group : 0;
    ev.u.subscription_update_ok.largest_object = has_largest ? largest_object : 0;
    ev.u.subscription_update_ok.has_expires = has_expires;
    ev.u.subscription_update_ok.expires_ms = has_expires ? expires_ms : 0;
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;   /* retryable; update_pending still set */
    /* Merge the peer-advertised Largest into the registry. */
    if (has_largest && e->hist)
        track_hist_merge(e->hist, largest_group, largest_object);
    /* Latch the acknowledged Forward state: object delivery on this
     * subscription gates on it from the NEXT object admission on. */
    if (e->update_has_forward)
        e->forward = e->update_forward;
    e->update_has_forward = false;
    /* latch the acknowledged filter type (Joining-FETCH gates on it). */
    if (e->update_has_filter)
        e->filter_type = e->update_filter_type;
    e->update_has_filter = false;
    /* §9.8: latch acknowledged timeout carriers and recompute the retained
     * legacy projection from the COMPLETE current pair. */
    if (e->dt_upd_has_object) {
        e->dt_sub_has_object = true;
        e->dt_sub_object_ms = e->dt_upd_object_ms;
    }
    if (e->dt_upd_has_subgroup) {
        e->dt_sub_has_subgroup = true;
        e->dt_sub_subgroup_ms = e->dt_upd_subgroup_ms;
    }
    if (e->dt_upd_has_object || e->dt_upd_has_subgroup)
        e->delivery_timeout_us = ms_to_us_sat(dt_negotiate_ms(
            e->dt_sub_has_object, e->dt_sub_object_ms,
            e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms));
    e->dt_upd_has_object = e->dt_upd_has_subgroup = false;
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
    e->update_has_forward = false;   /* never acknowledged */
    e->update_has_filter = false;    /* pending filter dies unacked */
    e->dt_upd_has_object = e->dt_upd_has_subgroup = false;
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
            /* The terminal frees a live publisher-role subscription whose
             * request bidi the peer may still be sending on, so it owes a drain
             * reference to absorb a message already in flight -- unless that
             * peer FIN has already been observed. This family is same-entry:
             * `handle_request_stream_bytes()` latched the FIN on THIS entry
             * before dispatching, and there is no ownership handoff and no
             * handoff marker, so `req_recv_fin` is the whole fact. Draft-16
             * carries the terminal on the control channel and never drains.
             *
             * ONE decision, reserved here and inserted below. It is reserved
             * BEFORE `sub_reset_subgroups()` because that helper queues resets
             * and mutates subgroup state: a refusal afterwards would leave a
             * half-torn-down subscription behind. */
            moq_sub_entry_t *ue = &s->subs[d->target_slot];
            bool need_drain = moq_session_uses_request_streams(s) &&
                              ue->request_stream_ref._v != 0 &&
                              !ue->req_recv_fin;
            if (need_drain && s->drain_ref_count >= s->drain_ref_cap) {
                result = MOQ_ERR_WOULD_BLOCK;
                goto cleanup_all;
            }

            /* Tear down the subscription's active subgroups (same as
             * UNSUBSCRIBE) before announcing PUBLISH_DONE and freeing it, so its
             * data streams are reset on the wire and its subgroup slots enter
             * MOQ_SG_RESETTING for the terminal reap rather than staying pinned
             * as live streams. WOULD_BLOCK here is retryable and resumes via the
             * idempotent reset loop; nothing is committed/freed yet. */
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
                /* The exact terminal is on the wire; take the reference the
                 * decision above reserved, before the entry is freed. */
                if (need_drain)
                    (void)drain_ref_add(s, up_ref);
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

    /* One registry snapshot: it feeds BOTH the REQUEST_OK Largest
     * Object and the stored resolved window. No app mediation on this path. */
    bool     usnap_has = e->hist && e->hist->has_largest;
    uint64_t usnap_g = usnap_has ? e->hist->largest_group : 0;
    uint64_t usnap_o = usnap_has ? e->hist->largest_object : 0;

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
        moq_request_update_ok_encode_args_t oka = {
            .request_id = d->request_id,
            .has_largest = usnap_has,
            .largest_group = usnap_g,
            .largest_object = usnap_o,
            .has_expires = false,   /* no EXPIRES source on this path  */
        };
        rc = s->profile->encode_request_update_ok(s, &ow, &oka);
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
    ev.u.subscribe_updated.has_filter = d->has_filter;
    ev.u.subscribe_updated.filter = d->filter_type;
    ev.u.subscribe_updated.start_group = d->start_group;
    ev.u.subscribe_updated.start_object = d->start_object;
    ev.u.subscribe_updated.end_group = d->end_group;

    rc = push_event(s, &ev);
    if (rc < 0) { result = rc; goto cleanup_all; }

    /* Commit. */
    if (d->has_forward)
        e->forward = d->forward;
    if (d->dt_has_object) {
        e->dt_sub_has_object = true;
        e->dt_sub_object_ms = d->dt_object_ms;
    }
    if (d->dt_has_subgroup) {
        e->dt_sub_has_subgroup = true;
        e->dt_sub_subgroup_ms = d->dt_subgroup_ms;
    }
    if (d->has_delivery_timeout)
        /* Retained legacy projection: recomputed from the COMPLETE current
         * pair AFTER applying this message's carriers (the event above
         * projected only the carriers present in the message). */
        e->delivery_timeout_us = ms_to_us_sat(dt_negotiate_ms(
            e->dt_sub_has_object, e->dt_sub_object_ms,
            e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms));
    if (d->has_filter) {
        e->filter_type = d->filter_type;
        /* Re-resolve + store the window against the SAME snapshot that fed the
         * REQUEST_OK Largest Object. */
        e->req_start_group = d->start_group;
        e->req_start_object = d->start_object;
        e->req_end_group = d->end_group;
        moq_resolve_filter_window(d->filter_type,
                                  d->start_group, d->start_object, d->end_group,
                                  usnap_has, usnap_g, usnap_o,
                                  s->profile->location_varint_max, &e->window);
    }
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
    if (moq_validate_full_track_name_min_fields(
            &cfg->track_namespace, cfg->track_name,
            s->profile->min_track_namespace_fields) < 0)
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

    /* Reserve this track's registry record: received objects on this
     * subscription max-merge their largest into it. A full registry fails the
     * local subscribe with NOMEM before anything is sent (the record is released
     * on every pre-commit failure path below and in sub_free_entry). */
    moq_track_hist_t *hist = track_hist_reserve(s, tid, tid_len);
    if (!hist) {
        if (tid) s->alloc.free(tid, tid_len, s->alloc.ctx);
        s->profile->abort_request(s, &req_ep);
        return MOQ_ERR_NOMEM;
    }

    /* Stream-correlated profiles open a dedicated bidi stream per request;
     * others send the request on the shared control channel. */
    bool req_stream = moq_session_uses_request_streams(s);
    moq_stream_ref_t req_ref = moq_stream_ref_from_u64(0);

    /* Encode directly into send_buf via profile op. */
    if (action_queue_full(s)) {
        track_hist_release(s, hist);
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
            track_hist_release(s, hist);
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
            track_hist_release(s, hist);
            if (tid) s->alloc.free(tid, tid_len, s->alloc.ctx);
            s->profile->abort_request(s, &req_ep);
            return arc;
        }
        s->send_len += encoded_len;
    }

    /* Commit. */
    moq_sub_entry_t *entry = &s->subs[slot];
    entry->generation |= 1;
    /* Allocated: join the occupancy list the preamble scans walk. */
    sub_occ_link(s, (size_t)slot);
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
    entry->hist = hist;
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
    /* §9.8 local property scanner: pure, and hoisted ABOVE
     * session_begin_advance so malformed local properties reject before ANY
     * session mutation (no clock/borrow-epoch advance, no reap). */
    if (cfg->track_properties.len > 0 && !cfg->track_properties.data)
        return MOQ_ERR_INVAL;
    moq_dt_scan_t dtscan;
    if (session_scan_dt_props(s, cfg->track_properties.data,
                              cfg->track_properties.len, true, &dtscan) < 0)
        return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->subs[slot].state != MOQ_SUB_PENDING_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;

    /* Validate all inputs before any state mutation. Bound Location fields
     * against the PROFILE's ceiling (design §4): draft-16 QUIC varint, draft-18
     * vi64 (full uint64). EXPIRES is a QUIC/vi64 duration -- keep its existing
     * QUIC-varint bound (it is not a Location). */
    uint64_t loc_max = s->profile->location_varint_max;
    if (cfg->has_largest) {
        if (cfg->largest_group > loc_max || cfg->largest_object > loc_max)
            return MOQ_ERR_INVAL;
    }
    if (cfg->has_expires && cfg->expires_ms > MOQ_QUIC_VARINT_MAX)
        return MOQ_ERR_INVAL;
    if (cfg->track_properties.len > 0 && !cfg->track_properties.data)
        return MOQ_ERR_INVAL;

    /* Single resolution snapshot: the max of the
     * app-supplied accept value and this track's registry record. That ONE value
     * feeds the outbound SUBSCRIBE_OK Largest Object, resolves the relative
     * filter into the stored window, resolves buffered Joining FETCHes, and is
     * merged back into the registry -- so all four agree (§5.1.2). */
    moq_track_hist_t *shist = s->subs[slot].hist;
    bool     snap_has = cfg->has_largest;
    uint64_t snap_g   = cfg->has_largest ? cfg->largest_group : 0;
    uint64_t snap_o   = cfg->has_largest ? cfg->largest_object : 0;
    if (shist && shist->has_largest &&
        (!snap_has || shist->largest_group > snap_g ||
         (shist->largest_group == snap_g && shist->largest_object > snap_o))) {
        snap_has = true;
        snap_g   = shist->largest_group;
        snap_o   = shist->largest_object;
    }

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
        .has_largest = snap_has,
        .largest_group = snap_g,
        .largest_object = snap_o,
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
            snap_has, snap_g, 1 /* OK action */,
            0 /* OK takes no drain */, ok_len);
    if (pjrc < 0) return pjrc;

    /* SUBSCRIBE_OK keeps the request bidi open for the subscription. */
    moq_result_t arc = queue_subscribe_response(s, (size_t)slot, ok_len,
                                                false /* fin */);
    if (arc < 0) return arc;

    /* Commit: all outputs reserved. */
    s->subs[slot].state = MOQ_SUB_ESTABLISHED;
    s->subs[slot].dt_pub_has_object   = dtscan.has_object;
    s->subs[slot].dt_pub_object_ms    = dtscan.object_ms;
    s->subs[slot].dt_pub_has_subgroup = dtscan.has_subgroup;
    s->subs[slot].dt_pub_subgroup_ms  = dtscan.subgroup_ms;
    s->subs[slot].track_alias = alias;
    s->subs[slot].has_largest = snap_has;
    s->subs[slot].largest_group = snap_has ? snap_g : 0;
    s->subs[slot].largest_object = snap_has ? snap_o : 0;
    /* Merge the snapshot back into the registry (an app seed never lowers an
     * observed value; the merge is monotonic) and resolve + store the window
     * against the SAME snapshot. The facade installs this window via
     * moq_session_sub_resolved_window. */
    if (snap_has && shist) track_hist_merge(shist, snap_g, snap_o);
    moq_resolve_filter_window(s->subs[slot].filter_type,
                              s->subs[slot].req_start_group,
                              s->subs[slot].req_start_object,
                              s->subs[slot].req_end_group,
                              snap_has, snap_g, snap_o, loc_max,
                              &s->subs[slot].window);
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
    /* The code must be representable in THIS profile's wire encoding; refuse
     * before any mutation rather than truncate (draft-16 encodes a QUIC
     * varint, draft-18 a vi64 spanning the full 64-bit range). */
    if (cfg->error_code > s->profile->request_error_wire_max)
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
 * RESET the request bidi, reserve and install a NORMAL drain reference -- but
 * only while the peer's request send half is still open, so a late SUBSCRIBE_OK /
 * REQUEST_ERROR is discarded rather than mistaken for a new inbound request; an
 * already-observed FIN leaves nothing to absorb and needs no reference. Then
 * free the entry. Reserve-before-mutate (WOULD_BLOCK leaves state intact). Shared
 * by moq_session_unsubscribe and the UNSUPPORTED_EXTENSION (0x33) path; the caller
 * has already validated role/state and that request streams are in use. Does NOT
 * call session_begin_advance, so it is safe inside an inbound advance. */
static moq_result_t subscribe_request_bidi_cancel(moq_session_t *s, int slot)
{
    moq_sub_entry_t *e = &s->subs[slot];
    moq_stream_ref_t ref = e->request_stream_ref;
    /* One drain decision for both the preflight and the insertion below. The
     * subscription is the same-entry family -- the staging slot IS the
     * subscription slot, so there is no handoff marker and `req_recv_fin` is the
     * whole observed-FIN fact. A peer that already closed its send half can send
     * nothing late, so the reference has nothing to absorb; the abort action and
     * the entry's retirement are unaffected. */
    bool need_drain = ref._v != 0 && !e->req_recv_fin;
    if (action_queue_avail(s) < 1) return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    moq_action_t abort_a;
    memset(&abort_a, 0, sizeof(abort_a));
    abort_a.kind = MOQ_ACTION_ABORT_BIDI_STREAM;
    abort_a.detail_size = (uint32_t)sizeof(moq_abort_bidi_stream_action_t);
    abort_a.borrow_epoch = s->borrow_epoch;
    abort_a.u.abort_bidi_stream.stream_ref = ref;
    abort_a.u.abort_bidi_stream.error_code = 0x1;   /* CANCELLED */
    moq_result_t src = push_action(s, &abort_a);
    if (src < 0) return src;
    if (need_drain)
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
    /* The publisher already terminated (deferred done): the subscription is
     * logically Terminated and no control may follow -- the pending
     * SUBSCRIBE_DONE surfaces once the advertised streams are processed. */
    if (e->done_pending)
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

/* §9.8: negotiated terminal-wait floor for this entry -- the larger of the
 * per-type negotiated timeouts (d18 §10.11 "larger of the two"; d16's
 * single value occupies both types), saturating ms->us. */
static uint64_t sub_dt_floor_us(const moq_sub_entry_t *e)
{
    uint64_t o = dt_negotiate_ms(e->dt_pub_has_object, e->dt_pub_object_ms,
                                 e->dt_sub_has_object, e->dt_sub_object_ms);
    uint64_t g = dt_negotiate_ms(e->dt_pub_has_subgroup,
                                 e->dt_pub_subgroup_ms,
                                 e->dt_sub_has_subgroup,
                                 e->dt_sub_subgroup_ms);
    return ms_to_us_sat(o > g ? o : g);
}

/* Atomically retire a crossed REQUEST_UPDATE at terminal commit: draft-16
 * keeps ONE tombstone (reserved by the caller) for the publisher's mandatory
 * single response and drops the by-id registry key, so a duplicate response
 * is then an unknown request (violation) instead of resolving through the
 * still-live entry; draft-18 just clears the pending state. */
static void sub_retire_crossed_update(moq_session_t *s, moq_sub_entry_t *e,
                                      bool need_tomb)
{
    if (!e->update_pending) return;
    if (need_tomb) unsub_tomb_add(s, e->update_request_id);
    request_registry_remove_by_id(s, e->update_request_id);
    e->update_pending = false;
    e->update_request_id = 0;
    e->update_has_forward = false;   /* never acknowledged */
    e->update_has_filter = false;    /* pending filter dies unacked */
    e->dt_upd_has_object = e->dt_upd_has_subgroup = false;
}

moq_result_t session_core_on_subscribe_done(moq_session_t *s,
                                             int slot,
                                             const moq_decoded_publish_done_t *d,
                                             bool free_now)
{
    moq_sub_entry_t *e = &s->subs[slot];
    /* On the terminal PUBLISH_DONE the subscriber closes its send half so the
     * publisher can retire the request bidi; reserve that action up front. */
    bool close_half = !free_now && e->request_stream_ref._v != 0;
    /* An in-flight REQUEST_UPDATE crossed the terminal. Draft-16 answers it
     * exactly once by request id on the control channel, so reserve ONE
     * tombstone for that mandatory ack (otherwise it would look like a
     * response to an unknown request and close the session); draft-18
     * correlates updates by request stream and never consumes id tombstones,
     * so it must not spend one. The update itself is retired atomically at
     * terminal commit (sub_retire_crossed_update). */
    bool need_tomb = e->update_pending &&
                     !moq_session_uses_request_streams(s);
    if (need_tomb && s->unsub_tomb_count + 1 > s->unsub_tomb_cap)
        return close_with_error(s, 0x3,
            "subscribe done tombstone budget exceeded");

    /* Stream-Count gating (draft-16 §9.15 / draft-18 §10.11), mirroring
     * session_core_on_publish_done: the terminal done is likely to precede
     * late-arriving / late-opening data streams. Keep the subscription
     * ESTABLISHED (alias bound, so those streams still bind and deliver)
     * until the advertised number of data streams have been processed; only
     * then surface SUBSCRIBE_DONE. A Stream Count of 0 finalizes
     * immediately; the 2^62-1 "unknown" sentinel DEFERS and is bounded by
     * the terminal deadline below (§9.8: the timeout is the drafts'
     * recovery for "unable to be exact"). The local send half still closes
     * now, exactly once, so the peer can retire the bidi while the gate
     * holds. */
    if (e->role == MOQ_SUB_ROLE_SUBSCRIBER &&
        d->stream_count != 0 &&
        (d->stream_count == MOQ_QUIC_VARINT_MAX ||
         e->processed_stream_count < d->stream_count)) {
        if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        uint8_t *rbuf = NULL;
        if (d->reason_len > 0) {
            rbuf = (uint8_t *)s->alloc.alloc(d->reason_len, s->alloc.ctx);
            if (!rbuf) return MOQ_ERR_NOMEM;
            memcpy(rbuf, d->reason, d->reason_len);
        }
        sub_retire_crossed_update(s, e, need_tomb);
        if (e->done_reason_buf)   /* unreachable: duplicates rejected upstream */
            s->alloc.free(e->done_reason_buf, e->done_reason_len, s->alloc.ctx);
        e->done_reason_buf = rbuf;
        e->done_reason_len = d->reason_len;
        e->done_status_code = d->status_code;
        e->done_stream_count = d->stream_count;
        e->done_pending = true;
        /* §9.8: stamp the terminal deadline ONCE (never restamped; later
         * acked timeout updates affect future deferrals only). The wait is
         * the configured/default bound floored by the negotiated delivery
         * timeouts; the 2^62-1 sentinel defers like any finite count and
         * is finalized by this deadline (the drafts' recovery for "unable
         * to be exact"). */
        {
            uint64_t wait_us = s->done_wait_timeout_us;
            uint64_t floor_us = sub_dt_floor_us(e);
            if (floor_us > wait_us) wait_us = floor_us;
            e->done_deadline_us = deadline_add(s->last_now_us, wait_us);
            e->done_expired = false;
        }
        if (close_half)
            (void)queue_close_bidi(s, e->request_stream_ref);
        return MOQ_OK;
    }

    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
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

    sub_retire_crossed_update(s, &s->subs[slot], need_tomb);
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
    moq_stream_ref_t req_ref = e->request_stream_ref;
    /* Reserve a drain slot so a late in-flight REQUEST_UPDATE arriving on the
     * request bidi after we FIN + free is discarded, not mistaken for a new
     * inbound request -- unless the subscriber's FIN was already observed, which
     * leaves nothing to absorb. `req_recv_fin` is the whole fact here: the
     * subscription is the same-entry family and carries no handoff marker. One
     * decision, used by this preflight and by the insertion below; the
     * PUBLISH_DONE + FIN and the entry's retirement are unaffected. */
    bool need_drain = req_stream && req_ref._v != 0 && !e->req_recv_fin;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

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
        if (need_drain)
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

/* Frozen original prefix: struct_size .. delivery_timeout_us (the layout
 * before the auth-token append; it matches the read path's minimum,
 * offsetof(auth_tokens)). The pointer-only initializer touches only this
 * prefix -- writing sizeof(current) would overflow a caller that allocated
 * the original-sized struct -- and every appended field (auth tokens,
 * new-group request, filter) stays disabled unless _init_sized opts in. */
#define MOQ_SUB_UPDATE_SESSION_CFG_V0_SIZE \
    ((offsetof(moq_subscription_update_cfg_t, delivery_timeout_us) + \
      sizeof(((moq_subscription_update_cfg_t *)0)->delivery_timeout_us) + \
      (_Alignof(moq_subscription_update_cfg_t) - 1)) & \
     ~(size_t)(_Alignof(moq_subscription_update_cfg_t) - 1))
/* The floor is derived INDEPENDENTLY (aligned end of the original last
 * field) so it cannot follow layout drift; equality-pin the first appended
 * field against it. Same discipline for the v0 boundary: aligned end of
 * new_group_request, equality-pinned by has_filter. */
_Static_assert(offsetof(moq_subscription_update_cfg_t, auth_tokens) ==
               MOQ_SUB_UPDATE_SESSION_CFG_V0_SIZE,
               "auth_tokens must sit exactly at the frozen v0 floor");
_Static_assert(offsetof(moq_subscription_update_cfg_t, has_filter) ==
               ((offsetof(moq_subscription_update_cfg_t, new_group_request) +
                 sizeof(((moq_subscription_update_cfg_t *)0)->new_group_request) +
                 (_Alignof(moq_subscription_update_cfg_t) - 1)) &
                ~(size_t)(_Alignof(moq_subscription_update_cfg_t) - 1)),
               "the filter block must sit exactly at the v0 sizeof");

void moq_subscription_update_cfg_init(moq_subscription_update_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_SUB_UPDATE_SESSION_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_SUB_UPDATE_SESSION_CFG_V0_SIZE;
}

void moq_subscription_update_cfg_init_sized(
    moq_subscription_update_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
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
    /* Filter block: whole-BLOCK gate on its LAST field, so a caller
     * whose struct ends mid-block can never produce a torn filter. */
    bool has_filter = false;
    uint32_t filter = 0;
    uint64_t f_sg = 0, f_so = 0, f_eg = 0;
    if (UPD_CFG_HAS(end_group) && cfg->has_filter) {
        has_filter = true;
        filter = (uint32_t)cfg->filter;
        f_sg = cfg->start_group;
        f_so = cfg->start_object;
        f_eg = cfg->end_group;
    }
#undef UPD_CFG_HAS
#undef UPD_CFG_MIN
    if (!cfg->has_subscriber_priority && !cfg->has_forward &&
        !cfg->has_delivery_timeout && auth_token_count == 0 &&
        !has_new_group_request && !has_filter)
        return MOQ_ERR_INVAL;
    if (cfg->has_delivery_timeout && cfg->delivery_timeout_us < 1000)
        return MOQ_ERR_INVAL;
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count) < 0)
        return MOQ_ERR_INVAL;
    /* Filter validation, BEFORE any mutation, matching moq_session_subscribe:
     * the enum member must be a real filter form; ABSOLUTE_RANGE requires
     * end_group >= start_group; locations on the relative forms are IGNORED
     * (the wire carries none for them). */
    if (has_filter) {
        if (filter != MOQ_SUBSCRIBE_FILTER_NEXT_GROUP &&
            filter != MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT &&
            filter != MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START &&
            filter != MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE)
            return MOQ_ERR_INVAL;
        if (filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE && f_eg < f_sg)
            return MOQ_ERR_INVAL;
        /* The APPLICABLE absolute locations must be encodable under this
         * profile's ceiling (draft-16 QUIC varints, draft-18 vi64) --
         * prechecked HERE so an over-ceiling value is INVAL with zero
         * mutation, never a post-advance encoder failure. */
        if (filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START ||
            filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE) {
            uint64_t loc_max = s->profile->location_varint_max;
            if (f_sg > loc_max || f_so > loc_max)
                return MOQ_ERR_INVAL;
            if (filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE &&
                f_eg > loc_max)
                return MOQ_ERR_INVAL;
        }
    }

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = sub_resolve_handle(s, sub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_sub_entry_t *e = &s->subs[slot];
    if (e->role != MOQ_SUB_ROLE_SUBSCRIBER)
        return MOQ_ERR_WRONG_STATE;
    if (e->state != MOQ_SUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    /* A deferred terminal (done_pending) is logically Terminated: the entry
     * stays ESTABLISHED only so late data streams still bind. No control may
     * be sent for the request after its terminal was accepted. */
    if (e->done_pending)
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
        .has_filter = has_filter,
        .filter = filter,
        .filter_start_group = f_sg,
        .filter_start_object = f_so,
        .filter_end_group = f_eg,
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
    /* A Forward change takes effect at the ACK (the CURRENT acknowledged
     * Forward state gates object delivery); remember it until then. */
    e->update_has_forward = cfg->has_forward;
    e->update_forward = cfg->has_forward ? cfg->forward : false;
    /* §9.8: a timeout change pends until the ACK. The generic cfg field is
     * emitted as BOTH d18 parameters (and d16's single one), so it pends
     * into both types, in the wire's milliseconds. */
    e->dt_upd_has_object = e->dt_upd_has_subgroup = cfg->has_delivery_timeout;
    e->dt_upd_object_ms = e->dt_upd_subgroup_ms =
        cfg->has_delivery_timeout ? cfg->delivery_timeout_us / 1000u : 0;
    /* the acknowledged filter TYPE latches at the ACK (its requester-
     * side consumer is the Joining-FETCH eligibility gate); pend it here. */
    e->update_has_filter = has_filter;
    e->update_filter_type = has_filter ? filter : 0;
    s->profile->commit_request(s, &req_ep);
    return MOQ_OK;
}
