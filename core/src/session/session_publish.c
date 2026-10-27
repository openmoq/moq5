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

int pub_resolve_handle(const moq_session_t *s, moq_publication_t h)
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

_Static_assert(sizeof(moq_publish_initial_parameters_t) >=
                   MOQ_PUBLISH_INITIAL_PARAMETERS_V0_SIZE,
               "initial PUBLISH output must cover its frozen floor");

moq_result_t moq_session_get_publish_initial_parameters(const moq_session_t *s,
    moq_publication_t pub, moq_publish_initial_parameters_t *out, size_t out_size)
{
    if (!s || !out || out_size < MOQ_PUBLISH_INITIAL_PARAMETERS_V0_SIZE)
        return MOQ_ERR_INVAL;
    if (!s->profile->publish_initial_parameters) return MOQ_ERR_UNSUPPORTED;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    const moq_pub_entry_t *entry = &s->publishes[slot];
    if (entry->role != MOQ_PUB_ROLE_SUBSCRIBER || entry->done_pending ||
        (entry->state != MOQ_PUB_PENDING_SUBSCRIBER && entry->state != MOQ_PUB_ESTABLISHED))
        return MOQ_ERR_WRONG_STATE;
    size_t n = out_size < sizeof(*out) ? out_size : sizeof(*out);
    memcpy(out, &entry->d22_initial, n);
    out->struct_size = (uint32_t)n;
    return MOQ_OK;
}

/* Defined with a parenthesized name: the same-name macro routes source
 * callers to the _sized form. */
moq_result_t (moq_session_publication_filter)(moq_session_t *s,
    moq_publication_t pub, moq_location_filter_cfg_t *out)
{
    return moq_session_publication_filter_sized(s, pub, out,
                                                MOQ_LOCATION_FILTER_CFG_V0_SIZE);
}

moq_result_t moq_session_publication_filter_sized(moq_session_t *s,
    moq_publication_t pub, moq_location_filter_cfg_t *out, size_t out_size)
{
    if (!s || !out || out_size < MOQ_LOCATION_FILTER_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    const moq_pub_entry_t *e = &s->publishes[slot];
    moq_location_filter_from_core(&(moq_core_filter_t){
        .type = e->filter_type,
        .start_group = e->req_start_group, .start_object = e->req_start_object,
        .end_group = e->req_end_group, .end_object = e->req_end_object },
        out, out_size);
    return MOQ_OK;
}

moq_result_t moq_session_publication_window(moq_session_t *s,
    moq_publication_t pub, moq_filter_window_t *out, size_t out_size)
{
    if (!s) return MOQ_ERR_INVAL;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return out ? MOQ_ERR_STALE_HANDLE : MOQ_ERR_INVAL;
    return moq_filter_window_out(&s->publishes[slot].window, out, out_size);
}

const moq_resolved_window_t *moq_session_pub_resolved_window(
    moq_session_t *s, moq_publication_t pub)
{
    if (!s) return NULL;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return NULL;
    moq_pub_entry_t *e = &s->publishes[slot];
    if (!e->window.has_window) return NULL;
    return &e->window;
}

moq_group_order_t moq_session_publication_group_order(
    const moq_session_t *s, moq_publication_t pub)
{
    if (!s) return MOQ_GROUP_ORDER_DEFAULT;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_GROUP_ORDER_DEFAULT;
    const moq_pub_entry_t *e = &s->publishes[slot];
    return session_group_order(e->group_order, e->track_group_order);
}

moq_result_t session_core_on_publish_update_ok(moq_session_t *s, int slot,
    bool has_largest, uint64_t largest_group, uint64_t largest_object,
    bool has_expires, uint64_t expires_ms)
{
    moq_pub_entry_t *e = &s->publishes[slot];
    if (e->state != MOQ_PUB_ESTABLISHED || !e->update_pending)
        return close_with_error(s, 0x3, "REQUEST_OK without a pending update");
    uint64_t acked_update_id = e->update_request_id;
    /* Emit MOQ_EVENT_PUBLICATION_UPDATE_OK exactly once; a full event queue is a
     * retryable WOULD_BLOCK checked BEFORE clearing update_pending. */
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLICATION_UPDATE_OK;
    ev.detail_size = (uint32_t)sizeof(moq_publication_update_ok_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.publication_update_ok.pub = e->handle;
    ev.u.publication_update_ok.has_largest = has_largest;
    ev.u.publication_update_ok.largest_group = has_largest ? largest_group : 0;
    ev.u.publication_update_ok.largest_object = has_largest ? largest_object : 0;
    ev.u.publication_update_ok.has_expires = has_expires;
    ev.u.publication_update_ok.expires_ms = has_expires ? expires_ms : 0;
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;   /* retryable; update_pending still set */
    if (e->role == MOQ_PUB_ROLE_PUBLISHER) {
        /* This side updated its own PUBLISH: its delivery timeouts are the
         * publisher-side carriers from now on. */
        if (e->dt_upd_has_object) {
            e->dt_pub_has_object = true;
            e->dt_pub_object_ms = e->dt_upd_object_ms;
        }
        if (e->dt_upd_has_subgroup) {
            e->dt_pub_has_subgroup = true;
            e->dt_pub_subgroup_ms = e->dt_upd_subgroup_ms;
        }
        e->dt_upd_has_object = e->dt_upd_has_subgroup = false;
        e->update_pending = false;
        e->update_request_id = 0;
        return MOQ_OK;
    }
    if (has_largest && e->hist)
        track_hist_merge(e->hist, largest_group, largest_object);
    /* Latch the acknowledged Forward state: object delivery (streams and
     * datagrams) gates on it from this point on. */
    const bool fwd_rising = e->update_has_forward && e->update_forward &&
                            !e->send_allowed;
    if (e->update_has_forward)
        e->send_allowed = e->update_forward;
    e->update_has_forward = false;
    if (e->update_has_subscriber_priority)
        e->subscriber_priority = e->update_subscriber_priority;
    e->update_has_subscriber_priority = false;
    if (e->update_has_filter) {
        e->filter_type = e->update_filter_type;
        e->req_start_group = e->update_start_group;
        e->req_start_object = e->update_start_object;
        e->req_end_group = e->update_end_group;
        e->req_end_object = e->update_end_object;
        bool snap = has_largest;
        uint64_t sg = largest_group, so = largest_object;
        if (!snap && e->hist && e->hist->has_largest) {
            snap = true;
            sg = e->hist->largest_group;
            so = e->hist->largest_object;
        }
        moq_resolve_filter_window(e->filter_type, e->req_start_group,
            e->req_start_object, e->req_end_group, e->req_end_object,
            snap, sg, so, s->profile->location_varint_max, &e->window);
    }
    e->update_has_filter = false;
    /* draft-18 5.1: a REQUEST_UPDATE_OK that changes the Forward State from 0
     * to 1 supplies a NEW Joining Location -- the Largest it carried, exactly
     * as carried. A rise with no Largest leaves the previous location alone
     * rather than inventing one from history.
     *
     * PROFILE-SCOPED to draft-18. Draft-16 5.1 saves the Largest communicated
     * in PUBLISH or SUBSCRIBE_OK WHEN ESTABLISHING the subscription and states
     * no update-driven transition rule, so applying this here would move a
     * draft-16 Joining Location the draft never moves.
     *
     * The location is latched DIRECTLY from the acknowledged snapshot; the
     * establishment record (publish_has_largest / publish_largest_*) is left
     * exactly as the PUBLISH set it. */
    if (moq_session_uses_request_streams(s) && fwd_rising && has_largest) {
        e->has_joining_loc = false;      /* re-latch under the new state */
        pub_latch_joining_location_at(s, e, largest_group, largest_object);
    }
    /* §9.8: latch acknowledged timeout carriers; retained legacy stays
     * EXACT milliseconds, recomputed from the complete current pair. */
    if (e->dt_upd_has_object) {
        e->dt_sub_has_object = true;
        e->dt_sub_object_ms = e->dt_upd_object_ms;
    }
    if (e->dt_upd_has_subgroup) {
        e->dt_sub_has_subgroup = true;
        e->dt_sub_subgroup_ms = e->dt_upd_subgroup_ms;
    }
    if (e->dt_upd_has_object || e->dt_upd_has_subgroup) {
        e->has_delivery_timeout = true;
        e->delivery_timeout_ms = dt_negotiate_ms(
            e->dt_sub_has_object, e->dt_sub_object_ms,
            e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms);
    }
    e->dt_upd_has_object = e->dt_upd_has_subgroup = false;
    e->update_pending = false;
    e->update_request_id = 0;
    /* The update's fill child learns its outcome from the acknowledged
     * Largest and the now-current Forward state. */
    fill_control_resolved(s, fill_parent_pub(s, (size_t)slot), acked_update_id, true, 0,
                          has_largest, largest_group, largest_object, !e->send_allowed);
    return MOQ_OK;
}

void pub_free_entry(moq_session_t *s, int slot)
{
    moq_pub_entry_t *e = &s->publishes[slot];
    if (e->mt_failing) {
        e->mt_failing = false;
        s->mt_failing_count--;
    }
    /* The publication ends: every fill child it owns is cancelled (see
     * sub_free_entry). */
    fill_parent_cancel(s, fill_parent_pub(s, (size_t)slot), MOQ_FILL_ERROR_NONE, 0);
    /* Leave the occupancy list BEFORE the reset below: unlinking repairs the
     * neighbours' links, which the memset would otherwise destroy. */
    pub_occ_unlink(s, (size_t)slot);
    request_registry_remove_by_id(s, e->request_id);
    if (e->update_pending)
        request_registry_remove_by_id(s, e->update_request_id);
    /* Stream-correlated profiles also key the entry by its PUBLISH request bidi. */
    if (e->rs.ref._v != 0)
        request_registry_remove_by_streamref(s, e->rs.ref);
    if (e->done_reason_buf) {
        s->alloc.free(e->done_reason_buf, e->done_reason_len, s->alloc.ctx);
        e->done_reason_buf = NULL;
        e->done_reason_len = 0;
    }
    /* An answer the application still owed dies with the publication:
     * nothing is answered after the terminal. */
    session_owed_update_clear(s, &e->owed_update);
    /* Release the reserved registry record exactly once BEFORE the memset
     * (empty reservations reclaimed; observed records pinned). */
    if (e->hist) {
        track_hist_release(s, e->hist);
        e->hist = NULL;
    }
    uint32_t next_gen = e->generation + 1;
    /* Preserve the co-allocated request-bidi receive buffer across reuse. */
    uint8_t *recv_buf = e->rs.recv_buf;
    size_t   recv_cap = e->rs.recv_cap;
    memset(e, 0, sizeof(*e));
    e->state = MOQ_PUB_FREE;
    e->generation = next_gen;
    e->rs.recv_buf = recv_buf;
    e->rs.recv_cap = recv_cap;
    e->occ_next = -1;      /* the memset zeroed these; -1 is "unlinked" */
    e->occ_prev = -1;
}

/* Emit the deferred PUBLISH_FINISHED for a subscriber-role publication whose
 * Stream Count has now been satisfied -- or whose terminal deadline EXPIRED
 * with no live bound streams left (§9.8) -- then free the entry. Returns
 * MOQ_ERR_WOULD_BLOCK if the event queue (or scratch) cannot take it yet; the
 * caller leaves done_pending set and retries via pub_reap_deferred_dones. */
static moq_result_t pub_finalize_done(moq_session_t *s, int slot)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    /* Stream-correlated profiles: if the request bidi has not FIN'd yet, freeing
     * the entry drops its by-streamref key, so a later FIN/bytes would be
     * misclassified. Reserve a drain-ref to absorb that late FIN. */
    bool need_drain = pe->rs.ref._v != 0 && !pub_peer_fin_observed(pe);
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
        (void)drain_ref_add(s, pe->rs.ref);   /* slot reserved above */
    /* The gate is satisfied or expired: the fill children end with the
     * parent, carrying the done status as their cause. */
    fill_parent_cancel(s, fill_parent_pub(s, (size_t)slot), MOQ_FILL_ERROR_PARENT_DONE,
                       pe->done_status_code);
    pub_free_entry(s, slot);
    return MOQ_OK;
}

/* Count a completed data stream -- FIN'd, or terminated by an identifiable
 * RESET (the receive path counts both at their terminal teardown sites) --
 * toward a subscriber-role publication's PUBLISH_DONE Stream Count. If a
 * deferred PUBLISH_DONE is now satisfied, finalize promptly (a WOULD_BLOCK is
 * retried by pub_reap_deferred_dones). */
void pub_note_stream_processed(moq_session_t *s, moq_publication_t pub)
{
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return;
    moq_pub_entry_t *pe = &s->publishes[slot];
    if (pe->role != MOQ_PUB_ROLE_SUBSCRIBER) return;
    pe->processed_stream_count++;
    /* §9.8: direct count-based finalization only in GATED (see
     * sub_note_stream_processed). */
    if (pe->done_pending && !pe->done_expired &&
        pe->done_stream_count != s->profile->stream_count_unknown &&
        pe->processed_stream_count >= pe->done_stream_count)
        (void)pub_finalize_done(s, slot);
}

/* Drive deferred publication terminals (§9.8): GATED count-satisfied
 * retries, deadline firing, and the EXPIRED scan/stop/rescan/finalize
 * loop; one blocked entry never aborts the sweep. */
/*
 * Sweep the publication pool for deferred completions, resumably.
 *
 * Starts at s->sweep_slot. SELECT classifies readiness first: an owner with no
 * runnable work skips UNCHARGED. Charged separately thereafter are the durable
 * due-mark, the stream-stop attempt, and the finalize attempt. With a
 * budget, it stops the moment the budget is exhausted and leaves sweep_slot on
 * the owner still to be processed, so the caller can resume exactly there.
 * Returns true when the pool retired, false when it suspended.
 *
 * Owners are evaluated against s->sweep_now_us -- the epoch the sweep began --
 * so a suspended sweep never mixes two clocks. Slots that become due later are
 * picked up by the caller's catch-up sweep, not by this one.
 */
/* Advance the sweep cursor to the successor captured before the work, unless
 * a free already moved it there through the occupancy unlink. */
#define PUB_SWEEP_ADVANCE()                                                   \
    do { if (s->sweep_slot == i)                                              \
             s->sweep_slot = (nxt >= 0) ? (size_t)nxt : s->pub_cap; } while (0)

bool pub_reap_deferred_dones_resumable(moq_session_t *s, uint32_t *budget)
{
    /*
     * Walks the publication OCCUPANCY list, not the pool: the reap's cost
     * follows the live publications, not `pub_cap`. Slot-ordered, so owners are
     * served -- and their completions emitted -- in the old scan's order.
     *
     * `nxt` is captured BEFORE any work, because finalizing frees the entry and
     * unlinks it; the unlink also advances this cursor, which is why the
     * advance below is conditional.
     */
    while (s->sweep_slot < s->pub_cap) {
        if (s->state == MOQ_SESS_CLOSED) return true;  /* finalize may close */

        size_t i = s->sweep_slot;
        int32_t nxt = s->publishes[i].occ_next;
        moq_pub_entry_t *pe = &s->publishes[i];
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_pub_reap_probes++;
#endif
        if (!pe->done_pending) {           /* costs nothing; never suspends */
            PUB_SWEEP_ADVANCE();
            continue;
        }
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_pub_reap_pending++;
#endif

        /* A suspension inside STOP_STREAMS or FINALIZE resumes in that phase:
         * SELECT already qualified this owner, and re-running it could reach a
         * different verdict now that earlier transitions have committed. */
        if (s->sweep_phase == MOQ_SWEEP_PHASE_SELECT) {
            /* SELECT: classify readiness BEFORE spending anything. An owner whose
             * count is unsatisfied and whose deadline is still in the future has no
             * runnable work, so it must skip uncharged -- otherwise a pool of
             * future owners could exhaust the budget and suspend a pass that had
             * nothing to do. */
            bool count_satisfied =
                (pe->done_stream_count != s->profile->stream_count_unknown &&
                 pe->processed_stream_count >= pe->done_stream_count);
            bool due = (s->sweep_now_us >= pe->done_deadline_us);
            if (!pe->done_expired && !count_satisfied && !due)
            {   PUB_SWEEP_ADVANCE();
                continue;                  /* not runnable: uncharged skip */
            }

            if (count_satisfied && !pe->done_expired) {
                /* FINALIZE attempt: one unit. */
                if (budget) {
                    if (*budget == 0) return false;
                    (*budget)--;
                }
                (void)pub_finalize_done(s, (int)i);
                PUB_SWEEP_ADVANCE();
                continue;
            }

            if (!pe->done_expired) {
                /* The durable due-mark is its own charged transition. */
                if (budget) {
                    if (*budget == 0) return false;
                    (*budget)--;
                }
                pe->done_expired = true;
            }
        }

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
            moq_result_t src = session_stop_bound_streams_resumable(s, MOQ_SUBSCRIPTION_INVALID,
                                                     pe->handle, budget);
            if (src == MOQ_SESSION_SUSPENDED)
                return false;              /* rx cursor stays: we resume here */
            if (src < 0) {
                /* Action capacity: this owner is abandoned for now, so its rx
                 * cursor must NOT leak into the next owner's scan. */
                session_sweep_owner_reset(s);
                PUB_SWEEP_ADVANCE();
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
        (void)pub_finalize_done(s, (int)i);
        session_sweep_owner_reset(s);
        PUB_SWEEP_ADVANCE();
    }
    s->sweep_phase = MOQ_SWEEP_PHASE_SELECT;
    return true;
}

#undef PUB_SWEEP_ADVANCE


bool peer_track_alias_in_use(moq_session_t *s, uint64_t alias)
{
    /* A Track Alias is chosen by the publisher of the track, so the aliases
     * the peer assigns name only what we receive: our subscriber-role
     * subscriptions and publications. The aliases we assign to what we
     * publish are a separate space the peer never sees as its own. */
    for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next) {
        const moq_sub_entry_t *e = &s->subs[i];
        if (e->role == MOQ_SUB_ROLE_SUBSCRIBER &&
            e->state == MOQ_SUB_ESTABLISHED && e->track_alias == alias)
            return true;
    }
    for (size_t i = 0; i < s->pub_cap; i++) {
        moq_pub_entry_t *e = &s->publishes[i];
        if (e->role != MOQ_PUB_ROLE_SUBSCRIBER || e->track_alias != alias) continue;
        /* A subscriber-role publication holds its alias from creation, not
         * just once established: a pending publication already routes early
         * objects, so a second inbound PUBLISH reusing the alias is
         * ambiguous. */
        if (e->state == MOQ_PUB_ESTABLISHED ||
            e->state == MOQ_PUB_PENDING_SUBSCRIBER)
            return true;
    }
    return false;
}

bool peer_alias_conflicts(moq_session_t *s, uint64_t alias,
                          const moq_track_hist_t *hist)
{
    if (!s->profile->allows_concurrent_subscriptions)
        return peer_track_alias_in_use(s, alias);
    /* Track identity is the track-history record: one per full track name,
     * shared by every entry of that track. */
    for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next) {
        const moq_sub_entry_t *e = &s->subs[i];
        if (e->role == MOQ_SUB_ROLE_SUBSCRIBER &&
            e->state == MOQ_SUB_ESTABLISHED && e->track_alias == alias &&
            e->hist != hist)
            return true;
    }
    for (size_t i = 0; i < s->pub_cap; i++) {
        moq_pub_entry_t *e = &s->publishes[i];
        if (e->role != MOQ_PUB_ROLE_SUBSCRIBER || e->track_alias != alias) continue;
        if ((e->state == MOQ_PUB_ESTABLISHED ||
             e->state == MOQ_PUB_PENDING_SUBSCRIBER) && e->hist != hist)
            return true;
    }
    return false;
}

static bool track_key_same(const uint8_t *a, size_t alen, const uint8_t *key,
                           size_t key_len, const moq_namespace_t *ns,
                           moq_bytes_t name)
{
    if (!a && alen > 0) return false;
    if (key || !ns)
        return alen == key_len && (alen == 0 || memcmp(a, key, alen) == 0);
    return moq_track_key_matches(a, alen, ns, name);
}

bool outbound_alias_shareable(moq_session_t *s, uint64_t alias,
                              const uint8_t *key, size_t key_len,
                              const moq_namespace_t *ns, moq_bytes_t name)
{
    if (!s->profile->allows_concurrent_subscriptions) return false;
    for (size_t i = 0; i < s->sub_cap; i++) {
        const moq_sub_entry_t *e = &s->subs[i];
        if (e->role != MOQ_SUB_ROLE_PUBLISHER ||
            e->state != MOQ_SUB_ESTABLISHED || e->track_alias != alias)
            continue;
        if (!track_key_same(e->track_id_buf, e->track_id_len, key, key_len, ns, name))
            return false;
    }
    for (size_t i = 0; i < s->pub_cap; i++) {
        const moq_pub_entry_t *e = &s->publishes[i];
        if (e->role != MOQ_PUB_ROLE_PUBLISHER || e->state == MOQ_PUB_FREE ||
            e->track_alias != alias)
            continue;
        if (!e->hist ||
            !track_key_same(e->hist->key, e->hist->key_len, key, key_len, ns, name))
            return false;
    }
    return true;
}

/* -- Owners of a peer-assigned alias (receive side) ------------------ */

static bool pub_alias_member(const moq_pub_entry_t *e, uint64_t alias)
{
    return e->role == MOQ_PUB_ROLE_SUBSCRIBER && e->track_alias == alias &&
           (e->state == MOQ_PUB_ESTABLISHED ||
            e->state == MOQ_PUB_PENDING_SUBSCRIBER);
}

void alias_iter_begin(moq_session_t *s, moq_alias_iter_t *it, uint64_t alias)
{
    it->alias = alias;
    it->sub = sub_find_by_alias_subscriber(s, alias);
    it->pub = s->pub_occ_head;   /* walks the live publications only */
}

bool alias_iter_next(moq_session_t *s, moq_alias_iter_t *it,
                     moq_subscription_t *sub, moq_publication_t *pub)
{
    *sub = MOQ_SUBSCRIPTION_INVALID;
    *pub = MOQ_PUBLICATION_INVALID;
    if (it->sub >= 0) {
        *sub = s->subs[it->sub].handle;
        it->sub = sub_alias_next(s, it->sub);
        return true;
    }
    while (it->pub >= 0) {
        const moq_pub_entry_t *e = &s->publishes[it->pub];
        it->pub = e->occ_next;
        if (pub_alias_member(e, it->alias)) { *pub = e->handle; return true; }
    }
    return false;
}

size_t alias_owner_count(moq_session_t *s, uint64_t alias)
{
    size_t n = sub_alias_count(s, alias);
    for (int32_t i = s->pub_occ_head; i >= 0; i = s->publishes[i].occ_next)
        if (pub_alias_member(&s->publishes[i], alias)) n++;
    return n;
}

static void pub_window(const moq_pub_entry_t *e, uint64_t loc_max,
                       moq_resolved_window_t *w)
{
    /* An acknowledged update resolves against its own Largest snapshot. */
    if (e->window.has_window) {
        *w = e->window;
        return;
    }
    moq_resolve_filter_window(e->filter_type, e->req_start_group,
                              e->req_start_object, e->req_end_group,
                              e->req_end_object, e->publish_has_largest,
                              e->publish_largest_group,
                              e->publish_largest_object, loc_max, w);
}

bool pub_accepts_group(const moq_pub_entry_t *e, uint64_t loc_max, uint64_t group)
{
    moq_resolved_window_t w;
    pub_window(e, loc_max, &w);
    if (w.unsatisfiable) return false;
    if (group < w.start_group) return false;
    if (w.has_end && group > w.end_group) return false;
    return true;
}

bool pub_accepts_object(const moq_pub_entry_t *e, uint64_t loc_max,
                        uint64_t group, uint64_t object)
{
    if (!e->send_allowed) return false;
    moq_resolved_window_t w;
    pub_window(e, loc_max, &w);
    if (w.unsatisfiable) return false;
    if (group < w.start_group ||
        (group == w.start_group && object < w.start_object))
        return false;
    if (w.has_end && group > w.end_group) return false;
    if (w.has_end_object && group == w.end_group && object > w.end_object)
        return false;
    return true;
}

bool alias_owner_accepts_object(moq_session_t *s, moq_subscription_t sub,
                                moq_publication_t pub,
                                uint64_t group, uint64_t object)
{
    if (moq_subscription_is_valid(sub)) {
        int i = sub_resolve_handle(s, sub);
        return i >= 0 && sub_accepts_object(&s->subs[i], group, object);
    }
    int i = pub_resolve_handle(s, pub);
    return i >= 0 && pub_accepts_object(&s->publishes[i],
                                        s->profile->location_varint_max,
                                        group, object);
}

bool alias_owner_accepts_group(moq_session_t *s, moq_subscription_t sub,
                               moq_publication_t pub, uint64_t group)
{
    if (moq_subscription_is_valid(sub)) {
        int i = sub_resolve_handle(s, sub);
        return i >= 0 && sub_accepts_group(&s->subs[i], group);
    }
    int i = pub_resolve_handle(s, pub);
    return i >= 0 && pub_accepts_group(&s->publishes[i],
                                       s->profile->location_varint_max, group);
}

moq_track_hist_t *alias_owner_hist(moq_session_t *s, moq_subscription_t sub,
                                   moq_publication_t pub)
{
    if (moq_subscription_is_valid(sub)) {
        int i = sub_resolve_handle(s, sub);
        return i >= 0 ? s->subs[i].hist : NULL;
    }
    int i = pub_resolve_handle(s, pub);
    return i >= 0 ? s->publishes[i].hist : NULL;
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
        return queue_send_bidi(s, s->publishes[slot].rs.ref,
                               data, len, fin);
    return queue_send_control(s, data, len);
}

/* -- Inbound PUBLISH handler --------------------------------------- */

moq_result_t session_core_on_publish(moq_session_t *s,
                                      moq_decoded_publish_t *d,
                                      bool request_fin_observed)
{
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    /* Reserved registry record; released on any pre-commit failure via
     * cleanup_all, or stored on the entry at commit. */
    moq_track_hist_t *phist = NULL;
    size_t scratch_saved = s->event_scratch_len;

    if (event_queue_full(s)) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    /* §9.8 inbound extraction, BEFORE any event/entry/registry mutation:
     * the publisher's timeout Track Properties/extension from THIS PUBLISH
     * (failure keeps the profile's protocol character, as on SUBSCRIBE_OK)
     * -- routed through cleanup_all so staged USE_ALIAS buffers and the
     * auth transaction are released, never leaked. */
    moq_dt_scan_t dtscan;
    if (session_scan_dt_props(s, d->track_properties,
                              d->track_properties_len, false, &dtscan) < 0) {
        result = close_with_error(s, 0x3, "malformed track properties");
        goto cleanup_all;
    }
    uint8_t default_priority = 128;
    if (s->profile->scan_default_publisher_priority &&
        s->profile->scan_default_publisher_priority(d->track_properties,
            d->track_properties_len, &default_priority) < 0) {
        result = close_with_error(s, 0x3, "malformed track properties");
        goto cleanup_all;
    }

    /* Stream-correlated profiles route a pre-commit reject (auth / pool-full) as
     * REQUEST_ERROR + FIN on the request bidi and retire it via the drain ring:
     * the publisher's send half stays open (PUBLISH is not first-and-only), so a
     * late message before it sees our FIN must be discarded, not reclassified as a
     * fresh request. Draft-16 carries the reject on the control channel and never
     * drains (it also handled auth-reject in its profile decode).
     *
     * An observed FIN closes the publisher's send half -- with this message, or
     * on an earlier call whose refusal staging retained -- so nothing can arrive
     * late and the drain reference is unnecessary: the rejection still emits its
     * REQUEST_ERROR + FIN and retires staging, it just takes no reference. This
     * selector is the single drain decision for all four pre-commit branches --
     * preflight and insertion alike. */
    bool reject_drain = d->endpoint.has_stream_ref && !request_fin_observed;

    /* Message-level authorization-token reject (stream-correlated profiles): a
     * REGISTER carried alongside still commits its alias (§10.2.2). */
    if (d->auth_reject_code) {
        result = session_core_reject_initial_request(s, &d->endpoint, &d->auth_txn,
            MOQ_REQUEST_FAMILY_PUBLISH, reject_drain,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = d->auth_reject_code });
        if (result == MOQ_OK && s->state != MOQ_SESS_CLOSED) auth_committed = true;
        goto cleanup_all;
    }

    /* §2.5.1: a PUBLISH whose Track Properties carry an unknown Mandatory Track
     * Property -> the subscriber MUST respond REQUEST_ERROR(UNSUPPORTED_EXTENSION).
     * Auto-reject on the request bidi (no PUBLISH_REQUEST event surfaces). */
    if (d->track_properties_unsupported) {
        result = session_core_reject_initial_request(s, &d->endpoint, &d->auth_txn,
            MOQ_REQUEST_FAMILY_PUBLISH, reject_drain,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id,
                .error_code = MOQ_REQUEST_ERROR_UNSUPPORTED_EXTENSION,
                .reason = (const uint8_t *)"unsupported mandatory track property",
                .reason_len = 36 });
        if (result == MOQ_OK && s->state != MOQ_SESS_CLOSED) auth_committed = true;
        goto cleanup_all;
    }

    int slot = pub_find_free(s);
    if (slot < 0) {
        moq_request_error_encode_args_t full = session_capacity_request_error(
            s, d->request_id, "publish pool full", 17);
        result = session_core_reject_initial_request(s, &d->endpoint, &d->auth_txn,
            MOQ_REQUEST_FAMILY_PUBLISH, reject_drain, &full);
        if (result == MOQ_OK && s->state != MOQ_SESS_CLOSED) auth_committed = true;
        goto cleanup_all;
    }

    /* Reserve this track's registry record, selecting allocation-free
     * first so a full registry and an allocation failure stay distinct.
     * A full registry is NOT a session-fatal parser error: reject with
     * the profile's capacity REQUEST_ERROR via the same pre-commit pattern as
     * the pool-full branch above -- no event, entry, or history is committed
     * until the error is queued, and WOULD_BLOCK leaves it replayable. The
     * session stays open. This rejection stays AHEAD of the later
     * duplicate-alias close, as recorded. */
    bool phist_full = moq_track_hist_select(s, &d->track_namespace,
                                            d->track_name) ==
                      MOQ_TH_SEL_FULL;
    if (!phist_full) {
        /* Funded execution: an existing record takes a ref, a free slot
         * creates one. A failed key copy is MOQ_ERR_NOMEM with nothing
         * mutated and no response queued -- never the history-full
         * rejection below. */
        moq_result_t hrc = track_hist_reserve_selected(
            s, &d->track_namespace, d->track_name, &phist);
        if (hrc < 0) { result = hrc; goto cleanup_all; }
    }
    if (phist_full) {
        moq_request_error_encode_args_t full = session_capacity_request_error(
            s, d->request_id, "track history full", 18);
        result = session_core_reject_initial_request(s, &d->endpoint, &d->auth_txn,
            MOQ_REQUEST_FAMILY_PUBLISH, reject_drain, &full);
        if (result == MOQ_OK && s->state != MOQ_SESS_CLOSED) auth_committed = true;
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

    /* Draft-22 §3.1.3 closes only when the alias names "a different Track
     * with an Established subscription"; drafts 16 and 18, with one
     * subscription per track and role, close on any reuse. */
    if (peer_alias_conflicts(s, d->track_alias, phist)) {
        result = close_with_error(s, 0x5, "duplicate track alias");
        goto cleanup_all;
    }

    moq_pub_entry_t *entry = &s->publishes[slot];
    uint32_t live_gen = entry->generation | 1;
    uint64_t packed = moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION,
                                       s->session_tag, live_gen,
                                       (uint32_t)slot);
    moq_publication_t handle = { packed };

    const uint8_t track_order = s->profile->track_properties_group_order(
        d->track_properties, d->track_properties_len);
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
    e.u.publish_request.has_largest = d->has_largest;
    e.u.publish_request.largest_group = d->has_largest ? d->largest_group : 0;
    e.u.publish_request.largest_object = d->has_largest ? d->largest_object : 0;
    e.u.publish_request.has_expires = d->has_expires;
    e.u.publish_request.expires_ms = d->has_expires ? d->expires_ms : 0;

    rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        result = rc;
        goto cleanup_all;
    }

    entry->generation = live_gen;
    /* Allocated: join the occupancy list the preamble scans walk. */
    pub_occ_link(s, (size_t)slot);
    entry->state = MOQ_PUB_PENDING_SUBSCRIBER;
    entry->role = MOQ_PUB_ROLE_SUBSCRIBER;
    entry->handle = handle;
    entry->request_id = d->request_id;
    entry->track_alias = d->track_alias;
    entry->dt_pub_has_object   = dtscan.has_object;
    entry->dt_pub_object_ms    = dtscan.object_ms;
    entry->dt_pub_has_subgroup = dtscan.has_subgroup;
    entry->dt_pub_subgroup_ms  = dtscan.subgroup_ms;
    entry->hist = phist;
    /* Max-merge the publisher-advertised Largest into the reserved record AT
     * COMMIT: relay largest reporting and later relative-window
     * resolution read fresh history. Monotonic/idempotent; values are wire
     * varints, so the profile ceiling (§4) is enforced by the codec. Placed
     * after push_event so a WOULD_BLOCK'd (replayable) request mutates nothing. */
    if (d->has_largest && phist)
        track_hist_merge(phist, d->largest_group, d->largest_object);
    /* Retain the EXACT Largest this PUBLISH advertised. It becomes the Joining
     * Location only once PUBLISH_OK establishes an eligible state (5.1); it is
     * deliberately not read back from history, which keeps moving. */
    entry->publish_has_largest = d->has_largest;
    entry->publish_largest_group = d->has_largest ? d->largest_group : 0;
    entry->publish_largest_object = d->has_largest ? d->largest_object : 0;
    /* Gates outbound new-group requests on the accept and later updates. */
    entry->dynamic_groups = d->dynamic_groups;
    /* Initial Forward State (§9.4): with FORWARD omitted/1 the publisher may begin
     * sending objects immediately, possibly before our PUBLISH_OK. Record it so the
     * inbound data path accepts those early objects on this still-pending
     * publication (send_allowed is only consumed by the publisher-role send path, so
     * setting it on this subscriber-role entry is inert there). */
    entry->send_allowed = d->forward;
    /* The publisher's other initial settings; the accept may change them. */
    entry->subscriber_priority =
        d->has_subscriber_priority ? d->subscriber_priority : 128;
    entry->group_order = d->has_group_order ? d->group_order
                                            : (uint8_t)MOQ_GROUP_ORDER_DEFAULT;
    entry->track_group_order = track_order;
    entry->default_publisher_priority = default_priority;
    if (s->profile->publish_initial_parameters) {
        entry->d22_initial = d->initial;
        if (!d->has_group_order)
            entry->d22_initial.group_order = entry->track_group_order == MOQ_GROUP_ORDER_DEFAULT
                ? MOQ_GROUP_ORDER_ASCENDING : entry->track_group_order;
        entry->group_order = (uint8_t)entry->d22_initial.group_order;
        entry->dt_sub_has_object = (d->initial.flags & MOQ_PUBLISH_INITIAL_HAS_OBJECT_TIMEOUT) != 0;
        entry->dt_sub_object_ms = d->initial.object_delivery_timeout_ms;
        entry->dt_sub_has_subgroup = (d->initial.flags & MOQ_PUBLISH_INITIAL_HAS_SUBGROUP_TIMEOUT) != 0;
        entry->dt_sub_subgroup_ms = d->initial.subgroup_delivery_timeout_ms;
        entry->has_delivery_timeout = entry->dt_sub_has_object || entry->dt_sub_has_subgroup;
        entry->delivery_timeout_ms = dt_negotiate_ms(entry->dt_sub_has_object,
            entry->dt_sub_object_ms, entry->dt_sub_has_subgroup, entry->dt_sub_subgroup_ms);
    }
    if (d->has_filter) {
        entry->filter_type = d->filter_type;
        entry->req_start_group = d->filter_start_group;
        entry->req_start_object = d->filter_start_object;
        entry->req_end_group = d->filter_end_group;
        entry->req_end_object = d->filter_end_object;
    } else {
        entry->filter_type = MOQ_SUBSCRIBE_FILTER_NONE;
    }
    d->endpoint.kind = MOQ_REQ_PUBLISH;
    d->endpoint.slot = slot;
    if (d->endpoint.has_stream_ref) {
        /* Hand the request bidi off from the staging sub slot to this publish
         * slot: re-key the stream-ref to (PUBLISH, slot). The request-stream
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
    /* Release the reserved record on every failure path (only stored on the
     * entry at the committed return above; reaching here means it was not). */
    if (phist) track_hist_release(s, phist);
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

    /* D22 PUBLISH_OK changes no initial subscription settings. */
    const bool    fwd  = d->params_absent ? e->publish_forward : d->forward;
    const uint8_t prio = d->params_absent ? e->subscriber_priority : d->subscriber_priority;
    const uint8_t go   = d->params_absent ? e->group_order
                                          : d->group_order;

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_OK;
    ev.detail_size = (uint32_t)sizeof(moq_publish_ok_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.publish_ok.pub = e->handle;
    ev.u.publish_ok.send_allowed = fwd;
    ev.u.publish_ok.subscriber_priority = prio;
    ev.u.publish_ok.group_order = go;
    ev.u.publish_ok.has_delivery_timeout = d->has_delivery_timeout;
    ev.u.publish_ok.delivery_timeout_ms = d->delivery_timeout_ms;
    ev.u.publish_ok.has_expires = d->has_expires;
    ev.u.publish_ok.expires_ms = d->expires_ms;
    ev.u.publish_ok.has_new_group_request = d->has_new_group_request;
    ev.u.publish_ok.new_group_request = d->new_group_request;
    /* The subscriber's SUBSCRIPTION_FILTER, surfaced RAW;
     * the resolved window stays internal (below). */
    ev.u.publish_ok.has_filter = d->has_filter;
    ev.u.publish_ok.filter = d->filter_type;
    ev.u.publish_ok.start_group = d->has_filter ? d->filter_start_group : 0;
    ev.u.publish_ok.start_object = d->has_filter ? d->filter_start_object : 0;
    ev.u.publish_ok.end_group = d->has_filter ? d->filter_end_group : 0;

    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;

    e->state = MOQ_PUB_ESTABLISHED;
    e->send_allowed = fwd;
    e->subscriber_priority = prio;
    e->group_order = go;
    if (!d->params_absent) {
        e->has_delivery_timeout = d->has_delivery_timeout;
        e->delivery_timeout_ms = d->delivery_timeout_ms;
        e->dt_sub_has_object   = d->dt_has_object;
        e->dt_sub_object_ms    = d->dt_object_ms;
        e->dt_sub_has_subgroup = d->dt_has_subgroup;
        e->dt_sub_subgroup_ms  = d->dt_subgroup_ms;
    }
    e->has_expires = d->has_expires;
    e->expires_ms = d->expires_ms;
    if (d->has_filter) {
        /* Keep the raw filter; its window (resolved below) is the
         * enforcement input, never exposed on the event. */
        e->filter_type = d->filter_type;
        e->req_start_group = d->filter_start_group;
        e->req_start_object = d->filter_start_object;
        e->req_end_group = d->filter_end_group;
        e->req_end_object = d->filter_end_object;
    }
    if (d->has_filter || (d->params_absent && e->filter_type != MOQ_SUBSCRIBE_FILTER_NONE)) {
        /* Against the Largest this PUBLISH carried, not the track history
         * now: the history is shared by every request on the track and may
         * have moved since, while the subscriber resolves the same filter
         * against the advertised Largest (draft-22 §9.8: delivery starts
         * relative to the Largest "at the time the publisher begins
         * sending"). Both sides hold one window. */
        moq_resolve_filter_window(e->filter_type,
                                  e->req_start_group,
                                  e->req_start_object,
                                  e->req_end_group,
                                  e->req_end_object,
                                  e->publish_has_largest,
                                  e->publish_largest_group,
                                  e->publish_largest_object,
                                  s->profile->location_varint_max, &e->window);
    }
    /* ESTABLISHED, with the peer's effective Forward/filter now known: latch
     * the Largest this PUBLISH advertised as the Joining Location (5.1). */
    pub_latch_joining_location(s, e);
    return MOQ_OK;
}

/* -- Joining Location (draft-18 5.1 / draft-16 5.1) ---------------- */

bool pub_joining_eligible(const moq_session_t *s, const moq_pub_entry_t *pe)
{
    if (moq_session_uses_request_streams(s))
        return pe->send_allowed;                 /* draft-18: Forward State 1 */
    return pe->filter_type == MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
}

/* Latch an EXPLICIT location as the Joining Location.
 *
 * The caller supplies the exact Largest it is acting on -- for an update this
 * is the snapshot encoded into the REQUEST_UPDATE_OK, never a re-read of
 * moving history. publish_has_largest / publish_largest_* are deliberately NOT
 * touched: those record what the PUBLISH carried at establishment and stay
 * truthful to that, so a later update cannot rewrite the establishment record. */
void pub_latch_joining_location_at(const moq_session_t *s, moq_pub_entry_t *pe,
                                   uint64_t group, uint64_t object)
{
    if (!pub_joining_eligible(s, pe)) return;
    pe->has_joining_loc = true;
    pe->joining_group = group;
    pe->joining_object = object;
}

/* Latch the ESTABLISHMENT-time location: the Largest the PUBLISH carried. */
void pub_latch_joining_location(const moq_session_t *s, moq_pub_entry_t *pe)
{
    if (!pe->publish_has_largest) return;        /* no Largest -> no location */
    pub_latch_joining_location_at(s, pe, pe->publish_largest_group,
                                  pe->publish_largest_object);
}

/* -- Inbound PUBLISH error (REQUEST_ERROR for publish) ------------- */

moq_result_t session_core_on_publish_error(moq_session_t *s, int slot,
    uint64_t error_code, bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len, bool free_now,
    const moq_decoded_redirect_t *redirect)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    /* Stream-correlated profiles keep the entry to drain the request bidi's FIN
     * and close our (the publisher's) still-open send half reciprocally. Reserve
     * the close action up front (reserve-before-mutate). */
    bool close_half = !free_now && pe->rs.ref._v != 0;
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    if (redirect) {
        /* The redirect event stands in for PUBLISH_ERROR; the emitter reports a
         * terminal close as MOQ_OK, so the teardown runs only while open. */
        moq_result_t rrc = session_core_emit_request_redirect(s,
            MOQ_REQUEST_FAMILY_PUBLISH, pe->handle._opaque, redirect,
            error_code, can_retry, retry_after_ms, reason, reason_len);
        if (rrc < 0 || s->state == MOQ_SESS_CLOSED) return rrc;
        if (free_now) {
            pub_free_entry(s, slot);
        } else {
            if (close_half) (void)queue_close_bidi(s, pe->rs.ref);
            pe->state = MOQ_PUB_DRAINING_RESPONSE;
        }
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

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_PUBLISH_ERROR;
    e.detail_size = (uint32_t)sizeof(moq_publish_error_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.publish_error.pub = s->publishes[slot].handle;
    e.u.publish_error.error_code = s->profile->semantic_request_error(error_code);
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
            (void)queue_close_bidi(s, pe->rs.ref);
        pe->state = MOQ_PUB_DRAINING_RESPONSE;
    }
    return MOQ_OK;
}

/* The subscriber refused this side's update of its own PUBLISH. §9.5.1: the
 * publisher terminates the subscription with PUBLISH_DONE(UPDATE_FAILED).
 * The publication stays established until the application sends that
 * terminal (moq_session_finish_publish, which then requires UPDATE_FAILED);
 * no further update may be sent. Reported as PUBLISH_ERROR naming the
 * established publication; a reason no arena of this session's size can
 * hold is dropped, never the session. */
moq_result_t session_core_on_own_publish_update_error(moq_session_t *s, int slot,
    uint64_t error_code, const uint8_t *reason, size_t reason_len)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t ev_reason = {0};
    switch (event_scratch_classify_bytes(s, reason_len)) {
    case MOQ_EVENT_SCRATCH_PERMANENT:
        reason_len = 0;   /* the outcome is the code; see moq_session_update_publication */
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
    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_PUBLISH_ERROR;
    e.detail_size = (uint32_t)sizeof(moq_publish_error_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.publish_error.pub = pe->handle;
    e.u.publish_error.error_code = s->profile->semantic_request_error(error_code);
    e.u.publish_error.reason = ev_reason;
    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }
    pe->update_pending = false;
    pe->update_request_id = 0;
    pe->dt_upd_has_object = pe->dt_upd_has_subgroup = false;
    pe->update_failed = true;
    return MOQ_OK;
}

/* -- Public API ---------------------------------------------------- */

/* The frozen v0 prefixes must never shrink: a v0 caller measures its whole
 * struct against them. These pin "v0 still covers every original field", so
 * moving a v0 field out of the prefix is a build error. */
_Static_assert(MOQ_PUBLISH_CFG_V0_SIZE >=
                   offsetof(moq_publish_cfg_t, track_properties) +
                       sizeof(moq_bytes_t),
               "publish cfg v0 prefix must cover through track_properties");
_Static_assert(MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE >=
                   offsetof(moq_accept_publish_cfg_t, group_order) +
                       sizeof(moq_group_order_t),
               "accept-publish cfg v0 prefix must cover through group_order");

/* Frozen-v0 pointer initializers. A pointer-only init cannot know
 * the caller's allocation, so it must never write past the frozen v0 prefix --
 * an old caller may have allocated exactly that much. Callers that set any
 * appended field use the _sized variants below. */
void moq_publish_cfg_init(moq_publish_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_PUBLISH_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_PUBLISH_CFG_V0_SIZE;
}

void moq_publish_cfg_init_sized(moq_publish_cfg_t *cfg, size_t cfg_size)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about (matches moq_session_cfg_init_sized). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

void moq_accept_publish_cfg_init(moq_accept_publish_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE;
}

void moq_accept_publish_cfg_init_sized(moq_accept_publish_cfg_t *cfg,
                                        size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
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
    const bool has_expires = PUB_CFG_HAS(has_expires) && cfg->has_expires;
    const bool has_priority = PUB_CFG_HAS(subscriber_priority) && cfg->has_subscriber_priority;
    const bool has_order = PUB_CFG_HAS(group_order) && cfg->has_group_order;
    const bool has_location = PUB_CFG_HAS(location) && cfg->location != NULL;
    const bool has_object_dt = PUB_CFG_HAS(object_delivery_timeout_ms) && cfg->has_object_delivery_timeout;
    const bool has_subgroup_dt = PUB_CFG_HAS(subgroup_delivery_timeout_ms) && cfg->has_subgroup_delivery_timeout;
    const bool typed_initial = s->profile->publish_initial_parameters;
    if (!typed_initial && (has_expires || has_priority || has_order || has_location ||
                 has_object_dt || has_subgroup_dt)) return MOQ_ERR_UNSUPPORTED;
    moq_publish_initial_parameters_t initial = {0};
    uint32_t filter_type = MOQ_SUBSCRIBE_FILTER_NONE;
    uint64_t start_group = 0, start_object = 0, end_group = 0, end_object = 0;
    if (typed_initial) {
        initial.struct_size = sizeof(initial);
        initial.flags = MOQ_PUBLISH_INITIAL_HAS_SUBSCRIBER_PRIORITY | MOQ_PUBLISH_INITIAL_HAS_GROUP_ORDER;
        initial.subscriber_priority = has_priority ? cfg->subscriber_priority : 128;
        if (has_order && cfg->group_order != MOQ_GROUP_ORDER_ASCENDING &&
            cfg->group_order != MOQ_GROUP_ORDER_DESCENDING) return MOQ_ERR_INVAL;
        if (has_location) {
            moq_result_t lrc = moq_location_filter_to_core(cfg->location, &filter_type,
                &start_group, &start_object, &end_group, &end_object);
            if (lrc < 0) return lrc;
            moq_local_filter_normalize(s, &filter_type, &start_group);
            initial.flags |= MOQ_PUBLISH_INITIAL_HAS_LOCATION;
            moq_location_filter_from_core(&(moq_core_filter_t){
                .type = filter_type,
                .start_group = start_group, .start_object = start_object,
                .end_group = end_group, .end_object = end_object },
                &initial.location, sizeof(initial.location));
        }
        if (has_object_dt) {
            initial.flags |= MOQ_PUBLISH_INITIAL_HAS_OBJECT_TIMEOUT;
            initial.object_delivery_timeout_ms = cfg->object_delivery_timeout_ms;
        }
        if (has_subgroup_dt) {
            initial.flags |= MOQ_PUBLISH_INITIAL_HAS_SUBGROUP_TIMEOUT;
            initial.subgroup_delivery_timeout_ms = cfg->subgroup_delivery_timeout_ms;
        }
    }
    const uint64_t expires_ms = has_expires ? cfg->expires_ms : 0;

    const moq_auth_token_t *auth_tokens = NULL;
    size_t auth_token_count = 0;
    if (PUB_CFG_HAS(auth_token_count) && cfg->auth_token_count > 0) {
        auth_tokens = cfg->auth_tokens;
        auth_token_count = cfg->auth_token_count;
    }

    /* §9.8 local property scanner: pure, and hoisted ABOVE
     * session_begin_advance so malformed local properties reject before ANY
     * session mutation -- not even the clock/borrow-epoch advance or a reap
     * runs for a call that fails INVAL. */
    if (cfg->track_properties.len > 0 && !cfg->track_properties.data)
        return MOQ_ERR_INVAL;
    if (s->profile->validate_track_properties &&
        s->profile->validate_track_properties(cfg->track_properties.data,
            cfg->track_properties.len) < 0)
        return MOQ_ERR_INVAL;
    moq_dt_scan_t dtscan;
    if (session_scan_dt_props(s, cfg->track_properties.data,
                              cfg->track_properties.len, true, &dtscan) < 0)
        return MOQ_ERR_INVAL;
    uint8_t default_priority = 128;
    if (s->profile->scan_default_publisher_priority &&
        s->profile->scan_default_publisher_priority(cfg->track_properties.data,
            cfg->track_properties.len, &default_priority) < 0)
        return MOQ_ERR_INVAL;
    /* The application may not publish under a reserved session-level
     * namespace. */
    if (s->profile->reserved_session_namespaces &&
        session_namespace_reserved(&cfg->track_namespace))
        return MOQ_ERR_INVAL;

    if (typed_initial) {
        uint8_t preferred = s->profile->track_properties_group_order(
            cfg->track_properties.data, cfg->track_properties.len);
        initial.group_order = has_order ? (uint32_t)cfg->group_order :
            preferred == MOQ_GROUP_ORDER_DEFAULT ? MOQ_GROUP_ORDER_ASCENDING : preferred;
    }
    *out_handle = MOQ_PUBLICATION_INVALID;

    session_begin_advance(s, now_us);

    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_received) return MOQ_ERR_GOAWAY;

    if (moq_validate_full_track_name_min_fields(
            &cfg->track_namespace, cfg->track_name,
            s->profile->min_track_namespace_fields) < 0)
        return MOQ_ERR_INVAL;
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count,
                                 s->profile->auth_token_type_max) < 0)
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

    /* Reserve this track's registry record: objects we publish on
     * this publication max-merge their largest into it as a backstop (the facade
     * also merges via its own reference). Transient key, freed immediately; a
     * full registry fails the local publish with NOMEM before anything is sent
     * (released on the pre-commit failure paths below and in pub_free_entry). */
    /* Declared here; RESERVED only AFTER alias validation below, so the
     * alias-collision / alias-search failure exits cannot leak a reservation
     *. Released on the encode/push failure paths and in
     * pub_free_entry. */
    moq_track_hist_t *phist = NULL;
    bool     adv_has_largest = false;
    uint64_t adv_largest_group = 0, adv_largest_object = 0;
    {
        bool forward = cfg->has_forward ? cfg->forward : true;

        uint64_t alias;
        uint64_t next_alias_after;
        if (cfg->has_track_alias) {
            alias = cfg->track_alias;
            /* Shareable with our other subscriptions to the same track where
             * concurrent subscriptions are allowed (draft-22 §3.1). */
            if ((pub_outbound_alias_in_use(s, alias) ||
                 sub_track_alias_in_use(s, alias)) &&
                !outbound_alias_shareable(s, alias, NULL, 0,
                                          &cfg->track_namespace, cfg->track_name)) {
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

        /* Alias validated: NOW reserve this track's registry record. A full
         * registry fails the local publish with NOMEM before anything is sent. */
        {
            size_t pklen = 0;
            uint8_t *pkey = moq_build_track_key(s, &cfg->track_namespace,
                                                cfg->track_name, &pklen);
            if (!pkey && pklen > 0) {
                s->profile->abort_request(s, &req_ep);
                return MOQ_ERR_NOMEM;
            }
            phist = track_hist_reserve(s, pkey, pklen);
            if (pkey) s->alloc.free(pkey, pklen, s->alloc.ctx);
            if (!phist) {
                s->profile->abort_request(s, &req_ep);
                return MOQ_ERR_NOMEM;
            }
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
            /* LARGEST_OBJECT rides the PUBLISH straight from the registry
             * record: MUST be advertised once Objects have
             * been published on this track; absent when nothing observed. */
            .has_largest = phist->has_largest,
            .largest_group = phist->has_largest ? phist->largest_group : 0,
            .largest_object = phist->has_largest ? phist->largest_object : 0,
            .initial = initial,
            .has_expires = has_expires,
            .expires_ms = expires_ms,
        };
        /* Same snapshot the encoder just used, captured for the entry so the
         * Joining Location can never diverge from what went on the wire. */
        adv_has_largest = args.has_largest;
        adv_largest_group = args.largest_group;
        adv_largest_object = args.largest_object;

        moq_buf_writer_t w;
        moq_buf_writer_init(&w, s->send_buf + s->send_len,
                             s->send_cap - s->send_len);

        moq_result_t rc2 = s->profile->encode_publish(s, &w, &args);
        if (rc2 == MOQ_ERR_BUFFER && s->send_len > 0) {
            moq_buf_writer_t probe;
            rc2 = send_probe_begin(s, &probe);
            if (rc2 == MOQ_OK) {
                rc2 = s->profile->encode_publish(s, &probe, &args);
                rc2 = send_probe_end(s, &probe, rc2);
            }
        }
        if (rc2 < 0) {
            track_hist_release(s, phist);
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
            track_hist_release(s, phist);
            s->profile->abort_request(s, &req_ep);
            return arc;
        }
        s->send_len += encoded_len;

        moq_pub_entry_t *entry = &s->publishes[slot];
        entry->generation |= 1;
        /* Allocated: join the occupancy list the preamble scans walk. */
        pub_occ_link(s, (size_t)slot);
        entry->state = MOQ_PUB_PENDING_PUBLISHER;
        entry->role = MOQ_PUB_ROLE_PUBLISHER;
        entry->request_id = req_ep.request_id;
        entry->track_alias = alias;
        entry->publish_forward = forward;
        if (typed_initial) {
            entry->subscriber_priority = initial.subscriber_priority;
            entry->group_order = (uint8_t)initial.group_order;
            entry->filter_type = filter_type;
            entry->req_start_group = start_group;
            entry->req_start_object = start_object;
            entry->req_end_group = end_group;
            entry->req_end_object = end_object;
            entry->dt_sub_has_object = has_object_dt;
            entry->dt_sub_object_ms = initial.object_delivery_timeout_ms;
            entry->dt_sub_has_subgroup = has_subgroup_dt;
            entry->dt_sub_subgroup_ms = initial.subgroup_delivery_timeout_ms;
            entry->has_delivery_timeout = has_object_dt || has_subgroup_dt;
            entry->delivery_timeout_ms = dt_negotiate_ms(has_object_dt,
                initial.object_delivery_timeout_ms, has_subgroup_dt,
                initial.subgroup_delivery_timeout_ms);
        }
        entry->hist = phist;
        entry->dt_pub_has_object   = dtscan.has_object;
        entry->dt_pub_object_ms    = dtscan.object_ms;
        entry->dt_pub_has_subgroup = dtscan.has_subgroup;
        entry->dt_pub_subgroup_ms  = dtscan.subgroup_ms;
        /* Latch whether OUR track properties advertised dynamic groups:
         * inbound new-group requests on PUBLISH_OK / publication updates are
         * gated on it (the peer MUST NOT send one otherwise, §10.2.13). */
        entry->dynamic_groups = s->profile->track_properties_dynamic_groups(
            cfg->track_properties.data, cfg->track_properties.len);
        entry->track_group_order = s->profile->track_properties_group_order(
            cfg->track_properties.data, cfg->track_properties.len);
        entry->default_publisher_priority = default_priority;
        entry->handle = pub_make_handle(s, (size_t)slot);
        /* Retain the EXACT Largest this PUBLISH put on the wire (the same
         * snapshot the encoder used, not a later history read). It becomes the
         * Joining Location when PUBLISH_OK establishes an eligible state. */
        entry->publish_has_largest = adv_has_largest;
        entry->publish_largest_group = adv_largest_group;
        entry->publish_largest_object = adv_largest_object;
        req_ep.kind = MOQ_REQ_PUBLISH;
        req_ep.slot = slot;
        if (req_stream) {
            /* Correlate the response by the request's bidi stream identity. */
            entry->rs.ref = req_ref;
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

/* A subscriber-side REQUEST_UPDATE on a publication, in two steps so a caller
 * that must send it behind another message can refuse before either is
 * queued. `prepare` takes the request id and encodes into the caller's buffer
 * (nothing else is mutated; on refusal the id is released). `queue` puts the
 * encoded bytes on the request bidi (no FIN) and records the update as the one
 * outstanding; the caller has reserved an action slot and send space. */
typedef struct pub_update_prep {
    moq_request_endpoint_t req_ep;
    moq_request_update_encode_args_t args;
    uint8_t buf[256];
    size_t  len;
} pub_update_prep_t;
static moq_result_t pub_update_prepare(moq_session_t *s, int slot,
                                       const moq_request_update_encode_args_t *args_in,
                                       pub_update_prep_t *prep);
static void pub_update_abort(moq_session_t *s, pub_update_prep_t *prep);
static moq_result_t pub_update_queue(moq_session_t *s, int slot, pub_update_prep_t *prep);

moq_result_t moq_session_accept_publish(
    moq_session_t *s,
    moq_publication_t pub,
    const moq_accept_publish_cfg_t *cfg,
    uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    /* ABI-additive cfg: the new-group-request, filter, and forward fields are
     * appended; callers compiled against the smaller struct remain valid and
     * send nothing. The filter and forward tails are read as COMPLETE SEMANTIC
     * BLOCKS -- gated on their LAST member -- so a struct_size landing partway
     * into a block never yields a torn half-present value. */
#define ACC_CFG_HAS(f) \
    (cfg->struct_size >= offsetof(moq_accept_publish_cfg_t, f) + \
     sizeof(cfg->f))
    if (cfg->struct_size < MOQ_ACCEPT_PUBLISH_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    bool has_new_group_request = false;
    uint64_t new_group_request = 0;
    if (ACC_CFG_HAS(new_group_request) && cfg->has_new_group_request) {
        has_new_group_request = true;
        new_group_request = cfg->new_group_request;
    }
    /* FILTER block: gated on end_group (its last member). */
    bool                   has_filter = false;
    moq_subscribe_filter_t filter = MOQ_SUBSCRIBE_FILTER_NONE;
    uint64_t f_start_group = 0, f_start_object = 0, f_end_group = 0;
    if (ACC_CFG_HAS(end_group) && cfg->filter != MOQ_SUBSCRIBE_FILTER_NONE) {
        has_filter = true;
        filter = cfg->filter;
        f_start_group = cfg->start_group;
        f_start_object = cfg->start_object;
        f_end_group = cfg->end_group;
    }
    /* FORWARD block: gated on forward (its last member). Omission from
     * PUBLISH_OK defaults to 1 (d16 §9.2.2.8 / d18 §10.2.12: only
     * REQUEST_UPDATE means "unchanged"), so the EFFECTIVE value is always
     * defined: an absent/ungated block resolves to forwarding. */
    bool fwd_chosen = ACC_CFG_HAS(forward) && cfg->has_forward;
    bool fwd_effective = fwd_chosen ? cfg->forward : true;
    const bool has_expires = ACC_CFG_HAS(has_expires) && cfg->has_expires;
    const uint64_t expires_ms = has_expires ? cfg->expires_ms : 0;
    /* TIMEOUT block: gated on subgroup_delivery_timeout_ms (its last). */
    bool dt_obj = false, dt_sg = false;
    uint64_t dt_obj_ms = 0, dt_sg_ms = 0;
    if (ACC_CFG_HAS(subgroup_delivery_timeout_ms)) {
        if ((cfg->delivery_timeout_flags &
             ~(MOQ_ACCEPT_PUBLISH_OBJECT_DELIVERY_TIMEOUT |
               MOQ_ACCEPT_PUBLISH_SUBGROUP_DELIVERY_TIMEOUT)) || cfg->reserved)
            return MOQ_ERR_INVAL;
        dt_obj = (cfg->delivery_timeout_flags &
                  MOQ_ACCEPT_PUBLISH_OBJECT_DELIVERY_TIMEOUT) != 0;
        dt_sg = (cfg->delivery_timeout_flags &
                 MOQ_ACCEPT_PUBLISH_SUBGROUP_DELIVERY_TIMEOUT) != 0;
        dt_obj_ms = dt_obj ? cfg->object_delivery_timeout_ms : 0;
        dt_sg_ms = dt_sg ? cfg->subgroup_delivery_timeout_ms : 0;
    }
#undef ACC_CFG_HAS
    /* D22 9.3 restricts PUBLISH_OK to EXPIRES, even for explicit choices
     * equal to the initial setting. Refuse before any advancing effects. */
    const bool bare_response = !s->profile->publish_ok_carries_params;
    if (bare_response && (cfg->has_subscriber_priority ||
        cfg->group_order != MOQ_GROUP_ORDER_DEFAULT || fwd_chosen ||
        has_filter || has_new_group_request || dt_obj || dt_sg))
        return MOQ_ERR_INVAL;
    {
        moq_result_t drc = session_check_dt_selection(s, dt_obj, dt_obj_ms,
                                                      dt_sg, dt_sg_ms);
        if (drc < 0) return drc;
    }
    if (has_expires && !s->profile->publish_initial_parameters)
        return MOQ_ERR_UNSUPPORTED;
    /* Same filter validity rules as the wire encoders (unknown type / inverted
     * range), applied before any advance or mutation. */
    if (has_filter) {
        if (filter != MOQ_SUBSCRIBE_FILTER_NEXT_GROUP &&
            filter != MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT &&
            filter != MOQ_SUBSCRIBE_FILTER_ABSOLUTE_START &&
            filter != MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE)
            return MOQ_ERR_INVAL;
        if (filter == MOQ_SUBSCRIBE_FILTER_ABSOLUTE_RANGE &&
            f_end_group < f_start_group)
            return MOQ_ERR_INVAL;
    }
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
    /* The publisher already terminated from Pending (Publisher) (§5.1) and
     * the done is deferred on its Stream Count: the subscription is
     * logically Terminated -- a response may no longer be sent. */
    if (s->publishes[slot].done_pending)
        return MOQ_ERR_WRONG_STATE;
    /* §10.2.13: a new-group request may ride PUBLISH_OK only when the track
     * carried DYNAMIC_GROUPS == 1. Refused before any mutation. */
    if (has_new_group_request && !s->publishes[slot].dynamic_groups)
        return MOQ_ERR_INVAL;

    /* D22 acceptance preserves the PUBLISH's paused/forwarding state. */
    const bool pub_initial_forward = s->publishes[slot].send_allowed;
    if (bare_response && !fwd_chosen) fwd_effective = pub_initial_forward;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        uint8_t priority = cfg->has_subscriber_priority ?
                           cfg->subscriber_priority : 128;
        moq_publish_ok_encode_args_t ok_args = {
            .request_id = s->publishes[slot].request_id,
            .subscriber_priority = priority,
            .group_order = (uint8_t)cfg->group_order,
            .has_new_group_request = has_new_group_request,
            .new_group_request = new_group_request,
            .has_filter = has_filter,
            .filter = filter,
            .start_group = f_start_group,
            .start_object = f_start_object,
            .end_group = f_end_group,
            /* Always the EFFECTIVE value; the profiles emit the parameter only
             * when it is 0 (omission defaults to 1 on PUBLISH_OK). */
            .has_forward = true,
            .forward = fwd_effective,
            .has_expires = has_expires,
            .expires_ms = expires_ms,
            .dt_has_object = dt_obj,
            .dt_object_ms = dt_obj_ms,
            .dt_has_subgroup = dt_sg,
            .dt_subgroup_ms = dt_sg_ms,
        };
        moq_result_t rc2 = s->profile->encode_publish_ok(s, &w, &ok_args);
        if (rc2 < 0) return rc2;

        /* PUBLISH_OK carries no FIN -- the subscription lives on the bidi. A
         * send-buffer shortfall is retryable (WOULD_BLOCK), not BUFFER. */
        moq_result_t arc = queue_publish_response(s, (size_t)slot, buf,
                                                  moq_buf_writer_offset(&w),
                                                  false);
        if (arc < 0) return arc;
    }

    /* Acceptance changes STATE only: this slot was allocated -- and linked --
     * when the peer's PUBLISH created it. */
    s->publishes[slot].state = MOQ_PUB_ESTABLISHED;

    /* Legacy response omission means Forward 1; D22 preserves the initial
     * PUBLISH state. The filter in effect is the one this answer chose, else
     * the PUBLISH's own; its window resolves against the Largest the PUBLISH
     * advertised, as the publisher's does against its track history. */
    s->publishes[slot].send_allowed = fwd_effective;
    if (cfg->group_order != MOQ_GROUP_ORDER_DEFAULT)
        s->publishes[slot].group_order = (uint8_t)cfg->group_order;
    if (dt_obj || dt_sg) {
        moq_pub_entry_t *pe = &s->publishes[slot];
        pe->dt_sub_has_object = dt_obj;
        pe->dt_sub_object_ms = dt_obj_ms;
        pe->dt_sub_has_subgroup = dt_sg;
        pe->dt_sub_subgroup_ms = dt_sg_ms;
    }
    if (has_filter) {
        s->publishes[slot].filter_type = (uint32_t)filter;
        s->publishes[slot].req_start_group = f_start_group;
        s->publishes[slot].req_start_object = f_start_object;
        s->publishes[slot].req_end_group = f_end_group;
    }
    {
        moq_pub_entry_t *pe = &s->publishes[slot];
        if (pe->filter_type != MOQ_SUBSCRIBE_FILTER_NONE)
            moq_resolve_filter_window(pe->filter_type, pe->req_start_group,
                pe->req_start_object, pe->req_end_group, pe->req_end_object,
                pe->publish_has_largest, pe->publish_largest_group,
                pe->publish_largest_object, s->profile->location_varint_max,
                &pe->window);
    }
    /* ESTABLISHED with the effective Forward/filter now known: the Largest the
     * PUBLISH advertised becomes this subscription's Joining Location (5.1). */
    pub_latch_joining_location(s, &s->publishes[slot]);
    return MOQ_OK;
}

/* Answer a PUBLISH still awaiting this side's answer with REQUEST_ERROR and
 * retire it (shared by moq_session_reject_publish and the malformed-Track
 * refusal). Reserve-before-mutate: a refusal leaves the request as it was. */
static moq_result_t pub_reject_pending(moq_session_t *s, int slot, uint64_t error_code,
                                       bool can_retry, uint64_t retry_after_ms,
                                       const uint8_t *reason, size_t reason_len)
{
    /* Stream-correlated profiles finish the request bidi with the terminal
     * REQUEST_ERROR and free the entry; reserve a drain slot up front for the
     * peer's late FIN (unless it already arrived). */
    moq_pub_entry_t *pe = &s->publishes[slot];
    bool req_stream = (pe->rs.ref._v != 0);
    bool need_drain = req_stream && !pub_peer_fin_observed(pe);
    moq_decoded_request_update_t pending;
    moq_result_t rc = moq_request_stream_prepare_pending_reject(s, &pe->rs,
        MOQ_REQUEST_FAMILY_PUBLISH, pub_peer_fin_observed(pe), &pending);
    if (rc < 0 || s->state == MOQ_SESS_CLOSED) goto abort_pending;
    bool cancel_update = pending.endpoint.has_request_id && need_drain;
    /* Objects may already flow on streams before the answer: refusing the
     * PUBLISH abandons "reading any publisher initiated streams associated
     * with that subscription using a STOP_SENDING frame" (draft-22 §9.8,
     * draft-18 §10.10, draft-16 §9.13). Reserve ERROR+FIN and the drain owner;
     * request/data-stream STOPs that cannot fit are issued as actions drain. */
    if (action_queue_avail(s) < 1) {
        rc = MOQ_ERR_WOULD_BLOCK;
        goto abort_pending;
    }
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap) {
        rc = MOQ_ERR_WOULD_BLOCK;
        goto abort_pending;
    }
    size_t fill_actions = fill_parent_actions_needed(s, fill_parent_pub(s, (size_t)slot));
    {
        uint8_t buf[256];
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, buf, sizeof(buf));

        moq_request_error_encode_args_t err_args = {
            .request_id = pe->request_id,
            .error_code = error_code,
            .can_retry = can_retry,
            .retry_after_ms = retry_after_ms,
            .reason = reason,
            .reason_len = reason_len,
        };
        /* A REDIRECT without a target: the all-empty tail means "same
         * session, same track" (the config carries no redirect target). */
        moq_result_t rc2 = reject_apply_redirect(s, &err_args, NULL, false);
        if (rc2 < 0) { rc = rc2; goto abort_pending; }
        rc2 = s->profile->encode_request_error(s, &w, &err_args);
        if (rc2 < 0) { rc = rc2; goto abort_pending; }
        size_t own_len = moq_buf_writer_offset(&w);
        if (pending.endpoint.has_request_id && own_len > s->send_cap) {
            rc = MOQ_ERR_BUFFER;
            goto abort_pending;
        }

        /* Terminal: finish the request bidi (FIN) for stream-correlated profiles.
         * A transient send-buffer shortfall remains retryable. */
        moq_result_t arc = queue_publish_response(s, (size_t)slot, buf,
                                                  own_len,
                                                  req_stream);
        if (arc < 0) { rc = arc; goto abort_pending; }
    }

    if (pending.endpoint.has_request_id) {
        s->profile->commit_inbound_request(s, &pending.endpoint);
        process_auth_tokens_commit_txn(s, &pending.auth_txn);
    }
    process_auth_tokens_free_staging(s, pending.tokens, pending.token_staged,
                                     pending.token_count);
    if (need_drain)
        (void)drain_ref_add_rejected(s, pe->rs.ref, cancel_update); /* reserved */
    drain_ref_retry_owed_stops(s);
    rx_end_bound_streams(s, MOQ_SUBSCRIPTION_INVALID, pe->handle, fill_actions);
    pub_free_entry(s, slot);
    return MOQ_OK;

abort_pending:
    process_auth_tokens_free_staging(s, pending.tokens, pending.token_staged,
                                     pending.token_count);
    process_auth_tokens_abort_txn(s, &pending.auth_txn);
    return rc;
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
    /* REDIRECT answers PUBLISH only where the profile allows it. */
    if (cfg->error_code == MOQ_REQUEST_ERROR_REDIRECT &&
        !s->profile->redirect_on_any_request)
        return MOQ_ERR_INVAL;
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

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_PENDING_SUBSCRIBER)
        return MOQ_ERR_WRONG_STATE;
    /* Logically Terminated by the publisher's deferred done (§5.1 pending
     * termination): a response may no longer be sent. */
    if (s->publishes[slot].done_pending)
        return MOQ_ERR_WRONG_STATE;

    return pub_reject_pending(s, slot, (uint64_t)cfg->error_code, cfg->can_retry,
                              cfg->retry_after_ms, cfg->reason.data, cfg->reason.len);
}

/* The PUBLISH's Track is malformed while this side's answer is still owed
 * (draft-22 §12.1: cancel "any corresponding subscription"): the session
 * refuses it with the profile's PUBLISH refusal carrying UNINTERESTED (0x20),
 * the code a subscriber declines a PUBLISH with; MALFORMED_TRACK (0x12) is
 * registered for a relay publisher's FETCH response, not for this refusal.
 * The application, which holds the PUBLISH_REQUEST, sees PUBLISH_FINISHED
 * with the MOQ_REQUEST_TERMINAL_MALFORMED_TRACK cause and the code sent. Its
 * handle is stale from then on. The terminal event and the answer are
 * reserved first. */
moq_result_t session_pub_reject_pending_malformed(moq_session_t *s, int slot)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    if (pe->role != MOQ_PUB_ROLE_SUBSCRIBER || pe->state != MOQ_PUB_PENDING_SUBSCRIBER ||
        pe->done_pending)
        return MOQ_OK;
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_publication_t handle = pe->handle;
    s->event_reserve++;
    moq_result_t rc = pub_reject_pending(s, slot, MOQ_REQUEST_ERROR_UNINTERESTED,
                                         false, 0, NULL, 0);
    s->event_reserve--;
    if (rc < 0) return rc;
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_PUBLISH_FINISHED;
    e.detail_size = (uint32_t)sizeof(moq_publish_finished_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.publish_finished.pub = handle;
    e.u.publish_finished.cause = MOQ_REQUEST_TERMINAL_MALFORMED_TRACK;
    e.u.publish_finished.stream_error_code = MOQ_REQUEST_ERROR_UNINTERESTED;
    return push_event(s, &e);   /* slot reserved above */
}

/* -- Inbound PUBLISH_DONE handler ---------------------------------- */

/* §9.8 negotiated terminal-wait floor (see sub_dt_floor_us). */
static uint64_t pub_dt_floor_us(const moq_pub_entry_t *e)
{
    uint64_t o = dt_negotiate_ms(e->dt_pub_has_object, e->dt_pub_object_ms,
                                 e->dt_sub_has_object, e->dt_sub_object_ms);
    uint64_t g = dt_negotiate_ms(e->dt_pub_has_subgroup,
                                 e->dt_pub_subgroup_ms,
                                 e->dt_sub_has_subgroup,
                                 e->dt_sub_subgroup_ms);
    return ms_to_us_sat(o > g ? o : g);
}

/* Mirror of sub_retire_crossed_update (see session_subscribe.c). */
static void pub_retire_crossed_update(moq_session_t *s, moq_pub_entry_t *pe,
                                      bool need_tomb)
{
    if (!pe->update_pending) return;
    if (need_tomb) unsub_tomb_add(s, pe->update_request_id);
    request_registry_remove_by_id(s, pe->update_request_id);
    pe->update_pending = false;
    pe->update_request_id = 0;
    pe->update_has_forward = false;   /* never acknowledged */
    pe->update_has_subscriber_priority = false;
    pe->update_has_filter = false;
    pe->dt_upd_has_object = pe->dt_upd_has_subgroup = false;
}

moq_result_t session_core_on_publish_done(moq_session_t *s,
                                           const moq_decoded_publish_done_t *d,
                                           bool free_now)
{
    moq_pub_entry_t *pe = &s->publishes[d->target_slot];
    /* Draft-16 answers a crossed update exactly once by id (tombstone it);
     * draft-18 correlates by request stream and never consumes id
     * tombstones, so it must not spend one. Retired atomically at commit. */
    bool need_tomb = pe->update_pending &&
                     !moq_session_uses_request_streams(s);
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
    bool close_half = !free_now && pe->rs.ref._v != 0;

    /* Stream-Count gating (draft-16 §9.15 / draft-18 §10.11): the terminal
     * done is likely to precede late-arriving / late-opening data streams.
     * Keep the publication live (so those streams still bind its alias and
     * deliver objects) until the advertised number of data streams have been
     * processed; only then surface PUBLISH_FINISHED and free the entry. A
     * Stream Count of 0 (datagram-only) finalizes immediately; the 2^62-1
     * "unknown" sentinel DEFERS and is bounded by the terminal deadline
     * below (§9.8). §5.1 permits the
     * publisher to terminate from Pending (Publisher) too; a pending
     * publication's subgroup streams still bind regardless of Forward State
     * (Forward 0 prohibits Objects, not streams -- empty subgroups count,
     * draft-18 §10.11), so the gate applies uniformly. */
    if (pe->role == MOQ_PUB_ROLE_SUBSCRIBER &&
        d->stream_count != 0 &&
        (d->stream_count == s->profile->stream_count_unknown ||
         pe->processed_stream_count < d->stream_count)) {
        if (close_half && action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
        uint8_t *rbuf = NULL;
        if (d->reason_len > 0) {
            rbuf = (uint8_t *)s->alloc.alloc(d->reason_len, s->alloc.ctx);
            if (!rbuf) return MOQ_ERR_NOMEM;
            memcpy(rbuf, d->reason, d->reason_len);
        }
        pub_retire_crossed_update(s, pe, need_tomb);
        if (pe->done_reason_buf)   /* unreachable: duplicates rejected upstream */
            s->alloc.free(pe->done_reason_buf, pe->done_reason_len, s->alloc.ctx);
        pe->done_reason_buf = rbuf;
        pe->done_reason_len = d->reason_len;
        pe->done_status_code = d->status_code;
        pe->done_stream_count = d->stream_count;
        pe->done_pending = true;
        /* §9.8: stamp the terminal deadline once (see the subscription
         * handler); the sentinel defers and is bounded by this deadline. */
        {
            uint64_t wait_us = s->done_wait_timeout_us;
            uint64_t floor_us = pub_dt_floor_us(pe);
            if (floor_us > wait_us) wait_us = floor_us;
            pe->done_deadline_us = deadline_add(s->last_now_us, wait_us);
            pe->done_expired = false;
        }
        if (close_half)
            (void)queue_close_bidi(s, pe->rs.ref);
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

    pub_retire_crossed_update(s, pe, need_tomb);
    /* The subscription is over: its fill children end now with the done
     * status as their cause, whether the entry is freed or kept to drain
     * the request bidi's FIN. */
    fill_parent_cancel(s, fill_parent_pub(s, (size_t)d->target_slot),
                       MOQ_FILL_ERROR_PARENT_DONE, d->status_code);
    if (free_now) {
        pub_free_entry(s, d->target_slot);
    } else {
        if (close_half)
            (void)queue_close_bidi(s, pe->rs.ref);
        pe->state = MOQ_PUB_DRAINING_RESPONSE;
    }
    return MOQ_OK;
}

/* -- Inbound UNSUBSCRIBE for publisher-role publication ------------ */

/* Reset every open subgroup of publication `slot` on the draft-16 rejected-
 * update path; WOULD_BLOCK leaves the rest for the retry. */
static moq_result_t pub_reset_subgroups(moq_session_t *s, int slot)
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
    return MOQ_OK;
}

moq_result_t session_core_on_publish_unsubscribed(moq_session_t *s, int slot,
                                                  moq_request_terminal_t term,
                                                  size_t reserve_after)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    moq_publication_t pub_handle = pe->handle;

    if (event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;
    /* Freeing the publication resets every fill stream it has open: those
     * resets, and what the caller queues next, are reserved. The subgroup
     * resets are not: each one that does not fit is owed on its stream and
     * issued once capacity returns, so the teardown always completes. */
    size_t keep = fill_parent_actions_needed(s, fill_parent_pub(s, (size_t)slot)) +
                  reserve_after;
    if (keep > action_queue_avail(s))
        return MOQ_ERR_WOULD_BLOCK;
    if (pe->state == MOQ_PUB_ESTABLISHED) {
        for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
            const moq_sg_entry_t *sg = &s->subgroups[i];
            if ((sg->state == MOQ_SG_OPEN || sg->state == MOQ_SG_STREAMING) &&
                moq_publication_is_valid(sg->pub) &&
                moq_publication_eq(sg->pub, pub_handle))
                sg_reset_or_owe(s, (size_t)i, 0x1 /* CANCELLED */, keep);
        }
    }

    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_PUBLISH_UNSUBSCRIBED;
    ev.detail_size = (uint32_t)sizeof(moq_publish_unsubscribed_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    ev.u.publish_unsubscribed.pub = pub_handle;
    ev.u.publish_unsubscribed.cause = term.cause;
    ev.u.publish_unsubscribed.stream_error_code = term.error_code;
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;

    pub_free_entry(s, slot);
    return MOQ_OK;
}

/* -- Inbound REQUEST_UPDATE for publisher-role publication ----------- */

/* A draft-16 rejected update (control channel, no answer owed to the
 * application): REQUEST_ERROR then PUBLISH_DONE(UPDATE_FAILED), after
 * resetting the publication's open subgroups. Draft-16 has no fills, so no
 * fill stream can be open here. */
static moq_result_t pub_reject_update_control(moq_session_t *s,
    const moq_decoded_request_update_t *d, uint64_t error_code)
{
    moq_pub_entry_t *pe = &s->publishes[d->target_slot];
    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_request_error(s, &w,
        &(moq_request_error_encode_args_t){
            .request_id = d->request_id, .error_code = error_code });
    if (rc < 0) return rc;
    size_t split = moq_buf_writer_offset(&w);
    rc = s->profile->encode_publish_done(s, &w,
        &(moq_finish_publish_encode_args_t){
            .request_id = pe->request_id, .status_code = 0x8,
            .stream_count = session_done_stream_count(s,
                pe->subgroup_streams_opened, pe->fill_streams_opened) });
    if (rc < 0) return rc;
    size_t total = moq_buf_writer_offset(&w);
    size_t actions = s->action_cap == 1 ? 1 : 2;
    /* Permanent BUFFER retains the input; draining cannot make it fit. */
    if (total > s->send_cap) return MOQ_ERR_BUFFER;
    if (total > s->send_cap - s->send_len || action_queue_avail(s) < actions)
        return MOQ_ERR_WOULD_BLOCK;

    /* From here WOULD_BLOCK retains teardown progress, not reply/ID/auth
     * commits. Producers must not create new work between reset retries. */
    pe->update_failed = true;
    rc = pub_reset_subgroups(s, d->target_slot);
    if (rc < 0) return rc;
    if (action_queue_avail(s) < actions) return MOQ_ERR_WOULD_BLOCK;
    if (actions == 1) {
        rc = queue_send_control(s, buf, total);
    } else {
        rc = queue_send_control(s, buf, split);
        if (rc < 0) return rc;
        rc = queue_send_control(s, buf + split, total - split);
    }
    if (rc < 0) return rc;
    session_tag_owner_done(s, pe->handle._opaque);
    s->profile->commit_inbound_request(s, &d->endpoint);
    pub_free_entry(s, d->target_slot);
    return MOQ_OK;
}

/* A subscriber's update of a subscription this side's PUBLISH began is
 * answered exactly as an update of a SUBSCRIBE-initiated one (§9.5 makes no
 * distinction): by the session at once, or -- with
 * moq_session_cfg_t.defer_subscription_updates -- by the application through
 * moq_session_accept_publication_update() /
 * moq_session_reject_publication_update(), with the same coalescing, bound
 * and failure terminal (session_subscribe.c holds the shared helpers). */

/* Take the Largest snapshot and admit the update's fill request. The child
 * inherits the update's priority where set, else the publication's
 * negotiated values. A pool with no slot for it does not hold the update up:
 * when the fill's range against this snapshot would open a stream, that
 * stream is opened and reset right after its header along with the
 * acknowledgement -- two more actions on top of `ok_actions`, reserved here.
 * Nothing is committed. */
static void pub_inbound_update_snapshot(moq_session_t *s, moq_pub_entry_t *e,
    moq_sub_update_t *u)
{
    if (!s->profile->subscription_update_options) return;
    moq_sub_update_snapshot_t *v = &e->update_snapshot;
    if (!v->valid || v->request_id != u->request_id) {
        memset(v, 0, sizeof(*v));
        v->valid = true;
        v->request_id = u->request_id;
        v->processed_at_us = s->last_now_us;
        v->has_largest = e->hist && e->hist->has_largest;
        v->largest_group = v->has_largest ? e->hist->largest_group : 0;
        v->largest_object = v->has_largest ? e->hist->largest_object : 0;
        v->subscriber_priority = u->has_subscriber_priority
            ? u->subscriber_priority : e->subscriber_priority;
        v->group_order = session_group_order(e->group_order, e->track_group_order);
        v->forward = u->has_forward ? u->forward : e->send_allowed;
        v->filter_type = u->has_filter ? u->filter_type : e->filter_type;
        v->start_group = u->has_filter ? u->start_group : e->req_start_group;
        v->start_object = u->has_filter ? u->start_object : e->req_start_object;
        v->end_group = u->has_filter ? u->end_group : e->req_end_group;
        v->end_object = u->has_filter ? u->end_object : e->req_end_object;
    }
    u->snapshot = *v;
}

/* Action slots the update's subscriber priority change needs. */
static size_t pub_inbound_update_priority_slots(const moq_session_t *s,
                                                const moq_pub_entry_t *e,
                                                const moq_sub_update_t *u)
{
    return session_owner_priority_slots(s, u->has_subscriber_priority,
                                        u->subscriber_priority,
                                        e->subscriber_priority);
}

static moq_result_t pub_inbound_update_prepare(moq_session_t *s, size_t slot,
                                       const moq_sub_update_t *u, size_t ok_actions,
                                       moq_update_plan_t *p)
{
    moq_pub_entry_t *e = &s->publishes[slot];
    const moq_sub_update_snapshot_t *v = &u->snapshot;
    memset(p, 0, sizeof(*p));
    p->snap_has = v->valid ? v->has_largest : e->hist && e->hist->has_largest;
    p->snap_group = v->valid ? v->largest_group : p->snap_has ? e->hist->largest_group : 0;
    p->snap_object = v->valid ? v->largest_object : p->snap_has ? e->hist->largest_object : 0;
    if (!u->fill.requested) return MOQ_OK;
    moq_result_t rc = fill_inbound_prepare(s, fill_parent_pub(s, slot),
        u->fill_request_id, &u->fill,
        v->valid ? v->subscriber_priority :
            u->has_subscriber_priority ? u->subscriber_priority : e->subscriber_priority,
        v->valid ? v->group_order : e->group_order,
        v->valid ? v->filter_type : u->has_filter ? u->filter_type : e->filter_type,
        v->valid ? v->start_group : u->has_filter ? u->start_group : e->req_start_group,
        v->valid ? v->start_object : u->has_filter ? u->start_object : e->req_start_object,
        v->valid ? v->end_group : u->has_filter ? u->end_group : e->req_end_group,
        v->valid ? v->end_object : u->has_filter ? u->end_object : e->req_end_object,
        &p->fres, &p->feff);
    if (rc == MOQ_ERR_REQUEST_BLOCKED) {
        p->fail_fill = fill_unserved_opens_stream(&p->feff, p->snap_has,
            p->snap_group, p->snap_object,
            !(v->valid ? v->forward : u->has_forward ? u->forward : e->send_allowed));
        if (p->fail_fill && action_queue_avail(s) < ok_actions + 2)
            return MOQ_ERR_WOULD_BLOCK;
        return MOQ_OK;
    }
    if (rc < 0) return rc;
    rc = session_update_freeze_fill(v, p);
    if (rc < 0) return rc;
    p->has_fill = true;
    return MOQ_OK;
}

/* Queue one REQUEST_OK on the PUBLISH request bidi (no FIN -- the
 * subscription continues), or on the control channel on profiles without
 * request streams. */
static moq_result_t pub_inbound_update_queue_ok(moq_session_t *s, bool req_stream,
                                        moq_stream_ref_t ref, uint64_t request_id,
                                        const moq_update_plan_t *p)
{
    uint8_t ok_buf[128];
    size_t ok_len = 0;
    moq_result_t rc = session_update_encode_ok(s, request_id, p, ok_buf, sizeof(ok_buf),
                                       &ok_len);
    if (rc < 0) return rc;
    return req_stream ? queue_send_bidi(s, ref, ok_buf, ok_len, false)
                      : queue_send_control(s, ok_buf, ok_len);
}

/* Apply an acknowledged update. Cannot fail: everything it queues was
 * reserved by pub_inbound_update_prepare and the caller. `ep`, when given, is the
 * update's request endpoint, committed in place. */
static void pub_inbound_update_commit(moq_session_t *s, size_t slot, const moq_sub_update_t *u,
                              const moq_update_plan_t *p,
                              const moq_request_endpoint_t *ep,
                              moq_pub_fill_receipt_t *receipt)
{
    moq_pub_entry_t *e = &s->publishes[slot];
    memset(&e->update_snapshot, 0, sizeof(e->update_snapshot));
    if (pub_inbound_update_priority_slots(s, e, u))
        (void)session_update_owner_priority(s, e->handle._opaque,
                                            u->subscriber_priority);
    if (u->has_subscriber_priority)
        e->subscriber_priority = u->subscriber_priority;
    /* draft-18 5.1: this endpoint is the PUBLISHER, and the REQUEST_UPDATE_OK
     * it queued is the message that changes the Forward State. When that
     * change is 0 -> 1 the publisher MUST save the Largest that ACK carried --
     * and a later publication-keyed Joining FETCH resolves on THIS entry, so
     * omitting it here would leave the serving side with no location while the
     * requester has one.
     *
     * The plan's snapshot is exactly what went into the ACK, so the latch
     * cannot disagree with the wire. It runs only once the response can no
     * longer fail. Draft-16 has no update-driven transition rule, so this is
     * draft-18 and later only. */
    const bool upd_fwd_rising =
        u->has_forward && u->forward && !e->send_allowed;
    if (u->has_forward)
        e->send_allowed = u->forward;
    if (moq_session_uses_request_streams(s) && upd_fwd_rising && p->snap_has) {
        e->has_joining_loc = false;      /* re-latch under the new state */
        pub_latch_joining_location_at(s, e, p->snap_group, p->snap_object);
    }
    if (u->dt_has_object) {
        e->dt_sub_has_object = true;
        e->dt_sub_object_ms = u->dt_object_ms;
    }
    if (u->dt_has_subgroup) {
        e->dt_sub_has_subgroup = true;
        e->dt_sub_subgroup_ms = u->dt_subgroup_ms;
    }
    if (u->has_delivery_timeout) {
        /* Retained legacy _ms projection: EXACT milliseconds, recomputed
         * from the COMPLETE current pair after applying this update's
         * carriers (the event projected only the update's carriers). */
        e->has_delivery_timeout = true;
        e->delivery_timeout_ms = dt_negotiate_ms(
            e->dt_sub_has_object, e->dt_sub_object_ms,
            e->dt_sub_has_subgroup, e->dt_sub_subgroup_ms);
    }
    if (u->has_filter) {
        /* The update's filter replaces the subscription's: later fills
         * inherit it and moq_session_publication_filter() reports it. */
        e->filter_type = u->filter_type;
        e->req_start_group = u->start_group;
        e->req_start_object = u->start_object;
        e->req_end_group = u->end_group;
        e->req_end_object = u->end_object;
        moq_resolve_filter_window(u->filter_type,
                                  u->start_group, u->start_object, u->end_group,
                                  u->end_object,
                                  p->snap_has, p->snap_group, p->snap_object,
                                  s->profile->location_varint_max, &e->window);
    }
    if (ep)
        s->profile->commit_inbound_request(s, ep);
    if (p->has_fill) {
        /* Acknowledged: the child's range resolves against the snapshot the
         * REQUEST_OK carried, paused per the now-current Forward state. */
        fill_inbound_commit(s, &p->fres, &p->feff);
        fill_control_resolved_receipt(s, fill_parent_pub(s, slot), u->fill_request_id,
                              true, 0, p->snap_has, p->snap_group, p->snap_object,
                              !e->send_allowed, receipt);
    } else if (p->fail_fill) {
        fill_open_failed(s, u->fill_request_id, p->feff.subscriber_priority,
                         &e->fill_streams_opened);
    }
}

static void pub_inbound_update_event(const moq_session_t *s, const moq_pub_entry_t *e,
                             const moq_sub_update_t *u,
                             const moq_resolved_token_t *tokens, size_t token_count,
                             const moq_subscription_update_options_t *options,
                             bool answer_owed, uint32_t update_count,
                             moq_event_t *ev)
{
    memset(ev, 0, sizeof(*ev));
    ev->kind = MOQ_EVENT_PUBLISH_UPDATED;
    ev->detail_size = (uint32_t)sizeof(moq_publish_updated_event_t);
    ev->borrow_epoch = s->borrow_epoch;
    moq_publish_updated_event_t *x = &ev->u.publish_updated;
    x->pub = e->handle;
    x->has_subscriber_priority = u->has_subscriber_priority;
    x->subscriber_priority = u->subscriber_priority;
    x->has_forward = u->has_forward;
    x->forward = u->forward;
    x->has_delivery_timeout = !options && u->has_delivery_timeout;
    x->delivery_timeout_us = options ? 0 : u->delivery_timeout_us;
    x->tokens = tokens;
    x->token_count = token_count;
    x->has_new_group_request = u->has_new_group_request;
    x->new_group_request = u->new_group_request;
    x->has_filter = !options && u->has_filter;
    x->filter = options ? 0 : u->filter_type;
    x->start_group = options ? 0 : u->start_group;
    x->start_object = options ? 0 : u->start_object;
    x->end_group = options ? 0 : u->end_group;
    x->options = options;
    x->answer_owed = answer_owed;
    x->fill_requested = u->fill.requested;
    x->update_count = update_count;
    x->update_request_id = u->request_id;
}

/* A failed update on a PUBLISH-initiated subscription: the REQUEST_ERROR(s)
 * answering it, then the terminal PUBLISH_DONE(UPDATE_FAILED) §9.5.1
 * requires, after resetting the publication's live data streams. `n_before`
 * updates of the owed run precede the failed one and fail with it. As for a
 * subscription (sub_update_fail_terminal): only the messages, the fill-child
 * actions and the drain reference are reserved; a subgroup reset that does
 * not fit is owed, and while one is owed the PUBLISH_DONE is held
 * (MOQ_PUB_DONE_HELD, MOQ_DONE returned) until session_release_held_dones
 * queues it after the last reset. Otherwise it goes out now and the caller
 * frees the entry, which also ends every fill child.
 *
 * Freeing the entry drops its request bidi's key while the peer may still be
 * sending on it, so a drain reference absorbs what is in flight unless the
 * peer's FIN was already observed (draft-16 carries the terminal on the
 * control channel and never drains). */
static moq_result_t pub_queue_update_done(moq_session_t *s, size_t slot,
                                          uint64_t done_count)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    uint8_t buf[64];
    moq_buf_writer_t dw;
    moq_buf_writer_init(&dw, buf, sizeof(buf));
    moq_finish_publish_encode_args_t done_args = {
        .request_id = pe->request_id,
        .status_code = 0x8,
        .stream_count = done_count,
    };
    moq_result_t rc = s->profile->encode_publish_done(s, &dw, &done_args);
    if (rc < 0) return rc;
    rc = pe->rs.ref._v != 0
        ? queue_send_bidi(s, pe->rs.ref, buf, moq_buf_writer_offset(&dw), true)
        : queue_send_control(s, buf, moq_buf_writer_offset(&dw));
    if (rc == MOQ_OK) session_tag_owner_done(s, pe->handle._opaque);
    return rc;
}

/* Whether a subgroup or fill stream of publication `slot` still owes the
 * action that closes it. */
static bool pub_owes_closures(const moq_session_t *s, size_t slot)
{
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next)
        if ((s->subgroups[i].state == MOQ_SG_RESET_OWED ||
             s->subgroups[i].state == MOQ_SG_TERMINAL_OWED) &&
            moq_publication_is_valid(s->subgroups[i].pub) &&
            moq_publication_eq(s->publishes[slot].handle, s->subgroups[i].pub))
            return true;
    return fill_parent_actions_needed(s, fill_parent_pub(s, slot)) > 0;
}

void pub_release_held_dones(moq_session_t *s)
{
    for (size_t i = 0; i < s->pub_cap; i++) {
        if (s->publishes[i].state != MOQ_PUB_DONE_HELD || pub_owes_closures(s, i))
            continue;
        if (action_queue_avail(s) < 1)
            return;
        if (s->send_cap - s->send_len < 64) return;
        if (pub_queue_update_done(s, i, s->publishes[i].held_done_count) < 0)
            return;
        pub_free_entry(s, (int)i);
    }
}

static moq_result_t pub_inbound_update_fail_terminal(moq_session_t *s, size_t slot,
                                             const moq_request_error_encode_args_t *ea,
                                             uint32_t n_before)
{
    moq_pub_entry_t *pe = &s->publishes[slot];
    bool req_stream = pe->rs.ref._v != 0;
    bool need_drain = req_stream && !pub_peer_fin_observed(pe);
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    size_t n_err = session_update_error_count(s, n_before);
    size_t err_total = 0, done_len = 0;
    const uint64_t done_count = session_done_stream_count(s,
        pe->subgroup_streams_opened, pe->fill_streams_opened);
    moq_result_t rc = session_update_fail_len(s, &pe->owed_update, pe->request_id,
                         done_count, ea, n_before, &err_total, &done_len);
    if (rc < 0) return rc;
    size_t fills = fill_parent_actions_needed(s, fill_parent_pub(s, slot));
    if (action_queue_avail(s) < n_err + 1)
        return MOQ_ERR_WOULD_BLOCK;
    if (req_stream && err_total + done_len > s->send_cap)
        return MOQ_ERR_BUFFER;              /* can never fit */
    if (err_total + done_len > s->send_cap - s->send_len)
        return MOQ_ERR_WOULD_BLOCK;         /* not now; retryable */

    /* Reserved above: from here nothing refuses. The resets that fit go
     * first, leaving the messages' slots free and, while the queue has
     * them, the fill children's; a fill reset that does not fit is retained
     * by the child and issued once actions drain. */
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
        const moq_sg_entry_t *sg = &s->subgroups[i];
        if ((sg->state == MOQ_SG_OPEN || sg->state == MOQ_SG_STREAMING) &&
            moq_publication_is_valid(sg->pub) &&
            moq_publication_eq(sg->pub, pe->handle))
            sg_reset_or_owe(s, (size_t)i, 0x1 /* CANCELLED */, n_err + 1 + fills);
    }
    uint8_t buf[64 + 1024];
    for (size_t k = 0; k < n_err; k++) {
        size_t len = 0;
        rc = session_update_encode_error(s, &pe->owed_update, ea, k, n_err, buf,
                                 sizeof(buf), &len);
        if (rc < 0) return rc;
        rc = req_stream ? queue_send_bidi(s, pe->rs.ref, buf, len, false)
                        : queue_send_control(s, buf, len);
        if (rc < 0) return rc;
    }
    if (need_drain)
        (void)drain_ref_add(s, pe->rs.ref);
    /* Every sending stream of the publication closes before its PUBLISH_DONE
     * (draft-22 section 9.9): its fill children are cancelled here, not when
     * the entry is freed, and the terminal is held while any subgroup or fill
     * stream still owes its reset. The cancellation keeps the slot reserved
     * for the PUBLISH_DONE. */
    fill_parent_cancel_keep(s, fill_parent_pub(s, slot), MOQ_FILL_ERROR_NONE, 0, 1);
    if (pub_owes_closures(s, slot)) {
        pe->state = MOQ_PUB_DONE_HELD;
        pe->held_done_count = done_count;
        return MOQ_DONE;
    }
    return pub_queue_update_done(s, slot, done_count);
}

/* Surface an update the session would accept and leave its answer to the
 * application (see sub_update_defer). */
static moq_result_t pub_inbound_update_defer(moq_session_t *s, moq_decoded_request_update_t *d,
                                     const moq_sub_update_t *u, size_t scratch_saved,
                                     bool *auth_committed)
{
    moq_pub_entry_t *e = &s->publishes[d->target_slot];
    moq_sub_update_t cum;
    uint32_t count = 0;
    moq_result_t rc = session_owed_update_stage(s, &e->owed_update, u, &cum, &count);
    if (rc < 0) return rc;

    moq_subscription_update_options_t *options = NULL;
    rc = session_update_stage_options(s, &cum, d->tokens, d->token_count, &options);
    if (rc < 0) return rc;
    moq_resolved_token_t *ev_tokens = NULL;
    rc = session_stage_tokens_for_event(s, d->tokens, d->token_staged,
                                        d->token_count, scratch_saved, &ev_tokens);
    if (rc < 0) return rc;
    moq_event_t ev;
    pub_inbound_update_event(s, e, &cum, ev_tokens, d->token_count, options,
                            true, count, &ev);
    rc = push_event(s, &ev);
    if (rc < 0) return rc;

    session_owed_update_record(&e->owed_update, &cum, count);
    memset(&e->update_snapshot, 0, sizeof(e->update_snapshot));
    s->profile->commit_inbound_request(s, &d->endpoint);
    *auth_committed = true;
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    return MOQ_OK;
}

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
    moq_pub_entry_t *e = &s->publishes[d->target_slot];

    /* §9.1.7: an update arriving while this side already owes answers to as
     * many updates on the stream as it advertised it would take closes the
     * session. */
    if (session_owed_update_at_bound(s, &e->owed_update)) {
        process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
                                         d->token_count);
        process_auth_tokens_abort_txn(s, &d->auth_txn);
        return close_with_error(s, s->profile->request_updates_exceeded_error,
            "too many outstanding REQUEST_UPDATEs");
    }

    /* §10.2.13: as with PUBLISH_OK, an inbound new-group request on a
     * publication update is a protocol violation unless OUR track
     * properties advertised DYNAMIC_GROUPS == 1. */
    if (d->has_new_group_request && !e->dynamic_groups) {
        process_auth_tokens_free_staging(s, d->tokens, d->token_staged,
                                         d->token_count);
        process_auth_tokens_abort_txn(s, &d->auth_txn);
        return close_with_error(s, 0x3,
            "NEW_GROUP_REQUEST without dynamic-group support");
    }

    /* A failed update -- an unsupported param or a message-level
     * authorization-token reject -- terminates the subscription exactly as
     * on a SUBSCRIBE-initiated one (§9.5.1): REQUEST_ERROR, then PUBLISH_DONE
     * (UPDATE_FAILED), and the publication ends; no PUBLISH_UPDATED is
     * surfaced. A REGISTER carried alongside still commits its alias (the
     * spec's register-even-on-reject rule), so the auth txn commits on this
     * path, after the output does. Updates whose answer the application
     * still owes fail with it: the same REQUEST_ERROR(s) answer them all.
     * Draft-16 with no answer owed takes pub_reject_update_control. */
    if (d->has_unsupported || d->auth_reject_code) {
        uint64_t err_code = d->auth_reject_code
            ? d->auth_reject_code : MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        if (!req_stream && !e->owed_update.owed) {
            result = pub_reject_update_control(s, d, err_code);
            if (result == MOQ_OK) {
                auth_committed = true;
                process_auth_tokens_commit_txn(s, &d->auth_txn);
            }
            goto cleanup_all;
        }
        rc = pub_inbound_update_fail_terminal(s, (size_t)d->target_slot,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = err_code },
            e->owed_update.owed ? e->owed_update.count : 0);
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        if (rc != MOQ_DONE)                     /* MOQ_DONE: terminal held */
            pub_free_entry(s, d->target_slot);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        result = MOQ_OK;
        goto cleanup_all;
    }

    moq_sub_update_t u;
    session_update_from_decoded(d, &u);

    pub_inbound_update_snapshot(s, e, &u);

    if (event_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }

    /* Request GOAWAY preserves facade ownership: it must reserve any fill
     * before the ACK. Only session-wide application deferral yields to the
     * core's existing migration policy. */
    if (e->facade_defer_updates ||
        (s->defer_subscription_updates && !e->goaway_sent)) {
        rc = pub_inbound_update_defer(s, d, &u, scratch_saved, &auth_committed);
        if (rc < 0) { result = rc; goto cleanup_all; }
        return MOQ_OK;
    }

    /* Plus the changed subscriber priority's owner-wide key. */
    size_t ok_actions = 1 + pub_inbound_update_priority_slots(s, e, &u);
    if (action_queue_avail(s) < ok_actions) {
        result = MOQ_ERR_WOULD_BLOCK;
        goto cleanup_all;
    }

    moq_subscription_update_options_t *options = NULL;
    rc = session_update_stage_options(s, &u, d->tokens, d->token_count, &options);
    if (rc < 0) { result = rc; goto cleanup_all; }
    moq_update_plan_t plan;
    rc = pub_inbound_update_prepare(s, (size_t)d->target_slot, &u, ok_actions, &plan);
    if (rc < 0) { result = rc; goto cleanup_all; }

    /* Copy resolved auth tokens into scratch for borrow-epoch-safe delivery. */
    moq_resolved_token_t *ev_tokens = NULL;
    rc = session_stage_tokens_for_event(s, d->tokens, d->token_staged,
                                        d->token_count, scratch_saved,
                                        &ev_tokens);
    if (rc < 0) { result = rc; goto cleanup_all; }

    rc = pub_inbound_update_queue_ok(s, req_stream, resp_ref, d->request_id, &plan);
    if (rc < 0) { result = rc; goto cleanup_all; }

    moq_event_t ev;
    pub_inbound_update_event(s, e, &u, ev_tokens, d->token_count, options,
                            false, 1, &ev);
    rc = push_event(s, &ev);
    if (rc < 0) { result = rc; goto cleanup_all; }

    pub_inbound_update_commit(s, (size_t)d->target_slot, &u, &plan, &d->endpoint, NULL);
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

/* -- Answering a deferred publication update (public API) ------------ */

/* Resolve the publication an answer names: a live publisher-role
 * publication owing an answer, to the newest update it covers. */
static moq_result_t pub_inbound_update_answer_target(moq_session_t *s, moq_publication_t pub,
                                             uint64_t update_request_id, int *out_slot)
{
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    const moq_pub_entry_t *e = &s->publishes[slot];
    if (e->role != MOQ_PUB_ROLE_PUBLISHER || e->state != MOQ_PUB_ESTABLISHED ||
        !e->owed_update.owed)
        return MOQ_ERR_WRONG_STATE;
    if (update_request_id != e->owed_update.rec.request_id)
        return MOQ_ERR_STALE_HANDLE;
    *out_slot = slot;
    return MOQ_OK;
}

moq_result_t moq_session_accept_publication_update(
    moq_session_t *s, moq_publication_t pub,
    const moq_accept_subscription_update_cfg_t *cfg, uint64_t now_us)
{
    return session_accept_publication_update_receipt(s, pub, cfg, now_us, NULL);
}

moq_result_t session_accept_publication_update_receipt(
    moq_session_t *s, moq_publication_t pub,
    const moq_accept_subscription_update_cfg_t *cfg, uint64_t now_us,
    moq_pub_fill_receipt_t *receipt)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MOQ_ACCEPT_SUBSCRIPTION_UPDATE_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = -1;
    moq_result_t rc = pub_inbound_update_answer_target(s, pub, cfg->update_request_id, &slot);
    if (rc < 0) return rc;
    moq_pub_entry_t *e = &s->publishes[slot];
    bool req_stream = e->rs.ref._v != 0;

    /* All wire ACKs share one output action; reserve the priority update
     * and fill separately before committing any part of the answer. */
    size_t ok_actions = 1 + pub_inbound_update_priority_slots(s, e, &e->owed_update.rec);
    if (action_queue_avail(s) < ok_actions) return MOQ_ERR_WOULD_BLOCK;
    moq_update_plan_t plan;
    rc = pub_inbound_update_prepare(s, (size_t)slot, &e->owed_update.rec, ok_actions,
                                    &plan);
    if (rc < 0) return rc;
    rc = session_update_queue_accept(s, req_stream, e->rs.ref,
                                    &e->owed_update, &plan);
    if (rc < 0) return rc;
    moq_sub_update_t u = e->owed_update.rec;
    session_owed_update_clear(s, &e->owed_update);
    if (receipt) {
        memset(receipt, 0, sizeof(*receipt));
        receipt->requested = u.fill.requested;
        receipt->has_largest = plan.snap_has;
        receipt->largest_group = plan.snap_group;
        receipt->largest_object = plan.snap_object;
    }
    pub_inbound_update_commit(s, (size_t)slot, &u, &plan, NULL, receipt);
    return MOQ_OK;
}

moq_result_t moq_session_reject_publication_update(
    moq_session_t *s, moq_publication_t pub,
    const moq_reject_subscription_update_cfg_t *cfg, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_result_t rc = session_update_reject_cfg_check(s, cfg);
    if (rc < 0) return rc;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = -1;
    rc = pub_inbound_update_answer_target(s, pub, cfg->update_request_id, &slot);
    if (rc < 0) return rc;
    moq_pub_entry_t *e = &s->publishes[slot];

    /* The covered run fails as one (see the subscription reject). */
    moq_request_error_encode_args_t ea;
    session_update_reject_args(&e->owed_update, cfg, &ea);

    /* Reserve the whole terminal before the shared path changes anything,
     * so a refusal leaves the publication exactly as it was: the drain
     * reference, the messages and their bytes. The subgroup and fill resets
     * need no room (see pub_inbound_update_fail_terminal). */
    uint32_t n_before = e->owed_update.count - 1;
    bool req_stream = e->rs.ref._v != 0;
    if (req_stream && !pub_peer_fin_observed(e) &&
        s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;
    if (action_queue_avail(s) < session_update_error_count(s, n_before) + 1)
        return MOQ_ERR_WOULD_BLOCK;
    size_t err_total = 0, done_len = 0;
    rc = session_update_fail_len(s, &e->owed_update, e->request_id,
                         session_done_stream_count(s, e->subgroup_streams_opened,
                                                   e->fill_streams_opened),
                         &ea, n_before, &err_total, &done_len);
    if (rc < 0) return rc;
    if (err_total + done_len > s->send_cap) return MOQ_ERR_BUFFER;
    if (err_total + done_len > s->send_cap - s->send_len) return MOQ_ERR_WOULD_BLOCK;

    rc = pub_inbound_update_fail_terminal(s, (size_t)slot, &ea, n_before);
    if (rc < 0) return rc;   /* unreachable: reserved above */
    if (rc != MOQ_DONE)      /* MOQ_DONE: the terminal is held */
        pub_free_entry(s, slot);
    return MOQ_OK;
}

moq_result_t (moq_session_publication_owed_update)(moq_session_t *s,
    moq_publication_t pub, uint64_t *out_update_request_id,
    bool *out_has_filter, moq_location_filter_cfg_t *out_filter)
{
    return moq_session_publication_owed_update_sized(s, pub,
        out_update_request_id, out_has_filter, out_filter,
        MOQ_LOCATION_FILTER_CFG_V0_SIZE);
}

moq_result_t moq_session_publication_owed_update_sized(moq_session_t *s,
    moq_publication_t pub, uint64_t *out_update_request_id,
    bool *out_has_filter, moq_location_filter_cfg_t *out_filter,
    size_t out_filter_size)
{
    if (!s) return MOQ_ERR_INVAL;
    if (out_filter && out_filter_size < MOQ_LOCATION_FILTER_CFG_V0_SIZE)
        return MOQ_ERR_INVAL;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    const moq_pub_entry_t *e = &s->publishes[slot];
    if (e->role != MOQ_PUB_ROLE_PUBLISHER) return MOQ_ERR_WRONG_STATE;
    /* Ended by the session itself, its terminal held behind owed resets:
     * stale, as once the entry is freed (see moq_session_finish_publish). */
    if (e->state == MOQ_PUB_DONE_HELD) return MOQ_ERR_STALE_HANDLE;
    return session_owed_update_observe(&e->owed_update, out_update_request_id,
        out_has_filter, out_filter, out_filter_size);
}

moq_result_t moq_session_get_publication_update_resolution(moq_session_t *s,
    moq_publication_t pub, uint64_t update_request_id,
    moq_update_resolution_t *out, size_t out_size)
{
    if (!s || !out || out_size < MOQ_UPDATE_RESOLUTION_V0_SIZE) return MOQ_ERR_INVAL;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    const moq_pub_entry_t *e = &s->publishes[slot];
    if (e->role != MOQ_PUB_ROLE_PUBLISHER) return MOQ_ERR_WRONG_STATE;
    if (!s->profile->subscription_update_options) return MOQ_ERR_UNSUPPORTED;
    if (e->state != MOQ_PUB_ESTABLISHED) return MOQ_ERR_WRONG_STATE;
    return session_update_resolution_observe(s, &e->owed_update,
        update_request_id, out, out_size);
}

/* -- Inbound REQUEST_UPDATE from the publisher of a publication -------- */

moq_result_t session_core_on_publication_update_by_publisher(
    moq_session_t *s, moq_decoded_request_update_t *d)
{
    moq_stream_ref_t resp_ref = d->endpoint.stream_ref;
    bool auth_committed = false;
    moq_result_t result = MOQ_OK;
    moq_result_t rc;
    size_t scratch_saved = s->event_scratch_len;
    moq_pub_entry_t *e = &s->publishes[d->target_slot];

    /* An unsupported parameter or a rejected token answers REQUEST_ERROR; the
     * publisher then owes PUBLISH_DONE with UPDATE_FAILED. A REGISTER carried
     * alongside still commits its alias. */
    if (d->has_unsupported || d->auth_reject_code) {
        uint64_t err_code = d->auth_reject_code
            ? d->auth_reject_code : MOQ_REQUEST_ERROR_NOT_SUPPORTED;
        if (action_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }
        uint8_t err_buf[128];
        moq_buf_writer_t ew;
        moq_buf_writer_init(&ew, err_buf, sizeof(err_buf));
        rc = s->profile->encode_request_error(s, &ew,
            &(moq_request_error_encode_args_t){
                .request_id = d->request_id, .error_code = err_code });
        if (rc < 0) { result = rc; goto cleanup_all; }
        rc = queue_send_bidi(s, resp_ref, err_buf, moq_buf_writer_offset(&ew), false);
        if (rc < 0) { result = rc; goto cleanup_all; }
        s->profile->commit_inbound_request(s, &d->endpoint);
        auth_committed = true;
        process_auth_tokens_commit_txn(s, &d->auth_txn);
        /* The publisher now owes PUBLISH_DONE(UPDATE_FAILED) (§9.5.1): held
         * as when an update this side sent is refused, so the terminal
         * check on PUBLISH_DONE applies and no further update is sent. */
        e->update_failed = true;
        result = MOQ_OK;
        goto cleanup_all;
    }

    if (event_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }
    if (action_queue_full(s)) { result = MOQ_ERR_WOULD_BLOCK; goto cleanup_all; }

    moq_sub_update_t u;
    session_update_from_decoded(d, &u);
    moq_subscription_update_options_t *options = NULL;
    rc = session_update_stage_options(s, &u, d->tokens, d->token_count, &options);
    if (rc < 0) { result = rc; goto cleanup_all; }
    moq_resolved_token_t *ev_tokens = NULL;
    rc = session_stage_tokens_for_event(s, d->tokens, d->token_staged,
                                        d->token_count, scratch_saved, &ev_tokens);
    if (rc < 0) { result = rc; goto cleanup_all; }

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

    moq_event_t ev;
    pub_inbound_update_event(s, e, &u, ev_tokens, d->token_count, options,
                            false, 1, &ev);
    rc = push_event(s, &ev);
    if (rc < 0) { result = rc; goto cleanup_all; }

    /* The subscriber's record of the publisher's settings, including its
     * delivery timeouts (the publisher-side carriers). */
    if (d->has_forward) e->send_allowed = d->forward;
    if (d->has_subscriber_priority) e->subscriber_priority = d->subscriber_priority;
    if (d->dt_has_object) {
        e->dt_pub_has_object = true;
        e->dt_pub_object_ms = d->dt_object_ms;
    }
    if (d->dt_has_subgroup) {
        e->dt_pub_has_subgroup = true;
        e->dt_pub_subgroup_ms = d->dt_subgroup_ms;
    }
    s->profile->commit_inbound_request(s, &d->endpoint);
    auth_committed = true;
    process_auth_tokens_commit_txn(s, &d->auth_txn);
    return MOQ_OK;

cleanup_all:
    s->event_scratch_len = scratch_saved;
    process_auth_tokens_free_staging(s, d->tokens, d->token_staged, d->token_count);
    if (!auth_committed)
        process_auth_tokens_abort_txn(s, &d->auth_txn);
    return result;
}

/* -- Update publication (subscriber side) -------------------------- */

/* The complete pre-draft-22 layout, including its trailing padding. */
#define MOQ_PUBLICATION_UPDATE_CFG_SIZE_THROUGH_NEW_GROUP_REQUEST \
    ((offsetof(moq_publication_update_cfg_t, new_group_request) + \
      sizeof(((moq_publication_update_cfg_t *)0)->new_group_request) + \
      (_Alignof(moq_publication_update_cfg_t) - 1)) & \
     ~(_Alignof(moq_publication_update_cfg_t) - 1))
_Static_assert(offsetof(moq_publication_update_cfg_t, location) ==
               MOQ_PUBLICATION_UPDATE_CFG_SIZE_THROUGH_NEW_GROUP_REQUEST,
               "typed publication UPDATE inputs must follow the complete old layout");

/* Parenthesized: defines the symbol, not the header's sized macro. */
void (moq_publication_update_cfg_init)(moq_publication_update_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, MOQ_PUBLICATION_UPDATE_CFG_SIZE_THROUGH_NEW_GROUP_REQUEST);
    cfg->struct_size =
        (uint32_t)MOQ_PUBLICATION_UPDATE_CFG_SIZE_THROUGH_NEW_GROUP_REQUEST;
}

void moq_publication_update_cfg_init_sized(moq_publication_update_cfg_t *cfg,
                                           size_t cfg_size)
{
    if (!cfg) return;
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
}

static moq_result_t pub_update_prepare(moq_session_t *s, int slot,
                                       const moq_request_update_encode_args_t *args_in,
                                       pub_update_prep_t *prep)
{
    (void)slot;
    moq_result_t prc = s->profile->prepare_request(s, &prep->req_ep);
    if (prc < 0) return prc;
    prep->args = *args_in;
    prep->args.request_id = prep->req_ep.request_id;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, prep->buf, sizeof(prep->buf));
    moq_result_t rc = s->profile->encode_request_update(s, &w, &prep->args);
    if (rc < 0) {
        s->profile->abort_request(s, &prep->req_ep);
        return rc;
    }
    prep->len = moq_buf_writer_offset(&w);
    return MOQ_OK;
}

static void pub_update_abort(moq_session_t *s, pub_update_prep_t *prep)
{
    s->profile->abort_request(s, &prep->req_ep);
}

static moq_result_t pub_update_queue(moq_session_t *s, int slot, pub_update_prep_t *prep)
{
    moq_pub_entry_t *e = &s->publishes[slot];
    /* The update travels on the publication's existing request bidi (no FIN);
     * the REQUEST_OK correlates by that stream, so no by-id registration. A
     * send-buffer shortfall is retryable (WOULD_BLOCK), not BUFFER. */
    moq_result_t arc = queue_publish_response(s, (size_t)slot, prep->buf, prep->len, false);
    if (arc < 0) {
        s->profile->abort_request(s, &prep->req_ep);
        return arc;
    }
    if (!moq_session_uses_request_streams(s)) {
        prep->req_ep.kind = MOQ_REQ_PUBLICATION_UPDATE;
        prep->req_ep.slot = slot;
        request_registry_insert_by_id(s, prep->req_ep.request_id, prep->req_ep);
    }

    e->update_pending = true;
    e->update_request_id = prep->req_ep.request_id;
    /* A Forward change takes effect at the ACK (the CURRENT acknowledged
     * Forward state gates object delivery); remember it until then. */
    e->update_has_forward = prep->args.has_forward;
    e->update_forward = prep->args.has_forward ? prep->args.forward : false;
    e->update_has_subscriber_priority = s->profile->subscription_update_options &&
        prep->args.has_subscriber_priority;
    e->update_subscriber_priority = prep->args.subscriber_priority;
    e->update_has_filter = prep->args.has_filter;
    e->update_filter_type = prep->args.filter;
    e->update_start_group = prep->args.filter_start_group;
    e->update_start_object = prep->args.filter_start_object;
    e->update_end_group = prep->args.filter_end_group;
    e->update_end_object = prep->args.filter_end_object;
    /* Independent exact carriers pend until ACK; legacy generic selects both. */
    e->dt_upd_has_object = prep->args.dt_has_object || prep->args.has_delivery_timeout;
    e->dt_upd_has_subgroup = prep->args.dt_has_subgroup || prep->args.has_delivery_timeout;
    e->dt_upd_object_ms = prep->args.has_delivery_timeout
        ? prep->args.delivery_timeout_us / 1000u : prep->args.dt_object_ms;
    e->dt_upd_subgroup_ms = prep->args.has_delivery_timeout
        ? prep->args.delivery_timeout_us / 1000u : prep->args.dt_subgroup_ms;
    s->profile->commit_request(s, &prep->req_ep);
    return MOQ_OK;
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
    bool typed_options = s->profile->subscription_update_options;
    bool has_priority = cfg->has_subscriber_priority;
    uint8_t priority = has_priority ? cfg->subscriber_priority : 0;
    bool has_forward = cfg->has_forward;
    bool forward = has_forward && cfg->forward;
    bool has_generic_timeout = cfg->has_delivery_timeout;
    uint64_t generic_timeout_us = has_generic_timeout ? cfg->delivery_timeout_us : 0;
    if (typed_options && has_generic_timeout) return MOQ_ERR_UNSUPPORTED;

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
    bool has_location = UPD_CFG_HAS(location) && cfg->location;
    bool has_fill = UPD_CFG_HAS(fill) && cfg->fill;
    uint32_t timeout_flags = 0;
    uint64_t object_ms = 0, subgroup_ms = 0;
    if (UPD_CFG_HAS(subgroup_delivery_timeout_ms)) {
        timeout_flags = cfg->delivery_timeout_flags;
        if ((timeout_flags & ~(MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT |
                MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT)) || cfg->reserved)
            return MOQ_ERR_INVAL;
        if (timeout_flags & MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT)
            object_ms = cfg->object_delivery_timeout_ms;
        if (timeout_flags & MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT)
            subgroup_ms = cfg->subgroup_delivery_timeout_ms;
    }
    if (!typed_options && (has_location || has_fill))
        return MOQ_ERR_UNSUPPORTED;
    if (timeout_flags && has_generic_timeout) return MOQ_ERR_INVAL;
    {
        moq_result_t drc = session_check_dt_selection(s,
            (timeout_flags & MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT) != 0,
            object_ms,
            (timeout_flags & MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT) != 0,
            subgroup_ms);
        if (drc < 0) return drc;
    }
    uint32_t filter = 0;
    uint64_t f_sg = 0, f_so = 0, f_eg = 0, f_eo = 0;
    if (has_location) {
        moq_result_t lrc = moq_location_filter_to_core(cfg->location, &filter,
                                                     &f_sg, &f_so, &f_eg, &f_eo);
        if (lrc < 0) return lrc;
        moq_local_filter_normalize(s, &filter, &f_sg);
    }
    moq_fill_input_t fill_req;
    memset(&fill_req, 0, sizeof(fill_req));
    if (has_fill) {
        moq_result_t frc = moq_fill_input_normalize(cfg->fill, &fill_req);
        if (frc < 0) return frc;
        if (!s->profile->supports_fill) return MOQ_ERR_UNSUPPORTED;
    }
#undef UPD_CFG_HAS
#undef UPD_CFG_MIN
    if (!typed_options && !has_priority && !has_forward &&
        !has_generic_timeout && !timeout_flags && auth_token_count == 0 &&
        !has_new_group_request && !has_fill)
        return MOQ_ERR_INVAL;
    if (has_generic_timeout && generic_timeout_us < 1000)
        return MOQ_ERR_INVAL;
    if (moq_validate_auth_tokens(auth_tokens, auth_token_count,
                                 s->profile->auth_token_type_max) < 0)
        return MOQ_ERR_INVAL;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;

    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;

    moq_pub_entry_t *e = &s->publishes[slot];
    /* The sender of a PUBLISH may update its own request (§9.5). It refreshes
     * its authorization and its own delivery timeouts; Forward, subscriber
     * priority, the Location Filter and NEW_GROUP_REQUEST are the
     * subscriber's to set (PUBLISH_STATE_NOTIFY reports the publisher's
     * state), and FILL_PARAMETERS is subscriber-originated (§3.4, §3.4.1). */
    const bool own_publish = e->role == MOQ_PUB_ROLE_PUBLISHER;
    if (own_publish) {
        if (!s->profile->requester_updates) return MOQ_ERR_UNSUPPORTED;
        if (has_priority || has_forward || has_new_group_request ||
            has_location || has_fill)
            return MOQ_ERR_INVAL;
        if (!auth_token_count && !timeout_flags) return MOQ_ERR_INVAL;
        if (e->owed_update.owed) return MOQ_ERR_WRONG_STATE;
    } else if (e->role != MOQ_PUB_ROLE_SUBSCRIBER) {
        return MOQ_ERR_WRONG_STATE;
    }
    if (e->state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    /* A deferred terminal (done_pending) is logically Terminated: the entry
     * stays live only so late data streams still bind. No control may be
     * sent for the request after its terminal was accepted. */
    if (e->done_pending)
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

    moq_request_update_encode_args_t args = {
        .existing_request_id = e->request_id,
        .has_subscriber_priority = has_priority,
        .subscriber_priority = priority,
        .has_forward = has_forward,
        .forward = forward,
        .has_delivery_timeout = has_generic_timeout,
        .delivery_timeout_us = generic_timeout_us,
        .auth_tokens = auth_tokens,
        .auth_token_count = auth_token_count,
        .has_new_group_request = has_new_group_request,
        .new_group_request = new_group_request,
        .has_filter = has_location,
        .filter = filter,
        .filter_start_group = f_sg,
        .filter_start_object = f_so,
        .filter_end_group = f_eg,
        .filter_end_object = f_eo,
        .dt_has_object = (timeout_flags & MOQ_SUBSCRIPTION_UPDATE_OBJECT_DELIVERY_TIMEOUT) != 0,
        .dt_object_ms = object_ms,
        .dt_has_subgroup = (timeout_flags & MOQ_SUBSCRIPTION_UPDATE_SUBGROUP_DELIVERY_TIMEOUT) != 0,
        .dt_subgroup_ms = subgroup_ms,
        .fill = has_fill ? &fill_req : NULL,
        .sender = own_publish ? MOQ_UPDATE_SENDER_PUBLISHER : MOQ_UPDATE_SENDER_SUBSCRIBER,
    };
    pub_update_prep_t prep;
    moq_result_t rc = pub_update_prepare(s, slot, &args, &prep);
    if (rc < 0) return rc;
    if (action_queue_full(s)) {
        pub_update_abort(s, &prep);
        return MOQ_ERR_WOULD_BLOCK;
    }
    /* The fill child: a pool slot and the event slot its FILL_CREATED needs,
     * both checked before the update is queued. It inherits the update's
     * chosen filter/priority where set, else the acknowledged parent values. */
    moq_fill_reservation_t fres;
    moq_fill_input_t fill_eff;
    if (has_fill) {
        moq_result_t frc = fill_local_prepare(s, fill_parent_pub(s, (size_t)slot),
            prep.req_ep.request_id, &fill_req,
            has_priority ? priority : e->subscriber_priority,
            e->group_order, has_location ? filter : e->filter_type,
            has_location ? f_sg : e->req_start_group,
            has_location ? f_so : e->req_start_object,
            has_location ? f_eg : e->req_end_group,
            has_location ? f_eo : e->req_end_object, &fres, &fill_eff);
        if (frc == MOQ_OK && event_queue_full(s)) frc = MOQ_ERR_WOULD_BLOCK;
        if (frc < 0) {
            pub_update_abort(s, &prep);
            return frc;
        }
    }
    rc = pub_update_queue(s, slot, &prep);
    if (rc < 0) return rc;
    if (has_fill) fill_local_commit(s, &fres, &fill_eff);
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
    /* The session already ended this publication itself (an update it
     * refused on its own): its UPDATE_FAILED terminal is held behind resets
     * it still owes, and goes out once they do. To the caller the handle is
     * stale, as once the entry is freed. That says nothing about delivery:
     * those resets and the terminal are still the session's to send. */
    if (s->publishes[slot].state == MOQ_PUB_DONE_HELD &&
        s->publishes[slot].role == MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[slot].state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->publishes[slot].role != MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (pub_control_reject_teardown(s, &s->publishes[slot])) return MOQ_ERR_WRONG_STATE;
    /* Migrated by an outbound GOAWAY: no PUBLISH_DONE on the request bidi. Check
     * before capacity preflight so the contract is WRONG_STATE, not WOULD_BLOCK. */
    if (s->publishes[slot].goaway_sent) return MOQ_ERR_WRONG_STATE;
    /* An owed update answer comes first: answering it after this terminal
     * is impossible, and leaving it unanswered is not allowed (§9.5). */
    if (s->publishes[slot].owed_update.owed) return MOQ_ERR_WRONG_STATE;
    /* §9.5.1: after a refused update the terminal is UPDATE_FAILED. */
    if (s->publishes[slot].update_failed && cfg->status_code != 0x8)
        return MOQ_ERR_INVAL;

    for (size_t i = 0; i < s->sg_cap; i++) {
        moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->state == MOQ_SG_TERMINAL_OWED && moq_publication_eq(sg->pub, pub))
            return MOQ_ERR_WOULD_BLOCK;
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
    bool req_stream = (pe->rs.ref._v != 0);
    bool need_drain = req_stream && !pub_peer_fin_observed(pe);
    /* PUBLISH_DONE plus a RESET for every fill stream still open: ending the
     * subscription resets them. */
    if (action_queue_avail(s) < 1 + fill_parent_actions_needed(s, fill_parent_pub(s, (size_t)slot)))
        return MOQ_ERR_WOULD_BLOCK;
    if (need_drain && s->drain_ref_count >= s->drain_ref_cap)
        return MOQ_ERR_WOULD_BLOCK;

    /* The Stream Count on the wire: the application's own data streams plus
     * every fill stream the session opened for this publication. */
    moq_finish_publish_encode_args_t args = {
        .request_id = pe->request_id,
        .status_code = cfg->status_code,
        .stream_count = fill_stream_count(s, cfg->stream_count, pe->fill_streams_opened),
        .reason = cfg->reason.data,
        .reason_len = cfg->reason.len,
    };

    uint8_t buf[256];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_publish_done(s, &w, &args);
    if (rc < 0) return rc;
    size_t elen = moq_buf_writer_offset(&w);
    /* The send arena's room is the last thing the terminal can be refused
     * on; check it before any child is touched, on every profile alike. A
     * message larger than the whole arena can never fit: BUFFER. Otherwise a
     * shortfall is only the tail occupied by queued output, which is
     * reclaimed once that output is taken: WOULD_BLOCK, with this
     * publication, its streams and the queues as they were. */
    if (elen > s->send_cap) return MOQ_ERR_BUFFER;
    if (elen > s->send_cap - s->send_len) return MOQ_ERR_WOULD_BLOCK;

    /* Every stream this publication opened is closed before PUBLISH_DONE:
     * the open fill streams are reset first, with the action slots the
     * preflight reserved for them, and the terminal follows. */
    fill_parent_cancel(s, fill_parent_pub(s, (size_t)slot), MOQ_FILL_ERROR_PARENT_DONE,
                       cfg->status_code);
    /* Terminal: finish the request bidi (FIN) for stream-correlated profiles. */
    moq_result_t arc = queue_publish_response(s, (size_t)slot, buf, elen, req_stream);
    if (arc < 0) return arc;
    session_tag_owner_done(s, pe->handle._opaque);

    if (need_drain)
        (void)drain_ref_add(s, pe->rs.ref);   /* slot reserved above */
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
        /* Pure ALIAS RESOLUTION, independent of Forward State: streams bind
         * and count regardless (Forward 0 prohibits Objects, not streams --
         * draft-18 §10.11 counts empty subgroups), and a paused
         * publication's datagram must still resolve HERE so it is dropped
         * at delivery (session_core_on_object_datagram, after the history
         * merge) instead of masquerading as unknown-alias data and entering
         * the staging ring behind an unrelated pending subscription. */
        if (e->state == MOQ_PUB_ESTABLISHED ||
            e->state == MOQ_PUB_PENDING_SUBSCRIBER)
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
    if (cfg->group_id > s->profile->location_varint_max) return MOQ_ERR_INVAL;
    if (cfg->subgroup_id > s->profile->location_varint_max) return MOQ_ERR_INVAL;
    *out_handle = MOQ_SUBGROUP_INVALID;

    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    int pub_slot = pub_resolve_handle(s, pub);
    if (pub_slot < 0) return MOQ_ERR_STALE_HANDLE;
    if (s->publishes[pub_slot].state != MOQ_PUB_ESTABLISHED)
        return MOQ_ERR_WRONG_STATE;
    if (s->publishes[pub_slot].role != MOQ_PUB_ROLE_PUBLISHER)
        return MOQ_ERR_WRONG_STATE;
    if (pub_control_reject_teardown(s, &s->publishes[pub_slot])) return MOQ_ERR_WRONG_STATE;
    if (!s->publishes[pub_slot].send_allowed)
        return MOQ_ERR_WRONG_STATE;

    for (size_t i = 0; i < s->sg_cap; i++) {
        moq_sg_entry_t *e = &s->subgroups[i];
        /* A reset stream carries nothing more (see sg_is_duplicate). */
        if (e->state == MOQ_SG_FREE || e->state == MOQ_SG_RESETTING ||
            e->state == MOQ_SG_RESET_OWED)
            continue;
        if (moq_publication_eq(e->pub, pub) &&
            e->group_id == cfg->group_id && e->subgroup_id == cfg->subgroup_id)
            return MOQ_ERR_INVAL;
    }

    int slot = sg_find_free(s);
    if (slot < 0) return MOQ_ERR_WOULD_BLOCK;

    /* The stream's key goes out ahead of its first SEND_DATA. */
    if (action_queue_avail(s) < (s->data_priority_updates ? 2u : 1u))
        return MOQ_ERR_WOULD_BLOCK;

    bool has_ext = false;
    if (cfg->struct_size >= offsetof(moq_subgroup_cfg_t, object_properties) +
        sizeof(cfg->object_properties))
        has_ext = cfg->object_properties;

    bool eog = false;
    if (cfg->struct_size >= offsetof(moq_subgroup_cfg_t, end_of_group) +
        sizeof(cfg->end_of_group))
        eog = cfg->end_of_group;
    bool first = false;
    if (cfg->struct_size >= offsetof(moq_subgroup_cfg_t, first_object) +
        sizeof(cfg->first_object))
        first = cfg->first_object;

    moq_subgroup_header_encode_args_t hdr_args = {
        .track_alias = s->publishes[pub_slot].track_alias,
        .group_id = cfg->group_id,
        .subgroup_id = cfg->subgroup_id,
        .publisher_priority = cfg->publisher_priority,
        .has_extensions = has_ext,
        .end_of_group = eog,
        .first_object = first,
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

    a.u.send_data.scheduling_priority = UINT64_C(0x10000) |
        ((uint64_t)s->publishes[pub_slot].subscriber_priority << 8) |
        cfg->publisher_priority;

    /* The header is queued before the entry exists; its scheduling record
     * is the record every later action on this stream will carry. */
    moq_sg_entry_t probe;
    memset(&probe, 0, sizeof(probe));
    probe.sub = MOQ_SUBSCRIPTION_INVALID;
    probe.pub = pub;
    probe.group_id = cfg->group_id;
    probe.subgroup_id = cfg->subgroup_id;
    probe.publisher_priority = cfg->publisher_priority;
    moq_action_meta_t meta;
    sg_stream_meta(s, &probe, &meta);
    meta.subgroup_header = true;

    moq_data_priority_t key;
    session_subgroup_data_key(&meta, &key);
    rc = session_push_data_key(s, a.u.send_data.stream_ref, &key, &meta);
    if (rc < 0) return rc;
    rc = push_action_ex(s, &a, &meta, UINT64_MAX);
    if (rc < 0) return rc;

    moq_sg_entry_t *entry = &s->subgroups[slot];
    entry->generation |= 1;
    /* Allocated: join the occupancy list the preamble scans walk. */
    sg_occ_link(s, (size_t)slot);
    entry->state = MOQ_SG_OPEN;
    entry->publisher_priority = cfg->publisher_priority;
    entry->header_len = a.u.send_data.header_len;
    entry->sub = MOQ_SUBSCRIPTION_INVALID;
    entry->pub = pub;
    entry->stream_ref = a.u.send_data.stream_ref;
    entry->group_id = cfg->group_id;
    entry->subgroup_id = cfg->subgroup_id;
    entry->has_prev_object = false;
    entry->has_extensions = has_ext;
    entry->delivery_deadline_us = UINT64_MAX;
    entry->streaming_deadline_us = UINT64_MAX;
    entry->ovr_has_object = entry->ovr_has_subgroup = false;
    entry->terminal_cause = MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE;
    entry->terminal_code = 0;
    /* Saturates at the profile's unknown Stream Count, which every
     * PUBLISH_DONE clamps to anyway. */
    if (s->publishes[pub_slot].subgroup_streams_opened <
        s->profile->stream_count_unknown)
        s->publishes[pub_slot].subgroup_streams_opened++;
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

moq_result_t moq_session_notify_publication_state(moq_session_t *s,
    moq_publication_t pub, const moq_publish_state_cfg_t *cfg, uint64_t now_us)
{
    if (!s || !cfg) return MOQ_ERR_INVAL;
    if (cfg->struct_size < MOQ_PUBLISH_STATE_CFG_V0_SIZE) return MOQ_ERR_INVAL;
    if (!s->profile->encode_publish_state_notify) return MOQ_ERR_UNSUPPORTED;
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    int slot = pub_resolve_handle(s, pub);
    if (slot < 0) return MOQ_ERR_STALE_HANDLE;
    moq_pub_entry_t *e = &s->publishes[slot];
    if (e->role != MOQ_PUB_ROLE_PUBLISHER || e->state != MOQ_PUB_ESTABLISHED ||
        e->done_pending || e->goaway_sent)
        return MOQ_ERR_WRONG_STATE;
    if (!cfg->has_forward && !cfg->filter) return MOQ_ERR_INVAL;
    moq_publish_state_encode_args_t args;
    memset(&args, 0, sizeof(args));
    args.has_forward = cfg->has_forward;
    args.forward = cfg->forward;
    if (cfg->filter) {
        moq_result_t frc = moq_location_filter_to_core(cfg->filter, &args.filter,
            &args.start_group, &args.start_object, &args.end_group, &args.end_object);
        if (frc < 0) return frc;
        moq_local_filter_normalize(s, &args.filter, &args.start_group);
        args.has_filter = true;
    }
    if (e->hist && e->hist->has_largest) {
        args.has_largest = true;
        args.largest_group = e->hist->largest_group;
        args.largest_object = e->hist->largest_object;
    }
    if (!session_publish_state_matches(&args, e->send_allowed, e->filter_type,
            e->req_start_group, e->req_start_object, e->req_end_group, e->req_end_object))
        return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_CLOSED;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    uint8_t buf[128];
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, sizeof(buf));
    moq_result_t rc = s->profile->encode_publish_state_notify(s, &w, &args);
    if (rc < 0) return rc;
    return queue_publish_response(s, (size_t)slot, buf, moq_buf_writer_offset(&w), false);
}
