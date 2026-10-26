#include <assert.h>
#include <stdlib.h>
#include "session_internal.h"
#include "../bridge/transport_bridge_internal.h"
#include "../internal/validate.h"
#include "session_event_admission.h"
#include "session_outbound_internal.h"

/* -- Advancing-call bookkeeping ------------------------------------ */

static void session_refresh_idle(moq_session_t *s, uint64_t now_us)
{
    if (s->idle_timeout_us > 0 && s->state != MOQ_SESS_CLOSED)
        s->idle_deadline_us = deadline_add(now_us, s->idle_timeout_us);
}

/* True once the armed idle deadline has elapsed. Side-effect-free: callers
 * close via close_with_error so the result/contract stays uniform. Disabled
 * (idle_timeout_us == 0 -> idle_deadline_us == UINT64_MAX) and not-yet-expired
 * sessions report false. The idle timeout is authoritative once it fires:
 * checked before inbound activity refreshes the deadline so a peer that has
 * already gone silent past it cannot revive an expired session by sending a
 * byte just before the timer driver delivers the tick. Must be read after
 * session_begin_advance() has stamped last_now_us. */
static bool session_idle_expired(const moq_session_t *s)
{
    return s->idle_deadline_us != UINT64_MAX &&
           s->last_now_us >= s->idle_deadline_us;
}

moq_result_t moq_session_track_properties_add_group_order(
    const moq_session_t *s, const uint8_t *in, size_t in_len,
    moq_group_order_t order, uint8_t *out, size_t out_cap, size_t *out_len)
{
    if (!s || !out || !out_len || (in_len > 0 && !in)) return MOQ_ERR_INVAL;
    if (order != MOQ_GROUP_ORDER_ASCENDING &&
        order != MOQ_GROUP_ORDER_DESCENDING)
        return MOQ_ERR_INVAL;
    return s->profile->track_properties_add_group_order(
        in, in_len, (uint8_t)order, out, out_cap, out_len);
}

moq_result_t session_check_dt_selection(const moq_session_t *s,
                                       bool has_object, uint64_t object_ms,
                                       bool has_subgroup, uint64_t subgroup_ms)
{
    uint64_t max = s->profile->location_varint_max;
    if ((has_object && object_ms > max) || (has_subgroup && subgroup_ms > max))
        return MOQ_ERR_INVAL;
    if (!s->profile->single_delivery_timeout) return MOQ_OK;
    if ((has_object && object_ms == 0) || (has_subgroup && subgroup_ms == 0))
        return MOQ_ERR_INVAL;
    if (has_object && has_subgroup && object_ms != subgroup_ms)
        return MOQ_ERR_INVAL;
    return MOQ_OK;
}

moq_result_t moq_session_track_properties_add_delivery_timeouts(
    const moq_session_t *s, const uint8_t *in, size_t in_len,
    bool has_object, uint64_t object_ms, bool has_subgroup,
    uint64_t subgroup_ms, uint8_t *out, size_t out_cap, size_t *out_len)
{
    if (!s || !out || !out_len || (in_len > 0 && !in)) return MOQ_ERR_INVAL;
    if (!has_object && !has_subgroup) return MOQ_ERR_INVAL;
    moq_result_t rc = session_check_dt_selection(s, has_object, object_ms,
                                                 has_subgroup, subgroup_ms);
    if (rc < 0) return rc;
    const moq_profile_ops_t *p = s->profile;
    if (p->single_delivery_timeout || !has_subgroup)
        return p->track_properties_add_object_timeout(
            in, in_len, has_object ? object_ms : subgroup_ms, out, out_cap,
            out_len);
    if (!has_object)
        return p->track_properties_add_subgroup_timeout(
            in, in_len, subgroup_ms, out, out_cap, out_len);
    /* Both types: the object one into scratch, then the subgroup one. */
    if (in_len > SIZE_MAX - 16) return MOQ_ERR_INVAL;
    size_t cap = in_len + 16, len = 0;
    uint8_t *tmp = (uint8_t *)s->alloc.alloc(cap, s->alloc.ctx);
    if (!tmp) return MOQ_ERR_NOMEM;
    rc = p->track_properties_add_object_timeout(in, in_len, object_ms, tmp,
                                                cap, &len);
    if (rc >= 0)
        rc = p->track_properties_add_subgroup_timeout(tmp, len, subgroup_ms,
                                                      out, out_cap, out_len);
    s->alloc.free(tmp, cap, s->alloc.ctx);
    return rc;
}

moq_result_t session_scan_dt_props(const moq_session_t *s,
                                   const uint8_t *data, size_t len,
                                   bool strict_local, moq_dt_scan_t *out)
{
    memset(out, 0, sizeof(*out));
    if (!data || len == 0) return MOQ_OK;
    moq_result_t rc = s->profile->scan_delivery_timeouts(data, len,
                                                         strict_local, out);
    if (rc < 0)
        memset(out, 0, sizeof(*out));   /* contract: zeroed on failure (the
                                         * d18 walk may have filled one type
                                         * before hitting a later duplicate) */
    return rc;
}

uint8_t session_resolve_publisher_priority(moq_session_t *s,
                                           moq_subscription_t sub,
                                           moq_publication_t pub,
                                           bool omitted, uint8_t priority)
{
    if (!omitted) return priority;
    if (!s->profile->scan_default_publisher_priority) return 128;
    int slot = sub_resolve_handle(s, sub);
    if (slot >= 0) return s->subs[slot].default_publisher_priority;
    slot = pub_resolve_handle(s, pub);
    if (slot >= 0) return s->publishes[slot].default_publisher_priority;
    return 128; /* No live recipient: callers discard it, never latch this. */
}

/*
 * ---- Pool occupancy lists ------------------------------------------------
 *
 * Every advancing-preamble scan used to walk its whole pool looking for the
 * few slots that were allocated, so its cost followed the CONFIGURED CAPACITY
 * instead of the live owner set. Each pool now carries an intrusive
 * doubly-linked list of its allocated slots, and the scans walk that.
 *
 * The list is kept in ASCENDING SLOT ORDER, which is deliberate: the sweeps
 * then serve owners in exactly the order the full scan served them, so the
 * correction changes only the cost of FINDING live owners and never the order
 * in which completions are emitted.
 *
 * Insertion is O(1) in the common case. `*_find_free()` returns the lowest
 * free slot, so every slot below a newly allocated one is allocated and the
 * predecessor is simply `slot - 1`; the general walk from the head is kept as
 * the correct fallback for any allocator that does not have that property.
 */
#define MOQ_DEFINE_POOL_OCC(NAME, ARR, CAP, HEAD, CURSOR_FIXUP)               \
void NAME##_occ_link(moq_session_t *s, size_t slot)                           \
{                                                                             \
    if (!s || slot >= s->CAP) return;                                         \
    if (s->ARR[slot].occ_linked) return;                                      \
    int32_t prev = -1;                                                        \
    if (slot > 0 && s->ARR[slot - 1].occ_linked) {                            \
        prev = (int32_t)(slot - 1);                                           \
    } else {                                                                  \
        int32_t it = s->HEAD;                                                 \
        while (it >= 0 && (size_t)it < slot) {                                \
            prev = it;                                                        \
            it = s->ARR[it].occ_next;                                         \
        }                                                                     \
    }                                                                         \
    int32_t next = (prev >= 0) ? s->ARR[prev].occ_next : s->HEAD;             \
    s->ARR[slot].occ_prev = prev;                                             \
    s->ARR[slot].occ_next = next;                                             \
    if (prev >= 0) s->ARR[prev].occ_next = (int32_t)slot;                     \
    else           s->HEAD = (int32_t)slot;                                   \
    if (next >= 0) s->ARR[next].occ_prev = (int32_t)slot;                     \
    s->ARR[slot].occ_linked = true;                                           \
}                                                                             \
                                                                              \
void NAME##_occ_unlink(moq_session_t *s, size_t slot)                         \
{                                                                             \
    if (!s || slot >= s->CAP) return;                                         \
    if (!s->ARR[slot].occ_linked) return;                                     \
    int32_t prev = s->ARR[slot].occ_prev;                                     \
    int32_t next = s->ARR[slot].occ_next;                                     \
    if (prev >= 0) s->ARR[prev].occ_next = next;                              \
    else           s->HEAD = next;                                            \
    if (next >= 0) s->ARR[next].occ_prev = prev;                              \
    s->ARR[slot].occ_prev = -1;                                               \
    s->ARR[slot].occ_next = -1;                                               \
    s->ARR[slot].occ_linked = false;                                          \
    CURSOR_FIXUP                                                              \
}

/*
 * A sweep parked on the slot being removed must move to its successor here:
 * the entry is about to leave the list, so resuming from it would follow a
 * stale link. The pools the sweep cursors over carry the fixup; the others
 * need none.
 */
MOQ_DEFINE_POOL_OCC(pub, publishes, pub_cap, pub_occ_head,
    if (s->sweep_active && s->sweep_stage == MOQ_SWEEP_STAGE_PUB &&
        s->sweep_slot == slot) {
        s->sweep_slot = (next >= 0) ? (size_t)next : s->pub_cap;
        session_sweep_owner_reset(s);
    })
MOQ_DEFINE_POOL_OCC(sub, subs, sub_cap, sub_occ_head,
    if (s->sweep_active && s->sweep_stage == MOQ_SWEEP_STAGE_SUB &&
        s->sweep_slot == slot) {
        s->sweep_slot = (next >= 0) ? (size_t)next : s->sub_cap;
        session_sweep_owner_reset(s);
    })
MOQ_DEFINE_POOL_OCC(sg, subgroups, sg_cap, sg_occ_head,
    if (s->sweep_active && s->sweep_stage == MOQ_SWEEP_STAGE_REAP_SUBGROUPS &&
        s->sweep_slot == slot)
        s->sweep_slot = (next >= 0) ? (size_t)next : s->sg_cap;)
/*
 * The rx cursor is only meaningful while a PUB/SUB owner's bound-stream scan is
 * actually running, and it is a plain reset 0 at every other time. Gating on
 * that state is what keeps a retired session's cursor image from becoming a
 * function of which stream happened to be freed last -- an ungated fixup would
 * move it whenever slot 0 was freed outside any sweep.
 */
MOQ_DEFINE_POOL_OCC(rx, rx_streams, rx_cap, rx_occ_head,
    if (s->sweep_active &&
        (s->sweep_stage == MOQ_SWEEP_STAGE_PUB ||
         s->sweep_stage == MOQ_SWEEP_STAGE_SUB) &&
        s->sweep_phase == MOQ_SWEEP_PHASE_STOP_STREAMS &&
        s->sweep_rx_pos == slot)
        s->sweep_rx_pos = (next >= 0) ? (size_t)next : s->rx_cap;)
MOQ_DEFINE_POOL_OCC(fetch, fetches, fetch_cap, fetch_occ_head, /* no cursor */)

#undef MOQ_DEFINE_POOL_OCC


/*
 * Drive the two-pool deferred-completion sweep to its next stopping point.
 *
 * Pool order is fixed (publications, then subscriptions) so a resumed sweep
 * runs the same order it would have uninterrupted. Returns true when BOTH
 * pools retired -- only then may the caller recompute expiry_retry_deadline_us,
 * which is armed from the state of the complete sweep and would be wrong if
 * derived from a partial one.
 */
static bool session_sweep_run(moq_session_t *s, uint32_t *budget)
{
    if (s->sweep_stage == MOQ_SWEEP_STAGE_REAP_SUBGROUPS) {
        /* Subgroups are only reaped when the action queue has drained, matching
         * the send_len reset above. */
        if (s->action_head == s->action_tail &&
            !sg_reap_terminal_resumable(s, budget))
            return false;
        s->sweep_stage = MOQ_SWEEP_STAGE_PUB;
        s->sweep_slot = moq_occ_first(s->pub_occ_head, s->pub_cap);
        s->sweep_phase = MOQ_SWEEP_PHASE_SELECT;
    }
    if (s->sweep_stage == MOQ_SWEEP_STAGE_PUB) {
        if (!pub_reap_deferred_dones_resumable(s, budget))
            return false;
        s->sweep_stage = MOQ_SWEEP_STAGE_SUB;
        s->sweep_slot = moq_occ_first(s->sub_occ_head, s->sub_cap);
        s->sweep_phase = MOQ_SWEEP_PHASE_SELECT;
    }
    if (s->sweep_stage == MOQ_SWEEP_STAGE_SUB) {
        if (!sub_reap_deferred_dones_resumable(s, budget))
            return false;
        s->sweep_stage = MOQ_SWEEP_STAGE_DONE;
    }
    return true;
}

/*
 * Per-physical-call preparation.
 *
 * Runs exactly once for EVERY advancing call, whether or not that call
 * inherits a suspended cursor, and before any sweep work. A continuation is a
 * real call: it must invalidate borrows and reclaim whatever the queues have
 * since drained, or a record polled before the suspension would stay valid
 * across it and newly empty queues would never release their scratch.
 */
static void session_call_prepare(moq_session_t *s)
{
    s->borrow_epoch++;
    if (s->state_location_retired != s->state_location_cap) {
        s->state_locations[s->state_location_retired_tail].next = s->state_location_free;
        s->state_location_free = s->state_location_retired;
        s->state_location_retired = s->state_location_cap;
        s->state_location_retired_tail = s->state_location_cap;
    }
    if (s->action_head == s->action_tail)
        s->send_len = 0;
#ifndef NDEBUG
    if (s->output_scratch_len > 0)
        memset(s->output_scratch, 0xCD, s->output_scratch_len);
#endif
    s->output_scratch_len = 0;
    if (s->event_head == s->event_tail) {
#ifndef NDEBUG
        if (s->event_scratch_len > 0)
            memset(s->event_scratch, 0xCD, s->event_scratch_len);
#endif
        s->event_scratch_len = 0;
    }
}

/* Start a fresh sweep at `now`, or resume the suspended one at ITS epoch.
 * Returns true when it armed a fresh sweep for this call. */
static bool session_sweep_arm(moq_session_t *s, uint64_t now_us)
{
    if (s->sweep_active) return false;       /* resume: keep sweep_now_us */
    s->sweep_active = true;
    s->sweep_stage = MOQ_SWEEP_STAGE_REAP_SUBGROUPS;
    s->sweep_slot = moq_occ_first(s->sg_occ_head, s->sg_cap);
    s->sweep_rx_pos = 0;
    s->sweep_phase = MOQ_SWEEP_PHASE_SELECT;
    s->sweep_reaped_subgroup = false;
    s->sweep_now_us = now_us;
    /* Logical time advances with the sweep, NOT with the physical call: an
     * inherited sweep must retire against the epoch it was suspended at, or
     * every time-derived recomputation it triggers (expiry_retry_deadline_us,
     * the reported subgroup deadline) would be stamped with a clock that sweep
     * never ran under. */
    s->last_now_us = now_us;
    return true;
}

/* Clear the per-owner cursor. Called when an owner completes or is abandoned,
 * so its rx scan position never leaks into the next owner or pool. */
void session_sweep_owner_reset(moq_session_t *s)
{
    s->sweep_phase = MOQ_SWEEP_PHASE_SELECT;
    s->sweep_rx_pos = 0;
    s->sweep_rx_found = false;
}

static void session_sweep_retire(moq_session_t *s)
{
    s->sweep_active = false;
    s->sweep_stage = MOQ_SWEEP_STAGE_IDLE;
    s->sweep_slot = 0;
    s->sweep_reaped_subgroup = false;
    session_sweep_owner_reset(s);
}

void session_sweep_discard(moq_session_t *s)
{
    if (s) session_sweep_retire(s);
}


#if defined(MOQ_SESSION_SWEEP_TESTING)
uint64_t session_budget_enter_count;
uint64_t session_budget_suspend_count;

/* Exact-work probe counters (session_internal.h documents the unit). Defined
 * here so every preamble stage shares one definition site; incremented at the
 * loop bodies they name. Observational: no control flow reads them. */
uint64_t session_work_sg_reap_probes;
uint64_t session_work_sg_reap_charged;
uint64_t session_work_sg_deadline_probes;
uint64_t session_work_pub_reap_probes;
uint64_t session_work_pub_reap_pending;
uint64_t session_work_sub_reap_probes;
uint64_t session_work_sub_reap_pending;
uint64_t session_work_retry_sub_probes;
uint64_t session_work_retry_pub_probes;
uint64_t session_work_rx_probes;
uint64_t session_work_rx_charged;
uint64_t session_work_close_sg_probes;
uint64_t session_work_close_rx_probes;
uint64_t session_work_fwd_pending_probes;
uint64_t session_work_deferred_rx_probes;
uint64_t session_work_join_probes;
uint64_t session_work_join_resolve_probes;
uint64_t session_work_join_reject_probes;
uint64_t session_work_staged_probes;
uint64_t session_work_queued_action;
uint64_t session_work_queued_event;
uint64_t session_work_tomb_probes;
uint64_t session_work_auth_staging;
uint64_t session_work_index_find;
uint64_t session_work_index_find_calls;
uint64_t session_work_index_rm_calls;
uint64_t session_work_index_rm_search;
uint64_t session_work_index_rm_backshift;
uint64_t session_work_deadline_sg_probes;
uint64_t session_work_deadline_sub_probes;
uint64_t session_work_deadline_pub_probes;
uint64_t session_work_queued_deadline;
uint64_t session_work_expiry_search;
uint64_t session_work_expiry_compact;
uint64_t session_work_reset_offer;
uint64_t session_work_sg_stream_find;
uint64_t session_work_catchup_passes;
uint64_t session_work_rx_rescan_passes;
#endif

uint32_t session_budget_remaining(const moq_session_t *s)
{
    return s ? s->budget_remaining : 0;
}

void session_budget_enter(moq_session_t *s, uint32_t budget)
{
    if (!s) return;
#if defined(MOQ_SESSION_SWEEP_TESTING)
    session_budget_enter_count++;
#endif
    /* The bridge is contractually non-reentrant, so nesting is not supported.
     * Overwriting an active context would silently discard the outer remaining
     * budget and make the resulting misbehaviour hard to localize. */
    assert(!s->budget_active);
    s->budget_active = true;
    s->budget_remaining = budget;
}

void session_budget_leave(moq_session_t *s)
{
    if (!s) return;
    s->budget_active = false;
    s->budget_remaining = 0;
}

/*
 * Arm or disarm the single session-level expiry retry.
 *
 * Armed iff ANY entry in EITHER pool remains expired with blocked work, which
 * is why it may only be evaluated after a COMPLETE two-pool sweep: derived from
 * a partial sweep, one entry finalizing could drop another entry's wake.
 */
static void session_expiry_retry_recompute(moq_session_t *s)
{
    if (s->state == MOQ_SESS_CLOSED) {
        s->expiry_retry_deadline_us = UINT64_MAX;
        return;
    }
    bool blocked = false;
    /*
     * Only an ALLOCATED owner can be deferred-and-expired, and only allocated
     * slots are linked -- so the early-exit search now walks the live owners
     * instead of the pool. That also removes the old scan's dependence on an
     * owner's PHYSICAL SLOT: it used to stop at the first blocked owner, so its
     * cost tracked that owner's index rather than the live set.
     */
    for (int32_t i = s->sub_occ_head; !blocked && i >= 0;
         i = s->subs[i].occ_next) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_retry_sub_probes++;
#endif
        if (s->subs[i].done_pending && s->subs[i].done_expired)
            blocked = true;
    }
    for (int32_t i = s->pub_occ_head; !blocked && i >= 0;
         i = s->publishes[i].occ_next) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_retry_pub_probes++;
#endif
        if (s->publishes[i].done_pending && s->publishes[i].done_expired)
            blocked = true;
    }
    s->expiry_retry_deadline_us = blocked
        ? deadline_add(s->last_now_us, MOQ_DONE_EXPIRY_RETRY_US)
        : UINT64_MAX;
}

/*
 * Run the deferred-completion sweep for one advancing call.
 *
 * Every call prepares itself once (borrows invalidated, drained queues
 * reclaimed), then: an inherited cursor is completed FIRST at ITS own epoch,
 * and this call afterwards runs ONE fresh sweep at its own now_us -- always,
 * not only when the clock advanced. Equal timestamps still owe one, because
 * replay/refeed since the suspension can make an owner the old cursor already
 * walked past runnable again.
 *
 * budget == NULL is unlimited mode, which never returns suspended -- what keeps
 * the private sentinel out of every application-facing result.
 *
 * With a budget it may stop mid-sweep and report suspension; the cursor holds
 * the resume point. expiry_retry_deadline_us is recomputed only when the
 * COMPLETE two-pool sweep retires.
 */
static bool session_advance_sweep(moq_session_t *s, uint64_t now_us,
                                  uint32_t *budget)
{
    session_call_prepare(s);
    /* Fill children retain their owed effects across refusals; every
     * advancing call gives them one chance before its own work. */
    if (session_is_active(s)) {
        fill_service(s);
        mt_retry_pending(s);
        ts_retry_pending(s);
        ann_retry_pending(s);
        ns_sub_retry_pending(s);
    }

    for (;;) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_catchup_passes++;
#endif
        bool fresh = session_sweep_arm(s, now_us);
        if (!session_sweep_run(s, budget))
            return false;                    /* suspended: cursor holds place */

        session_sweep_retire(s);
        session_expiry_retry_recompute(s);   /* complete sweep only */

        /* Catch-up: an INHERITED sweep ran at the epoch it was suspended at, so
         * this call still owes a sweep of its own before its caller mutates --
         * budgeted callers included. Keyed on which sweep just finished, not on
         * the timestamps: a continuation at the SAME time still owes one,
         * because replay/refeed since the suspension can have made an owner the
         * old cursor already walked past runnable again. */
        if (fresh)
            return true;
    }
}

/*
 * Budgeted form of the advancing-call preamble.
 *
 * Requires an active budget context (bridge service brackets one); it spends
 * s->budget_remaining directly, so the caller's ledger and the sweep's spending
 * are the same counter.
 *
 * Returns MOQ_SESSION_SUSPENDED ONLY when runnable work could not be afforded.
 * A pool that is empty, or holds only future/count-unsatisfied owners, has no
 * runnable work and therefore completes even at zero budget. On suspension the
 * cursor stays active and holds the exact resume point, including its own
 * sweep_now_us epoch.
 *
 * Private: declared in the non-installed session_transport.h and returned only
 * across internal boundaries. No application-facing caller can observe it.
 */
moq_result_t session_begin_advance_budgeted(moq_session_t *s, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    assert(s->budget_active);
    if (!session_advance_sweep(s, now_us, &s->budget_remaining)) {
#if defined(MOQ_SESSION_SWEEP_TESTING)
        session_budget_suspend_count++;
#endif
        return MOQ_SESSION_SUSPENDED;
    }
    return MOQ_OK;
}
/*
 * Begin an advancing call.
 *
 * Per-call preparation runs once, then the session-owned stage machine
 * (REAP_SUBGROUPS -> PUB -> SUB -> DONE) completes any inherited cursor at its
 * own epoch and runs one fresh sweep at this call's now_us. Unlimited mode
 * drives both to completion and cannot suspend.
 */
void session_begin_advance(moq_session_t *s, uint64_t now_us)
{
    (void)session_advance_sweep(s, now_us, NULL);
}

/*
 * Advancing-call preamble for the entry points bridge service can reach.
 *
 * With no budget context this is exactly session_begin_advance(): unlimited
 * mode drives the stage machine to completion and cannot suspend, so an
 * application-facing caller never observes MOQ_SESSION_SUSPENDED. Inside a
 * budget context the sweep may stop, and the caller must return the sentinel
 * WITHOUT refreshing idle or performing its own transition -- that transition
 * would otherwise mutate state the sweep has not yet reached.
 */
moq_result_t session_advance_entry(moq_session_t *s, uint64_t now_us)
{
    if (moq_binding_required_unbound(s)) return MOQ_ERR_WRONG_STATE;
    if (s->budget_active)
        return session_begin_advance_budgeted(s, now_us);
    session_begin_advance(s, now_us);
    return MOQ_OK;
}

/* -- Action resource cleanup --------------------------------------- */

void decref_queued_data_payloads(moq_session_t *s)
{
    for (size_t i = s->action_head; i < s->action_tail; i++) {
        moq_action_t *a = &s->actions[i % s->action_cap];
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_queued_action++;
#endif
        if (a->kind == MOQ_ACTION_SEND_DATA && a->u.send_data.payload) {
            moq_rcbuf_decref(a->u.send_data.payload);
            a->u.send_data.payload = NULL;
        }
    }
}

void decref_queued_event_payloads(moq_session_t *s)
{
    for (size_t i = s->event_head; i < s->event_tail; i++) {
        moq_event_t *e = &s->events[i % s->event_cap];
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_queued_event++;
#endif
        if (e->kind == MOQ_EVENT_PUBLISH_STATE && e->u.publish_state.location) {
            event_location_release(s, e->u.publish_state.location);
            e->u.publish_state.location = NULL;
        } else if (e->kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (e->u.object_received.payload) {
                size_t plen = moq_rcbuf_len(e->u.object_received.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.object_received.payload);
                e->u.object_received.payload = NULL;
            }
            if (e->u.object_received.properties) {
                size_t elen = moq_rcbuf_len(e->u.object_received.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.object_received.properties);
                e->u.object_received.properties = NULL;
            }
        } else if (e->kind == MOQ_EVENT_FETCH_OBJECT) {
            if (e->u.fetch_object.payload) {
                size_t plen = moq_rcbuf_len(e->u.fetch_object.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.fetch_object.payload);
                e->u.fetch_object.payload = NULL;
            }
            if (e->u.fetch_object.properties) {
                size_t elen = moq_rcbuf_len(e->u.fetch_object.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.fetch_object.properties);
                e->u.fetch_object.properties = NULL;
            }
        } else if (e->kind == MOQ_EVENT_FILL_OBJECT) {
            if (e->u.fill_object.payload) {
                size_t plen = moq_rcbuf_len(e->u.fill_object.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.fill_object.payload);
                e->u.fill_object.payload = NULL;
            }
            if (e->u.fill_object.properties) {
                size_t elen = moq_rcbuf_len(e->u.fill_object.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.fill_object.properties);
                e->u.fill_object.properties = NULL;
            }
        } else if (e->kind == MOQ_EVENT_OBJECT_CHUNK) {
            if (e->u.object_chunk.chunk) {
                moq_rcbuf_decref(e->u.object_chunk.chunk);
                e->u.object_chunk.chunk = NULL;
            }
            if (e->u.object_chunk.properties) {
                size_t elen = moq_rcbuf_len(e->u.object_chunk.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(e->u.object_chunk.properties);
                e->u.object_chunk.properties = NULL;
            }
        }
    }
}

void free_rx_stream_bufs(moq_session_t *s)
{
    /* Walks the rx OCCUPANCY list; only active streams hold buffers, and the
     * successor is captured before the unlink below. */
    for (int32_t i = s->rx_occ_head, nxt; i >= 0; i = nxt) {
        moq_rx_stream_t *rx = &s->rx_streams[i];
        nxt = rx->occ_next;
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_close_rx_probes++;
#endif
        if (rx->active) {
            moq_index_remove(s->idx_rx_by_ref, s->idx_rx_mask,
                              rx->stream_ref._v);
            if (rx->payload_rcbuf) {
                /* Payload assembled into an rcbuf but not yet emitted;
                 * payload_buf points into it, so decref instead of freeing. */
                moq_rcbuf_decref(rx->payload_rcbuf);
                if ((size_t)rx->payload_expected <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= (size_t)rx->payload_expected;
                else
                    s->recv_payload_bytes = 0;
                rx->payload_rcbuf = NULL;
                rx->payload_buf = NULL;
            }
            if (rx->cur_extensions) {
                size_t ext_len = moq_rcbuf_len(rx->cur_extensions);
                if (ext_len <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= ext_len;
                else
                    s->recv_payload_bytes = 0;
                moq_rcbuf_decref(rx->cur_extensions);
                rx->cur_extensions = NULL;
            }
            if (rx->pending_chunk) {
                moq_rcbuf_decref(rx->pending_chunk);
                rx->pending_chunk = NULL;
            }
            if (rx->input_buf) {
                if (rx->input_cap <= s->recv_input_bytes)
                    s->recv_input_bytes -= rx->input_cap;
                else
                    s->recv_input_bytes = 0;
                s->alloc.free(rx->input_buf, rx->input_cap, s->alloc.ctx);
                rx->input_buf = NULL;
            }
            /* A stream attributed per object (a shared alias) owns its
             * recipient list. */
            if (rx->fan) {
                s->alloc.free(rx->fan, (size_t)rx->fan_cap * sizeof(moq_rx_fan_t),
                              s->alloc.ctx);
                rx->fan = NULL;
                rx->fan_cap = rx->fan_count = 0;
            }
        }
        rx->active = false;
        rx_occ_unlink(s, (size_t)i);
    }
}

/* -- Hash index helpers -------------------------------------------- */

static uint64_t idx_hash(uint64_t key) {
    key ^= key >> 33;
    key *= 0xFF51AFD7ED558CCDULL;
    key ^= key >> 33;
    key *= 0xC4CEB9FE1A85EC53ULL;
    key ^= key >> 33;
    return key;
}

int moq_index_find(const moq_index_entry_t *tbl, size_t mask, uint64_t key)
{
    size_t i = (size_t)(idx_hash(key) & mask);
#ifdef MOQ_SESSION_SWEEP_TESTING
    session_work_index_find_calls++;
#endif
    for (;;) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_index_find++;
#endif
        if (tbl[i].slot < 0) return -1;
        if (tbl[i].key == key) return tbl[i].slot;
        i = (i + 1) & mask;
    }
}

void moq_index_insert(moq_index_entry_t *tbl, size_t mask,
                       uint64_t key, int slot)
{
    size_t i = (size_t)(idx_hash(key) & mask);
    while (tbl[i].slot >= 0) {
        i = (i + 1) & mask;
    }
    tbl[i].key = key;
    tbl[i].slot = (int32_t)slot;
}

void moq_index_remove(moq_index_entry_t *tbl, size_t mask, uint64_t key)
{
    size_t i = (size_t)(idx_hash(key) & mask);
#ifdef MOQ_SESSION_SWEEP_TESTING
    session_work_index_rm_calls++;
#endif
    for (;;) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_index_rm_search++;
#endif
        if (tbl[i].slot < 0) return;
        if (tbl[i].key == key) break;
        i = (i + 1) & mask;
    }
    tbl[i].slot = -1;
    /* Backshift to maintain probe chains. */
    size_t j = i;
    for (;;) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_index_rm_backshift++;
#endif
        j = (j + 1) & mask;
        if (tbl[j].slot < 0) break;
        size_t home = (size_t)(idx_hash(tbl[j].key) & mask);
        /* Check if j is displaced past its home position. */
        bool displaced = (j != home) &&
            ((j > i && (home <= i || home > j)) ||
             (j < i && (home <= i && home > j)));
        if (displaced) {
            tbl[i] = tbl[j];
            tbl[j].slot = -1;
            i = j;
        }
    }
}

/* -- Request registry helpers -------------------------------------- */

moq_request_endpoint_t request_registry_find_by_id(
    const moq_session_t *s, uint64_t request_id)
{
    int32_t packed = moq_index_find(s->idx_req_by_rid, s->idx_req_mask,
                                     request_id);
    if (packed < 0) return req_endpoint_none();
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = req_kind(packed);
    ep.slot = req_slot(packed);
    ep.has_request_id = true;
    ep.request_id = request_id;
    return ep;
}

void request_registry_insert_by_id(
    moq_session_t *s, uint64_t request_id, moq_request_endpoint_t ep)
{
    moq_index_insert(s->idx_req_by_rid, s->idx_req_mask,
                     request_id, req_pack(ep.kind, ep.slot));
}

void request_registry_remove_by_id(
    moq_session_t *s, uint64_t request_id)
{
    moq_request_endpoint_t ep = request_registry_find_by_id(s, request_id);
    if (ep.kind != MOQ_REQ_NONE)
        s->profile->release_request(s, &ep);
    moq_index_remove(s->idx_req_by_rid, s->idx_req_mask, request_id);
}

/* Stream-ref-keyed registry. Pure secondary-index operations over
 * idx_req_by_streamref (see the header note). remove_by_streamref does not
 * release or clear the request pool slot and does not touch the request-id
 * index; the two indexes are independent keys onto the same pool, and a
 * terminal request path must remove every key it populated before the slot is
 * reused. Used by stream-correlated request profiles; draft-16 never calls
 * these. */
moq_request_endpoint_t request_registry_find_by_streamref(
    const moq_session_t *s, moq_stream_ref_t stream_ref)
{
    int32_t packed = moq_index_find(s->idx_req_by_streamref,
                                     s->idx_req_streamref_mask, stream_ref._v);
    if (packed < 0) return req_endpoint_none();
    moq_request_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.kind = req_kind(packed);
    ep.slot = req_slot(packed);
    ep.has_stream_ref = true;
    ep.stream_ref = stream_ref;
    return ep;
}

void request_registry_insert_by_streamref(
    moq_session_t *s, moq_stream_ref_t stream_ref, moq_request_endpoint_t ep)
{
    moq_index_insert(s->idx_req_by_streamref, s->idx_req_streamref_mask,
                     stream_ref._v, req_pack(ep.kind, ep.slot));
}

void request_registry_remove_by_streamref(
    moq_session_t *s, moq_stream_ref_t stream_ref)
{
    moq_index_remove(s->idx_req_by_streamref, s->idx_req_streamref_mask,
                     stream_ref._v);
}

/* -- Profile capability accessors ----------------------------------- *
 * The core consults these instead of testing a draft version. NULL hooks fall
 * back to behavior identical to draft-16. */

bool moq_session_uses_request_streams(const moq_session_t *s)
{
    return s->profile->uses_request_streams;
}

bool moq_session_uses_uni_control(const moq_session_t *s)
{
    /* Explicit topology capability (independent of whether a uni-stream
     * classifier is installed). Draft-16 sets this false. */
    return s->profile->uses_uni_control_channel;
}

uint64_t moq_session_resource_limit_stream_error(const moq_session_t *s)
{
    return s->profile->resource_limit_stream_error;
}

/* Public: derived from the profile's own Track Namespace floor rather than a
 * second capability bit, so the two can never disagree. Unlike its neighbours
 * above -- core-internal, always called on a fully built session -- this one is
 * reachable from application code, so it tolerates a NULL session and a session
 * with no profile bound instead of dereferencing. */
bool moq_session_supports_zero_field_track_namespace(const moq_session_t *s)
{
    if (!s || !s->profile) return false;
    return s->profile->min_track_namespace_fields == 0;
}

uint64_t moq_session_event_progress_token(const moq_session_t *s)
{
    return s->event_progress_token;
}

bool moq_session_terminal_enqueued(const moq_session_t *s)
{
    return s != NULL && s->terminal_enqueued;
}

bool moq_session_terminal_observed(const moq_session_t *s)
{
    return s != NULL && s->terminal_observed;
}

bool moq_session_has_events(const moq_session_t *s)
{
    return s->event_head != s->event_tail;
}

moq_result_t moq_session_register_transport_owner(moq_session_t *s,
                                                  const void *owner)
{
    if (!s || !owner) return MOQ_ERR_INVAL;
    if (s->transport_owner && s->transport_owner != owner)
        return MOQ_ERR_WRONG_STATE;
    s->transport_owner = owner;
    return MOQ_OK;
}

void moq_session_unregister_transport_owner(moq_session_t *s,
                                            const void *owner)
{
    if (s && owner && s->transport_owner == owner) {
        s->transport_owner = NULL;
        s->has_owner_deadline = false;
        s->owner_deadline_us = 0;
    }
}

void moq_session_set_owner_deadline(moq_session_t *s, const void *owner,
                                    bool has, uint64_t deadline_us)
{
    if (!s || !owner || s->transport_owner != owner) return;
    s->has_owner_deadline = has;
    s->owner_deadline_us = has ? deadline_us : 0;
}

moq_uni_class_t moq_session_classify_peer_uni(const moq_session_t *s,
                                              const uint8_t *data, size_t len)
{
    /* Profiles without a unidirectional control channel (draft-16) leave the
     * hook NULL; every peer unidirectional stream is data. */
    if (!s->profile->classify_uni_stream)
        return MOQ_UNI_CLASS_DATA;
    return s->profile->classify_uni_stream(data, len);
}

bool bridge_session_has_data_stream_type(
    const moq_session_t *s, const uint8_t *data, size_t len)
{
    if (!s || !data || !len) return false;
    uint32_t kind = s->profile->classify_data_stream(data, len);
    return kind == MOQ_STREAM_KIND_SUBGROUP || kind == MOQ_STREAM_KIND_FETCH;
}

moq_result_t moq_session_validate_inbound_request_stream(
    moq_session_t *s, moq_stream_ref_t ref, uint64_t msg_type,
    uint64_t wire_request_id, moq_request_endpoint_t *out)
{
    if (moq_binding_required_unbound(s)) return MOQ_ERR_WRONG_STATE;
    /* Profiles that do not open a bidi stream per request (draft-16) leave the
     * hook NULL; this path is never taken for them. */
    if (!s->profile->validate_inbound_request_stream) {
        if (out) *out = req_endpoint_none();
        return MOQ_ERR_INVAL;
    }
    return s->profile->validate_inbound_request_stream(s, ref, msg_type,
                                                       wire_request_id, out);
}

/* -- Queue helpers ------------------------------------------------- */

bool action_queue_full(const moq_session_t *s)
{
    return (s->action_tail - s->action_head) >= s->action_cap;
}

size_t moq_session_action_capacity(const moq_session_t *s)
{
    if (!s) return 0;
    size_t used = s->action_tail - s->action_head;
    return used >= s->action_cap ? 0 : s->action_cap - used;
}

bool moq_session_has_pending_output(const moq_session_t *s)
{
    return s && session_has_outbound_obligation(s);
}

/* With keys, one call queues up to four actions at once: a FETCH_OK beside
 * its stream's key, header and reset, or an update's REQUEST_OK and key
 * beside a refused fill's header and reset. */
#define SESSION_DATA_PRIORITY_MIN_ACTIONS 4u

moq_result_t moq_session_set_data_priority_updates(moq_session_t *s,
                                                   bool enabled)
{
    if (!s) return MOQ_ERR_INVAL;
    if (enabled && s->action_cap < SESSION_DATA_PRIORITY_MIN_ACTIONS)
        return MOQ_ERR_INVAL;
    s->data_priority_updates = enabled;
    return MOQ_OK;
}

/* The scheduling record of a SEND_DATA action, from the stream it rides:
 * a subgroup stream, a FETCH response stream, or a fill fetch stream. Fills
 * in the public scheduling snapshot too when the producer left it absent. */
static void derive_stream_meta(moq_session_t *s, moq_action_t *queued,
                               moq_action_meta_t *m)
{
    moq_stream_ref_t ref = queued->u.send_data.stream_ref;
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
        const moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->stream_ref._v != ref._v) continue;
        sg_stream_meta(s, sg, m);
        if (!queued->u.send_data.scheduling_priority)
            queued->u.send_data.scheduling_priority = UINT64_C(0x10000) |
                ((uint64_t)m->subscriber_priority << 8) | m->publisher_priority;
        return;
    }
    for (int32_t i = s->fetch_occ_head; i >= 0; i = s->fetches[i].occ_next) {
        const moq_fetch_entry_t *f = &s->fetches[i];
        if (f->data_stream_ref._v != ref._v) continue;
        m->mode = MOQ_SCHED_MODE_FETCH;
        m->owner = f->handle._opaque;
        m->subscriber_priority = f->subscriber_priority;
        m->publisher_priority = f->prior.has_actual ? f->prior.publisher_priority : 128;
        if (!queued->u.send_data.scheduling_priority)
            queued->u.send_data.scheduling_priority = UINT64_C(0x10000) |
                ((uint64_t)m->subscriber_priority << 8) | m->publisher_priority;
        return;
    }
    for (size_t i = 0; s->fill_storage && i < s->fill_cap; i++) {
        const moq_fill_entry_t *f = &s->fill_storage[i];
        if (!f->active || !f->stream_bound || f->stream._v != ref._v) continue;
        fill_stream_meta(s, f, m);
        return;
    }
}

moq_result_t push_action_ex(moq_session_t *s, const moq_action_t *a,
                            const moq_action_meta_t *meta, uint64_t deadline_us)
{
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    size_t idx = s->action_tail % s->action_cap;
    s->actions[idx] = *a;
    moq_action_t *queued = &s->actions[idx];
    moq_action_meta_t *m = &s->action_meta[idx];
    memset(m, 0, sizeof(*m));
    m->deadline_us = UINT64_MAX;
    if (meta) {
        *m = *meta;
    } else if (a->kind == MOQ_ACTION_SEND_DATA) {
        derive_stream_meta(s, queued, m);
    } else if (a->kind == MOQ_ACTION_RESET_DATA) {
        moq_action_t data = { .kind = MOQ_ACTION_SEND_DATA };
        data.u.send_data.stream_ref = a->u.reset_data.stream_ref;
        derive_stream_meta(s, &data, m);
    } else if (a->kind == MOQ_ACTION_SET_DATA_PRIORITY) {
        m->subscriber_priority = a->u.set_data_priority.priority.subscriber_priority;
        m->publisher_priority = a->u.set_data_priority.priority.publisher_priority;
    }
    if (a->kind == MOQ_ACTION_SEND_DATA && queued->u.send_data.scheduling_priority) {
        /* The producer's snapshot is the authority for the priorities. */
        m->subscriber_priority = (uint8_t)(queued->u.send_data.scheduling_priority >> 8);
        m->publisher_priority = (uint8_t)queued->u.send_data.scheduling_priority;
    }
    m->deadline_us = deadline_us;
    session_action_enqueued(s, queued, m);
    s->action_tail++;
    session_refresh_idle(s, s->last_now_us);
    return MOQ_OK;
}

moq_result_t push_action(moq_session_t *s, const moq_action_t *a)
{
    return push_action_ex(s, a, NULL, UINT64_MAX);
}

void session_tag_owner_done(moq_session_t *s, uint64_t owner)
{
    assert(s->action_tail > s->action_head && owner != 0);
    s->action_meta[(s->action_tail - 1) % s->action_cap].terminal_owner = owner;
}

uint64_t session_queued_data_deadline(const moq_session_t *s)
{
    uint64_t d = UINT64_MAX;
    /* Untimed actions all carry UINT64_MAX: only a timed one can be earlier. */
    if (s->action_timed == 0) return d;
    for (size_t i = s->action_head; i < s->action_tail; i++) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_queued_deadline++;
#endif
        uint64_t x = s->action_meta[i % s->action_cap].deadline_us;
        if (x < d) d = x;
    }
    return d;
}

/* -- Scheduling (design: Priorities) ------------------------------- *
 * Among the queued data actions up to the next non-data action, the poll
 * takes the one the scheduling algorithm places first: the higher subscriber
 * priority, then the higher publisher priority, then -- within one request --
 * the group order between groups (a fill whose order differs from its
 * subscription's yields to the subscription-delivered object) and, within a
 * group, the fill-delivered object, then a datagram before a subgroup, then
 * the lowest Subgroup ID or Object ID. Actions on one stream keep their
 * order, ties keep queue order, and a non-data action holds its place. A
 * stream's key is data of its stream, scheduled with the header it
 * precedes; an owner-wide key holds its place. */

/* The data stream `a` acts on, if it is a stream action: SEND_DATA or a
 * stream's key. */
static bool action_data_stream(const moq_action_t *a, moq_stream_ref_t *ref)
{
    if (a->kind == MOQ_ACTION_SEND_DATA) {
        *ref = a->u.send_data.stream_ref;
        return true;
    }
    if (a->kind == MOQ_ACTION_SET_DATA_PRIORITY &&
        a->u.set_data_priority.stream_ref._v != 0) {
        *ref = a->u.set_data_priority.stream_ref;
        return true;
    }
    return false;
}

static bool action_is_data(const moq_action_t *a)
{
    moq_stream_ref_t ref;
    return a->kind == MOQ_ACTION_SEND_DATAGRAM || action_data_stream(a, &ref);
}

void session_action_enqueued(moq_session_t *s, const moq_action_t *a,
                             const moq_action_meta_t *m)
{
    if (m->deadline_us != UINT64_MAX) s->action_timed++;
    if (!action_is_data(a)) s->action_nondata++;
}

void session_action_dequeued(moq_session_t *s, const moq_action_t *a,
                             const moq_action_meta_t *m)
{
    if (m->deadline_us != UINT64_MAX) {
        assert(s->action_timed > 0);
        s->action_timed--;
    }
    if (!action_is_data(a)) {
        assert(s->action_nondata > 0);
        s->action_nondata--;
    }
}

static int sched_compare(const moq_action_meta_t *x, const moq_action_meta_t *y)
{
    if (x->subscriber_priority != y->subscriber_priority)
        return x->subscriber_priority < y->subscriber_priority ? -1 : 1;
    if (x->publisher_priority != y->publisher_priority)
        return x->publisher_priority < y->publisher_priority ? -1 : 1;
    if (x->owner == 0 || x->owner != y->owner || !x->has_location || !y->has_location)
        return 0;
    bool xf = x->mode == MOQ_SCHED_MODE_FILL, yf = y->mode == MOQ_SCHED_MODE_FILL;
    if (xf && yf) return 0;
    if (x->group_id != y->group_id) {
        if (xf != yf) {
            const moq_action_meta_t *fill = xf ? x : y;
            if (fill->fill_group_order != fill->group_order)
                return xf ? 1 : -1;
        }
        bool desc = x->group_order == MOQ_GROUP_ORDER_DESCENDING;
        bool x_first = desc ? x->group_id > y->group_id : x->group_id < y->group_id;
        return x_first ? -1 : 1;
    }
    if (xf != yf) return xf ? -1 : 1;
    if (x->mode != y->mode) {
        if (x->mode == MOQ_SCHED_MODE_DATAGRAM) return -1;
        if (y->mode == MOQ_SCHED_MODE_DATAGRAM) return 1;
        return 0;
    }
    if (x->mode == MOQ_SCHED_MODE_SUBGROUP && x->subgroup_id != y->subgroup_id)
        return x->subgroup_id < y->subgroup_id ? -1 : 1;
    if (x->mode == MOQ_SCHED_MODE_DATAGRAM && x->object_id != y->object_id)
        return x->object_id < y->object_id ? -1 : 1;
    return 0;
}

/* The queue index (head <= idx < tail) of the action the next poll takes. */
static size_t action_select_from(const moq_session_t *s, size_t head)
{
    size_t tail = s->action_tail, cap = s->action_cap;
    if (!action_is_data(&s->actions[head % cap])) return head;
    size_t best = head;
    for (size_t i = head + 1; i < tail; i++) {
        const moq_action_t *a = &s->actions[i % cap];
        if (!action_is_data(a)) break;
        moq_stream_ref_t ref, other;
        if (action_data_stream(a, &ref)) {
            bool later_on_its_stream = false;
            for (size_t j = head; j < i; j++) {
                if (action_data_stream(&s->actions[j % cap], &other) &&
                    other._v == ref._v) {
                    later_on_its_stream = true;
                    break;
                }
            }
            if (later_on_its_stream) continue;
        }
        if (sched_compare(&s->action_meta[i % cap], &s->action_meta[best % cap]) < 0)
            best = i;
    }
    return best;
}

static size_t action_select(const moq_session_t *s)
{
    return action_select_from(s, s->action_head);
}

static bool action_waits_for_terminal(const moq_session_t *s, size_t idx)
{
    uint64_t owner = s->action_meta[idx % s->action_cap].terminal_owner;
    if (owner == 0) return false;
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
        const moq_sg_entry_t *sg = &s->subgroups[i];
        if ((sg->state == MOQ_SG_TERMINAL_OWED ||
             (sg->state == MOQ_SG_RESET_OWED &&
              sg->terminal_cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE)) &&
            (sg->sub._opaque == owner || sg->pub._opaque == owner))
            return true;
    }
    return false;
}

/* Remove the action at idx from the ring into *out, keeping the order of the
 * actions it overtook. */
static void action_take(moq_session_t *s, size_t idx, moq_action_t *out)
{
    size_t cap = s->action_cap;
    session_action_dequeued(s, &s->actions[idx % cap], &s->action_meta[idx % cap]);
    *out = s->actions[idx % cap];
    for (size_t j = idx; j > s->action_head; j--) {
        s->actions[j % cap] = s->actions[(j - 1) % cap];
        s->action_meta[j % cap] = s->action_meta[(j - 1) % cap];
    }
    s->action_head++;
}

bool event_queue_full(const moq_session_t *s)
{
    return (s->event_tail - s->event_head) + s->event_reserve >= s->event_cap;
}

/* The Prior Group/Object ID Gaps an object event surfaces are read from the
 * properties it carries, so every producer of an object event reports them
 * the same way. A block that does not yield them (none, a malformed block,
 * or a profile without them) reports neither. */
static void event_set_gaps(const moq_session_t *s, moq_event_t *e)
{
    const moq_rcbuf_t *props = NULL;
    bool *hg = NULL, *ho = NULL;
    uint64_t *g = NULL, *o = NULL;
    switch (e->kind) {
    case MOQ_EVENT_OBJECT_RECEIVED: {
        moq_object_received_event_t *x = &e->u.object_received;
        props = x->properties; hg = &x->has_prior_group_gap;
        ho = &x->has_prior_object_gap; g = &x->prior_group_gap;
        o = &x->prior_object_gap;
        break;
    }
    case MOQ_EVENT_OBJECT_CHUNK: {
        moq_object_chunk_event_t *x = &e->u.object_chunk;
        if (!x->begin) return;
        props = x->properties; hg = &x->has_prior_group_gap;
        ho = &x->has_prior_object_gap; g = &x->prior_group_gap;
        o = &x->prior_object_gap;
        break;
    }
    case MOQ_EVENT_FETCH_OBJECT: {
        moq_fetch_object_event_t *x = &e->u.fetch_object;
        props = x->properties; hg = &x->has_prior_group_gap;
        ho = &x->has_prior_object_gap; g = &x->prior_group_gap;
        o = &x->prior_object_gap;
        break;
    }
    case MOQ_EVENT_FILL_OBJECT: {
        moq_fill_object_event_t *x = &e->u.fill_object;
        props = x->properties; hg = &x->has_prior_group_gap;
        ho = &x->has_prior_object_gap; g = &x->prior_group_gap;
        o = &x->prior_object_gap;
        break;
    }
    default:
        return;
    }
    *hg = *ho = false;
    *g = *o = 0;
    if (!props || !s->profile->scan_object_gaps) return;
    moq_obj_gaps_t gaps;
    if (s->profile->scan_object_gaps(moq_rcbuf_data(props), moq_rcbuf_len(props),
                                     &gaps) < 0)
        return;
    *hg = gaps.has_group_gap;
    *g = gaps.group_gap;
    *ho = gaps.has_object_gap;
    *o = gaps.object_gap;
}

moq_result_t push_event(moq_session_t *s, const moq_event_t *e)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_event_t *q = &s->events[s->event_tail % s->event_cap];
    *q = *e;
    event_set_gaps(s, q);
    s->event_tail++;
    /* Monotonic event-progress token: advances on every enqueue (here) AND every
     * dequeue (moq_session_poll_events_ex). A caller that services the session on
     * a pass separate from the one that drains events (the managed adapters'
     * coalesced doorbell) compares it across a whole pump+service cycle to tell
     * "progress was made" (the queue was refilled OR drained) from "nothing
     * moved". Combined with has_events it drives a bounded re-pump that closes
     * both the small-queue (service refilled) and large-queue (bounded pump left
     * a residual) cases, without spinning a non-draining app. Never reset
     * (queue-index resets do not touch it); unsigned wrap is fine — callers only
     * compare for equality. Surfaced through the private adapter SPI, never the
     * public API. */
    s->event_progress_token++;
    return MOQ_OK;
}

/* -- Output scratch ------------------------------------------------ */

uint8_t *scratch_alloc(moq_session_t *s, size_t len)
{
    if (len == 0) return s->output_scratch + s->output_scratch_len;
    if (len > s->output_scratch_cap - s->output_scratch_len) return NULL;
    uint8_t *p = s->output_scratch + s->output_scratch_len;
    s->output_scratch_len += len;
    return p;
}

void *scratch_alloc_aligned(moq_session_t *s, size_t len, size_t align)
{
    if (align == 0) align = 1;
    if ((align & (align - 1)) != 0) return NULL;
    size_t cur = s->output_scratch_len;
    size_t aligned = (cur + align - 1) & ~(align - 1);
    if (aligned < cur) return NULL;
    if (aligned > s->output_scratch_cap) return NULL;
    size_t remaining = s->output_scratch_cap - aligned;
    if (len > remaining) return NULL;
    s->output_scratch_len = aligned + len;
    return s->output_scratch + aligned;
}

uint8_t *scratch_copy(moq_session_t *s, const uint8_t *data, size_t len)
{
    if (len == 0) return NULL;
    uint8_t *p = scratch_alloc(s, len);
    if (!p) return NULL;
    memcpy(p, data, len);
    return p;
}

moq_location_filter_cfg_t *event_location_alloc(moq_session_t *s)
{
    if (s->state_location_free == s->state_location_cap) return NULL;
    size_t index = s->state_location_free;
    s->state_location_free = s->state_locations[index].next;
    return &s->state_locations[index].value;
}

void event_location_release(moq_session_t *s, const moq_location_filter_cfg_t *value)
{
    /* value is the first member of a cell reserved from this session's pool. */
    const moq_state_location_cell_t *cell = (const moq_state_location_cell_t *)value;
    size_t index = (size_t)(cell - s->state_locations);
    s->state_locations[index].next = s->state_location_free;
    s->state_location_free = index;
}

static void event_location_retire(moq_session_t *s, const moq_location_filter_cfg_t *value)
{
    const moq_state_location_cell_t *cell = (const moq_state_location_cell_t *)value;
    size_t index = (size_t)(cell - s->state_locations);
    if (s->state_location_retired == s->state_location_cap)
        s->state_location_retired_tail = index;
    s->state_locations[index].next = s->state_location_retired;
    s->state_location_retired = index;
}

/* -- Event borrow arena --------------------------------------------
 * Same allocators against the event arena. Anything copied here is
 * referenced by a queued moq_event_t and must survive later advancing
 * calls in the same batch; the arena recycles in session_begin_advance
 * once the event queue is empty. Exhaustion returns NULL, failing the
 * producing path exactly like transient scratch exhaustion would. */

uint8_t *event_scratch_alloc(moq_session_t *s, size_t len)
{
    if (len == 0) return s->event_scratch + s->event_scratch_len;
    if (len > s->event_scratch_cap - s->event_scratch_len) return NULL;
    uint8_t *p = s->event_scratch + s->event_scratch_len;
    s->event_scratch_len += len;
    return p;
}

void *event_scratch_alloc_aligned(moq_session_t *s, size_t len, size_t align)
{
    if (align == 0) align = 1;
    if ((align & (align - 1)) != 0) return NULL;
    size_t cur = s->event_scratch_len;
    size_t aligned = (cur + align - 1) & ~(align - 1);
    if (aligned < cur) return NULL;
    if (aligned > s->event_scratch_cap) return NULL;
    size_t remaining = s->event_scratch_cap - aligned;
    if (len > remaining) return NULL;
    s->event_scratch_len = aligned + len;
    return s->event_scratch + aligned;
}

uint8_t *event_scratch_copy(moq_session_t *s, const uint8_t *data, size_t len)
{
    if (len == 0) return NULL;
    uint8_t *p = event_scratch_alloc(s, len);
    if (!p) return NULL;
    memcpy(p, data, len);
    return p;
}

/* Predicate: would an `align`-aligned array of `count * elem_size` bytes,
 * followed by `tail_bytes` of unaligned data, BOTH fit in the event scratch
 * arena starting at the current event_scratch_len? Uses the SAME alignment math
 * as event_scratch_alloc_aligned(), so a preflight with this helper exactly
 * matches what the subsequent aligned array allocation + manual tail memcpy will
 * consume -- including the alignment padding the bare `array+tail <= cap-len`
 * checks missed (a padding-sized tail overflow). Pure: never mutates scratch.
 * Rejects the count*elem_size multiplication overflow and computes the capacity
 * test by subtraction so no addition can wrap. */
bool event_scratch_fits_aligned(const moq_session_t *s, size_t count,
                                size_t elem_size, size_t tail_bytes,
                                size_t align)
{
    if (align == 0) align = 1;
    if ((align & (align - 1)) != 0) return false;
    if (elem_size != 0 && count > SIZE_MAX / elem_size) return false;
    size_t array_bytes = count * elem_size;
    size_t cur = s->event_scratch_len;
    size_t aligned = (cur + align - 1) & ~(align - 1);
    if (aligned < cur) return false;                 /* alignment overflow */
    if (aligned > s->event_scratch_cap) return false;
    size_t remaining = s->event_scratch_cap - aligned;
    if (array_bytes > remaining) return false;
    remaining -= array_bytes;
    if (tail_bytes > remaining) return false;
    return true;
}

/* Copy a namespace (aligned parts array + per-part byte tails) into the event
 * scratch arena, transactionally: on any failure the arena cursor is restored
 * exactly and *dst is left unwritten -- a retry after capacity returns starts
 * from the same cursor, with no cumulative leak. A zero-part namespace
 * succeeds without touching the arena at all: the wire permits count == 0, an
 * empty array needs no storage, and consuming alignment padding for it would
 * charge capacity for nothing while shifting later regions' alignment.
 * Zero-length individual parts stay valid ({NULL, 0}, matching
 * event_scratch_copy's len == 0 result). The count multiplication is
 * overflow-checked; the aligned allocation's own arithmetic is already
 * overflow-safe.
 *
 * Callers that stage a namespace as one region of a larger event weigh the
 * whole composite first and use this only to copy that region -- the REDIRECT
 * event does exactly that, so its classifier, not this helper, decides whether
 * the copy can proceed at all. */
bool event_scratch_copy_namespace(moq_session_t *s,
                                  const moq_namespace_t *src,
                                  moq_namespace_t *dst)
{
    if (src->count == 0) {
        dst->parts = NULL;
        dst->count = 0;
        return true;
    }
    if (src->count > SIZE_MAX / sizeof(moq_bytes_t)) return false;

    size_t saved = s->event_scratch_len;
    moq_bytes_t *parts = (moq_bytes_t *)event_scratch_alloc_aligned(
        s, src->count * sizeof(moq_bytes_t), _Alignof(moq_bytes_t));
    if (!parts) return false;   /* alloc_aligned mutates nothing on failure */

    for (size_t i = 0; i < src->count; i++) {
        uint8_t *data = event_scratch_copy(s, src->parts[i].data,
                                           src->parts[i].len);
        if (!data && src->parts[i].len > 0) {
            s->event_scratch_len = saved;
            return false;
        }
        parts[i].data = data;
        parts[i].len  = src->parts[i].len;
    }

    dst->parts = parts;
    dst->count = src->count;
    return true;
}

/* Classify a SETUP completion event's total scratch requirement -- the resolved
 * token-value byte tails in order, then the alignment padding those tails leave
 * before an aligned moq_resolved_token_t array of token_count entries -- against
 * both an empty arena and the live cursor.
 *
 * The two answers are different questions. A requirement that cannot fit an
 * EMPTY arena can never be satisfied by draining events, so it is permanent and
 * the caller closes the session. A requirement that fits an empty arena but not
 * the current tail is transient: the arena recycles when the event queue drains,
 * so the caller retains the message and returns WOULD_BLOCK.
 *
 * The alignment padding is measured from the cursor AFTER the byte tails,
 * because that is where the array actually lands; measuring it from the arena
 * base, or ordering the array before the tails, computes a different (and
 * sometimes smaller) requirement than the copy will consume. All arithmetic is
 * subtraction-based so no sum can wrap; an overflowing requirement is
 * permanent, never a wait. A zero token_count needs no bytes and no padding. */
static bool setup_event_scratch_requirement_fits(const moq_session_t *s,
                                                 const moq_resolved_token_t *toks,
                                                 size_t token_count,
                                                 size_t from_cursor)
{
    if (token_count == 0) return true;
    if (token_count > SIZE_MAX / sizeof(moq_resolved_token_t)) return false;

    if (from_cursor > s->event_scratch_cap) return false;
    size_t remaining = s->event_scratch_cap - from_cursor;
    size_t cursor = from_cursor;

    for (size_t i = 0; i < token_count; i++) {
        size_t len = toks[i].token_value.len;
        if (len > remaining) return false;
        remaining -= len;
        cursor += len;
    }

    const size_t align = _Alignof(moq_resolved_token_t);
    size_t pad = (align - (cursor & (align - 1))) & (align - 1);
    if (pad > remaining) return false;
    remaining -= pad;

    size_t array_bytes = token_count * sizeof(moq_resolved_token_t);
    return array_bytes <= remaining;
}

/* Decide, before any copying, whether a SETUP completion event's tokens can be
 * staged now, later, or never. PERMANENT means close; BLOCKED means retain the
 * message and retry once events drain; FITS means proceed. */
moq_event_scratch_verdict_t setup_event_scratch_classify(
    const moq_session_t *s, const moq_resolved_token_t *toks,
    size_t token_count)
{
    if (!setup_event_scratch_requirement_fits(s, toks, token_count, 0))
        return MOQ_EVENT_SCRATCH_PERMANENT;
    if (!setup_event_scratch_requirement_fits(s, toks, token_count,
                                              s->event_scratch_len))
        return MOQ_EVENT_SCRATCH_BLOCKED;
    return MOQ_EVENT_SCRATCH_FITS;
}

/* Classify a single contiguous byte tail against both an empty arena and the
 * live cursor. The two answers are different questions: a tail larger than the
 * whole arena can never be satisfied by draining events, while one that only
 * the current tail cannot hold becomes satisfiable when the queue empties and
 * the arena recycles. Subtraction-only, and a cursor past the cap (which the
 * arena never produces) is treated as no room rather than wrapping. */
moq_event_scratch_verdict_t event_scratch_classify_bytes(
    const moq_session_t *s, size_t len)
{
    if (len == 0) return MOQ_EVENT_SCRATCH_FITS;
    if (len > s->event_scratch_cap) return MOQ_EVENT_SCRATCH_PERMANENT;
    if (s->event_scratch_len > s->event_scratch_cap ||
        len > s->event_scratch_cap - s->event_scratch_len)
        return MOQ_EVENT_SCRATCH_BLOCKED;
    return MOQ_EVENT_SCRATCH_FITS;
}

/* -- Send helper --------------------------------------------------- */

moq_result_t send_probe_begin(moq_session_t *s, moq_buf_writer_t *probe)
{
    /* Same bounded envelope allowance as SETUP preflight: a 16-bit payload
     * plus room for the selected profile's type and length framing. */
    size_t cap = (size_t)UINT16_MAX + 16u;
    if (cap > s->send_cap) cap = s->send_cap;
    uint8_t *bytes = (uint8_t *)s->alloc.alloc(cap, s->alloc.ctx);
    if (!bytes) return MOQ_ERR_NOMEM;
    moq_buf_writer_init(probe, bytes, cap);
    return MOQ_OK;
}

moq_result_t send_probe_end(moq_session_t *s, moq_buf_writer_t *probe,
                            moq_result_t encoded)
{
    session_wipe(probe->data, probe->cap);
    s->alloc.free(probe->data, probe->cap, s->alloc.ctx);
    /* Only a successful exact encoding proves drain-retry can help. This is
     * a local capacity classification, not a protocol-defined error code. */
    return encoded == MOQ_OK ? MOQ_ERR_WOULD_BLOCK : encoded;
}

static moq_result_t commit_send_control_tail(moq_session_t *s, size_t len)
{
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_CONTROL;
    a.detail_size = (uint32_t)sizeof(moq_send_control_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_control.data = s->send_buf + s->send_len;
    a.u.send_control.len  = len;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->send_len += len;
    return MOQ_OK;
}

moq_result_t queue_send_control(moq_session_t *s,
                                 const uint8_t *data, size_t len)
{
    if (len == 0) return MOQ_OK;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (len > s->send_cap - s->send_len) return MOQ_ERR_BUFFER;
    memcpy(s->send_buf + s->send_len, data, len);
    return commit_send_control_tail(s, len);
}

moq_result_t queue_send_cancel(moq_session_t *s, const uint8_t *data, size_t len)
{
    if (len > s->send_cap) return MOQ_ERR_BUFFER;
    if (len > s->send_cap - s->send_len) return MOQ_ERR_WOULD_BLOCK;
    return queue_send_control(s, data, len);
}

moq_result_t queue_send_bidi(moq_session_t *s, moq_stream_ref_t ref,
                              const uint8_t *data, size_t len, bool fin)
{
    /* A per-request GOAWAY sent on this bidi does not end the request (§10.4:
     * it "does not impact subscription state"), so what the session still owes
     * there -- the answer to an update already in flight, a terminal -- is
     * written after it. Which application operations remain open on a
     * migrating request is decided by each entry point. */
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    /* Distinguish a message that can never fit (permanent failure) from a
     * temporary shortfall in the remaining buffer (retryable): the latter must
     * not be a hard error, or the dispatch frees the request slot and the
     * required response is lost. */
    if (len > s->send_cap) return MOQ_ERR_BUFFER;
    if (len > s->send_cap - s->send_len) return MOQ_ERR_WOULD_BLOCK;
    memcpy(s->send_buf + s->send_len, data, len);

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_SEND_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_send_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.send_bidi_stream.stream_ref = ref;
    a.u.send_bidi_stream.data = s->send_buf + s->send_len;
    a.u.send_bidi_stream.len  = len;
    a.u.send_bidi_stream.fin  = fin;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->send_len += len;
    return MOQ_OK;
}

moq_result_t reject_apply_redirect(moq_session_t *s,
                                   moq_request_error_encode_args_t *args,
                                   const moq_redirect_target_t *rd,
                                   bool namespace_scoped)
{
    bool is_redirect = args->error_code == MOQ_REQUEST_ERROR_REDIRECT;
    bool tail_set = rd && (rd->connect_uri.len > 0 ||
                           rd->track_namespace.count > 0 ||
                           rd->track_name.len > 0);
    if (!is_redirect) {
        /* A redirect target only travels with the REDIRECT error code. */
        return tail_set ? MOQ_ERR_INVAL : MOQ_OK;
    }
    /* REDIRECT is a draft-18 REQUEST_ERROR tail on the request bidi. */
    if (!moq_session_uses_request_streams(s)) return MOQ_ERR_INVAL;
    if (s->perspective == MOQ_PERSPECTIVE_CLIENT && rd && rd->connect_uri.len > 0)
        return MOQ_ERR_INVAL;                 /* client must send a zero-length URI */
    if (namespace_scoped && rd && rd->track_name.len > 0)
        return MOQ_ERR_INVAL;                 /* namespace-scoped: empty track name */
    /* Validate the (public, caller-supplied) redirect target before it is read by
     * the encoder's size pass: bounded namespace count, non-NULL spans, and
     * non-empty namespace fields (draft-18 §1.4.2: 0..32 fields, each non-empty). */
    if (rd) {
        if (rd->connect_uri.len > 0 && !rd->connect_uri.data) return MOQ_ERR_INVAL;
        if (rd->track_name.len > 0 && !rd->track_name.data) return MOQ_ERR_INVAL;
        if (rd->track_namespace.count > 32) return MOQ_ERR_INVAL;
        if (rd->track_namespace.count > 0 && !rd->track_namespace.parts)
            return MOQ_ERR_INVAL;
        for (size_t i = 0; i < rd->track_namespace.count; i++)
            if (rd->track_namespace.parts[i].len == 0 ||
                !rd->track_namespace.parts[i].data)
                return MOQ_ERR_INVAL;
    }
    /* error_code == REDIRECT always emits the tail (an all-empty Redirect is valid). */
    args->has_redirect = true;
    if (rd) {
        args->connect_uri = rd->connect_uri.data;
        args->connect_uri_len = rd->connect_uri.len;
        args->redirect_namespace = rd->track_namespace;
        args->redirect_track_name = rd->track_name.data;
        args->redirect_track_name_len = rd->track_name.len;
    }
    return MOQ_OK;
}

moq_result_t queue_request_error_bidi(moq_session_t *s, moq_stream_ref_t ref,
                                      const moq_request_error_encode_args_t *args)
{
    size_t scratch_saved = s->output_scratch_len;
    /* Upper bound: envelope header + error_code + retry + reason span + redirect
     * (uri span + namespace tuple + track-name span), with vi64 slack per field. */
    size_t bound = 48 + args->reason_len;
    if (args->has_redirect) {
        bound += args->connect_uri_len + args->redirect_track_name_len + 16;
        for (size_t i = 0; i < args->redirect_namespace.count; i++)
            bound += args->redirect_namespace.parts[i].len + 9;
    }
    uint8_t *buf = (uint8_t *)scratch_alloc_aligned(s, bound, 1);
    if (!buf) return MOQ_ERR_BUFFER;   /* message larger than the scratch arena */
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, bound);
    moq_result_t rc = s->profile->encode_request_error(s, &w, args);
    if (rc < 0) { s->output_scratch_len = scratch_saved; return rc; }
    /* queue_send_bidi copies into send_buf and gives BUFFER (never fits send_cap)
     * vs WOULD_BLOCK (transient remaining shortfall); restore scratch either way. */
    rc = queue_send_bidi(s, ref, buf, moq_buf_writer_offset(&w), true);
    s->output_scratch_len = scratch_saved;
    return rc;
}

void moq_request_goaway_cfg_init(moq_request_goaway_cfg_t *cfg)
{
    if (!cfg) return;
    memset(cfg, 0, sizeof(*cfg));
    cfg->struct_size = sizeof(moq_request_goaway_cfg_t);
}

bool session_namespace_reserved(const moq_namespace_t *ns)
{
    if (!ns || ns->count == 0 || !ns->parts) return false;
    const moq_bytes_t *f = &ns->parts[0];
    if (f->len == 1 && f->data && f->data[0] == '.') return true;
    return f->len == 8 && f->data && memcmp(f->data, ".session", 8) == 0;
}

bool session_refuses_request(const moq_session_t *s, const moq_namespace_t *ns,
                             uint64_t *out_code)
{
    if (session_refuses_new_requests(s)) {
        *out_code = s->profile->going_away_request_error;
        return true;
    }
    if (s->profile->reserved_session_namespaces && session_namespace_reserved(ns)) {
        *out_code = MOQ_REQUEST_ERROR_DOES_NOT_EXIST;
        return true;
    }
    return false;
}

void session_retry_owed_stream_actions(moq_session_t *s)
{
    drain_ref_retry_owed_stops(s);
    session_retry_owed_stops(s);
    sg_retry_owed_resets(s);
    /* A fill stream's owed RESET or STOP, before the terminal it holds. */
    fill_service(s);
    /* A terminal held behind owed stream closures follows them. */
    session_release_held_dones(s);
}

moq_result_t session_core_send_request_goaway(
    moq_session_t *s, moq_request_family_t family, int slot,
    const uint8_t *uri, size_t uri_len, uint64_t timeout_ms)
{
    /* Draft-16 has no per-request GOAWAY (the encode op is NULL); the library
     * signals an unsupported draft-18 operation with MOQ_ERR_INVAL. */
    if (!s->profile->encode_request_goaway) return MOQ_ERR_INVAL;
    /* A client cannot instruct the server to connect elsewhere (§10.4). */
    if (s->perspective == MOQ_PERSPECTIVE_CLIENT && uri_len > 0)
        return MOQ_ERR_INVAL;
    /* Prevalidate the URI up front: a NULL or over-cap URI must be rejected
     * before it can drive an absurd `32 + uri_len` scratch allocation (the
     * encoder re-checks, but allocation happens first). 8192 = §10.4 cap. */
    if (uri_len > 0 && !uri) return MOQ_ERR_INVAL;
    if (uri_len > 8192) return MOQ_ERR_INVAL;

    /* Family × state eligibility (§10.4): the request must be live and the local
     * send half still open. Resolve the request bidi + the one-per-stream marker.
     * Per §10.4 the GOAWAY "does not impact subscription state", so the entry stays
     * live -- the app keeps producing on the old request until the peer tears the
     * old stream down (request_goaway_free_on_teardown; a publisher-role
     * subscription's RESET/STOP is a cancellation instead, see
     * request_stream_is_publisher_subscription). */
    moq_stream_ref_t ref;
    bool *sent;
    bool eligible;
    switch (family) {
    case MOQ_REQUEST_FAMILY_SUBSCRIBE:
        /* A deferred terminal (done_pending) is logically Terminated: no
         * control (including a per-request GOAWAY) may follow it. An owed
         * update answer must precede the GOAWAY, after which nothing more
         * may be written on the request stream. */
        eligible = s->subs[slot].state == MOQ_SUB_ESTABLISHED &&
                   !s->subs[slot].done_pending &&
                   !s->subs[slot].owed_update.owed;
        ref = s->subs[slot].rs.ref;
        sent = &s->subs[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_FETCH:
        eligible = s->fetches[slot].state == MOQ_FETCH_ACCEPTED;
        ref = s->fetches[slot].rs.ref;
        sent = &s->fetches[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_PUBLISH:
        /* As for a subscription: an owed update answer comes first. */
        eligible = s->publishes[slot].state == MOQ_PUB_ESTABLISHED &&
                   !s->publishes[slot].done_pending &&
                   !s->publishes[slot].owed_update.owed;
        ref = s->publishes[slot].rs.ref;
        sent = &s->publishes[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_ANNOUNCEMENT:
        eligible = s->announcements[slot].state == MOQ_ANN_ESTABLISHED;
        ref = s->announcements[slot].rs.ref;
        sent = &s->announcements[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_NS_SUB:
        eligible = s->ns_subs[slot].state == MOQ_NS_SUB_ESTABLISHED;
        ref = s->ns_subs[slot].stream_ref;
        sent = &s->ns_subs[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_SUBSCRIBE_TRACKS:
        eligible = s->track_subs[slot].state == MOQ_TRACK_SUB_ESTABLISHED;
        ref = s->track_subs[slot].rs.ref;
        sent = &s->track_subs[slot].goaway_sent; break;
    case MOQ_REQUEST_FAMILY_TRACK_STATUS:
        /* The requester opened the bidi with FIN; only the responder's send half
         * is still open, so only it may migrate (before answering). */
        eligible = s->track_statuses[slot].state == MOQ_TS_PENDING_PUBLISHER;
        ref = s->track_statuses[slot].rs.ref;
        sent = &s->track_statuses[slot].goaway_sent; break;
    default:
        return MOQ_ERR_INVAL;
    }
    if (!eligible || ref._v == 0) return MOQ_ERR_WRONG_STATE;
    if (*sent) return MOQ_ERR_WRONG_STATE;          /* one GOAWAY per request stream */

    /* Reserve-before-mutate: the action slot, then a sized scratch encode + a
     * queue_send_bidi with fin=false (our send half stays open for the deferred
     * timeout-reset; data streams stay alive). On success we only mark goaway_sent
     * -- the entry stays live so old-session output continues; the peer's
     * spec-mandated old-stream teardown is absorbed later. */
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    size_t scratch_saved = s->output_scratch_len;
    uint8_t *buf = (uint8_t *)scratch_alloc_aligned(s, 32 + uri_len, 1);
    if (!buf) return MOQ_ERR_BUFFER;
    moq_buf_writer_t w;
    moq_buf_writer_init(&w, buf, 32 + uri_len);
    moq_result_t rc = s->profile->encode_request_goaway(s, &w, uri, uri_len,
                                                        timeout_ms);
    if (rc < 0) { s->output_scratch_len = scratch_saved; return rc; }
    rc = queue_send_bidi(s, ref, buf, moq_buf_writer_offset(&w), false);
    s->output_scratch_len = scratch_saved;
    if (rc < 0) return rc;
    *sent = true;
    /* A namespace subscription's publisher side keeps the request it parsed
     * in its receive buffer; from here that buffer collects what the
     * subscriber still sends on the migrating stream. */
    if (family == MOQ_REQUEST_FAMILY_NS_SUB && !s->ns_subs[slot].local_subscriber)
        s->ns_subs[slot].recv_len = 0;
    return MOQ_OK;
}

/* Queue a graceful FIN of our send half on a request bidi (CLOSE_BIDI_STREAM).
 * Stream-correlated profiles use this to close the requester half after a
 * terminal response so the peer can retire the request bidi; it carries no data.
 * Returns MOQ_ERR_WOULD_BLOCK if the action queue is full (retryable). */
moq_result_t queue_close_bidi(moq_session_t *s, moq_stream_ref_t ref)
{
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_CLOSE_BIDI_STREAM;
    a.detail_size = (uint32_t)sizeof(moq_close_bidi_stream_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.close_bidi_stream.stream_ref = ref;
    return push_action(s, &a);
}

moq_result_t queue_open_uni_control(moq_session_t *s, moq_stream_ref_t ref,
                                    const uint8_t *data, size_t len)
{
    if (len == 0) return MOQ_OK;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    if (len > s->send_cap - s->send_len) return MOQ_ERR_BUFFER;
    memcpy(s->send_buf + s->send_len, data, len);

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
    a.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.open_uni_control.stream_ref = ref;
    a.u.open_uni_control.data = s->send_buf + s->send_len;
    a.u.open_uni_control.len  = len;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;

    s->send_len += len;
    return MOQ_OK;
}

/* Queue an OPEN_UNI_CONTROL for `len` bytes a caller has already written at
 * send_buf + send_len. The caller has checked that they fit; the cursor
 * advances only when the action is queued, so a refusal leaves the arena
 * exactly as it was and the bytes are rewritten on retry. */
moq_result_t queue_open_uni_control_in_place(moq_session_t *s,
                                             moq_stream_ref_t ref, size_t len)
{
    if (len == 0) return MOQ_OK;
    if (action_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_OPEN_UNI_CONTROL;
    a.detail_size = (uint32_t)sizeof(moq_open_uni_control_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.open_uni_control.stream_ref = ref;
    a.u.open_uni_control.data = s->send_buf + s->send_len;
    a.u.open_uni_control.len  = len;
    moq_result_t rc = push_action(s, &a);
    if (rc < 0) return rc;
    s->send_len += len;
    return MOQ_OK;
}

/*
 * Close the session with an error code. Internal helper.
 *
 * reason is BORROWED, not copied: the pointer is stored directly in the
 * queued CLOSE_SESSION action and SESSION_CLOSED event, so it must remain
 * valid until BOTH have been consumed. Internal callers pass string
 * literals and are safe unconditionally; moq_session_close() callers
 * supply their own storage under the public borrow contract
 * (moq/session.h), which permits caller storage of any duration that
 * outlives those two consumers. NULL means no reason phrase.
 */
moq_result_t close_with_error(moq_session_t *s,
                               uint64_t code, const char *reason)
{
    s->state = MOQ_SESS_CLOSED;
    /* A closing session has no deferred completions left to sweep; an inherited
     * cursor would resume against freed owners. */
    session_sweep_discard(s);
    s->goaway_deadline_us = UINT64_MAX;
    s->subgroup_deadline_us = UINT64_MAX;
    s->subgroup_retry_deadline_us = UINT64_MAX;
    s->idle_deadline_us = UINT64_MAX;
    s->expiry_retry_deadline_us = UINT64_MAX;
    decref_queued_data_payloads(s);
    decref_queued_event_payloads(s);
    free_rx_stream_bufs(s);
    s->action_head = s->action_tail = 0;
    s->action_timed = s->action_nondata = 0;
    s->send_len = 0;
    s->event_head = s->event_tail = 0;

    /* Invalidate all subgroup entries -- walking the occupancy list, so the
     * close's cost follows the live subgroups rather than `sg_cap`. */
    for (int32_t i = s->sg_occ_head, nxt; i >= 0; i = nxt) {
        nxt = s->subgroups[i].occ_next;
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_close_sg_probes++;
#endif
        if (s->subgroups[i].state != MOQ_SG_FREE)
            sg_free_entry(s, (size_t)i);
    }

    moq_action_t a;
    memset(&a, 0, sizeof(a));
    a.kind = MOQ_ACTION_CLOSE_SESSION;
    a.detail_size = (uint32_t)sizeof(moq_close_session_action_t);
    a.borrow_epoch = s->borrow_epoch;
    a.u.close_session.code = code;
    if (reason) {
        a.u.close_session.reason.data = (const uint8_t *)reason;
        a.u.close_session.reason.len  = strlen(reason);
    }
    push_action(s, &a);

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SESSION_CLOSED;
    e.detail_size = (uint32_t)sizeof(moq_session_closed_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.closed.code = code;
    if (reason) {
        e.u.closed.reason.data = (const uint8_t *)reason;
        e.u.closed.reason.len  = strlen(reason);
    }
    /* Record the enqueued fact ONLY on a successful placement: the fact means
     * "the terminal event is in the queue", not "a close was attempted". Both
     * queues were just cleared above, so this cannot block today -- checking the
     * result keeps the fact honest if that ever changes. Monotonic: an already
     * true fact stays true. The session's terminal outranks a slot a request
     * operation holds for its own terminal: that operation is unwinding
     * inside this close and emits nothing once it sees the session closed. */
    size_t held = s->event_reserve;
    s->event_reserve = 0;
    if (push_event(s, &e) == MOQ_OK)
        s->terminal_enqueued = true;
    s->event_reserve = held;
    return MOQ_OK;
}

/* -- Input handlers ------------------------------------------------ */

static moq_result_t handle_control_bytes(moq_session_t *s,
                                          const uint8_t *buf, size_t len)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    if (len > 0 && !buf) return MOQ_ERR_INVAL;

    if (len > 0) {
        if (len > s->recv_cap - s->recv_len) return MOQ_ERR_BUFFER;
        memcpy(s->recv_buf + s->recv_len, buf, len);
        s->recv_len += len;
    }

    if (s->recv_len == 0) return MOQ_OK;

    size_t consumed = 0;
    moq_result_t rc = s->profile->process_control_data(
        s, s->recv_buf, s->recv_len, &consumed);

    if (consumed > 0 && consumed < s->recv_len)
        memmove(s->recv_buf, s->recv_buf + consumed,
                s->recv_len - consumed);
    s->recv_len -= consumed;

    return rc;
}

static moq_result_t handle_tick(moq_session_t *s)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;

    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT,
                                "idle timeout");

    if (s->goaway_deadline_us != UINT64_MAX &&
        s->last_now_us >= s->goaway_deadline_us) {
        s->goaway_deadline_us = UINT64_MAX;
        return close_with_error(s, MOQ_CLOSE_GOAWAY_TIMEOUT,
                                "GOAWAY drain timeout");
    }

    session_service_outgoing_terminals(s);
    return sg_has_terminal_debt(s) ? MOQ_ERR_WOULD_BLOCK : MOQ_OK;
}

moq_result_t bridge_session_service_outgoing(moq_session_t *s, uint64_t now_us)
{
    if (!session_is_active(s)) return MOQ_OK;
    if (s->subgroup_deadline_us > now_us &&
        session_queued_data_deadline(s) > now_us && !sg_has_terminal_debt(s))
        return MOQ_OK;
    moq_result_t rc = session_advance_entry(s, now_us);
    if (rc != MOQ_OK) return rc;
    if (session_is_active(s)) session_service_outgoing_terminals(s);
    return MOQ_OK;
}

/* -- Datagrams held for an unestablished alias (reordering buffer) ---- */

static void staged_slot_free(moq_session_t *s, size_t i)
{
    moq_staged_datagram_t *e = &s->staged_dg[i];
    if (!e->in_use) return;
    if (e->bytes) s->alloc.free(e->bytes, e->len, s->alloc.ctx);
    if (e->payload) moq_rcbuf_decref(e->payload);
    if (e->properties) moq_rcbuf_decref(e->properties);
    if (e->len <= s->staged_bytes) s->staged_bytes -= e->len;
    else                           s->staged_bytes = 0;
    *e = (moq_staged_datagram_t){0};
    if (s->staged_count) s->staged_count--;
}

/* Free all held datagrams but keep the ring array (discard, not destroy). */
static void staged_clear(moq_session_t *s)
{
    if (!s->staged_dg) return;
    for (size_t i = 0; i < s->staged_cap; i++) staged_slot_free(s, i);
}

static bool staged_held_ready(moq_session_t *s, const moq_staged_datagram_t *e);

/* Free only held datagrams whose alias is NOT established. An established
 * alias's held data is still deliverable (merely stuck under event-queue
 * pressure) and must not be dropped just because no subscription is pending. */
static void staged_clear_unresolved(moq_session_t *s)
{
    if (!s->staged_dg) return;
    for (size_t i = 0; i < s->staged_cap; i++) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_staged_probes++;
#endif
        if (!s->staged_dg[i].in_use) continue;
        if (moq_subscription_is_valid(s->staged_dg[i].for_sub)) {
            /* Held for a subscription that has its answer by now: kept only
             * while that answer can still surface it. */
            if (!staged_held_ready(s, &s->staged_dg[i])) staged_slot_free(s, i);
            continue;
        }
        if (sub_find_by_alias_subscriber(s, s->staged_dg[i].alias) < 0)
            staged_slot_free(s, i);
    }
}

/* Free held datagrams and the ring array itself (session teardown). */
static void free_staged_datagrams(moq_session_t *s)
{
    if (!s->staged_dg) return;
    staged_clear(s);
    s->alloc.free(s->staged_dg, s->staged_cap * sizeof(*s->staged_dg),
                  s->alloc.ctx);
    s->staged_dg = NULL;
}

/* Evict the lowest-seq held datagram (drop-oldest), counted. */
static void staged_evict_oldest(moq_session_t *s)
{
    size_t best = s->staged_cap;
    uint64_t best_seq = UINT64_MAX;
    for (size_t i = 0; i < s->staged_cap; i++) {
        if (s->staged_dg[i].in_use && s->staged_dg[i].seq < best_seq) {
            best = i;
            best_seq = s->staged_dg[i].seq;
        }
    }
    if (best < s->staged_cap) {
        staged_slot_free(s, best);
        s->staged_dropped++;
    }
}

/* A free slot in the held ring for `len` more bytes, evicting the oldest
 * entries as needed; staged_cap when there is none (counted as a drop). */
static size_t staged_take_slot(moq_session_t *s, size_t len)
{
    if (s->staged_cap == 0 || len > s->staged_bytes_cap) { s->staged_dropped++; return s->staged_cap; }
    if (!s->staged_dg) {
        size_t sz = s->staged_cap * sizeof(*s->staged_dg);
        s->staged_dg = (moq_staged_datagram_t *)s->alloc.alloc(sz, s->alloc.ctx);
        if (!s->staged_dg) { s->staged_dropped++; return s->staged_cap; }
        memset(s->staged_dg, 0, sz);
    }
    while (s->staged_count > 0 && s->staged_bytes + len > s->staged_bytes_cap)
        staged_evict_oldest(s);
    size_t slot = s->staged_cap;
    for (size_t i = 0; i < s->staged_cap; i++)
        if (!s->staged_dg[i].in_use) { slot = i; break; }
    if (slot == s->staged_cap) { staged_evict_oldest(s);
        for (size_t i = 0; i < s->staged_cap; i++)
            if (!s->staged_dg[i].in_use) { slot = i; break; }
    }
    if (slot == s->staged_cap) s->staged_dropped++;
    return slot;
}

moq_request_error_encode_args_t session_capacity_request_error(
    const moq_session_t *s, uint64_t request_id, const char *reason,
    size_t reason_len)
{
    moq_request_error_encode_args_t a;
    memset(&a, 0, sizeof(a));
    a.request_id = request_id;
    a.error_code = s->profile->capacity_request_error;
    a.can_retry = s->profile->capacity_request_error_retryable;
    a.retry_after_ms = 0;
    a.reason = (const uint8_t *)reason;
    a.reason_len = reason_len;
    return a;
}

void session_stage_held_object(moq_session_t *s, moq_subscription_t for_sub,
                               uint64_t alias, const moq_held_object_t *o)
{
    size_t plen = o->payload ? moq_rcbuf_len(o->payload) : 0;
    size_t elen = o->properties ? moq_rcbuf_len(o->properties) : 0;
    if (elen > SIZE_MAX - plen) { s->staged_dropped++; return; }
    size_t slot = staged_take_slot(s, plen + elen);
    if (slot == s->staged_cap) return;
    s->staged_dg[slot] = (moq_staged_datagram_t){
        .in_use = true, .seq = s->staged_next_seq++, .alias = alias,
        .len = plen + elen, .for_sub = for_sub,
        .group_id = o->group_id, .subgroup_id = o->subgroup_id,
        .object_id = o->object_id, .publisher_priority = o->publisher_priority,
        .status = o->status, .end_of_group = o->end_of_group,
        .datagram = o->datagram,
        .default_priority = o->default_priority,
        .first_object = o->first_object,
        .subgroup_properties = o->subgroup_properties,
        .payload = o->payload ? moq_rcbuf_incref(o->payload) : NULL,
        .properties = o->properties ? moq_rcbuf_incref(o->properties) : NULL };
    s->staged_count++;
    s->staged_bytes += plen + elen;
}

void session_settle_pending(moq_session_t *s, moq_subscription_t sub,
                            bool established, uint64_t alias)
{
    if (!s->profile->allows_concurrent_subscriptions) return;
    if (s->staged_dg) {
        for (size_t i = 0; i < s->staged_cap; i++) {
            moq_staged_datagram_t *e = &s->staged_dg[i];
            if (!e->in_use || !moq_subscription_is_valid(e->for_sub) ||
                !moq_subscription_eq(e->for_sub, sub))
                continue;
            if (!established || e->alias != alias) staged_slot_free(s, i);
        }
    }
    rx_settle_pending(s, sub, established, alias);
}

/* An object held for a subscription that is now established under the
 * entry's alias: surfaced if the subscription's own filter accepts it,
 * dropped otherwise. WOULD_BLOCK keeps it held. */
static moq_result_t staged_deliver_held(moq_session_t *s, size_t i)
{
    moq_staged_datagram_t *e = &s->staged_dg[i];
    int slot = sub_resolve_handle(s, e->for_sub);
    if (slot < 0 || !sub_accepts_object(&s->subs[slot], e->group_id, e->object_id)) {
        staged_slot_free(s, i);
        return MOQ_OK;
    }
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;
    moq_event_t ev;
    memset(&ev, 0, sizeof(ev));
    ev.kind = MOQ_EVENT_OBJECT_RECEIVED;
    ev.detail_size = (uint32_t)sizeof(moq_object_received_event_t);
    ev.borrow_epoch = s->borrow_epoch;
    moq_object_received_event_t *o = &ev.u.object_received;
    o->sub = e->for_sub;
    o->group_id = e->group_id;
    o->subgroup_id = e->subgroup_id;
    o->object_id = e->object_id;
    o->publisher_priority = session_resolve_publisher_priority(s, e->for_sub,
        MOQ_PUBLICATION_INVALID, e->default_priority, e->publisher_priority);
    o->status = (moq_object_status_t)e->status;
    o->end_of_group = e->end_of_group;
    o->datagram = e->datagram;
    o->first_object = e->first_object;
    o->subgroup_properties = e->subgroup_properties;
    o->payload = e->payload;
    o->properties = e->properties;
    moq_result_t rc = push_event(s, &ev);
    if (rc < 0) return rc;
    /* The references moved to the event, and with them the budget share the
     * poll releases. */
    s->recv_payload_bytes += e->len;
    e->payload = NULL;
    e->properties = NULL;
    staged_slot_free(s, i);
    return MOQ_OK;
}

static bool staged_held_ready(moq_session_t *s, const moq_staged_datagram_t *e)
{
    int slot = sub_resolve_handle(s, e->for_sub);
    return slot >= 0 && s->subs[slot].state == MOQ_SUB_ESTABLISHED &&
           s->subs[slot].role == MOQ_SUB_ROLE_SUBSCRIBER &&
           s->subs[slot].track_alias == e->alias;
}

/* Copy a datagram for later replay once its alias is established. Bounded by
 * slot count + a byte budget that shares the receive buffer; on overflow or
 * allocation failure the data is DROPPED (counted) -- the peer should not be
 * sending data for an unestablished alias, and the spec permits dropping it,
 * so this never fails the receive path with NOMEM. */
static void stage_datagram(moq_session_t *s, uint64_t alias,
                           const uint8_t *buf, size_t len)
{
    if (s->staged_cap == 0) { s->staged_dropped++; return; }
    if (len > s->staged_bytes_cap) { s->staged_dropped++; return; }
    if (!s->staged_dg) {
        size_t sz = s->staged_cap * sizeof(*s->staged_dg);
        s->staged_dg = (moq_staged_datagram_t *)s->alloc.alloc(sz, s->alloc.ctx);
        if (!s->staged_dg) { s->staged_dropped++; return; }
        memset(s->staged_dg, 0, sz);
    }
    while (s->staged_count > 0 && s->staged_bytes + len > s->staged_bytes_cap)
        staged_evict_oldest(s);
    size_t slot = s->staged_cap;
    for (size_t i = 0; i < s->staged_cap; i++)
        if (!s->staged_dg[i].in_use) { slot = i; break; }
    if (slot == s->staged_cap) { staged_evict_oldest(s);
        for (size_t i = 0; i < s->staged_cap; i++)
            if (!s->staged_dg[i].in_use) { slot = i; break; }
    }
    if (slot == s->staged_cap) { s->staged_dropped++; return; }
    uint8_t *copy = NULL;
    if (len > 0) {
        copy = (uint8_t *)s->alloc.alloc(len, s->alloc.ctx);
        if (!copy) { s->staged_dropped++; return; }
        memcpy(copy, buf, len);
    }
    s->staged_dg[slot] = (moq_staged_datagram_t){
        .in_use = true, .seq = s->staged_next_seq++,
        .alias = alias, .bytes = copy, .len = len };
    s->staged_count++;
    s->staged_bytes += len;
}

/* Decode + deliver one datagram. When allow_stage is set and the datagram
 * parses cleanly but its alias is not yet established (and a forwarding
 * subscription is pending), it is held for control/data reordering instead
 * of dropped. Replay passes allow_stage=false. */
static moq_result_t deliver_datagram(moq_session_t *s,
                                     const uint8_t *buf, size_t len,
                                     bool allow_stage)
{
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;

    /* Zero-init: a padding/ignored datagram returns MOQ_DONE without
     * touching unknown_alias, so it must default false (never staged). */
    moq_decoded_object_datagram_t decoded = {0};
    moq_result_t rc = s->profile->decode_object_datagram(s, buf, len, &decoded);
    if (rc == MOQ_ERR_PROTO)
        return close_with_error(s, 0x3, "malformed object datagram");
    if (rc > 0) {
        if (allow_stage && decoded.unknown_alias &&
            session_has_forwarding_pending_subscriber(s))
            stage_datagram(s, decoded.track_alias, buf, len);
        return MOQ_OK;
    }
    if (rc < 0) return rc;
    if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;

    return session_core_on_object_datagram(s, &decoded,
        decoded.payload, decoded.payload_len,
        decoded.properties, decoded.properties_len);
}

static moq_result_t handle_datagram(moq_session_t *s,
                                     const uint8_t *buf, size_t len)
{
    return deliver_datagram(s, buf, len, true);
}

/* Deliver held datagrams whose alias is now established, oldest first.
 * `only_alias` (when not UINT64_MAX) restricts to one alias. Stops on
 * event-queue backpressure, leaving the rest held for a later retry.
 *
 * Oldest-first is preserved by a min-seq scan of the whole fixed array per
 * delivered datagram, so this is O(cap^2). That is intentional and safe ONLY
 * because cap is tiny (MOQ_STAGED_DG_SLOTS = 16); do not raise the cap without
 * replacing this min-scan with an indexed/heap/ordered structure. */
static void staged_replay(moq_session_t *s, uint64_t only_alias)
{
    if (!s->staged_dg) return;
    for (;;) {
        /* Select the lowest-seq deliverable slot (oldest-first). */
        size_t best = s->staged_cap;
        uint64_t best_seq = UINT64_MAX;
        for (size_t i = 0; i < s->staged_cap; i++) {
            moq_staged_datagram_t *e = &s->staged_dg[i];
            if (!e->in_use) continue;
            if (only_alias != UINT64_MAX && e->alias != only_alias) continue;
            if (moq_subscription_is_valid(e->for_sub)) {
                if (!staged_held_ready(s, e)) continue;
            } else if (sub_find_by_alias_subscriber(s, e->alias) < 0) continue;
            if (e->seq < best_seq) { best = i; best_seq = e->seq; }
        }
        if (best == s->staged_cap) break;
        if (moq_subscription_is_valid(s->staged_dg[best].for_sub)) {
            if (staged_deliver_held(s, best) == MOQ_ERR_WOULD_BLOCK) break;
            continue;
        }
        moq_result_t rc = deliver_datagram(s, s->staged_dg[best].bytes,
                                           s->staged_dg[best].len, false);
        if (rc == MOQ_ERR_WOULD_BLOCK) break;   /* no capacity; retry later */
        staged_slot_free(s, best);
    }
}

void session_release_staged_for_alias(moq_session_t *s, uint64_t alias)
{
    staged_replay(s, alias);
    session_resume_deferred_for_alias(s, alias);
}

void session_replay_staged(moq_session_t *s)
{
    if (s->staged_count) staged_replay(s, UINT64_MAX);
    session_retry_resumed_deferred(s);
}

void session_discard_staged_if_no_pending(moq_session_t *s)
{
    if (session_has_forwarding_pending_subscriber(s)) return;
    /* Drop only data for aliases that can never resolve now; data for an
     * already-established alias stays (it is just awaiting event capacity). */
    staged_clear_unresolved(s);
    session_discard_deferred_streams(s);
}

static moq_result_t session_step(moq_session_t *s, const moq_input_t *input)
{
    if (!s || !input) return MOQ_ERR_INVAL;

    moq_result_t arc = session_advance_entry(s, input->now_us);
    if (arc != MOQ_OK) return arc;

    switch (input->kind) {
    case MOQ_INPUT_START:
        return handle_start(s);
    case MOQ_INPUT_CONTROL_BYTES:
        if (session_idle_expired(s))
            return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT,
                                    "idle timeout");
        if (input->u.control_bytes.len > 0)
            session_refresh_idle(s, input->now_us);
        return handle_control_bytes(s, input->u.control_bytes.buf,
                                    input->u.control_bytes.len);
    case MOQ_INPUT_TICK:
        return handle_tick(s);
    case MOQ_INPUT_DATAGRAM:
        if (session_idle_expired(s))
            return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT,
                                    "idle timeout");
        session_refresh_idle(s, input->now_us);
        return handle_datagram(s, input->u.datagram.buf,
                               input->u.datagram.len);
    default:
        return MOQ_ERR_INVAL;
    }
}

/* -- Public API ---------------------------------------------------- */

/* Frozen v0 prefix: struct_size + alloc + perspective. A pointer-only init can
 * only safely touch these (it cannot know the caller's struct size), and
 * moq_session_create() requires at least this much. Must never shrink. */
#define MOQ_SESSION_CFG_V0_SIZE \
    (offsetof(moq_session_cfg_t, send_request_capacity))

void moq_session_cfg_init(moq_session_cfg_t *cfg,
                           const moq_alloc_t *alloc,
                           moq_perspective_t perspective)
{
    if (!cfg) return;
    /* Pointer-only: clear/stamp ONLY the frozen v0 prefix so an old caller's
     * smaller struct is never written past. Fields beyond the prefix are left
     * as-is and ignored by create (struct_size == V0); a caller that sets them
     * must use moq_session_cfg_init_sized(). */
    memset(cfg, 0, MOQ_SESSION_CFG_V0_SIZE);
    cfg->struct_size = (uint32_t)MOQ_SESSION_CFG_V0_SIZE;
    cfg->alloc = alloc;
    cfg->perspective = perspective;
}

void moq_session_cfg_init_sized(moq_session_cfg_t *cfg, size_t cfg_size,
                                 const moq_alloc_t *alloc,
                                 moq_perspective_t perspective)
{
    if (!cfg) return;
    /* Clear exactly what the caller allocated, never more than this library's
     * struct knows about (matches moq_pub_cfg_init_sized). */
    size_t n = cfg_size < sizeof(*cfg) ? cfg_size : sizeof(*cfg);
    if (n < sizeof(cfg->struct_size)) return;   /* too small to even stamp */
    memset(cfg, 0, n);
    cfg->struct_size = (uint32_t)n;
    if (n >= offsetof(moq_session_cfg_t, alloc) + sizeof(cfg->alloc))
        cfg->alloc = alloc;
    if (n >= offsetof(moq_session_cfg_t, perspective) + sizeof(cfg->perspective))
        cfg->perspective = perspective;
}

void moq_action_cleanup(moq_action_t *action)
{
    if (!action) return;
    if (action->kind == MOQ_ACTION_SEND_DATA && action->u.send_data.payload) {
        moq_rcbuf_decref(action->u.send_data.payload);
        action->u.send_data.payload = NULL;
    }
}

void moq_event_cleanup(moq_event_t *event)
{
    if (!event) return;
    if (event->kind == MOQ_EVENT_OBJECT_RECEIVED) {
        if (event->u.object_received.payload) {
            moq_rcbuf_decref(event->u.object_received.payload);
            event->u.object_received.payload = NULL;
        }
        if (event->u.object_received.properties) {
            moq_rcbuf_decref(event->u.object_received.properties);
            event->u.object_received.properties = NULL;
        }
    } else if (event->kind == MOQ_EVENT_FETCH_OBJECT) {
        if (event->u.fetch_object.payload) {
            moq_rcbuf_decref(event->u.fetch_object.payload);
            event->u.fetch_object.payload = NULL;
        }
        if (event->u.fetch_object.properties) {
            moq_rcbuf_decref(event->u.fetch_object.properties);
            event->u.fetch_object.properties = NULL;
        }
    } else if (event->kind == MOQ_EVENT_FILL_OBJECT) {
        if (event->u.fill_object.payload) {
            moq_rcbuf_decref(event->u.fill_object.payload);
            event->u.fill_object.payload = NULL;
        }
        if (event->u.fill_object.properties) {
            moq_rcbuf_decref(event->u.fill_object.properties);
            event->u.fill_object.properties = NULL;
        }
    } else if (event->kind == MOQ_EVENT_OBJECT_CHUNK) {
        if (event->u.object_chunk.chunk) {
            moq_rcbuf_decref(event->u.object_chunk.chunk);
            event->u.object_chunk.chunk = NULL;
        }
        if (event->u.object_chunk.properties) {
            moq_rcbuf_decref(event->u.object_chunk.properties);
            event->u.object_chunk.properties = NULL;
        }
    }
}

/* §9.8 pinned append offset: done_wait_timeout_us begins at the first
 * 8-byte-aligned slot after the pre-append tail. If this moves, the append
 * broke ABI. */
_Static_assert(offsetof(moq_session_cfg_t, done_wait_timeout_us) ==
               ((offsetof(moq_session_cfg_t, max_track_history_records)
                 + sizeof(uint32_t) + 7u) & ~(size_t)7u),
               "done_wait_timeout_us must occupy the first aligned slot "
               "after the pre-append tail");

/* setup_auth_tokens starts at or past the sizeof of the layout that ended at
 * max_namespace_suffixes_per_subscription (that end rounded up to the struct
 * alignment), so an old caller's tail padding is never read as the field. */
_Static_assert(offsetof(moq_session_cfg_t, setup_auth_tokens) >=
               ((offsetof(moq_session_cfg_t,
                          max_namespace_suffixes_per_subscription) +
                 sizeof(uint32_t) + _Alignof(moq_session_cfg_t) - 1u) &
                ~(size_t)(_Alignof(moq_session_cfg_t) - 1u)),
               "setup_auth_tokens must start past the previous sizeof");

/* Return the session block to its allocator. The allocator is copied out
 * first: a session that presented SETUP tokens wipes the whole block (the
 * copies, their encodings and any SETUP still in the send arena hold the
 * credential), and the wipe clears the session's own copy of the allocator. */
static void session_release_block(moq_session_t *s, size_t size)
{
    moq_alloc_t alloc = s->alloc;
    if (s->setup_token_count > 0) session_wipe(s, size);
    alloc.free(s, size, alloc.ctx);
}

static size_t cfg_read_u32(const moq_session_cfg_t *cfg, size_t field_offset,
                            size_t field_size)
{
    if (cfg->struct_size >= field_offset + field_size)
        return *(const uint32_t *)((const char *)cfg + field_offset);
    return 0;
}

moq_result_t moq_session_create(const moq_session_cfg_t *cfg,
                                 uint64_t now_us,
                                 moq_session_t **out)
{
    if (out) *out = NULL;
    if (!cfg || !out) return MOQ_ERR_INVAL;
    if (cfg->struct_size < offsetof(moq_session_cfg_t, send_request_capacity))
        return MOQ_ERR_INVAL;
    if (!cfg->alloc || !cfg->alloc->alloc || !cfg->alloc->realloc ||
        !cfg->alloc->free)
        return MOQ_ERR_INVAL;
    if (cfg->perspective != MOQ_PERSPECTIVE_CLIENT &&
        cfg->perspective != MOQ_PERSPECTIVE_SERVER)
        return MOQ_ERR_INVAL;

    /* Validate request capacity config (values consumed by profile init). */
    {
        bool send_request_capacity = false;
        uint64_t initial_request_capacity = 0;
        if (cfg->struct_size >=
            offsetof(moq_session_cfg_t, send_request_capacity) +
            sizeof(cfg->send_request_capacity))
            send_request_capacity = cfg->send_request_capacity;
        if (send_request_capacity) {
            if (cfg->struct_size <
                offsetof(moq_session_cfg_t, initial_request_capacity) +
                sizeof(cfg->initial_request_capacity))
                return MOQ_ERR_INVAL;
            initial_request_capacity = cfg->initial_request_capacity;
        }
        if (send_request_capacity && initial_request_capacity > MOQ_QUIC_VARINT_MAX)
            return MOQ_ERR_INVAL;
    }

    bool send_auth_token_cache_size = false;
    uint64_t auth_token_cache_size = 0;
    if (cfg->struct_size >=
        offsetof(moq_session_cfg_t, send_auth_token_cache_size) +
        sizeof(cfg->send_auth_token_cache_size))
        send_auth_token_cache_size = cfg->send_auth_token_cache_size;
    if (send_auth_token_cache_size) {
        if (cfg->struct_size >=
            offsetof(moq_session_cfg_t, auth_token_cache_size) +
            sizeof(cfg->auth_token_cache_size))
            auth_token_cache_size = cfg->auth_token_cache_size;
    }

    /* Resolve the effective profile before checking SETUP token width.
     * An absent/zero version retains the existing draft-16 default. */
    moq_version_t version = MOQ_VERSION_DRAFT_16;
    if (cfg->struct_size >= offsetof(moq_session_cfg_t, version) +
        sizeof(cfg->version)) {
        if (cfg->version != 0)
            version = cfg->version;
    }
    const moq_profile_ops_t *profile = moq_profile_lookup(version);
    if (!profile) return MOQ_ERR_INVAL;

    /* SETUP authorization tokens: validated here, copied into the arena. */
    const moq_auth_token_t *setup_tokens_in = NULL;
    size_t setup_token_count = 0;
    size_t setup_token_value_bytes = 0;
    if (cfg->struct_size >= offsetof(moq_session_cfg_t, setup_auth_token_count) +
        sizeof(cfg->setup_auth_token_count) && cfg->setup_auth_token_count > 0) {
        setup_tokens_in = cfg->setup_auth_tokens;
        setup_token_count = cfg->setup_auth_token_count;
        if (setup_token_count > MOQ_DECODED_MAX_TOKENS ||
            moq_validate_auth_tokens(setup_tokens_in, setup_token_count,
                                     profile->auth_token_type_max) < 0)
            return MOQ_ERR_INVAL;
        for (size_t i = 0; i < setup_token_count; i++) {
            if (setup_tokens_in[i].token_value.len > UINT16_MAX)
                return MOQ_ERR_INVAL;
            setup_token_value_bytes += setup_tokens_in[i].token_value.len;
        }
        /* Each token adds at most its type, its alias type, a length and an
         * option type to the setup message, whose length field is 16 bits. */
        if (setup_token_value_bytes + setup_token_count * 32u > UINT16_MAX)
            return MOQ_ERR_INVAL;
    }

    size_t action_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_actions), sizeof(cfg->max_actions));
    size_t event_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_events), sizeof(cfg->max_events));
    size_t send_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, send_buffer_size), sizeof(cfg->send_buffer_size));
    size_t recv_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, recv_buffer_size), sizeof(cfg->recv_buffer_size));
    size_t sub_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_subscriptions), sizeof(cfg->max_subscriptions));
    size_t scratch_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, output_scratch_size), sizeof(cfg->output_scratch_size));

    size_t sg_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_open_subgroups), sizeof(cfg->max_open_subgroups));
    size_t rx_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_data_streams), sizeof(cfg->max_data_streams));
    size_t max_obj_payload = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_object_payload_size), sizeof(cfg->max_object_payload_size));
    size_t max_recv_buf = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_receive_buffer_bytes), sizeof(cfg->max_receive_buffer_bytes));
    size_t ann_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_announcements), sizeof(cfg->max_announcements));

    if (!action_cap)  action_cap  = MOQ_DEFAULT_MAX_ACTIONS;
    if (!event_cap)   event_cap   = MOQ_DEFAULT_MAX_EVENTS;
    if (sub_cap > 0xFFFF) return MOQ_ERR_INVAL; /* handle slot is 16-bit */
    if (!send_cap)    send_cap    = MOQ_DEFAULT_SEND_BUF;
    if (!recv_cap)    recv_cap    = MOQ_DEFAULT_RECV_BUF;
    if (!sub_cap)     sub_cap     = MOQ_DEFAULT_MAX_SUBS;
    if (!scratch_cap) scratch_cap = MOQ_DEFAULT_OUTPUT_SCRATCH;
    if (!sg_cap)      sg_cap      = MOQ_DEFAULT_MAX_SUBGROUPS;
    if (sg_cap > 0xFFFF) return MOQ_ERR_INVAL;
    if (!rx_cap)      rx_cap      = MOQ_DEFAULT_MAX_DATA_STREAMS;
    if (!max_obj_payload)  max_obj_payload  = MOQ_DEFAULT_MAX_OBJ_PAYLOAD;
    if (!max_recv_buf) max_recv_buf = MOQ_DEFAULT_MAX_RECV_BUF;
    if (!ann_cap)     ann_cap     = MOQ_DEFAULT_MAX_ANNOUNCEMENTS;
    if (ann_cap > 0xFFFF) return MOQ_ERR_INVAL;

    size_t fetch_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_fetches),
        sizeof(cfg->max_fetches));
    if (!fetch_cap)   fetch_cap   = MOQ_DEFAULT_MAX_FETCHES;
    if (fetch_cap > 0xFFFF) return MOQ_ERR_INVAL;

    size_t pub_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_publishes),
        sizeof(cfg->max_publishes));
    if (!pub_cap)     pub_cap     = MOQ_DEFAULT_MAX_PUBLISHES;
    if (pub_cap > 0xFFFF) return MOQ_ERR_INVAL;

    size_t ts_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_track_statuses),
        sizeof(cfg->max_track_statuses));
    if (!ts_cap)      ts_cap      = MOQ_DEFAULT_MAX_TRACK_STATUSES;
    if (ts_cap > 0xFFFF) return MOQ_ERR_INVAL;

    size_t ns_sub_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_namespace_subscriptions),
        sizeof(cfg->max_namespace_subscriptions));
    if (!ns_sub_cap)  ns_sub_cap  = MOQ_DEFAULT_MAX_NS_SUBS;
    if (ns_sub_cap > 0xFFFF) return MOQ_ERR_INVAL;
    size_t ns_sub_recv_cap = recv_cap < 4096 ? recv_cap : 4096;

    /* Per-subscription active peer-announced suffix cap; 0 = default. Applies
     * in addition to the receive-byte budget, not instead of it. */
    size_t max_ns_suffixes = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_namespace_suffixes_per_subscription),
        sizeof(cfg->max_namespace_suffixes_per_subscription));
    if (!max_ns_suffixes) max_ns_suffixes = MOQ_DEFAULT_MAX_NS_SUFFIXES;

    size_t track_sub_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_track_subscriptions),
        sizeof(cfg->max_track_subscriptions));
    if (!track_sub_cap)  track_sub_cap = MOQ_DEFAULT_MAX_TRACK_SUBS;
    if (track_sub_cap > 0xFFFF) return MOQ_ERR_INVAL;

    bool defer_subscription_updates = false;
    if (cfg->struct_size >=
        offsetof(moq_session_cfg_t, defer_subscription_updates) +
        sizeof(cfg->defer_subscription_updates))
        defer_subscription_updates = cfg->defer_subscription_updates;
    bool defer_fetch_ranges = false;
    if (cfg->struct_size >=
        offsetof(moq_session_cfg_t, defer_fetch_ranges) +
        sizeof(cfg->defer_fetch_ranges))
        defer_fetch_ranges = cfg->defer_fetch_ranges;

    /* Fill children: separately allocated below. The slot is 16 bits wide. */
    size_t fill_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_fills), sizeof(cfg->max_fills));
    if (!fill_cap)    fill_cap = MOQ_DEFAULT_MAX_FILLS;
    if (fill_cap > 0x10000) return MOQ_ERR_INVAL;

    /* Track-largest history registry: default covers one record per
     * subscription + publish + track-status entry (each is a merge source
     * and reserves a record at establishment). Separately allocated below.
     * The default sum cannot overflow: each cap is already bounded <=0xFFFF.
     * A caller-supplied cap is only bounded by the pool byte size fitting in
     * size_t (checked here); an over-large but representable cap simply fails
     * the allocation with MOQ_ERR_NOMEM below -- no arbitrary public ceiling. */
    size_t th_cap = cfg_read_u32(cfg,
        offsetof(moq_session_cfg_t, max_track_history_records),
        sizeof(cfg->max_track_history_records));
    /* §9.8: done_wait_timeout_us (appended; read only when covered). 0 =
     * the finite library default -- old-size callers, an absent field, and
     * an explicit zero all select it. */
    uint64_t done_wait_us = 0;
    if (cfg->struct_size >= offsetof(moq_session_cfg_t, done_wait_timeout_us)
                            + sizeof(cfg->done_wait_timeout_us))
        done_wait_us = cfg->done_wait_timeout_us;
    if (done_wait_us == 0) done_wait_us = MOQ_DONE_WAIT_DEFAULT_US;
    if (!th_cap)      th_cap = sub_cap + pub_cap + ts_cap;
    if (th_cap > SIZE_MAX / sizeof(moq_track_hist_t)) return MOQ_ERR_INVAL;

    bool cluster_config = cfg->struct_size >= offsetof(moq_session_cfg_t, cluster_relay_cost) +
                                                  sizeof(cfg->cluster_relay_cost);
    bool cluster_enabled = cluster_config && cfg->cluster_enabled;
    if (cluster_enabled && !profile->cluster_metadata)
        return MOQ_ERR_UNSUPPORTED;
#define ALIGN_UP(x, a) (((x) + (a) - 1) & ~((a) - 1))
    /* MSVC's C headers lack max_align_t. Name the fallback type outside
     * _Alignof, where MSVC diagnoses an inline type definition. */
#if defined(_MSC_VER) && !defined(__clang__)
    typedef union {
        long long ll;
        long double ld;
        void *p;
    } moq_max_align_t;
    const size_t scratch_align = _Alignof(moq_max_align_t);
#else
    const size_t scratch_align = _Alignof(max_align_t);
#endif

    const size_t act_align    = _Alignof(moq_action_t);
    const size_t evt_align    = _Alignof(moq_event_t);
    const size_t sub_align    = _Alignof(moq_sub_entry_t);
    const size_t ann_align    = _Alignof(moq_ann_entry_t);
    const size_t fetch_align  = _Alignof(moq_fetch_entry_t);
    const size_t pub_align    = _Alignof(moq_pub_entry_t);
    const size_t ts_align     = _Alignof(moq_ts_entry_t);
    const size_t sg_align     = _Alignof(moq_sg_entry_t);
    const size_t rx_align     = _Alignof(moq_rx_stream_t);
    const size_t ns_sub_align = _Alignof(moq_ns_sub_entry_t);
    const size_t track_sub_align = _Alignof(moq_track_sub_entry_t);

    size_t off_actions = ALIGN_UP(sizeof(moq_session_t), act_align);
    size_t act_bytes   = action_cap * sizeof(moq_action_t);
    size_t off_action_meta = ALIGN_UP(off_actions + act_bytes,
                                      _Alignof(moq_action_meta_t));
    size_t action_meta_bytes = action_cap * sizeof(moq_action_meta_t);
    size_t off_events  = ALIGN_UP(off_action_meta + action_meta_bytes, evt_align);
    size_t evt_bytes   = event_cap * sizeof(moq_event_t);
    size_t location_cap = profile->encode_publish_state_notify ? event_cap : 0;
    size_t location_bytes = location_cap * sizeof(moq_state_location_cell_t);
    size_t off_locations = ALIGN_UP(off_events + evt_bytes,
                                     _Alignof(moq_state_location_cell_t));
    size_t off_send    = off_locations + location_bytes;
    size_t off_recv    = off_send + send_cap;
    size_t off_subs    = ALIGN_UP(off_recv + recv_cap, sub_align);
    size_t sub_bytes   = sub_cap * sizeof(moq_sub_entry_t);
    size_t off_ann     = ALIGN_UP(off_subs + sub_bytes, ann_align);
    size_t ann_bytes   = ann_cap * sizeof(moq_ann_entry_t);
    size_t off_fetch   = ALIGN_UP(off_ann + ann_bytes, fetch_align);
    size_t fetch_bytes = fetch_cap * sizeof(moq_fetch_entry_t);
    size_t off_pub     = ALIGN_UP(off_fetch + fetch_bytes, pub_align);
    size_t pub_bytes   = pub_cap * sizeof(moq_pub_entry_t);
    size_t off_ts      = ALIGN_UP(off_pub + pub_bytes, ts_align);
    size_t ts_bytes    = ts_cap * sizeof(moq_ts_entry_t);
    size_t off_sg      = ALIGN_UP(off_ts + ts_bytes, sg_align);
    size_t sg_bytes    = sg_cap * sizeof(moq_sg_entry_t);
    size_t off_rx      = ALIGN_UP(off_sg + sg_bytes, rx_align);
    size_t rx_bytes    = rx_cap * sizeof(moq_rx_stream_t);
    size_t off_rx_fin  = ALIGN_UP(off_rx + rx_bytes, _Alignof(uint64_t));
    size_t rx_fin_cap  = rx_cap < 16 ? 16 : rx_cap;
    size_t rx_fin_bytes = rx_fin_cap * sizeof(uint64_t);
    size_t off_rx_stopped = ALIGN_UP(off_rx_fin + rx_fin_bytes,
                                     _Alignof(uint64_t));
    size_t rx_stopped_cap = rx_fin_cap;
    size_t rx_stopped_bytes = rx_stopped_cap * sizeof(uint64_t);
    size_t off_unsub_tomb = ALIGN_UP(off_rx_stopped + rx_stopped_bytes,
                                      _Alignof(uint64_t));
    size_t unsub_tomb_cap = sub_cap + pub_cap;
    size_t unsub_tomb_bytes = unsub_tomb_cap * sizeof(uint64_t);
    /* Bounded grace cache of locally-cancelled FETCH request IDs (see the field
     * comment in moq_session_t). The fetch slot is freed on cancel; this absorbs
     * a late response for the cancelled id without reoccupying the pool. */
    size_t off_fetch_cancel_tomb = ALIGN_UP(off_unsub_tomb + unsub_tomb_bytes,
                                            _Alignof(uint64_t));
    size_t fetch_cancel_tomb_cap = fetch_cap;
    size_t fetch_cancel_tomb_bytes = fetch_cancel_tomb_cap * sizeof(uint64_t);
    /* Sibling grace cache of request IDs of locally ABORTED publisher-role
     * fetches (see the field comment): a crossing peer FETCH_CANCEL for one of
     * them is consumed rather than closing the session. Same bound. */
    size_t off_fetch_abort_tomb = ALIGN_UP(off_fetch_cancel_tomb + fetch_cancel_tomb_bytes,
                                           _Alignof(uint64_t));
    size_t fetch_abort_tomb_cap = fetch_cap;
    size_t fetch_abort_tomb_bytes = fetch_abort_tomb_cap * sizeof(uint64_t);
    /* Drain ring of request-bidi stream_refs locally cancelled while their
     * response was still possibly in flight (stream-correlated profiles). Late
     * response bytes on these refs are discarded until FIN/reset instead of
     * being mistaken for a new inbound request. Draft-16 leaves it empty. */
    size_t off_drain_ref = ALIGN_UP(off_fetch_abort_tomb + fetch_abort_tomb_bytes,
                                     _Alignof(uint64_t));
    /* Drain refs absorb late in-flight responses on locally-cancelled request
     * bidis: subscriptions (unsubscribe), fetches (fetch-cancel),
     * announcements (publish_namespace_done / cancel_namespace), namespace
     * subscriptions (a locally-terminated ns_sub request bidi), track-status
     * requests (a locally-terminated accept/reject response bidi),
     * track-subscriptions (a locally-terminated SUBSCRIBE_TRACKS reject/teardown
     * response bidi), and publishes (a locally-terminated PUBLISH reject /
     * PUBLISH_DONE response bidi). */
    size_t drain_ref_cap = sub_cap + fetch_cap + ann_cap + ns_sub_cap + ts_cap +
                           track_sub_cap + pub_cap;
    size_t drain_ref_bytes = drain_ref_cap * sizeof(uint64_t);
    /* Parallel per-drain-ref reason byte (NORMAL absorb vs GOAWAY-received). */
    size_t off_drain_reason = off_drain_ref + drain_ref_bytes;
    size_t drain_reason_bytes = drain_ref_cap * sizeof(uint8_t);
    /* Parallel framing state of a GOAWAY-received drain ref. */
    size_t off_drain_scan = ALIGN_UP(off_drain_reason + drain_reason_bytes,
                                     _Alignof(moq_drain_scan_t));
    size_t drain_scan_bytes = drain_ref_cap * sizeof(moq_drain_scan_t);
    size_t idx_sub_cap = moq_index_cap_for(sub_cap * 2 + ann_cap + fetch_cap + pub_cap + ts_cap + ns_sub_cap + track_sub_cap);
    size_t idx_rx_cap  = moq_index_cap_for(rx_cap);
    size_t idx_ns_cap  = moq_index_cap_for(ns_sub_cap);
    const size_t idx_align = _Alignof(moq_index_entry_t);
    size_t off_idx_sub = ALIGN_UP(off_drain_scan + drain_scan_bytes, idx_align);
    size_t idx_sub_bytes = idx_sub_cap * sizeof(moq_index_entry_t);
    size_t off_idx_rx  = ALIGN_UP(off_idx_sub + idx_sub_bytes, idx_align);
    size_t idx_rx_bytes = idx_rx_cap * sizeof(moq_index_entry_t);
    size_t off_ns_sub  = ALIGN_UP(off_idx_rx + idx_rx_bytes, ns_sub_align);
    size_t ns_sub_bytes = ns_sub_cap * sizeof(moq_ns_sub_entry_t);
    size_t off_idx_ns  = ALIGN_UP(off_ns_sub + ns_sub_bytes, idx_align);
    size_t idx_ns_bytes = idx_ns_cap * sizeof(moq_index_entry_t);
    /* Stream-ref request index: sized like idx_req_by_rid (holds at most the
     * same set of requests, one stream_ref each). Draft-16 leaves it empty. */
    size_t idx_reqref_cap = idx_sub_cap;
    size_t off_idx_reqref = ALIGN_UP(off_idx_ns + idx_ns_bytes, idx_align);
    size_t idx_reqref_bytes = idx_reqref_cap * sizeof(moq_index_entry_t);
    /* Subscriber-role established subscription alias -> slot index (replaces the
     * linear s->subs scan in sub_find_by_alias_subscriber on the receive path). */
    size_t idx_sub_alias_cap = moq_index_cap_for(sub_cap);
    size_t off_idx_sub_alias = ALIGN_UP(off_idx_reqref + idx_reqref_bytes, idx_align);
    size_t idx_sub_alias_bytes = idx_sub_alias_cap * sizeof(moq_index_entry_t);
    /* The per-stream receive buffers that follow (ns_recv and the *_req_recv
     * groups) are sized as `count * ns_sub_recv_cap` byte products WITHOUT an
     * explicit division-overflow check, unlike the struct-array segments above.
     * That is safe by construction: every entry count is capped at <= 0xFFFF and
     * ns_sub_recv_cap is <= 4096 (both bounded above), so each product is
     * <= ~256 MiB and cannot overflow size_t on any target. The monotonic-order
     * checks in the cross-check block below also catch any additive wrap. */
    size_t off_ns_recv = off_idx_sub_alias + idx_sub_alias_bytes;
    size_t ns_recv_bytes = ns_sub_cap * ns_sub_recv_cap;
    /* Per-subscription request-stream receive buffers (stream-correlated
     * profiles parse request/response control messages off the request bidi
     * here). Same per-stream cap as namespace-sub buffers. */
    size_t sub_req_recv_cap = ns_sub_recv_cap;
    size_t off_sub_req_recv = off_ns_recv + ns_recv_bytes;
    size_t sub_req_recv_bytes = sub_cap * sub_req_recv_cap;
    /* Per-fetch request-bidi RESPONSE receive buffers (FETCH_OK/REQUEST_ERROR
     * on stream-correlated profiles). Same per-stream cap as the sub buffers;
     * separate from the FETCH data-uni path. */
    size_t fetch_req_recv_cap = ns_sub_recv_cap;
    size_t off_fetch_req_recv = off_sub_req_recv + sub_req_recv_bytes;
    size_t fetch_req_recv_bytes = fetch_cap * fetch_req_recv_cap;
    /* Per-announcement request-bidi receive buffers (stream-correlated profiles
     * parse the PUBLISH_NAMESPACE response/REQUEST_UPDATE off the announce bidi
     * here). Same per-stream cap as the sub buffers. */
    size_t ann_req_recv_cap = ns_sub_recv_cap;
    size_t off_ann_req_recv = off_fetch_req_recv + fetch_req_recv_bytes;
    size_t ann_req_recv_bytes = ann_cap * ann_req_recv_cap;
    /* Per-track-status request-bidi receive buffers (stream-correlated profiles
     * buffer the TRACK_STATUS_OK / REQUEST_ERROR response off the request bidi
     * here). Same per-stream cap as the sub buffers. */
    size_t ts_req_recv_cap = ns_sub_recv_cap;
    size_t off_ts_req_recv = off_ann_req_recv + ann_req_recv_bytes;
    size_t ts_req_recv_bytes = ts_cap * ts_req_recv_cap;
    /* SUBSCRIBE_TRACKS pool (draft-18 only): entry array + per-entry request-bidi
     * receive buffers (REQUEST_OK / REQUEST_ERROR / PUBLISH_BLOCKED stream). */
    size_t off_track_sub = ALIGN_UP(off_ts_req_recv + ts_req_recv_bytes,
                                    track_sub_align);
    size_t track_sub_bytes = track_sub_cap * sizeof(moq_track_sub_entry_t);
    size_t track_sub_recv_cap = ns_sub_recv_cap;
    size_t off_track_sub_recv = off_track_sub + track_sub_bytes;
    size_t track_sub_recv_bytes = track_sub_cap * track_sub_recv_cap;
    /* Per-publish request-bidi receive buffers (stream-correlated profiles buffer
     * the PUBLISH bidi's response/lifecycle messages here). Same per-stream cap
     * as the sub buffers. */
    size_t pub_req_recv_cap = ns_sub_recv_cap;
    size_t off_pub_req_recv = off_track_sub_recv + track_sub_recv_bytes;
    size_t pub_req_recv_bytes = pub_cap * pub_req_recv_cap;
    size_t off_scratch = ALIGN_UP(off_pub_req_recv + pub_req_recv_bytes,
                                    scratch_align);
    /* Event borrow arena: same budget as the transient scratch. */
    size_t off_evscratch = ALIGN_UP(off_scratch + scratch_cap,
                                    scratch_align);
    size_t prof_align  = profile->state_align > 0 ? profile->state_align : 1;
    size_t off_profile = ALIGN_UP(off_evscratch + scratch_cap, prof_align);
    /* No-owner admission carriers. Allocated in the session arena at
     * create time so installing one during ingress can NEVER fail on
     * allocation: a refused no-owner request bidi always has a durable,
     * discoverable retry identity, so the bridge never fatalizes a retainable
     * WOULD_BLOCK. Bounded by the same total request-owner lifecycle capacity
     * as the drain ring -- the carrier's own landing spot for a no-FIN refusal
     * -- rather than an arbitrary floor. */
    size_t off_noslot_carrier = ALIGN_UP(off_profile + profile->state_size,
                                         _Alignof(moq_noslot_carrier_t));
    size_t noslot_carrier_cap = drain_ref_cap;
    size_t noslot_carrier_bytes =
        noslot_carrier_cap * sizeof(moq_noslot_carrier_t);
    /* SETUP authorization tokens: the copied tokens, their encoded option
     * values, then the bytes both point into (the values, then each encoded
     * value with room for its two leading varints). */
    size_t off_setup_tokens = ALIGN_UP(off_noslot_carrier + noslot_carrier_bytes,
                                       _Alignof(moq_auth_token_t));
    size_t setup_tokens_bytes = setup_token_count * sizeof(moq_auth_token_t);
    size_t off_setup_wire = ALIGN_UP(off_setup_tokens + setup_tokens_bytes,
                                     _Alignof(moq_bytes_t));
    size_t setup_wire_bytes = setup_token_count * sizeof(moq_bytes_t);
    size_t off_setup_bytes = off_setup_wire + setup_wire_bytes;
    size_t setup_bytes_cap = setup_token_count
        ? 2 * setup_token_value_bytes + setup_token_count * 18u : 0;
    size_t total       = off_setup_bytes + setup_bytes_cap;

#undef ALIGN_UP

    /* Layout-discipline gate: reject any capacity or offset that would overflow
     * or break monotonicity BEFORE allocating. Struct-array segments verify the
     * `count * elem_size` multiplication (bytes/elem == cap); every segment
     * verifies its offset does not run backwards (an additive wrap); the total is
     * capped at 64 MiB. RULE: any NEW arena segment added above MUST also be
     * added here (its order check, plus a mult check if it is a struct array) --
     * otherwise the layout discipline is incomplete and a bad cap could silently
     * under-size a region. */
    if (act_bytes / sizeof(moq_action_t) != action_cap ||
        action_meta_bytes / sizeof(moq_action_meta_t) != action_cap ||
        off_action_meta < off_actions ||
        evt_bytes / sizeof(moq_event_t)  != event_cap  ||
        location_bytes / sizeof(moq_state_location_cell_t) != location_cap ||
        sub_bytes / sizeof(moq_sub_entry_t) != sub_cap ||
        ann_bytes / sizeof(moq_ann_entry_t) != ann_cap ||
        fetch_bytes / sizeof(moq_fetch_entry_t) != fetch_cap ||
        pub_bytes / sizeof(moq_pub_entry_t) != pub_cap ||
        ts_bytes  / sizeof(moq_ts_entry_t)  != ts_cap  ||
        sg_bytes  / sizeof(moq_sg_entry_t)  != sg_cap  ||
        rx_bytes  / sizeof(moq_rx_stream_t) != rx_cap  ||
        rx_fin_bytes / sizeof(uint64_t)    != rx_fin_cap ||
        rx_stopped_bytes / sizeof(uint64_t) != rx_stopped_cap ||
        unsub_tomb_bytes / sizeof(uint64_t) != unsub_tomb_cap ||
        fetch_cancel_tomb_bytes / sizeof(uint64_t) != fetch_cancel_tomb_cap ||
        fetch_abort_tomb_bytes / sizeof(uint64_t) != fetch_abort_tomb_cap ||
        drain_ref_bytes / sizeof(uint64_t) != drain_ref_cap ||
        drain_scan_bytes / sizeof(moq_drain_scan_t) != drain_ref_cap ||
        off_fetch_cancel_tomb < off_unsub_tomb ||
        off_fetch_abort_tomb < off_fetch_cancel_tomb ||
        off_drain_ref < off_fetch_abort_tomb ||
        ns_sub_bytes / sizeof(moq_ns_sub_entry_t) != ns_sub_cap ||
        track_sub_bytes / sizeof(moq_track_sub_entry_t) != track_sub_cap ||
        idx_reqref_bytes / sizeof(moq_index_entry_t) != idx_reqref_cap ||
        idx_sub_alias_bytes / sizeof(moq_index_entry_t) != idx_sub_alias_cap ||
        off_events < off_action_meta || off_locations < off_events ||
        off_send < off_locations ||
        off_recv < off_send || off_subs < off_recv + recv_cap ||
        off_ann < off_subs || off_fetch < off_ann || off_pub < off_fetch ||
        off_ts < off_pub ||
        off_sg < off_pub ||
        off_rx < off_sg ||
        off_rx_fin < off_rx || off_rx_stopped < off_rx_fin ||
        off_unsub_tomb < off_rx_stopped || off_ns_sub < off_idx_rx ||
        off_idx_reqref < off_idx_ns ||
        off_idx_sub_alias < off_idx_reqref ||
        off_ns_recv < off_idx_sub_alias ||
        off_sub_req_recv < off_ns_recv ||
        off_fetch_req_recv < off_sub_req_recv ||
        off_ann_req_recv < off_fetch_req_recv ||
        off_ts_req_recv < off_ann_req_recv ||
        off_track_sub < off_ts_req_recv ||
        off_track_sub_recv < off_track_sub ||
        off_pub_req_recv < off_track_sub_recv ||
        off_scratch < off_pub_req_recv ||
        off_evscratch < off_scratch ||
        off_profile < off_evscratch ||
        noslot_carrier_bytes / sizeof(moq_noslot_carrier_t) !=
            noslot_carrier_cap ||
        off_noslot_carrier < off_profile ||
        setup_tokens_bytes / sizeof(moq_auth_token_t) != setup_token_count ||
        setup_wire_bytes / sizeof(moq_bytes_t) != setup_token_count ||
        off_setup_tokens < off_noslot_carrier ||
        off_setup_wire < off_setup_tokens ||
        off_setup_bytes < off_setup_wire ||
        total < off_setup_bytes ||
        total > (size_t)64 * 1024 * 1024)
        return MOQ_ERR_INVAL;

    uint8_t *mem = (uint8_t *)cfg->alloc->alloc(total, cfg->alloc->ctx);
    if (!mem) return MOQ_ERR_NOMEM;

    memset(mem, 0, total);
    moq_session_t *s = (moq_session_t *)mem;
    s->alloc = *cfg->alloc;
    s->alloc_size = total;
    s->perspective = cfg->perspective;
    s->state = MOQ_SESS_IDLE;
    s->last_now_us = now_us;
    s->borrow_epoch = 1;

    s->actions    = (moq_action_t *)(mem + off_actions);
    s->action_meta = (moq_action_meta_t *)(mem + off_action_meta);
    s->action_cap = action_cap;
    s->events     = (moq_event_t *)(mem + off_events);
    s->event_cap  = event_cap;
    s->state_locations = location_cap ? (moq_state_location_cell_t *)(mem + off_locations) : NULL;
    s->state_location_cap = location_cap;
    s->state_location_free = 0;
    s->state_location_retired = location_cap;
    s->state_location_retired_tail = location_cap;
    for (size_t i = 0; i < location_cap; ++i)
        s->state_locations[i].next = i + 1;
    s->send_buf   = mem + off_send;
    s->send_cap   = send_cap;
    s->recv_buf   = mem + off_recv;
    s->recv_cap   = recv_cap;
    s->subs       = (moq_sub_entry_t *)(mem + off_subs);
    s->sub_cap    = sub_cap;
    /* No-slot admission carriers: arena-backed (never allocation-fails
     * during ingress), bounded by the total request-owner lifecycle capacity. */
    s->noslot_carriers      = (moq_noslot_carrier_t *)(mem + off_noslot_carrier);
    s->noslot_carrier_cap   = noslot_carrier_cap;
    s->noslot_carrier_count = 0;
    s->announcements = (moq_ann_entry_t *)(mem + off_ann);
    s->ann_cap    = ann_cap;
    s->fetches    = (moq_fetch_entry_t *)(mem + off_fetch);
    s->fetch_cap  = fetch_cap;
    s->publishes  = (moq_pub_entry_t *)(mem + off_pub);
    s->pub_cap    = pub_cap;
    s->track_statuses = (moq_ts_entry_t *)(mem + off_ts);
    s->ts_cap     = ts_cap;
    s->subgroups  = (moq_sg_entry_t *)(mem + off_sg);
    s->sg_cap     = sg_cap;
    s->next_stream_ref = 1;
    s->rx_streams      = (moq_rx_stream_t *)(mem + off_rx);
    s->rx_cap          = rx_cap;
    s->rx_finished     = (uint64_t *)(mem + off_rx_fin);
    s->rx_fin_cap      = rx_fin_cap;
    s->rx_stopped_refs = (uint64_t *)(mem + off_rx_stopped);
    s->rx_stopped_cap  = rx_stopped_cap;
    s->rx_stopped_count = 0;
    s->max_obj_payload = max_obj_payload;
    s->max_recv_buf = max_recv_buf;
    /* Reordering buffer for data that arrives before its SUBSCRIBE_OK: a
     * small fixed slot ring (lazily allocated on first use) whose held bytes
     * share the receive-buffer budget. */
    s->staged_cap        = MOQ_STAGED_DG_SLOTS;
    s->staged_bytes_cap  = max_recv_buf;
    s->unsub_tombstones   = (uint64_t *)(mem + off_unsub_tomb);
    s->unsub_tomb_cap     = unsub_tomb_cap;
    s->fetch_cancel_tombs    = (uint64_t *)(mem + off_fetch_cancel_tomb);
    s->fetch_cancel_tomb_cap = fetch_cancel_tomb_cap;
    s->fetch_abort_tombs     = (uint64_t *)(mem + off_fetch_abort_tomb);
    s->fetch_abort_tomb_cap  = fetch_abort_tomb_cap;
    s->drain_refs         = (uint64_t *)(mem + off_drain_ref);
    s->drain_ref_reasons  = (uint8_t *)(mem + off_drain_reason);
    s->drain_ref_scans    = (moq_drain_scan_t *)(mem + off_drain_scan);
    s->drain_ref_cap      = drain_ref_cap;
    s->idx_req_by_rid     = (moq_index_entry_t *)(mem + off_idx_sub);
    s->idx_req_mask       = idx_sub_cap - 1;
    s->idx_rx_by_ref      = (moq_index_entry_t *)(mem + off_idx_rx);
    s->idx_rx_mask        = idx_rx_cap - 1;
    for (size_t i = 0; i < idx_sub_cap; i++)
        s->idx_req_by_rid[i].slot = -1;
    for (size_t i = 0; i < idx_rx_cap; i++)
        s->idx_rx_by_ref[i].slot = -1;
    s->ns_subs        = (moq_ns_sub_entry_t *)(mem + off_ns_sub);
    s->ns_sub_cap     = ns_sub_cap;
    s->max_ns_suffixes = max_ns_suffixes;
    s->idx_ns_by_ref  = (moq_index_entry_t *)(mem + off_idx_ns);
    s->idx_ns_mask    = idx_ns_cap - 1;
    for (size_t i = 0; i < idx_ns_cap; i++)
        s->idx_ns_by_ref[i].slot = -1;
    s->idx_req_by_streamref     = (moq_index_entry_t *)(mem + off_idx_reqref);
    s->idx_req_streamref_mask   = idx_reqref_cap - 1;
    for (size_t i = 0; i < idx_reqref_cap; i++)
        s->idx_req_by_streamref[i].slot = -1;
    s->idx_sub_by_alias   = (moq_index_entry_t *)(mem + off_idx_sub_alias);
    s->idx_sub_alias_mask = idx_sub_alias_cap - 1;
    for (size_t i = 0; i < idx_sub_alias_cap; i++)
        s->idx_sub_by_alias[i].slot = -1;
    for (size_t i = 0; i < ns_sub_cap; i++) {
        s->ns_subs[i].recv_buf = mem + off_ns_recv + i * ns_sub_recv_cap;
        s->ns_subs[i].recv_cap = ns_sub_recv_cap;
    }
    for (size_t i = 0; i < sub_cap; i++) {
        s->subs[i].rs.recv_buf = mem + off_sub_req_recv + i * sub_req_recv_cap;
        s->subs[i].rs.recv_cap = sub_req_recv_cap;
    }
    for (size_t i = 0; i < fetch_cap; i++) {
        s->fetches[i].rs.recv_buf =
            mem + off_fetch_req_recv + i * fetch_req_recv_cap;
        s->fetches[i].rs.recv_cap = fetch_req_recv_cap;
    }
    for (size_t i = 0; i < ann_cap; i++) {
        s->announcements[i].rs.recv_buf =
            mem + off_ann_req_recv + i * ann_req_recv_cap;
        s->announcements[i].rs.recv_cap = ann_req_recv_cap;
    }
    for (size_t i = 0; i < ts_cap; i++) {
        s->track_statuses[i].rs.recv_buf =
            mem + off_ts_req_recv + i * ts_req_recv_cap;
        s->track_statuses[i].rs.recv_cap = ts_req_recv_cap;
    }
    s->track_subs     = (moq_track_sub_entry_t *)(mem + off_track_sub);
    s->track_sub_cap  = track_sub_cap;
    for (size_t i = 0; i < track_sub_cap; i++) {
        s->track_subs[i].rs.recv_buf =
            mem + off_track_sub_recv + i * track_sub_recv_cap;
        s->track_subs[i].rs.recv_cap = track_sub_recv_cap;
    }
    for (size_t i = 0; i < pub_cap; i++) {
        s->publishes[i].rs.recv_buf =
            mem + off_pub_req_recv + i * pub_req_recv_cap;
        s->publishes[i].rs.recv_cap = pub_req_recv_cap;
    }
    /*
     * The occupancy lists start EMPTY. -1 is written explicitly because the
     * session slab is zero-filled and slot 0 is a valid member, so a zeroed
     * head would name a slot that is not allocated.
     */
    s->pub_occ_head   = -1;
    s->sub_occ_head   = -1;
    s->sg_occ_head    = -1;
    s->rx_occ_head    = -1;
    s->fetch_occ_head = -1;
    for (size_t i = 0; i < pub_cap; i++)
        { s->publishes[i].occ_next = -1; s->publishes[i].occ_prev = -1; }
    for (size_t i = 0; i < sub_cap; i++)
        { s->subs[i].occ_next = -1; s->subs[i].occ_prev = -1; }
    for (size_t i = 0; i < sg_cap; i++)
        { s->subgroups[i].occ_next = -1; s->subgroups[i].occ_prev = -1; }
    for (size_t i = 0; i < rx_cap; i++)
        { s->rx_streams[i].occ_next = -1; s->rx_streams[i].occ_prev = -1; }
    for (size_t i = 0; i < fetch_cap; i++)
        { s->fetches[i].occ_next = -1; s->fetches[i].occ_prev = -1; }

    s->output_scratch     = mem + off_scratch;
    s->output_scratch_cap = scratch_cap;
    s->event_scratch      = mem + off_evscratch;
    s->event_scratch_cap  = scratch_cap;
    s->profile            = profile;
    s->profile_state      = mem + off_profile;
    profile->init_in_place(s->profile_state, cfg);

    if (setup_token_count > 0) {
        s->setup_tokens = (moq_auth_token_t *)(void *)(mem + off_setup_tokens);
        s->setup_token_wire = (moq_bytes_t *)(void *)(mem + off_setup_wire);
        s->setup_token_count = setup_token_count;
        uint8_t *bytes = mem + off_setup_bytes;
        for (size_t i = 0; i < setup_token_count; i++) {
            size_t len = setup_tokens_in[i].token_value.len;
            s->setup_tokens[i].token_type = setup_tokens_in[i].token_type;
            s->setup_tokens[i].token_value.data = len ? bytes : NULL;
            s->setup_tokens[i].token_value.len = len;
            if (len) memcpy(bytes, setup_tokens_in[i].token_value.data, len);
            bytes += len;
        }
        for (size_t i = 0; profile->encode_setup_token && i < setup_token_count; i++) {
            size_t cap = s->setup_tokens[i].token_value.len + 18u;
            moq_buf_writer_t tw;
            moq_buf_writer_init(&tw, bytes, cap);
            if (profile->encode_setup_token(&tw, s->setup_tokens[i].token_type,
                                            s->setup_tokens[i].token_value) < 0) {
                if (profile->destroy) profile->destroy(s->profile_state);
                session_wipe(mem, total);
                cfg->alloc->free(mem, total, cfg->alloc->ctx);
                return MOQ_ERR_INVAL;
            }
            s->setup_token_wire[i].data = bytes;
            s->setup_token_wire[i].len = moq_buf_writer_offset(&tw);
            bytes += cap;
        }
    }

    if (cfg->struct_size >= offsetof(moq_session_cfg_t, streaming_objects) +
        sizeof(cfg->streaming_objects))
        s->streaming_objects = cfg->streaming_objects;

    s->goaway_deadline_us = UINT64_MAX;
    s->subgroup_deadline_us = UINT64_MAX;
    s->subgroup_retry_deadline_us = UINT64_MAX;
    s->idle_deadline_us = UINT64_MAX;
    s->done_wait_timeout_us = done_wait_us;
    s->expiry_retry_deadline_us = UINT64_MAX;
    if (cfg->struct_size >= offsetof(moq_session_cfg_t, goaway_timeout_us) +
        sizeof(cfg->goaway_timeout_us))
        s->goaway_timeout_us = cfg->goaway_timeout_us;
    if (cfg->struct_size >= offsetof(moq_session_cfg_t, idle_timeout_us) +
        sizeof(cfg->idle_timeout_us))
        s->idle_timeout_us = cfg->idle_timeout_us;
    /* Arm the idle deadline from creation time so a session that is created
     * and then left silent (e.g. a server session whose peer never sends the
     * setup) still advertises a finite deadline and idle-closes on tick. A
     * disabled timeout (0) leaves idle_deadline_us at UINT64_MAX. */
    if (s->idle_timeout_us > 0)
        s->idle_deadline_us = deadline_add(now_us, s->idle_timeout_us);

    {
        uintptr_t addr = (uintptr_t)(void *)s;
        uint64_t h = (uint64_t)addr;
        h ^= h >> 16;
        h ^= h >> 32;
        h *= 0x9E3779B97F4A7C15ULL;
        h ^= now_us;
        h ^= (uint64_t)cfg->perspective << 8;
        h ^= h >> 33;
        s->session_tag = (uint16_t)((h & 0x7FFF) | 1);
    }

    s->send_auth_token_cache_size = send_auth_token_cache_size;
    s->auth_token_cache_size = auth_token_cache_size;
    s->defer_subscription_updates = defer_subscription_updates;
    s->defer_fetch_ranges = defer_fetch_ranges;
    s->cluster_enabled = cluster_enabled;
    s->cluster_hop_id = cluster_enabled ? cfg->cluster_hop_id : 0;
    s->cluster_has_relay_cost = cluster_enabled && cfg->cluster_has_relay_cost;
    s->cluster_relay_cost = s->cluster_has_relay_cost ? cfg->cluster_relay_cost : 1;
    s->cluster_peer_relay_cost = 1;
    {
        size_t cache_max = (size_t)auth_token_cache_size;
        size_t cache_entries;
        if (cache_max > 0) {
            cache_entries = cache_max / 16;
            if (cache_entries > 4096) cache_entries = 4096;
            if (cache_entries == 0) cache_entries = 1;
        } else {
            cache_entries = 1;
        }
        moq_result_t crc = moq_token_cache_init(&s->peer_token_cache,
            &s->alloc, cache_max, cache_entries);
        if (crc < 0) {
            /* The profile was already initialized in place (above), so its
             * destroy hook must run before the arena is freed -- otherwise a
             * profile that acquires resources in init_in_place would leak on
             * this late create-failure path (the normal teardown in
             * moq_session_destroy calls it, but that never runs here). */
            profile->destroy(s->profile_state);
            session_release_block(s, total);
            return crc;
        }
    }

    /* Track-largest history registry: separately allocated (kept out of the
     * single session block so its cap never disturbs the block layout). */
    s->th_cap = th_cap;
    if (th_cap > 0) {
        size_t th_bytes = th_cap * sizeof(moq_track_hist_t);
        s->track_hist = (moq_track_hist_t *)s->alloc.alloc(th_bytes, s->alloc.ctx);
        if (!s->track_hist) {
            moq_token_cache_free(&s->peer_token_cache);
            profile->destroy(s->profile_state);
            session_release_block(s, total);
            return MOQ_ERR_NOMEM;
        }
        memset(s->track_hist, 0, th_bytes);
    }

    {
        moq_result_t frc = fill_pool_init(s, fill_cap);
        if (frc < 0) {
            track_hist_destroy(s);
            moq_token_cache_free(&s->peer_token_cache);
            profile->destroy(s->profile_state);
            session_release_block(s, total);
            return frc;
        }
    }

    /* The SETUP carrying configured tokens is encoded once here, with every
     * other option this side sends, into a buffer as large as the SETUP
     * message length allows: a configuration whose SETUP cannot be encoded
     * is refused now rather than failing when the session starts. (Whether a
     * given send arena holds it is a separate, later question.) */
    if (s->setup_token_count > 0 && profile->encode_own_setup) {
        size_t cap = (size_t)UINT16_MAX + 16u;
        uint8_t *tmp = (uint8_t *)s->alloc.alloc(cap, s->alloc.ctx);
        if (!tmp) {
            moq_session_destroy(s);
            return MOQ_ERR_NOMEM;
        }
        moq_buf_writer_t w;
        moq_buf_writer_init(&w, tmp, cap);
        moq_result_t erc = profile->encode_own_setup(s, &w);
        session_wipe(tmp, cap);
        s->alloc.free(tmp, cap, s->alloc.ctx);
        if (erc < 0) {
            moq_session_destroy(s);
            return MOQ_ERR_INVAL;
        }
    }

    *out = s;
    return MOQ_OK;
}

void moq_session_destroy(moq_session_t *s)
{
    if (!s) return;
    moq_binding_destroy(s);
    if (s->peer_implementation)
        s->alloc.free(s->peer_implementation, s->peer_implementation_len,
                      s->alloc.ctx);
    s->peer_implementation = NULL;
    s->peer_implementation_len = 0;
    session_sweep_discard(s);
    if (s->profile && s->profile_state)
        s->profile->destroy(s->profile_state);
    decref_queued_data_payloads(s);
    decref_queued_event_payloads(s);
    free_rx_stream_bufs(s);
    free_staged_datagrams(s);
    track_hist_destroy(s);
    mt_destroy_all(s);
    fill_pool_destroy(s);
    moq_token_cache_free(&s->peer_token_cache);
    for (size_t i = 0; i < s->sub_cap; i++) {
        if (s->subs[i].track_id_buf)
            s->alloc.free(s->subs[i].track_id_buf, s->subs[i].track_id_len,
                          s->alloc.ctx);
        /* A deferred done still owns its reason copy (freed in sub_free_entry
         * on the normal path); release it on teardown. */
        if (s->subs[i].done_reason_buf)
            s->alloc.free(s->subs[i].done_reason_buf,
                          s->subs[i].done_reason_len, s->alloc.ctx);
        /* An update answer still owed holds the older covered ids. */
        session_owed_update_clear(s, &s->subs[i].owed_update);
    }
    /* Same for a publication: the reason copy of a gated done that never
     * finalized, and the ids of an owed update answer (the normal path frees
     * both in pub_free_entry). */
    for (size_t i = 0; i < s->pub_cap; i++) {
        if (s->publishes[i].done_reason_buf)
            s->alloc.free(s->publishes[i].done_reason_buf,
                          s->publishes[i].done_reason_len, s->alloc.ctx);
        session_owed_update_clear(s, &s->publishes[i].owed_update);
    }
    /* Track-status entries own a canonical track-name key that is
     * freed in ts_free_entry on the normal path; free any still-live one here on
     * session teardown (the reserved registry records are freed by
     * track_hist_destroy above). */
    for (size_t i = 0; i < s->ts_cap; i++) {
        if (s->track_statuses[i].track_id_buf)
            s->alloc.free(s->track_statuses[i].track_id_buf,
                          s->track_statuses[i].track_id_len, s->alloc.ctx);
    }
    for (size_t i = 0; i < s->ann_cap; i++) {
        if (s->announcements[i].ns_id_buf)
            s->alloc.free(s->announcements[i].ns_id_buf,
                          s->announcements[i].ns_id_len,
                          s->alloc.ctx);
        session_cluster_free(s, &s->announcements[i].cluster);
        ns_update_release(s, &s->announcements[i].update);
    }
    /* A buffered or released-but-unaccepted Joining FETCH (§10.12.2) owns deep-copied
     * auth-token values until the fetch entry is freed; release them on teardown. */
    for (size_t i = 0; i < s->fetch_cap; i++) {
        if (s->fetches[i].join_token_count > 0)
            process_auth_tokens_free_staging(s, s->fetches[i].join_tokens,
                s->fetches[i].join_token_staged, s->fetches[i].join_token_count);
        /* A fetcher-role entry owns its Track key (freed in fetch_free_entry
         * on the normal path). */
        if (s->fetches[i].track_key)
            s->alloc.free(s->fetches[i].track_key, s->fetches[i].track_key_len,
                          s->alloc.ctx);
    }
    ns_sub_destroy_all(s);
    track_sub_destroy_all(s);
    /* noslot_carriers is arena-backed (part of the session slab); no separate
     * free -- the slab free below reclaims it. A session that presented SETUP
     * tokens wipes the whole slab first: the copies, their encodings and any
     * SETUP still in the send arena hold the credential. */
    session_release_block(s, s->alloc_size);
}

moq_session_state_t moq_session_state(const moq_session_t *s)
{
    return s ? s->state : MOQ_SESS_CLOSED;
}

moq_perspective_t moq_session_perspective(const moq_session_t *s)
{
    return s ? s->perspective : (moq_perspective_t)0;
}

/* The profile is bound at create and never reassigned, so this is a pure
 * read of the single authority -- no second copy to drift from it. */
moq_version_t moq_session_version(const moq_session_t *s)
{
    return s ? s->profile->version : (moq_version_t)0;
}

moq_result_t moq_session_start(moq_session_t *s, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_input_t input;
    memset(&input, 0, sizeof(input));
    input.kind = MOQ_INPUT_START;
    input.now_us = now_us;
    return session_step(s, &input);
}

moq_result_t moq_session_on_control_bytes(moq_session_t *s,
                                           const uint8_t *buf,
                                           size_t len,
                                           uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_input_t input;
    memset(&input, 0, sizeof(input));
    input.kind = MOQ_INPUT_CONTROL_BYTES;
    input.now_us = now_us;
    input.u.control_bytes.buf = buf;
    input.u.control_bytes.len = len;
    return session_step(s, &input);
}

moq_result_t moq_session_process_pending(moq_session_t *s, uint64_t now_us)
{
    return moq_session_on_control_bytes(s, NULL, 0, now_us);
}

/* Draft-neutral inbound-GOAWAY handling shared by all profiles: copy the
 * (already-decoded) New Session URI, enter DRAINING and surface
 * MOQ_EVENT_GOAWAY. No drain deadline is armed on receipt: the GOAWAY_TIMEOUT
 * close belongs to the GOAWAY's sender, and a deadline this side armed by
 * sending its own GOAWAY is kept. The profile owns the wire decode and the pre-decode
 * active/duplicate checks; this owns the state transition and the event. A
 * server receiving a non-empty URI is a protocol violation (§10.4). */
moq_result_t session_core_on_goaway(moq_session_t *s,
                                    const uint8_t *uri, size_t uri_len,
                                    bool has_timeout, uint64_t timeout_ms)
{
    if (s->perspective == MOQ_PERSPECTIVE_SERVER && uri_len > 0)
        return close_with_error(s, 0x3, "GOAWAY with URI to server");

    if (event_queue_full(s))
        return MOQ_ERR_WOULD_BLOCK;

    /* A URI larger than the whole arena can never be delivered, so it ends the
     * session immediately rather than after a wasted drain cycle; one that only
     * the live tail cannot hold waits for the queue to drain. */
    switch (event_scratch_classify_bytes(s, uri_len)) {
    case MOQ_EVENT_SCRATCH_PERMANENT:
        return close_with_error(s, 0x1,
            "event scratch permanently too small for GOAWAY URI");
    case MOQ_EVENT_SCRATCH_BLOCKED:
        return MOQ_ERR_WOULD_BLOCK;
    case MOQ_EVENT_SCRATCH_FITS:
        break;
    }

    uint8_t *uri_copy = NULL;
    if (uri_len > 0) {
        uri_copy = event_scratch_copy(s, uri, uri_len);
        if (!uri_copy)
            return close_with_error(s, 0x1,
                "internal: event scratch preflight mismatch");
    }

    /* The receiver arms no drain deadline: GOAWAY_TIMEOUT is the sender's
     * close code (draft-22 §9.2: "the sender closes the session with
     * GOAWAY_TIMEOUT after the indicated timeout"; draft-18 §10.4 says the
     * same, draft-16 §3.5 "The server closes the session with a
     * GOAWAY_TIMEOUT if the client doesn't close the session quickly
     * enough"). goaway_timeout_us bounds only a GOAWAY this side sends. */
    s->goaway_received = true;
    if (s->state == MOQ_SESS_ESTABLISHED)
        s->state = MOQ_SESS_DRAINING;

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_GOAWAY;
    e.detail_size = (uint32_t)sizeof(moq_goaway_event_t);
    e.borrow_epoch = s->borrow_epoch;
    if (uri_copy) {
        e.u.goaway.new_session_uri.data = uri_copy;
        e.u.goaway.new_session_uri.len = uri_len;
    }
    e.u.goaway.has_timeout = has_timeout;
    e.u.goaway.timeout_ms = has_timeout ? timeout_ms : 0;
    return push_event(s, &e);
}

/* Simulate a REDIRECT event's whole scratch requirement from `from_cursor`,
 * region by region in the order the copy below performs them: the Connect URI
 * bytes, then (for a nonzero namespace count) the padding an aligned parts
 * array needs at that point, the array itself and each part's bytes in order,
 * then the Track Name bytes and finally the reason bytes. Only the namespace
 * array is alignment-sensitive, so the running cursor is tracked as far as its
 * padding and no further -- every later region is raw bytes.
 *
 * A zero-part namespace consumes nothing, not even padding, matching
 * event_scratch_copy_namespace: charging padding for an empty array would both
 * bill capacity for nothing and shift where the later regions land.
 *
 * Pure, and all capacity arithmetic is subtraction from the remaining space so
 * no sum can wrap. The count multiplication is overflow-checked BEFORE the
 * per-part walk, so an overflowing count is rejected without dereferencing the
 * parts pointer it describes. `from_cursor` never exceeds the cap in practice;
 * a value that does is treated as no room rather than wrapping. */
static bool redirect_event_scratch_requirement_fits(
    const moq_session_t *s, const moq_decoded_redirect_t *rd,
    size_t reason_len, size_t from_cursor)
{
    if (from_cursor > s->event_scratch_cap) return false;
    size_t remaining = s->event_scratch_cap - from_cursor;

    if (rd->connect_uri_len > remaining) return false;
    remaining -= rd->connect_uri_len;
    /* Bounded by the cap, so the sum cannot wrap. */
    size_t cursor = from_cursor + rd->connect_uri_len;

    if (rd->track_namespace.count > 0) {
        if (rd->track_namespace.count > SIZE_MAX / sizeof(moq_bytes_t))
            return false;
        const size_t align = _Alignof(moq_bytes_t);
        size_t pad = (align - (cursor & (align - 1))) & (align - 1);
        if (pad > remaining) return false;
        remaining -= pad;
        size_t array_bytes = rd->track_namespace.count * sizeof(moq_bytes_t);
        if (array_bytes > remaining) return false;
        remaining -= array_bytes;
        for (size_t i = 0; i < rd->track_namespace.count; i++) {
            size_t plen = rd->track_namespace.parts[i].len;
            if (plen > remaining) return false;
            remaining -= plen;
        }
    }

    if (rd->track_name_len > remaining) return false;
    remaining -= rd->track_name_len;
    return reason_len <= remaining;
}

/* Decide, before any copying, whether a REDIRECT event's four regions can be
 * staged now, later, or never. A requirement that cannot fit an EMPTY arena can
 * never be satisfied by draining events, so it is permanent and the caller
 * closes the session; one that fits an empty arena but not the live cursor is
 * transient, and becomes satisfiable once the queue drains and the arena
 * recycles. The two answers can differ by more than the occupied bytes, since
 * the namespace array's padding depends on where the earlier regions leave the
 * cursor. */
moq_event_scratch_verdict_t redirect_event_scratch_classify(
    const moq_session_t *s, const moq_decoded_redirect_t *rd, size_t reason_len)
{
    if (!redirect_event_scratch_requirement_fits(s, rd, reason_len, 0))
        return MOQ_EVENT_SCRATCH_PERMANENT;
    if (!redirect_event_scratch_requirement_fits(s, rd, reason_len,
                                                 s->event_scratch_len))
        return MOQ_EVENT_SCRATCH_BLOCKED;
    return MOQ_EVENT_SCRATCH_FITS;
}

moq_result_t session_core_emit_request_redirect(
    moq_session_t *s, moq_request_family_t family, uint64_t handle_opaque,
    const moq_decoded_redirect_t *rd, uint64_t error_code,
    bool can_retry, uint64_t retry_after_ms,
    const uint8_t *reason, size_t reason_len)
{
    if (event_queue_full(s)) return MOQ_ERR_WOULD_BLOCK;

    size_t scratch_saved = s->event_scratch_len;
    moq_bytes_t ev_uri = {0}, ev_name = {0}, ev_reason = {0};
    moq_namespace_t ev_ns = {0};

    switch (redirect_event_scratch_classify(s, rd, reason_len)) {
    case MOQ_EVENT_SCRATCH_PERMANENT:
        return close_with_error(s, 0x1,
            "event scratch permanently too small for REDIRECT");
    case MOQ_EVENT_SCRATCH_BLOCKED:
        return MOQ_ERR_WOULD_BLOCK;
    case MOQ_EVENT_SCRATCH_FITS:
        break;
    }

    if (rd->connect_uri_len > 0) {
        ev_uri.data = event_scratch_copy(s, rd->connect_uri, rd->connect_uri_len);
        ev_uri.len = rd->connect_uri_len;
        if (!ev_uri.data) goto preflight_mismatch;
    }
    if (!event_scratch_copy_namespace(s, &rd->track_namespace, &ev_ns))
        goto preflight_mismatch;
    if (rd->track_name_len > 0) {
        ev_name.data = event_scratch_copy(s, rd->track_name, rd->track_name_len);
        ev_name.len = rd->track_name_len;
        if (!ev_name.data) goto preflight_mismatch;
    }
    if (reason_len > 0) {
        ev_reason.data = event_scratch_copy(s, reason, reason_len);
        ev_reason.len = reason_len;
        if (!ev_reason.data) goto preflight_mismatch;
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_REQUEST_REDIRECT;
    e.detail_size = (uint32_t)sizeof(moq_request_redirect_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.request_redirect.family = family;
    e.u.request_redirect.handle.raw = handle_opaque;
    /* REDIRECT recognition already happened against the FULL raw value; only
     * the semantic code is normalized here. */
    e.u.request_redirect.error_code =
        s->profile->semantic_request_error(error_code);
    e.u.request_redirect.can_retry = can_retry;
    e.u.request_redirect.retry_after_ms = retry_after_ms;
    e.u.request_redirect.connect_uri = ev_uri;
    e.u.request_redirect.track_namespace = ev_ns;
    e.u.request_redirect.track_name = ev_name;
    e.u.request_redirect.reason = ev_reason;

    moq_result_t rc = push_event(s, &e);
    if (rc < 0) {
        s->event_scratch_len = scratch_saved;
        return rc;
    }
    return MOQ_OK;

preflight_mismatch:
    /* The classifier said the whole composite fits: a copy failing here is an
     * internal inconsistency with no unblock edge to wait on. */
    s->event_scratch_len = scratch_saved;
    return close_with_error(s, 0x1,
        "internal: event scratch preflight mismatch");
}

moq_result_t moq_session_goaway(moq_session_t *s,
                                 const uint8_t *uri,
                                 size_t uri_len,
                                 uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    session_begin_advance(s, now_us);
    if (!session_is_active(s)) return MOQ_ERR_WRONG_STATE;
    if (s->goaway_sent) return MOQ_ERR_WRONG_STATE;
    if (s->perspective == MOQ_PERSPECTIVE_CLIENT && uri_len > 0)
        return MOQ_ERR_INVAL;

    moq_buf_writer_t w;
    moq_buf_writer_init(&w, s->send_buf + s->send_len,
                         s->send_cap - s->send_len);
    moq_goaway_encode_args_t ga = { .uri = uri, .uri_len = uri_len };
    moq_result_t rc = s->profile->encode_goaway(s, &w, &ga);
    if (rc < 0) {
        /* Argument validation precedes queue refusal; a valid message that
         * cannot fit still observes the original queue-before-capacity order. */
        return rc == MOQ_ERR_BUFFER && action_queue_full(s) ? MOQ_ERR_WOULD_BLOCK : rc;
    }

    rc = commit_send_control_tail(s, moq_buf_writer_offset(&w));
    if (rc < 0) return rc;

    s->goaway_sent = true;
    if (s->state == MOQ_SESS_ESTABLISHED)
        s->state = MOQ_SESS_DRAINING;
    if (s->goaway_timeout_us > 0 && s->goaway_deadline_us == UINT64_MAX)
        s->goaway_deadline_us = deadline_add(now_us, s->goaway_timeout_us);
    return MOQ_OK;
}

moq_result_t moq_session_tick(moq_session_t *s, uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_input_t input;
    memset(&input, 0, sizeof(input));
    input.kind = MOQ_INPUT_TICK;
    input.now_us = now_us;
    return session_step(s, &input);
}

bool moq_session_borrow_valid(const moq_session_t *s, uint64_t borrow_epoch)
{
    if (!s) return false;
    return s->borrow_epoch == borrow_epoch;
}

moq_result_t moq_session_on_datagram(moq_session_t *s,
                                      const uint8_t *data, size_t len,
                                      uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (len > 0 && !data) return MOQ_ERR_INVAL;
    moq_input_t input;
    memset(&input, 0, sizeof(input));
    input.kind = MOQ_INPUT_DATAGRAM;
    input.now_us = now_us;
    input.u.datagram.buf = data;
    input.u.datagram.len = len;
    return session_step(s, &input);
}

moq_result_t moq_session_close(moq_session_t *s,
                                uint64_t code,
                                const char *reason,
                                uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    /* Idempotent: an already-CLOSED session keeps whatever close outputs
     * are still queued (original code/reason) -- close_with_error would
     * clear and replace them. */
    if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;

    session_begin_advance(s, now_us);
    return close_with_error(s, code, reason);
}

moq_result_t moq_session_on_transport_close(moq_session_t *s,
                                              uint64_t code,
                                              uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (s->state == MOQ_SESS_CLOSED) return MOQ_OK;

    session_begin_advance(s, now_us);

    s->state = MOQ_SESS_CLOSED;
    s->goaway_deadline_us = UINT64_MAX;
    s->subgroup_deadline_us = UINT64_MAX;
    s->subgroup_retry_deadline_us = UINT64_MAX;
    s->idle_deadline_us = UINT64_MAX;
    s->expiry_retry_deadline_us = UINT64_MAX;
    decref_queued_data_payloads(s);
    decref_queued_event_payloads(s);
    free_rx_stream_bufs(s);
    s->action_head = s->action_tail = 0;
    s->action_timed = s->action_nondata = 0;
    s->send_len = 0;
    s->event_head = s->event_tail = 0;

    for (int32_t i = s->sg_occ_head, nxt; i >= 0; i = nxt) {
        nxt = s->subgroups[i].occ_next;
        if (s->subgroups[i].state != MOQ_SG_FREE)
            sg_free_entry(s, (size_t)i);
    }

    moq_event_t e;
    memset(&e, 0, sizeof(e));
    e.kind = MOQ_EVENT_SESSION_CLOSED;
    e.detail_size = (uint32_t)sizeof(moq_session_closed_event_t);
    e.borrow_epoch = s->borrow_epoch;
    e.u.closed.code = code;
    /* Same enqueued-fact rule as close_with_error: record it only when the
     * event is actually queued. This path reaches terminal from the transport
     * side, so it may run BEFORE or AFTER a local close enqueued the event --
     * the fact is monotonic and idempotent either way. */
    if (push_event(s, &e) == MOQ_OK)
        s->terminal_enqueued = true;
    return MOQ_OK;
}

moq_result_t moq_session_on_data_bytes(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       const uint8_t *buf,
                                       size_t len,
                                       bool fin,
                                       uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (len > 0 && !buf) return MOQ_ERR_INVAL;
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_data_bytes(s, stream_ref, buf, len, fin);
}

moq_result_t moq_session_on_data_rcbuf(moq_session_t *s,
                                       moq_stream_ref_t stream_ref,
                                       moq_rcbuf_t *data,
                                       bool fin,
                                       uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (moq_binding_required_unbound(s)) return MOQ_ERR_WRONG_STATE;
    session_begin_advance(s, now_us);
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_data_bytes_rcbuf(s, stream_ref, data, fin);
}

moq_result_t moq_session_on_data_reset(moq_session_t *s,
                                        moq_stream_ref_t stream_ref,
                                        uint64_t error_code,
                                        uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_data_reset(s, error_code, stream_ref);
}

moq_result_t moq_session_on_bidi_stream_bytes(moq_session_t *s,
                                               moq_stream_ref_t stream_ref,
                                               const uint8_t *buf,
                                               size_t len,
                                               bool fin,
                                               uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    if (len > 0 && !buf) return MOQ_ERR_INVAL;
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_bidi_stream_bytes(s, stream_ref, buf, len, fin);
}

moq_result_t moq_session_on_bidi_stream_reset(moq_session_t *s,
                                               moq_stream_ref_t stream_ref,
                                               uint64_t error_code,
                                               uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_bidi_stream_reset(s, stream_ref, error_code);
}

moq_result_t moq_session_on_bidi_stream_stop(moq_session_t *s,
                                              moq_stream_ref_t stream_ref,
                                              uint64_t error_code,
                                              uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    if (session_idle_expired(s))
        return close_with_error(s, MOQ_CLOSE_IDLE_TIMEOUT, "idle timeout");
    session_refresh_idle(s, now_us);
    return handle_bidi_stream_stop(s, stream_ref, error_code);
}

/* Ownership other than a locally-ended request's drain identity. The public
 * query includes that identity; local retirement must check without it. */
static bool session_has_live_transport_owner(const moq_session_t *s,
                                              moq_stream_ref_t ref)
{
    if (!s) return false;
    if (moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask, ref._v) >= 0)
        return true;
    /* A data stream STOPped without a receive entry: no entry, but
     * its identity is retained and its remaining bytes are consumed here
     * until the peer's FIN or RESET, so the bridge must keep delivering. */
    if (rx_stopped_ref_find(s, ref._v) >= 0)
        return true;
    if (moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v) >= 0)
        return true;
    /* A stream-correlated (draft-18) request bidi whose staging/response owner
     * is keyed by stream ref: recognised through the request registry's
     * by-streamref lookup, which returns the packed endpoint the registry holds
     * for that ref. Without this, a retained request-stream WOULD_BLOCK is
     * fatalized by the bridge. The lookup reports whatever ownership the
     * registry carries for the ref; it does not independently revalidate a
     * stale slot (an ordinal-0 request id shared across owners is not
     * disambiguated here). */
    if (request_registry_find_by_streamref(s, ref).kind != MOQ_REQ_NONE)
        return true;
    /* A no-slot admission carrier: the request has no owner yet, but a
     * fixed terminal is durably owed on this stream, so the bridge must retain
     * its retry rather than fatalize it. */
    if (noslot_carrier_find(s, ref) >= 0)
        return true;
    return false;
}

bool moq_session_has_transport_stream(const moq_session_t *s, moq_stream_ref_t ref)
{
    /* A locally-terminated request bidi keeps its drain identity until a
     * genuine terminal ends what it must absorb. Preserve the public query. */
    return s && (session_has_live_transport_owner(s, ref) ||
                 drain_ref_contains(s, ref));
}

static bool session_has_queued_bidi_action(const moq_session_t *s,
                                           moq_stream_ref_t ref)
{
    for (size_t i = s->action_head; i < s->action_tail; ++i) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        moq_stream_ref_t r = { 0 };
        switch (a->kind) {
        case MOQ_ACTION_OPEN_BIDI_STREAM:  r = a->u.open_bidi_stream.stream_ref; break;
        case MOQ_ACTION_SEND_BIDI_STREAM:  r = a->u.send_bidi_stream.stream_ref; break;
        case MOQ_ACTION_CLOSE_BIDI_STREAM: r = a->u.close_bidi_stream.stream_ref; break;
        case MOQ_ACTION_RESET_BIDI_STREAM: r = a->u.reset_bidi_stream.stream_ref; break;
        case MOQ_ACTION_STOP_BIDI_STREAM:  r = a->u.stop_bidi_stream.stream_ref; break;
        case MOQ_ACTION_ABORT_BIDI_STREAM: r = a->u.abort_bidi_stream.stream_ref; break;
        default: continue;
        }
        if (r._v == ref._v) return true;
    }
    return false;
}

bool bridge_session_bidi_owner_ended(const moq_session_t *s, moq_stream_ref_t ref)
{
    return !moq_session_has_transport_stream(s, ref) &&
           !session_has_queued_bidi_action(s, ref);
}

moq_result_t bridge_session_on_bidi_retired(moq_session_t *s,
                                            moq_stream_ref_t ref,
                                            uint64_t now_us)
{
    if (!s || ref._v == 0) return MOQ_ERR_INVAL;
    moq_result_t arc = session_advance_entry(s, now_us);
    if (arc != MOQ_OK) return arc;
    /* A cancelled request identity can predate SETUP completion. Only the
     * terminal session state forbids settling it; live owners still guard it. */
    if (s->state == MOQ_SESS_CLOSED) return MOQ_ERR_CLOSED;
    if (session_has_live_transport_owner(s, ref) ||
        session_has_queued_bidi_action(s, ref))
        return MOQ_ERR_WRONG_STATE;
    /* The bridge proved local abort completion plus a genuine terminal.
     * Release only its drain identity: no peer lifetime fact or event. */
    return drain_ref_remove(s, ref) ? MOQ_OK : MOQ_ERR_STALE_HANDLE;
}

bool moq_session_can_admit_data_stream(const moq_session_t *s)
{
    if (!s) return false;
    /* A terminal session takes the input to report its outcome. */
    if (!session_is_active(s)) return true;
    for (size_t i = 0; i < s->rx_cap; i++)
        if (!s->rx_streams[i].active) return true;
    return false;
}

/* The bridge's narrower question for the local-closed retirement decision: is
 * the PEER's terminal still owed here? This excludes a live request-registry
 * owner in general -- a local-origin request bidi the app itself FIN'd must
 * retire even while its owner lingers awaiting a response -- but keeps the
 * peer-origin receive paths, the two peer-terminal-awaiting signals (a no-slot
 * admission carrier and a drain-ring entry), and the one request whose
 * response is always owed on the peer's half after our FIN: TRACK_STATUS opens
 * its stream with FIN as its first and only message, and TRACK_STATUS_OK or
 * REQUEST_ERROR then arrives on that same stream (draft-18 §10.14). The
 * requester entry lives exactly until the peer's FIN or RESET ends that half. */
bool moq_session_stream_awaits_peer_terminal(const moq_session_t *s,
                                             moq_stream_ref_t ref)
{
    if (!s) return false;
    if (moq_index_find(s->idx_rx_by_ref, s->idx_rx_mask, ref._v) >= 0)
        return true;
    /* A data stream STOPped without a receive entry: no entry, but
     * its identity is retained and its remaining bytes are consumed here
     * until the peer's FIN or RESET, so the bridge must keep delivering. */
    if (rx_stopped_ref_find(s, ref._v) >= 0)
        return true;
    if (moq_index_find(s->idx_ns_by_ref, s->idx_ns_mask, ref._v) >= 0)
        return true;
    if (noslot_carrier_find(s, ref) >= 0)
        return true;
    if (drain_ref_contains(s, ref))
        return true;
    moq_request_endpoint_t ep = request_registry_find_by_streamref(s, ref);
    if (s->profile->premature_fin_fails_request) {
        if (ep.kind == MOQ_REQ_TRACK_STATUS) {
            const moq_request_lifetime_t *life = &s->track_statuses[ep.slot].rs.lifetime;
            return !life->peer_fin && !life->peer_reset;
        }
        if (ep.kind == MOQ_REQ_ANNOUNCEMENT) {
            const moq_request_lifetime_t *life = &s->announcements[ep.slot].rs.lifetime;
            return !life->peer_fin && !life->peer_reset;
        }
    }
    if (ep.kind == MOQ_REQ_TRACK_STATUS &&
        s->track_statuses[ep.slot].role == MOQ_TS_ROLE_REQUESTER)
        return true;
    return false;
}

/* -- Poll + observation -------------------------------------------- */

/* Frozen SEND_DATA poll floor: optional scheduling metadata must not make
 * legacy callers unable to receive and release their owned payload. */
typedef struct {
    moq_stream_ref_t stream_ref;
    uint8_t header[32];
    uint8_t header_len;
    moq_rcbuf_t *payload;
    bool fin;
} send_data_action_v0_t;
_Static_assert(offsetof(moq_send_data_action_t, scheduling_priority) >=
               sizeof(send_data_action_v0_t), "SEND_DATA v0 prefix changed");

static void session_action_poll_prepare(moq_session_t *s)
{
    if (session_is_active(s)) session_expire_queued_data(s);
    ts_retry_pending(s);
    ann_retry_pending(s);
    ns_sub_retry_pending(s);
}

static void session_action_poll_settled(moq_session_t *s, bool released)
{
    if (released && session_is_active(s)) {
        sg_retry_owed_resets(s);
        session_retry_owed_stream_actions(s);
        fill_service(s);
        mt_retry_pending(s);
    }
    ts_retry_pending(s);
    ann_retry_pending(s);
    ns_sub_retry_pending(s);
}

bool bridge_session_offer_data_reset(moq_session_t *s,
                                     moq_reset_data_action_t *out)
{
    /* Passive: expiry/retry belongs to service, and event admission belongs
     * to claim after the bridge has checked its pressure/prefix gates. */
    uint64_t done_owner = 0;
    /* The first non-data action decides; a ring of data alone has none. */
    for (size_t i = s->action_head;
         s->action_nondata != 0 && i < s->action_tail; ++i) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_reset_offer++;
#endif
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (action_is_data(a)) continue;
        if (a->kind != MOQ_ACTION_RESET_DATA) {
            done_owner = s->action_meta[i % s->action_cap].terminal_owner;
            if (done_owner == 0) return false;
            break;
        }
        memset(out, 0, sizeof(*out));
        out->stream_ref = a->u.reset_data.stream_ref;
        out->error_code = a->u.reset_data.error_code;
        if (a->detail_size >= offsetof(moq_reset_data_action_t, reliable_size) +
                              sizeof(out->reliable_size))
            out->reliable_size = a->u.reset_data.reliable_size;
        return true;
    }
    /* A full ring can leave a reset on its subgroup. A terminal may precede
     * its own DONE; every other non-data ordering barrier remains intact. */
    bool owed = false;
    for (int32_t i = s->sg_occ_head; s->sg_reset_owed && i >= 0;
         i = s->subgroups[i].occ_next) {
        const moq_sg_entry_t *sg = &s->subgroups[i];
        bool terminal = sg->terminal_cause != MOQ_OUTGOING_SUBGROUP_TERMINAL_NONE;
        if (sg->state != MOQ_SG_RESET_OWED && sg->state != MOQ_SG_TERMINAL_OWED)
            continue;
        owed = true;
        if (terminal && !sg_terminal_reset_ready(s, sg)) continue;
        if (done_owner && (!terminal ||
            (sg->sub._opaque != done_owner && sg->pub._opaque != done_owner)))
            continue;
        memset(out, 0, sizeof(*out));
        out->stream_ref = sg->stream_ref;
        out->error_code = terminal ? sg->terminal_code : sg->owed_reset_code;
        out->reliable_size = sg->header_len;
        return true;
    }
    /* An ineligible debt still needs retries: event/prefix pressure or an
     * unrelated owner's DONE can hide it from this passive offer. */
    s->sg_reset_owed = owed;
    return false;
}

bool bridge_session_claim_data_reset(moq_session_t *s,
                                      const moq_reset_data_action_t *reset)
{
    moq_reset_data_action_t offered;
    if (!bridge_session_offer_data_reset(s, &offered) ||
        offered.stream_ref._v != reset->stream_ref._v ||
        offered.error_code != reset->error_code ||
        offered.reliable_size != reset->reliable_size)
        return false;
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
        const moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->stream_ref._v == reset->stream_ref._v &&
            sg->state == MOQ_SG_TERMINAL_OWED)
            return sg_claim_terminal_reset(s, (size_t)i);
    }
    /* A queued reset or RESET_OWED already has local ownership, with any
     * outgoing-terminal event committed before this claim. */
    return true;
}

void bridge_session_discard_reset_data(moq_session_t *s, moq_stream_ref_t ref)
{
    size_t dst = s->action_head;
    const size_t tail = s->action_tail;
    for (size_t src = s->action_head; src < tail; ++src) {
        const size_t from = src % s->action_cap;
        moq_action_t *a = &s->actions[from];
        moq_stream_ref_t data_ref;
        if (action_data_stream(a, &data_ref) && data_ref._v == ref._v) {
            session_action_dequeued(s, a, &s->action_meta[from]);
            moq_action_cleanup(a);
            continue;
        }
        const size_t to = dst++ % s->action_cap;
        if (to != from) {
            s->actions[to] = *a;
            s->action_meta[to] = s->action_meta[from];
        }
    }
    s->action_tail = dst;
    for (; dst < tail; ++dst) {
        memset(&s->actions[dst % s->action_cap], 0, sizeof(*s->actions));
        memset(&s->action_meta[dst % s->action_cap], 0, sizeof(*s->action_meta));
    }
}

bool bridge_session_commit_data_reset(moq_session_t *s,
                                      const moq_reset_data_action_t *reset)
{
    for (size_t i = s->action_head; i < s->action_tail; ++i) {
        const moq_action_t *a = &s->actions[i % s->action_cap];
        if (a->kind != MOQ_ACTION_RESET_DATA ||
            a->u.reset_data.stream_ref._v != reset->stream_ref._v ||
            a->u.reset_data.error_code != reset->error_code)
            continue;
        moq_action_t taken;
        action_take(s, i, &taken);
        moq_action_cleanup(&taken);
        session_action_poll_settled(s, true);
        return true;
    }
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
        moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->state != MOQ_SG_RESET_OWED ||
            sg->stream_ref._v != reset->stream_ref._v ||
            sg->owed_reset_code != reset->error_code)
            continue;
        sg->state = MOQ_SG_RESETTING;
        session_action_poll_settled(s, true);
        return true;
    }
    return false;
}

static bool session_take_terminal_action(moq_session_t *s, moq_action_t *out,
                                          moq_action_meta_t *meta)
{
    /* A retained terminal with no queued data can precede its owner's DONE
     * even when that DONE occupies the last action slot. Commit its event
     * with the reset, just as the public poll does. */
    if (!session_is_active(s) || !sg_take_terminal_action(s, out)) return false;
    memset(meta, 0, sizeof(*meta));
    meta->deadline_us = UINT64_MAX;
    meta->outgoing_terminal = true;
    int slot = sg_find_by_stream_ref(s, out->u.reset_data.stream_ref);
    if (slot >= 0) {
        /* The owner itself may already have retired after queuing DONE.
         * Keep its full handle from the subgroup, without resolving it. */
        const moq_sg_entry_t *sg = &s->subgroups[slot];
        meta->owner = moq_subscription_is_valid(sg->sub)
            ? sg->sub._opaque : sg->pub._opaque;
    }
    session_action_poll_settled(s, true);
    return true;
}

static bool bridge_session_poll_action_impl(moq_session_t *s, moq_action_t *out,
                                            moq_action_meta_t *meta,
                                            bool data_only)
{
    session_action_poll_prepare(s);
    if (!data_only && session_take_terminal_action(s, out, meta))
        return true;
    if (s->action_head == s->action_tail ||
        (data_only && !action_is_data(&s->actions[s->action_head % s->action_cap]))) {
        if (!data_only) session_action_poll_settled(s, false);
        return false;
    }
    const size_t selected = action_select(s);
    if (s->action_meta[selected % s->action_cap].terminal_barrier ||
        action_waits_for_terminal(s, selected))
        return false;
    *meta = s->action_meta[selected % s->action_cap];
    action_take(s, selected, out);
    out->borrow_epoch = s->borrow_epoch;
    session_action_poll_settled(s, true);
    return true;
}

bool bridge_session_poll_leading_data(moq_session_t *s, moq_action_t *out,
                                     moq_action_meta_t *meta)
{
    return bridge_session_poll_action_impl(s, out, meta, true);
}

bool bridge_session_poll_action(moq_session_t *s, moq_action_t *out,
                                moq_action_meta_t *meta)
{
    return bridge_session_poll_action_impl(s, out, meta, false);
}

static bool action_follows_done(const moq_action_t *done, const moq_action_t *a)
{
    if (done->kind == MOQ_ACTION_SEND_CONTROL)
        return a->kind == MOQ_ACTION_SEND_CONTROL;
    const moq_stream_ref_t ref = done->u.send_bidi_stream.stream_ref;
    switch (a->kind) {
    case MOQ_ACTION_SEND_BIDI_STREAM:
        return a->u.send_bidi_stream.stream_ref._v == ref._v;
    case MOQ_ACTION_CLOSE_BIDI_STREAM:
        return a->u.close_bidi_stream.stream_ref._v == ref._v;
    case MOQ_ACTION_RESET_BIDI_STREAM:
        return a->u.reset_bidi_stream.stream_ref._v == ref._v;
    case MOQ_ACTION_STOP_BIDI_STREAM:
        return a->u.stop_bidi_stream.stream_ref._v == ref._v;
    case MOQ_ACTION_ABORT_BIDI_STREAM:
        return a->u.abort_bidi_stream.stream_ref._v == ref._v;
    default:
        return false;
    }
}

bool bridge_session_poll_ready_action(moq_session_t *s, moq_action_t *out,
    moq_action_meta_t *meta, bridge_done_ready_fn ready, void *ctx)
{
    session_action_poll_prepare(s);
    if (session_take_terminal_action(s, out, meta)) return true;
    for (size_t i = s->action_head; i < s->action_tail; ++i) {
        const size_t at = i % s->action_cap;
        const uint64_t owner = s->action_meta[at].terminal_owner;
        if (owner && (action_waits_for_terminal(s, i) || !ready(ctx, owner)))
            continue;
        bool blocked = false;
        for (size_t j = s->action_head; j < i; ++j) {
            const size_t prior = j % s->action_cap;
            if (s->action_meta[prior].terminal_owner &&
                action_follows_done(&s->actions[prior], &s->actions[at])) {
                blocked = true;
                break;
            }
        }
        if (blocked) continue;
        const size_t selected = action_select_from(s, i);
        if (s->action_meta[selected % s->action_cap].terminal_barrier)
            return false;
        *meta = s->action_meta[selected % s->action_cap];
        action_take(s, selected, out);
        out->borrow_epoch = s->borrow_epoch;
        session_action_poll_settled(s, true);
        return true;
    }
    session_action_poll_settled(s, false);
    return false;
}

/* The queue index of the action a mapped poll takes from the actions in
 * [head, end), or the tail when none qualifies. The gate only inspects. */
static size_t mapped_select(const moq_session_t *s, size_t end,
                            bridge_action_gate_fn gate, void *ctx)
{
    size_t best = s->action_tail;
    for (size_t i = s->action_head; i < end; ++i) {
        const size_t at = i % s->action_cap;
        const moq_action_t *a = &s->actions[at];
        const moq_action_meta_t *m = &s->action_meta[at];
        if (m->terminal_barrier || m->terminal_owner ||
            action_waits_for_terminal(s, i)) break;
        if (a->kind == MOQ_ACTION_CLOSE_SESSION) {
            /* close_with_error discards the core backlog before queuing this.
             * Supersede bridge storage, not an intervening core barrier. */
            if (i == s->action_head) best = i;
            break;
        }
        if (!action_is_data(a)) {
            if (gate(ctx, a, m, false) == BRIDGE_ACTION_SKIP) continue;
            /* A request's open can still be core-owned. Its following bytes
             * and FIN have the same map dependency; no other bidi terminal
             * or connection action is allowed through this exception. */
            moq_stream_ref_t ref = moq_stream_ref_from_u64(0);
            if (a->kind == MOQ_ACTION_SEND_BIDI_STREAM)
                ref = a->u.send_bidi_stream.stream_ref;
            else if (a->kind == MOQ_ACTION_CLOSE_BIDI_STREAM)
                ref = a->u.close_bidi_stream.stream_ref;
            bool blocked_open = false;
            for (size_t j = s->action_head; ref._v && j < i; ++j) {
                const size_t prior = j % s->action_cap;
                const moq_action_t *open = &s->actions[prior];
                if (open->kind == MOQ_ACTION_OPEN_BIDI_STREAM &&
                    open->u.open_bidi_stream.stream_ref._v == ref._v &&
                    gate(ctx, open, &s->action_meta[prior], false) ==
                        BRIDGE_ACTION_SKIP) {
                    blocked_open = true;
                    break;
                }
            }
            if (blocked_open) continue;
            break;
        }
        if (a->kind != MOQ_ACTION_SEND_DATA) continue;
        bool later = false;
        for (size_t j = s->action_head; j < i; ++j) {
            moq_stream_ref_t ref;
            if (action_data_stream(&s->actions[j % s->action_cap], &ref) &&
                ref._v == a->u.send_data.stream_ref._v) {
                later = true;
                break;
            }
        }
        if (later || gate(ctx, a, m, false) != BRIDGE_ACTION_READY) continue;
        if (best == s->action_tail ||
            sched_compare(m, &s->action_meta[best % s->action_cap]) < 0)
            best = i;
    }
    return best;
}

bool bridge_session_poll_mapped_action(moq_session_t *s, moq_action_t *out,
    moq_action_meta_t *meta, bool data_open, bridge_action_gate_fn gate,
    void *ctx)
{
    session_action_poll_prepare(s);
    /* Without admissible DATA nothing past the head can be selected, and
     * the gate's inspecting calls change nothing, so the head decides. */
    const size_t end = data_open || s->action_head == s->action_tail
        ? s->action_tail : s->action_head + 1;
    const size_t best = mapped_select(s, end, gate, ctx);
#ifdef MOQ_SESSION_SWEEP_TESTING
    /* Test builds hold the head-only selection to the full scan. */
    if (end != s->action_tail &&
        mapped_select(s, s->action_tail, gate, ctx) != best)
        abort();
#endif
    if (best == s->action_tail) return false;
    const size_t at = best % s->action_cap;
    if (gate(ctx, &s->actions[at], &s->action_meta[at], true) !=
        BRIDGE_ACTION_READY) return false;
    *meta = s->action_meta[at];
    action_take(s, best, out);
    out->borrow_epoch = s->borrow_epoch;
    session_action_poll_settled(s, true);
    return true;
}

moq_result_t moq_session_poll_actions_ex(moq_session_t *s,
                                          void *out, size_t cap,
                                          size_t element_size,
                                          size_t *out_count)
{
    if (out_count) *out_count = 0;
    if (!s || !out || !out_count) return MOQ_ERR_INVAL;
    if (cap == 0) return MOQ_OK;

    const size_t prefix = offsetof(moq_action_t, u);
    if (element_size < prefix) return MOQ_ERR_INVAL;

    const size_t lib_size = sizeof(moq_action_t);
    const size_t copy = lib_size < element_size ? lib_size : element_size;
    const size_t send_data_needed = prefix + sizeof(send_data_action_v0_t);

    size_t n = 0;
    bool terminal_blocked = false;
    uint8_t *dst = (uint8_t *)out;

    /* Handing an action to the adapter is the handover to the transport: an
     * object whose OBJECT_DELIVERY_TIMEOUT has elapsed must not cross it. */
    session_action_poll_prepare(s);

    while (n < cap && s->action_head < s->action_tail) {
        moq_action_t taken;
        if (!session_is_active(s) || !sg_take_terminal_action(s, &taken)) {
            size_t idx = action_select(s);
            const moq_action_t *head = &s->actions[idx % s->action_cap];
            if (s->action_meta[idx % s->action_cap].terminal_barrier ||
                action_waits_for_terminal(s, idx)) {
                terminal_blocked = true;
                break;
            }
            if (head->kind == MOQ_ACTION_SEND_DATA && element_size < send_data_needed)
                break;
            action_take(s, idx, &taken);
        }
        memset(dst, 0, element_size);
        memcpy(dst, &taken, copy);
        uint64_t epoch = s->borrow_epoch;
        memcpy(dst + offsetof(moq_action_t, borrow_epoch), &epoch, sizeof(epoch));
        dst += element_size;
        n++;
        if (session_is_active(s)) sg_retry_owed_resets(s);
    }

    if (n == 0 && terminal_blocked)
        return MOQ_ERR_WOULD_BLOCK;
    if (n == 0 && s->action_head < s->action_tail)
        return MOQ_ERR_ABI_MISMATCH;

    /* Draining actions freed action-queue capacity: a stream action owed
     * earlier is issued regardless of whether the peer ever sends another
     * byte on that stream. */
    session_action_poll_settled(s, n > 0);

    *out_count = n;
    return MOQ_OK;
}

static size_t event_min_poll_size(uint32_t kind)
{
    const size_t prefix = offsetof(moq_event_t, u);
    switch (kind) {
    case MOQ_EVENT_OBJECT_RECEIVED:
        return prefix + sizeof(moq_object_received_event_t);
    case MOQ_EVENT_FETCH_OBJECT:
        return prefix + sizeof(moq_fetch_object_event_t);
    case MOQ_EVENT_OBJECT_CHUNK:
        return prefix + sizeof(moq_object_chunk_event_t);
    /* Every fill event needs its complete frozen prefix: the object event
     * transfers owned buffers, and the others carry the identifying handles
     * a partial copy would truncate. */
    case MOQ_EVENT_FILL_CREATED:
        return prefix + MOQ_FILL_CREATED_EVENT_V0_SIZE;
    case MOQ_EVENT_FILL_REQUEST:
        return prefix + MOQ_FILL_REQUEST_EVENT_V0_SIZE;
    case MOQ_EVENT_FILL_OBJECT:
        return prefix + MOQ_FILL_OBJECT_EVENT_V0_SIZE;
    case MOQ_EVENT_FILL_GAP:
        return prefix + MOQ_FILL_GAP_EVENT_V0_SIZE;
    case MOQ_EVENT_FILL_TERMINAL:
        return prefix + MOQ_FILL_TERMINAL_EVENT_V0_SIZE;
    case MOQ_EVENT_OUTGOING_SUBGROUP_TERMINATED:
        return prefix + MOQ_OUTGOING_SUBGROUP_TERMINATED_EVENT_V0_SIZE;
    default:
        return prefix;
    }
}

/* An admission filter for the poll: `admit` decides each head event, and
 * `discarded` reports that one was dropped. */
typedef struct session_poll_admission {
    session_event_admission_t (*admit)(void *, const moq_event_t *);
    void *ctx;
    bool discarded;
} session_poll_admission_t;

static moq_result_t session_poll_events_impl(moq_session_t *s,
                                         void *out, size_t cap,
                                         size_t element_size,
                                         size_t *out_count,
                                         session_poll_admission_t *adm)
{
    if (out_count) *out_count = 0;
    if (!s || !out || !out_count) return MOQ_ERR_INVAL;
    if (cap == 0) return MOQ_OK;

    const size_t prefix = offsetof(moq_event_t, u);
    if (element_size < prefix) return MOQ_ERR_INVAL;

    const size_t lib_size = sizeof(moq_event_t);
    const size_t copy = lib_size < element_size ? lib_size : element_size;
    if (!adm || s->event_head == s->event_tail) {
        ts_retry_pending(s);
        ann_retry_pending(s);
        ns_sub_retry_pending(s);
    }

    size_t n = 0;
    uint8_t *dst = (uint8_t *)out;

    while (n < cap && s->event_head < s->event_tail) {
        const moq_event_t *head = &s->events[s->event_head % s->event_cap];

        if (element_size < event_min_poll_size(head->kind))
            break;

        session_event_admission_t decision = adm
            ? adm->admit(adm->ctx, head) : SESSION_EVENT_TAKE;
        if (decision == SESSION_EVENT_LEAVE) {
            *out_count = n;
            return MOQ_ERR_WOULD_BLOCK;
        }

        /* Release session budget when owned refs transfer to caller. */
        if (head->kind == MOQ_EVENT_OBJECT_RECEIVED) {
            if (head->u.object_received.payload) {
                size_t plen = moq_rcbuf_len(head->u.object_received.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
            }
            if (head->u.object_received.properties) {
                size_t elen = moq_rcbuf_len(head->u.object_received.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
            }
        } else if (head->kind == MOQ_EVENT_FETCH_OBJECT) {
            if (head->u.fetch_object.payload) {
                size_t plen = moq_rcbuf_len(head->u.fetch_object.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
            }
            if (head->u.fetch_object.properties) {
                size_t elen = moq_rcbuf_len(head->u.fetch_object.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
            }
        } else if (head->kind == MOQ_EVENT_FILL_OBJECT) {
            if (head->u.fill_object.payload) {
                size_t plen = moq_rcbuf_len(head->u.fill_object.payload);
                if (plen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= plen;
                else
                    s->recv_payload_bytes = 0;
            }
            if (head->u.fill_object.properties) {
                size_t elen = moq_rcbuf_len(head->u.fill_object.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
            }
        } else if (head->kind == MOQ_EVENT_OBJECT_CHUNK) {
            if (head->u.object_chunk.properties) {
                size_t elen = moq_rcbuf_len(head->u.object_chunk.properties);
                if (elen <= s->recv_payload_bytes)
                    s->recv_payload_bytes -= elen;
                else
                    s->recv_payload_bytes = 0;
            }
        }

        memset(dst, 0, element_size);
        memcpy(dst, head, copy);
        uint64_t epoch = s->borrow_epoch;
        memcpy(dst + offsetof(moq_event_t, borrow_epoch), &epoch, sizeof(epoch));
        /* The observed fact is recorded HERE -- at the transfer -- not when the
         * event was queued and not when the queue merely holds one. It means a
         * caller received MOQ_EVENT_SESSION_CLOSED. Placed after the copy
         * succeeded and before the head advance, so it cannot be set for an
         * event that was not handed out (the size-gated `break` above returns
         * without copying). terminal_observed therefore implies
         * terminal_enqueued by construction: the event could only be here
         * because a push_event placed it. */
        if (head->kind == MOQ_EVENT_SESSION_CLOSED)
            s->terminal_observed = true;
        if (head->kind == MOQ_EVENT_PUBLISH_STATE && head->u.publish_state.location)
            event_location_retire(s, head->u.publish_state.location);
        if (decision == SESSION_EVENT_DISCARD) {
            moq_event_cleanup((moq_event_t *)out);
            adm->discarded = true;
        }
        s->event_head++;
        /* Advance the progress token on event TRANSFER too (not only enqueue):
         * a bounded poll that dequeues events frees queue capacity, which is the
         * progress an adapter re-drive needs to see when the app drains a
         * pre-existing backlog that service() did not itself refill (a large
         * event queue never backpressures, so the enqueue signal alone misses
         * it). See moq_transport_bridge_event_progress_token. */
        s->event_progress_token++;
        dst += element_size;
        n++;
    }

    if (n == 0 && s->event_head < s->event_tail)
        return MOQ_ERR_ABI_MISMATCH;

    /* Draining events freed event-queue capacity: retry delivering any data
     * held for a now-established alias. Without this a tiny event queue could
     * strand held data, since replay otherwise only runs on SUBSCRIBE_OK or
     * fresh network input. Called unconditionally (not gated on staged
     * datagrams): a deferred subgroup/uni stream can be stranded with
     * staged_count == 0. Newly delivered objects surface on the next poll. */
    if (n > 0)
        session_replay_staged(s);

    /* Same freed-capacity rule for deferred early-arrival request bidis
     * (§3.3, buffered before establishment): their establishment-time refeed
     * can WOULD_BLOCK on a full event queue (e.g. SETUP_COMPLETE in a
     * max_events=1 queue), and there is no bridge retry for those bytes (they
     * were accepted) nor any guaranteed further peer activity on the stream.
     * Newly surfaced request events appear on the next poll. A hard dispatch
     * error closes the session inside the refeed; its CLOSED event/action
     * surface through the normal queues. */
    if (n > 0 && s->request_refeed_pending && session_is_active(s))
        (void)request_streams_refeed_deferred(s);

    /* Freed event capacity may let a fill child surface its owed request or
     * terminal (session_fill.c retains them until then). */
    if (n > 0 && session_is_active(s)) {
        sg_retry_owed_resets(s);
        fill_service(s);
        /* Likewise a malformed Track's cancellation that wanted an event. */
        mt_retry_pending(s);
    }

    ts_retry_pending(s);
    ann_retry_pending(s);
    ns_sub_retry_pending(s);

    *out_count = n;
    return MOQ_OK;
}

moq_result_t moq_session_poll_events_ex(moq_session_t *s,
    void *out, size_t cap, size_t element_size, size_t *out_count)
{
    return session_poll_events_impl(s, out, cap, element_size, out_count, NULL);
}

moq_result_t session_poll_event_admitted(moq_session_t *s, moq_event_t *out,
    session_event_admission_t (*admit)(void *, const moq_event_t *), void *ctx)
{
    if (!admit) return MOQ_ERR_INVAL;
    size_t count = 0;
    session_poll_admission_t adm = { .admit = admit, .ctx = ctx, .discarded = false };
    moq_result_t rc = session_poll_events_impl(s, out, 1, sizeof(*out), &count, &adm);
    if (rc != MOQ_OK) return rc;
    if (adm.discarded) return MOQ_ERR_BUFFER;
    return count ? MOQ_OK : MOQ_DONE;
}

/* -- Request capacity ---------------------------------------------- */

uint64_t moq_session_peer_request_capacity(const moq_session_t *s)
{
    if (!s) return 0;
    return s->profile->peer_request_capacity(s);
}

uint64_t moq_session_local_request_capacity(const moq_session_t *s)
{
    if (!s) return 0;
    return s->profile->local_request_capacity(s);
}

moq_result_t moq_session_grant_request_capacity(moq_session_t *s,
                                                  uint64_t new_capacity,
                                                  uint64_t now_us)
{
    if (!s) return MOQ_ERR_INVAL;
    return s->profile->grant_capacity(s, new_capacity, now_us);
}

/* D16 compatibility aliases — call through semantic API. */

uint64_t moq_session_peer_max_request_id(const moq_session_t *s)
{
    return moq_session_peer_request_capacity(s);
}

uint64_t moq_session_local_max_request_id(const moq_session_t *s)
{
    return moq_session_local_request_capacity(s);
}

moq_result_t moq_session_update_max_request_id(moq_session_t *s,
                                                uint64_t max_request_id,
                                                uint64_t now_us)
{
    return moq_session_grant_request_capacity(s, max_request_id, now_us);
}

uint64_t moq_session_peer_auth_token_cache_size(const moq_session_t *s)
{
    if (!s) return 0;
    return s->peer_setup.has_max_auth_token_cache_size ?
        s->peer_setup.max_auth_token_cache_size : 0;
}

/* Recognized terminal debts use a bounded retry wake. Every unrecognized
 * timer, due or future, retains its own deadline: one blocked debt must not
 * postpone recognition of another stream's expiry. */
static uint64_t reported_subgroup_deadline(const moq_session_t *s)
{
    uint64_t d = sg_has_terminal_debt(s)
        ? s->subgroup_retry_deadline_us : UINT64_MAX;
    /* subgroup_deadline_us is at or below every armed subgroup deadline (the
     * tick's own gate relies on it). Unarmed, skip that walk but retain the
     * retry wake for already recognized terminals. */
    if (s->subgroup_deadline_us == UINT64_MAX) return d;
    /* Only a live subgroup carries a deadline, and only those are linked. */
    for (int32_t i = s->sg_occ_head; i >= 0; i = s->subgroups[i].occ_next) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_deadline_sg_probes++;
#endif
        const moq_sg_entry_t *sg = &s->subgroups[i];
        if (sg->state == MOQ_SG_FREE) continue;
        if (sg->delivery_deadline_us < d) d = sg->delivery_deadline_us;
    }
    return d;
}

uint64_t moq_session_next_deadline_us(const moq_session_t *s)
{
    if (!s || s->state == MOQ_SESS_CLOSED) return UINT64_MAX;
    uint64_t d = s->goaway_deadline_us;
    uint64_t sg = reported_subgroup_deadline(s);
    if (sg < d) d = sg;
    uint64_t qd = session_queued_data_deadline(s);
    if (qd < d) d = qd;
    if (s->idle_deadline_us < d) d = s->idle_deadline_us;
    /* §9.8: every armed, not-yet-fired terminal deadline; fired entries
     * contribute only via the bounded retry deadline below. A terminal
     * deadline is armed only on a live owner, and freeing one clears it, so
     * the occupancy lists hold every candidate. */
    for (int32_t i = s->sub_occ_head; i >= 0; i = s->subs[i].occ_next) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_deadline_sub_probes++;
#endif
        if (s->subs[i].done_pending && !s->subs[i].done_expired &&
            s->subs[i].done_deadline_us < d)
            d = s->subs[i].done_deadline_us;
    }
    for (int32_t i = s->pub_occ_head; i >= 0; i = s->publishes[i].occ_next) {
#ifdef MOQ_SESSION_SWEEP_TESTING
        session_work_deadline_pub_probes++;
#endif
        if (s->publishes[i].done_pending && !s->publishes[i].done_expired &&
            s->publishes[i].done_deadline_us < d)
            d = s->publishes[i].done_deadline_us;
    }
    if (s->expiry_retry_deadline_us < d) d = s->expiry_retry_deadline_us;
    if (s->has_owner_deadline && s->owner_deadline_us < d)
        d = s->owner_deadline_us;
    return d;
}
