/*
 * Deferred-completion sweep: suspension, phase persistence and cursor
 * isolation.
 *
 * These call the private resumable operations directly so the suspension
 * boundary can be placed exactly where each property lives -- between two owner
 * phases, or mid-RX-scan -- which no caller-level entry point can express.
 *
 * The service-reachable entry points are covered in the bridge tests instead:
 * they suspend only inside a budget context, which the private budgeted service
 * entry establishes, and only at the granularity a whole advancing call gives.
 */
#include <moq/moq.h>
#include "test_support.h"
#include "../../core/src/session/session_internal.h"
#include "../../core/src/session/session_transport.h"
#include "../support/sweep_arm.h"
#include <string.h>

static moq_session_t *make_session_ex(uint32_t max_actions)
{
    moq_session_cfg_t cfg;
    moq_session_cfg_init_sized(&cfg, sizeof(cfg), moq_alloc_default(),
                               MOQ_PERSPECTIVE_CLIENT);
    if (max_actions) cfg.max_actions = max_actions;
    moq_session_t *s = NULL;
    if (moq_session_create(&cfg, 0, &s) < 0) return NULL;
    return s;
}

static moq_session_t *make_session(void)
{
    return make_session_ex(0);
}

/*
 * An owner the sweep will classify as runnable but cannot finish: expired, with
 * a bound rx stream whose STOP cannot be queued because the action queue is
 * full. It therefore stays done_pending across a completed sweep, which is what
 * arms expiry_retry_deadline_us.
 */
static void arm_blocked_expired_pub(moq_session_t *s, size_t slot,
                                    size_t rx_slot)
{
    sweep_arm_expired_pub(s, slot);
    sweep_bind_rx(s, rx_slot, s->publishes[slot].handle);
}


/*
 * A budget that runs out immediately before FINALIZE must leave the phase at
 * FINALIZE, and the resumption must finalize WITHOUT scanning rx again.
 *
 * The discriminator is the scan-entry counter, not the budget: re-entering
 * STOP_STREAMS increments it even when the rescan matches nothing and costs no
 * budget, which a budget-only check cannot see.
 */
static int test_suspend_before_finalize_resumes_at_finalize(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    s->sweep_now_us = 1000;
    s->sweep_slot = 0;
    session_sweep_owner_reset(s);
    sweep_arm_expired_pub(s, 0);
    sweep_bind_rx(s, 2, s->publishes[0].handle);   /* gives STOP_STREAMS real work */

    /* One STOP attempt, then the finalize check finds the budget empty. */
    uint32_t budget = 1;
    bool done = pub_reap_deferred_dones_resumable(s, &budget);
    MOQ_TEST_CHECK(!done);                                  /* suspended */
    MOQ_TEST_CHECK_EQ_INT((int)s->sweep_phase,
                          (int)MOQ_SWEEP_PHASE_FINALIZE);
    MOQ_TEST_CHECK(s->publishes[0].done_pending);           /* not finalized */

    /* Resume: the scan-entry count must not move. Re-entering STOP_STREAMS
     * would increment it even when the rescan finds nothing, which is what
     * makes this discriminating where a budget check alone is not. */
    uint64_t scans_before = session_stop_scan_entries;
    budget = 4;
    done = pub_reap_deferred_dones_resumable(s, &budget);
    MOQ_TEST_CHECK(done);
    MOQ_TEST_CHECK_EQ_U64(session_stop_scan_entries, scans_before);
    MOQ_TEST_CHECK(!s->publishes[0].done_pending);          /* finalized */
    MOQ_TEST_CHECK_EQ_INT((int)s->sweep_phase,
                          (int)MOQ_SWEEP_PHASE_SELECT);

    moq_session_destroy(s);
    return failures;
}

/*
 * A STOP that returns WOULD_BLOCK abandons the owner for this pass, and its rx
 * cursor must not survive. The owner is the only pending one, so nothing else
 * can reset the cursor incidentally -- a leak stays observable.
 */
static int test_would_block_does_not_leak_rx_cursor(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    s->sweep_now_us = 1000;
    s->sweep_slot = 0;
    session_sweep_owner_reset(s);
    /* Owner A is the ONLY pending owner, so no later owner's scan can mask a
     * leaked cursor by resetting it. */
    sweep_arm_expired_pub(s, 0);
    sweep_bind_rx(s, 2, s->publishes[0].handle);

    /* Fill the action queue so rx_try_stop() cannot queue its STOP_DATA. */
    while (!action_queue_full(s)) {
        moq_action_t a;
        memset(&a, 0, sizeof(a));
        a.kind = MOQ_ACTION_STOP_DATA;
        a.detail_size = (uint32_t)sizeof(moq_stop_data_action_t);
        a.borrow_epoch = s->borrow_epoch;
        a.u.stop_data.stream_ref = moq_stream_ref_from_u64(0xB000);
        if (push_action(s, &a) < 0) break;
    }
    MOQ_TEST_CHECK(action_queue_full(s));

    uint32_t budget = 64;
    (void)pub_reap_deferred_dones_resumable(s, &budget);

    /* Abandoned, not suspended: the cursor is cleared on the way out. */
    MOQ_TEST_CHECK_EQ_INT((int)s->sweep_phase, (int)MOQ_SWEEP_PHASE_SELECT);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->sweep_rx_pos, 0u);
    MOQ_TEST_CHECK(!s->sweep_rx_found);

    moq_session_destroy(s);
    return failures;
}

/* Discarding the cursor must clear every nested field, not just the flag. */
static int test_sweep_discard_clears_all_state(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    s->sweep_active = true;
    s->sweep_stage = MOQ_SWEEP_STAGE_SUB;
    s->sweep_slot = 3;
    s->sweep_rx_pos = 5;
    s->sweep_phase = MOQ_SWEEP_PHASE_STOP_STREAMS;
    s->sweep_rx_found = true;
    s->sweep_reaped_subgroup = true;

    session_sweep_discard(s);

    MOQ_TEST_CHECK(!s->sweep_active);
    MOQ_TEST_CHECK_EQ_INT((int)s->sweep_stage, (int)MOQ_SWEEP_STAGE_IDLE);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->sweep_slot, 0u);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->sweep_rx_pos, 0u);
    MOQ_TEST_CHECK_EQ_INT((int)s->sweep_phase, (int)MOQ_SWEEP_PHASE_SELECT);
    MOQ_TEST_CHECK(!s->sweep_rx_found);
    MOQ_TEST_CHECK(!s->sweep_reaped_subgroup);

    moq_session_destroy(s);
    return failures;
}

/* A pool of owners with unsatisfied counts and future deadlines has no runnable
 * work: it must not consume budget and must not suspend. */
static int test_future_owners_are_uncharged(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    s->sweep_now_us = 10;
    s->sweep_slot = 0;
    session_sweep_owner_reset(s);
    for (size_t i = 0; i < s->pub_cap && i < 4; i++) {
        moq_pub_entry_t *pe = &s->publishes[i];
        pe->done_pending = true;
        pe->done_expired = false;
        pe->done_stream_count = 5;
        pe->processed_stream_count = 0;
        pe->done_deadline_us = 1000000;   /* far future */
    }

    uint32_t budget = 0;                  /* zero budget, no runnable work */
    bool done = pub_reap_deferred_dones_resumable(s, &budget);
    MOQ_TEST_CHECK(done);                 /* completed, did not suspend */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)budget, 0u);

    moq_session_destroy(s);
    return failures;
}


/* Arm a subgroup so REAP_SUBGROUPS has an eligible entry. */
/* Each eligible subgroup reap costs exactly one unit; ineligible slots are
 * free, so a budget of N retires exactly N of them. */
static int test_one_subgroup_reap_per_unit(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    size_t armed = 0;
    for (size_t i = 0; i < s->sg_cap && armed < 3; i++, armed++)
        sweep_arm_closing_subgroup(s, i);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)armed, 3u);

    session_budget_enter(s, 2);            /* afford exactly two reaps */
    moq_result_t rc = session_begin_advance_budgeted(s, 100);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK(s->sweep_active);       /* cursor preserved */
    MOQ_TEST_CHECK_EQ_INT((int)s->sweep_stage,
                          (int)MOQ_SWEEP_STAGE_REAP_SUBGROUPS);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->budget_remaining, 0u);

    /* EXACTLY two reaps for two units: the third is untouched and the cursor
     * rests on it. Without this, one reap consuming both units would pass. */
    MOQ_TEST_CHECK(s->subgroups[0].state != MOQ_SG_CLOSING);
    MOQ_TEST_CHECK(s->subgroups[1].state != MOQ_SG_CLOSING);
    MOQ_TEST_CHECK(s->subgroups[2].state == MOQ_SG_CLOSING);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->sweep_slot, 2u);
    session_budget_leave(s);

    /* Resume through a RESULT-RETURNING unlimited API: it must complete the
     * exhausted cursor and must never hand the private sentinel to an
     * application-facing caller. session_begin_advance() returns void and so
     * cannot express this. */
    moq_result_t urc = moq_session_tick(s, 100);
    MOQ_TEST_CHECK(urc != MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK(!s->sweep_active);
    for (size_t i = 0; i < armed; i++)
        MOQ_TEST_CHECK(s->subgroups[i].state != MOQ_SG_CLOSING);

    moq_session_destroy(s);
    return failures;
}

/* Nothing runnable anywhere: a zero budget must still complete the whole stage
 * machine rather than suspending on the first stage it reaches. */
static int test_zero_budget_idle_sweep_completes(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    for (size_t i = 0; i < s->pub_cap && i < 4; i++) {
        moq_pub_entry_t *pe = &s->publishes[i];
        pe->done_pending = true;
        pe->done_expired = false;
        pe->done_stream_count = 5;
        pe->processed_stream_count = 0;
        pe->done_deadline_us = 1000000;    /* future: not runnable */
    }

    session_budget_enter(s, 0);
    moq_result_t rc = session_begin_advance_budgeted(s, 10);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(!s->sweep_active);      /* retired, not suspended */
    session_budget_leave(s);

    moq_session_destroy(s);
    return failures;
}

/* A suspended sweep finishes at its OWN epoch; a later unlimited caller then
 * runs a catch-up pass at its newer time, so an owner that became due during
 * the suspension is not skipped. */
static int test_old_epoch_then_catch_up(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    /* Owner 0 is due at the old epoch; owner 1 only becomes due later. */
    sweep_arm_expired_pub(s, 0);
    sweep_bind_rx(s, 2, s->publishes[0].handle);
    moq_pub_entry_t *late = &s->publishes[1];
    late->done_pending = true;
    late->done_expired = false;
    late->done_stream_count = MOQ_QUIC_VARINT_MAX;
    late->processed_stream_count = 0;
    late->done_deadline_us = 500;          /* future at 100, due at 1000 */
    late->handle = (moq_publication_t){
        moq_handle_pack(MOQ_HANDLE_POOL_PUBLICATION, 1u, 1u, 1u) };

    session_budget_enter(s, 1);
    moq_result_t rc = session_begin_advance_budgeted(s, 100);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK_EQ_U64(s->sweep_now_us, 100u);   /* epoch pinned */
    session_budget_leave(s);

    /* Unlimited call at a NEWER time, through a REAL service-reachable entry:
     * it completes the old sweep, then catches up so the owner that became due
     * in between is finalized too -- and the sentinel never escapes to it. */
    moq_result_t urc = moq_session_tick(s, 1000);
    MOQ_TEST_CHECK_EQ_INT((int)urc, (int)MOQ_OK);
    MOQ_TEST_CHECK(urc != MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK(!s->sweep_active);
    MOQ_TEST_CHECK(!s->publishes[0].done_pending);
    MOQ_TEST_CHECK(!s->publishes[1].done_pending);  /* catch-up caught it */

    moq_session_destroy(s);
    return failures;
}


/* Zero budget with genuinely runnable work is the other half of the contract:
 * it must suspend, preserve the cursor at that transition, and leave the work
 * undone. */
static int test_zero_budget_runnable_suspends(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    sweep_arm_closing_subgroup(s, 0);            /* one eligible reap */

    session_budget_enter(s, 0);
    moq_result_t rc = session_begin_advance_budgeted(s, 100);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK(s->sweep_active);
    MOQ_TEST_CHECK_EQ_INT((int)s->sweep_stage,
                          (int)MOQ_SWEEP_STAGE_REAP_SUBGROUPS);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->sweep_slot, 0u);
    MOQ_TEST_CHECK(s->subgroups[0].state == MOQ_SG_CLOSING);  /* untouched */
    session_budget_leave(s);

    moq_session_destroy(s);
    return failures;
}


/* Set a plausible mid-sweep cursor. */
static void dirty_cursor(moq_session_t *s)
{
    s->sweep_active = true;
    s->sweep_stage = MOQ_SWEEP_STAGE_SUB;
    s->sweep_slot = 2;
    s->sweep_rx_pos = 3;
    s->sweep_phase = MOQ_SWEEP_PHASE_STOP_STREAMS;
    s->sweep_rx_found = true;
    s->sweep_reaped_subgroup = true;
}

static int cursor_is_clear(moq_session_t *s)
{
    return !s->sweep_active && s->sweep_stage == MOQ_SWEEP_STAGE_IDLE &&
           s->sweep_slot == 0 && s->sweep_rx_pos == 0 &&
           s->sweep_phase == MOQ_SWEEP_PHASE_SELECT && !s->sweep_rx_found &&
           !s->sweep_reaped_subgroup;
}

/*
 * close_with_error() must discard the cursor: a closing session has no deferred
 * completions left to sweep, and resuming would walk freed owners.
 *
 * moq_session_destroy() is wired the same way but cannot be checked causally --
 * the object is gone afterwards -- so its placement is source-reviewed, not
 * asserted here. Adding a production seam purely to observe it would be worse
 * than the gap.
 */
static int test_close_with_error_discards_cursor(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    dirty_cursor(s);
    (void)close_with_error(s, 0x1, "test");
    MOQ_TEST_CHECK(cursor_is_clear(s));

    moq_session_destroy(s);
    return failures;
}

/*
 * A continuation is a real advancing call and is prepared like one: it
 * invalidates borrows and reclaims whatever the queues have since drained,
 * exactly once, before any sweep work.
 *
 * Each of the following pins one half of that contract at the SAME timestamp,
 * which is the discriminating case -- with a newer clock the fresh sweep runs
 * for an unrelated reason and the property under test is not isolated.
 */
static int test_same_time_continuation_invalidates_borrows(void)
{
    int failures = 0;
    moq_session_t *s = make_session_ex(1);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    arm_blocked_expired_pub(s, 0, 2);
    session_budget_enter(s, 0);
    MOQ_TEST_CHECK_EQ_INT((int)session_begin_advance_budgeted(s, 100),
                          (int)MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK(s->sweep_active);
    session_budget_leave(s);

    /* A real polled record's borrow, checked through the public predicate. */
    moq_action_t act;
    MOQ_TEST_CHECK(moq_session_poll_actions(s, &act, 1) == 1);
    MOQ_TEST_CHECK(moq_session_borrow_valid(s, act.borrow_epoch));

    uint64_t epoch = s->borrow_epoch;
    session_begin_advance(s, 100);              /* same time */
    MOQ_TEST_CHECK(!s->sweep_active);
    /* The continuation is a real advancing call: the borrow it inherited is
     * now stale, and exactly one invalidation happened. */
    MOQ_TEST_CHECK(!moq_session_borrow_valid(s, act.borrow_epoch));
    MOQ_TEST_CHECK_EQ_U64(s->borrow_epoch, epoch + 1);
    moq_action_cleanup(&act);

    moq_session_destroy(s);
    return failures;
}

static int test_same_time_continuation_reclaims_scratch(void)
{
    int failures = 0;
    moq_session_t *s = make_session_ex(1);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    /* Real queued output, so send_len is genuinely in use and the suspension
     * below cannot reclaim it: the action queue is still occupied. */
    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    MOQ_TEST_CHECK(s->action_head != s->action_tail);
    MOQ_TEST_CHECK(s->send_len > 0);

    arm_blocked_expired_pub(s, 0, 2);
    session_budget_enter(s, 0);
    MOQ_TEST_CHECK_EQ_INT((int)session_begin_advance_budgeted(s, 100),
                          (int)MOQ_SESSION_SUSPENDED);
    session_budget_leave(s);
    MOQ_TEST_CHECK(s->send_len > 0);            /* still queued: not reclaimable */

    /* Drain it. poll_actions does not advance, so nothing has reclaimed yet. */
    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0)
        moq_action_cleanup(&act);
    MOQ_TEST_CHECK(s->action_head == s->action_tail);
    MOQ_TEST_CHECK(s->send_len > 0);

    /* output_scratch_len is reclaimed unconditionally; it is set directly here
     * because no session path leaves it non-empty across a call boundary. */
    s->output_scratch_len = 5;

    session_begin_advance(s, 100);              /* same time */
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->send_len, 0u);
    MOQ_TEST_CHECK_EQ_U64((uint64_t)s->output_scratch_len, 0u);

    moq_session_destroy(s);
    return failures;
}

/*
 * An owner the inherited cursor has already walked past can become runnable
 * again behind it -- poll-driven replay/refeed does exactly that. The
 * continuation therefore owes a fresh sweep of its own even at equal time.
 *
 * Suspension is forced in the SUB stage, which runs last, so the PUB pool is
 * already behind the cursor when the publication is armed.
 */
static int test_same_time_continuation_sweeps_owner_behind_cursor(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    sweep_arm_expired_sub(s, 0);
    sweep_bind_rx(s, 1, (moq_publication_t){ 0 });
    s->rx_streams[1].sub = s->subs[0].handle;
    s->rx_streams[1].pub_handle = (moq_publication_t){ 0 };

    session_budget_enter(s, 0);
    MOQ_TEST_CHECK_EQ_INT((int)session_begin_advance_budgeted(s, 100),
                          (int)MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK(s->sweep_active);
    MOQ_TEST_CHECK_EQ_INT((int)s->sweep_stage, (int)MOQ_SWEEP_STAGE_SUB);
    session_budget_leave(s);

    /* Now behind the cursor: the inherited sweep will never revisit PUB. */
    sweep_arm_expired_pub(s, 0);

    session_begin_advance(s, 100);              /* same time */
    MOQ_TEST_CHECK(!s->sweep_active);
    MOQ_TEST_CHECK(!s->subs[0].done_pending);
    MOQ_TEST_CHECK(!s->publishes[0].done_pending);

    moq_session_destroy(s);
    return failures;
}

/*
 * No mixed clocks: an inherited sweep retires against the epoch it was
 * suspended at, not the clock of the call that happens to finish it.
 *
 * The old sweep at 100 leaves an owner expired-and-blocked, so retiring it arms
 * expiry_retry_deadline_us from ITS epoch. The continuation at 1000 then
 * suspends in its own fresh sweep, so that armed deadline is what survives --
 * and it must still be derived from 100.
 */
static int test_inherited_sweep_retires_at_its_own_epoch(void)
{
    int failures = 0;
    moq_session_t *s = make_session_ex(1);
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    MOQ_TEST_CHECK_EQ_INT((int)moq_session_start(s, 0), (int)MOQ_OK);
    arm_blocked_expired_pub(s, 0, 2);

    session_budget_enter(s, 0);
    MOQ_TEST_CHECK_EQ_INT((int)session_begin_advance_budgeted(s, 100),
                          (int)MOQ_SESSION_SUSPENDED);
    session_budget_leave(s);
    MOQ_TEST_CHECK(s->sweep_active);
    MOQ_TEST_CHECK_EQ_U64(s->sweep_now_us, 100u);
    MOQ_TEST_CHECK_EQ_U64(s->expiry_retry_deadline_us, UINT64_MAX);

    session_budget_enter(s, 1);
    moq_result_t rc = session_begin_advance_budgeted(s, 1000);
    session_budget_leave(s);

    /* The fresh sweep suspended, so the inherited sweep's arming is what stands. */
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK(s->sweep_active);
    MOQ_TEST_CHECK_EQ_U64(s->sweep_now_us, 1000u);
    MOQ_TEST_CHECK(s->publishes[0].done_pending);
    MOQ_TEST_CHECK(s->publishes[0].done_expired);
    MOQ_TEST_CHECK_EQ_U64(s->expiry_retry_deadline_us,
                          100u + MOQ_DONE_EXPIRY_RETRY_US);

    moq_session_destroy(s);
    return failures;
}

/*
 * The unwired entry. moq_session_on_transport_close() deliberately does NOT
 * take the budgeted advance (C2, its only service-reachable call site, has no
 * sentinel branch), so it is the one service-reachable function proven purely
 * on the unlimited side: given a GENUINE suspended cursor it must complete it,
 * catch up at its newer time, never observe the sentinel, and then close.
 */
static int test_transport_close_completes_a_real_cursor(void)
{
    int failures = 0;
    moq_session_t *s = make_session();
    MOQ_TEST_CHECK(s != NULL);
    if (!s) return failures;

    /* A real suspension, not a hand-set cursor. */
    sweep_arm_closing_subgroup(s, 0);
    session_budget_enter(s, 0);
    MOQ_TEST_CHECK_EQ_INT((int)session_begin_advance_budgeted(s, 100),
                          (int)MOQ_SESSION_SUSPENDED);
    MOQ_TEST_CHECK(s->sweep_active);
    MOQ_TEST_CHECK(s->subgroups[0].state == MOQ_SG_CLOSING);
    session_budget_leave(s);
    MOQ_TEST_CHECK(!s->budget_active);

    uint64_t epoch_before = s->borrow_epoch;

    /* Unlimited, at a newer time. */
    moq_result_t rc = moq_session_on_transport_close(s, 0x1, 1000);
    MOQ_TEST_CHECK_EQ_INT((int)rc, (int)MOQ_OK);
    MOQ_TEST_CHECK(rc != MOQ_SESSION_SUSPENDED);

    MOQ_TEST_CHECK(cursor_is_clear(s));
    /* Close cleanup only: moq_session_on_transport_close() frees every subgroup
     * itself, so this cannot attribute the reclamation to the sweep. */
    MOQ_TEST_CHECK(s->subgroups[0].state == MOQ_SG_FREE);
    /* Exactly one physical advancing call, so exactly one invalidation. */
    MOQ_TEST_CHECK_EQ_U64(s->borrow_epoch, epoch_before + 1);
    MOQ_TEST_CHECK(s->last_now_us == 1000u);

    MOQ_TEST_CHECK(moq_session_state(s) == MOQ_SESS_CLOSED);

    /* Exactly one terminal event, and no CLOSE_SESSION action: a peer-driven
     * close does not ask the adapter to close the transport again. */
    size_t terminals = 0;
    moq_event_t ev;
    while (moq_session_poll_events(s, &ev, 1) > 0) {
        if (ev.kind == MOQ_EVENT_SESSION_CLOSED) terminals++;
        moq_event_cleanup(&ev);
    }
    MOQ_TEST_CHECK(terminals == 1);

    moq_action_t act;
    while (moq_session_poll_actions(s, &act, 1) > 0) {
        MOQ_TEST_CHECK(act.kind != MOQ_ACTION_CLOSE_SESSION);
        moq_action_cleanup(&act);
    }

    moq_session_destroy(s);
    return failures;
}

int main(void)
{
    int failures = 0;
    failures += test_suspend_before_finalize_resumes_at_finalize();
    failures += test_would_block_does_not_leak_rx_cursor();
    failures += test_sweep_discard_clears_all_state();
    failures += test_future_owners_are_uncharged();
    failures += test_one_subgroup_reap_per_unit();
    failures += test_zero_budget_idle_sweep_completes();
    failures += test_zero_budget_runnable_suspends();
    failures += test_old_epoch_then_catch_up();
    failures += test_close_with_error_discards_cursor();
    failures += test_same_time_continuation_invalidates_borrows();
    failures += test_same_time_continuation_reclaims_scratch();
    failures += test_same_time_continuation_sweeps_owner_behind_cursor();
    failures += test_inherited_sweep_retires_at_its_own_epoch();
    failures += test_transport_close_completes_a_real_cursor();

    if (failures == 0)
        printf("test_session_sweep: all tests passed\n");
    else
        fprintf(stderr, "test_session_sweep: %d failure(s)\n", failures);
    return failures;
}
