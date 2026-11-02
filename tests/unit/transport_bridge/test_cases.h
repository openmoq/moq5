#ifndef BRIDGE_TEST_CASES_H
#define BRIDGE_TEST_CASES_H

#include "bridge_test_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

int test_budget_context_paired_on_every_exit(void);

int test_unlimited_service_enters_no_budget_context(void);

int test_budgeted_service_requires_output(void);

int test_data_retry_suspension_preserves_pending(void);

int test_data_reset_suspension_preserves_pending(void);

int test_bidi_retry_suspension_preserves_pending(void);

int test_bridge_nomem_ns_response(void);

int test_bridge_nomem_joining_fetch(void);

int test_bidi_reset_suspension_preserves_pending(void);

int test_bidi_stop_suspension_preserves_pending(void);

int test_data_stop_suspension_preserves_pending(void);

int test_tick_suspension_preserves_due_deadline(void);

int test_inbound_scan_stops_at_suspension(void);

int test_progress_then_suspension_in_one_pass(void);

int test_continuation_reclaims_event_scratch(void);

int test_control_retry_suspension_preserves_pending(void);

int test_create_destroy(void);

int test_create_rejects_bad_ops(void);

int test_setup_handshake(void);

int test_control_write_backpressure(void);

int test_transport_close(void);

int test_datagram_inbound_not_fatal(void);

int test_inbound_uni_after_setup(void);

int test_close_error_is_fatal_not_closed(void);

int test_empty_uni_no_ghost_stream(void);

int test_truncated_vtable_rejected(void);

int test_close_retry_after_blocked_control(void);

int test_deferred_close_clears_ordinary_pending(void);

int test_close_retry_would_block(void);

#ifdef MOQ_BRIDGE_TEST_INJECT

int test_deferred_close_keeps_its_own_code(void);

#endif

int test_reset_on_unknown_stream(void);

int test_transport_close_clears_state(void);

int test_inbound_uni_dropped_then_discarded(void);

int test_inbound_uni_rcbuf_dropped_then_discarded(void);

int test_pending_retry_keeps_bytes(void);

int test_pending_retry_keeps_rcbuf(void);

int test_terminal_facts_enqueued_then_observed(void);

int test_terminal_facts_not_set_by_other_events(void);

int test_already_fatal_transport_terminal(void);

int test_setup_scratch_shortfall_closes_not_fatal(void);

int test_ns_response_fin_bridge(void);

int test_ns_response_reset_bridge(void);

int test_fin_bridge_descriptor_validation(void);

int test_p7_bridge_fin_retirement(void);

int test_p7_retirement_idempotence_and_reuse(void);

int test_p3_bridge_fin_retirement(void);

int test_ns_sub_local_teardown_bridge_retry(void);

int test_noslot_exhaustion_reset(void);

int test_noslot_reset_bidi_wouldblock_then_accept(void);

int test_uni_reset_data_immediate_retire(void);

int test_noslot_reset_bidi_peer_before_accept(void);

int test_noslot_carrier_peer_reset(void);

int test_noslot_carrier_peer_stop(void);

int test_noslot_exhaustion_close(void);

int test_no_owner_admission_bridge(void);

int test_local_bidi_stop_keeps_response_half(void);

int test_local_bidi_stop_duplicate_is_idempotent(void);

int test_local_bidi_stop_blocked_reset_retries(void);

int test_peer_bidi_stop_is_request_cancellation(void);

int test_local_bidi_stop_then_peer_terminal_retires_once(void);

int test_local_bidi_stop_after_local_fin(void);

int test_local_bidi_normal_fin_unchanged(void);

int test_hol_unrelated_ref_progresses(void);

int test_hol_same_ref_no_bypass_fin_last(void);

int test_hol_two_parked_third_progresses(void);

int test_hol_terminal_precedence_over_bypass(void);

int test_hol_service_work_is_bounded(void);

int test_hol_close_intent_not_deferred(void);

int test_hol_close_priority_once_begun(void);

int test_hol_conservation_of_owned_actions(void);

int test_hol_explorer(void);

int test_hol_gate_orders_when_endpoint_would_accept(void);

int test_hol_mixed_identity_same_stream(void);

int test_hol_pending_close_blocks_stream_work(void);

int test_hol_refless_domains_are_distinct(void);

int test_hol_conservation_through_retry(void);


#if defined(MOQ_BRIDGE_SCAN_COUNTERS) && defined(MOQ_BRIDGE_TEST_INJECT)

int test_hol_scan_operation_counts(void);
int test_hol_full_fifo_mapped_progress(void);

#endif

#ifdef MOQ_BRIDGE_TEST_INJECT

int test_hol_action_domain_table(void);

int test_hol_ref_zero_does_not_alias(void);

int test_hol_mixed_identity_canonicalises(void);

int test_hol_depth_and_structural_bound(void);

#endif

int test_hol_drain_budget_is_a_real_backstop(void);

#ifdef MOQ_BRIDGE_TEST_INJECT

int test_hol_retention_releases_owned_payload_once(void);

int test_hol_initial_retention_failure_is_fatal_not_silent(void);

int test_hol_retention_failure_is_fatal_not_silent(void);

#endif

int test_hol_owned_payload_conservation(void);

int test_hol_retention_capacity_is_not_fatal(void);

#ifdef MOQ_BRIDGE_TEST_INJECT

int test_hol_canon_fills_sid_from_ref(void);

int test_hol_identityless_candidate_fails_closed(void);

int test_hol_pending_identity_completed_only_at_enqueue(void);

int test_hol_stream_id_zero_is_a_real_identity(void);

#endif

int test_hol_one_slot_per_action_and_no_drain_overflow(void);

int test_priority_survives_closed_subgroup(bool fail, bool legacy, bool d18,
                                           bool update_priority);

int test_local_uni_fin_awaits_commit(void);

int test_held_driver_replay(bool rcbuf);

int test_hold_bidi_records_fifo_and_expiry(void);

int test_hold_bidi_abort_and_blocked_cancel(void);

int test_hold_bidi_cancel_failure_fatal(void);

int test_hold_bidi_requires_capability(void);

int test_hold_uni_d16(void);

int test_control_last_entry(moq_version_t ver, bool split);

int test_uni_deferred_behind_control(moq_version_t ver);

int test_fm_wire_classes(moq_version_t ver);

int test_fm_data_held(moq_version_t ver, bool use_rcbuf);

int test_fm_offer_and_age(moq_version_t ver);

int test_fm_blocked_stop_then_reset(moq_version_t ver);

int test_fm_padding(moq_version_t ver, bool use_rcbuf);

int test_fm_reserve_both_controls(moq_version_t ver, bool local_first);

int test_fm_reserve_local_blocked(moq_version_t ver);

int test_fm_local_reserve_after_fin(moq_version_t ver);

int test_fm_cross_kind_fifo(moq_version_t ver);

int test_fm_behind_pending_control(moq_version_t ver);

int test_fm_unknown_unchanged(moq_version_t ver);

int test_fm_incomplete_type_known_limitation(moq_version_t ver);

int test_fm_capability_off(moq_version_t ver);

int test_min_streams_at_create(moq_version_t ver, moq_perspective_t persp);
#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* BRIDGE_TEST_CASES_H */
