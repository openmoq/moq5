#include "transport_bridge/test_cases.h"

int main(void)
{
    int failures = 0;
    failures += test_held_driver_replay(false);
    failures += test_held_driver_replay(true);
    for (int d18 = 0; d18 < 2; ++d18) {
        for (int upd = 0; upd <= 1; upd++) {
            failures += test_priority_survives_closed_subgroup(
                false, false, d18 != 0, upd != 0);
            failures += test_priority_survives_closed_subgroup(
                true, false, d18 != 0, upd != 0);
            failures += test_priority_survives_closed_subgroup(
                true, true, d18 != 0, upd != 0);
        }
    }

    failures += test_budget_context_paired_on_every_exit();
    failures += test_unlimited_service_enters_no_budget_context();
    failures += test_budgeted_service_requires_output();
    failures += test_control_retry_suspension_preserves_pending();
    failures += test_data_retry_suspension_preserves_pending();
    failures += test_data_reset_suspension_preserves_pending();
    failures += test_bidi_retry_suspension_preserves_pending();
    failures += test_fin_bridge_descriptor_validation();
    failures += test_ns_response_fin_bridge();
    failures += test_ns_response_reset_bridge();
    failures += test_p7_bridge_fin_retirement();
    failures += test_p7_retirement_idempotence_and_reuse();
    failures += test_p3_bridge_fin_retirement();
    failures += test_local_uni_fin_awaits_commit();
    failures += test_ns_sub_local_teardown_bridge_retry();
    failures += test_no_owner_admission_bridge();
    failures += test_noslot_exhaustion_reset();
    failures += test_noslot_reset_bidi_wouldblock_then_accept();
    failures += test_uni_reset_data_immediate_retire();
    failures += test_noslot_reset_bidi_peer_before_accept();
    failures += test_noslot_carrier_peer_reset();
    failures += test_noslot_carrier_peer_stop();
    failures += test_noslot_exhaustion_close();
    failures += test_bridge_nomem_ns_response();
    failures += test_bridge_nomem_joining_fetch();
    failures += test_bidi_reset_suspension_preserves_pending();
    failures += test_bidi_stop_suspension_preserves_pending();
    failures += test_data_stop_suspension_preserves_pending();
    failures += test_tick_suspension_preserves_due_deadline();
    failures += test_inbound_scan_stops_at_suspension();
    failures += test_progress_then_suspension_in_one_pass();
    failures += test_continuation_reclaims_event_scratch();
    failures += test_create_destroy();
    failures += test_create_rejects_bad_ops();
    failures += test_setup_handshake();
    failures += test_control_write_backpressure();
    failures += test_transport_close();
    failures += test_datagram_inbound_not_fatal();
    failures += test_inbound_uni_after_setup();
    failures += test_close_error_is_fatal_not_closed();

    /* Regression tests */
    failures += test_empty_uni_no_ghost_stream();
    failures += test_truncated_vtable_rejected();
    failures += test_inbound_uni_dropped_then_discarded();
    failures += test_inbound_uni_rcbuf_dropped_then_discarded();
    failures += test_pending_retry_keeps_bytes();
    failures += test_pending_retry_keeps_rcbuf();

    /* Hard retry tests */
    failures += test_close_retry_after_blocked_control();
    failures += test_deferred_close_clears_ordinary_pending();
    failures += test_close_retry_would_block();
#ifdef MOQ_BRIDGE_TEST_INJECT
    failures += test_deferred_close_keeps_its_own_code();
#endif
    failures += test_reset_on_unknown_stream();
    failures += test_transport_close_clears_state();

    /* Peer STOP_SENDING on a request bidi (draft-18 §3.3.2) */
    failures += test_local_bidi_stop_keeps_response_half();
    failures += test_local_bidi_stop_duplicate_is_idempotent();
    failures += test_local_bidi_stop_blocked_reset_retries();
    failures += test_peer_bidi_stop_is_request_cancellation();
    failures += test_local_bidi_stop_then_peer_terminal_retires_once();
    failures += test_local_bidi_stop_after_local_fin();
    failures += test_local_bidi_normal_fin_unchanged();

    /* per-stream head-of-line acceptance battery */
    failures += test_hol_unrelated_ref_progresses();
    failures += test_hol_same_ref_no_bypass_fin_last();
    failures += test_hol_two_parked_third_progresses();
    failures += test_hol_terminal_precedence_over_bypass();
    failures += test_hol_service_work_is_bounded();
    failures += test_hol_close_intent_not_deferred();
    failures += test_hol_close_priority_once_begun();
    failures += test_hol_conservation_of_owned_actions();
    failures += test_hol_gate_orders_when_endpoint_would_accept();
    failures += test_hol_mixed_identity_same_stream();
    failures += test_hol_pending_close_blocks_stream_work();
    failures += test_hol_refless_domains_are_distinct();
    failures += test_hol_conservation_through_retry();
    failures += test_hol_retention_capacity_is_not_fatal();
#ifdef MOQ_BRIDGE_TEST_INJECT
    failures += test_hol_canon_fills_sid_from_ref();
    failures += test_hol_stream_id_zero_is_a_real_identity();
    failures += test_hol_identityless_candidate_fails_closed();
    failures += test_hol_pending_identity_completed_only_at_enqueue();
#endif
    failures += test_hol_explorer();
    failures += test_hol_drain_budget_is_a_real_backstop();
    failures += test_hol_owned_payload_conservation();
#ifdef MOQ_BRIDGE_TEST_INJECT
    failures += test_hol_retention_releases_owned_payload_once();
    failures += test_hol_initial_retention_failure_is_fatal_not_silent();
    failures += test_hol_retention_failure_is_fatal_not_silent();
#endif
#ifdef MOQ_BRIDGE_TEST_INJECT
    failures += test_hol_action_domain_table();
    failures += test_hol_ref_zero_does_not_alias();
    failures += test_hol_mixed_identity_canonicalises();
    failures += test_hol_depth_and_structural_bound();
#endif
#ifdef MOQ_BRIDGE_SCAN_COUNTERS
    failures += test_hol_scan_operation_counts();
#endif

    /* terminal facts */
    failures += test_terminal_facts_enqueued_then_observed();
    failures += test_terminal_facts_not_set_by_other_events();
    failures += test_already_fatal_transport_terminal();
    failures += test_setup_scratch_shortfall_closes_not_fatal();

    failures += test_hol_one_slot_per_action_and_no_drain_overflow();

    /* held peer request streams */
    failures += test_hold_bidi_records_fifo_and_expiry();
    failures += test_hold_bidi_abort_and_blocked_cancel();
    failures += test_hold_bidi_cancel_failure_fatal();
    failures += test_hold_bidi_requires_capability();
    failures += test_hold_uni_d16();

    /* the peer control stream on the last free stream-map entry */
    failures += test_control_last_entry(MOQ_VERSION_DRAFT_18, false);
    failures += test_control_last_entry(MOQ_VERSION_DRAFT_18, true);
    failures += test_control_last_entry(MOQ_VERSION_DRAFT_22, false);
    failures += test_control_last_entry(MOQ_VERSION_DRAFT_22, true);

    /* minimum stream-map capacity at create */
    {
        const moq_version_t vers[3] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                        MOQ_VERSION_DRAFT_22 };
        const moq_perspective_t persps[2] = { MOQ_PERSPECTIVE_CLIENT,
                                              MOQ_PERSPECTIVE_SERVER };
        for (int v = 0; v < 3; v++)
            for (int k = 0; k < 2; k++)
                failures += test_min_streams_at_create(vers[v], persps[k]);
    }

    /* peer uni input deferred behind pending control input */
    failures += test_uni_deferred_behind_control(MOQ_VERSION_DRAFT_16);
    failures += test_uni_deferred_behind_control(MOQ_VERSION_DRAFT_18);
    failures += test_uni_deferred_behind_control(MOQ_VERSION_DRAFT_22);

    /* new peer uni streams placed by class on a uni-pair profile */
    {
        const moq_version_t vers[2] = { MOQ_VERSION_DRAFT_18, MOQ_VERSION_DRAFT_22 };
        for (int v = 0; v < 2; v++) {
            failures += test_fm_wire_classes(vers[v]);
            failures += test_fm_data_held(vers[v], false);
            failures += test_fm_data_held(vers[v], true);
            failures += test_fm_offer_and_age(vers[v]);
            failures += test_fm_blocked_stop_then_reset(vers[v]);
            failures += test_fm_padding(vers[v], false);
            failures += test_fm_padding(vers[v], true);
            failures += test_fm_reserve_both_controls(vers[v], true);
            failures += test_fm_reserve_both_controls(vers[v], false);
            failures += test_fm_reserve_local_blocked(vers[v]);
            failures += test_fm_local_reserve_after_fin(vers[v]);
            failures += test_fm_cross_kind_fifo(vers[v]);
            failures += test_fm_behind_pending_control(vers[v]);
            failures += test_fm_unknown_unchanged(vers[v]);
            failures += test_fm_incomplete_type_known_limitation(vers[v]);
            failures += test_fm_capability_off(vers[v]);
        }
    }

    if (failures == 0)
        printf("test_transport_bridge: all tests passed\n");
    else
        fprintf(stderr, "test_transport_bridge: %d failure(s)\n", failures);

    return failures;
}
