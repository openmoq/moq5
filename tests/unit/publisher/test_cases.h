#ifndef PUBLISHER_TEST_CASES_H
#define PUBLISHER_TEST_CASES_H

#include "publisher_test_internal.h"

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility push(hidden)
#endif

/* authorization */
void test_unsubscribed_retires_slot(void);
void test_deferred_accept(void);
void test_deferred_reject(void);
void test_deferred_resolution_keeps_multiple_owners(void);
void test_end_track_rejects_late_subscribe(void);
void test_end_track_rejects_deferred_subscribe(void);
void test_deferred_would_block_then_flush(void);
void test_deferred_remove_track_blocked(void);
void test_deferred_session_close_releases(void);
void test_deferred_does_not_block_other_tracks(void);
void test_deferred_unsub_before_resolve(void);
void test_deferred_stale_id_after_reuse(void);
void test_deferred_flush_fires_joined(void);
void test_deferred_old_cfg_struct_size(void);

/* basic */
void test_create_destroy(void);
void test_pub_cfg_init_old_prefix_no_overflow(void);
void test_pub_callbacks_init_old_prefix_no_overflow(void);
void test_add_remove_track(void);
void test_subscribe_accept_all(void);
void test_remove_track_retires_subscription(void);
void test_subscribe_no_match(void);
void test_subscribe_duplicate_rejected(void);
void test_write_single_sub(void);
void test_end_track_with_stream_keys(void);
void test_end_track(void);
void test_finish_subscribers(void);
void test_finish_subscribers_not_terminal(void);
void test_finish_subscribers_would_block(void);
void test_write_no_subs(void);
void test_write_group_change(void);
void test_end_group(void);
void test_remove_track_with_open_sg(void);
void test_destroy_with_live_subs(void);
void test_event_ignored(void);
void test_session_closed_event(void);
void test_publisher_priority_zero(void);
void test_publisher_priority_default(void);
void test_publisher_priority_old_struct(void);
void test_pub_track_cfg_init_canary(void);
void test_oversized_namespace_rejected(void);
void test_oversized_full_track_name_rejected(void);

/* subscribe callback */
void test_callback_accept(void);
void test_callback_reject_custom_error(void);
void test_callback_reject_default_error(void);
void test_callback_sees_filter_and_ns(void);
void test_callback_sees_tokens(void);
void test_callback_would_block(void);
void test_callback_null_rejected(void);
void test_callback_invalid_decision(void);
void test_callback_not_called_for_duplicate(void);
void test_old_struct_size_accept_all(void);

/* completion */
void test_declare_b_rejections(moq_version_t ver);
void test_declare_pending_and_merge(moq_version_t ver);
void test_declare_completion_basic(moq_version_t ver);
void test_declare_publication_completes(moq_version_t ver);
void test_declare_blocked_steps(moq_version_t ver, bool lower_redeclare);
void test_declare_fin_before_done(moq_version_t ver);
void test_declare_install_immediate(moq_version_t ver, bool tick_mode);
void test_declare_install_deferred_and_pending(moq_version_t ver);
void test_declare_update_paths(moq_version_t ver);
void test_declare_after_end_track(moq_version_t ver);
void test_declare_fetch_below_watermark(moq_version_t ver);
void test_declare_plain_track_unchanged(moq_version_t ver);
void test_declare_stale_completion(moq_version_t ver,
                                   bool publication_mode);
void test_declare_stale_armed_reset(moq_version_t ver);
void test_declare_end_track_reconcile(moq_version_t ver);
void test_declare_end_track_ceiling(moq_version_t ver);
void test_declare_end_track_blocked_preflight(moq_version_t ver);
void test_window_completion_status(moq_version_t ver);

/* event forwarding */
void test_pub_manual_forward_zero_pauses(void);
void test_pub_manual_broadcasts_and_results(void);
void test_pub_manual_publish_ok(void);
void test_pub_manual_unsubscribe_active(void);
void test_pub_manual_vs_tick_parity(void);
void test_pub_manual_forward_zero_resets_open_subgroup(void);
void test_pub_manual_publish_family_results(void);
void test_pub_manual_two_track_serialized_no_loss(void);
void test_pub_manual_multiple_owned_answers(void);
void test_pub_manual_close_releases_pending_fetch(void);
void test_pub_manual_publish_error_matched(void);

/* fanout */
void test_fanout_delivery_sub_first(void);
void test_fanout_delivery_pub_first(void);
void test_fanout_second_dest_would_block(void);
void test_fanout_mid_op_joiner_excluded(void);
void test_fanout_forward_zero_subscription(void);
void test_fanout_old_sized_object_cfg(void);
void test_fanout_publication_forward_drop(void);
void test_fanout_close_releases_pending_op(void);
void test_fanout_forward_enable_mid_op_joins_next(void);
void test_pub_object_cfg_init_old_prefix_no_overflow(void);
void test_fanout_drop_restore_excluded_from_pending(void);
void test_fanout_forward_drop_resets_eog_subgroup(void);
void test_finish_subscribers_skips_publication(void);
void test_finish_subscribers_pub_mid_object(void);

/* filter window */
void test_publication_end_status(moq_version_t ver);
void test_window_membership_matrix(moq_version_t ver);
void test_window_stream_objects(moq_version_t ver);
void test_window_publication_and_coexist(moq_version_t ver,
                                         bool sub_first);
void test_window_fully_filtered_history(moq_version_t ver);
void test_window_narrow_resets_open_subgroup(moq_version_t ver);
void test_window_streaming_bracket(moq_version_t ver);
void test_window_widen_midbracket(moq_version_t ver);
void test_window_unsatisfiable(moq_version_t ver);
void test_window_teardown_reuse(moq_version_t ver);
void test_window_finite_end_no_auto_finish(moq_version_t ver);
void test_window_fetch_independent(void);
void test_end_track_interrupted(moq_version_t ver, bool publication,
                                bool filter_cut, int phase);
void test_window_publish_updated(moq_version_t ver);
void test_window_end_track(moq_version_t ver, bool excluded);
void test_window_end_track_blocked(moq_version_t ver, bool wrote_first);
void test_window_end_track_forward_zero(moq_version_t ver);
void test_window_end_track_canonical_location(moq_version_t ver);
void test_window_end_track_ceiling(moq_version_t ver);
void test_exact_stream_counts(moq_version_t ver, bool use_stream);
void test_remove_track_exact_count(moq_version_t ver, bool use_stream);
void test_remove_track_publication_exact_count(moq_version_t ver,
                                               bool use_stream);
void test_remove_track_abandon_reset(moq_version_t ver, bool wire_shape);
void test_needs_reset_never_fin(moq_version_t ver, bool via_remove,
                                bool via_wire_check);
void test_streaming_needs_reset_termination(moq_version_t ver,
                                            bool via_remove);
void test_unpublish_recovery(moq_version_t ver);
void test_window_pending_op_narrow(moq_version_t ver, bool narrow_pub);

/* group history */
void test_retained_install_merges_monotonic_history(void);
void test_pub_history_reservation_and_merge(void);
void test_pub_history_capacity_at_add_track(void);
void test_pub_history_validate_before_mutate(void);
void test_pub_history_forward_zero_advances(void);
void test_pub_history_add_track_oom(void);
void test_pub_history_malformed_properties_d18(void);
void test_pub_history_exactly_once_retry(void);
void test_hist_identity_rules(moq_version_t ver);
void test_hist_d16_whole_group_fetch(void);
void test_hist_destination_close(moq_version_t ver);
void test_hist_end_of_track_status(moq_version_t ver);
void test_hist_capacity_sweep(moq_version_t ver);
void test_hist_end_window_completeness(void);
void test_hist_fetch_snapshot_failure(moq_version_t ver);
void test_hist_fetch_beyond_location_guard(moq_version_t ver);
void test_fetch_range_published(moq_version_t ver);
void test_hist_d16_fetch_staging(void);
void test_hist_internal_omission(moq_version_t ver);
void test_hist_retained_tail_displaced(void);
void test_hist_generation_identity(moq_version_t ver);
void test_hist_sparse_membership(moq_version_t ver);
void test_hist_membership_capacity(moq_version_t ver);
void test_hist_retained_subset_after_fin(moq_version_t ver);
void test_hist_fin_rollover(moq_version_t ver);
void test_hist_datagram_compacts_current(moq_version_t ver);
void test_hist_compact_stream(moq_version_t ver);

/* monotonic groups */
void test_mono_rejections(moq_version_t ver);
void test_mono_plain_regression(moq_version_t ver);
void test_mono_eog_open_vs_fin(moq_version_t ver);
void test_mono_reset_clears(moq_version_t ver);
void test_mono_crossed_and_blocked(moq_version_t ver, bool abandon);
void test_mono_group_advance(moq_version_t ver);
void test_mono_status_evidence(moq_version_t ver);
void test_mono_streaming_higher_dg(moq_version_t ver);
void test_mono_datagram_no_arm(moq_version_t ver);
void test_mono_abi_canary(moq_version_t ver);
void test_mono_declare_compose(moq_version_t ver);

/* namespace */
void test_invalid_namespace_inputs(void);
void test_namespace_accepted_then_remove(void);
void test_namespace_accepted_query(void);
void test_namespace_accepted_query_rejected(void);
void test_namespace_pending_remove_blocked(void);
void test_namespace_rejected_updates_state(void);
void test_namespace_terminal_readvertises(void);
void test_remove_pending_ns_with_open_sg(void);
void test_namespace_overflow(void);
void test_advertised_ns_duplicate(void);
void test_advertised_ns_reuse_after_terminal(void);
void test_ns_refcount_shared(void);
void test_ns_refcount_distinct(void);
void test_ns_refcount_would_block_retry(void);
void test_nsterm_rejected(moq_version_t ver);
void test_nsterm_cancelled(moq_version_t ver);
void test_nsterm_unmatched(void);
void test_nsterm_old_size_finished_preserved(moq_version_t ver);
void test_nsterm_abi_canary(void);

/* publish */
void test_publish_track_accepted(void);
void test_publish_track_write(void);
void test_end_track_cut(void);
void test_end_track_filter_cut(void);
void test_publish_end_track_eot(void);
void test_publish_and_subscribe_coexist(void);
void test_publish_restart_after_peer_unsubscribe_d16(void);
void test_publish_restart_d18_cancel(void);

/* streaming */
void test_write_object_ex_stream(void);
void test_write_object_ex_datagram(void);
void test_write_object_ex_status_datagram(void);
void test_write_object_ex_invalid_combos(void);
void test_streaming_lifecycle(void);
void test_streaming_invalid_order(void);
void test_remove_track_while_streaming(void);
void test_unsubscribe_then_write_noop(void);
void test_slot_reuse_after_unsub(void);
void test_second_subscriber_rejected_v1(void);
void test_unsub_clears_slot_and_resubscribe(void);
void test_session_rejects_same_track_duplicate(void);
void test_unsub_event_clears_slot(void);

/* subgroups */
void test_subgroup_header_mismatch_admission(void);
void test_facade_first_object(moq_version_t ver);

/* subscription update */
void test_update_callback_fires(void);
void test_update_old_callback_struct_size(void);
void test_pub_callbacks_old_prefix_copier_keeps_updated(void);
void test_pub_callbacks_nested_prefix_bounds_copy(void);
void test_pub_callbacks_partial_field_poison(void);
void test_update_after_unsubscribe_ignored(void);
void test_update_during_deferred(void);
void test_old_cfg_size_callbacks_still_fire(void);

/* tick */
void test_tick_subscribe_accept(void);
void test_pub_create_partial_callbacks(void);
void test_pub_streaming_no_subscriber_noop(void);
void test_tick_unsubscribe_clears(void);
void test_tick_goaway_draining(void);
void test_tick_session_closed(void);
void test_tick_pending_retry_fires_joined(void);
void test_tick_pending_then_closed(void);

/* track lifecycle */
void test_strict_alloc_sizes(void);
void test_reject_all_mode(void);
void test_duplicate_track_rejected(void);
void test_reset_group(void);
void test_flush_no_pending(void);
void test_flush_accept_would_block(void);
void test_flush_reject_would_block(void);
void test_remove_track_pending_accept(void);
void test_create_invalid_cfg(void);
void test_cross_publisher_track(void);
void test_pending_does_not_block_unmatched(void);
void test_end_group_stale_handle(void);
void test_remove_track_reset_would_block(void);
void test_destroy_with_active_pending(void);

#if defined(__GNUC__) || defined(__clang__)
#pragma GCC visibility pop
#endif

#endif /* PUBLISHER_TEST_CASES_H */
