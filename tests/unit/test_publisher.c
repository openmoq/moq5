#include "publisher/test_cases.h"

int main(void) {
    test_create_destroy();
    test_pub_cfg_init_old_prefix_no_overflow();
    test_pub_callbacks_init_old_prefix_no_overflow();
    test_add_remove_track();
    test_subscribe_accept_all();
    test_remove_track_retires_subscription();
    test_subscribe_no_match();
    test_subscribe_duplicate_rejected();
    test_write_single_sub();
    test_end_track();
    test_end_track_with_stream_keys();
    test_finish_subscribers();
    test_finish_subscribers_skips_publication();
    test_finish_subscribers_pub_mid_object();
    test_publish_restart_after_peer_unsubscribe_d16();
    test_publish_restart_d18_cancel();
    test_retained_install_merges_monotonic_history();
    test_pub_history_reservation_and_merge();
    test_pub_history_capacity_at_add_track();
    test_pub_history_validate_before_mutate();
    test_pub_history_add_track_oom();
    test_pub_history_malformed_properties_d18();
    test_pub_history_forward_zero_advances();
    test_pub_history_exactly_once_retry();
    test_pub_manual_forward_zero_pauses();
    test_pub_manual_broadcasts_and_results();
    test_pub_manual_publish_ok();
    test_pub_manual_unsubscribe_active();
    test_pub_manual_vs_tick_parity();
    test_pub_manual_forward_zero_resets_open_subgroup();
    test_pub_manual_publish_family_results();
    test_pub_manual_two_track_serialized_no_loss();
    test_pub_manual_multiple_owned_answers();
    test_pub_manual_publish_error_matched();
    test_nsterm_unmatched();
    test_nsterm_abi_canary();
    test_window_membership_matrix(MOQ_VERSION_DRAFT_16);
    test_window_membership_matrix(MOQ_VERSION_DRAFT_18);
    test_window_stream_objects(MOQ_VERSION_DRAFT_16);
    test_window_stream_objects(MOQ_VERSION_DRAFT_18);
    test_window_publication_and_coexist(MOQ_VERSION_DRAFT_16, true);
    test_window_publication_and_coexist(MOQ_VERSION_DRAFT_16, false);
    test_window_publication_and_coexist(MOQ_VERSION_DRAFT_18, true);
    test_window_publication_and_coexist(MOQ_VERSION_DRAFT_18, false);
    test_window_fully_filtered_history(MOQ_VERSION_DRAFT_16);
    test_window_fully_filtered_history(MOQ_VERSION_DRAFT_18);
    for (int v = 0; v < 2; v++) {
        moq_version_t ver = v ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_16;
        test_declare_b_rejections(ver);
        test_declare_pending_and_merge(ver);
        test_declare_completion_basic(ver);
        test_declare_publication_completes(ver);
        test_declare_blocked_steps(ver, false);
        test_declare_blocked_steps(ver, true);
        test_declare_fin_before_done(ver);
        test_declare_install_immediate(ver, false);
        test_declare_install_immediate(ver, true);
        test_declare_install_deferred_and_pending(ver);
        test_declare_update_paths(ver);
        test_declare_after_end_track(ver);
        test_declare_fetch_below_watermark(ver);
        test_declare_plain_track_unchanged(ver);
        test_declare_stale_completion(ver, false);
        test_declare_stale_completion(ver, true);
        test_declare_stale_armed_reset(ver);
        test_declare_end_track_reconcile(ver);
        test_declare_end_track_ceiling(ver);
        test_declare_end_track_blocked_preflight(ver);
        test_nsterm_rejected(ver);
        test_nsterm_cancelled(ver);
        test_nsterm_old_size_finished_preserved(ver);
        test_mono_rejections(ver);
        test_mono_plain_regression(ver);
        test_mono_eog_open_vs_fin(ver);
        test_mono_reset_clears(ver);
        test_mono_crossed_and_blocked(ver, false);
        test_mono_crossed_and_blocked(ver, true);
        test_mono_group_advance(ver);
        test_mono_status_evidence(ver);
        test_mono_streaming_higher_dg(ver);
        test_mono_datagram_no_arm(ver);
        test_mono_abi_canary(ver);
        test_mono_declare_compose(ver);
    }
    test_window_narrow_resets_open_subgroup(MOQ_VERSION_DRAFT_16);
    test_window_narrow_resets_open_subgroup(MOQ_VERSION_DRAFT_18);
    test_window_streaming_bracket(MOQ_VERSION_DRAFT_16);
    test_window_streaming_bracket(MOQ_VERSION_DRAFT_18);
    test_window_widen_midbracket(MOQ_VERSION_DRAFT_16);
    test_window_widen_midbracket(MOQ_VERSION_DRAFT_18);
    test_window_unsatisfiable(MOQ_VERSION_DRAFT_16);
    test_window_unsatisfiable(MOQ_VERSION_DRAFT_18);
    test_window_teardown_reuse(MOQ_VERSION_DRAFT_16);
    test_window_teardown_reuse(MOQ_VERSION_DRAFT_18);
    test_window_finite_end_no_auto_finish(MOQ_VERSION_DRAFT_16);
    test_window_finite_end_no_auto_finish(MOQ_VERSION_DRAFT_18);
    test_window_fetch_independent();
    test_window_publish_updated(MOQ_VERSION_DRAFT_16);
    test_window_publish_updated(MOQ_VERSION_DRAFT_18);
    test_window_end_track(MOQ_VERSION_DRAFT_16, true);
    test_window_end_track(MOQ_VERSION_DRAFT_16, false);
    test_window_end_track(MOQ_VERSION_DRAFT_18, true);
    test_window_end_track(MOQ_VERSION_DRAFT_18, false);
    test_window_end_track(MOQ_VERSION_DRAFT_22, true);
    test_window_end_track(MOQ_VERSION_DRAFT_22, false);
    test_window_completion_status(MOQ_VERSION_DRAFT_16);
    test_window_completion_status(MOQ_VERSION_DRAFT_18);
    test_window_completion_status(MOQ_VERSION_DRAFT_22);
    test_facade_first_object(MOQ_VERSION_DRAFT_16);
    test_facade_first_object(MOQ_VERSION_DRAFT_18);
    test_facade_first_object(MOQ_VERSION_DRAFT_22);
    test_hist_identity_rules(MOQ_VERSION_DRAFT_16);
    test_hist_identity_rules(MOQ_VERSION_DRAFT_18);
    test_hist_identity_rules(MOQ_VERSION_DRAFT_22);
    test_hist_d16_whole_group_fetch();
    test_hist_end_window_completeness();
    test_hist_d16_fetch_staging();
    test_hist_fetch_snapshot_failure(MOQ_VERSION_DRAFT_16);
    test_hist_fetch_snapshot_failure(MOQ_VERSION_DRAFT_18);
    test_hist_fetch_snapshot_failure(MOQ_VERSION_DRAFT_22);
    test_hist_fetch_beyond_location_guard(MOQ_VERSION_DRAFT_18);
    test_hist_fetch_beyond_location_guard(MOQ_VERSION_DRAFT_22);
    test_fetch_range_published(MOQ_VERSION_DRAFT_16);
    test_fetch_range_published(MOQ_VERSION_DRAFT_18);
    test_fetch_range_published(MOQ_VERSION_DRAFT_22);
    test_hist_internal_omission(MOQ_VERSION_DRAFT_16);
    test_hist_internal_omission(MOQ_VERSION_DRAFT_18);
    test_hist_internal_omission(MOQ_VERSION_DRAFT_22);
    test_hist_retained_tail_displaced();
    test_hist_generation_identity(MOQ_VERSION_DRAFT_16);
    test_hist_generation_identity(MOQ_VERSION_DRAFT_18);
    test_hist_generation_identity(MOQ_VERSION_DRAFT_22);
    test_hist_sparse_membership(MOQ_VERSION_DRAFT_16);
    test_hist_sparse_membership(MOQ_VERSION_DRAFT_18);
    test_hist_sparse_membership(MOQ_VERSION_DRAFT_22);
    for (int v = 0; v < 3; v++) {
        moq_version_t mv = v == 0 ? MOQ_VERSION_DRAFT_16
                         : v == 1 ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_22;
        test_hist_membership_capacity(mv);
        test_hist_retained_subset_after_fin(mv);
        test_hist_fin_rollover(mv);
        test_hist_datagram_compacts_current(mv);
        test_hist_compact_stream(mv);
    }
    for (int v = 0; v < 3; v++) {
        moq_version_t hv = v == 0 ? MOQ_VERSION_DRAFT_16
                         : v == 1 ? MOQ_VERSION_DRAFT_18 : MOQ_VERSION_DRAFT_22;
        test_hist_destination_close(hv);
        test_hist_end_of_track_status(hv);
        test_hist_capacity_sweep(hv);
    }
    test_window_end_track_blocked(MOQ_VERSION_DRAFT_16, true);
    test_window_end_track_blocked(MOQ_VERSION_DRAFT_16, false);
    test_window_end_track_blocked(MOQ_VERSION_DRAFT_18, true);
    test_window_end_track_blocked(MOQ_VERSION_DRAFT_18, false);
    test_window_end_track_ceiling(MOQ_VERSION_DRAFT_16);
    test_window_end_track_ceiling(MOQ_VERSION_DRAFT_18);
    test_exact_stream_counts(MOQ_VERSION_DRAFT_16, true);
    test_exact_stream_counts(MOQ_VERSION_DRAFT_16, false);
    test_exact_stream_counts(MOQ_VERSION_DRAFT_18, true);
    test_exact_stream_counts(MOQ_VERSION_DRAFT_18, false);
    test_remove_track_abandon_reset(MOQ_VERSION_DRAFT_16, false);
    test_remove_track_abandon_reset(MOQ_VERSION_DRAFT_16, true);
    test_remove_track_abandon_reset(MOQ_VERSION_DRAFT_18, false);
    test_remove_track_abandon_reset(MOQ_VERSION_DRAFT_18, true);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_16, true, false);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_16, false, false);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_16, false, true);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_18, true, false);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_18, false, false);
    test_needs_reset_never_fin(MOQ_VERSION_DRAFT_18, false, true);
    test_streaming_needs_reset_termination(MOQ_VERSION_DRAFT_16, true);
    test_streaming_needs_reset_termination(MOQ_VERSION_DRAFT_16, false);
    test_streaming_needs_reset_termination(MOQ_VERSION_DRAFT_18, true);
    test_streaming_needs_reset_termination(MOQ_VERSION_DRAFT_18, false);
    test_unpublish_recovery(MOQ_VERSION_DRAFT_16);
    test_unpublish_recovery(MOQ_VERSION_DRAFT_18);
    test_remove_track_publication_exact_count(MOQ_VERSION_DRAFT_16, true);
    test_remove_track_publication_exact_count(MOQ_VERSION_DRAFT_16, false);
    test_remove_track_publication_exact_count(MOQ_VERSION_DRAFT_18, true);
    test_remove_track_publication_exact_count(MOQ_VERSION_DRAFT_18, false);
    test_remove_track_exact_count(MOQ_VERSION_DRAFT_16, true);
    test_remove_track_exact_count(MOQ_VERSION_DRAFT_16, false);
    test_remove_track_exact_count(MOQ_VERSION_DRAFT_18, true);
    test_remove_track_exact_count(MOQ_VERSION_DRAFT_18, false);
    test_window_end_track_forward_zero(MOQ_VERSION_DRAFT_16);
    test_window_end_track_forward_zero(MOQ_VERSION_DRAFT_18);
    test_window_end_track_canonical_location(MOQ_VERSION_DRAFT_16);
    test_window_end_track_canonical_location(MOQ_VERSION_DRAFT_18);
    test_window_pending_op_narrow(MOQ_VERSION_DRAFT_16, false);
    test_window_pending_op_narrow(MOQ_VERSION_DRAFT_16, true);
    test_window_pending_op_narrow(MOQ_VERSION_DRAFT_18, false);
    test_window_pending_op_narrow(MOQ_VERSION_DRAFT_18, true);
    test_pub_manual_close_releases_pending_fetch();
    test_finish_subscribers_not_terminal();
    test_finish_subscribers_would_block();
    test_write_no_subs();
    test_write_group_change();
    test_end_group();
    test_remove_track_with_open_sg();
    test_destroy_with_live_subs();
    test_event_ignored();
    test_session_closed_event();
    test_invalid_namespace_inputs();
    test_strict_alloc_sizes();
    test_reject_all_mode();
    test_duplicate_track_rejected();
    test_namespace_accepted_then_remove();
    test_reset_group();
    test_namespace_accepted_query();
    test_namespace_accepted_query_rejected();
    test_namespace_pending_remove_blocked();
    test_namespace_rejected_updates_state();
    test_namespace_terminal_readvertises();
    test_flush_no_pending();
    test_flush_accept_would_block();
    test_flush_reject_would_block();
    test_remove_pending_ns_with_open_sg();
    test_remove_track_pending_accept();
    test_create_invalid_cfg();
    test_cross_publisher_track();
    test_namespace_overflow();
    test_advertised_ns_duplicate();
    test_pending_does_not_block_unmatched();
    test_end_group_stale_handle();
    test_remove_track_reset_would_block();
    test_advertised_ns_reuse_after_terminal();
    test_destroy_with_active_pending();
    test_callback_accept();
    test_callback_reject_custom_error();
    test_callback_reject_default_error();
    test_callback_sees_filter_and_ns();
    test_callback_sees_tokens();
    test_callback_would_block();
    test_callback_null_rejected();
    test_callback_invalid_decision();
    test_callback_not_called_for_duplicate();
    test_old_struct_size_accept_all();
    test_publisher_priority_zero();
    test_publisher_priority_default();
    test_publisher_priority_old_struct();
    test_pub_track_cfg_init_canary();
    test_oversized_namespace_rejected();
    test_oversized_full_track_name_rejected();
    test_tick_subscribe_accept();
    test_pub_create_partial_callbacks();
    test_pub_streaming_no_subscriber_noop();
    test_tick_unsubscribe_clears();
    test_tick_goaway_draining();
    test_tick_session_closed();
    test_tick_pending_retry_fires_joined();
    test_tick_pending_then_closed();
    test_write_object_ex_stream();
    test_write_object_ex_datagram();
    test_write_object_ex_status_datagram();
    test_write_object_ex_invalid_combos();
    test_streaming_lifecycle();
    test_streaming_invalid_order();
    test_remove_track_while_streaming();
    test_unsubscribe_then_write_noop();
    test_slot_reuse_after_unsub();
    test_unsub_clears_slot_and_resubscribe();
    test_second_subscriber_rejected_v1();
    test_session_rejects_same_track_duplicate();
    test_unsub_event_clears_slot();
    test_unsubscribed_retires_slot();
    test_deferred_accept();
    test_deferred_reject();
    test_deferred_resolution_keeps_multiple_owners();
    test_end_track_rejects_late_subscribe();
    test_end_track_rejects_deferred_subscribe();
    test_deferred_would_block_then_flush();
    test_deferred_remove_track_blocked();
    test_deferred_session_close_releases();
    test_deferred_does_not_block_other_tracks();
    test_deferred_unsub_before_resolve();
    test_deferred_stale_id_after_reuse();
    test_deferred_flush_fires_joined();
    test_deferred_old_cfg_struct_size();
    test_update_callback_fires();
    test_update_old_callback_struct_size();
    test_pub_callbacks_old_prefix_copier_keeps_updated();
    test_pub_callbacks_nested_prefix_bounds_copy();
    test_pub_callbacks_partial_field_poison();
    test_update_after_unsubscribe_ignored();
    test_update_during_deferred();
    test_old_cfg_size_callbacks_still_fire();
    test_ns_refcount_shared();
    test_ns_refcount_distinct();
    test_ns_refcount_would_block_retry();
    test_publish_track_accepted();
    test_publish_track_write();
    test_publish_end_track_eot();
    test_end_track_cut();
    test_end_track_filter_cut();
    for (size_t v = 0; v < 3; v++) {
        const moq_version_t versions[] = { MOQ_VERSION_DRAFT_16, MOQ_VERSION_DRAFT_18,
                                           MOQ_VERSION_DRAFT_22 };
        for (int phase = 1; phase <= 2; phase++) {
            test_end_track_interrupted(versions[v], true, false, phase);
            test_end_track_interrupted(versions[v], false, false, phase);
            test_end_track_interrupted(versions[v], true, true, phase);
        }
    }
    test_publication_end_status(MOQ_VERSION_DRAFT_16);
    test_publication_end_status(MOQ_VERSION_DRAFT_18);
    test_publication_end_status(MOQ_VERSION_DRAFT_22);
    test_publish_and_subscribe_coexist();
    test_fanout_delivery_sub_first();
    test_fanout_delivery_pub_first();
    test_fanout_second_dest_would_block();
    test_fanout_mid_op_joiner_excluded();
    test_fanout_forward_zero_subscription();
    test_fanout_old_sized_object_cfg();
    test_fanout_publication_forward_drop();
    test_fanout_close_releases_pending_op();
    test_fanout_forward_enable_mid_op_joins_next();
    test_pub_object_cfg_init_old_prefix_no_overflow();
    test_fanout_drop_restore_excluded_from_pending();
    test_fanout_forward_drop_resets_eog_subgroup();

    /* == end_of_group ABI: old struct_size must not read end_of_group == */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);

        moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);

        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("eog_abi");
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("eog_abi");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        /* Old caller: struct_size excludes end_of_group. Dirty padding. */
        moq_pub_object_cfg_t obj;
        memset(&obj, 0xFF, sizeof(obj));
        obj.struct_size = offsetof(moq_pub_object_cfg_t, _reserved_obj);
        obj.group_id = 0; obj.object_id = 0;
        obj.datagram = false; obj.has_status = false;
        uint8_t data[] = "hello";
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(&alloc, data, 5, &pl);
        obj.payload = pl; obj.properties = NULL;

        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);

        /* Verify EOG bit (0x08) is NOT set despite garbage padding. */
        moq_action_t act;
        MOQ_TEST_CHECK(moq_session_poll_actions(moq_simpair_server(sp), &act, 1) == 1);
        MOQ_TEST_CHECK(act.kind == MOQ_ACTION_SEND_DATA);
        MOQ_TEST_CHECK((act.u.send_data.header[0] & 0x08) == 0);
        moq_action_cleanup(&act);
        while (moq_session_poll_actions(moq_simpair_server(sp), &act, 1) == 1)
            moq_action_cleanup(&act);

        moq_rcbuf_decref(pl);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("end_of_group ABI old struct_size");
    }

    /* == end_of_group mismatch: same-group true->false rejected ======= */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);

        moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);

        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("eog_mm1");
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("eog_mm1");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        uint8_t data[] = "hi";
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl);

        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.group_id = 0; obj.object_id = 0;
        obj.payload = pl; obj.end_of_group = true;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);

        drain_all(sp);

        /* Same group, object 1 with end_of_group=false: mismatch. */
        moq_rcbuf_t *pl2 = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl2);
        moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.group_id = 0; obj.object_id = 1;
        obj.payload = pl2; obj.end_of_group = false;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

        moq_rcbuf_decref(pl); moq_rcbuf_decref(pl2);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("end_of_group mismatch true->false");
    }

    /* == end_of_group mismatch: same-group false->true rejected ======= */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);

        moq_pub_cfg_t pcfg; moq_pub_cfg_init_sized(&pcfg, sizeof(pcfg));
        pcfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &pcfg, &pub);

        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("live") };
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("eog_mm2");
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, moq_simpair_now_us(sp), &track);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("eog_mm2");
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg,
            moq_simpair_now_us(sp), &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        moq_pub_tick(pub, moq_simpair_now_us(sp));
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          while (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1)
              moq_event_cleanup(&ev); }

        uint8_t data[] = "hi";
        moq_rcbuf_t *pl = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl);

        moq_pub_object_cfg_t obj; moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.group_id = 0; obj.object_id = 0;
        obj.payload = pl; obj.end_of_group = false;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_OK);

        drain_all(sp);

        moq_rcbuf_t *pl2 = NULL;
        moq_rcbuf_create(&alloc, data, 2, &pl2);
        moq_pub_object_cfg_init_sized(&obj, sizeof(obj));
        obj.group_id = 0; obj.object_id = 1;
        obj.payload = pl2; obj.end_of_group = true;
        MOQ_TEST_CHECK(moq_pub_write_object_ex(pub, track, &obj,
            moq_simpair_now_us(sp)) == MOQ_ERR_WRONG_STATE);

        moq_rcbuf_decref(pl); moq_rcbuf_decref(pl2);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("end_of_group mismatch false->true");
    }

    test_subgroup_header_mismatch_admission();

    /* -- Retained catalog object answered via spec Joining FETCH -------- *
     * The origin advertises its retained object as the subscription's Largest
     * (so a joining FETCH is valid) and answers the FETCH from the retained
     * catalog cache. The spec mechanism is an explicit Joining FETCH; a plain
     * SUBSCRIBE delivers no retained objects. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        /* Register the retained catalog object at (group 0, object 0). */
        static const uint8_t catalog[] = { 'C','A','T', 0, 1, 2, 3, 4 };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, catalog, sizeof(catalog), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &st) == MOQ_OK);
        moq_rcbuf_decref(payload);   /* set_retained_group retained its own ref */

        /* Client subscribes (LargestObject): SUBSCRIBE_OK advertises the
         * retained object as Largest, making a joining FETCH valid. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        /* Drain SUBSCRIBE_OK (a plain SUBSCRIBE delivers no retained objects). */
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* Client issues a Relative Joining FETCH(offset=0) for the catalog. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* Publisher answers the FETCH from the retained object (tick path). */
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        bool got_object = false, got_complete = false;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0) {
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    got_object = true;
                    MOQ_TEST_CHECK(evts[i].u.fetch_object.group_id == 0);
                    MOQ_TEST_CHECK(evts[i].u.fetch_object.object_id == 0);
                    MOQ_TEST_CHECK(evts[i].u.fetch_object.payload != NULL &&
                        moq_rcbuf_len(evts[i].u.fetch_object.payload) == sizeof(catalog) &&
                        memcmp(moq_rcbuf_data(evts[i].u.fetch_object.payload),
                               catalog, sizeof(catalog)) == 0);
                } else if (evts[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                    got_complete = true;
                }
                moq_event_cleanup(&evts[i]);
            }
        }
        MOQ_TEST_CHECK(got_object);
        MOQ_TEST_CHECK(got_complete);

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained catalog joining fetch");
    }

    /* -- Retained FETCH rejects: standalone unsupported; no retained object - */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* Subscribe (LargestObject) so a Largest is advertised and a joining
         * FETCH is valid. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* A standalone FETCH is served from the retained cache, but only when
         * its explicit range covers object 0..last (see the standalone-replay
         * regression below). These negative cases must still be rejected.
         *
         * Phase 1 pumps WHILE the retained group is present (so the range and
         * unknown-track rejections are evaluated against a live cache); phase 2
         * clears it first (the no-retained-group rejections). */

        /* (a) Standalone FETCH starting past the Largest Object {0, 0} ->
         * INVALID_RANGE. [1,0)->[2,0) names nothing published. */
        moq_fetch_cfg_t f1; moq_fetch_cfg_init(&f1);
        f1.track_namespace.parts = ns_parts; f1.track_namespace.count = 1;
        f1.track_name = MOQ_BYTES_LITERAL("catalog");
        f1.start_group = 1; f1.start_object = 0; f1.end_group = 2; f1.end_object = 0;
        moq_fetch_t fh1;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &f1, now, &fh1) == MOQ_OK);

        /* (a2) Standalone FETCH for an UNKNOWN track -> DOES_NOT_EXIST. */
        moq_fetch_cfg_t f1b; moq_fetch_cfg_init(&f1b);
        f1b.track_namespace.parts = ns_parts; f1b.track_namespace.count = 1;
        f1b.track_name = MOQ_BYTES_LITERAL("nope");
        f1b.start_group = 0; f1b.start_object = 0; f1b.end_group = 0; f1b.end_object = 1;
        moq_fetch_t fh1b;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &f1b, now, &fh1b) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        bool got_invalid_range = false, got_does_not_exist = false, got_object = false;
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (evts[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      moq_request_error_t ec = evts[i].u.fetch_error.error_code;
                      if (ec == MOQ_REQUEST_ERROR_INVALID_RANGE) got_invalid_range = true;
                      if (ec == MOQ_REQUEST_ERROR_DOES_NOT_EXIST) got_does_not_exist = true;
                  } else if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      got_object = true;
                  }
                  moq_event_cleanup(&evts[i]);
              } }
        MOQ_TEST_CHECK(got_invalid_range);   /* (a) start past the Largest */
        MOQ_TEST_CHECK(got_does_not_exist);  /* (a2) unknown track */
        MOQ_TEST_CHECK(!got_object);         /* nothing served by the rejects */

        /* Phase 2: clear the retained group. The Largest was advertised before
         * the clear, so a joining FETCH is still issuable; the origin simply has
         * nothing to serve. Both a joining and a (range-covering) standalone
         * FETCH now resolve to DOES_NOT_EXIST. */
        moq_pub_clear_retained_group(pub, track);

        /* (b) Joining FETCH after the clear -> DOES_NOT_EXIST. */
        moq_fetch_cfg_t f2; moq_fetch_cfg_init(&f2);
        f2.is_joining = true; f2.joining_relative = true; f2.joining_start = 0;
        f2.joining_sub = sub_h;
        moq_fetch_t fh2;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &f2, now, &fh2) == MOQ_OK);

        /* (c) Standalone FETCH after the clear (no retained group) ->
         * DOES_NOT_EXIST, even with a range that would otherwise have covered. */
        moq_fetch_cfg_t f3; moq_fetch_cfg_init(&f3);
        f3.track_namespace.parts = ns_parts; f3.track_namespace.count = 1;
        f3.track_name = MOQ_BYTES_LITERAL("catalog");
        f3.start_group = 0; f3.start_object = 0; f3.end_group = 0; f3.end_object = 1;
        moq_fetch_t fh3;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &f3, now, &fh3) == MOQ_OK);

        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        int cleared_errors = 0; bool only_does_not_exist = true, served_after_clear = false;
        { moq_event_t evts[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (evts[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      cleared_errors++;
                      if (evts[i].u.fetch_error.error_code != MOQ_REQUEST_ERROR_DOES_NOT_EXIST)
                          only_does_not_exist = false;
                  } else if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      served_after_clear = true;
                  }
                  moq_event_cleanup(&evts[i]);
              } }
        MOQ_TEST_CHECK(cleared_errors == 2);     /* (b) joining + (c) standalone */
        MOQ_TEST_CHECK(only_does_not_exist);
        MOQ_TEST_CHECK(!served_after_clear);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained catalog fetch rejects");
    }

    /* -- Standalone FETCH replays the retained catalog --- *
     * Captured from moqx: after SUBSCRIBE_OK advertises Largest=[0,0], the relay
     * pulls the retained catalog with a STANDALONE FETCH (type 1) -- explicit
     * namespace/name + range [0,0)->[0,1) -- NOT a Joining FETCH. The retained
     * cache resolves the track by namespace/name and serves the whole retained
     * group from that range (object 0..last), so a catalog-driven player behind
     * a relay discovers its tracks. The range covers the whole one-object group;
     * a standalone range that omitted object 0 would be rejected (see the
     * "retained catalog fetch rejects" block above). */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* Subscribe (LargestObject) so SUBSCRIBE_OK advertises Largest=[0,0]. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* SUBSCRIBE_OK advertises Largest = [0,0] (this part already works). */
        bool saw_largest = false;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_SUBSCRIBE_OK &&
                      d[i].u.subscribe_ok.has_largest &&
                      d[i].u.subscribe_ok.largest_group == 0 &&
                      d[i].u.subscribe_ok.largest_object == 0)
                      saw_largest = true;
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(saw_largest);   /* Largest=[0,0] advertised */

        /* STANDALONE FETCH for the catalog, range [0,0]->[0,1] (the moqx shape).
         * is_joining stays false; the range covers the whole one-object group. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace.parts = ns_parts; fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 0; fcfg.end_object = 1;   /* covers object 0 */
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        /* The retained catalog object 0 is served and the fetch completes, with
         * no error. */
        int objs = 0; bool payload_ok = false, complete = false, errored = false;
        moq_request_error_t err_code = 0;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      if (d[i].u.fetch_object.group_id == 0 &&
                          d[i].u.fetch_object.object_id == 0 &&
                          d[i].u.fetch_object.payload &&
                          moq_rcbuf_len(d[i].u.fetch_object.payload) == sizeof(cat) &&
                          memcmp(moq_rcbuf_data(d[i].u.fetch_object.payload),
                                 cat, sizeof(cat)) == 0)
                          payload_ok = true;
                      objs++;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                      complete = true;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      errored = true; err_code = d[i].u.fetch_error.error_code;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        (void)err_code;
        MOQ_TEST_CHECK(!errored);    /* no FETCH_ERROR */
        MOQ_TEST_CHECK(objs == 1);   /* exactly the one retained object served */
        MOQ_TEST_CHECK(payload_ok);  /* catalog object 0 payload "XYZ" */
        MOQ_TEST_CHECK(complete);    /* fetch completes */

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained_group_standalone_fetch_replays_catalog");
    }

    /* -- Standalone retained FETCH authorization (REJECT_ALL) -------------- *
     * A REJECT_ALL publisher holds a retained catalog but never accepts a
     * subscription. A standalone FETCH that names the track and whose range
     * covers the whole retained group must NOT serve the retained objects: it
     * is rejected UNAUTHORIZED with zero objects. Without this gate the catalog
     * would leak to anyone who knows the track name. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_REJECT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* No SUBSCRIBE. Standalone FETCH for the catalog, range covering the
         * whole retained group [0,0]->[0,1]. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace.parts = ns_parts; fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 0; fcfg.end_object = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        int objs = 0; bool unauthorized = false; bool other_error = false;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      objs++;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      if (d[i].u.fetch_error.error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED)
                          unauthorized = true;
                      else
                          other_error = true;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(unauthorized);   /* rejected UNAUTHORIZED */
        MOQ_TEST_CHECK(!other_error);   /* not DOES_NOT_EXIST etc. */
        MOQ_TEST_CHECK(objs == 0);      /* no retained object leaked */

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained_standalone_fetch_unauthorized_reject_all");
    }

    /* -- Standalone retained FETCH authorization (CALLBACK, no accept) ----- *
     * A CALLBACK publisher holds a retained catalog. A standalone FETCH arrives
     * with NO prior accepted subscription. It must be rejected UNAUTHORIZED
     * (zero objects), and the FETCH path must NOT invoke the subscribe callback
     * (authorization is a pure state check). */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        cb_capture_t cap; memset(&cap, 0, sizeof(cap));
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_CALLBACK;
        cfg.on_subscribe = cb_capture;
        cfg.on_subscribe_ctx = &cap;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* No SUBSCRIBE (so the callback never had a chance to accept). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace.parts = ns_parts; fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 0; fcfg.end_object = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        int objs = 0; bool unauthorized = false; bool other_error = false;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      objs++;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      if (d[i].u.fetch_error.error_code == MOQ_REQUEST_ERROR_UNAUTHORIZED)
                          unauthorized = true;
                      else
                          other_error = true;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(unauthorized);   /* rejected UNAUTHORIZED */
        MOQ_TEST_CHECK(!other_error);
        MOQ_TEST_CHECK(objs == 0);      /* no retained object leaked */
        MOQ_TEST_CHECK(!cap.called);    /* FETCH path did not invoke on_subscribe */

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained_standalone_fetch_unauthorized_callback");
    }

    /* -- Standalone retained FETCH authorization (CALLBACK accept) --------- *
     * Compatibility with the relay flow: a CALLBACK publisher accepts a
     * SUBSCRIBE via callback, then a moqx-shaped standalone FETCH pulls the
     * retained catalog. The accepted subscription authorizes the standalone
     * FETCH, so the retained object IS served. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_CALLBACK;
        cfg.on_subscribe = cb_accept_all;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        static const uint8_t cat[] = { 'X','Y','Z' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        /* SUBSCRIBE accepted via callback -> track gains an accepted sub. */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_has_subscriber(pub, track));   /* accepted */
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* moqx-shaped standalone FETCH for the catalog (is_joining stays false). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.track_namespace.parts = ns_parts; fcfg.track_namespace.count = 1;
        fcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        fcfg.start_group = 0; fcfg.start_object = 0;
        fcfg.end_group = 0; fcfg.end_object = 1;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);

        int objs = 0; bool payload_ok = false, complete = false, errored = false;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                      if (d[i].u.fetch_object.group_id == 0 &&
                          d[i].u.fetch_object.object_id == 0 &&
                          d[i].u.fetch_object.payload &&
                          moq_rcbuf_len(d[i].u.fetch_object.payload) == sizeof(cat) &&
                          memcmp(moq_rcbuf_data(d[i].u.fetch_object.payload),
                                 cat, sizeof(cat)) == 0)
                          payload_ok = true;
                      objs++;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                      complete = true;
                  } else if (d[i].kind == MOQ_EVENT_FETCH_ERROR) {
                      errored = true;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(!errored);    /* authorized: no FETCH_ERROR */
        MOQ_TEST_CHECK(objs == 1);   /* retained object served */
        MOQ_TEST_CHECK(payload_ok);  /* catalog object 0 payload "XYZ" */
        MOQ_TEST_CHECK(complete);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained_standalone_fetch_authorized_callback_accept");
    }

    /* -- Retained FETCH survives a clear between accept and write ---------- *
     * Constrain action capacity (max_actions=3) so the serve accepts the FETCH
     * then defers the object write. Clear the retained object before the retry;
     * the response must still complete from the snapshot taken at stage time. */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_simpair_cfg_t spcfg = MOQ_SIMPAIR_CFG_INIT;
        spcfg.alloc = &alloc; spcfg.seed = 7; spcfg.initial_now_us = 1000;
        spcfg.client_send_request_capacity = true;
        spcfg.client_initial_request_capacity = 16;
        spcfg.server_send_request_capacity = true;
        spcfg.server_initial_request_capacity = 16;
        spcfg.max_actions = 3;   /* accept (2 actions) fits; write+FIN must defer */
        moq_simpair_t *sp = NULL;
        moq_simpair_create(&spcfg, &sp);
        moq_simpair_start(sp);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev);
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        static const uint8_t cat[] = { 'S','N','A','P' };
        moq_rcbuf_t *payload = NULL;
        moq_rcbuf_create(&alloc, cat, sizeof(cat), &payload);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = payload };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(payload);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        /* Drive the publisher (subscribe accept). */
        for (int i = 0; i < 8; i++) {
            moq_pub_tick(pub, now);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
        }
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* Joining FETCH; first serve accepts then defers the write. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 16, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_ERR_WOULD_BLOCK); /* accepted, write deferred */
        moq_simpair_run_until_quiescent(sp, 16, NULL);

        /* Clear the retained object while the fetch is accepted-but-unwritten. */
        moq_pub_clear_retained_group(pub, track);

        /* Subsequent ticks complete the write+FIN from the snapshot. */
        for (int i = 0; i < 8; i++) {
            moq_pub_tick(pub, now);
            moq_simpair_run_until_quiescent(sp, 16, NULL);
        }

        bool got_object = false, got_complete = false;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0) {
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    got_object = true;
                    MOQ_TEST_CHECK(evts[i].u.fetch_object.payload != NULL &&
                        moq_rcbuf_len(evts[i].u.fetch_object.payload) == sizeof(cat) &&
                        memcmp(moq_rcbuf_data(evts[i].u.fetch_object.payload),
                               cat, sizeof(cat)) == 0);
                } else if (evts[i].kind == MOQ_EVENT_FETCH_COMPLETE) {
                    got_complete = true;
                }
                moq_event_cleanup(&evts[i]);
            }
        }
        MOQ_TEST_CHECK(got_object);    /* snapshot delivered despite the clear */
        MOQ_TEST_CHECK(got_complete);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained catalog fetch survives clear");
    }

    /* -- Largest gating: a track that wrote a live object must NOT advertise
     * its retained object as Largest (no stale-largest reintroduced) ------- */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        /* A retained object is set... */
        static const uint8_t cat[] = { 'C','A','T' };
        moq_rcbuf_t *cp = NULL; moq_rcbuf_create(&alloc, cat, sizeof(cat), &cp);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = cp };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = 0; st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(cp);

        /* ...but then a live object is written successfully (no subscriber yet:
         * the no-slot OK path marks the track live). The caller retains the
         * payload ref on the no-sub path. */
        static const uint8_t live[] = { 'L','I','V','E' };
        moq_rcbuf_t *lp = NULL; moq_rcbuf_create(&alloc, live, sizeof(live), &lp);
        MOQ_TEST_CHECK(moq_pub_write_object(pub, track, 1, 0, lp, now) == MOQ_OK);
        moq_rcbuf_decref(lp);

        /* Subscribe (LargestObject): the stale RETAINED object (group 0) must not
         * be advertised as Largest -- but the LIVE object written at (1,0) was
         * observed by the registry (Layer B feed) and, per the accept snapshot
         *"), IS advertised as the true
         * Largest. So SUBSCRIBE_OK carries (1,0), not (0,0). */
        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        bool saw_ok = false; bool ok_has_largest = false;
        uint64_t ok_lg = 0, ok_lo = 0;
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_SUBSCRIBE_OK) {
                      saw_ok = true;
                      ok_has_largest = d[i].u.subscribe_ok.has_largest;
                      ok_lg = d[i].u.subscribe_ok.largest_group;
                      ok_lo = d[i].u.subscribe_ok.largest_object;
                  }
                  moq_event_cleanup(&d[i]);
              } }
        MOQ_TEST_CHECK(saw_ok);
        MOQ_TEST_CHECK(ok_has_largest);           /* the live largest is advertised */
        MOQ_TEST_CHECK_EQ_INT((int)ok_lg, 1);     /* group 1 (live), not 0 (retained) */
        MOQ_TEST_CHECK_EQ_INT((int)ok_lo, 0);

        /* Largest (1,0) known -> a joining FETCH now resolves client-side against
         * the live largest (proof the retained (0,0) was superseded, not lost). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh)
                       == MOQ_OK);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained largest gated by live write");
    }

    /* -- Unencodable retained Location is not advertised as Largest -------- *
     * A retained group whose group_id exceeds the QUIC varint max cannot carry
     * an encodable {group, object + 1} End Location, so it must not be advertised
     * as Largest -- and a joining FETCH against it is then invalid client-side. */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);

        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);

        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        static const uint8_t cat[] = { 'C','A','T' };
        moq_rcbuf_t *cp = NULL; moq_rcbuf_create(&alloc, cat, sizeof(cat), &cp);
        moq_pub_retained_object_t ro = { .object_id = 0, .payload = cp };
        moq_pub_retained_group_cfg_t st; moq_pub_retained_group_cfg_init(&st);
        st.group_id = MOQ_QUIC_VARINT_MAX + 1ull;   /* group unencodable */
        st.objects = &ro; st.object_count = 1;
        moq_pub_set_retained_group(pub, track, &st);
        moq_rcbuf_decref(cp);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res;
              moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* No Largest advertised -> joining FETCH invalid client-side. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh)
                       == MOQ_ERR_INVAL);

        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained unencodable largest not advertised");
    }

    /* -- Retained GROUP (objects 0..N): a Joining FETCH replays the whole group
     *    in order; a plain SUBSCRIBE delivers no retained objects. --------- */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        /* Retain a group of 3 objects in group 7: independent [0] + deltas [1],[2]. */
        moq_rcbuf_t *p0=NULL,*p1=NULL,*p2=NULL;
        moq_rcbuf_create(&alloc, (const uint8_t*)"obj0", 4, &p0);
        moq_rcbuf_create(&alloc, (const uint8_t*)"obj1", 4, &p1);
        moq_rcbuf_create(&alloc, (const uint8_t*)"obj2", 4, &p2);
        moq_pub_retained_object_t objs[3] = {
            { .object_id = 0, .payload = p0 },
            { .object_id = 1, .payload = p1 },
            { .object_id = 2, .payload = p2, .end_of_group = true },
        };
        moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
        gc.group_id = 7; gc.objects = objs; gc.object_count = 3;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              MOQ_TEST_CHECK(ev.kind == MOQ_EVENT_SUBSCRIBE_REQUEST);
              moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        /* (a) A plain SUBSCRIBE delivers NO retained objects (no re-push). */
        { int objs_n = 0; moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) {
                  if (d[i].kind == MOQ_EVENT_OBJECT_RECEIVED) objs_n++;
                  moq_event_cleanup(&d[i]);
              }
          MOQ_TEST_CHECK(objs_n == 0); }

        /* (b) Joining FETCH(offset 0) replays objects 0,1,2 in order + complete. */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int n_obj = 0; bool ord_ok = true, complete = false;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    if (evts[i].u.fetch_object.group_id != 7 ||
                        evts[i].u.fetch_object.object_id != (uint64_t)n_obj) ord_ok = false;
                    n_obj++;
                } else if (evts[i].kind == MOQ_EVENT_FETCH_COMPLETE) complete = true;
                moq_event_cleanup(&evts[i]);
            }
        MOQ_TEST_CHECK(n_obj == 3);   /* object 0 + deltas 1,2 in order */
        MOQ_TEST_CHECK(ord_ok);
        MOQ_TEST_CHECK(complete);

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group joining fetch 0..N");
    }

    /* -- set_retained_group replaces the prior group: FETCH serves the latest
     *    (only-latest-group retention). -------------------------------- */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        /* Gen A: 1 object in group 1. */
        moq_rcbuf_t *a0=NULL; moq_rcbuf_create(&alloc,(const uint8_t*)"A",1,&a0);
        moq_pub_retained_object_t oa[1] = { { .object_id=0, .payload=a0, .end_of_group=true } };
        moq_pub_retained_group_cfg_t ga; moq_pub_retained_group_cfg_init(&ga);
        ga.group_id = 1; ga.objects = oa; ga.object_count = 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &ga) == MOQ_OK);
        moq_rcbuf_decref(a0);
        /* Gen B: 2 objects in group 2 -> replaces gen A. */
        moq_rcbuf_t *b0=NULL,*b1=NULL;
        moq_rcbuf_create(&alloc,(const uint8_t*)"B0",2,&b0);
        moq_rcbuf_create(&alloc,(const uint8_t*)"B1",2,&b1);
        moq_pub_retained_object_t ob[2] = {
            { .object_id=0, .payload=b0 },
            { .object_id=1, .payload=b1, .end_of_group=true } };
        moq_pub_retained_group_cfg_t gb; moq_pub_retained_group_cfg_init(&gb);
        gb.group_id = 2; gb.objects = ob; gb.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gb) == MOQ_OK);
        moq_rcbuf_decref(b0); moq_rcbuf_decref(b1);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int n_obj = 0; bool grp_ok = true;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    if (evts[i].u.fetch_object.group_id != 2) grp_ok = false;  /* latest only */
                    n_obj++;
                }
                moq_event_cleanup(&evts[i]);
            }
        MOQ_TEST_CHECK(n_obj == 2);   /* gen B, not gen A */
        MOQ_TEST_CHECK(grp_ok);

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group replaced by latest");
    }

    /* -- set_retained_group bounds: count cap and byte budget rejected --- */
    {
        test_alloc_state_t as = {0};
        moq_alloc_t alloc = test_allocator(&as);
        moq_session_cfg_t scfg = MOQ_SESSION_CFG_INIT;
        scfg.alloc = &alloc;
        scfg.perspective = MOQ_PERSPECTIVE_SERVER;
        moq_session_t *server = NULL;
        moq_session_create(&scfg, 0, &server);
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(server, &alloc, &cfg, &pub);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg;
        moq_pub_track_cfg_init_sized(&tcfg, sizeof(tcfg));
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.max_retained_bytes = 8;            /* tiny byte budget */
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, 0, &track);

        /* object_count over the cap -> INVAL. */
        static moq_pub_retained_object_t big[MOQ_PUB_RETAINED_MAX_OBJECTS + 1];
        moq_rcbuf_t *one = NULL; moq_rcbuf_create(&alloc, (const uint8_t*)"x", 1, &one);
        for (size_t i = 0; i < MOQ_PUB_RETAINED_MAX_OBJECTS + 1; i++) {
            big[i].object_id = i; big[i].payload = one;
            big[i].properties = NULL; big[i].end_of_group = false;
        }
        moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
        gc.group_id = 0; gc.objects = big;
        gc.object_count = MOQ_PUB_RETAINED_MAX_OBJECTS + 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        /* total payload bytes over the budget -> INVAL. */
        moq_rcbuf_t *big_pl = NULL; moq_rcbuf_create(&alloc, (const uint8_t*)"123456789", 9, &big_pl);
        moq_pub_retained_object_t over[1] = { { .object_id=0, .payload=big_pl } };
        gc.objects = over; gc.object_count = 1;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        /* out-of-order object_ids -> INVAL. */
        moq_pub_retained_object_t ooo[2] = {
            { .object_id=2, .payload=one }, { .object_id=1, .payload=one } };
        gc.objects = ooo; gc.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        /* non-dense: a gap (0,2) -> INVAL (catalog replay needs 0..N). */
        moq_pub_retained_object_t gap[2] = {
            { .object_id=0, .payload=one }, { .object_id=2, .payload=one } };
        gc.objects = gap; gc.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        /* non-dense: missing object 0 (1,2) -> INVAL. */
        moq_pub_retained_object_t no0[2] = {
            { .object_id=1, .payload=one }, { .object_id=2, .payload=one } };
        gc.objects = no0; gc.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_ERR_INVAL);

        moq_rcbuf_decref(one); moq_rcbuf_decref(big_pl);
        moq_pub_destroy(pub);
        moq_session_destroy(server);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group bounds rejected");
    }

    /* -- Largest re-advertised after a live write + set_retained_group: the
     *    retained group's last object is the latest published location, so a
     *    joining FETCH is valid (correction: wrote_object must not suppress
     *    Largest for a retained group). --------------------------------- */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        /* A live write sets wrote_object (no subscriber yet -> retained, dropped). */
        moq_rcbuf_t *lv = NULL; moq_rcbuf_create(&alloc,(const uint8_t*)"live",4,&lv);
        moq_pub_object_cfg_t ocfg; moq_pub_object_cfg_init_sized(&ocfg, sizeof(ocfg));
        ocfg.group_id = 3; ocfg.object_id = 0; ocfg.payload = lv; ocfg.end_of_group = true;
        moq_pub_write_object_ex(pub, track, &ocfg, now);
        moq_rcbuf_decref(lv);

        /* set_retained_group then resets wrote_object: Largest = (9,1). */
        moq_rcbuf_t *c0=NULL,*c1=NULL;
        moq_rcbuf_create(&alloc,(const uint8_t*)"c0",2,&c0);
        moq_rcbuf_create(&alloc,(const uint8_t*)"c1",2,&c1);
        moq_pub_retained_object_t objs[2] = {
            { .object_id=0, .payload=c0 },
            { .object_id=1, .payload=c1, .end_of_group=true } };
        moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
        gc.group_id = 9; gc.objects = objs; gc.object_count = 2;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
        moq_rcbuf_decref(c0); moq_rcbuf_decref(c1);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* A joining FETCH is VALID only if Largest was advertised -> it serves
         * the retained group (group 9). */
        moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
        fcfg.is_joining = true; fcfg.joining_relative = true;
        fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
        moq_fetch_t fh;
        MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        MOQ_TEST_CHECK(moq_pub_tick(pub, now) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        int n_obj = 0; bool grp_ok = true, complete = false;
        moq_event_t evts[16]; size_t ne;
        while ((ne = moq_session_poll_events(moq_simpair_client(sp), evts, 16)) > 0)
            for (size_t i = 0; i < ne; i++) {
                if (evts[i].kind == MOQ_EVENT_FETCH_OBJECT) {
                    if (evts[i].u.fetch_object.group_id != 9) grp_ok = false;
                    n_obj++;
                } else if (evts[i].kind == MOQ_EVENT_FETCH_COMPLETE) complete = true;
                moq_event_cleanup(&evts[i]);
            }
        MOQ_TEST_CHECK(n_obj == 2);   /* Largest advertised -> fetch served */
        MOQ_TEST_CHECK(grp_ok);
        MOQ_TEST_CHECK(complete);

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group largest after live write");
    }

    /* -- Joining FETCH range that does not cover the whole retained group is
     *    rejected (object 0 omitted, or later deltas omitted). The range is
     *    injected by mutating the real FETCH_REQUEST event before handling it,
     *    since a normal offset-0 joining FETCH always covers the group. ---- */
    {
        test_alloc_state_t as; moq_alloc_t alloc; moq_simpair_t *sp;
        simpair_setup(&as, &alloc, &sp);
        uint64_t now = moq_simpair_now_us(sp);
        moq_pub_cfg_t cfg; moq_pub_cfg_init_sized(&cfg, sizeof(cfg));
        cfg.accept_mode = MOQ_PUB_ACCEPT_ALL;
        moq_publisher_t *pub = NULL;
        moq_pub_create(moq_simpair_server(sp), &alloc, &cfg, &pub);
        moq_bytes_t ns_parts[] = { MOQ_BYTES_LITERAL("svc") };
        moq_pub_track_cfg_t tcfg; moq_pub_track_cfg_init(&tcfg);
        tcfg.track_namespace.parts = ns_parts; tcfg.track_namespace.count = 1;
        tcfg.track_name = MOQ_BYTES_LITERAL("catalog");
        tcfg.advertise_namespace = true;
        moq_pub_track_t *track = NULL;
        moq_pub_add_track(pub, &tcfg, now, &track);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev; if (moq_session_poll_events(moq_simpair_client(sp), &ev, 1) == 1) moq_event_cleanup(&ev); }

        moq_rcbuf_t *p0=NULL,*p1=NULL,*p2=NULL;
        moq_rcbuf_create(&alloc,(const uint8_t*)"o0",2,&p0);
        moq_rcbuf_create(&alloc,(const uint8_t*)"o1",2,&p1);
        moq_rcbuf_create(&alloc,(const uint8_t*)"o2",2,&p2);
        moq_pub_retained_object_t objs[3] = {
            { .object_id=0, .payload=p0 }, { .object_id=1, .payload=p1 },
            { .object_id=2, .payload=p2, .end_of_group=true } };
        moq_pub_retained_group_cfg_t gc; moq_pub_retained_group_cfg_init(&gc);
        gc.group_id = 7; gc.objects = objs; gc.object_count = 3;
        MOQ_TEST_CHECK(moq_pub_set_retained_group(pub, track, &gc) == MOQ_OK);
        moq_rcbuf_decref(p0); moq_rcbuf_decref(p1); moq_rcbuf_decref(p2);

        moq_subscribe_cfg_t scfg; moq_subscribe_cfg_init(&scfg);
        scfg.track_namespace.parts = ns_parts; scfg.track_namespace.count = 1;
        scfg.track_name = MOQ_BYTES_LITERAL("catalog");
        scfg.filter = MOQ_SUBSCRIBE_FILTER_LARGEST_OBJECT;
        moq_subscription_t sub_h;
        MOQ_TEST_CHECK(moq_session_subscribe(moq_simpair_client(sp), &scfg, now, &sub_h) == MOQ_OK);
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t ev;
          if (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
              moq_pub_event_result_t res; moq_pub_handle_event(pub, &ev, now, &res);
              moq_event_cleanup(&ev);
          } }
        moq_simpair_run_until_quiescent(sp, 8, NULL);
        { moq_event_t d[16]; size_t ne;
          while ((ne = moq_session_poll_events(moq_simpair_client(sp), d, 16)) > 0)
              for (size_t i = 0; i < ne; i++) moq_event_cleanup(&d[i]); }

        /* Two malformed ranges, each injected into the real FETCH_REQUEST. */
        struct { uint64_t so, eg, eo; const char *what; } cases[] = {
            { 1, 7, 0, "omit object 0" },   /* start at object 1 -> 0 not covered */
            { 0, 7, 2, "omit later delta" },/* end at object 2 -> object 2 not covered */
        };
        for (size_t c = 0; c < 2; c++) {
            moq_fetch_cfg_t fcfg; moq_fetch_cfg_init(&fcfg);
            fcfg.is_joining = true; fcfg.joining_relative = true;
            fcfg.joining_start = 0; fcfg.joining_sub = sub_h;
            moq_fetch_t fh;
            MOQ_TEST_CHECK(moq_session_fetch(moq_simpair_client(sp), &fcfg, now, &fh) == MOQ_OK);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            /* Poll the FETCH_REQUEST, mutate its range to omit part of the group,
             * then hand it to the publisher. */
            bool handled = false;
            moq_event_t ev;
            while (moq_session_poll_events(moq_simpair_server(sp), &ev, 1) == 1) {
                if (ev.kind == MOQ_EVENT_FETCH_REQUEST) {
                    ev.u.fetch_request.start_group = 7;
                    ev.u.fetch_request.start_object = cases[c].so;
                    ev.u.fetch_request.end_group = cases[c].eg;
                    ev.u.fetch_request.end_object = cases[c].eo;
                    moq_pub_event_result_t res;
                    moq_pub_handle_event(pub, &ev, now, &res);
                    handled = true;
                }
                moq_event_cleanup(&ev);
            }
            MOQ_TEST_CHECK(handled);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            (void)moq_pub_tick(pub, now);
            moq_simpair_run_until_quiescent(sp, 8, NULL);
            bool got_err = false; int objs_seen = 0;
            moq_event_t e2;
            while (moq_session_poll_events(moq_simpair_client(sp), &e2, 1) == 1) {
                if (e2.kind == MOQ_EVENT_FETCH_ERROR &&
                    e2.u.fetch_error.error_code == MOQ_REQUEST_ERROR_NOT_SUPPORTED)
                    got_err = true;
                if (e2.kind == MOQ_EVENT_FETCH_OBJECT) objs_seen++;
                moq_event_cleanup(&e2);
            }
            MOQ_TEST_CHECK(got_err);      /* rejected */
            MOQ_TEST_CHECK(objs_seen == 0);   /* nothing served */
        }

        moq_pub_remove_track(pub, track, now);
        moq_pub_destroy(pub);
        drain_all(sp);
        moq_simpair_destroy(sp);
        MOQ_TEST_CHECK(as.balance == 0);
        MOQ_TEST_PASS("retained group fetch range must cover whole group");
    }

    if (failures == 0) {
        printf("PASS: all publisher tests\n");
        return 0;
    }
    printf("FAIL: %d publisher test failures\n", failures);
    return 1;
}
