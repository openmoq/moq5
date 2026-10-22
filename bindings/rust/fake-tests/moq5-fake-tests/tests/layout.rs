//! The generated types against the C compiler's view of the same headers:
//! sizeof, alignof and offsetof for every record the safe crate reads or
//! writes (with the `player` feature, the player's records and the fields of
//! their unions' members), and the frozen v0 floors and restated constants.

use std::collections::BTreeMap;
use std::ffi::CStr;
use std::mem::{align_of, offset_of, size_of};

use moq5_fake_tests as fake;
use moq5_fake_tests::{sys, LayoutEntry};

#[derive(Debug, PartialEq, Eq)]
enum Entry {
    Type { size: usize, align: usize },
    Field { offset: usize },
    Value(usize),
}

macro_rules! layout {
    ($($t:ident { $($f:ident),* $(,)? })*) => {{
        let mut v: BTreeMap<String, Entry> = BTreeMap::new();
        $(
            v.insert(stringify!($t).into(), Entry::Type {
                size: size_of::<sys::$t>(),
                align: align_of::<sys::$t>(),
            });
            $(
                v.insert(
                    format!("{}.{}", stringify!($t), stringify!($f)),
                    Entry::Field { offset: offset_of!(sys::$t, $f) },
                );
            )*
        )*
        v
    }};
}

fn rust_side() -> BTreeMap<String, Entry> {
    let mut v = layout! {
        moq_bytes_t { data, len }
        moq_namespace_t { parts, count }
        moq_auth_token_t { token_type, token_value }
        moq_alloc_t { ctx, alloc, realloc, free }
        moq_version_offer_t { struct_size, policy, versions, version_count }
        moq_endpoint_cfg_t {
            struct_size, url, protocol, backend, versions, sni, ca_file, insecure_skip_verify,
            wt_path, perspective, alloc, wt_profile, handshake_timeout_us, setup_auth_tokens,
            setup_auth_token_count, verify_certificate, verify_certificate_ctx, uri_mode,
            preferred_version, accept_uri, accept_uri_ctx,
        }
        moq_endpoint_terminal_t { struct_size, reason, detail_code }
        moq_media_sender_callbacks_t { struct_size, ctx, on_ready }
        moq_media_sender_cfg_t {
            struct_size, endpoint, namespace_, catalog_track, backpressure, block_timeout_us,
            queue_max_objects, queue_max_bytes, pre_ready_max_objects, pre_ready_max_bytes,
            validate_cmaf, content_protections, content_protection_count, callbacks,
            publish_tracks, drop_without_demand, catalog_refresh_interval_us, companion_profile,
        }
        moq_media_track_cfg_t {
            struct_size, name, media_type, packaging, codec, timescale, init_data, role, lang,
            is_live, width, height, framerate_millis, samplerate, channel_config, bitrate,
            has_max_grp_sap, max_grp_sap, has_max_obj_sap, max_obj_sap,
            content_protection_ref_ids, content_protection_ref_id_count, emit_sap_timeline,
            sap_timeline_history_groups, has_track_duration, track_duration_ms,
            emit_media_timeline, media_timeline_history_groups, has_alt_group, alt_group,
            has_publisher_priority, publisher_priority, extensions, extension_count,
            mpeg2ts_app_objects, track_properties, dynamic_groups, has_render_group,
            render_group, has_label, label,
        }
        moq_media_send_object_t {
            struct_size, payload, properties, is_sync, starts_group, ends_group, decode_time_us,
            presentation_time_us, has_capture_time, capture_time_us, has_sap_type, sap_type,
            metadata,
        }
        moq_media_vod_track_t { track, duration_ms }
        moq_media_sender_stats_t {
            struct_size, objects_written, objects_sent, objects_queued, bytes_queued,
            objects_dropped, groups_dropped, keyframes_dropped, groups_abandoned,
            backpressure_stalls, last_error, sap_records_evicted, completion_error,
        }
        moq_cmsf_url_t { present, url, has_type, type_ }
        moq_cmsf_drm_system_t {
            system_id, la_url, cert_url, auth_url, has_pssh, pssh, has_robustness, robustness,
        }
        moq_cmsf_content_protection_t {
            ref_id, default_kids, default_kid_count, scheme, drm_system,
        }
        moq_media_overflow_cfg_t { policy, max_objects, max_bytes }
        moq_media_receiver_cfg_t {
            struct_size, endpoint, namespace_, catalog_track, auto_subscribe, time_mode,
            overflow, max_track_events, mpeg2ts_reorder_slots, mpeg2ts_reorder_window_us,
            mpeg2ts_strip_prefix, mpeg2ts_join_psi, companion_profile, max_stream_terminals,
            surface_stream_terminals,
        }
        moq_media_receiver_track_subscribe_cfg_t {
            struct_size, start, has_priority, priority, mpeg2ts_program_number, location, fill,
        }
        moq_media_track_info_t {
            struct_size, media_type, packaging, timescale, transport_version,
        }
        moq_cmaf_init_info_t {
            struct_size, codec_kind, timescale, width, height, samplerate, channel_count,
            codec_config, track_id, has_cenc, scheme, default_is_protected,
            default_per_sample_iv_size, default_kid,
        }
        moq_cmaf_sample_t { duration, size, flags, composition_offset }
        moq_msf_media_template_t {
            start_media_ms, delta_media_ms, start_group, start_object, delta_group, delta_object,
            start_wallclock_ms, delta_wallclock_ms,
        }
        moq_media_track_desc_t {
            struct_size, name, role, codec, lang, label, info, has_init, init, init_data,
            has_width, width, has_height, height, has_samplerate, samplerate, channel_config,
            has_framerate, framerate_millis, has_bitrate, bitrate, has_max_grp_sap, max_grp_sap,
            has_max_obj_sap, max_obj_sap, content_protection_ref_ids,
            content_protection_ref_id_count, packaging, event_type, mime_type, depends,
            depends_count, has_template, template_, has_track_duration, track_duration_ms,
            is_live, extensions, extension_count, has_alt_group, alt_group, has_render_group,
            render_group, has_label,
        }
        moq_media_track_event_t {
            struct_size, kind, track, desc, config_generation, has_largest, largest_group,
            largest_object, has_expires, expires_ms, parse_drop_class, parse_drops_total,
            parse_drops_delta, local_result, initiating_request_id, fill_reason,
            fill_error_domain, fill_error_code, delivery_origin,
            delivery_initiating_request_id, fill_unreported_loss_causes,
            fill_unreported_loss_group, fill_unreported_loss_object, has_done_status,
            done_status,
        }
        moq_media_object_t {
            struct_size, track, config_generation, packaging, status, end_of_group, datagram,
            keyframe, has_capture_time, capture_time_us, decode_time_us, composition_offset_us,
            presentation_time_us, payload, fragment, mdat_offset, mdat_len, sample_count,
            samples, payload_ref, properties_ref, samples_owned, group_id, subgroup_id,
            object_id, publisher_priority, has_subgroup, has_loss_report, gap, gap_group,
            gap_object, gap_tail_unknown, gap_duration_known, gap_duration_27mhz,
            discontinuous, metadata, track_properties_ref, delivery_origin,
            initiating_request_id,
        }
        moq_media_stream_terminal_t {
            struct_size, kind, track, group_id, subgroup_id, object_id, last_group_id,
            error_code, has_last_object, has_cut_object, delivery_origin, last_object_id,
            cut_object_id, initiating_request_id,
        }
        moq_media_sap_record_t { struct_size, track, group, object, sap_type, ept_ms }
        moq_media_timeline_record_t {
            struct_size, track, media_time_ms, group, object, wallclock_ms,
        }
        moq_media_receiver_stats_t {
            struct_size, objects_received, objects_queued, bytes_queued, objects_dropped,
            groups_dropped, keyframes_dropped, parse_drops, overflow_events, pause_transitions,
            paused, catalog_drops, catalog_complete, gaps_reported, gap_tails_unknown,
            reorder_refused, tracks_malformed,
        }
        moq_location_filter_cfg_t {
            struct_size, kind, relative_group_offset, start_group, start_object, end_group,
            end_object,
        }
        moq_fill_cfg_t {
            struct_size, has_subscriber_priority, subscriber_priority, has_group_order,
            has_fill_timeout, fill_timeout_ms, group_order, location,
        }
        moq_media_receiver_info_t {
            struct_size, time_mode, overflow_policy, max_objects, max_bytes, auto_subscribe,
            surface_stream_terminals, companion_profile, transport_version,
            max_stream_terminals, track_events_polled,
        }
        moq_media_send_mpeg2ts_t { struct_size, data, end_object }
        moq_media_sender_track_stats_t {
            struct_size, objects_written, objects_sent, objects_queued, objects_dropped,
            bytes_queued, groups_dropped, keyframes_dropped, groups_abandoned, mpeg2ts_groups,
            mpeg2ts_objects, mpeg2ts_packets_in, mpeg2ts_packets_dropped_before_random_access,
            mpeg2ts_max_group_27mhz, mpeg2ts_groups_over_2s, mpeg2ts_peak_rate_bps,
            mpeg2ts_source_pcr_discontinuities, mpeg2ts_mux_rate_below_peak,
            mpeg2ts_program_gone, mpeg2ts_objects_parked, mpeg2ts_partial_bytes_discarded,
            mpeg2ts_continuation_objects_discarded,
        }
        moq_msf_mpeg2ts_t {
            struct_size, has_mode, mode, mode_name, has_packet_size, packet_size, has_es_pid,
            es_pid, has_program_number, program_number, has_pcr_pid, pcr_pid, has_mux_rate,
            mux_rate, has_si_pids, si_pids, si_pid_count, has_random_access, random_access,
            has_timestamp_mode, timestamp_mode, timestamp_mode_name, has_scte35_pid,
            scte35_pid, malformed,
        }
        moq_msf_track_ext_t { kind, u }
        moq_loc_metadata_t {
            struct_size, present, timestamp, timescale, marking_len, marking, audio_level,
            reserved, video_config, audio_config,
        }
        moq_media_metadata_t {
            struct_size, time_domain, time_us, loc, has_time_us, independence_known,
            independent, reserved,
        }
    };
    #[cfg(feature = "player")]
    player_side(&mut v);
    for (name, value) in [
        ("MOQ_ENDPOINT_CFG_V0_SIZE", sys::MOQ_ENDPOINT_CFG_V0_SIZE),
        (
            "MOQ_ENDPOINT_TERMINAL_V0_SIZE",
            sys::MOQ_ENDPOINT_TERMINAL_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_TRACK_CFG_V0_SIZE",
            sys::MOQ_MEDIA_TRACK_CFG_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_SEND_OBJECT_V0_SIZE",
            sys::MOQ_MEDIA_SEND_OBJECT_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_RECEIVER_CFG_V0_SIZE",
            sys::MOQ_MEDIA_RECEIVER_CFG_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_TRACK_DESC_V0_SIZE",
            sys::MOQ_MEDIA_TRACK_DESC_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE",
            sys::MOQ_MEDIA_RECEIVER_TRACK_SUBSCRIBE_CFG_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE",
            sys::MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_RECEIVER_INFO_V0_SIZE",
            sys::MOQ_MEDIA_RECEIVER_INFO_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_TRACK_INFO_V0_SIZE",
            sys::MOQ_MEDIA_TRACK_INFO_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_METADATA_V0_SIZE",
            sys::MOQ_MEDIA_METADATA_V0_SIZE,
        ),
        (
            "MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US",
            sys::MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US as usize,
        ),
        (
            "MOQ_MEDIA_COMPANION_AUTO",
            sys::MOQ_MEDIA_COMPANION_AUTO as usize,
        ),
        (
            "MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01",
            sys::MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 as usize,
        ),
        (
            "MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC",
            sys::MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC as usize,
        ),
        (
            "MOQ_ENDPOINT_URI_LEGACY",
            sys::MOQ_ENDPOINT_URI_LEGACY as usize,
        ),
        (
            "MOQ_ENDPOINT_URI_STANDARD",
            sys::MOQ_ENDPOINT_URI_STANDARD as usize,
        ),
        ("MOQ_LOC_METADATA_V0_SIZE", sys::MOQ_LOC_METADATA_V0_SIZE),
        ("MOQ_MSF_MPEG2TS_V0_SIZE", sys::MOQ_MSF_MPEG2TS_V0_SIZE),
        (
            "MOQ_LOCATION_FILTER_CFG_V0_SIZE",
            sys::MOQ_LOCATION_FILTER_CFG_V0_SIZE as usize,
        ),
        ("MOQ_FILL_CFG_V0_SIZE", sys::MOQ_FILL_CFG_V0_SIZE as usize),
        (
            "MOQ_LOC_METADATA_HAS_TIMESTAMP",
            sys::MOQ_LOC_METADATA_HAS_TIMESTAMP as usize,
        ),
        (
            "MOQ_LOC_METADATA_HAS_TIMESCALE",
            sys::MOQ_LOC_METADATA_HAS_TIMESCALE as usize,
        ),
        (
            "MOQ_LOC_METADATA_HAS_MARKING",
            sys::MOQ_LOC_METADATA_HAS_MARKING as usize,
        ),
        (
            "MOQ_LOC_METADATA_HAS_AUDIO_LEVEL",
            sys::MOQ_LOC_METADATA_HAS_AUDIO_LEVEL as usize,
        ),
        (
            "MOQ_LOC_METADATA_HAS_VIDEO_CONFIG",
            sys::MOQ_LOC_METADATA_HAS_VIDEO_CONFIG as usize,
        ),
        (
            "MOQ_LOC_METADATA_HAS_AUDIO_CONFIG",
            sys::MOQ_LOC_METADATA_HAS_AUDIO_CONFIG as usize,
        ),
        (
            "MOQ_LOC_METADATA_PRESENT_MASK",
            sys::MOQ_LOC_METADATA_PRESENT_MASK as usize,
        ),
    ] {
        v.insert(name.into(), Entry::Value(value));
    }
    v
}

/// The player's records, its union members' fields named
/// `record.u.member.field` (the union's offset plus the field's in the
/// member, which starts the union), and its floors.
#[cfg(feature = "player")]
fn player_side(v: &mut BTreeMap<String, Entry>) {
    v.extend(layout! {
        moq_playback_cfg_t {
            struct_size, max_tracks, max_buffered_objects, max_buffered_bytes,
            max_backlog_groups, max_commands, max_events, gap_timeout_us, max_release_per_tick,
            max_track_config_bytes, max_samples_per_object, ignore_eog_bit,
            infer_end_of_group_from_next_group, max_known_ranges, max_held_commands,
        }
        moq_media_player_cfg_t {
            struct_size, selection, receiver, playback, preferred_lang, end_tail_wait_us,
            cushion_us, max_outputs, max_events, timing_policy, max_held_bytes,
            assume_shared_epoch,
        }
        moq_media_player_select_cfg_t { struct_size, start, independent_timing, shared_epoch }
        moq_media_timeline_t {
            domain, timeline_id, timeline_epoch, anchor_media_us, anchor_local_us, rate,
        }
        moq_media_player_output_t {
            struct_size, kind, detail_size, timeline_id, configuration_generation, track, u,
        }
        moq_media_player_event_t { struct_size, kind, detail_size, track, u }
        moq_media_player_feedback_t { struct_size, kind, track, configuration_generation, u }
        moq_media_player_stats_t {
            struct_size, tracks_selected, outputs_queued, events_queued, events_dropped,
            held_objects, held_bytes, objects_taken, objects_pushed, objects_dropped,
            objects_discarded, terminals_taken, units_output, configures_output,
            discontinuities_output, ends_output, timeline_records_discarded, feedback_stale,
            outputs_bytes, selections_failed,
        }
    });
    macro_rules! members {
        ($record:ident.u { $($member:ident: $member_ty:ident { $($field:ident),* $(,)? })* }) => {
            $($(
                v.insert(
                    format!(
                        "{}.u.{}.{}",
                        stringify!($record),
                        stringify!($member),
                        stringify!($field)
                    ),
                    Entry::Field {
                        offset: offset_of!(sys::$record, u) + offset_of!(sys::$member_ty, $field),
                    },
                );
            )*)*
        };
    }
    members!(moq_media_player_output_t.u {
        configure: moq_media_player_output__bindgen_ty_1__bindgen_ty_1 {
            media_type, packaging, codec, codec_config, timescale, width, height, samplerate,
            channel_count,
        }
        unit: moq_media_player_output__bindgen_ty_1__bindgen_ty_2 {
            group_id, object_id, decode_time_us, presentation_time_us, composition_offset_us,
            has_capture_time, independence_known, independent, present, sample_duration_us,
            capture_time_us, payload, offset, len,
        }
        discontinuity: moq_media_player_output__bindgen_ty_1__bindgen_ty_3 {
            reason, decoder_reset_required,
        }
        end: moq_media_player_output__bindgen_ty_1__bindgen_ty_4 { cause, disposition }
    });
    members!(moq_media_player_event_t.u {
        gap: moq_media_player_event__bindgen_ty_1__bindgen_ty_1 {
            group_id, object_id, cause, crossed, hole_start_us, hole_duration_us,
        }
        skip: moq_media_player_event__bindgen_ty_1__bindgen_ty_2 { from_group_id, to_group_id }
        shed: moq_media_player_event__bindgen_ty_1__bindgen_ty_3 {
            dropped_groups, remaining_groups,
        }
        drop: moq_media_player_event__bindgen_ty_1__bindgen_ty_4 { reason, group_id, object_id }
        loss: moq_media_player_event__bindgen_ty_1__bindgen_ty_5 {
            kind, group_id, last_group_id, error_code,
        }
        knowledge_forgotten: moq_media_player_event__bindgen_ty_1__bindgen_ty_6 {
            first_group_id, last_group_id,
        }
        sync: moq_media_player_event__bindgen_ty_1__bindgen_ty_7 {
            has_render_group, render_group, reason,
        }
        started: moq_media_player_event__bindgen_ty_1__bindgen_ty_8 { timeline_id }
    });
    members!(moq_media_player_feedback_t.u {
        decoder_queue: moq_media_player_feedback__bindgen_ty_1__bindgen_ty_1 {
            depth, max_recommended,
        }
        decode_error: moq_media_player_feedback__bindgen_ty_1__bindgen_ty_2 { message }
    });
    for (name, value) in [
        (
            "MOQ_MEDIA_PLAYER_CFG_V0_SIZE",
            sys::MOQ_MEDIA_PLAYER_CFG_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_PLAYER_SELECT_CFG_V0_SIZE",
            sys::MOQ_MEDIA_PLAYER_SELECT_CFG_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_PLAYER_FEEDBACK_V0_SIZE",
            sys::MOQ_MEDIA_PLAYER_FEEDBACK_V0_SIZE,
        ),
        (
            "MOQ_MEDIA_PLAYER_STATS_V0_SIZE",
            sys::MOQ_MEDIA_PLAYER_STATS_V0_SIZE,
        ),
        ("MOQ_PLAYBACK_CFG_V0_SIZE", sys::MOQ_PLAYBACK_CFG_V0_SIZE),
        (
            "MOQ_MEDIA_PLAYER_OUTPUT_DETAIL_MAX",
            sys::MOQ_MEDIA_PLAYER_OUTPUT_DETAIL_MAX as usize,
        ),
        (
            "MOQ_MEDIA_PLAYER_EVENT_DETAIL_MAX",
            sys::MOQ_MEDIA_PLAYER_EVENT_DETAIL_MAX as usize,
        ),
    ] {
        v.insert(name.into(), Entry::Value(value));
    }
}

fn c_side() -> BTreeMap<String, Entry> {
    let mut count = 0usize;
    // SAFETY: the probe returns a static table of `count` entries.
    let entries = unsafe { fake::moq5_layout_entries(&mut count) };
    assert!(!entries.is_null() && count > 0);
    // SAFETY: as above.
    let entries: &[LayoutEntry] = unsafe { std::slice::from_raw_parts(entries, count) };
    entries
        .iter()
        .map(|e| {
            // SAFETY: each name is a static NUL-terminated string.
            let name = unsafe { CStr::from_ptr(e.name) }
                .to_str()
                .expect("a probe name is ASCII")
                .to_owned();
            let entry = if name.contains('.') {
                Entry::Field { offset: e.offset }
            } else if name.starts_with("MOQ_") {
                Entry::Value(e.size)
            } else {
                Entry::Type {
                    size: e.size,
                    align: e.align,
                }
            };
            (name, entry)
        })
        .collect()
}

#[test]
fn generated_types_match_the_c_compiler() {
    let rust = rust_side();
    let c = c_side();
    let mut mismatches = Vec::new();
    for (name, c_entry) in &c {
        match rust.get(name) {
            None => mismatches.push(format!("{name}: in C, not in the Rust table")),
            Some(r) if r != c_entry => {
                mismatches.push(format!("{name}: C {c_entry:?}, Rust {r:?}"))
            }
            Some(_) => {}
        }
    }
    for name in rust.keys() {
        if !c.contains_key(name) {
            mismatches.push(format!("{name}: in the Rust table, not in C"));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    assert!(c.len() > 250, "{} entries", c.len());
}

#[test]
fn the_floors_lie_within_the_records() {
    assert!(sys::MOQ_ENDPOINT_CFG_V0_SIZE <= size_of::<sys::moq_endpoint_cfg_t>());
    assert_eq!(
        sys::MOQ_ENDPOINT_CFG_V0_SIZE,
        offset_of!(sys::moq_endpoint_cfg_t, alloc) + size_of::<*const sys::moq_alloc_t>()
    );
    assert_eq!(
        sys::MOQ_ENDPOINT_TERMINAL_V0_SIZE,
        size_of::<sys::moq_endpoint_terminal_t>()
    );
    assert_eq!(
        sys::MOQ_MEDIA_TRACK_CFG_V0_SIZE,
        offset_of!(sys::moq_media_track_cfg_t, has_track_duration)
    );
    assert_eq!(
        sys::MOQ_MEDIA_SEND_OBJECT_V0_SIZE,
        offset_of!(sys::moq_media_send_object_t, metadata)
    );
    assert!(sys::MOQ_MEDIA_RECEIVER_CFG_V0_SIZE <= size_of::<sys::moq_media_receiver_cfg_t>());
    assert_eq!(
        sys::MOQ_MEDIA_TRACK_DESC_V0_SIZE,
        offset_of!(sys::moq_media_track_desc_t, is_live) + 1
    );
    assert_eq!(
        sys::MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE,
        size_of::<sys::moq_media_stream_terminal_t>()
    );
    assert_eq!(
        size_of::<moq5::cmaf::CmafSample>(),
        size_of::<sys::moq_cmaf_sample_t>()
    );
    assert_eq!(
        sys::MOQ_MEDIA_RECEIVER_INFO_V0_SIZE,
        size_of::<sys::moq_media_receiver_info_t>()
    );
    assert_eq!(
        sys::MOQ_MSF_MPEG2TS_V0_SIZE,
        offset_of!(sys::moq_msf_mpeg2ts_t, malformed) + size_of::<u32>()
    );
    assert_eq!(
        sys::MOQ_LOC_METADATA_V0_SIZE,
        offset_of!(sys::moq_loc_metadata_t, audio_config) + size_of::<sys::moq_bytes_t>()
    );
    assert_eq!(
        sys::MOQ_LOCATION_FILTER_CFG_V0_SIZE as usize,
        size_of::<sys::moq_location_filter_cfg_t>()
    );
    assert!(sys::MOQ_FILL_CFG_V0_SIZE as usize <= size_of::<sys::moq_fill_cfg_t>());
}

#[cfg(feature = "player")]
#[test]
fn the_player_floors_are_the_frozen_records() {
    // Outputs and events are frozen layouts whose union reserves are
    // pinned; the configuration and statistics floors are the whole
    // current records.
    assert_eq!(
        sys::MOQ_MEDIA_PLAYER_OUTPUT_DETAIL_MAX as usize,
        size_of::<sys::moq_media_player_output__bindgen_ty_1>()
    );
    assert_eq!(
        sys::MOQ_MEDIA_PLAYER_EVENT_DETAIL_MAX as usize,
        size_of::<sys::moq_media_player_event__bindgen_ty_1>()
    );
    assert_eq!(
        sys::MOQ_MEDIA_PLAYER_CFG_V0_SIZE,
        offset_of!(sys::moq_media_player_cfg_t, assume_shared_epoch) + 1
    );
    assert_eq!(
        sys::MOQ_MEDIA_PLAYER_STATS_V0_SIZE,
        size_of::<sys::moq_media_player_stats_t>()
    );
    assert_eq!(
        sys::MOQ_MEDIA_PLAYER_FEEDBACK_V0_SIZE,
        size_of::<sys::moq_media_player_feedback_t>()
    );
}
