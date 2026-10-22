//! The generated types against the C compiler's view of the same headers:
//! sizeof, alignof and offsetof for every record the safe crate reads or
//! writes, and the frozen v0 floors and restated constants.

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
    };
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
    ] {
        v.insert(name.into(), Entry::Value(value));
    }
    v
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
}
