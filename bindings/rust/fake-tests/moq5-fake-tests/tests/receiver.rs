//! Receiver rows: configuration mapping through the sized presets, track
//! events with every stamped field, description snapshots, owned objects
//! and their release, the stream-terminal lane, timeline records,
//! subscriptions, state, content protection and the stamped statistics.

use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use moq5::cmaf::CmafSample;
use moq5::{
    CompanionProfile, DeliveryOrigin, Endpoint, EndpointConfig, Error, Location, MediaType,
    Namespace, ObjectStatus, Packaging, ParseDropClass, Receiver, ReceiverConfig, SapType,
    StartMode, StreamTerminalKind, SubscribeOptions, TimeMode, Track, TrackEventKind, TrackState,
    Version, WaitResult,
};
use moq5_fake_tests as fake;
use moq5_fake_tests::{sys, Suite};

fn connect(s: &Suite) -> Endpoint {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433"))
        .expect("the provider accepts the connect");
    s.take_log();
    endpoint
}

fn namespace() -> Namespace {
    Namespace::parse("live/demo").expect("a two-part namespace parses")
}

fn attach(s: &Suite, endpoint: &Endpoint) -> Receiver {
    let receiver = Receiver::attach(
        endpoint,
        &ReceiverConfig::live(namespace()).auto_subscribe(false),
    )
    .expect("the provider accepts the receiver attach");
    s.take_log();
    receiver
}

fn raw_track(receiver: &Receiver, index: usize) -> *mut sys::moq_media_track_t {
    // SAFETY: a live receiver of the provider.
    unsafe { fake::moq5_fake_receiver_track(receiver.as_ptr(), index) }
}

fn push_event(event: sys::moq_media_track_event_t) {
    // SAFETY: the provider copies the record.
    unsafe { fake::moq5_fake_push_track_event(&event) }
}

fn raw_event(receiver: &Receiver, kind: u32, index: usize) -> sys::moq_media_track_event_t {
    sys::moq_media_track_event_t {
        kind,
        track: raw_track(receiver, index),
        ..Default::default()
    }
}

/// The handle the receiver reports for the provider'stats track `index`.
fn track(receiver: &Receiver, index: usize) -> Track {
    push_event(raw_event(receiver, sys::MOQ_MEDIA_TRACK_ADDED, index));
    receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable")
        .track()
        .expect("the Added event names its track")
}

static NAME: &[u8] = b"video";
static CODEC: &[u8] = b"avc1.42e01e";
static CMAF: &[u8] = b"cmaf";
static LABEL: &[u8] = b"Main";
static INIT: &[u8] = &[0, 0, 0, 8, b'f', b't', b'y', b'p'];
static CONFIG: &[u8] = &[1, 2, 3];
static REF_ID: &[u8] = b"cp0";
static DEP: &[u8] = b"video";
/// Spans over statics, for records the provider retains by pointer.
struct Spans<const N: usize>([sys::moq_bytes_t; N]);

// SAFETY: the spans point at immutable statics.
unsafe impl<const N: usize> Sync for Spans<N> {}

static REFS: Spans<1> = Spans([sys::moq_bytes_t {
    data: REF_ID.as_ptr(),
    len: 3,
}]);
static DEPS: Spans<1> = Spans([sys::moq_bytes_t {
    data: DEP.as_ptr(),
    len: 5,
}]);

fn raw_description(is_live: bool) -> sys::moq_media_track_desc_t {
    let mut description = sys::moq_media_track_desc_t {
        struct_size: std::mem::size_of::<sys::moq_media_track_desc_t>() as u32,
        name: fake::span(NAME),
        codec: fake::span(CODEC),
        label: fake::span(LABEL),
        info: sys::moq_media_track_info_t {
            struct_size: std::mem::size_of::<sys::moq_media_track_info_t>() as u32,
            media_type: sys::MOQ_MEDIA_TYPE_VIDEO,
            packaging: sys::MOQ_MEDIA_PACKAGING_CMAF,
            timescale: 90000,
            transport_version: 22,
        },
        has_init: true,
        init_data: fake::span(INIT),
        has_width: true,
        width: 1920,
        has_height: true,
        height: 1080,
        has_framerate: true,
        framerate_millis: 30000,
        has_bitrate: true,
        bitrate: 2_000_000,
        has_max_grp_sap: true,
        max_grp_sap: 1,
        content_protection_ref_ids: REFS.0.as_ptr(),
        content_protection_ref_id_count: 1,
        packaging: fake::span(CMAF),
        depends: DEPS.0.as_ptr(),
        depends_count: 1,
        has_template: true,
        is_live,
        has_alt_group: true,
        alt_group: -2,
        has_render_group: true,
        render_group: 0,
        has_label: true,
        ..Default::default()
    };
    description.init.struct_size = std::mem::size_of::<sys::moq_cmaf_init_info_t>() as u32;
    description.init.codec_kind = sys::MOQ_CMAF_CODEC_AVC;
    description.init.codec_config = fake::span(CONFIG);
    description.template_.start_media_ms = 10;
    description.template_.delta_group = 1;
    if !is_live {
        description.has_track_duration = true;
        description.track_duration_ms = 777;
    }
    description
}

fn set_description(receiver: &Receiver, index: usize, description: &sys::moq_media_track_desc_t) {
    // SAFETY: the provider copies the record; the spans point at statics.
    unsafe { fake::moq5_fake_set_track_desc(receiver.as_ptr(), index, description) }
}

#[test]
fn attach_maps_the_presets_and_every_field() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = Receiver::attach(&endpoint, &ReceiverConfig::live(namespace()))
        .expect("the provider accepts the receiver attach");
    let line_text = fake::line(&s.take_log(), "receiver_attach ");
    assert_eq!(fake::field(&line_text, "ns"), "live/demo");
    assert_eq!(fake::field(&line_text, "catalog"), "");
    assert_eq!(
        fake::field(&line_text, "policy"),
        sys::MOQ_MEDIA_OVERFLOW_DROP_TO_KEYFRAME.to_string()
    );
    assert_eq!(fake::field(&line_text, "time_mode"), "0");
    assert_eq!(fake::field(&line_text, "max_objects"), "0");
    assert_eq!(fake::field(&line_text, "max_bytes"), "0");
    assert_eq!(fake::field(&line_text, "max_events"), "0");
    assert_eq!(
        fake::field(&line_text, "mpeg2ts"),
        "0/0/0/0",
        "left at the initializer'stats defaults"
    );
    assert_eq!(
        fake::field(&line_text, "companion"),
        sys::MOQ_MEDIA_COMPANION_AUTO.to_string()
    );
    assert_eq!(fake::field(&line_text, "terminals"), "off");
    assert_eq!(
        fake::field(&line_text, "struct_size"),
        std::mem::size_of::<sys::moq_media_receiver_cfg_t>().to_string()
    );
    drop(receiver);
    s.take_log();
    let receiver = Receiver::attach(
        &endpoint,
        &ReceiverConfig::flow_control(namespace())
            .catalog_track("cat")
            .auto_subscribe(true)
            .time_mode(TimeMode::SharedEpoch)
            .queue_limits(64, 1 << 20)
            .max_track_events(16)
            .companion_profile(CompanionProfile::LegacyMsf01Loc01)
            .stream_terminals(8),
    )
    .expect("the provider accepts the receiver attach");
    let line_text = fake::line(&s.take_log(), "receiver_attach ");
    assert_eq!(fake::field(&line_text, "catalog"), "cat");
    assert_eq!(fake::field(&line_text, "auto"), "1");
    assert_eq!(
        fake::field(&line_text, "time_mode"),
        sys::MOQ_MEDIA_TIME_SHARED_EPOCH.to_string()
    );
    assert_eq!(
        fake::field(&line_text, "policy"),
        sys::MOQ_MEDIA_OVERFLOW_FLOW_CONTROL.to_string()
    );
    assert_eq!(fake::field(&line_text, "max_objects"), "64");
    assert_eq!(
        fake::field(&line_text, "max_bytes"),
        (1u64 << 20).to_string()
    );
    assert_eq!(fake::field(&line_text, "max_events"), "16");
    assert_eq!(
        fake::field(&line_text, "companion"),
        sys::MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01.to_string()
    );
    assert_eq!(
        fake::field(&line_text, "terminals"),
        "on:8",
        "the lane'stats two settings travel together"
    );
    drop(receiver);
    s.set(
        "receiver_attach_result",
        i64::from(sys::MOQ_ERR_WRONG_STATE),
    );
    assert_eq!(
        Receiver::attach(&endpoint, &ReceiverConfig::live(namespace())).err(),
        Some(Error::WRONG_STATE)
    );
}

#[test]
fn events_carry_every_stamped_field() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    assert_eq!(
        receiver
            .poll_track()
            .expect("poll_track on a live receiver")
            .map(|e| e.kind()),
        None
    );

    push_event(sys::moq_media_track_event_t {
        kind: sys::MOQ_MEDIA_CATALOG_READY,
        ..Default::default()
    });
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::CatalogReady);
    assert_eq!(event.track(), None);
    assert!(event
        .description()
        .expect("the event's description copies")
        .is_none());
    assert_eq!(event.largest(), None);
    assert_eq!(event.done_status(), None);
    assert_eq!(event.config_generation(), 0);

    let mut update = raw_event(&receiver, sys::MOQ_MEDIA_TRACK_UPDATE_OK, 0);
    update.has_largest = true;
    update.largest_group = 5;
    update.largest_object = 6;
    update.has_expires = true;
    update.expires_ms = 7;
    push_event(update);
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::UpdateOk);
    assert_eq!(
        event.largest(),
        Some(Location {
            group_id: 5,
            object_id: 6
        })
    );
    assert_eq!(event.expires_ms(), Some(7));
    assert_eq!(
        event.track().expect("the event names its track").as_ptr(),
        raw_track(&receiver, 0)
    );

    let mut parse_drop = raw_event(&receiver, sys::MOQ_MEDIA_TRACK_PARSE_DROP, 1);
    parse_drop.parse_drop_class = sys::MOQ_MEDIA_PARSE_DROP_SAP;
    parse_drop.parse_drops_total = 9;
    parse_drop.parse_drops_delta = 2;
    push_event(parse_drop);
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::ParseDrop);
    let drop = event
        .parse_drop()
        .expect("a ParseDrop event reports its drop");
    assert_eq!(drop.class, ParseDropClass::Sap);
    assert_eq!((drop.total, drop.delta), (9, 2));

    let mut local_failure = raw_event(&receiver, sys::MOQ_MEDIA_TRACK_LOCAL_FAILURE, 0);
    local_failure.local_result = sys::MOQ_ERR_NOMEM;
    push_event(local_failure);
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::LocalFailure);
    assert_eq!(event.local_result(), Some(Error::NOMEM));

    let mut fill_terminal = raw_event(&receiver, sys::MOQ_MEDIA_TRACK_FILL_TERMINAL, 0);
    fill_terminal.initiating_request_id = 0;
    fill_terminal.fill_reason = 3;
    fill_terminal.fill_error_domain = 1;
    fill_terminal.fill_error_code = 0;
    fill_terminal.fill_unreported_loss_causes = sys::MOQ_MEDIA_FILL_LOSS_UNKNOWN;
    fill_terminal.fill_unreported_loss_group = 4;
    fill_terminal.fill_unreported_loss_object = 1;
    push_event(fill_terminal);
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::FillTerminal);
    let terminal = event
        .fill_terminal()
        .expect("a FillTerminal event reports its terminal");
    assert_eq!(
        (
            terminal.initiating_request_id,
            terminal.reason,
            terminal.error_domain,
            terminal.error_code
        ),
        (0, 3, 1, 0)
    );
    let loss = event
        .fill_unreported_loss()
        .expect("the fill's unreported loss is stamped");
    assert_eq!(loss.causes, sys::MOQ_MEDIA_FILL_LOSS_UNKNOWN);
    assert_eq!(
        loss.start,
        Location {
            group_id: 4,
            object_id: 1
        }
    );
    assert_eq!(event.local_result(), None, "zero is none");

    let mut delivery_failure = raw_event(&receiver, sys::MOQ_MEDIA_TRACK_DELIVERY_LOCAL_FAILURE, 0);
    delivery_failure.delivery_origin = sys::MOQ_MEDIA_DELIVERY_FILL;
    delivery_failure.delivery_initiating_request_id = 42;
    delivery_failure.local_result = sys::MOQ_ERR_BUFFER;
    push_event(delivery_failure);
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::DeliveryLocalFailure);
    assert_eq!(event.delivery_origin(), Some(DeliveryOrigin::Fill));
    assert_eq!(event.delivery_initiating_request_id(), Some(42));
    assert_eq!(event.local_result(), Some(Error::BUFFER));

    let mut ended = raw_event(&receiver, sys::MOQ_MEDIA_TRACK_ENDED, 0);
    ended.has_done_status = true;
    ended.done_status = 2;
    push_event(ended);
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::Ended);
    assert_eq!(event.done_status(), Some(2));
    push_event(raw_event(&receiver, sys::MOQ_MEDIA_TRACK_ENDED, 0));
    assert_eq!(
        receiver
            .poll_track()
            .expect("poll_track on a live receiver")
            .expect("the pushed event is pollable")
            .done_status(),
        None
    );

    push_event(raw_event(
        &receiver,
        sys::MOQ_MEDIA_TRACK_UPDATE_LOCAL_FAILURE,
        0,
    ));
    assert_eq!(
        receiver
            .poll_track()
            .expect("poll_track on a live receiver")
            .expect("the pushed event is pollable")
            .kind(),
        TrackEventKind::UpdateLocalFailure
    );
    push_event(raw_event(&receiver, 77, 0));
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::Other(77));
    assert_eq!(event.kind().code(), 77);
    assert!(format!("{event:?}").starts_with("TrackEvent"));

    s.set("poll_track_result", i64::from(sys::MOQ_ERR_CLOSED));
    assert_eq!(receiver.poll_track().err(), Some(Error::CLOSED));
}

#[test]
fn event_fields_outside_the_stamp_are_absent() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    let mut update = raw_event(&receiver, sys::MOQ_MEDIA_TRACK_UPDATE_OK, 0);
    update.has_largest = true;
    update.largest_group = 5;
    update.has_done_status = true;
    update.done_status = 2;
    let v0 = std::mem::offset_of!(sys::moq_media_track_event_t, config_generation) + 4;
    s.set("poll_track_stamp", v0 as i64);
    push_event(update);
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::UpdateOk);
    assert_eq!(
        event.track().expect("the event names its track").as_ptr(),
        raw_track(&receiver, 0)
    );
    assert_eq!(event.largest(), None);
    assert_eq!(event.expires_ms(), None);
    assert_eq!(event.parse_drop(), None);
    assert_eq!(event.local_result(), None);
    assert_eq!(event.fill_terminal(), None);
    assert_eq!(event.delivery_origin(), None);
    assert_eq!(event.delivery_initiating_request_id(), None);
    assert_eq!(event.fill_unreported_loss(), None);
    assert_eq!(event.done_status(), None);
    s.set("poll_track_stamp", (v0 - 1) as i64);
    push_event(update);
    assert_eq!(receiver.poll_track().err(), Some(Error::ABI_MISMATCH));
    s.set("poll_track_stamp", 9999);
    push_event(update);
    assert_eq!(receiver.poll_track().err(), Some(Error::ABI_MISMATCH));
}

#[test]
fn descriptions_are_current_snapshots() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    set_description(&receiver, 0, &raw_description(true));
    let track = track(&receiver, 0);
    s.take_log();
    let description = receiver
        .track_description(track)
        .expect("the description of a discovered track copies");
    assert_eq!(
        fake::field(&fake::line(&s.take_log(), "desc_copy "), "out_size"),
        std::mem::size_of::<sys::moq_media_track_desc_t>().to_string()
    );
    assert_eq!(description.name(), b"video");
    assert_eq!(description.codec(), b"avc1.42e01e");
    assert_eq!(description.role(), b"");
    assert_eq!(description.label(), Some(&b"Main"[..]));
    assert_eq!(description.media_type(), Some(MediaType::Video));
    assert_eq!(description.packaging(), Some(Packaging::Cmaf));
    assert_eq!(description.packaging_name(), b"cmaf");
    assert_eq!(description.timescale(), 90000);
    assert_eq!(description.transport_version(), Some(Version::DRAFT_22));
    assert!(description.has_init());
    assert_eq!(
        description.init_info().codec_kind(),
        moq5::cmaf::CodecKind::Avc
    );
    assert_eq!(description.init_info().codec_config(), &[1, 2, 3]);
    assert_eq!(description.init_data(), INIT);
    assert_eq!(description.width(), Some(1920));
    assert_eq!(description.height(), Some(1080));
    assert_eq!(description.samplerate(), None);
    assert_eq!(description.channel_config(), b"");
    assert_eq!(description.framerate_millis(), Some(30000));
    assert_eq!(description.bitrate(), Some(2_000_000));
    assert_eq!(description.max_group_sap(), Some(1));
    assert_eq!(description.max_object_sap(), None);
    assert_eq!(
        description.content_protection_ref_ids().collect::<Vec<_>>(),
        [b"cp0"]
    );
    assert_eq!(description.depends().collect::<Vec<_>>(), [b"video"]);
    assert_eq!(description.event_type(), b"");
    let template = description
        .template()
        .expect("the description carries a media template");
    assert_eq!((template.start_media_ms, template.delta_group), (10, 1));
    assert_eq!(description.track_duration_ms(), None);
    assert!(description.is_live());
    assert_eq!(description.alt_group(), Some(-2));
    assert_eq!(
        description.render_group(),
        Some(0),
        "zero is a value when present"
    );
    assert!(format!("{description:?}").contains("video"));

    // The service rewrites the handle'stats description in place on a
    // live-to-VOD conversion: a new query shows it, the old copy does not.
    set_description(&receiver, 0, &raw_description(false));
    let later = receiver
        .track_description(track)
        .expect("the description of a discovered track copies");
    assert!(!later.is_live());
    assert_eq!(later.track_duration_ms(), Some(777));
    assert!(description.is_live(), "the earlier snapshot is unchanged");
    push_event(raw_event(&receiver, sys::MOQ_MEDIA_TRACK_UPDATED, 0));
    let event = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind(), TrackEventKind::Updated);
    assert!(!event
        .description()
        .expect("the event's description copies")
        .expect("an Updated event carries a description")
        .is_live());

    let mut bare = raw_description(true);
    bare.has_label = false;
    bare.has_alt_group = false;
    bare.has_render_group = false;
    bare.has_template = false;
    bare.info.media_type = 0;
    bare.info.packaging = 0;
    bare.info.transport_version = 0;
    set_description(&receiver, 0, &bare);
    let description = receiver
        .track_description(track)
        .expect("the description of a discovered track copies");
    assert_eq!(description.label(), None);
    assert_eq!(description.alt_group(), None);
    assert_eq!(description.render_group(), None);
    assert_eq!(description.template(), None);
    assert_eq!(description.media_type(), None);
    assert_eq!(description.packaging(), None);
    assert_eq!(description.transport_version(), None);

    s.set("desc_stamp", (sys::MOQ_MEDIA_TRACK_DESC_V0_SIZE) as i64);
    let description = receiver
        .track_description(track)
        .expect("the description of a discovered track copies");
    assert_eq!(description.name(), b"video");
    assert_eq!(description.alt_group(), None, "appended, not reported");
    s.set("desc_stamp", (sys::MOQ_MEDIA_TRACK_DESC_V0_SIZE - 1) as i64);
    assert_eq!(
        receiver.track_description(track).err(),
        Some(Error::ABI_MISMATCH)
    );
    s.set("desc_stamp", 9999);
    assert_eq!(
        receiver.track_description(track).err(),
        Some(Error::ABI_MISMATCH)
    );
    s.set("desc_stamp", 0);
    s.set("desc_result", i64::from(sys::MOQ_ERR_INVAL));
    assert_eq!(receiver.track_description(track).err(), Some(Error::INVAL));
}

#[test]
fn foreign_tracks_never_reach_the_service() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let first = attach(&s, &endpoint);
    let track = track(&first, 0);
    drop(first);
    let second = attach(&s, &endpoint);
    let before = s.get("desc_copy");
    assert_eq!(second.track_description(track).err(), Some(Error::INVAL));
    assert_eq!(second.subscribe_track(track, None), Err(Error::INVAL));
    assert_eq!(second.unsubscribe_track(track), Err(Error::INVAL));
    assert_eq!(second.track_state(track), Err(Error::INVAL));
    assert_eq!(s.get("desc_copy"), before);
    assert!(s.take_log().is_empty());
}

static RELEASES: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn count_release(_ctx: *mut c_void, _data: *const u8, _len: usize) {
    RELEASES.fetch_add(1, Ordering::SeqCst);
}

static PAYLOAD: [u8; 4] = [1, 2, 3, 4];
static SAMPLES: [sys::moq_cmaf_sample_t; 2] = [
    sys::moq_cmaf_sample_t {
        duration: 3000,
        size: 10,
        flags: 0x0200_0000,
        composition_offset: 0,
    },
    sys::moq_cmaf_sample_t {
        duration: 3000,
        size: 12,
        flags: 0x0101_0000,
        composition_offset: -5,
    },
];

fn push_object(receiver: &Receiver) {
    let mut payload = std::ptr::null_mut();
    // SAFETY: static bytes; the release only counts.
    let rc = unsafe {
        sys::moq_rcbuf_wrap(
            sys::moq_alloc_default(),
            PAYLOAD.as_ptr(),
            PAYLOAD.len(),
            Some(count_release),
            std::ptr::null_mut(),
            &mut payload,
        )
    };
    assert_eq!(rc, sys::MOQ_OK);
    let object = sys::moq_media_object_t {
        track: raw_track(receiver, 0),
        packaging: sys::MOQ_MEDIA_PACKAGING_CMAF,
        status: 3,
        end_of_group: true,
        keyframe: true,
        has_capture_time: true,
        capture_time_us: 11,
        decode_time_us: 12,
        composition_offset_us: -1,
        presentation_time_us: 13,
        fragment: fake::span(&PAYLOAD),
        mdat_offset: 1,
        mdat_len: 2,
        sample_count: 2,
        samples: SAMPLES.as_ptr(),
        payload_ref: payload,
        group_id: 5,
        subgroup_id: 7,
        object_id: 6,
        publisher_priority: 9,
        has_subgroup: true,
        has_loss_report: true,
        gap: true,
        gap_group: 4,
        gap_object: 3,
        gap_duration_known: true,
        gap_duration_27mhz: 27_000_000,
        discontinuous: true,
        delivery_origin: sys::MOQ_MEDIA_DELIVERY_FILL,
        initiating_request_id: 42,
        ..Default::default()
    };
    // SAFETY: the provider copies the record and takes the reference.
    unsafe { fake::moq5_fake_push_object(&object) };
}

#[test]
fn objects_own_their_buffers_until_dropped() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    RELEASES.store(0, Ordering::SeqCst);
    assert!(receiver
        .poll_object()
        .expect("poll_object on a live receiver")
        .is_none());
    push_object(&receiver);
    let object = receiver
        .poll_object()
        .expect("poll_object on a live receiver")
        .expect("the pushed object is pollable");
    assert_eq!(
        object.track().expect("the object names its track").as_ptr(),
        raw_track(&receiver, 0)
    );
    assert_eq!(object.packaging(), Some(Packaging::Cmaf));
    assert_eq!(object.status(), ObjectStatus::EndOfGroup);
    assert!(object.end_of_group());
    assert!(object.is_keyframe());
    assert!(!object.is_datagram());
    assert_eq!(object.capture_time_us(), Some(11));
    assert_eq!(object.decode_time_us(), 12);
    assert_eq!(object.composition_offset_us(), -1);
    assert_eq!(object.presentation_time_us(), 13);
    assert_eq!(object.payload(), b"");
    assert_eq!(object.fragment(), [1, 2, 3, 4]);
    assert_eq!(object.mdat(), [2, 3]);
    assert_eq!(
        object.samples(),
        [
            CmafSample {
                duration: 3000,
                size: 10,
                flags: 0x0200_0000,
                composition_offset: 0
            },
            CmafSample {
                duration: 3000,
                size: 12,
                flags: 0x0101_0000,
                composition_offset: -5
            }
        ]
    );
    assert!(object.samples()[0].is_sync());
    let id = object
        .identity()
        .expect("the object is stamped with its identity");
    assert_eq!(
        id.location,
        Location {
            group_id: 5,
            object_id: 6
        }
    );
    assert_eq!(id.subgroup_id, 7);
    assert!(id.has_subgroup);
    assert_eq!(id.publisher_priority, 9);
    let loss = object
        .loss_report()
        .expect("the object is stamped with its loss report");
    assert!(loss.gap && loss.discontinuous && !loss.gap_tail_unknown);
    assert_eq!(
        loss.gap_start,
        Location {
            group_id: 4,
            object_id: 3
        }
    );
    assert_eq!(loss.gap_duration_27mhz, Some(27_000_000));
    assert_eq!(object.delivery_origin(), Some(DeliveryOrigin::Fill));
    assert_eq!(object.initiating_request_id(), Some(42));
    assert!(format!("{object:?}").contains("EndOfGroup"));
    assert_eq!(s.get("object_cleanup"), 0);
    assert_eq!(RELEASES.load(Ordering::SeqCst), 0);
    drop(object);
    assert_eq!(s.get("object_cleanup"), 1);
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        1,
        "released once, on this thread"
    );
}

#[test]
fn a_refused_object_stamp_releases_the_record_once() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    let v0 = std::mem::offset_of!(sys::moq_media_object_t, samples_owned) + 8;

    RELEASES.store(0, Ordering::SeqCst);
    s.set("poll_object_stamp", (v0 - 1) as i64);
    push_object(&receiver);
    assert_eq!(receiver.poll_object().err(), Some(Error::ABI_MISMATCH));
    assert_eq!(s.get("object_cleanup"), 1);
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        1,
        "short stamp: released once"
    );

    s.set("poll_object_stamp", 9999);
    push_object(&receiver);
    assert_eq!(receiver.poll_object().err(), Some(Error::ABI_MISMATCH));
    assert_eq!(s.get("object_cleanup"), 2);
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        2,
        "oversized stamp: released once"
    );

    s.set("poll_object_stamp", v0 as i64);
    push_object(&receiver);
    let object = receiver
        .poll_object()
        .expect("poll_object on a live receiver")
        .expect("the pushed object is pollable");
    assert_eq!(object.fragment(), [1, 2, 3, 4]);
    assert_eq!(object.identity(), None, "not reported, never absent");
    assert_eq!(object.loss_report(), None);
    assert_eq!(object.delivery_origin(), None);
    assert_eq!(object.initiating_request_id(), None);
    drop(object);
    assert_eq!(s.get("object_cleanup"), 3);
    assert_eq!(RELEASES.load(Ordering::SeqCst), 3);

    s.set("poll_object_stamp", 0);
    endpoint.set_interrupted(true);
    assert_eq!(receiver.poll_object().err(), Some(Error::INTERRUPTED));
    assert_eq!(
        receiver.wait(Duration::from_millis(1)),
        Err(Error::INTERRUPTED)
    );
    endpoint.set_interrupted(false);
    s.set("poll_object_result", i64::from(sys::MOQ_ERR_CLOSED));
    assert_eq!(receiver.poll_object().err(), Some(Error::CLOSED));
}

#[test]
fn the_stream_terminal_lane_is_typed() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    assert!(receiver
        .poll_stream_terminal()
        .expect("poll_stream_terminal on a live receiver")
        .is_none());
    let base = sys::moq_media_stream_terminal_t {
        struct_size: std::mem::size_of::<sys::moq_media_stream_terminal_t>() as u32,
        track: raw_track(&receiver, 1),
        ..Default::default()
    };
    // SAFETY: the provider copies each record.
    unsafe {
        fake::moq5_fake_push_stream_terminal(&sys::moq_media_stream_terminal_t {
            kind: sys::MOQ_MEDIA_STREAM_END_OF_TRACK,
            group_id: 3,
            object_id: 4,
            delivery_origin: sys::MOQ_MEDIA_DELIVERY_LIVE,
            ..base
        });
        fake::moq5_fake_push_stream_terminal(&sys::moq_media_stream_terminal_t {
            kind: sys::MOQ_MEDIA_STREAM_RESET,
            group_id: 3,
            subgroup_id: 0,
            error_code: 0x10,
            has_last_object: true,
            last_object_id: 2,
            has_cut_object: true,
            cut_object_id: 3,
            delivery_origin: sys::MOQ_MEDIA_DELIVERY_LIVE,
            ..base
        });
        fake::moq5_fake_push_stream_terminal(&sys::moq_media_stream_terminal_t {
            kind: sys::MOQ_MEDIA_STREAM_RECEIVER_DROP,
            group_id: 1,
            last_group_id: 2,
            delivery_origin: sys::MOQ_MEDIA_DELIVERY_FILL,
            initiating_request_id: 0,
            ..base
        });
        fake::moq5_fake_push_stream_terminal(&sys::moq_media_stream_terminal_t { kind: 9, ..base });
    }
    let record = receiver
        .poll_stream_terminal()
        .expect("poll_stream_terminal on a live receiver")
        .expect("the pushed record is pollable");
    assert_eq!(record.kind, StreamTerminalKind::EndOfTrack);
    assert_eq!(record.track.as_ptr(), raw_track(&receiver, 1));
    assert_eq!((record.group_id, record.object_id), (3, 4));
    assert_eq!(record.last_object_id, None);
    assert_eq!(record.cut_object_id, None);
    assert_eq!(record.delivery_origin, DeliveryOrigin::Live);
    let record = receiver
        .poll_stream_terminal()
        .expect("poll_stream_terminal on a live receiver")
        .expect("the pushed record is pollable");
    assert_eq!(record.kind, StreamTerminalKind::Reset);
    assert_eq!(record.error_code, 0x10);
    assert_eq!(record.last_object_id, Some(2));
    assert_eq!(record.cut_object_id, Some(3));
    let record = receiver
        .poll_stream_terminal()
        .expect("poll_stream_terminal on a live receiver")
        .expect("the pushed record is pollable");
    assert_eq!(record.kind, StreamTerminalKind::ReceiverDrop);
    assert_eq!((record.group_id, record.last_group_id), (1, 2));
    assert_eq!(record.delivery_origin, DeliveryOrigin::Fill);
    assert_eq!(record.initiating_request_id, 0);
    let record = receiver
        .poll_stream_terminal()
        .expect("poll_stream_terminal on a live receiver")
        .expect("the pushed record is pollable");
    assert_eq!(record.kind, StreamTerminalKind::Other(9));
    assert_eq!(record.kind.code(), 9);
    assert!(receiver
        .poll_stream_terminal()
        .expect("poll_stream_terminal on a live receiver")
        .is_none());
    s.set(
        "poll_terminal_stamp",
        (sys::MOQ_MEDIA_STREAM_TERMINAL_V0_SIZE - 1) as i64,
    );
    // SAFETY: as above.
    unsafe { fake::moq5_fake_push_stream_terminal(&base) };
    assert_eq!(
        receiver.poll_stream_terminal().err(),
        Some(Error::ABI_MISMATCH)
    );
}

#[test]
fn timeline_records() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    assert!(receiver
        .poll_sap()
        .expect("poll_sap on a live receiver")
        .is_none());
    assert!(receiver
        .poll_media_timeline()
        .expect("poll_media_timeline on a live receiver")
        .is_none());
    // SAFETY: the provider copies each record.
    unsafe {
        fake::moq5_fake_push_sap(&sys::moq_media_sap_record_t {
            struct_size: std::mem::size_of::<sys::moq_media_sap_record_t>() as u32,
            track: raw_track(&receiver, 2),
            group: 8,
            object: 0,
            sap_type: sys::MOQ_SAP_TYPE_1,
            ept_ms: 4000,
        });
        fake::moq5_fake_push_timeline(&sys::moq_media_timeline_record_t {
            struct_size: std::mem::size_of::<sys::moq_media_timeline_record_t>() as u32,
            track: raw_track(&receiver, 3),
            media_time_ms: 1000,
            group: 8,
            object: 0,
            wallclock_ms: 0,
        });
    }
    assert_eq!(
        receiver.wait(Duration::ZERO),
        Ok(WaitResult::Ready),
        "something is pollable"
    );
    let sap = receiver
        .poll_sap()
        .expect("poll_sap on a live receiver")
        .expect("the pushed SAP record is pollable");
    assert_eq!(sap.track.as_ptr(), raw_track(&receiver, 2));
    assert_eq!(
        sap.location,
        Location {
            group_id: 8,
            object_id: 0
        }
    );
    assert_eq!((sap.sap_type, sap.ept_ms), (SapType::Type1, 4000));
    let timeline = receiver
        .poll_media_timeline()
        .expect("poll_media_timeline on a live receiver")
        .expect("the pushed timeline record is pollable");
    assert_eq!(timeline.track.as_ptr(), raw_track(&receiver, 3));
    assert_eq!(
        timeline.location,
        Location {
            group_id: 8,
            object_id: 0
        }
    );
    assert_eq!((timeline.media_time_ms, timeline.wallclock_ms), (1000, 0));
    assert_eq!(
        receiver.wait(Duration::from_millis(5)),
        Ok(WaitResult::TimedOut)
    );
    assert_eq!(receiver.wait(Duration::from_nanos(1)), Err(Error::INVAL));
    s.set("poll_sap_result", i64::from(sys::MOQ_ERR_CLOSED));
    assert_eq!(receiver.poll_sap().err(), Some(Error::CLOSED));
}

#[test]
fn subscriptions_and_state() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    let track = track(&receiver, 0);
    s.take_log();
    assert_eq!(receiver.subscribe_track(track, None), Ok(()));
    assert_eq!(
        receiver.subscribe_track(
            track,
            Some(SubscribeOptions {
                start: StartMode::NextGroup,
                priority: Some(5),
            })
        ),
        Ok(())
    );
    assert_eq!(
        receiver.subscribe_track(track, Some(SubscribeOptions::default())),
        Ok(())
    );
    assert_eq!(receiver.unsubscribe_track(track), Ok(()));
    let log = s.take_log();
    assert_eq!(log[0], "subscribe track=0 config=none");
    let line_text = &log[1];
    assert_eq!(
        fake::field(line_text, "start"),
        sys::MOQ_MEDIA_START_NEXT_GROUP.to_string()
    );
    assert_eq!(fake::field(line_text, "priority"), "5");
    assert_eq!(fake::field(line_text, "location"), "0");
    assert_eq!(fake::field(line_text, "fill"), "0");
    assert_eq!(
        fake::field(line_text, "struct_size"),
        std::mem::size_of::<sys::moq_media_receiver_track_subscribe_cfg_t>().to_string()
    );
    assert_eq!(fake::field(&log[2], "start"), "0");
    assert_eq!(fake::field(&log[2], "priority"), "none");
    assert_eq!(log[3], "unsubscribe track=0");
    s.set("subscribe_result", i64::from(sys::MOQ_ERR_UNSUPPORTED));
    assert_eq!(
        receiver.subscribe_track(track, None),
        Err(Error::UNSUPPORTED)
    );
    s.set("unsubscribe_result", i64::from(sys::MOQ_ERR_WRONG_STATE));
    assert_eq!(receiver.unsubscribe_track(track), Err(Error::WRONG_STATE));

    assert_eq!(receiver.track_state(track), Ok(TrackState::Discovered));
    for (raw, state) in [
        (sys::MOQ_MEDIA_TRACK_STATE_PENDING, TrackState::Pending),
        (sys::MOQ_MEDIA_TRACK_STATE_ACTIVE, TrackState::Active),
        (sys::MOQ_MEDIA_TRACK_STATE_PAUSED_APP, TrackState::PausedApp),
        (
            sys::MOQ_MEDIA_TRACK_STATE_PAUSED_FLOW,
            TrackState::PausedFlow,
        ),
        (sys::MOQ_MEDIA_TRACK_STATE_ENDED, TrackState::Ended),
        (sys::MOQ_MEDIA_TRACK_STATE_FAILED, TrackState::Failed),
        (99, TrackState::Other(99)),
    ] {
        // SAFETY: a live receiver of the provider.
        unsafe { fake::moq5_fake_set_track_state(receiver.as_ptr(), 0, raw) };
        assert_eq!(receiver.track_state(track), Ok(state));
    }
}

static KID: &[u8] = b"00000000-0000-0000-0000-000000000001";
static KIDS: Spans<1> = Spans([sys::moq_bytes_t {
    data: KID.as_ptr(),
    len: 36,
}]);
static SCHEME: &[u8] = b"cbcs";
static SYSTEM: &[u8] = b"edef8ba9-79d6-4ace-a3c8-27dcd51d21ed";
static LA: &[u8] = b"https://license";

#[test]
fn content_protection_resolves_against_the_catalog() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    assert!(receiver.find_content_protection(b"cp0").is_none());
    let protection = sys::moq_cmsf_content_protection_t {
        ref_id: fake::span(REF_ID),
        default_kids: KIDS.0.as_ptr() as *mut _,
        default_kid_count: 1,
        scheme: fake::span(SCHEME),
        drm_system: sys::moq_cmsf_drm_system_t {
            system_id: fake::span(SYSTEM),
            la_url: sys::moq_cmsf_url_t {
                present: true,
                url: fake::span(LA),
                ..Default::default()
            },
            ..Default::default()
        },
    };
    // SAFETY: the provider retains the pointer; protection outlives the receiver.
    unsafe { fake::moq5_fake_set_content_protection(&protection) };
    let found = receiver
        .find_content_protection(b"cp0")
        .expect("the provider retains cp0");
    assert_eq!(found.ref_id(), b"cp0");
    assert_eq!(found.default_kids().collect::<Vec<_>>(), [KID]);
    assert_eq!(found.scheme(), b"cbcs");
    assert_eq!(found.system_id(), SYSTEM);
    assert_eq!(found.la_url(), Some(LA));
    assert_eq!(found.cert_url(), None);
    assert_eq!(found.auth_url(), None);
    assert_eq!(found.pssh(), None);
    assert_eq!(found.robustness(), None);
    assert!(receiver.find_content_protection(b"nope").is_none());
    drop(receiver);
    // SAFETY: as above.
    unsafe { fake::moq5_fake_set_content_protection(std::ptr::null()) };
}

#[test]
fn stats_are_read_under_the_stamp() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    s.set("rstats_catalog_drops", 3);
    s.set("rstats_catalog_complete", 1);
    s.set("rstats_tracks_malformed", 2);
    let stats = receiver.stats().expect("stats on a live receiver");
    assert_eq!(stats.objects_received, 11);
    assert!(!stats.paused);
    assert_eq!(stats.catalog_drops, Some(3));
    assert_eq!(stats.catalog_complete, Some(true));
    assert_eq!(stats.gaps_reported, Some(5));
    assert_eq!(stats.gap_tails_unknown, Some(1));
    assert_eq!(stats.reorder_refused, Some(2));
    assert_eq!(stats.tracks_malformed, Some(2));
    let v0 = std::mem::offset_of!(sys::moq_media_receiver_stats_t, paused) + 1;
    s.set("receiver_stats_stamp", v0 as i64);
    let stats = receiver.stats().expect("stats on a live receiver");
    assert_eq!(stats.pause_transitions, 4);
    assert_eq!(stats.catalog_drops, None);
    assert_eq!(stats.catalog_complete, None);
    assert_eq!(stats.tracks_malformed, None);
    s.set("receiver_stats_stamp", (v0 - 1) as i64);
    assert_eq!(receiver.stats(), Err(Error::ABI_MISMATCH));
    s.set("receiver_stats_stamp", 9999);
    assert_eq!(receiver.stats(), Err(Error::ABI_MISMATCH));
    s.set("receiver_stats_stamp", 0);
    s.set("receiver_is_fatal", 1);
    s.set("receiver_fatal_code", 2);
    assert!(!receiver.is_closed());
    assert!(receiver.is_fatal());
    assert_eq!(receiver.fatal_code(), 2);
    assert_eq!(receiver.endpoint().as_ptr(), endpoint.as_ptr());
}

#[test]
fn receiver_holds_its_endpoint() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = attach(&s, &endpoint);
    let clone = endpoint.clone();
    drop(endpoint);
    drop(clone);
    assert!(s.take_log().is_empty());
    drop(receiver);
    assert_eq!(s.take_log(), ["receiver_destroy", "stop", "destroy"]);
}
