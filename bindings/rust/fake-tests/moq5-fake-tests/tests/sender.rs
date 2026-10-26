//! Sender rows: configuration mapping through the sized presets, track
//! configuration (the MPEG-TS record and input, draft-22 track properties),
//! write transfer and return with explicit metadata, MPEG-TS byte-stream
//! writes and their consumed counts, handle ownership, refusals, the
//! stamped statistics (sender-wide and per track), new-group requests and
//! the demand queries.

use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use moq5::{
    Backpressure, Buffer, CmsfUrl, CompanionProfile, ContentProtection, DrmSystem, Endpoint,
    EndpointConfig, Error, LocMetadata, Marking, MediaType, Mpeg2tsInput, Mpeg2tsMode,
    Mpeg2tsRecord, Mpeg2tsTimestampMode, Mpeg2tsWriteError, Namespace, Packaging, SapType,
    SendObject, SendTrackConfig, Sender, SenderConfig, WaitResult,
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

fn video() -> SendTrackConfig {
    SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e").bitrate(1)
}

static RELEASES: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn count_release(_ctx: *mut c_void, _data: *const u8, _len: usize) {
    RELEASES.fetch_add(1, Ordering::SeqCst);
}

static FRAME: [u8; 5] = *b"frame";

fn counted_buffer() -> Buffer {
    // SAFETY: static bytes; the release only counts.
    unsafe {
        Buffer::wrap_raw(
            FRAME.as_ptr(),
            FRAME.len(),
            Some(count_release),
            std::ptr::null_mut(),
        )
    }
    .expect("a static frame wraps")
}

#[test]
fn attach_maps_the_presets_and_every_field() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    let attach = fake::line(&s.take_log(), "sender_attach ");
    assert_eq!(fake::field(&attach, "ns"), "live/demo");
    assert_eq!(fake::field(&attach, "catalog"), "");
    assert_eq!(
        fake::field(&attach, "bp"),
        sys::MOQ_MEDIA_SEND_BP_DROP_TO_KEYFRAME.to_string()
    );
    assert_eq!(
        fake::field(&attach, "validate_cmaf"),
        "1",
        "the preset's default"
    );
    assert_eq!(
        fake::field(&attach, "companion"),
        sys::MOQ_MEDIA_COMPANION_AUTO.to_string()
    );
    assert_eq!(
        fake::field(&attach, "callbacks"),
        "0",
        "no callbacks are installed"
    );
    assert_eq!(fake::field(&attach, "cps"), "0[]");
    assert_eq!(
        fake::field(&attach, "struct_size"),
        std::mem::size_of::<sys::moq_media_sender_cfg_t>().to_string()
    );
    drop(sender);
    s.take_log();

    let config = SenderConfig::lossless(namespace())
        .catalog_track("cat")
        .backpressure(Backpressure::ReturnWouldBlock)
        .block_timeout(Duration::from_millis(1500))
        .queue_limits(10, 20)
        .pre_ready_limits(3, 4)
        .validate_cmaf(false)
        .publish_tracks(true)
        .drop_without_demand(true)
        .catalog_refresh_interval(Duration::from_secs(5))
        .companion_profile(CompanionProfile::D22Msf01Loc04Public)
        .content_protections(vec![ContentProtection {
            ref_id: "cp0".into(),
            default_kids: vec![
                "00000000-0000-0000-0000-000000000001".into(),
                "00000000-0000-0000-0000-000000000002".into(),
            ],
            scheme: "cenc".into(),
            drm_system: DrmSystem {
                system_id: "uuid".into(),
                la_url: Some(CmsfUrl::new("https://la")),
                pssh: Some("AAAA".into()),
                ..Default::default()
            },
        }]);
    let sender =
        Sender::attach(&endpoint, &config).expect("the provider accepts the sender attach");
    let attach = fake::line(&s.take_log(), "sender_attach ");
    assert_eq!(fake::field(&attach, "catalog"), "cat");
    assert_eq!(
        fake::field(&attach, "bp"),
        sys::MOQ_MEDIA_SEND_BP_RETURN_WOULD_BLOCK.to_string()
    );
    assert_eq!(fake::field(&attach, "block_timeout_us"), "1500000");
    assert_eq!(fake::field(&attach, "queue"), "10/20");
    assert_eq!(fake::field(&attach, "pre_ready"), "3/4");
    assert_eq!(fake::field(&attach, "validate_cmaf"), "0");
    assert_eq!(fake::field(&attach, "publish"), "1");
    assert_eq!(fake::field(&attach, "drop_without_demand"), "1");
    assert_eq!(fake::field(&attach, "refresh_us"), "5000000");
    assert_eq!(
        fake::field(&attach, "companion"),
        sys::MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC.to_string()
    );
    assert_eq!(fake::field(&attach, "cps"), "1[cp0:2:cenc:uuid:la+pssh]");
    drop(sender);
    s.take_log();

    let sender = Sender::attach(&endpoint, &SenderConfig::lossless(namespace()))
        .expect("the provider accepts the sender attach");
    let attach = fake::line(&s.take_log(), "sender_attach ");
    assert_eq!(
        fake::field(&attach, "bp"),
        sys::MOQ_MEDIA_SEND_BP_BLOCK_TIMEOUT.to_string()
    );
    assert_eq!(
        fake::field(&attach, "block_timeout_us"),
        "2000000",
        "the preset's default"
    );
    drop(sender);
}

#[test]
fn attach_refuses_inexact_durations_before_the_service() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let refused = Sender::attach(
        &endpoint,
        &SenderConfig::live(namespace()).block_timeout(Duration::from_nanos(999)),
    )
    .err();
    assert_eq!(refused, Some(Error::INVAL));
    let refused = Sender::attach(
        &endpoint,
        &SenderConfig::live(namespace()).catalog_refresh_interval(Duration::MAX),
    )
    .err();
    assert_eq!(refused, Some(Error::INVAL));
    assert_eq!(s.get("sender_attach"), 0);
    let sender = Sender::attach(
        &endpoint,
        &SenderConfig::live(namespace()).catalog_refresh_interval(Duration::ZERO),
    )
    .expect("the provider accepts the sender attach");
    assert_eq!(
        fake::field(&fake::line(&s.take_log(), "sender_attach "), "refresh_us"),
        "0",
        "zero is off"
    );
    drop(sender);
}

#[test]
fn add_track_maps_every_field() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    s.take_log();
    sender
        .add_track(&video())
        .expect("the provider accepts the track");
    let added = fake::line(&s.take_log(), "add_track ");
    assert_eq!(fake::field(&added, "name"), "video");
    assert_eq!(
        fake::field(&added, "media_type"),
        sys::MOQ_MEDIA_TYPE_VIDEO.to_string()
    );
    assert_eq!(
        fake::field(&added, "packaging"),
        sys::MOQ_MEDIA_PACKAGING_RAW.to_string()
    );
    assert_eq!(fake::field(&added, "codec"), "avc1.42e01e");
    assert_eq!(fake::field(&added, "timescale"), "0");
    assert_eq!(fake::field(&added, "is_live"), "1");
    assert_eq!(fake::field(&added, "bitrate"), "1");
    assert_eq!(fake::field(&added, "max_sap"), "none/none");
    assert_eq!(fake::field(&added, "refs"), "0[]");
    assert_eq!(fake::field(&added, "sap_timeline"), "off");
    assert_eq!(fake::field(&added, "duration"), "none");
    assert_eq!(fake::field(&added, "media_timeline"), "off");
    assert_eq!(fake::field(&added, "alt_group"), "none");
    assert_eq!(fake::field(&added, "priority"), "none");
    assert_eq!(fake::field(&added, "extensions"), "0");
    assert_eq!(fake::field(&added, "track_properties"), "0");
    assert_eq!(fake::field(&added, "dynamic_groups"), "0");
    assert_eq!(fake::field(&added, "render_group"), "none");
    assert_eq!(fake::field(&added, "label"), "none");
    assert_eq!(
        fake::field(&added, "struct_size"),
        std::mem::size_of::<sys::moq_media_track_cfg_t>().to_string()
    );

    let full = SendTrackConfig::new("audio", MediaType::Audio, Packaging::Cmaf, "opus")
        .timescale(48000)
        .init_data(vec![1, 2, 3])
        .role("main")
        .lang("en")
        .live(false)
        .dimensions(1920, 1080)
        .framerate_millis(30000)
        .samplerate(48000)
        .channel_config("2")
        .bitrate(128000)
        .max_group_sap(1)
        .max_object_sap(2)
        .content_protection_ref_ids(vec!["cp0".into()])
        .sap_timeline(4)
        .track_duration_ms(1234)
        .media_timeline(0)
        .alt_group(-3)
        .publisher_priority(7)
        .render_group(2)
        .label("Main");
    sender
        .add_track(&full)
        .expect("the provider accepts the track");
    let added = fake::line(&s.take_log(), "add_track ");
    assert_eq!(fake::field(&added, "name"), "audio");
    assert_eq!(
        fake::field(&added, "media_type"),
        sys::MOQ_MEDIA_TYPE_AUDIO.to_string()
    );
    assert_eq!(
        fake::field(&added, "packaging"),
        sys::MOQ_MEDIA_PACKAGING_CMAF.to_string()
    );
    assert_eq!(fake::field(&added, "codec"), "opus");
    assert_eq!(fake::field(&added, "timescale"), "48000");
    assert_eq!(fake::field(&added, "init_data"), "3");
    assert_eq!(fake::field(&added, "role"), "main");
    assert_eq!(fake::field(&added, "lang"), "en");
    assert_eq!(fake::field(&added, "is_live"), "0");
    assert_eq!(fake::field(&added, "dims"), "1920x1080");
    assert_eq!(fake::field(&added, "framerate_millis"), "30000");
    assert_eq!(fake::field(&added, "samplerate"), "48000");
    assert_eq!(fake::field(&added, "channel_config"), "2");
    assert_eq!(fake::field(&added, "bitrate"), "128000");
    assert_eq!(fake::field(&added, "max_sap"), "1/2");
    assert_eq!(fake::field(&added, "refs"), "1[cp0]");
    assert_eq!(fake::field(&added, "sap_timeline"), "on:4");
    assert_eq!(fake::field(&added, "duration"), "1234");
    assert_eq!(fake::field(&added, "media_timeline"), "on:0");
    assert_eq!(fake::field(&added, "alt_group"), "-3");
    assert_eq!(fake::field(&added, "priority"), "7");
    assert_eq!(fake::field(&added, "render_group"), "2");
    assert_eq!(fake::field(&added, "label"), "\"Main\"");

    sender
        .add_track(
            &SendTrackConfig::new("ts", MediaType::Other(5), Packaging::Mpeg2ts, "").label(""),
        )
        .expect("the provider accepts the track");
    let added = fake::line(&s.take_log(), "add_track ");
    assert_eq!(
        fake::field(&added, "media_type"),
        "5",
        "an unknown value passes through"
    );
    assert_eq!(
        fake::field(&added, "packaging"),
        sys::MOQ_MEDIA_PACKAGING_MPEG2TS.to_string()
    );
    assert_eq!(
        fake::field(&added, "label"),
        "\"\"",
        "an empty label is a label"
    );
    s.set("add_track_result", i64::from(sys::MOQ_ERR_WOULD_BLOCK));
    assert_eq!(sender.add_track(&video()).err(), Some(Error::WOULD_BLOCK));
}

#[test]
fn write_transfers_on_success_and_returns_on_failure() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    let track = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    s.take_log();
    RELEASES.store(0, Ordering::SeqCst);

    let mut object = SendObject::new(counted_buffer());
    object.is_sync = true;
    object.starts_group = true;
    object.decode_time_us = 1;
    object.presentation_time_us = 2;
    object.capture_time_us = Some(3);
    object.sap_type = Some(SapType::Type2);
    sender
        .write(track, object)
        .expect("the provider accepts the write");
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        1,
        "released by the service"
    );
    let written = fake::line(&s.take_log(), "write ");
    assert_eq!(fake::field(&written, "track"), "0");
    assert_eq!(fake::field(&written, "len"), "5");
    assert_eq!(fake::field(&written, "props"), "none");
    assert_eq!(fake::field(&written, "sync"), "1");
    assert_eq!(fake::field(&written, "starts"), "1");
    assert_eq!(fake::field(&written, "ends"), "0");
    assert_eq!(fake::field(&written, "dts"), "1");
    assert_eq!(fake::field(&written, "pts"), "2");
    assert_eq!(fake::field(&written, "capture"), "3");
    assert_eq!(
        fake::field(&written, "sap"),
        sys::MOQ_SAP_TYPE_2.to_string()
    );
    assert_eq!(fake::field(&written, "metadata"), "0");
    assert_eq!(
        fake::field(&written, "struct_size"),
        std::mem::size_of::<sys::moq_media_send_object_t>().to_string()
    );

    let mut object = SendObject::new(counted_buffer());
    object.properties = Some(Buffer::copy_from(b"props").expect("a five-byte copy allocates"));
    sender
        .write(track, object)
        .expect("the provider accepts the write");
    let written = fake::line(&s.take_log(), "write ");
    assert_eq!(fake::field(&written, "props"), "5");
    assert_eq!(fake::field(&written, "capture"), "none");
    assert_eq!(fake::field(&written, "sap"), "none");
    assert_eq!(RELEASES.load(Ordering::SeqCst), 2);

    s.set("write_result", i64::from(sys::MOQ_ERR_WOULD_BLOCK));
    let failure = sender
        .write(track, SendObject::new(counted_buffer()))
        .unwrap_err();
    assert_eq!(failure.error, Error::WOULD_BLOCK);
    assert_eq!(failure.object.payload.as_slice(), b"frame");
    assert_eq!(RELEASES.load(Ordering::SeqCst), 2, "still ours");
    assert_eq!(
        failure.to_string(),
        format!("media write failed: {}", Error::WOULD_BLOCK)
    );
    drop(failure);
    assert_eq!(RELEASES.load(Ordering::SeqCst), 3);
    assert_eq!(s.get("write"), 3);
}

#[test]
fn foreign_handles_never_reach_the_service() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let first = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    let track = first
        .add_track(&video())
        .expect("the provider accepts the track");
    drop(first);
    let second = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    s.take_log();
    let failure = second
        .write(
            track,
            SendObject::new(Buffer::copy_from(b"x").expect("a one-byte copy allocates")),
        )
        .unwrap_err();
    assert_eq!(failure.error, Error::INVAL);
    assert_eq!(failure.object.payload.len(), 1, "the object comes back");
    assert_eq!(second.end_track(track), Err(Error::INVAL));
    assert_eq!(second.remove_track(track), Err(Error::INVAL));
    assert_eq!(second.convert_to_vod([(track, 1)]), Err(Error::INVAL));
    assert_eq!(second.track_subscriptions(track), Err(Error::INVAL));
    assert_eq!(second.track_has_subscriber(track), Err(Error::INVAL));
    assert!(s.take_log().is_empty());
    assert_eq!(s.get("write"), 0);
}

#[test]
fn refusals_come_back_as_their_codes() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    let track = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    let other = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    s.take_log();

    endpoint.set_interrupted(true);
    let failure = sender
        .write(
            track,
            SendObject::new(Buffer::copy_from(b"x").expect("a one-byte copy allocates")),
        )
        .unwrap_err();
    assert_eq!(failure.error, Error::INTERRUPTED);
    assert_eq!(sender.end_track(track), Err(Error::INTERRUPTED));
    assert_eq!(sender.complete(), Err(Error::INTERRUPTED));
    assert_eq!(
        sender.wait(Duration::from_millis(1)),
        Err(Error::INTERRUPTED)
    );
    endpoint.set_interrupted(false);

    s.set("write_result", i64::from(sys::MOQ_ERR_CLOSED));
    let failure = sender
        .write(
            track,
            SendObject::new(Buffer::copy_from(b"x").expect("a one-byte copy allocates")),
        )
        .unwrap_err();
    assert_eq!(failure.error, Error::CLOSED);
    s.set("write_result", 0);

    assert_eq!(sender.remove_track(track), Ok(()));
    assert_eq!(
        sender.remove_track(track),
        Err(Error::WRONG_STATE),
        "already removed"
    );
    let failure = sender
        .write(
            track,
            SendObject::new(Buffer::copy_from(b"x").expect("a one-byte copy allocates")),
        )
        .unwrap_err();
    assert_eq!(failure.error, Error::WRONG_STATE, "removed");
    assert_eq!(sender.end_track(other), Ok(()));
    assert_eq!(sender.end_track(other), Ok(()), "idempotent");
    let failure = sender
        .write(
            other,
            SendObject::new(Buffer::copy_from(b"x").expect("a one-byte copy allocates")),
        )
        .unwrap_err();
    assert_eq!(failure.error, Error::WRONG_STATE, "ended");

    s.set("complete_result", i64::from(sys::MOQ_ERR_WRONG_STATE));
    assert_eq!(sender.complete(), Err(Error::WRONG_STATE));
    s.set("complete_result", 0);
    assert_eq!(sender.complete(), Ok(()));
    let log = s.take_log();
    assert!(log.iter().any(|l| l == "complete"));
    assert!(log.iter().any(|l| l == "remove_track 0"));
    assert!(log.iter().any(|l| l == "end_track 1"));
}

#[test]
fn vod_conversion_and_the_write_level() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    let first = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    let second = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    s.take_log();
    assert_eq!(
        sender.convert_to_vod([(first, 5000), (second, 6000)]),
        Ok(())
    );
    assert_eq!(sender.convert_to_vod([]), Err(Error::INVAL));
    assert_eq!(
        sender.wait(Duration::from_millis(250)),
        Ok(WaitResult::TimedOut)
    );
    s.set("sender_wait_result", i64::from(sys::MOQ_OK));
    assert_eq!(sender.wait(Duration::ZERO), Ok(WaitResult::Ready));
    s.set("sender_wait_result", i64::from(sys::MOQ_ERR_UNSUPPORTED));
    assert_eq!(sender.wait(Duration::ZERO), Err(Error::UNSUPPORTED));
    assert_eq!(sender.wait(Duration::from_nanos(5)), Err(Error::INVAL));
    let log = s.take_log();
    assert_eq!(log[0], "convert_to_vod 0:5000,1:6000");
    assert_eq!(log[1], "convert_to_vod ");
    assert_eq!(log[2], "sender_wait 250000");
    assert_eq!(log[3], "sender_wait 0");
    assert_eq!(log.len(), 5);
}

#[test]
fn stats_are_read_under_the_stamp() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    let stats = sender.stats().expect("stats on a live sender");
    assert_eq!(stats.objects_written, 7);
    assert_eq!(
        stats.objects_sent + stats.objects_queued + stats.objects_dropped,
        7
    );
    assert_eq!(stats.last_error, None);
    assert_eq!(
        stats.sap_records_evicted,
        Some(0),
        "a stamped zero is a value"
    );
    assert_eq!(stats.completion_error, None);

    s.set("stats_last_error", i64::from(sys::MOQ_ERR_WOULD_BLOCK));
    s.set("stats_sap_evicted", 4);
    s.set("stats_completion_error", i64::from(sys::MOQ_ERR_INTERNAL));
    let stats = sender.stats().expect("stats on a live sender");
    assert_eq!(stats.last_error, Some(Error::WOULD_BLOCK));
    assert_eq!(stats.sap_records_evicted, Some(4));
    assert_eq!(stats.completion_error, Some(Error::INTERNAL));

    let v0 = std::mem::offset_of!(sys::moq_media_sender_stats_t, last_error)
        + std::mem::size_of::<sys::moq_result_t>();
    s.set("sender_stats_stamp", v0 as i64);
    let stats = sender.stats().expect("stats on a live sender");
    assert_eq!(
        stats.last_error,
        Some(Error::WOULD_BLOCK),
        "within the floor"
    );
    assert_eq!(stats.sap_records_evicted, None, "not reported, never zero");
    assert_eq!(stats.completion_error, None);
    s.set("sender_stats_stamp", (v0 - 1) as i64);
    assert_eq!(sender.stats(), Err(Error::ABI_MISMATCH));
    s.set("sender_stats_stamp", 999);
    assert_eq!(sender.stats(), Err(Error::ABI_MISMATCH));
    s.set("sender_stats_stamp", 0);
    s.set("sender_stats_result", i64::from(sys::MOQ_ERR_INVAL));
    assert_eq!(sender.stats(), Err(Error::INVAL));
}

#[test]
fn demand_and_readiness_are_queries() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    let track = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    assert!(!sender.is_ready());
    s.set("sender_ready", 1);
    assert!(sender.is_ready());
    assert_eq!(sender.track_subscriptions(track), Ok(0));
    assert_eq!(sender.track_has_subscriber(track), Ok(false));
    assert!(!sender.has_media_subscriber());
    s.set("track_subscriptions", 2);
    s.set("has_media_subscriber", 1);
    assert_eq!(sender.track_subscriptions(track), Ok(2));
    assert_eq!(sender.track_has_subscriber(track), Ok(true));
    assert!(sender.has_media_subscriber());
    assert!(!sender.is_closed());
    assert!(!sender.is_fatal());
    s.set("sender_is_closed", 1);
    s.set("sender_is_fatal", 1);
    s.set("sender_fatal_code", 3);
    assert!(sender.is_closed());
    assert!(sender.is_fatal());
    assert_eq!(sender.fatal_code(), 3);
    assert_eq!(sender.endpoint().as_ptr(), endpoint.as_ptr());
}

#[test]
fn sender_holds_its_endpoint() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    s.take_log();
    drop(endpoint);
    assert!(s.take_log().is_empty());
    drop(sender);
    assert_eq!(s.take_log(), ["sender_destroy", "stop", "destroy"]);
}

fn sender(s: &Suite, endpoint: &Endpoint) -> Sender {
    let sender = Sender::attach(endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    s.take_log();
    sender
}

fn transport_stream() -> SendTrackConfig {
    SendTrackConfig::new("ts", MediaType::Video, Packaging::Mpeg2ts, "").bitrate(4_000_000)
}

#[test]
fn add_track_maps_the_mpeg2ts_record_and_input() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = sender(&s, &endpoint);
    let mut record = Mpeg2tsRecord::new(Mpeg2tsMode::PerProgram, 192);
    record.program_number = Some(7);
    record.pcr_pid = Some(0x101);
    record.mux_rate = Some(5_000_000);
    record.si_pids = Some(vec![0x20, 0x21]);
    record.random_access = Some(true);
    record.timestamp_mode = Some(Mpeg2tsTimestampMode::ArrivalTime);
    record.scte35_pid = Some(0x1F0);
    let input = Mpeg2tsInput::ByteStream {
        object_packets: 7,
        min_group: Duration::from_millis(500),
        forward_before_random_access: true,
    };
    sender
        .add_track(&transport_stream().mpeg2ts(record.clone(), input))
        .expect("the provider accepts the track");
    let added = fake::line(&s.take_log(), "add_track ");
    assert_eq!(
        fake::field(&added, "packaging"),
        sys::MOQ_MEDIA_PACKAGING_MPEG2TS.to_string()
    );
    assert_eq!(fake::field(&added, "extensions"), "1");
    assert_eq!(
        fake::field(&added, "mpeg2ts"),
        format!(
            "{}:192:-:7:257:5000000:32+33:1:{}:496:struct={}",
            sys::MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM,
            sys::MOQ_MSF_MPEG2TS_TIMESTAMP_ARRIVAL_TIME,
            std::mem::size_of::<sys::moq_msf_mpeg2ts_t>()
        )
    );
    assert_eq!(fake::field(&added, "mpeg2ts_app_objects"), "0");
    assert_eq!(fake::field(&added, "object_packets"), "7");
    assert_eq!(fake::field(&added, "min_group_ms"), "500");
    assert_eq!(fake::field(&added, "forward"), "1");

    // App objects, and a record with only its required keys and an empty
    // SI PID list (present, with no PID).
    let mut bare = Mpeg2tsRecord::new(Mpeg2tsMode::UnmodifiedProgram, 188);
    bare.si_pids = Some(Vec::new());
    sender
        .add_track(&transport_stream().mpeg2ts(bare, Mpeg2tsInput::AppObjects))
        .expect("the provider accepts the track");
    let added = fake::line(&s.take_log(), "add_track ");
    assert_eq!(
        fake::field(&added, "mpeg2ts"),
        format!(
            "{}:188:-:-:-:-:empty:-:-:-:struct={}",
            sys::MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM,
            std::mem::size_of::<sys::moq_msf_mpeg2ts_t>()
        )
    );
    assert_eq!(fake::field(&added, "mpeg2ts_app_objects"), "1");
    assert_eq!(fake::field(&added, "object_packets"), "0");

    // The service defaults, and a track without the record.
    sender
        .add_track(&transport_stream().mpeg2ts(
            Mpeg2tsRecord::new(Mpeg2tsMode::UnmodifiedProgram, 188),
            Mpeg2tsInput::default(),
        ))
        .expect("the provider accepts the track");
    let added = fake::line(&s.take_log(), "add_track ");
    assert_eq!(fake::field(&added, "mpeg2ts_app_objects"), "0");
    assert_eq!(fake::field(&added, "min_group_ms"), "0");
    assert_eq!(fake::field(&added, "forward"), "0");
    sender
        .add_track(&video())
        .expect("the provider accepts the track");
    assert_eq!(
        fake::field(&fake::line(&s.take_log(), "add_track "), "mpeg2ts"),
        "none"
    );

    // A minimum group finer than a millisecond, or beyond u32
    // milliseconds, is refused before the service.
    let adds = s.get("add_track");
    for min_group in [
        Duration::from_micros(1500),
        Duration::from_millis(u64::from(u32::MAX) + 1),
    ] {
        let config = transport_stream().mpeg2ts(
            Mpeg2tsRecord::new(Mpeg2tsMode::UnmodifiedProgram, 188),
            Mpeg2tsInput::ByteStream {
                object_packets: 0,
                min_group,
                forward_before_random_access: false,
            },
        );
        assert_eq!(sender.add_track(&config).err(), Some(Error::INVAL));
    }
    assert_eq!(s.get("add_track"), adds);
}

#[test]
fn add_track_maps_draft22_track_properties() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = sender(&s, &endpoint);
    sender
        .add_track(
            &video()
                .track_properties(vec![0x04, 0x01, 0x30])
                .dynamic_groups(true),
        )
        .expect("the provider accepts the track");
    let added = fake::line(&s.take_log(), "add_track ");
    assert_eq!(fake::field(&added, "track_properties"), "3");
    assert_eq!(fake::field(&added, "properties"), "[040130]");
    assert_eq!(fake::field(&added, "dynamic_groups"), "1");
    sender
        .add_track(&video())
        .expect("the provider accepts the track");
    let added = fake::line(&s.take_log(), "add_track ");
    assert_eq!(fake::field(&added, "properties"), "[]");
    assert_eq!(fake::field(&added, "dynamic_groups"), "0");
}

#[test]
fn write_carries_explicit_metadata() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = sender(&s, &endpoint);
    let track = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    s.take_log();
    RELEASES.store(0, Ordering::SeqCst);
    let mut object = SendObject::new(counted_buffer());
    object.metadata = Some(LocMetadata {
        timestamp: Some(90),
        timescale: Some(90_000),
        marking: Some(Marking::new(&[0x20, 0x01]).expect("two bytes")),
        audio_level: Some(0x85),
        video_config: Some(vec![1, 2, 3]),
        audio_config: Some(Vec::new()),
    });
    sender
        .write(track, object)
        .expect("the provider accepts the write");
    let written = fake::line(&s.take_log(), "write ");
    assert_eq!(fake::field(&written, "metadata"), "1");
    assert_eq!(
        fake::field(&written, "meta"),
        format!(
            "struct={}:loc_struct={}:present={}:ts=90:scale=90000:mark=2001:level=133:video=3:audio=0",
            std::mem::size_of::<sys::moq_media_metadata_t>(),
            std::mem::size_of::<sys::moq_loc_metadata_t>(),
            sys::MOQ_LOC_METADATA_PRESENT_MASK
        )
    );
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        1,
        "the payload transferred"
    );
    sender
        .write(track, SendObject::new(counted_buffer()))
        .expect("the provider accepts the write");
    let written = fake::line(&s.take_log(), "write ");
    assert_eq!(fake::field(&written, "metadata"), "0");
    assert_eq!(fake::field(&written, "meta"), "none");

    // A refused write hands the metadata back with the object.
    s.set("write_result", i64::from(sys::MOQ_ERR_INVAL));
    let mut object = SendObject::new(counted_buffer());
    object.metadata = Some(LocMetadata {
        timestamp: Some(1),
        ..LocMetadata::default()
    });
    let failure = sender.write(track, object).unwrap_err();
    assert_eq!(failure.error, Error::INVAL);
    assert_eq!(
        failure
            .object
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.timestamp),
        Some(1)
    );
}

#[test]
fn mpeg2ts_writes_report_what_they_took() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = sender(&s, &endpoint);
    let track = sender
        .add_track(&transport_stream().mpeg2ts(
            Mpeg2tsRecord::new(Mpeg2tsMode::UnmodifiedProgram, 188),
            Mpeg2tsInput::default(),
        ))
        .expect("the provider accepts the track");
    s.take_log();
    let bytes = vec![0x47u8; 188 * 3];
    assert_eq!(sender.write_mpeg2ts(track, &bytes, false), Ok(564));
    assert_eq!(
        s.take_log(),
        [format!(
            "write_mpeg2ts track=0 len=564 end_object=0 first=71 struct_size={}",
            std::mem::size_of::<sys::moq_media_send_mpeg2ts_t>()
        )]
    );
    // A short write is progress.
    s.set("mpeg2ts_consumed", 188);
    assert_eq!(sender.write_mpeg2ts(track, &bytes, true), Ok(188));
    assert_eq!(
        fake::field(&fake::line(&s.take_log(), "write_mpeg2ts "), "end_object"),
        "1"
    );
    // Empty data queues held objects.
    s.set("mpeg2ts_consumed", 0);
    assert_eq!(sender.write_mpeg2ts(track, &[], false), Ok(0));
    assert_eq!(
        fake::field(&fake::line(&s.take_log(), "write_mpeg2ts "), "len"),
        "0"
    );
    // Every failure carries what was taken before it: a refusal checked at
    // entry takes nothing, and one latched while the call yielded between
    // Objects reports the whole packets it took first.
    for (code, taken) in [
        (sys::MOQ_ERR_WOULD_BLOCK, -1),
        (sys::MOQ_ERR_NOMEM, 376),
        (sys::MOQ_ERR_PROTO, 188),
        (sys::MOQ_ERR_WRONG_STATE, -1),
        (sys::MOQ_ERR_UNSUPPORTED, -1),
        (sys::MOQ_ERR_UNSUPPORTED, 376),
        (sys::MOQ_ERR_CLOSED, -1),
    ] {
        s.set("write_mpeg2ts_result", i64::from(code));
        s.set("mpeg2ts_consumed", taken);
        assert_eq!(
            sender.write_mpeg2ts(track, &bytes, false),
            Err(Mpeg2tsWriteError {
                error: Error::from_code(code),
                consumed: taken.max(0) as usize,
            })
        );
    }
    s.set("write_mpeg2ts_result", 0);
    s.set("mpeg2ts_consumed", 188);
    endpoint.set_interrupted(true);
    let failure = sender.write_mpeg2ts(track, &bytes, false).unwrap_err();
    assert_eq!(
        (failure.error, failure.consumed),
        (Error::INTERRUPTED, 188),
        "an interrupt reports the bytes taken before it"
    );
    assert_eq!(Error::from(failure), Error::INTERRUPTED);
    endpoint.set_interrupted(false);
    assert!(failure.to_string().contains("188"));

    // A foreign track reports nothing taken and never reaches the service.
    let other = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    drop(sender);
    let second = Sender::attach(&endpoint, &SenderConfig::live(namespace()))
        .expect("the provider accepts the sender attach");
    let writes = s.get("write_mpeg2ts");
    assert_eq!(
        second.write_mpeg2ts(other, &bytes, false),
        Err(Mpeg2tsWriteError {
            error: Error::INVAL,
            consumed: 0
        })
    );
    assert_eq!(s.get("write_mpeg2ts"), writes);
}

#[test]
fn track_stats_are_read_under_the_stamp() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = sender(&s, &endpoint);
    let track = sender
        .add_track(&video())
        .expect("the provider accepts the track");
    s.set("track_stats_parked", 2);
    s.set("track_stats_program_gone", 1);
    let stats = sender
        .track_stats(track)
        .expect("stats of one of the sender's tracks");
    assert_eq!(
        (
            stats.objects_written,
            stats.objects_sent,
            stats.objects_queued,
            stats.objects_dropped
        ),
        (9, 5, 3, 1)
    );
    assert_eq!(
        stats.objects_written,
        stats.objects_sent + stats.objects_queued + stats.objects_dropped
    );
    assert_eq!(stats.bytes_queued, 600);
    assert_eq!((stats.groups_dropped, stats.keyframes_dropped), (1, 1));
    assert_eq!(stats.groups_abandoned, 2);
    let ts = stats.mpeg2ts;
    assert_eq!((ts.groups, ts.objects, ts.packets_in), (4, 12, 48));
    assert_eq!(ts.packets_dropped_before_random_access, 3);
    assert_eq!(ts.max_group_27mhz, 27_000_000);
    assert_eq!(ts.groups_over_2s, 0);
    assert_eq!(ts.peak_rate_bps, 4_000_000);
    assert_eq!(ts.source_pcr_discontinuities, 1);
    assert!(ts.mux_rate_below_peak);
    assert!(ts.program_gone);
    assert_eq!(ts.objects_parked, 2);
    assert_eq!(ts.partial_bytes_discarded, 7);
    assert_eq!(ts.continuation_objects_discarded, 2);
    assert_eq!(
        fake::field(&fake::line(&s.take_log(), "track_stats "), "out_size"),
        std::mem::size_of::<sys::moq_media_sender_track_stats_t>().to_string()
    );
    let full = std::mem::size_of::<sys::moq_media_sender_track_stats_t>();
    s.set("track_stats_stamp", (full - 1) as i64);
    assert_eq!(sender.track_stats(track), Err(Error::ABI_MISMATCH));
    s.set("track_stats_stamp", 9999);
    assert_eq!(sender.track_stats(track), Err(Error::ABI_MISMATCH));
    s.set("track_stats_stamp", 0);
    s.set("track_stats_result", i64::from(sys::MOQ_ERR_INVAL));
    assert_eq!(sender.track_stats(track), Err(Error::INVAL));
}

#[test]
fn new_group_requests_are_taken_once() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let sender = sender(&s, &endpoint);
    let track = sender
        .add_track(&video().dynamic_groups(true))
        .expect("the provider accepts the track");
    s.take_log();
    assert_eq!(sender.take_new_group_request(track), Ok(None));
    s.set("new_group_pending", 1);
    s.set("new_group_value", 0);
    assert_eq!(
        sender.take_new_group_request(track),
        Ok(Some(0)),
        "zero: the subscriber knows no group"
    );
    assert_eq!(sender.take_new_group_request(track), Ok(None), "cleared");
    s.set("new_group_pending", 1);
    s.set("new_group_value", 1 << 61);
    assert_eq!(sender.take_new_group_request(track), Ok(Some(1 << 61)));
    assert_eq!(
        s.take_log().first().map(String::as_str),
        Some("take_new_group_request track=0 out=1")
    );
}
