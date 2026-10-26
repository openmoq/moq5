//! Player rows: the configuration through the sized initializers, attach
//! and its refusal (the receiver comes back), the receiver a created player
//! attaches, selection and its ownership check, progress and waiting as the
//! library reports them, outputs that own their references until dropped and
//! are refused (and released) outside the frozen layout, events with every
//! detail, feedback, the stamped statistics, and the order of teardown.

#![cfg(feature = "player")]

use std::ffi::c_void;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use moq5::player::{
    DiscontinuityReason, DropReason, EndCause, EndDisposition, Feedback, FeedbackKind, Gap,
    GapCause, OutputDetail, PlaybackConfig, Player, PlayerConfig, PlayerEventKind, PlayerWait,
    SelectOptions, Selection, SyncReason, Tick, TimingPolicy,
};
use moq5::{
    Endpoint, EndpointConfig, Error, Location, MediaType, Namespace, Packaging, Receiver,
    ReceiverConfig, StartMode, StreamTerminalKind, TimeDomain, TimeMode, Track,
};
use moq5_fake_tests as fake;
use moq5_fake_tests::{sys, Suite};

fn connect(s: &Suite) -> Endpoint {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433"))
        .expect("the provider accepts the connect");
    s.take_log();
    endpoint
}

fn receiver_config() -> ReceiverConfig {
    ReceiverConfig::live(Namespace::parse("live/demo").expect("a two-part namespace parses"))
}

fn player(s: &Suite, endpoint: &Endpoint) -> Player {
    let player = Player::create(endpoint, &receiver_config(), &PlayerConfig::new())
        .expect("the provider accepts the player");
    s.take_log();
    player
}

fn raw_track(player: &Player, index: usize) -> *mut sys::moq_media_track_t {
    // SAFETY: the live receiver of a player of the provider.
    unsafe { fake::moq5_fake_receiver_track(player.receiver().as_ptr(), index) }
}

/// The handle a player reports for the provider's track `index`, from a
/// TRACK_AVAILABLE event.
fn track(player: &Player, index: usize) -> Track {
    push_event(sys::moq_media_player_event_t {
        kind: sys::MOQ_MEDIA_PLAYER_EVENT_TRACK_AVAILABLE,
        track: raw_track(player, index),
        ..Default::default()
    });
    let event = player
        .poll_event()
        .expect("poll_event on a live player")
        .expect("the pushed event is pollable");
    assert_eq!(event.kind, PlayerEventKind::TrackAvailable);
    event.track.expect("the event names its track")
}

fn push_event(event: sys::moq_media_player_event_t) {
    // SAFETY: the provider copies the record.
    unsafe { fake::moq5_fake_push_player_event(&event) }
}

fn push_output(output: sys::moq_media_player_output_t) {
    // SAFETY: the provider copies the record and holds its references for
    // the poller.
    unsafe { fake::moq5_fake_push_player_output(&output) }
}

#[test]
fn attach_maps_every_field() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = Receiver::attach(&endpoint, &receiver_config().auto_subscribe(false))
        .expect("the provider accepts the receiver attach");
    s.take_log();
    let player =
        Player::attach(receiver, &PlayerConfig::new()).expect("the provider accepts the player");
    let line_text = fake::line(&s.take_log(), "player_attach ");
    assert_eq!(
        fake::field(&line_text, "selection"),
        "0",
        "the default: AUTO"
    );
    assert_eq!(fake::field(&line_text, "receiver_cfg"), "0", "attach");
    assert_eq!(fake::field(&line_text, "playback"), "none");
    assert_eq!(fake::field(&line_text, "lang"), "");
    assert_eq!(fake::field(&line_text, "tail_us"), "0");
    assert_eq!(
        fake::field(&line_text, "struct_size"),
        std::mem::size_of::<sys::moq_media_player_cfg_t>().to_string()
    );
    let receiver = player.detach();

    let playback = PlaybackConfig::new()
        .max_tracks(2)
        .max_buffered_objects(64)
        .max_buffered_bytes(1 << 20)
        .max_backlog_groups(5)
        .max_commands(32)
        .max_events(8)
        .gap_timeout(Duration::from_millis(250))
        .max_release_per_tick(0)
        .max_track_config_bytes(2048)
        .max_samples_per_object(4)
        .max_known_ranges(16);
    let config = PlayerConfig::new()
        .selection(Selection::Manual)
        .playback(playback)
        .preferred_lang("en")
        .end_tail_wait(Duration::from_secs(3))
        .cushion(Duration::from_millis(40))
        .queue_limits(256, 32)
        .timing_policy(TimingPolicy::AllowIndependent)
        .max_held_bytes(1 << 16)
        .assume_shared_epoch(true);
    let player = Player::attach(receiver, &config).expect("the provider accepts the player");
    let line_text = fake::line(&s.take_log(), "player_attach ");
    assert_eq!(
        fake::field(&line_text, "selection"),
        sys::MOQ_MEDIA_SELECTION_MANUAL.to_string()
    );
    assert_eq!(
        fake::field(&line_text, "playback"),
        format!(
            "2/64/1048576/5/32/8/250000/0/2048/4/16/struct={}",
            std::mem::size_of::<sys::moq_playback_cfg_t>()
        )
    );
    assert_eq!(fake::field(&line_text, "lang"), "en");
    assert_eq!(fake::field(&line_text, "tail_us"), "3000000");
    assert_eq!(fake::field(&line_text, "cushion_us"), "40000");
    assert_eq!(fake::field(&line_text, "outputs"), "256");
    assert_eq!(fake::field(&line_text, "events"), "32");
    assert_eq!(
        fake::field(&line_text, "timing"),
        sys::MOQ_MEDIA_TIMING_ALLOW_INDEPENDENT.to_string()
    );
    assert_eq!(fake::field(&line_text, "held"), "65536");
    assert_eq!(fake::field(&line_text, "shared_epoch"), "1");
    drop(player);
}

#[test]
fn a_refused_attach_returns_the_receiver() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = Receiver::attach(&endpoint, &receiver_config())
        .expect("the provider accepts the receiver attach");
    let raw = receiver.as_ptr();
    s.set("player_attach_result", i64::from(sys::MOQ_ERR_WRONG_STATE));
    let failure = Player::attach(receiver, &PlayerConfig::new()).unwrap_err();
    assert_eq!(failure.error, Error::WRONG_STATE);
    assert_eq!(
        failure.receiver.as_ptr(),
        raw,
        "the same receiver, untouched"
    );
    assert_eq!(s.get("receiver_destroy"), 0);
    assert!(failure.to_string().contains("player attach failed"));
    // An inexact duration is refused before the service, receiver returned.
    s.set("player_attach_result", 0);
    let attaches = s.get("player_attach");
    let failure = Player::attach(
        failure.receiver,
        &PlayerConfig::new().cushion(Duration::from_nanos(1)),
    )
    .unwrap_err();
    assert_eq!(failure.error, Error::INVAL);
    assert_eq!(s.get("player_attach"), attaches);
    let failure = Player::attach(
        failure.receiver,
        &PlayerConfig::new().playback(PlaybackConfig::new().gap_timeout(Duration::from_nanos(1))),
    )
    .unwrap_err();
    assert_eq!(failure.error, Error::INVAL);
    assert_eq!(s.get("player_attach"), attaches);
    assert_eq!(Error::from(failure), Error::INVAL);
    assert_eq!(s.get("receiver_destroy"), 1, "dropped with the error");
}

#[test]
fn create_attaches_the_receiver_a_player_needs() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = Player::create(
        &endpoint,
        &receiver_config().auto_subscribe(true).stream_terminals(32),
        &PlayerConfig::new(),
    )
    .expect("the provider accepts the player");
    let log = s.take_log();
    let attach = fake::line(&log, "receiver_attach ");
    assert_eq!(fake::field(&attach, "auto"), "0", "never auto-subscribes");
    assert_eq!(
        fake::field(&attach, "terminals"),
        "on:32",
        "the lane as given"
    );
    assert!(log[1].starts_with("player_attach "), "{log:#?}");
    drop(player);
    let player = Player::create(&endpoint, &receiver_config(), &PlayerConfig::new())
        .expect("the provider accepts the player");
    assert_eq!(
        fake::field(&fake::line(&s.take_log(), "receiver_attach "), "terminals"),
        "on:0",
        "the lane on, at the service's depth"
    );
    drop(player);
    // A shared epoch is refused before anything is attached.
    let receivers = s.get("receiver_attach");
    assert_eq!(
        Player::create(
            &endpoint,
            &receiver_config().time_mode(TimeMode::SharedEpoch),
            &PlayerConfig::new(),
        )
        .err(),
        Some(Error::UNSUPPORTED)
    );
    assert_eq!(s.get("receiver_attach"), receivers);
    // A refused player destroys the receiver it attached.
    s.set("player_attach_result", i64::from(sys::MOQ_ERR_NOMEM));
    let destroyed = s.get("receiver_destroy");
    assert_eq!(
        Player::create(&endpoint, &receiver_config(), &PlayerConfig::new()).err(),
        Some(Error::NOMEM)
    );
    assert_eq!(s.get("receiver_destroy"), destroyed + 1);
}

#[test]
fn selection_maps_its_options_and_checks_ownership() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = player(&s, &endpoint);
    let track = track(&player, 2);
    assert_eq!(player.select_track(track, None), Ok(()));
    assert_eq!(
        player.select_track(
            track,
            Some(SelectOptions {
                start: StartMode::NextGroup,
                independent_timing: true,
                shared_epoch: true,
            }),
        ),
        Ok(())
    );
    assert_eq!(player.deselect_track(track), Ok(()));
    let log = s.take_log();
    assert_eq!(log[0], "select track=2 config=none");
    assert_eq!(
        log[1],
        format!(
            "select track=2 start={} independent=1 shared_epoch=1 struct_size={}",
            sys::MOQ_MEDIA_START_NEXT_GROUP,
            std::mem::size_of::<sys::moq_media_player_select_cfg_t>()
        )
    );
    assert_eq!(log[2], "deselect track=2");
    for code in [
        sys::MOQ_ERR_UNSUPPORTED,
        sys::MOQ_ERR_WRONG_STATE,
        sys::MOQ_ERR_LIMIT,
    ] {
        s.set("select_result", i64::from(code));
        assert_eq!(
            player.select_track(track, None),
            Err(Error::from_code(code))
        );
    }
    s.set("deselect_result", i64::from(sys::MOQ_ERR_INVAL));
    assert_eq!(player.deselect_track(track), Err(Error::INVAL));
    s.take_log();
    // A track of another receiver never reaches the player.
    let receiver = Receiver::attach(&endpoint, &receiver_config())
        .expect("the provider accepts the receiver attach");
    drop(player);
    let other =
        Player::attach(receiver, &PlayerConfig::new()).expect("the provider accepts the player");
    s.take_log();
    assert_eq!(other.select_track(track, None), Err(Error::INVAL));
    assert_eq!(other.deselect_track(track), Err(Error::INVAL));
    let feedback = Feedback {
        track,
        configuration_generation: 1,
        kind: FeedbackKind::FlushComplete,
    };
    assert_eq!(other.handle_feedback(&feedback, 0), Err(Error::INVAL));
    assert!(s.take_log().is_empty());
}

#[test]
fn progress_and_waiting_are_reported_as_the_library_reports_them() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = player(&s, &endpoint);
    assert_eq!(player.tick(5), Ok(Tick::Done));
    s.set("tick_result", i64::from(sys::MOQ_ERR_WOULD_BLOCK));
    assert_eq!(player.tick(6), Ok(Tick::OutputsFull), "work done stands");
    s.set("tick_result", i64::from(sys::MOQ_ERR_CLOSED));
    assert_eq!(player.tick(7), Err(Error::CLOSED));
    assert_eq!(s.take_log(), ["tick 5", "tick 6", "tick 7"]);

    assert_eq!(
        player.wait(10, Duration::from_millis(20)),
        Ok(PlayerWait::Idle),
        "nothing to do"
    );
    s.set("player_wait_result", i64::from(sys::MOQ_OK));
    assert_eq!(player.wait(11, Duration::ZERO), Ok(PlayerWait::Ready));
    s.set("player_wait_result", i64::from(sys::MOQ_ERR_CLOSED));
    assert_eq!(player.wait(12, Duration::ZERO), Err(Error::CLOSED));
    endpoint.set_interrupted(true);
    assert_eq!(player.wait(13, Duration::ZERO), Err(Error::INTERRUPTED));
    endpoint.set_interrupted(false);
    assert_eq!(
        player.wait(14, Duration::from_nanos(1)),
        Err(Error::INVAL),
        "an inexact timeout is refused before the player"
    );
    let waits: Vec<String> = s
        .take_log()
        .into_iter()
        .filter(|line| line.starts_with("player_wait "))
        .collect();
    assert_eq!(
        waits,
        [
            "player_wait now=10 timeout=20000",
            "player_wait now=11 timeout=0",
            "player_wait now=12 timeout=0",
            "player_wait now=13 timeout=0",
        ],
        "the inexact timeout never reached the player"
    );

    assert_eq!(player.next_deadline_us(), None);
    s.set("player_deadline", 1234);
    assert_eq!(player.next_deadline_us(), Some(1234));
}

static RELEASES: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn count_release(_ctx: *mut c_void, _data: *const u8, _len: usize) {
    RELEASES.fetch_add(1, Ordering::SeqCst);
}

static CODEC: &[u8] = b"avc1.42e01e";
static CONFIG: &[u8] = &[1, 0x42, 0xe0, 0x1e];
static UNIT: &[u8] = b"xxABCDyy";

/// A counted rcbuf over static bytes.
fn counted(bytes: &'static [u8]) -> *mut sys::moq_rcbuf_t {
    let mut buffer = std::ptr::null_mut();
    // SAFETY: static bytes; the release only counts.
    let rc = unsafe {
        sys::moq_rcbuf_wrap(
            sys::moq_alloc_default(),
            bytes.as_ptr(),
            bytes.len(),
            Some(count_release),
            std::ptr::null_mut(),
            &mut buffer,
        )
    };
    assert_eq!(rc, sys::MOQ_OK);
    buffer
}

fn output(player: &Player, kind: u32) -> sys::moq_media_player_output_t {
    let mut output = sys::moq_media_player_output_t {
        kind,
        timeline_id: 3,
        configuration_generation: 4,
        track: raw_track(player, 1),
        ..Default::default()
    };
    // SAFETY: only the sizes of the union's members are taken.
    output.detail_size = unsafe {
        match kind {
            sys::MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE => std::mem::size_of_val(&output.u.configure),
            sys::MOQ_MEDIA_PLAYER_OUTPUT_UNIT => std::mem::size_of_val(&output.u.unit),
            sys::MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY => {
                std::mem::size_of_val(&output.u.discontinuity)
            }
            sys::MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE => std::mem::size_of_val(&output.u.timeline),
            sys::MOQ_MEDIA_PLAYER_OUTPUT_END => std::mem::size_of_val(&output.u.end),
            _ => 0,
        }
    } as u32;
    output
}

#[test]
fn outputs_own_their_references_until_dropped() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = player(&s, &endpoint);
    RELEASES.store(0, Ordering::SeqCst);
    assert!(player
        .poll_output()
        .expect("poll_output on a live player")
        .is_none());

    let mut configure = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE);
    configure.u.configure = sys::moq_media_player_output__bindgen_ty_1__bindgen_ty_1 {
        media_type: sys::MOQ_MEDIA_TYPE_VIDEO,
        packaging: sys::MOQ_MEDIA_PACKAGING_RAW,
        codec: counted(CODEC),
        codec_config: counted(CONFIG),
        timescale: 1_000_000,
        width: 640,
        height: 360,
        samplerate: 0,
        channel_count: 0,
    };
    push_output(configure);
    let mut unit = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    unit.u.unit = sys::moq_media_player_output__bindgen_ty_1__bindgen_ty_2 {
        group_id: 5,
        object_id: 2,
        decode_time_us: 1000,
        presentation_time_us: 1000,
        composition_offset_us: 0,
        has_capture_time: true,
        independence_known: true,
        independent: false,
        present: true,
        sample_duration_us: 0,
        capture_time_us: 77,
        payload: counted(UNIT),
        offset: 2,
        len: 4,
    };
    push_output(unit);

    let first = player
        .poll_output()
        .expect("poll_output on a live player")
        .expect("the pushed output is pollable");
    assert_eq!(
        first.track().map(|t| t.as_ptr()),
        Some(raw_track(&player, 1))
    );
    assert_eq!(first.timeline_id(), 3);
    assert_eq!(first.configuration_generation(), 4);
    let OutputDetail::Configure(config) = first.detail() else {
        panic!("a configuration: {first:?}");
    };
    assert_eq!(config.media_type(), Some(MediaType::Video));
    assert_eq!(config.packaging(), Some(Packaging::Raw));
    assert_eq!(config.codec(), CODEC);
    assert_eq!(config.codec_config(), Some(CONFIG));
    assert_eq!((config.width(), config.height()), (640, 360));
    assert_eq!(config.timescale(), 1_000_000);
    // Two outputs held at once; nothing released yet.
    let second = player
        .poll_output()
        .expect("poll_output on a live player")
        .expect("the pushed output is pollable");
    let OutputDetail::Unit(unit) = second.detail() else {
        panic!("a unit: {second:?}");
    };
    assert_eq!(
        unit.location(),
        Location {
            group_id: 5,
            object_id: 2
        }
    );
    assert_eq!(unit.data(), b"ABCD", "the unit's bytes in the payload");
    assert_eq!(
        (unit.decode_time_us(), unit.presentation_time_us()),
        (1000, 1000)
    );
    assert_eq!(unit.capture_time_us(), Some(77));
    assert_eq!(unit.independent(), Some(false));
    assert!(unit.present());
    assert_eq!(unit.sample_duration_us(), None, "zero is unknown");
    assert_eq!(RELEASES.load(Ordering::SeqCst), 0);
    assert_eq!(s.get("output_cleanup"), 0);
    drop(first);
    assert_eq!(
        RELEASES.load(Ordering::SeqCst),
        2,
        "codec and configuration"
    );
    drop(second);
    assert_eq!(RELEASES.load(Ordering::SeqCst), 3, "the payload");
    assert_eq!(s.get("output_cleanup"), 2);
    assert_eq!(s.get("output_refs_released"), 3);

    // The plain kinds, an audio configuration without extradata, and a
    // kind this crate does not read, which is released all the same.
    let mut discontinuity = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_DISCONTINUITY);
    discontinuity.u.discontinuity.reason = sys::MOQ_MEDIA_PLAYER_DISCONTINUITY_DECODE_ERROR;
    discontinuity.u.discontinuity.decoder_reset_required = true;
    push_output(discontinuity);
    let mut timeline = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_TIMELINE);
    timeline.u.timeline = sys::moq_media_timeline_t {
        domain: sys::MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA,
        timeline_id: 3,
        timeline_epoch: 1,
        anchor_media_us: 1000,
        anchor_local_us: 101_000,
        rate: 1.0,
    };
    push_output(timeline);
    let mut end = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_END);
    end.u.end.cause = sys::MOQ_MEDIA_PLAYER_END_PEER_END_OF_TRACK;
    end.u.end.disposition = sys::MOQ_MEDIA_PLAYER_END_COMPLETE;
    push_output(end);
    let mut bare = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_CONFIGURE);
    bare.u.configure.media_type = sys::MOQ_MEDIA_TYPE_AUDIO;
    bare.u.configure.codec = counted(CODEC);
    bare.u.configure.samplerate = 48_000;
    bare.u.configure.channel_count = 2;
    push_output(bare);
    push_output(output(&player, 99));

    let next = || {
        player
            .poll_output()
            .expect("poll_output on a live player")
            .expect("the pushed output is pollable")
    };
    let out = next();
    match out.detail() {
        OutputDetail::Discontinuity(detail) => {
            assert_eq!(detail.reason, DiscontinuityReason::DecodeError);
            assert!(detail.decoder_reset_required);
        }
        other => panic!("a discontinuity: {other:?}"),
    }
    drop(out);
    let out = next();
    match out.detail() {
        OutputDetail::Timeline(detail) => {
            assert_eq!(detail.domain, TimeDomain::ApplicationMedia);
            assert_eq!((detail.timeline_id, detail.timeline_epoch), (3, 1));
            assert_eq!(
                (detail.anchor_media_us, detail.anchor_local_us),
                (1000, 101_000)
            );
            assert_eq!(detail.rate, 1.0);
        }
        other => panic!("a timeline: {other:?}"),
    }
    drop(out);
    let out = next();
    match out.detail() {
        OutputDetail::End(detail) => {
            assert_eq!(detail.cause, EndCause::PeerEndOfTrack);
            assert_eq!(detail.disposition, EndDisposition::Complete);
        }
        other => panic!("an end: {other:?}"),
    }
    drop(out);
    let out = next();
    match out.detail() {
        OutputDetail::Configure(config) => {
            assert_eq!(config.media_type(), Some(MediaType::Audio));
            assert_eq!(config.codec_config(), None);
            assert_eq!((config.samplerate(), config.channel_count()), (48_000, 2));
            assert_eq!((config.width(), config.height()), (0, 0));
        }
        other => panic!("a configuration: {other:?}"),
    }
    drop(out);
    let out = next();
    assert!(matches!(out.detail(), OutputDetail::Other(99)));
    drop(out);
    assert_eq!(RELEASES.load(Ordering::SeqCst), 4);
    assert_eq!(s.get("output_cleanup"), 7, "every polled output, any kind");

    s.set("poll_output_result", i64::from(sys::MOQ_ERR_CLOSED));
    assert_eq!(player.poll_output().err(), Some(Error::CLOSED));
}

#[test]
fn outputs_outside_the_frozen_layout_are_refused_and_released() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = player(&s, &endpoint);
    RELEASES.store(0, Ordering::SeqCst);
    let size = std::mem::size_of::<sys::moq_media_player_output_t>();
    for stamp in [size - 8, size + 8] {
        s.set("poll_output_stamp", stamp as i64);
        let mut unit = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
        unit.u.unit.payload = counted(UNIT);
        push_output(unit);
        assert_eq!(player.poll_output().err(), Some(Error::ABI_MISMATCH));
    }
    assert_eq!(RELEASES.load(Ordering::SeqCst), 2, "each released once");
    s.set("poll_output_stamp", 0);
    // A known kind whose detail does not cover what its view reads.
    let mut unit = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    unit.u.unit.payload = counted(UNIT);
    unit.detail_size -= 1;
    push_output(unit);
    assert_eq!(player.poll_output().err(), Some(Error::ABI_MISMATCH));
    assert_eq!(RELEASES.load(Ordering::SeqCst), 3);
    assert_eq!(s.get("output_cleanup"), 3);
    // Outputs still queued at destroy are the player's to release.
    let mut unit = output(&player, sys::MOQ_MEDIA_PLAYER_OUTPUT_UNIT);
    unit.u.unit.payload = counted(UNIT);
    push_output(unit);
    drop(player);
    assert_eq!(RELEASES.load(Ordering::SeqCst), 4);
}

#[test]
fn events_are_plain_values_with_every_detail() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = player(&s, &endpoint);
    let track = raw_track(&player, 0);
    let event = |kind| sys::moq_media_player_event_t {
        kind,
        track,
        ..Default::default()
    };
    let detailed = |mut ev: sys::moq_media_player_event_t, size: usize| {
        ev.detail_size = size as u32;
        ev
    };
    let mut gap = event(sys::MOQ_MEDIA_PLAYER_EVENT_GAP);
    gap.u.gap.group_id = 4;
    gap.u.gap.object_id = 2;
    gap.u.gap.cause = sys::MOQ_PLAYBACK_GAP_CAUSE_STREAM_RESET;
    // SAFETY: only the sizes of the union's members are taken.
    let sizes = unsafe {
        (
            std::mem::size_of_val(&gap.u.gap),
            std::mem::size_of_val(&gap.u.skip),
            std::mem::size_of_val(&gap.u.shed),
            std::mem::size_of_val(&gap.u.drop),
            std::mem::size_of_val(&gap.u.loss),
            std::mem::size_of_val(&gap.u.knowledge_forgotten),
            std::mem::size_of_val(&gap.u.sync),
            std::mem::size_of_val(&gap.u.started),
        )
    };
    push_event(detailed(gap, sizes.0));
    let mut crossed = event(sys::MOQ_MEDIA_PLAYER_EVENT_GAP);
    crossed.u.gap.crossed = true;
    crossed.u.gap.hole_start_us = 500;
    crossed.u.gap.hole_duration_us = 20;
    push_event(detailed(crossed, sizes.0));
    let mut skip = event(sys::MOQ_MEDIA_PLAYER_EVENT_SKIP);
    skip.u.skip.from_group_id = 3;
    skip.u.skip.to_group_id = 6;
    push_event(detailed(skip, sizes.1));
    let mut shed = event(sys::MOQ_MEDIA_PLAYER_EVENT_SHED);
    shed.u.shed.dropped_groups = 2;
    shed.u.shed.remaining_groups = 1;
    push_event(detailed(shed, sizes.2));
    let mut dropped = event(sys::MOQ_MEDIA_PLAYER_EVENT_DROP);
    dropped.u.drop.reason = sys::MOQ_MEDIA_PLAYER_DROP_OVERSIZE;
    dropped.u.drop.group_id = 8;
    dropped.u.drop.object_id = 1;
    push_event(detailed(dropped, sizes.3));
    let mut loss = event(sys::MOQ_MEDIA_PLAYER_EVENT_LOSS);
    loss.u.loss.kind = sys::MOQ_MEDIA_STREAM_RESET;
    loss.u.loss.group_id = 9;
    loss.u.loss.last_group_id = 9;
    loss.u.loss.error_code = 0x10;
    push_event(detailed(loss, sizes.4));
    let mut forgotten = event(sys::MOQ_MEDIA_PLAYER_EVENT_KNOWLEDGE_FORGOTTEN);
    forgotten.u.knowledge_forgotten.first_group_id = 1;
    forgotten.u.knowledge_forgotten.last_group_id = 2;
    push_event(detailed(forgotten, sizes.5));
    let mut sync = event(sys::MOQ_MEDIA_PLAYER_EVENT_SYNC_UNAVAILABLE);
    sync.track = std::ptr::null_mut();
    sync.u.sync.has_render_group = true;
    sync.u.sync.render_group = -1;
    sync.u.sync.reason = sys::MOQ_MEDIA_PLAYER_SYNC_MIXED_DOMAINS;
    push_event(detailed(sync, sizes.6));
    let mut independent = event(sys::MOQ_MEDIA_PLAYER_EVENT_TIMING_INDEPENDENT);
    independent.u.sync.reason = sys::MOQ_MEDIA_PLAYER_SYNC_NO_SHARED_EPOCH;
    push_event(detailed(independent, sizes.6));
    let mut started = event(sys::MOQ_MEDIA_PLAYER_EVENT_STARTED);
    started.u.started.timeline_id = 3;
    push_event(detailed(started, sizes.7));
    for kind in [
        sys::MOQ_MEDIA_PLAYER_EVENT_TRACK_REMOVED,
        sys::MOQ_MEDIA_PLAYER_EVENT_READY,
        sys::MOQ_MEDIA_PLAYER_EVENT_STALL,
        sys::MOQ_MEDIA_PLAYER_EVENT_ENDED,
        sys::MOQ_MEDIA_PLAYER_EVENT_CLOSED,
        99,
    ] {
        push_event(event(kind));
    }

    let mut kinds = Vec::new();
    while let Some(event) = player.poll_event().expect("poll_event on a live player") {
        kinds.push((event.kind, event.track.map(|t| t.as_ptr())));
    }
    let at = |group_id, object_id| Location {
        group_id,
        object_id,
    };
    let expected = [
        PlayerEventKind::Gap(Gap::Waiting {
            missing: at(4, 2),
            cause: GapCause::StreamReset,
        }),
        PlayerEventKind::Gap(Gap::Crossed {
            hole_start_us: 500,
            hole_duration_us: 20,
        }),
        PlayerEventKind::Skip {
            from_group_id: 3,
            to_group_id: 6,
        },
        PlayerEventKind::Shed {
            dropped_groups: 2,
            remaining_groups: 1,
        },
        PlayerEventKind::Drop {
            reason: DropReason::Oversize,
            location: at(8, 1),
        },
        PlayerEventKind::Loss {
            kind: StreamTerminalKind::Reset,
            group_id: 9,
            last_group_id: 9,
            error_code: 0x10,
        },
        PlayerEventKind::KnowledgeForgotten {
            first_group_id: 1,
            last_group_id: 2,
        },
        PlayerEventKind::SyncUnavailable {
            render_group: Some(-1),
            reason: SyncReason::MixedDomains,
        },
        PlayerEventKind::TimingIndependent {
            render_group: None,
            reason: SyncReason::NoSharedEpoch,
        },
        PlayerEventKind::Started { timeline_id: 3 },
        PlayerEventKind::TrackRemoved,
        PlayerEventKind::Ready,
        PlayerEventKind::Stall,
        PlayerEventKind::Ended,
        PlayerEventKind::Closed,
        PlayerEventKind::Other(99),
    ];
    assert_eq!(
        kinds.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
        expected
    );
    assert_eq!(kinds[7].1, None, "a player-wide event names no track");
    assert_eq!(kinds[0].1, Some(track));

    // A known kind whose detail is short of its view is refused; a record
    // outside the frozen layout too.
    let mut short = event(sys::MOQ_MEDIA_PLAYER_EVENT_SKIP);
    short.detail_size = (sizes.1 - 1) as u32;
    push_event(short);
    assert_eq!(player.poll_event().err(), Some(Error::ABI_MISMATCH));
    s.set(
        "poll_event_stamp",
        (std::mem::size_of::<sys::moq_media_player_event_t>() - 8) as i64,
    );
    push_event(event(sys::MOQ_MEDIA_PLAYER_EVENT_READY));
    assert_eq!(player.poll_event().err(), Some(Error::ABI_MISMATCH));
    s.set("poll_event_stamp", 0);
    s.set("poll_event_result", i64::from(sys::MOQ_ERR_CLOSED));
    assert_eq!(player.poll_event().err(), Some(Error::CLOSED));
}

#[test]
fn feedback_maps_each_kind() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = player(&s, &endpoint);
    let track = track(&player, 1);
    s.take_log();
    let size = std::mem::size_of::<sys::moq_media_player_feedback_t>();
    let cases = [
        (
            FeedbackKind::DecoderQueue {
                depth: 9,
                max_recommended: 4,
            },
            sys::MOQ_MEDIA_PLAYER_FEEDBACK_DECODER_QUEUE,
            "9/4".to_owned(),
        ),
        (
            FeedbackKind::DecodeError {
                message: b"bad slice",
            },
            sys::MOQ_MEDIA_PLAYER_FEEDBACK_DECODE_ERROR,
            "\"bad slice\"".to_owned(),
        ),
        (
            FeedbackKind::FlushComplete,
            sys::MOQ_MEDIA_PLAYER_FEEDBACK_FLUSH_COMPLETE,
            "-".to_owned(),
        ),
    ];
    for (kind, code, detail) in cases {
        let feedback = Feedback {
            track,
            configuration_generation: 7,
            kind,
        };
        assert_eq!(player.handle_feedback(&feedback, 1000), Ok(()));
        assert_eq!(
            s.take_log(),
            [format!(
                "feedback track=1 kind={code} generation=7 detail={detail} now=1000 \
                 struct_size={size}"
            )]
        );
    }
    s.set("feedback_result", i64::from(sys::MOQ_ERR_INVAL));
    let feedback = Feedback {
        track,
        configuration_generation: 0,
        kind: FeedbackKind::FlushComplete,
    };
    assert_eq!(player.handle_feedback(&feedback, 0), Err(Error::INVAL));
}

#[test]
fn stats_and_observers() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = player(&s, &endpoint);
    let stats = player.stats().expect("stats on a live player");
    assert_eq!(stats.tracks_selected, 2);
    assert_eq!((stats.outputs_queued, stats.events_queued), (0, 0));
    assert_eq!((stats.held_objects, stats.held_bytes), (1, 4096));
    assert_eq!((stats.objects_taken, stats.objects_pushed), (20, 18));
    assert_eq!(stats.units_output, 17);
    assert_eq!(stats.feedback_stale, 4);
    assert_eq!(stats.outputs_bytes, 2048);
    assert_eq!(stats.selections_failed, 1);
    s.set(
        "player_stats_stamp",
        (sys::MOQ_MEDIA_PLAYER_STATS_V0_SIZE - 1) as i64,
    );
    assert_eq!(player.stats(), Err(Error::ABI_MISMATCH));
    s.set("player_stats_stamp", 0);
    assert!(!player.is_closed());
    s.set("player_is_fatal", 1);
    s.set("player_fatal_code", 3);
    assert!(player.is_fatal());
    assert_eq!(player.fatal_code(), 3);
    // The receiver's observing calls.
    let view = player.receiver();
    assert_eq!(view.endpoint().as_ptr(), endpoint.as_ptr());
    assert!(view.info().is_ok());
    assert!(view.stats().is_ok());
    assert!(!view.is_closed());
    assert!(format!("{player:?}").starts_with("Player"));
}

#[test]
fn teardown_destroys_the_player_before_its_receiver() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let player = player(&s, &endpoint);
    let receiver = player.detach();
    assert_eq!(s.take_log(), ["player_destroy"], "the receiver stays");
    // The receiver's queues are the application's again.
    assert!(receiver
        .poll_object()
        .expect("poll_object on a live receiver")
        .is_none());
    let player =
        Player::attach(receiver, &PlayerConfig::new()).expect("the provider accepts the player");
    s.take_log();
    drop(endpoint);
    drop(player);
    assert_eq!(
        s.take_log(),
        ["player_destroy", "receiver_destroy", "stop", "destroy"]
    );
}
