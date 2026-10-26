//! Relay rows: a RAW/LOC video track a sender publishes plays through a
//! player. The track is announced as available and selected; a TIMELINE
//! and a CONFIGURE (the catalog's codec, dimensions and timescale) precede
//! its first unit; every unit comes out in decode order with its exact
//! bytes, Location and times; STARTED is reported once. Decoder-queue
//! feedback above its recommendation pauses release (the wait is idle at
//! once, and a stale report is ignored and counted), and feedback below it
//! resumes release. After the sender ends the track, END follows the last
//! unit with the cause and disposition of an END_OF_TRACK with every unit
//! present, and ENDED is reported. An opus track plays the same way, its
//! CONFIGURE carrying the catalog's sample rate and channel count and its
//! units no independence marking.

mod live;

use std::time::{Duration, Instant};

use moq5::player::{
    EndCause, EndDisposition, Feedback, FeedbackKind, OutputDetail, Player, PlayerConfig,
    PlayerEvent, PlayerEventKind, PlayerWait, Selection,
};
use moq5::{
    Buffer, Location, MediaType, Namespace, Packaging, ReceiverConfig, SendObject, SendTrack,
    SendTrackConfig, Sender, SenderConfig, TimeDomain, Track, Version,
};

/// What a polled output said, kept past its release.
#[derive(Debug, Clone, PartialEq)]
enum Seen {
    Timeline {
        domain: TimeDomain,
        timeline_id: u32,
        anchor_media_us: u64,
        rate: f64,
    },
    Configure {
        codec: Vec<u8>,
        codec_config: Option<Vec<u8>>,
        media_type: Option<MediaType>,
        timescale: u32,
        dimensions: (u32, u32),
        samplerate: u32,
        channel_count: u32,
    },
    Unit {
        location: Location,
        data: Vec<u8>,
        decode_time_us: u64,
        presentation_time_us: u64,
        independent: Option<bool>,
        generation: u64,
    },
    End {
        cause: EndCause,
        disposition: EndDisposition,
    },
    Other,
}

/// The player's `now_us`: a monotonic clock in microseconds.
struct Clock(Instant);

impl Clock {
    fn now_us(&self) -> u64 {
        self.0.elapsed().as_micros() as u64
    }
}

/// One tick, then every output and event it made pollable.
fn step(player: &Player, clock: &Clock, outputs: &mut Vec<Seen>, events: &mut Vec<PlayerEvent>) {
    player.tick(clock.now_us()).expect("tick on a live player");
    while let Some(output) = player.poll_output().expect("poll_output on a live player") {
        let generation = output.configuration_generation();
        outputs.push(match output.detail() {
            OutputDetail::Timeline(timeline) => Seen::Timeline {
                domain: timeline.domain,
                timeline_id: timeline.timeline_id,
                anchor_media_us: timeline.anchor_media_us,
                rate: timeline.rate,
            },
            OutputDetail::Configure(config) => Seen::Configure {
                codec: config.codec().to_vec(),
                codec_config: config.codec_config().map(<[u8]>::to_vec),
                media_type: config.media_type(),
                timescale: config.timescale(),
                dimensions: (config.width(), config.height()),
                samplerate: config.samplerate(),
                channel_count: config.channel_count(),
            },
            OutputDetail::Unit(unit) => Seen::Unit {
                location: unit.location(),
                data: unit.data().to_vec(),
                decode_time_us: unit.decode_time_us(),
                presentation_time_us: unit.presentation_time_us(),
                independent: unit.independent(),
                generation,
            },
            OutputDetail::End(end) => Seen::End {
                cause: end.cause,
                disposition: end.disposition,
            },
            _ => Seen::Other,
        });
    }
    while let Some(event) = player.poll_event().expect("poll_event on a live player") {
        events.push(event);
    }
}

/// Step until `done` holds for what was seen, waiting on the player between
/// steps.
fn run_until(
    player: &Player,
    clock: &Clock,
    outputs: &mut Vec<Seen>,
    events: &mut Vec<PlayerEvent>,
    done: impl Fn(&[Seen], &[PlayerEvent]) -> bool,
) {
    let started = Instant::now();
    loop {
        step(player, clock, outputs, events);
        if done(outputs, events) {
            return;
        }
        match player.wait(clock.now_us(), live::STEP) {
            Ok(_) => {}
            Err(error) => panic!("player wait: {error}"),
        }
        assert!(
            started.elapsed() < live::DEADLINE,
            "outputs {outputs:?}, events {events:?}"
        );
    }
}

fn units(outputs: &[Seen]) -> usize {
    outputs
        .iter()
        .filter(|seen| matches!(seen, Seen::Unit { .. }))
        .count()
}

fn count(events: &[PlayerEvent], kind: PlayerEventKind) -> usize {
    events.iter().filter(|event| event.kind == kind).count()
}

/// The first track the player reports available.
fn available_track(
    player: &Player,
    clock: &Clock,
    outputs: &mut Vec<Seen>,
    events: &mut Vec<PlayerEvent>,
) -> Track {
    run_until(player, clock, outputs, events, |_, events| {
        events
            .iter()
            .any(|event| event.kind == PlayerEventKind::TrackAvailable)
    });
    events
        .iter()
        .find(|event| event.kind == PlayerEventKind::TrackAvailable)
        .and_then(|event| event.track)
        .expect("the available event names its track")
}

fn end_track(sender: &Sender, track: SendTrack) {
    let started = Instant::now();
    loop {
        match sender.end_track(track) {
            Ok(()) => return,
            Err(moq5::Error::WOULD_BLOCK) => {
                let _ = sender.wait(live::STEP);
            }
            Err(error) => panic!("end_track: {error}"),
        }
        assert!(started.elapsed() < live::DEADLINE);
    }
}

/// Until the track's END and the player's ENDED.
fn run_until_ended(
    player: &Player,
    clock: &Clock,
    outputs: &mut Vec<Seen>,
    events: &mut Vec<PlayerEvent>,
) {
    run_until(player, clock, outputs, events, |outputs, events| {
        outputs.iter().any(|seen| matches!(seen, Seen::End { .. }))
            && events
                .iter()
                .any(|event| event.kind == PlayerEventKind::Ended)
    });
}

fn write(sender: &Sender, track: SendTrack, data: &[u8], sync: bool, ends: bool, pts: u64) {
    let mut object = SendObject::new(Buffer::copy_from(data).expect("a payload copy"));
    object.is_sync = sync;
    object.starts_group = sync;
    object.ends_group = ends;
    object.presentation_time_us = pts;
    object.decode_time_us = pts;
    sender
        .write(track, object)
        .unwrap_or_else(|failure| panic!("write at {pts}: {}", failure.error));
}

#[test]
#[ignore = "needs MOQ5_RELAY and an SDK with the player; run with --ignored"]
fn a_raw_track_plays_in_decode_order() {
    let mut relay = live::Relay::start("player", 14530);
    let namespace = Namespace::parse("live/rust-player").expect("a two-part namespace");

    let sender_endpoint = live::established(&relay.config());
    let sender = Sender::attach(&sender_endpoint, &SenderConfig::live(namespace.clone()))
        .expect("the sender attaches to an established endpoint");
    let video = sender
        .add_track(
            &SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e")
                .bitrate(1_000_000)
                .dimensions(640, 360),
        )
        .expect("a raw video track is added");
    live::ready(&sender);

    let receiver_endpoint = live::established(&relay.config());
    let player = Player::create(
        &receiver_endpoint,
        &ReceiverConfig::live(namespace),
        &PlayerConfig::new().selection(Selection::Manual),
    )
    .expect("a player with a receiver of its own");
    let clock = Clock(Instant::now());
    let mut outputs = Vec::new();
    let mut events = Vec::new();
    let track = available_track(&player, &clock, &mut outputs, &mut events);
    let description = player
        .receiver()
        .track_description(track)
        .expect("the receiver describes the track");
    assert_eq!(description.name(), b"video");
    player
        .select_track(track, None)
        .expect("a RAW/LOC video track is playable");
    live::subscribed(&sender, video);

    let frame = |i: u64| 1_000_000 + i * 33_333;
    let payloads: Vec<Vec<u8>> = (0..5u8)
        .map(|i| vec![0x40 + i; 100 + usize::from(i)])
        .collect();

    // The first unit, behind its timeline and configuration.
    write(&sender, video, &payloads[0], true, false, frame(0));
    run_until(&player, &clock, &mut outputs, &mut events, |outputs, _| {
        units(outputs) == 1
    });
    let generation = match outputs
        .iter()
        .find(|seen| matches!(seen, Seen::Unit { .. }))
    {
        Some(Seen::Unit { generation, .. }) => *generation,
        _ => unreachable!("one unit was seen"),
    };

    // Decoder-queue feedback above the recommendation pauses release.
    let queue = |depth, configuration_generation| Feedback {
        track,
        configuration_generation,
        kind: FeedbackKind::DecoderQueue {
            depth,
            max_recommended: 4,
        },
    };
    player
        .handle_feedback(&queue(9, generation), clock.now_us())
        .expect("feedback for the selected track");
    write(&sender, video, &payloads[1], false, false, frame(1));
    let started = Instant::now();
    while player.stats().expect("stats").objects_taken < 2 {
        step(&player, &clock, &mut outputs, &mut events);
        if player.stats().expect("stats").objects_taken < 2 {
            let _ = player.wait(clock.now_us(), live::STEP);
        }
        assert!(
            started.elapsed() < live::DEADLINE,
            "the second object never arrived"
        );
    }
    step(&player, &clock, &mut outputs, &mut events);
    assert_eq!(units(&outputs), 1, "release is paused");
    let before = Instant::now();
    assert_eq!(
        player.wait(clock.now_us(), Duration::from_secs(5)),
        Ok(PlayerWait::Idle),
        "the only work left waits for feedback"
    );
    assert!(
        before.elapsed() < Duration::from_secs(4),
        "idle at once, not after the timeout"
    );
    // A report for a newer generation than the outputs' is stale: ignored
    // and counted.
    player
        .handle_feedback(&queue(0, generation + 7), clock.now_us())
        .expect("stale feedback is accepted");
    step(&player, &clock, &mut outputs, &mut events);
    assert_eq!(units(&outputs), 1, "still paused");
    assert_eq!(player.stats().expect("stats").feedback_stale, 1);
    // Below the recommendation, release resumes.
    player
        .handle_feedback(&queue(0, generation), clock.now_us())
        .expect("feedback for the selected track");
    run_until(&player, &clock, &mut outputs, &mut events, |outputs, _| {
        units(outputs) == 2
    });

    // The rest of the first group, a second group, and the track's end.
    write(&sender, video, &payloads[2], false, true, frame(2));
    write(&sender, video, &payloads[3], true, false, frame(3));
    write(&sender, video, &payloads[4], false, false, frame(4));
    end_track(&sender, video);
    run_until_ended(&player, &clock, &mut outputs, &mut events);

    // Order: the timeline and configuration before the first unit, the end
    // after the last.
    let first_unit = outputs
        .iter()
        .position(|seen| matches!(seen, Seen::Unit { .. }))
        .expect("units were output");
    let timeline_at = outputs
        .iter()
        .position(|seen| matches!(seen, Seen::Timeline { .. }))
        .expect("a timeline was output");
    let configure_at = outputs
        .iter()
        .position(|seen| matches!(seen, Seen::Configure { .. }))
        .expect("a configuration was output");
    assert!(
        timeline_at < first_unit && configure_at < first_unit,
        "{outputs:?}"
    );
    assert!(
        matches!(outputs.last(), Some(Seen::End { .. })),
        "the end is last: {outputs:?}"
    );
    let timelines: Vec<&Seen> = outputs
        .iter()
        .filter(|seen| matches!(seen, Seen::Timeline { .. }))
        .collect();
    assert_eq!(timelines.len(), 1);
    let Seen::Timeline {
        domain,
        timeline_id,
        anchor_media_us,
        rate,
    } = timelines[0].clone()
    else {
        unreachable!()
    };
    // The sender's catalog carries a timescale for a RAW track, which on
    // draft 22 puts the track in application media time; drafts 16/18 are
    // wall-clock.
    let expected_domain = if live::draft() == Version::DRAFT_22 {
        TimeDomain::ApplicationMedia
    } else {
        TimeDomain::UnixMicros
    };
    assert_eq!(domain, expected_domain);
    assert_eq!(anchor_media_us, frame(0), "anchored at the first unit");
    assert_eq!(rate, 1.0);
    let configures: Vec<&Seen> = outputs
        .iter()
        .filter(|seen| matches!(seen, Seen::Configure { .. }))
        .collect();
    // The catalog's codec and dimensions, and the sender's default
    // timescale (microseconds).
    assert_eq!(
        configures,
        [&Seen::Configure {
            codec: b"avc1.42e01e".to_vec(),
            codec_config: None,
            media_type: Some(MediaType::Video),
            timescale: 1_000_000,
            dimensions: (640, 360),
            samplerate: 0,
            channel_count: 0,
        }]
    );
    let seen_units: Vec<&Seen> = outputs
        .iter()
        .filter(|seen| matches!(seen, Seen::Unit { .. }))
        .collect();
    assert_eq!(seen_units.len(), 5);
    let Seen::Unit {
        location: first, ..
    } = seen_units[0]
    else {
        unreachable!()
    };
    let group = first.group_id;
    let locations = [
        (group, 0),
        (group, 1),
        (group, 2),
        (group + 1, 0),
        (group + 1, 1),
    ];
    for (i, seen) in seen_units.iter().enumerate() {
        let Seen::Unit {
            location,
            data,
            decode_time_us,
            presentation_time_us,
            independent,
            ..
        } = seen
        else {
            unreachable!()
        };
        assert_eq!(
            (location.group_id, location.object_id),
            locations[i],
            "unit {i} in decode order"
        );
        assert_eq!(data, &payloads[i], "unit {i} bytes");
        assert_eq!(
            *presentation_time_us,
            frame(i as u64),
            "unit {i} presentation time"
        );
        assert_eq!(
            decode_time_us, presentation_time_us,
            "RAW: the LOC media time"
        );
        let sync = i == 0 || i == 3;
        assert_eq!(
            *independent,
            sync.then_some(true),
            "unit {i}: a keyframe is independent, the others unmarked"
        );
    }
    assert_eq!(
        outputs.last(),
        Some(&Seen::End {
            cause: EndCause::PeerEndOfTrack,
            disposition: EndDisposition::Complete,
        })
    );
    let started_events: Vec<&PlayerEvent> = events
        .iter()
        .filter(|event| matches!(event.kind, PlayerEventKind::Started { .. }))
        .collect();
    assert_eq!(started_events.len(), 1);
    assert_eq!(
        started_events[0].kind,
        PlayerEventKind::Started { timeline_id }
    );
    assert_eq!(count(&events, PlayerEventKind::Ended), 1);
    assert!(
        !events.iter().any(|event| matches!(
            event.kind,
            PlayerEventKind::Gap(_)
                | PlayerEventKind::Drop { .. }
                | PlayerEventKind::Skip { .. }
                | PlayerEventKind::Loss { .. }
        )),
        "no unit was missing or dropped: {events:?}"
    );
    let stats = player.stats().expect("stats");
    assert_eq!(
        (
            stats.units_output,
            stats.configures_output,
            stats.ends_output
        ),
        (5, 1, 1)
    );
    assert_eq!(stats.feedback_stale, 1);

    drop(player);
    drop(receiver_endpoint);
    drop(sender);
    drop(sender_endpoint);
    relay.stop();
}

#[test]
#[ignore = "needs MOQ5_RELAY and an SDK with the player; run with --ignored"]
fn an_opus_track_plays_with_its_audio_configuration() {
    let mut relay = live::Relay::start("player-opus", 14560);
    let namespace = Namespace::parse("live/rust-opus").expect("a two-part namespace");

    let sender_endpoint = live::established(&relay.config());
    let sender = Sender::attach(&sender_endpoint, &SenderConfig::live(namespace.clone()))
        .expect("the sender attaches to an established endpoint");
    let audio = sender
        .add_track(
            &SendTrackConfig::new("audio", MediaType::Audio, Packaging::Raw, "opus")
                .bitrate(64_000)
                .samplerate(48_000)
                .channel_config("2"),
        )
        .expect("a raw opus track is added");
    live::ready(&sender);

    let receiver_endpoint = live::established(&relay.config());
    let player = Player::create(
        &receiver_endpoint,
        &ReceiverConfig::live(namespace),
        &PlayerConfig::new().selection(Selection::Manual),
    )
    .expect("a player with a receiver of its own");
    let clock = Clock(Instant::now());
    let mut outputs = Vec::new();
    let mut events = Vec::new();
    let track = available_track(&player, &clock, &mut outputs, &mut events);
    player
        .select_track(track, None)
        .expect("a RAW/LOC opus track is playable");
    live::subscribed(&sender, audio);

    // Four 20 ms frames in one group, then the track's end.
    let frame = |i: u64| 2_000_000 + i * 20_000;
    let payloads: Vec<Vec<u8>> = (0..4u8)
        .map(|i| vec![0x80 + i; 60 + usize::from(i)])
        .collect();
    for (i, payload) in payloads.iter().enumerate() {
        let last = i + 1 == payloads.len();
        write(&sender, audio, payload, i == 0, last, frame(i as u64));
    }
    end_track(&sender, audio);
    run_until_ended(&player, &clock, &mut outputs, &mut events);

    // One timeline and one configuration, both before the first unit; the
    // end after the last.
    let first_unit = outputs
        .iter()
        .position(|seen| matches!(seen, Seen::Unit { .. }))
        .expect("units were output");
    let timelines: Vec<(usize, &Seen)> = outputs
        .iter()
        .enumerate()
        .filter(|(_, seen)| matches!(seen, Seen::Timeline { .. }))
        .collect();
    let configures: Vec<(usize, &Seen)> = outputs
        .iter()
        .enumerate()
        .filter(|(_, seen)| matches!(seen, Seen::Configure { .. }))
        .collect();
    assert_eq!((timelines.len(), configures.len()), (1, 1), "{outputs:?}");
    assert!(
        timelines[0].0 < first_unit && configures[0].0 < first_unit,
        "{outputs:?}"
    );
    assert_eq!(
        outputs.last(),
        Some(&Seen::End {
            cause: EndCause::PeerEndOfTrack,
            disposition: EndDisposition::Complete,
        })
    );
    let Seen::Timeline {
        domain,
        timeline_id,
        anchor_media_us,
        rate,
    } = timelines[0].1.clone()
    else {
        unreachable!()
    };
    let expected_domain = if live::draft() == Version::DRAFT_22 {
        TimeDomain::ApplicationMedia
    } else {
        TimeDomain::UnixMicros
    };
    assert_eq!(domain, expected_domain);
    assert_eq!(anchor_media_us, frame(0), "anchored at the first unit");
    assert_eq!(rate, 1.0);
    // The catalog's codec, sample rate and channel count, and the sender's
    // default timescale (microseconds).
    assert_eq!(
        configures[0].1,
        &Seen::Configure {
            codec: b"opus".to_vec(),
            codec_config: None,
            media_type: Some(MediaType::Audio),
            timescale: 1_000_000,
            dimensions: (0, 0),
            samplerate: 48_000,
            channel_count: 2,
        }
    );
    let seen_units: Vec<&Seen> = outputs
        .iter()
        .filter(|seen| matches!(seen, Seen::Unit { .. }))
        .collect();
    assert_eq!(seen_units.len(), payloads.len());
    let Seen::Unit {
        location: first, ..
    } = seen_units[0]
    else {
        unreachable!()
    };
    for (i, seen) in seen_units.iter().enumerate() {
        let Seen::Unit {
            location,
            data,
            decode_time_us,
            presentation_time_us,
            independent,
            ..
        } = seen
        else {
            unreachable!()
        };
        assert_eq!(
            (location.group_id, location.object_id),
            (first.group_id, i as u64),
            "unit {i} in decode order"
        );
        assert_eq!(data, &payloads[i], "unit {i} bytes");
        assert_eq!(*presentation_time_us, frame(i as u64));
        assert_eq!(decode_time_us, presentation_time_us);
        assert_eq!(*independent, None, "unit {i}: audio carries no marking");
    }
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == PlayerEventKind::Started { timeline_id })
            .count(),
        1
    );
    assert_eq!(count(&events, PlayerEventKind::Ended), 1);
    assert!(
        !events.iter().any(|event| matches!(
            event.kind,
            PlayerEventKind::Gap(_)
                | PlayerEventKind::Drop { .. }
                | PlayerEventKind::Skip { .. }
                | PlayerEventKind::Loss { .. }
        )),
        "no unit was missing or dropped: {events:?}"
    );
    let stats = player.stats().expect("stats");
    assert_eq!(
        (
            stats.units_output,
            stats.configures_output,
            stats.ends_output
        ),
        (payloads.len() as u64, 1, 1)
    );

    drop(player);
    drop(receiver_endpoint);
    drop(sender);
    drop(sender_endpoint);
    relay.stop();
}
