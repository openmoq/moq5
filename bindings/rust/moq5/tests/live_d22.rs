//! Relay rows: the draft-22 surface, with each row's draft-16/18 behavior
//! asserted where draft 22 is not negotiated.
//!
//! - Explicit LOC-04 metadata written with RAW objects arrives as each
//!   object's effective metadata, and a track's Track Properties (its own,
//!   and DYNAMIC_GROUPS) arrive with its objects; drafts 16/18 carry
//!   neither. A set that does not parse is refused when the track is added,
//!   on every draft, with the sender still usable and the name free for the
//!   corrected set.
//! - A late subscriber's fill delivers the objects the relay retains, each
//!   from the fill with its initiating request ID, and ends with one
//!   COMPLETE terminal after which the fill cannot be cancelled; live
//!   objects follow from the subscription. Drafts 16/18 refuse the fill and
//!   its cancellation.

mod live;

use std::time::Instant;

use moq5::{
    Buffer, CompanionProfile, DeliveryOrigin, Error, FillError, FillOptions, FillReason,
    LocMetadata, Location, LocationFilter, Marking, MediaObject, MediaType, Namespace,
    OverflowPolicy, Packaging, PollWithin, Receiver, ReceiverConfig, SendObject, SendTrack,
    SendTrackConfig, Sender, SenderConfig, SubscribeOptions, TimeDomain, TimeMode, Track,
    TrackEventKind, Version,
};

fn d22() -> bool {
    live::draft() == Version::DRAFT_22
}

fn write(sender: &Sender, track: SendTrack, object: SendObject) {
    sender
        .write(track, object)
        .unwrap_or_else(|failure| panic!("write: {}", failure.error));
}

/// What a received object carried, kept past its release.
#[derive(Debug, Clone, PartialEq)]
struct Got {
    track: Option<Track>,
    location: Location,
    payload: Vec<u8>,
    origin: Option<DeliveryOrigin>,
    request: Option<u64>,
    timestamp: Option<u64>,
    timescale: Option<u64>,
    time_domain: Option<TimeDomain>,
    time_us: Option<u64>,
    marking: Option<Vec<u8>>,
    video_config: Option<Vec<u8>>,
    has_metadata: bool,
    track_properties: Option<Vec<u8>>,
}

/// What `object` carried.
fn kept(object: &MediaObject) -> Got {
    let id = object
        .identity()
        .expect("a relayed object is stamped with its identity");
    let metadata = object.metadata();
    Got {
        track: object.track(),
        location: id.location,
        payload: object.payload().to_vec(),
        origin: object.delivery_origin(),
        request: object.initiating_request_id(),
        timestamp: metadata.and_then(|m| m.timestamp()),
        timescale: metadata.and_then(|m| m.timescale()),
        time_domain: metadata.map(|m| m.time_domain()),
        time_us: metadata.and_then(|m| m.time_us()),
        marking: metadata.and_then(|m| m.marking()).map(<[u8]>::to_vec),
        video_config: metadata.and_then(|m| m.video_config()).map(<[u8]>::to_vec),
        has_metadata: metadata.is_some(),
        track_properties: object.track_properties().map(<[u8]>::to_vec),
    }
}

/// Poll objects (and drain track events into `events`) until `done` holds.
fn receive_until(
    receiver: &Receiver,
    got: &mut Vec<Got>,
    events: &mut Vec<live::Seen>,
    fills: &mut Vec<moq5::FillTerminal>,
    done: impl Fn(&[Got], &[moq5::FillTerminal]) -> bool,
) {
    let started = Instant::now();
    loop {
        while let Some(event) = receiver
            .poll_track()
            .expect("poll_track on a live receiver")
        {
            if let Some(fill) = event.fill_terminal() {
                if event.kind() == TrackEventKind::FillTerminal {
                    fills.push(fill);
                }
            }
            events.push(live::Seen {
                kind: event.kind(),
                track: event.track(),
                done_status: event.done_status(),
            });
        }
        while let Some(object) = receiver
            .poll_object()
            .expect("poll_object on a live receiver")
        {
            got.push(kept(&object));
        }
        if done(got, fills) {
            return;
        }
        let _ = receiver.wait(live::STEP);
        assert!(
            started.elapsed() < live::DEADLINE,
            "objects {got:?}, fills {fills:?}, events {events:?}"
        );
    }
}

#[test]
#[ignore = "needs MOQ5_RELAY; run with --ignored"]
fn metadata_and_track_properties_arrive_with_the_objects() {
    let mut relay = live::Relay::start("d22-metadata", 14540);
    let namespace = Namespace::parse("live/rust-d22").expect("a two-part namespace");
    let sender_endpoint = live::established(&relay.config());
    let sender = Sender::attach(&sender_endpoint, &SenderConfig::live(namespace.clone()))
        .expect("the sender attaches to an established endpoint");
    let video_config = |properties: Vec<u8>| {
        SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e")
            .bitrate(1_000_000)
            .track_properties(properties)
    };
    // Track Properties are checked when the track is added: the property
    // below without its value parses on no draft, so the call refuses the
    // track, the sender stays usable and the name stays free.
    assert_eq!(
        sender.add_track(&video_config(vec![0x22])).err(),
        Some(Error::INVAL)
    );
    assert!(!sender.is_fatal(), "a refused set leaves the sender usable");
    // DEFAULT_PUBLISHER_GROUP_ORDER (0x22) Ascending (0x1), the default
    // stated explicitly: a property whose type and value are both below 64
    // encodes the same on every draft.
    let video = sender
        .add_track(&video_config(vec![0x22, 0x01]))
        .expect("the same name is added with the corrected set");
    let dynamic = sender
        .add_track(
            &SendTrackConfig::new("dynamic", MediaType::Video, Packaging::Raw, "avc1.42e01e")
                .bitrate(1_000_000)
                .dynamic_groups(true),
        )
        .expect("a raw video track with dynamic groups is added");
    live::ready(&sender);

    let receiver_endpoint = live::established(&relay.config());
    let receiver = Receiver::attach(
        &receiver_endpoint,
        &ReceiverConfig::flow_control(namespace).auto_subscribe(true),
    )
    .expect("the receiver attaches to an established endpoint");
    let mut events = Vec::new();
    live::announced_track(&receiver, &mut events);
    live::subscribed(&sender, video);
    live::subscribed(&sender, dynamic);

    // Unix-epoch microseconds (no timescale): the first object carries a
    // video configuration, a keyframe marking; the second a delta marking.
    let base = 1_760_000_000_000_000u64;
    let config = vec![0x01, 0x42, 0xC0, 0x1E];
    let mut first = SendObject::new(Buffer::copy_from(b"key").expect("a payload copy"));
    first.is_sync = true;
    first.starts_group = true;
    first.presentation_time_us = 1;
    first.metadata = Some(LocMetadata {
        timestamp: Some(base),
        marking: Some(Marking::new(&[0x20]).expect("one byte")),
        video_config: Some(config.clone()),
        ..LocMetadata::default()
    });
    write(&sender, video, first);
    let mut second = SendObject::new(Buffer::copy_from(b"delta").expect("a payload copy"));
    second.presentation_time_us = 2;
    second.metadata = Some(LocMetadata {
        timestamp: Some(base + 33_333),
        marking: Some(Marking::new(&[0x00]).expect("one byte")),
        ..LocMetadata::default()
    });
    write(&sender, video, second);
    let mut other = SendObject::new(Buffer::copy_from(b"dyn").expect("a payload copy"));
    other.is_sync = true;
    other.starts_group = true;
    other.presentation_time_us = 3;
    write(&sender, dynamic, other);

    // A bounded poll leaves a head larger than its allowance queued and
    // reports the head's charge; an allowance of exactly that charge takes
    // it.
    let started = Instant::now();
    let charge = loop {
        live::drain_queued(&receiver, &mut events);
        match receiver
            .poll_object_within(0)
            .expect("poll_object_within on a live receiver")
        {
            PollWithin::TooLarge { charge } => break charge,
            PollWithin::Empty => {
                let _ = receiver.wait(live::STEP);
            }
            taken @ PollWithin::Taken { .. } => {
                panic!("an object with a payload charges more than nothing: {taken:?}")
            }
        }
        assert!(started.elapsed() < live::DEADLINE, "no object queued");
    };
    match receiver
        .poll_object_within(charge - 1)
        .expect("poll_object_within on a live receiver")
    {
        PollWithin::TooLarge { charge: again } => {
            assert_eq!(again, charge, "the same head, still queued")
        }
        other => panic!("one byte short of the head's charge: {other:?}"),
    }
    let mut got = Vec::new();
    match receiver
        .poll_object_within(charge)
        .expect("poll_object_within on a live receiver")
    {
        PollWithin::Taken {
            object,
            charge: taken,
        } => {
            assert_eq!(taken, charge);
            assert_eq!(
                object.charge(),
                charge,
                "the record measures what was polled"
            );
            assert!(
                charge >= object.payload().len() as u64,
                "the charge covers the payload"
            );
            got.push(kept(&object));
        }
        other => panic!("an allowance of exactly the head's charge: {other:?}"),
    }
    let mut fills = Vec::new();
    receive_until(&receiver, &mut got, &mut events, &mut fills, |got, _| {
        got.len() == 3
    });
    let video_objects: Vec<&Got> = got.iter().filter(|g| g.payload != b"dyn").collect();
    let dynamic_object = got
        .iter()
        .find(|g| g.payload == b"dyn")
        .expect("the other track's object");
    assert_eq!(video_objects.len(), 2);
    assert_eq!(video_objects[0].payload, b"key");
    assert_eq!(video_objects[1].payload, b"delta");
    if d22() {
        let [key, delta] = [video_objects[0], video_objects[1]];
        assert!(key.has_metadata && delta.has_metadata);
        assert_eq!(key.timestamp, Some(base), "written as given");
        assert_eq!(delta.timestamp, Some(base + 33_333));
        assert_eq!((key.timescale, delta.timescale), (None, None));
        assert_eq!(
            key.time_domain,
            Some(TimeDomain::UnixMicros),
            "no timescale: Unix time"
        );
        assert_eq!(key.time_us, Some(base));
        assert_eq!(key.marking.as_deref(), Some(&[0x20][..]));
        assert_eq!(delta.marking.as_deref(), Some(&[0x00][..]));
        assert_eq!(key.video_config.as_deref(), Some(&config[..]));
        assert_eq!(
            delta.video_config, None,
            "neither the object nor the track has one"
        );
        assert_eq!(key.track_properties.as_deref(), Some(&[0x22, 0x01][..]));
        assert_eq!(delta.track_properties.as_deref(), Some(&[0x22, 0x01][..]));
        assert_eq!(
            dynamic_object.track_properties.as_deref(),
            Some(&[0x30, 0x01][..]),
            "DYNAMIC_GROUPS (0x30) = 1"
        );
    } else {
        for object in &got {
            assert!(
                !object.has_metadata,
                "drafts 16/18 carry no LOC-04 metadata"
            );
            assert_eq!(
                object.track_properties, None,
                "nor a track-properties block"
            );
        }
    }
    // No subscriber asked for a new group.
    assert_eq!(sender.take_new_group_request(dynamic), Ok(None));

    // The effective configuration: defaulted values as selected, the
    // companion as the negotiated draft resolved it, and every track event
    // this row polled.
    let info = receiver.info().expect("the receiver's information");
    assert_eq!(info.time_mode, TimeMode::Raw);
    assert_eq!(info.overflow_policy, OverflowPolicy::FlowControl);
    assert_eq!(
        (info.max_objects, info.max_bytes),
        (256, 32 << 20),
        "the queue defaults"
    );
    assert!(info.auto_subscribe);
    assert_eq!(info.stream_terminals, None, "the lane is off");
    assert_eq!(info.transport_version, Some(live::draft()));
    assert_eq!(
        info.companion_profile,
        if d22() {
            CompanionProfile::D22Msf01Loc04Public
        } else {
            CompanionProfile::LegacyMsf01Loc01
        }
    );
    assert_eq!(info.track_events_polled, events.len() as u64);

    drop(receiver);
    drop(sender);
    drop(receiver_endpoint);
    drop(sender_endpoint);
    relay.stop();
}

#[test]
#[ignore = "needs MOQ5_RELAY; run with --ignored"]
fn a_fill_delivers_retained_objects_and_ends_once() {
    let mut relay = live::Relay::start("d22-fill", 14550);
    let namespace = Namespace::parse("live/rust-fill").expect("a two-part namespace");
    let sender_endpoint = live::established(&relay.config());
    let sender = Sender::attach(&sender_endpoint, &SenderConfig::live(namespace.clone()))
        .expect("the sender attaches to an established endpoint");
    let video = sender
        .add_track(
            &SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e")
                .bitrate(1_000_000),
        )
        .expect("a raw video track is added");
    live::ready(&sender);

    // A first subscriber, through which the relay receives and retains two
    // groups.
    let first_endpoint = live::established(&relay.config());
    let first = Receiver::attach(
        &first_endpoint,
        &ReceiverConfig::flow_control(namespace.clone()).auto_subscribe(true),
    )
    .expect("the receiver attaches to an established endpoint");
    let mut events = Vec::new();
    live::announced_track(&first, &mut events);
    live::subscribed(&sender, video);
    let payloads: Vec<Vec<u8>> = (0..6u8).map(|i| vec![0x60 + i; 40]).collect();
    for (i, payload) in payloads.iter().enumerate() {
        let mut object = SendObject::new(Buffer::copy_from(payload).expect("a payload copy"));
        object.is_sync = i % 3 == 0;
        object.starts_group = i % 3 == 0;
        object.ends_group = i % 3 == 2;
        object.presentation_time_us = 1_000 * i as u64;
        write(&sender, video, object);
    }
    let mut got = Vec::new();
    let mut fills = Vec::new();
    receive_until(&first, &mut got, &mut events, &mut fills, |got, _| {
        got.len() == 6
    });
    let start = got[0].location;
    assert_eq!(start.object_id, 0);

    // A late subscriber: live from the next object, and a fill of what the
    // relay holds from the first group's start.
    let late_endpoint = live::established(&relay.config());
    let late = Receiver::attach(
        &late_endpoint,
        &ReceiverConfig::flow_control(namespace).auto_subscribe(false),
    )
    .expect("the receiver attaches to an established endpoint");
    let mut late_events = Vec::new();
    let track = live::announced_track(&late, &mut late_events);
    let options = SubscribeOptions {
        location: Some(LocationFilter::NextObject),
        fill: Some(FillOptions {
            location: Some(LocationFilter::AbsoluteStart(start)),
            ..FillOptions::default()
        }),
        ..SubscribeOptions::default()
    };
    if !d22() {
        assert_eq!(
            late.subscribe_track(track, Some(options)),
            Err(Error::UNSUPPORTED),
            "a fill needs the draft-22 companion"
        );
        assert_eq!(late.cancel_fill(track, 0), Err(Error::UNSUPPORTED));
        // The location alone is representable on drafts 16/18.
        late.subscribe_track(
            track,
            Some(SubscribeOptions {
                location: Some(LocationFilter::NextObject),
                ..SubscribeOptions::default()
            }),
        )
        .expect("a next-object subscription on drafts 16/18");
    } else {
        late.subscribe_track(track, Some(options))
            .expect("a subscription with a fill on draft 22");
        let mut late_got = Vec::new();
        let mut late_fills = Vec::new();
        receive_until(
            &late,
            &mut late_got,
            &mut late_events,
            &mut late_fills,
            |got, fills| got.len() >= 6 && !fills.is_empty(),
        );
        // The retained objects, all from the fill, under one request ID.
        let filled: Vec<&Got> = late_got
            .iter()
            .filter(|g| g.origin == Some(DeliveryOrigin::Fill))
            .collect();
        assert_eq!(filled.len(), 6, "every retained object: {late_got:?}");
        let request = filled[0].request.expect("a fill object names its request");
        for (i, object) in filled.iter().enumerate() {
            assert_eq!(object.payload, payloads[i], "fill object {i}");
            assert_eq!(
                object.location, got[i].location,
                "fill object {i}'s Location"
            );
            assert_eq!(object.request, Some(request));
        }
        assert_eq!(late_fills.len(), 1, "one terminal per fill");
        let terminal = late_fills[0];
        assert_eq!(terminal.initiating_request_id, request);
        assert_eq!(terminal.reason, FillReason::Complete);
        assert_eq!(terminal.error, FillError::None);
        // An ended fill cannot be cancelled, nor an unknown one.
        assert_eq!(late.cancel_fill(track, request), Err(Error::STALE_HANDLE));
        assert_eq!(
            late.cancel_fill(track, request + 1000),
            Err(Error::STALE_HANDLE)
        );
        // Live objects follow from the subscription.
        let mut object = SendObject::new(Buffer::copy_from(b"live").expect("a payload copy"));
        object.is_sync = true;
        object.starts_group = true;
        object.presentation_time_us = 9_000;
        write(&sender, video, object);
        receive_until(
            &late,
            &mut late_got,
            &mut late_events,
            &mut late_fills,
            |got, _| got.iter().any(|g| g.payload == b"live"),
        );
        let live_object = late_got
            .iter()
            .find(|g| g.payload == b"live")
            .expect("the live object arrived");
        assert_eq!(live_object.origin, Some(DeliveryOrigin::Live));
        assert_eq!(live_object.request, Some(0), "a live delivery stores zero");
    }

    drop(late);
    drop(first);
    drop(sender);
    drop(late_endpoint);
    drop(first_endpoint);
    drop(sender_endpoint);
    relay.stop();
}
