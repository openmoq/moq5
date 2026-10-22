//! Relay row: the two distinct facts about a finished track. The
//! END_OF_TRACK is observed on the stream-terminal lane, at its Location;
//! the track's end is observed as a track event, here with no done status,
//! since the END_OF_TRACK ended it before any completing message. (The
//! done-status row observes a completion that carries one.)

mod live;

use std::time::Instant;

use moq5::{
    Buffer, Error, MediaType, Namespace, Packaging, Receiver, ReceiverConfig, SendObject,
    SendTrackConfig, Sender, SenderConfig, StreamTerminalKind, TrackEventKind, TrackState,
};

#[test]
#[ignore = "needs MOQ5_RELAY; run with --ignored"]
fn end_of_track_on_the_lane_and_the_ended_event() {
    let mut relay = live::Relay::start("end-of-track", 14470);
    let namespace = Namespace::parse("live/rust-end").expect("a two-part namespace parses");

    let sender_endpoint = live::established(&relay.config());
    let sender = Sender::attach(&sender_endpoint, &SenderConfig::live(namespace.clone()))
        .expect("the sender attaches to an established endpoint");
    let video = sender
        .add_track(
            &SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e")
                .bitrate(500_000),
        )
        .expect("a raw video track is added");
    live::ready(&sender);

    let receiver_endpoint = live::established(&relay.config());
    let receiver = Receiver::attach(
        &receiver_endpoint,
        &ReceiverConfig::live(namespace)
            .auto_subscribe(true)
            .stream_terminals(16),
    )
    .expect("the receiver attaches to an established endpoint");
    let mut seen = Vec::new();
    let track = live::announced_track(&receiver, &mut seen);

    for i in 0..3u8 {
        let mut object =
            SendObject::new(Buffer::copy_from(&[i; 32]).expect("a 32-byte copy allocates"));
        object.is_sync = i == 0;
        object.starts_group = i == 0;
        object.presentation_time_us = u64::from(i) * 1_000;
        sender
            .write(video, object)
            .unwrap_or_else(|failure| panic!("write {i}: {}", failure.error));
    }
    let started = Instant::now();
    loop {
        match sender.end_track(video) {
            Ok(()) => break,
            Err(Error::WOULD_BLOCK) => {
                sender
                    .wait(live::STEP)
                    .expect("the sender's wait is not interrupted");
            }
            Err(error) => panic!("end_track: {error}"),
        }
        assert!(started.elapsed() < live::DEADLINE);
    }

    // The objects, then the lane: a record is pollable once every object
    // taken in before it has left the receiver. The subgroup's stream ends
    // (a FIN record) before the track does.
    let started = Instant::now();
    let mut last = None;
    let mut records = Vec::new();
    loop {
        while let Some(object) = receiver
            .poll_object()
            .expect("poll_object on a live receiver")
        {
            let id = object
                .identity()
                .expect("a relayed object is stamped with its identity");
            assert_eq!(object.payload(), vec![id.location.object_id as u8; 32]);
            last = Some((id.location.group_id, id.location.object_id));
        }
        if let Some(record) = receiver
            .poll_stream_terminal()
            .expect("poll_stream_terminal on a receiver with the lane enabled")
        {
            records.push(record);
            if record.kind == StreamTerminalKind::EndOfTrack {
                break;
            }
        } else {
            let _ = receiver.wait(live::STEP);
        }
        assert!(
            started.elapsed() < live::DEADLINE,
            "no END_OF_TRACK; last object {last:?}, records {records:?}"
        );
    }
    let terminal = records.pop().expect("the END_OF_TRACK record was pushed");
    let (group, object) = last.expect("objects arrived before the end");
    assert_eq!(object, 2, "every object arrived before the terminal");
    for record in &records {
        assert_eq!(record.track, track);
        assert!(
            matches!(
                record.kind,
                StreamTerminalKind::Fin | StreamTerminalKind::FinEndOfGroup
            ),
            "{record:?}"
        );
        assert_eq!(record.group_id, group);
        assert_eq!(
            record.last_object_id,
            Some(object),
            "the stream carried every object"
        );
    }
    assert_eq!(terminal.kind, StreamTerminalKind::EndOfTrack);
    assert_eq!(terminal.track, track);
    // The publisher places the terminal status object at {last published
    // group + 1, 0}, alone on a fresh subgroup.
    assert_eq!(
        terminal.group_id,
        group + 1,
        "the END_OF_TRACK opens the next group"
    );
    assert_eq!(terminal.object_id, 0, "at its first Location");

    // The track's end as a track event. The END_OF_TRACK ended it, so no
    // completing message's status is attached: done_status is None, which
    // is the C contract for a track ended at its END_OF_TRACK, and the
    // sender's own PUBLISH_DONE (TRACK_ENDED) that follows raises nothing
    // more.
    let ended = live::until_event(&receiver, &mut seen, |k| k == TrackEventKind::Ended);
    assert_eq!(ended.track, Some(track));
    assert_eq!(
        ended.done_status, None,
        "a track ended at its END_OF_TRACK carries no done status"
    );
    assert_eq!(receiver.track_state(track), Ok(TrackState::Ended));
    let stats = receiver.stats().expect("stats on a live receiver");
    assert_eq!(stats.objects_received, 3);
    assert_eq!(stats.objects_dropped, 0);
    // One Ended for the track in the whole queue as it stands once the track
    // reads Ended (an event queued behind the first is read too); whether a
    // later tick could add another is not a claim this live row makes.
    live::drain_queued(&receiver, &mut seen);
    assert_eq!(
        live::ended_count(&seen, track),
        1,
        "one Ended event for the track in the queue: {seen:?}"
    );

    drop(receiver);
    drop(sender);
    drop(receiver_endpoint);
    drop(sender_endpoint);
    relay.stop();
}
