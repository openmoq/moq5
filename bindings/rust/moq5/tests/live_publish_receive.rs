//! Relay row: objects published through a sender arrive at a receiver with
//! their exact bytes, timestamps and wire identity.

mod live;

use std::time::Instant;

use moq5::{
    Buffer, MediaType, Namespace, Packaging, Receiver, ReceiverConfig, SendObject, SendTrackConfig,
    Sender, SenderConfig,
};

#[test]
#[ignore = "needs MOQ5_RELAY; run with --ignored"]
fn publish_receive_exact_bytes() {
    let mut relay = live::Relay::start("publish-receive", 14460);
    let namespace = Namespace::parse("live/rust").expect("a two-part namespace parses");

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
    let receiver = Receiver::attach(
        &receiver_endpoint,
        &ReceiverConfig::live(namespace)
            .auto_subscribe(true)
            .stream_terminals(16),
    )
    .expect("the receiver attaches to an established endpoint");
    let mut seen = Vec::new();
    let track = live::announced_track(&receiver, &mut seen);
    let description = receiver
        .track_description(track)
        .expect("the announced track has a description");
    assert_eq!(description.name(), b"video");
    assert_eq!(description.codec(), b"avc1.42e01e");
    assert_eq!(description.media_type(), Some(MediaType::Video));
    assert_eq!(description.packaging(), Some(Packaging::Raw));
    assert_eq!(description.bitrate(), Some(1_000_000));
    assert_eq!(description.width(), Some(640));
    assert!(description.is_live());

    let payloads: Vec<Vec<u8>> = (0..8u8).map(|i| vec![i; 64 + usize::from(i)]).collect();
    for (i, bytes) in payloads.iter().enumerate() {
        let mut object =
            SendObject::new(Buffer::copy_from(bytes).expect("a payload copy allocates"));
        object.is_sync = i % 4 == 0;
        object.starts_group = i % 4 == 0;
        object.ends_group = i % 4 == 3;
        object.presentation_time_us = 1_000 * i as u64 + 1;
        object.decode_time_us = object.presentation_time_us;
        sender
            .write(video, object)
            .unwrap_or_else(|failure| panic!("write {i}: {}", failure.error));
    }

    let started = Instant::now();
    let mut got = Vec::new();
    while got.len() < payloads.len() {
        while let Some(object) = receiver
            .poll_object()
            .expect("poll_object on a live receiver")
        {
            assert_eq!(object.track(), Some(track));
            assert_eq!(object.packaging(), Some(Packaging::Raw));
            let id = object.identity().expect("identity is stamped");
            got.push((
                object.payload().to_vec(),
                object.presentation_time_us(),
                id.location.group_id,
                id.location.object_id,
                object.is_keyframe(),
            ));
        }
        if got.len() < payloads.len() {
            receiver
                .wait(live::STEP)
                .expect("the receiver's wait is not interrupted");
        }
        assert!(
            started.elapsed() < live::DEADLINE,
            "received {} of {}",
            got.len(),
            payloads.len()
        );
    }
    for (i, (bytes, pts, group, object, keyframe)) in got.iter().enumerate() {
        assert_eq!(bytes, &payloads[i], "object {i} bytes");
        assert_eq!(*pts, 1_000 * i as u64 + 1, "object {i} presentation time");
        assert_eq!(*keyframe, i % 4 == 0, "object {i} keyframe");
        assert_eq!(*object, (i % 4) as u64, "object {i} id");
        if i > 0 {
            let prev = &got[i - 1];
            assert!(*group >= prev.2, "groups do not go back");
            if i % 4 == 0 {
                assert_eq!(*group, prev.2 + 1, "object {i} opens the next group");
            } else {
                assert_eq!(*group, prev.2, "object {i} stays in its group");
            }
        }
    }
    let stats = sender.stats().expect("stats on a live sender");
    assert_eq!(stats.objects_written, 8);
    assert_eq!(stats.objects_dropped, 0);
    assert_eq!(stats.last_error, None);

    drop(receiver);
    drop(sender);
    drop(receiver_endpoint);
    drop(sender_endpoint);
    relay.stop();
}
