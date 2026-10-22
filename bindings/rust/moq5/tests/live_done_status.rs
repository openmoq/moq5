//! Relay row: a subscription completed by its publisher with a status is
//! observed as the track's Ended event carrying exactly that status -- a
//! status-propagation row: the peer injects the status, it runs no expiry.
//!
//! The media sender's own completions arrive after an END_OF_TRACK, which
//! ends the track first (the end-of-track row), so the completing message
//! here comes from a native publisher peer (`tests/support/done_peer.c`)
//! that serves the track through the relay and completes the subscription
//! with PUBLISH_DONE (the message's name on drafts 18 and 22 alike) carrying
//! EXPIRED, `0x6` in both registries, without negotiating or running the
//! expiry it names. The relay forwards the foreign terminal unchanged.

mod live;

use std::time::Instant;

use moq5::{Receiver, ReceiverConfig, TrackEventKind, TrackState, Version};

/// EXPIRED in the draft's PUBLISH_DONE status registry (draft 18 section
/// 10.11, draft 22 section 9.9).
fn expired_status(draft: Version) -> u64 {
    match draft {
        Version::DRAFT_18 | Version::DRAFT_22 => 0x6,
        other => panic!("no EXPIRED status known for {other:?}"),
    }
}

#[test]
#[ignore = "needs MOQ5_RELAY and MOQ5_DONE_PEER; run with --ignored"]
fn a_completed_subscription_carries_its_done_status() {
    let mut relay = live::Relay::start("done-status", 14500);
    let status = expired_status(live::draft());
    let mut peer = live::Peer::start(&relay, "live/rust-done", status, 3);

    let receiver_endpoint = live::established(&relay.config());
    let receiver = Receiver::attach(
        &receiver_endpoint,
        &ReceiverConfig::live(
            "live/rust-done"
                .parse()
                .expect("a two-part namespace parses"),
        )
        .auto_subscribe(true),
    )
    .expect("the receiver attaches to an established endpoint");
    let mut seen = Vec::new();
    let track = live::announced_track(&receiver, &mut seen);
    peer.await_line("subscribed video");
    peer.await_line("wrote 3");

    // Every object the peer wrote arrives before the completion.
    let started = Instant::now();
    let mut received = 0u64;
    while received < 3 {
        while let Some(object) = receiver
            .poll_object()
            .expect("poll_object on a live receiver")
        {
            assert_eq!(object.track(), Some(track));
            received += 1;
        }
        if received < 3 {
            let _ = receiver.wait(live::STEP);
        }
        assert!(
            started.elapsed() < live::DEADLINE,
            "received {received} of 3 objects; peer log:\n{}",
            peer.log_text()
        );
    }
    peer.await_line(&format!("done {status}"));

    // The completion, as the track's Ended event with the message's status.
    let ended = live::until_event(&receiver, &mut seen, |k| k == TrackEventKind::Ended);
    assert_eq!(ended.track, Some(track));
    assert_eq!(
        ended.done_status,
        Some(status),
        "the Ended event carries the completing message's status; peer log:\n{}",
        peer.log_text()
    );
    assert_eq!(receiver.track_state(track), Ok(TrackState::Ended));
    // One Ended for the track in the whole queue as it stands (an event
    // queued behind the first is read too); a later tick is not this live
    // row's claim.
    live::drain_queued(&receiver, &mut seen);
    assert_eq!(
        live::ended_count(&seen, track),
        1,
        "one Ended event for the track in the queue: {seen:?}"
    );

    drop(receiver);
    drop(receiver_endpoint);
    peer.stop();
    relay.stop();
}
