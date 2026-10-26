//! Relay row: the sticky interrupt latch returns a blocked wait at once on
//! every attachment and clears again.

mod live;

use std::time::{Duration, Instant};

use moq5::{
    Error, MediaType, Namespace, Packaging, Receiver, ReceiverConfig, SendTrackConfig, Sender,
    SenderConfig, WaitResult,
};

#[test]
#[ignore = "needs MOQ5_RELAY; run with --ignored"]
fn set_interrupted_returns_waits_and_clears() {
    let mut relay = live::Relay::start("interrupt", 14480);
    let namespace = Namespace::parse("live/rust-interrupt").expect("a two-part namespace parses");
    let endpoint = live::established(&relay.config());
    let sender = Sender::attach(&endpoint, &SenderConfig::live(namespace.clone()))
        .expect("the sender attaches to an established endpoint");
    sender
        .add_track(
            &SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e")
                .bitrate(1),
        )
        .expect("a raw video track is added");
    live::ready(&sender);
    let receiver = Receiver::attach(
        &endpoint,
        &ReceiverConfig::live(namespace).auto_subscribe(false),
    )
    .expect("the receiver attaches beside the sender");

    // The receiver is Send, not Sync: the blocked wait owns it on another
    // thread while this thread sets the latch through the shared endpoint.
    // The wait is level-triggered, so it returns Ready while discovery is
    // still delivering events; the row measures the return after the latch.
    let latched = std::sync::Mutex::new(None::<Instant>);
    let receiver = std::thread::scope(|s| {
        let waits = s.spawn(|| {
            let started = Instant::now();
            loop {
                match receiver.wait(Duration::from_secs(10)) {
                    Ok(WaitResult::Ready) => {
                        while receiver
                            .poll_track()
                            .expect("poll_track before the latch")
                            .is_some()
                        {}
                        assert!(started.elapsed() < live::DEADLINE, "never blocked");
                    }
                    other => return (other, Instant::now(), receiver),
                }
            }
        });
        std::thread::sleep(Duration::from_millis(300));
        *latched.lock().expect("the latch time is not poisoned") = Some(Instant::now());
        endpoint.set_interrupted(true);
        let (result, returned, receiver) = waits.join().expect("the waiting thread returns");
        assert_eq!(result, Err(Error::INTERRUPTED));
        let since_latch = returned.duration_since(
            latched
                .lock()
                .expect("the latch time is not poisoned")
                .expect("the latch time was recorded"),
        );
        assert!(
            since_latch < Duration::from_secs(2),
            "returned {since_latch:?} after the latch"
        );
        receiver
    });
    assert_eq!(
        receiver.wait(Duration::from_millis(10)),
        Err(Error::INTERRUPTED),
        "sticky"
    );
    assert_eq!(
        sender.wait(Duration::from_millis(10)),
        Err(Error::INTERRUPTED)
    );
    assert_eq!(
        endpoint.wait(Duration::from_millis(10)),
        Err(Error::INTERRUPTED)
    );
    assert_eq!(receiver.poll_object().err(), Some(Error::INTERRUPTED));

    endpoint.set_interrupted(false);
    assert!(matches!(
        receiver.wait(Duration::from_millis(50)),
        Ok(WaitResult::Ready | WaitResult::TimedOut)
    ));
    assert!(matches!(
        sender.wait(Duration::from_millis(50)),
        Ok(WaitResult::Ready | WaitResult::TimedOut)
    ));
    assert!(matches!(
        endpoint.wait(Duration::from_millis(50)),
        Ok(WaitResult::Ready | WaitResult::TimedOut)
    ));
    assert!(receiver
        .poll_object()
        .expect("poll_object after the latch cleared")
        .is_none());

    drop(receiver);
    drop(sender);
    drop(endpoint);
    relay.stop();
}
