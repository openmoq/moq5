// A payload leaves the session as a buffer with storage of its own, which
// may move between threads after the session advances.
use moq5_core::Event;
use moq5_core::{Instant, Perspective, Session, SessionConfig, Version};

fn session() -> Session {
    Session::try_new(
        &SessionConfig::new(Perspective::Client, Version::DRAFT_22),
        Instant::ZERO,
    )
    .unwrap()
}

fn main() {
    let mut session = session();
    let mut copy = None;
    if let Some(Event::ObjectReceived(object)) = session.poll_event().unwrap() {
        copy = object.payload.map(|payload| payload.into_buffer().unwrap());
    }
    session.tick(Instant::ZERO).unwrap();
    if let Some(buffer) = copy {
        std::thread::spawn(move || buffer.len()).join().unwrap();
    }
}
