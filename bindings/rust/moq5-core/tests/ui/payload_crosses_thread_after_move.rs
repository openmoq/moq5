// A payload borrows the session and shares its native count: it stays with
// the session. Moving the session to another thread while a payload is held
// would carry the payload across; the borrow forbids the move, and the
// payload itself is not Send.
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
    if let Some(Event::ObjectReceived(object)) = session.poll_event().unwrap() {
        if let Some(payload) = object.payload {
            std::thread::spawn(move || payload.len()).join().unwrap();
        }
    }
}
