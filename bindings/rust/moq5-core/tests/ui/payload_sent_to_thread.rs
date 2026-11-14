// A payload may share its native count with the session: it never moves to
// another thread.
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
            std::thread::scope(|scope| {
                scope.spawn(move || payload.len());
            });
        }
    }
}
