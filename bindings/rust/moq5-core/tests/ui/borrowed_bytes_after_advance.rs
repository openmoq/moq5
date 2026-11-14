// Borrowed event bytes point into the session's scratch: they cannot be
// used after the next advancing call.
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
    let reason = match session.poll_event().unwrap() {
        Some(Event::SessionClosed(closed)) => closed.reason,
        _ => &[][..],
    };
    session.tick(Instant::ZERO).unwrap();
    println!("{}", reason.len());
}
