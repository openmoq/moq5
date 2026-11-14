// A record is read in place and dropped; then the session advances.
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
    let mut reason = Vec::new();
    while let Some(event) = session.poll_event().unwrap() {
        if let Event::SessionClosed(closed) = event {
            reason = closed.reason.to_vec();
        }
    }
    session.tick(Instant::ZERO).unwrap();
    println!("{}", reason.len());
}
