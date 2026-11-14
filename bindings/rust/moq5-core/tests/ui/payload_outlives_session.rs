// A payload cannot outlive the session it came from.
use moq5_core::{Event, Payload};
use moq5_core::{Instant, Perspective, Session, SessionConfig, Version};

fn session() -> Session {
    Session::try_new(
        &SessionConfig::new(Perspective::Client, Version::DRAFT_22),
        Instant::ZERO,
    )
    .unwrap()
}

fn payload() -> Option<Payload<'static>> {
    let mut session = session();
    match session.poll_event().unwrap() {
        Some(Event::ObjectReceived(object)) => object.payload,
        _ => None,
    }
}

fn main() {
    drop(payload());
}
