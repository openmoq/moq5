// A payload is one native reference: it cannot be cloned.
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
            let _second = payload.clone();
        }
    }
}
