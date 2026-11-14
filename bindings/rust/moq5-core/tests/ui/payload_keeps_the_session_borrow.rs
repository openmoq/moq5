// A payload taken out of its record keeps the record's session borrow:
// dropping the record does not let the session advance while the payload
// lives.
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
    let event = session.poll_event().unwrap();
    let payload = match event {
        Some(Event::ObjectReceived(object)) => object.payload,
        _ => None,
    };
    session.tick(Instant::ZERO).unwrap();
    println!("{}", payload.map_or(0, |p| p.len()));
}
