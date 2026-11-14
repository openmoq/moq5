// Every event record, a scalar-only one included, stays on the session's
// thread.
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
    if let Some(Event::RequestReady(ready)) = session.poll_event().unwrap() {
        std::thread::scope(|scope| {
            scope.spawn(move || drop(ready));
        });
    }
}
