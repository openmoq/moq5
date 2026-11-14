// A view over a native array the session lends stays on the session's
// thread: it cannot be shared with another, scoped or not.
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
    if let Some(Event::SubscribeRequest(request)) = session.poll_event().unwrap() {
        let namespace = &request.namespace;
        std::thread::scope(|scope| {
            scope.spawn(|| namespace.len());
        });
    }
}
