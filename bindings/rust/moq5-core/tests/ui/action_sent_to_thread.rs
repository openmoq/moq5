// A polled action stays on the session's thread.
use moq5_core::ActionPoll;
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
    if let Ok(ActionPoll::Ready(action)) = session.poll_action() {
        std::thread::scope(|scope| {
            scope.spawn(move || action.kind());
        });
    }
}
