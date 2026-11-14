// A polled action borrows the session exclusively: no advancing call can
// run while it is alive.
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
        session.tick(Instant::ZERO).unwrap();
        drop(action);
    }
}
