// An owned action holds independent copies: it may move between threads.
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
        if let Ok(owned) = action.into_owned() {
            std::thread::spawn(move || drop(owned)).join().unwrap();
        }
    }
    session.tick(Instant::ZERO).unwrap();
}
