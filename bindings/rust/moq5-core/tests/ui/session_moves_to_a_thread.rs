// The session owner is Send: the native session has no thread affinity, so
// a whole session, with nothing lent out, may move to another thread and be
// driven there.
use moq5_core::{Instant, Perspective, Session, SessionConfig, Version};

fn session() -> Session {
    Session::try_new(
        &SessionConfig::new(Perspective::Client, Version::DRAFT_22),
        Instant::ZERO,
    )
    .unwrap()
}

fn main() {
    let session = session();
    let session = std::thread::spawn(move || {
        let mut session = session;
        session.tick(Instant::ZERO).unwrap();
        session
    })
    .join()
    .unwrap();
    drop(session);
}
