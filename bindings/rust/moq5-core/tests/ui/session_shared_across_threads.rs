// The session owner is not Sync: no other thread may observe it.
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
    std::thread::scope(|scope| {
        scope.spawn(|| session.state());
    });
}
