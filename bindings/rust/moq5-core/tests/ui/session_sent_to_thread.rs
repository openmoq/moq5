// The session owner is confined to the thread that created it: no written
// native contract lets a whole session move, so it is not Send.
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
    std::thread::spawn(move || drop(session));
}
