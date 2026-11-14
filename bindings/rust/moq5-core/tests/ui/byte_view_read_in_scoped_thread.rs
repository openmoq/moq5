// A borrowed byte slice is an ordinary shared slice: a scoped thread may
// read it while the session borrow it carries lasts, which keeps the session
// from advancing meanwhile.
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
    if let Some(Event::SessionClosed(closed)) = session.poll_event().unwrap() {
        let reason: &[u8] = closed.reason;
        let length = std::thread::scope(|scope| scope.spawn(|| reason.len()).join().unwrap());
        println!("{length}");
    }
    session.tick(Instant::ZERO).unwrap();
}
