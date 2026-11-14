// An owned buffer fed to the session is consumed: only a refusal hands it
// back.
use moq5_core::{Buffer, StreamRef};
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
    let buffer = Buffer::copy_from(b"bytes").unwrap();
    let stream = StreamRef::peer(0).unwrap();
    let _ = session.on_data(stream, buffer, false, Instant::ZERO);
    println!("{}", buffer.len());
}
