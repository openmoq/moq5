// A buffer's refcount is not atomic: it moves between threads but is not
// shared across them.
use moq5::Buffer;

fn main() {
    let buffer = Buffer::copy_from(b"frame").unwrap();
    let shared = &buffer;
    std::thread::scope(|s| {
        s.spawn(move || shared.len());
    });
}
