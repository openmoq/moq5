// A buffer is the sole reference to its bytes: it cannot be cloned.
use moq5::Buffer;

fn main() {
    let buffer = Buffer::copy_from(b"frame").unwrap();
    let _second = buffer.clone();
}
