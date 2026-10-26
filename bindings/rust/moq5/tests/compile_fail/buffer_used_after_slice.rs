// Slicing consumes the buffer, so the slice stays the only reference to
// the storage: the buffer cannot be read after it is sliced.
use moq5::Buffer;

fn main() {
    let buffer = Buffer::copy_from(b"frame").unwrap();
    let _slice = buffer.slice(1..3).unwrap();
    let _len = buffer.len();
}
