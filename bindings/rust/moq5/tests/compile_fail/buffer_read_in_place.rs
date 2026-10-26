// The positive control of the compile-fail path: a buffer read where it
// lives compiles, so a refusal of the other fixtures is the type checker's
// verdict on them, not on the fixture package.
use moq5::Buffer;

fn main() {
    let buffer = Buffer::copy_from(b"frame").unwrap();
    let _len = buffer.len();
    let _bytes = buffer.as_slice();
}
