// A chunk holds its decoder exclusively: no other push while it lives.
use moq5_media::locmaf::{Decoder, Limits, Track};

fn push_twice(track: &Track, first: &[u8], second: &[u8]) {
    let mut decoder = Decoder::new(track, Limits::default()).unwrap();
    let chunk = decoder.push(0, 0, first).unwrap();
    let _next = decoder.push(0, 1, second);
    let _ = chunk.samples();
}

fn main() {
    let _ = push_twice;
}
