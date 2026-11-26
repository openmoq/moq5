// A chunk borrows the decoder that produced it: it cannot be returned past
// the decoder's life.
use moq5_media::locmaf::{Chunk, Decoder, Limits, Track};

fn chunk_of<'object>(track: &Track, object: &'object [u8]) -> Chunk<'static, 'object> {
    let mut decoder = Decoder::new(track, Limits::default()).unwrap();
    decoder.push(0, 0, object).unwrap()
}

fn main() {
    let _ = chunk_of;
}
