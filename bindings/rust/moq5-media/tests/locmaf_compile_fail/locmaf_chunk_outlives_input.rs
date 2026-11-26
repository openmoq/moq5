// A chunk borrows the Object it was decoded from: it cannot outlive it.
use moq5_media::locmaf::{Decoder, Limits, Track};

fn read_after_the_object(track: &Track) {
    let mut decoder = Decoder::new(track, Limits::default()).unwrap();
    let chunk;
    {
        let object = vec![0x02u8, 0x04, 0x0a, 0x00, 0x0e, 0x01, 0xde, 0xad, 0xbe, 0xef];
        chunk = decoder.push(0, 0, &object).unwrap();
    }
    let _ = chunk.mdat_payload();
}

fn main() {
    let _ = read_after_the_object;
}
