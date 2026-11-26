// The positive control: a decoder and an encoder move to another thread,
// where they are used and dropped, and a chunk is read and released before
// the next push.
use moq5_media::locmaf::{write_cmaf, Decoder, Encoder, Limits, Track};

fn on_another_thread(track: Track, mut decoder: Decoder, mut encoder: Encoder) {
    std::thread::spawn(move || {
        let object = [0x02u8, 0x04, 0x0a, 0x00, 0x0e, 0x01, 0xde, 0xad, 0xbe, 0xef];
        let chunk = decoder.push(0, 0, &object).unwrap();
        let bytes = write_cmaf(&track, &chunk).unwrap();
        let _ = decoder.push(0, 1, &object[..2]);
        let _ = encoder.encode_chunk(0, 0, &bytes);
        encoder.commit();
        drop(decoder);
        drop(encoder);
    })
    .join()
    .unwrap();
    let _ = Limits::default();
}

fn main() {
    let _ = on_another_thread;
}
