// A decoder owns its storage alone: it is continued into another decoder's
// own storage (copy_from), never cloned.
use moq5_media::locmaf::Decoder;

fn duplicate(decoder: Decoder) -> (Decoder, Decoder) {
    let second = decoder.clone();
    (decoder, second)
}

fn main() {
    let _ = duplicate;
}
