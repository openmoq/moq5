// A decoder moves between threads but is never shared by two.
use moq5_media::locmaf::Decoder;

fn share(decoder: &Decoder) {
    std::thread::scope(|scope| {
        scope.spawn(|| decoder.expects(0, 0));
    });
}

fn main() {
    let _ = share;
}
