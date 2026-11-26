// An encoder moves between threads but is never shared by two.
use moq5_media::locmaf::Encoder;

fn share(encoder: &Encoder) {
    std::thread::scope(|scope| {
        scope.spawn(|| encoder.stats());
    });
}

fn main() {
    let _ = share;
}
