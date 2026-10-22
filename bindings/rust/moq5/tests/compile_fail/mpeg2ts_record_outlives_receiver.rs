// A track's MPEG-TS record borrows the receiver that owns the handle; it
// cannot outlive that receiver.
use moq5::{Endpoint, EndpointConfig, Mpeg2tsRecordRef, Namespace, Receiver, ReceiverConfig};

fn leak<'a>(endpoint: &Endpoint) -> Option<Mpeg2tsRecordRef<'a>> {
    let receiver = Receiver::attach(endpoint, &ReceiverConfig::live(Namespace::parse("ns").unwrap()))
        .unwrap();
    let event = receiver.poll_track().unwrap().unwrap();
    let description = receiver.track_description(event.track().unwrap()).unwrap();
    description.mpeg2ts()
}

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let _record = leak(&endpoint);
}
