// A track description borrows the receiver that owns the handle it reads;
// it cannot outlive that receiver.
use moq5::{Endpoint, EndpointConfig, Namespace, Receiver, ReceiverConfig, TrackDescription};

fn leak<'a>(endpoint: &Endpoint) -> TrackDescription<'a> {
    let receiver = Receiver::attach(endpoint, &ReceiverConfig::live(Namespace::parse("ns").unwrap()))
        .unwrap();
    let event = receiver.poll_track().unwrap().unwrap();
    event.description().unwrap().unwrap()
}

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let _description = leak(&endpoint);
}
