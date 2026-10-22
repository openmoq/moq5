// A received object is released on the polling thread: it is not Send.
use moq5::{Endpoint, EndpointConfig, Namespace, Receiver, ReceiverConfig};

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let receiver = Receiver::attach(&endpoint, &ReceiverConfig::live(Namespace::parse("ns").unwrap()))
        .unwrap();
    let object = receiver.poll_object().unwrap().unwrap();
    std::thread::spawn(move || drop(object));
}
