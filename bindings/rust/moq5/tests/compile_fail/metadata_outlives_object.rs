// An object's metadata borrows the object's buffers; it cannot outlive the
// object.
use moq5::{Endpoint, EndpointConfig, Namespace, ObjectMetadata, Receiver, ReceiverConfig};

fn leak<'a>(receiver: &Receiver) -> Option<ObjectMetadata<'a>> {
    let object = receiver.poll_object().unwrap().unwrap();
    object.metadata()
}

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let receiver = Receiver::attach(&endpoint, &ReceiverConfig::live(Namespace::parse("ns").unwrap()))
        .unwrap();
    let _metadata = leak(&receiver);
}
