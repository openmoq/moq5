// A sender takes calls from one application thread at a time: it is Send
// but not Sync, so a shared reference cannot cross to another thread.
use moq5::{Endpoint, EndpointConfig, Namespace, Sender, SenderConfig};

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let sender =
        Sender::attach(&endpoint, &SenderConfig::live(Namespace::parse("ns").unwrap())).unwrap();
    let shared = &sender;
    std::thread::scope(|s| {
        s.spawn(move || shared.is_ready());
    });
}
