// A player takes calls from one application thread at a time: it is Send
// but not Sync, so a shared reference cannot cross to another thread.
use moq5::player::{Player, PlayerConfig};
use moq5::{Endpoint, EndpointConfig, Namespace, ReceiverConfig};

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let receiver = ReceiverConfig::live(Namespace::parse("ns").unwrap());
    let player = Player::create(&endpoint, &receiver, &PlayerConfig::new()).unwrap();
    let shared = &player;
    std::thread::scope(|s| {
        s.spawn(move || shared.is_closed());
    });
}
