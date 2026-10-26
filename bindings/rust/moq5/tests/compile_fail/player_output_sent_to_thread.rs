// A player output shares buffers with its player's engine and is released
// on the player's thread: it is not Send.
use moq5::player::{Player, PlayerConfig};
use moq5::{Endpoint, EndpointConfig, Namespace, ReceiverConfig};

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let receiver = ReceiverConfig::live(Namespace::parse("ns").unwrap());
    let player = Player::create(&endpoint, &receiver, &PlayerConfig::new()).unwrap();
    let output = player.poll_output().unwrap().unwrap();
    std::thread::scope(|s| {
        s.spawn(move || drop(output));
    });
}
