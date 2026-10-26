// While a player exists its receiver's queues are the player's: the view of
// the receiver it offers has no poll.
use moq5::player::{Player, PlayerConfig};
use moq5::{Endpoint, EndpointConfig, Namespace, ReceiverConfig};

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let receiver = ReceiverConfig::live(Namespace::parse("ns").unwrap());
    let player = Player::create(&endpoint, &receiver, &PlayerConfig::new()).unwrap();
    let _object = player.receiver().poll_object();
}
