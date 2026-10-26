// An output borrows its player, so the player cannot move (to another
// thread, here) while an output of it is held.
use moq5::player::{Player, PlayerConfig};
use moq5::{Endpoint, EndpointConfig, Namespace, ReceiverConfig};

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let receiver = ReceiverConfig::live(Namespace::parse("ns").unwrap());
    let player = Player::create(&endpoint, &receiver, &PlayerConfig::new()).unwrap();
    let output = player.poll_output().unwrap().unwrap();
    let handle = std::thread::spawn(move || drop(player));
    drop(output);
    handle.join().unwrap();
}
