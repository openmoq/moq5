// The positive control of the player cases: a player holding no output
// moves to another thread and is used there.
use moq5::player::{Player, PlayerConfig};
use moq5::{Endpoint, EndpointConfig, Namespace, ReceiverConfig};

fn main() {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap();
    let receiver = ReceiverConfig::live(Namespace::parse("ns").unwrap());
    let player = Player::create(&endpoint, &receiver, &PlayerConfig::new()).unwrap();
    std::thread::spawn(move || {
        if let Ok(Some(output)) = player.poll_output() {
            drop(output);
        }
        drop(player);
    })
    .join()
    .unwrap();
}
