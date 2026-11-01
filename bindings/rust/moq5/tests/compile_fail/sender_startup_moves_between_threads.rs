fn requires_send<T: Send>() {}

fn main() {
    requires_send::<moq5::SenderStartup>();
}
