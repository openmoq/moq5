fn requires_sync<T: Sync>() {}

fn main() {
    requires_sync::<moq5::SenderStartup>();
}
