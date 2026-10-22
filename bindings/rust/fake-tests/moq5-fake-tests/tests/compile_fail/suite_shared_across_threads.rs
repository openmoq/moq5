// Two scoped threads reaching the provider through one shared `&Suite`:
// the suite is not `Sync`, so this does not compile, and the provider's
// globals cannot be touched from two threads through the safe controls.
use moq5_fake_tests as fake;

fn main() {
    let suite = fake::suite();
    std::thread::scope(|scope| {
        scope.spawn(|| suite.set("same_key", 1));
        scope.spawn(|| suite.set("same_key", 2));
    });
}
