// The positive control: the controls on the thread that took the suite
// compile, so a refusal of the shared case is the type checker's verdict on
// the sharing, not on the fixture package.
use moq5_fake_tests as fake;

fn main() {
    let suite = fake::suite();
    suite.set("same_key", 1);
    let _value = suite.get("same_key");
    let _log = suite.take_log();
}
