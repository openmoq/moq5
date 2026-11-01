use moq5::{Endpoint, EndpointConfig, Error, MediaType, Namespace, Packaging, SendTrackConfig, Sender, SenderConfig};
use moq5_fake_tests as fake;

fn config() -> SenderConfig {
    SenderConfig::live(Namespace::parse("initial/catalog").expect("a valid namespace"))
}

fn tracks() -> Vec<SendTrackConfig> {
    ["video", "audio"].into_iter().map(|name| {
        SendTrackConfig::new(name, MediaType::Video, Packaging::Raw, "avc1.42c01e")
            .bitrate(1_000_000)
    }).collect()
}

fn endpoint() -> Endpoint {
    Endpoint::connect(&EndpointConfig::new("https://relay.example/moq"))
        .expect("the provider accepts the endpoint")
}

#[test]
fn all_initial_tracks_precede_the_first_pump() {
    let suite = fake::suite();
    let endpoint = endpoint();
    let mut startup = Sender::attach_with_tracks(&endpoint, &config(), tracks())
        .expect("the task is accepted");
    assert!(startup.poll().expect("pending is not an error").is_none());
    assert_eq!(suite.get("sender_attach"), 0);
    suite.pump(&endpoint, false);
    let (sender, handles) = startup.poll().expect("registration succeeds").expect("completed");
    assert_eq!(handles.len(), 2);
    let log = suite.take_log();
    let last_add = log.iter().rposition(|line| line.starts_with("add_track ")).expect("tracks added");
    let pump = log.iter().position(|line| line.starts_with("pump ")).expect("the hook boundary");
    assert!(last_add < pump, "no hook observes a partial initial set: {log:?}");
    assert_eq!(startup.poll().expect_err("result taken once"), Error::WRONG_STATE);
    drop(sender);
    drop(startup);
    drop(endpoint);
    assert_eq!(suite.get("alloc_live"), 0);
}

#[test]
fn cancellation_before_execution_attaches_nothing_and_does_not_pin_the_endpoint() {
    let suite = fake::suite();
    let endpoint = endpoint();
    let startup = Sender::attach_with_tracks(&endpoint, &config(), tracks()).expect("queued");
    drop(endpoint);
    drop(startup);
    assert_eq!(suite.get("sender_attach"), 0);
    assert_eq!(suite.get("destroy"), 1);
    assert_eq!(suite.get("alloc_live"), 0);
}

#[test]
fn an_unpolled_result_is_destroyed_before_the_last_endpoint() {
    let suite = fake::suite();
    let endpoint = endpoint();
    let startup = Sender::attach_with_tracks(&endpoint, &config(), tracks()).expect("queued");
    suite.pump(&endpoint, false);
    drop(endpoint);
    drop(startup);
    let log = suite.take_log();
    let sender = log.iter().position(|line| line.starts_with("sender_destroy")).expect("sender destroyed");
    let stop = log.iter().position(|line| line == "stop").expect("endpoint stopped");
    assert!(sender < stop, "no attached sender at endpoint stop: {log:?}");
    assert_eq!(suite.get("alloc_live"), 0);
}

#[test]
fn a_failed_add_discards_the_partial_sender_before_the_hook() {
    let suite = fake::suite();
    suite.set("add_track_fail_at", 2);
    let endpoint = endpoint();
    let mut startup = Sender::attach_with_tracks(&endpoint, &config(), tracks()).expect("queued");
    suite.pump(&endpoint, false);
    assert_eq!(startup.poll().expect_err("second add fails"), Error::NOMEM);
    assert_eq!(suite.get("add_track"), 2);
    let log = suite.take_log();
    assert!(log.iter().any(|line| line == "pump attachments=0"), "partial sender never pumps: {log:?}");
    drop(startup);
    drop(endpoint);
    assert_eq!(suite.get("alloc_live"), 0);
}

#[test]
fn refusal_and_terminal_execution_preserve_errors() {
    let suite = fake::suite();
    let endpoint = endpoint();
    suite.set("post_result", i64::from(Error::CLOSED.code()));
    assert_eq!(Sender::attach_with_tracks(&endpoint, &config(), tracks()).expect_err("refused"), Error::CLOSED);
    suite.set("post_result", 0);
    let mut startup = Sender::attach_with_tracks(&endpoint, &config(), tracks()).expect("queued");
    suite.pump(&endpoint, true);
    assert_eq!(startup.poll().expect_err("terminal task"), Error::CLOSED);
    assert_eq!(suite.get("sender_attach"), 0);
    drop(startup);
    drop(endpoint);
    assert_eq!(suite.get("alloc_live"), 0);
}

#[test]
fn completion_before_post_returns_is_owned_once() {
    let suite = fake::suite();
    suite.set("post_immediate", 1);
    let endpoint = endpoint();
    let mut startup = Sender::attach_with_tracks(&endpoint, &config(), tracks()).expect("queued");
    let (sender, handles) = startup.poll().expect("completed").expect("result");
    assert_eq!(handles.len(), 2);
    drop(sender);
    drop(startup);
    drop(endpoint);
    assert_eq!(suite.get("alloc_live"), 0);
}

#[test]
fn formatter_unwind_does_not_poison_execution_or_teardown() {
    use std::fmt::{self, Write};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    struct PanickingWriter;
    impl Write for PanickingWriter {
        fn write_str(&mut self, _: &str) -> fmt::Result {
            panic!("application formatter unwound");
        }
    }

    let suite = fake::suite();
    let endpoint = endpoint();
    let mut startup = Sender::attach_with_tracks(&endpoint, &config(), tracks()).expect("queued");
    assert!(catch_unwind(AssertUnwindSafe(|| {
        write!(PanickingWriter, "{startup:?}").expect("formatting succeeds");
    })).is_err());
    // This is the former poisoning path: Mutex::fmt calls its writer with
    // "data" while the state guard is held. An opaque Debug must not reach it.
    struct StateWriter;
    impl Write for StateWriter {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            assert_ne!(text, "data", "application formatter saw protected state");
            Ok(())
        }
    }
    assert!(catch_unwind(AssertUnwindSafe(|| {
        write!(StateWriter, "{startup:?}").expect("opaque formatting succeeds");
    })).is_ok());
    assert_eq!(format!("{startup:?}"), "SenderStartup { .. }");
    assert!(startup.poll().expect("still pending").is_none());
    suite.pump(&endpoint, false);
    let (sender, handles) = startup.poll().expect("not poisoned").expect("completed");
    assert_eq!(handles.len(), 2);
    drop(sender);
    drop(startup);
    drop(endpoint);
    assert_eq!(suite.get("alloc_live"), 0);
}
