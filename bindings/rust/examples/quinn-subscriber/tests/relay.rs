//! The subscriber against a moq5-relay on loopback, over real QUIC, with a
//! native publisher behind the relay.
//!
//! Those rows are ignored by default: they need the relay binary and the
//! publisher peer the relay rows build (`moq5/tests/support/done_peer.c`).
//!
//! - `MOQ5_RELAY`: the moq5-relay binary.
//! - `MOQ5_DONE_PEER`: the publisher peer. It announces a namespace, serves
//!   one track named `video`, writes a number of 32-byte objects in one
//!   subgroup and completes the subscription with a status.
//! - `MOQ5_RELAY_DRAFT`: 18 or 22 (default 22).
//! - `MOQ5_RELAY_WORK`: where configurations and logs go (default: the
//!   test's temporary directory).
//! - `openssl` on the path: each row makes the relay a certificate with
//!   subject alternative names, which is also the subscriber's trust root.
//!
//! The tests at the end are not ignored and need none of that. They put
//! stand-ins in the relay's and the publisher's place to show that a stage
//! which fails to start leaves no process behind.

mod support;

use std::path::{Path, PathBuf};
use std::time::Duration;

use support::{Owned, Run};

/// The status the publisher completes the subscription with.
const STATUS: u64 = 6;
/// How long the relay and the publisher each have to become ready.
const READY: Duration = Duration::from_secs(10);
/// How long the subscriber has to end.
const SUBSCRIBED: Duration = Duration::from_secs(30);

fn draft() -> String {
    std::env::var("MOQ5_RELAY_DRAFT").unwrap_or_else(|_| "22".into())
}

fn words(words: &[&str]) -> Vec<String> {
    words.iter().map(|&word| word.to_owned()).collect()
}

/// What a stage runs: the relay and the publisher, as commands, and how
/// long each has to become ready.
struct Cast {
    relay: Vec<String>,
    publisher: Vec<String>,
    ready: Duration,
}

/// A relay and a publisher behind it. Each is owned from the moment it
/// exists, so both are stopped when this is dropped, and whichever had
/// started is stopped when starting the stage fails.
struct Stage {
    peer: Owned,
    _relay: Owned,
    port: u16,
    dir: PathBuf,
}

impl Stage {
    /// Start a relay on `port` (its admin endpoint on `port + 1`) and a
    /// publisher that will write `objects` objects.
    fn start(row: &str, port: u16, objects: u32) -> Stage {
        // Everything that can fail without a child is done before one
        // exists.
        let relay = std::env::var("MOQ5_RELAY").expect("MOQ5_RELAY names the moq5-relay binary");
        let publisher =
            std::env::var("MOQ5_DONE_PEER").expect("MOQ5_DONE_PEER names the publisher peer");
        let dir = support::work(&format!("quinn-{row}-{}", draft()));
        let cert = dir.join("cert.pem");
        let key = dir.join("key.pem");
        support::certificate(&dir, &cert, &key);
        let config = dir.join("relay.json");
        std::fs::write(
            &config,
            format!(
                r#"{{
  "listener": {{ "transport": "picoquic", "host": "127.0.0.1", "port": {port}, "versions": [{draft}], "lanes": 1, "cert": "{cert}", "key": "{key}" }},
  "admin": {{ "tcp": {{ "host": "127.0.0.1", "port": {admin} }} }},
  "auth": {{ "mode": "allow_all" }},
  "logging": {{ "format": "text" }}
}}
"#,
                draft = draft(),
                cert = cert.display(),
                key = key.display(),
                admin = port + 1,
            ),
        )
        .expect("the relay configuration is written");
        let config = config.to_str().expect("the path is UTF-8");
        let (port_arg, objects) = (port.to_string(), objects.to_string());
        let cast = Cast {
            relay: words(&[&relay, "serve", "--config", config]),
            publisher: words(&[
                &publisher,
                "--port",
                &port_arg,
                "--draft",
                &draft(),
                "--namespace",
                "quinn/example",
                "--status",
                &STATUS.to_string(),
                "--objects",
                &objects,
            ]),
            ready: READY,
        };
        Stage::assemble(&cast, port, dir)
    }

    /// Start the cast and wait for each to be ready. A failure here drops
    /// whatever had started.
    fn assemble(cast: &Cast, port: u16, dir: PathBuf) -> Stage {
        let log = dir.join("relay.log");
        let mut relay = support::spawn(&cast.relay, &log, None);
        relay.await_line(&log, "listening on", "the relay", cast.ready);
        let log = dir.join("peer.log");
        let mut peer = support::spawn(&cast.publisher, &log, None);
        peer.await_line(&log, "announced", "the publisher", cast.ready);
        Stage {
            peer,
            _relay: relay,
            port,
            dir,
        }
    }

    /// Run the subscriber to its end and return its lines. It must exit 0
    /// and print nothing on standard error.
    fn subscribe(&self, extra: &[&str]) -> Vec<String> {
        let url = format!("moqt://127.0.0.1:{}", self.port);
        let ca = self.dir.join("cert.pem");
        let mut args = vec!["--url", &url, "--ca", ca.to_str().expect("UTF-8")];
        args.extend(["--namespace", "quinn/example", "--track", "video"]);
        let draft = draft();
        args.extend(["--draft", &draft]);
        args.extend(extra);
        let run = support::run(&support::subscriber(&args), &self.dir, "sub", SUBSCRIBED);
        succeeded(&run, &self.dir);
        run.out
    }
}

/// The subscriber must have exited 0 with nothing on standard error; fail
/// with everything known otherwise.
fn succeeded(run: &Run, dir: &Path) {
    assert!(
        run.status.success() && run.err.is_empty(),
        "the subscriber ended with {}:\n{}\nstandard error:\n{}\nthe relay:\n{}\nthe publisher:\n{}",
        run.status,
        run.out.join("\n"),
        run.err,
        support::text(&dir.join("relay.log")),
        support::text(&dir.join("peer.log")),
    );
}

/// The lines every run begins with, and what follows them.
fn after_setup(lines: &[String], port: u16) -> &[String] {
    let connected = format!("connected 127.0.0.1:{port} moqt-{}", draft());
    assert_eq!(lines[0], connected);
    assert_eq!(lines[1], "setup complete");
    assert!(lines[2].starts_with("subscribed alias "), "{}", lines[2]);
    &lines[3..]
}

fn objects(count: u64) -> Vec<String> {
    (0..count)
        .map(|object| format!("object 0 {object} 32 bytes"))
        .collect()
}

/// The publisher writes five objects and completes the subscription. The
/// subscriber prints each object in order, the completion with the
/// publisher's status and its one stream, and closes the session cleanly.
/// Nothing had to be held.
#[test]
#[ignore = "needs MOQ5_RELAY and MOQ5_DONE_PEER; run with --ignored"]
fn the_publisher_completes_the_subscription() {
    let mut stage = Stage::start("complete", 14700, 5);
    let lines = stage.subscribe(&[]);
    let mut expected = objects(5);
    expected.push(format!("done status {STATUS} streams 1"));
    expected.push("closed 0".into());
    expected.push("held kept 0 refused 0 owed 0".into());
    assert_eq!(after_setup(&lines, stage.port), expected.as_slice());
    let log = stage.dir.join("peer.log");
    stage
        .peer
        .await_line(&log, &format!("done {STATUS}"), "the publisher", READY);
}

/// The ending of a run that wanted fewer objects than the publisher writes,
/// from the lines after its last object. One of two:
/// - the unsubscribe was accepted: the session stops the streams it was
///   reading; each is retired, or had ended by itself before the stop took
///   effect; then the session closes;
/// - the publisher had already completed the subscription: the unsubscribe
///   is refused, and the completion is reported instead.
///
/// Which one happens depends on whether the publisher's completion arrived
/// first; the row prints it. Returns the count of what the host held.
fn ending(rest: &[String]) -> u64 {
    let held: Vec<&str> = rest.last().expect("a last line").split(' ').collect();
    assert_eq!(held.len(), 7, "{rest:?}");
    assert_eq!(
        (held[0], held[1], held[3], held[4], held[5], held[6]),
        ("held", "kept", "refused", "0", "owed", "0")
    );
    let rest = &rest[..rest.len() - 1];
    if rest[0] == "unsubscribed" {
        let stopped: Vec<&str> = rest
            .iter()
            .filter_map(|l| l.strip_prefix("stopping "))
            .collect();
        let retired = rest.iter().filter_map(|l| l.strip_prefix("retired "));
        let ended = rest.iter().filter_map(|l| l.strip_prefix("ended "));
        let mut settled: Vec<&str> = retired.clone().chain(ended).collect();
        assert!(!stopped.is_empty(), "no stream was stopped: {rest:?}");
        let mut sorted = stopped.clone();
        sorted.sort_unstable();
        settled.sort_unstable();
        assert_eq!(sorted, settled, "every stopped stream is settled, once");
        assert_eq!(rest.len(), 2 + stopped.len() + settled.len());
        assert_eq!(rest[rest.len() - 1], "closed 0");
        println!(
            "ending: unsubscribed, {} streams stopped, {} of them retired",
            stopped.len(),
            retired.count()
        );
    } else {
        let done = format!("done status {STATUS} streams 1");
        assert_eq!(rest, [done, "closed 0".to_owned()]);
        println!("ending: the publisher had completed the subscription");
    }
    held[2].parse().expect("a count")
}

/// With an event queue of two, objects arrive faster than their events can
/// be queued: the session keeps input it cannot finish, and the host
/// resumes it. The subscriber wants three hundred objects of two thousand:
/// every one arrives, in order, and the run ends as [`ending`] describes.
/// The publisher's own exit is not judged here: it treats a write refused
/// after an unsubscribe as fatal.
#[test]
#[ignore = "needs MOQ5_RELAY and MOQ5_DONE_PEER; run with --ignored"]
fn a_tight_session_keeps_input_and_is_resumed() {
    let stage = Stage::start("tight", 14710, 2000);
    let lines = stage.subscribe(&["--tight", "--objects", "300"]);
    let rest = after_setup(&lines, stage.port);
    assert_eq!(&rest[..300], objects(300).as_slice());
    let kept = ending(&rest[300..]);
    assert!(kept >= 1, "the session kept no read: {rest:?}");
}

/// The subscriber wants two objects of two thousand. After the second it
/// unsubscribes, and the run ends as [`ending`] describes. The publisher's
/// own exit is not judged here either.
#[test]
#[ignore = "needs MOQ5_RELAY and MOQ5_DONE_PEER; run with --ignored"]
fn enough_objects_end_the_subscription() {
    let stage = Stage::start("enough", 14720, 2000);
    let lines = stage.subscribe(&["--objects", "2"]);
    let rest = after_setup(&lines, stage.port);
    assert_eq!(&rest[..2], objects(2).as_slice());
    ending(&rest[2..]);
}

/// A stand-in for a relay or a publisher: it writes its process ID to
/// `pid`, prints `line` and stays.
#[cfg(unix)]
fn stand_in(pid: &Path, line: &str) -> Vec<String> {
    let script = format!(
        "echo $$ > '{}'; echo '{line}'; exec sleep 60",
        pid.display()
    );
    words(&["sh", "-c", &script])
}

/// Whether the process whose ID is in `pid` is gone: not running, and not
/// waiting to be reaped either.
#[cfg(unix)]
fn gone(pid: &Path) -> bool {
    let id = support::text(pid);
    let id = id.trim();
    assert!(!id.is_empty(), "{} names no process", pid.display());
    let probe = std::process::Command::new("sh")
        .args(["-c", &format!("kill -0 {id}")])
        .stderr(std::process::Stdio::null())
        .status()
        .expect("the probe runs");
    !probe.success()
}

/// What `step` panicked with.
#[cfg(unix)]
fn failure(step: impl FnOnce()) -> String {
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(step));
    let payload = caught.expect_err("the step fails");
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => (*payload.downcast::<&str>().expect("a message")).to_owned(),
    }
}

/// How long a stand-in has to become ready, and how long one that never
/// will is waited for.
#[cfg(unix)]
const SOON: Duration = Duration::from_secs(2);

/// A relay that never says it is listening: the stage fails with that, and
/// the relay is gone. The publisher was never started.
#[cfg(unix)]
#[test]
fn a_relay_that_is_not_ready_is_not_left_behind() {
    let dir = support::work("quinn-guard-relay");
    let (relay, publisher) = (dir.join("relay.pid"), dir.join("publisher.pid"));
    let cast = Cast {
        relay: stand_in(&relay, "starting"),
        publisher: stand_in(&publisher, "announced"),
        ready: SOON,
    };
    let message = failure(|| drop(Stage::assemble(&cast, 0, dir.clone())));
    assert!(
        message.starts_with("the relay did not print \"listening on\":\nstarting"),
        "{message}"
    );
    assert!(gone(&relay));
    assert!(!publisher.exists());
}

/// A publisher that cannot be started: the stage fails with that, and the
/// relay, which was ready, is gone.
#[cfg(unix)]
#[test]
fn a_publisher_that_cannot_start_leaves_no_relay_behind() {
    let dir = support::work("quinn-guard-start");
    let relay = dir.join("relay.pid");
    let cast = Cast {
        relay: stand_in(&relay, "listening on"),
        publisher: words(&["/nonexistent/moq5-publisher"]),
        ready: SOON,
    };
    let message = failure(|| drop(Stage::assemble(&cast, 0, dir.clone())));
    assert!(
        message.starts_with("/nonexistent/moq5-publisher: "),
        "{message}"
    );
    assert!(gone(&relay));
}

/// A publisher that never announces: the stage fails with that, and both
/// are gone.
#[cfg(unix)]
#[test]
fn a_publisher_that_is_not_ready_leaves_nothing_behind() {
    let dir = support::work("quinn-guard-ready");
    let (relay, publisher) = (dir.join("relay.pid"), dir.join("publisher.pid"));
    let cast = Cast {
        relay: stand_in(&relay, "listening on"),
        publisher: stand_in(&publisher, "starting"),
        ready: SOON,
    };
    let message = failure(|| drop(Stage::assemble(&cast, 0, dir.clone())));
    assert!(
        message.starts_with("the publisher did not print \"announced\":\nstarting"),
        "{message}"
    );
    assert!(gone(&relay));
    assert!(gone(&publisher));
}

/// A stage that started is gone when it is dropped.
#[cfg(unix)]
#[test]
fn a_stage_that_is_dropped_leaves_nothing_behind() {
    let dir = support::work("quinn-guard-drop");
    let (relay, publisher) = (dir.join("relay.pid"), dir.join("publisher.pid"));
    let cast = Cast {
        relay: stand_in(&relay, "listening on"),
        publisher: stand_in(&publisher, "announced"),
        ready: SOON,
    };
    let stage = Stage::assemble(&cast, 0, dir.clone());
    assert!(!gone(&relay) && !gone(&publisher));
    drop(stage);
    assert!(gone(&relay) && gone(&publisher));
}

/// A subscriber that does not end is ended at its bound and reported as
/// failed, and is gone.
#[cfg(unix)]
#[test]
fn a_subscriber_that_does_not_end_is_not_left_behind() {
    let dir = support::work("quinn-guard-subscriber");
    let pid = dir.join("subscriber.pid");
    let message = failure(|| {
        let run = support::run(&stand_in(&pid, "connected"), &dir, "sub", SOON);
        succeeded(&run, &dir);
    });
    assert!(
        message.starts_with("the subscriber ended with signal: 9"),
        "{message}"
    );
    assert!(message.contains("\nconnected\n"), "{message}");
    assert!(gone(&pid));
}
