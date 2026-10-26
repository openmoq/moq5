//! The live rows' verdict on their native peer (`live::terminate`,
//! `live::peer_verdict`), checked with shell children: deterministic, no
//! relay. A child that installs its SIGTERM handler reports `ready` first,
//! so the signal never races the handler.

mod live;

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

const GRACE: Duration = Duration::from_secs(5);

/// A child that exits `code` on SIGTERM, once it reports its handler set.
fn exits_on_term(code: u8) -> Child {
    let mut child = Command::new("sh")
        .args([
            "-c",
            &format!("trap 'exit {code}' TERM; echo ready; while :; do sleep 0.05; done"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()
        .expect("sh runs");
    let mut line = String::new();
    BufReader::new(child.stdout.as_mut().expect("the child's stdout is piped"))
        .read_line(&mut line)
        .expect("the child reports");
    assert_eq!(line, "ready\n");
    child
}

/// A child that has already exited with `code` when it is stopped.
fn exited(code: u8) -> Child {
    let mut child = Command::new("sh")
        .args(["-c", &format!("exit {code}")])
        .stdin(Stdio::null())
        .spawn()
        .expect("sh runs");
    child.wait().expect("the child exits");
    child
}

#[test]
fn a_clean_exit_on_sigterm_passes() {
    let mut child = exits_on_term(0);
    let ending = live::terminate(&mut child, "the child", GRACE);
    assert!(!ending.before_stop);
    live::peer_verdict(&ending).expect("a clean exit passes");
}

#[test]
fn a_failing_exit_on_sigterm_fails() {
    let mut child = exits_on_term(3);
    let ending = live::terminate(&mut child, "the child", GRACE);
    let why = live::peer_verdict(&ending).expect_err("exit 3 fails");
    assert!(
        why.contains("exited with exit status: 3 on SIGTERM"),
        "{why}"
    );
}

#[test]
fn a_sanitizer_report_fails() {
    let mut child = exits_on_term(66);
    let ending = live::terminate(&mut child, "the child", GRACE);
    let why = live::peer_verdict(&ending).expect_err("exit 66 fails");
    assert!(why.contains("exit status: 66"), "{why}");
}

#[test]
fn a_failing_exit_before_the_stop_fails() {
    let mut child = exited(3);
    let ending = live::terminate(&mut child, "the child", GRACE);
    assert!(ending.before_stop);
    let why = live::peer_verdict(&ending).expect_err("exit 3 fails");
    assert!(
        why.contains("exited with exit status: 3 before the row stopped it"),
        "{why}"
    );
}

#[test]
fn a_clean_exit_before_the_stop_passes() {
    let mut child = exited(0);
    let ending = live::terminate(&mut child, "the child", GRACE);
    assert!(ending.before_stop);
    live::peer_verdict(&ending).expect("a clean exit passes");
}

#[test]
fn a_death_by_signal_fails() {
    let mut child = Command::new("sh")
        .args(["-c", "kill -KILL $$"])
        .stdin(Stdio::null())
        .spawn()
        .expect("sh runs");
    child.wait().expect("the child ends");
    let ending = live::terminate(&mut child, "the child", GRACE);
    let why = live::peer_verdict(&ending).expect_err("a signal death fails");
    assert!(why.contains("the done peer exited with signal"), "{why}");
}
