//! Harness for the relay rows: one `moq5-relay` per row on loopback, with
//! the test-only credentials the native service rows use, and the SDK's
//! picoquic backend on both ends. The done-status row also drives a native
//! publisher peer (`tests/support/done_peer.c`, built by the relay row).
//!
//! Environment (the rows are `#[ignore]`d and run with `--ignored`):
//! - `MOQ5_RELAY`: the relay binary (required; the row fails without it).
//! - `MOQ5_DONE_PEER`: the native peer binary (the done-status row).
//! - `MOQ5_RELAY_CERT` / `MOQ5_RELAY_KEY`: the relay's PEM credentials;
//!   default `adapters/msquic/tests/test_only_loopback_{cert,key}.pem`.
//! - `MOQ5_RELAY_DRAFT`: the draft both ends offer (default 22).
//! - `MOQ5_RELAY_WORK`: where the relay's configuration and log go
//!   (default: a directory under the system temp dir).

#![allow(dead_code)]

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use moq5::{
    Endpoint, EndpointConfig, Error, Receiver, Sender, Track, TrackEventKind, Version,
    VersionOffer, WaitResult,
};

pub const STEP: Duration = Duration::from_millis(100);
pub const DEADLINE: Duration = Duration::from_secs(20);

pub fn draft() -> Version {
    match std::env::var("MOQ5_RELAY_DRAFT") {
        Ok(v) => Version::from_draft(v.parse().expect("MOQ5_RELAY_DRAFT is a draft number")),
        Err(_) => Version::DRAFT_22,
    }
}

fn repo_path(rel: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .join(rel)
        .canonicalize()
        .unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// The row's work directory: the relay's (and the peer's) configuration and
/// logs, one directory per row and process.
fn work_dir(row: &str) -> PathBuf {
    let work = std::env::var("MOQ5_RELAY_WORK")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("moq5-live"))
        .join(format!("{row}-{}", std::process::id()));
    std::fs::create_dir_all(&work).unwrap_or_else(|e| panic!("{}: {e}", work.display()));
    work
}

fn file_text(path: &Path) -> String {
    let mut text = String::new();
    if let Ok(mut file) = std::fs::File::open(path) {
        let _ = file.read_to_string(&mut text);
    }
    text
}

/// Wait until a process's log contains `line`, or fail with the log when
/// the process exits or the deadline passes.
fn await_log_line(child: &mut Child, log: &Path, line: &str, what: &str) {
    let started = Instant::now();
    loop {
        if file_text(log).contains(line) {
            return;
        }
        if let Some(status) = child.try_wait().expect("the child can be polled") {
            panic!(
                "{what} exited ({status}) before `{line}`:\n{}",
                file_text(log)
            );
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{what} did not log `{line}` within 10 s:\n{}",
            file_text(log)
        );
        std::thread::sleep(STEP);
    }
}

/// How a stopped child ended: its exit status, and whether it had exited
/// before the stop.
pub struct Ending {
    pub status: ExitStatus,
    pub before_stop: bool,
}

/// Stop a child and return how it ended: the status it already exited with,
/// or SIGTERM and the status it then exits with. A child still running
/// `grace` after SIGTERM is killed and fails the row.
pub fn terminate(child: &mut Child, what: &str, grace: Duration) -> Ending {
    if let Some(status) = child.try_wait().expect("the child can be polled") {
        return Ending {
            status,
            before_stop: true,
        };
    }
    let _ = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status();
    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("the child can be polled") {
            return Ending {
                status,
                before_stop: false,
            };
        }
        if started.elapsed() > grace {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{what} did not stop on SIGTERM within {grace:?}");
        }
        std::thread::sleep(STEP);
    }
}

/// The done peer's exit contract (`tests/support/done_peer.c`): 0, whether
/// the row stopped it or it had already exited. Anything else fails: 1 is a
/// fatal session, 2 a timeout, a death by signal is a crash, and a sanitizer
/// that reported exits with its own code (66, as the instrumented row
/// states it).
pub fn peer_verdict(ending: &Ending) -> Result<(), String> {
    if ending.status.success() {
        return Ok(());
    }
    let when = if ending.before_stop {
        "before the row stopped it"
    } else {
        "on SIGTERM"
    };
    Err(format!(
        "the done peer exited with {} {when}",
        ending.status
    ))
}

pub struct Relay {
    child: Child,
    pub port: u16,
    pub cert: PathBuf,
    pub log: PathBuf,
    pub work: PathBuf,
}

impl Relay {
    /// Start a relay for `row` on `port` (admin on `port + 1`) and wait for
    /// its listening line.
    pub fn start(row: &str, port: u16) -> Relay {
        let binary = std::env::var("MOQ5_RELAY")
            .expect("MOQ5_RELAY names the moq5-relay binary for the relay rows");
        let cert = std::env::var("MOQ5_RELAY_CERT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| repo_path("adapters/msquic/tests/test_only_loopback_cert.pem"));
        let key = std::env::var("MOQ5_RELAY_KEY")
            .map(PathBuf::from)
            .unwrap_or_else(|_| repo_path("adapters/msquic/tests/test_only_loopback_key.pem"));
        let work = work_dir(row);
        let config = work.join("relay.json");
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
                draft = draft().draft(),
                cert = cert.display(),
                key = key.display(),
                admin = port + 1,
            ),
        )
        .expect("the relay configuration is written");
        let log = work.join("relay.log");
        let out = std::fs::File::create(&log).expect("the relay log is created");
        let err = out.try_clone().expect("the relay log handle is duplicated");
        let child = Command::new(&binary)
            .args(["serve", "--config"])
            .arg(&config)
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .spawn()
            .unwrap_or_else(|e| panic!("{binary}: {e}"));
        let mut relay = Relay {
            child,
            port,
            cert,
            log,
            work,
        };
        await_log_line(&mut relay.child, &relay.log, "listening on", "the relay");
        relay
    }

    pub fn log_text(&self) -> String {
        file_text(&self.log)
    }

    /// A client configuration for this relay: the picoquic backend, the
    /// test-only certificate as the trust root, its subject name as the SNI.
    pub fn config(&self) -> EndpointConfig {
        EndpointConfig::new(format!("moqt://127.0.0.1:{}", self.port))
            .sni("localhost")
            .ca_file(self.cert.to_str().expect("the certificate path is UTF-8"))
            .versions(VersionOffer::Exact(draft()))
    }

    /// Stop the relay: SIGTERM, then SIGKILL after 5 s. Its exit status is
    /// not part of the rows' contract: the rows judge what the clients
    /// observed, and the relay's handling of SIGTERM is its own.
    pub fn stop(&mut self) {
        let _ = terminate(&mut self.child, "the relay", Duration::from_secs(5));
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        if self
            .child
            .try_wait()
            .expect("the child can be polled")
            .is_none()
        {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// The native publisher peer: it announces a namespace to the relay,
/// serves the catalog and one raw video track, writes `objects` objects and
/// completes the track's subscription with `status`, the exact wire code
/// for the draft (its stdout log records each step).
pub struct Peer {
    child: Child,
    pub log: PathBuf,
}

impl Peer {
    pub fn start(relay: &Relay, namespace: &str, status: u64, objects: u32) -> Peer {
        let binary = std::env::var("MOQ5_DONE_PEER")
            .expect("MOQ5_DONE_PEER names the native done peer for the done-status row");
        let log = relay.work.join("peer.log");
        let out = std::fs::File::create(&log).expect("the peer log is created");
        let err = out.try_clone().expect("the peer log handle is duplicated");
        let child = Command::new(&binary)
            .args([
                "--port",
                &relay.port.to_string(),
                "--draft",
                &draft().draft().to_string(),
                "--namespace",
                namespace,
                "--status",
                &status.to_string(),
                "--objects",
                &objects.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .spawn()
            .unwrap_or_else(|e| panic!("{binary}: {e}"));
        let mut peer = Peer { child, log };
        await_log_line(&mut peer.child, &peer.log, "announced", "the peer");
        peer
    }

    /// Wait for one of the peer's step lines (`subscribed`, `wrote`,
    /// `done`).
    pub fn await_line(&mut self, line: &str) {
        await_log_line(&mut self.child, &self.log, line, "the peer");
    }

    pub fn log_text(&self) -> String {
        file_text(&self.log)
    }

    /// Stop the peer and hold it to its exit contract (`peer_verdict`):
    /// a peer that failed after delivering what the row observed fails the
    /// row, with its log (where a sanitizer report would be).
    pub fn stop(&mut self) {
        let ending = terminate(&mut self.child, "the peer", Duration::from_secs(5));
        if let Err(why) = peer_verdict(&ending) {
            panic!("{why}:\n{}", self.log_text());
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if self
            .child
            .try_wait()
            .expect("the child can be polled")
            .is_none()
        {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Connect and wait for establishment.
pub fn established(config: &EndpointConfig) -> Endpoint {
    let endpoint = Endpoint::connect(config).expect("the endpoint connects");
    let started = Instant::now();
    while endpoint.state() != moq5::EndpointState::Established {
        match endpoint.wait(STEP) {
            Ok(_) => {}
            Err(error) => panic!("endpoint wait: {error}; terminal {:?}", endpoint.terminal()),
        }
        assert!(
            started.elapsed() < DEADLINE,
            "not established: {:?}",
            endpoint.terminal()
        );
    }
    endpoint
}

/// Wait for the sender's write level (readiness with headroom).
pub fn ready(sender: &Sender) {
    let started = Instant::now();
    while !sender.is_ready() {
        match sender.wait(STEP) {
            Ok(WaitResult::Ready) | Ok(WaitResult::TimedOut) => {}
            Err(error) => panic!(
                "sender wait: {error}; fatal {} code {}",
                sender.is_fatal(),
                sender.fatal_code()
            ),
        }
        assert!(started.elapsed() < DEADLINE, "sender not ready");
    }
}

/// One track event, kept past its poll: the kind, the track, and for an
/// ended track the done status it carried.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Seen {
    pub kind: TrackEventKind,
    pub track: Option<Track>,
    pub done_status: Option<u64>,
}

/// Drain track events until `want` returns true for one, which is
/// returned; every event is appended to `seen`.
pub fn until_event(
    receiver: &Receiver,
    seen: &mut Vec<Seen>,
    want: impl Fn(TrackEventKind) -> bool,
) -> Seen {
    let started = Instant::now();
    loop {
        while let Some(event) = receiver
            .poll_track()
            .expect("poll_track on a live receiver")
        {
            let record = Seen {
                kind: event.kind(),
                track: event.track(),
                done_status: event.done_status(),
            };
            seen.push(record);
            if want(record.kind) {
                return record;
            }
        }
        match receiver.wait(STEP) {
            Ok(_) => {}
            Err(Error::CLOSED) => panic!(
                "receiver closed: fatal {} code {}",
                receiver.is_fatal(),
                receiver.fatal_code()
            ),
            Err(error) => panic!("receiver wait: {error}"),
        }
        assert!(started.elapsed() < DEADLINE, "event not seen; saw {seen:?}");
    }
}

/// Drain track events until a track is announced (an `Added` event), and
/// return it. The catalog's readiness and the announcement are separate
/// events whose order is not part of the contract.
pub fn announced_track(receiver: &Receiver, seen: &mut Vec<Seen>) -> Track {
    let added = until_event(receiver, seen, |k| k == TrackEventKind::Added);
    added.track.expect("the Added event names its track")
}

/// Take every track event already queued, appending to `seen`, so a count
/// over `seen` covers the whole queue as it stood: an event queued behind
/// the one `until_event` stopped at is not left unread.
pub fn drain_queued(receiver: &Receiver, seen: &mut Vec<Seen>) {
    while let Some(event) = receiver
        .poll_track()
        .expect("poll_track on a live receiver")
    {
        seen.push(Seen {
            kind: event.kind(),
            track: event.track(),
            done_status: event.done_status(),
        });
    }
}

/// The Ended events of one track among the events seen.
pub fn ended_count(seen: &[Seen], track: Track) -> usize {
    seen.iter()
        .filter(|event| event.kind == TrackEventKind::Ended && event.track == Some(track))
        .count()
}

/// Threads of this process.
pub fn thread_count() -> usize {
    if cfg!(target_os = "linux") {
        return std::fs::read_dir("/proc/self/task")
            .expect("/proc/self/task lists the threads")
            .count();
    }
    let out = Command::new("ps")
        .args(["-M", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps lists the threads");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .count()
        .saturating_sub(1)
}
