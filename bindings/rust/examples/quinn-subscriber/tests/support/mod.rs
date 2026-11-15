//! What the tests share: child processes that are never left behind, and a
//! certificate for a server on loopback.
//!
//! Each test file uses part of this.
#![allow(dead_code)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// A child process a test owns. It is killed and reaped when this is
/// dropped, whatever ends the test: `std::process::Child` does neither.
/// Dropping never fails, so it cannot replace the failure that caused it.
pub struct Owned(Child);

impl Drop for Owned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Start `command` (a program and its arguments) with its standard output
/// in the file `out` and its standard error in `err`, or in `out` too. The
/// child is owned from the moment it exists.
pub fn spawn(command: &[String], out: &Path, err: Option<&Path>) -> Owned {
    let stdout = File::create(out).expect("the output file is created");
    let stderr = match err {
        Some(err) => File::create(err).expect("the error file is created"),
        None => stdout.try_clone().expect("the output handle is duplicated"),
    };
    let child = Command::new(&command[0])
        .args(&command[1..])
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn();
    match child {
        Ok(child) => Owned(child),
        Err(error) => panic!("{}: {error}", command[0]),
    }
}

pub fn text(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

impl Owned {
    /// Wait for a line in the child's log; fail with the log if the child
    /// ends or `within` passes first.
    pub fn await_line(&mut self, log: &Path, line: &str, who: &str, within: Duration) {
        let end = Instant::now() + within;
        loop {
            if text(log).lines().any(|l| l.contains(line)) {
                return;
            }
            let ended = self.0.try_wait().expect("the child is polled");
            assert!(
                ended.is_none() && Instant::now() < end,
                "{who} did not print {line:?}:\n{}",
                text(log)
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Wait for the child to end, at most `within`; at that bound it is
    /// killed. Its exit status.
    pub fn end(mut self, within: Duration) -> ExitStatus {
        let end = Instant::now() + within;
        loop {
            if let Some(status) = self.0.try_wait().expect("the child is polled") {
                return status;
            }
            if Instant::now() > end {
                let _ = self.0.kill();
                return self.0.wait().expect("the child is reaped");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// How a program run to its end went.
pub struct Run {
    pub status: ExitStatus,
    /// Its standard output, by line.
    pub out: Vec<String>,
    pub err: String,
}

/// Run `command` to its end, at most `within`, with its output in
/// `<dir>/<name>.out` and `<dir>/<name>.err`.
pub fn run(command: &[String], dir: &Path, name: &str, within: Duration) -> Run {
    let out = dir.join(format!("{name}.out"));
    let err = dir.join(format!("{name}.err"));
    let status = spawn(command, &out, Some(&err)).end(within);
    Run {
        status,
        out: text(&out).lines().map(str::to_owned).collect(),
        err: text(&err),
    }
}

/// The subscriber with these arguments, as a command.
pub fn subscriber(args: &[&str]) -> Vec<String> {
    std::iter::once(env!("CARGO_BIN_EXE_moq5-quinn-subscriber"))
        .chain(args.iter().copied())
        .map(str::to_owned)
        .collect()
}

/// An empty directory for a test's files: under `MOQ5_RELAY_WORK` if that
/// is set, or the tests' temporary directory.
pub fn work(name: &str) -> PathBuf {
    let root = std::env::var("MOQ5_RELAY_WORK")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_TARGET_TMPDIR")));
    let dir = root.join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("the work directory is created");
    dir
}

/// Write a key and a self-signed certificate named for `localhost` and
/// `127.0.0.1`. `dir` takes the request configuration.
pub fn certificate(dir: &Path, cert: &Path, key: &Path) {
    let config = dir.join("cert.cnf");
    std::fs::write(
        &config,
        "[req]\ndistinguished_name = dn\nx509_extensions = ext\nprompt = no\n\
         [dn]\nCN = localhost\n\
         [ext]\nsubjectAltName = DNS:localhost, IP:127.0.0.1\n\
         basicConstraints = critical, CA:FALSE\nkeyUsage = critical, digitalSignature\n\
         extendedKeyUsage = serverAuth\n",
    )
    .expect("the certificate configuration is written");
    let openssl = |args: &[&str]| {
        let done = Command::new("openssl")
            .args(args)
            .output()
            .expect("openssl runs");
        assert!(
            done.status.success(),
            "openssl {args:?}: {}",
            String::from_utf8_lossy(&done.stderr)
        );
    };
    let (key, cert, config) = (
        key.to_str().expect("UTF-8"),
        cert.to_str().expect("UTF-8"),
        config.to_str().expect("UTF-8"),
    );
    openssl(&[
        "ecparam",
        "-name",
        "prime256v1",
        "-genkey",
        "-noout",
        "-out",
        key,
    ]);
    openssl(&[
        "req", "-new", "-x509", "-key", key, "-out", cert, "-days", "2", "-config", config,
    ]);
}
