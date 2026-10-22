//! The compile-fail cases: each fixture under `tests/compile_fail/` is
//! type-checked against the crate as it is now, and must be refused with the
//! expected error code and message; one fixture must compile (the positive
//! control of the same path). The cargo that runs this test checks the
//! crate, as the path dependency of a scratch package in a fresh target
//! directory. What is asserted: cargo's artifact record for the crate
//! compiled in this run from this directory; each refused fixture's error
//! and no artifact for it; the positive fixture's artifact and no error for
//! it. Only the code and the phrase of an error are matched, since the rest
//! of a diagnostic (notes, spans, suggestions) changes between compiler
//! releases and the invariant each case pins does not. Not covered: an
//! arbitrary external cargo configuration, and a fixture checked by another
//! compiler than the one cargo runs.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A fixture and what the type checker must say about it: `Some((code,
/// phrase))` for a refusal, `None` for one that must compile.
const CASES: &[(&str, Option<(&str, &str)>)] = &[
    ("buffer_cloned", Some(("E0599", "no method named `clone`"))),
    (
        "buffer_shared_across_threads",
        Some(("E0277", "cannot be shared between threads safely")),
    ),
    (
        "description_outlives_receiver",
        Some(("E0515", "cannot return value referencing local variable")),
    ),
    (
        "object_sent_to_thread",
        Some(("E0277", "cannot be sent between threads safely")),
    ),
    (
        "sender_shared_across_threads",
        Some(("E0277", "cannot be shared between threads safely")),
    ),
    ("buffer_read_in_place", None),
];

/// The scratch package: every fixture a binary of one crate that depends on
/// this crate by path.
fn write_scratch(scratch: &Path, manifest_dir: &Path, fixtures: &Path) {
    let bins = scratch.join("src/bin");
    std::fs::create_dir_all(&bins).expect("the scratch package directories are created");
    std::fs::write(
        scratch.join("Cargo.toml"),
        format!(
            "[package]\nname = \"moq5-compile-fail\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\
             publish = false\n\n[dependencies]\nmoq5 = {{ path = {path:?} }}\n\n[workspace]\n",
            path = manifest_dir.display().to_string()
        ),
    )
    .expect("the scratch manifest is written");
    for (name, _) in CASES {
        std::fs::copy(
            fixtures.join(format!("{name}.rs")),
            bins.join(format!("{name}.rs")),
        )
        .unwrap_or_else(|e| panic!("fixture {name}: {e}"));
    }
}

/// The error lines of one fixture in cargo's short diagnostic format:
/// `src/bin/<name>.rs:<line>:<col>: error[<code>]: <message>`.
fn errors_of<'stderr>(stderr: &'stderr str, name: &str) -> Vec<&'stderr str> {
    let prefix = format!("src/bin/{name}.rs:");
    stderr
        .lines()
        .filter(|line| line.starts_with(&prefix) && line.contains(": error"))
        .collect()
}

/// Cargo's `compiler-artifact` records, one JSON object per line.
fn artifacts(stdout: &str) -> Vec<&str> {
    stdout
        .lines()
        .filter(|line| line.starts_with('{') && line.contains("\"reason\":\"compiler-artifact\""))
        .collect()
}

/// Whether a fixture's binary was built: cargo records an artifact for a
/// binary that compiled, and none for one that did not.
fn bin_built(artifacts: &[&str], name: &str) -> bool {
    let named = format!("\"name\":\"{name}\"");
    artifacts
        .iter()
        .any(|record| record.contains("\"kind\":[\"bin\"]") && record.contains(&named))
}

#[test]
fn fixtures_are_checked_against_the_current_crate() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixtures = manifest_dir.join("tests/compile_fail");
    let scratch = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("compile_fail/scratch");
    let _ = std::fs::remove_dir_all(&scratch);
    write_scratch(&scratch, manifest_dir, &fixtures);

    // The nested build takes this test's build configuration (RUSTFLAGS and
    // the cargo variables) but not the instrumentation injected into this
    // test process: a preloaded sanitizer runtime would load into cargo,
    // rustc and libclang too.
    let output = Command::new(env!("CARGO"))
        .current_dir(&scratch)
        .env_remove("LD_PRELOAD")
        .env_remove("DYLD_INSERT_LIBRARIES")
        .env("CARGO_TARGET_DIR", scratch.join("target"))
        .env("CARGO_TERM_COLOR", "never")
        // Artifact records as JSON on stdout, diagnostics rendered short on
        // stderr.
        .args([
            "check",
            "--bins",
            "--keep-going",
            "--message-format=json-render-diagnostics,json-diagnostic-short",
        ])
        .output()
        .expect("cargo runs the scratch package");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let artifacts = artifacts(&stdout);
    // The crate the fixtures were checked against was compiled in this run
    // from this directory.
    let package_id = format!(
        "\"package_id\":\"path+file://{}#{}\"",
        manifest_dir.display(),
        env!("CARGO_PKG_VERSION")
    );
    assert!(
        artifacts
            .iter()
            .any(|record| record.contains(&package_id) && record.contains("\"fresh\":false")),
        "no artifact of this crate compiled in this run ({package_id}):\n{stdout}\n{stderr}"
    );
    for (name, expected) in CASES {
        let errors = errors_of(&stderr, name);
        let built = bin_built(&artifacts, name);
        match expected {
            None => assert!(
                errors.is_empty() && built,
                "{name} must compile (built: {built}); the type checker said:\n{errors:#?}"
            ),
            Some((code, phrase)) => {
                assert!(
                    !errors.is_empty() && !built,
                    "{name} compiled; it must be refused\n{stderr}"
                );
                assert!(
                    errors.iter().any(|line| line.contains(&format!("error[{code}]"))
                        && line.contains(phrase)),
                    "{name} was refused without {code} {phrase:?}; it was refused with:\n{errors:#?}"
                );
            }
        }
    }
}
