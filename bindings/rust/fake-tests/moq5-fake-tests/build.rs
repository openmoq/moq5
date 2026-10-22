//! Compiles the layout probe, a C translation unit that reports sizeof and
//! offsetof for every record the safe crate reads or writes, against the
//! same headers moq5-sys generated its bindings from (DEP_MOQ_INCLUDE).

use std::env;

fn main() {
    let include = env::var("DEP_MOQ_INCLUDE").expect("moq5-sys exports its include directories");
    let mut build = cc::Build::new();
    for dir in include.split(';').filter(|d| !d.is_empty()) {
        build.include(dir);
    }
    // The player's records, with the `player` feature (an SDK with it).
    if env::var_os("CARGO_FEATURE_PLAYER").is_some() {
        build.define("MOQ5_LAYOUT_PLAYER", None);
    }
    build
        .file("../layout_probe.c")
        .warnings(true)
        .warnings_into_errors(true)
        .compile("moq5_layout_probe");
    println!("cargo:rerun-if-changed=../layout_probe.c");
    // The provider archive run.sh rebuilds is linked, not compiled, so cargo
    // learns about a new one here.
    println!("cargo:rerun-if-env-changed=MOQ5_FAKE_ARCHIVE");
    if let Ok(archive) = env::var("MOQ5_FAKE_ARCHIVE") {
        println!("cargo:rerun-if-changed={archive}");
    }
}
