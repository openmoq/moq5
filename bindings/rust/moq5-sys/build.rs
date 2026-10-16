//! Generate bindings for, and statically link, the installed libmoq.
//!
//! libmoq is found under `MOQ_PREFIX` (an install prefix holding `include/moq`
//! and `lib/libmoq-*.a`). Unset, it defaults to `../local-prefix` next to the
//! moq5 checkout -- where `cmake --preset install` puts it -- so a plain
//! `cargo build` works after that preset's install.

use std::env;
use std::path::PathBuf;

/// Static archives in dependency order: each one only references archives
/// listed after it, so a single-pass linker resolves everything.
const LIBS: &[&str] = &[
    "moq-service",
    "moq-media-object",
    "moq-loc",
    "moq-cmaf",
    "moq-msf",
    "moq-core",
];

const HEADERS: &[&str] = &[
    "moq/types.h",
    "moq/rcbuf.h",
    "moq/version.h",
    "moq/endpoint.h",
    "moq/cmaf.h",
    "moq/msf.h",
    "moq/media_object.h",
    "moq/media_sender.h",
    "moq/media_receiver.h",
];

fn main() {
    println!("cargo:rerun-if-env-changed=MOQ_PREFIX");

    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let prefix = env::var_os("MOQ_PREFIX")
        .map(PathBuf::from)
        // bindings/rust/moq5-sys -> the checkout root -> its sibling.
        .unwrap_or_else(|| manifest.join("../../../../local-prefix"));
    let prefix = prefix.canonicalize().unwrap_or_else(|_| {
        panic!(
            "libmoq prefix {} not found; set MOQ_PREFIX to the moq5 install prefix",
            prefix.display()
        )
    });
    let include = prefix.join("include");
    let lib = prefix.join("lib");

    println!("cargo:rustc-link-search=native={}", lib.display());
    for l in LIBS {
        println!("cargo:rustc-link-lib=static={l}");
        println!(
            "cargo:rerun-if-changed={}",
            lib.join(format!("lib{l}.a")).display()
        );
    }
    // A libmoq built against picoquic/picotls/OpenSSL needs those too; the
    // service archives in the default prefix are self-contained.
    println!("cargo:rerun-if-env-changed=MOQ_EXTRA_LIBS");
    if let Ok(extra) = env::var("MOQ_EXTRA_LIBS") {
        for l in extra.split_whitespace() {
            println!("cargo:rustc-link-lib={l}");
        }
    }
    println!("cargo:include={}", include.display());

    let mut builder = bindgen::Builder::default().clang_arg(format!("-I{}", include.display()));
    // A distro libclang often ships without its resource headers (stddef.h,
    // stdbool.h). The C compiler's builtin include dir carries the same set.
    if let Some(dir) = cc_builtin_include() {
        builder = builder.clang_arg(format!("-isystem{}", dir.display()));
    }
    let mut builder = builder
        .allowlist_function("moq_.*")
        .allowlist_type("moq_.*")
        .allowlist_var("MOQ_.*")
        // Plain integer constants keep the C semantics: tests and mocks can
        // store any value a C caller could, and `match` works on them.
        .default_enum_style(bindgen::EnumVariation::Consts)
        .prepend_enum_name(false)
        .parse_callbacks(Box::new(ResultCodes))
        .derive_default(true)
        .layout_tests(false)
        .generate_comments(false);
    let mut wrapper = String::new();
    for h in HEADERS {
        wrapper.push_str(&format!("#include <{h}>\n"));
        println!("cargo:rerun-if-changed={}", include.join(h).display());
    }
    builder = builder.header_contents("moq_wrapper.h", &wrapper);

    let bindings = builder
        .generate()
        .expect("bindgen failed on the libmoq headers");
    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    bindings
        .write_to_file(out)
        .expect("could not write bindings");
}

/// `MOQ_OK`/`MOQ_DONE` are positive `#define`s, which bindgen would type as
/// `u32`; every result code is a `moq_result_t` (`int`) in C.
#[derive(Debug)]
struct ResultCodes;

impl bindgen::callbacks::ParseCallbacks for ResultCodes {
    fn int_macro(&self, name: &str, _value: i64) -> Option<bindgen::callbacks::IntKind> {
        (name == "MOQ_OK" || name == "MOQ_DONE" || name.starts_with("MOQ_ERR_"))
            .then_some(bindgen::callbacks::IntKind::Int)
    }
}

fn cc_builtin_include() -> Option<PathBuf> {
    let cc = env::var("CC").unwrap_or_else(|_| "cc".into());
    let out = std::process::Command::new(cc)
        .arg("-print-file-name=include")
        .output()
        .ok()?;
    let dir = PathBuf::from(String::from_utf8(out.stdout).ok()?.trim());
    dir.join("stddef.h").exists().then_some(dir)
}
