//! Link the installed libmoq service tier and generate its bindings.
//!
//! The SDK is found through pkg-config on `libmoq-service`, whose `Requires`
//! closure carries every library the service was built with (core, the media
//! helpers, and the managed providers the SDK expresses through pkg-config).
//! Nothing is reconstructed here: an SDK whose service component has no
//! pkg-config file is reported, not guessed at.
//!
//! Environment, host builds:
//! - `MOQ_PREFIX`: a convenience that puts `<prefix>/lib/pkgconfig` (and
//!   `lib64/pkgconfig`, `share/pkgconfig` when present) in front of the
//!   search variable the probe reads (`PKG_CONFIG_PATH`, or the host-specific
//!   spelling of it when one is set).
//! - `LIBMOQ_SERVICE_STATIC` / `LIBMOQ_SERVICE_DYNAMIC` and the
//!   `PKG_CONFIG_ALL_*` forms: the `pkg-config` crate's own static/shared
//!   selection for this probe. Unset, the installed libraries decide.
//! - `BINDGEN_EXTRA_CLANG_ARGS`: extra arguments for libclang, for instance
//!   the include directory of a libclang installed without its resource
//!   headers. The host C compiler's headers are never substituted for them.
//! - `DOCS_RS`: no SDK is probed; the crate compiles the committed snapshot.
//!
//! Cross builds (`--target` differs from the host) are resolved once, before
//! anything is probed, into one target search configuration and one sysroot.
//! `<target>` below is the target triple, or the triple with `-` as `_`.
//! - the target SDK: `MOQ_PREFIX_<target>` (its pkg-config directories), and
//!   the `pkg-config` crate's `PKG_CONFIG_PATH_<target>` and
//!   `PKG_CONFIG_LIBDIR_<target>`. Only these spellings name the target. Their
//!   directories, in that order, become the whole search configuration: they
//!   are handed to pkg-config as its library directory list, which replaces
//!   its default directories, and every other spelling of the path and
//!   libdir variables (`TARGET_`, `HOST_`, plain) is removed from the probe's
//!   environment, so an inherited one cannot widen the search. A sysroot on
//!   its own names no search directory.
//! - the target sysroot libclang parses the headers against, from every
//!   input the build will use: `MOQ_SYSROOT_<target>`; the sysroot pkg-config
//!   applies (`PKG_CONFIG_SYSROOT_DIR`, read as the `pkg-config` crate reads
//!   it: `_<target>`, `TARGET_`, plain); and every `--sysroot`/`-isysroot`
//!   operand among the clang arguments bindgen passes
//!   (`BINDGEN_EXTRA_CLANG_ARGS`, read as bindgen reads it: `_<target>`,
//!   plain; `build_sysroot.rs`). At least one is required, and all of them
//!   must name the same directory.
//!
//! Both are required; the validated pair is what permits the probe.
//! `PKG_CONFIG_ALLOW_CROSS` on its own is not support and does not admit a
//! host SDK.
//!
//! Beside the bindings, the leading comment of every bound header is written
//! to `preambles.md` and compiled into the crate documentation: the contracts
//! a header states for all of its declarations at once (rcbuf's shard and
//! overflow rules) are not attached to any one item by clang.
//!
//! The `player` feature binds the media player (`media_player.h`, with the
//! playback engine configuration it takes from `playback.h`), which an SDK
//! carries only when it was built with `MOQ_BUILD_PLAYBACK`. Its declarations
//! are generated into a file of their own, `bindings_player.rs`, with their
//! preambles in `preambles_player.md`, so the base bindings are the same with
//! the feature and without it. With the feature, an SDK without the player
//! headers fails the build; without it, they are never read.

use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[path = "build_sysroot.rs"]
mod sysroot;

const PROBE: &str = "libmoq-service";

/// The headers the safe crate wraps, in dependency order.
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

/// The headers the `player` feature binds, in dependency order.
const PLAYER_HEADERS: &[&str] = &["moq/playback.h", "moq/media_player.h"];

/// The pkg-config search configuration of a cross build, kept for the
/// diagnostics.
struct Cross {
    search: Vec<PathBuf>,
    /// The sysroot to hand to libclang, unless the extra arguments carry it.
    sysroot_arg: Option<PathBuf>,
}

fn main() {
    println!("cargo:rustc-check-cfg=cfg(docsrs)");
    println!("cargo:rerun-if-env-changed=MOQ_PREFIX");
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    println!("cargo:rerun-if-env-changed=RUSTFMT");
    if env::var_os("DOCS_RS").is_some() {
        println!("cargo:rustc-cfg=docsrs");
        return;
    }

    let host = env::var("HOST").unwrap_or_default();
    let target = env::var("TARGET").unwrap_or_default();
    let cross = if host != target {
        Some(configure_cross(&host, &target))
    } else {
        apply_moq_prefix(&host);
        None
    };
    let lib = match pkg_config::Config::new().probe(PROBE) {
        Ok(lib) => lib,
        Err(e) => panic!("{}", diagnose(&e, cross.as_ref())),
    };
    // pkg-config emitted the link-search and link-lib lines for the whole
    // closure. Dependents get the include directories through DEP_MOQ_INCLUDE.
    let include: Vec<String> = lib
        .include_paths
        .iter()
        .map(|p| p.display().to_string())
        .collect();
    println!("cargo:include={}", include.join(";"));
    // A static library is bundled into this crate's rlib when the crate is
    // compiled, so cargo is told to watch every archive the link names: an
    // SDK rebuilt in place then rebuilds the crate instead of linking the
    // copy bundled earlier.
    for archive in lib
        .link_files
        .iter()
        .cloned()
        .chain(lib.link_paths.iter().flat_map(|dir| {
            lib.libs
                .iter()
                .map(move |name| dir.join(format!("lib{name}.a")))
        }))
    {
        if archive.is_file() {
            println!("cargo:rerun-if-changed={}", archive.display());
        }
    }

    let mut clang_args: Vec<String> = lib
        .include_paths
        .iter()
        .map(|dir| format!("-I{}", dir.display()))
        .collect();
    if let Some(cross) = &cross {
        // The target SDK's headers come from its pkg-config file; the
        // sysroot from the resolved input, unless BINDGEN_EXTRA_CLANG_ARGS
        // for the target carries it (bindgen appends those itself). Nothing
        // of the host is added.
        clang_args.push(format!("--target={target}"));
        if let Some(sysroot) = &cross.sysroot_arg {
            clang_args.push(format!("--sysroot={}", sysroot.display()));
        }
    }
    // libclang's own resource headers (stddef.h, stdbool.h) are found by
    // clang-sys; an installation without them is given its include
    // directory through BINDGEN_EXTRA_CLANG_ARGS. The host C compiler's
    // headers are never substituted: a GCC stddef.h does not parse under
    // clang.
    let player = env::var_os("CARGO_FEATURE_PLAYER").is_some();
    let mut required = HEADERS.to_vec();
    if player {
        required.extend_from_slice(PLAYER_HEADERS);
    }
    for h in required {
        if !lib.include_paths.iter().any(|d| d.join(h).exists()) {
            let remedy = if PLAYER_HEADERS.contains(&h) {
                "the `player` feature needs an SDK built with MOQ_BUILD_PLAYBACK=ON"
            } else {
                "the SDK is incomplete or older than this crate requires"
            };
            panic!(
                "moq5-sys: `{h}` is not in the include directories pkg-config reported \
                 ({}); {remedy}",
                include.join(", ")
            );
        }
    }
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    let bindings = generator(&clang_args)
        .header("wrapper.h")
        .allowlist_function("moq_.*")
        .allowlist_type("moq_.*")
        .allowlist_var("MOQ_.*")
        .allowlist_var("MOQ5_SYSVAL_.*")
        .blocklist_file(".*/moq/control\\.h")
        .generate()
        .expect("bindgen failed on the libmoq headers");
    write_bindings(&bindings, &out_dir.join("bindings.rs"));
    write_preambles(
        &lib.include_paths,
        HEADERS,
        Some(PREAMBLES_INTRO),
        &out_dir.join("preambles.md"),
    );
    if player {
        // The player's own declarations and the playback configuration they
        // take. Every other libmoq header is blocklisted: what the player
        // references from them is in the base bindings, which the crate
        // includes beside these, and bindgen still sees those types, so the
        // derives it can prove for them hold here too.
        let bindings = generator(&clang_args)
            .header("wrapper_player.h")
            .blocklist_file(base_headers_pattern(&lib.include_paths))
            .parse_callbacks(Box::new(BaseDerives))
            .allowlist_file(".*/moq/media_player\\.h")
            .allowlist_type("moq_playback_cfg(_t)?")
            .allowlist_function("moq_playback_cfg_init_sized")
            .allowlist_var("MOQ_PLAYBACK_.*")
            .allowlist_var("MOQ5_SYSVAL_.*")
            .generate()
            .expect("bindgen failed on the libmoq player headers");
        write_bindings(&bindings, &out_dir.join("bindings_player.rs"));
        write_preambles(
            &lib.include_paths,
            PLAYER_HEADERS,
            None,
            &out_dir.join("preambles_player.md"),
        );
    }
}

/// A bindgen run over the SDK's headers with the settings every generation
/// shares; the caller adds the header and what to allowlist.
fn generator(clang_args: &[String]) -> bindgen::Builder {
    bindgen::Builder::default()
        .clang_args(clang_args)
        // The headers document their contracts in ordinary comments, which
        // clang attaches to declarations only when asked to parse them all.
        .clang_arg("-fparse-all-comments")
        // Plain integer constants keep the C semantics: an open set stays a
        // fixed-width integer, and `match` works on the values.
        .default_enum_style(bindgen::EnumVariation::Consts)
        .prepend_enum_name(false)
        .must_use_type("moq_result_t")
        .derive_default(true)
        .layout_tests(true)
        .generate_comments(true)
        // Formatted by rustfmt (RUSTFMT, else the one on PATH): the committed
        // snapshot is held byte-equal to a fresh generation on every qualified
        // toolchain, so a formatter release that changes the layout shows up
        // there as a regeneration, never silently. A missing rustfmt is not an
        // error for a consumer (bindgen writes the bindings unformatted).
        .formatter(bindgen::Formatter::Rustfmt)
        .parse_callbacks(Box::new(ResultCodes))
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
}

/// A pattern for every installed libmoq header except the player's, as the
/// SDK's include directories hold them.
fn base_headers_pattern(include_dirs: &[PathBuf]) -> String {
    let mut names: Vec<String> = include_dirs
        .iter()
        .filter_map(|dir| std::fs::read_dir(dir.join("moq")).ok())
        .flatten()
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| name.ends_with(".h"))
        .filter(|name| !PLAYER_HEADERS.contains(&format!("moq/{name}").as_str()))
        .map(|name| name.trim_end_matches(".h").replace('.', "\\."))
        .collect();
    names.sort();
    names.dedup();
    format!(".*/moq/({})\\.h", names.join("|"))
}

fn write_bindings(bindings: &bindgen::Bindings, out: &Path) {
    bindings
        .write_to_file(out)
        .unwrap_or_else(|e| panic!("could not write {}: {e}", out.display()));
}

/// `MOQ_PREFIX` names an install prefix; its pkg-config directories go
/// first on the search path the probe will read. The `pkg-config` crate
/// reads the first set of `PKG_CONFIG_PATH_<host>`, `PKG_CONFIG_PATH_<host
/// with _>`, `HOST_PKG_CONFIG_PATH` and `PKG_CONFIG_PATH`, so the prefix is
/// put in front of whichever of those is set, never behind a stray
/// host-specific variable.
fn apply_moq_prefix(host: &str) {
    let Some(prefix) = env::var_os("MOQ_PREFIX") else {
        return;
    };
    let mut dirs = prefix_pkgconfig_dirs("MOQ_PREFIX", &PathBuf::from(prefix));
    let names = [
        format!("PKG_CONFIG_PATH_{host}"),
        format!("PKG_CONFIG_PATH_{}", host.replace('-', "_")),
        "HOST_PKG_CONFIG_PATH".to_owned(),
        "PKG_CONFIG_PATH".to_owned(),
    ];
    for name in &names {
        println!("cargo:rerun-if-env-changed={name}");
    }
    let (name, existing) = names
        .iter()
        .find_map(|name| env::var_os(name).map(|value| (name.clone(), Some(value))))
        .unwrap_or_else(|| ("PKG_CONFIG_PATH".to_owned(), None));
    if let Some(existing) = existing {
        dirs.extend(env::split_paths(&existing));
    }
    let joined = env::join_paths(dirs).expect("PKG_CONFIG_PATH entries are plain paths");
    // The probe runs in this process; dependents never see this variable.
    env::set_var(name, joined);
}

/// The pkg-config directories of an install prefix, or a refusal naming the
/// variable that pointed at something else.
fn prefix_pkgconfig_dirs(var: &str, prefix: &Path) -> Vec<PathBuf> {
    let dirs: Vec<PathBuf> = ["lib/pkgconfig", "lib64/pkgconfig", "share/pkgconfig"]
        .iter()
        .map(|d| prefix.join(d))
        .filter(|d| d.is_dir())
        .collect();
    if dirs.is_empty() {
        panic!(
            "moq5-sys: {var}={} has no lib/pkgconfig, lib64/pkgconfig or \
             share/pkgconfig directory; it is not a libmoq install prefix",
            prefix.display()
        );
    }
    dirs
}

/// The spellings of a variable that name one target: `<var>_<target>` and
/// `<var>_<target with _>`.
fn triple_names(var: &str, target: &str) -> Vec<String> {
    vec![
        format!("{var}_{target}"),
        format!("{var}_{}", target.replace('-', "_")),
    ]
}

/// The first of `names` that is set, with its name: the value a reader with
/// that precedence takes.
fn first_set(names: &[String]) -> Option<(String, OsString)> {
    for name in names {
        println!("cargo:rerun-if-env-changed={name}");
    }
    names
        .iter()
        .find_map(|name| env::var_os(name).map(|value| (name.clone(), value)))
}

/// Validate a cross build's inputs and arrange the probe for them: the
/// target SDK's directories become the only pkg-config search
/// configuration, and the sysroots named must agree.
fn configure_cross(host: &str, target: &str) -> Cross {
    // The search configuration: the prefix's directories, then the
    // pkg-config crate's path and libdir variables for this target.
    let mut search: Vec<PathBuf> = Vec::new();
    if let Some((var, prefix)) = first_set(&triple_names("MOQ_PREFIX", target)) {
        search.extend(prefix_pkgconfig_dirs(&var, &PathBuf::from(prefix)));
    }
    for var in ["PKG_CONFIG_PATH", "PKG_CONFIG_LIBDIR"] {
        if let Some((_, value)) = first_set(&triple_names(var, target)) {
            search.extend(env::split_paths(&value).filter(|d| !d.as_os_str().is_empty()));
        }
    }
    let mut unique: Vec<PathBuf> = Vec::new();
    for dir in search {
        if !unique.contains(&dir) {
            unique.push(dir);
        }
    }
    let search = unique;
    if search.is_empty() {
        panic!(
            "moq5-sys: cross build for {target} on {host}: no target SDK is named. Set \
             MOQ_PREFIX_{target} to the target's libmoq install prefix (or the pkg-config \
             crate's PKG_CONFIG_PATH_{target} or PKG_CONFIG_LIBDIR_{target}). The host's \
             MOQ_PREFIX and PKG_CONFIG_PATH are not used for a foreign target, a sysroot \
             alone names no search directory, and PKG_CONFIG_ALLOW_CROSS alone is not support."
        );
    }
    for dir in &search {
        if !dir.is_dir() {
            panic!(
                "moq5-sys: cross build for {target}: the search directory {} does not exist",
                dir.display()
            );
        }
    }
    // Exactly this configuration is consulted: it becomes the target's
    // library directory list (which replaces pkg-config's default
    // directories), and every spelling the crate could read of a path or
    // libdir variable, for this target, the generic target or the host, is
    // removed from the probe's environment.
    let libdir_var = format!("PKG_CONFIG_LIBDIR_{target}");
    for var in ["PKG_CONFIG_PATH", "PKG_CONFIG_LIBDIR"] {
        let mut names = triple_names(var, target);
        names.extend([
            format!("TARGET_{var}"),
            format!("HOST_{var}"),
            var.to_owned(),
        ]);
        for name in names {
            println!("cargo:rerun-if-env-changed={name}");
            env::remove_var(name);
        }
    }
    env::set_var(
        &libdir_var,
        env::join_paths(&search).expect("search directories are plain paths"),
    );

    // The sysroot, from every input the build will use, each read with the
    // precedence of its reader; all of them must name the same directory.
    let mut named: Vec<(String, PathBuf)> = Vec::new();
    if let Some((var, dir)) = first_set(&triple_names("MOQ_SYSROOT", target)) {
        named.push((var, PathBuf::from(dir)));
    }
    // The pkg-config crate reads `_<target>`, `TARGET_`, then the plain name.
    let mut pc_sysroot = triple_names("PKG_CONFIG_SYSROOT_DIR", target);
    pc_sysroot.extend([
        "TARGET_PKG_CONFIG_SYSROOT_DIR".to_owned(),
        "PKG_CONFIG_SYSROOT_DIR".to_owned(),
    ]);
    if let Some((var, dir)) = first_set(&pc_sysroot) {
        named.push((var, PathBuf::from(dir)));
    }
    // bindgen reads `_<target>`, then the plain name (no `TARGET_` spelling).
    let mut clang_args = triple_names("BINDGEN_EXTRA_CLANG_ARGS", target);
    clang_args.push("BINDGEN_EXTRA_CLANG_ARGS".to_owned());
    let extra = first_set(&clang_args);
    let mut in_extra = false;
    if let Some((var, args)) = &extra {
        // Every root operand counts: clang takes headers from `-isysroot`
        // over `--sysroot`, so the last one alone is not the one in effect.
        let dirs = sysroot::sysroots_in_clang_args(&args.to_string_lossy())
            .unwrap_or_else(|why| panic!("moq5-sys: cross build for {target}: {why}"));
        in_extra = !dirs.is_empty();
        for (index, dir) in dirs.into_iter().enumerate() {
            named.push((format!("{var} (root operand {})", index + 1), dir));
        }
    }
    if named.is_empty() {
        panic!(
            "moq5-sys: cross build for {target} on {host}: no target sysroot is named. Set \
             MOQ_SYSROOT_{target} (or PKG_CONFIG_SYSROOT_DIR, or a --sysroot/-isysroot \
             argument in BINDGEN_EXTRA_CLANG_ARGS_{target}); the host compiler's headers \
             are never used for a foreign target."
        );
    }
    let root = sysroot::agreeing_root(&named)
        .unwrap_or_else(|why| panic!("moq5-sys: cross build for {target}: {why}"));
    // The validated inputs are the permission for a cross probe.
    env::set_var("PKG_CONFIG_ALLOW_CROSS", "1");
    Cross {
        search,
        sysroot_arg: (!in_extra).then_some(root),
    }
}

/// What was observed, and the remedies, never a guessed build configuration.
fn diagnose(err: &pkg_config::Error, cross: Option<&Cross>) -> String {
    let searched = match cross {
        Some(cross) => cross
            .search
            .iter()
            .map(|d| d.display().to_string())
            .collect::<Vec<_>>()
            .join(":"),
        None => env::var("PKG_CONFIG_PATH").unwrap_or_else(|_| "(unset)".into()),
    };
    match err {
        pkg_config::Error::Command { command, cause } => format!(
            "moq5-sys: pkg-config could not be run ({command}: {cause}); install \
             pkg-config, or set PKG_CONFIG to its path"
        ),
        pkg_config::Error::CrossCompilation => format!(
            "moq5-sys: pkg-config refused the cross probe; name the target SDK with \
             MOQ_PREFIX_<target> and its sysroot with MOQ_SYSROOT_<target> \
             (searched: {searched})"
        ),
        pkg_config::Error::EnvNoPkgConfig(var) => {
            format!("moq5-sys: {var} is set, so pkg-config was not consulted and no SDK was found")
        }
        other if cross.is_some() => format!(
            "moq5-sys: the target search configuration ({searched}) has no \
             {PROBE}.pc: it is the only configuration a cross build consults (no host \
             variable, no default directory). Point MOQ_PREFIX_<target> at the target \
             SDK's install prefix.\n{other}"
        ),
        other => {
            let core = pkg_config::Config::new()
                .cargo_metadata(false)
                .env_metadata(false)
                .print_system_libs(false)
                .probe("libmoq")
                .is_ok();
            if core {
                format!(
                    "moq5-sys: `libmoq.pc` resolves but `{PROBE}.pc` does not \
                     (PKG_CONFIG_PATH searched: {searched}). Either the service \
                     component was not installed into that prefix, or the service was \
                     built with a backend that has no pkg-config representation \
                     (mvfst, proxygen or the direct MsQuic managed backend), which this \
                     crate does not support. Remedies: install the SDK with \
                     MOQ_BUILD_SERVICE=ON, point MOQ_PREFIX or PKG_CONFIG_PATH at the \
                     prefix that has it, or use a pkg-config-expressible service \
                     build.\n{other}"
                )
            } else {
                format!(
                    "moq5-sys: no libmoq SDK on PKG_CONFIG_PATH ({searched}); set \
                     MOQ_PREFIX to an install prefix holding lib/pkgconfig/{PROBE}.pc, or \
                     add that directory to PKG_CONFIG_PATH.\n{other}"
                )
            }
        }
    }
}

/// The heading of the crate documentation's "Header preambles" section.
const PREAMBLES_INTRO: &str =
    "# Header preambles\n\nThe leading comment of each header this crate binds, verbatim: \
     what a header states for all of its declarations at once rather than for one of \
     them (rcbuf's shard and overflow contract among them), which clang attaches to no \
     single item.\n\n";

/// The leading comment of each of `headers` (the first block comment of the
/// file), written as sections of the crate documentation's "Header
/// preambles": the contracts a header states for all of its declarations at
/// once. `intro` opens the section; the player's preambles continue it.
fn write_preambles(include_dirs: &[PathBuf], headers: &[&str], intro: Option<&str>, out: &Path) {
    let mut text = String::from(intro.unwrap_or_default());
    for h in headers {
        let path = include_dirs
            .iter()
            .map(|d| d.join(h))
            .find(|p| p.exists())
            .unwrap_or_else(|| panic!("moq5-sys: {h} is in the include directories"));
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("moq5-sys: could not read {}: {e}", path.display()));
        text.push_str(&format!("## {h}\n\n"));
        match header_preamble(&source) {
            Some(preamble) => text.push_str(&format!("```text\n{preamble}\n```\n\n")),
            None => text.push_str("(no leading comment)\n\n"),
        }
    }
    // Sections are separated by a blank line; the file ends with one newline.
    let text = format!("{}\n", text.trim_end());
    std::fs::write(out, text).unwrap_or_else(|e| panic!("could not write {}: {e}", out.display()));
}

/// The first block comment of a header when it precedes the header's first
/// `#include` (a comment after the includes is a section banner, not a
/// preamble), its `*` margin removed.
fn header_preamble(source: &str) -> Option<String> {
    let start = source.find("/*")?;
    if source
        .find("#include")
        .is_some_and(|include| include < start)
    {
        return None;
    }
    let end = start + source[start..].find("*/")?;
    let body = &source[start + 2..end];
    let lines: Vec<&str> = body
        .lines()
        .map(|line| {
            let line = line.trim_end();
            let trimmed = line.trim_start();
            let line = trimmed.strip_prefix('*').unwrap_or(trimmed);
            line.strip_prefix(' ').unwrap_or(line)
        })
        .collect();
    let text = lines.join("\n");
    let text = text.trim_matches('\n');
    (!text.is_empty()).then(|| text.to_owned())
}

/// What the base bindings derive for the base types the player's records
/// hold by value, which bindgen does not see in the player run (they are
/// blocklisted there): the span derives `Debug` and `Copy` and implements
/// `Default` by hand (it holds a pointer), and the two codes are plain
/// integers. Without it, the records' unions would not be `Copy`, and bindgen
/// would emit them as structs of placeholder fields.
#[derive(Debug)]
struct BaseDerives;

impl bindgen::callbacks::ParseCallbacks for BaseDerives {
    fn blocklisted_type_implements_trait(
        &self,
        name: &str,
        derive_trait: bindgen::callbacks::DeriveTrait,
    ) -> Option<bindgen::callbacks::ImplementsTrait> {
        use bindgen::callbacks::{DeriveTrait, ImplementsTrait};
        match (name, derive_trait) {
            ("moq_bytes_t" | "moq_bytes", DeriveTrait::Copy | DeriveTrait::Debug) => {
                Some(ImplementsTrait::Yes)
            }
            ("moq_bytes_t" | "moq_bytes", DeriveTrait::Default) => Some(ImplementsTrait::Manually),
            ("moq_media_time_domain_t" | "moq_media_stream_terminal_kind_t", _) => {
                Some(ImplementsTrait::Yes)
            }
            _ => None,
        }
    }
}

/// Two spellings bindgen gets wrong on its own: `MOQ_OK`/`MOQ_DONE` are
/// positive `#define`s it would type as `u32`, while every result code is a
/// `moq_result_t` (`int`) in C; and the `MOQ5_SYSVAL_` statics of wrapper.h
/// carry the C names they restate.
#[derive(Debug)]
struct ResultCodes;

impl bindgen::callbacks::ParseCallbacks for ResultCodes {
    fn int_macro(&self, name: &str, _value: i64) -> Option<bindgen::callbacks::IntKind> {
        (name == "MOQ_OK" || name == "MOQ_DONE" || name.starts_with("MOQ_ERR_"))
            .then_some(bindgen::callbacks::IntKind::Int)
    }

    fn item_name(&self, item: bindgen::callbacks::ItemInfo<'_>) -> Option<String> {
        item.name.strip_prefix("MOQ5_SYSVAL_").map(str::to_owned)
    }
}
