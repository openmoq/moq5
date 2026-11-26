//! Link the installed libmoq tiers a dependency graph asks for and generate
//! their bindings.
//!
//! The tiers are additive features: none (the core, with the publisher and
//! subscriber facades), `sim`, `media`, `locmaf` (which implies `media`),
//! `service` (which implies `media`) and `player` (which implies `service`).
//! Cargo unifies features across the graph, so this script builds the
//! union, from one source: the installed SDK, found through pkg-config.
//! There is no other source and no fallback: each tier links through its
//! own package (`libmoq-core`, `libmoq-sim`, `libmoq-media`,
//! `libmoq-locmaf`, or `libmoq-service`, whose `Requires` closure carries
//! the core, the media parsers and the managed providers the SDK expresses
//! through pkg-config), every package the graph needs must resolve, and all
//! of them from one SDK. `libmoq.pc` is never probed for a lower tier: it
//! names every component the SDK was built with, adapters included. The
//! script prints which tiers it bound, from which SDK and why (and writes the
//! same lines to `provenance.txt` beside the bindings); a missing package or
//! a graph split across SDKs fails the build with the reason.
//!
//! Environment, host builds:
//! - `MOQ_PREFIX`: a convenience that puts `<prefix>/lib/pkgconfig` (and
//!   `lib64/pkgconfig`, `share/pkgconfig` when present) in front of the
//!   search variable the probe reads (`PKG_CONFIG_PATH`, or the host-specific
//!   spelling of it when one is set).
//! - `LIBMOQ_CORE_STATIC`, `LIBMOQ_SIM_STATIC`, `LIBMOQ_MEDIA_STATIC`,
//!   `LIBMOQ_LOCMAF_STATIC`, `LIBMOQ_SERVICE_STATIC` (each with its
//!   `_DYNAMIC` form) and the
//!   `PKG_CONFIG_ALL_*` forms: the `pkg-config` crate's own static/shared
//!   selection for each probe. Unset, the installed libraries decide.
//! - `BINDGEN_EXTRA_CLANG_ARGS`: extra arguments for libclang, for instance
//!   the include directory of a libclang installed without its resource
//!   headers. The host C compiler's headers are never substituted for them.
//! - `DOCS_RS`: no SDK is probed; the crate compiles the committed snapshot
//!   of each enabled tier.
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
//! Each tier is generated into a file of its own (`bindings_<tier>.rs`, with
//! its constants made from macros in `macros_<tier>.txt`), in tier order:
//! every libmoq header outside the tier is blocklisted, so what a tier
//! references from a lower one is in that tier's bindings, and the traits
//! bindgen asks about for those types are answered from the lower tiers'
//! generated text (`build_tiers.rs`). A tier's bindings are therefore the
//! same whichever other tiers the graph enables. Beside them, the leading
//! comment of every bound header is written to `preambles_<tier>.md` and
//! compiled into the crate documentation: the contracts a header states for
//! all of its declarations at once (rcbuf's shard and overflow rules) are not
//! attached to any one item by clang. A tier whose headers the SDK lacks
//! fails the build with the remedy; a tier that is not enabled is never read.

use std::collections::BTreeSet;
use std::env;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use bindgen::callbacks::{IntKind, ItemInfo, ParseCallbacks};

#[path = "build_sysroot.rs"]
mod sysroot;
#[path = "build_tiers.rs"]
mod tiers;

/// The prefix of the statics the wrappers restate values under, stripped
/// from their generated names.
const SYSVAL_PREFIX: &str = "MOQ5_SYSVAL_";

/// The pkg-config search configuration of a cross build, kept for the
/// diagnostics.
struct Cross {
    search: Vec<PathBuf>,
    /// The sysroot to hand to libclang, unless the extra arguments carry it.
    sysroot_arg: Option<PathBuf>,
}

fn main() {
    println!("cargo:rustc-check-cfg=cfg(docsrs)");
    for var in [
        "MOQ_PREFIX",
        "DOCS_RS",
        "RUSTFMT",
        "LIBCLANG_PATH",
        "BINDGEN_EXTRA_CLANG_ARGS",
    ] {
        println!("cargo:rerun-if-env-changed={var}");
    }
    let features = tiers::Features::from_enabled(|name| {
        env::var_os(format!("CARGO_FEATURE_{}", name.to_ascii_uppercase())).is_some()
    });
    let mut report = Report::default();
    report.line(format!(
        "moq5-sys: tiers {} (enabled tier features: {})",
        features.describe(),
        features.enabled()
    ));
    if env::var_os("DOCS_RS").is_some() {
        println!("cargo:rustc-cfg=docsrs");
        report.line(
            "moq5-sys: DOCS_RS is set: no SDK is probed or linked; the committed snapshot of \
             each enabled tier is compiled"
                .to_owned(),
        );
        report.print();
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
    let packages = features.packages();
    report.line(format!(
        "moq5-sys: native source: the installed SDK, found through pkg-config (this crate has \
         no other source); packages for these tiers: {}",
        packages.join(", ")
    ));
    let mut libs = Vec::new();
    for package in &packages {
        match pkg_config::Config::new().probe(package) {
            Ok(lib) => libs.push((*package, lib)),
            Err(e) => panic!("{}", diagnose(&e, package, features, cross.as_ref())),
        }
    }
    check_one_sdk(features, &mut report);
    // pkg-config emitted the link-search and link-lib lines for each
    // closure; rustc keeps one mention of a library named twice, the last,
    // which keeps the core after every tier that needs it. Dependents get
    // the include directories through DEP_MOQ_INCLUDE.
    let mut include_paths: Vec<PathBuf> = Vec::new();
    for (_, lib) in &libs {
        for dir in &lib.include_paths {
            if !include_paths.contains(dir) {
                include_paths.push(dir.clone());
            }
        }
    }
    let include: Vec<String> = include_paths
        .iter()
        .map(|p| p.display().to_string())
        .collect();
    println!("cargo:include={}", include.join(";"));
    // A static library is bundled into this crate's rlib when the crate is
    // compiled, so cargo is told to watch every archive the link names: an
    // SDK rebuilt in place then rebuilds the crate instead of linking the
    // copy bundled earlier.
    for (_, lib) in &libs {
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
    }

    let mut clang_args: Vec<String> = include_paths
        .iter()
        .map(|dir| format!("-I{}", dir.display()))
        .collect();
    if let Some(cross) = &cross {
        // The target SDK's headers come from its pkg-config files; the
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
    let tiers = features.tiers();
    for tier in &tiers {
        for h in tier.headers {
            if !include_paths.iter().any(|d| d.join(h).exists()) {
                panic!(
                    "moq5-sys: `{h}` (the {} tier) is not in the include directories \
                     pkg-config reported ({}); {}",
                    tier.name,
                    include.join(", "),
                    tier.missing_header
                );
            }
        }
    }
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("cargo sets OUT_DIR"));
    // Which libclang generated the bindings, for the snapshot test's report.
    write_file(
        &out_dir.join("libclang.txt"),
        &bindgen::clang_version().full,
    );
    let installed = installed_headers(&include_paths);
    let mut lower = tiers::Derives::default();
    for tier in &tiers {
        let text = generate(tier, &clang_args, &installed, &lower, &out_dir);
        lower.extend(tiers::Derives::scan(&text));
        write_preambles(
            &include_paths,
            tier.headers,
            (tier.name == "core").then_some(PREAMBLES_INTRO),
            &out_dir.join(format!("preambles_{}.md", tier.name)),
        );
        report.line(format!(
            "moq5-sys: generated the {} tier from {} into bindings_{}.rs",
            tier.name, tier.wrapper, tier.name
        ));
    }
    write_file(&out_dir.join("provenance.txt"), &report.text);
    report.print();
}

/// The lines that say what the build chose and why: printed on the build
/// script's output (shown by `cargo build -vv`) and written beside the
/// bindings.
#[derive(Default)]
struct Report {
    text: String,
}

impl Report {
    fn line(&mut self, line: String) {
        self.text.push_str(&line);
        self.text.push('\n');
    }

    fn print(&self) {
        print!("{}", self.text);
    }
}

/// Every SDK-owned package the graph's metadata depends on -- the packages
/// probed and the SDK-owned packages their `Requires` and
/// `Requires.private` name, transitively (the service's aggregate `libmoq`
/// and its managed adapters' packages among them) -- resolves from one
/// pkg-config directory and reports one version: one SDK, never a mix.
/// External providers keep their own prefixes and versions. The metadata
/// is read through the same search configuration as the probes and emits
/// nothing for cargo.
fn check_one_sdk(features: tiers::Features, report: &mut Report) {
    let unresolved = |package: &str, e: String| -> String {
        format!(
            "moq5-sys: `{package}.pc`, which the {} tiers' metadata requires, does not resolve on \
             the search path the probes used: {e}",
            features.describe()
        )
    };
    let closure = tiers::owned_closure(&features.packages(), |package| {
        let mut names = Vec::new();
        for query in ["--print-requires", "--print-requires-private"] {
            let output = pkg_config_query(&[query, package]).map_err(|e| unresolved(package, e))?;
            names.extend(tiers::required_names(&output));
        }
        Ok(names)
    })
    .unwrap_or_else(|why| panic!("{why}"));
    let mut resolved: Vec<(String, String)> = Vec::new();
    let mut versions: Vec<(String, String)> = Vec::new();
    for package in &closure {
        let dir = pkg_config_query(&["--variable=pcfiledir", package])
            .unwrap_or_else(|e| panic!("{}", unresolved(package, e)));
        let canonical = Path::new(&dir)
            .canonicalize()
            .map_or(dir.clone(), |path| path.display().to_string());
        resolved.push((package.clone(), canonical));
        let version = pkg_config_query(&["--modversion", package])
            .unwrap_or_else(|e| panic!("{}", unresolved(package, e)));
        versions.push((package.clone(), version));
    }
    if resolved.iter().any(|(_, dir)| *dir != resolved[0].1) {
        panic!("{}", tiers::mixed_provenance(features, &resolved));
    }
    if versions
        .iter()
        .any(|(_, version)| *version != versions[0].1)
    {
        panic!("{}", tiers::mixed_versions(features, &versions));
    }
    let mut line = String::from("moq5-sys: one SDK:");
    for (package, dir) in &resolved {
        let _ = write!(line, " {package}.pc in {dir};");
    }
    let _ = write!(line, " version {}", versions[0].1);
    report.line(line);
}

/// The value of a pkg-config variable as the `pkg-config` crate reads it:
/// `<VAR>_<target>`, `<VAR>_<target with _>`, `HOST_<VAR>` (`TARGET_<VAR>`
/// for a cross build), then `<VAR>`.
fn targeted_env(var: &str) -> Option<OsString> {
    let target = env::var("TARGET").unwrap_or_default();
    let host = env::var("HOST").unwrap_or_default();
    let kind = if host == target { "HOST" } else { "TARGET" };
    [
        format!("{var}_{target}"),
        format!("{var}_{}", target.replace('-', "_")),
        format!("{kind}_{var}"),
        var.to_owned(),
    ]
    .iter()
    .find_map(env::var_os)
}

/// `<package>.pc` in a directory of the search configuration the probes
/// read (`PKG_CONFIG_PATH`, then `PKG_CONFIG_LIBDIR`, as the `pkg-config`
/// crate spells them), if one holds it.
fn package_file(package: &str) -> Option<PathBuf> {
    ["PKG_CONFIG_PATH", "PKG_CONFIG_LIBDIR"]
        .iter()
        .filter_map(|var| targeted_env(var))
        .flat_map(|value| env::split_paths(&value).collect::<Vec<_>>())
        .map(|dir| dir.join(format!("{package}.pc")))
        .find(|file| file.is_file())
}

/// One metadata query (`--print-requires`, `--variable=...`,
/// `--modversion`), run as the `pkg-config` crate runs its probes: the same
/// executable (`PKG_CONFIG`, else `pkg-config`, else `pkgconf`) and the
/// same search configuration. It prints nothing for cargo. The output is
/// trimmed; a failure is the command's stderr.
fn pkg_config_query(args: &[&str]) -> Result<String, String> {
    let configured = targeted_env("PKG_CONFIG");
    let run = |exe: &OsString| {
        let mut command = std::process::Command::new(exe);
        command.args(args);
        for var in [
            "PKG_CONFIG_PATH",
            "PKG_CONFIG_LIBDIR",
            "PKG_CONFIG_SYSROOT_DIR",
        ] {
            if let Some(value) = targeted_env(var) {
                command.env(var, value);
            }
        }
        command
            .env("PKG_CONFIG_ALLOW_SYSTEM_LIBS", "1")
            .env("PKG_CONFIG_ALLOW_SYSTEM_CFLAGS", "1")
            .output()
    };
    let output = match &configured {
        Some(exe) => run(exe),
        None => run(&OsString::from("pkg-config")).or_else(|_| run(&OsString::from("pkgconf"))),
    }
    .map_err(|e| format!("pkg-config could not be run: {e}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

/// Every libmoq header the SDK installed, as `moq/<name>.h`.
fn installed_headers(include_dirs: &[PathBuf]) -> Vec<String> {
    let mut names: Vec<String> = include_dirs
        .iter()
        .filter_map(|dir| std::fs::read_dir(dir.join("moq")).ok())
        .flatten()
        .filter_map(|entry| entry.ok()?.file_name().into_string().ok())
        .filter(|name| name.ends_with(".h"))
        .map(|name| format!("moq/{name}"))
        .collect();
    names.sort();
    names.dedup();
    names
}

/// A pattern for every installed libmoq header outside `own`.
fn other_headers_pattern(installed: &[String], own: &[&str]) -> String {
    let names: Vec<String> = installed
        .iter()
        .filter(|header| !own.contains(&header.as_str()))
        .map(|header| {
            header
                .trim_start_matches("moq/")
                .trim_end_matches(".h")
                .replace('.', "\\.")
        })
        .collect();
    format!(".*/moq/({})\\.h", names.join("|"))
}

/// Generate one tier's bindings and its list of macro constants, returning
/// the bindings' text. Every installed libmoq header outside the tier is
/// blocklisted, and so is every type the lower tiers declare; the traits of
/// those types come from `lower`.
fn generate(
    tier: &tiers::Tier,
    clang_args: &[String],
    installed: &[String],
    lower: &tiers::Derives,
    out_dir: &Path,
) -> String {
    let macros = MacroNames::default();
    let mut builder = generator(clang_args)
        .header(tier.wrapper)
        .blocklist_file(other_headers_pattern(installed, tier.headers))
        .parse_callbacks(Box::new(TierDerives(lower.clone())));
    for name in lower.type_names() {
        builder = builder.blocklist_type(name);
    }
    let builder = if tier.name == "player" {
        // The player's own declarations and the playback engine
        // configuration they take, not the whole playback engine.
        builder
            .allowlist_file(".*/moq/media_player\\.h")
            .allowlist_type("moq_playback_cfg(_t)?")
            .allowlist_function("moq_playback_cfg_init_sized")
            .allowlist_var("MOQ_PLAYBACK_.*")
    } else {
        builder
            .allowlist_function("moq_.*")
            .allowlist_type("moq_.*")
            .allowlist_var("MOQ_.*")
    };
    let bindings = builder
        .allowlist_var("MOQ5_SYSVAL_.*")
        // After every other callback, as MacroNames requires.
        .parse_callbacks(Box::new(macros.clone()))
        .generate()
        .unwrap_or_else(|e| panic!("bindgen failed on the libmoq {} headers: {e}", tier.name));
    let out = out_dir.join(format!("bindings_{}.rs", tier.name));
    write_bindings(&bindings, &out);
    macros.write(&out_dir.join(format!("macros_{}.txt", tier.name)));
    std::fs::read_to_string(&out)
        .unwrap_or_else(|e| panic!("could not read {}: {e}", out.display()))
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
        // snapshot is held equal to a fresh generation on every qualified
        // toolchain, line for line (tests/snapshot.rs; only a macro constant's
        // documentation may differ, as libclang releases attach it
        // differently), so a formatter release that changes the layout shows
        // up there as a regeneration, never silently. A missing rustfmt is not an
        // error for a consumer (bindgen writes the bindings unformatted).
        .formatter(bindgen::Formatter::Rustfmt)
        .parse_callbacks(Box::new(ResultCodes))
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
}

fn write_bindings(bindings: &bindgen::Bindings, out: &Path) {
    bindings
        .write_to_file(out)
        .unwrap_or_else(|e| panic!("could not write {}: {e}", out.display()));
}

fn write_file(out: &Path, contents: &str) {
    std::fs::write(out, contents)
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
fn diagnose(
    err: &pkg_config::Error,
    package: &str,
    features: tiers::Features,
    cross: Option<&Cross>,
) -> String {
    let searched = match cross {
        Some(cross) => cross
            .search
            .iter()
            .map(|d| d.display().to_string())
            .collect::<Vec<_>>()
            .join(":"),
        None => {
            let path = env::var("PKG_CONFIG_PATH").unwrap_or_else(|_| "(unset)".into());
            match env::var("PKG_CONFIG_LIBDIR") {
                Ok(libdir) => format!("PKG_CONFIG_PATH {path}, PKG_CONFIG_LIBDIR {libdir}"),
                Err(_) => path,
            }
        }
    };
    let found = package_file(package).map(|file| file.display().to_string());
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
        // The package is there, but what it requires is not.
        other if found.is_some() => format!(
            "{}\n{other}",
            tiers::unresolved_requirements(package, features, &found.unwrap_or_default())
        ),
        other if cross.is_some() => format!(
            "moq5-sys: the target search configuration ({searched}) has no \
             {package}.pc: it is the only configuration a cross build consults (no host \
             variable, no default directory). Point MOQ_PREFIX_<target> at the target \
             SDK's install prefix.\n{other}"
        ),
        other => {
            let libmoq = pkg_config::Config::new()
                .cargo_metadata(false)
                .env_metadata(false)
                .print_system_libs(false)
                .probe("libmoq")
                .is_ok();
            format!(
                "{}\n{other}",
                tiers::missing_package(package, features, &searched, libmoq)
            )
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
/// once. `intro` opens the section (the core's); each later tier's
/// preambles continue it.
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

/// The traits of the lower tiers' types, which a tier's generation
/// blocklists and references by name: what those tiers' generated bindings
/// derive or implement, read from their text. Without it, bindgen would
/// assume none, and a record holding a span by value would lose `Copy`.
#[derive(Debug)]
struct TierDerives(tiers::Derives);

impl ParseCallbacks for TierDerives {
    fn blocklisted_type_implements_trait(
        &self,
        name: &str,
        derive_trait: bindgen::callbacks::DeriveTrait,
    ) -> Option<bindgen::callbacks::ImplementsTrait> {
        use bindgen::callbacks::{DeriveTrait, ImplementsTrait};
        let wanted = match derive_trait {
            DeriveTrait::Copy => tiers::Trait::Copy,
            DeriveTrait::Debug => tiers::Trait::Debug,
            DeriveTrait::Default => tiers::Trait::Default,
            DeriveTrait::Hash => tiers::Trait::Hash,
            DeriveTrait::PartialEqOrPartialOrd => tiers::Trait::PartialEq,
        };
        self.0.answer(name, wanted).map(|answer| match answer {
            tiers::Answer::Yes => ImplementsTrait::Yes,
            tiers::Answer::Manually => ImplementsTrait::Manually,
            tiers::Answer::No => ImplementsTrait::No,
        })
    }
}

/// Two spellings bindgen gets wrong on its own: `MOQ_OK`/`MOQ_DONE` are
/// positive `#define`s it would type as `u32`, while every result code is a
/// `moq_result_t` (`int`) in C; and the `MOQ5_SYSVAL_` statics of the
/// wrappers carry the C names they restate.
#[derive(Debug)]
struct ResultCodes;

impl bindgen::callbacks::ParseCallbacks for ResultCodes {
    fn int_macro(&self, name: &str, _value: i64) -> Option<bindgen::callbacks::IntKind> {
        (name == "MOQ_OK" || name == "MOQ_DONE" || name.starts_with("MOQ_ERR_"))
            .then_some(bindgen::callbacks::IntKind::Int)
    }

    fn item_name(&self, item: ItemInfo<'_>) -> Option<String> {
        item.name.strip_prefix(SYSVAL_PREFIX).map(str::to_owned)
    }
}

/// The names of the constants one generation made from macros, written
/// beside its bindings: the snapshot test tells those constants from every
/// other item by them. libclang 23 attaches the comment before a macro
/// definition to the macro under `-fparse-all-comments`, and earlier
/// releases attach none, so only those constants' documentation varies
/// with the libclang that generated them.
///
/// bindgen reports each macro it evaluates to an integer or a string, the
/// blocklisted headers' included; it reports no floating-point or character
/// macro (no bound constant is one), so a constant made from one would be
/// held to its documentation. A reported name a `MOQ5_SYSVAL_` static
/// restates is left out: the static is a declaration, whatever bindgen made
/// of the macro (it reports a blocklisted header's macros too, which the
/// blocklist then drops). The callbacks only observe, and bindgen asks
/// the last registered callback first and stops at the first answer, so this
/// one is registered after every other.
#[derive(Clone, Debug, Default)]
struct MacroNames(Arc<Mutex<Names>>);

#[derive(Debug, Default)]
struct Names {
    /// The macros bindgen evaluated to a constant.
    macros: BTreeSet<String>,
    /// The names the `MOQ5_SYSVAL_` statics restate.
    restated: BTreeSet<String>,
}

impl MacroNames {
    fn names(&self) -> MutexGuard<'_, Names> {
        self.0
            .lock()
            .expect("no bindgen callback panicked holding the names")
    }

    /// The macro constants' names, one a line, sorted.
    fn write(&self, out: &Path) {
        let names = self.names();
        let mut text = String::new();
        for name in names.macros.difference(&names.restated) {
            text.push_str(name);
            text.push('\n');
        }
        write_file(out, &text);
    }
}

impl ParseCallbacks for MacroNames {
    fn int_macro(&self, name: &str, _value: i64) -> Option<IntKind> {
        self.names().macros.insert(name.to_owned());
        None
    }

    fn str_macro(&self, name: &str, _value: &[u8]) {
        self.names().macros.insert(name.to_owned());
    }

    fn item_name(&self, item: ItemInfo<'_>) -> Option<String> {
        if let Some(name) = item.name.strip_prefix(SYSVAL_PREFIX) {
            self.names().restated.insert(name.to_owned());
        }
        None
    }
}
