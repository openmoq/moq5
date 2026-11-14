//! The tier rules of the build script (`build_tiers.rs`): which tiers a
//! feature set binds and which packages carry them (every row of the
//! supported-combination table an installed SDK serves), what a missing
//! package or a graph split across SDKs is told, which SDK-owned metadata
//! a graph's one-SDK check reads, and the derive facts a tier's generation
//! reads from the tiers below it.

#[path = "../build_tiers.rs"]
mod tiers;

use tiers::{Answer, Derives, Features, Trait};

/// The features a graph enabling `names` reports, closed as the manifest
/// closes them.
fn features(names: &[&str]) -> Features {
    Features::from_enabled(|name| names.contains(&name))
}

fn tier_names(features: Features) -> Vec<&'static str> {
    features.tiers().iter().map(|tier| tier.name).collect()
}

#[test]
fn the_core_alone_links_the_core_package() {
    let core = features(&[]);
    assert_eq!(tier_names(core), ["core"]);
    assert_eq!(core.packages(), ["libmoq-core"]);
    assert_eq!(core.describe(), "core");
    assert_eq!(core.enabled(), "none: the default core");
}

#[test]
fn the_simulator_links_its_package_and_the_core_it_requires() {
    let sim = features(&["sim"]);
    assert_eq!(tier_names(sim), ["core", "sim"]);
    assert_eq!(sim.packages(), ["libmoq-sim"]);
}

#[test]
fn the_media_tier_links_its_package_and_the_core_it_requires() {
    let media = features(&["media"]);
    assert_eq!(tier_names(media), ["core", "media"]);
    assert_eq!(media.packages(), ["libmoq-media"]);
}

#[test]
fn the_service_links_through_its_own_closure() {
    let service = features(&["service"]);
    assert_eq!(
        tier_names(service),
        ["core", "media", "service"],
        "service implies media"
    );
    assert_eq!(service.packages(), ["libmoq-service"]);
    let player = features(&["player"]);
    assert_eq!(
        tier_names(player),
        ["core", "media", "service", "player"],
        "player implies service"
    );
    assert_eq!(player.packages(), ["libmoq-service"]);
}

#[test]
fn tiers_unify_into_one_set_of_packages() {
    let both = features(&["sim", "media"]);
    assert_eq!(tier_names(both), ["core", "sim", "media"]);
    assert_eq!(both.packages(), ["libmoq-media", "libmoq-sim"]);
    let everything = features(&["sim", "media", "service", "player"]);
    assert_eq!(
        tier_names(everything),
        ["core", "sim", "media", "service", "player"]
    );
    assert_eq!(everything.packages(), ["libmoq-service", "libmoq-sim"]);
    assert_eq!(
        everything.describe(),
        "core + sim + media + service + player"
    );
    assert_eq!(everything.enabled(), "sim, media, service, player");
}

#[test]
fn no_probe_is_ever_the_whole_sdk_file() {
    for names in [
        &[][..],
        &["sim"],
        &["media"],
        &["sim", "media"],
        &["service"],
        &["player", "sim"],
    ] {
        let set = features(names);
        assert!(
            !set.packages().contains(&"libmoq"),
            "{names:?} links libmoq.pc"
        );
    }
}

/// The `Requires` (public and private) of a service SDK with the PicoWT
/// managed adapter and a WTQuic one, as its `.pc` files state them.
fn sdk_requires(package: &str) -> Result<Vec<String>, String> {
    let names: &[&str] = match package {
        "libmoq-service" => &[
            "libmoq",
            "libmoq-pico-wt-managed",
            "libmoq-wtquic-msquic-managed",
        ],
        "libmoq-pico-wt-managed" => &["libmoq-pico-wt"],
        "libmoq-pico-wt" => &["libmoq"],
        "libmoq-wtquic-msquic-managed" => &["wtquic-msquic"],
        "libmoq-sim" | "libmoq-media" => &["libmoq-core"],
        "libmoq" | "libmoq-core" => &[],
        other => return Err(format!("{other} is not an SDK-owned package of this SDK")),
    };
    Ok(names.iter().map(|name| (*name).to_owned()).collect())
}

#[test]
fn the_core_closure_is_the_core_package() {
    let closure = tiers::owned_closure(&features(&[]).packages(), sdk_requires);
    assert_eq!(closure, Ok(vec!["libmoq-core".to_owned()]));
}

#[test]
fn a_tier_package_closure_holds_the_core_it_requires() {
    for (names, expected) in [
        (&["sim"][..], &["libmoq-sim", "libmoq-core"][..]),
        (&["media"], &["libmoq-media", "libmoq-core"]),
        (
            &["sim", "media"],
            &["libmoq-media", "libmoq-core", "libmoq-sim"],
        ),
    ] {
        let closure = tiers::owned_closure(&features(names).packages(), sdk_requires)
            .expect("every package resolves");
        assert_eq!(closure, expected, "{names:?}");
    }
}

#[test]
fn the_service_closure_holds_its_aggregate_and_adapter_packages() {
    // The service's metadata is read through its whole SDK-owned closure:
    // the aggregate libmoq it requires and the managed adapters' packages,
    // not the external providers they name in turn.
    let closure = tiers::owned_closure(&features(&["service"]).packages(), sdk_requires)
        .expect("every package resolves");
    assert_eq!(
        closure,
        [
            "libmoq-service",
            "libmoq",
            "libmoq-pico-wt-managed",
            "libmoq-pico-wt",
            "libmoq-wtquic-msquic-managed",
        ]
    );
    assert!(!closure.iter().any(|name| name == "wtquic-msquic"));
    let with_sim = tiers::owned_closure(&features(&["player", "sim"]).packages(), sdk_requires)
        .expect("every package resolves");
    assert!(with_sim.starts_with(&closure));
    assert_eq!(&with_sim[closure.len()..], ["libmoq-sim", "libmoq-core"]);
    // A service SDK without MPEG-TS has no media package, and its closure
    // never asks for one.
    assert!(!with_sim.iter().any(|name| name == "libmoq-media"));
}

#[test]
fn a_closure_walk_stops_at_cycles_and_reports_failures() {
    let cyclic = |package: &str| -> Result<Vec<String>, String> {
        Ok(match package {
            "libmoq-a" => vec!["libmoq-b".to_owned()],
            _ => vec!["libmoq-a".to_owned()],
        })
    };
    assert_eq!(
        tiers::owned_closure(&["libmoq-a"], cyclic),
        Ok(vec!["libmoq-a".to_owned(), "libmoq-b".to_owned()])
    );
    let failing = |_: &str| -> Result<Vec<String>, String> { Err("no such package".to_owned()) };
    assert_eq!(
        tiers::owned_closure(&["libmoq-service"], failing),
        Err("no such package".to_owned())
    );
    assert!(tiers::is_sdk_owned("libmoq") && tiers::is_sdk_owned("libmoq-wtquic"));
    assert!(!tiers::is_sdk_owned("libmoqx") && !tiers::is_sdk_owned("wtquic"));
}

#[test]
fn requirement_lines_name_their_packages() {
    assert_eq!(
        tiers::required_names("libmoq\nlibmoq-pico-wt >= 0.1\n\nwtquic-msquic = 2.5\n"),
        ["libmoq", "libmoq-pico-wt", "wtquic-msquic"]
    );
    assert!(tiers::required_names("").is_empty());
}

#[test]
fn a_missing_package_names_what_was_asked_and_the_remedy() {
    let text = tiers::missing_package(
        "libmoq-media",
        features(&["media"]),
        "/sdk/lib/pkgconfig",
        true,
    );
    for phrase in [
        "the graph asks for the core + media tiers (features: media)",
        "pkg-config finds no `libmoq-media.pc` (searched: /sdk/lib/pkgconfig)",
        "MOQ_BUILD_MPEG2TS",
        "`libmoq.pc` resolves on that path, but it is not used in its place",
        "Set MOQ_PREFIX",
    ] {
        assert!(text.contains(phrase), "{phrase:?} is missing from: {text}");
    }
    let text = tiers::missing_package("libmoq-core", features(&[]), "(unset)", false);
    assert!(
        text.contains("the core tier (features: none: the default core)"),
        "{text}"
    );
    assert!(!text.contains("libmoq.pc` resolves"), "{text}");
    let text = tiers::missing_package("libmoq-sim", features(&["sim"]), "(unset)", false);
    assert!(text.contains("MOQ_BUILD_SIM=ON"), "{text}");
    let text = tiers::missing_package("libmoq-service", features(&["player"]), "(unset)", true);
    assert!(
        text.contains("core + media + service + player tiers"),
        "{text}"
    );
    assert!(
        text.contains("mvfst, proxygen or the direct MsQuic"),
        "{text}"
    );
}

#[test]
fn a_package_whose_requirements_fail_says_so() {
    let text = tiers::unresolved_requirements(
        "libmoq-service",
        features(&["service"]),
        "/sdk/lib/pkgconfig/libmoq-service.pc",
    );
    for phrase in [
        "the graph asks for the core + media + service tiers (features: media, service)",
        "/sdk/lib/pkgconfig/libmoq-service.pc is on the search path",
        "pkg-config cannot resolve what it requires",
        "needs its pkg-config directory on PKG_CONFIG_PATH",
    ] {
        assert!(text.contains(phrase), "{phrase:?} is missing from: {text}");
    }
}

#[test]
fn a_graph_split_across_sdks_is_refused_with_both_sources() {
    let resolved = [
        ("libmoq-media".to_owned(), "/a/lib/pkgconfig".to_owned()),
        ("libmoq-core".to_owned(), "/b/lib/pkgconfig".to_owned()),
    ];
    let text = tiers::mixed_provenance(features(&["media"]), &resolved);
    assert!(
        text.contains("one dependency graph links one native SDK"),
        "{text}"
    );
    assert!(
        text.contains("libmoq-media.pc from /a/lib/pkgconfig"),
        "{text}"
    );
    assert!(
        text.contains("libmoq-core.pc from /b/lib/pkgconfig"),
        "{text}"
    );
    let versions = [
        ("libmoq-service".to_owned(), "0.1.0".to_owned()),
        ("libmoq-sim".to_owned(), "0.2.0".to_owned()),
    ];
    let text = tiers::mixed_versions(features(&["service", "sim"]), &versions);
    assert!(
        text.contains("libmoq-service 0.1.0, libmoq-sim 0.2.0"),
        "{text}"
    );
}

#[test]
fn every_tier_has_a_wrapper_and_a_header_list() {
    for tier in tiers::TIERS {
        assert!(!tier.headers.is_empty(), "{}", tier.name);
        assert!(!tier.missing_header.is_empty(), "{}", tier.name);
        assert_eq!(
            tier.feature.is_none(),
            tier.name == "core",
            "only the core has no feature"
        );
        let wrapper = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(tier.wrapper),
        )
        .expect("the wrapper is in the crate");
        for header in tier.headers {
            assert!(
                wrapper.contains(&format!("#include <{header}>")),
                "{} binds {header}, which its wrapper does not include",
                tier.name
            );
        }
    }
    assert!(
        !tiers::CORE.headers.contains(&"moq/control.h")
            && !tiers::CORE.headers.contains(&"moq/codec.h"),
        "the codec is not a bound tier"
    );
}

/// Formatted bindings as bindgen and rustfmt write them.
const FORMATTED: &str = r#"
#[doc = " A span with a ] in its doc and a \" quote."]
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct moq_bytes {
    pub data: *const u8,
    pub len: usize,
}
impl Default for moq_bytes {
    fn default() -> Self {
        unsafe { ::std::mem::zeroed() }
    }
}
pub type moq_bytes_t = moq_bytes;
#[repr(C)]
#[derive(Debug, Default, Copy, Clone)]
pub struct moq_location_filter_cfg {
    pub kind: u32,
}
pub type moq_location_filter_cfg_t = moq_location_filter_cfg;
pub type moq_result_t = i32;
pub type moq_handle_fn =
    ::std::option::Option<unsafe extern "C" fn(ctx: *mut ::std::os::raw::c_void)>;
pub type moq_ptr_t = *mut moq_bytes;
pub type moq_version = ::std::os::raw::c_uint;
pub use self::moq_version as moq_version_t;
#[repr(C)]
#[derive(Debug)]
pub struct moq_opaque {
    _unused: [u8; 0],
}
pub const MOQ_STRING: &[u8; 6] = b"pub s\0";
"#;

#[test]
fn derives_are_read_from_formatted_bindings() {
    check_derives(&Derives::scan(FORMATTED));
}

#[test]
fn derives_are_read_from_unformatted_bindings() {
    // Without rustfmt bindgen writes its tokens on few lines, spaced.
    let unformatted = "# [doc = \" x \"] # [repr (C)] # [derive (Debug , Copy , Clone)] pub struct moq_bytes { pub data : * const u8 , pub len : usize , } impl Default for moq_bytes { fn default () -> Self { unsafe { :: std :: mem :: zeroed () } } } pub type moq_bytes_t = moq_bytes ; # [repr (C)] # [derive (Debug , Default , Copy , Clone)] pub struct moq_location_filter_cfg { pub kind : u32 , } pub type moq_location_filter_cfg_t = moq_location_filter_cfg ; pub type moq_result_t = i32 ; pub type moq_handle_fn = :: std :: option :: Option < unsafe extern \"C\" fn (ctx : * mut :: std :: os :: raw :: c_void) > ; pub type moq_ptr_t = * mut moq_bytes ; pub type moq_version = :: std :: os :: raw :: c_uint ; pub use self :: moq_version as moq_version_t ; # [repr (C)] # [derive (Debug)] pub struct moq_opaque { _unused : [u8 ; 0] , }";
    check_derives(&Derives::scan(unformatted));
}

fn check_derives(derives: &Derives) {
    use Answer::{Manually, No, Yes};
    // A record: what it derives, and a hand-written Default.
    assert_eq!(derives.answer("moq_bytes", Trait::Copy), Some(Yes));
    assert_eq!(derives.answer("moq_bytes", Trait::Debug), Some(Yes));
    assert_eq!(derives.answer("moq_bytes", Trait::Default), Some(Manually));
    assert_eq!(derives.answer("moq_bytes", Trait::Hash), Some(No));
    // Its typedef answers as the record does.
    assert_eq!(derives.answer("moq_bytes_t", Trait::Copy), Some(Yes));
    assert_eq!(
        derives.answer("moq_bytes_t", Trait::Default),
        Some(Manually)
    );
    assert_eq!(
        derives.answer("moq_location_filter_cfg_t", Trait::Default),
        Some(Yes)
    );
    // Primitives, optional function pointers and the enum re-export.
    assert_eq!(derives.answer("moq_result_t", Trait::Copy), Some(Yes));
    assert_eq!(derives.answer("moq_result_t", Trait::Hash), Some(Yes));
    assert_eq!(derives.answer("moq_handle_fn", Trait::Default), Some(Yes));
    assert_eq!(derives.answer("moq_version_t", Trait::Copy), Some(Yes));
    assert_eq!(derives.answer("moq_version_t", Trait::Default), Some(Yes));
    // A raw pointer is Copy; its Default is the zeroed one.
    assert_eq!(derives.answer("moq_ptr_t", Trait::Copy), Some(Yes));
    assert_eq!(derives.answer("moq_ptr_t", Trait::Default), Some(Manually));
    // An opaque record derives only Debug.
    assert_eq!(derives.answer("moq_opaque", Trait::Debug), Some(Yes));
    assert_eq!(derives.answer("moq_opaque", Trait::Copy), Some(No));
    assert_eq!(derives.answer("moq_bytes", Trait::PartialEq), Some(No));
    assert_eq!(derives.answer("moq_result_t", Trait::PartialEq), Some(Yes));
    // A name the bindings do not declare has no answer.
    assert_eq!(derives.answer("moq_unknown_t", Trait::Copy), None);
    let mut names: Vec<&str> = derives.type_names().collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "moq_bytes",
            "moq_bytes_t",
            "moq_handle_fn",
            "moq_location_filter_cfg",
            "moq_location_filter_cfg_t",
            "moq_opaque",
            "moq_ptr_t",
            "moq_result_t",
            "moq_version",
            "moq_version_t",
        ]
    );
}

#[test]
fn a_later_tier_reads_every_lower_tier() {
    let mut lower =
        Derives::scan("#[derive(Debug, Copy, Clone)]\npub struct moq_core_t { pub x: u32 }");
    lower.extend(Derives::scan("pub type moq_media_t = moq_core_t;"));
    assert_eq!(lower.answer("moq_media_t", Trait::Copy), Some(Answer::Yes));
    assert_eq!(lower.type_names().count(), 2);
}

#[test]
fn a_record_takes_only_its_own_attributes() {
    let text = "#[derive(Debug, Copy, Clone)]\npub struct moq_a { pub x: u32 }\n\
                unsafe extern \"C\" { pub fn moq_f(); }\npub struct moq_b { pub y: u32 }\n";
    let derives = Derives::scan(text);
    assert_eq!(derives.answer("moq_a", Trait::Copy), Some(Answer::Yes));
    assert_eq!(
        derives.answer("moq_b", Trait::Copy),
        Some(Answer::No),
        "the derive of moq_a does not carry over"
    );
}
