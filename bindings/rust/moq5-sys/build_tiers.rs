//! The tiers of the build script: which tiers a feature set binds, which
//! pkg-config packages carry them, what a missing package or a mixed graph
//! is told, and the derive facts one tier's generated bindings state for the
//! tiers generated after it. Included by the build script and by
//! `tests/tiers.rs`.

use std::collections::{BTreeMap, BTreeSet};

/// One tier of the C library, as this crate binds it.
#[derive(Debug)]
pub struct Tier {
    /// The tier's name, which also names its generated files
    /// (`bindings_<name>.rs`, `macros_<name>.txt`, `preambles_<name>.md`).
    pub name: &'static str,
    /// The cargo feature that selects it; the core has none.
    pub feature: Option<&'static str>,
    /// The bindgen input: the tier's headers and the values restated for it.
    pub wrapper: &'static str,
    /// The headers whose declarations the tier's bindings hold, in
    /// dependency order. Every other libmoq header is blocklisted in the
    /// tier's generation: what the tier references from a lower tier is in
    /// that tier's bindings.
    pub headers: &'static [&'static str],
    /// What an SDK without one of those headers lacks.
    pub missing_header: &'static str,
}

/// The core: the session, its vocabulary and the facades. The codec headers
/// (`codec.h`, `control.h`, `buf.h`, `kvp.h`, `wire.h`, `vi64.h`) are not
/// bound.
pub const CORE: Tier = Tier {
    name: "core",
    feature: None,
    wrapper: "wrapper_core.h",
    headers: &[
        "moq/types.h",
        "moq/rcbuf.h",
        "moq/version.h",
        "moq/location.h",
        "moq/fill.h",
        "moq/cluster.h",
        "moq/session.h",
        "moq/transport_binding.h",
        "moq/connection_target.h",
        "moq/publisher.h",
        "moq/subscriber.h",
    ],
    missing_header: "the SDK is incomplete or older than this crate requires",
};

/// The deterministic simulator.
pub const SIM: Tier = Tier {
    name: "sim",
    feature: Some("sim"),
    wrapper: "wrapper_sim.h",
    headers: &["moq/sim.h"],
    missing_header: "the `sim` feature needs an SDK built with MOQ_BUILD_SIM=ON",
};

/// The media parsers the safe crates wrap.
pub const MEDIA: Tier = Tier {
    name: "media",
    feature: Some("media"),
    wrapper: "wrapper_media.h",
    headers: &["moq/loc.h", "moq/cmaf.h", "moq/media_object.h", "moq/msf.h"],
    missing_header: "the `media` feature needs an SDK built with its media parsers \
                     (MOQ_BUILD_LOC, MOQ_BUILD_CMAF, MOQ_BUILD_MEDIA_OBJECT, MOQ_BUILD_MSF)",
};

/// The managed endpoint, media sender and media receiver.
pub const SERVICE: Tier = Tier {
    name: "service",
    feature: Some("service"),
    wrapper: "wrapper_service.h",
    headers: &[
        "moq/endpoint.h",
        "moq/media_auth.h",
        "moq/media_sender.h",
        "moq/media_receiver.h",
    ],
    missing_header: "the `service` feature needs an SDK built with MOQ_BUILD_SERVICE=ON",
};

/// The media player, with the playback engine configuration it takes.
pub const PLAYER: Tier = Tier {
    name: "player",
    feature: Some("player"),
    wrapper: "wrapper_player.h",
    headers: &["moq/playback.h", "moq/media_player.h"],
    missing_header: "the `player` feature needs an SDK built with MOQ_BUILD_PLAYBACK=ON",
};

/// Every tier, in generation order: a tier is generated after every tier it
/// references.
pub const TIERS: [&Tier; 5] = [&CORE, &SIM, &MEDIA, &SERVICE, &PLAYER];

/// The tier features a dependency graph enabled. Cargo unifies features
/// across the graph, so this is the union of every crate's request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Features {
    /// `sim`.
    pub sim: bool,
    /// `media`.
    pub media: bool,
    /// `service`.
    pub service: bool,
    /// `player`.
    pub player: bool,
}

impl Features {
    /// The features `enabled` reports for each feature name (the build
    /// script reads `CARGO_FEATURE_<NAME>`), closed under the manifest's
    /// implications: `player` implies `service`, which implies `media`.
    pub fn from_enabled(enabled: impl Fn(&str) -> bool) -> Features {
        let player = enabled("player");
        let service = player || enabled("service");
        Features {
            sim: enabled("sim"),
            media: service || enabled("media"),
            service,
            player,
        }
    }

    fn has(self, tier: &Tier) -> bool {
        match tier.feature {
            None => true,
            Some("sim") => self.sim,
            Some("media") => self.media,
            Some("service") => self.service,
            Some("player") => self.player,
            Some(_) => false,
        }
    }

    /// The tiers this graph binds, in generation order; the core always.
    pub fn tiers(self) -> Vec<&'static Tier> {
        TIERS.into_iter().filter(|tier| self.has(tier)).collect()
    }

    /// The tiers as the diagnostics name them: `core + sim + media`.
    pub fn describe(self) -> String {
        self.tiers()
            .iter()
            .map(|tier| tier.name)
            .collect::<Vec<_>>()
            .join(" + ")
    }

    /// The enabled tier features, as the diagnostics name them.
    pub fn enabled(self) -> String {
        let names: Vec<&str> = self
            .tiers()
            .iter()
            .filter_map(|tier| tier.feature)
            .collect();
        if names.is_empty() {
            "none: the default core".to_owned()
        } else {
            names.join(", ")
        }
    }

    /// The pkg-config packages that carry these tiers, in probe order. Each
    /// tier package requires the ones below it, so the highest package of
    /// each line of tiers is enough: the service's (whose closure brings
    /// the core and the media parsers), else the media parsers', and the
    /// simulator's beside either; the core's alone when nothing else is
    /// asked for. `libmoq.pc` is never one of them: it names every
    /// component the SDK was built with, adapters included, so linking it
    /// for a lower tier would pull in a QUIC stack.
    pub fn packages(self) -> Vec<&'static str> {
        let mut packages = Vec::new();
        if self.service {
            packages.push("libmoq-service");
        } else if self.media {
            packages.push("libmoq-media");
        }
        if self.sim {
            packages.push("libmoq-sim");
        }
        if packages.is_empty() {
            packages.push("libmoq-core");
        }
        packages
    }
}

/// A package the SDK itself installs: `libmoq` and every `libmoq-*`
/// package (the tiers and the managed adapters' packages). Everything else
/// a closure names (wtquic, MsQuic, OpenSSL, ...) is an external provider
/// with its own prefix and version.
pub fn is_sdk_owned(package: &str) -> bool {
    package == "libmoq" || package.starts_with("libmoq-")
}

/// The package names a `pkg-config --print-requires` (or
/// `--print-requires-private`) output lists, one a line, each possibly
/// followed by a version constraint (`libmoq >= 0.1`).
pub fn required_names(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_owned)
        .collect()
}

/// The SDK-owned packages a graph's metadata depends on: the packages it
/// probes and every SDK-owned package their `Requires` and
/// `Requires.private` name, transitively, in discovery order. `requires`
/// answers one package's requirements (both kinds). External packages end
/// the walk: they are neither listed nor followed. This only reads
/// metadata; the probes alone decide what links, so the service's
/// aggregate `libmoq` is inspected here and never linked for a lower tier.
pub fn owned_closure(
    probed: &[&str],
    requires: impl Fn(&str) -> Result<Vec<String>, String>,
) -> Result<Vec<String>, String> {
    let mut closure: Vec<String> = Vec::new();
    let mut pending: Vec<String> = probed.iter().map(|name| (*name).to_owned()).collect();
    pending.reverse();
    while let Some(package) = pending.pop() {
        if closure.contains(&package) || !is_sdk_owned(&package) {
            continue;
        }
        let mut next: Vec<String> = requires(&package)?
            .into_iter()
            .filter(|name| is_sdk_owned(name) && !closure.contains(name))
            .collect();
        closure.push(package);
        next.reverse();
        pending.extend(next);
    }
    Ok(closure)
}

/// Why a package probe found nothing, and what to do: a missing package is
/// never replaced by another one.
pub fn missing_package(
    package: &str,
    features: Features,
    searched: &str,
    libmoq_resolves: bool,
) -> String {
    let plural = features.tiers().len() > 1;
    let mut text = format!(
        "moq5-sys: the graph asks for the {} {} (features: {}), which {} through \
         `{package}.pc`; pkg-config finds no `{package}.pc` (searched: {searched}). ",
        features.describe(),
        if plural { "tiers" } else { "tier" },
        features.enabled(),
        if plural { "link" } else { "links" },
    );
    let remedy = match package {
        "libmoq-core" => {
            "Every SDK that ships this crate's core tier installs it beside the core library."
        }
        "libmoq-sim" => "It is installed by an SDK built with MOQ_BUILD_SIM=ON.",
        "libmoq-media" => {
            "It is installed by an SDK that builds every media parser: MOQ_BUILD_LOC, \
             MOQ_BUILD_CMAF, MOQ_BUILD_MPEG2TS, MOQ_BUILD_MEDIA_OBJECT and MOQ_BUILD_MSF."
        }
        "libmoq-service" => {
            "It is installed by an SDK built with MOQ_BUILD_SERVICE=ON whose backend has a \
             pkg-config representation; a service built with mvfst, proxygen or the direct \
             MsQuic managed backend has none, and this crate does not support it."
        }
        _ => "",
    };
    text.push_str(remedy);
    if libmoq_resolves {
        text.push_str(
            " `libmoq.pc` resolves on that path, but it is not used in its place: it names \
             every component the SDK was built with, adapters included.",
        );
    }
    text.push_str(
        " Set MOQ_PREFIX to the install prefix of an SDK that carries every requested tier, \
         or put its lib/pkgconfig directory on PKG_CONFIG_PATH.",
    );
    text
}

/// Why a package that is on the search path did not probe: what it
/// requires does not resolve. `file` is where it was found.
pub fn unresolved_requirements(package: &str, features: Features, file: &str) -> String {
    let plural = features.tiers().len() > 1;
    format!(
        "moq5-sys: the graph asks for the {} {} (features: {}), which {} through \
         `{package}.pc`; {file} is on the search path, but pkg-config cannot resolve what it \
         requires (its output follows). An SDK-owned package missing beside it means an \
         incomplete SDK; an external provider's package (wtquic, MsQuic, ...) needs its \
         pkg-config directory on PKG_CONFIG_PATH.",
        features.describe(),
        if plural { "tiers" } else { "tier" },
        features.enabled(),
        if plural { "link" } else { "links" },
    )
}

/// The refusal of a graph whose packages resolve from more than one SDK:
/// `resolved` is each package with the pkg-config directory it came from.
pub fn mixed_provenance(features: Features, resolved: &[(String, String)]) -> String {
    let listed: Vec<String> = resolved
        .iter()
        .map(|(package, dir)| format!("{package}.pc from {dir}"))
        .collect();
    format!(
        "moq5-sys: one dependency graph links one native SDK, but the packages of the {} \
         tiers resolve from more than one: {}. Set MOQ_PREFIX (or PKG_CONFIG_PATH) so that \
         every package comes from the one SDK that carries all of these tiers.",
        features.describe(),
        listed.join(", ")
    )
}

/// The refusal of a graph whose packages carry different versions.
pub fn mixed_versions(features: Features, versions: &[(String, String)]) -> String {
    let listed: Vec<String> = versions
        .iter()
        .map(|(package, version)| format!("{package} {version}"))
        .collect();
    format!(
        "moq5-sys: the packages of the {} tiers report different versions ({}); one graph \
         links one SDK.",
        features.describe(),
        listed.join(", ")
    )
}

/// A trait bindgen asks about for a type it does not generate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Trait {
    /// `Copy`.
    Copy,
    /// `Debug`.
    Debug,
    /// `Default`.
    Default,
    /// `Hash`.
    Hash,
    /// `PartialEq` (and `PartialOrd`).
    PartialEq,
}

impl Trait {
    fn derive_name(self) -> &'static str {
        match self {
            Trait::Copy => "Copy",
            Trait::Debug => "Debug",
            Trait::Default => "Default",
            Trait::Hash => "Hash",
            Trait::PartialEq => "PartialEq",
        }
    }
}

/// Whether a type has a trait: derived, implemented by hand, or neither.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// Derived (or a primitive's own).
    Yes,
    /// Implemented by hand (bindgen's zeroing `Default`).
    Manually,
    /// Not implemented.
    No,
}

/// What a tier's generated bindings declare about each of their types: the
/// derives of each record, the records with a hand-written `Default`, and
/// each alias's target. A tier generated after it blocklists those types
/// and asks for their traits here, so its records derive exactly what they
/// would if every tier were generated at once.
#[derive(Clone, Debug, Default)]
pub struct Derives {
    records: BTreeMap<String, BTreeSet<String>>,
    manual_default: BTreeSet<String>,
    aliases: BTreeMap<String, String>,
}

impl Derives {
    /// Read the declarations of generated bindings, formatted or not.
    pub fn scan(text: &str) -> Derives {
        let mut derives = Derives::default();
        let mut pending: Option<BTreeSet<String>> = None;
        let mut i = 0;
        while i < text.len() {
            let rest = &text[i..];
            let c = rest.as_bytes()[0];
            if c.is_ascii_whitespace() {
                i += 1;
            } else if rest.starts_with("//") {
                i += rest.find('\n').map_or(rest.len(), |n| n + 1);
            } else if rest.starts_with("/*") {
                i += rest.find("*/").map_or(rest.len(), |n| n + 2);
            } else if c == b'#' {
                let (end, attribute) = attribute(rest);
                if let Some(list) = derive_list(attribute) {
                    pending.get_or_insert_with(BTreeSet::new).extend(list);
                }
                i += end;
            } else if c == b'"' {
                i += string_end(rest, 0) + 1;
            } else if let Some((name, used)) =
                item(rest, &["pub", "struct"]).or_else(|| item(rest, &["pub", "union"]))
            {
                derives
                    .records
                    .insert(name.to_owned(), pending.take().unwrap_or_default());
                i += used;
            } else if let Some((name, used)) = item(rest, &["pub", "type"]) {
                let after = &rest[used..];
                let end = after.find(';').unwrap_or(after.len());
                // Whitespace is dropped: unformatted output spaces every
                // token (`:: std :: os :: raw :: c_int`).
                let target: String = after[..end]
                    .trim_start()
                    .trim_start_matches('=')
                    .chars()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                derives.aliases.insert(name.to_owned(), target);
                pending = None;
                i += used + end;
            } else if let Some((path, used)) = use_item(rest) {
                // bindgen spells a typedef of an enum `pub use self::A as B;`.
                if let Some((target, name)) = path.rsplit_once(" as ") {
                    let target = target.rsplit("::").next().unwrap_or(target);
                    derives
                        .aliases
                        .insert(name.trim().to_owned(), target.trim().to_owned());
                }
                pending = None;
                i += used;
            } else if let Some((name, used)) = item(rest, &["impl", "Default", "for"]) {
                derives.manual_default.insert(name.to_owned());
                pending = None;
                i += used;
            } else {
                // Any other token ends the attributes of the next item.
                pending = None;
                i += token_len(rest);
            }
        }
        derives
    }

    /// Every type these bindings declare, records and aliases: a later
    /// tier blocklists each name, so a forward declaration in its headers
    /// (`struct moq_sub_object;` in `media_object.h`) refers to the lower
    /// tier's definition rather than declaring a second, opaque one.
    pub fn type_names(&self) -> impl Iterator<Item = &str> {
        self.records
            .keys()
            .chain(self.aliases.keys())
            .map(String::as_str)
    }

    /// Add another tier's declarations.
    pub fn extend(&mut self, other: Derives) {
        self.records.extend(other.records);
        self.manual_default.extend(other.manual_default);
        self.aliases.extend(other.aliases);
    }

    /// Whether `name`, a type these bindings declare, has `wanted`; `None`
    /// for a name they do not declare.
    pub fn answer(&self, name: &str, wanted: Trait) -> Option<Answer> {
        self.answer_within(name, wanted, 16)
    }

    fn answer_within(&self, name: &str, wanted: Trait, depth: u32) -> Option<Answer> {
        if depth == 0 {
            return None;
        }
        if let Some(derived) = self.records.get(name) {
            return Some(if derived.contains(wanted.derive_name()) {
                Answer::Yes
            } else if wanted == Trait::Default && self.manual_default.contains(name) {
                Answer::Manually
            } else {
                Answer::No
            });
        }
        let target = self.aliases.get(name)?;
        self.target_answer(target, wanted, depth - 1)
    }

    fn target_answer(&self, target: &str, wanted: Trait, depth: u32) -> Option<Answer> {
        let target = target.trim();
        if target.starts_with('*') {
            // Raw pointers are Copy, Debug, Hash and PartialEq whatever they
            // point to; bindgen implements Default for their records by hand.
            return Some(if wanted == Trait::Default {
                Answer::Manually
            } else {
                Answer::Yes
            });
        }
        if target.starts_with("::std::option::Option") || target.starts_with("Option") {
            // An optional function pointer.
            return Some(Answer::Yes);
        }
        if let Some(element) = target.strip_prefix('[') {
            let (element, count) = element.trim_end_matches(']').rsplit_once(';')?;
            let count: usize = count.trim().trim_end_matches("usize").parse().ok()?;
            if wanted == Trait::Default && count > 32 {
                return Some(Answer::Manually);
            }
            return self.target_answer(element, wanted, depth);
        }
        let primitive = target.trim_start_matches("::");
        let primitive = primitive
            .strip_prefix("std::os::raw::")
            .or_else(|| primitive.strip_prefix("core::ffi::"))
            .unwrap_or(primitive);
        const PRIMITIVES: &[&str] = &[
            "c_char",
            "c_schar",
            "c_uchar",
            "c_short",
            "c_ushort",
            "c_int",
            "c_uint",
            "c_long",
            "c_ulong",
            "c_longlong",
            "c_ulonglong",
            "c_float",
            "c_double",
            "u8",
            "u16",
            "u32",
            "u64",
            "u128",
            "i8",
            "i16",
            "i32",
            "i64",
            "i128",
            "usize",
            "isize",
            "bool",
            "f32",
            "f64",
        ];
        if PRIMITIVES.contains(&primitive) {
            return Some(Answer::Yes);
        }
        self.answer_within(target, wanted, depth)
    }
}

/// The identifier `rest` starts with.
fn identifier(rest: &str) -> &str {
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(rest.len());
    &rest[..end]
}

/// When `rest` starts with the words `keywords` and then an identifier
/// (whitespace of any length between them), that identifier and the bytes
/// read through it.
fn item<'a>(rest: &'a str, keywords: &[&str]) -> Option<(&'a str, usize)> {
    let mut used = 0;
    for keyword in keywords {
        let at = &rest[used..];
        let skipped = at.len() - at.trim_start().len();
        if used > 0 && skipped == 0 {
            return None;
        }
        let at = &at[skipped..];
        if identifier(at) != *keyword {
            return None;
        }
        used += skipped + keyword.len();
    }
    let at = &rest[used..];
    let skipped = at.len() - at.trim_start().len();
    if skipped == 0 {
        return None;
    }
    let name = identifier(&at[skipped..]);
    if name.is_empty() {
        return None;
    }
    Some((name, used + skipped + name.len()))
}

/// When `rest` starts with `pub use`, the path up to the `;`, whitespace
/// collapsed, and the bytes read through the `;`.
fn use_item(rest: &str) -> Option<(String, usize)> {
    let (_, used) = item(rest, &["pub", "use"])?;
    let start = rest[..used]
        .rfind(char::is_whitespace)
        .map_or(0, |at| at + 1);
    let end = used + rest[used..].find(';')?;
    let path = rest[start..end]
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    Some((path, end + 1))
}

/// The length of the token `rest` starts with: an identifier, or one byte.
fn token_len(rest: &str) -> usize {
    let name = identifier(rest);
    if name.is_empty() {
        rest.chars().next().map_or(1, char::len_utf8)
    } else {
        name.len()
    }
}

/// The index of the quote closing the string literal that opens at `start`.
fn string_end(text: &str, start: usize) -> usize {
    let bytes = text.as_bytes();
    let mut j = start + 1;
    while j < bytes.len() {
        match bytes[j] {
            b'\\' => j += 2,
            b'"' => return j,
            _ => j += 1,
        }
    }
    bytes.len()
}

/// The attribute `rest` starts with (`#[...]` or `#![...]`, with any
/// whitespace after the `#`): the bytes it spans and its contents.
fn attribute(rest: &str) -> (usize, &str) {
    let bytes = rest.as_bytes();
    let Some(open) = rest.find('[') else {
        return (rest.len(), "");
    };
    let mut depth = 0;
    let mut j = open;
    while j < bytes.len() {
        match bytes[j] {
            b'"' => j = string_end(rest, j),
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    return (j + 1, &rest[open + 1..j]);
                }
            }
            _ => {}
        }
        j += 1;
    }
    (bytes.len(), &rest[open + 1..])
}

/// The traits of a `derive(...)` attribute's contents.
fn derive_list(attribute: &str) -> Option<Vec<String>> {
    let list = attribute.trim().strip_prefix("derive")?.trim();
    let list = list.strip_prefix('(')?.strip_suffix(')')?;
    Some(
        list.split(',')
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty())
            .collect(),
    )
}
