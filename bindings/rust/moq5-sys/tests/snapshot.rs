//! The docs.rs snapshot matches a fresh generation: `docs/bindings.rs`, past
//! its provenance header, holds what the build script generated from the
//! installed headers on this target, and `docs/preambles.md` the header
//! preambles it extracted. With the `player` feature, so do
//! `docs/bindings_player.rs` and `docs/preambles_player.md`; the base pair
//! is the same with the feature and without it. The row runs on every
//! qualified target, which is what lets the manifests restrict docs.rs to
//! them.
//!
//! The bindings match line for line: every declaration, attribute and
//! layout assertion, and every documentation attribute, with one exception.
//! A constant bindgen made from a macro may carry its documentation on one
//! side only: libclang 23 attaches the comment before a macro definition to
//! the macro under `-fparse-all-comments`, and earlier releases attach none,
//! so no snapshot can equal both generations byte for byte. Where both sides
//! document such a constant, the text must agree. The build script lists the
//! constants each generation made from macros (`macros.txt` and
//! `macros_player.txt` beside the bindings), and the list is held to name
//! no declaration. The preambles match byte for byte.

use std::collections::BTreeSet;

const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings.rs"));
const SNAPSHOT: &str = include_str!("../docs/bindings.rs");
const MACROS: &str = include_str!(concat!(env!("OUT_DIR"), "/macros.txt"));
const WRAPPER: &str = include_str!("../wrapper.h");
const GENERATED_PREAMBLES: &str = include_str!(concat!(env!("OUT_DIR"), "/preambles.md"));
const SNAPSHOT_PREAMBLES: &str = include_str!("../docs/preambles.md");
const LIBCLANG: &str = include_str!(concat!(env!("OUT_DIR"), "/libclang.txt"));

/// A snapshot without its leading `//` provenance lines and the blank line
/// after them.
fn snapshot_body(snapshot: &str) -> &str {
    let mut rest = snapshot;
    loop {
        let line_end = rest.find('\n').map_or(rest.len(), |i| i + 1);
        let line = &rest[..line_end];
        if line.starts_with("//") || line.trim().is_empty() {
            rest = &rest[line_end..];
            if rest.is_empty() {
                return rest;
            }
        } else {
            return rest;
        }
    }
}

/// The provenance line naming the snapshot's generator.
fn provenance(snapshot: &str) -> &str {
    snapshot
        .lines()
        .find_map(|line| line.strip_prefix("// generated on: "))
        .unwrap_or("(no provenance line)")
}

/// One line of bindings with the documentation attributes right above it.
#[derive(Debug)]
struct Item<'a> {
    /// The line's number in its text, from 1.
    number: usize,
    docs: Vec<&'a str>,
    text: &'a str,
}

/// A bindings text as its non-documentation lines, each with the
/// documentation attributes before it.
fn items(text: &str) -> Vec<Item<'_>> {
    let mut items = Vec::new();
    let mut docs = Vec::new();
    let mut count = 0;
    for (i, line) in text.lines().enumerate() {
        count = i + 1;
        if line.trim_start().starts_with("#[doc = ") {
            docs.push(line);
        } else {
            items.push(Item {
                number: i + 1,
                docs: std::mem::take(&mut docs),
                text: line,
            });
        }
    }
    if !docs.is_empty() {
        // Documentation with no line after it, as an item of its own.
        items.push(Item {
            number: count + 1,
            docs,
            text: "",
        });
    }
    items
}

/// The constant a line declares, if it declares one.
fn constant_name(line: &str) -> Option<&str> {
    line.strip_prefix("pub const ")?.split(':').next()
}

/// The first difference between a snapshot body and a generation that the
/// contract above does not allow, described; `None` when they match.
fn difference(snapshot: &str, generated: &str, macros: &BTreeSet<&str>) -> Option<String> {
    let ours = items(snapshot);
    let theirs = items(generated);
    for i in 0..ours.len().max(theirs.len()) {
        let (ours, theirs) = (ours.get(i), theirs.get(i));
        let matched = match (ours, theirs) {
            (Some(a), Some(b)) if a.text == b.text => {
                a.docs == b.docs
                    || ((a.docs.is_empty() || b.docs.is_empty())
                        && constant_name(a.text).is_some_and(|name| macros.contains(name)))
            }
            _ => false,
        };
        if !matched {
            return Some(format!(
                "snapshot {}\ngenerated {}",
                describe(ours),
                describe(theirs)
            ));
        }
    }
    None
}

fn describe(item: Option<&Item<'_>>) -> String {
    match item {
        None => "(past its end)".to_owned(),
        Some(item) => {
            let mut text = format!("line {}:\n", item.number);
            for line in item.docs.iter().chain(std::iter::once(&item.text)) {
                text.push_str("    ");
                text.push_str(line);
                text.push('\n');
            }
            text
        }
    }
}

/// A snapshot matches its fresh generation, or the first difference is
/// shown with both sides and both generators named.
fn assert_bindings_match(
    name: &str,
    snapshot: &'static str,
    generated: &str,
    generated_file: &str,
    macros: &str,
) {
    assert!(
        generated.starts_with("/* automatically generated by rust-bindgen"),
        "the generated file starts with bindgen's banner"
    );
    let macros: BTreeSet<&str> = macros.lines().collect();
    if let Some(difference) = difference(snapshot_body(snapshot), generated, &macros) {
        panic!(
            "docs/{name} differs from the generated bindings beyond a macro constant's \
             documentation:\n\n{difference}\nThe snapshot was generated on {}; this build used \
             {LIBCLANG} and wrote {}/{generated_file}. Regenerate the snapshot with \
             scripts/update_snapshot.sh <sdk-prefix> once the difference is one the headers \
             or the generator should make.",
            provenance(snapshot),
            env!("OUT_DIR"),
        );
    }
}

/// The build script's list names constants bindgen made from macros and no
/// declaration: no static the wrapper restates and no enumerator (a
/// constant of an enum's type). `expected` are macro constants it must name,
/// a result code among them: bindgen asks the last registered callback first,
/// and the one typing the result codes answers for them, so the list sees
/// them only from a callback registered after that one.
fn assert_macro_list(macros: &str, generated: &str, wrapper: &str, expected: &[&str]) {
    let macros: BTreeSet<&str> = macros.lines().collect();
    let restated: Vec<&str> = wrapper
        .lines()
        .filter_map(|line| {
            let (_, name) = line.strip_prefix("MOQ5_SYSVAL(")?.split_once(", ")?;
            name.strip_suffix(");")
        })
        .collect();
    assert!(
        !restated.is_empty(),
        "no MOQ5_SYSVAL line was found in the wrapper"
    );
    for name in restated {
        assert!(
            !macros.contains(name),
            "{name}, a restated static, is listed as a macro constant"
        );
    }
    for line in generated.lines() {
        if let Some((name, ty)) = line
            .strip_prefix("pub const ")
            .and_then(|rest| rest.split_once(": "))
        {
            assert!(
                !(macros.contains(name) && ty.starts_with("moq_")),
                "{name}, an enumerator, is listed as a macro constant"
            );
        }
    }
    for name in expected {
        assert!(
            macros.contains(name),
            "{name}, a macro constant, is missing from the list"
        );
    }
}

fn assert_preambles_equal(name: &str, snapshot: &str, generated: &str) {
    if snapshot != generated {
        let line = snapshot
            .lines()
            .zip(generated.lines())
            .position(|(a, b)| a != b)
            .map_or(
                snapshot.lines().count().min(generated.lines().count()),
                |i| i + 1,
            );
        panic!(
            "docs/{name} differs from the generated preambles at line {line}; \
             regenerate it with scripts/update_snapshot.sh <sdk-prefix>"
        );
    }
}

#[test]
fn snapshot_matches_the_generated_bindings() {
    assert_bindings_match("bindings.rs", SNAPSHOT, GENERATED, "bindings.rs", MACROS);
}

#[test]
fn the_macro_list_names_only_macro_constants() {
    assert_macro_list(
        MACROS,
        GENERATED,
        WRAPPER,
        &[
            "MOQ_OK",
            "MOQ_ERR_INTERRUPTED",
            "MOQ_HANDLE_POOL_SUBSCRIPTION",
            "MOQ_VERSION_STRING",
        ],
    );
}

#[test]
fn snapshot_preambles_match_the_generated_preambles() {
    assert_preambles_equal("preambles.md", SNAPSHOT_PREAMBLES, GENERATED_PREAMBLES);
}

#[cfg(feature = "player")]
mod player {
    const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings_player.rs"));
    const SNAPSHOT: &str = include_str!("../docs/bindings_player.rs");
    const MACROS: &str = include_str!(concat!(env!("OUT_DIR"), "/macros_player.txt"));
    const WRAPPER: &str = include_str!("../wrapper_player.h");
    const GENERATED_PREAMBLES: &str =
        include_str!(concat!(env!("OUT_DIR"), "/preambles_player.md"));
    const SNAPSHOT_PREAMBLES: &str = include_str!("../docs/preambles_player.md");

    #[test]
    fn snapshot_matches_the_generated_player_bindings() {
        super::assert_bindings_match(
            "bindings_player.rs",
            SNAPSHOT,
            GENERATED,
            "bindings_player.rs",
            MACROS,
        );
    }

    #[test]
    fn the_player_macro_list_names_only_macro_constants() {
        super::assert_macro_list(
            MACROS,
            GENERATED,
            WRAPPER,
            &["MOQ_PLAYBACK_EVENT_GAP_CONTINUED"],
        );
    }

    #[test]
    fn snapshot_preambles_match_the_generated_player_preambles() {
        super::assert_preambles_equal(
            "preambles_player.md",
            SNAPSHOT_PREAMBLES,
            GENERATED_PREAMBLES,
        );
    }
}

/// The comparison itself, on small bindings: only a macro constant's
/// documentation may be on one side only.
mod contract {
    use std::collections::BTreeSet;

    use super::difference;

    const BINDINGS: &str = r#"/* automatically generated by rust-bindgen 0.73.2 */

#[doc = " A constant made from a macro."]
pub const MOQ_LIMIT: u32 = 4096;
#[doc = " An enumerator."]
pub const MOQ_KIND_A: moq_kind = 0;
pub type moq_kind = ::std::os::raw::c_uint;
#[repr(C)]
#[derive(Debug, Default, Copy, Clone)]
pub struct moq_pair {
    #[doc = " The first field."]
    pub first: u32,
    pub second: u64,
}
#[allow(clippy::unnecessary_operation, clippy::identity_op)]
const _: () = {
    ["Size of moq_pair"][::std::mem::size_of::<moq_pair>() - 16usize];
    ["Offset of field: moq_pair::second"][::std::mem::offset_of!(moq_pair, second) - 8usize];
};
unsafe extern "C" {
    #[doc = " Does nothing."]
    pub fn moq_nothing() -> u32;
}
"#;

    fn macros() -> BTreeSet<&'static str> {
        ["MOQ_LIMIT"].into_iter().collect()
    }

    /// The bindings with one exact substitution.
    fn changed(from: &str, to: &str) -> String {
        assert_eq!(BINDINGS.matches(from).count(), 1, "{from:?} occurs once");
        BINDINGS.replacen(from, to, 1)
    }

    #[test]
    fn identical_bindings_match() {
        assert_eq!(difference(BINDINGS, BINDINGS, &macros()), None);
    }

    #[test]
    fn a_macro_constant_documented_on_one_side_matches() {
        let undocumented = changed("#[doc = \" A constant made from a macro.\"]\n", "");
        assert_eq!(difference(BINDINGS, &undocumented, &macros()), None);
        assert_eq!(difference(&undocumented, BINDINGS, &macros()), None);
    }

    #[test]
    fn a_macro_constant_documented_differently_is_refused() {
        let other = changed("A constant made from a macro.", "Another text.");
        assert!(difference(BINDINGS, &other, &macros()).is_some());
    }

    #[test]
    fn an_enumerator_documented_on_one_side_is_refused() {
        let undocumented = changed("#[doc = \" An enumerator.\"]\n", "");
        assert!(difference(BINDINGS, &undocumented, &macros()).is_some());
    }

    #[test]
    fn a_constant_not_named_a_macro_is_held_to_its_documentation() {
        let undocumented = changed("#[doc = \" A constant made from a macro.\"]\n", "");
        assert!(difference(BINDINGS, &undocumented, &BTreeSet::new()).is_some());
    }

    #[test]
    fn a_changed_constant_value_is_refused() {
        let other = changed("MOQ_LIMIT: u32 = 4096;", "MOQ_LIMIT: u32 = 4097;");
        assert!(difference(BINDINGS, &other, &macros()).is_some());
    }

    #[test]
    fn a_changed_constant_type_is_refused() {
        let other = changed("MOQ_LIMIT: u32 = 4096;", "MOQ_LIMIT: u64 = 4096;");
        assert!(difference(BINDINGS, &other, &macros()).is_some());
    }

    #[test]
    fn a_changed_field_type_is_refused() {
        let other = changed("pub second: u64,", "pub second: u32,");
        assert!(difference(BINDINGS, &other, &macros()).is_some());
    }

    #[test]
    fn a_changed_layout_assertion_is_refused() {
        let other = changed(
            "size_of::<moq_pair>() - 16usize",
            "size_of::<moq_pair>() - 24usize",
        );
        assert!(difference(BINDINGS, &other, &macros()).is_some());
    }

    #[test]
    fn a_changed_field_documentation_is_refused() {
        let other = changed("The first field.", "The second field.");
        assert!(difference(BINDINGS, &other, &macros()).is_some());
    }

    #[test]
    fn a_dropped_function_documentation_is_refused() {
        let other = changed("    #[doc = \" Does nothing.\"]\n", "");
        assert!(difference(BINDINGS, &other, &macros()).is_some());
    }

    #[test]
    fn a_removed_declaration_is_refused() {
        let other = changed("    pub fn moq_nothing() -> u32;\n", "");
        assert!(difference(BINDINGS, &other, &macros()).is_some());
        assert!(difference(&other, BINDINGS, &macros()).is_some());
    }

    #[test]
    fn the_report_shows_both_sides() {
        let other = changed("MOQ_LIMIT: u32 = 4096;", "MOQ_LIMIT: u32 = 4097;");
        let report = difference(BINDINGS, &other, &macros()).expect("the bindings differ");
        assert!(report.contains("snapshot line 4:"), "{report}");
        assert!(
            report.contains("pub const MOQ_LIMIT: u32 = 4096;"),
            "{report}"
        );
        assert!(report.contains("generated line 4:"), "{report}");
        assert!(
            report.contains("pub const MOQ_LIMIT: u32 = 4097;"),
            "{report}"
        );
    }
}
