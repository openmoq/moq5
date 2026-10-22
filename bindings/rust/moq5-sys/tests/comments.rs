//! The headers' comments are the raw API's documentation: the lifecycle,
//! threading and ownership contracts the C headers state reach the generated
//! items, and the contracts a header states for all of its declarations at
//! once (rcbuf's shard and overflow rules) reach the crate documentation's
//! header preambles -- in the fresh generation and in the docs.rs snapshot.

const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings.rs"));
const SNAPSHOT: &str = include_str!("../docs/bindings.rs");
const GENERATED_PREAMBLES: &str = include_str!(concat!(env!("OUT_DIR"), "/preambles.md"));
const SNAPSHOT_PREAMBLES: &str = include_str!("../docs/preambles.md");

/// The documentation attached to `fn <name>(`: the text between the item's
/// `extern "C" {` opener and its declaration.
fn function_doc<'bindings>(bindings: &'bindings str, name: &str) -> &'bindings str {
    let declaration = format!("pub fn {name}(");
    let at = bindings
        .find(&declaration)
        .unwrap_or_else(|| panic!("`{name}` is declared"));
    let opener = bindings[..at]
        .rfind("extern \"C\" {")
        .unwrap_or_else(|| panic!("`{name}` is in an extern block"));
    &bindings[opener..at]
}

/// The documentation attached to field `<field>` of `struct <record>`.
fn field_doc<'bindings>(bindings: &'bindings str, record: &str, field: &str) -> &'bindings str {
    let declaration = format!("pub struct {record} {{");
    let start = bindings
        .find(&declaration)
        .unwrap_or_else(|| panic!("`{record}` is declared"));
    let body = &bindings[start..];
    let end = body
        .find("\n}")
        .unwrap_or_else(|| panic!("`{record}` closes"));
    let body = &body[..end];
    let field_at = body
        .find(&format!("pub {field}:"))
        .unwrap_or_else(|| panic!("`{record}.{field}` is declared"));
    let previous_field = body[..field_at].rfind("\n    pub ").map_or(0, |i| i + 1);
    &body[previous_field..field_at]
}

/// Representative contracts, one per family: endpoint lifecycle (teardown
/// precondition and the refusal with live attachments), buffer ownership
/// on the error path, and the cross-shard copy's reference to the header's
/// threading contract.
const CONTRACTS: &[(&str, &str)] = &[
    (
        "moq_endpoint_destroy",
        "Precondition: no live attachments and",
    ),
    ("moq_endpoint_stop", "MOQ_ERR_WRONG_STATE with live"),
    ("moq_rcbuf_wrap", "does not call `release`"),
    ("moq_endpoint_connect", "handshake complete asynchronously"),
    ("moq_rcbuf_clone", "blessed way to cross a shard boundary"),
];

/// What rcbuf.h states for every rcbuf operation in its preamble: the
/// shard rule for the non-atomic refcount and the overflow trap.
const RCBUF_PREAMBLE: &[&str] = &[
    "refcount operations (incref/decref) are\nNOT atomic",
    "All references to a given rcbuf must stay within one\nshard",
    "incref aborts if the refcount reaches\nUINT32_MAX",
];

#[test]
fn function_contracts_reach_the_generated_documentation() {
    for (name, phrase) in CONTRACTS {
        assert!(
            function_doc(GENERATED, name).contains(phrase),
            "the generated `{name}` carries its header comment ({phrase:?})"
        );
        assert!(
            function_doc(SNAPSHOT, name).contains(phrase),
            "the snapshot's `{name}` carries its header comment ({phrase:?})"
        );
    }
}

#[test]
fn field_contracts_reach_the_generated_documentation() {
    // bindgen names the struct by its C tag; the `_t` typedef aliases it.
    let phrase = "the track ended because its subscription completed";
    assert!(
        field_doc(GENERATED, "moq_media_track_event", "has_done_status").contains(phrase),
        "the generated `has_done_status` carries its header comment"
    );
    assert!(
        field_doc(SNAPSHOT, "moq_media_track_event", "has_done_status").contains(phrase),
        "the snapshot's `has_done_status` carries its header comment"
    );
}

#[test]
fn header_preambles_reach_the_crate_documentation() {
    for preambles in [GENERATED_PREAMBLES, SNAPSHOT_PREAMBLES] {
        assert!(
            preambles.contains("## moq/rcbuf.h"),
            "the preambles name rcbuf.h"
        );
        for phrase in RCBUF_PREAMBLE {
            assert!(
                preambles.contains(phrase),
                "the rcbuf.h preamble reaches the crate documentation ({phrase:?})"
            );
        }
    }
}
