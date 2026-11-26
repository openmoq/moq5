//! The headers' comments are the raw API's documentation: the lifecycle,
//! threading and ownership contracts the C headers state reach the generated
//! items of each tier, and the contracts a header states for all of its
//! declarations at once (rcbuf's shard and overflow rules) reach the crate
//! documentation's header preambles -- in the fresh generation and in the
//! docs.rs snapshot.

const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings_core.rs"));
const SNAPSHOT: &str = include_str!("../docs/bindings_core.rs");
const GENERATED_PREAMBLES: &str = include_str!(concat!(env!("OUT_DIR"), "/preambles_core.md"));
const SNAPSHOT_PREAMBLES: &str = include_str!("../docs/preambles_core.md");

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
#[cfg(feature = "service")]
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

/// Representative core contracts: buffer ownership on the error path, the
/// cross-shard copy's reference to the header's threading contract, the
/// session's ownership, the public host operation that routes a peer
/// stream, and both facades' event dispatch.
const CONTRACTS: &[(&str, &str)] = &[
    ("moq_rcbuf_wrap", "does not call `release`"),
    ("moq_rcbuf_clone", "blessed way to cross a shard boundary"),
    (
        "moq_session_create",
        "Ownership: caller owns *out until moq_session_destroy()",
    ),
    (
        "moq_session_route_peer_uni",
        "Say where the bytes of a peer-opened unidirectional stream belong",
    ),
    (
        "moq_pub_tick",
        "Process session events and retry pending operations.",
    ),
    (
        "moq_sub_tick",
        "Process session events. Dispatches subscribe, object, fetch,",
    ),
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

/// The simulator's preamble, in its own generation and snapshot: the
/// harness is separate from the production core.
#[cfg(feature = "sim")]
mod sim {
    const GENERATED_PREAMBLES: &str = include_str!(concat!(env!("OUT_DIR"), "/preambles_sim.md"));
    const SNAPSHOT_PREAMBLES: &str = include_str!("../docs/preambles_sim.md");

    #[test]
    fn sim_preambles_reach_the_crate_documentation() {
        for preambles in [GENERATED_PREAMBLES, SNAPSHOT_PREAMBLES] {
            assert!(preambles.contains("## moq/sim.h"));
            assert!(
                preambles.contains("Deterministic simulation helpers for libmoq."),
                "the sim.h preamble reaches the crate documentation"
            );
        }
    }
}

/// A media parser's sized-output contract, in the media tier's generation
/// and snapshot.
#[cfg(feature = "media")]
mod media {
    const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings_media.rs"));
    const SNAPSHOT: &str = include_str!("../docs/bindings_media.rs");

    #[test]
    fn media_contracts_reach_the_generated_documentation() {
        let (name, phrase) = (
            "moq_cmaf_parse_init_sized",
            "Parse the first trak from a CMAF init segment",
        );
        for (bindings, which) in [(GENERATED, "generated"), (SNAPSHOT, "snapshot")] {
            assert!(
                super::function_doc(bindings, name).contains(phrase),
                "the {which} `{name}` carries its header comment ({phrase:?})"
            );
        }
    }
}

/// The LOCMAF tier's contracts, in its own generation and snapshot: what a
/// decoded chunk borrows and for how long, how long a decoder keeps its
/// storage, and what a commit adopts.
#[cfg(feature = "locmaf")]
mod locmaf {
    const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings_locmaf.rs"));
    const SNAPSHOT: &str = include_str!("../docs/bindings_locmaf.rs");
    const GENERATED_PREAMBLES: &str =
        include_str!(concat!(env!("OUT_DIR"), "/preambles_locmaf.md"));
    const SNAPSHOT_PREAMBLES: &str = include_str!("../docs/preambles_locmaf.md");

    #[test]
    fn locmaf_contracts_reach_the_generated_documentation() {
        for (name, phrase) in [
            (
                "moq_locmaf_decoder_push",
                "decoder's storage until the next push, reset or copy into this decoder",
            ),
            (
                "moq_locmaf_decoder_init",
                "storage until it is re-initialized or no longer used",
            ),
            (
                "moq_locmaf_encoder_commit",
                "Adopt what the last successful encode staged",
            ),
        ] {
            for (bindings, which) in [(GENERATED, "generated"), (SNAPSHOT, "snapshot")] {
                assert!(
                    super::function_doc(bindings, name).contains(phrase),
                    "the {which} `{name}` carries its header comment ({phrase:?})"
                );
            }
        }
    }

    #[test]
    fn locmaf_preambles_reach_the_crate_documentation() {
        for preambles in [GENERATED_PREAMBLES, SNAPSHOT_PREAMBLES] {
            assert!(preambles.contains("## moq/locmaf.h"));
            assert!(preambles.contains("Sans-I/O and allocation-free."));
        }
    }
}

/// The service tier's contracts, in its own generation and snapshot:
/// endpoint lifecycle (teardown precondition and the refusal with live
/// attachments) and a receiver record's field.
#[cfg(feature = "service")]
mod service {
    const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings_service.rs"));
    const SNAPSHOT: &str = include_str!("../docs/bindings_service.rs");

    const CONTRACTS: &[(&str, &str)] = &[
        (
            "moq_endpoint_destroy",
            "Precondition: no live attachments and",
        ),
        ("moq_endpoint_stop", "MOQ_ERR_WRONG_STATE with live"),
        ("moq_endpoint_connect", "handshake complete asynchronously"),
    ];

    #[test]
    fn service_contracts_reach_the_generated_documentation() {
        for (name, phrase) in CONTRACTS {
            for (bindings, which) in [(GENERATED, "generated"), (SNAPSHOT, "snapshot")] {
                assert!(
                    super::function_doc(bindings, name).contains(phrase),
                    "the {which} `{name}` carries its header comment ({phrase:?})"
                );
            }
        }
    }

    #[test]
    fn field_contracts_reach_the_generated_documentation() {
        // bindgen names the struct by its C tag; the `_t` typedef aliases it.
        let phrase = "the track ended because its subscription completed";
        for (bindings, which) in [(GENERATED, "generated"), (SNAPSHOT, "snapshot")] {
            assert!(
                super::field_doc(bindings, "moq_media_track_event", "has_done_status")
                    .contains(phrase),
                "the {which} `has_done_status` carries its header comment"
            );
        }
    }
}

/// The player's ownership contracts, in its own generation and snapshot:
/// attachment takes the receiver's queues, an output is never truncated
/// because it owns references, and an output's release outlives the player.
#[cfg(feature = "player")]
mod player {
    const GENERATED: &str = include_str!(concat!(env!("OUT_DIR"), "/bindings_player.rs"));
    const SNAPSHOT: &str = include_str!("../docs/bindings_player.rs");
    const GENERATED_PREAMBLES: &str =
        include_str!(concat!(env!("OUT_DIR"), "/preambles_player.md"));
    const SNAPSHOT_PREAMBLES: &str = include_str!("../docs/preambles_player.md");

    const CONTRACTS: &[(&str, &str)] = &[
        (
            "moq_media_player_attach",
            "becomes the sole consumer of every receive queue",
        ),
        (
            "moq_media_player_poll_output",
            "a record owns refs, so it is never",
        ),
        ("moq_media_player_output_cleanup", "legal after the player"),
    ];

    #[test]
    fn player_contracts_reach_the_generated_documentation() {
        for (name, phrase) in CONTRACTS {
            for (bindings, which) in [(GENERATED, "generated"), (SNAPSHOT, "snapshot")] {
                assert!(
                    super::function_doc(bindings, name).contains(phrase),
                    "the {which} `{name}` carries its header comment ({phrase:?})"
                );
            }
        }
    }

    #[test]
    fn player_preambles_reach_the_crate_documentation() {
        for preambles in [GENERATED_PREAMBLES, SNAPSHOT_PREAMBLES] {
            assert!(preambles.contains("## moq/media_player.h"));
            assert!(
                preambles.contains("Threading: the player owns no thread."),
                "the media_player.h preamble reaches the crate documentation"
            );
        }
    }
}
