//! Kind parity: every action and event kind `session.h` defines has a Rust
//! variant of its own, read from the header in this source tree, so a kind
//! added to C without one fails here. Unknown kinds stay representable.

use std::path::PathBuf;

use moq5_core::{ActionKind, EventKind};

/// `#define MOQ_<prefix>NAME <decimal>u` lines of session.h: (name, code).
fn kinds(prefix: &str) -> Vec<(String, u32)> {
    let header =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../core/include/moq/session.h");
    let text = std::fs::read_to_string(&header)
        .unwrap_or_else(|e| panic!("{} must be readable: {e}", header.display()));
    text.lines()
        .filter_map(|line| {
            let rest = line.strip_prefix("#define ")?;
            let mut words = rest.split_whitespace();
            let name = words.next()?;
            let value = words.next()?;
            if !name.starts_with(prefix) || words.next().is_some() {
                return None;
            }
            let code = value.strip_suffix('u')?.parse().ok()?;
            Some((name.to_owned(), code))
        })
        .collect()
}

#[test]
fn every_action_kind_has_a_variant() {
    let kinds: Vec<_> = kinds("MOQ_ACTION_")
        .into_iter()
        .filter(|(name, _)| name != "MOQ_ACTION_DETAIL_MAX")
        .collect();
    assert_eq!(kinds.len(), 15, "{kinds:?}");
    for (name, code) in &kinds {
        let kind = ActionKind::from_code(*code);
        assert!(
            !matches!(kind, ActionKind::Other(_)),
            "{name} ({code}) has no variant"
        );
        assert_eq!(kind.code(), *code, "{name}");
    }
    assert_eq!(ActionKind::from_code(1000), ActionKind::Other(1000));
}

#[test]
fn every_event_kind_has_a_variant() {
    let kinds: Vec<_> = kinds("MOQ_EVENT_")
        .into_iter()
        .filter(|(name, _)| name != "MOQ_EVENT_DETAIL_MAX")
        .collect();
    assert_eq!(kinds.len(), 66, "{kinds:?}");
    assert_eq!(EventKind::known().count(), kinds.len());
    for (name, code) in &kinds {
        let kind = EventKind::from_code(*code);
        assert!(
            !matches!(kind, EventKind::Other(_)),
            "{name} ({code}) has no variant"
        );
        assert_eq!(kind.code(), *code, "{name}");
    }
    assert_eq!(EventKind::from_code(1000), EventKind::Other(1000));
}
