//! Raw, bindgen-generated bindings to the libmoq service tier.
//!
//! The declarations come from the installed headers at build time, so they
//! match the SDK this crate links against; `cfg(docsrs)` compiles the
//! committed snapshot in `docs/` instead. The header comments are the
//! contract: each declaration's comment is its item's documentation, and the
//! leading comment of each header follows below, since what a header states
//! for all of its declarations (the rcbuf shard and overflow rules) is
//! attached to no single item.
//!
//! The `player` feature adds the media player (`media_player.h`) and the
//! playback engine configuration it takes; it needs an SDK built with the
//! player (`MOQ_BUILD_PLAYBACK=ON`).
//!
#![cfg_attr(not(docsrs), doc = include_str!(concat!(env!("OUT_DIR"), "/preambles.md")))]
#![cfg_attr(docsrs, doc = include_str!("../docs/preambles.md"))]
#![cfg_attr(
    all(not(docsrs), feature = "player"),
    doc = include_str!(concat!(env!("OUT_DIR"), "/preambles_player.md"))
)]
#![cfg_attr(
    all(docsrs, feature = "player"),
    doc = include_str!("../docs/preambles_player.md")
)]
#![allow(non_camel_case_types, non_snake_case, non_upper_case_globals)]
#![allow(clippy::all)]
#![allow(rustdoc::all)]

#[cfg(not(docsrs))]
include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[cfg(docsrs)]
include!("../docs/bindings.rs");

#[cfg(all(not(docsrs), feature = "player"))]
include!(concat!(env!("OUT_DIR"), "/bindings_player.rs"));

#[cfg(all(docsrs, feature = "player"))]
include!("../docs/bindings_player.rs");
