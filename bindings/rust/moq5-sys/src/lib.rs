//! Raw, bindgen-generated bindings to libmoq, by tier.
//!
//! The default is the core tier: the sans-I/O session, its vocabulary, and
//! the publisher and subscriber facades. The features add tiers, each
//! generated into its own file so a tier's declarations are the same
//! whichever others are enabled:
//!
//! - `sim`: the deterministic simulator (`sim.h`; an SDK built with
//!   `MOQ_BUILD_SIM=ON`);
//! - `media`: the LOC, CMAF, media-object and MSF parsers;
//! - `service` (implies `media`): the managed endpoint, media sender and
//!   media receiver;
//! - `player` (implies `service`): the media player and the playback engine
//!   configuration it takes (an SDK built with `MOQ_BUILD_PLAYBACK=ON`).
//!
//! The declarations come from the installed headers at build time, so they
//! match the SDK this crate links against; `cfg(docsrs)` compiles the
//! committed snapshot of each enabled tier in `docs/` instead. The header
//! comments are the contract: each declaration's comment is its item's
//! documentation, and the leading comment of each header follows below,
//! since what a header states for all of its declarations (the rcbuf shard
//! and overflow rules) is attached to no single item.
//!
#![cfg_attr(not(docsrs), doc = include_str!(concat!(env!("OUT_DIR"), "/preambles_core.md")))]
#![cfg_attr(docsrs, doc = include_str!("../docs/preambles_core.md"))]
#![cfg_attr(
    all(not(docsrs), feature = "sim"),
    doc = include_str!(concat!(env!("OUT_DIR"), "/preambles_sim.md"))
)]
#![cfg_attr(all(docsrs, feature = "sim"), doc = include_str!("../docs/preambles_sim.md"))]
#![cfg_attr(
    all(not(docsrs), feature = "media"),
    doc = include_str!(concat!(env!("OUT_DIR"), "/preambles_media.md"))
)]
#![cfg_attr(all(docsrs, feature = "media"), doc = include_str!("../docs/preambles_media.md"))]
#![cfg_attr(
    all(not(docsrs), feature = "service"),
    doc = include_str!(concat!(env!("OUT_DIR"), "/preambles_service.md"))
)]
#![cfg_attr(
    all(docsrs, feature = "service"),
    doc = include_str!("../docs/preambles_service.md")
)]
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
include!(concat!(env!("OUT_DIR"), "/bindings_core.rs"));

#[cfg(docsrs)]
include!("../docs/bindings_core.rs");

#[cfg(all(not(docsrs), feature = "sim"))]
include!(concat!(env!("OUT_DIR"), "/bindings_sim.rs"));

#[cfg(all(docsrs, feature = "sim"))]
include!("../docs/bindings_sim.rs");

#[cfg(all(not(docsrs), feature = "media"))]
include!(concat!(env!("OUT_DIR"), "/bindings_media.rs"));

#[cfg(all(docsrs, feature = "media"))]
include!("../docs/bindings_media.rs");

#[cfg(all(not(docsrs), feature = "service"))]
include!(concat!(env!("OUT_DIR"), "/bindings_service.rs"));

#[cfg(all(docsrs, feature = "service"))]
include!("../docs/bindings_service.rs");

#[cfg(all(not(docsrs), feature = "player"))]
include!(concat!(env!("OUT_DIR"), "/bindings_player.rs"));

#[cfg(all(docsrs, feature = "player"))]
include!("../docs/bindings_player.rs");
