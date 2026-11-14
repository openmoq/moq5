//! Safe Rust API for the media parsers of libmoq, the MOQ5 Media over QUIC
//! library.
//!
//! The media tier is stateless and sans-I/O: pure functions and value types
//! over bytes the application holds.
//!
//! - [`cmaf`]: CMAF init segments (ftyp+moov) and media fragments
//!   (moof+mdat), and CMSF object packaging checks.
//! - LOC-04 object metadata: what a sender writes with a draft-22 object
//!   ([`LocMetadata`], [`Marking`]) and what a receiver reports for one
//!   ([`ObjectMetadata`]).
//! - The MPEG-TS catalog record of draft-gregoire-moq-msfts: what a track
//!   declares ([`Mpeg2tsRecord`]) and what a received catalog carries
//!   ([`Mpeg2tsRecordRef`], [`TrackExtension`]).
//! - The media value types: [`MediaType`], [`Packaging`], [`SapType`],
//!   [`CompanionProfile`] and [`TimeDomain`].
//!
//! Errors are the core crate's [`moq5_core::Error`]. The service crate
//! (`moq5`) re-exports every name here. The raw C API stays available as
//! [`sys`] (the `moq5-sys` crate, with its `media` tier). Each type converts
//! to and from its raw form through narrow, documented methods: producing a
//! raw value is safe, and a conversion that reads through a raw record's
//! pointers is `unsafe`, with the caller's obligations stated.
//!
//! # Linking
//!
//! `moq5-sys` finds the installed SDK through pkg-config: with this crate
//! and the core crate in the graph it probes `libmoq-media`, which links the
//! media parsers and the core and nothing above them.

pub use moq5_sys as sys;

pub mod cmaf;
mod ffi;
mod metadata;
mod mpeg2ts;
mod types;

pub use metadata::{LocMetadata, Marking, ObjectMetadata};
pub use mpeg2ts::{
    Mpeg2tsKeys, Mpeg2tsMode, Mpeg2tsRecord, Mpeg2tsRecordRef, Mpeg2tsTimestampMode, TrackExtension,
};
pub use types::{CompanionProfile, MediaType, Packaging, SapType, TimeDomain};
