//! Safe Rust API for the sans-I/O core of libmoq, the MOQ5 Media over QUIC
//! library.
//!
//! This crate wraps the core tier: the session state machine and the
//! vocabulary every tier shares. It holds no socket, thread or clock; a
//! consumer that owns its transport drives a session through it and takes
//! on the host contract the core documents: feeding input, draining actions
//! and events, custody of refused input, and time. Media applications
//! normally use the service crate (`moq5`), which re-exports the shared
//! names below.
//!
//! The shared vocabulary:
//!
//! - [`Buffer`]: an exclusively owned libmoq refcounted buffer.
//! - [`Error`] and [`Result`]: every libmoq result code, unknown ones kept.
//! - [`Namespace`]: a track namespace, the zero-part root included.
//! - [`Version`], [`Location`], [`LocationFilter`], [`GroupOrder`] and
//!   [`ObjectStatus`]: protocol values in their native domain.
//!
//! The raw C API stays available as [`sys`] (the `moq5-sys` crate, with its
//! default core tier), whose header comments are the contract. Each shared
//! type converts to and from its raw form through narrow, documented
//! methods (`as_raw`/`from_raw`, `code`/`from_code`): producing a raw value
//! is safe, and using one is the caller's `unsafe` call.
//!
//! # Linking
//!
//! `moq5-sys` finds the installed SDK through pkg-config: with only this
//! crate in the graph it probes `libmoq-core` and links the core library
//! alone. See its documentation for `MOQ_PREFIX` and the tier features.

pub use moq5_sys as sys;

mod buffer;
mod error;
mod ffi;
mod namespace;
mod session;
mod types;

pub use buffer::{Buffer, SliceError};
pub use error::{Error, Result};
pub use namespace::Namespace;
pub use session::*;
pub use types::{libmoq_version, GroupOrder, Location, LocationFilter, ObjectStatus, Version};
