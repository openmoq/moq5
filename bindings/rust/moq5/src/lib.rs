//! Safe Rust API for libmoq, the MOQ5 Media over QUIC library.
//!
//! This crate wraps libmoq's *service tier*: a managed [`Endpoint`]
//! (connection, MoQ session and network thread), a [`Sender`] that
//! publishes a namespace with an MSF/CMSF catalog, and a [`Receiver`] that
//! discovers and receives one. The raw C API stays available as [`sys`]
//! (the `moq5-sys` crate), whose header comments are the contract; its
//! generated names are used qualified (`sys::moq_endpoint_cfg_t`) rather
//! than imported, as one large group of generated types.
//!
//! What the types enforce that C leaves to documentation:
//!
//! - **Teardown order.** Senders and receivers hold their endpoint, and the
//!   endpoint is stopped and destroyed only when its last handle drops.
//! - **Handle ownership.** Track handles are checked against the sender or
//!   receiver that issued them; a foreign one fails with [`Error::INVAL`]
//!   before reaching libmoq.
//! - **Buffer ownership.** [`Sender::write`] takes a [`Buffer`] and hands it
//!   back on failure; received [`MediaObject`]s release their buffers on
//!   drop, on the polling thread.
//! - **Sized records.** Every configuration is built with the library's
//!   sized initializer, so every field reaches the service; every sized
//!   output is read under the `struct_size` the library stamped, a field it
//!   did not report is `None`, and a stamp outside the contract is
//!   [`Error::ABI_MISMATCH`].
//! - **Snapshots.** A [`TrackDescription`] is the track's current state,
//!   copied under the receiver's lock; nothing reads the mutable native
//!   description concurrently with the service.
//! - **Threading.** [`Endpoint`] is `Send + Sync` (interrupting from another
//!   thread is the point of its latch); senders and receivers are `Send` but
//!   not `Sync`; media objects stay on the polling thread; buffers move.
//!
//! **No application code runs on the service's threads.** This crate
//! installs no C callbacks, posts no tasks, and adopts no buffer whose
//! release runs application code: a buffer release only deallocates. So the
//! last [`Endpoint`] drop always happens on an application thread, and
//! `stop()` never joins the thread it runs on. Anything that would run
//! application code on a native thread (a raw endpoint from
//! [`Endpoint::from_raw`], a release given to [`Buffer::wrap_raw`]) is
//! `unsafe` and re-establishes that invariant itself.
//!
//! Demand is observed, not signalled: [`Sender::is_ready`] and the
//! subscriber queries are point-in-time snapshots, and [`Sender::wait`] waits
//! for the write level (ready with send-queue headroom), not for a
//! subscriber.
//!
//! The public enums that mirror a native set and the records the library
//! stamps are `#[non_exhaustive]`: they grow with the C API, and an unknown
//! native value is carried as an `Other(raw)` variant.
//!
//! ```no_run
//! use moq5::{
//!     Buffer, Endpoint, EndpointConfig, MediaType, Namespace, Packaging, SendObject,
//!     SendTrackConfig, Sender, SenderConfig,
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let endpoint = Endpoint::connect(&EndpointConfig::new("https://relay.example:4443/moq"))?;
//! let sender = Sender::attach(&endpoint, &SenderConfig::live(Namespace::parse("live/demo")?))?;
//! let video = sender.add_track(
//!     &SendTrackConfig::new("video", MediaType::Video, Packaging::Raw, "avc1.42e01e")
//!         .bitrate(2_000_000),
//! )?;
//! let mut object = SendObject::new(Buffer::from_vec(vec![0u8; 100])?);
//! object.is_sync = true;
//! object.starts_group = true;
//! sender.write(video, object)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Linking
//!
//! `moq5-sys` finds the installed SDK through pkg-config (`libmoq-service`);
//! see its documentation for `MOQ_PREFIX` and the supported SDK builds.

#![warn(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

pub use moq5_sys as sys;

mod buffer;
pub mod cmaf;
mod endpoint;
mod error;
mod ffi;
mod namespace;
mod receiver;
mod sender;
mod types;

pub use buffer::Buffer;
pub use endpoint::{
    AuthToken, Backend, Endpoint, EndpointConfig, EndpointState, Protocol, Terminal,
    TerminalReason, UriMode, VersionOffer, WtProfile,
};
pub use error::{Error, Result};
pub use namespace::Namespace;
pub use receiver::{
    ContentProtectionRef, FillLoss, FillTerminal, LossReport, MediaObject, MediaTemplate,
    ObjectIdentity, ParseDrop, ParseDropClass, Receiver, ReceiverConfig, ReceiverStats, SapRecord,
    StartMode, StreamTerminal, StreamTerminalKind, SubscribeOptions, TimeMode, TimelineRecord,
    Track, TrackDescription, TrackEvent, TrackEventKind, TrackState,
};
pub use sender::{
    Backpressure, CmsfUrl, ContentProtection, DrmSystem, SendObject, SendTrack, SendTrackConfig,
    Sender, SenderConfig, SenderStats, WriteError,
};
pub use types::{
    libmoq_version, CompanionProfile, DeliveryOrigin, Location, MediaType, ObjectStatus, Packaging,
    SapType, Version, WaitResult,
};
