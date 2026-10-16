//! Safe Rust API for libmoq, the MOQ5 Media over QUIC library.
//!
//! This crate wraps libmoq's *service tier*: a managed [`Endpoint`]
//! (connection, MoQ session and network thread), a [`MediaSender`] that
//! publishes a namespace with an MSF/CMSF catalog, and a [`MediaReceiver`]
//! that discovers and receives one. The raw C API stays available as
//! [`sys`] (the `moq5-sys` crate).
//!
//! What the wrapper enforces that C leaves to documentation:
//!
//! - **Teardown order.** Senders and receivers hold their endpoint, and the
//!   endpoint is stopped and destroyed only when its last handle drops.
//! - **Handle ownership.** Track handles are checked against the sender or
//!   receiver that issued them; a foreign one fails with [`Error::INVAL`].
//! - **Payload ownership.** [`MediaSender::write`] takes an [`RcBuf`] and
//!   hands it back on failure; received [`MediaObject`]s release their
//!   buffers on drop, on the polling thread.
//! - **Lifetimes.** Track descriptions borrow the receiver that owns them.
//! - **Threading.** [`Endpoint`] is `Send + Sync` (interrupting from another
//!   thread is the point of its latch); senders and receivers are `Send` but
//!   not `Sync`, so their calls are serialized.
//!
//! ```no_run
//! use std::time::Duration;
//! use moq5::{
//!     Endpoint, EndpointConfig, MediaSender, MediaType, Namespace, Packaging,
//!     RcBuf, SendObject, SenderConfig, StartupStatus, TrackConfig,
//! };
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let ep = Endpoint::connect(&EndpointConfig::new("https://relay.example:4443/moq"))?;
//! let cfg = SenderConfig::live(Namespace::parse("live/demo")?);
//! let video = TrackConfig::new("video", MediaType::Video, Packaging::Loc, "avc1.42e01e")
//!     .bitrate(2_000_000);
//! // Attach and add the track in one network turn, so the first catalog has it.
//! let mut startup = MediaSender::start(&ep, cfg, vec![video])?;
//! let (sender, tracks) = loop {
//!     match startup.wait(Duration::from_millis(100)) {
//!         StartupStatus::Done(r) => break r?,
//!         StartupStatus::Pending(s) => startup = s,
//!     }
//! };
//! let mut obj = SendObject::new(RcBuf::from_owner(vec![0u8; 100])?);
//! obj.is_sync = true;
//! obj.starts_group = true;
//! sender.write(tracks[0], obj)?;
//! # Ok(())
//! # }
//! ```
//!
//! # Linking
//!
//! `moq5-sys` links the static libmoq archives from an install prefix; see
//! its documentation for `MOQ_PREFIX`.
//!
//! # Testing without a relay
//!
//! With the `testing` feature, `testing::install` replaces libmoq's
//! service calls with a table of fakes until the returned guard drops, so an
//! integration can be tested deterministically.

#![warn(missing_docs)]

pub use moq5_sys as sys;

pub mod cmaf;
mod endpoint;
mod error;
mod ffi;
mod namespace;
mod rcbuf;
mod receiver;
mod sender;
mod types;

pub use endpoint::{
    Backend, Endpoint, EndpointConfig, EndpointState, Protocol, TaskContext, Terminal,
    TerminalReason, VersionOffer, WtProfile,
};
pub use error::{Error, Result};
pub use namespace::{Namespace, NamespaceError};
pub use rcbuf::RcBuf;
pub use receiver::{
    ContentProtectionRef, MediaObject, MediaReceiver, ReceiverConfig, ReceiverStats, ReceiverTrack,
    SapRecord, StartMode, SubscribeOptions, TimeMode, TimelineRecord, TrackDesc, TrackEvent,
    TrackEventKind, TrackState,
};
pub use sender::{
    Backpressure, CmsfUrl, ContentProtection, DrmSystem, MediaSender, MediaTrack, SendObject,
    SenderConfig, SenderStartup, SenderStats, StartupStatus, TrackConfig, WriteError,
};
pub use types::{MediaType, Packaging, SapType, Version, Wait};

/// The libmoq version string, e.g. `0.1.0`.
pub fn libmoq_version() -> &'static str {
    // SAFETY: a static NUL-terminated string.
    unsafe { std::ffi::CStr::from_ptr(sys::moq_version_string()) }
        .to_str()
        .unwrap_or("unknown")
}

/// Replace libmoq's service calls with fakes, for tests.
///
/// ```ignore
/// use moq5::testing::{install, Api, REAL};
/// unsafe extern "C" fn connect(
///     _cfg: *const moq5::sys::moq_endpoint_cfg_t,
///     _out: *mut *mut moq5::sys::moq_endpoint_t,
/// ) -> moq5::sys::moq_result_t {
///     moq5::sys::MOQ_ERR_UNSUPPORTED
/// }
/// let _guard = install(Api { moq_endpoint_connect: connect, ..REAL });
/// ```
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    pub use crate::ffi::seam::{install, ApiGuard};
    pub use crate::ffi::{Api, REAL};
}

#[cfg(test)]
mod tests;
