use std::ptr::NonNull;
use std::sync::Arc;
use std::time::Duration;
use std::{mem, ptr};

use crate::error::{Error, Result};
use crate::types::{Version, WaitResult};
use crate::{ffi, sys};

/// How the endpoint reaches the relay (`moq_transport_protocol_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Protocol {
    /// Derive from the URL scheme: `moqt://`/`moq://` is raw QUIC, `https://`
    /// is WebTransport.
    #[default]
    Auto,
    /// Native MoQ over QUIC (moqt-NN ALPN).
    RawQuic,
    /// HTTP/3 WebTransport.
    WebTransport,
}

impl Protocol {
    fn raw(self) -> sys::moq_transport_protocol_t {
        match self {
            Protocol::Auto => sys::MOQ_TRANSPORT_PROTOCOL_AUTO,
            Protocol::RawQuic => sys::MOQ_TRANSPORT_PROTOCOL_RAW_QUIC,
            Protocol::WebTransport => sys::MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT,
        }
    }
}

/// Which QUIC stack serves the connection (`moq_transport_backend_t`).
/// Backends other than the picoquic default are opt-in and present only
/// when libmoq was built with them.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Backend {
    /// The picoquic family, libmoq's stable default; never an opt-in backend.
    #[default]
    Auto,
    Picoquic,
    Mvfst,
    Proxygen,
    MsQuic,
    WtquicNetwork,
    WtquicMsQuic,
}

impl Backend {
    fn raw(self) -> sys::moq_transport_backend_t {
        match self {
            Backend::Auto => sys::MOQ_TRANSPORT_BACKEND_AUTO,
            Backend::Picoquic => sys::MOQ_TRANSPORT_BACKEND_PICOQUIC,
            Backend::Mvfst => sys::MOQ_TRANSPORT_BACKEND_MVFST,
            Backend::Proxygen => sys::MOQ_TRANSPORT_BACKEND_PROXYGEN,
            Backend::MsQuic => sys::MOQ_TRANSPORT_BACKEND_MSQUIC,
            Backend::WtquicNetwork => sys::MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK,
            Backend::WtquicMsQuic => sys::MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC,
        }
    }
}

/// Which MoQ Transport versions to offer (`moq_version_policy_t`).
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum VersionOffer {
    /// Every version this libmoq build supports, newest first, with
    /// [`EndpointConfig::preferred_version`] moved to the front.
    #[default]
    Auto,
    /// Exactly these, in preference order; the peer picks.
    List(Vec<Version>),
    /// Exactly this one; anything else fails the connection.
    Exact(Version),
}

/// The WebTransport-over-HTTP/3 wire profile (`moq_wt_profile_t`). Only a
/// backend that can choose a dialect (wtquic-msquic) accepts an explicit
/// one; the others refuse it with [`Error::UNSUPPORTED`] at connect.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WtProfile {
    /// The backend's own default.
    #[default]
    BackendDefault,
    /// The current WebTransport-H3 draft.
    Current,
    /// Drafts 13/14 compatibility.
    D13_14Compat,
}

impl WtProfile {
    fn raw(self) -> u32 {
        match self {
            WtProfile::BackendDefault => sys::MOQ_WT_PROFILE_BACKEND_DEFAULT,
            WtProfile::Current => sys::MOQ_WT_PROFILE_CURRENT,
            WtProfile::D13_14Compat => sys::MOQ_WT_PROFILE_D13_14_COMPAT,
        }
    }
}

/// How the service reads the URL (`moq_endpoint_cfg_t.uri_mode`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UriMode {
    /// The original parser: lowercase `moqt`/`moq`/`https`, port 4433 for
    /// the QUIC schemes, the `wt_path` override and the `/moq` WebTransport
    /// default.
    #[default]
    Legacy,
    /// The MOQT URI scheme: `moqt`/`https` case-insensitively, port 443 when
    /// omitted, the URL's own path and query as the WebTransport path; a
    /// non-empty `wt_path` is [`Error::INVAL`].
    Standard,
    /// A mode this crate does not know; libmoq refuses it.
    Other(u32),
}

impl UriMode {
    /// The `MOQ_ENDPOINT_URI_*` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            UriMode::Legacy => sys::MOQ_ENDPOINT_URI_LEGACY,
            UriMode::Standard => sys::MOQ_ENDPOINT_URI_STANDARD,
            UriMode::Other(code) => code,
        }
    }
}

/// An authorization token the session presents in its SETUP
/// (`moq_auth_token_t`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthToken {
    /// The token type; draft-16 carries values up to 2^62-1.
    pub token_type: u64,
    /// The token value, at most 65535 bytes.
    pub value: Vec<u8>,
}

/// How to connect an [`Endpoint`].
///
/// Every connect builds the C configuration with the sized initializer, so
/// every field here reaches the service. What this crate does not expose
/// stays at the initializer's default: the allocator, the certificate trust
/// callback, the server-side URI policy and the perspective; those remain
/// reachable through [`sys`](crate::sys).
#[derive(Clone, Debug)]
pub struct EndpointConfig {
    url: String,
    protocol: Protocol,
    backend: Backend,
    versions: VersionOffer,
    sni: Option<String>,
    ca_file: Option<String>,
    insecure_skip_verify: bool,
    wt_path: Option<String>,
    wt_profile: WtProfile,
    handshake_timeout: Option<Duration>,
    setup_auth_tokens: Vec<AuthToken>,
    uri_mode: UriMode,
    preferred_version: Option<Version>,
}

impl EndpointConfig {
    /// Connect to `url`: `moqt://host:port[/path]` or `https://host:port/path`.
    #[must_use]
    pub fn new(url: impl Into<String>) -> EndpointConfig {
        EndpointConfig {
            url: url.into(),
            protocol: Protocol::Auto,
            backend: Backend::Auto,
            versions: VersionOffer::Auto,
            sni: None,
            ca_file: None,
            insecure_skip_verify: false,
            wt_path: None,
            wt_profile: WtProfile::BackendDefault,
            handshake_timeout: None,
            setup_auth_tokens: Vec::new(),
            uri_mode: UriMode::Legacy,
            preferred_version: None,
        }
    }

    /// Override the protocol the URL scheme implies.
    #[must_use]
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Pick a QUIC backend.
    #[must_use]
    pub fn backend(mut self, backend: Backend) -> Self {
        self.backend = backend;
        self
    }

    /// Which versions to offer.
    #[must_use]
    pub fn versions(mut self, versions: VersionOffer) -> Self {
        self.versions = versions;
        self
    }

    /// TLS server name; defaults to the URL host.
    #[must_use]
    pub fn sni(mut self, sni: impl Into<String>) -> Self {
        self.sni = Some(sni.into());
        self
    }

    /// PEM CA bundle to verify the peer against instead of the backend's
    /// default roots.
    #[must_use]
    pub fn ca_file(mut self, path: impl Into<String>) -> Self {
        self.ca_file = Some(path.into());
        self
    }

    /// Skip certificate verification. For tests and demos only.
    #[must_use]
    pub fn insecure_skip_verify(mut self, insecure: bool) -> Self {
        self.insecure_skip_verify = insecure;
        self
    }

    /// WebTransport CONNECT target override; defaults to the URL path, or
    /// `/moq`. Only under [`UriMode::Legacy`].
    #[must_use]
    pub fn wt_path(mut self, path: impl Into<String>) -> Self {
        self.wt_path = Some(path.into());
        self
    }

    /// WebTransport wire profile.
    #[must_use]
    pub fn wt_profile(mut self, profile: WtProfile) -> Self {
        self.wt_profile = profile;
        self
    }

    /// Upper bound on the QUIC/TLS handshake, exact microseconds. Unset
    /// leaves the backend's own value (30 s on picoquic); only the picoquic
    /// backend honours it, and the service refuses a value above
    /// `MOQ_ENDPOINT_HANDSHAKE_TIMEOUT_MAX_US`. It does not bound the
    /// WebTransport CONNECT or the MoQ SETUP.
    #[must_use]
    pub fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = Some(timeout);
        self
    }

    /// Authorization tokens for the SETUP, copied by the service at connect.
    /// At most 16; only the picoquic backend carries them, the others refuse
    /// a non-empty list with [`Error::UNSUPPORTED`].
    #[must_use]
    pub fn setup_auth_tokens(mut self, tokens: Vec<AuthToken>) -> Self {
        self.setup_auth_tokens = tokens;
        self
    }

    /// How the URL is read.
    #[must_use]
    pub fn uri_mode(mut self, mode: UriMode) -> Self {
        self.uri_mode = mode;
        self
    }

    /// The version placed first in an [`VersionOffer::Auto`] offer. With a
    /// LIST or EXACT offer the service answers [`Error::INVAL`].
    #[must_use]
    pub fn preferred_version(mut self, version: Version) -> Self {
        self.preferred_version = Some(version);
        self
    }
}

/// The managed connection to a relay: one QUIC/WebTransport connection and
/// MoQ session, with libmoq's network thread behind it.
///
/// `Endpoint` is a shared handle over an `Arc`: clones refer to the same
/// connection, and every [`Sender`](crate::Sender) and
/// [`Receiver`](crate::Receiver) attached to it holds one, so the endpoint
/// outlives its attachments without the application sequencing the drops,
/// and a clone on another thread can wake or interrupt a blocked call
/// (which is the point of the latch). When the last handle drops the
/// endpoint is stopped (the network thread joined) and destroyed. That last
/// drop always happens on an application thread: this crate runs no
/// application code on the service's threads (no callbacks, no posted tasks,
/// no buffer owners with destructors), so the thread `stop()` joins is never
/// the one running the drop.
#[derive(Clone, Debug)]
pub struct Endpoint(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    raw: NonNull<sys::moq_endpoint_t>,
}

// SAFETY: the service tier is built for cross-thread use: the interrupt latch
// and wake are callable from any thread, the blocking calls from any
// application thread, and the observers from anywhere.
unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

impl Drop for Inner {
    fn drop(&mut self) {
        // SAFETY: the last handle. Attachments hold a handle, so none is live
        // and stop() is not refused for one. A raw attachment surviving an
        // adopted endpoint (from_raw) breaks that adoption's contract; the
        // native destroy asserts the precondition in debug builds rather
        // than recovering from it.
        unsafe {
            let _ = sys::moq_endpoint_stop(self.raw.as_ptr());
            sys::moq_endpoint_destroy(self.raw.as_ptr());
        }
    }
}

/// Lifecycle state of an endpoint (`moq_endpoint_state_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointState {
    /// The handshake is in progress.
    Connecting,
    /// The session is up.
    Established,
    /// Reserved; never entered today.
    Reconnecting,
    /// GOAWAY received.
    Draining,
    /// Terminal.
    Closed,
    /// A state this crate does not know.
    Other(u32),
}

/// Why an endpoint went terminal (`moq_endpoint_terminal_reason_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalReason {
    /// Not terminal, or the reason is unknown.
    None,
    /// Clean close.
    Clean,
    /// MoQ-level fatal; `detail_code` is the fatal code.
    Protocol,
    /// The peer certificate failed verification.
    TlsCertificate,
    /// Another TLS handshake failure.
    Tls,
    /// A generic transport or handshake failure.
    Transport,
    /// A reason this crate does not know.
    Other(u32),
}

impl TerminalReason {
    fn from_raw(code: sys::moq_endpoint_terminal_reason_t) -> TerminalReason {
        match code {
            sys::MOQ_ENDPOINT_TERMINAL_NONE => TerminalReason::None,
            sys::MOQ_ENDPOINT_TERMINAL_CLEAN => TerminalReason::Clean,
            sys::MOQ_ENDPOINT_TERMINAL_PROTOCOL => TerminalReason::Protocol,
            sys::MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE => TerminalReason::TlsCertificate,
            sys::MOQ_ENDPOINT_TERMINAL_TLS => TerminalReason::Tls,
            sys::MOQ_ENDPOINT_TERMINAL_TRANSPORT => TerminalReason::Transport,
            code => TerminalReason::Other(code),
        }
    }

    /// The `moq_endpoint_terminal_reason_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            TerminalReason::None => sys::MOQ_ENDPOINT_TERMINAL_NONE,
            TerminalReason::Clean => sys::MOQ_ENDPOINT_TERMINAL_CLEAN,
            TerminalReason::Protocol => sys::MOQ_ENDPOINT_TERMINAL_PROTOCOL,
            TerminalReason::TlsCertificate => sys::MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE,
            TerminalReason::Tls => sys::MOQ_ENDPOINT_TERMINAL_TLS,
            TerminalReason::Transport => sys::MOQ_ENDPOINT_TERMINAL_TRANSPORT,
            TerminalReason::Other(code) => code,
        }
    }
}

/// An endpoint's terminal classification (`moq_endpoint_terminal_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Terminal {
    /// The classification.
    pub reason: TerminalReason,
    /// Transport-native detail as raw bits: the MoQ fatal code for
    /// [`TerminalReason::Protocol`]; for the TLS and transport reasons the
    /// backend's native error (a negative OSStatus is stored two's
    /// complement); 0 when clean or not terminal.
    pub detail_code: u64,
}

impl Endpoint {
    /// Validate `config` and start connecting. The handshake completes
    /// asynchronously; watch it with [`wait`](Endpoint::wait) and
    /// [`state`](Endpoint::state). Unless `insecure_skip_verify` is set,
    /// the peer certificate is verified.
    pub fn connect(config: &EndpointConfig) -> Result<Endpoint> {
        let handshake_timeout_us = match config.handshake_timeout {
            Some(timeout) => ffi::micros(timeout)?,
            None => 0,
        };
        let versions: Vec<sys::moq_version_t> = match &config.versions {
            VersionOffer::Auto => Vec::new(),
            VersionOffer::List(list) => list.iter().map(|version| version.raw()).collect(),
            VersionOffer::Exact(version) => vec![version.raw()],
        };
        let mut tokens: Vec<sys::moq_auth_token_t> =
            Vec::with_capacity(config.setup_auth_tokens.len());
        for token in &config.setup_auth_tokens {
            tokens.push(sys::moq_auth_token_t {
                token_type: token.token_type,
                token_value: ffi::span(&token.value),
            });
        }

        let mut raw_config = sys::moq_endpoint_cfg_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe { sys::moq_endpoint_cfg_init_sized(&mut raw_config, mem::size_of_val(&raw_config)) };
        raw_config.url = ffi::span(config.url.as_bytes());
        raw_config.protocol = config.protocol.raw();
        raw_config.backend = config.backend.raw();
        if !matches!(config.versions, VersionOffer::Auto) {
            raw_config.versions.struct_size = mem::size_of_val(&raw_config.versions) as u32;
            raw_config.versions.policy = match config.versions {
                VersionOffer::Exact(_) => sys::MOQ_VERSION_POLICY_EXACT,
                _ => sys::MOQ_VERSION_POLICY_LIST,
            };
            raw_config.versions.versions = versions.as_ptr();
            raw_config.versions.version_count = versions.len();
        }
        raw_config.sni = ffi::optional_span(config.sni.as_deref());
        raw_config.ca_file = ffi::optional_span(config.ca_file.as_deref());
        raw_config.insecure_skip_verify = config.insecure_skip_verify;
        raw_config.wt_path = ffi::optional_span(config.wt_path.as_deref());
        raw_config.wt_profile = config.wt_profile.raw();
        raw_config.handshake_timeout_us = handshake_timeout_us;
        if !tokens.is_empty() {
            raw_config.setup_auth_tokens = tokens.as_ptr();
            raw_config.setup_auth_token_count = tokens.len();
        }
        raw_config.uri_mode = config.uri_mode.code();
        raw_config.preferred_version = config.preferred_version.map_or(0, Version::raw);

        let mut raw = ptr::null_mut();
        // SAFETY: every pointer in raw_config points into config, versions or
        // tokens, all alive for the call; the service copies what it keeps.
        ffi::check(unsafe { sys::moq_endpoint_connect(&raw_config, &mut raw) })?;
        let raw = NonNull::new(raw).ok_or(Error::INTERNAL)?;
        Ok(Endpoint(Arc::new(Inner { raw })))
    }

    /// Take ownership of an endpoint created through [`sys`](crate::sys).
    ///
    /// # Safety
    /// `raw` must be a live endpoint that nothing else will stop or destroy.
    /// Its configuration must keep this crate's invariant, no application
    /// code on the service's threads: no certificate callback, no posted
    /// task and no allocator that reaches back into the application; and
    /// its attachments must be dropped before the last handle.
    ///
    /// # Panics
    /// If `raw` is null.
    pub unsafe fn from_raw(raw: *mut sys::moq_endpoint_t) -> Endpoint {
        Endpoint(Arc::new(Inner {
            raw: NonNull::new(raw).expect("a null endpoint cannot be adopted"),
        }))
    }

    /// Give up ownership without stopping the endpoint. Fails, returning the
    /// handle, unless this is the only one.
    pub fn into_raw(self) -> std::result::Result<*mut sys::moq_endpoint_t, Endpoint> {
        let inner = Arc::try_unwrap(self.0).map_err(Endpoint)?;
        let raw = inner.raw.as_ptr();
        mem::forget(inner);
        Ok(raw)
    }

    /// The underlying `moq_endpoint_t`.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sys::moq_endpoint_t {
        self.0.raw.as_ptr()
    }

    /// Block until the endpoint changes state or wakes, or `timeout`
    /// (exact microseconds; zero polls) elapses. Fails with
    /// [`Error::INTERRUPTED`] while the latch is set and [`Error::CLOSED`]
    /// once terminal.
    pub fn wait(&self, timeout: Duration) -> Result<WaitResult> {
        let timeout_us = ffi::micros(timeout)?;
        // SAFETY: a live endpoint; waits are legal from any application
        // thread.
        ffi::wait_result(unsafe { sys::moq_endpoint_wait(self.as_ptr(), timeout_us) })
    }

    /// Schedule a pump cycle soon. Callable from any thread; never fails
    /// and does not clear the interrupt latch.
    pub fn wake(&self) {
        // SAFETY: a live endpoint; non-allocating and infallible.
        unsafe { sys::moq_endpoint_wake(self.as_ptr()) }
    }

    /// Block until the output the attached services owe has been handed to
    /// the session and flushed by the transport (and acknowledged, where the
    /// backend observes it), so a stop does not truncate it.
    /// [`WaitResult::Ready`] means flushed; it does not mean the peer
    /// application consumed the media, and it is not proof that a broadcast
    /// completed. Fails with [`Error::INTERRUPTED`], [`Error::CLOSED`],
    /// [`Error::UNSUPPORTED`] when the backend cannot prove a flush, and
    /// [`Error::INTERNAL`] for an unsuccessful flush.
    pub fn drain(&self, timeout: Duration) -> Result<WaitResult> {
        let timeout_us = ffi::micros(timeout)?;
        // SAFETY: a live endpoint, called from an application thread.
        ffi::wait_result(unsafe { sys::moq_endpoint_drain(self.as_ptr(), timeout_us) })
    }

    /// Stop the endpoint now: joins the network thread. Fails with
    /// [`Error::WRONG_STATE`] while a sender or receiver is attached.
    /// Dropping the last handle does this anyway; call it to stop early.
    pub fn stop(&self) -> Result<()> {
        // SAFETY: a live endpoint, called from an application thread.
        ffi::check(unsafe { sys::moq_endpoint_stop(self.as_ptr()) })
    }

    /// Set or clear the sticky interrupt latch. While set, every blocking
    /// call on the endpoint and its attachments returns
    /// [`Error::INTERRUPTED`] at once. Callable from any thread.
    pub fn set_interrupted(&self, interrupted: bool) {
        // SAFETY: a live endpoint; callable from any thread.
        unsafe { sys::moq_endpoint_set_interrupted(self.as_ptr(), interrupted) }
    }

    /// The current lifecycle state.
    #[must_use]
    pub fn state(&self) -> EndpointState {
        // SAFETY: a live endpoint.
        match unsafe { sys::moq_endpoint_state(self.as_ptr()) } {
            sys::MOQ_ENDPOINT_CONNECTING => EndpointState::Connecting,
            sys::MOQ_ENDPOINT_ESTABLISHED => EndpointState::Established,
            sys::MOQ_ENDPOINT_RECONNECTING => EndpointState::Reconnecting,
            sys::MOQ_ENDPOINT_DRAINING => EndpointState::Draining,
            sys::MOQ_ENDPOINT_CLOSED => EndpointState::Closed,
            code => EndpointState::Other(code),
        }
    }

    /// The endpoint has reached its closed state: its managed teardown ran,
    /// or a terminal was recorded. Why it closed -- a clean close, a drain,
    /// a fatal error -- is the [`Terminal`]'s reason, from
    /// [`Endpoint::terminal`]; this alone does not say the close was clean.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        // SAFETY: a live endpoint.
        unsafe { sys::moq_endpoint_is_closed(self.as_ptr()) }
    }

    /// Failed. Use [`terminal`](Endpoint::terminal) to find out why.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        // SAFETY: a live endpoint.
        unsafe { sys::moq_endpoint_is_fatal(self.as_ptr()) }
    }

    /// The MoQ-level fatal code; 0 for a transport or TLS failure, so zero
    /// is not "no error".
    #[must_use]
    pub fn fatal_code(&self) -> u64 {
        // SAFETY: a live endpoint.
        unsafe { sys::moq_endpoint_fatal_code(self.as_ptr()) }
    }

    /// Why the endpoint went terminal ([`TerminalReason::None`] while it is
    /// not).
    pub fn terminal(&self) -> Result<Terminal> {
        let mut terminal = sys::moq_endpoint_terminal_t::default();
        let size = mem::size_of_val(&terminal);
        // SAFETY: a writable struct of exactly the size passed.
        ffi::check(unsafe { sys::moq_endpoint_get_terminal(self.as_ptr(), &mut terminal, size) })?;
        ffi::check_stamp(
            terminal.struct_size,
            sys::MOQ_ENDPOINT_TERMINAL_V0_SIZE,
            size,
        )?;
        Ok(Terminal {
            reason: TerminalReason::from_raw(terminal.reason),
            detail_code: terminal.detail_code,
        })
    }

    /// The negotiated version, once established.
    #[must_use]
    pub fn negotiated_version(&self) -> Option<Version> {
        // SAFETY: a live endpoint.
        match unsafe { sys::moq_endpoint_negotiated_version(self.as_ptr()) } {
            0 => None,
            draft => Some(Version::from_draft(draft)),
        }
    }

    /// The New Session URI of a session-wide GOAWAY the peer sent (the
    /// endpoint is then draining): `Some(empty)` means the current URI,
    /// `None` that no GOAWAY was recorded. Fails with [`Error::NOMEM`] when
    /// one was received but its URI could not be kept.
    pub fn goaway_uri(&self) -> Result<Option<Vec<u8>>> {
        let mut uri: Vec<u8> = Vec::new();
        loop {
            let mut needed = 0usize;
            let capacity = uri.capacity();
            let buffer = if capacity == 0 {
                ptr::null_mut()
            } else {
                uri.as_mut_ptr()
            };
            // SAFETY: buffer is writable for capacity bytes (or NULL with a
            // capacity of 0); needed is written on every return.
            let code = unsafe {
                sys::moq_endpoint_get_goaway_uri(self.as_ptr(), buffer, capacity, &mut needed)
            };
            match code {
                sys::MOQ_OK => {
                    // SAFETY: the service wrote `needed` bytes, at most the
                    // capacity.
                    unsafe { uri.set_len(needed.min(capacity)) };
                    return Ok(Some(uri));
                }
                sys::MOQ_DONE => return Ok(None),
                sys::MOQ_ERR_BUFFER => uri.reserve_exact(needed.max(1)),
                code => return Err(Error::from_code(code)),
            }
        }
    }
}
