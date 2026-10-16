use std::cell::Cell;
use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{check, Error, Result};
use crate::ffi;
use crate::sys;
use crate::types::{micros, span, wait_result, Version, Wait};

/// How the endpoint reaches the relay.
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

/// Which QUIC stack serves the connection. Backends other than the picoquic
/// default are opt-in and only present when libmoq was built with them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Backend {
    /// The picoquic family, libmoq's stable default.
    #[default]
    Auto,
    Picoquic,
    Mvfst,
    Proxygen,
    MsQuic,
    WtquicNetwork,
    WtquicMsQuic,
}

/// Which MoQ Transport versions to offer.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum VersionOffer {
    /// Every version this libmoq build supports, newest first.
    #[default]
    Auto,
    /// Exactly these, in preference order; the peer picks.
    List(Vec<Version>),
    /// Exactly this one; anything else fails the connection.
    Exact(Version),
}

/// The WebTransport-over-HTTP/3 wire profile. Only a backend that can choose
/// a dialect accepts an explicit one.
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

/// How to connect an [`Endpoint`].
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
}

impl EndpointConfig {
    /// Connect to `url`: `moqt://host:port[/path]` or `https://host:port/path`.
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
        }
    }

    /// Override the protocol the URL scheme implies.
    pub fn protocol(mut self, protocol: Protocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Pick a QUIC backend.
    pub fn backend(mut self, backend: Backend) -> Self {
        self.backend = backend;
        self
    }

    /// Which versions to offer.
    pub fn versions(mut self, versions: VersionOffer) -> Self {
        self.versions = versions;
        self
    }

    /// TLS server name; defaults to the URL host.
    pub fn sni(mut self, sni: impl Into<String>) -> Self {
        self.sni = Some(sni.into());
        self
    }

    /// PEM CA bundle to verify the peer against instead of the backend's
    /// default roots.
    pub fn ca_file(mut self, path: impl Into<String>) -> Self {
        self.ca_file = Some(path.into());
        self
    }

    /// Skip certificate verification. For tests and demos only.
    pub fn insecure_skip_verify(mut self, insecure: bool) -> Self {
        self.insecure_skip_verify = insecure;
        self
    }

    /// WebTransport path; defaults to the URL path, or `/moq`.
    pub fn wt_path(mut self, path: impl Into<String>) -> Self {
        self.wt_path = Some(path.into());
        self
    }

    /// WebTransport wire profile.
    pub fn wt_profile(mut self, profile: WtProfile) -> Self {
        self.wt_profile = profile;
        self
    }

    /// Upper bound on the transport handshake (picoquic only).
    pub fn handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = Some(timeout);
        self
    }
}

fn opt_span(s: &Option<String>) -> sys::moq_bytes_t {
    s.as_deref().map(|s| span(s.as_bytes())).unwrap_or_default()
}

/// The managed connection to a relay: one QUIC/WebTransport connection and
/// MoQ session, with libmoq's network thread behind it.
///
/// `Endpoint` is a shared handle: clones refer to the same connection, and
/// every [`MediaSender`](crate::MediaSender) and
/// [`MediaReceiver`](crate::MediaReceiver) attached to it holds one. When
/// the last handle is dropped the endpoint is stopped (the network thread
/// joined) and destroyed, so it always outlives its attachments.
///
/// Do not move the last handle into a [`post`](Endpoint::post) task or a
/// sender callback: they run on the endpoint's own thread, which cannot stop
/// itself. If that happens anyway the endpoint is leaked rather than stopped.
#[derive(Clone)]
pub struct Endpoint(Arc<Inner>);

struct Inner {
    raw: NonNull<sys::moq_endpoint_t>,
}

// SAFETY: libmoq's service tier is built for cross-thread use: the interrupt
// latch and wake are callable from any thread, and the blocking calls from any
// application thread. Only the endpoint's own thread is excluded, see Drop.
unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

thread_local! {
    /// Nonzero while this thread runs a libmoq task or callback.
    static IN_SERVICE: Cell<u32> = const { Cell::new(0) };
}

/// Marks the current thread as running inside a libmoq task or callback.
pub(crate) struct ServiceScope;

impl ServiceScope {
    pub(crate) fn enter() -> ServiceScope {
        IN_SERVICE.with(|c| c.set(c.get() + 1));
        ServiceScope
    }
}

impl Drop for ServiceScope {
    fn drop(&mut self) {
        IN_SERVICE.with(|c| c.set(c.get() - 1));
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if IN_SERVICE.with(|c| c.get()) > 0 {
            // Stopping joins the network thread we are running on.
            return;
        }
        unsafe {
            ffi::moq_endpoint_stop(self.raw.as_ptr());
            ffi::moq_endpoint_destroy(self.raw.as_ptr());
        }
    }
}

/// Lifecycle state of an endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum EndpointState {
    Connecting,
    Established,
    Reconnecting,
    /// GOAWAY received.
    Draining,
    Closed,
}

/// Why an endpoint went terminal.
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
    /// A reason this crate does not know yet.
    Other(u32),
}

impl TerminalReason {
    fn from_raw(v: sys::moq_endpoint_terminal_reason_t) -> TerminalReason {
        match v {
            sys::MOQ_ENDPOINT_TERMINAL_NONE => TerminalReason::None,
            sys::MOQ_ENDPOINT_TERMINAL_CLEAN => TerminalReason::Clean,
            sys::MOQ_ENDPOINT_TERMINAL_PROTOCOL => TerminalReason::Protocol,
            sys::MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE => TerminalReason::TlsCertificate,
            sys::MOQ_ENDPOINT_TERMINAL_TLS => TerminalReason::Tls,
            sys::MOQ_ENDPOINT_TERMINAL_TRANSPORT => TerminalReason::Transport,
            v => TerminalReason::Other(v),
        }
    }

    /// The `moq_endpoint_terminal_reason_t` value.
    pub fn code(self) -> u32 {
        match self {
            TerminalReason::None => sys::MOQ_ENDPOINT_TERMINAL_NONE,
            TerminalReason::Clean => sys::MOQ_ENDPOINT_TERMINAL_CLEAN,
            TerminalReason::Protocol => sys::MOQ_ENDPOINT_TERMINAL_PROTOCOL,
            TerminalReason::TlsCertificate => sys::MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE,
            TerminalReason::Tls => sys::MOQ_ENDPOINT_TERMINAL_TLS,
            TerminalReason::Transport => sys::MOQ_ENDPOINT_TERMINAL_TRANSPORT,
            TerminalReason::Other(v) => v,
        }
    }
}

/// An endpoint's terminal classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Terminal {
    /// The classification.
    pub reason: TerminalReason,
    /// Backend-native detail: the MoQ fatal code for
    /// [`TerminalReason::Protocol`], the TLS/QUIC error otherwise, 0 when
    /// clean.
    pub detail_code: u64,
}

/// What a [`post`](Endpoint::post)ed task runs with on a live session.
pub struct TaskContext {
    endpoint: *mut sys::moq_endpoint_t,
    session: NonNull<sys::moq_session_t>,
    now_us: u64,
}

impl TaskContext {
    /// The endpoint's monotonic clock, in microseconds.
    pub fn now_us(&self) -> u64 {
        self.now_us
    }

    /// The live MoQ session, for the core `moq_session_*` API in
    /// [`sys`](crate::sys). Valid only for the duration of the task.
    pub fn session_ptr(&self) -> *mut sys::moq_session_t {
        self.session.as_ptr()
    }

    /// The endpoint the task runs on.
    pub fn endpoint_ptr(&self) -> *mut sys::moq_endpoint_t {
        self.endpoint
    }
}

type Task = Box<dyn FnOnce(Option<&TaskContext>) -> Result<()> + Send>;

unsafe extern "C" fn run_task(
    ep: *mut sys::moq_endpoint_t,
    session: *mut sys::moq_session_t,
    now_us: u64,
    ctx: *mut c_void,
) -> sys::moq_result_t {
    let _scope = ServiceScope::enter();
    let task = Box::from_raw(ctx as *mut Task);
    let tc = NonNull::new(session).map(|session| TaskContext {
        endpoint: ep,
        session,
        now_us,
    });
    match catch_unwind(AssertUnwindSafe(move || task(tc.as_ref()))) {
        Ok(Ok(())) => sys::MOQ_OK,
        Ok(Err(e)) => e.code(),
        Err(_) => sys::MOQ_ERR_INTERNAL,
    }
}

impl Endpoint {
    /// Validate `cfg` and start connecting. The handshake completes
    /// asynchronously; watch it with [`wait`](Endpoint::wait) and
    /// [`state`](Endpoint::state).
    pub fn connect(cfg: &EndpointConfig) -> Result<Endpoint> {
        let versions: Vec<sys::moq_version_t> = match &cfg.versions {
            VersionOffer::Auto => Vec::new(),
            VersionOffer::List(v) => v.iter().map(|v| v.raw()).collect(),
            VersionOffer::Exact(v) => vec![v.raw()],
        };
        let tail = cfg.wt_profile != WtProfile::BackendDefault || cfg.handshake_timeout.is_some();

        let mut raw = sys::moq_endpoint_cfg_t::default();
        unsafe {
            if tail {
                ffi::moq_endpoint_cfg_init_sized(&mut raw, std::mem::size_of_val(&raw));
            } else {
                // The v0 initializer: the tail fields stay absent.
                ffi::moq_endpoint_cfg_init(&mut raw);
            }
        }
        raw.url = span(cfg.url.as_bytes());
        raw.protocol = match cfg.protocol {
            Protocol::Auto => sys::MOQ_TRANSPORT_PROTOCOL_AUTO,
            Protocol::RawQuic => sys::MOQ_TRANSPORT_PROTOCOL_RAW_QUIC,
            Protocol::WebTransport => sys::MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT,
        };
        raw.backend = match cfg.backend {
            Backend::Auto => sys::MOQ_TRANSPORT_BACKEND_AUTO,
            Backend::Picoquic => sys::MOQ_TRANSPORT_BACKEND_PICOQUIC,
            Backend::Mvfst => sys::MOQ_TRANSPORT_BACKEND_MVFST,
            Backend::Proxygen => sys::MOQ_TRANSPORT_BACKEND_PROXYGEN,
            Backend::MsQuic => sys::MOQ_TRANSPORT_BACKEND_MSQUIC,
            Backend::WtquicNetwork => sys::MOQ_TRANSPORT_BACKEND_WTQUIC_NETWORK,
            Backend::WtquicMsQuic => sys::MOQ_TRANSPORT_BACKEND_WTQUIC_MSQUIC,
        };
        if !matches!(cfg.versions, VersionOffer::Auto) {
            raw.versions.struct_size = std::mem::size_of_val(&raw.versions) as u32;
            raw.versions.policy = match cfg.versions {
                VersionOffer::Exact(_) => sys::MOQ_VERSION_POLICY_EXACT,
                _ => sys::MOQ_VERSION_POLICY_LIST,
            };
            raw.versions.versions = versions.as_ptr();
            raw.versions.version_count = versions.len();
        }
        raw.sni = opt_span(&cfg.sni);
        raw.ca_file = opt_span(&cfg.ca_file);
        raw.insecure_skip_verify = cfg.insecure_skip_verify;
        raw.wt_path = opt_span(&cfg.wt_path);
        if tail {
            raw.wt_profile = match cfg.wt_profile {
                WtProfile::BackendDefault => sys::MOQ_WT_PROFILE_BACKEND_DEFAULT,
                WtProfile::Current => sys::MOQ_WT_PROFILE_CURRENT,
                WtProfile::D13_14Compat => sys::MOQ_WT_PROFILE_D13_14_COMPAT,
            };
            raw.handshake_timeout_us = cfg.handshake_timeout.map(micros).unwrap_or(0);
        }

        let mut ep = std::ptr::null_mut();
        check(unsafe { ffi::moq_endpoint_connect(&raw, &mut ep) })?;
        let raw = NonNull::new(ep).ok_or(Error::INTERNAL)?;
        Ok(Endpoint(Arc::new(Inner { raw })))
    }

    /// Take ownership of an endpoint created elsewhere.
    ///
    /// # Safety
    /// `raw` must be a live endpoint that nothing else will stop or destroy.
    pub unsafe fn from_raw(raw: *mut sys::moq_endpoint_t) -> Endpoint {
        Endpoint(Arc::new(Inner {
            raw: NonNull::new(raw).expect("null endpoint"),
        }))
    }

    /// Give up ownership without stopping the endpoint. Fails, returning the
    /// handle, unless this is the only one.
    pub fn into_raw(self) -> std::result::Result<*mut sys::moq_endpoint_t, Endpoint> {
        let inner = Arc::try_unwrap(self.0).map_err(Endpoint)?;
        let raw = inner.raw.as_ptr();
        std::mem::forget(inner);
        Ok(raw)
    }

    /// The underlying `moq_endpoint_t`.
    pub fn as_ptr(&self) -> *mut sys::moq_endpoint_t {
        self.0.raw.as_ptr()
    }

    /// Block until the endpoint changes state or wakes, or `timeout` elapses.
    /// Fails with [`Error::INTERRUPTED`] while the latch is set and
    /// [`Error::CLOSED`] once terminal.
    pub fn wait(&self, timeout: Duration) -> Result<Wait> {
        wait_result(unsafe { ffi::moq_endpoint_wait(self.as_ptr(), micros(timeout)) })
    }

    /// Schedule a pump cycle soon. Callable from any thread; never fails.
    pub fn wake(&self) {
        unsafe { ffi::moq_endpoint_wake(self.as_ptr()) }
    }

    /// Block until local stream data has been handed to the transport, so a
    /// stop does not truncate it. [`Wait::Ready`] means flushed. It does not
    /// mean the peer acknowledged or consumed anything.
    pub fn drain(&self, timeout: Duration) -> Result<Wait> {
        wait_result(unsafe { ffi::moq_endpoint_drain(self.as_ptr(), micros(timeout)) })
    }

    /// Stop the endpoint now: joins the network thread. Fails with
    /// [`Error::WRONG_STATE`] while a sender or receiver is attached. Dropping
    /// the last handle does this anyway; call it to stop early.
    pub fn stop(&self) -> Result<()> {
        check(unsafe { ffi::moq_endpoint_stop(self.as_ptr()) })
    }

    /// Set or clear the sticky interrupt latch. While set, every blocking
    /// call on the endpoint and its attachments returns
    /// [`Error::INTERRUPTED`] at once. Callable from any thread.
    pub fn set_interrupted(&self, interrupted: bool) {
        unsafe { ffi::moq_endpoint_set_interrupted(self.as_ptr(), interrupted) }
    }

    /// The current lifecycle state.
    pub fn state(&self) -> EndpointState {
        match unsafe { ffi::moq_endpoint_state(self.as_ptr()) } {
            sys::MOQ_ENDPOINT_CONNECTING => EndpointState::Connecting,
            sys::MOQ_ENDPOINT_ESTABLISHED => EndpointState::Established,
            sys::MOQ_ENDPOINT_RECONNECTING => EndpointState::Reconnecting,
            sys::MOQ_ENDPOINT_DRAINING => EndpointState::Draining,
            _ => EndpointState::Closed,
        }
    }

    /// Closed cleanly or drained.
    pub fn is_closed(&self) -> bool {
        unsafe { ffi::moq_endpoint_is_closed(self.as_ptr()) }
    }

    /// Failed. Use [`terminal`](Endpoint::terminal) to find out why.
    pub fn is_fatal(&self) -> bool {
        unsafe { ffi::moq_endpoint_is_fatal(self.as_ptr()) }
    }

    /// The MoQ-level fatal code; 0 for a transport or TLS failure.
    pub fn fatal_code(&self) -> u64 {
        unsafe { ffi::moq_endpoint_fatal_code(self.as_ptr()) }
    }

    /// Why the endpoint went terminal ([`TerminalReason::None`] while it is
    /// not).
    pub fn terminal(&self) -> Result<Terminal> {
        let mut t = sys::moq_endpoint_terminal_t::default();
        check(unsafe {
            ffi::moq_endpoint_get_terminal(self.as_ptr(), &mut t, std::mem::size_of_val(&t))
        })?;
        Ok(Terminal {
            reason: TerminalReason::from_raw(t.reason),
            detail_code: t.detail_code,
        })
    }

    /// The negotiated version, once established.
    pub fn negotiated_version(&self) -> Option<Version> {
        match unsafe { ffi::moq_endpoint_negotiated_version(self.as_ptr()) } {
            0 => None,
            v => Some(Version::from_draft(v)),
        }
    }

    /// Run `task` on the endpoint's network thread, where the core session
    /// API is legal. Tasks run once each, in submission order. On a live
    /// session the task gets a [`TaskContext`]; when the endpoint stops or
    /// goes terminal first it still runs, with `None`.
    ///
    /// The task must not block, and must not own the last [`Endpoint`]
    /// handle. Fails with [`Error::CLOSED`] if the endpoint is already
    /// terminal, in which case `task` is dropped without running.
    pub fn post<F>(&self, task: F) -> Result<()>
    where
        F: FnOnce(Option<&TaskContext>) -> Result<()> + Send + 'static,
    {
        let ctx = Box::into_raw(Box::new(Box::new(task) as Task));
        let rc =
            unsafe { ffi::moq_endpoint_post(self.as_ptr(), Some(run_task), ctx as *mut c_void) };
        if rc != sys::MOQ_OK {
            // Not accepted: libmoq never runs it, so the context is ours.
            drop(unsafe { Box::from_raw(ctx) });
        }
        check(rc)
    }
}

impl std::fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Endpoint").field(&self.as_ptr()).finish()
    }
}
