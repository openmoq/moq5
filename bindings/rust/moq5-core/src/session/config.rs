//! Session configuration and the session's observable state.

use std::marker::PhantomData;
use std::time::Duration;

use super::raw;
use super::request::{tokens_ptr, tokens_raw, AuthToken};
use crate::{sys, Result, Version};

/// Who opened the connection (`moq_perspective_t`). This is not a publish
/// or subscribe role: either side may do both. It decides who begins the
/// setup on draft 16 and the parity of request IDs.
///
/// Closed: a connection has two ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Perspective {
    /// The side that opened the connection.
    Client,
    /// The side that accepted it.
    Server,
}

impl Perspective {
    pub(super) fn raw(self) -> sys::moq_perspective_t {
        match self {
            Perspective::Client => sys::MOQ_PERSPECTIVE_CLIENT,
            Perspective::Server => sys::MOQ_PERSPECTIVE_SERVER,
        }
    }
}

/// The session's state (`moq_session_state_t`, an open set).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SessionState {
    /// Created, setup not begun.
    Idle,
    /// This side sent its setup.
    SetupSent,
    /// Setup completed on both sides.
    Established,
    /// A GOAWAY was sent or received.
    Draining,
    /// Closed; only the close's own outputs remain to be polled.
    Closed,
    /// A state this crate does not know.
    Other(u32),
}

impl SessionState {
    pub(super) fn from_raw(raw: sys::moq_session_state_t) -> SessionState {
        match raw {
            sys::MOQ_SESS_IDLE => SessionState::Idle,
            sys::MOQ_SESS_SETUP_SENT => SessionState::SetupSent,
            sys::MOQ_SESS_ESTABLISHED => SessionState::Established,
            sys::MOQ_SESS_DRAINING => SessionState::Draining,
            sys::MOQ_SESS_CLOSED => SessionState::Closed,
            other => SessionState::Other(other),
        }
    }
}

/// How a [`Session`](crate::Session) is created (`moq_session_cfg_t`).
///
/// The perspective and the wire version are required; every limit left at
/// zero keeps the library's default, which is what zero means to C. The
/// record is built with the library's sized initializer, so every field set
/// here reaches the session. Sessions use the library's default allocator;
/// no other is selectable.
///
/// Every native selection has a setter except the allocator. The reserved
/// fields (`_reserved_version`, `_reserved_setup_auth`) are never written.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub(super) perspective: Perspective,
    pub(super) version: Version,
    request_capacity: Option<u64>,
    limits: Limits,
    streaming_objects: bool,
    idle_timeout: Option<Duration>,
    goaway_timeout: Option<Duration>,
    defer_subscription_updates: bool,
    defer_fetch_ranges: bool,
    cluster: Option<(u64, Option<u64>)>,
    auth_token_cache_size: Option<u64>,
    setup_auth_tokens: Vec<AuthToken>,
    done_wait_timeout: Option<Duration>,
}

/// The session's resource limits, each a `moq_session_cfg_t` field where
/// zero selects the library default.
#[derive(Clone, Copy, Debug, Default)]
struct Limits {
    max_actions: u32,
    max_events: u32,
    send_buffer_size: u32,
    recv_buffer_size: u32,
    max_subscriptions: u32,
    output_scratch_size: u32,
    max_open_subgroups: u32,
    max_data_streams: u32,
    max_object_payload_size: u32,
    max_receive_buffer_bytes: u32,
    max_announcements: u32,
    max_namespace_subscriptions: u32,
    max_fetches: u32,
    max_publishes: u32,
    max_track_statuses: u32,
    max_fills: u32,
    max_track_subscriptions: u32,
    max_track_history_records: u32,
    max_namespace_suffixes_per_subscription: u32,
}

macro_rules! limit_setter {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        ///
        /// Zero keeps the library default.
        #[must_use]
        pub fn $name(mut self, value: u32) -> SessionConfig {
            self.limits.$name = value;
            self
        }
    };
}

impl SessionConfig {
    /// A configuration for `perspective` speaking `version`, every limit
    /// at the library default. A version this build cannot speak is refused
    /// by [`Session::try_new`](crate::Session::try_new).
    #[must_use]
    pub fn new(perspective: Perspective, version: Version) -> SessionConfig {
        SessionConfig {
            perspective,
            version,
            request_capacity: None,
            limits: Limits::default(),
            streaming_objects: false,
            idle_timeout: None,
            goaway_timeout: None,
            defer_subscription_updates: false,
            defer_fetch_ranges: false,
            cluster: None,
            auth_token_cache_size: None,
            setup_auth_tokens: Vec::new(),
            done_wait_timeout: None,
        }
    }

    /// Advertise `size` bytes of authorization-token cache to the peer in
    /// this side's setup (MAX_AUTH_TOKEN_CACHE_SIZE); zero advertises that
    /// no alias may be registered. Unset, the setup carries no size.
    #[must_use]
    pub fn auth_token_cache_size(mut self, size: u64) -> SessionConfig {
        self.auth_token_cache_size = Some(size);
        self
    }

    /// Present `token` in this side's setup, by value, for the peer to
    /// authorize the session. C copies each token at creation. At most 16,
    /// each value at most 65535 bytes and draft 16's token type at most
    /// `2^62-1`, all fitting one setup message, or
    /// [`Session::try_new`](crate::Session::try_new) refuses with
    /// [`Error::INVAL`](crate::Error::INVAL). Creation checks that the setup
    /// encodes, not that it fits this side's send buffer
    /// ([`send_buffer_size`](SessionConfig::send_buffer_size)): a setup
    /// larger than the buffer is refused when this side sends it.
    ///
    /// - A draft-16 client, and either side on drafts 18 and 22, sends its
    ///   setup from [`Session::start`](crate::Session::start), which
    ///   returns [`Error::BUFFER`](crate::Error::BUFFER).
    /// - A draft-16 server cannot start: it sends its setup while handling
    ///   the client's, so the refusal comes from the control input that
    ///   delivers the client's setup.
    ///   [`Session::on_control_bytes`](crate::Session::on_control_bytes)
    ///   takes those bytes and fails with
    ///   [`Error::LIMIT`](crate::Error::LIMIT), and every
    ///   [`Session::process_pending`](crate::Session::process_pending)
    ///   returns the same: the client's setup stays with the session and can
    ///   never be answered.
    #[must_use]
    pub fn setup_auth_token(mut self, token: AuthToken) -> SessionConfig {
        self.setup_auth_tokens.push(token);
        self
    }

    /// The most a terminal done gated on its Stream Count waits for the
    /// data streams it advertised, before the session discards the rest and
    /// surfaces the terminal (library default 30 seconds; never below the
    /// subscription's delivery timeouts). A whole number of microseconds.
    #[must_use]
    pub fn done_wait_timeout(mut self, timeout: Duration) -> SessionConfig {
        self.done_wait_timeout = Some(timeout);
        self
    }

    /// Advertise `capacity` requests to the peer in this side's setup
    /// (draft 16). Unset, the setup advertises none and the peer can make
    /// no request until capacity is granted. Drafts 18 and 22 carry each
    /// request on its own stream and have no request capacity: the value is
    /// checked there but not sent.
    #[must_use]
    pub fn request_capacity(mut self, capacity: u64) -> SessionConfig {
        self.request_capacity = Some(capacity);
        self
    }

    /// Surface objects as chunks ([`Event::ObjectChunk`](crate::Event))
    /// instead of whole objects.
    #[must_use]
    pub fn streaming_objects(mut self, streaming: bool) -> SessionConfig {
        self.streaming_objects = streaming;
        self
    }

    /// Close the session after this long without activity. Unset, there is
    /// no idle timeout. It must be a whole number of microseconds.
    #[must_use]
    pub fn idle_timeout(mut self, timeout: Duration) -> SessionConfig {
        self.idle_timeout = Some(timeout);
        self
    }

    /// After this side sends GOAWAY, close the session this long later.
    /// Unset, there is no such deadline. It must be a whole number of
    /// microseconds.
    #[must_use]
    pub fn goaway_timeout(mut self, timeout: Duration) -> SessionConfig {
        self.goaway_timeout = Some(timeout);
        self
    }

    /// Leave the answer to a subscriber's update of a subscription or
    /// publication this side publishes to the application: the update
    /// surfaces with `answer_owed` and nothing it asks for takes effect until
    /// [`Session::accept_subscription_update`](crate::Session::accept_subscription_update)
    /// (or its publication form) answers it. Unset, the session answers
    /// every update it can apply itself.
    #[must_use]
    pub fn defer_subscription_updates(mut self, defer: bool) -> SessionConfig {
        self.defer_subscription_updates = defer;
        self
    }

    /// Leave the resolution of a FETCH range stated relative to the Largest
    /// Object to the application (a relay whose Largest is its own log): such
    /// a FETCH surfaces unresolved and is answered with the End Location the
    /// application resolved. Unset, the session resolves it against what it
    /// published.
    #[must_use]
    pub fn defer_fetch_ranges(mut self, defer: bool) -> SessionConfig {
        self.defer_fetch_ranges = defer;
        self
    }

    /// Take part in cluster draft-01 (draft 22 only) with this Hop ID (zero is
    /// a valid withheld ID) and, when given, this relay cost.
    #[must_use]
    pub fn cluster(mut self, hop_id: u64, relay_cost: Option<u64>) -> SessionConfig {
        self.cluster = Some((hop_id, relay_cost));
        self
    }

    limit_setter!(
        /// Entries in the action queue (library default 64).
        max_actions
    );
    limit_setter!(
        /// Entries in the event queue (library default 16).
        max_events
    );
    limit_setter!(
        /// Outbound bytes the session buffers (library default 4096).
        send_buffer_size
    );
    limit_setter!(
        /// Inbound control bytes the session buffers (library default 4096).
        recv_buffer_size
    );
    limit_setter!(
        /// Subscriptions (library default 64).
        max_subscriptions
    );
    limit_setter!(
        /// Bytes of event scratch that borrowed event fields point into
        /// (library default 65536).
        output_scratch_size
    );
    limit_setter!(
        /// Outgoing subgroups open at once (library default 64).
        max_open_subgroups
    );
    limit_setter!(
        /// Incoming data streams the session tracks at once (library
        /// default 64). A new stream past it is not admitted; see
        /// [`FeedError::NotAdmitted`](crate::FeedError).
        max_data_streams
    );
    limit_setter!(
        /// The largest object payload accepted, in bytes (library default
        /// 4 MiB).
        max_object_payload_size
    );
    limit_setter!(
        /// Received bytes the session retains at once (library default
        /// 16 MiB).
        max_receive_buffer_bytes
    );
    limit_setter!(
        /// Namespace advertisements (library default 64).
        max_announcements
    );
    limit_setter!(
        /// Namespace subscriptions (library default 16).
        max_namespace_subscriptions
    );
    limit_setter!(
        /// Fetches (library default 16).
        max_fetches
    );
    limit_setter!(
        /// Publications (library default 16).
        max_publishes
    );
    limit_setter!(
        /// Track status requests (library default 8).
        max_track_statuses
    );
    limit_setter!(
        /// Fill children (library default 16, at most 65536).
        max_fills
    );
    limit_setter!(
        /// SUBSCRIBE_TRACKS requests (library default 16).
        max_track_subscriptions
    );
    limit_setter!(
        /// Records of each track's largest location (library default: the
        /// subscription, publication and track-status limits together). A
        /// request needing a new record past it is refused.
        max_track_history_records
    );
    limit_setter!(
        /// Namespace suffixes one namespace subscription tracks as active at
        /// once (library default 4096). A suffix past it retires that
        /// subscription's stream, not the session.
        max_namespace_suffixes_per_subscription
    );

    /// The native configuration, built with the sized initializer, with
    /// the token records it points at; both borrow `self`. The allocator
    /// pointer is the library's static default.
    pub(super) fn raw(&self) -> Result<RawConfig<'_>> {
        let tokens = tokens_raw(&self.setup_auth_tokens);
        // SAFETY: zero is a valid bit pattern for this plain C record (its
        // pointers become null); the initializer overwrites it anyway.
        let mut cfg: sys::moq_session_cfg_t = unsafe { std::mem::zeroed() };
        // SAFETY: a writable record of exactly the size passed; the default
        // allocator is static and outlives every session.
        unsafe {
            sys::moq_session_cfg_init_sized(
                &mut cfg,
                std::mem::size_of::<sys::moq_session_cfg_t>(),
                sys::moq_alloc_default(),
                self.perspective.raw(),
            );
        }
        cfg.version = self.version.as_raw();
        if let Some(capacity) = self.request_capacity {
            cfg.send_request_capacity = true;
            cfg.initial_request_capacity = capacity;
        }
        let l = self.limits;
        cfg.max_actions = l.max_actions;
        cfg.max_events = l.max_events;
        cfg.send_buffer_size = l.send_buffer_size;
        cfg.recv_buffer_size = l.recv_buffer_size;
        cfg.max_subscriptions = l.max_subscriptions;
        cfg.output_scratch_size = l.output_scratch_size;
        cfg.max_open_subgroups = l.max_open_subgroups;
        cfg.max_data_streams = l.max_data_streams;
        cfg.max_object_payload_size = l.max_object_payload_size;
        cfg.max_receive_buffer_bytes = l.max_receive_buffer_bytes;
        cfg.max_announcements = l.max_announcements;
        cfg.max_namespace_subscriptions = l.max_namespace_subscriptions;
        cfg.max_fetches = l.max_fetches;
        cfg.max_publishes = l.max_publishes;
        cfg.max_track_statuses = l.max_track_statuses;
        cfg.max_fills = l.max_fills;
        cfg.max_track_subscriptions = l.max_track_subscriptions;
        cfg.max_track_history_records = l.max_track_history_records;
        cfg.max_namespace_suffixes_per_subscription = l.max_namespace_suffixes_per_subscription;
        if let Some(size) = self.auth_token_cache_size {
            cfg.send_auth_token_cache_size = true;
            cfg.auth_token_cache_size = size;
        }
        if let Some(timeout) = self.done_wait_timeout {
            cfg.done_wait_timeout_us = raw::micros(timeout)?;
        }
        cfg.setup_auth_tokens = tokens_ptr(&tokens);
        cfg.setup_auth_token_count = tokens.len();
        cfg.streaming_objects = self.streaming_objects;
        if let Some(timeout) = self.idle_timeout {
            cfg.idle_timeout_us = raw::micros(timeout)?;
        }
        if let Some(timeout) = self.goaway_timeout {
            cfg.goaway_timeout_us = raw::micros(timeout)?;
        }
        cfg.defer_subscription_updates = self.defer_subscription_updates;
        cfg.defer_fetch_ranges = self.defer_fetch_ranges;
        if let Some((hop_id, relay_cost)) = self.cluster {
            cfg.cluster_enabled = true;
            cfg.cluster_hop_id = hop_id;
            cfg.cluster_has_relay_cost = relay_cost.is_some();
            cfg.cluster_relay_cost = relay_cost.unwrap_or(0);
        }
        Ok(RawConfig {
            cfg,
            _tokens: tokens,
            _config: PhantomData,
        })
    }
}

/// A native session configuration and the token records it points at,
/// borrowing the [`SessionConfig`] whose token values they point into.
pub(super) struct RawConfig<'config> {
    pub(super) cfg: sys::moq_session_cfg_t,
    _tokens: Vec<sys::moq_auth_token_t>,
    _config: PhantomData<&'config SessionConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_reach_the_native_record() {
        let cfg = SessionConfig::new(Perspective::Server, Version::DRAFT_22)
            .max_events(2)
            .max_data_streams(1)
            .request_capacity(64)
            .idle_timeout(Duration::from_millis(5))
            .raw()
            .expect("a valid configuration")
            .cfg;
        assert_eq!(
            cfg.struct_size as usize,
            std::mem::size_of::<sys::moq_session_cfg_t>()
        );
        assert_eq!(cfg.perspective, sys::MOQ_PERSPECTIVE_SERVER);
        assert_eq!(cfg.version, Version::DRAFT_22.as_raw());
        assert_eq!(cfg.max_events, 2);
        assert_eq!(cfg.max_data_streams, 1);
        assert_eq!(cfg.max_actions, 0, "unset keeps the default");
        assert!(cfg.send_request_capacity);
        assert_eq!(cfg.initial_request_capacity, 64);
        assert_eq!(cfg.idle_timeout_us, 5_000);
        assert!(SessionConfig::new(Perspective::Client, Version::DRAFT_16)
            .goaway_timeout(Duration::from_nanos(1))
            .raw()
            .is_err());
    }
}
