//! The transport binding: what the host tells the session about the
//! connection before the session starts (`transport_binding.h`).

use std::ffi::c_void;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::raw;
use crate::{sys, Error};

/// One fact about the connection a profile may need before its setup
/// (`MOQ_BINDING_*`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Fact {
    /// The transport handshake completed.
    HandshakeComplete,
    /// This side negotiated QUIC datagrams.
    LocalQuicDatagram,
    /// The peer negotiated QUIC datagrams.
    PeerQuicDatagram,
    /// Datagrams can be sent.
    DatagramSend,
    /// Datagrams can be received.
    DatagramReceive,
}

impl Fact {
    /// Every fact, in bit order.
    pub const ALL: [Fact; 5] = [
        Fact::HandshakeComplete,
        Fact::LocalQuicDatagram,
        Fact::PeerQuicDatagram,
        Fact::DatagramSend,
        Fact::DatagramReceive,
    ];

    fn bit(self) -> u32 {
        match self {
            Fact::HandshakeComplete => sys::MOQ_BINDING_HANDSHAKE_COMPLETE,
            Fact::LocalQuicDatagram => sys::MOQ_BINDING_LOCAL_QUIC_DATAGRAM,
            Fact::PeerQuicDatagram => sys::MOQ_BINDING_PEER_QUIC_DATAGRAM,
            Fact::DatagramSend => sys::MOQ_BINDING_DATAGRAM_SEND,
            Fact::DatagramReceive => sys::MOQ_BINDING_DATAGRAM_RECEIVE,
        }
    }
}

/// A set of [`Fact`]s, such as the facts a profile requires before it
/// starts ([`Session::binding_requirements`](crate::Session::binding_requirements)).
/// Bits this crate does not name are kept.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct FactSet(u32);

impl FactSet {
    /// No fact.
    pub const EMPTY: FactSet = FactSet(0);

    /// The set with these native bits.
    #[must_use]
    pub const fn from_bits(bits: u32) -> FactSet {
        FactSet(bits)
    }

    /// The native bits.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }

    /// Whether `fact` is in the set.
    #[must_use]
    pub fn contains(self, fact: Fact) -> bool {
        self.0 & fact.bit() != 0
    }

    /// Whether the set is empty.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The set with `fact` added.
    #[must_use]
    pub fn with(self, fact: Fact) -> FactSet {
        FactSet(self.0 | fact.bit())
    }
}

impl fmt::Debug for FactSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut set = f.debug_set();
        for fact in Fact::ALL {
            if self.contains(fact) {
                set.entry(&fact);
            }
        }
        let known = Fact::ALL.iter().fold(0, |bits, fact| bits | fact.bit());
        if self.0 & !known != 0 {
            set.entry(&format_args!("{:#x}", self.0 & !known));
        }
        set.finish()
    }
}

/// What the host knows about the connection. A fact is known or not, and a
/// known fact is supported or not; C keeps the two apart, and so does this
/// record. A profile without requirements (drafts 16 and 18) reads none.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Facts {
    known: u32,
    supported: u32,
}

impl Facts {
    /// Nothing known yet.
    pub const UNKNOWN: Facts = Facts {
        known: 0,
        supported: 0,
    };

    /// Every fact known and true: a completed QUIC handshake with datagrams
    /// in both directions.
    #[must_use]
    pub fn all_supported() -> Facts {
        let mut facts = Facts::UNKNOWN;
        for fact in Fact::ALL {
            facts.set(fact, true);
        }
        facts
    }

    /// Record that `fact` is known to be `supported`.
    pub fn set(&mut self, fact: Fact, supported: bool) {
        self.known |= fact.bit();
        if supported {
            self.supported |= fact.bit();
        } else {
            self.supported &= !fact.bit();
        }
    }

    /// Forget `fact`: it is no longer known.
    pub fn clear(&mut self, fact: Fact) {
        self.known &= !fact.bit();
        self.supported &= !fact.bit();
    }

    /// `None` while `fact` is unknown, else whether it holds.
    #[must_use]
    pub fn get(&self, fact: Fact) -> Option<bool> {
        if self.known & fact.bit() == 0 {
            None
        } else {
            Some(self.supported & fact.bit() != 0)
        }
    }

    /// The facts with an answer.
    #[must_use]
    pub fn known(&self) -> FactSet {
        FactSet(self.known)
    }
}

/// The target a raw-QUIC client or a WebTransport client names: the URI
/// authority and the path with any query, as encoded (nothing is
/// normalized). C checks their RFC 3986 syntax at bind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionTarget {
    authority: Vec<u8>,
    path_query: Vec<u8>,
}

impl ConnectionTarget {
    /// A target with both components present. An empty `path_query` is
    /// present and empty.
    #[must_use]
    pub fn new(authority: impl Into<Vec<u8>>, path_query: impl Into<Vec<u8>>) -> ConnectionTarget {
        ConnectionTarget {
            authority: authority.into(),
            path_query: path_query.into(),
        }
    }

    /// The authority.
    #[must_use]
    pub fn authority(&self) -> &[u8] {
        &self.authority
    }

    /// The path and query.
    #[must_use]
    pub fn path_query(&self) -> &[u8] {
        &self.path_query
    }
}

/// The target a peer named in its setup, as the server's URI policy sees it.
/// `None` is an absent component; `Some(&[])` is present and empty: C
/// distinguishes them, and so does the policy.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UriTarget<'call> {
    /// The URI authority, as encoded.
    pub authority: Option<&'call [u8]>,
    /// The path and query, as encoded.
    pub path_query: Option<&'call [u8]>,
}

/// A raw-QUIC server's answer to a peer's target (`MOQ_URI_*`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UriVerdict {
    /// Serve the session.
    Accept,
    /// Refuse it: this server does not serve the authority.
    InvalidAuthority,
    /// Refuse it: this server does not serve the path.
    InvalidPath,
}

/// A raw-QUIC server's URI policy.
///
/// The session calls it inside its own advancing calls, on the host's
/// thread, once for each peer setup it is about to accept; it may ask again
/// for the same setup if accepting it has to be retried. Its contract is
/// C's: serialized, nonblocking, free of side effects and repeatable. It
/// receives only the target, so it cannot reach the session. A panic is
/// caught at the boundary and closes the session (defence in depth: under
/// `panic=abort` the process aborts instead). The bound is `Send` so the
/// policy can move with a session owner that is itself allowed to move.
pub type UriPolicy = Box<dyn Fn(UriTarget<'_>) -> UriVerdict + Send + 'static>;

/// How the session is carried.
enum Carrier {
    RawQuicClient(ConnectionTarget),
    RawQuicServer(UriPolicy),
    WebTransport(ConnectionTarget),
}

/// The binding of a session to its transport (`moq_transport_binding_t`):
/// how it is carried, the facts about the connection, and the client's
/// target or the server's URI policy. Draft 22 requires one before
/// [`Session::start`](crate::Session::start); drafts 16 and 18 accept one.
pub struct TransportBinding {
    carrier: Carrier,
    facts: Facts,
}

impl TransportBinding {
    /// A raw-QUIC client naming `target`.
    #[must_use]
    pub fn raw_quic_client(target: ConnectionTarget, facts: Facts) -> TransportBinding {
        TransportBinding {
            carrier: Carrier::RawQuicClient(target),
            facts,
        }
    }

    /// A raw-QUIC server deciding each peer's target with `policy`.
    #[must_use]
    pub fn raw_quic_server(policy: UriPolicy, facts: Facts) -> TransportBinding {
        TransportBinding {
            carrier: Carrier::RawQuicServer(policy),
            facts,
        }
    }

    /// A session inside a WebTransport-over-HTTP/3 session, naming the
    /// target that session was established for.
    #[must_use]
    pub fn webtransport(target: ConnectionTarget, facts: Facts) -> TransportBinding {
        TransportBinding {
            carrier: Carrier::WebTransport(target),
            facts,
        }
    }

    /// The facts, to update before a retry.
    pub fn facts_mut(&mut self) -> &mut Facts {
        &mut self.facts
    }

    /// The facts.
    #[must_use]
    pub fn facts(&self) -> &Facts {
        &self.facts
    }
}

impl fmt::Debug for TransportBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let carrier = match &self.carrier {
            Carrier::RawQuicClient(target) => format!("raw QUIC client {target:?}"),
            Carrier::RawQuicServer(_) => "raw QUIC server".to_owned(),
            Carrier::WebTransport(target) => format!("WebTransport {target:?}"),
        };
        f.debug_struct("TransportBinding")
            .field("carrier", &carrier)
            .field("facts", &self.facts)
            .finish()
    }
}

/// Why C refused a bind (`moq_session_bind_transport`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BindRefusal {
    /// A fact the profile requires is not known yet: set it and bind again
    /// (`MOQ_ERR_WOULD_BLOCK`).
    FactsPending,
    /// A required fact is known false (`MOQ_ERR_UNSUPPORTED`).
    Unsupported,
    /// The binding is malformed: a role, target or URI syntax C refuses
    /// (`MOQ_ERR_INVAL`).
    Invalid,
    /// The session is already bound, started or closed
    /// (`MOQ_ERR_WRONG_STATE`).
    WrongState,
    /// The target could not be copied (`MOQ_ERR_NOMEM`).
    NoMemory,
    /// A code this crate does not know.
    Other(Error),
}

impl BindRefusal {
    fn from_error(error: Error) -> BindRefusal {
        match error {
            Error::WOULD_BLOCK => BindRefusal::FactsPending,
            Error::UNSUPPORTED => BindRefusal::Unsupported,
            Error::INVAL => BindRefusal::Invalid,
            Error::WRONG_STATE => BindRefusal::WrongState,
            Error::NOMEM => BindRefusal::NoMemory,
            other => BindRefusal::Other(other),
        }
    }

    /// The native code.
    pub fn error(self) -> Error {
        match self {
            BindRefusal::FactsPending => Error::WOULD_BLOCK,
            BindRefusal::Unsupported => Error::UNSUPPORTED,
            BindRefusal::Invalid => Error::INVAL,
            BindRefusal::WrongState => Error::WRONG_STATE,
            BindRefusal::NoMemory => Error::NOMEM,
            BindRefusal::Other(error) => error,
        }
    }
}

/// A refused bind. C kept nothing and left the session unchanged, so the
/// binding comes back whole, its URI policy included, for a retry. The pair
/// is the whole record; it is not `#[non_exhaustive]`.
#[must_use]
pub struct BindRefused {
    /// The binding, untouched.
    pub binding: TransportBinding,
    /// Why C refused it.
    pub reason: BindRefusal,
}

impl fmt::Debug for BindRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BindRefused")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for BindRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transport bind refused: {}", self.reason.error())
    }
}

impl std::error::Error for BindRefused {}

/// Converting drops the binding deliberately, keeping only the code.
impl From<BindRefused> for Error {
    fn from(refused: BindRefused) -> Error {
        refused.reason.error()
    }
}

/// The heap cell C's policy context points at. It has its own allocation,
/// so moving the session owner (or the binding) never moves it.
pub(super) struct PolicyCell {
    policy: UriPolicy,
}

/// The verdict C reads as "fail closed": any value other than the three.
const FAIL_CLOSED: u32 = u32::MAX;

unsafe extern "C" fn accept_uri(
    ctx: *mut c_void,
    present: u32,
    authority: sys::moq_bytes_t,
    path_query: sys::moq_bytes_t,
) -> u32 {
    // SAFETY: ctx is the PolicyCell this module passed at bind, which the
    // session owner keeps alive until after the session is destroyed; C
    // calls the policy only from inside an advancing call on that session,
    // serialized with every other call, so no other reference to the cell
    // is in use.
    let cell = unsafe { &*(ctx as *const PolicyCell) };
    // SAFETY: C states the spans are borrowed for this call; a component
    // whose presence bit is clear is not read.
    let component = |bit: u32, span: sys::moq_bytes_t| -> Result<Option<&[u8]>, Error> {
        if present & bit == 0 {
            Ok(None)
        } else {
            // SAFETY: as above.
            unsafe { raw::record_span(span) }.map(Some)
        }
    };
    let (Ok(authority), Ok(path_query)) = (
        component(sys::MOQ_BINDING_AUTHORITY_PRESENT, authority),
        component(sys::MOQ_BINDING_PATH_PRESENT, path_query),
    ) else {
        return FAIL_CLOSED;
    };
    let target = UriTarget {
        authority,
        path_query,
    };
    // No unwind crosses into C: a panicking policy fails the setup closed.
    match catch_unwind(AssertUnwindSafe(|| (cell.policy)(target))) {
        Ok(UriVerdict::Accept) => sys::MOQ_URI_ACCEPT,
        Ok(UriVerdict::InvalidAuthority) => sys::MOQ_URI_INVALID_AUTHORITY,
        Ok(UriVerdict::InvalidPath) => sys::MOQ_URI_INVALID_PATH,
        Err(_) => FAIL_CLOSED,
    }
}

/// A binding converted for one bind call: the native record, and the owner
/// of what it points at (the target's bytes or the policy's cell).
pub(super) struct Prepared {
    pub(super) raw: sys::moq_transport_binding_t,
    pub(super) owner: Owner,
    facts: Facts,
}

/// What a prepared record points into.
pub(super) enum Owner {
    Client(ConnectionTarget),
    WebTransport(ConnectionTarget),
    Server(Box<PolicyCell>),
}

impl TransportBinding {
    /// Split the binding into its native record and the owner of what that
    /// record points at. The record's pointers stay valid while the owner is
    /// alive and unmodified: they point into the target's heap buffers or the
    /// policy's heap cell, neither of which moves when the owner moves.
    pub(super) fn prepare(self) -> Prepared {
        // SAFETY: zero is a valid bit pattern for this plain C record; the
        // initializer stamps it.
        let mut raw: sys::moq_transport_binding_t = unsafe { std::mem::zeroed() };
        // SAFETY: a writable record of exactly the size passed.
        unsafe {
            sys::moq_transport_binding_init_sized(
                &mut raw,
                std::mem::size_of::<sys::moq_transport_binding_t>(),
            )
        };
        raw.facts_known = self.facts.known;
        raw.facts_supported = self.facts.supported;
        let owner = match self.carrier {
            Carrier::RawQuicClient(target) => {
                raw.kind = sys::MOQ_BINDING_RAW_QUIC;
                Owner::Client(target)
            }
            Carrier::WebTransport(target) => {
                raw.kind = sys::MOQ_BINDING_WEBTRANSPORT_H3;
                Owner::WebTransport(target)
            }
            Carrier::RawQuicServer(policy) => {
                raw.kind = sys::MOQ_BINDING_RAW_QUIC;
                Owner::Server(Box::new(PolicyCell { policy }))
            }
        };
        match &owner {
            Owner::Client(target) | Owner::WebTransport(target) => {
                raw.target_present = sys::MOQ_BINDING_TARGET_COMPLETE;
                raw.authority = raw::span(&target.authority);
                raw.path_query = raw::span(&target.path_query);
            }
            Owner::Server(cell) => {
                raw.accept_uri = Some(accept_uri);
                raw.accept_uri_ctx = &**cell as *const PolicyCell as *mut c_void;
            }
        }
        Prepared {
            raw,
            owner,
            facts: self.facts,
        }
    }
}

impl Prepared {
    /// The binding back, after C refused it.
    pub(super) fn refused(self, error: Error) -> BindRefused {
        let carrier = match self.owner {
            Owner::Client(target) => Carrier::RawQuicClient(target),
            Owner::WebTransport(target) => Carrier::WebTransport(target),
            Owner::Server(cell) => Carrier::RawQuicServer(cell.policy),
        };
        BindRefused {
            binding: TransportBinding {
                carrier,
                facts: self.facts,
            },
            reason: BindRefusal::from_error(error),
        }
    }

    /// After C accepted it: the policy cell, which must outlive the session.
    pub(super) fn into_policy(self) -> Option<Box<PolicyCell>> {
        match self.owner {
            Owner::Server(cell) => Some(cell),
            Owner::Client(_) | Owner::WebTransport(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn facts_keep_known_and_supported_apart() {
        let mut facts = Facts::UNKNOWN;
        assert_eq!(facts.get(Fact::HandshakeComplete), None);
        facts.set(Fact::HandshakeComplete, false);
        assert_eq!(facts.get(Fact::HandshakeComplete), Some(false));
        facts.set(Fact::HandshakeComplete, true);
        assert_eq!(facts.get(Fact::HandshakeComplete), Some(true));
        facts.clear(Fact::HandshakeComplete);
        assert_eq!(facts.get(Fact::HandshakeComplete), None);
        let all = Facts::all_supported();
        assert_eq!(all.known().bits(), sys::MOQ_BINDING_FACTS_ALL);
        let set = FactSet::from_bits(sys::MOQ_BINDING_DATAGRAM_SEND | 0x100);
        assert!(set.contains(Fact::DatagramSend));
        assert_eq!(format!("{set:?}"), "{DatagramSend, 0x100}");
        assert_eq!(
            BindRefusal::from_error(Error::from_code(-77))
                .error()
                .code(),
            -77
        );
    }

    #[test]
    fn a_prepared_binding_comes_back_whole() {
        let target = ConnectionTarget::new("example.test", "/moq?x=1");
        let binding = TransportBinding::raw_quic_client(target.clone(), Facts::all_supported());
        let prepared = binding.prepare();
        assert_eq!(prepared.raw.kind, sys::MOQ_BINDING_RAW_QUIC);
        assert_eq!(
            prepared.raw.target_present,
            sys::MOQ_BINDING_TARGET_COMPLETE
        );
        assert_eq!(prepared.raw.authority.len, 12);
        let refused = prepared.refused(Error::WOULD_BLOCK);
        assert_eq!(refused.reason, BindRefusal::FactsPending);
        match refused.binding.carrier {
            Carrier::RawQuicClient(back) => assert_eq!(back, target),
            _ => panic!("the client binding comes back as a client binding"),
        }
    }
}
