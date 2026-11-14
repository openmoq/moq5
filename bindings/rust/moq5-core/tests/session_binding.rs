//! The transport binding: draft 22 requires one before start; a refused
//! bind returns the binding whole, URI policy included, so a retry once the
//! missing fact is known succeeds; an absent and a present-empty target
//! component reach the server's policy distinctly; a policy that refuses or
//! panics fails the setup closed. Input refused before setup keeps the
//! native code each input returned, with its exact input and FIN.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use moq5_core::{
    BindRefusal, ConnectionTarget, Error, EventKind, Fact, Facts, FeedError, Instant,
    NotAdmittedReason, Perspective, Session, SessionConfig, SessionState, StreamRef,
    TransportBinding, UriTarget, UriVerdict, Version,
};

// The host and its transport are shared with the example and the other
// tests, which use parts of them this file does not.
#[allow(dead_code)]
#[path = "../examples/transport_host/host.rs"]
mod host;
#[allow(dead_code)]
#[path = "../examples/transport_host/wire.rs"]
mod wire;

use host::{Options, Pair, SharedPolicy, CLIENT, SERVER};

const PASSES: u32 = 400;
const T0: Instant = Instant::from_micros(1);

fn session(perspective: Perspective, version: Version) -> Session {
    Session::try_new(&SessionConfig::new(perspective, version), T0).expect("a session")
}

#[test]
fn draft_22_starts_only_once_bound() {
    let mut client = session(Perspective::Client, Version::DRAFT_22);
    assert!(!client.binding_requirements().is_empty());
    assert_eq!(client.start(T0), Err(Error::WRONG_STATE));
    let binding = TransportBinding::raw_quic_client(
        ConnectionTarget::new("relay.example", "/moq"),
        Facts::all_supported(),
    );
    client.bind_transport(binding).expect("a bind");
    client.start(T0).expect("a start once bound");
    assert_eq!(client.state(), SessionState::SetupSent);
    // Drafts 16 and 18 require nothing and start unbound.
    for version in [Version::DRAFT_16, Version::DRAFT_18] {
        let mut client = session(Perspective::Client, version);
        assert!(client.binding_requirements().is_empty());
        client.start(T0).expect("an unbound start");
    }
}

#[test]
fn a_refused_bind_returns_the_binding_and_its_policy() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let policy = Box::new(move |_: UriTarget<'_>| {
        seen.fetch_add(1, Ordering::SeqCst);
        UriVerdict::Accept
    });
    let mut server = session(Perspective::Server, Version::DRAFT_22);
    // No fact known yet: C keeps nothing and the binding comes back.
    let refused = server
        .bind_transport(TransportBinding::raw_quic_server(policy, Facts::UNKNOWN))
        .expect_err("facts are pending");
    assert_eq!(refused.reason, BindRefusal::FactsPending);
    assert_eq!(refused.reason.error(), Error::WOULD_BLOCK);
    let mut binding = refused.binding;
    assert_eq!(binding.facts().get(Fact::HandshakeComplete), None);
    for fact in Fact::ALL {
        binding.facts_mut().set(fact, true);
    }
    server
        .bind_transport(binding)
        .expect("the retry with the fact known");
    // A second bind is refused: the session is bound.
    let again =
        TransportBinding::raw_quic_server(Box::new(|_| UriVerdict::Accept), Facts::all_supported());
    let refused = server.bind_transport(again).expect_err("already bound");
    assert_eq!(refused.reason, BindRefusal::WrongState);
    drop(server);
    // The returned policy is the one that was bound: nothing called it, and
    // dropping the session dropped it (the counter is ours alone again).
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(Arc::strong_count(&calls), 1);
}

#[test]
fn a_malformed_target_is_refused_whole() {
    let mut client = session(Perspective::Client, Version::DRAFT_22);
    let binding =
        TransportBinding::raw_quic_client(ConnectionTarget::new("", "/x"), Facts::all_supported());
    let refused = client
        .bind_transport(binding)
        .expect_err("an empty host is refused");
    assert_eq!(refused.reason, BindRefusal::Invalid);
    let mut binding = refused.binding;
    binding.facts_mut().clear(Fact::DatagramSend);
    assert_eq!(binding.facts().get(Fact::DatagramSend), None);
    // A server binding on a client perspective is a role mismatch.
    let server_binding =
        TransportBinding::raw_quic_server(Box::new(|_| UriVerdict::Accept), Facts::all_supported());
    assert_eq!(
        client
            .bind_transport(server_binding)
            .expect_err("role mismatch")
            .reason,
        BindRefusal::Invalid
    );
    assert_eq!(
        client.state(),
        SessionState::Idle,
        "every refusal leaves the session unchanged"
    );
}

type Recorded = Arc<Mutex<Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>>>;

fn recording(verdict: UriVerdict) -> (SharedPolicy, Recorded) {
    let recorded: Recorded = Arc::default();
    let sink = Arc::clone(&recorded);
    let policy: SharedPolicy = Arc::new(move |target: UriTarget<'_>| {
        sink.lock().expect("the record lock").push((
            target.authority.map(<[u8]>::to_vec),
            target.path_query.map(<[u8]>::to_vec),
        ));
        verdict
    });
    (policy, recorded)
}

#[test]
fn absent_and_present_empty_targets_reach_the_policy_distinctly() {
    // Draft 18: a binding is optional, so an unbound client names no target.
    let (policy, recorded) = recording(UriVerdict::Accept);
    let options = Options {
        server_policy: Some(Arc::clone(&policy)),
        ..Options::default()
    };
    let mut pair = Pair::new(Version::DRAFT_18, &options).expect("a pair");
    assert!(pair.run(PASSES));
    assert!(pair.established());
    {
        let calls = recorded.lock().expect("the record lock");
        assert!(!calls.is_empty());
        assert!(calls.iter().all(|call| *call == (None, None)), "{calls:?}");
    }
    recorded.lock().expect("the record lock").clear();

    // A bound client with an empty path: present and empty.
    let options = Options {
        server_policy: Some(policy),
        client_target: Some((b"relay.example".to_vec(), Vec::new())),
        ..Options::default()
    };
    let mut pair = Pair::new(Version::DRAFT_18, &options).expect("a pair");
    assert!(pair.run(PASSES));
    assert!(pair.established());
    let calls = recorded.lock().expect("the record lock");
    assert!(!calls.is_empty());
    assert!(
        calls
            .iter()
            .all(|call| *call == (Some(b"relay.example".to_vec()), Some(Vec::new()))),
        "{calls:?}"
    );
}

#[test]
fn a_refusing_or_panicking_policy_fails_the_setup_closed() {
    let panicking: SharedPolicy = Arc::new(|_: UriTarget<'_>| -> UriVerdict {
        panic!("a policy that panics");
    });
    let (refusing, _) = recording(UriVerdict::InvalidPath);
    for policy in [refusing, panicking] {
        for version in [Version::DRAFT_18, Version::DRAFT_22] {
            let options = Options {
                server_policy: Some(policy.clone()),
                client_target: Some((b"relay.example".to_vec(), b"/nowhere".to_vec())),
                ..Options::default()
            };
            let mut pair = Pair::new(version, &options).expect("a pair");
            pair.run(PASSES);
            assert_eq!(pair.hosts[SERVER].session.state(), SessionState::Closed);
            let server = pair.drain(SERVER);
            assert!(server
                .iter()
                .any(|s| s.kind == Some(EventKind::SessionClosed)));
            assert_ne!(
                pair.hosts[CLIENT].session.state(),
                SessionState::Established
            );
        }
    }
}

#[test]
fn setup_refusals_keep_the_native_code() {
    // A draft-16 server before the peer's setup.
    let mut server = session(Perspective::Server, Version::DRAFT_16);
    let stream = StreamRef::peer(0).expect("a peer ref");
    let bytes: &[u8] = b"request";

    // A bidirectional stream: WRONG_STATE, input and FIN as given.
    match server.on_bidi_bytes(stream, bytes, true, T0) {
        Err(FeedError::NotAdmitted(refused)) => {
            assert_eq!(refused.reason, NotAdmittedReason::SetupIncomplete);
            assert_eq!(refused.error, Error::WRONG_STATE);
            assert!(std::ptr::eq(refused.input, bytes), "the very input");
            assert!(refused.fin);
            assert_eq!(
                Error::from(FeedError::NotAdmitted(refused)),
                Error::WRONG_STATE
            );
        }
        other => panic!("bidi bytes before setup: {other:?}"),
    }
    // A datagram: WRONG_STATE.
    match server.on_datagram(bytes, T0) {
        Err(FeedError::NotAdmitted(refused)) => {
            assert_eq!(refused.reason, NotAdmittedReason::SetupIncomplete);
            assert_eq!(refused.error, Error::WRONG_STATE);
            assert!(std::ptr::eq(refused.input, bytes));
            assert!(!refused.fin);
        }
        other => panic!("a datagram before setup: {other:?}"),
    }
    // A data stream: CLOSED, on a session that is not closed.
    match server.on_data_bytes(stream, bytes, false, T0) {
        Err(FeedError::NotAdmitted(refused)) => {
            assert_eq!(refused.reason, NotAdmittedReason::SetupIncomplete);
            assert_eq!(refused.error, Error::CLOSED);
            assert!(std::ptr::eq(refused.input, bytes));
            assert!(!server.is_closed());
        }
        other => panic!("data bytes before setup: {other:?}"),
    }

    // A draft-22 session with no binding: the same WRONG_STATE is a failure.
    let mut unbound = session(Perspective::Server, Version::DRAFT_22);
    match unbound.on_bidi_bytes(stream, bytes, false, T0) {
        Err(FeedError::Failed(error)) => assert_eq!(error, Error::WRONG_STATE),
        other => panic!("bidi bytes with no binding: {other:?}"),
    }
}
