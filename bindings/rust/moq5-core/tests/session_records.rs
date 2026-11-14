//! Polled records and request handles at the public boundary: a foreign
//! handle is refused before C; the close reason is copied into the owner;
//! `into_owned` copies every borrowed span and the payload; a payload leaves
//! the session only as a copy with storage of its own; time is the host's.

use std::time::Duration;

use moq5_core::{
    AcceptSubscribe, ActionDetail, ActionPoll, Buffer, Error, Event, Instant, Location,
    LocationFilter, Namespace, OrderKey, OwnedActionDetail, Perspective, Session, SessionConfig,
    SessionState, StreamRef, SubgroupConfig, SubscribeConfig, Tick, Version,
};

// The host and its transport are shared with the example and the other
// tests, which use parts of them this file does not.
#[allow(dead_code)]
#[path = "../examples/transport_host/host.rs"]
mod host;
#[allow(dead_code)]
#[path = "../examples/transport_host/wire.rs"]
mod wire;

use host::{Options, Pair, CLIENT, SERVER};

const PASSES: u32 = 400;
const T0: Instant = Instant::from_micros(1);
const VERSIONS: [Version; 3] = [Version::DRAFT_16, Version::DRAFT_18, Version::DRAFT_22];

fn refcount(buffer: &Buffer) -> u32 {
    // SAFETY: a live rcbuf the buffer owns; the call only reads its count.
    unsafe { moq5_core::sys::moq_rcbuf_refcount(buffer.as_ptr()) }
}

/// A pair with one accepted subscription: (pair, client's, server's).
fn subscribed(version: Version) -> (Pair, moq5_core::Subscription, moq5_core::Subscription) {
    let mut pair = Pair::new(version, &Options::default()).expect("a pair");
    assert!(pair.run(PASSES) && pair.established());
    pair.drain(CLIENT);
    pair.drain(SERVER);
    let config = SubscribeConfig::new(
        Namespace::parse("live").expect("a namespace"),
        "t",
        LocationFilter::AbsoluteStart(Location::default()),
    );
    let now = pair.now;
    let client = pair
        .session(CLIENT)
        .subscribe(&config, now)
        .expect("subscribe");
    assert!(pair.run(PASSES));
    let server = pair
        .drain(SERVER)
        .iter()
        .find_map(|s| s.subscription)
        .expect("the request's subscription");
    let now = pair.now;
    pair.session(SERVER)
        .accept_subscribe(server, &AcceptSubscribe::default(), now)
        .expect("accept");
    assert!(pair.run(PASSES));
    pair.drain(CLIENT);
    (pair, client, server)
}

#[test]
fn a_foreign_handle_is_refused_before_c() {
    let (mut a, client_a, server_a) = subscribed(Version::DRAFT_18);
    let (mut b, _, _) = subscribed(Version::DRAFT_18);
    let now = b.now;
    // The same native value may exist in both sessions; the owner decides.
    assert_eq!(
        b.session(CLIENT).unsubscribe(client_a, now),
        Err(Error::WRONG_SESSION)
    );
    assert_eq!(
        b.session(SERVER)
            .open_subgroup(server_a, &SubgroupConfig::new(0), now)
            .err(),
        Some(Error::WRONG_SESSION)
    );
    // A refused write hands the payload back untouched.
    let now = a.now;
    let group = a
        .session(SERVER)
        .open_subgroup(server_a, &SubgroupConfig::new(0), now)
        .expect("a subgroup");
    let payload = Buffer::copy_from(b"not for b").expect("a payload");
    let raw = payload.as_ptr();
    let refused = b
        .session(SERVER)
        .write_object(group, 0, payload, now)
        .expect_err("a subgroup of another session");
    assert_eq!(refused.error, Error::WRONG_SESSION);
    let payload = refused.payload.expect("the payload comes back");
    assert_eq!(payload.as_ptr(), raw);
    assert_eq!(refcount(&payload), 1);
    // The issuing session still takes it.
    a.session(SERVER)
        .write_object(group, 0, payload, now)
        .expect("its own session");
}

#[test]
fn the_close_reason_is_copied_into_the_owner() {
    for version in VERSIONS {
        let mut session = Session::try_new(&SessionConfig::new(Perspective::Client, version), T0)
            .expect("a session");
        let reason = String::from("going away now");
        session.close(0x7, &reason, T0).expect("a close");
        drop(reason);
        assert_eq!(session.state(), SessionState::Closed);
        match session.poll_action().expect("a poll") {
            ActionPoll::Ready(action) => match action.detail() {
                ActionDetail::CloseSession { code, reason } => {
                    assert_eq!(*code, 0x7);
                    assert_eq!(*reason, b"going away now");
                }
                other => panic!("expected the close action, got {other:?}"),
            },
            other => panic!("expected an action, got {other:?}"),
        }
        match session.poll_event().expect("a poll") {
            Some(Event::SessionClosed(closed)) => {
                assert_eq!(closed.code, 0x7);
                assert_eq!(closed.reason, b"going away now");
            }
            other => panic!("expected the close event, got {other:?}"),
        }
        assert!(matches!(session.poll_action(), Ok(ActionPoll::Empty)));
        assert!(session.poll_event().expect("a poll").is_none());
        // A second close changes nothing; an interior NUL is refused.
        session.close(9, "again", T0).expect("idempotent");
        assert!(session.poll_event().expect("a poll").is_none());
        assert_eq!(session.close(9, "a\0b", T0), Err(Error::INVAL));
    }
}

#[test]
fn into_owned_copies_every_borrowed_span() {
    for version in VERSIONS {
        let mut session = Session::try_new(&SessionConfig::new(Perspective::Client, version), T0)
            .expect("a session");
        if !session.binding_requirements().is_empty() {
            session
                .bind_transport(moq5_core::TransportBinding::raw_quic_client(
                    moq5_core::ConnectionTarget::new("relay.example", ""),
                    moq5_core::Facts::all_supported(),
                ))
                .expect("a bind");
        }
        session.start(T0).expect("a start");
        let action = match session.poll_action().expect("a poll") {
            ActionPoll::Ready(action) => action,
            other => panic!("the setup action, got {other:?}"),
        };
        assert_eq!(action.order_key(), OrderKey::Control);
        let (borrowed, borrowed_ptr) = match action.detail() {
            ActionDetail::SendControl { bytes } | ActionDetail::OpenUniControl { bytes, .. } => {
                (bytes.to_vec(), bytes.as_ptr())
            }
            other => panic!("the setup action, got {other:?}"),
        };
        let owned = action
            .into_owned()
            .map_err(|(_, e)| e)
            .expect("an owned copy");
        // The borrow has ended: the session advances while the copy lives.
        let _ = session.tick(T0);
        let bytes = match owned.detail() {
            OwnedActionDetail::SendControl { bytes }
            | OwnedActionDetail::OpenUniControl { bytes, .. } => bytes,
            other => panic!("the same action, got {other:?}"),
        };
        assert_eq!(*bytes, borrowed);
        assert_ne!(
            bytes.as_ptr(),
            borrowed_ptr,
            "a copy, not the session's buffer"
        );
        assert_eq!(owned.order_key(), OrderKey::Control);
    }
}

#[test]
fn payloads_leave_only_as_copies() {
    for version in VERSIONS {
        let (mut pair, _, server) = subscribed(version);
        let now = pair.now;
        let group = pair
            .session(SERVER)
            .open_subgroup(server, &SubgroupConfig::new(2), now)
            .expect("a subgroup");
        let input = Buffer::copy_from(b"object bytes").expect("a payload");
        let input_ptr = input.as_slice().as_ptr();
        pair.session(SERVER)
            .write_object(group, 0, input, now)
            .expect("a write");

        // The publisher's action carries the payload; its owned form is a
        // buffer of its own.
        let mut owned_payload = None;
        while let ActionPoll::Ready(action) = pair.session(SERVER).poll_action().expect("a poll") {
            let is_payload = matches!(
                action.detail(),
                ActionDetail::SendData {
                    payload: Some(_),
                    ..
                }
            );
            if !is_payload {
                continue;
            }
            let owned = action
                .into_owned()
                .map_err(|(_, e)| e)
                .expect("an owned copy");
            if let OwnedActionDetail::SendData {
                payload: Some(payload),
                ..
            } = owned.into_detail()
            {
                owned_payload = Some(payload);
            }
            break;
        }
        let owned_payload = owned_payload.expect("the object's payload");
        assert_eq!(owned_payload.as_slice(), b"object bytes");
        assert_ne!(
            owned_payload.as_slice().as_ptr(),
            input_ptr,
            "copied, never shared"
        );
        assert_eq!(refcount(&owned_payload), 1);
        drop(pair);

        // The subscriber's event payload, through a full delivery.
        let (mut pair, client2, server2) = subscribed(version);
        let now = pair.now;
        let group = pair
            .session(SERVER)
            .open_subgroup(server2, &SubgroupConfig::new(2), now)
            .expect("a subgroup");
        pair.session(SERVER)
            .write_object(
                group,
                0,
                Buffer::copy_from(b"delivered").expect("a payload"),
                now,
            )
            .expect("a write");
        assert!(pair.run(PASSES));
        let mut buffer = None;
        while let Some(event) = pair.session(CLIENT).poll_event().expect("a poll") {
            if let Event::ObjectReceived(object) = event {
                assert_eq!(object.subscription, Some(client2));
                let payload = object.payload.expect("a payload");
                let event_ptr = payload.as_bytes().as_ptr();
                let copy = payload.into_buffer().expect("a copy");
                assert_ne!(copy.as_slice().as_ptr(), event_ptr);
                buffer = Some(copy);
            }
        }
        let buffer = buffer.expect("the object arrived");
        assert_eq!(buffer.as_slice(), b"delivered");
        assert_eq!(
            refcount(&buffer),
            1,
            "exclusive: it may move to another thread"
        );
        drop(pair);
        // The copy outlives the session and moves freely.
        let moved = std::thread::spawn(move || buffer.len())
            .join()
            .expect("the thread");
        assert_eq!(moved, 9);
    }
}

#[test]
fn time_is_the_hosts() {
    let config = SessionConfig::new(Perspective::Client, Version::DRAFT_18)
        .idle_timeout(Duration::from_millis(5));
    let mut session = Session::try_new(&config, T0).expect("a session");
    session.start(T0).expect("a start");
    let due = session.next_deadline().expect("the idle deadline");
    assert_eq!(due, Instant::from_micros(1 + 5_000));
    // Before the deadline, a tick changes nothing.
    assert_eq!(session.tick(Instant::from_micros(10)), Ok(Tick::Done));
    assert_ne!(session.state(), SessionState::Closed);
    assert_eq!(session.tick(due), Ok(Tick::Done));
    assert_eq!(session.state(), SessionState::Closed);
    let mut idle = false;
    while let Some(event) = session.poll_event().expect("a poll") {
        if let Event::SessionClosed(closed) = event {
            assert_eq!(closed.code, 0x11, "MOQ_CLOSE_IDLE_TIMEOUT");
            idle = true;
        }
    }
    assert!(idle);
    // A sub-microsecond timeout is refused, not rounded.
    let bad = SessionConfig::new(Perspective::Client, Version::DRAFT_18)
        .idle_timeout(Duration::from_nanos(1500));
    assert_eq!(Session::try_new(&bad, T0).err(), Some(Error::INVAL));
}

#[test]
fn route_and_retirement_answers() {
    for version in [Version::DRAFT_18, Version::DRAFT_22] {
        let (mut pair, _, _) = subscribed(version);
        let session = pair.session(SERVER);
        // Nothing is consumed by asking.
        assert_eq!(
            session.route_peer_uni(&[]),
            Ok(moq5_core::UniRoute::NeedMore)
        );
        assert_eq!(
            session.route_peer_uni(&[0xaf]),
            Ok(moq5_core::UniRoute::NeedMore)
        );
        assert_eq!(
            session.route_peer_uni(&[0x06]),
            Ok(moq5_core::UniRoute::UnknownType)
        );
        // A stream the session never stopped is not held.
        let stream = StreamRef::peer(999).expect("a peer ref");
        let now = pair.now;
        assert_eq!(
            pair.session(SERVER).on_data_stream_retired(stream, now),
            Ok(moq5_core::Retirement::NotHeld)
        );
        assert_eq!(
            pair.session(SERVER).on_bidi_stream_retired(stream, now),
            Ok(moq5_core::Retirement::NotHeld)
        );
    }
    let (mut pair, _, _) = subscribed(Version::DRAFT_16);
    // Draft 16: every peer unidirectional stream is data.
    assert_eq!(
        pair.session(SERVER).route_peer_uni(&[]),
        Ok(moq5_core::UniRoute::Data)
    );
}
