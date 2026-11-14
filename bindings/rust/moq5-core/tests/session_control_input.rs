//! Input and who keeps it. A delivery the session cannot take comes back
//! unchanged. Once the session took the bytes it keeps them: a message that
//! waits for output to drain is pending, and one whose handling can never
//! fit this side's own output capacity is a failure (`Error::LIMIT`), never
//! a refused delivery handed back for redelivery. Control input, and a
//! request stream's input.

use moq5_core::{
    ActionDetail, ActionPoll, AuthToken, Error, EventKind, Fed, FeedError, Instant, Location,
    LocationFilter, Namespace, NotAdmittedReason, Perspective, Session, SessionConfig,
    SessionState, SubscribeConfig, Version,
};

const T0: Instant = Instant::from_micros(1);

fn at(micros: u64) -> Instant {
    Instant::from_micros(micros)
}

/// The control bytes `session` queued, in order; every action taken.
fn control_bytes(session: &mut Session) -> Vec<u8> {
    let mut out = Vec::new();
    for _ in 0..64 {
        match session.poll_action().expect("a poll") {
            ActionPoll::Ready(action) => match action.detail() {
                ActionDetail::SendControl { bytes }
                | ActionDetail::OpenUniControl { bytes, .. } => {
                    out.extend_from_slice(bytes);
                }
                _ => {}
            },
            _ => break,
        }
    }
    out
}

/// Events `session` queued, as kinds; every event taken.
fn event_kinds(session: &mut Session) -> Vec<EventKind> {
    let mut kinds = Vec::new();
    for _ in 0..64 {
        match session.poll_event().expect("a poll") {
            Some(event) => kinds.push(event.kind()),
            None => break,
        }
    }
    kinds
}

/// The CLIENT_SETUP of a fresh draft-16 client.
fn client_setup() -> Vec<u8> {
    let config = SessionConfig::new(Perspective::Client, Version::DRAFT_16);
    let mut client = Session::try_new(&config, T0).expect("a client");
    client.start(T0).expect("a start");
    control_bytes(&mut client)
}

#[test]
fn a_delivery_the_control_buffer_cannot_take_comes_back() {
    let config = SessionConfig::new(Perspective::Server, Version::DRAFT_16).recv_buffer_size(128);
    let mut server = Session::try_new(&config, T0).expect("a server");
    let big = vec![0u8; 200];
    let refused = server
        .on_control_bytes(&big, at(2))
        .expect_err("more than the control buffer holds");
    let FeedError::NotAdmitted(refused) = refused else {
        panic!("a refused delivery");
    };
    assert!(std::ptr::eq(refused.input, &big[..]), "the same bytes");
    assert_eq!(
        (refused.reason, refused.error),
        (NotAdmittedReason::NoRoom, Error::BUFFER)
    );
    // Nothing was kept: a delivery that fits is taken whole.
    let setup = client_setup();
    assert!(matches!(
        server.on_control_bytes(&setup, at(3)),
        Ok(Fed::Accepted)
    ));
    assert_eq!(server.state(), SessionState::Established);
}

#[test]
fn a_message_waiting_for_send_room_stays_with_the_session() {
    let client_config = SessionConfig::new(Perspective::Client, Version::DRAFT_16);
    let mut client = Session::try_new(&client_config, T0).expect("a client");
    let server_config = SessionConfig::new(Perspective::Server, Version::DRAFT_16)
        .request_capacity(64)
        .send_buffer_size(64);
    let mut server = Session::try_new(&server_config, T0).expect("a server");
    client.start(T0).expect("a start");
    let setup = control_bytes(&mut client);
    assert!(matches!(
        server.on_control_bytes(&setup, at(2)),
        Ok(Fed::Accepted)
    ));
    let answer = control_bytes(&mut server);
    assert!(matches!(
        client.on_control_bytes(&answer, at(2)),
        Ok(Fed::Accepted)
    ));
    event_kinds(&mut client);
    event_kinds(&mut server);

    let live = Namespace::parse("live").expect("a namespace");
    let subscribe = SubscribeConfig::new(
        live,
        "video",
        LocationFilter::AbsoluteStart(Location::default()),
    );
    client.subscribe(&subscribe, at(3)).expect("a subscribe");
    let request = control_bytes(&mut client);
    assert!(!request.is_empty());

    // The server's GOAWAY fills most of its send buffer, unpolled; the
    // SUBSCRIBE it then refuses has to wait for that room.
    server.goaway(&[b'u'; 56], at(4)).expect("a GOAWAY");
    assert!(matches!(
        server.on_control_bytes(&request, at(5)),
        Ok(Fed::Pending)
    ));
    let goaway = control_bytes(&mut server);
    assert_eq!(goaway.first(), Some(&0x10), "only the GOAWAY so far");
    assert_eq!(server.process_pending(at(6)), Ok(Fed::Accepted));
    let refusal = control_bytes(&mut server);
    assert_eq!(refusal.first(), Some(&0x05), "one REQUEST_ERROR");
    assert_eq!(server.process_pending(at(7)), Ok(Fed::Accepted));
    assert!(control_bytes(&mut server).is_empty(), "sent once");
}

#[test]
fn a_server_setup_larger_than_the_send_buffer_is_a_limit_not_a_refusal() {
    let setup = client_setup();
    let config = SessionConfig::new(Perspective::Server, Version::DRAFT_16)
        .setup_auth_token(AuthToken {
            token_type: 1,
            value: vec![0x61; 96],
        })
        .send_buffer_size(64);
    let mut server = Session::try_new(&config, T0).expect("a server");
    let state = server.state();
    // The session took the bytes: the result is its failure, the input stays
    // with it, and nothing comes back for redelivery.
    match server.on_control_bytes(&setup, at(2)) {
        Err(FeedError::Failed(error)) => assert_eq!(error, Error::LIMIT),
        other => panic!("expected the admitted LIMIT failure, got {other:?}"),
    }
    assert_eq!(server.state(), state);
    assert!(control_bytes(&mut server).is_empty());
    assert!(event_kinds(&mut server).is_empty());
    for micros in 3..6 {
        assert_eq!(server.process_pending(at(micros)), Err(Error::LIMIT));
        assert_eq!(server.state(), state);
        assert!(control_bytes(&mut server).is_empty());
    }
}

/// The request streams `client` opened, with their first bytes.
fn request_streams(client: &mut Session) -> Vec<(moq5_core::StreamRef, Vec<u8>)> {
    let mut out = Vec::new();
    for _ in 0..64 {
        match client.poll_action().expect("a poll") {
            ActionPoll::Ready(action) => {
                if let ActionDetail::OpenBidiStream { stream, bytes, .. } = action.detail() {
                    out.push((*stream, bytes.to_vec()));
                }
            }
            _ => break,
        }
    }
    out
}

/// A draft-18 pair whose server's event arena is `size` bytes; two
/// SUBSCRIBE requests reach it. `None` when the arena is too small for the
/// setup or the first request, `Some(false)` when both requests fit, and
/// `Some(true)` once the second waited for the first one's room and then
/// surfaced.
fn second_request_waits(size: u32) -> Option<bool> {
    let client_config = SessionConfig::new(Perspective::Client, Version::DRAFT_18);
    let server_config =
        SessionConfig::new(Perspective::Server, Version::DRAFT_18).output_scratch_size(size);
    let mut client = Session::try_new(&client_config, T0).expect("a client");
    let mut server = Session::try_new(&server_config, T0).ok()?;
    client.start(T0).expect("a start");
    server.start(T0).ok()?;
    let setup = control_bytes(&mut client);
    if !matches!(server.on_control_bytes(&setup, at(2)), Ok(Fed::Accepted)) {
        return None;
    }
    let answer = control_bytes(&mut server);
    if !matches!(client.on_control_bytes(&answer, at(2)), Ok(Fed::Accepted)) {
        return None;
    }
    if server.state() != SessionState::Established {
        return None;
    }
    event_kinds(&mut client);
    event_kinds(&mut server);
    for name in ["a", "b"] {
        let live = Namespace::parse("live").expect("a namespace");
        let config = SubscribeConfig::new(
            live,
            name,
            LocationFilter::AbsoluteStart(Location::default()),
        );
        client.subscribe(&config, at(3)).expect("a subscribe");
    }
    let streams = request_streams(&mut client);
    assert_eq!(streams.len(), 2);
    let (first, second) = (&streams[0], &streams[1]);
    if !matches!(
        server.on_bidi_bytes(first.0, &first.1, false, at(4)),
        Ok(Fed::Accepted)
    ) {
        return None;
    }
    match server.on_bidi_bytes(second.0, &second.1, false, at(5)) {
        Ok(Fed::Accepted) => Some(false),
        Ok(Fed::Pending) => {
            assert!(server.has_transport_stream(second.0), "the session kept it");
            let kinds = event_kinds(&mut server);
            assert_eq!(
                kinds
                    .iter()
                    .filter(|k| **k == EventKind::SubscribeRequest)
                    .count(),
                1
            );
            assert_eq!(server.resume_bidi(second.0, at(6)), Ok(Fed::Accepted));
            let kinds = event_kinds(&mut server);
            assert_eq!(
                kinds
                    .iter()
                    .filter(|k| **k == EventKind::SubscribeRequest)
                    .count(),
                1,
                "the second request, once"
            );
            Some(true)
        }
        Err(FeedError::NotAdmitted(refused)) => {
            panic!("bytes the session took came back as refused: {refused:?}")
        }
        Err(FeedError::Failed(_)) => None,
    }
}

#[test]
fn a_request_waiting_for_event_room_stays_with_the_session() {
    let mut found = None;
    for size in (8..=4096).step_by(4) {
        if second_request_waits(size) == Some(true) {
            found = Some(size);
            break;
        }
    }
    assert!(
        found.is_some(),
        "an arena that holds one request's event, not two"
    );
}
