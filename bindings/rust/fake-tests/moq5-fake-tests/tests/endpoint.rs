//! Endpoint rows: configuration mapping, durations, teardown order, the
//! sized terminal snapshot, the latch, GOAWAY and raw adoption.

use std::time::Duration;

use moq5::{
    AuthToken, Backend, Endpoint, EndpointConfig, EndpointState, Error, Namespace, Protocol,
    Receiver, ReceiverConfig, TerminalReason, UriMode, Version, VersionOffer, WaitResult,
    WtProfile,
};
use moq5_fake_tests as fake;
use moq5_fake_tests::{sys, Suite};

fn connect(s: &Suite) -> Endpoint {
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433"))
        .expect("the provider accepts the connect");
    s.take_log();
    endpoint
}

#[test]
fn connect_maps_every_field_through_the_sized_initializer() {
    let s = fake::suite();
    let config = EndpointConfig::new("https://relay:4443/moq")
        .protocol(Protocol::WebTransport)
        .backend(Backend::Picoquic)
        .versions(VersionOffer::List(vec![
            Version::DRAFT_22,
            Version::DRAFT_18,
        ]))
        .sni("relay.example")
        .ca_file("/ca.pem")
        .insecure_skip_verify(true)
        .wt_path("/alt")
        .wt_profile(WtProfile::Current)
        .handshake_timeout(Duration::from_millis(1500))
        .setup_auth_tokens(vec![
            AuthToken {
                token_type: 7,
                value: b"abc".to_vec(),
            },
            AuthToken {
                token_type: 9,
                value: b"xyz".to_vec(),
            },
        ])
        .uri_mode(UriMode::Standard)
        .preferred_version(Version::DRAFT_22);
    let endpoint = Endpoint::connect(&config).expect("the provider accepts the connect");
    let connect = fake::line(&s.take_log(), "connect ");
    assert_eq!(fake::field(&connect, "url"), "https://relay:4443/moq");
    assert_eq!(
        fake::field(&connect, "protocol"),
        sys::MOQ_TRANSPORT_PROTOCOL_WEBTRANSPORT.to_string()
    );
    assert_eq!(
        fake::field(&connect, "backend"),
        sys::MOQ_TRANSPORT_BACKEND_PICOQUIC.to_string()
    );
    assert_eq!(
        fake::field(&connect, "policy"),
        sys::MOQ_VERSION_POLICY_LIST.to_string()
    );
    assert_eq!(fake::field(&connect, "versions"), "22,18");
    assert_eq!(fake::field(&connect, "sni"), "relay.example");
    assert_eq!(fake::field(&connect, "ca_file"), "/ca.pem");
    assert_eq!(fake::field(&connect, "insecure"), "1");
    assert_eq!(fake::field(&connect, "wt_path"), "/alt");
    assert_eq!(
        fake::field(&connect, "perspective"),
        sys::MOQ_PERSPECTIVE_CLIENT.to_string()
    );
    assert_eq!(fake::field(&connect, "alloc"), "0");
    assert_eq!(
        fake::field(&connect, "wt_profile"),
        sys::MOQ_WT_PROFILE_CURRENT.to_string()
    );
    assert_eq!(fake::field(&connect, "handshake_us"), "1500000");
    assert_eq!(fake::field(&connect, "tokens"), "7:abc;9:xyz");
    assert_eq!(fake::field(&connect, "verify_cb"), "0");
    assert_eq!(
        fake::field(&connect, "uri_mode"),
        sys::MOQ_ENDPOINT_URI_STANDARD.to_string()
    );
    assert_eq!(fake::field(&connect, "preferred"), "22");
    assert_eq!(fake::field(&connect, "accept_uri"), "0");
    assert_eq!(
        fake::field(&connect, "struct_size"),
        std::mem::size_of::<sys::moq_endpoint_cfg_t>().to_string()
    );
    drop(endpoint);
    assert_eq!(s.take_log(), ["stop", "destroy"]);
}

#[test]
fn connect_defaults_and_the_exact_offer() {
    let s = fake::suite();
    let endpoint = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433"))
        .expect("the provider accepts the connect");
    let connect = fake::line(&s.take_log(), "connect ");
    for (key, value) in [
        ("protocol", "0"),
        ("backend", "0"),
        ("policy", "0"),
        ("versions", ""),
        ("sni", ""),
        ("ca_file", ""),
        ("insecure", "0"),
        ("wt_path", ""),
        ("wt_profile", "0"),
        ("handshake_us", "0"),
        ("tokens", ""),
        ("uri_mode", "0"),
        ("preferred", "0"),
    ] {
        assert_eq!(fake::field(&connect, key), value, "{key}");
    }
    assert_eq!(
        fake::field(&connect, "struct_size"),
        std::mem::size_of::<sys::moq_endpoint_cfg_t>().to_string(),
        "always the sized initializer"
    );
    drop(endpoint);
    s.take_log();
    let endpoint = Endpoint::connect(
        &EndpointConfig::new("moqt://relay:4433").versions(VersionOffer::Exact(Version::DRAFT_16)),
    )
    .expect("the provider accepts the connect");
    let connect = fake::line(&s.take_log(), "connect ");
    assert_eq!(
        fake::field(&connect, "policy"),
        sys::MOQ_VERSION_POLICY_EXACT.to_string()
    );
    assert_eq!(fake::field(&connect, "versions"), "16");
    drop(endpoint);
}

#[test]
fn durations_are_exact_microseconds() {
    let s = fake::suite();
    let refused = Endpoint::connect(
        &EndpointConfig::new("moqt://relay:4433").handshake_timeout(Duration::from_nanos(1500)),
    );
    assert_eq!(refused.err(), Some(Error::INVAL), "a sub-microsecond value");
    let refused = Endpoint::connect(
        &EndpointConfig::new("moqt://relay:4433")
            .handshake_timeout(Duration::from_micros(u64::MAX) + Duration::from_micros(1)),
    );
    assert_eq!(refused.err(), Some(Error::INVAL), "beyond u64 microseconds");
    assert_eq!(
        s.get("connect"),
        0,
        "refused before the service was reached"
    );

    let endpoint = connect(&s);
    assert_eq!(
        endpoint.wait(Duration::ZERO),
        Ok(WaitResult::TimedOut),
        "zero polls"
    );
    assert_eq!(
        endpoint.wait(Duration::from_millis(250)),
        Ok(WaitResult::TimedOut)
    );
    assert_eq!(endpoint.wait(Duration::from_nanos(999)), Err(Error::INVAL));
    assert_eq!(
        endpoint.drain(Duration::from_secs(2)),
        Ok(WaitResult::TimedOut)
    );
    s.set("drain_result", i64::from(sys::MOQ_OK));
    assert_eq!(
        endpoint.drain(Duration::from_micros(7)),
        Ok(WaitResult::Ready)
    );
    assert_eq!(
        s.take_log(),
        ["wait 0", "wait 250000", "drain 2000000", "drain 7"]
    );
}

#[test]
fn endpoint_stops_after_its_last_handle() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let receiver = Receiver::attach(
        &endpoint,
        &ReceiverConfig::live(Namespace::parse("live/demo").expect("a two-part namespace parses")),
    )
    .expect("the provider accepts the receiver attach");
    assert_eq!(
        endpoint.stop(),
        Err(Error::WRONG_STATE),
        "refused while attached"
    );
    let log = s.take_log();
    assert!(log.iter().any(|l| l.starts_with("receiver_attach ")));
    assert_eq!(
        log.last().map(String::as_str),
        Some("stop"),
        "the refused stop"
    );
    let clone = endpoint.clone();
    drop(endpoint);
    drop(clone);
    assert!(
        s.take_log().is_empty(),
        "the receiver still holds the endpoint"
    );
    drop(receiver);
    assert_eq!(s.take_log(), ["receiver_destroy", "stop", "destroy"]);
}

#[test]
fn observers_and_the_sized_terminal() {
    let s = fake::suite();
    let endpoint = connect(&s);
    assert_eq!(endpoint.state(), EndpointState::Connecting);
    s.set("state", i64::from(sys::MOQ_ENDPOINT_ESTABLISHED));
    assert_eq!(endpoint.state(), EndpointState::Established);
    s.set("state", 77);
    assert_eq!(endpoint.state(), EndpointState::Other(77));
    assert_eq!(endpoint.negotiated_version(), None);
    s.set("negotiated_version", 22);
    assert_eq!(endpoint.negotiated_version(), Some(Version::DRAFT_22));
    assert!(!endpoint.is_closed());
    s.set("is_closed", 1);
    assert!(endpoint.is_closed());
    s.set("is_fatal", 1);
    s.set("fatal_code", 0x1234);
    assert!(endpoint.is_fatal());
    assert_eq!(endpoint.fatal_code(), 0x1234);

    let terminal = endpoint.terminal().expect("terminal on a live endpoint");
    assert_eq!(terminal.reason, TerminalReason::None);
    assert_eq!(terminal.detail_code, 0);
    s.set(
        "terminal_reason",
        i64::from(sys::MOQ_ENDPOINT_TERMINAL_TLS_CERTIFICATE),
    );
    s.set("terminal_detail", 0xdead);
    let terminal = endpoint.terminal().expect("terminal on a live endpoint");
    assert_eq!(terminal.reason, TerminalReason::TlsCertificate);
    assert_eq!(terminal.detail_code, 0xdead);
    s.set("terminal_reason", 99);
    assert_eq!(
        endpoint
            .terminal()
            .expect("terminal on a live endpoint")
            .reason,
        TerminalReason::Other(99)
    );
    s.set("terminal_stamp", 8);
    assert_eq!(
        endpoint.terminal(),
        Err(Error::ABI_MISMATCH),
        "below the v0 floor"
    );
    s.set("terminal_stamp", 999);
    assert_eq!(
        endpoint.terminal(),
        Err(Error::ABI_MISMATCH),
        "above what was passed"
    );
    s.set("terminal_stamp", 0);
    assert!(endpoint.terminal().is_ok());
    endpoint.wake();
    assert_eq!(s.get("wake"), 1);
}

#[test]
fn the_latch_is_sticky_until_cleared() {
    let s = fake::suite();
    let endpoint = connect(&s);
    endpoint.set_interrupted(true);
    assert_eq!(
        endpoint.wait(Duration::from_millis(1)),
        Err(Error::INTERRUPTED)
    );
    assert_eq!(
        endpoint.drain(Duration::from_millis(1)),
        Err(Error::INTERRUPTED)
    );
    endpoint.set_interrupted(false);
    assert_eq!(
        endpoint.wait(Duration::from_millis(1)),
        Ok(WaitResult::TimedOut)
    );
    assert_eq!(
        s.take_log(),
        [
            "set_interrupted 1",
            "wait 1000",
            "drain 1000",
            "set_interrupted 0",
            "wait 1000"
        ]
    );
}

#[test]
fn goaway_uri_sizes_its_buffer() {
    let s = fake::suite();
    let endpoint = connect(&s);
    assert_eq!(endpoint.goaway_uri(), Ok(None));
    // SAFETY: the provider copies the bytes.
    unsafe { moq5_fake_tests::moq5_fake_set_goaway(std::ptr::null(), 0, true) };
    assert_eq!(
        endpoint.goaway_uri(),
        Ok(Some(Vec::new())),
        "empty means the current URI"
    );
    let uri = b"moqt://next:4433";
    // SAFETY: as above.
    unsafe { moq5_fake_tests::moq5_fake_set_goaway(uri.as_ptr(), uri.len(), true) };
    let before = s.get("goaway");
    assert_eq!(endpoint.goaway_uri(), Ok(Some(uri.to_vec())));
    assert_eq!(s.get("goaway") - before, 2, "a sizing call, then the copy");
}

#[test]
fn unknown_codes_are_preserved() {
    let s = fake::suite();
    s.set("connect_result", -1000);
    let error = Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).unwrap_err();
    assert_eq!(error, Error::from_code(-1000));
    assert_eq!(error.code(), -1000);
    assert_eq!(format!("{error:?}"), "Error(-1000)");
    s.set("connect_result", i64::from(sys::MOQ_ERR_UNSUPPORTED));
    assert_eq!(
        Endpoint::connect(&EndpointConfig::new("moqt://relay:4433")).err(),
        Some(Error::UNSUPPORTED)
    );
}

#[test]
fn raw_handles_round_trip() {
    let s = fake::suite();
    let endpoint = connect(&s);
    let clone = endpoint.clone();
    let endpoint = endpoint.into_raw().expect_err("another handle exists");
    drop(clone);
    let raw = endpoint.into_raw().expect("the only handle");
    assert!(s.take_log().is_empty(), "nothing stopped");
    // SAFETY: the endpoint into_raw gave up, with no attachments.
    let endpoint = unsafe { Endpoint::from_raw(raw) };
    assert_eq!(endpoint.as_ptr(), raw);
    drop(endpoint);
    assert_eq!(s.take_log(), ["stop", "destroy"]);
}
