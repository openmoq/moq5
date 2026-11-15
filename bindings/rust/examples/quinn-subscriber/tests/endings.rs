//! The subscriber against a QUIC server on loopback that speaks no MoQ. The
//! server holds back credit and then breaks the protocol, or ends the
//! connection in a chosen way. What the subscriber prints and how it exits
//! are the example's account of how the session ended.
//!
//! These need `openssl` on the path, for the server's certificate.

mod support;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::QuicServerConfig;
use quinn::{Connection, ConnectionError, IdleTimeout, TransportConfig, VarInt};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// The longest the server or the subscriber may take. Reaching it is a
/// failure. The one time a test waits out is the idle timeout of the test
/// that is about it.
const BOUND: Duration = Duration::from_secs(20);
const DRAFTS: [&str; 2] = ["22", "18"];
/// The first and the second byte of a stream type neither draft defines
/// (256, in the two-byte form of the drafts' variable-length integer).
const UNKNOWN_TYPE: [u8; 2] = [0x81, 0x00];

/// One subscriber's run against one server.
struct Met<T> {
    run: support::Run,
    port: u16,
    /// What the server's script returned.
    server: T,
}

/// Run the subscriber against a server with `limits`, which does `script`
/// with the subscriber's connection.
fn meet<F, Fut, T>(name: &str, draft: &str, limits: TransportConfig, script: F) -> Met<T>
where
    F: FnOnce(Connection) -> Fut + Send + 'static,
    Fut: Future<Output = T>,
    T: Send + 'static,
{
    let dir = support::work(&format!("quinn-{name}-{draft}"));
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    support::certificate(&dir, &cert, &key);
    let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert)
        .expect("the certificate is read")
        .collect::<Result<_, _>>()
        .expect("the certificate is PEM");
    let secret = PrivateKeyDer::from_pem_file(&key).expect("the key is read");

    let (bound, port) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        runtime.block_on(async move {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let mut tls = rustls::ServerConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13])
                .expect("TLS 1.3")
                .with_no_client_auth()
                .with_single_cert(chain, secret)
                .expect("the certificate is accepted");
            tls.alpn_protocols = vec![b"moqt-22".to_vec(), b"moqt-18".to_vec()];
            let crypto = QuicServerConfig::try_from(tls).expect("a QUIC configuration");
            let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
            config.transport_config(Arc::new(limits));
            let local = "127.0.0.1:0".parse().expect("an address");
            let endpoint = quinn::Endpoint::server(config, local).expect("the server binds");
            let port = endpoint.local_addr().expect("its address").port();
            bound.send(port).expect("the test is waiting");
            let served = async {
                let incoming = endpoint.accept().await.expect("a connection");
                let conn = incoming.await.expect("a handshake");
                let seen = script(conn).await;
                endpoint.wait_idle().await;
                seen
            };
            tokio::time::timeout(BOUND, served)
                .await
                .expect("the server is done in time")
        })
    });

    let port = port.recv_timeout(BOUND).expect("the server's port");
    let url = format!("moqt://127.0.0.1:{port}");
    let args = ["--url", &url, "--ca", cert.to_str().expect("UTF-8")];
    let args = [&args[..], &["--namespace", "quinn/example"]].concat();
    let args = [&args[..], &["--track", "video", "--draft", draft]].concat();
    let run = support::run(&support::subscriber(&args), &dir, "sub", BOUND);
    Met {
        run,
        port,
        server: server.join().expect("the server"),
    }
}

/// Wait until a count the server keeps is above zero, and return it. The
/// server's bound ends a wait that never is.
async fn counted(count: impl Fn() -> u64) -> u64 {
    loop {
        match count() {
            0 => tokio::time::sleep(Duration::from_millis(5)).await,
            count => return count,
        }
    }
}

impl<T> Met<T> {
    /// The subscriber printed `ending` after connecting, held nothing,
    /// wrote nothing to standard error and exited 1.
    fn failed_with(&self, draft: &str, ending: &str) {
        let expected = [
            format!("connected 127.0.0.1:{} moqt-{draft}", self.port),
            ending.to_owned(),
            "held kept 0 refused 0 owed 0".to_owned(),
        ];
        assert_eq!(self.run.out, expected, "standard error: {}", self.run.err);
        assert_eq!(self.run.err, "");
        assert_eq!(self.run.status.code(), Some(1));
    }
}

/// The subscriber closed the connection with the protocol violation the
/// unknown stream type is.
fn closed_for_the_type(seen: &ConnectionError) {
    match seen {
        ConnectionError::ApplicationClosed(close) => {
            assert_eq!(close.error_code, VarInt::from_u32(0x3));
            assert_eq!(&close.reason[..], b"unknown stream type");
        }
        other => panic!("the server saw {other:?}"),
    }
}

/// The server gives the subscriber's streams no flow-control credit, so the
/// first write of the subscriber's control stream cannot proceed. The
/// server waits until the subscriber has told it so, and then completes an
/// unknown stream type. The session must close for that, and the close
/// must reach the server although the writer is still held: the subscriber
/// ends, and the server sees the session's code.
#[test]
fn the_close_ends_a_write_the_peer_holds() {
    for draft in DRAFTS {
        let mut limits = TransportConfig::default();
        limits.stream_receive_window(VarInt::from_u32(0));
        let met = meet("held-write", draft, limits, |conn| async move {
            let mut stream = conn.open_uni().await.expect("a stream");
            stream.write_all(&UNKNOWN_TYPE[..1]).await.expect("a byte");
            let blocked = counted(|| conn.stats().frame_rx.stream_data_blocked).await;
            stream.write_all(&UNKNOWN_TYPE[1..]).await.expect("a byte");
            (blocked, conn.closed().await)
        });
        let (blocked, seen) = &met.server;
        assert!(*blocked >= 1);
        closed_for_the_type(seen);
        met.failed_with(draft, "closed 3 unknown stream type");
    }
}

/// The server lets the subscriber open no stream, so the open of the
/// subscriber's control stream cannot proceed, and sends an unknown stream
/// type. The close must reach the server although the writer is still
/// held. No stream frame ever came from the subscriber.
#[test]
fn the_close_ends_an_open_the_peer_holds() {
    for draft in DRAFTS {
        let mut limits = TransportConfig::default();
        limits.max_concurrent_uni_streams(VarInt::from_u32(0));
        let met = meet("held-open", draft, limits, |conn| async move {
            let mut stream = conn.open_uni().await.expect("a stream");
            stream.write_all(&UNKNOWN_TYPE).await.expect("the type");
            let seen = conn.closed().await;
            (conn.stats().frame_rx.stream, seen)
        });
        let (stream_frames, seen) = &met.server;
        assert_eq!(*stream_frames, 0);
        closed_for_the_type(seen);
        met.failed_with(draft, "closed 3 unknown stream type");
    }
}

/// The peer's application closes the connection with code zero before
/// anything was subscribed. That is reported as the peer's close, and the
/// run fails: the subscription never ended as asked.
#[test]
fn a_clean_close_by_the_peer_is_reported_as_the_peers() {
    for draft in DRAFTS {
        let met = meet(
            "peer-zero",
            draft,
            TransportConfig::default(),
            |conn| async move {
                conn.close(VarInt::from_u32(0), b"");
                conn.closed().await
            },
        );
        met.failed_with(draft, "peer closed 0");
    }
}

/// The peer's application closes the connection with a code and a reason:
/// both are reported, and the run fails.
#[test]
fn a_close_by_the_peer_with_a_code_is_reported() {
    for draft in DRAFTS {
        let met = meet(
            "peer-code",
            draft,
            TransportConfig::default(),
            |conn| async move {
                conn.close(VarInt::from_u32(0x10), b"going away");
                conn.closed().await
            },
        );
        met.failed_with(draft, "peer closed 16 going away");
    }
}

/// The peer goes silent and the connection times out. No close arrived, so
/// none is reported: the run fails with what happened.
#[test]
fn a_connection_that_times_out_is_not_a_close() {
    for draft in DRAFTS {
        let mut limits = TransportConfig::default();
        let idle = IdleTimeout::try_from(Duration::from_secs(1)).expect("a timeout");
        limits.max_idle_timeout(Some(idle));
        let met = meet("timed-out", draft, limits, |conn| async move {
            conn.closed().await
        });
        assert!(matches!(met.server, ConnectionError::TimedOut));
        met.failed_with(draft, "connection lost: timed out");
    }
}
