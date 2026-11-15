//! A bare MoQ subscriber over quinn.
//!
//! It connects to a relay or publisher over raw QUIC, completes the MoQ
//! setup, subscribes to one track and prints each object it receives. The
//! MoQ session is `moq5_core::Session`, which does no I/O: this program is
//! its host. `host.rs` is the task that owns the session; `transport.rs` is
//! the quinn side.
//!
//! Drafts 18 and 22, where each side opens a unidirectional control stream.
//! The session's output is polled untagged and written in poll order.
//!
//! ```text
//! moq5-quinn-subscriber --url moqt://relay.example:4443 --ca roots.pem \
//!     --namespace live/demo --track video [--draft 22] [--objects 10] [--tight]
//! ```
//!
//! `--tight` gives the session an event queue of two, so that it has to
//! keep input it cannot finish at once and the host has to resume it: the
//! custody the host contract asks for, on any connection.
//!
//! It prints one line per step: `connected`, `setup complete`,
//! `subscribed`, `object <group> <object> <n> bytes`, then `done` when the
//! publisher completes the subscription, or `unsubscribed` after
//! `--objects`, and then how the session ended: `closed` with this side's
//! code, `peer closed` with the peer's, or `connection lost` with what
//! happened. The last line counts what the host had to hold: reads the
//! session kept, reads it did not admit, signals it owed.
//!
//! It exits 0 when the subscription ended in one of those two ways and the
//! session was then closed with code 0. A connection that is lost has no
//! code: it is a failure whatever had arrived.

mod host;
#[cfg(test)]
mod tests;
mod transport;

use std::error::Error;
use std::net::{SocketAddr, ToSocketAddrs};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use moq5_core::{
    ConnectionTarget, Fact, Facts, Instant, Namespace, Perspective, Session, SessionConfig,
    TransportBinding, Version,
};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::host::{Host, Summary, Want};

/// Peer streams open at once. Each holds at most one read here, so this
/// bounds what the host keeps for a session that is not taking input.
const PEER_STREAMS: u32 = 64;
/// Messages from the transport tasks waiting for the session task.
const INPUTS: usize = 64;
/// Actions on their way to the writer. When this is full the session is
/// not polled, and its own queues push back.
const OUTPUTS: usize = 32;
/// The session's event queue under `--tight`: two events.
const TIGHT_EVENTS: u32 = 2;
/// How often the connection is kept alive while nothing else is sent. A
/// subscription can be quiet for longer than QUIC's idle timeout.
const KEEP_ALIVE: Duration = Duration::from_secs(10);
/// What the connection is closed with when the session task fails.
const CLOSE_INTERNAL_ERROR: u32 = 0x1;

const USAGE: &str = "usage: moq5-quinn-subscriber --url moqt://HOST:PORT[/PATH] --ca ROOTS.pem \
--namespace A/B --track NAME [--draft 18|22] [--objects N] [--tight]";

struct Args {
    url: String,
    ca: String,
    namespace: String,
    track: String,
    draft: u32,
    objects: Option<u64>,
    tight: bool,
}

fn args() -> Result<Args, Box<dyn Error>> {
    let (mut url, mut ca, mut namespace, mut track) = (None, None, None, None);
    let (mut draft, mut objects, mut tight) = (22, None, false);
    let mut words = std::env::args().skip(1);
    while let Some(word) = words.next() {
        if word == "--tight" {
            tight = true;
            continue;
        }
        let value = words.next().ok_or(USAGE)?;
        match word.as_str() {
            "--url" => url = Some(value),
            "--ca" => ca = Some(value),
            "--namespace" => namespace = Some(value),
            "--track" => track = Some(value),
            "--draft" => draft = value.parse()?,
            "--objects" => objects = Some(value.parse()?),
            _ => return Err(USAGE.into()),
        }
    }
    if draft != 18 && draft != 22 {
        return Err("this example speaks drafts 18 and 22".into());
    }
    Ok(Args {
        url: url.ok_or(USAGE)?,
        ca: ca.ok_or(USAGE)?,
        namespace: namespace.ok_or(USAGE)?,
        track: track.ok_or(USAGE)?,
        draft,
        objects,
        tight,
    })
}

/// `moqt://AUTHORITY[PATH]` as (authority, path and query, host, port).
fn split(url: &str) -> Result<(&str, &str, &str, u16), Box<dyn Error>> {
    let rest = url
        .strip_prefix("moqt://")
        .ok_or("the URL must begin moqt://")?;
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let (host, port) = authority.rsplit_once(':').ok_or("the URL needs a port")?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    Ok((authority, path, host, port.parse()?))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let done = match args() {
        Ok(args) => run(args).await,
        Err(error) => Err(error),
    };
    match done {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("moq5-quinn-subscriber: {error}");
            ExitCode::FAILURE
        }
    }
}

/// TLS 1.3 with the given roots and the draft's ALPN, and this example's
/// limits on the connection.
fn client_config(
    roots: rustls::RootCertStore,
    alpn: &str,
) -> Result<quinn::ClientConfig, Box<dyn Error>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![alpn.as_bytes().to_vec()];

    // This example serves no requests, so the peer may open no
    // bidirectional stream; its unidirectional streams are limited.
    let mut limits = quinn::TransportConfig::default();
    limits.max_concurrent_bidi_streams(0u32.into());
    limits.max_concurrent_uni_streams(PEER_STREAMS.into());
    limits.keep_alive_interval(Some(KEEP_ALIVE));
    let mut client = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls)?));
    client.transport_config(Arc::new(limits));
    Ok(client)
}

/// The session, bound to what this transport is: a raw QUIC client
/// connection to `authority`, its handshake complete, with `datagrams` if
/// the peer accepts them.
fn session(
    version: Version,
    tight: bool,
    datagrams: bool,
    authority: &str,
    path: &str,
) -> Result<Session, Box<dyn Error>> {
    let mut config = SessionConfig::new(Perspective::Client, version);
    if tight {
        config = config.max_events(TIGHT_EVENTS);
    }
    let mut session = Session::try_new(&config, Instant::ZERO)?;
    let mut facts = Facts::all_supported();
    facts.set(Fact::PeerQuicDatagram, datagrams);
    facts.set(Fact::DatagramSend, datagrams);
    let target = ConnectionTarget::new(authority, path);
    session
        .bind_transport(TransportBinding::raw_quic_client(target, facts))
        .map_err(|refused| refused.to_string())?;
    Ok(session)
}

/// Run the session over the connection until the session has closed, the
/// connection has ended and every task started here has ended with it.
async fn drive(
    conn: &quinn::Connection,
    session: Session,
    want: Want,
) -> Result<Summary, Box<dyn Error>> {
    let epoch = std::time::Instant::now();
    let (inputs, input) = mpsc::channel(INPUTS);
    let (output, queued) = mpsc::channel(OUTPUTS);
    let (closing, close) = oneshot::channel();
    let mut tasks = JoinSet::new();
    tasks.spawn(transport::accept_streams(conn.clone(), inputs.clone()));
    tasks.spawn(transport::read_datagrams(conn.clone(), inputs.clone()));
    tasks.spawn(transport::watch_close(conn.clone(), inputs.clone()));
    let mut writer = tokio::spawn(transport::write_output(conn.clone(), queued, close, inputs));
    let mut run = Box::pin(Host::new(session, epoch, input, output, closing, want).run());

    // The writer ends on the session's close, or when the session task
    // ends. Ending before either, it failed.
    let first = tokio::select! {
        biased;
        outcome = &mut run => Ok(outcome),
        written = &mut writer => Err(written),
    };
    // The session task is over, or abandoned: its ends of the channels go
    // with it, so no task is left waiting to hand it something.
    drop(run);
    let (outcome, written) = match first {
        Ok(outcome) => {
            // A session task that failed raised no close: the connection
            // is ended here, which also ends whatever the writer waits for.
            if outcome.is_err() {
                conn.close(CLOSE_INTERNAL_ERROR.into(), b"host error");
            }
            (outcome, writer.await)
        }
        Err(written) => (Err("the writer ended before the session".into()), written),
    };
    // The writer has carried out the session's close, if there was one.
    // Closing a connection that is closed, or that had ended by itself,
    // changes nothing; one that is still open ends here, and with it the
    // tasks that read it.
    conn.close(CLOSE_INTERNAL_ERROR.into(), b"");
    let mut failed = written.err();
    while let Some(joined) = tasks.join_next().await {
        failed = failed.or(joined.err());
    }
    match failed {
        Some(error) => Err(error.into()),
        None => outcome,
    }
}

async fn run(args: Args) -> Result<bool, Box<dyn Error>> {
    let (authority, path, host, port) = split(&args.url)?;
    let version = Version::from_draft(args.draft);
    let want = Want {
        namespace: Namespace::parse(&args.namespace)?,
        track: args.track.clone(),
        objects: args.objects,
    };
    let mut roots = rustls::RootCertStore::empty();
    for certificate in CertificateDer::pem_file_iter(&args.ca)? {
        roots.add(certificate?)?;
    }
    let alpn = format!("moqt-{}", args.draft);
    let client = client_config(roots, &alpn)?;

    let peer = (host, port)
        .to_socket_addrs()?
        .next()
        .ok_or("the host has no address")?;
    let local: SocketAddr = if peer.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    }
    .parse()?;
    let endpoint = quinn::Endpoint::client(local)?;
    let conn = endpoint.connect_with(client, peer, host)?.await?;
    println!("connected {peer} {alpn}");

    let datagrams = conn.max_datagram_size().is_some();
    let session = session(version, args.tight, datagrams, authority, path)?;
    let driven = drive(&conn, session, want).await;
    // The connection is closed; it is given a moment to say so to the peer.
    let _ = tokio::time::timeout(Duration::from_secs(3), endpoint.wait_idle()).await;

    let summary = driven?;
    println!(
        "held kept {} refused {} owed {}",
        summary.kept_reads, summary.refused_reads, summary.owed_signals
    );
    Ok(summary.succeeded())
}
