//! How the session task ends, with the test as its transport.
//!
//! The test holds the other end of the session task's channels: it takes
//! the task's output as the writer would, and feeds it input as the reading
//! tasks would, so every step is in its hands and nothing waits for time.
//! The bytes it feeds are a real publisher's: a second session, on the
//! server side, which the test drives.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use moq5_core::{
    AcceptSubscribe, ActionPoll, Buffer, Event, Facts, Instant, Namespace, OwnedActionDetail,
    Perspective, Session, SessionConfig, StreamRef, SubgroupConfig, Subscription, TransportBinding,
    UriVerdict, Version,
};
use quinn::{ApplicationClose, ConnectionClose, ConnectionError, TransportErrorCode, VarInt};
use tokio::sync::{mpsc, oneshot};

use crate::host::{Ending, Host, Summary, Want};
use crate::transport::{Close, Ended, Input, Output};

/// The longest a run may take. Reaching it is a failure: nothing here
/// waits for time to pass.
const BOUND: Duration = Duration::from_secs(20);
const DRAFTS: [u32; 2] = [22, 18];

/// The transport's ends of the session task's channels.
struct Wire {
    input: mpsc::Sender<Input>,
    output: mpsc::Receiver<Output>,
    closing: oneshot::Receiver<Close>,
    /// For each stream the session task reads: how it asks for a stop.
    stops: HashMap<StreamRef, oneshot::Receiver<u64>>,
    /// The peer streams opened so far.
    peer_streams: u64,
}

/// A session task that wants `objects` objects, and its transport. `room`
/// is the room in the writer's queue.
fn subscriber(draft: u32, objects: Option<u64>, room: usize) -> (Host, Wire) {
    let version = Version::from_draft(draft);
    let session = crate::session(version, false, true, "localhost:4443", "").expect("a session");
    let (input, inputs) = mpsc::channel(crate::INPUTS);
    let (outputs, output) = mpsc::channel(room);
    let (closing, close) = oneshot::channel();
    let want = Want {
        namespace: Namespace::parse("quinn/example").expect("a namespace"),
        track: "video".into(),
        objects,
    };
    let epoch = std::time::Instant::now();
    let host = Host::new(session, epoch, inputs, outputs, closing, want);
    let wire = Wire {
        input,
        output,
        closing: close,
        stops: HashMap::new(),
        peer_streams: 0,
    };
    (host, wire)
}

impl Wire {
    async fn say(&self, input: Input) {
        self.input
            .send(input)
            .await
            .unwrap_or_else(|_| panic!("the session task has ended"));
    }

    /// A stream the session task reads now exists.
    async fn open(&mut self, stream: StreamRef) {
        let (stop, stopped) = oneshot::channel();
        self.stops.insert(stream, stopped);
        self.say(Input::Opened { stream, stop }).await;
    }

    /// The peer opens a unidirectional stream.
    async fn open_peer(&mut self) -> StreamRef {
        let stream = StreamRef::peer(self.peer_streams).expect("a stream");
        self.peer_streams += 1;
        self.open(stream).await;
        stream
    }

    /// One read of a stream. Returns once the session task has taken it.
    async fn give(&self, stream: StreamRef, bytes: &[u8], fin: bool) {
        let (taken, wait) = oneshot::channel();
        let bytes = Bytes::copy_from_slice(bytes);
        self.say(Input::Bytes {
            stream,
            bytes,
            fin,
            taken,
        })
        .await;
        wait.await.expect("the read is taken");
    }

    /// The session task's next action, as the writer would take it.
    async fn action(&mut self) -> OwnedActionDetail {
        loop {
            match self.output.recv().await.expect("the session task runs") {
                Output::Action(action) => return action.into_detail(),
                Output::Flush(_) => {}
            }
        }
    }

    /// The session asked for `stream` to be stopped.
    async fn stopped(&mut self, stream: StreamRef) {
        let stop = self
            .stops
            .remove(&stream)
            .expect("a stream that was opened");
        stop.await.expect("a stop");
    }
}

/// The publisher: a server-side session whose transport is the test.
struct Publisher {
    session: Session,
    micros: u64,
    /// The subscriber's request stream, as this session names it.
    request: StreamRef,
}

impl Publisher {
    /// A started session: its first action opens its control stream.
    fn new(draft: u32) -> Publisher {
        let config = SessionConfig::new(Perspective::Server, Version::from_draft(draft));
        let mut session = Session::try_new(&config, Instant::ZERO).expect("a session");
        let binding = TransportBinding::raw_quic_server(
            Box::new(|_| UriVerdict::Accept),
            Facts::all_supported(),
        );
        if let Err(refused) = session.bind_transport(binding) {
            panic!("the binding is refused: {refused}");
        }
        let mut publisher = Publisher {
            session,
            micros: 0,
            request: StreamRef::peer(0).expect("a stream"),
        };
        let now = publisher.now();
        publisher.session.start(now).expect("the session starts");
        publisher
    }

    fn now(&mut self) -> Instant {
        self.micros += 1;
        Instant::from_micros(self.micros)
    }

    /// Everything the session has to send, in order.
    fn actions(&mut self) -> Vec<OwnedActionDetail> {
        let mut actions = Vec::new();
        while let ActionPoll::Ready(action) = self.session.poll_action().expect("a poll") {
            let owned = action.into_owned().map_err(|(_, error)| error);
            actions.push(owned.expect("a copy").into_detail());
        }
        actions
    }

    /// The subscription the subscriber asked for.
    fn requested(&mut self) -> Subscription {
        let mut requested = None;
        while let Some(event) = self.session.poll_event().expect("a poll") {
            if let Event::SubscribeRequest(request) = event {
                requested = Some(request.subscription);
            }
        }
        requested.expect("a subscribe request")
    }
}

/// What a stream the publisher writes is to the subscriber.
#[derive(Default)]
struct Carried {
    /// The subscriber's request stream, as the subscriber names it.
    request: Option<StreamRef>,
    /// The publisher's control stream, as the subscriber names it.
    control: Option<StreamRef>,
    /// The publisher's data streams, as the subscriber names them.
    data: HashMap<StreamRef, StreamRef>,
}

impl Carried {
    /// Carry the publisher's output to the subscriber.
    async fn publisher_sends(&mut self, wire: &mut Wire, publisher: &mut Publisher) {
        for action in publisher.actions() {
            match action {
                OwnedActionDetail::OpenUniControl { bytes, .. } => {
                    let stream = wire.open_peer().await;
                    self.control = Some(stream);
                    wire.give(stream, &bytes, false).await;
                }
                OwnedActionDetail::SendControl { bytes } => {
                    let stream = self.control.expect("a control stream");
                    wire.give(stream, &bytes, false).await;
                }
                OwnedActionDetail::SendBidiStream { bytes, fin, .. } => {
                    let stream = self.request.expect("a request stream");
                    wire.give(stream, &bytes, fin).await;
                }
                OwnedActionDetail::SendData {
                    stream,
                    header,
                    payload,
                    ..
                } => {
                    let to = match self.data.get(&stream) {
                        Some(&to) => to,
                        None => {
                            let to = wire.open_peer().await;
                            self.data.insert(stream, to);
                            to
                        }
                    };
                    let mut bytes = header.as_bytes().to_vec();
                    bytes.extend_from_slice(payload.as_ref().map_or(&[][..], Buffer::as_slice));
                    wire.give(to, &bytes, false).await;
                }
                other => panic!("the publisher sends {other:?}"),
            }
        }
    }

    /// Carry the subscriber's next action to the publisher.
    async fn subscriber_sends(&mut self, wire: &mut Wire, publisher: &mut Publisher) {
        let now = publisher.now();
        let fed = match wire.action().await {
            OwnedActionDetail::OpenUniControl { bytes, .. }
            | OwnedActionDetail::SendControl { bytes } => {
                publisher.session.on_control_bytes(&bytes, now).is_ok()
            }
            OwnedActionDetail::OpenBidiStream { stream, bytes, fin } => {
                self.request = Some(stream);
                wire.open(stream).await;
                let request = publisher.request;
                publisher
                    .session
                    .on_bidi_bytes(request, &bytes, fin, now)
                    .is_ok()
            }
            other => panic!("the subscriber sends {other:?}"),
        };
        assert!(fed, "the publisher takes the subscriber's bytes");
    }
}

/// Bring a subscription to two delivered objects, with the subgroup's
/// stream still open. The stream that carries them, as the subscriber
/// names it.
async fn two_objects(
    wire: &mut Wire,
    publisher: &mut Publisher,
    carried: &mut Carried,
) -> StreamRef {
    // Each side's control stream and setup.
    carried.subscriber_sends(wire, publisher).await;
    carried.publisher_sends(wire, publisher).await;
    // The subscriber's request, accepted.
    carried.subscriber_sends(wire, publisher).await;
    carried.publisher_sends(wire, publisher).await;
    let subscription = publisher.requested();
    let now = publisher.now();
    publisher
        .session
        .accept_subscribe(subscription, &AcceptSubscribe::default(), now)
        .expect("the subscription is accepted");
    carried.publisher_sends(wire, publisher).await;
    // Two objects of one subgroup, which stays open.
    let subgroup = publisher
        .session
        .open_subgroup(subscription, &SubgroupConfig::new(0), now)
        .expect("a subgroup");
    for object in 0..2 {
        let payload = Buffer::copy_from(&[0x5a; 32]).expect("a payload");
        if let Err(refused) = publisher
            .session
            .write_object(subgroup, object, payload, now)
        {
            panic!("object {object} is refused: {}", refused.error);
        }
    }
    carried.publisher_sends(wire, publisher).await;
    *carried.data.values().next().expect("a data stream")
}

/// Run the session task with `script` as its transport.
async fn run<S>(host: Host, script: S) -> (Summary, S::Output)
where
    S: std::future::Future,
{
    let both = async { tokio::join!(host.run(), script) };
    let (summary, scripted) = tokio::time::timeout(BOUND, both)
        .await
        .expect("the session task ends in time");
    (
        summary.expect("the session task ends without an error"),
        scripted,
    )
}

/// The subscriber wants two objects and gets them. It unsubscribes, and
/// the session asks for the subgroup's stream to be stopped. Nothing
/// answers that, so the stream's retirement is outstanding when the
/// connection ends as `ended`.
async fn ended_during_cleanup(draft: u32, ended: Ended) -> Summary {
    let (host, mut wire) = subscriber(draft, Some(2), crate::OUTPUTS);
    let mut publisher = Publisher::new(draft);
    let script = async {
        let mut carried = Carried::default();
        let data = two_objects(&mut wire, &mut publisher, &mut carried).await;
        wire.stopped(data).await;
        wire.say(Input::Closed(ended)).await;
    };
    let (summary, ()) = run(host, script).await;
    assert_eq!(summary.objects, 2);
    assert!(summary.enough);
    summary
}

fn peer_application(code: u64, reason: &str) -> Ended {
    Ended::PeerApplication {
        code,
        reason: reason.as_bytes().to_vec(),
    }
}

/// Enough objects have arrived and the subscription's streams are still
/// being settled when the connection ends. Only a close by the peer's
/// application with code zero leaves the run a success. Every other end is
/// a failure, reported as what it was: none is turned into a close with
/// code zero, and a transport's code zero is not the session's.
#[tokio::test]
async fn a_lost_connection_fails_the_run_whatever_had_arrived() {
    let lost = [
        (
            peer_application(0x10, "going away"),
            "peer closed 16 going away",
        ),
        (
            Ended::PeerTransport {
                code: 0,
                reason: Vec::new(),
            },
            "connection lost: peer transport error 0x0",
        ),
        (
            Ended::PeerTransport {
                code: 0xa,
                reason: b"bad frame".to_vec(),
            },
            "connection lost: peer transport error 0xa bad frame",
        ),
        (
            Ended::LocalTransport {
                code: 0x3,
                reason: "too much data".into(),
            },
            "connection lost: transport error 0x3 too much data",
        ),
        (Ended::Lost("timed out"), "connection lost: timed out"),
        (
            Ended::Lost("reset by the peer"),
            "connection lost: reset by the peer",
        ),
    ];
    for draft in DRAFTS {
        let clean = peer_application(0, "");
        let summary = ended_during_cleanup(draft, clean.clone()).await;
        assert_eq!(summary.ending, Some(Ending::Connection(clean)));
        assert_eq!(
            summary.ending.as_ref().map(Ending::to_string).as_deref(),
            Some("peer closed 0")
        );
        assert!(summary.succeeded());

        for (ended, diagnostic) in lost.clone() {
            let summary = ended_during_cleanup(draft, ended.clone()).await;
            let ending = summary.ending.clone().expect("an ending");
            assert_eq!(ending, Ending::Connection(ended));
            assert_eq!(ending.to_string(), diagnostic);
            assert!(!summary.succeeded(), "{diagnostic} is a failure");
        }
    }
}

/// The same subscription, with the transport answering: each stream the
/// session asks to stop is stopped and retired, and the session closes
/// itself with code zero. That close comes on the path of its own, and the
/// run is a success.
#[tokio::test]
async fn a_settled_unsubscribe_closes_the_session_itself() {
    for draft in DRAFTS {
        let (host, mut wire) = subscriber(draft, Some(2), crate::OUTPUTS);
        let mut publisher = Publisher::new(draft);
        let script = async {
            let mut carried = Carried::default();
            let data = two_objects(&mut wire, &mut publisher, &mut carried).await;
            let request = carried.request.expect("a request stream");
            for stream in [request, data] {
                wire.stopped(stream).await;
                wire.say(Input::ReadStopped { stream }).await;
            }
            // The writer answers each marker once it has passed.
            loop {
                tokio::select! {
                    close = &mut wire.closing => break close.expect("a close"),
                    next = wire.output.recv() => {
                        if let Some(Output::Flush(stream)) = next {
                            wire.say(Input::Flushed { stream }).await;
                        }
                    }
                }
            }
        };
        let (summary, close) = run(host, script).await;
        let clean = Close {
            code: 0,
            reason: Vec::new(),
        };
        assert_eq!(close, clean);
        let closed = Ending::Closed {
            code: 0,
            reason: Vec::new(),
        };
        assert_eq!(summary.ending, Some(closed));
        assert!(summary.succeeded());
    }
}

/// The writer's queue has room for one output, and the writer takes none.
/// The session's first output fills the queue; once the setup is complete
/// its SUBSCRIBE has to wait behind that. Then the peer sends an unknown
/// stream type. The session's close must reach the transport all the same:
/// it supersedes what waits, and does not stand behind the queue.
#[tokio::test]
async fn a_full_queue_does_not_hold_the_close() {
    for draft in DRAFTS {
        let (host, mut wire) = subscriber(draft, None, 1);
        let mut publisher = Publisher::new(draft);
        let script = async {
            // The publisher's setup, without the subscriber's: nothing has
            // left the queue.
            let mut carried = Carried::default();
            carried.publisher_sends(&mut wire, &mut publisher).await;
            assert_eq!(wire.output.len(), wire.output.max_capacity());

            let stream = wire.open_peer().await;
            let (taken, _) = oneshot::channel();
            wire.say(Input::Bytes {
                stream,
                bytes: Bytes::from_static(&[0x81, 0x00]),
                fin: false,
                taken,
            })
            .await;
            (&mut wire.closing).await.expect("a close")
        };
        let (summary, close) = run(host, script).await;
        let violation = Close {
            code: 0x3,
            reason: b"unknown stream type".to_vec(),
        };
        assert_eq!(close, violation);
        let closed = Ending::Closed {
            code: 0x3,
            reason: b"unknown stream type".to_vec(),
        };
        assert_eq!(summary.ending, Some(closed));
        assert!(!summary.succeeded());

        // The one output that ever entered the queue is the first: the
        // SUBSCRIBE never left the session task.
        let first = wire.output.recv().await.expect("the first output");
        let Output::Action(first) = first else {
            panic!("the first output is a marker");
        };
        assert!(matches!(
            first.detail(),
            OwnedActionDetail::OpenUniControl { .. }
        ));
        assert!(wire.output.recv().await.is_none());
    }
}

/// What each way a connection ends is taken for. Only a close by the
/// peer's application has a termination code; this side's own close is not
/// an end to report.
#[test]
fn each_end_of_a_connection_is_told_apart() {
    let application = ConnectionError::ApplicationClosed(ApplicationClose {
        error_code: VarInt::from_u32(0x10),
        reason: Bytes::from_static(b"going away"),
    });
    assert_eq!(
        Ended::of(application),
        Some(peer_application(0x10, "going away"))
    );
    let transport = ConnectionError::ConnectionClosed(ConnectionClose {
        error_code: TransportErrorCode::NO_ERROR,
        frame_type: None,
        reason: Bytes::new(),
    });
    let peer_transport = Ended::PeerTransport {
        code: 0,
        reason: Vec::new(),
    };
    assert_eq!(Ended::of(transport), Some(peer_transport));
    let local = ConnectionError::TransportError(TransportErrorCode::FLOW_CONTROL_ERROR.into());
    let local_transport = Ended::LocalTransport {
        code: 0x3,
        reason: String::new(),
    };
    assert_eq!(Ended::of(local), Some(local_transport));
    assert_eq!(
        Ended::of(ConnectionError::TimedOut),
        Some(Ended::Lost("timed out"))
    );
    assert_eq!(
        Ended::of(ConnectionError::Reset),
        Some(Ended::Lost("reset by the peer"))
    );
    assert_eq!(
        Ended::of(ConnectionError::VersionMismatch),
        Some(Ended::Lost("no QUIC version in common"))
    );
    assert_eq!(
        Ended::of(ConnectionError::CidsExhausted),
        Some(Ended::Lost("out of connection IDs"))
    );
    assert_eq!(Ended::of(ConnectionError::LocallyClosed), None);
}
