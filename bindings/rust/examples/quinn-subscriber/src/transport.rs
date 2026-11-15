//! The quinn side: tasks that move bytes between the connection and the
//! session task. None of them calls the session.
//!
//! - One task accepts the peer's unidirectional streams and names each one
//!   for the session.
//! - One task per stream reads it, a read at a time: it waits until the
//!   session task has taken a read before it makes the next. A stream the
//!   session does not take is therefore not read, and QUIC flow control
//!   holds the peer.
//! - One task writes. It takes the session's output from a single queue and
//!   hands it to quinn in exactly that order.
//! - The session's close does not travel in that queue. It reaches the
//!   writer on a path of its own, and ends an open or a write the transport
//!   is making wait: a peer that withholds credit cannot hold the close.
//!
//! Every task here ends when the connection does, and each one waits for
//! the tasks it started.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;

use bytes::Bytes;
use moq5_core::{OwnedAction, OwnedActionDetail, StreamRef};
use quinn::{
    Connection, ConnectionError, ReadError, RecvStream, SendStream, StoppedError, VarInt,
    WriteError,
};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinSet};

/// The most one read hands the session.
const READ: usize = 16 * 1024;

/// What the transport tasks tell the session task.
pub enum Input {
    /// A stream this side reads now exists. Sending a code on `stop` stops
    /// it (STOP_SENDING); dropping `stop` abandons it.
    Opened {
        stream: StreamRef,
        stop: oneshot::Sender<u64>,
    },
    /// The next bytes of a stream, in order. A FIN comes alone, after the
    /// last bytes. The reading task makes no further read until `taken`
    /// is answered.
    Bytes {
        stream: StreamRef,
        bytes: Bytes,
        fin: bool,
        taken: oneshot::Sender<()>,
    },
    /// The peer reset a stream this side reads.
    Reset {
        stream: StreamRef,
        code: u64,
    },
    /// The peer stopped a stream this side writes.
    Stopped {
        stream: StreamRef,
        code: u64,
    },
    /// This side stopped reading the stream, as asked: nothing more of it
    /// will be delivered.
    ReadStopped {
        stream: StreamRef,
    },
    /// The answer to [`Output::Flush`].
    Flushed {
        stream: StreamRef,
    },
    Datagram(Bytes),
    /// The connection ended, and this side did not close it.
    Closed(Ended),
    /// The writer met output it cannot carry out.
    WriteFailed(String),
}

/// What the session task hands the writer.
pub enum Output {
    Action(OwnedAction),
    /// Answer [`Input::Flushed`] once everything queued before this has been
    /// handed to quinn.
    Flush(StreamRef),
}

/// The session's close: what the connection is closed with.
#[derive(Debug, PartialEq, Eq)]
pub struct Close {
    pub code: u64,
    pub reason: Vec<u8>,
}

/// How the connection ended when this side did not close it. Only the
/// first carries a MoQ termination code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ended {
    /// The peer's application closed it. Over raw QUIC its code is the
    /// session's termination code.
    PeerApplication { code: u64, reason: Vec<u8> },
    /// The peer's QUIC stack closed it, with a transport error code.
    PeerTransport { code: u64, reason: Vec<u8> },
    /// This side's QUIC stack closed it for what the peer sent, with a
    /// transport error code.
    LocalTransport { code: u64, reason: String },
    /// No close arrived: what happened instead.
    Lost(&'static str),
}

impl Ended {
    /// What a connection's end means here; `None` when this side closed it.
    pub fn of(error: ConnectionError) -> Option<Ended> {
        Some(match error {
            ConnectionError::LocallyClosed => return None,
            ConnectionError::ApplicationClosed(close) => Ended::PeerApplication {
                code: close.error_code.into_inner(),
                reason: close.reason.to_vec(),
            },
            ConnectionError::ConnectionClosed(close) => Ended::PeerTransport {
                code: close.error_code.into(),
                reason: close.reason.to_vec(),
            },
            ConnectionError::TransportError(error) => Ended::LocalTransport {
                code: error.code.into(),
                reason: error.reason,
            },
            ConnectionError::TimedOut => Ended::Lost("timed out"),
            ConnectionError::Reset => Ended::Lost("reset by the peer"),
            ConnectionError::VersionMismatch => Ended::Lost("no QUIC version in common"),
            ConnectionError::CidsExhausted => Ended::Lost("out of connection IDs"),
        })
    }
}

/// A code, and its reason phrase if it has one.
pub fn coded(code: impl fmt::Display, reason: &[u8]) -> String {
    match reason {
        [] => code.to_string(),
        reason => format!("{code} {}", String::from_utf8_lossy(reason)),
    }
}

impl fmt::Display for Ended {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Ended::PeerApplication { code, reason } => {
                write!(f, "peer closed {}", coded(code, reason))
            }
            Ended::PeerTransport { code, reason } => {
                let code = format!("{code:#x}");
                write!(
                    f,
                    "connection lost: peer transport error {}",
                    coded(code, reason)
                )
            }
            Ended::LocalTransport { code, reason } => {
                let code = format!("{code:#x}");
                write!(
                    f,
                    "connection lost: transport error {}",
                    coded(code, reason.as_bytes())
                )
            }
            Ended::Lost(how) => write!(f, "connection lost: {how}"),
        }
    }
}

/// A task this one started has ended. Its panic, if it had one, becomes
/// this task's, so that it reaches whoever waits for this one.
fn settled(joined: Result<(), JoinError>) {
    if let Err(error) = joined {
        if let Ok(panic) = error.try_into_panic() {
            std::panic::resume_unwind(panic);
        }
    }
}

fn varint(code: u64) -> VarInt {
    VarInt::from_u64(code).unwrap_or(VarInt::MAX)
}

/// Accept the peer's unidirectional streams. The `index`th one, counted from
/// zero, is `StreamRef::peer(index)` to the session.
pub async fn accept_streams(conn: Connection, input: mpsc::Sender<Input>) {
    let mut readers = JoinSet::new();
    let mut index = 0;
    loop {
        let recv = tokio::select! {
            accepted = conn.accept_uni() => match accepted {
                Ok(recv) => recv,
                Err(_) => break,
            },
            Some(joined) = readers.join_next() => {
                settled(joined);
                continue;
            }
        };
        let Some(stream) = StreamRef::peer(index) else {
            break;
        };
        index += 1;
        let (stop, stopped) = oneshot::channel();
        if input.send(Input::Opened { stream, stop }).await.is_err() {
            break;
        }
        readers.spawn(read_stream(recv, stream, stopped, input.clone()));
    }
    while let Some(joined) = readers.join_next().await {
        settled(joined);
    }
}

/// Read one stream until its FIN, its reset, or a stop from the session
/// task.
async fn read_stream(
    mut recv: RecvStream,
    stream: StreamRef,
    mut stop: oneshot::Receiver<u64>,
    input: mpsc::Sender<Input>,
) {
    loop {
        let (bytes, fin) = tokio::select! {
            code = &mut stop => return stop_reading(recv, stream, code.ok(), &input).await,
            read = recv.read_chunk(READ, true) => match read {
                Ok(Some(chunk)) => (chunk.bytes, false),
                Ok(None) => (Bytes::new(), true),
                Err(ReadError::Reset(code)) => {
                    let code = code.into_inner();
                    let _ = input.send(Input::Reset { stream, code }).await;
                    return;
                }
                // The connection is gone; its end is reported by `watch_close`.
                Err(_) => return,
            },
        };
        let (taken, wait) = oneshot::channel();
        let read = Input::Bytes {
            stream,
            bytes,
            fin,
            taken,
        };
        if input.send(read).await.is_err() {
            return;
        }
        tokio::select! {
            code = &mut stop => return stop_reading(recv, stream, code.ok(), &input).await,
            taken = wait => if taken.is_err() || fin {
                return;
            },
        }
    }
}

/// Stop the stream with the session's code and say so. Without a code the
/// session task has dropped the stream, and there is nothing to report.
async fn stop_reading(
    mut recv: RecvStream,
    stream: StreamRef,
    code: Option<u64>,
    input: &mpsc::Sender<Input>,
) {
    if let Some(code) = code {
        let _ = recv.stop(varint(code));
        let _ = input.send(Input::ReadStopped { stream }).await;
    }
}

/// Report the peer's stop of a stream this side writes, whenever it comes.
async fn watch_stop(
    stopped: impl Future<Output = Result<Option<VarInt>, StoppedError>>,
    stream: StreamRef,
    input: mpsc::Sender<Input>,
) {
    if let Ok(Some(code)) = stopped.await {
        let code = code.into_inner();
        let _ = input.send(Input::Stopped { stream, code }).await;
    }
}

/// Deliver datagrams as they arrive. quinn bounds how many it keeps unread.
pub async fn read_datagrams(conn: Connection, input: mpsc::Sender<Input>) {
    while let Ok(bytes) = conn.read_datagram().await {
        if input.send(Input::Datagram(bytes)).await.is_err() {
            return;
        }
    }
}

/// Report the end of the connection, unless this side closed it.
pub async fn watch_close(conn: Connection, input: mpsc::Sender<Input>) {
    if let Some(ended) = Ended::of(conn.closed().await) {
        let _ = input.send(Input::Closed(ended)).await;
    }
}

/// Hand the session's output to quinn in the order it was queued, and end
/// the connection with the session's close when `closing` brings it. This
/// task ends only then, or when the session task ends without a close.
///
/// Output already queued goes first. The transport can make an open wait
/// for stream credit and a write for flow control, for as long as the peer
/// likes; the close ends that wait instead of standing behind it. The
/// connection is closed then, so the output cut short is never resumed.
pub async fn write_output(
    conn: Connection,
    mut output: mpsc::Receiver<Output>,
    mut closing: oneshot::Receiver<Close>,
    input: mpsc::Sender<Input>,
) {
    let mut writer = Writer {
        conn,
        input,
        control: None,
        streams: HashMap::new(),
        ended: HashSet::new(),
        tasks: JoinSet::new(),
    };
    let close = loop {
        let next = tokio::select! {
            biased;
            next = output.recv() => next,
            close = &mut closing => break close.ok(),
        };
        // The session task has ended, and with it the queue. Its close, if
        // it raised one, is there.
        let Some(next) = next else {
            break closing.await.ok();
        };
        let carried = tokio::select! {
            biased;
            carried = writer.carry(next) => carried,
            close = &mut closing => break close.ok(),
        };
        match carried {
            Ok(Carried::On) => {}
            // Nothing more can be written. The session task hears of the
            // connection's end from `watch_close`; this task ends when the
            // session task does.
            Ok(Carried::Ended) => break closing.await.ok(),
            // The session task closes the session for this.
            Err(why) => {
                let _ = writer.say(Input::WriteFailed(why)).await;
                break closing.await.ok();
            }
        }
    };
    writer.end(close).await;
}

enum Carried {
    On,
    /// The connection is lost, or the session task is gone: nothing more
    /// is written.
    Ended,
}

struct Writer {
    conn: Connection,
    input: mpsc::Sender<Input>,
    /// This side's control stream.
    control: Option<StreamRef>,
    /// The streams this side still writes.
    streams: HashMap<StreamRef, SendStream>,
    /// Streams this side finished or reset, or the peer stopped: output for
    /// one of these is dropped.
    ended: HashSet<StreamRef>,
    /// The tasks this one started: the reader of each stream it opened in
    /// both directions, and a watcher for each stream it writes.
    tasks: JoinSet<()>,
}

impl Writer {
    async fn say(&self, input: Input) -> Result<(), ()> {
        self.input.send(input).await.map_err(|_| ())
    }

    /// Close the connection with the session's close, if there is one, and
    /// wait for the tasks this one started. They end with the connection:
    /// when the session task ended without a close, whoever ran it ends
    /// the connection.
    async fn end(mut self, close: Option<Close>) {
        if let Some(close) = close {
            self.conn.close(varint(close.code), &close.reason);
        }
        while let Some(joined) = self.tasks.join_next().await {
            settled(joined);
        }
    }

    async fn carry(&mut self, next: Output) -> Result<Carried, String> {
        match next {
            Output::Flush(stream) => match self.say(Input::Flushed { stream }).await {
                Ok(()) => Ok(Carried::On),
                Err(()) => Ok(Carried::Ended),
            },
            Output::Action(action) => self.carry_out(action).await,
        }
    }

    async fn carry_out(&mut self, action: OwnedAction) -> Result<Carried, String> {
        match action.into_detail() {
            OwnedActionDetail::OpenUniControl { stream, bytes } => {
                let Ok(send) = self.conn.open_uni().await else {
                    return Ok(Carried::Ended);
                };
                self.control = Some(stream);
                self.open(stream, send);
                self.write(stream, &bytes, false).await
            }
            // The session's later control messages come as `SendControl`;
            // one that names the stream is the same write.
            OwnedActionDetail::SendControl { bytes }
            | OwnedActionDetail::SendUniControl { bytes, .. } => match self.control {
                Some(stream) => self.write(stream, &bytes, false).await,
                None => Err("control output before the control stream".into()),
            },
            OwnedActionDetail::OpenBidiStream { stream, bytes, fin } => {
                let Ok((send, recv)) = self.conn.open_bi().await else {
                    return Ok(Carried::Ended);
                };
                let (stop, stopped) = oneshot::channel();
                if self.say(Input::Opened { stream, stop }).await.is_err() {
                    return Ok(Carried::Ended);
                }
                let input = self.input.clone();
                self.tasks.spawn(read_stream(recv, stream, stopped, input));
                self.open(stream, send);
                self.write(stream, &bytes, fin).await
            }
            OwnedActionDetail::SendBidiStream { stream, bytes, fin } => {
                self.write(stream, &bytes, fin).await
            }
            OwnedActionDetail::CloseBidiStream { stream } => self.write(stream, &[], true).await,
            // The reading half of an aborted stream was stopped by the
            // session task; this half is reset here.
            OwnedActionDetail::ResetBidiStream { stream, error_code }
            | OwnedActionDetail::AbortBidiStream { stream, error_code } => {
                if let Some(mut send) = self.streams.remove(&stream) {
                    let _ = send.reset(varint(error_code));
                }
                self.ended.insert(stream);
                Ok(Carried::On)
            }
            // A subscriber sends no objects and no datagrams, and the
            // session's close does not come this way.
            other => Err(format!("this example does not carry out {other:?}")),
        }
    }

    /// Keep a stream this side writes, and watch for the peer stopping it.
    fn open(&mut self, stream: StreamRef, send: SendStream) {
        let input = self.input.clone();
        self.tasks.spawn(watch_stop(send.stopped(), stream, input));
        self.streams.insert(stream, send);
    }

    /// Write `bytes` on `stream`, then its FIN if `fin`.
    async fn write(
        &mut self,
        stream: StreamRef,
        bytes: &[u8],
        fin: bool,
    ) -> Result<Carried, String> {
        let Some(send) = self.streams.get_mut(&stream) else {
            return if self.ended.contains(&stream) {
                Ok(Carried::On)
            } else {
                Err(format!("output for {stream:?}, which was never opened"))
            };
        };
        match send.write_all(bytes).await {
            Ok(()) => {
                if fin {
                    let _ = send.finish();
                    self.streams.remove(&stream);
                    self.ended.insert(stream);
                }
                Ok(Carried::On)
            }
            // The peer stopped the stream, which `watch_stop` reports.
            Err(WriteError::Stopped(_)) => {
                self.streams.remove(&stream);
                self.ended.insert(stream);
                Ok(Carried::On)
            }
            Err(_) => Ok(Carried::Ended),
        }
    }
}
